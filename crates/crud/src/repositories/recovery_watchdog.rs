//! Authoritative watchdog fence. The reader's policy/liveness is preparation;
//! the conditional writer statement decides whether the episode is unchanged.
use anyhow::Result;
use pioneer_entity::{recovery_job, turn_liveness};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, QueryTrait};

/// Exact persisted inputs, kept separate from the public lossy legacy mapper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawPolicy {
    pub(crate) action: String,
    pub(crate) error_class: Option<String>,
    pub(crate) policy_snapshot: String,
    pub(crate) scheduled_at: DateTimeWithTimeZone,
    pub(crate) active_attempt_started_at: Option<DateTimeWithTimeZone>,
    pub(crate) updated_at: DateTimeWithTimeZone,
}
impl RawPolicy {
    pub(crate) fn agrees_with(&self, expected: &crate::RecoveryJobRecord) -> bool {
        let action = crate::convention::recovery_action_from_db(&self.action)
            .unwrap_or(pioneer_protocol::RecoveryAction::RetryAttempt);
        let error_class = self
            .error_class
            .as_deref()
            .and_then(crate::convention::provider_failure_class_from_db);
        self.scheduled_at.timestamp() == expected.scheduled_at_unix
            && self.active_attempt_started_at.map(|at| at.timestamp())
                == expected.active_attempt_started_at_unix
            && self.updated_at.timestamp() == expected.updated_at_unix
            && action == expected.action
            && error_class == expected.error_class
            && serde_json::from_str::<serde_json::Value>(&self.policy_snapshot)
                .unwrap_or_else(|_| serde_json::json!({}))
                == expected.policy_snapshot
    }
}

pub(crate) async fn raw_policy<C: ConnectionTrait>(
    db: &C,
    id: &str,
    locked_token: Option<&str>,
) -> Result<Option<RawPolicy>> {
    let mut query = recovery_job::Entity::find_by_id(id.to_owned())
        .select_only()
        .column(recovery_job::Column::Action)
        .column(recovery_job::Column::ErrorClass)
        .column(recovery_job::Column::PolicySnapshot)
        .column(recovery_job::Column::ScheduledAt)
        .column(recovery_job::Column::ActiveAttemptStartedAt)
        .column(recovery_job::Column::UpdatedAt);
    if let Some(token) = locked_token {
        // ORM FOR UPDATE is a current read under InnoDB Repeatable Read, not
        // the earlier CLI-proof consistent snapshot. SQLite omits this clause
        // and already holds its serialized writer transaction after fence().
        query = query
            .filter(recovery_job::Column::ClaimToken.eq(token.to_owned()))
            .lock_exclusive();
    }
    Ok(query
        .into_tuple::<(
            String,
            Option<String>,
            String,
            DateTimeWithTimeZone,
            Option<DateTimeWithTimeZone>,
            DateTimeWithTimeZone,
        )>()
        .one(db)
        .await?
        .map(
            |(
                action,
                error_class,
                policy_snapshot,
                scheduled_at,
                active_attempt_started_at,
                updated_at,
            )| RawPolicy {
                action,
                error_class,
                policy_snapshot,
                scheduled_at,
                active_attempt_started_at,
                updated_at,
            },
        ))
}

pub(crate) async fn fence<C: ConnectionTrait>(
    db: &C,
    expected: &crate::RecoveryJobRecord,
    raw: &RawPolicy,
    observed_liveness: Option<&turn_liveness::Model>,
    token: &str,
) -> Result<bool> {
    // observe_activity changes the frontier only with a newer timestamp or
    // sequence. Matching both is its existing version fence. Causal-kind
    // classification stays in Rust, independent of backend text collations.
    let mut frontier = turn_liveness::Entity::find()
        .select_only()
        .column(turn_liveness::Column::TurnId)
        .filter(turn_liveness::Column::TurnId.eq(expected.turn_id.clone()));
    if let Some(observed) = observed_liveness {
        frontier = frontier
            .filter(turn_liveness::Column::LastActivitySequence.eq(observed.last_activity_sequence))
            .filter(turn_liveness::Column::LastActivityAt.eq(observed.last_activity_at));
    }
    let frontier = frontier.into_query();
    let mut update = recovery_job::Entity::update_many()
        // A transaction-local fence, always changed even with MySQL affected
        // row semantics. All success paths clear it before committing.
        .col_expr(
            recovery_job::Column::ClaimToken,
            Expr::value(Some(token.to_owned())),
        )
        .filter(recovery_job::Column::Id.eq(expected.id.clone()))
        .filter(recovery_job::Column::Status.eq("active"))
        .filter(recovery_job::Column::Action.eq(raw.action.clone()))
        .filter(recovery_job::Column::ScheduledAt.eq(raw.scheduled_at))
        .filter(recovery_job::Column::RunCount.eq(expected.run_count))
        .filter(recovery_job::Column::MaxAttempts.eq(expected.max_attempts))
        .filter(recovery_job::Column::PolicySnapshot.eq(raw.policy_snapshot.clone()))
        .filter(recovery_job::Column::TurnId.eq(expected.turn_id.clone()));
    update = if observed_liveness.is_some() {
        update.filter(recovery_job::Column::TurnId.in_subquery(frontier))
    } else {
        update.filter(recovery_job::Column::TurnId.not_in_subquery(frontier))
    };
    update = match &raw.error_class {
        Some(class) => update.filter(recovery_job::Column::ErrorClass.eq(class.clone())),
        None => update.filter(recovery_job::Column::ErrorClass.is_null()),
    };
    update = match &expected.active_attempt_id {
        Some(id) => update.filter(recovery_job::Column::ActiveAttemptId.eq(id.clone())),
        None => update.filter(recovery_job::Column::ActiveAttemptId.is_null()),
    };
    update = match raw.active_attempt_started_at {
        Some(at) => update.filter(recovery_job::Column::ActiveAttemptStartedAt.eq(at)),
        None => update
            .filter(recovery_job::Column::ActiveAttemptStartedAt.is_null())
            .filter(recovery_job::Column::UpdatedAt.eq(raw.updated_at)),
    };
    Ok(update.exec(db).await?.rows_affected > 0)
}

pub(crate) async fn active_upper<C: ConnectionTrait>(db: &C) -> Result<Option<String>> {
    use sea_orm::QueryOrder;
    Ok(recovery_job::Entity::find()
        .select_only()
        .column(recovery_job::Column::Id)
        .filter(recovery_job::Column::Status.eq("active"))
        .order_by_desc(recovery_job::Column::Id)
        .limit(1)
        .into_tuple::<String>()
        .one(db)
        .await?)
}

pub(crate) async fn active_page<C: ConnectionTrait>(
    db: &C,
    after: Option<&str>,
    upper: &str,
    limit: u64,
) -> Result<(Option<String>, Vec<Result<recovery_job::Model>>)> {
    use sea_orm::QueryOrder;
    let mut ids = recovery_job::Entity::find()
        .select_only()
        .column(recovery_job::Column::Id)
        .filter(recovery_job::Column::Status.eq("active"))
        .filter(recovery_job::Column::Id.lte(upper.to_owned()));
    if let Some(after) = after {
        ids = ids.filter(recovery_job::Column::Id.gt(after.to_owned()));
    }
    let ids = ids
        .order_by_asc(recovery_job::Column::Id)
        .limit(limit)
        .into_tuple::<String>()
        .all(db)
        .await?;
    // Cursor ownership follows raw keys, even when a row disappears, changes
    // status, or fails typed hydration. Each point read releases its reader
    // before the caller prepares a watchdog decision.
    let after = ids.last().cloned();
    let mut rows = Vec::with_capacity(ids.len());
    for id in ids {
        match recovery_job::Entity::find_by_id(id)
            .filter(recovery_job::Column::Status.eq("active"))
            .one(db)
            .await
        {
            Ok(Some(row)) => rows.push(Ok(row)),
            Ok(None) => {}
            Err(error) => rows.push(Err(error.into())),
        }
    }
    Ok((after, rows))
}
