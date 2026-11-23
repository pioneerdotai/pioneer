//! Current delivering work plus error-only recovery bookkeeping. Discovery
//! limits raw input before looking at retries; payloads are read by selected PK.
use anyhow::{Context, Result, bail};
use pioneer_entity::{task_delivery, task_delivery_attempt, task_delivery_recovery_retry as retry};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, FromQueryResult, QueryFilter,
    QueryOrder, QuerySelect, Set,
};

pub const DELIVERY_RECOVERY_BUDGET: u64 = 64;

const STUCK_DELIVERY_AGE_SECONDS: i64 = 300;

#[derive(Clone, Debug, PartialEq, Eq, FromQueryResult)]
pub struct DeliveryRecoveryCursor {
    pub id: String,
    pub updated_at: DateTimeWithTimeZone,
}

// Keep discovery to indexed keys and apply the retry policy after the bounded read.
fn source_query(
    cutoff: i64,
    after: Option<&DeliveryRecoveryCursor>,
    limit: u64,
) -> sea_orm::Select<task_delivery::Entity> {
    use task_delivery::Column as F;
    let cutoff = chrono::DateTime::from_timestamp(cutoff, 0)
        .unwrap_or_else(chrono::Utc::now)
        .fixed_offset();
    let mut query = task_delivery::Entity::find()
        .select_only()
        .columns([F::Id, F::UpdatedAt])
        .filter(F::Status.eq("delivering"))
        .filter(F::UpdatedAt.lte(cutoff));
    if let Some(cursor) = after {
        query = query.filter(Expr::tuple([Expr::col(F::UpdatedAt), Expr::col(F::Id)]).gt(
            Expr::tuple([Expr::val(cursor.updated_at), Expr::val(&cursor.id)]),
        ));
    }
    query
        .order_by_asc(F::UpdatedAt)
        .order_by_asc(F::Id)
        .limit(limit)
}

pub async fn source_page<C: ConnectionTrait>(
    db: &C,
    cutoff: i64,
    after: Option<&DeliveryRecoveryCursor>,
    limit: u64,
) -> Result<Vec<DeliveryRecoveryCursor>> {
    if limit == 0 || limit > DELIVERY_RECOVERY_BUDGET {
        bail!("invalid delivery recovery input budget");
    }
    source_query(cutoff, after, limit)
        .into_model::<DeliveryRecoveryCursor>()
        .all(db)
        .await
        .context("read delivery recovery page")
}

pub async fn due_page<C: ConnectionTrait>(
    db: &C,
    now: i64,
    limit: u64,
) -> Result<Vec<retry::Model>> {
    if limit > DELIVERY_RECOVERY_BUDGET {
        bail!("invalid delivery recovery retry budget");
    }
    if limit == 0 {
        return Ok(Vec::new());
    }
    Ok(due_query(now, limit).all(db).await?)
}

fn due_query(now: i64, limit: u64) -> sea_orm::Select<retry::Entity> {
    retry::Entity::find()
        .filter(retry::Column::NextProbeAt.lte(now))
        .order_by_asc(retry::Column::NextProbeAt)
        .order_by_asc(retry::Column::DeliveryId)
        .limit(limit)
}

#[derive(Clone, Debug)]
pub struct DeliveryRecoverySnapshot {
    pub delivery: task_delivery::Model,
    pub attempt: Option<task_delivery_attempt::Model>,
    pub retry: Option<retry::Model>,
    exact_snapshots: (Option<String>, Option<String>),
}

impl DeliveryRecoverySnapshot {
    pub fn eligible(&self, now: i64) -> bool {
        self.delivery.status == "delivering"
            && self.delivery.updated_at.timestamp()
                <= now.saturating_sub(STUCK_DELIVERY_AGE_SECONDS)
            && self.retry.as_ref().is_none_or(|r| r.next_probe_at <= now)
    }

    /// Decode/serialize only after reader resources have been released.
    pub fn failure_event(&self, now: i64) -> Result<pioneer_protocol::TaskEventPayload> {
        super::delivery::validate_durable_delivery(&self.delivery)?;
        let raw_attempt = self
            .attempt
            .clone()
            .context("stuck Task delivery has no exact attempt")?;
        super::delivery::validate_durable_attempt(&raw_attempt)?;
        let mut delivery = super::delivery::task_delivery_from_db_model(self.delivery.clone())?;
        let mut attempt = super::delivery::task_delivery_attempt_from_db_model(raw_attempt)?;
        let retryable = delivery.attempt_count < delivery.max_attempts;
        delivery.status = if retryable {
            pioneer_protocol::TaskDeliveryStatus::Pending
        } else {
            pioneer_protocol::TaskDeliveryStatus::Failed
        };
        delivery.next_attempt_at = retryable.then_some(now);
        delivery.last_error = Some("task_delivery_recovered".to_owned());
        delivery.updated_at = now;
        attempt.status = pioneer_protocol::TaskDeliveryAttemptStatus::Failed;
        attempt.completed_at = Some(now);
        attempt.error = delivery.last_error.clone();
        Ok(pioneer_protocol::TaskEventPayload::DeliveryFailed { delivery, attempt })
    }
}

pub async fn snapshot<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<DeliveryRecoverySnapshot>> {
    let Some(delivery) = super::delivery::find_delivery_by_id(db, id).await? else {
        return Ok(None);
    };
    // Raw i64, including malformed counts: never synthesize a missing attempt.
    let attempt = task_delivery_attempt::Entity::find()
        .filter(task_delivery_attempt::Column::DeliveryId.eq(id))
        .filter(task_delivery_attempt::Column::AttemptNumber.eq(delivery.attempt_count))
        .one(db)
        .await?;
    let retry = retry::Entity::find_by_id(id.to_owned()).one(db).await?;
    let exact_snapshots = task_delivery::Entity::find_by_id(id.to_owned())
        .select_only()
        .expr_as(
            Expr::col(task_delivery::Column::ResultSnapshotJson).cast_as(Alias::new("text")),
            "result_snapshot",
        )
        .expr_as(
            Expr::col(task_delivery::Column::ErrorSnapshotJson).cast_as(Alias::new("text")),
            "error_snapshot",
        )
        .into_tuple::<(Option<String>, Option<String>)>()
        .one(db)
        .await?
        .context("delivery disappeared while assembling its recovery snapshot")?;
    Ok(Some(DeliveryRecoverySnapshot {
        delivery,
        attempt,
        retry,
        exact_snapshots,
    }))
}

pub async fn matches(
    db: &DatabaseTransaction,
    expected: &DeliveryRecoverySnapshot,
) -> Result<bool> {
    if task_delivery::Entity::find_by_id(expected.delivery.id.clone())
        .select_only()
        .column(task_delivery::Column::Id)
        .lock_exclusive()
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_none()
    {
        return Ok(false);
    }
    let Some(current) = snapshot(db, &expected.delivery.id).await? else {
        return Ok(false);
    };
    // Full raw source facts, exact immutable attempt ID/verified absence, and
    // full observed retry snapshot. Same timestamps alone cannot authorize work.
    Ok(current.delivery == expected.delivery
        && current.attempt == expected.attempt
        && current.retry == expected.retry
        && current.exact_snapshots == expected.exact_snapshots)
}

pub fn retry_delay(attempts: i64) -> i64 {
    std::cmp::min(
        5_i64.saturating_mul(1_i64 << attempts.saturating_sub(1).clamp(0, 15)),
        300,
    )
}

pub async fn defer(
    db: &DatabaseTransaction,
    expected: &DeliveryRecoverySnapshot,
    now: i64,
    token: &str,
) -> Result<bool> {
    if expected.delivery.status != "delivering" || !matches(db, expected).await? {
        return Ok(false);
    }
    let attempts = expected
        .retry
        .as_ref()
        .map_or(1, |r| std::cmp::min(r.attempts.saturating_add(1), 16));
    let model = retry::ActiveModel {
        delivery_id: Set(expected.delivery.id.clone()),
        expected_attempt_id: Set(expected.attempt.as_ref().map(|a| a.id.clone())),
        expected_attempt_count: Set(expected.delivery.attempt_count),
        expected_updated_at: Set(expected.delivery.updated_at),
        retry_token: Set(token.to_owned()),
        next_probe_at: Set(now.saturating_add(retry_delay(attempts))),
        attempts: Set(attempts),
    };
    if expected.retry.is_some() {
        // Writer transaction already compared the full old snapshot.
        retry::Entity::update(model).exec(db).await?;
    } else {
        // Never overwrite a concurrent/new retry, including after delete/reinsert.
        retry::Entity::insert(model)
            .on_conflict(
                OnConflict::column(retry::Column::DeliveryId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(db)
            .await?;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_backoff_saturates_without_overflow() {
        assert_eq!(
            (1..=8).map(retry_delay).collect::<Vec<_>>(),
            [5, 10, 20, 40, 80, 160, 300, 300]
        );
        assert_eq!(retry_delay(i64::MAX), 300);
    }
    #[tokio::test]
    async fn recovery_plans_seek_source_and_due_indexes_without_sorting_history() -> Result<()> {
        use pioneer_postgres_test::with_database;
        use sea_orm::{DbBackend, QueryTrait, TransactionTrait};
        with_database(async |fixture| {
            let db = fixture.connection().begin().await?;
            // On a tiny fixture the planner may choose a sequential scan.
            // Check that the exact production statements can use both indexes
            // without an extra sort, using their original bound values.
            db.execute_unprepared(
                "SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off",
            )
            .await?;
            let after = DeliveryRecoveryCursor {
                id: "after".into(),
                updated_at: chrono::DateTime::from_timestamp(100, 0)
                    .unwrap()
                    .fixed_offset(),
            };
            for (mut statement, index) in [
                (
                    source_query(200, None, 64).build(DbBackend::Postgres),
                    "idx_task_delivery_recovery_source",
                ),
                (
                    source_query(200, Some(&after), 32).build(DbBackend::Postgres),
                    "idx_task_delivery_recovery_source",
                ),
                (
                    due_query(200, 32).build(DbBackend::Postgres),
                    "idx_task_delivery_recovery_retry_due",
                ),
            ] {
                statement.sql = format!("EXPLAIN {}", statement.sql);
                let plan = db
                    .query_all_raw(statement)
                    .await?
                    .into_iter()
                    .map(|row| row.try_get::<String>("", "QUERY PLAN"))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                assert!(
                    plan.iter()
                        .any(|line| line.contains(index) && line.contains("Index")),
                    "{plan:?}"
                );
                assert!(
                    plan.iter().any(|line| line.contains("Index Cond:")),
                    "{plan:?}"
                );
                assert!(!plan.iter().any(|line| line.contains("Sort")), "{plan:?}");
            }
            db.rollback().await?;
            Ok(())
        })
        .await
    }
}
