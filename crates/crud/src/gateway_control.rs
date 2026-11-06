//! Durable scheduling heads for the general control owner.
//!
//! Call after a complete bounded source pass. `None` means no future timed
//! obligation; it is not permission to discard an unfinished scan or an error.
//! The owner retains those separately and waits on gateway.control. Due queue
//! entries blocked by capacity/proof do not manufacture an immediate deadline.
use crate::{CrudStore, Result, unix_to_datetime};
use pioneer_entity::{
    agent_work_queue, native_terminal_effect_outbox, recovery_job, recovery_terminalization_outbox,
    skill_upload_session, turn_execution, turn_item_attempt,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, ExprTrait, Func};
use sea_orm::{ColumnTrait, Condition, EntityTrait, QueryFilter, QueryOrder, QuerySelect};

/// Adapter for exported repository operations that also serve raw-connection
/// fixtures. Production SQLite mutations use the runtime handle. Transaction
/// primitives cannot notify before commit; their CrudStore owner holds the guard.
pub trait GatewayControlConnection: sea_orm::ConnectionTrait {
    fn gateway_control_change(&self) -> Option<pioneer_sqlite::WorkChange>;
}
impl GatewayControlConnection for pioneer_sqlite::SqliteDatabase {
    fn gateway_control_change(&self) -> Option<pioneer_sqlite::WorkChange> {
        Some(self.work_signal("gateway.control").changing())
    }
}
impl GatewayControlConnection for sea_orm::DatabaseConnection {
    fn gateway_control_change(&self) -> Option<pioneer_sqlite::WorkChange> {
        None
    }
}
impl GatewayControlConnection for sea_orm::DatabaseTransaction {
    fn gateway_control_change(&self) -> Option<pioneer_sqlite::WorkChange> {
        None
    }
}

impl CrudStore {
    pub async fn control_turn_item_attempt_upper_id(&self) -> Result<Option<String>> {
        Ok(turn_item_attempt::Entity::find()
            .select_only()
            .column(turn_item_attempt::Column::Id)
            .order_by_desc(turn_item_attempt::Column::Id)
            .limit(1)
            .into_tuple::<String>()
            .one(&self.connection)
            .await?)
    }

    /// Snapshot membership ceilings once per pass. Eligibility is revalidated
    /// by every page and mutation; no snapshot or DB capacity is retained.
    pub async fn control_turn_execution_upper_id(&self) -> Result<Option<String>> {
        Ok(turn_execution::Entity::find()
            .select_only()
            .column(turn_execution::Column::TurnId)
            .order_by_desc(turn_execution::Column::TurnId)
            .limit(1)
            .into_tuple::<String>()
            .one(&self.connection)
            .await?)
    }

    pub async fn control_recovery_job_upper_id(&self) -> Result<Option<String>> {
        Ok(recovery_job::Entity::find()
            .select_only()
            .column(recovery_job::Column::Id)
            .order_by_desc(recovery_job::Column::Id)
            .limit(1)
            .into_tuple::<String>()
            .one(&self.connection)
            .await?)
    }

    pub async fn list_expired_foreign_turn_execution_page(
        &self,
        owner: &str,
        now: i64,
        after: Option<&str>,
        upper: &str,
        limit: u64,
    ) -> Result<(Vec<crate::TurnExecutionRecord>, Option<String>)> {
        let limit = limit.clamp(1, 512);
        let mut query = turn_execution::Entity::find()
            .filter(turn_execution::Column::OwnerId.ne(owner))
            .filter(turn_execution::Column::LeaseUntil.lte(unix_to_datetime(now)))
            .filter(turn_execution::Column::Status.is_in([
                "queued",
                "starting",
                "running",
                "recovering",
            ]))
            .filter(turn_execution::Column::TurnId.lte(upper));
        if let Some(after) = after {
            query = query.filter(turn_execution::Column::TurnId.gt(after));
        }
        let rows = query
            .order_by_asc(turn_execution::Column::TurnId)
            .limit(limit)
            .all(&self.connection)
            .await?;
        let next = if rows.len() as u64 == limit {
            rows.last()
                .filter(|row| row.turn_id != upper)
                .map(|row| row.turn_id.clone())
        } else {
            None
        };
        Ok((
            rows.into_iter()
                .map(crate::repositories::turn_execution::record_from_model)
                .collect::<Result<Vec<_>>>()?,
            next,
        ))
    }

    pub async fn list_due_terminal_repair_page(
        &self,
        now: i64,
        limit: u64,
        after: Option<&str>,
        upper: &str,
    ) -> Result<(Vec<crate::RecoveryJobRecord>, Option<String>)> {
        let limit = limit.clamp(1, 512);
        let at = unix_to_datetime(now);
        let mut query = recovery_job::Entity::find()
            .filter(recovery_job::Column::Id.lte(upper))
            .filter(recovery_job::Column::Status.eq("pending"))
            .filter(recovery_job::Column::Action.eq("mark_failed"))
            .filter(recovery_job::Column::NextRunAt.lte(at))
            .filter(
                Condition::any()
                    .add(recovery_job::Column::ClaimExpiresAt.is_null())
                    .add(recovery_job::Column::ClaimExpiresAt.lte(at)),
            );
        if let Some(after) = after {
            query = query.filter(recovery_job::Column::Id.gt(after));
        }
        let rows = query
            .order_by_asc(recovery_job::Column::Id)
            .limit(limit)
            .all(&self.connection)
            .await?;
        let next = if rows.len() as u64 == limit {
            rows.last()
                .filter(|row| row.id != upper)
                .map(|row| row.id.clone())
        } else {
            None
        };
        Ok((
            rows.into_iter()
                .map(crate::recovery_job_record_from_model)
                .collect(),
            next,
        ))
    }

    /// A finite orphan repair sweep must advance through idempotent rows even
    /// when enqueueing their already existing job emitted no domain event.
    pub async fn list_owned_recovering_turn_execution_page(
        &self,
        owner: &str,
        after: Option<&str>,
        upper: &str,
        limit: u64,
    ) -> Result<(Vec<crate::TurnExecutionRecord>, Option<String>)> {
        let limit = limit.clamp(1, 512);
        let mut query = turn_execution::Entity::find()
            .filter(turn_execution::Column::TurnId.lte(upper))
            .filter(turn_execution::Column::OwnerId.eq(owner))
            .filter(turn_execution::Column::Status.eq("recovering"));
        if let Some(after) = after {
            query = query.filter(turn_execution::Column::TurnId.gt(after));
        }
        let rows = query
            .order_by_asc(turn_execution::Column::TurnId)
            .limit(limit)
            .all(&self.connection)
            .await?;
        let next = if rows.len() as u64 == limit {
            rows.last()
                .filter(|row| row.turn_id != upper)
                .map(|row| row.turn_id.clone())
        } else {
            None
        };
        let records = rows
            .into_iter()
            .map(crate::repositories::turn_execution::record_from_model)
            .collect::<Result<Vec<_>>>()?;
        Ok((records, next))
    }

    /// Future heads after draining source work. Combine with the coordinator's
    /// next_supervision_deadline(now), next_skill_cleanup_deadline(now),
    /// CLI/RAM deadlines and retained failures.
    /// Full input quanta/cursors continue immediately, never by a safety timer.
    pub async fn next_gateway_control_deadline(&self, now: i64) -> Result<Option<i64>> {
        let db = &self.connection;
        let at = unix_to_datetime(now);
        let mut deadlines = Vec::new();
        for column in [
            turn_item_attempt::Column::LeaseExpiresAt,
            turn_item_attempt::Column::IdleDeadlineAt,
            turn_item_attempt::Column::HardDeadlineAt,
        ] {
            deadlines.extend(
                turn_item_attempt::Entity::find()
                    .select_only()
                    .column(column)
                    .filter(turn_item_attempt::Column::Status.eq("running"))
                    .filter(turn_item_attempt::Column::ExecutionClass.is_not_null())
                    .filter(column.gt(at))
                    .order_by_asc(column)
                    .limit(1)
                    .into_tuple::<Option<DateTimeWithTimeZone>>()
                    .one(db)
                    .await?
                    .flatten()
                    .map(crate::util::deadline_unix_seconds),
            );
        }
        deadlines.extend(
            agent_work_queue::Entity::find()
                .select_only()
                .column(agent_work_queue::Column::EligibleAt)
                .filter(agent_work_queue::Column::State.eq("queued"))
                .filter(agent_work_queue::Column::EligibleAt.gt(at))
                .order_by_asc(agent_work_queue::Column::EligibleAt)
                .limit(1)
                .into_tuple::<Option<DateTimeWithTimeZone>>()
                .one(db)
                .await?
                .flatten()
                .map(crate::util::deadline_unix_seconds),
        );
        deadlines.extend(
            turn_execution::Entity::find()
                .select_only()
                .column(turn_execution::Column::LeaseUntil)
                .filter(turn_execution::Column::Status.is_in([
                    "queued",
                    "starting",
                    "running",
                    "recovering",
                ]))
                .filter(turn_execution::Column::LeaseUntil.gt(at))
                .order_by_asc(turn_execution::Column::LeaseUntil)
                .limit(1)
                .into_tuple::<DateTimeWithTimeZone>()
                .one(db)
                .await?
                .map(crate::util::deadline_unix_seconds),
        );
        deadlines.extend(
            crate::repositories::agent_domain::next_agent_action_outbox_deadline(db, at).await?,
        );
        for (column, statuses) in [
            (
                recovery_terminalization_outbox::Column::NextRunAt,
                vec!["pending", "failed"],
            ),
            (
                recovery_terminalization_outbox::Column::ClaimExpiresAt,
                vec!["delivering"],
            ),
        ] {
            deadlines.extend(
                recovery_terminalization_outbox::Entity::find()
                    .select_only()
                    .column(column)
                    .filter(recovery_terminalization_outbox::Column::Status.is_in(statuses))
                    .filter(column.gt(at))
                    .order_by_asc(column)
                    .limit(1)
                    .into_tuple::<Option<DateTimeWithTimeZone>>()
                    .one(db)
                    .await?
                    .flatten()
                    .map(crate::util::deadline_unix_seconds),
            );
        }
        // Terminal-policy repair is independent of the ordinary due-job owner.
        let eligible: Expr = Expr::case(
            Expr::col(recovery_job::Column::ClaimExpiresAt)
                .gt(Expr::col(recovery_job::Column::NextRunAt)),
            Expr::col(recovery_job::Column::ClaimExpiresAt),
        )
        .finally(Expr::col(recovery_job::Column::NextRunAt))
        .into();
        deadlines.extend(
            recovery_job::Entity::find()
                .select_only()
                .expr(Func::min(eligible.clone()))
                .filter(recovery_job::Column::Status.eq("pending"))
                .filter(recovery_job::Column::Action.eq("mark_failed"))
                .filter(eligible.gt(at))
                .into_tuple::<Option<DateTimeWithTimeZone>>()
                .one(db)
                .await?
                .flatten()
                .map(crate::util::deadline_unix_seconds),
        );
        deadlines.extend(
            skill_upload_session::Entity::find()
                .select_only()
                .column(skill_upload_session::Column::ExpiresAtUnix)
                .filter(skill_upload_session::Column::Status.is_in(["receiving", "finalized"]))
                .filter(skill_upload_session::Column::ExpiresAtUnix.gt(now))
                .order_by_asc(skill_upload_session::Column::ExpiresAtUnix)
                .limit(1)
                .into_tuple::<i64>()
                .one(db)
                .await?,
        );
        deadlines.extend(
            self.next_native_terminal_effect_recovery_deadline(now)
                .await?,
        );
        deadlines.extend(
            crate::repositories::agent_domain::next_agent_ledger_retention_at(db, at).await?,
        );
        const RETENTION: i64 = 30 * 24 * 60 * 60;
        deadlines.extend(
            native_terminal_effect_outbox::Entity::find()
                .select_only()
                .column(native_terminal_effect_outbox::Column::CompletedAt)
                .filter(native_terminal_effect_outbox::Column::Status.is_in([
                    "succeeded",
                    "discarded",
                    "superseded",
                ]))
                .filter(
                    native_terminal_effect_outbox::Column::CompletedAt
                        .gt(unix_to_datetime(now.saturating_sub(RETENTION))),
                )
                .order_by_asc(native_terminal_effect_outbox::Column::CompletedAt)
                .limit(1)
                .into_tuple::<Option<DateTimeWithTimeZone>>()
                .one(db)
                .await?
                .flatten()
                .map(|value| crate::util::deadline_unix_seconds(value).saturating_add(RETENTION)),
        );
        Ok(deadlines.into_iter().min())
    }
}
