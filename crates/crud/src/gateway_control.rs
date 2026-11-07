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

/// Reasons carried by the existing coalesced control signal. Row identities
/// are advisory and bounded by WorkSignal; UNKNOWN/overflow means a full pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayControlWake {
    Turn(String),
    Activity(String),
    Request(String),
    Attempt(String),
    CommandTurn(String),
    Native(String, String),
    Thread(String, String),
    Cli,
    Commands,
    Buffers,
    Timeouts,
    Supervision,
    Resources,
    Actions,
    Skills,
    Ledger,
    Terminalization,
    NativeRetention,
}

impl GatewayControlWake {
    pub fn key(&self) -> String {
        match self {
            Self::Turn(id) => format!("turn:{id}"),
            Self::Activity(id) => format!("activity:{id}"),
            Self::Request(id) => format!("request:{id}"),
            Self::Attempt(id) => format!("attempt:{id}"),
            Self::CommandTurn(id) => format!("command-turn:{id}"),
            Self::Native(runtime, native) => format!(
                "native:{}",
                serde_json::to_string(&(runtime, native)).expect("string pair")
            ),
            Self::Thread(runtime, thread) => format!(
                "thread:{}",
                serde_json::to_string(&(runtime, thread)).expect("string pair")
            ),
            Self::Cli => "cli".into(),
            Self::Commands => "commands".into(),
            Self::Buffers => "buffers".into(),
            Self::Timeouts => "timeouts".into(),
            Self::Supervision => "supervision".into(),
            Self::Resources => "resources".into(),
            Self::Actions => "actions".into(),
            Self::Skills => "skills".into(),
            Self::Ledger => "ledger".into(),
            Self::Terminalization => "terminalization".into(),
            Self::NativeRetention => "native-retention".into(),
        }
    }

    pub fn from_key(key: &str) -> Option<Self> {
        if let Some(id) = key.strip_prefix("activity:") {
            return Some(Self::Activity(id.into()));
        }
        if let Some(id) = key.strip_prefix("turn:") {
            return Some(Self::Turn(id.into()));
        }
        if let Some(id) = key.strip_prefix("request:") {
            return Some(Self::Request(id.into()));
        }
        if let Some(id) = key.strip_prefix("attempt:") {
            return Some(Self::Attempt(id.into()));
        }
        if let Some(id) = key.strip_prefix("command-turn:") {
            return Some(Self::CommandTurn(id.into()));
        }
        if let Some(value) = key.strip_prefix("thread:") {
            let (runtime, thread) = serde_json::from_str(value).ok()?;
            return Some(Self::Thread(runtime, thread));
        }
        if let Some(value) = key.strip_prefix("native:") {
            let (runtime, native) = serde_json::from_str(value).ok()?;
            return Some(Self::Native(runtime, native));
        }
        Some(match key {
            "cli" => Self::Cli,
            "commands" => Self::Commands,
            "buffers" => Self::Buffers,
            "timeouts" => Self::Timeouts,
            "supervision" => Self::Supervision,
            "resources" => Self::Resources,
            "actions" => Self::Actions,
            "skills" => Self::Skills,
            "ledger" => Self::Ledger,
            "terminalization" => Self::Terminalization,
            "native-retention" => Self::NativeRetention,
            _ => return None,
        })
    }
}

pub(crate) fn event_wakes(events: &[crate::events::TurnEventPayload]) -> Vec<GatewayControlWake> {
    use crate::events::CanonicalTurnEventPayload::*;
    let mut wakes = Vec::new();
    for event in events {
        wakes.push(
            if matches!(
                event,
                ItemStarted(_)
                    | ItemCompleted(_)
                    | ItemUpdated(_)
                    | TurnExecutionWindowCheckpointed(_)
                    | TurnPermissionAudit(_)
            ) {
                GatewayControlWake::Activity(event.turn_id().into())
            } else {
                GatewayControlWake::Turn(event.turn_id().into())
            },
        );
        match event {
            TurnStarted(_) | TurnCompleted(_) | TurnFailed(_) | TurnBlocked(_) => {
                wakes.extend([
                    GatewayControlWake::Resources,
                    GatewayControlWake::Supervision,
                    GatewayControlWake::Terminalization,
                    GatewayControlWake::Actions,
                ]);
                if !matches!(event, TurnStarted(_)) {
                    wakes.extend([
                        GatewayControlWake::Ledger,
                        GatewayControlWake::NativeRetention,
                    ]);
                }
                wakes.push(GatewayControlWake::CommandTurn(event.turn_id().into()));
            }
            ItemStarted(notification) => {
                if notification.item.item_type() == pioneer_protocol::TurnItemType::CommandExecution
                {
                    wakes.push(GatewayControlWake::CommandTurn(event.turn_id().into()));
                }
            }
            ItemCompleted(notification) => {
                if notification.item.item_type() == pioneer_protocol::TurnItemType::CommandExecution
                {
                    wakes.push(GatewayControlWake::CommandTurn(event.turn_id().into()));
                }
            }
            ItemTimeoutDetected(_)
            | ItemRecoveryOpened(_)
            | ItemRecoveryAttached(_)
            | ItemRetryScheduled(_)
            | ItemRetryAttemptStarted(_)
            | ItemRecoverySucceeded(_)
            | ItemRecoveryExhausted(_)
            | ItemToolRetryScheduled(_)
            | ItemToolRetryResolved(_)
            | ItemToolRetryExhausted(_)
            | TurnExecutionWindowStarted(_)
            | TurnExecutionWindowExhausted(_)
            | TurnExecutionWindowContinued(_)
            | TurnExecutionWindowBlocked(_) => {
                wakes.extend([
                    GatewayControlWake::Supervision,
                    GatewayControlWake::Terminalization,
                ]);
            }
            ItemUpdated(_)
            | TurnMessageEdited(_)
            | TurnMessageDeleted(_)
            | TurnToolLoopBudgetExceeded(_)
            | TurnExecutionWindowCheckpointed(_)
            | TurnPermissionAudit(_) => {}
        }
    }
    wakes
}

impl CrudStore {
    /// Call before dispatch; complete only after commit and resource release.
    /// Other consumers of a multi-domain publisher retain their old protocol.
    pub(crate) fn control_work_change(
        &self,
        domain: &str,
        work: &[GatewayControlWake],
    ) -> pioneer_sqlite::WorkChange {
        let change = self.connection.work_signal(domain).changing();
        if matches!(domain, "gateway.control" | "gateway.cli") {
            change.with_keys(&work.iter().map(GatewayControlWake::key).collect::<Vec<_>>())
        } else {
            change
        }
    }
}

/// Adapter for exported repository operations that also serve raw-connection
/// fixtures. Production SQLite mutations use the runtime handle. Transaction
/// primitives cannot notify before commit; their CrudStore owner holds the guard.
pub trait GatewayControlConnection: sea_orm::ConnectionTrait {
    fn gateway_control_change(&self) -> Option<pioneer_sqlite::WorkChange>;
    fn gateway_control_change_for(
        &self,
        work: &[GatewayControlWake],
    ) -> Option<pioneer_sqlite::WorkChange> {
        let keys = work.iter().map(GatewayControlWake::key).collect::<Vec<_>>();
        self.gateway_control_change()
            .map(|change| change.with_keys(&keys))
    }
}
impl GatewayControlConnection for pioneer_sqlite::SqliteDatabase {
    fn gateway_control_change(&self) -> Option<pioneer_sqlite::WorkChange> {
        Some(self.work_signal("gateway.control").changing().with_keys(&[
            GatewayControlWake::Resources.key(),
            GatewayControlWake::Actions.key(),
        ]))
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
    ) -> Result<(Vec<Result<crate::TurnExecutionRecord>>, Option<String>)> {
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
                .collect(),
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
    ) -> Result<(Vec<Result<crate::TurnExecutionRecord>>, Option<String>)> {
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
            .collect();
        Ok((records, next))
    }

    /// Future heads after draining source work. Combine with the coordinator's
    /// next_supervision_deadline(now), next_skill_cleanup_deadline(now),
    /// CLI/RAM deadlines and retained failures.
    /// Full input quanta/cursors continue immediately, never by a safety timer.
    pub async fn next_gateway_control_deadline(&self, now: i64) -> Result<Option<i64>> {
        let mut next = self
            .next_native_terminal_effect_recovery_deadline(now)
            .await?;
        for scope in [
            GatewayControlWake::Timeouts,
            GatewayControlWake::Resources,
            GatewayControlWake::Supervision,
            GatewayControlWake::Actions,
            GatewayControlWake::Terminalization,
            GatewayControlWake::Skills,
            GatewayControlWake::Ledger,
            GatewayControlWake::NativeRetention,
        ] {
            next = next
                .into_iter()
                .chain(self.next_gateway_control_deadline_for(&scope, now).await?)
                .min();
        }
        Ok(next)
    }

    /// Query only the heads belonging to an invalidated or due control family.
    pub async fn next_gateway_control_deadline_for(
        &self,
        scope: &GatewayControlWake,
        now: i64,
    ) -> Result<Option<i64>> {
        let db = &self.connection;
        let at = unix_to_datetime(now);
        let mut deadlines = Vec::new();
        if matches!(scope, GatewayControlWake::Timeouts) {
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
        }
        if matches!(scope, GatewayControlWake::Resources) {
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
        }
        if matches!(scope, GatewayControlWake::Supervision) {
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
        }
        if matches!(scope, GatewayControlWake::Actions) {
            deadlines.extend(
                crate::repositories::agent_domain::next_agent_action_outbox_deadline(db, at)
                    .await?,
            );
        }
        if matches!(scope, GatewayControlWake::Terminalization) {
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
        }
        if matches!(scope, GatewayControlWake::Terminalization) {
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
        }
        if matches!(scope, GatewayControlWake::Skills) {
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
        }
        if matches!(scope, GatewayControlWake::Ledger) {
            deadlines.extend(
                crate::repositories::agent_domain::next_agent_ledger_retention_at(db, at).await?,
            );
        }
        if matches!(scope, GatewayControlWake::NativeRetention) {
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
                    .map(|value| {
                        crate::util::deadline_unix_seconds(value).saturating_add(RETENTION)
                    }),
            );
        }
        Ok(deadlines.into_iter().min())
    }
}
