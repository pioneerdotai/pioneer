use anyhow::{Context, Result};
use pioneer_entity::recovery_job;
use pioneer_protocol::{
    ProviderFailureClass, ProviderFailureStage, RecoveryAction, RecoveryJobStatus, RecoveryTrigger,
    TurnItemType, generate_id,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, ExprTrait, PaginatorTrait, QueryFilter,
    QueryOrder, QuerySelect, Set,
};

use crate::convention::{
    provider_failure_class_to_db, provider_failure_stage_to_db, recovery_action_to_db,
    recovery_job_status_to_db, recovery_trigger_to_db, turn_item_type_to_db,
};

const DB_ID_LEN: usize = 21;
const CLAIM_TOKEN_LEN: usize = 21;

/// Recovery job owns these domain counters; run_count remains total attempts.
/// The only wire owner is semanticProgress in its existing diagnostic (512 B,
/// enclosing diagnostic4096 B). Missing legacy state is Unknown, never a stall.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryProgressState {
    pub schema_version: u32,
    pub baseline: Option<[u64; 3]>,
    pub consecutive_no_progress: u32,
    pub consecutive_unconfirmed: u32,
    pub last_outcome: Option<pioneer_protocol::ExecutionProgressSummary>,
}

impl Default for RecoveryProgressState {
    fn default() -> Self {
        Self {
            schema_version: 1,
            baseline: None,
            consecutive_no_progress: 0,
            consecutive_unconfirmed: 0,
            last_outcome: None,
        }
    }
}

impl RecoveryProgressState {
    pub fn valid(&self) -> bool {
        self.schema_version == 1
            && self.consecutive_no_progress <= self.consecutive_unconfirmed
            && self.consecutive_unconfirmed <= 4096
            && self.last_outcome.is_none_or(|summary| summary.valid())
    }

    pub(crate) fn start(&mut self, frontier: Option<[u64; 3]>) {
        self.baseline = frontier;
    }

    pub(crate) fn observe_outcome(&mut self, frontier: Option<[u64; 3]>) {
        self.last_outcome = self
            .baseline
            .zip(frontier)
            .map(
                |(baseline, frontier)| pioneer_protocol::ExecutionProgressSummary {
                    schema_version: 1,
                    baseline,
                    frontier,
                },
            )
            .filter(pioneer_protocol::ExecutionProgressSummary::valid);
        match self.last_outcome.map_or(
            pioneer_protocol::ExecutionProgressKind::Unknown,
            |summary| summary.kind(),
        ) {
            pioneer_protocol::ExecutionProgressKind::Advanced => {
                self.consecutive_no_progress = 0;
                self.consecutive_unconfirmed = 0;
            }
            pioneer_protocol::ExecutionProgressKind::Unchanged => {
                self.consecutive_no_progress =
                    std::cmp::min(self.consecutive_no_progress.saturating_add(1), 4096);
                self.consecutive_unconfirmed =
                    std::cmp::min(self.consecutive_unconfirmed.saturating_add(1), 4096);
            }
            pioneer_protocol::ExecutionProgressKind::Unknown => {
                self.consecutive_unconfirmed =
                    std::cmp::min(self.consecutive_unconfirmed.saturating_add(1), 4096);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryProgressLoad {
    LegacyUnknown,
    Available(RecoveryProgressState),
    Unavailable,
}

#[derive(Debug, Clone, Copy)]
pub struct RecoveryProgressUnavailable;
impl std::fmt::Display for RecoveryProgressUnavailable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("recovery semantic progress state unavailable")
    }
}
impl std::error::Error for RecoveryProgressUnavailable {}

/// Safe public diagnostics keep their original strict shape; private policy
/// state is decoded separately and is never reconstructed from failure prose.
pub(crate) fn decode_stored_diagnostic(
    wire: Option<&str>,
) -> Result<(
    pioneer_protocol::RecoveryDiagnostic,
    Option<RecoveryProgressState>,
)> {
    let Some(wire) = wire else {
        return Ok((Default::default(), None));
    };
    anyhow::ensure!(wire.len() <= 4096, RecoveryProgressUnavailable);
    let mut value: serde_json::Value =
        serde_json::from_str(wire).map_err(|_| RecoveryProgressUnavailable)?;
    let object = value.as_object_mut().ok_or(RecoveryProgressUnavailable)?;
    let progress = object
        .remove("semanticProgress")
        .map(|value| -> Result<RecoveryProgressState> {
            anyhow::ensure!(
                value.as_object().is_some_and(|fields| [
                    "schema_version",
                    "baseline",
                    "consecutive_no_progress",
                    "consecutive_unconfirmed",
                    "last_outcome"
                ]
                .iter()
                .all(|name| fields.contains_key(*name))),
                RecoveryProgressUnavailable,
            );
            anyhow::ensure!(
                serde_json::to_vec(&value)?.len() <= 512,
                RecoveryProgressUnavailable
            );
            let progress: RecoveryProgressState =
                serde_json::from_value(value).map_err(|_| RecoveryProgressUnavailable)?;
            anyhow::ensure!(progress.valid(), RecoveryProgressUnavailable);
            Ok(progress)
        })
        .transpose()?;
    let diagnostic = serde_json::from_value(value).map_err(|_| RecoveryProgressUnavailable)?;
    Ok((diagnostic, progress))
}

/// Public typed failure facts remain readable even when private progress is
/// incompatible. This projection grants no progress or retry authorization.
pub(crate) fn decode_public_diagnostic(wire: &str) -> Result<pioneer_protocol::RecoveryDiagnostic> {
    anyhow::ensure!(wire.len() <= 4096, RecoveryProgressUnavailable);
    let mut value: serde_json::Value =
        serde_json::from_str(wire).map_err(|_| RecoveryProgressUnavailable)?;
    value
        .as_object_mut()
        .ok_or(RecoveryProgressUnavailable)?
        .remove("semanticProgress");
    serde_json::from_value(value).map_err(|_| RecoveryProgressUnavailable.into())
}

pub(crate) fn encode_stored_diagnostic(
    diagnostic: &pioneer_protocol::RecoveryDiagnostic,
    progress: &RecoveryProgressState,
) -> Result<String> {
    anyhow::ensure!(progress.valid(), RecoveryProgressUnavailable);
    let progress = serde_json::to_value(progress)?;
    anyhow::ensure!(
        serde_json::to_vec(&progress)?.len() <= 512,
        RecoveryProgressUnavailable
    );
    let mut value = serde_json::to_value(diagnostic)?;
    value
        .as_object_mut()
        .context("safe recovery diagnostic shape")?
        .insert("semanticProgress".to_owned(), progress);
    let wire = serde_json::to_string(&value)?;
    anyhow::ensure!(wire.len() <= 4096, RecoveryProgressUnavailable);
    Ok(wire)
}

/// Immutable reader preparation. Commit checks the exact job/checkpoint
/// predecessors before applying the original atomic owner transition.
#[derive(Debug, Clone)]
pub struct PreparedRecoveryProgress {
    pub(crate) state: RecoveryProgressState,
    pub(crate) diagnostic: pioneer_protocol::RecoveryDiagnostic,
    pub(crate) job_id: String,
    pub(crate) active_attempt_id: Option<String>,
    pub(crate) claim_token: Option<String>,
    pub(crate) status: String,
    pub(crate) revalidation: sea_orm::Statement,
}

impl PreparedRecoveryProgress {
    pub fn state(&self) -> &RecoveryProgressState {
        &self.state
    }

    pub(crate) fn encoded_diagnostic(
        &self,
        override_value: Option<&pioneer_protocol::RecoveryDiagnostic>,
    ) -> Result<String> {
        encode_stored_diagnostic(override_value.unwrap_or(&self.diagnostic), &self.state)
    }

    pub(crate) async fn current<C: ConnectionTrait>(&self, db: &C) -> Result<bool> {
        let row = db
            .query_one_raw(self.revalidation.clone())
            .await?
            .context("recovery progress predecessor proof unavailable")?;
        Ok(row.try_get::<i64>("", "current")? == 1)
    }
}

/// Call only with the reader scope outside a writer/transaction. Each fetched
/// frame has a physical byte bound; JSON preparation follows returned rows.
pub(crate) async fn prepare_progress<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    outcome: bool,
) -> Result<Option<PreparedRecoveryProgress>> {
    let Some(row) = db.query_one_raw(sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT turn_id, status, run_count, active_attempt_id, claim_token, \
          CASE WHEN length(CAST(diagnostic AS BLOB)) <= 4096 THEN diagnostic ELSE NULL END AS diagnostic, \
          coalesce(length(CAST(diagnostic AS BLOB)),0) AS diagnostic_bytes \
          FROM recovery_job WHERE id=?",
        [job_id.to_owned().into()],
    )).await? else { return Ok(None); };
    let turn_id: String = row.try_get("", "turn_id")?;
    let status: String = row.try_get("", "status")?;
    let run_count: i64 = row.try_get("", "run_count")?;
    let active_attempt_id: Option<String> = row.try_get("", "active_attempt_id")?;
    let claim_token: Option<String> = row.try_get("", "claim_token")?;
    anyhow::ensure!(
        row.try_get::<i64>("", "diagnostic_bytes")? <= 4096,
        RecoveryProgressUnavailable
    );
    let previous_diagnostic: Option<String> = row.try_get("", "diagnostic")?;
    let (diagnostic, stored) = decode_stored_diagnostic(previous_diagnostic.as_deref())?;
    let mut state = stored.unwrap_or_default();
    let checkpoint_id = super::turn_execution_window::semantic_checkpoint_id(&turn_id);
    let checkpoint = db.query_one_raw(sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT c.turn_id, c.workspace_id, c.thread_id, c.checkpoint_kind, \
          t.thread_id AS owner_thread, th.workspace_id AS owner_workspace, \
          CASE WHEN length(CAST(c.payload_json AS BLOB)) <= 131072 THEN c.payload_json ELSE NULL END AS payload_json, \
          length(CAST(c.payload_json AS BLOB)) AS payload_bytes \
          FROM turn_execution_checkpoint c LEFT JOIN \"turn\" t ON t.id=c.turn_id \
          LEFT JOIN thread th ON th.id=t.thread_id WHERE c.id=?",
        [checkpoint_id.clone().into()],
    )).await?;
    let previous_checkpoint = checkpoint
        .as_ref()
        .map(|row| -> Result<String> {
            anyhow::ensure!(
                row.try_get::<i64>("", "payload_bytes")? <= 131072
                    && row.try_get::<String>("", "turn_id")? == turn_id
                    && row.try_get::<String>("", "checkpoint_kind")? == "semantic_state"
                    && row
                        .try_get::<Option<String>>("", "owner_thread")?
                        .as_deref()
                        == Some(row.try_get::<String>("", "thread_id")?.as_str())
                    && row
                        .try_get::<Option<String>>("", "owner_workspace")?
                        .as_deref()
                        == Some(row.try_get::<String>("", "workspace_id")?.as_str()),
                RecoveryProgressUnavailable
            );
            row.try_get("", "payload_json").map_err(Into::into)
        })
        .transpose()?;
    let frontier = if let (Some(wire), Some(row)) =
        (previous_checkpoint.as_deref(), checkpoint.as_ref())
    {
        let payload: pioneer_protocol::ExecutionCheckpointPayload =
            serde_json::from_str(wire).map_err(|_| RecoveryProgressUnavailable)?;
        anyhow::ensure!(
            payload.schema_version == pioneer_protocol::EXECUTION_CHECKPOINT_PAYLOAD_SCHEMA_VERSION
                && payload.turn_id == turn_id
                && payload.workspace_id == row.try_get::<String>("", "workspace_id")?
                && payload.thread_id == row.try_get::<String>("", "thread_id")?,
            RecoveryProgressUnavailable
        );
        let summary = payload.window.progress.filter(|summary| summary.valid());
        let native = payload
            .native_semantic_state
            .as_ref()
            .ok_or(RecoveryProgressUnavailable)?;
        anyhow::ensure!(
            native["schema_version"].as_u64()
                == Some(u64::from(
                    pioneer_protocol::NATIVE_SEMANTIC_STATE_SCHEMA_VERSION
                )),
            RecoveryProgressUnavailable
        );
        let wait = native
            .get("attached_task_wait")
            .ok_or(RecoveryProgressUnavailable)?;
        if !wait.is_null() {
            anyhow::ensure!(
                wait.as_object().is_some_and(|fields| fields.len() == 6)
                    && wait["baseline"]
                        .as_array()
                        .is_some_and(|bytes| bytes.len() == 32
                            && bytes
                                .iter()
                                .all(|byte| byte.as_u64().is_some_and(|value| value <= 255)))
                    && wait["item_id"]
                        .as_str()
                        .is_some_and(|id| !id.is_empty() && id.len() <= 21)
                    && wait["checkpoint_id"]
                        .as_str()
                        .is_some_and(|id| id.len() == 21),
                RecoveryProgressUnavailable
            );
            let wait: pioneer_protocol::NativeAttachedTaskWait =
                serde_json::from_value(wait.clone()).map_err(|_| RecoveryProgressUnavailable)?;
            anyhow::ensure!(
                wait.valid()
                    && wait.checkpoint_id
                        == pioneer_protocol::native_semantic_checkpoint_id(&turn_id),
                RecoveryProgressUnavailable
            );
        }
        let semantic = &native["progress_frontier"];
        anyhow::ensure!(
            semantic["schema_version"].as_u64() == Some(1),
            RecoveryProgressUnavailable
        );
        let counts = [
            "advanced_observations",
            "unchanged_observations",
            "unknown_observations",
        ]
        .map(|field| semantic[field].as_u64());
        match (summary, counts) {
            (Some(summary), [Some(a), Some(u), Some(unknown)])
                if summary.frontier == [a, u, unknown] =>
            {
                Some(summary.frontier)
            }
            _ => None, // Legacy/missing domain evidence stays Unknown.
        }
    } else {
        None
    };
    if outcome {
        state.observe_outcome(frontier);
    } else {
        state.start(frontier);
    }
    let revalidation = sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT EXISTS(SELECT 1 FROM recovery_job j WHERE j.id=?1 AND j.turn_id=?2 \
          AND j.status=?3 AND j.run_count=?4 AND j.active_attempt_id IS ?5 AND j.claim_token IS ?6 \
          AND j.diagnostic IS ?7 AND ((?9 IS NULL AND NOT EXISTS(SELECT 1 FROM turn_execution_checkpoint WHERE id=?8)) \
          OR EXISTS(SELECT 1 FROM turn_execution_checkpoint c JOIN \"turn\" t ON t.id=c.turn_id \
            JOIN thread th ON th.id=t.thread_id WHERE c.id=?8 AND c.turn_id=?2 \
            AND c.thread_id=?10 AND c.workspace_id=?11 AND t.thread_id=c.thread_id \
            AND th.workspace_id=c.workspace_id AND c.checkpoint_kind='semantic_state' \
            AND c.payload_json=?9))) AS current",
        [
            job_id.to_owned().into(),
            turn_id.into(),
            status.clone().into(),
            run_count.into(),
            active_attempt_id.clone().into(),
            claim_token.clone().into(),
            previous_diagnostic.into(),
            checkpoint_id.into(),
            previous_checkpoint.into(),
            checkpoint
                .as_ref()
                .map(|row| row.try_get::<String>("", "thread_id"))
                .transpose()?
                .into(),
            checkpoint
                .as_ref()
                .map(|row| row.try_get::<String>("", "workspace_id"))
                .transpose()?
                .into(),
        ],
    );
    Ok(Some(PreparedRecoveryProgress {
        state,
        diagnostic,
        job_id: job_id.to_owned(),
        active_attempt_id,
        claim_token,
        status,
        revalidation,
    }))
}

fn bounded_job_query() -> sea_orm::Select<recovery_job::Entity> {
    use recovery_job::Column as C;
    recovery_job::Entity::find()
        .select_only()
        .columns([
            C::Id,
            C::TurnId,
            C::ItemId,
            C::ItemType,
            C::SourceAttemptId,
            C::Status,
            C::Trigger,
            C::Action,
            C::Reason,
            C::Policy,
            C::ErrorClass,
            C::TransportStage,
            C::RetryAfterMs,
            C::ProviderAttemptNumber,
            C::PolicySnapshot,
            C::RunCount,
            C::MaxAttempts,
            C::LastError,
            C::LastFailureAttemptId,
            C::ScheduledAt,
            C::NextRunAt,
            C::ClaimToken,
            C::ClaimedAt,
            C::ClaimExpiresAt,
            C::ActiveAttemptId,
            C::ActiveAttemptStartedAt,
            C::ResolutionPending,
            C::CreatedAt,
            C::UpdatedAt,
        ])
        .column_as(
            sea_orm::sea_query::Expr::cust(
                "CASE WHEN diagnostic IS NULL OR length(CAST(diagnostic AS BLOB)) <= 4096 \
          THEN diagnostic ELSE '{\"semanticProgress\":{\"schema_version\":0}}' END",
            ),
            C::Diagnostic,
        )
}

fn resolution_remains_pending(status: RecoveryJobStatus) -> bool {
    matches!(
        status,
        RecoveryJobStatus::Pending
            | RecoveryJobStatus::Active
            | RecoveryJobStatus::Failed
            | RecoveryJobStatus::Exhausted
            | RecoveryJobStatus::Blocked
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimedJobActivation {
    Activated,
    BlockedByActiveRecovery,
    ClaimNotFound,
}

#[derive(Debug, Clone)]
pub struct NewRecoveryJob {
    pub diagnostic_json: Option<String>,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub source_attempt_id: Option<String>,
    pub trigger: RecoveryTrigger,
    pub action: RecoveryAction,
    pub reason: Option<String>,
    pub policy_json: String,
    pub error_class: Option<ProviderFailureClass>,
    pub transport_stage: Option<ProviderFailureStage>,
    pub retry_after_ms: Option<i64>,
    pub provider_attempt_number: i64,
    pub policy_snapshot_json: String,
    pub max_attempts: i64,
    pub created_at: DateTimeWithTimeZone,
    pub scheduled_at: DateTimeWithTimeZone,
    pub next_run_at: DateTimeWithTimeZone,
}

pub async fn enqueue_recovery_job<C: ConnectionTrait>(
    db: &C,
    job: NewRecoveryJob,
) -> Result<recovery_job::Model> {
    let id = generate_id(DB_ID_LEN);

    recovery_job::Entity::insert(recovery_job::ActiveModel {
        id: Set(id.clone()),
        turn_id: Set(job.turn_id),
        item_id: Set(job.item_id),
        item_type: Set(turn_item_type_to_db(job.item_type).to_owned()),
        source_attempt_id: Set(job.source_attempt_id),
        status: Set(recovery_job_status_to_db(RecoveryJobStatus::Pending).to_owned()),
        trigger: Set(recovery_trigger_to_db(job.trigger).to_owned()),
        action: Set(recovery_action_to_db(job.action).to_owned()),
        reason: Set(job.reason),
        policy: Set(job.policy_json),
        error_class: Set(job
            .error_class
            .map(provider_failure_class_to_db)
            .map(str::to_owned)),
        transport_stage: Set(job
            .transport_stage
            .map(provider_failure_stage_to_db)
            .map(str::to_owned)),
        retry_after_ms: Set(job.retry_after_ms),
        provider_attempt_number: Set(job.provider_attempt_number),
        policy_snapshot: Set(job.policy_snapshot_json),
        run_count: Set(0),
        max_attempts: Set(job.max_attempts),
        last_error: Set(None),
        diagnostic: Set(job.diagnostic_json),
        last_failure_attempt_id: Set(None),
        scheduled_at: Set(job.scheduled_at),
        next_run_at: Set(job.next_run_at),
        claim_token: Set(None),
        claimed_at: Set(None),
        claim_expires_at: Set(None),
        active_attempt_id: Set(None),
        active_attempt_started_at: Set(None),
        resolution_pending: Set(true),
        created_at: Set(job.created_at),
        updated_at: Set(job.created_at),
    })
    .exec(db)
    .await
    .context("failed to insert recovery job")?;

    bounded_job_query()
        .filter(recovery_job::Column::Id.eq(id))
        .one(db)
        .await
        .context("failed to reload recovery job row")?
        .context("recovery job row missing after insert")
}

pub async fn count_jobs_for_turn<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<u64> {
    recovery_job::Entity::find()
        .filter(recovery_job::Column::TurnId.eq(turn_id.to_owned()))
        .count(db)
        .await
        .with_context(|| format!("failed to count recovery jobs for turn `{turn_id}`"))
}

pub async fn claim_due_jobs<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    claim_expires_at: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<recovery_job::Model>> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);

    let candidates = bounded_job_query()
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::NextRunAt.lte(now))
        .filter(
            Condition::any()
                .add(recovery_job::Column::ClaimExpiresAt.is_null())
                .add(recovery_job::Column::ClaimExpiresAt.lte(now)),
        )
        .order_by_asc(recovery_job::Column::NextRunAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to load due recovery jobs")?;

    let mut claimed = Vec::new();

    for candidate in candidates {
        let claim_token = generate_id(CLAIM_TOKEN_LEN);
        let claimed_now = recovery_job::Entity::update_many()
            .col_expr(
                recovery_job::Column::ClaimToken,
                sea_orm::sea_query::Expr::value(Some(claim_token.clone())),
            )
            .col_expr(
                recovery_job::Column::ClaimedAt,
                sea_orm::sea_query::Expr::value(Some(now)),
            )
            .col_expr(
                recovery_job::Column::ClaimExpiresAt,
                sea_orm::sea_query::Expr::value(Some(claim_expires_at)),
            )
            .col_expr(
                recovery_job::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(now),
            )
            .filter(recovery_job::Column::Id.eq(candidate.id.clone()))
            .filter(recovery_job::Column::Status.eq(pending))
            .filter(
                Condition::any()
                    .add(recovery_job::Column::ClaimExpiresAt.is_null())
                    .add(recovery_job::Column::ClaimExpiresAt.lte(now)),
            )
            .exec(db)
            .await
            .with_context(|| format!("failed to claim recovery job `{}`", candidate.id))?
            .rows_affected
            > 0;

        if !claimed_now {
            continue;
        }

        if let Some(job) = bounded_job_query()
            .filter(recovery_job::Column::Id.eq(candidate.id.clone()))
            .one(db)
            .await
            .with_context(|| format!("failed to reload claimed recovery job `{}`", candidate.id))?
        {
            claimed.push(job);
        }
    }

    Ok(claimed)
}

pub async fn list_due_pending_jobs_by_action<C: ConnectionTrait>(
    db: &C,
    action: RecoveryAction,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<recovery_job::Model>> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);
    let action_db = recovery_action_to_db(action);

    bounded_job_query()
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::Action.eq(action_db))
        .filter(recovery_job::Column::NextRunAt.lte(now))
        .filter(
            Condition::any()
                .add(recovery_job::Column::ClaimExpiresAt.is_null())
                .add(recovery_job::Column::ClaimExpiresAt.lte(now)),
        )
        .order_by_asc(recovery_job::Column::NextRunAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to load due pending recovery jobs for action `{}`",
                recovery_action_to_db(action)
            )
        })
}

pub async fn find_blocked_job_by_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    job_id: Option<&str>,
) -> Result<Option<recovery_job::Model>> {
    let blocked = recovery_job_status_to_db(RecoveryJobStatus::Blocked);
    let mut query = bounded_job_query()
        .filter(recovery_job::Column::TurnId.eq(turn_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(blocked));
    if let Some(job_id) = job_id {
        query = query.filter(recovery_job::Column::Id.eq(job_id.to_owned()));
    }
    query
        .order_by_desc(recovery_job::Column::UpdatedAt)
        .one(db)
        .await
        .with_context(|| format!("failed to find blocked recovery job for turn `{turn_id}`"))
}

pub async fn resume_blocked_job<C: ConnectionTrait>(
    db: &C,
    job: &recovery_job::Model,
    action: RecoveryAction,
    max_attempts: i64,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let blocked = recovery_job_status_to_db(RecoveryJobStatus::Blocked);
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);
    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(pending.to_owned()),
        )
        .col_expr(
            recovery_job::Column::Action,
            sea_orm::sea_query::Expr::value(recovery_action_to_db(action).to_owned()),
        )
        .col_expr(
            recovery_job::Column::MaxAttempts,
            sea_orm::sea_query::Expr::value(max_attempts),
        )
        .col_expr(
            recovery_job::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            recovery_job::Column::LastFailureAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_job::Column::Id.eq(job.id.clone()))
        .filter(recovery_job::Column::Status.eq(blocked))
        .filter(recovery_job::Column::UpdatedAt.eq(job.updated_at))
        .exec(db)
        .await
        .with_context(|| format!("failed to resume blocked recovery job `{}`", job.id))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_job_retrying<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    active_attempt_id: &str,
    next_run_at: DateTimeWithTimeZone,
    budget_origin_after_cooldown: Option<DateTimeWithTimeZone>,
    last_error: Option<String>,
    diagnostic_json: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);
    let active = recovery_job_status_to_db(RecoveryJobStatus::Active);

    let mut update = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(pending.to_owned()),
        )
        .col_expr(
            recovery_job::Column::RunCount,
            sea_orm::sea_query::Expr::col(recovery_job::Column::RunCount).add(1),
        )
        .col_expr(
            recovery_job::Column::ProviderAttemptNumber,
            sea_orm::sea_query::Expr::col(recovery_job::Column::ProviderAttemptNumber).add(1),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error.clone()),
        )
        .col_expr(
            recovery_job::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(next_run_at),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        );
    // Fence replay to the last applied failure transition, even when a runtime
    // start error supplies no new provider facts.
    update = update.col_expr(
        recovery_job::Column::LastFailureAttemptId,
        sea_orm::sea_query::Expr::value(Some(active_attempt_id.to_owned())),
    );
    if let Some(value) = diagnostic_json {
        update = update.col_expr(
            recovery_job::Column::Diagnostic,
            sea_orm::sea_query::Expr::value(Some(value)),
        );
    }
    if let Some(budget_origin_after_cooldown) = budget_origin_after_cooldown {
        // Provider-declared cooldown is not execution time. Shift the budget
        // origin by exactly the cooldown while preserving time already spent
        // in active recovery attempts.
        update = update.col_expr(
            recovery_job::Column::ScheduledAt,
            sea_orm::sea_query::Expr::value(budget_origin_after_cooldown),
        );
    }
    let affected = update
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(active))
        .filter(recovery_job::Column::ActiveAttemptId.eq(active_attempt_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark recovery job `{job_id}` retrying"))?
        .rows_affected
        > 0;

    Ok(affected)
}

/// Returns an active observation attempt to the pending queue without
/// consuming a recovery attempt. A temporarily unavailable authority or
/// runtime is not evidence that the recovered execution made no progress.
pub async fn defer_active_job<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    active_attempt_id: &str,
    next_run_at: DateTimeWithTimeZone,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);
    let active = recovery_job_status_to_db(RecoveryJobStatus::Active);

    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(pending.to_owned()),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error),
        )
        .col_expr(
            recovery_job::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(next_run_at),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(active))
        .filter(recovery_job::Column::ActiveAttemptId.eq(active_attempt_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to defer active recovery job `{job_id}`"))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_claimed_job_retrying<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    next_run_at: DateTimeWithTimeZone,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);

    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(pending.to_owned()),
        )
        .col_expr(
            recovery_job::Column::RunCount,
            sea_orm::sea_query::Expr::col(recovery_job::Column::RunCount).add(1),
        )
        .col_expr(
            recovery_job::Column::ProviderAttemptNumber,
            sea_orm::sea_query::Expr::col(recovery_job::Column::ProviderAttemptNumber).add(1),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error),
        )
        .col_expr(
            recovery_job::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(next_run_at),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark claimed recovery job `{job_id}` retrying"))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_claimed_job_active<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    active_attempt_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<ClaimedJobActivation> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);
    let active = recovery_job_status_to_db(RecoveryJobStatus::Active);

    let Some(job) = bounded_job_query()
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::ClaimToken.eq(claim_token.to_owned()))
        .one(db)
        .await
        .with_context(|| format!("failed to load claimed recovery job `{job_id}`"))?
    else {
        return Ok(ClaimedJobActivation::ClaimNotFound);
    };

    let active_for_turn = bounded_job_query()
        .filter(recovery_job::Column::TurnId.eq(job.turn_id.clone()))
        .filter(recovery_job::Column::Status.eq(active))
        .filter(recovery_job::Column::Id.ne(job_id.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to check active recovery job for turn `{}`",
                job.turn_id
            )
        })?
        .is_some();

    if active_for_turn {
        return Ok(ClaimedJobActivation::BlockedByActiveRecovery);
    }

    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(active.to_owned()),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Some(active_attempt_id.to_owned())),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Some(now)),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark claimed recovery job `{job_id}` active"))?
        .rows_affected
        > 0;

    if affected {
        Ok(ClaimedJobActivation::Activated)
    } else {
        Ok(ClaimedJobActivation::ClaimNotFound)
    }
}

pub async fn release_claimed_job<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    next_run_at: DateTimeWithTimeZone,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);

    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error),
        )
        .col_expr(
            recovery_job::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(next_run_at),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to release claimed recovery job `{job_id}`"))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_due_pending_job_terminal_if_turn_idle<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    action: RecoveryAction,
    status: RecoveryJobStatus,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);
    let active = recovery_job_status_to_db(RecoveryJobStatus::Active);
    let action_db = recovery_action_to_db(action);
    let status_db = recovery_job_status_to_db(status).to_owned();
    let resolution_pending = resolution_remains_pending(status);

    let claim_is_available = || {
        Condition::any()
            .add(recovery_job::Column::ClaimExpiresAt.is_null())
            .add(recovery_job::Column::ClaimExpiresAt.lte(now))
    };

    let Some(job) = bounded_job_query()
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::Action.eq(action_db))
        .filter(recovery_job::Column::NextRunAt.lte(now))
        .filter(claim_is_available())
        .one(db)
        .await
        .with_context(|| format!("failed to load due pending recovery job `{job_id}`"))?
    else {
        return Ok(false);
    };

    let active_for_turn = bounded_job_query()
        .filter(recovery_job::Column::TurnId.eq(job.turn_id))
        .filter(recovery_job::Column::Status.eq(active))
        .filter(recovery_job::Column::Id.ne(job_id.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!("failed to check active recovery jobs before terminalizing `{job_id}`")
        })?;
    if active_for_turn.is_some() {
        return Ok(false);
    }

    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(status_db),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            recovery_job::Column::ResolutionPending,
            sea_orm::sea_query::Expr::value(resolution_pending),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::Action.eq(action_db))
        .filter(recovery_job::Column::NextRunAt.lte(now))
        .filter(claim_is_available())
        .exec(db)
        .await
        .with_context(|| format!("failed to mark due pending recovery job `{job_id}` terminal"))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_claimed_job_terminal<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    status: RecoveryJobStatus,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);
    let status_db = recovery_job_status_to_db(status).to_owned();
    let resolution_pending = resolution_remains_pending(status);
    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(status_db),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            recovery_job::Column::ResolutionPending,
            sea_orm::sea_query::Expr::value(resolution_pending),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark claimed recovery job `{job_id}` terminal"))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_job_terminal<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    status: RecoveryJobStatus,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let status_db = recovery_job_status_to_db(status).to_owned();
    let resolution_pending = resolution_remains_pending(status);

    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(status_db),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ResolutionPending,
            sea_orm::sea_query::Expr::value(resolution_pending),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark recovery job `{job_id}` terminal"))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_active_without_attempt_terminal<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    status: RecoveryJobStatus,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let status_db = recovery_job_status_to_db(status).to_owned();
    let active = recovery_job_status_to_db(RecoveryJobStatus::Active);
    let resolution_pending = resolution_remains_pending(status);

    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(status_db),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ResolutionPending,
            sea_orm::sea_query::Expr::value(resolution_pending),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(active))
        .filter(recovery_job::Column::ActiveAttemptId.is_null())
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to mark malformed active recovery job `{job_id}` terminal")
        })?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_job_terminal_after_attempt<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    active_attempt_id: &str,
    status: RecoveryJobStatus,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let status_db = recovery_job_status_to_db(status).to_owned();
    let resolution_pending = resolution_remains_pending(status);
    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(status_db),
        )
        .col_expr(
            recovery_job::Column::RunCount,
            sea_orm::sea_query::Expr::col(recovery_job::Column::RunCount).add(1),
        )
        .col_expr(
            recovery_job::Column::ProviderAttemptNumber,
            sea_orm::sea_query::Expr::col(recovery_job::Column::ProviderAttemptNumber).add(1),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error),
        )
        .col_expr(
            recovery_job::Column::LastFailureAttemptId,
            sea_orm::sea_query::Expr::value(Some(active_attempt_id.to_owned())),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ResolutionPending,
            sea_orm::sea_query::Expr::value(resolution_pending),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(
            recovery_job::Column::Status.eq(recovery_job_status_to_db(RecoveryJobStatus::Active)),
        )
        .filter(recovery_job::Column::ActiveAttemptId.eq(active_attempt_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark recovery job `{job_id}` terminal after attempt"))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn find_job_by_id<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
) -> Result<Option<recovery_job::Model>> {
    bounded_job_query()
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .one(db)
        .await
        .with_context(|| format!("failed to load recovery job `{job_id}`"))
}

pub async fn find_open_jobs_by_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<recovery_job::Model>> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending).to_owned();
    let active = recovery_job_status_to_db(RecoveryJobStatus::Active).to_owned();

    bounded_job_query()
        .filter(recovery_job::Column::TurnId.eq(turn_id.to_owned()))
        .filter(recovery_job::Column::Status.is_in([pending, active]))
        .order_by_asc(recovery_job::Column::ScheduledAt)
        .all(db)
        .await
        .with_context(|| format!("failed to load open recovery jobs for turn `{turn_id}`"))
}

/// Returns the single durable recovery episode which still owns resolution of
/// this Turn. Unlike `find_open_jobs_by_turn`, terminal jobs remain unresolved
/// until their terminalization outbox is delivered, superseded, or
/// quarantined.
pub async fn find_unresolved_job_by_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<recovery_job::Model>> {
    bounded_job_query()
        .filter(recovery_job::Column::TurnId.eq(turn_id.to_owned()))
        .filter(recovery_job::Column::ResolutionPending.eq(true))
        .order_by_asc(recovery_job::Column::CreatedAt)
        .one(db)
        .await
        .with_context(|| format!("failed to load unresolved recovery for turn `{turn_id}`"))
}

pub async fn cancel_open_jobs_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    exclude_job_id: Option<&str>,
    reason: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<Vec<recovery_job::Model>> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending).to_owned();
    let active = recovery_job_status_to_db(RecoveryJobStatus::Active).to_owned();
    let cancelled = recovery_job_status_to_db(RecoveryJobStatus::Cancelled).to_owned();

    let mut query = bounded_job_query()
        .filter(recovery_job::Column::TurnId.eq(turn_id.to_owned()))
        .filter(recovery_job::Column::Status.is_in([pending.clone(), active.clone()]));
    if let Some(excluded) = exclude_job_id {
        query = query.filter(recovery_job::Column::Id.ne(excluded.to_owned()));
    }

    let jobs = query
        .all(db)
        .await
        .with_context(|| format!("failed to load open recovery jobs for turn `{turn_id}`"))?;

    if jobs.is_empty() {
        return Ok(Vec::new());
    }

    let ids = jobs.iter().map(|job| job.id.clone()).collect::<Vec<_>>();
    recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(cancelled),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(reason),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            recovery_job::Column::ResolutionPending,
            sea_orm::sea_query::Expr::value(false),
        )
        .filter(recovery_job::Column::Id.is_in(ids))
        .exec(db)
        .await
        .with_context(|| format!("failed to cancel open recovery jobs for turn `{turn_id}`"))?;

    Ok(jobs)
}

pub async fn find_jobs_by_turn_and_status<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    status: RecoveryJobStatus,
) -> Result<Vec<recovery_job::Model>> {
    bounded_job_query()
        .filter(recovery_job::Column::TurnId.eq(turn_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(recovery_job_status_to_db(status)))
        .all(db)
        .await
        .with_context(|| {
            format!("failed to load recovery jobs for turn `{turn_id}` with status `{status:?}`")
        })
}

pub async fn list_active_jobs<C: ConnectionTrait>(
    db: &C,
    limit: u64,
) -> Result<Vec<recovery_job::Model>> {
    bounded_job_query()
        .filter(
            recovery_job::Column::Status.eq(recovery_job_status_to_db(RecoveryJobStatus::Active)),
        )
        .order_by_asc(recovery_job::Column::ActiveAttemptStartedAt)
        .order_by_asc(recovery_job::Column::UpdatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to load active recovery jobs")
}

/// Called only inside the transaction of a successful fenced transition.
pub async fn set_diagnostic<C: ConnectionTrait>(db: &C, job_id: &str, value: String) -> Result<()> {
    recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Diagnostic,
            sea_orm::sea_query::Expr::cust_with_values(
                "CASE WHEN json_type(?, '$.semanticProgress') IS NOT NULL THEN ? \
                  WHEN diagnostic IS NULL THEN ? \
                  WHEN length(CAST(diagnostic AS BLOB)) > 4096 THEN json_set(?, '$.semanticProgress', json('{\"schema_version\":0}')) \
                  WHEN NOT json_valid(diagnostic) THEN json_set(?, '$.semanticProgress', json('{\"schema_version\":0}')) \
                  WHEN json_type(diagnostic, '$.semanticProgress') IS NOT NULL \
                  THEN json_set(?, '$.semanticProgress', json_extract(diagnostic, '$.semanticProgress')) \
                  ELSE ? END",
                [value.clone(), value.clone(), value.clone(), value.clone(), value.clone(), value.clone(), value],
            ),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .exec(db)
        .await
        .context("failed to persist recovery diagnostic")?;
    Ok(())
}

/// A new untyped blocked transition cannot inherit a stop reason from a prior
/// terminalization (for example, a resumed job). Keep its last provider facts.
/// Called only after a successful fenced transition in the same transaction.
pub async fn clear_diagnostic_stop_reason<C: ConnectionTrait>(db: &C, job_id: &str) -> Result<()> {
    recovery_job::Entity::update_many()
        .col_expr(recovery_job::Column::Diagnostic,
            sea_orm::sea_query::Expr::cust("CASE WHEN diagnostic IS NULL THEN NULL WHEN length(CAST(diagnostic AS BLOB)) > 4096 THEN '{\"semanticProgress\":{\"schema_version\":0}}' WHEN NOT json_valid(diagnostic) THEN '{\"semanticProgress\":{\"schema_version\":0}}' ELSE json_remove(diagnostic, '$.stopReason') END"))
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .exec(db).await.context("failed to clear stale recovery stop reason")?;
    Ok(())
}

#[cfg(test)]
mod progress_tests {
    use super::*;

    #[test]
    fn recovery_progress_preserves_stall_through_unknown_and_restores_exact_baseline() {
        let mut state = RecoveryProgressState::default();
        state.start(Some([1, 0, 0]));
        state.observe_outcome(Some([1, 1, 0]));
        assert_eq!(state.consecutive_no_progress, 1);
        assert_eq!(state.consecutive_unconfirmed, 1);
        let wire = encode_stored_diagnostic(&Default::default(), &state).unwrap();
        let (safe, restored) = decode_stored_diagnostic(Some(&wire)).unwrap();
        assert_eq!(safe, pioneer_protocol::RecoveryDiagnostic::default());
        let mut state = restored.unwrap();
        assert_eq!(state.baseline, Some([1, 0, 0]));
        state.start(Some([1, 1, 0]));
        state.observe_outcome(Some([1, 1, 3]));
        assert_eq!(
            state.consecutive_no_progress, 1,
            "Unknown cannot clear known stall"
        );
        assert_eq!(state.consecutive_unconfirmed, 2);
        state.start(Some([1, 1, 3]));
        state.observe_outcome(Some([1, 1, 3]));
        assert_eq!(
            state.consecutive_no_progress, 1,
            "lease/heartbeat without a domain observation is Unknown"
        );
        assert_eq!(state.consecutive_unconfirmed, 3);
        state.start(Some([1, 1, 3]));
        state.observe_outcome(Some([2, 1, 4]));
        assert_eq!(state.consecutive_no_progress, 0);
        assert_eq!(state.consecutive_unconfirmed, 0);
        assert_eq!(
            state.last_outcome.unwrap().kind(),
            pioneer_protocol::ExecutionProgressKind::Advanced
        );
        state.start(None);
        state.observe_outcome(Some([3, 1, 4]));
        assert_eq!(state.consecutive_no_progress, 0);
        assert_eq!(
            state.consecutive_unconfirmed, 1,
            "no admitted baseline cannot attest advancement"
        );
        let mut malformed = serde_json::from_str::<serde_json::Value>(&wire).unwrap();
        malformed["semanticProgress"]["schema_version"] = 99.into();
        assert!(decode_stored_diagnostic(Some(&malformed.to_string())).is_err());
        malformed["semanticProgress"]["schema_version"] = 1.into();
        malformed["semanticProgress"]
            .as_object_mut()
            .unwrap()
            .remove("baseline");
        assert!(decode_stored_diagnostic(Some(&malformed.to_string())).is_err());
        assert!(decode_stored_diagnostic(Some(&" ".repeat(4097))).is_err());
        assert_eq!(decode_stored_diagnostic(None).unwrap().1, None);
        assert_eq!(decode_stored_diagnostic(Some("{}")).unwrap().1, None);
    }

    #[tokio::test]
    async fn recovery_progress_cas_fences_checkpoint_drift_and_duplicate_outcome() {
        use super::super::turn_execution_window::{
            NewTurnExecutionCheckpointRecord, NewTurnExecutionWindowRecord,
            TurnExecutionCheckpointKind,
        };
        use migration::{Migrator, MigratorTrait};
        let connection = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&connection, None).await.unwrap();
        let store = crate::CrudStore::new(connection).with_maintenance_access();
        // The claim API accepts whole Unix seconds; schedule at that same precision.
        let now = chrono::DateTime::from_timestamp(1_792_000_000, 0)
            .unwrap()
            .fixed_offset();
        store.run_serialized_write(|| async {
            for sql in [
                "INSERT INTO workspace (id,name,is_active,is_current,created_at,updated_at) VALUES ('ws_progress_cas','test',1,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
                "INSERT INTO thread (id,workspace_id,preview,mode,model,model_provider,status,origin_kind,sidebar_visibility,created_at,updated_at,access_class) VALUES ('thread_progress_cas','ws_progress_cas','','agent','fixture','fixture','active','user','visible',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'private')",
                "INSERT INTO \"turn\" (id,thread_id,status,prompt_manifest_json,created_at,updated_at,turn_kind,origin,mentions_json,message_revision,work_owner) VALUES ('turn_progress_cas','thread_progress_cas','in_progress','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'conversation','user','[]',0,'turn')",
            ] {
                store.connection.execute_raw(sea_orm::Statement::from_string(
                    sea_orm::DatabaseBackend::Sqlite, sql)).await?;
            }
            Ok(())
        }).await.unwrap();
        let job_input = NewRecoveryJob {
            diagnostic_json: None,
            turn_id: "turn_progress_cas".into(),
            item_id: "item_progress_cas".into(),
            item_type: TurnItemType::Reasoning,
            source_attempt_id: None,
            trigger: RecoveryTrigger::RuntimeFailure,
            action: RecoveryAction::RestartTurn,
            reason: None,
            policy_json: "{}".into(),
            error_class: None,
            transport_stage: None,
            retry_after_ms: None,
            provider_attempt_number: 1,
            policy_snapshot_json: "{}".into(),
            max_attempts: 10,
            created_at: now,
            scheduled_at: now,
            next_run_at: now,
        };
        let job = store
            .run_serialized_write(|| async {
                enqueue_recovery_job(&store.connection, job_input.clone()).await
            })
            .await
            .unwrap();
        let window = store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: "ws_progress_cas".into(),
                    thread_id: "thread_progress_cas".into(),
                    turn_id: job.turn_id.clone(),
                    window_index: 1,
                    status: pioneer_protocol::ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 0,
                    tool_call_count: 0,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({}),
                    started_at: now,
                },
                now,
                now,
            )
            .await
            .unwrap();
        let checkpoint_id =
            super::super::turn_execution_window::semantic_checkpoint_id(&job.turn_id);
        let checkpoint_payload = |counts: [u64; 3]| {
            serde_json::json!({
                "schema_version":pioneer_protocol::EXECUTION_CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
                "workspace_id":"ws_progress_cas", "thread_id":"thread_progress_cas", "turn_id":job.turn_id,
                "original_request":{"input_count":0,"text_truncated":false,"attachment_count":0},
                "provider_budget":{"agent_round_count":0,"tool_call_count":0,"provider_usage_available":false},
                "tools":{"requested_count":0,"executed_count":0,"unexecuted_count":0,"total_count":0,"succeeded_count":0,"failed_count":0,"in_progress_count":0,"detail_limit":1,"details_truncated":false},
                "window":{"window_index":1,"agent_round_count":0,"tool_call_count":0,"progress":pioneer_protocol::ExecutionProgressSummary { schema_version: 1, baseline: [0;3], frontier: counts }},
                "native_semantic_state":{"schema_version":pioneer_protocol::NATIVE_SEMANTIC_STATE_SCHEMA_VERSION,"attached_task_wait":null,"progress_frontier":{
                    "schema_version":1,"advanced_observations":counts[0], "unchanged_observations":counts[1], "unknown_observations":counts[2]}}
            })
        };
        store
            .save_turn_execution_checkpoint(NewTurnExecutionCheckpointRecord {
                id: Some(checkpoint_id.clone()),
                window_id: window.id,
                workspace_id: "ws_progress_cas".into(),
                thread_id: "thread_progress_cas".into(),
                turn_id: job.turn_id.clone(),
                checkpoint_kind: TurnExecutionCheckpointKind::SemanticState,
                payload_json: checkpoint_payload([1, 0, 0]),
                created_at: now,
            })
            .await
            .unwrap();
        let now_unix = now.timestamp();
        let claim = store
            .claim_due_recovery_jobs(now_unix, 30, 1)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(
            store
                .mark_claimed_recovery_job_active(
                    &job.id,
                    claim.claim_token.as_deref().unwrap(),
                    "attempt_cas",
                    now_unix
                )
                .await
                .unwrap(),
            crate::ClaimedRecoveryActivation::Activated
        );
        assert!(
            store
                .prepare_recovery_progress_outcome(&job.id, "wrong_attempt")
                .await
                .unwrap()
                .is_none()
        );
        // Codec8 requires explicit null or a supported host wait; incomplete new
        // state must not grant recovery progress authority on cold restore.
        for wait in [None, Some(serde_json::json!({"schema_version":2}))] {
            let mut malformed = checkpoint_payload([1, 0, 0]);
            if let Some(wait) = wait {
                malformed["native_semantic_state"]["attached_task_wait"] = wait;
            } else {
                malformed["native_semantic_state"]
                    .as_object_mut()
                    .unwrap()
                    .remove("attached_task_wait");
            }
            let malformed = malformed.to_string();
            store
                .run_serialized_write(|| async {
                    store
                        .connection
                        .execute_raw(sea_orm::Statement::from_sql_and_values(
                            sea_orm::DatabaseBackend::Sqlite,
                            "UPDATE turn_execution_checkpoint SET payload_json=? WHERE id=?",
                            [malformed.clone().into(), checkpoint_id.clone().into()],
                        ))
                        .await
                        .map_err(Into::into)
                })
                .await
                .unwrap();
            let error = store
                .prepare_recovery_progress_outcome(&job.id, "attempt_cas")
                .await
                .unwrap_err();
            assert!(error.is::<RecoveryProgressUnavailable>());
        }
        let supported = checkpoint_payload([1, 0, 0]).to_string();
        store
            .run_serialized_write(|| async {
                store
                    .connection
                    .execute_raw(sea_orm::Statement::from_sql_and_values(
                        sea_orm::DatabaseBackend::Sqlite,
                        "UPDATE turn_execution_checkpoint SET payload_json=? WHERE id=?",
                        [supported.clone().into(), checkpoint_id.clone().into()],
                    ))
                    .await
                    .map_err(Into::into)
            })
            .await
            .unwrap();
        let prior = store
            .prepare_recovery_progress_outcome(&job.id, "attempt_cas")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(prior.state().consecutive_no_progress, 0);
        assert_eq!(prior.state().consecutive_unconfirmed, 1);
        let advanced_json = checkpoint_payload([2, 0, 0]).to_string();
        store
            .run_serialized_write(|| async {
                store
                    .connection
                    .execute_raw(sea_orm::Statement::from_sql_and_values(
                        sea_orm::DatabaseBackend::Sqlite,
                        "UPDATE turn_execution_checkpoint SET payload_json=? WHERE id=?",
                        [advanced_json.clone().into(), checkpoint_id.clone().into()],
                    ))
                    .await
                    .map_err(Into::into)
            })
            .await
            .unwrap();
        assert!(
            !store
                .mark_recovery_job_retrying_with_diagnostic_and_progress(
                    &job.id,
                    "attempt_cas",
                    now_unix + 1,
                    None,
                    None,
                    None,
                    now_unix,
                    None,
                    Some(&prior)
                )
                .await
                .unwrap(),
            "stale prepared Unknown must not spend after real advancement"
        );
        let unchanged_owner = store.get_recovery_job(&job.id).await.unwrap().unwrap();
        assert_eq!(unchanged_owner.run_count, 0);
        assert_eq!(unchanged_owner.status, RecoveryJobStatus::Active);
        let advanced = store
            .prepare_recovery_progress_outcome(&job.id, "attempt_cas")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            advanced.state().last_outcome.unwrap().kind(),
            pioneer_protocol::ExecutionProgressKind::Advanced
        );
        assert!(
            store
                .mark_recovery_job_retrying_with_diagnostic_and_progress(
                    &job.id,
                    "attempt_cas",
                    now_unix + 1,
                    None,
                    None,
                    None,
                    now_unix,
                    None,
                    Some(&advanced)
                )
                .await
                .unwrap()
        );
        assert!(
            !store
                .mark_recovery_job_retrying_with_diagnostic_and_progress(
                    &job.id,
                    "attempt_cas",
                    now_unix + 1,
                    None,
                    None,
                    None,
                    now_unix,
                    None,
                    Some(&advanced)
                )
                .await
                .unwrap(),
            "duplicate outcome cannot spend twice"
        );
        let cold = crate::CrudStore::new(store.connection.clone()).with_maintenance_access();
        let saved = cold.get_recovery_job(&job.id).await.unwrap().unwrap();
        assert_eq!(
            saved.run_count, 1,
            "real progress never resets actual total attempts"
        );
        let crate::RecoveryProgressLoad::Available(state) = saved.progress else {
            panic!("durable state expected");
        };
        assert_eq!(state.consecutive_no_progress, 0);
        assert_eq!(state.consecutive_unconfirmed, 0);
        // Exercise real admission/outcome commits and a fresh facade between
        // attempts. Echo/lease-only deltas preserve known stalls; actual causal
        // advancement resets consecutive counts while total attempts grow.
        for (index, counts, expected_stalls, expected_unknown) in [
            (2, [2, 1, 0], 1, 1),
            (3, [2, 2, 0], 2, 2),
            (4, [2, 2, 1], 2, 3),
            (5, [3, 2, 1], 0, 0),
        ] {
            let current = cold.get_recovery_job(&job.id).await.unwrap().unwrap();
            let claim = cold
                .claim_due_recovery_jobs(current.next_run_at_unix, 30, 1)
                .await
                .unwrap()
                .remove(0);
            let attempt = format!("attempt_cas_{index}");
            assert_eq!(
                cold.mark_claimed_recovery_job_active(
                    &job.id,
                    claim.claim_token.as_deref().unwrap(),
                    &attempt,
                    current.next_run_at_unix
                )
                .await
                .unwrap(),
                crate::ClaimedRecoveryActivation::Activated
            );
            let wire = checkpoint_payload(counts).to_string();
            cold.run_serialized_write(|| async {
                cold.connection
                    .execute_raw(sea_orm::Statement::from_sql_and_values(
                        sea_orm::DatabaseBackend::Sqlite,
                        "UPDATE turn_execution_checkpoint SET payload_json=? WHERE id=?",
                        [wire.clone().into(), checkpoint_id.clone().into()],
                    ))
                    .await?;
                Ok(())
            })
            .await
            .unwrap();
            assert!(
                cold.mark_recovery_job_retrying(
                    &job.id,
                    &attempt,
                    current.next_run_at_unix + 1,
                    None,
                    None,
                    current.next_run_at_unix
                )
                .await
                .unwrap()
            );
            let restored = crate::CrudStore::new(cold.connection.clone())
                .with_maintenance_access()
                .get_recovery_job(&job.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(restored.run_count, index);
            let crate::RecoveryProgressLoad::Available(state) = restored.progress else {
                panic!("state");
            };
            assert_eq!(state.consecutive_no_progress, expected_stalls);
            assert_eq!(state.consecutive_unconfirmed, expected_unknown);
        }
        // A checkpoint whose own row and payload agree on a foreign scope is
        // still invalid against the canonical Turn owner.
        cold.run_serialized_write(|| async {
            cold.connection.execute_raw(sea_orm::Statement::from_sql_and_values(
                sea_orm::DatabaseBackend::Sqlite,
                "UPDATE turn_execution_checkpoint SET workspace_id='foreign', payload_json=json_set(payload_json,'$.workspace_id','foreign') WHERE id=?",
                [checkpoint_id.clone().into()])).await?;
            Ok(())
        }).await.unwrap();
        assert!(
            prepare_progress(&cold.connection, &job.id, false)
                .await
                .unwrap_err()
                .is::<RecoveryProgressUnavailable>()
        );
        cold.run_serialized_write(|| async {
            cold.connection.execute_raw(sea_orm::Statement::from_sql_and_values(
                sea_orm::DatabaseBackend::Sqlite,
                "UPDATE turn_execution_checkpoint SET workspace_id='ws_progress_cas', payload_json=json_set(payload_json,'$.workspace_id','ws_progress_cas','$.schema_version',99) WHERE id=?",
                [checkpoint_id.clone().into()])).await?;
            Ok(())
        }).await.unwrap();
        assert!(
            prepare_progress(&cold.connection, &job.id, false)
                .await
                .unwrap_err()
                .is::<RecoveryProgressUnavailable>()
        );
        assert_eq!(
            cold.database_connection().read_class(),
            pioneer_sqlite::SqliteReadClass::Maintenance
        );
    }
}
