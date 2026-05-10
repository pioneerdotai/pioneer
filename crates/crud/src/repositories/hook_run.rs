use anyhow::{Context, Result, bail, ensure};
use chrono::Duration as ChronoDuration;
use pioneer_entity::{hook_audit_event, hook_run, hook_run_attempt};
use pioneer_hooks::{
    HookActor, HookActorKind, HookAgentId, HookAuditEventKind, HookContext, HookContextMode,
    HookContributionHash, HookDiagnosticCode, HookDiagnosticMessage, HookDiagnosticPreview, HookId,
    HookIdError, HookMetadata, HookPhase, HookRecoveryScan, HookRetrySchedule, HookRunAttemptId,
    HookRunErrorSummary, HookRunId, HookRunIdempotencyKey, HookRunResumeState, HookRunScopeId,
    HookRunStatus, HookSubscriptionId, HookTaskId, HookThreadId, HookTurnId, HookValue,
    HookWorkspaceId,
};
use pioneer_protocol::generate_id;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use crate::convention::{DB_ID_LEN, hook_run_status_from_db, hook_run_status_to_db};
use crate::util::unix_ms_to_datetime;

pub const HOOK_RUN_IDEMPOTENCY_KEY_MAX_CHARS: usize = 255;
pub const HOOK_RUN_DIAGNOSTIC_PREVIEW_MAX_COUNT: usize = 40;
pub const HOOK_RUN_DIAGNOSTIC_MESSAGE_MAX_CHARS: usize = 512;
pub const HOOK_RUN_ERROR_MESSAGE_MAX_CHARS: usize = 512;
pub const HOOK_RUN_CONTRIBUTION_HASH_MAX_COUNT: usize = 80;

const REDACTED_DIAGNOSTIC_MESSAGE: &str = "diagnostic redacted";
const REDACTED_ERROR_MESSAGE: &str = "hook error redacted";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HookRunScopeKind {
    Workspace,
    Thread,
    Turn,
    Task,
    Agent,
    Hook,
    Custom(String),
}

impl HookRunScopeKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Workspace => "workspace",
            Self::Thread => "thread",
            Self::Turn => "turn",
            Self::Task => "task",
            Self::Agent => "agent",
            Self::Hook => "hook",
            Self::Custom(kind) => kind.as_str(),
        }
    }
}

impl From<&str> for HookRunScopeKind {
    fn from(value: &str) -> Self {
        match value {
            "workspace" => Self::Workspace,
            "thread" => Self::Thread,
            "turn" => Self::Turn,
            "task" => Self::Task,
            "agent" => Self::Agent,
            "hook" => Self::Hook,
            other => Self::Custom(other.to_owned()),
        }
    }
}

impl From<String> for HookRunScopeKind {
    fn from(value: String) -> Self {
        match value.as_str() {
            "workspace" => Self::Workspace,
            "thread" => Self::Thread,
            "turn" => Self::Turn,
            "task" => Self::Task,
            "agent" => Self::Agent,
            "hook" => Self::Hook,
            _ => Self::Custom(value),
        }
    }
}

impl fmt::Display for HookRunScopeKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookRunScope {
    pub kind: HookRunScopeKind,
    pub id: HookRunScopeId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewHookRunRecord {
    pub id: Option<HookRunId>,
    pub idempotency_key: HookRunIdempotencyKey,
    pub subscription_id: HookSubscriptionId,
    pub hook_id: HookId,
    pub phase: HookPhase,
    pub status: HookRunStatus,
    pub scope: Option<HookRunScope>,
    pub context: HookContext,
    pub contribution_hashes: Vec<HookContributionHash>,
    pub diagnostic_previews: Vec<HookDiagnosticPreview>,
    pub error: Option<HookRunErrorSummary>,
    pub queued_at: Option<DateTimeWithTimeZone>,
    pub started_at: Option<DateTimeWithTimeZone>,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub deadline_at: Option<DateTimeWithTimeZone>,
    pub resume_state: Option<HookRunResumeState>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HookRunRecord {
    pub id: HookRunId,
    pub idempotency_key: HookRunIdempotencyKey,
    pub subscription_id: HookSubscriptionId,
    pub hook_id: HookId,
    pub phase: HookPhase,
    pub status: HookRunStatus,
    pub scope: Option<HookRunScope>,
    pub context: HookContext,
    pub attempt_count: u16,
    pub contribution_count: usize,
    pub diagnostic_count: usize,
    pub contribution_hashes: Vec<HookContributionHash>,
    pub diagnostic_previews: Vec<HookDiagnosticPreview>,
    pub error: Option<HookRunErrorSummary>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
    pub queued_at: Option<DateTimeWithTimeZone>,
    pub started_at: Option<DateTimeWithTimeZone>,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub deadline_at: Option<DateTimeWithTimeZone>,
    pub resume_state: Option<HookRunResumeState>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoverableHookRunRecord {
    pub run: HookRunRecord,
    pub resume_state: Option<HookRunResumeState>,
    pub attempts: Vec<HookRunAttemptRecord>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HookRunCompletionRecord {
    pub status: HookRunStatus,
    pub contribution_hashes: Vec<HookContributionHash>,
    pub diagnostic_previews: Vec<HookDiagnosticPreview>,
    pub error: Option<HookRunErrorSummary>,
    pub completed_at: Option<DateTimeWithTimeZone>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewHookRunAttemptRecord {
    pub id: Option<HookRunAttemptId>,
    pub hook_run_id: HookRunId,
    pub attempt_number: u16,
    pub status: HookRunStatus,
    pub contribution_hashes: Vec<HookContributionHash>,
    pub diagnostic_previews: Vec<HookDiagnosticPreview>,
    pub error: Option<HookRunErrorSummary>,
    pub started_at: Option<DateTimeWithTimeZone>,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub duration_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HookRunAttemptRecord {
    pub id: HookRunAttemptId,
    pub hook_run_id: HookRunId,
    pub attempt_number: u16,
    pub status: HookRunStatus,
    pub contribution_count: usize,
    pub diagnostic_count: usize,
    pub contribution_hashes: Vec<HookContributionHash>,
    pub diagnostic_previews: Vec<HookDiagnosticPreview>,
    pub error: Option<HookRunErrorSummary>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
    pub started_at: Option<DateTimeWithTimeZone>,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub duration_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HookRunAttemptCompletionRecord {
    pub status: HookRunStatus,
    pub contribution_hashes: Vec<HookContributionHash>,
    pub diagnostic_previews: Vec<HookDiagnosticPreview>,
    pub error: Option<HookRunErrorSummary>,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub duration_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewHookAuditEventRecord {
    pub hook_run_id: HookRunId,
    pub hook_run_attempt_id: Option<HookRunAttemptId>,
    pub subscription_id: HookSubscriptionId,
    pub hook_id: HookId,
    pub phase: HookPhase,
    pub context: HookContext,
    pub event_kind: HookAuditEventKind,
    pub contribution_hash: Option<HookContributionHash>,
    pub details: HookValue,
    pub safe_for_user: bool,
    pub created_at: Option<DateTimeWithTimeZone>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HookAuditEventRecord {
    pub id: String,
    pub hook_run_id: HookRunId,
    pub hook_run_attempt_id: Option<HookRunAttemptId>,
    pub subscription_id: HookSubscriptionId,
    pub hook_id: HookId,
    pub phase: HookPhase,
    pub context: HookContext,
    pub event_kind: HookAuditEventKind,
    pub contribution_hash: Option<HookContributionHash>,
    pub details: HookValue,
    pub safe_for_user: bool,
    pub created_at: DateTimeWithTimeZone,
}

pub async fn create_hook_run<C: ConnectionTrait>(
    db: &C,
    run: NewHookRunRecord,
    now: DateTimeWithTimeZone,
) -> Result<HookRunRecord> {
    ensure_idempotency_key_is_bounded(&run.idempotency_key)?;
    let id = run
        .id
        .map(HookRunId::into_inner)
        .unwrap_or_else(|| generate_id(DB_ID_LEN));
    let contribution_count = run.contribution_hashes.len();
    let diagnostic_count = run.diagnostic_previews.len();
    let contribution_hashes = bounded_contribution_hashes(run.contribution_hashes);
    let diagnostic_previews = bounded_diagnostic_previews(run.diagnostic_previews);
    let error = sanitized_error(run.error);
    let (error_code, error_message_preview, error_retryable, error_safe_for_user) =
        error_columns(error);
    let queued_at = run.queued_at.or(Some(now));
    let metadata_json = serialize_metadata(&run.context.metadata)?;
    let resume_state_json = optional_serialize_resume_state(&run.resume_state)?;
    let scope_kind = run
        .scope
        .as_ref()
        .map(|scope| scope.kind.as_str().to_owned());
    let scope_id = run.scope.as_ref().map(|scope| scope.id.as_str().to_owned());
    let actor_kind = run
        .context
        .actor
        .as_ref()
        .map(|actor| actor.kind.as_str().to_owned());
    let actor_id = run
        .context
        .actor
        .as_ref()
        .and_then(|actor| actor.id.as_ref())
        .map(|id| id.as_str().to_owned());

    hook_run::Entity::insert(hook_run::ActiveModel {
        id: Set(id.clone()),
        idempotency_key: Set(run.idempotency_key.into_inner()),
        subscription_id: Set(run.subscription_id.into_inner()),
        hook_id: Set(run.hook_id.into_inner()),
        phase: Set(run.phase.as_str().to_owned()),
        status: Set(hook_run_status_to_db(run.status).to_owned()),
        scope_kind: Set(scope_kind),
        scope_id: Set(scope_id),
        workspace_id: Set(run.context.workspace_id.map(HookWorkspaceId::into_inner)),
        thread_id: Set(run.context.thread_id.map(HookThreadId::into_inner)),
        turn_id: Set(run.context.turn_id.map(HookTurnId::into_inner)),
        task_id: Set(run.context.task_id.map(HookTaskId::into_inner)),
        agent_id: Set(run.context.agent_id.map(HookAgentId::into_inner)),
        actor_kind: Set(actor_kind),
        actor_id: Set(actor_id),
        context_mode: Set(run.context.mode.map(|mode| mode.as_str().to_owned())),
        attempt_count: Set(0),
        contribution_count: Set(usize_to_i64(contribution_count, "contribution_count")?),
        diagnostic_count: Set(usize_to_i64(diagnostic_count, "diagnostic_count")?),
        contribution_hashes_json: Set(serialize_contribution_hashes(&contribution_hashes)?),
        diagnostic_previews_json: Set(serialize_diagnostic_previews(&diagnostic_previews)?),
        error_code: Set(error_code),
        error_message_preview: Set(error_message_preview),
        error_retryable: Set(error_retryable),
        error_safe_for_user: Set(error_safe_for_user),
        metadata_json: Set(metadata_json),
        resume_state_json: Set(resume_state_json),
        created_at: Set(now),
        updated_at: Set(now),
        queued_at: Set(queued_at),
        started_at: Set(run.started_at),
        completed_at: Set(run.completed_at),
        deadline_at: Set(run.deadline_at),
    })
    .exec(db)
    .await
    .context("failed to insert hook_run row")?;

    let run_id = HookRunId::new(id)?;
    find_hook_run_by_id(db, &run_id)
        .await?
        .context("inserted hook_run row missing")
}

pub async fn find_hook_run_by_id<C: ConnectionTrait>(
    db: &C,
    run_id: &HookRunId,
) -> Result<Option<HookRunRecord>> {
    let Some(model) = hook_run::Entity::find_by_id(run_id.as_str().to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to find hook run `{run_id}`"))?
    else {
        return Ok(None);
    };
    Ok(Some(hook_run_record_from_model(model)?))
}

pub async fn find_hook_run_by_idempotency_key<C: ConnectionTrait>(
    db: &C,
    idempotency_key: &HookRunIdempotencyKey,
) -> Result<Option<HookRunRecord>> {
    ensure_idempotency_key_is_bounded(idempotency_key)?;
    let Some(model) = hook_run::Entity::find()
        .filter(hook_run::Column::IdempotencyKey.eq(idempotency_key.as_str().to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to find hook run by idempotency key `{}`",
                idempotency_key.as_str()
            )
        })?
    else {
        return Ok(None);
    };
    Ok(Some(hook_run_record_from_model(model)?))
}

pub async fn mark_hook_run_running<C: ConnectionTrait>(
    db: &C,
    run_id: &HookRunId,
    now: DateTimeWithTimeZone,
) -> Result<Option<HookRunRecord>> {
    let affected = hook_run::Entity::update_many()
        .col_expr(
            hook_run::Column::Status,
            Expr::value(hook_run_status_to_db(HookRunStatus::Running).to_owned()),
        )
        .col_expr(hook_run::Column::StartedAt, Expr::value(Some(now)))
        .col_expr(hook_run::Column::UpdatedAt, Expr::value(now))
        .filter(hook_run::Column::Id.eq(run_id.as_str().to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark hook run `{run_id}` running"))?
        .rows_affected;
    if affected == 0 {
        return Ok(None);
    }
    find_hook_run_by_id(db, run_id).await
}

pub async fn complete_hook_run<C: ConnectionTrait>(
    db: &C,
    run_id: &HookRunId,
    completion: HookRunCompletionRecord,
    now: DateTimeWithTimeZone,
) -> Result<Option<HookRunRecord>> {
    ensure_terminal_status(completion.status)?;
    let contribution_count = completion.contribution_hashes.len();
    let diagnostic_count = completion.diagnostic_previews.len();
    let contribution_hashes = bounded_contribution_hashes(completion.contribution_hashes);
    let diagnostic_previews = bounded_diagnostic_previews(completion.diagnostic_previews);
    let error = sanitized_error(completion.error);
    let (error_code, error_message_preview, error_retryable, error_safe_for_user) =
        error_columns(error);
    let completed_at = completion.completed_at.unwrap_or(now);

    let affected = hook_run::Entity::update_many()
        .col_expr(
            hook_run::Column::Status,
            Expr::value(hook_run_status_to_db(completion.status).to_owned()),
        )
        .col_expr(
            hook_run::Column::ContributionCount,
            Expr::value(usize_to_i64(contribution_count, "contribution_count")?),
        )
        .col_expr(
            hook_run::Column::DiagnosticCount,
            Expr::value(usize_to_i64(diagnostic_count, "diagnostic_count")?),
        )
        .col_expr(
            hook_run::Column::ContributionHashesJson,
            Expr::value(serialize_contribution_hashes(&contribution_hashes)?),
        )
        .col_expr(
            hook_run::Column::DiagnosticPreviewsJson,
            Expr::value(serialize_diagnostic_previews(&diagnostic_previews)?),
        )
        .col_expr(hook_run::Column::ErrorCode, Expr::value(error_code))
        .col_expr(
            hook_run::Column::ErrorMessagePreview,
            Expr::value(error_message_preview),
        )
        .col_expr(
            hook_run::Column::ErrorRetryable,
            Expr::value(error_retryable),
        )
        .col_expr(
            hook_run::Column::ErrorSafeForUser,
            Expr::value(error_safe_for_user),
        )
        .col_expr(
            hook_run::Column::CompletedAt,
            Expr::value(Some(completed_at)),
        )
        .col_expr(hook_run::Column::UpdatedAt, Expr::value(now))
        .filter(hook_run::Column::Id.eq(run_id.as_str().to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to complete hook run `{run_id}`"))?
        .rows_affected;
    if affected == 0 {
        return Ok(None);
    }
    find_hook_run_by_id(db, run_id).await
}

pub async fn append_hook_run_attempt<C: ConnectionTrait>(
    db: &C,
    attempt: NewHookRunAttemptRecord,
    now: DateTimeWithTimeZone,
) -> Result<HookRunAttemptRecord> {
    ensure!(
        attempt.attempt_number > 0,
        "hook attempt number must be > 0"
    );
    let id = attempt
        .id
        .map(HookRunAttemptId::into_inner)
        .unwrap_or_else(|| generate_id(DB_ID_LEN));
    let contribution_count = attempt.contribution_hashes.len();
    let diagnostic_count = attempt.diagnostic_previews.len();
    let contribution_hashes = bounded_contribution_hashes(attempt.contribution_hashes);
    let diagnostic_previews = bounded_diagnostic_previews(attempt.diagnostic_previews);
    let error = sanitized_error(attempt.error);
    let (error_code, error_message_preview, error_retryable, error_safe_for_user) =
        error_columns(error);
    let hook_run_id = attempt.hook_run_id.into_inner();
    let attempt_number = i64::from(attempt.attempt_number);

    hook_run_attempt::Entity::insert(hook_run_attempt::ActiveModel {
        id: Set(id.clone()),
        hook_run_id: Set(hook_run_id.clone()),
        attempt_number: Set(attempt_number),
        status: Set(hook_run_status_to_db(attempt.status).to_owned()),
        contribution_count: Set(usize_to_i64(contribution_count, "contribution_count")?),
        diagnostic_count: Set(usize_to_i64(diagnostic_count, "diagnostic_count")?),
        contribution_hashes_json: Set(serialize_contribution_hashes(&contribution_hashes)?),
        diagnostic_previews_json: Set(serialize_diagnostic_previews(&diagnostic_previews)?),
        error_code: Set(error_code),
        error_message_preview: Set(error_message_preview),
        error_retryable: Set(error_retryable),
        error_safe_for_user: Set(error_safe_for_user),
        created_at: Set(now),
        updated_at: Set(now),
        started_at: Set(attempt.started_at),
        completed_at: Set(attempt.completed_at),
        duration_ms: Set(attempt.duration_ms),
    })
    .exec(db)
    .await
    .with_context(|| format!("failed to insert hook_run_attempt row for run `{hook_run_id}`"))?;

    hook_run::Entity::update_many()
        .col_expr(hook_run::Column::AttemptCount, Expr::value(attempt_number))
        .col_expr(hook_run::Column::UpdatedAt, Expr::value(now))
        .filter(hook_run::Column::Id.eq(hook_run_id.clone()))
        .filter(hook_run::Column::AttemptCount.lt(attempt_number))
        .exec(db)
        .await
        .with_context(|| format!("failed to update hook run `{hook_run_id}` attempt count"))?;

    find_hook_run_attempt_by_id(db, &HookRunAttemptId::new(id)?)
        .await?
        .context("inserted hook_run_attempt row missing")
}

pub async fn complete_hook_run_attempt<C: ConnectionTrait>(
    db: &C,
    attempt_id: &HookRunAttemptId,
    completion: HookRunAttemptCompletionRecord,
    now: DateTimeWithTimeZone,
) -> Result<Option<HookRunAttemptRecord>> {
    ensure_terminal_status(completion.status)?;
    let Some(existing) = hook_run_attempt::Entity::find_by_id(attempt_id.as_str().to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to find hook run attempt `{attempt_id}`"))?
    else {
        return Ok(None);
    };
    let contribution_count = completion.contribution_hashes.len();
    let diagnostic_count = completion.diagnostic_previews.len();
    let contribution_hashes = bounded_contribution_hashes(completion.contribution_hashes);
    let diagnostic_previews = bounded_diagnostic_previews(completion.diagnostic_previews);
    let error = sanitized_error(completion.error);
    let (error_code, error_message_preview, error_retryable, error_safe_for_user) =
        error_columns(error);
    let completed_at = completion.completed_at.unwrap_or(now);
    let duration_ms = completion.duration_ms.or_else(|| {
        existing
            .started_at
            .map(|started_at| (completed_at - started_at).num_milliseconds().max(0))
    });

    let affected = hook_run_attempt::Entity::update_many()
        .col_expr(
            hook_run_attempt::Column::Status,
            Expr::value(hook_run_status_to_db(completion.status).to_owned()),
        )
        .col_expr(
            hook_run_attempt::Column::ContributionCount,
            Expr::value(usize_to_i64(contribution_count, "contribution_count")?),
        )
        .col_expr(
            hook_run_attempt::Column::DiagnosticCount,
            Expr::value(usize_to_i64(diagnostic_count, "diagnostic_count")?),
        )
        .col_expr(
            hook_run_attempt::Column::ContributionHashesJson,
            Expr::value(serialize_contribution_hashes(&contribution_hashes)?),
        )
        .col_expr(
            hook_run_attempt::Column::DiagnosticPreviewsJson,
            Expr::value(serialize_diagnostic_previews(&diagnostic_previews)?),
        )
        .col_expr(hook_run_attempt::Column::ErrorCode, Expr::value(error_code))
        .col_expr(
            hook_run_attempt::Column::ErrorMessagePreview,
            Expr::value(error_message_preview),
        )
        .col_expr(
            hook_run_attempt::Column::ErrorRetryable,
            Expr::value(error_retryable),
        )
        .col_expr(
            hook_run_attempt::Column::ErrorSafeForUser,
            Expr::value(error_safe_for_user),
        )
        .col_expr(
            hook_run_attempt::Column::CompletedAt,
            Expr::value(Some(completed_at)),
        )
        .col_expr(
            hook_run_attempt::Column::DurationMs,
            Expr::value(duration_ms),
        )
        .col_expr(hook_run_attempt::Column::UpdatedAt, Expr::value(now))
        .filter(hook_run_attempt::Column::Id.eq(attempt_id.as_str().to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to complete hook run attempt `{attempt_id}`"))?
        .rows_affected;
    if affected == 0 {
        return Ok(None);
    }
    find_hook_run_attempt_by_id(db, attempt_id).await
}

pub async fn list_hook_run_attempts<C: ConnectionTrait>(
    db: &C,
    run_id: &HookRunId,
) -> Result<Vec<HookRunAttemptRecord>> {
    let models = hook_run_attempt::Entity::find()
        .filter(hook_run_attempt::Column::HookRunId.eq(run_id.as_str().to_owned()))
        .order_by_asc(hook_run_attempt::Column::AttemptNumber)
        .all(db)
        .await
        .with_context(|| format!("failed to list hook run attempts for `{run_id}`"))?;

    models
        .into_iter()
        .map(hook_run_attempt_record_from_model)
        .collect()
}

pub async fn list_recoverable_hook_runs<C: ConnectionTrait>(
    db: &C,
    scan: HookRecoveryScan,
) -> Result<Vec<RecoverableHookRunRecord>> {
    let now = unix_ms_to_datetime(scan.now_unix_ms);
    let stale_started_before = now
        .checked_sub_signed(ChronoDuration::milliseconds(
            i64::try_from(scan.stale_running_after_ms).unwrap_or(i64::MAX),
        ))
        .unwrap_or(now);
    let mut condition = Condition::any()
        .add(hook_run::Column::Status.eq(hook_run_status_to_db(HookRunStatus::Queued)))
        .add(hook_run::Column::Status.eq(hook_run_status_to_db(HookRunStatus::Running)));
    if let Some(phases) = scan.phases {
        let phase_values = phases
            .into_iter()
            .map(|phase| phase.as_str().to_owned())
            .collect::<Vec<_>>();
        condition = Condition::all()
            .add(condition)
            .add(hook_run::Column::Phase.is_in(phase_values));
    }
    let limit = u64::try_from(scan.batch_size.max(1)).unwrap_or(u64::MAX);
    let rows = hook_run::Entity::find()
        .filter(condition)
        .filter(hook_run::Column::ResumeStateJson.is_not_null())
        .order_by_asc(hook_run::Column::QueuedAt)
        .order_by_asc(hook_run::Column::CreatedAt)
        .limit(limit.saturating_mul(4))
        .all(db)
        .await
        .context("failed to list recoverable hook runs")?;

    let mut records = Vec::new();
    for row in rows {
        let due = match row.status.as_str() {
            "queued" => row.queued_at.is_none_or(|queued_at| queued_at <= now),
            "running" => {
                row.deadline_at
                    .is_some_and(|deadline_at| deadline_at <= now)
                    || row
                        .started_at
                        .is_some_and(|started_at| started_at <= stale_started_before)
            }
            _ => false,
        };
        if !due {
            continue;
        }
        let run = hook_run_record_from_model(row)?;
        let attempts = list_hook_run_attempts(db, &run.id).await?;
        records.push(RecoverableHookRunRecord {
            resume_state: run.resume_state.clone(),
            run,
            attempts,
        });
        if records.len() >= scan.batch_size.max(1) {
            break;
        }
    }
    Ok(records)
}

pub async fn schedule_hook_run_retry<C: ConnectionTrait>(
    db: &C,
    run_id: &HookRunId,
    schedule: HookRetrySchedule,
    now: DateTimeWithTimeZone,
) -> Result<Option<HookRunRecord>> {
    let diagnostic_previews = bounded_diagnostic_previews(schedule.diagnostic_previews);
    let queued_at = unix_ms_to_datetime(schedule.queued_at_unix_ms);
    let deadline_at = schedule.deadline_at_unix_ms.map(unix_ms_to_datetime);
    let affected = hook_run::Entity::update_many()
        .col_expr(
            hook_run::Column::Status,
            Expr::value(hook_run_status_to_db(HookRunStatus::Queued).to_owned()),
        )
        .col_expr(
            hook_run::Column::DiagnosticCount,
            Expr::value(usize_to_i64(diagnostic_previews.len(), "diagnostic_count")?),
        )
        .col_expr(
            hook_run::Column::DiagnosticPreviewsJson,
            Expr::value(serialize_diagnostic_previews(&diagnostic_previews)?),
        )
        .col_expr(
            hook_run::Column::ErrorCode,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            hook_run::Column::ErrorMessagePreview,
            Expr::value(Option::<String>::None),
        )
        .col_expr(hook_run::Column::ErrorRetryable, Expr::value(false))
        .col_expr(hook_run::Column::ErrorSafeForUser, Expr::value(true))
        .col_expr(hook_run::Column::QueuedAt, Expr::value(Some(queued_at)))
        .col_expr(
            hook_run::Column::StartedAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            hook_run::Column::CompletedAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(hook_run::Column::DeadlineAt, Expr::value(deadline_at))
        .col_expr(hook_run::Column::UpdatedAt, Expr::value(now))
        .filter(hook_run::Column::Id.eq(run_id.as_str().to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to schedule hook run `{run_id}` retry"))?
        .rows_affected;
    if affected == 0 {
        return Ok(None);
    }
    find_hook_run_by_id(db, run_id).await
}

pub async fn mark_stale_hook_run_timed_out<C: ConnectionTrait>(
    db: &C,
    run_id: &HookRunId,
    completion: HookRunCompletionRecord,
    now: DateTimeWithTimeZone,
) -> Result<Option<HookRunRecord>> {
    let latest_running_attempt = hook_run_attempt::Entity::find()
        .filter(hook_run_attempt::Column::HookRunId.eq(run_id.as_str().to_owned()))
        .filter(hook_run_attempt::Column::Status.eq(hook_run_status_to_db(HookRunStatus::Running)))
        .order_by_desc(hook_run_attempt::Column::AttemptNumber)
        .one(db)
        .await
        .with_context(|| format!("failed to find stale running hook attempt for `{run_id}`"))?;
    if let Some(attempt) = latest_running_attempt {
        let attempt_id =
            HookRunAttemptId::new(attempt.id).context("invalid stale hook run attempt id")?;
        let _ = complete_hook_run_attempt(
            db,
            &attempt_id,
            HookRunAttemptCompletionRecord {
                status: HookRunStatus::TimedOut,
                contribution_hashes: completion.contribution_hashes.clone(),
                diagnostic_previews: completion.diagnostic_previews.clone(),
                error: completion.error.clone(),
                completed_at: completion.completed_at,
                duration_ms: None,
            },
            now,
        )
        .await?;
    }
    complete_hook_run(db, run_id, completion, now).await
}

pub async fn mark_hook_run_unrecoverable<C: ConnectionTrait>(
    db: &C,
    run_id: &HookRunId,
    completion: HookRunCompletionRecord,
    now: DateTimeWithTimeZone,
) -> Result<Option<HookRunRecord>> {
    complete_hook_run(db, run_id, completion, now).await
}

pub async fn append_hook_audit_events<C: ConnectionTrait>(
    db: &C,
    records: Vec<NewHookAuditEventRecord>,
    now: DateTimeWithTimeZone,
) -> Result<Vec<HookAuditEventRecord>> {
    let mut created = Vec::with_capacity(records.len());
    for record in records {
        let id = generate_id(DB_ID_LEN);
        let created_at = record.created_at.unwrap_or(now);
        let actor_kind = record
            .context
            .actor
            .as_ref()
            .map(|actor| actor.kind.as_str().to_owned());
        let actor_id = record
            .context
            .actor
            .as_ref()
            .and_then(|actor| actor.id.as_ref())
            .map(|id| id.as_str().to_owned());

        hook_audit_event::Entity::insert(hook_audit_event::ActiveModel {
            id: Set(id.clone()),
            hook_run_id: Set(record.hook_run_id.into_inner()),
            hook_run_attempt_id: Set(record.hook_run_attempt_id.map(HookRunAttemptId::into_inner)),
            subscription_id: Set(record.subscription_id.into_inner()),
            hook_id: Set(record.hook_id.into_inner()),
            phase: Set(record.phase.as_str().to_owned()),
            event_kind: Set(record.event_kind.into_inner()),
            contribution_hash: Set(record
                .contribution_hash
                .map(HookContributionHash::into_inner)),
            workspace_id: Set(record.context.workspace_id.map(HookWorkspaceId::into_inner)),
            thread_id: Set(record.context.thread_id.map(HookThreadId::into_inner)),
            turn_id: Set(record.context.turn_id.map(HookTurnId::into_inner)),
            task_id: Set(record.context.task_id.map(HookTaskId::into_inner)),
            agent_id: Set(record.context.agent_id.map(HookAgentId::into_inner)),
            actor_kind: Set(actor_kind),
            actor_id: Set(actor_id),
            context_mode: Set(record.context.mode.map(|mode| mode.as_str().to_owned())),
            safe_for_user: Set(record.safe_for_user),
            details_json: Set(serialize_hook_value(&record.details)?),
            created_at: Set(created_at),
        })
        .exec(db)
        .await
        .context("failed to insert hook_audit_event row")?;

        let row = hook_audit_event::Entity::find_by_id(id)
            .one(db)
            .await
            .context("failed to load inserted hook_audit_event row")?
            .context("inserted hook_audit_event row missing")?;
        created.push(hook_audit_event_record_from_model(row)?);
    }
    Ok(created)
}

pub async fn list_hook_audit_events_for_run<C: ConnectionTrait>(
    db: &C,
    run_id: &HookRunId,
) -> Result<Vec<HookAuditEventRecord>> {
    let rows = hook_audit_event::Entity::find()
        .filter(hook_audit_event::Column::HookRunId.eq(run_id.as_str().to_owned()))
        .order_by_asc(hook_audit_event::Column::CreatedAt)
        .all(db)
        .await
        .with_context(|| format!("failed to list hook audit events for run `{run_id}`"))?;

    rows.into_iter()
        .map(hook_audit_event_record_from_model)
        .collect()
}

async fn find_hook_run_attempt_by_id<C: ConnectionTrait>(
    db: &C,
    attempt_id: &HookRunAttemptId,
) -> Result<Option<HookRunAttemptRecord>> {
    let Some(model) = hook_run_attempt::Entity::find_by_id(attempt_id.as_str().to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to find hook run attempt `{attempt_id}`"))?
    else {
        return Ok(None);
    };
    Ok(Some(hook_run_attempt_record_from_model(model)?))
}

fn hook_run_record_from_model(model: hook_run::Model) -> Result<HookRunRecord> {
    let metadata = deserialize_metadata(model.metadata_json.as_str())?;
    let actor = match model.actor_kind {
        Some(kind) => Some(HookActor {
            kind: HookActorKind::from(kind),
            id: parse_optional_hook_id(model.actor_id, "hook_run.actor_id")?,
        }),
        None => None,
    };
    let context = HookContext {
        workspace_id: parse_optional_hook_id(model.workspace_id, "hook_run.workspace_id")?,
        thread_id: parse_optional_hook_id(model.thread_id, "hook_run.thread_id")?,
        turn_id: parse_optional_hook_id(model.turn_id, "hook_run.turn_id")?,
        task_id: parse_optional_hook_id(model.task_id, "hook_run.task_id")?,
        agent_id: parse_optional_hook_id(model.agent_id, "hook_run.agent_id")?,
        mode: model.context_mode.map(HookContextMode::from),
        actor,
        now_unix: None,
        runtime_home: None,
        feature_flags: BTreeMap::new(),
        metadata,
    };
    let scope = match (model.scope_kind, model.scope_id) {
        (Some(kind), Some(id)) => Some(HookRunScope {
            kind: HookRunScopeKind::from(kind),
            id: HookRunScopeId::new(id).context("invalid hook_run.scope_id")?,
        }),
        (None, None) => None,
        _ => bail!("hook_run scope_kind and scope_id must both be present or absent"),
    };
    Ok(HookRunRecord {
        id: HookRunId::new(model.id).context("invalid hook_run.id")?,
        idempotency_key: HookRunIdempotencyKey::new(model.idempotency_key)
            .context("invalid hook_run.idempotency_key")?,
        subscription_id: HookSubscriptionId::new(model.subscription_id)
            .context("invalid hook_run.subscription_id")?,
        hook_id: HookId::new(model.hook_id).context("invalid hook_run.hook_id")?,
        phase: HookPhase::from_str(model.phase.as_str()).context("invalid hook_run.phase")?,
        status: hook_run_status_from_db(model.status.as_str())
            .context("invalid hook_run.status")?,
        scope,
        context,
        attempt_count: i64_to_u16(model.attempt_count, "hook_run.attempt_count")?,
        contribution_count: i64_to_usize(model.contribution_count, "hook_run.contribution_count")?,
        diagnostic_count: i64_to_usize(model.diagnostic_count, "hook_run.diagnostic_count")?,
        contribution_hashes: deserialize_contribution_hashes(
            model.contribution_hashes_json.as_str(),
        )?,
        diagnostic_previews: deserialize_diagnostic_previews(
            model.diagnostic_previews_json.as_str(),
        )?,
        error: error_from_columns(
            model.error_code,
            model.error_message_preview,
            model.error_retryable,
            model.error_safe_for_user,
        )?,
        created_at: model.created_at,
        updated_at: model.updated_at,
        queued_at: model.queued_at,
        started_at: model.started_at,
        completed_at: model.completed_at,
        deadline_at: model.deadline_at,
        resume_state: optional_deserialize_resume_state(model.resume_state_json)?,
    })
}

fn hook_run_attempt_record_from_model(
    model: hook_run_attempt::Model,
) -> Result<HookRunAttemptRecord> {
    Ok(HookRunAttemptRecord {
        id: HookRunAttemptId::new(model.id).context("invalid hook_run_attempt.id")?,
        hook_run_id: HookRunId::new(model.hook_run_id)
            .context("invalid hook_run_attempt.hook_run_id")?,
        attempt_number: i64_to_u16(model.attempt_number, "hook_run_attempt.attempt_number")?,
        status: hook_run_status_from_db(model.status.as_str())
            .context("invalid hook_run_attempt.status")?,
        contribution_count: i64_to_usize(
            model.contribution_count,
            "hook_run_attempt.contribution_count",
        )?,
        diagnostic_count: i64_to_usize(
            model.diagnostic_count,
            "hook_run_attempt.diagnostic_count",
        )?,
        contribution_hashes: deserialize_contribution_hashes(
            model.contribution_hashes_json.as_str(),
        )?,
        diagnostic_previews: deserialize_diagnostic_previews(
            model.diagnostic_previews_json.as_str(),
        )?,
        error: error_from_columns(
            model.error_code,
            model.error_message_preview,
            model.error_retryable,
            model.error_safe_for_user,
        )?,
        created_at: model.created_at,
        updated_at: model.updated_at,
        started_at: model.started_at,
        completed_at: model.completed_at,
        duration_ms: model.duration_ms,
    })
}

fn hook_audit_event_record_from_model(
    model: hook_audit_event::Model,
) -> Result<HookAuditEventRecord> {
    let actor = match model.actor_kind {
        Some(kind) => Some(HookActor {
            kind: HookActorKind::from(kind),
            id: parse_optional_hook_id(model.actor_id, "hook_audit_event.actor_id")?,
        }),
        None => None,
    };
    let context = HookContext {
        workspace_id: parse_optional_hook_id(model.workspace_id, "hook_audit_event.workspace_id")?,
        thread_id: parse_optional_hook_id(model.thread_id, "hook_audit_event.thread_id")?,
        turn_id: parse_optional_hook_id(model.turn_id, "hook_audit_event.turn_id")?,
        task_id: parse_optional_hook_id(model.task_id, "hook_audit_event.task_id")?,
        agent_id: parse_optional_hook_id(model.agent_id, "hook_audit_event.agent_id")?,
        mode: model.context_mode.map(HookContextMode::from),
        actor,
        now_unix: None,
        runtime_home: None,
        feature_flags: BTreeMap::new(),
        metadata: HookMetadata::default(),
    };
    Ok(HookAuditEventRecord {
        id: model.id,
        hook_run_id: HookRunId::new(model.hook_run_id)
            .context("invalid hook_audit_event.hook_run_id")?,
        hook_run_attempt_id: parse_optional_hook_id(
            model.hook_run_attempt_id,
            "hook_audit_event.hook_run_attempt_id",
        )?,
        subscription_id: HookSubscriptionId::new(model.subscription_id)
            .context("invalid hook_audit_event.subscription_id")?,
        hook_id: HookId::new(model.hook_id).context("invalid hook_audit_event.hook_id")?,
        phase: HookPhase::from_str(model.phase.as_str())
            .context("invalid hook_audit_event.phase")?,
        context,
        event_kind: HookAuditEventKind::new(model.event_kind)
            .context("invalid hook_audit_event.event_kind")?,
        contribution_hash: parse_optional_hook_id(
            model.contribution_hash,
            "hook_audit_event.contribution_hash",
        )?,
        details: deserialize_hook_value(model.details_json.as_str())?,
        safe_for_user: model.safe_for_user,
        created_at: model.created_at,
    })
}

fn ensure_idempotency_key_is_bounded(key: &HookRunIdempotencyKey) -> Result<()> {
    let chars = key.as_str().chars().count();
    ensure!(
        chars <= HOOK_RUN_IDEMPOTENCY_KEY_MAX_CHARS,
        "hook run idempotency key exceeds {} chars",
        HOOK_RUN_IDEMPOTENCY_KEY_MAX_CHARS
    );
    Ok(())
}

fn ensure_terminal_status(status: HookRunStatus) -> Result<()> {
    match status {
        HookRunStatus::Succeeded
        | HookRunStatus::Failed
        | HookRunStatus::TimedOut
        | HookRunStatus::Skipped => Ok(()),
        HookRunStatus::Queued | HookRunStatus::Running => {
            bail!("hook run completion status must be terminal")
        }
    }
}

fn bounded_contribution_hashes(mut hashes: Vec<HookContributionHash>) -> Vec<HookContributionHash> {
    hashes.truncate(HOOK_RUN_CONTRIBUTION_HASH_MAX_COUNT);
    hashes
}

fn bounded_diagnostic_previews(previews: Vec<HookDiagnosticPreview>) -> Vec<HookDiagnosticPreview> {
    previews
        .into_iter()
        .take(HOOK_RUN_DIAGNOSTIC_PREVIEW_MAX_COUNT)
        .map(sanitized_diagnostic_preview)
        .collect()
}

fn sanitized_diagnostic_preview(mut preview: HookDiagnosticPreview) -> HookDiagnosticPreview {
    if !preview.safe_for_user {
        preview.message =
            HookDiagnosticMessage::new(REDACTED_DIAGNOSTIC_MESSAGE).expect("redacted message");
        preview.safe_for_user = true;
        preview.redacted = true;
        return preview;
    }
    let (message, truncated) =
        bounded_message(preview.message, HOOK_RUN_DIAGNOSTIC_MESSAGE_MAX_CHARS);
    preview.message = message;
    preview.redacted = preview.redacted || truncated;
    preview
}

fn sanitized_error(error: Option<HookRunErrorSummary>) -> Option<HookRunErrorSummary> {
    error.map(|mut error| {
        if !error.safe_for_user {
            error.message =
                HookDiagnosticMessage::new(REDACTED_ERROR_MESSAGE).expect("redacted error");
            error.safe_for_user = true;
            return error;
        }
        let (message, _) = bounded_message(error.message, HOOK_RUN_ERROR_MESSAGE_MAX_CHARS);
        error.message = message;
        error
    })
}

fn bounded_message(
    message: HookDiagnosticMessage,
    max_chars: usize,
) -> (HookDiagnosticMessage, bool) {
    let value = message.as_str();
    if value.chars().count() <= max_chars {
        return (message, false);
    }
    let bounded = value
        .chars()
        .take(max_chars.saturating_sub(3))
        .chain("...".chars())
        .collect::<String>();
    (
        HookDiagnosticMessage::new(bounded).expect("bounded message should be valid"),
        true,
    )
}

fn error_columns(
    error: Option<HookRunErrorSummary>,
) -> (Option<String>, Option<String>, bool, bool) {
    match error {
        Some(error) => (
            Some(error.code.into_inner()),
            Some(error.message.into_inner()),
            error.retryable,
            error.safe_for_user,
        ),
        None => (None, None, false, true),
    }
}

fn error_from_columns(
    code: Option<String>,
    message: Option<String>,
    retryable: bool,
    safe_for_user: bool,
) -> Result<Option<HookRunErrorSummary>> {
    let Some(code) = code else {
        return Ok(None);
    };
    let message = message.unwrap_or_else(|| "hook error".to_owned());
    Ok(Some(HookRunErrorSummary {
        code: HookDiagnosticCode::new(code).context("invalid hook run error code")?,
        message: HookDiagnosticMessage::new(message)
            .context("invalid hook run error message preview")?,
        retryable,
        safe_for_user,
    }))
}

fn serialize_contribution_hashes(hashes: &[HookContributionHash]) -> Result<String> {
    serde_json::to_string(hashes).context("failed to serialize hook contribution hashes")
}

fn deserialize_contribution_hashes(value: &str) -> Result<Vec<HookContributionHash>> {
    serde_json::from_str(value).context("failed to deserialize hook contribution hashes")
}

fn serialize_diagnostic_previews(previews: &[HookDiagnosticPreview]) -> Result<String> {
    serde_json::to_string(previews).context("failed to serialize hook diagnostic previews")
}

fn deserialize_diagnostic_previews(value: &str) -> Result<Vec<HookDiagnosticPreview>> {
    serde_json::from_str(value).context("failed to deserialize hook diagnostic previews")
}

fn serialize_metadata(metadata: &HookMetadata) -> Result<String> {
    serde_json::to_string(metadata).context("failed to serialize hook run metadata")
}

fn deserialize_metadata(value: &str) -> Result<HookMetadata> {
    if value.trim().is_empty() {
        return Ok(HookMetadata::default());
    }
    serde_json::from_str(value).context("failed to deserialize hook run metadata")
}

fn optional_serialize_resume_state(value: &Option<HookRunResumeState>) -> Result<Option<String>> {
    value
        .as_ref()
        .map(|state| serde_json::to_string(state).context("failed to serialize hook resume state"))
        .transpose()
}

fn optional_deserialize_resume_state(value: Option<String>) -> Result<Option<HookRunResumeState>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.trim().is_empty() {
        return Ok(None);
    }
    serde_json::from_str(value.as_str()).context("failed to deserialize hook resume state")
}

fn serialize_hook_value(value: &HookValue) -> Result<String> {
    serde_json::to_string(value).context("failed to serialize hook value")
}

fn deserialize_hook_value(value: &str) -> Result<HookValue> {
    if value.trim().is_empty() {
        return Ok(HookValue::Null);
    }
    serde_json::from_str(value).context("failed to deserialize hook value")
}

fn parse_optional_hook_id<T>(value: Option<String>, field: &'static str) -> Result<Option<T>>
where
    T: TryFrom<String, Error = HookIdError>,
{
    value
        .map(T::try_from)
        .transpose()
        .with_context(|| format!("invalid {field}"))
}

fn usize_to_i64(value: usize, field: &'static str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{field} does not fit in i64"))
}

fn i64_to_usize(value: i64, field: &'static str) -> Result<usize> {
    usize::try_from(value).with_context(|| format!("{field} cannot be converted to usize"))
}

fn i64_to_u16(value: i64, field: &'static str) -> Result<u16> {
    u16::try_from(value).with_context(|| format!("{field} cannot be converted to u16"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CrudStore;
    use migration::{Migrator, MigratorTrait};
    use pioneer_hooks::{HookActorId, HookDiagnosticSeverity, HookMetadataKey, HookValue};
    use sea_orm::{Database, EntityTrait};

    async fn migrated_store() -> (sea_orm::DatabaseConnection, CrudStore) {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        let store = CrudStore::new(connection.clone());
        (connection, store)
    }

    fn timestamp(offset: i64) -> DateTimeWithTimeZone {
        crate::util::unix_to_datetime(1_700_000_000 + offset)
    }

    fn idempotency_key(value: &str) -> HookRunIdempotencyKey {
        HookRunIdempotencyKey::new(value).expect("valid idempotency key")
    }

    fn sample_context() -> HookContext {
        let mut metadata = HookMetadata::default();
        metadata.insert(
            HookMetadataKey::new("runtime.version").expect("valid metadata key"),
            HookValue::Text("phase-14-test".to_owned()),
        );

        HookContext {
            workspace_id: Some(HookWorkspaceId::new("workspace_phase14").expect("valid id")),
            thread_id: Some(HookThreadId::new("thread_phase14").expect("valid id")),
            turn_id: Some(HookTurnId::new("turn_phase14").expect("valid id")),
            task_id: Some(HookTaskId::new("task_phase14").expect("valid id")),
            agent_id: Some(HookAgentId::new("agent.phase14").expect("valid id")),
            mode: Some(HookContextMode::Agent),
            actor: Some(HookActor {
                kind: HookActorKind::Agent,
                id: Some(HookActorId::new("agent.phase14").expect("valid actor id")),
            }),
            metadata,
            ..HookContext::default()
        }
    }

    fn new_run(key: &str) -> NewHookRunRecord {
        NewHookRunRecord {
            id: None,
            idempotency_key: idempotency_key(key),
            subscription_id: HookSubscriptionId::new("subscription.phase14").expect("valid id"),
            hook_id: HookId::new("hook.phase14").expect("valid id"),
            phase: HookPhase::TurnPostTurn,
            status: HookRunStatus::Queued,
            scope: Some(HookRunScope {
                kind: HookRunScopeKind::Turn,
                id: HookRunScopeId::new("turn_phase14").expect("valid scope id"),
            }),
            context: sample_context(),
            contribution_hashes: Vec::new(),
            diagnostic_previews: Vec::new(),
            error: None,
            queued_at: None,
            started_at: None,
            completed_at: None,
            deadline_at: Some(timestamp(60)),
            resume_state: None,
        }
    }

    fn contribution_hash(value: &str) -> HookContributionHash {
        HookContributionHash::new(value).expect("valid contribution hash")
    }

    fn diagnostic_preview(message: &str, safe_for_user: bool) -> HookDiagnosticPreview {
        HookDiagnosticPreview {
            code: HookDiagnosticCode::new("hook.phase14.diagnostic").expect("valid code"),
            message: HookDiagnosticMessage::new(message).expect("valid message"),
            severity: HookDiagnosticSeverity::Warning,
            safe_for_user,
            redacted: false,
        }
    }

    #[tokio::test]
    async fn create_run_persists_typed_context_and_safe_defaults() {
        let (_connection, store) = migrated_store().await;
        let created = store
            .create_hook_run(
                new_run("turn-1:turn.post_turn:subscription-1:hook-1"),
                timestamp(0),
            )
            .await
            .expect("hook run should be created");

        assert_eq!(
            created.idempotency_key.as_str(),
            "turn-1:turn.post_turn:subscription-1:hook-1"
        );
        assert_eq!(created.subscription_id.as_str(), "subscription.phase14");
        assert_eq!(created.hook_id.as_str(), "hook.phase14");
        assert_eq!(created.phase, HookPhase::TurnPostTurn);
        assert_eq!(created.status, HookRunStatus::Queued);
        assert_eq!(
            created
                .context
                .workspace_id
                .as_ref()
                .map(HookWorkspaceId::as_str),
            Some("workspace_phase14")
        );
        assert_eq!(
            created.scope.as_ref().map(|scope| &scope.kind),
            Some(&HookRunScopeKind::Turn)
        );
        assert_eq!(created.attempt_count, 0);
        assert_eq!(created.contribution_count, 0);
        assert_eq!(created.diagnostic_count, 0);
        assert!(created.contribution_hashes.is_empty());
        assert!(created.diagnostic_previews.is_empty());
        assert!(created.error.is_none());
        assert!(created.queued_at.is_some());
        assert_eq!(created.deadline_at, Some(timestamp(60)));
        assert_eq!(
            created
                .context
                .metadata
                .get(&HookMetadataKey::new("runtime.version").expect("valid key")),
            Some(&HookValue::Text("phase-14-test".to_owned()))
        );
    }

    #[tokio::test]
    async fn append_attempt_persists_attempt_and_updates_parent_count() {
        let (_connection, store) = migrated_store().await;
        let created = store
            .create_hook_run(
                new_run("turn-2:turn.post_turn:subscription-1:hook-1"),
                timestamp(0),
            )
            .await
            .expect("hook run should be created");

        let attempt = store
            .append_hook_run_attempt(
                NewHookRunAttemptRecord {
                    id: None,
                    hook_run_id: created.id.clone(),
                    attempt_number: 1,
                    status: HookRunStatus::Running,
                    contribution_hashes: vec![contribution_hash("sha256:attempt")],
                    diagnostic_previews: vec![diagnostic_preview("attempt diagnostic", true)],
                    error: None,
                    started_at: Some(timestamp(1)),
                    completed_at: None,
                    duration_ms: None,
                },
                timestamp(1),
            )
            .await
            .expect("hook run attempt should be appended");

        assert_eq!(attempt.hook_run_id, created.id);
        assert_eq!(attempt.attempt_number, 1);
        assert_eq!(attempt.status, HookRunStatus::Running);
        assert_eq!(attempt.contribution_count, 1);
        assert_eq!(attempt.diagnostic_count, 1);

        let attempts = store
            .list_hook_run_attempts(&created.id)
            .await
            .expect("attempts should list");
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].id, attempt.id);

        let parent = store
            .find_hook_run(&created.id)
            .await
            .expect("parent find should succeed")
            .expect("parent should exist");
        assert_eq!(parent.attempt_count, 1);

        let duplicate = store
            .append_hook_run_attempt(
                NewHookRunAttemptRecord {
                    id: None,
                    hook_run_id: created.id.clone(),
                    attempt_number: 1,
                    status: HookRunStatus::Running,
                    contribution_hashes: Vec::new(),
                    diagnostic_previews: Vec::new(),
                    error: None,
                    started_at: Some(timestamp(2)),
                    completed_at: None,
                    duration_ms: None,
                },
                timestamp(2),
            )
            .await;
        assert!(duplicate.is_err(), "duplicate run attempt number must fail");
    }

    #[tokio::test]
    async fn complete_run_persists_terminal_status_hashes_and_timestamps() {
        let (_connection, store) = migrated_store().await;
        let created = store
            .create_hook_run(
                new_run("turn-3:turn.post_turn:subscription-1:hook-1"),
                timestamp(0),
            )
            .await
            .expect("hook run should be created");
        let running = store
            .mark_hook_run_running(&created.id, timestamp(1))
            .await
            .expect("mark running should succeed")
            .expect("run should exist");
        assert_eq!(running.status, HookRunStatus::Running);
        assert_eq!(running.started_at, Some(timestamp(1)));

        let completed = store
            .complete_hook_run(
                &created.id,
                HookRunCompletionRecord {
                    status: HookRunStatus::Succeeded,
                    contribution_hashes: vec![contribution_hash("sha256:completed")],
                    diagnostic_previews: vec![diagnostic_preview("completed diagnostic", true)],
                    error: None,
                    completed_at: Some(timestamp(3)),
                },
                timestamp(4),
            )
            .await
            .expect("complete should succeed")
            .expect("run should exist");

        assert_eq!(completed.status, HookRunStatus::Succeeded);
        assert_eq!(completed.contribution_count, 1);
        assert_eq!(completed.diagnostic_count, 1);
        assert_eq!(
            completed.contribution_hashes[0].as_str(),
            "sha256:completed"
        );
        assert_eq!(
            completed.diagnostic_previews[0].message.as_str(),
            "completed diagnostic"
        );
        assert!(completed.error.is_none());
        assert_eq!(completed.completed_at, Some(timestamp(3)));
        assert_eq!(completed.updated_at, timestamp(4));
    }

    #[tokio::test]
    async fn find_by_idempotency_key_returns_original_and_uniqueness_is_enforced() {
        let (_connection, store) = migrated_store().await;
        let key = "turn-4:turn.post_turn:subscription-1:hook-1";
        let created = store
            .create_hook_run(new_run(key), timestamp(0))
            .await
            .expect("hook run should be created");

        let found = store
            .find_hook_run_by_idempotency_key(&idempotency_key(key))
            .await
            .expect("idempotency lookup should succeed")
            .expect("hook run should exist");
        assert_eq!(found.id, created.id);

        let duplicate = store.create_hook_run(new_run(key), timestamp(1)).await;
        assert!(duplicate.is_err(), "duplicate idempotency key must fail");

        let missing = store
            .find_hook_run_by_idempotency_key(&idempotency_key(
                "turn-4:turn.post_turn:subscription-1:hook-missing",
            ))
            .await
            .expect("missing lookup should succeed");
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn diagnostics_are_bounded_before_storage() {
        let (connection, store) = migrated_store().await;
        let created = store
            .create_hook_run(
                new_run("turn-5:turn.post_turn:subscription-1:hook-1"),
                timestamp(0),
            )
            .await
            .expect("hook run should be created");
        let long_message = "x".repeat(HOOK_RUN_DIAGNOSTIC_MESSAGE_MAX_CHARS + 100);
        let mut diagnostics = (0..(HOOK_RUN_DIAGNOSTIC_PREVIEW_MAX_COUNT + 5))
            .map(|index| diagnostic_preview(format!("diagnostic-{index}").as_str(), true))
            .collect::<Vec<_>>();
        diagnostics[0] = diagnostic_preview(long_message.as_str(), true);
        diagnostics[1] = diagnostic_preview("password=secret", false);

        let completed = store
            .complete_hook_run(
                &created.id,
                HookRunCompletionRecord {
                    status: HookRunStatus::Failed,
                    contribution_hashes: Vec::new(),
                    diagnostic_previews: diagnostics,
                    error: Some(HookRunErrorSummary {
                        code: HookDiagnosticCode::new("hook.phase14.failed").expect("valid code"),
                        message: HookDiagnosticMessage::new("token=secret").expect("valid error"),
                        retryable: true,
                        safe_for_user: false,
                    }),
                    completed_at: Some(timestamp(1)),
                },
                timestamp(1),
            )
            .await
            .expect("complete should succeed")
            .expect("run should exist");

        assert_eq!(
            completed.diagnostic_previews.len(),
            HOOK_RUN_DIAGNOSTIC_PREVIEW_MAX_COUNT
        );
        assert_eq!(
            completed.diagnostic_count,
            HOOK_RUN_DIAGNOSTIC_PREVIEW_MAX_COUNT + 5
        );
        assert!(
            completed.diagnostic_previews[0]
                .message
                .as_str()
                .chars()
                .count()
                <= HOOK_RUN_DIAGNOSTIC_MESSAGE_MAX_CHARS
        );
        assert!(completed.diagnostic_previews[0].redacted);
        assert_eq!(
            completed.diagnostic_previews[1].message.as_str(),
            REDACTED_DIAGNOSTIC_MESSAGE
        );
        assert!(completed.diagnostic_previews[1].safe_for_user);
        assert_eq!(
            completed.error.as_ref().map(|error| error.message.as_str()),
            Some(REDACTED_ERROR_MESSAGE)
        );

        let raw = hook_run::Entity::find_by_id(completed.id.as_str().to_owned())
            .one(&connection)
            .await
            .expect("raw hook_run query should succeed")
            .expect("raw hook_run row should exist");
        assert!(!raw.diagnostic_previews_json.contains("password=secret"));
        assert_eq!(
            raw.error_message_preview.as_deref(),
            Some(REDACTED_ERROR_MESSAGE)
        );
    }
}
