use anyhow::{Context, Result, anyhow};
use pioneer_entity::{
    cli_runtime_native_event, cli_runtime_pending_request, thread_cli_runtime_binding,
    turn_cli_runtime_attempt, turn_cli_runtime_binding,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};
use serde::{Serialize, de::DeserializeOwned};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliRuntimePendingRequestStatus {
    Pending,
    Answered,
    Resolved,
    Cancelled,
    Expired,
}

impl CliRuntimePendingRequestStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Answered => "answered",
            Self::Resolved => "resolved",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
        }
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "answered" => Ok(Self::Answered),
            "resolved" => Ok(Self::Resolved),
            "cancelled" => Ok(Self::Cancelled),
            "expired" => Ok(Self::Expired),
            other => Err(anyhow!(
                "unknown CLI runtime pending request status `{other}`"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeThreadBindingRecord {
    pub thread_id: String,
    pub workspace_id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub native_thread_id: String,
    pub native_session_id: Option<String>,
    pub native_root_thread_id: Option<String>,
    pub native_cwd: Option<String>,
    pub native_model: Option<String>,
    pub resume_cursor_json: String,
    pub status: String,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCliRuntimeThreadBinding {
    pub thread_id: String,
    pub workspace_id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub native_thread_id: String,
    pub native_session_id: Option<String>,
    pub native_root_thread_id: Option<String>,
    pub native_cwd: Option<String>,
    pub native_model: Option<String>,
    pub resume_cursor_json: String,
    pub status: String,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeTurnBindingRecord {
    pub turn_id: String,
    pub thread_id: String,
    pub workspace_id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub native_thread_id: String,
    pub native_turn_id: Option<String>,
    pub request_id: Option<String>,
    pub status: String,
    pub model: Option<String>,
    pub cwd: Option<String>,
    pub sandbox_json: Option<String>,
    pub approval_policy: Option<String>,
    pub input_mapping_json: String,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCliRuntimeTurnBinding {
    pub turn_id: String,
    pub thread_id: String,
    pub workspace_id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub native_thread_id: String,
    pub native_turn_id: Option<String>,
    pub request_id: Option<String>,
    pub status: String,
    pub model: Option<String>,
    pub cwd: Option<String>,
    pub sandbox_json: Option<String>,
    pub approval_policy: Option<String>,
    pub input_mapping_json: String,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliRuntimeTurnAttemptStatus {
    Starting,
    Running,
    Completed,
    Failed,
    Interrupted,
}

impl CliRuntimeTurnAttemptStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        }
    }

    pub const fn is_active(self) -> bool {
        matches!(self, Self::Starting | Self::Running)
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "starting" => Ok(Self::Starting),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "interrupted" => Ok(Self::Interrupted),
            other => Err(anyhow!("unknown CLI runtime turn attempt status `{other}`")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeTurnAttemptRecord {
    pub id: String,
    pub turn_id: String,
    pub attempt_index: u32,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub native_thread_id: String,
    pub native_turn_id: Option<String>,
    pub recovery_job_id: Option<String>,
    pub recovery_attempt_id: Option<String>,
    pub recovery_confirmed_at: Option<DateTimeWithTimeZone>,
    pub execution_window_index: Option<u32>,
    pub status: CliRuntimeTurnAttemptStatus,
    pub failure_reason: Option<String>,
    pub started_at: Option<DateTimeWithTimeZone>,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCliRuntimeTurnAttempt {
    pub id: String,
    pub turn_id: String,
    pub attempt_index: u32,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub native_thread_id: String,
    pub native_turn_id: Option<String>,
    pub recovery_job_id: Option<String>,
    pub recovery_attempt_id: Option<String>,
    pub execution_window_index: Option<u32>,
    pub status: CliRuntimeTurnAttemptStatus,
    pub failure_reason: Option<String>,
    pub started_at: Option<DateTimeWithTimeZone>,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CliRuntimeTurnBindingListFilter {
    pub workspace_id: Option<String>,
    pub runtime_id: Option<String>,
    pub runtime_kind: Option<String>,
    pub thread_id: Option<String>,
    pub statuses: Vec<String>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimePendingRequestRecord {
    pub request_id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: Option<String>,
    pub native_thread_id: Option<String>,
    pub native_turn_id: Option<String>,
    pub native_item_id: Option<String>,
    pub request_kind: String,
    pub payload_json: String,
    pub status: CliRuntimePendingRequestStatus,
    pub response_json: Option<String>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
    pub resolved_at: Option<DateTimeWithTimeZone>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCliRuntimePendingRequest {
    pub request_id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: Option<String>,
    pub native_thread_id: Option<String>,
    pub native_turn_id: Option<String>,
    pub native_item_id: Option<String>,
    pub request_kind: String,
    pub payload_json: String,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveCliRuntimePendingRequest {
    pub request_id: String,
    pub status: CliRuntimePendingRequestStatus,
    pub response_json: Option<String>,
    pub updated_at: DateTimeWithTimeZone,
    pub resolved_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CliRuntimePendingRequestListFilter {
    pub workspace_id: Option<String>,
    pub runtime_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub status: Option<CliRuntimePendingRequestStatus>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeNativeEventRecord {
    pub id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub workspace_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub native_thread_id: Option<String>,
    pub native_turn_id: Option<String>,
    pub native_method: String,
    pub payload_redacted_json: String,
    pub sequence: i64,
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCliRuntimeNativeEvent {
    pub id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub workspace_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub native_thread_id: Option<String>,
    pub native_turn_id: Option<String>,
    pub native_method: String,
    pub payload_redacted_json: String,
    pub sequence: i64,
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CliRuntimeNativeEventListFilter {
    pub runtime_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub native_thread_id: Option<String>,
    pub native_turn_id: Option<String>,
    pub limit: Option<u64>,
}

pub fn serialize_cli_runtime_json<T: Serialize + ?Sized>(value: &T) -> Result<String> {
    serde_json::to_string(value).context("failed to serialize CLI runtime JSON payload")
}

pub fn deserialize_cli_runtime_json<T: DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_str(value).context("failed to deserialize CLI runtime JSON payload")
}

pub async fn upsert_thread_binding<C: ConnectionTrait>(
    db: &C,
    binding: NewCliRuntimeThreadBinding,
) -> Result<CliRuntimeThreadBindingRecord> {
    let thread_id = binding.thread_id.clone();
    thread_cli_runtime_binding::Entity::insert(active_thread_binding_from_new(binding))
        .on_conflict(
            OnConflict::column(thread_cli_runtime_binding::Column::ThreadId)
                .update_columns([
                    thread_cli_runtime_binding::Column::WorkspaceId,
                    thread_cli_runtime_binding::Column::RuntimeId,
                    thread_cli_runtime_binding::Column::RuntimeKind,
                    thread_cli_runtime_binding::Column::NativeThreadId,
                    thread_cli_runtime_binding::Column::NativeSessionId,
                    thread_cli_runtime_binding::Column::NativeRootThreadId,
                    thread_cli_runtime_binding::Column::NativeCwd,
                    thread_cli_runtime_binding::Column::NativeModel,
                    thread_cli_runtime_binding::Column::ResumeCursorJson,
                    thread_cli_runtime_binding::Column::Status,
                    thread_cli_runtime_binding::Column::UpdatedAt,
                ])
                .to_owned(),
        )
        .exec(db)
        .await
        .context("failed to upsert CLI runtime thread binding")?;

    find_thread_binding(db, thread_id.as_str())
        .await?
        .context("upserted CLI runtime thread binding is missing")
}

pub async fn find_thread_binding<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Option<CliRuntimeThreadBindingRecord>> {
    thread_cli_runtime_binding::Entity::find_by_id(thread_id.to_owned())
        .one(db)
        .await
        .context("failed to query CLI runtime thread binding")?
        .map(thread_binding_record_from_model)
        .transpose()
}

pub async fn find_thread_binding_by_native_thread<C: ConnectionTrait>(
    db: &C,
    runtime_id: &str,
    native_thread_id: &str,
) -> Result<Option<CliRuntimeThreadBindingRecord>> {
    thread_cli_runtime_binding::Entity::find()
        .filter(thread_cli_runtime_binding::Column::RuntimeId.eq(runtime_id.to_owned()))
        .filter(thread_cli_runtime_binding::Column::NativeThreadId.eq(native_thread_id.to_owned()))
        .one(db)
        .await
        .context("failed to query CLI runtime thread binding by native thread")?
        .map(thread_binding_record_from_model)
        .transpose()
}

pub async fn list_thread_bindings_for_runtime<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    runtime_id: &str,
) -> Result<Vec<CliRuntimeThreadBindingRecord>> {
    thread_cli_runtime_binding::Entity::find()
        .filter(thread_cli_runtime_binding::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_cli_runtime_binding::Column::RuntimeId.eq(runtime_id.to_owned()))
        .order_by_asc(thread_cli_runtime_binding::Column::UpdatedAt)
        .all(db)
        .await
        .context("failed to list CLI runtime thread bindings")?
        .into_iter()
        .map(thread_binding_record_from_model)
        .collect()
}

pub async fn upsert_turn_binding<C: ConnectionTrait>(
    db: &C,
    binding: NewCliRuntimeTurnBinding,
) -> Result<CliRuntimeTurnBindingRecord> {
    let turn_id = binding.turn_id.clone();
    turn_cli_runtime_binding::Entity::insert(active_turn_binding_from_new(binding))
        .on_conflict(
            OnConflict::column(turn_cli_runtime_binding::Column::TurnId)
                .update_columns([
                    turn_cli_runtime_binding::Column::ThreadId,
                    turn_cli_runtime_binding::Column::WorkspaceId,
                    turn_cli_runtime_binding::Column::RuntimeId,
                    turn_cli_runtime_binding::Column::RuntimeKind,
                    turn_cli_runtime_binding::Column::NativeThreadId,
                    turn_cli_runtime_binding::Column::NativeTurnId,
                    turn_cli_runtime_binding::Column::RequestId,
                    turn_cli_runtime_binding::Column::Status,
                    turn_cli_runtime_binding::Column::Model,
                    turn_cli_runtime_binding::Column::Cwd,
                    turn_cli_runtime_binding::Column::SandboxJson,
                    turn_cli_runtime_binding::Column::ApprovalPolicy,
                    turn_cli_runtime_binding::Column::InputMappingJson,
                    turn_cli_runtime_binding::Column::UpdatedAt,
                ])
                .to_owned(),
        )
        .exec(db)
        .await
        .context("failed to upsert CLI runtime turn binding")?;

    find_turn_binding(db, turn_id.as_str())
        .await?
        .context("upserted CLI runtime turn binding is missing")
}

pub async fn find_turn_binding<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<CliRuntimeTurnBindingRecord>> {
    turn_cli_runtime_binding::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .context("failed to query CLI runtime turn binding")?
        .map(turn_binding_record_from_model)
        .transpose()
}

pub async fn find_turn_binding_by_request<C: ConnectionTrait>(
    db: &C,
    request_id: &str,
) -> Result<Option<CliRuntimeTurnBindingRecord>> {
    turn_cli_runtime_binding::Entity::find()
        .filter(turn_cli_runtime_binding::Column::RequestId.eq(request_id.to_owned()))
        .one(db)
        .await
        .context("failed to query CLI runtime turn binding by request")?
        .map(turn_binding_record_from_model)
        .transpose()
}

pub async fn find_turn_binding_by_native_turn<C: ConnectionTrait>(
    db: &C,
    runtime_id: &str,
    native_turn_id: &str,
) -> Result<Option<CliRuntimeTurnBindingRecord>> {
    turn_cli_runtime_binding::Entity::find()
        .filter(turn_cli_runtime_binding::Column::RuntimeId.eq(runtime_id.to_owned()))
        .filter(turn_cli_runtime_binding::Column::NativeTurnId.eq(native_turn_id.to_owned()))
        .one(db)
        .await
        .context("failed to query CLI runtime turn binding by native turn")?
        .map(turn_binding_record_from_model)
        .transpose()
}

pub async fn list_turn_bindings_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
    turn_cli_runtime_binding::Entity::find()
        .filter(turn_cli_runtime_binding::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_asc(turn_cli_runtime_binding::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list CLI runtime turn bindings")?
        .into_iter()
        .map(turn_binding_record_from_model)
        .collect()
}

pub async fn list_turn_bindings<C: ConnectionTrait>(
    db: &C,
    filter: CliRuntimeTurnBindingListFilter,
) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
    let mut query = turn_cli_runtime_binding::Entity::find();
    if let Some(workspace_id) = filter.workspace_id {
        query = query.filter(turn_cli_runtime_binding::Column::WorkspaceId.eq(workspace_id));
    }
    if let Some(runtime_id) = filter.runtime_id {
        query = query.filter(turn_cli_runtime_binding::Column::RuntimeId.eq(runtime_id));
    }
    if let Some(runtime_kind) = filter.runtime_kind {
        query = query.filter(turn_cli_runtime_binding::Column::RuntimeKind.eq(runtime_kind));
    }
    if let Some(thread_id) = filter.thread_id {
        query = query.filter(turn_cli_runtime_binding::Column::ThreadId.eq(thread_id));
    }
    if !filter.statuses.is_empty() {
        query = query.filter(turn_cli_runtime_binding::Column::Status.is_in(filter.statuses));
    }
    if let Some(limit) = filter.limit {
        query = query.limit(limit);
    }

    query
        .order_by_asc(turn_cli_runtime_binding::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list CLI runtime turn bindings")?
        .into_iter()
        .map(turn_binding_record_from_model)
        .collect()
}

pub async fn create_turn_attempt<C: ConnectionTrait>(
    db: &C,
    attempt: NewCliRuntimeTurnAttempt,
) -> Result<CliRuntimeTurnAttemptRecord> {
    validate_new_turn_attempt(&attempt)?;
    let id = attempt.id.clone();
    turn_cli_runtime_attempt::Entity::insert(active_turn_attempt_from_new(attempt))
        .exec(db)
        .await
        .context("failed to insert CLI runtime turn attempt")?;

    find_turn_attempt(db, id.as_str())
        .await?
        .context("inserted CLI runtime turn attempt is missing")
}

pub async fn find_turn_attempt<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<CliRuntimeTurnAttemptRecord>> {
    turn_cli_runtime_attempt::Entity::find_by_id(id.to_owned())
        .one(db)
        .await
        .context("failed to query CLI runtime turn attempt")?
        .map(turn_attempt_record_from_model)
        .transpose()
}

pub async fn find_turn_attempt_by_native_turn<C: ConnectionTrait>(
    db: &C,
    runtime_id: &str,
    native_turn_id: &str,
) -> Result<Option<CliRuntimeTurnAttemptRecord>> {
    turn_cli_runtime_attempt::Entity::find()
        .filter(turn_cli_runtime_attempt::Column::RuntimeId.eq(runtime_id.to_owned()))
        .filter(turn_cli_runtime_attempt::Column::NativeTurnId.eq(native_turn_id.to_owned()))
        .one(db)
        .await
        .context("failed to query CLI runtime turn attempt by native turn")?
        .map(turn_attempt_record_from_model)
        .transpose()
}

pub async fn find_turn_attempt_by_recovery_attempt<C: ConnectionTrait>(
    db: &C,
    recovery_attempt_id: &str,
) -> Result<Option<CliRuntimeTurnAttemptRecord>> {
    turn_cli_runtime_attempt::Entity::find()
        .filter(
            turn_cli_runtime_attempt::Column::RecoveryAttemptId.eq(recovery_attempt_id.to_owned()),
        )
        .one(db)
        .await
        .context("failed to query CLI runtime turn attempt by recovery attempt")?
        .map(turn_attempt_record_from_model)
        .transpose()
}

pub async fn latest_turn_attempt<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<CliRuntimeTurnAttemptRecord>> {
    turn_cli_runtime_attempt::Entity::find()
        .filter(turn_cli_runtime_attempt::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_desc(turn_cli_runtime_attempt::Column::AttemptIndex)
        .one(db)
        .await
        .context("failed to query latest CLI runtime turn attempt")?
        .map(turn_attempt_record_from_model)
        .transpose()
}

pub async fn mark_turn_attempt_running<C: ConnectionTrait>(
    db: &C,
    id: &str,
    native_turn_id: &str,
    started_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let result = turn_cli_runtime_attempt::Entity::update_many()
        .col_expr(
            turn_cli_runtime_attempt::Column::NativeTurnId,
            sea_orm::sea_query::Expr::value(Some(native_turn_id.to_owned())),
        )
        .col_expr(
            turn_cli_runtime_attempt::Column::Status,
            sea_orm::sea_query::Expr::value(CliRuntimeTurnAttemptStatus::Running.as_str()),
        )
        .col_expr(
            turn_cli_runtime_attempt::Column::StartedAt,
            sea_orm::sea_query::Expr::value(Some(started_at)),
        )
        .col_expr(
            turn_cli_runtime_attempt::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(started_at),
        )
        .filter(turn_cli_runtime_attempt::Column::Id.eq(id.to_owned()))
        .filter(
            turn_cli_runtime_attempt::Column::Status
                .eq(CliRuntimeTurnAttemptStatus::Starting.as_str()),
        )
        .exec(db)
        .await
        .context("failed to mark CLI runtime turn attempt running")?;
    Ok(result.rows_affected == 1)
}

pub async fn mark_turn_attempt_terminal<C: ConnectionTrait>(
    db: &C,
    id: &str,
    status: CliRuntimeTurnAttemptStatus,
    failure_reason: Option<String>,
    completed_at: DateTimeWithTimeZone,
) -> Result<bool> {
    if status.is_active() {
        return Err(anyhow!(
            "CLI runtime terminal attempt status cannot be `{}`",
            status.as_str()
        ));
    }
    let result = turn_cli_runtime_attempt::Entity::update_many()
        .col_expr(
            turn_cli_runtime_attempt::Column::Status,
            sea_orm::sea_query::Expr::value(status.as_str()),
        )
        .col_expr(
            turn_cli_runtime_attempt::Column::FailureReason,
            sea_orm::sea_query::Expr::value(failure_reason),
        )
        .col_expr(
            turn_cli_runtime_attempt::Column::CompletedAt,
            sea_orm::sea_query::Expr::value(Some(completed_at)),
        )
        .col_expr(
            turn_cli_runtime_attempt::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(completed_at),
        )
        .filter(turn_cli_runtime_attempt::Column::Id.eq(id.to_owned()))
        .filter(turn_cli_runtime_attempt::Column::Status.is_in([
            CliRuntimeTurnAttemptStatus::Starting.as_str(),
            CliRuntimeTurnAttemptStatus::Running.as_str(),
        ]))
        .exec(db)
        .await
        .context("failed to mark CLI runtime turn attempt terminal")?;
    Ok(result.rows_affected == 1)
}

pub async fn mark_turn_attempt_recovery_confirmed<C: ConnectionTrait>(
    db: &C,
    id: &str,
    recovery_attempt_id: &str,
    confirmed_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let result = turn_cli_runtime_attempt::Entity::update_many()
        .col_expr(
            turn_cli_runtime_attempt::Column::RecoveryConfirmedAt,
            sea_orm::sea_query::Expr::value(Some(confirmed_at)),
        )
        .col_expr(
            turn_cli_runtime_attempt::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(confirmed_at),
        )
        .filter(turn_cli_runtime_attempt::Column::Id.eq(id.to_owned()))
        .filter(
            turn_cli_runtime_attempt::Column::RecoveryAttemptId.eq(recovery_attempt_id.to_owned()),
        )
        .filter(turn_cli_runtime_attempt::Column::RecoveryConfirmedAt.is_null())
        .exec(db)
        .await
        .context("failed to mark CLI runtime recovery confirmed")?;
    Ok(result.rows_affected == 1)
}

pub async fn create_pending_request<C: ConnectionTrait>(
    db: &C,
    request: NewCliRuntimePendingRequest,
) -> Result<CliRuntimePendingRequestRecord> {
    if let Some(existing) = find_pending_request(db, request.request_id.as_str()).await? {
        if existing.status != CliRuntimePendingRequestStatus::Pending {
            return Ok(existing);
        }
        return update_pending_request_metadata(db, request).await;
    }

    let request_id = request.request_id.clone();
    cli_runtime_pending_request::Entity::insert(active_pending_request_from_new(request))
        .exec(db)
        .await
        .context("failed to insert CLI runtime pending request")?;

    find_pending_request(db, request_id.as_str())
        .await?
        .context("inserted CLI runtime pending request is missing")
}

pub async fn open_pending_request<C: ConnectionTrait>(
    db: &C,
    request: NewCliRuntimePendingRequest,
) -> Result<CliRuntimePendingRequestRecord> {
    create_pending_request(db, request).await
}

pub async fn find_pending_request<C: ConnectionTrait>(
    db: &C,
    request_id: &str,
) -> Result<Option<CliRuntimePendingRequestRecord>> {
    cli_runtime_pending_request::Entity::find_by_id(request_id.to_owned())
        .one(db)
        .await
        .context("failed to query CLI runtime pending request")?
        .map(pending_request_record_from_model)
        .transpose()
}

pub async fn list_pending_requests<C: ConnectionTrait>(
    db: &C,
    filter: CliRuntimePendingRequestListFilter,
) -> Result<Vec<CliRuntimePendingRequestRecord>> {
    let mut query = cli_runtime_pending_request::Entity::find();
    if let Some(workspace_id) = filter.workspace_id {
        query = query.filter(cli_runtime_pending_request::Column::WorkspaceId.eq(workspace_id));
    }
    if let Some(runtime_id) = filter.runtime_id {
        query = query.filter(cli_runtime_pending_request::Column::RuntimeId.eq(runtime_id));
    }
    if let Some(thread_id) = filter.thread_id {
        query = query.filter(cli_runtime_pending_request::Column::ThreadId.eq(thread_id));
    }
    if let Some(turn_id) = filter.turn_id {
        query = query.filter(cli_runtime_pending_request::Column::TurnId.eq(turn_id));
    }
    if let Some(status) = filter.status {
        query = query.filter(cli_runtime_pending_request::Column::Status.eq(status.as_str()));
    }
    if let Some(limit) = filter.limit {
        query = query.limit(limit);
    }

    query
        .order_by_asc(cli_runtime_pending_request::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list CLI runtime pending requests")?
        .into_iter()
        .map(pending_request_record_from_model)
        .collect()
}

pub async fn resolve_pending_request<C: ConnectionTrait>(
    db: &C,
    resolution: ResolveCliRuntimePendingRequest,
) -> Result<Option<CliRuntimePendingRequestRecord>> {
    if resolution.status == CliRuntimePendingRequestStatus::Pending {
        return Err(anyhow!(
            "CLI runtime pending request resolution cannot keep pending status"
        ));
    }

    let Some(existing) = find_pending_request(db, resolution.request_id.as_str()).await? else {
        return Ok(None);
    };
    if existing.status != CliRuntimePendingRequestStatus::Pending {
        return Err(anyhow!(
            "CLI runtime pending request `{}` is already `{}`",
            existing.request_id,
            existing.status.as_str()
        ));
    }

    cli_runtime_pending_request::Entity::update(cli_runtime_pending_request::ActiveModel {
        request_id: Set(resolution.request_id.clone()),
        status: Set(resolution.status.as_str().to_owned()),
        response_json: Set(resolution.response_json),
        updated_at: Set(resolution.updated_at),
        resolved_at: Set(Some(resolution.resolved_at)),
        ..Default::default()
    })
    .exec(db)
    .await
    .context("failed to resolve CLI runtime pending request")?;

    find_pending_request(db, resolution.request_id.as_str()).await
}

pub async fn cancel_pending_request<C: ConnectionTrait>(
    db: &C,
    request_id: String,
    response_json: Option<String>,
    updated_at: DateTimeWithTimeZone,
) -> Result<Option<CliRuntimePendingRequestRecord>> {
    resolve_pending_request(
        db,
        ResolveCliRuntimePendingRequest {
            request_id,
            status: CliRuntimePendingRequestStatus::Cancelled,
            response_json,
            updated_at,
            resolved_at: updated_at,
        },
    )
    .await
}

pub async fn expire_pending_request<C: ConnectionTrait>(
    db: &C,
    request_id: String,
    response_json: Option<String>,
    updated_at: DateTimeWithTimeZone,
) -> Result<Option<CliRuntimePendingRequestRecord>> {
    resolve_pending_request(
        db,
        ResolveCliRuntimePendingRequest {
            request_id,
            status: CliRuntimePendingRequestStatus::Expired,
            response_json,
            updated_at,
            resolved_at: updated_at,
        },
    )
    .await
}

pub async fn append_native_event<C: ConnectionTrait>(
    db: &C,
    event: NewCliRuntimeNativeEvent,
) -> Result<CliRuntimeNativeEventRecord> {
    let id = event.id.clone();
    cli_runtime_native_event::Entity::insert(active_native_event_from_new(event))
        .exec(db)
        .await
        .context("failed to append CLI runtime native event")?;

    find_native_event(db, id.as_str())
        .await?
        .context("inserted CLI runtime native event is missing")
}

pub async fn find_native_event<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<CliRuntimeNativeEventRecord>> {
    cli_runtime_native_event::Entity::find_by_id(id.to_owned())
        .one(db)
        .await
        .context("failed to query CLI runtime native event")?
        .map(native_event_record_from_model)
        .transpose()
}

pub async fn list_native_events<C: ConnectionTrait>(
    db: &C,
    filter: CliRuntimeNativeEventListFilter,
) -> Result<Vec<CliRuntimeNativeEventRecord>> {
    let query = filter_native_events(cli_runtime_native_event::Entity::find(), filter);
    query
        .order_by_asc(cli_runtime_native_event::Column::Sequence)
        .all(db)
        .await
        .context("failed to list CLI runtime native events")?
        .into_iter()
        .map(native_event_record_from_model)
        .collect()
}

pub async fn latest_native_event<C: ConnectionTrait>(
    db: &C,
    filter: CliRuntimeNativeEventListFilter,
) -> Result<Option<CliRuntimeNativeEventRecord>> {
    filter_native_events(cli_runtime_native_event::Entity::find(), filter)
        .order_by_desc(cli_runtime_native_event::Column::Sequence)
        .one(db)
        .await
        .context("failed to query latest CLI runtime native event")?
        .map(native_event_record_from_model)
        .transpose()
}

fn filter_native_events(
    mut query: sea_orm::Select<cli_runtime_native_event::Entity>,
    filter: CliRuntimeNativeEventListFilter,
) -> sea_orm::Select<cli_runtime_native_event::Entity> {
    if let Some(runtime_id) = filter.runtime_id {
        query = query.filter(cli_runtime_native_event::Column::RuntimeId.eq(runtime_id));
    }
    if let Some(thread_id) = filter.thread_id {
        query = query.filter(cli_runtime_native_event::Column::ThreadId.eq(thread_id));
    }
    if let Some(turn_id) = filter.turn_id {
        query = query.filter(cli_runtime_native_event::Column::TurnId.eq(turn_id));
    }
    if let Some(native_thread_id) = filter.native_thread_id {
        query = query.filter(cli_runtime_native_event::Column::NativeThreadId.eq(native_thread_id));
    }
    if let Some(native_turn_id) = filter.native_turn_id {
        query = query.filter(cli_runtime_native_event::Column::NativeTurnId.eq(native_turn_id));
    }
    if let Some(limit) = filter.limit {
        query = query.limit(limit);
    }
    query
}

async fn update_pending_request_metadata<C: ConnectionTrait>(
    db: &C,
    request: NewCliRuntimePendingRequest,
) -> Result<CliRuntimePendingRequestRecord> {
    cli_runtime_pending_request::Entity::update(cli_runtime_pending_request::ActiveModel {
        request_id: Set(request.request_id.clone()),
        runtime_id: Set(request.runtime_id),
        runtime_kind: Set(request.runtime_kind),
        workspace_id: Set(request.workspace_id),
        thread_id: Set(request.thread_id),
        turn_id: Set(request.turn_id),
        native_thread_id: Set(request.native_thread_id),
        native_turn_id: Set(request.native_turn_id),
        native_item_id: Set(request.native_item_id),
        request_kind: Set(request.request_kind),
        payload_json: Set(request.payload_json),
        status: Set(CliRuntimePendingRequestStatus::Pending.as_str().to_owned()),
        response_json: Set(None),
        updated_at: Set(request.updated_at),
        resolved_at: Set(None),
        ..Default::default()
    })
    .exec(db)
    .await
    .context("failed to update CLI runtime pending request metadata")?;

    find_pending_request(db, request.request_id.as_str())
        .await?
        .context("updated CLI runtime pending request is missing")
}

fn active_thread_binding_from_new(
    binding: NewCliRuntimeThreadBinding,
) -> thread_cli_runtime_binding::ActiveModel {
    thread_cli_runtime_binding::ActiveModel {
        thread_id: Set(binding.thread_id),
        workspace_id: Set(binding.workspace_id),
        runtime_id: Set(binding.runtime_id),
        runtime_kind: Set(binding.runtime_kind),
        native_thread_id: Set(binding.native_thread_id),
        native_session_id: Set(binding.native_session_id),
        native_root_thread_id: Set(binding.native_root_thread_id),
        native_cwd: Set(binding.native_cwd),
        native_model: Set(binding.native_model),
        resume_cursor_json: Set(binding.resume_cursor_json),
        status: Set(binding.status),
        created_at: Set(binding.created_at),
        updated_at: Set(binding.updated_at),
    }
}

fn active_turn_binding_from_new(
    binding: NewCliRuntimeTurnBinding,
) -> turn_cli_runtime_binding::ActiveModel {
    turn_cli_runtime_binding::ActiveModel {
        turn_id: Set(binding.turn_id),
        thread_id: Set(binding.thread_id),
        workspace_id: Set(binding.workspace_id),
        runtime_id: Set(binding.runtime_id),
        runtime_kind: Set(binding.runtime_kind),
        native_thread_id: Set(binding.native_thread_id),
        native_turn_id: Set(binding.native_turn_id),
        request_id: Set(binding.request_id),
        status: Set(binding.status),
        model: Set(binding.model),
        cwd: Set(binding.cwd),
        sandbox_json: Set(binding.sandbox_json),
        approval_policy: Set(binding.approval_policy),
        input_mapping_json: Set(binding.input_mapping_json),
        created_at: Set(binding.created_at),
        updated_at: Set(binding.updated_at),
    }
}

fn active_turn_attempt_from_new(
    attempt: NewCliRuntimeTurnAttempt,
) -> turn_cli_runtime_attempt::ActiveModel {
    turn_cli_runtime_attempt::ActiveModel {
        id: Set(attempt.id),
        turn_id: Set(attempt.turn_id),
        attempt_index: Set(i64::from(attempt.attempt_index)),
        runtime_id: Set(attempt.runtime_id),
        runtime_kind: Set(attempt.runtime_kind),
        native_thread_id: Set(attempt.native_thread_id),
        native_turn_id: Set(attempt.native_turn_id),
        recovery_job_id: Set(attempt.recovery_job_id),
        recovery_attempt_id: Set(attempt.recovery_attempt_id),
        recovery_confirmed_at: Set(None),
        execution_window_index: Set(attempt.execution_window_index.map(i64::from)),
        status: Set(attempt.status.as_str().to_owned()),
        failure_reason: Set(attempt.failure_reason),
        started_at: Set(attempt.started_at),
        completed_at: Set(attempt.completed_at),
        created_at: Set(attempt.created_at),
        updated_at: Set(attempt.updated_at),
    }
}

fn validate_new_turn_attempt(attempt: &NewCliRuntimeTurnAttempt) -> Result<()> {
    if attempt.attempt_index == 0 {
        return Err(anyhow!("CLI runtime turn attempt index must be positive"));
    }
    if attempt.execution_window_index == Some(0) {
        return Err(anyhow!(
            "CLI runtime turn attempt execution window index must be positive"
        ));
    }
    if attempt.recovery_job_id.is_some() != attempt.recovery_attempt_id.is_some() {
        return Err(anyhow!(
            "CLI runtime turn attempt recovery job and attempt ids must both be present or absent"
        ));
    }
    if attempt.status == CliRuntimeTurnAttemptStatus::Running
        && attempt.native_turn_id.as_deref().is_none_or(str::is_empty)
    {
        return Err(anyhow!(
            "running CLI runtime turn attempt must have a native turn id"
        ));
    }
    if attempt.status == CliRuntimeTurnAttemptStatus::Starting && attempt.native_turn_id.is_some() {
        return Err(anyhow!(
            "starting CLI runtime turn attempt cannot have a native turn id"
        ));
    }
    Ok(())
}

fn active_pending_request_from_new(
    request: NewCliRuntimePendingRequest,
) -> cli_runtime_pending_request::ActiveModel {
    cli_runtime_pending_request::ActiveModel {
        request_id: Set(request.request_id),
        runtime_id: Set(request.runtime_id),
        runtime_kind: Set(request.runtime_kind),
        workspace_id: Set(request.workspace_id),
        thread_id: Set(request.thread_id),
        turn_id: Set(request.turn_id),
        native_thread_id: Set(request.native_thread_id),
        native_turn_id: Set(request.native_turn_id),
        native_item_id: Set(request.native_item_id),
        request_kind: Set(request.request_kind),
        payload_json: Set(request.payload_json),
        status: Set(CliRuntimePendingRequestStatus::Pending.as_str().to_owned()),
        response_json: Set(None),
        created_at: Set(request.created_at),
        updated_at: Set(request.updated_at),
        resolved_at: Set(None),
    }
}

fn active_native_event_from_new(
    event: NewCliRuntimeNativeEvent,
) -> cli_runtime_native_event::ActiveModel {
    cli_runtime_native_event::ActiveModel {
        id: Set(event.id),
        runtime_id: Set(event.runtime_id),
        runtime_kind: Set(event.runtime_kind),
        workspace_id: Set(event.workspace_id),
        thread_id: Set(event.thread_id),
        turn_id: Set(event.turn_id),
        native_thread_id: Set(event.native_thread_id),
        native_turn_id: Set(event.native_turn_id),
        native_method: Set(event.native_method),
        payload_redacted_json: Set(event.payload_redacted_json),
        sequence: Set(event.sequence),
        created_at: Set(event.created_at),
    }
}

fn thread_binding_record_from_model(
    model: thread_cli_runtime_binding::Model,
) -> Result<CliRuntimeThreadBindingRecord> {
    Ok(CliRuntimeThreadBindingRecord {
        thread_id: model.thread_id,
        workspace_id: model.workspace_id,
        runtime_id: model.runtime_id,
        runtime_kind: model.runtime_kind,
        native_thread_id: model.native_thread_id,
        native_session_id: model.native_session_id,
        native_root_thread_id: model.native_root_thread_id,
        native_cwd: model.native_cwd,
        native_model: model.native_model,
        resume_cursor_json: model.resume_cursor_json,
        status: model.status,
        created_at: model.created_at,
        updated_at: model.updated_at,
    })
}

fn turn_binding_record_from_model(
    model: turn_cli_runtime_binding::Model,
) -> Result<CliRuntimeTurnBindingRecord> {
    Ok(CliRuntimeTurnBindingRecord {
        turn_id: model.turn_id,
        thread_id: model.thread_id,
        workspace_id: model.workspace_id,
        runtime_id: model.runtime_id,
        runtime_kind: model.runtime_kind,
        native_thread_id: model.native_thread_id,
        native_turn_id: model.native_turn_id,
        request_id: model.request_id,
        status: model.status,
        model: model.model,
        cwd: model.cwd,
        sandbox_json: model.sandbox_json,
        approval_policy: model.approval_policy,
        input_mapping_json: model.input_mapping_json,
        created_at: model.created_at,
        updated_at: model.updated_at,
    })
}

fn turn_attempt_record_from_model(
    model: turn_cli_runtime_attempt::Model,
) -> Result<CliRuntimeTurnAttemptRecord> {
    Ok(CliRuntimeTurnAttemptRecord {
        id: model.id,
        turn_id: model.turn_id,
        attempt_index: u32::try_from(model.attempt_index)
            .context("CLI runtime turn attempt index is outside u32")?,
        runtime_id: model.runtime_id,
        runtime_kind: model.runtime_kind,
        native_thread_id: model.native_thread_id,
        native_turn_id: model.native_turn_id,
        recovery_job_id: model.recovery_job_id,
        recovery_attempt_id: model.recovery_attempt_id,
        recovery_confirmed_at: model.recovery_confirmed_at,
        execution_window_index: model
            .execution_window_index
            .map(u32::try_from)
            .transpose()
            .context("CLI runtime execution window index is outside u32")?,
        status: CliRuntimeTurnAttemptStatus::from_db(model.status.as_str())?,
        failure_reason: model.failure_reason,
        started_at: model.started_at,
        completed_at: model.completed_at,
        created_at: model.created_at,
        updated_at: model.updated_at,
    })
}

fn pending_request_record_from_model(
    model: cli_runtime_pending_request::Model,
) -> Result<CliRuntimePendingRequestRecord> {
    Ok(CliRuntimePendingRequestRecord {
        request_id: model.request_id,
        runtime_id: model.runtime_id,
        runtime_kind: model.runtime_kind,
        workspace_id: model.workspace_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        native_thread_id: model.native_thread_id,
        native_turn_id: model.native_turn_id,
        native_item_id: model.native_item_id,
        request_kind: model.request_kind,
        payload_json: model.payload_json,
        status: CliRuntimePendingRequestStatus::from_db(model.status.as_str())?,
        response_json: model.response_json,
        created_at: model.created_at,
        updated_at: model.updated_at,
        resolved_at: model.resolved_at,
    })
}

fn native_event_record_from_model(
    model: cli_runtime_native_event::Model,
) -> Result<CliRuntimeNativeEventRecord> {
    Ok(CliRuntimeNativeEventRecord {
        id: model.id,
        runtime_id: model.runtime_id,
        runtime_kind: model.runtime_kind,
        workspace_id: model.workspace_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        native_thread_id: model.native_thread_id,
        native_turn_id: model.native_turn_id,
        native_method: model.native_method,
        payload_redacted_json: model.payload_redacted_json,
        sequence: model.sequence,
        created_at: model.created_at,
    })
}
