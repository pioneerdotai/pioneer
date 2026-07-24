use anyhow::{Context, Result, anyhow};
use pioneer_entity::{
    cli_runtime_native_event, cli_runtime_pending_request, thread_cli_runtime_binding,
    turn_cli_runtime_attempt, turn_cli_runtime_binding, turn_cli_runtime_execution_segment,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, Set,
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
    pub mcp: Option<CliRuntimeThreadMcpMetadata>,
    pub provider_session: Option<CliRuntimeProviderSessionBinding>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliRuntimeProviderSessionLifecycle {
    Prepared,
    Verified,
    Invalid,
}

impl CliRuntimeProviderSessionLifecycle {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Verified => "verified",
            Self::Invalid => "invalid",
        }
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "prepared" => Ok(Self::Prepared),
            "verified" => Ok(Self::Verified),
            "invalid" => Ok(Self::Invalid),
            other => Err(anyhow!(
                "unknown CLI provider session lifecycle state `{other}`"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeProviderSessionBinding {
    pub provider_session_id: String,
    pub lifecycle: CliRuntimeProviderSessionLifecycle,
    pub last_verified_process_generation: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareClaudeProviderSessionBinding {
    pub thread_binding: NewCliRuntimeThreadBinding,
    pub proposed_provider_session_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedClaudeProviderSessionMode {
    New,
    Resume,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedClaudeProviderSessionBinding {
    pub binding: CliRuntimeThreadBindingRecord,
    pub mode: PreparedClaudeProviderSessionMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeThreadMcpMetadata {
    pub adapter_kind: String,
    pub manifest_hash: String,
    pub projection_fingerprint: String,
    pub provider_contract_fingerprint: String,
    pub isolation_contract_fingerprint: String,
    pub session_generation: i64,
    pub provider_session_id: Option<String>,
    pub provider_session_lifecycle_state: Option<String>,
    pub provider_session_last_verified_process_generation: Option<i64>,
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
    pub continuation_thread_id: String,
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
    pub mcp: Option<CliRuntimeTurnMcpMetadata>,
    pub native_goal_status: Option<String>,
    pub native_goal_turn_id: Option<String>,
    pub native_goal_observed_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeTurnMcpMetadata {
    pub adapter_kind: String,
    pub manifest_hash: String,
    pub projection_fingerprint: String,
    pub provider_contract_fingerprint: String,
    pub isolation_contract_fingerprint: String,
    pub session_generation: i64,
    pub projection_activation_generation: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCliRuntimeTurnBinding {
    pub turn_id: String,
    pub thread_id: String,
    pub continuation_thread_id: String,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliRuntimeExecutionSegmentStatus {
    Running,
    Completed,
    Failed,
    Interrupted,
}

impl CliRuntimeExecutionSegmentStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        }
    }

    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Running)
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "interrupted" => Ok(Self::Interrupted),
            other => Err(anyhow!(
                "unknown CLI runtime execution segment status `{other}`"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeExecutionSegmentRecord {
    pub id: String,
    pub attempt_id: String,
    pub turn_id: String,
    pub segment_index: u32,
    pub runtime_id: String,
    pub native_thread_id: String,
    pub native_turn_id: String,
    pub status: CliRuntimeExecutionSegmentStatus,
    pub failure_reason: Option<String>,
    pub started_at: DateTimeWithTimeZone,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeNativeTurnOwner {
    pub binding: CliRuntimeTurnBindingRecord,
    pub attempt: CliRuntimeTurnAttemptRecord,
    pub segment: Option<CliRuntimeExecutionSegmentRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCliRuntimeExecutionSegment {
    pub id: String,
    pub attempt_id: String,
    pub turn_id: String,
    pub segment_index: u32,
    pub runtime_id: String,
    pub native_thread_id: String,
    pub native_turn_id: String,
    pub status: CliRuntimeExecutionSegmentStatus,
    pub failure_reason: Option<String>,
    pub started_at: DateTimeWithTimeZone,
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
    pub continuation_thread_id: Option<String>,
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

/// Atomically load or create the durable Claude provider UUID before a
/// provider process can be spawned. The caller supplies a freshly generated
/// candidate, but an existing binding always wins.
pub async fn prepare_claude_provider_session_binding<C: ConnectionTrait>(
    db: &C,
    request: PrepareClaudeProviderSessionBinding,
) -> Result<PreparedClaudeProviderSessionBinding> {
    validate_provider_session_id(request.proposed_provider_session_id.as_str())?;
    let thread_id = request.thread_binding.thread_id.clone();
    let workspace_id = request.thread_binding.workspace_id.clone();
    let runtime_id = request.thread_binding.runtime_id.clone();
    if let Some(model) = thread_cli_runtime_binding::Entity::find_by_id(thread_id.clone())
        .one(db)
        .await
        .context("failed to query Claude provider session binding")?
    {
        validate_claude_binding_identity(&model, &request.thread_binding)?;
        let existing = provider_session_binding_from_model(&model)?;
        if let Some(existing) = existing {
            if existing.lifecycle == CliRuntimeProviderSessionLifecycle::Invalid {
                return Err(anyhow!(
                    "Claude provider session binding for thread `{thread_id}` is invalid"
                ));
            }
            let mode = if existing.lifecycle == CliRuntimeProviderSessionLifecycle::Verified {
                PreparedClaudeProviderSessionMode::Resume
            } else {
                PreparedClaudeProviderSessionMode::New
            };
            return Ok(PreparedClaudeProviderSessionBinding {
                binding: thread_binding_record_from_model(model)?,
                mode,
            });
        }

        return Err(anyhow!(
            "existing Claude thread binding has no durable real provider session identity"
        ));
    }

    let proposed_provider_session_id = request.proposed_provider_session_id;
    let mut active = active_thread_binding_from_new(request.thread_binding);
    active.native_thread_id = Set(proposed_provider_session_id.clone());
    active.native_session_id = Set(Some(proposed_provider_session_id.clone()));
    active.provider_session_id = Set(Some(proposed_provider_session_id));
    active.provider_session_lifecycle_state = Set(Some(
        CliRuntimeProviderSessionLifecycle::Prepared
            .as_str()
            .to_owned(),
    ));
    active.provider_session_last_verified_process_generation = Set(None);
    thread_cli_runtime_binding::Entity::insert(active)
        .on_conflict(
            OnConflict::column(thread_cli_runtime_binding::Column::ThreadId)
                .do_nothing()
                .to_owned(),
        )
        .exec(db)
        .await
        .context("failed to atomically prepare Claude provider session binding")?;
    let binding = find_thread_binding(db, thread_id.as_str())
        .await?
        .context("prepared Claude provider session binding is missing")?;
    if binding.workspace_id != workspace_id
        || binding.runtime_id != runtime_id
        || binding.runtime_kind != "claude"
    {
        return Err(anyhow!(
            "concurrent Claude provider session binding has an incompatible identity"
        ));
    }
    let lifecycle = binding
        .provider_session
        .as_ref()
        .context("concurrent Claude provider session binding is incomplete")?
        .lifecycle;
    if lifecycle == CliRuntimeProviderSessionLifecycle::Invalid {
        return Err(anyhow!(
            "concurrent Claude provider session binding is invalid"
        ));
    }
    Ok(PreparedClaudeProviderSessionBinding {
        binding,
        mode: if lifecycle == CliRuntimeProviderSessionLifecycle::Verified {
            PreparedClaudeProviderSessionMode::Resume
        } else {
            PreparedClaudeProviderSessionMode::New
        },
    })
}

/// Verify the emitted Claude UUID against the durable prepared identity. A
/// mismatch is terminal for the binding. An event from an older process
/// generation can neither verify nor invalidate a newer generation.
pub async fn verify_claude_provider_session_binding<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    expected_provider_session_id: &str,
    emitted_provider_session_id: Option<&str>,
    process_generation: i64,
) -> Result<CliRuntimeThreadBindingRecord> {
    if process_generation <= 0 {
        return Err(anyhow!(
            "Claude provider session process generation must be positive"
        ));
    }
    validate_provider_session_id(expected_provider_session_id)?;
    let model = thread_cli_runtime_binding::Entity::find_by_id(thread_id.to_owned())
        .one(db)
        .await
        .context("failed to query Claude provider session for verification")?
        .context("Claude provider session binding is missing")?;
    let provider = provider_session_binding_from_model(&model)?
        .context("Claude provider session identity is missing")?;
    if provider.provider_session_id != expected_provider_session_id {
        return Err(anyhow!(
            "Claude provider session launch identity does not match the durable binding"
        ));
    }
    if provider.lifecycle == CliRuntimeProviderSessionLifecycle::Invalid {
        return Err(anyhow!("Claude provider session binding is invalid"));
    }
    if provider
        .last_verified_process_generation
        .is_some_and(|generation| process_generation < generation)
    {
        return Err(anyhow!(
            "stale Claude process generation cannot verify the provider session"
        ));
    }

    let emitted_matches = emitted_provider_session_id
        .and_then(|value| validate_provider_session_id(value).ok().map(|_| value))
        == Some(expected_provider_session_id);
    let mut active: thread_cli_runtime_binding::ActiveModel = model.into();
    if emitted_matches {
        active.provider_session_lifecycle_state = Set(Some(
            CliRuntimeProviderSessionLifecycle::Verified
                .as_str()
                .to_owned(),
        ));
        active.provider_session_last_verified_process_generation = Set(Some(process_generation));
    } else {
        active.provider_session_lifecycle_state = Set(Some(
            CliRuntimeProviderSessionLifecycle::Invalid
                .as_str()
                .to_owned(),
        ));
    }
    let model = active
        .update(db)
        .await
        .context("failed to persist Claude provider session verification")?;
    let binding = thread_binding_record_from_model(model)?;
    if !emitted_matches {
        return Err(anyhow!(
            "Claude emitted a missing, invalid, or mismatched provider session identity"
        ));
    }
    Ok(binding)
}

fn validate_provider_session_id(value: &str) -> Result<()> {
    let provider_session_id = uuid::Uuid::parse_str(value)
        .map_err(|_| anyhow!("invalid CLI provider session identity"))?;
    if provider_session_id.is_nil() {
        return Err(anyhow!("invalid CLI provider session identity"));
    }
    Ok(())
}

fn validate_claude_binding_identity(
    model: &thread_cli_runtime_binding::Model,
    requested: &NewCliRuntimeThreadBinding,
) -> Result<()> {
    if model.workspace_id != requested.workspace_id
        || model.runtime_id != requested.runtime_id
        || model.runtime_kind != "claude"
        || requested.runtime_kind != "claude"
    {
        return Err(anyhow!(
            "existing CLI thread binding does not match the requested Claude runtime identity"
        ));
    }
    if model.status != "active" {
        return Err(anyhow!("existing Claude thread binding is not active"));
    }
    Ok(())
}

pub async fn set_thread_mcp_metadata<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    metadata: Option<CliRuntimeThreadMcpMetadata>,
) -> Result<CliRuntimeThreadBindingRecord> {
    let model = thread_cli_runtime_binding::Entity::find_by_id(thread_id.to_owned())
        .one(db)
        .await
        .context("failed to query CLI runtime thread binding for MCP metadata update")?
        .context("CLI runtime thread binding is missing for MCP metadata update")?;
    let mut active: thread_cli_runtime_binding::ActiveModel = model.into();
    let (
        adapter_kind,
        manifest_hash,
        projection_fingerprint,
        provider_contract_fingerprint,
        isolation_contract_fingerprint,
        session_generation,
        provider_session_id,
        provider_session_lifecycle_state,
        provider_session_last_verified_process_generation,
    ) = match metadata {
        Some(metadata) => (
            Some(metadata.adapter_kind),
            Some(metadata.manifest_hash),
            Some(metadata.projection_fingerprint),
            Some(metadata.provider_contract_fingerprint),
            Some(metadata.isolation_contract_fingerprint),
            Some(metadata.session_generation),
            metadata.provider_session_id,
            metadata.provider_session_lifecycle_state,
            metadata.provider_session_last_verified_process_generation,
        ),
        None => (None, None, None, None, None, None, None, None, None),
    };
    active.mcp_adapter_kind = Set(adapter_kind);
    active.mcp_manifest_hash = Set(manifest_hash);
    active.mcp_projection_fingerprint = Set(projection_fingerprint);
    active.mcp_provider_contract_fingerprint = Set(provider_contract_fingerprint);
    active.mcp_isolation_contract_fingerprint = Set(isolation_contract_fingerprint);
    active.mcp_session_generation = Set(session_generation);
    active.provider_session_id = Set(provider_session_id);
    active.provider_session_lifecycle_state = Set(provider_session_lifecycle_state);
    active.provider_session_last_verified_process_generation =
        Set(provider_session_last_verified_process_generation);
    let model = active
        .update(db)
        .await
        .context("failed to update CLI runtime thread MCP metadata")?;
    thread_binding_record_from_model(model)
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
                    turn_cli_runtime_binding::Column::ContinuationThreadId,
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

pub async fn set_turn_mcp_metadata<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    metadata: Option<CliRuntimeTurnMcpMetadata>,
) -> Result<CliRuntimeTurnBindingRecord> {
    let model = turn_cli_runtime_binding::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .context("failed to query CLI runtime turn binding for MCP metadata update")?
        .context("CLI runtime turn binding is missing for MCP metadata update")?;
    let mut active: turn_cli_runtime_binding::ActiveModel = model.into();
    let (
        adapter_kind,
        manifest_hash,
        projection_fingerprint,
        provider_contract_fingerprint,
        isolation_contract_fingerprint,
        session_generation,
        projection_activation_generation,
    ) = match metadata {
        Some(metadata) => (
            Some(metadata.adapter_kind),
            Some(metadata.manifest_hash),
            Some(metadata.projection_fingerprint),
            Some(metadata.provider_contract_fingerprint),
            Some(metadata.isolation_contract_fingerprint),
            Some(metadata.session_generation),
            Some(metadata.projection_activation_generation),
        ),
        None => (None, None, None, None, None, None, None),
    };
    active.mcp_adapter_kind = Set(adapter_kind);
    active.mcp_manifest_hash = Set(manifest_hash);
    active.mcp_projection_fingerprint = Set(projection_fingerprint);
    active.mcp_provider_contract_fingerprint = Set(provider_contract_fingerprint);
    active.mcp_isolation_contract_fingerprint = Set(isolation_contract_fingerprint);
    active.mcp_session_generation = Set(session_generation);
    active.mcp_projection_activation_generation = Set(projection_activation_generation);
    let model = active
        .update(db)
        .await
        .context("failed to update CLI runtime turn MCP metadata")?;
    turn_binding_record_from_model(model)
}

pub async fn set_turn_native_goal_state<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    status: Option<String>,
    native_goal_turn_id: Option<String>,
    observed_at: DateTimeWithTimeZone,
) -> Result<CliRuntimeTurnBindingRecord> {
    if status
        .as_deref()
        .is_some_and(|status| status.trim().is_empty())
    {
        return Err(anyhow!("CLI runtime native Goal status cannot be empty"));
    }
    let model = turn_cli_runtime_binding::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .context("failed to query CLI runtime turn binding for native Goal update")?
        .context("CLI runtime turn binding is missing for native Goal update")?;
    let mut active: turn_cli_runtime_binding::ActiveModel = model.into();
    active.native_goal_status = Set(status);
    active.native_goal_turn_id = Set(native_goal_turn_id);
    active.native_goal_observed_at = Set(Some(observed_at));
    active.updated_at = Set(observed_at);
    let model = active
        .update(db)
        .await
        .context("failed to update CLI runtime native Goal state")?;
    turn_binding_record_from_model(model)
}

pub async fn clear_turn_native_goal_state<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    updated_at: DateTimeWithTimeZone,
) -> Result<CliRuntimeTurnBindingRecord> {
    let model = turn_cli_runtime_binding::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .context("failed to query CLI runtime turn binding for native Goal reset")?
        .context("CLI runtime turn binding is missing for native Goal reset")?;
    let mut active: turn_cli_runtime_binding::ActiveModel = model.into();
    active.native_goal_status = Set(None);
    active.native_goal_turn_id = Set(None);
    active.native_goal_observed_at = Set(None);
    active.updated_at = Set(updated_at);
    let model = active
        .update(db)
        .await
        .context("failed to reset CLI runtime native Goal state")?;
    turn_binding_record_from_model(model)
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
    if let Some(continuation_thread_id) = filter.continuation_thread_id {
        query = query.filter(
            turn_cli_runtime_binding::Column::ContinuationThreadId.eq(continuation_thread_id),
        );
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
            sea_orm::sea_query::Expr::value(failure_reason.clone()),
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
    if result.rows_affected == 1 {
        let segment_status = match status {
            CliRuntimeTurnAttemptStatus::Completed => CliRuntimeExecutionSegmentStatus::Completed,
            CliRuntimeTurnAttemptStatus::Failed => CliRuntimeExecutionSegmentStatus::Failed,
            CliRuntimeTurnAttemptStatus::Interrupted => {
                CliRuntimeExecutionSegmentStatus::Interrupted
            }
            CliRuntimeTurnAttemptStatus::Starting | CliRuntimeTurnAttemptStatus::Running => {
                unreachable!("active attempt status was rejected above")
            }
        };
        turn_cli_runtime_execution_segment::Entity::update_many()
            .col_expr(
                turn_cli_runtime_execution_segment::Column::Status,
                sea_orm::sea_query::Expr::value(segment_status.as_str()),
            )
            .col_expr(
                turn_cli_runtime_execution_segment::Column::FailureReason,
                sea_orm::sea_query::Expr::value(failure_reason),
            )
            .col_expr(
                turn_cli_runtime_execution_segment::Column::CompletedAt,
                sea_orm::sea_query::Expr::value(Some(completed_at)),
            )
            .col_expr(
                turn_cli_runtime_execution_segment::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(completed_at),
            )
            .filter(turn_cli_runtime_execution_segment::Column::AttemptId.eq(id.to_owned()))
            .filter(
                turn_cli_runtime_execution_segment::Column::Status
                    .eq(CliRuntimeExecutionSegmentStatus::Running.as_str()),
            )
            .exec(db)
            .await
            .context("failed to terminalize active CLI runtime execution segments")?;
    }
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

pub async fn create_execution_segment<C: ConnectionTrait>(
    db: &C,
    segment: NewCliRuntimeExecutionSegment,
) -> Result<CliRuntimeExecutionSegmentRecord> {
    validate_new_execution_segment(&segment)?;
    let id = segment.id.clone();
    turn_cli_runtime_execution_segment::Entity::insert(active_execution_segment_from_new(segment))
        .exec(db)
        .await
        .context("failed to insert CLI runtime execution segment")?;
    find_execution_segment(db, id.as_str())
        .await?
        .context("inserted CLI runtime execution segment is missing")
}

pub async fn find_execution_segment<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<CliRuntimeExecutionSegmentRecord>> {
    turn_cli_runtime_execution_segment::Entity::find_by_id(id.to_owned())
        .one(db)
        .await
        .context("failed to query CLI runtime execution segment")?
        .map(execution_segment_record_from_model)
        .transpose()
}

pub async fn find_execution_segment_by_native_turn<C: ConnectionTrait>(
    db: &C,
    runtime_id: &str,
    native_turn_id: &str,
) -> Result<Option<CliRuntimeExecutionSegmentRecord>> {
    turn_cli_runtime_execution_segment::Entity::find()
        .filter(turn_cli_runtime_execution_segment::Column::RuntimeId.eq(runtime_id.to_owned()))
        .filter(
            turn_cli_runtime_execution_segment::Column::NativeTurnId.eq(native_turn_id.to_owned()),
        )
        .one(db)
        .await
        .context("failed to query CLI runtime execution segment by native turn")?
        .map(execution_segment_record_from_model)
        .transpose()
}

pub async fn latest_execution_segment_for_attempt<C: ConnectionTrait>(
    db: &C,
    attempt_id: &str,
) -> Result<Option<CliRuntimeExecutionSegmentRecord>> {
    turn_cli_runtime_execution_segment::Entity::find()
        .filter(turn_cli_runtime_execution_segment::Column::AttemptId.eq(attempt_id.to_owned()))
        .order_by_desc(turn_cli_runtime_execution_segment::Column::SegmentIndex)
        .one(db)
        .await
        .context("failed to query latest CLI runtime execution segment")?
        .map(execution_segment_record_from_model)
        .transpose()
}

pub async fn latest_running_execution_segment_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<CliRuntimeExecutionSegmentRecord>> {
    turn_cli_runtime_execution_segment::Entity::find()
        .filter(turn_cli_runtime_execution_segment::Column::TurnId.eq(turn_id.to_owned()))
        .filter(
            turn_cli_runtime_execution_segment::Column::Status
                .eq(CliRuntimeExecutionSegmentStatus::Running.as_str()),
        )
        .order_by_desc(turn_cli_runtime_execution_segment::Column::SegmentIndex)
        .one(db)
        .await
        .context("failed to query running CLI runtime execution segment")?
        .map(execution_segment_record_from_model)
        .transpose()
}

pub async fn resolve_native_turn_owner<C: ConnectionTrait>(
    db: &C,
    runtime_id: &str,
    native_turn_id: &str,
) -> Result<Option<CliRuntimeNativeTurnOwner>> {
    let (attempt, segment) = if let Some(segment) =
        find_execution_segment_by_native_turn(db, runtime_id, native_turn_id).await?
    {
        let attempt = find_turn_attempt(db, segment.attempt_id.as_str())
            .await?
            .context("CLI runtime execution segment owning attempt is missing")?;
        (attempt, Some(segment))
    } else if let Some(attempt) =
        find_turn_attempt_by_native_turn(db, runtime_id, native_turn_id).await?
    {
        (attempt, None)
    } else {
        return Ok(None);
    };
    let binding = find_turn_binding(db, attempt.turn_id.as_str())
        .await?
        .context("CLI runtime native turn binding is missing")?;
    if binding.runtime_id != runtime_id
        || binding.turn_id != attempt.turn_id
        || binding.native_thread_id != attempt.native_thread_id
        || segment.as_ref().is_some_and(|segment| {
            segment.turn_id != attempt.turn_id
                || segment.attempt_id != attempt.id
                || segment.runtime_id != runtime_id
                || segment.native_thread_id != attempt.native_thread_id
        })
    {
        return Err(anyhow!("CLI runtime native turn owner is inconsistent"));
    }
    Ok(Some(CliRuntimeNativeTurnOwner {
        binding,
        attempt,
        segment,
    }))
}

pub async fn mark_execution_segment_terminal<C: ConnectionTrait>(
    db: &C,
    id: &str,
    status: CliRuntimeExecutionSegmentStatus,
    failure_reason: Option<String>,
    completed_at: DateTimeWithTimeZone,
) -> Result<bool> {
    if !status.is_terminal() {
        return Err(anyhow!(
            "CLI runtime terminal execution segment status cannot be `{}`",
            status.as_str()
        ));
    }
    let result = turn_cli_runtime_execution_segment::Entity::update_many()
        .col_expr(
            turn_cli_runtime_execution_segment::Column::Status,
            sea_orm::sea_query::Expr::value(status.as_str()),
        )
        .col_expr(
            turn_cli_runtime_execution_segment::Column::FailureReason,
            sea_orm::sea_query::Expr::value(failure_reason),
        )
        .col_expr(
            turn_cli_runtime_execution_segment::Column::CompletedAt,
            sea_orm::sea_query::Expr::value(Some(completed_at)),
        )
        .col_expr(
            turn_cli_runtime_execution_segment::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(completed_at),
        )
        .filter(turn_cli_runtime_execution_segment::Column::Id.eq(id.to_owned()))
        .filter(
            turn_cli_runtime_execution_segment::Column::Status
                .eq(CliRuntimeExecutionSegmentStatus::Running.as_str()),
        )
        .exec(db)
        .await
        .context("failed to mark CLI runtime execution segment terminal")?;
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
        ..Default::default()
    }
}

fn active_turn_binding_from_new(
    binding: NewCliRuntimeTurnBinding,
) -> turn_cli_runtime_binding::ActiveModel {
    turn_cli_runtime_binding::ActiveModel {
        turn_id: Set(binding.turn_id),
        thread_id: Set(binding.thread_id),
        continuation_thread_id: Set(binding.continuation_thread_id),
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
        ..Default::default()
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

fn active_execution_segment_from_new(
    segment: NewCliRuntimeExecutionSegment,
) -> turn_cli_runtime_execution_segment::ActiveModel {
    turn_cli_runtime_execution_segment::ActiveModel {
        id: Set(segment.id),
        attempt_id: Set(segment.attempt_id),
        turn_id: Set(segment.turn_id),
        segment_index: Set(i64::from(segment.segment_index)),
        runtime_id: Set(segment.runtime_id),
        native_thread_id: Set(segment.native_thread_id),
        native_turn_id: Set(segment.native_turn_id),
        status: Set(segment.status.as_str().to_owned()),
        failure_reason: Set(segment.failure_reason),
        started_at: Set(segment.started_at),
        completed_at: Set(segment.completed_at),
        created_at: Set(segment.created_at),
        updated_at: Set(segment.updated_at),
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

fn validate_new_execution_segment(segment: &NewCliRuntimeExecutionSegment) -> Result<()> {
    for (label, value) in [
        ("id", segment.id.as_str()),
        ("attempt_id", segment.attempt_id.as_str()),
        ("turn_id", segment.turn_id.as_str()),
        ("runtime_id", segment.runtime_id.as_str()),
        ("native_thread_id", segment.native_thread_id.as_str()),
        ("native_turn_id", segment.native_turn_id.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(anyhow!(
                "CLI runtime execution segment `{label}` cannot be empty"
            ));
        }
    }
    if segment.segment_index == 0 {
        return Err(anyhow!(
            "CLI runtime execution segment index must be positive"
        ));
    }
    if segment.status != CliRuntimeExecutionSegmentStatus::Running
        || segment.completed_at.is_some()
        || segment.failure_reason.is_some()
    {
        return Err(anyhow!(
            "new CLI runtime execution segment must be running and non-terminal"
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
    let mcp = thread_mcp_metadata_from_model(&model);
    let provider_session = provider_session_binding_from_model(&model)?;
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
        mcp,
        provider_session,
        created_at: model.created_at,
        updated_at: model.updated_at,
    })
}

fn provider_session_binding_from_model(
    model: &thread_cli_runtime_binding::Model,
) -> Result<Option<CliRuntimeProviderSessionBinding>> {
    match (
        model.provider_session_id.as_deref(),
        model.provider_session_lifecycle_state.as_deref(),
    ) {
        (None, None)
            if model
                .provider_session_last_verified_process_generation
                .is_none() =>
        {
            Ok(None)
        }
        (Some(provider_session_id), Some(lifecycle)) => {
            validate_provider_session_id(provider_session_id)?;
            let lifecycle = CliRuntimeProviderSessionLifecycle::from_db(lifecycle)?;
            let last_verified_process_generation =
                model.provider_session_last_verified_process_generation;
            if last_verified_process_generation.is_some_and(|generation| generation <= 0) {
                return Err(anyhow!(
                    "CLI provider session verified process generation must be positive"
                ));
            }
            if lifecycle == CliRuntimeProviderSessionLifecycle::Prepared
                && last_verified_process_generation.is_some()
            {
                return Err(anyhow!(
                    "prepared CLI provider session cannot have a verified process generation"
                ));
            }
            Ok(Some(CliRuntimeProviderSessionBinding {
                provider_session_id: provider_session_id.to_owned(),
                lifecycle,
                last_verified_process_generation,
            }))
        }
        _ => Err(anyhow!(
            "CLI provider session binding columns are incomplete"
        )),
    }
}

fn turn_binding_record_from_model(
    model: turn_cli_runtime_binding::Model,
) -> Result<CliRuntimeTurnBindingRecord> {
    let mcp = turn_mcp_metadata_from_model(&model);
    Ok(CliRuntimeTurnBindingRecord {
        turn_id: model.turn_id,
        thread_id: model.thread_id,
        continuation_thread_id: model.continuation_thread_id,
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
        mcp,
        native_goal_status: model.native_goal_status,
        native_goal_turn_id: model.native_goal_turn_id,
        native_goal_observed_at: model.native_goal_observed_at,
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

fn execution_segment_record_from_model(
    model: turn_cli_runtime_execution_segment::Model,
) -> Result<CliRuntimeExecutionSegmentRecord> {
    Ok(CliRuntimeExecutionSegmentRecord {
        id: model.id,
        attempt_id: model.attempt_id,
        turn_id: model.turn_id,
        segment_index: u32::try_from(model.segment_index)
            .context("CLI runtime execution segment index is outside u32 range")?,
        runtime_id: model.runtime_id,
        native_thread_id: model.native_thread_id,
        native_turn_id: model.native_turn_id,
        status: CliRuntimeExecutionSegmentStatus::from_db(model.status.as_str())?,
        failure_reason: model.failure_reason,
        started_at: model.started_at,
        completed_at: model.completed_at,
        created_at: model.created_at,
        updated_at: model.updated_at,
    })
}

fn thread_mcp_metadata_from_model(
    model: &thread_cli_runtime_binding::Model,
) -> Option<CliRuntimeThreadMcpMetadata> {
    Some(CliRuntimeThreadMcpMetadata {
        adapter_kind: model.mcp_adapter_kind.clone()?,
        manifest_hash: model.mcp_manifest_hash.clone()?,
        projection_fingerprint: model.mcp_projection_fingerprint.clone()?,
        provider_contract_fingerprint: model.mcp_provider_contract_fingerprint.clone()?,
        isolation_contract_fingerprint: model.mcp_isolation_contract_fingerprint.clone()?,
        session_generation: model.mcp_session_generation?,
        provider_session_id: model.provider_session_id.clone(),
        provider_session_lifecycle_state: model.provider_session_lifecycle_state.clone(),
        provider_session_last_verified_process_generation: model
            .provider_session_last_verified_process_generation,
    })
}

fn turn_mcp_metadata_from_model(
    model: &turn_cli_runtime_binding::Model,
) -> Option<CliRuntimeTurnMcpMetadata> {
    Some(CliRuntimeTurnMcpMetadata {
        adapter_kind: model.mcp_adapter_kind.clone()?,
        manifest_hash: model.mcp_manifest_hash.clone()?,
        projection_fingerprint: model.mcp_projection_fingerprint.clone()?,
        provider_contract_fingerprint: model.mcp_provider_contract_fingerprint.clone()?,
        isolation_contract_fingerprint: model.mcp_isolation_contract_fingerprint.clone()?,
        session_generation: model.mcp_session_generation?,
        projection_activation_generation: model.mcp_projection_activation_generation?,
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
