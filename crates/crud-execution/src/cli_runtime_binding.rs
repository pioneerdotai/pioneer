//! Durable CLI thread and turn bindings, provider sessions and fork intents.
pub use crate::cli_runtime_attempts::{
    CliRuntimeExecutionSegmentRecord, CliRuntimeExecutionSegmentStatus,
    CliRuntimeTurnAttemptRecord, CliRuntimeTurnAttemptStatus, NewCliRuntimeExecutionSegment,
    NewCliRuntimeTurnAttempt, create_execution_segment, create_turn_attempt,
    find_execution_segment, find_execution_segment_by_native_turn, find_turn_attempt,
    find_turn_attempt_by_native_turn, find_turn_attempt_by_recovery_attempt,
    latest_execution_segment_for_attempt, latest_running_execution_segment_for_turn,
    latest_turn_attempt, mark_execution_segment_terminal, mark_turn_attempt_recovery_confirmed,
    mark_turn_attempt_running, mark_turn_attempt_terminal,
};
use crate::input_mapping::InputMapping;
pub use crate::native_events::{
    CliRuntimeNativeEventListFilter, CliRuntimeNativeEventRecord, NewCliRuntimeNativeEvent,
    append_native_event, append_native_event_if_absent, append_native_event_if_absent_with_outcome,
    find_native_event, latest_native_event, list_native_events,
};
pub use crate::pending_requests::{
    AcceptCliRuntimePendingRequestResponse, CLI_RUNTIME_PENDING_REQUEST_PAGE_MAX,
    CliRuntimePendingRequestListFilter, CliRuntimePendingRequestRecord,
    CliRuntimePendingRequestStatus, CliRuntimeRequestAuthorizationBinding,
    NewCliRuntimePendingRequest, ResolveCliRuntimePendingRequest,
    TransitionCliRuntimePendingRequestDelivery, accept_pending_request_response,
    bind_pending_request_authorization, cancel_pending_request, create_pending_request,
    ensure_pending_request_capacity, expire_pending_request, find_pending_request,
    list_open_pending_requests_for_descendant_threads, list_pending_requests, open_pending_request,
    pending_request_frontier_for_turn, resolve_pending_request,
    transition_pending_request_delivery,
};
use anyhow::{Context, Result, anyhow, bail};
use pioneer_entity::{thread_cli_runtime_binding, turn_cli_runtime_binding};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, ExprTrait, Iterable, QueryFilter,
    QueryOrder, QuerySelect, Set,
};
use std::future::Future;

/// Proof supplied by the retention owner and checked on the caller's
/// connection before a continuation can publish a frozen manifest locator.
pub trait FrozenBindingRoot: Sync {
    fn locator(&self) -> Option<String>;
    fn verify<'a, C: ConnectionTrait>(
        &'a self,
        db: &'a C,
        workspace: &'a str,
    ) -> impl Future<Output = Result<()>> + Send + 'a;
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
    pub force_new: bool,
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

/// Only the persistent active set participates in global background discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliRuntimeActiveTurnBindingStatus {
    Starting,
    Running,
}

impl CliRuntimeActiveTurnBindingStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
        }
    }
}

pub const CLI_RUNTIME_ACTIVE_TURN_BINDING_PAGE_MAX: u64 = 64;

#[derive(Debug, Clone)]
pub struct CliRuntimeTurnBindingRecord {
    /// Storage provenance used only by CRUD. Callers receive logical JSON.
    #[doc(hidden)]
    pub input_mapping_frame: Option<InputMapping>,
    #[doc(hidden)]
    pub input_mapping_has_inline_history: bool,
    #[doc(hidden)]
    pub input_mapping_revision: i64,
    #[doc(hidden)]
    pub input_mapping_identity: String,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeNativeTurnOwner {
    pub binding: CliRuntimeTurnBindingRecord,
    pub attempt: CliRuntimeTurnAttemptRecord,
    pub segment: Option<CliRuntimeExecutionSegmentRecord>,
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

pub use crate::native_events::{deserialize_cli_runtime_json, serialize_cli_runtime_json};

pub async fn upsert_thread_binding<C: ConnectionTrait>(
    db: &C,
    binding: NewCliRuntimeThreadBinding,
    root: &impl FrozenBindingRoot,
) -> Result<CliRuntimeThreadBindingRecord> {
    root.verify(db, &binding.workspace_id).await?;
    let thread_id = binding.thread_id.clone();
    thread_cli_runtime_binding::Entity::insert(active_thread_binding_from_new(binding, root))
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
                    thread_cli_runtime_binding::Column::FrozenManifestId,
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

pub async fn insert_fork_intent_if_absent<C: ConnectionTrait>(
    db: &C,
    pending: NewCliRuntimeThreadBinding,
    root: &impl FrozenBindingRoot,
) -> Result<bool> {
    root.verify(db, &pending.workspace_id).await?;
    anyhow::ensure!(
        pending.status == "fork_pending",
        "CLI fork intent must be pending"
    );
    let result =
        thread_cli_runtime_binding::Entity::insert(active_thread_binding_from_new(pending, root))
            .on_conflict(
                OnConflict::column(thread_cli_runtime_binding::Column::ThreadId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(db)
            .await
            .context("failed to insert CLI fork intent")?;
    Ok(result == 1)
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

/// Remove only an unsent fork intent after a typed provider rejection proves
/// that no branch was created. A concurrent state change is never erased.
pub async fn discard_rejected_fork_intent<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    runtime_id: &str,
    source_native_thread_id: &str,
    expected_cursor_json: &str,
) -> Result<bool> {
    let result = thread_cli_runtime_binding::Entity::delete_many()
        .filter(thread_cli_runtime_binding::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(thread_cli_runtime_binding::Column::RuntimeId.eq(runtime_id.to_owned()))
        .filter(
            thread_cli_runtime_binding::Column::NativeThreadId
                .eq(source_native_thread_id.to_owned()),
        )
        .filter(thread_cli_runtime_binding::Column::Status.eq("fork_pending"))
        .filter(
            thread_cli_runtime_binding::Column::ResumeCursorJson
                .eq(expected_cursor_json.to_owned()),
        )
        .exec(db)
        .await
        .context("failed to discard rejected CLI fork intent")?;
    Ok(result.rows_affected == 1)
}

/// Commit a provider fork only while the exact durable intent is still
/// pending. The caller performs provider I/O before entering the writer.
pub async fn confirm_marked_fork_intent<C: ConnectionTrait>(
    db: &C,
    pending: &CliRuntimeThreadBindingRecord,
    fork_native_thread_id: &str,
    native_cwd: Option<String>,
    native_model: Option<String>,
    resume_cursor_json: String,
    updated_at: DateTimeWithTimeZone,
    root: &impl FrozenBindingRoot,
) -> Result<CliRuntimeThreadBindingRecord> {
    root.verify(db, &pending.workspace_id).await?;
    let result = thread_cli_runtime_binding::Entity::update_many()
        .col_expr(
            thread_cli_runtime_binding::Column::FrozenManifestId,
            Expr::value(root.locator()),
        )
        .col_expr(
            thread_cli_runtime_binding::Column::NativeThreadId,
            Expr::value(fork_native_thread_id.to_owned()),
        )
        .col_expr(
            thread_cli_runtime_binding::Column::NativeCwd,
            Expr::value(native_cwd),
        )
        .col_expr(
            thread_cli_runtime_binding::Column::NativeModel,
            Expr::value(native_model),
        )
        .col_expr(
            thread_cli_runtime_binding::Column::ResumeCursorJson,
            Expr::value(resume_cursor_json),
        )
        .col_expr(
            thread_cli_runtime_binding::Column::Status,
            Expr::value("active"),
        )
        .col_expr(
            thread_cli_runtime_binding::Column::UpdatedAt,
            Expr::value(updated_at),
        )
        .filter(thread_cli_runtime_binding::Column::ThreadId.eq(pending.thread_id.clone()))
        .filter(thread_cli_runtime_binding::Column::WorkspaceId.eq(pending.workspace_id.clone()))
        .filter(thread_cli_runtime_binding::Column::RuntimeId.eq(pending.runtime_id.clone()))
        .filter(
            thread_cli_runtime_binding::Column::NativeThreadId.eq(pending.native_thread_id.clone()),
        )
        .filter(
            thread_cli_runtime_binding::Column::ResumeCursorJson
                .eq(pending.resume_cursor_json.clone()),
        )
        .filter(thread_cli_runtime_binding::Column::Status.eq("fork_pending"))
        .exec(db)
        .await
        .context("failed to commit marked CLI fork")?;
    if result.rows_affected != 1 {
        bail!("CLI fork intent changed before provider result commit");
    }
    find_thread_binding(db, pending.thread_id.as_str())
        .await?
        .context("confirmed CLI fork binding is missing")
}

pub async fn update_thread_resume_cursor<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    expected_native_thread_id: &str,
    expected_cursor_json: &str,
    resume_cursor_json: String,
    updated_at: DateTimeWithTimeZone,
    root: &impl FrozenBindingRoot,
) -> Result<CliRuntimeThreadBindingRecord> {
    let current = find_thread_binding(db, thread_id)
        .await?
        .context("CLI binding missing")?;
    root.verify(db, &current.workspace_id).await?;
    let result = thread_cli_runtime_binding::Entity::update_many()
        .col_expr(
            thread_cli_runtime_binding::Column::FrozenManifestId,
            Expr::value(root.locator()),
        )
        .col_expr(
            thread_cli_runtime_binding::Column::ResumeCursorJson,
            Expr::value(resume_cursor_json),
        )
        .col_expr(
            thread_cli_runtime_binding::Column::UpdatedAt,
            Expr::value(updated_at),
        )
        .filter(thread_cli_runtime_binding::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(
            thread_cli_runtime_binding::Column::NativeThreadId
                .eq(expected_native_thread_id.to_owned()),
        )
        .filter(
            thread_cli_runtime_binding::Column::ResumeCursorJson
                .eq(expected_cursor_json.to_owned()),
        )
        .exec(db)
        .await
        .context("failed to update CLI runtime context receipt")?;
    if result.rows_affected != 1 {
        bail!("CLI runtime thread changed before context receipt commit");
    }
    find_thread_binding(db, thread_id)
        .await?
        .context("CLI runtime thread binding disappeared after context receipt commit")
}

/// Atomically load or create the durable Claude provider UUID before a
/// provider process can be spawned. The caller supplies a freshly generated
/// candidate, but an existing binding always wins.
pub async fn prepare_claude_provider_session_binding<C: ConnectionTrait>(
    db: &C,
    request: PrepareClaudeProviderSessionBinding,
    root: &impl FrozenBindingRoot,
) -> Result<PreparedClaudeProviderSessionBinding> {
    validate_provider_session_id(request.proposed_provider_session_id.as_str())?;
    let thread_id = request.thread_binding.thread_id.clone();
    let workspace_id = request.thread_binding.workspace_id.clone();
    let runtime_id = request.thread_binding.runtime_id.clone();
    if let Some(model) = thread_cli_runtime_binding::Entity::find_by_id(thread_id.clone())
        .lock_exclusive()
        .one(db)
        .await
        .context("failed to query Claude provider session binding")?
    {
        let crossing_runtime = request.force_new
            && model.workspace_id == request.thread_binding.workspace_id
            && model.status == "active"
            && (model.runtime_id != request.thread_binding.runtime_id
                || model.runtime_kind != "claude");
        if !crossing_runtime {
            validate_claude_binding_identity(&model, &request.thread_binding)?;
        }
        if request.force_new {
            root.verify(db, &request.thread_binding.workspace_id)
                .await?;
            // The transition fence differs for a provider switch, but both
            // paths prepare the same durable Claude session fields once.
            let mut active: thread_cli_runtime_binding::ActiveModel = model.into();
            if crossing_runtime {
                active.runtime_id = Set(request.thread_binding.runtime_id);
                active.runtime_kind = Set("claude".to_owned());
            }
            let proposed_provider_session_id = request.proposed_provider_session_id;
            active.native_thread_id = Set(proposed_provider_session_id.clone());
            active.native_session_id = Set(Some(proposed_provider_session_id.clone()));
            active.native_root_thread_id = Set(request.thread_binding.native_root_thread_id);
            active.native_cwd = Set(request.thread_binding.native_cwd);
            active.native_model = Set(request.thread_binding.native_model);
            active.resume_cursor_json = Set(request.thread_binding.resume_cursor_json);
            active.frozen_manifest_id = Set(root.locator());
            active.provider_session_id = Set(Some(proposed_provider_session_id));
            active.provider_session_lifecycle_state = Set(Some(
                CliRuntimeProviderSessionLifecycle::Prepared
                    .as_str()
                    .to_owned(),
            ));
            active.provider_session_last_verified_process_generation = Set(None);
            active.updated_at = Set(request.thread_binding.updated_at);
            let binding = active
                .update(db)
                .await
                .context("failed to replace Claude provider session binding")?;
            return Ok(PreparedClaudeProviderSessionBinding {
                binding: thread_binding_record_from_model(binding)?,
                mode: PreparedClaudeProviderSessionMode::New,
            });
        }
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

    root.verify(db, &request.thread_binding.workspace_id)
        .await?;
    let proposed_provider_session_id = request.proposed_provider_session_id;
    let mut active = active_thread_binding_from_new(request.thread_binding, root);
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
        .exec_without_returning(db)
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
    mapping: InputMapping,
    root: &impl FrozenBindingRoot,
) -> Result<CliRuntimeTurnBindingRecord> {
    root.verify(db, &binding.workspace_id).await?;
    let turn_id = binding.turn_id.clone();
    turn_cli_runtime_binding::Entity::insert(active_turn_binding_from_new(binding, mapping, root)?)
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
                    turn_cli_runtime_binding::Column::InputMappingCodec,
                    turn_cli_runtime_binding::Column::InputMappingBytes,
                    turn_cli_runtime_binding::Column::InputMappingChars,
                    turn_cli_runtime_binding::Column::InputMappingSha256,
                    turn_cli_runtime_binding::Column::InputMappingHasInlineHistory,
                    turn_cli_runtime_binding::Column::FrozenManifestId,
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

/// Control-only projection used by recovery/activation/terminal transactions.
/// Root identity and sent mapping were loaded in that same writer and remain
/// untouched, including a legacy unknown locator awaiting reconciliation.
pub async fn update_turn_control<C: ConnectionTrait>(
    db: &C,
    binding: NewCliRuntimeTurnBinding,
) -> Result<CliRuntimeTurnBindingRecord> {
    update_turn_control_projection(db, binding, true).await
}

pub async fn update_turn_control_projection<C: ConnectionTrait>(
    db: &C,
    binding: NewCliRuntimeTurnBinding,
    capture_mapping: bool,
) -> Result<CliRuntimeTurnBindingRecord> {
    let id = binding.turn_id;
    let result = turn_cli_runtime_binding::Entity::update_many()
        .col_expr(
            turn_cli_runtime_binding::Column::NativeTurnId,
            Expr::value(binding.native_turn_id),
        )
        .col_expr(
            turn_cli_runtime_binding::Column::RequestId,
            Expr::value(binding.request_id),
        )
        .col_expr(
            turn_cli_runtime_binding::Column::Status,
            Expr::value(binding.status),
        )
        .col_expr(
            turn_cli_runtime_binding::Column::UpdatedAt,
            Expr::value(binding.updated_at),
        )
        .filter(turn_cli_runtime_binding::Column::TurnId.eq(&id))
        .filter(turn_cli_runtime_binding::Column::WorkspaceId.eq(binding.workspace_id))
        .filter(turn_cli_runtime_binding::Column::ThreadId.eq(binding.thread_id))
        .filter(
            turn_cli_runtime_binding::Column::ContinuationThreadId
                .eq(binding.continuation_thread_id),
        )
        .exec(db)
        .await?;
    anyhow::ensure!(result.rows_affected == 1, "CLI control identity changed");
    find_turn_binding_projection(db, &id, capture_mapping)
        .await?
        .context("CLI control binding missing")
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

// Control reads deliberately omit logical mapping bytes and never acquire a
// payload hold. A real content reader must reload through the ordinary API.
fn turn_control_query(
    query: sea_orm::Select<turn_cli_runtime_binding::Entity>,
) -> sea_orm::Select<turn_cli_runtime_binding::Entity> {
    use turn_cli_runtime_binding::Column;
    query
        .select_only()
        .columns(Column::iter().filter(|column| !matches!(column, Column::InputMappingJson)))
        .column_as(Expr::val(Vec::<u8>::new()), "input_mapping_json")
}

pub async fn find_turn_binding_projection<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    capture_mapping: bool,
) -> Result<Option<CliRuntimeTurnBindingRecord>> {
    if capture_mapping {
        find_turn_binding(db, turn_id).await
    } else {
        find_turn_binding_control(db, turn_id).await
    }
}

pub async fn find_turn_binding_control<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<CliRuntimeTurnBindingRecord>> {
    turn_control_query(turn_cli_runtime_binding::Entity::find_by_id(
        turn_id.to_owned(),
    ))
    .into_model::<turn_cli_runtime_binding::Model>()
    .one(db)
    .await?
    .map(|model| turn_binding_record_from_model_inner(model, false))
    .transpose()
}

/// Project provider identity for a bounded timeline page without loading native payloads.
pub async fn load_runtime_kinds_for_turns<C: ConnectionTrait>(
    db: &C,
    turn_ids: &[String],
) -> Result<std::collections::BTreeMap<String, pioneer_protocol::CLIAgentRuntimeKind>> {
    if turn_ids.is_empty() {
        return Ok(Default::default());
    }
    let rows = turn_cli_runtime_binding::Entity::find()
        .select_only()
        .columns([
            turn_cli_runtime_binding::Column::TurnId,
            turn_cli_runtime_binding::Column::RuntimeKind,
        ])
        .filter(turn_cli_runtime_binding::Column::TurnId.is_in(turn_ids.iter().cloned()))
        .into_tuple::<(String, String)>()
        .all(db)
        .await?;
    rows.into_iter()
        .map(|(turn_id, kind)| {
            let kind = serde_json::from_value(serde_json::Value::String(kind))
                .context("invalid persisted CLI provider kind")?;
            Ok((turn_id, kind))
        })
        .collect()
}

/// A receipt cannot rule out a newer dispatch (including an unknown RPC
/// outcome). Use the continuation-owner index and return only one identifier;
/// never load or parse the thread's older input mappings. Timestamp ties are
/// deliberately inconclusive.
pub async fn has_dispatch_at_or_after<C: ConnectionTrait>(
    db: &C,
    source: &CliRuntimeTurnBindingRecord,
) -> Result<bool> {
    use turn_cli_runtime_binding::Column;
    Ok(turn_cli_runtime_binding::Entity::find()
        .select_only()
        .column(Column::TurnId)
        .filter(Column::WorkspaceId.eq(source.workspace_id.clone()))
        .filter(Column::RuntimeId.eq(source.runtime_id.clone()))
        .filter(Column::ContinuationThreadId.eq(source.continuation_thread_id.clone()))
        .filter(Column::CreatedAt.gte(source.created_at))
        .filter(Column::TurnId.ne(source.turn_id.clone()))
        .into_tuple::<String>()
        .one(db)
        .await
        .context("failed to check newer CLI dispatch evidence")?
        .is_some())
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

pub async fn set_turn_native_goal_state_projection<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    status: Option<String>,
    native_goal_turn_id: Option<String>,
    observed_at: DateTimeWithTimeZone,
    capture_mapping: bool,
) -> Result<CliRuntimeTurnBindingRecord> {
    if status
        .as_deref()
        .is_some_and(|status| status.trim().is_empty())
    {
        return Err(anyhow!("CLI runtime native Goal status cannot be empty"));
    }
    use turn_cli_runtime_binding::Column;
    let result = turn_cli_runtime_binding::Entity::update_many()
        .col_expr(Column::NativeGoalStatus, Expr::value(status))
        .col_expr(Column::NativeGoalTurnId, Expr::value(native_goal_turn_id))
        .col_expr(Column::NativeGoalObservedAt, Expr::value(Some(observed_at)))
        .col_expr(Column::UpdatedAt, Expr::value(observed_at))
        .filter(Column::TurnId.eq(turn_id))
        .exec(db)
        .await
        .context("failed to update CLI runtime native Goal state")?;
    anyhow::ensure!(
        result.rows_affected == 1,
        "CLI runtime turn binding is missing for native Goal update"
    );
    find_turn_binding_projection(db, turn_id, capture_mapping)
        .await?
        .context("CLI runtime turn binding is missing after native Goal update")
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

// One equality range per status; the row-value seek matches the global
// (status, created_at, turn_id) index, including timestamp ties.
pub fn active_turn_binding_page_query(
    status: CliRuntimeActiveTurnBindingStatus,
    after: Option<&(DateTimeWithTimeZone, String)>,
) -> sea_orm::Select<turn_cli_runtime_binding::Entity> {
    use turn_cli_runtime_binding::Column;
    let mut query =
        turn_cli_runtime_binding::Entity::find().filter(Column::Status.eq(status.as_str()));
    if let Some((created_at, turn_id)) = after {
        query = query.filter(
            Expr::tuple([Expr::col(Column::CreatedAt), Expr::col(Column::TurnId)]).gt(Expr::tuple(
                [Expr::value(*created_at), Expr::value(turn_id.clone())],
            )),
        );
    }
    query
        .order_by_asc(Column::CreatedAt)
        .order_by_asc(Column::TurnId)
        .limit(CLI_RUNTIME_ACTIVE_TURN_BINDING_PAGE_MAX)
}

pub async fn list_active_turn_binding_page<C: ConnectionTrait>(
    db: &C,
    status: CliRuntimeActiveTurnBindingStatus,
    after: Option<&(DateTimeWithTimeZone, String)>,
) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
    active_turn_binding_page_query(status, after)
        .all(db)
        .await
        .context("failed to list active CLI runtime turn binding page")?
        .into_iter()
        .map(turn_binding_record_from_model)
        .collect()
}

/// Capture a finite active-set frontier; later mutations remain on the owner's
/// unacknowledged work signal and are revisited in the next pass.
pub async fn active_turn_binding_frontier<C: ConnectionTrait>(
    db: &C,
    status: CliRuntimeActiveTurnBindingStatus,
) -> Result<Option<(DateTimeWithTimeZone, String)>> {
    use turn_cli_runtime_binding::Column;
    Ok(turn_cli_runtime_binding::Entity::find()
        .filter(Column::Status.eq(status.as_str()))
        .order_by_desc(Column::CreatedAt)
        .order_by_desc(Column::TurnId)
        .limit(1)
        .one(db)
        .await?
        .map(|row| (row.created_at, row.turn_id)))
}

pub async fn list_active_turn_binding_page_through<C: ConnectionTrait>(
    db: &C,
    status: CliRuntimeActiveTurnBindingStatus,
    after: Option<&(DateTimeWithTimeZone, String)>,
    through: &(DateTimeWithTimeZone, String),
) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
    use turn_cli_runtime_binding::Column;
    active_turn_binding_page_query(status, after)
        .filter(
            Expr::tuple([Expr::col(Column::CreatedAt), Expr::col(Column::TurnId)]).lte(
                Expr::tuple([Expr::value(through.0), Expr::value(through.1.clone())]),
            ),
        )
        .all(db)
        .await?
        .into_iter()
        .map(turn_binding_record_from_model)
        .collect()
}

pub async fn list_active_turn_binding_control_page_through<C: ConnectionTrait>(
    db: &C,
    status: CliRuntimeActiveTurnBindingStatus,
    after: Option<&(DateTimeWithTimeZone, String)>,
    through: &(DateTimeWithTimeZone, String),
) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
    use turn_cli_runtime_binding::Column;
    let query = active_turn_binding_page_query(status, after).filter(
        Expr::tuple([Expr::col(Column::CreatedAt), Expr::col(Column::TurnId)]).lte(Expr::tuple([
            Expr::value(through.0),
            Expr::value(through.1.clone()),
        ])),
    );
    turn_control_query(query)
        .into_model::<turn_cli_runtime_binding::Model>()
        .all(db)
        .await?
        .into_iter()
        .map(|model| turn_binding_record_from_model_inner(model, false))
        .collect()
}

pub async fn list_turn_bindings<C: ConnectionTrait>(
    db: &C,
    filter: CliRuntimeTurnBindingListFilter,
) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
    list_turn_binding_records(db, filter, true).await
}

pub async fn list_turn_binding_controls<C: ConnectionTrait>(
    db: &C,
    filter: CliRuntimeTurnBindingListFilter,
) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
    list_turn_binding_records(db, filter, false).await
}

async fn list_turn_binding_records<C: ConnectionTrait>(
    db: &C,
    filter: CliRuntimeTurnBindingListFilter,
    capture_mapping: bool,
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

    let query = if capture_mapping {
        query
    } else {
        turn_control_query(query)
    };
    query
        .order_by_asc(turn_cli_runtime_binding::Column::CreatedAt)
        .into_model::<turn_cli_runtime_binding::Model>()
        .all(db)
        .await
        .context("failed to list CLI runtime turn bindings")?
        .into_iter()
        .map(|model| turn_binding_record_from_model_inner(model, capture_mapping))
        .collect()
}

pub async fn resolve_native_turn_owner<C: ConnectionTrait>(
    db: &C,
    runtime_id: &str,
    native_turn_id: &str,
) -> Result<Option<CliRuntimeNativeTurnOwner>> {
    resolve_native_turn_owner_record(db, runtime_id, native_turn_id, true).await
}

pub async fn resolve_native_turn_control_owner<C: ConnectionTrait>(
    db: &C,
    runtime_id: &str,
    native_turn_id: &str,
) -> Result<Option<CliRuntimeNativeTurnOwner>> {
    resolve_native_turn_owner_record(db, runtime_id, native_turn_id, false).await
}

async fn resolve_native_turn_owner_record<C: ConnectionTrait>(
    db: &C,
    runtime_id: &str,
    native_turn_id: &str,
    capture_mapping: bool,
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
    let binding = if capture_mapping {
        find_turn_binding(db, attempt.turn_id.as_str()).await?
    } else {
        find_turn_binding_control(db, attempt.turn_id.as_str()).await?
    }
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

fn active_thread_binding_from_new(
    binding: NewCliRuntimeThreadBinding,
    root: &impl FrozenBindingRoot,
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
        frozen_manifest_id: Set(root.locator()),
        status: Set(binding.status),
        created_at: Set(binding.created_at),
        updated_at: Set(binding.updated_at),
        ..Default::default()
    }
}

fn active_turn_binding_from_new(
    binding: NewCliRuntimeTurnBinding,
    mapping: InputMapping,
    root: &impl FrozenBindingRoot,
) -> Result<turn_cli_runtime_binding::ActiveModel> {
    let mut model = turn_cli_runtime_binding::ActiveModel {
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
        sandbox_json: Set(binding
            .sandbox_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?),
        approval_policy: Set(binding.approval_policy),
        frozen_manifest_id: Set(root.locator()),
        created_at: Set(binding.created_at),
        updated_at: Set(binding.updated_at),
        ..Default::default()
    };
    mapping.set(&mut model)?;
    Ok(model)
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
    turn_binding_record_from_model_inner(model, true)
}

fn turn_binding_record_from_model_inner(
    mut model: turn_cli_runtime_binding::Model,
    capture_mapping: bool,
) -> Result<CliRuntimeTurnBindingRecord> {
    let mcp = turn_mcp_metadata_from_model(&model);
    let input_mapping_frame = capture_mapping
        .then(|| InputMapping::take(&mut model))
        .transpose()?;
    Ok(CliRuntimeTurnBindingRecord {
        input_mapping_frame,
        input_mapping_has_inline_history: model.input_mapping_has_inline_history,
        input_mapping_revision: model.portable_revision,
        input_mapping_identity: model.portable_identity,
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
        sandbox_json: model.sandbox_json.map(|value| value.to_string()),
        approval_policy: model.approval_policy,
        input_mapping_json: String::new(),
        mcp,
        native_goal_status: model.native_goal_status,
        native_goal_turn_id: model.native_goal_turn_id,
        native_goal_observed_at: model.native_goal_observed_at,
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

/// Decode a body captured by the source query after releasing database capacity.
pub async fn hydrate_record(
    _db: &pioneer_postgres_pools::PostgresDatabase,
    mut record: CliRuntimeTurnBindingRecord,
) -> Result<CliRuntimeTurnBindingRecord> {
    if let Some(mapping) = record.input_mapping_frame.take() {
        record.input_mapping_json = mapping.decode()?;
    }
    Ok(record)
}

/// Physical cutover changes reference/projection, not logical revision. Writer
/// guards compare the recorded logical version and every original control fact
/// without hashing/parsing/decompressing large mapping bytes in a transaction.
pub fn same_binding_version(
    stored: &CliRuntimeTurnBindingRecord,
    expected: &CliRuntimeTurnBindingRecord,
) -> bool {
    stored.input_mapping_identity == expected.input_mapping_identity
        && stored.input_mapping_revision == expected.input_mapping_revision
        && same_binding_control(stored, expected)
}

// Public record equality remains equality of the original logical fields.
// A physical reference or storage revision is not part of the public value.
impl PartialEq for CliRuntimeTurnBindingRecord {
    fn eq(&self, other: &Self) -> bool {
        same_binding_control(self, other) && self.input_mapping_json == other.input_mapping_json
    }
}
impl Eq for CliRuntimeTurnBindingRecord {}

fn same_binding_control(
    stored: &CliRuntimeTurnBindingRecord,
    expected: &CliRuntimeTurnBindingRecord,
) -> bool {
    stored.turn_id == expected.turn_id
        && stored.thread_id == expected.thread_id
        && stored.continuation_thread_id == expected.continuation_thread_id
        && stored.workspace_id == expected.workspace_id
        && stored.runtime_id == expected.runtime_id
        && stored.runtime_kind == expected.runtime_kind
        && stored.native_thread_id == expected.native_thread_id
        && stored.native_turn_id == expected.native_turn_id
        && stored.request_id == expected.request_id
        && stored.status == expected.status
        && stored.model == expected.model
        && stored.cwd == expected.cwd
        && stored.sandbox_json == expected.sandbox_json
        && stored.approval_policy == expected.approval_policy
        && stored.mcp == expected.mcp
        && stored.native_goal_status == expected.native_goal_status
        && stored.native_goal_turn_id == expected.native_goal_turn_id
        && stored.native_goal_observed_at == expected.native_goal_observed_at
        && stored.created_at == expected.created_at
        && stored.updated_at == expected.updated_at
}
