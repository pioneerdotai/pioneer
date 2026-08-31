use super::agent_runtime::TurnFailureRecoveryKind;
use super::*;
use crate::authorization::{
    AuthorizationExternalError, AuthorizationService, AuthorizedTurn,
    ExecutionAuthorizationAdmission, RuntimeDraftCreator, RuntimeDraftMaterialization,
};
use pioneer_protocol::{
    AgentExecutionBackend, CLIAgentRuntimeKind, UserInput, VoiceError, VoiceErrorKind,
    VoiceSessionOutcome, VoiceSessionResultNotification,
};
use serde_json::json;

pub(super) struct PreparedApiProviderTurnStart {
    outcome: crate::thread::TurnStartOutcome,
    user_message_capability_attachments: Vec<pioneer_protocol::UserMessageAttachment>,
    workspace_skill_policies:
        HashMap<pioneer_skills::SkillPolicyKey, pioneer_agent::WorkspaceSkillPolicy>,
    skill_catalog: pioneer_skills::SkillCatalogSnapshot,
    agent_skill_overlay: Vec<pioneer_skills::AgentSkillRuntimeEntry>,
    resolved_artifacts: Vec<ResolvedArtifactInput>,
    runtime_environment: HashMap<String, String>,
    history: Vec<ChatMessage>,
    effective_reasoning_effort: Option<String>,
    permission_profile: pioneer_protocol::TurnPermissionProfileSnapshot,
    execution_security_snapshot: pioneer_protocol::TurnExecutionSecuritySnapshot,
}

pub(super) enum ApiProviderTurnAdmission {
    New(PreparedApiProviderTurnStart),
    Replay(pioneer_protocol::TurnStartResponse),
}

#[derive(Debug)]
pub(super) struct TurnStartFailure {
    public_code: pioneer_protocol::PublicErrorCode,
    diagnostic: String,
}

impl TurnStartFailure {
    fn new(public_code: pioneer_protocol::PublicErrorCode, diagnostic: impl Into<String>) -> Self {
        Self {
            public_code,
            diagnostic: diagnostic.into(),
        }
    }

    fn invalid_input(diagnostic: impl Into<String>) -> Self {
        Self::new(pioneer_protocol::PublicErrorCode::InvalidInput, diagnostic)
    }

    fn policy_denied(diagnostic: impl Into<String>) -> Self {
        Self::new(pioneer_protocol::PublicErrorCode::PolicyDenied, diagnostic)
    }

    fn unavailable(diagnostic: impl Into<String>) -> Self {
        Self::new(pioneer_protocol::PublicErrorCode::Unavailable, diagnostic)
    }

    fn conflict(diagnostic: impl Into<String>) -> Self {
        Self::new(pioneer_protocol::PublicErrorCode::Conflict, diagnostic)
    }

    fn internal(diagnostic: impl Into<String>) -> Self {
        Self::new(pioneer_protocol::PublicErrorCode::Internal, diagnostic)
    }

    #[cfg(test)]
    pub(super) const fn public_code(&self) -> pioneer_protocol::PublicErrorCode {
        self.public_code
    }

    #[cfg(test)]
    pub(super) fn diagnostic(&self) -> &str {
        self.diagnostic.as_str()
    }
}

impl From<String> for TurnStartFailure {
    fn from(diagnostic: String) -> Self {
        Self::internal(diagnostic)
    }
}

impl From<&'static str> for TurnStartFailure {
    fn from(diagnostic: &'static str) -> Self {
        Self::internal(diagnostic)
    }
}

impl std::fmt::Display for TurnStartFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.diagnostic.as_str())
    }
}

/// Positive authority proof for every execution-capable Turn start.
///
/// Interactive and voice starts carry a fresh admission which is finalized at
/// the durable materialization boundary. Task continuations carry the already
/// persisted and revalidated parent context. Keeping these states in one
/// exhaustive enum makes the forbidden `None/None` and ambiguous `Some/Some`
/// combinations unrepresentable.
#[derive(Clone)]
pub(super) enum TurnExecutionAuthority {
    Fresh(ExecutionAuthorizationAdmission),
    Durable {
        context: crate::authorization::ExecutionAuthorizationContext,
        revalidation: std::sync::Arc<crate::authorization::RevalidatedExecutionAuthorization>,
    },
}

#[derive(Clone, Copy)]
enum ExecutionEnvelopeSource<'a> {
    Fresh(&'a ExecutionAuthorizationAdmission),
    Durable {
        context: &'a crate::authorization::ExecutionAuthorizationContext,
        revalidation: &'a crate::authorization::RevalidatedExecutionAuthorization,
    },
}

impl TurnExecutionAuthority {
    fn source(&self) -> ExecutionEnvelopeSource<'_> {
        match self {
            Self::Fresh(admission) => ExecutionEnvelopeSource::Fresh(admission),
            Self::Durable {
                context,
                revalidation,
            } => ExecutionEnvelopeSource::Durable {
                context,
                revalidation: revalidation.as_ref(),
            },
        }
    }
}

impl<'a> ExecutionEnvelopeSource<'a> {
    fn policy_revision(self) -> u64 {
        match self {
            Self::Fresh(admission) => admission.policy_provenance().1,
            Self::Durable { revalidation, .. } => revalidation.validated_policy_generation(),
        }
    }

    fn runtime_draft(self) -> Option<&'a RuntimeDraftMaterialization> {
        match self {
            Self::Fresh(admission) => admission.runtime_draft(),
            Self::Durable { .. } => None,
        }
    }

    fn effective_native_event_resource_budget(
        self,
    ) -> anyhow::Result<pioneer_cli_agent_runtime::NativeEventBudget> {
        match self {
            Self::Fresh(admission) => Ok(admission.native_event_resource_budget()),
            Self::Durable { context, .. } => context.effective_native_event_resource_budget(),
        }
    }
}

fn public_turn_start_error(
    request_id: RequestId,
    failure: impl Into<TurnStartFailure>,
) -> JsonRpcErrorResponse {
    let failure = failure.into();
    crate::public_error::agent_rpc_error(
        Some(request_id),
        INVALID_REQUEST_CODE,
        failure.public_code,
        pioneer_protocol::PublicErrorStage::Admission,
        failure.diagnostic,
    )
}

/// Returns the largest sequence-preserving prefix that fits the response-page
/// target. A single event is always returned intact so pagination can never
/// make durable history unreadable.
fn fit_turn_items_page_to_budget(
    page: pioneer_protocol::TurnItemsResponse,
    requested_after_sequence: Option<i64>,
    max_items: usize,
    max_encoded_bytes: usize,
) -> Result<pioneer_protocol::TurnItemsResponse, String> {
    fn encoded_len(page: &pioneer_protocol::TurnItemsResponse) -> Result<usize, String> {
        serde_json::to_vec(page)
            .map(|encoded| encoded.len())
            .map_err(|_| "turn/items page could not be encoded".to_owned())
    }

    fn prefix(
        page: &pioneer_protocol::TurnItemsResponse,
        count: usize,
        requested_cursor: i64,
    ) -> Result<pioneer_protocol::TurnItemsResponse, String> {
        let mut candidate = page.clone();
        candidate.events.truncate(count);
        if count < page.events.len() {
            let cursor = candidate
                .events
                .last()
                .map(|event| event.sequence)
                .unwrap_or(requested_cursor);
            if cursor <= requested_cursor {
                return Err("turn/items page could not advance its cursor".to_owned());
            }
            candidate.last_sequence = cursor;
            candidate.has_more = true;
            candidate.next_cursor = Some(cursor);
        }
        Ok(candidate)
    }

    let max_items = max_items.max(1);
    let max_encoded_bytes = max_encoded_bytes.max(1);
    if page.events.len() <= max_items && encoded_len(&page)? <= max_encoded_bytes {
        return Ok(page);
    }

    let requested_cursor = requested_after_sequence.unwrap_or(0);
    let upper = page.events.len().min(max_items);
    if upper == 0 {
        return Ok(page);
    }

    let smallest = prefix(&page, 1, requested_cursor)?;
    if encoded_len(&smallest)? > max_encoded_bytes {
        return Ok(smallest);
    }

    let mut best = smallest;
    let mut low = 2usize;
    let mut high = upper;
    while low <= high {
        let middle = low + (high - low) / 2;
        let candidate = prefix(&page, middle, requested_cursor)?;
        if encoded_len(&candidate)? <= max_encoded_bytes {
            best = candidate;
            low = middle.saturating_add(1);
        } else {
            high = middle.saturating_sub(1);
        }
    }
    Ok(best)
}

fn public_turn_error(
    request_id: Option<RequestId>,
    jsonrpc_code: i64,
    stage: pioneer_protocol::PublicErrorStage,
    diagnostic: impl std::fmt::Display,
) -> JsonRpcErrorResponse {
    let public_code = if jsonrpc_code == INVALID_PARAMS_CODE {
        pioneer_protocol::PublicErrorCode::InvalidInput
    } else {
        pioneer_protocol::PublicErrorCode::Internal
    };
    crate::public_error::agent_rpc_error(request_id, jsonrpc_code, public_code, stage, diagnostic)
}

fn native_turn_admission_digest(
    actor: &pioneer_protocol::PersistedActorRef,
    params: &TurnStartParams,
) -> Result<String, String> {
    use sha2::{Digest, Sha256};

    let canonical = serde_json::to_vec(&(actor, params))
        .map_err(|error| format!("failed to encode native Turn admission identity: {error}"))?;
    Ok(hex::encode(Sha256::digest(canonical)))
}

fn cli_runtime_session_authorization_scope_fingerprint(
    context: &crate::authorization::ExecutionAuthorizationContext,
    security: &pioneer_protocol::TurnExecutionSecuritySnapshot,
) -> anyhow::Result<String> {
    use sha2::{Digest, Sha256};

    let authorization = context.cli_runtime_collaboration_scope_fingerprint()?;
    let canonical = serde_json::to_vec(&(
        authorization,
        &security.permission_profile,
        &security.authority_cap,
        &security.approval,
        &security.backend,
    ))
    .context("failed to encode CLI runtime session authorization scope")?;
    Ok(hex::encode(Sha256::digest(canonical)))
}

fn validate_root_agent_launch_matches_turn(
    params: &TurnStartParams,
) -> Result<(), TurnStartFailure> {
    let Some(launch) = params.agent_launch.as_ref() else {
        return Ok(());
    };
    if params.mode != Some(pioneer_protocol::ThreadMode::Agent) {
        return Err(TurnStartFailure::invalid_input(
            "agent_launch is valid only for an Agent Turn",
        ));
    }
    if launch.execution.reasoning != params.reasoning {
        return Err(TurnStartFailure::invalid_input(
            "root Agent launch reasoning differs from the Turn request",
        ));
    }
    if launch.execution.permission_profile.as_ref() != params.permission_profile.as_ref() {
        return Err(TurnStartFailure::invalid_input(
            "root Agent launch permission profile differs from the Turn request",
        ));
    }
    Ok(())
}

/// Compare a root launch with the already normalized execution capability
/// set. Skill packs have been expanded at this point and MCP tools are reduced
/// to their exact, scope-qualified server grant.
pub(super) fn validate_root_agent_launch_capabilities(
    params: &TurnStartParams,
) -> Result<(), TurnStartFailure> {
    let Some(launch) = params.agent_launch.as_ref() else {
        return Ok(());
    };
    let mut selected_skills = params
        .capabilities
        .iter()
        .filter_map(|capability| match &capability.kind {
            pioneer_protocol::TurnCapabilityKind::Skill { skill_id, .. } => Some(skill_id.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    selected_skills.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    selected_skills.dedup();
    let mut launch_skills = launch.execution.skill_ids.clone();
    launch_skills.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    if launch_skills.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(TurnStartFailure::invalid_input(
            "root Agent launch contains duplicate Skills",
        ));
    }
    launch_skills.dedup();
    if selected_skills != launch_skills {
        return Err(TurnStartFailure::invalid_input(
            "root Agent launch Skills differ from the Turn capabilities",
        ));
    }
    let mut selected_mcp = params
        .capabilities
        .iter()
        .filter_map(|capability| match &capability.kind {
            pioneer_protocol::TurnCapabilityKind::McpServer { name, scope_kind } => Some(
                pioneer_protocol::mcp_server_capability_key(*scope_kind, name),
            ),
            pioneer_protocol::TurnCapabilityKind::McpTool {
                server_name,
                scope_kind,
                ..
            } => Some(pioneer_protocol::mcp_server_capability_key(
                *scope_kind,
                server_name,
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    selected_mcp.sort();
    selected_mcp.dedup();
    let mut launch_mcp = launch.execution.mcp_server_ids.clone();
    launch_mcp.sort();
    if launch_mcp.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(TurnStartFailure::invalid_input(
            "root Agent launch contains duplicate MCP servers",
        ));
    }
    launch_mcp.dedup();
    if selected_mcp != launch_mcp {
        return Err(TurnStartFailure::invalid_input(
            "root Agent launch MCP selection differs from the Turn capabilities",
        ));
    }
    Ok(())
}

async fn persist_admitted_turn_start(
    crud_store: &pioneer_crud::CrudStore,
    provider_registry: &pioneer_provider::ProviderRegistry,
    execution_owner_id: &str,
    params: &TurnStartParams,
    materialization: &crate::thread::TurnStartMaterialization,
    reasoning_effort: Option<&str>,
    actor: pioneer_protocol::PersistedActorRef,
    audit_event: pioneer_protocol::TurnPermissionAuditEvent,
    execution_authority: ExecutionEnvelopeSource<'_>,
    request_digest: Option<String>,
    execution_security_snapshot: &pioneer_protocol::TurnExecutionSecuritySnapshot,
    security_audit_events: Vec<pioneer_protocol::TurnPermissionAuditEvent>,
    execution_graph: Option<pioneer_crud::AgentExecutionGraphCommitInput>,
    agent_turn_response: Option<pioneer_crud::AgentTurnResponseInput>,
) -> anyhow::Result<Option<pioneer_crud::AgentExecutionGraphCommitResult>> {
    let admission = match execution_authority {
        ExecutionEnvelopeSource::Fresh(admission) => {
            admission
                .validate_durable_start(crud_store, provider_registry)
                .await?;
            let (role_key, policy_generation, policy_fingerprint) = admission.policy_provenance();
            let request_digest = request_digest
                .map(Ok)
                .unwrap_or_else(|| native_turn_admission_digest(&actor, params))
                .map_err(anyhow::Error::msg)?;
            pioneer_crud::NewTurnAdmission {
                turn_id: materialization.turn.id.clone(),
                thread_id: materialization.thread.id.clone(),
                workspace_id: materialization.thread.workspace_id.clone(),
                request_digest,
                policy_generation: Some(policy_generation),
                role_key: Some(role_key.to_owned()),
                policy_fingerprint: Some(policy_fingerprint.to_owned()),
                // The root Turn owns the durable principal/workspace
                // admission lease.  Child Turns remain independently
                // authorized, but their resource coordination is recorded in
                // the root work graph rather than as another full quota lease.
                execution_lease: (materialization.thread.id == admission.root_thread_id())
                    .then(|| {
                        admission.execution_quota_lease("turn", materialization.turn.id.as_str())
                    })
                    .transpose()?,
            }
        }
        ExecutionEnvelopeSource::Durable {
            context,
            revalidation,
        } => {
            context.verify_current_provider_authority(provider_registry)?;
            context.durable_turn_admission_after_revalidation(
                materialization.thread.id.as_str(),
                materialization.turn.id.as_str(),
                params.execution_backend.as_ref(),
                revalidation,
            )?
        }
    };
    let authorization_context = match execution_authority {
        ExecutionEnvelopeSource::Durable { context, .. } => context.clone(),
        ExecutionEnvelopeSource::Fresh(admission) => admission.finalize(
            materialization.thread.workspace_id.as_str(),
            materialization.thread.id.as_str(),
            materialization.thread.model_provider.as_str(),
            materialization.thread.model.as_str(),
            params.execution_backend.as_ref(),
            materialization.capabilities.as_slice(),
            &execution_security_snapshot.permission_profile,
        )?,
    };
    if authorization_context.workspace_id() != materialization.thread.workspace_id {
        anyhow::bail!("execution authority envelope differs from materialized Turn workspace");
    }
    if authorization_context.root_thread_id() != materialization.thread.id {
        let lineage = crud_store
            .get_task_thread_lineage(materialization.thread.id.as_str())
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("non-root execution Turn has no durable internal thread lineage")
            })?;
        if lineage.child_thread_id != materialization.thread.id
            || lineage.root_thread_id != authorization_context.root_thread_id()
            || lineage.root_thread_id == lineage.child_thread_id
        {
            anyhow::bail!("execution authority envelope differs from materialized Turn scope");
        }
    }
    let authority_envelope_json = authorization_context.to_persisted_json()?;
    let runtime_draft = execution_authority
        .runtime_draft()
        .map(RuntimeDraftMaterialization::creator)
        .map(|creator| match creator {
            RuntimeDraftCreator::ScopedPrincipal {
                gateway_id,
                principal_id,
                access_class,
            } => pioneer_crud::AuthorizedTurnRuntimeDraft::ScopedPrincipal {
                gateway_id: gateway_id.clone(),
                principal_id: principal_id.clone(),
                access_class: *access_class,
            },
            RuntimeDraftCreator::Absolute { access_class } => {
                pioneer_crud::AuthorizedTurnRuntimeDraft::Absolute {
                    access_class: *access_class,
                }
            }
        });
    let execution = new_turn_execution(
        execution_owner_id,
        params.execution_backend.as_ref(),
        materialization,
    )?;
    // The admitted Turn and its execution graph form one durable transaction,
    // but the transaction does not need to inherit the RPC dispatch poll stack.
    // Own its inputs and poll the complete commit from a fresh runtime task.
    let crud_store = crud_store.clone();
    let materialization = materialization.clone();
    let reasoning_effort = reasoning_effort.map(str::to_owned);
    let execution_security_snapshot = execution_security_snapshot.clone();
    message_fresh_task(async move {
        crud_store
            .materialize_authorized_turn_start_with_reasoning_effort_and_permission_audit(
                &materialization.thread,
                materialization.sandbox_mode,
                &materialization.turn,
                &materialization.input,
                reasoning_effort.as_deref(),
                actor,
                audit_event,
                authority_envelope_json.as_str(),
                runtime_draft,
                Some(admission),
                Some(execution),
                &execution_security_snapshot,
                security_audit_events,
                execution_graph,
                agent_turn_response,
            )
            .await
    })
    .await
    .map_err(|error| anyhow::anyhow!("admitted turn/start persistence task failed: {error}"))?
}

pub(super) fn new_turn_execution(
    execution_owner_id: &str,
    backend: Option<&AgentExecutionBackend>,
    materialization: &crate::thread::TurnStartMaterialization,
) -> anyhow::Result<pioneer_crud::NewTurnExecution> {
    let created_at = chrono::Utc::now().fixed_offset();
    let (executor_kind, executor_key) = match backend {
        None => (
            pioneer_crud::TurnExecutorKind::NativeAgent,
            Some(materialization.thread.model_provider.clone()),
        ),
        Some(AgentExecutionBackend::ApiProvider { provider }) => (
            pioneer_crud::TurnExecutorKind::ApiProvider,
            Some(provider.clone()),
        ),
        Some(AgentExecutionBackend::CLIAgentRuntime { runtime_id, .. }) => (
            pioneer_crud::TurnExecutorKind::CliRuntime,
            Some(runtime_id.clone()),
        ),
        Some(AgentExecutionBackend::ACPAgentRuntime { runtime_id }) => (
            pioneer_crud::TurnExecutorKind::AcpRuntime,
            Some(runtime_id.clone()),
        ),
    };
    Ok(pioneer_crud::NewTurnExecution {
        turn_id: materialization.turn.id.clone(),
        thread_id: materialization.thread.id.clone(),
        workspace_id: materialization.thread.workspace_id.clone(),
        executor_kind,
        executor_key,
        status: pioneer_crud::TurnExecutionStatus::Starting,
        owner_id: execution_owner_id.to_owned(),
        lease_until: created_at
            + chrono::Duration::seconds(super::TURN_EXECUTION_OWNER_LEASE_SECONDS),
        created_at,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct NormalizedTurnCapabilities {
    pub(super) presentation: Vec<pioneer_protocol::TurnCapability>,
    pub(super) execution: Vec<pioneer_protocol::TurnCapability>,
    pub(super) pack_names: HashMap<pioneer_protocol::SkillPackId, String>,
}

pub(super) struct PreparedCliRuntimeNativeTurnStart {
    outcome: crate::thread::TurnStartOutcome,
    user_message_capability_attachments: Vec<pioneer_protocol::UserMessageAttachment>,
    session_instance: crate::cli_runtime::session_instance::CliSessionInstanceId,
    cli_session: std::sync::Arc<dyn crate::cli_runtime::manager::CLIAgentRuntimeSession>,
    native_thread_id: String,
    turn_start_params: crate::cli_runtime::manager::CLIAgentRuntimeTurnStartParams,
    request_timeout_ms: u64,
}

struct PreparedCliRuntimeCombinedPreflight {
    plan: crate::cli_runtime::skills::CliRuntimeCombinedPreflightPlan,
    codex_mcp_launch_projection:
        Option<crate::cli_runtime::codex_mcp::CodexMcpSessionLaunchProjection>,
    claude_mcp_launch_projection:
        Option<crate::cli_runtime::claude_mcp::ClaudeMcpSessionLaunchProjection>,
}

struct CliRuntimeAdmissionPhase {
    thread: pioneer_protocol::Thread,
    normalized_presentation_capabilities: Vec<pioneer_protocol::TurnCapability>,
    normalized_pack_names: HashMap<pioneer_protocol::SkillPackId, String>,
    manager: std::sync::Arc<crate::cli_runtime::manager::CLIAgentRuntimeManager>,
    continuation_thread_id: String,
    context_thread_id: String,
    session_key: crate::cli_runtime::manager::CLIAgentRuntimeSessionKey,
    session_turn_lease: tokio::sync::OwnedMutexGuard<()>,
    combined_preflight: crate::cli_runtime::skills::CliRuntimeCombinedPreflightPlan,
    codex_mcp_launch_projection:
        Option<crate::cli_runtime::codex_mcp::CodexMcpSessionLaunchProjection>,
    claude_mcp_launch_projection:
        Option<crate::cli_runtime::claude_mcp::ClaudeMcpSessionLaunchProjection>,
}

struct CliRuntimeStartedPhase {
    outcome: crate::thread::TurnStartOutcome,
    user_message_capability_attachments: Vec<pioneer_protocol::UserMessageAttachment>,
    proxy_url: Option<String>,
    input_mapping: pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputMapping,
    effective_approval_policy: String,
    sandbox_policy_value: Option<JsonValue>,
    provider_permissions_id: Option<String>,
    effective_cli_runtime_effort: Option<String>,
    cli_runtime_personality: Option<String>,
    cli_runtime_summary: Option<String>,
    security_params: TurnStartParams,
}

struct CliRuntimeMaterializedPhase {
    security_snapshot: pioneer_protocol::TurnExecutionSecuritySnapshot,
    installed_skills: Vec<crate::cli_runtime::skills::CliRuntimeSkillInstallResult>,
    native_event_budget: pioneer_cli_agent_runtime::NativeEventBudget,
}

pub(super) struct RestoredCliRuntimeLaunchSpec {
    pub(super) binding: pioneer_crud::CliRuntimeTurnBindingRecord,
    pub(super) runtime_kind: CLIAgentRuntimeKind,
    pub(super) runtime: pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig,
    pub(super) session_key: crate::cli_runtime::manager::CLIAgentRuntimeSessionKey,
    pub(super) launch_spec: crate::cli_runtime::continuation::CliSessionLaunchSpec,
    pub(super) native_cwd: String,
    pub(super) sandbox: Option<JsonValue>,
    pub(super) permissions: Option<String>,
    pub(super) elevated_instructions:
        pioneer_cli_agent_runtime::instructions::CLIRuntimeElevatedInstructions,
}

pub(super) enum CliRuntimeLaunchSpecRestore {
    Ready(RestoredCliRuntimeLaunchSpec),
    Unavailable { diagnostic: String },
    InvalidBinding { diagnostic: String },
}

pub(super) enum CliRuntimeRecoveryStartFailure {
    Unavailable { diagnostic: String },
    InvalidBinding { diagnostic: String },
}

impl From<anyhow::Error> for CliRuntimeRecoveryStartFailure {
    fn from(error: anyhow::Error) -> Self {
        Self::Unavailable {
            diagnostic: format!("{error:#}"),
        }
    }
}

struct ComposerDetachedStartedPhase {
    launch: TurnStartParams,
    outcome: crate::thread::TurnStartOutcome,
    capability_attachments: Vec<pioneer_protocol::UserMessageAttachment>,
    profile_audit: pioneer_protocol::TurnPermissionAuditEvent,
}

struct ComposerDetachedMaterializedPhase {
    launch: TurnStartParams,
    outcome: crate::thread::TurnStartOutcome,
    capability_attachments: Vec<pioneer_protocol::UserMessageAttachment>,
    security_snapshot: pioneer_protocol::TurnExecutionSecuritySnapshot,
}

struct ComposerDetachedTaskCreatedPhase {
    launch: TurnStartParams,
    outcome: crate::thread::TurnStartOutcome,
    capability_attachments: Vec<pioneer_protocol::UserMessageAttachment>,
}

fn task_creator_actor_id(actor: &pioneer_protocol::PersistedActorRef) -> Option<String> {
    match actor {
        pioneer_protocol::PersistedActorRef::Principal(principal_id) => {
            Some(principal_id.to_string())
        }
        pioneer_protocol::PersistedActorRef::AgentExecution(execution_id) => {
            Some(execution_id.to_string())
        }
        pioneer_protocol::PersistedActorRef::System => None,
    }
}

pub(super) enum TurnStartSuccessResponse {
    TurnStart,
    VoiceSessionFinalizeAccepted {
        session_id: String,
    },
    Task {
        permission_profile: pioneer_protocol::TurnPermissionProfileSnapshot,
        execution_security_snapshot: pioneer_protocol::TurnExecutionSecuritySnapshot,
        continuation_thread_id: String,
        context_thread_id: String,
        task_run_id: String,
        execution_id: String,
        conversation_history: Vec<ChatMessage>,
        agent_author: Option<pioneer_protocol::TurnAuthorSnapshot>,
        agent_turn_response: pioneer_crud::AgentTurnResponseInput,
        completion: std::sync::Arc<
            std::sync::Mutex<
                Option<
                    tokio::sync::oneshot::Sender<anyhow::Result<PreparedCliRuntimeNativeTurnStart>>,
                >,
            >,
        >,
    },
    DurableAgent {
        permission_profile: pioneer_protocol::TurnPermissionProfileSnapshot,
        execution_security_snapshot: pioneer_protocol::TurnExecutionSecuritySnapshot,
        continuation_thread_id: String,
        context_thread_id: String,
        conversation_history: Vec<ChatMessage>,
        agent_author: pioneer_protocol::TurnAuthorSnapshot,
        completion: std::sync::Arc<
            std::sync::Mutex<
                Option<
                    tokio::sync::oneshot::Sender<anyhow::Result<PreparedCliRuntimeNativeTurnStart>>,
                >,
            >,
        >,
    },
}

impl TurnStartSuccessResponse {
    fn continuation_thread_id<'a>(&'a self, execution_thread_id: &'a str) -> &'a str {
        match self {
            Self::Task {
                continuation_thread_id,
                ..
            }
            | Self::DurableAgent {
                continuation_thread_id,
                ..
            } => continuation_thread_id.as_str(),
            Self::TurnStart | Self::VoiceSessionFinalizeAccepted { .. } => execution_thread_id,
        }
    }

    fn context_thread_id<'a>(&'a self, execution_thread_id: &'a str) -> &'a str {
        match self {
            Self::Task {
                context_thread_id, ..
            }
            | Self::DurableAgent {
                context_thread_id, ..
            } => context_thread_id.as_str(),
            Self::TurnStart | Self::VoiceSessionFinalizeAccepted { .. } => execution_thread_id,
        }
    }

    fn is_task(&self) -> bool {
        matches!(self, Self::Task { .. })
    }

    fn task_permission_profile(&self) -> Option<pioneer_protocol::TurnPermissionProfileSnapshot> {
        match self {
            Self::Task {
                permission_profile, ..
            }
            | Self::DurableAgent {
                permission_profile, ..
            } => Some(permission_profile.clone()),
            Self::TurnStart | Self::VoiceSessionFinalizeAccepted { .. } => None,
        }
    }

    fn task_agent_author(&self) -> Option<pioneer_protocol::TurnAuthorSnapshot> {
        match self {
            Self::Task { agent_author, .. } => agent_author.clone(),
            Self::DurableAgent { agent_author, .. } => Some(agent_author.clone()),
            Self::TurnStart | Self::VoiceSessionFinalizeAccepted { .. } => None,
        }
    }

    fn agent_turn_response(&self) -> Option<pioneer_crud::AgentTurnResponseInput> {
        match self {
            Self::Task {
                agent_turn_response,
                ..
            } => Some(agent_turn_response.clone()),
            Self::TurnStart
            | Self::VoiceSessionFinalizeAccepted { .. }
            | Self::DurableAgent { .. } => None,
        }
    }

    fn task_execution_security_snapshot(
        &self,
    ) -> Option<pioneer_protocol::TurnExecutionSecuritySnapshot> {
        match self {
            Self::Task {
                execution_security_snapshot,
                ..
            }
            | Self::DurableAgent {
                execution_security_snapshot,
                ..
            } => Some(execution_security_snapshot.clone()),
            Self::TurnStart | Self::VoiceSessionFinalizeAccepted { .. } => None,
        }
    }

    fn task_queue_identity(&self) -> Option<(&str, &str)> {
        match self {
            Self::Task {
                task_run_id,
                execution_id,
                ..
            } => Some((task_run_id.as_str(), execution_id.as_str())),
            Self::TurnStart
            | Self::VoiceSessionFinalizeAccepted { .. }
            | Self::DurableAgent { .. } => None,
        }
    }

    fn task_conversation_history(&self) -> Option<&[ChatMessage]> {
        match self {
            Self::Task {
                conversation_history,
                ..
            }
            | Self::DurableAgent {
                conversation_history,
                ..
            } => Some(conversation_history.as_slice()),
            Self::TurnStart | Self::VoiceSessionFinalizeAccepted { .. } => None,
        }
    }

    fn complete_task(&self, result: anyhow::Result<PreparedCliRuntimeNativeTurnStart>) -> bool {
        let completion = match self {
            Self::Task { completion, .. } | Self::DurableAgent { completion, .. } => completion,
            Self::TurnStart | Self::VoiceSessionFinalizeAccepted { .. } => return false,
        };
        let sender = completion.lock().ok().and_then(|mut sender| sender.take());
        if let Some(sender) = sender {
            return sender.send(result).is_ok();
        }
        false
    }
}

fn cli_runtime_forbidden_input_kind(input: &UserInput) -> Option<&'static str> {
    match input {
        UserInput::Text { .. } => None,
        UserInput::Image { .. }
        | UserInput::LocalImage { .. }
        | UserInput::File { .. }
        | UserInput::LocalFile { .. }
        | UserInput::Audio { .. }
        | UserInput::LocalAudio { .. }
        | UserInput::Video { .. }
        | UserInput::LocalVideo { .. }
        | UserInput::Artifact { .. } => None,
        UserInput::Mention { .. } => Some("mention"),
    }
}

fn cli_runtime_execution_disabled_message() -> String {
    "CLI agent runtime execution is disabled or no CLI runtimes are configured".to_owned()
}

fn execution_backend_allows_agent_skill_overlay(
    execution_backend: Option<&AgentExecutionBackend>,
) -> bool {
    matches!(
        execution_backend,
        None | Some(AgentExecutionBackend::ApiProvider { .. })
    )
}

impl MessageProcessor {
    #[allow(clippy::too_many_arguments)]
    async fn admit_composite_execution_request(
        &self,
        admission: &mut ExecutionAuthorizationAdmission,
        entry_point: crate::authorization::ExecutionAdmissionEntryPoint,
        additional_required_actions: Vec<crate::authorization::ResourceAction>,
        workspace_id: &str,
        target_thread_id: &str,
        provider: &str,
        model: &str,
        params: &TurnStartParams,
        capabilities: &[pioneer_protocol::TurnCapability],
    ) -> Result<(), TurnStartFailure> {
        if admission.workspace_id() != workspace_id
            || admission.target_thread_id() != target_thread_id
            || params.thread_id.trim() != target_thread_id
        {
            return Err(TurnStartFailure::policy_denied(
                "execution target differs from the authorized thread".to_owned(),
            ));
        }
        let provider_authority_fingerprint = match params.execution_backend.as_ref() {
            Some(AgentExecutionBackend::CLIAgentRuntime { .. })
            | Some(AgentExecutionBackend::ACPAgentRuntime { .. }) => None,
            _ => Some(
                self.provider_registry
                    .authority_fingerprint_for_workspace(workspace_id, provider)
                    .map_err(|error| {
                        TurnStartFailure::unavailable(format!(
                            "provider authority could not be resolved: {error:#}"
                        ))
                    })?
                    .as_str()
                    .to_owned(),
            ),
        };
        let request = crate::authorization::ExecutionAdmissionRequest::for_turn(
            entry_point,
            crate::authorization::ResourceAction::AgentTurnStart,
            additional_required_actions,
            workspace_id,
            admission.root_thread_id(),
            provider,
            model,
            params,
            capabilities,
            provider_authority_fingerprint,
        )
        .map_err(|error| {
            TurnStartFailure::invalid_input(format!("invalid composite execution intent: {error}"))
        })?;
        admission
            .authorize_composite(self.crud_store.as_ref(), &request)
            .await
            .map_err(|error| {
                TurnStartFailure::policy_denied(format!("execution authorization denied: {error}"))
            })
    }

    pub(super) async fn enforce_scoped_skill_capability_projection(
        &self,
        workspace_id: &str,
        capabilities: &[pioneer_protocol::TurnCapability],
    ) -> Result<(), TurnStartFailure> {
        let context = self.skills_runtime_context(workspace_id).map_err(|error| {
            TurnStartFailure::unavailable(format!(
                "Member skill projection is unavailable: {error:#}"
            ))
        })?;
        let catalog = self
            .load_skills_catalog(workspace_id, &context)
            .await
            .map_err(|error| {
                TurnStartFailure::unavailable(format!(
                    "Member skill projection is unavailable: {error:#}"
                ))
            })?;
        let workspace_policies = self
            .crud_store
            .list_workspace_skill_policies(workspace_id)
            .await
            .map_err(|error| {
                TurnStartFailure::unavailable(format!(
                    "Member skill projection is unavailable: {error:#}"
                ))
            })?;
        let policy_set = self.build_policy_set(&catalog.skills, &workspace_policies, &context);
        let installations = self
            .crud_store
            .list_skill_installations()
            .await
            .map_err(|error| {
                TurnStartFailure::unavailable(format!(
                    "Member skill projection is unavailable: {error:#}"
                ))
            })?
            .into_iter()
            .map(|installation| (installation.skill_id.clone(), installation))
            .collect::<HashMap<_, _>>();

        for capability in capabilities {
            let pioneer_protocol::TurnCapabilityKind::Skill {
                skill_id,
                pack_id: None,
            } = &capability.kind
            else {
                continue;
            };
            if capability.id != pioneer_protocol::skill_capability_key(skill_id) {
                return Err(TurnStartFailure::invalid_input(format!(
                    "skill capability `{}` does not match its server identity",
                    capability.id
                )));
            }
            let skill = catalog
                .skills
                .iter()
                .find(|skill| &skill.identity.skill_id == skill_id)
                .ok_or_else(|| {
                    TurnStartFailure::invalid_input(format!("skill `{skill_id}` is not installed"))
                })?;
            if !skill.is_available() {
                return Err(TurnStartFailure::unavailable(format!(
                    "skill `{skill_id}` is unavailable"
                )));
            }
            if !pioneer_skills::effective_policy_for_skill(skill, &policy_set).enabled {
                return Err(TurnStartFailure::policy_denied(format!(
                    "skill `{skill_id}` is not enabled for workspace `{workspace_id}`"
                )));
            }

            match skill.identity.source_kind {
                pioneer_skills::SkillSourceKind::System => {
                    if let Some(installation) = installations.get(skill_id)
                        && (installation.source_kind != "system"
                            || installation.fingerprint != skill.identity.fingerprint)
                    {
                        return Err(TurnStartFailure::invalid_input(format!(
                            "system skill `{skill_id}` does not match its installed identity"
                        )));
                    }
                }
                pioneer_skills::SkillSourceKind::User
                | pioneer_skills::SkillSourceKind::Registry => {
                    let installation = installations.get(skill_id).ok_or_else(|| {
                        TurnStartFailure::invalid_input(format!(
                            "skill `{skill_id}` is not installed"
                        ))
                    })?;
                    if installation.scope_key != workspace_id
                        || installation.source_kind != skill.identity.source_kind.as_db_value()
                        || installation.fingerprint != skill.identity.fingerprint
                        || installation.version != skill.identity.version_hint
                    {
                        return Err(TurnStartFailure::invalid_input(format!(
                            "skill `{skill_id}` does not match its active workspace installation"
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) async fn validate_turn_skill_capabilities(
        &self,
        workspace_id: &str,
        capabilities: &[pioneer_protocol::TurnCapability],
    ) -> Result<pioneer_skills::SkillCatalogSnapshot, TurnStartFailure> {
        let selected = capabilities
            .iter()
            .filter_map(|capability| match &capability.kind {
                pioneer_protocol::TurnCapabilityKind::Skill { skill_id, pack_id } => {
                    if pack_id.is_some() {
                        return None;
                    }
                    Some((capability, skill_id))
                }
                pioneer_protocol::TurnCapabilityKind::SkillPack { .. }
                | pioneer_protocol::TurnCapabilityKind::McpServer { .. }
                | pioneer_protocol::TurnCapabilityKind::McpTool { .. } => None,
            })
            .collect::<Vec<_>>();
        if capabilities.iter().any(|capability| {
            matches!(
                capability.kind,
                pioneer_protocol::TurnCapabilityKind::Skill {
                    pack_id: Some(_),
                    ..
                } | pioneer_protocol::TurnCapabilityKind::SkillPack { .. }
            )
        }) {
            return Err(TurnStartFailure::invalid_input(
                "skill pack metadata reached the ordinary skill validation boundary",
            ));
        }
        let context = self.skills_runtime_context(workspace_id).map_err(|error| {
            TurnStartFailure::unavailable(format!(
                "failed to resolve skills runtime context: {error:#}"
            ))
        })?;
        let catalog = self
            .load_skills_catalog(workspace_id, &context)
            .await
            .map_err(|error| {
                TurnStartFailure::unavailable(format!("failed to load skills catalog: {error:#}"))
            })?;

        for (capability, skill_id) in selected {
            let expected_capability_id = format!("skill:{skill_id}");
            if capability.id != expected_capability_id {
                return Err(TurnStartFailure::invalid_input(format!(
                    "skill capability `{}` must use exact ID `{expected_capability_id}`",
                    capability.id
                )));
            }
            let Some(skill) = catalog
                .skills
                .iter()
                .find(|skill| &skill.identity.skill_id == skill_id)
            else {
                return Err(TurnStartFailure::invalid_input(format!(
                    "skill `{skill_id}` was not found"
                )));
            };
            if !skill.is_available() {
                return Err(TurnStartFailure::unavailable(format!(
                    "skill `{skill_id}` is unavailable"
                )));
            }
        }
        Ok(catalog)
    }

    pub(super) async fn normalize_turn_skill_capabilities(
        &self,
        workspace_id: &str,
        capabilities: &[pioneer_protocol::TurnCapability],
    ) -> Result<NormalizedTurnCapabilities, TurnStartFailure> {
        use pioneer_protocol::{TurnCapability, TurnCapabilityKind};

        let _skills_guard = self.acquire_skills_write_lock().await;
        let mut presentation = Vec::with_capacity(capabilities.len());
        let mut full_pack_ids = HashSet::new();
        let mut pack_children = HashMap::new();
        let mut pack_names = HashMap::new();

        for capability in capabilities {
            match &capability.kind {
                TurnCapabilityKind::Skill { skill_id, pack_id } => {
                    let installation = self
                        .crud_store
                        .find_skill_installation(skill_id)
                        .await
                        .map_err(|error| {
                            TurnStartFailure::unavailable(format!(
                                "failed to load skill `{skill_id}` installation: {error:#}"
                            ))
                        })?;
                    let Some(installation) = installation else {
                        if let Some(requested_pack_id) = pack_id {
                            return Err(TurnStartFailure::invalid_input(format!(
                                "skill `{skill_id}` is not a member of pack `{requested_pack_id}`"
                            )));
                        }
                        presentation.push(TurnCapability {
                            id: capability.id.clone(),
                            label: capability.label.clone(),
                            kind: TurnCapabilityKind::Skill {
                                skill_id: skill_id.clone(),
                                pack_id: None,
                            },
                        });
                        continue;
                    };
                    if installation.source_kind != "system"
                        && installation.scope_key != workspace_id
                    {
                        return Err(TurnStartFailure::invalid_input(format!(
                            "skill `{skill_id}` is not installed in workspace `{workspace_id}`"
                        )));
                    }

                    let authoritative_pack_id = installation.pack_id.clone();
                    if let Some(requested_pack_id) = pack_id {
                        if authoritative_pack_id.as_ref() != Some(requested_pack_id) {
                            return Err(TurnStartFailure::invalid_input(format!(
                                "skill `{skill_id}` is not a member of pack `{requested_pack_id}`"
                            )));
                        }
                    }
                    if let Some(authoritative_pack_id) = authoritative_pack_id.as_ref() {
                        if installation.scope_key != workspace_id {
                            return Err(TurnStartFailure::invalid_input(format!(
                                "skill `{skill_id}` pack membership is outside workspace `{workspace_id}`"
                            )));
                        }
                        let parent = self
                            .crud_store
                            .find_skill_pack_installation(workspace_id, authoritative_pack_id)
                            .await
                            .map_err(|error| {
                                TurnStartFailure::unavailable(format!(
                                    "failed to load skill pack `{authoritative_pack_id}`: {error:#}"
                                ))
                            })?
                            .ok_or_else(|| {
                                TurnStartFailure::invalid_input(format!(
                                    "skill `{skill_id}` references missing pack `{authoritative_pack_id}`"
                                ))
                            })?;
                        pack_names.insert(authoritative_pack_id.clone(), parent.name);
                    }

                    presentation.push(TurnCapability {
                        id: capability.id.clone(),
                        label: capability.label.clone(),
                        kind: TurnCapabilityKind::Skill {
                            skill_id: skill_id.clone(),
                            pack_id: authoritative_pack_id,
                        },
                    });
                }
                TurnCapabilityKind::SkillPack { pack_id } => {
                    if !full_pack_ids.insert(pack_id.clone()) {
                        return Err(TurnStartFailure::invalid_input(format!(
                            "skill pack `{pack_id}` is selected more than once"
                        )));
                    }
                    let parent = self
                        .crud_store
                        .find_skill_pack_installation(workspace_id, pack_id)
                        .await
                        .map_err(|error| {
                            TurnStartFailure::unavailable(format!(
                                "failed to load skill pack `{pack_id}`: {error:#}"
                            ))
                        })?
                        .ok_or_else(|| {
                            TurnStartFailure::invalid_input(format!(
                                "skill pack `{pack_id}` was not found in workspace `{workspace_id}`"
                            ))
                        })?;
                    pack_names.insert(pack_id.clone(), parent.name);
                    let children = self
                        .crud_store
                        .list_skill_installations_for_pack(workspace_id, pack_id)
                        .await
                        .map_err(|error| {
                            TurnStartFailure::unavailable(format!(
                                "failed to load children for skill pack `{pack_id}`: {error:#}"
                            ))
                        })?;
                    if children.is_empty() {
                        return Err(TurnStartFailure::invalid_input(format!(
                            "skill pack `{pack_id}` is empty"
                        )));
                    }
                    if children.iter().any(|child| {
                        child.scope_key != workspace_id
                            || child.pack_id.as_ref() != Some(pack_id)
                            || child.pack_member_key.as_deref().is_none_or(str::is_empty)
                    }) {
                        return Err(TurnStartFailure::invalid_input(format!(
                            "skill pack `{pack_id}` has invalid authoritative membership"
                        )));
                    }
                    pack_children.insert(pack_id.clone(), children);
                    presentation.push(capability.clone());
                }
                TurnCapabilityKind::McpServer { .. } | TurnCapabilityKind::McpTool { .. } => {
                    presentation.push(capability.clone());
                }
            }
        }

        let mut execution = Vec::new();
        let mut seen_skill_ids = HashSet::new();
        for capability in &presentation {
            match &capability.kind {
                TurnCapabilityKind::Skill { skill_id, pack_id } => {
                    if pack_id
                        .as_ref()
                        .is_some_and(|pack_id| full_pack_ids.contains(pack_id))
                    {
                        return Err(TurnStartFailure::invalid_input(format!(
                            "skill pack and child `{skill_id}` cannot be selected together"
                        )));
                    }
                    if !seen_skill_ids.insert(skill_id.clone()) {
                        return Err(TurnStartFailure::invalid_input(format!(
                            "skill `{skill_id}` is selected more than once"
                        )));
                    }
                    execution.push(TurnCapability {
                        id: pioneer_protocol::skill_capability_key(skill_id),
                        label: if pack_id.is_some() {
                            None
                        } else {
                            capability.label.clone()
                        },
                        kind: TurnCapabilityKind::Skill {
                            skill_id: skill_id.clone(),
                            pack_id: None,
                        },
                    });
                }
                TurnCapabilityKind::SkillPack { pack_id } => {
                    let children = pack_children
                        .get(pack_id)
                        .expect("validated skill pack children");
                    for child in children {
                        if !seen_skill_ids.insert(child.skill_id.clone()) {
                            return Err(TurnStartFailure::invalid_input(format!(
                                "skill `{}` is duplicated after pack expansion",
                                child.skill_id
                            )));
                        }
                        execution.push(TurnCapability {
                            id: pioneer_protocol::skill_capability_key(&child.skill_id),
                            label: None,
                            kind: TurnCapabilityKind::Skill {
                                skill_id: child.skill_id.clone(),
                                pack_id: None,
                            },
                        });
                    }
                }
                TurnCapabilityKind::McpServer { .. } | TurnCapabilityKind::McpTool { .. } => {
                    execution.push(capability.clone());
                }
            }
        }

        Ok(NormalizedTurnCapabilities {
            presentation,
            execution,
            pack_names,
        })
    }

    pub(super) fn turn_start<'a>(
        &'a self,
        request_context: &'a RequestContext,
        execution_admission: ExecutionAuthorizationAdmission,
        request_id: RequestId,
        params: TurnStartParams,
    ) -> MessageFuture<'a, ()> {
        let connection_id = request_context.connection_id();
        let request_actor = request_context.persisted_actor();
        let scoped_principal_id = execution_admission
            .uses_scoped_collaboration_policy()
            .then(|| request_context.principal().principal_id.clone());
        message_future(async move {
            if execution_admission.target_thread_id() != params.thread_id.trim() {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::NotFound.response(request_id),
                )
                .await;
                return;
            }
            if params.thread_id.trim().is_empty() {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        pioneer_protocol::PublicErrorStage::Admission,
                        format!(
                            "invalid params for `{}`: `thread_id` is required",
                            methods::TURN_START
                        ),
                    ),
                )
                .await;
                return;
            }

            if params.turn_id.trim().is_empty() {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        pioneer_protocol::PublicErrorStage::Admission,
                        format!(
                            "invalid params for `{}`: `turn_id` is required",
                            methods::TURN_START
                        ),
                    ),
                )
                .await;
                return;
            }
            if let Err(failure) = validate_root_agent_launch_matches_turn(&params) {
                self.send_error(connection_id, public_turn_start_error(request_id, failure))
                    .await;
                return;
            }
            let thread = match self
                .thread_manager
                .thread_get(params.thread_id.trim())
                .await
            {
                Some(thread) => thread,
                None => {
                    self.send_error(
                        connection_id,
                        public_turn_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            pioneer_protocol::PublicErrorStage::Admission,
                            format!("thread `{}` is not loaded", params.thread_id.trim()),
                        ),
                    )
                    .await;
                    return;
                }
            };
            if let Err(error) = execution_admission.validate_provider_request(
                thread.model_provider.as_str(),
                thread.model.as_str(),
                params.model_provider.as_deref(),
                params.model.as_deref(),
                params.execution_backend.as_ref(),
            ) {
                self.send_error(
                    connection_id,
                    public_turn_start_error(
                        request_id,
                        TurnStartFailure::policy_denied(error.to_string()),
                    ),
                )
                .await;
                return;
            }
            if thread.origin_kind.composer_execution_mode()
                == pioneer_protocol::ThreadComposerExecutionMode::DetachedTask
            {
                self.composer_detached_task_start(
                    connection_id,
                    request_id,
                    request_actor.clone(),
                    params,
                    thread,
                    execution_admission,
                    TurnStartSuccessResponse::TurnStart,
                )
                .await;
                return;
            }
            let requested_reasoning_effort = requested_reasoning_effort(&params);
            if let Some(effort) = requested_reasoning_effort.as_deref() {
                debug!(
                    effort,
                    turn_id = params.turn_id.as_str(),
                    thread_id = params.thread_id.as_str(),
                    "turn/start requested reasoning effort"
                );
            }
            if let Some(backend) = params.execution_backend.clone() {
                match backend {
                    AgentExecutionBackend::CLIAgentRuntime {
                        runtime_id,
                        runtime_kind,
                    } => {
                        self.turn_start_cli_runtime(
                            connection_id,
                            request_id,
                            request_actor,
                            params,
                            runtime_id,
                            runtime_kind,
                            TurnExecutionAuthority::Fresh(execution_admission),
                            TurnStartSuccessResponse::TurnStart,
                        )
                        .await;
                        return;
                    }
                    AgentExecutionBackend::ACPAgentRuntime { runtime_id } => {
                        self.send_error(
                            connection_id,
                            public_turn_error(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                pioneer_protocol::PublicErrorStage::Admission,
                                format!("ACP agent runtime `{runtime_id}` is not supported"),
                            ),
                        )
                        .await;
                        return;
                    }
                    AgentExecutionBackend::ApiProvider { .. } => {}
                }
            }

            let admission = match self
                .prepare_api_provider_turn_start(
                    connection_id,
                    request_actor,
                    scoped_principal_id,
                    params,
                    requested_reasoning_effort.as_deref(),
                    crate::authorization::ExecutionAdmissionEntryPoint::InteractiveTurn,
                    execution_admission,
                )
                .await
            {
                Ok(prepared) => prepared,
                Err(message) => {
                    self.send_error(connection_id, public_turn_start_error(request_id, message))
                        .await;
                    return;
                }
            };
            let prepared = match admission {
                ApiProviderTurnAdmission::New(prepared) => prepared,
                ApiProviderTurnAdmission::Replay(response) => {
                    self.session_manager
                        .set_connection_workspace(connection_id, Some(thread.workspace_id.clone()))
                        .await;
                    match JsonRpcResponse::from_result(request_id, &response) {
                        Ok(response) => {
                            if let Err(error) = self.send_json(connection_id, &response).await {
                                warn!(
                                    connection_id,
                                    error = %format!("{error:#}"),
                                    "failed to send idempotent turn/start replay response"
                                );
                            }
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                public_turn_error(
                                    None,
                                    INVALID_REQUEST_CODE,
                                    pioneer_protocol::PublicErrorStage::Delivery,
                                    format!(
                                        "failed to encode idempotent turn/start response: {error}"
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                    return;
                }
            };
            if !message_future(self.finish_turn_start_success(
                connection_id,
                request_id,
                &prepared.outcome,
                prepared.user_message_capability_attachments.as_slice(),
            ))
            .await
            {
                self.mark_turn_blocked(
                    prepared.outcome.started_notification.thread_id.clone(),
                    prepared.outcome.started_notification.turn.id.clone(),
                    "failed to commit native turn start lifecycle".to_owned(),
                )
                .await;
                return;
            }
            self.dispatch_prepared_api_provider_turn_start(prepared)
                .await;
        })
    }

    async fn complete_runtime_draft_materialization(
        &self,
        execution_authority: ExecutionEnvelopeSource<'_>,
    ) {
        let Some(runtime_draft) = execution_authority.runtime_draft() else {
            return;
        };
        self.complete_runtime_draft_materialization_record(runtime_draft)
            .await;
    }

    pub(super) async fn complete_runtime_draft_materialization_record(
        &self,
        runtime_draft: &RuntimeDraftMaterialization,
    ) {
        let access = runtime_draft.access();
        if !self.thread_manager.mark_runtime_draft_durable(access).await {
            warn!(
                workspace_id = access.workspace_id(),
                thread_id = access.thread_id(),
                "first turn committed but runtime draft lifecycle could not be promoted"
            );
        }
        if let RuntimeDraftCreator::ScopedPrincipal { principal_id, .. } = runtime_draft.creator() {
            self.publish_committed_authorization_invalidation(
                AccessChangeKind::ThreadCreated,
                Some(principal_id.clone()),
                access.workspace_id().to_owned(),
                Some(access.thread_id().to_owned()),
            )
            .await;
        }
    }

    /// Admit a collaborative Composer submission as a durable user message
    /// followed by an immediate detached task. The message turn completes
    /// synchronously; the task child owns all agent execution.
    pub(super) fn composer_detached_task_start<'a>(
        &'a self,
        connection_id: ConnectionId,
        request_id: RequestId,
        request_actor: pioneer_protocol::PersistedActorRef,
        mut params: TurnStartParams,
        thread: pioneer_protocol::Thread,
        mut execution_admission: ExecutionAuthorizationAdmission,
        success_response: TurnStartSuccessResponse,
    ) -> MessageFuture<'a, ()> {
        message_future(async move {
            let admission_entry_point =
                crate::authorization::ExecutionAdmissionEntryPoint::DetachedTask;
            let turn_id = params.turn_id.clone();
            // Keep Composer admission, durable materialization, detached Task
            // creation, and finalization in separate heap-backed futures. Polling
            // the complete lifecycle as one state machine stacks its large frame
            // on top of the database projector and can exhaust a standard Tokio
            // worker stack.
            let started_phase = message_future(async {
                let normalized_capabilities = match self
                    .normalize_turn_skill_capabilities(
                        thread.workspace_id.as_str(),
                        params.capabilities.as_slice(),
                    )
                    .await
                {
                    Ok(normalized) => normalized,
                    Err(message) => {
                        self.send_turn_start_failure(
                            connection_id,
                            request_id.clone(),
                            &success_response,
                            thread.id.as_str(),
                            turn_id.as_str(),
                            message,
                        )
                        .await;
                        return None;
                    }
                };
                if execution_admission.uses_scoped_collaboration_policy()
                    && let Err(message) = self
                        .enforce_scoped_skill_capability_projection(
                            thread.workspace_id.as_str(),
                            normalized_capabilities.execution.as_slice(),
                        )
                        .await
                {
                    self.send_turn_start_failure(
                        connection_id,
                        request_id.clone(),
                        &success_response,
                        thread.id.as_str(),
                        turn_id.as_str(),
                        message,
                    )
                    .await;
                    return None;
                }
                if let Err(error) =
                    super::message_turn::normalize_turn_collaboration_params(&mut params)
                {
                    self.send_turn_start_failure(
                        connection_id,
                        request_id.clone(),
                        &success_response,
                        thread.id.as_str(),
                        turn_id.as_str(),
                        format!("invalid Turn collaboration metadata: {error}"),
                    )
                    .await;
                    return None;
                }
                let resolved_permission_profile = execution_admission
                    .uses_scoped_collaboration_policy()
                    .then(|| {
                        let requested = pioneer_protocol::resolve_turn_permission_profile(
                            params.permission_profile.as_ref(),
                        );
                        execution_admission.cap_permission_profile(&requested)
                    });
                if let Some(profile) = resolved_permission_profile.as_ref() {
                    params.permission_profile =
                        Some(pioneer_protocol::TurnPermissionProfileSelection {
                            mode: profile.mode,
                        });
                }
                // The detached Task must replay the exact presentation selected in the
                // Composer. Skill packs are expanded only at the execution boundary;
                // persisting the expanded capabilities here would make the child
                // message render every pack member as an individually selected skill.
                let launch = params.clone();
                params.capabilities = normalized_capabilities.execution.clone();
                if let Err(failure) = validate_root_agent_launch_capabilities(&params) {
                    self.send_turn_start_failure(
                        connection_id,
                        request_id.clone(),
                        &success_response,
                        thread.id.as_str(),
                        turn_id.as_str(),
                        failure,
                    )
                    .await;
                    return None;
                }
                if let Err(error) = self
                    .validate_turn_artifact_user_inputs(
                        thread.workspace_id.as_str(),
                        thread.id.as_str(),
                        params.input.as_slice(),
                    )
                    .await
                {
                    self.send_turn_start_failure(
                        connection_id,
                        request_id.clone(),
                        &success_response,
                        thread.id.as_str(),
                        turn_id.as_str(),
                        format!("failed to validate artifact input: {error:#}"),
                    )
                    .await;
                    return None;
                }
                let skill_catalog = match self
                    .validate_turn_skill_capabilities(
                        thread.workspace_id.as_str(),
                        params.capabilities.as_slice(),
                    )
                    .await
                {
                    Ok(catalog) => catalog,
                    Err(message) => {
                        self.send_turn_start_failure(
                            connection_id,
                            request_id.clone(),
                            &success_response,
                            thread.id.as_str(),
                            turn_id.as_str(),
                            message,
                        )
                        .await;
                        return None;
                    }
                };
                let capability_attachments =
            match super::agent_runtime::user_message_attachments_from_capabilities_and_catalog(
                normalized_capabilities.presentation.as_slice(),
                &skill_catalog,
                &normalized_capabilities.pack_names,
            ) {
                Ok(attachments) => attachments,
                Err(error) => {
                    self.send_turn_start_failure(
                        connection_id,
                        request_id.clone(),
                        &success_response,
                        thread.id.as_str(),
                        turn_id.as_str(),
                        format!("failed to snapshot selected capability presentation: {error:#}"),
                    )
                    .await;
                    return None;
                }
            };

                // Preserve the exact client launch for Task replay, but admit the
                // parent message with the canonical provider selected by its
                // execution backend. CLI clients intentionally omit `model_provider`,
                // so leaving the field empty here would keep the parent's previous
                // API provider even though the detached child runs in Codex/Claude.
                canonicalize_cli_runtime_model_provider(&mut params);
                let author = match super::message_turn::resolve_turn_author_snapshot(
                    self.crud_store.as_ref(),
                    &request_actor,
                )
                .await
                {
                    Ok(author) => author,
                    Err(error) => {
                        self.send_turn_start_failure(
                            connection_id,
                            request_id.clone(),
                            &success_response,
                            thread.id.as_str(),
                            turn_id.as_str(),
                            format!("failed to resolve Turn author: {error:#}"),
                        )
                        .await;
                        return None;
                    }
                };
                let mentions = match super::message_turn::resolve_turn_collaboration_metadata(
                    self.crud_store.as_ref(),
                    &request_actor,
                    &params,
                )
                .await
                {
                    Ok(mentions) => mentions,
                    Err(error) => {
                        self.send_turn_start_failure(
                            connection_id,
                            request_id.clone(),
                            &success_response,
                            thread.id.as_str(),
                            turn_id.as_str(),
                            format!("invalid Turn collaboration metadata: {error}"),
                        )
                        .await;
                        return None;
                    }
                };
                let outcome_result = match resolved_permission_profile {
                    Some(profile) => {
                        self.thread_manager
                            .turn_start_with_user_metadata_and_permission_profile(
                                connection_id,
                                params,
                                profile,
                                author,
                                mentions,
                            )
                            .await
                    }
                    None => {
                        self.thread_manager
                            .turn_start_with_user_metadata(connection_id, params, author, mentions)
                            .await
                    }
                };
                let outcome = match outcome_result {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        self.send_turn_start_failure(
                            connection_id,
                            request_id.clone(),
                            &success_response,
                            thread.id.as_str(),
                            turn_id.as_str(),
                            format!("failed to admit Composer message: {error:#}"),
                        )
                        .await;
                        return None;
                    }
                };
                let profile_audit = match self.turn_profile_selected_audit_event(&outcome) {
                    Ok(event) => event,
                    Err(error) => {
                        self.thread_manager
                            .rollback_turn_start(outcome.rollback_context.clone())
                            .await;
                        self.send_turn_start_failure(
                            connection_id,
                            request_id.clone(),
                            &success_response,
                            thread.id.as_str(),
                            turn_id.as_str(),
                            format!("failed to resolve Composer permission profile: {error:#}"),
                        )
                        .await;
                        return None;
                    }
                };
                Some(ComposerDetachedStartedPhase {
                    launch,
                    outcome,
                    capability_attachments,
                    profile_audit,
                })
            })
            .await;
            let Some(ComposerDetachedStartedPhase {
                launch,
                outcome,
                capability_attachments,
                profile_audit,
            }) = started_phase
            else {
                return;
            };
            let materialized_phase = message_future(async {
                if let Err(message) = self
                    .admit_composite_execution_request(
                        &mut execution_admission,
                        admission_entry_point,
                        vec![crate::authorization::ResourceAction::TaskCreate],
                        outcome.started_notification.workspace_id.as_str(),
                        outcome.started_notification.thread_id.as_str(),
                        outcome.materialization.thread.model_provider.as_str(),
                        outcome.materialization.thread.model.as_str(),
                        &launch,
                        outcome.materialization.capabilities.as_slice(),
                    )
                    .await
                {
                    self.thread_manager
                        .rollback_turn_start(outcome.rollback_context.clone())
                        .await;
                    self.send_turn_start_failure(
                        connection_id,
                        request_id.clone(),
                        &success_response,
                        thread.id.as_str(),
                        turn_id.as_str(),
                        message,
                    )
                    .await;
                    return None;
                }
                let security_snapshot = match self
                    .resolve_turn_execution_security_snapshot(
                        &launch,
                        &outcome,
                        None,
                        ExecutionEnvelopeSource::Fresh(&execution_admission),
                    )
                    .await
                {
                    Ok(snapshot) => snapshot,
                    Err(failure) => {
                        self.thread_manager
                            .rollback_turn_start(outcome.rollback_context.clone())
                            .await;
                        self.send_turn_start_failure(
                            connection_id,
                            request_id.clone(),
                            &success_response,
                            thread.id.as_str(),
                            turn_id.as_str(),
                            failure,
                        )
                        .await;
                        return None;
                    }
                };
                let security_audit_events = self.turn_security_audit_events_for_turn(
                    outcome.started_notification.workspace_id.as_str(),
                    outcome.started_notification.thread_id.as_str(),
                    outcome.started_notification.turn.id.as_str(),
                    &security_snapshot,
                );
                if let Err(error) = persist_admitted_turn_start(
                    self.crud_store.as_ref(),
                    self.provider_registry.as_ref(),
                    self.turn_execution_owner_id.as_ref(),
                    &launch,
                    &outcome.materialization,
                    requested_reasoning_effort(&launch).as_deref(),
                    request_actor.clone(),
                    profile_audit,
                    ExecutionEnvelopeSource::Fresh(&execution_admission),
                    None,
                    &security_snapshot,
                    security_audit_events,
                    None,
                    None,
                )
                .await
                {
                    self.thread_manager
                        .rollback_turn_start(outcome.rollback_context.clone())
                        .await;
                    self.send_turn_start_failure(
                        connection_id,
                        request_id.clone(),
                        &success_response,
                        thread.id.as_str(),
                        turn_id.as_str(),
                        format!("failed to persist Composer message: {error:#}"),
                    )
                    .await;
                    return None;
                }
                self.complete_runtime_draft_materialization(ExecutionEnvelopeSource::Fresh(
                    &execution_admission,
                ))
                .await;
                if let Err(error) = self
                    .register_execution_lease(outcome.started_notification.turn.id.as_str())
                    .await
                {
                    let message = format!("failed to register execution lease: {error:#}");
                    self.mark_turn_blocked(
                        thread.id.clone(),
                        launch.turn_id.clone(),
                        message.clone(),
                    )
                    .await;
                    self.send_turn_start_failure(
                        connection_id,
                        request_id.clone(),
                        &success_response,
                        thread.id.as_str(),
                        turn_id.as_str(),
                        TurnStartFailure::internal(message),
                    )
                    .await;
                    return None;
                }

                Some(ComposerDetachedMaterializedPhase {
                    launch,
                    outcome,
                    capability_attachments,
                    security_snapshot,
                })
            })
            .await;
            let Some(ComposerDetachedMaterializedPhase {
                launch,
                outcome,
                capability_attachments,
                security_snapshot,
            }) = materialized_phase
            else {
                return;
            };
            let task_created_phase = message_future(async {
                let first_text = first_user_text(launch.input.as_slice());
                let goal = first_text
                    .as_deref()
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .unwrap_or("Composer task")
                    .to_owned();
                let title = goal.chars().take(96).collect::<String>();
                let permission_profile = outcome.materialization.turn.permission_profile.clone();
                let task_model_provider = outcome.materialization.thread.model_provider.clone();
                // Freeze the causally closed parent branch before the Task becomes
                // visible to the scheduler. A later sibling message must not change
                // the context of this run, even if execution starts later.
                let frozen_history = self
                    .load_conversation_history_for_workspace_in_execution_excluding_turn(
                        thread.workspace_id.as_str(),
                        thread.id.as_str(),
                        thread.id.as_str(),
                        launch.turn_id.as_str(),
                        Some(launch.turn_id.as_str()),
                        Some(outcome.materialization.thread.model.as_str()),
                        Some(task_model_provider.as_str()),
                    )
                    .await;
                let frozen_history_json = match serde_json::to_string(&frozen_history) {
                    Ok(history_json) => history_json,
                    Err(error) => {
                        self.mark_turn_blocked(
                            thread.id.clone(),
                            launch.turn_id.clone(),
                            format!("failed to freeze Composer task history: {error:#}"),
                        )
                        .await;
                        self.send_turn_start_failure(
                            connection_id,
                            request_id.clone(),
                            &success_response,
                            thread.id.as_str(),
                            turn_id.as_str(),
                            format!("failed to freeze Composer task history: {error:#}"),
                        )
                        .await;
                        return None;
                    }
                };
                let mut task_params = pioneer_protocol::TaskCreateParams {
                    workspace_id: thread.workspace_id.clone(),
                    owner_kind: pioneer_protocol::TaskOwnerKind::Thread,
                    owner_id: Some(thread.id.clone()),
                    created_by_thread_id: Some(thread.id.clone()),
                    created_by_turn_id: Some(launch.turn_id.clone()),
                    parent_task_id: None,
                    executor_kind: pioneer_protocol::TaskExecutorKind::Agent,
                    title,
                    goal: goal.clone(),
                    priority: 0,
                    trigger: pioneer_protocol::TaskTriggerInput {
                        spec: pioneer_protocol::TaskTriggerSpec::Immediate,
                    },
                    launch: None,
                    agent_spec: Some(pioneer_protocol::TaskAgentSpecInput {
                        agent_role: thread.agent_role.clone(),
                        agent_nickname: thread.agent_nickname.clone(),
                        model: Some(outcome.materialization.thread.model.clone()),
                        model_provider: Some(task_model_provider.clone()),
                        prompt: pioneer_protocol::TaskAgentPrompt {
                            goal,
                            instructions: Vec::new(),
                            input: None,
                            output_instructions: None,
                        },
                        context_policy: None,
                        tool_policy: None,
                        permission_cap: Some(pioneer_protocol::task_permission_cap_from_snapshot(
                            &permission_profile,
                        )),
                        security_cap: Some(crate::turn_security::task_security_cap_from_snapshot(
                            &security_snapshot,
                        )),
                        result_contract: None,
                        review_policy: None,
                        depth: 0,
                        max_depth: 3,
                    }),
                    lifecycle_policy: Some(pioneer_protocol::TaskLifecyclePolicy {
                        attachment: pioneer_protocol::TaskAttachmentMode::Detached,
                        on_parent_cancel: pioneer_protocol::TaskParentTerminalAction::KeepRunning,
                        on_parent_failure: pioneer_protocol::TaskParentTerminalAction::KeepRunning,
                        completion: pioneer_protocol::TaskCompletionBehavior::CompleteOnTerminalRun,
                    }),
                    delivery_policy: Some(pioneer_protocol::TaskDeliveryPolicy {
                        mode: pioneer_protocol::TaskDeliveryMode::Thread,
                        thread_target: Some(
                            pioneer_protocol::TaskDeliveryThreadTarget::OriginThread,
                        ),
                        thread_id: Some(thread.id.clone()),
                        webhook_url: None,
                        include_result: true,
                        format: pioneer_protocol::TaskDeliveryFormat::FullResult,
                    }),
                    retry_policy: None,
                    timeout_policy: None,
                    concurrency_policy: None,
                    metadata: Some(pioneer_protocol::TaskMetadata {
                        labels: vec!["composer".to_owned()],
                        data: None,
                        composer_work: Some(pioneer_protocol::TaskComposerWork::v1(launch.clone())),
                    }),
                };
                let task_execution_context = match self
                    .load_turn_execution_authorization_context(launch.turn_id.as_str())
                    .await
                {
                    Ok(context) => context,
                    Err(error) => {
                        self.mark_turn_blocked(
                            thread.id.clone(),
                            launch.turn_id.clone(),
                            format!("failed to load Composer execution admission: {error:#}"),
                        )
                        .await;
                        return None;
                    }
                };
                let authorization_context_json = match task_execution_context.to_persisted_json() {
                    Ok(json) => json,
                    Err(error) => {
                        self.mark_turn_blocked(
                            thread.id.clone(),
                            launch.turn_id.clone(),
                            format!("failed to persist Composer execution admission: {error:#}"),
                        )
                        .await;
                        return None;
                    }
                };
                let (execution_resources, task_resources) =
                    match task_execution_context.admitted_resource_budgets() {
                        Ok(resources) => resources,
                        Err(error) => {
                            self.mark_turn_blocked(
                                thread.id.clone(),
                                launch.turn_id.clone(),
                                format!(
                                    "Composer execution admission has no registered resource budgets: {error:#}"
                                ),
                            )
                            .await;
                            return None;
                        }
                    };
                let (canonical_launch, resolved_launch) =
                    match super::agent_action_tools::resolve_workspace_task_launch(
                        self,
                        thread.workspace_id.as_str(),
                        task_model_provider.as_str(),
                        outcome.materialization.thread.model.as_str(),
                        launch.agent_launch.as_ref(),
                        launch.execution_backend.as_ref(),
                        launch.turn_id.as_str(),
                    )
                    .await
                    {
                        Ok(resolved) => resolved,
                        Err(error) => {
                            self.mark_turn_blocked(
                                thread.id.clone(),
                                launch.turn_id.clone(),
                                format!("failed to resolve Composer Task launch: {error:#}"),
                            )
                            .await;
                            return None;
                        }
                    };
                task_params.launch = Some(canonical_launch.clone());
                let agent_authorization_grant = match resolved_launch.as_ref() {
                    Some((identity, profile)) => {
                        let child_skill_ids = match task_execution_context
                            .granted_skill_ids()
                            .iter()
                            .map(|id| pioneer_protocol::SkillId::new(id.clone()))
                            .collect::<Result<Vec<_>, _>>()
                        {
                            Ok(ids) => ids,
                            Err(error) => {
                                self.mark_turn_blocked(
                                    thread.id.clone(),
                                    launch.turn_id.clone(),
                                    format!(
                                        "Composer child Skill grant is invalid: {error:?}"
                                    ),
                                )
                                .await;
                                return None;
                            }
                        };
                        let child_launch_grant = match super::agent_action_tools::current_workspace_child_launch_ceiling(
                            self,
                            thread.workspace_id.as_str(),
                            identity,
                            profile,
                            task_model_provider.as_str(),
                            outcome.materialization.thread.model.as_str(),
                            true,
                            child_skill_ids,
                            task_execution_context
                                .granted_mcp_server_capability_ids()
                                .to_vec(),
                            task_execution_context.permission_profile_cap().clone(),
                        )
                        .await
                        {
                            Ok(grant) => grant,
                            Err(error) => {
                                self.mark_turn_blocked(
                                    thread.id.clone(),
                                    launch.turn_id.clone(),
                                    format!(
                                        "failed to freeze Composer child launch ceiling: {error:#}"
                                    ),
                                )
                                .await;
                                return None;
                            }
                        };
                        match crate::authorization::derive_task_agent_authorization_grant_seed(
                            identity.id.clone(),
                            task_execution_context.root_thread_id(),
                            "thread_agent",
                            task_execution_context.policy_revision(),
                            child_launch_grant,
                        ) {
                            Ok(grant) => Some(grant),
                            Err(error) => {
                                self.mark_turn_blocked(
                                    thread.id.clone(),
                                    launch.turn_id.clone(),
                                    format!(
                                        "failed to freeze Composer Agent authorization: {error:?}"
                                    ),
                                )
                                .await;
                                return None;
                            }
                        }
                    }
                    None => None,
                };
                let create_context = pioneer_tasks::TaskCreateContext {
                    actor_id: task_creator_actor_id(&request_actor),
                    launch_selection: Some(canonical_launch),
                    resolved_launch_identity: resolved_launch
                        .as_ref()
                        .map(|(identity, _)| identity.clone()),
                    resolved_launch_profile: resolved_launch.map(|(_, profile)| profile),
                    agent_authorization_grant,
                    conversation_snapshot: Some(pioneer_tasks::TaskRunConversationSnapshotSeed {
                        conversation_thread_id: thread.id.clone(),
                        source_turn_id: Some(launch.turn_id.clone()),
                        history_json: frozen_history_json,
                    }),
                    execution_admission: Some(pioneer_tasks::TaskExecutionAdmissionSeed {
                        workspace_id: task_execution_context.workspace_id().to_owned(),
                        root_thread_id: task_execution_context.root_thread_id().to_owned(),
                        initiating_principal_id: task_execution_context
                            .initiating_principal_id()
                            .to_string(),
                        authorization_context_json,
                        role_key: task_execution_context.role_key().to_owned(),
                        policy_fingerprint: task_execution_context.policy_fingerprint().to_owned(),
                        execution_resources,
                        task_resources,
                    }),
                    ..Default::default()
                };
                if let Some(seed) = create_context.execution_admission.as_ref()
                    && let Err(error) = self.validate_task_execution_admission_seed(seed).await
                {
                    self.mark_turn_blocked(
                        thread.id.clone(),
                        launch.turn_id.clone(),
                        format!("Composer execution admission became stale: {error:#}"),
                    )
                    .await;
                    return None;
                }
                if let Err(error) = self
                    .task_runtime
                    .service()
                    .create_task(create_context, task_params)
                    .await
                {
                    self.mark_turn_blocked(
                        thread.id.clone(),
                        launch.turn_id.clone(),
                        format!("failed to create detached Composer task: {error:#}"),
                    )
                    .await;
                    self.send_turn_start_failure(
                        connection_id,
                        request_id.clone(),
                        &success_response,
                        thread.id.as_str(),
                        turn_id.as_str(),
                        format!("failed to create detached Composer task: {error:#}"),
                    )
                    .await;
                    return None;
                }

                Some(ComposerDetachedTaskCreatedPhase {
                    launch,
                    outcome,
                    capability_attachments,
                })
            })
            .await;
            let Some(ComposerDetachedTaskCreatedPhase {
                launch,
                outcome,
                capability_attachments,
            }) = task_created_phase
            else {
                return;
            };

            let success_sent = match &success_response {
                TurnStartSuccessResponse::TurnStart => {
                    self.finish_turn_start_success(
                        connection_id,
                        request_id,
                        &outcome,
                        capability_attachments.as_slice(),
                    )
                    .await
                }
                TurnStartSuccessResponse::VoiceSessionFinalizeAccepted { session_id } => {
                    self.finish_voice_session_finalize_accepted_turn_start_success(
                        connection_id,
                        &outcome,
                        capability_attachments.as_slice(),
                        session_id,
                    )
                    .await
                }
                TurnStartSuccessResponse::Task { .. }
                | TurnStartSuccessResponse::DurableAgent { .. } => false,
            };
            if !success_sent {
                self.mark_turn_blocked(
                    thread.id.clone(),
                    launch.turn_id.clone(),
                    "failed to commit Composer turn start lifecycle".to_owned(),
                )
                .await;
                return;
            }
            if !self
                .complete_turn(thread.id.clone(), launch.turn_id.clone(), None)
                .await
            {
                self.mark_turn_blocked(
                    thread.id,
                    launch.turn_id,
                    "failed to durably complete Composer turn after task creation".to_owned(),
                )
                .await;
            }
        })
    }

    pub(super) async fn prepare_api_provider_turn_start(
        &self,
        connection_id: ConnectionId,
        request_actor: pioneer_protocol::PersistedActorRef,
        scoped_principal_id: Option<pioneer_protocol::PrincipalId>,
        mut params: TurnStartParams,
        requested_reasoning_effort: Option<&str>,
        entry_point: crate::authorization::ExecutionAdmissionEntryPoint,
        mut execution_admission: ExecutionAuthorizationAdmission,
    ) -> Result<ApiProviderTurnAdmission, TurnStartFailure> {
        let allow_agent_skill_overlay =
            execution_backend_allows_agent_skill_overlay(params.execution_backend.as_ref());
        let thread = self
            .thread_manager
            .thread_get(params.thread_id.trim())
            .await
            .ok_or_else(|| {
                TurnStartFailure::unavailable(format!(
                    "thread `{}` is not loaded",
                    params.thread_id.trim()
                ))
            })?;
        let normalized_capabilities = self
            .normalize_turn_skill_capabilities(
                thread.workspace_id.as_str(),
                params.capabilities.as_slice(),
            )
            .await?;
        if execution_admission.uses_scoped_collaboration_policy() {
            self.enforce_scoped_skill_capability_projection(
                thread.workspace_id.as_str(),
                normalized_capabilities.execution.as_slice(),
            )
            .await?;
        }
        params.capabilities = normalized_capabilities.execution.clone();
        validate_root_agent_launch_capabilities(&params)?;
        super::message_turn::normalize_turn_collaboration_params(&mut params).map_err(|error| {
            TurnStartFailure::invalid_input(format!("invalid Turn collaboration metadata: {error}"))
        })?;
        let request_digest = native_turn_admission_digest(&request_actor, &params)
            .map_err(TurnStartFailure::invalid_input)?;
        let existing_admission = self
            .crud_store
            .get_turn_admission(params.turn_id.trim())
            .await
            .map_err(|error| {
                TurnStartFailure::internal(format!(
                    "failed to verify Turn admission request: {error:#}"
                ))
            })?;
        let existing_turn = self
            .crud_store
            .get_turn(params.thread_id.trim(), params.turn_id.trim())
            .await
            .map_err(|error| {
                TurnStartFailure::internal(format!(
                    "failed to verify Turn admission identity: {error:#}"
                ))
            })?;
        if let Some(admission) = existing_admission {
            if admission.workspace_id != thread.workspace_id
                || admission.thread_id != params.thread_id.trim()
                || admission.request_digest != request_digest
            {
                return Err(TurnStartFailure::conflict(format!(
                    "turn `{}` already has a conflicting durable admission request",
                    params.turn_id.trim()
                )));
            }
            let Some((workspace_id, existing)) = existing_turn else {
                return Err(TurnStartFailure::internal(format!(
                    "turn `{}` has durable admission state but no authoritative Turn",
                    params.turn_id.trim()
                )));
            };
            if workspace_id != thread.workspace_id {
                return Err(TurnStartFailure::internal(format!(
                    "turn `{}` admission workspace differs from its authoritative Turn",
                    params.turn_id.trim()
                )));
            }
            return Ok(ApiProviderTurnAdmission::Replay(
                pioneer_protocol::TurnStartResponse { turn: existing },
            ));
        }
        if let Some((_workspace_id, existing)) = existing_turn {
            return Err(TurnStartFailure::conflict(format!(
                "turn `{}` already exists in thread `{}` with status `{:?}` but has no durable native admission identity",
                params.turn_id.trim(),
                params.thread_id.trim(),
                existing.status
            )));
        }
        self.validate_turn_artifact_user_inputs(
            thread.workspace_id.as_str(),
            thread.id.as_str(),
            params.input.as_slice(),
        )
        .await
        .map_err(|error| {
            TurnStartFailure::unavailable(format!("failed to validate artifact input: {error}"))
        })?;
        let resolved_permission_profile = execution_admission
            .uses_scoped_collaboration_policy()
            .then(|| {
                let requested = pioneer_protocol::resolve_turn_permission_profile(
                    params.permission_profile.as_ref(),
                );
                execution_admission.cap_permission_profile(&requested)
            });
        if let Some(profile) = resolved_permission_profile.as_ref() {
            params.permission_profile =
                Some(pioneer_protocol::TurnPermissionProfileSelection { mode: profile.mode });
        }
        let security_params = params.clone();
        let author = super::message_turn::resolve_turn_author_snapshot(
            self.crud_store.as_ref(),
            &request_actor,
        )
        .await
        .map_err(|error| {
            TurnStartFailure::internal(format!("failed to resolve Turn author: {error:#}"))
        })?;
        let mentions = super::message_turn::resolve_turn_collaboration_metadata(
            self.crud_store.as_ref(),
            &request_actor,
            &params,
        )
        .await
        .map_err(|error| {
            TurnStartFailure::invalid_input(format!("invalid Turn collaboration metadata: {error}"))
        })?;
        let outcome_result = match resolved_permission_profile {
            Some(profile) => {
                self.thread_manager
                    .turn_start_with_user_metadata_and_permission_profile(
                        connection_id,
                        params,
                        profile,
                        author,
                        mentions,
                    )
                    .await
            }
            None => {
                self.thread_manager
                    .turn_start_with_user_metadata(connection_id, params, author, mentions)
                    .await
            }
        };
        let outcome = outcome_result.map_err(|error| {
            TurnStartFailure::internal(format!("failed to start turn: {error:#}"))
        })?;
        let effective_reasoning_effort = match self
            .resolve_turn_reasoning_effort(
                outcome.started_notification.workspace_id.as_str(),
                ReasoningModelLookupBackend::ApiProvider {
                    provider: outcome.materialization.thread.model_provider.as_str(),
                },
                outcome.materialization.thread.model.as_str(),
                requested_reasoning_effort,
            )
            .await
        {
            Ok(effort) => effort,
            Err(message) => {
                self.thread_manager
                    .rollback_turn_start(outcome.rollback_context.clone())
                    .await;
                return Err(TurnStartFailure::invalid_input(message));
            }
        };
        let skill_catalog = match self
            .validate_turn_skill_capabilities(
                outcome.started_notification.workspace_id.as_str(),
                outcome.materialization.capabilities.as_slice(),
            )
            .await
        {
            Ok(catalog) => catalog,
            Err(message) => {
                self.thread_manager
                    .rollback_turn_start(outcome.rollback_context.clone())
                    .await;
                return Err(message);
            }
        };
        let mut agent_skill_overlay = if allow_agent_skill_overlay
            && self.native_api_provider_supports_agent_skill_overlay(
                outcome.started_notification.workspace_id.as_str(),
                outcome.materialization.thread.model_provider.as_str(),
            ) {
            if let Some(scoped_principal_id) = scoped_principal_id.as_ref() {
                match self
                    .load_agent_skill_overlay_for_scoped_turn(
                        scoped_principal_id,
                        outcome.started_notification.workspace_id.as_str(),
                        outcome.started_notification.turn.id.as_str(),
                    )
                    .await
                {
                    Ok(overlay) => overlay,
                    Err(error) => {
                        warn!(
                            workspace_id = outcome.started_notification.workspace_id,
                            turn_id = outcome.started_notification.turn.id,
                            error = %format!("{error:#}"),
                            "failed to resolve Member Agent skill projection"
                        );
                        self.thread_manager
                            .rollback_turn_start(outcome.rollback_context.clone())
                            .await;
                        return Err(TurnStartFailure::unavailable(
                            "Member Agent skill projection is unavailable",
                        ));
                    }
                }
            } else {
                self.load_agent_skill_overlay_for_new_native_turn(
                    outcome.started_notification.workspace_id.as_str(),
                    outcome.started_notification.turn.id.as_str(),
                )
                .await
            }
        } else {
            Vec::new()
        };
        let execution_grant_capabilities =
            crate::authorization::execution_grant_capabilities_with_agent_skills(
                outcome.materialization.capabilities.as_slice(),
                agent_skill_overlay
                    .iter()
                    .map(|entry| entry.skill_id.clone()),
            );
        if let Err(message) = self
            .admit_composite_execution_request(
                &mut execution_admission,
                entry_point,
                Vec::new(),
                outcome.started_notification.workspace_id.as_str(),
                outcome.started_notification.thread_id.as_str(),
                outcome.materialization.thread.model_provider.as_str(),
                outcome.materialization.thread.model.as_str(),
                &security_params,
                execution_grant_capabilities.as_slice(),
            )
            .await
        {
            self.thread_manager
                .rollback_turn_start(outcome.rollback_context.clone())
                .await;
            return Err(message);
        }
        let user_message_capability_attachments =
            match super::agent_runtime::user_message_attachments_from_capabilities_and_catalog(
                normalized_capabilities.presentation.as_slice(),
                &skill_catalog,
                &normalized_capabilities.pack_names,
            ) {
                Ok(attachments) => attachments,
                Err(error) => {
                    self.thread_manager
                        .rollback_turn_start(outcome.rollback_context.clone())
                        .await;
                    return Err(TurnStartFailure::internal(format!(
                        "failed to snapshot selected skill presentation: {error:#}"
                    )));
                }
            };
        let profile_selected_audit = match self.turn_profile_selected_audit_event(&outcome) {
            Ok(event) => event,
            Err(error) => {
                self.thread_manager
                    .rollback_turn_start(outcome.rollback_context.clone())
                    .await;
                return Err(TurnStartFailure::internal(format!(
                    "failed to resolve turn permission profile: {error:#}"
                )));
            }
        };
        let execution_security_snapshot = match self
            .resolve_turn_execution_security_snapshot(
                &security_params,
                &outcome,
                None,
                ExecutionEnvelopeSource::Fresh(&execution_admission),
            )
            .await
        {
            Ok(snapshot) => snapshot,
            Err(failure) => {
                self.thread_manager
                    .rollback_turn_start(outcome.rollback_context.clone())
                    .await;
                return Err(failure);
            }
        };
        let security_audit_events = self.turn_security_audit_events_for_turn(
            outcome.started_notification.workspace_id.as_str(),
            outcome.started_notification.thread_id.as_str(),
            outcome.started_notification.turn.id.as_str(),
            &execution_security_snapshot,
        );
        let is_root_executable_turn = outcome.materialization.turn.mode
            != pioneer_protocol::ThreadMode::Message
            && outcome.materialization.thread.id == execution_admission.root_thread_id();
        let prepared_root_execution = if is_root_executable_turn {
            let authority = execution_admission
                .finalize(
                    outcome.materialization.thread.workspace_id.as_str(),
                    outcome.materialization.thread.id.as_str(),
                    outcome.materialization.thread.model_provider.as_str(),
                    outcome.materialization.thread.model.as_str(),
                    security_params.execution_backend.as_ref(),
                    outcome.materialization.capabilities.as_slice(),
                    &execution_security_snapshot.permission_profile,
                )
                .map_err(|error| {
                    TurnStartFailure::policy_denied(format!(
                        "failed to finalize root Agent authority: {error:#}"
                    ))
                })?;
            Some(
                super::agent_action_tools::prepare_root_agent_execution_admission(
                    self,
                    &pioneer_agent::TurnToolContext {
                        workspace_id: outcome.materialization.thread.workspace_id.clone(),
                        thread_id: outcome.materialization.thread.id.clone(),
                        turn_id: outcome.materialization.turn.id.clone(),
                    },
                    &outcome.materialization.thread,
                    &authority,
                    execution_admission
                        .runtime_draft()
                        .map(|draft| draft.access()),
                    security_params.agent_launch.as_ref(),
                    security_params.execution_backend.as_ref(),
                    None,
                )
                .await
                .map_err(|error| {
                    TurnStartFailure::policy_denied(format!(
                        "failed to admit root Agent execution: {error:#}"
                    ))
                })?,
            )
        } else {
            None
        };
        let persisted = message_future(persist_admitted_turn_start(
            self.crud_store.as_ref(),
            self.provider_registry.as_ref(),
            self.turn_execution_owner_id.as_ref(),
            &security_params,
            &outcome.materialization,
            effective_reasoning_effort.as_deref(),
            request_actor,
            profile_selected_audit,
            ExecutionEnvelopeSource::Fresh(&execution_admission),
            Some(request_digest),
            &execution_security_snapshot,
            security_audit_events,
            prepared_root_execution
                .as_ref()
                .map(|prepared| prepared.graph.clone()),
            None,
        ))
        .await;
        let graph_result = match persisted {
            Ok(result) => result,
            Err(error) => {
                self.thread_manager
                    .rollback_turn_start(outcome.rollback_context.clone())
                    .await;

                return Err(TurnStartFailure::internal(format!(
                    "failed to persist turn/start state and permission audit: {error:#}"
                )));
            }
        };
        if graph_result.as_ref().is_some_and(|result| result.queued) {
            return Err(TurnStartFailure::internal(
                "root Agent execution was unexpectedly queued",
            ));
        }
        if let Some(prepared) = prepared_root_execution {
            if let Err(error) =
                super::agent_action_tools::register_prepared_root_agent_action_binding(
                    self,
                    &pioneer_agent::TurnToolContext {
                        workspace_id: outcome.materialization.thread.workspace_id.clone(),
                        thread_id: outcome.materialization.thread.id.clone(),
                        turn_id: outcome.materialization.turn.id.clone(),
                    },
                    prepared,
                )
                .await
            {
                let message = format!("failed to bind admitted root Agent execution: {error:#}");
                self.mark_turn_blocked(
                    outcome.materialization.thread.id.clone(),
                    outcome.materialization.turn.id.clone(),
                    message.clone(),
                )
                .await;
                return Err(TurnStartFailure::internal(message));
            }
        }
        self.complete_runtime_draft_materialization(ExecutionEnvelopeSource::Fresh(
            &execution_admission,
        ))
        .await;
        if let Err(error) = self
            .register_execution_lease(outcome.started_notification.turn.id.as_str())
            .await
        {
            let message = format!("failed to register execution lease: {error:#}");
            self.mark_turn_blocked(
                outcome.started_notification.thread_id.clone(),
                outcome.started_notification.turn.id.clone(),
                message.clone(),
            )
            .await;
            return Err(TurnStartFailure::internal(message));
        }

        self.ensure_hook_runtime_with_run_store().await;
        if let Err(error) = self
            .agent_manager
            .ensure_thread(
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.workspace_id.as_str(),
            )
            .await
        {
            self.mark_turn_blocked(
                outcome.started_notification.thread_id.clone(),
                outcome.started_notification.turn.id.clone(),
                format!("failed to prepare agent thread runtime: {error}"),
            )
            .await;
            return Err(TurnStartFailure::unavailable(format!(
                "failed to prepare agent thread runtime: {error}"
            )));
        }

        if let Err(error) = self
            .ensure_agent_listener_task(outcome.started_notification.thread_id.as_str())
            .await
        {
            let message = format!("failed to activate native durable listener: {error:#}");
            self.mark_turn_blocked(
                outcome.started_notification.thread_id.clone(),
                outcome.started_notification.turn.id.clone(),
                message.clone(),
            )
            .await;
            return Err(TurnStartFailure::unavailable(message));
        }
        let history = self
            .load_conversation_history_for_workspace(
                outcome.started_notification.workspace_id.as_str(),
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.turn.id.as_str(),
            )
            .await;
        let workspace_skill_policies = match self
            .crud_store
            .list_workspace_skill_policies(outcome.started_notification.workspace_id.as_str())
            .await
        {
            Ok(records) => records
                .into_iter()
                .map(|record| {
                    (
                        pioneer_skills::SkillPolicyKey::new(record.skill_id),
                        pioneer_agent::WorkspaceSkillPolicy {
                            enabled: record.enabled,
                            allow_implicit_invocation: record.allow_implicit_invocation,
                        },
                    )
                })
                .collect::<HashMap<_, _>>(),
            Err(error) => {
                warn!(
                    workspace_id = outcome.started_notification.workspace_id,
                    error = %format!("{error:#}"),
                    "failed to load authoritative workspace skill policies"
                );
                let message = if scoped_principal_id.is_some() {
                    "Member skill projection is unavailable".to_owned()
                } else {
                    format!("failed to load workspace skill policies: {error:#}")
                };
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    message.clone(),
                )
                .await;
                return Err(TurnStartFailure::unavailable(message));
            }
        };
        let resolved_artifacts = match self
            .resolve_provider_artifact_inputs(
                outcome.started_notification.workspace_id.as_str(),
                outcome.materialization.input.as_slice(),
            )
            .await
        {
            Ok(resolved_artifacts) => resolved_artifacts,
            Err(error) => {
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    format!("failed to resolve artifact input for provider: {error:#}"),
                )
                .await;
                return Err(TurnStartFailure::unavailable(format!(
                    "failed to resolve artifact input for provider: {error:#}"
                )));
            }
        };
        let runtime_environment = match self
            .create_artifact_output_environment(
                outcome.started_notification.workspace_id.as_str(),
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.turn.id.as_str(),
            )
            .await
        {
            Ok(runtime_environment) => runtime_environment.into_iter().collect::<HashMap<_, _>>(),
            Err(error) => {
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    format!("failed to prepare artifact output directory: {error:#}"),
                )
                .await;
                return Err(TurnStartFailure::unavailable(format!(
                    "failed to prepare artifact output directory: {error:#}"
                )));
            }
        };
        let hook_runtime_context = scoped_principal_id.map_or_else(
            pioneer_agent::AgentTurnHookRuntimeContext::default,
            |principal_id| pioneer_agent::AgentTurnHookRuntimeContext {
                actor_kind: pioneer_hooks::HookActorKind::User,
                actor_id: Some(principal_id.to_string()),
                ..pioneer_agent::AgentTurnHookRuntimeContext::default()
            },
        );
        let snapshot_result = self
            .persist_turn_runtime_snapshot_with_optional_agent_overlay(
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.workspace_id.as_str(),
                outcome.started_notification.turn.id.as_str(),
                outcome.materialization.turn.mode,
                &hook_runtime_context,
                &outcome.materialization.thread.model,
                &outcome.materialization.thread.model_provider,
                effective_reasoning_effort.as_deref(),
                &workspace_skill_policies,
                outcome.materialization.input.as_slice(),
                outcome.materialization.capabilities.as_slice(),
                resolved_artifacts.as_slice(),
                &runtime_environment,
                history.as_slice(),
                &mut agent_skill_overlay,
            )
            .await;
        if let Err(error) = snapshot_result {
            self.mark_turn_blocked(
                outcome.started_notification.thread_id.clone(),
                outcome.started_notification.turn.id.clone(),
                format!("failed to persist turn runtime snapshot: {error:#}"),
            )
            .await;
            return Err(TurnStartFailure::internal(format!(
                "failed to persist turn runtime snapshot: {error:#}"
            )));
        }
        let permission_profile =
            match self.materialized_turn_permission_profile(&outcome.materialization.turn) {
                Ok(permission_profile) => permission_profile,
                Err(error) => {
                    self.mark_turn_blocked(
                        outcome.started_notification.thread_id.clone(),
                        outcome.started_notification.turn.id.clone(),
                        format!("failed to resolve turn permission profile: {error:#}"),
                    )
                    .await;
                    return Err(TurnStartFailure::internal(format!(
                        "failed to resolve turn permission profile: {error:#}"
                    )));
                }
            };

        Ok(ApiProviderTurnAdmission::New(
            PreparedApiProviderTurnStart {
                outcome,
                user_message_capability_attachments,
                workspace_skill_policies,
                skill_catalog,
                agent_skill_overlay,
                resolved_artifacts,
                runtime_environment,
                history,
                effective_reasoning_effort,
                permission_profile,
                execution_security_snapshot,
            },
        ))
    }

    pub(super) async fn finish_api_provider_turn_start_without_response(
        &self,
        connection_id: ConnectionId,
        prepared: &PreparedApiProviderTurnStart,
    ) -> bool {
        self.session_manager
            .set_connection_workspace(
                connection_id,
                Some(prepared.outcome.started_notification.workspace_id.clone()),
            )
            .await;
        message_future(self.publish_turn_start_success(
            &prepared.outcome,
            prepared.user_message_capability_attachments.as_slice(),
        ))
        .await
    }

    pub(super) async fn block_prepared_api_provider_turn_start(
        &self,
        prepared: &PreparedApiProviderTurnStart,
        reason: String,
    ) -> bool {
        self.mark_turn_blocked(
            prepared.outcome.started_notification.thread_id.clone(),
            prepared.outcome.started_notification.turn.id.clone(),
            reason,
        )
        .await
    }

    pub(super) async fn dispatch_prepared_api_provider_turn_start(
        &self,
        prepared: PreparedApiProviderTurnStart,
    ) {
        let outcome = prepared.outcome;
        let start_result = self
            .agent_manager
            .start_turn_with_resolved_artifacts_environment_reasoning_permission_profile_security_snapshot_and_agent_skill_overlay(
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.turn.id.as_str(),
                outcome.materialization.turn.mode,
                &outcome.materialization.thread.model,
                &outcome.materialization.thread.model_provider,
                prepared.workspace_skill_policies,
                prepared.skill_catalog,
                prepared.agent_skill_overlay,
                outcome.materialization.input.clone(),
                outcome.materialization.capabilities.clone(),
                prepared.resolved_artifacts,
                prepared.runtime_environment,
                prepared.history,
                prepared.effective_reasoning_effort.as_deref(),
                prepared.permission_profile,
                prepared.execution_security_snapshot,
            )
            .await;
        if let Err(error) = start_result {
            let reason = format!("failed to dispatch turn to agent runtime: {error}");
            if !self
                .report_turn_failure(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    TurnFailureRecoveryKind::TurnDispatch,
                    reason.clone(),
                )
                .await
            {
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    reason,
                )
                .await;
            }
            return;
        }

        let now = now_timestamp_secs();
        let ownership_started = self
            .crud_store
            .mark_turn_execution_running_owned(
                outcome.started_notification.turn.id.as_str(),
                self.turn_execution_owner_id.as_ref(),
                now,
                now.saturating_add(super::TURN_EXECUTION_OWNER_LEASE_SECONDS),
            )
            .await;
        if !matches!(ownership_started, Ok(true)) {
            let reason = match ownership_started {
                Ok(false) => {
                    "Turn execution ownership changed before native dispatch completed".to_owned()
                }
                Err(error) => {
                    format!("failed to persist native Turn execution ownership: {error:#}")
                }
                Ok(true) => unreachable!(),
            };
            let _ = self
                .agent_manager
                .cancel_turn(
                    outcome.started_notification.thread_id.as_str(),
                    outcome.started_notification.turn.id.as_str(),
                    reason.as_str(),
                )
                .await;
            if !self
                .report_turn_failure(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    TurnFailureRecoveryKind::TurnDispatch,
                    reason.clone(),
                )
                .await
            {
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    reason,
                )
                .await;
            }
        }
    }

    /// Builds the ordinary API-provider runtime payload for a agent domain
    /// child whose canonical Turn/action/execution graph is already durable.
    /// This is the post-commit half of native admission; it never rewrites the
    /// Turn or substitutes a session/principal actor.
    pub(super) async fn prepare_committed_agent_api_turn(
        &self,
        params: &TurnStartParams,
        outcome: crate::thread::TurnStartOutcome,
        authority: &crate::authorization::ExecutionAuthorizationContext,
    ) -> Result<PreparedApiProviderTurnStart, String> {
        if !matches!(
            &params.execution_backend,
            Some(pioneer_protocol::AgentExecutionBackend::ApiProvider { .. })
        ) {
            return Err("committed API child has a non-API execution backend".to_owned());
        }
        let effective_reasoning_effort = self
            .resolve_turn_reasoning_effort(
                outcome.started_notification.workspace_id.as_str(),
                ReasoningModelLookupBackend::ApiProvider {
                    provider: outcome.materialization.thread.model_provider.as_str(),
                },
                outcome.materialization.thread.model.as_str(),
                requested_reasoning_effort(params).as_deref(),
            )
            .await?;
        let skill_catalog = self
            .validate_turn_skill_capabilities(
                outcome.started_notification.workspace_id.as_str(),
                outcome.materialization.capabilities.as_slice(),
            )
            .await
            .map_err(|error| error.diagnostic)?;
        let execution_security_snapshot = self
            .crud_store
            .get_turn_execution_security_snapshot(outcome.started_notification.turn.id.as_str())
            .await
            .map_err(|error| format!("failed to load committed security snapshot: {error:#}"))?
            .ok_or_else(|| "committed Agent Turn has no security snapshot".to_owned())?
            .snapshot;
        if execution_security_snapshot.permission_profile
            != outcome.materialization.turn.permission_profile
        {
            return Err(
                "committed Agent Turn security snapshot differs from its permission profile"
                    .to_owned(),
            );
        }
        if authority.workspace_id() != outcome.started_notification.workspace_id {
            return Err("committed Agent Turn authority differs from its workspace".to_owned());
        }
        self.register_execution_lease(outcome.started_notification.turn.id.as_str())
            .await
            .map_err(|error| format!("failed to register execution lease: {error:#}"))?;

        self.ensure_hook_runtime_with_run_store().await;
        self.agent_manager
            .ensure_thread(
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.workspace_id.as_str(),
            )
            .await
            .map_err(|error| format!("failed to prepare agent thread runtime: {error}"))?;
        self.ensure_agent_listener_task(outcome.started_notification.thread_id.as_str())
            .await
            .map_err(|error| format!("failed to activate durable listener: {error:#}"))?;
        let history = self
            .load_conversation_history_for_workspace(
                outcome.started_notification.workspace_id.as_str(),
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.turn.id.as_str(),
            )
            .await;
        let workspace_skill_policies = self
            .crud_store
            .list_workspace_skill_policies(outcome.started_notification.workspace_id.as_str())
            .await
            .map_err(|error| format!("failed to load workspace skill policies: {error:#}"))?
            .into_iter()
            .map(|record| {
                (
                    pioneer_skills::SkillPolicyKey::new(record.skill_id),
                    pioneer_agent::WorkspaceSkillPolicy {
                        enabled: record.enabled,
                        allow_implicit_invocation: record.allow_implicit_invocation,
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        let resolved_artifacts = self
            .resolve_provider_artifact_inputs(
                outcome.started_notification.workspace_id.as_str(),
                outcome.materialization.input.as_slice(),
            )
            .await
            .map_err(|error| format!("failed to resolve artifact input: {error:#}"))?;
        let runtime_environment = self
            .create_artifact_output_environment(
                outcome.started_notification.workspace_id.as_str(),
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.turn.id.as_str(),
            )
            .await
            .map_err(|error| format!("failed to prepare artifact output: {error:#}"))?
            .into_iter()
            .collect::<HashMap<_, _>>();
        let mut agent_skill_overlay = Vec::new();
        self.persist_turn_runtime_snapshot_with_optional_agent_overlay(
            outcome.started_notification.thread_id.as_str(),
            outcome.started_notification.workspace_id.as_str(),
            outcome.started_notification.turn.id.as_str(),
            outcome.materialization.turn.mode,
            &pioneer_agent::AgentTurnHookRuntimeContext::default(),
            &outcome.materialization.thread.model,
            &outcome.materialization.thread.model_provider,
            effective_reasoning_effort.as_deref(),
            &workspace_skill_policies,
            outcome.materialization.input.as_slice(),
            outcome.materialization.capabilities.as_slice(),
            resolved_artifacts.as_slice(),
            &runtime_environment,
            history.as_slice(),
            &mut agent_skill_overlay,
        )
        .await
        .map_err(|error| format!("failed to persist child runtime snapshot: {error:#}"))?;
        let permission_profile = self
            .materialized_turn_permission_profile(&outcome.materialization.turn)
            .map_err(|error| format!("failed to resolve permission profile: {error:#}"))?;

        Ok(PreparedApiProviderTurnStart {
            outcome,
            user_message_capability_attachments: Vec::new(),
            workspace_skill_policies,
            skill_catalog,
            agent_skill_overlay,
            resolved_artifacts,
            runtime_environment,
            history,
            effective_reasoning_effort,
            permission_profile,
            execution_security_snapshot,
        })
    }

    async fn send_turn_start_failure(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        success_response: &TurnStartSuccessResponse,
        thread_id: &str,
        turn_id: &str,
        failure: impl Into<TurnStartFailure>,
    ) {
        let failure = failure.into();
        let public_error = crate::public_error::map_agent_failure(
            failure.public_code,
            pioneer_protocol::PublicErrorStage::Admission,
            failure.diagnostic,
        );
        match success_response {
            TurnStartSuccessResponse::TurnStart => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse {
                        jsonrpc: pioneer_protocol::JSONRPC_VERSION.to_owned(),
                        id: Some(request_id),
                        error: pioneer_protocol::JsonRpcError {
                            code: INVALID_REQUEST_CODE,
                            message: public_error.message.clone(),
                            data: Some(json!({ "public_error": public_error })),
                        },
                    },
                )
                .await;
            }
            TurnStartSuccessResponse::VoiceSessionFinalizeAccepted { session_id } => {
                let error = VoiceError {
                    kind: VoiceErrorKind::Unknown,
                    message: public_error.message.clone(),
                    public_error: Some(public_error),
                };
                warn!(
                    connection_id,
                    session_id = %session_id,
                    turn_id,
                    error = %error.message,
                    "accepted voice finalize failed while admitting turn"
                );
                self.send_voice_session_result_notification(
                    connection_id,
                    thread_id,
                    VoiceSessionResultNotification {
                        session_id: session_id.clone(),
                        outcome: VoiceSessionOutcome::Failed,
                        turn_id: (!turn_id.trim().is_empty()).then(|| turn_id.to_owned()),
                        error: Some(error),
                    },
                )
                .await;
            }
            TurnStartSuccessResponse::Task { .. }
            | TurnStartSuccessResponse::DurableAgent { .. } => {
                let encoded = serde_json::to_string(&public_error)
                    .unwrap_or_else(|_| public_error.correlation_id.clone());
                success_response.complete_task(Err(anyhow::anyhow!(encoded)));
            }
        }
    }

    pub(super) async fn prepare_task_cli_runtime_turn(
        &self,
        params: TurnStartParams,
        runtime_id: String,
        runtime_kind: CLIAgentRuntimeKind,
        permission_profile: pioneer_protocol::TurnPermissionProfileSnapshot,
        execution_security_snapshot: pioneer_protocol::TurnExecutionSecuritySnapshot,
        execution_authorization_context: crate::authorization::ExecutionAuthorizationContext,
        execution_authorization_revalidation: crate::authorization::RevalidatedExecutionAuthorization,
        continuation_thread_id: String,
        context_thread_id: String,
        task_run_id: String,
        execution_id: String,
        conversation_history: Vec<ChatMessage>,
        agent_author: pioneer_protocol::TurnAuthorSnapshot,
        agent_turn_response: pioneer_crud::AgentTurnResponseInput,
    ) -> anyhow::Result<PreparedCliRuntimeNativeTurnStart> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let response = TurnStartSuccessResponse::Task {
            permission_profile,
            execution_security_snapshot,
            continuation_thread_id,
            context_thread_id,
            task_run_id,
            execution_id,
            conversation_history,
            agent_author: Some(agent_author),
            agent_turn_response,
            completion: std::sync::Arc::new(std::sync::Mutex::new(Some(sender))),
        };
        self.turn_start_cli_runtime(
            0,
            RequestId::new(generate_id(pioneer_protocol::REQUEST_ID_LEN))
                .expect("generated request id must have protocol length"),
            pioneer_protocol::PersistedActorRef::System,
            params,
            runtime_id,
            runtime_kind,
            TurnExecutionAuthority::Durable {
                context: execution_authorization_context,
                revalidation: std::sync::Arc::new(execution_authorization_revalidation),
            },
            response,
        )
        .await;
        receiver
            .await
            .context("task CLI runtime preparation ended without a result")?
    }

    pub(super) async fn prepare_committed_agent_cli_runtime_turn(
        &self,
        params: TurnStartParams,
        runtime_id: String,
        runtime_kind: CLIAgentRuntimeKind,
        permission_profile: pioneer_protocol::TurnPermissionProfileSnapshot,
        execution_security_snapshot: pioneer_protocol::TurnExecutionSecuritySnapshot,
        execution_authorization_context: crate::authorization::ExecutionAuthorizationContext,
        continuation_thread_id: String,
        context_thread_id: String,
        conversation_history: Vec<ChatMessage>,
        agent_author: pioneer_protocol::TurnAuthorSnapshot,
    ) -> anyhow::Result<PreparedCliRuntimeNativeTurnStart> {
        let execution_authorization_revalidation = self
            .execution_leases
            .revalidate_for_turn(
                self.crud_store.as_ref(),
                &execution_authorization_context,
                execution_authorization_context.workspace_id(),
                params.thread_id.as_str(),
                params.turn_id.as_str(),
                crate::authorization::ResourceAction::AgentTurnStart,
                self.authorization_invalidation_hub
                    .current_revision()
                    .await
                    .context("committed Agent policy generation is unavailable")?,
            )
            .await
            .context("committed Agent authority no longer permits CLI activation")?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let response = TurnStartSuccessResponse::DurableAgent {
            permission_profile,
            execution_security_snapshot,
            continuation_thread_id,
            context_thread_id,
            conversation_history,
            agent_author,
            completion: std::sync::Arc::new(std::sync::Mutex::new(Some(sender))),
        };
        self.turn_start_cli_runtime(
            0,
            RequestId::new(generate_id(pioneer_protocol::REQUEST_ID_LEN))
                .expect("generated request id must have protocol length"),
            pioneer_protocol::PersistedActorRef::System,
            params,
            runtime_id,
            runtime_kind,
            TurnExecutionAuthority::Durable {
                context: execution_authorization_context,
                revalidation: std::sync::Arc::new(execution_authorization_revalidation),
            },
            response,
        )
        .await;
        receiver
            .await
            .context("committed Agent CLI runtime preparation ended without a result")?
    }

    pub(super) async fn activate_prepared_committed_agent_cli_runtime_turn(
        &self,
        prepared: PreparedCliRuntimeNativeTurnStart,
    ) -> anyhow::Result<()> {
        let turn_id = prepared.outcome.started_notification.turn.id.clone();
        if !self
            .publish_turn_start_success(
                &prepared.outcome,
                prepared.user_message_capability_attachments.as_slice(),
            )
            .await
        {
            self.release_cli_runtime_session_turn_lease(turn_id.as_str())
                .await;
            anyhow::bail!("failed to publish committed Agent CLI runtime turn start");
        }
        self.spawn_prepared_cli_runtime_native_turn(prepared);
        Ok(())
    }

    pub(super) async fn activate_prepared_task_cli_runtime_turn(
        &self,
        prepared: PreparedCliRuntimeNativeTurnStart,
    ) -> anyhow::Result<()> {
        let turn_id = prepared.outcome.started_notification.turn.id.clone();
        if !self
            .publish_turn_start_success(
                &prepared.outcome,
                prepared.user_message_capability_attachments.as_slice(),
            )
            .await
        {
            self.release_cli_runtime_session_turn_lease(turn_id.as_str())
                .await;
            let thread_id = prepared.outcome.started_notification.thread_id.clone();
            let reason = "failed to publish task CLI runtime turn start".to_owned();
            if !self
                .report_turn_failure(
                    thread_id.clone(),
                    turn_id.clone(),
                    TurnFailureRecoveryKind::TaskDispatch,
                    reason.clone(),
                )
                .await
            {
                self.mark_turn_blocked(thread_id, turn_id, reason).await;
            }
            anyhow::bail!("failed to publish task CLI runtime turn start");
        }
        self.spawn_prepared_cli_runtime_native_turn(prepared);
        Ok(())
    }

    pub(super) async fn abort_prepared_task_cli_runtime_turn(
        &self,
        prepared: PreparedCliRuntimeNativeTurnStart,
        reason: String,
    ) {
        let thread_id = prepared.outcome.started_notification.thread_id.clone();
        let turn_id = prepared.outcome.started_notification.turn.id.clone();
        if !self
            .mark_turn_blocked(thread_id.clone(), turn_id.clone(), reason.clone())
            .await
        {
            warn!(
                thread_id,
                turn_id,
                reason,
                "failed to durably close an aborted prepared task CLI runtime turn"
            );
        }
        self.release_cli_runtime_session_turn_lease(turn_id.as_str())
            .await;
    }

    fn prepare_cli_runtime_combined_preflight<'a>(
        &'a self,
        thread: &'a pioneer_protocol::Thread,
        params: &'a TurnStartParams,
        runtime_id: &'a str,
        runtime_kind: CLIAgentRuntimeKind,
        runtime_config: &'a pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig,
        capability_partition: crate::cli_runtime::skills::CliRuntimeCapabilityPartition,
        requested_mcp: bool,
        provider_claim_matches: bool,
    ) -> MessageFuture<'a, Result<PreparedCliRuntimeCombinedPreflight, TurnStartFailure>> {
        message_future(async move {
            let readiness_snapshot = self
                .cli_runtime_probe_snapshot(thread.workspace_id.as_str(), runtime_id)
                .await
                .map_err(|error| {
                    TurnStartFailure::internal(format!(
                        "failed to load CLI runtime readiness snapshot: {error:#}"
                    ))
                })?
                .ok_or_else(|| {
                    TurnStartFailure::internal(format!(
                        "CLI runtime `{runtime_id}` is absent from the readiness snapshot"
                    ))
                })?;
            let readiness_summary = readiness_snapshot.summary;
            let cached_mcp_readiness = readiness_snapshot.mcp_readiness;
            if !matches!(readiness_summary.status, RuntimeStatus::Ready) {
                return Err(TurnStartFailure::invalid_input(format!(
                    "CLI runtime `{runtime_id}` is not ready: {}",
                    cli_runtime_unavailable_reason(&readiness_summary.status)
                )));
            }

            let mcp_projection = match self
                .mcp_service
                .resolve_mcp_turn_projection(&pioneer_agent::AgentMcpMaterializationRequest {
                    workspace_id: thread.workspace_id.clone(),
                    turn_id: params.turn_id.clone(),
                    explicit_servers: capability_partition.mcp_servers.clone(),
                    explicit_tools: capability_partition.mcp_tools.clone(),
                })
                .await
            {
                Ok(projection) => Some(projection),
                Err(error) => {
                    let code = match error.reason {
                        pioneer_agent::AgentMcpMaterializationFailureReason::ExplicitCapabilityRejected => {
                            crate::cli_mcp_client_validation::CliMcpClientValidationRejectionCode::ExplicitCapabilityUnresolved.as_str()
                        }
                        pioneer_agent::AgentMcpMaterializationFailureReason::RequiredInstallationUnavailable => {
                            "cli_runtime.mcp.required_installation_unavailable"
                        }
                        pioneer_agent::AgentMcpMaterializationFailureReason::ResolutionUncertain => {
                            "cli_runtime.mcp.resolution_uncertain"
                        }
                        pioneer_agent::AgentMcpMaterializationFailureReason::ProjectionInvalid => {
                            "cli_runtime.mcp.projection_invalid"
                        }
                        pioneer_agent::AgentMcpMaterializationFailureReason::ProviderUnavailable => {
                            "cli_runtime.mcp.provider_unavailable"
                        }
                    };
                    crate::cli_mcp_client_validation::persist_cli_mcp_materialization_rejections(
                        &self.crud_store,
                        crate::cli_mcp_client_validation::CliMcpClientValidationAuditContext {
                            workspace_id: Some(thread.workspace_id.as_str()),
                            thread_id: thread.id.as_str(),
                            turn_id: params.turn_id.as_str(),
                            runtime_id,
                        },
                        cli_mcp_client_target(runtime_kind),
                        code,
                        error.rejected_capabilities.as_slice(),
                    )
                    .await;
                    return Err(TurnStartFailure::internal(format!(
                        "{code}: combined MCP and skill preflight failed: {error}"
                    )));
                }
            };
            let combined_preflight_input =
                crate::cli_runtime::skills::CliRuntimeCombinedPreflightInput {
                    capabilities: capability_partition,
                    mcp_projection,
                };
            let projected_mcp_availability = combined_preflight_input.exact_mcp_availability();
            let attachments = combined_preflight_input.capabilities.skills.as_slice();
            let (skill_install_plans, resolved_skill_bindings) = if attachments.is_empty() {
                (Vec::new(), Vec::new())
            } else {
                let preflight_started = std::time::Instant::now();
                let resolved = match self
                    .resolve_cli_runtime_skill_attachments(
                        thread.workspace_id.as_str(),
                        attachments,
                        &projected_mcp_availability,
                    )
                    .await
                {
                    Ok(resolved) => resolved,
                    Err(error) => {
                        let failure_reason =
                            format!("failed to resolve CLI runtime skills: {error:#}");
                        for attachment in attachments {
                            warn!(
                                event = "cli_runtime_skill_preflight",
                                runtime_id,
                                runtime_kind = ?runtime_kind,
                                skill_id = %attachment.skill_id,
                                capability_id = attachment.capability_id.as_str(),
                                result = "failed",
                                failure_reason = failure_reason.as_str(),
                                elapsed_ms = preflight_started.elapsed().as_millis(),
                                "CLI runtime skill preflight failed"
                            );
                        }
                        return Err(TurnStartFailure::policy_denied(failure_reason));
                    }
                };
                if let Err(error) =
                    crate::cli_runtime::skills::ensure_cli_runtime_skills_exportable(&resolved)
                {
                    warn!(
                        event = "cli_runtime_skill_preflight",
                        runtime_id,
                        runtime_kind = ?runtime_kind,
                        skill_slug = error.skill_slug.as_str(),
                        source_kind = "system",
                        result = "failed",
                        failure_reason = crate::cli_runtime::skills::CLI_RUNTIME_SYSTEM_SKILL_NOT_EXPORTABLE,
                        elapsed_ms = preflight_started.elapsed().as_millis(),
                        "CLI runtime skill preflight rejected Pioneer-only required system skill"
                    );
                    return Err(TurnStartFailure::invalid_input(error.to_string()));
                }
                if let Err(error) =
                    crate::cli_runtime::skills::ensure_cli_runtime_skill_invocation_eligible(
                        runtime_kind,
                        &runtime_config.display_name,
                        &resolved,
                    )
                {
                    if let Some(skill) = resolved
                        .iter()
                        .find(|skill| skill.definition.runtime.disable_model_invocation)
                    {
                        warn!(
                            event = "cli_runtime_skill_preflight",
                            runtime_id,
                            runtime_kind = ?runtime_kind,
                            skill_slug = skill.slug.as_str(),
                            source_kind = skill.definition.identity.source_kind.as_db_value(),
                            result = "failed",
                            failure_reason = crate::cli_runtime::skills::CLI_RUNTIME_CLAUDE_SKILL_NOT_MODEL_INVOCABLE,
                            elapsed_ms = preflight_started.elapsed().as_millis(),
                            "CLI runtime skill preflight rejected unsupported native invocation"
                        );
                    }
                    return Err(TurnStartFailure::internal(error.to_string()));
                }
                let resolved_skill_bindings =
                    crate::cli_runtime::skills::cli_runtime_turn_skill_bindings(&resolved);
                let receipt_path = self
                    .artifact_runtime_home
                    .join(pioneer_skills::EXTERNAL_RUNTIME_RECEIPT_FILE_NAME);
                let plans = match crate::cli_runtime::skills::build_cli_runtime_skill_install_plans(
                    runtime_config,
                    runtime_kind,
                    &resolved,
                    &receipt_path,
                ) {
                    Ok(plans) => plans,
                    Err(error) => {
                        let failure_reason =
                            format!("failed to plan CLI runtime skills: {error:#}");
                        for skill in &resolved {
                            warn!(
                                event = "cli_runtime_skill_preflight",
                                runtime_id,
                                runtime_kind = ?runtime_kind,
                                skill_slug = skill.slug.as_str(),
                                source_kind = skill.definition.identity.source_kind.as_db_value(),
                                install_name = pioneer_skills::sanitize_name(
                                    &skill.definition.identity.name
                                ),
                                result = "failed",
                                failure_reason = failure_reason.as_str(),
                                elapsed_ms = preflight_started.elapsed().as_millis(),
                                "CLI runtime skill preflight planning failed"
                            );
                        }
                        return Err(TurnStartFailure::internal(failure_reason));
                    }
                };
                (plans, resolved_skill_bindings)
            };
            let plan = crate::cli_runtime::skills::CliRuntimeCombinedPreflightPlan {
                mcp_projection: combined_preflight_input.mcp_projection,
                skill_install_plans,
                skill_bindings: resolved_skill_bindings,
            };
            let mut codex_mcp_launch_projection = None;
            let mut claude_mcp_launch_projection = None;
            if let Some(projection) = plan.mcp_projection.as_ref() {
                // Managed Claude always receives the reserved first-party
                // filesystem facade, even when the user selected no external
                // MCP server.  Codex keeps its provider-owned aggregate path
                // and therefore retains the old requested/selected gate.
                let has_mcp_projection = requested_mcp
                    || !projection.tools.is_empty()
                    || runtime_kind == CLIAgentRuntimeKind::Claude;
                if has_mcp_projection {
                    let validation =
                        crate::cli_mcp_client_validation::validate_cli_mcp_client_request_durably(
                            &self.crud_store,
                            crate::cli_mcp_client_validation::CliMcpClientValidationAuditContext {
                                workspace_id: Some(thread.workspace_id.as_str()),
                                thread_id: thread.id.as_str(),
                                turn_id: params.turn_id.as_str(),
                                runtime_id,
                            },
                            crate::cli_mcp_client_validation::CliMcpClientValidationEvidence {
                                target: cli_mcp_client_target(runtime_kind),
                                has_mcp_projection,
                                provider_claim_matches,
                                runtime_snapshot_current: true,
                                runtime_supports_mcp_tools: readiness_summary
                                    .capabilities
                                    .supports_mcp_tools,
                                projection_workspace_matches: projection.workspace_id
                                    == thread.workspace_id,
                                explicit_capabilities_resolved:
                                    cli_mcp_projection_resolves_all_explicit_capabilities(
                                        &combined_preflight_input.capabilities,
                                        projection,
                                    ),
                            },
                        )
                        .await;
                    if let Err(rejection) = validation {
                        let diagnostic = readiness_summary
                            .diagnostics
                            .iter()
                            .find(|diagnostic| diagnostic.code.starts_with("cli_runtime.mcp."));
                        let code = diagnostic
                            .map(|diagnostic| diagnostic.code.as_str())
                            .unwrap_or_else(|| rejection.code.as_str());
                        let message = diagnostic
                            .map(|diagnostic| diagnostic.message.as_str())
                            .unwrap_or(rejection.message);
                        warn!(
                            event = "combined_cli_preflight",
                            runtime_id,
                            runtime_kind = ?runtime_kind,
                            manifest_hash = projection.manifest_hash.as_str(),
                            diagnostic_code = code,
                            rejection_reason = ?rejection.reason,
                            "combined MCP and skill preflight rejected the client MCP claim"
                        );
                        return Err(TurnStartFailure::internal(format!("{code}: {message}")));
                    }
                    let readiness = cached_mcp_readiness.clone().ok_or_else(|| {
                        TurnStartFailure::unavailable(
                            "cli_runtime.mcp.readiness_unavailable: Gateway MCP readiness snapshot is not available",
                        )
                    })?;
                    if !readiness.supported {
                        let diagnostic = readiness
                            .diagnostics
                            .iter()
                            .find(|diagnostic| diagnostic.code.starts_with("cli_runtime.mcp."));
                        return Err(TurnStartFailure::unavailable(format!(
                            "{}: {}",
                            diagnostic
                                .map(|diagnostic| diagnostic.code.as_str())
                                .unwrap_or("cli_runtime.mcp.readiness_unavailable"),
                            diagnostic
                                .map(|diagnostic| diagnostic.message.as_str())
                                .unwrap_or("MCP tool readiness is not available")
                        )));
                    }
                    if runtime_kind == CLIAgentRuntimeKind::Codex {
                        codex_mcp_launch_projection = Some(
                            crate::cli_runtime::codex_mcp::build_codex_mcp_session_launch_projection(
                                projection.clone(),
                                readiness.contract_fingerprint,
                            )
                            .map_err(|error| {
                                TurnStartFailure::internal(format!(
                                    "failed to prepare Codex MCP schema projection: {error}"
                                ))
                            })?,
                        );
                    } else if runtime_kind == CLIAgentRuntimeKind::Claude {
                        claude_mcp_launch_projection = Some(
                            crate::cli_runtime::claude_mcp::build_claude_mcp_session_launch_projection(
                                projection.clone(),
                                readiness.contract_fingerprint,
                            )
                            .map_err(|error| {
                                TurnStartFailure::internal(format!(
                                    "failed to prepare Claude MCP schema projection: {error}"
                                ))
                            })?,
                        );
                    }
                }
            }
            Ok(PreparedCliRuntimeCombinedPreflight {
                plan,
                codex_mcp_launch_projection,
                claude_mcp_launch_projection,
            })
        })
    }

    pub(super) fn turn_start_cli_runtime<'a>(
        &'a self,
        connection_id: ConnectionId,
        request_id: RequestId,
        request_actor: pioneer_protocol::PersistedActorRef,
        mut params: TurnStartParams,
        runtime_id: String,
        runtime_kind: CLIAgentRuntimeKind,
        mut execution_authority: TurnExecutionAuthority,
        success_response: TurnStartSuccessResponse,
    ) -> MessageFuture<'a, ()> {
        message_future(async move {
            // A hidden Task CLI turn is still authored by the admitted agent.
            // The transport request itself is System-owned, so prefer the
            // immutable author snapshot carried by the Task response before
            // resolving any conversation metadata or persisted turn author.
            let request_actor = success_response
                .task_agent_author()
                .map(|author| author.actor)
                .unwrap_or(request_actor);
            let admission_entry_point = match &success_response {
                TurnStartSuccessResponse::TurnStart => {
                    crate::authorization::ExecutionAdmissionEntryPoint::CliRuntime
                }
                TurnStartSuccessResponse::VoiceSessionFinalizeAccepted { .. } => {
                    crate::authorization::ExecutionAdmissionEntryPoint::VoiceTurn
                }
                TurnStartSuccessResponse::Task { .. } => {
                    crate::authorization::ExecutionAdmissionEntryPoint::Task
                }
                TurnStartSuccessResponse::DurableAgent { .. } => {
                    crate::authorization::ExecutionAdmissionEntryPoint::AgentTurnStart
                }
            };
            let response_turn_id = params.turn_id.clone();
            let response_thread_id = params.thread_id.clone();
            let submitted_model_provider = params.model_provider.clone();
            macro_rules! send_turn_start_failure {
                ($message:expr) => {{
                    self.send_turn_start_failure(
                        connection_id,
                        request_id.clone(),
                        &success_response,
                        response_thread_id.as_str(),
                        response_turn_id.as_str(),
                        $message,
                    )
                    .await;
                }};
            }

            let authority_matches_response = matches!(
                (&success_response, &execution_authority),
                (
                    TurnStartSuccessResponse::Task { .. },
                    TurnExecutionAuthority::Durable { .. }
                ) | (
                    TurnStartSuccessResponse::DurableAgent { .. },
                    TurnExecutionAuthority::Durable { .. }
                ) | (
                    TurnStartSuccessResponse::TurnStart
                        | TurnStartSuccessResponse::VoiceSessionFinalizeAccepted { .. },
                    TurnExecutionAuthority::Fresh(_)
                )
            );
            if !authority_matches_response {
                send_turn_start_failure!(TurnStartFailure::internal(
                    "CLI runtime response and execution authority variants differ",
                ));
                return;
            }

            let Some(runtime_config) = self
                .validate_cli_runtime_turn_start_backend(
                    connection_id,
                    request_id.clone(),
                    runtime_id.as_str(),
                    runtime_kind,
                    &success_response,
                    response_thread_id.as_str(),
                    response_turn_id.as_str(),
                )
                .await
            else {
                return;
            };
            params.model_provider = Some(cli_runtime_provider_key(runtime_id.as_str()));

            // Keep backend discovery, admission/preflight, and durable launch in
            // separate heap-backed futures. Combining this entire lifecycle in one
            // async state machine creates a poll frame large enough to exhaust a
            // standard Tokio worker stack before ordinary callees can run.
            let admission_phase = message_future(async {

            let Some(thread) = self
                .thread_manager
                .thread_get(params.thread_id.trim())
                .await
            else {
                send_turn_start_failure!(format!(
                    "thread `{}` is not loaded",
                    params.thread_id.trim()
                ));
                return None;
            };
            let normalized_capabilities = match self
                .normalize_turn_skill_capabilities(
                    thread.workspace_id.as_str(),
                    params.capabilities.as_slice(),
                )
                .await
            {
                Ok(normalized) => normalized,
                Err(message) => {
                    send_turn_start_failure!(message);
                    return None;
                }
            };
            if matches!(
                &execution_authority,
                TurnExecutionAuthority::Fresh(admission)
                    if admission.uses_scoped_collaboration_policy()
            )
                && let Err(message) = self
                    .enforce_scoped_skill_capability_projection(
                        thread.workspace_id.as_str(),
                        normalized_capabilities.execution.as_slice(),
                    )
                    .await
            {
                send_turn_start_failure!(message);
                return None;
            }
            let normalized_presentation_capabilities = normalized_capabilities.presentation;
            let normalized_pack_names = normalized_capabilities.pack_names;
            params.capabilities = normalized_capabilities.execution;
            if let Err(failure) = validate_root_agent_launch_capabilities(&params) {
                send_turn_start_failure!(failure);
                return None;
            }

            // Validate the semantic catalog projection before any provider-specific
            // preflight. This keeps malformed/missing capabilities in the typed
            // InvalidInput lane while storage/catalog failures remain Unavailable.
            if let Err(failure) = self
                .validate_turn_skill_capabilities(
                    thread.workspace_id.as_str(),
                    params.capabilities.as_slice(),
                )
                .await
            {
                send_turn_start_failure!(failure);
                return None;
            }

            let capability_partition =
                match crate::cli_runtime::skills::partition_cli_runtime_capabilities(
                    &params.capabilities,
                ) {
                    Ok(partition) => partition,
                    Err(message) => {
                        send_turn_start_failure!(message);
                        return None;
                    }
                };
            let requested_mcp = capability_partition.has_mcp();
            let provider_claim_matches = submitted_model_provider
                .as_deref()
                .map(str::trim)
                .filter(|provider| !provider.is_empty())
                .is_none_or(|provider| provider == cli_runtime_provider_key(runtime_id.as_str()));
            if let Err(rejection) =
                crate::cli_mcp_client_validation::validate_cli_mcp_client_request_durably(
                    &self.crud_store,
                    crate::cli_mcp_client_validation::CliMcpClientValidationAuditContext {
                        workspace_id: None,
                        thread_id: params.thread_id.as_str(),
                        turn_id: params.turn_id.as_str(),
                        runtime_id: runtime_id.as_str(),
                    },
                    crate::cli_mcp_client_validation::CliMcpClientValidationEvidence {
                        target: cli_mcp_client_target(runtime_kind),
                        has_mcp_projection: requested_mcp,
                        provider_claim_matches,
                        runtime_snapshot_current: true,
                        runtime_supports_mcp_tools: true,
                        projection_workspace_matches: true,
                        explicit_capabilities_resolved: true,
                    },
                )
                .await
            {
                send_turn_start_failure!(rejection.to_string());
                return None;
            }
            if let Some(input_kind) = params
                .input
                .iter()
                .find_map(cli_runtime_forbidden_input_kind)
            {
                send_turn_start_failure!(format!(
                    "CLI runtime providers only support text and attachment inputs; `{input_kind}` input is not supported"
                ));
                return None;
            }

            let Some(manager) = self.cli_runtime_manager.clone() else {
                send_turn_start_failure!(
                    "CLI runtime manager is not available for turn start".to_owned()
                );
                return None;
            };
            let continuation_thread_id = success_response
                .continuation_thread_id(thread.id.as_str())
                .to_owned();
            let context_thread_id = success_response
                .context_thread_id(thread.id.as_str())
                .to_owned();
            let session_key = match crate::cli_runtime::manager::CLIAgentRuntimeSessionKey::new(
                thread.workspace_id.as_str(),
                runtime_id.as_str(),
                continuation_thread_id.as_str(),
            ) {
                Ok(session_key) => session_key,
                Err(error) => {
                    send_turn_start_failure!(format!("invalid CLI runtime session key: {error:#}"));
                    return None;
                }
            };
            let session_turn_mutex = self.cli_runtime_session_turn_mutex(&session_key).await;
            let session_turn_lease = if success_response.is_task() {
                let lock = session_turn_mutex.lock_owned();
                tokio::pin!(lock);
                let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(5));
                loop {
                    tokio::select! {
                        lease = &mut lock => break lease,
                        _ = heartbeat.tick() => {
                            let Some((task_run_id, execution_id)) =
                                success_response.task_queue_identity()
                            else {
                                unreachable!("task response must have a queue identity");
                            };
                            match self
                                .keep_task_cli_runtime_queue_alive(task_run_id, execution_id)
                                .await
                            {
                                Ok(true) => {}
                                Ok(false) => {
                                    send_turn_start_failure!(
                                        "task run ended while waiting for its CLI runtime continuation"
                                            .to_owned()
                                    );
                                    return None;
                                }
                                Err(error) => {
                                    send_turn_start_failure!(format!(
                                        "failed to maintain queued task CLI runtime execution: {error:#}"
                                    ));
                                    return None;
                                }
                            }
                        }
                    }
                }
            } else {
                match session_turn_mutex.try_lock_owned() {
                    Ok(lease) => lease,
                    Err(_) => {
                        send_turn_start_failure!(format!(
                            "CLI runtime continuation `{}` already has an active turn",
                            session_key.thread_id
                        ));
                        return None;
                    }
                }
            };
            let mut blocker_poll_count = 0_u32;
            loop {
                match self
                    .cli_runtime_turn_start_blocker_for_thread(&session_key)
                    .await
                {
                    Ok(Some(_)) if success_response.is_task() => {
                        blocker_poll_count = blocker_poll_count.saturating_add(1);
                        if blocker_poll_count.is_multiple_of(50) {
                            let Some((task_run_id, execution_id)) =
                                success_response.task_queue_identity()
                            else {
                                unreachable!("task response must have a queue identity");
                            };
                            match self
                                .keep_task_cli_runtime_queue_alive(task_run_id, execution_id)
                                .await
                            {
                                Ok(true) => {}
                                Ok(false) => {
                                    send_turn_start_failure!(
                                        "task run ended while waiting for its CLI runtime continuation"
                                            .to_owned()
                                    );
                                    return None;
                                }
                                Err(error) => {
                                    send_turn_start_failure!(format!(
                                        "failed to maintain queued task CLI runtime execution: {error:#}"
                                    ));
                                    return None;
                                }
                            }
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                    Ok(Some(message)) => {
                        send_turn_start_failure!(message);
                        return None;
                    }
                    Ok(None) => break,
                    Err(error) => {
                        send_turn_start_failure!(format!(
                            "failed to check active CLI runtime turns: {error:#}"
                        ));
                        return None;
                    }
                }
            }
            if let Some((task_run_id, execution_id)) = success_response.task_queue_identity() {
                match self
                    .keep_task_cli_runtime_queue_alive(task_run_id, execution_id)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        send_turn_start_failure!(
                            "task run ended before its CLI runtime turn started".to_owned()
                        );
                        return None;
                    }
                    Err(error) => {
                        send_turn_start_failure!(format!(
                            "failed to validate task CLI runtime execution: {error:#}"
                        ));
                        return None;
                    }
                }
            }
            let PreparedCliRuntimeCombinedPreflight {
                plan: combined_preflight,
                codex_mcp_launch_projection,
                claude_mcp_launch_projection,
            } = match self
                .prepare_cli_runtime_combined_preflight(
                    &thread,
                    &params,
                    runtime_id.as_str(),
                    runtime_kind,
                    &runtime_config,
                    capability_partition,
                    requested_mcp,
                    provider_claim_matches,
                )
                .await
            {
                Ok(prepared) => prepared,
                Err(message) => {
                    send_turn_start_failure!(message);
                    return None;
                }
            };
            Some(CliRuntimeAdmissionPhase {
                thread,
                normalized_presentation_capabilities,
                normalized_pack_names,
                manager,
                continuation_thread_id,
                context_thread_id,
                session_key,
                session_turn_lease,
                combined_preflight,
                codex_mcp_launch_projection,
                claude_mcp_launch_projection,
            })
            })
            .await;
            let Some(CliRuntimeAdmissionPhase {
                thread,
                normalized_presentation_capabilities,
                normalized_pack_names,
                manager,
                continuation_thread_id,
                context_thread_id,
                session_key,
                session_turn_lease,
                combined_preflight,
                codex_mcp_launch_projection,
                claude_mcp_launch_projection,
            }) = admission_phase
            else {
                return;
            };
            let started_phase = message_future(async {
            let proxy_url = match self
                .prepare_cli_runtime_proxy_url(thread.workspace_id.as_str(), runtime_id.as_str())
                .await
            {
                Ok(proxy_url) => proxy_url,
                Err(error) => {
                    send_turn_start_failure!(format!(
                        "failed to prepare CLI runtime proxy settings: {error:#}"
                    ));
                    return None;
                }
            };
            // Detached Task execution prepares its hidden child before that
            // child is materialized in the durable thread table. Validate
            // user artifacts against the already-authorized context root in
            // that narrow preparation window; ordinary CLI turns validate
            // against their own thread. Once materialized, internal child
            // threads resolve to the same authorization root through durable
            // lineage.
            let artifact_authorization_thread_id =
                success_response.context_thread_id(thread.id.as_str());
            if let Err(error) = self
                .validate_turn_artifact_user_inputs(
                    thread.workspace_id.as_str(),
                    artifact_authorization_thread_id,
                    params.input.as_slice(),
                )
                .await
            {
                send_turn_start_failure!(format!(
                    "failed to validate CLI runtime artifact input: {error:#}"
                ));
                return None;
            }
            let resolved_artifacts = match self
                .resolve_provider_artifact_inputs(
                    thread.workspace_id.as_str(),
                    params.input.as_slice(),
                )
                .await
            {
                Ok(resolved_artifacts) => resolved_artifacts,
                Err(error) => {
                    send_turn_start_failure!(format!(
                        "failed to materialize CLI runtime artifact input: {error:#}"
                    ));
                    return None;
                }
            };
            let input_mapping = match match runtime_kind {
                CLIAgentRuntimeKind::Codex => {
                    crate::cli_runtime::input_mapping::map_codex_turn_input_from_pioneer(
                        params.input.as_slice(),
                        resolved_artifacts.as_slice(),
                    )
                }
                CLIAgentRuntimeKind::Claude => {
                    crate::cli_runtime::input_mapping::map_claude_turn_input_from_pioneer(
                        params.input.as_slice(),
                        resolved_artifacts.as_slice(),
                    )
                }
            } {
                Ok(input_mapping) => input_mapping,
                Err(error) => {
                    send_turn_start_failure!(format!("{error}"));
                    return None;
                }
            };
            let task_permission_selection =
                success_response.task_permission_profile().map(|profile| {
                    pioneer_protocol::TurnPermissionProfileSelection { mode: profile.mode }
                });
            if let Err(error) =
                super::message_turn::normalize_turn_collaboration_params(&mut params)
            {
                send_turn_start_failure!(format!("invalid Turn collaboration metadata: {error}"));
                return None;
            }
            let resolved_permission_profile = match &execution_authority {
                TurnExecutionAuthority::Fresh(admission)
                    if admission.uses_scoped_collaboration_policy() =>
                {
                    let requested = pioneer_protocol::resolve_turn_permission_profile(
                        params.permission_profile.as_ref(),
                    );
                    Some(admission.cap_permission_profile(&requested))
                }
                TurnExecutionAuthority::Fresh(_) | TurnExecutionAuthority::Durable { .. } => None,
            };
            if let Some(profile) = resolved_permission_profile.as_ref() {
                params.permission_profile =
                    Some(pioneer_protocol::TurnPermissionProfileSelection { mode: profile.mode });
            }
            let permission_adapter =
                crate::cli_runtime::permissions::adapt_cli_runtime_permissions_for_turn(
                    runtime_kind,
                    task_permission_selection
                        .as_ref()
                        .or(params.permission_profile.as_ref()),
                    params.cli_runtime_options.take(),
                );
            debug!(
                runtime_id = runtime_id.as_str(),
                runtime_kind = cli_runtime_protocol_kind_label(runtime_kind),
                pioneer_permission_mode = permission_adapter.output.profile.mode.as_str(),
                provider_permission_mode = permission_adapter.output.provider_mode_label.as_str(),
                mapping_quality = ?permission_adapter.output.mapping_quality,
                notes = ?permission_adapter.output.notes,
                "adapted Pioneer turn permission profile for CLI runtime"
            );
            params.cli_runtime_options = Some(permission_adapter.options.clone());
            let effective_approval_policy = permission_adapter.output.approval_policy.clone();
            let sandbox_policy_value: Option<JsonValue> = None;
            let provider_permissions_id: Option<String> = None;
            let requested_reasoning_effort = requested_reasoning_effort(&params);
            let cli_runtime_effort = cli_runtime_effort(&params);
            // Transition rule: CLI turns may carry the legacy runtime effort, the
            // top-level reasoning effort, or both when they agree. New clients use
            // the top-level field; the native runtime still receives one value.
            let effective_cli_runtime_effort = match effective_cli_runtime_effort(
                requested_reasoning_effort.as_deref(),
                cli_runtime_effort.as_deref(),
            ) {
                Ok(effort) => effort,
                Err(message) => {
                    send_turn_start_failure!(message);
                    return None;
                }
            };
            let cli_runtime_personality = params
                .cli_runtime_options
                .as_ref()
                .and_then(|options| options.personality.clone());
            let cli_runtime_summary = params
                .cli_runtime_options
                .as_ref()
                .and_then(|options| options.summary.clone());

            let security_params = params.clone();
            #[cfg(test)]
            self.cli_runtime_skill_preflight_test_events
                .lock()
                .await
                .push("thread_manager_turn_start".to_owned());
            let author = match super::message_turn::resolve_turn_author_snapshot(
                self.crud_store.as_ref(),
                &request_actor,
            )
            .await
            {
                Ok(author) => author,
                Err(error) => {
                    send_turn_start_failure!(format!("failed to resolve Turn author: {error:#}"));
                    return None;
                }
            };
            let mentions = match super::message_turn::resolve_turn_collaboration_metadata(
                self.crud_store.as_ref(),
                &request_actor,
                &params,
            )
            .await
            {
                Ok(mentions) => mentions,
                Err(error) => {
                    send_turn_start_failure!(format!(
                        "invalid Turn collaboration metadata: {error}"
                    ));
                    return None;
                }
            };
            let outcome_result = if let Some(permission_profile) =
                success_response.task_permission_profile()
            {
                if let Some(agent_author) = success_response.task_agent_author() {
                    self.thread_manager
                        .agent_turn_start_with_permission_profile(
                            params,
                            permission_profile,
                            agent_author,
                        )
                        .await
                } else {
                    self.thread_manager
                        .system_turn_start_with_permission_profile(params, permission_profile)
                        .await
                }
            } else if let Some(permission_profile) = resolved_permission_profile {
                self.thread_manager
                    .turn_start_with_user_metadata_and_permission_profile(
                        connection_id,
                        params,
                        permission_profile,
                        author,
                        mentions,
                    )
                    .await
            } else {
                self.thread_manager
                    .turn_start_with_user_metadata(connection_id, params, author, mentions)
                    .await
            };
            let outcome = match outcome_result {
                Ok(outcome) => outcome,
                Err(error) => {
                    send_turn_start_failure!(format!(
                        "failed to start CLI runtime turn: {error:#}"
                    ));
                    return None;
                }
            };
            if let Err(message) = self
                .validate_turn_skill_capabilities(
                    outcome.started_notification.workspace_id.as_str(),
                    outcome.materialization.capabilities.as_slice(),
                )
                .await
            {
                self.thread_manager
                    .rollback_turn_start(outcome.rollback_context.clone())
                    .await;
                send_turn_start_failure!(message);
                return None;
            }
            let user_message_capability_attachments =
                match super::agent_runtime::user_message_attachments_from_capabilities_and_bindings(
                    normalized_presentation_capabilities.as_slice(),
                    combined_preflight.skill_bindings.as_slice(),
                    &normalized_pack_names,
                ) {
                    Ok(attachments) => attachments,
                    Err(error) => {
                        self.thread_manager
                            .rollback_turn_start(outcome.rollback_context.clone())
                            .await;
                        send_turn_start_failure!(format!(
                            "failed to snapshot selected skill presentation: {error:#}"
                        ));
                        return None;
                    }
                };
            let effective_cli_runtime_effort = match self
                .resolve_turn_reasoning_effort(
                    outcome.started_notification.workspace_id.as_str(),
                    ReasoningModelLookupBackend::CliRuntime {
                        runtime_id: runtime_id.as_str(),
                        runtime_kind,
                    },
                    outcome.materialization.thread.model.as_str(),
                    effective_cli_runtime_effort.as_deref(),
                )
                .await
            {
                Ok(effort) => effort,
                Err(message) => {
                    self.thread_manager
                        .rollback_turn_start(outcome.rollback_context.clone())
                        .await;
                    send_turn_start_failure!(message);
                    return None;
                }
            };
            Some(CliRuntimeStartedPhase {
                outcome,
                user_message_capability_attachments,
                proxy_url,
                input_mapping,
                effective_approval_policy,
                sandbox_policy_value,
                provider_permissions_id,
                effective_cli_runtime_effort,
                cli_runtime_personality,
                cli_runtime_summary,
                security_params,
            })
            })
            .await;
            let Some(CliRuntimeStartedPhase {
                outcome,
                user_message_capability_attachments,
                proxy_url,
                mut input_mapping,
                effective_approval_policy,
                sandbox_policy_value,
                mut provider_permissions_id,
                effective_cli_runtime_effort,
                cli_runtime_personality,
                cli_runtime_summary,
                security_params,
            }) = started_phase
            else {
                return;
            };
            let materialized_phase = message_future(async {
            let mut installed_skills =
                Vec::with_capacity(combined_preflight.skill_install_plans.len());
            if let TurnExecutionAuthority::Fresh(admission) = &mut execution_authority
                && let Err(message) = self
                    .admit_composite_execution_request(
                        admission,
                        admission_entry_point,
                        Vec::new(),
                        outcome.started_notification.workspace_id.as_str(),
                        outcome.started_notification.thread_id.as_str(),
                        outcome.materialization.thread.model_provider.as_str(),
                        outcome.materialization.thread.model.as_str(),
                        &security_params,
                        outcome.materialization.capabilities.as_slice(),
                    )
                    .await
            {
                self.thread_manager
                    .rollback_turn_start(outcome.rollback_context.clone())
                    .await;
                send_turn_start_failure!(message);
                return None;
            }
            let profile_selected_audit = match self.turn_profile_selected_audit_event(&outcome) {
                Ok(event) => event,
                Err(error) => {
                    self.thread_manager
                        .rollback_turn_start(outcome.rollback_context.clone())
                        .await;
                    send_turn_start_failure!(format!(
                        "failed to resolve turn permission profile: {error:#}"
                    ));
                    return None;
                }
            };
            let security_snapshot = match self
                .resolve_turn_execution_security_snapshot(
                    &security_params,
                    &outcome,
                    success_response.task_execution_security_snapshot(),
                    execution_authority.source(),
                )
                .await
            {
                Ok(snapshot) => snapshot,
                Err(failure) => {
                    self.thread_manager
                        .rollback_turn_start(outcome.rollback_context.clone())
                        .await;
                    send_turn_start_failure!(failure);
                    return None;
                }
            };
            let security_audit_events = self.turn_security_audit_events_for_turn(
                outcome.started_notification.workspace_id.as_str(),
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.turn.id.as_str(),
                &security_snapshot,
            );
            let root_agent_admission = match &execution_authority {
                TurnExecutionAuthority::Fresh(admission)
                    if outcome.materialization.turn.mode
                        != pioneer_protocol::ThreadMode::Message
                        && outcome.materialization.thread.id == admission.root_thread_id() =>
                {
                    Some(admission)
                }
                _ => None,
            };
            let prepared_root_execution = match root_agent_admission {
                Some(admission) => {
                    let authority = match admission.finalize(
                        outcome.materialization.thread.workspace_id.as_str(),
                        outcome.materialization.thread.id.as_str(),
                        outcome.materialization.thread.model_provider.as_str(),
                        outcome.materialization.thread.model.as_str(),
                        security_params.execution_backend.as_ref(),
                        outcome.materialization.capabilities.as_slice(),
                        &security_snapshot.permission_profile,
                    ) {
                        Ok(authority) => authority,
                        Err(error) => {
                            self.thread_manager
                                .rollback_turn_start(outcome.rollback_context.clone())
                                .await;
                            send_turn_start_failure!(format!(
                                "failed to finalize root CLI Agent authority: {error:#}"
                            ));
                            return None;
                        }
                    };
                    match super::agent_action_tools::prepare_root_agent_execution_admission(
                        self,
                        &pioneer_agent::TurnToolContext {
                            workspace_id: outcome.materialization.thread.workspace_id.clone(),
                            thread_id: outcome.materialization.thread.id.clone(),
                            turn_id: outcome.materialization.turn.id.clone(),
                        },
                        &outcome.materialization.thread,
                        &authority,
                        admission.runtime_draft().map(|draft| draft.access()),
                        security_params.agent_launch.as_ref(),
                        security_params.execution_backend.as_ref(),
                        None,
                    )
                    .await
                    {
                        Ok(prepared) => Some(prepared),
                        Err(error) => {
                            self.thread_manager
                                .rollback_turn_start(outcome.rollback_context.clone())
                                .await;
                            send_turn_start_failure!(format!(
                                "failed to admit root CLI Agent execution: {error:#}"
                            ));
                            return None;
                        }
                    }
                }
                _ => None,
            };
            let materialization_authority = execution_authority.clone();
            let materialization_params = security_params.clone();
            let materialization_security_snapshot = security_snapshot.clone();
            let agent_turn_response = success_response.agent_turn_response();
            let execution_graph = prepared_root_execution
                .as_ref()
                .map(|prepared| prepared.graph.clone());
            let materialization_result = {
                let crud_store = self.crud_store.clone();
                let provider_registry = self.provider_registry.clone();
                let execution_owner_id = self.turn_execution_owner_id.clone();
                let materialization = outcome.materialization.clone();
                let effective_reasoning_effort = effective_cli_runtime_effort.clone();
                let workflow = message_future(async move {
                    persist_admitted_turn_start(
                        crud_store.as_ref(),
                        provider_registry.as_ref(),
                        execution_owner_id.as_ref(),
                        &materialization_params,
                        &materialization,
                        effective_reasoning_effort.as_deref(),
                        request_actor,
                        profile_selected_audit,
                        materialization_authority.source(),
                        None,
                        &materialization_security_snapshot,
                        security_audit_events,
                        execution_graph,
                        agent_turn_response,
                    )
                    .await
                });
                message_fresh_task(workflow).await
            };
            let materialization_result = match materialization_result {
                Ok(result) => result,
                Err(error) => Err(anyhow::anyhow!(
                    "CLI runtime turn/start materialization task failed: {error}"
                )),
            };
            let graph_result = match materialization_result {
                Ok(result) => result,
                Err(error) => {
                    self.thread_manager
                        .rollback_turn_start(outcome.rollback_context.clone())
                        .await;

                    send_turn_start_failure!(format!(
                        "failed to persist CLI runtime turn/start state and permission audit: {error:#}"
                    ));
                    return None;
                }
            };
            if graph_result.as_ref().is_some_and(|result| result.queued) {
                send_turn_start_failure!("root CLI Agent execution was unexpectedly queued");
                return None;
            }
            if let Some(prepared) = prepared_root_execution {
                if let Err(error) =
                    super::agent_action_tools::register_prepared_root_agent_action_binding(
                        self,
                        &pioneer_agent::TurnToolContext {
                            workspace_id: outcome.materialization.thread.workspace_id.clone(),
                            thread_id: outcome.materialization.thread.id.clone(),
                            turn_id: outcome.materialization.turn.id.clone(),
                        },
                        prepared,
                    )
                    .await
                {
                    self.mark_turn_blocked(
                        outcome.materialization.thread.id.clone(),
                        outcome.materialization.turn.id.clone(),
                        format!("failed to bind admitted root CLI Agent execution: {error:#}"),
                    )
                    .await;
                    send_turn_start_failure!(format!(
                        "failed to bind admitted root CLI Agent execution: {error:#}"
                    ));
                    return None;
                }
            }
            self.complete_runtime_draft_materialization(execution_authority.source())
                .await;
            if let Err(error) = self
                .register_execution_lease(outcome.started_notification.turn.id.as_str())
                .await
            {
                let message = format!("failed to register execution lease: {error:#}");
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    message.clone(),
                )
                .await;
                send_turn_start_failure!(TurnStartFailure::internal(message));
                return None;
            }
            let native_event_budget = match execution_authority
                .source()
                .effective_native_event_resource_budget()
            {
                Ok(budget) => budget,
                Err(error) => {
                    let message = format!(
                        "failed to resolve CLI native event resource policy: {error:#}"
                    );
                    self.mark_turn_blocked(
                        outcome.started_notification.thread_id.clone(),
                        outcome.started_notification.turn.id.clone(),
                        message.clone(),
                    )
                    .await;
                    send_turn_start_failure!(message);
                    return None;
                }
            };
            if let Some(projection) = combined_preflight.mcp_projection.as_ref() {
                let provider_bindings = codex_mcp_launch_projection
                    .as_ref()
                    .map(|launch| {
                        launch
                            .preflight
                            .tools
                            .iter()
                            .map(|tool| {
                                crate::turn_mcp::persistence::TurnMcpProviderBindingIdentity {
                                    canonical_callable_name: tool.canonical_callable_name.clone(),
                                    provider_callable_name: format!(
                                        "mcp__pioneer__{}",
                                        tool.canonical_callable_name
                                    ),
                                    provider_schema_fingerprint: tool
                                        .transformed_schema_fingerprint
                                        .clone(),
                                }
                            })
                            .collect::<Vec<_>>()
                    })
                    .or_else(|| {
                        claude_mcp_launch_projection.as_ref().map(|launch| {
                            launch
                                .preflight
                                .tools
                                .iter()
                                .map(|tool| {
                                    crate::turn_mcp::persistence::TurnMcpProviderBindingIdentity {
                                        canonical_callable_name: tool
                                            .canonical_callable_name
                                            .clone(),
                                        provider_callable_name: format!(
                                            "mcp__pioneer__{}",
                                            tool.canonical_callable_name
                                        ),
                                        provider_schema_fingerprint: tool
                                            .transformed_schema_fingerprint
                                            .clone(),
                                    }
                                })
                                .collect::<Vec<_>>()
                        })
                    })
                    .unwrap_or_default();
                let persisted = match self
                    .mcp_service
                    .persist_cli_resolved_mcp_turn_projection(projection, &provider_bindings)
                    .await
                {
                    Ok(persisted) => persisted,
                    Err(error) => {
                        self.mark_turn_blocked(
                            outcome.started_notification.thread_id.clone(),
                            outcome.started_notification.turn.id.clone(),
                            format!(
                                "failed to persist resolved MCP projection before CLI provider start: {error}"
                            ),
                        )
                        .await;
                        send_turn_start_failure!(format!(
                            "failed to persist resolved MCP projection: {error}"
                        ));
                        return None;
                    }
                };
                if persisted.turn_id != outcome.started_notification.turn.id
                    || persisted.manifest_hash != projection.manifest_hash
                    || persisted.tool_count != provider_bindings.len().max(projection.tools.len())
                {
                    self.mark_turn_blocked(
                        outcome.started_notification.thread_id.clone(),
                        outcome.started_notification.turn.id.clone(),
                        "persisted MCP projection acknowledgement did not match CLI preflight"
                            .to_owned(),
                    )
                    .await;
                    send_turn_start_failure!(
                        "persisted MCP projection acknowledgement did not match CLI preflight"
                            .to_owned()
                    );
                    return None;
                }

                let event = AgentDurableEvent::TurnCapabilitiesResolved {
                    thread_id: outcome.started_notification.thread_id.clone(),
                    turn_id: outcome.started_notification.turn.id.clone(),
                    accepted: projection.accepted_capabilities.clone(),
                    rejected: projection.rejected_capabilities.clone(),
                    mcp_bindings: projection
                        .tools
                        .iter()
                        .map(|tool| pioneer_protocol::McpTurnBindingSummary {
                            server_installation_id: tool.server_installation_id.clone(),
                            server_name: tool.server_name.clone(),
                            raw_tool_name: tool.raw_tool_name.clone(),
                            callable_name: tool.canonical_callable_name.clone(),
                            catalog_version: tool.catalog_version.clone(),
                            fingerprint: tool.installation_fingerprint.clone(),
                            selection_reason: tool
                                .selection_reason
                                .legacy_binding_value()
                                .to_owned(),
                            capability_id: tool.capability_id.clone(),
                        })
                        .collect(),
                };
                if !self.handle_durable_agent_event(event).await {
                    self.mark_turn_blocked(
                        outcome.started_notification.thread_id.clone(),
                        outcome.started_notification.turn.id.clone(),
                        "failed to emit durable CLI MCP capability result".to_owned(),
                    )
                    .await;
                    send_turn_start_failure!(
                        "failed to emit durable CLI MCP capability result".to_owned()
                    );
                    return None;
                }
            }
            let event = AgentDurableEvent::TurnSkillsResolved {
                thread_id: outcome.started_notification.thread_id.clone(),
                turn_id: outcome.started_notification.turn.id.clone(),
                bindings: combined_preflight.skill_bindings.clone(),
            };
            if !self.handle_durable_agent_event(event).await {
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    "failed to commit CLI runtime turn skill bindings".to_owned(),
                )
                .await;
                send_turn_start_failure!(
                    "failed to commit CLI runtime turn skill bindings".to_owned()
                );
                return None;
            }

            for plan in &combined_preflight.skill_install_plans {
                let install_started = std::time::Instant::now();
                match self.install_one_cli_runtime_skill(plan).await {
                    Ok(result) => {
                        let result_name = match result.status {
                            crate::cli_runtime::skills::CliRuntimeSkillInstallStatus::Current => {
                                "current"
                            }
                            crate::cli_runtime::skills::CliRuntimeSkillInstallStatus::Installed => {
                                "installed"
                            }
                            crate::cli_runtime::skills::CliRuntimeSkillInstallStatus::Updated => {
                                "updated"
                            }
                        };
                        let source_hash_prefix =
                            &result.source_folder_hash[..result.source_folder_hash.len().min(12)];
                        info!(
                            event = "cli_runtime_skill_preflight",
                            runtime_id = plan.runtime_id.as_str(),
                            runtime_kind = plan.runtime_kind.as_str(),
                            skill_slug = plan.skill_slug.as_str(),
                            source_kind = plan.source_kind.as_str(),
                            install_name = plan.install_name.as_str(),
                            destination = %plan.destination.display(),
                            result = result_name,
                            source_hash_prefix,
                            elapsed_ms = install_started.elapsed().as_millis(),
                            "CLI runtime skill preflight completed after MCP projection persistence"
                        );
                        installed_skills.push(result);
                    }
                    Err(error) => {
                        let failure_reason = format!(
                            "failed to prepare CLI runtime skill `{}`: {error:#}",
                            plan.skill_slug
                        );
                        warn!(
                            event = "cli_runtime_skill_preflight",
                            runtime_id = plan.runtime_id.as_str(),
                            runtime_kind = plan.runtime_kind.as_str(),
                            skill_slug = plan.skill_slug.as_str(),
                            source_kind = plan.source_kind.as_str(),
                            install_name = plan.install_name.as_str(),
                            destination = %plan.destination.display(),
                            result = "failed",
                            failure_reason = failure_reason.as_str(),
                            elapsed_ms = install_started.elapsed().as_millis(),
                            "CLI runtime skill preflight failed"
                        );
                        self.mark_turn_blocked(
                            outcome.started_notification.thread_id.clone(),
                            outcome.started_notification.turn.id.clone(),
                            failure_reason.clone(),
                        )
                        .await;
                        send_turn_start_failure!(failure_reason);
                        return None;
                    }
                }
            }
            #[cfg(test)]
            self.cli_runtime_skill_preflight_test_events
                .lock()
                .await
                .push("preflight_complete".to_owned());
            if let Some(cutoff_ms) = installed_skills
                .iter()
                .map(|skill| skill.receipt_updated_at_unix_ms)
                .max()
                && let Err(error) = manager
                    .close_session_if_started_at_or_before(&session_key, cutoff_ms)
                    .await
            {
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    format!("failed to refresh CLI runtime session for selected skills: {error:#}"),
                )
                .await;
                send_turn_start_failure!(format!(
                    "failed to refresh CLI runtime session for selected skills: {error:#}"
                ));
                return None;
            }
            Some(CliRuntimeMaterializedPhase {
                security_snapshot,
                installed_skills,
                native_event_budget,
            })
            })
            .await;
            let Some(CliRuntimeMaterializedPhase {
                security_snapshot,
                installed_skills,
                native_event_budget,
            }) = materialized_phase
            else {
                return;
            };
            message_future(async move {
            if matches!(runtime_kind, CLIAgentRuntimeKind::Codex) {
                crate::cli_runtime::skills::prepend_codex_installed_skill_items(
                    &installed_skills,
                    &mut input_mapping,
                );
            }
            if runtime_kind == CLIAgentRuntimeKind::Codex {
                provider_permissions_id = Some(
                    crate::cli_runtime::permissions::codex_permissions_profile_for_security_snapshot(
                        &security_snapshot,
                    )
                    .to_owned(),
                );
            }
            let sandbox_json = match sandbox_policy_value.as_ref() {
                Some(sandbox_policy) => {
                    match pioneer_crud::serialize_cli_runtime_json(sandbox_policy) {
                        Ok(sandbox_json) => Some(sandbox_json),
                        Err(error) => {
                            let message = format!(
                                "failed to serialize CLI runtime sandbox policy: {error:#}"
                            );
                            self.mark_turn_blocked(
                                outcome.started_notification.thread_id.clone(),
                                outcome.started_notification.turn.id.clone(),
                                message.clone(),
                            )
                            .await;
                            send_turn_start_failure!(message);
                            return;
                        }
                    }
                }
                None => None,
            };
            self.ensure_hook_runtime_with_run_store().await;
            let selected_skill_names = installed_skills
                .iter()
                .map(|skill| skill.install_name.clone())
                .collect::<Vec<_>>();
            let delivery_plan = match self
                .compile_cli_runtime_delivery_plan_for_turn(
                    runtime_id.as_str(),
                    runtime_kind,
                    &outcome,
                    continuation_thread_id.as_str(),
                    context_thread_id.as_str(),
                    combined_preflight.mcp_projection.as_ref(),
                    selected_skill_names.as_slice(),
                    success_response.task_conversation_history(),
                )
                .await
            {
                Ok(delivery_plan) => delivery_plan,
                Err(error) => {
                    self.mark_turn_blocked(
                        outcome.started_notification.thread_id.clone(),
                        outcome.started_notification.turn.id.clone(),
                        format!("failed to compile CLI runtime delivery plan: {error:#}"),
                    )
                    .await;
                    send_turn_start_failure!(format!(
                        "failed to compile CLI runtime delivery plan: {error:#}"
                    ));
                    return;
                }
            };
            crate::cli_runtime::context::prepend_cli_turn_context_input(
                &mut input_mapping,
                &delivery_plan,
                cli_runtime_context_label(runtime_kind),
            );
            let elevated_instructions = match pioneer_cli_agent_runtime::instructions::CLIRuntimeElevatedInstructions::try_new(
                delivery_plan.provider_instructions.text.clone(),
                delivery_plan.provider_instructions.fingerprint.clone(),
            ) {
                Ok(instructions) => instructions,
                Err(error) => {
                    self.mark_turn_blocked(
                        outcome.started_notification.thread_id.clone(),
                        outcome.started_notification.turn.id.clone(),
                        format!("failed to project CLI elevated instructions: {error:#}"),
                    )
                    .await;
                    send_turn_start_failure!(format!(
                        "failed to project CLI elevated instructions: {error:#}"
                    ));
                    return;
                }
            };
            if let Err(error) =
                crate::cli_runtime::instruction_projection::persist_cli_runtime_instruction_projection(
                    self.crud_store.as_ref(),
                    outcome.started_notification.turn.id.as_str(),
                    runtime_kind,
                    &delivery_plan,
                    &elevated_instructions,
                )
                .await
            {
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    format!("failed to persist CLI elevated instructions: {error:#}"),
                )
                .await;
                send_turn_start_failure!(format!(
                    "failed to persist CLI elevated instructions: {error:#}"
                ));
                return;
            }
            let execution_authorization_context = match self
                .load_turn_execution_authorization_context(
                    outcome.started_notification.turn.id.as_str(),
                )
                .await
            {
                Ok(context) => context,
                Err(error) => {
                    self.mark_turn_blocked(
                        outcome.started_notification.thread_id.clone(),
                        outcome.started_notification.turn.id.clone(),
                        format!(
                            "failed to bind CLI runtime session authorization scope: {error:#}"
                        ),
                    )
                    .await;
                    send_turn_start_failure!(format!(
                        "failed to bind CLI runtime session authorization scope: {error:#}"
                    ));
                    return;
                }
            };
            let authorization_scope_fingerprint =
                match cli_runtime_session_authorization_scope_fingerprint(
                    &execution_authorization_context,
                    &security_snapshot,
                ) {
                    Ok(fingerprint) => fingerprint,
                    Err(error) => {
                        self.mark_turn_blocked(
                            outcome.started_notification.thread_id.clone(),
                            outcome.started_notification.turn.id.clone(),
                            format!(
                                "failed to fingerprint CLI runtime session authorization scope: {error:#}"
                            ),
                        )
                        .await;
                        send_turn_start_failure!(format!(
                            "failed to fingerprint CLI runtime session authorization scope: {error:#}"
                        ));
                        return;
                    }
                };
            let native_cwd = security_snapshot.sandbox.cwd.clone();
            let proxy_env = crate::cli_runtime::config::proxy_env(proxy_url.as_deref());
            let session_options = crate::cli_runtime::manager::CLIAgentRuntimeSessionStartOptions {
                cwd: Some(std::path::PathBuf::from(native_cwd.as_str())),
                approval_policy: Some(effective_approval_policy.clone()),
                authorization_scope_fingerprint: Some(authorization_scope_fingerprint),
                env: proxy_env,
                selected_skills: if matches!(runtime_kind, CLIAgentRuntimeKind::Claude) {
                    installed_skills
                        .iter()
                        .map(crate::cli_runtime::skills::CliRuntimeSelectedSkill::from)
                        .collect()
                } else {
                    Vec::new()
                },
                elevated_instructions: matches!(runtime_kind, CLIAgentRuntimeKind::Claude)
                    .then_some(elevated_instructions.clone()),
                ..Default::default()
            };
            let session_result = if runtime_kind == CLIAgentRuntimeKind::Codex {
                let persisted_binding = match self
                    .crud_store
                    .get_cli_runtime_thread_binding(continuation_thread_id.as_str())
                    .await
                {
                    Ok(binding) => binding,
                    Err(error) => {
                        self.mark_turn_blocked(
                            outcome.started_notification.thread_id.clone(),
                            outcome.started_notification.turn.id.clone(),
                            format!("failed to load Codex continuation binding: {error:#}"),
                        )
                        .await;
                        send_turn_start_failure!(format!(
                            "failed to load Codex continuation binding: {error:#}"
                        ));
                        return;
                    }
                };
                let native_thread_id = match persisted_binding {
                    Some(binding)
                        if binding.workspace_id == outcome.started_notification.workspace_id
                            && binding.runtime_id == runtime_id
                            && binding.runtime_kind
                                == cli_runtime_protocol_kind_label(runtime_kind) =>
                    {
                        Some(binding.native_thread_id)
                    }
                    Some(_) => {
                        self.mark_turn_blocked(
                            outcome.started_notification.thread_id.clone(),
                            outcome.started_notification.turn.id.clone(),
                            "persisted Codex continuation binding does not match the requested runtime"
                                .to_owned(),
                        )
                        .await;
                        send_turn_start_failure!(
                            "persisted Codex continuation binding does not match the requested runtime"
                                .to_owned()
                        );
                        return;
                    }
                    None => None,
                };
                manager
                    .get_or_start_with_launch_spec(
                        session_key.clone(),
                        crate::cli_runtime::continuation::CliSessionLaunchSpec::codex(
                            session_options,
                            codex_mcp_launch_projection
                                .clone()
                                .map(crate::cli_runtime::continuation::CliMcpSessionLaunch::Codex)
                                .unwrap_or(
                                    crate::cli_runtime::continuation::CliMcpSessionLaunch::Disabled,
                                ),
                            native_thread_id,
                        )
                        .with_native_event_budget(native_event_budget),
                    )
                    .await
            } else {
                let continuation =
                    match crate::cli_runtime::thread_binding::prepare_claude_provider_session(
                        self.crud_store.as_ref(),
                        crate::cli_runtime::thread_binding::ClaudeProviderSessionPrepareRequest {
                            workspace_id: outcome.started_notification.workspace_id.clone(),
                            thread_id: continuation_thread_id.clone(),
                            runtime_id: runtime_id.clone(),
                            cwd: native_cwd.clone(),
                            model: Some(outcome.materialization.thread.model.clone()),
                            prepared_at: chrono::Utc::now().fixed_offset(),
                        },
                    )
                    .await
                    {
                        Ok(continuation) => continuation,
                        Err(error) => {
                            self.mark_turn_blocked(
                                outcome.started_notification.thread_id.clone(),
                                outcome.started_notification.turn.id.clone(),
                                format!("failed to prepare Claude provider session: {error:#}"),
                            )
                            .await;
                            send_turn_start_failure!(format!(
                                "failed to prepare Claude provider session: {error:#}"
                            ));
                            return;
                        }
                    };
                let launch_spec = match continuation {
                    crate::cli_runtime::continuation::CliProviderContinuation::ClaudeNew {
                        provider_session_id,
                    } => crate::cli_runtime::continuation::CliSessionLaunchSpec::claude_new(
                        session_options,
                        claude_mcp_launch_projection
                            .clone()
                            .map(crate::cli_runtime::continuation::CliMcpSessionLaunch::Claude)
                            .unwrap_or(
                                crate::cli_runtime::continuation::CliMcpSessionLaunch::Disabled,
                            ),
                        provider_session_id,
                    )
                    .with_native_event_budget(native_event_budget),
                    crate::cli_runtime::continuation::CliProviderContinuation::ClaudeResume {
                        provider_session_id,
                    } => crate::cli_runtime::continuation::CliSessionLaunchSpec::claude_resume(
                        session_options,
                        claude_mcp_launch_projection
                            .clone()
                            .map(crate::cli_runtime::continuation::CliMcpSessionLaunch::Claude)
                            .unwrap_or(
                                crate::cli_runtime::continuation::CliMcpSessionLaunch::Disabled,
                            ),
                        provider_session_id,
                    )
                    .with_native_event_budget(native_event_budget),
                    crate::cli_runtime::continuation::CliProviderContinuation::CodexRpcThread {
                        ..
                    } => unreachable!("Claude preparation returned a Codex continuation"),
                };
                manager
                    .get_or_start_with_launch_spec(session_key.clone(), launch_spec)
                    .await
            };
            let session_handle = match session_result {
                Ok(handle) => handle,
                Err(error) => {
                    self.mark_turn_blocked(
                        outcome.started_notification.thread_id.clone(),
                        outcome.started_notification.turn.id.clone(),
                        format!("failed to start CLI runtime session: {error:#}"),
                    )
                    .await;
                    send_turn_start_failure!(format!(
                        "failed to start CLI runtime session: {error:#}"
                    ));
                    return;
                }
            };
            let cli_session = session_handle.session();
            self.ensure_cli_runtime_session_event_pumps(
                session_handle.instance(),
                cli_session.clone(),
                runtime_config.debug_native_events,
            )
            .await;
            let thread_sandbox_label = sandbox_policy_value.as_ref().map(|sandbox_policy| {
                serde_json::json!(cli_runtime_thread_sandbox_label(sandbox_policy))
            });
            let native_thread =
                match crate::cli_runtime::thread_binding::open_cli_runtime_thread_binding(
                    self.crud_store.as_ref(),
                    &cli_session,
                    crate::cli_runtime::thread_binding::CLIAgentRuntimeThreadBindingOpenRequest {
                        workspace_id: outcome.started_notification.workspace_id.clone(),
                        thread_id: continuation_thread_id.clone(),
                        runtime_id: runtime_id.clone(),
                        runtime_kind: cli_runtime_protocol_kind_label(runtime_kind).to_owned(),
                        cwd: native_cwd,
                        model: Some(outcome.materialization.thread.model.clone()),
                        approval_policy: Some(effective_approval_policy.clone()),
                        sandbox: thread_sandbox_label,
                        permissions: provider_permissions_id.clone(),
                        service_tier: None,
                        resume_existing: cli_runtime_supports_durable_thread_resume(runtime_kind),
                        request_timeout: std::time::Duration::from_millis(
                            runtime_config.request_timeout_ms,
                        ),
                        opened_at: chrono::Utc::now().fixed_offset(),
                    },
                )
                .await
                {
                    Ok(opened) => opened,
                    Err(error) => {
                        self.mark_turn_blocked(
                            outcome.started_notification.thread_id.clone(),
                            outcome.started_notification.turn.id.clone(),
                            format!("failed to open CLI runtime thread: {error:#}"),
                        )
                        .await;
                        send_turn_start_failure!(format!(
                            "failed to open CLI runtime thread: {error:#}"
                        ));
                        return;
                    }
                };
            let input_mapping_json = match pioneer_crud::serialize_cli_runtime_json(&input_mapping)
            {
                Ok(input_mapping_json) => input_mapping_json,
                Err(error) => {
                    self.mark_turn_blocked(
                        outcome.started_notification.thread_id.clone(),
                        outcome.started_notification.turn.id.clone(),
                        format!("failed to serialize CLI runtime input mapping: {error:#}"),
                    )
                    .await;
                    send_turn_start_failure!(format!(
                        "failed to serialize CLI runtime input mapping: {error:#}"
                    ));
                    return;
                }
            };
            if let Err(error) = self
                .persist_cli_runtime_input_mapping_if_thread_bound(
                    runtime_id.as_str(),
                    runtime_kind,
                    continuation_thread_id.as_str(),
                    input_mapping_json,
                    sandbox_json,
                    Some(effective_approval_policy.clone()),
                    &outcome,
                )
                .await
            {
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    format!("failed to persist CLI runtime input mapping: {error:#}"),
                )
                .await;
                send_turn_start_failure!(format!(
                    "failed to persist CLI runtime input mapping: {error:#}"
                ));
                return;
            }
            let native_turn_input = match serde_json::to_value(&input_mapping.input) {
                Ok(input) => input,
                Err(error) => {
                    self.mark_turn_blocked(
                        outcome.started_notification.thread_id.clone(),
                        outcome.started_notification.turn.id.clone(),
                        format!("failed to encode CLI runtime turn input: {error:#}"),
                    )
                    .await;
                    send_turn_start_failure!(format!(
                        "failed to encode CLI runtime turn input: {error:#}"
                    ));
                    return;
                }
            };
            if let Err(error) = self
                .persist_cli_runtime_prompt_manifest(
                    outcome.started_notification.thread_id.as_str(),
                    outcome.started_notification.turn.id.as_str(),
                    &delivery_plan,
                )
                .await
            {
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    format!("failed to persist CLI runtime prompt manifest: {error:#}"),
                )
                .await;
                send_turn_start_failure!(format!(
                    "failed to persist CLI runtime prompt manifest: {error:#}"
                ));
                return;
            }
            let native_thread_id = native_thread.binding.native_thread_id;
            if matches!(runtime_kind, CLIAgentRuntimeKind::Codex)
                && let Err(error) = cli_session
                    .reset_native_thread_goal(native_thread_id.as_str())
                    .await
            {
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    format!("failed to reset Codex Goal before turn start: {error:#}"),
                )
                .await;
                send_turn_start_failure!(format!(
                    "failed to reset Codex Goal before turn start: {error:#}"
                ));
                return;
            }
            let native_turn_start_params =
                crate::cli_runtime::manager::CLIAgentRuntimeTurnStartParams {
                    native_thread_id: native_thread_id.clone(),
                    input: native_turn_input,
                    cwd: native_thread.binding.native_cwd,
                    approval_policy: Some(effective_approval_policy),
                    sandbox: sandbox_policy_value,
                    permissions: provider_permissions_id,
                    model: Some(outcome.materialization.thread.model.clone()),
                    effort: effective_cli_runtime_effort,
                    personality: cli_runtime_personality,
                    summary: cli_runtime_summary,
                    elevated_instructions,
                };
            let native_turn_start = PreparedCliRuntimeNativeTurnStart {
                outcome,
                user_message_capability_attachments,
                session_instance: session_handle.instance().clone(),
                cli_session,
                native_thread_id,
                turn_start_params: native_turn_start_params,
                request_timeout_ms: runtime_config.request_timeout_ms,
            };

            let success_sent = match &success_response {
                TurnStartSuccessResponse::TurnStart => {
                    message_future(
                        self.finish_turn_start_success(
                            connection_id,
                            request_id.clone(),
                            &native_turn_start.outcome,
                            native_turn_start
                                .user_message_capability_attachments
                                .as_slice(),
                        ),
                    )
                    .await
                }
                TurnStartSuccessResponse::VoiceSessionFinalizeAccepted { session_id } => {
                    message_future(
                        self.finish_voice_session_finalize_accepted_turn_start_success(
                            connection_id,
                            &native_turn_start.outcome,
                            native_turn_start
                                .user_message_capability_attachments
                                .as_slice(),
                            session_id,
                        ),
                    )
                    .await
                }
                TurnStartSuccessResponse::Task { .. }
                | TurnStartSuccessResponse::DurableAgent { .. } => true,
            };
            if !success_sent {
                self.mark_turn_blocked(
                    native_turn_start
                        .outcome
                        .started_notification
                        .thread_id
                        .clone(),
                    native_turn_start
                        .outcome
                        .started_notification
                        .turn
                        .id
                        .clone(),
                    "failed to commit CLI runtime turn start lifecycle".to_owned(),
                )
                .await;
                success_response.complete_task(Err(anyhow::anyhow!(
                    "failed to publish task CLI runtime turn start"
                )));
                return;
            }
            let pioneer_turn_id = native_turn_start
                .outcome
                .started_notification
                .turn
                .id
                .clone();
            self.retain_cli_runtime_session_turn_lease(
                pioneer_turn_id.as_str(),
                session_turn_lease,
            )
            .await;
            if matches!(
                success_response,
                TurnStartSuccessResponse::Task { .. }
                    | TurnStartSuccessResponse::DurableAgent { .. }
            ) {
                if !success_response.complete_task(Ok(native_turn_start)) {
                    self.release_cli_runtime_session_turn_lease(pioneer_turn_id.as_str())
                        .await;
                }
            } else {
                self.spawn_prepared_cli_runtime_native_turn(native_turn_start);
            }
            })
            .await;
        })
    }

    pub(super) fn spawn_prepared_cli_runtime_native_turn(
        &self,
        prepared: PreparedCliRuntimeNativeTurnStart,
    ) {
        let processor = self.clone();
        let _handle = tokio::spawn(async move {
            processor
                .start_prepared_cli_runtime_native_turn(prepared)
                .await;
        });
    }

    async fn start_prepared_cli_runtime_native_turn(
        &self,
        prepared: PreparedCliRuntimeNativeTurnStart,
    ) {
        let PreparedCliRuntimeNativeTurnStart {
            outcome,
            user_message_capability_attachments: _,
            session_instance,
            cli_session,
            native_thread_id,
            turn_start_params,
            request_timeout_ms,
        } = prepared;
        let pioneer_turn_id = outcome.started_notification.turn.id.clone();
        let mcp_metadata = match cli_session
            .prepare_mcp_turn(
                outcome.started_notification.thread_id.as_str(),
                pioneer_turn_id.as_str(),
            )
            .await
        {
            Ok(metadata) => metadata,
            Err(error) => {
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    pioneer_turn_id,
                    format!("failed to reserve CLI MCP turn lease: {error:#}"),
                )
                .await;
                return;
            }
        };
        if let Some(metadata) = mcp_metadata {
            let session_generation = match i64::try_from(metadata.session_generation) {
                Ok(generation) => generation,
                Err(_) => {
                    let _ = cli_session
                        .terminal_mcp_turn(pioneer_turn_id.as_str())
                        .await;
                    self.mark_turn_blocked(
                        outcome.started_notification.thread_id.clone(),
                        pioneer_turn_id,
                        "CLI MCP session generation exceeds durable range".to_owned(),
                    )
                    .await;
                    return;
                }
            };
            let projection_activation_generation =
                match i64::try_from(metadata.projection_activation_generation) {
                    Ok(generation) => generation,
                    Err(_) => {
                        let _ = cli_session
                            .terminal_mcp_turn(pioneer_turn_id.as_str())
                            .await;
                        self.mark_turn_blocked(
                            outcome.started_notification.thread_id.clone(),
                            pioneer_turn_id,
                            "CLI MCP activation generation exceeds durable range".to_owned(),
                        )
                        .await;
                        return;
                    }
                };
            if let Err(error) = self
                .crud_store
                .bind_cli_runtime_turn_mcp_activation(
                    pioneer_turn_id.as_str(),
                    pioneer_crud::CliRuntimeTurnMcpMetadata {
                        adapter_kind: metadata.adapter_kind,
                        manifest_hash: metadata.manifest_hash,
                        projection_fingerprint: metadata.projection_fingerprint,
                        provider_contract_fingerprint: metadata.provider_contract_fingerprint,
                        isolation_contract_fingerprint: metadata.isolation_contract_fingerprint,
                        session_generation,
                        projection_activation_generation,
                    },
                )
                .await
            {
                let _ = cli_session
                    .terminal_mcp_turn(pioneer_turn_id.as_str())
                    .await;
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    pioneer_turn_id,
                    format!("failed to persist CLI MCP turn lease: {error:#}"),
                )
                .await;
                return;
            }
        }
        let native_turn = match cli_session
            .start_turn(
                turn_start_params,
                std::time::Duration::from_millis(request_timeout_ms),
            )
            .await
        {
            Ok(native_turn) => native_turn,
            Err(error) => {
                let _ = cli_session
                    .terminal_mcp_turn(pioneer_turn_id.as_str())
                    .await;
                self.fail_initial_cli_runtime_turn_attempt(
                    outcome.started_notification.turn.id.as_str(),
                    format!("failed to start native CLI runtime turn: {error:#}"),
                )
                .await;
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    pioneer_turn_id,
                    format!("failed to start CLI runtime turn: {error:#}"),
                )
                .await;
                return;
            }
        };
        let native_turn_id = native_turn.native_turn_id.clone();
        let turn_binding = match
            crate::cli_runtime::turn_binding::persist_cli_runtime_turn_binding_after_native_start_owned(
                self.crud_store.as_ref(),
                crate::cli_runtime::turn_binding::CLIAgentRuntimeNativeTurnStarted {
                    turn_id: outcome.started_notification.turn.id.clone(),
                    native_turn_id: native_turn_id.clone(),
                    request_id: None,
                    started_at: chrono::Utc::now().fixed_offset(),
                },
                self.turn_execution_owner_id.as_ref(),
            )
            .await
        {
            Ok(binding) => binding,
            Err(error) => {
                let _ = cli_session
                    .terminal_mcp_turn(pioneer_turn_id.as_str())
                    .await;
                self.fail_initial_cli_runtime_turn_attempt(
                    outcome.started_notification.turn.id.as_str(),
                    format!("failed to persist native CLI runtime owner: {error:#}"),
                )
                .await;
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    format!("failed to persist CLI runtime native turn id: {error:#}"),
                )
                .await;
                let _ = cli_session
                    .interrupt_turn(Some(native_thread_id.as_str()), Some(native_turn_id.as_str()))
                    .await;
                return;
            }
        };
        self.notify_semantic_timeline_turn_state_changed(
            turn_binding.workspace_id.as_str(),
            turn_binding.thread_id.as_str(),
            turn_binding.turn_id.as_str(),
        )
        .await;
        let attempt = match self
            .crud_store
            .latest_cli_runtime_turn_attempt(outcome.started_notification.turn.id.as_str())
            .await
        {
            Ok(Some(attempt)) => attempt,
            Ok(None) => {
                let _ = cli_session
                    .terminal_mcp_turn(pioneer_turn_id.as_str())
                    .await;
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    "CLI runtime native turn started without a durable attempt".to_owned(),
                )
                .await;
                let _ = cli_session
                    .interrupt_turn(
                        Some(native_thread_id.as_str()),
                        Some(native_turn_id.as_str()),
                    )
                    .await;
                return;
            }
            Err(error) => {
                let _ = cli_session
                    .terminal_mcp_turn(pioneer_turn_id.as_str())
                    .await;
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    format!("failed to load CLI runtime attempt after native start: {error:#}"),
                )
                .await;
                let _ = cli_session
                    .interrupt_turn(
                        Some(native_thread_id.as_str()),
                        Some(native_turn_id.as_str()),
                    )
                    .await;
                return;
            }
        };
        if let Err(error) = self
            .publish_cli_runtime_attempt_window_started(&session_instance, &turn_binding, &attempt)
            .await
        {
            let _ = cli_session
                .terminal_mcp_turn(pioneer_turn_id.as_str())
                .await;
            self.fail_cli_runtime_turn_attempt(
                attempt.id.as_str(),
                format!("failed to open CLI runtime execution window: {error:#}"),
            )
            .await;
            self.mark_turn_blocked(
                outcome.started_notification.thread_id.clone(),
                outcome.started_notification.turn.id.clone(),
                format!("failed to open CLI runtime execution window: {error:#}"),
            )
            .await;
            let _ = cli_session
                .interrupt_turn(
                    Some(native_thread_id.as_str()),
                    Some(native_turn_id.as_str()),
                )
                .await;
            return;
        }
        if let Err(error) = cli_session
            .activate_mcp_turn(
                pioneer_turn_id.as_str(),
                native_thread_id.as_str(),
                native_turn_id.as_str(),
            )
            .await
        {
            let _ = cli_session
                .terminal_mcp_turn(pioneer_turn_id.as_str())
                .await;
            self.fail_cli_runtime_turn_attempt(
                attempt.id.as_str(),
                format!("failed to activate CLI MCP turn lease: {error:#}"),
            )
            .await;
            let _ = cli_session
                .interrupt_turn(
                    Some(native_thread_id.as_str()),
                    Some(native_turn_id.as_str()),
                )
                .await;
            self.mark_turn_blocked(
                outcome.started_notification.thread_id.clone(),
                pioneer_turn_id,
                format!("failed to activate CLI MCP turn lease: {error:#}"),
            )
            .await;
            return;
        }
        if turn_binding.runtime_kind == "codex" {
            self.bind_buffered_codex_root_execution_segments(
                &session_instance,
                native_thread_id.as_str(),
            )
            .await;
        }
        self.flush_cli_runtime_events_for_native_turn(
            &session_instance,
            native_thread_id.as_str(),
            native_turn_id.as_str(),
        )
        .await;
    }

    async fn publish_cli_runtime_attempt_window_started(
        &self,
        session_instance: &crate::cli_runtime::session_instance::CliSessionInstanceId,
        binding: &pioneer_crud::CliRuntimeTurnBindingRecord,
        attempt: &pioneer_crud::CliRuntimeTurnAttemptRecord,
    ) -> anyhow::Result<()> {
        let window_index = attempt
            .execution_window_index
            .context("CLI runtime attempt has no execution window index")?;
        let mut latest = self
            .crud_store
            .latest_turn_execution_window(binding.turn_id.as_str())
            .await?;
        if let Some(previous) = latest.as_ref()
            && previous.window_index.checked_add(1) == Some(window_index)
            && previous.status == pioneer_protocol::ExecutionWindowStatus::Checkpointed
        {
            let checkpoint = self
                .crud_store
                .latest_turn_execution_checkpoint_for_turn(binding.turn_id.as_str())
                .await?
                .context("CLI runtime recovery has no durable execution checkpoint")?;
            if checkpoint.window_id != previous.id {
                anyhow::bail!(
                    "CLI runtime recovery checkpoint `{}` does not belong to the latest execution window `{}`",
                    checkpoint.id,
                    previous.id
                );
            }
            let previous_runtime_window_id = previous
                .metadata_json
                .get("runtimeWindowId")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{}:window:{}", binding.turn_id, previous.window_index));
            let continued_at_unix_ms = chrono::Utc::now()
                .timestamp_millis()
                .max(
                    previous
                        .completed_at
                        .unwrap_or(previous.updated_at)
                        .timestamp_millis(),
                )
                .max(checkpoint.created_at.timestamp_millis());
            self.publish_cli_runtime_durable_and_wait(
                session_instance,
                AgentDurableEvent::TurnExecutionWindowContinued {
                    notification: pioneer_protocol::TurnExecutionWindowContinuedNotification {
                        workspace_id: binding.workspace_id.clone(),
                        thread_id: binding.thread_id.clone(),
                        turn_id: binding.turn_id.clone(),
                        window_id: attempt.id.clone(),
                        window_index,
                        status: pioneer_protocol::ExecutionWindowStatus::Continued,
                        previous_window_id: previous_runtime_window_id,
                        previous_window_index: previous.window_index,
                        checkpoint_id: checkpoint.id,
                        continued_at_unix_ms,
                    },
                },
            )
            .await
            .map_err(|error| anyhow::anyhow!("failed to continue execution window: {error}"))?;
            latest = self
                .crud_store
                .latest_turn_execution_window(binding.turn_id.as_str())
                .await?;
        }
        if let Some(latest) = latest.as_ref() {
            if latest.window_index == window_index {
                let owns_window = latest.status == pioneer_protocol::ExecutionWindowStatus::Running
                    && latest
                        .metadata_json
                        .get("runtimeWindowId")
                        .and_then(serde_json::Value::as_str)
                        == Some(attempt.id.as_str());
                let was_continued = latest
                    .metadata_json
                    .get("createdByContinuationCheckpointId")
                    .and_then(serde_json::Value::as_str)
                    .is_some();
                if owns_window && !was_continued {
                    return Ok(());
                }
                if !owns_window {
                    anyhow::bail!(
                        "CLI runtime attempt window {window_index} is already owned by another execution"
                    );
                }
            }
            if latest.window_index >= window_index {
                let owns_continued_window = latest.window_index == window_index
                    && latest.status == pioneer_protocol::ExecutionWindowStatus::Running
                    && latest
                        .metadata_json
                        .get("runtimeWindowId")
                        .and_then(serde_json::Value::as_str)
                        == Some(attempt.id.as_str())
                    && latest
                        .metadata_json
                        .get("createdByContinuationCheckpointId")
                        .and_then(serde_json::Value::as_str)
                        .is_some();
                if !owns_continued_window {
                    anyhow::bail!(
                        "CLI runtime attempt window {window_index} is behind stored window {}",
                        latest.window_index
                    );
                }
            }
        }
        let started_at = latest
            .as_ref()
            .filter(|window| window.window_index == window_index)
            .map(|window| window.started_at.timestamp_millis())
            .unwrap_or_else(|| {
                attempt
                    .started_at
                    .unwrap_or(attempt.created_at)
                    .timestamp_millis()
            });
        self.publish_cli_runtime_durable_and_wait(
            session_instance,
            AgentDurableEvent::TurnExecutionWindowStarted {
                notification: pioneer_protocol::TurnExecutionWindowStartedNotification {
                    workspace_id: binding.workspace_id.clone(),
                    thread_id: binding.thread_id.clone(),
                    turn_id: binding.turn_id.clone(),
                    window_id: attempt.id.clone(),
                    window_index,
                    status: pioneer_protocol::ExecutionWindowStatus::Running,
                    started_at_unix_ms: started_at,
                },
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("failed to commit execution window: {error}"))
    }

    async fn fail_initial_cli_runtime_turn_attempt(&self, turn_id: &str, reason: String) {
        match self
            .crud_store
            .latest_cli_runtime_turn_attempt(turn_id)
            .await
        {
            Ok(Some(attempt))
                if attempt.status.is_active() && attempt.recovery_attempt_id.is_none() =>
            {
                self.fail_cli_runtime_turn_attempt(attempt.id.as_str(), reason)
                    .await;
            }
            Ok(_) => {}
            Err(error) => {
                warn!(
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to load CLI runtime attempt while recording start failure"
                );
            }
        }
    }

    async fn fail_cli_runtime_turn_attempt(&self, attempt_id: &str, reason: String) {
        match self
            .crud_store
            .mark_cli_runtime_turn_attempt_terminal(
                attempt_id,
                pioneer_crud::CliRuntimeTurnAttemptStatus::Failed,
                Some(reason),
                chrono::Utc::now().fixed_offset(),
            )
            .await
        {
            Ok(true) | Ok(false) => {}
            Err(error) => {
                warn!(
                    attempt_id,
                    error = %format!("{error:#}"),
                    "failed to terminalize CLI runtime attempt after start failure"
                );
            }
        }
    }

    /// Reconstructs the exact durable process contract for every CLI runtime
    /// continuation. No caller may substitute process cwd, Gateway cwd, `/`,
    /// current MCP selection, or default session options.
    pub(super) async fn restore_cli_runtime_launch_spec(
        &self,
        requested_binding: &pioneer_crud::CliRuntimeTurnBindingRecord,
    ) -> CliRuntimeLaunchSpecRestore {
        let binding = match self
            .crud_store
            .get_cli_runtime_turn_binding(requested_binding.turn_id.as_str())
            .await
        {
            Ok(Some(binding)) => binding,
            Ok(None) => {
                return CliRuntimeLaunchSpecRestore::InvalidBinding {
                    diagnostic: format!(
                        "CLI runtime turn `{}` has no durable launch binding",
                        requested_binding.turn_id
                    ),
                };
            }
            Err(error) => {
                return CliRuntimeLaunchSpecRestore::Unavailable {
                    diagnostic: format!("failed to load durable CLI runtime binding: {error:#}"),
                };
            }
        };
        if binding.thread_id != requested_binding.thread_id
            || binding.continuation_thread_id != requested_binding.continuation_thread_id
            || binding.workspace_id != requested_binding.workspace_id
            || binding.runtime_id != requested_binding.runtime_id
            || binding.runtime_kind != requested_binding.runtime_kind
            || binding.native_thread_id != requested_binding.native_thread_id
        {
            return CliRuntimeLaunchSpecRestore::InvalidBinding {
                diagnostic: format!(
                    "CLI runtime turn binding `{}` changed execution identity",
                    binding.turn_id
                ),
            };
        }

        let encoded_authority = match self
            .crud_store
            .get_turn_execution_authorization_context(binding.turn_id.as_str())
            .await
        {
            Ok(Some(encoded)) => encoded,
            Ok(None) => {
                return CliRuntimeLaunchSpecRestore::InvalidBinding {
                    diagnostic: format!(
                        "CLI runtime turn `{}` has no durable authorization context",
                        binding.turn_id
                    ),
                };
            }
            Err(error) => {
                return CliRuntimeLaunchSpecRestore::Unavailable {
                    diagnostic: format!(
                        "failed to load CLI runtime authorization context: {error:#}"
                    ),
                };
            }
        };
        let authorization_context =
            match crate::authorization::ExecutionAuthorizationContext::from_persisted_json(
                encoded_authority.as_str(),
            ) {
                Ok(context) => context,
                Err(error) => {
                    return CliRuntimeLaunchSpecRestore::InvalidBinding {
                        diagnostic: format!(
                            "CLI runtime turn `{}` has an invalid durable authorization context: {error:#}",
                            binding.turn_id
                        ),
                    };
                }
            };
        let policy_revision = match self.current_authorization_revision().await {
            Ok(revision) => revision,
            Err(error) => {
                return CliRuntimeLaunchSpecRestore::Unavailable {
                    diagnostic: format!(
                        "CLI runtime continuation policy generation is unavailable: {error:#}"
                    ),
                };
            }
        };
        if let Err(error) = self
            .execution_leases
            .revalidate_for_turn(
                self.crud_store.as_ref(),
                &authorization_context,
                binding.workspace_id.as_str(),
                binding.thread_id.as_str(),
                binding.turn_id.as_str(),
                crate::authorization::ResourceAction::CliRuntimeUse,
                policy_revision,
            )
            .await
        {
            return CliRuntimeLaunchSpecRestore::InvalidBinding {
                diagnostic: format!(
                    "CLI runtime continuation authority is no longer active: {error:#}"
                ),
            };
        }

        let runtime_kind = match binding.runtime_kind.as_str() {
            "codex" => CLIAgentRuntimeKind::Codex,
            "claude" => CLIAgentRuntimeKind::Claude,
            other => {
                return CliRuntimeLaunchSpecRestore::InvalidBinding {
                    diagnostic: format!("unsupported durable CLI runtime kind `{other}`"),
                };
            }
        };
        if let Err(error) = authorization_context.verify_cli_runtime_projection(
            binding.workspace_id.as_str(),
            binding.runtime_id.as_str(),
            runtime_kind,
        ) {
            return CliRuntimeLaunchSpecRestore::InvalidBinding {
                diagnostic: format!(
                    "CLI runtime identity differs from its immutable authorization projection: {error:#}"
                ),
            };
        }
        let native_event_budget =
            match authorization_context.effective_native_event_resource_budget() {
                Ok(budget) => budget,
                Err(error) => {
                    return CliRuntimeLaunchSpecRestore::InvalidBinding {
                        diagnostic: format!(
                            "CLI runtime native-event contract cannot be restored: {error:#}"
                        ),
                    };
                }
            };

        let security_snapshot = match self
            .crud_store
            .get_turn_execution_security_snapshot(binding.turn_id.as_str())
            .await
        {
            Ok(Some(record)) => record.snapshot,
            Ok(None) => {
                return CliRuntimeLaunchSpecRestore::InvalidBinding {
                    diagnostic: format!(
                        "CLI runtime turn `{}` has no durable execution security snapshot",
                        binding.turn_id
                    ),
                };
            }
            Err(error) => {
                return CliRuntimeLaunchSpecRestore::Unavailable {
                    diagnostic: format!(
                        "failed to load CLI runtime execution security snapshot: {error:#}"
                    ),
                };
            }
        };
        if security_snapshot.schema_version
            != pioneer_protocol::TURN_EXECUTION_SECURITY_SNAPSHOT_SCHEMA_VERSION
            || security_snapshot.version == 0
        {
            return CliRuntimeLaunchSpecRestore::InvalidBinding {
                diagnostic: "CLI runtime execution security snapshot has an unsupported identity"
                    .to_owned(),
            };
        }
        let native_cwd = security_snapshot.sandbox.cwd.trim().to_owned();
        if native_cwd.is_empty() || !std::path::Path::new(native_cwd.as_str()).is_absolute() {
            return CliRuntimeLaunchSpecRestore::InvalidBinding {
                diagnostic:
                    "CLI runtime execution security snapshot has no absolute working directory"
                        .to_owned(),
            };
        }
        if binding.cwd.as_deref() != Some(native_cwd.as_str()) {
            return CliRuntimeLaunchSpecRestore::InvalidBinding {
                diagnostic: format!(
                    "CLI runtime turn `{}` working directory differs from its immutable security snapshot",
                    binding.turn_id
                ),
            };
        }

        let sandbox = match binding
            .sandbox_json
            .as_deref()
            .map(pioneer_crud::deserialize_cli_runtime_json)
            .transpose()
        {
            Ok(sandbox) => sandbox,
            Err(error) => {
                return CliRuntimeLaunchSpecRestore::InvalidBinding {
                    diagnostic: format!("durable CLI runtime sandbox is invalid: {error:#}"),
                };
            }
        };
        let permissions = matches!(runtime_kind, CLIAgentRuntimeKind::Codex).then(|| {
            crate::cli_runtime::permissions::codex_permissions_profile_for_security_snapshot(
                &security_snapshot,
            )
            .to_owned()
        });

        let skill_bindings = match self
            .crud_store
            .find_turn_skill_bindings(binding.turn_id.as_str())
            .await
        {
            Ok(bindings) => bindings,
            Err(error) => {
                return CliRuntimeLaunchSpecRestore::Unavailable {
                    diagnostic: format!(
                        "failed to load frozen CLI runtime skill projection: {error:#}"
                    ),
                };
            }
        };
        if !skill_bindings.is_empty()
            && let Err(error) = self
                .revalidate_persisted_turn_skill_projection(
                    binding.thread_id.as_str(),
                    binding.turn_id.as_str(),
                )
                .await
        {
            return CliRuntimeLaunchSpecRestore::InvalidBinding {
                diagnostic: format!(
                    "CLI runtime skill projection is no longer authorized: {error:#}"
                ),
            };
        }

        let projection_exists = match self
            .crud_store
            .get_cli_runtime_instruction_projection(binding.turn_id.as_str())
            .await
        {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(error) => {
                return CliRuntimeLaunchSpecRestore::Unavailable {
                    diagnostic: format!(
                        "failed to load frozen CLI runtime instruction projection: {error:#}"
                    ),
                };
            }
        };
        if !projection_exists {
            return CliRuntimeLaunchSpecRestore::InvalidBinding {
                diagnostic: format!(
                    "CLI runtime turn `{}` has no frozen instruction projection",
                    binding.turn_id
                ),
            };
        }
        let elevated_instructions = match crate::cli_runtime::instruction_projection::load_cli_runtime_instruction_projection(
            self.crud_store.as_ref(),
            binding.turn_id.as_str(),
            runtime_kind,
        )
        .await
        {
            Ok(projection) => projection,
            Err(error) => {
                return CliRuntimeLaunchSpecRestore::InvalidBinding {
                    diagnostic: format!(
                        "CLI runtime frozen instruction projection is invalid: {error:#}"
                    ),
                };
            }
        };

        let runtime = match self.load_cli_runtime_instances() {
            Ok(runtimes) => match runtimes
                .into_iter()
                .find(|runtime| runtime.id == binding.runtime_id)
            {
                Some(runtime) => runtime,
                None => {
                    return CliRuntimeLaunchSpecRestore::InvalidBinding {
                        diagnostic: format!(
                            "configured CLI runtime `{}` for continuation is missing",
                            binding.runtime_id
                        ),
                    };
                }
            },
            Err(error) => {
                return CliRuntimeLaunchSpecRestore::Unavailable {
                    diagnostic: format!("CLI runtime configuration is unavailable: {error:#}"),
                };
            }
        };
        if !runtime.enabled {
            return CliRuntimeLaunchSpecRestore::InvalidBinding {
                diagnostic: format!("CLI runtime `{}` is disabled", runtime.id),
            };
        }
        if !cli_runtime_kind_matches_config(runtime_kind, runtime.kind) {
            return CliRuntimeLaunchSpecRestore::InvalidBinding {
                diagnostic: format!(
                    "CLI runtime `{}` kind differs from its durable turn binding",
                    runtime.id
                ),
            };
        }

        let proxy_url = match self
            .prepare_cli_runtime_proxy_url(
                binding.workspace_id.as_str(),
                binding.runtime_id.as_str(),
            )
            .await
        {
            Ok(proxy_url) => proxy_url,
            Err(error) => {
                return CliRuntimeLaunchSpecRestore::Unavailable {
                    diagnostic: format!(
                        "CLI runtime proxy environment cannot be reconstructed: {error:#}"
                    ),
                };
            }
        };
        let authorization_scope_fingerprint =
            match cli_runtime_session_authorization_scope_fingerprint(
                &authorization_context,
                &security_snapshot,
            ) {
                Ok(fingerprint) => fingerprint,
                Err(error) => {
                    return CliRuntimeLaunchSpecRestore::InvalidBinding {
                        diagnostic: format!(
                            "CLI runtime continuation authorization scope is invalid: {error:#}"
                        ),
                    };
                }
            };
        let selected_skills = if matches!(runtime_kind, CLIAgentRuntimeKind::Claude) {
            let frozen_bindings = skill_bindings
                .iter()
                .map(|binding| pioneer_protocol::TurnSkillBinding {
                    skill_id: binding.skill_id.clone(),
                    skill_owner: binding.skill_owner.clone(),
                    skill_slug: binding.skill_slug.clone(),
                    skill_version: binding.skill_version.clone(),
                    fingerprint: binding.fingerprint.clone(),
                    source_kind: binding.source_kind.clone(),
                    resolved_reason: binding.resolved_reason.clone(),
                })
                .collect::<Vec<_>>();
            let receipt_path = self
                .artifact_runtime_home
                .join(pioneer_skills::EXTERNAL_RUNTIME_RECEIPT_FILE_NAME);
            match crate::cli_runtime::skills::restore_cli_runtime_selected_skills(
                &runtime,
                runtime_kind,
                frozen_bindings.as_slice(),
                receipt_path.as_path(),
            ) {
                Ok(selected) => selected,
                Err(error) => {
                    return CliRuntimeLaunchSpecRestore::InvalidBinding {
                        diagnostic: format!(
                            "failed to restore exact Claude skill projection: {error:#}"
                        ),
                    };
                }
            }
        } else {
            Vec::new()
        };
        let session_options = crate::cli_runtime::manager::CLIAgentRuntimeSessionStartOptions {
            cwd: Some(std::path::PathBuf::from(native_cwd.as_str())),
            approval_policy: binding.approval_policy.clone(),
            authorization_scope_fingerprint: Some(authorization_scope_fingerprint),
            env: crate::cli_runtime::config::proxy_env(proxy_url.as_deref()),
            selected_skills,
            elevated_instructions: matches!(runtime_kind, CLIAgentRuntimeKind::Claude)
                .then_some(elevated_instructions.clone()),
            ..Default::default()
        };
        let (max_tools, max_total_schema_bytes) = self.mcp_service.projection_limit_values();
        let mcp = match crate::cli_runtime::mcp::recovery::restore_cli_mcp_session_launch(
            self.crud_store.as_ref(),
            self.mcp_service.as_ref(),
            &binding,
            runtime_kind,
            crate::turn_mcp::McpProjectionLimits {
                max_tools,
                max_total_schema_bytes,
                ..crate::turn_mcp::McpProjectionLimits::default()
            },
        )
        .await
        {
            Ok(mcp) => mcp,
            Err(
                crate::cli_runtime::mcp::recovery::CliMcpSessionLaunchRestoreError::Unavailable(
                    error,
                ),
            ) => {
                return CliRuntimeLaunchSpecRestore::Unavailable {
                    diagnostic: format!(
                        "frozen CLI runtime MCP projection is temporarily unavailable: {error:#}"
                    ),
                };
            }
            Err(crate::cli_runtime::mcp::recovery::CliMcpSessionLaunchRestoreError::Invalid(
                error,
            )) => {
                return CliRuntimeLaunchSpecRestore::InvalidBinding {
                    diagnostic: format!("frozen CLI runtime MCP projection is invalid: {error:#}"),
                };
            }
        };
        let launch_spec = match runtime_kind {
            CLIAgentRuntimeKind::Codex => {
                crate::cli_runtime::continuation::CliSessionLaunchSpec::codex(
                    session_options,
                    mcp,
                    Some(binding.native_thread_id.clone()),
                )
                .with_native_event_budget(native_event_budget)
            }
            CLIAgentRuntimeKind::Claude => {
                let provider_session_id =
                    match uuid::Uuid::parse_str(binding.native_thread_id.as_str()) {
                        Ok(provider_session_id) if !provider_session_id.is_nil() => {
                            provider_session_id
                        }
                        _ => {
                            return CliRuntimeLaunchSpecRestore::InvalidBinding {
                                diagnostic:
                                    "durable Claude continuation identity is not a non-nil UUID"
                                        .to_owned(),
                            };
                        }
                    };
                crate::cli_runtime::continuation::CliSessionLaunchSpec::claude_resume(
                    session_options,
                    mcp,
                    provider_session_id,
                )
                .with_native_event_budget(native_event_budget)
            }
        };
        let session_key = match crate::cli_runtime::manager::CLIAgentRuntimeSessionKey::new(
            binding.workspace_id.as_str(),
            binding.runtime_id.as_str(),
            binding.continuation_thread_id.as_str(),
        ) {
            Ok(key) => key,
            Err(error) => {
                return CliRuntimeLaunchSpecRestore::InvalidBinding {
                    diagnostic: format!(
                        "durable CLI runtime session identity is invalid: {error:#}"
                    ),
                };
            }
        };

        CliRuntimeLaunchSpecRestore::Ready(RestoredCliRuntimeLaunchSpec {
            binding,
            runtime_kind,
            runtime,
            session_key,
            launch_spec,
            native_cwd,
            sandbox,
            permissions,
            elevated_instructions,
        })
    }

    pub(super) async fn start_cli_runtime_recovery_attempt(
        &self,
        request: crate::resilience::CliRuntimeRecoveryAttemptRequest,
    ) -> Result<bool, CliRuntimeRecoveryStartFailure> {
        let restored = match self.restore_cli_runtime_launch_spec(&request.binding).await {
            CliRuntimeLaunchSpecRestore::Ready(restored) => restored,
            CliRuntimeLaunchSpecRestore::Unavailable { diagnostic } => {
                return Err(CliRuntimeRecoveryStartFailure::Unavailable { diagnostic });
            }
            CliRuntimeLaunchSpecRestore::InvalidBinding { diagnostic } => {
                return Err(CliRuntimeRecoveryStartFailure::InvalidBinding { diagnostic });
            }
        };
        let binding = restored.binding.clone();
        let Some((_workspace_id, turn)) = self
            .crud_store
            .get_turn(binding.thread_id.as_str(), binding.turn_id.as_str())
            .await?
        else {
            return Err(CliRuntimeRecoveryStartFailure::InvalidBinding {
                diagnostic: format!("Pioneer turn `{}` is missing", binding.turn_id),
            });
        };
        if turn.status != TurnStatus::InProgress {
            return Err(CliRuntimeRecoveryStartFailure::InvalidBinding {
                diagnostic: format!(
                    "Pioneer turn `{}` is `{}` and cannot start CLI recovery",
                    binding.turn_id,
                    format!("{:?}", turn.status).to_ascii_lowercase()
                ),
            });
        }
        if let Some(existing) = self
            .crud_store
            .get_cli_runtime_turn_attempt_by_recovery_attempt(request.recovery_attempt_id.as_str())
            .await?
        {
            if existing.turn_id != request.turn_id
                || existing.recovery_job_id.as_deref() != Some(request.job_id.as_str())
                || existing.execution_window_index != Some(request.execution_window_index)
                || existing.runtime_id != binding.runtime_id
                || existing.runtime_kind != binding.runtime_kind
                || existing.native_thread_id != binding.native_thread_id
            {
                return Err(CliRuntimeRecoveryStartFailure::InvalidBinding {
                    diagnostic: format!(
                        "recovery attempt `{}` is owned by a different CLI runtime recovery",
                        request.recovery_attempt_id
                    ),
                });
            }
            match existing.status {
                pioneer_crud::CliRuntimeTurnAttemptStatus::Running
                    if existing.native_turn_id.is_some()
                        && existing.native_turn_id == binding.native_turn_id
                        && binding.status
                            == crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_RUNNING =>
                {
                    return Ok(false);
                }
                pioneer_crud::CliRuntimeTurnAttemptStatus::Starting => {}
                status => {
                    return Err(CliRuntimeRecoveryStartFailure::InvalidBinding {
                        diagnostic: format!(
                            "CLI runtime recovery attempt `{}` is `{}` and cannot start",
                            existing.id,
                            status.as_str()
                        ),
                    });
                }
            }
        }
        let manager = self
            .cli_runtime_manager
            .as_ref()
            .context("CLI runtime manager is unavailable")?;
        let session_handle = manager
            .get_or_start_with_launch_spec(
                restored.session_key.clone(),
                restored.launch_spec.clone(),
            )
            .await?;
        let cli_session = session_handle.session();
        self.ensure_cli_runtime_session_event_pumps(
            session_handle.instance(),
            cli_session.clone(),
            restored.runtime.debug_native_events,
        )
        .await;
        let resumed = cli_session
            .resume_thread(
                binding.native_thread_id.as_str(),
                crate::cli_runtime::manager::CLIAgentRuntimeThreadOpenParams {
                    cwd: restored.native_cwd.clone(),
                    model: binding.model.clone(),
                    approval_policy: binding.approval_policy.clone(),
                    sandbox: restored.sandbox.clone(),
                    permissions: restored.permissions.clone(),
                    service_tier: None,
                },
                std::time::Duration::from_millis(restored.runtime.request_timeout_ms),
            )
            .await?;
        if resumed.native_thread_id != binding.native_thread_id {
            return Err(CliRuntimeRecoveryStartFailure::InvalidBinding {
                diagnostic: format!(
                    "CLI runtime resumed native thread `{}` instead of `{}`",
                    resumed.native_thread_id, binding.native_thread_id
                ),
            });
        }
        if matches!(restored.runtime_kind, CLIAgentRuntimeKind::Codex) {
            cli_session
                .reset_native_thread_goal(binding.native_thread_id.as_str())
                .await
                .context("failed to reset Codex Goal before recovery")?;
        }

        let prepared_at = chrono::Utc::now().fixed_offset();
        let (prepared_binding, attempt) = self
            .crud_store
            .prepare_cli_runtime_recovery_turn_attempt(
                binding.turn_id.as_str(),
                pioneer_protocol::generate_id(21),
                request.job_id.clone(),
                request.recovery_attempt_id.clone(),
                request.execution_window_index,
                request.previous_failure_reason.clone(),
                prepared_at,
            )
            .await?;
        match attempt.status {
            pioneer_crud::CliRuntimeTurnAttemptStatus::Running
                if attempt.native_turn_id.is_some() =>
            {
                return Ok(false);
            }
            pioneer_crud::CliRuntimeTurnAttemptStatus::Starting => {}
            status => {
                return Err(CliRuntimeRecoveryStartFailure::InvalidBinding {
                    diagnostic: format!(
                        "CLI runtime recovery attempt `{}` is `{}` and cannot start",
                        attempt.id,
                        status.as_str()
                    ),
                });
            }
        }
        if let Err(error) = self
            .publish_cli_runtime_attempt_window_started(
                session_handle.instance(),
                &prepared_binding,
                &attempt,
            )
            .await
        {
            self.fail_cli_runtime_turn_attempt(
                attempt.id.as_str(),
                format!("failed to open CLI runtime recovery execution window: {error:#}"),
            )
            .await;
            return Err(error.into());
        }

        let native_turn = match cli_session
            .start_turn(
                crate::cli_runtime::manager::CLIAgentRuntimeTurnStartParams {
                    native_thread_id: prepared_binding.native_thread_id.clone(),
                    input: crate::cli_runtime::turn_recovery::cli_runtime_recovery_turn_input(),
                    cwd: Some(restored.native_cwd.clone()),
                    model: prepared_binding.model.clone(),
                    approval_policy: prepared_binding.approval_policy.clone(),
                    sandbox: restored.sandbox.clone(),
                    permissions: restored.permissions.clone(),
                    effort: None,
                    personality: None,
                    summary: None,
                    elevated_instructions: restored.elevated_instructions.clone(),
                },
                std::time::Duration::from_millis(restored.runtime.request_timeout_ms),
            )
            .await
        {
            Ok(native_turn) => native_turn,
            Err(error) => {
                self.fail_cli_runtime_turn_attempt(
                    attempt.id.as_str(),
                    format!("failed to start native CLI recovery turn: {error:#}"),
                )
                .await;
                return Err(error.into());
            }
        };
        let native_turn_id = native_turn.native_turn_id.clone();
        if let Err(error) = self
            .crud_store
            .activate_cli_runtime_turn_attempt_owned(
                prepared_binding.turn_id.as_str(),
                attempt.id.as_str(),
                native_turn_id.as_str(),
                None,
                chrono::Utc::now().fixed_offset(),
                self.turn_execution_owner_id.as_ref(),
                chrono::Utc::now().fixed_offset()
                    + chrono::Duration::seconds(super::TURN_EXECUTION_OWNER_LEASE_SECONDS),
            )
            .await
        {
            let _ = cli_session
                .interrupt_turn(
                    Some(prepared_binding.native_thread_id.as_str()),
                    Some(native_turn_id.as_str()),
                )
                .await;
            self.fail_cli_runtime_turn_attempt(
                attempt.id.as_str(),
                format!("failed to persist native recovery owner: {error:#}"),
            )
            .await;
            return Err(error.into());
        }
        if prepared_binding.runtime_kind == "codex" {
            self.bind_buffered_codex_root_execution_segments(
                session_handle.instance(),
                prepared_binding.native_thread_id.as_str(),
            )
            .await;
        }
        self.flush_cli_runtime_events_for_native_turn(
            session_handle.instance(),
            prepared_binding.native_thread_id.as_str(),
            native_turn_id.as_str(),
        )
        .await;
        Ok(true)
    }

    async fn validate_cli_runtime_turn_start_backend(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        runtime_id: &str,
        runtime_kind: CLIAgentRuntimeKind,
        success_response: &TurnStartSuccessResponse,
        thread_id: &str,
        turn_id: &str,
    ) -> Option<pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig> {
        if self.cli_runtime_manager.is_none() {
            self.send_turn_start_failure(
                connection_id,
                request_id,
                success_response,
                thread_id,
                turn_id,
                cli_runtime_execution_disabled_message(),
            )
            .await;
            return None;
        }

        let runtimes = match self.load_cli_runtime_instances() {
            Ok(runtimes) => runtimes,
            Err(error) => {
                self.send_turn_start_failure(
                    connection_id,
                    request_id,
                    success_response,
                    thread_id,
                    turn_id,
                    format!("failed to load CLI runtime config: {error:#}"),
                )
                .await;
                return None;
            }
        };
        if runtimes.is_empty() {
            self.send_turn_start_failure(
                connection_id,
                request_id,
                success_response,
                thread_id,
                turn_id,
                cli_runtime_execution_disabled_message(),
            )
            .await;
            return None;
        }

        let Some(runtime) = runtimes
            .into_iter()
            .find(|runtime| runtime.id == runtime_id)
        else {
            self.send_turn_start_failure(
                connection_id,
                request_id,
                success_response,
                thread_id,
                turn_id,
                format!("unknown CLI runtime `{runtime_id}`"),
            )
            .await;
            return None;
        };
        if !runtime.enabled {
            self.send_turn_start_failure(
                connection_id,
                request_id,
                success_response,
                thread_id,
                turn_id,
                format!("CLI runtime `{runtime_id}` is disabled"),
            )
            .await;
            return None;
        }
        if !cli_runtime_kind_matches_config(runtime_kind, runtime.kind) {
            self.send_turn_start_failure(
                connection_id,
                request_id,
                success_response,
                thread_id,
                turn_id,
                format!(
                    "CLI runtime `{runtime_id}` is configured as `{}` but request asked for `{}`",
                    cli_runtime_config_kind_label(runtime.kind),
                    cli_runtime_protocol_kind_label(runtime_kind)
                ),
            )
            .await;
            return None;
        }

        Some(runtime)
    }

    pub(super) async fn cli_runtime_turn_start_blocker_for_thread(
        &self,
        key: &crate::cli_runtime::manager::CLIAgentRuntimeSessionKey,
    ) -> anyhow::Result<Option<String>> {
        let bindings = self
            .crud_store
            .list_cli_runtime_turn_bindings(pioneer_crud::CliRuntimeTurnBindingListFilter {
                workspace_id: Some(key.workspace_id.clone()),
                runtime_id: Some(key.runtime_id.clone()),
                continuation_thread_id: Some(key.thread_id.clone()),
                ..Default::default()
            })
            .await?;
        for binding in bindings.into_iter().rev() {
            if binding.workspace_id != key.workspace_id || binding.runtime_id != key.runtime_id {
                continue;
            }
            if binding.status != crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_STARTING
                && binding.status
                    != crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_RUNNING
            {
                continue;
            }

            let turn_status = if let Some((_workspace_id, turn)) = self
                .thread_manager
                .turn_get(binding.thread_id.as_str(), binding.turn_id.as_str())
                .await
            {
                Some(turn.status)
            } else {
                self.crud_store
                    .get_turn(binding.thread_id.as_str(), binding.turn_id.as_str())
                    .await?
                    .map(|(_workspace_id, turn)| turn.status)
            };
            let Some(turn_status) = turn_status else {
                continue;
            };
            if turn_status != TurnStatus::InProgress {
                self.cleanup_cli_runtime_terminal_turn_status(
                    &binding,
                    turn_status,
                    "CLI runtime turn start blocker",
                )
                .await;
                continue;
            }

            return Ok(Some(format!(
                "CLI runtime thread `{}` already has active turn `{}`; wait for it to finish or cancel it before starting another CLI runtime turn",
                key.thread_id, binding.turn_id
            )));
        }

        let mut pending_native_turn_ids = self
            .cli_runtime_pending_turn_events
            .lock()
            .await
            .keys()
            .filter(|pending| {
                pending.workspace_id == key.workspace_id
                    && pending.runtime_id == key.runtime_id
                    && pending.thread_id == key.thread_id
            })
            .map(|pending| pending.native_turn_id.clone())
            .collect::<Vec<_>>();
        pending_native_turn_ids.extend(
            self.cli_runtime_pending_turn_server_requests
                .lock()
                .await
                .keys()
                .filter(|pending| {
                    pending.workspace_id == key.workspace_id
                        && pending.runtime_id == key.runtime_id
                        && pending.thread_id == key.thread_id
                })
                .map(|pending| pending.native_turn_id.clone()),
        );
        pending_native_turn_ids.sort();
        pending_native_turn_ids.dedup();
        pending_native_turn_ids.truncate(3);
        if !pending_native_turn_ids.is_empty() {
            return Ok(Some(format!(
                "CLI runtime thread `{}` has unbound native turn activity for `{}`; wait for the native turn to finish before starting another CLI runtime turn",
                key.thread_id,
                pending_native_turn_ids.join(", ")
            )));
        }

        Ok(None)
    }

    async fn cli_runtime_session_turn_mutex(
        &self,
        key: &crate::cli_runtime::manager::CLIAgentRuntimeSessionKey,
    ) -> Arc<tokio::sync::Mutex<()>> {
        let mut mutexes = self.cli_runtime_session_turn_mutexes.lock().await;
        mutexes
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    async fn keep_task_cli_runtime_queue_alive(
        &self,
        task_run_id: &str,
        execution_id: &str,
    ) -> anyhow::Result<bool> {
        let Some(run) = self.crud_store.get_task_run(task_run_id).await? else {
            return Ok(false);
        };
        if run.status.is_terminal() {
            return Ok(false);
        }
        let Some(execution) = self.crud_store.load_execution_for_run(task_run_id).await? else {
            return Ok(false);
        };
        if execution.id != execution_id || execution.status.is_terminal() {
            return Ok(false);
        }
        let Some(resource_state) = pioneer_crud::load_agent_execution_resource_state(
            &self.crud_store.database_connection(),
            execution_id,
        )
        .await?
        else {
            return Ok(false);
        };
        let now = now_timestamp_secs();
        let heartbeat = self
            .crud_store
            .heartbeat_execution_for_agent_attempt(
                execution_id,
                resource_state.attempt_generation,
                now,
                Some(now.saturating_add(pioneer_tasks::TASK_EXECUTION_LEASE_SECONDS)),
            )
            .await?;
        Ok(heartbeat.is_some())
    }

    async fn retain_cli_runtime_session_turn_lease(
        &self,
        turn_id: &str,
        lease: tokio::sync::OwnedMutexGuard<()>,
    ) {
        self.cli_runtime_session_turn_leases
            .lock()
            .await
            .insert(turn_id.to_owned(), lease);
    }

    pub(crate) async fn release_cli_runtime_session_turn_lease(&self, turn_id: &str) {
        self.cli_runtime_session_turn_leases
            .lock()
            .await
            .remove(turn_id);
    }

    pub(super) fn materialized_turn_permission_profile(
        &self,
        turn: &pioneer_protocol::Turn,
    ) -> anyhow::Result<pioneer_protocol::TurnPermissionProfileSnapshot> {
        Ok(turn.permission_profile.clone())
    }

    /// Resolve and validate the exact security envelope before any durable
    /// Turn/AgentExecution write. An unavailable backend therefore leaves no
    /// visible Turn, admission lease, action, or execution-graph ghost.
    async fn resolve_turn_execution_security_snapshot(
        &self,
        params: &TurnStartParams,
        outcome: &crate::thread::TurnStartOutcome,
        resolved_override: Option<pioneer_protocol::TurnExecutionSecuritySnapshot>,
        execution_authority: ExecutionEnvelopeSource<'_>,
    ) -> Result<pioneer_protocol::TurnExecutionSecuritySnapshot, TurnStartFailure> {
        let permission_profile = self
            .materialized_turn_permission_profile(&outcome.materialization.turn)
            .map_err(|error| {
                TurnStartFailure::internal(format!(
                    "failed to resolve turn permission profile for security snapshot: {error:#}"
                ))
            })?;
        let mut snapshot = if let Some(snapshot) = resolved_override {
            snapshot
        } else {
            let workspace_id = outcome.started_notification.workspace_id.clone();
            let cwd = std::env::current_dir().map_err(|error| {
                TurnStartFailure::internal(format!("failed to resolve turn cwd: {error}"))
            })?;
            let input_context = crate::turn_security::TurnSecurityResolverInputContext {
                workspace_id: workspace_id.clone(),
                cwd: Some(cwd),
                project_roots: Vec::new(),
                app_read_roots: Vec::new(),
                effective_model_provider: outcome.materialization.thread.model_provider.clone(),
                resolved_permission_profile: permission_profile,
                parent_cap: None,
                managed_policy: crate::turn_security::TurnSecurityManagedPolicyInput::default(),
                created_at_unix_ms: now_timestamp_secs().saturating_mul(1000),
            };
            let resolver_input =
                crate::turn_security::TurnSecurityResolverInput::from_turn_start_params(
                    params,
                    input_context,
                )
                .map_err(|error| {
                    TurnStartFailure::internal(format!(
                        "failed to build turn execution security resolver input: {error:#}"
                    ))
                })?;
            crate::turn_security::resolve_turn_execution_security(&resolver_input).map_err(
                |error| {
                    TurnStartFailure::internal(format!(
                        "failed to resolve turn execution security snapshot: {error:#}"
                    ))
                },
            )?
        };
        snapshot.authority_cap.resource_binding_revision = execution_authority.policy_revision();
        self.add_native_turn_runtime_sandbox_roots(
            &mut snapshot,
            outcome.started_notification.workspace_id.as_str(),
            outcome.started_notification.thread_id.as_str(),
            outcome.started_notification.turn.id.as_str(),
        )
        .map_err(|error| {
            TurnStartFailure::internal(format!(
                "failed to resolve native runtime sandbox roots: {error:#}"
            ))
        })?;
        self.log_turn_security_snapshot(
            outcome.started_notification.workspace_id.as_str(),
            outcome.started_notification.thread_id.as_str(),
            outcome.started_notification.turn.id.as_str(),
            &snapshot,
        );
        if let pioneer_protocol::TurnSecurityEnforcementStatus::Unavailable { reason } =
            &snapshot.enforcement
        {
            return Err(TurnStartFailure::unavailable(format!(
                "turn execution security unavailable: {reason}"
            )));
        }
        Ok(snapshot)
    }

    pub(crate) fn add_native_turn_runtime_sandbox_roots(
        &self,
        snapshot: &mut pioneer_protocol::TurnExecutionSecuritySnapshot,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) -> anyhow::Result<()> {
        if snapshot.backend.execution_backend
            != pioneer_protocol::TurnSecurityExecutionBackendKind::Native
            || snapshot.sandbox.filesystem.kind
                == pioneer_protocol::TurnFilesystemSandboxKind::Unrestricted
        {
            return Ok(());
        }

        for root in self.turn_security_app_read_roots(workspace_id)? {
            crate::turn_security::add_native_runtime_read_root(snapshot, root.as_path());
        }
        let artifact_output_root = pioneer_artifacts::artifact_output_dir_path(
            self.artifact_runtime_home.join("artifacts").as_path(),
            workspace_id,
            thread_id,
            turn_id,
        )?;
        crate::turn_security::add_native_runtime_write_root(
            snapshot,
            artifact_output_root.as_path(),
        );
        Ok(())
    }

    pub(crate) async fn load_turn_execution_authorization_context(
        &self,
        turn_id: &str,
    ) -> anyhow::Result<crate::authorization::ExecutionAuthorizationContext> {
        crate::authorization::ExecutionAuthorizationContext::load_for_turn(
            self.crud_store.as_ref(),
            turn_id,
        )
        .await
    }

    fn log_turn_security_snapshot(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        snapshot: &pioneer_protocol::TurnExecutionSecuritySnapshot,
    ) {
        let diagnostic = crate::turn_security::turn_security_diagnostic_summary(snapshot);
        let security_snapshot_id = snapshot.audit_id(turn_id);
        match &snapshot.enforcement {
            pioneer_protocol::TurnSecurityEnforcementStatus::Active => {
                info!(
                    target: "pioneer.security",
                    workspace_id,
                    thread_id,
                    turn_id,
                    security_snapshot_id = security_snapshot_id.as_str(),
                    security_snapshot_version = snapshot.version,
                    security_backend = ?snapshot.backend.execution_backend,
                    sandbox_backend = ?snapshot.backend.sandbox_backend,
                    enforcement_status = diagnostic.enforcement_status,
                    security_diagnostic_code = diagnostic.diagnostic_code,
                    degraded_capabilities = ?diagnostic.degraded_capabilities,
                    "turn execution security snapshot resolved"
                );
            }
            pioneer_protocol::TurnSecurityEnforcementStatus::PartiallyActive { .. }
            | pioneer_protocol::TurnSecurityEnforcementStatus::Unavailable { .. } => {
                warn!(
                    target: "pioneer.security",
                    workspace_id,
                    thread_id,
                    turn_id,
                    security_snapshot_id = security_snapshot_id.as_str(),
                    security_snapshot_version = snapshot.version,
                    security_backend = ?snapshot.backend.execution_backend,
                    sandbox_backend = ?snapshot.backend.sandbox_backend,
                    enforcement_status = diagnostic.enforcement_status,
                    security_diagnostic_code = diagnostic.diagnostic_code,
                    degraded_capabilities = ?diagnostic.degraded_capabilities,
                    "turn execution security snapshot degraded or unavailable"
                );
            }
        }
    }

    pub(super) fn turn_security_audit_events_for_turn(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        snapshot: &pioneer_protocol::TurnExecutionSecuritySnapshot,
    ) -> Vec<pioneer_protocol::TurnPermissionAuditEvent> {
        let mut events = vec![self.turn_security_audit_event_for_turn(
            workspace_id,
            thread_id,
            turn_id,
            snapshot,
            pioneer_protocol::TurnPermissionAuditEventKind::SecuritySnapshotResolved,
            Some("snapshot_resolved"),
            None,
        )];

        match &snapshot.enforcement {
            pioneer_protocol::TurnSecurityEnforcementStatus::Active => {}
            pioneer_protocol::TurnSecurityEnforcementStatus::PartiallyActive { degraded } => {
                events.extend(degraded.iter().map(|degradation| {
                    self.turn_security_audit_event_for_turn(
                        workspace_id,
                        thread_id,
                        turn_id,
                        snapshot,
                        pioneer_protocol::TurnPermissionAuditEventKind::SecuritySandboxDegraded,
                        Some("sandbox_degraded"),
                        Some(degradation.capability),
                    )
                }));
            }
            pioneer_protocol::TurnSecurityEnforcementStatus::Unavailable { .. } => {
                events.push(self.turn_security_audit_event_for_turn(
                    workspace_id,
                    thread_id,
                    turn_id,
                    snapshot,
                    pioneer_protocol::TurnPermissionAuditEventKind::SecuritySandboxUnavailable,
                    Some("sandbox_unavailable"),
                    None,
                ));
            }
        }

        events
    }

    fn turn_security_audit_event_for_turn(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        snapshot: &pioneer_protocol::TurnExecutionSecuritySnapshot,
        event_kind: pioneer_protocol::TurnPermissionAuditEventKind,
        security_reason_code: Option<&str>,
        security_capability: Option<pioneer_protocol::TurnSecurityCapabilityKind>,
    ) -> pioneer_protocol::TurnPermissionAuditEvent {
        pioneer_protocol::TurnPermissionAuditEvent {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            event_kind,
            profile_mode: snapshot.permission_profile.mode,
            profile_source: snapshot.permission_profile.source,
            security_snapshot_id: Some(snapshot.audit_id(turn_id)),
            security_snapshot_version: Some(snapshot.version),
            security_reason_code: security_reason_code.map(str::to_owned),
            security_capability,
            item_id: None,
            tool_call_id: None,
            tool_name: None,
            action_kind: None,
            request_key: None,
            decision: None,
            reason: None,
            cached: false,
        }
    }

    pub(super) fn turn_profile_selected_audit_event(
        &self,
        outcome: &crate::thread::TurnStartOutcome,
    ) -> anyhow::Result<pioneer_protocol::TurnPermissionAuditEvent> {
        let permission_profile =
            self.materialized_turn_permission_profile(&outcome.materialization.turn)?;
        Ok(self.turn_profile_selected_audit_event_for_turn(
            outcome.started_notification.workspace_id.as_str(),
            outcome.started_notification.thread_id.as_str(),
            outcome.started_notification.turn.id.as_str(),
            permission_profile,
        ))
    }

    pub(super) fn turn_profile_selected_audit_event_for_turn(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        permission_profile: pioneer_protocol::TurnPermissionProfileSnapshot,
    ) -> pioneer_protocol::TurnPermissionAuditEvent {
        pioneer_protocol::TurnPermissionAuditEvent {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            event_kind: pioneer_protocol::TurnPermissionAuditEventKind::ProfileSelected,
            profile_mode: permission_profile.mode,
            profile_source: permission_profile.source,
            security_snapshot_id: None,
            security_snapshot_version: None,
            security_reason_code: None,
            security_capability: None,
            item_id: None,
            tool_call_id: None,
            tool_name: None,
            action_kind: None,
            request_key: None,
            decision: None,
            reason: None,
            cached: false,
        }
    }

    async fn compile_cli_runtime_delivery_plan_for_turn(
        &self,
        runtime_id: &str,
        runtime_kind: CLIAgentRuntimeKind,
        outcome: &crate::thread::TurnStartOutcome,
        continuation_thread_id: &str,
        context_thread_id: &str,
        mcp_projection: Option<&crate::turn_mcp::ResolvedMcpTurnProjection>,
        selected_skill_names: &[String],
        frozen_history: Option<&[ChatMessage]>,
    ) -> anyhow::Result<pioneer_promt::CompiledInstructionDeliveryPlan> {
        let native_cwd = self
            .crud_store
            .get_cli_runtime_thread_binding(continuation_thread_id)
            .await?
            .and_then(|binding| binding.native_cwd);
        let history = match frozen_history {
            Some(history) => history.to_vec(),
            None => {
                self.load_conversation_history_for_workspace(
                    outcome.started_notification.workspace_id.as_str(),
                    context_thread_id,
                    outcome.started_notification.turn.id.as_str(),
                )
                .await
            }
        };
        let permission_profile =
            self.materialized_turn_permission_profile(&outcome.materialization.turn)?;
        crate::cli_runtime::context::compile_cli_runtime_delivery_plan(
            self.artifact_runtime_home.as_path(),
            crate::cli_runtime::context::CLIRuntimeContextBuildInput {
                workspace_id: outcome.started_notification.workspace_id.as_str(),
                thread_id: context_thread_id,
                turn_id: outcome.started_notification.turn.id.as_str(),
                runtime_id,
                runtime_label: cli_runtime_context_label(runtime_kind),
                runtime_kind,
                model: Some(outcome.materialization.thread.model.as_str()),
                cwd: native_cwd.as_deref(),
                permission_profile,
                history: history.as_slice(),
                selected_skill_names,
                selected_capabilities:
                    crate::cli_runtime::context::cli_runtime_mcp_capabilities_input(mcp_projection),
            },
        )
    }

    async fn persist_cli_runtime_prompt_manifest(
        &self,
        thread_id: &str,
        turn_id: &str,
        plan: &pioneer_promt::CompiledInstructionDeliveryPlan,
    ) -> anyhow::Result<()> {
        let manifest = crate::cli_runtime::context::cli_runtime_prompt_manifest_from_plan(plan);
        self.thread_manager
            .set_turn_prompt_manifest(thread_id, turn_id, manifest.clone())
            .await;
        self.crud_store
            .update_turn_prompt_manifest(thread_id, turn_id, &manifest, now_timestamp_secs())
            .await
            .with_context(|| {
                format!("failed to update prompt manifest for CLI runtime turn `{turn_id}`")
            })?;
        Ok(())
    }

    async fn persist_cli_runtime_input_mapping_if_thread_bound(
        &self,
        runtime_id: &str,
        runtime_kind: CLIAgentRuntimeKind,
        continuation_thread_id: &str,
        input_mapping_json: String,
        sandbox_json: Option<String>,
        approval_policy: Option<String>,
        outcome: &crate::thread::TurnStartOutcome,
    ) -> anyhow::Result<()> {
        let Some(thread_binding) = self
            .crud_store
            .get_cli_runtime_thread_binding(continuation_thread_id)
            .await?
        else {
            return Ok(());
        };
        let created_at = cli_runtime_binding_timestamp();

        crate::cli_runtime::turn_binding::persist_cli_runtime_turn_binding_before_native_start(
            self.crud_store.as_ref(),
            crate::cli_runtime::turn_binding::CLIAgentRuntimeTurnBindingStartRequest {
                workspace_id: outcome.started_notification.workspace_id.clone(),
                thread_id: outcome.started_notification.thread_id.clone(),
                continuation_thread_id: continuation_thread_id.to_owned(),
                turn_id: outcome.started_notification.turn.id.clone(),
                runtime_id: runtime_id.to_owned(),
                runtime_kind: cli_runtime_protocol_kind_label(runtime_kind).to_owned(),
                native_thread_id: thread_binding.native_thread_id,
                request_id: None,
                model: Some(outcome.materialization.thread.model.clone()),
                cwd: thread_binding.native_cwd,
                sandbox_json,
                approval_policy,
                input_mapping_json,
                created_at,
            },
        )
        .await?;
        Ok(())
    }

    async fn finish_turn_start_success(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        outcome: &crate::thread::TurnStartOutcome,
        user_message_capability_attachments: &[pioneer_protocol::UserMessageAttachment],
    ) -> bool {
        self.session_manager
            .set_connection_workspace(
                connection_id,
                Some(outcome.started_notification.workspace_id.clone()),
            )
            .await;
        if !message_future(
            self.publish_turn_start_success(outcome, user_message_capability_attachments),
        )
        .await
        {
            self.send_error(
                connection_id,
                public_turn_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    pioneer_protocol::PublicErrorStage::Persistence,
                    "failed to commit native turn start lifecycle".to_owned(),
                ),
            )
            .await;
            return false;
        }
        let response = match JsonRpcResponse::from_result(request_id, &outcome.response) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        None,
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Delivery,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return true;
            }
        };
        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send turn/start response"
            );
            return true;
        }
        true
    }

    async fn finish_voice_session_finalize_accepted_turn_start_success(
        &self,
        connection_id: ConnectionId,
        outcome: &crate::thread::TurnStartOutcome,
        user_message_capability_attachments: &[pioneer_protocol::UserMessageAttachment],
        session_id: &str,
    ) -> bool {
        self.session_manager
            .set_connection_workspace(
                connection_id,
                Some(outcome.started_notification.workspace_id.clone()),
            )
            .await;
        if !message_future(
            self.publish_turn_start_success(outcome, user_message_capability_attachments),
        )
        .await
        {
            self.send_voice_session_result_notification(
                connection_id,
                outcome.started_notification.thread_id.as_str(),
                VoiceSessionResultNotification {
                    session_id: session_id.to_owned(),
                    outcome: VoiceSessionOutcome::Failed,
                    turn_id: Some(outcome.started_notification.turn.id.clone()),
                    error: Some(VoiceError {
                        kind: VoiceErrorKind::Unknown,
                        message: "failed to publish CLI runtime turn start".to_owned(),
                        public_error: None,
                    }),
                },
            )
            .await;
            return false;
        }
        self.send_voice_session_result_notification(
            connection_id,
            outcome.started_notification.thread_id.as_str(),
            VoiceSessionResultNotification {
                session_id: session_id.to_owned(),
                outcome: VoiceSessionOutcome::TurnStarted,
                turn_id: Some(outcome.started_notification.turn.id.clone()),
                error: None,
            },
        )
        .await;
        true
    }

    async fn publish_turn_start_success(
        &self,
        outcome: &crate::thread::TurnStartOutcome,
        user_message_capability_attachments: &[pioneer_protocol::UserMessageAttachment],
    ) -> bool {
        if let Err(error) = message_future(self.emit_user_message_item_lifecycle(
            outcome.started_notification.workspace_id.as_str(),
            outcome.started_notification.thread_id.as_str(),
            outcome.started_notification.turn.id.as_str(),
            outcome.materialization.input.as_slice(),
            user_message_capability_attachments,
        ))
        .await
        {
            warn!(
                thread_id = outcome.started_notification.thread_id,
                turn_id = outcome.started_notification.turn.id,
                error = %format!("{error:#}"),
                "failed to commit native turn start item lifecycle"
            );
            return false;
        }
        self.send_notification_to_authorized_thread_connections(
            outcome.started_notification.thread_id.as_str(),
            events::TURN_STARTED,
            &outcome.started_notification,
            outcome.started_notification_connection_ids.clone(),
        )
        .await;
        self.notify_thread_tree_changed(outcome.started_notification.workspace_id.clone())
            .await;

        // Spawn background title generation on first turn (fire-and-forget) only for user-origin threads.
        if outcome.materialization.thread.name.is_none()
            && matches!(
                outcome.materialization.thread.origin_kind,
                pioneer_protocol::ThreadOriginKind::User
                    | pioneer_protocol::ThreadOriginKind::Collaborative
                    | pioneer_protocol::ThreadOriginKind::DirectMessage
            )
        {
            self.spawn_initial_thread_title_task(
                outcome.started_notification.thread_id.clone(),
                first_user_text(outcome.materialization.input.as_slice()),
            );
        }

        true
    }

    pub(super) async fn turn_cancel(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedTurn,
        request_id: RequestId,
        params: TurnCancelParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.thread_id() != params.thread_id.trim()
            || authorization.turn_id() != params.turn_id.trim()
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        if params.thread_id.trim().is_empty() || params.turn_id.trim().is_empty() {
            self.send_error(
                connection_id,
                public_turn_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    pioneer_protocol::PublicErrorStage::Execution,
                    format!(
                        "invalid params for `{}`: `thread_id` and `turn_id` are required",
                        methods::TURN_CANCEL
                    ),
                ),
            )
            .await;
            return;
        }

        let thread_id = params.thread_id.trim().to_owned();
        let turn_id = params.turn_id.trim().to_owned();
        let reason = params
            .reason
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("turn cancelled by user")
            .to_owned();

        let Some((workspace_id, turn)) = self
            .thread_manager
            .turn_get(thread_id.as_str(), turn_id.as_str())
            .await
        else {
            self.send_error(
                connection_id,
                public_turn_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    pioneer_protocol::PublicErrorStage::Execution,
                    format!("turn `{turn_id}` not found in thread `{thread_id}`"),
                ),
            )
            .await;
            return;
        };
        if workspace_id != authorization.workspace_id() {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        if turn.status != TurnStatus::InProgress {
            if turn.status == TurnStatus::Interrupted
                && !self
                    .mark_turn_interrupted(
                        thread_id.clone(),
                        turn_id.clone(),
                        turn.error.clone().unwrap_or_else(|| reason.clone()),
                    )
                    .await
            {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Persistence,
                        format!(
                            "failed to reconcile interrupted turn `{turn_id}` in thread `{thread_id}`"
                        ),
                    ),
                )
                .await;
                return;
            }
            self.send_turn_cancel_response(
                connection_id,
                request_id,
                TurnCancelResponse {
                    thread_id,
                    workspace_id,
                    turn,
                },
            )
            .await;
            return;
        }

        let cancel_intent_key = (thread_id.clone(), turn_id.clone());
        self.user_turn_cancel_intents
            .lock()
            .await
            .insert(cancel_intent_key.clone(), reason.clone());

        if let Err(error) = self
            .cancel_root_agent_work_graph_for_turn(thread_id.as_str(), &turn, reason.as_str())
            .await
        {
            self.user_turn_cancel_intents
                .lock()
                .await
                .remove(&cancel_intent_key);
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                failure_class = "root_agent_work_graph_fence_failed",
                "failed to fence cancelled root Agent work graph"
            );
            self.send_error(
                connection_id,
                public_turn_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    pioneer_protocol::PublicErrorStage::Persistence,
                    "failed to cancel the root Agent work graph".to_owned(),
                ),
            )
            .await;
            return;
        }

        self.mcp_service
            .cancel_turn_mcp_invocations(turn_id.as_str());

        let cli_turn_binding = match self
            .crud_store
            .get_cli_runtime_turn_binding(turn_id.as_str())
            .await
        {
            Ok(binding) => binding,
            Err(error) => {
                self.user_turn_cancel_intents
                    .lock()
                    .await
                    .remove(&cancel_intent_key);
                self.send_error(
                    connection_id,
                    public_turn_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Persistence,
                        format!("failed to load CLI runtime turn binding: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        if let Some(cli_turn_binding) =
            cli_turn_binding.filter(|binding| binding.thread_id == thread_id)
        {
            if !self
                .mark_turn_interrupted(thread_id.clone(), turn_id.clone(), reason.clone())
                .await
            {
                if let Some((workspace_id, turn)) = self
                    .thread_manager
                    .turn_get(thread_id.as_str(), turn_id.as_str())
                    .await
                    .filter(|(_, turn)| turn.status != TurnStatus::InProgress)
                {
                    self.user_turn_cancel_intents
                        .lock()
                        .await
                        .remove(&cancel_intent_key);
                    self.send_turn_cancel_response(
                        connection_id,
                        request_id,
                        TurnCancelResponse {
                            thread_id,
                            workspace_id,
                            turn,
                        },
                    )
                    .await;
                    return;
                }
                self.user_turn_cancel_intents
                    .lock()
                    .await
                    .remove(&cancel_intent_key);
                self.send_error(
                    connection_id,
                    public_turn_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Persistence,
                        format!("failed to interrupt turn `{turn_id}` in thread `{thread_id}`"),
                    ),
                )
                .await;
                return;
            }
            self.ensure_cli_runtime_turn_interrupted_cleanup(
                &cli_turn_binding,
                Some(reason.as_str()),
            )
            .await;
            self.user_turn_cancel_intents
                .lock()
                .await
                .remove(&cancel_intent_key);

            let Some((workspace_id, turn)) = self
                .thread_manager
                .turn_get(thread_id.as_str(), turn_id.as_str())
                .await
            else {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Observation,
                        format!("turn `{turn_id}` disappeared after cancellation"),
                    ),
                )
                .await;
                return;
            };

            self.send_turn_cancel_response(
                connection_id,
                request_id,
                TurnCancelResponse {
                    thread_id,
                    workspace_id,
                    turn,
                },
            )
            .await;
            return;
        }

        let interrupted = self
            .mark_turn_interrupted(thread_id.clone(), turn_id.clone(), reason.clone())
            .await;
        if !interrupted {
            self.user_turn_cancel_intents
                .lock()
                .await
                .remove(&cancel_intent_key);
            // Cancellation races with fail-closed admission/runtime paths.
            // If that path already committed a terminal state, cancellation
            // has reached its intended durable outcome and is idempotently
            // successful. Only an absent or still-running Turn is a
            // persistence failure.
            if let Some((workspace_id, turn)) = self
                .thread_manager
                .turn_get(thread_id.as_str(), turn_id.as_str())
                .await
                .filter(|(_, turn)| turn.status != TurnStatus::InProgress)
            {
                self.send_turn_cancel_response(
                    connection_id,
                    request_id,
                    TurnCancelResponse {
                        thread_id,
                        workspace_id,
                        turn,
                    },
                )
                .await;
                return;
            }
            self.send_error(
                connection_id,
                public_turn_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    pioneer_protocol::PublicErrorStage::Persistence,
                    format!("failed to interrupt turn `{turn_id}` in thread `{thread_id}`"),
                ),
            )
            .await;
            return;
        }

        // The durable lifecycle transition is the cancellation commit point.
        // Runtime signalling is deliberately downstream and best-effort: a
        // provider actor that is blocked while publishing data-plane events
        // cannot make the control-plane request fail or hang.
        match self
            .agent_manager
            .cancel_turn(thread_id.as_str(), turn_id.as_str(), reason.as_str())
            .await
        {
            Ok(()) => {}
            Err(pioneer_agent::AgentControlError::ThreadNotFound)
            | Err(pioneer_agent::AgentControlError::NoActiveTurn) => {
                debug!(
                    thread_id,
                    turn_id, "agent runtime had no active owner after durable cancellation"
                );
            }
            Err(error) => warn!(
                thread_id,
                turn_id,
                error = %error,
                "durable cancellation committed but runtime signalling failed"
            ),
        }
        self.user_turn_cancel_intents
            .lock()
            .await
            .remove(&cancel_intent_key);

        let Some((workspace_id, turn)) = self
            .thread_manager
            .turn_get(thread_id.as_str(), turn_id.as_str())
            .await
        else {
            self.send_error(
                connection_id,
                public_turn_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    pioneer_protocol::PublicErrorStage::Observation,
                    format!("turn `{turn_id}` disappeared after cancellation"),
                ),
            )
            .await;
            return;
        };

        self.send_turn_cancel_response(
            connection_id,
            request_id,
            TurnCancelResponse {
                thread_id,
                workspace_id,
                turn,
            },
        )
        .await;
    }

    pub(super) async fn cancel_task_cli_runtime_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        reason: &str,
    ) -> anyhow::Result<bool> {
        let Some(binding) = self
            .crud_store
            .get_cli_runtime_turn_binding(turn_id)
            .await?
        else {
            return Ok(false);
        };
        if binding.thread_id != thread_id {
            anyhow::bail!(
                "CLI runtime turn binding `{turn_id}` belongs to thread `{}`, not `{thread_id}`",
                binding.thread_id
            );
        }
        self.mcp_service.cancel_turn_mcp_invocations(turn_id);
        if !self
            .mark_turn_interrupted(thread_id.to_owned(), turn_id.to_owned(), reason.to_owned())
            .await
        {
            anyhow::bail!("failed to interrupt CLI runtime turn `{turn_id}`");
        }
        self.ensure_cli_runtime_turn_interrupted_cleanup(&binding, Some(reason))
            .await;
        Ok(true)
    }

    async fn send_turn_cancel_response(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        response_payload: TurnCancelResponse,
    ) {
        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        None,
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Delivery,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send turn/cancel response"
            );
        }
    }

    pub(super) async fn turn_resume(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedTurn,
        request_id: RequestId,
        params: TurnResumeParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.thread_id() != params.thread_id.trim()
            || authorization.turn_id() != params.turn_id.trim()
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        if params.thread_id.trim().is_empty() || params.turn_id.trim().is_empty() {
            self.send_error(
                connection_id,
                public_turn_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    pioneer_protocol::PublicErrorStage::Execution,
                    format!(
                        "invalid params for `{}`: `thread_id` and `turn_id` are required",
                        methods::TURN_RESUME
                    ),
                ),
            )
            .await;
            return;
        }

        let thread_id = params.thread_id.trim().to_owned();
        let turn_id = params.turn_id.trim().to_owned();
        let recovery_job_id = params
            .recovery_job_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);

        let Some((workspace_id, turn)) = (match self
            .crud_store
            .get_turn(thread_id.as_str(), turn_id.as_str())
            .await
        {
            Ok(value) => value,
            Err(error) => {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Observation,
                        format!("failed to fetch turn before resume: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        }) else {
            self.send_error(
                connection_id,
                public_turn_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    pioneer_protocol::PublicErrorStage::Observation,
                    format!("turn `{turn_id}` not found in thread `{thread_id}`"),
                ),
            )
            .await;
            return;
        };
        if workspace_id != authorization.workspace_id() {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        if turn.status != TurnStatus::Blocked {
            self.send_error(
                connection_id,
                public_turn_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    pioneer_protocol::PublicErrorStage::Execution,
                    format!("turn `{turn_id}` is not blocked and cannot be resumed"),
                ),
            )
            .await;
            return;
        }

        let now_unix = now_timestamp_secs();
        // Task-owned child Turns have a second durable owner (TaskRun).  Let
        // the task executor reopen that aggregate and dispatch through the
        // native executor; generic recovery would otherwise revive only the
        // Turn and bypass task locks/execution leases.
        let task_owned_resume = match self
            .task_agent_executor
            .resume_blocked_child_turn(
                thread_id.as_str(),
                turn_id.as_str(),
                recovery_job_id.as_deref(),
                now_unix,
            )
            .await
        {
            Ok(value) => value,
            Err(error) => {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Execution,
                        format!("failed to resume task-owned turn `{turn_id}`: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let resumed_job_id = match task_owned_resume {
            Some(task_agent_executor::TaskChildResumeOutcome::Resumed { recovery_job_id }) => {
                recovery_job_id
            }
            Some(task_agent_executor::TaskChildResumeOutcome::MissingRuntimeSnapshot {
                recovery_job_id,
            }) => {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Execution,
                        format!(
                            "task-owned turn `{turn_id}` cannot resume: durable runtime snapshot `{recovery_job_id}` is missing"
                        ),
                    ),
                )
                .await;
                return;
            }
            Some(task_agent_executor::TaskChildResumeOutcome::Conflict { reason }) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("task-owned turn `{turn_id}` resume blocked by aggregate conflict: {reason}"),
                    ),
                )
                .await;
                return;
            }
            Some(task_agent_executor::TaskChildResumeOutcome::NotFound) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!(
                            "task-owned turn `{turn_id}` has no resumable blocked TaskRun aggregate"
                        ),
                    ),
                )
                .await;
                return;
            }
            None => {
                let resumed_job = match self
                    .recovery_coordinator
                    .resume_blocked_turn(
                        thread_id.as_str(),
                        turn_id.as_str(),
                        recovery_job_id.as_deref(),
                        now_unix,
                    )
                    .await
                {
                    Ok(Some(job)) => job,
                    Ok(None) => {
                        self.send_error(
                            connection_id,
                            JsonRpcErrorResponse::new(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                format!("turn `{turn_id}` has no blocked recovery job to resume"),
                            ),
                        )
                        .await;
                        return;
                    }
                    Err(error) => {
                        self.send_error(
                            connection_id,
                            JsonRpcErrorResponse::new(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                format!("failed to resume turn `{turn_id}`: {error:#}"),
                            ),
                        )
                        .await;
                        return;
                    }
                };

                match self.recovery_coordinator.run_ready_jobs(now_unix, 16).await {
                    Ok(events) => {
                        for event in events {
                            self.handle_recovery_event(event, now_unix).await;
                        }
                    }
                    Err(error) => {
                        self.send_error(
                            connection_id,
                            JsonRpcErrorResponse::new(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                format!(
                                    "turn `{turn_id}` was resumed but recovery start failed: {error:#}"
                                ),
                            ),
                        )
                        .await;
                        return;
                    }
                }
                resumed_job.id
            }
        };

        let turn = match self
            .crud_store
            .get_turn(thread_id.as_str(), turn_id.as_str())
            .await
        {
            Ok(Some((_workspace_id, turn))) => turn,
            Ok(None) => {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Observation,
                        format!("turn `{turn_id}` disappeared after resume"),
                    ),
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Observation,
                        format!("failed to fetch turn after resume: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let response = match JsonRpcResponse::from_result(
            request_id,
            &TurnResumeResponse {
                thread_id,
                workspace_id,
                turn,
                recovery_job_id: resumed_job_id,
            },
        ) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        None,
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Delivery,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send turn/resume response"
            );
        }
    }

    pub(super) async fn turn_get(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedTurn,
        request_id: RequestId,
        params: TurnGetParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.thread_id() != params.thread_id.trim()
            || authorization.turn_id() != params.turn_id.trim()
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        if params.thread_id.trim().is_empty() || params.turn_id.trim().is_empty() {
            self.send_error(
                connection_id,
                public_turn_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    pioneer_protocol::PublicErrorStage::Observation,
                    format!(
                        "invalid params for `{}`: `thread_id` and `turn_id` are required",
                        methods::TURN_GET
                    ),
                ),
            )
            .await;
            return;
        }

        let result = if let Some((workspace_id, turn)) = self
            .thread_manager
            .turn_get(params.thread_id.as_str(), params.turn_id.as_str())
            .await
        {
            Some(TurnGetResponse {
                thread_id: params.thread_id.clone(),
                workspace_id,
                turn,
            })
        } else {
            match self
                .crud_store
                .get_turn(params.thread_id.as_str(), params.turn_id.as_str())
                .await
            {
                Ok(Some((workspace_id, turn))) => Some(TurnGetResponse {
                    thread_id: params.thread_id.clone(),
                    workspace_id,
                    turn,
                }),
                Ok(None) => None,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        public_turn_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            pioneer_protocol::PublicErrorStage::Observation,
                            format!("failed to fetch turn: {error:#}"),
                        ),
                    )
                    .await;
                    return;
                }
            }
        };

        let Some(result) = result else {
            self.send_error(
                connection_id,
                public_turn_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    pioneer_protocol::PublicErrorStage::Observation,
                    format!(
                        "turn `{}` in thread `{}` was not found",
                        params.turn_id, params.thread_id
                    ),
                ),
            )
            .await;
            return;
        };
        if result.workspace_id != authorization.workspace_id() {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        self.session_manager
            .set_connection_workspace(connection_id, Some(result.workspace_id.clone()))
            .await;

        let response = match JsonRpcResponse::from_result(request_id, &result) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        None,
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Delivery,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send turn/get response"
            );
        }
    }

    pub(super) async fn turn_items_page(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedTurn,
        request_id: RequestId,
        params: TurnItemsParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.thread_id() != params.thread_id.trim()
            || authorization.turn_id() != params.turn_id.trim()
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        if params.thread_id.trim().is_empty() || params.turn_id.trim().is_empty() {
            self.send_error(
                connection_id,
                public_turn_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    pioneer_protocol::PublicErrorStage::Observation,
                    format!(
                        "invalid params for `{}`: `thread_id` and `turn_id` are required",
                        methods::TURN_ITEMS_PAGE
                    ),
                ),
            )
            .await;
            return;
        }

        if params.after_sequence.is_some_and(|cursor| cursor < 0) {
            self.send_error(
                connection_id,
                public_turn_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    pioneer_protocol::PublicErrorStage::Observation,
                    "turn/items/page cursor must be non-negative".to_owned(),
                ),
            )
            .await;
            return;
        }

        let principal = request_context.principal();
        let policy_service = AuthorizationService::new();
        let Some(role_key) =
            policy_service.resolved_role_key(principal.kind, principal.role_key.as_ref())
        else {
            self.send_error(
                connection_id,
                AuthorizationExternalError::Unavailable.response(request_id),
            )
            .await;
            return;
        };
        let Some(observation_policy) =
            policy_service.observation_resource_policy(principal.kind, principal.role_key.as_ref())
        else {
            self.send_error(
                connection_id,
                AuthorizationExternalError::Unavailable.response(request_id),
            )
            .await;
            return;
        };
        let _observation_permit = match self
            .observation_governor
            .acquire_page(
                principal.principal_id.as_str(),
                role_key,
                authorization.workspace_id(),
                observation_policy,
            )
            .await
        {
            Ok(permit) => permit,
            Err(error) => {
                warn!(
                    principal_id = %principal.principal_id,
                    workspace_id = authorization.workspace_id(),
                    error = %format!("{error:#}"),
                    "turn observation page rejected by resource governor"
                );
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::Unavailable.response(request_id),
                )
                .await;
                return;
            }
        };
        let max_page_items = (observation_policy.max_turn_page_items as usize).max(1);
        let max_page_bytes = observation_policy.max_turn_page_bytes.max(1);
        let limit = params
            .limit
            .map(|limit| limit as usize)
            .unwrap_or(max_page_items)
            .min(max_page_items)
            .max(1);
        let result = match self
            .crud_store
            .get_turn_item_events_page(
                params.thread_id.as_str(),
                params.turn_id.as_str(),
                params.after_sequence,
                limit,
            )
            .await
        {
            Ok(Some(value)) => value,
            Ok(None) => {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Observation,
                        format!(
                            "turn `{}` in thread `{}` was not found",
                            params.turn_id, params.thread_id
                        ),
                    ),
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Observation,
                        format!("failed to fetch turn items: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        if result.workspace_id != authorization.workspace_id() {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        let mut result = result;
        Self::enrich_turn_item_events_markdown(result.events.as_mut_slice());
        let result = match fit_turn_items_page_to_budget(
            result,
            params.after_sequence,
            max_page_items,
            max_page_bytes,
        ) {
            Ok(result) => result,
            Err(error) => {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Observation,
                        error,
                    ),
                )
                .await;
                return;
            }
        };

        self.session_manager
            .set_connection_workspace(connection_id, Some(result.workspace_id.clone()))
            .await;

        let response = match JsonRpcResponse::from_result(request_id, &result) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    public_turn_error(
                        None,
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Delivery,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send turn/items response"
            );
        }
    }

    async fn lookup_reasoning_model_capabilities(
        &self,
        workspace_id: &str,
        backend: ReasoningModelLookupBackend<'_>,
        model_id: &str,
    ) -> Option<ProviderModelInfo> {
        let model_id = model_id.trim();
        if model_id.is_empty() {
            debug!(
                workspace_id,
                "skipping reasoning model capability lookup because model id is empty"
            );
            return None;
        }

        let model = match backend {
            ReasoningModelLookupBackend::ApiProvider { provider } => {
                self.lookup_api_provider_model_for_reasoning(workspace_id, provider, model_id)
                    .await
            }
            ReasoningModelLookupBackend::CliRuntime {
                runtime_id,
                runtime_kind,
            } => {
                self.lookup_cli_runtime_model_for_reasoning(
                    workspace_id,
                    runtime_id,
                    runtime_kind,
                    model_id,
                )
                .await
            }
        };

        match model
            .as_ref()
            .and_then(|model| model.capabilities.reasoning.as_ref())
        {
            Some(reasoning) => {
                debug!(
                    workspace_id,
                    model_id,
                    supported = ?reasoning.supported,
                    efforts = ?reasoning.effort_options,
                    source = reasoning_capability_source_label(reasoning.source),
                    "resolved reasoning model capability metadata"
                );
            }
            None if model.is_some() => {
                debug!(
                    workspace_id,
                    model_id,
                    source = reasoning_capability_source_label(None),
                    "resolved model but reasoning capability metadata is missing"
                );
            }
            None => {
                debug!(
                    workspace_id,
                    model_id,
                    source = reasoning_capability_source_label(None),
                    "reasoning model capability metadata is unavailable"
                );
            }
        }

        model
    }

    async fn resolve_turn_reasoning_effort(
        &self,
        workspace_id: &str,
        backend: ReasoningModelLookupBackend<'_>,
        model_id: &str,
        effort: Option<&str>,
    ) -> Result<Option<String>, String> {
        let Some(effort) = effort.map(str::trim).filter(|effort| !effort.is_empty()) else {
            return Ok(None);
        };
        let backend_label = reasoning_model_lookup_backend_label(backend);
        let validation_policy = backend.reasoning_effort_validation_policy();
        let model = self
            .lookup_reasoning_model_capabilities(workspace_id, backend, model_id)
            .await;

        let result = resolve_reasoning_effort_for_model(
            validation_policy,
            backend_label.as_str(),
            model_id,
            effort,
            model.as_ref(),
        )
        .map(Some);
        let capability_source = reasoning_capability_source_for_model(model.as_ref());
        let supported_efforts = reasoning_effort_options_for_model(model.as_ref());
        match &result {
            Ok(_) => {
                debug!(
                    workspace_id,
                    backend = backend_label.as_str(),
                    model_id,
                    effort,
                    capability_source,
                    supported_efforts = ?supported_efforts,
                    "accepted reasoning effort selection"
                );
            }
            Err(message) => {
                debug!(
                    workspace_id,
                    backend = backend_label.as_str(),
                    model_id,
                    effort,
                    capability_source,
                    supported_efforts = ?supported_efforts,
                    error = message.as_str(),
                    "rejected reasoning effort selection"
                );
            }
        }
        result
    }

    async fn lookup_api_provider_model_for_reasoning(
        &self,
        workspace_id: &str,
        provider_id: &str,
        model_id: &str,
    ) -> Option<ProviderModelInfo> {
        let provider = match self
            .provider_registry
            .get_or_create_for_workspace(workspace_id, provider_id)
        {
            Ok(provider) => provider,
            Err(error) => {
                debug!(
                    workspace_id,
                    provider = provider_id,
                    model_id,
                    error = %format!("{error:#}"),
                    "failed to create provider for reasoning capability lookup"
                );
                return None;
            }
        };

        match provider.list_models().await {
            Ok(models) => models
                .into_iter()
                .find(|model| model.id == model_id)
                .or_else(|| {
                    debug!(
                        workspace_id,
                        provider = provider_id,
                        model_id,
                        "provider model list did not contain selected model for reasoning lookup"
                    );
                    None
                }),
            Err(error) => {
                debug!(
                    workspace_id,
                    provider = provider_id,
                    model_id,
                    error = %format!("{error:#}"),
                    "failed to list provider models for reasoning capability lookup"
                );
                None
            }
        }
    }

    async fn lookup_cli_runtime_model_for_reasoning(
        &self,
        workspace_id: &str,
        runtime_id: &str,
        runtime_kind: CLIAgentRuntimeKind,
        model_id: &str,
    ) -> Option<ProviderModelInfo> {
        let runtime_snapshot = match self
            .cli_runtime_probe_snapshot(workspace_id, runtime_id)
            .await
        {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                debug!(
                    workspace_id,
                    runtime_id, model_id, "CLI runtime readiness is absent for reasoning lookup"
                );
                return None;
            }
            Err(error) => {
                debug!(
                    workspace_id,
                    runtime_id,
                    model_id,
                    error = %format!("{error:#}"),
                    "failed to read CLI runtime readiness for reasoning lookup"
                );
                return None;
            }
        };
        if runtime_snapshot.summary.kind != runtime_kind
            || !matches!(runtime_snapshot.summary.status, RuntimeStatus::Ready)
        {
            debug!(
                workspace_id,
                runtime_id,
                requested_kind = cli_runtime_protocol_kind_label(runtime_kind),
                cached_kind = cli_runtime_protocol_kind_label(runtime_snapshot.summary.kind),
                model_id,
                "CLI runtime readiness does not permit reasoning metadata lookup"
            );
            return None;
        }

        let Some(model_snapshot) = runtime_snapshot.models else {
            debug!(
                workspace_id,
                runtime_id, model_id, "Gateway CLI model cache is unavailable for reasoning lookup"
            );
            return None;
        };

        model_snapshot
            .result
            .models
            .into_iter()
            .find(|model| model.id == model_id)
            .map(|model| {
                provider_model_from_runtime_model_for_reasoning_lookup(
                    cli_runtime_provider_key(runtime_id).as_str(),
                    model,
                )
            })
            .or_else(|| {
                debug!(
                    workspace_id,
                    runtime_id,
                    model_id,
                    "CLI runtime model list did not contain selected model for reasoning lookup"
                );
                None
            })
    }
}

#[derive(Clone, Copy)]
enum ReasoningModelLookupBackend<'a> {
    ApiProvider {
        provider: &'a str,
    },
    CliRuntime {
        runtime_id: &'a str,
        runtime_kind: CLIAgentRuntimeKind,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReasoningEffortValidationPolicy {
    // API adapters consume the typed Pioneer ReasoningEffort enum.
    KnownOnly,
    // CLI runtimes consume the exact values advertised by their model metadata.
    MetadataDefined,
}

impl ReasoningModelLookupBackend<'_> {
    fn reasoning_effort_validation_policy(self) -> ReasoningEffortValidationPolicy {
        match self {
            Self::ApiProvider { .. } => ReasoningEffortValidationPolicy::KnownOnly,
            Self::CliRuntime { .. } => ReasoningEffortValidationPolicy::MetadataDefined,
        }
    }
}

fn reasoning_model_lookup_backend_label(backend: ReasoningModelLookupBackend<'_>) -> String {
    match backend {
        ReasoningModelLookupBackend::ApiProvider { provider } => {
            format!("provider `{provider}`")
        }
        ReasoningModelLookupBackend::CliRuntime { runtime_id, .. } => {
            format!("CLI runtime `{runtime_id}`")
        }
    }
}

fn reasoning_capability_source_label(source: Option<ReasoningCapabilitySource>) -> &'static str {
    match source {
        Some(ReasoningCapabilitySource::ProviderMetadata) => "provider_metadata",
        Some(ReasoningCapabilitySource::CliMetadata) => "cli_metadata",
        Some(ReasoningCapabilitySource::StaticRegistry) => "static_registry",
        Some(ReasoningCapabilitySource::ConfigOverride) => "config_override",
        Some(ReasoningCapabilitySource::Unknown) | None => "unknown",
    }
}

fn reasoning_capability_source_for_model(model: Option<&ProviderModelInfo>) -> &'static str {
    reasoning_capability_source_label(
        model
            .and_then(|model| model.capabilities.reasoning.as_ref())
            .and_then(|reasoning| reasoning.source),
    )
}

fn reasoning_effort_options_for_model(model: Option<&ProviderModelInfo>) -> Option<&[String]> {
    model
        .and_then(|model| model.capabilities.reasoning.as_ref())
        .map(|reasoning| reasoning.effort_options.as_slice())
}

fn normalized_reasoning_effort_for_policy(
    policy: ReasoningEffortValidationPolicy,
    value: &str,
) -> Option<String> {
    match policy {
        ReasoningEffortValidationPolicy::KnownOnly => {
            pioneer_protocol::ReasoningEffort::canonical_value(value).map(str::to_owned)
        }
        ReasoningEffortValidationPolicy::MetadataDefined => {
            pioneer_protocol::normalize_metadata_reasoning_effort(value)
        }
    }
}

fn reasoning_effort_comparison_key(value: &str) -> String {
    pioneer_protocol::reasoning_effort_comparison_key(value).unwrap_or_default()
}

fn supported_efforts_for_error(
    reasoning: &ProviderModelReasoningCapabilities,
    policy: ReasoningEffortValidationPolicy,
) -> String {
    let mut effort_options = Vec::new();
    for effort in &reasoning.effort_options {
        let Some(effort) = normalized_reasoning_effort_for_policy(policy, effort.as_str()) else {
            continue;
        };
        if reasoning.mandatory == Some(true)
            && reasoning_effort_comparison_key(effort.as_str()) == "none"
        {
            continue;
        }
        if !effort_options.contains(&effort) {
            effort_options.push(effort);
        }
    }

    if effort_options.is_empty() {
        "unknown".to_owned()
    } else {
        effort_options.join(", ")
    }
}

fn resolve_reasoning_effort_for_model(
    policy: ReasoningEffortValidationPolicy,
    backend_label: &str,
    model_id: &str,
    effort: &str,
    model: Option<&ProviderModelInfo>,
) -> Result<String, String> {
    let normalized_effort = normalized_reasoning_effort_for_policy(policy, effort).ok_or_else(|| {
        format!(
            "reasoning effort `{effort}` is not recognized by Pioneer for {backend_label} model `{model_id}`"
        )
    })?;
    let normalized_effort_key = reasoning_effort_comparison_key(normalized_effort.as_str());
    let Some(model) = model else {
        return Err(format!(
            "reasoning effort `{effort}` cannot be used with {backend_label} model `{model_id}` because model capability metadata is unavailable; capability source: unknown"
        ));
    };

    let Some(reasoning) = model.capabilities.reasoning.as_ref() else {
        return Err(format!(
            "reasoning effort `{effort}` cannot be used with {backend_label} model `{model_id}` because reasoning capability metadata is missing; capability source: unknown"
        ));
    };
    let capability_source = reasoning_capability_source_label(reasoning.source);

    if reasoning.supported == Some(false) {
        return Err(format!(
            "reasoning effort `{effort}` is not supported by {backend_label} model `{model_id}`; supported efforts: {}; capability source: {capability_source}",
            supported_efforts_for_error(reasoning, policy)
        ));
    }

    if reasoning.effort_options.is_empty() {
        return Err(format!(
            "reasoning effort `{effort}` cannot be used with {backend_label} model `{model_id}` because supported reasoning efforts are unknown; capability source: {capability_source}"
        ));
    }

    if reasoning.mandatory == Some(true) && normalized_effort_key == "none" {
        return Err(format!(
            "reasoning effort `{effort}` is not supported by {backend_label} model `{model_id}`; supported efforts: {}; capability source: {capability_source}",
            supported_efforts_for_error(reasoning, policy)
        ));
    }

    let matched_effort = reasoning
        .effort_options
        .iter()
        .filter_map(|supported_effort| {
            let supported_effort =
                normalized_reasoning_effort_for_policy(policy, supported_effort.as_str())?;
            if reasoning.mandatory == Some(true)
                && reasoning_effort_comparison_key(supported_effort.as_str()) == "none"
            {
                return None;
            }
            Some(supported_effort)
        })
        .find(|supported_effort| {
            reasoning_effort_comparison_key(supported_effort.as_str()) == normalized_effort_key
        });
    let Some(matched_effort) = matched_effort else {
        return Err(format!(
            "reasoning effort `{effort}` is not supported by {backend_label} model `{model_id}`; supported efforts: {}; capability source: {capability_source}",
            supported_efforts_for_error(reasoning, policy)
        ));
    };

    Ok(matched_effort)
}

#[cfg(test)]
fn validate_reasoning_effort_for_model(
    backend_label: &str,
    model_id: &str,
    effort: &str,
    model: Option<&ProviderModelInfo>,
) -> Result<(), String> {
    resolve_reasoning_effort_for_model(
        ReasoningEffortValidationPolicy::KnownOnly,
        backend_label,
        model_id,
        effort,
        model,
    )
    .map(|_| ())
}

fn effective_cli_runtime_effort(
    requested_reasoning_effort: Option<&str>,
    cli_runtime_effort: Option<&str>,
) -> Result<Option<String>, String> {
    let requested_reasoning_effort =
        requested_reasoning_effort.map(normalized_reasoning_effort_for_comparison);
    let cli_runtime_effort = cli_runtime_effort.map(normalized_reasoning_effort_for_comparison);

    match (
        requested_reasoning_effort.as_deref(),
        cli_runtime_effort.as_deref(),
    ) {
        (Some(requested), Some(cli)) if requested != cli => Err(format!(
            "CLI runtime reasoning effort conflict: top-level reasoning effort `{requested}` does not match cli_runtime_options effort `{cli}`"
        )),
        (Some(requested), _) => Ok(Some(requested.to_owned())),
        (None, Some(cli)) => Ok(Some(cli.to_owned())),
        (None, None) => Ok(None),
    }
}

fn normalized_reasoning_effort_for_comparison(value: &str) -> String {
    reasoning_effort_comparison_key(value)
}

fn provider_model_from_runtime_model_for_reasoning_lookup(
    provider_key: &str,
    model: RuntimeModelInfo,
) -> ProviderModelInfo {
    let supports_reasoning = model
        .supports_reasoning
        .or_else(|| (!model.effort_options.is_empty()).then_some(true));
    let reasoning = supports_reasoning.map(|supported| ProviderModelReasoningCapabilities {
        supported: Some(supported),
        effort_options: model.effort_options.clone(),
        default_effort: None,
        mandatory: None,
        supports_token_budget: None,
        source: Some(ReasoningCapabilitySource::CliMetadata),
    });

    ProviderModelInfo {
        id: model.id,
        name: model.name,
        description: model.description,
        created: None,
        provider: provider_key.to_owned(),
        owned_by: None,
        limits: ProviderModelLimits {
            max_input_tokens: model.max_input_tokens,
            max_output_tokens: model.max_output_tokens,
            context_window: model.max_input_tokens,
        },
        capabilities: ProviderModelCapabilities {
            vision: model.supports_vision,
            tool_calling: None,
            json_output: None,
            streaming: Some(true),
            embeddings: None,
            transcription: None,
            thinking: supports_reasoning,
            reasoning,
            fine_tuning: None,
            input_modalities: (!model.input_modalities.is_empty())
                .then_some(model.input_modalities),
            output_modalities: (!model.output_modalities.is_empty())
                .then_some(model.output_modalities),
        },
        transcription: None,
        pricing: None,
        active: model.active,
        family: model.family,
        lifecycle_status: None,
    }
}

pub(super) fn requested_reasoning_effort(params: &TurnStartParams) -> Option<String> {
    params
        .reasoning
        .as_ref()
        .map(|reasoning| reasoning.effort.clone())
}

fn cli_runtime_effort(params: &TurnStartParams) -> Option<String> {
    params
        .cli_runtime_options
        .as_ref()
        .and_then(|options| options.effort.clone())
}

fn cli_runtime_thread_sandbox_label(sandbox_policy: &JsonValue) -> String {
    let raw = sandbox_policy
        .as_str()
        .or_else(|| sandbox_policy.get("type").and_then(JsonValue::as_str))
        .unwrap_or("workspace-write");
    match normalize_cli_runtime_sandbox_label(raw).as_str() {
        "dangerfullaccess" | "fullaccess" | "dangerfull" => "danger-full-access".to_owned(),
        "readonly" | "read" => "read-only".to_owned(),
        "workspacewrite" | "workspace" | "write" => "workspace-write".to_owned(),
        _ => raw.trim().to_owned(),
    }
}

fn normalize_cli_runtime_sandbox_label(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn cli_runtime_provider_key(runtime_id: &str) -> String {
    format!("cli_runtime:{}", runtime_id.trim())
}

fn canonicalize_cli_runtime_model_provider(params: &mut TurnStartParams) {
    if let Some(AgentExecutionBackend::CLIAgentRuntime { runtime_id, .. }) =
        params.execution_backend.as_ref()
    {
        params.model_provider = Some(cli_runtime_provider_key(runtime_id));
    }
}

fn cli_mcp_client_target(
    runtime_kind: CLIAgentRuntimeKind,
) -> crate::cli_mcp_client_validation::CliMcpClientTarget {
    match runtime_kind {
        CLIAgentRuntimeKind::Codex => crate::cli_mcp_client_validation::CliMcpClientTarget::Codex,
        CLIAgentRuntimeKind::Claude => crate::cli_mcp_client_validation::CliMcpClientTarget::Claude,
    }
}

fn cli_mcp_projection_resolves_all_explicit_capabilities(
    capabilities: &crate::cli_runtime::skills::CliRuntimeCapabilityPartition,
    projection: &crate::turn_mcp::ResolvedMcpTurnProjection,
) -> bool {
    let requested = capabilities
        .mcp_servers
        .iter()
        .map(|capability| capability.capability_id.as_str())
        .chain(
            capabilities
                .mcp_tools
                .iter()
                .map(|capability| capability.capability_id.as_str()),
        )
        .collect::<std::collections::HashSet<_>>();
    let accepted = projection
        .accepted_capabilities
        .iter()
        .map(|capability| capability.id.as_str())
        .collect::<std::collections::HashSet<_>>();

    projection.rejected_capabilities.is_empty() && requested == accepted
}

fn cli_runtime_kind_matches_config(
    protocol_kind: CLIAgentRuntimeKind,
    config_kind: pioneer_config::GatewayCliAgentRuntimeKindConfig,
) -> bool {
    matches!(
        (protocol_kind, config_kind),
        (
            CLIAgentRuntimeKind::Codex,
            pioneer_config::GatewayCliAgentRuntimeKindConfig::Codex
        ) | (
            CLIAgentRuntimeKind::Claude,
            pioneer_config::GatewayCliAgentRuntimeKindConfig::Claude
        )
    )
}

fn cli_runtime_protocol_kind_label(kind: CLIAgentRuntimeKind) -> &'static str {
    match kind {
        CLIAgentRuntimeKind::Codex => "codex",
        CLIAgentRuntimeKind::Claude => "claude",
    }
}

fn cli_runtime_context_label(kind: CLIAgentRuntimeKind) -> &'static str {
    match kind {
        CLIAgentRuntimeKind::Codex => "Codex CLI",
        CLIAgentRuntimeKind::Claude => "Claude CLI",
    }
}

fn cli_runtime_supports_durable_thread_resume(kind: CLIAgentRuntimeKind) -> bool {
    match kind {
        CLIAgentRuntimeKind::Codex => true,
        CLIAgentRuntimeKind::Claude => false,
    }
}

fn cli_runtime_config_kind_label(
    kind: pioneer_config::GatewayCliAgentRuntimeKindConfig,
) -> &'static str {
    match kind {
        pioneer_config::GatewayCliAgentRuntimeKindConfig::Codex => "codex",
        pioneer_config::GatewayCliAgentRuntimeKindConfig::Claude => "claude",
    }
}

fn cli_runtime_binding_timestamp() -> sea_orm::entity::prelude::DateTimeWithTimeZone {
    use chrono::{FixedOffset, TimeZone};

    FixedOffset::east_opt(0)
        .expect("UTC offset should exist")
        .timestamp_opt(now_timestamp_secs(), 0)
        .single()
        .expect("current timestamp should be valid")
}

fn cli_runtime_unavailable_reason(status: &RuntimeStatus) -> &'static str {
    match status {
        RuntimeStatus::Disabled => "runtime is disabled",
        RuntimeStatus::MissingBinary { .. } => "runtime binary is unavailable",
        RuntimeStatus::SpawnFailed { .. } => "runtime failed to start",
        RuntimeStatus::Initializing => "Gateway readiness check is still running",
        RuntimeStatus::NeedsAuth => "runtime authentication is required",
        RuntimeStatus::Ready => "runtime is ready",
        RuntimeStatus::Degraded { .. } => "runtime readiness is degraded",
        RuntimeStatus::UnsupportedVersion { .. } => "runtime version is unsupported",
        RuntimeStatus::Error { .. } => "runtime readiness check failed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detached_task_creator_keeps_the_exact_request_actor() {
        let principal = pioneer_protocol::PersistedActorRef::Principal(
            pioneer_protocol::PrincipalId::new("P00000000000000000001").unwrap(),
        );
        let execution = pioneer_protocol::PersistedActorRef::AgentExecution(
            pioneer_protocol::AgentExecutionId::new("E00000000000000000001").unwrap(),
        );

        assert_eq!(
            task_creator_actor_id(&principal).as_deref(),
            Some("P00000000000000000001")
        );
        assert_eq!(
            task_creator_actor_id(&execution).as_deref(),
            Some("E00000000000000000001")
        );
        assert_eq!(
            task_creator_actor_id(&pioneer_protocol::PersistedActorRef::System),
            None
        );
    }

    #[test]
    fn root_agent_launch_matches_normalized_skill_and_scope_qualified_mcp_authority() {
        let skill_id = pioneer_protocol::SkillId::new("K12345678901234567890").unwrap();
        let scope = pioneer_protocol::McpScopeKind::Workspace;
        let server_id = pioneer_protocol::mcp_server_capability_key(scope, "github");
        let mut params = root_agent_launch_params(
            vec![
                pioneer_protocol::TurnCapability {
                    id: pioneer_protocol::skill_capability_key(&skill_id),
                    label: None,
                    kind: pioneer_protocol::TurnCapabilityKind::Skill {
                        skill_id: skill_id.clone(),
                        pack_id: None,
                    },
                },
                pioneer_protocol::TurnCapability {
                    id: pioneer_protocol::mcp_tool_capability_key(scope, "github", "issues"),
                    label: None,
                    kind: pioneer_protocol::TurnCapabilityKind::McpTool {
                        server_name: "github".to_owned(),
                        raw_tool_name: "issues".to_owned(),
                        scope_kind: scope,
                    },
                },
            ],
            vec![skill_id],
            vec![server_id],
        );

        validate_root_agent_launch_matches_turn(&params).unwrap();
        validate_root_agent_launch_capabilities(&params).unwrap();

        params
            .agent_launch
            .as_mut()
            .unwrap()
            .execution
            .mcp_server_ids = vec![pioneer_protocol::mcp_tool_capability_key(
            scope, "github", "issues",
        )];
        assert!(validate_root_agent_launch_capabilities(&params).is_err());
    }

    #[test]
    fn root_agent_launch_rejects_non_agent_mode_and_duplicate_capability_grants() {
        let server_id = pioneer_protocol::mcp_server_capability_key(
            pioneer_protocol::McpScopeKind::User,
            "github",
        );
        let mut params = root_agent_launch_params(Vec::new(), Vec::new(), Vec::new());
        params.mode = Some(pioneer_protocol::ThreadMode::Message);
        assert!(validate_root_agent_launch_matches_turn(&params).is_err());

        params.mode = Some(pioneer_protocol::ThreadMode::Agent);
        params
            .agent_launch
            .as_mut()
            .unwrap()
            .execution
            .mcp_server_ids = vec![server_id.clone(), server_id];
        assert!(validate_root_agent_launch_capabilities(&params).is_err());
    }

    fn root_agent_launch_params(
        capabilities: Vec<pioneer_protocol::TurnCapability>,
        skill_ids: Vec<pioneer_protocol::SkillId>,
        mcp_server_ids: Vec<String>,
    ) -> TurnStartParams {
        let reasoning = pioneer_protocol::TurnReasoningSelection {
            effort: "medium".to_owned(),
        };
        let permission_profile = pioneer_protocol::TurnPermissionProfileSelection {
            mode: pioneer_protocol::TurnPermissionMode::Supervised,
        };
        TurnStartParams {
            agent_delegation_routes: Vec::new(),
            thread_id: "thread-root".to_owned(),
            turn_id: "turn-root".to_owned(),
            input: Vec::new(),
            capabilities,
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: Some(pioneer_protocol::ThreadMode::Agent),
            agent_launch: Some(pioneer_protocol::AgentLaunchSelection {
                agent: pioneer_protocol::AgentIdentitySelection::DefaultPioneer,
                execution: pioneer_protocol::AgentExecutionSelection {
                    profile: pioneer_protocol::AgentExecutionProfileSelection::Exact {
                        profile_id: pioneer_protocol::AgentExecutionProfileId::new(
                            "P12345678901234567890",
                        )
                        .unwrap(),
                    },
                    reasoning: Some(reasoning.clone()),
                    permission_profile: Some(permission_profile.clone()),
                    skill_ids,
                    mcp_server_ids,
                },
            }),
            reply_to_turn_id: None,
            mentioned_principal_ids: Vec::new(),
            execution_backend: None,
            reasoning: Some(reasoning),
            permission_profile: Some(permission_profile),
            cli_runtime_options: None,
        }
    }

    fn observation_event(sequence: i64, message_bytes: usize) -> pioneer_protocol::TurnItemEvent {
        pioneer_protocol::TurnItemEvent {
            sequence,
            created_at: sequence,
            payload: pioneer_protocol::TurnItemEventPayload::ItemCompleted {
                workspace_id: "workspace-a".to_owned(),
                thread_id: "thread-a".to_owned(),
                turn_id: "turn-a".to_owned(),
                item: pioneer_protocol::TurnItem::SystemEvent {
                    id: format!("item-{sequence}"),
                    level: pioneer_protocol::SystemEventLevel::Info,
                    message: "x".repeat(message_bytes),
                    code: None,
                    details: None,
                },
            },
        }
    }

    fn observation_page(
        event_count: i64,
        message_bytes: usize,
    ) -> pioneer_protocol::TurnItemsResponse {
        pioneer_protocol::TurnItemsResponse {
            thread_id: "thread-a".to_owned(),
            workspace_id: "workspace-a".to_owned(),
            turn_id: "turn-a".to_owned(),
            events: (1..=event_count)
                .map(|sequence| observation_event(sequence, message_bytes))
                .collect(),
            last_sequence: event_count,
            has_more: false,
            next_cursor: None,
        }
    }

    #[test]
    fn turn_items_page_budget_preserves_progress_and_never_truncates_an_event() {
        let page = observation_page(3, 128);
        let mut two_event_prefix = page.clone();
        two_event_prefix.events.truncate(2);
        two_event_prefix.last_sequence = 2;
        two_event_prefix.has_more = true;
        two_event_prefix.next_cursor = Some(2);
        let budget = serde_json::to_vec(&two_event_prefix).unwrap().len();

        let fitted = fit_turn_items_page_to_budget(page, None, 200, budget)
            .expect("two-event prefix should fit exactly");
        assert_eq!(fitted.events.len(), 2);
        assert_eq!(fitted.next_cursor, Some(2));

        let oversized = fit_turn_items_page_to_budget(observation_page(2, 4_096), None, 200, 256)
            .expect("one oversized event must remain readable");
        assert_eq!(oversized.events.len(), 1);
        assert_eq!(oversized.next_cursor, Some(1));
        assert_eq!(
            oversized.events[0].payload,
            observation_event(1, 4_096).payload
        );
    }

    #[test]
    fn agent_skill_overlay_is_native_api_only() {
        assert!(execution_backend_allows_agent_skill_overlay(None));
        assert!(execution_backend_allows_agent_skill_overlay(Some(
            &AgentExecutionBackend::ApiProvider {
                provider: "openai".to_owned(),
            },
        )));
        assert!(!execution_backend_allows_agent_skill_overlay(Some(
            &AgentExecutionBackend::CLIAgentRuntime {
                runtime_id: "codex".to_owned(),
                runtime_kind: CLIAgentRuntimeKind::Codex,
            },
        )));
        assert!(!execution_backend_allows_agent_skill_overlay(Some(
            &AgentExecutionBackend::ACPAgentRuntime {
                runtime_id: "acp".to_owned(),
            },
        )));
    }

    fn reasoning_test_model(
        reasoning: Option<ProviderModelReasoningCapabilities>,
    ) -> ProviderModelInfo {
        ProviderModelInfo {
            id: "model-a".to_owned(),
            name: None,
            description: None,
            created: None,
            provider: "provider-a".to_owned(),
            owned_by: None,
            limits: ProviderModelLimits {
                max_input_tokens: None,
                max_output_tokens: None,
                context_window: None,
            },
            capabilities: ProviderModelCapabilities {
                vision: None,
                tool_calling: None,
                json_output: None,
                streaming: None,
                embeddings: None,
                thinking: None,
                reasoning,
                fine_tuning: None,
                input_modalities: None,
                output_modalities: None,
                transcription: None,
            },
            pricing: None,
            active: None,
            family: None,
            lifecycle_status: None,
            transcription: None,
        }
    }

    fn reasoning_capabilities(
        supported: Option<bool>,
        effort_options: &[&str],
    ) -> ProviderModelReasoningCapabilities {
        ProviderModelReasoningCapabilities {
            supported,
            effort_options: effort_options
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            default_effort: None,
            mandatory: None,
            supports_token_budget: None,
            source: Some(ReasoningCapabilitySource::StaticRegistry),
        }
    }

    #[test]
    fn reasoning_effort_validation_rejects_missing_or_unknown_metadata() {
        let missing =
            validate_reasoning_effort_for_model("provider `openai`", "unknown-model", "high", None)
                .expect_err("missing model metadata should reject selected effort");
        assert!(missing.contains("metadata is unavailable"));

        let model_without_reasoning = reasoning_test_model(None);
        let absent = validate_reasoning_effort_for_model(
            "provider `openai`",
            "model-a",
            "high",
            Some(&model_without_reasoning),
        )
        .expect_err("missing reasoning capability should reject selected effort");
        assert!(absent.contains("reasoning capability metadata is missing"));

        let model_without_efforts =
            reasoning_test_model(Some(reasoning_capabilities(Some(true), &[])));
        let unknown = validate_reasoning_effort_for_model(
            "provider `openai`",
            "model-a",
            "high",
            Some(&model_without_efforts),
        )
        .expect_err("empty effort list should reject selected effort");
        assert!(unknown.contains("supported reasoning efforts are unknown"));
    }

    #[test]
    fn reasoning_effort_validation_rejects_unsupported_model_or_value() {
        let unsupported_model =
            reasoning_test_model(Some(reasoning_capabilities(Some(false), &["low", "high"])));
        let error = validate_reasoning_effort_for_model(
            "provider `openai`",
            "model-a",
            "high",
            Some(&unsupported_model),
        )
        .expect_err("unsupported model should reject selected effort");
        assert!(error.contains("is not supported"));

        let unsupported_value =
            reasoning_test_model(Some(reasoning_capabilities(Some(true), &["low", "medium"])));
        let error = validate_reasoning_effort_for_model(
            "provider `openai`",
            "model-a",
            "high",
            Some(&unsupported_value),
        )
        .expect_err("unsupported value should reject selected effort");
        assert!(error.contains("supported efforts: low, medium"));
    }

    #[test]
    fn reasoning_effort_validation_error_includes_debuggable_context() {
        let model =
            reasoning_test_model(Some(reasoning_capabilities(Some(true), &["low", "medium"])));

        let error = validate_reasoning_effort_for_model(
            "provider `openai`",
            "model-a",
            "high",
            Some(&model),
        )
        .expect_err("unsupported value should reject selected effort");

        assert_eq!(
            error,
            "reasoning effort `high` is not supported by provider `openai` model `model-a`; supported efforts: low, medium; capability source: static_registry"
        );
    }

    #[test]
    fn reasoning_effort_validation_accepts_known_effort() {
        let model = reasoning_test_model(Some(reasoning_capabilities(
            Some(true),
            &["low", "medium", "high"],
        )));

        validate_reasoning_effort_for_model("provider `openai`", "model-a", "medium", Some(&model))
            .expect("known effort should pass validation");
    }

    #[test]
    fn reasoning_effort_validation_accepts_known_aliases() {
        let model = reasoning_test_model(Some(reasoning_capabilities(
            Some(true),
            &["low", "extra-high", "maximum"],
        )));

        validate_reasoning_effort_for_model("provider `openai`", "model-a", "xhigh", Some(&model))
            .expect("canonical effort should match provider alias");
        validate_reasoning_effort_for_model("provider `openai`", "model-a", "max", Some(&model))
            .expect("canonical max should match provider alias");
    }

    #[test]
    fn reasoning_effort_validation_rejects_unknown_provider_effort_values() {
        let model = reasoning_test_model(Some(reasoning_capabilities(
            Some(true),
            &["low", "turbo-high"],
        )));

        validate_reasoning_effort_for_model("provider `openai`", "model-a", "low", Some(&model))
            .expect("known effort should pass validation");
        let error = validate_reasoning_effort_for_model(
            "provider `openai`",
            "model-a",
            "turbo-high",
            Some(&model),
        )
        .expect_err("unknown effort should be rejected even if metadata reports it");

        assert_eq!(
            error,
            "reasoning effort `turbo-high` is not recognized by Pioneer for provider `openai` model `model-a`"
        );
    }

    #[test]
    fn cli_reasoning_effort_validation_accepts_metadata_defined_values() {
        let mut model = reasoning_test_model(Some(reasoning_capabilities(
            Some(true),
            &["low", "high", "xhigh", "max", "ultra"],
        )));
        model
            .capabilities
            .reasoning
            .as_mut()
            .expect("reasoning capabilities")
            .source = Some(ReasoningCapabilitySource::CliMetadata);

        let resolved = resolve_reasoning_effort_for_model(
            ReasoningEffortValidationPolicy::MetadataDefined,
            "CLI runtime `codex`",
            "gpt-5.6-sol",
            "Ultra",
            Some(&model),
        )
        .expect("runtime-defined effort should pass validation");

        assert_eq!(resolved, "ultra");
    }

    #[test]
    fn api_reasoning_effort_validation_keeps_closed_provider_contract() {
        let model =
            reasoning_test_model(Some(reasoning_capabilities(Some(true), &["low", "ultra"])));

        let error = resolve_reasoning_effort_for_model(
            ReasoningEffortValidationPolicy::KnownOnly,
            "provider `openai`",
            "gpt-future",
            "ultra",
            Some(&model),
        )
        .expect_err("API providers require an implemented Pioneer effort mapping");

        assert!(error.contains("is not recognized by Pioneer"));
    }

    #[test]
    fn reasoning_effort_validation_rejects_none_for_mandatory_reasoning() {
        let mut model = reasoning_test_model(Some(reasoning_capabilities(
            Some(true),
            &["none", "low", "medium"],
        )));
        model
            .capabilities
            .reasoning
            .as_mut()
            .expect("reasoning capabilities")
            .mandatory = Some(true);

        let error = validate_reasoning_effort_for_model(
            "provider `openrouter`",
            "model-a",
            "none",
            Some(&model),
        )
        .expect_err("mandatory reasoning should reject none");

        assert!(error.contains("supported efforts: low, medium"));
    }

    #[test]
    fn effective_cli_runtime_effort_accepts_legacy_top_level_or_matching_values() {
        assert_eq!(
            effective_cli_runtime_effort(None, Some("high")).expect("legacy effort"),
            Some("high".to_owned())
        );
        assert_eq!(
            effective_cli_runtime_effort(Some("medium"), None).expect("top-level effort"),
            Some("medium".to_owned())
        );
        assert_eq!(
            effective_cli_runtime_effort(Some("low"), Some("low")).expect("matching efforts"),
            Some("low".to_owned())
        );
        assert_eq!(
            effective_cli_runtime_effort(Some("Extra High"), Some("xhigh"))
                .expect("matching alias efforts"),
            Some("xhigh".to_owned())
        );
        assert_eq!(
            effective_cli_runtime_effort(None, None).expect("no effort"),
            None
        );
        assert_eq!(
            effective_cli_runtime_effort(Some("Ultra"), Some("ultra"))
                .expect("runtime-defined efforts compare case-insensitively"),
            Some("ultra".to_owned())
        );
    }

    #[test]
    fn effective_cli_runtime_effort_rejects_conflicting_values() {
        let error = effective_cli_runtime_effort(Some("high"), Some("low"))
            .expect_err("conflicting CLI efforts should reject");
        assert!(error.contains("top-level reasoning effort `high`"));
        assert!(error.contains("cli_runtime_options effort `low`"));
    }

    #[test]
    fn runtime_model_reasoning_lookup_infers_legacy_thinking_from_efforts() {
        let model = provider_model_from_runtime_model_for_reasoning_lookup(
            "cli_runtime:codex",
            RuntimeModelInfo {
                id: "gpt-5".to_owned(),
                name: Some("GPT 5".to_owned()),
                description: None,
                family: None,
                is_custom: false,
                active: Some(true),
                effort_options: vec!["low".to_owned(), "high".to_owned()],
                input_modalities: Vec::new(),
                output_modalities: Vec::new(),
                supports_reasoning: None,
                supports_vision: None,
                max_input_tokens: None,
                max_output_tokens: None,
            },
        );

        assert_eq!(model.capabilities.thinking, Some(true));
        assert_eq!(
            model
                .capabilities
                .reasoning
                .as_ref()
                .and_then(|reasoning| reasoning.supported),
            Some(true)
        );
    }
}
