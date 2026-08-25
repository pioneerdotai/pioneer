use super::result::CanonicalMcpToolResult;
use async_trait::async_trait;
use pioneer_crud::{CrudStore, TurnMcpBindingRecord};
use pioneer_protocol::TurnStatus;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::authorization::{
    AuthorizationInvalidationHub, AuthorizationResolver, AuthorizationService, CapabilityKind,
    ExecutionAuthorizationContext, ExecutionLeaseRegistry, ProofResolution, ResourceAction,
};

const FIRST_PARTY_FILE_SERVER_ID: &str = "pioneer-file-tools-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TurnMcpInvocationOrigin {
    NativeApi,
    CliFacade,
}

impl TurnMcpInvocationOrigin {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::NativeApi => "native_api",
            Self::CliFacade => "cli_facade",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TurnMcpInvocation {
    pub(crate) workspace_id: String,
    pub(crate) thread_id: String,
    pub(crate) turn_id: String,
    pub(crate) runtime_id: Option<String>,
    pub(crate) session_generation: Option<u64>,
    pub(crate) provider_call_id: String,
    pub(crate) canonical_callable_name: String,
    pub(crate) arguments: JsonValue,
    pub(crate) origin: TurnMcpInvocationOrigin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TurnMcpInvocationErrorCode {
    InvalidRequest,
    ScopeMismatch,
    TurnNotActive,
    ProjectionUnavailable,
    ToolUnbound,
    SessionBindingUnavailable,
    SessionGenerationStale,
    InstallationUnavailable,
    RuntimeNotLive,
    ToolDrift,
    SecuritySnapshotUnavailable,
    PermissionDenied,
    ResourceExhausted,
    Cancelled,
    TimedOut,
    ExecutionFailed,
    ResultInvalid,
    Internal,
}

impl TurnMcpInvocationErrorCode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::ScopeMismatch => "scope_mismatch",
            Self::TurnNotActive => "turn_not_active",
            Self::ProjectionUnavailable => "projection_unavailable",
            Self::ToolUnbound => "tool_unbound",
            Self::SessionBindingUnavailable => "session_binding_unavailable",
            Self::SessionGenerationStale => "session_generation_stale",
            Self::InstallationUnavailable => "installation_unavailable",
            Self::RuntimeNotLive => "runtime_not_live",
            Self::ToolDrift => "tool_drift",
            Self::SecuritySnapshotUnavailable => "security_snapshot_unavailable",
            Self::PermissionDenied => "permission_denied",
            Self::ResourceExhausted => "resource_exhausted",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::ExecutionFailed => "execution_failed",
            Self::ResultInvalid => "result_invalid",
            Self::Internal => "internal",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnMcpInvocationError {
    pub(crate) code: TurnMcpInvocationErrorCode,
    pub(crate) message: String,
}

impl TurnMcpInvocationError {
    pub(crate) fn new(code: TurnMcpInvocationErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub(crate) fn reason_code(&self) -> &'static str {
        self.code.as_str()
    }
}

impl fmt::Display for TurnMcpInvocationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for TurnMcpInvocationError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentMcpToolIdentity {
    pub(crate) server_installation_id: String,
    pub(crate) server_name: String,
    pub(crate) raw_tool_name: String,
    pub(crate) description: Option<String>,
    pub(crate) catalog_version: String,
    pub(crate) installation_fingerprint: String,
    pub(crate) canonical_schema_fingerprint: String,
    pub(crate) canonical_schema: JsonValue,
    pub(crate) annotations_json: String,
    pub(crate) annotations_digest: String,
    pub(crate) effective_timeout_ms: u64,
    pub(crate) runtime_generation: u64,
}

pub(crate) struct ValidatedTurnMcpInvocation {
    pub(crate) invocation: TurnMcpInvocation,
    pub(crate) manifest_hash: String,
    pub(crate) binding: TurnMcpBindingRecord,
    pub(crate) current_tool: CurrentMcpToolIdentity,
    pub(crate) invocation_limits: pioneer_protocol::McpInvocationResourceLimits,
    _concurrency_permit: OwnedSemaphorePermit,
}

#[derive(Default)]
pub(crate) struct McpInvocationGovernor {
    states: Mutex<HashMap<String, Arc<McpInvocationTurnState>>>,
}

struct McpInvocationTurnState {
    capacity: usize,
    semaphore: Arc<Semaphore>,
    queued: AtomicUsize,
}

impl McpInvocationTurnState {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            semaphore: Arc::new(Semaphore::new(capacity)),
            queued: AtomicUsize::new(0),
        }
    }

    fn is_idle(&self) -> bool {
        self.semaphore.available_permits() == self.capacity
            && self.queued.load(Ordering::Acquire) == 0
    }
}

struct McpInvocationQueueGuard {
    state: Arc<McpInvocationTurnState>,
}

impl Drop for McpInvocationQueueGuard {
    fn drop(&mut self) {
        self.state.queued.fetch_sub(1, Ordering::AcqRel);
    }
}

impl McpInvocationGovernor {
    fn state_for(
        &self,
        turn_id: &str,
        capacity: usize,
    ) -> Result<Arc<McpInvocationTurnState>, TurnMcpInvocationError> {
        let mut states = self
            .states
            .lock()
            .map_err(|_| internal_error("MCP invocation governor is unavailable"))?;
        states.retain(|key, state| key == turn_id || !state.is_idle());
        if let Some(state) = states.get(turn_id) {
            if state.capacity == capacity {
                return Ok(state.clone());
            }
            if !state.is_idle() {
                return Err(invocation_error(
                    TurnMcpInvocationErrorCode::ProjectionUnavailable,
                    "MCP invocation resource policy changed while calls were active",
                ));
            }
        }
        let state = Arc::new(McpInvocationTurnState::new(capacity));
        states.insert(turn_id.to_owned(), state.clone());
        Ok(state)
    }

    async fn acquire(
        &self,
        turn_id: &str,
        limits: pioneer_protocol::McpInvocationResourceLimits,
        cancellation: &CancellationToken,
    ) -> Result<OwnedSemaphorePermit, TurnMcpInvocationError> {
        if !limits.is_valid() {
            return Err(internal_error("MCP invocation resource policy is invalid"));
        }
        let state = self.state_for(turn_id, limits.max_concurrent_calls)?;
        if let Ok(permit) = state.semaphore.clone().try_acquire_owned() {
            return Ok(permit);
        }

        state
            .queued
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |queued| {
                (queued < limits.max_queued_calls).then_some(queued + 1)
            })
            .map_err(|_| {
                invocation_error(
                    TurnMcpInvocationErrorCode::ResourceExhausted,
                    "MCP invocation queue is full for this turn",
                )
            })?;
        let queue_guard = McpInvocationQueueGuard {
            state: state.clone(),
        };
        let acquire = tokio::time::timeout(
            Duration::from_millis(limits.max_queue_wait_ms),
            state.semaphore.clone().acquire_owned(),
        );
        let permit = tokio::select! {
            _ = cancellation.cancelled() => Err(invocation_error(
                TurnMcpInvocationErrorCode::Cancelled,
                "MCP invocation was cancelled while waiting for capacity",
            )),
            result = acquire => match result {
                Ok(Ok(permit)) => Ok(permit),
                Ok(Err(_)) => Err(internal_error("MCP invocation governor was closed")),
                Err(_) => Err(invocation_error(
                    TurnMcpInvocationErrorCode::TimedOut,
                    "MCP invocation capacity wait timed out",
                )),
            },
        };
        drop(queue_guard);
        permit
    }
}

#[async_trait]
pub(crate) trait TurnMcpRuntimeView: Send + Sync {
    async fn current_tool_identity(
        &self,
        workspace_id: &str,
        binding: &TurnMcpBindingRecord,
    ) -> Result<CurrentMcpToolIdentity, TurnMcpInvocationError>;
}

#[async_trait]
pub(crate) trait TurnMcpValidatedExecution: Send + Sync {
    async fn execute(
        &self,
        validated: ValidatedTurnMcpInvocation,
        cancellation: CancellationToken,
    ) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError>;
}

#[async_trait]
pub(crate) trait TurnMcpInvoker: Send + Sync {
    async fn invoke(
        &self,
        invocation: TurnMcpInvocation,
        cancellation: CancellationToken,
    ) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError>;
}

pub(crate) struct GatewayTurnMcpInvoker {
    crud_store: Arc<CrudStore>,
    runtime_view: Arc<dyn TurnMcpRuntimeView>,
    execution: Arc<dyn TurnMcpValidatedExecution>,
    authorization_invalidation_hub: Arc<AuthorizationInvalidationHub>,
    execution_leases: Arc<ExecutionLeaseRegistry>,
    invocation_governor: Arc<McpInvocationGovernor>,
}

impl GatewayTurnMcpInvoker {
    pub(crate) fn new(
        crud_store: Arc<CrudStore>,
        runtime_view: Arc<dyn TurnMcpRuntimeView>,
        execution: Arc<dyn TurnMcpValidatedExecution>,
        authorization_invalidation_hub: Arc<AuthorizationInvalidationHub>,
        execution_leases: Arc<ExecutionLeaseRegistry>,
        invocation_governor: Arc<McpInvocationGovernor>,
    ) -> Self {
        Self {
            crud_store,
            runtime_view,
            execution,
            authorization_invalidation_hub,
            execution_leases,
            invocation_governor,
        }
    }

    async fn validate(
        &self,
        invocation: TurnMcpInvocation,
        cancellation: &CancellationToken,
    ) -> Result<ValidatedTurnMcpInvocation, TurnMcpInvocationError> {
        validate_required_identity(&invocation)?;
        let turn = self
            .crud_store
            .get_turn(invocation.thread_id.as_str(), invocation.turn_id.as_str())
            .await
            .map_err(|_| internal_error("failed to verify MCP turn state"))?
            .ok_or_else(|| {
                invocation_error(
                    TurnMcpInvocationErrorCode::TurnNotActive,
                    "MCP invocation turn is not active",
                )
            })?;
        if turn.0 != invocation.workspace_id {
            return Err(invocation_error(
                TurnMcpInvocationErrorCode::ScopeMismatch,
                "MCP invocation workspace does not match the active turn",
            ));
        }
        if turn.1.status != TurnStatus::InProgress {
            return Err(invocation_error(
                TurnMcpInvocationErrorCode::TurnNotActive,
                "MCP invocation turn is not in progress",
            ));
        }

        let projection = self
            .crud_store
            .get_turn_mcp_projection(invocation.turn_id.as_str())
            .await
            .map_err(|_| internal_error("failed to load frozen MCP projection"))?
            .ok_or_else(|| {
                invocation_error(
                    TurnMcpInvocationErrorCode::ProjectionUnavailable,
                    "frozen MCP projection is unavailable",
                )
            })?;
        if projection.workspace_id != invocation.workspace_id {
            return Err(invocation_error(
                TurnMcpInvocationErrorCode::ScopeMismatch,
                "frozen MCP projection workspace does not match the invocation",
            ));
        }
        if projection.projection_version <= 0
            || !projection.resolution_status.starts_with("resolved")
        {
            return Err(invocation_error(
                TurnMcpInvocationErrorCode::ProjectionUnavailable,
                "frozen MCP projection is not executable",
            ));
        }

        let bindings = self
            .crud_store
            .list_turn_mcp_bindings(invocation.turn_id.as_str())
            .await
            .map_err(|_| internal_error("failed to load frozen MCP bindings"))?;
        let mut matches = bindings.into_iter().filter(|binding| {
            binding.canonical_callable_name == invocation.canonical_callable_name
        });
        let binding = matches.next().ok_or_else(|| {
            invocation_error(
                TurnMcpInvocationErrorCode::ToolUnbound,
                "MCP callable is not bound to the active turn",
            )
        })?;
        if matches.next().is_some() {
            return Err(internal_error(
                "frozen MCP projection contains duplicate callable bindings",
            ));
        }
        if binding.server_installation_id == FIRST_PARTY_FILE_SERVER_ID
            && (!matches!(invocation.origin, TurnMcpInvocationOrigin::CliFacade)
                || !invocation
                    .runtime_id
                    .as_deref()
                    .is_some_and(|runtime| runtime.to_ascii_lowercase().contains("claude")))
        {
            return Err(invocation_error(
                TurnMcpInvocationErrorCode::PermissionDenied,
                "first-party filesystem tools are available only through managed Claude",
            ));
        }

        let invocation_limits = self
            .revalidate_execution_authorization(&invocation, &projection, &binding)
            .await?;
        self.validate_origin_session(&invocation, &projection.manifest_hash, &binding)
            .await?;
        let current_tool = self
            .runtime_view
            .current_tool_identity(invocation.workspace_id.as_str(), &binding)
            .await?;
        validate_frozen_identity(&binding, &current_tool)?;
        let concurrency_permit = self
            .invocation_governor
            .acquire(invocation.turn_id.as_str(), invocation_limits, cancellation)
            .await?;

        Ok(ValidatedTurnMcpInvocation {
            invocation,
            manifest_hash: projection.manifest_hash,
            binding,
            current_tool,
            invocation_limits,
            _concurrency_permit: concurrency_permit,
        })
    }

    async fn revalidate_execution_authorization(
        &self,
        invocation: &TurnMcpInvocation,
        projection: &pioneer_crud::TurnMcpProjectionRecord,
        binding: &TurnMcpBindingRecord,
    ) -> Result<pioneer_protocol::McpInvocationResourceLimits, TurnMcpInvocationError> {
        let context = ExecutionAuthorizationContext::load_for_turn(
            self.crud_store.as_ref(),
            invocation.turn_id.as_str(),
        )
        .await
        .map_err(|_| {
            invocation_error(
                TurnMcpInvocationErrorCode::ProjectionUnavailable,
                "MCP execution authorization context is invalid",
            )
        })?;
        let projection_version = u32::try_from(projection.projection_version).map_err(|_| {
            invocation_error(
                TurnMcpInvocationErrorCode::ProjectionUnavailable,
                "MCP projection version is invalid",
            )
        })?;
        context
            .verify_mcp_projection(
                invocation.workspace_id.as_str(),
                projection_version,
                projection.manifest_hash.as_str(),
            )
            .map_err(|_| {
                invocation_error(
                    TurnMcpInvocationErrorCode::ProjectionUnavailable,
                    "MCP projection is stale or is not bound to this execution",
                )
            })?;
        let revalidated = self
            .execution_leases
            .revalidate_for_turn(
                self.crud_store.as_ref(),
                &context,
                invocation.workspace_id.as_str(),
                invocation.thread_id.as_str(),
                invocation.turn_id.as_str(),
                ResourceAction::McpUse,
                self.authorization_invalidation_hub
                    .current_revision()
                    .await
                    .map_err(|_| {
                        invocation_error(
                            TurnMcpInvocationErrorCode::ProjectionUnavailable,
                            "MCP authorization policy generation is unavailable",
                        )
                    })?,
            )
            .await
            .map_err(|_| {
                invocation_error(
                    TurnMcpInvocationErrorCode::PermissionDenied,
                    "current execution authorization no longer permits MCP use",
                )
            })?;

        if binding.server_installation_id != FIRST_PARTY_FILE_SERVER_ID {
            let action_gate = AuthorizationService::new().authorize_action(
                revalidated.principal().kind,
                revalidated.principal().role_key.as_ref(),
                ResourceAction::McpUse,
            );
            let capability = AuthorizationResolver::new(self.crud_store.as_ref().clone())
                .authorize_persisted_capability(
                    revalidated.principal(),
                    &action_gate,
                    ResourceAction::McpUse,
                    invocation.workspace_id.as_str(),
                    CapabilityKind::McpServer,
                    binding.server_name.as_str(),
                )
                .await
                .map_err(|_| internal_error("failed to revalidate current MCP workspace policy"))?;
            if !matches!(capability, ProofResolution::Authorized(_)) {
                return Err(invocation_error(
                    TurnMcpInvocationErrorCode::PermissionDenied,
                    "current MCP workspace policy no longer permits this server",
                ));
            }
        }
        let admitted_limits = context
            .effective_mcp_invocation_resource_limits()
            .map_err(|_| {
                invocation_error(
                    TurnMcpInvocationErrorCode::PermissionDenied,
                    "execution MCP resource policy is no longer valid",
                )
            })?;
        let collaborator_limits = AuthorizationService::new()
            .mcp_invocation_resource_limits(
                revalidated.principal().kind,
                revalidated.principal().role_key.as_ref(),
            )
            .ok_or_else(|| {
                invocation_error(
                    TurnMcpInvocationErrorCode::PermissionDenied,
                    "current collaborator has no MCP resource policy",
                )
            })?;
        let effective_limits = admitted_limits
            .intersect(collaborator_limits)
            .ok_or_else(|| {
                invocation_error(
                    TurnMcpInvocationErrorCode::PermissionDenied,
                    "MCP resource policies are incompatible",
                )
            })?;
        Ok(effective_limits)
    }

    async fn validate_origin_session(
        &self,
        invocation: &TurnMcpInvocation,
        manifest_hash: &str,
        binding: &TurnMcpBindingRecord,
    ) -> Result<(), TurnMcpInvocationError> {
        match invocation.origin {
            TurnMcpInvocationOrigin::NativeApi => {
                if invocation.runtime_id.is_some() || invocation.session_generation.is_some() {
                    return Err(invocation_error(
                        TurnMcpInvocationErrorCode::InvalidRequest,
                        "native API MCP invocation cannot carry a CLI session identity",
                    ));
                }
            }
            TurnMcpInvocationOrigin::CliFacade => {
                let runtime_id = invocation.runtime_id.as_deref().ok_or_else(|| {
                    invocation_error(
                        TurnMcpInvocationErrorCode::SessionBindingUnavailable,
                        "CLI MCP invocation is missing its runtime identity",
                    )
                })?;
                let session_generation = invocation.session_generation.ok_or_else(|| {
                    invocation_error(
                        TurnMcpInvocationErrorCode::SessionBindingUnavailable,
                        "CLI MCP invocation is missing its session generation",
                    )
                })?;
                let turn_binding = self
                    .crud_store
                    .get_cli_runtime_turn_binding(invocation.turn_id.as_str())
                    .await
                    .map_err(|_| internal_error("failed to verify CLI MCP session binding"))?
                    .ok_or_else(|| {
                        invocation_error(
                            TurnMcpInvocationErrorCode::SessionBindingUnavailable,
                            "CLI MCP turn binding is unavailable",
                        )
                    })?;
                if turn_binding.workspace_id != invocation.workspace_id
                    || turn_binding.thread_id != invocation.thread_id
                    || turn_binding.runtime_id != runtime_id
                {
                    return Err(invocation_error(
                        TurnMcpInvocationErrorCode::ScopeMismatch,
                        "CLI MCP session scope does not match the active turn",
                    ));
                }
                if turn_binding.status != "running" {
                    return Err(invocation_error(
                        TurnMcpInvocationErrorCode::TurnNotActive,
                        "CLI MCP turn binding is not running",
                    ));
                }
                let metadata = turn_binding.mcp.ok_or_else(|| {
                    invocation_error(
                        TurnMcpInvocationErrorCode::SessionBindingUnavailable,
                        "CLI MCP session metadata is unavailable",
                    )
                })?;
                let expected_session_generation =
                    i64::try_from(session_generation).map_err(|_| {
                        invocation_error(
                            TurnMcpInvocationErrorCode::SessionGenerationStale,
                            "CLI MCP session generation is outside the supported range",
                        )
                    })?;
                if metadata.session_generation != expected_session_generation
                    || metadata.manifest_hash != manifest_hash
                    || metadata.projection_activation_generation
                        != binding.projection_activation_generation
                {
                    return Err(invocation_error(
                        TurnMcpInvocationErrorCode::SessionGenerationStale,
                        "CLI MCP session generation or projection activation is stale",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl TurnMcpInvoker for GatewayTurnMcpInvoker {
    async fn invoke(
        &self,
        invocation: TurnMcpInvocation,
        cancellation: CancellationToken,
    ) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
        let validated = self.validate(invocation, &cancellation).await?;
        self.execution.execute(validated, cancellation).await
    }
}

fn validate_required_identity(
    invocation: &TurnMcpInvocation,
) -> Result<(), TurnMcpInvocationError> {
    if invocation.workspace_id.trim().is_empty()
        || invocation.thread_id.trim().is_empty()
        || invocation.turn_id.trim().is_empty()
        || invocation.provider_call_id.trim().is_empty()
        || invocation.canonical_callable_name.trim().is_empty()
    {
        return Err(invocation_error(
            TurnMcpInvocationErrorCode::InvalidRequest,
            "MCP invocation identity is incomplete",
        ));
    }
    Ok(())
}

pub(crate) fn validate_frozen_identity(
    binding: &TurnMcpBindingRecord,
    current: &CurrentMcpToolIdentity,
) -> Result<(), TurnMcpInvocationError> {
    let timeout_matches = i64::try_from(current.effective_timeout_ms)
        .is_ok_and(|timeout| timeout == binding.effective_timeout_ms);
    let generation_matches = i64::try_from(current.runtime_generation)
        .is_ok_and(|generation| generation == binding.runtime_generation);
    let exact = binding.server_installation_id == current.server_installation_id
        && binding.server_name == current.server_name
        && binding.raw_tool_name == current.raw_tool_name
        && binding.catalog_version == current.catalog_version
        && binding.fingerprint == current.installation_fingerprint
        && binding.canonical_schema_fingerprint == current.canonical_schema_fingerprint
        && binding.annotations_json == current.annotations_json
        && binding.annotations_digest == current.annotations_digest
        && timeout_matches
        && generation_matches;
    if !exact {
        return Err(invocation_error(
            TurnMcpInvocationErrorCode::ToolDrift,
            "frozen MCP binding no longer matches the current runtime tool",
        ));
    }
    Ok(())
}

fn invocation_error(
    code: TurnMcpInvocationErrorCode,
    message: impl Into<String>,
) -> TurnMcpInvocationError {
    TurnMcpInvocationError::new(code, message)
}

fn internal_error(message: impl Into<String>) -> TurnMcpInvocationError {
    invocation_error(TurnMcpInvocationErrorCode::Internal, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn narrow_limits() -> pioneer_protocol::McpInvocationResourceLimits {
        pioneer_protocol::McpInvocationResourceLimits {
            max_concurrent_calls: 1,
            max_queued_calls: 1,
            ..Default::default()
        }
    }

    async fn wait_for_queued_call(governor: &McpInvocationGovernor, turn_id: &str) {
        for _ in 0..100 {
            let queued = governor
                .states
                .lock()
                .expect("governor lock")
                .get(turn_id)
                .map(|state| state.queued.load(Ordering::Acquire))
                .unwrap_or(0);
            if queued == 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("queued MCP invocation was not registered");
    }

    #[tokio::test]
    async fn governor_enforces_role_owned_active_and_queue_limits_per_turn() {
        let governor = Arc::new(McpInvocationGovernor::default());
        let limits = narrow_limits();
        let first = governor
            .acquire("turn-a", limits, &CancellationToken::new())
            .await
            .expect("first invocation acquires the active slot");

        let queued_governor = governor.clone();
        let queued = tokio::spawn(async move {
            queued_governor
                .acquire("turn-a", limits, &CancellationToken::new())
                .await
        });
        wait_for_queued_call(governor.as_ref(), "turn-a").await;

        let saturated = governor
            .acquire("turn-a", limits, &CancellationToken::new())
            .await;
        assert!(matches!(
            saturated,
            Err(TurnMcpInvocationError {
                code: TurnMcpInvocationErrorCode::ResourceExhausted,
                ..
            })
        ));

        let independent = governor
            .acquire("turn-b", limits, &CancellationToken::new())
            .await
            .expect("a different turn has an independent budget");
        drop(independent);
        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(1), queued)
            .await
            .expect("queued invocation should resume")
            .expect("queued task should not panic")
            .expect("queued invocation should acquire the released slot");
        drop(second);
    }

    #[tokio::test]
    async fn governor_cancellation_removes_the_queued_reservation() {
        let governor = Arc::new(McpInvocationGovernor::default());
        let limits = narrow_limits();
        let first = governor
            .acquire("turn-a", limits, &CancellationToken::new())
            .await
            .expect("first invocation acquires the active slot");
        let cancellation = CancellationToken::new();
        let queued_governor = governor.clone();
        let queued_cancellation = cancellation.clone();
        let queued = tokio::spawn(async move {
            queued_governor
                .acquire("turn-a", limits, &queued_cancellation)
                .await
        });
        wait_for_queued_call(governor.as_ref(), "turn-a").await;
        cancellation.cancel();
        let result = queued.await.expect("queued task should not panic");
        assert!(matches!(
            result,
            Err(TurnMcpInvocationError {
                code: TurnMcpInvocationErrorCode::Cancelled,
                ..
            })
        ));
        assert_eq!(
            governor
                .states
                .lock()
                .expect("governor lock")
                .get("turn-a")
                .expect("turn state")
                .queued
                .load(Ordering::Acquire),
            0
        );
        drop(first);
    }

    #[tokio::test]
    async fn governor_capacity_wait_has_a_recoverable_deadline() {
        let governor = Arc::new(McpInvocationGovernor::default());
        let limits = pioneer_protocol::McpInvocationResourceLimits {
            max_queue_wait_ms: 10,
            ..narrow_limits()
        };
        let first = governor
            .acquire("turn-a", limits, &CancellationToken::new())
            .await
            .expect("first invocation acquires the active slot");
        let result = governor
            .acquire("turn-a", limits, &CancellationToken::new())
            .await;
        assert!(matches!(
            result,
            Err(TurnMcpInvocationError {
                code: TurnMcpInvocationErrorCode::TimedOut,
                ..
            })
        ));
        drop(first);
    }
}
