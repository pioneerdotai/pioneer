use super::result::CanonicalMcpToolResult;
use async_trait::async_trait;
use pioneer_crud::{CrudStore, TurnMcpBindingRecord};
use pioneer_protocol::TurnStatus;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::fmt;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

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
    pub(crate) catalog_version: String,
    pub(crate) installation_fingerprint: String,
    pub(crate) canonical_schema_fingerprint: String,
    pub(crate) canonical_schema: JsonValue,
    pub(crate) annotations_json: String,
    pub(crate) annotations_digest: String,
    pub(crate) effective_timeout_ms: u64,
    pub(crate) runtime_generation: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ValidatedTurnMcpInvocation {
    pub(crate) invocation: TurnMcpInvocation,
    pub(crate) manifest_hash: String,
    pub(crate) binding: TurnMcpBindingRecord,
    pub(crate) current_tool: CurrentMcpToolIdentity,
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
}

impl GatewayTurnMcpInvoker {
    pub(crate) fn new(
        crud_store: Arc<CrudStore>,
        runtime_view: Arc<dyn TurnMcpRuntimeView>,
        execution: Arc<dyn TurnMcpValidatedExecution>,
    ) -> Self {
        Self {
            crud_store,
            runtime_view,
            execution,
        }
    }

    async fn validate(
        &self,
        invocation: TurnMcpInvocation,
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

        self.validate_origin_session(&invocation, &projection.manifest_hash, &binding)
            .await?;
        let current_tool = self
            .runtime_view
            .current_tool_identity(invocation.workspace_id.as_str(), &binding)
            .await?;
        validate_frozen_identity(&binding, &current_tool)?;

        Ok(ValidatedTurnMcpInvocation {
            invocation,
            manifest_hash: projection.manifest_hash,
            binding,
            current_tool,
        })
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
        let validated = self.validate(invocation).await?;
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

fn validate_frozen_identity(
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
