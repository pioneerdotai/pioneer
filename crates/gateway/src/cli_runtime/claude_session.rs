use crate::cli_runtime::claude_mcp::{
    ClaudeManagedMcpLaunchMode, ClaudeMcpSessionLaunchProjection, ClaudeNativeMcpPermissionRequest,
    append_claude_exact_allowed_tools, is_claude_native_mcp_permission_candidate,
    materialize_claude_mcp_config, parse_claude_native_mcp_permission_request,
};
use crate::cli_runtime::config::{
    claude_account_probe_config_from_instance, load_effective_cli_runtime_instances,
};
use crate::cli_runtime::continuation::{
    CliMcpSessionLaunch, CliProviderContinuation, CliSessionLaunchSpec,
};
use crate::cli_runtime::manager::{
    CLIAgentRuntimeEventReceivers, CLIAgentRuntimeMcpTurnMetadata,
    CLIAgentRuntimeObservedTurnStatus, CLIAgentRuntimeSession, CLIAgentRuntimeSessionFactory,
    CLIAgentRuntimeSessionKey, CLIAgentRuntimeSessionStartOptions, CLIAgentRuntimeThreadOpenParams,
    CLIAgentRuntimeThreadOpenSnapshot, CLIAgentRuntimeTurnObservation,
    CLIAgentRuntimeTurnStartParams, CLIAgentRuntimeTurnStartSnapshot,
    CLIAgentRuntimeTurnSteerRequest, CLIAgentRuntimeTurnSteerResult,
};
use crate::cli_runtime::mcp::coordinator::{
    CliMcpProjectionFingerprint, CliMcpProjectionGeneration,
};
use crate::cli_runtime::mcp::facade::CliMcpFacadeProjection;
use crate::cli_runtime::mcp::grants::{CliMcpGrantScope, CliMcpManifestHash};
use crate::cli_runtime::mcp::limits::CliMcpFacadeLimits;
use crate::cli_runtime::mcp::server::{CliMcpBridgeFacadeHandle, CliMcpBridgeFacadeServer};
use crate::cli_runtime::mcp::supervisor::{CliMcpBridgeLaunch, CliMcpBridgeSupervisor};
use crate::cli_runtime::permissions::{
    ClaudeMcpPermissionFallbackDecision, claude_mcp_permission_fallback_response,
};
use crate::cli_runtime::session_instance::{CliSessionGenerationAllocator, CliSessionInstanceId};
use crate::turn_mcp::invoker::{
    TurnMcpInvocation, TurnMcpInvocationError, TurnMcpInvocationErrorCode, TurnMcpInvoker,
};
use crate::turn_mcp::projection::{
    McpProjectionLimits, McpSelectionReason, ResolvedMcpTurnProjection, ResolvedMcpTurnTool,
};
use crate::turn_mcp::result::CanonicalMcpToolResult;
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use base64::Engine as _;
use pioneer_cli_agent_runtime::claude::{
    ClaudeAccountProbeStatus, ClaudeManagedMcpConfigDescriptor, ClaudeManagedMcpConfigIdentity,
    ClaudeProbe, ClaudeProviderSessionLaunch, cleanup_claude_managed_mcp_config,
    materialize_claude_system_prompt_extension,
};
use pioneer_cli_agent_runtime::event::{
    RuntimeAgentMessagePhase, RuntimeErrorEvent, RuntimeEvent, RuntimeItemCompleted,
    RuntimeItemDelta, RuntimeItemDeltaKind, RuntimeItemStarted, RuntimeNativeEvent,
    RuntimeRequestOpened, RuntimeRequestResolved, RuntimeThreadStateChanged, RuntimeTurnCompleted,
    RuntimeTurnFailed, RuntimeTurnInterrupted, RuntimeTurnStarted,
};
use pioneer_cli_agent_runtime::process::{
    CLIAgentProcess, CLIAgentProcessSpawnConfig, SensitiveEnvironment, expand_home_path,
    spawn_cli_agent_process,
};
use pioneer_cli_agent_runtime::reserved_args::validate_claude_custom_args;
use pioneer_config::{
    EffectiveGatewayCliAgentRuntimeInstanceConfig, GatewayCliAgentRuntimeKindConfig,
};
use pioneer_crud::CliRuntimeProviderSessionLifecycle;
use pioneer_crud::CrudStore;
use pioneer_protocol::ToolMetadataValue;
use pioneer_runtime_events::{OrderedEventIngress, OrderedIngressConfig, OrderedIngressOffer};
use serde_json::{Value as JsonValue, json};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout as tokio_timeout;
use tokio_util::sync::CancellationToken;

const CLAUDE_SDK_ENTRYPOINT: &str = "sdk-rs";
const CLAUDE_SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClaudeMcpLocalProviderProbeEvidence {
    pub(crate) config_artifact_digest: String,
    pub(crate) projection_fingerprint: String,
    pub(crate) helper_binary_sha256: String,
    pub(crate) strict_launch_fingerprint: String,
    pub(crate) continuation_fingerprint: String,
}

struct ClaudeReadinessNeverInvoke;

#[async_trait]
impl TurnMcpInvoker for ClaudeReadinessNeverInvoke {
    async fn invoke(
        &self,
        _invocation: TurnMcpInvocation,
        _cancellation: CancellationToken,
    ) -> std::result::Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
        Err(TurnMcpInvocationError::new(
            TurnMcpInvocationErrorCode::TurnNotActive,
            "Claude local readiness probe never activates a model turn",
        ))
    }
}

struct ClaudePreparedMcpBridge {
    launch: CliMcpBridgeLaunch,
    projection: CliMcpFacadeProjection,
    projection_generation: CliMcpProjectionGeneration,
    canonical_manifest_hash: String,
    provider_contract_fingerprint: String,
    isolation_contract_fingerprint: String,
}

struct ClaudeRequiredMcpBridge {
    supervisor: Arc<CliMcpBridgeSupervisor>,
    process_instance: CliSessionInstanceId,
    launch: CliMcpBridgeLaunch,
    projection: CliMcpFacadeProjection,
    projection_generation: CliMcpProjectionGeneration,
    projection_fingerprint: CliMcpProjectionFingerprint,
    canonical_manifest_hash: String,
    provider_contract_fingerprint: String,
    isolation_contract_fingerprint: String,
    invoker: Arc<dyn TurnMcpInvoker>,
    native_items: Arc<ClaudeNativeMcpCorrelationLedger>,
    state: Mutex<ClaudeRequiredMcpBridgeState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ClaudeNativeMcpItemKey {
    native_thread_id: String,
    native_turn_id: String,
    native_item_id: String,
}

struct ClaudeNativeMcpItemCorrelation {
    canonical_callable_name: String,
    arguments_fingerprint: String,
    sequence: u64,
    facade_request_id: Option<String>,
}

#[derive(Default)]
struct ClaudeNativeMcpCorrelationLedger {
    items: std::sync::Mutex<HashMap<ClaudeNativeMcpItemKey, ClaudeNativeMcpItemCorrelation>>,
    next_sequence: AtomicU64,
    changed: tokio::sync::Notify,
}

impl ClaudeNativeMcpCorrelationLedger {
    fn register(
        &self,
        binding: &crate::cli_runtime::claude_mcp::ClaudeNativeMcpItemBinding,
    ) -> Result<()> {
        let key = ClaudeNativeMcpItemKey {
            native_thread_id: binding.native_thread_id.clone(),
            native_turn_id: binding.native_turn_id.clone(),
            native_item_id: binding.native_item_id.clone(),
        };
        let mut items = self
            .items
            .lock()
            .expect("Claude native MCP correlation ledger should not be poisoned");
        if let Some(existing) = items.get(&key) {
            if existing.canonical_callable_name != binding.canonical_callable_name
                || existing.arguments_fingerprint != binding.arguments_fingerprint
            {
                bail!("Claude native MCP item identity changed during replay");
            }
            return Ok(());
        }
        items.insert(
            key,
            ClaudeNativeMcpItemCorrelation {
                canonical_callable_name: binding.canonical_callable_name.clone(),
                arguments_fingerprint: binding.arguments_fingerprint.clone(),
                sequence: self.next_sequence.fetch_add(1, Ordering::Relaxed),
                facade_request_id: None,
            },
        );
        drop(items);
        self.changed.notify_waiters();
        Ok(())
    }

    async fn claim(
        &self,
        canonical_callable_name: &str,
        arguments_fingerprint: &str,
        facade_request_id: &str,
    ) -> Option<ClaudeNativeMcpItemKey> {
        let notified = self.changed.notified();
        if let Some(key) = self.claim_now(
            canonical_callable_name,
            arguments_fingerprint,
            facade_request_id,
        ) {
            return Some(key);
        }
        let _ = tokio_timeout(Duration::from_secs(2), notified).await;
        self.claim_now(
            canonical_callable_name,
            arguments_fingerprint,
            facade_request_id,
        )
    }

    fn claim_now(
        &self,
        canonical_callable_name: &str,
        arguments_fingerprint: &str,
        facade_request_id: &str,
    ) -> Option<ClaudeNativeMcpItemKey> {
        let mut items = self
            .items
            .lock()
            .expect("Claude native MCP correlation ledger should not be poisoned");
        let key = items
            .iter()
            .filter(|(_, item)| {
                item.facade_request_id.is_none()
                    && item.canonical_callable_name == canonical_callable_name
                    && item.arguments_fingerprint == arguments_fingerprint
            })
            .min_by_key(|(_, item)| item.sequence)
            .map(|(key, _)| key.clone())?;
        items
            .get_mut(&key)
            .expect("selected Claude MCP correlation must exist")
            .facade_request_id = Some(facade_request_id.to_owned());
        Some(key)
    }

    fn contains_exact_permission(&self, request: &ClaudeNativeMcpPermissionRequest) -> bool {
        self.items
            .lock()
            .expect("Claude native MCP correlation ledger should not be poisoned")
            .get(&ClaudeNativeMcpItemKey {
                native_thread_id: request.native_thread_id.clone(),
                native_turn_id: request.native_turn_id.clone(),
                native_item_id: request.native_item_id.clone(),
            })
            .is_some_and(|item| {
                item.canonical_callable_name == request.canonical_callable_name
                    && item.arguments_fingerprint == request.arguments_fingerprint
            })
    }

    fn clear_turn(&self, native_thread_id: &str, native_turn_id: &str) {
        self.items
            .lock()
            .expect("Claude native MCP correlation ledger should not be poisoned")
            .retain(|key, _| {
                key.native_thread_id != native_thread_id || key.native_turn_id != native_turn_id
            });
    }
}

#[async_trait]
trait ClaudeMcpPermissionAuthorizer: Send + Sync {
    async fn authorize_permission(
        &self,
        request: &ClaudeNativeMcpPermissionRequest,
    ) -> Result<bool>;
}

struct ClaudeCorrelatingTurnMcpInvoker {
    inner: Arc<dyn TurnMcpInvoker>,
    native_items: Arc<ClaudeNativeMcpCorrelationLedger>,
}

#[async_trait]
impl TurnMcpInvoker for ClaudeCorrelatingTurnMcpInvoker {
    async fn invoke(
        &self,
        mut invocation: crate::turn_mcp::invoker::TurnMcpInvocation,
        cancellation: CancellationToken,
    ) -> std::result::Result<
        crate::turn_mcp::result::CanonicalMcpToolResult,
        crate::turn_mcp::invoker::TurnMcpInvocationError,
    > {
        let arguments_fingerprint =
            crate::cli_runtime::codex_mcp::canonical_value_fingerprint(&invocation.arguments)
                .map_err(|_| {
                    crate::turn_mcp::invoker::TurnMcpInvocationError::new(
                        crate::turn_mcp::invoker::TurnMcpInvocationErrorCode::InvalidRequest,
                        "Claude MCP invocation arguments are not canonicalizable",
                    )
                })?;
        let facade_request_id = invocation.provider_call_id.clone();
        let native_item = tokio::select! {
            biased;
            () = cancellation.cancelled() => None,
            native_item = self.native_items.claim(
                invocation.canonical_callable_name.as_str(),
                arguments_fingerprint.as_str(),
                facade_request_id.as_str(),
            ) => native_item,
        }
        .ok_or_else(|| {
            crate::turn_mcp::invoker::TurnMcpInvocationError::new(
                crate::turn_mcp::invoker::TurnMcpInvocationErrorCode::SessionBindingUnavailable,
                "Claude MCP facade call has no matching native tool_use item",
            )
        })?;
        invocation.provider_call_id = native_item.native_item_id;
        self.inner.invoke(invocation, cancellation).await
    }
}

struct ClaudeActiveMcpTurn {
    pioneer_turn_id: String,
    activation_generation: crate::cli_runtime::mcp::coordinator::CliMcpActivationGeneration,
    native_thread_id: Option<String>,
    native_turn_id: Option<String>,
}

enum ClaudeRequiredMcpBridgeState {
    Pending,
    Serving {
        bound_grant: crate::cli_runtime::mcp::grants::CliMcpBoundGrant,
        handle: CliMcpBridgeFacadeHandle,
        server: JoinHandle<Result<(), crate::cli_runtime::mcp::server::CliMcpBridgeServerError>>,
    },
    Ready {
        bound_grant: crate::cli_runtime::mcp::grants::CliMcpBoundGrant,
        handle: CliMcpBridgeFacadeHandle,
        server: JoinHandle<Result<(), crate::cli_runtime::mcp::server::CliMcpBridgeServerError>>,
        active_turn: Option<ClaudeActiveMcpTurn>,
    },
    Failed,
}

impl ClaudeRequiredMcpBridge {
    async fn ensure_ready(&self, readiness_timeout: Duration) -> Result<()> {
        if readiness_timeout.is_zero() {
            bail!("Claude required MCP readiness timeout must be non-zero");
        }
        let bound = {
            let mut state = self.state.lock().await;
            match &*state {
                ClaudeRequiredMcpBridgeState::Ready { .. } => return Ok(()),
                ClaudeRequiredMcpBridgeState::Failed => {
                    bail!("Claude required MCP bridge generation is failed")
                }
                ClaudeRequiredMcpBridgeState::Serving { bound_grant, .. } => bound_grant.clone(),
                ClaudeRequiredMcpBridgeState::Pending => {
                    let attachment = self
                        .supervisor
                        .await_attach(&self.process_instance, readiness_timeout)
                        .await
                        .map_err(|error| {
                            anyhow!("Claude required MCP helper attach failed: {error}")
                        })?;
                    let transport = self
                        .supervisor
                        .take_transport(&self.process_instance)
                        .await
                        .map_err(|error| {
                            anyhow!("Claude required MCP transport failed: {error}")
                        })?;
                    let (handle, server) = CliMcpBridgeFacadeServer::build(
                        transport,
                        self.supervisor.coordinator(),
                        Arc::new(ClaudeCorrelatingTurnMcpInvoker {
                            inner: self.invoker.clone(),
                            native_items: self.native_items.clone(),
                        }),
                        self.projection_generation,
                        self.projection.clone(),
                        CliMcpFacadeLimits::default(),
                    )
                    .map_err(|error| anyhow!("Claude MCP facade build failed: {error}"))?;
                    let server = tokio::spawn(server.run());
                    let bound = attachment.bound_grant;
                    *state = ClaudeRequiredMcpBridgeState::Serving {
                        bound_grant: bound.clone(),
                        handle,
                        server,
                    };
                    bound
                }
            }
        };
        tokio_timeout(
            readiness_timeout,
            self.supervisor.coordinator().wait_projection_ready(
                &bound,
                self.projection_generation,
                &self.projection_fingerprint,
            ),
        )
        .await
        .map_err(|_| anyhow!("Claude required MCP tools/list readiness timed out"))?
        .map_err(|error| anyhow!("Claude required MCP tools/list readiness failed: {error:?}"))?;
        let mut state = self.state.lock().await;
        match std::mem::replace(&mut *state, ClaudeRequiredMcpBridgeState::Failed) {
            ClaudeRequiredMcpBridgeState::Serving {
                bound_grant,
                handle,
                server,
            } => {
                *state = ClaudeRequiredMcpBridgeState::Ready {
                    bound_grant,
                    handle,
                    server,
                    active_turn: None,
                };
                Ok(())
            }
            ready @ ClaudeRequiredMcpBridgeState::Ready { .. } => {
                *state = ready;
                Ok(())
            }
            other => {
                *state = other;
                bail!("Claude required MCP bridge changed state before readiness")
            }
        }
    }

    async fn fail_closed(&self) {
        let mut state = self.state.lock().await;
        let previous = std::mem::replace(&mut *state, ClaudeRequiredMcpBridgeState::Failed);
        match previous {
            ClaudeRequiredMcpBridgeState::Serving { server, .. }
            | ClaudeRequiredMcpBridgeState::Ready { server, .. } => server.abort(),
            ClaudeRequiredMcpBridgeState::Pending | ClaudeRequiredMcpBridgeState::Failed => {}
        }
        drop(state);
        self.supervisor.revoke_session(&self.process_instance).await;
    }

    async fn prepare_turn(&self, pioneer_turn_id: &str) -> Result<CLIAgentRuntimeMcpTurnMetadata> {
        self.ensure_ready(Duration::from_secs(30)).await?;
        let mut state = self.state.lock().await;
        let ClaudeRequiredMcpBridgeState::Ready { active_turn, .. } = &mut *state else {
            bail!("Claude MCP bridge is not ready for turn reservation")
        };
        if active_turn.is_some() {
            bail!("Claude MCP bridge already has a non-terminal turn lease")
        }
        let reservation = self
            .supervisor
            .coordinator()
            .reserve_turn(
                self.launch.grant_ref(),
                self.projection_generation,
                pioneer_turn_id,
            )
            .await
            .map_err(|error| anyhow!("failed to reserve Claude MCP turn: {error:?}"))?;
        *active_turn = Some(ClaudeActiveMcpTurn {
            pioneer_turn_id: pioneer_turn_id.to_owned(),
            activation_generation: reservation.activation_generation,
            native_thread_id: None,
            native_turn_id: None,
        });
        Ok(CLIAgentRuntimeMcpTurnMetadata {
            adapter_kind: "claude_strict_mcp".to_owned(),
            manifest_hash: self.canonical_manifest_hash.clone(),
            projection_fingerprint: self.projection_fingerprint.as_str().to_owned(),
            provider_contract_fingerprint: self.provider_contract_fingerprint.clone(),
            isolation_contract_fingerprint: self.isolation_contract_fingerprint.clone(),
            session_generation: self.process_instance.generation(),
            projection_activation_generation: reservation.activation_generation.get(),
        })
    }

    async fn activate_turn(
        &self,
        pioneer_turn_id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
        let ClaudeRequiredMcpBridgeState::Ready {
            bound_grant,
            handle,
            active_turn,
            ..
        } = &mut *state
        else {
            bail!("Claude MCP bridge is not ready for turn activation")
        };
        let turn = active_turn
            .as_mut()
            .ok_or_else(|| anyhow!("Claude MCP turn was not reserved"))?;
        if turn.pioneer_turn_id != pioneer_turn_id {
            bail!("Claude MCP turn reservation does not match the Pioneer turn")
        }
        self.supervisor
            .coordinator()
            .activate_turn(
                bound_grant,
                turn.activation_generation,
                native_thread_id,
                native_turn_id,
            )
            .await
            .map_err(|error| anyhow!("failed to activate Claude MCP turn: {error:?}"))?;
        handle
            .set_activation(Some(turn.activation_generation))
            .await;
        turn.native_thread_id = Some(native_thread_id.to_owned());
        turn.native_turn_id = Some(native_turn_id.to_owned());
        Ok(())
    }

    async fn terminal_turn(&self, pioneer_turn_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        let ClaudeRequiredMcpBridgeState::Ready {
            bound_grant,
            handle,
            active_turn,
            ..
        } = &mut *state
        else {
            return Ok(());
        };
        let Some(turn) = active_turn.take() else {
            return Ok(());
        };
        if turn.pioneer_turn_id != pioneer_turn_id {
            *active_turn = Some(turn);
            bail!("Claude MCP terminal turn does not match the active lease")
        }
        self.supervisor
            .coordinator()
            .terminal_turn(bound_grant, turn.activation_generation)
            .await
            .map_err(|error| anyhow!("failed to terminalize Claude MCP turn: {error:?}"))?;
        handle.set_activation(None).await;
        if let (Some(native_thread_id), Some(native_turn_id)) = (
            turn.native_thread_id.as_deref(),
            turn.native_turn_id.as_deref(),
        ) {
            self.native_items
                .clear_turn(native_thread_id, native_turn_id);
        }
        Ok(())
    }

    async fn authorize_native_permission(
        &self,
        request: &ClaudeNativeMcpPermissionRequest,
    ) -> Result<bool> {
        if request.runtime_id != self.process_instance.key().runtime_id
            || request.session_generation != self.process_instance.generation()
            || request.manifest_hash != self.canonical_manifest_hash
            || request.provider_contract_fingerprint != self.provider_contract_fingerprint
            || request.qualified_tool_name
                != format!(
                    "{}{}",
                    pioneer_cli_agent_runtime::claude::CLAUDE_PIONEER_MCP_TOOL_PREFIX,
                    request.canonical_callable_name
                )
            || self.launch.grant_ref().scope().manifest_hash.as_str()
                != self.canonical_manifest_hash
            || !self
                .projection
                .contains_tool(request.canonical_callable_name.as_str())
        {
            return Ok(false);
        }
        let notified = self.native_items.changed.notified();
        if !self.native_items.contains_exact_permission(request) {
            let _ = tokio_timeout(Duration::from_millis(250), notified).await;
        }
        if !self.native_items.contains_exact_permission(request) {
            return Ok(false);
        }
        let state = self.state.lock().await;
        let ClaudeRequiredMcpBridgeState::Ready {
            bound_grant,
            active_turn: Some(turn),
            ..
        } = &*state
        else {
            return Ok(false);
        };
        if turn.native_thread_id.as_deref() != Some(request.native_thread_id.as_str())
            || turn.native_turn_id.as_deref() != Some(request.native_turn_id.as_str())
        {
            return Ok(false);
        }
        Ok(self
            .supervisor
            .coordinator()
            .authorize_call(bound_grant, turn.activation_generation)
            .await
            .is_ok())
    }
}

#[async_trait]
impl ClaudeMcpPermissionAuthorizer for ClaudeRequiredMcpBridge {
    async fn authorize_permission(
        &self,
        request: &ClaudeNativeMcpPermissionRequest,
    ) -> Result<bool> {
        self.authorize_native_permission(request).await
    }
}

pub(crate) struct ClaudeCLIAgentRuntimeSessionFactory {
    runtime_home: PathBuf,
    bridge_supervisor: Option<Arc<CliMcpBridgeSupervisor>>,
    turn_mcp_invoker: Option<Arc<dyn TurnMcpInvoker>>,
    crud_store: Arc<CrudStore>,
}

impl ClaudeCLIAgentRuntimeSessionFactory {
    pub(crate) fn new_with_bridge(
        runtime_home: PathBuf,
        bridge_supervisor: Arc<CliMcpBridgeSupervisor>,
        turn_mcp_invoker: Arc<dyn TurnMcpInvoker>,
        crud_store: Arc<CrudStore>,
    ) -> Self {
        Self {
            runtime_home,
            bridge_supervisor: Some(bridge_supervisor),
            turn_mcp_invoker: Some(turn_mcp_invoker),
            crud_store,
        }
    }

    fn runtime_instance(
        &self,
        runtime_id: &str,
    ) -> Result<EffectiveGatewayCliAgentRuntimeInstanceConfig> {
        load_effective_cli_runtime_instances(self.runtime_home.as_path())?
            .into_iter()
            .find(|instance| instance.id == runtime_id)
            .ok_or_else(|| anyhow!("unknown CLI runtime `{runtime_id}`"))
    }
}

#[async_trait]
impl CLIAgentRuntimeSessionFactory for ClaudeCLIAgentRuntimeSessionFactory {
    async fn start_session(
        &self,
        process_instance: &CliSessionInstanceId,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
        self.start_session_with_options(
            process_instance,
            &CLIAgentRuntimeSessionStartOptions::default(),
        )
        .await
    }

    async fn start_session_with_options(
        &self,
        _process_instance: &CliSessionInstanceId,
        _options: &CLIAgentRuntimeSessionStartOptions,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
        bail!("Claude CLI session start requires a durable typed provider continuation")
    }

    async fn start_session_with_launch_spec(
        &self,
        process_instance: &CliSessionInstanceId,
        launch_spec: &CliSessionLaunchSpec,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
        let options = &launch_spec.options;
        let launch_projection = match &launch_spec.mcp {
            CliMcpSessionLaunch::Disabled | CliMcpSessionLaunch::ManagementOnly => None,
            CliMcpSessionLaunch::Claude(projection) => Some(projection),
            CliMcpSessionLaunch::Codex(_) => {
                bail!("Claude CLI runtime cannot consume a Codex MCP launch projection")
            }
        };
        let provider_session_id = launch_spec
            .continuation
            .claude_provider_session_id()
            .ok_or_else(|| anyhow!("Claude CLI runtime requires a typed Claude continuation"))?;
        if provider_session_id.is_nil() {
            bail!("Claude CLI provider session UUID cannot be nil");
        }
        let key = process_instance.key();
        let instance = self.runtime_instance(key.runtime_id.as_str())?;
        if !instance.enabled {
            bail!("CLI runtime `{}` is disabled", instance.id);
        }
        if instance.kind != GatewayCliAgentRuntimeKindConfig::Claude {
            bail!(
                "CLI runtime `{}` is configured as unsupported kind `{:?}` for Claude session",
                instance.id,
                instance.kind
            );
        }

        let mut probe_config = claude_account_probe_config_from_instance(&instance);
        probe_config.env.extend_from(&options.env);
        let probe = ClaudeProbe::account_read(probe_config).await;
        match probe.status {
            ClaudeAccountProbeStatus::Ready => {}
            ClaudeAccountProbeStatus::NeedsAuth => {
                bail!(
                    "{}",
                    probe
                        .message
                        .unwrap_or_else(|| "Claude CLI authentication is required".to_owned())
                );
            }
            ClaudeAccountProbeStatus::MissingBinary => {
                bail!(
                    "{}",
                    probe
                        .message
                        .unwrap_or_else(|| "Claude CLI binary was not found".to_owned())
                );
            }
            ClaudeAccountProbeStatus::SpawnFailed
            | ClaudeAccountProbeStatus::UnsupportedVersion
            | ClaudeAccountProbeStatus::Error => {
                bail!(
                    "{}",
                    probe
                        .message
                        .unwrap_or_else(|| "Claude CLI probe failed".to_owned())
                );
            }
        }

        let managed_mcp_identity = ClaudeManagedMcpConfigIdentity::new(
            key.workspace_id.clone(),
            key.runtime_id.clone(),
            key.thread_id.clone(),
            process_instance.boot_id().as_str(),
            process_instance.generation(),
        )
        .map_err(|error| anyhow!("failed to identify Claude managed MCP config: {error}"))?;
        let mut prepared_mcp_bridge = None;
        let managed_mcp_mode = if let Some(launch_projection) = launch_projection {
            if launch_projection.preflight.tools.is_empty() {
                ClaudeManagedMcpLaunchMode::Empty
            } else {
                let supervisor = self
                    .bridge_supervisor
                    .as_ref()
                    .ok_or_else(|| anyhow!("Claude MCP launch requires the bridge supervisor"))?;
                let projection = launch_projection.facade_projection().map_err(|error| {
                    anyhow!("failed to build Claude facade projection: {error}")
                })?;
                let manifest_hash = CliMcpManifestHash::new(
                    launch_projection.preflight.canonical_manifest_hash.clone(),
                )
                .map_err(|error| anyhow!("invalid Claude MCP launch manifest: {error:?}"))?;
                let scope = CliMcpGrantScope::new(process_instance.clone(), manifest_hash);
                let launch = supervisor
                    .prepare(
                        scope,
                        claude_mcp_bootstrap_expiry(instance.startup_probe_timeout_ms)?,
                    )
                    .await
                    .map_err(|error| anyhow!("failed to prepare Claude MCP bridge: {error}"))?;
                let reservation = match supervisor
                    .coordinator()
                    .stage_projection(launch.grant_ref(), projection.fingerprint().clone())
                    .await
                {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        supervisor.revoke_session(process_instance).await;
                        return Err(anyhow!(
                            "failed to stage Claude MCP list identity: {error:?}"
                        ));
                    }
                };
                let bootstrap_path = launch.bootstrap_path().to_path_buf();
                prepared_mcp_bridge = Some(ClaudePreparedMcpBridge {
                    launch,
                    projection,
                    projection_generation: reservation.generation,
                    canonical_manifest_hash: launch_projection
                        .preflight
                        .canonical_manifest_hash
                        .clone(),
                    provider_contract_fingerprint: launch_projection
                        .preflight
                        .provider_contract_fingerprint
                        .clone(),
                    isolation_contract_fingerprint: launch_projection
                        .semantic_restart_fingerprint()
                        .to_owned(),
                });
                ClaudeManagedMcpLaunchMode::Pioneer { bootstrap_path }
            }
        } else {
            ClaudeManagedMcpLaunchMode::Empty
        };
        let managed_mcp_config = materialize_claude_mcp_config(
            self.runtime_home
                .join("cli-runtime")
                .join("claude-mcp-configs")
                .as_path(),
            managed_mcp_identity,
            managed_mcp_mode,
        )
        .map_err(|error| anyhow!("failed to materialize strict Claude MCP config: {error}"));
        let managed_mcp_config = match managed_mcp_config {
            Ok(config) => config,
            Err(error) => {
                if let Some(supervisor) = self.bridge_supervisor.as_ref() {
                    supervisor.revoke_session(process_instance).await;
                }
                return Err(error);
            }
        };
        let mut managed_mcp_guard = ClaudeManagedMcpConfigStartupGuard::new(managed_mcp_config);
        let allowed_tool_names = launch_projection
            .map(|projection| projection.preflight.allowed_tool_names.as_slice())
            .unwrap_or_default();
        let process_config = claude_process_config_from_instance_with_managed_mcp(
            &instance,
            options,
            managed_mcp_guard.descriptor(),
            allowed_tool_names,
            &launch_spec.continuation,
        )?
        .with_process_generation(process_instance.generation())
        .context("failed to bind Claude process generation")?;
        let mut process = spawn_cli_agent_process(&process_config)
            .with_context(|| format!("failed to spawn Claude CLI for runtime `{}`", instance.id))?;
        let stderr = process.stderr();
        let mcp_native_items = prepared_mcp_bridge
            .as_ref()
            .map(|_| Arc::new(ClaudeNativeMcpCorrelationLedger::default()));
        let required_mcp_bridge = if let Some(prepared) = prepared_mcp_bridge {
            let supervisor = self
                .bridge_supervisor
                .as_ref()
                .expect("prepared Claude bridge requires supervisor");
            let provider_process_id = match process.id() {
                Some(process_id) if process_id != 0 => process_id,
                _ => {
                    supervisor.revoke_session(process_instance).await;
                    let _ = process.terminate_with_grace(Duration::from_secs(2)).await;
                    bail!("Claude provider process identity is unavailable");
                }
            };
            if let Err(error) = supervisor
                .associate_provider_process(process_instance, provider_process_id, None)
                .await
            {
                supervisor.revoke_session(process_instance).await;
                let _ = process.terminate_with_grace(Duration::from_secs(2)).await;
                bail!("failed to bind Claude MCP bridge to provider process: {error}");
            }
            let invoker = self
                .turn_mcp_invoker
                .as_ref()
                .ok_or_else(|| anyhow!("Claude MCP launch requires the turn MCP invoker"))?
                .clone();
            let projection_fingerprint = prepared.projection.fingerprint().clone();
            Some(Arc::new(ClaudeRequiredMcpBridge {
                supervisor: supervisor.clone(),
                process_instance: process_instance.clone(),
                launch: prepared.launch,
                projection: prepared.projection,
                projection_generation: prepared.projection_generation,
                projection_fingerprint,
                canonical_manifest_hash: prepared.canonical_manifest_hash,
                provider_contract_fingerprint: prepared.provider_contract_fingerprint,
                isolation_contract_fingerprint: prepared.isolation_contract_fingerprint,
                invoker,
                native_items: mcp_native_items
                    .as_ref()
                    .expect("prepared Claude bridge requires native item ledger")
                    .clone(),
                state: Mutex::new(ClaudeRequiredMcpBridgeState::Pending),
            }))
        } else {
            None
        };
        let (stdout, stdin) = process.take_stdio()?;
        let (event_tx, event_rx) = mpsc::channel(instance.event_channel_capacity.max(1));
        let client = Arc::new(ClaudeStreamClient::new(
            stdin,
            event_tx,
            ClaudeProviderSessionVerifier {
                crud_store: self.crud_store.clone(),
                thread_id: key.thread_id.clone(),
                expected_provider_session_id: provider_session_id,
                process_generation: process_instance.generation(),
            },
            launch_projection
                .cloned()
                .zip(mcp_native_items)
                .map(|(projection, native_items)| ClaudeMcpEventContext {
                    runtime_id: key.runtime_id.clone(),
                    session_generation: process_instance.generation(),
                    manifest_hash: projection.preflight.canonical_manifest_hash.clone(),
                    provider_contract_fingerprint: projection
                        .preflight
                        .provider_contract_fingerprint
                        .clone(),
                    projection,
                    native_items,
                    permission_authorizer: required_mcp_bridge
                        .clone()
                        .map(|bridge| bridge as Arc<dyn ClaudeMcpPermissionAuthorizer>),
                }),
        ));
        client.spawn_reader(stdout);
        if let Err(error) = client
            .initialize(Duration::from_millis(instance.startup_probe_timeout_ms))
            .await
        {
            let provider_session_id = provider_session_id.to_string();
            let _ = self
                .crud_store
                .verify_claude_provider_session_binding(
                    key.thread_id.as_str(),
                    provider_session_id.as_str(),
                    None,
                    i64::try_from(process_instance.generation()).unwrap_or(i64::MAX),
                )
                .await;
            let _ = process.terminate_with_grace(Duration::from_secs(2)).await;
            if let Some(bridge) = required_mcp_bridge.as_ref() {
                bridge.fail_closed().await;
            }
            return Err(error).context("Claude initialize handshake failed");
        }
        let managed_mcp_config = managed_mcp_guard.disarm();

        Ok(Arc::new(ClaudeCLIAgentRuntimeSession {
            client,
            process: Mutex::new(process),
            request_timeout: Duration::from_millis(instance.request_timeout_ms),
            shutdown_grace: Duration::from_secs(5),
            native_thread_id: Mutex::new(None),
            provider_session_id,
            process_generation: process_instance.generation(),
            crud_store: self.crud_store.clone(),
            required_mcp_bridge,
            replacement_checkpoint: Mutex::new(ClaudeReplacementCheckpoint::Idle),
            process_closed: std::sync::atomic::AtomicBool::new(false),
            event_receivers: std::sync::Mutex::new(Some(CLIAgentRuntimeEventReceivers {
                process_instance: process_instance.clone(),
                runtime_kind: "claude".to_owned(),
                events: event_rx,
            })),
            _stderr: stderr,
            managed_mcp_config: std::sync::Mutex::new(Some(managed_mcp_config)),
        }))
    }
}

struct ClaudeManagedMcpConfigStartupGuard {
    descriptor: Option<ClaudeManagedMcpConfigDescriptor>,
}

impl ClaudeManagedMcpConfigStartupGuard {
    fn new(descriptor: ClaudeManagedMcpConfigDescriptor) -> Self {
        Self {
            descriptor: Some(descriptor),
        }
    }

    fn descriptor(&self) -> &ClaudeManagedMcpConfigDescriptor {
        self.descriptor
            .as_ref()
            .expect("Claude managed MCP config guard must be armed")
    }

    fn disarm(&mut self) -> ClaudeManagedMcpConfigDescriptor {
        self.descriptor
            .take()
            .expect("Claude managed MCP config guard must be armed")
    }
}

impl Drop for ClaudeManagedMcpConfigStartupGuard {
    fn drop(&mut self) {
        let Some(descriptor) = self.descriptor.take() else {
            return;
        };
        if let Err(error) = cleanup_claude_managed_mcp_config(&descriptor) {
            tracing::warn!(
                error = %error,
                config = %descriptor.config_path.display(),
                "failed to clean up Claude managed MCP config after startup failure"
            );
        }
    }
}

fn claude_process_config_from_instance_with_managed_mcp(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    options: &CLIAgentRuntimeSessionStartOptions,
    managed_mcp_config: &ClaudeManagedMcpConfigDescriptor,
    allowed_tool_names: &[String],
    continuation: &CliProviderContinuation,
) -> Result<CLIAgentProcessSpawnConfig> {
    validate_claude_custom_args(&instance.app_server_args)
        .context("Claude instance custom launch arguments are invalid")?;
    validate_claude_custom_args(&options.app_server_args)
        .context("Claude session custom launch arguments are invalid")?;
    let config_dir = expand_home_path(
        instance
            .shadow_home_path
            .as_deref()
            .unwrap_or(instance.home_path.as_str()),
        None,
    )?;
    let mut env = SensitiveEnvironment::new();
    env.insert_plain(
        "CLAUDE_CONFIG_DIR".to_owned(),
        config_dir.to_string_lossy().into_owned(),
    );
    env.insert_plain(
        "CLAUDE_CODE_ENTRYPOINT".to_owned(),
        CLAUDE_SDK_ENTRYPOINT.to_owned(),
    );
    env.insert_plain(
        "CLAUDE_AGENT_SDK_VERSION".to_owned(),
        CLAUDE_SDK_VERSION.to_owned(),
    );
    env.insert_plain(
        "CLAUDE_AGENT_SDK_CLIENT_APP".to_owned(),
        "pioneer".to_owned(),
    );
    env.extend_from(&options.env);
    let permission_mode = options
        .approval_policy
        .as_deref()
        .map(str::trim)
        .filter(|mode| !mode.is_empty())
        .unwrap_or("default")
        .to_owned();

    let elevated_prompt_path = options
        .elevated_instructions
        .as_ref()
        .map(|instructions| {
            materialize_claude_system_prompt_extension(managed_mcp_config, instructions)
        })
        .transpose()
        .context("failed to materialize Claude elevated system prompt extension")?;

    let mut args = vec![
        "--output-format".to_owned(),
        "stream-json".to_owned(),
        "--verbose".to_owned(),
    ];
    if let Some(path) = elevated_prompt_path {
        args.push("--append-system-prompt-file".to_owned());
        args.push(path.to_string_lossy().into_owned());
    }
    args.extend([
        "--permission-prompt-tool".to_owned(),
        "stdio".to_owned(),
        "--permission-mode".to_owned(),
        permission_mode.clone(),
        "--mcp-config".to_owned(),
        managed_mcp_config
            .config_path
            .to_string_lossy()
            .into_owned(),
        "--strict-mcp-config".to_owned(),
        if options.enable_user_skills {
            "--setting-sources=user".to_owned()
        } else {
            "--setting-sources=".to_owned()
        },
        "--include-partial-messages".to_owned(),
        "--input-format".to_owned(),
        "stream-json".to_owned(),
    ]);
    if !options.enable_user_skills
        && !managed_mcp_config.has_pioneer_server
        && permission_mode != "bypassPermissions"
    {
        args.push("--safe-mode".to_owned());
    }
    append_claude_exact_allowed_tools(&mut args, managed_mcp_config, allowed_tool_names)
        .context("Claude exact MCP allowed-tool projection is invalid")?;
    match continuation {
        CliProviderContinuation::ClaudeNew {
            provider_session_id,
        } => {
            ClaudeProviderSessionLaunch::new(*provider_session_id)
                .context("invalid Claude new-session continuation")?
                .append_process_args(&mut args);
        }
        CliProviderContinuation::ClaudeResume {
            provider_session_id,
        } => {
            ClaudeProviderSessionLaunch::resume(*provider_session_id)
                .context("invalid Claude resume continuation")?
                .append_process_args(&mut args);
        }
        CliProviderContinuation::CodexRpcThread { .. } => {
            bail!("Claude process launch requires a typed Claude continuation");
        }
    }
    args.extend(instance.app_server_args.clone());
    args.extend(options.app_server_args.clone());

    Ok(CLIAgentProcessSpawnConfig {
        executable: instance.binary_path.clone(),
        args,
        cwd: options.cwd.clone().or_else(|| std::env::current_dir().ok()),
        home_path: None,
        home_dir: None,
        env,
        env_remove: vec!["CLAUDECODE".to_owned()],
        stderr_ring_lines: instance.stderr_ring_lines,
        process_group: true,
        process_generation: None,
    })
}

/// Cheap Claude MCP provider probe. It starts the configured binary and its
/// managed Pioneer stdio helper, but never sends user input or a model request.
/// The probe deliberately places hostile native MCP configuration in both the
/// user and project locations and requires the strict generated configuration
/// to remain the only surface that attaches.
pub(crate) async fn run_claude_mcp_local_provider_probe(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    runtime_home: &Path,
    proxy_url: Option<&str>,
) -> Result<ClaudeMcpLocalProviderProbeEvidence> {
    if instance.kind != GatewayCliAgentRuntimeKindConfig::Claude {
        bail!("Claude local provider probe requires a Claude runtime instance");
    }
    let probe_parent = runtime_home.join("cli-runtime").join("claude-readiness");
    std::fs::create_dir_all(probe_parent.as_path())
        .context("failed to create Claude readiness probe root")?;
    let temporary = tempfile::Builder::new()
        .prefix("probe-")
        .tempdir_in(probe_parent.as_path())
        .context("failed to create private Claude readiness directory")?;
    let probe_root = temporary.path().to_path_buf();
    let config_home = probe_root.join("config-home");
    let workspace = probe_root.join("workspace");
    std::fs::create_dir_all(config_home.as_path())?;
    std::fs::create_dir_all(workspace.as_path())?;
    let malicious_marker = workspace.join("unmanaged-mcp-started");
    write_claude_malicious_native_mcp_fixture(
        config_home.as_path(),
        workspace.as_path(),
        malicious_marker.as_path(),
    )?;

    let mut probed_instance = instance.clone();
    probed_instance.shadow_home_path = Some(config_home.to_string_lossy().into_owned());
    let projection_contract = pioneer_cli_agent_runtime::codex_attestation::sha256_json(&json!({
        "contract": "claude-local-readiness-projection-v1",
        "provider": "claude",
    }))?;
    let mut projection = ResolvedMcpTurnProjection::empty("readiness-workspace", "readiness-turn");
    projection.tools.push(ResolvedMcpTurnTool {
        canonical_callable_name: String::new(),
        workspace_id: "readiness-workspace".to_owned(),
        server_installation_id: "readiness-installation".to_owned(),
        server_name: "readiness".to_owned(),
        raw_tool_name: "sentinel".to_owned(),
        description: Some("Pioneer local readiness sentinel".to_owned()),
        input_schema: json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false,
        }),
        annotations: None,
        timeout_ms: 1_000,
        catalog_version: "local-readiness-v1".to_owned(),
        installation_fingerprint: "local-readiness-installation-v1".to_owned(),
        schema_fingerprint: String::new(),
        runtime_generation: 1,
        selection_reason: McpSelectionReason::ExplicitTool,
        capability_id: Some("readiness-sentinel".to_owned()),
    });
    projection
        .finalize_identity(McpProjectionLimits::default())
        .context("failed to finalize Claude readiness projection")?;
    let launch_projection =
        crate::cli_runtime::claude_mcp::build_claude_mcp_session_launch_projection(
            projection,
            projection_contract,
        )
        .context("failed to transform Claude readiness projection")?;
    let facade_projection = launch_projection
        .facade_projection()
        .context("failed to build Claude readiness facade projection")?;

    let process_instance =
        CliSessionGenerationAllocator::default().allocate(CLIAgentRuntimeSessionKey::new(
            "readiness-workspace",
            instance.id.clone(),
            "readiness-thread",
        )?)?;
    let supervisor = CliMcpBridgeSupervisor::new(probe_root.join("bridge"));
    let manifest_hash =
        CliMcpManifestHash::new(launch_projection.preflight.canonical_manifest_hash.clone())
            .map_err(|error| anyhow!("invalid Claude readiness manifest: {error:?}"))?;
    let scope = CliMcpGrantScope::new(process_instance.clone(), manifest_hash);
    let launch = supervisor
        .prepare(
            scope,
            claude_mcp_bootstrap_expiry(instance.startup_probe_timeout_ms)?,
        )
        .await
        .map_err(|error| anyhow!("failed to prepare Claude readiness bridge: {error}"))?;
    let reservation = supervisor
        .coordinator()
        .stage_projection(launch.grant_ref(), facade_projection.fingerprint().clone())
        .await
        .map_err(|error| anyhow!("failed to stage Claude readiness projection: {error:?}"))?;
    let bootstrap_path = launch.bootstrap_path().to_path_buf();
    let native_items = Arc::new(ClaudeNativeMcpCorrelationLedger::default());
    let bridge = Arc::new(ClaudeRequiredMcpBridge {
        supervisor: supervisor.clone(),
        process_instance: process_instance.clone(),
        launch,
        projection: facade_projection.clone(),
        projection_generation: reservation.generation,
        projection_fingerprint: facade_projection.fingerprint().clone(),
        canonical_manifest_hash: launch_projection.preflight.canonical_manifest_hash.clone(),
        provider_contract_fingerprint: launch_projection
            .preflight
            .provider_contract_fingerprint
            .clone(),
        isolation_contract_fingerprint: launch_projection.semantic_restart_fingerprint().to_owned(),
        invoker: Arc::new(ClaudeReadinessNeverInvoke),
        native_items: native_items.clone(),
        state: Mutex::new(ClaudeRequiredMcpBridgeState::Pending),
    });

    let managed_root = probe_root.join("managed-configs");
    let managed_identity = ClaudeManagedMcpConfigIdentity::new(
        "readiness-workspace",
        instance.id.clone(),
        "readiness-thread",
        process_instance.boot_id().as_str(),
        process_instance.generation(),
    )?;
    let managed = materialize_claude_mcp_config(
        managed_root.as_path(),
        managed_identity,
        ClaudeManagedMcpLaunchMode::Pioneer {
            bootstrap_path: bootstrap_path.clone(),
        },
    )?;
    validate_claude_owner_only_probe_artifacts(&managed, bootstrap_path.as_path())?;
    let empty = materialize_claude_mcp_config(
        managed_root.as_path(),
        ClaudeManagedMcpConfigIdentity::new(
            "readiness-workspace",
            instance.id.clone(),
            "readiness-empty-thread",
            process_instance.boot_id().as_str(),
            process_instance.generation().saturating_add(1),
        )?,
        ClaudeManagedMcpLaunchMode::Empty,
    )?;

    let provider_session_id = uuid::Uuid::new_v4();
    let continuation = CliProviderContinuation::ClaudeNew {
        provider_session_id,
    };
    let mut options = CLIAgentRuntimeSessionStartOptions {
        cwd: Some(workspace.clone()),
        approval_policy: Some("default".to_owned()),
        ..Default::default()
    };
    if let Some(proxy_url) = proxy_url.filter(|value| !value.trim().is_empty()) {
        options
            .env
            .insert_plain("HTTP_PROXY".to_owned(), proxy_url.to_owned());
        options
            .env
            .insert_plain("HTTPS_PROXY".to_owned(), proxy_url.to_owned());
    }
    let process_config = claude_process_config_from_instance_with_managed_mcp(
        &probed_instance,
        &options,
        &managed,
        launch_projection.preflight.allowed_tool_names.as_slice(),
        &continuation,
    )?
    .with_process_generation(process_instance.generation())?;
    validate_claude_readiness_launch_matrix(
        &probed_instance,
        &options,
        &managed,
        &empty,
        launch_projection.preflight.allowed_tool_names.as_slice(),
        &continuation,
        &process_config,
    )?;
    let strict_launch_fingerprint =
        pioneer_cli_agent_runtime::codex_attestation::sha256_json(&json!({
            "args": process_config.args,
            "configArtifactDigest": managed.artifact_digest,
            "allowedTools": launch_projection.preflight.allowed_tool_names,
            "strictMcpConfig": true,
        }))?;

    let mut provider_process = None;
    let mut client_for_cleanup: Option<Arc<ClaudeStreamClient>> = None;
    let outcome = async {
        let mut process = spawn_cli_agent_process(&process_config)
            .context("failed to spawn Claude local readiness process")?;
        let provider_process_id = process
            .id()
            .filter(|process_id| *process_id != 0)
            .ok_or_else(|| anyhow!("Claude readiness process identity is unavailable"))?;
        supervisor
            .associate_provider_process(&process_instance, provider_process_id, None)
            .await
            .map_err(|error| anyhow!("failed to bind Claude readiness process: {error}"))?;
        let (stdout, stdin) = process.take_stdio()?;
        provider_process = Some(process);
        let (event_tx, _event_rx) = mpsc::channel(instance.event_channel_capacity.max(1));
        let client = Arc::new(ClaudeStreamClient::new_for_local_probe(
            stdin,
            event_tx,
            provider_session_id,
        ));
        client.spawn_reader(stdout);
        client_for_cleanup = Some(client.clone());
        let wait = Duration::from_millis(instance.startup_probe_timeout_ms.max(1));
        tokio::try_join!(client.initialize(wait), bridge.ensure_ready(wait))?;
        if malicious_marker.exists() {
            bail!("unmanaged Claude MCP sentinel started despite strict config");
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    drop(client_for_cleanup.take());
    if let Some(mut process) = provider_process.take() {
        let _ = process.terminate_with_grace(Duration::from_secs(2)).await;
    }
    bridge.fail_closed().await;
    supervisor.shutdown().await;
    cleanup_claude_managed_mcp_config(&managed)
        .context("failed to clean Claude readiness managed config")?;
    cleanup_claude_managed_mcp_config(&empty)
        .context("failed to clean Claude readiness empty config")?;
    outcome?;
    if bootstrap_path.exists() || malicious_marker.exists() {
        bail!("Claude readiness probe left a bootstrap or unmanaged sentinel artifact");
    }
    let helper = crate::cli_runtime::config::resolve_current_pioneer_cli_mcp_helper()?;
    Ok(ClaudeMcpLocalProviderProbeEvidence {
        config_artifact_digest: managed.artifact_digest,
        projection_fingerprint: facade_projection.fingerprint().as_str().to_owned(),
        helper_binary_sha256: pioneer_cli_agent_runtime::codex_attestation::sha256_file_contents(
            helper.as_path(),
        )?,
        strict_launch_fingerprint,
        continuation_fingerprint:
            pioneer_cli_agent_runtime::claude::claude_continuation_contract_fingerprint()?,
    })
}

fn validate_claude_readiness_launch_matrix(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    base_options: &CLIAgentRuntimeSessionStartOptions,
    managed: &ClaudeManagedMcpConfigDescriptor,
    empty: &ClaudeManagedMcpConfigDescriptor,
    allowed_tool_names: &[String],
    continuation: &CliProviderContinuation,
    non_empty_config: &CLIAgentProcessSpawnConfig,
) -> Result<()> {
    let has = |config: &CLIAgentProcessSpawnConfig, flag: &str| {
        config.args.iter().any(|argument| argument == flag)
    };
    if !has(non_empty_config, "--strict-mcp-config")
        || !has(non_empty_config, "--mcp-config")
        || has(non_empty_config, "--safe-mode")
        || has(non_empty_config, "--no-session-persistence")
        || non_empty_config
            .args
            .iter()
            .any(|argument| argument.contains('*') || argument == "bypassPermissions")
    {
        bail!("Claude non-empty readiness launch violates strict managed MCP policy");
    }
    let allowed = non_empty_config
        .args
        .windows(2)
        .filter(|window| window[0] == "--allowedTools")
        .map(|window| window[1].as_str())
        .collect::<Vec<_>>();
    if allowed
        != allowed_tool_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    {
        bail!("Claude readiness launch does not contain the exact allowed tool set");
    }
    for (skills, expected_safe_mode) in [(false, true), (true, false)] {
        let mut options = base_options.clone();
        options.enable_user_skills = skills;
        let config = claude_process_config_from_instance_with_managed_mcp(
            instance,
            &options,
            empty,
            &[],
            continuation,
        )?;
        if has(&config, "--safe-mode") != expected_safe_mode {
            bail!("Claude empty-projection safe-mode launch matrix changed");
        }
    }
    let mut skills_options = base_options.clone();
    skills_options.enable_user_skills = true;
    let skills_with_mcp = claude_process_config_from_instance_with_managed_mcp(
        instance,
        &skills_options,
        managed,
        allowed_tool_names,
        continuation,
    )?;
    if has(&skills_with_mcp, "--safe-mode") {
        bail!("Claude skills plus MCP readiness launch unexpectedly enabled safe mode");
    }
    Ok(())
}

fn write_claude_malicious_native_mcp_fixture(
    config_home: &Path,
    workspace: &Path,
    marker: &Path,
) -> Result<()> {
    #[cfg(unix)]
    let command = {
        use std::os::unix::fs::PermissionsExt;
        let script = workspace.join("unmanaged-mcp-sentinel.sh");
        std::fs::write(
            script.as_path(),
            format!("#!/bin/sh\nprintf started > '{}'\n", marker.display()),
        )?;
        std::fs::set_permissions(script.as_path(), std::fs::Permissions::from_mode(0o700))?;
        script
    };
    #[cfg(windows)]
    let command = {
        let script = workspace.join("unmanaged-mcp-sentinel.cmd");
        std::fs::write(
            script.as_path(),
            format!("@echo started>\"{}\"\r\n", marker.display()),
        )?;
        script
    };
    let fixture = json!({
        "mcpServers": {
            "unmanaged_sentinel": {
                "type": "stdio",
                "command": command,
                "args": []
            }
        }
    });
    let encoded = serde_json::to_vec_pretty(&fixture)?;
    std::fs::write(workspace.join(".mcp.json"), encoded.as_slice())?;
    std::fs::write(config_home.join(".mcp.json"), encoded.as_slice())?;
    std::fs::write(config_home.join("settings.json"), encoded)?;
    Ok(())
}

fn validate_claude_owner_only_probe_artifacts(
    descriptor: &ClaudeManagedMcpConfigDescriptor,
    bootstrap_path: &Path,
) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for (path, expected) in [
            (descriptor.session_root_path.as_path(), 0o700),
            (descriptor.config_path.as_path(), 0o600),
            (bootstrap_path, 0o600),
        ] {
            let actual = std::fs::metadata(path)?.permissions().mode() & 0o777;
            if actual != expected {
                bail!("Claude readiness artifact is not owner-only");
            }
        }
    }
    #[cfg(windows)]
    for path in [descriptor.config_path.as_path(), bootstrap_path] {
        if !path.is_file() {
            bail!("Claude readiness owner-only artifact is unavailable");
        }
    }
    Ok(())
}

fn claude_mcp_bootstrap_expiry(startup_timeout_ms: u64) -> Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_millis();
    let lifetime = u128::from(startup_timeout_ms.max(1)).saturating_add(30_000);
    u64::try_from(now.saturating_add(lifetime))
        .context("Claude MCP bootstrap expiry exceeds supported range")
}

#[cfg(test)]
fn claude_process_config_from_instance(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    options: &CLIAgentRuntimeSessionStartOptions,
) -> Result<CLIAgentProcessSpawnConfig> {
    static TEST_GENERATION: AtomicU64 = AtomicU64::new(1);
    let generation = TEST_GENERATION.fetch_add(1, Ordering::Relaxed);
    let managed_root = expand_home_path(instance.home_path.as_str(), None)?
        .join("pioneer-test-managed-mcp-configs");
    let descriptor = materialize_claude_mcp_config(
        managed_root.as_path(),
        ClaudeManagedMcpConfigIdentity::new(
            "test-workspace",
            instance.id.as_str(),
            format!("test-thread-{generation}"),
            "test-gateway-boot",
            generation,
        )?,
        ClaudeManagedMcpLaunchMode::Empty,
    )?;
    claude_process_config_from_instance_with_managed_mcp(
        instance,
        options,
        &descriptor,
        &[],
        &CliProviderContinuation::ClaudeNew {
            provider_session_id: uuid::Uuid::new_v4(),
        },
    )
}

struct ClaudeCLIAgentRuntimeSession {
    client: Arc<ClaudeStreamClient>,
    process: Mutex<CLIAgentProcess>,
    request_timeout: Duration,
    shutdown_grace: Duration,
    native_thread_id: Mutex<Option<String>>,
    provider_session_id: uuid::Uuid,
    process_generation: u64,
    crud_store: Arc<CrudStore>,
    required_mcp_bridge: Option<Arc<ClaudeRequiredMcpBridge>>,
    replacement_checkpoint: Mutex<ClaudeReplacementCheckpoint>,
    process_closed: std::sync::atomic::AtomicBool,
    event_receivers: std::sync::Mutex<Option<CLIAgentRuntimeEventReceivers>>,
    _stderr: pioneer_cli_agent_runtime::process::StderrRing,
    managed_mcp_config: std::sync::Mutex<Option<ClaudeManagedMcpConfigDescriptor>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaudeReplacementCheckpoint {
    Idle,
    Armed,
    Confirmed,
}

#[async_trait]
impl CLIAgentRuntimeSession for ClaudeCLIAgentRuntimeSession {
    async fn close(&self) -> Result<()> {
        let mut process = self.process.lock().await;
        let _ = process.terminate_with_grace(self.shutdown_grace).await?;
        self.process_closed.store(true, Ordering::Release);
        drop(process);
        let managed_mcp_config = self
            .managed_mcp_config
            .lock()
            .expect("Claude managed MCP config mutex should not be poisoned")
            .clone();
        if let Some(managed_mcp_config) = managed_mcp_config {
            cleanup_claude_managed_mcp_config(&managed_mcp_config).map_err(|error| {
                anyhow!("failed to clean up Claude managed MCP config: {error}")
            })?;
            self.managed_mcp_config
                .lock()
                .expect("Claude managed MCP config mutex should not be poisoned")
                .take();
        }
        Ok(())
    }

    async fn prepare_for_replacement(&self) -> Result<()> {
        self.client
            .wait_provider_identity_and_idle(self.request_timeout)
            .await?;
        if let Some(bridge) = self.required_mcp_bridge.as_ref() {
            let state = bridge.state.lock().await;
            if matches!(
                &*state,
                ClaudeRequiredMcpBridgeState::Ready {
                    active_turn: Some(_),
                    ..
                }
            ) {
                bail!("Claude MCP turn lease is not terminal");
            }
        }
        let binding = self
            .crud_store
            .get_cli_runtime_thread_binding(
                self.client
                    .provider_session_verifier
                    .as_ref()
                    .ok_or_else(|| anyhow!("Claude durable session verifier is unavailable"))?
                    .thread_id
                    .as_str(),
            )
            .await?
            .ok_or_else(|| anyhow!("Claude durable provider session binding is missing"))?;
        let provider = binding
            .provider_session
            .ok_or_else(|| anyhow!("Claude provider session metadata is missing"))?;
        if provider.lifecycle != CliRuntimeProviderSessionLifecycle::Verified
            || provider.provider_session_id != self.provider_session_id.to_string()
            || provider.last_verified_process_generation
                != Some(
                    i64::try_from(self.process_generation)
                        .context("Claude process generation exceeds durable replacement range")?,
                )
        {
            bail!("Claude durable provider session binding is not verified for this generation");
        }
        *self.replacement_checkpoint.lock().await = ClaudeReplacementCheckpoint::Armed;
        Ok(())
    }

    async fn confirm_replacement_checkpoint(&self) -> Result<()> {
        if !self.process_closed.load(Ordering::Acquire) {
            bail!("Claude provider process did not complete its graceful close barrier");
        }
        let mut checkpoint = self.replacement_checkpoint.lock().await;
        if *checkpoint != ClaudeReplacementCheckpoint::Armed {
            bail!("Claude replacement checkpoint was not armed before process close");
        }
        // The terminal stream event is the provider-owned journal checkpoint;
        // successful process shutdown above is the flush barrier. We never
        // infer continuity by replaying Pioneer's own timeline.
        self.client
            .wait_provider_identity_and_idle(Duration::from_millis(1))
            .await?;
        *checkpoint = ClaudeReplacementCheckpoint::Confirmed;
        Ok(())
    }

    fn take_event_receivers(&self) -> Option<CLIAgentRuntimeEventReceivers> {
        self.event_receivers
            .lock()
            .expect("Claude event receiver mutex should not be poisoned")
            .take()
    }

    async fn start_thread(
        &self,
        params: CLIAgentRuntimeThreadOpenParams,
        _timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        let native_thread_id = self.provider_session_id.to_string();
        *self.native_thread_id.lock().await = Some(native_thread_id.clone());
        self.client
            .set_native_thread_id(native_thread_id.clone())
            .await;
        Ok(CLIAgentRuntimeThreadOpenSnapshot {
            native_thread_id,
            cwd: Some(params.cwd),
            model: params.model,
            raw: json!({ "provider": "claude", "mode": "started" }),
        })
    }

    async fn resume_thread(
        &self,
        native_thread_id: &str,
        params: CLIAgentRuntimeThreadOpenParams,
        _timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        let native_thread_id = uuid::Uuid::parse_str(native_thread_id.trim())
            .context("Claude native thread id is not a UUID")?;
        if native_thread_id != self.provider_session_id {
            bail!("Claude native thread id does not match the launched provider session");
        }
        let native_thread_id = native_thread_id.to_string();
        *self.native_thread_id.lock().await = Some(native_thread_id.clone());
        self.client
            .set_native_thread_id(native_thread_id.clone())
            .await;
        Ok(CLIAgentRuntimeThreadOpenSnapshot {
            native_thread_id,
            cwd: Some(params.cwd),
            model: params.model,
            raw: json!({ "provider": "claude", "mode": "resumed_from_pioneer_history" }),
        })
    }

    async fn start_turn(
        &self,
        params: CLIAgentRuntimeTurnStartParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeTurnStartSnapshot> {
        if !matches!(
            *self.replacement_checkpoint.lock().await,
            ClaudeReplacementCheckpoint::Idle
        ) {
            bail!("Claude session is being replaced and cannot accept user input");
        }
        let native_turn_id = format!("claude_turn_{}", new_runtime_id());
        self.client
            .start_turn(
                params.native_thread_id.clone(),
                native_turn_id.clone(),
                params.model.clone(),
                params.effort.clone(),
                params.input.clone(),
                timeout,
            )
            .await?;
        Ok(CLIAgentRuntimeTurnStartSnapshot {
            native_thread_id: params.native_thread_id,
            native_turn_id,
            raw: json!({ "provider": "claude" }),
        })
    }

    async fn prepare_mcp_turn(
        &self,
        pioneer_turn_id: &str,
    ) -> Result<Option<CLIAgentRuntimeMcpTurnMetadata>> {
        self.client
            .wait_turn_preparation_barrier(self.request_timeout)
            .await?;
        match self.required_mcp_bridge.as_ref() {
            Some(bridge) => bridge.prepare_turn(pioneer_turn_id).await.map(Some),
            None => Ok(None),
        }
    }

    async fn activate_mcp_turn(
        &self,
        pioneer_turn_id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> Result<()> {
        if let Some(bridge) = self.required_mcp_bridge.as_ref() {
            bridge
                .activate_turn(pioneer_turn_id, native_thread_id, native_turn_id)
                .await?;
        }
        Ok(())
    }

    async fn terminal_mcp_turn(&self, pioneer_turn_id: &str) -> Result<()> {
        if let Some(bridge) = self.required_mcp_bridge.as_ref() {
            bridge.terminal_turn(pioneer_turn_id).await?;
        }
        Ok(())
    }

    async fn mcp_permission_fallback_count(&self) -> Result<usize> {
        Ok(self.client.state.lock().await.mcp_permission_fallback_count)
    }

    async fn respond_to_request(
        &self,
        native_request_id: JsonValue,
        response: JsonValue,
    ) -> Result<()> {
        let request_id = native_request_id
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| anyhow!("Claude native request id must be a string"))?;
        self.client
            .send_control_response(request_id, response)
            .await
            .context("failed to respond to Claude control request")
    }

    async fn interrupt_turn(
        &self,
        _native_thread_id: Option<&str>,
        _native_turn_id: Option<&str>,
    ) -> Result<()> {
        self.client
            .send_control_request(json!({ "subtype": "interrupt" }), self.request_timeout)
            .await?;
        Ok(())
    }

    async fn observe_turn(
        &self,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> Result<Option<CLIAgentRuntimeTurnObservation>> {
        let state = self.client.state.lock().await;
        if state.native_thread_id.as_deref() == Some(native_thread_id)
            && state.active_turn_id.as_deref() == Some(native_turn_id)
        {
            return Ok(Some(CLIAgentRuntimeTurnObservation {
                status: CLIAgentRuntimeObservedTurnStatus::InProgress,
                message: None,
                reconciliation_events: Vec::new(),
            }));
        }
        if state.native_thread_id.as_deref() == Some(native_thread_id)
            && state.observed_turn_id.as_deref() == Some(native_turn_id)
        {
            return Ok(state.last_turn_observation.clone());
        }
        Ok(None)
    }

    async fn steer_turn(
        &self,
        request: CLIAgentRuntimeTurnSteerRequest,
    ) -> Result<CLIAgentRuntimeTurnSteerResult> {
        let _ = request;
        bail!("Claude CLI does not support in-flight turn steering")
    }
}

#[derive(Clone)]
struct ClaudeProviderSessionVerifier {
    crud_store: Arc<CrudStore>,
    thread_id: String,
    expected_provider_session_id: uuid::Uuid,
    process_generation: u64,
}

#[derive(Clone)]
struct ClaudeMcpEventContext {
    runtime_id: String,
    session_generation: u64,
    manifest_hash: String,
    provider_contract_fingerprint: String,
    projection: ClaudeMcpSessionLaunchProjection,
    native_items: Arc<ClaudeNativeMcpCorrelationLedger>,
    permission_authorizer: Option<Arc<dyn ClaudeMcpPermissionAuthorizer>>,
}

struct ClaudeStreamClient {
    stdin: Mutex<ChildStdin>,
    pending: Mutex<HashMap<String, oneshot::Sender<Result<JsonValue, String>>>>,
    event_ingress: OrderedEventIngress<RuntimeEvent>,
    state: Mutex<ClaudeStreamState>,
    state_changed: tokio::sync::Notify,
    request_counter: AtomicU64,
    expected_provider_session_id: uuid::Uuid,
    provider_session_verifier: Option<ClaudeProviderSessionVerifier>,
    mcp: Option<ClaudeMcpEventContext>,
}

#[derive(Default)]
struct ClaudeStreamState {
    native_thread_id: Option<String>,
    active_turn_id: Option<String>,
    observed_turn_id: Option<String>,
    reconciliation_events: Vec<RuntimeEvent>,
    last_turn_observation: Option<CLIAgentRuntimeTurnObservation>,
    active_text_item_id: Option<String>,
    active_reasoning_item_id: Option<String>,
    active_text_item_started: bool,
    active_reasoning_item_started: bool,
    emitted_final_text: bool,
    tool_items: HashMap<String, ClaudeToolItemState>,
    mcp_items: HashMap<String, ClaudeMcpToolItemState>,
    completed_mcp_permission_requests: HashSet<String>,
    mcp_permission_fallback_count: usize,
    mcp_session_invalid: bool,
    provider_session_verified: bool,
    provider_session_invalid: bool,
}

impl ClaudeStreamState {
    fn is_pristine_provider_launch(&self) -> bool {
        !self.provider_session_verified
            && !self.provider_session_invalid
            && self.active_turn_id.is_none()
            && self.observed_turn_id.is_none()
            && self.last_turn_observation.is_none()
    }
}

pub(crate) fn claim_claude_mcp_permission_request(
    completed: &mut HashSet<String>,
    request_id: &str,
) -> bool {
    completed.insert(request_id.to_owned())
}

#[derive(Debug, Clone)]
struct ClaudeToolItemState {
    item_id: String,
    item_kind: String,
    tool_name: String,
    input: JsonValue,
}

#[derive(Debug, Clone)]
struct ClaudeMcpToolItemState {
    binding: crate::cli_runtime::claude_mcp::ClaudeNativeMcpItemBinding,
    lifecycle: ClaudeMcpToolLifecycle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaudeMcpToolLifecycle {
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl ClaudeStreamClient {
    fn new(
        stdin: ChildStdin,
        event_tx: mpsc::Sender<RuntimeEvent>,
        provider_session_verifier: ClaudeProviderSessionVerifier,
        mcp: Option<ClaudeMcpEventContext>,
    ) -> Self {
        let expected_provider_session_id = provider_session_verifier.expected_provider_session_id;
        Self {
            stdin: Mutex::new(stdin),
            pending: Mutex::new(HashMap::new()),
            event_ingress: OrderedEventIngress::spawn(event_tx, OrderedIngressConfig::default()),
            state: Mutex::new(ClaudeStreamState::default()),
            state_changed: tokio::sync::Notify::new(),
            request_counter: AtomicU64::new(0),
            expected_provider_session_id,
            provider_session_verifier: Some(provider_session_verifier),
            mcp,
        }
    }

    fn new_for_local_probe(
        stdin: ChildStdin,
        event_tx: mpsc::Sender<RuntimeEvent>,
        expected_provider_session_id: uuid::Uuid,
    ) -> Self {
        Self {
            stdin: Mutex::new(stdin),
            pending: Mutex::new(HashMap::new()),
            event_ingress: OrderedEventIngress::spawn(event_tx, OrderedIngressConfig::default()),
            state: Mutex::new(ClaudeStreamState::default()),
            state_changed: tokio::sync::Notify::new(),
            request_counter: AtomicU64::new(0),
            expected_provider_session_id,
            provider_session_verifier: None,
            mcp: None,
        }
    }

    #[cfg(test)]
    fn new_for_test_with_mcp(
        stdin: ChildStdin,
        event_tx: mpsc::Sender<RuntimeEvent>,
        expected_provider_session_id: uuid::Uuid,
        mcp: Option<ClaudeMcpEventContext>,
    ) -> Self {
        Self {
            stdin: Mutex::new(stdin),
            pending: Mutex::new(HashMap::new()),
            event_ingress: OrderedEventIngress::spawn(event_tx, OrderedIngressConfig::default()),
            state: Mutex::new(ClaudeStreamState::default()),
            state_changed: tokio::sync::Notify::new(),
            request_counter: AtomicU64::new(0),
            expected_provider_session_id,
            provider_session_verifier: None,
            mcp,
        }
    }

    fn spawn_reader(self: &Arc<Self>, stdout: ChildStdout) {
        let client = self.clone();
        tokio::spawn(async move {
            client.read_loop(stdout).await;
        });
    }

    async fn initialize(&self, timeout: Duration) -> Result<()> {
        self.send_control_request(json!({ "subtype": "initialize", "hooks": null }), timeout)
            .await?;
        Ok(())
    }

    async fn wait_provider_identity_and_idle(&self, wait: Duration) -> Result<()> {
        tokio_timeout(wait, async {
            loop {
                let notified = self.state_changed.notified();
                {
                    let state = self.state.lock().await;
                    if state.provider_session_invalid {
                        bail!("Claude provider session identity is invalid");
                    }
                    if state.mcp_session_invalid {
                        bail!("Claude MCP stream binding is invalid");
                    }
                    let terminal = state.active_turn_id.is_none()
                        && (state.observed_turn_id.is_none()
                            || state.last_turn_observation.is_some());
                    if state.provider_session_verified && terminal {
                        return Ok(());
                    }
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| anyhow!("Claude provider identity/terminal barrier timed out"))?
    }

    async fn wait_turn_preparation_barrier(&self, wait: Duration) -> Result<()> {
        {
            let state = self.state.lock().await;
            if state.provider_session_invalid {
                bail!("Claude provider session identity is invalid");
            }
            if state.mcp_session_invalid {
                bail!("Claude MCP stream binding is invalid");
            }
            // Claude 2.1.197 does not emit its stream-owned session identity
            // until the first user frame starts the provider stream. The UUID
            // is still prepared durably and supplied through typed
            // `--session-id` before spawn. Permit only that pristine first-turn
            // state to stage the MCP lease; every provider event remains gated
            // by `verify_provider_session_message`, so no model/tool event is
            // accepted and no MCP permission can succeed before the emitted
            // UUID matches the prepared binding.
            if state.is_pristine_provider_launch() {
                return Ok(());
            }
        }
        self.wait_provider_identity_and_idle(wait).await
    }

    async fn set_native_thread_id(&self, native_thread_id: String) {
        let mut state = self.state.lock().await;
        state.native_thread_id = Some(native_thread_id);
    }

    async fn start_turn(
        &self,
        native_thread_id: String,
        native_turn_id: String,
        model: Option<String>,
        effort: Option<String>,
        input: JsonValue,
        timeout: Duration,
    ) -> Result<()> {
        if let Some(model) = model.filter(|model| !model.trim().is_empty()) {
            self.send_control_request(json!({ "subtype": "set_model", "model": model }), timeout)
                .await?;
        }
        if let Some(effort) = effort
            .as_deref()
            .map(str::trim)
            .filter(|effort| !effort.is_empty())
        {
            self.send_control_request(
                json!({
                    "subtype": "apply_flag_settings",
                    "settings": {
                        "effortLevel": effort,
                    },
                }),
                timeout,
            )
            .await?;
        }
        {
            let mut state = self.state.lock().await;
            state.native_thread_id = Some(native_thread_id.clone());
            state.active_turn_id = Some(native_turn_id.clone());
            state.observed_turn_id = Some(native_turn_id.clone());
            state.reconciliation_events.clear();
            state.last_turn_observation = None;
            state.active_text_item_id = None;
            state.active_reasoning_item_id = None;
            state.active_text_item_started = false;
            state.active_reasoning_item_started = false;
            state.emitted_final_text = false;
            state.tool_items.clear();
            state.mcp_items.clear();
            state.completed_mcp_permission_requests.clear();
            state.mcp_permission_fallback_count = 0;
        }
        self.emit(RuntimeEvent::TurnStarted(RuntimeTurnStarted {
            native_thread_id: Some(native_thread_id.clone()),
            native_turn_id: native_turn_id.clone(),
            native: Some(native_event(
                "turn/started",
                json!({ "provider": "claude" }),
            )),
        }))
        .await;

        let prompt = claude_prompt_from_input(input)?;
        self.write_json_line(json!({
            "type": "user",
            "message": { "role": "user", "content": prompt },
            "parent_tool_use_id": null,
            "session_id": self.expected_provider_session_id.to_string(),
        }))
        .await
    }

    async fn send_control_request(
        &self,
        request: JsonValue,
        timeout_value: Duration,
    ) -> Result<JsonValue> {
        let request_id = format!(
            "req_{}_{}",
            self.request_counter.fetch_add(1, Ordering::Relaxed),
            new_runtime_id()
        );
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(request_id.clone(), tx);
        if let Err(error) = self
            .write_json_line(json!({
                "type": "control_request",
                "request_id": request_id,
                "request": request,
            }))
            .await
        {
            self.pending.lock().await.remove(&request_id);
            return Err(error);
        }
        match tokio::time::timeout(timeout_value, rx).await {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(error))) => {
                self.pending.lock().await.remove(&request_id);
                bail!("{error}")
            }
            Ok(Err(_)) => {
                self.pending.lock().await.remove(&request_id);
                bail!("Claude control request response channel closed")
            }
            Err(_) => {
                self.pending.lock().await.remove(&request_id);
                bail!("Claude control request timed out")
            }
        }
    }

    async fn send_control_response(&self, request_id: String, response: JsonValue) -> Result<()> {
        self.write_json_line(json!({
            "type": "control_response",
            "response": {
                "subtype": "success",
                "request_id": request_id,
                "response": response,
            }
        }))
        .await
    }

    async fn write_json_line(&self, value: JsonValue) -> Result<()> {
        let mut stdin = self.stdin.lock().await;
        let line = serde_json::to_string(&value).context("failed to encode Claude JSON line")?;
        stdin
            .write_all(line.as_bytes())
            .await
            .context("failed to write Claude JSON line")?;
        stdin
            .write_all(b"\n")
            .await
            .context("failed to write Claude JSON newline")?;
        stdin.flush().await.context("failed to flush Claude stdin")
    }

    async fn read_loop(&self, stdout: ChildStdout) {
        let mut lines = BufReader::new(stdout).lines();
        let mut buffer = String::new();
        loop {
            let line = match lines.next_line().await {
                Ok(Some(line)) => line,
                Ok(None) => break,
                Err(error) => {
                    let (native_thread_id, native_turn_id) = {
                        let state = self.state.lock().await;
                        (state.native_thread_id.clone(), state.active_turn_id.clone())
                    };
                    self.emit(RuntimeEvent::Error(RuntimeErrorEvent {
                        native_thread_id,
                        native_turn_id,
                        message: format!("Claude stdout read failed: {error}"),
                        code: Some("claude_stdout_read_failed".to_owned()),
                        retryable: false,
                        native: None,
                    }))
                    .await;
                    break;
                }
            };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if buffer.is_empty() && !trimmed.starts_with('{') {
                continue;
            }
            buffer.push_str(trimmed);
            let value = match serde_json::from_str::<JsonValue>(&buffer) {
                Ok(value) => {
                    buffer.clear();
                    value
                }
                Err(_) => continue,
            };
            self.handle_incoming(value).await;
        }
        self.fail_pending_requests("Claude CLI process ended".to_owned())
            .await;
        let (native_thread_id, native_turn_id, mcp_terminal_events) = {
            let mut state = self.state.lock().await;
            let ids = (state.native_thread_id.clone(), state.active_turn_id.clone());
            let mcp_terminal_events = terminalize_running_claude_mcp_items(
                &mut state,
                ClaudeMcpToolLifecycle::Failed,
                "Claude process disconnected before the MCP call reached a terminal result",
                "process_eof/mcp_terminal_reconciliation",
            );
            if ids.1.is_some() {
                state.active_turn_id = None;
                state.active_text_item_id = None;
                state.active_reasoning_item_id = None;
                state.active_text_item_started = false;
                state.active_reasoning_item_started = false;
                state.emitted_final_text = false;
                state.tool_items.clear();
            }
            (ids.0, ids.1, mcp_terminal_events)
        };
        for event in mcp_terminal_events {
            self.emit(event).await;
        }
        if let Some(native_turn_id) = native_turn_id {
            self.emit(RuntimeEvent::TurnFailed(RuntimeTurnFailed {
                native_thread_id,
                native_turn_id: Some(native_turn_id),
                message: "Claude CLI process ended before the turn completed".to_owned(),
                code: Some("claude_process_exited".to_owned()),
                native: Some(native_event("process/eof", json!({ "provider": "claude" }))),
            }))
            .await;
        }
        self.event_ingress.close();
    }

    async fn handle_incoming(&self, value: JsonValue) {
        let message_type = value.get("type").and_then(JsonValue::as_str);
        let provider_message = !matches!(
            message_type,
            Some("control_response" | "control_request" | "control_cancel_request")
        ) || (message_type == Some("control_response")
            && claude_emitted_session_id(&value).is_some());
        if provider_message && let Err(error) = self.verify_provider_session_message(&value).await {
            let (native_thread_id, native_turn_id) = {
                let state = self.state.lock().await;
                (state.native_thread_id.clone(), state.active_turn_id.clone())
            };
            self.emit(RuntimeEvent::Error(RuntimeErrorEvent {
                native_thread_id,
                native_turn_id,
                message: "Claude provider session identity verification failed".to_owned(),
                code: Some("claude_session_identity_invalid".to_owned()),
                retryable: false,
                native: Some(native_event(
                    "provider_session/invalid",
                    json!({ "provider": "claude", "detail": error.to_string() }),
                )),
            }))
            .await;
            return;
        }
        match message_type {
            Some("control_response") => {
                self.handle_control_response(value).await;
            }
            Some("control_request") => {
                self.handle_control_request(value).await;
            }
            Some("control_cancel_request") => {
                if let Some(request_id) = value.get("request_id").and_then(JsonValue::as_str) {
                    self.emit(RuntimeEvent::RequestResolved(RuntimeRequestResolved {
                        native_request_id: request_id.to_owned(),
                        native: Some(native_event("control_cancel_request", value)),
                    }))
                    .await;
                }
            }
            _ => {
                for event in self.map_message(value).await {
                    self.emit(event).await;
                }
            }
        }
    }

    async fn verify_provider_session_message(&self, value: &JsonValue) -> Result<()> {
        let message_type = value.get("type").and_then(JsonValue::as_str);
        let subtype = value.get("subtype").and_then(JsonValue::as_str);
        let emitted = claude_emitted_session_id(value);
        let requires_identity = matches!(message_type, Some("assistant" | "result"))
            || matches!((message_type, subtype), (Some("system"), Some("init")));

        {
            let state = self.state.lock().await;
            if state.provider_session_invalid {
                bail!("Claude provider session binding is already invalid");
            }
            if emitted.is_none() && !requires_identity {
                if !state.provider_session_verified {
                    drop(state);
                    self.persist_provider_session_identity(None).await?;
                    bail!("Claude message arrived before session identity verification");
                }
                return Ok(());
            }
            if state.provider_session_verified
                && emitted.and_then(|value| uuid::Uuid::parse_str(value).ok())
                    == Some(self.expected_provider_session_id)
            {
                return Ok(());
            }
        }

        self.persist_provider_session_identity(emitted).await
    }

    async fn persist_provider_session_identity(&self, emitted: Option<&str>) -> Result<()> {
        let expected = self.expected_provider_session_id.to_string();
        let Some(verifier) = self.provider_session_verifier.as_ref() else {
            let mut state = self.state.lock().await;
            if emitted.and_then(|value| uuid::Uuid::parse_str(value).ok())
                == Some(self.expected_provider_session_id)
            {
                state.provider_session_verified = true;
                self.state_changed.notify_waiters();
                return Ok(());
            }
            state.provider_session_invalid = true;
            self.state_changed.notify_waiters();
            bail!("Claude test stream emitted an invalid provider session identity");
        };
        let process_generation = i64::try_from(verifier.process_generation)
            .context("Claude process generation exceeds durable range")?;
        let result = verifier
            .crud_store
            .verify_claude_provider_session_binding(
                verifier.thread_id.as_str(),
                expected.as_str(),
                emitted,
                process_generation,
            )
            .await;
        let mut state = self.state.lock().await;
        match result {
            Ok(_) => {
                state.provider_session_verified = true;
                self.state_changed.notify_waiters();
                Ok(())
            }
            Err(error) => {
                state.provider_session_invalid = true;
                self.state_changed.notify_waiters();
                Err(error)
            }
        }
    }

    async fn fail_pending_requests(&self, message: String) {
        let pending = {
            let mut pending = self.pending.lock().await;
            pending
                .drain()
                .map(|(_, sender)| sender)
                .collect::<Vec<_>>()
        };
        for sender in pending {
            let _ = sender.send(Err(message.clone()));
        }
    }

    async fn handle_control_response(&self, value: JsonValue) {
        let response = value.get("response").cloned().unwrap_or(JsonValue::Null);
        let Some(request_id) = response.get("request_id").and_then(JsonValue::as_str) else {
            return;
        };
        let result = if response.get("subtype").and_then(JsonValue::as_str) == Some("error") {
            Err(response
                .get("error")
                .and_then(JsonValue::as_str)
                .unwrap_or("Claude control request failed")
                .to_owned())
        } else {
            Ok(response.get("response").cloned().unwrap_or(JsonValue::Null))
        };
        if let Some(tx) = self.pending.lock().await.remove(request_id) {
            let _ = tx.send(result);
        }
    }

    async fn handle_control_request(&self, value: JsonValue) {
        let request_id = value
            .get("request_id")
            .and_then(JsonValue::as_str)
            .unwrap_or("")
            .to_owned();
        if request_id.is_empty() {
            return;
        }
        let request = value.get("request").cloned().unwrap_or(JsonValue::Null);
        if is_claude_native_mcp_permission_candidate(&request) {
            self.handle_mcp_permission_fallback(request_id, request)
                .await;
            return;
        }
        if request.get("subtype").and_then(JsonValue::as_str) != Some("can_use_tool") {
            self.send_control_response(
                request_id,
                json!({ "behavior": "deny", "message": "Unsupported Claude control request" }),
            )
            .await
            .ok();
            return;
        }
        let tool_name = request
            .get("tool_name")
            .and_then(JsonValue::as_str)
            .unwrap_or("tool")
            .to_owned();
        let (native_thread_id, active_turn_id, payload) = {
            let state = self.state.lock().await;
            let tool_input = request.get("input").cloned().unwrap_or(JsonValue::Null);
            let native_thread_id = state.native_thread_id.clone();
            let active_turn_id = state.active_turn_id.clone();
            let payload = json!({
                "nativeRequestId": request_id,
                "nativeRequestIdJson": request_id,
                "toolName": tool_name,
                "command": command_from_claude_tool(tool_name.as_str(), &tool_input),
                "input": tool_input,
                "title": request.get("title").cloned(),
                "displayName": request.get("display_name").cloned(),
                "description": request.get("description").cloned(),
                "reason": request.get("decision_reason").cloned(),
                "threadId": native_thread_id,
                "turnId": active_turn_id,
                "itemId": request.get("tool_use_id").and_then(JsonValue::as_str),
                "raw": request,
            });
            (native_thread_id, active_turn_id, payload)
        };
        self.emit(RuntimeEvent::RequestOpened(RuntimeRequestOpened {
            native_request_id: request_id,
            native_request_id_json: payload.get("nativeRequestIdJson").cloned(),
            request_kind: request_kind_for_claude_tool(tool_name.as_str()).to_owned(),
            native_thread_id,
            native_turn_id: active_turn_id,
            native_item_id: payload
                .get("itemId")
                .and_then(JsonValue::as_str)
                .map(str::to_owned),
            payload_redacted: Some(payload),
            native: Some(native_event("control_request/can_use_tool", value)),
        }))
        .await;
    }

    async fn handle_mcp_permission_fallback(&self, request_id: String, request: JsonValue) {
        let (native_thread_id, native_turn_id, provider_session_verified) = {
            let mut state = self.state.lock().await;
            if !claim_claude_mcp_permission_request(
                &mut state.completed_mcp_permission_requests,
                request_id.as_str(),
            ) {
                return;
            }
            (
                state.native_thread_id.clone(),
                state.active_turn_id.clone(),
                state.provider_session_verified && !state.provider_session_invalid,
            )
        };
        let Some(context) = self.mcp.as_ref() else {
            let response = claude_mcp_permission_fallback_response(
                ClaudeMcpPermissionFallbackDecision::Deny {
                    reason: "Pioneer MCP is not active for this Claude process".to_owned(),
                },
                request.get("input").unwrap_or(&JsonValue::Null),
            );
            let _ = self.send_control_response(request_id, response).await;
            return;
        };
        let parsed = match (native_thread_id.as_deref(), native_turn_id.as_deref()) {
            (Some(native_thread_id), Some(native_turn_id)) if provider_session_verified => {
                parse_claude_native_mcp_permission_request(
                    &request,
                    context.runtime_id.as_str(),
                    context.session_generation,
                    native_thread_id,
                    native_turn_id,
                    context.manifest_hash.as_str(),
                    context.provider_contract_fingerprint.as_str(),
                )
            }
            _ => Err(
                crate::cli_runtime::claude_mcp::ClaudeNativeMcpPermissionParseError::InvalidIdentity,
            ),
        };
        let decision = match parsed {
            Ok(parsed) => {
                let binding = context.projection.bind_native_tool_use(
                    context.runtime_id.as_str(),
                    context.session_generation,
                    parsed.native_thread_id.as_str(),
                    parsed.native_turn_id.as_str(),
                    parsed.native_item_id.as_str(),
                    parsed.qualified_tool_name.as_str(),
                    &parsed.arguments,
                );
                match binding.and_then(|binding| {
                    context
                        .native_items
                        .register(&binding)
                        .map(|()| binding)
                        .map_err(|_| {
                            crate::cli_runtime::claude_mcp::ClaudeNativeMcpEventError::Serialization
                        })
                }) {
                    Ok(_) => match context.permission_authorizer.as_ref() {
                        Some(authorizer) => match authorizer.authorize_permission(&parsed).await {
                            Ok(true) => ClaudeMcpPermissionFallbackDecision::AllowExact,
                            Ok(false) => ClaudeMcpPermissionFallbackDecision::Deny {
                                reason: "Claude MCP permission request does not match the active frozen binding"
                                    .to_owned(),
                            },
                            Err(error) => ClaudeMcpPermissionFallbackDecision::Deny {
                                reason: format!("Claude MCP permission validation failed: {error}"),
                            },
                        },
                        None => ClaudeMcpPermissionFallbackDecision::Deny {
                            reason: "Claude MCP permission authorizer is unavailable".to_owned(),
                        },
                    },
                    Err(error) => ClaudeMcpPermissionFallbackDecision::Deny {
                        reason: error.to_string(),
                    },
                }
            }
            Err(error) => ClaudeMcpPermissionFallbackDecision::Deny {
                reason: error.to_string(),
            },
        };
        let allowed_exact = matches!(&decision, ClaudeMcpPermissionFallbackDecision::AllowExact);
        let response = claude_mcp_permission_fallback_response(
            decision,
            request.get("input").unwrap_or(&JsonValue::Null),
        );
        if allowed_exact {
            let mut state = self.state.lock().await;
            state.mcp_permission_fallback_count =
                state.mcp_permission_fallback_count.saturating_add(1);
        }
        if let Err(error) = self.send_control_response(request_id, response).await {
            tracing::debug!(
                error = %format!("{error:#}"),
                "Claude MCP permission callback lane closed before the bounded response"
            );
        }
    }

    async fn map_message(&self, value: JsonValue) -> Vec<RuntimeEvent> {
        match value.get("type").and_then(JsonValue::as_str) {
            Some("system") => self.map_system_message(value).await,
            Some("stream_event") => self.map_stream_event(value).await,
            Some("assistant") => self.map_assistant_message(value).await,
            Some("user") => self.map_user_message(value).await,
            Some("result") => self.map_result_message(value).await,
            Some("error") => self.map_error_message(value).await,
            _ => Vec::new(),
        }
    }

    async fn map_system_message(&self, value: JsonValue) -> Vec<RuntimeEvent> {
        let state = self.state.lock().await;
        let subtype = value
            .get("subtype")
            .and_then(JsonValue::as_str)
            .unwrap_or("system");
        match subtype {
            "session_state_changed" => vec![RuntimeEvent::ThreadStateChanged(
                RuntimeThreadStateChanged {
                    native_thread_id: state.native_thread_id.clone(),
                    status: value
                        .get("status")
                        .and_then(JsonValue::as_str)
                        .unwrap_or("changed")
                        .to_owned(),
                    native: Some(native_event("system/session_state_changed", value)),
                },
            )],
            "permission_denied" => vec![RuntimeEvent::Error(RuntimeErrorEvent {
                native_thread_id: state.native_thread_id.clone(),
                native_turn_id: state.active_turn_id.clone(),
                message: value
                    .get("message")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("Claude denied a tool call")
                    .to_owned(),
                code: Some("claude_permission_denied".to_owned()),
                retryable: false,
                native: Some(native_event("system/permission_denied", value)),
            })],
            _ => Vec::new(),
        }
    }

    async fn map_stream_event(&self, value: JsonValue) -> Vec<RuntimeEvent> {
        let event = value.get("event").cloned().unwrap_or(JsonValue::Null);
        let event_type = event.get("type").and_then(JsonValue::as_str);
        let mut state = self.state.lock().await;
        let Some(native_thread_id) = state.native_thread_id.clone() else {
            return Vec::new();
        };
        let Some(native_turn_id) = state.active_turn_id.clone() else {
            return Vec::new();
        };
        match event_type {
            Some("content_block_delta") => {
                let delta = event.get("delta").cloned().unwrap_or(JsonValue::Null);
                let delta_type = delta.get("type").and_then(JsonValue::as_str);
                let text = delta
                    .get("text")
                    .or_else(|| delta.get("thinking"))
                    .and_then(JsonValue::as_str)
                    .unwrap_or("");
                if text.is_empty() {
                    return Vec::new();
                }
                let mut events = Vec::new();
                let (item_id, item_kind, delta_kind) = if delta_type == Some("thinking_delta") {
                    let item_id = state
                        .active_reasoning_item_id
                        .get_or_insert_with(|| format!("claude_reasoning_{}", new_runtime_id()))
                        .clone();
                    if !state.active_reasoning_item_started {
                        state.active_reasoning_item_started = true;
                        events.push(item_started(
                            &state,
                            item_id.as_str(),
                            "reasoning",
                            None,
                            None,
                        ));
                    }
                    (
                        item_id,
                        "reasoning".to_owned(),
                        RuntimeItemDeltaKind::ReasoningSummary,
                    )
                } else {
                    let item_id = state
                        .active_text_item_id
                        .get_or_insert_with(|| format!("claude_message_{}", new_runtime_id()))
                        .clone();
                    if !state.active_text_item_started {
                        state.active_text_item_started = true;
                        events.push(item_started(
                            &state,
                            item_id.as_str(),
                            "agentMessage",
                            None,
                            None,
                        ));
                    }
                    (
                        item_id,
                        "agentMessage".to_owned(),
                        RuntimeItemDeltaKind::AgentMessage,
                    )
                };
                events.push(RuntimeEvent::ItemDelta(RuntimeItemDelta {
                    native_thread_id: Some(native_thread_id),
                    native_turn_id,
                    native_item_id: item_id,
                    item_kind,
                    delta_kind,
                    delta: text.to_owned(),
                    metadata: None,
                    native: Some(native_event("stream_event/content_block_delta", value)),
                }));
                events
            }
            _ => Vec::new(),
        }
    }

    async fn map_assistant_message(&self, value: JsonValue) -> Vec<RuntimeEvent> {
        let content = value
            .get("message")
            .and_then(|message| message.get("content"))
            .and_then(JsonValue::as_array)
            .cloned()
            .unwrap_or_default();
        let mut events = Vec::new();
        for (index, block) in content.into_iter().enumerate() {
            match block.get("type").and_then(JsonValue::as_str) {
                Some("text") => {
                    let text = block
                        .get("text")
                        .and_then(JsonValue::as_str)
                        .unwrap_or("")
                        .to_owned();
                    if text.is_empty() {
                        continue;
                    }
                    let mut state = self.state.lock().await;
                    let item_id = state
                        .active_text_item_id
                        .take()
                        .unwrap_or_else(|| claude_item_id(&value, "message", index));
                    let item_started_emitted = state.active_text_item_started;
                    state.active_text_item_started = false;
                    state.emitted_final_text = true;
                    if !item_started_emitted {
                        events.push(item_started(
                            &state,
                            item_id.as_str(),
                            "agentMessage",
                            None,
                            None,
                        ));
                    }
                    events.push(RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                        native_thread_id: state.native_thread_id.clone(),
                        native_turn_id: state.active_turn_id.clone().unwrap_or_default(),
                        native_item_id: item_id,
                        item_kind: "agentMessage".to_owned(),
                        text: Some(text),
                        summary: Vec::new(),
                        content: Vec::new(),
                        phase: RuntimeAgentMessagePhase::FinalAnswer,
                        metadata: None,
                        native_item_redacted: Some(block),
                        native: Some(native_event("assistant/text", value.clone())),
                    }));
                }
                Some("thinking") => {
                    let thinking = block
                        .get("thinking")
                        .and_then(JsonValue::as_str)
                        .unwrap_or("")
                        .to_owned();
                    let mut state = self.state.lock().await;
                    let item_id = state
                        .active_reasoning_item_id
                        .take()
                        .unwrap_or_else(|| claude_item_id(&value, "reasoning", index));
                    let item_started_emitted = state.active_reasoning_item_started;
                    state.active_reasoning_item_started = false;
                    if !item_started_emitted {
                        events.push(item_started(
                            &state,
                            item_id.as_str(),
                            "reasoning",
                            None,
                            None,
                        ));
                    }
                    events.push(RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                        native_thread_id: state.native_thread_id.clone(),
                        native_turn_id: state.active_turn_id.clone().unwrap_or_default(),
                        native_item_id: item_id,
                        item_kind: "reasoning".to_owned(),
                        text: None,
                        summary: if thinking.is_empty() {
                            Vec::new()
                        } else {
                            vec![thinking]
                        },
                        content: Vec::new(),
                        phase: RuntimeAgentMessagePhase::FinalAnswer,
                        metadata: None,
                        native_item_redacted: Some(block),
                        native: Some(native_event("assistant/thinking", value.clone())),
                    }));
                }
                Some("tool_use") => {
                    let tool_name = block
                        .get("name")
                        .and_then(JsonValue::as_str)
                        .unwrap_or("tool")
                        .to_owned();
                    if tool_name.starts_with(
                        pioneer_cli_agent_runtime::claude::CLAUDE_PIONEER_MCP_TOOL_PREFIX,
                    ) {
                        events.extend(self.map_mcp_tool_use_block(&value, &block, index).await);
                        continue;
                    }
                    let mut state = self.state.lock().await;
                    let tool_id = block
                        .get("id")
                        .and_then(JsonValue::as_str)
                        .map(str::to_owned)
                        .unwrap_or_else(|| claude_item_id(&value, "tool", index));
                    let input = block.get("input").cloned().unwrap_or(JsonValue::Null);
                    let item_kind = item_kind_for_claude_tool(tool_name.as_str()).to_owned();
                    let metadata = metadata_for_claude_tool(tool_name.as_str(), &input, None, None);
                    events.push(item_started(
                        &state,
                        tool_id.as_str(),
                        item_kind.as_str(),
                        Some(tool_name.clone()),
                        Some(metadata),
                    ));
                    state.tool_items.insert(
                        tool_id.clone(),
                        ClaudeToolItemState {
                            item_id: tool_id,
                            item_kind,
                            tool_name,
                            input,
                        },
                    );
                }
                Some("tool_result") => {
                    events.extend(self.map_tool_result_block(&value, &block).await);
                }
                _ => {}
            }
        }
        events
    }

    async fn map_user_message(&self, value: JsonValue) -> Vec<RuntimeEvent> {
        let content = value
            .get("message")
            .and_then(|message| message.get("content"))
            .and_then(JsonValue::as_array)
            .cloned()
            .unwrap_or_default();
        let mut events = Vec::new();
        for block in content {
            if block.get("type").and_then(JsonValue::as_str) == Some("tool_result") {
                events.extend(self.map_tool_result_block(&value, &block).await);
            }
        }
        events
    }

    async fn map_mcp_tool_use_block(
        &self,
        value: &JsonValue,
        block: &JsonValue,
        index: usize,
    ) -> Vec<RuntimeEvent> {
        let tool_use_id = block
            .get("id")
            .and_then(JsonValue::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| claude_item_id(value, "mcp-tool", index));
        let qualified_tool_name = block
            .get("name")
            .and_then(JsonValue::as_str)
            .unwrap_or_default();
        let arguments = block
            .get("input")
            .cloned()
            .unwrap_or_else(|| JsonValue::Object(Default::default()));
        let Some(context) = self.mcp.as_ref() else {
            let mut state = self.state.lock().await;
            state.mcp_session_invalid = true;
            self.state_changed.notify_waiters();
            return vec![claude_mcp_stream_error(
                &state,
                "Claude emitted a Pioneer MCP tool without an active frozen projection",
                "claude_mcp_projection_missing",
                value.clone(),
            )];
        };
        let mut state = self.state.lock().await;
        let (Some(native_thread_id), Some(native_turn_id)) =
            (state.native_thread_id.clone(), state.active_turn_id.clone())
        else {
            state.mcp_session_invalid = true;
            self.state_changed.notify_waiters();
            return vec![claude_mcp_stream_error(
                &state,
                "Claude MCP tool_use arrived without an active provider turn",
                "claude_mcp_turn_binding_missing",
                value.clone(),
            )];
        };
        let binding = match context.projection.bind_native_tool_use(
            context.runtime_id.as_str(),
            context.session_generation,
            native_thread_id.as_str(),
            native_turn_id.as_str(),
            tool_use_id.as_str(),
            qualified_tool_name,
            &arguments,
        ) {
            Ok(binding) => binding,
            Err(error) => {
                state.mcp_session_invalid = true;
                self.state_changed.notify_waiters();
                return vec![claude_mcp_stream_error(
                    &state,
                    error.to_string().as_str(),
                    "claude_mcp_binding_invalid",
                    value.clone(),
                )];
            }
        };
        if let Some(existing) = state.mcp_items.get(tool_use_id.as_str()) {
            if existing.binding.canonical_callable_name == binding.canonical_callable_name
                && existing.binding.arguments_fingerprint == binding.arguments_fingerprint
            {
                return Vec::new();
            }
            state.mcp_session_invalid = true;
            self.state_changed.notify_waiters();
            return vec![claude_mcp_stream_error(
                &state,
                "Claude replay changed an existing MCP tool_use identity",
                "claude_mcp_replay_mismatch",
                value.clone(),
            )];
        }
        if let Err(error) = context.native_items.register(&binding) {
            state.mcp_session_invalid = true;
            self.state_changed.notify_waiters();
            return vec![claude_mcp_stream_error(
                &state,
                error.to_string().as_str(),
                "claude_mcp_correlation_invalid",
                value.clone(),
            )];
        }
        let event = RuntimeEvent::ItemStarted(RuntimeItemStarted {
            native_thread_id: Some(binding.native_thread_id.clone()),
            native_turn_id: binding.native_turn_id.clone(),
            native_item_id: binding.native_item_id.clone(),
            item_kind: "mcpToolCall".to_owned(),
            title: Some(binding.canonical_callable_name.clone()),
            phase: RuntimeAgentMessagePhase::FinalAnswer,
            metadata: Some(binding.metadata.to_json()),
            native_item_redacted: Some(block.clone()),
            native: Some(native_event("assistant/mcp_tool_use", value.clone())),
        });
        state.mcp_items.insert(
            tool_use_id,
            ClaudeMcpToolItemState {
                binding,
                lifecycle: ClaudeMcpToolLifecycle::Running,
            },
        );
        vec![event]
    }

    async fn map_tool_result_block(
        &self,
        value: &JsonValue,
        block: &JsonValue,
    ) -> Vec<RuntimeEvent> {
        let tool_use_id = block
            .get("tool_use_id")
            .and_then(JsonValue::as_str)
            .unwrap_or_default();
        let output = claude_tool_result_text(block);
        let is_error = block
            .get("is_error")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false);
        let mut state = self.state.lock().await;
        if let Some(tool) = state.mcp_items.get_mut(tool_use_id) {
            if tool.lifecycle != ClaudeMcpToolLifecycle::Running {
                return Vec::new();
            }
            tool.lifecycle = if is_error {
                ClaudeMcpToolLifecycle::Failed
            } else {
                ClaudeMcpToolLifecycle::Completed
            };
            let mut metadata = tool.binding.metadata.clone();
            metadata.insert(
                "status".to_owned(),
                ToolMetadataValue::from_json(JsonValue::String(
                    if is_error { "failed" } else { "completed" }.to_owned(),
                )),
            );
            metadata.insert(
                "success".to_owned(),
                ToolMetadataValue::from_json(JsonValue::Bool(!is_error)),
            );
            metadata.insert(
                "message".to_owned(),
                ToolMetadataValue::from_json(JsonValue::String(output.clone())),
            );
            if is_error {
                metadata.insert(
                    "error".to_owned(),
                    ToolMetadataValue::from_json(
                        json!({"message": if output.is_empty() { "Claude MCP tool failed" } else { output.as_str() }}),
                    ),
                );
            }
            return vec![RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                native_thread_id: Some(tool.binding.native_thread_id.clone()),
                native_turn_id: tool.binding.native_turn_id.clone(),
                native_item_id: tool.binding.native_item_id.clone(),
                item_kind: "mcpToolCall".to_owned(),
                text: Some(output),
                summary: Vec::new(),
                content: Vec::new(),
                phase: RuntimeAgentMessagePhase::FinalAnswer,
                metadata: Some(metadata.to_json()),
                native_item_redacted: Some(block.clone()),
                native: Some(native_event("user/mcp_tool_result", value.clone())),
            })];
        }
        let Some(tool) = state.tool_items.remove(tool_use_id) else {
            return Vec::new();
        };
        let metadata = metadata_for_claude_tool(
            tool.tool_name.as_str(),
            &tool.input,
            Some(output.as_str()),
            Some(!is_error),
        );
        vec![RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
            native_thread_id: state.native_thread_id.clone(),
            native_turn_id: state.active_turn_id.clone().unwrap_or_default(),
            native_item_id: tool.item_id,
            item_kind: tool.item_kind,
            text: Some(output),
            summary: Vec::new(),
            content: Vec::new(),
            phase: RuntimeAgentMessagePhase::FinalAnswer,
            metadata: Some(metadata),
            native_item_redacted: Some(block.clone()),
            native: Some(native_event("user/tool_result", value.clone())),
        })]
    }

    async fn map_result_message(&self, value: JsonValue) -> Vec<RuntimeEvent> {
        let mut state = self.state.lock().await;
        let Some(native_thread_id) = state.native_thread_id.clone() else {
            return Vec::new();
        };
        let Some(native_turn_id) = state.active_turn_id.clone() else {
            return Vec::new();
        };
        let is_error = value
            .get("is_error")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false);
        let interrupted = matches!(
            value.get("subtype").and_then(JsonValue::as_str),
            Some("interrupted" | "cancelled" | "canceled")
        );
        let mut events = Vec::new();
        if !state.emitted_final_text
            && let Some(result) = value
                .get("result")
                .and_then(JsonValue::as_str)
                .filter(|text| !text.trim().is_empty())
        {
            let item_id = state
                .active_text_item_id
                .take()
                .unwrap_or_else(|| format!("claude_result_{}", new_runtime_id()));
            let item_started_emitted = state.active_text_item_started;
            state.active_text_item_started = false;
            if !item_started_emitted {
                events.push(item_started(
                    &state,
                    item_id.as_str(),
                    "agentMessage",
                    None,
                    None,
                ));
            }
            events.push(RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                native_thread_id: Some(native_thread_id.clone()),
                native_turn_id: native_turn_id.clone(),
                native_item_id: item_id,
                item_kind: "agentMessage".to_owned(),
                text: Some(result.to_owned()),
                summary: Vec::new(),
                content: Vec::new(),
                phase: RuntimeAgentMessagePhase::FinalAnswer,
                metadata: None,
                native_item_redacted: None,
                native: Some(native_event("result/final_text", value.clone())),
            }));
        }
        events.extend(terminalize_running_claude_mcp_items(
            &mut state,
            if interrupted {
                ClaudeMcpToolLifecycle::Cancelled
            } else {
                ClaudeMcpToolLifecycle::Failed
            },
            if interrupted {
                "Claude turn ended while the MCP call was cancelled"
            } else {
                "Claude turn ended before an MCP tool_result was observed"
            },
            if interrupted {
                "result/mcp_cancelled_reconciliation"
            } else {
                "result/mcp_terminal_reconciliation"
            },
        ));
        if interrupted {
            events.push(RuntimeEvent::TurnInterrupted(RuntimeTurnInterrupted {
                native_thread_id: Some(native_thread_id),
                native_turn_id: native_turn_id.clone(),
                reason: claude_result_error_message(&value),
                native: Some(native_event("result/interrupted", value)),
            }));
        } else if is_error {
            events.push(RuntimeEvent::TurnFailed(RuntimeTurnFailed {
                native_thread_id: Some(native_thread_id),
                native_turn_id: Some(native_turn_id.clone()),
                message: claude_result_error_message(&value),
                code: value
                    .get("subtype")
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned),
                native: Some(native_event("result/error", value)),
            }));
        } else {
            events.push(RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
                native_thread_id: Some(native_thread_id),
                native_turn_id: native_turn_id.clone(),
                status: "completed".to_owned(),
                native: Some(native_event("result/success", value)),
            }));
        }
        state.active_turn_id = None;
        state.active_text_item_id = None;
        state.active_reasoning_item_id = None;
        state.active_text_item_started = false;
        state.active_reasoning_item_started = false;
        state.emitted_final_text = false;
        state.tool_items.clear();
        events
    }

    async fn map_error_message(&self, value: JsonValue) -> Vec<RuntimeEvent> {
        let mut state = self.state.lock().await;
        let native_thread_id = state.native_thread_id.clone();
        let native_turn_id = state.active_turn_id.clone();
        let mut events = terminalize_running_claude_mcp_items(
            &mut state,
            ClaudeMcpToolLifecycle::Failed,
            "Claude stream failed before the MCP call reached a terminal result",
            "error/mcp_terminal_reconciliation",
        );
        state.active_turn_id = None;
        state.active_text_item_id = None;
        state.active_reasoning_item_id = None;
        state.active_text_item_started = false;
        state.active_reasoning_item_started = false;
        state.emitted_final_text = false;
        state.tool_items.clear();
        events.push(RuntimeEvent::TurnFailed(RuntimeTurnFailed {
            native_thread_id,
            native_turn_id,
            message: value
                .get("error")
                .and_then(JsonValue::as_str)
                .unwrap_or("Claude CLI emitted an error")
                .to_owned(),
            code: Some("claude_stream_error".to_owned()),
            native: Some(native_event("error", value)),
        }));
        events
    }

    async fn emit(&self, event: RuntimeEvent) {
        self.record_turn_observation(&event).await;
        match self.event_ingress.offer(event).await {
            OrderedIngressOffer::Accepted => {}
            OrderedIngressOffer::CoalescedKeyLimit(_) => {
                tracing::debug!("Claude runtime progress coalescer reached its key limit")
            }
            OrderedIngressOffer::Closed(_) => {
                tracing::warn!("Claude runtime event ingress is closed")
            }
        }
    }

    async fn record_turn_observation(&self, event: &RuntimeEvent) {
        let mut state = self.state.lock().await;
        match event {
            RuntimeEvent::TurnStarted(started) => {
                state.observed_turn_id = Some(started.native_turn_id.clone());
                state.reconciliation_events.clear();
                state.last_turn_observation = None;
            }
            RuntimeEvent::ItemCompleted(completed)
                if state.observed_turn_id.as_deref() == Some(completed.native_turn_id.as_str()) =>
            {
                if let Some(existing) = state.reconciliation_events.iter_mut().find(|candidate| {
                    matches!(
                        candidate,
                        RuntimeEvent::ItemCompleted(item)
                            if item.native_item_id == completed.native_item_id
                    )
                }) {
                    *existing = event.clone();
                } else {
                    state.reconciliation_events.push(event.clone());
                }
            }
            RuntimeEvent::TurnCompleted(completed)
                if state.observed_turn_id.as_deref() == Some(completed.native_turn_id.as_str()) =>
            {
                let reconciliation_events = state.reconciliation_events.clone();
                state.last_turn_observation = Some(CLIAgentRuntimeTurnObservation {
                    status: CLIAgentRuntimeObservedTurnStatus::Completed,
                    message: None,
                    reconciliation_events,
                });
            }
            RuntimeEvent::TurnFailed(failed)
                if failed.native_turn_id.as_deref() == state.observed_turn_id.as_deref() =>
            {
                let reconciliation_events = state.reconciliation_events.clone();
                state.last_turn_observation = Some(CLIAgentRuntimeTurnObservation {
                    status: CLIAgentRuntimeObservedTurnStatus::Failed,
                    message: Some(failed.message.clone()),
                    reconciliation_events,
                });
            }
            RuntimeEvent::TurnInterrupted(interrupted)
                if state.observed_turn_id.as_deref()
                    == Some(interrupted.native_turn_id.as_str()) =>
            {
                let reconciliation_events = state.reconciliation_events.clone();
                state.last_turn_observation = Some(CLIAgentRuntimeTurnObservation {
                    status: CLIAgentRuntimeObservedTurnStatus::Interrupted,
                    message: Some(interrupted.reason.clone()),
                    reconciliation_events,
                });
            }
            _ => {}
        }
        self.state_changed.notify_waiters();
    }
}

fn claude_prompt_from_input(input: JsonValue) -> Result<JsonValue> {
    let items = input
        .as_array()
        .ok_or_else(|| anyhow!("Claude turn input must be an array"))?;
    let mut content = Vec::new();
    for item in items {
        match item.get("type").and_then(JsonValue::as_str) {
            Some("text") => {
                if let Some(text) = item.get("text").and_then(JsonValue::as_str) {
                    push_claude_text_block(&mut content, text);
                }
            }
            Some("localImage") | Some("local_image") => {
                if let Some(path) = item.get("path").and_then(JsonValue::as_str) {
                    content.push(claude_local_image_block(path)?);
                }
            }
            Some("image") => {
                if let Some(url) = item.get("url").and_then(JsonValue::as_str) {
                    push_claude_text_block(
                        &mut content,
                        format!(
                            "Attached image available at URL:\n{url}\nUse this URL if the task requires inspecting the image."
                        ),
                    );
                }
            }
            Some("fileReference") | Some("file_reference") => {
                if let Some(path) = item.get("path").and_then(JsonValue::as_str) {
                    push_claude_text_block(
                        &mut content,
                        format!(
                            "Attached file available at local path:\n{path}\nUse this path if the task requires inspecting the file."
                        ),
                    );
                } else if let Some(url) = item.get("url").and_then(JsonValue::as_str) {
                    push_claude_text_block(
                        &mut content,
                        format!(
                            "Attached file available at URL:\n{url}\nUse this URL if the task requires inspecting the file."
                        ),
                    );
                } else if let Some(reference) = item.get("reference").and_then(JsonValue::as_str) {
                    push_claude_text_block(
                        &mut content,
                        format!("Attached file reference:\n{reference}"),
                    );
                }
            }
            other => {
                push_claude_text_block(
                    &mut content,
                    format!("Unsupported CLI attachment input: {other:?}"),
                );
            }
        }
    }
    Ok(JsonValue::Array(content))
}

fn push_claude_text_block(content: &mut Vec<JsonValue>, text: impl Into<String>) {
    let text = text.into();
    if !text.trim().is_empty() {
        content.push(json!({ "type": "text", "text": text }));
    }
}

fn claude_local_image_block(path: &str) -> Result<JsonValue> {
    let media_type = claude_image_media_type(path)
        .ok_or_else(|| anyhow!("unsupported Claude image attachment type for `{path}`"))?;
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read Claude image attachment `{path}`"))?;
    let data = base64::engine::general_purpose::STANDARD.encode(bytes);
    Ok(json!({
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": media_type,
            "data": data,
        },
    }))
}

fn claude_image_media_type(path: &str) -> Option<&'static str> {
    let mime = mime_guess::from_path(Path::new(path)).first()?;
    match (mime.type_().as_str(), mime.subtype().as_str()) {
        ("image", "png") => Some("image/png"),
        ("image", "jpeg") => Some("image/jpeg"),
        ("image", "gif") => Some("image/gif"),
        ("image", "webp") => Some("image/webp"),
        _ => None,
    }
}

fn native_event(method: &str, raw: JsonValue) -> RuntimeNativeEvent {
    RuntimeNativeEvent {
        method: method.to_owned(),
        payload_redacted: Some(redact_native(raw.clone())),
        raw_redacted: Some(redact_native(raw)),
    }
}

fn redact_native(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => JsonValue::Object(
            map.into_iter()
                .map(|(key, value)| {
                    let lowered = key.to_ascii_lowercase();
                    if lowered.contains("token")
                        || lowered.contains("secret")
                        || lowered.contains("password")
                        || lowered.contains("authorization")
                    {
                        (key, JsonValue::String("<redacted>".to_owned()))
                    } else {
                        (key, redact_native(value))
                    }
                })
                .collect(),
        ),
        JsonValue::Array(items) => JsonValue::Array(items.into_iter().map(redact_native).collect()),
        other => other,
    }
}

fn item_started(
    state: &ClaudeStreamState,
    item_id: &str,
    item_kind: &str,
    title: Option<String>,
    metadata: Option<JsonValue>,
) -> RuntimeEvent {
    RuntimeEvent::ItemStarted(RuntimeItemStarted {
        native_thread_id: state.native_thread_id.clone(),
        native_turn_id: state.active_turn_id.clone().unwrap_or_default(),
        native_item_id: item_id.to_owned(),
        item_kind: item_kind.to_owned(),
        title,
        phase: RuntimeAgentMessagePhase::FinalAnswer,
        metadata,
        native_item_redacted: None,
        native: Some(native_event(
            "item/started",
            json!({ "provider": "claude" }),
        )),
    })
}

fn item_kind_for_claude_tool(tool_name: &str) -> &'static str {
    match tool_name {
        "Bash" => "commandExecution",
        "Edit" | "MultiEdit" | "Write" | "NotebookEdit" => "fileChange",
        _ => "dynamicToolCall",
    }
}

fn request_kind_for_claude_tool(tool_name: &str) -> &'static str {
    match tool_name {
        "Edit" | "MultiEdit" | "Write" | "NotebookEdit" => "file_change_approval",
        _ => "command_approval",
    }
}

fn metadata_for_claude_tool(
    tool_name: &str,
    input: &JsonValue,
    output: Option<&str>,
    success: Option<bool>,
) -> JsonValue {
    match tool_name {
        "Bash" => json!({
            "toolName": "Bash",
            "command": command_from_claude_tool(tool_name, input)
                .map(|command| vec![command])
                .unwrap_or_default(),
            "cwd": input.get("cwd").and_then(JsonValue::as_str),
            "stdout": output,
            "success": success,
        }),
        "Edit" | "MultiEdit" | "Write" | "NotebookEdit" => json!({
            "toolName": tool_name,
            "changedFiles": changed_files_from_claude_tool(input),
            "stdout": output,
            "success": success,
            "arguments": input,
        }),
        _ => json!({
            "toolName": tool_name,
            "tool": tool_name,
            "message": output,
            "success": success,
            "arguments": input,
        }),
    }
}

fn claude_mcp_stream_error(
    state: &ClaudeStreamState,
    message: &str,
    code: &str,
    raw: JsonValue,
) -> RuntimeEvent {
    RuntimeEvent::Error(RuntimeErrorEvent {
        native_thread_id: state.native_thread_id.clone(),
        native_turn_id: state.active_turn_id.clone(),
        message: message.to_owned(),
        code: Some(code.to_owned()),
        retryable: false,
        native: Some(native_event("claude_mcp/invalid", raw)),
    })
}

fn terminalize_running_claude_mcp_items(
    state: &mut ClaudeStreamState,
    lifecycle: ClaudeMcpToolLifecycle,
    message: &str,
    native_method: &str,
) -> Vec<RuntimeEvent> {
    let (status, success) = match lifecycle {
        ClaudeMcpToolLifecycle::Completed => ("completed", true),
        ClaudeMcpToolLifecycle::Failed => ("failed", false),
        ClaudeMcpToolLifecycle::Cancelled => ("cancelled", false),
        ClaudeMcpToolLifecycle::Running => ("failed", false),
    };
    let mut events = Vec::new();
    for item in state.mcp_items.values_mut() {
        if item.lifecycle != ClaudeMcpToolLifecycle::Running {
            continue;
        }
        item.lifecycle = lifecycle;
        let mut metadata = item.binding.metadata.clone();
        metadata.insert(
            "status".to_owned(),
            ToolMetadataValue::from_json(JsonValue::String(status.to_owned())),
        );
        metadata.insert(
            "success".to_owned(),
            ToolMetadataValue::from_json(JsonValue::Bool(success)),
        );
        metadata.insert(
            "message".to_owned(),
            ToolMetadataValue::from_json(JsonValue::String(message.to_owned())),
        );
        if !success {
            metadata.insert(
                "error".to_owned(),
                ToolMetadataValue::from_json(json!({"message": message})),
            );
        }
        events.push(RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
            native_thread_id: Some(item.binding.native_thread_id.clone()),
            native_turn_id: item.binding.native_turn_id.clone(),
            native_item_id: item.binding.native_item_id.clone(),
            item_kind: "mcpToolCall".to_owned(),
            text: Some(message.to_owned()),
            summary: Vec::new(),
            content: Vec::new(),
            phase: RuntimeAgentMessagePhase::FinalAnswer,
            metadata: Some(metadata.to_json()),
            native_item_redacted: None,
            native: Some(native_event(
                native_method,
                json!({"provider": "claude", "reconciled": true}),
            )),
        }));
    }
    events
}

fn command_from_claude_tool(tool_name: &str, input: &JsonValue) -> Option<String> {
    if tool_name == "Bash" {
        return input
            .get("command")
            .and_then(JsonValue::as_str)
            .map(str::to_owned);
    }
    input
        .get("description")
        .or_else(|| input.get("file_path"))
        .or_else(|| input.get("path"))
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
}

fn changed_files_from_claude_tool(input: &JsonValue) -> Vec<String> {
    ["file_path", "path", "notebook_path"]
        .into_iter()
        .filter_map(|key| {
            input
                .get(key)
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
        })
        .collect()
}

fn claude_tool_result_text(block: &JsonValue) -> String {
    match block.get("content") {
        Some(JsonValue::String(text)) => text.clone(),
        Some(JsonValue::Array(items)) => items
            .iter()
            .filter_map(|item| {
                item.get("text")
                    .and_then(JsonValue::as_str)
                    .or_else(|| item.as_str())
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Some(value) => value.to_string(),
        None => String::new(),
    }
}

fn claude_emitted_session_id(value: &JsonValue) -> Option<&str> {
    value
        .get("session_id")
        .and_then(JsonValue::as_str)
        .or_else(|| value.get("sessionId").and_then(JsonValue::as_str))
        .or_else(|| {
            value
                .get("response")
                .and_then(|response| response.get("response"))
                .and_then(|response| {
                    response
                        .get("session_id")
                        .or_else(|| response.get("sessionId"))
                })
                .and_then(JsonValue::as_str)
        })
}

fn claude_result_error_message(value: &JsonValue) -> String {
    value
        .get("errors")
        .and_then(JsonValue::as_array)
        .map(|errors| {
            errors
                .iter()
                .filter_map(JsonValue::as_str)
                .collect::<Vec<_>>()
                .join("; ")
        })
        .filter(|message| !message.is_empty())
        .or_else(|| {
            value
                .get("result")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "Claude CLI turn failed".to_owned())
}

fn claude_item_id(value: &JsonValue, label: &str, index: usize) -> String {
    value
        .get("uuid")
        .and_then(JsonValue::as_str)
        .map(|uuid| format!("claude_{label}_{uuid}_{index}"))
        .unwrap_or_else(|| format!("claude_{label}_{}", new_runtime_id()))
}

fn new_runtime_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{now:x}_{counter:x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli_runtime::claude_mcp::build_claude_mcp_session_launch_projection;
    use crate::cli_runtime::projector::{CLIRuntimeProjectorContext, project_cli_runtime_event};
    use crate::turn_mcp::projection::{
        McpProjectionLimits, McpSelectionReason, ResolvedMcpTurnProjection, ResolvedMcpTurnTool,
    };
    use pioneer_cli_agent_runtime::instructions::CLIRuntimeElevatedInstructions;
    use pioneer_cli_mcp_bridge::helper::run_hidden_helper_with_io;
    use pioneer_protocol::{AgentDurableEvent, ToolCallStatus, ToolMetadata, TurnItem};
    use sha2::{Digest, Sha256};
    use std::collections::HashSet;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::process::Stdio;
    use tokio::io::duplex;
    use tokio_util::sync::CancellationToken;

    struct NeverInvoke;

    #[test]
    fn claude_session_identity_pristine_launch_is_only_preverification_window() {
        let mut state = ClaudeStreamState::default();
        assert!(state.is_pristine_provider_launch());

        state.active_turn_id = Some("turn".to_owned());
        assert!(!state.is_pristine_provider_launch());
        state.active_turn_id = None;

        state.observed_turn_id = Some("turn".to_owned());
        assert!(!state.is_pristine_provider_launch());
        state.observed_turn_id = None;

        state.provider_session_verified = true;
        assert!(!state.is_pristine_provider_launch());
        state.provider_session_verified = false;

        state.provider_session_invalid = true;
        assert!(!state.is_pristine_provider_launch());
    }

    #[async_trait]
    impl TurnMcpInvoker for NeverInvoke {
        async fn invoke(
            &self,
            _invocation: crate::turn_mcp::invoker::TurnMcpInvocation,
            _cancellation: CancellationToken,
        ) -> std::result::Result<
            crate::turn_mcp::result::CanonicalMcpToolResult,
            crate::turn_mcp::invoker::TurnMcpInvocationError,
        > {
            Err(crate::turn_mcp::invoker::TurnMcpInvocationError::new(
                crate::turn_mcp::invoker::TurnMcpInvocationErrorCode::TurnNotActive,
                "test does not invoke tools",
            ))
        }
    }

    struct FixtureClaudePermissionAuthorizer {
        calls: AtomicU64,
    }

    #[async_trait]
    impl ClaudeMcpPermissionAuthorizer for FixtureClaudePermissionAuthorizer {
        async fn authorize_permission(
            &self,
            request: &ClaudeNativeMcpPermissionRequest,
        ) -> Result<bool> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(request.canonical_callable_name == "mcp_server_tool_a"
                && request.native_item_id == "call-exact")
        }
    }

    #[tokio::test]
    async fn claude_mcp_restart_helper_list_and_bootstrap_barrier_is_exact() {
        #[cfg(unix)]
        let temporary = tempfile::tempdir_in("/tmp").expect("temporary root");
        #[cfg(windows)]
        let temporary = tempfile::tempdir().expect("temporary root");
        let supervisor = CliMcpBridgeSupervisor::new(temporary.path().join("bridge"));
        let process_instance =
            crate::cli_runtime::session_instance::CliSessionGenerationAllocator::default()
                .allocate(
                    crate::cli_runtime::manager::CLIAgentRuntimeSessionKey::new(
                        "workspace",
                        "claude",
                        "restart-list-barrier",
                    )
                    .expect("session key"),
                )
                .expect("process instance");
        let projection = crate::cli_runtime::mcp::facade::CliMcpFacadeProjection::new(
            vec![
                crate::cli_runtime::mcp::facade::CliMcpFacadeTool::new(
                    "mcp__server__tool",
                    Some("fixture".to_owned()),
                    json!({"type": "object"}),
                    json!({}),
                )
                .expect("tool"),
            ],
            crate::cli_runtime::mcp::facade::CliMcpFacadeProjectionLimits::default(),
        )
        .expect("projection");
        let scope = CliMcpGrantScope::new(
            process_instance.clone(),
            CliMcpManifestHash::new("a".repeat(64)).expect("manifest"),
        );
        let launch = supervisor
            .prepare(scope, claude_mcp_bootstrap_expiry(2_000).expect("expiry"))
            .await
            .expect("bridge launch");
        let bootstrap_path = launch.bootstrap_path().to_path_buf();
        let reservation = supervisor
            .coordinator()
            .stage_projection(launch.grant_ref(), projection.fingerprint().clone())
            .await
            .expect("projection reservation");
        supervisor
            .associate_provider_process(&process_instance, std::process::id(), None)
            .await
            .expect("provider identity");
        let required = Arc::new(ClaudeRequiredMcpBridge {
            supervisor: supervisor.clone(),
            process_instance: process_instance.clone(),
            launch,
            projection_fingerprint: projection.fingerprint().clone(),
            projection,
            projection_generation: reservation.generation,
            canonical_manifest_hash: "a".repeat(64),
            provider_contract_fingerprint: "b".repeat(64),
            isolation_contract_fingerprint: "c".repeat(64),
            invoker: Arc::new(NeverInvoke),
            native_items: Arc::new(ClaudeNativeMcpCorrelationLedger::default()),
            state: Mutex::new(ClaudeRequiredMcpBridgeState::Pending),
        });
        let ready = {
            let required = required.clone();
            tokio::spawn(async move { required.ensure_ready(Duration::from_secs(2)).await })
        };
        let (mut provider_writer, helper_stdin) = duplex(64 * 1024);
        let (helper_stdout, provider_reader) = duplex(64 * 1024);
        let helper_bootstrap = bootstrap_path.clone();
        let helper = tokio::spawn(async move {
            run_hidden_helper_with_io(&helper_bootstrap, helper_stdin, helper_stdout).await
        });
        let mut provider_reader = BufReader::new(provider_reader);
        provider_writer
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"clientInfo\":{\"name\":\"claude-test\",\"version\":\"1\"}}}\n{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n",
            )
            .await
            .expect("send handshake/list");
        let mut line = String::new();
        provider_reader
            .read_line(&mut line)
            .await
            .expect("initialize response");
        line.clear();
        provider_reader
            .read_line(&mut line)
            .await
            .expect("list response");
        let list: JsonValue = serde_json::from_str(&line).expect("list JSON");
        assert_eq!(list["result"]["tools"][0]["name"], "mcp__server__tool");
        ready
            .await
            .expect("readiness task")
            .expect("exact helper/list barrier");
        assert!(
            !bootstrap_path.exists(),
            "one-use bootstrap must be gone before turn preparation"
        );
        required
            .prepare_turn("pioneer-turn")
            .await
            .expect("turn reservation after readiness");
        required
            .activate_turn("pioneer-turn", "provider-session", "provider-turn")
            .await
            .expect("turn activation");
        required
            .terminal_turn("pioneer-turn")
            .await
            .expect("turn terminalization");
        required.fail_closed().await;
        drop(provider_writer);
        let _ = tokio_timeout(Duration::from_secs(1), helper).await;
        assert!(!supervisor.revoke_session(&process_instance).await);
    }

    #[tokio::test]
    async fn claude_mcp_permission_exact_active_binding_allows_and_stale_variants_deny() {
        #[cfg(unix)]
        let temporary = tempfile::tempdir_in("/tmp").expect("temporary root");
        #[cfg(windows)]
        let temporary = tempfile::tempdir().expect("temporary root");
        let supervisor = CliMcpBridgeSupervisor::new(temporary.path().join("bridge"));
        let process_instance =
            crate::cli_runtime::session_instance::CliSessionGenerationAllocator::default()
                .allocate(
                    crate::cli_runtime::manager::CLIAgentRuntimeSessionKey::new(
                        "workspace",
                        "claude",
                        "permission-fallback",
                    )
                    .expect("session key"),
                )
                .expect("process instance");
        let projection = crate::cli_runtime::mcp::facade::CliMcpFacadeProjection::new(
            vec![
                crate::cli_runtime::mcp::facade::CliMcpFacadeTool::new(
                    "mcp__server__tool",
                    Some("destructive fixture".to_owned()),
                    json!({"type": "object"}),
                    json!({"destructiveHint": true}),
                )
                .expect("tool"),
            ],
            crate::cli_runtime::mcp::facade::CliMcpFacadeProjectionLimits::default(),
        )
        .expect("projection");
        let scope = CliMcpGrantScope::new(
            process_instance.clone(),
            CliMcpManifestHash::new("a".repeat(64)).expect("manifest"),
        );
        let launch = supervisor
            .prepare(scope, claude_mcp_bootstrap_expiry(2_000).expect("expiry"))
            .await
            .expect("bridge launch");
        let bootstrap_path = launch.bootstrap_path().to_path_buf();
        let reservation = supervisor
            .coordinator()
            .stage_projection(launch.grant_ref(), projection.fingerprint().clone())
            .await
            .expect("projection reservation");
        supervisor
            .associate_provider_process(&process_instance, std::process::id(), None)
            .await
            .expect("provider identity");
        let native_items = Arc::new(ClaudeNativeMcpCorrelationLedger::default());
        let required = Arc::new(ClaudeRequiredMcpBridge {
            supervisor: supervisor.clone(),
            process_instance: process_instance.clone(),
            launch,
            projection_fingerprint: projection.fingerprint().clone(),
            projection,
            projection_generation: reservation.generation,
            canonical_manifest_hash: "a".repeat(64),
            provider_contract_fingerprint: "b".repeat(64),
            isolation_contract_fingerprint: "c".repeat(64),
            invoker: Arc::new(NeverInvoke),
            native_items: native_items.clone(),
            state: Mutex::new(ClaudeRequiredMcpBridgeState::Pending),
        });
        let ready = {
            let required = required.clone();
            tokio::spawn(async move { required.ensure_ready(Duration::from_secs(2)).await })
        };
        let (mut provider_writer, helper_stdin) = duplex(64 * 1024);
        let (helper_stdout, provider_reader) = duplex(64 * 1024);
        let helper = tokio::spawn(async move {
            run_hidden_helper_with_io(&bootstrap_path, helper_stdin, helper_stdout).await
        });
        let mut provider_reader = BufReader::new(provider_reader);
        provider_writer
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"clientInfo\":{\"name\":\"claude-test\",\"version\":\"1\"}}}\n{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n",
            )
            .await
            .expect("send handshake/list");
        let mut line = String::new();
        provider_reader
            .read_line(&mut line)
            .await
            .expect("initialize response");
        line.clear();
        provider_reader
            .read_line(&mut line)
            .await
            .expect("list response");
        ready
            .await
            .expect("readiness task")
            .expect("exact helper/list barrier");
        let provider_session_id =
            uuid::Uuid::parse_str("01900000-0000-7000-8000-000000000041").expect("UUID");
        required
            .prepare_turn("pioneer-turn")
            .await
            .expect("turn reservation");
        required
            .activate_turn(
                "pioneer-turn",
                provider_session_id.to_string().as_str(),
                "provider-turn",
            )
            .await
            .expect("turn activation");
        let arguments = json!({"command": "rm -rf ./generated"});
        native_items
            .register(
                &crate::cli_runtime::claude_mcp::ClaudeNativeMcpItemBinding {
                    native_thread_id: provider_session_id.to_string(),
                    native_turn_id: "provider-turn".to_owned(),
                    native_item_id: "native-item".to_owned(),
                    canonical_callable_name: "mcp__server__tool".to_owned(),
                    arguments_fingerprint:
                        crate::cli_runtime::codex_mcp::canonical_value_fingerprint(&arguments)
                            .expect("arguments fingerprint"),
                    metadata: ToolMetadata::empty(),
                },
            )
            .expect("register exact native item");
        let exact = ClaudeNativeMcpPermissionRequest {
            runtime_id: "claude".to_owned(),
            session_generation: process_instance.generation(),
            native_thread_id: provider_session_id.to_string(),
            native_turn_id: "provider-turn".to_owned(),
            native_item_id: "native-item".to_owned(),
            qualified_tool_name: "mcp__pioneer__mcp__server__tool".to_owned(),
            canonical_callable_name: "mcp__server__tool".to_owned(),
            arguments: arguments.clone(),
            arguments_fingerprint: crate::cli_runtime::codex_mcp::canonical_value_fingerprint(
                &arguments,
            )
            .expect("arguments fingerprint"),
            manifest_hash: "a".repeat(64),
            provider_contract_fingerprint: "b".repeat(64),
        };
        assert!(required.authorize_native_permission(&exact).await.unwrap());
        let mut stale = exact.clone();
        stale.session_generation += 1;
        assert!(!required.authorize_native_permission(&stale).await.unwrap());
        let mut cross_session = exact.clone();
        cross_session.native_thread_id = uuid::Uuid::new_v4().to_string();
        assert!(
            !required
                .authorize_native_permission(&cross_session)
                .await
                .unwrap()
        );
        let mut wrong_manifest = exact.clone();
        wrong_manifest.manifest_hash = "d".repeat(64);
        assert!(
            !required
                .authorize_native_permission(&wrong_manifest)
                .await
                .unwrap()
        );
        required
            .terminal_turn("pioneer-turn")
            .await
            .expect("terminalize turn");
        assert!(!required.authorize_native_permission(&exact).await.unwrap());
        required.fail_closed().await;
        drop(provider_writer);
        let _ = tokio_timeout(Duration::from_secs(1), helper).await;
    }

    async fn fake_claude_stream_client(
        log_path: &Path,
    ) -> (Arc<ClaudeStreamClient>, tokio::process::Child) {
        let (client, child, _event_rx) =
            fake_claude_stream_client_with_mcp(log_path, uuid::Uuid::new_v4(), None).await;
        (client, child)
    }

    async fn fake_claude_stream_client_with_mcp(
        log_path: &Path,
        expected_provider_session_id: uuid::Uuid,
        mcp: Option<ClaudeMcpEventContext>,
    ) -> (
        Arc<ClaudeStreamClient>,
        tokio::process::Child,
        mpsc::Receiver<RuntimeEvent>,
    ) {
        let script = r#"
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$LOG"
  case "$line" in
    *'"type":"control_request"'*)
      request_id=$(printf '%s\n' "$line" | sed -n 's/.*"request_id":"\([^"]*\)".*/\1/p')
      if [ -n "$request_id" ]; then
        printf '{"type":"control_response","response":{"subtype":"success","request_id":"%s","response":{}}}\n' "$request_id"
      fi
      ;;
  esac
done
"#;
        let mut child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(script)
            .env("LOG", log_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn fake Claude stream process");
        let stdin = child.stdin.take().expect("fake Claude stdin");
        let stdout = child.stdout.take().expect("fake Claude stdout");
        let (event_tx, event_rx) = mpsc::channel(64);
        let client = Arc::new(ClaudeStreamClient::new_for_test_with_mcp(
            stdin,
            event_tx,
            expected_provider_session_id,
            mcp,
        ));
        client.spawn_reader(stdout);
        (client, child, event_rx)
    }

    fn claude_mcp_event_projection(turn_id: &str) -> ClaudeMcpSessionLaunchProjection {
        let mut projection = ResolvedMcpTurnProjection::empty("workspace", turn_id);
        projection.tools = ["tool_a", "tool_b"]
            .into_iter()
            .enumerate()
            .map(|(index, raw_tool_name)| ResolvedMcpTurnTool {
                canonical_callable_name: String::new(),
                workspace_id: "workspace".to_owned(),
                server_installation_id: format!("installation-{index}"),
                server_name: "server".to_owned(),
                raw_tool_name: raw_tool_name.to_owned(),
                description: Some(format!("fixture {raw_tool_name}")),
                input_schema: json!({
                    "type": "object",
                    "properties": {"value": {"type": "integer"}},
                    "required": ["value"]
                }),
                annotations: None,
                timeout_ms: 20_000,
                catalog_version: "catalog-v1".to_owned(),
                installation_fingerprint: format!("installation-fingerprint-{index}"),
                schema_fingerprint: String::new(),
                runtime_generation: 11,
                selection_reason: McpSelectionReason::ExplicitTool,
                capability_id: Some(format!("capability-{index}")),
            })
            .collect();
        projection
            .finalize_identity(McpProjectionLimits::default())
            .expect("finalized Claude event projection");
        build_claude_mcp_session_launch_projection(projection, "a".repeat(64))
            .expect("Claude event launch projection")
    }

    fn claude_mcp_event_context(
        projection: ClaudeMcpSessionLaunchProjection,
        session_generation: u64,
    ) -> ClaudeMcpEventContext {
        ClaudeMcpEventContext {
            runtime_id: "claude".to_owned(),
            session_generation,
            manifest_hash: projection.preflight.canonical_manifest_hash.clone(),
            provider_contract_fingerprint: projection
                .preflight
                .provider_contract_fingerprint
                .clone(),
            projection,
            native_items: Arc::new(ClaudeNativeMcpCorrelationLedger::default()),
            permission_authorizer: None,
        }
    }

    async fn bind_claude_mcp_test_turn(
        client: &ClaudeStreamClient,
        provider_session_id: uuid::Uuid,
        native_turn_id: &str,
    ) {
        let mut state = client.state.lock().await;
        state.provider_session_verified = true;
        state.native_thread_id = Some(provider_session_id.to_string());
        state.active_turn_id = Some(native_turn_id.to_owned());
        state.observed_turn_id = Some(native_turn_id.to_owned());
    }

    #[tokio::test]
    async fn claude_permission_fallback_responds_once_without_generic_prompt_or_invoker() {
        let temp = tempfile::tempdir().expect("tempdir");
        let provider_session_id =
            uuid::Uuid::parse_str("01900000-0000-7000-8000-000000000031").expect("UUID");
        let mut context =
            claude_mcp_event_context(claude_mcp_event_projection("provider-turn"), 12);
        let authorizer = Arc::new(FixtureClaudePermissionAuthorizer {
            calls: AtomicU64::new(0),
        });
        context.permission_authorizer = Some(authorizer.clone());
        let log_path = temp.path().join("mcp-permission.log");
        let (client, mut child, mut event_rx) = fake_claude_stream_client_with_mcp(
            log_path.as_path(),
            provider_session_id,
            Some(context),
        )
        .await;
        bind_claude_mcp_test_turn(&client, provider_session_id, "provider-turn").await;
        let fixture: JsonValue = serde_json::from_str(include_str!(
            "../../tests/fixtures/claude_mcp_permission_callbacks.json"
        ))
        .expect("Claude permission callback fixture");

        client
            .handle_control_request(fixture["exactDestructive"].clone())
            .await;
        client
            .handle_control_request(fixture["exactDestructive"].clone())
            .await;
        client
            .handle_control_request(fixture["unknown"].clone())
            .await;
        client
            .handle_control_request(fixture["wildcard"].clone())
            .await;

        let responses = wait_logged_json_lines(log_path.as_path(), 3).await;
        assert_eq!(responses.len(), 3, "callback replay must not respond twice");
        assert_eq!(
            responses[0]["response"]["request_id"],
            json!("permission-exact")
        );
        assert_eq!(
            responses[0]["response"]["response"]["behavior"],
            json!("allow")
        );
        assert_eq!(
            responses[0]["response"]["response"]["updatedInput"]["command"],
            json!("rm -rf ./generated"),
            "provider preallow preserves input; Gateway intent remains authoritative later"
        );
        assert_eq!(
            responses[1]["response"]["response"]["behavior"],
            json!("deny")
        );
        assert_eq!(
            responses[2]["response"]["response"]["behavior"],
            json!("deny")
        );
        assert_eq!(
            authorizer.calls.load(Ordering::Relaxed),
            1,
            "unknown and malformed tools stop before any authorization contour"
        );
        assert!(
            event_rx.try_recv().is_err(),
            "synthetic permission fallback must not open a generic approval event"
        );

        child.kill().await.expect("disconnect fake Claude provider");
        client
            .handle_control_request(fixture["malformed"].clone())
            .await;
        assert!(
            client
                .state
                .lock()
                .await
                .completed_mcp_permission_requests
                .contains("permission-malformed"),
            "a disconnected callback is consumed exactly once rather than retried elsewhere"
        );
    }

    #[tokio::test]
    async fn claude_mcp_timeline_parallel_success_error_and_replay_is_idempotent() {
        let temp = tempfile::tempdir().expect("tempdir");
        let provider_session_id =
            uuid::Uuid::parse_str("01900000-0000-7000-8000-000000000031").expect("UUID");
        let projection = claude_mcp_event_projection("provider-turn");
        let context = claude_mcp_event_context(projection.clone(), 7);
        let (client, mut child, _event_rx) = fake_claude_stream_client_with_mcp(
            &temp.path().join("mcp-timeline.log"),
            provider_session_id,
            Some(context),
        )
        .await;
        bind_claude_mcp_test_turn(&client, provider_session_id, "provider-turn").await;

        let fixture: JsonValue = serde_json::from_str(include_str!(
            "../../../cli-agent-runtime/tests/fixtures/claude_mcp/lifecycle.json"
        ))
        .expect("Claude MCP lifecycle fixture");
        let messages = fixture["messages"].as_array().expect("messages");
        let started = client.map_message(messages[0].clone()).await;
        let completed = client.map_message(messages[1].clone()).await;
        let replayed_start = client.map_message(messages[2].clone()).await;
        let replayed_result = client.map_message(messages[3].clone()).await;

        assert_eq!(started.len(), 2, "parallel calls have two starts");
        assert_eq!(completed.len(), 2, "parallel calls have two terminals");
        assert!(replayed_start.is_empty(), "tool_use replay is idempotent");
        assert!(
            replayed_result.is_empty(),
            "tool_result replay is idempotent"
        );
        let started_ids = started
            .iter()
            .filter_map(|event| match event {
                RuntimeEvent::ItemStarted(item) => Some(item.native_item_id.as_str()),
                _ => None,
            })
            .collect::<HashSet<_>>();
        assert_eq!(started_ids, HashSet::from(["call-a", "call-b"]));

        let RuntimeEvent::ItemStarted(first_started) = &started[0] else {
            panic!("expected first MCP item start");
        };
        let metadata = first_started.metadata.as_ref().expect("frozen metadata");
        assert_eq!(metadata["serverInstallationId"], json!("installation-0"));
        assert_eq!(metadata["serverName"], json!("server"));
        assert_eq!(metadata["rawToolName"], json!("tool_a"));
        assert_eq!(
            metadata["canonicalCallableName"],
            json!("mcp_server_tool_a")
        );
        assert_eq!(metadata["sessionGeneration"], json!(7));

        let projector_context = CLIRuntimeProjectorContext {
            workspace_id: "workspace".to_owned(),
            thread_id: "thread".to_owned(),
            turn_id: "turn".to_owned(),
            recovery: None,
        };
        let projected_start = project_cli_runtime_event(&projector_context, &started[0]);
        let projected_terminal = project_cli_runtime_event(&projector_context, &completed[0]);
        let AgentDurableEvent::ItemStarted { notification } = &projected_start.durable[0] else {
            panic!("expected projected MCP start");
        };
        let TurnItem::DynamicToolCall {
            id,
            tool_name,
            status,
            ..
        } = &notification.item
        else {
            panic!("expected one canonical dynamic tool item");
        };
        assert_eq!(id, "call-a");
        assert_eq!(tool_name, "mcp_server_tool_a");
        assert_eq!(*status, ToolCallStatus::InProgress);
        let AgentDurableEvent::ItemCompleted { notification } = &projected_terminal.durable[0]
        else {
            panic!("expected projected MCP terminal");
        };
        let TurnItem::DynamicToolCall {
            id,
            status,
            success,
            ..
        } = &notification.item
        else {
            panic!("expected terminal canonical dynamic tool item");
        };
        assert_eq!(id, "call-a");
        assert_eq!(*status, ToolCallStatus::Completed);
        assert_eq!(*success, Some(true));

        let error_terminal = completed
            .iter()
            .find_map(|event| match event {
                RuntimeEvent::ItemCompleted(item) if item.native_item_id == "call-b" => Some(item),
                _ => None,
            })
            .expect("failed parallel item");
        assert_eq!(
            error_terminal.metadata.as_ref().unwrap()["status"],
            json!("failed")
        );

        assert!(
            projection
                .bind_native_tool_use(
                    "claude",
                    0,
                    provider_session_id.to_string().as_str(),
                    "provider-turn",
                    "old-generation-call",
                    "mcp__pioneer__mcp_server_tool_a",
                    &json!({"value": 1}),
                )
                .is_err(),
            "zero/unknown process generation must fail closed"
        );

        let ledger = ClaudeNativeMcpCorrelationLedger::default();
        let first_parallel = projection
            .bind_native_tool_use(
                "claude",
                7,
                provider_session_id.to_string().as_str(),
                "provider-turn",
                "same-signature-a",
                "mcp__pioneer__mcp_server_tool_a",
                &json!({"value": 8}),
            )
            .expect("first parallel binding");
        let second_parallel = projection
            .bind_native_tool_use(
                "claude",
                7,
                provider_session_id.to_string().as_str(),
                "provider-turn",
                "same-signature-b",
                "mcp__pioneer__mcp_server_tool_a",
                &json!({"value": 8}),
            )
            .expect("second parallel binding");
        ledger.register(&first_parallel).expect("register first");
        ledger.register(&second_parallel).expect("register second");
        let first_claim = ledger
            .claim(
                first_parallel.canonical_callable_name.as_str(),
                first_parallel.arguments_fingerprint.as_str(),
                "facade-a",
            )
            .await
            .expect("claim first parallel item");
        let second_claim = ledger
            .claim(
                second_parallel.canonical_callable_name.as_str(),
                second_parallel.arguments_fingerprint.as_str(),
                "facade-b",
            )
            .await
            .expect("claim second parallel item");
        assert_eq!(first_claim.native_item_id, "same-signature-a");
        assert_eq!(second_claim.native_item_id, "same-signature-b");
        let _ = child.kill().await;
    }

    #[tokio::test]
    async fn cli_mcp_reconciliation_terminalizes_once_and_rejects_late_or_wrong_session_events() {
        let temp = tempfile::tempdir().expect("tempdir");
        let provider_session_id =
            uuid::Uuid::parse_str("01900000-0000-7000-8000-000000000031").expect("UUID");
        let context = claude_mcp_event_context(claude_mcp_event_projection("provider-turn"), 9);
        let (client, mut child, mut event_rx) = fake_claude_stream_client_with_mcp(
            &temp.path().join("mcp-reconciliation.log"),
            provider_session_id,
            Some(context),
        )
        .await;
        bind_claude_mcp_test_turn(&client, provider_session_id, "provider-turn").await;
        let tool_use = json!({
            "type": "assistant",
            "session_id": provider_session_id,
            "message": {"content": [{
                "type": "tool_use",
                "id": "call-terminal",
                "name": "mcp__pioneer__mcp_server_tool_a",
                "input": {"value": 4}
            }]}
        });
        assert_eq!(client.map_message(tool_use).await.len(), 1);
        let terminal = client
            .map_message(json!({
                "type": "result",
                "session_id": provider_session_id,
                "subtype": "interrupted",
                "is_error": true,
                "result": "cancelled"
            }))
            .await;
        assert_eq!(
            terminal
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::ItemCompleted(item) if item.native_item_id == "call-terminal"))
                .count(),
            1,
            "turn cancellation owns exactly one MCP terminal"
        );
        let late = client
            .map_message(json!({
                "type": "user",
                "session_id": provider_session_id,
                "message": {"content": [{
                    "type": "tool_result",
                    "tool_use_id": "call-terminal",
                    "content": "late-success"
                }]}
            }))
            .await;
        assert!(late.is_empty(), "late result cannot overwrite cancellation");

        client
            .handle_incoming(json!({
                "type": "assistant",
                "session_id": uuid::Uuid::new_v4(),
                "message": {"content": []}
            }))
            .await;
        let mismatch = tokio_timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("session mismatch event timeout")
            .expect("session mismatch event");
        assert!(matches!(
            mismatch,
            RuntimeEvent::Error(RuntimeErrorEvent { code: Some(code), .. })
                if code == "claude_session_identity_invalid"
        ));
        assert!(client.state.lock().await.provider_session_invalid);
        let _ = child.kill().await;

        let eof_context =
            claude_mcp_event_context(claude_mcp_event_projection("provider-eof-turn"), 10);
        let (eof_client, mut eof_child, mut eof_events) = fake_claude_stream_client_with_mcp(
            &temp.path().join("mcp-eof.log"),
            provider_session_id,
            Some(eof_context),
        )
        .await;
        bind_claude_mcp_test_turn(&eof_client, provider_session_id, "provider-eof-turn").await;
        assert_eq!(
            eof_client
                .map_message(json!({
                    "type": "assistant",
                    "session_id": provider_session_id,
                    "message": {"content": [{
                        "type": "tool_use",
                        "id": "call-eof",
                        "name": "mcp__pioneer__mcp_server_tool_a",
                        "input": {"value": 5}
                    }]}
                }))
                .await
                .len(),
            1
        );
        eof_child.kill().await.expect("kill fake provider for EOF");
        let mut eof_terminal_count = 0;
        let mut eof_turn_failed = false;
        for _ in 0..2 {
            let event = tokio_timeout(Duration::from_secs(1), eof_events.recv())
                .await
                .expect("EOF reconciliation event timeout")
                .expect("EOF reconciliation event");
            match event {
                RuntimeEvent::ItemCompleted(item) if item.native_item_id == "call-eof" => {
                    eof_terminal_count += 1;
                    assert_eq!(item.metadata.unwrap()["status"], json!("failed"));
                }
                RuntimeEvent::TurnFailed(_) => eof_turn_failed = true,
                other => panic!("unexpected EOF reconciliation event: {other:?}"),
            }
        }
        assert_eq!(eof_terminal_count, 1);
        assert!(eof_turn_failed);
        assert!(
            eof_client
                .map_message(json!({
                    "type": "user",
                    "session_id": provider_session_id,
                    "message": {"content": [{
                        "type": "tool_result",
                        "tool_use_id": "call-eof",
                        "content": "too-late"
                    }]}
                }))
                .await
                .is_empty(),
            "late result after EOF cannot create a second terminal"
        );
    }

    async fn wait_logged_json_lines(log_path: &Path, expected_min: usize) -> Vec<JsonValue> {
        for _ in 0..50 {
            let lines = std::fs::read_to_string(log_path)
                .unwrap_or_default()
                .lines()
                .map(|line| serde_json::from_str(line).expect("logged line should be JSON"))
                .collect::<Vec<_>>();
            if lines.len() >= expected_min {
                return lines;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        std::fs::read_to_string(log_path)
            .expect("read fake Claude log")
            .lines()
            .map(|line| serde_json::from_str(line).expect("logged line should be JSON"))
            .collect()
    }

    #[tokio::test]
    async fn claude_session_identity_blocks_missing_or_mismatched_stream_identity() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (client, mut child) = fake_claude_stream_client(&temp.path().join("exact.log")).await;
        let expected = client.expected_provider_session_id.to_string();
        client
            .verify_provider_session_message(&json!({
                "type": "system",
                "subtype": "init",
                "session_id": expected,
            }))
            .await
            .expect("exact emitted identity");
        assert!(client.state.lock().await.provider_session_verified);
        assert!(
            client
                .verify_provider_session_message(&json!({
                    "type": "assistant",
                    "session_id": uuid::Uuid::new_v4().to_string(),
                    "message": {"content": []}
                }))
                .await
                .is_err(),
            "mismatched assistant identity must fail before projection"
        );
        assert!(client.state.lock().await.provider_session_invalid);
        let _ = child.kill().await;

        let (missing, mut child) =
            fake_claude_stream_client(&temp.path().join("missing.log")).await;
        assert!(
            missing
                .verify_provider_session_message(&json!({
                    "type": "assistant",
                    "message": {"content": []}
                }))
                .await
                .is_err(),
            "missing identity must fail before model event projection"
        );
        assert!(missing.state.lock().await.provider_session_invalid);
        let _ = child.kill().await;
    }

    fn claude_instance(home_path: String) -> EffectiveGatewayCliAgentRuntimeInstanceConfig {
        EffectiveGatewayCliAgentRuntimeInstanceConfig {
            id: "claude".to_owned(),
            kind: GatewayCliAgentRuntimeKindConfig::Claude,
            display_name: "Claude CLI".to_owned(),
            enabled: true,
            binary_path: "claude".to_owned(),
            home_path,
            shadow_home_path: None,
            custom_models: Vec::new(),
            app_server_args: Vec::new(),
            startup_probe_timeout_ms: 1_000,
            request_timeout_ms: 1_000,
            idle_session_ttl_secs: 60,
            event_channel_capacity: 16,
            stderr_ring_lines: 16,
            debug_native_events: false,
        }
    }

    fn arg_value_after(args: &[String], flag: &str) -> Option<String> {
        args.windows(2)
            .find(|pair| pair[0] == flag)
            .map(|pair| pair[1].clone())
    }

    fn has_arg(args: &[String], flag: &str) -> bool {
        args.iter().any(|arg| arg == flag)
    }

    fn elevated_instructions(text: &str) -> CLIRuntimeElevatedInstructions {
        let fingerprint = Sha256::digest(text.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        CLIRuntimeElevatedInstructions::try_new(text, fingerprint)
            .expect("valid elevated instructions")
    }

    #[test]
    #[cfg(unix)]
    fn claude_mcp_launch_four_cell_matrix_preserves_strict_permission_boundary() {
        use pioneer_cli_agent_runtime::claude::{
            ClaudeManagedMcpConfigInput, materialize_claude_managed_mcp_config,
            serialize_claude_managed_mcp_config,
        };

        let temp = tempfile::tempdir().expect("tempdir");
        let mut instance = claude_instance(
            temp.path()
                .join("claude-home")
                .to_string_lossy()
                .into_owned(),
        );
        let malicious_config = temp.path().join("user-config");
        std::fs::create_dir_all(malicious_config.as_path()).expect("user config");
        std::fs::write(
            malicious_config.join(".mcp.json"),
            r#"{"mcpServers":{"malicious_sentinel":{"command":"/sentinel"}}}"#,
        )
        .expect("malicious sentinel");
        instance.shadow_home_path = Some(malicious_config.to_string_lossy().into_owned());
        let helper = std::env::current_exe().expect("current executable");
        let bootstrap = temp.path().join("bootstrap.json");
        std::fs::write(bootstrap.as_path(), b"{}").expect("bootstrap");
        std::fs::set_permissions(bootstrap.as_path(), std::fs::Permissions::from_mode(0o600))
            .expect("bootstrap permissions");
        let managed_root = temp.path().join("managed");
        let mut generation = 1;

        for enable_user_skills in [false, true] {
            for mcp_enabled in [false, true] {
                let artifact = serialize_claude_managed_mcp_config(if mcp_enabled {
                    ClaudeManagedMcpConfigInput::pioneer(helper.clone(), bootstrap.clone())
                } else {
                    ClaudeManagedMcpConfigInput::empty()
                })
                .expect("managed artifact");
                let descriptor = materialize_claude_managed_mcp_config(
                    managed_root.as_path(),
                    ClaudeManagedMcpConfigIdentity::new(
                        "workspace",
                        "claude",
                        format!("thread-{generation}"),
                        "gateway-boot",
                        generation,
                    )
                    .expect("identity"),
                    &artifact,
                )
                .expect("managed config");
                generation += 1;
                let allowed_tool_names = if mcp_enabled {
                    vec!["mcp__pioneer__fixture".to_owned()]
                } else {
                    Vec::new()
                };
                let process = claude_process_config_from_instance_with_managed_mcp(
                    &instance,
                    &CLIAgentRuntimeSessionStartOptions {
                        approval_policy: Some("default".to_owned()),
                        enable_user_skills,
                        ..Default::default()
                    },
                    &descriptor,
                    allowed_tool_names.as_slice(),
                    &CliProviderContinuation::ClaudeNew {
                        provider_session_id: uuid::Uuid::new_v4(),
                    },
                )
                .expect("launch config");

                assert!(has_arg(&process.args, "--strict-mcp-config"));
                assert_eq!(
                    arg_value_after(&process.args, "--mcp-config").as_deref(),
                    descriptor.config_path.to_str()
                );
                assert_eq!(
                    arg_value_after(&process.args, "--permission-prompt-tool").as_deref(),
                    Some("stdio")
                );
                assert_eq!(
                    has_arg(&process.args, "--safe-mode"),
                    !enable_user_skills && !mcp_enabled
                );
                assert_eq!(
                    has_arg(&process.args, "--setting-sources=user"),
                    enable_user_skills
                );
                assert_eq!(has_arg(&process.args, "--allowedTools"), mcp_enabled);
                if mcp_enabled {
                    let flag_index = process
                        .args
                        .iter()
                        .position(|arg| arg == "--allowedTools")
                        .expect("exact allowed-tools flag");
                    assert_eq!(
                        process.args.get(flag_index + 1),
                        Some(&"mcp__pioneer__fixture".to_owned())
                    );
                }
                assert!(
                    !std::fs::read_to_string(descriptor.config_path.as_path())
                        .expect("config contents")
                        .contains("malicious_sentinel")
                );
                cleanup_claude_managed_mcp_config(&descriptor).expect("cleanup");
            }
        }
    }

    #[test]
    fn claude_launch_matrix_always_uses_fresh_strict_empty_mcp_config() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let config_dir = temp_dir.path().join("claude-user-config");
        std::fs::create_dir_all(config_dir.join("skills/test-skill")).expect("create user skill");
        std::fs::write(
            config_dir.join("skills/test-skill/SKILL.md"),
            "# test skill",
        )
        .expect("write user skill");
        std::fs::write(
            config_dir.join(".mcp.json"),
            r#"{"mcpServers":{"malicious_sentinel":{"command":"/sentinel"}}}"#,
        )
        .expect("write malicious user MCP config");
        let mut instance = claude_instance(
            temp_dir
                .path()
                .join("claude-home")
                .to_string_lossy()
                .into_owned(),
        );
        instance.shadow_home_path = Some(config_dir.to_string_lossy().into_owned());
        let mut generated_paths = HashSet::new();

        for enable_user_skills in [false, true] {
            for permission_mode in ["default", "acceptEdits", "bypassPermissions"] {
                let process = claude_process_config_from_instance(
                    &instance,
                    &CLIAgentRuntimeSessionStartOptions {
                        approval_policy: Some(permission_mode.to_owned()),
                        enable_user_skills,
                        ..Default::default()
                    },
                )
                .expect("Claude launch config");
                let generated = arg_value_after(process.args.as_slice(), "--mcp-config")
                    .expect("managed MCP config path");
                assert!(Path::new(generated.as_str()).is_absolute());
                assert!(generated_paths.insert(generated.clone()));
                assert_eq!(
                    std::fs::read_to_string(generated.as_str()).expect("read generated MCP config"),
                    "{\"mcpServers\":{}}\n"
                );
                assert!(has_arg(&process.args, "--strict-mcp-config"));
                assert!(!has_arg(&process.args, "--allowedTools"));
                assert!(
                    !process
                        .args
                        .iter()
                        .any(|arg| arg.contains("malicious_sentinel"))
                );
                assert_eq!(
                    has_arg(&process.args, "--safe-mode"),
                    !enable_user_skills && permission_mode != "bypassPermissions"
                );
                assert_eq!(
                    has_arg(&process.args, "--setting-sources=user"),
                    enable_user_skills
                );
            }
        }
        assert_eq!(generated_paths.len(), 6);
    }

    #[test]
    fn claude_malicious_mcp_sentinel_custom_overrides_are_rejected() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let mut instance = claude_instance(temp_dir.path().to_string_lossy().into_owned());
        instance.app_server_args = vec![
            "--mcp-config".to_owned(),
            "/tmp/malicious-sentinel.json".to_owned(),
        ];
        let error = claude_process_config_from_instance(
            &instance,
            &CLIAgentRuntimeSessionStartOptions::default(),
        )
        .expect_err("custom MCP config must be rejected before spawn");
        assert!(format!("{error:#}").contains("reserved option `--mcp-config`"));
    }

    #[test]
    #[cfg(unix)]
    fn claude_process_appends_elevated_prompt_from_owner_only_file() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let instance = claude_instance(temp_dir.path().to_string_lossy().into_owned());
        let governing_text = "Pioneer elevated instructions for Claude";
        let config = claude_process_config_from_instance(
            &instance,
            &CLIAgentRuntimeSessionStartOptions {
                elevated_instructions: Some(elevated_instructions(governing_text)),
                ..Default::default()
            },
        )
        .expect("config should build");

        let prompt_path = arg_value_after(&config.args, "--append-system-prompt-file")
            .expect("managed system prompt path");
        assert_eq!(
            std::fs::read_to_string(prompt_path.as_str()).expect("system prompt contents"),
            governing_text
        );
        assert_eq!(
            std::fs::metadata(prompt_path.as_str())
                .expect("system prompt metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert!(!has_arg(&config.args, "--system-prompt"));
        assert!(!has_arg(&config.args, "--append-system-prompt"));
        assert!(
            !config
                .args
                .iter()
                .any(|argument| argument.contains(governing_text)),
            "governing prompt text must not be exposed in argv"
        );
    }

    #[test]
    fn claude_process_config_uses_session_permission_mode() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let instance = claude_instance(temp_dir.path().to_string_lossy().into_owned());

        let config = claude_process_config_from_instance(
            &instance,
            &CLIAgentRuntimeSessionStartOptions {
                cwd: None,
                approval_policy: Some("acceptEdits".to_owned()),
                app_server_args: Vec::new(),
                env: Default::default(),
                enable_user_skills: false,
                elevated_instructions: None,
            },
        )
        .expect("config should build");

        assert_eq!(
            arg_value_after(config.args.as_slice(), "--permission-mode").as_deref(),
            Some("acceptEdits")
        );
    }

    #[test]
    fn claude_cli_runtime_full_access_uses_bypass_permissions_without_safe_mode() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let instance = claude_instance(temp_dir.path().to_string_lossy().into_owned());

        let config = claude_process_config_from_instance(
            &instance,
            &CLIAgentRuntimeSessionStartOptions {
                cwd: None,
                approval_policy: Some("bypassPermissions".to_owned()),
                app_server_args: Vec::new(),
                env: Default::default(),
                enable_user_skills: false,
                elevated_instructions: None,
            },
        )
        .expect("config should build");

        assert_eq!(
            arg_value_after(config.args.as_slice(), "--permission-mode").as_deref(),
            Some("bypassPermissions")
        );
        assert!(
            !has_arg(config.args.as_slice(), "--safe-mode"),
            "FullAccess must not leave Claude trapped in safe-mode"
        );
    }

    #[test]
    fn claude_cli_runtime_restricted_modes_keep_safe_mode() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let instance = claude_instance(temp_dir.path().to_string_lossy().into_owned());

        for permission_mode in ["default", "acceptEdits"] {
            let config = claude_process_config_from_instance(
                &instance,
                &CLIAgentRuntimeSessionStartOptions {
                    cwd: None,
                    approval_policy: Some(permission_mode.to_owned()),
                    app_server_args: Vec::new(),
                    env: Default::default(),
                    enable_user_skills: false,
                    elevated_instructions: None,
                },
            )
            .expect("config should build");

            assert_eq!(
                arg_value_after(config.args.as_slice(), "--permission-mode").as_deref(),
                Some(permission_mode)
            );
            assert!(
                has_arg(config.args.as_slice(), "--safe-mode"),
                "restricted Claude mode `{permission_mode}` should keep safe-mode"
            );
        }
    }

    #[test]
    fn claude_process_config_skill_enabled_mode_uses_complete_user_source() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let config_dir = temp_dir.path().join("effective-config");
        for relative in [
            "settings.json",
            "hooks/hook.json",
            "commands/command.md",
            "agents/agent.md",
            "rules/rule.md",
            "CLAUDE.md",
            "skills/unrelated/SKILL.md",
        ] {
            let path = config_dir.join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, relative).unwrap();
        }
        let mut instance =
            claude_instance(temp_dir.path().join("home").to_string_lossy().into_owned());
        instance.shadow_home_path = Some(config_dir.to_string_lossy().into_owned());

        for permission_mode in ["default", "acceptEdits", "bypassPermissions"] {
            let normal = claude_process_config_from_instance(
                &instance,
                &CLIAgentRuntimeSessionStartOptions {
                    approval_policy: Some(permission_mode.to_owned()),
                    ..Default::default()
                },
            )
            .unwrap();
            let enabled = claude_process_config_from_instance(
                &instance,
                &CLIAgentRuntimeSessionStartOptions {
                    approval_policy: Some(permission_mode.to_owned()),
                    enable_user_skills: true,
                    ..Default::default()
                },
            )
            .unwrap();

            assert!(has_arg(&normal.args, "--setting-sources="));
            assert!(!has_arg(&normal.args, "--setting-sources=user"));
            assert!(has_arg(&enabled.args, "--setting-sources=user"));
            assert!(!has_arg(&enabled.args, "--setting-sources="));
            assert!(!has_arg(&enabled.args, "--safe-mode"));
            assert_eq!(
                arg_value_after(&enabled.args, "--permission-mode").as_deref(),
                Some(permission_mode)
            );
            assert_eq!(normal.executable, enabled.executable);
            assert_eq!(normal.cwd, enabled.cwd);
            assert_eq!(normal.home_path, enabled.home_path);
            assert_eq!(normal.env, enabled.env);
            assert_eq!(
                enabled.env.expose("CLAUDE_CONFIG_DIR"),
                Some(config_dir.to_string_lossy().as_ref())
            );
        }

        for relative in [
            "settings.json",
            "hooks/hook.json",
            "commands/command.md",
            "agents/agent.md",
            "rules/rule.md",
            "CLAUDE.md",
            "skills/unrelated/SKILL.md",
        ] {
            assert!(config_dir.join(relative).exists());
        }
    }

    #[test]
    fn claude_prompt_from_input_encodes_local_images_as_image_blocks() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let image_path = temp_dir.path().join("screen.png");
        std::fs::write(&image_path, b"png bytes").expect("write image");

        let content = claude_prompt_from_input(json!([
            { "type": "text", "text": "Inspect this image." },
            { "type": "localImage", "path": image_path.to_string_lossy() },
        ]))
        .expect("content");

        assert_eq!(
            content,
            json!([
                { "type": "text", "text": "Inspect this image." },
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": base64::engine::general_purpose::STANDARD.encode(b"png bytes"),
                    },
                },
            ])
        );
    }

    #[test]
    fn claude_prompt_from_input_keeps_files_as_text_references() {
        let content = claude_prompt_from_input(json!([
            { "type": "fileReference", "path": "/tmp/report.pdf" },
        ]))
        .expect("content");

        assert_eq!(
            content,
            json!([
                {
                    "type": "text",
                    "text": "Attached file available at local path:\n/tmp/report.pdf\nUse this path if the task requires inspecting the file.",
                },
            ])
        );
    }

    #[test]
    fn claude_prompt_from_input_keeps_native_skill_directive_as_text() {
        let directive = "Before completing the user's task, invoke every selected skill through Claude's native Skill tool in this order: pdf, slides.";
        let content = claude_prompt_from_input(json!([
            { "type": "text", "text": directive },
            { "type": "text", "text": "Create the report" }
        ]))
        .unwrap();
        assert_eq!(
            content,
            json!([
                { "type": "text", "text": directive },
                { "type": "text", "text": "Create the report" }
            ])
        );
    }

    #[tokio::test]
    async fn claude_skill_fake_stream_preserves_directive_tree_and_zero_skill_regression() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let config_dir = temp_dir.path().join("custom-claude-config");
        let installed = config_dir.join("skills/proposal-51-sentinel");
        std::fs::create_dir_all(installed.join("references")).unwrap();
        std::fs::write(installed.join("SKILL.md"), b"# sentinel\n").unwrap();
        std::fs::write(
            installed.join("references/guide.txt"),
            b"CLAUDE SUPPORTING FILE SENTINEL\n",
        )
        .unwrap();
        let mut instance = claude_instance(temp_dir.path().join("home").to_string_lossy().into());
        instance.shadow_home_path = Some(config_dir.to_string_lossy().into_owned());
        let skill_config = claude_process_config_from_instance(
            &instance,
            &CLIAgentRuntimeSessionStartOptions {
                cwd: Some(temp_dir.path().join("workspace")),
                approval_policy: Some("acceptEdits".to_owned()),
                enable_user_skills: true,
                ..Default::default()
            },
        )
        .unwrap();
        let zero_config = claude_process_config_from_instance(
            &instance,
            &CLIAgentRuntimeSessionStartOptions {
                cwd: Some(temp_dir.path().join("workspace")),
                approval_policy: Some("acceptEdits".to_owned()),
                enable_user_skills: false,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(has_arg(&skill_config.args, "--setting-sources=user"));
        assert!(!has_arg(&skill_config.args, "--safe-mode"));
        assert!(has_arg(&zero_config.args, "--setting-sources="));
        assert!(has_arg(&zero_config.args, "--safe-mode"));
        assert_eq!(skill_config.executable, zero_config.executable);
        assert_eq!(skill_config.cwd, zero_config.cwd);
        assert_eq!(
            skill_config.env.expose("CLAUDE_CONFIG_DIR"),
            Some(config_dir.to_string_lossy().as_ref())
        );

        let directive = "Before completing the user's task, invoke every selected skill through Claude's native Skill tool in this order: proposal-51-sentinel.";
        let log_path = temp_dir.path().join("claude-skill-jsonl.log");
        let (client, mut child) = fake_claude_stream_client(log_path.as_path()).await;
        client
            .start_turn(
                "claude-thread".to_owned(),
                "claude-skill-turn".to_owned(),
                None,
                None,
                json!([
                    { "type": "text", "text": directive },
                    { "type": "text", "text": "Create the report" }
                ]),
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        client
            .start_turn(
                "claude-thread".to_owned(),
                "claude-zero-turn".to_owned(),
                None,
                None,
                json!([{ "type": "text", "text": "Ordinary follow-up" }]),
                Duration::from_secs(2),
            )
            .await
            .unwrap();

        let lines = wait_logged_json_lines(log_path.as_path(), 2).await;
        let skill_line = serde_json::to_string(&lines[0]).unwrap();
        let zero_line = serde_json::to_string(&lines[1]).unwrap();
        assert!(skill_line.contains("proposal-51-sentinel"));
        assert!(skill_line.contains("Create the report"));
        assert!(!zero_line.contains("native Skill tool"));
        assert!(zero_line.contains("Ordinary follow-up"));
        assert_eq!(
            std::fs::read(installed.join("references/guide.txt")).unwrap(),
            b"CLAUDE SUPPORTING FILE SENTINEL\n"
        );

        let _ = child.kill().await;
    }

    #[tokio::test]
    async fn claude_start_turn_sends_effort_control_request() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let log_path = temp_dir.path().join("claude-jsonl.log");
        let (client, mut child) = fake_claude_stream_client(log_path.as_path()).await;

        client
            .start_turn(
                "claude-thread".to_owned(),
                "claude-turn".to_owned(),
                Some("sonnet".to_owned()),
                Some("medium".to_owned()),
                json!([{ "type": "text", "text": "Run tests" }]),
                Duration::from_secs(2),
            )
            .await
            .expect("start turn should send effort and prompt");

        let lines = wait_logged_json_lines(log_path.as_path(), 3).await;
        assert_eq!(lines[0]["type"], "control_request");
        assert_eq!(lines[0]["request"]["subtype"], "set_model");
        assert_eq!(lines[0]["request"]["model"], "sonnet");
        assert_eq!(lines[1]["type"], "control_request");
        assert_eq!(lines[1]["request"]["subtype"], "apply_flag_settings");
        assert_eq!(
            lines[1]["request"]["settings"]["effortLevel"],
            json!("medium")
        );
        assert_eq!(lines[2]["type"], "user");
        assert!(!serde_json::to_string(&lines[2]).unwrap().contains("medium"));

        let _ = child.kill().await;
    }

    #[tokio::test]
    async fn claude_start_turn_omits_effort_control_request_when_not_selected() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let log_path = temp_dir.path().join("claude-jsonl.log");
        let (client, mut child) = fake_claude_stream_client(log_path.as_path()).await;

        client
            .start_turn(
                "claude-thread".to_owned(),
                "claude-turn".to_owned(),
                Some("sonnet".to_owned()),
                None,
                json!([{ "type": "text", "text": "Run tests" }]),
                Duration::from_secs(2),
            )
            .await
            .expect("start turn should send prompt");

        let lines = wait_logged_json_lines(log_path.as_path(), 2).await;
        assert_eq!(lines[0]["type"], "control_request");
        assert_eq!(lines[0]["request"]["subtype"], "set_model");
        assert_eq!(lines[1]["type"], "user");
        assert!(
            lines
                .iter()
                .all(|line| line["request"]["subtype"] != "apply_flag_settings")
        );

        let _ = child.kill().await;
    }

    #[tokio::test]
    async fn claude_turn_ledger_retains_terminal_state_and_final_items_after_stream_closes() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let log_path = temp_dir.path().join("claude-jsonl.log");
        let (client, mut child) = fake_claude_stream_client(log_path.as_path()).await;
        client
            .start_turn(
                "claude-thread".to_owned(),
                "claude-turn".to_owned(),
                None,
                None,
                json!([{ "type": "text", "text": "Run tests" }]),
                Duration::from_secs(2),
            )
            .await
            .expect("start turn should succeed");

        client
            .emit(RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                native_thread_id: Some("claude-thread".to_owned()),
                native_turn_id: "claude-turn".to_owned(),
                native_item_id: "claude-final".to_owned(),
                item_kind: "agentMessage".to_owned(),
                text: Some("final answer".to_owned()),
                summary: Vec::new(),
                content: Vec::new(),
                phase: RuntimeAgentMessagePhase::FinalAnswer,
                metadata: None,
                native_item_redacted: None,
                native: None,
            }))
            .await;
        client
            .emit(RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
                native_thread_id: Some("claude-thread".to_owned()),
                native_turn_id: "claude-turn".to_owned(),
                status: "completed".to_owned(),
                native: None,
            }))
            .await;

        let state = client.state.lock().await;
        let observation = state
            .last_turn_observation
            .as_ref()
            .expect("terminal observation should remain available");
        assert_eq!(
            observation.status,
            CLIAgentRuntimeObservedTurnStatus::Completed
        );
        assert!(matches!(
            observation.reconciliation_events.as_slice(),
            [RuntimeEvent::ItemCompleted(item)] if item.native_item_id == "claude-final"
        ));
        drop(state);

        let _ = child.kill().await;
    }
}
