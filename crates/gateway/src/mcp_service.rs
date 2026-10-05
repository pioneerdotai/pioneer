use crate::patch_history_observer::{SqlitePatchObserver, reserve_patch_history_capacity};
use anyhow::{Context, Result};
use pioneer_agent::{
    AgentMcpAvailability, AgentMcpMaterialization, AgentMcpMaterializationError,
    AgentMcpMaterializationFailureReason, AgentMcpMaterializationRequest,
    AgentMcpPersistedProjection, AgentMcpProjectionPersistenceError,
    AgentMcpProjectionPersistenceRequest, AgentMcpResolutionDiagnostic, AgentMcpServerRef,
    AgentMcpToolProvider, AgentMcpToolRef,
};
use pioneer_crud::{
    CrudStore, McpAuditEventRecord, McpServerCatalogSnapshotRecord, McpServerInstallationRecord,
};
use pioneer_mcp::{
    McpAuthConfig, McpCatalogSnapshot, McpRetryPolicy, McpRuntimeConnector, McpRuntimeError,
    McpRuntimeErrorKind, McpRuntimeState as DomainRuntimeState, McpScopeKind as DomainScopeKind,
    McpSecretRef, McpSecretResolver, McpServerInstallation, McpServerRuntimeSnapshot,
    McpSourceKind as DomainSourceKind, McpToolCallResult, McpTransportConfig, RmcpRuntimeConnector,
    effective_secret_material_fingerprint,
};
use pioneer_protocol::{
    JsonRpcNotification, McpRuntimeState, McpRuntimeStatus, McpScopeKind,
    McpServerCatalogChangedNotification, McpServerStatus, McpServerStatusChangedNotification,
    McpServerStatusItem, TurnAcceptedCapability, TurnCapabilityAcceptedReason, TurnCapabilityKind,
    TurnCapabilityRejectedReason, TurnRejectedCapability, constants::events,
};
use pioneer_tools::ToolHandler;
use pioneer_tools::apply_patch::history::InvocationIdentity;
use serde::Serialize;
use serde_json::Value as JsonValue;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tracing::{error, warn};

use crate::message::now_timestamp_secs;
use crate::secrets::GatewaySecrets;
use crate::session::SessionManager;
use crate::turn_mcp::invoker::{
    CurrentMcpToolIdentity, GatewayTurnMcpInvoker, TurnMcpInvocationError,
    TurnMcpInvocationErrorCode, TurnMcpRuntimeView, TurnMcpValidatedExecution,
    ValidatedTurnMcpInvocation,
};
use crate::turn_mcp::persistence::{
    TurnMcpPersistenceCoordinator, persistence_request_from_projection,
};
use crate::turn_mcp::projection::{canonical_annotations_identity, canonical_schema_identity};
use crate::turn_mcp::result::CanonicalMcpToolResult;
use crate::turn_mcp::{
    DEFAULT_MCP_TURN_TOOL_TIMEOUT_MS, MCP_TURN_PROJECTION_VERSION, McpProjectionLimits,
    McpResolutionDiagnostic, McpSelectionReason, ResolvedMcpTurnProjection, ResolvedMcpTurnTool,
};
use crate::{
    auth::GatewayAuthService,
    authorization::{
        ActionGateDecision, AuthorizationInvalidationHub, AuthorizationService,
        ExecutionLeaseRegistry, ResourceAction,
    },
};

const FIRST_PARTY_FILE_SERVER_ID: &str = "pioneer-file-tools-v1";
const FIRST_PARTY_FILE_SERVER_NAME: &str = "pioneer_files";
const FIRST_PARTY_FILE_CATALOG: &str = "pioneer-file-tools-v1";
const FIRST_PARTY_READ_FILE: &str = "read_file";
const FIRST_PARTY_APPLY_PATCH: &str = "apply_patch";

#[derive(Clone)]
pub(crate) struct McpService {
    pub(crate) inner: Arc<McpServiceInner>,
    pub(crate) runtime_store: Option<Arc<CrudStore>>,
}

pub(crate) struct McpServiceInner {
    pub(crate) crud_store: Arc<CrudStore>,
    authorization_invalidation_hub: Arc<AuthorizationInvalidationHub>,
    execution_leases: Arc<ExecutionLeaseRegistry>,
    invocation_governor: Arc<crate::turn_mcp::invoker::McpInvocationGovernor>,
    pub(crate) session_manager: Arc<SessionManager>,
    auth_service: RwLock<Option<Arc<GatewayAuthService>>>,
    oauth: pioneer_mcp_oauth::McpOAuthService,
    oauth_events: std::sync::Mutex<Option<JoinHandle<()>>>,
    #[cfg(test)]
    oauth_recovery_ack_timeout: std::sync::Mutex<Duration>,
    #[cfg(test)]
    pub(crate) oauth_rpc_read_barrier:
        std::sync::Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
    #[cfg(test)]
    pub(crate) oauth_effect_barrier: std::sync::Mutex<
        Option<(
            Arc<tokio::sync::Notify>,
            Arc<tokio::sync::Notify>,
            Arc<tokio::sync::Notify>,
        )>,
    >,
    gateway_secrets: Arc<GatewaySecrets>,
    snapshot_version: Arc<AtomicU64>,
    runtime_generation_counter: AtomicU64,
    runtime_generations: Mutex<HashMap<String, u64>>,
    active_invocation_counter: AtomicU64,
    active_invocations: std::sync::Mutex<HashMap<u64, ActiveMcpInvocation>>,
    secret_fingerprint_key: [u8; 32],
    permission_approval_broker:
        tokio::sync::RwLock<Arc<dyn pioneer_tools::PermissionApprovalBroker>>,
    projection_persistence: TurnMcpPersistenceCoordinator,
    connector: RwLock<Arc<dyn McpRuntimeConnector>>,
    lifecycle_gates:
        std::sync::Mutex<HashMap<(String, String, String), std::sync::Weak<Mutex<()>>>>,
    tasks: Mutex<HashMap<String, McpServerTaskHandle>>,
    snapshots: Mutex<HashMap<String, McpServerRuntimeSnapshot>>,
    retry_policy: McpRetryPolicy,
    projection_limits: RwLock<McpProjectionLimits>,
}

struct McpServerTaskHandle {
    name: String,
    scope_kind: String,
    scope_key: String,
    fingerprint: String,
    effective_secret_fingerprint: String,
    call_tx: mpsc::Sender<McpServerCommand>,
    oauth_failure_revision: Arc<std::sync::atomic::AtomicU64>,
    recovery: Arc<OAuthRecoveryMailbox>,
    shutdown_tx: Option<oneshot::Sender<McpShutdownRequest>>,
    completed: Arc<tokio_util::sync::CancellationToken>,
    join: Arc<McpTaskJoin>,
    session: Arc<Mutex<Option<Box<dyn pioneer_mcp::McpRuntimeSession>>>>,
}

// Completion is distinct from the shutdown request. It remains in the native
// task map until observed, even when the stop caller is cancelled.
struct McpTaskCompletion(Arc<tokio_util::sync::CancellationToken>);
impl Drop for McpTaskCompletion {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

struct McpTaskJoin {
    state: Mutex<McpTaskJoinState>,
    finished: tokio::task::AbortHandle,
}
struct McpTaskJoinState {
    handle: Option<JoinHandle<Result<()>>>,
    outcome: Option<Result<(), String>>,
}
impl McpTaskJoin {
    fn new(handle: JoinHandle<Result<()>>) -> Arc<Self> {
        Arc::new(Self {
            finished: handle.abort_handle(),
            state: Mutex::new(McpTaskJoinState {
                handle: Some(handle),
                outcome: None,
            }),
        })
    }
    fn is_finished(&self) -> bool {
        self.finished.is_finished()
    }
    async fn wait(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        if let Some(outcome) = &state.outcome {
            return outcome.clone().map_err(anyhow::Error::msg);
        }
        let outcome = match state
            .handle
            .as_mut()
            .context("MCP task join owner is unknown")?
            .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err("MCP runtime cleanup failed".to_owned()),
            Err(_) => Err("MCP runtime task join failed; cleanup unknown".to_owned()),
        };
        state.outcome = Some(outcome.clone());
        state.handle = None;
        outcome.map_err(anyhow::Error::msg)
    }
}

struct McpShutdownRequest {
    final_state: DomainRuntimeState,
    crud_store: Arc<CrudStore>,
}
enum McpServerCommand {
    Tool(McpServerCallCommand),
    OAuthRecovered {
        store: Arc<CrudStore>,
        cancellation: tokio_util::sync::CancellationToken,
        completed: oneshot::Sender<()>,
        admission: Arc<std::sync::Mutex<Option<tokio::sync::OwnedMutexGuard<()>>>>,
        event: Option<pioneer_mcp_oauth::OAuthEvent>,
        failure_revision: u64,
        cause: Option<pioneer_mcp::OAuthFailureCause>,
    },
}
#[derive(Default)]
struct OAuthRecoveryMailbox {
    pending: std::sync::Mutex<Option<McpServerCommand>>,
    wake: tokio::sync::Notify,
}
/// A queued recovery must not retain runtime admission after its waiter expires.
/// An actor that already claimed admission owns it through the actual effect.
struct OAuthRecoveryWait {
    cancellation: tokio_util::sync::CancellationToken,
    admission: Arc<std::sync::Mutex<Option<tokio::sync::OwnedMutexGuard<()>>>>,
}
impl Drop for OAuthRecoveryWait {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}
struct McpServerCallCommand {
    request: pioneer_tools::McpToolCallRequest,
    cancellation: tokio_util::sync::CancellationToken,
    response_tx: oneshot::Sender<Result<McpToolCallResult, McpRuntimeError>>,
}

struct ActiveMcpInvocation {
    turn_id: String,
    server_installation_id: String,
    provider_call_id: String,
    cancellation: tokio_util::sync::CancellationToken,
}

struct ActiveMcpInvocationGuard {
    inner: Arc<McpServiceInner>,
    registration_id: u64,
}

struct McpUpstreamCancellationGuard {
    cancellation: tokio_util::sync::CancellationToken,
}

impl Drop for ActiveMcpInvocationGuard {
    fn drop(&mut self) {
        self.inner
            .active_invocations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.registration_id);
    }
}

impl Drop for McpUpstreamCancellationGuard {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

struct GatewayMcpSecretResolver {
    gateway_secrets: Arc<GatewaySecrets>,
}

#[derive(Default)]
struct WorkspaceMcpToolState {
    available_mcp: Vec<String>,
    blocked_mcp: Vec<String>,
    projected_mcp: Vec<String>,
    projected_blocked_mcp: Vec<String>,
    tools: Vec<ResolvedMcpTurnTool>,
    diagnostics: Vec<McpResolutionDiagnostic>,
    required_unavailable: Vec<McpResolutionDiagnostic>,
    accepted_capabilities: Vec<TurnAcceptedCapability>,
    rejected_capabilities: Vec<TurnRejectedCapability>,
}

#[derive(Clone)]
struct McpToolSelection {
    reason: McpSelectionReason,
    capability_id: Option<String>,
}

fn agent_mcp_diagnostics(
    diagnostics: &[McpResolutionDiagnostic],
) -> Vec<AgentMcpResolutionDiagnostic> {
    diagnostics
        .iter()
        .map(|diagnostic| AgentMcpResolutionDiagnostic {
            code: diagnostic.code.to_owned(),
            message: diagnostic.message.clone(),
        })
        .collect()
}

fn mcp_materialization_error(
    reason: AgentMcpMaterializationFailureReason,
    message: impl Into<String>,
    diagnostics: &[McpResolutionDiagnostic],
    accepted_capabilities: Vec<TurnAcceptedCapability>,
    rejected_capabilities: Vec<TurnRejectedCapability>,
) -> AgentMcpMaterializationError {
    AgentMcpMaterializationError {
        reason,
        message: message.into(),
        diagnostics: agent_mcp_diagnostics(diagnostics),
        accepted_capabilities,
        rejected_capabilities,
    }
}

#[cfg(test)]
const MCP_SELECTION_IMPLICIT_POLICY: &str = "implicit_policy";
#[cfg(test)]
const MCP_SELECTION_EXPLICIT_CAPABILITY: &str = "explicit_composer_capability";

impl McpSecretResolver for GatewayMcpSecretResolver {
    fn resolve_mcp_secret(&self, ref_id: &str) -> Option<String> {
        match self.gateway_secrets.get_mcp_secret(ref_id) {
            Ok(value) => value,
            Err(error) => {
                warn!(
                    ref_id,
                    error = %format!("{error:#}"),
                    "failed to resolve MCP secret"
                );
                None
            }
        }
    }
}

impl McpService {
    #[cfg(test)]
    pub(crate) async fn oauth_recovered(&self, installation_id: &str) {
        self.oauth_recovered_with_admission(installation_id, None, None, None)
            .await;
    }
    async fn oauth_recovered_with_admission(
        &self,
        installation_id: &str,
        guard: Option<tokio::sync::OwnedMutexGuard<()>>,
        event: Option<pioneer_mcp_oauth::OAuthEvent>,
        cause: Option<pioneer_mcp::OAuthFailureCause>,
    ) {
        let admission = Arc::new(std::sync::Mutex::new(guard));
        let tx = self
            .inner
            .tasks
            .lock()
            .await
            .get(installation_id)
            .filter(|task| task.shutdown_tx.is_some())
            .map(|task| {
                (
                    task.call_tx.clone(),
                    task.oauth_failure_revision.load(Ordering::SeqCst),
                    task.recovery.clone(),
                )
            });
        if let Some((_tx, failure_revision, recovery)) = tx {
            let cancellation = tokio_util::sync::CancellationToken::new();
            let cleanup = OAuthRecoveryWait {
                cancellation: cancellation.clone(),
                admission: admission.clone(),
            };
            let (completed, completion) = oneshot::channel();
            let cause = cause.or_else(|| {
                event.as_ref().map(|event| pioneer_mcp::OAuthFailureCause {
                    generation: event.generation.clone(),
                    revision: event.revision,
                })
            });
            let command = McpServerCommand::OAuthRecovered {
                store: self
                    .runtime_store
                    .clone()
                    .unwrap_or_else(|| self.inner.crud_store.clone()),
                cancellation: cancellation.clone(),
                completed,
                admission,
                event,
                failure_revision,
                cause,
            };
            // One mailbox belongs to the runtime actor. Coalescing drops the old
            // waiter/envelope; ack expiry releases admission but never loses work.
            *recovery
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(command);
            recovery.wake.notify_one();
            #[cfg(test)]
            let ack_deadline = *self.inner.oauth_recovery_ack_timeout.lock().unwrap();
            #[cfg(not(test))]
            let ack_deadline = Duration::from_secs(30);
            let _ = tokio::time::timeout(ack_deadline, completion).await;
            drop(cleanup);
        }
    }

    fn runtime_store(&self) -> &CrudStore {
        self.runtime_store
            .as_deref()
            .unwrap_or(self.inner.crud_store.as_ref())
    }
    pub(crate) fn new(
        crud_store: Arc<CrudStore>,
        session_manager: Arc<SessionManager>,
        gateway_secrets: Arc<GatewaySecrets>,
        snapshot_version: Arc<AtomicU64>,
        authorization_invalidation_hub: Arc<AuthorizationInvalidationHub>,
        execution_leases: Arc<ExecutionLeaseRegistry>,
    ) -> Self {
        Self::new_with_oauth_options(
            crud_store,
            session_manager,
            gateway_secrets,
            snapshot_version,
            authorization_invalidation_hub,
            execution_leases,
            pioneer_mcp_oauth::OAuthServiceOptions::default(),
        )
    }

    pub(crate) fn new_with_oauth_options(
        crud_store: Arc<CrudStore>,
        session_manager: Arc<SessionManager>,
        gateway_secrets: Arc<GatewaySecrets>,
        snapshot_version: Arc<AtomicU64>,
        authorization_invalidation_hub: Arc<AuthorizationInvalidationHub>,
        execution_leases: Arc<ExecutionLeaseRegistry>,
        options: pioneer_mcp_oauth::OAuthServiceOptions,
    ) -> Self {
        let projection_persistence = TurnMcpPersistenceCoordinator::new(crud_store.clone());
        let (events, event_rx) = mpsc::unbounded_channel();
        let inner = Arc::new_cyclic(|owner| {
            let sink = Arc::new(crate::mcp_oauth::GatewayOAuthSink {
                owner: owner.clone(),
                events,
            });
            let oauth = pioneer_mcp_oauth::McpOAuthService::with_options(
                gateway_secrets.mcp_oauth_persistence(),
                sink,
                options,
            )
            .expect("OAuth HTTP client");
            McpServiceInner {
                oauth: oauth.clone(),
                oauth_events: std::sync::Mutex::new(None),
                #[cfg(test)]
                oauth_effect_barrier: std::sync::Mutex::new(None),
                #[cfg(test)]
                oauth_rpc_read_barrier: std::sync::Mutex::new(None),
                #[cfg(test)]
                oauth_recovery_ack_timeout: std::sync::Mutex::new(Duration::from_secs(30)),
                crud_store,
                authorization_invalidation_hub,
                execution_leases,
                invocation_governor: Arc::new(
                    crate::turn_mcp::invoker::McpInvocationGovernor::default(),
                ),
                session_manager,
                auth_service: RwLock::new(None),
                gateway_secrets,
                snapshot_version,
                runtime_generation_counter: AtomicU64::new(runtime_generation_seed()),
                runtime_generations: Mutex::new(HashMap::new()),
                active_invocation_counter: AtomicU64::new(0),
                active_invocations: std::sync::Mutex::new(HashMap::new()),
                secret_fingerprint_key: rand::random(),
                permission_approval_broker: tokio::sync::RwLock::new(Arc::new(
                    pioneer_tools::StaticPermissionApprovalBroker::default(),
                )),
                projection_persistence,
                connector: RwLock::new(Arc::new(RmcpRuntimeConnector::with_oauth(Arc::new(oauth)))),
                lifecycle_gates: std::sync::Mutex::new(HashMap::new()),
                tasks: Mutex::new(HashMap::new()),
                snapshots: Mutex::new(HashMap::new()),
                retry_policy: McpRetryPolicy::default(),
                projection_limits: RwLock::new(McpProjectionLimits::default()),
            }
        });
        let worker = tokio::spawn(crate::mcp_oauth::consume_oauth_events(
            Arc::downgrade(&inner),
            event_rx,
        ));
        *inner
            .oauth_events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(worker);
        Self {
            inner,
            runtime_store: None,
        }
    }

    pub(crate) fn set_auth_service(&self, auth_service: Arc<GatewayAuthService>) {
        *self
            .inner
            .auth_service
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(auth_service);
    }

    #[cfg(test)]
    pub(crate) fn set_connector_for_tests(&self, connector: Arc<dyn McpRuntimeConnector>) {
        *self
            .inner
            .connector
            .write()
            .expect("MCP connector lock poisoned") = connector;
    }

    #[cfg(test)]
    pub(crate) async fn persist_resolved_mcp_turn_projection(
        &self,
        projection: &ResolvedMcpTurnProjection,
    ) -> Result<AgentMcpPersistedProjection, AgentMcpProjectionPersistenceError> {
        let request = persistence_request_from_projection(projection)
            .map_err(|message| AgentMcpProjectionPersistenceError { message })?;
        self.inner.projection_persistence.persist(&request).await
    }

    pub(crate) async fn persist_cli_resolved_mcp_turn_projection(
        &self,
        projection: &ResolvedMcpTurnProjection,
        provider_bindings: &[crate::turn_mcp::persistence::TurnMcpProviderBindingIdentity],
    ) -> Result<AgentMcpPersistedProjection, AgentMcpProjectionPersistenceError> {
        let request =
            crate::turn_mcp::persistence::persistence_request_from_projection_with_provider(
                projection,
                provider_bindings,
            )
            .map_err(|message| AgentMcpProjectionPersistenceError { message })?;
        self.inner.projection_persistence.persist(&request).await
    }

    pub(crate) fn configure_projection_limits(
        &self,
        max_tools: usize,
        max_total_schema_bytes: usize,
    ) {
        *self
            .inner
            .projection_limits
            .write()
            .expect("MCP projection limits lock poisoned") = McpProjectionLimits {
            max_tools: max_tools.max(1),
            max_total_schema_bytes: max_total_schema_bytes.max(1),
            ..McpProjectionLimits::default()
        };
    }

    pub(crate) fn projection_limit_values(&self) -> (usize, usize) {
        let limits = self
            .inner
            .projection_limits
            .read()
            .expect("MCP projection limits lock poisoned");
        (limits.max_tools, limits.max_total_schema_bytes)
    }

    pub(crate) async fn set_permission_approval_broker(
        &self,
        broker: Arc<dyn pioneer_tools::PermissionApprovalBroker>,
    ) {
        *self.inner.permission_approval_broker.write().await = broker;
    }

    pub(crate) fn turn_mcp_invoker(&self) -> GatewayTurnMcpInvoker {
        let shared = Arc::new(self.clone());
        GatewayTurnMcpInvoker::new(
            self.inner.crud_store.clone(),
            shared.clone(),
            shared,
            self.inner.authorization_invalidation_hub.clone(),
            self.inner.execution_leases.clone(),
            self.inner.invocation_governor.clone(),
        )
    }

    fn register_active_mcp_invocation(
        &self,
        turn_id: &str,
        server_installation_id: &str,
        provider_call_id: &str,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> ActiveMcpInvocationGuard {
        let registration_id = self
            .inner
            .active_invocation_counter
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        self.inner
            .active_invocations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                registration_id,
                ActiveMcpInvocation {
                    turn_id: turn_id.to_owned(),
                    server_installation_id: server_installation_id.to_owned(),
                    provider_call_id: provider_call_id.to_owned(),
                    cancellation,
                },
            );
        ActiveMcpInvocationGuard {
            inner: self.inner.clone(),
            registration_id,
        }
    }

    pub(crate) fn cancel_turn_mcp_invocations(&self, turn_id: &str) -> usize {
        self.cancel_active_mcp_invocations(|invocation| invocation.turn_id == turn_id)
    }

    fn cancel_installation_mcp_invocations(&self, installation_id: &str) -> usize {
        self.cancel_active_mcp_invocations(|invocation| {
            invocation.server_installation_id == installation_id
        })
    }

    fn cancel_all_mcp_invocations(&self) -> usize {
        self.cancel_active_mcp_invocations(|_| true)
    }

    fn cancel_active_mcp_invocations(
        &self,
        predicate: impl Fn(&ActiveMcpInvocation) -> bool,
    ) -> usize {
        let cancellations = self
            .inner
            .active_invocations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|invocation| predicate(invocation))
            .map(|invocation| {
                (
                    invocation.provider_call_id.clone(),
                    invocation.cancellation.clone(),
                )
            })
            .collect::<Vec<_>>();
        for (provider_call_id, cancellation) in &cancellations {
            cancellation.cancel();
            tracing::debug!(provider_call_id, "cancelled active MCP invocation");
        }
        cancellations.len()
    }

    pub(crate) fn oauth(&self) -> &pioneer_mcp_oauth::McpOAuthService {
        &self.inner.oauth
    }

    pub(crate) async fn stop_oauth_connection(&self, row: &McpServerInstallationRecord) {
        if !row.enabled {
            if let Some(id) = &row.id {
                self.stop_task(id, DomainRuntimeState::Disabled).await;
            }
            self.publish_status(
                row,
                DomainRuntimeState::Disabled,
                Some("server is disabled".into()),
                None,
                0,
                None,
                None,
            )
            .await;
            return;
        }
        if let Some(id) = &row.id {
            self.stop_task(id, DomainRuntimeState::AuthRequired).await;
        }
        self.publish_oauth_required(row, None).await;
    }
    pub(crate) async fn publish_oauth_required(
        &self,
        row: &McpServerInstallationRecord,
        diagnostic: Option<String>,
    ) {
        if let Some(id) = &row.id {
            self.stop_task(id, DomainRuntimeState::AuthRequired).await;
        }
        self.publish_status(
            row,
            DomainRuntimeState::AuthRequired,
            Some(diagnostic.unwrap_or_else(|| "OAuth sign-in required".into())),
            None,
            0,
            None,
            None,
        )
        .await;
    }
    pub(crate) async fn shutdown(&self) {
        self.inner.oauth.shutdown().await;
        if let Some(worker) = self
            .inner
            .oauth_events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            worker.abort();
        }
        self.cancel_all_mcp_invocations();
        let installation_ids = self
            .inner
            .tasks
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for installation_id in installation_ids {
            self.stop_task(installation_id.as_str(), DomainRuntimeState::Stopped)
                .await;
        }
    }

    pub(crate) async fn reload_workspace(&self, workspace_id: &str) -> Result<()> {
        self.reload_workspace_with_store(self.inner.crud_store.as_ref(), workspace_id)
            .await
    }

    pub(crate) async fn reload_workspace_for_maintenance(&self, workspace_id: &str) -> Result<()> {
        let store = self.inner.crud_store.with_maintenance_access();
        self.reload_workspace_with_store(&store, workspace_id).await
    }

    async fn reload_workspace_with_store(
        &self,
        store: &CrudStore,
        workspace_id: &str,
    ) -> Result<()> {
        let rows = store
            .list_mcp_server_installations("workspace", workspace_id)
            .await
            .context("failed to load MCP workspace installations for reload")?;

        let mut desired = HashSet::new();
        for candidate in rows {
            let _guard = self
                .installation_lifecycle_guard(
                    &candidate.scope_kind,
                    &candidate.scope_key,
                    &candidate.name,
                )
                .await;
            let Some(row) = store
                .find_mcp_server_installation(
                    &candidate.scope_kind,
                    &candidate.scope_key,
                    &candidate.name,
                )
                .await?
            else {
                continue;
            };
            let installation_id = row
                .id
                .clone()
                .context("MCP installation row is missing id")?;

            let installation = installation_from_record(&row)?;
            self.inner
                .oauth
                .synchronize(&installation_id, &installation)
                .await?;
            if !row.enabled {
                self.inner.oauth.suspend(&installation_id).await?;
                self.stop_task(&installation_id, DomainRuntimeState::Disabled)
                    .await;
                self.publish_status(
                    &row,
                    DomainRuntimeState::Disabled,
                    Some("server is disabled".to_owned()),
                    None,
                    0,
                    None,
                    None,
                )
                .await;
                continue;
            }

            desired.insert(installation_id.clone());
            let effective_secret_fingerprint = match self.effective_secret_fingerprint_for_row(&row)
            {
                Ok(fingerprint) => fingerprint,
                Err(error) => {
                    if self.task_exists(&installation_id).await {
                        self.publish_status(
                            &row,
                            DomainRuntimeState::Restarting,
                            Some(
                                "effective MCP secret material became unavailable; stopping"
                                    .to_owned(),
                            ),
                            None,
                            0,
                            None,
                            None,
                        )
                        .await;
                        self.stop_task(&installation_id, DomainRuntimeState::Stopped)
                            .await;
                    }
                    self.publish_status(
                        &row,
                        DomainRuntimeState::AuthRequired,
                        Some(error.message.clone()),
                        Some(error.message),
                        0,
                        None,
                        None,
                    )
                    .await;
                    continue;
                }
            };
            let (should_start, secret_material_changed) = {
                let tasks = self.inner.tasks.lock().await;
                match tasks.get(&installation_id) {
                    Some(handle)
                        if handle.shutdown_tx.is_some()
                            && !handle.join.is_finished()
                            && handle.fingerprint == row.fingerprint
                            && handle.effective_secret_fingerprint
                                == effective_secret_fingerprint =>
                    {
                        (false, false)
                    }
                    Some(handle) => (
                        true,
                        handle.effective_secret_fingerprint != effective_secret_fingerprint,
                    ),
                    None => (true, false),
                }
            };

            if should_start {
                if self.task_exists(&installation_id).await {
                    let reason = if secret_material_changed {
                        "effective MCP secret material changed; restarting"
                    } else {
                        "server configuration changed; restarting"
                    };
                    self.publish_status(
                        &row,
                        DomainRuntimeState::Restarting,
                        Some(reason.to_owned()),
                        None,
                        0,
                        None,
                        None,
                    )
                    .await;
                    self.stop_task_result(&installation_id, DomainRuntimeState::Stopped)
                        .await?;
                }
                self.start_task(row, effective_secret_fingerprint).await?;
            }
        }

        let obsolete = {
            let tasks = self.inner.tasks.lock().await;
            tasks
                .iter()
                .filter(|(id, handle)| {
                    handle.scope_kind == "workspace"
                        && handle.scope_key == workspace_id
                        && !desired.contains(*id)
                })
                .map(|(id, task)| (id.clone(), task.name.clone()))
                .collect::<Vec<_>>()
        };
        for (installation_id, name) in obsolete {
            let _guard = self
                .installation_lifecycle_guard("workspace", workspace_id, &name)
                .await;
            // A task installed after the original listing is not obsolete.
            if store
                .find_mcp_server_installation("workspace", workspace_id, &name)
                .await?
                .is_some_and(|row| row.enabled && row.id.as_deref() == Some(&installation_id))
            {
                continue;
            }
            if self
                .stop_task_result(&installation_id, DomainRuntimeState::Stopped)
                .await
                .is_ok()
            {
                self.inner.tasks.lock().await.remove(&installation_id);
            }
        }

        Ok(())
    }

    /// Serialize installation mutation with runtime effects. This owns no DB
    /// capacity and may span stop/join; database operations remain individually scoped.
    pub(crate) async fn installation_lifecycle_guard(
        &self,
        scope_kind: &str,
        scope_key: &str,
        name: &str,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        let gate = {
            let mut gates = self
                .inner
                .lifecycle_gates
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            gates.retain(|_, gate| gate.strong_count() != 0);
            let key = (scope_kind.to_owned(), scope_key.to_owned(), name.to_owned());
            if let Some(gate) = gates.get(&key).and_then(std::sync::Weak::upgrade) {
                gate
            } else {
                let gate = Arc::new(Mutex::new(()));
                gates.insert(key, Arc::downgrade(&gate));
                gate
            }
        };
        gate.lock_owned().await
    }

    pub(crate) async fn apply_oauth_runtime_event(
        &self,
        event: &pioneer_mcp_oauth::OAuthEvent,
        name: &str,
    ) -> Result<()> {
        let _guard = self
            .installation_lifecycle_guard(&event.scope_kind, &event.scope_key, name)
            .await;
        let Some(row) = self
            .runtime_store()
            .find_mcp_server_installation(&event.scope_kind, &event.scope_key, name)
            .await?
        else {
            return Ok(());
        };
        if row.id.as_deref() != Some(&event.installation_id) {
            return Ok(());
        }
        let installation = installation_from_record(&row)?;
        if !self.oauth().event_is_current(event, &installation).await {
            return Ok(());
        }
        match event.state {
            pioneer_mcp_oauth::OAuthState::Authorized => {
                self.stop_task(&event.installation_id, DomainRuntimeState::Stopped)
                    .await;
                // Recheck operation generation after joining the retired actor.
                // The admission guard excludes durable resource/UUID replacement.
                if row.enabled && self.oauth().event_is_current(event, &installation).await {
                    let fingerprint = self.effective_secret_fingerprint_for_row(&row)?;
                    self.start_task(row, fingerprint).await?;
                }
            }
            pioneer_mcp_oauth::OAuthState::Recovered => {
                self.oauth_recovered_with_admission(
                    &event.installation_id,
                    Some(_guard),
                    Some(event.clone()),
                    None,
                )
                .await
            }
            pioneer_mcp_oauth::OAuthState::AuthRequired
            | pioneer_mcp_oauth::OAuthState::InsufficientScope => {
                self.stop_task(&event.installation_id, DomainRuntimeState::AuthRequired)
                    .await;
                if self.oauth().event_is_current(event, &installation).await {
                    self.publish_status(
                        &row,
                        DomainRuntimeState::AuthRequired,
                        event
                            .diagnostic
                            .clone()
                            .or_else(|| Some("MCP authorization required".into())),
                        None,
                        0,
                        None,
                        None,
                    )
                    .await;
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub(crate) async fn restart_server(
        &self,
        scope_kind: &str,
        scope_key: &str,
        name: &str,
    ) -> Result<Option<McpServerInstallationRecord>> {
        let _guard = self
            .installation_lifecycle_guard(scope_kind, scope_key, name)
            .await;
        let row = self
            .runtime_store()
            .find_mcp_server_installation(scope_kind, scope_key, name)
            .await
            .context("failed to query MCP installation for restart")?;
        let Some(row) = row else {
            return Ok(None);
        };
        self.restart_admitted_server(&row).await?;
        Ok(Some(row))
    }

    /// Caller owns the lifecycle gate and has durably revalidated this exact row.
    pub(crate) async fn restart_admitted_server(
        &self,
        row: &McpServerInstallationRecord,
    ) -> Result<()> {
        if let Some(installation_id) = row.id.as_deref() {
            self.publish_status(
                &row,
                DomainRuntimeState::Restarting,
                Some("manual restart requested".to_owned()),
                None,
                0,
                None,
                None,
            )
            .await;
            if self.task_exists(installation_id).await {
                self.stop_task_result(installation_id, DomainRuntimeState::Stopped)
                    .await?;
            }
        }
        if row.enabled {
            match self.effective_secret_fingerprint_for_row(&row) {
                Ok(effective_secret_fingerprint) => {
                    self.start_task(row.clone(), effective_secret_fingerprint)
                        .await?;
                }
                Err(error) => {
                    self.publish_status(
                        &row,
                        DomainRuntimeState::AuthRequired,
                        Some(error.message.clone()),
                        Some(error.message),
                        0,
                        None,
                        None,
                    )
                    .await;
                }
            }
        } else {
            self.publish_status(
                &row,
                DomainRuntimeState::Disabled,
                Some("server is disabled".to_owned()),
                None,
                0,
                None,
                None,
            )
            .await;
        }
        Ok(())
    }

    pub(crate) async fn runtime_snapshot(
        &self,
        scope_kind: &str,
        scope_key: &str,
    ) -> HashMap<String, McpServerRuntimeSnapshot> {
        self.inner
            .snapshots
            .lock()
            .await
            .iter()
            .filter(|(_, snapshot)| {
                snapshot.scope_kind.as_str() == scope_kind && snapshot.scope_key == scope_key
            })
            .map(|(id, snapshot)| (id.clone(), snapshot.clone()))
            .collect()
    }

    async fn workspace_mcp_tool_state(
        &self,
        workspace_id: &str,
        include_implicit_tools: bool,
        explicit_servers: &[AgentMcpServerRef],
        explicit_tools: &[AgentMcpToolRef],
    ) -> Result<WorkspaceMcpToolState> {
        self.reload_workspace(workspace_id).await?;
        let mut rows = self
            .inner
            .crud_store
            .list_mcp_server_installations("workspace", workspace_id)
            .await
            .context("failed to load MCP workspace installations for tool materialization")?;
        for row in &mut rows {
            if let Some(id) = &row.id {
                if self
                    .inner
                    .crud_store
                    .find_mcp_plugin_owner(id)
                    .await?
                    .is_some()
                {
                    row.allow_implicit_invocation = false;
                    row.enabled &= self
                        .inner
                        .crud_store
                        .plugin_child_available("mcp", id, workspace_id)
                        .await?;
                }
            }
        }
        let runtime = self.runtime_snapshot("workspace", workspace_id).await;
        let mut state = WorkspaceMcpToolState::default();
        let mut explicit_servers_by_name = HashMap::<String, Vec<&AgentMcpServerRef>>::new();
        let mut explicit_tools_by_server = HashMap::<String, Vec<&AgentMcpToolRef>>::new();
        let mut explicit_tools_by_key = HashMap::<(String, String), Vec<&AgentMcpToolRef>>::new();
        let mut matched_server_capability_ids = HashSet::<String>::new();
        let mut matched_tool_capability_ids = HashSet::<String>::new();
        let mut seen_server_capability_ids = HashSet::<String>::new();
        let mut seen_tool_capability_ids = HashSet::<String>::new();

        for reference in explicit_servers {
            if reference.capability_id.trim().is_empty() || reference.name.trim().is_empty() {
                state
                    .rejected_capabilities
                    .push(reject_mcp_server_capability(
                        reference,
                        TurnCapabilityRejectedReason::InvalidInput,
                        "MCP server capability is missing an id or server name.",
                    ));
                continue;
            }
            if reference.scope_kind != McpScopeKind::Workspace {
                state
                    .rejected_capabilities
                    .push(reject_mcp_server_capability(
                        reference,
                        TurnCapabilityRejectedReason::ProviderUnsupported,
                        "Only workspace MCP servers can be attached to a turn.",
                    ));
                continue;
            }
            let expected_capability_id =
                pioneer_protocol::mcp_server_capability_key(reference.scope_kind, &reference.name);
            if reference.capability_id != expected_capability_id {
                state
                    .rejected_capabilities
                    .push(reject_mcp_server_capability(
                        reference,
                        TurnCapabilityRejectedReason::InvalidInput,
                        "MCP server capability id does not match its exact server identity.",
                    ));
                continue;
            }
            if !seen_server_capability_ids.insert(reference.capability_id.clone()) {
                state
                    .rejected_capabilities
                    .push(reject_mcp_server_capability(
                        reference,
                        TurnCapabilityRejectedReason::InvalidInput,
                        "MCP server capability is selected more than once.",
                    ));
                continue;
            }
            explicit_servers_by_name
                .entry(mcp_ref_key(reference.name.as_str()))
                .or_default()
                .push(reference);
        }

        for reference in explicit_tools {
            if reference.capability_id.trim().is_empty()
                || reference.server_name.trim().is_empty()
                || reference.raw_tool_name.trim().is_empty()
            {
                state.rejected_capabilities.push(reject_mcp_tool_capability(
                    reference,
                    TurnCapabilityRejectedReason::InvalidInput,
                    "MCP tool capability is missing an id, server name, or tool name.",
                ));
                continue;
            }
            if reference.scope_kind != McpScopeKind::Workspace {
                state.rejected_capabilities.push(reject_mcp_tool_capability(
                    reference,
                    TurnCapabilityRejectedReason::ProviderUnsupported,
                    "Only workspace MCP tools can be attached to a turn.",
                ));
                continue;
            }
            let expected_capability_id = pioneer_protocol::mcp_tool_capability_key(
                reference.scope_kind,
                &reference.server_name,
                &reference.raw_tool_name,
            );
            if reference.capability_id != expected_capability_id {
                state.rejected_capabilities.push(reject_mcp_tool_capability(
                    reference,
                    TurnCapabilityRejectedReason::InvalidInput,
                    "MCP tool capability id does not match its exact server/tool identity.",
                ));
                continue;
            }
            if !seen_tool_capability_ids.insert(reference.capability_id.clone()) {
                state.rejected_capabilities.push(reject_mcp_tool_capability(
                    reference,
                    TurnCapabilityRejectedReason::InvalidInput,
                    "MCP tool capability is selected more than once.",
                ));
                continue;
            }
            let server_key = mcp_ref_key(reference.server_name.as_str());
            let tool_key = mcp_ref_key(reference.raw_tool_name.as_str());
            explicit_tools_by_server
                .entry(server_key.clone())
                .or_default()
                .push(reference);
            explicit_tools_by_key
                .entry((server_key, tool_key))
                .or_default()
                .push(reference);
        }

        for row in rows {
            let server_key = mcp_ref_key(row.name.as_str());
            let server_refs = explicit_servers_by_name
                .get(server_key.as_str())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let tool_refs_for_server = explicit_tools_by_server
                .get(server_key.as_str())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let resolution_relevant = row.required
                || (include_implicit_tools && row.allow_implicit_invocation)
                || !server_refs.is_empty()
                || !tool_refs_for_server.is_empty();
            let installation_id = match row.id.clone() {
                Some(id) => id,
                None => {
                    if resolution_relevant {
                        state.projected_blocked_mcp.push(row.name.clone());
                    }
                    record_unavailable_installation(
                        &mut state,
                        &row,
                        resolution_relevant,
                        format!("MCP server `{}` has no installation id", row.name),
                    );
                    reject_mcp_refs_for_server(
                        &mut state,
                        server_refs,
                        tool_refs_for_server,
                        &mut matched_server_capability_ids,
                        &mut matched_tool_capability_ids,
                        TurnCapabilityRejectedReason::Unavailable,
                        format!("MCP server `{}` is unavailable.", row.name).as_str(),
                    );
                    continue;
                }
            };

            if !row.enabled {
                state.blocked_mcp.push(row.name.clone());
                if resolution_relevant {
                    state.projected_blocked_mcp.push(row.name.clone());
                }
                record_unavailable_installation(
                    &mut state,
                    &row,
                    resolution_relevant,
                    format!("MCP server `{}` is disabled by workspace policy.", row.name),
                );
                reject_mcp_refs_for_server(
                    &mut state,
                    server_refs,
                    tool_refs_for_server,
                    &mut matched_server_capability_ids,
                    &mut matched_tool_capability_ids,
                    TurnCapabilityRejectedReason::DisabledByPolicy,
                    format!("MCP server `{}` is disabled by workspace policy.", row.name).as_str(),
                );
                continue;
            }

            let Some(snapshot) = runtime.get(installation_id.as_str()) else {
                state.blocked_mcp.push(row.name.clone());
                if resolution_relevant {
                    state.projected_blocked_mcp.push(row.name.clone());
                }
                record_unavailable_installation(
                    &mut state,
                    &row,
                    resolution_relevant,
                    format!("MCP server `{}` is not started", row.name),
                );
                reject_mcp_refs_for_server(
                    &mut state,
                    server_refs,
                    tool_refs_for_server,
                    &mut matched_server_capability_ids,
                    &mut matched_tool_capability_ids,
                    TurnCapabilityRejectedReason::Unavailable,
                    format!("MCP server `{}` is not started.", row.name).as_str(),
                );
                continue;
            };

            if !snapshot.state.live() {
                state.blocked_mcp.push(row.name.clone());
                if resolution_relevant {
                    state.projected_blocked_mcp.push(row.name.clone());
                }
                record_unavailable_installation(
                    &mut state,
                    &row,
                    resolution_relevant,
                    format!(
                        "MCP server `{}` is not live ({:?})",
                        row.name, snapshot.state
                    ),
                );
                reject_mcp_refs_for_server(
                    &mut state,
                    server_refs,
                    tool_refs_for_server,
                    &mut matched_server_capability_ids,
                    &mut matched_tool_capability_ids,
                    TurnCapabilityRejectedReason::Unavailable,
                    format!("MCP server `{}` is not live.", row.name).as_str(),
                );
                continue;
            }

            let catalog = self
                .inner
                .crud_store
                .find_mcp_server_catalog_snapshot(installation_id.as_str())
                .await
                .context("failed to load MCP server catalog snapshot")?;
            let Some(catalog) = catalog else {
                state.blocked_mcp.push(row.name.clone());
                if resolution_relevant {
                    state.projected_blocked_mcp.push(row.name.clone());
                }
                record_unavailable_installation(
                    &mut state,
                    &row,
                    resolution_relevant,
                    format!("MCP server `{}` has no catalog snapshot", row.name),
                );
                reject_mcp_refs_for_server(
                    &mut state,
                    server_refs,
                    tool_refs_for_server,
                    &mut matched_server_capability_ids,
                    &mut matched_tool_capability_ids,
                    TurnCapabilityRejectedReason::CatalogMissing,
                    format!("MCP server `{}` has no tool catalog snapshot.", row.name).as_str(),
                );
                continue;
            };

            state.available_mcp.push(row.name.clone());
            if (include_implicit_tools && row.allow_implicit_invocation) || !server_refs.is_empty()
            {
                state.projected_mcp.push(row.name.clone());
            }
            for reference in server_refs {
                matched_server_capability_ids.insert(reference.capability_id.clone());
                state
                    .accepted_capabilities
                    .push(accept_mcp_server_capability(reference));
            }
            let gateway_tool_timeout_ms =
                mcp_transport_tool_timeout_ms(row.transport_json.as_str()).with_context(|| {
                    format!("failed to read MCP tool timeout for installation `{installation_id}`")
                })?;
            let tools = parse_catalog_tools(catalog.tools_json.as_str());
            for tool in tools {
                state
                    .available_mcp
                    .push(format!("{}/{}", row.name, tool.raw_tool_name));
                let tool_refs = explicit_tools_by_key
                    .get(&(server_key.clone(), mcp_ref_key(tool.raw_tool_name.as_str())))
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                for reference in tool_refs {
                    matched_tool_capability_ids.insert(reference.capability_id.clone());
                    state
                        .accepted_capabilities
                        .push(accept_mcp_tool_capability(reference));
                }
                let mut selection = None;
                if include_implicit_tools && row.allow_implicit_invocation {
                    select_mcp_tool(
                        &mut selection,
                        McpToolSelection {
                            reason: McpSelectionReason::ImplicitPolicy,
                            capability_id: None,
                        },
                    );
                }
                if let Some(reference) = server_refs.first() {
                    select_mcp_tool(
                        &mut selection,
                        McpToolSelection {
                            reason: McpSelectionReason::ExplicitServer,
                            capability_id: Some(reference.capability_id.clone()),
                        },
                    );
                }
                if let Some(reference) = tool_refs.first() {
                    select_mcp_tool(
                        &mut selection,
                        McpToolSelection {
                            reason: McpSelectionReason::ExplicitTool,
                            capability_id: Some(reference.capability_id.clone()),
                        },
                    );
                }
                if let Some(selection) = selection {
                    state
                        .projected_mcp
                        .push(format!("{}/{}", row.name, tool.raw_tool_name));
                    let annotations =
                        (tool.annotations != Default::default()).then_some(tool.annotations);
                    state.tools.push(ResolvedMcpTurnTool {
                        canonical_callable_name: String::new(),
                        workspace_id: workspace_id.to_owned(),
                        server_installation_id: installation_id.clone(),
                        server_name: row.name.clone(),
                        raw_tool_name: tool.raw_tool_name,
                        description: Some(tool.description),
                        input_schema: tool.parameters,
                        annotations,
                        timeout_ms: effective_mcp_tool_timeout_ms(
                            tool.timeout_ms,
                            gateway_tool_timeout_ms,
                        ),
                        catalog_version: catalog.catalog_version.clone(),
                        installation_fingerprint: row.fingerprint.clone(),
                        schema_fingerprint: String::new(),
                        runtime_generation: snapshot.runtime_generation,
                        selection_reason: selection.reason,
                        capability_id: selection.capability_id,
                    });
                }
            }
            for reference in tool_refs_for_server {
                if matched_tool_capability_ids.contains(reference.capability_id.as_str()) {
                    continue;
                }
                matched_tool_capability_ids.insert(reference.capability_id.clone());
                state.rejected_capabilities.push(reject_mcp_tool_capability(
                    reference,
                    TurnCapabilityRejectedReason::ToolMissing,
                    format!(
                        "MCP server `{}` does not expose tool `{}`.",
                        row.name, reference.raw_tool_name
                    )
                    .as_str(),
                ));
            }
        }

        for reference in explicit_servers {
            if !eligible_workspace_mcp_server_ref(reference)
                || matched_server_capability_ids.contains(reference.capability_id.as_str())
            {
                continue;
            }
            state
                .rejected_capabilities
                .push(reject_mcp_server_capability(
                    reference,
                    TurnCapabilityRejectedReason::NotFound,
                    format!(
                        "MCP server `{}` is not installed in this workspace.",
                        reference.name
                    )
                    .as_str(),
                ));
        }

        for reference in explicit_tools {
            if !eligible_workspace_mcp_tool_ref(reference)
                || matched_tool_capability_ids.contains(reference.capability_id.as_str())
            {
                continue;
            }
            state.rejected_capabilities.push(reject_mcp_tool_capability(
                reference,
                TurnCapabilityRejectedReason::NotFound,
                format!(
                    "MCP server `{}` is not installed in this workspace.",
                    reference.server_name
                )
                .as_str(),
            ));
        }

        state.available_mcp.sort();
        state.available_mcp.dedup();
        state.blocked_mcp.sort();
        state.blocked_mcp.dedup();
        state.projected_mcp.sort();
        state.projected_mcp.dedup();
        state.projected_blocked_mcp.sort();
        state.projected_blocked_mcp.dedup();
        state.tools.sort_by(|left, right| {
            left.canonical_callable_name
                .cmp(&right.canonical_callable_name)
                .then_with(|| left.server_name.cmp(&right.server_name))
                .then_with(|| left.raw_tool_name.cmp(&right.raw_tool_name))
        });

        Ok(state)
    }

    pub(crate) async fn resolve_mcp_turn_projection(
        &self,
        request: &AgentMcpMaterializationRequest,
    ) -> Result<ResolvedMcpTurnProjection, AgentMcpMaterializationError> {
        let state = match self
            .workspace_mcp_tool_state(
                request.workspace_id.as_str(),
                true,
                request.explicit_servers.as_slice(),
                request.explicit_tools.as_slice(),
            )
            .await
        {
            Ok(state) => state,
            Err(error) => {
                let message = format!("MCP resolution is uncertain: {error:#}");
                let diagnostic = McpResolutionDiagnostic {
                    code: "mcp.resolution.uncertain",
                    message: message.clone(),
                };
                return Err(mcp_materialization_error(
                    AgentMcpMaterializationFailureReason::ResolutionUncertain,
                    message,
                    &[diagnostic],
                    Vec::new(),
                    reject_explicit_mcp_refs_for_uncertainty(
                        request,
                        "MCP capability availability could not be verified.",
                    ),
                ));
            }
        };
        let required_unavailable = state.required_unavailable.clone();
        let mut projection = ResolvedMcpTurnProjection {
            projection_version: MCP_TURN_PROJECTION_VERSION,
            workspace_id: request.workspace_id.clone(),
            turn_id: request.turn_id.clone(),
            tools: state.tools,
            accepted_capabilities: state.accepted_capabilities,
            rejected_capabilities: state.rejected_capabilities,
            available_mcp: state.projected_mcp,
            blocked_mcp: state.projected_blocked_mcp,
            diagnostics: state.diagnostics,
            manifest_hash: String::new(),
        };
        if !required_unavailable.is_empty() {
            return Err(mcp_materialization_error(
                AgentMcpMaterializationFailureReason::RequiredInstallationUnavailable,
                "a required MCP installation is unavailable",
                projection.diagnostics.as_slice(),
                projection.accepted_capabilities,
                projection.rejected_capabilities,
            ));
        }
        if !projection.rejected_capabilities.is_empty() {
            return Err(mcp_materialization_error(
                AgentMcpMaterializationFailureReason::ExplicitCapabilityRejected,
                "one or more explicit MCP capabilities were rejected",
                projection.diagnostics.as_slice(),
                projection.accepted_capabilities,
                projection.rejected_capabilities,
            ));
        }
        let limits = *self
            .inner
            .projection_limits
            .read()
            .expect("MCP projection limits lock poisoned");
        if let Err(error) = projection.finalize_identity(limits) {
            let diagnostic = McpResolutionDiagnostic {
                code: "mcp.projection.invalid",
                message: error.to_string(),
            };
            return Err(mcp_materialization_error(
                AgentMcpMaterializationFailureReason::ProjectionInvalid,
                "MCP projection validation failed",
                &[diagnostic],
                projection.accepted_capabilities,
                projection.rejected_capabilities,
            ));
        }
        Ok(projection)
    }

    async fn task_exists(&self, installation_id: &str) -> bool {
        self.inner.tasks.lock().await.contains_key(installation_id)
    }

    async fn call_tool(
        &self,
        request: pioneer_tools::McpToolCallRequest,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<pioneer_tools::McpToolCallOutput, pioneer_tools::ToolError> {
        let row = self
            .inner
            .crud_store
            .list_mcp_server_installations("workspace", request.workspace_id.as_str())
            .await
            .map_err(|error| {
                pioneer_tools::ToolError::Internal(format!(
                    "failed to load MCP installations before call: {error:#}"
                ))
            })?
            .into_iter()
            .find(|row| row.id.as_deref() == Some(request.server_id.as_str()))
            .ok_or_else(|| {
                pioneer_tools::ToolError::NotFound(format!(
                    "MCP server `{}` is not installed",
                    request.server_name
                ))
            })?;

        if !self
            .inner
            .crud_store
            .plugin_turn_child_available(
                &request.turn_id,
                "mcp",
                &request.server_id,
                &request.workspace_id,
            )
            .await
            .map_err(|_| {
                pioneer_tools::ToolError::Rejected("plugin ownership unavailable".into())
            })?
        {
            return Err(pioneer_tools::ToolError::Rejected(
                "plugin selection is unavailable for this turn".into(),
            ));
        }
        if !row.enabled {
            self.audit_tool_call(
                &row,
                &request,
                "tool_call_blocked",
                "blocked",
                Some("server_disabled"),
                json!({}),
            )
            .await;
            return Err(pioneer_tools::ToolError::Rejected(format!(
                "MCP server `{}` is disabled",
                row.name
            )));
        }

        let snapshot = self
            .inner
            .snapshots
            .lock()
            .await
            .get(request.server_id.as_str())
            .cloned();
        if !snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.state.live())
        {
            self.audit_tool_call(
                &row,
                &request,
                "tool_call_blocked",
                "blocked",
                Some("server_not_live"),
                json!({ "runtime": snapshot.map(|snapshot| format!("{:?}", snapshot.state)) }),
            )
            .await;
            return Err(pioneer_tools::ToolError::NotFound(format!(
                "MCP server `{}` is not live",
                row.name
            )));
        }
        let runtime_state_at_call = snapshot
            .as_ref()
            .map(|snapshot| domain_runtime_state_code(snapshot.state))
            .unwrap_or("unknown")
            .to_owned();

        let catalog = self
            .inner
            .crud_store
            .find_mcp_server_catalog_snapshot(request.server_id.as_str())
            .await
            .map_err(|error| {
                pioneer_tools::ToolError::Internal(format!(
                    "failed to load MCP catalog before call: {error:#}"
                ))
            })?
            .ok_or_else(|| {
                pioneer_tools::ToolError::NotFound(format!(
                    "MCP server `{}` has no catalog snapshot",
                    row.name
                ))
            })?;
        let catalog_drift = catalog.catalog_version != request.catalog_version;
        let tool_exists = parse_catalog_tools(catalog.tools_json.as_str())
            .into_iter()
            .any(|tool| tool.raw_tool_name == request.raw_tool_name);
        if !tool_exists {
            self.audit_tool_call(
                &row,
                &request,
                "tool_call_blocked",
                "blocked",
                Some("tool_missing"),
                json!({
                    "catalog_drift": catalog_drift,
                    "request_catalog_version": request.catalog_version.as_str(),
                    "current_catalog_version": catalog.catalog_version.as_str(),
                }),
            )
            .await;
            return Err(pioneer_tools::ToolError::NotFound(format!(
                "MCP tool `{}` is not present in server `{}` catalog",
                request.raw_tool_name, row.name
            )));
        }

        let call_tx = self
            .inner
            .tasks
            .lock()
            .await
            .get(request.server_id.as_str())
            .filter(|handle| handle.shutdown_tx.is_some())
            .map(|handle| handle.call_tx.clone())
            .ok_or_else(|| {
                pioneer_tools::ToolError::NotFound(format!(
                    "MCP server `{}` task is not running",
                    row.name
                ))
            })?;

        self.audit_tool_call(
            &row,
            &request,
            "tool_call_started",
            "allowed",
            None,
            json!({
                "arguments_keys": json_object_keys(&request.arguments),
                "catalog_drift": catalog_drift,
                "request_catalog_version": request.catalog_version.as_str(),
                "current_catalog_version": catalog.catalog_version.as_str(),
            }),
        )
        .await;

        if cancellation.is_cancelled() {
            return Err(pioneer_tools::ToolError::cancelled(
                "MCP invocation cancelled before gateway runtime dispatch",
            ));
        }
        let upstream_cancellation = cancellation.child_token();
        let _upstream_cancellation_guard = McpUpstreamCancellationGuard {
            cancellation: upstream_cancellation.clone(),
        };
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(request.timeout_ms.max(1));
        let (response_tx, response_rx) = oneshot::channel();
        let command = McpServerCallCommand {
            request: request.clone(),
            cancellation: upstream_cancellation.clone(),
            response_tx,
        };
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(pioneer_tools::ToolError::cancelled(
                    "MCP invocation cancelled before gateway runtime dispatch",
                ));
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(pioneer_tools::ToolError::execution_failed(
                    "MCP tool request timed out while waiting for runtime capacity",
                ));
            }
            result = call_tx.send(McpServerCommand::Tool(command)) => result.map_err(|_| {
                pioneer_tools::ToolError::ExecutionFailed(format!(
                    "MCP server `{}` task is unavailable",
                    row.name
                ))
            })?,
        }

        let response = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                upstream_cancellation.cancel();
                Err(pioneer_tools::ToolError::cancelled(
                    "MCP invocation cancelled while awaiting the gateway runtime",
                ))
            }
            _ = tokio::time::sleep_until(deadline) => {
                upstream_cancellation.cancel();
                Err(pioneer_tools::ToolError::execution_failed(
                    "MCP tool request timed out",
                ))
            }
            response = response_rx => response.map_err(|_| {
                pioneer_tools::ToolError::ExecutionFailed(format!(
                    "MCP server `{}` task dropped the call response",
                    row.name
                ))
            }),
        };
        let result = match response {
            Ok(result) => result,
            Err(error) => {
                let reason_code = match &error {
                    pioneer_tools::ToolError::Cancelled(_) => "cancelled",
                    pioneer_tools::ToolError::ExecutionFailed(message)
                        if message == "MCP tool request timed out" =>
                    {
                        "timed_out"
                    }
                    _ => "gateway_runtime_unavailable",
                };
                self.audit_tool_call(
                    &row,
                    &request,
                    "tool_call_failed",
                    "allowed",
                    Some(reason_code),
                    json!({ "catalog_drift": catalog_drift }),
                )
                .await;
                return Err(error);
            }
        };

        match result {
            Ok(output) => {
                self.audit_tool_call(
                    &row,
                    &request,
                    "tool_call_completed",
                    "allowed",
                    None,
                    json!({
                        "is_error": output.is_error,
                        "duration_ms": output.duration_ms,
                        "catalog_drift": catalog_drift,
                    }),
                )
                .await;
                Ok(pioneer_tools::McpToolCallOutput {
                    content: output.content,
                    structured_content: output.structured_content,
                    is_error: output.is_error,
                    duration_ms: output.duration_ms,
                    meta: Some(enrich_tool_output_meta(
                        output.meta,
                        runtime_state_at_call.as_str(),
                        catalog_drift,
                        catalog.catalog_version.as_str(),
                    )),
                })
            }
            Err(error) => {
                let reason_code = match error.kind {
                    McpRuntimeErrorKind::Cancelled => "cancelled",
                    McpRuntimeErrorKind::TimedOut => "timed_out",
                    McpRuntimeErrorKind::Failed
                    | McpRuntimeErrorKind::AuthRequired
                    | McpRuntimeErrorKind::RefreshRejected
                    | McpRuntimeErrorKind::TransientRefresh
                    | McpRuntimeErrorKind::CredentialStore
                    | McpRuntimeErrorKind::InsufficientScope
                    | McpRuntimeErrorKind::Forbidden => "runtime_error",
                };
                self.audit_tool_call(
                    &row,
                    &request,
                    "tool_call_failed",
                    "allowed",
                    Some(reason_code),
                    json!({
                        "state": format!("{:?}", error.state),
                    }),
                )
                .await;
                match error.kind {
                    McpRuntimeErrorKind::Cancelled => Err(pioneer_tools::ToolError::cancelled(
                        "MCP invocation cancelled by the upstream runtime",
                    )),
                    McpRuntimeErrorKind::TimedOut => Err(
                        pioneer_tools::ToolError::execution_failed("MCP tool request timed out"),
                    ),
                    McpRuntimeErrorKind::Failed
                    | McpRuntimeErrorKind::AuthRequired
                    | McpRuntimeErrorKind::RefreshRejected
                    | McpRuntimeErrorKind::TransientRefresh
                    | McpRuntimeErrorKind::CredentialStore
                    | McpRuntimeErrorKind::InsufficientScope
                    | McpRuntimeErrorKind::Forbidden => {
                        Err(pioneer_tools::ToolError::ExecutionFailed(format!(
                            "MCP tool `{}` failed: {}",
                            request.raw_tool_name, error.message
                        )))
                    }
                }
            }
        }
    }

    async fn start_task(
        &self,
        row: McpServerInstallationRecord,
        effective_secret_fingerprint: String,
    ) -> Result<()> {
        let installation = installation_from_record(&row)?;
        let installation_id = row
            .id
            .clone()
            .context("MCP installation row is missing id")?;
        let connector = self
            .inner
            .connector
            .read()
            .expect("MCP connector lock poisoned")
            .clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (call_tx, call_rx) = mpsc::channel(32);
        let service = self.clone();
        let name = row.name.clone();
        let fingerprint = row.fingerprint.clone();
        let scope_kind = row.scope_kind.clone();
        let scope_key = row.scope_key.clone();
        let task_installation_id = installation_id.clone();
        let runtime_generation = self.next_runtime_generation();
        self.inner
            .runtime_generations
            .lock()
            .await
            .insert(installation_id.clone(), runtime_generation);
        let oauth_failure_revision = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let actor_failure_revision = oauth_failure_revision.clone();
        let recovery = Arc::new(OAuthRecoveryMailbox::default());
        let actor_recovery = recovery.clone();
        let session = Arc::new(Mutex::new(None));
        let actor_session = session.clone();
        let completed = Arc::new(tokio_util::sync::CancellationToken::new());
        let completion = McpTaskCompletion(completed.clone());
        let join = tokio::spawn(async move {
            let _completion = completion;
            service
                .run_server_task(
                    row,
                    installation,
                    task_installation_id,
                    connector,
                    shutdown_rx,
                    call_rx,
                    actor_failure_revision,
                    actor_recovery,
                    actor_session,
                )
                .await
        });
        let join = McpTaskJoin::new(join);

        self.inner.tasks.lock().await.insert(
            installation_id,
            McpServerTaskHandle {
                name,
                scope_kind,
                scope_key,
                fingerprint,
                effective_secret_fingerprint,
                call_tx,
                oauth_failure_revision,
                recovery,
                shutdown_tx: Some(shutdown_tx),
                completed,
                join,
                session,
            },
        );

        Ok(())
    }

    fn effective_secret_fingerprint_for_row(
        &self,
        row: &McpServerInstallationRecord,
    ) -> Result<String, McpRuntimeError> {
        let installation = installation_from_record(row).map_err(|error| {
            McpRuntimeError::failed(format!("invalid MCP installation: {error}"))
        })?;
        let resolver = GatewayMcpSecretResolver {
            gateway_secrets: self.inner.gateway_secrets.clone(),
        };
        effective_secret_material_fingerprint(
            &installation,
            &resolver,
            self.inner.secret_fingerprint_key.as_slice(),
        )
    }

    async fn stop_task(&self, installation_id: &str, final_state: DomainRuntimeState) {
        // Preserve the legacy best-effort wrapper. An absent task is never an
        // acknowledgement through the Result-returning business entrypoint.
        if !self.task_exists(installation_id).await {
            return;
        }
        if let Err(error) = self.stop_task_result(installation_id, final_state).await {
            warn!(error = %error, "MCP server task join failed");
        }
    }

    // Caller owns the existing installation lifecycle guard. The actor owns
    // cleanup independently of this waiter and the task map keeps its outcome.
    pub(crate) async fn stop_task_result(
        &self,
        installation_id: &str,
        final_state: DomainRuntimeState,
    ) -> Result<()> {
        let (completion, join, session) = {
            let mut tasks = self.inner.tasks.lock().await;
            let handle = tasks
                .get_mut(installation_id)
                .context("MCP runtime stop owner is unknown")?;
            handle
                .recovery
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(shutdown) = handle.shutdown_tx.take() {
                let _ = shutdown.send(McpShutdownRequest {
                    final_state,
                    crud_store: self
                        .runtime_store
                        .clone()
                        .unwrap_or_else(|| self.inner.crud_store.clone()),
                });
            }
            (
                handle.completed.clone(),
                handle.join.clone(),
                handle.session.clone(),
            )
        };
        self.cancel_installation_mcp_invocations(installation_id);
        // Token means future ended, not success: always inspect retained join.
        completion.cancelled().await;
        let outcome = join.wait().await;
        if outcome.is_err() {
            // A panicked/failed actor does not discard its native session.
            // Retry actual cleanup, while preserving the failed join outcome.
            if let Some(session) = session.lock().await.as_mut() {
                let _ = session.shutdown_result().await;
            }
        }
        outcome?;
        let tasks = self.inner.tasks.lock().await;
        if tasks
            .get(installation_id)
            .is_some_and(|handle| !Arc::ptr_eq(&handle.completed, &completion))
        {
            anyhow::bail!("MCP runtime changed during shutdown");
        }
        Ok(())
    }

    async fn run_server_task(
        &self,
        row: McpServerInstallationRecord,
        installation: McpServerInstallation,
        installation_id: String,
        connector: Arc<dyn McpRuntimeConnector>,
        mut shutdown_rx: oneshot::Receiver<McpShutdownRequest>,
        mut call_rx: mpsc::Receiver<McpServerCommand>,
        oauth_failure_revision: Arc<std::sync::atomic::AtomicU64>,
        recovery: Arc<OAuthRecoveryMailbox>,
        retained_session: Arc<Mutex<Option<Box<dyn pioneer_mcp::McpRuntimeSession>>>>,
    ) -> Result<()> {
        let resolver = Arc::new(GatewayMcpSecretResolver {
            gateway_secrets: self.inner.gateway_secrets.clone(),
        });
        let mut retry_attempt = 0_u32;

        'connection: loop {
            let now = now_timestamp_secs();
            self.audit(&row, "start", None, json!({"retry_attempt": retry_attempt}))
                .await;
            self.publish_status(
                &row,
                DomainRuntimeState::Starting,
                Some("starting MCP server".to_owned()),
                None,
                retry_attempt,
                None,
                None,
            )
            .await;

            // Do not drop a connector which already owns a spawned child.
            // The existing connector startup budget applies; a stop observer's
            // deadline can expire without disposing this actual owner.
            if let Ok(final_state) = shutdown_rx.try_recv() {
                self.publish_scoped_shutdown(&row, final_state).await;
                return Ok(());
            }
            let connect = connector
                .connect(
                    installation.clone(),
                    installation_id.clone(),
                    resolver.clone(),
                    now,
                )
                .await;

            let session = match connect {
                Ok(session) => session,
                Err(error) => {
                    let next_retry_at = if error.state == DomainRuntimeState::Failed {
                        Some(now + self.inner.retry_policy.delay_secs(retry_attempt) as i64)
                    } else {
                        None
                    };
                    self.audit(
                        &row,
                        if error.state == DomainRuntimeState::AuthRequired {
                            "auth_required"
                        } else {
                            "start_failed"
                        },
                        None,
                        json!({"reason": error.message, "retry_attempt": retry_attempt}),
                    )
                    .await;
                    self.publish_status(
                        &row,
                        error.state,
                        Some(error.message.clone()),
                        Some(error.message),
                        retry_attempt,
                        next_retry_at,
                        None,
                    )
                    .await;

                    if error.state == DomainRuntimeState::AuthRequired {
                        if let Ok(final_state) = shutdown_rx.await {
                            self.publish_scoped_shutdown(&row, final_state).await;
                        }
                        return Ok(());
                    }

                    let delay = self.inner.retry_policy.delay_secs(retry_attempt);
                    retry_attempt = retry_attempt.saturating_add(1);
                    tokio::select! {
                        final_state = &mut shutdown_rx => {
                            if let Ok(final_state) = final_state {
                                self.publish_scoped_shutdown(&row, final_state).await;
                            }
                            return Ok(());
                        }
                        _ = sleep(Duration::from_secs(delay)) => {}
                    }
                    continue;
                }
            };

            let mut session_owner = retained_session.lock().await;
            *session_owner = Some(session);
            let session = session_owner.as_mut().expect("native MCP session owner");
            if let Some(error) = session.startup_failure().cloned() {
                self.publish_status(
                    &row,
                    DomainRuntimeState::Failed,
                    Some(error.message.clone()),
                    Some(error.message),
                    0,
                    None,
                    None,
                )
                .await;
                if let Ok(state) = shutdown_rx.await {
                    self.publish_scoped_shutdown(&row, state).await;
                }
                session
                    .shutdown_result()
                    .await
                    .map_err(|_| anyhow::anyhow!("MCP startup cleanup unconfirmed"))?;
                anyhow::bail!("MCP startup cleanup previously failed; native stop outcome unknown");
            }
            let catalog = session.initial_catalog().clone();
            self.persist_catalog_and_notify(&row, &catalog).await;
            let ready_state = if session.degraded_reason().is_some() {
                DomainRuntimeState::Degraded
            } else {
                DomainRuntimeState::Ready
            };
            let ready_reason = session
                .degraded_reason()
                .map(str::to_owned)
                .or_else(|| Some("MCP server is ready".to_owned()));
            self.audit(
                &row,
                "started",
                Some(catalog.catalog_version.as_str()),
                json!({}),
            )
            .await;
            self.publish_status(
                &row,
                ready_state,
                ready_reason,
                None,
                0,
                None,
                Some(catalog.catalog_version.clone()),
            )
            .await;

            let mut oauth_degraded = false;
            let mut oauth_failure_cause: Option<pioneer_mcp::OAuthFailureCause> = None;
            let mut before_oauth_degradation: Option<McpServerRuntimeSnapshot> = None;
            loop {
                tokio::select! {
                    final_state = &mut shutdown_rx => {
                        let shutdown_store = final_state.as_ref().ok().map(|request| request.crud_store.clone());
                        let shutdown_service = McpService { inner: self.inner.clone(), runtime_store: shutdown_store.or_else(|| self.runtime_store.clone()) };
                        shutdown_service.publish_status(
                            &row,
                            DomainRuntimeState::Stopping,
                            Some("stopping MCP server".to_owned()),
                            None,
                            0,
                            None,
                            Some(catalog.catalog_version.clone()),
                        )
                        .await;
                        shutdown_service.audit(&row, "stop", Some(catalog.catalog_version.as_str()), json!({})).await;
                        session.shutdown_result().await.map_err(|_| anyhow::anyhow!("MCP session cleanup failed"))?;
                        if let Ok(final_state) = final_state {
                            self.publish_scoped_shutdown(&row, final_state).await;
                        }
                        shutdown_service.audit(&row, "stopped", Some(catalog.catalog_version.as_str()), json!({})).await;
                        return Ok(());
                    }
                    event = session.wait_for_event() => {
                        match event {
                            pioneer_mcp::McpSessionEvent::CatalogChanged => {
                                match session.refresh_catalog().await {
                                    Ok(catalog) => {
                                        // A successful catalog reconciliation supersedes the
                                        // earlier OAuth failure and its saved status snapshot.
                                        oauth_degraded = false;
                                        before_oauth_degradation = None;
                                        let degraded_reason = session
                                            .degraded_reason()
                                            .map(ToOwned::to_owned);
                                        let (state, reason) = if let Some(reason) = degraded_reason {
                                            (DomainRuntimeState::Degraded, reason)
                                        } else {
                                            (
                                                DomainRuntimeState::Ready,
                                                "MCP catalog refreshed".to_owned(),
                                            )
                                        };
                                        self.persist_catalog_and_notify(&row, &catalog).await;
                                        self.publish_status(
                                            &row,
                                            state,
                                            Some(reason),
                                            None,
                                            0,
                                            None,
                                            Some(catalog.catalog_version.clone()),
                                        )
                                        .await;
                                    }
                                    Err(error) => {
                                        let oauth_error = matches!(error.kind, McpRuntimeErrorKind::TransientRefresh | McpRuntimeErrorKind::CredentialStore);
                                        if oauth_error { oauth_failure_revision.fetch_add(1, Ordering::SeqCst); oauth_failure_cause = error.oauth_failure.clone(); }
                                        if oauth_error && !oauth_degraded { before_oauth_degradation = self.inner.snapshots.lock().await.get(&installation_id).cloned(); }
                                        if !oauth_error { before_oauth_degradation = None; }
                                        oauth_degraded = oauth_error;
                                        self.publish_status(
                                            &row,
                                            if error.state==DomainRuntimeState::AuthRequired {DomainRuntimeState::AuthRequired}else{DomainRuntimeState::Degraded},
                                            Some(error.message.clone()),
                                            Some(error.message),
                                            0,
                                            None,
                                            None,
                                        )
                                        .await;
                                    }
                                }
                            }
                            pioneer_mcp::McpSessionEvent::Closed => {
                                let error = McpRuntimeError::failed("MCP session closed");
                                self.publish_status(
                                    &row,
                                    DomainRuntimeState::Failed,
                                    Some(error.message.clone()),
                                    Some(error.message),
                                    0,
                                    None,
                                    None,
                                )
                                .await;
                                session.shutdown_result().await.map_err(|_| anyhow::anyhow!("MCP session cleanup failed"))?;
                                let delay=self.inner.retry_policy.delay_secs(retry_attempt);
                                retry_attempt=retry_attempt.saturating_add(1);
                                tokio::select!{final_state=&mut shutdown_rx=>{if let Ok(state)=final_state{self.publish_scoped_shutdown(&row, state).await;}return Ok(());},_=sleep(Duration::from_secs(delay))=>{}}
                                continue 'connection;
                            }
                        }
                    }
                    command = async {
                        tokio::select! {
                            command=call_rx.recv()=>command,
                            _=recovery.wake.notified()=>recovery.pending.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take(),
                        }
                    } => {
                        let Some(command) = command else {
                            continue;
                        };
                        let command = match command {
                            McpServerCommand::Tool(command) => command,
                            McpServerCommand::OAuthRecovered { store, cancellation, completed, admission, event, failure_revision, cause } => {
                                let _admission = {
                                    let mut guard = admission.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                                    if cancellation.is_cancelled() { guard.take(); None } else { guard.take() }
                                };
                                // A timed-out waiter released its transferable guard.
                                // The still-owned mailbox must reacquire admission, while
                                // shutdown can interrupt acquisition (no stop/join deadlock).
                                let _retry_admission = if _admission.is_none() {
                                    tokio::select! {
                                        state=&mut shutdown_rx=>{ if let Ok(state)=state { self.publish_scoped_shutdown(&row,state).await; } session.shutdown_result().await.map_err(|_| anyhow::anyhow!("MCP session cleanup failed"))?; return Ok(()); },
                                        guard=self.installation_lifecycle_guard(&row.scope_kind,&row.scope_key,&row.name)=>Some(guard),
                                    }
                                } else { None };
                                let recovery_service = McpService { inner: self.inner.clone(), runtime_store: Some(store) };
                                let current = match event.as_ref() {
                                    Some(event) => match recovery_service.runtime_store().find_mcp_server_installation(&row.scope_kind,&row.scope_key,&row.name).await {
                                        Ok(Some(current)) if current.id == row.id => match installation_from_record(&current) { Ok(installation)=>self.oauth().event_is_current(event,&installation).await, Err(_)=>false },
                                        _=>false,
                                    },
                                    None => true,
                                };
                                let same_failure = match (&oauth_failure_cause, &cause) {
                                    (Some(failure), Some(recovery)) => failure.generation == recovery.generation && failure.revision <= recovery.revision,
                                    _ => failure_revision == oauth_failure_revision.load(Ordering::SeqCst),
                                };
                                if oauth_degraded && current && same_failure {
                                    oauth_degraded = false;
                                    let prior = before_oauth_degradation.take().filter(|snapshot| snapshot.state == DomainRuntimeState::Degraded);
                                    let degraded = session.degraded_reason().map(str::to_owned);
                                    let state = if prior.is_some() || degraded.is_some() { DomainRuntimeState::Degraded } else { DomainRuntimeState::Ready };
                                    let reason = prior.as_ref().and_then(|snapshot| snapshot.status_reason.clone()).or(degraded).or_else(|| Some("MCP authorization recovered".into()));
                                    let last_error = prior.and_then(|snapshot| snapshot.last_error);
                                    recovery_service.publish_status(&row, state, reason, last_error, 0, None, Some(catalog.catalog_version.clone())).await;
                                }
                                let _ = completed.send(());
                                continue;
                            }
                        };
                        let raw_tool_name = command.request.raw_tool_name.clone();
                        let arguments = command.request.arguments.clone();
                        let result = session
                            .call_tool(
                                raw_tool_name.as_str(),
                                arguments,
                                pioneer_mcp::McpInvocationBudget {
                                    max_arguments_bytes: command.request.max_arguments_bytes,
                                },
                                Duration::from_millis(command.request.timeout_ms.max(1)),
                                command.cancellation,
                            )
                            .await;
                        if let Err(error) = &result
                            && error.kind != McpRuntimeErrorKind::Cancelled
                        {
                            let oauth_error = matches!(error.kind, McpRuntimeErrorKind::TransientRefresh | McpRuntimeErrorKind::CredentialStore);
                                        if oauth_error { oauth_failure_revision.fetch_add(1, Ordering::SeqCst); oauth_failure_cause = error.oauth_failure.clone(); }
                                        if oauth_error && !oauth_degraded { before_oauth_degradation = self.inner.snapshots.lock().await.get(&installation_id).cloned(); }
                                        if !oauth_error { before_oauth_degradation = None; }
                                        oauth_degraded = oauth_error;
                            self.publish_status(
                                &row,
                                if error.state==DomainRuntimeState::AuthRequired {DomainRuntimeState::AuthRequired}else{DomainRuntimeState::Degraded},
                                Some(error.message.clone()),
                                Some(error.message.clone()),
                                0,
                                None,
                                Some(catalog.catalog_version.clone()),
                            )
                            .await;
                        }
                        let _ = command.response_tx.send(result);
                    }
                }
            }
        }
    }

    async fn publish_scoped_shutdown(
        &self,
        row: &McpServerInstallationRecord,
        request: McpShutdownRequest,
    ) {
        let service = McpService {
            inner: self.inner.clone(),
            runtime_store: Some(request.crud_store),
        };
        service
            .publish_shutdown_status(row, request.final_state)
            .await;
    }

    async fn publish_shutdown_status(
        &self,
        row: &McpServerInstallationRecord,
        final_state: DomainRuntimeState,
    ) {
        let reason = match final_state {
            DomainRuntimeState::Disabled => "server is disabled",
            DomainRuntimeState::Stopped => "MCP server stopped",
            _ => "MCP server stopped",
        };
        self.publish_status(
            row,
            final_state,
            Some(reason.to_owned()),
            None,
            0,
            None,
            None,
        )
        .await;
    }

    async fn publish_status(
        &self,
        row: &McpServerInstallationRecord,
        state: DomainRuntimeState,
        status_reason: Option<String>,
        last_error: Option<String>,
        retry_attempt: u32,
        next_retry_at_unix: Option<i64>,
        catalog_version: Option<String>,
    ) {
        let Some(installation_id) = row.id.clone() else {
            return;
        };
        let scope_kind = match DomainScopeKind::from_str(row.scope_kind.as_str()) {
            Ok(scope_kind) => scope_kind,
            Err(error) => {
                error!(error, "failed to map MCP runtime scope");
                return;
            }
        };
        let snapshot = McpServerRuntimeSnapshot {
            installation_id: installation_id.clone(),
            name: row.name.clone(),
            scope_kind,
            scope_key: row.scope_key.clone(),
            fingerprint: row.fingerprint.clone(),
            runtime_generation: self
                .inner
                .runtime_generations
                .lock()
                .await
                .get(installation_id.as_str())
                .copied()
                .unwrap_or(0),
            state,
            live: state.live(),
            status_reason,
            last_seen_at_unix: state.live().then_some(now_timestamp_secs()),
            last_error,
            retry_attempt,
            next_retry_at_unix,
            catalog_version,
        };

        let changed = {
            let mut snapshots = self.inner.snapshots.lock().await;
            if snapshots
                .get(installation_id.as_str())
                .is_some_and(|existing| existing == &snapshot)
            {
                false
            } else {
                snapshots.insert(installation_id.clone(), snapshot.clone());
                true
            }
        };
        if !changed {
            return;
        }

        let snapshot_version = self.next_snapshot_version();
        let notification = McpServerStatusChangedNotification {
            workspace_id: row.scope_key.clone(),
            snapshot_version,
            server: McpServerStatusItem {
                id: installation_id,
                name: snapshot.name.clone(),
                scope_kind: protocol_scope_kind(snapshot.scope_kind.clone()),
                runtime: McpRuntimeStatus {
                    state: protocol_runtime_state(snapshot.state),
                    live: snapshot.live,
                    last_seen_at: snapshot.last_seen_at_unix,
                },
                status: McpServerStatus::from(protocol_runtime_state(snapshot.state)),
            },
        };
        self.send_management_notification(events::MCP_SERVER_STATUS_CHANGED, &notification)
            .await;
    }

    async fn persist_catalog_and_notify(
        &self,
        row: &McpServerInstallationRecord,
        catalog: &McpCatalogSnapshot,
    ) {
        let existing = self
            .runtime_store()
            .find_mcp_server_catalog_snapshot(catalog.server_installation_id.as_str())
            .await
            .ok()
            .flatten();
        let changed = existing
            .as_ref()
            .map(|existing| existing.catalog_version != catalog.catalog_version)
            .unwrap_or(true);

        let record = McpServerCatalogSnapshotRecord {
            server_installation_id: catalog.server_installation_id.clone(),
            catalog_version: catalog.catalog_version.clone(),
            server_info_json: catalog.server_info_json.clone(),
            server_instructions_hash: catalog.server_instructions_hash.clone(),
            tools_json: catalog.tools_json.clone(),
            resources_json: catalog.resources_json.clone(),
            resource_templates_json: catalog.resource_templates_json.clone(),
            prompts_json: catalog.prompts_json.clone(),
            generated_at_unix: catalog.generated_at_unix,
        };
        if let Err(error) = self
            .runtime_store()
            .upsert_mcp_server_catalog_snapshot(&record, now_timestamp_secs())
            .await
        {
            warn!(
                server = row.name,
                error = %format!("{error:#}"),
                "failed to persist MCP catalog snapshot"
            );
            return;
        }
        self.audit(
            row,
            "catalog_refreshed",
            Some(catalog.catalog_version.as_str()),
            json!({
                "tools_count": catalog.tools_count(),
                "resources_count": catalog.resources_count(),
                "resource_templates_count": catalog.resource_templates_count(),
                "prompts_count": catalog.prompts_count(),
            }),
        )
        .await;

        if !changed {
            return;
        }

        let notification = McpServerCatalogChangedNotification {
            workspace_id: row.scope_key.clone(),
            snapshot_version: self.next_snapshot_version(),
            server_id: catalog.server_installation_id.clone(),
            name: row.name.clone(),
            catalog_version: catalog.catalog_version.clone(),
            tools_count: catalog.tools_count(),
            resources_count: catalog.resources_count(),
            resource_templates_count: catalog.resource_templates_count(),
            prompts_count: catalog.prompts_count(),
        };
        self.send_management_notification(events::MCP_SERVER_CATALOG_CHANGED, &notification)
            .await;
    }

    async fn audit(
        &self,
        row: &McpServerInstallationRecord,
        action: &str,
        catalog_version: Option<&str>,
        details: serde_json::Value,
    ) {
        let audit = McpAuditEventRecord {
            turn_id: None,
            server_installation_id: row.id.clone(),
            server_name: row.name.clone(),
            raw_tool_name: None,
            callable_name: None,
            catalog_version: catalog_version.map(str::to_owned),
            action: action.to_owned(),
            decision: "allowed".to_owned(),
            reason_code: None,
            details_json: serde_json::to_string(&details).unwrap_or_else(|_| "{}".to_owned()),
            created_at_unix: now_timestamp_secs(),
        };
        if let Err(error) = self
            .runtime_store()
            .insert_mcp_audit_event_record(&audit)
            .await
        {
            warn!(
                server = row.name,
                action,
                error = %format!("{error:#}"),
                "failed to write MCP runtime audit event"
            );
        }
    }

    async fn audit_tool_call(
        &self,
        row: &McpServerInstallationRecord,
        request: &pioneer_tools::McpToolCallRequest,
        action: &str,
        decision: &str,
        reason_code: Option<&str>,
        details: serde_json::Value,
    ) {
        let audit = McpAuditEventRecord {
            turn_id: Some(request.turn_id.clone()),
            server_installation_id: row.id.clone(),
            server_name: row.name.clone(),
            raw_tool_name: Some(request.raw_tool_name.clone()),
            callable_name: Some(request.callable_name.clone()),
            catalog_version: Some(request.catalog_version.clone()),
            action: action.to_owned(),
            decision: decision.to_owned(),
            reason_code: reason_code.map(str::to_owned),
            details_json: serde_json::to_string(&details).unwrap_or_else(|_| "{}".to_owned()),
            created_at_unix: now_timestamp_secs(),
        };
        if let Err(error) = self
            .inner
            .crud_store
            .insert_mcp_audit_event_record(&audit)
            .await
        {
            warn!(
                server = row.name,
                action,
                error = %format!("{error:#}"),
                "failed to write MCP tool audit event"
            );
        }
    }

    fn next_snapshot_version(&self) -> u64 {
        self.inner
            .snapshot_version
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1)
    }

    fn next_runtime_generation(&self) -> u64 {
        self.inner
            .runtime_generation_counter
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1)
    }

    pub(crate) async fn send_management_notification<T: Serialize>(
        &self,
        method: &str,
        payload: &T,
    ) {
        let candidate_connection_ids = self.inner.session_manager.connection_ids().await;
        let initially_authorized_connection_ids = self
            .authorized_management_notification_recipients(candidate_connection_ids)
            .await;
        if initially_authorized_connection_ids.is_empty() {
            return;
        }
        let serialization_connection_ids = self
            .authorized_management_notification_recipients(initially_authorized_connection_ids)
            .await;
        if serialization_connection_ids.is_empty() {
            return;
        }
        let notification = match JsonRpcNotification::from_params(method, payload) {
            Ok(notification) => notification,
            Err(error) => {
                error!(method, error = %error, "failed to encode MCP notification");
                return;
            }
        };
        let serialized = match serde_json::to_string(&notification) {
            Ok(payload) => payload,
            Err(error) => {
                error!(method, error = %error, "failed to serialize MCP notification");
                return;
            }
        };
        let connection_ids = self
            .authorized_management_notification_recipients(serialization_connection_ids)
            .await;
        for connection_id in connection_ids {
            if let Err(error) = self
                .inner
                .session_manager
                .send_text(connection_id, serialized.clone())
                .await
            {
                warn!(
                    connection_id,
                    method,
                    error = %format!("{error:#}"),
                    "failed to send MCP notification"
                );
            }
        }
    }

    pub(crate) async fn authorized_management_notification_recipients(
        &self,
        candidate_connection_ids: Vec<u64>,
    ) -> Vec<u64> {
        let auth_service = self
            .inner
            .auth_service
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let authorization_service = AuthorizationService::new();
        let mut connection_ids = Vec::with_capacity(candidate_connection_ids.len());
        for connection_id in candidate_connection_ids {
            let Ok(principal) = self
                .inner
                .session_manager
                .connection_principal(connection_id)
                .await
            else {
                continue;
            };
            if let Some(auth_service) = auth_service.as_ref() {
                let valid = if let Some(store) = &self.runtime_store {
                    auth_service
                        .validate_session_lease_with_database(
                            principal.as_ref(),
                            &store.database_connection(),
                        )
                        .await
                } else {
                    auth_service
                        .validate_session_lease(principal.as_ref())
                        .await
                };
                if valid.is_err() {
                    continue;
                }
            }
            let action_gate = authorization_service.authorize_action(
                principal.kind,
                principal.role_key.as_ref(),
                ResourceAction::McpManage,
            );
            if action_gate == ActionGateDecision::AllowAbsolute {
                connection_ids.push(connection_id);
            }
        }
        connection_ids
    }
}

#[async_trait::async_trait]
impl AgentMcpToolProvider for McpService {
    async fn mcp_availability(&self, workspace_id: &str) -> Result<AgentMcpAvailability, String> {
        let state = self
            .workspace_mcp_tool_state(workspace_id, false, &[], &[])
            .await
            .map_err(|error| format!("{error:#}"))?;
        Ok(AgentMcpAvailability {
            available_mcp: state.available_mcp,
            blocked_mcp: state.blocked_mcp,
        })
    }

    async fn materialize_mcp_tools(
        &self,
        request: AgentMcpMaterializationRequest,
    ) -> Result<AgentMcpMaterialization, AgentMcpMaterializationError> {
        let mut projection = self.resolve_mcp_turn_projection(&request).await?;
        let executor: Arc<dyn pioneer_tools::McpToolExecutor> = Arc::new(self.clone());
        let descriptors = projection
            .tools
            .iter()
            .map(ResolvedMcpTurnTool::as_dynamic_descriptor)
            .collect::<Vec<_>>();
        let materialized = pioneer_tools::materialize_mcp_runtime_tools(&descriptors, executor);
        for excluded in &materialized.excluded_tools {
            projection
                .diagnostics
                .push(McpResolutionDiagnostic::selection(format!(
                    "MCP tool `{}/{}` excluded as `{}`: {}",
                    excluded.server_name,
                    excluded.raw_tool_name,
                    excluded.callable_name,
                    excluded.reason
                )));
        }
        if !materialized.excluded_tools.is_empty() {
            return Err(mcp_materialization_error(
                AgentMcpMaterializationFailureReason::ProjectionInvalid,
                "resolved MCP tools could not be materialized without omission",
                projection.diagnostics.as_slice(),
                projection.accepted_capabilities,
                projection.rejected_capabilities,
            ));
        }
        let persistence = persistence_request_from_projection(&projection).map_err(|message| {
            mcp_materialization_error(
                AgentMcpMaterializationFailureReason::ProjectionInvalid,
                message,
                projection.diagnostics.as_slice(),
                projection.accepted_capabilities.clone(),
                projection.rejected_capabilities.clone(),
            )
        })?;
        let mcp_bindings = materialized
            .bindings
            .iter()
            .map(|binding| pioneer_protocol::McpTurnBindingSummary {
                server_installation_id: binding.server_installation_id.clone(),
                server_name: binding.server_name.clone(),
                raw_tool_name: binding.raw_tool_name.clone(),
                callable_name: binding.callable_name.clone(),
                catalog_version: binding.catalog_version.clone(),
                fingerprint: binding.fingerprint.clone(),
                selection_reason: binding.selection_reason.clone(),
                capability_id: binding.capability_id.clone(),
            })
            .collect::<Vec<_>>();
        Ok(AgentMcpMaterialization {
            bundles: materialized.bundles,
            available_mcp: projection.available_mcp,
            blocked_mcp: projection.blocked_mcp,
            diagnostics: projection
                .diagnostics
                .into_iter()
                .map(|diagnostic| AgentMcpResolutionDiagnostic {
                    code: diagnostic.code.to_owned(),
                    message: diagnostic.message,
                })
                .collect(),
            accepted_capabilities: projection.accepted_capabilities,
            rejected_capabilities: projection.rejected_capabilities,
            mcp_bindings,
            persistence: Some(persistence),
        })
    }

    async fn persist_mcp_projection(
        &self,
        request: AgentMcpProjectionPersistenceRequest,
    ) -> Result<AgentMcpPersistedProjection, AgentMcpProjectionPersistenceError> {
        self.inner.projection_persistence.persist(&request).await
    }
}

#[async_trait::async_trait]
impl TurnMcpRuntimeView for McpService {
    async fn current_tool_identity(
        &self,
        workspace_id: &str,
        binding: &pioneer_crud::TurnMcpBindingRecord,
    ) -> Result<CurrentMcpToolIdentity, TurnMcpInvocationError> {
        if binding.server_installation_id == FIRST_PARTY_FILE_SERVER_ID {
            return first_party_file_tool_identity(workspace_id, binding);
        }
        let row = self
            .inner
            .crud_store
            .list_mcp_server_installations("workspace", workspace_id)
            .await
            .map_err(|_| {
                TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::Internal,
                    "failed to load the current MCP installation",
                )
            })?
            .into_iter()
            .find(|row| {
                row.id.as_deref() == Some(binding.server_installation_id.as_str())
                    && row.scope_key == workspace_id
            })
            .ok_or_else(|| {
                TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::InstallationUnavailable,
                    "frozen MCP installation is unavailable",
                )
            })?;
        if !row.enabled {
            return Err(TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::InstallationUnavailable,
                "frozen MCP installation is disabled",
            ));
        }

        let snapshot = self
            .inner
            .snapshots
            .lock()
            .await
            .get(binding.server_installation_id.as_str())
            .cloned()
            .filter(|snapshot| {
                snapshot.scope_kind == DomainScopeKind::Workspace
                    && snapshot.scope_key == workspace_id
            })
            .ok_or_else(|| {
                TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::RuntimeNotLive,
                    "frozen MCP runtime is unavailable",
                )
            })?;
        if !snapshot.state.live() {
            return Err(TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::RuntimeNotLive,
                "frozen MCP runtime is not live",
            ));
        }

        let catalog = self
            .inner
            .crud_store
            .find_mcp_server_catalog_snapshot(binding.server_installation_id.as_str())
            .await
            .map_err(|_| {
                TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::Internal,
                    "failed to load the current MCP catalog",
                )
            })?
            .ok_or_else(|| {
                TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::InstallationUnavailable,
                    "current MCP catalog is unavailable",
                )
            })?;
        let tool = parse_catalog_tools(catalog.tools_json.as_str())
            .into_iter()
            .find(|tool| tool.raw_tool_name == binding.raw_tool_name)
            .ok_or_else(|| {
                TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::ToolDrift,
                    "frozen MCP tool is absent from the current catalog",
                )
            })?;
        let (canonical_schema, canonical_schema_fingerprint, _) =
            canonical_schema_identity(&tool.parameters).map_err(|_| {
                TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::Internal,
                    "failed to canonicalize the current MCP schema",
                )
            })?;
        let (annotations_json, annotations_digest) =
            canonical_annotations_identity(&tool.annotations).map_err(|_| {
                TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::Internal,
                    "failed to canonicalize the current MCP annotations",
                )
            })?;
        let gateway_tool_timeout_ms = mcp_transport_tool_timeout_ms(row.transport_json.as_str())
            .map_err(|_| {
                TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::Internal,
                    "failed to read the current MCP runtime timeout",
                )
            })?;

        Ok(CurrentMcpToolIdentity {
            server_installation_id: binding.server_installation_id.clone(),
            server_name: row.name,
            raw_tool_name: tool.raw_tool_name,
            description: Some(tool.description),
            catalog_version: catalog.catalog_version,
            installation_fingerprint: row.fingerprint,
            canonical_schema_fingerprint,
            canonical_schema,
            annotations_json,
            annotations_digest,
            effective_timeout_ms: effective_mcp_tool_timeout_ms(
                tool.timeout_ms,
                gateway_tool_timeout_ms,
            ),
            runtime_generation: snapshot.runtime_generation,
        })
    }
}

fn first_party_file_tool_identity(
    workspace_id: &str,
    binding: &pioneer_crud::TurnMcpBindingRecord,
) -> Result<CurrentMcpToolIdentity, TurnMcpInvocationError> {
    // The first-party binding is a reserved capability, not an ordinary
    // database-backed MCP installation.  Do not let a stale or corrupted
    // durable row define its server/name/catalog identity and then validate
    // that self-consistent corruption as if it were the canonical facade.
    if binding.server_installation_id != FIRST_PARTY_FILE_SERVER_ID
        || binding.server_name != FIRST_PARTY_FILE_SERVER_NAME
        || binding.catalog_version != FIRST_PARTY_FILE_CATALOG
        || binding.fingerprint != FIRST_PARTY_FILE_SERVER_ID
        || binding.raw_tool_name != binding.canonical_callable_name
        || binding.annotations_json != "{}"
        || binding.annotations_digest != crate::turn_mcp::projection::sha256_hex(b"{}")
        || binding.runtime_generation <= 0
        || binding.effective_timeout_ms <= 0
    {
        return Err(TurnMcpInvocationError::new(
            TurnMcpInvocationErrorCode::ToolDrift,
            "reserved first-party file binding has a non-canonical identity",
        ));
    }
    let canonical_schema = match binding.canonical_callable_name.as_str() {
        FIRST_PARTY_READ_FILE => pioneer_provider::read_file_tool_schema()
            .get("input")
            .cloned()
            .ok_or_else(|| {
                TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::Internal,
                    "reserved read_file schema is unavailable",
                )
            })?,
        FIRST_PARTY_APPLY_PATCH => json!({
            "type": "object",
            "properties": {"patch": {"type": "string"}},
            "required": ["patch"],
            "additionalProperties": false
        }),
        _ => {
            return Err(TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::ToolUnbound,
                "unknown first-party file callable",
            ));
        }
    };
    let (canonical_schema, schema_fingerprint, _) =
        crate::turn_mcp::projection::canonical_schema_identity(&canonical_schema).map_err(
            |_| {
                TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::Internal,
                    "reserved file schema could not be canonicalized",
                )
            },
        )?;
    if schema_fingerprint != binding.canonical_schema_fingerprint || workspace_id.trim().is_empty()
    {
        return Err(TurnMcpInvocationError::new(
            TurnMcpInvocationErrorCode::ToolDrift,
            "reserved first-party file binding no longer matches its canonical schema",
        ));
    }
    let runtime_generation = u64::try_from(binding.runtime_generation).map_err(|_| {
        TurnMcpInvocationError::new(
            TurnMcpInvocationErrorCode::ToolDrift,
            "reserved first-party file runtime generation is invalid",
        )
    })?;
    let effective_timeout_ms = u64::try_from(binding.effective_timeout_ms).map_err(|_| {
        TurnMcpInvocationError::new(
            TurnMcpInvocationErrorCode::ToolDrift,
            "reserved first-party file timeout is invalid",
        )
    })?;
    Ok(CurrentMcpToolIdentity {
        server_installation_id: FIRST_PARTY_FILE_SERVER_ID.to_owned(),
        server_name: FIRST_PARTY_FILE_SERVER_NAME.to_owned(),
        raw_tool_name: binding.canonical_callable_name.clone(),
        description: Some(match binding.canonical_callable_name.as_str() {
            FIRST_PARTY_READ_FILE => {
                "Read a text file and return the exact version token used by Apply Patch."
                    .to_owned()
            }
            _ => "Apply one bounded text patch. The input must be the complete patch document."
                .to_owned(),
        }),
        catalog_version: FIRST_PARTY_FILE_CATALOG.to_owned(),
        installation_fingerprint: FIRST_PARTY_FILE_SERVER_ID.to_owned(),
        canonical_schema_fingerprint: schema_fingerprint,
        canonical_schema,
        annotations_json: binding.annotations_json.clone(),
        annotations_digest: binding.annotations_digest.clone(),
        effective_timeout_ms,
        runtime_generation,
    })
}

fn managed_history_coverage(value: &JsonValue) -> &'static str {
    match value
        .pointer("/tracking/status")
        .and_then(JsonValue::as_str)
    {
        Some("recorded_and_projected") => "engine_verified_steps",
        Some("recorded_projection_pending") | Some("pending") | Some("incomplete") => "incomplete",
        Some("not_applicable") => "not_applicable",
        _ => "incomplete",
    }
}

#[async_trait::async_trait]
impl TurnMcpValidatedExecution for McpService {
    async fn execute(
        &self,
        validated: ValidatedTurnMcpInvocation,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
        let _active_invocation = self.register_active_mcp_invocation(
            validated.invocation.turn_id.as_str(),
            validated.binding.server_installation_id.as_str(),
            validated.invocation.provider_call_id.as_str(),
            cancellation.clone(),
        );
        let result = async {
            let security = self
                .inner
                .crud_store
                .get_turn_execution_security_snapshot(validated.invocation.turn_id.as_str())
                .await
                .map_err(|_| {
                    TurnMcpInvocationError::new(
                        TurnMcpInvocationErrorCode::Internal,
                        "failed to load the turn execution security snapshot",
                    )
                })?
                .ok_or_else(|| {
                    TurnMcpInvocationError::new(
                        TurnMcpInvocationErrorCode::SecuritySnapshotUnavailable,
                        "turn execution security snapshot is unavailable",
                    )
                })?;
            if validated.binding.server_installation_id == FIRST_PARTY_FILE_SERVER_ID {
                return self
                    .execute_first_party_file_tool(&validated, &security, cancellation.clone())
                    .await;
            }
            let annotations = serde_json::from_str::<pioneer_tools::McpDynamicToolAnnotations>(
                validated.binding.annotations_json.as_str(),
            )
            .map_err(|_| {
                TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::ResultInvalid,
                    "frozen MCP annotations are invalid",
                )
            })?;
            let descriptor = pioneer_tools::McpDynamicToolDescriptor {
                callable_name: validated.binding.canonical_callable_name.clone(),
                workspace_id: validated.invocation.workspace_id.clone(),
                server_id: validated.binding.server_installation_id.clone(),
                server_name: validated.binding.server_name.clone(),
                raw_tool_name: validated.binding.raw_tool_name.clone(),
                catalog_version: validated.binding.catalog_version.clone(),
                fingerprint: validated.binding.fingerprint.clone(),
                snapshot_version: validated.current_tool.runtime_generation,
                description: format!(
                    "MCP server `{}` raw tool `{}`.",
                    validated.binding.server_name, validated.binding.raw_tool_name
                ),
                parameters: validated.current_tool.canonical_schema.clone(),
                annotations,
                timeout_ms: Some(validated.current_tool.effective_timeout_ms.max(1)),
                max_arguments_bytes: validated.invocation_limits.max_arguments_bytes,
                selection_reason: validated.binding.selection_reason.clone(),
                capability_id: validated.binding.capability_id.clone(),
            };
            let backend: Arc<dyn pioneer_tools::McpToolExecutor> = Arc::new(self.clone());
            let materialized = pioneer_tools::materialize_mcp_runtime_tools(&[descriptor], backend);
            if !materialized.excluded_tools.is_empty() || materialized.bundles.len() != 1 {
                return Err(TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::ResultInvalid,
                    "validated MCP tool could not be materialized for execution",
                ));
            }

            let permission_context = pioneer_tools::PermissionEvaluationContext::for_turn(
                validated.invocation.workspace_id.clone(),
                validated.invocation.thread_id.clone(),
                validated.invocation.turn_id.clone(),
                security.snapshot.permission_profile.clone(),
            );
            let workdir = std::path::PathBuf::from(security.snapshot.sandbox.cwd.clone());
            let tools = pioneer_tools::build_tools_with_environment_and_security_snapshot(
                workdir,
                validated.invocation.turn_id.clone(),
                permission_context,
                pioneer_tools::WebToolsConfig::default(),
                pioneer_tools::ComputerUseToolsConfig::default(),
                materialized.bundles,
                std::collections::BTreeMap::new(),
                Some(security.snapshot),
            )
            .map_err(|_| {
                TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::Internal,
                    "failed to build the shared MCP tool runtime",
                )
            })?
            .with_permission_approval_broker(
                self.inner.permission_approval_broker.read().await.clone(),
            );
            let arguments =
                serde_json::to_string(&validated.invocation.arguments).map_err(|_| {
                    TurnMcpInvocationError::new(
                        TurnMcpInvocationErrorCode::InvalidRequest,
                        "MCP invocation arguments are not serializable",
                    )
                })?;
            let call = tools
                .router
                .build_tool_call(pioneer_tools::RawToolCall {
                    call_id: validated.invocation.provider_call_id.clone(),
                    tool_name: validated.binding.canonical_callable_name.clone(),
                    arguments,
                })
                .map_err(map_shared_tool_error)?;
            let result = match tools
                .runtime
                .execute_tool_call_with_cancellation(call, cancellation)
                .await
            {
                Ok(result) => result,
                Err(pioneer_tools::ToolError::Rejected(message)) => {
                    return Ok(canonical_permission_denied_result(message.as_str()));
                }
                Err(error) => return Err(map_shared_tool_error(error)),
            };
            let projection = result.projection().ok_or_else(|| {
                TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::Internal,
                    "shared MCP tool result projection is unavailable",
                )
            })?;
            canonical_result_from_tool_output(&projection.llm_payload())
        }
        .await;
        self.audit_turn_mcp_invocation_outcome(&validated, &result)
            .await;
        result
    }
}

impl McpService {
    async fn execute_first_party_file_tool(
        &self,
        validated: &ValidatedTurnMcpInvocation,
        security: &pioneer_crud::TurnExecutionSecuritySnapshotRecord,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
        if cancellation.is_cancelled() {
            return Err(TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::Cancelled,
                "managed file invocation was cancelled before execution",
            ));
        }
        let tool_name = validated.invocation.canonical_callable_name.as_str();
        let payload = match tool_name {
            FIRST_PARTY_READ_FILE | FIRST_PARTY_APPLY_PATCH => {
                pioneer_tools::ToolPayload::Function {
                    arguments: validated.invocation.arguments.clone(),
                }
            }
            _ => {
                return Err(TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::ToolUnbound,
                    "unknown first-party file callable",
                ));
            }
        };
        let mut invocation = pioneer_tools::ToolInvocation {
            call_id: validated.invocation.provider_call_id.clone(),
            tool_name: tool_name.to_owned(),
            source: pioneer_tools::ToolCallSource::Model,
            payload,
            workdir: std::path::PathBuf::from(security.snapshot.sandbox.cwd.clone()),
            environment: std::collections::BTreeMap::new(),
            attempt_id: 1,
            idempotency_key: Some(validated.invocation.provider_call_id.clone()),
            recovery: pioneer_tools::ToolRecoveryMetadata::default(),
            permission_metadata: pioneer_tools::ToolPermissionMetadata::default(),
            execution_security_snapshot: Some(security.snapshot.clone()),
            apply_patch_preflight: None,
            cancellation: cancellation.clone(),
        };
        let trace = pioneer_tools::ToolEventBus::default().start_trace(
            validated.invocation.turn_id.clone(),
            validated.invocation.provider_call_id.clone(),
            tool_name.to_owned(),
        );
        let permission_context = pioneer_tools::PermissionEvaluationContext::for_turn(
            validated.invocation.workspace_id.clone(),
            validated.invocation.thread_id.clone(),
            validated.invocation.turn_id.clone(),
            security.snapshot.permission_profile.clone(),
        );
        let direct_orchestrator = pioneer_tools::ToolOrchestrator::with_approval_broker(
            pioneer_tools::OrchestratorPolicy::default(),
            self.inner.permission_approval_broker.read().await.clone(),
        );
        if let Err(error) = direct_orchestrator
            .authorize_direct_invocation(&mut invocation, &trace, &permission_context)
            .await
        {
            return match error {
                pioneer_tools::ToolError::Rejected(message)
                    if tool_name == FIRST_PARTY_APPLY_PATCH =>
                {
                    canonical_apply_patch_permission_denied_result(
                        message.as_str(),
                        security.snapshot.permission_profile.mode.as_str(),
                    )
                }
                pioneer_tools::ToolError::Rejected(message) => {
                    Ok(canonical_permission_denied_result(message.as_str()))
                }
                other => Err(map_shared_tool_error(other)),
            };
        }
        let patch_identity = if tool_name == FIRST_PARTY_APPLY_PATCH {
            Some(
                InvocationIdentity::new(
                    validated.invocation.thread_id.clone(),
                    validated.invocation.turn_id.clone(),
                    validated.invocation.provider_call_id.clone(),
                )
                .map_err(|error| {
                    TurnMcpInvocationError::new(
                        TurnMcpInvocationErrorCode::InvalidRequest,
                        format!("managed patch identity is invalid: {error}"),
                    )
                })?,
            )
        } else {
            None
        };
        let history_observer = if let Some(identity) = patch_identity.as_ref() {
            let admission_permit = match reserve_patch_history_capacity().await {
                Ok(permit) => permit,
                Err(error) => {
                    tracing::warn!(
                        error = %format!("{error:#}"),
                        "managed patch history admission capacity is unavailable"
                    );
                    return canonical_apply_patch_history_capacity_result(
                        security.snapshot.permission_profile.mode.as_str(),
                    );
                }
            };
            Some(SqlitePatchObserver::new(
                self.inner.crud_store.database_connection(),
                identity.clone(),
                admission_permit,
            ))
        } else {
            None
        };
        let output_result = if tool_name == FIRST_PARTY_READ_FILE {
            pioneer_tools::handlers::ReadFileHandler
                .handle(invocation, trace)
                .await
        } else {
            let identity = patch_identity
                .as_ref()
                .expect("managed apply_patch identity should be present");
            let observer = history_observer
                .as_ref()
                .expect("managed apply_patch observer should be present");
            pioneer_tools::handlers::ApplyPatchHandler
                .handle_with_source_and_observer(
                    invocation,
                    trace,
                    pioneer_tools::apply_patch::file_mutation::PatchRequestSource::ManagedClaude,
                    identity,
                    observer,
                )
                .await
        };
        let output = match output_result {
            Ok(output) => output,
            Err(error) => return Err(map_shared_tool_error(error)),
        };
        let is_error = !output.success();
        let structured_content = output.raw_json();
        let history_coverage = managed_history_coverage(&structured_content);
        Ok(CanonicalMcpToolResult {
            content: json!([{"type": "text", "text": output.raw_text()}]),
            structured_content: Some(structured_content),
            is_error,
            duration_ms: 0,
            meta: Some(json!({
                "pioneer": {
                    "authority": "managed_claude_patch_engine",
                    "historyCoverage": history_coverage,
                    "canonicalTool": tool_name,
                }
            })),
        })
    }
}

impl McpService {
    async fn audit_turn_mcp_invocation_outcome(
        &self,
        validated: &ValidatedTurnMcpInvocation,
        result: &Result<CanonicalMcpToolResult, TurnMcpInvocationError>,
    ) {
        let (decision, reason_code, is_error, duration_ms) = match result {
            Ok(result) if result.is_error => {
                let reason_code = result
                    .meta
                    .as_ref()
                    .and_then(|meta| meta.pointer("/pioneer/reasonCode"))
                    .and_then(JsonValue::as_str)
                    .unwrap_or("upstream_error");
                let decision =
                    if reason_code == TurnMcpInvocationErrorCode::PermissionDenied.as_str() {
                        "blocked"
                    } else if reason_code == "history_capacity" {
                        "failed"
                    } else {
                        "allowed"
                    };
                (
                    decision,
                    Some(reason_code.to_owned()),
                    true,
                    Some(result.duration_ms),
                )
            }
            Ok(result) => ("allowed", None, false, Some(result.duration_ms)),
            Err(error) => (
                if matches!(
                    error.code,
                    TurnMcpInvocationErrorCode::PermissionDenied
                        | TurnMcpInvocationErrorCode::Cancelled
                ) {
                    "blocked"
                } else {
                    "failed"
                },
                Some(error.reason_code().to_owned()),
                true,
                None,
            ),
        };
        let record = McpAuditEventRecord {
            turn_id: Some(validated.invocation.turn_id.clone()),
            server_installation_id: Some(validated.binding.server_installation_id.clone()),
            server_name: validated.binding.server_name.clone(),
            raw_tool_name: Some(validated.binding.raw_tool_name.clone()),
            callable_name: Some(validated.binding.canonical_callable_name.clone()),
            catalog_version: Some(validated.binding.catalog_version.clone()),
            action: "turn_mcp_invocation_outcome".to_owned(),
            decision: decision.to_owned(),
            reason_code,
            details_json: json!({
                "workspace_id": validated.invocation.workspace_id.as_str(),
                "thread_id": validated.invocation.thread_id.as_str(),
                "runtime_id": validated.invocation.runtime_id.as_deref(),
                "session_generation": validated.invocation.session_generation,
                "origin": validated.invocation.origin.as_str(),
                "provider_call_id": validated.invocation.provider_call_id.as_str(),
                "manifest_hash": validated.manifest_hash.as_str(),
                "canonical_schema_fingerprint": validated.binding.canonical_schema_fingerprint.as_str(),
                "provider_schema_fingerprint": validated.binding.provider_schema_fingerprint.as_str(),
                "annotations_digest": validated.binding.annotations_digest.as_str(),
                "runtime_generation": validated.binding.runtime_generation,
                "mcp_resource_profile_version": validated.invocation_limits.profile_version,
                "is_error": is_error,
                "duration_ms": duration_ms,
            })
            .to_string(),
            created_at_unix: now_timestamp_secs(),
        };
        if let Err(error) = self
            .inner
            .crud_store
            .insert_mcp_audit_event_record(&record)
            .await
        {
            warn!(
                turn_id = validated.invocation.turn_id,
                provider_call_id = validated.invocation.provider_call_id,
                error = %format!("{error:#}"),
                "failed to write MCP invocation outcome audit"
            );
        }
    }
}

fn map_shared_tool_error(error: pioneer_tools::ToolError) -> TurnMcpInvocationError {
    use pioneer_tools::ToolError;

    let (code, message) = match error {
        ToolError::InvalidArguments(_) => (
            TurnMcpInvocationErrorCode::InvalidRequest,
            "MCP invocation arguments are invalid".to_owned(),
        ),
        ToolError::Cancelled(_) => (
            TurnMcpInvocationErrorCode::Cancelled,
            "MCP invocation was cancelled".to_owned(),
        ),
        ToolError::Rejected(message) => (
            TurnMcpInvocationErrorCode::PermissionDenied,
            bounded_invocation_diagnostic(message.as_str()),
        ),
        ToolError::NotFound(_) | ToolError::NotVisible(_) => (
            TurnMcpInvocationErrorCode::ResultInvalid,
            "validated MCP tool is unavailable in the shared runtime".to_owned(),
        ),
        ToolError::ExecutionFailed(message) if message == "MCP tool request timed out" => (
            TurnMcpInvocationErrorCode::TimedOut,
            "MCP tool request timed out".to_owned(),
        ),
        ToolError::ExecutionFailed(_) => (
            TurnMcpInvocationErrorCode::ExecutionFailed,
            "MCP tool execution failed".to_owned(),
        ),
        ToolError::Internal(_) => (
            TurnMcpInvocationErrorCode::Internal,
            "shared MCP tool execution failed internally".to_owned(),
        ),
    };
    TurnMcpInvocationError::new(code, message)
}

fn canonical_permission_denied_result(message: &str) -> CanonicalMcpToolResult {
    let message = bounded_invocation_diagnostic(message);
    CanonicalMcpToolResult {
        content: json!([{
            "type": "text",
            "text": message,
        }]),
        structured_content: None,
        is_error: true,
        duration_ms: 0,
        meta: Some(json!({
            "pioneer": {
                "reasonCode": TurnMcpInvocationErrorCode::PermissionDenied.as_str(),
            }
        })),
    }
}

fn canonical_apply_patch_permission_denied_result(
    message: &str,
    profile: &'static str,
) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
    canonical_apply_patch_rejection_result(
        pioneer_tools::apply_patch::file_mutation::PatchStage::Authorize,
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::PermissionDenied,
        bounded_invocation_diagnostic(message),
        TurnMcpInvocationErrorCode::PermissionDenied.as_str(),
        profile,
    )
}

fn canonical_apply_patch_history_capacity_result(
    profile: &'static str,
) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
    canonical_apply_patch_rejection_result(
        pioneer_tools::apply_patch::file_mutation::PatchStage::Record,
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::HistoryCapacity,
        "patch history capacity is unavailable; retry after a delay".to_owned(),
        "history_capacity",
        profile,
    )
}

fn canonical_apply_patch_rejection_result(
    stage: pioneer_tools::apply_patch::file_mutation::PatchStage,
    code: pioneer_tools::apply_patch::file_mutation::PatchErrorCode,
    message: String,
    reason_code: &str,
    profile: &'static str,
) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
    let output = pioneer_tools::handlers::ApplyPatchHandler::canonical_rejection_output(
        pioneer_tools::apply_patch::file_mutation::PatchRequestSource::ManagedClaude,
        profile,
        stage,
        code,
        message,
        None,
    )
    .map_err(map_shared_tool_error)?;
    let structured_content = output.raw_json();
    Ok(CanonicalMcpToolResult {
        content: json!([{"type": "text", "text": output.raw_text()}]),
        structured_content: Some(structured_content.clone()),
        is_error: true,
        duration_ms: 0,
        meta: Some(json!({
            "pioneer": {
                "authority": "managed_claude_patch_engine",
                "historyCoverage": managed_history_coverage(&structured_content),
                "canonicalTool": FIRST_PARTY_APPLY_PATCH,
                "reasonCode": reason_code,
            }
        })),
    })
}

fn canonical_result_from_tool_output(
    raw: &JsonValue,
) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
    let object = raw.as_object().ok_or_else(|| {
        TurnMcpInvocationError::new(
            TurnMcpInvocationErrorCode::ResultInvalid,
            "shared MCP tool returned a non-object result",
        )
    })?;
    let content = object.get("content").cloned().ok_or_else(|| {
        TurnMcpInvocationError::new(
            TurnMcpInvocationErrorCode::ResultInvalid,
            "shared MCP tool result is missing content",
        )
    })?;
    if !content.is_array() && !content.is_string() {
        return Err(TurnMcpInvocationError::new(
            TurnMcpInvocationErrorCode::ResultInvalid,
            "shared MCP tool result content has an unsupported shape",
        ));
    }
    let is_error = object
        .get("isError")
        .and_then(JsonValue::as_bool)
        .ok_or_else(|| {
            TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::ResultInvalid,
                "shared MCP tool result is missing isError",
            )
        })?;
    let duration_ms = object
        .get("durationMs")
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::ResultInvalid,
                "shared MCP tool result is missing durationMs",
            )
        })?;

    Ok(CanonicalMcpToolResult {
        content,
        structured_content: object
            .get("structuredContent")
            .filter(|value| !value.is_null())
            .cloned(),
        is_error,
        duration_ms,
        meta: object.get("meta").filter(|value| !value.is_null()).cloned(),
    })
}

fn bounded_invocation_diagnostic(message: &str) -> String {
    const MAX_DIAGNOSTIC_CHARS: usize = 512;
    let mut characters = message.chars();
    let bounded = characters
        .by_ref()
        .take(MAX_DIAGNOSTIC_CHARS)
        .collect::<String>();
    if characters.next().is_some() {
        format!("{bounded}…")
    } else {
        bounded
    }
}

#[async_trait::async_trait]
impl pioneer_tools::McpToolExecutor for McpService {
    async fn call_mcp_tool(
        &self,
        request: pioneer_tools::McpToolCallRequest,
        trace: pioneer_tools::ToolEventTrace,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<pioneer_tools::McpToolCallOutput, pioneer_tools::ToolError> {
        let _active_invocation = self.register_active_mcp_invocation(
            request.turn_id.as_str(),
            request.server_id.as_str(),
            request.call_id.as_str(),
            cancellation.clone(),
        );
        trace.emit_stage(
            1,
            "mcp.call.started",
            None,
            Some(json!({
                "server_id": request.server_id.as_str(),
                "server_name": request.server_name.as_str(),
                "raw_tool_name": request.raw_tool_name.as_str(),
                "catalog_version": request.catalog_version.as_str(),
            })),
        );
        let result = self.call_tool(request, cancellation).await;
        match &result {
            Ok(output) => trace.emit_stage(
                1,
                "mcp.call.completed",
                None,
                Some(json!({
                    "duration_ms": output.duration_ms,
                    "is_error": output.is_error,
                })),
            ),
            Err(error) => trace.emit_stage(1, "mcp.call.failed", Some(error.to_string()), None),
        }
        result
    }
}

pub(crate) fn installation_from_record(
    row: &McpServerInstallationRecord,
) -> Result<McpServerInstallation> {
    Ok(McpServerInstallation {
        scope_kind: DomainScopeKind::from_str(row.scope_kind.as_str())
            .map_err(anyhow::Error::msg)?,
        scope_key: row.scope_key.clone(),
        name: row.name.clone(),
        display_name: row.display_name.clone(),
        source_kind: DomainSourceKind::from_str(row.source_kind.as_str())
            .map_err(anyhow::Error::msg)?,
        source_ref: serde_json::from_str(row.source_ref.as_str())
            .context("failed to decode MCP source_ref")?,
        transport: serde_json::from_str::<McpTransportConfig>(row.transport_json.as_str())
            .context("failed to decode MCP transport")?,
        auth: serde_json::from_str::<McpAuthConfig>(row.auth_json.as_str())
            .context("failed to decode MCP auth config")?,
        secret_refs: serde_json::from_str::<Vec<McpSecretRef>>(row.secret_refs_json.as_str())
            .context("failed to decode MCP secret refs")?,
        enabled: row.enabled,
        allow_implicit_invocation: row.allow_implicit_invocation,
        required: row.required,
        fingerprint: row.fingerprint.clone(),
    })
}

fn protocol_scope_kind(scope_kind: DomainScopeKind) -> McpScopeKind {
    match scope_kind {
        DomainScopeKind::Workspace => McpScopeKind::Workspace,
        DomainScopeKind::User => McpScopeKind::User,
    }
}

fn protocol_runtime_state(state: DomainRuntimeState) -> McpRuntimeState {
    match state {
        DomainRuntimeState::NotStarted => McpRuntimeState::NotStarted,
        DomainRuntimeState::Disabled => McpRuntimeState::Disabled,
        DomainRuntimeState::Starting => McpRuntimeState::Starting,
        DomainRuntimeState::Ready => McpRuntimeState::Ready,
        DomainRuntimeState::Degraded => McpRuntimeState::Degraded,
        DomainRuntimeState::AuthRequired => McpRuntimeState::AuthRequired,
        DomainRuntimeState::Failed => McpRuntimeState::Failed,
        DomainRuntimeState::Stopping => McpRuntimeState::Stopping,
        DomainRuntimeState::Stopped => McpRuntimeState::Stopped,
        DomainRuntimeState::Restarting => McpRuntimeState::Restarting,
    }
}

fn mcp_ref_key(value: &str) -> String {
    value.trim().to_owned()
}

fn eligible_workspace_mcp_server_ref(reference: &AgentMcpServerRef) -> bool {
    reference.scope_kind == McpScopeKind::Workspace
        && !reference.name.trim().is_empty()
        && reference.capability_id
            == pioneer_protocol::mcp_server_capability_key(reference.scope_kind, &reference.name)
}

fn eligible_workspace_mcp_tool_ref(reference: &AgentMcpToolRef) -> bool {
    reference.scope_kind == McpScopeKind::Workspace
        && !reference.server_name.trim().is_empty()
        && !reference.raw_tool_name.trim().is_empty()
        && reference.capability_id
            == pioneer_protocol::mcp_tool_capability_key(
                reference.scope_kind,
                &reference.server_name,
                &reference.raw_tool_name,
            )
}

fn mcp_server_capability_kind(reference: &AgentMcpServerRef) -> TurnCapabilityKind {
    TurnCapabilityKind::McpServer {
        name: reference.name.clone(),
        scope_kind: reference.scope_kind,
    }
}

fn mcp_tool_capability_kind(reference: &AgentMcpToolRef) -> TurnCapabilityKind {
    TurnCapabilityKind::McpTool {
        server_name: reference.server_name.clone(),
        raw_tool_name: reference.raw_tool_name.clone(),
        scope_kind: reference.scope_kind,
    }
}

fn accept_mcp_server_capability(reference: &AgentMcpServerRef) -> TurnAcceptedCapability {
    TurnAcceptedCapability {
        id: reference.capability_id.clone(),
        label: reference.label.clone(),
        kind: mcp_server_capability_kind(reference),
        reason: TurnCapabilityAcceptedReason::ExplicitComposerCapability,
    }
}

fn accept_mcp_tool_capability(reference: &AgentMcpToolRef) -> TurnAcceptedCapability {
    TurnAcceptedCapability {
        id: reference.capability_id.clone(),
        label: reference.label.clone(),
        kind: mcp_tool_capability_kind(reference),
        reason: TurnCapabilityAcceptedReason::ExplicitComposerCapability,
    }
}

fn reject_mcp_server_capability(
    reference: &AgentMcpServerRef,
    reason: TurnCapabilityRejectedReason,
    message: &str,
) -> TurnRejectedCapability {
    TurnRejectedCapability {
        id: reference.capability_id.clone(),
        label: reference.label.clone(),
        kind: mcp_server_capability_kind(reference),
        reason,
        message: message.to_owned(),
    }
}

fn reject_mcp_tool_capability(
    reference: &AgentMcpToolRef,
    reason: TurnCapabilityRejectedReason,
    message: &str,
) -> TurnRejectedCapability {
    TurnRejectedCapability {
        id: reference.capability_id.clone(),
        label: reference.label.clone(),
        kind: mcp_tool_capability_kind(reference),
        reason,
        message: message.to_owned(),
    }
}

fn reject_mcp_refs_for_server(
    state: &mut WorkspaceMcpToolState,
    server_refs: &[&AgentMcpServerRef],
    tool_refs: &[&AgentMcpToolRef],
    matched_server_capability_ids: &mut HashSet<String>,
    matched_tool_capability_ids: &mut HashSet<String>,
    reason: TurnCapabilityRejectedReason,
    message: &str,
) {
    for reference in server_refs {
        matched_server_capability_ids.insert(reference.capability_id.clone());
        state
            .rejected_capabilities
            .push(reject_mcp_server_capability(reference, reason, message));
    }
    for reference in tool_refs {
        matched_tool_capability_ids.insert(reference.capability_id.clone());
        state
            .rejected_capabilities
            .push(reject_mcp_tool_capability(reference, reason, message));
    }
}

fn reject_explicit_mcp_refs_for_uncertainty(
    request: &AgentMcpMaterializationRequest,
    message: &str,
) -> Vec<TurnRejectedCapability> {
    let mut rejected = request
        .explicit_servers
        .iter()
        .map(|reference| {
            reject_mcp_server_capability(
                reference,
                TurnCapabilityRejectedReason::Unavailable,
                message,
            )
        })
        .collect::<Vec<_>>();
    rejected.extend(request.explicit_tools.iter().map(|reference| {
        reject_mcp_tool_capability(
            reference,
            TurnCapabilityRejectedReason::Unavailable,
            message,
        )
    }));
    rejected
}

fn record_unavailable_installation(
    state: &mut WorkspaceMcpToolState,
    installation: &McpServerInstallationRecord,
    resolution_relevant: bool,
    message: impl Into<String>,
) {
    if !resolution_relevant {
        return;
    }
    let diagnostic = McpResolutionDiagnostic::installation_unavailable(message);
    if installation.required {
        state.required_unavailable.push(diagnostic.clone());
    }
    state.diagnostics.push(diagnostic);
}

fn select_mcp_tool(current: &mut Option<McpToolSelection>, candidate: McpToolSelection) {
    if current
        .as_ref()
        .is_none_or(|selected| candidate.reason.priority() > selected.reason.priority())
    {
        *current = Some(candidate);
    }
}

fn domain_runtime_state_code(state: DomainRuntimeState) -> &'static str {
    match state {
        DomainRuntimeState::NotStarted => "not_started",
        DomainRuntimeState::Disabled => "disabled",
        DomainRuntimeState::Starting => "starting",
        DomainRuntimeState::Ready => "ready",
        DomainRuntimeState::Degraded => "degraded",
        DomainRuntimeState::AuthRequired => "auth_required",
        DomainRuntimeState::Failed => "failed",
        DomainRuntimeState::Stopping => "stopping",
        DomainRuntimeState::Stopped => "stopped",
        DomainRuntimeState::Restarting => "restarting",
    }
}

fn enrich_tool_output_meta(
    meta: Option<JsonValue>,
    runtime_state: &str,
    catalog_drift: bool,
    current_catalog_version: &str,
) -> JsonValue {
    let mut object = match meta {
        Some(JsonValue::Object(object)) => object,
        Some(value) => {
            let mut object = serde_json::Map::new();
            object.insert("server_meta".to_owned(), value);
            object
        }
        None => serde_json::Map::new(),
    };
    object.insert(
        "pioneer".to_owned(),
        json!({
            "runtime_state": runtime_state,
            "runtimeState": runtime_state,
            "catalog_drift": catalog_drift,
            "catalogDrift": catalog_drift,
            "current_catalog_version": current_catalog_version,
            "currentCatalogVersion": current_catalog_version,
        }),
    );
    JsonValue::Object(object)
}

struct CatalogTool {
    raw_tool_name: String,
    description: String,
    parameters: JsonValue,
    annotations: pioneer_tools::McpDynamicToolAnnotations,
    timeout_ms: Option<u64>,
}

fn mcp_transport_tool_timeout_ms(transport_json: &str) -> Result<u64> {
    serde_json::from_str::<McpTransportConfig>(transport_json)
        .context("failed to decode MCP transport configuration")
        .map(|transport| transport.tool_timeout_ms())
}

fn effective_mcp_tool_timeout_ms(
    catalog_tool_timeout_ms: Option<u64>,
    gateway_maximum_ms: u64,
) -> u64 {
    catalog_tool_timeout_ms
        .unwrap_or(DEFAULT_MCP_TURN_TOOL_TIMEOUT_MS)
        .max(1)
        .min(gateway_maximum_ms.max(1))
}

fn parse_catalog_tools(tools_json: &str) -> Vec<CatalogTool> {
    let tools = serde_json::from_str::<Vec<JsonValue>>(tools_json).unwrap_or_default();
    tools
        .into_iter()
        .filter_map(|tool| {
            let raw_tool_name = tool
                .get("name")
                .and_then(JsonValue::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())?
                .to_owned();
            let description = tool
                .get("description")
                .and_then(JsonValue::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("MCP runtime tool")
                .to_owned();
            let parameters = tool
                .get("inputSchema")
                .or_else(|| tool.get("input_schema"))
                .cloned()
                .filter(JsonValue::is_object)
                .unwrap_or_else(|| {
                    json!({
                        "type": "object",
                        "additionalProperties": true
                    })
                });
            let annotations = tool
                .get("annotations")
                .cloned()
                .and_then(|value| {
                    serde_json::from_value::<pioneer_tools::McpDynamicToolAnnotations>(value).ok()
                })
                .unwrap_or_default();
            let timeout_ms = tool
                .get("_meta")
                .and_then(|meta| meta.get("pioneer"))
                .and_then(|pioneer| pioneer.get("timeoutMs"))
                .and_then(JsonValue::as_u64);
            Some(CatalogTool {
                raw_tool_name,
                description,
                parameters,
                annotations,
                timeout_ms,
            })
        })
        .collect()
}

fn json_object_keys(value: &JsonValue) -> Vec<String> {
    let mut keys = value
        .as_object()
        .map(|map| map.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    keys.sort();
    keys
}

fn runtime_generation_seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX - 1))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthenticatedSessionPrincipal;
    use crate::authorization::ExecutionAuthorizationContext;
    use crate::bootstrap::bootstrap;
    use crate::session::test_support::{
        authenticated_test_superuser, ensure_test_superuser_execution_authority,
    };
    use crate::turn_mcp::invoker::{
        GatewayTurnMcpInvoker, TurnMcpInvocation, TurnMcpInvocationError,
        TurnMcpInvocationErrorCode, TurnMcpInvocationOrigin, TurnMcpInvoker,
        TurnMcpValidatedExecution, ValidatedTurnMcpInvocation,
    };
    use crate::turn_mcp::result::CanonicalMcpToolResult;
    use crate::workspace::DEFAULT_WORKSPACE_ID;
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{
        CliRuntimeTurnMcpMetadata, NewCliRuntimeTurnBinding, NewTaskExecutionAdmission,
    };
    use pioneer_entity::turn;
    use pioneer_keystore::MemorySecretStore;
    use pioneer_protocol::{
        AuthSessionId, DeviceId, GatewayId, PermissionBehavior, PrincipalId, PrincipalKind,
        RoleKey, SandboxMode, Task, TaskEventPayload, TaskExecutorKind, TaskOwnerKind, TaskRun,
        TaskRunStatus, TaskStatus, Thread, ThreadLineage, ThreadMode, ThreadOriginKind,
        ThreadSidebarVisibility, ThreadStatus, Turn, TurnExecutionSecuritySnapshot,
        TurnNetworkPolicySnapshot, TurnPermissionMode, TurnPermissionProfileSnapshot,
        TurnPermissionProfileSource, TurnStatus,
    };
    use sea_orm::{ConnectionTrait, Database, EntityTrait};
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicUsize;
    use tokio_util::sync::CancellationToken;

    #[derive(Default)]
    struct TestValidatedMcpExecution {
        calls: AtomicUsize,
    }

    struct CountingPermissionApprovalBroker {
        calls: AtomicUsize,
        resolution: pioneer_tools::PermissionApprovalResolution,
    }

    #[derive(Default)]
    struct BlockingPermissionApprovalBroker {
        calls: AtomicUsize,
        started: tokio::sync::Notify,
    }

    impl CountingPermissionApprovalBroker {
        fn new(resolution: pioneer_tools::PermissionApprovalResolution) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                resolution,
            }
        }
    }

    #[async_trait::async_trait]
    impl pioneer_tools::PermissionApprovalBroker for CountingPermissionApprovalBroker {
        async fn request_approval(
            &self,
            _context: &pioneer_tools::PermissionEvaluationContext,
            _invocation: &pioneer_tools::ToolInvocation,
            _intent: &pioneer_tools::PermissionIntent,
            _key: &pioneer_tools::PermissionRequestKey,
            _reason: pioneer_tools::PermissionDecisionReason,
        ) -> pioneer_tools::PermissionApprovalResolution {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.resolution.clone()
        }
    }

    #[async_trait::async_trait]
    impl pioneer_tools::PermissionApprovalBroker for BlockingPermissionApprovalBroker {
        async fn request_approval(
            &self,
            _context: &pioneer_tools::PermissionEvaluationContext,
            _invocation: &pioneer_tools::ToolInvocation,
            _intent: &pioneer_tools::PermissionIntent,
            _key: &pioneer_tools::PermissionRequestKey,
            _reason: pioneer_tools::PermissionDecisionReason,
        ) -> pioneer_tools::PermissionApprovalResolution {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            std::future::pending::<pioneer_tools::PermissionApprovalResolution>().await
        }
    }

    #[async_trait::async_trait]
    impl TurnMcpValidatedExecution for TestValidatedMcpExecution {
        async fn execute(
            &self,
            validated: ValidatedTurnMcpInvocation,
            cancellation: tokio_util::sync::CancellationToken,
        ) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
            assert!(!cancellation.is_cancelled());
            assert_eq!(
                validated.binding.canonical_callable_name,
                validated.invocation.canonical_callable_name
            );
            assert_eq!(
                validated.current_tool.runtime_generation,
                u64::try_from(validated.binding.runtime_generation).unwrap()
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(CanonicalMcpToolResult {
                content: json!([{"type": "text", "text": "validated"}]),
                structured_content: None,
                is_error: false,
                duration_ms: 1,
                meta: None,
            })
        }
    }

    struct TestTurnMcpInvokerFixture {
        service: McpService,
        crud_store: Arc<CrudStore>,
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        callable_name: String,
        manifest_hash: String,
        execution: Arc<TestValidatedMcpExecution>,
        invoker: GatewayTurnMcpInvoker,
    }

    struct TestMcpRuntimeConnector {
        tools: Vec<&'static str>,
        fail_auth: bool,
    }

    #[async_trait::async_trait]
    impl McpRuntimeConnector for TestMcpRuntimeConnector {
        async fn connect(
            &self,
            _installation: McpServerInstallation,
            installation_id: String,
            _resolver: Arc<dyn McpSecretResolver>,
            now_unix: i64,
        ) -> Result<Box<dyn pioneer_mcp::McpRuntimeSession>, McpRuntimeError> {
            if self.fail_auth {
                return Err(McpRuntimeError::auth_required("missing test secret"));
            }

            let tools = self
                .tools
                .iter()
                .map(|name| json!({ "name": name }))
                .collect::<Vec<_>>();
            let catalog = McpCatalogSnapshot::from_json_values(
                installation_id,
                json!({"name":"test-mcp","version":"test"}),
                None,
                json!(tools),
                json!([]),
                json!([]),
                json!([]),
                now_unix,
            )
            .expect("test MCP catalog should build");
            Ok(Box::new(TestMcpRuntimeSession { catalog }))
        }
    }

    struct TestMcpRuntimeSession {
        catalog: McpCatalogSnapshot,
    }

    #[derive(Default)]
    struct PausingMcpControl {
        calls: AtomicUsize,
        started: tokio::sync::Notify,
        cancelled: tokio::sync::Notify,
        released: tokio::sync::Notify,
    }

    struct PausingMcpRuntimeConnector {
        control: Arc<PausingMcpControl>,
        catalog_timeout_ms: u64,
    }

    struct PausingMcpRuntimeSession {
        catalog: McpCatalogSnapshot,
        control: Arc<PausingMcpControl>,
    }

    #[async_trait::async_trait]
    impl McpRuntimeConnector for PausingMcpRuntimeConnector {
        async fn connect(
            &self,
            _installation: McpServerInstallation,
            installation_id: String,
            _resolver: Arc<dyn McpSecretResolver>,
            now_unix: i64,
        ) -> Result<Box<dyn pioneer_mcp::McpRuntimeSession>, McpRuntimeError> {
            let catalog = McpCatalogSnapshot::from_json_values(
                installation_id,
                json!({"name":"pausing-test-mcp","version":"test"}),
                None,
                json!([{
                    "name": "send",
                    "inputSchema": {
                        "type": "object",
                        "additionalProperties": true
                    },
                    "_meta": {
                        "pioneer": {
                            "timeoutMs": self.catalog_timeout_ms
                        }
                    }
                }]),
                json!([]),
                json!([]),
                json!([]),
                now_unix,
            )
            .expect("pausing test MCP catalog should build");
            Ok(Box::new(PausingMcpRuntimeSession {
                catalog,
                control: self.control.clone(),
            }))
        }
    }

    #[async_trait::async_trait]
    impl pioneer_mcp::McpRuntimeSession for TestMcpRuntimeSession {
        fn initial_catalog(&self) -> &McpCatalogSnapshot {
            &self.catalog
        }

        async fn wait_for_event(&mut self) -> pioneer_mcp::McpSessionEvent {
            std::future::pending::<pioneer_mcp::McpSessionEvent>().await
        }

        async fn refresh_catalog(&mut self) -> Result<McpCatalogSnapshot, McpRuntimeError> {
            Ok(self.catalog.clone())
        }

        async fn call_tool(
            &mut self,
            raw_tool_name: &str,
            arguments: JsonValue,
            _budget: pioneer_mcp::McpInvocationBudget,
            _timeout: Duration,
            _cancellation: CancellationToken,
        ) -> Result<McpToolCallResult, McpRuntimeError> {
            Ok(McpToolCallResult {
                content: json!([{"type":"text","text":format!("called {raw_tool_name}")}]),
                structured_content: Some(json!({
                    "tool": raw_tool_name,
                    "arguments": arguments,
                })),
                is_error: false,
                duration_ms: 1,
                meta: None,
            })
        }

        async fn shutdown(&mut self) {}
        async fn shutdown_result(&mut self) -> Result<(), pioneer_mcp::McpRuntimeError> {
            self.shutdown().await;
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl pioneer_mcp::McpRuntimeSession for PausingMcpRuntimeSession {
        fn initial_catalog(&self) -> &McpCatalogSnapshot {
            &self.catalog
        }

        async fn wait_for_event(&mut self) -> pioneer_mcp::McpSessionEvent {
            std::future::pending::<pioneer_mcp::McpSessionEvent>().await
        }

        async fn refresh_catalog(&mut self) -> Result<McpCatalogSnapshot, McpRuntimeError> {
            Ok(self.catalog.clone())
        }

        async fn call_tool(
            &mut self,
            raw_tool_name: &str,
            arguments: JsonValue,
            _budget: pioneer_mcp::McpInvocationBudget,
            _timeout: Duration,
            cancellation: tokio_util::sync::CancellationToken,
        ) -> Result<McpToolCallResult, McpRuntimeError> {
            self.control.calls.fetch_add(1, Ordering::SeqCst);
            self.control.started.notify_one();
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    self.control.cancelled.notify_one();
                    Err(McpRuntimeError::cancelled(
                        "pausing test MCP request was cancelled",
                    ))
                }
                _ = self.control.released.notified() => {
                    Ok(McpToolCallResult {
                        content: json!([{
                            "type": "text",
                            "text": format!("late release of {raw_tool_name}"),
                        }]),
                        structured_content: Some(json!({
                            "tool": raw_tool_name,
                            "arguments": arguments,
                        })),
                        is_error: false,
                        duration_ms: 1,
                        meta: None,
                    })
                }
            }
        }

        async fn shutdown(&mut self) {}
        async fn shutdown_result(&mut self) -> Result<(), pioneer_mcp::McpRuntimeError> {
            self.shutdown().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn cancelled_stop_retains_native_handle_until_a_retry_observes_completion() {
        let (service, _, workspace) = test_mcp_service().await;
        let id = "stopping-native-installation";
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (call_tx, _call_rx) = mpsc::channel(1);
        let completed = Arc::new(CancellationToken::new());
        let completion = McpTaskCompletion(completed.clone());
        let (entered, stopping) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let join = tokio::spawn(async move {
            let _completion = completion;
            shutdown_rx.await.expect("native shutdown request");
            entered.send(()).expect("shutdown observer");
            released.await.expect("shutdown completion release");
            Ok(())
        });
        let join = McpTaskJoin::new(join);
        service.inner.tasks.lock().await.insert(
            id.into(),
            McpServerTaskHandle {
                name: "stopping-native".into(),
                scope_kind: "workspace".into(),
                scope_key: workspace,
                fingerprint: "configuration".into(),
                effective_secret_fingerprint: "material".into(),
                call_tx,
                oauth_failure_revision: Arc::new(AtomicU64::new(0)),
                recovery: Arc::new(OAuthRecoveryMailbox::default()),
                shutdown_tx: Some(shutdown_tx),
                completed,
                join,
                session: Arc::new(Mutex::new(None)),
            },
        );
        let native = service.clone();
        let first = tokio::spawn(async move {
            native
                .stop_task_result(id, DomainRuntimeState::Stopped)
                .await
        });
        stopping.await.expect("native stop was requested");
        first.abort();
        assert!(
            first
                .await
                .expect_err("caller was cancelled")
                .is_cancelled()
        );
        assert!(
            service.task_exists(id).await,
            "unacknowledged native owner remains reachable"
        );
        tokio::time::timeout(Duration::from_secs(1), service.oauth_recovered(id))
            .await
            .expect("stopping actors must not receive OAuth recovery commands");
        assert!(
            service.inner.tasks.lock().await[id]
                .recovery
                .pending
                .lock()
                .unwrap()
                .is_none()
        );
        let native = service.clone();
        let retry = tokio::spawn(async move {
            native
                .stop_task_result(id, DomainRuntimeState::Stopped)
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !retry.is_finished(),
            "a cancellation signal is not a shutdown acknowledgement"
        );
        release.send(()).expect("allow native completion");
        retry
            .await
            .expect("retry join")
            .expect("native shutdown acknowledged");
        assert!(service.inner.tasks.lock().await[id].shutdown_tx.is_none());
        service
            .stop_task_result(id, DomainRuntimeState::Stopped)
            .await
            .expect("same retained result on repeat");
        service.shutdown().await;
    }

    #[tokio::test]
    async fn delayed_stop_does_not_remove_a_replacement_runtime_with_the_same_installation_id() {
        let (service, _, workspace) = test_mcp_service().await;
        let id = "replacement-native-installation";
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (call_tx, _call_rx) = mpsc::channel(1);
        let completed = Arc::new(CancellationToken::new());
        let completion = McpTaskCompletion(completed.clone());
        let (entered, stopping) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let join = tokio::spawn(async move {
            let _completion = completion;
            shutdown_rx.await.expect("original shutdown");
            entered.send(()).expect("original stop observer");
            released.await.expect("original completion release");
            Ok(())
        });
        let join = McpTaskJoin::new(join);
        service.inner.tasks.lock().await.insert(
            id.into(),
            McpServerTaskHandle {
                name: "replacement-native".into(),
                scope_kind: "workspace".into(),
                scope_key: workspace.clone(),
                fingerprint: "original".into(),
                effective_secret_fingerprint: "material".into(),
                call_tx,
                oauth_failure_revision: Arc::new(AtomicU64::new(0)),
                recovery: Arc::new(OAuthRecoveryMailbox::default()),
                shutdown_tx: Some(shutdown_tx),
                completed,
                join,
                session: Arc::new(Mutex::new(None)),
            },
        );
        let native = service.clone();
        let old_stop = tokio::spawn(async move {
            native
                .stop_task_result(id, DomainRuntimeState::Stopped)
                .await
        });
        stopping.await.expect("original stop admitted");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (call_tx, _call_rx) = mpsc::channel(1);
        let completed = Arc::new(CancellationToken::new());
        let completion = McpTaskCompletion(completed.clone());
        let join = tokio::spawn(async move {
            let _completion = completion;
            shutdown_rx
                .await
                .expect("replacement retains its own shutdown sender");
            Ok(())
        });
        let join = McpTaskJoin::new(join);
        service.inner.tasks.lock().await.insert(
            id.into(),
            McpServerTaskHandle {
                name: "replacement-native".into(),
                scope_kind: "workspace".into(),
                scope_key: workspace,
                fingerprint: "replacement".into(),
                effective_secret_fingerprint: "material".into(),
                call_tx,
                oauth_failure_revision: Arc::new(AtomicU64::new(0)),
                recovery: Arc::new(OAuthRecoveryMailbox::default()),
                shutdown_tx: Some(shutdown_tx),
                completed,
                join,
                session: Arc::new(Mutex::new(None)),
            },
        );
        release.send(()).expect("finish original runtime");
        old_stop
            .await
            .expect("original stop caller")
            .expect_err("replacement is not the original owner");
        assert_eq!(
            service.inner.tasks.lock().await[id].fingerprint,
            "replacement"
        );
        service
            .stop_task_result(id, DomainRuntimeState::Stopped)
            .await
            .expect("stop exact replacement");
        assert!(service.inner.tasks.lock().await[id].shutdown_tx.is_none());
        service
            .stop_task_result(id, DomainRuntimeState::Stopped)
            .await
            .expect("same retained result on repeat");
        service.shutdown().await;
    }

    #[tokio::test]
    async fn native_mcp_join_error_survives_cancelled_and_concurrent_observers() {
        let (release, released) = oneshot::channel::<()>();
        let owner = McpTaskJoin::new(tokio::spawn(async move {
            let _ = released.await;
            anyhow::bail!("native cleanup failed")
        }));
        let observer = owner.clone();
        let waiter = tokio::spawn(async move { observer.wait().await });
        tokio::task::yield_now().await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        release.send(()).unwrap();
        let (first, second) = tokio::join!(owner.wait(), owner.wait());
        assert!(first.is_err());
        assert!(second.is_err());
        assert!(owner.wait().await.is_err());
        let panic = McpTaskJoin::new(tokio::spawn(async {
            panic!("native actor panic");
        }));
        assert!(panic.wait().await.is_err());
        assert!(panic.wait().await.is_err());
    }

    async fn test_mcp_service() -> (McpService, Arc<CrudStore>, String) {
        let (service, crud_store, workspace_id, _gateway_secrets) =
            test_mcp_service_with_secrets().await;
        (service, crud_store, workspace_id)
    }

    async fn test_mcp_service_with_secrets()
    -> (McpService, Arc<CrudStore>, String, Arc<GatewaySecrets>) {
        test_mcp_service_with_store(Arc::new(MemorySecretStore::new())).await
    }

    async fn test_mcp_service_with_store(
        store: Arc<dyn pioneer_keystore::SecretStore>,
    ) -> (McpService, Arc<CrudStore>, String, Arc<GatewaySecrets>) {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        bootstrap(&connection)
            .await
            .expect("bootstrap should create default workspace");
        let crud_store = Arc::new(CrudStore::new(connection));
        let gateway_secrets = Arc::new(GatewaySecrets::new(store));
        let service = McpService::new(
            crud_store.clone(),
            Arc::new(SessionManager::new()),
            gateway_secrets.clone(),
            Arc::new(AtomicU64::new(1)),
            Arc::new(AuthorizationInvalidationHub::default()),
            Arc::new(ExecutionLeaseRegistry::default()),
        );
        (
            service,
            crud_store,
            DEFAULT_WORKSPACE_ID.to_owned(),
            gateway_secrets,
        )
    }

    struct ConnectReadStore {
        inner: MemorySecretStore,
        fail_delete: std::sync::Mutex<Option<String>>,
        armed: std::sync::atomic::AtomicBool,
        entered: tokio::sync::Notify,
        released: std::sync::Mutex<bool>,
        resumed: std::sync::Condvar,
    }
    impl ConnectReadStore {
        fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.resumed.notify_all();
        }
    }
    impl pioneer_keystore::SecretStore for ConnectReadStore {
        fn get_string(
            &self,
            id: &pioneer_keystore::SecretId,
        ) -> pioneer_keystore::Result<Option<String>> {
            if self.armed.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                let mut released = self.released.lock().unwrap();
                while !*released {
                    let next = self
                        .resumed
                        .wait_timeout(released, Duration::from_secs(5))
                        .unwrap();
                    released = next.0;
                    assert!(!next.1.timed_out(), "owned connect read must be released");
                }
            }
            self.inner.get_string(id)
        }
        fn put_string(
            &self,
            id: &pioneer_keystore::SecretId,
            value: &str,
            meta: pioneer_keystore::SecretMeta,
        ) -> pioneer_keystore::Result<()> {
            self.inner.put_string(id, value, meta)
        }
        fn delete(&self, id: &pioneer_keystore::SecretId) -> pioneer_keystore::Result<bool> {
            if self
                .fail_delete
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|target| {
                    id.user() == target || id.user() == format!("{target}::promotion")
                })
            {
                return Err(pioneer_keystore::KeystoreError::DeleteFailed(
                    "injected cleanup fault".into(),
                ));
            }
            self.inner.delete(id)
        }
        fn exists(&self, id: &pioneer_keystore::SecretId) -> pioneer_keystore::Result<bool> {
            self.inner.exists(id)
        }
        fn list(
            &self,
            filter: pioneer_keystore::SecretFilter,
        ) -> pioneer_keystore::Result<Vec<pioneer_keystore::SecretEntryMeta>> {
            self.inner.list(filter)
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cleanup_holder_does_not_abort_later_enabled_rows_or_disappear_on_disable() {
        let secrets = Arc::new(ConnectReadStore {
            inner: MemorySecretStore::new(),
            fail_delete: Default::default(),
            armed: Default::default(),
            entered: Default::default(),
            released: std::sync::Mutex::new(true),
            resumed: Default::default(),
        });
        let (service, store, workspace, _) = test_mcp_service_with_store(secrets.clone()).await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send"],
            fail_auth: false,
        }));
        let a = seed_mcp_installation(&store, &workspace, "a-cleanup", true, false).await;
        let b = seed_mcp_installation(&store, &workspace, "b-enabled", true, false).await;
        let mut row = store
            .find_mcp_server_installation("workspace", &workspace, "a-cleanup")
            .await
            .unwrap()
            .unwrap();
        row.transport_kind = "streamable_http".into();
        row.transport_json = serde_json::to_string(&McpTransportConfig::StreamableHttp {
            url: "http://127.0.0.1:9/mcp".into(),
            headers: Default::default(),
            startup_timeout_ms: 5000,
            tool_timeout_ms: 5000,
        })
        .unwrap();
        store
            .upsert_mcp_server_installation(&row, now_timestamp_secs())
            .await
            .unwrap();
        let installation = installation_from_record(&row).unwrap();
        *secrets.fail_delete.lock().unwrap() = Some(a.clone());
        assert!(
            service
                .oauth()
                .disconnect_managed(&a, &installation, 7, &workspace)
                .await
                .is_err()
        );
        let rows = store
            .list_mcp_server_installations("workspace", &workspace)
            .await
            .unwrap();
        assert!(
            rows.iter()
                .position(|r| r.id.as_deref() == Some(&a))
                .unwrap()
                < rows
                    .iter()
                    .position(|r| r.id.as_deref() == Some(&b))
                    .unwrap(),
            "cleanup must precede B in real repository ordering"
        );
        assert!(
            !service.task_exists(&b).await,
            "B must not be started by setup"
        );
        service.reload_workspace(&workspace).await.unwrap();
        wait_for_runtime_state(&service, &workspace, &b, DomainRuntimeState::Ready).await;
        assert!(service.task_exists(&b).await);
        let before = service.runtime_snapshot("workspace", &workspace).await[&b].runtime_generation;
        row.enabled = false;
        store
            .upsert_mcp_server_installation(&row, now_timestamp_secs())
            .await
            .unwrap();
        service.reload_workspace(&workspace).await.unwrap();
        wait_for_runtime_state(&service, &workspace, &a, DomainRuntimeState::Disabled).await;
        assert!(!service.task_exists(&a).await);
        assert!(
            service
                .oauth()
                .bound_to_installation(&a, &installation)
                .await
        );
        assert_eq!(
            service.oauth().state(&a).await,
            Some(pioneer_mcp_oauth::OAuthState::CleanupRequired)
        );
        assert!(
            pioneer_mcp::McpOAuthProvider::client(service.oauth(), &a, &installation)
                .await
                .is_err()
        );
        assert!(
            service
                .oauth()
                .sign_in(
                    &a,
                    &installation,
                    7,
                    "http://127.0.0.1:37643/oauth/mcp/callback"
                )
                .await
                .is_err()
        );
        *secrets.fail_delete.lock().unwrap() = None;
        service
            .oauth()
            .disconnect_managed(&a, &installation, 7, &workspace)
            .await
            .unwrap();
        // Confirmed management Clear retains a credential-free signed-out
        // admission. It must not look like an unprobed installation that can
        // start anonymous OAuth recovery on the next reconciliation.
        assert_eq!(
            service
                .oauth()
                .management_projection(&a, &installation)
                .await,
            (Some(pioneer_mcp_oauth::OAuthState::AuthRequired), false)
        );
        for account in [a.clone(), format!("{a}::promotion")] {
            assert!(
                !pioneer_keystore::SecretStore::exists(
                    &secrets.inner,
                    &pioneer_keystore::SecretId::mcp_oauth(&account).unwrap(),
                )
                .unwrap(),
                "confirmed Clear must remove the account and promotion fence"
            );
        }
        let error = pioneer_mcp::McpOAuthProvider::client(service.oauth(), &a, &installation)
            .await
            .err()
            .expect("signed-out admission must not restore an OAuth client");
        assert_eq!(error.state, DomainRuntimeState::AuthRequired);
        assert!(!service.task_exists(&a).await);
        let snapshot = service.runtime_snapshot("workspace", &workspace).await;
        assert_eq!(snapshot[&a].state, DomainRuntimeState::Disabled);
        assert_eq!(snapshot[&b].state, DomainRuntimeState::Ready);
        assert_eq!(snapshot[&b].runtime_generation, before);
        service.shutdown().await;
    }
    struct OwnedConnectConnector {
        owner: pioneer_mcp_oauth::McpOAuthService,
        store: Arc<ConnectReadStore>,
        completed: Arc<std::sync::atomic::AtomicBool>,
    }
    #[async_trait::async_trait]
    impl McpRuntimeConnector for OwnedConnectConnector {
        async fn connect(
            &self,
            installation: McpServerInstallation,
            id: String,
            resolver: Arc<dyn McpSecretResolver>,
            now: i64,
        ) -> Result<Box<dyn pioneer_mcp::McpRuntimeSession>, McpRuntimeError> {
            self.store.armed.store(true, Ordering::SeqCst);
            let result =
                pioneer_mcp::McpOAuthProvider::client(&self.owner, &id, &installation).await;
            self.completed.store(true, Ordering::SeqCst);
            result?;
            TestMcpRuntimeConnector {
                tools: vec!["send"],
                fail_auth: false,
            }
            .connect(installation, id, resolver, now)
            .await
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn gateway_shutdown_waits_for_oauth_connect_caller_before_actor_stop_join() {
        let secrets = Arc::new(ConnectReadStore {
            inner: MemorySecretStore::new(),
            fail_delete: Default::default(),
            armed: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            released: std::sync::Mutex::new(false),
            resumed: std::sync::Condvar::new(),
        });
        struct Release(Arc<ConnectReadStore>);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.release();
            }
        }
        let _release = Release(secrets.clone());
        let (service, store, workspace, _) = test_mcp_service_with_store(secrets.clone()).await;
        let id = seed_mcp_installation(&store, &workspace, "owned-connect", true, false).await;
        let mut row = store
            .find_mcp_server_installation("workspace", &workspace, "owned-connect")
            .await
            .unwrap()
            .unwrap();
        row.transport_json = serde_json::to_string(&McpTransportConfig::StreamableHttp {
            url: "http://127.0.0.1:9/mcp".into(),
            headers: Default::default(),
            startup_timeout_ms: 30_000,
            tool_timeout_ms: 120_000,
        })
        .unwrap();
        store
            .upsert_mcp_server_installation(&row, crate::message::now_timestamp_secs())
            .await
            .unwrap();
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        service.set_connector_for_tests(Arc::new(OwnedConnectConnector {
            owner: service.oauth().clone(),
            store: secrets.clone(),
            completed: completed.clone(),
        }));
        service
            .restart_server("workspace", &workspace, "owned-connect")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), secrets.entered.notified())
            .await
            .unwrap();
        let owner = service.clone();
        let mut shutdown = tokio::spawn(async move { owner.shutdown().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut shutdown)
                .await
                .is_err()
        );
        assert!(!completed.load(Ordering::SeqCst));
        assert!(
            service.task_exists(&id).await,
            "OAuth shutdown must still own connect before actor removal"
        );
        secrets.release();
        tokio::time::timeout(Duration::from_secs(3), shutdown)
            .await
            .unwrap()
            .unwrap();
        assert!(completed.load(Ordering::SeqCst));
        assert!(!service.task_exists(&id).await);
        assert_eq!(
            service.runtime_snapshot("workspace", &workspace).await[&id].state,
            DomainRuntimeState::Stopped
        );
    }

    #[tokio::test]
    async fn management_notifications_are_superuser_only() {
        let (service, _, _) = test_mcp_service().await;
        let session_manager = service.inner.session_manager.clone();

        let (superuser_tx, mut superuser_rx) = mpsc::channel(2);
        session_manager
            .register_connection(superuser_tx, authenticated_test_superuser())
            .await
            .expect("register Superuser");

        let mut member = authenticated_test_superuser().as_ref().clone();
        member.principal_id =
            PrincipalId::new("P0000000000000000000A").expect("Member principal id");
        member.kind = PrincipalKind::User;
        member.role_key = Some(RoleKey::member());
        member.device_id = DeviceId::new("D0000000000000000000A").expect("Member device id");
        member.session_id = AuthSessionId::new("S0000000000000000000A").expect("Member session id");
        let (member_tx, mut member_rx) = mpsc::channel(2);
        session_manager
            .register_connection(member_tx, Arc::new(member))
            .await
            .expect("register Member");

        service
            .send_management_notification("test/mcp-management", &json!({"name": "private-server"}))
            .await;

        assert!(
            superuser_rx.recv().await.is_some(),
            "Superuser must receive MCP management notifications"
        );
        assert!(
            member_rx.try_recv().is_err(),
            "Member must not receive MCP management, status, or catalog metadata"
        );
    }

    async fn seed_started_turn(
        crud_store: &CrudStore,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) {
        let timestamp = 1_700_000_000;
        let thread = Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Agent,
            model: "test-model".to_owned(),
            model_provider: "test-provider".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        ensure_test_superuser_execution_authority(crud_store).await;
        let execution_principal = authenticated_test_superuser();
        let execution_actor = pioneer_protocol::PersistedActorRef::Principal(
            execution_principal.principal_id.clone(),
        );
        crud_store
            .upsert_thread_model(&thread, execution_actor.clone())
            .await
            .expect("test thread should persist");
        crud_store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                execution_actor,
            )
            .await
            .expect("test turn should persist before MCP projection");
        let persisted = turn::Entity::find_by_id(turn_id)
            .one(&crud_store.database_connection())
            .await
            .expect("MCP background turn provenance query should succeed")
            .expect("MCP background turn should exist");
        assert_eq!(
            persisted.initiated_by_actor_kind.as_deref(),
            Some("principal")
        );
        assert_eq!(
            persisted.initiated_by_actor_id.as_deref(),
            Some(execution_principal.principal_id.as_str())
        );
        let mut execution_context = ExecutionAuthorizationContext::for_test(
            execution_principal.as_ref(),
            workspace_id,
            thread_id,
            &pioneer_protocol::default_turn_permission_profile_snapshot(),
            None,
        );
        execution_context.bind_test_mcp_server_grants(
            pioneer_protocol::McpScopeKind::Workspace,
            ["resend".to_owned()],
        );
        let execution_context = execution_context
            .to_persisted_json()
            .expect("test MCP execution context should serialize");
        crud_store
            .set_turn_execution_authorization_context(turn_id, execution_context.as_str())
            .await
            .expect("test MCP execution context should persist");
    }

    async fn seed_mcp_installation(
        crud_store: &CrudStore,
        workspace_id: &str,
        name: &str,
        enabled: bool,
        allow_implicit_invocation: bool,
    ) -> String {
        let transport = McpTransportConfig::Stdio {
            command: "test-mcp".to_owned(),
            args: Vec::new(),
            cwd: None,
            env: BTreeMap::new(),
            startup_timeout_ms: 5_000,
            tool_timeout_ms: 5_000,
        };
        let record = McpServerInstallationRecord {
            id: None,
            scope_kind: "workspace".to_owned(),
            scope_key: workspace_id.to_owned(),
            name: name.to_owned(),
            display_name: None,
            source_kind: "config".to_owned(),
            source_ref: json!({"kind":"test"}).to_string(),
            transport_kind: "stdio".to_owned(),
            transport_json: serde_json::to_string(&transport).expect("transport serializes"),
            auth_json: serde_json::to_string(&McpAuthConfig::default()).expect("auth serializes"),
            secret_refs_json: "[]".to_owned(),
            enabled,
            allow_implicit_invocation,
            required: false,
            fingerprint: format!("{name}-fingerprint"),
            updated_at_unix: 1_700_000_000,
        };
        crud_store
            .upsert_mcp_server_installation(&record, 1_700_000_000)
            .await
            .expect("test MCP installation should persist");
        crud_store
            .find_mcp_server_installation("workspace", workspace_id, name)
            .await
            .expect("test MCP installation lookup should succeed")
            .and_then(|row| row.id)
            .expect("test MCP installation should have id")
    }

    async fn configure_mcp_installation_secret(
        crud_store: &CrudStore,
        workspace_id: &str,
        name: &str,
        ref_id: &str,
    ) {
        let mut installation = crud_store
            .find_mcp_server_installation("workspace", workspace_id, name)
            .await
            .expect("test installation lookup should succeed")
            .expect("test installation should exist");
        installation.transport_json = serde_json::to_string(&McpTransportConfig::Stdio {
            command: "test-mcp".to_owned(),
            args: Vec::new(),
            cwd: None,
            env: BTreeMap::from([(
                "TOKEN".to_owned(),
                pioneer_mcp::McpConfigValue::SecretRef {
                    ref_id: ref_id.to_owned(),
                },
            )]),
            startup_timeout_ms: 5_000,
            tool_timeout_ms: 5_000,
        })
        .expect("secret transport should serialize");
        installation.secret_refs_json = serde_json::to_string(&vec![McpSecretRef {
            ref_id: ref_id.to_owned(),
            name: "TOKEN".to_owned(),
            source: "env".to_owned(),
        }])
        .expect("secret refs should serialize");
        crud_store
            .upsert_mcp_server_installation(&installation, 1_700_000_001)
            .await
            .expect("secret-backed installation should persist");
    }

    async fn mark_mcp_installation_required(
        crud_store: &CrudStore,
        workspace_id: &str,
        name: &str,
    ) {
        let mut installation = crud_store
            .find_mcp_server_installation("workspace", workspace_id, name)
            .await
            .expect("test MCP installation lookup should succeed")
            .expect("test MCP installation should exist");
        installation.required = true;
        crud_store
            .upsert_mcp_server_installation(&installation, 1_700_000_001)
            .await
            .expect("required MCP installation should persist");
    }

    async fn wait_for_catalog(crud_store: &CrudStore, installation_id: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if crud_store
                .find_mcp_server_catalog_snapshot(installation_id)
                .await
                .expect("catalog lookup should succeed")
                .is_some()
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() <= deadline,
                "test MCP catalog should be persisted"
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn wait_for_runtime_state(
        service: &McpService,
        workspace_id: &str,
        installation_id: &str,
        expected: DomainRuntimeState,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let snapshot = service.runtime_snapshot("workspace", workspace_id).await;
            if snapshot
                .get(installation_id)
                .is_some_and(|snapshot| snapshot.state == expected)
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() <= deadline,
                "test MCP runtime should reach {expected:?}"
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn wait_for_test_notification(notification: &tokio::sync::Notify, context: &str) {
        tokio::time::timeout(Duration::from_secs(5), notification.notified())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {context}"));
    }

    async fn assert_turn_mcp_outcome_reason(
        fixture: &TestTurnMcpInvokerFixture,
        expected_reason: &str,
    ) {
        let audits = fixture
            .crud_store
            .list_recent_mcp_audit_event_records("resend", 20)
            .await
            .expect("MCP invocation audit should be readable");
        let outcomes = audits
            .iter()
            .filter(|audit| audit.action == "turn_mcp_invocation_outcome")
            .collect::<Vec<_>>();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].reason_code.as_deref(), Some(expected_reason));
    }

    fn server_ref(id: &str, name: &str) -> AgentMcpServerRef {
        AgentMcpServerRef {
            capability_id: id.to_owned(),
            label: Some(name.to_owned()),
            name: name.to_owned(),
            scope_kind: McpScopeKind::Workspace,
        }
    }

    fn tool_ref(id: &str, server_name: &str, raw_tool_name: &str) -> AgentMcpToolRef {
        AgentMcpToolRef {
            capability_id: id.to_owned(),
            label: Some(format!("{server_name}/{raw_tool_name}")),
            server_name: server_name.to_owned(),
            raw_tool_name: raw_tool_name.to_owned(),
            scope_kind: McpScopeKind::Workspace,
        }
    }

    fn materialized_tool_names(materialization: &AgentMcpMaterialization) -> Vec<String> {
        let mut names = materialization
            .bundles
            .iter()
            .flat_map(|bundle| bundle.specs.iter())
            .map(|configured| configured.spec.name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    async fn turn_mcp_invoker_fixture() -> TestTurnMcpInvokerFixture {
        turn_mcp_invoker_fixture_with_connector(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send"],
            fail_auth: false,
        }))
        .await
    }

    async fn turn_mcp_invoker_fixture_with_connector(
        connector: Arc<dyn McpRuntimeConnector>,
    ) -> TestTurnMcpInvokerFixture {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        let thread_id = "T00000000000000000001".to_owned();
        let turn_id = "U00000000000000000001".to_owned();
        seed_started_turn(
            &crud_store,
            workspace_id.as_str(),
            thread_id.as_str(),
            turn_id.as_str(),
        )
        .await;
        ensure_test_superuser_execution_authority(crud_store.as_ref()).await;
        let mut execution_context = ExecutionAuthorizationContext::for_test(
            authenticated_test_superuser().as_ref(),
            workspace_id.as_str(),
            thread_id.as_str(),
            &pioneer_protocol::default_turn_permission_profile_snapshot(),
            None,
        );
        execution_context.bind_test_mcp_server_grants(
            pioneer_protocol::McpScopeKind::Workspace,
            ["resend".to_owned()],
        );
        let execution_context = execution_context
            .to_persisted_json()
            .expect("test MCP execution context should serialize");
        crud_store
            .set_turn_execution_authorization_context(turn_id.as_str(), execution_context.as_str())
            .await
            .expect("test MCP execution context should persist");
        service.set_connector_for_tests(connector);
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, false).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("invoker MCP runtime should start");
        wait_for_catalog(&crud_store, installation_id.as_str()).await;
        let projection = service
            .resolve_mcp_turn_projection(&AgentMcpMaterializationRequest {
                workspace_id: workspace_id.clone(),
                turn_id: turn_id.clone(),
                explicit_servers: Vec::new(),
                explicit_tools: vec![tool_ref("mcp-tool:workspace:resend:send", "resend", "send")],
            })
            .await
            .expect("invoker projection should resolve");
        service
            .persist_resolved_mcp_turn_projection(&projection)
            .await
            .expect("invoker projection should persist");
        crud_store
            .set_turn_execution_security_snapshot(
                turn_id.as_str(),
                &TurnExecutionSecuritySnapshot::unrestricted_full_access(
                    std::env::temp_dir().display().to_string(),
                    1_700_000_000_000,
                ),
            )
            .await
            .expect("invoker security snapshot should persist");
        let callable_name = projection.tools[0].canonical_callable_name.clone();
        let manifest_hash = projection.manifest_hash.clone();
        let execution = Arc::new(TestValidatedMcpExecution::default());
        let invoker = GatewayTurnMcpInvoker::new(
            crud_store.clone(),
            Arc::new(service.clone()),
            execution.clone(),
            service.inner.authorization_invalidation_hub.clone(),
            service.inner.execution_leases.clone(),
            service.inner.invocation_governor.clone(),
        );
        TestTurnMcpInvokerFixture {
            service,
            crud_store,
            workspace_id,
            thread_id,
            turn_id,
            callable_name,
            manifest_hash,
            execution,
            invoker,
        }
    }

    async fn authorize_turn_mcp_fixture_for_member(fixture: &TestTurnMcpInvokerFixture) {
        const GATEWAY_ID: &str = "G00000000000000000001";
        const PRINCIPAL_ID: &str = "P0000000000000000000A";
        const DEVICE_ID: &str = "D0000000000000000000A";
        const SESSION_ID: &str = "S0000000000000000000A";

        fixture
            .crud_store
            .database_connection()
            .execute_unprepared(
                format!(
                    "INSERT OR IGNORE INTO gateway_identity(\
                        id,singleton_key,identity_bootstrap_version,auth_schema_version,\
                        created_at,updated_at\
                     ) VALUES(\
                        '{GATEWAY_ID}',1,1,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP\
                     );\
                     INSERT INTO gateway_principal(\
                        id,gateway_id,kind,role_key,status,display_name,nickname,nickname_key,\
                        created_at,updated_at,removed_at\
                     ) VALUES(\
                        '{PRINCIPAL_ID}','{GATEWAY_ID}','user','member','active',\
                        'MCP Member','mcp-member','mcp-member',\
                        CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL\
                     );\
                     INSERT INTO workspace_membership(\
                        principal_id,workspace_id,granted_by_actor_kind,granted_by_actor_id,\
                        created_at,updated_at\
                     ) VALUES(\
                        '{PRINCIPAL_ID}','{}','system',NULL,\
                        CURRENT_TIMESTAMP,CURRENT_TIMESTAMP\
                     );\
                     UPDATE thread \
                     SET access_class='private',\
                         created_by_actor_kind='principal',\
                         created_by_actor_id='{PRINCIPAL_ID}'\
                     WHERE id='{}';\
                     INSERT INTO thread_membership(\
                        thread_id,principal_id,added_by_actor_kind,added_by_actor_id,\
                        created_at,updated_at\
                     ) VALUES(\
                        '{}','{PRINCIPAL_ID}','system',NULL,\
                        CURRENT_TIMESTAMP,CURRENT_TIMESTAMP\
                     );\
                     INSERT INTO device(\
                        id,gateway_id,principal_id,installation_id,display_name,client_kind,\
                        platform,client_version,status,created_at,updated_at,last_seen_at,revoked_at\
                     ) VALUES(\
                        '{DEVICE_ID}','{GATEWAY_ID}','{PRINCIPAL_ID}','fixture-mcp-member',\
                        'MCP Member Desktop','desktop','test','1','active',\
                        CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL\
                     );\
                     INSERT INTO auth_session(\
                        id,gateway_id,principal_id,device_id,token_family_id,\
                        created_by_session_id,activation_token_hash,activation_locator_hash,\
                        activation_failed_attempts,activation_expires_at,activated_at,status,\
                        refresh_generation,created_at,updated_at,last_seen_at,last_refreshed_at,\
                        refresh_expires_at,revoked_at,revoke_reason\
                     ) VALUES(\
                        '{SESSION_ID}','{GATEWAY_ID}','{PRINCIPAL_ID}','{DEVICE_ID}',\
                        'F0000000000000000000A',NULL,\
                        X'000000000000000000000000000000000000000000000000000000000000000A',\
                        X'100000000000000000000000000000000000000000000000000000000000000A',\
                        0,datetime('now','+10 minutes'),CURRENT_TIMESTAMP,'active',0,\
                        CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,\
                        datetime('now','+90 days'),NULL,NULL\
                     );\
                     UPDATE turn \
                     SET initiated_by_actor_kind='principal',\
                         initiated_by_actor_id='{PRINCIPAL_ID}'\
                     WHERE id='{}';",
                    fixture.workspace_id,
                    fixture.thread_id,
                    fixture.thread_id,
                    fixture.turn_id,
                )
                .as_str(),
            )
            .await
            .expect("Member MCP authorization foundation should persist");

        let principal = AuthenticatedSessionPrincipal {
            gateway_id: GatewayId::new(GATEWAY_ID).expect("test Gateway id"),
            principal_id: PrincipalId::new(PRINCIPAL_ID).expect("test Member principal id"),
            kind: PrincipalKind::User,
            role_key: Some(RoleKey::member()),
            device_id: DeviceId::new(DEVICE_ID).expect("test Member device id"),
            session_id: AuthSessionId::new(SESSION_ID).expect("test Member session id"),
            access_jti: "J0000000000000000000A".to_owned(),
            access_expires_at_unix: u64::MAX,
        };
        let mut context = ExecutionAuthorizationContext::for_test(
            &principal,
            fixture.workspace_id.as_str(),
            fixture.thread_id.as_str(),
            &pioneer_protocol::compile_turn_permission_profile(
                TurnPermissionMode::Supervised,
                TurnPermissionProfileSource::TaskPermissionCap,
            ),
            None,
        );
        context.bind_test_mcp_server_grants(
            pioneer_protocol::McpScopeKind::Workspace,
            ["resend".to_owned()],
        );
        context
            .bind_mcp_projection(
                fixture.workspace_id.as_str(),
                MCP_TURN_PROJECTION_VERSION,
                fixture.manifest_hash.as_str(),
                &["resend".to_owned()],
            )
            .expect("Member execution should bind the frozen MCP projection");
        let context_json = context
            .to_persisted_json()
            .expect("Member MCP execution context should serialize");
        fixture
            .crud_store
            .set_turn_execution_authorization_context(
                fixture.turn_id.as_str(),
                context_json.as_str(),
            )
            .await
            .expect("Member MCP execution context should persist");
    }

    fn native_mcp_invocation(fixture: &TestTurnMcpInvokerFixture) -> TurnMcpInvocation {
        TurnMcpInvocation {
            workspace_id: fixture.workspace_id.clone(),
            thread_id: fixture.thread_id.clone(),
            turn_id: fixture.turn_id.clone(),
            runtime_id: None,
            session_generation: None,
            provider_call_id: "provider-call-test".to_owned(),
            canonical_callable_name: fixture.callable_name.clone(),
            arguments: json!({"secret": "argument-canary-must-not-enter-errors"}),
            origin: TurnMcpInvocationOrigin::NativeApi,
        }
    }

    async fn set_invoker_permission_mode(
        fixture: &TestTurnMcpInvokerFixture,
        mode: TurnPermissionMode,
    ) -> TurnExecutionSecuritySnapshot {
        let mut snapshot = TurnExecutionSecuritySnapshot::unrestricted_full_access(
            std::env::temp_dir().display().to_string(),
            1_700_000_000_001,
        );
        snapshot.permission_profile =
            TurnPermissionProfileSnapshot::from_mode(mode, TurnPermissionProfileSource::Composer);
        fixture
            .crud_store
            .set_turn_execution_security_snapshot(fixture.turn_id.as_str(), &snapshot)
            .await
            .expect("invoker permission snapshot should persist");
        snapshot
    }

    fn expect_mcp_materialization_failure(
        result: Result<AgentMcpMaterialization, AgentMcpMaterializationError>,
        context: &str,
    ) -> AgentMcpMaterializationError {
        match result {
            Ok(_) => panic!("{context}"),
            Err(error) => error,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_projection_empty_workspace_is_a_successful_empty_projection() {
        let (service, _crud_store, workspace_id) = test_mcp_service().await;
        let projection = service
            .resolve_mcp_turn_projection(&AgentMcpMaterializationRequest {
                workspace_id: workspace_id.clone(),
                turn_id: "turn_empty_projection".to_owned(),
                explicit_servers: Vec::new(),
                explicit_tools: Vec::new(),
            })
            .await
            .expect("empty MCP projection should resolve");

        assert_eq!(projection.workspace_id, workspace_id);
        assert_eq!(projection.turn_id, "turn_empty_projection");
        assert!(projection.tools.is_empty());
        assert!(projection.accepted_capabilities.is_empty());
        assert!(projection.rejected_capabilities.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_mcp_parity_after_turn_mcp_persistence_commit() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        let turn_id = "turn_api_mcp_parity";
        seed_started_turn(
            &crud_store,
            workspace_id.as_str(),
            "thread_api_mcp_parity",
            turn_id,
        )
        .await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send", "domains"],
            fail_auth: false,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, false).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_catalog(&crud_store, installation_id.as_str()).await;

        let explicit_tool = tool_ref("mcp-tool:workspace:resend:send", "resend", "send");
        let projection = service
            .resolve_mcp_turn_projection(&AgentMcpMaterializationRequest {
                workspace_id: workspace_id.clone(),
                turn_id: turn_id.to_owned(),
                explicit_servers: Vec::new(),
                explicit_tools: vec![explicit_tool.clone()],
            })
            .await
            .expect("MCP projection should resolve");
        let materialization = service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id: workspace_id.clone(),
                turn_id: turn_id.to_owned(),
                explicit_servers: Vec::new(),
                explicit_tools: vec![explicit_tool],
            })
            .await
            .expect("native MCP materialization should succeed");

        let persistence = materialization
            .persistence
            .clone()
            .expect("native materialization should carry one persistence request");
        let persisted = service
            .persist_mcp_projection(persistence)
            .await
            .expect("projection must commit before native provider startup");
        assert_eq!(persisted.turn_id, turn_id);
        assert_eq!(persisted.manifest_hash, projection.manifest_hash);
        assert_eq!(persisted.tool_count, projection.tools.len());

        let durable_projection = crud_store
            .get_turn_mcp_projection(turn_id)
            .await
            .expect("projection header lookup should succeed")
            .expect("projection header should be durable");
        assert_eq!(durable_projection.workspace_id, workspace_id);
        assert_eq!(durable_projection.manifest_hash, projection.manifest_hash);
        assert_eq!(durable_projection.tool_count, 1);
        let durable_bindings = crud_store
            .list_turn_mcp_bindings(turn_id)
            .await
            .expect("projection binding lookup should succeed");
        assert_eq!(durable_bindings.len(), 1);
        assert_eq!(
            durable_bindings[0].canonical_callable_name,
            projection.tools[0].canonical_callable_name
        );
        assert_eq!(
            durable_bindings[0].provider_callable_name,
            projection.tools[0].canonical_callable_name
        );
        assert_eq!(
            durable_bindings[0].canonical_schema_fingerprint,
            projection.tools[0].schema_fingerprint
        );
        assert_eq!(
            durable_bindings[0].provider_schema_fingerprint,
            projection.tools[0].schema_fingerprint
        );

        assert_eq!(
            projection
                .tools
                .iter()
                .map(|tool| tool.canonical_callable_name.clone())
                .collect::<Vec<_>>(),
            materialized_tool_names(&materialization)
        );
        assert_eq!(projection.tools.len(), 1);
        assert_eq!(
            projection.tools[0].selection_reason,
            McpSelectionReason::ExplicitTool
        );
        assert_eq!(projection.tools[0].server_installation_id, installation_id);
        let descriptor = projection.tools[0].as_dynamic_descriptor();
        let configured = materialization.bundles[0]
            .specs
            .first()
            .expect("native API materialization should expose one configured tool");
        assert_eq!(configured.spec.name, descriptor.callable_name);
        assert_eq!(configured.spec.parameters, descriptor.parameters);
        assert_eq!(
            descriptor.annotations,
            projection.tools[0].annotations.clone().unwrap_or_default()
        );
        assert_eq!(descriptor.timeout_ms, Some(projection.tools[0].timeout_ms));
        assert_eq!(
            configured.payload_binding,
            pioneer_tools::ToolPayloadBinding::Mcp {
                server_id: descriptor.server_id.clone(),
                server_name: descriptor.server_name.clone(),
                raw_tool_name: descriptor.raw_tool_name.clone(),
                catalog_version: descriptor.catalog_version.clone(),
                snapshot_version: descriptor.snapshot_version,
                read_only_hint: descriptor.annotations.read_only_hint,
                destructive_hint: descriptor.annotations.destructive_hint,
                open_world_hint: descriptor.annotations.open_world_hint,
            }
        );
        assert_eq!(materialization.mcp_bindings.len(), 1);
        assert_eq!(
            materialization.mcp_bindings[0].callable_name,
            descriptor.callable_name
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_persistence_fault_rolls_back_coordinator_transaction() {
        let fixture = turn_mcp_invoker_fixture().await;
        let committed_projection = fixture
            .crud_store
            .get_turn_mcp_projection(fixture.turn_id.as_str())
            .await
            .expect("committed MCP projection query should succeed")
            .expect("committed MCP projection should exist");
        let committed_bindings = fixture
            .crud_store
            .list_turn_mcp_bindings(fixture.turn_id.as_str())
            .await
            .expect("committed MCP bindings query should succeed");

        let projection = fixture
            .service
            .resolve_mcp_turn_projection(&AgentMcpMaterializationRequest {
                workspace_id: fixture.workspace_id.clone(),
                turn_id: fixture.turn_id.clone(),
                explicit_servers: Vec::new(),
                explicit_tools: vec![tool_ref("mcp-tool:workspace:resend:send", "resend", "send")],
            })
            .await
            .expect("replacement MCP projection should resolve");
        let mut attempted = persistence_request_from_projection(&projection)
            .expect("replacement MCP persistence request should build");
        attempted.manifest_hash = "manifest-fault-attempt".to_owned();
        let mut first = attempted.bindings[0].clone();
        first.callable_name = "mcp_resend_a".to_owned();
        first.canonical_callable_name = "mcp_resend_a".to_owned();
        first.provider_callable_name = "mcp_resend_a".to_owned();
        let mut failing = attempted.bindings[0].clone();
        failing.callable_name = "mcp_resend_z".to_owned();
        failing.canonical_callable_name = "mcp_resend_z".to_owned();
        failing.provider_callable_name = "mcp_resend_z".to_owned();
        attempted.bindings = vec![first, failing];

        fixture
            .crud_store
            .database_connection()
            .execute_unprepared(
                format!(
                    "CREATE TRIGGER fail_turn_mcp_projection_insert \
                     BEFORE INSERT ON turn_mcp_binding \
                     WHEN NEW.turn_id = '{}' AND NEW.canonical_callable_name = 'mcp_resend_z' \
                     BEGIN SELECT RAISE(ABORT, 'injected turn MCP persistence fault'); END",
                    fixture.turn_id
                )
                .as_str(),
            )
            .await
            .expect("fault-injection trigger should install");

        fixture
            .service
            .inner
            .projection_persistence
            .persist(&attempted)
            .await
            .expect_err("injected binding failure must abort the whole projection transaction");

        assert_eq!(
            fixture
                .crud_store
                .get_turn_mcp_projection(fixture.turn_id.as_str())
                .await
                .expect("projection query after fault should succeed"),
            Some(committed_projection)
        );
        assert_eq!(
            fixture
                .crud_store
                .list_turn_mcp_bindings(fixture.turn_id.as_str())
                .await
                .expect("binding query after fault should succeed"),
            committed_bindings
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_invoker_binding_exact_match_advances_and_unbound_fails_before_execution() {
        let fixture = turn_mcp_invoker_fixture().await;

        let output = fixture
            .invoker
            .invoke(native_mcp_invocation(&fixture), CancellationToken::new())
            .await
            .expect("exact frozen binding should advance to execution");
        assert_eq!(output.content[0]["text"], "validated");
        assert_eq!(fixture.execution.calls.load(Ordering::SeqCst), 1);

        let mut unknown = native_mcp_invocation(&fixture);
        unknown.canonical_callable_name = "mcp_resend_unbound".to_owned();
        let error = fixture
            .invoker
            .invoke(unknown, CancellationToken::new())
            .await
            .expect_err("unbound callable must fail before execution");
        assert_eq!(error.code, TurnMcpInvocationErrorCode::ToolUnbound);
        assert_eq!(error.reason_code(), "tool_unbound");
        assert!(!error.to_string().contains("argument-canary"));
        assert_eq!(fixture.execution.calls.load(Ordering::SeqCst), 1);

        let mut cross_scope = native_mcp_invocation(&fixture);
        cross_scope.workspace_id = "workspace-cross-scope".to_owned();
        let error = fixture
            .invoker
            .invoke(cross_scope, CancellationToken::new())
            .await
            .expect_err("cross-scope invocation must fail before execution");
        assert_eq!(error.code, TurnMcpInvocationErrorCode::ScopeMismatch);
        assert_eq!(fixture.execution.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn member_turn_mcp_policy_uses_server_name_and_revalidates_revocation() {
        let fixture = turn_mcp_invoker_fixture().await;
        authorize_turn_mcp_fixture_for_member(&fixture).await;
        let binding = fixture
            .crud_store
            .list_turn_mcp_bindings(fixture.turn_id.as_str())
            .await
            .expect("Member MCP binding should load")
            .remove(0);
        assert_ne!(
            binding.server_installation_id, binding.server_name,
            "the regression requires distinct policy and frozen-installation identities"
        );

        fixture
            .invoker
            .invoke(native_mcp_invocation(&fixture), CancellationToken::new())
            .await
            .expect("enabled workspace MCP policy should authorize the Member invocation");
        assert_eq!(fixture.execution.calls.load(Ordering::SeqCst), 1);

        let mut installation = fixture
            .crud_store
            .find_mcp_server_installation(
                "workspace",
                fixture.workspace_id.as_str(),
                binding.server_name.as_str(),
            )
            .await
            .expect("Member MCP installation lookup should succeed")
            .expect("Member MCP installation should exist");
        installation.enabled = false;
        fixture
            .crud_store
            .upsert_mcp_server_installation(&installation, 1_700_000_100)
            .await
            .expect("MCP policy revocation should persist");

        let error = fixture
            .invoker
            .invoke(native_mcp_invocation(&fixture), CancellationToken::new())
            .await
            .expect_err("revoked workspace MCP policy must deny the Member invocation");
        assert_eq!(error.code, TurnMcpInvocationErrorCode::PermissionDenied);
        assert_eq!(
            fixture.execution.calls.load(Ordering::SeqCst),
            1,
            "revoked MCP policy must fail before tool execution"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn member_task_child_invokes_the_exact_mcp_projection_through_authorized_root_lineage() {
        let fixture = turn_mcp_invoker_fixture().await;
        authorize_turn_mcp_fixture_for_member(&fixture).await;

        let child_thread_id = "T00000000000000000002";
        let child_turn_id = "U00000000000000000002";
        seed_started_turn(
            fixture.crud_store.as_ref(),
            fixture.workspace_id.as_str(),
            child_thread_id,
            child_turn_id,
        )
        .await;
        const MEMBER_PRINCIPAL_ID: &str = "P0000000000000000000A";
        let task_id = "task_turn_mcp_task_child";
        let run_id = "run_turn_mcp_task_child";
        let parent_context = fixture
            .crud_store
            .get_turn_execution_authorization_context(fixture.turn_id.as_str())
            .await
            .expect("parent execution authorization context should load")
            .expect("parent execution authorization context should exist");
        fixture
            .crud_store
            .database_connection()
            .execute_unprepared(
                format!(
                    "UPDATE thread SET access_class='internal' WHERE id='{child_thread_id}'; \
                     UPDATE turn SET initiated_by_actor_kind='principal', \
                     initiated_by_actor_id='{MEMBER_PRINCIPAL_ID}' WHERE id='{child_turn_id}'"
                )
                .as_str(),
            )
            .await
            .expect("task child actor provenance should persist");
        fixture
            .crud_store
            .append_task_events_with_execution_admission(
                vec![
                    TaskEventPayload::TaskCreated {
                        task: Task {
                            id: task_id.to_owned(),
                            workspace_id: fixture.workspace_id.clone(),
                            owner_kind: TaskOwnerKind::Thread,
                            owner_id: Some(fixture.thread_id.clone()),
                            created_by_thread_id: Some(fixture.thread_id.clone()),
                            created_by_turn_id: Some(fixture.turn_id.clone()),
                            root_task_id: Some(task_id.to_owned()),
                            parent_task_id: None,
                            executor_kind: TaskExecutorKind::Agent,
                            status: TaskStatus::Running,
                            title: "MCP child authority fixture".to_owned(),
                            goal: "Prove exact inherited MCP projection".to_owned(),
                            priority: 0,
                            lifecycle_policy: None,
                            delivery_policy: None,
                            retry_policy: None,
                            timeout_policy: None,
                            concurrency_policy: None,
                            metadata: None,
                            result: None,
                            error: None,
                            revision: 1,
                            created_at: 1_700_000_001,
                            updated_at: 1_700_000_001,
                            completed_at: None,
                        },
                    },
                    TaskEventPayload::RunCreated {
                        run: TaskRun {
                            id: run_id.to_owned(),
                            task_id: task_id.to_owned(),
                            trigger_id: None,
                            parent_run_id: None,
                            run_group_id: "run_group_turn_mcp_task_child".to_owned(),
                            attempt_number: 1,
                            retry_of_run_id: None,
                            ready_at: Some(1_700_000_001),
                            run_number: 1,
                            status: TaskRunStatus::Running,
                            executor_kind: TaskExecutorKind::Agent,
                            started_at: Some(1_700_000_001),
                            completed_at: None,
                            heartbeat_at: None,
                            locked_by: None,
                            lock_expires_at: None,
                            result: None,
                            error: None,
                            created_at: 1_700_000_001,
                            updated_at: 1_700_000_001,
                        },
                        agent_spec: None,
                    },
                    TaskEventPayload::ChildThreadLinked {
                        lineage: ThreadLineage {
                            child_thread_id: child_thread_id.to_owned(),
                            child_turn_id: child_turn_id.to_owned(),
                            parent_thread_id: fixture.thread_id.clone(),
                            parent_turn_id: Some(fixture.turn_id.clone()),
                            task_id: task_id.to_owned(),
                            task_run_id: run_id.to_owned(),
                            root_thread_id: fixture.thread_id.clone(),
                            depth: 1,
                            created_at: 1_700_000_001,
                        },
                    },
                ],
                1_700_000_001,
                NewTaskExecutionAdmission {
                    task_id: task_id.to_owned(),
                    workspace_id: fixture.workspace_id.clone(),
                    root_thread_id: fixture.thread_id.clone(),
                    initiating_principal_id: MEMBER_PRINCIPAL_ID.to_owned(),
                    authorization_context_json: parent_context.clone(),
                    created_at: chrono::Utc::now().fixed_offset(),
                    execution_lease: None,
                },
            )
            .await
            .expect("task child lineage should persist");
        fixture
            .crud_store
            .set_turn_execution_authorization_context(child_turn_id, parent_context.as_str())
            .await
            .expect("task child execution authorization context should persist");

        let child_projection = fixture
            .service
            .resolve_mcp_turn_projection(&AgentMcpMaterializationRequest {
                workspace_id: fixture.workspace_id.clone(),
                turn_id: child_turn_id.to_owned(),
                explicit_servers: Vec::new(),
                explicit_tools: vec![tool_ref("mcp-tool:workspace:resend:send", "resend", "send")],
            })
            .await
            .expect("task child MCP projection should resolve");
        assert_eq!(
            child_projection.manifest_hash, fixture.manifest_hash,
            "the child must inherit the exact immutable MCP capability projection"
        );
        fixture
            .service
            .persist_resolved_mcp_turn_projection(&child_projection)
            .await
            .expect("task child MCP projection should persist");
        fixture
            .crud_store
            .set_turn_execution_security_snapshot(
                child_turn_id,
                &TurnExecutionSecuritySnapshot::unrestricted_full_access(
                    std::env::temp_dir().display().to_string(),
                    1_700_000_000_002,
                ),
            )
            .await
            .expect("task child security snapshot should persist");
        let mut invocation = native_mcp_invocation(&fixture);
        invocation.thread_id = child_thread_id.to_owned();
        invocation.turn_id = child_turn_id.to_owned();
        fixture
            .invoker
            .invoke(invocation, CancellationToken::new())
            .await
            .expect("authorized task child should invoke its exact frozen MCP projection");
        assert_eq!(fixture.execution.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_invoker_rejects_projection_not_bound_to_execution_context() {
        let fixture = turn_mcp_invoker_fixture().await;
        let context = json!({
            "version": 3,
            "initiating_principal_id": "P0000000000000000000A",
            "initiating_session_id": "S0000000000000000000A",
            "workspace_id": fixture.workspace_id.as_str(),
            "root_thread_id": fixture.thread_id.as_str(),
            "policy_revision": 1,
            "role_key": "member",
            "policy_fingerprint": crate::authorization::RoleDefinitionRegistry::new()
                .policy_fingerprint(),
            "capability_projection_fingerprint": "a".repeat(64),
            "permission_profile_cap": pioneer_protocol::task_permission_cap_for_mode(
                TurnPermissionMode::Supervised,
            ),
            "grant_manifest": {
                "version": 1,
                "entry_point": "interactive_turn",
                "operational_projection_fingerprint": "c".repeat(64),
                "actions": ["agent_turn_start", "mcp_use", "provider_use"],
                "provider": {
                    "provider": "test-provider",
                    "model": "test-model",
                    "authority_fingerprint": "d".repeat(64),
                },
            },
            "mcp_projection": {
                "version": MCP_TURN_PROJECTION_VERSION,
                "manifest_hash": "b".repeat(64),
            },
        })
        .to_string();
        fixture
            .crud_store
            .set_turn_execution_authorization_context(fixture.turn_id.as_str(), context.as_str())
            .await
            .expect("spoofed execution projection should persist for the negative test");

        let error = fixture
            .invoker
            .invoke(native_mcp_invocation(&fixture), CancellationToken::new())
            .await
            .expect_err("projection not bound to execution must fail before execution");
        assert_eq!(
            error.code,
            TurnMcpInvocationErrorCode::ProjectionUnavailable
        );
        assert_eq!(fixture.execution.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn inactive_turn_mcp_invocation_fails_before_execution() {
        let fixture = turn_mcp_invoker_fixture().await;
        fixture
            .crud_store
            .update_turn_status(
                fixture.thread_id.as_str(),
                fixture.turn_id.as_str(),
                TurnStatus::Completed,
                None,
                1_700_000_100,
            )
            .await
            .expect("test turn status should update");

        let error = fixture
            .invoker
            .invoke(native_mcp_invocation(&fixture), CancellationToken::new())
            .await
            .expect_err("terminal turn must fail before execution");
        assert_eq!(error.code, TurnMcpInvocationErrorCode::TurnNotActive);
        assert_eq!(fixture.execution.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_drift_schema_change_fails_before_execution() {
        let fixture = turn_mcp_invoker_fixture().await;
        let binding = fixture
            .crud_store
            .list_turn_mcp_bindings(fixture.turn_id.as_str())
            .await
            .expect("binding lookup should succeed")
            .remove(0);
        let mut catalog = fixture
            .crud_store
            .find_mcp_server_catalog_snapshot(binding.server_installation_id.as_str())
            .await
            .expect("catalog lookup should succeed")
            .expect("catalog should exist");
        catalog.tools_json = json!([{
            "name": "send",
            "inputSchema": {
                "type": "object",
                "properties": {"changed": {"type": "boolean"}}
            }
        }])
        .to_string();
        fixture
            .crud_store
            .upsert_mcp_server_catalog_snapshot(&catalog, 1_700_000_100)
            .await
            .expect("drifted catalog should persist");

        let error = fixture
            .invoker
            .invoke(native_mcp_invocation(&fixture), CancellationToken::new())
            .await
            .expect_err("schema drift must fail before execution");
        assert_eq!(error.code, TurnMcpInvocationErrorCode::ToolDrift);
        assert_eq!(fixture.execution.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_drift_runtime_generation_change_fails_before_execution() {
        let fixture = turn_mcp_invoker_fixture().await;
        fixture
            .service
            .restart_server("workspace", fixture.workspace_id.as_str(), "resend")
            .await
            .expect("runtime restart should succeed")
            .expect("runtime installation should exist");
        let binding = fixture
            .crud_store
            .list_turn_mcp_bindings(fixture.turn_id.as_str())
            .await
            .expect("binding lookup should succeed")
            .remove(0);
        wait_for_runtime_state(
            &fixture.service,
            fixture.workspace_id.as_str(),
            binding.server_installation_id.as_str(),
            DomainRuntimeState::Ready,
        )
        .await;

        let error = fixture
            .invoker
            .invoke(native_mcp_invocation(&fixture), CancellationToken::new())
            .await
            .expect_err("runtime generation drift must fail before execution");
        assert_eq!(error.code, TurnMcpInvocationErrorCode::ToolDrift);
        assert_eq!(fixture.execution.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_invoker_binding_rejects_stale_cli_session_generation() {
        let fixture = turn_mcp_invoker_fixture().await;
        let now = chrono::Utc::now().fixed_offset();
        fixture
            .crud_store
            .upsert_cli_runtime_turn_binding(NewCliRuntimeTurnBinding {
                turn_id: fixture.turn_id.clone(),
                thread_id: fixture.thread_id.clone(),
                continuation_thread_id: fixture.thread_id.clone(),
                workspace_id: fixture.workspace_id.clone(),
                runtime_id: "runtime-codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "native-thread".to_owned(),
                native_turn_id: Some("native-turn".to_owned()),
                request_id: Some("request-id".to_owned()),
                status: "running".to_owned(),
                model: None,
                cwd: None,
                sandbox_json: None,
                approval_policy: None,
                input_mapping_json: "{}".to_owned(),
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("CLI turn binding should persist");
        fixture
            .crud_store
            .set_cli_runtime_turn_mcp_metadata(
                fixture.turn_id.as_str(),
                Some(CliRuntimeTurnMcpMetadata {
                    adapter_kind: "codex_dynamic_tools".to_owned(),
                    manifest_hash: fixture.manifest_hash.clone(),
                    projection_fingerprint: "projection-fingerprint".to_owned(),
                    provider_contract_fingerprint: "provider-fingerprint".to_owned(),
                    isolation_contract_fingerprint: "isolation-fingerprint".to_owned(),
                    session_generation: 2,
                    projection_activation_generation: 0,
                }),
            )
            .await
            .expect("CLI MCP metadata should persist");
        let mut invocation = native_mcp_invocation(&fixture);
        invocation.origin = TurnMcpInvocationOrigin::CliFacade;
        invocation.runtime_id = Some("runtime-codex".to_owned());
        invocation.session_generation = Some(1);

        let error = fixture
            .invoker
            .invoke(invocation, CancellationToken::new())
            .await
            .expect_err("stale CLI session generation must fail before execution");
        assert_eq!(
            error.code,
            TurnMcpInvocationErrorCode::SessionGenerationStale
        );
        assert_eq!(fixture.execution.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn managed_apply_patch_permission_denial_keeps_the_canonical_v1_shape() {
        let output =
            canonical_apply_patch_permission_denied_result("denied by policy", "supervised")
                .expect("canonical permission result");
        let structured = output
            .structured_content
            .as_ref()
            .expect("apply_patch denials must retain structured content");

        assert!(output.is_error);
        assert_eq!(structured["schema_version"], 1);
        assert_eq!(structured["status"], "rejected");
        assert_eq!(structured["success"], false);
        assert_eq!(structured["exact"], true);
        assert_eq!(structured["changed_files"], json!([]));
        assert_eq!(structured["error"]["stage"], "authorize");
        assert_eq!(structured["error"]["code"], "permission_denied");
        assert_eq!(
            output
                .meta
                .as_ref()
                .and_then(|meta| meta.pointer("/pioneer/reasonCode"))
                .and_then(JsonValue::as_str),
            Some("permission_denied")
        );

        let capacity = canonical_apply_patch_history_capacity_result("supervised")
            .expect("canonical history-capacity result");
        let structured = capacity.structured_content.as_ref().unwrap();
        assert!(capacity.is_error);
        assert_eq!(structured["schema_version"], 1);
        assert_eq!(structured["status"], "rejected");
        assert_eq!(structured["exact"], true);
        assert_eq!(structured["error"]["stage"], "record");
        assert_eq!(structured["error"]["code"], "history_capacity");
        assert_eq!(
            capacity
                .meta
                .as_ref()
                .and_then(|meta| meta.pointer("/pioneer/reasonCode"))
                .and_then(JsonValue::as_str),
            Some("history_capacity")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_mcp_execution_parity_preserves_canonical_rich_result() {
        let fixture = turn_mcp_invoker_fixture().await;
        let api_materialization = fixture
            .service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id: fixture.workspace_id.clone(),
                turn_id: fixture.turn_id.clone(),
                explicit_servers: Vec::new(),
                explicit_tools: vec![tool_ref("mcp-tool:workspace:resend:send", "resend", "send")],
            })
            .await
            .expect("native API MCP materialization should succeed");
        let security = fixture
            .crud_store
            .get_turn_execution_security_snapshot(fixture.turn_id.as_str())
            .await
            .expect("native API security snapshot should load")
            .expect("native API security snapshot should exist")
            .snapshot;
        let api_tools = pioneer_tools::build_tools_with_environment_and_security_snapshot(
            std::path::PathBuf::from(security.sandbox.cwd.clone()),
            fixture.turn_id.clone(),
            pioneer_tools::PermissionEvaluationContext::for_turn(
                fixture.workspace_id.clone(),
                fixture.thread_id.clone(),
                fixture.turn_id.clone(),
                security.permission_profile.clone(),
            ),
            pioneer_tools::WebToolsConfig::default(),
            pioneer_tools::ComputerUseToolsConfig::default(),
            api_materialization.bundles,
            BTreeMap::new(),
            Some(security),
        )
        .expect("native API shared tool runtime should build");
        let api_call = api_tools
            .router
            .build_tool_call(pioneer_tools::RawToolCall {
                call_id: "provider-call-api-reference".to_owned(),
                tool_name: fixture.callable_name.clone(),
                arguments: json!({"secret": "argument-canary-must-not-enter-errors"}).to_string(),
            })
            .expect("native API MCP call should build");
        let api_result = api_tools
            .runtime
            .execute_tool_call_with_cancellation(api_call, CancellationToken::new())
            .await
            .expect("native API MCP call should execute");
        let api_projection = api_result
            .projection()
            .expect("native API MCP result should have a typed projection")
            .llm_payload();
        let api_output = canonical_result_from_tool_output(&api_projection)
            .expect("native API MCP result should have the canonical shape");

        let output = fixture
            .service
            .turn_mcp_invoker()
            .invoke(native_mcp_invocation(&fixture), CancellationToken::new())
            .await
            .expect("shared invoker should execute through the native MCP tool machinery");

        assert_eq!(
            output, api_output,
            "native API projected MCP output: {api_projection}"
        );
        assert_eq!(
            output.content,
            json!([{"type":"text","text":"called send"}])
        );
        assert_eq!(
            output.structured_content,
            Some(json!({
                "tool": "send",
                "arguments": {"secret": "argument-canary-must-not-enter-errors"},
            }))
        );
        assert!(!output.is_error);
        assert_eq!(output.duration_ms, 1);
        let output_meta = output
            .meta
            .as_ref()
            .expect("canonical MCP result should preserve safe Gateway metadata");
        assert_eq!(output_meta["pioneer"]["runtime_state"], "ready");
        assert_eq!(output_meta["pioneer"]["catalog_drift"], false);
        assert!(
            output_meta["pioneer"]["current_catalog_version"]
                .as_str()
                .is_some_and(|version| version.starts_with("sha256:"))
        );

        let audits = fixture
            .crud_store
            .list_recent_mcp_audit_event_records("resend", 10)
            .await
            .expect("MCP invocation audit should be readable");
        let outcome = audits
            .iter()
            .find(|audit| audit.action == "turn_mcp_invocation_outcome")
            .expect("shared invoker should write one correlation outcome");
        let details: JsonValue = serde_json::from_str(outcome.details_json.as_str())
            .expect("MCP invocation audit details should be valid JSON");
        assert_eq!(details["provider_call_id"], "provider-call-test");
        assert_eq!(details["origin"], "native_api");
        assert_eq!(details["manifest_hash"], fixture.manifest_hash);
        assert!(!outcome.details_json.contains("argument-canary"));
        assert_eq!(
            audits
                .iter()
                .filter(|audit| audit.action == "turn_mcp_invocation_outcome")
                .count(),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_permission_ask_uses_exactly_one_shared_mcp_approval() {
        let fixture = turn_mcp_invoker_fixture().await;
        set_invoker_permission_mode(&fixture, TurnPermissionMode::Supervised).await;
        let broker = Arc::new(CountingPermissionApprovalBroker::new(
            pioneer_tools::PermissionApprovalResolution::AllowOnce,
        ));
        fixture
            .service
            .set_permission_approval_broker(broker.clone())
            .await;

        let output = fixture
            .service
            .turn_mcp_invoker()
            .invoke(native_mcp_invocation(&fixture), CancellationToken::new())
            .await
            .expect("approved MCP invocation should execute");

        assert!(!output.is_error);
        assert_eq!(broker.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_permission_user_deny_returns_canonical_error_without_second_approval() {
        let fixture = turn_mcp_invoker_fixture().await;
        set_invoker_permission_mode(&fixture, TurnPermissionMode::Supervised).await;
        let broker = Arc::new(CountingPermissionApprovalBroker::new(
            pioneer_tools::PermissionApprovalResolution::Deny {
                message: "denied by test user".to_owned(),
            },
        ));
        fixture
            .service
            .set_permission_approval_broker(broker.clone())
            .await;

        let output = fixture
            .service
            .turn_mcp_invoker()
            .invoke(native_mcp_invocation(&fixture), CancellationToken::new())
            .await
            .expect("permission denial should be a canonical MCP tool result");

        assert!(output.is_error);
        assert_eq!(output.content[0]["text"], "denied by test user");
        assert_eq!(
            output.meta.as_ref().and_then(|meta| meta
                .pointer("/pioneer/reasonCode")
                .and_then(JsonValue::as_str)),
            Some("permission_denied")
        );
        assert_eq!(broker.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_permission_approval_cancel_is_typed_and_stops_execution() {
        let fixture = turn_mcp_invoker_fixture().await;
        set_invoker_permission_mode(&fixture, TurnPermissionMode::Supervised).await;
        let broker = Arc::new(CountingPermissionApprovalBroker::new(
            pioneer_tools::PermissionApprovalResolution::Cancelled,
        ));
        fixture
            .service
            .set_permission_approval_broker(broker.clone())
            .await;

        let error = fixture
            .service
            .turn_mcp_invoker()
            .invoke(native_mcp_invocation(&fixture), CancellationToken::new())
            .await
            .expect_err("cancelled approval must stop MCP execution");

        assert_eq!(error.code, TurnMcpInvocationErrorCode::Cancelled);
        assert_eq!(broker.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_permission_policy_deny_bypasses_approval_and_upstream() {
        let fixture = turn_mcp_invoker_fixture().await;
        let mut snapshot =
            set_invoker_permission_mode(&fixture, TurnPermissionMode::Supervised).await;
        snapshot
            .permission_profile
            .effective_policy
            .mcp_write_or_unknown = PermissionBehavior::Deny;
        fixture
            .crud_store
            .set_turn_execution_security_snapshot(fixture.turn_id.as_str(), &snapshot)
            .await
            .expect("denying MCP security snapshot should persist");
        let broker = Arc::new(CountingPermissionApprovalBroker::new(
            pioneer_tools::PermissionApprovalResolution::AllowOnce,
        ));
        fixture
            .service
            .set_permission_approval_broker(broker.clone())
            .await;

        let output = fixture
            .service
            .turn_mcp_invoker()
            .invoke(native_mcp_invocation(&fixture), CancellationToken::new())
            .await
            .expect("policy denial should be a canonical MCP tool result");

        assert!(output.is_error);
        assert_eq!(broker.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_permission_network_policy_denies_before_upstream() {
        let fixture = turn_mcp_invoker_fixture().await;
        let mut snapshot =
            set_invoker_permission_mode(&fixture, TurnPermissionMode::FullAccess).await;
        snapshot.network = TurnNetworkPolicySnapshot::disabled();
        snapshot.sandbox.network = TurnNetworkPolicySnapshot::disabled();
        snapshot.authority_cap.network = TurnNetworkPolicySnapshot::disabled();
        fixture
            .crud_store
            .set_turn_execution_security_snapshot(fixture.turn_id.as_str(), &snapshot)
            .await
            .expect("network-denying MCP security snapshot should persist");
        let persisted = fixture
            .crud_store
            .get_turn_execution_security_snapshot(fixture.turn_id.as_str())
            .await
            .expect("network-denying MCP security snapshot should be readable")
            .expect("network-denying MCP security snapshot should exist");
        assert_eq!(
            persisted.snapshot.network.mode,
            pioneer_protocol::TurnNetworkMode::Disabled
        );
        let broker = Arc::new(CountingPermissionApprovalBroker::new(
            pioneer_tools::PermissionApprovalResolution::AllowOnce,
        ));
        fixture
            .service
            .set_permission_approval_broker(broker.clone())
            .await;

        let output = fixture
            .service
            .turn_mcp_invoker()
            .invoke(native_mcp_invocation(&fixture), CancellationToken::new())
            .await
            .expect("network denial should be a canonical MCP tool result");

        assert!(output.is_error, "unexpected MCP output: {output:?}");
        assert_eq!(broker.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_cancel_while_approval_is_pending_stops_before_upstream() {
        let fixture = turn_mcp_invoker_fixture().await;
        set_invoker_permission_mode(&fixture, TurnPermissionMode::Supervised).await;
        let broker = Arc::new(BlockingPermissionApprovalBroker::default());
        fixture
            .service
            .set_permission_approval_broker(broker.clone())
            .await;

        let cancellation = CancellationToken::new();
        let invocation_cancellation = cancellation.clone();
        let service = fixture.service.clone();
        let invocation = native_mcp_invocation(&fixture);
        let invocation_task = tokio::spawn(async move {
            service
                .turn_mcp_invoker()
                .invoke(invocation, invocation_cancellation)
                .await
        });

        wait_for_test_notification(&broker.started, "pending MCP approval").await;
        cancellation.cancel();
        let error = tokio::time::timeout(Duration::from_secs(2), invocation_task)
            .await
            .expect("cancelled MCP approval should finish promptly")
            .expect("cancelled MCP approval task should not panic")
            .expect_err("cancelled MCP approval must not execute upstream");

        assert_eq!(error.code, TurnMcpInvocationErrorCode::Cancelled);
        assert_eq!(broker.calls.load(Ordering::SeqCst), 1);
        assert_turn_mcp_outcome_reason(&fixture, "cancelled").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_cancel_reaches_upstream_and_late_release_cannot_succeed() {
        let control = Arc::new(PausingMcpControl::default());
        let fixture =
            turn_mcp_invoker_fixture_with_connector(Arc::new(PausingMcpRuntimeConnector {
                control: control.clone(),
                catalog_timeout_ms: 5_000,
            }))
            .await;
        let cancellation = CancellationToken::new();
        let invocation_cancellation = cancellation.clone();
        let service = fixture.service.clone();
        let invocation = native_mcp_invocation(&fixture);
        let invocation_task = tokio::spawn(async move {
            service
                .turn_mcp_invoker()
                .invoke(invocation, invocation_cancellation)
                .await
        });

        wait_for_test_notification(&control.started, "upstream MCP request").await;
        cancellation.cancel();
        wait_for_test_notification(&control.cancelled, "upstream MCP cancellation").await;
        let error = tokio::time::timeout(Duration::from_secs(2), invocation_task)
            .await
            .expect("cancelled upstream MCP request should finish promptly")
            .expect("cancelled upstream MCP task should not panic")
            .expect_err("cancelled upstream MCP request must not succeed");
        control.released.notify_one();
        tokio::task::yield_now().await;

        assert_eq!(error.code, TurnMcpInvocationErrorCode::Cancelled);
        assert_eq!(control.calls.load(Ordering::SeqCst), 1);
        assert_turn_mcp_outcome_reason(&fixture, "cancelled").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_cancel_dropped_outer_future_does_not_orphan_upstream() {
        let control = Arc::new(PausingMcpControl::default());
        let fixture =
            turn_mcp_invoker_fixture_with_connector(Arc::new(PausingMcpRuntimeConnector {
                control: control.clone(),
                catalog_timeout_ms: 5_000,
            }))
            .await;
        let service = fixture.service.clone();
        let invocation = native_mcp_invocation(&fixture);
        let invocation_task = tokio::spawn(async move {
            service
                .turn_mcp_invoker()
                .invoke(invocation, CancellationToken::new())
                .await
        });

        wait_for_test_notification(&control.started, "drop-guard MCP request").await;
        invocation_task.abort();
        let task_error = invocation_task
            .await
            .expect_err("aborted outer MCP future should report task cancellation");
        assert!(task_error.is_cancelled());
        wait_for_test_notification(&control.cancelled, "drop-guard upstream cancellation").await;
        control.released.notify_one();
        tokio::task::yield_now().await;

        assert_eq!(control.calls.load(Ordering::SeqCst), 1);
        let audits = fixture
            .crud_store
            .list_recent_mcp_audit_event_records("resend", 20)
            .await
            .expect("MCP invocation audit should be readable");
        assert!(
            audits
                .iter()
                .all(|audit| audit.action != "turn_mcp_invocation_outcome")
        );
        assert!(
            audits
                .iter()
                .all(|audit| audit.action != "tool_call_completed")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_cancel_terminal_turn_revokes_active_upstream() {
        let control = Arc::new(PausingMcpControl::default());
        let fixture =
            turn_mcp_invoker_fixture_with_connector(Arc::new(PausingMcpRuntimeConnector {
                control: control.clone(),
                catalog_timeout_ms: 5_000,
            }))
            .await;
        let service = fixture.service.clone();
        let invocation = native_mcp_invocation(&fixture);
        let invocation_task = tokio::spawn(async move {
            service
                .turn_mcp_invoker()
                .invoke(invocation, CancellationToken::new())
                .await
        });

        wait_for_test_notification(&control.started, "terminal-turn MCP request").await;
        assert!(
            fixture
                .service
                .cancel_turn_mcp_invocations(fixture.turn_id.as_str())
                >= 1
        );
        wait_for_test_notification(&control.cancelled, "terminal-turn upstream cancellation").await;
        let error = tokio::time::timeout(Duration::from_secs(2), invocation_task)
            .await
            .expect("terminal-turn MCP cancellation should finish promptly")
            .expect("terminal-turn MCP task should not panic")
            .expect_err("terminal-turn MCP request must not succeed");

        assert_eq!(error.code, TurnMcpInvocationErrorCode::Cancelled);
        assert_turn_mcp_outcome_reason(&fixture, "cancelled").await;
    }

    #[tokio::test]
    async fn poisoned_active_invocation_registry_still_fails_closed_on_revoke() {
        let (service, _, _) = test_mcp_service().await;
        let inner = service.inner.clone();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = inner
                .active_invocations
                .lock()
                .expect("registry should begin healthy");
            panic!("poison active invocation registry");
        }));

        let cancellation = CancellationToken::new();
        let _guard = service.register_active_mcp_invocation(
            "turn-poisoned-registry",
            "installation-poisoned-registry",
            "call-poisoned-registry",
            cancellation.clone(),
        );
        assert_eq!(
            service.cancel_turn_mcp_invocations("turn-poisoned-registry"),
            1
        );
        assert!(cancellation.is_cancelled());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_cancel_runtime_revoke_reaches_active_upstream() {
        let control = Arc::new(PausingMcpControl::default());
        let fixture =
            turn_mcp_invoker_fixture_with_connector(Arc::new(PausingMcpRuntimeConnector {
                control: control.clone(),
                catalog_timeout_ms: 5_000,
            }))
            .await;
        let binding = fixture
            .crud_store
            .list_turn_mcp_bindings(fixture.turn_id.as_str())
            .await
            .expect("MCP binding lookup should succeed")
            .remove(0);
        let service = fixture.service.clone();
        let invocation = native_mcp_invocation(&fixture);
        let invocation_task = tokio::spawn(async move {
            service
                .turn_mcp_invoker()
                .invoke(invocation, CancellationToken::new())
                .await
        });

        wait_for_test_notification(&control.started, "runtime-revoke MCP request").await;
        fixture
            .service
            .stop_task(
                binding.server_installation_id.as_str(),
                DomainRuntimeState::Stopped,
            )
            .await;
        wait_for_test_notification(&control.cancelled, "runtime-revoke upstream cancellation")
            .await;
        let error = tokio::time::timeout(Duration::from_secs(2), invocation_task)
            .await
            .expect("runtime-revoked MCP request should finish promptly")
            .expect("runtime-revoked MCP task should not panic")
            .expect_err("runtime-revoked MCP request must not succeed");

        assert_eq!(error.code, TurnMcpInvocationErrorCode::Cancelled);
        assert_turn_mcp_outcome_reason(&fixture, "cancelled").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_cancel_gateway_shutdown_reaches_active_upstream() {
        let control = Arc::new(PausingMcpControl::default());
        let fixture =
            turn_mcp_invoker_fixture_with_connector(Arc::new(PausingMcpRuntimeConnector {
                control: control.clone(),
                catalog_timeout_ms: 5_000,
            }))
            .await;
        let service = fixture.service.clone();
        let invocation = native_mcp_invocation(&fixture);
        let invocation_task = tokio::spawn(async move {
            service
                .turn_mcp_invoker()
                .invoke(invocation, CancellationToken::new())
                .await
        });

        wait_for_test_notification(&control.started, "shutdown MCP request").await;
        fixture.service.shutdown().await;
        wait_for_test_notification(&control.cancelled, "shutdown upstream cancellation").await;
        let error = tokio::time::timeout(Duration::from_secs(2), invocation_task)
            .await
            .expect("shutdown MCP request should finish promptly")
            .expect("shutdown MCP task should not panic")
            .expect_err("shutdown MCP request must not succeed");

        assert_eq!(error.code, TurnMcpInvocationErrorCode::Cancelled);
        assert_turn_mcp_outcome_reason(&fixture, "cancelled").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_timeout_is_typed_and_cancels_the_active_upstream_request() {
        let control = Arc::new(PausingMcpControl::default());
        let fixture =
            turn_mcp_invoker_fixture_with_connector(Arc::new(PausingMcpRuntimeConnector {
                control: control.clone(),
                catalog_timeout_ms: 25,
            }))
            .await;

        let error = fixture
            .service
            .turn_mcp_invoker()
            .invoke(native_mcp_invocation(&fixture), CancellationToken::new())
            .await
            .expect_err("timed-out MCP request must not succeed");
        wait_for_test_notification(&control.cancelled, "timed-out upstream cancellation").await;

        assert_eq!(error.code, TurnMcpInvocationErrorCode::TimedOut);
        assert_eq!(control.calls.load(Ordering::SeqCst), 1);
        assert_turn_mcp_outcome_reason(&fixture, "timed_out").await;
    }

    struct RefreshFailureConnector {
        connects: Arc<AtomicUsize>,
        change: Arc<tokio::sync::Notify>,
        unrelated_degradation: bool,
        tool_barrier: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
        tool_outcome: Option<Result<(), McpRuntimeError>>,
        provider: Option<pioneer_mcp_oauth::McpOAuthService>,
    }
    struct RefreshFailureSession {
        inner: Box<dyn pioneer_mcp::McpRuntimeSession>,
        change: Arc<tokio::sync::Notify>,
        unrelated_degradation: bool,
        tool_barrier: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
        tool_outcome: Option<Result<(), McpRuntimeError>>,
        provider: Option<(
            pioneer_mcp_oauth::McpOAuthService,
            McpServerInstallation,
            String,
            pioneer_mcp::OAuthHttpClient,
        )>,
    }
    #[async_trait::async_trait]
    impl McpRuntimeConnector for RefreshFailureConnector {
        async fn connect(
            &self,
            installation: McpServerInstallation,
            id: String,
            resolver: Arc<dyn McpSecretResolver>,
            now: i64,
        ) -> Result<Box<dyn pioneer_mcp::McpRuntimeSession>, McpRuntimeError> {
            self.connects.fetch_add(1, Ordering::SeqCst);
            let provider = if let Some(owner) = &self.provider {
                pioneer_mcp::McpOAuthProvider::client(owner, &id, &installation)
                    .await?
                    .map(|client| (owner.clone(), installation.clone(), id.clone(), client))
            } else {
                None
            };
            let inner = TestMcpRuntimeConnector {
                tools: vec!["send"],
                fail_auth: false,
            }
            .connect(installation, id, resolver, now)
            .await?;
            Ok(Box::new(RefreshFailureSession {
                inner,
                change: self.change.clone(),
                unrelated_degradation: self.unrelated_degradation,
                tool_barrier: self.tool_barrier.clone(),
                tool_outcome: self.tool_outcome.clone(),
                provider,
            }))
        }
    }
    #[async_trait::async_trait]
    impl pioneer_mcp::McpRuntimeSession for RefreshFailureSession {
        fn initial_catalog(&self) -> &McpCatalogSnapshot {
            self.inner.initial_catalog()
        }
        fn degraded_reason(&self) -> Option<&str> {
            self.unrelated_degradation
                .then_some("unrelated stream limitation")
        }
        async fn wait_for_event(&mut self) -> pioneer_mcp::McpSessionEvent {
            self.change.notified().await;
            pioneer_mcp::McpSessionEvent::CatalogChanged
        }
        async fn refresh_catalog(&mut self) -> Result<McpCatalogSnapshot, McpRuntimeError> {
            Err(McpRuntimeError {
                oauth_failure: None,
                kind: McpRuntimeErrorKind::TransientRefresh,
                state: DomainRuntimeState::Failed,
                message: "OAuth token refresh temporarily unavailable".into(),
            })
        }
        async fn call_tool(
            &mut self,
            name: &str,
            args: JsonValue,
            budget: pioneer_mcp::McpInvocationBudget,
            timeout: Duration,
            cancellation: tokio_util::sync::CancellationToken,
        ) -> Result<McpToolCallResult, McpRuntimeError> {
            if let Some((entered, release)) = &self.tool_barrier {
                let notified_error = if let Some((owner, installation, id, client)) = &self.provider
                {
                    let cause = pioneer_mcp::McpOAuthProvider::transient_failure_from_session(
                        owner,
                        id,
                        installation,
                        Some(client),
                    )
                    .await;
                    let mut error = McpRuntimeError::failed("notified OAuth failure A");
                    error.kind = McpRuntimeErrorKind::TransientRefresh;
                    error.oauth_failure = cause;
                    Some(error)
                } else {
                    None
                };
                entered.notify_one();
                release.notified().await;
                if let Some(error) = notified_error {
                    return Err(error);
                }
                match &self.tool_outcome {
                    Some(Ok(())) => {}
                    Some(Err(error)) => return Err(error.clone()),
                    None => {
                        return Err(McpRuntimeError {
                            oauth_failure: None,
                            kind: McpRuntimeErrorKind::TransientRefresh,
                            state: DomainRuntimeState::Failed,
                            message: "new OAuth failure B".into(),
                        });
                    }
                }
            }
            self.inner
                .call_tool(name, args, budget, timeout, cancellation)
                .await
        }
        async fn shutdown_result(&mut self) -> Result<(), pioneer_mcp::McpRuntimeError> {
            self.shutdown().await;
            Ok(())
        }

        async fn shutdown(&mut self) {
            self.inner.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn queued_oauth_recovery_cannot_clear_a_newer_blocked_tool_failure() {
        let (service, store, workspace) = test_mcp_service().await;
        let change = Arc::new(tokio::sync::Notify::new());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        struct Release(Arc<tokio::sync::Notify>);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.notify_waiters();
                self.0.notify_one();
            }
        }
        let _release_on_exit = Release(release.clone());
        let connects = Arc::new(AtomicUsize::new(0));
        service.set_connector_for_tests(Arc::new(RefreshFailureConnector {
            connects: connects.clone(),
            change: change.clone(),
            unrelated_degradation: false,
            tool_barrier: Some((entered.clone(), release.clone())),
            tool_outcome: None,
            provider: None,
        }));
        let id = seed_mcp_installation(&store, &workspace, "queued-recovery", true, false).await;
        service.reload_workspace(&workspace).await.unwrap();
        wait_for_runtime_state(&service, &workspace, &id, DomainRuntimeState::Ready).await;
        let generation =
            service.runtime_snapshot("workspace", &workspace).await[&id].runtime_generation;
        change.notify_one();
        wait_for_runtime_state(&service, &workspace, &id, DomainRuntimeState::Degraded).await;
        let tx = service.inner.tasks.lock().await[&id].call_tx.clone();
        let revision = service.inner.tasks.lock().await[&id]
            .oauth_failure_revision
            .load(Ordering::SeqCst);
        assert!(revision > 0);
        let (response_tx, response_rx) = oneshot::channel();
        tx.send(McpServerCommand::Tool(McpServerCallCommand {
            request: pioneer_tools::McpToolCallRequest {
                workspace_id: workspace.clone(),
                turn_id: "test-turn".into(),
                call_id: "tool-B".into(),
                callable_name: "send".into(),
                server_id: id.clone(),
                server_name: "queued-recovery".into(),
                raw_tool_name: "send".into(),
                catalog_version: "test".into(),
                arguments: json!({}),
                timeout_ms: 5000,
                max_arguments_bytes: 1024,
            },
            cancellation: CancellationToken::new(),
            response_tx,
        }))
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(3), entered.notified())
            .await
            .unwrap();
        let (done, completed) = oneshot::channel();
        tx.send(McpServerCommand::OAuthRecovered {
            store: Arc::new(store.with_maintenance_access()),
            cancellation: CancellationToken::new(),
            completed: done,
            admission: Arc::new(std::sync::Mutex::new(None)),
            event: None,
            failure_revision: revision,
            cause: None,
        })
        .await
        .unwrap();
        release.notify_one();
        assert_eq!(
            response_rx.await.unwrap().unwrap_err().message,
            "new OAuth failure B"
        );
        completed.await.unwrap();
        let after = service.runtime_snapshot("workspace", &workspace).await[&id].clone();
        assert_eq!(after.state, DomainRuntimeState::Degraded);
        assert_eq!(after.last_error.as_deref(), Some("new OAuth failure B"));
        assert_eq!(after.runtime_generation, generation);
        service.oauth_recovered(&id).await;
        assert_eq!(
            service.runtime_snapshot("workspace", &workspace).await[&id].state,
            DomainRuntimeState::Ready
        );
        assert_eq!(connects.load(Ordering::SeqCst), 1);
        service.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn real_oauth_recovery_before_actor_receives_notified_error_restores_projection() {
        use axum::{
            Json, Router,
            routing::{get, post},
        };
        for unrelated in [false, true] {
            let (old, store, workspace, secrets) = test_mcp_service_with_secrets().await;
            old.shutdown().await;
            let sessions = Arc::new(SessionManager::new());
            let (recipient_tx, mut recipient) = mpsc::channel(1024);
            let mut principal = (*authenticated_test_superuser()).clone();
            principal.access_expires_at_unix = now_timestamp_secs() as u64 + 3600;
            let client = sessions
                .register_connection(recipient_tx, Arc::new(principal))
                .await
                .unwrap();
            let service = McpService::new_with_oauth_options(
                store.clone(),
                sessions,
                secrets,
                Arc::new(AtomicU64::new(1)),
                Arc::new(AuthorizationInvalidationHub::default()),
                Arc::new(ExecutionLeaseRegistry::default()),
                pioneer_mcp_oauth::OAuthServiceOptions {
                    poll_interval: Duration::from_millis(10),
                    ..Default::default()
                },
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let issuer = format!("http://{}", listener.local_addr().unwrap());
            let metadata = json!({"issuer":issuer,"authorization_endpoint":format!("{issuer}/authorize"),"token_endpoint":format!("{issuer}/token"),"registration_endpoint":format!("{issuer}/register"),"response_types_supported":["code"],"code_challenge_methods_supported":["S256"],"authorization_response_iss_parameter_supported":true});
            let router=Router::new().route("/.well-known/oauth-authorization-server",get(move||{let metadata=metadata.clone();async move{Json(metadata)}}))
                .route("/register",post(|Json(body):Json<JsonValue>|async move{Json(json!({"client_id":"real-recovery-client","redirect_uris":body["redirect_uris"]}))}))
                .route("/token",post(||async{Json(json!({"access_token":"recovered-token","refresh_token":"rotated-token","token_type":"Bearer","expires_in":3600}))}));
            let server = tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            });
            struct Server(tokio::task::JoinHandle<()>);
            impl Drop for Server {
                fn drop(&mut self) {
                    self.0.abort();
                }
            }
            let _server = Server(server);
            let id = seed_mcp_installation(&store, &workspace, "real-recovery", true, false).await;
            let mut row = store
                .find_mcp_server_installation("workspace", &workspace, "real-recovery")
                .await
                .unwrap()
                .unwrap();
            row.transport_kind = "streamable_http".into();
            row.transport_json = serde_json::to_string(&McpTransportConfig::StreamableHttp {
                url: format!("{issuer}/mcp"),
                headers: BTreeMap::new(),
                startup_timeout_ms: 5000,
                tool_timeout_ms: 120000,
            })
            .unwrap();
            store
                .upsert_mcp_server_installation(&row, now_timestamp_secs())
                .await
                .unwrap();
            let installation = installation_from_record(&row).unwrap();
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            struct Release(Arc<tokio::sync::Notify>);
            impl Drop for Release {
                fn drop(&mut self) {
                    self.0.notify_waiters();
                    self.0.notify_one();
                }
            }
            let _release = Release(release.clone());
            let connects = Arc::new(AtomicUsize::new(0));
            service.set_connector_for_tests(Arc::new(RefreshFailureConnector {
                connects: connects.clone(),
                change: Arc::new(tokio::sync::Notify::new()),
                unrelated_degradation: unrelated,
                tool_barrier: Some((entered.clone(), release.clone())),
                tool_outcome: None,
                provider: Some(service.oauth().clone()),
            }));
            service
                .oauth()
                .sign_in(
                    &id,
                    &installation,
                    client,
                    "http://127.0.0.1:37643/oauth/mcp/callback",
                )
                .await
                .unwrap();
            let browser = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let axum::extract::ws::Message::Text(text) = recipient.recv().await.unwrap()
                    else {
                        continue;
                    };
                    let value: JsonValue = serde_json::from_str(text.as_str()).unwrap();
                    if value["params"]["state"] == "awaiting_callback" {
                        break value["params"].clone();
                    }
                }
            })
            .await
            .unwrap();
            let url = url::Url::parse(browser["authorization_url"].as_str().unwrap()).unwrap();
            let state = url
                .query_pairs()
                .find(|(key, _)| key == "state")
                .unwrap()
                .1
                .into_owned();
            service
                .oauth()
                .callback(
                    &id,
                    client,
                    pioneer_mcp_oauth::OAuthCallback {
                        flow_id: browser["flow_id"].as_str().unwrap().into(),
                        state,
                        code: Some("single-code".into()),
                        issuer: Some(issuer),
                        error: None,
                    },
                )
                .await
                .unwrap();
            let expected = if unrelated {
                DomainRuntimeState::Degraded
            } else {
                DomainRuntimeState::Ready
            };
            wait_for_runtime_state(&service, &workspace, &id, expected).await;
            let generation =
                service.runtime_snapshot("workspace", &workspace).await[&id].runtime_generation;
            let (tx, mailbox) = {
                let tasks = service.inner.tasks.lock().await;
                (tasks[&id].call_tx.clone(), tasks[&id].recovery.clone())
            };
            let (response_tx, response) = oneshot::channel();
            tx.send(McpServerCommand::Tool(McpServerCallCommand {
                request: pioneer_tools::McpToolCallRequest {
                    workspace_id: workspace.clone(),
                    turn_id: "test-turn".into(),
                    call_id: "tool-A".into(),
                    callable_name: "send".into(),
                    server_id: id.clone(),
                    server_name: "real-recovery".into(),
                    raw_tool_name: "send".into(),
                    catalog_version: "test".into(),
                    arguments: json!({}),
                    timeout_ms: 120000,
                    max_arguments_bytes: 1024,
                },
                cancellation: CancellationToken::new(),
                response_tx,
            }))
            .await
            .unwrap();
            tokio::time::timeout(Duration::from_secs(3), entered.notified())
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if mailbox.pending.lock().unwrap().is_some() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                service.inner.tasks.lock().await[&id]
                    .oauth_failure_revision
                    .load(Ordering::SeqCst),
                0,
                "error A has not reached actor yet"
            );
            release.notify_one();
            assert_eq!(
                response.await.unwrap().unwrap_err().message,
                "notified OAuth failure A"
            );
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let snapshot =
                        service.runtime_snapshot("workspace", &workspace).await[&id].clone();
                    if mailbox.pending.lock().unwrap().is_none() && snapshot.last_error.is_none() {
                        assert_eq!(snapshot.state, expected);
                        assert_eq!(snapshot.runtime_generation, generation);
                        if unrelated {
                            assert_eq!(
                                snapshot.status_reason.as_deref(),
                                Some("unrelated stream limitation")
                            );
                        }
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(connects.load(Ordering::SeqCst), 1);
            service.shutdown().await;
        }
    }
    #[cfg(feature = "oauth-test-support")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn retired_winner_delivers_resolving_and_addressed_reset_through_gateway_consumer() {
        use axum::{
            Json, Router,
            routing::{get, post},
        };
        for deadline in [false, true] {
            let (old, store, workspace, secrets) = test_mcp_service_with_secrets().await;
            old.shutdown().await;
            let sessions = Arc::new(SessionManager::new());
            let (recipient_tx, mut recipient) = mpsc::channel(1024);
            let mut principal = (*authenticated_test_superuser()).clone();
            principal.access_expires_at_unix = now_timestamp_secs() as u64 + 3600;
            let client = sessions
                .register_connection(recipient_tx, Arc::new(principal))
                .await
                .unwrap();
            let hooks = Arc::new(pioneer_mcp_oauth::OAuthTestHooks::default());
            hooks.pause_after_winner.store(true, Ordering::SeqCst);
            struct Release(Arc<pioneer_mcp_oauth::OAuthTestHooks>);
            impl Drop for Release {
                fn drop(&mut self) {
                    self.0.publish_resolution.notify_one();
                }
            }
            let _release = Release(hooks.clone());
            struct Clock(std::sync::Mutex<std::time::SystemTime>);
            impl pioneer_mcp_oauth::OAuthClock for Clock {
                fn now(&self) -> std::time::SystemTime {
                    *self.0.lock().unwrap()
                }
            }
            let clock = Arc::new(Clock(std::sync::Mutex::new(std::time::SystemTime::now())));
            let service = McpService::new_with_oauth_options(
                store.clone(),
                sessions.clone(),
                secrets,
                Arc::new(AtomicU64::new(1)),
                Arc::new(AuthorizationInvalidationHub::default()),
                Arc::new(ExecutionLeaseRegistry::default()),
                pioneer_mcp_oauth::OAuthServiceOptions {
                    test_hooks: Some(hooks.clone()),
                    clock: clock.clone(),
                    poll_interval: Duration::from_millis(5),
                    ..Default::default()
                },
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let issuer = format!("http://{}", listener.local_addr().unwrap());
            let metadata = json!({"issuer":issuer,"authorization_endpoint":format!("{issuer}/authorize"),"token_endpoint":format!("{issuer}/token"),"registration_endpoint":format!("{issuer}/register"),"response_types_supported":["code"],"code_challenge_methods_supported":["S256"],"authorization_response_iss_parameter_supported":true});
            let router=Router::new().route("/.well-known/oauth-authorization-server",get(move||{let metadata=metadata.clone();async move{Json(metadata)}}))
                .route("/register",post(|Json(body):Json<JsonValue>|async move{Json(json!({"client_id":"real-recovery-client","redirect_uris":body["redirect_uris"]}))}))
                .route("/token",post(||async{Json(json!({"access_token":"recovered-token","refresh_token":"rotated-token","token_type":"Bearer","expires_in":3600}))}));
            let server = tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            });
            struct Server(tokio::task::JoinHandle<()>);
            impl Drop for Server {
                fn drop(&mut self) {
                    self.0.abort();
                }
            }
            let _server = Server(server);
            let id = seed_mcp_installation(&store, &workspace, "real-recovery", false, false).await;
            let mut row = store
                .find_mcp_server_installation("workspace", &workspace, "real-recovery")
                .await
                .unwrap()
                .unwrap();
            row.transport_kind = "streamable_http".into();
            row.transport_json = serde_json::to_string(&McpTransportConfig::StreamableHttp {
                url: format!("{issuer}/mcp"),
                headers: BTreeMap::new(),
                startup_timeout_ms: 5000,
                tool_timeout_ms: 120000,
            })
            .unwrap();
            store
                .upsert_mcp_server_installation(&row, now_timestamp_secs())
                .await
                .unwrap();
            let installation = installation_from_record(&row).unwrap();
            service
                .oauth()
                .sign_in(
                    &id,
                    &installation,
                    client,
                    "http://127.0.0.1:37643/oauth/mcp/callback",
                )
                .await
                .unwrap();
            let browser = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let axum::extract::ws::Message::Text(text) = recipient.recv().await.unwrap()
                    else {
                        continue;
                    };
                    let value: JsonValue = serde_json::from_str(text.as_str()).unwrap();
                    if value["params"]["state"] == "awaiting_callback" {
                        break value["params"].clone();
                    }
                }
            })
            .await
            .unwrap();
            let url = url::Url::parse(browser["authorization_url"].as_str().unwrap()).unwrap();
            let state = url
                .query_pairs()
                .find(|(key, _)| key == "state")
                .unwrap()
                .1
                .into_owned();
            service
                .oauth()
                .callback(
                    &id,
                    client,
                    pioneer_mcp_oauth::OAuthCallback {
                        flow_id: browser["flow_id"].as_str().unwrap().into(),
                        state,
                        code: Some("single-code".into()),
                        issuer: Some(issuer),
                        error: None,
                    },
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(3), hooks.winner_reserved.notified())
                .await
                .unwrap();
            assert_eq!(
                service.oauth().state(&id).await,
                Some(pioneer_mcp_oauth::OAuthState::Resolving)
            );
            if deadline {
                *clock.0.lock().unwrap() += Duration::from_secs(1200);
            } else {
                sessions.unregister_connection(client).await;
            }
            tokio::time::timeout(Duration::from_secs(3), hooks.retirement_observed.notified())
                .await
                .unwrap();
            hooks.publish_resolution.notify_one();
            let flow = browser["flow_id"].as_str().unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                while !service
                    .oauth()
                    .resolution_finished_for_test(&id, flow)
                    .await
                {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            if deadline {
                async fn wire_state(
                    recipient: &mut mpsc::Receiver<axum::extract::ws::Message>,
                    state: &str,
                ) -> JsonValue {
                    tokio::time::timeout(Duration::from_secs(3), async {
                        loop {
                            if let Some(axum::extract::ws::Message::Text(text)) =
                                recipient.recv().await
                            {
                                let value: JsonValue = serde_json::from_str(text.as_str()).unwrap();
                                assert_ne!(value["params"]["state"], "authorized");
                                if value["params"]["state"] == state {
                                    break value["params"].clone();
                                }
                            }
                        }
                    })
                    .await
                    .unwrap()
                }
                let resolving = wire_state(&mut recipient, "resolving").await;
                assert_eq!(resolving["flow_id"], browser["flow_id"]);
                assert!(resolving["authorization_url"].is_null());
                let (second_tx, mut second_rx) = mpsc::channel(1024);
                let mut second_principal = (*authenticated_test_superuser()).clone();
                second_principal.access_expires_at_unix = now_timestamp_secs() as u64 + 3600;
                let second = sessions
                    .register_connection(second_tx, Arc::new(second_principal))
                    .await
                    .unwrap();
                let mut replacement = row.clone();
                let mut config = installation.clone();
                config.auth.oauth = Some(pioneer_mcp::McpOAuthConfig {
                    scopes: vec!["additional".into()],
                    ..Default::default()
                });
                replacement.auth_json = serde_json::to_string(&config.auth).unwrap();
                store
                    .upsert_mcp_server_installation(&replacement, now_timestamp_secs())
                    .await
                    .unwrap();
                service
                    .oauth()
                    .sign_in_in_workspace(
                        &id,
                        &config,
                        second,
                        "http://127.0.0.1:37643/oauth/mcp/callback",
                        &workspace,
                    )
                    .await
                    .unwrap();
                let retired = wire_state(&mut recipient, "retired").await;
                assert_eq!(retired["flow_id"], browser["flow_id"]);
                assert!(retired["authorization_url"].is_null());
                let next = wire_state(&mut second_rx, "awaiting_callback").await;
                assert_ne!(next["flow_id"], browser["flow_id"]);
                assert!(next["authorization_url"].is_string());
                while let Ok(frame) = recipient.try_recv() {
                    if let axum::extract::ws::Message::Text(text) = frame {
                        let value: JsonValue = serde_json::from_str(text.as_str()).unwrap();
                        assert!(
                            value["params"]["authorization_url"].is_null(),
                            "client A must never receive client B's consent URL"
                        );
                    }
                }
            } else {
                assert_eq!(
                    service.oauth().state(&id).await,
                    Some(pioneer_mcp_oauth::OAuthState::Resolving)
                );
                while let Ok(frame) = recipient.try_recv() {
                    if let axum::extract::ws::Message::Text(text) = frame {
                        let value: JsonValue = serde_json::from_str(text.as_str()).unwrap();
                        assert_ne!(
                            value["params"]["state"], "resolving",
                            "retirement notification cannot reach a disconnected initiator"
                        );
                        assert_ne!(value["params"]["state"], "authorized");
                    }
                }
            }
            service.shutdown().await;
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_mailbox_accepts_late_delivery_of_the_same_failure_cause() {
        for unrelated in [false, true] {
            delayed_recovery_mailbox(true, unrelated, 0).await;
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_mailbox_survives_ack_deadline_and_retires_on_replacement_or_shutdown() {
        for retire in [0, 1, 2] {
            delayed_recovery_mailbox(false, false, retire).await;
        }
    }
    async fn delayed_recovery_mailbox(same_cause: bool, unrelated: bool, retire: u8) {
        let (service, store, workspace) = test_mcp_service().await;
        let change = Arc::new(tokio::sync::Notify::new());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        struct Release(Arc<tokio::sync::Notify>);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.notify_waiters();
                self.0.notify_one();
            }
        }
        let _release = Release(release.clone());
        let cause = pioneer_mcp::OAuthFailureCause {
            generation: "manager-A".into(),
            revision: 7,
        };
        let mut error = McpRuntimeError::failed("delayed OAuth failure A");
        error.kind = McpRuntimeErrorKind::TransientRefresh;
        error.oauth_failure = Some(cause.clone());
        let connects = Arc::new(AtomicUsize::new(0));
        service.set_connector_for_tests(Arc::new(RefreshFailureConnector {
            connects: connects.clone(),
            change: change.clone(),
            unrelated_degradation: unrelated,
            tool_barrier: Some((entered.clone(), release.clone())),
            tool_outcome: Some(if same_cause { Err(error) } else { Ok(()) }),
            provider: None,
        }));
        let id = seed_mcp_installation(&store, &workspace, "late-recovery", true, false).await;
        service.reload_workspace(&workspace).await.unwrap();
        let expected = if unrelated {
            DomainRuntimeState::Degraded
        } else {
            DomainRuntimeState::Ready
        };
        wait_for_runtime_state(&service, &workspace, &id, expected).await;
        let generation =
            service.runtime_snapshot("workspace", &workspace).await[&id].runtime_generation;
        change.notify_one();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if service.runtime_snapshot("workspace", &workspace).await[&id]
                    .last_error
                    .is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        let (tx, mailbox) = {
            let tasks = service.inner.tasks.lock().await;
            (tasks[&id].call_tx.clone(), tasks[&id].recovery.clone())
        };
        let (response_tx, response) = oneshot::channel();
        tx.send(McpServerCommand::Tool(McpServerCallCommand {
            request: pioneer_tools::McpToolCallRequest {
                workspace_id: workspace.clone(),
                turn_id: "test-turn".into(),
                call_id: "delayed-tool".into(),
                callable_name: "send".into(),
                server_id: id.clone(),
                server_name: "late-recovery".into(),
                raw_tool_name: "send".into(),
                catalog_version: "test".into(),
                arguments: json!({}),
                timeout_ms: 120_000,
                max_arguments_bytes: 1024,
            },
            cancellation: CancellationToken::new(),
            response_tx,
        }))
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(3), entered.notified())
            .await
            .unwrap();
        if !same_cause {
            *service.inner.oauth_recovery_ack_timeout.lock().unwrap() = Duration::from_millis(20);
        }
        let scoped = crate::mcp_oauth::maintenance_service(service.inner.clone());
        let recovery_service = scoped.clone();
        let recovery_id = id.clone();
        let mut recovered = Some(tokio::spawn(async move {
            recovery_service
                .oauth_recovered_with_admission(
                    &recovery_id,
                    None,
                    None,
                    if same_cause { Some(cause) } else { None },
                )
                .await;
        }));
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if mailbox.pending.lock().unwrap().is_some() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        if !same_cause {
            tokio::time::timeout(Duration::from_secs(1), recovered.take().unwrap())
                .await
                .unwrap()
                .unwrap();
            assert!(
                mailbox.pending.lock().unwrap().is_some(),
                "ack expiry must retain owned recovery"
            );
            let admission = tokio::time::timeout(
                Duration::from_millis(100),
                service.installation_lifecycle_guard("workspace", &workspace, "late-recovery"),
            )
            .await
            .unwrap();
            drop(admission);
        } else {
            assert!(!recovered.as_ref().unwrap().is_finished());
        }
        let stop = if retire != 0 {
            let service = scoped.clone();
            let id = id.clone();
            Some(tokio::spawn(async move {
                if retire == 2 {
                    service.shutdown().await;
                } else {
                    service.stop_task(&id, DomainRuntimeState::Stopped).await;
                }
            }))
        } else {
            None
        };
        if retire != 0 {
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if mailbox.pending.lock().unwrap().is_none() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        release.notify_one();
        let result = response.await.unwrap();
        assert_eq!(result.is_err(), same_cause);
        if let Some(stop) = stop {
            stop.await.unwrap();
        } else {
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let snapshot =
                        service.runtime_snapshot("workspace", &workspace).await[&id].clone();
                    if snapshot.last_error.is_none() {
                        assert_eq!(snapshot.state, expected);
                        assert_eq!(snapshot.runtime_generation, generation);
                        if unrelated {
                            assert_eq!(
                                snapshot.status_reason.as_deref(),
                                Some("unrelated stream limitation")
                            );
                        }
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .unwrap();
        }
        if let Some(recovered) = recovered {
            recovered.await.unwrap();
        }
        assert!(mailbox.pending.lock().unwrap().is_none());
        assert_eq!(connects.load(Ordering::SeqCst), 1);
        if retire == 1 {
            service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
                tools: vec!["send"],
                fail_auth: false,
            }));
            service
                .restart_server("workspace", &workspace, "late-recovery")
                .await
                .unwrap();
            wait_for_runtime_state(&service, &workspace, &id, DomainRuntimeState::Ready).await;
            let replacement = service.runtime_snapshot("workspace", &workspace).await[&id].clone();
            assert!(replacement.runtime_generation > generation);
            assert!(replacement.last_error.is_none());
            assert!(mailbox.pending.lock().unwrap().is_none());
        } else if retire == 2 {
            assert_eq!(
                service.runtime_snapshot("workspace", &workspace).await[&id].state,
                DomainRuntimeState::Stopped
            );
        }
        service.shutdown().await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn oauth_recovery_keeps_session_generation_and_unrelated_degraded_reason() {
        for unrelated in [false, true] {
            let (service, store, workspace) = test_mcp_service().await;
            let connects = Arc::new(AtomicUsize::new(0));
            let change = Arc::new(tokio::sync::Notify::new());
            service.set_connector_for_tests(Arc::new(RefreshFailureConnector {
                connects: connects.clone(),
                change: change.clone(),
                unrelated_degradation: unrelated,
                tool_barrier: None,
                tool_outcome: None,
                provider: None,
            }));
            let id = seed_mcp_installation(&store, &workspace, "oauth-recovery", true, false).await;
            service.reload_workspace(&workspace).await.unwrap();
            let original_state = if unrelated {
                DomainRuntimeState::Degraded
            } else {
                DomainRuntimeState::Ready
            };
            wait_for_runtime_state(&service, &workspace, &id, original_state).await;
            let before = service.runtime_snapshot("workspace", &workspace).await[&id].clone();
            change.notify_one();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let snapshots = service.runtime_snapshot("workspace", &workspace).await;
                    if snapshots[&id].last_error.as_deref()
                        == Some("OAuth token refresh temporarily unavailable")
                    {
                        break;
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            service.oauth_recovered(&id).await;
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let snapshots = service.runtime_snapshot("workspace", &workspace).await;
                    let snapshot = &snapshots[&id];
                    if snapshot.state == original_state && snapshot.last_error.is_none() {
                        break;
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let after = service.runtime_snapshot("workspace", &workspace).await[&id].clone();
            assert_eq!(before.runtime_generation, after.runtime_generation);
            assert_eq!(connects.load(Ordering::SeqCst), 1);
            if unrelated {
                assert_eq!(
                    after.status_reason.as_deref(),
                    Some("unrelated stream limitation")
                );
            }
            // Repeated/ordinary successful refresh is a no-op for this actor.
            service.oauth_recovered(&id).await;
            assert_eq!(connects.load(Ordering::SeqCst), 1);
            service.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn oauth_loss_event_stops_live_runtime_and_disconnect_preserves_installation() {
        use pioneer_mcp::McpOAuthProvider;
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send"],
            fail_auth: false,
        }));
        let id = seed_mcp_installation(&crud_store, &workspace_id, "oauth-test", true, false).await;
        let mut row = crud_store
            .find_mcp_server_installation("workspace", &workspace_id, "oauth-test")
            .await
            .unwrap()
            .unwrap();
        row.transport_kind = "streamable_http".into();
        row.transport_json = serde_json::to_string(&McpTransportConfig::StreamableHttp {
            url: "http://127.0.0.1:1/mcp".into(),
            headers: BTreeMap::new(),
            startup_timeout_ms: 500,
            tool_timeout_ms: 500,
        })
        .unwrap();
        crud_store
            .upsert_mcp_server_installation(&row, 1_700_000_001)
            .await
            .unwrap();
        service.reload_workspace(&workspace_id).await.unwrap();
        wait_for_runtime_state(&service, &workspace_id, &id, DomainRuntimeState::Ready).await;
        assert!(service.task_exists(&id).await);
        // The OAuth service emits through the production Gateway adapter. There
        // is no browser intent at startup and the fake MCP connector uses no network.
        service.oauth().authorization_lost(&id).await;
        wait_for_runtime_state(
            &service,
            &workspace_id,
            &id,
            DomainRuntimeState::AuthRequired,
        )
        .await;
        assert!(!service.task_exists(&id).await);
        service.oauth().disconnect(&id).await.unwrap();
        service.stop_oauth_connection(&row).await;
        assert!(
            crud_store
                .find_mcp_server_installation("workspace", &workspace_id, "oauth-test")
                .await
                .unwrap()
                .is_some()
        );
        assert!(!service.task_exists(&id).await);
        service.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_secret_rotation_same_ref_restarts_runtime_and_advances_runtime_generation() {
        let (service, crud_store, workspace_id, gateway_secrets) =
            test_mcp_service_with_secrets().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send"],
            fail_auth: false,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, false).await;
        let secret_ref_id = "mcp-secret-same-ref";
        configure_mcp_installation_secret(
            &crud_store,
            workspace_id.as_str(),
            "resend",
            secret_ref_id,
        )
        .await;
        gateway_secrets
            .put_mcp_secret(secret_ref_id, "secret-canary-before-rotation", None)
            .expect("initial MCP secret should persist");

        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("initial secret-backed runtime should start");
        wait_for_runtime_state(
            &service,
            workspace_id.as_str(),
            installation_id.as_str(),
            DomainRuntimeState::Ready,
        )
        .await;
        let first_generation = service
            .runtime_snapshot("workspace", workspace_id.as_str())
            .await[installation_id.as_str()]
        .runtime_generation;
        let explicit_tool = tool_ref("mcp-tool:workspace:resend:send", "resend", "send");
        let first_projection = service
            .resolve_mcp_turn_projection(&AgentMcpMaterializationRequest {
                workspace_id: workspace_id.clone(),
                turn_id: "turn_before_secret_rotation".to_owned(),
                explicit_servers: Vec::new(),
                explicit_tools: vec![explicit_tool.clone()],
            })
            .await
            .expect("first projection should resolve");
        assert_eq!(
            first_projection.tools[0].runtime_generation,
            first_generation
        );

        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("unchanged secret material should reuse runtime");
        let unchanged_generation = service
            .runtime_snapshot("workspace", workspace_id.as_str())
            .await[installation_id.as_str()]
        .runtime_generation;
        assert_eq!(unchanged_generation, first_generation);

        gateway_secrets
            .put_mcp_secret(secret_ref_id, "secret-canary-after-rotation", None)
            .expect("rotated MCP secret should persist under the same ref");
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("secret rotation should replace the upstream runtime");
        wait_for_runtime_state(
            &service,
            workspace_id.as_str(),
            installation_id.as_str(),
            DomainRuntimeState::Ready,
        )
        .await;
        let rotated_generation = service
            .runtime_snapshot("workspace", workspace_id.as_str())
            .await[installation_id.as_str()]
        .runtime_generation;
        assert!(rotated_generation > first_generation);

        let rotated_projection = service
            .resolve_mcp_turn_projection(&AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_after_secret_rotation".to_owned(),
                explicit_servers: Vec::new(),
                explicit_tools: vec![explicit_tool],
            })
            .await
            .expect("projection after secret rotation should resolve");
        assert_eq!(
            rotated_projection.tools[0].runtime_generation,
            rotated_generation
        );
        assert_ne!(
            rotated_projection.manifest_hash,
            first_projection.manifest_hash
        );
        let projection_debug = format!("{rotated_projection:?}");
        assert!(!projection_debug.contains("secret-canary-before-rotation"));
        assert!(!projection_debug.contains("secret-canary-after-rotation"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn turn_mcp_projection_preserves_wrong_scope_rejection_without_tools() {
        let (service, _crud_store, workspace_id) = test_mcp_service().await;
        let failure = service
            .resolve_mcp_turn_projection(&AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_wrong_scope".to_owned(),
                explicit_servers: vec![AgentMcpServerRef {
                    capability_id: "mcp-server:user:resend".to_owned(),
                    label: Some("resend".to_owned()),
                    name: "resend".to_owned(),
                    scope_kind: McpScopeKind::User,
                }],
                explicit_tools: Vec::new(),
            })
            .await
            .expect_err("wrong-scope capability should fail as a typed rejection");

        assert_eq!(
            failure.reason,
            AgentMcpMaterializationFailureReason::ExplicitCapabilityRejected
        );
        assert!(failure.accepted_capabilities.is_empty());
        assert_eq!(failure.rejected_capabilities.len(), 1);
        assert_eq!(
            failure.rejected_capabilities[0].reason,
            TurnCapabilityRejectedReason::ProviderUnsupported
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capability_resolution_accepts_live_mcp_server_selection() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send", "domains"],
            fail_auth: false,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, true).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_catalog(&crud_store, installation_id.as_str()).await;

        let materialization = service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_mcp_server_capability".to_owned(),
                explicit_servers: vec![server_ref("mcp-server:workspace:resend", "resend")],
                explicit_tools: Vec::new(),
            })
            .await
            .expect("MCP materialization should succeed");

        assert!(materialization.rejected_capabilities.is_empty());
        assert_eq!(materialization.accepted_capabilities.len(), 1);
        assert_eq!(
            materialization.accepted_capabilities[0].reason,
            TurnCapabilityAcceptedReason::ExplicitComposerCapability
        );
        assert!(
            materialization
                .bundles
                .iter()
                .flat_map(|bundle| bundle.specs.iter())
                .any(|configured| configured.spec.name == "mcp_resend_send")
        );
        assert_eq!(
            materialization.available_mcp,
            vec![
                "resend".to_owned(),
                "resend/domains".to_owned(),
                "resend/send".to_owned(),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_capability_validation_accepts_live_tool_selection() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send", "domains"],
            fail_auth: false,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, true).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_catalog(&crud_store, installation_id.as_str()).await;

        let materialization = service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_mcp_tool_capability".to_owned(),
                explicit_servers: Vec::new(),
                explicit_tools: vec![tool_ref("mcp-tool:workspace:resend:send", "resend", "send")],
            })
            .await
            .expect("MCP materialization should succeed");

        assert!(materialization.rejected_capabilities.is_empty());
        assert_eq!(materialization.accepted_capabilities.len(), 1);
        assert_eq!(
            materialization.accepted_capabilities[0].id,
            "mcp-tool:workspace:resend:send"
        );
        assert!(
            materialization
                .bundles
                .iter()
                .flat_map(|bundle| bundle.specs.iter())
                .any(|configured| configured.spec.name == "mcp_resend_send")
        );
        assert_eq!(
            materialization.available_mcp,
            vec![
                "resend".to_owned(),
                "resend/domains".to_owned(),
                "resend/send".to_owned(),
            ]
        );
        let bindings = materialization.mcp_bindings.as_slice();
        let persisted_bindings = crud_store
            .list_turn_mcp_bindings("turn_mcp_tool_capability")
            .await
            .expect("turn MCP bindings should load");
        assert!(
            persisted_bindings.is_empty(),
            "materialization is a side-effect-free combined preflight stage"
        );
        let send_binding = bindings
            .iter()
            .find(|binding| binding.raw_tool_name == "send")
            .expect("selected tool should persist a binding");
        assert_eq!(
            send_binding.selection_reason,
            MCP_SELECTION_EXPLICIT_CAPABILITY
        );
        assert_eq!(
            send_binding.capability_id.as_deref(),
            Some("mcp-tool:workspace:resend:send")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn explicit_capability_preflight_rejects_unavailable_mcp_server() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send"],
            fail_auth: true,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, true).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_runtime_state(
            &service,
            workspace_id.as_str(),
            installation_id.as_str(),
            DomainRuntimeState::AuthRequired,
        )
        .await;

        let failure = expect_mcp_materialization_failure(
            service
                .materialize_mcp_tools(AgentMcpMaterializationRequest {
                    workspace_id,
                    turn_id: "turn_mcp_unavailable_capability".to_owned(),
                    explicit_servers: vec![server_ref("mcp-server:workspace:resend", "resend")],
                    explicit_tools: Vec::new(),
                })
                .await,
            "explicit unavailable MCP server should fail preflight",
        );

        assert_eq!(
            failure.reason,
            AgentMcpMaterializationFailureReason::ExplicitCapabilityRejected
        );
        assert!(failure.accepted_capabilities.is_empty());
        assert_eq!(failure.rejected_capabilities.len(), 1);
        assert_eq!(
            failure.rejected_capabilities[0].reason,
            TurnCapabilityRejectedReason::Unavailable
        );
        assert!(
            failure.rejected_capabilities[0]
                .message
                .contains("not live")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_capability_validation_rejects_missing_tool() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send"],
            fail_auth: false,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, true).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_catalog(&crud_store, installation_id.as_str()).await;

        let failure = expect_mcp_materialization_failure(
            service
                .materialize_mcp_tools(AgentMcpMaterializationRequest {
                    workspace_id,
                    turn_id: "turn_mcp_missing_tool_capability".to_owned(),
                    explicit_servers: Vec::new(),
                    explicit_tools: vec![tool_ref(
                        "mcp-tool:workspace:resend:missing",
                        "resend",
                        "missing",
                    )],
                })
                .await,
            "explicit missing MCP tool should fail preflight",
        );

        assert_eq!(
            failure.reason,
            AgentMcpMaterializationFailureReason::ExplicitCapabilityRejected
        );
        assert!(failure.accepted_capabilities.is_empty());
        assert_eq!(failure.rejected_capabilities.len(), 1);
        assert_eq!(
            failure.rejected_capabilities[0].reason,
            TurnCapabilityRejectedReason::ToolMissing
        );
        assert!(
            failure.rejected_capabilities[0]
                .message
                .contains("does not expose tool")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn workspace_mcp_tool_state_preserves_implicit_policy_without_explicit_refs() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send", "domains"],
            fail_auth: false,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, true).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_catalog(&crud_store, installation_id.as_str()).await;

        let materialization = service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_mcp_implicit_policy".to_owned(),
                explicit_servers: Vec::new(),
                explicit_tools: Vec::new(),
            })
            .await
            .expect("MCP materialization should succeed");

        assert!(materialization.accepted_capabilities.is_empty());
        assert!(materialization.rejected_capabilities.is_empty());
        assert_eq!(
            materialized_tool_names(&materialization),
            vec!["mcp_resend_domains", "mcp_resend_send"]
        );
        assert_eq!(
            materialization.available_mcp,
            vec![
                "resend".to_owned(),
                "resend/domains".to_owned(),
                "resend/send".to_owned(),
            ]
        );

        let bindings = materialization.mcp_bindings.as_slice();
        let persisted_bindings = crud_store
            .list_turn_mcp_bindings("turn_mcp_implicit_policy")
            .await
            .expect("turn MCP bindings should load");
        assert!(persisted_bindings.is_empty());
        assert_eq!(bindings.len(), 2);
        for binding in bindings {
            assert_eq!(binding.selection_reason, MCP_SELECTION_IMPLICIT_POLICY);
            assert_eq!(binding.capability_id, None);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_selection_rules_explicit_tool_attaches_only_selected_tool_when_implicit_false() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send", "domains"],
            fail_auth: false,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, false).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_catalog(&crud_store, installation_id.as_str()).await;

        let materialization = service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_mcp_explicit_tool_only".to_owned(),
                explicit_servers: Vec::new(),
                explicit_tools: vec![tool_ref("mcp-tool:workspace:resend:send", "resend", "send")],
            })
            .await
            .expect("MCP materialization should succeed");

        assert!(materialization.rejected_capabilities.is_empty());
        assert_eq!(materialization.accepted_capabilities.len(), 1);
        assert_eq!(
            materialized_tool_names(&materialization),
            vec!["mcp_resend_send"]
        );
        assert_eq!(materialization.available_mcp, vec!["resend/send"]);

        let bindings = materialization.mcp_bindings.as_slice();
        let persisted_bindings = crud_store
            .list_turn_mcp_bindings("turn_mcp_explicit_tool_only")
            .await
            .expect("turn MCP bindings should load");
        assert!(persisted_bindings.is_empty());
        assert_eq!(bindings.len(), 1);
        let binding = bindings
            .first()
            .expect("explicit tool should persist one MCP binding");
        assert_eq!(binding.raw_tool_name, "send");
        assert_eq!(binding.selection_reason, MCP_SELECTION_EXPLICIT_CAPABILITY);
        assert_eq!(
            binding.capability_id.as_deref(),
            Some("mcp-tool:workspace:resend:send")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_selection_rules_explicit_server_expands_all_tools_when_implicit_false() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send", "domains"],
            fail_auth: false,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, false).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_catalog(&crud_store, installation_id.as_str()).await;

        let materialization = service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_mcp_explicit_server_all_tools".to_owned(),
                explicit_servers: vec![server_ref("mcp-server:workspace:resend", "resend")],
                explicit_tools: Vec::new(),
            })
            .await
            .expect("MCP materialization should succeed");

        assert!(materialization.rejected_capabilities.is_empty());
        assert_eq!(materialization.accepted_capabilities.len(), 1);
        assert_eq!(
            materialized_tool_names(&materialization),
            vec!["mcp_resend_domains", "mcp_resend_send"]
        );
        assert_eq!(
            materialization.available_mcp,
            vec![
                "resend".to_owned(),
                "resend/domains".to_owned(),
                "resend/send".to_owned(),
            ]
        );

        let bindings = materialization.mcp_bindings.as_slice();
        let persisted_bindings = crud_store
            .list_turn_mcp_bindings("turn_mcp_explicit_server_all_tools")
            .await
            .expect("turn MCP bindings should load");
        assert!(persisted_bindings.is_empty());
        assert_eq!(bindings.len(), 2);
        for binding in bindings {
            assert_eq!(binding.selection_reason, MCP_SELECTION_EXPLICIT_CAPABILITY);
            assert_eq!(
                binding.capability_id.as_deref(),
                Some("mcp-server:workspace:resend")
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_selection_rules_reject_disabled_server() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send"],
            fail_auth: false,
        }));
        seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", false, false).await;

        let failure = expect_mcp_materialization_failure(
            service
                .materialize_mcp_tools(AgentMcpMaterializationRequest {
                    workspace_id,
                    turn_id: "turn_mcp_disabled_server".to_owned(),
                    explicit_servers: vec![server_ref("mcp-server:workspace:resend", "resend")],
                    explicit_tools: Vec::new(),
                })
                .await,
            "explicit disabled MCP server should fail preflight",
        );

        assert_eq!(
            failure.reason,
            AgentMcpMaterializationFailureReason::ExplicitCapabilityRejected
        );
        assert!(failure.accepted_capabilities.is_empty());
        assert_eq!(failure.rejected_capabilities.len(), 1);
        assert_eq!(
            failure.rejected_capabilities[0].reason,
            TurnCapabilityRejectedReason::DisabledByPolicy
        );
        assert!(
            failure.rejected_capabilities[0]
                .message
                .contains("disabled by workspace policy")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_selection_rules_reject_missing_catalog() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send"],
            fail_auth: false,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, false).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_catalog(&crud_store, installation_id.as_str()).await;
        crud_store
            .delete_mcp_server_catalog_snapshot(installation_id.as_str())
            .await
            .expect("test MCP catalog snapshot should delete");

        let failure = expect_mcp_materialization_failure(
            service
                .materialize_mcp_tools(AgentMcpMaterializationRequest {
                    workspace_id,
                    turn_id: "turn_mcp_missing_catalog".to_owned(),
                    explicit_servers: vec![server_ref("mcp-server:workspace:resend", "resend")],
                    explicit_tools: Vec::new(),
                })
                .await,
            "explicit server without catalog should fail preflight",
        );

        assert_eq!(
            failure.reason,
            AgentMcpMaterializationFailureReason::ExplicitCapabilityRejected
        );
        assert!(failure.accepted_capabilities.is_empty());
        assert_eq!(failure.rejected_capabilities.len(), 1);
        assert_eq!(
            failure.rejected_capabilities[0].reason,
            TurnCapabilityRejectedReason::CatalogMissing
        );
        assert!(
            failure.rejected_capabilities[0]
                .message
                .contains("no tool catalog snapshot")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_resolution_failure_required_unavailable_is_fatal_without_explicit_refs() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        seed_mcp_installation(&crud_store, workspace_id.as_str(), "required", false, false).await;
        mark_mcp_installation_required(&crud_store, workspace_id.as_str(), "required").await;

        let failure = expect_mcp_materialization_failure(
            service
                .materialize_mcp_tools(AgentMcpMaterializationRequest {
                    workspace_id,
                    turn_id: "turn_required_unavailable".to_owned(),
                    explicit_servers: Vec::new(),
                    explicit_tools: Vec::new(),
                })
                .await,
            "required unavailable MCP installation must fail preflight",
        );

        assert_eq!(
            failure.reason,
            AgentMcpMaterializationFailureReason::RequiredInstallationUnavailable
        );
        assert!(failure.rejected_capabilities.is_empty());
        assert_eq!(failure.diagnostics.len(), 1);
        assert_eq!(
            failure.diagnostics[0].code,
            "mcp.resolution.installation_unavailable"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_resolution_failure_implicit_non_required_degrades_to_empty_projection() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        seed_mcp_installation(&crud_store, workspace_id.as_str(), "optional", false, true).await;

        let materialization = service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_optional_unavailable".to_owned(),
                explicit_servers: Vec::new(),
                explicit_tools: Vec::new(),
            })
            .await
            .expect("implicit non-required MCP outage should degrade");

        assert!(materialization.bundles.is_empty());
        assert!(materialization.accepted_capabilities.is_empty());
        assert!(materialization.rejected_capabilities.is_empty());
        assert_eq!(materialization.diagnostics.len(), 1);
        assert_eq!(
            materialization.diagnostics[0].code,
            "mcp.resolution.installation_unavailable"
        );
    }
}
