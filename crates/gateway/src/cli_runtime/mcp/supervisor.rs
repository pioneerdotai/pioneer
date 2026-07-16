use super::coordinator::{CliMcpCoordinator, CliMcpCoordinatorError};
use super::grants::{CliMcpBoundGrant, CliMcpConnectionId, CliMcpGrantRef, CliMcpGrantScope};
use crate::cli_runtime::manager::CLIAgentRuntimeSessionLifecycle;
use crate::cli_runtime::session_instance::CliSessionInstanceId;
use async_trait::async_trait;
use pioneer_cli_mcp_bridge::{
    AttachRequest, BootstrapDocument, BridgeEndpoint, BridgeFrame, BridgeFrameTransport,
    BridgeFrameType, BridgeGeneration, PlatformConnection, PlatformListener, PrivateArtifactError,
    PrivateBootstrapArtifact, PrivateEndpointConfig, PrivateIpcError, PrivateSessionDirectory,
    bind_private_endpoint, create_private_session_directory,
};
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep, timeout, timeout_at};
use tokio_util::sync::CancellationToken;

const BOOTSTRAP_CONSUME_POLL: Duration = Duration::from_millis(5);
const CONTROL_FRAME_TIMEOUT: Duration = Duration::from_millis(100);
static NEXT_SUPERVISOR_ID: AtomicU64 = AtomicU64::new(1);

#[cfg(unix)]
fn private_endpoint_root(_artifact_root: &Path) -> PathBuf {
    let supervisor_id = NEXT_SUPERVISOR_ID.fetch_add(1, Ordering::Relaxed);
    PathBuf::from("/tmp").join(format!(
        "pioneer-mcp-{}-{supervisor_id}",
        std::process::id()
    ))
}

#[cfg(windows)]
fn private_endpoint_root(artifact_root: &Path) -> PathBuf {
    let _ = NEXT_SUPERVISOR_ID.fetch_add(1, Ordering::Relaxed);
    artifact_root.to_path_buf()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CliMcpBridgeSessionState {
    Prepared,
    AwaitingAttach,
    Attached,
    TransportOwned,
    Revoking,
}

#[derive(Debug)]
pub(crate) enum CliMcpBridgeSupervisorError {
    ShuttingDown,
    InvalidTimeout,
    InvalidProcess,
    AlreadyExists,
    UnknownSession,
    StaleGeneration,
    InvalidTransition,
    AttachTimeout,
    UnexpectedFrame,
    BootstrapNotConsumed,
    Coordinator(CliMcpCoordinatorError),
    Artifact(PrivateArtifactError),
    Ipc(PrivateIpcError),
    Bootstrap(pioneer_cli_mcp_bridge::BootstrapDecodeError),
    Frame(pioneer_cli_mcp_bridge::BridgeFrameError),
}

impl fmt::Display for CliMcpBridgeSupervisorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShuttingDown => formatter.write_str("CLI MCP bridge supervisor is shutting down"),
            Self::InvalidTimeout => formatter.write_str("CLI MCP bridge timeout must be positive"),
            Self::InvalidProcess => formatter.write_str("invalid CLI MCP provider/helper process"),
            Self::AlreadyExists => formatter.write_str("CLI MCP bridge generation already exists"),
            Self::UnknownSession => formatter.write_str("unknown CLI MCP bridge generation"),
            Self::StaleGeneration => formatter.write_str("stale CLI MCP bridge generation"),
            Self::InvalidTransition => {
                formatter.write_str("invalid CLI MCP bridge lifecycle transition")
            }
            Self::AttachTimeout => formatter.write_str("CLI MCP bridge attach timed out"),
            Self::UnexpectedFrame => formatter.write_str("unexpected CLI MCP bridge attach frame"),
            Self::BootstrapNotConsumed => {
                formatter.write_str("CLI MCP bootstrap was not consumed before readiness")
            }
            Self::Coordinator(error) => write!(formatter, "CLI MCP bridge grant failed: {error:?}"),
            Self::Artifact(error) => error.fmt(formatter),
            Self::Ipc(error) => error.fmt(formatter),
            Self::Bootstrap(error) => error.fmt(formatter),
            Self::Frame(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for CliMcpBridgeSupervisorError {}

impl From<CliMcpCoordinatorError> for CliMcpBridgeSupervisorError {
    fn from(value: CliMcpCoordinatorError) -> Self {
        Self::Coordinator(value)
    }
}

impl From<PrivateArtifactError> for CliMcpBridgeSupervisorError {
    fn from(value: PrivateArtifactError) -> Self {
        Self::Artifact(value)
    }
}

impl From<PrivateIpcError> for CliMcpBridgeSupervisorError {
    fn from(value: PrivateIpcError) -> Self {
        Self::Ipc(value)
    }
}

impl From<pioneer_cli_mcp_bridge::BootstrapDecodeError> for CliMcpBridgeSupervisorError {
    fn from(value: pioneer_cli_mcp_bridge::BootstrapDecodeError) -> Self {
        Self::Bootstrap(value)
    }
}

impl From<pioneer_cli_mcp_bridge::BridgeFrameError> for CliMcpBridgeSupervisorError {
    fn from(value: pioneer_cli_mcp_bridge::BridgeFrameError) -> Self {
        Self::Frame(value)
    }
}

pub(crate) struct CliMcpBridgeLaunch {
    process_instance: CliSessionInstanceId,
    grant_ref: CliMcpGrantRef,
    endpoint: BridgeEndpoint,
    bootstrap_path: PathBuf,
    #[cfg(test)]
    cancellation: CancellationToken,
}

impl fmt::Debug for CliMcpBridgeLaunch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CliMcpBridgeLaunch")
            .field("process_instance", &self.process_instance)
            .field("grant_ref", &self.grant_ref)
            .field("endpoint", &self.endpoint)
            .field("bootstrap_path", &"[REDACTED]")
            .finish()
    }
}

impl CliMcpBridgeLaunch {
    #[cfg(test)]
    pub(crate) fn process_instance(&self) -> &CliSessionInstanceId {
        &self.process_instance
    }

    pub(crate) fn grant_ref(&self) -> &CliMcpGrantRef {
        &self.grant_ref
    }

    #[cfg(test)]
    pub(crate) fn endpoint(&self) -> &BridgeEndpoint {
        &self.endpoint
    }

    pub(crate) fn bootstrap_path(&self) -> &Path {
        self.bootstrap_path.as_path()
    }

    #[cfg(test)]
    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CliMcpBridgeAttachment {
    pub(crate) process_instance: CliSessionInstanceId,
    pub(crate) bound_grant: CliMcpBoundGrant,
}

struct CliMcpBridgeSession {
    process_instance: CliSessionInstanceId,
    scope: CliMcpGrantScope,
    grant_ref: CliMcpGrantRef,
    state: CliMcpBridgeSessionState,
    provider_process_id: Option<u32>,
    listener: Option<PlatformListener>,
    connection: Option<PlatformConnection>,
    bound_grant: Option<CliMcpBoundGrant>,
    bootstrap: Option<PrivateBootstrapArtifact>,
    bootstrap_directory: Option<PrivateSessionDirectory>,
    endpoint_directory: Option<PrivateSessionDirectory>,
    cancellation: CancellationToken,
}

pub(crate) struct CliMcpBridgeSupervisor {
    artifact_root: PathBuf,
    endpoint_root: PathBuf,
    coordinator: Arc<CliMcpCoordinator>,
    prepare_lock: Mutex<()>,
    sessions: Mutex<HashMap<CliSessionInstanceId, CliMcpBridgeSession>>,
    issuing_stopped: AtomicBool,
}

impl CliMcpBridgeSupervisor {
    pub(crate) fn new(artifact_root: PathBuf) -> Arc<Self> {
        let endpoint_root = private_endpoint_root(artifact_root.as_path());
        Arc::new(Self {
            artifact_root,
            endpoint_root,
            coordinator: Arc::new(CliMcpCoordinator::default()),
            prepare_lock: Mutex::new(()),
            sessions: Mutex::new(HashMap::new()),
            issuing_stopped: AtomicBool::new(false),
        })
    }

    pub(crate) fn coordinator(&self) -> Arc<CliMcpCoordinator> {
        self.coordinator.clone()
    }

    pub(crate) async fn prepare(
        &self,
        scope: CliMcpGrantScope,
        expires_at_unix_ms: u64,
    ) -> Result<CliMcpBridgeLaunch, CliMcpBridgeSupervisorError> {
        self.prepare_at(scope, expires_at_unix_ms, self.artifact_root.as_path())
            .await
    }

    /// Prepare an exact bridge generation beneath a provider generation's
    /// private overlay instead of the supervisor-wide fallback root.
    pub(crate) async fn prepare_in_overlay(
        &self,
        scope: CliMcpGrantScope,
        expires_at_unix_ms: u64,
        overlay_bootstrap_root: &Path,
    ) -> Result<CliMcpBridgeLaunch, CliMcpBridgeSupervisorError> {
        self.prepare_at(scope, expires_at_unix_ms, overlay_bootstrap_root)
            .await
    }

    async fn prepare_at(
        &self,
        scope: CliMcpGrantScope,
        expires_at_unix_ms: u64,
        artifact_root: &Path,
    ) -> Result<CliMcpBridgeLaunch, CliMcpBridgeSupervisorError> {
        let _prepare_guard = self.prepare_lock.lock().await;
        if self.issuing_stopped.load(Ordering::Acquire) {
            return Err(CliMcpBridgeSupervisorError::ShuttingDown);
        }
        let process_instance = scope.process_instance.clone();
        let replaced = {
            let sessions = self.sessions.lock().await;
            if sessions.contains_key(&process_instance) {
                return Err(CliMcpBridgeSupervisorError::AlreadyExists);
            }
            sessions
                .keys()
                .filter(|candidate| {
                    candidate.key() == process_instance.key() && **candidate != process_instance
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        for stale in replaced {
            self.revoke_session(&stale).await;
        }

        let issued = self
            .coordinator
            .issue_grant(scope.clone(), expires_at_unix_ms)
            .await?;
        let grant_ref = issued.grant_ref();
        let generation = BridgeGeneration::new(process_instance.generation())?;
        let mut endpoint_directory = match create_private_session_directory(
            self.endpoint_root.as_path(),
            &issued.bridge_session_id,
            generation,
        ) {
            Ok(directory) => directory,
            Err(error) => {
                let _ = self.coordinator.revoke_grant(&grant_ref).await;
                return Err(error.into());
            }
        };
        let mut bootstrap_directory = if self.endpoint_root == artifact_root {
            None
        } else {
            match create_private_session_directory(
                artifact_root,
                &issued.bridge_session_id,
                generation,
            ) {
                Ok(directory) => Some(directory),
                Err(error) => {
                    let _ = self.coordinator.revoke_grant(&grant_ref).await;
                    let _ = endpoint_directory.cleanup();
                    return Err(error.into());
                }
            }
        };
        let endpoint_config = PrivateEndpointConfig {
            managed_directory: endpoint_directory.path().to_path_buf(),
            session_id: issued.bridge_session_id.clone(),
            generation,
            expected_peer_pid: None,
        };
        let listener = match bind_private_endpoint(&endpoint_config) {
            Ok(listener) => listener,
            Err(error) => {
                let _ = self.coordinator.revoke_grant(&grant_ref).await;
                if let Some(directory) = bootstrap_directory.as_mut() {
                    let _ = directory.cleanup();
                }
                let _ = endpoint_directory.cleanup();
                return Err(error.into());
            }
        };
        let document = BootstrapDocument {
            session_id: issued.bridge_session_id,
            generation,
            endpoint: listener.endpoint().clone(),
            nonce: issued.nonce,
            expires_at_unix_ms: issued.expires_at_unix_ms,
        };
        let bootstrap_owner = bootstrap_directory.as_ref().unwrap_or(&endpoint_directory);
        let bootstrap = match bootstrap_owner.write_bootstrap(&document) {
            Ok(bootstrap) => bootstrap,
            Err(error) => {
                drop(listener);
                let _ = self.coordinator.revoke_grant(&grant_ref).await;
                if let Some(directory) = bootstrap_directory.as_mut() {
                    let _ = directory.cleanup();
                }
                let _ = endpoint_directory.cleanup();
                return Err(error.into());
            }
        };
        let endpoint = document.endpoint;
        let bootstrap_path = bootstrap.path().to_path_buf();
        let cancellation = CancellationToken::new();
        #[cfg(test)]
        let launch_cancellation = cancellation.clone();
        let session = CliMcpBridgeSession {
            process_instance: process_instance.clone(),
            scope,
            grant_ref: grant_ref.clone(),
            state: CliMcpBridgeSessionState::Prepared,
            provider_process_id: None,
            listener: Some(listener),
            connection: None,
            bound_grant: None,
            bootstrap: Some(bootstrap),
            bootstrap_directory,
            endpoint_directory: Some(endpoint_directory),
            cancellation,
        };
        let mut sessions = self.sessions.lock().await;
        if self.issuing_stopped.load(Ordering::Acquire)
            || sessions.insert(process_instance.clone(), session).is_some()
        {
            drop(sessions);
            let _ = self.coordinator.revoke_grant(&grant_ref).await;
            self.revoke_session(&process_instance).await;
            return Err(CliMcpBridgeSupervisorError::ShuttingDown);
        }
        Ok(CliMcpBridgeLaunch {
            process_instance,
            grant_ref,
            endpoint,
            bootstrap_path,
            #[cfg(test)]
            cancellation: launch_cancellation,
        })
    }

    pub(crate) async fn associate_provider_process(
        &self,
        process_instance: &CliSessionInstanceId,
        provider_process_id: u32,
        expected_helper_process_id: Option<u32>,
    ) -> Result<(), CliMcpBridgeSupervisorError> {
        if provider_process_id == 0 || expected_helper_process_id == Some(0) {
            return Err(CliMcpBridgeSupervisorError::InvalidProcess);
        }
        let mut sessions = self.sessions.lock().await;
        let session = sessions
            .get_mut(process_instance)
            .ok_or(CliMcpBridgeSupervisorError::UnknownSession)?;
        if session.process_instance != *process_instance
            || session.state != CliMcpBridgeSessionState::Prepared
        {
            return Err(CliMcpBridgeSupervisorError::InvalidTransition);
        }
        if let Some(helper_process_id) = expected_helper_process_id {
            session
                .listener
                .as_mut()
                .ok_or(CliMcpBridgeSupervisorError::InvalidTransition)?
                .set_expected_peer_pid(helper_process_id);
        }
        session.provider_process_id = Some(provider_process_id);
        Ok(())
    }

    pub(crate) async fn await_attach(
        &self,
        process_instance: &CliSessionInstanceId,
        attach_timeout: Duration,
    ) -> Result<CliMcpBridgeAttachment, CliMcpBridgeSupervisorError> {
        if attach_timeout.is_zero() {
            return Err(CliMcpBridgeSupervisorError::InvalidTimeout);
        }
        let deadline = Instant::now() + attach_timeout;
        let (mut listener, scope) = {
            let mut sessions = self.sessions.lock().await;
            let session = sessions
                .get_mut(process_instance)
                .ok_or(CliMcpBridgeSupervisorError::UnknownSession)?;
            if session.state != CliMcpBridgeSessionState::Prepared
                || session.provider_process_id.is_none()
            {
                return Err(CliMcpBridgeSupervisorError::InvalidTransition);
            }
            session.state = CliMcpBridgeSessionState::AwaitingAttach;
            (
                session
                    .listener
                    .take()
                    .ok_or(CliMcpBridgeSupervisorError::InvalidTransition)?,
                session.scope.clone(),
            )
        };

        let outcome = async {
            let mut connection = timeout_at(deadline, listener.accept())
                .await
                .map_err(|_| CliMcpBridgeSupervisorError::AttachTimeout)??;
            let frame = timeout_at(deadline, connection.receive_frame())
                .await
                .map_err(|_| CliMcpBridgeSupervisorError::AttachTimeout)??
                .ok_or(CliMcpBridgeSupervisorError::UnexpectedFrame)?;
            if frame.frame_type() != BridgeFrameType::Attach {
                return Err(CliMcpBridgeSupervisorError::UnexpectedFrame);
            }
            let request = AttachRequest::decode(frame.payload())?;
            let bound = match self
                .coordinator
                .attach(&request, &scope, CliMcpConnectionId::random())
                .await
            {
                Ok(bound) => bound,
                Err(error) => {
                    let reject = BridgeFrame::new(BridgeFrameType::Error, Vec::new())?;
                    let _ = connection.send_frame(&reject).await;
                    return Err(error.into());
                }
            };
            let accepted = BridgeFrame::new(BridgeFrameType::Result, Vec::new())?;
            connection.send_frame(&accepted).await?;
            loop {
                let consumed = {
                    let sessions = self.sessions.lock().await;
                    let session = sessions
                        .get(process_instance)
                        .ok_or(CliMcpBridgeSupervisorError::UnknownSession)?;
                    session
                        .bootstrap
                        .as_ref()
                        .ok_or(CliMcpBridgeSupervisorError::InvalidTransition)?
                        .is_consumed()?
                };
                if consumed {
                    break;
                }
                if Instant::now() >= deadline {
                    return Err(CliMcpBridgeSupervisorError::BootstrapNotConsumed);
                }
                sleep(BOOTSTRAP_CONSUME_POLL).await;
            }
            Ok((connection, bound))
        }
        .await;
        drop(listener);

        let (connection, bound_grant) = match outcome {
            Ok(attached) => attached,
            Err(error) => {
                self.revoke_session(process_instance).await;
                return Err(error);
            }
        };
        let mut sessions = self.sessions.lock().await;
        let session = sessions
            .get_mut(process_instance)
            .ok_or(CliMcpBridgeSupervisorError::UnknownSession)?;
        if session.state != CliMcpBridgeSessionState::AwaitingAttach {
            drop(sessions);
            self.revoke_session(process_instance).await;
            return Err(CliMcpBridgeSupervisorError::StaleGeneration);
        }
        session.connection = Some(connection);
        session.bound_grant = Some(bound_grant.clone());
        session.state = CliMcpBridgeSessionState::Attached;
        Ok(CliMcpBridgeAttachment {
            process_instance: process_instance.clone(),
            bound_grant,
        })
    }

    pub(crate) async fn take_transport(
        self: &Arc<Self>,
        process_instance: &CliSessionInstanceId,
    ) -> Result<CliMcpBridgeTransport, CliMcpBridgeSupervisorError> {
        let mut sessions = self.sessions.lock().await;
        let session = sessions
            .get_mut(process_instance)
            .ok_or(CliMcpBridgeSupervisorError::UnknownSession)?;
        if session.state != CliMcpBridgeSessionState::Attached {
            return Err(CliMcpBridgeSupervisorError::InvalidTransition);
        }
        let connection = session
            .connection
            .take()
            .ok_or(CliMcpBridgeSupervisorError::InvalidTransition)?;
        let bound_grant = session
            .bound_grant
            .clone()
            .ok_or(CliMcpBridgeSupervisorError::InvalidTransition)?;
        session.state = CliMcpBridgeSessionState::TransportOwned;
        Ok(CliMcpBridgeTransport {
            supervisor: Arc::clone(self),
            process_instance: process_instance.clone(),
            bound_grant,
            connection: Some(connection),
            terminated: false,
        })
    }

    pub(crate) async fn cancel_session(&self, process_instance: &CliSessionInstanceId) {
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sessions.get_mut(process_instance) {
            session.cancellation.cancel();
            if let Some(connection) = session.connection.as_mut()
                && let Ok(frame) = BridgeFrame::new(BridgeFrameType::Cancellation, Vec::new())
            {
                let _ = timeout(CONTROL_FRAME_TIMEOUT, connection.send_frame(&frame)).await;
            }
        }
    }

    pub(crate) async fn revoke_session(&self, process_instance: &CliSessionInstanceId) -> bool {
        let mut session = {
            let mut sessions = self.sessions.lock().await;
            let Some(mut session) = sessions.remove(process_instance) else {
                return false;
            };
            session.state = CliMcpBridgeSessionState::Revoking;
            session
        };
        session.cancellation.cancel();
        if let Some(mut connection) = session.connection.take() {
            if let Ok(frame) = BridgeFrame::new(BridgeFrameType::Cancellation, Vec::new()) {
                let _ = timeout(CONTROL_FRAME_TIMEOUT, connection.send_frame(&frame)).await;
            }
            if let Ok(frame) = BridgeFrame::new(BridgeFrameType::Shutdown, Vec::new()) {
                let _ = timeout(CONTROL_FRAME_TIMEOUT, connection.send_frame(&frame)).await;
            }
            let _ = timeout(CONTROL_FRAME_TIMEOUT, connection.shutdown()).await;
        }
        drop(session.listener.take());
        let _ = self.coordinator.revoke_grant(&session.grant_ref).await;
        if let Some(mut bootstrap) = session.bootstrap.take() {
            let _ = bootstrap.cleanup();
        }
        if let Some(mut directory) = session.bootstrap_directory.take() {
            let _ = directory.cleanup();
        }
        if let Some(mut directory) = session.endpoint_directory.take() {
            let _ = directory.cleanup();
        }
        true
    }

    pub(crate) async fn stop_issuing_and_cancel(&self) {
        self.issuing_stopped.store(true, Ordering::Release);
        let sessions = self.sessions.lock().await;
        for session in sessions.values() {
            session.cancellation.cancel();
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.stop_issuing_and_cancel().await;
        let instances = self
            .sessions
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for instance in instances {
            self.revoke_session(&instance).await;
        }
        self.coordinator.shutdown().await;
        if self.endpoint_root != self.artifact_root {
            let _ = std::fs::remove_dir(self.endpoint_root.as_path());
        }
    }

    #[cfg(test)]
    async fn state(
        &self,
        process_instance: &CliSessionInstanceId,
    ) -> Option<CliMcpBridgeSessionState> {
        self.sessions
            .lock()
            .await
            .get(process_instance)
            .map(|session| session.state)
    }
}

pub(crate) struct CliMcpBridgeTransport {
    supervisor: Arc<CliMcpBridgeSupervisor>,
    process_instance: CliSessionInstanceId,
    bound_grant: CliMcpBoundGrant,
    connection: Option<PlatformConnection>,
    terminated: bool,
}

impl CliMcpBridgeTransport {
    pub(crate) fn bound_grant(&self) -> &CliMcpBoundGrant {
        &self.bound_grant
    }

    pub(crate) async fn receive_frame(
        &mut self,
    ) -> Result<Option<BridgeFrame>, CliMcpBridgeSupervisorError> {
        let outcome = self
            .connection
            .as_mut()
            .ok_or(CliMcpBridgeSupervisorError::InvalidTransition)?
            .receive_frame()
            .await;
        match outcome {
            Ok(Some(frame)) => Ok(Some(frame)),
            Ok(None) => {
                self.terminate().await;
                Ok(None)
            }
            Err(error) => {
                self.terminate().await;
                Err(error.into())
            }
        }
    }

    pub(crate) async fn send_frame(
        &mut self,
        frame: &BridgeFrame,
    ) -> Result<(), CliMcpBridgeSupervisorError> {
        let outcome = self
            .connection
            .as_mut()
            .ok_or(CliMcpBridgeSupervisorError::InvalidTransition)?
            .send_frame(frame)
            .await;
        if let Err(error) = outcome {
            self.terminate().await;
            return Err(error.into());
        }
        Ok(())
    }

    pub(crate) async fn terminate(&mut self) {
        if self.terminated {
            return;
        }
        self.terminated = true;
        if let Some(connection) = self.connection.as_mut() {
            let _ = connection.shutdown().await;
        }
        self.connection.take();
        self.supervisor.revoke_session(&self.process_instance).await;
    }
}

impl Drop for CliMcpBridgeTransport {
    fn drop(&mut self) {
        if self.terminated {
            return;
        }
        self.connection.take();
        let supervisor = self.supervisor.clone();
        let process_instance = self.process_instance.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                supervisor.revoke_session(&process_instance).await;
            });
        }
    }
}

#[async_trait]
impl CLIAgentRuntimeSessionLifecycle for CliMcpBridgeSupervisor {
    async fn shutdown_started(&self) {
        self.stop_issuing_and_cancel().await;
    }

    async fn before_session_close(&self, instance: &CliSessionInstanceId) {
        self.cancel_session(instance).await;
    }

    async fn after_session_close(&self, instance: &CliSessionInstanceId) {
        self.revoke_session(instance).await;
    }

    async fn shutdown_finished(&self) {
        self.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli_runtime::manager::CLIAgentRuntimeSessionKey;
    use crate::cli_runtime::mcp::grants::CliMcpManifestHash;
    use pioneer_cli_mcp_bridge::{
        BootstrapNonce, connect_private_endpoint, helper::run_hidden_helper_with_io,
    };
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn instance_for(thread_id: &str, generation: u64) -> CliSessionInstanceId {
        CliSessionInstanceId::unmanaged_for_test(
            CLIAgentRuntimeSessionKey::new("workspace", "codex", thread_id).expect("key"),
            generation,
        )
        .expect("instance")
    }

    fn instance(generation: u64) -> CliSessionInstanceId {
        instance_for("thread", generation)
    }

    fn scope(generation: u64) -> CliMcpGrantScope {
        CliMcpGrantScope::new(
            instance(generation),
            CliMcpManifestHash::new("a".repeat(64)).expect("manifest"),
        )
    }

    fn expiry() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis() as u64
            + 60_000
    }

    fn temporary_root() -> tempfile::TempDir {
        // Unix-domain socket paths have a small platform limit. Keep the test
        // root short so the generated, generation-scoped endpoint remains a
        // valid production-shaped path on macOS as well as Linux.
        #[cfg(unix)]
        {
            tempfile::tempdir_in("/tmp").expect("root")
        }
        #[cfg(windows)]
        {
            tempfile::tempdir().expect("root")
        }
    }

    #[tokio::test]
    async fn cli_mcp_supervisor_paused_attach_is_bounded_and_cleans_artifacts() {
        let root = temporary_root();
        let supervisor = CliMcpBridgeSupervisor::new(root.path().join("sessions"));
        let launch = supervisor
            .prepare(scope(1), expiry())
            .await
            .expect("prepare");
        supervisor
            .associate_provider_process(launch.process_instance(), std::process::id(), None)
            .await
            .expect("associate");
        assert!(matches!(
            supervisor
                .await_attach(launch.process_instance(), Duration::from_millis(10))
                .await,
            Err(CliMcpBridgeSupervisorError::AttachTimeout)
        ));
        assert!(supervisor.state(launch.process_instance()).await.is_none());
        assert!(!launch.bootstrap_path().exists());
    }

    #[tokio::test]
    async fn cli_mcp_shutdown_revokes_prepared_generation_and_is_idempotent() {
        let root = temporary_root();
        let supervisor = CliMcpBridgeSupervisor::new(root.path().join("sessions"));
        let launch = supervisor
            .prepare(scope(1), expiry())
            .await
            .expect("prepare");
        supervisor.shutdown().await;
        supervisor.shutdown().await;
        assert!(launch.cancellation().is_cancelled());
        assert!(!launch.bootstrap_path().exists());
        assert!(matches!(
            supervisor.prepare(scope(2), expiry()).await,
            Err(CliMcpBridgeSupervisorError::ShuttingDown)
        ));
    }

    #[tokio::test]
    async fn cli_mcp_supervisor_stale_cleanup_cannot_remove_replacement_generation() {
        let root = temporary_root();
        let supervisor = CliMcpBridgeSupervisor::new(root.path().join("sessions"));
        let old = supervisor.prepare(scope(1), expiry()).await.expect("old");
        let replacement = supervisor
            .prepare(scope(2), expiry())
            .await
            .expect("replacement");
        assert!(old.cancellation().is_cancelled());
        assert!(!old.bootstrap_path().exists());
        assert!(replacement.bootstrap_path().exists());
        assert!(
            supervisor
                .state(replacement.process_instance())
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn codex_mcp_overlay_bridge_artifacts_are_scoped_to_exact_overlay() {
        let root = temporary_root();
        let supervisor = CliMcpBridgeSupervisor::new(root.path().join("fallback"));
        let overlay_a = root.path().join("overlay-a/bootstrap");
        let overlay_b = root.path().join("overlay-b/bootstrap");
        let scope_a = CliMcpGrantScope::new(
            instance_for("thread-a", 1),
            CliMcpManifestHash::new("a".repeat(64)).expect("manifest A"),
        );
        let scope_b = CliMcpGrantScope::new(
            instance_for("thread-b", 1),
            CliMcpManifestHash::new("a".repeat(64)).expect("manifest B"),
        );

        let launch_a = supervisor
            .prepare_in_overlay(scope_a, expiry(), overlay_a.as_path())
            .await
            .expect("prepare overlay A");
        let launch_b = supervisor
            .prepare_in_overlay(scope_b, expiry(), overlay_b.as_path())
            .await
            .expect("prepare overlay B");

        assert!(launch_a.bootstrap_path().starts_with(overlay_a.as_path()));
        assert!(!launch_a.bootstrap_path().starts_with(overlay_b.as_path()));
        assert!(launch_b.bootstrap_path().starts_with(overlay_b.as_path()));
        assert!(!launch_b.bootstrap_path().starts_with(overlay_a.as_path()));
        assert_ne!(launch_a.bootstrap_path(), launch_b.bootstrap_path());
        assert_ne!(launch_a.endpoint(), launch_b.endpoint());
        #[cfg(unix)]
        {
            assert!(!Path::new(launch_a.endpoint().address()).starts_with(overlay_a.as_path()));
            assert!(launch_a.endpoint().address().len() <= 103);
        }

        assert!(supervisor.revoke_session(launch_a.process_instance()).await);
        assert!(!launch_a.bootstrap_path().exists());
        assert!(launch_b.bootstrap_path().exists());
        assert!(supervisor.revoke_session(launch_b.process_instance()).await);
        assert!(!launch_b.bootstrap_path().exists());
    }

    #[tokio::test]
    async fn codex_mcp_overlay_long_bootstrap_path_uses_short_private_endpoint() {
        let root = temporary_root();
        let supervisor = CliMcpBridgeSupervisor::new(root.path().join("fallback"));
        let long_overlay = root
            .path()
            .join("generation-overlays")
            .join("x".repeat(96))
            .join("bootstrap");
        let launch = supervisor
            .prepare_in_overlay(scope(1), expiry(), long_overlay.as_path())
            .await
            .expect("long overlay must not lengthen Unix endpoint");
        assert!(launch.bootstrap_path().starts_with(long_overlay.as_path()));
        #[cfg(unix)]
        {
            assert!(launch.bootstrap_path().to_string_lossy().len() > 103);
            assert!(launch.endpoint().address().len() <= 103);
            assert!(!Path::new(launch.endpoint().address()).starts_with(long_overlay.as_path()));
        }
        supervisor.shutdown().await;
        assert!(!launch.bootstrap_path().exists());
    }

    #[tokio::test]
    async fn cli_mcp_attach_wrong_nonce_is_rejected_and_generation_is_cleaned() {
        let root = temporary_root();
        let supervisor = CliMcpBridgeSupervisor::new(root.path().join("sessions"));
        let launch = supervisor
            .prepare(scope(1), expiry())
            .await
            .expect("prepare");
        supervisor
            .associate_provider_process(launch.process_instance(), std::process::id(), None)
            .await
            .expect("associate");
        let document = BootstrapDocument::decode(
            fs::read(launch.bootstrap_path())
                .expect("bootstrap")
                .as_slice(),
        )
        .expect("document");
        #[cfg(unix)]
        let managed_directory = Path::new(document.endpoint.address())
            .parent()
            .expect("endpoint session directory")
            .to_path_buf();
        #[cfg(windows)]
        let managed_directory = launch
            .bootstrap_path()
            .parent()
            .expect("session directory")
            .to_path_buf();
        let connection_config = PrivateEndpointConfig {
            managed_directory,
            session_id: document.session_id.clone(),
            generation: document.generation,
            expected_peer_pid: Some(std::process::id()),
        };
        let wrong = AttachRequest {
            session_id: document.session_id,
            generation: document.generation,
            nonce: BootstrapNonce::new([0x55; pioneer_cli_mcp_bridge::NONCE_BYTES]).expect("nonce"),
        };
        let client = async {
            let mut connection = connect_private_endpoint(&connection_config)
                .await
                .expect("connect");
            let frame = BridgeFrame::new(BridgeFrameType::Attach, wrong.encode().expect("encode"))
                .expect("frame");
            connection.send_frame(&frame).await.expect("send");
            connection.receive_frame().await.expect("response")
        };
        let (attach, response) = tokio::join!(
            supervisor.await_attach(launch.process_instance(), Duration::from_secs(1)),
            client
        );
        assert!(matches!(
            attach,
            Err(CliMcpBridgeSupervisorError::Coordinator(_))
        ));
        assert!(matches!(
            response,
            Some(frame) if frame.frame_type() == BridgeFrameType::Error
        ));
        assert!(!launch.bootstrap_path().exists());
    }

    #[tokio::test]
    async fn cli_mcp_supervisor_helper_eof_revokes_exact_attached_generation() {
        let root = temporary_root();
        let supervisor = CliMcpBridgeSupervisor::new(root.path().join("sessions"));
        let launch = supervisor
            .prepare(scope(1), expiry())
            .await
            .expect("prepare");
        supervisor
            .associate_provider_process(
                launch.process_instance(),
                std::process::id(),
                Some(std::process::id()),
            )
            .await
            .expect("associate");
        let helper = run_hidden_helper_with_io(
            launch.bootstrap_path(),
            tokio::io::empty(),
            tokio::io::sink(),
        );
        let (attached, helper_result) = tokio::join!(
            supervisor.await_attach(launch.process_instance(), Duration::from_secs(1)),
            helper
        );
        attached.expect("attach");
        helper_result.expect("helper exits on provider EOF");
        assert!(!launch.bootstrap_path().exists());

        let mut transport = supervisor
            .take_transport(launch.process_instance())
            .await
            .expect("transport");
        assert!(matches!(
            transport.receive_frame().await.expect("shutdown frame"),
            Some(frame) if frame.frame_type() == BridgeFrameType::Shutdown
        ));
        assert!(
            transport
                .receive_frame()
                .await
                .expect("helper EOF")
                .is_none()
        );
        assert!(supervisor.state(launch.process_instance()).await.is_none());
    }

    #[tokio::test]
    async fn cli_mcp_supervisor_provider_eof_cancels_and_cleans_exact_generation() {
        let root = temporary_root();
        let supervisor = CliMcpBridgeSupervisor::new(root.path().join("sessions"));
        let launch = supervisor
            .prepare(scope(1), expiry())
            .await
            .expect("prepare");
        CLIAgentRuntimeSessionLifecycle::after_session_close(
            supervisor.as_ref(),
            launch.process_instance(),
        )
        .await;
        assert!(launch.cancellation().is_cancelled());
        assert!(!launch.bootstrap_path().exists());
        assert!(supervisor.state(launch.process_instance()).await.is_none());
    }
}
