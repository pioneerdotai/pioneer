//! Agent Client Protocol v1, independent of the configured agent implementation.
//!
//! Wire types are pinned to the official stable schema. No draft feature is
//! enabled. Gateway owns authority, durable events, history and user interaction.

use agent_client_protocol_schema::ProtocolVersion;
pub use agent_client_protocol_schema::v1 as schema;
pub mod auth_terminal;
mod configuration;
mod dispatch;
mod elicitation;
pub mod reducer;
pub mod transport;

use crate::process::{
    CLIAgentProcess, CLIAgentProcessSpawnConfig, SensitiveEnvironment, spawn_cli_agent_process,
};
pub use configuration::{ConfigurationSnapshot, ConfigurationVersion, validate_config_value};
pub use dispatch::validate_response as validate_callback_response;
pub use dispatch::{CallbackRequest, Host};

/// Restore a human request from Pioneer's existing pending-request payload.
/// Method-directed parsing avoids the schema SDK's untagged request enum.
pub fn parse_human_request(
    method: &str,
    params: serde_json::Value,
) -> schema::Result<CallbackRequest> {
    match method {
        "session/request_permission" => serde_json::from_value(params)
            .map(CallbackRequest::RequestPermissionRequest)
            .map_err(|_| schema::Error::invalid_params()),
        "elicitation/create" => {
            let request: schema::CreateElicitationRequest =
                serde_json::from_value(params).map_err(|_| schema::Error::invalid_params())?;
            elicitation::validate_request(&request)?;
            Ok(CallbackRequest::CreateElicitationRequest(request))
        }
        _ => Err(schema::Error::method_not_found()),
    }
}
use schema::*;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use transport::{PendingRequest, Sender, Transport, TransportLimits};

pub(crate) struct ProtocolState {
    pub(crate) connection_id: uuid::Uuid,
    initialized: Option<InitializeResponse>,
    pub(crate) configurations: HashMap<String, ConfigurationSnapshot>,
    pub(crate) active_prompts: HashMap<String, transport::RequestId>,
    pub(crate) pending_configuration: HashSet<String>,
    pub(crate) revision: u64,
}

/// One configured ACP invocation. Workspace, environment references and the
/// execution generation are resolved by the existing Gateway launch owner.
/// There are no native provider homes, installers, rules or compatibility code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Launch {
    pub executable: String,
    pub args: Vec<String>,
    pub cwd: std::path::PathBuf,
    pub env: SensitiveEnvironment,
    pub env_remove: Vec<String>,
    pub stderr_ring_lines: usize,
    pub process_generation: u64,
}

impl Launch {
    fn process_config(&self) -> Result<CLIAgentProcessSpawnConfig> {
        if !self.cwd.is_absolute() || self.process_generation == 0 {
            return Err(Error::invalid_params());
        }
        Ok(CLIAgentProcessSpawnConfig {
            executable: self.executable.clone(),
            args: self.args.clone(),
            cwd: Some(self.cwd.clone()),
            home_path: None,
            home_dir: None,
            env: self.env.clone(),
            env_remove: self.env_remove.clone(),
            stderr_ring_lines: self.stderr_ring_lines,
            process_group: true,
            process_generation: Some(self.process_generation),
        })
    }
}

impl Default for ProtocolState {
    fn default() -> Self {
        Self {
            connection_id: uuid::Uuid::new_v4(),
            initialized: None,
            configurations: HashMap::new(),
            active_prompts: HashMap::new(),
            pending_configuration: HashSet::new(),
            revision: 0,
        }
    }
}

impl ProtocolState {
    pub(crate) fn next_revision(&mut self) -> Result<u64> {
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or_else(Error::internal_error)?;
        Ok(self.revision)
    }
}

/// Owns exactly one process generation. Never replays a dispatched prompt after
/// timeout/EOF, and never treats load/resume as evidence of a prior outcome.
pub struct Connection {
    launch: Launch,
    sender: Sender,
    state: Arc<Mutex<ProtocolState>>,
    process: Arc<AsyncMutex<CLIAgentProcess>>,
    driver: AsyncMutex<Option<JoinHandle<anyhow::Result<()>>>>,
    operations: AsyncMutex<()>,
    capabilities: ClientCapabilities,
}

impl Connection {
    pub fn spawn(
        launch: &Launch,
        host: Arc<dyn Host>,
        limits: TransportLimits,
    ) -> anyhow::Result<Self> {
        limits.validate().map_err(|e| anyhow::anyhow!(e.message))?;
        let config = launch
            .process_config()
            .map_err(|e| anyhow::anyhow!(e.message))?;
        let mut process = spawn_cli_agent_process(&config)?;
        let (stdout, stdin) = process.take_stdio()?;
        let transport =
            Transport::connect(stdout, stdin, limits).map_err(|e| anyhow::anyhow!(e.message))?;
        let sender = transport.sender.clone();
        let state = Arc::new(Mutex::new(ProtocolState::default()));
        let capabilities = host.capabilities();
        let process = Arc::new(AsyncMutex::new(process));
        let driver_process = process.clone();
        let driver_state = state.clone();
        let driver_capabilities = capabilities.clone();
        let driver = tokio::spawn(async move {
            dispatch::run(
                transport,
                host.clone(),
                driver_state,
                driver_capabilities,
                limits.in_flight,
            )
            .await;
            // stdout EOF is not evidence that the process or its descendants
            // exited. Stop the exact owned process before revoking host scope.
            let stopped = driver_process
                .lock()
                .await
                .terminate_with_grace(Duration::from_secs(2))
                .await;
            host.disconnected().await;
            stopped.map(|_| ())
        });
        Ok(Self {
            launch: launch.clone(),
            sender,
            state,
            process,
            driver: AsyncMutex::new(Some(driver)),
            operations: AsyncMutex::new(()),
            capabilities,
        })
    }

    async fn request<Q: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        params: &Q,
    ) -> Result<R> {
        let value = self.sender.request(method, params)?.receive().await?;
        serde_json::from_value(value).map_err(|_| Error::new(-32603, "Invalid ACP response"))
    }

    fn session_request<Q: Serialize>(
        &self,
        method: &str,
        session_id: &SessionId,
        request: &Q,
        allow_active: bool,
    ) -> Result<PendingRequest> {
        let mut state = self.state.lock().expect("ACP state poisoned");
        let id = session_id.to_string();
        if state.pending_configuration.contains(&id)
            || (!allow_active && state.active_prompts.contains_key(&id))
        {
            return Err(Error::invalid_request());
        }
        let pending = self.sender.request(method, request)?;
        state.pending_configuration.insert(id);
        Ok(pending)
    }

    pub async fn initialize(&self) -> Result<InitializeResponse> {
        let _operation = self.operations.lock().await;
        if let Some(response) = self
            .state
            .lock()
            .expect("ACP state poisoned")
            .initialized
            .clone()
        {
            return Ok(response);
        }
        let response: InitializeResponse = self
            .request(
                "initialize",
                &InitializeRequest::new(ProtocolVersion::V1)
                    .client_info(Implementation::new("pioneer", env!("CARGO_PKG_VERSION")))
                    .client_capabilities(self.capabilities.clone()),
            )
            .await?;
        if response.protocol_version != ProtocolVersion::V1 {
            self.sender.close();
            return Err(Error::new(-32603, "Unsupported ACP protocol version"));
        }
        // The ordered dispatcher published initialization before releasing the
        // response waiter. No readiness path performs authentication.
        Ok(response)
    }

    fn negotiated(&self) -> Result<InitializeResponse> {
        self.state
            .lock()
            .expect("ACP state poisoned")
            .initialized
            .clone()
            .ok_or_else(|| Error::new(-32600, "ACP is not initialized"))
    }

    pub fn auth_methods(&self) -> Result<Vec<AuthMethod>> {
        Ok(self.negotiated()?.auth_methods)
    }

    pub async fn authenticate(&self, method_id: AuthMethodId) -> Result<AuthenticateResponse> {
        let _operation = self.operations.lock().await;
        let advertised = self
            .negotiated()?
            .auth_methods
            .into_iter()
            .find(|m| m.id() == &method_id)
            .ok_or_else(Error::invalid_params)?;
        if !matches!(advertised, AuthMethod::Agent(_)) {
            return Err(Error::invalid_params());
        }
        self.request("authenticate", &AuthenticateRequest::new(method_id))
            .await
    }

    /// Terminal authentication is a separate explicit user operation. The
    /// caller presents this invocation in its shared interactive terminal and
    /// reconnects after exit; it MUST NOT call authenticate for this descriptor.
    pub fn terminal_auth_invocation(
        &self,
        method_id: &AuthMethodId,
        auth_generation: u64,
    ) -> Result<Launch> {
        if auth_generation == 0 || auth_generation == self.launch.process_generation {
            return Err(Error::invalid_params());
        }
        if !self.capabilities.auth.terminal {
            return Err(Error::method_not_found());
        }
        let descriptor = self
            .negotiated()?
            .auth_methods
            .into_iter()
            .find(|m| m.id() == method_id)
            .ok_or_else(Error::invalid_params)?;
        let AuthMethod::Terminal(method) = descriptor else {
            return Err(Error::invalid_params());
        };
        let mut invocation = self.launch.clone();
        invocation.process_generation = auth_generation;
        invocation.args.extend(method.args);
        for (name, value) in method.env {
            invocation
                .env
                .insert_secret(name, crate::process::SecretString::new(value));
        }
        Ok(invocation)
    }

    pub async fn logout(&self) -> Result<LogoutResponse> {
        let _operation = self.operations.lock().await;
        if self.negotiated()?.agent_capabilities.auth.logout.is_none() {
            return Err(Error::method_not_found());
        }
        let response = self.request("logout", &LogoutRequest::new()).await;
        // Session grants after logout are not guaranteed by the protocol.
        self.sender.close();
        response
    }

    fn validate_setup(
        &self,
        cwd: &std::path::Path,
        directories: &[std::path::PathBuf],
        servers: &[McpServer],
    ) -> Result<()> {
        let capabilities = self.negotiated()?.agent_capabilities;
        if !cwd.is_absolute() || directories.iter().any(|p| !p.is_absolute()) {
            return Err(Error::invalid_params());
        }
        if !directories.is_empty()
            && capabilities
                .session_capabilities
                .additional_directories
                .is_none()
        {
            return Err(Error::method_not_found());
        }
        for server in servers {
            match server {
                McpServer::Stdio(_) => {}
                McpServer::Http(_) if capabilities.mcp_capabilities.http => {}
                McpServer::Sse(_) if capabilities.mcp_capabilities.sse => {}
                _ => return Err(Error::method_not_found()),
            }
        }
        Ok(())
    }

    pub async fn new_session(&self, request: NewSessionRequest) -> Result<NewSessionResponse> {
        let _operation = self.operations.lock().await;
        self.validate_setup(
            &request.cwd,
            &request.additional_directories,
            &request.mcp_servers,
        )?;
        self.request("session/new", &request).await
    }

    pub async fn load_session(&self, request: LoadSessionRequest) -> Result<LoadSessionResponse> {
        let _operation = self.operations.lock().await;
        if !self.negotiated()?.agent_capabilities.load_session {
            return Err(Error::method_not_found());
        }
        self.validate_setup(
            &request.cwd,
            &request.additional_directories,
            &request.mcp_servers,
        )?;
        let pending = self.session_request("session/load", &request.session_id, &request, false)?;
        serde_json::from_value(pending.receive().await?).map_err(|_| Error::internal_error())
    }

    pub async fn resume_session(
        &self,
        request: ResumeSessionRequest,
    ) -> Result<ResumeSessionResponse> {
        let _operation = self.operations.lock().await;
        if self
            .negotiated()?
            .agent_capabilities
            .session_capabilities
            .resume
            .is_none()
        {
            return Err(Error::method_not_found());
        }
        self.validate_setup(
            &request.cwd,
            &request.additional_directories,
            &request.mcp_servers,
        )?;
        let pending =
            self.session_request("session/resume", &request.session_id, &request, false)?;
        serde_json::from_value(pending.receive().await?).map_err(|_| Error::internal_error())
    }

    pub async fn list_sessions(
        &self,
        request: ListSessionsRequest,
    ) -> Result<ListSessionsResponse> {
        if self
            .negotiated()?
            .agent_capabilities
            .session_capabilities
            .list
            .is_none()
        {
            return Err(Error::method_not_found());
        }
        if request.cwd.as_ref().is_some_and(|p| !p.is_absolute()) {
            return Err(Error::invalid_params());
        }
        self.request("session/list", &request).await
    }

    pub async fn delete_session(
        &self,
        request: DeleteSessionRequest,
    ) -> Result<DeleteSessionResponse> {
        let _operation = self.operations.lock().await;
        if self
            .negotiated()?
            .agent_capabilities
            .session_capabilities
            .delete
            .is_none()
        {
            return Err(Error::method_not_found());
        }
        let pending =
            self.session_request("session/delete", &request.session_id, &request, true)?;
        serde_json::from_value(pending.receive().await?).map_err(|_| Error::internal_error())
    }

    pub async fn close_session(
        &self,
        request: CloseSessionRequest,
    ) -> Result<CloseSessionResponse> {
        let _operation = self.operations.lock().await;
        if self
            .negotiated()?
            .agent_capabilities
            .session_capabilities
            .close
            .is_none()
        {
            return Err(Error::method_not_found());
        }
        let pending = self.session_request("session/close", &request.session_id, &request, true)?;
        serde_json::from_value(pending.receive().await?).map_err(|_| Error::internal_error())
    }

    pub fn configuration(&self, session_id: &SessionId) -> Option<ConfigurationSnapshot> {
        self.state
            .lock()
            .expect("ACP state poisoned")
            .configurations
            .get(&session_id.to_string())
            .cloned()
    }

    pub async fn set_config_option(
        &self,
        request: SetSessionConfigOptionRequest,
        expected: ConfigurationVersion,
    ) -> Result<SetSessionConfigOptionResponse> {
        let _operation = self.operations.lock().await;
        self.negotiated()?;
        let (pending, choice) = {
            let mut state = self.state.lock().expect("ACP state poisoned");
            let snapshot = state
                .configurations
                .get(&request.session_id.to_string())
                .ok_or_else(Error::invalid_params)?;
            if snapshot.version() != expected
                || state
                    .pending_configuration
                    .contains(&request.session_id.to_string())
                || state
                    .active_prompts
                    .contains_key(&request.session_id.to_string())
            {
                return Err(Error::new(-32602, "Stale or active ACP configuration"));
            }
            validate_config_value(&snapshot.options, &request.config_id, &request.value)?;
            if snapshot.configuration_replay.len() >= 256 {
                return Err(Error::new(
                    -32602,
                    "ACP configuration choice budget exceeded; prepare a fresh session",
                ));
            }
            let choice = pioneer_protocol::AcpConfigurationChoice {
                descriptor_fingerprint: pioneer_protocol::acp_descriptor_fingerprint(
                    &snapshot.options,
                    snapshot.modes.as_ref(),
                )
                .map_err(|_| Error::internal_error())?,
                config_id: request.config_id.to_string(),
                value: request.value.clone(),
            };
            let choice_bytes = serde_json::to_vec(&choice)
                .map_err(|_| Error::internal_error())?
                .len();
            let retained =
                snapshot
                    .configuration_replay
                    .iter()
                    .try_fold(0usize, |bytes, choice| {
                        serde_json::to_vec(choice)
                            .map(|value| bytes.saturating_add(value.len()))
                            .map_err(|_| Error::internal_error())
                    })?;
            if choice_bytes > (256usize * 1024).saturating_sub(retained) {
                return Err(Error::new(
                    -32602,
                    "ACP configuration choice byte budget exceeded",
                ));
            }
            let pending = self.sender.request("session/set_config_option", &request)?;
            state
                .pending_configuration
                .insert(request.session_id.to_string());
            (pending, choice)
        };
        let response = serde_json::from_value(pending.receive().await?)
            .map_err(|_| Error::internal_error())?;
        self.state
            .lock()
            .expect("ACP state poisoned")
            .configurations
            .get_mut(&request.session_id.to_string())
            .ok_or_else(Error::invalid_params)?
            .configuration_replay
            .push(choice);
        Ok(response)
    }

    pub async fn set_mode(
        &self,
        request: SetSessionModeRequest,
        expected: ConfigurationVersion,
    ) -> Result<SetSessionModeResponse> {
        let _operation = self.operations.lock().await;
        self.negotiated()?;
        let pending = {
            let mut state = self.state.lock().expect("ACP state poisoned");
            let snapshot = state
                .configurations
                .get(&request.session_id.to_string())
                .ok_or_else(Error::invalid_params)?;
            if snapshot.version() != expected
                || snapshot.has_config_options
                || state
                    .pending_configuration
                    .contains(&request.session_id.to_string())
                || state
                    .active_prompts
                    .contains_key(&request.session_id.to_string())
            {
                return Err(Error::new(
                    -32602,
                    "Stale or active configuration, or configOptions supersede legacy modes",
                ));
            }
            let modes = snapshot.modes.as_ref().ok_or_else(Error::invalid_params)?;
            if !modes
                .available_modes
                .iter()
                .any(|m| m.id == request.mode_id)
            {
                return Err(Error::invalid_params());
            }
            let pending = self.sender.request("session/set_mode", &request)?;
            state
                .pending_configuration
                .insert(request.session_id.to_string());
            pending
        };
        serde_json::from_value(pending.receive().await?).map_err(|_| Error::internal_error())
    }

    /// Registration and enqueue occur together; the returned handle can be
    /// journaled by Gateway before it starts awaiting completion. Gateway must
    /// commit its dispatch fence BEFORE calling this method.
    pub fn validate_prompt_frame(&self, request: &PromptRequest) -> Result<()> {
        self.sender.validate_request("session/prompt", request)
    }

    pub fn prompt(
        &self,
        request: PromptRequest,
        expected: ConfigurationVersion,
    ) -> Result<PendingRequest> {
        let caps = self.negotiated()?.agent_capabilities.prompt_capabilities;
        for content in &request.prompt {
            match content {
                ContentBlock::Text(_) | ContentBlock::ResourceLink(_) => {}
                ContentBlock::Image(_) if caps.image => {}
                ContentBlock::Audio(_) if caps.audio => {}
                ContentBlock::Resource(_) if caps.embedded_context => {}
                _ => return Err(Error::method_not_found()),
            }
        }
        let mut state = self.state.lock().expect("ACP state poisoned");
        let id = request.session_id.to_string();
        let snapshot = state
            .configurations
            .get(&id)
            .ok_or_else(Error::invalid_params)?;
        if snapshot.version() != expected
            || state.active_prompts.contains_key(&id)
            || state.pending_configuration.contains(&id)
        {
            return Err(Error::new(
                -32602,
                "Stale configuration or active ACP prompt",
            ));
        }
        self.sender.begin_prompt(&id);
        let pending = self.sender.request("session/prompt", &request)?;
        state.active_prompts.insert(id, pending.id().clone());
        Ok(pending)
    }

    pub fn cancel(&self, session_id: SessionId) -> Result<()> {
        self.negotiated()?;
        let state = self.state.lock().expect("ACP state poisoned");
        if !state.configurations.contains_key(&session_id.to_string()) {
            return Err(Error::invalid_params());
        }
        // Also reaches the local dispatcher to settle pending permissions.
        dispatch::cancel_permissions(&self.sender, &session_id)?;
        self.sender
            .notify("session/cancel", &CancelNotification::new(session_id))
            .inspect_err(|_| self.sender.close())
    }

    /// The caller disarms only after its mutation and publication are confirmed.
    /// A dropped preparation cannot leave a reusable uncertain transport.
    pub fn retirement_guard(&self) -> tokio_util::sync::DropGuard {
        self.sender.cancellation().drop_guard()
    }

    /// Stop the exact owned process without waiting for presentation ingress.
    /// The existing driver still owns/join-cleans its callbacks and EOF events.
    pub async fn retire(&self) -> anyhow::Result<()> {
        self.sender.close();
        self.process
            .lock()
            .await
            .terminate_with_grace(Duration::from_secs(2))
            .await?;
        Ok(())
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        let process_result = self.retire().await;
        if let Some(driver) = self.driver.lock().await.take() {
            driver.await??;
        }
        process_result?;
        Ok(())
    }

    pub async fn process_id(&self) -> Option<u32> {
        self.process.lock().await.id()
    }

    pub fn negotiated_capabilities(&self) -> Result<AgentCapabilities> {
        Ok(self.negotiated()?.agent_capabilities)
    }

    pub fn is_closed(&self) -> bool {
        self.sender.cancellation().is_cancelled()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.sender.close();
        if let Some(driver) = self.driver.get_mut().take() {
            driver.abort();
        }
        // CLIAgentProcess owns KillOnDrop and process-group teardown.
    }
}

pub(crate) fn value<T: Serialize>(input: T) -> Result<Value> {
    serde_json::to_value(input).map_err(|_| Error::internal_error())
}
