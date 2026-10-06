#![allow(dead_code)]
// Owns reusable CLI runtime sessions across provider kinds.

use crate::cli_runtime::continuation::{CliSessionLaunchSpec, requires_restart};
use crate::cli_runtime::session_instance::{CliSessionGenerationAllocator, CliSessionInstanceId};
use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use pioneer_cli_agent_runtime::codex::{
    CodexJsonlRpcClientDiagnostic, CodexJsonlRpcNotificationEvent, CodexJsonlRpcServerRequest,
};
use pioneer_cli_agent_runtime::event::RuntimeEvent;
use pioneer_cli_agent_runtime::instructions::CLIRuntimeElevatedInstructions;
use pioneer_cli_agent_runtime::process::SensitiveEnvironment;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{
    Mutex as StdMutex,
    atomic::{AtomicBool, Ordering},
};
#[path = "manager_ownership.rs"]
mod ownership;
use ownership::{CLI_CALLBACK_INSTANCE, CliSessionOwner, StartupWaitGuard};
pub(crate) use ownership::{
    CLIAgentRuntimeSessionStartup, CliSessionStopOwner, CliStartupProcessCleanup,
};
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, mpsc};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CLIAgentRuntimeSessionKey {
    pub workspace_id: String,
    pub runtime_id: String,
    pub thread_id: String,
}

impl CLIAgentRuntimeSessionKey {
    pub(crate) fn new(
        workspace_id: impl Into<String>,
        runtime_id: impl Into<String>,
        thread_id: impl Into<String>,
    ) -> Result<Self> {
        let workspace_id = normalize_key_part(workspace_id.into(), "workspace_id")?;
        let runtime_id = normalize_key_part(runtime_id.into(), "runtime_id")?;
        let thread_id = normalize_key_part(thread_id.into(), "thread_id")?;
        Ok(Self {
            workspace_id,
            runtime_id,
            thread_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CLIAgentRuntimeThreadCompactRequest {
    pub native_thread_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CLIAgentRuntimeThreadCompactResult {
    pub native_thread_id: String,
    pub raw: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CLIAgentRuntimeThreadNameSetRequest {
    pub native_thread_id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CLIAgentRuntimeThreadNameSetResult {
    pub native_thread_id: String,
    pub raw: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CLIAgentRuntimeThreadForkRequest {
    pub source_logical_thread_id: String,
    pub native_thread_id: String,
    pub last_turn_id: Option<String>,
    pub thread_source: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CLIAgentRuntimeThreadForkResult {
    pub native_thread_id: String,
    pub native_cwd: Option<String>,
    pub native_model: Option<String>,
    pub raw: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CLIAgentRuntimeTurnSteerRequest {
    pub native_thread_id: String,
    pub native_turn_id: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CLIAgentRuntimeTurnSteerResult {
    pub native_thread_id: String,
    pub native_turn_id: String,
    pub raw: Option<JsonValue>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CLIAgentRuntimeSessionStartOptions {
    pub cwd: Option<PathBuf>,
    pub approval_policy: Option<String>,
    /// Stable identity of the principal, role, permission ceiling and maximum
    /// security authority allowed to share provider-side session grants.
    pub authorization_scope_fingerprint: Option<String>,
    pub app_server_args: Vec<String>,
    pub env: SensitiveEnvironment,
    /// Exact selected Claude skill installations. Claude receives these as
    /// generation-scoped managed plugins; the shared user settings source is
    /// never enabled for skill discovery.
    pub selected_skills: Vec<super::skills::CliRuntimeSelectedSkill>,
    pub elevated_instructions: Option<CLIRuntimeElevatedInstructions>,
}

pub(crate) struct CLIAgentRuntimeCodexEventReceivers {
    pub process_instance: CliSessionInstanceId,
    pub notifications: mpsc::Receiver<CodexJsonlRpcNotificationEvent>,
    pub server_requests: mpsc::Receiver<CodexJsonlRpcServerRequest>,
    pub diagnostics: mpsc::Receiver<CodexJsonlRpcClientDiagnostic>,
}

pub(crate) struct CLIAgentRuntimeEventReceivers {
    pub process_instance: CliSessionInstanceId,
    pub runtime_kind: String,
    pub events: mpsc::Receiver<RuntimeEvent>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CLIAgentRuntimeThreadOpenParams {
    pub cwd: String,
    pub model: Option<String>,
    pub approval_policy: Option<String>,
    pub sandbox: Option<JsonValue>,
    pub permissions: Option<String>,
    pub service_tier: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CLIAgentRuntimeThreadOpenSnapshot {
    pub native_thread_id: String,
    pub cwd: Option<String>,
    pub model: Option<String>,
    pub raw: JsonValue,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CLIAgentRuntimeTurnStartParams {
    pub native_thread_id: String,
    pub input: JsonValue,
    pub cwd: Option<String>,
    pub model: Option<String>,
    pub approval_policy: Option<String>,
    pub sandbox: Option<JsonValue>,
    pub permissions: Option<String>,
    pub effort: Option<String>,
    pub personality: Option<String>,
    pub summary: Option<String>,
    pub elevated_instructions: CLIRuntimeElevatedInstructions,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CLIAgentRuntimeTurnStartSnapshot {
    pub native_thread_id: String,
    pub native_turn_id: String,
    pub raw: JsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CLIAgentRuntimeMcpTurnMetadata {
    pub adapter_kind: String,
    pub manifest_hash: String,
    pub projection_fingerprint: String,
    pub provider_contract_fingerprint: String,
    pub isolation_contract_fingerprint: String,
    pub session_generation: u64,
    pub projection_activation_generation: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CLIAgentRuntimeNativeMcpApprovalRequest {
    pub native_thread_id: String,
    pub native_turn_id: String,
    pub native_item_id: String,
    pub requested_permissions: JsonValue,
}

pub(crate) use pioneer_runtime_events::ExecutionTurnStatus as CLIAgentRuntimeObservedTurnStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CLIAgentRuntimeTurnLivenessProbe {
    /// The provider confirms that the native thread currently owns a live
    /// Turn. No history hydration is required.
    ConfirmedActive,
    /// The provider is not currently active, so the bound Turn must be read
    /// from its authoritative terminal snapshot.
    SnapshotRequired,
    /// The provider cannot currently establish either condition.
    Unavailable,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CLIAgentRuntimeTurnObservation {
    pub status: CLIAgentRuntimeObservedTurnStatus,
    pub message: Option<String>,
    /// Canonical runtime events reconstructed from an authoritative snapshot.
    /// They are replayed before a repaired terminal lifecycle event.
    pub reconciliation_events: Vec<RuntimeEvent>,
}

#[async_trait]
pub(crate) trait CLIAgentRuntimeSession: Send + Sync {
    async fn close(&self) -> Result<()>;
    /// Separate native drain from root cleanup so Gateway callbacks drain first.
    async fn stop_and_wait(&self) -> Result<()> {
        self.close().await
    }
    async fn cleanup_after_stop(&self) -> Result<()> {
        Ok(())
    }

    /// Provider-specific barrier used only for a launch-contract replacement.
    /// The cached generation remains published when this fails.
    async fn prepare_for_replacement(&self) -> Result<()> {
        Ok(())
    }

    /// Confirm the provider continuity checkpoint after the old process has
    /// closed but before its generation is revoked and a replacement starts.
    async fn confirm_replacement_checkpoint(&self) -> Result<()> {
        Ok(())
    }

    fn take_codex_event_receivers(&self) -> Option<CLIAgentRuntimeCodexEventReceivers> {
        None
    }

    fn take_event_receivers(&self) -> Option<CLIAgentRuntimeEventReceivers> {
        None
    }

    fn supports_thread_name_sync(&self) -> bool {
        false
    }

    async fn start_thread(
        &self,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        let _ = (params, timeout);
        bail!("CLI runtime session does not support thread start");
    }

    async fn resume_thread(
        &self,
        native_thread_id: &str,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        let _ = (native_thread_id, params, timeout);
        bail!("CLI runtime session does not support thread resume");
    }

    async fn start_turn(
        &self,
        params: CLIAgentRuntimeTurnStartParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeTurnStartSnapshot> {
        let _ = (params, timeout);
        bail!("CLI runtime session does not support turn start");
    }

    async fn prepare_mcp_turn(
        &self,
        _pioneer_thread_id: &str,
        _pioneer_turn_id: &str,
    ) -> Result<Option<CLIAgentRuntimeMcpTurnMetadata>> {
        Ok(None)
    }

    async fn activate_mcp_turn(
        &self,
        _pioneer_turn_id: &str,
        _native_thread_id: &str,
        _native_turn_id: &str,
    ) -> Result<()> {
        Ok(())
    }

    async fn retarget_mcp_turn(
        &self,
        _pioneer_turn_id: &str,
        _native_thread_id: &str,
        _native_turn_id: &str,
    ) -> Result<()> {
        Ok(())
    }

    async fn terminal_mcp_turn(&self, _pioneer_turn_id: &str) -> Result<()> {
        Ok(())
    }

    async fn reset_native_thread_goal(&self, _native_thread_id: &str) -> Result<()> {
        Ok(())
    }

    async fn clear_native_thread_goal(&self, _native_thread_id: &str) -> Result<()> {
        Ok(())
    }

    async fn native_mcp_approval_response(
        &self,
        _request: CLIAgentRuntimeNativeMcpApprovalRequest,
    ) -> Result<Option<JsonValue>> {
        Ok(None)
    }

    async fn mcp_permission_fallback_count(&self) -> Result<usize> {
        Ok(0)
    }

    fn enrich_runtime_event(&self, _event: &mut RuntimeEvent) -> Result<()> {
        Ok(())
    }

    async fn respond_to_request(
        &self,
        native_request_id: JsonValue,
        response: JsonValue,
    ) -> Result<()> {
        let _ = (native_request_id, response);
        bail!("CLI runtime session does not support server request responses");
    }

    async fn fail_request(
        &self,
        native_request_id: JsonValue,
        code: i64,
        message: String,
        data: Option<JsonValue>,
    ) -> Result<()> {
        let _ = (native_request_id, code, message, data);
        bail!("CLI runtime session does not support server request error responses");
    }

    async fn interrupt_turn(
        &self,
        native_thread_id: Option<&str>,
        native_turn_id: Option<&str>,
    ) -> Result<()> {
        let _ = (native_thread_id, native_turn_id);
        bail!("CLI runtime session does not support turn interrupt");
    }

    async fn probe_turn_liveness(
        &self,
        _native_thread_id: &str,
        _native_turn_id: &str,
    ) -> Result<CLIAgentRuntimeTurnLivenessProbe> {
        Ok(CLIAgentRuntimeTurnLivenessProbe::SnapshotRequired)
    }

    async fn load_turn_snapshot(
        &self,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> Result<Option<CLIAgentRuntimeTurnObservation>> {
        let _ = (native_thread_id, native_turn_id);
        Ok(None)
    }

    async fn thread_compact(
        &self,
        request: CLIAgentRuntimeThreadCompactRequest,
    ) -> Result<CLIAgentRuntimeThreadCompactResult> {
        let _ = request;
        bail!("CLI runtime session does not support thread compaction");
    }

    async fn set_thread_name(
        &self,
        request: CLIAgentRuntimeThreadNameSetRequest,
    ) -> Result<CLIAgentRuntimeThreadNameSetResult> {
        let _ = request;
        bail!("CLI runtime session does not support thread name sync");
    }

    async fn fork_thread(
        &self,
        request: CLIAgentRuntimeThreadForkRequest,
    ) -> Result<CLIAgentRuntimeThreadForkResult> {
        let _ = request;
        bail!("CLI runtime session does not support thread fork");
    }

    async fn find_marked_fork(
        &self,
        _source_thread_id: &str,
        _boundary_turn_id: &str,
        _marker: &str,
    ) -> Result<Option<CLIAgentRuntimeThreadForkResult>> {
        bail!("CLI runtime session does not support fork reconciliation");
    }

    async fn steer_turn(
        &self,
        request: CLIAgentRuntimeTurnSteerRequest,
    ) -> Result<CLIAgentRuntimeTurnSteerResult> {
        let _ = request;
        bail!("CLI runtime session does not support turn steering");
    }
}

#[async_trait]
pub(crate) trait CLIAgentRuntimeSessionFactory: Send + Sync {
    async fn start_session_with_launch_spec(
        &self,
        instance: &CliSessionInstanceId,
        launch_spec: &CliSessionLaunchSpec,
        startup: &CLIAgentRuntimeSessionStartup,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>>;
}

/// Exact-generation lifecycle hooks for resources that surround a provider
/// process (for example the private CLI MCP bridge). Hooks never perform
/// logical-key lookup and are called outside the manager's session locks.
#[async_trait]
pub(crate) trait CLIAgentRuntimeSessionLifecycle: Send + Sync {
    async fn shutdown_started(&self) {}

    async fn before_session_close(&self, _instance: &CliSessionInstanceId) {}

    async fn after_session_close(&self, _instance: &CliSessionInstanceId) {}

    async fn after_session_close_result(&self, instance: &CliSessionInstanceId) -> Result<()> {
        self.after_session_close(instance).await;
        Ok(())
    }
    async fn shutdown_finished(&self) {}
}

struct NoopCLIAgentRuntimeSessionLifecycle;

#[async_trait]
impl CLIAgentRuntimeSessionLifecycle for NoopCLIAgentRuntimeSessionLifecycle {}

#[derive(Clone)]
pub(crate) struct CLIAgentRuntimeSessionHandle {
    instance: CliSessionInstanceId,
    session: Arc<dyn CLIAgentRuntimeSession>,
}

impl CLIAgentRuntimeSessionHandle {
    pub(crate) fn key(&self) -> &CLIAgentRuntimeSessionKey {
        self.instance.key()
    }

    pub(crate) fn instance(&self) -> &CliSessionInstanceId {
        &self.instance
    }

    pub(crate) fn session(&self) -> Arc<dyn CLIAgentRuntimeSession> {
        self.session.clone()
    }

    #[cfg(test)]
    fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.session, &other.session)
    }
}

/// A response route captured from the exact native process that emitted a
/// machine request. It must never be reconstructed through a logical-key
/// lookup because that key may already point at a replacement generation.
#[derive(Clone)]
pub(crate) struct CLIAgentRuntimeMachineRequestResponder {
    instance: CliSessionInstanceId,
    native_request_id: JsonValue,
    session: Arc<dyn CLIAgentRuntimeSession>,
}

impl std::fmt::Debug for CLIAgentRuntimeMachineRequestResponder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CLIAgentRuntimeMachineRequestResponder")
            .field("instance", &self.instance)
            .field("native_request_id", &self.native_request_id)
            .finish_non_exhaustive()
    }
}

impl CLIAgentRuntimeMachineRequestResponder {
    pub(crate) fn new(
        instance: CliSessionInstanceId,
        native_request_id: JsonValue,
        session: Arc<dyn CLIAgentRuntimeSession>,
    ) -> Self {
        Self {
            instance,
            native_request_id,
            session,
        }
    }

    pub(crate) fn instance(&self) -> &CliSessionInstanceId {
        &self.instance
    }

    pub(crate) fn native_request_id(&self) -> &JsonValue {
        &self.native_request_id
    }

    pub(crate) fn session(&self) -> Arc<dyn CLIAgentRuntimeSession> {
        self.session.clone()
    }

    pub(crate) async fn respond(&self, response: JsonValue) -> Result<()> {
        self.session
            .respond_to_request(self.native_request_id.clone(), response)
            .await
    }

    pub(crate) async fn fail(
        &self,
        code: i64,
        message: impl Into<String>,
        data: Option<JsonValue>,
    ) -> Result<()> {
        self.session
            .fail_request(self.native_request_id.clone(), code, message.into(), data)
            .await
    }
}

#[derive(Clone)]
struct CLIAgentRuntimeCachedSession {
    instance: CliSessionInstanceId,
    owner: Arc<CliSessionOwner>,
    launch_spec: CliSessionLaunchSpec,
    started_at_ms: u64,
    last_used_at_ms: u64,
}
impl CLIAgentRuntimeCachedSession {
    fn handle(&self) -> Option<CLIAgentRuntimeSessionHandle> {
        if !self.owner.ready.load(Ordering::Acquire) || self.owner.closing.load(Ordering::Acquire) {
            return None;
        }
        Some(CLIAgentRuntimeSessionHandle {
            instance: self.instance.clone(),
            session: self.owner.startup.session()?,
        })
    }
}
pub(crate) struct CLIAgentRuntimeManager {
    factory: Arc<dyn CLIAgentRuntimeSessionFactory>,
    lifecycle: Arc<dyn CLIAgentRuntimeSessionLifecycle>,
    idle_session_ttl: Duration,
    // Only synchronous map access. Never retain this guard over any await.
    sessions: StdMutex<HashMap<CLIAgentRuntimeSessionKey, CLIAgentRuntimeCachedSession>>,
    start_locks: Mutex<HashMap<CLIAgentRuntimeSessionKey, Weak<Mutex<()>>>>,
    generations: CliSessionGenerationAllocator,
    shutting_down: AtomicBool,
}

impl CLIAgentRuntimeManager {
    pub(crate) fn new(
        factory: Arc<dyn CLIAgentRuntimeSessionFactory>,
        idle_session_ttl: Duration,
    ) -> Result<Self> {
        Self::new_with_lifecycle(
            factory,
            idle_session_ttl,
            Arc::new(NoopCLIAgentRuntimeSessionLifecycle),
        )
    }

    pub(crate) fn new_with_lifecycle(
        factory: Arc<dyn CLIAgentRuntimeSessionFactory>,
        idle_session_ttl: Duration,
        lifecycle: Arc<dyn CLIAgentRuntimeSessionLifecycle>,
    ) -> Result<Self> {
        if idle_session_ttl.is_zero() {
            bail!("CLI runtime idle session TTL must be greater than zero");
        }
        Ok(Self {
            factory,
            lifecycle,
            idle_session_ttl,
            sessions: StdMutex::new(HashMap::new()),
            shutting_down: AtomicBool::new(false),
            start_locks: Mutex::new(HashMap::new()),
            generations: CliSessionGenerationAllocator::default(),
        })
    }

    #[cfg(test)]
    pub(crate) async fn get_or_start(
        &self,
        key: CLIAgentRuntimeSessionKey,
    ) -> Result<CLIAgentRuntimeSessionHandle> {
        self.get_or_start_with_options(key, CLIAgentRuntimeSessionStartOptions::default())
            .await
    }

    #[cfg(test)]
    pub(crate) async fn get_or_start_with_options(
        &self,
        key: CLIAgentRuntimeSessionKey,
        options: CLIAgentRuntimeSessionStartOptions,
    ) -> Result<CLIAgentRuntimeSessionHandle> {
        self.get_or_start_with_launch_spec(key, CliSessionLaunchSpec::unmanaged_codex(options))
            .await
    }

    pub(crate) async fn get_or_start_with_launch_spec(
        &self,
        key: CLIAgentRuntimeSessionKey,
        launch_spec: CliSessionLaunchSpec,
    ) -> Result<CLIAgentRuntimeSessionHandle> {
        self.get_or_start_with_launch_spec_at(key, launch_spec, current_time_millis())
            .await
    }

    #[cfg(test)]
    async fn get_or_start_at(
        &self,
        key: CLIAgentRuntimeSessionKey,
        options: CLIAgentRuntimeSessionStartOptions,
        now_ms: u64,
    ) -> Result<CLIAgentRuntimeSessionHandle> {
        self.get_or_start_with_launch_spec_at(
            key,
            CliSessionLaunchSpec::unmanaged_codex(options),
            now_ms,
        )
        .await
    }

    async fn get_or_start_with_launch_spec_at(
        &self,
        key: CLIAgentRuntimeSessionKey,
        launch_spec: CliSessionLaunchSpec,
        now_ms: u64,
    ) -> Result<CLIAgentRuntimeSessionHandle> {
        if CLI_CALLBACK_INSTANCE
            .try_with(|id| id.key() == &key)
            .unwrap_or(false)
        {
            bail!("CLI callback cannot wait for acquisition of its own logical session");
        }
        let start_lock = self.start_lock_for_key(&key).await;
        let startup_wait = pioneer_observability::turn_startup::current_stage(
            pioneer_observability::turn_startup::Stage::CliSessionWait,
        );
        let _guard = start_lock.lock().await;
        drop(startup_wait);
        if self.shutting_down.load(Ordering::Acquire) {
            bail!("CLI manager is shutting down");
        }

        if let Some(handle) = self
            .touch_reusable_session(&key, &launch_spec, now_ms)
            .await
        {
            pioneer_observability::turn_startup::session_state(
                pioneer_observability::turn_startup::SessionState::Reused,
            );
            return Ok(handle);
        }
        pioneer_observability::turn_startup::session_state(
            pioneer_observability::turn_startup::SessionState::New,
        );
        if let Some(stale) = self.cached(&key) {
            if CLI_CALLBACK_INSTANCE
                .try_with(|id| id == &stale.instance)
                .unwrap_or(false)
            {
                stale.owner.begin_close(self.lifecycle.clone());
                bail!("CLI callback replacement requires its own drain");
            }
            if !stale.owner.closing.load(Ordering::Acquire) {
                validate_replacement_continuation(&stale.launch_spec, &launch_spec)?;
                let session = stale
                    .owner
                    .startup
                    .session()
                    .ok_or_else(|| anyhow!("CLI startup is unresolved"))?;
                session.prepare_for_replacement().await?;
                stale
                    .owner
                    .replacement_prepared
                    .store(true, Ordering::Release);
            }
            stale.owner.close_and_wait(self.lifecycle.clone()).await?;
            if stale.owner.replacement_prepared.load(Ordering::Acquire) {
                if let Some(session) = stale.owner.startup.session() {
                    session.confirm_replacement_checkpoint().await?;
                }
            }
            self.release_stopped(&stale.owner)?;
        }

        self.start_and_publish_locked(key, launch_spec, now_ms)
            .await
    }

    /// Obtain an existing process for a management operation without ever
    /// replacing its active turn launch contract. An isolated empty process is
    /// created only when the logical key has no cached generation.
    pub(crate) async fn existing_or_start_management(
        &self,
        key: CLIAgentRuntimeSessionKey,
        options: CLIAgentRuntimeSessionStartOptions,
    ) -> Result<CLIAgentRuntimeSessionHandle> {
        if CLI_CALLBACK_INSTANCE
            .try_with(|id| id.key() == &key)
            .unwrap_or(false)
        {
            bail!("CLI callback cannot wait for acquisition of its own logical session");
        }
        let start_lock = self.start_lock_for_key(&key).await;
        let _guard = start_lock.lock().await;
        if self.shutting_down.load(Ordering::Acquire) {
            bail!("CLI manager is shutting down");
        }
        let now_ms = current_time_millis();
        if let Some(handle) = self.touch_any_existing_session(&key, now_ms).await {
            return Ok(handle);
        }
        self.start_and_publish_locked(key, CliSessionLaunchSpec::codex_management(options), now_ms)
            .await
    }

    async fn start_and_publish_locked(
        &self,
        key: CLIAgentRuntimeSessionKey,
        launch_spec: CliSessionLaunchSpec,
        now_ms: u64,
    ) -> Result<CLIAgentRuntimeSessionHandle> {
        let instance = self.generations.allocate(key.clone())?;
        let owner = CliSessionOwner::new(instance.clone());
        {
            let mut sessions = self.sessions.lock().expect("CLI registry poisoned");
            if self.shutting_down.load(Ordering::Acquire) {
                bail!("CLI manager is shutting down");
            }
            if sessions.contains_key(&key) {
                bail!("CLI predecessor stop is unresolved");
            }
            sessions.insert(
                key.clone(),
                CLIAgentRuntimeCachedSession {
                    instance: instance.clone(),
                    owner: owner.clone(),
                    launch_spec: launch_spec.clone(),
                    started_at_ms: now_ms,
                    last_used_at_ms: now_ms,
                },
            );
            // Registry publication and actual factory task registration are synchronous.
            let factory = self.factory.clone();
            let startup = owner.startup.clone();
            let factory_owner = owner.clone();
            let lifecycle = self.lifecycle.clone();
            owner.spawn_start_task(async move {
                let result = factory
                    .start_session_with_launch_spec(&instance, &launch_spec, &startup)
                    .await
                    .map_err(|e| format!("{e:#}"));
                let outcome = result.map(|session| {
                    startup.retain_session(session);
                });
                if outcome.is_err() || factory_owner.closing.load(Ordering::Acquire) {
                    factory_owner.begin_close(lifecycle);
                }
                outcome
            });
        }
        let mut waiter = StartupWaitGuard(owner.clone(), false, self.lifecycle.clone());
        if let Err(error) = owner.finish_startup().await {
            owner.request_stop();
            // Failed cleanup stays in this same registry entry for repeat.
            owner.close_and_wait(self.lifecycle.clone()).await?;
            self.release_stopped(&owner)?;
            return Err(error);
        }
        let sessions = self.sessions.lock().expect("CLI registry poisoned");
        let current = sessions
            .get(&key)
            .filter(|cached| cached.instance == owner.instance)
            .ok_or_else(|| anyhow!("CLI startup identity changed"))?;
        if owner.closing.load(Ordering::Acquire) || self.shutting_down.load(Ordering::Acquire) {
            bail!("CLI startup was stopped");
        }
        owner.ready.store(true, Ordering::Release);
        waiter.1 = true;
        current
            .handle()
            .ok_or_else(|| anyhow!("CLI startup did not retain its session"))
    }

    fn cached(&self, key: &CLIAgentRuntimeSessionKey) -> Option<CLIAgentRuntimeCachedSession> {
        self.sessions
            .lock()
            .expect("CLI registry poisoned")
            .get(key)
            .cloned()
    }
    fn release_stopped(&self, owner: &Arc<CliSessionOwner>) -> Result<()> {
        if !owner.stopped.load(Ordering::Acquire) {
            bail!("CLI native stop is unresolved");
        }
        let mut sessions = self.sessions.lock().expect("CLI registry poisoned");
        if sessions
            .get(owner.instance.key())
            .is_some_and(|current| current.instance == owner.instance)
        {
            sessions.remove(owner.instance.key());
        }
        Ok(())
    }
    /// Includes accepted factory startup and failed/closing native owners.
    pub(crate) fn stop_inventory(&self, workspace_id: &str) -> Vec<CliSessionStopOwner> {
        self.sessions
            .lock()
            .expect("CLI registry poisoned")
            .values()
            .filter(|entry| entry.instance.key().workspace_id == workspace_id)
            .map(|entry| CliSessionStopOwner(entry.owner.clone()))
            .collect()
    }
    pub(crate) fn capture_stop_owner(
        &self,
        instance: &CliSessionInstanceId,
    ) -> Result<CliSessionStopOwner> {
        self.cached(instance.key())
            .filter(|entry| entry.instance == *instance)
            .map(|entry| CliSessionStopOwner(entry.owner))
            .ok_or_else(|| anyhow!("CLI instance stop owner is unknown"))
    }
    /// Captured actual owner, caller deadline, never late logical-key lookup.
    pub(crate) async fn stop_and_wait(
        &self,
        captured: &CliSessionStopOwner,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        // Startup holds the key lock while awaiting its factory. Deliver stop
        // to the captured owner before waiting for that lock or caller deadline.
        captured.0.begin_close(self.lifecycle.clone());
        if CLI_CALLBACK_INSTANCE
            .try_with(|id| id == captured.instance())
            .unwrap_or(false)
        {
            bail!("CLI callback requested stop; its own completion cannot be acknowledged here");
        }
        tokio::time::timeout_at(deadline, async {
            let lock = self.start_lock_for_key(captured.instance().key()).await;
            let _guard = lock.lock().await;
            captured.0.close_and_wait(self.lifecycle.clone()).await?;
            self.release_stopped(&captured.0)
        })
        .await
        .map_err(|_| anyhow!("CLI native stop deadline expired; owner retained"))?
    }
    pub(crate) fn spawn_instance_work<F>(&self, instance: &CliSessionInstanceId, future: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.capture_stop_owner(instance)
            .is_ok_and(|owner| owner.0.spawn_callback(future))
    }
    pub(crate) fn record_instance_failure(&self, instance: &CliSessionInstanceId, error: String) {
        if let Ok(owner) = self.capture_stop_owner(instance) {
            owner.0.record_callback_failure(error);
        }
    }
    pub(crate) fn instance_cancellation(
        &self,
        instance: &CliSessionInstanceId,
    ) -> Result<tokio_util::sync::CancellationToken> {
        Ok(self.capture_stop_owner(instance)?.0.cancellation.clone())
    }
    pub(crate) async fn close_session_if_started_at_or_before(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        cutoff_ms: u64,
    ) -> Result<bool> {
        let Some(cached) = self
            .cached(key)
            .filter(|cached| cached.started_at_ms <= cutoff_ms)
        else {
            return Ok(false);
        };
        cached.owner.begin_close(self.lifecycle.clone());
        if CLI_CALLBACK_INSTANCE
            .try_with(|id| id.key() == key)
            .unwrap_or(false)
        {
            bail!("CLI callback stop is pending its own drain");
        }
        let lock = self.start_lock_for_key(key).await;
        let _guard = lock.lock().await;
        let Some(current) = self.cached(key).filter(|current| {
            current.instance == cached.instance && current.started_at_ms <= cutoff_ms
        }) else {
            return Ok(false);
        };
        self.close_cached(&current).await
    }
    async fn close_cached(&self, cached: &CLIAgentRuntimeCachedSession) -> Result<bool> {
        if CLI_CALLBACK_INSTANCE
            .try_with(|id| id == &cached.instance)
            .unwrap_or(false)
        {
            cached.owner.begin_close(self.lifecycle.clone());
            bail!("CLI callback stop is pending its own drain");
        }
        cached.owner.close_and_wait(self.lifecycle.clone()).await?;
        self.release_stopped(&cached.owner)?;
        Ok(true)
    }
    async fn touch_reusable_session(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        launch_spec: &CliSessionLaunchSpec,
        now_ms: u64,
    ) -> Option<CLIAgentRuntimeSessionHandle> {
        let mut sessions = self.sessions.lock().expect("CLI registry poisoned");
        let cached = sessions.get_mut(key)?;
        if requires_restart(&cached.launch_spec, launch_spec) {
            return None;
        }
        let handle = cached.handle()?;
        cached.launch_spec.continuation = launch_spec.continuation.clone();
        cached.last_used_at_ms = now_ms;
        Some(handle)
    }
    async fn touch_any_existing_session(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        now_ms: u64,
    ) -> Option<CLIAgentRuntimeSessionHandle> {
        let mut sessions = self.sessions.lock().expect("CLI registry poisoned");
        let cached = sessions.get_mut(key)?;
        let handle = cached.handle()?;
        cached.last_used_at_ms = now_ms;
        Some(handle)
    }
    pub(crate) async fn existing_session(
        &self,
        key: &CLIAgentRuntimeSessionKey,
    ) -> Option<CLIAgentRuntimeSessionHandle> {
        if self.shutting_down.load(Ordering::Acquire) {
            return None;
        }
        self.cached(key)?.handle()
    }
    pub(crate) async fn is_current_instance(&self, instance: &CliSessionInstanceId) -> bool {
        if self.shutting_down.load(Ordering::Acquire) {
            return false;
        }
        self.cached(instance.key())
            .is_some_and(|cached| cached.instance == *instance && cached.handle().is_some())
    }
    /// EOF closes admission and initiates owned cleanup; it is not stop proof.
    pub(crate) async fn remove_if_generation(&self, instance: &CliSessionInstanceId) -> bool {
        let Ok(owner) = self.capture_stop_owner(instance) else {
            return false;
        };
        owner.0.begin_close(self.lifecycle.clone());
        true
    }
    async fn start_lock_for_key(&self, key: &CLIAgentRuntimeSessionKey) -> Arc<Mutex<()>> {
        let mut locks = self.start_locks.lock().await;
        if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(key.clone(), Arc::downgrade(&lock));
        lock
    }
    pub(crate) async fn close_idle_sessions(&self) -> Result<usize> {
        self.close_idle_sessions_at(current_time_millis()).await
    }
    async fn close_idle_sessions_at(&self, now_ms: u64) -> Result<usize> {
        let ttl_ms = self.idle_session_ttl.as_millis() as u64;
        let candidates = self
            .sessions
            .lock()
            .expect("CLI registry poisoned")
            .values()
            .filter(|cached| {
                cached.owner.closing.load(Ordering::Acquire)
                    || now_ms.saturating_sub(cached.last_used_at_ms) >= ttl_ms
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut count = 0;
        let mut first_error = None;
        for candidate in candidates {
            let lock = self.start_lock_for_key(candidate.instance.key()).await;
            let _guard = lock.lock().await;
            let Some(current) = self.cached(candidate.instance.key()).filter(|current| {
                current.instance == candidate.instance
                    && (current.owner.closing.load(Ordering::Acquire)
                        || now_ms.saturating_sub(current.last_used_at_ms) >= ttl_ms)
            }) else {
                continue;
            };
            match self.close_cached(&current).await {
                Ok(true) => count += 1,
                Err(error) => {
                    first_error.get_or_insert(error);
                }
                _ => {}
            }
        }
        self.start_locks
            .lock()
            .await
            .retain(|_, lock| lock.strong_count() > 0);
        first_error.map_or(Ok(count), Err)
    }
    pub(crate) async fn close_session(&self, key: &CLIAgentRuntimeSessionKey) -> Result<bool> {
        // Capture before waiting for the key lock: never close a replacement.
        let Some(cached) = self.cached(key) else {
            return Ok(false);
        };
        self.close_session_instance(&cached.instance).await
    }
    pub(crate) async fn close_session_instance(
        &self,
        instance: &CliSessionInstanceId,
    ) -> Result<bool> {
        let Ok(captured) = self.capture_stop_owner(instance) else {
            return Ok(false);
        };
        captured.0.begin_close(self.lifecycle.clone());
        if CLI_CALLBACK_INSTANCE
            .try_with(|id| id == instance)
            .unwrap_or(false)
        {
            bail!("CLI callback stop is pending its own drain");
        }
        let lock = self.start_lock_for_key(instance.key()).await;
        let _guard = lock.lock().await;
        captured.0.close_and_wait(self.lifecycle.clone()).await?;
        self.release_stopped(&captured.0)?;
        Ok(true)
    }
    pub(crate) async fn close_all(&self) -> Result<usize> {
        let instances = {
            let sessions = self.sessions.lock().expect("CLI registry poisoned");
            self.shutting_down.store(true, Ordering::Release);
            for cached in sessions.values() {
                cached.owner.begin_close(self.lifecycle.clone());
            }
            sessions
                .values()
                .map(|cached| cached.instance.clone())
                .collect::<Vec<_>>()
        };
        self.lifecycle.shutdown_started().await;
        let mut count = 0;
        let mut first_error = None;
        for instance in instances {
            match self.close_session_instance(&instance).await {
                Ok(true) => count += 1,
                Err(error) => {
                    first_error.get_or_insert(error);
                }
                _ => {}
            }
        }
        if first_error.is_none() {
            self.lifecycle.shutdown_finished().await;
        }
        first_error.map_or(Ok(count), Err)
    }
    #[cfg(test)]
    async fn session_count(&self) -> usize {
        self.sessions.lock().expect("CLI registry poisoned").len()
    }
    #[cfg(test)]
    async fn cached_started_at_ms(&self, key: &CLIAgentRuntimeSessionKey) -> Option<u64> {
        self.cached(key).map(|cached| cached.started_at_ms)
    }
}

fn normalize_key_part(value: String, label: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("CLI runtime session key `{label}` cannot be empty");
    }
    Ok(trimmed.to_owned())
}

fn validate_replacement_continuation(
    old: &CliSessionLaunchSpec,
    new: &CliSessionLaunchSpec,
) -> Result<()> {
    use crate::cli_runtime::continuation::CliProviderContinuation;
    match (&old.continuation, &new.continuation) {
        (
            CliProviderContinuation::ClaudeNew {
                provider_session_id: old_id,
            }
            | CliProviderContinuation::ClaudeResume {
                provider_session_id: old_id,
            }
            | CliProviderContinuation::ClaudeFork {
                provider_session_id: old_id,
                ..
            },
            CliProviderContinuation::ClaudeResume {
                provider_session_id: new_id,
            },
        ) if old_id == new_id => Ok(()),
        (
            CliProviderContinuation::ClaudeNew { .. }
            | CliProviderContinuation::ClaudeResume { .. }
            | CliProviderContinuation::ClaudeFork { .. },
            CliProviderContinuation::ClaudeNew { .. },
        ) => bail!("Claude process replacement requires spawn-time resume of the durable UUID"),
        (
            CliProviderContinuation::ClaudeNew { .. }
            | CliProviderContinuation::ClaudeResume { .. }
            | CliProviderContinuation::ClaudeFork { .. },
            CliProviderContinuation::ClaudeResume { .. },
        ) => bail!("Claude process replacement cannot change the durable provider UUID"),
        (
            CliProviderContinuation::CodexRpcThread { .. },
            CliProviderContinuation::CodexRpcThread { .. },
        ) => Ok(()),
        _ => bail!("CLI process replacement cannot change provider continuation kind"),
    }
}

fn current_time_millis() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CLIAgentRuntimeMachineRequestResponder, CLIAgentRuntimeManager, CLIAgentRuntimeSession,
        CLIAgentRuntimeSessionFactory, CLIAgentRuntimeSessionKey,
        CLIAgentRuntimeSessionStartOptions, CLIAgentRuntimeSessionStartup,
    };
    use crate::cli_runtime::claude_mcp::{
        ClaudeMcpSessionLaunchProjection, build_claude_mcp_session_launch_projection,
    };
    use crate::cli_runtime::codex_mcp::{
        CodexMcpSessionLaunchProjection, build_codex_mcp_session_launch_projection,
    };
    use crate::cli_runtime::continuation::{
        CliMcpSessionLaunch, CliProviderContinuation, CliSessionLaunchSpec,
    };
    use crate::turn_mcp::projection::{
        McpProjectionLimits, McpSelectionReason, ResolvedMcpTurnProjection, ResolvedMcpTurnTool,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use serde_json::{Value as JsonValue, json};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::{Mutex, Notify};

    #[derive(Default)]
    struct FakeFactory {
        starts: AtomicUsize,
        closes: Arc<AtomicUsize>,
        release: Option<Arc<Notify>>,
        startup_wait_for_cancel: bool,
        startup_entered: Option<Arc<Notify>>,
        startup_cancel_seen: Option<Arc<Notify>>,
        startup_cancel_release: Option<Arc<Notify>>,
        close_started: Option<Arc<Notify>>,
        close_release: Option<Arc<Notify>>,
        fail_after: Option<usize>,
        fail_replacement_prepare: bool,
        launch_specs: Arc<Mutex<Vec<CliSessionLaunchSpec>>>,
        responses: Arc<Mutex<Vec<(usize, JsonValue, JsonValue)>>>,
        replacement_events: Arc<Mutex<Vec<(usize, &'static str)>>>,
    }

    #[async_trait]
    impl CLIAgentRuntimeSessionFactory for FakeFactory {
        async fn start_session_with_launch_spec(
            &self,
            _instance: &crate::cli_runtime::session_instance::CliSessionInstanceId,
            launch_spec: &CliSessionLaunchSpec,
            startup: &CLIAgentRuntimeSessionStartup,
        ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
            self.launch_specs.lock().await.push(launch_spec.clone());
            let id = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
            if let Some(entered) = &self.startup_entered {
                entered.notify_one();
            }
            if self.startup_wait_for_cancel {
                startup.cancelled().await;
                if let Some(seen) = &self.startup_cancel_seen {
                    seen.notify_one();
                }
                if let Some(release) = &self.startup_cancel_release {
                    release.notified().await;
                }
            }
            if let Some(release) = self.release.as_ref() {
                release.notified().await;
            }
            if self.fail_after.is_some_and(|maximum| id > maximum) {
                anyhow::bail!("injected CLI session start failure");
            }
            Ok(Arc::new(FakeSession {
                id,
                closes: self.closes.clone(),
                close_started: self.close_started.clone(),
                close_release: self.close_release.clone(),
                responses: self.responses.clone(),
                replacement_events: self.replacement_events.clone(),
                fail_replacement_prepare: self.fail_replacement_prepare,
            }))
        }
    }

    struct FakeSession {
        id: usize,
        closes: Arc<AtomicUsize>,
        close_started: Option<Arc<Notify>>,
        close_release: Option<Arc<Notify>>,
        responses: Arc<Mutex<Vec<(usize, JsonValue, JsonValue)>>>,
        replacement_events: Arc<Mutex<Vec<(usize, &'static str)>>>,
        fail_replacement_prepare: bool,
    }

    #[async_trait]
    impl CLIAgentRuntimeSession for FakeSession {
        async fn close(&self) -> Result<()> {
            let _ = self.id;
            self.replacement_events
                .lock()
                .await
                .push((self.id, "close"));
            self.closes.fetch_add(1, Ordering::SeqCst);
            if let Some(started) = self.close_started.as_ref() {
                started.notify_one();
            }
            if let Some(release) = self.close_release.as_ref() {
                release.notified().await;
            }
            Ok(())
        }

        async fn prepare_for_replacement(&self) -> Result<()> {
            self.replacement_events
                .lock()
                .await
                .push((self.id, "prepare"));
            if self.fail_replacement_prepare {
                anyhow::bail!("injected non-terminal or unverified provider session");
            }
            Ok(())
        }

        async fn confirm_replacement_checkpoint(&self) -> Result<()> {
            self.replacement_events
                .lock()
                .await
                .push((self.id, "confirm"));
            Ok(())
        }

        async fn respond_to_request(
            &self,
            native_request_id: JsonValue,
            response: JsonValue,
        ) -> Result<()> {
            self.responses
                .lock()
                .await
                .push((self.id, native_request_id, response));
            Ok(())
        }
    }

    fn key(thread_id: &str) -> CLIAgentRuntimeSessionKey {
        CLIAgentRuntimeSessionKey::new("ws", "codex", thread_id).expect("valid key")
    }

    fn codex_mcp_launch_projection(
        turn_id: &str,
        provider_contract_byte: char,
    ) -> CodexMcpSessionLaunchProjection {
        let mut projection = ResolvedMcpTurnProjection::empty("ws", turn_id);
        projection.tools.push(ResolvedMcpTurnTool {
            canonical_callable_name: String::new(),
            workspace_id: "ws".to_owned(),
            server_installation_id: "installation".to_owned(),
            server_name: "server".to_owned(),
            raw_tool_name: "tool".to_owned(),
            description: Some("fixture".to_owned()),
            input_schema: json!({"type": "object"}),
            annotations: None,
            timeout_ms: 20_000,
            catalog_version: "catalog".to_owned(),
            installation_fingerprint: "installation-fingerprint".to_owned(),
            schema_fingerprint: String::new(),
            runtime_generation: 1,
            selection_reason: McpSelectionReason::ExplicitTool,
            capability_id: Some("capability".to_owned()),
        });
        projection
            .finalize_identity(McpProjectionLimits::default())
            .expect("canonical projection");
        build_codex_mcp_session_launch_projection(
            projection,
            provider_contract_byte.to_string().repeat(64),
        )
        .expect("Codex MCP launch projection")
    }

    fn codex_empty_mcp_launch_projection(
        turn_id: &str,
        provider_contract_byte: char,
    ) -> CodexMcpSessionLaunchProjection {
        let mut projection = ResolvedMcpTurnProjection::empty("ws", turn_id);
        projection
            .finalize_identity(McpProjectionLimits::default())
            .expect("canonical empty projection");
        build_codex_mcp_session_launch_projection(
            projection,
            provider_contract_byte.to_string().repeat(64),
        )
        .expect("empty Codex MCP launch projection")
    }

    fn claude_mcp_launch_projection(
        turn_id: &str,
        provider_contract_byte: char,
    ) -> ClaudeMcpSessionLaunchProjection {
        let mut projection = ResolvedMcpTurnProjection::empty("ws", turn_id);
        projection.tools.push(ResolvedMcpTurnTool {
            canonical_callable_name: String::new(),
            workspace_id: "ws".to_owned(),
            server_installation_id: "installation".to_owned(),
            server_name: "server".to_owned(),
            raw_tool_name: "tool".to_owned(),
            description: Some("fixture".to_owned()),
            input_schema: json!({"type": "object"}),
            annotations: None,
            timeout_ms: 20_000,
            catalog_version: "catalog".to_owned(),
            installation_fingerprint: "installation-fingerprint".to_owned(),
            schema_fingerprint: String::new(),
            runtime_generation: 1,
            selection_reason: McpSelectionReason::ExplicitTool,
            capability_id: Some("capability".to_owned()),
        });
        projection
            .finalize_identity(McpProjectionLimits::default())
            .expect("canonical projection");
        build_claude_mcp_session_launch_projection(
            projection,
            provider_contract_byte.to_string().repeat(64),
        )
        .expect("Claude MCP launch projection")
    }

    fn claude_empty_mcp_launch_projection(
        turn_id: &str,
        provider_contract_byte: char,
    ) -> ClaudeMcpSessionLaunchProjection {
        let mut projection = ResolvedMcpTurnProjection::empty("ws", turn_id);
        projection
            .finalize_identity(McpProjectionLimits::default())
            .expect("canonical empty projection");
        build_claude_mcp_session_launch_projection(
            projection,
            provider_contract_byte.to_string().repeat(64),
        )
        .expect("empty Claude MCP launch projection")
    }

    fn manager_with_factory(factory: Arc<FakeFactory>) -> CLIAgentRuntimeManager {
        CLIAgentRuntimeManager::new(factory, Duration::from_millis(1_000))
            .expect("manager should build")
    }

    #[tokio::test]
    async fn cli_runtime_manager_reuses_active_session() {
        let factory = Arc::new(FakeFactory::default());
        let manager = manager_with_factory(factory.clone());
        let key = key("thread-a");

        let first = manager
            .get_or_start_at(
                key.clone(),
                CLIAgentRuntimeSessionStartOptions::default(),
                1_000,
            )
            .await
            .expect("first start should succeed");
        let second = manager
            .get_or_start_at(
                key.clone(),
                CLIAgentRuntimeSessionStartOptions::default(),
                1_100,
            )
            .await
            .expect("second get should succeed");

        assert!(first.ptr_eq(&second));
        assert_eq!(first.key(), &key);
        assert_eq!(first.instance().generation(), 1);
        assert_eq!(second.instance(), first.instance());
        assert_eq!(factory.starts.load(Ordering::SeqCst), 1);
        assert_eq!(manager.cached_started_at_ms(&key).await, Some(1_000));
        assert_eq!(manager.session_count().await, 1);
    }

    #[tokio::test]
    async fn cli_runtime_manager_restarts_session_when_start_options_change() {
        let factory = Arc::new(FakeFactory::default());
        let manager = manager_with_factory(factory.clone());
        let key = key("thread-a");
        let options_a = CLIAgentRuntimeSessionStartOptions {
            cwd: None,
            approval_policy: Some("on-request".to_owned()),
            authorization_scope_fingerprint: Some("scope-a".to_owned()),
            app_server_args: vec!["-c".to_owned(), "model=\"gpt-5-codex\"".to_owned()],
            env: Default::default(),
            selected_skills: Vec::new(),
            elevated_instructions: None,
        };
        let options_b = CLIAgentRuntimeSessionStartOptions {
            cwd: None,
            approval_policy: Some("never".to_owned()),
            authorization_scope_fingerprint: Some("scope-b".to_owned()),
            app_server_args: vec!["-c".to_owned(), "model=\"gpt-5\"".to_owned()],
            env: Default::default(),
            selected_skills: Vec::new(),
            elevated_instructions: None,
        };

        let first = manager
            .get_or_start_at(key.clone(), options_a.clone(), 1_000)
            .await
            .expect("first start should succeed");
        let reused = manager
            .get_or_start_at(key.clone(), options_a, 1_100)
            .await
            .expect("same options should reuse");
        let restarted = manager
            .get_or_start_at(key.clone(), options_b, 1_200)
            .await
            .expect("changed options should restart");

        assert!(first.ptr_eq(&reused));
        assert!(!first.ptr_eq(&restarted));
        assert!(restarted.instance().generation() > first.instance().generation());
        assert_eq!(factory.starts.load(Ordering::SeqCst), 2);
        assert_eq!(factory.closes.load(Ordering::SeqCst), 1);
        assert_eq!(manager.session_count().await, 1);
    }

    #[tokio::test]
    async fn codex_mcp_restart_reuses_manifest_and_resumes_changed_manifest() {
        let factory = Arc::new(FakeFactory::default());
        let manager = manager_with_factory(factory.clone());
        let key = key("mcp-semantic-session");
        let options_a = CliSessionLaunchSpec::codex(
            CLIAgentRuntimeSessionStartOptions::default(),
            CliMcpSessionLaunch::Codex(codex_mcp_launch_projection("turn-a", 'a')),
            None,
        );
        let options_b = CliSessionLaunchSpec::codex(
            CLIAgentRuntimeSessionStartOptions::default(),
            CliMcpSessionLaunch::Codex(codex_mcp_launch_projection("turn-b", 'a')),
            Some("native-thread".to_owned()),
        );
        let changed_contract = CliSessionLaunchSpec::codex(
            CLIAgentRuntimeSessionStartOptions::default(),
            CliMcpSessionLaunch::Codex(codex_mcp_launch_projection("turn-c", 'b')),
            Some("native-thread".to_owned()),
        );

        let first = manager
            .get_or_start_with_launch_spec_at(key.clone(), options_a, 1_000)
            .await
            .expect("first semantic projection should start");
        let reused = manager
            .get_or_start_with_launch_spec_at(key.clone(), options_b, 1_100)
            .await
            .expect("same semantic projection should reuse");
        let restarted = manager
            .get_or_start_with_launch_spec_at(key, changed_contract, 1_200)
            .await
            .expect("changed semantic projection should restart");

        assert!(first.ptr_eq(&reused));
        assert!(!first.ptr_eq(&restarted));
        assert_eq!(factory.starts.load(Ordering::SeqCst), 2);
        assert_eq!(factory.closes.load(Ordering::SeqCst), 1);
        let launches = factory.launch_specs.lock().await;
        assert_eq!(launches.len(), 2);
        assert!(matches!(
            &launches[1].continuation,
            CliProviderContinuation::CodexRpcThread {
                native_thread_id: Some(native_thread_id)
            } if native_thread_id == "native-thread"
        ));
    }

    #[tokio::test]
    async fn codex_manifest_transition_empty_nonempty_and_back_restarts() {
        let factory = Arc::new(FakeFactory::default());
        let manager = manager_with_factory(factory.clone());
        let key = key("codex-manifest-transition");
        let empty = |turn_id: &str| {
            CliSessionLaunchSpec::codex(
                CLIAgentRuntimeSessionStartOptions::default(),
                CliMcpSessionLaunch::Codex(codex_empty_mcp_launch_projection(turn_id, 'a')),
                Some("native-thread".to_owned()),
            )
        };
        let non_empty = CliSessionLaunchSpec::codex(
            CLIAgentRuntimeSessionStartOptions::default(),
            CliMcpSessionLaunch::Codex(codex_mcp_launch_projection("turn-b", 'a')),
            Some("native-thread".to_owned()),
        );

        let first = manager
            .get_or_start_with_launch_spec_at(key.clone(), empty("turn-a"), 1_000)
            .await
            .unwrap();
        let non_empty_handle = manager
            .get_or_start_with_launch_spec_at(key.clone(), non_empty, 1_100)
            .await
            .unwrap();
        let final_empty = manager
            .get_or_start_with_launch_spec_at(key, empty("turn-c"), 1_200)
            .await
            .unwrap();

        assert!(!first.ptr_eq(&non_empty_handle));
        assert!(!non_empty_handle.ptr_eq(&final_empty));
        assert_eq!(factory.starts.load(Ordering::SeqCst), 3);
        assert_eq!(factory.closes.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn codex_mcp_restart_failure_never_falls_back_to_stale_generation() {
        let factory = Arc::new(FakeFactory {
            fail_after: Some(1),
            ..FakeFactory::default()
        });
        let manager = manager_with_factory(factory.clone());
        let key = key("restart-failure");
        manager
            .get_or_start_with_launch_spec(
                key.clone(),
                CliSessionLaunchSpec::codex(
                    CLIAgentRuntimeSessionStartOptions::default(),
                    CliMcpSessionLaunch::Codex(codex_mcp_launch_projection("turn-a", 'a')),
                    None,
                ),
            )
            .await
            .unwrap();

        let failure = manager
            .get_or_start_with_launch_spec(
                key.clone(),
                CliSessionLaunchSpec::codex(
                    CLIAgentRuntimeSessionStartOptions::default(),
                    CliMcpSessionLaunch::Codex(codex_mcp_launch_projection("turn-b", 'b')),
                    Some("native-thread".to_owned()),
                ),
            )
            .await;

        assert!(failure.is_err());
        assert!(manager.existing_session(&key).await.is_none());
        assert_eq!(factory.closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn claude_mcp_restart_reuses_unchanged_and_resumes_changed_manifest_in_order() {
        let factory = Arc::new(FakeFactory::default());
        let manager = manager_with_factory(factory.clone());
        let key =
            CLIAgentRuntimeSessionKey::new("ws", "claude", "claude-restart").expect("valid key");
        let provider_session_id = uuid::Uuid::new_v4();
        let first_spec = CliSessionLaunchSpec::claude_new(
            CLIAgentRuntimeSessionStartOptions::default(),
            CliMcpSessionLaunch::Claude(claude_mcp_launch_projection("turn-a", 'a')),
            provider_session_id,
        );
        let unchanged = CliSessionLaunchSpec::claude_resume(
            CLIAgentRuntimeSessionStartOptions::default(),
            CliMcpSessionLaunch::Claude(claude_mcp_launch_projection("turn-b", 'a')),
            provider_session_id,
        );
        let changed = CliSessionLaunchSpec::claude_resume(
            CLIAgentRuntimeSessionStartOptions::default(),
            CliMcpSessionLaunch::Claude(claude_mcp_launch_projection("turn-c", 'b')),
            provider_session_id,
        );

        let first = manager
            .get_or_start_with_launch_spec(key.clone(), first_spec)
            .await
            .expect("fresh Claude process");
        let reused = manager
            .get_or_start_with_launch_spec(key.clone(), unchanged)
            .await
            .expect("unchanged Claude launch must reuse");
        let replacement = manager
            .get_or_start_with_launch_spec(key, changed)
            .await
            .expect("changed Claude launch must resume");

        assert!(first.ptr_eq(&reused));
        assert!(!first.ptr_eq(&replacement));
        assert_eq!(factory.starts.load(Ordering::SeqCst), 2);
        assert_eq!(factory.closes.load(Ordering::SeqCst), 1);
        assert_eq!(
            factory.replacement_events.lock().await.as_slice(),
            &[(1, "prepare"), (1, "close"), (1, "confirm")]
        );
        let launches = factory.launch_specs.lock().await;
        assert_eq!(launches.len(), 2);
        assert!(matches!(
            launches[0].continuation,
            CliProviderContinuation::ClaudeNew { provider_session_id: id } if id == provider_session_id
        ));
        assert!(matches!(
            launches[1].continuation,
            CliProviderContinuation::ClaudeResume { provider_session_id: id } if id == provider_session_id
        ));
    }

    #[tokio::test]
    async fn claude_manifest_transition_empty_nonempty_and_back_always_restarts() {
        let factory = Arc::new(FakeFactory::default());
        let manager = manager_with_factory(factory.clone());
        let key = CLIAgentRuntimeSessionKey::new("ws", "claude", "claude-empty-transition")
            .expect("valid key");
        let provider_session_id = uuid::Uuid::new_v4();
        let empty = |turn_id: &str, fresh: bool| {
            let mcp = CliMcpSessionLaunch::Claude(claude_empty_mcp_launch_projection(turn_id, 'a'));
            if fresh {
                CliSessionLaunchSpec::claude_new(
                    CLIAgentRuntimeSessionStartOptions::default(),
                    mcp,
                    provider_session_id,
                )
            } else {
                CliSessionLaunchSpec::claude_resume(
                    CLIAgentRuntimeSessionStartOptions::default(),
                    mcp,
                    provider_session_id,
                )
            }
        };
        let non_empty = CliSessionLaunchSpec::claude_resume(
            CLIAgentRuntimeSessionStartOptions::default(),
            CliMcpSessionLaunch::Claude(claude_mcp_launch_projection("turn-b", 'a')),
            provider_session_id,
        );

        let first = manager
            .get_or_start_with_launch_spec(key.clone(), empty("turn-a", true))
            .await
            .unwrap();
        let second = manager
            .get_or_start_with_launch_spec(key.clone(), non_empty)
            .await
            .unwrap();
        let third = manager
            .get_or_start_with_launch_spec(key, empty("turn-c", false))
            .await
            .unwrap();

        assert!(!first.ptr_eq(&second));
        assert!(!second.ptr_eq(&third));
        assert_eq!(factory.starts.load(Ordering::SeqCst), 3);
        assert_eq!(factory.closes.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn claude_resume_rejection_never_falls_back_to_old_or_empty_conversation() {
        let factory = Arc::new(FakeFactory {
            fail_after: Some(1),
            ..FakeFactory::default()
        });
        let manager = manager_with_factory(factory.clone());
        let key = CLIAgentRuntimeSessionKey::new("ws", "claude", "claude-resume-rejected")
            .expect("valid key");
        let provider_session_id = uuid::Uuid::new_v4();
        manager
            .get_or_start_with_launch_spec(
                key.clone(),
                CliSessionLaunchSpec::claude_new(
                    CLIAgentRuntimeSessionStartOptions::default(),
                    CliMcpSessionLaunch::Claude(claude_mcp_launch_projection("turn-a", 'a')),
                    provider_session_id,
                ),
            )
            .await
            .unwrap();

        let failure = manager
            .get_or_start_with_launch_spec(
                key.clone(),
                CliSessionLaunchSpec::claude_resume(
                    CLIAgentRuntimeSessionStartOptions::default(),
                    CliMcpSessionLaunch::Claude(claude_mcp_launch_projection("turn-b", 'b')),
                    provider_session_id,
                ),
            )
            .await;

        assert!(failure.is_err());
        assert!(manager.existing_session(&key).await.is_none());
        assert_eq!(factory.starts.load(Ordering::SeqCst), 2);
        assert_eq!(factory.closes.load(Ordering::SeqCst), 1);
        let launches = factory.launch_specs.lock().await;
        assert_eq!(launches.len(), 2, "no untyped fallback launch is allowed");
        assert!(matches!(
            launches[1].continuation,
            CliProviderContinuation::ClaudeResume { provider_session_id: id } if id == provider_session_id
        ));
    }

    #[tokio::test]
    async fn claude_resume_waits_for_terminal_verified_replacement_barrier() {
        let factory = Arc::new(FakeFactory {
            fail_replacement_prepare: true,
            ..FakeFactory::default()
        });
        let manager = manager_with_factory(factory.clone());
        let key =
            CLIAgentRuntimeSessionKey::new("ws", "claude", "claude-barrier").expect("valid key");
        let provider_session_id = uuid::Uuid::new_v4();
        let old = manager
            .get_or_start_with_launch_spec(
                key.clone(),
                CliSessionLaunchSpec::claude_new(
                    CLIAgentRuntimeSessionStartOptions::default(),
                    CliMcpSessionLaunch::Claude(claude_mcp_launch_projection("turn-a", 'a')),
                    provider_session_id,
                ),
            )
            .await
            .unwrap();

        let failure = manager
            .get_or_start_with_launch_spec(
                key.clone(),
                CliSessionLaunchSpec::claude_resume(
                    CLIAgentRuntimeSessionStartOptions::default(),
                    CliMcpSessionLaunch::Claude(claude_mcp_launch_projection("turn-b", 'b')),
                    provider_session_id,
                ),
            )
            .await;

        assert!(failure.is_err());
        let still_current = manager.existing_session(&key).await.unwrap();
        assert!(old.ptr_eq(&still_current));
        assert_eq!(factory.starts.load(Ordering::SeqCst), 1);
        assert_eq!(factory.closes.load(Ordering::SeqCst), 0);
        assert_eq!(
            factory.replacement_events.lock().await.as_slice(),
            &[(1, "prepare")]
        );
    }

    #[tokio::test]
    async fn claude_resume_rejects_changed_manifest_without_spawn_time_resume() {
        let factory = Arc::new(FakeFactory::default());
        let manager = manager_with_factory(factory.clone());
        let key = CLIAgentRuntimeSessionKey::new("ws", "claude", "claude-resume-required")
            .expect("valid key");
        let provider_session_id = uuid::Uuid::new_v4();
        let old = manager
            .get_or_start_with_launch_spec(
                key.clone(),
                CliSessionLaunchSpec::claude_new(
                    CLIAgentRuntimeSessionStartOptions::default(),
                    CliMcpSessionLaunch::Claude(claude_mcp_launch_projection("turn-a", 'a')),
                    provider_session_id,
                ),
            )
            .await
            .unwrap();
        let rejected = manager
            .get_or_start_with_launch_spec(
                key.clone(),
                CliSessionLaunchSpec::claude_new(
                    CLIAgentRuntimeSessionStartOptions::default(),
                    CliMcpSessionLaunch::Claude(claude_mcp_launch_projection("turn-b", 'b')),
                    provider_session_id,
                ),
            )
            .await;

        assert!(rejected.is_err());
        assert!(old.ptr_eq(&manager.existing_session(&key).await.unwrap()));
        assert_eq!(factory.starts.load(Ordering::SeqCst), 1);
        assert_eq!(factory.closes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn claude_management_acquisition_never_clobbers_active_mcp_launch() {
        let factory = Arc::new(FakeFactory::default());
        let manager = manager_with_factory(factory.clone());
        let key =
            CLIAgentRuntimeSessionKey::new("ws", "claude", "claude-management").expect("valid key");
        let provider_session_id = uuid::Uuid::new_v4();
        let active = manager
            .get_or_start_with_launch_spec(
                key.clone(),
                CliSessionLaunchSpec::claude_new(
                    CLIAgentRuntimeSessionStartOptions::default(),
                    CliMcpSessionLaunch::Claude(claude_mcp_launch_projection("turn-a", 'a')),
                    provider_session_id,
                ),
            )
            .await
            .unwrap();
        let management = manager
            .existing_or_start_management(
                key,
                CLIAgentRuntimeSessionStartOptions {
                    approval_policy: Some("management-defaults-must-not-replace".to_owned()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(active.ptr_eq(&management));
        assert_eq!(factory.starts.load(Ordering::SeqCst), 1);
        assert_eq!(factory.closes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn management_and_fork_acquisition_never_clobbers_mcp_session() {
        let factory = Arc::new(FakeFactory::default());
        let manager = manager_with_factory(factory.clone());
        let key = key("management-no-clobber");
        let active = manager
            .get_or_start_with_launch_spec(
                key.clone(),
                CliSessionLaunchSpec::codex(
                    CLIAgentRuntimeSessionStartOptions::default(),
                    CliMcpSessionLaunch::Codex(codex_mcp_launch_projection("turn-a", 'a')),
                    Some("native-thread".to_owned()),
                ),
            )
            .await
            .unwrap();
        let management = manager
            .existing_or_start_management(
                key,
                CLIAgentRuntimeSessionStartOptions {
                    approval_policy: Some("different-management-policy".to_owned()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(active.ptr_eq(&management));
        assert_eq!(factory.starts.load(Ordering::SeqCst), 1);
        assert_eq!(factory.closes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cli_runtime_generation_compare_and_swap_cannot_remove_replacement() {
        let factory = Arc::new(FakeFactory::default());
        let manager = manager_with_factory(factory);
        let key = key("generation-cas");
        let first = manager
            .get_or_start_with_options(key.clone(), CLIAgentRuntimeSessionStartOptions::default())
            .await
            .unwrap();
        let replacement = manager
            .get_or_start_with_options(
                key.clone(),
                CLIAgentRuntimeSessionStartOptions {
                    approval_policy: Some("never".to_owned()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(!manager.remove_if_generation(first.instance()).await);
        let current = manager.existing_session(&key).await.unwrap();
        assert_eq!(current.instance(), replacement.instance());
        assert!(manager.remove_if_generation(replacement.instance()).await);
        assert!(manager.existing_session(&key).await.is_none());
        assert_eq!(manager.stop_inventory("ws").len(), 1);
        manager
            .close_session_instance(replacement.instance())
            .await
            .unwrap();
        assert!(manager.stop_inventory("ws").is_empty());
    }

    #[tokio::test]
    async fn captured_stop_and_requested_closes_cancel_startup_before_waiting_for_key_lock() {
        for mode in ["strict", "instance", "cutoff"] {
            let entered = Arc::new(Notify::new());
            let factory = Arc::new(FakeFactory {
                startup_wait_for_cancel: true,
                startup_entered: Some(entered.clone()),
                ..Default::default()
            });
            let manager = Arc::new(manager_with_factory(factory.clone()));
            let key = key(mode);
            let starter = {
                let manager = manager.clone();
                let key = key.clone();
                tokio::spawn(async move {
                    manager
                        .get_or_start_at(key, CLIAgentRuntimeSessionStartOptions::default(), 1_000)
                        .await
                })
            };
            entered.notified().await;
            let captured = manager.stop_inventory("ws").pop().unwrap();
            assert!(!captured.0.ready.load(Ordering::Acquire));
            tokio::time::timeout(Duration::from_secs(1), async {
                match mode {
                    "strict" => manager
                        .stop_and_wait(
                            &captured,
                            tokio::time::Instant::now() + Duration::from_secs(1),
                        )
                        .await
                        .unwrap(),
                    "instance" => {
                        assert!(
                            manager
                                .close_session_instance(captured.instance())
                                .await
                                .unwrap()
                        );
                    }
                    _ => {
                        assert!(
                            manager
                                .close_session_if_started_at_or_before(&key, 1_000)
                                .await
                                .unwrap()
                        );
                    }
                }
            })
            .await
            .expect("startup cancellation must not depend on the startup key lock");
            assert!(starter.await.unwrap().is_err());
            assert!(captured.0.closing.load(Ordering::Acquire));
            assert!(!captured.0.ready.load(Ordering::Acquire));
            assert!(manager.stop_inventory("ws").is_empty());
            assert_eq!(factory.closes.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn startup_stop_deadline_and_cancelled_waiter_retain_closing_before_key_lock() {
        for cancel_waiter in [false, true] {
            let entered = Arc::new(Notify::new());
            let cancelled = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let factory = Arc::new(FakeFactory {
                startup_wait_for_cancel: true,
                startup_entered: Some(entered.clone()),
                startup_cancel_seen: Some(cancelled.clone()),
                startup_cancel_release: Some(release.clone()),
                ..Default::default()
            });
            let manager = Arc::new(manager_with_factory(factory.clone()));
            let key = key("startup-stop-deadline");
            let starter = {
                let manager = manager.clone();
                let key = key.clone();
                tokio::spawn(async move { manager.get_or_start(key).await })
            };
            entered.notified().await;
            let captured = manager.stop_inventory("ws").pop().unwrap();
            let waiter = {
                let manager = manager.clone();
                let captured = captured.clone();
                tokio::spawn(async move {
                    manager
                        .stop_and_wait(
                            &captured,
                            tokio::time::Instant::now()
                                + if cancel_waiter {
                                    Duration::from_secs(60)
                                } else {
                                    Duration::from_millis(25)
                                },
                        )
                        .await
                })
            };
            tokio::time::timeout(Duration::from_secs(1), cancelled.notified())
                .await
                .expect("stop must signal the factory while startup still owns the lock");
            if cancel_waiter {
                waiter.abort();
                assert!(waiter.await.unwrap_err().is_cancelled());
            } else {
                assert!(waiter.await.unwrap().is_err());
            }
            assert!(!starter.is_finished());
            assert!(captured.0.closing.load(Ordering::Acquire));
            assert!(!captured.0.ready.load(Ordering::Acquire));
            assert!(captured.0.startup.check_admission().is_err());
            assert_eq!(manager.stop_inventory("ws").len(), 1);
            assert!(manager.existing_session(&key).await.is_none());
            // A factory returning a session after cancellation must never
            // publish Ready. The retained cleanup still joins/closes it once.
            release.notify_one();
            assert!(starter.await.unwrap().is_err());
            manager
                .stop_and_wait(
                    &captured,
                    tokio::time::Instant::now() + Duration::from_secs(1),
                )
                .await
                .unwrap();
            assert!(manager.stop_inventory("ws").is_empty());
            assert!(!captured.0.ready.load(Ordering::Acquire));
            assert_eq!(factory.closes.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn callback_stop_closes_admission_without_waiting_for_itself_or_reentrant_key() {
        let factory = Arc::new(FakeFactory::default());
        let manager = Arc::new(manager_with_factory(factory.clone()));
        let key = key("callback-stop");
        let handle = manager.get_or_start(key.clone()).await.unwrap();
        let captured = manager.capture_stop_owner(handle.instance()).unwrap();
        let (done, completion) = tokio::sync::oneshot::channel();
        let callback_manager = manager.clone();
        let callback_owner = captured.clone();
        assert!(manager.spawn_instance_work(handle.instance(), async move {
            assert!(callback_manager.get_or_start(key).await.is_err());
            assert!(
                callback_manager
                    .stop_and_wait(
                        &callback_owner,
                        tokio::time::Instant::now() + Duration::from_secs(1)
                    )
                    .await
                    .is_err()
            );
            assert!(
                callback_manager
                    .close_session_instance(callback_owner.instance())
                    .await
                    .is_err()
            );
            assert!(callback_owner.0.closing.load(Ordering::Acquire));
            done.send(()).unwrap();
        }));
        tokio::time::timeout(Duration::from_secs(1), completion)
            .await
            .unwrap()
            .unwrap();
        manager
            .stop_and_wait(
                &captured,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert!(manager.stop_inventory("ws").is_empty());
        assert_eq!(factory.closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn strict_stop_deadline_keeps_closing_inventory_until_actual_completion() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let factory = Arc::new(FakeFactory {
            close_started: Some(entered.clone()),
            close_release: Some(release.clone()),
            ..Default::default()
        });
        let manager = Arc::new(manager_with_factory(factory.clone()));
        let key = key("strict-deadline");
        let handle = manager.get_or_start(key.clone()).await.unwrap();
        let captured = manager.capture_stop_owner(handle.instance()).unwrap();
        let waiter = {
            let manager = manager.clone();
            let captured = captured.clone();
            tokio::spawn(async move {
                manager
                    .stop_and_wait(
                        &captured,
                        tokio::time::Instant::now() + Duration::from_millis(25),
                    )
                    .await
            })
        };
        entered.notified().await;
        assert!(waiter.await.unwrap().is_err());
        assert_eq!(manager.stop_inventory("ws").len(), 1);
        assert!(manager.existing_session(&key).await.is_none());
        assert_eq!(factory.starts.load(Ordering::SeqCst), 1);
        release.notify_one();
        manager
            .stop_and_wait(
                &captured,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert!(manager.stop_inventory("ws").is_empty());
        assert_eq!(factory.closes.load(Ordering::SeqCst), 1);
        manager.get_or_start(key).await.unwrap();
        assert_eq!(factory.starts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn captured_stop_owner_cannot_stop_a_replacement_and_shutdown_closes_start_admission() {
        let factory = Arc::new(FakeFactory::default());
        let manager = manager_with_factory(factory.clone());
        let key = key("captured-stop");
        let old = manager.get_or_start(key.clone()).await.unwrap();
        let captured = manager.capture_stop_owner(old.instance()).unwrap();
        manager
            .close_session_instance(old.instance())
            .await
            .unwrap();
        let replacement = manager.get_or_start(key.clone()).await.unwrap();
        manager
            .stop_and_wait(
                &captured,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert!(manager.is_current_instance(replacement.instance()).await);
        assert_eq!(factory.closes.load(Ordering::SeqCst), 1);
        manager.close_all().await.unwrap();
        assert!(manager.get_or_start(key).await.is_err());
        assert_eq!(factory.starts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cli_runtime_generation_origin_bound_responder_never_uses_replacement_process() {
        let factory = Arc::new(FakeFactory::default());
        let responses = factory.responses.clone();
        let manager = manager_with_factory(factory);
        let key = key("origin-responder");
        let old = manager
            .get_or_start_with_options(key.clone(), CLIAgentRuntimeSessionStartOptions::default())
            .await
            .unwrap();
        let responder = CLIAgentRuntimeMachineRequestResponder::new(
            old.instance().clone(),
            json!(7),
            old.session(),
        );
        let replacement = manager
            .get_or_start_with_options(
                key,
                CLIAgentRuntimeSessionStartOptions {
                    approval_policy: Some("never".to_owned()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        responder
            .respond(json!({"decision": "cancel"}))
            .await
            .unwrap();
        let responses = responses.lock().await;
        assert_eq!(
            responses.as_slice(),
            &[(1, json!(7), json!({"decision": "cancel"}))]
        );
        assert_ne!(responder.instance(), replacement.instance());
    }

    #[tokio::test]
    async fn stale_process_event_is_fenced_after_codex_and_claude_replacement() {
        for runtime_id in ["codex", "claude"] {
            let factory = Arc::new(FakeFactory::default());
            let manager = manager_with_factory(factory);
            let key = CLIAgentRuntimeSessionKey::new("ws", runtime_id, "replacement-race")
                .expect("valid key");
            let old = manager
                .get_or_start_with_options(
                    key.clone(),
                    CLIAgentRuntimeSessionStartOptions::default(),
                )
                .await
                .unwrap();
            let replacement = manager
                .get_or_start_with_options(
                    key,
                    CLIAgentRuntimeSessionStartOptions {
                        approval_policy: Some("never".to_owned()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();

            assert!(!manager.is_current_instance(old.instance()).await);
            assert!(manager.is_current_instance(replacement.instance()).await);
            assert!(
                !manager
                    .close_session_instance(old.instance())
                    .await
                    .unwrap()
            );
            assert!(manager.is_current_instance(replacement.instance()).await);
        }
    }

    #[tokio::test]
    async fn cli_runtime_manager_restarts_session_when_selected_skills_change() {
        let factory = Arc::new(FakeFactory::default());
        let manager = manager_with_factory(factory.clone());
        let key = key("thread-skill-mode");
        let normal = CLIAgentRuntimeSessionStartOptions::default();
        let skill_enabled = CLIAgentRuntimeSessionStartOptions {
            selected_skills: vec![crate::cli_runtime::skills::CliRuntimeSelectedSkill {
                install_name: "selected".to_owned(),
                installed_path: std::path::PathBuf::from("/tmp/selected"),
                source_folder_hash: "selected-hash".to_owned(),
            }],
            ..Default::default()
        };

        manager
            .get_or_start_at(key.clone(), normal, 1_000)
            .await
            .unwrap();
        manager
            .get_or_start_at(key, skill_enabled, 1_001)
            .await
            .unwrap();
        assert_eq!(factory.starts.load(Ordering::SeqCst), 2);
        assert_eq!(factory.closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cli_runtime_concurrency_same_key_starts_once() {
        let release = Arc::new(Notify::new());
        let factory = Arc::new(FakeFactory {
            release: Some(release.clone()),
            ..FakeFactory::default()
        });
        let manager = Arc::new(manager_with_factory(factory.clone()));
        let key = key("thread-concurrent");

        let first = {
            let manager = manager.clone();
            let key = key.clone();
            tokio::spawn(async move {
                manager
                    .get_or_start_at(key, CLIAgentRuntimeSessionStartOptions::default(), 1_000)
                    .await
            })
        };
        let second = {
            let manager = manager.clone();
            let key = key.clone();
            tokio::spawn(async move {
                manager
                    .get_or_start_at(key, CLIAgentRuntimeSessionStartOptions::default(), 1_010)
                    .await
            })
        };

        while factory.starts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        release.notify_waiters();

        let first = first
            .await
            .expect("first task should join")
            .expect("first get should succeed");
        let second = second
            .await
            .expect("second task should join")
            .expect("second get should succeed");

        assert!(first.ptr_eq(&second));
        assert_eq!(factory.starts.load(Ordering::SeqCst), 1);
        assert_eq!(manager.session_count().await, 1);
    }

    #[tokio::test]
    async fn cli_runtime_manager_closes_idle_and_forced_sessions() {
        let factory = Arc::new(FakeFactory::default());
        let manager = manager_with_factory(factory.clone());
        let key_a = key("thread-a");
        let key_b = key("thread-b");

        manager
            .get_or_start_at(
                key_a.clone(),
                CLIAgentRuntimeSessionStartOptions::default(),
                1_000,
            )
            .await
            .expect("session a should start");
        manager
            .get_or_start_at(
                key_b.clone(),
                CLIAgentRuntimeSessionStartOptions::default(),
                1_800,
            )
            .await
            .expect("session b should start");

        let idle_closed = manager
            .close_idle_sessions_at(2_000)
            .await
            .expect("idle close should succeed");
        assert_eq!(idle_closed, 1);
        assert_eq!(factory.closes.load(Ordering::SeqCst), 1);
        assert_eq!(manager.session_count().await, 1);

        assert!(
            !manager
                .close_session(&key_a)
                .await
                .expect("missing close should succeed")
        );
        assert!(
            manager
                .close_session(&key_b)
                .await
                .expect("forced close should succeed")
        );
        assert_eq!(factory.closes.load(Ordering::SeqCst), 2);
        assert_eq!(manager.session_count().await, 0);
    }

    #[tokio::test]
    async fn cli_runtime_manager_closes_sessions_not_proven_newer_than_receipt() {
        for (started_at_ms, cutoff_ms, expected_closed) in [
            (1_000, 999, false),
            (1_000, 1_000, true),
            (1_000, 1_001, true),
        ] {
            let factory = Arc::new(FakeFactory::default());
            let manager = manager_with_factory(factory.clone());
            let key = key(&format!("freshness-{started_at_ms}-{cutoff_ms}"));
            manager
                .get_or_start_at(
                    key.clone(),
                    CLIAgentRuntimeSessionStartOptions::default(),
                    started_at_ms,
                )
                .await
                .unwrap();
            assert_eq!(
                manager
                    .close_session_if_started_at_or_before(&key, cutoff_ms)
                    .await
                    .unwrap(),
                expected_closed
            );
            assert_eq!(
                factory.closes.load(Ordering::SeqCst),
                usize::from(expected_closed)
            );
            assert_eq!(manager.session_count().await, usize::from(!expected_closed));
        }

        let factory = Arc::new(FakeFactory::default());
        let manager = manager_with_factory(factory);
        assert!(
            !manager
                .close_session_if_started_at_or_before(&key("missing"), u64::MAX)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn cli_runtime_manager_freshness_close_serializes_concurrent_restart() {
        let close_started = Arc::new(Notify::new());
        let close_release = Arc::new(Notify::new());
        let factory = Arc::new(FakeFactory {
            close_started: Some(close_started.clone()),
            close_release: Some(close_release.clone()),
            ..FakeFactory::default()
        });
        let manager = Arc::new(manager_with_factory(factory.clone()));
        let key = key("freshness-concurrent");
        manager
            .get_or_start_at(
                key.clone(),
                CLIAgentRuntimeSessionStartOptions::default(),
                1_000,
            )
            .await
            .unwrap();

        let closer = {
            let manager = manager.clone();
            let key = key.clone();
            tokio::spawn(async move {
                manager
                    .close_session_if_started_at_or_before(&key, 1_000)
                    .await
            })
        };
        close_started.notified().await;
        let starter = {
            let manager = manager.clone();
            let key = key.clone();
            tokio::spawn(async move {
                manager
                    .get_or_start_at(key, CLIAgentRuntimeSessionStartOptions::default(), 2_000)
                    .await
            })
        };
        tokio::task::yield_now().await;
        assert_eq!(factory.starts.load(Ordering::SeqCst), 1);
        close_release.notify_waiters();
        assert!(closer.await.unwrap().unwrap());
        starter.await.unwrap().unwrap();
        assert_eq!(factory.closes.load(Ordering::SeqCst), 1);
        assert_eq!(factory.starts.load(Ordering::SeqCst), 2);
        assert_eq!(manager.cached_started_at_ms(&key).await, Some(2_000));
        assert_eq!(manager.session_count().await, 1);
    }

    #[tokio::test]
    async fn cli_runtime_manager_close_all_releases_all_sessions() {
        let factory = Arc::new(FakeFactory::default());
        let manager = manager_with_factory(factory.clone());

        manager
            .get_or_start_at(
                key("thread-a"),
                CLIAgentRuntimeSessionStartOptions::default(),
                1_000,
            )
            .await
            .expect("session a should start");
        manager
            .get_or_start_at(
                key("thread-b"),
                CLIAgentRuntimeSessionStartOptions::default(),
                1_000,
            )
            .await
            .expect("session b should start");

        let closed = manager.close_all().await.expect("close all should succeed");
        assert_eq!(closed, 2);
        assert_eq!(factory.closes.load(Ordering::SeqCst), 2);
        assert_eq!(manager.session_count().await, 0);
    }
}
