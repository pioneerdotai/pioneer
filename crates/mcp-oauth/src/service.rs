use crate::store::{AuthorizationRecord, OAuthPersistence, PersistentCredentials, Registration};
use async_trait::async_trait;
use pioneer_mcp::{
    McpOAuthProvider, McpRuntimeError, McpServerInstallation, McpTransportConfig, OAuthHttpClient,
    oauth_runtime_error,
};
use rmcp::transport::auth::{
    AuthError, AuthorizationManager, InMemoryStateStore, OAuthClientConfig, ScopeUpgradeConfig,
    StateStore,
};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;
use url::Url;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OAuthState {
    Idle,
    Preparing,
    AwaitingCallback,
    Exchanging,
    /// Consent is decided; durable outcome is being resolved. Clear account can retire it.
    Resolving,
    /// Presentation retirement only; never a consent outcome or runtime effect.
    Retired,
    /// Account deletion failed or is uncertain; retry Clear before using it.
    CleanupRequired,
    Authorized,
    /// Silent token recovery, distinct from completion of user consent.
    Recovered,
    AuthRequired,
    InsufficientScope,
    Denied,
    Cancelled,
    TimedOut,
    Failed,
}
#[derive(Clone)]
pub struct OAuthEvent {
    pub installation_id: String,
    pub workspace_id: String,
    pub scope_kind: String,
    pub scope_key: String,
    pub identity: String,
    pub generation: String,
    pub revision: u64,
    pub client_id: Option<u64>,
    pub flow_id: Option<String>,
    pub state: OAuthState,
    pub authorization_url: Option<String>,
    pub diagnostic: Option<String>,
}
impl std::fmt::Debug for OAuthEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthEvent")
            .field("state", &self.state)
            .finish()
    }
}
#[async_trait]
pub trait OAuthEventSink: Send + Sync {
    async fn client_available(&self, client_id: u64, workspace_id: &str) -> bool;
    async fn emit(&self, event: OAuthEvent);
}
/// Callback fields are intentionally never Debug or serialized by this crate.
pub struct OAuthCallback {
    pub flow_id: String,
    pub state: String,
    pub code: Option<String>,
    pub issuer: Option<String>,
    pub error: Option<String>,
}
struct Intent {
    id: String,
    client: u64,
    workspace: String,
    redirect: String,
    preparation_failed: bool,
    explicit_consent: bool,
    deadline: tokio::time::Instant,
    wall_deadline: std::time::SystemTime,
}
struct Flow {
    id: String,
    state: String,
    deadline: tokio::time::Instant,
    wall_deadline: std::time::SystemTime,
    consumed: bool,
}
struct Entry {
    cleanup_available: std::sync::atomic::AtomicBool,
    installation: McpServerInstallation,
    identity: String,
    generation: String,
    oauth_http: Arc<crate::network::ExchangeBudgetHttpClient>,
    credentials: PersistentCredentials,
    cancellation: CancellationToken,
    active_flow: std::sync::Mutex<Option<ActiveFlow>>,
    projection: std::sync::Mutex<ObservableProjection>,
    data: Mutex<EntryData>,
    #[cfg(feature = "test-support")]
    completed_exchange: std::sync::Mutex<Option<String>>,
    presentation_owner: std::sync::Mutex<Option<(String, u64, String)>>,
}
#[derive(Clone)]
struct ObservableProjection {
    revision: u64,
    state: OAuthState,
    operation: Option<String>,
}
struct ActiveFlow {
    terminal_decision: Option<OAuthState>,
    started: bool,
    callback_ready: bool,
    callback_accepted: bool,
    deadline: tokio::time::Instant,
    wall_deadline: std::time::SystemTime,
    id: String,
    client: u64,
    workspace: String,
}
struct EntryData {
    intent: Option<Intent>,
    flow: Option<Flow>,
    manager: Option<OAuthHttpClient>,
    manager_http: Option<Arc<crate::network::ClientAuthenticationHttpClient>>,
    states: InMemoryStateStore,
    challenge: Option<String>,
    status: OAuthState,
    next_attempt: tokio::time::Instant,
    attempts: u32,
    terminal: bool,
    refresh_recovery_pending: bool,
    revision: u64,
}
#[cfg(feature = "test-support")]
#[derive(Default)]
pub struct OAuthTestHooks {
    pub pause_after_exchange: std::sync::atomic::AtomicBool,
    pub exchange_returned: tokio::sync::Notify,
    pub decide_exchange: tokio::sync::Notify,
    pub pause_after_winner: std::sync::atomic::AtomicBool,
    pub winner_reserved: tokio::sync::Notify,
    pub publish_resolution: tokio::sync::Notify,
    pub retirement_observed: tokio::sync::Notify,
}
#[cfg(feature = "test-support")]
struct ExchangeCompleted(Arc<Entry>, String);
#[cfg(feature = "test-support")]
impl Drop for ExchangeCompleted {
    fn drop(&mut self) {
        *self.0.completed_exchange.lock().unwrap() = Some(self.1.clone());
    }
}
#[derive(Clone)]
pub struct OAuthServiceOptions {
    #[cfg(feature = "test-support")]
    pub test_hooks: Option<Arc<OAuthTestHooks>>,
    pub flow_timeout: Duration,
    pub install_timeout: Duration,
    pub poll_interval: Duration,
    pub clock: Arc<dyn crate::OAuthClock>,
}
impl Default for OAuthServiceOptions {
    fn default() -> Self {
        Self {
            #[cfg(feature = "test-support")]
            test_hooks: None,
            flow_timeout: Duration::from_secs(300),
            install_timeout: Duration::from_secs(600),
            poll_interval: Duration::from_secs(5),
            clock: Arc::new(crate::clock::SystemOAuthClock),
        }
    }
}
// Admission covers external callers as well as service-owned tasks. Closing it
// and waiting for all permits is the mutation frontier before the final IO drain.
#[derive(Default)]
struct LifecycleAdmission {
    state: std::sync::Mutex<(bool, usize)>,
    changed: tokio::sync::Notify,
}
struct LifecycleCaller(Arc<LifecycleAdmission>);
impl Drop for LifecycleCaller {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.1 -= 1;
        self.0.changed.notify_waiters();
    }
}
impl LifecycleAdmission {
    fn enter(self: &Arc<Self>) -> Result<LifecycleCaller, AuthError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.0 {
            return Err(AuthError::AuthorizationRequired);
        }
        state.1 += 1;
        Ok(LifecycleCaller(self.clone()))
    }
    fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0 = true;
    }
    async fn drained(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .1
                == 0
            {
                return;
            }
            changed.await;
        }
    }
}
struct Inner {
    options: OAuthServiceOptions,
    persistence: OAuthPersistence,
    sink: Arc<dyn OAuthEventSink>,
    entries: Mutex<HashMap<String, Arc<Entry>>>,
    bindings: std::sync::Mutex<HashMap<String, std::sync::Weak<Mutex<()>>>>,
    callers: Arc<LifecycleAdmission>,
    shutdown_gate: Mutex<()>,
    shutdown: CancellationToken,
    tasks: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    retired_events: std::sync::Mutex<std::collections::VecDeque<OAuthEvent>>,
    http: reqwest::Client,
    oauth_http: Arc<dyn rmcp::transport::auth::OAuthHttpClient>,
}
#[derive(Clone)]
pub struct McpOAuthService {
    inner: Arc<Inner>,
}

fn identity(installation: &McpServerInstallation, client_secret: Option<&str>) -> String {
    let resource = match &installation.transport {
        McpTransportConfig::StreamableHttp { url, .. } => url.as_str(),
        _ => "",
    };
    // Tokens are outside configuration and effective-secret fingerprints.
    let bytes = serde_json::to_vec(&(
        resource,
        &installation.auth,
        client_secret,
        installation.transport.has_authorization_header(),
    ))
    .expect("serializable OAuth identity");
    hex::encode(Sha256::digest(bytes))
}
fn resource(installation: &McpServerInstallation) -> Result<&str, AuthError> {
    match &installation.transport {
        McpTransportConfig::StreamableHttp { url, .. } => Ok(url),
        _ => Err(AuthError::NoAuthorizationSupport),
    }
}
fn valid_redirect(value: &str) -> bool {
    Url::parse(value).is_ok_and(|u| {
        u.scheme() == "http"
            && u.host_str() == Some("127.0.0.1")
            && u.port().is_some()
            && u.path() == "/oauth/mcp/callback"
            && u.query().is_none()
            && u.fragment().is_none()
            && u.username().is_empty()
            && u.password().is_none()
    })
}
impl McpOAuthService {
    pub fn new(
        persistence: OAuthPersistence,
        sink: Arc<dyn OAuthEventSink>,
    ) -> Result<Self, McpRuntimeError> {
        Self::with_options(persistence, sink, OAuthServiceOptions::default())
    }
    pub fn with_options(
        persistence: OAuthPersistence,
        sink: Arc<dyn OAuthEventSink>,
        options: OAuthServiceOptions,
    ) -> Result<Self, McpRuntimeError> {
        // No MCP headers, no redirects (including token endpoint redirects).
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| McpRuntimeError::failed("OAuth HTTP client initialization failed"))?;
        let oauth_http = Arc::new(
            rmcp::transport::auth::default_oauth_http_client()
                .map_err(|e| oauth_runtime_error(&e))?,
        );
        Ok(Self {
            inner: Arc::new(Inner {
                oauth_http,
                options,
                persistence,
                sink,
                entries: Mutex::new(HashMap::new()),
                bindings: Default::default(),
                callers: Arc::new(LifecycleAdmission::default()),
                shutdown_gate: Mutex::new(()),
                shutdown: CancellationToken::new(),
                tasks: std::sync::Mutex::new(Vec::new()),
                retired_events: std::sync::Mutex::new(std::collections::VecDeque::new()),
                http,
            }),
        })
    }
    fn spawn(&self, future: impl std::future::Future<Output = ()> + Send + 'static) {
        let mut tasks = self
            .inner
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Checked under the same registry mutex used by shutdown: no task
        // can be registered after shutdown has taken its final batch.
        if self.inner.shutdown.is_cancelled() {
            return;
        }
        tasks.retain(|task| !task.is_finished());
        tasks.push(tokio::spawn(future));
    }
    async fn connection_identity(
        &self,
        installation: &McpServerInstallation,
    ) -> Result<String, AuthError> {
        let secret = if let Some(reference) = installation
            .auth
            .oauth
            .as_ref()
            .and_then(|config| config.client_secret_ref.as_deref())
        {
            Some(
                self.inner
                    .persistence
                    .client_secret(reference)
                    .await?
                    .ok_or_else(|| {
                        AuthError::CredentialStoreError("OAuth client secret unavailable".into())
                    })?,
            )
        } else {
            None
        };
        Ok(identity(installation, secret.as_deref()))
    }
    async fn binding_guard(&self, id: &str) -> Result<tokio::sync::OwnedMutexGuard<()>, AuthError> {
        let gate = {
            let mut gates = self
                .inner
                .bindings
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            gates.retain(|_, gate| gate.strong_count() > 0);
            match gates.get(id).and_then(std::sync::Weak::upgrade) {
                Some(gate) => gate,
                None => {
                    let gate = Arc::new(Mutex::new(()));
                    gates.insert(id.into(), Arc::downgrade(&gate));
                    gate
                }
            }
        };
        tokio::select! { _=self.inner.shutdown.cancelled()=>Err(AuthError::AuthorizationRequired), guard=gate.lock_owned()=>Ok(guard) }
    }
    fn new_entry(
        &self,
        id: &str,
        installation: &McpServerInstallation,
        hash: String,
    ) -> Arc<Entry> {
        let cancellation = self.inner.shutdown.child_token();
        Arc::new(Entry {
            installation: installation.clone(),
            identity: hash.clone(),
            generation: uuid::Uuid::new_v4().to_string(),
            oauth_http: Arc::new(crate::network::ExchangeBudgetHttpClient::new(
                self.inner.oauth_http.clone(),
                cancellation.clone(),
            )),
            credentials: self.inner.persistence.credential_store(
                id.into(),
                hash,
                Arc::new(Mutex::new(())),
                cancellation.clone(),
            ),
            cancellation,
            active_flow: std::sync::Mutex::new(None),
            cleanup_available: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "test-support")]
            completed_exchange: std::sync::Mutex::new(None),
            presentation_owner: std::sync::Mutex::new(None),
            projection: std::sync::Mutex::new(ObservableProjection {
                revision: 0,
                state: OAuthState::Idle,
                operation: None,
            }),
            data: Mutex::new(EntryData {
                intent: None,
                flow: None,
                manager: None,
                manager_http: None,
                states: InMemoryStateStore::new(),
                challenge: None,
                status: OAuthState::Idle,
                next_attempt: tokio::time::Instant::now(),
                attempts: 0,
                terminal: false,
                refresh_recovery_pending: false,
                revision: 0,
            }),
        })
    }
    async fn bind(
        &self,
        id: &str,
        installation: &McpServerInstallation,
    ) -> Result<Arc<Entry>, AuthError> {
        self.bind_inner(id, installation, false).await
    }
    async fn bind_inner(
        &self,
        id: &str,
        installation: &McpServerInstallation,
        management_only: bool,
    ) -> Result<Arc<Entry>, AuthError> {
        let _caller = self.inner.callers.enter()?;
        if self.inner.shutdown.is_cancelled() {
            return Err(AuthError::AuthorizationRequired);
        }
        let _binding = self.binding_guard(id).await?;
        let old = self.inner.entries.lock().await.get(id).cloned();
        if self.inner.shutdown.is_cancelled() {
            return Err(AuthError::AuthorizationRequired);
        }
        if let Some(entry) = old.as_ref() {
            if Self::same_configuration(&entry.installation, installation)
                && entry
                    .projection
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .state
                    == OAuthState::CleanupRequired
            {
                if management_only {
                    // Synchronization admits a non-usable management holder;
                    // transport and consent callers still fail closed.
                    return Ok(entry.clone());
                }
                return Err(AuthError::CredentialStoreError(
                    "OAuth account cleanup requires retry".into(),
                ));
            }
        }
        let hash = self.connection_identity(installation).await?;
        if self.inner.shutdown.is_cancelled() {
            return Err(AuthError::AuthorizationRequired);
        }
        if let Some(entry) = old.as_ref() {
            if entry.identity == hash && !entry.cancellation.is_cancelled() {
                let failed_consent = entry.credentials.consent_staged()
                    && entry
                        .projection
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .state
                        == OAuthState::Failed
                    && entry
                        .active_flow
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .is_none();
                if failed_consent {
                    let mut data = entry.data.lock().await;
                    // Recover the previous record without retiring a healthy old
                    // manager. Local refresh and file leases fence SDK rotation.
                    let _refresh = entry.credentials.gate.lock().await;
                    self.inner
                        .persistence
                        .clear_stale_record(id, &entry.identity)
                        .await?;
                    entry.credentials.finish_failed_consent();
                    data.terminal = false;
                    data.next_attempt = tokio::time::Instant::now();
                }
                return Ok(entry.clone());
            }
            entry.cancellation.cancel();
            // The old actor is fenced before the replacement can use credentials.
        }
        let entry = self.new_entry(id, installation, hash);
        // Keep the replacement invisible until old exchanges and durable writes
        // have drained. No database capacity is held by this registry lock.
        if let Some(old) = old {
            let mut data = old.data.lock().await;
            self.retire_presentation(id, &old, &data).await;
            if data.intent.is_some() && data.status != OAuthState::Resolving {
                data.status = OAuthState::Cancelled;
                self.event(id, &old, &data, None, None).await;
            }
            if let Some(flow) = data.flow.take() {
                data.states.delete(&flow.state).await?;
            }
            let _guard = old.credentials.gate.lock().await;
            self.inner
                .persistence
                .clear_stale_record(id, &entry.identity)
                .await?;
        } else {
            self.inner
                .persistence
                .clear_stale_record(id, &entry.identity)
                .await?;
        }
        // Initialize the observable projection before publishing the entry.
        // Details never performs keystore I/O while an actor owns network work.
        let record = entry.credentials.record().await?;
        entry
            .cleanup_available
            .store(record.is_some(), std::sync::atomic::Ordering::Release);
        let status = match record {
            Some(record) if record.credentials.is_some() => OAuthState::Authorized,
            Some(_) => OAuthState::AuthRequired,
            None => OAuthState::Idle,
        };
        entry
            .projection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .state = status;
        {
            let mut entries = self.inner.entries.lock().await;
            if self.inner.shutdown.is_cancelled() {
                return Err(AuthError::AuthorizationRequired);
            }
            entries.insert(id.into(), entry.clone());
        }
        let service = self.clone();
        let id = id.to_owned();
        let worker_entry = entry.clone();
        self.spawn(async move {
            service.worker(id, worker_entry).await;
        });
        Ok(entry)
    }
    /// Volatile intent belongs to the current authenticated connection only.
    /// It is never replayed from installation persistence at application startup.
    pub async fn begin_install(
        &self,
        id: &str,
        installation: &McpServerInstallation,
        client: u64,
        redirect: &str,
    ) -> Result<(), McpRuntimeError> {
        self.begin_install_in_workspace(id, installation, client, redirect, &installation.scope_key)
            .await
    }
    pub async fn begin_install_in_workspace(
        &self,
        id: &str,
        installation: &McpServerInstallation,
        client: u64,
        redirect: &str,
        workspace: &str,
    ) -> Result<(), McpRuntimeError> {
        self.begin_intent(id, installation, client, redirect, workspace, false, false)
            .await
    }
    pub async fn begin_install_without_callback(
        &self,
        id: &str,
        installation: &McpServerInstallation,
        client: u64,
        workspace: &str,
    ) -> Result<(), McpRuntimeError> {
        self.begin_intent(id, installation, client, "", workspace, false, true)
            .await
    }
    async fn begin_intent(
        &self,
        id: &str,
        installation: &McpServerInstallation,
        client: u64,
        redirect: &str,
        workspace: &str,
        explicit_sign_in: bool,
        preparation_failed: bool,
    ) -> Result<(), McpRuntimeError> {
        if installation.transport.has_authorization_header() {
            return Ok(());
        }
        if !preparation_failed && !valid_redirect(redirect) {
            return Err(McpRuntimeError::failed(
                "Invalid OAuth loopback redirect URI",
            ));
        }
        let entry = self
            .bind(id, installation)
            .await
            .map_err(|e| oauth_runtime_error(&e))?;
        if entry
            .active_flow
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
        {
            return Ok(());
        }
        let mut data = entry.data.lock().await;
        if data.intent.is_none() && data.flow.is_none() {
            // A saved registration belongs to a previous operation. Updating or
            // repeating an install never converts authorization loss into a new
            // automatic browser flow. Explicit consent may reuse registration.
            if !explicit_sign_in
                && entry
                    .credentials
                    .record()
                    .await
                    .map_err(|error| oauth_runtime_error(&error))?
                    .is_some()
            {
                return Ok(());
            }
            let operation_id = uuid::Uuid::new_v4().to_string();
            let deadline = tokio::time::Instant::now() + self.inner.options.install_timeout;
            let wall_deadline = self.inner.options.clock.now() + self.inner.options.install_timeout;
            entry
                .projection
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .operation = Some(operation_id.clone());
            *entry
                .active_flow
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ActiveFlow {
                terminal_decision: None,
                started: false,
                callback_ready: false,
                callback_accepted: false,
                deadline,
                wall_deadline,
                id: operation_id.clone(),
                client,
                workspace: workspace.into(),
            });
            let monitor_service = self.clone();
            let monitor_entry = entry.clone();
            let monitor_id = id.to_owned();
            let monitor_operation = operation_id.clone();
            let monitor_workspace = workspace.to_owned();
            self.spawn(async move {
                loop {
                    tokio::select! { _=monitor_entry.cancellation.cancelled()=>return, _=tokio::time::sleep(monitor_service.inner.options.poll_interval)=>{} }
                    let deadlines = monitor_entry.active_flow.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref().filter(|a| a.id == monitor_operation).map(|a| (a.deadline, a.wall_deadline));
                    let Some((deadline, wall_deadline)) = deadlines else { return; };
                    let expired = tokio::time::Instant::now() >= deadline || monitor_service.inner.options.clock.now() >= wall_deadline;
                    if expired || !monitor_service.inner.sink.client_available(client, &monitor_workspace).await {
                        if monitor_service.retire_resolution(&monitor_entry, &monitor_operation) {
                            // Consent cannot be retroactively cancelled. Stop the owned
                            // reconciliation; durable uncertainty remains manageable
                            // through Clear account, replacement or restart recovery.
                            return;
                        }
                        let result = monitor_service.cancel_operation(&monitor_id, client, &monitor_operation, if expired { OAuthState::TimedOut } else { OAuthState::Cancelled }).await;
                        if result.is_err() {
                            // The terminal decision may win while availability was
                            // being checked. Revalidate at the admission mutex.
                            monitor_service.retire_resolution(&monitor_entry, &monitor_operation);
                        }
                        return;
                    }
                }
            });
            // Consent has its own temporary SDK state store. An existing
            // live manager must not retain the new operation's PKCE verifier.
            data.states = InMemoryStateStore::new();
            data.intent = Some(Intent {
                id: operation_id,
                client,
                workspace: workspace.into(),
                redirect: redirect.into(),
                preparation_failed,
                explicit_consent: explicit_sign_in,
                deadline,
                wall_deadline,
            });
        }
        Ok(())
    }
    pub async fn sign_in(
        &self,
        id: &str,
        installation: &McpServerInstallation,
        client: u64,
        redirect: &str,
    ) -> Result<(), McpRuntimeError> {
        self.sign_in_in_workspace(id, installation, client, redirect, &installation.scope_key)
            .await
    }
    pub async fn sign_in_in_workspace(
        &self,
        id: &str,
        installation: &McpServerInstallation,
        client: u64,
        redirect: &str,
        workspace: &str,
    ) -> Result<(), McpRuntimeError> {
        if installation.transport.has_authorization_header() {
            return Err(McpRuntimeError::failed(
                "Remove the explicit Authorization header before using OAuth",
            ));
        }
        self.begin_intent(id, installation, client, redirect, workspace, true, false)
            .await?;
        let entry = self
            .inner
            .entries
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| McpRuntimeError::failed("OAuth installation unavailable"))?;
        {
            let mut active = entry
                .active_flow
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(operation) = active.as_mut() {
                if operation.started {
                    return Ok(());
                }
                operation.started = true;
            }
        }
        {
            let mut data = entry.data.lock().await;
            if data.flow.is_some() {
                return Ok(());
            }
            data.terminal = false;
            data.attempts = 0;
            data.next_attempt = tokio::time::Instant::now();
            data.status = OAuthState::AuthRequired;
        }
        let service = self.clone();
        let id = id.to_owned();
        self.spawn(async move {
            service.prepare_flow(&id, &entry).await;
        });
        Ok(())
    }
    fn retire_resolution(&self, entry: &Entry, operation: &str) -> bool {
        let active = entry
            .active_flow
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if active.as_ref().is_some_and(|flow| {
            flow.id == operation && flow.terminal_decision == Some(OAuthState::Authorized)
        }) {
            entry.cancellation.cancel();
            #[cfg(feature = "test-support")]
            if let Some(hooks) = &self.inner.options.test_hooks {
                hooks.retirement_observed.notify_one();
            }
            true
        } else {
            false
        }
    }
    async fn retire_presentation(&self, id: &str, entry: &Entry, data: &EntryData) {
        let owner = entry
            .presentation_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some((flow_id, client_id, workspace_id)) = owner else {
            return;
        };
        let event = OAuthEvent {
            installation_id: id.into(),
            workspace_id,
            scope_kind: entry.installation.scope_kind.as_str().into(),
            scope_key: entry.installation.scope_key.clone(),
            identity: entry.identity.clone(),
            generation: entry.generation.clone(),
            revision: data.revision,
            client_id: Some(client_id),
            flow_id: Some(flow_id),
            state: OAuthState::Retired,
            authorization_url: None,
            diagnostic: None,
        };
        // This signal contains only safe operation identity. It may be delivered
        // after resource replacement; Client removes only the matching flow.
        self.emit_event(event).await;
    }
    #[cfg(feature = "test-support")]
    pub async fn resolution_finished_for_test(&self, id: &str, flow: &str) -> bool {
        let Some(entry) = self.inner.entries.lock().await.get(id).cloned() else {
            return false;
        };
        if entry.completed_exchange.lock().unwrap().as_deref() != Some(flow) {
            return false;
        }
        let Ok(data) = entry.data.try_lock() else {
            return false;
        };
        if data.flow.is_some()
            || data.intent.is_some()
            || data.manager.is_some()
            || data.manager_http.is_some()
            || entry.active_flow.lock().unwrap().is_some()
        {
            return false;
        }
        // Exchange completion is signaled after all local guards drop. This
        // tests the real lease, including blocking writers retaining its Arc.
        let Ok(_gate) = entry.credentials.gate.try_lock() else {
            return false;
        };
        self.inner
            .persistence
            .refresh_lease_available_for_test(id)
            .await
    }
    async fn event(
        &self,
        id: &str,
        entry: &Entry,
        data: &EntryData,
        url: Option<String>,
        diagnostic: Option<String>,
    ) {
        if entry.cancellation.is_cancelled()
            && !matches!(
                data.status,
                OAuthState::Cancelled
                    | OAuthState::TimedOut
                    | OAuthState::Failed
                    | OAuthState::Resolving
                    | OAuthState::CleanupRequired
            )
        {
            return;
        }
        if let Some(intent) = &data.intent {
            let operation = data
                .flow
                .as_ref()
                .map(|f| f.id.clone())
                .unwrap_or_else(|| intent.id.clone());
            *entry
                .presentation_owner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some((operation, intent.client, intent.workspace.clone()));
        }
        *entry
            .projection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = ObservableProjection {
            revision: data.revision,
            state: data.status,
            operation: data
                .flow
                .as_ref()
                .map(|flow| flow.id.clone())
                .or_else(|| data.intent.as_ref().map(|intent| intent.id.clone())),
        };
        self.emit_event(OAuthEvent {
            installation_id: id.into(),
            workspace_id: data
                .intent
                .as_ref()
                .map(|i| i.workspace.clone())
                .unwrap_or_else(|| entry.installation.scope_key.clone()),
            scope_kind: entry.installation.scope_kind.as_str().into(),
            scope_key: entry.installation.scope_key.clone(),
            identity: entry.identity.clone(),
            generation: entry.generation.clone(),
            revision: data.revision,
            client_id: data.intent.as_ref().map(|i| i.client),
            flow_id: data
                .flow
                .as_ref()
                .map(|f| f.id.clone())
                .or_else(|| data.intent.as_ref().map(|i| i.id.clone())),
            state: data.status,
            authorization_url: url,
            diagnostic,
        })
        .await;
    }
    async fn emit_event(&self, event: OAuthEvent) {
        if matches!(
            event.state,
            OAuthState::Cancelled | OAuthState::TimedOut | OAuthState::Failed | OAuthState::Retired
        ) && event.flow_id.is_some()
            && event.authorization_url.is_none()
        {
            let mut retired = self
                .inner
                .retired_events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if retired.len() == 1024 {
                retired.pop_front();
            }
            retired.push_back(event.clone());
        }
        self.inner.sink.emit(event).await;
    }
    async fn make_manager(
        &self,
        entry: &Entry,
        data: &EntryData,
    ) -> Result<
        (
            AuthorizationManager,
            Arc<crate::network::ClientAuthenticationHttpClient>,
        ),
        AuthError,
    > {
        let oauth_http = Arc::new(crate::network::ClientAuthenticationHttpClient::new(
            entry.oauth_http.clone(),
        ));
        let mut manager = AuthorizationManager::new_with_oauth_http_client(
            resource(&entry.installation)?,
            oauth_http.clone(),
        )
        .await?;
        manager.set_state_store(data.states.clone());
        manager.set_credential_store(entry.credentials.clone());
        let mut upgrade = ScopeUpgradeConfig::default();
        upgrade.auto_upgrade = false;
        upgrade.max_upgrade_attempts = 0;
        manager.set_scope_upgrade_config(upgrade);
        let resolution = manager
            .resolve_metadata_from_challenge(data.challenge.as_deref())
            .await?;
        if !resolution.source.is_discovered() {
            return Err(AuthError::NoAuthorizationSupport);
        }
        let issuer = resolution
            .metadata
            .issuer
            .as_deref()
            .ok_or_else(|| AuthError::MetadataError("OAuth issuer missing".into()))?;
        if let Some(expected) = entry
            .installation
            .auth
            .oauth
            .as_ref()
            .and_then(|o| o.issuer.as_deref())
        {
            if issuer != expected {
                return Err(AuthError::AuthorizationServerMismatch {
                    expected_issuer: expected.into(),
                    received_issuer: issuer.into(),
                });
            }
        }
        if let Some(record) = entry.credentials.record().await? {
            if record.issuer != issuer {
                return Err(AuthError::AuthorizationRequired);
            }
            oauth_http.configure_authentication(&record.registration, &resolution.metadata)?;
            manager.set_metadata(resolution.metadata);
            // initialize_from_store would replace both secret and redirect URI.
            manager.configure_client(record.registration.config())?;
        } else {
            manager.set_metadata(resolution.metadata);
        }
        Ok((manager, oauth_http))
    }
    async fn restore(
        &self,
        id: &str,
        entry: &Entry,
        data: &mut EntryData,
    ) -> Result<Option<OAuthHttpClient>, AuthError> {
        if let Some(manager) = &data.manager {
            return Ok(Some(manager.clone()));
        }
        if entry.credentials.record().await?.is_none() {
            return Ok(None);
        }
        let (manager, oauth_http) = self
            .make_manager(entry, data)
            .with_subscriber(tracing::subscriber::NoSubscriber::default())
            .await?;
        let client = OAuthHttpClient::new(self.inner.http.clone(), manager);
        data.manager = Some(client.clone());
        data.manager_http = Some(oauth_http);
        self.event(id, entry, data, None, None).await;
        Ok(Some(client))
    }
    async fn prepare_authorization(
        &self,
        _id: &str,
        entry: &Entry,
        data: &EntryData,
        redirect: &str,
    ) -> Result<
        (
            OAuthHttpClient,
            String,
            Arc<crate::network::ClientAuthenticationHttpClient>,
        ),
        AuthError,
    > {
        // Discover afresh for consent; never send an old token to a new issuer.
        let oauth_http = Arc::new(crate::network::ClientAuthenticationHttpClient::new(
            entry.oauth_http.clone(),
        ));
        let mut manager = AuthorizationManager::new_with_oauth_http_client(
            resource(&entry.installation)?,
            oauth_http.clone(),
        )
        .await?;
        manager.set_credential_store(entry.credentials.clone());
        manager.set_state_store(data.states.clone());
        let mut upgrade = ScopeUpgradeConfig::default();
        upgrade.auto_upgrade = false;
        upgrade.max_upgrade_attempts = 0;
        manager.set_scope_upgrade_config(upgrade);
        let resolution = manager
            .resolve_metadata_from_challenge(data.challenge.as_deref())
            .await?;
        if !resolution.source.is_discovered() {
            return Err(AuthError::NoAuthorizationSupport);
        }
        let issuer = resolution
            .metadata
            .issuer
            .clone()
            .ok_or_else(|| AuthError::MetadataError("OAuth issuer missing".into()))?;
        if let Some(expected) = entry
            .installation
            .auth
            .oauth
            .as_ref()
            .and_then(|o| o.issuer.as_deref())
        {
            if issuer != expected {
                return Err(AuthError::AuthorizationServerMismatch {
                    expected_issuer: expected.into(),
                    received_issuer: issuer,
                });
            }
        }
        let metadata = resolution.metadata.clone();
        manager.set_metadata(resolution.metadata);
        let configured = entry
            .installation
            .auth
            .oauth
            .as_ref()
            .map(|o| o.scopes.iter().map(String::as_str).collect::<Vec<_>>())
            .unwrap_or_default();
        let mut scopes = manager.select_scopes(None, &configured);
        for scope in configured {
            if !scopes.iter().any(|s| s == scope) {
                scopes.push(scope.into());
            }
        }
        let previous = entry.credentials.record().await?;
        if let Some(previous) = previous.as_ref().filter(|r| r.issuer == issuer) {
            if let Some(credentials) = &previous.credentials {
                for scope in &credentials.granted_scopes {
                    if !scopes.contains(scope) {
                        scopes.push(scope.clone());
                    }
                }
            }
        }
        let scope_refs = scopes.iter().map(String::as_str).collect::<Vec<_>>();
        let registration = if let Some(previous) = previous.filter(|r| r.issuer == issuer) {
            if previous.registration.redirect_uri != redirect {
                return Err(AuthError::RegistrationFailed(
                    "Stored redirect URI is unavailable on this device".into(),
                ));
            }
            previous.registration
        } else if let Some(config) = entry
            .installation
            .auth
            .oauth
            .as_ref()
            .filter(|o| o.client_id.is_some())
        {
            let mut client = OAuthClientConfig::new(config.client_id.clone().unwrap(), redirect)
                .with_scopes(scopes.clone());
            if let Some(reference) = &config.client_secret_ref {
                client.client_secret = self.inner.persistence.client_secret(reference).await?;
                if client.client_secret.is_none() {
                    return Err(AuthError::CredentialStoreError(
                        "OAuth client secret unavailable".into(),
                    ));
                }
            }
            let mut registration = Registration::from(client);
            registration.token_endpoint_auth_method = config.token_endpoint_auth_method.clone();
            registration
        } else {
            entry.oauth_http.reset_registration_failure();
            let mut registration = Registration::from(
                manager
                    .register_client("Pioneer", redirect, &scope_refs)
                    .await?,
            );
            if let Some((request, response)) = entry.oauth_http.take_registration().await {
                if !response.redirect_uris.iter().any(|uri| uri == redirect) {
                    return Err(AuthError::RegistrationFailed(
                        "Stored redirect URI is unavailable on this device".into(),
                    ));
                }
                registration.registration_request = Some(request);
                registration.registration_response = Some(response);
            }
            registration
        };
        oauth_http.configure_authentication(&registration, &metadata)?;
        manager.configure_client(registration.config())?;
        // Registration and tokens share one atomic record. Guard the read/write
        // against request/background refresh; no DB transaction crosses this wait.
        let _guard = entry.credentials.acquire_refresh_guard().await?;
        let credentials = entry
            .credentials
            .record()
            .await?
            .filter(|r| r.issuer == issuer && r.registration.client_id == registration.client_id)
            .and_then(|r| r.credentials);
        // A failed write can have mutated storage. Keep cleanup actionable until
        // explicit deletion confirms both account and promotion fence are gone.
        entry
            .cleanup_available
            .store(true, std::sync::atomic::Ordering::Release);
        entry
            .credentials
            .write_record(AuthorizationRecord {
                identity: entry.identity.clone(),
                resource: resource(&entry.installation)?.into(),
                issuer,
                registration,
                credentials,
                pending_consent: None,
            })
            .await?;
        let url = manager.get_authorization_url(&scope_refs).await?;
        Ok((
            OAuthHttpClient::new(self.inner.http.clone(), manager),
            url,
            oauth_http,
        ))
    }
    async fn prepare_flow(&self, id: &str, entry: &Arc<Entry>) {
        let mut data = entry.data.lock().await;
        if entry.cancellation.is_cancelled()
            || data.flow.is_some()
            || data.terminal
            || tokio::time::Instant::now() < data.next_attempt
        {
            return;
        }
        let Some(intent) = &data.intent else {
            return;
        };
        if (tokio::time::Instant::now() >= intent.deadline
            || self.inner.options.clock.now() >= intent.wall_deadline)
            || !self
                .inner
                .sink
                .client_available(intent.client, &intent.workspace)
                .await
        {
            data.status = if tokio::time::Instant::now() >= intent.deadline
                || self.inner.options.clock.now() >= intent.wall_deadline
            {
                OAuthState::TimedOut
            } else {
                OAuthState::Cancelled
            };
            data.terminal = true;
            self.event(id, entry, &data, None, None).await;
            data.intent = None;
            *entry
                .active_flow
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            return;
        }
        if intent.preparation_failed {
            data.status = OAuthState::Failed;
            data.terminal = true;
            self.event(
                id,
                entry,
                &data,
                None,
                Some("oauth_callback_preparation_failed".into()),
            )
            .await;
            data.intent = None;
            *entry
                .active_flow
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            return;
        }
        let redirect = intent.redirect.clone();
        if let Some(operation) = entry
            .active_flow
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
        {
            operation.started = true;
        }
        data.status = OAuthState::Preparing;
        self.event(id, entry, &data, None, None).await;
        let result = tokio::select! {
            _=entry.cancellation.cancelled()=>return,
            result=self.prepare_authorization(id, entry, &data, &redirect).with_subscriber(tracing::subscriber::NoSubscriber::default())=>result,
        };
        match result {
            Ok((manager, url, oauth_http)) => {
                let parsed = match Url::parse(&url) {
                    Ok(u) => u,
                    Err(_) => {
                        data.status = OAuthState::Failed;
                        data.terminal = true;
                        self.event(
                            id,
                            entry,
                            &data,
                            None,
                            Some("OAuth authorization URL unavailable".into()),
                        )
                        .await;
                        data.states = InMemoryStateStore::new();
                        data.intent = None;
                        *entry
                            .active_flow
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                        return;
                    }
                };
                let state = parsed
                    .query_pairs()
                    .find(|(k, _)| k == "state")
                    .map(|(_, v)| v.into_owned());
                let Some(state) = state else {
                    data.status = OAuthState::Failed;
                    data.terminal = true;
                    self.event(
                        id,
                        entry,
                        &data,
                        None,
                        Some("OAuth authorization state unavailable".into()),
                    )
                    .await;
                    data.states = InMemoryStateStore::new();
                    data.intent = None;
                    *entry
                        .active_flow
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                    return;
                };
                // Discovery/registration can finish after sleep or after the
                // initiating connection disappears. Revalidate before emitting
                // an effect, and remove the SDK's temporary PKCE state.
                let intent = data.intent.as_ref().unwrap();
                let expired = tokio::time::Instant::now() >= intent.deadline
                    || self.inner.options.clock.now() >= intent.wall_deadline;
                let available = self
                    .inner
                    .sink
                    .client_available(intent.client, &intent.workspace)
                    .await;
                if expired || !available || entry.cancellation.is_cancelled() {
                    data.states.delete(&state).await.ok();
                    data.status = if expired {
                        OAuthState::TimedOut
                    } else {
                        OAuthState::Cancelled
                    };
                    data.terminal = true;
                    self.event(id, entry, &data, None, None).await;
                    data.states = InMemoryStateStore::new();
                    data.intent = None;
                    *entry
                        .active_flow
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                    return;
                }
                data.flow = Some(Flow {
                    id: data.intent.as_ref().unwrap().id.clone(),
                    state,
                    deadline: (tokio::time::Instant::now() + self.inner.options.flow_timeout)
                        .min(data.intent.as_ref().unwrap().deadline),
                    wall_deadline: (self.inner.options.clock.now()
                        + self.inner.options.flow_timeout)
                        .min(data.intent.as_ref().unwrap().wall_deadline),
                    consumed: false,
                });
                *entry
                    .active_flow
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ActiveFlow {
                    terminal_decision: None,
                    started: true,
                    callback_ready: true,
                    callback_accepted: false,
                    deadline: data.flow.as_ref().unwrap().deadline,
                    wall_deadline: data.flow.as_ref().unwrap().wall_deadline,
                    id: data.flow.as_ref().unwrap().id.clone(),
                    client: data.intent.as_ref().unwrap().client,
                    workspace: data.intent.as_ref().unwrap().workspace.clone(),
                });
                data.manager = Some(manager);
                data.manager_http = Some(oauth_http);
                data.status = OAuthState::AwaitingCallback;
                data.terminal = true;
                self.event(id, entry, &data, Some(url), None).await;
            }
            Err(error) => {
                data.attempts += 1;
                data.next_attempt = tokio::time::Instant::now()
                    + Duration::from_secs(
                        (1u64 << data.attempts.min(6))
                            + u64::from(uuid::Uuid::new_v4().as_bytes()[0]) % 3,
                    );
                data.terminal = data.attempts >= 5
                    || matches!(
                        error,
                        AuthError::NoAuthorizationSupport
                            | AuthError::PkceUnsupported
                            | AuthError::AuthorizationServerMismatch { .. }
                    )
                    || (matches!(error, AuthError::RegistrationFailed(_))
                        && !entry.oauth_http.registration_was_transient());
                data.status = if data.terminal {
                    OAuthState::Failed
                } else {
                    OAuthState::Preparing
                };
                self.event(
                    id,
                    entry,
                    &data,
                    None,
                    Some(oauth_runtime_error(&error).message),
                )
                .await;
                if data.terminal {
                    data.states = InMemoryStateStore::new();
                    data.intent = None;
                    *entry
                        .active_flow
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                }
            }
        }
    }
    pub async fn callback(
        &self,
        id: &str,
        client: u64,
        callback: OAuthCallback,
    ) -> Result<(), McpRuntimeError> {
        let entry = self
            .inner
            .entries
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| McpRuntimeError::failed("OAuth flow no longer exists"))?;
        {
            let active = entry
                .active_flow
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !active.as_ref().is_some_and(|operation| {
                operation.id == callback.flow_id
                    && operation.client == client
                    && operation.callback_ready
                    && !operation.callback_accepted
            }) {
                return Err(McpRuntimeError::failed("OAuth callback rejected"));
            }
        }
        let mut data = entry.data.lock().await;
        if entry.cancellation.is_cancelled()
            || data.intent.as_ref().map(|i| i.client) != Some(client)
        {
            return Err(McpRuntimeError::failed(
                "OAuth flow no longer belongs to this client",
            ));
        }
        let flow = data
            .flow
            .as_mut()
            .ok_or_else(|| McpRuntimeError::failed("OAuth flow no longer exists"))?;
        if flow.id != callback.flow_id
            || flow.state != callback.state
            || flow.consumed
            || tokio::time::Instant::now() >= flow.deadline
            || self.inner.options.clock.now() >= flow.wall_deadline
        {
            return Err(McpRuntimeError::failed("OAuth callback rejected"));
        }
        // Invalid issuer does not consume a valid flow. rmcp validates it again.
        let stored = data
            .states
            .load(&callback.state)
            .await
            .map_err(|e| oauth_runtime_error(&e))?
            .ok_or_else(|| McpRuntimeError::failed("OAuth state unavailable"))?;
        if let Some(issuer) = &callback.issuer {
            if stored.expected_issuer.as_ref() != Some(issuer) {
                return Err(McpRuntimeError::failed("OAuth callback issuer mismatch"));
            }
        }
        if stored.require_issuer && callback.issuer.is_none() {
            return Err(McpRuntimeError::failed("OAuth callback issuer missing"));
        }
        if callback.error.is_some() {
            data.states
                .delete(&callback.state)
                .await
                .map_err(|e| oauth_runtime_error(&e))?;
            data.status = OAuthState::Denied;
            self.event(
                id,
                &entry,
                &data,
                None,
                Some("OAuth consent declined".into()),
            )
            .await;
            data.flow = None;
            data.intent = None;
            *entry
                .active_flow
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            return Ok(());
        }
        let code = callback
            .code
            .ok_or_else(|| McpRuntimeError::failed("OAuth callback missing code"))?;
        data.flow.as_mut().unwrap().consumed = true;
        if let Some(operation) = entry
            .active_flow
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
        {
            operation.callback_accepted = true;
        }
        let deadline = data.flow.as_ref().unwrap().deadline;
        let wall_deadline = data.flow.as_ref().unwrap().wall_deadline;
        data.status = OAuthState::Exchanging;
        self.event(id, &entry, &data, None, None).await;
        let manager = data.manager.as_ref().unwrap().clone();
        let service = self.clone();
        let id = id.to_owned();
        drop(data);
        self.spawn(async move {
            // Observe the owning task after the inner future and its captured
            // manager/temporary SDK state have actually been destroyed.
            #[cfg(feature = "test-support")]
            let _completed = ExchangeCompleted(
                entry.clone(),
                entry
                    .projection
                    .lock()
                    .unwrap()
                    .operation
                    .clone()
                    .unwrap_or_default(),
            );
            service
                .finish_callback(
                    id,
                    entry,
                    manager,
                    code,
                    callback.state,
                    callback.issuer,
                    deadline,
                    wall_deadline,
                )
                .await;
        });
        Ok(())
    }
    async fn expire_operation(&self, id: &str, entry: &Entry, data: &mut EntryData) {
        entry.cancellation.cancel();
        data.status = OAuthState::TimedOut;
        data.terminal = true;
        self.event(id, entry, data, None, None).await;
        if let Some(flow) = &data.flow {
            data.states.delete(&flow.state).await.ok();
        }
        data.flow = None;
        data.intent = None;
        data.manager = None;
        data.manager_http = None;
        *entry
            .active_flow
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
    async fn finish_callback(
        &self,
        id: String,
        entry: Arc<Entry>,
        manager: OAuthHttpClient,
        code: String,
        state: String,
        issuer: Option<String>,
        deadline: tokio::time::Instant,
        wall_deadline: std::time::SystemTime,
    ) {
        let mut data = entry.data.lock().await;
        if entry.cancellation.is_cancelled() {
            return;
        }
        if !data
            .flow
            .as_ref()
            .is_some_and(|flow| flow.state == state && flow.consumed)
        {
            return;
        }
        if tokio::time::Instant::now() >= deadline
            || self.inner.options.clock.now() >= wall_deadline
        {
            self.expire_operation(&id, &entry, &mut data).await;
            return;
        }
        let mut authorization = tokio::select! {
            _=entry.cancellation.cancelled()=>return,
            _=tokio::time::sleep_until(deadline)=>{ self.expire_operation(&id, &entry, &mut data).await; return; },
            manager=manager.auth_manager.lock()=>manager
        };
        let guard_result = tokio::select! {
            _=entry.cancellation.cancelled()=>return,
            _=tokio::time::sleep_until(deadline)=>{ self.expire_operation(&id, &entry, &mut data).await; return; },
            guard=entry.credentials.acquire_refresh_guard()=>guard
        };
        let refresh_guard = match guard_result {
            Ok(guard) => guard,
            Err(error) => {
                if entry.cancellation.is_cancelled() {
                    return;
                }
                data.status = OAuthState::Failed;
                self.event(
                    &id,
                    &entry,
                    &data,
                    None,
                    Some(oauth_runtime_error(&error).message),
                )
                .await;
                data.states.delete(&state).await.ok();
                data.states = InMemoryStateStore::new();
                data.manager = None;
                data.manager_http = None;
                data.flow = None;
                data.intent = None;
                *entry
                    .active_flow
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                return;
            }
        };
        let previous = match entry.credentials.record().await {
            Ok(record) => record,
            Err(error) => {
                data.status = OAuthState::Failed;
                self.event(
                    &id,
                    &entry,
                    &data,
                    None,
                    Some(oauth_runtime_error(&error).message),
                )
                .await;
                data.states.delete(&state).await.ok();
                data.states = InMemoryStateStore::new();
                data.manager = None;
                data.manager_http = None;
                data.flow = None;
                data.intent = None;
                *entry
                    .active_flow
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                return;
            }
        };
        entry.credentials.stage_consent(
            previous
                .as_ref()
                .and_then(|record| record.credentials.clone()),
        );
        let result = tokio::select! {
            _=entry.cancellation.cancelled()=>Err(AuthError::AuthorizationRequired),
            _=tokio::time::sleep_until(deadline)=>{
                entry.cancellation.cancel();
                Err(AuthError::AuthorizationRequired)
            },
            result=async {
                authorization.set_credential_store(entry.credentials.with_deadline(wall_deadline, self.inner.options.clock.clone()));
                authorization.exchange_code_for_token_with_issuer(&code,&state,issuer.as_deref()).await
            }.with_subscriber(tracing::subscriber::NoSubscriber::default())=>result,
        };
        authorization.set_credential_store(entry.credentials.clone());
        // Tests can observe the completed exchange before the terminal decision
        // without blocking a synchronous clock call under an admission mutex.
        #[cfg(feature = "test-support")]
        if let Some(hooks) = &self.inner.options.test_hooks {
            if hooks
                .pause_after_exchange
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                hooks.exchange_returned.notify_one();
                hooks.decide_exchange.notified().await;
            }
        }
        // This mutex is the linearization point shared with Cancel. Close
        // admission before releasing the refresh lease or performing any await.
        // A cancellation that won remains recorded until the exchange drains.
        // Read the injectable wall clock outside admission as well: observing
        // time never owns the decision lock. Check monotonic time inside it.
        let observed_wall_time = self.inner.options.clock.now();
        let mut decision = {
            let mut active = entry
                .active_flow
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let decision = active
                .as_ref()
                .and_then(|flow| flow.terminal_decision)
                .unwrap_or_else(|| {
                    if tokio::time::Instant::now() >= deadline
                        || observed_wall_time >= wall_deadline
                    {
                        OAuthState::TimedOut
                    } else if entry.cancellation.is_cancelled() {
                        OAuthState::Cancelled
                    } else if result.is_ok() {
                        OAuthState::Authorized
                    } else {
                        OAuthState::Failed
                    }
                });
            if let Some(active) = active.as_mut() {
                active.terminal_decision = Some(decision);
                if decision == OAuthState::Authorized {
                    *entry
                        .projection
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        ObservableProjection {
                            revision: data.revision,
                            state: OAuthState::Resolving,
                            operation: Some(active.id.clone()),
                        };
                }
            }
            decision
        };
        if decision == OAuthState::Authorized {
            #[cfg(feature = "test-support")]
            if let Some(hooks) = &self.inner.options.test_hooks {
                if hooks
                    .pause_after_winner
                    .swap(false, std::sync::atomic::Ordering::SeqCst)
                {
                    hooks.winner_reserved.notify_one();
                    hooks.publish_resolution.notified().await;
                }
            }
            data.status = OAuthState::Resolving;
            self.event(&id, &entry, &data, None, None).await;
        }
        let commit_failed =
            decision == OAuthState::Authorized && entry.credentials.commit_consent().await.is_err();
        if commit_failed {
            // Retirement while an acknowledged promotion's fence cleanup is
            // uncertain must not publish Failed for a possibly committed grant.
            if entry.cancellation.is_cancelled() {
                data.states = InMemoryStateStore::new();
                data.flow = None;
                data.intent = None;
                data.manager = None;
                data.manager_http = None;
                *entry
                    .active_flow
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                return;
            }
            decision = OAuthState::Failed;
        }
        if matches!(decision, OAuthState::Cancelled | OAuthState::TimedOut) {
            // The blocking writer owns its lease until actual completion.
            // Restore the previous atomic record while our refresh lease still
            // excludes every refresh/replacement, including scope upgrades.
            self.inner.persistence.drain().await;
            if let Some(previous) = previous {
                if entry.credentials.restore_record(previous).await.is_err() {
                    data.status = OAuthState::Failed;
                    self.event(
                        &id,
                        &entry,
                        &data,
                        None,
                        Some("OAuth storage rollback failed".into()),
                    )
                    .await;
                    data.manager = None;
                    data.manager_http = None;
                    data.states = InMemoryStateStore::new();
                    data.flow = None;
                    data.intent = None;
                    *entry
                        .active_flow
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                    return;
                }
            }
        }
        if decision == OAuthState::Authorized && entry.cancellation.is_cancelled() {
            data.states = InMemoryStateStore::new();
            data.flow = None;
            data.intent = None;
            data.manager = None;
            data.manager_http = None;
            *entry
                .active_flow
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            return;
        }
        *entry
            .active_flow
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        data.status = decision;
        drop(refresh_guard);
        drop(authorization);
        data.states.delete(&state).await.ok();
        data.terminal = decision != OAuthState::Authorized;
        if data.status != OAuthState::Authorized {
            // The SDK may cache a token before its store reports failure. Do
            // not offer an unacknowledged/unsaved grant to a new MCP session.
            data.manager = None;
            data.manager_http = None;
        }
        data.next_attempt = tokio::time::Instant::now();
        self.event(
            &id,
            &entry,
            &data,
            None,
            if commit_failed {
                Some("OAuth credential commit failed".into())
            } else {
                result
                    .as_ref()
                    .err()
                    .map(|e| oauth_runtime_error(e).message)
            },
        )
        .await;
        data.flow = None;
        data.intent = None;
    }
    async fn worker(&self, id: String, entry: Arc<Entry>) {
        let mut retry = 0u32;
        loop {
            tokio::select! {_ =entry.cancellation.cancelled()=>return,_=tokio::time::sleep(self.inner.options.poll_interval)=>{}}
            let mut data = entry.data.lock().await;
            // A poll may have won select before retirement and waited behind
            // the exchange owner. Never restore a manager/projection afterwards.
            if entry.cancellation.is_cancelled() {
                return;
            }
            // The operation monitor owns expiry/client-disappearance for
            // Preparing, AwaitingCallback and Exchanging alike. Do not retire
            // a flow here through a second, partially cleaning cancellation path.
            if data.flow.is_some() {
                continue;
            }
            let needs_flow = data
                .intent
                .as_ref()
                .is_some_and(|intent| intent.explicit_consent || data.challenge.is_some())
                && !data.terminal;
            if needs_flow {
                drop(data);
                self.prepare_flow(&id, &entry).await;
                continue;
            }
            if data.terminal || tokio::time::Instant::now() < data.next_attempt {
                continue;
            }
            let restored = tokio::select! {_=entry.cancellation.cancelled()=>return,result=self.restore(&id,&entry,&mut data)=>result};
            let manager = match restored {
                Ok(Some(c)) => c,
                Ok(None) => continue,
                Err(error) => {
                    retry += 1;
                    data.next_attempt = tokio::time::Instant::now()
                        + Duration::from_secs(
                            (1u64 << retry.min(6))
                                + u64::from(uuid::Uuid::new_v4().as_bytes()[0]) % 5,
                        );
                    data.revision = data.revision.wrapping_add(1);
                    let runtime = oauth_runtime_error(&error);
                    data.status = if runtime.state == pioneer_mcp::McpRuntimeState::AuthRequired {
                        OAuthState::AuthRequired
                    } else {
                        OAuthState::Failed
                    };
                    data.terminal = data.status == OAuthState::AuthRequired;
                    self.event(
                        &id,
                        &entry,
                        &data,
                        None,
                        Some(oauth_runtime_error(&error).message),
                    )
                    .await;
                    continue;
                }
            };
            let recovering = data.refresh_recovery_pending;
            let result = tokio::select! {_=entry.cancellation.cancelled()=>return,result=async {
                if recovering { manager.auth_manager.lock().await.refresh_token().await.map(|_| String::new()) }
                else { manager.get_access_token().await }
            }.with_subscriber(tracing::subscriber::NoSubscriber::default())=>result};
            match result {
                Ok(_) => {
                    retry = 0;
                    data.refresh_recovery_pending = false;
                    let recovered = data.status == OAuthState::Failed;
                    data.status = OAuthState::Authorized;
                    if recovered {
                        data.status = OAuthState::Recovered;
                        self.event(&id, &entry, &data, None, None).await;
                        data.status = OAuthState::Authorized;
                    }
                    entry
                        .projection
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .state = OAuthState::Authorized;
                }
                Err(error) => {
                    data.revision = data.revision.wrapping_add(1);
                    let runtime = oauth_runtime_error(&error);
                    data.status = if runtime.state == pioneer_mcp::McpRuntimeState::AuthRequired {
                        OAuthState::AuthRequired
                    } else {
                        OAuthState::Failed
                    };
                    data.terminal = data.status == OAuthState::AuthRequired;
                    retry += 1;
                    data.next_attempt = tokio::time::Instant::now()
                        + Duration::from_secs(
                            (1u64 << retry.min(6))
                                + u64::from(uuid::Uuid::new_v4().as_bytes()[0]) % 5,
                        );
                    self.event(&id, &entry, &data, None, Some(runtime.message))
                        .await;
                }
            }
        }
    }
    /// Configuration-only identity fence, excluding token rotation and timeout.
    pub fn same_configuration(a: &McpServerInstallation, b: &McpServerInstallation) -> bool {
        identity(a, None) == identity(b, None)
    }
    /// Callback/Cancel admission never restores, replaces or writes a binding.
    pub async fn bound_to_installation(
        &self,
        id: &str,
        installation: &McpServerInstallation,
    ) -> bool {
        self.inner
            .entries
            .lock()
            .await
            .get(id)
            .is_some_and(|entry| Self::same_configuration(&entry.installation, installation))
    }
    /// Safe, non-blocking management projection; no token or keystore read.
    pub async fn cleanup_available(&self, id: &str) -> bool {
        let entry = self.inner.entries.lock().await.get(id).cloned();
        entry.is_some_and(|entry| {
            entry
                .cleanup_available
                .load(std::sync::atomic::Ordering::Acquire)
                || entry
                    .projection
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .state
                    == OAuthState::CleanupRequired
        })
    }
    /// Snapshot both management facts from the same current configuration.
    /// Reading this never waits for consent, exchange or persistent storage.
    pub async fn management_projection(
        &self,
        id: &str,
        installation: &McpServerInstallation,
    ) -> (Option<OAuthState>, bool) {
        let Some(entry) = self.inner.entries.lock().await.get(id).cloned() else {
            return (None, false);
        };
        if !Self::same_configuration(&entry.installation, installation) {
            return (None, false);
        }
        let status = entry
            .projection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .state;
        let cleanup = entry
            .cleanup_available
            .load(std::sync::atomic::Ordering::Acquire)
            || status == OAuthState::CleanupRequired;
        let state = if status == OAuthState::Idle
            || (entry.installation.transport.has_authorization_header()
                && status != OAuthState::CleanupRequired)
        {
            None
        } else {
            Some(status)
        };
        (state, cleanup)
    }
    pub async fn state(&self, id: &str) -> Option<OAuthState> {
        let entry = self.inner.entries.lock().await.get(id).cloned()?;
        let status = entry
            .projection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .state;
        if entry.installation.transport.has_authorization_header()
            && status != OAuthState::CleanupRequired
        {
            return None;
        }
        if status != OAuthState::Idle {
            return Some(status);
        }
        None
    }
    pub async fn event_is_current(
        &self,
        event: &OAuthEvent,
        installation: &McpServerInstallation,
    ) -> bool {
        // Terminal retirement events carry no effects or secrets. They release
        // the old operation's shell relay even after durable identity replacement.
        // Consumer revalidates installation UUID/rights; client fences flow UUID.
        if matches!(
            event.state,
            OAuthState::Cancelled | OAuthState::TimedOut | OAuthState::Failed | OAuthState::Retired
        ) && event.flow_id.is_some()
            && event.authorization_url.is_none()
        {
            return self
                .inner
                .retired_events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .any(|retired| {
                    retired.installation_id == event.installation_id
                        && retired.generation == event.generation
                        && retired.revision == event.revision
                        && retired.identity == event.identity
                        && retired.flow_id == event.flow_id
                        && retired.client_id == event.client_id
                        && retired.workspace_id == event.workspace_id
                        && retired.state == event.state
                });
        }
        if !self
            .connection_identity(installation)
            .await
            .is_ok_and(|identity| event.identity == identity)
        {
            return false;
        }
        let entry = self
            .inner
            .entries
            .lock()
            .await
            .get(&event.installation_id)
            .cloned();
        let Some(entry) = entry else {
            return matches!(event.state, OAuthState::Cancelled | OAuthState::TimedOut);
        };
        if entry.identity != event.identity || entry.generation != event.generation {
            return false;
        }
        if matches!(event.state, OAuthState::Cancelled | OAuthState::TimedOut) {
            return true;
        }
        if entry.cancellation.is_cancelled()
            && !matches!(
                event.state,
                OAuthState::Resolving | OAuthState::CleanupRequired
            )
        {
            return false;
        }
        let projection = entry
            .projection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let status = projection.state;
        if event.revision != projection.revision || event.flow_id != projection.operation {
            return false;
        }
        if matches!(
            event.state,
            OAuthState::Preparing | OAuthState::AwaitingCallback | OAuthState::Exchanging
        ) {
            // The injectable clock may block; it must not own Cancel admission.
            let observed_wall_time = self.inner.options.clock.now();
            let active = entry
                .active_flow
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            return status == event.state
                && active.as_ref().is_some_and(|flow| {
                    event.flow_id.as_deref() == Some(flow.id.as_str())
                        && event.client_id == Some(flow.client)
                        && flow.terminal_decision.is_none()
                        && flow.deadline > tokio::time::Instant::now()
                        && flow.wall_deadline > observed_wall_time
                        && (event.state != OAuthState::AwaitingCallback
                            || (flow.callback_ready && !flow.callback_accepted))
                });
        }
        status == event.state
            || (event.state == OAuthState::Recovered && status == OAuthState::Authorized)
    }

    pub async fn synchronize(
        &self,
        id: &str,
        installation: &McpServerInstallation,
    ) -> Result<(), McpRuntimeError> {
        if !matches!(
            installation.transport,
            McpTransportConfig::StreamableHttp { .. }
        ) {
            let preserve_cleanup = {
                let _caller = self
                    .inner
                    .callers
                    .enter()
                    .map_err(|e| oauth_runtime_error(&e))?;
                let _binding = self
                    .binding_guard(id)
                    .await
                    .map_err(|e| oauth_runtime_error(&e))?;
                let entry = self.inner.entries.lock().await.get(id).cloned();
                entry.is_some_and(|entry| {
                    Self::same_configuration(&entry.installation, installation)
                        && entry
                            .projection
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .state
                            == OAuthState::CleanupRequired
                })
            };
            if preserve_cleanup {
                return Ok(());
            }
            return self.disconnect(id).await;
        }
        self.bind_inner(id, installation, true)
            .await
            .map(|_| ())
            .map_err(|e| oauth_runtime_error(&e))
    }
    pub async fn suspend(&self, id: &str) -> Result<(), McpRuntimeError> {
        let _caller = self
            .inner
            .callers
            .enter()
            .map_err(|e| oauth_runtime_error(&e))?;
        let _binding = self
            .binding_guard(id)
            .await
            .map_err(|e| oauth_runtime_error(&e))?;
        let existing = { self.inner.entries.lock().await.get(id).cloned() };
        if existing.as_ref().is_some_and(|entry| {
            entry
                .projection
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .state
                == OAuthState::CleanupRequired
        }) {
            // Already cancelled/drained. Disable stops the runtime in Gateway,
            // but must not retire the only presentation for retrying cleanup.
            return Ok(());
        }
        let retired = { self.inner.entries.lock().await.remove(id) };
        if let Some(entry) = retired {
            entry.cancellation.cancel();
            let mut data = entry.data.lock().await;
            if let Some(flow) = &data.flow {
                data.states
                    .delete(&flow.state)
                    .await
                    .map_err(|e| oauth_runtime_error(&e))?;
            }
            self.retire_presentation(id, &entry, &data).await;
            if data.status != OAuthState::Resolving {
                data.status = OAuthState::Cancelled;
                self.event(id, &entry, &data, None, None).await;
            }
            data.flow = None;
            data.intent = None;
        }
        Ok(())
    }
    pub async fn cancel(
        &self,
        id: &str,
        client: u64,
        flow_id: &str,
    ) -> Result<(), McpRuntimeError> {
        self.cancel_operation(id, client, flow_id, OAuthState::Cancelled)
            .await
    }
    async fn cancel_operation(
        &self,
        id: &str,
        client: u64,
        flow_id: &str,
        terminal_state: OAuthState,
    ) -> Result<(), McpRuntimeError> {
        let entry = self
            .inner
            .entries
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| McpRuntimeError::failed("OAuth flow unavailable"))?;
        let observed_wall_time = self.inner.options.clock.now();
        let (workspace, terminal_state) = {
            let mut active = entry
                .active_flow
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if active.as_ref().map(|flow| (flow.id.as_str(), flow.client))
                != Some((flow_id, client))
            {
                return Err(McpRuntimeError::failed("OAuth flow unavailable"));
            }
            // Interrupt an in-flight code exchange before waiting for its actor.
            // The credential adapter also fences late blocking writes.
            let active = active.as_mut().unwrap();
            if active.terminal_decision.is_some() {
                return Err(McpRuntimeError::failed("OAuth operation already completed"));
            }
            let terminal_state = if tokio::time::Instant::now() >= active.deadline
                || observed_wall_time >= active.wall_deadline
            {
                OAuthState::TimedOut
            } else {
                terminal_state
            };
            active.terminal_decision = Some(terminal_state);
            entry.cancellation.cancel();
            (active.workspace.clone(), terminal_state)
        };
        let service = self.clone();
        let id = id.to_owned();
        let flow_id = flow_id.to_owned();
        self.spawn(async move {
            let mut data = entry.data.lock().await;
            if data.intent.is_none() {
                // The exchange/preparation owner has already reported its final
                // decision (including rollback failure). Never overwrite it.
                service.inner.persistence.drain().await;
                return;
            }
            if let Some(flow) = &data.flow {
                data.states.delete(&flow.state).await.ok();
            }
            data.status = terminal_state;
            *entry
                .active_flow
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            *entry
                .projection
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = ObservableProjection {
                revision: data.revision,
                state: terminal_state,
                operation: Some(flow_id.clone()),
            };
            // Exchange cancellation may already have cleared its actor state. Keep
            // the initiating client and flow envelope captured before interrupting it.
            service
                .emit_event(OAuthEvent {
                    installation_id: id.clone(),
                    workspace_id: workspace,
                    scope_kind: entry.installation.scope_kind.as_str().into(),
                    scope_key: entry.installation.scope_key.clone(),
                    identity: entry.identity.clone(),
                    generation: entry.generation.clone(),
                    revision: data.revision,
                    client_id: Some(client),
                    flow_id: Some(flow_id.clone()),
                    state: terminal_state,
                    authorization_url: None,
                    diagnostic: None,
                })
                .await;
            data.flow = None;
            data.intent = None;
            data.manager = None;
            data.states = InMemoryStateStore::new();
            service.inner.persistence.drain().await;
        });
        Ok(())
    }
    pub async fn disconnect(&self, id: &str) -> Result<(), McpRuntimeError> {
        self.disconnect_inner(id, None).await
    }
    /// Management admission supplies the durable configuration and recipient.
    /// It also retains a retry projection when no runtime entry exists yet.
    pub async fn disconnect_managed(
        &self,
        id: &str,
        installation: &McpServerInstallation,
        client: u64,
        workspace: &str,
    ) -> Result<(), McpRuntimeError> {
        self.disconnect_inner(id, Some((installation, client, workspace)))
            .await
    }
    async fn disconnect_inner(
        &self,
        id: &str,
        management: Option<(&McpServerInstallation, u64, &str)>,
    ) -> Result<(), McpRuntimeError> {
        let _caller = self
            .inner
            .callers
            .enter()
            .map_err(|e| oauth_runtime_error(&e))?;
        let _binding = self
            .binding_guard(id)
            .await
            .map_err(|e| oauth_runtime_error(&e))?;
        let mut entry = { self.inner.entries.lock().await.get(id).cloned() };
        if let Some((installation, _, _)) = management {
            if entry
                .as_ref()
                .is_some_and(|entry| !Self::same_configuration(&entry.installation, installation))
            {
                return Err(McpRuntimeError::failed("MCP installation was replaced"));
            }
            if entry.is_none() {
                // No restore, registration or token read is needed to clear an account.
                let hash = self
                    .connection_identity(installation)
                    .await
                    .unwrap_or_else(|_| identity(installation, None));
                let fresh = self.new_entry(id, installation, hash);
                fresh.cancellation.cancel();
                let mut entries = self.inner.entries.lock().await;
                if self.inner.shutdown.is_cancelled() {
                    return Err(McpRuntimeError::failed("OAuth service is shutting down"));
                }
                entries.insert(id.into(), fresh.clone());
                drop(entries);
                entry = Some(fresh);
            }
        }
        let Some(entry) = entry else {
            return self
                .inner
                .persistence
                .delete(id)
                .await
                .map_err(|e| oauth_runtime_error(&e));
        };
        entry.cancellation.cancel();
        let mut data = entry.data.lock().await;
        // Retirement ends the old consent/presentation, not management cleanup.
        self.retire_presentation(id, &entry, &data).await;
        if !matches!(
            data.status,
            OAuthState::Resolving | OAuthState::CleanupRequired
        ) {
            data.status = OAuthState::Cancelled;
            self.event(id, &entry, &data, None, None).await;
        }
        data.states = InMemoryStateStore::new();
        data.flow = None;
        data.intent = None;
        data.manager = None;
        data.manager_http = None;
        *entry
            .active_flow
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        let _guard = entry.credentials.gate.lock().await;
        // delete owns the cross-process lease and actual blocking work. A failed
        // or uncertain delete never makes this cancelled entry usable again.
        if let Err(error) = self.inner.persistence.delete(id).await {
            data.status = OAuthState::CleanupRequired;
            data.revision += 1;
            let owner = management
                .map(|(_, client, workspace)| (client, workspace.to_owned()))
                .or_else(|| {
                    entry
                        .presentation_owner
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .as_ref()
                        .map(|(_, client, workspace)| (*client, workspace.clone()))
                });
            let flow = owner.as_ref().map(|_| uuid::Uuid::new_v4().to_string());
            *entry
                .presentation_owner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = owner
                .as_ref()
                .zip(flow.as_ref())
                .map(|((client, workspace), flow)| (flow.clone(), *client, workspace.clone()));
            *entry
                .projection
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = ObservableProjection {
                revision: data.revision,
                state: OAuthState::CleanupRequired,
                operation: flow.clone(),
            };
            self.emit_event(OAuthEvent {
                installation_id: id.into(),
                workspace_id: owner
                    .as_ref()
                    .map(|(_, workspace)| workspace.clone())
                    .unwrap_or_else(|| entry.installation.scope_key.clone()),
                scope_kind: entry.installation.scope_kind.as_str().into(),
                scope_key: entry.installation.scope_key.clone(),
                identity: entry.identity.clone(),
                generation: entry.generation.clone(),
                revision: data.revision,
                client_id: owner.map(|(client, _)| client),
                flow_id: flow,
                state: OAuthState::CleanupRequired,
                authorization_url: None,
                diagnostic: None,
            })
            .await;
            return Err(oauth_runtime_error(&error));
        }
        // Per-ID admission excludes replacement until deletion and projection
        // retirement finish. The registry is held only for the final replacement/removal.
        if management.is_some() {
            // Clear completed durably. Keep a fresh, credential-free admission
            // for this installation so an automatic workspace reload cannot
            // anonymously reconnect while the user is signed out.
            let fresh = self.new_entry(id, &entry.installation, entry.identity.clone());
            {
                let mut fresh_data = fresh.data.lock().await;
                fresh_data.status = OAuthState::AuthRequired;
                fresh_data.terminal = true;
            }
            fresh
                .projection
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .state = OAuthState::AuthRequired;
            {
                let mut entries = self.inner.entries.lock().await;
                if self.inner.shutdown.is_cancelled() {
                    return Err(McpRuntimeError::failed("OAuth service is shutting down"));
                }
                entries.insert(id.into(), fresh.clone());
            }
            let service = self.clone();
            let id = id.to_owned();
            self.spawn(async move {
                service.worker(id, fresh).await;
            });
        } else {
            self.inner.entries.lock().await.remove(id);
        }
        Ok(())
    }
    pub async fn garbage_collect(
        &self,
        active: &std::collections::HashSet<String>,
    ) -> Result<(), McpRuntimeError> {
        let _caller = self
            .inner
            .callers
            .enter()
            .map_err(|e| oauth_runtime_error(&e))?;
        let stored = self
            .inner
            .persistence
            .ids()
            .await
            .map_err(|e| oauth_runtime_error(&e))?;
        let entries = self
            .inner
            .entries
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for id in stored.into_iter().chain(entries) {
            if !active.contains(&id) {
                self.disconnect(&id).await?;
            }
        }
        Ok(())
    }
    pub async fn shutdown(&self) {
        let _shutdown_owner = self.inner.shutdown_gate.lock().await;
        self.inner.callers.close();
        self.inner.shutdown.cancel();
        // External runtime connects/cleanup may still hold accepted permits.
        // No registry or database capacity is held while they finish or drop.
        self.inner.callers.drained().await;
        // A task already handling an expiry may enqueue its owned cleanup
        // after shutdown starts. Drain successive batches, not one snapshot.
        loop {
            let tasks = std::mem::take(
                &mut *self
                    .inner
                    .tasks
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
            if tasks.is_empty() {
                break;
            }
            for task in tasks {
                let _ = task.await;
            }
        }
        self.inner.persistence.drain().await;
        self.inner.entries.lock().await.clear();
    }
    async fn challenge_entry(
        &self,
        id: &str,
        entry: Arc<Entry>,
        session: Option<(&McpServerInstallation, Option<&OAuthHttpClient>)>,
        challenge: &str,
        insufficient_scope: bool,
    ) {
        {
            let mut data = entry.data.lock().await;
            if let Some((installation, client)) = session {
                if resource(&entry.installation).ok() != resource(installation).ok()
                    || entry.installation.auth != installation.auth
                    || entry.installation.transport.has_authorization_header()
                        != installation.transport.has_authorization_header()
                {
                    return;
                }
                match (client, data.manager.as_ref()) {
                    (Some(old), Some(current))
                        if Arc::ptr_eq(&old.auth_manager, &current.auth_manager) => {}
                    (Some(_), _) => return,
                    // An anonymous session predates this manager. Its delayed
                    // challenge cannot revoke the current grant, even if storage
                    // is temporarily unavailable.
                    (None, Some(_)) => return,
                    _ => {}
                }
            }
            if data.flow.is_some() || entry.cancellation.is_cancelled() {
                return;
            }
            data.revision = data.revision.wrapping_add(1);
            data.challenge = Some(challenge.into());
            data.status = if insufficient_scope {
                OAuthState::InsufficientScope
            } else {
                OAuthState::AuthRequired
            };
            if data.intent.is_none() {
                data.terminal = true;
            }
            self.event(id, &entry, &data, None, None).await;
        }
        let service = self.clone();
        let id = id.to_owned();
        self.spawn(async move {
            service.prepare_flow(&id, &entry).await;
        });
    }
}
use rmcp::transport::auth::CredentialStore;
#[async_trait]
impl McpOAuthProvider for McpOAuthService {
    async fn connection_established(
        &self,
        id: &str,
        installation: &McpServerInstallation,
        authorized: bool,
    ) {
        if authorized {
            return;
        }
        let Some(entry) = self.inner.entries.lock().await.get(id).cloned() else {
            return;
        };
        if entry.cancellation.is_cancelled()
            || resource(installation).ok() != resource(&entry.installation).ok()
            || installation.auth != entry.installation.auth
        {
            return;
        }
        let operation = {
            let active = entry
                .active_flow
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(active) = active
                .as_ref()
                .filter(|active| !active.started && active.terminal_decision.is_none())
            else {
                return;
            };
            active.id.clone()
        };
        // A background credential read may own the entry temporarily. Retiring
        // an unused install must not be lost merely because that read is busy.
        let mut data = tokio::select! {
            _ = entry.cancellation.cancelled() => return,
            data = entry.data.lock() => data,
        };
        let mut active = entry
            .active_flow
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !entry.cancellation.is_cancelled()
            && data.status == OAuthState::Idle
            && data.manager.is_none()
            && data.flow.is_none()
            && active.as_ref().is_some_and(|active| {
                active.id == operation && !active.started && active.terminal_decision.is_none()
            })
        {
            // A public server completed initialization/catalog discovery. Its
            // unused install intent must not produce a timeout/login later.
            data.intent = None;
            *active = None;
        }
    }

    async fn transient_failure_from_session(
        &self,
        id: &str,
        installation: &McpServerInstallation,
        client: Option<&OAuthHttpClient>,
    ) -> Option<pioneer_mcp::OAuthFailureCause> {
        let Some(entry) = self.inner.entries.lock().await.get(id).cloned() else {
            return None;
        };
        if entry.cancellation.is_cancelled()
            || resource(installation).ok() != resource(&entry.installation).ok()
        {
            return None;
        }
        let mut data = entry.data.lock().await;
        if data.flow.is_some()
            || !matches!((&data.manager, client), (Some(current), Some(client)) if Arc::ptr_eq(&current.auth_manager, &client.auth_manager))
        {
            return None;
        }
        data.status = OAuthState::Failed;
        data.terminal = false;
        data.revision = data.revision.wrapping_add(1);
        data.refresh_recovery_pending = true;
        self.event(
            id,
            &entry,
            &data,
            None,
            Some("OAuth provider temporarily unavailable".into()),
        )
        .await;
        Some(pioneer_mcp::OAuthFailureCause {
            generation: entry.generation.clone(),
            revision: data.revision,
        })
    }

    async fn client(
        &self,
        id: &str,
        installation: &McpServerInstallation,
    ) -> Result<Option<OAuthHttpClient>, McpRuntimeError> {
        let _caller = self
            .inner
            .callers
            .enter()
            .map_err(|e| oauth_runtime_error(&e))?;
        if let McpTransportConfig::StreamableHttp { headers, .. } = &installation.transport {
            if headers
                .keys()
                .any(|k| k.eq_ignore_ascii_case("authorization"))
            {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }
        let entry = self
            .bind(id, installation)
            .await
            .map_err(|e| oauth_runtime_error(&e))?;
        let mut data = entry.data.lock().await;
        if data.terminal
            && data.flow.is_none()
            && matches!(
                data.status,
                OAuthState::AuthRequired | OAuthState::InsufficientScope
            )
        {
            return Err(McpRuntimeError::auth_required("OAuth sign-in required"));
        }
        // A new MCP session must verify discovery again even when reusing its
        // shared manager. An issuer replacement cannot inherit the old grant.
        if let Some(client) = &data.manager {
            let result = async {
                let record = entry
                    .credentials
                    .record()
                    .await?
                    .ok_or(AuthError::AuthorizationRequired)?;
                let mut manager = client.auth_manager.lock().await;
                let resolution = manager
                    .resolve_metadata_from_challenge(data.challenge.as_deref())
                    .await?;
                if !resolution.source.is_discovered() {
                    return Err(AuthError::NoAuthorizationSupport);
                }
                if resolution.metadata.issuer.as_deref() != Some(record.issuer.as_str()) {
                    return Err(AuthError::AuthorizationRequired);
                }
                data.manager_http
                    .as_ref()
                    .ok_or_else(|| {
                        AuthError::InternalError("OAuth HTTP configuration unavailable".into())
                    })?
                    .configure_authentication(&record.registration, &resolution.metadata)?;
                manager.set_metadata(resolution.metadata);
                manager.configure_client(record.registration.config())
            }
            .with_subscriber(tracing::subscriber::NoSubscriber::default())
            .await;
            if let Err(error) = result {
                let runtime = oauth_runtime_error(&error);
                if runtime.state == pioneer_mcp::McpRuntimeState::AuthRequired {
                    data.status = OAuthState::AuthRequired;
                    data.terminal = true;
                    self.event(id, &entry, &data, None, Some(runtime.message.clone()))
                        .await;
                }
                return Err(runtime);
            }
        }
        self.restore(id, &entry, &mut data)
            .await
            .map_err(|e| oauth_runtime_error(&e))
    }
    async fn challenge(&self, id: &str, challenge: &str, insufficient_scope: bool) {
        let Some(entry) = self.inner.entries.lock().await.get(id).cloned() else {
            return;
        };
        self.challenge_entry(id, entry, None, challenge, insufficient_scope)
            .await;
    }
    async fn authorization_lost(&self, id: &str) {
        self.challenge(id, "Bearer", false).await;
    }
    async fn challenge_from_session(
        &self,
        id: &str,
        installation: &McpServerInstallation,
        client: Option<&OAuthHttpClient>,
        challenge: &str,
        insufficient_scope: bool,
    ) {
        let Some(entry) = self.inner.entries.lock().await.get(id).cloned() else {
            return;
        };
        self.challenge_entry(
            id,
            entry,
            Some((installation, client)),
            challenge,
            insufficient_scope,
        )
        .await;
    }
}
