#![allow(dead_code)]
// The manager is introduced before Codex turn execution is wired in later WP steps.

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use pioneer_cli_agent_runtime::codex::{
    CodexJsonlRpcClientDiagnostic, CodexJsonlRpcNotificationEvent, CodexJsonlRpcServerRequest,
    CodexThreadOpenSnapshot, CodexThreadStartParams, CodexTurnStartParams, CodexTurnStartSnapshot,
};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
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
    pub native_thread_id: String,
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
    pub app_server_args: Vec<String>,
    pub env: BTreeMap<String, String>,
}

pub(crate) struct CLIAgentRuntimeCodexEventReceivers {
    pub notifications: mpsc::Receiver<CodexJsonlRpcNotificationEvent>,
    pub server_requests: mpsc::Receiver<CodexJsonlRpcServerRequest>,
    pub diagnostics: mpsc::Receiver<CodexJsonlRpcClientDiagnostic>,
}

#[async_trait]
pub(crate) trait CLIAgentRuntimeSession: Send + Sync {
    async fn close(&self) -> Result<()>;

    fn take_codex_event_receivers(&self) -> Option<CLIAgentRuntimeCodexEventReceivers> {
        None
    }

    async fn start_codex_thread(
        &self,
        params: CodexThreadStartParams,
        timeout: Duration,
    ) -> Result<CodexThreadOpenSnapshot> {
        let _ = (params, timeout);
        bail!("CLI runtime session does not support Codex thread start");
    }

    async fn resume_codex_thread(
        &self,
        native_thread_id: &str,
        params: CodexThreadStartParams,
        timeout: Duration,
    ) -> Result<CodexThreadOpenSnapshot> {
        let _ = (native_thread_id, params, timeout);
        bail!("CLI runtime session does not support Codex thread resume");
    }

    async fn start_codex_turn(
        &self,
        params: CodexTurnStartParams,
        timeout: Duration,
    ) -> Result<CodexTurnStartSnapshot> {
        let _ = (params, timeout);
        bail!("CLI runtime session does not support Codex turn start");
    }

    async fn respond_to_request(
        &self,
        native_request_id: JsonValue,
        response: JsonValue,
    ) -> Result<()> {
        let _ = (native_request_id, response);
        bail!("CLI runtime session does not support server request responses");
    }

    async fn interrupt_turn(
        &self,
        native_thread_id: Option<&str>,
        native_turn_id: Option<&str>,
    ) -> Result<()> {
        let _ = (native_thread_id, native_turn_id);
        bail!("CLI runtime session does not support turn interrupt");
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
    async fn start_session(
        &self,
        key: &CLIAgentRuntimeSessionKey,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>>;

    async fn start_session_with_options(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        options: &CLIAgentRuntimeSessionStartOptions,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
        let _ = options;
        self.start_session(key).await
    }
}

#[derive(Clone)]
pub(crate) struct CLIAgentRuntimeSessionHandle {
    key: CLIAgentRuntimeSessionKey,
    session: Arc<dyn CLIAgentRuntimeSession>,
}

impl CLIAgentRuntimeSessionHandle {
    pub(crate) fn key(&self) -> &CLIAgentRuntimeSessionKey {
        &self.key
    }

    pub(crate) fn session(&self) -> Arc<dyn CLIAgentRuntimeSession> {
        self.session.clone()
    }

    #[cfg(test)]
    fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.session, &other.session)
    }
}

struct CLIAgentRuntimeCachedSession {
    session: Arc<dyn CLIAgentRuntimeSession>,
    start_options: CLIAgentRuntimeSessionStartOptions,
    started_at_ms: u64,
    last_used_at_ms: u64,
}

impl CLIAgentRuntimeCachedSession {
    fn handle(&self, key: &CLIAgentRuntimeSessionKey) -> CLIAgentRuntimeSessionHandle {
        CLIAgentRuntimeSessionHandle {
            key: key.clone(),
            session: self.session.clone(),
        }
    }
}

pub(crate) struct CLIAgentRuntimeManager {
    factory: Arc<dyn CLIAgentRuntimeSessionFactory>,
    idle_session_ttl: Duration,
    sessions: Mutex<HashMap<CLIAgentRuntimeSessionKey, CLIAgentRuntimeCachedSession>>,
    start_locks: Mutex<HashMap<CLIAgentRuntimeSessionKey, Arc<Mutex<()>>>>,
}

impl CLIAgentRuntimeManager {
    pub(crate) fn new(
        factory: Arc<dyn CLIAgentRuntimeSessionFactory>,
        idle_session_ttl: Duration,
    ) -> Result<Self> {
        if idle_session_ttl.is_zero() {
            bail!("CLI runtime idle session TTL must be greater than zero");
        }
        Ok(Self {
            factory,
            idle_session_ttl,
            sessions: Mutex::new(HashMap::new()),
            start_locks: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) async fn get_or_start(
        &self,
        key: CLIAgentRuntimeSessionKey,
    ) -> Result<CLIAgentRuntimeSessionHandle> {
        self.get_or_start_with_options(key, CLIAgentRuntimeSessionStartOptions::default())
            .await
    }

    pub(crate) async fn get_or_start_with_options(
        &self,
        key: CLIAgentRuntimeSessionKey,
        options: CLIAgentRuntimeSessionStartOptions,
    ) -> Result<CLIAgentRuntimeSessionHandle> {
        self.get_or_start_at(key, options, current_time_millis())
            .await
    }

    async fn get_or_start_at(
        &self,
        key: CLIAgentRuntimeSessionKey,
        options: CLIAgentRuntimeSessionStartOptions,
        now_ms: u64,
    ) -> Result<CLIAgentRuntimeSessionHandle> {
        if let Some(handle) = self.touch_existing_session(&key, &options, now_ms).await {
            return Ok(handle);
        }

        let start_lock = self.start_lock_for_key(&key).await;
        let _guard = start_lock.lock().await;

        if let Some(handle) = self.touch_existing_session(&key, &options, now_ms).await {
            return Ok(handle);
        }
        if let Some(stale) = self
            .remove_session_with_different_options(&key, &options)
            .await
        {
            stale.session.close().await.map_err(|error| {
                anyhow!(
                    "failed to close stale CLI runtime session `{}/{}/{}`: {error:#}",
                    key.workspace_id,
                    key.runtime_id,
                    key.thread_id
                )
            })?;
        }

        let session = self
            .factory
            .start_session_with_options(&key, &options)
            .await
            .map_err(|error| anyhow!("failed to start CLI runtime session: {error:#}"))?;
        let handle = CLIAgentRuntimeSessionHandle {
            key: key.clone(),
            session: session.clone(),
        };
        self.sessions.lock().await.insert(
            key,
            CLIAgentRuntimeCachedSession {
                session,
                start_options: options,
                started_at_ms: now_ms,
                last_used_at_ms: now_ms,
            },
        );
        Ok(handle)
    }

    async fn touch_existing_session(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        options: &CLIAgentRuntimeSessionStartOptions,
        now_ms: u64,
    ) -> Option<CLIAgentRuntimeSessionHandle> {
        let mut sessions = self.sessions.lock().await;
        let cached = sessions.get_mut(key)?;
        if &cached.start_options != options {
            return None;
        }
        cached.last_used_at_ms = now_ms;
        Some(cached.handle(key))
    }

    pub(crate) async fn existing_session(
        &self,
        key: &CLIAgentRuntimeSessionKey,
    ) -> Option<CLIAgentRuntimeSessionHandle> {
        self.sessions
            .lock()
            .await
            .get(key)
            .map(|cached| cached.handle(key))
    }

    async fn remove_session_with_different_options(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        options: &CLIAgentRuntimeSessionStartOptions,
    ) -> Option<CLIAgentRuntimeCachedSession> {
        let mut sessions = self.sessions.lock().await;
        let cached = sessions.get(key)?;
        if &cached.start_options == options {
            return None;
        }
        sessions.remove(key)
    }

    async fn start_lock_for_key(&self, key: &CLIAgentRuntimeSessionKey) -> Arc<Mutex<()>> {
        let mut locks = self.start_locks.lock().await;
        locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub(crate) async fn close_idle_sessions(&self) -> Result<usize> {
        self.close_idle_sessions_at(current_time_millis()).await
    }

    async fn close_idle_sessions_at(&self, now_ms: u64) -> Result<usize> {
        let ttl_ms = self.idle_session_ttl.as_millis() as u64;
        let idle_sessions = {
            let mut sessions = self.sessions.lock().await;
            let keys = sessions
                .iter()
                .filter_map(|(key, cached)| {
                    let idle_for_ms = now_ms.saturating_sub(cached.last_used_at_ms);
                    (idle_for_ms >= ttl_ms).then_some(key.clone())
                })
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| sessions.remove(&key).map(|cached| (key, cached)))
                .collect::<Vec<_>>()
        };

        let closed_count = idle_sessions.len();
        for (key, cached) in idle_sessions {
            cached.session.close().await.map_err(|error| {
                anyhow!(
                    "failed to close idle CLI runtime session `{}/{}/{}`: {error:#}",
                    key.workspace_id,
                    key.runtime_id,
                    key.thread_id
                )
            })?;
            self.remove_start_lock(&key).await;
        }
        Ok(closed_count)
    }

    pub(crate) async fn close_session(&self, key: &CLIAgentRuntimeSessionKey) -> Result<bool> {
        let session = self.sessions.lock().await.remove(key);
        let Some(cached) = session else {
            return Ok(false);
        };
        cached.session.close().await.map_err(|error| {
            anyhow!(
                "failed to close CLI runtime session `{}/{}/{}`: {error:#}",
                key.workspace_id,
                key.runtime_id,
                key.thread_id
            )
        })?;
        self.remove_start_lock(key).await;
        Ok(true)
    }

    pub(crate) async fn close_all(&self) -> Result<usize> {
        let sessions = {
            let mut sessions = self.sessions.lock().await;
            sessions.drain().collect::<Vec<_>>()
        };
        self.start_locks.lock().await.clear();

        let closed_count = sessions.len();
        for (key, cached) in sessions {
            cached.session.close().await.map_err(|error| {
                anyhow!(
                    "failed to close CLI runtime session `{}/{}/{}`: {error:#}",
                    key.workspace_id,
                    key.runtime_id,
                    key.thread_id
                )
            })?;
        }
        Ok(closed_count)
    }

    async fn remove_start_lock(&self, key: &CLIAgentRuntimeSessionKey) {
        self.start_locks.lock().await.remove(key);
    }

    #[cfg(test)]
    async fn session_count(&self) -> usize {
        self.sessions.lock().await.len()
    }

    #[cfg(test)]
    async fn cached_started_at_ms(&self, key: &CLIAgentRuntimeSessionKey) -> Option<u64> {
        self.sessions
            .lock()
            .await
            .get(key)
            .map(|cached| cached.started_at_ms)
    }
}

fn normalize_key_part(value: String, label: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("CLI runtime session key `{label}` cannot be empty");
    }
    Ok(trimmed.to_owned())
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
        CLIAgentRuntimeManager, CLIAgentRuntimeSession, CLIAgentRuntimeSessionFactory,
        CLIAgentRuntimeSessionKey, CLIAgentRuntimeSessionStartOptions,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Notify;

    #[derive(Default)]
    struct FakeFactory {
        starts: AtomicUsize,
        closes: Arc<AtomicUsize>,
        release: Option<Arc<Notify>>,
    }

    #[async_trait]
    impl CLIAgentRuntimeSessionFactory for FakeFactory {
        async fn start_session(
            &self,
            _key: &CLIAgentRuntimeSessionKey,
        ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
            let id = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
            if let Some(release) = self.release.as_ref() {
                release.notified().await;
            }
            Ok(Arc::new(FakeSession {
                id,
                closes: self.closes.clone(),
            }))
        }
    }

    struct FakeSession {
        id: usize,
        closes: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl CLIAgentRuntimeSession for FakeSession {
        async fn close(&self) -> Result<()> {
            let _ = self.id;
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn key(thread_id: &str) -> CLIAgentRuntimeSessionKey {
        CLIAgentRuntimeSessionKey::new("ws", "codex", thread_id).expect("valid key")
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
            app_server_args: vec!["-c".to_owned(), "model=\"gpt-5-codex\"".to_owned()],
            env: Default::default(),
        };
        let options_b = CLIAgentRuntimeSessionStartOptions {
            app_server_args: vec!["-c".to_owned(), "model=\"gpt-5\"".to_owned()],
            env: Default::default(),
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
        assert_eq!(factory.starts.load(Ordering::SeqCst), 2);
        assert_eq!(factory.closes.load(Ordering::SeqCst), 1);
        assert_eq!(manager.session_count().await, 1);
    }

    #[tokio::test]
    async fn cli_runtime_manager_concurrent_same_key_starts_once() {
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
