//! Completion ownership for one entry in the existing CLI session registry.
use super::{CLIAgentRuntimeSession, CLIAgentRuntimeSessionLifecycle};
use crate::cli_runtime::session_instance::CliSessionInstanceId;
use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use pioneer_cli_agent_runtime::process::CLIAgentProcess;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

tokio::task_local! { pub(super) static CLI_CALLBACK_INSTANCE: CliSessionInstanceId; }

pub(crate) enum CliStartupProcessCleanup {
    Codex(pioneer_cli_agent_runtime::codex::CodexGenerationOverlayDescriptor),
    Claude(pioneer_cli_agent_runtime::claude::ClaudeManagedMcpConfigDescriptor),
}

#[derive(Default)]
pub(crate) struct CLIAgentRuntimeSessionStartup {
    session: StdMutex<Option<Arc<dyn CLIAgentRuntimeSession>>>,
    cancellation: CancellationToken,
}
impl CLIAgentRuntimeSessionStartup {
    pub(crate) fn check_admission(&self) -> Result<()> {
        if self.cancellation.is_cancelled() {
            bail!("CLI startup was stopped");
        }
        Ok(())
    }
    pub(crate) async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }
    /// Files/grants prepared before spawn are resources too. Retain the
    /// managed descriptor until the actual factory task has ended and the
    /// supervisor has released its references, without inventing a process.
    pub(crate) fn retain_preparation(&self, cleanup: CliStartupProcessCleanup) {
        self.retain_session(Arc::new(StartupProcessSession {
            process: None,
            cleanup: StdMutex::new(Some(cleanup)),
        }));
    }
    /// Synchronous publication immediately after spawn, before the next await.
    pub(crate) fn retain_process(
        &self,
        process: Arc<Mutex<CLIAgentProcess>>,
        cleanup: CliStartupProcessCleanup,
    ) {
        self.retain_session(Arc::new(StartupProcessSession {
            process: Some(process),
            cleanup: StdMutex::new(Some(cleanup)),
        }));
    }
    pub(crate) fn retain_session(&self, session: Arc<dyn CLIAgentRuntimeSession>) {
        *self.session.lock().expect("CLI startup ownership poisoned") = Some(session);
    }
    pub(super) fn session(&self) -> Option<Arc<dyn CLIAgentRuntimeSession>> {
        self.session
            .lock()
            .expect("CLI startup ownership poisoned")
            .clone()
    }
}
struct StartupProcessSession {
    process: Option<Arc<Mutex<CLIAgentProcess>>>,
    cleanup: StdMutex<Option<CliStartupProcessCleanup>>,
}
#[async_trait]
impl CLIAgentRuntimeSession for StartupProcessSession {
    async fn close(&self) -> Result<()> {
        self.stop_and_wait().await?;
        self.cleanup_after_stop().await
    }
    async fn stop_and_wait(&self) -> Result<()> {
        if let Some(process) = &self.process {
            process
                .lock()
                .await
                .terminate_with_grace(Duration::from_secs(2))
                .await?;
        }
        Ok(())
    }
    async fn cleanup_after_stop(&self) -> Result<()> {
        let mut cleanup = self.cleanup.lock().expect("CLI startup cleanup poisoned");
        match cleanup.as_ref() {
            Some(CliStartupProcessCleanup::Codex(descriptor)) => {
                pioneer_cli_agent_runtime::codex::cleanup_codex_generation_overlay(descriptor)
                    .map_err(|e| anyhow!("Codex startup cleanup failed: {e}"))?
            }
            Some(CliStartupProcessCleanup::Claude(descriptor)) => {
                pioneer_cli_agent_runtime::claude::cleanup_claude_managed_mcp_config(descriptor)
                    .map_err(|e| anyhow!("Claude startup cleanup failed: {e}"))?
            }
            None => {}
        }
        cleanup.take();
        Ok(())
    }
}

#[derive(Default)]
struct StartupCompletion {
    task: Option<JoinHandle<Result<(), String>>>,
    outcome: Option<Result<(), String>>,
    panicked: bool,
}
#[derive(Default)]
struct CallbackCompletion {
    tasks: Vec<JoinHandle<()>>,
    failure: Option<String>,
}
pub(super) struct CliSessionOwner {
    pub(super) instance: CliSessionInstanceId,
    pub(super) startup: Arc<CLIAgentRuntimeSessionStartup>,
    startup_completion: Mutex<StartupCompletion>,
    pub(super) closing: AtomicBool,
    pub(super) ready: AtomicBool,
    pub(super) replacement_prepared: AtomicBool,
    pub(super) cancellation: CancellationToken,
    callbacks: StdMutex<CallbackCompletion>,
    callback_join: Mutex<()>,
    close: Mutex<Option<JoinHandle<Result<(), String>>>>,
    close_failure: StdMutex<Option<String>>,
    pub(super) stopped: AtomicBool,
}
impl CliSessionOwner {
    pub(super) fn new(instance: CliSessionInstanceId) -> Arc<Self> {
        Arc::new(Self {
            instance,
            startup: Arc::default(),
            startup_completion: Mutex::new(StartupCompletion::default()),
            closing: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            replacement_prepared: AtomicBool::new(false),
            cancellation: CancellationToken::new(),
            callbacks: StdMutex::new(CallbackCompletion::default()),
            callback_join: Mutex::new(()),
            close: Mutex::new(None),
            close_failure: StdMutex::new(None),
            stopped: AtomicBool::new(false),
        })
    }
    pub(super) fn spawn_start_task<F>(&self, future: F)
    where
        F: Future<Output = Result<(), String>> + Send + 'static,
    {
        // Lock before spawn: its future can run on another worker immediately,
        // including its failed-start cleanup path. No observer can consume an
        // empty startup owner between task creation and handle publication.
        let mut completion = self
            .startup_completion
            .try_lock()
            .expect("startup task publication is synchronous");
        completion.task = Some(tokio::spawn(future));
    }
    pub(super) async fn finish_startup(&self) -> Result<()> {
        let mut completion = self.startup_completion.lock().await;
        if completion.outcome.is_none() {
            let Some(task) = completion.task.as_mut() else {
                bail!("CLI startup owner is unknown");
            };
            let outcome = match task.await {
                Ok(outcome) => outcome,
                Err(_) => {
                    completion.panicked = true;
                    Err("CLI factory task failed".into())
                }
            };
            completion.task.take();
            completion.outcome = Some(outcome);
        }
        completion
            .outcome
            .clone()
            .unwrap()
            .map_err(|e| anyhow!("{e}"))
    }
    pub(super) fn request_stop(&self) {
        // Registration and gate closure share this short native owner lock.
        let _callbacks = self
            .callbacks
            .lock()
            .expect("CLI callback ownership poisoned");
        self.closing.store(true, Ordering::Release);
        self.ready.store(false, Ordering::Release);
        self.cancellation.cancel();
        self.startup.cancellation.cancel();
    }
    pub(super) fn spawn_callback<F>(&self, future: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut callbacks = self
            .callbacks
            .lock()
            .expect("CLI callback ownership poisoned");
        if self.closing.load(Ordering::Acquire) {
            return false;
        }
        let mut consumed_failure = None;
        callbacks.tasks.retain_mut(|task| {
            if !task.is_finished() {
                return true;
            }
            let mut context = std::task::Context::from_waker(std::task::Waker::noop());
            if let std::task::Poll::Ready(result) =
                std::future::Future::poll(std::pin::Pin::new(task), &mut context)
            {
                if result.is_err() {
                    consumed_failure = Some("CLI callback task failed".to_owned());
                }
                return false;
            }
            true
        });
        if let Some(error) = consumed_failure {
            callbacks.failure.get_or_insert(error);
        }
        // Existing pumps/listeners per session are small and bounded. A poisoned
        // or overflowing owner fails closed instead of dropping a live handle.
        if callbacks.tasks.len() >= 32 {
            callbacks.failure = Some("CLI callback ownership limit exceeded".into());
            self.closing.store(true, Ordering::Release);
            self.cancellation.cancel();
            return false;
        }
        callbacks.tasks.push(tokio::spawn(
            CLI_CALLBACK_INSTANCE.scope(self.instance.clone(), future),
        ));
        true
    }
    pub(super) fn record_callback_failure(&self, error: String) {
        self.callbacks
            .lock()
            .expect("CLI callbacks poisoned")
            .failure
            .get_or_insert(error);
    }
    async fn join_callbacks(&self) -> Result<()> {
        let _join = self.callback_join.lock().await;
        loop {
            // No callback registration after request_stop. Take one actual
            // handle only into the instance-owned cleanup task, never a caller.
            let task = self
                .callbacks
                .lock()
                .expect("CLI callbacks poisoned")
                .tasks
                .pop();
            let Some(task) = task else {
                break;
            };
            if task.await.is_err() {
                self.callbacks
                    .lock()
                    .expect("CLI callbacks poisoned")
                    .failure
                    .get_or_insert("CLI callback task failed".into());
            }
        }
        match self
            .callbacks
            .lock()
            .expect("CLI callbacks poisoned")
            .failure
            .clone()
        {
            Some(error) => Err(anyhow!("{error}")),
            None => Ok(()),
        }
    }
    pub(super) fn begin_close(
        self: &Arc<Self>,
        lifecycle: Arc<dyn CLIAgentRuntimeSessionLifecycle>,
    ) {
        self.request_stop();
        let Ok(mut close) = self.close.try_lock() else {
            return;
        };
        if close.is_none()
            && !self.stopped.load(Ordering::Acquire)
            && self
                .close_failure
                .lock()
                .expect("CLI close failure poisoned")
                .is_none()
        {
            let owner = self.clone();
            *close = Some(tokio::spawn(async move {
                owner.drain(lifecycle).await.map_err(|e| format!("{e:#}"))
            }));
        }
    }
    pub(super) async fn close_and_wait(
        self: &Arc<Self>,
        lifecycle: Arc<dyn CLIAgentRuntimeSessionLifecycle>,
    ) -> Result<()> {
        self.begin_close(lifecycle);
        let mut close = self.close.lock().await;
        if let Some(task) = close.as_mut() {
            let result = match task.await {
                Ok(result) => result,
                Err(_) => {
                    let error = "CLI native cleanup task failed".to_owned();
                    *self
                        .close_failure
                        .lock()
                        .expect("CLI close failure poisoned") = Some(error.clone());
                    Err(error)
                }
            };
            close.take();
            result.map_err(|e| anyhow!("{e}"))?;
        }
        if let Some(error) = self
            .close_failure
            .lock()
            .expect("CLI close failure poisoned")
            .clone()
        {
            bail!("{error}");
        }
        if !self.stopped.load(Ordering::Acquire) {
            bail!("CLI stop completion is unknown");
        }
        Ok(())
    }
    async fn drain(
        self: Arc<Self>,
        lifecycle: Arc<dyn CLIAgentRuntimeSessionLifecycle>,
    ) -> Result<()> {
        let _startup_outcome = self.finish_startup().await;
        lifecycle.before_session_close(&self.instance).await;
        let session = self.startup.session();
        let native_result = match &session {
            Some(session) => session.stop_and_wait().await,
            None => Ok(()),
        };
        let callbacks_result = self.join_callbacks().await;
        native_result?;
        callbacks_result?;
        {
            let completion = self.startup_completion.lock().await;
            if completion.panicked || completion.outcome.is_none() {
                bail!("CLI startup completion is unknown or panicked; owner remains failed");
            }
        }
        lifecycle.after_session_close_result(&self.instance).await?;
        if let Some(session) = session {
            session.cleanup_after_stop().await?;
        }
        self.stopped.store(true, Ordering::Release);
        Ok(())
    }
}

/// Dropping the caller closes admission, not the owned factory task/resources.
pub(super) struct StartupWaitGuard(
    pub(super) Arc<CliSessionOwner>,
    pub(super) bool,
    pub(super) Arc<dyn CLIAgentRuntimeSessionLifecycle>,
);
impl Drop for StartupWaitGuard {
    fn drop(&mut self) {
        if !self.1 {
            self.0.begin_close(self.2.clone());
        }
    }
}

#[derive(Clone)]
pub(crate) struct CliSessionStopOwner(pub(super) Arc<CliSessionOwner>);
impl CliSessionStopOwner {
    pub(crate) fn instance(&self) -> &CliSessionInstanceId {
        &self.0.instance
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli_runtime::manager::{
        CLIAgentRuntimeSessionKey, NoopCLIAgentRuntimeSessionLifecycle,
    };
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Notify;

    struct Native {
        worker: Mutex<Option<JoinHandle<()>>>,
        stop_entered: Notify,
        cleaned: AtomicUsize,
    }
    #[async_trait]
    impl CLIAgentRuntimeSession for Native {
        async fn close(&self) -> Result<()> {
            self.stop_and_wait().await?;
            self.cleanup_after_stop().await
        }
        async fn stop_and_wait(&self) -> Result<()> {
            self.stop_entered.notify_one();
            let mut worker = self.worker.lock().await;
            if let Some(task) = worker.as_mut() {
                task.await?;
                worker.take();
            }
            Ok(())
        }
        async fn cleanup_after_stop(&self) -> Result<()> {
            self.cleaned.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    fn owner() -> Arc<CliSessionOwner> {
        CliSessionOwner::new(
            CliSessionInstanceId::unmanaged_for_test(
                CLIAgentRuntimeSessionKey::new("ws", "codex", "thread").unwrap(),
                1,
            )
            .unwrap(),
        )
    }
    fn native(release: Arc<Notify>) -> Arc<Native> {
        Arc::new(Native {
            worker: Mutex::new(Some(tokio::spawn(async move {
                release.notified().await;
            }))),
            stop_entered: Notify::new(),
            cleaned: AtomicUsize::new(0),
        })
    }
    struct PreparedLifecycle {
        root: std::path::PathBuf,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }
    #[async_trait]
    impl CLIAgentRuntimeSessionLifecycle for PreparedLifecycle {
        async fn after_session_close_result(&self, _instance: &CliSessionInstanceId) -> Result<()> {
            assert!(
                self.root.is_dir(),
                "managed root must survive until supervisor cleanup"
            );
            self.entered.notify_one();
            self.release.notified().await;
            Ok(())
        }
    }
    #[tokio::test]
    async fn prepared_without_process_retains_real_root_until_strict_cleanup_finishes() {
        let temporary = tempfile::tempdir().unwrap();
        let owner = owner();
        let identity = pioneer_cli_agent_runtime::claude::ClaudeManagedMcpConfigIdentity::new(
            "ws",
            "claude",
            "thread",
            owner.instance.boot_id().as_str(),
            1,
        )
        .unwrap();
        let descriptor = pioneer_cli_agent_runtime::claude::materialize_claude_mcp_config(
            temporary.path().join("managed").as_path(),
            identity,
            pioneer_cli_agent_runtime::claude::ClaudeManagedMcpLaunchMode::Empty,
        )
        .unwrap();
        let root = descriptor.session_root_path.clone();
        owner
            .startup
            .retain_preparation(CliStartupProcessCleanup::Claude(descriptor));
        owner.spawn_start_task(async { Err("prepared startup failed before spawn".to_owned()) });
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let lifecycle = Arc::new(PreparedLifecycle {
            root: root.clone(),
            entered: entered.clone(),
            release: release.clone(),
        });
        let waiter = {
            let owner = owner.clone();
            let lifecycle = lifecycle.clone();
            tokio::spawn(async move { owner.close_and_wait(lifecycle).await })
        };
        entered.notified().await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(root.is_dir());
        assert!(!owner.stopped.load(Ordering::Acquire));
        release.notify_one();
        owner.close_and_wait(lifecycle).await.unwrap();
        assert!(!root.exists());
        assert!(owner.stopped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn cleanup_started_inside_factory_waits_for_its_actual_start_handle() {
        let owner = owner();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let factory_owner = owner.clone();
        let announced = entered.clone();
        let gate = release.clone();
        owner.spawn_start_task(async move {
            factory_owner.begin_close(Arc::new(NoopCLIAgentRuntimeSessionLifecycle));
            announced.notify_one();
            gate.notified().await;
            Err("injected failed factory".to_owned())
        });
        entered.notified().await;
        let waiter = {
            let owner = owner.clone();
            tokio::spawn(async move {
                owner
                    .close_and_wait(Arc::new(NoopCLIAgentRuntimeSessionLifecycle))
                    .await
            })
        };
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        assert!(!owner.stopped.load(Ordering::Acquire));
        release.notify_one();
        waiter.await.unwrap().unwrap();
        assert!(owner.stopped.load(Ordering::Acquire));
        assert!(owner.finish_startup().await.is_err());
    }

    #[tokio::test]
    async fn cancelled_start_and_close_retain_actual_native_completion() {
        let owner = owner();
        let release_worker = Arc::new(Notify::new());
        let native = native(release_worker.clone());
        let release_factory = Arc::new(Notify::new());
        let published = Arc::new(Notify::new());
        let startup = owner.startup.clone();
        let factory_native = native.clone();
        let release = release_factory.clone();
        let announced = published.clone();
        owner.spawn_start_task(async move {
            startup.retain_session(factory_native);
            announced.notify_one();
            release.notified().await;
            Ok(())
        });
        let waiter = {
            let owner = owner.clone();
            tokio::spawn(async move {
                let _guard = StartupWaitGuard(
                    owner.clone(),
                    false,
                    Arc::new(NoopCLIAgentRuntimeSessionLifecycle),
                );
                owner.finish_startup().await
            })
        };
        published.notified().await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(owner.closing.load(Ordering::Acquire));
        assert!(owner.startup.session().is_some());
        let close = {
            let owner = owner.clone();
            tokio::spawn(async move {
                owner
                    .close_and_wait(Arc::new(NoopCLIAgentRuntimeSessionLifecycle))
                    .await
            })
        };
        release_factory.notify_one();
        native.stop_entered.notified().await;
        close.abort();
        assert!(close.await.unwrap_err().is_cancelled());
        assert_eq!(native.cleaned.load(Ordering::SeqCst), 0);
        assert!(!owner.stopped.load(Ordering::Acquire));
        release_worker.notify_one();
        owner
            .close_and_wait(Arc::new(NoopCLIAgentRuntimeSessionLifecycle))
            .await
            .unwrap();
        owner
            .close_and_wait(Arc::new(NoopCLIAgentRuntimeSessionLifecycle))
            .await
            .unwrap();
        assert_eq!(native.cleaned.load(Ordering::SeqCst), 1);
        assert!(owner.stopped.load(Ordering::Acquire));
    }
    #[tokio::test]
    async fn stop_joins_inflight_callback_before_cleanup_and_closes_registration() {
        let owner = owner();
        let release_worker = Arc::new(Notify::new());
        let native = native(release_worker.clone());
        owner.startup.retain_session(native.clone());
        owner.spawn_start_task(async { Ok(()) });
        owner.finish_startup().await.unwrap();
        let callback_entered = Arc::new(Notify::new());
        let release_callback = Arc::new(Notify::new());
        let entered = callback_entered.clone();
        let release = release_callback.clone();
        assert!(owner.spawn_callback(async move {
            entered.notify_one();
            release.notified().await;
        }));
        callback_entered.notified().await;
        let close = {
            let owner = owner.clone();
            tokio::spawn(async move {
                owner
                    .close_and_wait(Arc::new(NoopCLIAgentRuntimeSessionLifecycle))
                    .await
            })
        };
        native.stop_entered.notified().await;
        assert!(!owner.spawn_callback(async {
            panic!("late callback must never run");
        }));
        release_worker.notify_one();
        tokio::task::yield_now().await;
        assert!(!close.is_finished());
        assert_eq!(native.cleaned.load(Ordering::SeqCst), 0);
        release_callback.notify_one();
        close.await.unwrap().unwrap();
        assert_eq!(native.cleaned.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn consumed_callback_panic_remains_failed_on_repeat() {
        let owner = owner();
        owner.spawn_start_task(async { Ok(()) });
        owner.finish_startup().await.unwrap();
        assert!(owner.spawn_callback(async {
            panic!("injected callback failure");
        }));
        assert!(
            owner
                .close_and_wait(Arc::new(NoopCLIAgentRuntimeSessionLifecycle))
                .await
                .is_err()
        );
        assert!(
            owner
                .close_and_wait(Arc::new(NoopCLIAgentRuntimeSessionLifecycle))
                .await
                .is_err()
        );
        assert!(!owner.stopped.load(Ordering::Acquire));
    }
}
