//! Completion belongs to a native run, rather than its cancellation command.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopError {
    UnknownOwner,
    Deadline,
    Cleanup(String),
}

impl std::fmt::Display for StopError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownOwner => f.write_str("native execution owner is unknown"),
            Self::Deadline => {
                f.write_str("native stop deadline expired before cleanup was confirmed")
            }
            Self::Cleanup(message) => write!(f, "native execution cleanup failed: {message}"),
        }
    }
}
impl std::error::Error for StopError {}

/// The handle stays in this owner throughout an await. Dropping an observer
/// releases the per-task mutex, never the handle or a completed join error.
pub(crate) struct NativeTask {
    state: tokio::sync::Mutex<NativeTaskState>,
    abort: tokio::task::AbortHandle,
}
struct NativeTaskState {
    handle: Option<JoinHandle<()>>,
    result: Option<Result<(), StopError>>,
}
impl NativeTask {
    pub(crate) fn new(handle: JoinHandle<()>) -> Arc<Self> {
        Arc::new(Self {
            abort: handle.abort_handle(),
            state: tokio::sync::Mutex::new(NativeTaskState {
                handle: Some(handle),
                result: None,
            }),
        })
    }
    pub(crate) fn abort(&self) {
        self.abort.abort();
    }
    pub(crate) async fn join(&self) -> Result<(), StopError> {
        let mut state = self.state.lock().await;
        if let Some(result) = &state.result {
            return result.clone();
        }
        let result = match state.handle.as_mut().ok_or(StopError::UnknownOwner)?.await {
            Ok(()) => Ok(()),
            // An abort is acknowledged by join. Shell cleanup is a separate,
            // mandatory part of the run result below.
            Err(error) if error.is_cancelled() => Ok(()),
            Err(_) => Err(StopError::Cleanup("native task panicked".to_owned())),
        };
        state.result = Some(result.clone());
        state.handle = None;
        result
    }
}

#[derive(Default)]
pub(crate) struct NativeRunCompletion {
    root: StdMutex<Option<Arc<NativeTask>>>,
    predecessors: StdMutex<Vec<Arc<NativeRunCompletion>>>,
    panicked: std::sync::atomic::AtomicBool,
    tools: StdMutex<Vec<Arc<NativeTask>>>,
    shells: StdMutex<Vec<Arc<pioneer_tools::handlers::UnifiedExecHandler>>>,
    cleanup: tokio::sync::Mutex<()>,
    result: StdMutex<Option<Result<(), StopError>>>,
}
impl NativeRunCompletion {
    pub(crate) fn retain_predecessor(&self, prior: Arc<Self>) {
        if prior.outcome() != Some(Ok(())) {
            self.predecessors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(prior);
        }
    }
    pub(crate) fn mark_panic(&self) {
        self.panicked.store(true, Ordering::Release);
    }
    pub(crate) fn set_root(&self, root: Arc<NativeTask>) {
        *self
            .root
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(root);
    }
    pub(crate) fn retain_tool(&self, tool: Arc<NativeTask>) {
        self.tools
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(tool);
    }
    pub(crate) fn retain_shell(&self, shell: Arc<pioneer_tools::handlers::UnifiedExecHandler>) {
        self.shells
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(shell);
    }
    pub(crate) fn outcome(&self) -> Option<Result<(), StopError>> {
        self.result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    pub(crate) async fn finish(&self, cancel: bool) -> Result<(), StopError> {
        // This is a single native owner's cleanup mutex, never a registry/DB
        // lock. Both the actor and an internal observer use the same drain.
        let _cleanup = self.cleanup.lock().await;
        if let Some(result) = self.outcome() {
            return result;
        }
        let root = self
            .root
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or(StopError::UnknownOwner)?;
        let root_result = if cancel {
            match tokio::time::timeout(Duration::from_secs(2), root.join()).await {
                Ok(result) => result,
                Err(_) => {
                    root.abort();
                    root.join().await
                }
            }
        } else {
            root.join().await
        };
        // Root has ended, so it can no longer publish new tasks or runtimes.
        let tools = self
            .tools
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut outcome = root_result;
        if self.panicked.load(Ordering::Acquire) {
            outcome = Err(StopError::Cleanup("native root panicked".to_owned()));
        }
        for tool in tools {
            if let Err(error) = tool.join().await {
                outcome = Err(error);
            }
        }
        let shells = self
            .shells
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for shell in shells {
            if shell.stop_and_wait().await.is_err() {
                // Keep native owners available for repair, including when a
                // waiter was dropped in the middle of process cleanup.
                return Err(StopError::Cleanup(
                    "shell process cleanup was not confirmed".to_owned(),
                ));
            }
        }
        let predecessors = self
            .predecessors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for predecessor in predecessors {
            Box::pin(predecessor.finish(true)).await?;
        }
        self.predecessors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        *self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(outcome.clone());
        self.tools
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.shells
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn completed_root(owner: &NativeRunCompletion) {
        owner.set_root(NativeTask::new(tokio::spawn(async {})));
    }

    #[tokio::test]
    async fn admission_and_root_join_do_not_acknowledge_delayed_nested_tool() {
        let owner = Arc::new(NativeRunCompletion::default());
        completed_root(&owner);
        let (release, released) = oneshot::channel();
        let tool = NativeTask::new(tokio::spawn(async move {
            let _ = released.await;
        }));
        owner.retain_tool(tool);
        // Existing cancellation admission can be acknowledged independently.
        let (ack, admitted) = oneshot::channel();
        ack.send(()).unwrap();
        admitted.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), owner.finish(false))
                .await
                .is_err()
        );
        assert!(owner.outcome().is_none());
        release.send(()).unwrap();
        owner.finish(false).await.unwrap();
        assert_eq!(owner.outcome(), Some(Ok(())));
    }

    #[tokio::test]
    async fn cancelled_completion_waiter_keeps_the_actual_tool_join_for_retry() {
        let owner = Arc::new(NativeRunCompletion::default());
        completed_root(&owner);
        let (entered, entry) = oneshot::channel();
        let (release, released) = oneshot::channel();
        owner.retain_tool(NativeTask::new(tokio::spawn(async move {
            entered.send(()).unwrap();
            let _ = released.await;
        })));
        entry.await.unwrap();
        let observer = owner.clone();
        let waiter = tokio::spawn(async move { observer.finish(false).await });
        tokio::task::yield_now().await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(owner.outcome().is_none());
        release.send(()).unwrap();
        owner.finish(false).await.unwrap();
        owner.finish(true).await.unwrap();
    }

    #[tokio::test]
    async fn panic_and_unknown_root_are_not_success_or_cancellation_ack() {
        let unknown = NativeRunCompletion::default();
        assert_eq!(unknown.finish(true).await, Err(StopError::UnknownOwner));
        let owner = NativeRunCompletion::default();
        owner.set_root(NativeTask::new(tokio::spawn(async {
            panic!("native root failure");
        })));
        assert!(matches!(
            owner.finish(true).await,
            Err(StopError::Cleanup(_))
        ));
        assert_eq!(owner.finish(true).await, owner.outcome().unwrap());
    }

    #[tokio::test]
    async fn cleared_and_retired_owner_survives_deadline_and_later_retry() {
        let manager = AgentManager::new(
            Arc::new(ProviderRegistry::new(|_| String::new())),
            crate::manager_tests::test_tool_loop_config(),
        );
        let (tx, _rx) = mpsc::channel(1);
        let control = TurnExecutionControl::new(tx, 19);
        completed_root(&control.completion);
        let (release, released) = oneshot::channel();
        control
            .completion
            .retain_tool(NativeTask::new(tokio::spawn(async move {
                let _ = released.await;
            })));
        let plane = AgentThreadControlPlane::default();
        plane.activate("turn".into(), 19, control.clone());
        plane.clear("turn", 19);
        assert!(plane.execution_for("turn").is_none());
        assert_eq!(plane.completion_for("turn").unwrap().run_id, 19);
        manager
            .state
            .write()
            .await
            .retiring_executions
            .insert("thread".into(), vec![plane]);
        assert_eq!(
            manager
                .cancel_turn_and_wait(
                    "thread",
                    "turn",
                    "stop",
                    tokio::time::Instant::now() + Duration::from_millis(10)
                )
                .await,
            Err(StopError::Deadline)
        );
        assert!(
            manager
                .state
                .read()
                .await
                .retiring_executions
                .contains_key("thread")
        );
        release.send(()).unwrap();
        manager
            .cancel_turn_and_wait(
                "thread",
                "turn",
                "retry",
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert!(
            !manager
                .state
                .read()
                .await
                .retiring_executions
                .contains_key("thread")
        );
        assert_eq!(
            manager
                .cancel_turn_and_wait(
                    "missing",
                    "turn",
                    "stop",
                    tokio::time::Instant::now() + Duration::from_secs(1)
                )
                .await,
            Err(StopError::UnknownOwner)
        );
    }

    #[tokio::test]
    async fn recovery_replacement_run_does_not_inherit_an_old_success() {
        let plane = AgentThreadControlPlane::default();
        let (tx, _rx) = mpsc::channel(1);
        let old = TurnExecutionControl::new(tx.clone(), 1);
        completed_root(&old.completion);
        plane.activate("turn".into(), 1, old.clone());
        old.completion.finish(false).await.unwrap();
        let new = TurnExecutionControl::new(tx, 2);
        let (release, released) = oneshot::channel();
        new.completion
            .set_root(NativeTask::new(tokio::spawn(async move {
                let _ = released.await;
            })));
        plane.activate("turn".into(), 2, new.clone());
        assert_eq!(plane.completion_for("turn").unwrap().run_id, 2);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), new.completion.finish(false))
                .await
                .is_err()
        );
        release.send(()).unwrap();
        new.completion.finish(false).await.unwrap();
    }
}
