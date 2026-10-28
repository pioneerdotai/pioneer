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
    joined: std::sync::atomic::AtomicBool,
}
struct NativeTaskState {
    handle: Option<JoinHandle<()>>,
    result: Option<Result<(), StopError>>,
}
impl NativeTask {
    pub(crate) fn new(handle: JoinHandle<()>) -> Arc<Self> {
        Arc::new(Self {
            abort: handle.abort_handle(),
            joined: std::sync::atomic::AtomicBool::new(false),
            state: tokio::sync::Mutex::new(NativeTaskState {
                handle: Some(handle),
                result: None,
            }),
        })
    }
    pub(crate) fn is_finished(&self) -> bool {
        self.abort.is_finished()
    }
    pub(crate) fn is_joined(&self) -> bool {
        self.joined.load(Ordering::Acquire)
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
        self.joined.store(true, Ordering::Release);
        result
    }
}

#[derive(Default)]
pub(crate) struct NativeRunCompletion {
    root: StdMutex<Option<Arc<NativeTask>>>,
    predecessors: StdMutex<Vec<(String, TurnExecutionControl)>>,
    panicked: std::sync::atomic::AtomicBool,
    quiescent: std::sync::atomic::AtomicBool,
    tools: StdMutex<Vec<Arc<NativeTask>>>,
    shells: StdMutex<Vec<Arc<pioneer_tools::handlers::UnifiedExecHandler>>>,
    cleanup: tokio::sync::Mutex<()>,
    result: StdMutex<Option<Result<(), StopError>>>,
}
impl NativeRunCompletion {
    pub(crate) fn retain_predecessor(&self, turn_id: String, prior: TurnExecutionControl) {
        if !prior.completion.is_quiescent() {
            self.predecessors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((turn_id, prior));
        }
    }
    pub(crate) fn is_quiescent(&self) -> bool {
        self.quiescent.load(Ordering::Acquire)
    }
    pub(crate) fn predecessors(&self) -> Vec<(String, TurnExecutionControl)> {
        self.predecessors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
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
        if self.is_quiescent() {
            return self.outcome().ok_or(StopError::UnknownOwner)?;
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
        // Keep this run's consumed failure across a cancelled cleanup waiter.
        let mut outcome = self.outcome().unwrap_or(root_result.clone());
        if outcome.is_ok() {
            outcome = root_result;
        }
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
        let mut own_quiescent = true;
        for shell in shells {
            if let Err(error) = shell.stop_and_wait().await {
                outcome = Err(StopError::Cleanup(error.to_string()));
            }
            own_quiescent &= shell.cleanup_is_quiescent();
        }
        *self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(outcome.clone());
        // A predecessor's drained panic stays its own stop result. It is not
        // an error of this successor, nor evidence of outstanding ownership.
        let mut pending_error = None;
        for (_, predecessor) in self.predecessors() {
            let result = Box::pin(predecessor.completion.finish(true)).await;
            if !predecessor.completion.is_quiescent() {
                pending_error = Some(result.err().unwrap_or(StopError::UnknownOwner));
            }
        }
        let mut predecessors = self
            .predecessors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        predecessors.retain(|(_, p)| !p.completion.is_quiescent());
        if own_quiescent && predecessors.is_empty() {
            self.tools
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
            self.shells
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
            self.quiescent.store(true, Ordering::Release);
        }
        drop(predecessors);
        if let Some(error) = pending_error {
            return Err(error);
        }

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
    #[tokio::test]
    async fn drained_root_and_tool_panics_do_not_poison_successors_or_grow_predecessors() {
        for panic_root in [true, false] {
            let plane = AgentThreadControlPlane::default();
            let (tx, _rx) = mpsc::channel(1);
            let a = TurnExecutionControl::new(tx.clone(), 1);
            if panic_root {
                a.completion.set_root(NativeTask::new(tokio::spawn(async {
                    panic!("A root");
                })));
            } else {
                completed_root(&a.completion);
                a.completion
                    .retain_tool(NativeTask::new(tokio::spawn(async {
                        panic!("A tool");
                    })));
            }
            plane.activate("A".into(), 1, a.clone());
            assert!(a.completion.finish(false).await.is_err());
            let captured_error = a.completion.outcome().unwrap();
            assert!(a.completion.is_quiescent());
            for run in 2..=4 {
                let successor = TurnExecutionControl::new(tx.clone(), run);
                completed_root(&successor.completion);
                plane.activate(format!("successor-{run}"), run, successor.clone());
                assert!(successor.completion.predecessors().is_empty());
                successor.completion.finish(false).await.unwrap();
                assert_eq!(successor.completion.outcome(), Some(Ok(())));
                assert!(!plane.has_pending_cleanup());
            }
            assert_eq!(a.completion.finish(true).await, captured_error);
        }
    }

    #[tokio::test]
    async fn pending_panicked_predecessor_is_retained_through_cancelled_drain_then_released() {
        let plane = AgentThreadControlPlane::default();
        let (tx, _rx) = mpsc::channel(1);
        let a = TurnExecutionControl::new(tx.clone(), 1);
        a.completion.set_root(NativeTask::new(tokio::spawn(async {
            panic!("A root");
        })));
        let (release, released) = oneshot::channel();
        a.completion
            .retain_tool(NativeTask::new(tokio::spawn(async move {
                let _ = released.await;
            })));
        plane.activate("A".into(), 1, a.clone());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), a.completion.finish(false))
                .await
                .is_err()
        );
        assert!(!a.completion.is_quiescent());
        let b = TurnExecutionControl::new(tx.clone(), 2);
        completed_root(&b.completion);
        plane.activate("B".into(), 2, b.clone());
        assert_eq!(b.completion.predecessors().len(), 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), b.completion.finish(false))
                .await
                .is_err()
        );
        assert!(!b.completion.is_quiescent());
        assert!(plane.has_pending_cleanup());
        release.send(()).unwrap();
        b.completion.finish(false).await.unwrap();
        assert!(a.completion.is_quiescent());
        assert!(a.completion.finish(true).await.is_err());
        assert!(b.completion.predecessors().is_empty());
        let c = TurnExecutionControl::new(tx, 3);
        completed_root(&c.completion);
        plane.activate("C".into(), 3, c.clone());
        c.completion.finish(false).await.unwrap();
        assert!(c.completion.predecessors().is_empty());
    }

    #[tokio::test]
    async fn snapshot_keeps_exact_initial_and_revision_owners_and_drops_drained_history() {
        let manager = AgentManager::new(
            Arc::new(ProviderRegistry::new(|_| String::new())),
            crate::manager_tests::test_tool_loop_config(),
        );
        let plane = AgentThreadControlPlane::default();
        let (tx, _rx) = mpsc::channel(1);
        let initial = TurnExecutionControl::new(tx.clone(), 1);
        completed_root(&initial.completion);
        let (release, released) = oneshot::channel();
        initial
            .completion
            .retain_tool(NativeTask::new(tokio::spawn(async move {
                let _ = released.await;
            })));
        plane.activate("initial".into(), 1, initial.clone());
        let revision = TurnExecutionControl::new(tx, 2);
        completed_root(&revision.completion);
        plane.activate("revision".into(), 2, revision.clone());
        manager
            .state
            .write()
            .await
            .retiring_executions
            .insert("thread".into(), vec![plane.clone()]);
        let threads = vec!["thread".into()];
        let owners = manager
            .capture_native_stop_owners(&threads, 8)
            .await
            .unwrap();
        let keys = owners
            .iter()
            .map(|owner| (owner.thread_id(), owner.turn_id(), owner.run_id()))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            keys,
            std::collections::BTreeSet::from([("thread", "initial", 1), ("thread", "revision", 2)])
        );
        assert!(
            manager
                .cancel_captured_turn_and_wait(
                    &owners[0],
                    "fenced",
                    tokio::time::Instant::now() + Duration::from_millis(10)
                )
                .await
                .is_err()
        );
        assert_eq!(
            manager
                .capture_native_stop_owners(&threads, 8)
                .await
                .unwrap()
                .len(),
            2
        );
        release.send(()).unwrap();
        for owner in &owners {
            manager
                .cancel_captured_turn_and_wait(
                    owner,
                    "retry",
                    tokio::time::Instant::now() + Duration::from_secs(1),
                )
                .await
                .unwrap();
        }
        assert!(
            manager
                .capture_native_stop_owners(&threads, 8)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(!plane.has_pending_cleanup());
    }

    #[tokio::test]
    async fn retirement_snapshot_rejects_unjoined_actor_and_preserves_join_after_waiter_cancel() {
        let manager = AgentManager::new(
            Arc::new(ProviderRegistry::new(|_| String::new())),
            crate::manager_tests::test_tool_loop_config(),
        );
        let plane = AgentThreadControlPlane::default();
        let (release, released) = oneshot::channel();
        let actor = NativeTask::new(tokio::spawn(async move {
            let _ = released.await;
        }));
        *plane.retiring_actor.lock().unwrap() = Some(actor.clone());
        manager
            .state
            .write()
            .await
            .retiring_executions
            .insert("thread".into(), vec![plane.clone()]);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), actor.join())
                .await
                .is_err()
        );
        assert!(matches!(
            manager
                .capture_native_stop_owners(&["thread".into()], 8)
                .await,
            Err(StopError::UnknownOwner)
        ));
        assert!(plane.has_pending_cleanup());
        release.send(()).unwrap();
        actor.join().await.unwrap();
        assert!(
            manager
                .capture_native_stop_owners(&["thread".into()], 8)
                .await
                .unwrap()
                .is_empty()
        );
        manager.drain_retiring_native_execution("thread").await;
        assert!(
            !manager
                .state
                .read()
                .await
                .retiring_executions
                .contains_key("thread")
        );
    }
    #[tokio::test]
    async fn captured_drained_failure_is_sticky_but_retirement_releases_its_ownership() {
        let manager = AgentManager::new(
            Arc::new(ProviderRegistry::new(|_| String::new())),
            crate::manager_tests::test_tool_loop_config(),
        );
        let (tx, _rx) = mpsc::channel(1);
        let control = TurnExecutionControl::new(tx, 1);
        control
            .completion
            .set_root(NativeTask::new(tokio::spawn(async {
                panic!("drained failure");
            })));
        let plane = AgentThreadControlPlane::default();
        plane.activate("failed".into(), 1, control.clone());
        manager
            .state
            .write()
            .await
            .retiring_executions
            .insert("thread".into(), vec![plane]);
        let captured = manager
            .capture_turn_stop_owner("thread", "failed")
            .await
            .unwrap();
        let error = manager
            .cancel_captured_turn_and_wait(
                &captured,
                "stop",
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(control.completion.is_quiescent());
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
                .cancel_captured_turn_and_wait(
                    &captured,
                    "repeat",
                    tokio::time::Instant::now() + Duration::from_secs(1)
                )
                .await,
            Err(error)
        );
    }
}
