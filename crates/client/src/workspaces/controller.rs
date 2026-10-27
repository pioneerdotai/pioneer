//! Workspace request lifecycle, independent from shell transport delivery.
use super::{
    directory::{InitialDirectoryRestore, ThreadTreeLoadState},
    intents::WorkspaceIntent,
};
use crate::core::{ClientCore, ClientMutationAuthority, ClientTransition};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, mpsc},
    thread::JoinHandle,
    time::{Duration, Instant},
};

const MAX_QUEUED_COMMANDS: usize = 64;
pub(crate) struct ThreadDraftRetry {
    pub workspace: String,
    pub thread: String,
    pub connection: Option<u64>,
    pub authorization: u64,
    pub workspace_generation: u64,
    pub plan: crate::threads::start::ThreadStartRetryPlan,
}
enum WorkspaceRequest {
    ReconcilePush(String),
    ReconcileDraft,
    RetryDraft(ThreadDraftRetry),
    Bootstrap {
        preferred: Option<String>,
        connection: Option<u64>,
    },
    Refresh {
        workspace: String,
        connection: Option<u64>,
    },
    Command {
        intent: WorkspaceIntent,
        connection: Option<u64>,
    },
}
#[derive(Default)]
pub(crate) struct WorkspaceController {
    pub(super) push: super::push::PushController,
    queue: Option<pioneer_observability::queues::QueueObserver>,
    sender: Option<mpsc::SyncSender<()>>,
    // At most one refresh per retained workspace; hints during a request leave
    // one follow-up. The wake channel carries no domain data and cannot lose it.
    refresh: BTreeMap<String, Option<u64>>,
    bootstrap: Option<(Option<String>, Option<u64>)>,
    commands: VecDeque<(WorkspaceIntent, Option<u64>)>,
    draft_retry: Option<ThreadDraftRetry>,
    reconcile_draft: bool,
    task: Option<JoinHandle<()>>,
}
impl WorkspaceController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.refresh.clear();
        self.push = Default::default();
        self.bootstrap = None;
        self.commands.clear();
        self.draft_retry = None;
        self.reconcile_draft = false;
    }
    pub(super) fn wake(&self) {
        let queue = self
            .queue
            .as_ref()
            .map(pioneer_observability::queues::QueueObserver::in_current_epoch);
        if let Some(sender) = &self.sender {
            if let Some(queue) = &queue {
                queue.enqueued(pioneer_observability::queues::QueueBytes::Known(0));
            }
            match sender.try_send(()) {
                Ok(()) => {}
                Err(mpsc::TrySendError::Full(_)) => {
                    if let Some(queue) = &queue {
                        queue.coalesced_send(pioneer_observability::queues::QueueBytes::Known(0));
                    }
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    if let Some(queue) = &queue {
                        queue.rejected_send(pioneer_observability::queues::QueueBytes::Known(0));
                    }
                }
            }
        }
    }
    fn retry_wait(&self, now: Instant) -> Option<Duration> {
        self.draft_retry
            .as_ref()
            .map(|retry| retry.plan.next_attempt_at.saturating_duration_since(now))
    }
    fn next(&mut self, now: Instant) -> Option<WorkspaceRequest> {
        if let Some((preferred, connection)) = self.bootstrap.take() {
            Some(WorkspaceRequest::Bootstrap {
                preferred,
                connection,
            })
        } else if let Some((intent, connection)) = self.commands.pop_front() {
            Some(WorkspaceRequest::Command { intent, connection })
        } else if std::mem::take(&mut self.reconcile_draft) {
            Some(WorkspaceRequest::ReconcileDraft)
        } else if self
            .draft_retry
            .as_ref()
            .is_some_and(|retry| retry.plan.next_attempt_at <= now)
        {
            self.draft_retry.take().map(WorkspaceRequest::RetryDraft)
        } else if let Some((workspace, connection)) = self.refresh.pop_first() {
            Some(WorkspaceRequest::Refresh {
                workspace,
                connection,
            })
        } else {
            self.push
                .pending
                .pop_first()
                .map(WorkspaceRequest::ReconcilePush)
        }
    }
}
impl Drop for WorkspaceController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
/// One actual post-wake quantum. The native recv/timeout remains outside this
/// scope; early shutdown/owner loss and unwind drop an unfinished guard.
fn run_workspace_cycle(
    weak: &std::sync::Weak<ClientCore>,
    mut handle: impl FnMut(&ClientCore, WorkspaceRequest),
) -> bool {
    use pioneer_observability::{
        live::WorkGuard,
        work::{Boundary, ExecutorRole, Outcome, Trigger, WorkCarrier, WorkKind},
    };
    let cycle = WorkCarrier::independent(
        Boundary::Timer,
        WorkKind::ClientCommand,
        Trigger::Reconcile,
        ExecutorRole::ClientController,
    );
    let _scope = cycle.enter();
    let mut guard = WorkGuard::start(cycle.context(), false);
    loop {
        let Some(core) = weak.upgrade() else {
            return false;
        };
        if core.is_stopped() {
            return false;
        }
        let request = core
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned")
            .next(Instant::now());
        let Some(request) = request else {
            break;
        };
        handle(&core, request);
    }
    guard.finish(Outcome::Completed);
    true
}
impl ClientCore {
    pub(crate) fn queue_workspace_draft_reconciliation(&self) {
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        if owner.sender.is_some() && !self.is_stopped() {
            owner.reconcile_draft = true;
            owner.wake();
        }
    }
    pub(crate) fn queue_thread_draft_retry(&self, retry: ThreadDraftRetry) {
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        if owner.sender.is_some() && !self.is_stopped() {
            owner.draft_retry = Some(retry);
            owner.wake();
        }
    }
    fn thread_draft_retry_is_current(&self, retry: &ThreadDraftRetry) -> bool {
        let navigation = self.navigation_snapshot();
        let start = self.thread_start_snapshot();
        !self.is_stopped()
            && self.gateway_http_generation() == retry.connection
            && self.authorization_connection_generation() == retry.authorization
            && self.workspace_operation_generation(&retry.workspace) == retry.workspace_generation
            && navigation.workspace_id() == Some(retry.workspace.as_str())
            && navigation
                .active_thread_id()
                .is_none_or(|id| id == retry.thread)
            && navigation.draft(&retry.workspace).is_none()
            && start.pending_thread_id.as_deref() == Some(retry.thread.as_str())
            && !start.in_progress
            && start.retry_attempt == retry.plan.attempt
            && start.next_attempt_at == Some(retry.plan.next_attempt_at)
    }
    fn retire_thread_draft_retry(&self, retry: &ThreadDraftRetry) {
        let mut start = self.thread_start_mutation();
        if start.pending_thread_id.as_deref() == Some(retry.thread.as_str())
            && start.retry_attempt == retry.plan.attempt
            && start.next_attempt_at == Some(retry.plan.next_attempt_at)
        {
            start.next_attempt_at = None;
        }
    }
    pub(crate) fn start_workspace_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel(1);
        let queue = pioneer_observability::queues::QueueObserver::new(
            pioneer_observability::queues::QueueKind::ClientController,
            Some(1),
            "coalescing wake tokens only; zero payload bytes; domain state is reconciled separately",
        );
        let receiver_queue = queue.clone();
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-workspace-directory".into())
            .spawn(move || {
                loop {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    let wait = core
                        .workspace_controller
                        .lock()
                        .expect("workspace controller poisoned")
                        .retry_wait(Instant::now());
                    drop(core);
                    match wait {
                        Some(wait) => {
                            if matches!(
                                receiver.recv_timeout(wait).map(|()| {
                                    receiver_queue.dequeued(
                                        pioneer_observability::queues::QueueBytes::Known(0),
                                    );
                                    receiver_queue.outcome(
                                        pioneer_observability::queues::QueueOutcome::Processed,
                                        1,
                                    );
                                }),
                                Err(mpsc::RecvTimeoutError::Disconnected)
                            ) {
                                return;
                            }
                        }
                        None => {
                            if receiver
                                .recv()
                                .map(|()| {
                                    receiver_queue.dequeued(
                                        pioneer_observability::queues::QueueBytes::Known(0),
                                    );
                                    receiver_queue.outcome(
                                        pioneer_observability::queues::QueueOutcome::Processed,
                                        1,
                                    );
                                })
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                    if !run_workspace_cycle(&weak, |core, request| {
                        core.execute_workspace_request(request)
                    }) {
                        return;
                    }
                }
            })
            .expect("workspace worker could not start");
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        owner.queue = Some(queue);
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
    fn execute_workspace_request(&self, request: WorkspaceRequest) {
        let core = self;
        match request {
            WorkspaceRequest::ReconcilePush(workspace) => core.reconcile_workspace_push(&workspace),
            WorkspaceRequest::ReconcileDraft => {
                if let Some(workspace) = core.navigation_snapshot().workspace_id() {
                    core.restore_selected_workspace_thread(workspace);
                }
            }
            WorkspaceRequest::RetryDraft(retry) => {
                let current = core.thread_draft_retry_is_current(&retry);
                core.retire_thread_draft_retry(&retry);
                if current {
                    let _ = core.execute_workspace_intent(WorkspaceIntent::NewThread {
                        workspace_id: retry.workspace,
                    });
                }
            }
            WorkspaceRequest::Bootstrap {
                preferred,
                connection,
            } if core.gateway_http_generation() == connection => {
                if let Ok(reduction) = core.bootstrap_workspace_catalog(preferred) {
                    core.load_selected_workspace_directory(&reduction.selected.workspace_id);
                }
            }
            WorkspaceRequest::Refresh {
                workspace,
                connection,
            } if core.gateway_http_generation() == connection
                && core.workspace_refresh_is_demanded(&workspace) =>
            {
                core.refresh_workspace_directory_with_transport(
                    &workspace,
                    &core.transport_runtime().ws_command_sender(),
                );
            }
            WorkspaceRequest::Command { intent, connection }
                if core.gateway_http_generation() == connection =>
            {
                let operation = core.begin_directory_action(&intent);
                let result = core.execute_workspace_intent(intent);
                if core.gateway_http_generation() == connection {
                    if let Some(operation) = operation {
                        core.complete_directory_action(
                            operation,
                            result.err().map(|error| format!("{error:#}")),
                        );
                    }
                }
            }
            _ => {}
        }
    }
    pub fn request_workspace_bootstrap(&self, preferred: Option<String>) {
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        if owner.sender.is_some() && !self.is_stopped() {
            owner.bootstrap = Some((preferred, self.gateway_http_generation()));
            owner.wake();
        }
    }
    pub(crate) fn observe_workspace_notification(
        &self,
        notification: &pioneer_protocol::GatewayNotification,
    ) -> bool {
        use pioneer_protocol::GatewayNotification;
        match notification {
            GatewayNotification::Unknown(change)
                if change.method == pioneer_protocol::constants::events::WORKSPACE_PUSH_CHANGED =>
            {
                self.recover_malformed_workspace_push();
                true
            }
            GatewayNotification::WorkspacePush(change) => {
                self.observe_workspace_push(change);
                true
            }
            GatewayNotification::WorkspaceChanged(_) => {
                self.observe_workspace_catalog(notification);
                true
            }
            GatewayNotification::ThreadReadCursorChanged(change) => {
                self.apply_directory_read(
                    &change.workspace_id,
                    &change.thread_id,
                    &change.cursor,
                    change.unread_count,
                );
                true
            }
            GatewayNotification::ThreadTreeChanged(change) => {
                self.queue_directory_refresh(&change.workspace_id);
                true
            }
            GatewayNotification::ThreadAgentsDocChanged(change) => {
                self.queue_directory_refresh(&change.workspace_id);
                false
            }
            _ => false,
        }
    }
    /// Explicit refresh from a producer that changed thread metadata outside the directory UI.
    pub fn request_workspace_tree_refresh(&self, workspace: &str) {
        self.queue_directory_refresh(workspace);
    }
    pub(crate) fn load_selected_workspace_directory(&self, workspace: &str) {
        self.load_selected_workspace_directory_with_transport(
            workspace,
            &self.transport_runtime().ws_command_sender(),
        );
    }
    fn load_selected_workspace_directory_with_transport(
        &self,
        workspace: &str,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
    ) {
        {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            if !self.is_stopped() && registry.navigation.workspace_id() == Some(workspace) {
                registry.directory.initial_restore = Some(InitialDirectoryRestore {
                    workspace: workspace.to_owned(),
                    connection: self.gateway_http_generation(),
                    navigation_revision: registry.navigation_revision,
                });
            }
        }
        self.refresh_workspace_directory_with_transport(workspace, transport);
    }
    fn refresh_workspace_directory_with_transport(
        &self,
        workspace: &str,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
    ) {
        let _ = self.refresh_workspace_tree_with_transport(workspace, transport);
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let directory = registry.directory.snapshot(workspace);
        let state = directory
            .as_ref()
            .filter(|p| p.error().is_none())
            .and_then(|p| p.load())
            .map(|load| load.state);
        let ready = state == Some(ThreadTreeLoadState::Ready);
        let mut continue_initial = false;
        if let Some(initial) = &registry.directory.initial_restore
            && initial.workspace == workspace
        {
            let current = !self.is_stopped()
                && initial.connection == self.gateway_http_generation()
                && initial.navigation_revision == registry.navigation_revision
                && registry.navigation.workspace_id() == Some(workspace);
            if !current || state != Some(ThreadTreeLoadState::Pending) {
                // Consume before restore can mutate navigation or start a draft.
                continue_initial = current && ready;
                registry.directory.initial_restore = None;
            }
        }
        let remembered = registry.navigation.last_active(workspace).is_some()
            || registry.navigation.draft(workspace).is_some();
        drop(registry);
        if ready && (continue_initial || remembered) {
            self.restore_selected_workspace_thread(workspace);
        }
    }
    fn restore_selected_workspace_thread(&self, workspace: &str) {
        if self.workspace_tree(workspace).is_none_or(|directory| {
            directory.is_loading()
                || directory.error().is_some()
                || directory
                    .load()
                    .is_some_and(|load| load.state != ThreadTreeLoadState::Ready)
        }) {
            return;
        }
        let navigation = self.navigation_snapshot();
        if navigation.workspace_id() != Some(workspace) || navigation.active_thread_id().is_some() {
            return;
        }
        let restored = crate::threads::tree::restore_workspace_thread_state(
            workspace,
            navigation.last_active(workspace),
            navigation.draft(workspace),
            |id, workspace| {
                self.thread_snapshot(id)
                    .is_some_and(|snapshot| snapshot.coordinator().workspace_id == workspace)
                    || self
                        .thread_registry
                        .lock()
                        .expect("thread registry poisoned")
                        .directory
                        .threads
                        .get(id)
                        .is_some_and(|thread| thread.workspace_id == workspace)
            },
        );
        if let Some(thread_id) = restored.active_thread_id {
            // Directory loading no longer creates every coordinator. Restore
            // the selected one through the existing registry owner only.
            drop(self.thread_mutation(&thread_id, workspace));
            self.navigate(
                crate::navigation::NavigationIntent::SelectThread {
                    workspace_id: Some(workspace.to_owned()),
                    thread_id: Some(thread_id),
                },
                None,
            );
        } else {
            // Workspace capabilities can arrive after the directory. Their
            // publication wakes this owner; absence is not a completed bootstrap.
            if !self
                .authorization_snapshot(Some(workspace), None)
                .and_then(|authorization| authorization.workspace)
                .is_some_and(|workspace| workspace.capabilities.can_create_thread)
                || self.thread_start_snapshot().in_progress
                || self.thread_start_snapshot().next_attempt_at.is_some()
            {
                return;
            }
            let _ = self.execute_workspace_intent(WorkspaceIntent::NewThread {
                workspace_id: workspace.to_owned(),
            });
        }
    }
    pub(crate) fn cancel_workspace_requests(&self, workspace: &str) {
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        owner.refresh.remove(workspace);
        let retry = if owner
            .draft_retry
            .as_ref()
            .is_some_and(|retry| retry.workspace == workspace)
        {
            owner.draft_retry.take()
        } else {
            None
        };
        owner
            .commands
            .retain(|(intent, _)| intent.workspace_id() != Some(workspace));
        drop(owner);
        {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            if registry
                .directory
                .initial_restore
                .as_ref()
                .is_some_and(|initial| initial.workspace == workspace)
            {
                registry.directory.cancel_initial_restore();
            }
        }
        if let Some(retry) = retry {
            self.retire_thread_draft_retry(&retry);
        }
    }
    pub(crate) fn queue_directory_refresh(&self, workspace: &str) {
        if !self.workspace_refresh_is_demanded(workspace) {
            return;
        }
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        if owner.sender.is_some() && !self.is_stopped() {
            owner
                .refresh
                .insert(workspace.to_owned(), self.gateway_http_generation());
            owner.wake();
        }
    }
    pub(crate) fn dispatch_workspace_intent(&self, intent: WorkspaceIntent) -> ClientTransition {
        let authority = ClientMutationAuthority { _private: () };
        if matches!(intent, WorkspaceIntent::SelectThread { .. }) {
            return if self.execute_workspace_intent(intent).is_ok() {
                self.transition(&authority, vec![], vec![])
            } else {
                self.reject_intent()
            };
        }
        // Identity publications wake this queue while holding the identity owner.
        // Read authorization before taking the queue lock to keep that order acyclic.
        if let WorkspaceIntent::NewThread { workspace_id } = &intent {
            if !self
                .authorization_snapshot(Some(workspace_id), None)
                .and_then(|snapshot| snapshot.workspace)
                .is_some_and(|workspace| workspace.capabilities.can_create_thread)
            {
                return self.reject_intent();
            }
        }
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        if self.is_stopped()
            || owner.sender.is_none()
            || owner.commands.len() >= MAX_QUEUED_COMMANDS
        {
            return self.reject_intent();
        }
        if owner.commands.iter().any(|(queued, connection)| {
            queued == &intent && *connection == self.gateway_http_generation()
        }) {
            return self.transition(&authority, vec![], vec![]);
        }
        let selection = if let WorkspaceIntent::NewThread { workspace_id } = &intent {
            let draft = self
                .navigation_snapshot()
                .draft(workspace_id)
                .map(str::to_owned);
            Some(self.open_workspace_thread(workspace_id.clone(), draft, None))
        } else {
            None
        };
        owner
            .commands
            .push_back((intent, self.gateway_http_generation()));
        owner.wake();
        selection.unwrap_or_else(|| self.transition(&authority, vec![], vec![]))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn paused_combined_scope_resumes_after_renewal_or_page_deferred_action() {
        super::super::push::tests::paused_combined_scope_resumes_after_renewal_or_page_deferred_action(
            |core| {
                assert!(run_workspace_cycle(&Arc::downgrade(core), |core, request| {
                    core.execute_workspace_request(request);
                }));
            },
        );
    }
    #[test]
    fn actual_workspace_quantum_is_live_during_handlers_and_terminal_before_idle() {
        use pioneer_observability::{
            live::{self, JournalPayload, WorkState},
            work::{self, Outcome},
        };
        let _lock = crate::OBSERVABILITY_TEST_LOCK.lock().unwrap();
        let old = pioneer_observability::telemetry_enabled();
        pioneer_observability::set_telemetry_enabled(false);
        pioneer_observability::set_telemetry_enabled(true);
        let core = Arc::new(ClientCore::new());
        struct Cleanup {
            core: Arc<ClientCore>,
            enabled: bool,
        }
        impl Drop for Cleanup {
            fn drop(&mut self) {
                self.core.shutdown();
                pioneer_observability::set_telemetry_enabled(self.enabled);
            }
        }
        let _cleanup = Cleanup {
            core: core.clone(),
            enabled: old,
        };
        {
            let mut owner = core.workspace_controller.lock().unwrap();
            owner.bootstrap = Some((None, None));
            owner.refresh.insert("ws".into(), None);
            owner.commands.push_back((
                WorkspaceIntent::NewThread {
                    workspace_id: "ws".into(),
                },
                None,
            ));
        }
        let mut order = Vec::new();
        let mut token = None;
        run_workspace_cycle(&Arc::downgrade(&core), |_, request| {
            let current = work::current().unwrap();
            token = Some(current.token);
            let snapshot = live::snapshot().unwrap();
            assert!(
                snapshot
                    .active
                    .iter()
                    .any(|entry| entry.context.token == current.token
                        && entry.state == WorkState::Running)
            );
            assert!(!snapshot.journal.iter().any(|r| matches!(&r.payload, JournalPayload::Work(w) if w.context.token == current.token && matches!(w.state, WorkState::Terminal(_)))));
            order.push(match request {
                WorkspaceRequest::Bootstrap { .. } => "bootstrap",
                WorkspaceRequest::Command { .. } => "command",
                WorkspaceRequest::Refresh { .. } => "refresh",
                _ => panic!("unexpected fixture request"),
            });
        });
        assert_eq!(order, ["bootstrap", "command", "refresh"]);
        assert!(token.is_some());
        assert!(
            live::snapshot()
                .unwrap()
                .active
                .iter()
                .all(|entry| Some(entry.context.token) != token)
        ); // native next wake wait owns no active cycle
        let terminals: Vec<_> = live::snapshot()
            .unwrap()
            .journal
            .iter()
            .filter_map(|r| match &r.payload {
                JournalPayload::Work(w) if Some(w.context.token) == token => match w.state {
                    WorkState::Terminal(outcome) => Some(outcome),
                    _ => None,
                },
                _ => None,
            })
            .collect();
        assert_eq!(terminals, [Outcome::Completed]);
        core.workspace_controller.lock().unwrap().bootstrap = Some((None, None));
        let mut panic_token = None;
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_workspace_cycle(
                &Arc::downgrade(&core),
                |_, _| {
                    panic_token = work::current().map(|c| c.token);
                    panic!("fake handler panic")
                }
            )))
            .is_err()
        );
        core.workspace_controller.lock().unwrap().bootstrap = Some((None, None));
        let mut cancelled_token = None;
        assert!(!run_workspace_cycle(&Arc::downgrade(&core), |core, _| {
            cancelled_token = work::current().map(|c| c.token);
            core.shutdown();
        }));
        assert_eq!(work::current(), None);
        assert!(!run_workspace_cycle(
            &std::sync::Weak::new(),
            |_, _| unreachable!()
        ));
        let snapshot = live::snapshot().unwrap();
        assert!(snapshot.active.iter().all(|entry| {
            ![token, panic_token, cancelled_token].contains(&Some(entry.context.token))
        }));
        for (token, outcome) in [
            (panic_token, Outcome::Panicked),
            (cancelled_token, Outcome::Cancelled),
        ] {
            assert!(token.is_some());
            let terminal: Vec<_> = snapshot
                .journal
                .iter()
                .filter_map(|r| match &r.payload {
                    JournalPayload::Work(w)
                        if Some(w.context.token) == token
                            && matches!(w.state, WorkState::Terminal(_)) =>
                    {
                        Some(w.state)
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(terminal, [WorkState::Terminal(outcome)]);
        }
    }

    use pioneer_protocol::{
        SandboxMode, SandboxPolicy, ThreadStartParams, ThreadStartResponse, ThreadVisibility,
    };
    struct DraftTransport(Option<&'static str>);
    impl crate::rpc::JsonRpcRequestTransport for DraftTransport {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            if let Some(error) = self.0 {
                return Err(error.into());
            }
            let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
            let params: ThreadStartParams =
                serde_json::from_value(request["params"].clone()).unwrap();
            let thread = serde_json::from_value(serde_json::json!({"id":params.thread_id,"workspace_id":params.workspace_id,"preview":"","mode":"Chat","model":"model","model_provider":"provider","created_at":1,"updated_at":1,"status":"Idle","turns":[]})).unwrap();
            response
                .send(Ok(serde_json::to_value(ThreadStartResponse {
                    thread,
                    sandbox: SandboxPolicy::from_mode(SandboxMode::FullAccess),
                })
                .unwrap()))
                .unwrap();
            Ok(())
        }
    }
    fn draft_fixture() -> (Arc<ClientCore>, String, mpsc::Receiver<()>) {
        let core = Arc::new(ClientCore::new());
        let (sender, receiver) = mpsc::sync_channel(1);
        core.workspace_controller.lock().unwrap().sender = Some(sender);
        core.activate_thread(None, Some("ws"));
        let id = core.prepare_thread_draft().unwrap();
        (core, id, receiver)
    }
    #[test]
    fn draft_creation_retry_uses_existing_backoff_and_same_identity_with_virtual_time() {
        let (core, id, _receiver) = draft_fixture();
        let mut now = Instant::now();
        for attempt in 0..8 {
            assert!(
                core.create_workspace_thread_draft_with_clock(
                    &DraftTransport(Some("websocket connection timeout")),
                    "ws",
                    ThreadVisibility::Private,
                    || now
                )
                .is_err()
            );
            let delay = crate::threads::start::thread_start_retry_delay(attempt);
            let mut owner = core.workspace_controller.lock().unwrap();
            assert_eq!(owner.retry_wait(now), Some(delay));
            assert!(owner.next(now + delay - Duration::from_nanos(1)).is_none());
            let Some(WorkspaceRequest::RetryDraft(retry)) = owner.next(now + delay) else {
                panic!("retry must become due once");
            };
            assert!(owner.next(now + delay).is_none());
            drop(owner);
            assert_eq!(retry.thread, id);
            assert!(core.thread_draft_retry_is_current(&retry));
            core.retire_thread_draft_retry(&retry);
            now += delay;
        }
        assert_eq!(
            core.create_workspace_thread_draft_with_clock(
                &DraftTransport(None),
                "ws",
                ThreadVisibility::Private,
                || now
            )
            .unwrap(),
            id
        );
        assert_eq!(
            core.navigation_snapshot().active_thread_id(),
            Some(id.as_str())
        );
        assert!(!core.thread_start_snapshot().in_progress);
        assert!(core.thread_start_snapshot().next_attempt_at.is_none());
        assert!(
            core.workspace_controller
                .lock()
                .unwrap()
                .next(now + Duration::from_secs(60))
                .is_none()
        );
    }
    #[test]
    fn draft_retry_rejects_other_workspace_and_is_removed_on_exit() {
        let (core, _, _receiver) = draft_fixture();
        let now = Instant::now();
        assert!(
            core.create_workspace_thread_draft_with_clock(
                &DraftTransport(Some("connection timeout")),
                "ws",
                ThreadVisibility::Private,
                || now
            )
            .is_err()
        );
        core.activate_thread(None, Some("other"));
        let owner = core.workspace_controller.lock().unwrap();
        assert!(!core.thread_draft_retry_is_current(owner.draft_retry.as_ref().unwrap()));
        drop(owner);
        core.cancel_workspace_requests("ws");
        assert!(core.thread_start_snapshot().next_attempt_at.is_none());
        assert!(
            core.workspace_controller
                .lock()
                .unwrap()
                .next(now + Duration::from_secs(60))
                .is_none()
        );
    }
    #[test]
    fn late_draft_response_cannot_overwrite_creation_after_connection_change() {
        struct ReconnectingTransport(Arc<ClientCore>);
        impl crate::rpc::JsonRpcRequestTransport for ReconnectingTransport {
            fn send_json_rpc_request(
                &self,
                _: String,
                payload: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                self.0
                    .begin_authorization_epoch(Some(("synthetic-endpoint".into(), 1)));
                assert!(!self.0.thread_start_snapshot().in_progress);
                self.0
                    .create_workspace_thread_draft(
                        &DraftTransport(None),
                        "ws",
                        ThreadVisibility::Private,
                    )
                    .unwrap();
                DraftTransport(None).send_json_rpc_request(String::new(), payload, response)
            }
        }
        let (core, id, _receiver) = draft_fixture();
        let error = core
            .create_workspace_thread_draft(
                &ReconnectingTransport(core.clone()),
                "ws",
                ThreadVisibility::Private,
            )
            .unwrap_err();
        assert!(error.to_string().contains("cancelled"));
        assert_eq!(core.navigation_snapshot().draft("ws"), Some(id.as_str()));
        assert!(core.thread_snapshot(&id).is_some());
        assert!(!core.thread_start_snapshot().in_progress);
        assert!(core.thread_start_snapshot().pending_thread_id.is_none());
        assert!(
            core.workspace_controller
                .lock()
                .unwrap()
                .draft_retry
                .is_none()
        );
    }

    #[test]
    fn invalid_draft_request_is_terminal_until_explicit_retry() {
        let (core, id, _receiver) = draft_fixture();
        let now = Instant::now();
        assert!(
            core.create_workspace_thread_draft_with_clock(
                &DraftTransport(Some("invalid request")),
                "ws",
                ThreadVisibility::Private,
                || now
            )
            .is_err()
        );
        assert!(!core.thread_start_snapshot().in_progress);
        assert!(core.thread_start_snapshot().next_attempt_at.is_none());
        assert!(
            core.workspace_controller
                .lock()
                .unwrap()
                .next(now + Duration::from_secs(60))
                .is_none()
        );
        assert_eq!(core.prepare_thread_draft().as_deref(), Some(id.as_str()));
    }
    #[test]
    fn late_workspace_capabilities_resume_the_waiting_draft_creation() {
        use pioneer_protocol::*;
        let (core, _, _receiver) = draft_fixture();
        let mut authorization = AuthorizationCapabilitySnapshot {
            schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
            authorization_revision: 1,
            principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
            role_key: "member".into(),
            role: AuthorizationRolePresentation {
                key: "member".into(),
                display_name: "Synthetic".into(),
                description: String::new(),
                built_in: false,
            },
            global: Default::default(),
            workspace: None,
            thread: None,
        };
        assert_eq!(
            core.accept_authorization_projection(0, None, authorization.clone()),
            crate::authorization::AuthorizationProjectionAcceptance::Accepted
        );
        core.upsert_thread(serde_json::from_value(serde_json::json!({"id":"existing","workspace_id":"ws","preview":"","mode":"Chat","model":"model","model_provider":"provider","created_at":1,"updated_at":1,"status":"Idle","turns":[]})).unwrap());
        core.workspace_controller
            .lock()
            .unwrap()
            .next(Instant::now());
        core.restore_selected_workspace_thread("ws");
        assert!(!core.thread_start_snapshot().in_progress);
        assert!(core.thread_start_snapshot().next_attempt_at.is_none());
        assert!(
            core.workspace_controller
                .lock()
                .unwrap()
                .next(Instant::now())
                .is_none()
        );
        let resources = AuthorizationOperationalResourceProjection {
            fingerprint: "synthetic".into(),
            ..Default::default()
        };
        authorization.workspace = Some(AuthorizationWorkspaceCapabilitySnapshot {
            workspace_id: "ws".into(),
            capabilities: AuthorizationWorkspaceCapabilities {
                can_create_thread: true,
                thread_visibility_options: vec![ThreadVisibility::Private],
                ..Default::default()
            },
            operational_resources: resources.clone(),
            execution_draft_policy: AuthorizationExecutionDraftPolicyProjection {
                fingerprint: "synthetic".into(),
                resources,
                permission_options: vec![],
                can_attach_artifacts: false,
                mcp_invocation_limits: Default::default(),
            },
        });
        assert_eq!(
            core.accept_authorization_projection(core.current_auth_ticket().0, None, authorization),
            crate::authorization::AuthorizationProjectionAcceptance::Accepted
        );
        assert!(matches!(
            core.workspace_controller
                .lock()
                .unwrap()
                .next(Instant::now()),
            Some(WorkspaceRequest::ReconcileDraft)
        ));
        // No connection/endpoints are configured: the actual command path reaches
        // thread/start, fails locally, and schedules its first transport retry.
        core.restore_selected_workspace_thread("ws");
        assert_eq!(core.thread_start_snapshot().retry_attempt, 1);
        assert!(
            core.workspace_controller
                .lock()
                .unwrap()
                .draft_retry
                .is_some()
        );
        core.cancel_workspace_requests("ws");
    }

    #[test]
    fn authorization_publication_wakes_draft_reconciliation_after_directory_loading() {
        let (core, _, _receiver) = draft_fixture();
        let now = Instant::now();
        assert!(
            core.workspace_controller
                .lock()
                .unwrap()
                .next(now)
                .is_none()
        );
        core.begin_authorization_epoch(Some(("synthetic-endpoint".into(), 1)));
        assert!(matches!(
            core.workspace_controller.lock().unwrap().next(now),
            Some(WorkspaceRequest::ReconcileDraft)
        ));
        core.restore_selected_workspace_thread("ws");
        assert!(!core.thread_start_snapshot().in_progress);
        assert!(
            core.workspace_controller
                .lock()
                .unwrap()
                .next(now)
                .is_none()
        );
        core.begin_authorization_epoch(Some(("synthetic-endpoint".into(), 1)));
        assert!(
            core.workspace_controller
                .lock()
                .unwrap()
                .next(now)
                .is_none()
        );
    }
    #[test]
    fn startup_directory_recovers_when_capabilities_arrive_after_or_during_loading() {
        use crate::authorization::AuthorizationProjectionAcceptance;
        use crate::core::ClientScope;
        use std::num::NonZeroUsize;

        struct DirectoryTransport<'a> {
            before_response: &'a dyn Fn(),
            running: bool,
        }
        impl crate::rpc::JsonRpcRequestTransport for DirectoryTransport<'_> {
            fn send_json_rpc_request(
                &self,
                _: String,
                payload: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
                assert_eq!(request["method"], "thread/tree");
                (self.before_response)();
                response
                    .send(Ok(serde_json::json!({
                        "workspace_id": "ws",
                        "threads": [{"id":"existing","workspace_id":"ws","preview":"history",
                            "mode":"Chat","model":"model","model_provider":"provider",
                            "created_at":1,"updated_at":1,"status":"Idle","turns":[]}],
                        "running_task_thread_ids": if self.running { vec!["existing"] } else { vec![] },
                        "unread":[],"folders":[
                            {"id":"parent","workspace_id":"ws","name":"Parent","created_at":1,"updated_at":1},
                            {"id":"child","workspace_id":"ws","parent_folder_id":"parent","name":"Child","created_at":1,"updated_at":1}
                        ],"placements":[{"thread_id":"existing","workspace_id":"ws","folder_id":"child"}],"agents_docs":[]
                    })))
                    .unwrap();
                Ok(())
            }
        }
        // Exercise both orders deterministically, without timers or UI remounts.
        for during_request in [false, true] {
            let core = Arc::new(ClientCore::new());
            let (sender, _receiver) = mpsc::sync_channel(1);
            core.workspace_controller.lock().unwrap().sender = Some(sender);
            let scope = ClientScope::WorkspaceTree {
                workspace_id: Some("ws".into()),
            };
            let subscription = core.subscribe(scope.clone(), NonZeroUsize::new(8).unwrap());
            core.activate_thread(None, Some("ws"));
            let capabilities = crate::catalog_test_support::client()
                .authorization_snapshot(None, None)
                .unwrap();
            let accept = || {
                let (generation, connection) = core.current_auth_ticket();
                assert_eq!(
                    core.accept_authorization_projection(
                        generation,
                        connection,
                        capabilities.clone()
                    ),
                    AuthorizationProjectionAcceptance::Accepted
                );
            };
            let before_response = || {
                if during_request {
                    accept();
                }
            };
            let initial = core.refresh_workspace_tree_with_transport(
                "ws",
                &DirectoryTransport {
                    before_response: &before_response,
                    running: true,
                },
            );
            if during_request {
                assert!(initial.is_err(), "the pre-fence response must be rejected");
            } else {
                assert_eq!(initial.unwrap().snapshot().threads_by_id.len(), 1);
                accept();
            }
            assert!(
                core.workspace_tree("ws").is_none(),
                "protected data is cleared"
            );
            assert!(
                core.workspace_refresh_is_demanded("ws"),
                "a fence must retain the live directory demand"
            );
            let mut refreshes = 0;
            while let Some(request) = core
                .workspace_controller
                .lock()
                .unwrap()
                .next(Instant::now())
            {
                if let WorkspaceRequest::Refresh { workspace, .. } = request {
                    assert_eq!(workspace, "ws");
                    refreshes += 1;
                }
            }
            assert_eq!(
                refreshes, 1,
                "accepted capabilities must reload the cleared directory"
            );
            let loaded = core
                .refresh_workspace_tree_with_transport(
                    "ws",
                    &DirectoryTransport {
                        before_response: &|| {},
                        running: true,
                    },
                )
                .unwrap();
            assert_eq!(loaded.snapshot().threads_by_id.len(), 1);
            assert!(
                loaded
                    .snapshot()
                    .running_task_thread_ids
                    .contains("existing")
            );
            assert!(loaded.snapshot().running_task_folder_ids.contains("parent"));
            assert!(
                core.thread_snapshot("existing").is_none(),
                "directory refresh must not hydrate unopened threads"
            );
            drop(core.thread_mutation("existing", "ws"));
            let coordinator = core.thread_snapshot("existing").unwrap().coordinator();
            assert!(
                !coordinator.history_loaded,
                "directory recovery needs no timeline"
            );
            assert!(!coordinator.history_loading);
            assert!(coordinator.thread().unwrap().turns.is_empty());
            assert_eq!(core.navigation_snapshot().workspace_id(), Some("ws"));
            accept();
            assert!(
                core.workspace_controller.lock().unwrap().refresh.is_empty(),
                "equal capabilities must not cause a reload loop"
            );
            assert!(core.observe_workspace_notification(
                &pioneer_protocol::GatewayNotification::ThreadTreeChanged(
                    pioneer_protocol::ThreadTreeChangedNotification {
                        workspace_id: "ws".into()
                    }
                )
            ));
            assert_eq!(core.workspace_controller.lock().unwrap().refresh.len(), 1);
            core.workspace_controller.lock().unwrap().refresh.clear();
            core.workspace_demand_changed(&scope, crate::core::ClientDemand::Suspended);
            let changed = pioneer_protocol::GatewayNotification::ThreadTreeChanged(
                pioneer_protocol::ThreadTreeChangedNotification {
                    workspace_id: "ws".into(),
                },
            );
            core.observe_workspace_notification(&changed);
            assert!(core.workspace_controller.lock().unwrap().refresh.is_empty());
            core.workspace_demand_changed(&scope, crate::core::ClientDemand::Visible);
            assert_eq!(core.workspace_controller.lock().unwrap().refresh.len(), 1);
            let resumed = core
                .refresh_workspace_tree_with_transport(
                    "ws",
                    &DirectoryTransport {
                        before_response: &|| {},
                        running: false,
                    },
                )
                .unwrap();
            assert!(resumed.snapshot().running_task_thread_ids.is_empty());
            assert!(resumed.snapshot().running_task_folder_ids.is_empty());
            core.workspace_controller.lock().unwrap().refresh.clear();
            drop(subscription);
            core.observe_workspace_notification(&changed);
            assert!(core.workspace_controller.lock().unwrap().refresh.is_empty());
            let _resubscribed = core.subscribe(scope, NonZeroUsize::new(8).unwrap());
            assert_eq!(core.workspace_controller.lock().unwrap().refresh.len(), 1);
        }
    }

    #[test]
    fn authorization_directory_reload_waits_for_capabilities_and_visible_demand() {
        use crate::core::{ClientDemand, ClientScope};
        use std::num::NonZeroUsize;
        let core = crate::catalog_test_support::client();
        let (sender, _receiver) = mpsc::sync_channel(1);
        core.workspace_controller.lock().unwrap().sender = Some(sender);
        let scope = ClientScope::WorkspaceTree {
            workspace_id: Some("ws".into()),
        };
        let subscription = core.subscribe(scope.clone(), NonZeroUsize::new(8).unwrap());
        core.upsert_thread(serde_json::from_value(serde_json::json!({"id":"existing","workspace_id":"ws","preview":"","mode":"Chat","model":"model","model_provider":"provider","created_at":1,"updated_at":1,"status":"Idle","turns":[]})).unwrap());
        let mut capabilities = core.authorization_snapshot(None, None).unwrap();
        capabilities.authorization_revision += 1;
        core.invalidate_authorization_revision(capabilities.authorization_revision);
        assert!(core.workspace_tree("ws").is_none());
        assert!(core.workspace_controller.lock().unwrap().refresh.is_empty());
        // Navigation/draft reconciliation can publish an empty local directory
        // after the fence; its existence does not mean the server was reloaded.
        {
            let mut registry = core.thread_registry.lock().unwrap();
            registry.directory.project("ws", None, false, None);
        }
        core.workspace_demand_changed(&scope, ClientDemand::Suspended);
        let (generation, connection) = core.current_auth_ticket();
        core.accept_authorization_projection(generation, connection, capabilities);
        assert!(core.workspace_controller.lock().unwrap().refresh.is_empty());
        core.workspace_demand_changed(&scope, ClientDemand::Visible);
        assert_eq!(core.workspace_controller.lock().unwrap().refresh.len(), 1);
        drop(subscription);
        assert!(!core.workspace_refresh_is_demanded("ws"));
        assert!(core.workspace_controller.lock().unwrap().refresh.is_empty());
    }

    #[test]
    fn initial_read_reconciliation_continues_draft_bootstrap_once_without_new_auth_events() {
        use pioneer_protocol::*;

        struct TreeReply<'a> {
            core: &'a ClientCore,
            read_during_request: bool,
        }
        impl crate::rpc::JsonRpcRequestTransport for TreeReply<'_> {
            fn send_json_rpc_request(
                &self,
                _: String,
                payload: String,
                tx: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
                assert_eq!(request["method"], "thread/tree");
                if self.read_during_request {
                    assert!(self.core.observe_workspace_notification(
                        &GatewayNotification::ThreadReadCursorChanged(
                            ThreadReadCursorChangedNotification {
                                workspace_id: "ws".into(),
                                thread_id: "existing".into(),
                                cursor: ThreadReadCursor {
                                    sort_key: "002".into(),
                                    through_turn_id: "turn".into()
                                },
                                unread_count: 0,
                            }
                        )
                    ));
                }
                tx.send(Ok(serde_json::json!({
                    "workspace_id":"ws", "threads":[{"id":"existing","workspace_id":"ws","preview":"history","mode":"Chat","model":"model","model_provider":"provider","created_at":1,"updated_at":1,"status":"Idle","turns":[]}],
                    "running_task_thread_ids":["existing"], "unread":[{"thread_id":"existing","unread_count":1}], "folders":[], "placements":[], "agents_docs":[]
                }))).unwrap();
                Ok(())
            }
        }
        let core = Arc::new(ClientCore::new());
        let (sender, _receiver) = mpsc::sync_channel(1);
        core.workspace_controller.lock().unwrap().sender = Some(sender);
        core.activate_thread(None, Some("ws"));
        let mut authorization = crate::catalog_test_support::client()
            .authorization_snapshot(None, None)
            .unwrap();
        let resources = AuthorizationOperationalResourceProjection {
            fingerprint: "synthetic".into(),
            ..Default::default()
        };
        authorization.workspace = Some(AuthorizationWorkspaceCapabilitySnapshot {
            workspace_id: "ws".into(),
            capabilities: AuthorizationWorkspaceCapabilities {
                can_create_thread: true,
                thread_visibility_options: vec![ThreadVisibility::Private],
                ..Default::default()
            },
            operational_resources: resources.clone(),
            execution_draft_policy: AuthorizationExecutionDraftPolicyProjection {
                fingerprint: "synthetic".into(),
                resources,
                permission_options: vec![],
                can_attach_artifacts: false,
                mcp_invocation_limits: Default::default(),
            },
        });
        assert_eq!(
            core.accept_authorization_projection(0, None, authorization),
            crate::authorization::AuthorizationProjectionAcceptance::Accepted
        );
        // The one earlier capability wake has already been handled. No further
        // authorization or Administration event rescues the pending load.
        assert!(matches!(
            core.workspace_controller
                .lock()
                .unwrap()
                .next(Instant::now()),
            Some(WorkspaceRequest::ReconcileDraft)
        ));
        let steady = TreeReply {
            core: &core,
            read_during_request: false,
        };
        core.refresh_workspace_directory_with_transport("ws", &steady);
        assert!(core.navigation_snapshot().active_thread_id().is_none());
        assert!(core.navigation_snapshot().last_active("ws").is_none());
        assert!(core.navigation_snapshot().draft("ws").is_none());
        assert!(
            core.thread_start_snapshot().pending_thread_id.is_none(),
            "an ordinary hint cannot start a draft"
        );

        core.load_selected_workspace_directory_with_transport(
            "ws",
            &TreeReply {
                core: &core,
                read_during_request: true,
            },
        );
        assert_eq!(
            core.workspace_tree("ws").unwrap().load().unwrap().state,
            ThreadTreeLoadState::Pending
        );
        assert!(
            core.thread_registry
                .lock()
                .unwrap()
                .directory
                .initial_restore
                .is_some()
        );
        assert!(core.thread_start_snapshot().pending_thread_id.is_none());
        let Some(WorkspaceRequest::Refresh {
            workspace,
            connection,
        }) = core
            .workspace_controller
            .lock()
            .unwrap()
            .next(Instant::now())
        else {
            panic!("read conflict must queue one follow-up")
        };
        assert_eq!(workspace, "ws");
        assert_eq!(connection, core.gateway_http_generation());
        core.refresh_workspace_directory_with_transport(&workspace, &steady);
        assert_eq!(
            core.workspace_tree("ws").unwrap().load().unwrap().state,
            ThreadTreeLoadState::Ready
        );
        assert!(
            core.thread_registry
                .lock()
                .unwrap()
                .directory
                .initial_restore
                .is_none()
        );
        // The unchanged restore/create rule reaches thread/start. Without a
        // configured endpoint it prepares one identity and the existing retry.
        let start = core.thread_start_snapshot();
        let id = start.pending_thread_id.unwrap();
        assert_eq!(start.retry_attempt, 1);
        assert!(
            core.workspace_controller
                .lock()
                .unwrap()
                .draft_retry
                .is_some()
        );
        for _ in 0..2 {
            core.queue_directory_refresh("ws");
        }
        assert_eq!(core.workspace_controller.lock().unwrap().refresh.len(), 1);
        core.workspace_controller
            .lock()
            .unwrap()
            .refresh
            .remove("ws");
        core.refresh_workspace_directory_with_transport("ws", &steady);
        assert_eq!(
            core.thread_start_snapshot().pending_thread_id.as_deref(),
            Some(id.as_str())
        );
        assert_eq!(
            core.thread_start_snapshot().retry_attempt,
            1,
            "hints must not restart the bootstrap"
        );
        core.cancel_workspace_requests("ws");
    }

    #[test]
    fn initial_restore_cancels_with_navigation_suspend_and_invalidation() {
        let core = ClientCore::new();
        core.activate_thread(None, Some("ws"));
        for teardown in 0..3 {
            {
                let mut registry = core.thread_registry.lock().unwrap();
                registry.directory.initial_restore = Some(InitialDirectoryRestore {
                    workspace: "ws".into(),
                    connection: core.gateway_http_generation(),
                    navigation_revision: registry.navigation_revision,
                });
            }
            match teardown {
                0 => {
                    core.activate_thread(None, Some("other"));
                    core.activate_thread(None, Some("ws"));
                }
                1 => core.workspace_demand_changed(
                    &crate::core::ClientScope::WorkspaceTree {
                        workspace_id: Some("ws".into()),
                    },
                    crate::core::ClientDemand::Suspended,
                ),
                _ => core.thread_registry.lock().unwrap().directory.invalidate(),
            }
            assert!(
                core.thread_registry
                    .lock()
                    .unwrap()
                    .directory
                    .initial_restore
                    .is_none()
            );
        }
    }

    #[test]
    fn last_active_thread_restores_from_directory_without_creating_other_stores() {
        let core = crate::catalog_test_support::client();
        core.navigate(
            crate::navigation::NavigationIntent::SelectWorkspace {
                workspace_id: Some("ws".into()),
            },
            None,
        );
        core.navigate(
            crate::navigation::NavigationIntent::RememberLast {
                workspace_id: "ws".into(),
                thread_id: Some("last".into()),
            },
            None,
        );
        {
            let mut registry = core.thread_registry.lock().unwrap();
            for id in ["last", "unopened"] {
                let t = serde_json::from_value::<pioneer_protocol::Thread>(serde_json::json!({"id":id,"workspace_id":"ws","preview":"","mode":"Chat","model":"model","model_provider":"provider","created_at":1,"updated_at":1,"status":"Idle","turns":[]})).unwrap();
                registry.directory.threads.insert(id.into(), t);
            }
            registry.directory.project("ws", None, false, None);
        }
        assert!(core.thread_snapshots().is_empty());
        core.restore_selected_workspace_thread("ws");
        assert_eq!(core.navigation_snapshot().active_thread_id(), Some("last"));
        assert!(core.thread_snapshot("last").is_some());
        assert!(core.thread_snapshot("unopened").is_none());
    }

    #[test]
    fn read_reconciliation_uses_one_existing_queue_slot_and_suspension_cancels_it() {
        let core = crate::catalog_test_support::client();
        let (sender, _receiver) = mpsc::sync_channel(1);
        core.workspace_controller.lock().unwrap().sender = Some(sender);
        // Reconciliation follows a tree load; hints for an unknown directory
        // deliberately do not enqueue work.
        core.thread_registry
            .lock()
            .unwrap()
            .directory
            .project("ws", None, false, None);
        let scope = crate::core::ClientScope::WorkspaceTree {
            workspace_id: Some("ws".into()),
        };
        let _subscription = core.subscribe(scope.clone(), std::num::NonZeroUsize::new(8).unwrap());
        for _ in 0..1000 {
            core.queue_directory_refresh("ws");
        }
        assert_eq!(core.workspace_controller.lock().unwrap().refresh.len(), 1);
        core.workspace_demand_changed(&scope, crate::core::ClientDemand::Suspended);
        assert!(core.workspace_controller.lock().unwrap().refresh.is_empty());
        core.workspace_demand_changed(&scope, crate::core::ClientDemand::Visible);
        assert_eq!(core.workspace_controller.lock().unwrap().refresh.len(), 1);
        core.shutdown();
        assert!(
            core.workspace_controller
                .lock()
                .unwrap()
                .next(Instant::now())
                .is_none()
        );
    }
}
