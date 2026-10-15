//! Workspace request lifecycle, independent from shell transport delivery.
use super::intents::WorkspaceIntent;
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
        self.bootstrap = None;
        self.commands.clear();
        self.draft_retry = None;
        self.reconcile_draft = false;
    }
    fn wake(&self) {
        if let Some(sender) = &self.sender {
            let _ = sender.try_send(());
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
        } else {
            self.refresh
                .pop_first()
                .map(|(workspace, connection)| WorkspaceRequest::Refresh {
                    workspace,
                    connection,
                })
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
                                receiver.recv_timeout(wait),
                                Err(mpsc::RecvTimeoutError::Disconnected)
                            ) {
                                return;
                            }
                        }
                        None => {
                            if receiver.recv().is_err() {
                                return;
                            }
                        }
                    }
                    loop {
                        let Some(core) = weak.upgrade() else {
                            return;
                        };
                        if core.is_stopped() {
                            return;
                        }
                        let request = core
                            .workspace_controller
                            .lock()
                            .expect("workspace controller poisoned")
                            .next(Instant::now());
                        match request {
                            Some(WorkspaceRequest::ReconcileDraft) => {
                                if let Some(workspace) = core.navigation_snapshot().workspace_id() {
                                    core.restore_selected_workspace_thread(workspace);
                                }
                            }
                            Some(WorkspaceRequest::RetryDraft(retry)) => {
                                let current = core.thread_draft_retry_is_current(&retry);
                                core.retire_thread_draft_retry(&retry);
                                if current {
                                    let _ =
                                        core.execute_workspace_intent(WorkspaceIntent::NewThread {
                                            workspace_id: retry.workspace,
                                        });
                                }
                            }
                            Some(WorkspaceRequest::Bootstrap {
                                preferred,
                                connection,
                            }) if core.gateway_http_generation() == connection => {
                                if let Ok(reduction) = core.bootstrap_workspace_catalog(preferred) {
                                    core.load_selected_workspace_directory(
                                        &reduction.selected.workspace_id,
                                    );
                                }
                            }
                            Some(WorkspaceRequest::Refresh {
                                workspace,
                                connection,
                            }) if core.gateway_http_generation() == connection
                                && core.workspace_refresh_is_demanded(&workspace) =>
                            {
                                let _ = core.refresh_workspace_tree(&workspace);
                            }
                            Some(WorkspaceRequest::Command { intent, connection })
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
                            Some(_) => {}
                            None => break,
                        }
                    }
                }
            })
            .expect("workspace worker could not start");
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
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
        let Ok(directory) = self.refresh_workspace_tree(workspace) else {
            return;
        };
        if directory.error().is_some() {
            return;
        }
        self.restore_selected_workspace_thread(workspace);
    }
    fn restore_selected_workspace_thread(&self, workspace: &str) {
        if self
            .workspace_tree(workspace)
            .is_none_or(|directory| directory.is_loading() || directory.error().is_some())
        {
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
            },
        );
        if let Some(thread_id) = restored.active_thread_id {
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
                        "unread":[],"folders":[],"placements":[],"agents_docs":[]
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
            let _subscription = core.subscribe(
                ClientScope::WorkspaceTree {
                    workspace_id: Some("ws".into()),
                },
                NonZeroUsize::new(8).unwrap(),
            );
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
                    },
                )
                .unwrap();
            assert_eq!(loaded.snapshot().threads_by_id.len(), 1);
            assert_eq!(core.navigation_snapshot().workspace_id(), Some("ws"));
            accept();
            assert!(
                core.workspace_controller.lock().unwrap().refresh.is_empty(),
                "equal capabilities must not cause a reload loop"
            );
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
    fn repeated_hints_keep_one_follow_up_and_teardown_discards_pending_work() {
        let mut owner = WorkspaceController::default();
        for _ in 0..1000 {
            owner.refresh.insert("workspace".into(), Some(1));
        }
        assert_eq!(owner.refresh.len(), 1);
        assert!(matches!(
            owner.next(Instant::now()),
            Some(WorkspaceRequest::Refresh { .. })
        ));
        assert!(owner.next(Instant::now()).is_none());
        owner.refresh.insert("workspace".into(), Some(2));
        owner.stop();
        assert!(owner.next(Instant::now()).is_none());
    }
}
