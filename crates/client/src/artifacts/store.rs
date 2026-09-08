//! Thread-scoped artifact publications and the existing bounded list workflow.

use crate::core::*;
use pioneer_protocol::{ArtifactSummary, GatewayNotification};
use std::{
    collections::HashMap,
    sync::{Arc, mpsc},
    thread::JoinHandle,
};
use tokio_util::sync::CancellationToken;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArtifactIntent {
    Observe {
        thread_id: String,
    },
    Retry {
        thread_id: String,
    },
    BeginAction {
        thread_id: String,
        artifact_id: String,
        version_id: Option<String>,
        action: super::workflow::ArtifactActionKind,
    },
    ClaimPresentation {
        identity: super::workflow::ArtifactActionIdentity,
    },
    CompletePresentation {
        identity: super::workflow::ArtifactActionIdentity,
        error: Option<String>,
    },
    FailPreparation {
        identity: super::workflow::ArtifactActionIdentity,
        code: String,
    },
    CancelAction {
        identity: super::workflow::ArtifactActionIdentity,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArtifactReadState {
    #[default]
    Idle,
    Loading,
    Ready,
    Failed {
        message: String,
    },
    Cancelled,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize)]
pub struct ArtifactPublication {
    pub thread_id: String,
    pub workspace_id: String,
    pub revision: u64,
    pub generation: u64,
    pub items: Vec<ArtifactSummary>,
    pub request: ArtifactReadState,
    pub downloads: Vec<super::operations::ArtifactDownloadPublication>,
    pub actions: Vec<super::workflow::ArtifactActionPublication>,
    pub previews: Vec<super::preview_workflow::ArtifactPreviewPublication>,
}

#[derive(Clone)]
struct ArtifactReadRequest {
    thread_id: String,
    workspace_id: String,
    generation: u64,
    auth_ticket: (u64, Option<u64>),
    cancellation: CancellationToken,
}

#[derive(Default)]
pub(crate) struct ArtifactStore {
    generation: u64,
    pub(super) preview_requests: HashMap<u64, super::preview_workflow::ArtifactPreviewRequest>,
    pub(super) preview_sender:
        Option<mpsc::SyncSender<super::preview_workflow::ArtifactPreviewRequest>>,
    pub(super) preview_task: Option<JoinHandle<()>>,
    pub(super) publications: HashMap<String, Arc<ArtifactPublication>>,
    requests: HashMap<String, ArtifactReadRequest>,
    refresh_after_current: std::collections::HashSet<String>,
    subscriptions: HashMap<String, usize>,
    pub(super) suspended: std::collections::HashSet<String>,
    sender: Option<mpsc::SyncSender<ArtifactReadRequest>>,
    task: Option<JoinHandle<()>>,
}

impl ArtifactStore {
    pub(crate) fn stop(&mut self) {
        for request in self.requests.values() {
            request.cancellation.cancel();
        }
        self.requests.clear();
        for request in self.preview_requests.values() {
            request.cancellation.cancel();
        }
        self.preview_requests.clear();
        self.preview_sender.take();
        self.sender.take();
        self.publications.clear();
        self.subscriptions.clear();
        self.suspended.clear();
        self.refresh_after_current.clear();
    }
    pub(super) fn next_generation(&mut self) -> u64 {
        self.generation = self
            .generation
            .checked_add(1)
            .expect("artifact generation exhausted");
        self.generation
    }
}

impl Drop for ArtifactStore {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.preview_task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}

impl ClientCore {
    pub fn artifact_snapshot(&self, thread_id: &str) -> Option<Arc<ArtifactPublication>> {
        self.artifact_store
            .lock()
            .expect("artifact owner poisoned")
            .publications
            .get(thread_id)
            .cloned()
    }

    pub(super) fn publish_artifact(
        &self,
        owner: &mut ArtifactStore,
        mut next: ArtifactPublication,
    ) -> ClientTransition {
        let scope = ClientScope::Artifact {
            thread_id: next.thread_id.clone(),
        };
        next.revision = owner
            .publications
            .get(&next.thread_id)
            .map_or_else(
                || {
                    self.snapshot(&scope)
                        .map_or(0, |p| p.revisions().scoped().get())
                },
                |p| p.revision,
            )
            .checked_add(1)
            .expect("artifact revision exhausted");
        next.downloads = self.artifact_downloads_for_thread(&next.thread_id);
        let revision = next.revision;
        let next = Arc::new(next);
        owner
            .publications
            .insert(next.thread_id.clone(), next.clone());
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(revision),
            next,
            vec![],
        )
    }

    pub(crate) fn publish_artifact_downloads(&self, thread_id: Option<&str>) {
        let Some(thread_id) = thread_id else {
            return;
        };
        let mut owner = self.artifact_store.lock().expect("artifact owner poisoned");
        if let Some(current) = owner.publications.get(thread_id).cloned() {
            self.publish_artifact(&mut owner, (*current).clone());
        }
    }

    pub fn artifact_intent(&self, intent: ArtifactIntent) -> ClientTransition {
        let changed = match intent {
            ArtifactIntent::BeginAction {
                thread_id,
                artifact_id,
                version_id,
                action,
            } => return self.begin_artifact_action(thread_id, artifact_id, version_id, action),
            ArtifactIntent::ClaimPresentation { ref identity } => {
                Some(self.claim_artifact_presentation(identity))
            }
            ArtifactIntent::CompletePresentation {
                ref identity,
                ref error,
            } => Some(self.complete_artifact_presentation(identity, error.clone())),
            ArtifactIntent::FailPreparation {
                ref identity,
                ref code,
            } => Some(self.fail_artifact_preparation(identity, code.clone())),
            ArtifactIntent::CancelAction { ref identity } => {
                Some(self.cancel_artifact_action(identity))
            }
            _ => None,
        };
        if let Some(changed) = changed {
            return self.artifact_action_transition(changed);
        }
        let (thread_id, retry) = match intent {
            ArtifactIntent::Observe { thread_id } => (thread_id, false),
            ArtifactIntent::Retry { thread_id } => (thread_id, true),
            _ => unreachable!("action handled above"),
        };
        if self.is_stopped() {
            return self.reject_intent();
        }
        let Some(coordinator) = self.thread_coordinator_snapshot(&thread_id) else {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        };
        let Some(thread) = coordinator.thread() else {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        };
        if self.navigation_snapshot().draft(&thread.workspace_id) == Some(thread_id.as_str()) {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        }
        let ticket = self.current_auth_ticket();
        let authorization =
            self.authorization_snapshot(Some(&thread.workspace_id), Some(&thread_id));
        if ticket.1.is_none()
            || !authorization
                .as_ref()
                .and_then(|s| s.thread.as_ref())
                .is_some_and(|t| t.capabilities.can_read_artifacts)
        {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        }
        self.enqueue_artifact_read(thread_id, thread.workspace_id.clone(), ticket, retry)
    }

    fn enqueue_artifact_read(
        &self,
        thread_id: String,
        workspace_id: String,
        auth_ticket: (u64, Option<u64>),
        retry: bool,
    ) -> ClientTransition {
        let mut owner = self.artifact_store.lock().expect("artifact owner poisoned");
        if self.is_stopped() {
            return self.reject_intent();
        }
        let current = owner.publications.get(&thread_id).cloned();
        if current.as_ref().is_some_and(|p| {
            p.workspace_id == workspace_id && p.request == ArtifactReadState::Loading
        }) {
            if retry {
                owner.refresh_after_current.insert(thread_id);
            }
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        }
        if !retry
            && current.as_ref().is_some_and(|p| {
                p.workspace_id == workspace_id
                    && !matches!(
                        p.request,
                        ArtifactReadState::Idle | ArtifactReadState::Cancelled
                    )
            })
        {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        }
        let generation = owner.next_generation();
        let request = ArtifactReadRequest {
            thread_id: thread_id.clone(),
            workspace_id: workspace_id.clone(),
            generation,
            auth_ticket,
            cancellation: CancellationToken::new(),
        };
        owner.requests.insert(thread_id.clone(), request.clone());
        let next = ArtifactPublication {
            thread_id,
            workspace_id,
            revision: 0,
            generation,
            request: ArtifactReadState::Loading,
            previews: current
                .as_ref()
                .map_or_else(Vec::new, |p| p.previews.clone()),
            actions: current
                .as_ref()
                .map_or_else(Vec::new, |p| p.actions.clone()),
            items: current
                .filter(|p| p.workspace_id == request.workspace_id)
                .map_or_else(Vec::new, |p| p.items.clone()),
            downloads: vec![],
        };
        let transition = self.publish_artifact(&mut owner, next);
        if owner
            .sender
            .as_ref()
            .is_none_or(|sender| sender.try_send(request.clone()).is_err())
        {
            drop(owner);
            return self
                .complete_artifact_read(&request, Err("Artifact request unavailable".into()));
        }
        transition
    }

    fn artifact_read_matches(&self, request: &ArtifactReadRequest) -> bool {
        !self.is_stopped()
            && !request.cancellation.is_cancelled()
            && self.current_auth_ticket() == request.auth_ticket
            && self.artifact_snapshot(&request.thread_id).is_some_and(|p| {
                p.generation == request.generation
                    && p.workspace_id == request.workspace_id
                    && p.request == ArtifactReadState::Loading
            })
    }

    fn complete_artifact_read(
        &self,
        request: &ArtifactReadRequest,
        result: Result<Vec<ArtifactSummary>, String>,
    ) -> ClientTransition {
        let mut owner = self.artifact_store.lock().expect("artifact owner poisoned");
        let Some(current) = owner.publications.get(&request.thread_id).filter(|p| {
            p.generation == request.generation
                && p.workspace_id == request.workspace_id
                && p.request == ArtifactReadState::Loading
        }) else {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        };
        if self.is_stopped() || request.cancellation.is_cancelled() {
            return self.reject_intent();
        }
        let mut next = (**current).clone();
        match result {
            Ok(items) => {
                next.items = items;
                next.request = ArtifactReadState::Ready;
            }
            Err(message) => {
                next.request = ArtifactReadState::Failed { message };
            }
        }
        owner.requests.remove(&request.thread_id);
        let repeat = owner.refresh_after_current.remove(&request.thread_id);
        let transition = self.publish_artifact(&mut owner, next);
        drop(owner);
        if repeat {
            self.artifact_intent(ArtifactIntent::Retry {
                thread_id: request.thread_id.clone(),
            });
        }
        transition
    }

    pub(crate) fn invalidate_artifacts(&self, thread_id: Option<&str>) {
        self.cancel_artifact_previews(thread_id, true);
        let mut owner = self.artifact_store.lock().expect("artifact owner poisoned");
        let entries = owner
            .publications
            .values()
            .filter(|p| thread_id.is_none_or(|id| id == p.thread_id))
            .cloned()
            .collect::<Vec<_>>();
        for current in entries {
            if let Some(request) = owner.requests.remove(&current.thread_id) {
                request.cancellation.cancel();
            }
            owner.refresh_after_current.remove(&current.thread_id);
            let mut next = (*current).clone();
            next.generation = owner.next_generation();
            next.items.clear();
            next.actions.clear();
            next.request = ArtifactReadState::Cancelled;
            self.publish_artifact(&mut owner, next);
        }
    }

    pub(crate) fn artifact_subscription_changed(&self, scope: &ClientScope, added: bool) {
        let ClientScope::Artifact { thread_id } = scope else {
            return;
        };
        let mut owner = self.artifact_store.lock().expect("artifact owner poisoned");
        let count = owner.subscriptions.entry(thread_id.clone()).or_default();
        if added {
            *count += 1;
        } else {
            *count = count.saturating_sub(1);
        }
        let retire = *count == 0;
        if retire {
            owner.subscriptions.remove(thread_id);
            owner.suspended.remove(thread_id);
            owner.refresh_after_current.remove(thread_id);
            if let Some(request) = owner.requests.remove(thread_id) {
                request.cancellation.cancel();
                if let Some(current) = owner.publications.get(thread_id).cloned() {
                    let mut next = (*current).clone();
                    next.generation = owner.next_generation();
                    next.request = ArtifactReadState::Cancelled;
                    self.publish_artifact(&mut owner, next);
                }
            }
        }
        drop(owner);
        if retire {
            self.cancel_artifact_previews(Some(thread_id), false);
            self.cancel_artifact_actions(thread_id);
            self.cancel_artifact_downloads(Some(thread_id));
        }
    }

    pub(crate) fn artifact_demand_changed(&self, scope: &ClientScope, demand: ClientDemand) {
        let ClientScope::Artifact { thread_id } = scope else {
            return;
        };
        let mut owner = self.artifact_store.lock().expect("artifact owner poisoned");
        if demand == ClientDemand::Suspended {
            owner.suspended.insert(thread_id.clone());
            owner.refresh_after_current.remove(thread_id);
            if let Some(request) = owner.requests.remove(thread_id) {
                request.cancellation.cancel();
                if let Some(current) = owner.publications.get(thread_id).cloned() {
                    let mut next = (*current).clone();
                    next.generation = owner.next_generation();
                    next.request = ArtifactReadState::Cancelled;
                    self.publish_artifact(&mut owner, next);
                }
            }
            drop(owner);
            self.cancel_artifact_previews(Some(thread_id), false);
            self.cancel_artifact_actions(thread_id);
            self.cancel_artifact_downloads(Some(thread_id));
        } else {
            owner.suspended.remove(thread_id);
            drop(owner);
            self.artifact_intent(ArtifactIntent::Observe {
                thread_id: thread_id.clone(),
            });
        }
    }

    pub(crate) fn observe_artifact_notification(&self, notification: &GatewayNotification) {
        let (workspace, thread) = match notification {
            GatewayNotification::ThreadArtifactsChanged(n) => {
                (n.workspace_id.as_str(), Some(n.thread_id.as_str()))
            }
            GatewayNotification::ArtifactCreated(n) => (n.workspace_id.as_str(), None),
            GatewayNotification::ArtifactUpdated(n) => (n.workspace_id.as_str(), None),
            GatewayNotification::ArtifactDeleted(n) => (n.workspace_id.as_str(), None),
            GatewayNotification::ArtifactProjectionUpdated(n) => (n.workspace_id.as_str(), None),
            _ => return,
        };
        let threads = {
            let owner = self.artifact_store.lock().expect("artifact owner poisoned");
            owner
                .publications
                .values()
                .filter(|p| {
                    p.workspace_id == workspace
                        && thread.is_none_or(|id| id == p.thread_id)
                        && owner.subscriptions.contains_key(&p.thread_id)
                        && !owner.suspended.contains(&p.thread_id)
                })
                .map(|p| p.thread_id.clone())
                .collect::<Vec<_>>()
        };
        for thread_id in threads {
            self.artifact_intent(ArtifactIntent::Retry { thread_id });
        }
    }

    pub(crate) fn start_artifact_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<ArtifactReadRequest>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new().name("client-artifacts".into()).spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build().expect("artifact retry timer unavailable");
            while let Ok(request) = receiver.recv() {
                let Some(core) = weak.upgrade() else { return; };
                if !core.artifact_read_matches(&request) { core.complete_artifact_read(&request, Err("Artifact request cancelled".into())); continue; }
                let sender = core.compatibility_runtime().ws_command_sender();
                drop(core);
                let current = || weak.upgrade().is_some_and(|core| core.artifact_read_matches(&request));
                let result = load_artifacts(&request, &sender, current, |delay| runtime.block_on(async {
                    tokio::select! { _ = request.cancellation.cancelled() => false, _ = tokio::time::sleep(delay) => true }
                }));
                let Some(core) = weak.upgrade() else { return; };
                if core.artifact_read_matches(&request) { core.complete_artifact_read(&request, result.map_err(|error| format!("{error:#}"))); } else { core.complete_artifact_read(&request, Err("Artifact request cancelled".into())); }
            }
        }).expect("artifact worker unavailable");
        let mut owner = self.artifact_store.lock().expect("artifact owner poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

fn load_artifacts(
    request: &ArtifactReadRequest,
    transport: &impl crate::rpc::JsonRpcRequestTransport,
    current: impl Fn() -> bool,
    wait: impl Fn(std::time::Duration) -> bool,
) -> anyhow::Result<Vec<ArtifactSummary>> {
    let mut attempt = 0;
    loop {
        anyhow::ensure!(current(), "Artifact request cancelled");
        let result = crate::transport::ws::command_sender::artifact_list_for_thread(
            transport,
            super::state::artifact_list_for_thread_params(
                &request.workspace_id,
                &request.thread_id,
            ),
        );
        anyhow::ensure!(current(), "Artifact request cancelled");
        match result {
            Ok(response) => {
                anyhow::ensure!(
                    response
                        .items
                        .iter()
                        .all(|item| item.workspace_id == request.workspace_id),
                    "Artifact response scope mismatch"
                );
                return Ok(response.items);
            }
            Err(error)
                if super::state::is_artifact_thread_not_found_error(
                    &request.thread_id,
                    &format!("{error:#}"),
                ) && attempt
                    < super::state::THREAD_ARTIFACTS_TRANSIENT_RETRY_DELAYS_MS.len() =>
            {
                let delay = std::time::Duration::from_millis(
                    super::state::THREAD_ARTIFACTS_TRANSIENT_RETRY_DELAYS_MS[attempt],
                );
                attempt += 1;
                anyhow::ensure!(wait(delay), "Artifact request cancelled");
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};

    struct Transport {
        calls: Cell<usize>,
        result: Result<serde_json::Value, String>,
    }
    impl crate::rpc::JsonRpcRequestTransport for Transport {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            reply: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(request["method"], "artifact/list/thread");
            assert_eq!(
                request["params"]["limit"],
                super::super::state::THREAD_ARTIFACT_LIST_LIMIT
            );
            self.calls.set(self.calls.get() + 1);
            match &self.result {
                Ok(value) => reply.send(Ok(value.clone())).map_err(|e| e.to_string()),
                Err(error) => Err(error.clone()),
            }
        }
    }
    fn enqueue(core: &ClientCore, id: &str, retry: bool) {
        core.enqueue_artifact_read(id.into(), "workspace".into(), (0, None), retry);
    }

    #[test]
    fn transient_not_found_uses_the_existing_five_delays_and_stops_until_retry() {
        let core = Arc::new(ClientCore::new());
        let (sender, receiver) = mpsc::sync_channel(2);
        core.artifact_store.lock().unwrap().sender = Some(sender);
        enqueue(&core, "a", false);
        let request = receiver.try_recv().unwrap();
        let transport = Transport {
            calls: Cell::new(0),
            result: Err("thread `a` not found".into()),
        };
        let waits = RefCell::new(vec![]);
        let result = load_artifacts(
            &request,
            &transport,
            || true,
            |delay| {
                waits.borrow_mut().push(delay.as_millis());
                true
            },
        );
        assert!(result.is_err());
        assert_eq!(transport.calls.get(), 6);
        assert_eq!(&*waits.borrow(), &[250, 500, 1000, 2000, 4000]);
        core.complete_artifact_read(&request, result.map_err(|e| e.to_string()));
        let failed = core.artifact_snapshot("a").unwrap();
        assert!(matches!(failed.request, ArtifactReadState::Failed { .. }));
        for _ in 0..20 {
            enqueue(&core, "a", false);
        }
        assert!(receiver.try_recv().is_err());
        assert!(Arc::ptr_eq(&failed, &core.artifact_snapshot("a").unwrap()));
        enqueue(&core, "a", true);
        let retry = receiver.try_recv().unwrap();
        assert!(retry.generation > request.generation);
        let pending = core.artifact_snapshot("a").unwrap();
        core.complete_artifact_read(&request, Ok(vec![]));
        assert!(Arc::ptr_eq(&pending, &core.artifact_snapshot("a").unwrap()));
        assert!(load_artifacts(&retry, &transport, || true, |_| false).is_err());
        assert_eq!(transport.calls.get(), 7);
    }

    #[test]
    fn route_drop_cancels_only_its_read_and_late_data_cannot_replace_another_thread() {
        let core = Arc::new(ClientCore::new());
        let subscription = core.subscribe(
            ClientScope::Artifact {
                thread_id: "a".into(),
            },
            std::num::NonZeroUsize::new(8).unwrap(),
        );
        let (sender, receiver) = mpsc::sync_channel(2);
        core.artifact_store.lock().unwrap().sender = Some(sender);
        enqueue(&core, "a", false);
        let a = receiver.try_recv().unwrap();
        enqueue(&core, "b", false);
        let b = receiver.try_recv().unwrap();
        let before_b = core.artifact_snapshot("b").unwrap();
        drop(subscription);
        assert!(a.cancellation.is_cancelled());
        let retired = core.artifact_snapshot("a").unwrap();
        core.complete_artifact_read(&a, Ok(vec![]));
        assert!(Arc::ptr_eq(&retired, &core.artifact_snapshot("a").unwrap()));
        assert!(Arc::ptr_eq(
            &before_b,
            &core.artifact_snapshot("b").unwrap()
        ));
        let transport = Transport {
            calls: Cell::new(0),
            result: Ok(serde_json::json!({"items": []})),
        };
        let output = load_artifacts(
            &b,
            &transport,
            || true,
            |_| panic!("success must not retry"),
        )
        .unwrap();
        core.complete_artifact_read(&b, Ok(output));
        let ready = core.artifact_snapshot("b").unwrap();
        assert_eq!(ready.request, ArtifactReadState::Ready);
        core.complete_artifact_read(&b, Err("duplicate failure".into()));
        assert!(Arc::ptr_eq(&ready, &core.artifact_snapshot("b").unwrap()));
        core.invalidate_artifacts(None);
        assert!(core.artifact_snapshot("b").unwrap().items.is_empty());
    }

    #[test]
    fn suspension_cancels_read_and_transfer_and_publishes_the_terminal_state_once() {
        use super::super::operations::{ArtifactDownloadState, ArtifactDownloadTarget};
        let core = Arc::new(ClientCore::new());
        let (sender, receiver) = mpsc::sync_channel(2);
        core.artifact_store.lock().unwrap().sender = Some(sender);
        enqueue(&core, "a", false);
        let request = receiver.try_recv().unwrap();
        let download = core
            .begin_artifact_download_for_target(ArtifactDownloadTarget {
                thread_id: Some("a".into()),
                workspace_id: "workspace".into(),
                artifact_id: "artifact".into(),
                version_id: Some("version".into()),
            })
            .unwrap();
        core.artifact_demand_changed(
            &ClientScope::Artifact {
                thread_id: "a".into(),
            },
            ClientDemand::Suspended,
        );
        assert!(request.cancellation.is_cancelled());
        assert!(download.cancellation().is_cancelled());
        let cancelled = core.artifact_snapshot("a").unwrap();
        assert_eq!(cancelled.request, ArtifactReadState::Cancelled);
        assert_eq!(
            cancelled.downloads[0].state,
            ArtifactDownloadState::Cancelled
        );
        core.cancel_artifact_downloads(Some("a"));
        core.complete_artifact_read(&request, Ok(vec![]));
        assert!(!download.finish(ArtifactDownloadState::Completed, None));
        assert!(Arc::ptr_eq(
            &cancelled,
            &core.artifact_snapshot("a").unwrap()
        ));
    }

    #[test]
    fn download_progress_publishes_only_its_thread_and_equal_progress_is_a_noop() {
        use super::super::operations::ArtifactDownloadTarget;
        let core = Arc::new(ClientCore::new());
        let (sender, _receiver) = mpsc::sync_channel(2);
        core.artifact_store.lock().unwrap().sender = Some(sender);
        enqueue(&core, "a", false);
        enqueue(&core, "b", false);
        let b = core.artifact_snapshot("b").unwrap();
        let download = core
            .begin_artifact_download_for_target(ArtifactDownloadTarget {
                thread_id: Some("a".into()),
                workspace_id: "workspace".into(),
                artifact_id: "artifact".into(),
                version_id: Some("version".into()),
            })
            .unwrap();
        let progress = super::super::http_download::ArtifactHttpDownloadProgress {
            downloaded_bytes: 7,
            total_bytes: 10,
            resumed_from_bytes: 3,
        };
        download.update_progress(progress);
        let a = core.artifact_snapshot("a").unwrap();
        assert_eq!(a.downloads[0].downloaded_bytes, 7);
        assert_eq!(a.downloads[0].identity, *download.identity());
        download.update_progress(progress);
        assert!(Arc::ptr_eq(&a, &core.artifact_snapshot("a").unwrap()));
        assert!(Arc::ptr_eq(&b, &core.artifact_snapshot("b").unwrap()));
    }
}
