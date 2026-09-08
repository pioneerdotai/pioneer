//! Voice availability for a mounted draft. Native capture remains at the platform port.
use super::store::{ComposerOperationIdentity, ComposerOperationKind, ComposerStore, DraftId};
use crate::core::{ClientCore, ClientMutationAuthority, ClientTransition};
use pioneer_protocol::{GatewayNotification, VoiceStatus, VoiceStatusParams, VoiceStatusResponse};
use std::{
    collections::BTreeMap,
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComposerVoiceReadinessDemand {
    Suspended,
    UntilReady,
    WhileVisible,
}
impl ComposerVoiceReadinessDemand {
    fn delay(self, ready: bool) -> Option<Duration> {
        match self {
            Self::UntilReady if !ready => Some(Duration::from_secs(5)),
            Self::WhileVisible => Some(Duration::from_secs(15)),
            _ => None,
        }
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ComposerVoiceReadinessPublication {
    pub identity: ComposerOperationIdentity,
    pub demand: ComposerVoiceReadinessDemand,
    pub loading: bool,
    pub response: Option<VoiceStatusResponse>,
    pub error: Option<String>,
}
#[derive(Clone)]
pub(super) struct VoiceReadinessRequest {
    identity: ComposerOperationIdentity,
    workspace: String,
    token: crate::threads::registry::ThreadOperationToken,
    auth: (u64, Option<u64>),
    demand: ComposerVoiceReadinessDemand,
    completion_pending: bool,
}
#[derive(Default)]
pub(crate) struct ComposerVoiceReadinessController {
    sender: Option<mpsc::SyncSender<VoiceReadinessRequest>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl ComposerVoiceReadinessController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
    }
}
impl Drop for ComposerVoiceReadinessController {
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
    pub(super) fn set_composer_voice_readiness_demand(
        &self,
        thread: &str,
        draft: DraftId,
        demand: ComposerVoiceReadinessDemand,
    ) -> ClientTransition {
        let unchanged =
            || self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        if demand == ComposerVoiceReadinessDemand::Suspended {
            return self
                .cancel_composer_voice_readiness(thread, Some(draft))
                .unwrap_or_else(unchanged);
        }
        let auth = self.current_auth_ticket();
        let Some(coordinator) = self.thread_coordinator_snapshot(thread) else {
            return self.reject_intent();
        };
        let workspace = coordinator.workspace_id.clone();
        if self.is_stopped() || auth.1.is_none() {
            return self.reject_intent();
        }
        self.enqueue_composer_voice_readiness(thread, draft, workspace, auth, demand)
    }
    fn enqueue_composer_voice_readiness(
        &self,
        thread: &str,
        draft: DraftId,
        workspace: String,
        auth: (u64, Option<u64>),
        demand: ComposerVoiceReadinessDemand,
    ) -> ClientTransition {
        let unchanged =
            || self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        let Some(token) = self.thread_operation_token(thread) else {
            return self.reject_intent();
        };
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        let Some(current) = store
            .drafts
            .get(thread)
            .filter(|p| p.draft_id() == draft)
            .cloned()
        else {
            return self.reject_intent();
        };
        if store.suspended.contains(thread) {
            return self.reject_intent();
        }
        if store.voice_readiness.get(thread).is_some_and(|request| {
            request.identity.draft_id == draft
                && request.auth == auth
                && request.demand == demand
                && request.workspace == workspace
        }) {
            return unchanged();
        }
        if !store.voice_readiness.contains_key(thread) && store.voice_readiness.len() >= 64 {
            return self.reject_intent();
        }
        store.next_operation = store
            .next_operation
            .checked_add(1)
            .expect("voice readiness generation exhausted");
        let identity = ComposerOperationIdentity {
            thread_id: thread.into(),
            draft_id: draft,
            generation: store.next_operation,
        };
        let request = VoiceReadinessRequest {
            identity: identity.clone(),
            workspace,
            token,
            auth,
            demand,
            completion_pending: true,
        };
        store.voice_readiness.insert(thread.into(), request.clone());
        let mut next = (*current).clone();
        next.voice_readiness = Some(ComposerVoiceReadinessPublication {
            identity,
            demand,
            loading: true,
            response: None,
            error: None,
        });
        let transition = self.publish_composer_model_display(&mut store, next);
        drop(store);
        if !self
            .composer_voice_readiness
            .lock()
            .expect("voice readiness poisoned")
            .sender
            .as_ref()
            .is_some_and(|sender| sender.try_send(request.clone()).is_ok())
        {
            self.complete_composer_voice_readiness(
                &request,
                Err("Voice readiness queue unavailable".into()),
            );
        }
        transition
    }
    fn voice_readiness_matches(
        &self,
        store: &ComposerStore,
        request: &VoiceReadinessRequest,
    ) -> bool {
        !self.is_stopped()
            && self.current_auth_ticket() == request.auth
            && self.thread_operation_token(&request.identity.thread_id)
                == Some(request.token.clone())
            && !store.suspended.contains(&request.identity.thread_id)
            && store
                .voice_readiness
                .get(&request.identity.thread_id)
                .is_some_and(|current| current.identity == request.identity)
            && store
                .drafts
                .get(&request.identity.thread_id)
                .is_some_and(|draft| draft.draft_id() == request.identity.draft_id)
            && self
                .thread_coordinator_snapshot(&request.identity.thread_id)
                .is_some_and(|thread| thread.workspace_id == request.workspace)
    }
    fn begin_voice_readiness_poll(&self, request: &mut VoiceReadinessRequest) -> bool {
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        if !self.voice_readiness_matches(&store, request) {
            return false;
        }
        if store
            .voice_readiness
            .get(&request.identity.thread_id)
            .unwrap()
            .completion_pending
        {
            return true;
        }
        let mut next = (**store.drafts.get(&request.identity.thread_id).unwrap()).clone();
        if request.demand == ComposerVoiceReadinessDemand::UntilReady
            && next
                .voice_readiness
                .as_ref()
                .and_then(|p| p.response.as_ref())
                .is_some_and(|p| p.status == VoiceStatus::Ready)
        {
            return false;
        }
        store.next_operation = store
            .next_operation
            .checked_add(1)
            .expect("voice readiness generation exhausted");
        request.identity.generation = store.next_operation;
        request.completion_pending = true;
        store
            .voice_readiness
            .insert(request.identity.thread_id.clone(), request.clone());
        next.voice_readiness.as_mut().unwrap().identity = request.identity.clone();
        self.publish_composer_model_display(&mut store, next);
        true
    }
    fn complete_composer_voice_readiness(
        &self,
        request: &VoiceReadinessRequest,
        response: Result<VoiceStatusResponse, String>,
    ) -> Option<Duration> {
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        if !self.voice_readiness_matches(&store, request) {
            return None;
        }
        let pending = &mut store
            .voice_readiness
            .get_mut(&request.identity.thread_id)?
            .completion_pending;
        if !*pending {
            return None;
        }
        *pending = false;
        let mut next = (**store.drafts.get(&request.identity.thread_id).unwrap()).clone();
        let publication = next.voice_readiness.as_mut()?;
        let ready = response
            .as_ref()
            .is_ok_and(|response| response.status == VoiceStatus::Ready);
        publication.loading = false;
        match response {
            Ok(response) => {
                publication.response = Some(response);
                publication.error = None;
            }
            Err(error) => {
                publication.response = None;
                publication.error = Some(error);
            }
        }
        if next != **store.drafts.get(&request.identity.thread_id).unwrap() {
            self.publish_composer_model_display(&mut store, next);
        }
        request.demand.delay(ready)
    }
    pub(super) fn cancel_composer_voice_readiness(
        &self,
        thread: &str,
        draft: Option<DraftId>,
    ) -> Option<ClientTransition> {
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        let current = store.drafts.get(thread)?.clone();
        if draft.is_some_and(|draft| current.draft_id() != draft) {
            return None;
        }
        store.voice_readiness.remove(thread);
        let mut next = (*current).clone();
        let publication = next.voice_readiness.as_mut()?;
        if publication.demand == ComposerVoiceReadinessDemand::Suspended {
            return None;
        }
        publication.demand = ComposerVoiceReadinessDemand::Suspended;
        publication.loading = false;
        publication.response = None;
        publication.error = None;
        Some(self.publish_composer_model_display(&mut store, next))
    }
    pub(super) fn reconcile_composer_voice_readiness_draft(&self, thread: &str) {
        let changed = {
            let store = self.composer_store.lock().expect("composer store poisoned");
            store
                .voice_readiness
                .get(thread)
                .zip(store.drafts.get(thread))
                .is_some_and(|(request, draft)| request.identity.draft_id != draft.draft_id())
        };
        if changed {
            self.refresh_composer_voice_readiness(thread);
        }
    }
    pub(super) fn refresh_composer_voice_readiness(&self, thread: &str) {
        let previous = self
            .composer_store
            .lock()
            .expect("composer store poisoned")
            .voice_readiness
            .get(thread)
            .cloned();
        let Some(previous) = previous else {
            return;
        };
        let Some(current) = self.composer_snapshot(thread) else {
            return;
        };
        self.cancel_composer_voice_readiness(thread, None);
        self.set_composer_voice_readiness_demand(thread, current.draft_id(), previous.demand);
    }
    pub(crate) fn observe_composer_voice_readiness_notification(
        &self,
        notification: &GatewayNotification,
    ) {
        let GatewayNotification::GatewayVoiceInputStatusChanged(change) = notification else {
            return;
        };
        let requests: Vec<_> = self
            .composer_store
            .lock()
            .expect("composer store poisoned")
            .voice_readiness
            .values()
            .cloned()
            .collect();
        for previous in requests {
            let thread = &previous.identity.thread_id;
            self.cancel_composer_voice_readiness(thread, Some(previous.identity.draft_id));
            self.set_composer_voice_readiness_demand(
                thread,
                previous.identity.draft_id,
                previous.demand,
            );
            let request = self
                .composer_store
                .lock()
                .expect("composer store poisoned")
                .voice_readiness
                .get(thread)
                .cloned();
            if let Some(request) = request {
                self.complete_composer_voice_readiness(
                    &request,
                    Ok(VoiceStatusResponse {
                        status: change.settings.runtime.phase.coarse_voice_status(),
                        active_session_id: None,
                        error: None,
                    }),
                );
            }
        }
    }
    pub(crate) fn start_composer_voice_readiness_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<VoiceReadinessRequest>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-composer-voice-readiness".into())
            .spawn(move || {
                let mut scheduled: BTreeMap<String, (Instant, VoiceReadinessRequest)> =
                    BTreeMap::new();
                loop {
                    let delay = scheduled
                        .values()
                        .map(|(deadline, _)| deadline.saturating_duration_since(Instant::now()))
                        .min();
                    let message = match delay {
                        Some(delay) => receiver.recv_timeout(delay),
                        None => receiver
                            .recv()
                            .map_err(|_| mpsc::RecvTimeoutError::Disconnected),
                    };
                    match message {
                        Ok(request) => {
                            if scheduled.len() < 64
                                || scheduled.contains_key(&request.identity.thread_id)
                            {
                                scheduled.insert(
                                    request.identity.thread_id.clone(),
                                    (Instant::now(), request),
                                );
                            }
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => return,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    scheduled.retain(|_, (_, request)| {
                        let store = core.composer_store.lock().expect("composer store poisoned");
                        core.voice_readiness_matches(&store, request)
                    });
                    let next = scheduled
                        .iter()
                        .filter(|(_, (at, _))| *at <= Instant::now())
                        .min_by_key(|(_, (at, _))| *at)
                        .map(|(id, _)| id.clone());
                    let Some(id) = next else {
                        continue;
                    };
                    let (_, mut request) = scheduled.remove(&id).unwrap();
                    let capturing = core
                        .composer_snapshot(&id)
                        .and_then(|p| p.operation().cloned())
                        .is_some_and(|op| op.kind == ComposerOperationKind::Voice && op.pending());
                    if capturing {
                        scheduled.insert(id, (Instant::now() + Duration::from_secs(5), request));
                        continue;
                    }
                    if !core.begin_voice_readiness_poll(&mut request) {
                        continue;
                    }
                    let sender = core.compatibility_runtime().ws_command_sender();
                    drop(core);
                    let response = sender
                        .voice_status(VoiceStatusParams {
                            workspace_id: Some(request.workspace.clone()),
                        })
                        .map_err(|error| format!("{error:#}"));
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if let Some(delay) = core.complete_composer_voice_readiness(&request, response)
                    {
                        scheduled.insert(id, (Instant::now() + delay, request));
                    }
                }
            })
            .expect("composer voice readiness worker");
        let mut owner = self
            .composer_voice_readiness
            .lock()
            .expect("voice readiness poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composer::store::{ComposerIntent, ComposerOperationCompletion};
    use crate::core::{ClientDemand, ClientScope, ClientTransitionOutcome};

    fn fixture() -> (ClientCore, mpsc::Receiver<VoiceReadinessRequest>, DraftId) {
        let core = ClientCore::new();
        core.upsert_thread(
            serde_json::from_value(serde_json::json!({
                "workspace_id":"ws", "id":"a", "preview":"", "mode":"Message",
                "model":"", "model_provider":"", "created_at":1,"updated_at":1,
                "status":"Idle","origin_kind":"user","sidebar_visibility":"visible","turns":[]
            }))
            .unwrap(),
        );
        core.composer_intent(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: Default::default(),
        });
        let draft = core.composer_snapshot("a").unwrap().draft_id();
        let (sender, receiver) = mpsc::sync_channel(64);
        core.composer_voice_readiness.lock().unwrap().sender = Some(sender);
        (core, receiver, draft)
    }
    fn observe(
        core: &ClientCore,
        draft: DraftId,
        demand: ComposerVoiceReadinessDemand,
    ) -> ClientTransition {
        // Synthetic transport-free ingress to the same request owner. Public ingress
        // additionally requires authenticated HTTP authority.
        core.enqueue_composer_voice_readiness(
            "a",
            draft,
            "ws".into(),
            core.current_auth_ticket(),
            demand,
        )
    }
    fn ready() -> Result<VoiceStatusResponse, String> {
        Ok(VoiceStatusResponse {
            status: VoiceStatus::Ready,
            active_session_id: None,
            error: None,
        })
    }
    #[test]
    fn readiness_preserves_existing_observation_cadences() {
        assert_eq!(
            ComposerVoiceReadinessDemand::UntilReady.delay(false),
            Some(Duration::from_secs(5))
        );
        assert_eq!(ComposerVoiceReadinessDemand::UntilReady.delay(true), None);
        assert_eq!(
            ComposerVoiceReadinessDemand::WhileVisible.delay(false),
            Some(Duration::from_secs(15))
        );
        assert_eq!(
            ComposerVoiceReadinessDemand::WhileVisible.delay(true),
            Some(Duration::from_secs(15))
        );
        assert_eq!(ComposerVoiceReadinessDemand::Suspended.delay(false), None);
    }
    #[test]
    fn equal_observation_and_duplicate_completion_do_not_publish_or_enqueue() {
        let (core, receiver, draft) = fixture();
        observe(&core, draft, ComposerVoiceReadinessDemand::UntilReady);
        let request = receiver.recv().unwrap();
        let before = core.composer_snapshot("a").unwrap();
        assert_eq!(
            observe(&core, draft, ComposerVoiceReadinessDemand::UntilReady).outcome(),
            ClientTransitionOutcome::Noop
        );
        assert!(receiver.try_recv().is_err());
        assert!(Arc::ptr_eq(&before, &core.composer_snapshot("a").unwrap()));
        assert_eq!(
            core.complete_composer_voice_readiness(&request, ready()),
            None
        );
        let ready = core.composer_snapshot("a").unwrap();
        core.complete_composer_voice_readiness(&request, Err("late duplicate".into()));
        assert!(Arc::ptr_eq(&ready, &core.composer_snapshot("a").unwrap()));
    }
    #[test]
    fn periodic_attempt_advances_generation_and_rejects_previous_response() {
        let (core, receiver, draft) = fixture();
        observe(&core, draft, ComposerVoiceReadinessDemand::WhileVisible);
        let first = receiver.recv().unwrap();
        assert_eq!(
            core.complete_composer_voice_readiness(&first, ready()),
            Some(Duration::from_secs(15))
        );
        let mut second = first.clone();
        assert!(core.begin_voice_readiness_poll(&mut second));
        assert!(second.identity.generation > first.identity.generation);
        let before = core.composer_snapshot("a").unwrap();
        core.complete_composer_voice_readiness(&first, Err("stale failure".into()));
        assert!(Arc::ptr_eq(&before, &core.composer_snapshot("a").unwrap()));
        core.complete_composer_voice_readiness(&second, Err("current failure".into()));
        assert_eq!(
            core.composer_snapshot("a")
                .unwrap()
                .voice_readiness()
                .unwrap()
                .error
                .as_deref(),
            Some("current failure")
        );
    }
    #[test]
    fn scope_draft_and_shutdown_retirement_reject_late_readiness() {
        for retirement in 0..4 {
            let (core, receiver, draft) = fixture();
            observe(&core, draft, ComposerVoiceReadinessDemand::WhileVisible);
            let request = receiver.recv().unwrap();
            match retirement {
                0 => {
                    core.cancel_composer_voice_readiness("a", Some(draft));
                }
                1 => {
                    core.composer_intent(ComposerIntent::Clear {
                        thread_id: "a".into(),
                        draft_id: draft,
                    });
                }
                2 => core.composer_demand_changed(
                    &ClientScope::Composer {
                        thread_id: "a".into(),
                    },
                    ClientDemand::Suspended,
                ),
                _ => core.shutdown(),
            }
            let before = core.composer_snapshot("a");
            core.complete_composer_voice_readiness(&request, ready());
            assert_eq!(before.as_deref(), core.composer_snapshot("a").as_deref());
        }
    }
    #[test]
    fn read_failure_preserves_draft_and_does_not_change_capture_operation() {
        let (core, receiver, draft) = fixture();
        core.composer_intent(ComposerIntent::EditText {
            thread_id: "a".into(),
            draft_id: draft,
            text: "keep text".into(),
        });
        observe(&core, draft, ComposerVoiceReadinessDemand::UntilReady);
        let request = receiver.recv().unwrap();
        core.complete_composer_voice_readiness(&request, Err("unavailable".into()));
        let input = core.composer_snapshot("a").unwrap();
        assert_eq!(input.draft().text, "keep text");
        assert!(input.operation().is_none());
        assert!(
            !core.complete_composer_operation(
                request.identity,
                ComposerOperationCompletion::Cancelled
            )
        );
        assert_eq!(
            core.composer_snapshot("a").unwrap().draft().text,
            "keep text"
        );
    }
}
