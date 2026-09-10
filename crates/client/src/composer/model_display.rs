//! Selected-model metadata requests belong to the draft owner, not a render callback.
use super::store::{ComposerOperationIdentity, ComposerPublication, DraftId};
use crate::{
    core::{ClientCore, ClientMutationAuthority, ClientScope, ClientTransition},
    providers::presentation::{self, ProviderModelDisplayKey},
};
use pioneer_protocol::ProviderListModelsResponse;
use std::{
    collections::HashMap,
    sync::{Arc, mpsc},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ComposerModelDisplayRequestState {
    Loading,
    Ready,
    Failed { message: String },
    Cancelled,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ComposerModelDisplayPublication {
    pub identity: ComposerOperationIdentity,
    pub key: ProviderModelDisplayKey,
    pub reasoning_effort: Option<String>,
    pub request: ComposerModelDisplayRequestState,
    pub label: Option<String>,
    pub reasoning_effort_label: Option<String>,
}

#[derive(Clone)]
struct ModelRequest {
    retry: bool,
    publication: ComposerModelDisplayPublication,
    auth_ticket: (u64, Option<u64>),
}
enum ModelWork {
    Resolve(ModelRequest),
    SelectionChanged,
}

#[derive(Default)]
pub(crate) struct ComposerModelDisplayController {
    sender: Option<mpsc::SyncSender<ModelWork>>,
    selection_pending: bool,
    in_flight: HashMap<String, (ComposerOperationIdentity, (u64, Option<u64>))>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl ComposerModelDisplayController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.in_flight.clear();
    }
}
impl Drop for ComposerModelDisplayController {
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
    pub(super) fn observe_composer_model_display(
        &self,
        thread_id: &str,
        retry: Option<DraftId>,
    ) -> Option<ClientTransition> {
        let ticket = self.current_auth_ticket();
        if ticket.1.is_none() {
            return None;
        }
        let workspace = self
            .thread_coordinator_snapshot(thread_id)
            .map(|t| t.workspace_id.clone());
        self.enqueue_composer_model_display(thread_id, workspace.as_deref(), ticket, retry)
    }

    fn enqueue_composer_model_display(
        &self,
        thread_id: &str,
        workspace: Option<&str>,
        ticket: (u64, Option<u64>),
        retry: Option<DraftId>,
    ) -> Option<ClientTransition> {
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        let Some(current) = store.drafts.get(thread_id).cloned() else {
            return None;
        };
        if self.is_stopped()
            || store.suspended.contains(thread_id)
            || retry.is_some_and(|id| id != current.draft_id())
        {
            return None;
        }
        let key = presentation::provider_model_display_key(
            workspace,
            current.domain().selected_provider.as_deref(),
            current.domain().selected_model.as_deref(),
        );
        let Some(key) = key else {
            if current.model_display.is_none() {
                return None;
            }
            let mut next = (*current).clone();
            next.model_display = None;
            return Some(self.publish_composer_model_display(&mut store, next));
        };
        let effort = current.domain().selected_reasoning_effort.clone();
        if current.model_display.as_ref().is_some_and(|display| {
            display.identity.draft_id == current.draft_id()
                && display.key == key
                && display.reasoning_effort == effort
                && display.request != ComposerModelDisplayRequestState::Cancelled
                && (display.request != ComposerModelDisplayRequestState::Loading
                    || self
                        .composer_models
                        .lock()
                        .expect("composer models poisoned")
                        .in_flight
                        .get(thread_id)
                        .is_some_and(|(identity, auth)| {
                            *identity == display.identity && *auth == ticket
                        }))
                && (retry.is_none() || display.request == ComposerModelDisplayRequestState::Loading)
        }) {
            return None;
        }
        store.next_operation = store
            .next_operation
            .checked_add(1)
            .expect("composer model generation exhausted");
        let publication = ComposerModelDisplayPublication {
            identity: ComposerOperationIdentity {
                thread_id: thread_id.into(),
                draft_id: current.draft_id(),
                generation: store.next_operation,
            },
            key,
            reasoning_effort: effort,
            request: ComposerModelDisplayRequestState::Loading,
            label: None,
            reasoning_effort_label: None,
        };
        let request = ModelRequest {
            retry: retry.is_some(),
            publication: publication.clone(),
            auth_ticket: ticket,
        };
        self.composer_models
            .lock()
            .expect("composer models poisoned")
            .in_flight
            .insert(
                thread_id.to_owned(),
                (request.publication.identity.clone(), ticket),
            );
        let mut next = (*current).clone();
        next.model_display = Some(publication);
        let transition = self.publish_composer_model_display(&mut store, next);
        drop(store);
        let queued = self
            .composer_models
            .lock()
            .expect("composer models poisoned")
            .sender
            .as_ref()
            .is_some_and(|sender| sender.try_send(ModelWork::Resolve(request.clone())).is_ok());
        if !queued {
            self.complete_composer_model_display(
                request,
                Err("Model request queue unavailable".into()),
            );
        }
        Some(transition)
    }

    pub(super) fn publish_composer_model_display(
        &self,
        store: &mut super::store::ComposerStore,
        mut next: ComposerPublication,
    ) -> ClientTransition {
        next.revision = next
            .revision
            .checked_add(1)
            .expect("composer revision exhausted");
        next.refresh_runtime_readiness();
        let next = Arc::new(next);
        store.drafts.insert(next.thread_id().into(), next.clone());
        self.publish(
            &ClientMutationAuthority { _private: () },
            ClientScope::Composer {
                thread_id: next.thread_id().into(),
            },
            crate::core::ClientRevisions::new(
                crate::core::DomainRevision::new(next.revision()),
                crate::core::PresentationRevision::new(next.revision()),
                crate::core::ContentRevision::ZERO,
                crate::core::ScopedRevision::new(next.revision()),
            ),
            next,
            vec![],
        )
    }

    fn composer_model_request_is_current(&self, request: &ModelRequest) -> bool {
        if self.is_stopped() || self.current_auth_ticket() != request.auth_ticket {
            return false;
        }
        let store = self.composer_store.lock().expect("composer store poisoned");
        let id = &request.publication.identity;
        !store.suspended.contains(&id.thread_id)
            && store.drafts.get(&id.thread_id).is_some_and(|p| {
                p.draft_id() == id.draft_id
                    && p.domain().selected_provider.as_deref()
                        == Some(request.publication.key.provider.as_str())
                    && p.domain().selected_model.as_deref()
                        == Some(request.publication.key.model.as_str())
                    && p.domain().selected_reasoning_effort == request.publication.reasoning_effort
                    && p.model_display.as_ref().is_some_and(|display| {
                        display.identity == *id
                            && display.request == ComposerModelDisplayRequestState::Loading
                    })
            })
    }

    fn complete_composer_model_display(
        &self,
        request: ModelRequest,
        response: Result<ProviderListModelsResponse, String>,
    ) {
        if self.is_stopped() || self.current_auth_ticket() != request.auth_ticket {
            return;
        }
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        let id = &request.publication.identity;
        if store.suspended.contains(&id.thread_id) {
            return;
        }
        let Some(current) = store
            .drafts
            .get(&id.thread_id)
            .filter(|p| {
                p.draft_id() == id.draft_id
                    && p.domain().selected_provider.as_deref()
                        == Some(request.publication.key.provider.as_str())
                    && p.domain().selected_model.as_deref()
                        == Some(request.publication.key.model.as_str())
                    && p.domain().selected_reasoning_effort == request.publication.reasoning_effort
                    && p.model_display.as_ref().is_some_and(|display| {
                        display.identity == *id
                            && display.request == ComposerModelDisplayRequestState::Loading
                    })
            })
            .cloned()
        else {
            return;
        };
        self.composer_models
            .lock()
            .expect("composer models poisoned")
            .in_flight
            .remove(&id.thread_id);
        let mut display = request.publication;
        match response {
            Ok(response) if response.provider == display.key.provider => {
                display.label = presentation::resolve_provider_model_display_from_response(
                    &display.key,
                    &response,
                )
                .label;
                display.reasoning_effort_label = response
                    .models
                    .iter()
                    .find(|model| {
                        model.provider == display.key.provider && model.id == display.key.model
                    })
                    .and_then(|model| {
                        presentation::reasoning_effort_rows_for_model(
                            model,
                            display.reasoning_effort.as_deref(),
                        )
                        .into_iter()
                        .find(|row| row.selected)
                        .map(|row| row.label)
                    });
                display.request = ComposerModelDisplayRequestState::Ready;
            }
            Ok(_) => {
                display.request = ComposerModelDisplayRequestState::Failed {
                    message: "Model response provider mismatch".into(),
                }
            }
            Err(message) => display.request = ComposerModelDisplayRequestState::Failed { message },
        }
        let mut next = (*current).clone();
        next.model_display = Some(display);
        self.publish_composer_model_display(&mut store, next);
    }

    pub(super) fn cancel_composer_model_display(&self, thread: &str) {
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        self.composer_models
            .lock()
            .expect("composer models poisoned")
            .in_flight
            .remove(thread);
        let Some(current) = store
            .drafts
            .get(thread)
            .filter(|p| {
                p.model_display.as_ref().is_some_and(|display| {
                    display.request == ComposerModelDisplayRequestState::Loading
                })
            })
            .cloned()
        else {
            return;
        };
        let mut next = (*current).clone();
        next.model_display.as_mut().unwrap().request = ComposerModelDisplayRequestState::Cancelled;
        self.publish_composer_model_display(&mut store, next);
    }

    pub(crate) fn queue_composer_model_selection_refresh(&self) {
        let mut owner = self
            .composer_models
            .lock()
            .expect("composer models poisoned");
        if owner.sender.is_none() || owner.selection_pending {
            return;
        }
        owner.selection_pending = true;
        // A full queue already wakes the worker, which drains this coalesced flag.
        let _ = owner
            .sender
            .as_ref()
            .unwrap()
            .try_send(ModelWork::SelectionChanged);
    }

    fn refresh_composer_model_selections(&self) {
        let pending = {
            let mut owner = self
                .composer_models
                .lock()
                .expect("composer models poisoned");
            std::mem::take(&mut owner.selection_pending)
        };
        if !pending || self.is_stopped() {
            return;
        }
        let drafts = {
            let store = self.composer_store.lock().expect("composer store poisoned");
            store
                .drafts
                .values()
                .filter(|draft| !store.suspended.contains(draft.thread_id()))
                .map(|draft| (draft.thread_id().to_owned(), draft.draft_id()))
                .collect::<Vec<_>>()
        };
        for (thread_id, draft_id) in drafts {
            self.reconcile_composer_authorization(&thread_id, draft_id);
            self.observe_composer_runtime(&thread_id, None, false);
            let selection = self.resolved_composer_model_selection(&thread_id);
            let Some(current) = self.composer_snapshot(&thread_id) else {
                continue;
            };
            let action = super::state_machine::ComposerDomainAction::SyncResolvedModelSelection {
                selection,
                capability_target: None,
            };
            if super::state_machine::reduce_composer_domain_state(current.domain(), action.clone())
                .changed
            {
                self.composer_intent(super::store::ComposerIntent::Domain {
                    thread_id: thread_id.clone(),
                    draft_id,
                    action,
                });
            } else {
                self.observe_composer_model_display(&thread_id, None);
            }
        }
    }

    pub(crate) fn start_composer_model_display_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<ModelWork>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-composer-model".into())
            .spawn(move || {
                while let Ok(work) = receiver.recv() {
                    if let ModelWork::Resolve(request) = work {
                        let Some(core) = weak.upgrade() else {
                            return;
                        };
                        if core.composer_model_request_is_current(&request) {
                            let key = &request.publication.key;
                            let read = core.read_provider_collection(
                                crate::providers::store::ProviderCollectionKey::models(
                                    key.workspace_id.clone(),
                                    key.provider.clone(),
                                    crate::providers::store::ProviderModelKind::Chat,
                                ),
                                request.retry,
                            );
                            drop(core);
                            let result = read
                                .and_then(|read| {
                                    read.wait_while(|| {
                                        weak.upgrade().is_some_and(|core| {
                                            core.composer_model_request_is_current(&request)
                                        })
                                    })
                                })
                                .and_then(|p| p.models_response());
                            let Some(core) = weak.upgrade() else {
                                return;
                            };
                            core.complete_composer_model_display(
                                request,
                                result.map_err(|error| format!("{error:#}")),
                            );
                        }
                    }
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    core.refresh_composer_model_selections();
                }
            })
            .expect("composer model worker");
        let mut owner = self
            .composer_models
            .lock()
            .expect("composer models poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        composer::{
            state_machine::{ComposerDomainAction, ComposerDomainState},
            store::ComposerIntent,
        },
        core::{ClientDemand, ClientIntent, ClientTransitionOutcome},
    };

    fn fixture() -> (Arc<ClientCore>, mpsc::Receiver<ModelWork>) {
        let core = Arc::new(ClientCore::new());
        let (sender, receiver) = mpsc::sync_channel(64);
        core.composer_models.lock().unwrap().sender = Some(sender);
        for thread_id in ["a", "b"] {
            core.composer_intent(ComposerIntent::Open {
                thread_id: thread_id.into(),
                defaults: ComposerDomainState {
                    selected_provider: Some("provider".into()),
                    selected_model: Some("model".into()),
                    selected_reasoning_effort: Some("high".into()),
                    ..Default::default()
                },
            });
        }
        (core, receiver)
    }
    fn receive(receiver: &mpsc::Receiver<ModelWork>) -> ModelRequest {
        match receiver.try_recv().unwrap() {
            ModelWork::Resolve(request) => request,
            ModelWork::SelectionChanged => panic!("unexpected selection wake"),
        }
    }
    fn observe(core: &ClientCore, retry: bool) -> ClientTransitionOutcome {
        let draft = core.composer_snapshot("a").unwrap();
        core.enqueue_composer_model_display(
            "a",
            Some("workspace"),
            core.current_auth_ticket(),
            retry.then(|| draft.draft_id()),
        )
        .map_or(ClientTransitionOutcome::Noop, |transition| {
            transition.outcome()
        })
    }
    fn response() -> ProviderListModelsResponse {
        serde_json::from_value(serde_json::json!({"provider": "provider", "models": [{
            "id": "model", "name": "Model display name", "provider": "provider", "limits": {},
            "capabilities": {"reasoning": {"supported": true, "effort_options": ["low", "high"]}}
        }]}))
        .unwrap()
    }

    #[test]
    fn changed_auth_ticket_replaces_loading_request_without_manual_retry() {
        let (core, receiver) = fixture();
        core.enqueue_composer_model_display("a", Some("workspace"), (999, None), None)
            .unwrap();
        let stale = receive(&receiver);
        assert!(!core.composer_model_request_is_current(&stale));
        assert_eq!(observe(&core, false), ClientTransitionOutcome::Changed);
        let current = receive(&receiver);
        assert_ne!(current.publication.identity, stale.publication.identity);
        let before = core.composer_snapshot("a").unwrap();
        core.complete_composer_model_display(stale, Ok(response()));
        assert!(Arc::ptr_eq(&before, &core.composer_snapshot("a").unwrap()));
        core.complete_composer_model_display(current, Ok(response()));
        assert_eq!(
            core.composer_snapshot("a")
                .unwrap()
                .model_display()
                .unwrap()
                .request,
            ComposerModelDisplayRequestState::Ready
        );
        assert!(core.composer_models.lock().unwrap().in_flight.is_empty());
    }

    #[test]
    fn selected_model_and_reasoning_labels_publish_without_echo_or_repeat_request() {
        let (core, receiver) = fixture();
        assert_eq!(observe(&core, false), ClientTransitionOutcome::Changed);
        let request = receive(&receiver);
        assert!(core.composer_model_request_is_current(&request));
        assert_eq!(observe(&core, false), ClientTransitionOutcome::Noop);
        assert_eq!(observe(&core, true), ClientTransitionOutcome::Noop);
        assert!(receiver.try_recv().is_err());
        let b = core.composer_snapshot("b").unwrap();
        core.complete_composer_model_display(request.clone(), Ok(response()));
        let current = core.composer_snapshot("a").unwrap();
        let display = current.model_display().unwrap();
        assert_eq!(display.request, ComposerModelDisplayRequestState::Ready);
        assert_eq!(display.label.as_deref(), Some("Model display name"));
        assert_eq!(display.reasoning_effort_label.as_deref(), Some("High"));
        assert!(Arc::ptr_eq(&b, &core.composer_snapshot("b").unwrap()));
        assert_eq!(observe(&core, false), ClientTransitionOutcome::Noop);
        core.complete_composer_model_display(request, Err("duplicate".into()));
        assert!(Arc::ptr_eq(&current, &core.composer_snapshot("a").unwrap()));
        assert_eq!(
            core.composer_intent(ComposerIntent::EditText {
                thread_id: "a".into(),
                draft_id: current.draft_id(),
                text: "next user edit".into()
            })
            .outcome(),
            ClientTransitionOutcome::Changed
        );
        let input = core.composer_snapshot("a").unwrap();
        let delivered = core
            .snapshot(&ClientScope::Composer {
                thread_id: "a".into(),
            })
            .unwrap()
            .typed::<ComposerPublication>()
            .unwrap()
            .payload();
        assert_eq!(delivered, input);
        assert_eq!(delivered.draft().text, "next user edit");
    }

    #[test]
    fn persistent_failure_waits_for_retry_and_rejects_old_generation() {
        let (core, receiver) = fixture();
        observe(&core, false);
        let first = receive(&receiver);
        core.complete_composer_model_display(first.clone(), Err("persistent failure".into()));
        let failed = core.composer_snapshot("a").unwrap();
        for _ in 0..16 {
            assert_eq!(observe(&core, false), ClientTransitionOutcome::Noop);
        }
        assert!(receiver.try_recv().is_err());
        assert!(Arc::ptr_eq(&failed, &core.composer_snapshot("a").unwrap()));
        assert_eq!(observe(&core, true), ClientTransitionOutcome::Changed);
        let second = receive(&receiver);
        assert_ne!(
            first.publication.identity.generation,
            second.publication.identity.generation
        );
        let pending = core.composer_snapshot("a").unwrap();
        core.complete_composer_model_display(first, Ok(response()));
        assert!(Arc::ptr_eq(&pending, &core.composer_snapshot("a").unwrap()));
        core.complete_composer_model_display(second, Ok(response()));
        assert_eq!(
            core.composer_snapshot("a")
                .unwrap()
                .model_display()
                .unwrap()
                .request,
            ComposerModelDisplayRequestState::Ready
        );
    }

    #[test]
    fn selection_change_replacement_draft_and_auth_ticket_reject_old_completion() {
        for changed in ["selection", "draft", "auth"] {
            let (core, receiver) = fixture();
            observe(&core, false);
            let mut request = receive(&receiver);
            let draft = core.composer_snapshot("a").unwrap();
            match changed {
                "selection" => {
                    core.composer_intent(ComposerIntent::Domain {
                        thread_id: "a".into(),
                        draft_id: draft.draft_id(),
                        action: ComposerDomainAction::SetModelSelectionFromUser {
                            provider: Some("other".into()),
                            model: Some("other".into()),
                            capability_target: None,
                        },
                    });
                }
                "draft" => {
                    core.composer_intent(ComposerIntent::Clear {
                        thread_id: "a".into(),
                        draft_id: draft.draft_id(),
                    });
                }
                _ => request.auth_ticket.0 += 1,
            }
            let current = core.composer_snapshot("a").unwrap();
            assert!(!core.composer_model_request_is_current(&request));
            core.complete_composer_model_display(request, Ok(response()));
            assert!(Arc::ptr_eq(&current, &core.composer_snapshot("a").unwrap()));
        }
    }

    #[test]
    fn binding_drop_suspension_and_shutdown_cancel_late_model_results() {
        for teardown in ["binding", "suspend", "shutdown", "retire"] {
            let (core, receiver) = fixture();
            let scope = ClientScope::Composer {
                thread_id: "a".into(),
            };
            let lease = core.subscribe(scope.clone(), std::num::NonZeroUsize::new(4).unwrap());
            observe(&core, false);
            let request = receive(&receiver);
            match teardown {
                "binding" => drop(lease),
                "suspend" => {
                    core.dispatch(ClientIntent::SetScopeDemand {
                        scope,
                        demand: ClientDemand::Suspended,
                        generation: crate::core::ClientGeneration::new(1),
                    });
                }
                "retire" => core.remove_thread_store("a"),
                _ => core.shutdown(),
            }
            assert!(!core.composer_model_request_is_current(&request));
            let current = core.composer_snapshot("a");
            core.complete_composer_model_display(request, Ok(response()));
            assert_eq!(current, core.composer_snapshot("a"));
        }
    }

    #[test]
    fn mismatched_provider_and_full_queue_finish_with_bounded_failure() {
        let (core, receiver) = fixture();
        observe(&core, false);
        let request = receive(&receiver);
        let mut wrong = response();
        wrong.provider = "other".into();
        core.complete_composer_model_display(request, Ok(wrong));
        assert!(matches!(
            core.composer_snapshot("a")
                .unwrap()
                .model_display()
                .unwrap()
                .request,
            ComposerModelDisplayRequestState::Failed { .. }
        ));
        let (sender, _receiver) = mpsc::sync_channel(0);
        core.composer_models.lock().unwrap().sender = Some(sender);
        observe(&core, true);
        assert!(matches!(
            core.composer_snapshot("a")
                .unwrap()
                .model_display()
                .unwrap()
                .request,
            ComposerModelDisplayRequestState::Failed { .. }
        ));
        assert_eq!(observe(&core, false), ClientTransitionOutcome::Noop);
    }
}
