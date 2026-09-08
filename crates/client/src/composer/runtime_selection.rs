//! Runtime readiness for draft selection. Provider administration remains independent.
use super::{
    capabilities::composer_capability_target_for_provider,
    catalog::{ComposerCatalogRequest, ComposerCatalogRequestState},
    store::{ComposerOperationIdentity, ComposerStore, DraftId},
};
#[cfg(any(test, feature = "test-support"))]
use crate::core::ClientMutationAuthority;
use crate::{
    core::{ClientCore, ClientTransition},
    providers::list::{self, CliRuntimeSnapshotLoad, CliRuntimeSnapshotUpdate, ProviderListState},
};
use pioneer_protocol::{CLIRuntimeListResponse, GatewayNotification, RuntimeSummary};
use std::sync::{Arc, mpsc};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ComposerRuntimePublication {
    pub identity: ComposerOperationIdentity,
    pub workspace_id: String,
    pub request: ComposerCatalogRequest,
    pub selected_provider: Option<String>,
    pub selected_provider_ready: bool,
    pub active_runtime_supports_steer: Option<bool>,
}
#[derive(Clone)]
pub(super) struct RuntimeRequest {
    identity: ComposerOperationIdentity,
    workspace: String,
    auth: (u64, Option<u64>),
    attempt: usize,
}
pub(super) struct ComposerRuntimeState {
    draft: DraftId,
    list: ProviderListState,
    request: RuntimeRequest,
}
#[derive(Default)]
pub(crate) struct ComposerRuntimeController {
    sender: Option<mpsc::SyncSender<RuntimeRequest>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl ComposerRuntimeController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
    }
}
impl Drop for ComposerRuntimeController {
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
    pub(crate) fn observe_composer_runtime(
        &self,
        thread: &str,
        retry: Option<DraftId>,
        picker: bool,
    ) -> Option<ClientTransition> {
        let auth = self.current_auth_ticket();
        if self.is_stopped() {
            return None;
        }
        let Some(coordinator) = self.thread_coordinator_snapshot(thread) else {
            return None;
        };
        let workspace = coordinator.workspace_id.clone();
        let allowed = self
            .thread_capability_snapshot(thread)
            .and_then(|p| p.snapshot.clone())
            .or_else(|| self.authorization_snapshot(Some(&workspace), None))
            .is_some_and(|p| {
                crate::authorization::principal_presentation_capabilities(&p).can_use_cli_runtimes
            });
        if !allowed {
            return None;
        }

        let mut store = self.composer_store.lock().expect("composer store poisoned");
        let Some(current) = store.drafts.get(thread).cloned() else {
            return None;
        };
        if store.suspended.contains(thread) || retry.is_some_and(|id| id != current.draft_id()) {
            return None;
        }
        let selected_cli = current
            .domain()
            .selected_provider
            .as_deref()
            .and_then(list::runtime_id_from_cli_runtime_provider_key)
            .is_some();
        if !picker
            && !selected_cli
            && self
                .thread_snapshot(thread)
                .is_none_or(|s| s.cli_binding().is_none())
        {
            return None;
        }
        if store
            .runtimes
            .get(thread)
            .is_some_and(|s| s.draft == current.draft_id())
        {
            if let Some(publication) = current.runtime_selection() {
                if publication.request.state == ComposerCatalogRequestState::Loading
                    || (retry.is_none()
                        && publication.request.state != ComposerCatalogRequestState::Cancelled)
                {
                    self.publish_composer_runtime_selection(&mut store, thread);
                    return None;
                }
            }
        }
        store.next_operation = store
            .next_operation
            .checked_add(1)
            .expect("runtime request generation exhausted");
        let identity = ComposerOperationIdentity {
            thread_id: thread.into(),
            draft_id: current.draft_id(),
            generation: store.next_operation,
        };
        let request = RuntimeRequest {
            identity: identity.clone(),
            workspace: workspace.clone(),
            auth,
            attempt: 0,
        };
        let list = store
            .runtimes
            .remove(thread)
            .filter(|s| s.draft == current.draft_id())
            .map(|s| s.list)
            .unwrap_or_default();
        store.runtimes.insert(
            thread.into(),
            ComposerRuntimeState {
                draft: current.draft_id(),
                list,
                request: request.clone(),
            },
        );
        let mut next = (*current).clone();
        next.runtime_selection = Some(ComposerRuntimePublication {
            identity,
            workspace_id: workspace,
            request: ComposerCatalogRequest {
                generation: request.identity.generation,
                state: ComposerCatalogRequestState::Loading,
            },
            selected_provider: current.domain().selected_provider.clone(),
            selected_provider_ready: current
                .runtime_selection()
                .map_or(!selected_cli, |p| p.selected_provider_ready),
            active_runtime_supports_steer: current
                .runtime_selection()
                .and_then(|p| p.active_runtime_supports_steer),
        });
        let transition = self.publish_composer_model_display(&mut store, next);
        drop(store);
        self.enqueue_composer_runtime(request);
        Some(transition)
    }
    fn enqueue_composer_runtime(&self, request: RuntimeRequest) {
        let queued = self
            .composer_runtimes
            .lock()
            .expect("composer runtimes poisoned")
            .sender
            .as_ref()
            .is_some_and(|sender| sender.try_send(request.clone()).is_ok());
        if !queued {
            self.complete_composer_runtime(
                request,
                Err("Runtime request queue unavailable".into()),
            );
        }
    }
    fn runtime_request_matches(&self, store: &ComposerStore, request: &RuntimeRequest) -> bool {
        !self.is_stopped()
            && self.current_auth_ticket() == request.auth
            && !store.suspended.contains(&request.identity.thread_id)
            && store
                .runtimes
                .get(&request.identity.thread_id)
                .is_some_and(|s| {
                    s.request.identity == request.identity && s.request.attempt == request.attempt
                })
            && store
                .drafts
                .get(&request.identity.thread_id)
                .is_some_and(|draft| {
                    draft.draft_id() == request.identity.draft_id
                        && draft.runtime_selection().is_some_and(|p| {
                            p.identity == request.identity
                                && p.request.state == ComposerCatalogRequestState::Loading
                        })
                })
    }
    fn complete_composer_runtime(
        &self,
        request: RuntimeRequest,
        response: Result<CLIRuntimeListResponse, String>,
    ) {
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        if !self.runtime_request_matches(&store, &request) {
            return;
        }
        let state = store.runtimes.get_mut(&request.identity.thread_id).unwrap();
        let outcome = match response {
            Ok(response) => match state.list.apply_cli_runtime_snapshot_response(response) {
                CliRuntimeSnapshotLoad::Applied => ComposerCatalogRequestState::Ready,
                CliRuntimeSnapshotLoad::RetryRequired => ComposerCatalogRequestState::Failed {
                    message: "Runtime snapshot predates observed update".into(),
                },
            },
            Err(message) => ComposerCatalogRequestState::Failed { message },
        };
        if matches!(outcome, ComposerCatalogRequestState::Failed { .. }) && request.attempt < 3 {
            let mut retry = request;
            retry.attempt += 1;
            state.request = retry.clone();
            drop(store);
            self.enqueue_composer_runtime(retry);
            return;
        }
        let mut next = (**store.drafts.get(&request.identity.thread_id).unwrap()).clone();
        next.runtime_selection.as_mut().unwrap().request.state = outcome;
        self.publish_composer_model_display(&mut store, next);
        self.publish_composer_runtime_selection(&mut store, &request.identity.thread_id);
        drop(store);
        self.resume_composer_model_picker_models(&request.identity.thread_id);
    }
    pub(super) fn composer_runtime_rows(
        store: &ComposerStore,
        thread: &str,
    ) -> Vec<RuntimeSummary> {
        store
            .runtimes
            .get(thread)
            .map(|s| s.list.cli_runtimes().to_vec())
            .unwrap_or_default()
    }
    fn publish_composer_runtime_selection(&self, store: &mut ComposerStore, thread: &str) {
        let Some(current) = store.drafts.get(thread).cloned() else {
            return;
        };
        let runtimes = Self::composer_runtime_rows(store, thread);
        let mut next = (*current).clone();
        let provider = next.domain().selected_provider.clone();
        if let Some(publication) = next.runtime_selection.as_mut() {
            publication.selected_provider = provider.clone();
            publication.selected_provider_ready =
                list::provider_ready_for_model_selector(provider.as_deref(), &runtimes);
            publication.active_runtime_supports_steer = self
                .thread_snapshot(thread)
                .and_then(|p| p.cli_binding().map(|b| b.runtime_id.clone()))
                .and_then(|id| {
                    runtimes
                        .iter()
                        .find(|r| r.runtime_id == id)
                        .map(|r| r.capabilities.supports_steer)
                });
        }
        let target = composer_capability_target_for_provider(provider.as_deref(), &runtimes);
        self.apply_composer_runtime_target(&mut next, target);
        if next != *current {
            self.publish_composer_model_display(store, next);
        }
        self.sync_composer_model_picker_runtimes(store, thread, runtimes);
    }
    pub(super) fn cancel_composer_runtime(&self, thread: &str) {
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        store.runtimes.remove(thread);
        let Some(current) = store
            .drafts
            .get(thread)
            .filter(|p| p.runtime_selection().is_some())
            .cloned()
        else {
            return;
        };
        let mut next = (*current).clone();
        let ready = list::provider_ready_for_model_selector(
            next.domain().selected_provider.as_deref(),
            &[],
        );
        let publication = next.runtime_selection.as_mut().unwrap();
        publication.request.state = ComposerCatalogRequestState::Cancelled;
        publication.selected_provider_ready = ready;
        publication.active_runtime_supports_steer = None;
        self.publish_composer_model_display(&mut store, next);
    }
    pub(crate) fn observe_composer_runtime_notification(&self, notification: &GatewayNotification) {
        let workspace = match notification {
            GatewayNotification::CLIRuntimeStatusChanged(n) => &n.workspace_id,
            GatewayNotification::CLIRuntimeAccountUpdated(n) => &n.workspace_id,
            GatewayNotification::CLIRuntimeAppsChanged(n) => &n.workspace_id,
            _ => return,
        };
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        let threads: Vec<_> = store
            .runtimes
            .iter()
            .filter(|(thread, state)| {
                &state.request.workspace == workspace && !store.suspended.contains(*thread)
            })
            .map(|(t, _)| t.clone())
            .collect();
        let mut reload = Vec::new();
        let mut changed_threads = Vec::new();
        for thread in threads {
            let state = store.runtimes.get_mut(&thread).unwrap();
            let changed = match notification {
                GatewayNotification::CLIRuntimeStatusChanged(n) => state
                    .list
                    .apply_cli_runtime_snapshot_update(n.revision, n.runtime.clone(), n.removed),
                _ => CliRuntimeSnapshotUpdate::ReloadRequired,
            };
            match changed {
                CliRuntimeSnapshotUpdate::Stale => {}
                CliRuntimeSnapshotUpdate::Applied => {
                    self.publish_composer_runtime_selection(&mut store, &thread);
                    changed_threads.push(thread);
                }
                CliRuntimeSnapshotUpdate::ReloadRequired => reload.push((thread, state.draft)),
            }
        }
        drop(store);
        for thread in changed_threads {
            self.resume_composer_model_picker_models(&thread);
        }
        for (thread, draft) in reload {
            self.observe_composer_runtime(&thread, Some(draft), true);
        }
    }
    pub(crate) fn start_composer_runtime_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<RuntimeRequest>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-composer-runtime".into())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    if request.attempt > 0 {
                        std::thread::sleep(std::time::Duration::from_millis(
                            [0, 500, 2000, 5000][request.attempt],
                        ));
                    }
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    let matches = {
                        let store = core.composer_store.lock().expect("composer store poisoned");
                        core.runtime_request_matches(&store, &request)
                    };
                    if !matches {
                        continue;
                    }
                    let sender = core.compatibility_runtime().ws_command_sender();
                    drop(core);
                    let response = sender
                        .cli_runtime_list(list::cli_runtime_list_params(request.workspace.clone()))
                        .map_err(|e| format!("{e:#}"));
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    core.complete_composer_runtime(request, response);
                }
            })
            .expect("composer runtime worker");
        let mut owner = self
            .composer_runtimes
            .lock()
            .expect("composer runtimes poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        composer::store::ComposerIntent,
        core::{ClientDemand, ClientScope},
    };
    use pioneer_protocol::*;
    fn fixture() -> (Arc<ClientCore>, mpsc::Receiver<RuntimeRequest>) {
        let core = Arc::new(ClientCore::new());
        let (sender, receiver) = mpsc::sync_channel(64);
        core.composer_runtimes.lock().unwrap().sender = Some(sender);
        core.upsert_thread(serde_json::from_value(serde_json::json!({
            "workspace_id":"ws", "id":"a", "preview":"", "mode":"Agent", "model":"model", "model_provider":"cli_runtime:codex", "created_at":1,"updated_at":1,"status":"Idle","origin_kind":"user","sidebar_visibility":"visible","turns":[]
        })).unwrap());
        ClientMutationAuthority { _private: () }.accept_thread_capabilities_for_test(
            &core,
            AuthorizationCapabilitySnapshot {
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
                workspace: Some(AuthorizationWorkspaceCapabilitySnapshot {
                    workspace_id: "ws".into(),
                    capabilities: AuthorizationWorkspaceCapabilities {
                        can_use_providers: true,
                        can_use_cli_runtimes: true,
                        can_use_skills: true,
                        can_use_mcp: true,
                        ..Default::default()
                    },
                    operational_resources: AuthorizationOperationalResourceProjection {
                        provider_models_all: true,
                        cli_models_all: true,
                        cli_runtimes: AuthorizationResourceSelector {
                            all: true,
                            ids: vec![],
                        },
                        skills: AuthorizationResourceSelector {
                            all: true,
                            ids: vec![],
                        },
                        mcp_servers: AuthorizationResourceSelector {
                            all: true,
                            ids: vec![],
                        },
                        ..Default::default()
                    },
                    execution_draft_policy: AuthorizationExecutionDraftPolicyProjection {
                        fingerprint: "policy".into(),
                        resources: AuthorizationOperationalResourceProjection {
                            provider_models_all: true,
                            cli_models_all: true,
                            cli_runtimes: AuthorizationResourceSelector {
                                all: true,
                                ids: vec![],
                            },
                            providers: AuthorizationResourceSelector {
                                all: true,
                                ids: vec![],
                            },
                            skills: AuthorizationResourceSelector {
                                all: true,
                                ids: vec![],
                            },
                            mcp_servers: AuthorizationResourceSelector {
                                all: true,
                                ids: vec![],
                            },
                            ..Default::default()
                        },
                        permission_options: vec![],
                        can_attach_artifacts: false,
                        mcp_invocation_limits: Default::default(),
                    },
                }),
                thread: Some(AuthorizationThreadCapabilitySnapshot {
                    thread_id: "a".into(),
                    workspace_id: "ws".into(),
                    capabilities: AuthorizationThreadCapabilities {
                        can_start_turn: true,
                        ..Default::default()
                    },
                }),
            },
        );
        core.composer_intent(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: super::super::state_machine::ComposerDomainState {
                selected_mode: ThreadMode::Agent,
                ..Default::default()
            },
        });
        let draft = core.composer_snapshot("a").unwrap();
        core.composer_intent(ComposerIntent::Domain {
            thread_id: "a".into(),
            draft_id: draft.draft_id(),
            action: super::super::state_machine::ComposerDomainAction::SetModelSelectionFromUser {
                provider: Some("cli_runtime:codex".into()),
                model: Some("model".into()),
                capability_target: None,
            },
        });
        (core, receiver)
    }
    fn runtime(ready: bool) -> RuntimeSummary {
        serde_json::from_value(serde_json::json!({"runtime_id":"codex","kind":"codex","display_name":"Codex","enabled":true,"status":{"state":if ready {"ready"} else {"needs_auth"}},"capabilities": RuntimeCapabilities { supports_threads: true, supports_model_list: true, supports_steer: true, supports_skills: true, supports_mcp_tools: true, ..Default::default() }})).unwrap()
    }
    fn response(revision: u64, ready: bool) -> CLIRuntimeListResponse {
        CLIRuntimeListResponse {
            revision,
            runtimes: vec![runtime(ready)],
        }
    }
    fn delta(core: &ClientCore, revision: u64, ready: bool) {
        core.observe_composer_runtime_notification(&GatewayNotification::CLIRuntimeStatusChanged(
            CLIRuntimeStatusChangedNotification {
                workspace_id: "ws".into(),
                revision,
                runtime: runtime(ready),
                removed: false,
            },
        ));
    }
    #[test]
    fn selected_runtime_readiness_and_capability_target_have_one_publication_owner() {
        let (core, rx) = fixture();
        let request = rx.try_recv().unwrap();
        assert!(
            !core
                .composer_snapshot("a")
                .unwrap()
                .selected_provider_ready()
        );
        core.complete_composer_runtime(request.clone(), Ok(response(1, true)));
        let ready = core.composer_snapshot("a").unwrap();
        assert!(ready.selected_provider_ready());
        assert_eq!(
            serde_json::to_value(&ready).unwrap()["selected_provider_ready"],
            true
        );
        assert!(ready.domain().capability_target.policy().supports_skills);
        core.complete_composer_runtime(request, Err("duplicate".into()));
        assert!(Arc::ptr_eq(&ready, &core.composer_snapshot("a").unwrap()));
        delta(&core, 2, false);
        let unavailable = core.composer_snapshot("a").unwrap();
        assert!(!unavailable.selected_provider_ready());
        delta(&core, 1, true);
        assert!(Arc::ptr_eq(
            &unavailable,
            &core.composer_snapshot("a").unwrap()
        ));
        assert!(rx.try_recv().is_err());
    }
    #[test]
    fn persistent_failure_is_bounded_and_only_explicit_retry_starts_a_new_generation() {
        let (core, rx) = fixture();
        let first = rx.try_recv().unwrap();
        let mut request = first.clone();
        for attempt in 0..4 {
            assert_eq!(request.attempt, attempt);
            core.complete_composer_runtime(request.clone(), Err("persistent".into()));
            if attempt < 3 {
                request = rx.try_recv().unwrap();
            }
        }
        let draft = core.composer_snapshot("a").unwrap();
        assert!(matches!(
            draft.runtime_selection().unwrap().request.state,
            ComposerCatalogRequestState::Failed { .. }
        ));
        for _ in 0..100 {
            core.observe_composer_runtime("a", None, false);
        }
        assert!(rx.try_recv().is_err());
        core.composer_intent(ComposerIntent::RetryRuntimeSelection {
            thread_id: "a".into(),
            draft_id: draft.draft_id(),
        });
        let retry = rx.try_recv().unwrap();
        assert_ne!(first.identity.generation, retry.identity.generation);
        core.complete_composer_runtime(first, Ok(response(100, true)));
        assert!(
            !core
                .composer_snapshot("a")
                .unwrap()
                .selected_provider_ready()
        );
        core.complete_composer_runtime(retry, Ok(response(1, true)));
        assert!(
            core.composer_snapshot("a")
                .unwrap()
                .selected_provider_ready()
        );
    }
    #[test]
    fn a_revision_gap_fences_the_in_flight_full_response() {
        let (core, rx) = fixture();
        let first = rx.try_recv().unwrap();
        delta(&core, 3, false);
        core.complete_composer_runtime(first.clone(), Ok(response(1, true)));
        assert!(
            !core
                .composer_snapshot("a")
                .unwrap()
                .selected_provider_ready()
        );
        let retry = rx.try_recv().unwrap();
        assert_eq!(retry.identity, first.identity);
        core.complete_composer_runtime(retry, Ok(response(3, false)));
        delta(&core, 4, true);
        assert!(
            core.composer_snapshot("a")
                .unwrap()
                .selected_provider_ready()
        );
        assert!(rx.try_recv().is_err());
    }
    #[test]
    fn route_draft_access_and_shutdown_retirement_reject_late_callbacks() {
        for scenario in 0..5 {
            let (core, rx) = fixture();
            let old = rx.try_recv().unwrap();
            match scenario {
                0 => {
                    let lease = core.subscribe(
                        ClientScope::Composer {
                            thread_id: "a".into(),
                        },
                        std::num::NonZeroUsize::new(8).unwrap(),
                    );
                    drop(lease);
                }
                1 => core.composer_demand_changed(
                    &ClientScope::Composer {
                        thread_id: "a".into(),
                    },
                    ClientDemand::Suspended,
                ),
                2 => {
                    let id = core.composer_snapshot("a").unwrap().draft_id();
                    core.composer_intent(ComposerIntent::Clear {
                        thread_id: "a".into(),
                        draft_id: id,
                    });
                }
                3 => core.clear_authorization_projections(),
                _ => core.shutdown(),
            }
            let before = core.composer_snapshot("a");
            core.complete_composer_runtime(old, Ok(response(10, true)));
            assert_eq!(before, core.composer_snapshot("a"));
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl ClientMutationAuthority {
    /// Resolves a synthetic runtime read through the same generation-fenced completion.
    pub fn accept_composer_runtime_for_test(
        &self,
        core: &ClientCore,
        thread: &str,
        response: CLIRuntimeListResponse,
    ) {
        let (sender, receiver) = mpsc::sync_channel(64);
        let previous = core
            .composer_runtimes
            .lock()
            .unwrap()
            .sender
            .replace(sender);
        core.cancel_composer_runtime(thread);
        let draft = core.composer_snapshot(thread).unwrap();
        core.observe_composer_runtime(thread, Some(draft.draft_id()), true);
        core.composer_runtimes.lock().unwrap().sender = previous;
        let request = receiver.try_recv().expect("synthetic runtime request");
        core.complete_composer_runtime(request, Ok(response));
    }
}
