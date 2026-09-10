//! Draft-specific runtime readiness derived from the shared provider owner.
use super::{
    capabilities::composer_capability_target_for_provider,
    catalog::{ComposerCatalogRequest, ComposerCatalogRequestState},
    store::{ComposerOperationIdentity, ComposerStore, DraftId},
};
#[cfg(any(test, feature = "test-support"))]
use crate::core::ClientMutationAuthority;
use crate::{
    core::{ClientCore, ClientTransition},
    providers::{
        list,
        runtime::{
            ProviderRuntimeDemand, ProviderRuntimeIntent, ProviderRuntimePublication,
            ProviderRuntimeRequestState,
        },
    },
};
use pioneer_protocol::RuntimeSummary;
use std::sync::Arc;

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
pub(super) struct ComposerRuntimeState {
    draft: DraftId,
    workspace: String,
    _demand: ProviderRuntimeDemand,
    snapshot: Option<Arc<ProviderRuntimePublication>>,
}
impl ClientCore {
    pub(crate) fn observe_composer_runtime(
        &self,
        thread: &str,
        retry: Option<DraftId>,
        picker: bool,
    ) -> Option<ClientTransition> {
        if self.is_stopped() {
            return None;
        }
        let coordinator = self.thread_coordinator_snapshot(thread)?;
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
        let current = store.drafts.get(thread)?.clone();
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
            .is_some_and(|s| s.draft == current.draft_id() && s.workspace == workspace)
        {
            self.publish_composer_runtime_selection(&mut store, thread);
            drop(store);
            if retry.is_some() {
                self.provider_runtime_intent(ProviderRuntimeIntent::Refresh {
                    workspace_id: workspace,
                });
            }
            return None;
        }
        // Registration only changes the provider owner and never calls a consumer
        // back while the composer lock is held.
        store.runtimes.remove(thread);
        let demand = self.retain_provider_runtime(&workspace);
        store.runtimes.insert(
            thread.into(),
            ComposerRuntimeState {
                draft: current.draft_id(),
                workspace: workspace.clone(),
                _demand: demand,
                snapshot: self.provider_runtime_snapshot(&workspace),
            },
        );
        store.next_operation = store
            .next_operation
            .checked_add(1)
            .expect("runtime projection identity exhausted");
        let identity = ComposerOperationIdentity {
            thread_id: thread.into(),
            draft_id: current.draft_id(),
            generation: store.next_operation,
        };
        let mut next = (*current).clone();
        next.runtime_selection = Some(ComposerRuntimePublication {
            identity: identity.clone(),
            workspace_id: workspace.clone(),
            request: ComposerCatalogRequest {
                generation: identity.generation,
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
        self.sync_composer_provider_runtimes(&workspace);
        Some(transition)
    }
    pub(super) fn composer_runtime_rows(
        store: &ComposerStore,
        thread: &str,
    ) -> Vec<RuntimeSummary> {
        store
            .runtimes
            .get(thread)
            .and_then(|s| s.snapshot.as_ref())
            .map(|p| {
                p.runtimes()
                    .iter()
                    .map(|row| row.runtime().clone())
                    .collect()
            })
            .unwrap_or_default()
    }
    pub(crate) fn sync_composer_provider_runtimes(&self, workspace: &str) {
        let snapshot = self.provider_runtime_snapshot(workspace);
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        let threads: Vec<_> = store
            .runtimes
            .iter()
            .filter(|(thread, s)| s.workspace == workspace && !store.suspended.contains(*thread))
            .map(|(thread, _)| thread.clone())
            .collect();
        for thread in &threads {
            let state = store.runtimes.get_mut(thread).unwrap();
            state.snapshot = snapshot.clone();
            self.publish_composer_runtime_selection(&mut store, thread);
        }
        drop(store);
        for thread in threads {
            self.resume_composer_model_picker_models(&thread);
        }
    }
    fn publish_composer_runtime_selection(&self, store: &mut ComposerStore, thread: &str) {
        let Some(current) = store.drafts.get(thread).cloned() else {
            return;
        };
        let runtimes = Self::composer_runtime_rows(store, thread);
        let mut next = (*current).clone();
        let provider = next.domain().selected_provider.clone();
        if let Some(publication) = next.runtime_selection.as_mut() {
            if let Some(snapshot) = store
                .runtimes
                .get(thread)
                .and_then(|state| state.snapshot.as_ref())
            {
                publication.request.state = match snapshot.request() {
                    ProviderRuntimeRequestState::Idle | ProviderRuntimeRequestState::Loading => {
                        ComposerCatalogRequestState::Loading
                    }
                    ProviderRuntimeRequestState::Ready => ComposerCatalogRequestState::Ready,
                    ProviderRuntimeRequestState::Failed => ComposerCatalogRequestState::Failed {
                        message: "Runtime snapshot unavailable".into(),
                    },
                    ProviderRuntimeRequestState::Cancelled => {
                        ComposerCatalogRequestState::Cancelled
                    }
                };
            }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        composer::store::ComposerIntent,
        core::{ClientDemand, ClientScope},
    };
    use pioneer_protocol::*;
    fn fixture() -> Arc<ClientCore> {
        let core = Arc::new(ClientCore::new());
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
        let mut capability = core
            .thread_capability_snapshot("a")
            .unwrap()
            .snapshot
            .clone()
            .unwrap();
        let workspace = capability.workspace.as_mut().unwrap();
        workspace.operational_resources = workspace.execution_draft_policy.resources.clone();
        workspace.operational_resources.fingerprint = "synthetic-policy".into();
        workspace.execution_draft_policy.resources = workspace.operational_resources.clone();
        assert_eq!(
            core.accept_authorization_projection(0, None, capability.clone()),
            crate::authorization::AuthorizationProjectionAcceptance::Accepted
        );
        core.upsert_thread(serde_json::from_value(serde_json::json!({
            "workspace_id":"ws", "id":"a", "preview":"", "mode":"Agent", "model":"model", "model_provider":"cli_runtime:codex", "created_at":1,"updated_at":1,"status":"Idle","origin_kind":"user","sidebar_visibility":"visible","turns":[]
        })).unwrap());
        ClientMutationAuthority { _private: () }
            .accept_thread_capabilities_for_test(&core, capability);
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
        core
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
        core.observe_provider_runtime_notification(&GatewayNotification::CLIRuntimeStatusChanged(
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
        let core = fixture();
        let request = core.provider_runtime_request_for_test("ws").unwrap();
        assert!(
            !core
                .composer_snapshot("a")
                .unwrap()
                .selected_provider_ready()
        );
        core.complete_provider_runtime_for_test(request.clone(), Ok(response(1, true)));
        let ready = core.composer_snapshot("a").unwrap();
        assert!(ready.selected_provider_ready());
        assert_eq!(
            serde_json::to_value(&ready).unwrap()["selected_provider_ready"],
            true
        );
        assert!(ready.domain().capability_target.policy().supports_skills);
        core.complete_provider_runtime_for_test(request, Err(()));
        assert!(Arc::ptr_eq(&ready, &core.composer_snapshot("a").unwrap()));
        delta(&core, 2, false);
        let unavailable = core.composer_snapshot("a").unwrap();
        assert!(!unavailable.selected_provider_ready());
        delta(&core, 1, true);
        assert!(Arc::ptr_eq(
            &unavailable,
            &core.composer_snapshot("a").unwrap()
        ));
        assert!(core.provider_runtime_request_for_test("ws").is_none());
    }
    #[test]
    fn unrelated_runtime_delta_preserves_the_selected_composer_projection() {
        let core = fixture();
        let work = core.provider_runtime_request_for_test("ws").unwrap();
        core.complete_provider_runtime_for_test(work, Ok(response(1, true)));
        let before = core.composer_snapshot("a").unwrap();
        let mut other = runtime(false);
        other.runtime_id = "other".into();
        core.observe_provider_runtime_notification(&GatewayNotification::CLIRuntimeStatusChanged(
            CLIRuntimeStatusChangedNotification {
                workspace_id: "ws".into(),
                revision: 2,
                runtime: other,
                removed: false,
            },
        ));
        assert_eq!(
            core.provider_runtime_snapshot("ws")
                .unwrap()
                .runtimes()
                .len(),
            2
        );
        assert!(Arc::ptr_eq(&before, &core.composer_snapshot("a").unwrap()));
    }
    #[test]
    fn closing_composer_preserves_another_shells_workspace_demand() {
        let core = fixture();
        let work = core.provider_runtime_request_for_test("ws").unwrap();
        core.provider_runtime_intent(ProviderRuntimeIntent::Observe {
            workspace_id: "ws".into(),
        });
        assert_eq!(core.provider_runtime_request_for_test("ws").unwrap(), work);
        core.cancel_composer_runtime("a");
        let before = core.composer_snapshot("a").unwrap();
        core.complete_provider_runtime_for_test(work, Ok(response(1, true)));
        assert_eq!(
            core.provider_runtime_snapshot("ws").unwrap().request(),
            &ProviderRuntimeRequestState::Ready
        );
        assert!(Arc::ptr_eq(&before, &core.composer_snapshot("a").unwrap()));
        core.provider_runtime_intent(ProviderRuntimeIntent::Release {
            workspace_id: "ws".into(),
        });
    }
    #[test]
    fn persistent_failure_is_bounded_and_only_explicit_retry_starts_a_new_generation() {
        let core = fixture();
        let first = core.provider_runtime_request_for_test("ws").unwrap();
        let mut request = first.clone();
        for attempt in 0..4 {
            assert_eq!(request.attempt, attempt);
            core.complete_provider_runtime_for_test(request.clone(), Err(()));
            if attempt < 3 {
                request = core.provider_runtime_request_for_test("ws").unwrap();
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
        assert!(core.provider_runtime_request_for_test("ws").is_none());
        core.composer_intent(ComposerIntent::RetryRuntimeSelection {
            thread_id: "a".into(),
            draft_id: draft.draft_id(),
        });
        let retry = core.provider_runtime_request_for_test("ws").unwrap();
        assert_ne!(first.generation, retry.generation);
        core.complete_provider_runtime_for_test(first, Ok(response(100, true)));
        assert!(
            !core
                .composer_snapshot("a")
                .unwrap()
                .selected_provider_ready()
        );
        core.complete_provider_runtime_for_test(retry, Ok(response(1, true)));
        assert!(
            core.composer_snapshot("a")
                .unwrap()
                .selected_provider_ready()
        );
    }
    #[test]
    fn a_revision_gap_fences_the_in_flight_full_response() {
        let core = fixture();
        let first = core.provider_runtime_request_for_test("ws").unwrap();
        delta(&core, 3, false);
        core.complete_provider_runtime_for_test(first.clone(), Ok(response(1, true)));
        assert!(
            !core
                .composer_snapshot("a")
                .unwrap()
                .selected_provider_ready()
        );
        let retry = core.provider_runtime_request_for_test("ws").unwrap();
        assert_eq!(retry.generation, first.generation);
        core.complete_provider_runtime_for_test(retry, Ok(response(3, false)));
        delta(&core, 4, true);
        assert!(
            core.composer_snapshot("a")
                .unwrap()
                .selected_provider_ready()
        );
        assert!(core.provider_runtime_request_for_test("ws").is_none());
    }
    #[test]
    fn route_draft_access_and_shutdown_retirement_reject_late_callbacks() {
        for scenario in 0..5 {
            let core = fixture();
            let old = core.provider_runtime_request_for_test("ws").unwrap();
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
            core.complete_provider_runtime_for_test(old, Ok(response(10, true)));
            assert_eq!(before, core.composer_snapshot("a"));
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl ClientMutationAuthority {
    /// Resolves a synthetic runtime read through the shared provider controller.
    pub fn accept_composer_runtime_for_test(
        &self,
        core: &ClientCore,
        thread: &str,
        response: pioneer_protocol::CLIRuntimeListResponse,
    ) {
        core.cancel_composer_runtime(thread);
        let draft = core.composer_snapshot(thread).unwrap();
        core.observe_composer_runtime(thread, Some(draft.draft_id()), true);
        let workspace = core
            .thread_coordinator_snapshot(thread)
            .unwrap()
            .workspace_id
            .clone();
        core.provider_runtime_intent(ProviderRuntimeIntent::Refresh {
            workspace_id: workspace.clone(),
        });
        let request = core
            .provider_runtime_request_for_test(&workspace)
            .expect("synthetic runtime request");
        core.complete_provider_runtime_for_test(request, Ok(response));
    }
}
