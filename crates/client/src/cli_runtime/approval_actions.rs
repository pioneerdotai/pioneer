//! Response workflows for the pending-request registry. Views retain only answer inputs.

use super::approvals::{
    PendingRequest, PendingRequestResolution, PendingRequestResponseAction,
    PendingRequestsReduction, plan_pending_request_response,
};
use crate::core::{
    ClientCore, ClientDemand, ClientMutationAuthority, ClientScope, ClientTransition,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, mpsc},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ApprovalActionIntent {
    Observe {
        thread_id: String,
        request_id: String,
    },
    Respond {
        thread_id: String,
        request_id: String,
        request_generation: u64,
        resolution: PendingRequestResolution,
    },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ApprovalActionState {
    Idle,
    Pending,
    Completed,
    Failed { message: String },
    Cancelled,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, PartialEq)]
pub struct ApprovalActionPublication {
    pub thread_id: String,
    pub request_id: String,
    pub revision: u64,
    pub generation: u64,
    pub request_generation: Option<u64>,
    pub request: Option<PendingRequest>,
    pub can_respond: bool,
    pub state: ApprovalActionState,
}
#[derive(Clone)]
struct ActionRequest {
    thread_id: String,
    request: PendingRequest,
    request_generation: u64,
    generation: u64,
    auth_ticket: (u64, Option<u64>),
    action: PendingRequestResponseAction,
}
#[derive(Default)]
pub(crate) struct ApprovalActionController {
    generation: u64,
    publications: BTreeMap<(String, String), Arc<ApprovalActionPublication>>,
    subscriptions: BTreeMap<(String, String), usize>,
    suspended: BTreeSet<(String, String)>,
    sender: Option<mpsc::SyncSender<ActionRequest>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl ApprovalActionController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.publications.clear();
    }
    fn next_generation(&mut self) -> u64 {
        self.generation = self
            .generation
            .checked_add(1)
            .expect("approval generation exhausted");
        self.generation
    }
}
impl Drop for ApprovalActionController {
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
    pub fn approval_action_snapshot(
        &self,
        thread: &str,
        request: &str,
    ) -> Option<Arc<ApprovalActionPublication>> {
        self.approval_actions
            .lock()
            .expect("approval action owner poisoned")
            .publications
            .get(&(thread.into(), request.into()))
            .cloned()
    }
    fn publish_approval_action(
        &self,
        owner: &mut ApprovalActionController,
        mut next: ApprovalActionPublication,
    ) -> ClientTransition {
        let scope = ClientScope::ApprovalAction {
            thread_id: next.thread_id.clone(),
            request_id: next.request_id.clone(),
        };
        let key = (next.thread_id.clone(), next.request_id.clone());
        next.revision = owner
            .publications
            .get(&key)
            .map(|p| p.revision)
            .unwrap_or_else(|| {
                self.snapshot(&scope)
                    .map_or(0, |p| p.revisions().scoped().get())
            })
            .checked_add(1)
            .expect("approval revision exhausted");
        let revision = next.revision;
        let next = Arc::new(next);
        owner.publications.insert(key, next.clone());
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(revision),
            next,
            vec![],
        )
    }
    fn can_respond_to_approval(&self, thread_id: &str, request: &PendingRequest) -> bool {
        if self
            .authorization_snapshot(None, None)
            .is_some_and(|snapshot| {
                crate::authorization::principal_presentation_capabilities(&snapshot)
                    .can_manage_all_threads
            })
        {
            return true;
        }
        self.thread_capability_snapshot(thread_id).is_some_and(|p| {
            p.request == crate::threads::capabilities::ThreadCapabilityRequestState::Ready
                && p.workspace_id == request.workspace_id
                && p.snapshot.as_ref().is_some_and(|snapshot| {
                    crate::authorization::principal_presentation_capabilities(snapshot)
                        .can_manage_all_threads
                        || snapshot
                            .thread
                            .as_ref()
                            .is_some_and(|t| t.capabilities.can_respond_to_agent_requests)
                })
        })
    }
    pub fn approval_action_intent(&self, intent: ApprovalActionIntent) -> ClientTransition {
        let (thread_id, request_id) = match &intent {
            ApprovalActionIntent::Observe {
                thread_id,
                request_id,
            }
            | ApprovalActionIntent::Respond {
                thread_id,
                request_id,
                ..
            } => (thread_id.clone(), request_id.clone()),
        };
        if self.is_stopped() || thread_id.is_empty() || request_id.is_empty() {
            return self.reject_intent();
        }
        let ticket = self.current_auth_ticket();
        let input = self.pending_request_for_action(&thread_id, &request_id);
        let can_respond = input
            .as_ref()
            .is_some_and(|(request, _)| self.can_respond_to_approval(&thread_id, request));
        let mut owner = self
            .approval_actions
            .lock()
            .expect("approval action owner poisoned");
        let key = (thread_id.clone(), request_id.clone());
        if owner.suspended.contains(&key) {
            return self.reject_intent();
        }
        let mut next = owner
            .publications
            .get(&key)
            .map(|p| (**p).clone())
            .unwrap_or(ApprovalActionPublication {
                thread_id,
                request_id,
                revision: 0,
                generation: 0,
                request_generation: None,
                request: None,
                can_respond: false,
                state: ApprovalActionState::Idle,
            });
        let request_generation = input.as_ref().map(|(_, generation)| *generation);
        let request = input.map(|(request, _)| request);
        let changed = next.request_generation != request_generation
            || next.request != request
            || next.can_respond != can_respond;
        if changed {
            next.request_generation = request_generation;
            next.request = request;
            next.can_respond = can_respond;
            next.generation = owner.next_generation();
            next.state = if next.state == ApprovalActionState::Pending {
                ApprovalActionState::Cancelled
            } else {
                ApprovalActionState::Idle
            };
        }
        match intent {
            ApprovalActionIntent::Observe { .. } => {
                if !changed && owner.publications.contains_key(&key) {
                    return self.transition(
                        &ClientMutationAuthority { _private: () },
                        vec![],
                        vec![],
                    );
                }
            }
            ApprovalActionIntent::Respond {
                request_generation,
                resolution,
                ..
            } => {
                if next.request_generation != Some(request_generation) {
                    return self.reject_intent();
                }
                let Some(request) = next.request.as_ref().filter(|_| next.can_respond) else {
                    return self.reject_intent();
                };
                if owner.publications.values().any(|p| {
                    p.request_id == next.request_id && p.state == ApprovalActionState::Pending
                }) {
                    return self.transition(
                        &ClientMutationAuthority { _private: () },
                        vec![],
                        vec![],
                    );
                }
                let action = plan_pending_request_response(request, resolution);
                next.generation = owner.next_generation();
                next.state = match action {
                    Ok(action) => {
                        let queued = owner.sender.as_ref().is_some_and(|sender| {
                            sender
                                .try_send(ActionRequest {
                                    thread_id: next.thread_id.clone(),
                                    request: request.clone(),
                                    request_generation: next.request_generation.unwrap(),
                                    generation: next.generation,
                                    auth_ticket: ticket,
                                    action,
                                })
                                .is_ok()
                        });
                        if queued {
                            ApprovalActionState::Pending
                        } else {
                            ApprovalActionState::Failed {
                                message: "Approval response unavailable".into(),
                            }
                        }
                    }
                    Err(error) => ApprovalActionState::Failed {
                        message: format!("invalid pending request response: {error:?}"),
                    },
                };
            }
        }
        self.publish_approval_action(&mut owner, next)
    }
    fn approval_request_current(&self, request: &ActionRequest) -> bool {
        !self.is_stopped()
            && self.current_auth_ticket() == request.auth_ticket
            && self.pending_request_for_action(&request.thread_id, &request.request.request_id)
                == Some((request.request.clone(), request.request_generation))
            && self.can_respond_to_approval(&request.thread_id, &request.request)
            && self
                .approval_action_snapshot(&request.thread_id, &request.request.request_id)
                .is_some_and(|p| {
                    p.generation == request.generation && p.state == ApprovalActionState::Pending
                })
    }
    fn complete_approval_action(&self, request: &ActionRequest, result: Result<(), String>) {
        let current = self.approval_request_current(request);
        let mut owner = self
            .approval_actions
            .lock()
            .expect("approval action owner poisoned");
        let key = (
            request.thread_id.clone(),
            request.request.request_id.clone(),
        );
        let Some(input) = owner.publications.get(&key).filter(|p| {
            p.generation == request.generation && p.state == ApprovalActionState::Pending
        }) else {
            return;
        };
        let mut next = (**input).clone();
        next.state = if !current {
            ApprovalActionState::Cancelled
        } else {
            match result {
                Ok(()) => {
                    if self.apply_pending_requests_matching(
                        PendingRequestsReduction::ResolvedInWorkspace {
                            workspace_id: request.request.workspace_id.clone(),
                            request_id: request.request.request_id.clone(),
                        },
                        Some((&request.request, request.request_generation)),
                    ) {
                        ApprovalActionState::Completed
                    } else {
                        ApprovalActionState::Cancelled
                    }
                }
                Err(message) => ApprovalActionState::Failed { message },
            }
        };
        if next.state == ApprovalActionState::Completed {
            next.request = None;
            next.can_respond = false;
        }
        self.publish_approval_action(&mut owner, next);
    }
    pub(crate) fn refresh_approval_actions(&self) {
        let keys = self
            .approval_actions
            .lock()
            .expect("approval action owner poisoned")
            .publications
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for (thread_id, request_id) in keys {
            self.approval_action_intent(ApprovalActionIntent::Observe {
                thread_id,
                request_id,
            });
        }
    }
    pub(crate) fn invalidate_approval_actions(&self, thread: Option<&str>) {
        let mut owner = self
            .approval_actions
            .lock()
            .expect("approval action owner poisoned");
        let entries = owner
            .publications
            .values()
            .filter(|p| thread.is_none_or(|thread| p.thread_id == thread))
            .cloned()
            .collect::<Vec<_>>();
        for entry in entries {
            let mut next = (*entry).clone();
            next.generation = owner.next_generation();
            next.request = None;
            next.can_respond = false;
            next.state = ApprovalActionState::Cancelled;
            self.publish_approval_action(&mut owner, next);
        }
    }
    pub(crate) fn approval_action_demand_changed(&self, scope: &ClientScope, demand: ClientDemand) {
        let ClientScope::ApprovalAction {
            thread_id,
            request_id,
        } = scope
        else {
            return;
        };
        let key = (thread_id.clone(), request_id.clone());
        let mut owner = self
            .approval_actions
            .lock()
            .expect("approval action owner poisoned");
        if demand != ClientDemand::Suspended {
            owner.suspended.remove(&key);
            drop(owner);
            self.approval_action_intent(ApprovalActionIntent::Observe {
                thread_id: thread_id.clone(),
                request_id: request_id.clone(),
            });
            return;
        }
        owner.suspended.insert(key.clone());
        if let Some(input) = owner.publications.get(&key).cloned() {
            let mut next = (*input).clone();
            next.generation = owner.next_generation();
            next.request = None;
            next.can_respond = false;
            next.state = ApprovalActionState::Cancelled;
            self.publish_approval_action(&mut owner, next);
            owner.publications.remove(&key);
        }
    }
    pub(crate) fn approval_action_subscription_changed(&self, scope: &ClientScope, added: bool) {
        let ClientScope::ApprovalAction {
            thread_id,
            request_id,
        } = scope
        else {
            return;
        };
        let key = (thread_id.clone(), request_id.clone());
        let mut owner = self
            .approval_actions
            .lock()
            .expect("approval action owner poisoned");
        if added {
            *owner.subscriptions.entry(key.clone()).or_default() += 1;
            owner.suspended.remove(&key);
            drop(owner);
            self.approval_action_intent(ApprovalActionIntent::Observe {
                thread_id: thread_id.clone(),
                request_id: request_id.clone(),
            });
        } else if let Some(count) = owner.subscriptions.get_mut(&key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                owner.subscriptions.remove(&key);
                drop(owner);
                self.approval_action_demand_changed(scope, ClientDemand::Suspended);
                self.approval_actions
                    .lock()
                    .expect("approval action owner poisoned")
                    .suspended
                    .remove(&key);
            }
        }
    }
    pub(crate) fn start_approval_action_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<ActionRequest>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-approval-action".into())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.approval_request_current(&request) {
                        core.complete_approval_action(&request, Ok(()));
                        continue;
                    }
                    let sender = core.transport_runtime().ws_command_sender();
                    drop(core);
                    let result = match &request.action {
                        PendingRequestResponseAction::CLIRuntime { params, .. } => sender
                            .cli_runtime_request_respond(params.clone())
                            .map_err(|error| format!("{error:#}"))
                            .and_then(|response| {
                                if response.workspace_id == params.workspace_id
                                    && response.runtime_id == params.runtime_id
                                    && response.request_id == params.request_id
                                {
                                    Ok(())
                                } else {
                                    Err("Approval response identity mismatch".into())
                                }
                            }),
                        PendingRequestResponseAction::NativePermissionGate { params, .. } => sender
                            .turn_permission_request_respond(params.clone())
                            .map_err(|error| format!("{error:#}"))
                            .and_then(|response| {
                                if response.request_id == params.request_id {
                                    Ok(())
                                } else {
                                    Err("Approval response identity mismatch".into())
                                }
                            }),
                    };
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    core.complete_approval_action(&request, result);
                }
            })
            .expect("approval worker could not start");
        let mut owner = self
            .approval_actions
            .lock()
            .expect("approval action owner poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ClientTransitionOutcome;
    use pioneer_protocol::*;

    fn pending(id: &str, native: bool) -> PendingRequest {
        if native {
            PendingRequest::from_native_permission_request(TurnPermissionApprovalRequest {
                request_id: id.into(),
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                turn_id: "turn".into(),
                visible_thread_ids: vec![],
                tool_name: "exec_command".into(),
                action: TurnPermissionActionKind::ShellCommand,
                scope_hash: "scope".into(),
                reason: TurnPermissionDecisionReason::PolicyRequiresApproval,
                summary: Some("Approve command".into()),
                details: vec![],
            })
        } else {
            PendingRequest::from_cli_runtime_opened_notification(
                CLIRuntimeRequestOpenedNotification {
                    workspace_id: "ws".into(),
                    runtime_id: "runtime".into(),
                    request_id: id.into(),
                    thread_id: Some("a".into()),
                    turn_id: Some("turn".into()),
                    item_id: None,
                    visible_thread_ids: vec![],
                    request: CLIRuntimePendingRequest {
                        kind: CLIRuntimeRequestKind::CommandApproval,
                        title: Some("Run command".into()),
                        message: None,
                        native_request_id: None,
                        payload: None,
                    },
                },
            )
        }
    }
    fn fixture() -> (Arc<ClientCore>, mpsc::Receiver<ActionRequest>) {
        let core = Arc::new(ClientCore::new());
        let thread: Thread = serde_json::from_value(serde_json::json!({"workspace_id":"ws", "id":"a", "preview":"", "mode":"Agent", "model":"m", "model_provider":"p", "created_at":1,"updated_at":1,"status":"Idle","origin_kind":"user","sidebar_visibility":"visible","turns":[]})).unwrap();
        core.upsert_thread(thread);
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
                workspace: None,
                thread: Some(AuthorizationThreadCapabilitySnapshot {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    capabilities: AuthorizationThreadCapabilities {
                        can_respond_to_agent_requests: true,
                        ..Default::default()
                    },
                }),
            },
        );
        let (sender, receiver) = mpsc::sync_channel(1);
        core.approval_actions.lock().unwrap().sender = Some(sender);
        (core, receiver)
    }
    fn respond(core: &ClientCore, id: &str) -> ClientTransition {
        core.approval_action_intent(ApprovalActionIntent::Respond {
            thread_id: "a".into(),
            request_id: id.into(),
            request_generation: core.pending_request_for_action("a", id).unwrap().1,
            resolution: PendingRequestResolution::Allow,
        })
    }
    #[test]
    fn both_origins_use_canonical_request_and_only_matching_completion_resolves() {
        for native in [false, true] {
            let (core, receiver) = fixture();
            core.apply_pending_requests(PendingRequestsReduction::Opened(pending(
                "request", native,
            )));
            core.apply_pending_requests(PendingRequestsReduction::Opened(pending("other", native)));
            assert_eq!(
                respond(&core, "request").outcome(),
                ClientTransitionOutcome::Changed
            );
            let request = receiver.try_recv().unwrap();
            assert_eq!(
                matches!(
                    request.action,
                    PendingRequestResponseAction::NativePermissionGate { .. }
                ),
                native
            );
            let before = core.approval_action_snapshot("a", "request").unwrap();
            assert_eq!(
                respond(&core, "request").outcome(),
                ClientTransitionOutcome::Noop
            );
            assert!(receiver.try_recv().is_err());
            let mut stale = request.clone();
            stale.generation += 1;
            core.complete_approval_action(&stale, Ok(()));
            assert!(Arc::ptr_eq(
                &before,
                &core.approval_action_snapshot("a", "request").unwrap()
            ));
            core.complete_approval_action(&request, Ok(()));
            let after = core.approval_action_snapshot("a", "request").unwrap();
            assert_eq!(after.state, ApprovalActionState::Completed);
            assert!(core.pending_request_for_action("a", "request").is_none());
            assert!(core.pending_request_for_action("a", "other").is_some());
            core.complete_approval_action(&request, Err("duplicate".into()));
            assert!(Arc::ptr_eq(
                &after,
                &core.approval_action_snapshot("a", "request").unwrap()
            ));
        }
    }
    #[test]
    fn failure_is_bounded_until_explicit_retry_and_keeps_registry_entry() {
        let (core, receiver) = fixture();
        core.apply_pending_requests(PendingRequestsReduction::Opened(pending("request", false)));
        respond(&core, "request");
        let request = receiver.try_recv().unwrap();
        core.complete_approval_action(&request, Err("persistent failure".into()));
        let before = core.approval_action_snapshot("a", "request").unwrap();
        for _ in 0..20 {
            core.refresh_approval_actions();
        }
        assert!(receiver.try_recv().is_err());
        assert!(Arc::ptr_eq(
            &before,
            &core.approval_action_snapshot("a", "request").unwrap()
        ));
        assert!(core.pending_request_for_action("a", "request").is_some());
        respond(&core, "request");
        let retry = receiver.try_recv().unwrap();
        assert_ne!(request.generation, retry.generation);
        core.complete_approval_action(&request, Ok(()));
        assert!(core.pending_request_for_action("a", "request").is_some());
        core.complete_approval_action(&retry, Ok(()));
        assert!(core.pending_request_for_action("a", "request").is_none());
    }
    #[test]
    fn reopen_with_same_id_and_payload_has_a_new_incarnation() {
        let (core, receiver) = fixture();
        let entry = pending("request", false);
        core.apply_pending_requests(PendingRequestsReduction::Opened(entry.clone()));
        respond(&core, "request");
        let old = receiver.try_recv().unwrap();
        assert!(!core.apply_pending_requests(PendingRequestsReduction::Opened(entry.clone())));
        assert_eq!(
            core.pending_request_for_action("a", "request").unwrap().1,
            old.request_generation
        );
        core.apply_pending_requests(PendingRequestsReduction::Resolved {
            request_id: "request".into(),
        });
        core.apply_pending_requests(PendingRequestsReduction::Opened(entry));
        let fresh = core.pending_request_for_action("a", "request").unwrap();
        assert_ne!(fresh.1, old.request_generation);
        core.complete_approval_action(&old, Ok(()));
        assert_eq!(
            core.pending_request_for_action("a", "request"),
            Some(fresh.clone())
        );
        assert!(!core.apply_pending_requests_matching(
            PendingRequestsReduction::Resolved {
                request_id: "request".into()
            },
            Some((&old.request, old.request_generation))
        ));
        assert_eq!(core.pending_request_for_action("a", "request"), Some(fresh));
    }
    #[test]
    fn unmount_access_loss_suspension_and_shutdown_fence_completions() {
        for scenario in 0..4 {
            let (core, receiver) = fixture();
            core.apply_pending_requests(PendingRequestsReduction::Opened(pending(
                "request", false,
            )));
            let scope = ClientScope::ApprovalAction {
                thread_id: "a".into(),
                request_id: "request".into(),
            };
            let subscription =
                core.subscribe(scope.clone(), std::num::NonZeroUsize::new(8).unwrap());
            respond(&core, "request");
            let request = receiver.try_recv().unwrap();
            match scenario {
                0 => drop(subscription),
                1 => core.clear_authorization_projections(),
                2 => core.approval_action_demand_changed(&scope, ClientDemand::Suspended),
                _ => core.shutdown(),
            }
            let before_request = core.pending_request_for_action("a", "request");
            let before = core.approval_action_snapshot("a", "request");
            core.complete_approval_action(&request, Ok(()));
            assert_eq!(before, core.approval_action_snapshot("a", "request"));
            assert_eq!(
                before_request,
                core.pending_request_for_action("a", "request")
            );
        }
    }
    #[test]
    fn full_queue_and_wrong_thread_do_not_create_unowned_work() {
        let (core, receiver) = fixture();
        core.apply_pending_requests(PendingRequestsReduction::Opened(pending("one", false)));
        core.apply_pending_requests(PendingRequestsReduction::Opened(pending("two", false)));
        respond(&core, "one");
        respond(&core, "two");
        assert!(matches!(
            core.approval_action_snapshot("a", "two").unwrap().state,
            ApprovalActionState::Failed { .. }
        ));
        assert_eq!(receiver.try_recv().unwrap().request.request_id, "one");
        assert!(receiver.try_recv().is_err());
        assert_eq!(
            core.approval_action_intent(ApprovalActionIntent::Respond {
                thread_id: "wrong".into(),
                request_id: "one".into(),
                request_generation: core.pending_request_for_action("a", "one").unwrap().1,
                resolution: PendingRequestResolution::Allow
            })
            .outcome(),
            ClientTransitionOutcome::Rejected
        );
    }
}
