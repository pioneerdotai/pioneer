//! Cancellation requests retain the original turn and thread-store incarnation.
use crate::{
    core::{ClientCore, ClientDemand, ClientMutationAuthority, ClientScope, ClientTransition},
    threads::registry::ThreadOperationToken,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, mpsc},
};
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TurnCancellationIdentity {
    pub thread_id: String,
    pub turn_id: String,
    pub generation: u64,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TurnCancellationState {
    Pending,
    Completed,
    Failed { message: String },
    Cancelled,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TurnCancellationPublication {
    pub identity: TurnCancellationIdentity,
    pub revision: u64,
    pub state: TurnCancellationState,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TurnCancellationIntent {
    pub thread_id: String,
    pub reason: Option<String>,
}
#[derive(Clone)]
struct CancellationRequest {
    identity: TurnCancellationIdentity,
    token: ThreadOperationToken,
    params: pioneer_protocol::TurnCancelParams,
    auth: (u64, Option<u64>),
}
#[derive(Default)]
pub(crate) struct TurnCancellationController {
    generation: u64,
    publications: BTreeMap<String, Arc<TurnCancellationPublication>>,
    requests: BTreeMap<String, CancellationRequest>,
    suspended: BTreeSet<String>,
    subscriptions: BTreeMap<String, usize>,
    sender: Option<mpsc::SyncSender<CancellationRequest>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl TurnCancellationController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.requests.clear();
        self.publications.clear();
    }
}
impl Drop for TurnCancellationController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take()
            && task.thread().id() != std::thread::current().id()
        {
            let _ = task.join();
        }
    }
}
impl ClientCore {
    pub fn turn_cancellation_snapshot(
        &self,
        thread: &str,
    ) -> Option<Arc<TurnCancellationPublication>> {
        self.turn_cancellations
            .lock()
            .expect("turn cancellations poisoned")
            .publications
            .get(thread)
            .cloned()
    }
    fn publish_turn_cancellation(
        &self,
        owner: &mut TurnCancellationController,
        mut next: TurnCancellationPublication,
    ) -> ClientTransition {
        let scope = ClientScope::TurnCancellation {
            thread_id: next.identity.thread_id.clone(),
        };
        next.revision = owner
            .publications
            .get(&next.identity.thread_id)
            .map_or_else(
                || {
                    self.snapshot(&scope)
                        .map_or(0, |p| p.revisions().scoped().get())
                },
                |p| p.revision,
            )
            .checked_add(1)
            .expect("turn cancellation revision exhausted");
        let revision = next.revision;
        let next = Arc::new(next);
        owner
            .publications
            .insert(next.identity.thread_id.clone(), next.clone());
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(revision),
            next,
            vec![],
        )
    }
    pub fn request_turn_cancellation(&self, intent: TurnCancellationIntent) -> ClientTransition {
        if self.is_stopped()
            || !self
                .thread_capability_snapshot(&intent.thread_id)
                .and_then(|p| p.snapshot.clone())
                .and_then(|p| p.thread)
                .is_some_and(|p| p.capabilities.can_cancel_agent_execution)
        {
            return self.reject_intent();
        }
        let auth = self.current_auth_ticket();
        let mut owner = self
            .turn_cancellations
            .lock()
            .expect("turn cancellations poisoned");
        if let Some(previous) = owner.requests.get(&intent.thread_id) {
            let replaced = self.thread_operation_token(&intent.thread_id).as_ref()
                != Some(&previous.token)
                || self
                    .thread_coordinator_snapshot(&intent.thread_id)
                    .is_none_or(|p| {
                        p.conversation.in_flight_turn_id()
                            != Some(previous.identity.turn_id.as_str())
                    });
            if replaced {
                owner.requests.remove(&intent.thread_id);
            }
        }
        if owner.requests.len() >= 64 {
            return self.reject_intent();
        }
        if owner.suspended.contains(&intent.thread_id)
            || owner.requests.contains_key(&intent.thread_id)
        {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        }
        let Some((token, params)) =
            self.begin_thread_turn_cancellation(&intent.thread_id, intent.reason)
        else {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        };
        owner.generation = owner
            .generation
            .checked_add(1)
            .expect("turn cancellation generation exhausted");
        let identity = TurnCancellationIdentity {
            thread_id: intent.thread_id.clone(),
            turn_id: params.turn_id.clone(),
            generation: owner.generation,
        };
        let request = CancellationRequest {
            identity: identity.clone(),
            token,
            params,
            auth,
        };
        owner.requests.insert(intent.thread_id, request.clone());
        let transition = self.publish_turn_cancellation(
            &mut owner,
            TurnCancellationPublication {
                identity,
                revision: 0,
                state: TurnCancellationState::Pending,
            },
        );
        let queued = owner
            .sender
            .as_ref()
            .is_some_and(|tx| tx.try_send(request.clone()).is_ok());
        drop(owner);
        if !queued {
            self.complete_turn_cancellation(request, Err("Turn cancellation unavailable".into()));
        }
        transition
    }
    fn turn_cancellation_matches(&self, request: &CancellationRequest) -> bool {
        !self.is_stopped()
            && self.current_auth_ticket() == request.auth
            && self
                .thread_operation_token(&request.identity.thread_id)
                .as_ref()
                == Some(&request.token)
            && self
                .turn_cancellations
                .lock()
                .expect("turn cancellations poisoned")
                .requests
                .get(&request.identity.thread_id)
                .is_some_and(|r| r.identity == request.identity)
    }
    fn complete_turn_cancellation(
        &self,
        request: CancellationRequest,
        result: Result<pioneer_protocol::TurnCancelResponse, String>,
    ) {
        if !self.turn_cancellation_matches(&request) {
            return;
        }
        let mut owner = self
            .turn_cancellations
            .lock()
            .expect("turn cancellations poisoned");
        if owner
            .requests
            .get(&request.identity.thread_id)
            .is_none_or(|r| r.identity != request.identity)
        {
            return;
        }
        let (event, state) = match result {
            Ok(response)
                if response.thread_id == request.identity.thread_id
                    && response.turn.id == request.identity.turn_id =>
            {
                (
                    super::cancel::turn_cancel_response_event(response),
                    TurnCancellationState::Completed,
                )
            }
            Ok(_) => {
                let message = "Mismatched turn cancellation response".to_owned();
                (
                    Some(super::cancel::local_turn_cancel_rejected_event(
                        &request.identity.thread_id,
                        &request.identity.turn_id,
                        &message,
                    )),
                    TurnCancellationState::Failed { message },
                )
            }
            Err(message) => (
                Some(super::cancel::local_turn_cancel_rejected_event(
                    &request.identity.thread_id,
                    &request.identity.turn_id,
                    &message,
                )),
                TurnCancellationState::Failed { message },
            ),
        };
        let matched = self.finish_thread_turn_cancellation(
            &request.token,
            &request.identity.turn_id,
            event,
            false,
        );
        owner.requests.remove(&request.identity.thread_id);
        self.publish_turn_cancellation(
            &mut owner,
            TurnCancellationPublication {
                identity: request.identity,
                revision: 0,
                state: if matched {
                    state
                } else {
                    TurnCancellationState::Cancelled
                },
            },
        );
    }
    pub(crate) fn invalidate_turn_cancellations(&self, thread: Option<&str>) {
        let mut owner = self
            .turn_cancellations
            .lock()
            .expect("turn cancellations poisoned");
        let ids: Vec<_> = owner
            .publications
            .keys()
            .filter(|id| thread.is_none_or(|t| t == id.as_str()))
            .cloned()
            .collect();
        for id in ids {
            if let Some(request) = owner.requests.remove(&id) {
                self.finish_thread_turn_cancellation(
                    &request.token,
                    &request.identity.turn_id,
                    None,
                    true,
                );
            }
            if let Some(input) = owner.publications.get(&id).cloned() {
                self.publish_turn_cancellation(
                    &mut owner,
                    TurnCancellationPublication {
                        state: TurnCancellationState::Cancelled,
                        ..(*input).clone()
                    },
                );
            }
            owner.publications.remove(&id);
        }
    }
    pub(crate) fn turn_cancellation_subscription_changed(&self, scope: &ClientScope, added: bool) {
        let ClientScope::TurnCancellation { thread_id } = scope else {
            return;
        };
        let mut owner = self
            .turn_cancellations
            .lock()
            .expect("turn cancellations poisoned");
        let count = owner.subscriptions.entry(thread_id.clone()).or_default();
        if added {
            *count += 1;
            owner.suspended.remove(thread_id);
        } else {
            *count = count.saturating_sub(1);
            if *count == 0 {
                owner.subscriptions.remove(thread_id);
                drop(owner);
                self.invalidate_turn_cancellations(Some(thread_id));
            }
        }
    }
    pub(crate) fn turn_cancellation_demand_changed(
        &self,
        scope: &ClientScope,
        demand: ClientDemand,
    ) {
        let ClientScope::TurnCancellation { thread_id } = scope else {
            return;
        };
        let mut owner = self
            .turn_cancellations
            .lock()
            .expect("turn cancellations poisoned");
        if demand != ClientDemand::Suspended {
            owner.suspended.remove(thread_id);
            return;
        }
        owner.suspended.insert(thread_id.clone());
        drop(owner);
        self.invalidate_turn_cancellations(Some(thread_id));
    }
    pub(crate) fn start_turn_cancellation_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<CancellationRequest>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-turn-cancellation".into())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.turn_cancellation_matches(&request) {
                        continue;
                    }
                    let sender = core.transport_runtime().ws_command_sender();
                    drop(core);
                    let result = sender
                        .turn_cancel(request.params.clone())
                        .map_err(|e| format!("{e:#}"));
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    core.complete_turn_cancellation(request, result);
                }
            })
            .expect("turn cancellation worker could not start");
        let mut owner = self
            .turn_cancellations
            .lock()
            .expect("turn cancellations poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        composer::store::ComposerIntent, conversation::events::ConversationEvent,
        core::ClientTransitionOutcome,
    };
    use pioneer_protocol::*;
    fn fixture() -> (Arc<ClientCore>, mpsc::Receiver<CancellationRequest>) {
        let core = Arc::new(ClientCore::new());
        core.upsert_thread(serde_json::from_value(serde_json::json!({
            "workspace_id":"ws", "id":"a", "preview":"", "mode":"Agent", "model":"model", "model_provider":"provider", "created_at":1,"updated_at":1,"status":"Idle","origin_kind":"user","sidebar_visibility":"visible","turns":[]
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
                        can_cancel_agent_execution: true,
                        ..Default::default()
                    },
                }),
            },
        );
        core.composer_intent(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: crate::composer::state_machine::ComposerDomainState {
                selected_mode: ThreadMode::Agent,
                ..Default::default()
            },
        });
        let (sender, receiver) = mpsc::sync_channel(64);
        core.turn_cancellations.lock().unwrap().sender = Some(sender);
        start(&core, "turn");
        (core, receiver)
    }

    fn start(core: &ClientCore, turn: &str) {
        let mut source = core.existing_thread_mutation("a").unwrap();
        source.conversation.reset();
        source
            .conversation
            .apply(ConversationEvent::LocalTurnStartRequested {
                thread_id: "a".into(),
                turn_id: turn.into(),
                pending_request_id: "request".into(),
                mode: ThreadMode::Agent,
                user_text: "text".into(),
                attachments: vec![],
            });
        source
            .conversation
            .apply(ConversationEvent::LocalTurnStartAccepted {
                thread_id: "a".into(),
                turn_id: turn.into(),
                pending_request_id: "request".into(),
                mode: ThreadMode::Agent,
            });
    }
    fn request(core: &ClientCore) -> ClientTransitionOutcome {
        core.request_turn_cancellation(TurnCancellationIntent {
            thread_id: "a".into(),
            reason: Some("Synthetic stop".into()),
        })
        .outcome()
    }
    fn response(turn: &str) -> TurnCancelResponse {
        TurnCancelResponse {
            thread_id: "a".into(),
            workspace_id: "ws".into(),
            turn: Turn {
                id: turn.into(),
                status: TurnStatus::Interrupted,
                turn_kind: Default::default(),
                origin: Default::default(),
                mode: ThreadMode::Agent,
                author: None,
                reply_to_turn_id: None,
                mentions: vec![],
                message_revision: 0,
                message_deleted: false,
                error: None,
                prompt_manifest: None,
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
            },
        }
    }
    #[test]
    fn failure_retry_and_duplicate_completion_preserve_the_draft() {
        let (core, rx) = fixture();
        let draft = core.composer_snapshot("a").unwrap();
        assert_eq!(request(&core), ClientTransitionOutcome::Changed);
        let first = rx.try_recv().unwrap();
        for _ in 0..50 {
            assert_eq!(request(&core), ClientTransitionOutcome::Noop);
        }
        assert!(rx.try_recv().is_err());
        core.complete_turn_cancellation(first.clone(), Err("persistent".into()));
        let source = core.thread_coordinator_snapshot("a").unwrap();
        assert_eq!(source.conversation.status_label(), "running");
        assert_eq!(
            source.conversation.projection().last_error.as_deref(),
            Some("persistent")
        );
        assert!(Arc::ptr_eq(&draft, &core.composer_snapshot("a").unwrap()));
        assert_eq!(request(&core), ClientTransitionOutcome::Changed);
        let retry = rx.try_recv().unwrap();
        assert_ne!(retry.identity.generation, first.identity.generation);
        core.complete_turn_cancellation(first, Ok(response("turn")));
        assert_eq!(
            core.turn_cancellation_snapshot("a").unwrap().state,
            TurnCancellationState::Pending
        );
        core.complete_turn_cancellation(retry.clone(), Ok(response("turn")));
        let ready = core.turn_cancellation_snapshot("a").unwrap();
        assert_eq!(ready.state, TurnCancellationState::Completed);
        core.complete_turn_cancellation(retry, Err("duplicate".into()));
        assert!(Arc::ptr_eq(
            &ready,
            &core.turn_cancellation_snapshot("a").unwrap()
        ));
        assert!(Arc::ptr_eq(&draft, &core.composer_snapshot("a").unwrap()));
    }
    #[test]
    fn a_new_turn_accepts_its_own_cancel_and_rejects_the_previous_turn_result() {
        let (core, rx) = fixture();
        request(&core);
        let old = rx.try_recv().unwrap();
        start(&core, "new-turn");
        assert_eq!(request(&core), ClientTransitionOutcome::Changed);
        let new = rx.try_recv().unwrap();
        core.complete_turn_cancellation(old, Ok(response("turn")));
        assert_eq!(
            core.turn_cancellation_snapshot("a").unwrap().identity,
            new.identity
        );
        assert!(
            core.thread_coordinator_snapshot("a")
                .unwrap()
                .conversation
                .is_cancelling_turn()
        );
        core.complete_turn_cancellation(new, Ok(response("new-turn")));
        assert_eq!(
            core.thread_coordinator_snapshot("a")
                .unwrap()
                .conversation
                .status_label(),
            "cancelled"
        );
    }
    #[test]
    fn binding_drop_restores_running_without_error_and_rejects_late_rpc() {
        let (core, rx) = fixture();
        let lease = core.subscribe(
            ClientScope::TurnCancellation {
                thread_id: "a".into(),
            },
            std::num::NonZeroUsize::new(8).unwrap(),
        );
        request(&core);
        let work = rx.try_recv().unwrap();
        drop(lease);
        let source = core.thread_coordinator_snapshot("a").unwrap();
        assert_eq!(source.conversation.status_label(), "running");
        assert!(source.conversation.projection().last_error.is_none());
        core.complete_turn_cancellation(work, Err("late".into()));
        assert!(core.turn_cancellation_snapshot("a").is_none());
        assert!(
            core.thread_coordinator_snapshot("a")
                .unwrap()
                .conversation
                .projection()
                .last_error
                .is_none()
        );
    }
    #[test]
    fn transport_disconnect_retires_request_before_advancing_thread_incarnation() {
        let (core, rx) = fixture();
        request(&core);
        let work = rx.try_recv().unwrap();
        core.cancel_thread_requests();
        let source = core.thread_coordinator_snapshot("a").unwrap();
        assert_eq!(source.conversation.status_label(), "running");
        assert!(source.conversation.projection().last_error.is_none());
        core.complete_turn_cancellation(work, Ok(response("turn")));
        assert!(core.turn_cancellation_snapshot("a").is_none());
        assert_eq!(
            core.thread_coordinator_snapshot("a")
                .unwrap()
                .conversation
                .status_label(),
            "running"
        );
        assert_eq!(request(&core), ClientTransitionOutcome::Changed);
    }
    #[test]
    fn suspension_access_loss_thread_retirement_and_shutdown_reject_late_completion() {
        for scenario in 0..4 {
            let (core, rx) = fixture();
            request(&core);
            let work = rx.try_recv().unwrap();
            match scenario {
                0 => core.turn_cancellation_demand_changed(
                    &ClientScope::TurnCancellation {
                        thread_id: "a".into(),
                    },
                    ClientDemand::Suspended,
                ),
                1 => core.clear_authorization_projections(),
                2 => core.remove_thread_store("a"),
                _ => core.shutdown(),
            }
            let before = core.turn_cancellation_snapshot("a");
            core.complete_turn_cancellation(work, Ok(response("turn")));
            assert_eq!(before, core.turn_cancellation_snapshot("a"));
        }
    }
}
