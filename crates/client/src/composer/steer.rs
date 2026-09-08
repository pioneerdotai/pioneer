//! Steering consumes a captured draft and active execution; shells publish only the intent.

use super::store::{
    ComposerIntent, ComposerOperationCompletion, ComposerOperationKind, ComposerOperationPlan,
    ComposerOperationStatus, ComposerPublication, DraftId,
};
use crate::core::{ClientCore, ClientTransition, ClientTransitionOutcome};
use std::sync::{Arc, mpsc};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerSteerTarget {
    pub workspace_id: String,
    pub runtime_id: String,
    pub turn_id: String,
}

struct SteerRequest {
    plan: ComposerOperationPlan,
    auth_ticket: (u64, Option<u64>),
}

#[derive(Default)]
pub(crate) struct ComposerSteerController {
    sender: Option<mpsc::SyncSender<SteerRequest>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl ComposerSteerController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
    }
}
impl Drop for ComposerSteerController {
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
    pub(super) fn composer_steer_target(&self, thread_id: &str) -> Option<ComposerSteerTarget> {
        let capability = self.thread_capability_snapshot(thread_id)?;
        if capability.request != crate::threads::capabilities::ThreadCapabilityRequestState::Ready
            || !capability
                .snapshot
                .as_ref()?
                .thread
                .as_ref()?
                .capabilities
                .can_steer_agent_execution
        {
            return None;
        }
        let source = self.thread_snapshot(thread_id)?;
        let thread = source.coordinator();
        Some(ComposerSteerTarget {
            workspace_id: thread.workspace_id.clone(),
            runtime_id: source.cli_binding()?.runtime_id.clone(),
            turn_id: thread.conversation.in_flight_turn_id()?.to_owned(),
        })
    }

    pub(super) fn submit_composer_steer(
        &self,
        thread_id: String,
        draft_id: DraftId,
    ) -> ClientTransition {
        let ticket = self.current_auth_ticket();
        let transition = self.composer_intent(ComposerIntent::BeginOperation {
            thread_id: thread_id.clone(),
            draft_id,
            operation: ComposerOperationKind::Steer,
        });
        if transition.outcome() != ClientTransitionOutcome::Changed {
            return transition;
        }
        let Some(plan) = transition
            .changes()
            .publications()
            .iter()
            .filter_map(|p| p.typed::<ComposerPublication>())
            .find(|p| p.payload().thread_id() == thread_id)
            .and_then(|p| p.payload().operation().and_then(|op| op.plan.clone()))
        else {
            return transition;
        };
        let claimed = self.composer_intent(ComposerIntent::PrepareOperation {
            identity: plan.identity.clone(),
        });
        if claimed.outcome() != ClientTransitionOutcome::Changed {
            return claimed;
        }
        let identity = plan.identity.clone();
        let queued = self
            .composer_steers
            .lock()
            .expect("composer steer controller poisoned")
            .sender
            .as_ref()
            .is_some_and(|sender| {
                sender
                    .try_send(SteerRequest {
                        plan,
                        auth_ticket: ticket,
                    })
                    .is_ok()
            });
        if !queued {
            return self.composer_intent(ComposerIntent::CompleteOperation {
                identity,
                completion: ComposerOperationCompletion::Failed {
                    message: "steer unavailable".into(),
                },
            });
        }
        claimed
    }

    fn composer_steer_is_current(&self, request: &SteerRequest) -> bool {
        !self.is_stopped()
            && self.current_auth_ticket() == request.auth_ticket
            && self.composer_steer_target(&request.plan.identity.thread_id)
                == request.plan.steer_target
            && request.plan.steer_target.is_some()
            && self
                .composer_snapshot(&request.plan.identity.thread_id)
                .is_some_and(|p| {
                    p.draft_id() == request.plan.identity.draft_id
                        && p.operation().is_some_and(|op| {
                            op.identity == request.plan.identity
                                && op.kind == ComposerOperationKind::Steer
                                && op.status == ComposerOperationStatus::Preparing
                        })
                })
    }

    fn finish_composer_steer(&self, request: &SteerRequest, result: Result<(), String>) {
        let completion = if !self.composer_steer_is_current(request) {
            ComposerOperationCompletion::Cancelled
        } else {
            match result {
                Ok(()) => ComposerOperationCompletion::Sent,
                Err(message) => ComposerOperationCompletion::Failed { message },
            }
        };
        self.complete_composer_operation(request.plan.identity.clone(), completion);
    }

    pub(crate) fn start_composer_steer_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<SteerRequest>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-composer-steer".into())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.composer_steer_is_current(&request) {
                        core.finish_composer_steer(&request, Ok(()));
                        continue;
                    }
                    let sender = core.compatibility_runtime().ws_command_sender();
                    drop(core);
                    let target = request.plan.steer_target.as_ref().unwrap();
                    let params = crate::turns::steer::plan_cli_runtime_turn_steer(
                        &target.workspace_id,
                        &target.runtime_id,
                        &request.plan.identity.thread_id,
                        &target.turn_id,
                        &request.plan.draft.text,
                    );
                    let result = params
                        .ok_or_else(|| "invalid steer request".to_owned())
                        .and_then(|params| {
                            sender
                                .cli_runtime_turn_steer(params)
                                .map(|_| ())
                                .map_err(|error| error.to_string())
                        });
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    core.finish_composer_steer(&request, result);
                }
            })
            .expect("composer steer worker could not start");
        let mut owner = self
            .composer_steers
            .lock()
            .expect("composer steer controller poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ClientMutationAuthority, ClientScope};
    use pioneer_protocol::*;

    fn fixture() -> (Arc<ClientCore>, mpsc::Receiver<SteerRequest>) {
        let core = Arc::new(ClientCore::new());
        let thread: Thread = serde_json::from_value(serde_json::json!({
            "workspace_id":"ws", "id":"a", "name":null, "preview":"", "mode":"Chat",
            "model":"model", "model_provider":"provider", "created_at":1,"updated_at":1,
            "status":"Idle", "origin_kind":"user", "sidebar_visibility":"visible", "turns":[]
        }))
        .unwrap();
        core.upsert_thread(thread);
        core.set_thread_cli_binding(
            "a",
            Some(CLIRuntimeThreadBinding {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                runtime_id: "runtime".into(),
                runtime_kind: CLIAgentRuntimeKind::Codex,
                status: "ready".into(),
            }),
        );
        core.apply_thread_conversation_event(
            "ws",
            crate::conversation::events::ConversationEvent::TurnStarted {
                thread_id: "a".into(),
                turn: serde_json::from_value(serde_json::json!({
                    "id":"turn", "items":[], "status":"InProgress", "error":null, "permission_profile": default_turn_permission_profile_snapshot()
                }))
                .unwrap(),
            },
            None,
        );
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
                        can_steer_agent_execution: true,
                        ..Default::default()
                    },
                }),
            },
        );
        core.composer_intent(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: Default::default(),
        });
        let input = core.composer_snapshot("a").unwrap();
        core.composer_intent(ComposerIntent::EditText {
            thread_id: "a".into(),
            draft_id: input.draft_id(),
            text: "  keep going  ".into(),
        });
        let (sender, receiver) = mpsc::sync_channel(1);
        core.composer_steers.lock().unwrap().sender = Some(sender);
        (core, receiver)
    }
    fn submit(core: &ClientCore, receiver: &mpsc::Receiver<SteerRequest>) -> SteerRequest {
        let input = core.composer_snapshot("a").unwrap();
        assert_eq!(
            core.composer_intent(ComposerIntent::SubmitSteer {
                thread_id: "a".into(),
                draft_id: input.draft_id()
            })
            .outcome(),
            ClientTransitionOutcome::Changed
        );
        receiver.try_recv().unwrap()
    }
    #[test]
    fn steering_captures_execution_and_only_matching_success_clears_draft() {
        let (core, receiver) = fixture();
        let request = submit(&core, &receiver);
        let target = request.plan.steer_target.as_ref().unwrap();
        assert_eq!(
            (&target.workspace_id, &target.runtime_id, &target.turn_id),
            (&"ws".into(), &"runtime".into(), &"turn".into())
        );
        assert_eq!(request.plan.draft.text, "  keep going  ");
        let pending = core.composer_snapshot("a").unwrap();
        core.composer_intent(ComposerIntent::SubmitSteer {
            thread_id: "a".into(),
            draft_id: pending.draft_id(),
        });
        assert!(receiver.try_recv().is_err());
        core.composer_intent(ComposerIntent::EditText {
            thread_id: "a".into(),
            draft_id: pending.draft_id(),
            text: pending.draft().text.clone(),
        });
        assert!(Arc::ptr_eq(&pending, &core.composer_snapshot("a").unwrap()));
        core.finish_composer_steer(&request, Ok(()));
        let complete = core.composer_snapshot("a").unwrap();
        assert!(complete.draft().text.is_empty());
        assert_ne!(complete.draft_id(), pending.draft_id());
        core.finish_composer_steer(&request, Err("duplicate".into()));
        assert!(Arc::ptr_eq(
            &complete,
            &core.composer_snapshot("a").unwrap()
        ));
    }
    #[test]
    fn changed_text_replacement_cancel_and_binding_drop_reject_late_success() {
        for scenario in 0..5 {
            let (core, receiver) = fixture();
            let binding = core.subscribe(
                ClientScope::Composer {
                    thread_id: "a".into(),
                },
                std::num::NonZeroUsize::new(8).unwrap(),
            );
            let request = submit(&core, &receiver);
            match scenario {
                0 => {
                    core.composer_intent(ComposerIntent::EditText {
                        thread_id: "a".into(),
                        draft_id: request.plan.identity.draft_id,
                        text: "new text".into(),
                    });
                }
                1 => {
                    core.composer_intent(ComposerIntent::Clear {
                        thread_id: "a".into(),
                        draft_id: request.plan.identity.draft_id,
                    });
                }
                2 => {
                    core.complete_composer_operation(
                        request.plan.identity.clone(),
                        ComposerOperationCompletion::Cancelled,
                    );
                }
                3 => drop(binding),
                _ => core.remove_thread_store("a"),
            }
            let before = core.composer_snapshot("a");
            core.finish_composer_steer(&request, Ok(()));
            match before {
                Some(before) => {
                    assert!(Arc::ptr_eq(&before, &core.composer_snapshot("a").unwrap()))
                }
                None => assert!(core.composer_snapshot("a").is_none()),
            }
        }
    }
    #[test]
    fn failed_request_preserves_text_and_full_queue_is_terminal_until_retry() {
        let (core, receiver) = fixture();
        let request = submit(&core, &receiver);
        core.finish_composer_steer(&request, Err("synthetic failure".into()));
        let failed = core.composer_snapshot("a").unwrap();
        assert_eq!(failed.draft().text, request.plan.draft.text);
        assert_eq!(
            failed.operation().unwrap().status,
            ComposerOperationStatus::Failed {
                message: "synthetic failure".into()
            }
        );
        core.composer_intent(ComposerIntent::SubmitSteer {
            thread_id: "a".into(),
            draft_id: failed.draft_id(),
        });
        let queued = core
            .composer_snapshot("a")
            .unwrap()
            .operation()
            .unwrap()
            .identity
            .clone();
        core.complete_composer_operation(queued, ComposerOperationCompletion::Cancelled);
        core.composer_intent(ComposerIntent::SubmitSteer {
            thread_id: "a".into(),
            draft_id: failed.draft_id(),
        });
        let unavailable = core.composer_snapshot("a").unwrap();
        assert!(matches!(
            unavailable.operation().unwrap().status,
            ComposerOperationStatus::Failed { .. }
        ));
        let queued = receiver.try_recv().unwrap();
        core.finish_composer_steer(&queued, Ok(()));
        assert!(Arc::ptr_eq(
            &unavailable,
            &core.composer_snapshot("a").unwrap()
        ));
    }
    #[test]
    fn lost_execution_permission_and_wrong_auth_cannot_commit() {
        for lose_permission in [false, true] {
            let (core, receiver) = fixture();
            let mut request = submit(&core, &receiver);
            if lose_permission {
                core.invalidate_thread_capabilities(Some("a"));
            } else {
                request.auth_ticket.0 += 1;
            }
            core.finish_composer_steer(&request, Ok(()));
            let input = core.composer_snapshot("a").unwrap();
            assert_eq!(input.draft().text, request.plan.draft.text);
            assert_eq!(
                input.operation().unwrap().status,
                ComposerOperationStatus::Cancelled
            );
        }
    }
}
