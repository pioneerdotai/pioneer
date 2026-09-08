//! Confirmed message deletion, scoped to one immutable message revision and operation.

use crate::core::{
    ClientCore, ClientDemand, ClientMutationAuthority, ClientScope, ClientTransition,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, mpsc},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct MessageDeletionIdentity {
    pub thread_id: String,
    pub generation: u64,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct MessageDeletionPlan {
    pub identity: MessageDeletionIdentity,
    pub workspace_id: String,
    pub turn_id: String,
    pub expected_revision: u64,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MessageDeletionState {
    Confirming,
    Pending,
    Completed,
    Failed { conflicted: bool },
    Cancelled,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
pub struct MessageDeletionPublication {
    pub thread_id: String,
    pub revision: u64,
    pub request_generation: u64,
    pub plan: MessageDeletionPlan,
    pub state: MessageDeletionState,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MessageDeletionIntent {
    Begin {
        thread_id: String,
        turn_id: String,
        expected_revision: u64,
    },
    Confirm {
        identity: MessageDeletionIdentity,
    },
    Cancel {
        identity: MessageDeletionIdentity,
    },
}
#[derive(Clone)]
struct DeletionRequest {
    request_generation: u64,
    plan: MessageDeletionPlan,
    auth_ticket: (u64, Option<u64>),
}
#[derive(Default)]
pub(crate) struct MessageDeletionController {
    generation: u64,
    publications: BTreeMap<String, Arc<MessageDeletionPublication>>,
    subscriptions: BTreeMap<String, usize>,
    suspended: BTreeSet<String>,
    sender: Option<mpsc::SyncSender<DeletionRequest>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl MessageDeletionController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.publications.clear();
    }
    fn generation(&mut self) -> u64 {
        self.generation = self
            .generation
            .checked_add(1)
            .expect("message deletion generation exhausted");
        self.generation
    }
}
impl Drop for MessageDeletionController {
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
    pub fn message_deletion_snapshot(
        &self,
        thread: &str,
    ) -> Option<Arc<MessageDeletionPublication>> {
        self.message_deletions
            .lock()
            .expect("message deletion owner poisoned")
            .publications
            .get(thread)
            .cloned()
    }
    fn publish_message_deletion(
        &self,
        owner: &mut MessageDeletionController,
        mut next: MessageDeletionPublication,
    ) -> ClientTransition {
        let scope = ClientScope::MessageDeletion {
            thread_id: next.thread_id.clone(),
        };
        next.revision = owner
            .publications
            .get(&next.thread_id)
            .map(|p| p.revision)
            .unwrap_or_else(|| {
                self.snapshot(&scope)
                    .map_or(0, |p| p.revisions().scoped().get())
            })
            .checked_add(1)
            .expect("message deletion revision exhausted");
        let revision = next.revision;
        let next = Arc::new(next);
        owner
            .publications
            .insert(next.thread_id.clone(), next.clone());
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            super::registry::revisions(revision),
            next,
            vec![],
        )
    }
    fn message_deletion_target(&self, thread_id: &str, turn_id: &str) -> Option<(String, u64)> {
        let source = self.thread_snapshot(thread_id)?;
        let principal = pioneer_protocol::PrincipalId::new(source.current_principal_id()?).ok()?;
        let publication = self
            .snapshot(&ClientScope::Timeline {
                thread_id: thread_id.into(),
            })?
            .typed::<crate::timeline::presentation::TimelineSnapshot>()?;
        publication.payload().rows().iter().find_map(|row| {
            let crate::timeline::presentation::TimelineRenderRow::Timeline(row) = row.value()
            else {
                return None;
            };
            let crate::timeline::rows::TimelineRowKind::UserMessage { presentation, .. } =
                &row.kind
            else {
                return None;
            };
            (presentation.thread_id == thread_id
                && presentation.turn_id == turn_id
                && crate::timeline::rows::user_message_mutation_availability(
                    presentation,
                    &principal,
                )
                .can_delete)
                .then(|| (presentation.workspace_id.clone(), presentation.revision))
        })
    }
    pub fn message_deletion_intent(&self, intent: MessageDeletionIntent) -> ClientTransition {
        if self.is_stopped() {
            return self.reject_intent();
        }
        let thread_id = match &intent {
            MessageDeletionIntent::Begin { thread_id, .. } => thread_id.clone(),
            MessageDeletionIntent::Confirm { identity }
            | MessageDeletionIntent::Cancel { identity } => identity.thread_id.clone(),
        };
        let target = if let MessageDeletionIntent::Begin { turn_id, .. } = &intent {
            self.message_deletion_target(&thread_id, turn_id)
        } else {
            None
        };
        let ticket = self.current_auth_ticket();
        let mut owner = self
            .message_deletions
            .lock()
            .expect("message deletion owner poisoned");
        if owner.suspended.contains(&thread_id) {
            return self.reject_intent();
        }
        let next = match intent {
            MessageDeletionIntent::Begin {
                turn_id,
                expected_revision,
                ..
            } => {
                let Some((workspace_id, revision)) = target else {
                    return self.reject_intent();
                };
                if revision != expected_revision {
                    return self.reject_intent();
                }
                if owner
                    .publications
                    .get(&thread_id)
                    .is_some_and(|p| p.state == MessageDeletionState::Pending)
                {
                    return self.reject_intent();
                }
                if let Some(current) = owner.publications.get(&thread_id).filter(|p| {
                    p.state == MessageDeletionState::Confirming
                        && p.plan.turn_id == turn_id
                        && p.plan.expected_revision == expected_revision
                }) {
                    let _ = current;
                    return self.transition(
                        &ClientMutationAuthority { _private: () },
                        vec![],
                        vec![],
                    );
                }
                MessageDeletionPublication {
                    thread_id: thread_id.clone(),
                    revision: 0,
                    request_generation: 0,
                    state: MessageDeletionState::Confirming,
                    plan: MessageDeletionPlan {
                        identity: MessageDeletionIdentity {
                            thread_id: thread_id.clone(),
                            generation: owner.generation(),
                        },
                        workspace_id,
                        turn_id,
                        expected_revision,
                    },
                }
            }
            MessageDeletionIntent::Confirm { identity } => {
                let Some(current) = owner.publications.get(&thread_id).filter(|p| {
                    p.plan.identity == identity
                        && matches!(
                            p.state,
                            MessageDeletionState::Confirming
                                | MessageDeletionState::Failed { conflicted: false }
                        )
                }) else {
                    return self.transition(
                        &ClientMutationAuthority { _private: () },
                        vec![],
                        vec![],
                    );
                };
                let mut next = (**current).clone();
                next.request_generation = owner.generation();
                let queued = owner.sender.as_ref().is_some_and(|sender| {
                    sender
                        .try_send(DeletionRequest {
                            plan: next.plan.clone(),
                            request_generation: next.request_generation,
                            auth_ticket: ticket,
                        })
                        .is_ok()
                });
                next.state = if queued {
                    MessageDeletionState::Pending
                } else {
                    MessageDeletionState::Failed { conflicted: false }
                };
                next
            }
            MessageDeletionIntent::Cancel { identity } => {
                let Some(current) = owner.publications.get(&thread_id).filter(|p| {
                    p.plan.identity == identity
                        && p.state != MessageDeletionState::Cancelled
                        && p.state != MessageDeletionState::Completed
                }) else {
                    return self.transition(
                        &ClientMutationAuthority { _private: () },
                        vec![],
                        vec![],
                    );
                };
                let mut next = (**current).clone();
                next.state = MessageDeletionState::Cancelled;
                next
            }
        };
        self.publish_message_deletion(&mut owner, next)
    }
    fn deletion_request_current(&self, request: &DeletionRequest) -> bool {
        !self.is_stopped()
            && self.current_auth_ticket() == request.auth_ticket
            && self
                .message_deletion_snapshot(&request.plan.identity.thread_id)
                .is_some_and(|p| {
                    p.plan.identity == request.plan.identity
                        && p.request_generation == request.request_generation
                        && p.state == MessageDeletionState::Pending
                })
    }
    fn complete_message_deletion(&self, request: &DeletionRequest, result: Result<(), bool>) {
        let current = self.deletion_request_current(request);
        let mut owner = self
            .message_deletions
            .lock()
            .expect("message deletion owner poisoned");
        let Some(input) = owner
            .publications
            .get(&request.plan.identity.thread_id)
            .filter(|p| {
                p.plan.identity == request.plan.identity
                    && p.request_generation == request.request_generation
                    && p.state == MessageDeletionState::Pending
            })
        else {
            return;
        };
        let mut next = (**input).clone();
        let refresh = current && (result.is_ok() || result == Err(true));
        next.state = if !current {
            MessageDeletionState::Cancelled
        } else {
            match result {
                Ok(()) => MessageDeletionState::Completed,
                Err(conflicted) => MessageDeletionState::Failed { conflicted },
            }
        };
        self.publish_message_deletion(&mut owner, next);
        drop(owner);
        if refresh {
            self.refresh_thread_timeline(&request.plan.identity.thread_id);
            self.request_workspace_tree_refresh(&request.plan.workspace_id);
        }
    }
    pub(crate) fn invalidate_message_deletions(&self, thread: Option<&str>) {
        let mut owner = self
            .message_deletions
            .lock()
            .expect("message deletion owner poisoned");
        let entries = owner
            .publications
            .values()
            .filter(|p| thread.is_none_or(|thread| p.thread_id == thread))
            .cloned()
            .collect::<Vec<_>>();
        for entry in entries {
            let mut next = (*entry).clone();
            next.state = MessageDeletionState::Cancelled;
            self.publish_message_deletion(&mut owner, next);
        }
    }
    pub(crate) fn message_deletion_demand_changed(
        &self,
        scope: &ClientScope,
        demand: ClientDemand,
    ) {
        let ClientScope::MessageDeletion { thread_id } = scope else {
            return;
        };
        if demand != ClientDemand::Suspended {
            self.message_deletions
                .lock()
                .expect("message deletion owner poisoned")
                .suspended
                .remove(thread_id);
            return;
        }
        self.invalidate_message_deletions(Some(thread_id));
        let mut owner = self
            .message_deletions
            .lock()
            .expect("message deletion owner poisoned");
        owner.publications.remove(thread_id);
        owner.suspended.insert(thread_id.clone());
    }
    pub(crate) fn message_deletion_subscription_changed(&self, scope: &ClientScope, added: bool) {
        let ClientScope::MessageDeletion { thread_id } = scope else {
            return;
        };
        let mut owner = self
            .message_deletions
            .lock()
            .expect("message deletion owner poisoned");
        if added {
            *owner.subscriptions.entry(thread_id.clone()).or_default() += 1;
            owner.suspended.remove(thread_id);
        } else if let Some(count) = owner.subscriptions.get_mut(thread_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                owner.subscriptions.remove(thread_id);
                drop(owner);
                self.message_deletion_demand_changed(scope, ClientDemand::Suspended);
                self.message_deletions
                    .lock()
                    .expect("message deletion owner poisoned")
                    .suspended
                    .remove(thread_id);
            }
        }
    }
    pub(crate) fn start_message_deletion_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<DeletionRequest>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-message-deletion".into())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.deletion_request_current(&request) {
                        core.complete_message_deletion(&request, Ok(()));
                        continue;
                    }
                    if core.message_deletion_target(
                        &request.plan.identity.thread_id,
                        &request.plan.turn_id,
                    ) != Some((
                        request.plan.workspace_id.clone(),
                        request.plan.expected_revision,
                    )) {
                        core.complete_message_deletion(&request, Err(true));
                        continue;
                    }
                    let sender = core.compatibility_runtime().ws_command_sender();
                    drop(core);
                    let result = sender
                        .turn_message_delete(pioneer_protocol::TurnMessageDeleteParams {
                            thread_id: request.plan.identity.thread_id.clone(),
                            turn_id: request.plan.turn_id.clone(),
                            expected_revision: request.plan.expected_revision,
                        })
                        .map(|_| ())
                        .map_err(|error| {
                            crate::transport::ws::command_sender::turn_message_error_reason(&error)
                                == Some(pioneer_protocol::TurnMessageErrorReason::RevisionConflict)
                        });
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    core.complete_message_deletion(&request, result);
                }
            })
            .expect("message deletion worker could not start");
        let mut owner = self
            .message_deletions
            .lock()
            .expect("message deletion owner poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composer::{state_machine::ComposerDomainState, store::ComposerIntent};
    use crate::core::{ClientSubscription, ClientTransitionOutcome};
    use pioneer_protocol::*;
    use std::num::NonZeroUsize;
    fn fixture() -> (Arc<ClientCore>, ClientSubscription) {
        let core = Arc::new(ClientCore::new());
        core.upsert_thread(Thread {
            workspace_id: "ws".into(),
            id: "a".into(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Chat,
            model: "model".into(),
            model_provider: "provider".into(),
            reasoning_effort: None,
            created_at: 1,
            updated_at: 2,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: vec![],
        });
        core.update_thread_presentation_identity(Some("PAAAAAAAAAAAAAAAAAAAA".into()));
        core.apply_thread_timeline_page(
            ThreadTimelinePageResponse {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                projection_version: 4,
                blocks: vec![TimelineBlock {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    block_id: "block".into(),
                    turn_id: Some("turn".into()),
                    sort_key: "1".into(),
                    started_at_unix_ms: Some(1),
                    updated_at_unix_ms: Some(2),
                    kind: TimelineBlockKind::UserMessage {
                        item_id: Some("item".into()),
                        inputs: vec![],
                        text: "original @alice".into(),
                        attachments: vec![],
                        mode: ThreadMode::Message,
                        author: Some(TurnAuthorSnapshot {
                            actor: PersistedActorRef::Principal(
                                PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
                            ),
                            display_name: "Alice".into(),
                            nickname: "alice".into(),
                            avatar_revision: None,
                            agent: None,
                        }),
                        route: None,
                        reply: None,
                        mentions: vec![],
                        revision: 3,
                        edited: false,
                        deleted: false,
                    },
                }],
                page: Default::default(),
            },
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        let lease = core.subscribe(
            ClientScope::Timeline {
                thread_id: "a".into(),
            },
            NonZeroUsize::new(16).unwrap(),
        );
        core.thread_presentation_snapshot("a").unwrap();
        core.composer_intent(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: ComposerDomainState::default(),
        });
        (core, lease)
    }

    fn begin(core: &ClientCore) -> MessageDeletionPlan {
        assert_eq!(
            core.message_deletion_intent(MessageDeletionIntent::Begin {
                thread_id: "a".into(),
                turn_id: "turn".into(),
                expected_revision: 3
            })
            .outcome(),
            ClientTransitionOutcome::Changed
        );
        core.message_deletion_snapshot("a").unwrap().plan.clone()
    }
    fn confirm(core: &ClientCore, plan: &MessageDeletionPlan) -> DeletionRequest {
        let (sender, receiver) = mpsc::sync_channel(1);
        core.message_deletions.lock().unwrap().sender = Some(sender);
        core.message_deletion_intent(MessageDeletionIntent::Confirm {
            identity: plan.identity.clone(),
        });
        receiver.try_recv().unwrap()
    }
    #[test]
    fn confirmation_is_explicit_and_only_matching_result_changes_the_operation() {
        let (core, _lease) = fixture();
        let plan = begin(&core);
        assert_eq!(
            core.message_deletion_snapshot("a").unwrap().state,
            MessageDeletionState::Confirming
        );
        assert_eq!(plan.expected_revision, 3);
        assert_eq!(plan.turn_id, "turn");
        let request = confirm(&core, &plan);
        let before = core.message_deletion_snapshot("a").unwrap();
        assert_eq!(
            core.message_deletion_intent(MessageDeletionIntent::Confirm {
                identity: plan.identity.clone()
            })
            .outcome(),
            ClientTransitionOutcome::Noop
        );
        let mut stale = request.clone();
        stale.plan.identity.generation += 1;
        core.complete_message_deletion(&stale, Ok(()));
        assert!(Arc::ptr_eq(
            &before,
            &core.message_deletion_snapshot("a").unwrap()
        ));
        core.complete_message_deletion(&request, Ok(()));
        let completed = core.message_deletion_snapshot("a").unwrap();
        assert_eq!(completed.state, MessageDeletionState::Completed);
        core.complete_message_deletion(&request, Err(false));
        assert!(Arc::ptr_eq(
            &completed,
            &core.message_deletion_snapshot("a").unwrap()
        ));
    }
    #[test]
    fn retry_uses_a_new_generation_and_conflict_waits_for_a_new_confirmation() {
        let (core, _lease) = fixture();
        let plan = begin(&core);
        let request = confirm(&core, &plan);
        core.complete_message_deletion(&request, Err(false));
        let retry = confirm(&core, &plan);
        assert_eq!(retry.plan.identity, request.plan.identity);
        assert_ne!(retry.request_generation, request.request_generation);
        core.complete_message_deletion(&request, Ok(()));
        assert_eq!(
            core.message_deletion_snapshot("a").unwrap().state,
            MessageDeletionState::Pending
        );
        core.complete_message_deletion(&retry, Err(true));
        let conflict = core.message_deletion_snapshot("a").unwrap();
        assert_eq!(
            core.message_deletion_intent(MessageDeletionIntent::Confirm {
                identity: retry.plan.identity
            })
            .outcome(),
            ClientTransitionOutcome::Noop
        );
        assert!(Arc::ptr_eq(
            &conflict,
            &core.message_deletion_snapshot("a").unwrap()
        ));
    }
    #[test]
    fn wrong_revision_author_or_thread_cannot_create_a_confirmation() {
        let (core, _lease) = fixture();
        for (thread, turn, revision) in [("wrong", "turn", 3), ("a", "wrong", 3), ("a", "turn", 2)]
        {
            assert_eq!(
                core.message_deletion_intent(MessageDeletionIntent::Begin {
                    thread_id: thread.into(),
                    turn_id: turn.into(),
                    expected_revision: revision
                })
                .outcome(),
                ClientTransitionOutcome::Rejected
            );
        }
        core.update_thread_presentation_identity(Some("PBBBBBBBBBBBBBBBBBBBB".into()));
        assert_eq!(
            core.message_deletion_intent(MessageDeletionIntent::Begin {
                thread_id: "a".into(),
                turn_id: "turn".into(),
                expected_revision: 3
            })
            .outcome(),
            ClientTransitionOutcome::Rejected
        );
    }
    #[test]
    fn cancellation_replacement_unmount_and_access_loss_reject_old_results() {
        for scenario in 0..5 {
            let (core, _lease) = fixture();
            let subscription = core.subscribe(
                ClientScope::MessageDeletion {
                    thread_id: "a".into(),
                },
                NonZeroUsize::new(8).unwrap(),
            );
            let plan = begin(&core);
            let request = confirm(&core, &plan);
            match scenario {
                0 => {
                    core.message_deletion_intent(MessageDeletionIntent::Cancel {
                        identity: plan.identity,
                    });
                }
                1 => {
                    core.message_deletion_intent(MessageDeletionIntent::Cancel {
                        identity: plan.identity,
                    });
                    begin(&core);
                }
                2 => drop(subscription),
                3 => core.clear_authorization_projections(),
                _ => core.shutdown(),
            }
            let before = core.message_deletion_snapshot("a");
            core.complete_message_deletion(&request, Ok(()));
            assert_eq!(before, core.message_deletion_snapshot("a"));
        }
    }
    #[test]
    fn unavailable_worker_is_bounded_failure_and_does_not_clear_the_composer() {
        let (core, _lease) = fixture();
        let composer = core.composer_snapshot("a").unwrap();
        let plan = begin(&core);
        core.message_deletion_intent(MessageDeletionIntent::Confirm {
            identity: plan.identity,
        });
        assert_eq!(
            core.message_deletion_snapshot("a").unwrap().state,
            MessageDeletionState::Failed { conflicted: false }
        );
        assert!(Arc::ptr_eq(
            &composer,
            &core.composer_snapshot("a").unwrap()
        ));
    }
}
