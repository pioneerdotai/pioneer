//! Message edits consume one immutable draft plan. Shells only present the result.

use super::store::{
    ComposerIntent, ComposerOperationCompletion, ComposerOperationIdentity, ComposerOperationKind,
    ComposerOperationPlan, ComposerOperationStatus, DraftId,
};
use crate::core::{ClientCore, ClientScope, ClientTransition, ClientTransitionOutcome};
use crate::timeline::{
    presentation::{TimelineRenderRow, TimelineSnapshot},
    rows::{TimelineRowKind, UserMessagePresentation},
};
use pioneer_protocol::{
    ArtifactRef, TurnItem, TurnMessageEditParams, TurnMessageErrorReason, UserInput,
};
use std::sync::{Arc, mpsc};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerMessageEditTarget {
    pub presentation: UserMessagePresentation,
    pub preview: String,
    pub artifacts: Vec<ArtifactRef>,
    pub failed: bool,
    pub conflicted: bool,
}

struct EditRequest {
    identity: ComposerOperationIdentity,
    params: TurnMessageEditParams,
    workspace_id: String,
}

#[derive(Default)]
pub(crate) struct ComposerMessageEditController {
    sender: Option<mpsc::SyncSender<EditRequest>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl ComposerMessageEditController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
    }
}
impl Drop for ComposerMessageEditController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}

fn edit_params(plan: &ComposerOperationPlan) -> Option<TurnMessageEditParams> {
    let target = plan.message_edit.as_ref()?;
    if plan.kind != ComposerOperationKind::EditMessage
        || target.conflicted
        || target.presentation.thread_id != plan.identity.thread_id
    {
        return None;
    }
    let text = plan.draft.text.trim();
    let mut input = Vec::new();
    if !text.is_empty() {
        input.push(UserInput::Text {
            text: text.to_owned(),
            text_elements: Vec::new(),
        });
    }
    input.extend(target.artifacts.iter().map(|artifact| UserInput::Artifact {
        artifact_id: artifact.artifact_id.clone(),
        version_id: artifact.version_id.clone(),
    }));
    if input.is_empty() {
        return None;
    }
    let mut mentioned_principal_ids = Vec::new();
    let mentions = target
        .presentation
        .mentions
        .iter()
        .map(|mention| {
            (
                mention.principal_id.clone(),
                format!("@{}", mention.nickname.trim()),
            )
        })
        .chain(
            plan.draft
                .domain
                .selected_mentions
                .iter()
                .map(|mention| (mention.principal_id.clone(), mention.text_token.clone())),
        );
    for (principal, token) in mentions {
        if !token.trim().is_empty()
            && text.contains(&token)
            && !mentioned_principal_ids.contains(&principal)
        {
            mentioned_principal_ids.push(principal);
        }
    }
    Some(TurnMessageEditParams {
        thread_id: plan.identity.thread_id.clone(),
        turn_id: target.presentation.turn_id.clone(),
        expected_revision: target.presentation.revision,
        input,
        mentioned_principal_ids,
    })
}

impl ClientCore {
    pub(super) fn composer_message_edit_target(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Option<ComposerMessageEditTarget> {
        let source = self.thread_snapshot(thread_id)?;
        let principal = pioneer_protocol::PrincipalId::new(source.current_principal_id()?).ok()?;
        let publication = self
            .snapshot(&ClientScope::Timeline {
                thread_id: thread_id.into(),
            })?
            .typed::<TimelineSnapshot>()?;
        publication.payload().rows().iter().find_map(|row| {
            let TimelineRenderRow::Timeline(row_value) = row.value() else {
                return None;
            };
            let TimelineRowKind::UserMessage { presentation, .. } = &row_value.kind else {
                return None;
            };
            if presentation.thread_id != thread_id
                || presentation.turn_id != turn_id
                || !crate::timeline::rows::user_message_mutation_availability(
                    presentation,
                    &principal,
                )
                .can_edit
            {
                return None;
            }
            let TurnItem::UserMessage {
                text, attachments, ..
            } = &row.item()?.item
            else {
                return None;
            };
            let artifacts = crate::timeline::labels::parse_user_attachments(attachments)
                .into_iter()
                .filter_map(|attachment| attachment.artifact)
                .filter(|artifact| {
                    artifact
                        .version_id
                        .as_deref()
                        .is_some_and(|id| !id.trim().is_empty())
                })
                .collect();
            Some(ComposerMessageEditTarget {
                presentation: presentation.clone(),
                preview: text.clone(),
                artifacts,
                failed: false,
                conflicted: false,
            })
        })
    }

    pub(super) fn submit_composer_message_edit(
        &self,
        thread_id: String,
        draft_id: DraftId,
    ) -> ClientTransition {
        let transition = self.composer_intent(ComposerIntent::BeginOperation {
            thread_id: thread_id.clone(),
            draft_id,
            operation: ComposerOperationKind::EditMessage,
        });
        if transition.outcome() != ClientTransitionOutcome::Changed {
            return transition;
        }
        let Some(plan) = transition
            .changes()
            .publications()
            .iter()
            .filter_map(|publication| publication.typed::<super::store::ComposerPublication>())
            .find(|publication| publication.payload().thread_id() == thread_id)
            .and_then(|publication| {
                publication
                    .payload()
                    .operation()
                    .and_then(|operation| operation.plan.clone())
            })
        else {
            return transition;
        };
        if plan.identity.draft_id != draft_id || plan.kind != ComposerOperationKind::EditMessage {
            return transition;
        }
        let claimed = self.composer_intent(ComposerIntent::PrepareOperation {
            identity: plan.identity.clone(),
        });
        if claimed.outcome() != ClientTransitionOutcome::Changed {
            return claimed;
        }
        let request = edit_params(&plan).map(|params| EditRequest {
            identity: plan.identity.clone(),
            params,
            workspace_id: plan
                .message_edit
                .as_ref()
                .unwrap()
                .presentation
                .workspace_id
                .clone(),
        });
        let queued = request.is_some_and(|request| {
            self.composer_edits
                .lock()
                .expect("composer edit controller poisoned")
                .sender
                .as_ref()
                .is_some_and(|sender| sender.try_send(request).is_ok())
        });
        if !queued {
            return self.composer_intent(ComposerIntent::CompleteOperation {
                identity: plan.identity,
                completion: ComposerOperationCompletion::MessageEditFailed { conflicted: false },
            });
        }
        claimed
    }

    fn composer_edit_is_current(&self, identity: &ComposerOperationIdentity) -> bool {
        !self.is_stopped()
            && self
                .composer_snapshot(&identity.thread_id)
                .is_some_and(|p| {
                    p.draft_id() == identity.draft_id
                        && p.operation().is_some_and(|op| {
                            op.identity == *identity
                                && op.kind == ComposerOperationKind::EditMessage
                                && op.status == ComposerOperationStatus::Preparing
                        })
                })
    }

    fn complete_composer_message_edit(&self, request: EditRequest, result: Result<(), bool>) {
        let conflict = result == Err(true);
        let succeeded = result.is_ok();
        let completion = match result {
            Ok(()) => ComposerOperationCompletion::Sent,
            Err(conflicted) => ComposerOperationCompletion::MessageEditFailed { conflicted },
        };
        if self.complete_composer_operation(request.identity.clone(), completion)
            && (succeeded || conflict)
        {
            self.refresh_thread_timeline(&request.identity.thread_id);
            self.request_workspace_tree_refresh(&request.workspace_id);
        }
    }

    pub(crate) fn start_composer_message_edit_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<EditRequest>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-composer-edit".into())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.composer_edit_is_current(&request.identity) {
                        continue;
                    }
                    let sender = core.transport_runtime().ws_command_sender();
                    drop(core);
                    let result = sender
                        .turn_message_edit(request.params.clone())
                        .map(|_| ())
                        .map_err(|error| {
                            crate::transport::ws::command_sender::turn_message_error_reason(&error)
                                == Some(TurnMessageErrorReason::RevisionConflict)
                        });
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    core.complete_composer_message_edit(request, result);
                }
            })
            .expect("composer edit worker could not start");
        let mut owner = self
            .composer_edits
            .lock()
            .expect("composer edit controller poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composer::state_machine::ComposerDomainState;
    use crate::core::{ClientIntent, ClientSubscription};
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
    fn start(core: &ClientCore) -> DraftId {
        let old = core.composer_snapshot("a").unwrap();
        assert_eq!(
            core.dispatch(ClientIntent::Composer {
                intent: ComposerIntent::StartMessageEdit {
                    thread_id: "a".into(),
                    draft_id: old.draft_id(),
                    turn_id: "turn".into()
                }
            })
            .outcome(),
            ClientTransitionOutcome::Changed
        );
        core.composer_snapshot("a").unwrap().draft_id()
    }
    fn submit(core: &ClientCore) -> EditRequest {
        let (sender, receiver) = mpsc::sync_channel(1);
        core.composer_edits.lock().unwrap().sender = Some(sender);
        let draft = core.composer_snapshot("a").unwrap();
        assert_eq!(
            core.composer_intent(ComposerIntent::SubmitMessageEdit {
                thread_id: "a".into(),
                draft_id: draft.draft_id()
            })
            .outcome(),
            ClientTransitionOutcome::Changed
        );
        receiver.try_recv().unwrap()
    }

    #[test]
    fn message_edit_uses_authoritative_row_and_matching_completion_clears_only_its_draft() {
        let (core, _lease) = fixture();
        let draft = start(&core);
        let input = core.composer_snapshot("a").unwrap();
        assert_eq!(input.draft().text, "original @alice");
        assert_eq!(input.domain().selected_mode, ThreadMode::Message);
        assert_eq!(input.message_edit().unwrap().presentation.revision, 3);
        let request = submit(&core);
        assert_eq!(request.params.expected_revision, 3);
        assert_eq!(request.params.turn_id, "turn");
        assert_eq!(
            request.params.input,
            vec![UserInput::Text {
                text: "original @alice".into(),
                text_elements: vec![]
            }]
        );
        let identity = request.identity.clone();
        core.complete_composer_message_edit(request, Ok(()));
        let result = core.composer_snapshot("a").unwrap();
        assert!(result.message_edit().is_none());
        assert!(result.draft().text.is_empty());
        assert_ne!(result.draft_id(), draft);
        assert!(!core.complete_composer_operation(identity, ComposerOperationCompletion::Sent));
        assert!(Arc::ptr_eq(&result, &core.composer_snapshot("a").unwrap()));
    }

    #[test]
    fn late_edit_success_does_not_clear_replacement_or_changed_text() {
        for replace in [false, true] {
            let (core, _lease) = fixture();
            let draft_id = start(&core);
            let request = submit(&core);
            if replace {
                core.composer_intent(ComposerIntent::Clear {
                    thread_id: "a".into(),
                    draft_id,
                });
            }
            let input = core.composer_snapshot("a").unwrap();
            core.composer_intent(ComposerIntent::EditText {
                thread_id: "a".into(),
                draft_id: input.draft_id(),
                text: "new unsent message".into(),
            });
            let before = core.composer_snapshot("a").unwrap();
            core.complete_composer_message_edit(request, Ok(()));
            assert!(Arc::ptr_eq(&before, &core.composer_snapshot("a").unwrap()));
            assert_eq!(before.draft().text, "new unsent message");
        }
    }

    #[test]
    fn controlled_equal_edit_is_noop_and_duplicate_submit_cannot_queue_twice() {
        let (core, _lease) = fixture();
        let draft_id = start(&core);
        let request = submit(&core);
        let before = core.composer_snapshot("a").unwrap();
        for intent in [
            ComposerIntent::EditText {
                thread_id: "a".into(),
                draft_id,
                text: before.draft().text.clone(),
            },
            ComposerIntent::SubmitMessageEdit {
                thread_id: "a".into(),
                draft_id,
            },
        ] {
            assert_eq!(
                core.composer_intent(intent).outcome(),
                ClientTransitionOutcome::Noop
            );
        }
        assert!(Arc::ptr_eq(&before, &core.composer_snapshot("a").unwrap()));
        core.complete_composer_message_edit(request, Ok(()));
        assert!(
            core.composer_snapshot("a")
                .unwrap()
                .message_edit()
                .is_none()
        );
    }

    #[test]
    fn conflict_preserves_text_and_target_and_blocks_resubmission_until_reopened() {
        let (core, _lease) = fixture();
        let draft_id = start(&core);
        let request = submit(&core);
        core.complete_composer_message_edit(request, Err(true));
        let failed = core.composer_snapshot("a").unwrap();
        assert_eq!(failed.draft().text, "original @alice");
        assert!(failed.message_edit().unwrap().conflicted);
        assert!(failed.message_edit().unwrap().failed);
        assert_eq!(
            core.composer_intent(ComposerIntent::SubmitMessageEdit {
                thread_id: "a".into(),
                draft_id
            })
            .outcome(),
            ClientTransitionOutcome::Noop
        );
        start(&core);
        assert!(
            !core
                .composer_snapshot("a")
                .unwrap()
                .message_edit()
                .unwrap()
                .conflicted
        );
    }

    #[test]
    fn queue_failure_preserves_draft_and_explicit_retry_can_succeed() {
        let (core, _lease) = fixture();
        let draft_id = start(&core);
        core.composer_intent(ComposerIntent::SubmitMessageEdit {
            thread_id: "a".into(),
            draft_id,
        });
        let failed = core.composer_snapshot("a").unwrap();
        assert!(failed.message_edit().unwrap().failed);
        assert_eq!(failed.draft_id(), draft_id);
        assert_eq!(failed.draft().text, "original @alice");
        let request = submit(&core);
        assert!(
            !core
                .composer_snapshot("a")
                .unwrap()
                .message_edit()
                .unwrap()
                .failed
        );
        core.complete_composer_message_edit(request, Ok(()));
    }

    #[test]
    fn cancellation_and_wrong_thread_completion_do_not_change_edit_target() {
        let (core, _lease) = fixture();
        start(&core);
        let mut request = submit(&core);
        let before = core.composer_snapshot("a").unwrap();
        let correct = request.identity.clone();
        request.identity.thread_id = "b".into();
        core.complete_composer_message_edit(request, Ok(()));
        assert!(Arc::ptr_eq(&before, &core.composer_snapshot("a").unwrap()));
        core.cancel_composer_requests_for_thread("a");
        let cancelled = core.composer_snapshot("a").unwrap();
        assert!(!core.complete_composer_operation(correct, ComposerOperationCompletion::Sent));
        assert!(Arc::ptr_eq(
            &cancelled,
            &core.composer_snapshot("a").unwrap()
        ));
        assert!(cancelled.message_edit().is_some());
    }

    #[test]
    fn unavailable_or_other_author_row_cannot_replace_draft() {
        let (core, _lease) = fixture();
        let before = core.composer_snapshot("a").unwrap();
        core.update_thread_presentation_identity(Some("PBBBBBBBBBBBBBBBBBBBB".into()));
        for turn in ["turn", "absent"] {
            core.composer_intent(ComposerIntent::StartMessageEdit {
                thread_id: "a".into(),
                draft_id: before.draft_id(),
                turn_id: turn.into(),
            });
            assert!(Arc::ptr_eq(&before, &core.composer_snapshot("a").unwrap()));
        }
    }
    #[test]
    fn last_composer_binding_drop_cancels_edit_but_leaves_other_consumers_live() {
        let (core, _timeline) = fixture();
        let scope = ClientScope::Composer {
            thread_id: "a".into(),
        };
        let first = core.subscribe(scope.clone(), NonZeroUsize::new(8).unwrap());
        let second = core.subscribe(scope.clone(), NonZeroUsize::new(8).unwrap());
        start(&core);
        let request = submit(&core);
        drop(first);
        assert!(core.composer_edit_is_current(&request.identity));
        drop(second);
        let cancelled = core.composer_snapshot("a").unwrap();
        assert_eq!(
            cancelled.operation().unwrap().status,
            ComposerOperationStatus::Cancelled
        );
        core.complete_composer_message_edit(request, Ok(()));
        assert!(Arc::ptr_eq(
            &cancelled,
            &core.composer_snapshot("a").unwrap()
        ));
        let _replacement = core.subscribe(scope, NonZeroUsize::new(8).unwrap());
        let retry = submit(&core);
        assert!(core.composer_edit_is_current(&retry.identity));
    }

    #[test]
    fn composer_suspension_rejects_late_edit_and_new_operations_until_visible() {
        use crate::core::{ClientDemand, ClientGeneration};
        let (core, _timeline) = fixture();
        start(&core);
        let request = submit(&core);
        let scope = ClientScope::Composer {
            thread_id: "a".into(),
        };
        core.dispatch(ClientIntent::SetScopeDemand {
            scope: scope.clone(),
            demand: ClientDemand::Suspended,
            generation: ClientGeneration::new(1),
        });
        let cancelled = core.composer_snapshot("a").unwrap();
        core.complete_composer_message_edit(request, Err(true));
        assert!(Arc::ptr_eq(
            &cancelled,
            &core.composer_snapshot("a").unwrap()
        ));
        assert_eq!(
            core.composer_intent(ComposerIntent::BeginOperation {
                thread_id: "a".into(),
                draft_id: cancelled.draft_id(),
                operation: ComposerOperationKind::EditMessage
            })
            .outcome(),
            ClientTransitionOutcome::Rejected
        );
        core.dispatch(ClientIntent::SetScopeDemand {
            scope,
            demand: ClientDemand::Visible,
            generation: ClientGeneration::new(2),
        });
        let retry = submit(&core);
        assert!(core.composer_edit_is_current(&retry.identity));
    }
}
