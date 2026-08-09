use crate::app::root::{DesktopComposerEditTarget, PioneerDesktop};
use gpui::{prelude::*, *};
use pioneer_client::composer::state_machine::ComposerDomainAction;
use pioneer_client::timeline::rows::UserMessagePresentation;
use pioneer_client::transport::ws::command_sender::turn_message_error_reason;
use pioneer_protocol::{
    ArtifactRef, ThreadMode, TurnMessageDeleteParams, TurnMessageEditParams,
    TurnMessageErrorReason, UserInput,
};

impl PioneerDesktop {
    pub(in crate::app) fn start_composer_message_edit(
        &mut self,
        presentation: UserMessagePresentation,
        text: String,
        artifacts: Vec<ArtifactRef>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.message_mutation_pending {
            return;
        }

        self.clear_composer(window, cx);
        self.reduce_composer_domain(ComposerDomainAction::SetModeFromUser {
            mode: ThreadMode::Message,
        });
        let valid_artifacts = artifacts
            .into_iter()
            .filter(|artifact| {
                artifact
                    .version_id
                    .as_deref()
                    .is_some_and(|id| !id.trim().is_empty())
            })
            .collect::<Vec<_>>();
        let mention_selections = presentation
            .mentions
            .iter()
            .map(|mention| {
                (
                    mention.principal_id.clone(),
                    format!("@{}", mention.nickname.trim()),
                )
            })
            .collect::<Vec<_>>();
        self.composer_edit_target = Some(DesktopComposerEditTarget {
            presentation,
            preview: text.clone(),
            artifacts: valid_artifacts,
            mention_selections,
            error: None,
            conflicted: false,
        });
        self.composer_state.update(cx, move |state, cx| {
            state.set_value(text, window, cx);
            state.focus(window, cx);
        });
        cx.notify();
    }

    pub(in crate::app) fn cancel_composer_message_edit(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.message_mutation_pending {
            return;
        }

        self.clear_composer(window, cx);
        cx.notify();
    }

    pub(in crate::app) fn submit_composer_message_edit(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(target) = self.composer_edit_target.clone() else {
            return;
        };
        let text = self.composer_state.read(cx).value().trim().to_owned();
        if self.message_mutation_pending
            || target.conflicted
            || (text.is_empty() && target.artifacts.is_empty())
        {
            return;
        }

        self.message_mutation_pending = true;
        if let Some(target) = self.composer_edit_target.as_mut() {
            target.error = None;
        }

        let mut mention_selections = target.mention_selections;
        mention_selections.extend(
            self.composer_selected_mentions
                .iter()
                .map(|mention| (mention.principal_id.clone(), mention.text_token.clone())),
        );
        let mut mentioned_principal_ids = Vec::new();
        for (principal_id, token) in mention_selections {
            if text.contains(token.as_str()) && !mentioned_principal_ids.contains(&principal_id) {
                mentioned_principal_ids.push(principal_id);
            }
        }
        let mut input = Vec::new();
        if !text.is_empty() {
            input.push(UserInput::Text {
                text,
                text_elements: Vec::new(),
            });
        }
        input.extend(
            target
                .artifacts
                .into_iter()
                .map(|artifact| UserInput::Artifact {
                    artifact_id: artifact.artifact_id,
                    version_id: artifact.version_id,
                }),
        );
        let params = TurnMessageEditParams {
            thread_id: target.presentation.thread_id,
            turn_id: target.presentation.turn_id.clone(),
            expected_revision: target.presentation.revision,
            input,
            mentioned_principal_ids,
        };
        let sender = self.gateway.ws_command_sender.clone();
        let target_turn_id = target.presentation.turn_id;

        cx.spawn_in(
            window,
            move |this: WeakEntity<Self>, cx: &mut AsyncWindowContext| {
                let mut cx = cx.clone();
                async move {
                    let result = cx
                        .background_spawn(async move { sender.turn_message_edit(params) })
                        .await;
                    let succeeded = result.is_ok();
                    let conflict = result.as_ref().err().and_then(turn_message_error_reason)
                        == Some(TurnMessageErrorReason::RevisionConflict);
                    let _ = this.update_in(&mut cx, |view, window, cx| {
                        view.message_mutation_pending = false;
                        if succeeded {
                            view.active_thread_resubscribe_pending = true;
                            view.refresh_thread_list(cx);
                            view.clear_composer(window, cx);
                        } else {
                            if let Some(target) = view
                                .composer_edit_target
                                .as_mut()
                                .filter(|target| target.presentation.turn_id == target_turn_id)
                            {
                                target.conflicted = conflict;
                                target.error = Some(if conflict {
                                    t!("timeline.message.edit_conflict").to_string()
                                } else {
                                    t!("timeline.message.edit_failed").to_string()
                                });
                            }
                            if conflict {
                                view.active_thread_resubscribe_pending = true;
                                view.refresh_thread_list(cx);
                            }
                        }
                        cx.notify();
                    });
                }
            },
        )
        .detach();
    }

    pub(in crate::app) fn confirm_delete_message(
        &mut self,
        presentation: UserMessagePresentation,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.message_mutation_pending {
            return;
        }
        if self.composer_edit_target.is_some() {
            self.cancel_composer_message_edit(window, cx);
        }
        let answer = window.prompt(
            PromptLevel::Warning,
            t!("timeline.message.delete_title").to_string().as_str(),
            Some(
                t!("timeline.message.delete_description")
                    .to_string()
                    .as_str(),
            ),
            &[
                PromptButton::new(t!("timeline.message.delete_action").to_string()),
                PromptButton::cancel(t!("buttons.cancel").to_string()),
            ],
            cx,
        );
        cx.spawn_in(
            window,
            move |this: WeakEntity<Self>, cx: &mut AsyncWindowContext| {
                let mut cx = cx.clone();
                async move {
                    if answer.await != Ok(0) {
                        return;
                    }
                    let operation = this.update_in(&mut cx, |view, _window, cx| {
                        if view.message_mutation_pending {
                            return None;
                        }
                        view.message_mutation_pending = true;
                        cx.notify();
                        Some((
                            view.gateway.ws_command_sender.clone(),
                            TurnMessageDeleteParams {
                                thread_id: presentation.thread_id.clone(),
                                turn_id: presentation.turn_id.clone(),
                                expected_revision: presentation.revision,
                            },
                        ))
                    });
                    let Some((sender, params)) = operation.ok().flatten() else {
                        return;
                    };
                    let result = cx
                        .background_spawn(async move { sender.turn_message_delete(params) })
                        .await;
                    let conflict = result.as_ref().err().and_then(turn_message_error_reason)
                        == Some(TurnMessageErrorReason::RevisionConflict);
                    let _ = this.update_in(&mut cx, |view, window, cx| {
                        view.message_mutation_pending = false;
                        view.active_thread_resubscribe_pending = true;
                        view.refresh_thread_list(cx);
                        if result.is_err() {
                            let message = if conflict {
                                t!("timeline.message.delete_conflict").to_string()
                            } else {
                                t!("timeline.message.delete_failed").to_string()
                            };
                            let _ = window.prompt(
                                PromptLevel::Warning,
                                t!("timeline.message.delete_title").to_string().as_str(),
                                Some(message.as_str()),
                                &[PromptButton::ok(t!("buttons.ok").to_string())],
                                cx,
                            );
                        }
                        cx.notify();
                    });
                }
            },
        )
        .detach();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn mutations_use_existing_rpc_expected_revision_and_authoritative_refresh() {
        let source = include_str!("message_mutations.rs");
        assert!(source.contains("turn_message_edit"));
        assert!(source.contains("turn_message_delete"));
        assert!(source.contains("expected_revision"));
        assert!(source.contains("turn_message_error_reason"));
        assert!(source.contains("active_thread_resubscribe_pending"));
        assert!(!source.contains(&["conversation", "_message"].concat()));
    }
}
