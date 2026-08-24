use super::super::root::{ComposerCapability, PioneerDesktop};
use crate::gateway::DesktopGatewayWsCommandSenderExt;
use gpui::{prelude::*, *};
use pioneer_client::cli_runtime::approvals::{
    PendingRequest, PendingRequestResolution, PendingRequestResponseAction,
    PendingRequestsReduction, plan_pending_request_response,
};
use pioneer_client::composer::attachments as composer_attachments;
use pioneer_client::composer::state_machine::{
    ComposerDomainAction, bound_composer_mentioned_principal_ids,
};
use pioneer_client::composer::turn_prepare::{
    self as composer_turn_prepare, PrepareComposerTurnRequest,
};
use pioneer_client::providers::list as provider_list;
use pioneer_client::threads::start as thread_start;
use pioneer_client::turns::{cancel as turn_cancel, start as turn_start, steer as turn_steer};
use pioneer_protocol::{ArtifactRef, CLIRuntimeRequestResolvedNotification};
use std::{path::PathBuf, time::Duration};
use tracing::warn;

const COMPOSER_SESSION_READY_RETRY_DELAY: Duration = Duration::from_millis(50);

impl PioneerDesktop {
    pub(super) fn steer_active_cli_runtime_turn(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.can_steer_active_thread_agent_presentation() {
            return;
        }
        let snapshot = self.client_snapshot().active_thread;
        let (Some(thread_id), Some(workspace_id), Some(turn_id)) = (
            snapshot.thread_id.clone(),
            snapshot.workspace_id.clone(),
            snapshot.in_flight_turn_id.clone(),
        ) else {
            return;
        };
        let Some(binding) = self.cli_runtime_binding_for_thread(thread_id.as_str()) else {
            return;
        };
        let message = self.composer_state.read(cx).value().trim().to_owned();
        let Some(params) = turn_steer::plan_cli_runtime_turn_steer(
            workspace_id,
            binding.runtime_id.clone(),
            thread_id.clone(),
            turn_id.clone(),
            message,
        ) else {
            return;
        };

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn_in(
            window,
            move |this: WeakEntity<Self>, cx: &mut AsyncWindowContext| {
                let mut cx = cx.clone();
                async move {
                    let result = cx
                        .background_spawn(async move { ws_sender.cli_runtime_turn_steer(params) })
                        .await;

                    let _ = this.update_in(&mut cx, move |view, window, cx| {
                        match result {
                            Ok(_) => {
                                view.clear_composer(window, cx);
                            }
                            Err(error) => {
                                warn!(
                                    thread_id = thread_id.as_str(),
                                    turn_id = turn_id.as_str(),
                                    error = %format!("{error:#}"),
                                    "failed to steer CLI runtime turn"
                                );
                            }
                        }
                        cx.notify();
                    });
                }
            },
        )
        .detach();
    }

    pub(super) fn respond_pending_request(
        &mut self,
        request: PendingRequest,
        resolution: PendingRequestResolution,
        cx: &mut Context<Self>,
    ) {
        let visible_in_active_scope = self
            .active_thread_pending_requests()
            .iter()
            .any(|pending| pending.request_id == request.request_id);
        if !visible_in_active_scope
            || !self.can_respond_to_agent_requests_presentation(self.current_active_thread_id())
        {
            return;
        }
        let action = match plan_pending_request_response(&request, resolution) {
            Ok(action) => action,
            Err(error) => {
                warn!(
                    request_id = request.request_id.as_str(),
                    error = ?error,
                    "failed to plan pending request response"
                );
                return;
            }
        };
        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                match action {
                    PendingRequestResponseAction::CLIRuntime { params, .. } => {
                        let request_id = params.request_id.clone();
                        let runtime_id = params.runtime_id.clone();
                        let result = cx
                            .background_spawn(async move {
                                ws_sender.cli_runtime_request_respond(params)
                            })
                            .await;

                        let _ = this.update(&mut cx, |view, cx| match result {
                            Ok(response) => {
                                let reduction =
                                    pioneer_client::cli_runtime::approvals::reduce_cli_runtime_request_resolved_notification(
                                        CLIRuntimeRequestResolvedNotification {
                                            workspace_id: response.workspace_id,
                                            runtime_id: response.runtime_id,
                                            request_id: response.request_id,
                                            thread_id: response.thread_id,
                                            turn_id: response.turn_id,
                                            item_id: response.item_id,
                                            resolution: response.resolution,
                                        },
                                    );
                                view.pending_requests.apply(reduction);
                                cx.notify();
                            }
                            Err(error) => {
                                warn!(
                                    request_id = request_id.as_str(),
                                    runtime_id = runtime_id.as_str(),
                                    error = %format!("{error:#}"),
                                    "failed to respond to CLI runtime pending request"
                                );
                            }
                        });
                    }
                    PendingRequestResponseAction::NativePermissionGate { params, .. } => {
                        let request_id = params.request_id.clone();
                        let result = cx
                            .background_spawn(async move {
                                ws_sender.turn_permission_request_respond(params)
                            })
                            .await;

                        let _ = this.update(&mut cx, |view, cx| match result {
                            Ok(response) => {
                                view.pending_requests.apply(
                                    PendingRequestsReduction::Resolved {
                                        request_id: response.request_id,
                                    },
                                );
                                cx.notify();
                            }
                            Err(error) => {
                                warn!(
                                    request_id = request_id.as_str(),
                                    error = %format!("{error:#}"),
                                    "failed to respond to native pending request"
                                );
                            }
                        });
                    }
                }
            }
        })
        .detach();
    }

    pub(super) fn open_composer_file_picker(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.active_artifact_presentation_policy().can_attach {
            return;
        }
        let Some(target_thread_id) = self.current_active_thread_id().map(str::to_owned) else {
            return;
        };
        let selection = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: true,
            prompt: None,
        });

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let selection = match selection.await {
                    Ok(selection) => selection,
                    Err(_) => return,
                };

                let paths = match selection {
                    Ok(paths) => paths,
                    Err(_) => return,
                };

                let Some(paths) = paths else {
                    return;
                };

                let _ = this.update(&mut cx, |view, cx| {
                    if view.current_active_thread_id() != Some(target_thread_id.as_str()) {
                        return;
                    }
                    view.append_composer_attachment_paths(paths);
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn remove_composer_attachment_at(&mut self, index: usize) {
        if self.desktop_voice_context_locked() {
            return;
        }
        if self
            .reduce_composer_domain(ComposerDomainAction::RemoveAttachmentAt { index })
            .changed
        {
            self.composer_upload_error = None;
        }
    }

    pub(super) fn remove_composer_capability_at(&mut self, index: usize) {
        if self.desktop_voice_context_locked() {
            return;
        }
        self.reduce_composer_domain(ComposerDomainAction::RemoveCapabilityAt { index });
    }

    pub(super) fn remove_composer_skill_selection(
        &mut self,
        selection: pioneer_client::composer::skill_selection::ComposerSkillSelection,
    ) {
        if self.desktop_voice_context_locked() {
            return;
        }
        let selections = self
            .composer_skill_selections
            .iter()
            .filter(|existing| *existing != &selection)
            .cloned()
            .collect();
        self.reduce_composer_domain(ComposerDomainAction::SetSkillSelections { selections });
    }

    pub(super) fn add_composer_capabilities(
        &mut self,
        capabilities: impl IntoIterator<Item = ComposerCapability>,
    ) {
        if self.desktop_voice_context_locked() {
            return;
        }
        for capability in capabilities {
            self.reduce_composer_domain(ComposerDomainAction::AddCapability { capability });
        }
    }

    pub(super) fn submit_composer_message(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.reconcile_composer_draft_with_capabilities();
        if self
            .gateway
            .capability_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.workspace.as_ref())
            .is_none()
        {
            cx.notify();
            return;
        }
        if !self.can_submit_message(cx) {
            return;
        }
        let Some(authorization_fingerprint) = self.composer_authorization_fingerprint.clone()
        else {
            return;
        };

        let composer_state = self.composer_state.clone();
        let selected_mode = self.composer_turn_mode;
        let selected_permission_mode = self.composer_permission_mode;
        let selected_model = self.composer_selected_model.clone();
        let selected_provider = self.composer_selected_provider.clone();
        let selected_reasoning_effort = self.composer_selected_reasoning_effort.clone();
        let selected_cli_runtime_backend = if selected_mode == pioneer_protocol::ThreadMode::Message
        {
            None
        } else {
            match provider_list::resolve_cli_runtime_execution_backend(
                selected_provider.as_deref(),
                self.providers.cli_runtimes(),
            ) {
                Ok(backend) => backend,
                Err(error) => {
                    self.composer_upload_error = Some(error);
                    cx.notify();
                    return;
                }
            }
        };
        let turn_model_provider = if selected_cli_runtime_backend.is_some() {
            None
        } else {
            selected_provider.clone()
        };
        let Some(thread_id) = self.active_thread_id.clone() else {
            return;
        };
        let composer_execution_mode = self
            .thread_coordinator(thread_id.as_str())
            .and_then(|coordinator| coordinator.thread())
            .map(|thread| thread.origin_kind.composer_execution_mode())
            .unwrap_or(pioneer_protocol::ThreadComposerExecutionMode::ForegroundTurn);

        let composer_text = composer_state.read(cx).value().trim().to_owned();
        let composer_domain_state = self.composer_domain_state();
        let reply_to_turn_id = composer_domain_state
            .reply_target
            .as_ref()
            .map(|target| target.turn_id.clone());
        let mentioned_principal_ids =
            bound_composer_mentioned_principal_ids(&composer_domain_state, composer_text.as_str());
        let composer_attachments = self.composer_attachments.clone();
        let submission =
            self.composer_submission_plan(composer_text.as_str(), !composer_attachments.is_empty());
        if !submission.has_composer_payload && self.composer_skill_selections.is_empty() {
            return;
        }
        let composer_capabilities = submission.capabilities;
        let composer_skill_selections = self.composer_skill_selections.clone();
        let composer_skill_picker = self.composer_skill_picker_projection("");
        self.reduce_composer_domain(ComposerDomainAction::SetCapabilities {
            capabilities: composer_capabilities.clone(),
        });
        let turn_start_ids = turn_start::plan_turn_start_ids();
        let turn_id = turn_start_ids.turn_id;
        let pending_request_id = turn_start_ids.pending_request_id;
        let workspace_id = self
            .thread_workspace_id(thread_id.as_str())
            .map(str::to_owned)
            .unwrap_or_else(|| self.default_thread_start_scope());
        let endpoint_kind = self
            .gateway
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.active_gateway().map(|gateway| gateway.kind));
        let upload_sender = self.gateway.ws_command_sender.clone();
        let turn_start_sender = self.gateway.ws_command_sender.clone();

        self.composer_upload_in_progress = true;
        self.composer_upload_error = None;
        self.reduce_composer_domain(ComposerDomainAction::MarkAttachmentsUploading);
        cx.notify();

        cx.spawn_in(
            window,
            move |this: WeakEntity<Self>, cx: &mut AsyncWindowContext| {
                let mut cx = cx.clone();
                let thread_id_for_prepare = thread_id.clone();
                let turn_id_for_prepare = turn_id.clone();
                let composer_text_for_prepare = composer_text.clone();
                let composer_attachments_for_prepare = composer_attachments.clone();
                let composer_capabilities_for_prepare = composer_capabilities.clone();
                let composer_skill_selections_for_prepare = composer_skill_selections.clone();
                let composer_skill_picker_for_prepare = composer_skill_picker.clone();
                let workspace_id_for_prepare = workspace_id.clone();

                async move {
                    while this
                        .update_in(&mut cx, |view, _, _| {
                            view.gateway.session_refresh_in_flight
                        })
                        .unwrap_or(false)
                    {
                        cx.background_executor().timer(COMPOSER_SESSION_READY_RETRY_DELAY).await;
                    }
                    let prepare_result = cx
                        .background_spawn(async move {
                            upload_sender.prepare_composer_turn(PrepareComposerTurnRequest {
                                workspace_id: workspace_id_for_prepare,
                                thread_id: thread_id_for_prepare,
                                turn_id: turn_id_for_prepare,
                                endpoint_kind,
                                text: composer_text_for_prepare,
                                attachments: composer_attachments_for_prepare,
                                capabilities: composer_capabilities_for_prepare,
                                skill_selections: composer_skill_selections_for_prepare,
                                skill_picker: composer_skill_picker_for_prepare,
                            })
                        })
                        .await;

                    let _ = this.update_in(&mut cx, move |view, window, cx| {
                        if view.composer_authorization_fingerprint.as_deref()
                            != Some(authorization_fingerprint.as_str())
                        {
                            view.composer_upload_in_progress = false;
                            view.reconcile_composer_draft_with_capabilities();
                            view.composer_upload_error = Some(
                                "Composer policy changed before submission; review the updated selections"
                                    .to_owned(),
                            );
                            cx.notify();
                            return;
                        }
                        let prepared = match prepare_result {
                            Ok(prepared) => prepared,
                            Err(_error) => {
                                let reduction =
                                    composer_turn_prepare::reduce_prepare_composer_turn_failure(
                                        t!("chat.composer.send_failed").to_string(),
                                    );
                                view.composer_upload_in_progress =
                                    reduction.composer_upload_in_progress;
                                view.composer_upload_error =
                                    Some(reduction.composer_upload_error.clone());
                                view.reduce_composer_domain(
                                    ComposerDomainAction::MarkAttachmentsFailed {
                                        error: reduction
                                            .mark_uploading_attachments_failed_error
                                            .clone(),
                                    },
                                );
                                cx.notify();
                                return;
                            }
                        };

                        let reduction =
                            composer_turn_prepare::reduce_prepared_composer_turn_submit_success(
                                composer_turn_prepare::PreparedComposerTurnSubmitContext {
                                    thread_id: thread_id.clone(),
                                    turn_id: turn_id.clone(),
                                    pending_request_id: pending_request_id.clone(),
                                    composer_execution_mode,
                                    selected_model: selected_model.clone(),
                                    selected_provider: selected_provider.clone(),
                                    turn_model_provider: turn_model_provider.clone(),
                                    selected_mode: Some(selected_mode),
                                    reply_to_turn_id: reply_to_turn_id.clone(),
                                    mentioned_principal_ids: mentioned_principal_ids.clone(),
                                    permission_mode: selected_permission_mode,
                                    execution_backend: selected_cli_runtime_backend.clone(),
                                    selected_reasoning_effort: selected_reasoning_effort.clone(),
                                    cli_runtime_options: None,
                                    updated_at_unix: turn_start::now_unix_seconds(),
                                },
                                prepared,
                            );

                        // Keep the existing Composer loading state through the
                        // authoritative thread/start + turn/start handoff. This
                        // also prevents a token rotation from replacing the
                        // connection between those two requests.
                        view.composer_upload_in_progress = true;
                        if reduction.clear_composer_upload_error {
                            view.composer_upload_error = None;
                        }
                        let clear_composer_on_accept = reduction.clear_composer;
                        let clear_thread_draft_id = reduction.clear_thread_draft_id.clone();
                        view.reduce_composer_domain(
                            ComposerDomainAction::ApplyUploadedAttachments {
                                artifacts: reduction.uploaded_attachment_artifacts,
                            },
                        );

                        if let Some(coordinator) = view.thread_coordinator_mut(thread_id.as_str()) {
                            if let Some(thread) = coordinator.thread_mut() {
                                turn_start::apply_prepared_turn_to_thread_snapshot(
                                    thread,
                                    selected_mode,
                                    reduction.thread_snapshot_update.selected_model.as_deref(),
                                    reduction
                                        .thread_snapshot_update
                                        .selected_provider
                                        .as_deref(),
                                    reduction
                                        .thread_snapshot_update
                                        .selected_reasoning_effort
                                        .as_deref(),
                                    reduction.thread_snapshot_update.user_text.as_str(),
                                    reduction.thread_snapshot_update.updated_at_unix,
                                );
                            }
                        }

                        let promoted_from_draft = view.promote_thread_from_draft(
                            reduction.promote_thread_from_draft_id.as_str(),
                        );
                        if promoted_from_draft {
                            view.rebuild_sidebar_tree_state(cx);
                            let _ = view.drive_thread_start_queue(cx);
                        }

                        let Some(conversation) = view.thread_conversation_mut(thread_id.as_str())
                        else {
                            view.composer_upload_in_progress = false;
                            cx.notify();
                            return;
                        };
                        conversation.apply(reduction.local_turn_start_requested_event.clone());
                        if pioneer_client::timeline::semantic::apply_local_composer_event_to_semantic_timeline(
                            &mut view.semantic_timelines,
                            workspace_id.as_str(),
                            &reduction.local_turn_start_requested_event,
                            reduction.composer_execution_mode,
                            pioneer_client::timeline::labels::now_unix_ms(),
                        ) {
                            view.semantic_timeline_revision =
                                view.semantic_timeline_revision.saturating_add(1);
                        }

                        let ws_sender = turn_start_sender.clone();
                        let turn_start_params_plan = reduction.turn_start_params_plan;
                        let send_context = reduction.send_context;
                        let workspace_id_for_update = workspace_id.clone();

                        cx.spawn_in(
                            window,
                            move |this: WeakEntity<Self>, cx: &mut AsyncWindowContext| {
                            let mut cx = cx.clone();
                            let thread_id_for_update = send_context.thread_id.clone();
                            let send_context_for_update = send_context.clone();
                            let workspace_id_for_preflight = workspace_id_for_update.clone();
                            async move {
                                let result = cx
                                    .background_spawn(async move {
                                        ws_sender.thread_start(thread_start::thread_start_params(
                                            send_context.thread_id.clone(),
                                            workspace_id_for_preflight,
                                        ))?;
                                        ws_sender.turn_start(
                                            turn_start::turn_start_params_from_plan(
                                                turn_start_params_plan,
                                            ),
                                        )
                                    })
                                    .await;

                                let _ = this.update_in(&mut cx, |view, window, cx| {
                                    view.composer_upload_in_progress = false;
                                    let reduction = match result {
                                        Ok(response) => turn_start::reduce_turn_start_send_success(
                                            send_context_for_update,
                                            response,
                                        ),
                                        Err(_error) => turn_start::reduce_turn_start_send_failure(
                                            send_context_for_update,
                                            t!("chat.composer.send_failed").to_string(),
                                        ),
                                    };
                                    let (events, accepted) = match reduction {
                                        turn_start::TurnStartSendReduction::Accepted { events } => {
                                            (events, true)
                                        }
                                        turn_start::TurnStartSendReduction::Rejected { event } => {
                                            (vec![event], false)
                                        }
                                    };
                                    if accepted {
                                        if clear_composer_on_accept
                                            && view.current_active_thread_id()
                                                == Some(thread_id_for_update.as_str())
                                        {
                                            view.composer_state.update(cx, |state, cx| {
                                                state.set_value("", window, cx)
                                            });
                                            view.reduce_composer_domain(
                                                ComposerDomainAction::SendSucceeded,
                                            );
                                        }
                                        view.clear_thread_draft(clear_thread_draft_id.as_str());
                                        view.composer_upload_error = None;
                                    } else {
                                        view.reduce_composer_domain(
                                            ComposerDomainAction::SendFailed,
                                        );
                                        view.composer_upload_error = Some(
                                            t!("chat.composer.send_failed").to_string(),
                                        );
                                    }
                                    {
                                        let Some(conversation) = view
                                            .thread_conversation_mut(thread_id_for_update.as_str())
                                        else {
                                            cx.notify();
                                            return;
                                        };
                                        for event in &events {
                                            conversation.apply(event.clone());
                                        }
                                    }
                                    for event in &events {
                                        if pioneer_client::timeline::semantic::apply_conversation_event_to_semantic_timeline(
                                            &mut view.semantic_timelines,
                                            workspace_id_for_update.as_str(),
                                            event,
                                            pioneer_client::timeline::labels::now_unix_ms(),
                                        ) {
                                            view.semantic_timeline_revision = view
                                                .semantic_timeline_revision
                                                .saturating_add(1);
                                        }
                                    }

                                    cx.notify();
                                });
                            }
                        },
                        )
                        .detach();

                        cx.notify();
                    });
                }
            },
        )
        .detach();

        cx.notify();
    }

    pub(super) fn stop_active_turn(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.can_cancel_active_thread_agent_presentation() {
            return;
        }
        let Some(thread_id) = self.active_thread_id.clone() else {
            return;
        };
        let Some(turn_id) = self.in_flight_turn_id_for_thread(thread_id.as_str()) else {
            return;
        };

        let Some(conversation) = self.thread_conversation_mut(thread_id.as_str()) else {
            return;
        };
        let Some(cancel_request) = turn_cancel::plan_turn_cancel_request(
            thread_id.clone(),
            turn_id.clone(),
            conversation.is_cancelling_turn(),
            Some(t!("chat.composer.stop_reason").to_string()),
        ) else {
            return;
        };

        let turn_cancel::TurnCancelRequest {
            requested_event,
            params,
        } = cancel_request;
        conversation.apply(requested_event);

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();

            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.turn_cancel(params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    match result {
                        Ok(response) => {
                            if let Some(event) = turn_cancel::turn_cancel_response_event(response)
                                && let Some(conversation) =
                                    view.thread_conversation_mut(thread_id.as_str())
                            {
                                conversation.apply(event);
                            }
                        }
                        Err(error) => {
                            if let Some(conversation) =
                                view.thread_conversation_mut(thread_id.as_str())
                            {
                                conversation.apply(turn_cancel::local_turn_cancel_rejected_event(
                                    thread_id.clone(),
                                    turn_id.clone(),
                                    format!("{error:#}"),
                                ));
                            }
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();

        cx.notify();
    }

    fn append_composer_attachment_paths(&mut self, paths: Vec<PathBuf>) {
        if self.desktop_voice_context_locked() {
            return;
        }
        let mut attachments = self.composer_attachments.clone();
        if composer_attachments::append_composer_attachment_paths(&mut attachments, paths) {
            self.reduce_composer_domain(ComposerDomainAction::SetAttachments { attachments });
        }
    }

    pub(in crate::app) fn attach_artifact_to_composer(
        &mut self,
        artifact: ArtifactRef,
        cx: &mut Context<Self>,
    ) {
        if self.desktop_voice_context_locked() {
            return;
        }
        if self
            .reduce_composer_domain(ComposerDomainAction::AddArtifactAttachment { artifact })
            .changed
        {
            self.composer_upload_error = None;
            cx.notify();
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn desktop_message_send_reuses_turn_start_and_clears_only_after_acceptance() {
        let source = include_str!("actions.rs");
        assert!(source.contains("ws_sender.turn_start"));
        assert!(source.contains("reply_to_turn_id: reply_to_turn_id.clone()"));
        assert!(source.contains("mentioned_principal_ids: mentioned_principal_ids.clone()"));
        assert!(source.contains("ComposerDomainAction::SendSucceeded"));
        assert!(source.contains("ComposerDomainAction::SendFailed"));
        assert!(source.contains("if accepted"));
        assert!(!source.contains(&["message", "/send"].concat()));
    }
}
