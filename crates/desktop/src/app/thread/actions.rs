use super::super::root::{ComposerCapability, PioneerDesktop};
use crate::gateway::DesktopGatewayWsCommandSenderExt;
use gpui::{prelude::*, *};
use pioneer_client::composer::attachments as composer_attachments;
use pioneer_client::composer::capabilities as composer_capabilities;
use pioneer_client::composer::turn_prepare::{
    self as composer_turn_prepare, PrepareComposerTurnRequest,
};
use pioneer_client::providers::list as provider_list;
use pioneer_client::turns::{cancel as turn_cancel, start as turn_start, steer as turn_steer};
use pioneer_protocol::{
    ArtifactRef, CLIRuntimeRequestResolution, CLIRuntimeRequestResolvedNotification,
    CLIRuntimeRequestRespondParams,
};
use std::path::PathBuf;
use tracing::warn;

impl PioneerDesktop {
    pub(super) fn steer_active_cli_runtime_turn(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
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

    pub(super) fn respond_cli_runtime_pending_request(
        &mut self,
        request_id: String,
        resolution: CLIRuntimeRequestResolution,
        cx: &mut Context<Self>,
    ) {
        let Some(entry) = self
            .cli_runtime_pending_requests
            .request(&request_id)
            .cloned()
        else {
            return;
        };

        let ws_sender = self.gateway.ws_command_sender.clone();
        let params = CLIRuntimeRequestRespondParams {
            workspace_id: entry.workspace_id.clone(),
            runtime_id: entry.runtime_id.clone(),
            request_id: entry.request_id.clone(),
            resolution,
        };

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.cli_runtime_request_respond(params) })
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
                        view.cli_runtime_pending_requests.apply(reduction);
                        cx.notify();
                    }
                    Err(error) => {
                        warn!(
                            request_id = entry.request_id.as_str(),
                            runtime_id = entry.runtime_id.as_str(),
                            error = %format!("{error:#}"),
                            "failed to respond to CLI runtime pending request"
                        );
                    }
                });
            }
        })
        .detach();
    }

    pub(super) fn open_composer_file_picker(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
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
                    view.append_composer_attachment_paths(paths);
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn remove_composer_attachment_at(&mut self, index: usize) {
        if composer_attachments::remove_composer_attachment_at(
            &mut self.composer_attachments,
            index,
        ) {
            self.composer_upload_error = None;
        }
    }

    pub(super) fn remove_composer_capability_at(&mut self, index: usize) {
        composer_capabilities::remove_composer_capability_at(
            &mut self.composer_capabilities,
            index,
        );
    }

    pub(super) fn add_composer_capabilities(
        &mut self,
        capabilities: impl IntoIterator<Item = ComposerCapability>,
    ) {
        for capability in capabilities {
            composer_capabilities::add_composer_capability(
                &mut self.composer_capabilities,
                capability,
            );
        }
    }

    pub(super) fn submit_composer_message(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.can_submit_message(cx) {
            return;
        }

        let composer_state = self.composer_state.clone();
        let selected_mode = self.composer_turn_mode;
        let selected_model = self.composer_selected_model.clone();
        let selected_provider = self.composer_selected_provider.clone();
        let selected_reasoning_effort = self.composer_selected_reasoning_effort.clone();
        let selected_cli_runtime_backend =
            match provider_list::resolve_cli_runtime_execution_backend(
                selected_provider.as_deref(),
                self.providers.cli_runtimes(),
                self.gateway
                    .settings
                    .as_ref()
                    .map(|settings| &settings.cli_runtimes),
            ) {
                Ok(backend) => backend,
                Err(error) => {
                    self.composer_upload_error = Some(error);
                    cx.notify();
                    return;
                }
            };
        let cli_runtime_selected = selected_cli_runtime_backend.is_some();
        let turn_model_provider = if selected_cli_runtime_backend.is_some() {
            None
        } else {
            selected_provider.clone()
        };
        let Some(thread_id) = self.active_thread_id.clone() else {
            return;
        };

        let composer_text = composer_state.read(cx).value().trim().to_owned();
        if cli_runtime_selected {
            self.composer_capabilities.clear();
            self.composer_upload_error = None;
        }
        let composer_attachments = self.composer_attachments.clone();
        let composer_capabilities = if cli_runtime_selected {
            Vec::new()
        } else {
            self.composer_capabilities.clone()
        };
        if !composer_turn_prepare::composer_has_sendable_content(
            composer_text.as_str(),
            !composer_attachments.is_empty(),
            !composer_capabilities.is_empty(),
        ) {
            return;
        }
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
        composer_turn_prepare::mark_pending_composer_attachments_uploading(
            &mut self.composer_attachments,
        );
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
                let workspace_id_for_prepare = workspace_id.clone();

                async move {
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
                            })
                        })
                        .await;

                    let _ = this.update_in(&mut cx, move |view, window, cx| {
                        let prepared = match prepare_result {
                            Ok(prepared) => prepared,
                            Err(error) => {
                                let reduction =
                                    composer_turn_prepare::reduce_prepare_composer_turn_failure(
                                        format!("{error:#}"),
                                    );
                                view.composer_upload_in_progress =
                                    reduction.composer_upload_in_progress;
                                view.composer_upload_error =
                                    Some(reduction.composer_upload_error.clone());
                                composer_turn_prepare::mark_uploading_composer_attachments_failed(
                                    &mut view.composer_attachments,
                                    reduction.mark_uploading_attachments_failed_error.as_str(),
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
                                    selected_model: selected_model.clone(),
                                    selected_provider: selected_provider.clone(),
                                    turn_model_provider: turn_model_provider.clone(),
                                    selected_mode: Some(selected_mode),
                                    execution_backend: selected_cli_runtime_backend.clone(),
                                    selected_reasoning_effort: selected_reasoning_effort.clone(),
                                    cli_runtime_options: None,
                                    updated_at_unix: turn_start::now_unix_seconds(),
                                },
                                prepared,
                            );

                        view.composer_upload_in_progress = reduction.composer_upload_in_progress;
                        if reduction.clear_composer_upload_error {
                            view.composer_upload_error = None;
                        }
                        composer_turn_prepare::apply_uploaded_composer_attachment_artifacts(
                            &mut view.composer_attachments,
                            reduction.uploaded_attachment_artifacts,
                        );
                        if reduction.clear_composer {
                            view.clear_composer(window, cx);
                        }
                        view.clear_thread_draft(reduction.clear_thread_draft_id.as_str());

                        if let Some(coordinator) = view.thread_coordinator_mut(thread_id.as_str()) {
                            if let Some(thread) = coordinator.thread_mut() {
                                turn_start::apply_prepared_turn_to_thread_snapshot(
                                    thread,
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
                            cx.notify();
                            return;
                        };
                        conversation.apply(reduction.local_turn_start_requested_event.clone());
                        if pioneer_client::timeline::semantic::apply_conversation_event_to_semantic_timeline(
                            &mut view.semantic_timelines,
                            workspace_id.as_str(),
                            &reduction.local_turn_start_requested_event,
                            pioneer_client::timeline::labels::now_unix_ms(),
                        ) {
                            view.semantic_timeline_revision =
                                view.semantic_timeline_revision.saturating_add(1);
                        }

                        let ws_sender = turn_start_sender.clone();
                        let turn_start_params_plan = reduction.turn_start_params_plan;
                        let send_context = reduction.send_context;
                        let workspace_id_for_update = workspace_id.clone();

                        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
                            let mut cx = cx.clone();
                            let thread_id_for_update = send_context.thread_id.clone();
                            let send_context_for_update = send_context.clone();
                            async move {
                                let result = cx
                                    .background_spawn(async move {
                                        ws_sender.turn_start(
                                            turn_start::turn_start_params_from_plan(
                                                turn_start_params_plan,
                                            ),
                                        )
                                    })
                                    .await;

                                let _ = this.update(&mut cx, |view, cx| {
                                    let reduction = match result {
                                        Ok(response) => turn_start::reduce_turn_start_send_success(
                                            send_context_for_update,
                                            response,
                                        ),
                                        Err(error) => turn_start::reduce_turn_start_send_failure(
                                            send_context_for_update,
                                            format!("{error:#}"),
                                        ),
                                    };
                                    let events = match reduction {
                                        turn_start::TurnStartSendReduction::Accepted { events } => {
                                            events
                                        }
                                        turn_start::TurnStartSendReduction::Rejected { event } => {
                                            vec![event]
                                        }
                                    };
                                    {
                                        let Some(conversation) = view
                                            .thread_conversation_mut(thread_id_for_update.as_str())
                                        else {
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
                        })
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
                    if let Err(error) = result
                        && let Some(conversation) = view.thread_conversation_mut(thread_id.as_str())
                    {
                        conversation.apply(turn_cancel::local_turn_cancel_rejected_event(
                            thread_id.clone(),
                            turn_id.clone(),
                            format!("{error:#}"),
                        ));
                    }

                    cx.notify();
                });
            }
        })
        .detach();

        cx.notify();
    }

    fn append_composer_attachment_paths(&mut self, paths: Vec<PathBuf>) {
        composer_attachments::append_composer_attachment_paths(
            &mut self.composer_attachments,
            paths,
        );
    }

    pub(in crate::app) fn attach_artifact_to_composer(
        &mut self,
        artifact: ArtifactRef,
        cx: &mut Context<Self>,
    ) {
        if composer_attachments::add_composer_attachment_from_artifact(
            &mut self.composer_attachments,
            artifact,
        ) {
            self.composer_upload_error = None;
            cx.notify();
        }
    }
}
