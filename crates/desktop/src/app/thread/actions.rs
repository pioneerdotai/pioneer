#[cfg(test)]
use super::super::root::ComposerAttachment;
use super::super::{
    conversation::ConversationEvent,
    root::{ComposerCapability, PioneerDesktop},
};
use gpui::{prelude::*, *};
use pioneer_client::composer::attachments as composer_attachments;
use pioneer_client::composer::capabilities as composer_capabilities;
use pioneer_client::composer::turn_prepare::{
    self as composer_turn_prepare, PrepareComposerTurnRequest,
};
use pioneer_client::turns::{cancel as turn_cancel, start as turn_start};
use pioneer_protocol::ArtifactRef;
#[cfg(test)]
use pioneer_protocol::UserInput;
use std::path::PathBuf;

impl PioneerDesktop {
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
        let Some(thread_id) = self.active_thread_id.clone() else {
            return;
        };

        let composer_text = composer_state.read(cx).value().trim().to_owned();
        let composer_attachments = self.composer_attachments.clone();
        let composer_capabilities = self.composer_capabilities.clone();
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
                                let error = format!("{error:#}");
                                view.composer_upload_in_progress = false;
                                view.composer_upload_error = Some(error.clone());
                                composer_turn_prepare::mark_uploading_composer_attachments_failed(
                                    &mut view.composer_attachments,
                                    error.as_str(),
                                );
                                cx.notify();
                                return;
                            }
                        };

                        view.composer_upload_in_progress = false;
                        view.composer_upload_error = None;
                        composer_turn_prepare::apply_uploaded_composer_attachment_artifacts(
                            &mut view.composer_attachments,
                            prepared
                                .attachments
                                .iter()
                                .map(|prepared_attachment| prepared_attachment.artifact.clone()),
                        );
                        view.clear_composer(window, cx);
                        view.clear_thread_draft(thread_id.as_str());

                        if let Some(coordinator) = view.thread_coordinator_mut(thread_id.as_str()) {
                            if let Some(thread) = coordinator.thread_mut() {
                                if let (Some(model), Some(provider)) =
                                    (selected_model.as_deref(), selected_provider.as_deref())
                                {
                                    thread.model = model.to_owned();
                                    thread.model_provider = provider.to_owned();
                                }
                                if thread.preview.trim().is_empty() {
                                    thread.preview = prepared.user_text.clone();
                                }
                                thread.updated_at = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|duration| duration.as_secs() as i64)
                                    .unwrap_or_default();
                            }
                        }

                        let promoted_from_draft =
                            view.promote_thread_from_draft(thread_id.as_str());
                        if promoted_from_draft {
                            view.rebuild_sidebar_tree_state(cx);
                            let _ = view.drive_thread_start_queue(cx);
                        }

                        let Some(conversation) = view.thread_conversation_mut(thread_id.as_str())
                        else {
                            cx.notify();
                            return;
                        };
                        conversation.apply(turn_start::local_turn_start_requested_event(
                            thread_id.clone(),
                            turn_id.clone(),
                            pending_request_id.clone(),
                            prepared.user_message_text.clone(),
                            prepared.user_attachments.clone(),
                        ));

                        let ws_sender = turn_start_sender.clone();
                        let thread_id_for_send = thread_id.clone();
                        let turn_id_for_send = turn_id.clone();
                        let turn_input_for_send = prepared.input.clone();
                        let turn_capabilities_for_send = prepared.capabilities.clone();
                        let pending_request_id_for_send = pending_request_id.clone();
                        let selected_model_for_send = selected_model.clone();
                        let selected_provider_for_send = selected_provider.clone();

                        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
                            let mut cx = cx.clone();
                            async move {
                                let result = cx
                                    .background_spawn(async move {
                                        ws_sender.turn_start(
                                            turn_start::turn_start_params_from_plan(
                                                turn_start::TurnStartParamsPlan {
                                                    thread_id: thread_id_for_send.clone(),
                                                    turn_id: turn_id_for_send.clone(),
                                                    input: turn_input_for_send,
                                                    capabilities: turn_capabilities_for_send,
                                                    model: selected_model_for_send,
                                                    model_provider: selected_provider_for_send,
                                                    mode: Some(selected_mode),
                                                },
                                            ),
                                        )
                                    })
                                    .await;

                                let _ = this.update(&mut cx, |view, cx| {
                                    match result {
                                        Ok(response) => {
                                            let Some(conversation) =
                                                view.thread_conversation_mut(thread_id.as_str())
                                            else {
                                                return;
                                            };
                                            conversation.apply(
                                                turn_start::local_turn_start_accepted_event(
                                                    thread_id.clone(),
                                                    turn_id.clone(),
                                                    pending_request_id_for_send.clone(),
                                                ),
                                            );
                                            conversation.apply(ConversationEvent::TurnStarted {
                                                thread_id: thread_id.clone(),
                                                turn: response.turn,
                                            });
                                        }
                                        Err(error) => {
                                            let Some(conversation) =
                                                view.thread_conversation_mut(thread_id.as_str())
                                            else {
                                                return;
                                            };
                                            conversation.apply(
                                                turn_start::local_turn_start_rejected_event(
                                                    thread_id.clone(),
                                                    turn_id.clone(),
                                                    pending_request_id_for_send.clone(),
                                                    format!("{error:#}"),
                                                ),
                                            );
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

#[cfg(test)]
fn build_turn_input_from_composer(
    text: &str,
    attachments: &[ComposerAttachment],
) -> Vec<UserInput> {
    composer_turn_prepare::turn_input_from_composer_attachments(text, attachments)
}

#[cfg(test)]
fn user_message_preview_from_input(input: &[UserInput]) -> String {
    let mut body_lines = Vec::new();
    let mut attachment_names = Vec::new();

    for item in input {
        match item {
            UserInput::Text { text, .. } => {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    body_lines.push(trimmed.to_owned());
                }
            }
            UserInput::Image { url }
            | UserInput::File { url }
            | UserInput::Audio { url }
            | UserInput::Video { url } => {
                attachment_names.push(composer_turn_prepare::attachment_name_from_reference(
                    url.as_str(),
                ));
            }
            UserInput::LocalImage { path }
            | UserInput::LocalFile { path }
            | UserInput::LocalAudio { path }
            | UserInput::LocalVideo { path } => {
                attachment_names.push(composer_turn_prepare::attachment_name_from_reference(
                    path.as_str(),
                ));
            }
            UserInput::Artifact {
                artifact_id,
                version_id,
            } => {
                attachment_names.push(
                    version_id
                        .as_deref()
                        .filter(|value| !value.trim().is_empty())
                        .unwrap_or(artifact_id.as_str())
                        .to_owned(),
                );
            }
            UserInput::Mention { name, .. } => {
                body_lines.push(t!("chat.composer.preview.mention", name = name).to_string())
            }
        }
    }

    if !attachment_names.is_empty() {
        body_lines.push(attachment_names.join(", "));
    }

    let rendered = body_lines.join("\n");
    if rendered.trim().is_empty() {
        String::new()
    } else {
        rendered
    }
}

#[cfg(test)]
mod tests {
    use super::{build_turn_input_from_composer, user_message_preview_from_input};
    use crate::app::root::{
        ComposerAttachment, ComposerAttachmentUploadState, ComposerCapability,
        ComposerCapabilityKind,
    };
    use pioneer_client::composer::attachments::{
        ComposerAttachmentKind, composer_attachment_kind_from_artifact_kind, infer_attachment_kind,
        uploaded_artifact_from_attachment,
    };
    use pioneer_client::composer::capabilities::add_composer_capability;
    use pioneer_client::composer::turn_prepare::{
        self as composer_turn_prepare, PreparedComposerAttachment, build_prepared_composer_turn,
    };
    use pioneer_protocol::{
        ArtifactKind, ArtifactRef, ArtifactStatus, McpScopeKind, TurnCapabilityKind, UserInput,
        UserMessageAttachment,
    };

    fn attachment(path: &str, file_name: &str, kind: ComposerAttachmentKind) -> ComposerAttachment {
        ComposerAttachment {
            path: path.to_owned(),
            file_name: file_name.to_owned(),
            kind,
            upload_state: ComposerAttachmentUploadState::Local,
        }
    }

    fn artifact_ref(id: &str, version_id: &str, display_name: &str) -> ArtifactRef {
        ArtifactRef {
            artifact_id: id.to_owned(),
            version_id: Some(version_id.to_owned()),
            display_name: display_name.to_owned(),
            kind: ArtifactKind::File,
            mime_type: Some("text/plain".to_owned()),
            size_bytes: Some(5),
            sha256: Some("a".repeat(64)),
            status: ArtifactStatus::Ready,
            preview: None,
        }
    }

    fn skill_capability(slug: &str) -> ComposerCapability {
        ComposerCapability {
            id: format!("skill:user:{slug}"),
            label: slug.to_owned(),
            kind: ComposerCapabilityKind::Skill {
                slug: slug.to_owned(),
                source_kind: "user".to_owned(),
            },
        }
    }

    fn mcp_tool_capability(server_name: &str, raw_tool_name: &str) -> ComposerCapability {
        ComposerCapability {
            id: format!("mcp-tool:workspace:{server_name}:{raw_tool_name}"),
            label: format!("{server_name} / {raw_tool_name}"),
            kind: ComposerCapabilityKind::McpTool {
                server_name: server_name.to_owned(),
                raw_tool_name: raw_tool_name.to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
        }
    }

    fn mcp_server_capability(server_name: &str) -> ComposerCapability {
        ComposerCapability {
            id: format!("mcp-server:workspace:{server_name}"),
            label: server_name.to_owned(),
            kind: ComposerCapabilityKind::McpServer {
                name: server_name.to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
        }
    }

    #[test]
    fn add_composer_capability_replaces_server_with_tool_for_same_mcp_server() {
        let mut capabilities = vec![mcp_server_capability("resend")];

        add_composer_capability(
            &mut capabilities,
            mcp_tool_capability("resend", "send_email"),
        );

        assert_eq!(capabilities.len(), 1);
        assert!(matches!(
            capabilities[0].kind,
            ComposerCapabilityKind::McpTool {
                ref server_name,
                ref raw_tool_name,
                ..
            } if server_name == "resend" && raw_tool_name == "send_email"
        ));
    }

    #[test]
    fn add_composer_capability_replaces_tools_with_server_for_same_mcp_server() {
        let mut capabilities = vec![
            mcp_tool_capability("resend", "send_email"),
            mcp_tool_capability("browser", "open"),
        ];

        add_composer_capability(&mut capabilities, mcp_server_capability("resend"));

        assert_eq!(capabilities.len(), 2);
        assert!(capabilities.iter().any(|capability| matches!(
            capability.kind,
            ComposerCapabilityKind::McpServer { ref name, .. } if name == "resend"
        )));
        assert!(capabilities.iter().any(|capability| matches!(
            capability.kind,
            ComposerCapabilityKind::McpTool {
                ref server_name,
                ref raw_tool_name,
                ..
            } if server_name == "browser" && raw_tool_name == "open"
        )));
    }

    #[test]
    fn build_turn_input_maps_local_attachments_by_kind() {
        let attachments = vec![
            attachment("/tmp/snap.png", "snap.png", ComposerAttachmentKind::Image),
            attachment(
                "/tmp/readme.pdf",
                "readme.pdf",
                ComposerAttachmentKind::File,
            ),
            attachment("/tmp/note.mp3", "note.mp3", ComposerAttachmentKind::Audio),
            attachment("/tmp/demo.mp4", "demo.mp4", ComposerAttachmentKind::Video),
        ];

        let input = build_turn_input_from_composer("please analyze", attachments.as_slice());

        assert_eq!(input.len(), 5);
        assert!(matches!(
            input[0],
            UserInput::Text { ref text, .. } if text == "please analyze"
        ));
        assert!(matches!(input[1], UserInput::LocalImage { .. }));
        assert!(matches!(input[2], UserInput::LocalFile { .. }));
        assert!(matches!(input[3], UserInput::LocalAudio { .. }));
        assert!(matches!(input[4], UserInput::LocalVideo { .. }));
    }

    #[test]
    fn build_turn_input_without_attachments_keeps_plain_text() {
        let input = build_turn_input_from_composer("hello world", &[]);
        assert_eq!(input.len(), 1);
        assert!(matches!(
            input[0],
            UserInput::Text { ref text, .. } if text == "hello world"
        ));
    }

    #[test]
    fn prepared_composer_allows_capability_only_submit_payload() {
        let prepared =
            build_prepared_composer_turn(String::new(), Vec::new(), vec![skill_capability("docs")]);

        assert!(prepared.input.is_empty());
        assert_eq!(prepared.capabilities.len(), 1);
        assert!(matches!(
            prepared.capabilities[0].kind,
            TurnCapabilityKind::Skill { ref slug, .. } if slug == "docs"
        ));
        assert!(matches!(
            prepared.user_attachments[0],
            UserMessageAttachment::Skill { ref capability }
                if capability.slug == "docs" && capability.label == "docs"
        ));
        assert_eq!(prepared.user_message_text, "");
    }

    #[test]
    fn prepared_composer_keeps_text_separate_from_capability_labels() {
        let prepared = build_prepared_composer_turn(
            "hello world".to_owned(),
            Vec::new(),
            vec![mcp_tool_capability("browser", "open")],
        );

        assert_eq!(prepared.input.len(), 1);
        assert!(matches!(
            prepared.input[0],
            UserInput::Text { ref text, .. } if text == "hello world"
        ));
        assert_eq!(prepared.capabilities.len(), 1);
        assert!(matches!(
            prepared.capabilities[0].kind,
            TurnCapabilityKind::McpTool {
                ref server_name,
                ref raw_tool_name,
                ..
            } if server_name == "browser" && raw_tool_name == "open"
        ));
        assert!(matches!(
            prepared.user_attachments[0],
            UserMessageAttachment::McpTool { ref capability }
                if capability.server_name == "browser" && capability.raw_tool_name == "open"
        ));
        assert_eq!(prepared.user_message_text, "hello world");
    }

    #[test]
    fn preview_renders_text_with_attachment_names() {
        let input = vec![
            UserInput::Text {
                text: "summary".to_owned(),
                text_elements: Vec::new(),
            },
            UserInput::LocalFile {
                path: "/tmp/report.pdf".to_owned(),
            },
        ];
        let preview = user_message_preview_from_input(input.as_slice());
        assert_eq!(preview, "summary\nreport.pdf");
    }

    #[test]
    fn prepared_composer_maps_uploaded_attachments_to_artifact_input() {
        let prepared = vec![
            PreparedComposerAttachment {
                attachment: attachment("/tmp/a.txt", "a.txt", ComposerAttachmentKind::File),
                artifact: Some(artifact_ref("art_a", "av_a", "a.txt")),
            },
            PreparedComposerAttachment {
                attachment: attachment("/tmp/b.txt", "b.txt", ComposerAttachmentKind::File),
                artifact: Some(artifact_ref("art_b", "av_b", "b.txt")),
            },
        ];

        let input =
            composer_turn_prepare::turn_input_from_prepared_composer("hello", prepared.as_slice());

        assert_eq!(input.len(), 3);
        assert!(matches!(
            input[1],
            UserInput::Artifact {
                ref artifact_id,
                ref version_id,
            } if artifact_id == "art_a" && version_id.as_deref() == Some("av_a")
        ));
        assert!(matches!(
            input[2],
            UserInput::Artifact {
                ref artifact_id,
                ref version_id,
            } if artifact_id == "art_b" && version_id.as_deref() == Some("av_b")
        ));
    }

    #[test]
    fn prepared_composer_keeps_local_input_when_upload_is_not_required() {
        let prepared = vec![PreparedComposerAttachment {
            attachment: attachment("/tmp/a.txt", "a.txt", ComposerAttachmentKind::File),
            artifact: None,
        }];

        let input =
            composer_turn_prepare::turn_input_from_prepared_composer("hello", prepared.as_slice());

        assert!(matches!(input[1], UserInput::LocalFile { .. }));
    }

    #[test]
    fn prepared_composer_preview_uses_uploaded_display_names() {
        let prepared = vec![PreparedComposerAttachment {
            attachment: attachment("/tmp/a.txt", "local-a.txt", ComposerAttachmentKind::File),
            artifact: Some(artifact_ref("art_a", "av_a", "remote-a.txt")),
        }];

        let preview = composer_turn_prepare::user_message_preview_from_prepared_composer(
            "hello",
            prepared.as_slice(),
        );

        assert_eq!(preview, "hello\nremote-a.txt");
    }

    #[test]
    fn uploaded_composer_attachment_reuses_existing_artifact_ref() {
        let artifact = artifact_ref("art_a", "av_a", "remote-a.txt");
        let attachment = ComposerAttachment {
            path: "artifact://art_a#av_a".to_owned(),
            file_name: "remote-a.txt".to_owned(),
            kind: ComposerAttachmentKind::File,
            upload_state: ComposerAttachmentUploadState::Uploaded {
                artifact: artifact.clone(),
            },
        };

        assert_eq!(
            uploaded_artifact_from_attachment(&attachment),
            Some(artifact)
        );
    }

    #[test]
    fn artifact_kind_maps_to_composer_attachment_kind() {
        assert_eq!(
            composer_attachment_kind_from_artifact_kind(ArtifactKind::GeneratedImage),
            ComposerAttachmentKind::Image
        );
        assert_eq!(
            composer_attachment_kind_from_artifact_kind(ArtifactKind::Audio),
            ComposerAttachmentKind::Audio
        );
        assert_eq!(
            composer_attachment_kind_from_artifact_kind(ArtifactKind::Json),
            ComposerAttachmentKind::File
        );
    }

    #[test]
    fn infer_attachment_kind_uses_extension() {
        assert_eq!(
            infer_attachment_kind("/tmp/sample.png"),
            ComposerAttachmentKind::Image
        );
        assert_eq!(
            infer_attachment_kind("/tmp/sample.wav"),
            ComposerAttachmentKind::Audio
        );
        assert_eq!(
            infer_attachment_kind("/tmp/sample.mp4"),
            ComposerAttachmentKind::Video
        );
        assert_eq!(
            infer_attachment_kind("/tmp/sample.bin"),
            ComposerAttachmentKind::File
        );
    }
}
