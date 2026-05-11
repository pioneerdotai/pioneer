use super::super::{
    conversation::ConversationEvent,
    root::{
        ComposerAttachment, ComposerAttachmentKind, ComposerAttachmentUploadState, PioneerDesktop,
    },
};
use crate::gateway::{DesktopArtifactUploadRequest, GatewayEndpointKind, GatewayWsCommandSender};
use anyhow::{Context as AnyhowContext, Result, anyhow};
use gpui::{prelude::*, *};
use pioneer_protocol::{
    ArtifactCapabilitiesParams, ArtifactCapabilitiesResponse, ArtifactKind, ArtifactRef,
    REQUEST_ID_LEN, TurnCancelParams, TurnStartParams, UserInput, generate_id,
};
use std::path::{Path, PathBuf};

const TURN_ID_LEN: usize = 21;

#[derive(Debug, Clone)]
struct PreparedComposerAttachment {
    attachment: ComposerAttachment,
    artifact: Option<ArtifactRef>,
}

#[derive(Debug, Clone)]
struct PreparedComposerTurn {
    input: Vec<UserInput>,
    user_text: String,
    attachments: Vec<PreparedComposerAttachment>,
}

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
        if index < self.composer_attachments.len() {
            self.composer_attachments.remove(index);
            self.composer_upload_error = None;
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
        if composer_text.is_empty() && composer_attachments.is_empty() {
            return;
        }
        let turn_id = generate_id(TURN_ID_LEN);
        let pending_request_id = generate_id(REQUEST_ID_LEN);
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
        for attachment in &mut self.composer_attachments {
            if !matches!(
                attachment.upload_state,
                ComposerAttachmentUploadState::Uploaded { .. }
            ) {
                attachment.upload_state = ComposerAttachmentUploadState::Uploading;
            }
        }
        cx.notify();

        cx.spawn_in(
            window,
            move |this: WeakEntity<Self>, cx: &mut AsyncWindowContext| {
                let mut cx = cx.clone();
                let thread_id_for_prepare = thread_id.clone();
                let turn_id_for_prepare = turn_id.clone();
                let composer_text_for_prepare = composer_text.clone();
                let composer_attachments_for_prepare = composer_attachments.clone();
                let workspace_id_for_prepare = workspace_id.clone();

                async move {
                    let prepare_result = cx
                        .background_spawn(async move {
                            prepare_composer_turn(
                                upload_sender,
                                workspace_id_for_prepare,
                                thread_id_for_prepare,
                                turn_id_for_prepare,
                                endpoint_kind,
                                composer_text_for_prepare,
                                composer_attachments_for_prepare,
                            )
                        })
                        .await;

                    let _ = this.update_in(&mut cx, move |view, window, cx| {
                        let prepared = match prepare_result {
                            Ok(prepared) => prepared,
                            Err(error) => {
                                let error = format!("{error:#}");
                                view.composer_upload_in_progress = false;
                                view.composer_upload_error = Some(error.clone());
                                for attachment in &mut view.composer_attachments {
                                    if matches!(
                                        attachment.upload_state,
                                        ComposerAttachmentUploadState::Uploading
                                    ) {
                                        attachment.upload_state =
                                            ComposerAttachmentUploadState::Failed {
                                                error: error.clone(),
                                            };
                                    }
                                }
                                cx.notify();
                                return;
                            }
                        };

                        view.composer_upload_in_progress = false;
                        view.composer_upload_error = None;
                        for (attachment, prepared_attachment) in view
                            .composer_attachments
                            .iter_mut()
                            .zip(prepared.attachments.iter())
                        {
                            if let Some(artifact) = prepared_attachment.artifact.as_ref() {
                                attachment.upload_state = ComposerAttachmentUploadState::Uploaded {
                                    artifact: artifact.clone(),
                                };
                            }
                        }
                        view.clear_composer(window, cx);
                        view.clear_thread_draft(thread_id.as_str());

                        if let Some(coordinator) = view.thread_coordinator_mut(thread_id.as_str()) {
                            if let Some(thread) = coordinator.thread_mut() {
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
                        conversation.apply(ConversationEvent::LocalTurnStartRequested {
                            thread_id: thread_id.clone(),
                            turn_id: turn_id.clone(),
                            pending_request_id: pending_request_id.clone(),
                            user_text: prepared.user_text.clone(),
                        });

                        let ws_sender = turn_start_sender.clone();
                        let thread_id_for_send = thread_id.clone();
                        let turn_id_for_send = turn_id.clone();
                        let turn_input_for_send = prepared.input.clone();
                        let pending_request_id_for_send = pending_request_id.clone();
                        let selected_model_for_send = selected_model.clone();
                        let selected_provider_for_send = selected_provider.clone();

                        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
                            let mut cx = cx.clone();
                            async move {
                                let result = cx
                                    .background_spawn(async move {
                                        ws_sender.turn_start(TurnStartParams {
                                            thread_id: thread_id_for_send.clone(),
                                            turn_id: turn_id_for_send.clone(),
                                            input: turn_input_for_send,
                                            model: selected_model_for_send,
                                            model_provider: selected_provider_for_send,
                                            sandbox_policy: None,
                                            mode: Some(selected_mode),
                                        })
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
                                                ConversationEvent::LocalTurnStartAccepted {
                                                    thread_id: thread_id.clone(),
                                                    turn_id: turn_id.clone(),
                                                    pending_request_id: pending_request_id_for_send
                                                        .clone(),
                                                },
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
                                                ConversationEvent::LocalTurnStartRejected {
                                                    thread_id: thread_id.clone(),
                                                    turn_id: turn_id.clone(),
                                                    pending_request_id: pending_request_id_for_send
                                                        .clone(),
                                                    error: format!("{error:#}"),
                                                },
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
        if conversation.is_cancelling_turn() {
            return;
        }

        conversation.apply(ConversationEvent::LocalTurnCancelRequested {
            thread_id: thread_id.clone(),
            turn_id: turn_id.clone(),
        });

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let thread_id_for_request = thread_id.clone();
            let turn_id_for_request = turn_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.turn_cancel(TurnCancelParams {
                            thread_id: thread_id_for_request,
                            turn_id: turn_id_for_request,
                            reason: Some(t!("chat.composer.stop_reason").to_string()),
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if let Err(error) = result
                        && let Some(conversation) = view.thread_conversation_mut(thread_id.as_str())
                    {
                        conversation.apply(ConversationEvent::LocalTurnCancelRejected {
                            thread_id: thread_id.clone(),
                            turn_id: turn_id.clone(),
                            error: format!("{error:#}"),
                        });
                    }

                    cx.notify();
                });
            }
        })
        .detach();

        cx.notify();
    }

    fn append_composer_attachment_paths(&mut self, paths: Vec<PathBuf>) {
        for path in paths {
            let path_value = path.to_string_lossy().trim().to_owned();
            if path_value.is_empty() {
                continue;
            }

            if self
                .composer_attachments
                .iter()
                .any(|attachment| attachment.path == path_value)
            {
                continue;
            }

            let file_name = path
                .file_name()
                .and_then(|value| value.to_str())
                .map(str::to_owned)
                .unwrap_or_else(|| path_value.clone());

            self.composer_attachments.push(ComposerAttachment {
                path: path_value.clone(),
                file_name,
                kind: infer_attachment_kind(path_value.as_str()),
                upload_state: ComposerAttachmentUploadState::Local,
            });
        }
    }

    pub(in crate::app) fn attach_artifact_to_composer(
        &mut self,
        artifact: ArtifactRef,
        cx: &mut Context<Self>,
    ) {
        if artifact.status != pioneer_protocol::ArtifactStatus::Ready {
            return;
        }
        if self.composer_attachments.iter().any(|attachment| {
            matches!(
                &attachment.upload_state,
                ComposerAttachmentUploadState::Uploaded { artifact: existing }
                    if existing.artifact_id == artifact.artifact_id
                        && existing.version_id == artifact.version_id
            )
        }) {
            return;
        }

        let file_name = if artifact.display_name.trim().is_empty() {
            artifact.artifact_id.clone()
        } else {
            artifact.display_name.clone()
        };
        let version_suffix = artifact
            .version_id
            .as_ref()
            .map(|version_id| format!("#{version_id}"))
            .unwrap_or_default();
        self.composer_attachments.push(ComposerAttachment {
            path: format!("artifact://{}{}", artifact.artifact_id, version_suffix),
            file_name,
            kind: composer_attachment_kind_from_artifact_kind(artifact.kind),
            upload_state: ComposerAttachmentUploadState::Uploaded { artifact },
        });
        self.composer_upload_error = None;
        cx.notify();
    }
}

fn prepare_composer_turn(
    ws_sender: GatewayWsCommandSender,
    workspace_id: String,
    thread_id: String,
    turn_id: String,
    endpoint_kind: Option<GatewayEndpointKind>,
    text: String,
    attachments: Vec<ComposerAttachment>,
) -> Result<PreparedComposerTurn> {
    if workspace_id.trim().is_empty() {
        return Err(anyhow!(
            "workspace_id is required before uploading attachments"
        ));
    }

    let capabilities = if attachments.is_empty() {
        None
    } else {
        ws_sender
            .artifact_capabilities(ArtifactCapabilitiesParams {
                workspace_id: workspace_id.clone(),
            })
            .ok()
    };
    let upload_required =
        local_attachments_require_artifact_upload(endpoint_kind, capabilities.as_ref());
    let mut prepared_attachments = Vec::with_capacity(attachments.len());

    for (index, attachment) in attachments.into_iter().enumerate() {
        let artifact = if let Some(artifact) = uploaded_artifact_from_attachment(&attachment) {
            Some(artifact)
        } else if upload_required {
            let client_attachment_id = format!("{turn_id}_attachment_{index}");
            Some(
                ws_sender
                    .upload_artifact_file(DesktopArtifactUploadRequest {
                        workspace_id: workspace_id.clone(),
                        thread_id: Some(thread_id.clone()),
                        planned_turn_id: Some(turn_id.clone()),
                        client_attachment_id,
                        path: PathBuf::from(attachment.path.clone()),
                        mime_type: None,
                    })
                    .with_context(|| {
                        format!(
                            "failed to upload `{}` before starting turn",
                            attachment.file_name
                        )
                    })?,
            )
        } else {
            None
        };

        prepared_attachments.push(PreparedComposerAttachment {
            attachment,
            artifact,
        });
    }

    let input =
        build_turn_input_from_prepared_composer(text.as_str(), prepared_attachments.as_slice());
    let user_text =
        user_message_preview_from_prepared_composer(text.as_str(), prepared_attachments.as_slice());

    Ok(PreparedComposerTurn {
        input,
        user_text,
        attachments: prepared_attachments,
    })
}

fn local_attachments_require_artifact_upload(
    endpoint_kind: Option<GatewayEndpointKind>,
    capabilities: Option<&ArtifactCapabilitiesResponse>,
) -> bool {
    if capabilities.is_some_and(|capabilities| capabilities.upload.required_for_local_paths) {
        return true;
    }

    !matches!(endpoint_kind, Some(GatewayEndpointKind::Local)) || capabilities.is_none()
}

fn build_turn_input_from_prepared_composer(
    text: &str,
    attachments: &[PreparedComposerAttachment],
) -> Vec<UserInput> {
    let mut input = Vec::new();

    let text = text.trim();
    if !text.is_empty() {
        input.push(UserInput::Text {
            text: text.to_owned(),
            text_elements: Vec::new(),
        });
    }

    for prepared in attachments {
        if let Some(artifact) = prepared.artifact.as_ref() {
            input.push(UserInput::Artifact {
                artifact_id: artifact.artifact_id.clone(),
                version_id: artifact.version_id.clone(),
            });
        } else {
            input.push(local_user_input_from_attachment(&prepared.attachment));
        }
    }

    input
}

#[cfg(test)]
fn build_turn_input_from_composer(
    text: &str,
    attachments: &[ComposerAttachment],
) -> Vec<UserInput> {
    let mut input = Vec::new();

    let text = text.trim();
    if !text.is_empty() {
        input.push(UserInput::Text {
            text: text.to_owned(),
            text_elements: Vec::new(),
        });
    }

    for attachment in attachments {
        input.push(local_user_input_from_attachment(attachment));
    }

    input
}

fn local_user_input_from_attachment(attachment: &ComposerAttachment) -> UserInput {
    match attachment.kind {
        ComposerAttachmentKind::Image => UserInput::LocalImage {
            path: attachment.path.clone(),
        },
        ComposerAttachmentKind::Audio => UserInput::LocalAudio {
            path: attachment.path.clone(),
        },
        ComposerAttachmentKind::Video => UserInput::LocalVideo {
            path: attachment.path.clone(),
        },
        ComposerAttachmentKind::File => UserInput::LocalFile {
            path: attachment.path.clone(),
        },
    }
}

fn uploaded_artifact_from_attachment(attachment: &ComposerAttachment) -> Option<ArtifactRef> {
    match &attachment.upload_state {
        ComposerAttachmentUploadState::Uploaded { artifact } => Some(artifact.clone()),
        _ => None,
    }
}

fn user_message_preview_from_prepared_composer(
    text: &str,
    attachments: &[PreparedComposerAttachment],
) -> String {
    let mut body_lines = Vec::new();
    let trimmed = text.trim();
    if !trimmed.is_empty() {
        body_lines.push(trimmed.to_owned());
    }

    let attachment_names = attachments
        .iter()
        .map(|prepared| {
            let display_name = prepared
                .artifact
                .as_ref()
                .map(|artifact| artifact.display_name.as_str())
                .filter(|display_name| !display_name.trim().is_empty())
                .unwrap_or(prepared.attachment.file_name.as_str());
            if display_name.trim().is_empty() {
                attachment_name_from_reference(prepared.attachment.path.as_str())
            } else {
                display_name.to_owned()
            }
        })
        .filter(|name| !name.trim().is_empty())
        .collect::<Vec<_>>();

    if !attachment_names.is_empty() {
        body_lines.push(attachment_names.join(", "));
    }

    body_lines.join("\n")
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
                attachment_names.push(attachment_name_from_reference(url.as_str()));
            }
            UserInput::LocalImage { path }
            | UserInput::LocalFile { path }
            | UserInput::LocalAudio { path }
            | UserInput::LocalVideo { path } => {
                attachment_names.push(attachment_name_from_reference(path.as_str()));
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
            UserInput::Skill { name, .. } => {
                body_lines.push(t!("chat.composer.preview.skill", name = name).to_string())
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

fn attachment_name_from_reference(reference: &str) -> String {
    if reference.contains("://") || reference.starts_with("data:") {
        let without_query = reference
            .split_once('?')
            .map_or(reference, |(value, _)| value);
        let without_fragment = without_query
            .split_once('#')
            .map_or(without_query, |(value, _)| value);
        let candidate = without_fragment
            .rsplit('/')
            .next()
            .unwrap_or(without_fragment);
        if candidate.is_empty() {
            reference.to_owned()
        } else {
            candidate.to_owned()
        }
    } else {
        Path::new(reference)
            .file_name()
            .and_then(|value| value.to_str())
            .map(str::to_owned)
            .unwrap_or_else(|| reference.to_owned())
    }
}

fn infer_attachment_kind(path: &str) -> ComposerAttachmentKind {
    let extension = Path::new(path)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase());

    match extension.as_deref() {
        Some("png") | Some("jpg") | Some("jpeg") | Some("gif") | Some("webp") | Some("bmp")
        | Some("tif") | Some("tiff") | Some("avif") | Some("heic") | Some("heif") | Some("svg") => {
            ComposerAttachmentKind::Image
        }
        Some("mp3") | Some("wav") | Some("m4a") | Some("aac") | Some("ogg") | Some("oga")
        | Some("flac") => ComposerAttachmentKind::Audio,
        Some("mp4") | Some("mov") | Some("webm") | Some("mkv") | Some("avi") | Some("mpeg")
        | Some("mpg") => ComposerAttachmentKind::Video,
        _ => ComposerAttachmentKind::File,
    }
}

fn composer_attachment_kind_from_artifact_kind(kind: ArtifactKind) -> ComposerAttachmentKind {
    match kind {
        ArtifactKind::Image | ArtifactKind::GeneratedImage | ArtifactKind::Screenshot => {
            ComposerAttachmentKind::Image
        }
        ArtifactKind::Audio => ComposerAttachmentKind::Audio,
        ArtifactKind::Video => ComposerAttachmentKind::Video,
        _ => ComposerAttachmentKind::File,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PreparedComposerAttachment, build_turn_input_from_composer,
        build_turn_input_from_prepared_composer, composer_attachment_kind_from_artifact_kind,
        infer_attachment_kind, local_attachments_require_artifact_upload,
        uploaded_artifact_from_attachment, user_message_preview_from_input,
        user_message_preview_from_prepared_composer,
    };
    use crate::{
        app::root::{ComposerAttachment, ComposerAttachmentKind, ComposerAttachmentUploadState},
        gateway::GatewayEndpointKind,
    };
    use pioneer_protocol::{
        ArtifactCapabilitiesResponse, ArtifactDownloadCapabilities, ArtifactKind, ArtifactRef,
        ArtifactStatus, ArtifactUploadCapabilities, UserInput,
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

    fn capabilities(required_for_local_paths: bool) -> ArtifactCapabilitiesResponse {
        ArtifactCapabilitiesResponse {
            upload: ArtifactUploadCapabilities {
                required_for_local_paths,
                recommended_chunk_size_bytes: 1024,
                max_chunk_size_bytes: 1024,
                max_file_size_bytes: 1024 * 1024,
                max_files_per_turn: 16,
            },
            download: ArtifactDownloadCapabilities {
                recommended_chunk_size_bytes: 1024,
                max_chunk_size_bytes: 1024,
                max_concurrent_downloads: 2,
            },
        }
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
    fn remote_gateway_requires_artifact_upload_even_when_capability_is_permissive() {
        let caps = capabilities(false);

        assert!(local_attachments_require_artifact_upload(
            Some(GatewayEndpointKind::Remote),
            Some(&caps)
        ));
        assert!(local_attachments_require_artifact_upload(None, Some(&caps)));
        assert!(!local_attachments_require_artifact_upload(
            Some(GatewayEndpointKind::Local),
            Some(&caps)
        ));
    }

    #[test]
    fn capability_required_for_local_paths_forces_upload() {
        let caps = capabilities(true);

        assert!(local_attachments_require_artifact_upload(
            Some(GatewayEndpointKind::Local),
            Some(&caps)
        ));
    }

    #[test]
    fn missing_capability_response_falls_back_to_upload() {
        assert!(local_attachments_require_artifact_upload(
            Some(GatewayEndpointKind::Local),
            None
        ));
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

        let input = build_turn_input_from_prepared_composer("hello", prepared.as_slice());

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

        let input = build_turn_input_from_prepared_composer("hello", prepared.as_slice());

        assert!(matches!(input[1], UserInput::LocalFile { .. }));
    }

    #[test]
    fn prepared_composer_preview_uses_uploaded_display_names() {
        let prepared = vec![PreparedComposerAttachment {
            attachment: attachment("/tmp/a.txt", "local-a.txt", ComposerAttachmentKind::File),
            artifact: Some(artifact_ref("art_a", "av_a", "remote-a.txt")),
        }];

        let preview = user_message_preview_from_prepared_composer("hello", prepared.as_slice());

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
