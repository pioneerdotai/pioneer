use super::super::{
    conversation::ConversationEvent,
    root::{ComposerAttachment, ComposerAttachmentKind, PioneerDesktop},
};
use gpui::{prelude::*, *};
use pioneer_protocol::{REQUEST_ID_LEN, TurnStartParams, UserInput, generate_id};
use std::path::{Path, PathBuf};

const TURN_ID_LEN: usize = 21;

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
        let turn_input =
            build_turn_input_from_composer(composer_text.as_str(), composer_attachments.as_slice());
        let user_text = user_message_preview_from_input(turn_input.as_slice());

        let turn_id = generate_id(TURN_ID_LEN);
        let pending_request_id = generate_id(REQUEST_ID_LEN);
        self.clear_composer(window, cx);
        self.clear_thread_draft(thread_id.as_str());

        if let Some(coordinator) = self.thread_coordinator_mut(thread_id.as_str()) {
            if let Some(thread) = coordinator.thread_mut() {
                if thread.preview.trim().is_empty() {
                    thread.preview = user_text.clone();
                }
                thread.updated_at = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|duration| duration.as_secs() as i64)
                    .unwrap_or_default();
            }
        }

        let promoted_from_draft = self.promote_thread_from_draft(thread_id.as_str());
        if promoted_from_draft {
            self.rebuild_sidebar_tree_state(cx);
            let _ = self.drive_thread_start_queue(cx);
        }

        let Some(conversation) = self.thread_conversation_mut(thread_id.as_str()) else {
            return;
        };
        conversation.apply(ConversationEvent::LocalTurnStartRequested {
            thread_id: thread_id.clone(),
            turn_id: turn_id.clone(),
            pending_request_id: pending_request_id.clone(),
            user_text: user_text.clone(),
        });

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let thread_id_for_send = thread_id.clone();
            let turn_id_for_send = turn_id.clone();
            let turn_input_for_send = turn_input.clone();
            let pending_request_id_for_send = pending_request_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.turn_start(TurnStartParams {
                            thread_id: thread_id_for_send.clone(),
                            turn_id: turn_id_for_send.clone(),
                            input: turn_input_for_send,
                            model: selected_model,
                            model_provider: selected_provider,
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
                            conversation.apply(ConversationEvent::LocalTurnStartAccepted {
                                thread_id: thread_id.clone(),
                                turn_id: turn_id.clone(),
                                pending_request_id: pending_request_id_for_send.clone(),
                            });
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
                            conversation.apply(ConversationEvent::LocalTurnStartRejected {
                                thread_id: thread_id.clone(),
                                turn_id: turn_id.clone(),
                                pending_request_id: pending_request_id_for_send.clone(),
                                error: format!("{error:#}"),
                            });
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
            });
        }
    }
}

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
        let item = match attachment.kind {
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
        };
        input.push(item);
    }

    input
}

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

#[cfg(test)]
mod tests {
    use super::{
        build_turn_input_from_composer, infer_attachment_kind, user_message_preview_from_input,
    };
    use crate::app::root::{ComposerAttachment, ComposerAttachmentKind};
    use pioneer_protocol::UserInput;

    #[test]
    fn build_turn_input_maps_local_attachments_by_kind() {
        let attachments = vec![
            ComposerAttachment {
                path: "/tmp/snap.png".to_owned(),
                file_name: "snap.png".to_owned(),
                kind: ComposerAttachmentKind::Image,
            },
            ComposerAttachment {
                path: "/tmp/readme.pdf".to_owned(),
                file_name: "readme.pdf".to_owned(),
                kind: ComposerAttachmentKind::File,
            },
            ComposerAttachment {
                path: "/tmp/note.mp3".to_owned(),
                file_name: "note.mp3".to_owned(),
                kind: ComposerAttachmentKind::Audio,
            },
            ComposerAttachment {
                path: "/tmp/demo.mp4".to_owned(),
                file_name: "demo.mp4".to_owned(),
                kind: ComposerAttachmentKind::Video,
            },
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
