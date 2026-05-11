use crate::app::{
    conversation::{ItemView, TimelineEntry},
    root::PioneerDesktop,
};
use chrono::{Local, TimeZone};
use gpui::{prelude::*, *};
use gpui_component::{Icon, clipboard::Clipboard, h_flex, theme::ActiveTheme, v_flex};
use pioneer_protocol::{ArtifactRef, TurnItem, UserMessageAttachment};
use std::path::{Path, PathBuf};

#[derive(Clone)]
struct ParsedUserAttachment {
    display_name: String,
    artifact: Option<ArtifactRef>,
}

impl PioneerDesktop {
    pub(super) fn render_item_user_message(
        &self,
        entry: &TimelineEntry,
        item_view: &ItemView,
        item: &TurnItem,
        is_first_row: bool,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let (raw_text, attachments) = match item {
            TurnItem::UserMessage {
                text, attachments, ..
            } => (text.as_str(), parse_user_attachments(attachments)),
            _ => (Self::timeline_entry_text(item_view), Vec::new()),
        };

        let timestamp_text = item_view
            .started_at_unix_ms
            .or(item_view.updated_at_unix_ms)
            .or(item_view.completed_at_unix_ms)
            .and_then(|ts| Local.timestamp_millis_opt(ts).single())
            .map(|dt| dt.format("%d.%m.%Y %H:%M").to_string())
            .unwrap_or_default();

        let copy_text = raw_text.to_owned();
        let active_workspace_id = self
            .current_active_thread_id()
            .and_then(|thread_id| self.thread_workspace_id(thread_id))
            .map(str::to_owned);

        let mut row = div().flex().w_full().justify_center();

        if is_first_row {
            row = row.pt(px(40.));
        } else {
            row = row.pt(px(30.));
        }

        if is_last_row {
            row = row.pb(px(10.));
        }

        row.child(
            v_flex()
                .w(content_width)
                .px_6()
                .items_end()
                .max_w_3_4()
                .group(format!("user-message-{}", item_view.id))
                .when(!attachments.is_empty(), |this| {
                    this.child(self.render_user_message_attachment_badges(
                        attachments.clone(),
                        active_workspace_id.clone(),
                        cx,
                    ))
                })
                .child(
                    div()
                        .min_w_0()
                        .overflow_hidden()
                        .bg(cx.theme().muted)
                        .rounded_2xl()
                        .p_4()
                        .child(v_flex().when(!raw_text.trim().is_empty(), |this| {
                            this.child(self.render_markdown_auto(
                                raw_text,
                                item_view.partial_markdown.as_ref(),
                                cx,
                            ))
                        })),
                )
                .child(
                    h_flex()
                        .h(px(30.))
                        .justify_end()
                        .items_center()
                        .gap_2()
                        .text_xs()
                        .opacity(0.0)
                        .group_hover(format!("user-message-{}", item_view.id), |this| {
                            this.opacity(0.6)
                        })
                        .child(timestamp_text)
                        .child(
                            Clipboard::new(("copy-user-message", entry.item_index))
                                .value(copy_text),
                        ),
                ),
        )
        .into_any_element()
    }

    fn render_user_message_attachment_badges(
        &self,
        attachments: Vec<ParsedUserAttachment>,
        workspace_id: Option<String>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        h_flex()
            .min_w_0()
            .max_w_3_4()
            .justify_end()
            .items_center()
            .flex_wrap()
            .gap_1p5()
            .pb_2()
            .children(
                attachments
                    .into_iter()
                    .enumerate()
                    .map(|(chip_index, attachment)| {
                        let artifact = attachment.artifact.clone();
                        let preview_image_path = artifact.as_ref().and_then(|artifact| {
                            if let Some(workspace_id) = workspace_id.as_deref() {
                                self.request_thread_artifact_preview_load(
                                    workspace_id,
                                    artifact,
                                    cx,
                                );
                            }
                            self.thread_artifacts
                                .preview_square_image_path(artifact)
                                .map(PathBuf::from)
                        });
                        let artifact_id = artifact
                            .as_ref()
                            .map(|artifact| artifact.artifact_id.clone());

                        h_flex()
                            .id(("timeline-user-attachment-chip", chip_index))
                            .h(px(32.))
                            .max_w(px(196.))
                            .min_w_0()
                            .flex_initial()
                            .pl_1()
                            .pr_2()
                            .rounded_full()
                            .border_1()
                            .border_color(cx.theme().border)
                            .items_center()
                            .gap_2()
                            .child(
                                self.render_user_message_attachment_preview(preview_image_path, cx),
                            )
                            .child(
                                div()
                                    .flex_1()
                                    .min_w_0()
                                    .overflow_hidden()
                                    .whitespace_nowrap()
                                    .text_ellipsis()
                                    .text_xs()
                                    .child(attachment.display_name),
                            )
                            .when_some(artifact_id, |this, artifact_id| {
                                this.hover(|this| this.opacity(0.8)).on_click(cx.listener(
                                    move |view, _, _, cx| {
                                        view.open_thread_artifact_in_sidebar(
                                            artifact_id.clone(),
                                            cx,
                                        );
                                    },
                                ))
                            })
                    }),
            )
            .into_any_element()
    }

    fn render_user_message_attachment_preview(
        &self,
        preview_image_path: Option<PathBuf>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if let Some(image_path) = preview_image_path {
            div()
                .size(px(22.))
                .flex_none()
                .relative()
                .overflow_hidden()
                .rounded_full()
                .bg(cx.theme().muted)
                .child(
                    img(image_path)
                        .absolute()
                        .top_0()
                        .left_0()
                        .right_0()
                        .bottom_0()
                        .w_full()
                        .h_full()
                        .rounded_full()
                        .object_fit(ObjectFit::Fill)
                        .with_fallback(move || {
                            div()
                                .size(px(22.))
                                .flex()
                                .items_center()
                                .justify_center()
                                .child(
                                    Icon::new(gpui_component::IconName::File)
                                        .size_3()
                                        .opacity(0.8),
                                )
                                .into_any_element()
                        }),
                )
                .into_any_element()
        } else {
            div()
                .size(px(22.))
                .flex_none()
                .flex()
                .items_center()
                .justify_center()
                .child(
                    Icon::new(gpui_component::IconName::File)
                        .size_3()
                        .opacity(0.8),
                )
                .into_any_element()
        }
    }
}

fn parse_user_attachments(attachments: &[UserMessageAttachment]) -> Vec<ParsedUserAttachment> {
    attachments
        .iter()
        .map(|attachment| ParsedUserAttachment {
            display_name: display_name_from_attachment(attachment),
            artifact: artifact_from_attachment(attachment),
        })
        .collect()
}

fn display_name_from_attachment(attachment: &UserMessageAttachment) -> String {
    let source = match attachment {
        UserMessageAttachment::Image { url }
        | UserMessageAttachment::File { url }
        | UserMessageAttachment::Audio { url }
        | UserMessageAttachment::Video { url } => url.as_str(),
        UserMessageAttachment::LocalImage { path }
        | UserMessageAttachment::LocalFile { path }
        | UserMessageAttachment::LocalAudio { path }
        | UserMessageAttachment::LocalVideo { path } => path.as_str(),
        UserMessageAttachment::Artifact { artifact } => return artifact.display_name.clone(),
    };

    if source.contains("://") || source.starts_with("data:") {
        let without_query = source.split_once('?').map_or(source, |(value, _)| value);
        let without_fragment = without_query
            .split_once('#')
            .map_or(without_query, |(value, _)| value);
        let candidate = without_fragment
            .rsplit('/')
            .next()
            .unwrap_or(without_fragment);
        if candidate.is_empty() {
            source.to_owned()
        } else {
            candidate.to_owned()
        }
    } else {
        Path::new(source)
            .file_name()
            .and_then(|value| value.to_str())
            .map(str::to_owned)
            .unwrap_or_else(|| source.to_owned())
    }
}

fn artifact_from_attachment(attachment: &UserMessageAttachment) -> Option<ArtifactRef> {
    match attachment {
        UserMessageAttachment::Artifact { artifact } => Some(artifact.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{artifact_from_attachment, display_name_from_attachment};
    use pioneer_protocol::{ArtifactKind, ArtifactRef, ArtifactStatus, UserMessageAttachment};

    #[test]
    fn display_name_uses_local_file_name() {
        assert_eq!(
            display_name_from_attachment(&UserMessageAttachment::LocalFile {
                path: "/tmp/report.pdf".to_owned(),
            }),
            "report.pdf"
        );
    }

    #[test]
    fn display_name_uses_url_tail_segment() {
        assert_eq!(
            display_name_from_attachment(&UserMessageAttachment::File {
                url: "https://example.com/path/to/file.mov?token=1".to_owned(),
            }),
            "file.mov"
        );
    }

    #[test]
    fn artifact_attachment_uses_artifact_display_name_and_detail() {
        let attachment = UserMessageAttachment::Artifact {
            artifact: ArtifactRef {
                artifact_id: "art_1".to_owned(),
                version_id: Some("av_1".to_owned()),
                display_name: "report.pdf".to_owned(),
                kind: ArtifactKind::Pdf,
                mime_type: Some("application/pdf".to_owned()),
                size_bytes: Some(2048),
                sha256: None,
                status: ArtifactStatus::Ready,
                preview: None,
            },
        };

        assert_eq!(display_name_from_attachment(&attachment), "report.pdf");
        assert_eq!(
            artifact_from_attachment(&attachment)
                .as_ref()
                .map(|artifact| artifact.artifact_id.as_str()),
            Some("art_1")
        );
    }
}
