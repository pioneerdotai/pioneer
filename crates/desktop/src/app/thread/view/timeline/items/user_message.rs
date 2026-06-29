use crate::app::{
    conversation::{ItemView, TimelineEntry},
    root::PioneerDesktop,
};
use crate::assets::PioneerIconName;
use chrono::{Local, TimeZone};
use gpui::{prelude::*, *};
use gpui_component::{Icon, clipboard::Clipboard, h_flex, theme::ActiveTheme, v_flex};
use pioneer_client::timeline::labels::{
    ParsedUserAttachment, ParsedUserAttachmentKind, parse_user_attachments,
    stable_user_message_attachment_chip_id,
};
use pioneer_protocol::{TurnItem, TurnPermissionProfileSnapshot};
use std::path::PathBuf;

impl PioneerDesktop {
    pub(super) fn render_item_user_message(
        &self,
        entry: &TimelineEntry,
        item_view: &ItemView,
        item: &TurnItem,
        permission_profile: Option<&TurnPermissionProfileSnapshot>,
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
                .group(format!("user-message-{}", item_view.id))
                .when(!attachments.is_empty(), |this| {
                    this.child(self.render_user_message_attachment_badges(
                        item_view.id.as_str(),
                        attachments.clone(),
                        active_workspace_id.clone(),
                        cx,
                    ))
                })
                .child(
                    div()
                        .max_w_3_4()
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
                .when_some(permission_profile, |this, permission_profile| {
                    this.child(
                        h_flex()
                            .pt_2()
                            .justify_end()
                            .child(self.render_turn_permission_badge(permission_profile, cx)),
                    )
                })
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
        item_id: &str,
        attachments: Vec<ParsedUserAttachment>,
        workspace_id: Option<String>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let item_id = item_id.to_owned();
        h_flex()
            .w_full()
            .min_w_0()
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
                        let chip_id =
                            stable_user_message_attachment_chip_id(item_id.as_str(), chip_index);
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
                            .id(("timeline-user-attachment-chip", chip_id))
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
                            .child(self.render_user_message_attachment_preview(
                                preview_image_path,
                                attachment.kind,
                                cx,
                            ))
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
        kind: ParsedUserAttachmentKind,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if matches!(
            kind,
            ParsedUserAttachmentKind::Skill | ParsedUserAttachmentKind::Mcp
        ) {
            return attachment_capability_icon(kind, cx).into_any_element();
        }

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
                        .with_fallback(move || attachment_file_icon(false).into_any_element()),
                )
                .into_any_element()
        } else {
            attachment_file_icon(true).into_any_element()
        }
    }
}

fn attachment_file_icon(flex_none: bool) -> gpui::Div {
    let mut container = div().size(px(22.)).flex().items_center().justify_center();
    if flex_none {
        container = container.flex_none();
    }

    container.child(
        Icon::new(gpui_component::IconName::File)
            .size_3()
            .opacity(0.8),
    )
}

fn attachment_capability_icon(
    kind: ParsedUserAttachmentKind,
    cx: &mut Context<PioneerDesktop>,
) -> gpui::Div {
    let icon = match kind {
        ParsedUserAttachmentKind::Skill => PioneerIconName::Zap,
        ParsedUserAttachmentKind::Mcp => PioneerIconName::Mcp,
        ParsedUserAttachmentKind::File => PioneerIconName::Paperclip,
    };

    div()
        .flex_none()
        .size(px(20.))
        .rounded_full()
        .flex()
        .items_center()
        .justify_center()
        .child(
            Icon::new(icon)
                .size_3()
                .opacity(0.8)
                .text_color(cx.theme().foreground),
        )
}
