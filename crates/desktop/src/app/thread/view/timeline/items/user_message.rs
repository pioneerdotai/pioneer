use crate::app::{
    conversation::{ItemView, TimelineEntry},
    root::PioneerDesktop,
};
use chrono::{Local, TimeZone};
use gpui::{prelude::*, *};
use gpui_component::{Icon, clipboard::Clipboard, h_flex, theme::ActiveTheme, v_flex};
use pioneer_protocol::{TurnItem, UserMessageAttachment};
use std::path::Path;

#[derive(Clone)]
struct ParsedUserAttachment {
    display_name: String,
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
                .child(
                    div()
                        .max_w_3_4()
                        .bg(cx.theme().muted)
                        .rounded_2xl()
                        .p_4()
                        .child(
                            v_flex()
                                .w_full()
                                .gap_2()
                                .when(!attachments.is_empty(), |this| {
                                    this.child(self.render_user_message_attachment_badges(
                                        attachments.clone(),
                                        cx,
                                    ))
                                })
                                .when(!raw_text.trim().is_empty(), |this| {
                                    this.child(self.render_markdown_auto(
                                        raw_text,
                                        item_view.partial_markdown.as_ref(),
                                        cx,
                                    ))
                                }),
                        ),
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
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let rows = attachments
            .chunks(4)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>();

        v_flex()
            .w_full()
            .gap_1p5()
            .children(rows.into_iter().enumerate().map(|(row_index, row)| {
                h_flex()
                    .id(("timeline-user-attachment-row", row_index))
                    .w_full()
                    .gap_2()
                    .children(
                        row.into_iter()
                            .enumerate()
                            .map(|(column_index, attachment)| {
                                let chip_index = row_index * 4 + column_index;
                                h_flex()
                                    .id(("timeline-user-attachment-chip", chip_index))
                                    .h(px(30.))
                                    .max_w(px(196.))
                                    .px_2()
                                    .rounded_full()
                                    .border_1()
                                    .border_color(cx.theme().border)
                                    .items_center()
                                    .gap_2()
                                    .child(
                                        div()
                                            .size(px(18.))
                                            .rounded_full()
                                            .bg(cx.theme().background.opacity(0.85))
                                            .flex()
                                            .items_center()
                                            .justify_center()
                                            .child(
                                                Icon::new(gpui_component::IconName::File)
                                                    .size_3()
                                                    .opacity(0.8),
                                            ),
                                    )
                                    .child(
                                        div()
                                            .flex_1()
                                            .overflow_hidden()
                                            .whitespace_nowrap()
                                            .text_ellipsis()
                                            .text_sm()
                                            .child(attachment.display_name),
                                    )
                            }),
                    )
            }))
            .into_any_element()
    }
}

fn parse_user_attachments(attachments: &[UserMessageAttachment]) -> Vec<ParsedUserAttachment> {
    attachments
        .iter()
        .map(|attachment| ParsedUserAttachment {
            display_name: display_name_from_attachment(attachment),
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

#[cfg(test)]
mod tests {
    use super::display_name_from_attachment;
    use pioneer_protocol::UserMessageAttachment;

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
}
