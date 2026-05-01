use super::{format_elapsed, format_elapsed_ms, host_from_url, now_unix_ms};
use crate::app::{
    conversation::{ItemView, TimelineEntry, TimelineEntryStatus},
    root::PioneerDesktop,
};
use gpui::{prelude::*, *};
use gpui_component::{collapsible::Collapsible, h_flex, spinner::Spinner, v_flex, *};
use pioneer_protocol::TurnItem;
use serde_json::Value as JsonValue;
use std::hash::{Hash, Hasher};

fn download_url_from_arguments(arguments: &JsonValue) -> Option<String> {
    arguments
        .get("url")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(str::to_owned)
}

fn final_download_status(
    status: TimelineEntryStatus,
    success: Option<bool>,
    status_code: Option<u16>,
) -> (String, bool) {
    match status {
        TimelineEntryStatus::Cancelled => (t!("timeline.download.cancelled").to_string(), false),
        TimelineEntryStatus::Failed => (t!("timeline.download.failed").to_string(), false),
        TimelineEntryStatus::Running => (t!("timeline.download.running").to_string(), true),
        TimelineEntryStatus::Completed => {
            if matches!(success, Some(false)) || status_code.is_some_and(|code| code >= 400) {
                (t!("timeline.download.failed").to_string(), false)
            } else {
                (t!("timeline.download.completed").to_string(), true)
            }
        }
    }
}

fn byte_unit_label(unit_idx: usize) -> String {
    match unit_idx {
        0 => t!("timeline.download.unit_b").to_string(),
        1 => t!("timeline.download.unit_kb").to_string(),
        2 => t!("timeline.download.unit_mb").to_string(),
        3 => t!("timeline.download.unit_gb").to_string(),
        _ => t!("timeline.download.unit_tb").to_string(),
    }
}

fn format_bytes_human(bytes: u64) -> String {
    let mut value = bytes as f64;
    let mut unit_idx = 0usize;
    while value >= 1024.0 && unit_idx < 4 {
        value /= 1024.0;
        unit_idx += 1;
    }
    let unit = byte_unit_label(unit_idx);

    if unit_idx == 0 {
        format!("{bytes} {unit}")
    } else if value.fract() < 0.05 {
        format!("{value:.0} {unit}")
    } else {
        format!("{value:.1} {unit}")
    }
}

impl PioneerDesktop {
    pub(super) fn render_item_download(
        &self,
        entry: &TimelineEntry,
        item_view: &ItemView,
        item: &TurnItem,
        is_first_row: bool,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let (display_url, status_code, success, bytes_written) = match item {
            TurnItem::Download {
                arguments,
                url,
                final_url,
                status_code,
                success,
                bytes_written,
                ..
            } => (
                final_url
                    .as_ref()
                    .or(url.as_ref())
                    .cloned()
                    .or_else(|| download_url_from_arguments(arguments)),
                *status_code,
                *success,
                *bytes_written,
            ),
            _ => (None, None, None, None),
        };

        let requested_url = display_url
            .clone()
            .unwrap_or_else(|| t!("timeline.common.url_missing").to_string());
        let host_label =
            host_from_url(requested_url.as_str()).unwrap_or_else(|| requested_url.clone());

        let favicon_url = self.timeline_favicon_url(None, requested_url.as_str());
        let host_with_favicon_running =
            self.timeline_host_with_favicon(host_label.as_str(), favicon_url.clone(), cx);
        let host_with_favicon_collapsed =
            self.timeline_host_with_favicon(host_label.as_str(), favicon_url.clone(), cx);

        let elapsed_label = format_elapsed(item_view);
        let running_elapsed_label = item_view
            .started_at_unix_ms
            .map(|started| format_elapsed_ms(now_unix_ms().saturating_sub(started) as u64));

        let open = self
            .thread_timeline_item_expanded
            .borrow()
            .contains(entry.id.as_str());

        let entry_id = entry.id.clone();
        let mut toggle_id_hasher = std::collections::hash_map::DefaultHasher::new();
        entry.id.hash(&mut toggle_id_hasher);
        let toggle_id = toggle_id_hasher.finish();

        let (final_status, is_successful) =
            final_download_status(item_view.status, success, status_code);

        let content = if item_view.status == TimelineEntryStatus::Running {
            v_flex()
                .w_full()
                .gap_3()
                .child(host_with_favicon_running)
                .child(
                    h_flex()
                        .w_full()
                        .items_center()
                        .justify_between()
                        .text_sm()
                        .font_semibold()
                        .child(
                            h_flex()
                                .items_center()
                                .gap_2()
                                .child(Spinner::new().icon(IconName::Loader))
                                .child(t!("timeline.download.running").to_string()),
                        )
                        .child(
                            h_flex()
                                .items_center()
                                .gap_2()
                                .when_some(running_elapsed_label, |this, elapsed| {
                                    this.child(elapsed)
                                }),
                        ),
                )
                .into_any_element()
        } else {
            let full_url_row = if display_url.is_none() {
                div()
                    .w_full()
                    .text_sm()
                    .opacity(0.75)
                    .line_height(relative(1.45))
                    .child(requested_url.clone())
                    .into_any_element()
            } else {
                div()
                    .id(("download-link", toggle_id))
                    .w_full()
                    .text_sm()
                    .line_height(relative(1.45))
                    .text_color(cx.theme().link)
                    .underline()
                    .hover({
                        let link_hover = cx.theme().link_hover;
                        move |this| this.text_color(link_hover)
                    })
                    .child(Self::truncate_for_card(requested_url.as_str(), 300))
                    .on_click({
                        let url = requested_url.clone();
                        cx.listener(move |_, _, _, cx| {
                            cx.open_url(url.as_str());
                        })
                    })
                    .into_any_element()
            };

            let collapsed_content = v_flex()
                .w_full()
                .gap_2()
                .pt_1()
                .child(full_url_row)
                .child(
                    h_flex()
                        .items_center()
                        .gap_2()
                        .text_sm()
                        .child(
                            Icon::new(if is_successful {
                                IconName::Check
                            } else {
                                IconName::TriangleAlert
                            })
                            .size_3p5(),
                        )
                        .child(final_status),
                )
                .when_some(bytes_written, |this, bytes| {
                    this.child(div().text_sm().opacity(0.8).child(
                        t!("timeline.download.size", size = format_bytes_human(bytes)).to_string(),
                    ))
                })
                .into_any_element();

            Collapsible::new()
                .gap_2()
                .open(open)
                .child(
                    div()
                        .id(("download-toggle", toggle_id))
                        .w_full()
                        .flex()
                        .items_center()
                        .opacity(0.7)
                        .hover(|this| this.opacity(0.9))
                        .child(
                            h_flex()
                                .w_full()
                                .items_center()
                                .justify_between()
                                .gap_3()
                                .child(host_with_favicon_collapsed)
                                .child(
                                    h_flex()
                                        .items_center()
                                        .gap_2()
                                        .text_sm()
                                        .when_some(elapsed_label, |this, elapsed| {
                                            this.child(elapsed)
                                        })
                                        .child(
                                            Icon::new(if open {
                                                IconName::ChevronUp
                                            } else {
                                                IconName::ChevronDown
                                            })
                                            .size_4(),
                                        ),
                                ),
                        )
                        .on_click({
                            let entry_id = entry_id.clone();
                            cx.listener(move |this, _, _, cx| {
                                this.toggle_timeline_item_expanded(entry_id.as_str(), cx);
                            })
                        }),
                )
                .content(collapsed_content)
                .into_any_element()
        };

        self.render_item_row(is_first_row, is_last_row, content_width, content)
    }
}
