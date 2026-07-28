use super::{format_running_elapsed, host_from_url};
use crate::app::{
    conversation::{ItemView, TimelineEntry, TimelineEntryStatus},
    root::PioneerDesktop,
};
use gpui::{prelude::*, *};
use gpui_component::{collapsible::Collapsible, h_flex, spinner::Spinner, v_flex, *};
use pioneer_client::timeline::labels::{
    TimelineFinalStatusKind, final_web_fetch_status, web_fetch_display_url,
};
use pioneer_protocol::TurnItem;
use std::hash::{Hash, Hasher};

impl PioneerDesktop {
    pub(super) fn render_item_web_fetch(
        &self,
        entry: &TimelineEntry,
        item_view: &ItemView,
        item: &TurnItem,
        is_first_row: bool,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let (display_url, status_code, success, meta_favicon) = match item {
            TurnItem::WebFetch {
                arguments,
                url,
                final_url,
                status_code,
                success,
                ..
            } => (
                web_fetch_display_url(arguments, url.as_deref(), final_url.as_deref()),
                *status_code,
                *success,
                None,
            ),
            _ => (None, None, None, None),
        };

        let requested_url = display_url
            .clone()
            .unwrap_or_else(|| t!("timeline.common.url_missing").to_string());
        let host_label =
            host_from_url(requested_url.as_str()).unwrap_or_else(|| requested_url.clone());
        let is_running = item_view.status == TimelineEntryStatus::Running;

        let favicon_url = self.timeline_favicon_url(meta_favicon, requested_url.as_str());
        let host_with_loader = h_flex()
            .w_full()
            .items_center()
            .gap_2()
            .child(Spinner::new().icon(IconName::Loader))
            .child(
                div()
                    .text_sm()
                    .opacity(0.9)
                    .line_height(relative(1.45))
                    .child(Self::truncate_for_card(host_label.as_str(), 160)),
            )
            .into_any_element();
        let host_with_favicon_collapsed =
            self.timeline_host_with_favicon(host_label.as_str(), favicon_url.clone(), cx);

        let running_elapsed_label = format_running_elapsed(item_view);

        let open = self
            .thread_timeline_item_expanded
            .borrow()
            .contains(entry.id.as_str());

        let entry_id = entry.id.clone();
        let mut toggle_id_hasher = std::collections::hash_map::DefaultHasher::new();
        entry.id.hash(&mut toggle_id_hasher);
        let toggle_id = toggle_id_hasher.finish();

        let status = final_web_fetch_status(item_view.status, success, status_code);
        let final_status = web_fetch_status_label(status.kind);
        let is_successful = status.successful;

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
                .id(("web-fetch-link", toggle_id))
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

        let details = v_flex()
            .w_full()
            .gap_2()
            .pt_1()
            .child(full_url_row)
            .child(
                h_flex()
                    .items_center()
                    .gap_2()
                    .text_sm()
                    .when(!is_running, |this| {
                        this.child(
                            Icon::new(if is_successful {
                                IconName::Check
                            } else {
                                IconName::TriangleAlert
                            })
                            .size_3p5(),
                        )
                    })
                    .child(final_status),
            )
            .into_any_element();

        let content = if is_running {
            Collapsible::new()
                .gap_2()
                .open(open)
                .child(
                    div()
                        .id(("web-fetch-toggle", toggle_id))
                        .w_full()
                        .flex()
                        .items_center()
                        .hover(|this| this.opacity(0.9))
                        .child(
                            h_flex()
                                .w_full()
                                .items_center()
                                .justify_between()
                                .gap_3()
                                .child(host_with_loader)
                                .child(
                                    h_flex()
                                        .items_center()
                                        .gap_2()
                                        .text_sm()
                                        .font_semibold()
                                        .child(t!("timeline.web_fetch.running").to_string())
                                        .when_some(running_elapsed_label, |this, elapsed| {
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
                .content(details)
                .into_any_element()
        } else {
            Collapsible::new()
                .gap_2()
                .open(open)
                .child(
                    div()
                        .id(("web-fetch-toggle", toggle_id))
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
                                    h_flex().items_center().gap_2().text_sm().child(
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
                .content(details)
                .into_any_element()
        };

        self.render_item_row(is_first_row, is_last_row, content_width, content)
    }
}

fn web_fetch_status_label(kind: TimelineFinalStatusKind) -> String {
    match kind {
        TimelineFinalStatusKind::Cancelled => t!("timeline.web_fetch.cancelled").to_string(),
        TimelineFinalStatusKind::Blocked => t!("timeline.web_fetch.blocked").to_string(),
        TimelineFinalStatusKind::Failed => t!("timeline.web_fetch.failed").to_string(),
        TimelineFinalStatusKind::Running => t!("timeline.web_fetch.running").to_string(),
        TimelineFinalStatusKind::Completed => t!("timeline.web_fetch.completed").to_string(),
    }
}
