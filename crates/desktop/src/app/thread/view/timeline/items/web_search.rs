use super::super::TimelineRowTopSpacing;
use super::{format_running_elapsed, host_from_url};
use crate::app::{
    conversation::{ItemView, TimelineEntry, TimelineEntryStatus},
    root::PioneerDesktop,
};
use gpui_kit::component::{collapsible::Collapsible, h_flex, spinner::Spinner, v_flex, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::timeline::labels::web_search_display_query;
use pioneer_protocol::{TurnItem, WebSearchResultItem};
use std::hash::{Hash, Hasher};

fn results_count_label(count: usize) -> String {
    t!("timeline.web_search.results_count", count = count).to_string()
}

impl PioneerDesktop {
    pub(super) fn render_item_web_search(
        &self,
        entry: &TimelineEntry,
        item_view: &ItemView,
        item: &TurnItem,
        top_spacing: TimelineRowTopSpacing,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let (search_query, result_count, results) = match item {
            TurnItem::WebSearch {
                arguments,
                query,
                result_count,
                results,
                ..
            } => (
                web_search_display_query(arguments, query.as_deref())
                    .unwrap_or_else(|| t!("timeline.web_search.fallback_query").to_string()),
                result_count.unwrap_or(results.len()),
                results.clone(),
            ),
            _ => (
                t!("timeline.web_search.fallback_query").to_string(),
                0,
                Vec::new(),
            ),
        };
        let is_running = item_view.status == TimelineEntryStatus::Running;

        let query_with_icon = || {
            h_flex()
                .w_full()
                .items_center()
                .gap_2()
                .when(is_running, |this| {
                    this.child(Spinner::new().icon(IconName::Loader))
                })
                .when(!is_running, |this| {
                    this.child(Icon::new(IconName::Search).size_4().opacity(0.8))
                })
                .child(
                    div()
                        .flex_1()
                        .min_w_0()
                        .overflow_hidden()
                        .text_ellipsis()
                        .text_sm()
                        .line_height(relative(1.45))
                        .child(search_query.clone()),
                )
                .into_any_element()
        };

        let running_elapsed_label = format_running_elapsed(item_view);

        let open = self
            .thread_timeline_item_expanded
            .borrow()
            .contains(entry.id.as_str());

        let entry_id = entry.id.clone();
        let mut toggle_id_hasher = std::collections::hash_map::DefaultHasher::new();
        entry.id.hash(&mut toggle_id_hasher);
        let toggle_id = toggle_id_hasher.finish();

        let result_rows = if results.is_empty() {
            v_flex()
                .w_full()
                .gap_2()
                .pt_1()
                .child(
                    div()
                        .text_sm()
                        .opacity(0.75)
                        .child(t!("timeline.web_search.no_results").to_string()),
                )
                .into_any_element()
        } else {
            let mut list = v_flex()
                .w_full()
                .gap_2()
                .rounded_lg()
                .border_1()
                .border_color(cx.theme().border)
                .p_1();

            for (index, result) in results.iter().enumerate() {
                list = list.child(self.web_search_result_row(result, toggle_id, index, cx));
            }

            v_flex().w_full().pt_1().child(list).into_any_element()
        };

        let content = if is_running {
            Collapsible::new()
                .gap_2()
                .open(open)
                .child(
                    div()
                        .id(("web-search-toggle", toggle_id))
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
                                .child(query_with_icon())
                                .child(
                                    h_flex()
                                        .items_center()
                                        .gap_2()
                                        .text_sm()
                                        .font_semibold()
                                        .child(t!("timeline.web_search.running").to_string())
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
                .content(result_rows)
                .into_any_element()
        } else {
            Collapsible::new()
                .gap_2()
                .open(open)
                .child(
                    div()
                        .id(("web-search-toggle", toggle_id))
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
                                .child(query_with_icon())
                                .child(
                                    h_flex()
                                        .items_center()
                                        .gap_2()
                                        .text_sm()
                                        .child(results_count_label(result_count))
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
                .content(result_rows)
                .into_any_element()
        };

        self.render_item_row(top_spacing, is_last_row, content_width, content)
    }

    fn web_search_result_row(
        &self,
        result: &WebSearchResultItem,
        toggle_id: u64,
        index: usize,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let host = host_from_url(result.url.as_str()).unwrap_or_else(|| result.url.clone());
        let favicon_url = self.timeline_favicon_url(None, result.url.as_str());
        let row_id = toggle_id.wrapping_mul(1_000_003).wrapping_add(index as u64);

        div()
            .id(("web-search-result-link", row_id))
            .w_full()
            .flex()
            .items_center()
            .justify_between()
            .gap_3()
            .px_2()
            .py_1()
            .rounded_md()
            .hover(|this| this.bg(cx.theme().secondary))
            .child(
                h_flex()
                    .flex_1()
                    .min_w_0()
                    .items_center()
                    .gap_2()
                    .child(self.timeline_favicon_icon(favicon_url, px(14.0), cx))
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .overflow_hidden()
                            .text_ellipsis()
                            .text_sm()
                            .line_height(relative(1.35))
                            .child(result.title.clone()),
                    ),
            )
            .child(div().flex_none().text_sm().opacity(0.6).child(host))
            .on_click({
                let url = result.url.clone();
                cx.listener(move |_, _, _, cx| {
                    cx.open_url(url.as_str());
                })
            })
            .into_any_element()
    }
}
