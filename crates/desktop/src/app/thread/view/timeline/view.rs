use super::{
    TimelinePendingRequestRow, TimelineRenderRow,
    items::format_elapsed_ms,
    model::{
        TimelineCoalescedToolsKind, TimelineCoalescedToolsRow, TimelineRow, TimelineRowKind,
        TurnWorkGroupRow, build_timeline_rows, timeline_rows_layout_hash,
    },
};
use crate::app::{
    conversation::ConversationViewState,
    root::{CLIRuntimePendingRequestEntry, PioneerDesktop},
};
use gpui::{prelude::*, *};
use gpui_component::{Icon, IconName, h_flex, scroll::Scrollbar, v_flex, v_virtual_list};
use std::{
    hash::{Hash, Hasher},
    rc::Rc,
};

impl PioneerDesktop {
    pub(crate) fn render_timeline(
        &self,
        active_thread_id: Option<&str>,
        projection: &ConversationViewState,
        pending_cli_runtime_requests: Vec<CLIRuntimePendingRequestEntry>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        self.sync_timeline_layout_width(cx);

        if projection.timeline.is_empty() && pending_cli_runtime_requests.is_empty() {
            return v_flex()
                .w_full()
                .h_full()
                .justify_center()
                .items_center()
                .text_sm()
                .opacity(0.6)
                .child(t!("timeline.empty.start_thread").to_string())
                .into_any_element();
        }

        let list_width = self.timeline_content_width(window);
        let content_width = self.timeline_entry_content_width(list_width);
        let expanded_revision = self.thread_timeline_view_state.borrow().expanded_revision;
        let model_signature_hash = Self::timeline_model_signature_hash(
            active_thread_id,
            projection.revision,
            expanded_revision,
        );

        let (rows, rows_layout_hash) = {
            let mut state = self.thread_timeline_view_state.borrow_mut();
            if state.cached_model_signature_hash == model_signature_hash
                && let Some(rows) = state.cached_model_rows.as_ref()
            {
                (rows.clone(), state.cached_model_rows_layout_hash)
            } else {
                let expanded = self.thread_timeline_item_expanded.borrow().clone();
                let rows = Rc::new(build_timeline_rows(projection, &expanded));
                let rows_layout_hash =
                    timeline_rows_layout_hash(projection, rows.as_ref(), &expanded);

                state.cached_model_signature_hash = model_signature_hash;
                state.cached_model_rows_layout_hash = rows_layout_hash;
                state.cached_model_rows = Some(rows.clone());

                (rows, rows_layout_hash)
            }
        };

        let rows = self.hydrate_running_turn_rows(rows, cx);
        let rows = Rc::new(timeline_render_rows(rows, pending_cli_runtime_requests));
        let rows_layout_hash = rows_layout_hash ^ timeline_render_rows_layout_hash(rows.as_ref());

        self.sync_timeline_scroll(active_thread_id, projection, rows.as_ref());

        if rows.is_empty() {
            return div().w_full().h_full().into_any_element();
        }

        let width_px = (list_width / px(1.)).round() as i32;
        let tail_row_key = rows.last().map(|row| row.key());

        let item_sizes = {
            let mut state = self.thread_timeline_view_state.borrow_mut();

            let can_reuse = state.cached_render_active_thread_id.as_deref() == active_thread_id
                && state.cached_render_width_px == width_px
                && state.cached_render_item_count == rows.len()
                && state.cached_render_model_layout_hash == rows_layout_hash;

            if can_reuse && let Some(sizes) = state.cached_item_sizes.as_ref() {
                sizes.clone()
            } else {
                let expanded = self.thread_timeline_item_expanded.borrow().clone();
                let item_sizes = self.compute_timeline_item_sizes(
                    &mut state,
                    projection,
                    rows.as_ref(),
                    list_width,
                    content_width,
                    &expanded,
                    window,
                    cx,
                );

                state.cached_render_active_thread_id = active_thread_id.map(str::to_owned);
                state.cached_render_width_px = width_px;
                state.cached_render_item_count = rows.len();
                state.cached_render_tail_entry_id = tail_row_key.map(str::to_owned);
                state.cached_render_tail_layout_hash = rows_layout_hash;
                state.cached_render_model_layout_hash = rows_layout_hash;
                state.cached_item_sizes = Some(item_sizes.clone());

                item_sizes
            }
        };

        let render_thread_id = active_thread_id.map(str::to_owned);
        let render_rows = rows.clone();
        let render_row_count = render_rows.len();

        div()
            .w_full()
            .h_full()
            .relative()
            .overflow_hidden()
            .child(
                v_virtual_list(
                    cx.entity(),
                    "thread-timeline-virtual-list",
                    item_sizes,
                    move |view, visible_range, _, cx| {
                        let Some(projection) = render_thread_id
                            .as_deref()
                            .and_then(|thread_id| view.thread_conversation(thread_id))
                            .map(|conversation| conversation.projection())
                        else {
                            return Vec::new();
                        };

                        visible_range
                            .filter_map(|ix| {
                                render_rows.get(ix).map(|row| {
                                    view.render_timeline_row(
                                        projection,
                                        row,
                                        ix == 0,
                                        ix + 1 == render_row_count,
                                        content_width,
                                        cx,
                                    )
                                })
                            })
                            .collect::<Vec<_>>()
                    },
                )
                .with_sizing_behavior(ListSizingBehavior::Auto)
                .track_scroll(&self.thread_timeline_scroll_handle),
            )
            .child(
                div()
                    .absolute()
                    .top_0()
                    .left_0()
                    .right_0()
                    .bottom_0()
                    .child(Scrollbar::vertical(&self.thread_timeline_scroll_handle)),
            )
            .into_any_element()
    }

    pub(super) fn render_timeline_row(
        &self,
        projection: &ConversationViewState,
        row: &TimelineRenderRow,
        is_first_row: bool,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        match row {
            TimelineRenderRow::Timeline(TimelineRow {
                kind: TimelineRowKind::Item { timeline_index },
                ..
            }) => {
                let Some(entry) = projection.timeline.get(*timeline_index) else {
                    return div().into_any_element();
                };
                let Some(item_view) = projection.item_for_timeline_entry(entry) else {
                    return div().into_any_element();
                };

                self.render_turn_item_entry(
                    entry,
                    item_view,
                    &item_view.item,
                    is_first_row,
                    is_last_row,
                    content_width,
                    cx,
                )
            }
            TimelineRenderRow::Timeline(TimelineRow {
                kind: TimelineRowKind::TurnWorkToggle(group),
                ..
            }) => self.render_turn_work_group_toggle(
                group,
                is_first_row,
                is_last_row,
                content_width,
                cx,
            ),
            TimelineRenderRow::Timeline(TimelineRow {
                kind: TimelineRowKind::CoalescedTools(group),
                ..
            }) => self.render_coalesced_tools_toggle(
                group,
                is_first_row,
                is_last_row,
                content_width,
                cx,
            ),
            TimelineRenderRow::Timeline(TimelineRow {
                kind: TimelineRowKind::RunningTurn(running_turn),
                ..
            }) => self.render_running_turn_row(
                running_turn,
                is_first_row,
                is_last_row,
                content_width,
                cx,
            ),
            TimelineRenderRow::PendingRequest(row) => {
                let content = self.render_cli_runtime_pending_request_card(row.entry.clone(), cx);
                self.render_item_row(is_first_row, is_last_row, content_width, content)
            }
        }
    }

    fn render_coalesced_tools_toggle(
        &self,
        group: &TimelineCoalescedToolsRow,
        is_first_row: bool,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let mut toggle_hasher = std::collections::hash_map::DefaultHasher::new();
        toggle_hasher.write(group.toggle_key.as_bytes());
        let toggle_id = toggle_hasher.finish();
        let label = coalesced_tools_label(group);

        let toggle = div()
            .id(("timeline-coalesced-tools-toggle", toggle_id))
            .w_full()
            .flex()
            .items_center()
            .opacity(0.6)
            .hover(|this| this.opacity(0.85))
            .child(
                h_flex()
                    .w_full()
                    .items_center()
                    .justify_between()
                    .text_sm()
                    .child(label)
                    .child(
                        Icon::new(if group.is_open {
                            IconName::ChevronUp
                        } else {
                            IconName::ChevronDown
                        })
                        .size_4(),
                    ),
            )
            .on_click({
                let toggle_key = group.toggle_key.clone();
                cx.listener(move |this, _, _, cx| {
                    this.toggle_timeline_item_expanded(toggle_key.as_str(), cx);
                })
            });

        self.render_item_row(
            is_first_row,
            is_last_row,
            content_width,
            toggle.into_any_element(),
        )
    }

    fn render_turn_work_group_toggle(
        &self,
        group: &TurnWorkGroupRow,
        is_first_row: bool,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let elapsed_label = group.elapsed_ms.map(format_elapsed_ms);

        let mut toggle_hasher = std::collections::hash_map::DefaultHasher::new();
        toggle_hasher.write(group.toggle_key.as_bytes());
        let toggle_id = toggle_hasher.finish();

        let toggle = div()
            .id(("turn-work-group-toggle", toggle_id))
            .w_full()
            .flex()
            .items_center()
            .opacity(0.65)
            .hover(|this| this.opacity(0.85))
            .child(
                h_flex()
                    .w_full()
                    .items_center()
                    .justify_between()
                    .text_sm()
                    .child(t!("timeline.work_group.completed").to_string())
                    .child(
                        h_flex()
                            .items_center()
                            .gap_2()
                            .when_some(elapsed_label, |this, elapsed| this.child(elapsed))
                            .child(
                                Icon::new(if group.is_open {
                                    IconName::ChevronUp
                                } else {
                                    IconName::ChevronDown
                                })
                                .size_4(),
                            ),
                    ),
            )
            .on_click({
                let toggle_key = group.toggle_key.clone();
                cx.listener(move |this, _, _, cx| {
                    this.toggle_timeline_item_expanded(toggle_key.as_str(), cx);
                })
            });

        self.render_item_row(
            is_first_row,
            is_last_row,
            content_width,
            toggle.into_any_element(),
        )
    }

    fn timeline_model_signature_hash(
        active_thread_id: Option<&str>,
        projection_revision: u64,
        expanded_revision: u64,
    ) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        active_thread_id.hash(&mut hasher);
        projection_revision.hash(&mut hasher);
        expanded_revision.hash(&mut hasher);
        hasher.finish()
    }
}

fn coalesced_tools_label(group: &TimelineCoalescedToolsRow) -> String {
    match group.kind {
        TimelineCoalescedToolsKind::CompletedTaskTools => t!(
            "timeline.coalesced_tools.completed_task_tools",
            count = group.count
        )
        .to_string(),
        TimelineCoalescedToolsKind::RepeatedTaskWait => t!(
            "timeline.coalesced_tools.repeated_task_wait",
            count = group.count
        )
        .to_string(),
    }
}

fn timeline_render_rows(
    rows: Rc<Vec<TimelineRow>>,
    pending_requests: Vec<CLIRuntimePendingRequestEntry>,
) -> Vec<TimelineRenderRow> {
    if pending_requests.is_empty() {
        return rows
            .iter()
            .cloned()
            .map(TimelineRenderRow::Timeline)
            .collect();
    }

    let running_index = rows
        .iter()
        .position(|row| matches!(row.kind, TimelineRowKind::RunningTurn(_)))
        .unwrap_or(rows.len());

    let mut render_rows = Vec::with_capacity(rows.len() + pending_requests.len());
    render_rows.extend(
        rows[..running_index]
            .iter()
            .cloned()
            .map(TimelineRenderRow::Timeline),
    );
    render_rows.extend(pending_requests.into_iter().map(|entry| {
        TimelineRenderRow::PendingRequest(TimelinePendingRequestRow {
            key: format!("timeline-cli-runtime-request::{}", entry.request_id),
            entry,
        })
    }));
    render_rows.extend(
        rows[running_index..]
            .iter()
            .cloned()
            .map(TimelineRenderRow::Timeline),
    );
    render_rows
}

fn timeline_render_rows_layout_hash(rows: &[TimelineRenderRow]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for row in rows {
        row.key().hash(&mut hasher);
        if let TimelineRenderRow::PendingRequest(row) = row {
            row.entry.request_id.hash(&mut hasher);
            row.entry.request.title.hash(&mut hasher);
            row.entry.request.message.hash(&mut hasher);
            row.entry
                .request
                .payload
                .as_ref()
                .map(serde_json::Value::to_string)
                .hash(&mut hasher);
        }
    }
    hasher.finish()
}
