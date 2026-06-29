use super::{
    TimelinePendingRequestRow, TimelineRenderModel, TimelineRenderRow,
    items::format_elapsed_ms,
    model::{
        TimelineCoalescedToolsKind, TimelineCoalescedToolsRow, TimelineRow, TimelineRowKind,
        TurnWorkGroupRow,
    },
};
use crate::app::{
    conversation::ConversationViewState,
    root::{PendingRequest, PioneerDesktop},
};
use gpui::{prelude::*, *};
use gpui_component::{Icon, IconName, h_flex, scroll::Scrollbar, v_flex, v_virtual_list};
use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    rc::Rc,
};

impl PioneerDesktop {
    pub(crate) fn render_timeline(
        &self,
        active_thread_id: Option<&str>,
        model: TimelineRenderModel,
        pending_requests: Vec<PendingRequest>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        self.sync_timeline_layout_width(cx);
        let projection = model.projection.clone();

        if model.rows.is_empty() && pending_requests.is_empty() {
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
        let semantic_row_ids = model.semantic_row_ids.clone();
        let expanded_revision = self.thread_timeline_view_state.borrow().expanded_revision;
        let model_signature_hash = Self::timeline_model_signature_hash(
            active_thread_id,
            projection.revision,
            expanded_revision,
        );

        let rows = self.hydrate_running_turn_render_rows(model.rows, cx);
        let rows = Rc::new(merge_pending_timeline_render_rows(rows, pending_requests));
        let rows_layout_hash =
            model_signature_hash ^ timeline_render_rows_layout_hash(rows.as_ref());

        let should_follow_bottom =
            self.sync_timeline_scroll(active_thread_id, projection.as_ref(), rows.as_ref());

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
                    projection.as_ref(),
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

        self.restore_pending_timeline_scroll_anchor(
            active_thread_id,
            rows.as_ref(),
            item_sizes.as_ref(),
        );
        if should_follow_bottom {
            self.scroll_timeline_to_bottom_for_item_sizes(item_sizes.as_ref());
        }

        let render_thread_id = active_thread_id.map(str::to_owned);
        let render_projection = projection.clone();
        let render_semantic_row_ids = semantic_row_ids.clone();
        let render_semantic_rows = model.semantic_rows.clone();
        let render_rows = rows.clone();
        let render_row_count = render_rows.len();

        div()
            .w_full()
            .h_full()
            .relative()
            .overflow_hidden()
            .on_scroll_wheel(cx.listener(|view, event, window, cx| {
                view.on_timeline_scroll_wheel(event, window, cx);
            }))
            .child(
                v_virtual_list(
                    cx.entity(),
                    "thread-timeline-virtual-list",
                    item_sizes,
                    move |view, visible_range, _, cx| {
                        let projection = render_projection.as_ref();
                        let visible_indices = visible_range.collect::<Vec<_>>();
                        if let Some(thread_id) = render_thread_id.as_deref() {
                            view.request_semantic_timeline_prefetch_for_visible_rows(
                                thread_id,
                                render_rows.as_ref(),
                                render_semantic_row_ids.as_ref(),
                                render_semantic_rows.as_ref(),
                                visible_indices.as_slice(),
                                cx,
                            );
                        }

                        visible_indices
                            .into_iter()
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
                let permission_profile = projection.turn_permission_profile(entry.turn_id.as_str());

                self.render_turn_item_entry(
                    entry,
                    item_view,
                    &item_view.item,
                    permission_profile,
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
                let content = self.render_pending_request_card(row.request.clone(), cx);
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
                    this.toggle_turn_work_group_expanded(toggle_key.as_str(), cx);
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
                    this.toggle_turn_work_group_expanded(toggle_key.as_str(), cx);
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

pub(in crate::app::thread::view::timeline) fn merge_pending_timeline_render_rows(
    rows: Rc<Vec<TimelineRenderRow>>,
    pending_requests: Vec<PendingRequest>,
) -> Vec<TimelineRenderRow> {
    if pending_requests.is_empty() {
        return rows.as_ref().clone();
    }

    let running_index = rows
        .iter()
        .position(|row| {
            matches!(
                row,
                TimelineRenderRow::Timeline(TimelineRow {
                    kind: TimelineRowKind::RunningTurn(_),
                    ..
                })
            )
        })
        .unwrap_or(rows.len());
    let existing_keys = rows
        .iter()
        .map(TimelineRenderRow::key)
        .collect::<HashSet<_>>();

    let mut render_rows = Vec::with_capacity(rows.len() + pending_requests.len());
    render_rows.extend(rows[..running_index].iter().cloned());
    render_rows.extend(pending_requests.into_iter().filter_map(|request| {
        let key = format!("timeline-pending-request::{}", request.request_id);
        (!existing_keys.contains(key.as_str())).then_some(TimelineRenderRow::PendingRequest(
            TimelinePendingRequestRow { key, request },
        ))
    }));
    render_rows.extend(rows[running_index..].iter().cloned());
    render_rows
}

impl PioneerDesktop {
    fn hydrate_running_turn_render_rows(
        &self,
        rows: Rc<Vec<TimelineRenderRow>>,
        cx: &mut Context<Self>,
    ) -> Rc<Vec<TimelineRenderRow>> {
        let timeline_rows = rows
            .iter()
            .filter_map(|row| match row {
                TimelineRenderRow::Timeline(row) => Some(row.clone()),
                TimelineRenderRow::PendingRequest(_) => None,
            })
            .collect::<Vec<_>>();
        let hydrated = self.hydrate_running_turn_rows(Rc::new(timeline_rows), cx);
        let mut hydrated_iter = hydrated.iter();
        let mut changed = false;
        let mut render_rows = rows.as_ref().clone();
        for row in &mut render_rows {
            if let TimelineRenderRow::Timeline(row) = row
                && matches!(row.kind, TimelineRowKind::RunningTurn(_))
                && let Some(hydrated_row) = hydrated_iter.next()
            {
                changed |= row != hydrated_row;
                *row = hydrated_row.clone();
            } else if matches!(row, TimelineRenderRow::Timeline(_)) {
                let _ = hydrated_iter.next();
            }
        }
        if changed { Rc::new(render_rows) } else { rows }
    }
}

fn timeline_render_rows_layout_hash(rows: &[TimelineRenderRow]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for row in rows {
        row.key().hash(&mut hasher);
        if let TimelineRenderRow::PendingRequest(row) = row {
            row.request.request_id.hash(&mut hasher);
            row.request.title.hash(&mut hasher);
            row.request.message.hash(&mut hasher);
            format!("{:?}", row.request.origin).hash(&mut hasher);
            format!("{:?}", row.request.kind).hash(&mut hasher);
            format!("{:?}", row.request.payload).hash(&mut hasher);
        }
    }
    hasher.finish()
}
