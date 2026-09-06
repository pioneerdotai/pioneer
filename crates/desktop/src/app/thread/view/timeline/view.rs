use super::{
    TimelineAvatarGroupKind, TimelineGrouping, TimelineLayoutIndex, TimelinePresentationContext,
    TimelineRenderModel, TimelineRenderRow, TimelineRowLayout, TimelineRowTopSpacing,
    items::format_elapsed_ms,
    layout::{
        TIMELINE_AVATAR_RAIL_WIDTH, TIMELINE_AVATAR_SIZE, TIMELINE_CONTENT_HORIZONTAL_PADDING,
    },
    markdown::timeline_message_text_bottom_inset,
    model::{
        TimelineCoalescedToolsKind, TimelineCoalescedToolsRow, TimelineRow, TimelineRowKind,
        TurnWorkGroupRow,
    },
};
use crate::app::{conversation::ConversationViewState, root::PioneerDesktop};
use gpui_kit::component::{
    Icon, IconName, StyledExt, h_flex, scroll::Scrollbar, v_flex, v_virtual_list,
};
use gpui_kit::{prelude::*, *};
use std::{hash::Hasher, time::Instant};

impl PioneerDesktop {
    pub(crate) fn render_timeline(
        &mut self,
        active_thread_id: Option<&str>,
        model: TimelineRenderModel,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        pioneer_observability::record_qualification_diagnostic!(record_render(
            pioneer_observability::RenderRegion::Timeline
        ));
        self.sync_timeline_layout_width(cx);
        let projection = model.projection.clone();
        let item_presentations = model.item_presentations.clone();

        if model.rows.is_empty() {
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
        let row_revisions = model.row_revisions.clone();

        let rows = model.rows;
        let expanded = self.thread_timeline_item_expanded.borrow().clone();
        let expanded_revision = self.thread_timeline_view_state.borrow().expanded_revision;
        let rows_render_fingerprint = (projection.revision, expanded_revision);

        let should_follow_bottom =
            self.sync_timeline_scroll(active_thread_id, projection.as_ref(), rows.as_ref());

        let render_current_principal_id = self
            .gateway
            .current_auth
            .as_ref()
            .map(|auth| auth.principal.id.as_str().to_owned());
        let presentation_context = TimelinePresentationContext {
            task_child_thread: self.active_task_thread_navigation().is_some(),
        };

        if rows.is_empty() {
            return div().w_full().h_full().into_any_element();
        }

        // Timeline row heights depend on the capped content width, not on the empty
        // margins around it. Sidebar resizing above the cap must not invalidate every row.
        let width_px = (content_width / px(1.)).round() as i32;
        let tail_row_key = rows.last().map(|row| row.key());
        let message_text_bottom_inset = timeline_message_text_bottom_inset(window);

        let item_sizes_started = Instant::now();
        let (grouping, item_sizes, layout_index) = {
            let mut state = self.thread_timeline_view_state.borrow_mut();

            let can_reuse = state.cached_render_active_thread_id.as_deref() == active_thread_id
                && state.cached_render_width_px == width_px
                && state.cached_render_item_count == rows.len()
                && state.cached_render_model_fingerprint == rows_render_fingerprint.0
                && state.cached_render_expanded_revision == rows_render_fingerprint.1
                && state.cached_render_principal_id == render_current_principal_id
                && state.cached_render_task_child_thread == presentation_context.task_child_thread;

            if can_reuse
                && let Some(sizes) = state.cached_item_sizes.as_ref()
                && let Some(layout_index) = state.cached_timeline_layout_index.as_ref()
            {
                pioneer_observability::record_qualification_diagnostic!(record_timeline(
                    pioneer_observability::TimelineStage::ItemSizesCacheLookup,
                    pioneer_observability::DiagnosticAction::Hit,
                    u64::try_from(rows.len()).unwrap_or(u64::MAX),
                ));
                pioneer_observability::record_desktop_timeline_stage(
                    pioneer_observability::DesktopTimelineStageMetric {
                        stage: pioneer_observability::DesktopTimelineStage::ItemSizes,
                        cache: pioneer_observability::DesktopTimelineCacheStatus::Hit,
                        content: pioneer_observability::DesktopTimelineContentKind::Mixed,
                        outcome: pioneer_observability::DesktopTimelineOutcome::Ok,
                        elapsed: item_sizes_started.elapsed(),
                        input_bytes: None,
                        block_count: None,
                        row_count: Some(rows.len()),
                    },
                );
                (
                    layout_index.grouping_rc(),
                    sizes.clone(),
                    layout_index.clone(),
                )
            } else {
                pioneer_observability::record_qualification_diagnostic!(record_timeline(
                    pioneer_observability::TimelineStage::ItemSizesCacheLookup,
                    pioneer_observability::DiagnosticAction::Miss,
                    u64::try_from(rows.len()).unwrap_or(u64::MAX),
                ));
                let grouping = TimelineGrouping::from_snapshot(
                    rows.as_ref(),
                    model.groups.as_ref(),
                    projection.as_ref(),
                    render_current_principal_id.as_deref(),
                    presentation_context,
                    message_text_bottom_inset,
                );
                let item_sizes = self.compute_timeline_item_sizes(
                    &mut state,
                    projection.as_ref(),
                    item_presentations.as_ref(),
                    rows.as_ref(),
                    grouping.as_ref(),
                    list_width,
                    content_width,
                    row_revisions.as_ref(),
                    &expanded,
                    window,
                    cx,
                );
                pioneer_observability::record_qualification_diagnostic!(record_timeline(
                    pioneer_observability::TimelineStage::ItemSizesBuild,
                    pioneer_observability::DiagnosticAction::Completed,
                    u64::try_from(item_sizes.len()).unwrap_or(u64::MAX),
                ));
                let layout_index = TimelineLayoutIndex::new(grouping.clone(), item_sizes.clone());

                state.cached_render_active_thread_id = active_thread_id.map(str::to_owned);
                state.cached_render_width_px = width_px;
                state.cached_render_item_count = rows.len();
                state.cached_render_tail_entry_id = tail_row_key.map(str::to_owned);
                state.cached_render_tail_fingerprint = rows_render_fingerprint.0;
                state.cached_render_model_fingerprint = rows_render_fingerprint.0;
                state.cached_render_expanded_revision = rows_render_fingerprint.1;
                state.cached_render_principal_id = render_current_principal_id.clone();
                state.cached_render_task_child_thread = presentation_context.task_child_thread;
                state.cached_item_sizes = Some(item_sizes.clone());
                state.cached_timeline_layout_index = Some(layout_index.clone());

                pioneer_observability::record_desktop_timeline_stage(
                    pioneer_observability::DesktopTimelineStageMetric {
                        stage: pioneer_observability::DesktopTimelineStage::ItemSizes,
                        cache: pioneer_observability::DesktopTimelineCacheStatus::Miss,
                        content: pioneer_observability::DesktopTimelineContentKind::Mixed,
                        outcome: pioneer_observability::DesktopTimelineOutcome::Ok,
                        elapsed: item_sizes_started.elapsed(),
                        input_bytes: None,
                        block_count: None,
                        row_count: Some(rows.len()),
                    },
                );

                (grouping, item_sizes, layout_index)
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
        self.request_mark_active_thread_read_if_viewed(
            active_thread_id,
            rows.as_ref(),
            window.is_window_active(),
            cx,
        );

        let render_thread_id = active_thread_id.map(str::to_owned);
        let render_projection = projection.clone();
        let render_source_revision = model.source_revision;
        let render_item_presentations = item_presentations.clone();
        let render_rows = rows.clone();
        let render_grouping = grouping.clone();
        let render_row_count = render_rows.len();
        let timeline_avatar_rail = self.render_timeline_avatar_rail(
            layout_index,
            self.thread_timeline_scroll_handle.clone(),
            content_width,
            list_width,
            cx,
        );

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
                        pioneer_observability::record_qualification_diagnostic!(record_timeline(
                            pioneer_observability::TimelineStage::VisibleRowTraversal,
                            pioneer_observability::DiagnosticAction::Executed,
                            u64::try_from(visible_indices.len()).unwrap_or(u64::MAX),
                        ));
                        if let Some(thread_id) = render_thread_id.clone() {
                            let row_ids = visible_indices
                                .iter()
                                .filter_map(|ix| render_rows.get(*ix))
                                .map(|row| row.key().to_owned())
                                .collect::<Vec<_>>();
                            let owner = cx.weak_entity();
                            cx.defer(move |cx| {
                                let _ = owner.update(cx, |view, cx| {
                                    view.request_semantic_timeline_prefetch_for_visible_rows(
                                        &thread_id,
                                        &row_ids,
                                        render_source_revision,
                                        cx,
                                    )
                                });
                            });
                        }

                        let elements = visible_indices
                            .into_iter()
                            .filter_map(|ix| {
                                render_rows.get(ix).map(|row| {
                                    view.render_timeline_row(
                                        projection,
                                        render_item_presentations.as_ref(),
                                        row,
                                        ix + 1 == render_row_count,
                                        render_grouping.row_layout(ix),
                                        render_grouping.agent_author_for_group_start(ix),
                                        content_width,
                                        cx,
                                    )
                                })
                            })
                            .collect::<Vec<_>>();
                        pioneer_observability::record_qualification_diagnostic!(record_timeline(
                            pioneer_observability::TimelineStage::VisibleRowElementBuild,
                            pioneer_observability::DiagnosticAction::Completed,
                            u64::try_from(elements.len()).unwrap_or(u64::MAX),
                        ));
                        elements
                    },
                )
                .gap_0()
                .p_0()
                .with_sizing_behavior(ListSizingBehavior::Auto)
                .track_scroll(&self.thread_timeline_scroll_handle),
            )
            .child(timeline_avatar_rail)
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
        item_presentations: &super::TimelineItemPresentations,
        row: &TimelineRenderRow,
        is_last_row: bool,
        row_layout: TimelineRowLayout,
        agent_group_author: Option<&pioneer_protocol::TurnAuthorSnapshot>,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        pioneer_observability::record_qualification_diagnostic!(record_render(
            pioneer_observability::RenderRegion::TimelineRowSlot
        ));
        pioneer_observability::record_qualification_diagnostic!(record_timeline(
            pioneer_observability::TimelineStage::RowBuild,
            pioneer_observability::DiagnosticAction::Executed,
            1,
        ));
        if row_layout.avatar_group_kind == Some(TimelineAvatarGroupKind::Agent) {
            let grouped_content_width = (content_width - TIMELINE_AVATAR_RAIL_WIDTH).max(px(1.));
            let body_top_spacing = if row_layout.starts_avatar_group {
                TimelineRowTopSpacing::Compact
            } else {
                row_layout.top_spacing
            };
            let body = self.render_timeline_row_body(
                projection,
                item_presentations,
                row,
                is_last_row,
                body_top_spacing,
                grouped_content_width,
                cx,
            );
            return self.render_agent_timeline_group_row(
                body,
                agent_group_author,
                row_layout,
                content_width,
            );
        }

        self.render_timeline_row_body(
            projection,
            item_presentations,
            row,
            is_last_row,
            row_layout.top_spacing,
            content_width,
            cx,
        )
    }

    fn render_timeline_row_body(
        &self,
        projection: &ConversationViewState,
        item_presentations: &super::TimelineItemPresentations,
        row: &TimelineRenderRow,
        is_last_row: bool,
        top_spacing: TimelineRowTopSpacing,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        pioneer_observability::record_qualification_diagnostic!(record_render(
            pioneer_observability::RenderRegion::RowBody
        ));
        match row {
            TimelineRenderRow::Timeline(TimelineRow {
                author,
                kind:
                    TimelineRowKind::UserMessage {
                        timeline_index,
                        presentation,
                    },
                ..
            }) => {
                let Some(entry) = projection.timeline.get(*timeline_index) else {
                    return div().into_any_element();
                };
                let Some(item_view) = projection.item_for_timeline_entry(entry) else {
                    return div().into_any_element();
                };
                self.render_item_user_message(
                    entry,
                    item_view,
                    &item_view.item,
                    Some(presentation),
                    author.as_ref(),
                    top_spacing,
                    is_last_row,
                    content_width,
                    cx,
                )
            }
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
                    item_presentations
                        .get(&item_view.id)
                        .expect("captured published item content"),
                    &item_view.item,
                    top_spacing,
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
                top_spacing,
                is_last_row,
                content_width,
                cx,
            ),
            TimelineRenderRow::Timeline(TimelineRow {
                kind: TimelineRowKind::CoalescedTools(group),
                ..
            }) => self.render_coalesced_tools_toggle(
                group,
                top_spacing,
                is_last_row,
                content_width,
                cx,
            ),
            TimelineRenderRow::Timeline(TimelineRow {
                kind: TimelineRowKind::RunningTurn(running_turn),
                ..
            }) => self.render_running_turn_row(
                running_turn,
                top_spacing,
                is_last_row,
                content_width,
                cx,
            ),
            TimelineRenderRow::PendingRequest(row) => {
                let content = self.render_pending_request_card(row.request.clone(), cx);
                self.render_item_row(top_spacing, is_last_row, content_width, content)
            }
        }
    }

    fn render_agent_timeline_group_row(
        &self,
        body: AnyElement,
        author: Option<&pioneer_protocol::TurnAuthorSnapshot>,
        row_layout: TimelineRowLayout,
        content_width: Pixels,
    ) -> AnyElement {
        v_flex()
            .w_full()
            .when(row_layout.starts_avatar_group, |this| {
                this.pt(row_layout.top_spacing.pixels()).child(
                    div().flex().w_full().justify_center().child(
                        h_flex()
                            .w(content_width)
                            .h(TIMELINE_AVATAR_SIZE)
                            .px(TIMELINE_CONTENT_HORIZONTAL_PADDING)
                            .items_center()
                            .child(
                                div()
                                    .ml(TIMELINE_AVATAR_RAIL_WIDTH)
                                    .text_sm()
                                    .font_semibold()
                                    .child(super::timeline_agent_label(author).unwrap_or_else(
                                        || t!("chat.composer.mode.agent_label").to_string(),
                                    )),
                            ),
                    ),
                )
            })
            .child(div().w_full().pl(TIMELINE_AVATAR_RAIL_WIDTH).child(body))
            .into_any_element()
    }

    fn render_coalesced_tools_toggle(
        &self,
        group: &TimelineCoalescedToolsRow,
        top_spacing: TimelineRowTopSpacing,
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
            top_spacing,
            is_last_row,
            content_width,
            toggle.into_any_element(),
        )
    }

    fn render_turn_work_group_toggle(
        &self,
        group: &TurnWorkGroupRow,
        top_spacing: TimelineRowTopSpacing,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let elapsed_label = group.elapsed_ms.map(format_elapsed_ms);
        let status_label = match group.state.as_ref() {
            Some(&pioneer_protocol::TurnWorkState::Starting) => {
                t!("timeline.task.status.queued").to_string()
            }
            Some(&pioneer_protocol::TurnWorkState::Running)
            | Some(&pioneer_protocol::TurnWorkState::Stalled)
            | Some(&pioneer_protocol::TurnWorkState::WaitingForApproval) => {
                t!("timeline.task.status.running").to_string()
            }
            Some(&pioneer_protocol::TurnWorkState::Failed) => {
                t!("timeline.task.status.failed").to_string()
            }
            Some(&pioneer_protocol::TurnWorkState::Interrupted) => {
                t!("timeline.task.status.cancelled").to_string()
            }
            Some(&pioneer_protocol::TurnWorkState::Blocked) => {
                t!("timeline.task.status.blocked").to_string()
            }
            Some(&pioneer_protocol::TurnWorkState::Completed) | None => {
                t!("timeline.work_group.completed").to_string()
            }
        };

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
                    .child(status_label)
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
            top_spacing,
            is_last_row,
            content_width,
            toggle.into_any_element(),
        )
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
