use super::TimelineAvatarGroupKind;
use super::TimelineGrouping;
use super::TimelineLayoutIndex;
use super::TimelinePresentationContext;
use super::TimelineRenderModel;
use super::TimelineRenderRow;
use super::TimelineRowLayout;
use super::TimelineRowTopSpacing;
use super::items::format_elapsed_ms;
use super::layout::TIMELINE_AVATAR_RAIL_WIDTH;
use super::layout::TIMELINE_AVATAR_SIZE;
use super::layout::TIMELINE_CONTENT_HORIZONTAL_PADDING;
use super::markdown::timeline_message_text_bottom_inset;
use super::model::TimelineCoalescedToolsKind;
use super::model::TimelineCoalescedToolsRow;
use super::model::TimelineRow;
use super::model::TimelineRowKind;
use super::model::TurnWorkGroupRow;
use crate::screen::TimelineView;
use gpui_kit::component::Icon;
use gpui_kit::component::IconName;
use gpui_kit::component::StyledExt;
use gpui_kit::component::h_flex;
use gpui_kit::component::scroll::Scrollbar;
use gpui_kit::component::v_flex;
use gpui_kit::component::v_virtual_list;
use gpui_kit::prelude::*;
use gpui_kit::*;
use pioneer_client::conversation::reducer::ConversationViewState;
use std::hash::Hasher;

#[derive(Clone)]
pub(crate) struct PreparedTimeline {
    pub(crate) model: TimelineRenderModel,
    pub(crate) grouping: std::rc::Rc<TimelineGrouping>,
    pub(crate) item_sizes: std::rc::Rc<Vec<Size<Pixels>>>,
    pub(crate) layout_index: std::rc::Rc<TimelineLayoutIndex>,
    pub(crate) content_width: Pixels,
    pub(crate) list_width: Pixels,
}

impl TimelineView {
    pub(crate) fn reconcile_timeline(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let model = self.thread_timeline_view_state.model.clone();
        let thread_id = self.thread_id.clone();
        let active_thread_id = Some(thread_id.as_str());
        let projection = model.projection.clone();
        let item_presentations = model.item_presentations.clone();

        let list_width = self.timeline_content_width(window);
        let content_width = self.timeline_entry_content_width(list_width);
        let row_revisions = model.row_revisions.clone();

        let rows = model.rows.clone();
        let expanded = self.thread_timeline_view_state.expanded.borrow().clone();
        let expanded_revision = self.thread_timeline_view_state.borrow().expanded_revision;
        let rows_render_fingerprint = (projection.revision, expanded_revision);

        let should_follow_bottom =
            self.sync_timeline_scroll(active_thread_id, projection.as_ref(), rows.as_ref());
        // Coalescing layout inputs must retain an already requested follow until
        // commit. A user scroll event can cancel it while measurement is pending.
        self.thread_timeline_view_state
            .borrow_mut()
            .pending_follow_bottom |= should_follow_bottom;
        for entry in &projection.timeline {
            if let Some(item) = projection.item_for_timeline_entry(entry) {
                if let pioneer_client::timeline::types::TurnItem::Task { item: task } = &item.item
                    && task.status == pioneer_client::timeline::types::TaskStatus::Running
                {
                    let id = format!("task:{}", task.id);
                    self.prepare_running_dino(format!("content:{id}"), cx);
                    self.prepare_running_elapsed(
                        id,
                        task.started_at
                            .map(|value| value.saturating_mul(1_000))
                            .or(item.started_at_unix_ms)
                            .unwrap_or(task.created_at.saturating_mul(1_000)),
                        true,
                        cx,
                    );
                }
                if matches!(
                    item.item,
                    pioneer_client::timeline::types::TurnItem::CommandExecution { .. }
                ) {
                    self.prepare_command_terminal(entry, item, content_width, cx);
                }
                if let Some(content) = item_presentations
                    .get(&item.id)
                    .filter(|content| !content.streaming)
                {
                    if let Some(document) = &content.markdown {
                        self.prepare_markdown_highlights(document, cx);
                    }
                }
            }
        }
        for row in rows.iter() {
            if let TimelineRenderRow::Timeline(TimelineRow {
                kind: TimelineRowKind::RunningTurn(turn),
                ..
            }) = row
            {
                let id = format!("turn:{}", turn.turn_id);
                let show_dino = self.active_task_thread_navigation().is_none();
                if show_dino {
                    self.prepare_running_dino(format!("content:{id}"), cx);
                }
                self.prepare_running_elapsed(
                    id,
                    turn.started_at_unix_ms
                        .unwrap_or_else(pioneer_client::timeline::labels::now_unix_ms),
                    show_dino,
                    cx,
                );
            }
        }

        let render_current_principal_id = self
            .identity_input
            .as_ref()
            .and_then(|input| input.current_auth.as_ref())
            .map(|auth| auth.principal.id.as_str().to_owned());
        let presentation_context = TimelinePresentationContext {
            task_child_thread: self.active_task_thread_navigation().is_some(),
        };

        // Timeline row heights depend on the capped content width, not on the empty
        // margins around it. Sidebar resizing above the cap must not invalidate every row.
        let width_px = (content_width / px(1.)).round() as i32;
        let tail_row_key = rows.last().map(|row| row.key());
        let message_text_bottom_inset = timeline_message_text_bottom_inset(window);

        let generation = self
            .thread_timeline_view_state
            .layout_generation
            .saturating_add(1);
        self.thread_timeline_view_state.layout_generation = generation;
        self.thread_timeline_view_state.measurement = None;
        let state = self.thread_timeline_view_state.borrow();
        let can_reuse = state.cached_render_active_thread_id.as_deref() == active_thread_id
            && state.cached_render_width_px == width_px
            && state.cached_render_item_count == rows.len()
            && state.cached_render_model_fingerprint == rows_render_fingerprint.0
            && state.cached_render_expanded_revision == rows_render_fingerprint.1
            && state.cached_render_principal_id == render_current_principal_id
            && state.cached_render_task_child_thread == presentation_context.task_child_thread;
        if can_reuse
            && let Some(item_sizes) = state.cached_item_sizes.clone()
            && let Some(layout_index) = state.cached_timeline_layout_index.clone()
        {
            let grouping = layout_index.grouping_rc();
            drop(state);
            self.commit_timeline_layout(PreparedTimeline {
                model,
                grouping,
                item_sizes,
                layout_index,
                content_width,
                list_width,
            });
            return;
        }
        let grouping = TimelineGrouping::from_snapshot(
            rows.as_ref(),
            model.groups.as_ref(),
            projection.as_ref(),
            render_current_principal_id.as_deref(),
            presentation_context,
            message_text_bottom_inset,
        );
        let measurement = self.prepare_timeline_item_sizes(
            &state,
            projection.as_ref(),
            item_presentations.as_ref(),
            rows.as_ref(),
            grouping.as_ref(),
            list_width,
            content_width,
            row_revisions.as_ref(),
            &expanded,
            cx,
        );
        drop(state);
        let tail_row_key = tail_row_key.map(str::to_owned);
        let entity = cx.weak_entity();
        self.thread_timeline_view_state.measurement = Some(std::rc::Rc::new(
            std::cell::RefCell::new(Some(Box::new(move |window, cx| {
                let (item_sizes, cache) = measurement.measure(window, cx);
                window.defer(cx, move |window, cx| {
                    let _ = entity.update(cx, |view, cx| {
                        if view.thread_timeline_view_state.layout_generation != generation {
                            return;
                        }
                        view.thread_timeline_view_state.measurement = None;
                        let layout_index =
                            TimelineLayoutIndex::new(grouping.clone(), item_sizes.clone());
                        {
                            let mut state = view.thread_timeline_view_state.borrow_mut();
                            state.entry_layout_cache = cache;
                            state.cached_render_active_thread_id = Some(thread_id);
                            state.cached_render_width_px = width_px;
                            state.cached_render_item_count = rows.len();
                            state.cached_render_tail_entry_id = tail_row_key;
                            state.cached_render_tail_fingerprint = rows_render_fingerprint.0;
                            state.cached_render_model_fingerprint = rows_render_fingerprint.0;
                            state.cached_render_expanded_revision = rows_render_fingerprint.1;
                            state.cached_render_principal_id = render_current_principal_id;
                            state.cached_render_task_child_thread =
                                presentation_context.task_child_thread;
                            state.cached_item_sizes = Some(item_sizes.clone());
                            state.cached_timeline_layout_index = Some(layout_index.clone());
                        }
                        view.commit_timeline_layout(PreparedTimeline {
                            model,
                            grouping,
                            item_sizes,
                            layout_index,
                            content_width,
                            list_width,
                        });
                        super::controller::DesktopTimelineController::schedule(view, window, cx);
                        cx.notify();
                    });
                });
            }))),
        ));
    }

    fn commit_timeline_layout(&mut self, prepared: PreparedTimeline) {
        let should_follow_bottom = {
            let mut state = self.thread_timeline_view_state.borrow_mut();
            let follow = state.pending_follow_bottom && !state.autoscroll_paused_by_user;
            state.pending_follow_bottom = false;
            follow
        };
        self.thread_timeline_view_state
            .borrow_mut()
            .reconcile_scroll(
                Some(&self.thread_id),
                prepared.model.rows.clone(),
                prepared.item_sizes.clone(),
                &self.thread_timeline_view_state.scroll_handle,
                should_follow_bottom,
            );
        self.thread_timeline_view_state.prepared = Some(prepared);
    }

    fn timeline_measurement_pass(&self, cx: &Context<Self>) -> AnyElement {
        let measurement = self.thread_timeline_view_state.measurement.clone();
        let scroll = self.thread_timeline_view_state.scroll_handle.clone();
        let bounds = self.thread_timeline_view_state.viewport;
        let offset = self.thread_timeline_view_state.viewport_offset;
        let rem = self.thread_timeline_view_state.layout_rem;
        let visible = self.thread_timeline_view_state.visible;
        let entity = cx.weak_entity();
        canvas(
            move |_, window, cx| {
                // Only the single-use draw payload is consumed here. Cache, scroll,
                // snapshot and notification changes happen in its deferred handler.
                if let Some(measurement) = measurement
                    && let Some(measure) = measurement.borrow_mut().take()
                {
                    measure(window, cx);
                }
            },
            move |_, _, window, cx| {
                // The stock handle is committed by paint. Deliver only changed
                // geometry; no viewport mutation or notify occurs during layout.
                if visible
                    && (scroll.bounds() != bounds
                        || scroll.offset() != offset
                        || window.rem_size() != rem)
                {
                    window.defer(cx, move |window, cx| {
                        let _ = entity.update(cx, |view, cx| {
                            if view.thread_timeline_view_state.visible {
                                super::controller::DesktopTimelineController::viewport(
                                    view, window, cx,
                                );
                            }
                        });
                    });
                }
            },
        )
        .absolute()
        .size_full()
        .into_any_element()
    }

    pub(crate) fn render_timeline(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let Some(prepared) = self
            .thread_timeline_view_state
            .prepared
            .clone()
            .filter(|p| !p.model.rows.is_empty())
        else {
            return v_flex()
                .w_full()
                .h_full()
                .justify_center()
                .items_center()
                .text_sm()
                .opacity(0.6)
                .child(t!("timeline.empty.start_thread").to_string())
                .child(self.timeline_measurement_pass(cx))
                .into_any_element();
        };
        let PreparedTimeline {
            model,
            grouping,
            item_sizes,
            layout_index,
            content_width,
            list_width,
        } = prepared;
        let projection = model.projection.clone();
        let item_presentations = model.item_presentations.clone();
        let rows = model.rows.clone();
        let render_projection = projection.clone();
        let render_item_presentations = item_presentations.clone();
        let render_rows = rows.clone();
        let render_grouping = grouping.clone();
        let render_row_count = render_rows.len();
        let timeline_avatar_rail = self.render_timeline_avatar_rail(
            layout_index,
            self.thread_timeline_view_state.scroll_handle.clone(),
            content_width,
            list_width,
            cx,
        );

        div()
            .w_full()
            .h_full()
            .relative()
            .overflow_hidden()
            .on_scroll_wheel(cx.listener(|view, event: &ScrollWheelEvent, window, cx| {
                super::controller::DesktopTimelineController::dispatch(view, &super::controller::TimelineAction::Scroll { delta_y: event.delta.pixel_delta(window.line_height()).y }, window, cx);
            }))
            .on_mouse_move(cx.listener(|view, event: &MouseMoveEvent, window, cx| { if event.pressed_button == Some(MouseButton::Left) { super::controller::DesktopTimelineController::schedule(view, window, cx); } }))
            .child(
                v_virtual_list(
                    cx.entity(),
                    "thread-timeline-virtual-list",
                    item_sizes,
                    move |view, visible_range, _, cx| {
                        let projection = render_projection.as_ref();
                        let visible_indices = visible_range.collect::<Vec<_>>();
                        pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_timeline(
                            pioneer_client::timeline::diagnostics::TimelineStage::VisibleRowTraversal,
                            pioneer_client::timeline::diagnostics::DiagnosticAction::Executed,
                            u64::try_from(visible_indices.len()).unwrap_or(u64::MAX),
                        ));
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
                        pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_timeline(
                            pioneer_client::timeline::diagnostics::TimelineStage::VisibleRowElementBuild,
                            pioneer_client::timeline::diagnostics::DiagnosticAction::Completed,
                            u64::try_from(elements.len()).unwrap_or(u64::MAX),
                        ));
                        elements
                    },
                )
                .gap_0()
                .p_0()
                .with_sizing_behavior(ListSizingBehavior::Auto)
                .track_scroll(&self.thread_timeline_view_state.scroll_handle),
            )
            .child(timeline_avatar_rail)
            .child(self.timeline_measurement_pass(cx))
            .child(
                div()
                    .absolute()
                    .top_0()
                    .left_0()
                    .right_0()
                    .bottom_0()
                    .child(Scrollbar::vertical(&self.thread_timeline_view_state.scroll_handle)),
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
        agent_group_author: Option<&pioneer_client::timeline::types::TurnAuthorSnapshot>,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_render(
            pioneer_client::timeline::diagnostics::RenderRegion::TimelineRowSlot
        ));
        pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_timeline(
            pioneer_client::timeline::diagnostics::TimelineStage::RowBuild,
            pioneer_client::timeline::diagnostics::DiagnosticAction::Executed,
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
        pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_render(
            pioneer_client::timeline::diagnostics::RenderRegion::RowBody
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
                let content = self
                    .current_active_thread_id()
                    .and_then(|thread| {
                        self.pending_request_views
                            .get(&(thread.to_owned(), row.request.request_id.clone()))
                    })
                    .map(|view| view.clone().into_any_element())
                    .unwrap_or_else(|| div().into_any_element());
                self.render_item_row(top_spacing, is_last_row, content_width, content)
            }
        }
    }

    fn render_agent_timeline_group_row(
        &self,
        body: AnyElement,
        author: Option<&pioneer_client::timeline::types::TurnAuthorSnapshot>,
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
                cx.listener(move |this, _, window, cx| {
                    this.toggle_turn_work_group_expanded(toggle_key.as_str(), window, cx);
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
            Some(&pioneer_client::timeline::types::TurnWorkState::Starting) => {
                t!("timeline.task.status.queued").to_string()
            }
            Some(&pioneer_client::timeline::types::TurnWorkState::Running)
            | Some(&pioneer_client::timeline::types::TurnWorkState::Stalled)
            | Some(&pioneer_client::timeline::types::TurnWorkState::WaitingForApproval) => {
                t!("timeline.task.status.running").to_string()
            }
            Some(&pioneer_client::timeline::types::TurnWorkState::Failed) => {
                t!("timeline.task.status.failed").to_string()
            }
            Some(&pioneer_client::timeline::types::TurnWorkState::Interrupted) => {
                t!("timeline.task.status.cancelled").to_string()
            }
            Some(&pioneer_client::timeline::types::TurnWorkState::Blocked) => {
                t!("timeline.task.status.blocked").to_string()
            }
            Some(&pioneer_client::timeline::types::TurnWorkState::Completed) | None => {
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
                cx.listener(move |this, _, window, cx| {
                    this.toggle_turn_work_group_expanded(toggle_key.as_str(), window, cx);
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
