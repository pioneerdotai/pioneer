use super::TimelineAvatarGroupKind;
use super::TimelineRenderRow;
use super::TimelineRowLayout;
use super::TimelineRowTopSpacing;
use super::items::format_elapsed_ms;
use super::layout::TIMELINE_AVATAR_RAIL_WIDTH;
use super::layout::TIMELINE_AVATAR_SIZE;
use super::layout::TIMELINE_CONTENT_HORIZONTAL_PADDING;
use super::model::TimelineCoalescedToolsKind;
use super::model::TimelineCoalescedToolsRow;
use super::model::TimelineRow;
use super::model::TimelineRowKind;
use super::model::TurnWorkGroupRow;
use gpui_kit::component::Icon;
use gpui_kit::component::IconName;
use gpui_kit::component::StyledExt;
use gpui_kit::component::h_flex;
use gpui_kit::component::v_flex;
use gpui_kit::prelude::*;
use gpui_kit::*;
use pioneer_client::conversation::reducer::ConversationViewState;
use std::hash::Hasher;

use super::row_view::RowPresentation;
impl RowPresentation {
    pub(super) fn render_timeline_row(
        &self,
        projection: &ConversationViewState,
        item_presentations: &super::TimelineItemPresentations,
        row: &TimelineRenderRow,
        is_last_row: bool,
        row_layout: TimelineRowLayout,
        agent_group_author: Option<&pioneer_client::timeline::types::TurnAuthorSnapshot>,
        content_width: Pixels,
        expanded: bool,
        terminal: Option<Entity<terminal::TerminalView>>,
        cx: &mut App,
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
                expanded,
                terminal,
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
            expanded,
            terminal,
            cx,
        )
    }
    pub(super) fn render_timeline_row_body(
        &self,
        projection: &ConversationViewState,
        item_presentations: &super::TimelineItemPresentations,
        row: &TimelineRenderRow,
        is_last_row: bool,
        top_spacing: TimelineRowTopSpacing,
        content_width: Pixels,
        expanded: bool,
        terminal: Option<Entity<terminal::TerminalView>>,
        cx: &mut App,
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
                    item_presentations
                        .get(&item_view.id)
                        .and_then(|row| row.content())
                        .expect("Client message content"),
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
                        .and_then(|row| row.content())
                        .expect("captured published item content"),
                    &item_view.item,
                    top_spacing,
                    is_last_row,
                    content_width,
                    expanded,
                    terminal,
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
        cx: &mut App,
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
                self.actions.listener(move |this, _, window, cx| {
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
        cx: &mut App,
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
                self.actions.listener(move |this, _, window, cx| {
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
