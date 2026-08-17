use super::TimelineRowTopSpacing;
use super::items::format_elapsed_ms;
use super::model::{TimelineRow, TimelineRowKind};
use crate::app::conversation::ConversationViewState;
use crate::app::root::PioneerDesktop;
use gpui::{ImageSource, Resource, prelude::*, *};
use gpui_component::{StyledExt, h_flex, theme::ActiveTheme, v_flex};
use pioneer_client::security::ClientTurnSecuritySummary;
use pioneer_client::timeline::labels::{RunningTurnDisplay, now_unix_ms};
use pioneer_protocol::{TaskStatus, TurnItem};
use std::{rc::Rc, time::Duration};

fn running_turn_dino_image_source(is_dark: bool) -> ImageSource {
    let asset_path = if is_dark {
        "dino-dark.webp"
    } else {
        "dino-light.webp"
    };
    ImageSource::Resource(Resource::Embedded(asset_path.into()))
}

pub(super) fn render_running_turn_dino(image_id: ElementId, is_dark: bool) -> AnyElement {
    // GPUI retains animated-image frame state and requests subsequent frames only
    // for stateful images with a stable global element id.
    img(running_turn_dino_image_source(is_dark))
        .id(image_id)
        .w_full()
        .h_full()
        .object_fit(ObjectFit::Contain)
        .into_any_element()
}

impl PioneerDesktop {
    pub(super) fn semantic_timeline_has_running_turn_row(&self) -> bool {
        let active_thread_id = self.current_active_thread_id().map(str::to_owned);
        let model = self.semantic_timeline_render_model(active_thread_id.as_deref());
        model.rows.iter().any(|row| {
            matches!(
                row,
                super::TimelineRenderRow::Timeline(TimelineRow {
                    kind: TimelineRowKind::RunningTurn(_),
                    ..
                })
            )
        })
    }

    pub(super) fn semantic_timeline_has_running_activity(&self) -> bool {
        let active_thread_id = self.current_active_thread_id().map(str::to_owned);
        let model = self.semantic_timeline_render_model(active_thread_id.as_deref());
        model.rows.iter().any(|row| {
            matches!(
                row,
                super::TimelineRenderRow::Timeline(TimelineRow {
                    kind: TimelineRowKind::RunningTurn(_),
                    ..
                })
            )
        }) || projection_has_running_task(model.projection.as_ref())
    }

    pub(super) fn ensure_running_task_indicator_timer(
        &self,
        projection: &ConversationViewState,
        cx: &mut Context<Self>,
    ) {
        if projection_has_running_task(projection) {
            self.ensure_running_indicator_timer(cx);
        }
    }

    pub(super) fn hydrate_running_turn_rows(
        &self,
        rows: Rc<Vec<TimelineRow>>,
        cx: &mut Context<Self>,
    ) -> Rc<Vec<TimelineRow>> {
        let Some((running_row_index, running_turn)) =
            rows.iter().enumerate().find_map(|(index, row)| {
                if let TimelineRowKind::RunningTurn(running_turn) = &row.kind {
                    Some((index, running_turn))
                } else {
                    None
                }
            })
        else {
            return rows;
        };

        let now = now_unix_ms();
        let started_at = {
            let mut state = self.thread_timeline_view_state.borrow_mut();
            let started_at = if let Some(started_at) = running_turn.started_at_unix_ms {
                state.running_turn_indicator_fallback_turn_id = Some(running_turn.turn_id.clone());
                state.running_turn_indicator_fallback_started_at_unix_ms = Some(started_at);
                started_at
            } else {
                if state.running_turn_indicator_fallback_turn_id.as_deref()
                    != Some(running_turn.turn_id.as_str())
                {
                    state.running_turn_indicator_fallback_turn_id =
                        Some(running_turn.turn_id.clone());
                    state.running_turn_indicator_fallback_started_at_unix_ms = Some(now);
                }

                state
                    .running_turn_indicator_fallback_started_at_unix_ms
                    .unwrap_or(now)
            };

            started_at
        };

        self.ensure_running_indicator_timer(cx);

        if running_turn.started_at_unix_ms == Some(started_at) {
            return rows;
        }

        let mut hydrated_rows = rows.as_ref().clone();
        if let Some(row) = hydrated_rows.get_mut(running_row_index)
            && let TimelineRowKind::RunningTurn(running_turn) = &mut row.kind
        {
            running_turn.started_at_unix_ms = Some(started_at);
        }

        Rc::new(hydrated_rows)
    }

    pub(super) fn render_running_turn_row(
        &self,
        running_turn: &RunningTurnDisplay,
        top_spacing: TimelineRowTopSpacing,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let content = self.render_running_activity_content(
            format!("turn:{}", running_turn.turn_id),
            running_turn.started_at_unix_ms,
            running_turn.state.clone(),
            running_turn.security_summary.as_ref(),
            self.active_task_thread_navigation().is_none(),
            cx,
        );

        self.render_item_row(
            top_spacing,
            is_last_row,
            content_width,
            div().w_full().pt_5().child(content).into_any_element(),
        )
    }

    pub(super) fn render_running_activity_content(
        &self,
        activity_id: String,
        started_at_unix_ms: Option<i64>,
        state: Option<pioneer_protocol::TurnWorkState>,
        security_summary: Option<&ClientTurnSecuritySummary>,
        show_dino: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        // Timeline measurement renders item cards while holding a mutable borrow of the
        // timeline view state. Keep this shared renderer state-borrow-free.
        let now = now_unix_ms();
        let started_at = started_at_unix_ms.unwrap_or(now);
        let elapsed_ms = now.saturating_sub(started_at).max(0) as u64;
        let elapsed = if elapsed_ms >= 1_000 {
            format_elapsed_ms(elapsed_ms)
        } else {
            String::new()
        };
        let elapsed_second = elapsed_ms / 1_000;
        let elapsed_id = ElementId::from((
            ElementId::from((
                ElementId::from("running-activity-elapsed"),
                activity_id.clone(),
            )),
            elapsed_second.to_string(),
        ));
        let image_id = ElementId::from((ElementId::from("running-activity-image"), activity_id));
        let is_dark = cx.theme().mode.is_dark();
        let status_label = match state {
            Some(pioneer_protocol::TurnWorkState::Starting)
            | Some(pioneer_protocol::TurnWorkState::Stalled) => {
                t!("timeline.task.status.queued").to_string()
            }
            Some(pioneer_protocol::TurnWorkState::WaitingForApproval) => {
                t!("timeline.task.status.waiting").to_string()
            }
            _ => t!("timeline.running.turn").to_string(),
        };

        h_flex()
            .w_full()
            .items_center()
            .justify_between()
            .gap_4()
            .text_sm()
            .child(
                h_flex()
                    .items_center()
                    .gap_2()
                    .when(show_dino, |this| {
                        this.child(
                            div()
                                .size_8()
                                .child(render_running_turn_dino(image_id, is_dark)),
                        )
                    })
                    .child(
                        v_flex()
                            .pt_1()
                            .gap_1()
                            .when(!show_dino, |this| this.pt_0().mb(px(2.)))
                            .child(div().font_semibold().child(status_label))
                            .when_some(security_summary, |this, summary| {
                                this.child(self.render_turn_security_summary(summary, cx))
                            }),
                    ),
            )
            .child(
                div()
                    .id(elapsed_id)
                    .pt_1()
                    .when(!show_dino, |this| this.pt_0().mb(px(2.)))
                    .font_semibold()
                    .child(elapsed),
            )
            .into_any_element()
    }

    fn ensure_running_indicator_timer(&self, cx: &mut Context<Self>) {
        let should_start_timer = {
            let mut state = self.thread_timeline_view_state.borrow_mut();
            if state.running_turn_indicator_timer_active {
                false
            } else {
                state.running_turn_indicator_timer_active = true;
                true
            }
        };

        if should_start_timer {
            self.spawn_running_turn_indicator_timer(cx);
        }
    }

    fn spawn_running_turn_indicator_timer(&self, cx: &mut Context<Self>) {
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();

            async move {
                loop {
                    cx.background_executor().timer(Duration::from_secs(1)).await;

                    let keep_running = this
                        .update(&mut cx, |view, cx| {
                            let visible = view.semantic_timeline_has_running_activity();

                            if !visible {
                                let mut state = view.thread_timeline_view_state.borrow_mut();
                                state.running_turn_indicator_timer_active = false;
                                state.running_turn_indicator_fallback_turn_id = None;
                                state.running_turn_indicator_fallback_started_at_unix_ms = None;
                            }

                            cx.notify();
                            visible
                        })
                        .unwrap_or(false);

                    if !keep_running {
                        break;
                    }
                }
            }
        })
        .detach();
    }
}

fn projection_has_running_task(projection: &ConversationViewState) -> bool {
    projection.items.iter().any(|item_view| {
        matches!(
            &item_view.item,
            TurnItem::Task { item } if item.status == TaskStatus::Running
        )
    })
}

#[cfg(test)]
mod tests {
    use super::projection_has_running_task;
    use crate::app::conversation::{ConversationViewState, ItemView, TimelineEntryStatus};
    use pioneer_protocol::{
        TaskAttachmentMode, TaskExecutorKind, TaskStatus, TaskTriggerKind, TaskTurnItem, TurnItem,
    };

    #[test]
    fn task_activity_timer_tracks_only_running_task_cards() {
        assert!(projection_has_running_task(&projection_with_task(
            TaskStatus::Running
        )));
        assert!(!projection_has_running_task(&projection_with_task(
            TaskStatus::Queued
        )));
        assert!(!projection_has_running_task(&projection_with_task(
            TaskStatus::Completed
        )));
    }

    fn projection_with_task(status: TaskStatus) -> ConversationViewState {
        ConversationViewState {
            items: vec![ItemView {
                id: "task-anchor".to_owned(),
                turn_id: "task-turn".to_owned(),
                item_type: "task".to_owned(),
                status: if status.is_terminal() {
                    TimelineEntryStatus::Completed
                } else {
                    TimelineEntryStatus::Running
                },
                started_at_unix_ms: Some(1_000),
                updated_at_unix_ms: Some(2_000),
                completed_at_unix_ms: status.is_terminal().then_some(2_000),
                partial_text: "Background task".to_owned(),
                final_text: status.is_terminal().then(|| "Background task".to_owned()),
                partial_markdown: None,
                final_markdown: None,
                item: TurnItem::Task {
                    item: TaskTurnItem {
                        id: "task-anchor".to_owned(),
                        task_id: "task".to_owned(),
                        created_by_turn_id: Some("source-turn".to_owned()),
                        run_id: Some("run".to_owned()),
                        parent_task_id: None,
                        root_task_id: None,
                        title: "Background task".to_owned(),
                        status,
                        attachment: TaskAttachmentMode::Detached,
                        trigger_kind: TaskTriggerKind::Immediate,
                        executor_kind: TaskExecutorKind::Agent,
                        child_thread_id: Some("child-thread".to_owned()),
                        child_turn_id: Some("child-turn".to_owned()),
                        agent_role: None,
                        depth: 0,
                        max_depth: 3,
                        next_fire_at: None,
                        progress_preview: None,
                        result_preview: None,
                        error_preview: None,
                        started_at: Some(1),
                        created_at: 1_000,
                        updated_at: 2_000,
                    },
                },
                timeline_origin: None,
                opaque_meta: None,
            }],
            ..ConversationViewState::default()
        }
    }
}
