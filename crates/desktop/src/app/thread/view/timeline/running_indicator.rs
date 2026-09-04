use super::TimelineRowTopSpacing;
use super::items::format_elapsed_ms;
use super::model::{TimelineRow, TimelineRowKind};
use crate::app::root::PioneerDesktop;
use gpui::{ImageSource, Resource, prelude::*, *};
use gpui_component::{StyledExt, h_flex, theme::ActiveTheme, v_flex};
use pioneer_client::security::ClientTurnSecuritySummary;
use pioneer_client::timeline::labels::{RunningTurnDisplay, now_unix_ms};
use std::{collections::HashMap, rc::Rc, time::Duration};

fn running_turn_dino_image_source(is_dark: bool) -> ImageSource {
    let asset_path = if is_dark {
        "dino-dark.webp"
    } else {
        "dino-light.webp"
    };
    ImageSource::Resource(Resource::Embedded(asset_path.into()))
}

fn render_running_turn_dino(is_dark: bool) -> AnyElement {
    // GPUI retains animated-image frame state and requests subsequent frames only
    // for stateful images with a stable global element id. This image lives in
    // its own Entity so those animation-frame notifications cannot invalidate
    // the PioneerDesktop root and rebuild the complete timeline at 60 FPS.
    img(running_turn_dino_image_source(is_dark))
        .id("running-turn-dino-frame")
        .w_full()
        .h_full()
        .object_fit(ObjectFit::Contain)
        .into_any_element()
}

pub(crate) struct RunningDinoView;

impl Render for RunningDinoView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        render_running_turn_dino(cx.theme().mode.is_dark())
    }
}

pub(crate) struct RunningElapsedView {
    started_at_unix_ms: i64,
    show_dino: bool,
}

impl RunningElapsedView {
    fn new(started_at_unix_ms: i64, show_dino: bool, cx: &mut Context<Self>) -> Self {
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                loop {
                    cx.background_executor().timer(Duration::from_secs(1)).await;
                    if this.update(&mut cx, |_view, cx| cx.notify()).is_err() {
                        break;
                    }
                }
            }
        })
        .detach();
        Self {
            started_at_unix_ms,
            show_dino,
        }
    }
}

impl Render for RunningElapsedView {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        let elapsed_ms = now_unix_ms().saturating_sub(self.started_at_unix_ms).max(0) as u64;
        let elapsed = if elapsed_ms >= 1_000 {
            format_elapsed_ms(elapsed_ms)
        } else {
            String::new()
        };

        div()
            .id("running-activity-elapsed")
            .pt_1()
            .when(!self.show_dino, |this| this.pt_0().mb(px(2.)))
            .font_semibold()
            .child(elapsed)
    }
}

struct RunningElapsedViewEntry {
    started_at_unix_ms: i64,
    show_dino: bool,
    view: WeakEntity<RunningElapsedView>,
}

#[derive(Default)]
pub(crate) struct RunningIndicatorViewCache {
    dino: HashMap<String, WeakEntity<RunningDinoView>>,
    elapsed: HashMap<String, RunningElapsedViewEntry>,
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

    pub(super) fn hydrate_running_turn_rows(
        &self,
        rows: Rc<Vec<TimelineRow>>,
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
            let mut state = self.thread_timeline_view_state.borrow_mut();
            state.running_turn_indicator_fallback_turn_id = None;
            state.running_turn_indicator_fallback_started_at_unix_ms = None;
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

    pub(super) fn running_turn_dino_view(
        &self,
        activity_id: String,
        cx: &mut Context<Self>,
    ) -> Entity<RunningDinoView> {
        let mut cache = self.running_indicator_views.borrow_mut();
        cache.dino.retain(|_, view| view.upgrade().is_some());
        if let Some(view) = cache.dino.get(&activity_id).and_then(WeakEntity::upgrade) {
            return view;
        }

        let view = cx.new(|_| RunningDinoView);
        cache.dino.insert(activity_id, view.downgrade());
        view
    }

    fn running_elapsed_view(
        &self,
        activity_id: String,
        started_at_unix_ms: i64,
        show_dino: bool,
        cx: &mut Context<Self>,
    ) -> Entity<RunningElapsedView> {
        let mut cache = self.running_indicator_views.borrow_mut();
        cache
            .elapsed
            .retain(|_, entry| entry.view.upgrade().is_some());
        if let Some(entry) = cache.elapsed.get(&activity_id)
            && entry.started_at_unix_ms == started_at_unix_ms
            && entry.show_dino == show_dino
            && let Some(view) = entry.view.upgrade()
        {
            return view;
        }

        let view = cx.new(|cx| RunningElapsedView::new(started_at_unix_ms, show_dino, cx));
        cache.elapsed.insert(
            activity_id,
            RunningElapsedViewEntry {
                started_at_unix_ms,
                show_dino,
                view: view.downgrade(),
            },
        );
        view
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
        // Timeline measurement can hold a mutable borrow of the timeline cache,
        // so animated child views use their own independent cache.
        let started_at = started_at_unix_ms.unwrap_or_else(now_unix_ms);
        let dino =
            show_dino.then(|| self.running_turn_dino_view(format!("content:{activity_id}"), cx));
        let elapsed = self.running_elapsed_view(activity_id, started_at, show_dino, cx);
        let status_label = match state {
            Some(pioneer_protocol::TurnWorkState::Starting) => {
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
                    .when_some(dino, |this, dino| this.child(div().size_8().child(dino)))
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
            .child(elapsed)
            .into_any_element()
    }
}
