use super::items::format_elapsed_ms;
use super::model::{TimelineRow, TimelineRowKind};
use crate::app::root::PioneerDesktop;
use gpui::{ImageSource, Resource, prelude::*, *};
use gpui_component::{StyledExt, h_flex, theme::ActiveTheme, v_flex};
use pioneer_client::timeline::labels::{RunningTurnDisplay, now_unix_ms};
use std::{rc::Rc, time::Duration};

fn running_turn_dino_image_source(is_dark: bool) -> ImageSource {
    let asset_path = if is_dark {
        "dino-dark.webp"
    } else {
        "dino-light.webp"
    };
    ImageSource::Resource(Resource::Embedded(asset_path.into()))
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

    pub(super) fn running_turn_row_size(
        &self,
        is_first_row: bool,
        is_last_row: bool,
    ) -> Size<Pixels> {
        let top = if is_first_row { px(40.) } else { px(10.) };
        let bottom = if is_last_row { px(40.) } else { px(10.) };
        size(px(0.), top + px(66.) + bottom + px(1.))
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
        let mut should_start_timer = false;
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

            if !state.running_turn_indicator_timer_active {
                state.running_turn_indicator_timer_active = true;
                should_start_timer = true;
            }

            started_at
        };

        if should_start_timer {
            self.spawn_running_turn_indicator_timer(cx);
        }

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
        is_first_row: bool,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let now = now_unix_ms();
        let started_at = running_turn.started_at_unix_ms.unwrap_or(now);
        let elapsed_ms = now.saturating_sub(started_at).max(0) as u64;
        let elapsed = if elapsed_ms >= 1_000 {
            format_elapsed_ms(elapsed_ms)
        } else {
            String::new()
        };
        let elapsed_tick = self
            .thread_timeline_view_state
            .borrow()
            .running_turn_indicator_tick;
        let elapsed_id = ElementId::from((
            ElementId::from((
                ElementId::from("running-turn-elapsed"),
                running_turn.turn_id.clone(),
            )),
            elapsed_tick.to_string(),
        ));
        let is_dark = cx.theme().mode.is_dark();
        let dino_image_source = running_turn_dino_image_source(is_dark);

        let content = h_flex()
            .w_full()
            .pt_5()
            .items_center()
            .justify_between()
            .gap_4()
            .text_sm()
            .child(
                h_flex()
                    .items_center()
                    .gap_4()
                    .child(
                        div().size_8().child(
                            img(dino_image_source)
                                .id("running-turn-dino")
                                .w_full()
                                .h_full()
                                .object_fit(ObjectFit::Contain),
                        ),
                    )
                    .child(
                        v_flex()
                            .pt_1()
                            .gap_1()
                            .child(
                                div()
                                    .font_semibold()
                                    .child(t!("timeline.running.turn").to_string()),
                            )
                            .when_some(running_turn.security_summary.as_ref(), |this, summary| {
                                this.child(self.render_turn_security_summary(summary, cx))
                            }),
                    ),
            )
            .child(div().id(elapsed_id).pt_1().font_semibold().child(elapsed))
            .into_any_element();

        self.render_item_row(is_first_row, is_last_row, content_width, content)
    }

    fn spawn_running_turn_indicator_timer(&self, cx: &mut Context<Self>) {
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();

            async move {
                loop {
                    cx.background_executor().timer(Duration::from_secs(1)).await;

                    let keep_running = this
                        .update(&mut cx, |view, cx| {
                            let visible = view.semantic_timeline_has_running_turn_row();

                            if !visible {
                                let mut state = view.thread_timeline_view_state.borrow_mut();
                                state.running_turn_indicator_timer_active = false;
                                state.running_turn_indicator_tick = 0;
                                state.running_turn_indicator_fallback_turn_id = None;
                                state.running_turn_indicator_fallback_started_at_unix_ms = None;
                            } else {
                                let mut state = view.thread_timeline_view_state.borrow_mut();
                                state.running_turn_indicator_tick =
                                    state.running_turn_indicator_tick.wrapping_add(1);
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
