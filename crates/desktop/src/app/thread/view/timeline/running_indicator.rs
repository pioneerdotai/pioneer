use super::items::format_elapsed_ms;
use super::model::{TimelineRow, TimelineRowKind};
use crate::app::root::PioneerDesktop;
use gpui::{prelude::*, *};
use gpui_component::{StyledExt, h_flex, theme::ActiveTheme};
use pioneer_client::timeline::labels::{RunningTurnDisplay, now_unix_ms, running_turn_display};
use std::{path::PathBuf, rc::Rc, time::Duration};

fn running_turn_dino_path(is_dark: bool) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(if is_dark {
        "assets/dino-dark.webp"
    } else {
        "assets/dino-light.webp"
    })
}

impl PioneerDesktop {
    pub(super) fn running_turn_row_size(
        &self,
        is_first_row: bool,
        is_last_row: bool,
    ) -> Size<Pixels> {
        let top = if is_first_row { px(40.) } else { px(10.) };
        let bottom = if is_last_row { px(40.) } else { px(10.) };
        size(px(0.), top + px(52.) + bottom + px(1.))
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
        let dino_path = running_turn_dino_path(cx.theme().mode.is_dark());

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
                            img(dino_path)
                                .id("running-turn-dino")
                                .w_full()
                                .h_full()
                                .object_fit(ObjectFit::Contain),
                        ),
                    )
                    .child(
                        div()
                            .pt_1()
                            .font_semibold()
                            .child(t!("timeline.running.turn").to_string()),
                    ),
            )
            .child(div().pt_1().font_semibold().child(elapsed))
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
                            let visible =
                                view.active_thread_conversation()
                                    .is_some_and(|conversation| {
                                        running_turn_display(conversation.projection()).is_some()
                                    });

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
