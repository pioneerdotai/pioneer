mod items;
mod markdown;
pub(crate) mod model;
mod scroll;
mod view;

use self::model::TimelineRow;
use crate::app::{
    conversation::{ConversationViewState, ItemView},
    root::{CachedTimelineEntryLayout, PioneerDesktop, ThreadTimelineViewState},
};
use gpui::{prelude::*, *};
use std::{collections::HashSet, rc::Rc};

impl PioneerDesktop {
    fn sync_timeline_layout_width(&self, cx: &mut Context<Self>) {
        let measured_width = self.thread_timeline_scroll_handle.bounds().size.width;
        let mut should_notify = false;

        {
            let mut state = self.thread_timeline_view_state.borrow_mut();
            if measured_width > px(1.) {
                state.pending_width_probe = false;
                state.width_probe_attempts = 0;
                if (state.measured_list_width - measured_width).abs() > px(1.) {
                    state.measured_list_width = measured_width;
                    state.entry_layout_cache.clear();
                    state.cached_item_sizes = None;
                }
            } else if state.measured_list_width <= px(1.) {
                if state.width_probe_attempts < 12 {
                    state.width_probe_attempts = state.width_probe_attempts.saturating_add(1);
                    state.pending_width_probe = true;
                    should_notify = true;
                }
            }
        }

        if should_notify {
            cx.notify();
        }
    }

    fn timeline_entry_text(item_view: &ItemView) -> &str {
        pioneer_client::timeline::labels::timeline_entry_text(item_view)
    }

    fn timeline_row_layout_hash(
        &self,
        projection: &ConversationViewState,
        row: &TimelineRow,
        expanded: &HashSet<String>,
    ) -> u64 {
        model::timeline_row_layout_hash(projection, row, expanded)
    }

    fn timeline_content_width(&self, window: &Window) -> Pixels {
        let measured_width = self.thread_timeline_scroll_handle.bounds().size.width;
        if measured_width > px(1.) {
            return measured_width.max(px(280.));
        }

        let cached_width = self.thread_timeline_view_state.borrow().measured_list_width;
        if cached_width > px(1.) {
            return cached_width.max(px(280.));
        }

        let fallback_window_width = match window.window_bounds() {
            WindowBounds::Windowed(bounds)
            | WindowBounds::Maximized(bounds)
            | WindowBounds::Fullscreen(bounds) => bounds.size.width,
        };
        if fallback_window_width > px(1.) {
            return fallback_window_width.max(px(280.));
        }

        px(320.)
    }

    fn timeline_entry_content_width(&self, list_width: Pixels) -> Pixels {
        list_width.max(px(1.)).min(px(800.))
    }

    fn measure_timeline_row_size(
        &self,
        projection: &ConversationViewState,
        row: &TimelineRow,
        is_first_row: bool,
        is_last_row: bool,
        row_width: Pixels,
        content_width: Pixels,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Size<Pixels> {
        let mut row_element = self.render_timeline_row(
            projection,
            row,
            is_first_row,
            is_last_row,
            content_width,
            cx,
        );
        let measured = row_element.layout_as_root(
            size(
                AvailableSpace::Definite(row_width),
                AvailableSpace::MaxContent,
            ),
            window,
            cx,
        );

        size(px(0.), (measured.height + px(1.)).max(px(1.)))
    }

    fn cached_or_measure_timeline_row_size(
        &self,
        state: &mut ThreadTimelineViewState,
        projection: &ConversationViewState,
        row: &TimelineRow,
        is_first_row: bool,
        is_last_row: bool,
        row_width: Pixels,
        content_width: Pixels,
        expanded: &HashSet<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Size<Pixels> {
        let mut layout_hash = self.timeline_row_layout_hash(projection, row, expanded);
        if is_first_row {
            layout_hash = layout_hash.wrapping_add(1);
        }
        if is_last_row {
            layout_hash = layout_hash.wrapping_add(2);
        }

        if let Some(cached) = state.entry_layout_cache.get(row.key.as_str())
            && cached.layout_hash == layout_hash
        {
            return size(px(0.), cached.height.max(px(1.)));
        }

        let measured = self.measure_timeline_row_size(
            projection,
            row,
            is_first_row,
            is_last_row,
            row_width,
            content_width,
            window,
            cx,
        );
        state.entry_layout_cache.insert(
            row.key.clone(),
            CachedTimelineEntryLayout {
                layout_hash,
                height: measured.height,
            },
        );
        measured
    }

    fn compute_timeline_item_sizes(
        &self,
        state: &mut ThreadTimelineViewState,
        projection: &ConversationViewState,
        rows: &[TimelineRow],
        row_width: Pixels,
        content_width: Pixels,
        expanded: &HashSet<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Rc<Vec<Size<Pixels>>> {
        let row_len = rows.len();
        Rc::new(
            rows.iter()
                .enumerate()
                .map(|(ix, row)| {
                    self.cached_or_measure_timeline_row_size(
                        state,
                        projection,
                        row,
                        ix == 0,
                        ix + 1 == row_len,
                        row_width,
                        content_width,
                        expanded,
                        window,
                        cx,
                    )
                })
                .collect::<Vec<_>>(),
        )
    }
}
