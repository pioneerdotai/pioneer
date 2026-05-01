use super::model::{TimelineRow, timeline_row_text_len, timeline_row_toggle_key};
use super::*;

impl PioneerDesktop {
    pub(super) fn sync_timeline_scroll(
        &self,
        active_thread_id: Option<&str>,
        projection: &ConversationViewState,
        rows: &[TimelineRow],
    ) {
        let item_count = rows.len();

        let tail_entry_id = rows.last().map(|row| row.key.as_str());
        let tail_text_len = rows
            .last()
            .map(|row| timeline_row_text_len(projection, row))
            .unwrap_or_default();

        let mut state = self.thread_timeline_view_state.borrow_mut();

        let thread_changed = state.active_thread_id.as_deref() != active_thread_id;

        let timeline_changed = state.item_count != item_count
            || state.tail_entry_id.as_deref() != tail_entry_id
            || state.tail_text_len != tail_text_len;

        let should_follow = if thread_changed {
            item_count > 0
        } else {
            timeline_changed && item_count > 0 && self.timeline_is_near_bottom()
        };

        state.active_thread_id = active_thread_id.map(str::to_owned);
        state.item_count = item_count;
        state.tail_entry_id = tail_entry_id.map(str::to_owned);
        state.tail_text_len = tail_text_len;

        if thread_changed {
            state.entry_layout_cache.clear();
            state.cached_item_sizes = None;
        } else if timeline_changed {
            let live_row_keys = rows
                .iter()
                .map(|row| row.key.as_str())
                .collect::<HashSet<_>>();
            state
                .entry_layout_cache
                .retain(|row_key, _| live_row_keys.contains(row_key.as_str()));
            state.cached_item_sizes = None;
        }

        drop(state);

        if thread_changed {
            let mut expanded = self.thread_timeline_item_expanded.borrow_mut();
            if !expanded.is_empty() {
                let mut state = self.thread_timeline_view_state.borrow_mut();
                state.expanded_revision = state.expanded_revision.saturating_add(1);
            }
            expanded.clear();

            let mut terminal_views = self.thread_timeline_terminal_item.borrow_mut();
            terminal_views.clear();
        } else if timeline_changed {
            let live_entry_ids = projection
                .timeline
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<HashSet<_>>();
            let live_expand_keys = rows
                .iter()
                .filter_map(timeline_row_toggle_key)
                .chain(live_entry_ids.iter().copied())
                .collect::<HashSet<_>>();

            {
                let mut expanded = self.thread_timeline_item_expanded.borrow_mut();
                let before = expanded.len();
                expanded.retain(|key| live_expand_keys.contains(key.as_str()));
                if expanded.len() != before {
                    let mut state = self.thread_timeline_view_state.borrow_mut();
                    state.expanded_revision = state.expanded_revision.saturating_add(1);
                }
            }

            let mut terminal_views = self.thread_timeline_terminal_item.borrow_mut();
            terminal_views.retain(|entry_id, _| live_entry_ids.contains(entry_id.as_str()));
        }

        if should_follow {
            self.thread_timeline_scroll_handle.scroll_to_bottom();
        }
    }

    fn timeline_is_near_bottom(&self) -> bool {
        let max_offset = self.thread_timeline_scroll_handle.max_offset().height;
        if max_offset <= px(1.) {
            return true;
        }

        let current_offset = self.thread_timeline_scroll_handle.offset().y;
        let bottom_offset = px(0.) - max_offset;
        (current_offset - bottom_offset).abs() <= px(24.)
    }
}
