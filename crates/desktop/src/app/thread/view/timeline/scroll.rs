use super::*;
impl PioneerDesktop {
    pub(super) fn capture_timeline_scroll_anchor_before_semantic_update(&self) {
        if self.timeline_is_near_bottom() {
            self.thread_timeline_view_state
                .borrow_mut()
                .pending_scroll_anchor = None;
            return;
        }
        let Some(thread_id) = self.current_active_thread_id().map(str::to_owned) else {
            return;
        };
        let model = self.semantic_timeline_render_model(Some(thread_id.as_str()));
        let rows = model.rows.as_ref();
        if rows.is_empty() {
            return;
        }
        let Some(item_sizes) = self
            .thread_timeline_view_state
            .borrow()
            .cached_item_sizes
            .clone()
        else {
            return;
        };
        if item_sizes.len() != rows.len() {
            return;
        }

        let viewport_top = -self.thread_timeline_scroll_handle.offset().y;
        let mut row_top = px(0.);
        for (index, row) in rows.iter().enumerate() {
            let row_height = item_sizes
                .get(index)
                .map(|size| size.height)
                .unwrap_or_else(|| px(0.));
            let row_bottom = row_top + row_height;
            if row_bottom > viewport_top {
                self.thread_timeline_view_state
                    .borrow_mut()
                    .pending_scroll_anchor = Some(TimelineScrollAnchor {
                    thread_id,
                    row_key: row.key().to_owned(),
                    row_top_offset_px: row_top - viewport_top,
                });
                return;
            }
            row_top = row_bottom;
        }
    }

    pub(super) fn restore_pending_timeline_scroll_anchor(
        &self,
        active_thread_id: Option<&str>,
        rows: &[TimelineRenderRow],
        item_sizes: &[Size<Pixels>],
    ) {
        let Some(anchor) = self
            .thread_timeline_view_state
            .borrow_mut()
            .pending_scroll_anchor
            .take()
        else {
            return;
        };
        if active_thread_id != Some(anchor.thread_id.as_str()) {
            return;
        }
        if rows.len() != item_sizes.len() {
            return;
        }

        let mut row_top = px(0.);
        for (index, row) in rows.iter().enumerate() {
            if row.key() == anchor.row_key {
                let desired_viewport_top = row_top - anchor.row_top_offset_px;
                let max_offset = self.thread_timeline_scroll_handle.max_offset().height;
                let min_offset = px(0.) - max_offset;
                let next_offset = (px(0.) - desired_viewport_top).clamp(min_offset, px(0.));
                let mut offset = self.thread_timeline_scroll_handle.offset();
                offset.y = next_offset;
                self.thread_timeline_scroll_handle.set_offset(offset);
                return;
            }
            row_top += item_sizes
                .get(index)
                .map(|size| size.height)
                .unwrap_or_else(|| px(0.));
        }
    }

    pub(super) fn sync_timeline_scroll(
        &self,
        active_thread_id: Option<&str>,
        projection: &ConversationViewState,
        rows: &[TimelineRenderRow],
    ) -> bool {
        let item_count = rows.len();

        let tail_entry_id = rows.last().map(TimelineRenderRow::key);
        let tail_text_len = rows
            .last()
            .map(|row| Self::timeline_render_row_text_len(projection, row))
            .unwrap_or_default();

        let mut state = self.thread_timeline_view_state.borrow_mut();

        let thread_changed = state.active_thread_id.as_deref() != active_thread_id;

        let timeline_changed = state.item_count != item_count
            || state.tail_entry_id.as_deref() != tail_entry_id
            || state.tail_text_len != tail_text_len;

        let force_follow = rows.iter().any(|row| {
            matches!(
                row,
                TimelineRenderRow::PendingRequest(_)
                    | TimelineRenderRow::Timeline(TimelineRow {
                        kind: TimelineRowKind::RunningTurn(_),
                        ..
                    })
            )
        });

        if thread_changed || !force_follow {
            state.autoscroll_paused_by_user = false;
        }

        let should_follow = if force_follow {
            item_count > 0 && !state.autoscroll_paused_by_user
        } else if thread_changed {
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
                .filter_map(Self::timeline_render_row_toggle_key)
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
        }

        should_follow
    }

    pub(super) fn scroll_timeline_to_bottom_for_item_sizes(&self, item_sizes: &[Size<Pixels>]) {
        let viewport_height = self.thread_timeline_scroll_handle.bounds().size.height;
        if viewport_height <= px(1.) {
            self.thread_timeline_scroll_handle.scroll_to_bottom();
            return;
        }

        let content_height = item_sizes
            .iter()
            .fold(px(0.), |height, size| height + size.height);
        let max_offset = (content_height - viewport_height).max(px(0.));
        let mut offset = self.thread_timeline_scroll_handle.offset();
        offset.y = px(0.) - max_offset;
        self.thread_timeline_scroll_handle.set_offset(offset);
    }

    pub(super) fn timeline_is_near_bottom(&self) -> bool {
        let max_offset = self.thread_timeline_scroll_handle.max_offset().height;
        if max_offset <= px(1.) {
            return true;
        }

        let current_offset = self.thread_timeline_scroll_handle.offset().y;
        let bottom_offset = px(0.) - max_offset;
        (current_offset - bottom_offset).abs() <= px(24.)
    }

    pub(super) fn on_timeline_scroll_wheel(
        &self,
        event: &ScrollWheelEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let delta_y = event.delta.pixel_delta(window.line_height()).y;
        if delta_y != px(0.) {
            let mut state = self.thread_timeline_view_state.borrow_mut();
            state.semantic_prefetch_scroll_generation =
                state.semantic_prefetch_scroll_generation.saturating_add(1);
        }

        let force_follow_active = self.semantic_timeline_has_running_turn_row()
            || !self.active_thread_pending_requests().is_empty();
        if !force_follow_active {
            return;
        }

        if delta_y > px(0.) {
            self.thread_timeline_view_state
                .borrow_mut()
                .autoscroll_paused_by_user = true;
            cx.notify();
            return;
        }

        if delta_y < px(0.) && self.timeline_scroll_wheel_reaches_bottom(delta_y) {
            self.thread_timeline_view_state
                .borrow_mut()
                .autoscroll_paused_by_user = false;
            cx.notify();
        }
    }

    fn timeline_scroll_wheel_reaches_bottom(&self, delta_y: Pixels) -> bool {
        let max_offset = self.thread_timeline_scroll_handle.max_offset().height;
        if max_offset <= px(1.) {
            return true;
        }

        let next_offset = (self.thread_timeline_scroll_handle.offset().y + delta_y)
            .clamp(px(0.) - max_offset, px(0.));
        let bottom_offset = px(0.) - max_offset;
        (next_offset - bottom_offset).abs() <= px(24.)
    }
}
