use super::state::TimelinePresentationState;
use super::*;

/// Geometry of the last rendered publication, independent of request/loading state.
#[derive(Default)]
pub(crate) struct TimelineScrollState {
    layout: Option<TimelineScrollLayout>,
    expansion: Option<WorkExpansionAnchor>,
}

struct TimelineScrollLayout {
    thread_id: String,
    rows: std::sync::Arc<Vec<TimelineRenderRow>>,
    sizes: Rc<Vec<Size<Pixels>>>,
}

struct WorkExpansionAnchor {
    toggle_key: String,
    after_key: Option<String>,
    boundary_offset: Pixels,
}

impl TimelinePresentationState {
    pub(super) fn reconcile_scroll(
        &mut self,
        thread_id: Option<&str>,
        rows: std::sync::Arc<Vec<TimelineRenderRow>>,
        sizes: Rc<Vec<Size<Pixels>>>,
        handle: &gpui_kit::component::VirtualListScrollHandle,
        follow_bottom: bool,
    ) {
        self.scroll
            .reconcile(thread_id, rows, sizes, handle, follow_bottom);
    }
}

impl TimelineScrollState {
    fn cancel_expansion(&mut self) {
        self.expansion = None;
    }

    pub(super) fn set_work_expansion_anchor(
        &mut self,
        thread_id: &str,
        toggle_key: &str,
        expanded: bool,
        handle: &gpui_kit::component::VirtualListScrollHandle,
    ) {
        self.expansion = None;
        let Some(layout) = self
            .layout
            .as_ref()
            .filter(|layout| layout.thread_id == thread_id)
        else {
            return;
        };
        if !expanded {
            return;
        }
        let Some(ix) = layout.rows.iter().position(|row| row.key() == toggle_key) else {
            return;
        };
        let boundary = layout
            .sizes
            .iter()
            .take(ix + 1)
            .map(|size| size.height)
            .sum::<Pixels>();
        self.expansion = Some(WorkExpansionAnchor {
            toggle_key: toggle_key.to_owned(),
            after_key: layout.rows.get(ix + 1).map(|row| row.key().to_owned()),
            boundary_offset: boundary + handle.offset().y,
        });
    }

    pub(super) fn reconcile(
        &mut self,
        thread_id: Option<&str>,
        rows: std::sync::Arc<Vec<TimelineRenderRow>>,
        sizes: Rc<Vec<Size<Pixels>>>,
        handle: &gpui_kit::component::VirtualListScrollHandle,
        follow_bottom: bool,
    ) -> bool {
        let Some(thread_id) = thread_id else {
            return false;
        };
        let previous = self
            .layout
            .as_ref()
            .filter(|layout| layout.thread_id == thread_id);
        let Some(previous) = previous else {
            self.expansion = None;
            scroll_to_bottom(handle, &sizes);
            self.layout = Some(TimelineScrollLayout {
                thread_id: thread_id.to_owned(),
                rows,
                sizes,
            });
            return false;
        };
        if std::sync::Arc::ptr_eq(&previous.rows, &rows) && Rc::ptr_eq(&previous.sizes, &sizes) {
            if follow_bottom {
                scroll_to_bottom(handle, &sizes);
            }
            return false;
        }

        let old_keys = previous
            .rows
            .iter()
            .map(TimelineRenderRow::key)
            .collect::<HashSet<_>>();
        let inserted_rows = rows.iter().any(|row| !old_keys.contains(row.key()));
        let prepended = previous.rows.first().is_some_and(|first| {
            rows.iter()
                .position(|row| row.key() == first.key())
                .is_some_and(|ix| ix > 0)
        });
        let mut top = px(0.);
        let positions = rows
            .iter()
            .zip(sizes.iter())
            .map(|(row, size)| {
                let position = (row.key(), top);
                top += size.height;
                position
            })
            .collect::<HashMap<_, _>>();

        let expansion_offset = self.expansion.as_ref().and_then(|anchor| {
            let toggle_ix = rows.iter().position(|row| row.key() == anchor.toggle_key)?;
            let boundary = match anchor.after_key.as_deref() {
                Some(key) => *positions.get(key)?,
                None => top,
            };
            let toggle_bottom = positions[anchor.toggle_key.as_str()] + sizes[toggle_ix].height;
            // Expansion and its initial page may arrive in separate publications.
            (boundary > toggle_bottom).then_some(anchor.boundary_offset - boundary)
        });
        if let Some(offset) = expansion_offset {
            set_timeline_offset(handle, &sizes, offset);
            self.expansion = None;
        } else if follow_bottom && !prepended {
            scroll_to_bottom(handle, &sizes);
        } else {
            let viewport_top = -handle.offset().y;
            let mut old_top = px(0.);
            let mut anchor_offset = None;
            for (row, size) in previous.rows.iter().zip(previous.sizes.iter()) {
                if old_top + size.height > viewport_top
                    && let Some(new_top) = positions.get(row.key())
                {
                    let offset = old_top - viewport_top - *new_top;
                    anchor_offset.get_or_insert(offset);
                    // A Worked header stays above the inserted page. Preserve the
                    // visible work item below it, rather than anchoring that header.
                    if inserted_rows
                        && *new_top > old_top
                        && old_top < viewport_top + handle.bounds().size.height
                    {
                        anchor_offset = Some(offset);
                        break;
                    }
                }
                old_top += size.height;
            }
            if let Some(offset) = anchor_offset {
                set_timeline_offset(handle, &sizes, offset);
            }
        }
        self.layout = Some(TimelineScrollLayout {
            thread_id: thread_id.to_owned(),
            rows,
            sizes,
        });
        inserted_rows
    }
}

fn set_timeline_offset(
    handle: &gpui_kit::component::VirtualListScrollHandle,
    sizes: &[Size<Pixels>],
    y: Pixels,
) {
    let max_offset = timeline_max_offset_for_item_sizes(sizes, handle.bounds().size.height);
    let mut offset = handle.offset();
    offset.y = y.clamp(-max_offset, px(0.));
    handle.set_offset(offset);
}

fn scroll_to_bottom(handle: &gpui_kit::component::VirtualListScrollHandle, sizes: &[Size<Pixels>]) {
    if handle.bounds().size.height <= px(1.) {
        // The handle's item count still belongs to the previous frame.
        handle.scroll_to_item(sizes.len().saturating_sub(1), ScrollStrategy::Top);
    } else {
        set_timeline_offset(
            handle,
            sizes,
            -timeline_max_offset_for_item_sizes(sizes, handle.bounds().size.height),
        );
    }
}

impl TimelineView {
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
                TimelineRenderRow::Timeline(TimelineRow {
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

        drop(state);

        if thread_changed {
            let mut expanded = self.thread_timeline_view_state.expanded.borrow_mut();
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
                let mut expanded = self.thread_timeline_view_state.expanded.borrow_mut();
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

    pub(crate) fn timeline_is_near_bottom(&self) -> bool {
        let max_offset = self.thread_timeline_view_state.scroll_handle.max_offset().y;
        if max_offset <= px(1.) {
            return true;
        }

        let current_offset = self.thread_timeline_view_state.scroll_handle.offset().y;
        let bottom_offset = px(0.) - max_offset;
        (current_offset - bottom_offset).abs() <= px(24.)
    }

    pub(super) fn on_timeline_scroll_delta(&self, delta_y: Pixels, cx: &mut Context<Self>) {
        if delta_y != px(0.) {
            let mut state = self.thread_timeline_view_state.borrow_mut();
            state.scroll.cancel_expansion();
            state.pending_follow_bottom = false;
            record_semantic_prefetch_scroll_intent(&mut state);
        }

        let force_follow_active = self.semantic_timeline_has_running_turn_row();
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
        let max_offset = self.thread_timeline_view_state.scroll_handle.max_offset().y;
        if max_offset <= px(1.) {
            return true;
        }

        let next_offset = (self.thread_timeline_view_state.scroll_handle.offset().y + delta_y)
            .clamp(px(0.) - max_offset, px(0.));
        let bottom_offset = px(0.) - max_offset;
        (next_offset - bottom_offset).abs() <= px(24.)
    }

    pub(super) fn consume_all_semantic_prefetch_scroll_intents(&self) {
        self.client.timeline_intent(
            pioneer_client::timeline::controller::TimelineIntent::ConsumeScroll {
                thread_id: self.thread_id.clone(),
                consumer_id: format!("desktop:{}", self.mount),
                generation: self.thread_timeline_view_state.demand_generation,
                scroll_generation: self
                    .thread_timeline_view_state
                    .borrow()
                    .semantic_prefetch_scroll_generation,
            },
        );
    }
}

fn record_semantic_prefetch_scroll_intent(state: &mut TimelinePresentationState) {
    state.semantic_prefetch_scroll_generation =
        state.semantic_prefetch_scroll_generation.saturating_add(1);
}

fn timeline_max_offset_for_item_sizes(
    item_sizes: &[Size<Pixels>],
    viewport_height: Pixels,
) -> Pixels {
    let content_height = item_sizes
        .iter()
        .fold(px(0.), |height, size| height + size.height);
    (content_height - viewport_height).max(px(0.))
}

#[cfg(test)]
mod tests {
    use super::{TimelinePresentationState, timeline_max_offset_for_item_sizes};
    use super::{TimelineRenderRow, TimelineRow, TimelineRowKind};
    use gpui_kit::component::{VirtualListScrollHandle, v_virtual_list};
    use gpui_kit::{
        Context, Entity, IntoElement, Pixels, Render, Size, TestAppContext, VisualTestContext,
        Window, div, point, prelude::*, px, size,
    };
    use std::{rc::Rc, sync::Arc};

    struct ScrollHarness {
        thread_id: String,
        rows: Arc<Vec<TimelineRenderRow>>,
        sizes: Rc<Vec<Size<Pixels>>>,
        handle: VirtualListScrollHandle,
        state: TimelinePresentationState,
        follow_bottom: bool,
    }

    impl ScrollHarness {
        fn page(&mut self, keys: &[&str]) {
            self.rows = Arc::new(
                keys.iter()
                    .map(|key| {
                        TimelineRenderRow::Timeline(TimelineRow {
                            key: (*key).to_owned(),
                            author: None,
                            kind: TimelineRowKind::Item { timeline_index: 0 },
                        })
                    })
                    .collect(),
            );
            self.sizes = Rc::new(vec![size(px(200.), px(40.)); keys.len()]);
        }
    }

    impl Render for ScrollHarness {
        fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            self.state.reconcile_scroll(
                Some(&self.thread_id),
                self.rows.clone(),
                self.sizes.clone(),
                &self.handle,
                self.follow_bottom,
            );
            v_virtual_list(
                cx.entity(),
                "scroll-regression",
                self.sizes.clone(),
                |_, range, _, _| {
                    range
                        .map(|_| div().w(px(200.)).h(px(40.)))
                        .collect::<Vec<_>>()
                },
            )
            .w(px(200.))
            .h(px(120.))
            .track_scroll(&self.handle)
        }
    }

    fn harness(cx: &mut TestAppContext) -> (Entity<ScrollHarness>, &mut VisualTestContext) {
        cx.add_window_view(|_, _| {
            let mut harness = ScrollHarness {
                thread_id: "a".into(),
                rows: Default::default(),
                sizes: Default::default(),
                handle: VirtualListScrollHandle::new(),
                state: TimelinePresentationState::default(),
                follow_bottom: false,
            };
            harness.page(&["a", "b", "c", "d", "e", "f"]);
            harness
        })
    }

    #[gpui_kit::test]
    fn first_frame_and_thread_switch_open_at_latest_message(cx: &mut TestAppContext) {
        let (view, cx) = harness(cx);
        cx.update(|window, cx| window.draw(cx).clear(cx));
        let handle = view.read_with(cx, |view, _| view.handle.clone());
        assert_eq!(handle.offset().y, -handle.max_offset().y);
        assert!(handle.offset().y < px(0.));
        handle.set_offset(point(px(0.), px(0.)));
        view.update(cx, |view, cx| {
            view.thread_id = "b".into();
            view.page(&["x", "y", "z", "u", "v"]);
            cx.notify();
        });
        cx.update(|window, cx| window.draw(cx).clear(cx));
        assert_eq!(handle.offset().y, -handle.max_offset().y);
    }

    #[gpui_kit::test]
    fn prepend_preserves_current_position_after_loading_frames_and_more_scrolling(
        cx: &mut TestAppContext,
    ) {
        let (view, cx) = harness(cx);
        cx.update(|window, cx| window.draw(cx).clear(cx));
        let handle = view.read_with(cx, |view, _| view.handle.clone());
        handle.set_offset(point(px(0.), px(-15.)));
        // An intermediate publication changes loading state but has the same rows.
        view.update(cx, |view, cx| {
            view.page(&["a", "b", "c", "d", "e", "f"]);
            cx.notify();
        });
        cx.update(|window, cx| window.draw(cx).clear(cx));
        handle.set_offset(point(px(0.), px(-7.)));
        view.update(cx, |view, cx| {
            view.page(&["older-2", "older-1", "a", "b", "c", "d", "e", "f"]);
            cx.notify();
        });
        cx.update(|window, cx| window.draw(cx).clear(cx));
        assert_eq!(handle.offset().y, px(-87.));
        cx.update(|window, cx| window.draw(cx).clear(cx));
        assert_eq!(handle.offset().y, px(-87.));
    }

    #[gpui_kit::test]
    fn worked_expansion_keeps_group_end_and_prepended_items_keep_their_position(
        cx: &mut TestAppContext,
    ) {
        let (view, cx) = harness(cx);
        view.update(cx, |view, _| {
            view.page(&["message", "worked", "answer", "next-1", "next-2"])
        });
        cx.update(|window, cx| window.draw(cx).clear(cx));
        let handle = view.read_with(cx, |view, _| view.handle.clone());
        handle.set_offset(point(px(0.), px(0.)));
        view.update(cx, |view, cx| {
            view.state
                .scroll
                .set_work_expansion_anchor("a", "worked", true, &view.handle);
            view.page(&["message", "worked", "answer", "next-1", "next-2"]);
            cx.notify();
        });
        cx.update(|window, cx| window.draw(cx).clear(cx));
        assert_eq!(handle.offset().y, px(0.));
        view.update(cx, |view, cx| {
            view.page(&[
                "message", "worked", "work-3", "work-4", "work-5", "answer", "next-1", "next-2",
            ]);
            cx.notify();
        });
        cx.update(|window, cx| window.draw(cx).clear(cx));
        // The answer still starts at y=80: the end of Worked stays in place.
        assert_eq!(handle.offset().y, px(-120.));
        handle.set_offset(point(px(0.), px(-45.)));
        view.update(cx, |view, cx| {
            view.page(&[
                "message", "worked", "work-1", "work-2", "work-3", "work-4", "work-5", "answer",
                "next-1", "next-2",
            ]);
            cx.notify();
        });
        cx.update(|window, cx| window.draw(cx).clear(cx));
        assert_eq!(handle.offset().y, px(-125.));
    }

    #[test]
    fn anchor_clamp_uses_new_item_sizes_after_prepend() {
        let item_sizes = vec![
            size(px(100.), px(40.)),
            size(px(100.), px(50.)),
            size(px(100.), px(60.)),
        ];

        assert_eq!(
            timeline_max_offset_for_item_sizes(&item_sizes, px(80.)),
            px(70.)
        );
    }
}
