use super::TimelineGrouping;
use super::TimelineLayoutIndex;
use super::TimelinePresentationContext;
use super::TimelineRenderModel;
use super::markdown::timeline_message_text_bottom_inset;
use crate::screen::TimelineView;
use gpui_kit::component::scroll::Scrollbar;
use gpui_kit::component::v_flex;
use gpui_kit::component::v_virtual_list;
use gpui_kit::prelude::*;
use gpui_kit::*;

#[derive(Clone)]
pub(crate) struct PreparedTimeline {
    pub(crate) row_inputs: Vec<(
        super::layout_store::RowMeasurementKey,
        Option<pioneer_client::timeline::types::TurnAuthorSnapshot>,
    )>,
    pub(crate) model: TimelineRenderModel,
    pub(crate) expanded: std::rc::Rc<std::collections::HashSet<String>>,
    pub(crate) slots: Vec<std::sync::Arc<super::row_registry::TimelineRowSlotView>>,
    pub(crate) grouping: std::rc::Rc<TimelineGrouping>,
    pub(crate) item_sizes: std::rc::Rc<Vec<Size<Pixels>>>,
    pub(crate) layout_index: std::rc::Rc<TimelineLayoutIndex>,
    pub(crate) content_width: Pixels,
    pub(crate) list_width: Pixels,
}

impl TimelineView {
    pub(crate) fn reconcile_timeline(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let model = self.thread_timeline_view_state.model.clone();
        let changes = self.thread_bindings.take_timeline_changes();
        if let Some(snapshot) = &model.snapshot {
            if !self.row_registry.matches(snapshot) {
                for change in changes {
                    if !self.row_registry.apply(&change) {
                        break;
                    }
                }
                if !self.row_registry.matches(snapshot) {
                    self.row_registry.reset(snapshot);
                }
            }
        }

        // An old painted snapshot can remain while the replacement is measured.
        // Its clocks must already stop on removal or a changed semantic source.
        if let Some(previous) = &self.thread_timeline_view_state.prepared {
            for slot in &previous.slots {
                let still_current =
                    self.row_registry
                        .get(slot.snapshot().id())
                        .is_some_and(|next| {
                            slot.view.as_ref().is_some_and(|owner| {
                                next.view.as_ref() == Some(owner)
                                    && owner.read(cx).matches_activity(next.snapshot())
                            })
                        });
                if !still_current {
                    if let Some(owner) = &slot.view {
                        owner.update(cx, |owner, cx| owner.set_visible(false, cx));
                    }
                }
            }
        }

        let thread_id = self.thread_id.clone();
        let active_thread_id = Some(thread_id.as_str());
        let projection = model.projection.clone();

        // First composition mounts the same stock list to obtain its actual
        // pane width. Window width includes sidebars and is not a row constraint.
        if self
            .thread_timeline_view_state
            .scroll_handle
            .bounds()
            .size
            .width
            <= px(1.)
        {
            return;
        }
        let list_width = self.timeline_content_width(window);
        let content_width = self.timeline_entry_content_width(list_width);

        let rows = model.rows.clone();

        let should_follow_bottom =
            self.sync_timeline_scroll(active_thread_id, projection.as_ref(), rows.as_ref());
        // Coalescing layout inputs must retain an already requested follow until
        // commit. A user scroll event can cancel it while measurement is pending.
        self.thread_timeline_view_state
            .borrow_mut()
            .pending_follow_bottom |= should_follow_bottom;
        let render_current_principal_id = self
            .identity_input
            .as_ref()
            .and_then(|input| input.current_auth.as_ref())
            .map(|auth| auth.principal.id.as_str().to_owned());
        let presentation_context = TimelinePresentationContext {
            task_child_thread: self.active_task_thread_navigation().is_some(),
        };

        let message_text_bottom_inset = timeline_message_text_bottom_inset(window);
        let grouping = TimelineGrouping::from_snapshot(
            rows.as_ref(),
            model.groups.as_ref(),
            projection.as_ref(),
            render_current_principal_id.as_deref(),
            presentation_context,
            message_text_bottom_inset,
        );
        let measurement = self.prepare_timeline_item_sizes(
            &model,
            &grouping,
            list_width,
            content_width,
            window,
            cx,
        );
        let row_inputs = measurement.row_inputs.clone();
        let order = model
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.rows().iter().map(|row| row.id().clone()).collect())
            .unwrap_or_default();
        let expanded = std::rc::Rc::new(self.thread_timeline_view_state.expanded.borrow().clone());
        let ticket = self
            .measurement_coordinator
            .begin(model.revision, self.layout_store.context_revision);
        if measurement.entries.is_empty() {
            self.measurement_coordinator.accept(ticket);
            let item_sizes = self.layout_store.commit(order, Vec::new());
            let layout_index =
                TimelineLayoutIndex::from_store(grouping.clone(), self.layout_store.index.clone());
            self.commit_timeline_layout(
                PreparedTimeline {
                    row_inputs,
                    expanded,
                    slots: self.row_registry.slots(),
                    model,
                    grouping,
                    item_sizes,
                    layout_index,
                    content_width,
                    list_width,
                },
                cx,
            );
            return;
        }
        let entity = cx.weak_entity();
        self.measurement_coordinator.draw = Some(std::rc::Rc::new(std::cell::RefCell::new(Some(
            Box::new(move |window, cx| {
                let measured_rem = window.rem_size();
                let measured_style = window.text_style();
                let measured_locale = rust_i18n::locale().to_string();
                let (measured, bodies) = measurement.measure(window, cx);
                window.defer(cx, move |window, cx| {
                    let _ = entity.update(cx, |view, cx| {
                        if view.thread_timeline_view_state.model.revision != ticket.presentation
                            || view.layout_store.context_revision != ticket.context
                            || !view.measurement_coordinator.is_pending(ticket)
                        {
                            return;
                        }
                        if view.thread_timeline_view_state.layout_rem != measured_rem
                            || view.thread_timeline_view_state.layout_text_style != measured_style
                            || view.thread_timeline_view_state.layout_locale != measured_locale
                        {
                            view.thread_timeline_view_state.layout_rem = measured_rem;
                            view.thread_timeline_view_state.layout_text_style = measured_style;
                            view.thread_timeline_view_state.layout_locale = measured_locale;
                            view.layout_store.context_changed();
                            view.reconcile_timeline(window, cx);
                            cx.notify();
                            return;
                        }
                        view.measurement_coordinator.accept(ticket);
                        view.measurement_coordinator.draw = None;
                        view.layout_store.body_heights.extend(bodies);
                        let item_sizes = view.layout_store.commit(order, measured);
                        let layout_index = TimelineLayoutIndex::from_store(
                            grouping.clone(),
                            view.layout_store.index.clone(),
                        );
                        view.commit_timeline_layout(
                            PreparedTimeline {
                                row_inputs,
                                expanded,
                                slots: view.row_registry.slots(),
                                model,
                                grouping,
                                item_sizes,
                                layout_index,
                                content_width,
                                list_width,
                            },
                            cx,
                        );
                        super::controller::DesktopTimelineController::schedule(view, window, cx);
                        cx.notify();
                    });
                });
            }),
        ))));
    }

    fn commit_timeline_layout(&mut self, mut prepared: PreparedTimeline, cx: &mut Context<Self>) {
        let projection = &prepared.model.projection;
        let item_presentations = &prepared.model.item_presentations;
        let live_terminals = projection
            .timeline
            .iter()
            .filter(|entry| {
                projection
                    .item_for_timeline_entry(entry)
                    .is_some_and(|item| {
                        matches!(
                            item.item,
                            pioneer_client::timeline::types::TurnItem::CommandExecution { .. }
                        )
                    })
            })
            .map(|entry| entry.id.as_str())
            .collect::<std::collections::HashSet<_>>();
        self.thread_timeline_terminal_item
            .borrow_mut()
            .retain(|id| live_terminals.contains(id));
        let live_highlights = item_presentations
            .values()
            .filter_map(|row| row.content())
            .filter(|content| !content.streaming)
            .filter_map(|content| content.markdown_presentation.as_ref())
            .flat_map(|document| {
                document.code_blocks().into_iter().map(|node| {
                    super::markdown::markdown_node_interaction_id(&document.document_id, node.id)
                })
            })
            .collect::<std::collections::HashSet<_>>();
        self.markdown_highlights
            .borrow_mut()
            .retain(|id, _| live_highlights.contains(id));

        let old_slots = self
            .thread_timeline_view_state
            .prepared
            .as_ref()
            .map(|old| {
                old.slots
                    .iter()
                    .map(|slot| (slot.snapshot().id(), slot))
                    .collect::<std::collections::HashMap<_, _>>()
            })
            .unwrap_or_default();
        let context = (
            prepared.content_width,
            self.layout_store.theme_revision,
            self.thread_timeline_view_state.layout_text_style.clone(),
        );
        let context_changed = self.retained_context.as_ref() != Some(&context);
        for slot in &mut prepared.slots {
            let unchanged = old_slots
                .get(slot.snapshot().id())
                .is_some_and(|old| std::sync::Arc::ptr_eq(old, slot));
            if unchanged && !context_changed {
                continue;
            }
            let snapshot = slot.snapshot().clone();
            if let Some(item) = snapshot.item() {
                if matches!(
                    item.item,
                    pioneer_client::timeline::types::TurnItem::CommandExecution { .. }
                ) {
                    let entry = pioneer_client::conversation::TimelineEntry {
                        id: slot.snapshot().id().as_str().to_owned(),
                        turn_id: item.turn_id.clone(),
                        item_id: item.id.clone(),
                        item_index: 0,
                    };
                    let terminal = self.prepare_command_terminal(
                        &entry,
                        item,
                        prepared.content_width,
                        old_slots
                            .get(snapshot.id())
                            .and_then(|slot| slot.terminal.as_ref()),
                        cx,
                    );
                    std::sync::Arc::make_mut(slot).terminal = Some(terminal);
                    self.row_registry.publish_slot(slot.clone());
                }
                if let Some(content) = slot
                    .snapshot()
                    .content()
                    .filter(|content| !content.streaming)
                {
                    if let Some(document) = &content.markdown_presentation {
                        self.prepare_markdown_highlights(document, cx);
                    }
                }
            }
        }
        for (slot, (key, author)) in prepared.slots.iter_mut().zip(&prepared.row_inputs) {
            if slot
                .view
                .as_ref()
                .is_some_and(|view| view.read(cx).matches_input(key))
            {
                continue;
            }
            let presentation = super::row_view::RowPresentation::new(
                slot.clone(),
                key.clone(),
                author.clone(),
                self,
                cx,
            );
            let view = if let Some(view) = &slot.view {
                view.update(cx, |view, cx| {
                    view.synchronize(
                        presentation,
                        self.layout_store.body_heights.get(&key.id).copied(),
                        cx,
                    )
                });
                view.clone()
            } else {
                cx.new(|cx| {
                    super::row_view::TimelineRowView::new(
                        presentation,
                        self.layout_store.body_heights.get(&key.id).copied(),
                        cx,
                    )
                })
            };
            std::sync::Arc::make_mut(slot).view = Some(view);
            self.row_registry.publish_slot(slot.clone());
        }
        self.retained_context = Some(context);

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
        let measurement = self.measurement_coordinator.draw.clone();
        let scroll = self.thread_timeline_view_state.scroll_handle.clone();
        let bounds = self.thread_timeline_view_state.viewport;
        let offset = self.thread_timeline_view_state.viewport_offset;
        let rem = self.thread_timeline_view_state.layout_rem;
        let text_style = self.thread_timeline_view_state.layout_text_style.clone();
        let locale = self.thread_timeline_view_state.layout_locale.clone();
        let visible = self.thread_timeline_view_state.visible;
        let entity = cx.weak_entity();
        canvas(
            move |_, window, cx| {
                // Only the single-use draw payload is consumed here. Layout, scroll,
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
                        || window.rem_size() != rem
                        || window.text_style() != text_style
                        || rust_i18n::locale().as_bytes() != locale.as_bytes())
                {
                    let text_style = window.text_style();
                    let locale = rust_i18n::locale().to_string();
                    window.defer(cx, move |window, cx| {
                        let _ = entity.update(cx, |view, cx| {
                            if view.thread_timeline_view_state.visible {
                                if view.thread_timeline_view_state.layout_text_style != text_style
                                    || view.thread_timeline_view_state.layout_locale != locale
                                {
                                    view.thread_timeline_view_state.layout_text_style = text_style;
                                    view.thread_timeline_view_state.layout_locale = locale;
                                    view.layout_store.context_changed();
                                    view.reconcile_timeline(window, cx);
                                }
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
            let empty = self.thread_timeline_view_state.model.rows.is_empty();
            return div()
                .debug_selector(|| "thread-empty-timeline".into())
                .relative()
                .size_full()
                .child(
                    v_virtual_list(
                        cx.entity(),
                        "thread-timeline-virtual-list",
                        std::rc::Rc::new(Vec::new()),
                        |_, _, _, _| Vec::<AnyElement>::new(),
                    )
                    .gap_0()
                    .p_0()
                    .with_sizing_behavior(ListSizingBehavior::Auto)
                    .track_scroll(&self.thread_timeline_view_state.scroll_handle),
                )
                .when(empty, |this| {
                    this.child(
                        v_flex()
                            .absolute()
                            .size_full()
                            .justify_center()
                            .items_center()
                            .text_sm()
                            .opacity(0.6)
                            .child(t!("timeline.empty.start_thread").to_string()),
                    )
                })
                .child(self.timeline_measurement_pass(cx))
                .into_any_element();
        };
        let PreparedTimeline {
            row_inputs: _,
            expanded: _,
            slots,
            model: _,
            grouping: _,
            item_sizes,
            layout_index,
            content_width,
            list_width,
        } = prepared;
        let render_sizes = item_sizes.clone();
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
                    move |_, visible_range, _, cx| {
                        let visible_indices = visible_range.collect::<Vec<_>>();
                        pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_timeline(
                            pioneer_client::timeline::diagnostics::TimelineStage::VisibleRowTraversal,
                            pioneer_client::timeline::diagnostics::DiagnosticAction::Executed,
                            u64::try_from(visible_indices.len()).unwrap_or(u64::MAX),
                        ));
                        let elements = visible_indices
                            .into_iter()
                            .map(|ix| slots[ix].render(render_sizes[ix].height, cx))
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
}
