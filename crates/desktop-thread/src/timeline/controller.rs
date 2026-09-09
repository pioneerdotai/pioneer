use super::{
    TimelineRenderRow,
    model::{TimelineRow, TimelineRowKind},
};
use crate::screen::TimelineView;
use gpui_kit::{prelude::*, *};
use pioneer_client::timeline::controller::{TimelineDemand, TimelineIntent};

/// Routes commands to the retained viewport and the process-local Client policy.
pub(crate) struct DesktopTimelineController;

#[derive(Clone, PartialEq, Action)]
#[action(namespace = thread_timeline, no_json)]
pub(crate) enum TimelineAction {
    Expand { entry_id: String },
    Scroll { delta_y: Pixels },
}

impl DesktopTimelineController {
    pub(crate) fn reconcile_publication(
        view: &mut TimelineView,
        window: &mut Window,
        cx: &mut Context<TimelineView>,
    ) -> bool {
        if view.timeline_access_revoked {
            return false;
        }
        let Some(model) = view.thread_bindings.timeline_model(Some(&view.thread_id)) else {
            let changed = view.thread_timeline_view_state.prepared.is_some()
                || view.measurement_coordinator.draw.is_some();
            view.retire_timeline_rows(cx);
            return changed;
        };
        if (view.thread_timeline_view_state.prepared.is_some()
            || view.measurement_coordinator.draw.is_some())
            && view.thread_timeline_view_state.model.revision == model.revision
        {
            return false;
        }
        view.thread_timeline_view_state.model = model;
        view.reconcile_timeline(window, cx);
        Self::schedule(view, window, cx);
        true
    }
    pub(crate) fn dispatch(
        view: &mut TimelineView,
        action: &TimelineAction,
        window: &mut Window,
        cx: &mut Context<TimelineView>,
    ) {
        match action {
            TimelineAction::Expand { entry_id } => Self::expand(view, entry_id, window, cx),
            TimelineAction::Scroll { delta_y } => {
                view.on_timeline_scroll_delta(*delta_y, cx);
                Self::schedule(view, window, cx);
            }
        }
    }
    pub(crate) fn expand(
        view: &mut TimelineView,
        entry_id: &str,
        window: &mut Window,
        cx: &mut Context<TimelineView>,
    ) {
        {
            let mut expanded = view.thread_timeline_view_state.expanded.borrow_mut();
            if !expanded.remove(entry_id) {
                expanded.insert(entry_id.to_owned());
            }
            let mut state = view.thread_timeline_view_state.borrow_mut();
            state.expanded_revision = state.expanded_revision.saturating_add(1);
        }
        view.reconcile_timeline(window, cx);
        Self::schedule(view, window, cx);
        cx.notify();
    }
    pub(crate) fn schedule(
        view: &mut TimelineView,
        window: &mut Window,
        cx: &mut Context<TimelineView>,
    ) {
        if view.thread_timeline_view_state.reconciliation_pending {
            return;
        }
        view.thread_timeline_view_state.reconciliation_pending = true;
        // Read the stock handle after its next committed layout, never from prepaint.
        let entity = cx.weak_entity();
        window.on_next_frame(move |window, _| {
            window.on_next_frame(move |window, cx| {
                let _ = entity.update(cx, |view, cx| {
                    view.thread_timeline_view_state.reconciliation_pending = false;
                    if view.thread_timeline_view_state.visible {
                        Self::viewport(view, window, cx);
                    }
                });
            });
        });
    }

    pub(crate) fn reconcile(
        view: &mut TimelineView,
        window: &mut Window,
        cx: &mut Context<TimelineView>,
    ) {
        if view.timeline_access_revoked {
            return;
        }
        if let Some(model) = view.thread_bindings.timeline_model(Some(&view.thread_id)) {
            view.thread_timeline_view_state.model = model;
        }
        view.reconcile_timeline(window, cx);
        Self::schedule(view, window, cx);
    }

    pub(crate) fn viewport(
        view: &mut TimelineView,
        window: &mut Window,
        cx: &mut Context<TimelineView>,
    ) {
        let available = view.thread_timeline_view_state.visible
            && window.is_window_active()
            && view.connection_state == crate::screen::GatewayConnectionState::Connected
            && view
                .identity_input
                .as_ref()
                .is_some_and(|input| input.current_auth.is_some());
        if !available {
            view.set_row_activities_visible(&std::collections::HashSet::new(), cx);
            view.avatar_activities.borrow_mut().set_active(false, cx);
            Self::exit(view);
        } else {
            view.avatar_activities.borrow_mut().set_active(true, cx);
        }
        let bounds = view.thread_timeline_view_state.scroll_handle.bounds();
        let rem_changed = view.thread_timeline_view_state.layout_rem != window.rem_size();
        if rem_changed {
            view.thread_timeline_view_state.layout_rem = window.rem_size();
            view.layout_store.context_changed();
        }
        let width_changed = view.update_timeline_layout_width(bounds.size.width) || rem_changed;
        let bounds_changed = bounds != view.thread_timeline_view_state.viewport;
        view.thread_timeline_view_state.viewport = bounds;
        view.thread_timeline_view_state.viewport_offset =
            view.thread_timeline_view_state.scroll_handle.offset();
        if width_changed {
            view.layout_store.context_changed();
            view.reconcile_timeline(window, cx);
            Self::schedule(view, window, cx);
        }
        if view.measurement_coordinator.draw.is_some() {
            return;
        }
        let Some(prepared) = view.thread_timeline_view_state.prepared.clone() else {
            return;
        };
        let offset = -view.thread_timeline_view_state.scroll_handle.offset().y;
        let range = prepared
            .layout_index
            .visible_range(offset, bounds.size.height);
        let range_changed = range != view.thread_timeline_view_state.visible_range;
        view.thread_timeline_view_state.visible_range = range.clone();
        if bounds_changed || range_changed {
            view.thread_timeline_view_state.viewport_revision += 1;
        }
        if available {
            view.prepare_timeline_avatars(prepared.layout_index.clone(), cx);
        }
        let ids = prepared.model.rows[range]
            .iter()
            .map(|row| row.key().to_owned())
            .collect::<Vec<_>>();
        let visible_rows = if available {
            ids.iter().cloned().collect()
        } else {
            std::collections::HashSet::new()
        };
        view.set_row_activities_visible(&visible_rows, cx);
        let mut activities = std::collections::HashSet::new();
        for row in prepared
            .model
            .rows
            .iter()
            .filter(|row| ids.iter().any(|id| id == row.key()))
        {
            let id = match row {
                TimelineRenderRow::Timeline(TimelineRow {
                    kind: TimelineRowKind::RunningTurn(turn),
                    ..
                }) => Some(format!("turn:{}", turn.turn_id)),
                TimelineRenderRow::Timeline(TimelineRow {
                    kind: TimelineRowKind::Item { timeline_index },
                    ..
                }) => prepared
                    .model
                    .projection
                    .timeline
                    .get(*timeline_index)
                    .and_then(|entry| prepared.model.projection.item_for_timeline_entry(entry))
                    .and_then(|item| match &item.item {
                        pioneer_client::timeline::types::TurnItem::Task { item } => {
                            Some(format!("task:{}", item.id))
                        }
                        _ => None,
                    }),
                _ => None,
            };
            if let Some(id) = id {
                activities.insert(format!("content:{id}"));
                activities.insert(id);
            }
        }
        for group in prepared.layout_index.grouping().avatar_groups() {
            if prepared
                .layout_index
                .avatar_group_bounds(group)
                .is_some_and(|(top, bottom)| top < offset + bounds.size.height && bottom > offset)
            {
                activities.insert(group.activity_id.clone());
            }
        }
        view.avatar_activities
            .borrow_mut()
            .set_visible_activities(&activities, cx);
        if available && let Some(workspace) = view.thread_workspace_id(&view.thread_id) {
            for row in prepared
                .model
                .rows
                .iter()
                .filter(|row| ids.iter().any(|id| id == row.key()))
            {
                let entry = match row {
                    TimelineRenderRow::Timeline(TimelineRow {
                        kind:
                            TimelineRowKind::UserMessage { timeline_index, .. }
                            | TimelineRowKind::Item { timeline_index },
                        ..
                    }) => prepared.model.projection.timeline.get(*timeline_index),
                    _ => None,
                };
                if let Some(item) =
                    entry.and_then(|entry| prepared.model.projection.item_for_timeline_entry(entry))
                    && let Some(content) = prepared
                        .model
                        .item_presentations
                        .get(&item.id)
                        .and_then(|row| row.content())
                {
                    for artifact in content
                        .attachments
                        .iter()
                        .filter_map(|a| a.artifact.as_ref())
                    {
                        view.request_thread_artifact_preview_load(&workspace, artifact, cx);
                    }
                }
            }
        }
        if !available {
            Self::exit(view);
        } else {
            let latest = prepared.model.rows.iter().rev().find_map(|row| match row {
                TimelineRenderRow::Timeline(TimelineRow {
                    kind: TimelineRowKind::UserMessage { presentation, .. },
                    ..
                }) => Some(presentation.turn_id.clone()),
                _ => None,
            });
            let scroll = view.thread_timeline_view_state.scroll_handle.clone();
            let maximum = scroll.max_offset().y;
            let demand = TimelineDemand {
                thread_id: view.thread_id.clone(),
                consumer_id: format!("desktop:{}", view.mount),
                generation: view.thread_timeline_view_state.demand_generation,
                source_revision: prepared.model.source_revision,
                row_ids: ids,
                threshold: pioneer_client::timeline::semantic::DEFAULT_PREFETCH_THRESHOLD_ROWS,
                before: maximum > px(1.) && scroll.offset().y >= px(-24.),
                after: maximum > px(1.) && view.timeline_is_near_bottom(),
                work: maximum > px(1.),
                presented_rows: false,
                scroll_generation: view
                    .thread_timeline_view_state
                    .borrow()
                    .semantic_prefetch_scroll_generation,
                viewed_through_turn_id: view
                    .timeline_is_near_bottom()
                    .then(|| latest.clone())
                    .flatten(),
                latest_user_turn_id: latest,
                read_requires_unread: false,
                prefetch_on_visibility: true,
                boundary_request_limit: 1,
            };
            view.thread_timeline_view_state.demand_active = true;
            view.client
                .timeline_intent(TimelineIntent::Update { demand });
        }
        if width_changed || bounds_changed || range_changed {
            cx.notify();
        }
    }

    pub(crate) fn exit(view: &mut TimelineView) {
        if !view.thread_timeline_view_state.demand_active {
            return;
        }
        view.thread_timeline_view_state.demand_active = false;
        view.client.timeline_intent(TimelineIntent::Exit {
            thread_id: view.thread_id.clone(),
            consumer_id: format!("desktop:{}", view.mount),
            generation: view.thread_timeline_view_state.demand_generation,
        });
        view.thread_timeline_view_state.demand_generation += 1;
    }
}
