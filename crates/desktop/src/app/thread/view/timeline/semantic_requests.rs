use crate::app::root::{GatewayConnectionState, PioneerDesktop};
use gpui_kit::Context;
use pioneer_client::timeline::semantic::{
    self, DEFAULT_PREFETCH_THRESHOLD_ROWS, DEFAULT_TOP_LEVEL_PAGE_LIMIT,
    SemanticTimelineRequestAction, SemanticTimelineRequestKey, TopLevelPageMergeMode,
    WorkPageMergeMode,
};
use pioneer_protocol::{ThreadTimelinePageParams, TimelinePageAnchor};

use super::semantic_adapter::SEMANTIC_TURN_WORK_GROUP_PREFIX;

impl PioneerDesktop {
    pub(crate) fn request_semantic_thread_newest_page(
        &mut self,
        thread_id: String,
        cx: &mut Context<Self>,
    ) {
        self.execute_semantic_timeline_action(
            SemanticTimelineRequestAction::ThreadTimelinePage {
                key: SemanticTimelineRequestKey::ThreadNewest {
                    thread_id: thread_id.clone(),
                },
                params: ThreadTimelinePageParams {
                    thread_id,
                    anchor: TimelinePageAnchor::Newest,
                    limit: Some(DEFAULT_TOP_LEVEL_PAGE_LIMIT),
                },
            },
            cx,
        );
    }

    pub(crate) fn reconcile_semantic_timeline_after_reconnect(&mut self, _cx: &mut Context<Self>) {
        let Some(thread_id) = self.current_active_thread_id().map(str::to_owned) else {
            return;
        };
        self.capture_timeline_scroll_anchor_before_semantic_update(false);
        self.gateway
            .client_runtime
            .client_core()
            .refresh_thread_timeline(&thread_id);
    }

    pub(super) fn request_semantic_timeline_prefetch_for_visible_rows(
        &mut self,
        thread_id: &str,
        row_ids: &[String],
        source_revision: u64,
        cx: &mut Context<Self>,
    ) {
        if self.current_active_thread_id() != Some(thread_id) {
            return;
        }
        let core = self.gateway.client_runtime.client_core();
        let actions = core.plan_thread_timeline_viewport(
            thread_id,
            source_revision,
            row_ids,
            DEFAULT_PREFETCH_THRESHOLD_ROWS,
            self.semantic_prefetch_can_request_thread_before(),
            self.semantic_prefetch_can_request_thread_after(),
            self.semantic_prefetch_can_request_work_range(),
            false,
        );

        if actions.is_empty() {
            return;
        }

        let allow_thread_before = self.semantic_prefetch_can_request_thread_before();
        let allow_thread_after = self.semantic_prefetch_can_request_thread_after();
        let allow_work_prefetch = self.semantic_prefetch_can_request_work_range();
        let scroll_generation = self
            .thread_timeline_view_state
            .borrow()
            .semantic_prefetch_scroll_generation;
        let consumed_scroll_generation = self
            .thread_timeline_view_state
            .borrow()
            .semantic_prefetch_consumed_scroll_generation;
        let allow_boundary_prefetch = scroll_generation > consumed_scroll_generation;
        let mut consumed_boundary_prefetch = false;
        for action in actions {
            let requires_scroll_intent = semantic_action_requires_scroll_intent(&action);
            if requires_scroll_intent && (!allow_boundary_prefetch || consumed_boundary_prefetch) {
                continue;
            }
            if semantic_action_allowed_by_scroll(
                &action,
                allow_thread_before,
                allow_thread_after,
                allow_work_prefetch,
            ) {
                if requires_scroll_intent {
                    consumed_boundary_prefetch = true;
                }
                self.execute_semantic_timeline_action(action, cx);
            }
        }
        if consumed_boundary_prefetch {
            self.thread_timeline_view_state
                .borrow_mut()
                .semantic_prefetch_consumed_scroll_generation = scroll_generation;
        }
    }

    pub(super) fn toggle_turn_work_group_expanded(
        &mut self,
        toggle_key: &str,
        cx: &mut Context<Self>,
    ) {
        let Some(turn_id) = toggle_key.strip_prefix(SEMANTIC_TURN_WORK_GROUP_PREFIX) else {
            self.toggle_timeline_item_expanded(toggle_key, cx);
            return;
        };
        let Some(thread_id) = self.current_active_thread_id().map(str::to_owned) else {
            return;
        };

        let is_expanded = self
            .gateway
            .client_runtime
            .client_core()
            .thread_semantic_snapshot(&thread_id)
            .thread(thread_id.as_str())
            .and_then(|thread| {
                thread
                    .cached_turn_work_block(turn_id)
                    .map(|work| semantic::resolve_work_expanded(work, &thread.expansion))
            })
            .unwrap_or(false);

        self.capture_timeline_scroll_anchor_before_semantic_update(false);
        self.gateway
            .client_runtime
            .client_core()
            .set_thread_turn_work_expanded(&thread_id, turn_id, !is_expanded);
    }

    pub(super) fn execute_semantic_timeline_action(
        &mut self,
        action: SemanticTimelineRequestAction,
        _cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let id = match &action {
            SemanticTimelineRequestAction::ThreadTimelinePage { params, .. } => &params.thread_id,
            SemanticTimelineRequestAction::TurnWorkPage { params, .. } => &params.thread_id,
            SemanticTimelineRequestAction::TurnWorkItemsGet { params, .. } => &params.thread_id,
        };
        if self.current_active_thread_id() == Some(id.as_str()) {
            let key = semantic::semantic_timeline_request_key(&action);
            self.capture_timeline_scroll_anchor_before_semantic_update(matches!(
                key,
                SemanticTimelineRequestKey::ThreadBefore { .. }
                    | SemanticTimelineRequestKey::TurnWorkBefore { .. }
            ));
            if semantic_request_key_requires_scroll_intent(key) {
                self.consume_all_semantic_prefetch_scroll_intents();
            }
        }
        self.gateway
            .client_runtime
            .client_core()
            .schedule_thread_semantic_request(action);
    }

    fn semantic_prefetch_can_request_thread_before(&self) -> bool {
        let max_offset = self.thread_timeline_scroll_handle.max_offset().y;
        if max_offset <= gpui_kit::px(1.) {
            return false;
        }
        self.thread_timeline_scroll_handle.offset().y >= gpui_kit::px(-24.)
    }

    fn semantic_prefetch_can_request_thread_after(&self) -> bool {
        let max_offset = self.thread_timeline_scroll_handle.max_offset().y;
        if max_offset <= gpui_kit::px(1.) {
            return false;
        }
        self.timeline_is_near_bottom()
    }

    fn semantic_prefetch_can_request_work_range(&self) -> bool {
        self.thread_timeline_scroll_handle.max_offset().y > gpui_kit::px(1.)
    }
}

fn turn_work_page_merge_mode(key: &SemanticTimelineRequestKey) -> WorkPageMergeMode {
    match key {
        SemanticTimelineRequestKey::TurnWorkBefore { .. } => WorkPageMergeMode::MergeBefore,
        SemanticTimelineRequestKey::TurnWorkAfter { .. } => WorkPageMergeMode::MergeAfter,
        // Initial and live newest-page requests share a key so they are deduplicated. Merging is
        // equivalent to reset for an empty range; on refresh it preserves the oldest loaded
        // cursor and extends the range toward its newest boundary.
        SemanticTimelineRequestKey::TurnWorkInitial { .. } => WorkPageMergeMode::MergeAfter,
        _ => WorkPageMergeMode::Merge,
    }
}

fn top_level_page_preserves_near_bottom_anchor(merge_mode: TopLevelPageMergeMode) -> bool {
    matches!(merge_mode, TopLevelPageMergeMode::MergeBefore)
}

fn work_page_preserves_near_bottom_anchor(merge_mode: WorkPageMergeMode) -> bool {
    matches!(merge_mode, WorkPageMergeMode::MergeBefore)
}

fn semantic_action_allowed_by_scroll(
    action: &SemanticTimelineRequestAction,
    allow_thread_before: bool,
    allow_thread_after: bool,
    allow_work_prefetch: bool,
) -> bool {
    match action {
        SemanticTimelineRequestAction::ThreadTimelinePage { key, .. } => match key {
            SemanticTimelineRequestKey::ThreadNewest { .. } => true,
            SemanticTimelineRequestKey::ThreadBefore { .. } => allow_thread_before,
            SemanticTimelineRequestKey::ThreadAfter { .. } => allow_thread_after,
            _ => false,
        },
        SemanticTimelineRequestAction::TurnWorkPage { key, .. } => {
            matches!(key, SemanticTimelineRequestKey::TurnWorkInitial { .. }) || allow_work_prefetch
        }
        SemanticTimelineRequestAction::TurnWorkItemsGet { .. } => true,
    }
}

fn semantic_action_requires_scroll_intent(action: &SemanticTimelineRequestAction) -> bool {
    match action {
        SemanticTimelineRequestAction::ThreadTimelinePage { key, .. } => {
            !matches!(key, SemanticTimelineRequestKey::ThreadNewest { .. })
        }
        SemanticTimelineRequestAction::TurnWorkPage { key, .. } => {
            !matches!(key, SemanticTimelineRequestKey::TurnWorkInitial { .. })
        }
        SemanticTimelineRequestAction::TurnWorkItemsGet { .. } => false,
    }
}

fn semantic_request_key_requires_scroll_intent(key: &SemanticTimelineRequestKey) -> bool {
    match key {
        SemanticTimelineRequestKey::ThreadBefore { .. }
        | SemanticTimelineRequestKey::ThreadAfter { .. }
        | SemanticTimelineRequestKey::TurnWorkBefore { .. }
        | SemanticTimelineRequestKey::TurnWorkAfter { .. } => true,
        SemanticTimelineRequestKey::ThreadNewest { .. }
        | SemanticTimelineRequestKey::TurnWorkInitial { .. }
        | SemanticTimelineRequestKey::TurnWorkItems { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newest_turn_work_page_merges_with_loaded_range() {
        assert_eq!(
            turn_work_page_merge_mode(&SemanticTimelineRequestKey::TurnWorkInitial {
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_a".to_owned(),
            }),
            WorkPageMergeMode::MergeAfter
        );
    }

    #[test]
    fn prepend_pages_preserve_scroll_anchor_even_near_bottom() {
        assert!(top_level_page_preserves_near_bottom_anchor(
            TopLevelPageMergeMode::MergeBefore
        ));
        assert!(work_page_preserves_near_bottom_anchor(
            WorkPageMergeMode::MergeBefore
        ));
        assert!(!top_level_page_preserves_near_bottom_anchor(
            TopLevelPageMergeMode::MergeAfter
        ));
        assert!(!work_page_preserves_near_bottom_anchor(
            WorkPageMergeMode::MergeAfter
        ));
    }
}
