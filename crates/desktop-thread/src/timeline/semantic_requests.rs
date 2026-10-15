use crate::screen::GatewayConnectionState;
use crate::screen::TimelineView;
use gpui_kit::Context;
use pioneer_client::timeline::semantic;
use pioneer_client::timeline::semantic::DEFAULT_TOP_LEVEL_PAGE_LIMIT;
use pioneer_client::timeline::semantic::SemanticTimelineRequestAction;
use pioneer_client::timeline::semantic::SemanticTimelineRequestKey;
use pioneer_client::timeline::types::ThreadTimelinePageParams;
use pioneer_client::timeline::types::TimelinePageAnchor;

use super::semantic_adapter::SEMANTIC_TURN_WORK_GROUP_PREFIX;

impl TimelineView {
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
        self.client.refresh_thread_timeline(&thread_id);
    }

    pub(super) fn toggle_turn_work_group_expanded(
        &mut self,
        toggle_key: &str,
        window: &mut gpui_kit::Window,
        cx: &mut Context<Self>,
    ) {
        let Some(turn_id) = toggle_key.strip_prefix(SEMANTIC_TURN_WORK_GROUP_PREFIX) else {
            self.toggle_timeline_item_expanded(toggle_key, window, cx);
            return;
        };
        let Some(thread_id) = self.current_active_thread_id().map(str::to_owned) else {
            return;
        };

        let is_expanded = self
            .client
            .thread_semantic_snapshot(&thread_id)
            .thread(thread_id.as_str())
            .and_then(|thread| {
                thread
                    .cached_turn_work_block(turn_id)
                    .map(|work| semantic::resolve_work_expanded(work, &thread.expansion))
            })
            .unwrap_or(false);

        {
            let mut state = self.thread_timeline_view_state.borrow_mut();
            state.scroll.set_work_expansion_anchor(
                &thread_id,
                toggle_key,
                !is_expanded,
                &self.thread_timeline_view_state.scroll_handle,
            );
        }
        self.consume_all_semantic_prefetch_scroll_intents();
        self.client
            .set_thread_turn_work_expanded(&thread_id, turn_id, !is_expanded);
    }

    pub(super) fn execute_semantic_timeline_action(
        &mut self,
        action: SemanticTimelineRequestAction,
        _cx: &mut Context<Self>,
    ) {
        if self.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let id = match &action {
            SemanticTimelineRequestAction::ThreadTimelinePage { params, .. } => &params.thread_id,
            SemanticTimelineRequestAction::TurnWorkPage { params, .. } => &params.thread_id,
            SemanticTimelineRequestAction::TurnWorkItemsGet { params, .. } => &params.thread_id,
        };
        if self.current_active_thread_id() == Some(id.as_str()) {
            let key = semantic::semantic_timeline_request_key(&action);
            if semantic_request_key_requires_scroll_intent(key) {
                self.consume_all_semantic_prefetch_scroll_intents();
            }
        }
        self.client.schedule_thread_semantic_request(action);
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
