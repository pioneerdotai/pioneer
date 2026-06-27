use super::*;
use pioneer_crud::{
    ProjectionPlacement, ProjectionVisibility, approval_block_id, assistant_block_id,
    classify_turn_item_with_db_status, user_block_id, work_block_id, work_item_projection_id,
};
use pioneer_protocol::{
    ThreadTimelineBlocksChangedNotification, TimelineChangeReason,
    TurnWorkItemsChangedNotification, TurnWorkStateChangedNotification,
};

impl MessageProcessor {
    pub(super) async fn notify_semantic_timeline_item_changed(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item: &TurnItem,
        db_status: Option<&str>,
    ) {
        let classification = classify_turn_item_with_db_status(item, db_status);
        let item_id = item.item_id();
        let mut changed_block_ids = Vec::new();
        let mut changed_work_item_ids = Vec::new();
        let mut removed_work_item_ids = Vec::new();
        let mut notify_work_state = false;

        match classification.placement {
            ProjectionPlacement::TopLevelAssistantMessage => {
                notify_work_state = true;
                changed_block_ids.push(work_block_id(turn_id));
                changed_block_ids.push(assistant_block_id(turn_id, item_id));
                removed_work_item_ids.push(work_item_projection_id(turn_id, item_id));
            }
            ProjectionPlacement::TurnWork | ProjectionPlacement::Hidden => {
                notify_work_state = true;
                changed_block_ids.push(work_block_id(turn_id));
                if classification.visibility == ProjectionVisibility::Visible {
                    changed_work_item_ids.push(work_item_projection_id(turn_id, item_id));
                }
            }
            ProjectionPlacement::TopLevelUserMessage => {
                changed_block_ids.push(user_block_id(turn_id));
            }
        }

        self.notify_semantic_timeline_blocks_changed(
            workspace_id,
            thread_id,
            changed_block_ids,
            Vec::new(),
        )
        .await;
        if !changed_work_item_ids.is_empty() || !removed_work_item_ids.is_empty() {
            self.notify_semantic_turn_work_items_changed(
                workspace_id,
                thread_id,
                turn_id,
                changed_work_item_ids,
                removed_work_item_ids,
            )
            .await;
        }
        if notify_work_state {
            self.notify_semantic_turn_work_state_changed(workspace_id, thread_id, turn_id)
                .await;
        }
    }

    pub(super) async fn notify_semantic_timeline_turn_state_changed(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) {
        self.notify_semantic_timeline_blocks_changed(
            workspace_id,
            thread_id,
            vec![work_block_id(turn_id)],
            Vec::new(),
        )
        .await;
        self.notify_semantic_turn_work_state_changed(workspace_id, thread_id, turn_id)
            .await;
    }

    pub(super) async fn notify_semantic_timeline_work_item_id_changed(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
    ) {
        self.notify_semantic_timeline_blocks_changed(
            workspace_id,
            thread_id,
            vec![work_block_id(turn_id)],
            Vec::new(),
        )
        .await;
        self.notify_semantic_turn_work_items_changed(
            workspace_id,
            thread_id,
            turn_id,
            vec![work_item_projection_id(turn_id, item_id)],
            Vec::new(),
        )
        .await;
        self.notify_semantic_turn_work_state_changed(workspace_id, thread_id, turn_id)
            .await;
    }

    pub(super) async fn notify_semantic_timeline_pending_request_changed(
        &self,
        request: &CliRuntimePendingRequestRecord,
    ) {
        let Some(turn_id) = request.turn_id.as_deref() else {
            return;
        };
        self.notify_semantic_timeline_blocks_changed(
            request.workspace_id.as_str(),
            request.thread_id.as_str(),
            vec![
                work_block_id(turn_id),
                approval_block_id(turn_id, request.request_id.as_str()),
            ],
            Vec::new(),
        )
        .await;
        self.notify_semantic_turn_work_state_changed(
            request.workspace_id.as_str(),
            request.thread_id.as_str(),
            turn_id,
        )
        .await;
    }

    async fn notify_semantic_timeline_blocks_changed(
        &self,
        workspace_id: &str,
        thread_id: &str,
        mut changed_block_ids: Vec<String>,
        mut removed_block_ids: Vec<String>,
    ) {
        changed_block_ids.sort();
        changed_block_ids.dedup();
        removed_block_ids.sort();
        removed_block_ids.dedup();
        if changed_block_ids.is_empty() && removed_block_ids.is_empty() {
            return;
        }

        let payload = ThreadTimelineBlocksChangedNotification {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            changed_block_ids,
            removed_block_ids,
            before_cursor: None,
            after_cursor: None,
            reason: TimelineChangeReason::LiveEvent,
        };
        self.send_notification_to_thread_subscribers(
            thread_id,
            events::THREAD_TIMELINE_BLOCKS_CHANGED,
            &payload,
        )
        .await;
    }

    async fn notify_semantic_turn_work_items_changed(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        mut changed_work_item_ids: Vec<String>,
        mut removed_work_item_ids: Vec<String>,
    ) {
        changed_work_item_ids.sort();
        changed_work_item_ids.dedup();
        removed_work_item_ids.sort();
        removed_work_item_ids.dedup();

        let payload = TurnWorkItemsChangedNotification {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            changed_work_item_ids,
            removed_work_item_ids,
            before_cursor: None,
            after_cursor: None,
            reason: TimelineChangeReason::LiveEvent,
        };
        self.send_notification_to_thread_subscribers(
            thread_id,
            events::TURN_WORK_ITEMS_CHANGED,
            &payload,
        )
        .await;
    }

    async fn notify_semantic_turn_work_state_changed(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) {
        let projection = match self.crud_store.get_turn_work_projection(turn_id).await {
            Ok(Some(projection)) => projection,
            Ok(None) => return,
            Err(error) => {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to load turn work projection for semantic state notification"
                );
                return;
            }
        };
        let work = match self.turn_work_block_from_projection(projection).await {
            Ok(work) => work,
            Err(error) => {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to build turn work block for semantic state notification"
                );
                return;
            }
        };

        let payload = TurnWorkStateChangedNotification {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            work,
            reason: TimelineChangeReason::LiveEvent,
        };
        self.send_notification_to_thread_subscribers(
            thread_id,
            events::TURN_WORK_STATE_CHANGED,
            &payload,
        )
        .await;
    }
}
