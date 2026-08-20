use super::*;
use pioneer_crud::{
    ProjectionPlacement, ProjectionVisibility, approval_block_id, assistant_block_id,
    classify_turn_item_with_db_status, detached_task_run_block_id, user_block_id, work_block_id,
    work_item_projection_id,
};
use pioneer_protocol::{
    TaskAttachmentMode, ThreadTimelineBlocksChangedNotification, TimelineChangeReason, TurnKind,
    TurnOrigin, TurnWorkItemsChangedNotification, TurnWorkStateChangedNotification,
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
        let detached_task_run = self.is_detached_task_run_turn(thread_id, turn_id).await;
        let mut changed_block_ids = Vec::new();
        let mut removed_block_ids = Vec::new();
        let mut changed_work_item_ids = Vec::new();
        let mut removed_work_item_ids = Vec::new();
        let mut notify_work_state = false;

        match classification.placement {
            ProjectionPlacement::TopLevelDetachedTaskRun => {
                changed_block_ids.push(detached_task_run_block_id(turn_id, item_id));
                removed_block_ids.push(work_block_id(turn_id));
                removed_work_item_ids.push(work_item_projection_id(turn_id, item_id));
            }
            ProjectionPlacement::TopLevelAssistantMessage => {
                notify_work_state = !detached_task_run;
                if detached_task_run {
                    removed_block_ids.push(work_block_id(turn_id));
                } else {
                    changed_block_ids.push(work_block_id(turn_id));
                }
                changed_block_ids.push(assistant_block_id(turn_id, item_id));
                removed_work_item_ids.push(work_item_projection_id(turn_id, item_id));
            }
            ProjectionPlacement::TurnWork | ProjectionPlacement::Hidden => {
                if detached_task_run && matches!(item, TurnItem::Task { .. }) {
                    changed_block_ids.push(detached_task_run_block_id(turn_id, item_id));
                    removed_block_ids.push(work_block_id(turn_id));
                    removed_work_item_ids.push(work_item_projection_id(turn_id, item_id));
                } else {
                    notify_work_state = true;
                    changed_block_ids.push(work_block_id(turn_id));
                    if classification.visibility == ProjectionVisibility::Visible {
                        changed_work_item_ids.push(work_item_projection_id(turn_id, item_id));
                    } else {
                        removed_work_item_ids.push(work_item_projection_id(turn_id, item_id));
                    }
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
            removed_block_ids,
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
        if self.is_detached_task_run_turn(thread_id, turn_id).await {
            self.notify_semantic_timeline_blocks_changed(
                workspace_id,
                thread_id,
                Vec::new(),
                vec![work_block_id(turn_id)],
            )
            .await;
            return;
        }
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

    pub(super) async fn notify_semantic_user_message_changed(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) {
        self.notify_semantic_timeline_blocks_changed(
            workspace_id,
            thread_id,
            vec![user_block_id(turn_id)],
            Vec::new(),
        )
        .await;
    }

    async fn is_detached_task_run_turn(&self, thread_id: &str, turn_id: &str) -> bool {
        let Ok(Some((_, turn))) = self.crud_store.get_turn(thread_id, turn_id).await else {
            return false;
        };
        if turn.turn_kind != TurnKind::TaskRun {
            return false;
        }

        if let Ok(items) = self
            .crud_store
            .list_turn_items_by_type(turn_id, "task")
            .await
            && let Some(attachment) = items.iter().find_map(|item| match item {
                TurnItem::Task { item } => Some(item.attachment),
                _ => None,
            })
        {
            return attachment == TaskAttachmentMode::Detached;
        }

        matches!(
            turn.origin,
            TurnOrigin::ScheduledTask | TurnOrigin::DetachedTask
        )
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
        let approval_block_id = approval_block_id(turn_id, request.request_id.as_str());
        let is_pending = request.status.is_open();
        let (changed_block_ids, removed_block_ids) = if is_pending {
            (vec![work_block_id(turn_id), approval_block_id], Vec::new())
        } else {
            (vec![work_block_id(turn_id)], vec![approval_block_id])
        };
        self.notify_semantic_timeline_blocks_changed(
            request.workspace_id.as_str(),
            request.thread_id.as_str(),
            changed_block_ids,
            removed_block_ids,
        )
        .await;
        self.notify_semantic_turn_work_state_changed(
            request.workspace_id.as_str(),
            request.thread_id.as_str(),
            turn_id,
        )
        .await;
        self.notify_semantic_timeline_pending_request_changed_for_ancestors(request, turn_id)
            .await;
    }

    async fn notify_semantic_timeline_pending_request_changed_for_ancestors(
        &self,
        request: &CliRuntimePendingRequestRecord,
        turn_id: &str,
    ) {
        let ancestor_thread_ids = self.pending_request_ancestor_thread_ids(request).await;
        if ancestor_thread_ids.is_empty() {
            return;
        }

        let block_id = approval_block_id(turn_id, request.request_id.as_str());
        let is_pending = request.status.is_open();
        for ancestor_thread_id in ancestor_thread_ids {
            let (changed_block_ids, removed_block_ids) = if is_pending {
                (vec![block_id.clone()], Vec::new())
            } else {
                (Vec::new(), vec![block_id.clone()])
            };
            self.notify_semantic_timeline_blocks_changed(
                request.workspace_id.as_str(),
                ancestor_thread_id.as_str(),
                changed_block_ids,
                removed_block_ids,
            )
            .await;
        }
    }

    async fn pending_request_ancestor_thread_ids(
        &self,
        request: &CliRuntimePendingRequestRecord,
    ) -> Vec<String> {
        let mut ancestor_thread_ids = Vec::new();
        let mut seen = HashSet::<String>::new();
        let mut current = request.thread_id.clone();
        for _ in 0..64 {
            let lineage = match self
                .crud_store
                .get_task_thread_lineage(current.as_str())
                .await
            {
                Ok(Some(lineage)) => lineage,
                Ok(None) => break,
                Err(error) => {
                    warn!(
                        thread_id = current,
                        error = %format!("{error:#}"),
                        "failed to resolve ancestor timeline targets for pending request"
                    );
                    break;
                }
            };
            let parent_thread_id = lineage.parent_thread_id;
            if !seen.insert(parent_thread_id.clone()) {
                break;
            }
            ancestor_thread_ids.push(parent_thread_id.clone());
            current = parent_thread_id;
        }
        ancestor_thread_ids
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

    pub(super) async fn notify_agent_work_graph_state_changed(&self, root_execution_id: &str) {
        let target = match self
            .crud_store
            .get_agent_work_graph_projection_target(root_execution_id)
            .await
        {
            Ok(Some(target)) => target,
            Ok(None) => return,
            Err(_error) => {
                warn!(
                    root_execution_id,
                    failure_class = "agent_work_graph_notification_target_failed",
                    "failed to resolve Agent work-graph notification target"
                );
                return;
            }
        };
        self.notify_semantic_turn_work_state_changed(
            target.workspace_id.as_str(),
            target.thread_id.as_str(),
            target.turn_id.as_str(),
        )
        .await;
    }

    pub(super) async fn finalize_agent_execution_and_notify(
        &self,
        execution_id: &str,
        terminal_status: &str,
    ) -> anyhow::Result<bool> {
        let database = self.crud_store.database_connection();
        let Some(execution) = pioneer_crud::load_agent_execution(&database, execution_id).await?
        else {
            return Ok(false);
        };
        let root_execution_id = execution.work_graph_root_execution_id;
        let finalized = self
            .crud_store
            .finalize_agent_execution(execution_id, terminal_status, pioneer_crud::utc_now())
            .await?;
        if finalized {
            self.notify_agent_work_graph_state_changed(root_execution_id.as_str())
                .await;
        }
        Ok(finalized)
    }

    pub(super) async fn notify_semantic_turn_work_state_changed(
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
        let source_high_watermark = projection.source_high_watermark;
        let projection_updated_at_unix_micros = projection.updated_at.timestamp_micros();
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
            source_high_watermark,
            projection_updated_at_unix_micros,
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
