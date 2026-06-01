use super::{ITEM_TYPE_TASK, TurnItemHandler};
use crate::app::conversation::reducer::{ConversationProjector, TimelineEntryStatus};
use pioneer_protocol::{TaskStatus, TurnItem};

pub(super) struct TaskHandler;

impl TurnItemHandler for TaskHandler {
    fn on_started(
        &self,
        projector: &mut ConversationProjector,
        turn_id: &str,
        item: &TurnItem,
        ts_unix_ms: i64,
    ) {
        let TurnItem::Task { item: task_item } = item else {
            return;
        };

        projector.start_item_view(
            task_item.id.as_str(),
            turn_id,
            ITEM_TYPE_TASK,
            status_for_task(task_item.status),
            task_item.title.clone(),
            None,
            item.clone(),
            None,
            ts_unix_ms,
        );
    }

    fn on_delta(
        &self,
        _projector: &mut ConversationProjector,
        _turn_id: &str,
        _item_id: &str,
        _delta: &str,
        _stream: Option<pioneer_protocol::ItemDeltaStream>,
        _payload: Option<&serde_json::Value>,
        _markdown: Option<&pioneer_protocol::MarkdownDocument>,
        _markdown_version: Option<u16>,
        _ts_unix_ms: i64,
    ) {
    }

    fn on_completed(
        &self,
        projector: &mut ConversationProjector,
        _turn_id: &str,
        item: &TurnItem,
        ts_unix_ms: i64,
    ) {
        let TurnItem::Task { item: task_item } = item else {
            return;
        };

        projector.complete_item_view(
            task_item.id.as_str(),
            status_for_task(task_item.status),
            Some(task_item.title.as_str()),
            None,
            item.clone(),
            None,
            ts_unix_ms,
        );
    }
}

fn status_for_task(status: TaskStatus) -> TimelineEntryStatus {
    match status {
        TaskStatus::Completed => TimelineEntryStatus::Completed,
        TaskStatus::Failed => TimelineEntryStatus::Failed,
        TaskStatus::Cancelled => TimelineEntryStatus::Cancelled,
        TaskStatus::Draft
        | TaskStatus::Scheduled
        | TaskStatus::Queued
        | TaskStatus::Running
        | TaskStatus::WaitingReview
        | TaskStatus::Waiting => TimelineEntryStatus::Running,
    }
}
