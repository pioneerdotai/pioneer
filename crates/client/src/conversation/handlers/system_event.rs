use super::{ITEM_TYPE_SYSTEM_EVENT, TurnItemHandler};
use crate::conversation::reducer::{ConversationProjector, TimelineEntryStatus};
use pioneer_protocol::TurnItem;

pub struct SystemEventHandler;

impl TurnItemHandler for SystemEventHandler {
    fn on_started(
        &self,
        projector: &mut ConversationProjector,
        turn_id: &str,
        item: &TurnItem,
        ts_unix_ms: i64,
    ) {
        let TurnItem::SystemEvent { id, message, .. } = item else {
            return;
        };

        projector.start_item_view(
            id,
            turn_id,
            ITEM_TYPE_SYSTEM_EVENT,
            TimelineEntryStatus::Running,
            message.clone(),
            None,
            item.clone(),
            None,
            ts_unix_ms,
        );
    }

    fn on_delta(
        &self,
        projector: &mut ConversationProjector,
        _turn_id: &str,
        item_id: &str,
        delta: &str,
        _stream: Option<pioneer_protocol::ItemDeltaStream>,
        _payload: Option<&serde_json::Value>,
        _markdown: Option<&pioneer_protocol::MarkdownDocument>,
        _markdown_version: Option<u16>,
        ts_unix_ms: i64,
    ) {
        projector.append_item_delta(item_id, delta, None, ts_unix_ms);
    }

    fn on_completed(
        &self,
        projector: &mut ConversationProjector,
        turn_id: &str,
        item: &TurnItem,
        ts_unix_ms: i64,
    ) {
        let TurnItem::SystemEvent { id, message, .. } = item else {
            return;
        };

        let status = match item.context_compaction_status() {
            Some(pioneer_protocol::TurnWorkItemStatus::Running) => {
                self.on_started(projector, turn_id, item, ts_unix_ms);
                return;
            }
            Some(pioneer_protocol::TurnWorkItemStatus::Failed) => TimelineEntryStatus::Failed,
            Some(pioneer_protocol::TurnWorkItemStatus::Cancelled) => TimelineEntryStatus::Cancelled,
            Some(pioneer_protocol::TurnWorkItemStatus::Blocked) => TimelineEntryStatus::Blocked,
            _ => TimelineEntryStatus::Completed,
        };
        projector.complete_item_view(
            id,
            status,
            Some(message.as_str()),
            None,
            item.clone(),
            None,
            ts_unix_ms,
        );
    }
}
