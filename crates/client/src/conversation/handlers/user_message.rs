use super::{ITEM_TYPE_USER_MESSAGE, TurnItemHandler};
use crate::conversation::reducer::{ConversationProjector, TimelineEntryStatus};
use pioneer_protocol::TurnItem;

pub struct UserMessageHandler;

impl TurnItemHandler for UserMessageHandler {
    fn on_started(
        &self,
        projector: &mut ConversationProjector,
        turn_id: &str,
        item: &TurnItem,
        ts_unix_ms: i64,
    ) {
        let TurnItem::UserMessage { id, text, .. } = item else {
            return;
        };

        projector.start_item_view(
            id,
            turn_id,
            ITEM_TYPE_USER_MESSAGE,
            TimelineEntryStatus::Completed,
            text.clone(),
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
        let TurnItem::UserMessage { id, text, .. } = item else {
            return;
        };

        projector.complete_item_view(
            id,
            TimelineEntryStatus::Completed,
            Some(text.as_str()),
            None,
            item.clone(),
            None,
            ts_unix_ms,
        );
    }
}
