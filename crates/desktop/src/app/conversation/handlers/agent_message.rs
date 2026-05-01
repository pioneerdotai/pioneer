use super::{ITEM_TYPE_AGENT_MESSAGE, TurnItemHandler};
use crate::app::conversation::reducer::{ConversationProjector, TimelineEntryStatus};
use pioneer_protocol::{MARKDOWN_AST_VERSION, MarkdownDocument, TurnItem};

pub(super) struct AgentMessageHandler;

impl TurnItemHandler for AgentMessageHandler {
    fn on_started(
        &self,
        projector: &mut ConversationProjector,
        turn_id: &str,
        item: &TurnItem,
        ts_unix_ms: i64,
    ) {
        let TurnItem::AgentMessage {
            id,
            text,
            markdown,
            markdown_version,
        } = item
        else {
            return;
        };
        let markdown = if Self::is_supported_markdown_version(*markdown_version) {
            markdown.clone()
        } else {
            None
        };

        projector.start_item_view(
            id,
            turn_id,
            ITEM_TYPE_AGENT_MESSAGE,
            TimelineEntryStatus::Running,
            text.clone(),
            markdown,
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
        markdown: Option<&MarkdownDocument>,
        markdown_version: Option<u16>,
        ts_unix_ms: i64,
    ) {
        let markdown = if Self::is_supported_markdown_version(markdown_version) {
            markdown.cloned()
        } else {
            None
        };
        projector.append_item_delta(item_id, delta, markdown, ts_unix_ms);
    }

    fn on_completed(
        &self,
        projector: &mut ConversationProjector,
        _turn_id: &str,
        item: &TurnItem,
        ts_unix_ms: i64,
    ) {
        let TurnItem::AgentMessage {
            id,
            text,
            markdown,
            markdown_version,
        } = item
        else {
            return;
        };
        let markdown = if Self::is_supported_markdown_version(*markdown_version) {
            markdown.clone()
        } else {
            None
        };

        projector.complete_item_view(
            id,
            TimelineEntryStatus::Completed,
            Some(text.as_str()),
            markdown,
            item.clone(),
            None,
            ts_unix_ms,
        );
    }
}

impl AgentMessageHandler {
    fn is_supported_markdown_version(version: Option<u16>) -> bool {
        version.is_none_or(|version| version == MARKDOWN_AST_VERSION)
    }
}
