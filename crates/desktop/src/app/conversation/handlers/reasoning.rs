use super::{ITEM_TYPE_REASONING, TurnItemHandler};
use crate::app::conversation::reducer::{ConversationProjector, TimelineEntryStatus};
use pioneer_protocol::{MARKDOWN_AST_VERSION, MarkdownDocument, TurnItem};

pub(super) struct ReasoningHandler;

impl TurnItemHandler for ReasoningHandler {
    fn on_started(
        &self,
        projector: &mut ConversationProjector,
        turn_id: &str,
        item: &TurnItem,
        ts_unix_ms: i64,
    ) {
        let TurnItem::Reasoning { id, .. } = item else {
            return;
        };

        projector.start_item_view(
            id,
            turn_id,
            ITEM_TYPE_REASONING,
            TimelineEntryStatus::Running,
            "".to_owned(),
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
        let TurnItem::Reasoning {
            id,
            summary,
            content,
        } = item
        else {
            return;
        };

        let mut final_text = String::new();
        if !summary.is_empty() {
            final_text.push_str(&summary.join("\n"));
        }
        if !content.is_empty() {
            if !final_text.is_empty() {
                final_text.push_str("\n\n");
            }
            final_text.push_str(&content.join("\n"));
        }

        let final_text = (!final_text.is_empty()).then_some(final_text);

        projector.complete_item_view(
            id,
            TimelineEntryStatus::Completed,
            final_text.as_deref(),
            None,
            item.clone(),
            None,
            ts_unix_ms,
        );
    }
}

impl ReasoningHandler {
    fn is_supported_markdown_version(version: Option<u16>) -> bool {
        version.is_none_or(|version| version == MARKDOWN_AST_VERSION)
    }
}
