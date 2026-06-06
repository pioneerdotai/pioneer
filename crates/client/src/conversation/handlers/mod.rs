mod agent_message;
mod reasoning;
mod system_event;
mod task;
mod tool_call;
mod user_message;

use super::events::turn_item_type;
use super::reducer::ConversationProjector;
use agent_message::AgentMessageHandler;
use pioneer_protocol::{MarkdownDocument, TurnItem};
use reasoning::ReasoningHandler;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use system_event::SystemEventHandler;
use task::TaskHandler;
use tool_call::ToolCallHandler;
use user_message::UserMessageHandler;

pub const ITEM_TYPE_USER_MESSAGE: &str = "user_message";
pub const ITEM_TYPE_AGENT_MESSAGE: &str = "agent_message";
pub const ITEM_TYPE_REASONING: &str = "reasoning";
pub const ITEM_TYPE_SYSTEM_EVENT: &str = "system_event";
pub const ITEM_TYPE_TASK: &str = "task";
pub const ITEM_TYPE_COMMAND_EXECUTION: &str = "command_execution";
pub const ITEM_TYPE_FILE_CHANGE: &str = "file_change";
pub const ITEM_TYPE_WEB_SEARCH: &str = "web_search";
pub const ITEM_TYPE_WEB_FETCH: &str = "web_fetch";
pub const ITEM_TYPE_DOWNLOAD: &str = "download";
pub const ITEM_TYPE_DYNAMIC_TOOL_CALL: &str = "dynamic_tool_call";

pub trait TurnItemHandler: Send + Sync {
    fn on_started(
        &self,
        projector: &mut ConversationProjector,
        turn_id: &str,
        item: &TurnItem,
        ts_unix_ms: i64,
    );

    fn on_delta(
        &self,
        projector: &mut ConversationProjector,
        turn_id: &str,
        item_id: &str,
        delta: &str,
        stream: Option<pioneer_protocol::ItemDeltaStream>,
        payload: Option<&JsonValue>,
        markdown: Option<&MarkdownDocument>,
        markdown_version: Option<u16>,
        ts_unix_ms: i64,
    );

    fn on_completed(
        &self,
        projector: &mut ConversationProjector,
        turn_id: &str,
        item: &TurnItem,
        ts_unix_ms: i64,
    );
}

pub struct TurnItemHandlerRegistry {
    handlers: HashMap<String, Box<dyn TurnItemHandler>>,
}

impl TurnItemHandlerRegistry {
    pub fn new() -> Self {
        let mut handlers: HashMap<String, Box<dyn TurnItemHandler>> = HashMap::new();
        handlers.insert(
            ITEM_TYPE_USER_MESSAGE.to_owned(),
            Box::new(UserMessageHandler),
        );
        handlers.insert(
            ITEM_TYPE_AGENT_MESSAGE.to_owned(),
            Box::new(AgentMessageHandler),
        );
        handlers.insert(ITEM_TYPE_REASONING.to_owned(), Box::new(ReasoningHandler));
        handlers.insert(
            ITEM_TYPE_SYSTEM_EVENT.to_owned(),
            Box::new(SystemEventHandler),
        );
        handlers.insert(ITEM_TYPE_TASK.to_owned(), Box::new(TaskHandler));
        handlers.insert(
            ITEM_TYPE_COMMAND_EXECUTION.to_owned(),
            Box::new(ToolCallHandler),
        );
        handlers.insert(ITEM_TYPE_FILE_CHANGE.to_owned(), Box::new(ToolCallHandler));
        handlers.insert(ITEM_TYPE_WEB_SEARCH.to_owned(), Box::new(ToolCallHandler));
        handlers.insert(ITEM_TYPE_WEB_FETCH.to_owned(), Box::new(ToolCallHandler));
        handlers.insert(ITEM_TYPE_DOWNLOAD.to_owned(), Box::new(ToolCallHandler));
        handlers.insert(
            ITEM_TYPE_DYNAMIC_TOOL_CALL.to_owned(),
            Box::new(ToolCallHandler),
        );

        Self { handlers }
    }

    pub fn apply_started(
        &self,
        projector: &mut ConversationProjector,
        turn_id: &str,
        item: &TurnItem,
        ts_unix_ms: i64,
    ) {
        let item_type = turn_item_type(item);
        if let Some(handler) = self.handlers.get(item_type).map(Box::as_ref) {
            handler.on_started(projector, turn_id, item, ts_unix_ms);
        }
    }

    pub fn apply_delta(
        &self,
        projector: &mut ConversationProjector,
        turn_id: &str,
        item_id: &str,
        delta: &str,
        stream: Option<pioneer_protocol::ItemDeltaStream>,
        payload: Option<&JsonValue>,
        markdown: Option<&MarkdownDocument>,
        markdown_version: Option<u16>,
        ts_unix_ms: i64,
    ) {
        let Some(handler) = projector
            .item_type_by_id(item_id)
            .and_then(|item_type| self.handlers.get(item_type).map(Box::as_ref))
        else {
            return;
        };
        handler.on_delta(
            projector,
            turn_id,
            item_id,
            delta,
            stream,
            payload,
            markdown,
            markdown_version,
            ts_unix_ms,
        );
    }

    pub fn apply_completed(
        &self,
        projector: &mut ConversationProjector,
        turn_id: &str,
        item: &TurnItem,
        ts_unix_ms: i64,
    ) {
        let item_type = turn_item_type(item);
        if let Some(handler) = self.handlers.get(item_type).map(Box::as_ref) {
            handler.on_completed(projector, turn_id, item, ts_unix_ms);
        }
    }
}

impl Default for TurnItemHandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}
