use super::{ITEM_TYPE_WEB_FETCH, TurnItemHandler};
use crate::conversation::display::tool_display_text;
use crate::conversation::events::turn_item_type;
use crate::conversation::reducer::{ConversationProjector, TimelineEntryStatus};
use pioneer_protocol::{
    ItemDeltaStream, MARKDOWN_AST_VERSION, MarkdownDocument, ToolCallStatus, ToolStoragePayload,
    TurnItem,
};
use serde_json::Value as JsonValue;

macro_rules! client_tool_label {
    ("timeline.tool.started", tool_name = $tool_name:expr) => {
        format!("Running tool: {}", $tool_name)
    };
    ("timeline.tool.started_kind", tool_name = $tool_name:expr, kind = $kind:expr) => {
        format!("Running tool: {} ({})", $tool_name, $kind)
    };
    ("timeline.tool.finished", tool_name = $tool_name:expr) => {
        format!("Tool `{}` finished", $tool_name)
    };
    ("timeline.tool.command", command = $command:expr) => {
        format!("Command: {}", $command)
    };
    ("timeline.tool.cwd", cwd = $cwd:expr) => {
        format!("Working directory: {}", $cwd)
    };
    ("timeline.tool.query", query = $query:expr) => {
        format!("Query: {}", $query)
    };
    ("timeline.tool.url", url = $url:expr) => {
        format!("URL: {}", $url)
    };
    ("timeline.tool.exit_code", exit_code = $exit_code:expr) => {
        format!("Exit code: {}", $exit_code)
    };
    ("timeline.tool.duration_ms", duration_ms = $duration_ms:expr) => {
        format!("Duration: {} ms", $duration_ms)
    };
    ("timeline.tool.timed_out") => {
        "Timed out".to_owned()
    };
    ("timeline.tool.changed_files") => {
        "Changed files:".to_owned()
    };
    ("timeline.tool.results", count = $count:expr) => {
        format!("Results: {}", $count)
    };
    ("timeline.tool.provider", provider = $provider:expr) => {
        format!("Provider: {}", $provider)
    };
    ("timeline.tool.status_code", status_code = $status_code:expr) => {
        format!("Status code: {}", $status_code)
    };
    ("timeline.tool.title", title = $title:expr) => {
        format!("Title: {}", $title)
    };
    ("timeline.tool.path", path = $path:expr) => {
        format!("Path: {}", $path)
    };
    ("timeline.tool.bytes", bytes = $bytes:expr) => {
        format!("Bytes: {}", $bytes)
    };
    ("timeline.tool.progress", value = $value:expr) => {
        format!("[Progress] {}", $value)
    };
    ("timeline.tool.stderr", value = $value:expr) => {
        format!("[Stderr] {}", $value)
    };
}

pub struct ToolCallHandler;

impl TurnItemHandler for ToolCallHandler {
    fn on_started(
        &self,
        projector: &mut ConversationProjector,
        turn_id: &str,
        item: &TurnItem,
        ts_unix_ms: i64,
    ) {
        let Some(id) = tool_item_id(item) else {
            return;
        };
        let Some(tool_name) = tool_item_name(item) else {
            return;
        };
        let item_type = turn_item_type(item);
        let text = render_started_text(item, tool_name);

        projector.start_item_view(
            id,
            turn_id,
            item_type,
            TimelineEntryStatus::Running,
            text,
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
        stream: Option<ItemDeltaStream>,
        payload: Option<&JsonValue>,
        markdown: Option<&MarkdownDocument>,
        markdown_version: Option<u16>,
        ts_unix_ms: i64,
    ) {
        let is_content_heavy_tool_item = projector
            .view_state()
            .item_by_id(item_id)
            .is_some_and(|item| item.item_type == ITEM_TYPE_WEB_FETCH);

        let can_render_delta_stream = matches!(stream, Some(ItemDeltaStream::ToolProgress));

        if is_content_heavy_tool_item && !can_render_delta_stream {
            let rendered = render_tool_delta("", stream, payload);
            if rendered.is_empty() {
                return;
            }
            projector.append_item_delta(item_id, rendered.as_str(), None, ts_unix_ms);
            return;
        }

        let rendered = render_tool_delta(delta, stream, payload);
        if rendered.is_empty() {
            return;
        }
        let markdown = if Self::is_supported_markdown_version(markdown_version) {
            markdown.cloned()
        } else {
            None
        };
        projector.append_item_delta(item_id, rendered.as_str(), markdown, ts_unix_ms);
    }

    fn on_completed(
        &self,
        projector: &mut ConversationProjector,
        _turn_id: &str,
        item: &TurnItem,
        ts_unix_ms: i64,
    ) {
        let Some(id) = tool_item_id(item) else {
            return;
        };
        let Some(tool_name) = tool_item_name(item) else {
            return;
        };
        let status = tool_item_terminal_status(item);
        let final_text = tool_item_output(item)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| render_completed_text(item, tool_name));

        projector.complete_item_view(
            id,
            status,
            Some(final_text.as_str()),
            None,
            item.clone(),
            None,
            ts_unix_ms,
        );
    }
}

impl ToolCallHandler {
    fn is_supported_markdown_version(version: Option<u16>) -> bool {
        version.is_none_or(|version| version == MARKDOWN_AST_VERSION)
    }
}

fn tool_item_id(item: &TurnItem) -> Option<&str> {
    match item {
        TurnItem::CommandExecution { id, .. }
        | TurnItem::FileChange { id, .. }
        | TurnItem::WebSearch { id, .. }
        | TurnItem::WebFetch { id, .. }
        | TurnItem::Download { id, .. }
        | TurnItem::DynamicToolCall { id, .. } => Some(id.as_str()),
        _ => None,
    }
}

fn tool_item_name(item: &TurnItem) -> Option<&str> {
    match item {
        TurnItem::CommandExecution { tool_name, .. }
        | TurnItem::FileChange { tool_name, .. }
        | TurnItem::WebSearch { tool_name, .. }
        | TurnItem::WebFetch { tool_name, .. }
        | TurnItem::Download { tool_name, .. }
        | TurnItem::DynamicToolCall { tool_name, .. } => Some(tool_name.as_str()),
        _ => None,
    }
}

fn tool_item_kind(item: &TurnItem) -> &'static str {
    match item {
        TurnItem::CommandExecution { .. } => "command_execution",
        TurnItem::FileChange { .. } => "file_change",
        TurnItem::WebSearch { .. } => "web_search",
        TurnItem::WebFetch { .. } => "web_fetch",
        TurnItem::Download { .. } => "download",
        TurnItem::DynamicToolCall { .. } => "dynamic_tool_call",
        _ => "unknown",
    }
}

fn tool_item_output(item: &TurnItem) -> Option<String> {
    match item {
        TurnItem::CommandExecution { display, .. } => tool_display_text(display),
        TurnItem::FileChange { display, .. }
        | TurnItem::WebSearch { display, .. }
        | TurnItem::Download { display, .. }
        | TurnItem::DynamicToolCall { display, .. } => tool_display_text(display),
        TurnItem::WebFetch { .. } => None,
        _ => None,
    }
}

fn tool_item_terminal_status(item: &TurnItem) -> TimelineEntryStatus {
    match item {
        TurnItem::CommandExecution {
            status, success, ..
        }
        | TurnItem::FileChange {
            status, success, ..
        }
        | TurnItem::WebSearch {
            status, success, ..
        }
        | TurnItem::WebFetch {
            status, success, ..
        }
        | TurnItem::Download {
            status, success, ..
        }
        | TurnItem::DynamicToolCall {
            status, success, ..
        } => {
            if matches!(status, ToolCallStatus::Failed) || matches!(success, Some(false)) {
                TimelineEntryStatus::Failed
            } else if matches!(status, ToolCallStatus::Completed) {
                TimelineEntryStatus::Completed
            } else {
                TimelineEntryStatus::Running
            }
        }
        _ => TimelineEntryStatus::Completed,
    }
}

fn render_started_text(item: &TurnItem, tool_name: &str) -> String {
    let kind = tool_item_kind(item);
    match item {
        TurnItem::CommandExecution { command, cwd, .. } => {
            let mut lines = vec![
                client_tool_label!("timeline.tool.started", tool_name = tool_name).to_string(),
            ];
            if !command.is_empty() {
                lines.push(
                    client_tool_label!("timeline.tool.command", command = command.join(" "))
                        .to_string(),
                );
            }
            if let Some(cwd) = cwd.as_deref() {
                lines.push(client_tool_label!("timeline.tool.cwd", cwd = cwd).to_string());
            }
            lines.join("\n")
        }
        TurnItem::WebSearch { query, .. } => {
            if let Some(query) = query.as_deref() {
                return format!(
                    "{}\n{}",
                    client_tool_label!("timeline.tool.started", tool_name = tool_name),
                    client_tool_label!("timeline.tool.query", query = query)
                );
            }
            client_tool_label!("timeline.tool.started", tool_name = tool_name).to_string()
        }
        TurnItem::WebFetch { url, .. } | TurnItem::Download { url, .. } => {
            if let Some(url) = url.as_deref() {
                return format!(
                    "{}\n{}",
                    client_tool_label!("timeline.tool.started", tool_name = tool_name),
                    client_tool_label!("timeline.tool.url", url = url)
                );
            }
            client_tool_label!("timeline.tool.started", tool_name = tool_name).to_string()
        }
        _ => client_tool_label!(
            "timeline.tool.started_kind",
            tool_name = tool_name,
            kind = kind
        )
        .to_string(),
    }
}

fn render_completed_text(item: &TurnItem, tool_name: &str) -> String {
    match item {
        TurnItem::CommandExecution { storage, .. } => {
            let mut lines = vec![
                client_tool_label!("timeline.tool.finished", tool_name = tool_name).to_string(),
            ];
            if let ToolStoragePayload::Shell {
                exit_code,
                duration_ms,
                timed_out,
                ..
            } = storage
            {
                if let Some(exit_code) = exit_code {
                    lines.push(
                        client_tool_label!("timeline.tool.exit_code", exit_code = exit_code)
                            .to_string(),
                    );
                }
                if let Some(duration_ms) = duration_ms {
                    lines.push(
                        client_tool_label!("timeline.tool.duration_ms", duration_ms = duration_ms)
                            .to_string(),
                    );
                }
                if timed_out.unwrap_or(false) {
                    lines.push(client_tool_label!("timeline.tool.timed_out").to_string());
                }
            }
            lines.join("\n")
        }
        TurnItem::FileChange {
            changed_files,
            exit_code,
            ..
        } => {
            let mut lines = vec![
                client_tool_label!("timeline.tool.finished", tool_name = tool_name).to_string(),
            ];
            if let Some(exit_code) = exit_code {
                lines.push(
                    client_tool_label!("timeline.tool.exit_code", exit_code = exit_code)
                        .to_string(),
                );
            }
            if !changed_files.is_empty() {
                lines.push(client_tool_label!("timeline.tool.changed_files").to_string());
                for file in changed_files.iter().take(20) {
                    lines.push(format!("- {file}"));
                }
            }
            lines.join("\n")
        }
        TurnItem::WebSearch {
            query,
            result_count,
            provider,
            ..
        } => {
            let mut lines = vec![
                client_tool_label!("timeline.tool.finished", tool_name = tool_name).to_string(),
            ];
            if let Some(query) = query.as_deref() {
                lines.push(client_tool_label!("timeline.tool.query", query = query).to_string());
            }
            if let Some(result_count) = result_count {
                lines.push(
                    client_tool_label!("timeline.tool.results", count = result_count).to_string(),
                );
            }
            if let Some(provider) = provider.as_deref() {
                lines.push(
                    client_tool_label!("timeline.tool.provider", provider = provider).to_string(),
                );
            }
            lines.join("\n")
        }
        TurnItem::WebFetch {
            final_url,
            status_code,
            title,
            ..
        } => {
            let mut lines = vec![
                client_tool_label!("timeline.tool.finished", tool_name = tool_name).to_string(),
            ];
            if let Some(final_url) = final_url.as_deref() {
                lines.push(client_tool_label!("timeline.tool.url", url = final_url).to_string());
            }
            if let Some(status_code) = status_code {
                lines.push(
                    client_tool_label!("timeline.tool.status_code", status_code = status_code)
                        .to_string(),
                );
            }
            if let Some(title) = title.as_deref() {
                lines.push(client_tool_label!("timeline.tool.title", title = title).to_string());
            }
            lines.join("\n")
        }
        TurnItem::Download {
            final_url,
            path,
            bytes_written,
            ..
        } => {
            let mut lines = vec![
                client_tool_label!("timeline.tool.finished", tool_name = tool_name).to_string(),
            ];
            if let Some(final_url) = final_url.as_deref() {
                lines.push(client_tool_label!("timeline.tool.url", url = final_url).to_string());
            }
            if let Some(path) = path.as_deref() {
                lines.push(client_tool_label!("timeline.tool.path", path = path).to_string());
            }
            if let Some(bytes_written) = bytes_written {
                lines.push(
                    client_tool_label!("timeline.tool.bytes", bytes = bytes_written).to_string(),
                );
            }
            lines.join("\n")
        }
        TurnItem::DynamicToolCall { .. } => {
            client_tool_label!("timeline.tool.finished", tool_name = tool_name).to_string()
        }
        _ => client_tool_label!("timeline.tool.finished", tool_name = tool_name).to_string(),
    }
}

fn render_tool_delta(
    delta: &str,
    stream: Option<ItemDeltaStream>,
    payload: Option<&JsonValue>,
) -> String {
    if !delta.is_empty() {
        return match stream.unwrap_or(ItemDeltaStream::Generic) {
            ItemDeltaStream::ToolProgress => {
                format!(
                    "{}\n",
                    client_tool_label!("timeline.tool.progress", value = delta)
                )
            }
            ItemDeltaStream::Stderr => {
                client_tool_label!("timeline.tool.stderr", value = delta).to_string()
            }
            _ => delta.to_owned(),
        };
    }

    let Some(payload) = payload else {
        return String::new();
    };
    if let Some(status) = payload.get("status").and_then(JsonValue::as_str) {
        return format!(
            "{}\n",
            client_tool_label!("timeline.tool.progress", value = status)
        );
    }
    String::new()
}
