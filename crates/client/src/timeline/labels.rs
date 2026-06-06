//! UI-neutral timeline labels and status codes.

use super::rows::{TimelineCoalescedToolsKind, TimelineCoalescedToolsRow};
use crate::conversation::{ItemView, TimelineEntryStatus};
use pioneer_protocol::{
    ArtifactRef, SystemEventLevel, ToolDisplayPayload, ToolMetadataValue, ToolStoragePayload,
    TurnItem, UserMessageAttachment,
};
use serde_json::Value as JsonValue;
use std::{
    collections::BTreeMap,
    hash::{Hash, Hasher},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn now_unix_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

pub fn format_elapsed_ms(elapsed_ms: u64) -> String {
    let total_seconds = elapsed_ms / 1_000;
    let hours = total_seconds / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;

    if hours > 0 {
        format!("{hours}h {minutes:02}m")
    } else if minutes > 0 {
        format!("{minutes}m {seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

pub fn format_item_elapsed(item_view: &ItemView) -> Option<String> {
    let started = item_view.started_at_unix_ms?;
    let ended = item_view
        .completed_at_unix_ms
        .or(item_view.updated_at_unix_ms)
        .unwrap_or(started);
    Some(format_elapsed_ms(ended.saturating_sub(started) as u64))
}

pub fn timeline_entry_text(item_view: &ItemView) -> &str {
    item_view
        .final_text
        .as_deref()
        .unwrap_or(item_view.partial_text.as_str())
}

pub fn timeline_work_group_completed_label() -> &'static str {
    "Completed"
}

pub fn coalesced_tools_label(group: &TimelineCoalescedToolsRow) -> String {
    match group.kind {
        TimelineCoalescedToolsKind::CompletedTaskTools => {
            format!("{} completed tool calls", group.count)
        }
        TimelineCoalescedToolsKind::RepeatedTaskWait => {
            format!("{} repeated task_wait calls", group.count)
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ParsedUserAttachment {
    pub display_name: String,
    pub kind: ParsedUserAttachmentKind,
    pub artifact: Option<ArtifactRef>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ParsedUserAttachmentKind {
    File,
    Skill,
    Mcp,
}

pub fn parse_user_attachments(attachments: &[UserMessageAttachment]) -> Vec<ParsedUserAttachment> {
    attachments
        .iter()
        .map(|attachment| ParsedUserAttachment {
            display_name: display_name_from_attachment(attachment),
            kind: attachment_kind(attachment),
            artifact: artifact_from_attachment(attachment),
        })
        .collect()
}

pub fn attachment_kind(attachment: &UserMessageAttachment) -> ParsedUserAttachmentKind {
    match attachment {
        UserMessageAttachment::Skill { .. } => ParsedUserAttachmentKind::Skill,
        UserMessageAttachment::McpServer { .. } | UserMessageAttachment::McpTool { .. } => {
            ParsedUserAttachmentKind::Mcp
        }
        _ => ParsedUserAttachmentKind::File,
    }
}

pub fn display_name_from_attachment(attachment: &UserMessageAttachment) -> String {
    let source = match attachment {
        UserMessageAttachment::Image { url }
        | UserMessageAttachment::File { url }
        | UserMessageAttachment::Audio { url }
        | UserMessageAttachment::Video { url } => url.as_str(),
        UserMessageAttachment::LocalImage { path }
        | UserMessageAttachment::LocalFile { path }
        | UserMessageAttachment::LocalAudio { path }
        | UserMessageAttachment::LocalVideo { path } => path.as_str(),
        UserMessageAttachment::Artifact { artifact } => return artifact.display_name.clone(),
        UserMessageAttachment::Skill { capability } => return capability.label.clone(),
        UserMessageAttachment::McpServer { capability } => return capability.label.clone(),
        UserMessageAttachment::McpTool { capability } => return capability.label.clone(),
    };

    if source.contains("://") || source.starts_with("data:") {
        let without_query = source.split_once('?').map_or(source, |(value, _)| value);
        let without_fragment = without_query
            .split_once('#')
            .map_or(without_query, |(value, _)| value);
        let candidate = without_fragment
            .rsplit('/')
            .next()
            .unwrap_or(without_fragment);
        if candidate.is_empty() {
            source.to_owned()
        } else {
            candidate.to_owned()
        }
    } else {
        Path::new(source)
            .file_name()
            .and_then(|value| value.to_str())
            .map(str::to_owned)
            .unwrap_or_else(|| source.to_owned())
    }
}

pub fn artifact_from_attachment(attachment: &UserMessageAttachment) -> Option<ArtifactRef> {
    match attachment {
        UserMessageAttachment::Artifact { artifact } => Some(artifact.clone()),
        _ => None,
    }
}

pub fn stable_user_message_attachment_chip_id(item_id: &str, chip_index: usize) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    item_id.hash(&mut hasher);
    chip_index.hash(&mut hasher);
    hasher.finish()
}

pub fn command_from_arguments(arguments: &JsonValue) -> Option<String> {
    if let Some(cmd) = arguments.get("cmd").and_then(JsonValue::as_str)
        && !cmd.trim().is_empty()
    {
        return Some(cmd.to_owned());
    }

    arguments
        .get("command")
        .and_then(JsonValue::as_array)
        .map(|parts| {
            parts
                .iter()
                .filter_map(JsonValue::as_str)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|cmd| !cmd.trim().is_empty())
}

pub fn shell_output_from_display(display: &ToolDisplayPayload) -> Option<&str> {
    match display {
        ToolDisplayPayload::Shell {
            aggregated_output,
            stdout,
            stderr,
            ..
        } => aggregated_output
            .as_deref()
            .or_else(|| stdout.as_deref())
            .or_else(|| stderr.as_deref()),
        _ => None,
    }
}

pub fn shell_output_from_storage(storage: &ToolStoragePayload) -> Option<&str> {
    match storage {
        ToolStoragePayload::Shell {
            aggregated_output,
            stdout,
            stderr,
            ..
        } => aggregated_output
            .as_deref()
            .or_else(|| stdout.as_deref())
            .or_else(|| stderr.as_deref()),
        _ => None,
    }
}

pub fn normalize_for_terminal(text: &str) -> String {
    let unified = text.replace("\r\n", "\n").replace('\r', "\n");
    unified.replace('\n', "\r\n")
}

pub fn command_execution_terminal_text(
    item: &TurnItem,
    fallback_text: &str,
    truncate_output: impl FnOnce(&str) -> String,
) -> String {
    let TurnItem::CommandExecution {
        arguments,
        command,
        display,
        storage,
        ..
    } = item
    else {
        return normalize_for_terminal(fallback_text);
    };

    let command_line = if command.is_empty() {
        command_from_arguments(arguments)
            .map(|cmd| format!("$ {cmd}"))
            .unwrap_or_default()
    } else {
        format!("$ {}", command.join(" "))
    };

    let command_output = shell_output_from_display(display)
        .or_else(|| shell_output_from_storage(storage))
        .unwrap_or_default();

    let output_block = if command_output.trim().is_empty() {
        fallback_text.to_owned()
    } else {
        truncate_output(command_output.replace('\t', "    ").as_str())
    };

    let content = if command_line.is_empty() {
        output_block
    } else {
        format!("{command_line}\n{output_block}")
    };

    normalize_for_terminal(content.as_str())
}

pub fn download_url_from_arguments(arguments: &JsonValue) -> Option<String> {
    arguments
        .get("url")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(str::to_owned)
}

pub fn web_search_query_from_arguments(arguments: &JsonValue) -> Option<String> {
    arguments
        .get("query")
        .or_else(|| arguments.get("q"))
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

pub fn web_fetch_url_from_arguments(arguments: &JsonValue) -> Option<String> {
    arguments
        .get("url")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(str::to_owned)
}

pub fn web_search_display_query(arguments: &JsonValue, query: Option<&str>) -> Option<String> {
    query
        .map(str::to_owned)
        .or_else(|| web_search_query_from_arguments(arguments))
}

pub fn web_fetch_display_url(
    arguments: &JsonValue,
    url: Option<&str>,
    final_url: Option<&str>,
) -> Option<String> {
    preferred_url(arguments, url, final_url, web_fetch_url_from_arguments)
}

pub fn download_display_url(
    arguments: &JsonValue,
    url: Option<&str>,
    final_url: Option<&str>,
) -> Option<String> {
    preferred_url(arguments, url, final_url, download_url_from_arguments)
}

fn preferred_url(
    arguments: &JsonValue,
    url: Option<&str>,
    final_url: Option<&str>,
    from_arguments: impl FnOnce(&JsonValue) -> Option<String>,
) -> Option<String> {
    final_url
        .or(url)
        .filter(|url| !url.is_empty())
        .map(str::to_owned)
        .or_else(|| from_arguments(arguments))
}

pub fn host_from_url(url: &str) -> Option<String> {
    url::Url::parse(url)
        .ok()
        .or_else(|| url::Url::parse(format!("https://{url}").as_str()).ok())
        .and_then(|parsed| parsed.host_str().map(str::to_owned))
}

pub fn default_favicon_url(page_url: &str) -> Option<String> {
    let mut parsed = url::Url::parse(page_url).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return None;
    }

    parsed.set_path("/favicon.ico");
    parsed.set_query(None);
    parsed.set_fragment(None);
    Some(parsed.to_string())
}

pub fn timeline_favicon_url(primary: Option<String>, page_url: &str) -> Option<String> {
    primary
        .and_then(|value| {
            let value = value.trim().to_owned();
            (!value.is_empty()).then_some(value)
        })
        .or_else(|| {
            host_from_url(page_url)
                .map(|host| format!("https://icons.duckduckgo.com/ip3/{host}.ico"))
        })
        .or_else(|| default_favicon_url(page_url))
}

pub fn reasoning_text(summary: &[String], content: &[String], fallback: &str) -> String {
    let mut parts = Vec::new();
    if !summary.is_empty() {
        parts.push(summary.join("\n"));
    }
    if !content.is_empty() {
        parts.push(content.join("\n"));
    }

    if parts.is_empty() {
        fallback.to_owned()
    } else {
        parts.join("\n\n")
    }
}

pub fn file_change_display_text(
    stdout: Option<&str>,
    stderr: Option<&str>,
    fallback: Option<&str>,
) -> Option<String> {
    stdout
        .or(stderr)
        .or(fallback)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
}

pub fn is_task_timeline_agent_message(item_view: &ItemView) -> bool {
    item_view.timeline_origin.as_ref().is_some_and(|origin| {
        origin.task_id.is_some() || origin.run_id.is_some() || origin.child_turn_id.is_some()
    })
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum TimelineFinalStatusKind {
    Cancelled,
    Blocked,
    Failed,
    Running,
    Completed,
}

impl TimelineFinalStatusKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Cancelled => "Cancelled",
            Self::Blocked => "Blocked",
            Self::Failed => "Failed",
            Self::Running => "Running",
            Self::Completed => "Completed",
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TimelineFinalStatus {
    pub kind: TimelineFinalStatusKind,
    pub successful: bool,
}

impl TimelineFinalStatus {
    pub fn new(kind: TimelineFinalStatusKind, successful: bool) -> Self {
        Self { kind, successful }
    }

    pub fn label(self) -> &'static str {
        self.kind.label()
    }

    pub fn into_label_tuple(self) -> (String, bool) {
        (self.label().to_owned(), self.successful)
    }
}

pub fn final_download_status(
    status: TimelineEntryStatus,
    success: Option<bool>,
    status_code: Option<u16>,
) -> TimelineFinalStatus {
    final_http_tool_status(status, success, status_code)
}

pub fn final_web_fetch_status(
    status: TimelineEntryStatus,
    success: Option<bool>,
    status_code: Option<u16>,
) -> (String, bool) {
    final_http_tool_status(status, success, status_code).into_label_tuple()
}

pub fn final_file_change_status(
    status: TimelineEntryStatus,
    success: Option<bool>,
    exit_code: Option<i32>,
) -> TimelineFinalStatus {
    match status {
        TimelineEntryStatus::Cancelled => {
            TimelineFinalStatus::new(TimelineFinalStatusKind::Cancelled, false)
        }
        TimelineEntryStatus::Blocked => {
            TimelineFinalStatus::new(TimelineFinalStatusKind::Blocked, false)
        }
        TimelineEntryStatus::Failed => {
            TimelineFinalStatus::new(TimelineFinalStatusKind::Failed, false)
        }
        TimelineEntryStatus::Running => {
            TimelineFinalStatus::new(TimelineFinalStatusKind::Running, true)
        }
        TimelineEntryStatus::Completed => {
            if matches!(success, Some(false)) || exit_code.is_some_and(|code| code != 0) {
                TimelineFinalStatus::new(TimelineFinalStatusKind::Failed, false)
            } else {
                TimelineFinalStatus::new(TimelineFinalStatusKind::Completed, true)
            }
        }
    }
}

pub fn format_bytes_human(bytes: u64) -> String {
    let mut value = bytes as f64;
    let mut unit_idx = 0usize;
    while value >= 1024.0 && unit_idx < 4 {
        value /= 1024.0;
        unit_idx += 1;
    }
    let unit = byte_unit_label(unit_idx);

    if unit_idx == 0 {
        format!("{bytes} {unit}")
    } else if value.fract() < 0.05 {
        format!("{value:.0} {unit}")
    } else {
        format!("{value:.1} {unit}")
    }
}

fn final_http_tool_status(
    status: TimelineEntryStatus,
    success: Option<bool>,
    status_code: Option<u16>,
) -> TimelineFinalStatus {
    match status {
        TimelineEntryStatus::Cancelled => {
            TimelineFinalStatus::new(TimelineFinalStatusKind::Cancelled, false)
        }
        TimelineEntryStatus::Blocked => {
            TimelineFinalStatus::new(TimelineFinalStatusKind::Blocked, false)
        }
        TimelineEntryStatus::Failed => {
            TimelineFinalStatus::new(TimelineFinalStatusKind::Failed, false)
        }
        TimelineEntryStatus::Running => {
            TimelineFinalStatus::new(TimelineFinalStatusKind::Running, true)
        }
        TimelineEntryStatus::Completed => {
            if matches!(success, Some(false)) || status_code.is_some_and(|code| code >= 400) {
                TimelineFinalStatus::new(TimelineFinalStatusKind::Failed, false)
            } else {
                TimelineFinalStatus::new(TimelineFinalStatusKind::Completed, true)
            }
        }
    }
}

fn byte_unit_label(unit_idx: usize) -> &'static str {
    match unit_idx {
        0 => "B",
        1 => "KB",
        2 => "MB",
        3 => "GB",
        _ => "TB",
    }
}

pub fn final_dynamic_tool_status(
    status: TimelineEntryStatus,
    success: Option<bool>,
) -> (String, bool) {
    match status {
        TimelineEntryStatus::Cancelled => ("Cancelled".to_owned(), false),
        TimelineEntryStatus::Blocked => ("Blocked".to_owned(), false),
        TimelineEntryStatus::Failed => ("Failed".to_owned(), false),
        TimelineEntryStatus::Running => ("Running".to_owned(), true),
        TimelineEntryStatus::Completed => {
            if matches!(success, Some(false)) {
                ("Failed".to_owned(), false)
            } else {
                ("Completed".to_owned(), true)
            }
        }
    }
}

pub fn pretty_json(value: &JsonValue) -> Option<String> {
    if value.is_null() {
        return None;
    }
    let text = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    (!text.trim().is_empty() && text.trim() != "{}").then_some(text)
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct McpTimelineMetadata {
    pub server_id: Option<String>,
    pub server_name: String,
    pub raw_tool_name: String,
    pub catalog_version: Option<String>,
    pub snapshot_version: Option<u64>,
    pub runtime_state: Option<String>,
    pub duration_ms: Option<u64>,
    pub result_truncated: Option<bool>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TaskWaitReviewDisplay {
    pub review_required_count: u32,
    pub mode: Option<String>,
    pub items: Vec<TaskWaitReviewDisplayItem>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TaskWaitReviewDisplayItem {
    pub task_id: String,
    pub run_id: Option<String>,
    pub title: Option<String>,
    pub status: Option<String>,
    pub candidate_id: String,
    pub candidate_status: Option<String>,
    pub review_mode: Option<String>,
    pub user_approval_required: bool,
    pub round: Option<u32>,
    pub summary: Option<String>,
    pub result_preview: Option<String>,
    pub extraction_error_preview: Option<String>,
    pub diagnostics: Vec<String>,
    pub max_revision_rounds: Option<u32>,
    pub remaining_revision_rounds: Option<u32>,
    pub allowed_actions: Vec<String>,
    pub revision_blocked_reason: Option<String>,
}

impl McpTimelineMetadata {
    pub fn label(&self) -> String {
        format!("{}/{}", self.server_name, self.raw_tool_name)
    }

    pub fn details(&self) -> String {
        let mut lines = vec![
            format!("Server: {}", self.server_name),
            format!("Tool: {}", self.raw_tool_name),
        ];
        if let Some(catalog_version) = self.catalog_version.as_deref() {
            lines.push(format!("Catalog: {catalog_version}"));
        }
        if let Some(snapshot_version) = self.snapshot_version {
            lines.push(format!("Snapshot: {snapshot_version}"));
        }
        if let Some(runtime_state) = self.runtime_state.as_deref() {
            lines.push(format!("Runtime: {runtime_state}"));
        }
        if let Some(duration_ms) = self.duration_ms {
            lines.push(format!("Duration: {duration_ms} ms"));
        }
        if self.result_truncated == Some(true) {
            lines.push("Result: truncated".to_owned());
        }
        lines.join("\n")
    }
}

impl TaskWaitReviewDisplay {
    pub fn details(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "{} review-required candidate(s)",
            self.review_required_count.max(self.items.len() as u32)
        ));
        if let Some(mode) = self.mode.as_deref() {
            lines.push(format!("Wait mode: {mode}"));
        }

        for (index, item) in self.items.iter().enumerate() {
            if !lines.is_empty() {
                lines.push(String::new());
            }
            lines.push(format!("Candidate {}", index + 1));
            lines.extend(item.details());
        }

        lines.join("\n")
    }
}

impl TaskWaitReviewDisplayItem {
    pub fn user_controls_allowed(&self) -> bool {
        self.user_approval_required && self.review_mode.as_deref() == Some("user_approval")
    }

    pub fn allows_action(&self, action: &str) -> bool {
        self.allowed_actions.iter().any(|value| value == action)
    }

    pub fn details(&self) -> Vec<String> {
        let mut lines = Vec::new();
        if let Some(title) = self.title.as_deref() {
            lines.push(format!("Task: {title}"));
        }
        lines.push(format!("Task id: {}", self.task_id));
        if let Some(run_id) = self.run_id.as_deref() {
            lines.push(format!("Run id: {run_id}"));
        }
        lines.push(format!("Candidate id: {}", self.candidate_id));
        if let Some(status) = self.status.as_deref() {
            lines.push(format!("Task status: {status}"));
        }
        if let Some(candidate_status) = self.candidate_status.as_deref() {
            lines.push(format!("Candidate status: {candidate_status}"));
        }
        if let Some(round) = self.round {
            lines.push(format!("Round: {round}"));
        }
        if let Some(review_mode) = self.review_mode.as_deref() {
            lines.push(format!("Review mode: {review_mode}"));
        }
        if self.user_approval_required {
            lines.push("User approval required".to_owned());
        }
        if !self.allowed_actions.is_empty() {
            lines.push(format!(
                "Action required: {}",
                self.allowed_actions.join(" or ")
            ));
        }
        if let Some(remaining) = self.remaining_revision_rounds {
            let max = self
                .max_revision_rounds
                .map(|max| max.to_string())
                .unwrap_or_else(|| "?".to_owned());
            lines.push(format!("Revision rounds remaining: {remaining}/{max}"));
        }
        if let Some(reason) = self.revision_blocked_reason.as_deref() {
            lines.push(format!("Revision blocked: {reason}"));
        }
        if let Some(summary) = self.summary.as_deref() {
            lines.push(format!("Summary: {summary}"));
        }
        if let Some(result_preview) = self.result_preview.as_deref() {
            lines.push(format!("Result preview: {result_preview}"));
        }
        if let Some(error_preview) = self.extraction_error_preview.as_deref() {
            lines.push(format!("Extraction error: {error_preview}"));
        }
        if !self.diagnostics.is_empty() {
            lines.push(format!("Diagnostics: {}", self.diagnostics.join("; ")));
        }
        lines
    }
}

pub fn mcp_timeline_metadata(display: &ToolDisplayPayload) -> Option<McpTimelineMetadata> {
    let metadata = match display {
        ToolDisplayPayload::Summary(summary) => &summary.metadata,
        ToolDisplayPayload::Progress { metadata, .. } => metadata,
        _ => return None,
    };
    let source_is_mcp = metadata
        .get("source")
        .and_then(ToolMetadataValue::as_str)
        .is_some_and(|source| source == "mcp");
    let mcp = metadata.get("mcp").and_then(ToolMetadataValue::as_object)?;

    if !source_is_mcp && mcp.is_empty() {
        return None;
    }

    Some(McpTimelineMetadata {
        server_id: metadata_string(mcp, &["server_id", "serverId"]),
        server_name: metadata_string(mcp, &["server_name", "serverName"])?,
        raw_tool_name: metadata_string(mcp, &["raw_tool_name", "rawToolName"])?,
        catalog_version: metadata_string(mcp, &["catalog_version", "catalogVersion"]),
        snapshot_version: metadata_u64(mcp, &["snapshot_version", "snapshotVersion"]),
        runtime_state: metadata_string(mcp, &["runtime_state", "runtimeState"]),
        duration_ms: metadata_u64(mcp, &["duration_ms", "durationMs"])
            .or_else(|| {
                metadata
                    .get("duration_ms")
                    .and_then(ToolMetadataValue::as_u64)
            })
            .or_else(|| {
                metadata
                    .get("durationMs")
                    .and_then(ToolMetadataValue::as_u64)
            }),
        result_truncated: metadata_bool(mcp, &["result_truncated", "resultTruncated"]).or_else(
            || {
                metadata
                    .get("truncated")
                    .and_then(ToolMetadataValue::as_bool)
            },
        ),
    })
}

pub fn task_wait_review_display(
    tool_name: &str,
    display: &ToolDisplayPayload,
) -> Option<TaskWaitReviewDisplay> {
    if tool_name != "task_wait" {
        return None;
    }
    let value = display_summary_sanitized_result(display)?;
    let review_required = value.get("reviewRequired")?.as_array()?;
    if review_required.is_empty() {
        return None;
    }

    let items = review_required
        .iter()
        .filter_map(task_wait_review_display_item)
        .collect::<Vec<_>>();
    if items.is_empty() {
        return None;
    }

    Some(TaskWaitReviewDisplay {
        review_required_count: json_u32(&value, "reviewRequiredCount")
            .unwrap_or(items.len() as u32),
        mode: json_string(&value, "mode"),
        items,
    })
}

fn display_summary_sanitized_result(display: &ToolDisplayPayload) -> Option<JsonValue> {
    let ToolDisplayPayload::Summary(summary) = display else {
        return None;
    };
    summary
        .metadata
        .get("sanitizedResult")
        .map(ToolMetadataValue::to_json)
}

fn task_wait_review_display_item(value: &JsonValue) -> Option<TaskWaitReviewDisplayItem> {
    let task_id = json_string(value, "taskId")?;
    let candidate_id = json_string(value, "candidateId")?;
    Some(TaskWaitReviewDisplayItem {
        task_id,
        run_id: json_string(value, "runId"),
        title: json_string(value, "title"),
        status: json_string(value, "status"),
        candidate_id,
        candidate_status: json_string(value, "candidateStatus"),
        review_mode: json_string(value, "reviewMode"),
        user_approval_required: json_bool(value, "userApprovalRequired").unwrap_or(false),
        round: json_u32(value, "round"),
        summary: json_string(value, "summary"),
        result_preview: json_string(value, "resultPreview"),
        extraction_error_preview: json_string(value, "extractionErrorPreview"),
        diagnostics: json_string_array(value, "diagnostics"),
        max_revision_rounds: json_u32(value, "maxRevisionRounds"),
        remaining_revision_rounds: json_u32(value, "remainingRevisionRounds"),
        allowed_actions: json_string_array(value, "allowedActions"),
        revision_blocked_reason: json_string(value, "revisionBlockedReason"),
    })
}

pub fn task_review_action_key(candidate_id: &str, action: &str) -> String {
    format!("task-review:{candidate_id}:{action}")
}

pub fn task_review_button_id(candidate_id: &str, action: &'static str) -> (&'static str, u64) {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    candidate_id.hash(&mut hasher);
    (action, hasher.finish())
}

fn metadata_string(fields: &BTreeMap<String, ToolMetadataValue>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| fields.get(*key).and_then(ToolMetadataValue::as_str))
        .map(str::to_owned)
}

fn metadata_u64(fields: &BTreeMap<String, ToolMetadataValue>, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| fields.get(*key).and_then(ToolMetadataValue::as_u64))
}

fn metadata_bool(fields: &BTreeMap<String, ToolMetadataValue>, keys: &[&str]) -> Option<bool> {
    keys.iter()
        .find_map(|key| fields.get(*key).and_then(ToolMetadataValue::as_bool))
}

fn json_string(value: &JsonValue, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn json_bool(value: &JsonValue, key: &str) -> Option<bool> {
    value.get(key).and_then(JsonValue::as_bool)
}

fn json_u32(value: &JsonValue, key: &str) -> Option<u32> {
    value
        .get(key)
        .and_then(JsonValue::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

fn json_string_array(value: &JsonValue, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(JsonValue::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(JsonValue::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SystemEventPresentation {
    pub message: String,
    pub label: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SystemEventDetailRow {
    pub label: String,
    pub value: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct CapabilityRejectionRow {
    pub label: String,
    pub kind: String,
    pub message: String,
}

pub fn system_event_label(level: &SystemEventLevel) -> String {
    match level {
        SystemEventLevel::Info => "Info".to_owned(),
        SystemEventLevel::Warning => "Warning".to_owned(),
        SystemEventLevel::Error => "Error".to_owned(),
    }
}

pub fn pretty_details(details: &JsonValue) -> Option<String> {
    let text = serde_json::to_string_pretty(details).unwrap_or_else(|_| details.to_string());
    (!text.trim().is_empty() && text.trim() != "null").then_some(text)
}

fn capability_rejection_kind(kind: &JsonValue) -> String {
    match kind.get("type").and_then(JsonValue::as_str) {
        Some("skill") => "Skill".to_owned(),
        Some("mcpServer") => "MCP server".to_owned(),
        Some("mcpTool") => "MCP tool".to_owned(),
        _ => "Capability".to_owned(),
    }
}

fn fallback_capability_label(kind: &JsonValue) -> String {
    match kind.get("type").and_then(JsonValue::as_str) {
        Some("skill") => kind
            .get("slug")
            .and_then(JsonValue::as_str)
            .unwrap_or("skill")
            .to_owned(),
        Some("mcpServer") => kind
            .get("name")
            .and_then(JsonValue::as_str)
            .unwrap_or("MCP server")
            .to_owned(),
        Some("mcpTool") => {
            let server = kind
                .get("serverName")
                .and_then(JsonValue::as_str)
                .unwrap_or("MCP");
            let tool = kind
                .get("rawToolName")
                .and_then(JsonValue::as_str)
                .unwrap_or("tool");
            format!("{server}/{tool}")
        }
        _ => "Capability".to_owned(),
    }
}

pub fn capability_rejection_rows(details: Option<&JsonValue>) -> Vec<CapabilityRejectionRow> {
    details
        .and_then(|details| details.get("rejected"))
        .and_then(JsonValue::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let kind = item.get("kind")?;
                    let label = item
                        .get("label")
                        .and_then(JsonValue::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_owned)
                        .unwrap_or_else(|| fallback_capability_label(kind));
                    let message = item
                        .get("message")
                        .and_then(JsonValue::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())?
                        .to_owned();
                    Some(CapabilityRejectionRow {
                        label,
                        kind: capability_rejection_kind(kind),
                        message,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

pub fn capability_rejection_rows_for_event(
    code: Option<&str>,
    details: Option<&JsonValue>,
) -> Vec<CapabilityRejectionRow> {
    if code == Some("capability.rejected") {
        capability_rejection_rows(details)
    } else {
        Vec::new()
    }
}

fn detail_string(details: Option<&JsonValue>, key: &str) -> Option<String> {
    details?
        .get(key)
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn detail_u64(details: Option<&JsonValue>, key: &str) -> Option<u64> {
    details?.get(key).and_then(JsonValue::as_u64)
}

fn execution_window_code(code: Option<&str>) -> bool {
    matches!(
        code,
        Some("turn_execution_window_started")
            | Some("turn_execution_window_exhausted")
            | Some("turn_execution_window_checkpointed")
            | Some("turn_execution_window_continued")
            | Some("turn_execution_window_blocked")
    )
}

fn window_index_label(details: Option<&JsonValue>) -> Option<String> {
    detail_u64(details, "window_index").map(|window_index| format!("Window #{window_index}"))
}

fn execution_window_presentation_label(
    code: Option<&str>,
    level: &SystemEventLevel,
    details: Option<&JsonValue>,
) -> String {
    match code {
        Some("turn_execution_window_checkpointed") => "Checkpoint".to_owned(),
        Some("turn_execution_window_continued") => "Continued".to_owned(),
        Some("turn_execution_window_blocked") => "Paused".to_owned(),
        Some("turn_execution_window_started") | Some("turn_execution_window_exhausted") => {
            window_index_label(details).unwrap_or_else(|| "Execution window".to_owned())
        }
        _ => system_event_label(level),
    }
}

fn push_detail_row(
    rows: &mut Vec<SystemEventDetailRow>,
    label: impl Into<String>,
    value: Option<String>,
) {
    let Some(value) = value.map(|value| value.trim().to_owned()) else {
        return;
    };
    if value.is_empty() {
        return;
    }
    rows.push(SystemEventDetailRow {
        label: label.into(),
        value,
    });
}

pub fn execution_window_detail_rows(
    code: Option<&str>,
    details: Option<&JsonValue>,
) -> Vec<SystemEventDetailRow> {
    if !execution_window_code(code) {
        return Vec::new();
    }

    let mut rows = Vec::new();
    push_detail_row(&mut rows, "Window", window_index_label(details));
    push_detail_row(&mut rows, "Status", detail_string(details, "status"));
    if code == Some("turn_execution_window_blocked") {
        push_detail_row(&mut rows, "Reason", detail_string(details, "reason"));
        push_detail_row(
            &mut rows,
            "Window exhaustion",
            detail_string(details, "exhaustion_reason"),
        );
    } else {
        push_detail_row(
            &mut rows,
            "Reason",
            detail_string(details, "exhaustion_reason")
                .or_else(|| detail_string(details, "reason")),
        );
    }
    push_detail_row(
        &mut rows,
        "Checkpoint",
        detail_string(details, "checkpoint_id"),
    );
    push_detail_row(
        &mut rows,
        "Previous window",
        detail_u64(details, "previous_window_index")
            .map(|window_index| format!("Window #{window_index}")),
    );
    push_detail_row(
        &mut rows,
        "Limit",
        match (
            detail_u64(details, "observed"),
            detail_u64(details, "limit"),
        ) {
            (Some(observed), Some(limit)) => Some(format!("{observed}/{limit}")),
            _ => None,
        },
    );
    push_detail_row(
        &mut rows,
        "Agent rounds",
        detail_u64(details, "agent_round_count").map(|value| value.to_string()),
    );
    push_detail_row(
        &mut rows,
        "Tool calls",
        detail_u64(details, "tool_call_count")
            .or_else(|| detail_u64(details, "total_tool_calls"))
            .map(|value| value.to_string()),
    );
    push_detail_row(
        &mut rows,
        "Provider tokens",
        detail_u64(details, "provider_token_count").map(|value| value.to_string()),
    );
    push_detail_row(
        &mut rows,
        "Total windows",
        detail_u64(details, "total_windows").map(|value| value.to_string()),
    );
    push_detail_row(
        &mut rows,
        "Checkpoint kind",
        detail_string(details, "checkpoint_kind"),
    );
    push_detail_row(
        &mut rows,
        "Checkpoint size",
        detail_u64(details, "payload_bytes").map(|value| format!("{value} bytes")),
    );
    rows
}

fn tool_name_from_details(details: Option<&JsonValue>) -> String {
    detail_string(details, "tool_name").unwrap_or_else(|| "Tool".to_owned())
}

fn attempt_label(details: Option<&JsonValue>) -> Option<String> {
    detail_u64(details, "attempt_no").map(|attempt| format!("Attempt {attempt}"))
}

fn next_attempt_label(details: Option<&JsonValue>) -> Option<String> {
    detail_u64(details, "next_attempt_no").map(|attempt| format!("Attempt {attempt}"))
}

fn detail_has_string(details: Option<&JsonValue>, key: &str) -> bool {
    detail_string(details, key).is_some()
}

fn is_recovery_failure_message(message: &str) -> bool {
    message.starts_with("recovery failed for item `")
}

pub fn system_event_presentation(
    level: &SystemEventLevel,
    message: &str,
    code: Option<&str>,
    details: Option<&JsonValue>,
) -> SystemEventPresentation {
    match code {
        Some("item_timeout_detected") => {
            let recovery_started = detail_has_string(details, "recovery_job_id");
            SystemEventPresentation {
                message: if recovery_started {
                    "Timeout detected; recovery started".to_owned()
                } else {
                    "Timeout detected".to_owned()
                },
                label: attempt_label(details).unwrap_or_else(|| "Timeout".to_owned()),
            }
        }
        Some("item_recovery_opened") => SystemEventPresentation {
            message: "Recovery opened".to_owned(),
            label: attempt_label(details).unwrap_or_else(|| "Recovery".to_owned()),
        },
        Some("item_recovery_attached") => SystemEventPresentation {
            message: "Recovery attached".to_owned(),
            label: next_attempt_label(details).unwrap_or_else(|| "Recovery".to_owned()),
        },
        Some("item_retry_scheduled") => SystemEventPresentation {
            message: "Retry scheduled".to_owned(),
            label: attempt_label(details).unwrap_or_else(|| "Retry".to_owned()),
        },
        Some("item_retry_attempt_started") => SystemEventPresentation {
            message: "Retry started".to_owned(),
            label: attempt_label(details).unwrap_or_else(|| "Retry".to_owned()),
        },
        Some("item_recovery_succeeded") => SystemEventPresentation {
            message: "Recovery succeeded".to_owned(),
            label: "Recovered".to_owned(),
        },
        Some("item_recovery_exhausted") => SystemEventPresentation {
            message: "Recovery failed".to_owned(),
            label: "Error".to_owned(),
        },
        Some("item_tool_retry_scheduled") => {
            let tool_name = tool_name_from_details(details);
            SystemEventPresentation {
                message: format!("{tool_name} retry scheduled"),
                label: attempt_label(details).unwrap_or_else(|| "Retry".to_owned()),
            }
        }
        Some("item_tool_retry_resolved") => {
            let tool_name = tool_name_from_details(details);
            SystemEventPresentation {
                message: format!("{tool_name} retry resolved"),
                label: "Retry resolved".to_owned(),
            }
        }
        Some("item_tool_retry_exhausted") => {
            let tool_name = tool_name_from_details(details);
            SystemEventPresentation {
                message: format!("{tool_name} retries exhausted"),
                label: "Retries exhausted".to_owned(),
            }
        }
        Some("turn_tool_loop_budget_exceeded") => SystemEventPresentation {
            message: "Tool loop budget exceeded".to_owned(),
            label: system_event_label(level),
        },
        code if execution_window_code(code) => SystemEventPresentation {
            message: message.to_owned(),
            label: execution_window_presentation_label(code, level, details),
        },
        Some("turn_failed") if is_recovery_failure_message(message) => SystemEventPresentation {
            message: "Recovery failed".to_owned(),
            label: system_event_label(level),
        },
        _ => SystemEventPresentation {
            message: message.to_owned(),
            label: system_event_label(level),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        ArtifactKind, ArtifactStatus, McpScopeKind, TimelineLane, TimelineOrigin,
        TimelineOriginKind, ToolCallStatus, ToolDisplayPayload, ToolMetadata,
        ToolOutputPolicySnapshot, ToolOutputSummary, ToolStoragePayload, TurnItem,
        TurnMcpToolCapabilitySummary, TurnSkillCapabilitySummary,
    };
    use serde_json::json;

    fn item_view_with_origin(timeline_origin: Option<TimelineOrigin>) -> ItemView {
        ItemView {
            id: "item_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            item_type: "agentMessage".to_owned(),
            status: TimelineEntryStatus::Completed,
            started_at_unix_ms: None,
            updated_at_unix_ms: None,
            completed_at_unix_ms: None,
            partial_text: String::new(),
            final_text: None,
            partial_markdown: None,
            final_markdown: None,
            item: TurnItem::AgentMessage {
                id: "item_1".to_owned(),
                text: String::new(),
                markdown: None,
                markdown_version: None,
            },
            timeline_origin,
            opaque_meta: None,
        }
    }

    fn timeline_origin(
        task_id: Option<&str>,
        run_id: Option<&str>,
        child_turn_id: Option<&str>,
    ) -> TimelineOrigin {
        TimelineOrigin {
            kind: TimelineOriginKind::ChildTurn,
            task_id: task_id.map(str::to_owned),
            run_id: run_id.map(str::to_owned),
            child_thread_id: None,
            child_turn_id: child_turn_id.map(str::to_owned),
            origin_event_id: None,
            origin_turn_item_id: None,
            origin_sequence: 1,
            occurred_at: 2,
            lane: TimelineLane::ChildAgent,
        }
    }

    #[test]
    fn user_attachment_selectors_preserve_display_labels_and_artifacts() {
        let artifact = ArtifactRef {
            artifact_id: "art_1".to_owned(),
            version_id: Some("ver_1".to_owned()),
            display_name: "report.pdf".to_owned(),
            kind: ArtifactKind::Pdf,
            mime_type: Some("application/pdf".to_owned()),
            size_bytes: Some(2048),
            sha256: None,
            status: ArtifactStatus::Ready,
            preview: None,
        };
        let attachments = vec![
            UserMessageAttachment::LocalFile {
                path: "/tmp/report.pdf".to_owned(),
            },
            UserMessageAttachment::Artifact {
                artifact: artifact.clone(),
            },
            UserMessageAttachment::Skill {
                capability: TurnSkillCapabilitySummary {
                    id: "skill:user:docs".to_owned(),
                    label: "docs".to_owned(),
                    slug: "docs".to_owned(),
                    source_kind: "user".to_owned(),
                },
            },
            UserMessageAttachment::McpTool {
                capability: TurnMcpToolCapabilitySummary {
                    id: "mcp-tool:workspace:resend:send".to_owned(),
                    label: "resend/send".to_owned(),
                    server_name: "resend".to_owned(),
                    raw_tool_name: "send".to_owned(),
                    scope_kind: McpScopeKind::Workspace,
                },
            },
        ];

        let parsed = parse_user_attachments(&attachments);

        assert_eq!(
            parsed
                .iter()
                .map(|attachment| attachment.display_name.as_str())
                .collect::<Vec<_>>(),
            vec!["report.pdf", "report.pdf", "docs", "resend/send"]
        );
        assert_eq!(parsed[0].kind, ParsedUserAttachmentKind::File);
        assert_eq!(parsed[2].kind, ParsedUserAttachmentKind::Skill);
        assert_eq!(parsed[3].kind, ParsedUserAttachmentKind::Mcp);
        assert_eq!(parsed[1].artifact.as_ref(), Some(&artifact));
        assert_ne!(
            stable_user_message_attachment_chip_id("user_1", 0),
            stable_user_message_attachment_chip_id("user_2", 0)
        );
    }

    #[test]
    fn item_elapsed_uses_completed_then_updated_timestamp() {
        let mut item_view = item_view_with_origin(None);
        item_view.started_at_unix_ms = Some(1_000);
        item_view.updated_at_unix_ms = Some(62_000);
        item_view.completed_at_unix_ms = None;

        assert_eq!(format_item_elapsed(&item_view).as_deref(), Some("1m 01s"));

        item_view.completed_at_unix_ms = Some(3_661_000);

        assert_eq!(format_item_elapsed(&item_view).as_deref(), Some("1h 01m"));
    }

    #[test]
    fn timeline_entry_text_prefers_final_text() {
        let mut item_view = item_view_with_origin(None);
        item_view.partial_text = "partial".to_owned();

        assert_eq!(timeline_entry_text(&item_view), "partial");

        item_view.final_text = Some("final".to_owned());

        assert_eq!(timeline_entry_text(&item_view), "final");
    }

    #[test]
    fn task_timeline_agent_message_detects_task_run_or_child_turn_origin() {
        assert!(!is_task_timeline_agent_message(&item_view_with_origin(
            None
        )));
        assert!(is_task_timeline_agent_message(&item_view_with_origin(
            Some(timeline_origin(Some("task_1"), None, None))
        )));
        assert!(is_task_timeline_agent_message(&item_view_with_origin(
            Some(timeline_origin(None, Some("run_1"), None))
        )));
        assert!(is_task_timeline_agent_message(&item_view_with_origin(
            Some(timeline_origin(None, None, Some("turn_child")))
        )));
        assert!(!is_task_timeline_agent_message(&item_view_with_origin(
            Some(timeline_origin(None, None, None))
        )));
    }

    #[test]
    fn command_and_web_fetch_selectors_normalize_shell_neutral_data() {
        assert_eq!(
            command_from_arguments(&json!({ "command": ["cargo", "check"] })).as_deref(),
            Some("cargo check")
        );
        assert_eq!(normalize_for_terminal("a\nb"), "a\r\nb");
        assert_eq!(
            download_url_from_arguments(&json!({ "url": " https://example.com/file.zip " }))
                .as_deref(),
            Some("https://example.com/file.zip")
        );
        assert_eq!(
            web_search_query_from_arguments(&json!({ "q": " rust schemars " })).as_deref(),
            Some("rust schemars")
        );
        assert_eq!(
            web_search_display_query(&json!({ "q": " rust schemars " }), Some("explicit"))
                .as_deref(),
            Some("explicit")
        );
        assert_eq!(
            web_fetch_url_from_arguments(&json!({ "url": " https://example.com/a " })).as_deref(),
            Some("https://example.com/a")
        );
        assert_eq!(
            web_fetch_display_url(
                &json!({ "url": "https://example.com/a" }),
                Some("https://example.com/b"),
                Some("https://example.com/final")
            )
            .as_deref(),
            Some("https://example.com/final")
        );
        assert_eq!(
            download_display_url(
                &json!({ "url": " https://example.com/file.zip " }),
                None,
                None
            )
            .as_deref(),
            Some("https://example.com/file.zip")
        );
        assert_eq!(
            host_from_url("example.com/a").as_deref(),
            Some("example.com")
        );
        assert_eq!(
            default_favicon_url("https://example.com/a?x=1#frag").as_deref(),
            Some("https://example.com/favicon.ico")
        );
        assert_eq!(
            timeline_favicon_url(
                Some(" https://cdn.example/icon.png ".to_owned()),
                "https://example.com/a"
            )
            .as_deref(),
            Some("https://cdn.example/icon.png")
        );
        assert_eq!(
            timeline_favicon_url(None, "https://example.com/a").as_deref(),
            Some("https://icons.duckduckgo.com/ip3/example.com.ico")
        );
        assert_eq!(
            final_web_fetch_status(TimelineEntryStatus::Completed, Some(true), Some(200)),
            ("Completed".to_owned(), true)
        );
        assert_eq!(
            final_web_fetch_status(TimelineEntryStatus::Completed, Some(true), Some(500)),
            ("Failed".to_owned(), false)
        );
        assert_eq!(
            final_download_status(TimelineEntryStatus::Completed, Some(true), Some(200)),
            TimelineFinalStatus::new(TimelineFinalStatusKind::Completed, true)
        );
        assert_eq!(
            final_file_change_status(TimelineEntryStatus::Completed, Some(true), Some(1)),
            TimelineFinalStatus::new(TimelineFinalStatusKind::Failed, false)
        );
        assert_eq!(format_bytes_human(1536), "1.5 KB");
        assert_eq!(
            reasoning_text(&["summary".to_owned()], &["detail".to_owned()], "fallback"),
            "summary\n\ndetail"
        );
        assert_eq!(
            file_change_display_text(None, Some(" stderr "), Some("fallback")).as_deref(),
            Some(" stderr ")
        );
    }

    #[test]
    fn command_execution_terminal_text_builds_command_line_output_and_fallback() {
        let item = TurnItem::CommandExecution {
            id: "item_1".to_owned(),
            tool_name: "exec_command".to_owned(),
            arguments: json!({ "cmd": "cargo test" }),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("exec_command"),
            display: ToolDisplayPayload::Shell {
                stdout: Some("ok\t1".to_owned()),
                stderr: None,
                aggregated_output: None,
                exit_code: Some(0),
                duration_ms: None,
                timed_out: None,
                truncated: false,
            },
            storage: ToolStoragePayload::default(),
            recovery: None,
            command: Vec::new(),
            cwd: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };

        assert_eq!(
            command_execution_terminal_text(&item, "fallback", |output| output.to_owned()),
            "$ cargo test\r\nok    1"
        );

        let fallback_item = TurnItem::CommandExecution {
            id: "item_2".to_owned(),
            tool_name: "exec_command".to_owned(),
            arguments: json!({ "cmd": "cargo test" }),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("exec_command"),
            display: ToolDisplayPayload::default(),
            storage: ToolStoragePayload::default(),
            recovery: None,
            command: Vec::new(),
            cwd: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };
        assert_eq!(
            command_execution_terminal_text(&fallback_item, "fallback\ntext", |output| {
                output.to_owned()
            }),
            "$ cargo test\r\nfallback\r\ntext"
        );
    }

    #[test]
    fn dynamic_tool_selectors_extract_mcp_and_review_metadata() {
        let display = ToolDisplayPayload::Summary(ToolOutputSummary {
            title: "MCP resend/send completed".to_owned(),
            lines: vec!["12 ms".to_owned()],
            metadata: ToolMetadata::from_json(json!({
                "source": "mcp",
                "duration_ms": 12,
                "mcp": {
                    "server_name": "resend",
                    "raw_tool_name": "send",
                    "catalog_version": "cat_1",
                    "snapshot_version": 7,
                    "runtime_state": "ready",
                    "result_truncated": false
                }
            })),
            truncated: false,
        });
        let metadata = mcp_timeline_metadata(&display).expect("mcp metadata");
        assert_eq!(metadata.label(), "resend/send");
        assert_eq!(metadata.duration_ms, Some(12));

        let review_display = ToolDisplayPayload::Summary(ToolOutputSummary {
            title: "task_wait completed".to_owned(),
            lines: Vec::new(),
            metadata: ToolMetadata::from_json(json!({
                "sanitizedResult": {
                    "reviewRequiredCount": 1,
                    "mode": "user_approval",
                    "reviewRequired": [{
                        "taskId": "task_1",
                        "runId": "run_1",
                        "candidateId": "candidate_1",
                        "reviewMode": "user_approval",
                        "userApprovalRequired": true,
                        "allowedActions": ["task_accept", "task_revise"]
                    }]
                }
            })),
            truncated: false,
        });
        let review = task_wait_review_display("task_wait", &review_display).expect("review");
        assert_eq!(review.review_required_count, 1);
        assert!(review.items[0].user_controls_allowed());
        assert!(review.items[0].allows_action("task_accept"));
        assert_eq!(
            task_review_action_key("candidate_1", "accept"),
            "task-review:candidate_1:accept"
        );

        let parent_agent_review_display = ToolDisplayPayload::Summary(ToolOutputSummary {
            title: "task_wait completed".to_owned(),
            lines: Vec::new(),
            metadata: ToolMetadata::from_json(json!({
                "sanitizedResult": {
                    "mode": "all_terminal_or_review_required",
                    "reviewRequiredCount": 1,
                    "reviewRequired": [{
                        "taskId": "task_1",
                        "runId": "run_1",
                        "candidateId": "candidate_parent_1",
                        "reviewMode": "parent_agent",
                        "userApprovalRequired": false,
                        "allowedActions": ["task_accept", "task_revise", "task_cancel"]
                    }]
                }
            })),
            truncated: false,
        });
        let review =
            task_wait_review_display("task_wait", &parent_agent_review_display).expect("review");
        assert_eq!(review.items[0].review_mode.as_deref(), Some("parent_agent"));
        assert!(!review.items[0].user_controls_allowed());
        assert!(review.details().contains("Review mode: parent_agent"));
        assert!(!review.details().contains("User approval required"));
    }

    #[test]
    fn system_event_selectors_render_capability_and_window_rows() {
        let details = json!({
            "rejected": [{
                "kind": {
                    "type": "mcpTool",
                    "serverName": "resend",
                    "rawToolName": "send"
                },
                "message": "MCP server `resend` does not expose tool `send`."
            }]
        });
        let rows = capability_rejection_rows_for_event(Some("capability.rejected"), Some(&details));
        assert_eq!(rows[0].label, "resend/send");
        assert_eq!(rows[0].kind, "MCP tool");

        let malformed_details = json!({
            "rejected": [
                {
                    "kind": {
                        "type": "mcpServer",
                        "name": "github"
                    },
                    "message": "MCP server `github` is disabled."
                },
                {
                    "kind": {
                        "type": "skill",
                        "slug": "missing-message"
                    }
                }
            ]
        });
        assert!(
            capability_rejection_rows_for_event(Some("other.event"), Some(&malformed_details))
                .is_empty()
        );
        let rows = capability_rejection_rows(Some(&malformed_details));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].label, "github");
        assert_eq!(rows[0].kind, "MCP server");

        let continued_details = json!({
            "window_index": 2,
            "status": "continued",
            "previous_window_index": 1,
            "checkpoint_id": "chk_000000000000000001",
            "payload": {
                "large": "payload must not be rendered inline"
            }
        });
        let presentation = system_event_presentation(
            &SystemEventLevel::Info,
            "Continued in execution window #2 after window #1 limit",
            Some("turn_execution_window_continued"),
            Some(&continued_details),
        );
        assert_eq!(presentation.label, "Continued");
        let rows = execution_window_detail_rows(
            Some("turn_execution_window_continued"),
            Some(&continued_details),
        );
        assert!(
            rows.iter()
                .any(|row| row.label == "Previous window" && row.value == "Window #1")
        );
        assert!(
            !rows
                .iter()
                .any(|row| row.label == "payload" || row.value.contains("large"))
        );

        let window_details = json!({
            "window_index": 3,
            "status": "blocked",
            "exhaustion_reason": "max_agent_rounds_per_window",
            "checkpoint_id": "chk_1",
            "total_windows": 3,
            "total_tool_calls": 384,
            "reason": "max_total_windows_exceeded"
        });
        let presentation = system_event_presentation(
            &SystemEventLevel::Warning,
            "Execution paused: max_total_windows_exceeded",
            Some("turn_execution_window_blocked"),
            Some(&window_details),
        );
        assert_eq!(presentation.label, "Paused");
        let rows = execution_window_detail_rows(
            Some("turn_execution_window_blocked"),
            Some(&window_details),
        );
        assert!(
            rows.iter()
                .any(|row| row.label == "Reason" && row.value == "max_total_windows_exceeded")
        );
    }
}
