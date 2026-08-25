use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::context::ToolOutcome;
use pioneer_provider::{
    AttachmentDataSource, ChatMessage, MessageAttachment, MessageContentPart, ModelInputItem, Role,
};

pub use pioneer_protocol::{
    DeltaOutputPolicy, DiagnosticExcerptPolicy, LlmOutputPolicy, LlmRetentionPolicy,
    RecoveryOutputPolicy, StorageOutputPolicy, TimelineOutputPolicy, ToolDisplayPayload,
    ToolMetadata, ToolMetadataRawKind, ToolMetadataValue, ToolOutputPolicySnapshot,
    ToolOutputSummary, ToolRecoveryView, ToolStoragePayload,
};

pub type ToolOutputPolicy = ToolOutputPolicySnapshot;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ToolOutputProjectionKind {
    Builtin,
    DynamicGeneric,
    DynamicHttp,
    DynamicShell,
    DynamicMcp,
    DynamicFunctionProxy {
        target_tool_name: String,
        target_policy: ToolOutputPolicySnapshot,
        target_projection_kind: Box<ToolOutputProjectionKind>,
    },
}

impl Default for ToolOutputProjectionKind {
    fn default() -> Self {
        Self::Builtin
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ToolResultView {
    Text { text: String, truncated: bool },
    Json { value: JsonValue, truncated: bool },
    Empty,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolResultEnvelope {
    pub llm_view: ToolResultView,
    pub display: ToolDisplayPayload,
    pub storage: ToolStoragePayload,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<ToolRecoveryView>,
    pub outcome: ToolOutcome,
    pub success: bool,
    pub output_policy: ToolOutputPolicySnapshot,
}

impl ToolResultEnvelope {
    pub fn llm_payload(&self) -> JsonValue {
        let mut payload = match &self.llm_view {
            ToolResultView::Text { text, truncated } => serde_json::json!({
                "output": text,
                "truncated": truncated,
            }),
            ToolResultView::Json { value, truncated } => {
                let mut value = value.clone();
                if !value.is_object() {
                    value = serde_json::json!({ "value": value });
                }
                if let Some(map) = value.as_object_mut() {
                    map.entry("truncated".to_owned())
                        .or_insert_with(|| serde_json::json!(truncated));
                }
                value
            }
            ToolResultView::Empty => serde_json::json!({}),
        };

        if let Some(map) = payload.as_object_mut() {
            map.insert(
                "tool_outcome".to_owned(),
                serde_json::to_value(&self.outcome).unwrap_or_else(|_| serde_json::json!({})),
            );
            map.insert(
                "partial_output".to_owned(),
                serde_json::json!({
                    "is_partial": self.outcome.incomplete
                        || matches!(self.outcome.status, crate::context::ToolOutcomeStatus::PartialSuccess),
                    "reason": self.outcome.incomplete_reason.clone(),
                    "continuation_available": self.outcome.should_retry,
                }),
            );
        }

        payload
    }

    pub fn llm_text(&self) -> String {
        match &self.llm_view {
            ToolResultView::Text { text, .. } => text.clone(),
            ToolResultView::Json { value, .. } => {
                serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
            }
            ToolResultView::Empty => String::new(),
        }
    }

    pub fn to_model_input_item(&self, call_id: &str, tool_name: &str) -> ModelInputItem {
        let payload = self.llm_payload();
        if let Some(attachment_part) = attachment_part_from_llm_context(&payload) {
            let content = serde_json::to_string(&payload).unwrap_or_else(|_| payload.to_string());
            return ModelInputItem::message(ChatMessage {
                role: Role::Tool,
                content,
                reasoning_content: None,
                content_parts: vec![attachment_part],
                tool_call_id: Some(call_id.to_owned()),
                name: Some(tool_name.to_owned()),
                tool_calls: None,
                provider_replay_state: None,
            });
        }

        ModelInputItem::tool_result(
            call_id.to_owned(),
            tool_name.to_owned(),
            self.llm_text(),
            Some(payload),
        )
    }
}

impl ToolResultView {
    pub fn bounded_to_bytes(&self, max_bytes: usize) -> Self {
        if self.serialized_size_bytes() <= max_bytes {
            return self.clone();
        }
        if max_bytes == 0 {
            return Self::Empty;
        }

        match self {
            Self::Text { text, .. } => bounded_text_view(text, max_bytes),
            Self::Json { value, .. } => bounded_json_view(value, max_bytes),
            Self::Empty => Self::Empty,
        }
    }

    pub fn serialized_size_bytes(&self) -> usize {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(0)
    }
}

fn bounded_text_view(text: &str, max_bytes: usize) -> ToolResultView {
    let empty = ToolResultView::Text {
        text: String::new(),
        truncated: true,
    };
    if empty.serialized_size_bytes() > max_bytes {
        return ToolResultView::Empty;
    }

    let text = max_prefix_for_view(text, max_bytes, |prefix| ToolResultView::Text {
        text: prefix.to_owned(),
        truncated: true,
    });
    ToolResultView::Text {
        text,
        truncated: true,
    }
}

fn bounded_json_view(value: &JsonValue, max_bytes: usize) -> ToolResultView {
    let serialized = serde_json::to_vec(value).unwrap_or_default();
    let preview_source = String::from_utf8_lossy(&serialized);
    let original_bytes = serialized.len();

    let minimal = ToolResultView::Json {
        value: serde_json::json!({
            "truncated": true,
            "originalBytes": original_bytes,
        }),
        truncated: true,
    };
    if minimal.serialized_size_bytes() > max_bytes {
        let smallest = ToolResultView::Json {
            value: serde_json::json!({ "truncated": true }),
            truncated: true,
        };
        return if smallest.serialized_size_bytes() <= max_bytes {
            smallest
        } else {
            ToolResultView::Empty
        };
    }

    let preview = max_prefix_for_view(preview_source.as_ref(), max_bytes, |prefix| {
        ToolResultView::Json {
            value: serde_json::json!({
                "truncated": true,
                "originalBytes": original_bytes,
                "preview": prefix,
            }),
            truncated: true,
        }
    });

    if preview.is_empty() {
        minimal
    } else {
        ToolResultView::Json {
            value: serde_json::json!({
                "truncated": true,
                "originalBytes": original_bytes,
                "preview": preview,
            }),
            truncated: true,
        }
    }
}

fn max_prefix_for_view<F>(text: &str, max_bytes: usize, make_view: F) -> String
where
    F: Fn(&str) -> ToolResultView,
{
    let boundaries = text
        .char_indices()
        .map(|(index, _)| index)
        .chain(std::iter::once(text.len()))
        .collect::<Vec<_>>();
    let mut low = 0usize;
    let mut high = boundaries.len().saturating_sub(1);
    let mut best = 0usize;

    while low <= high {
        let mid = (low + high) / 2;
        let byte_index = boundaries[mid];
        let candidate = make_view(&text[..byte_index]);
        if candidate.serialized_size_bytes() <= max_bytes {
            best = byte_index;
            low = mid.saturating_add(1);
        } else if mid == 0 {
            break;
        } else {
            high = mid - 1;
        }
    }

    text[..best].to_owned()
}

fn attachment_part_from_llm_context(payload: &JsonValue) -> Option<MessageContentPart> {
    let attachment = payload.get("llm_context")?.get("attachment")?;
    let path = attachment
        .get("path")?
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_owned();

    let mime = attachment
        .get("mime_type")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("application/octet-stream")
        .to_ascii_lowercase();

    let name = attachment
        .get("name")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);

    let size_bytes = attachment.get("size_bytes").and_then(JsonValue::as_u64);

    let sha256 = attachment
        .get("sha256")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);

    let attachment = MessageAttachment {
        mime_type: mime.clone(),
        name,
        size_bytes,
        sha256,
        source: AttachmentDataSource::Path { path },
        artifact: None,
    };

    if mime.starts_with("image/") {
        Some(MessageContentPart::image(attachment))
    } else if mime.starts_with("audio/") {
        Some(MessageContentPart::audio(attachment))
    } else if mime.starts_with("video/") {
        Some(MessageContentPart::video(attachment))
    } else {
        Some(MessageContentPart::file(attachment))
    }
}

pub fn shell_output_policy() -> ToolOutputPolicy {
    ToolOutputPolicySnapshot::for_tool_name("exec_command")
}

pub fn model_only_metadata_policy() -> ToolOutputPolicy {
    ToolOutputPolicySnapshot::for_tool_name("read_file")
}

pub fn web_fetch_output_policy() -> ToolOutputPolicy {
    ToolOutputPolicySnapshot::for_tool_name("web_fetch")
}

pub fn web_search_output_policy() -> ToolOutputPolicy {
    ToolOutputPolicySnapshot::for_tool_name("web_search")
}

pub fn download_output_policy() -> ToolOutputPolicy {
    ToolOutputPolicySnapshot::for_tool_name("download_url")
}

pub fn computer_use_output_policy() -> ToolOutputPolicy {
    ToolOutputPolicySnapshot::for_tool_name("computer_use")
}

pub fn dynamic_unknown_output_policy() -> ToolOutputPolicy {
    ToolOutputPolicySnapshot::for_tool_name("__dynamic_unknown__")
}

pub fn mcp_output_policy() -> ToolOutputPolicy {
    ToolOutputPolicySnapshot {
        llm: LlmOutputPolicy::Structured {
            max_bytes: 2 * 1024 * 1024,
        },
        llm_retention: LlmRetentionPolicy::UntilTurnTerminal {
            max_bytes: 2 * 1024 * 1024,
        },
        timeline: TimelineOutputPolicy::Summary { max_chars: 2_000 },
        storage: StorageOutputPolicy::MetadataOnly,
        recovery: RecoveryOutputPolicy::MetadataOnly,
        deltas: DeltaOutputPolicy::ProgressOnly,
    }
}

pub fn builtin_output_policy(tool_name: &str) -> ToolOutputPolicy {
    ToolOutputPolicySnapshot::for_tool_name(tool_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn web_fetch_never_uses_full_storage_policy() {
        let policy = web_fetch_output_policy();
        assert!(matches!(policy.storage, StorageOutputPolicy::MetadataOnly));
        assert!(matches!(policy.deltas, DeltaOutputPolicy::ProgressOnly));
    }

    #[test]
    fn model_only_tools_do_not_persist_full_output() {
        for tool_name in [
            "read_file",
            "read_skill",
            "list_dir",
            "grep_files",
            "apply_patch",
        ] {
            let policy = builtin_output_policy(tool_name);
            assert!(!matches!(policy.storage, StorageOutputPolicy::Full { .. }));
            assert!(matches!(policy.deltas, DeltaOutputPolicy::Disabled));
        }
    }

    #[test]
    fn unknown_dynamic_tools_default_to_metadata_only_storage_and_timeline() {
        let policy = dynamic_unknown_output_policy();
        assert!(matches!(
            policy.timeline,
            TimelineOutputPolicy::MetadataOnly
        ));
        assert!(matches!(policy.storage, StorageOutputPolicy::MetadataOnly));
        assert!(matches!(
            policy.llm_retention,
            LlmRetentionPolicy::UntilTurnTerminal { .. }
        ));
    }

    #[test]
    fn mcp_tools_use_summary_timeline_without_full_storage() {
        let policy = mcp_output_policy();
        assert!(matches!(
            policy.timeline,
            TimelineOutputPolicy::Summary { .. }
        ));
        assert!(matches!(policy.storage, StorageOutputPolicy::MetadataOnly));
        assert!(matches!(policy.deltas, DeltaOutputPolicy::ProgressOnly));
    }

    #[test]
    fn shell_tools_persist_and_display_output() {
        let policy = builtin_output_policy("exec_command");
        assert!(matches!(policy.storage, StorageOutputPolicy::Full { .. }));
        assert!(matches!(
            policy.deltas,
            DeltaOutputPolicy::PersistAndDisplay { .. }
        ));
    }

    #[test]
    fn bounded_tool_result_view_enforces_serialized_budget_for_text() {
        let max_bytes = 128;
        let view = ToolResultView::Text {
            text: format!("prefix-{}", "x".repeat(1_000)),
            truncated: false,
        };

        let bounded = view.bounded_to_bytes(max_bytes);

        assert!(bounded.serialized_size_bytes() <= max_bytes);
        assert!(matches!(
            bounded,
            ToolResultView::Text {
                truncated: true,
                ..
            }
        ));
    }

    #[test]
    fn bounded_tool_result_view_enforces_serialized_budget_for_json() {
        let max_bytes = 256;
        let view = ToolResultView::Json {
            value: serde_json::json!({
                "output": format!("secret-{}", "x".repeat(2_000)),
                "path": "/tmp/large.txt"
            }),
            truncated: false,
        };

        let bounded = view.bounded_to_bytes(max_bytes);

        assert!(bounded.serialized_size_bytes() <= max_bytes);
        assert!(
            serde_json::to_string(&bounded)
                .expect("bounded view should serialize")
                .contains("originalBytes")
        );
    }
}
