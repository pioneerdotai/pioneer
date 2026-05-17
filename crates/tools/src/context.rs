use crate::output_policy::ToolResultEnvelope;
use crate::spec::ToolRecoveryMetadata;
use pioneer_provider::ModelInputItem;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecCommandArgs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub yield_time_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tty: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteStdinArgs {
    pub session_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chars: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub yield_time_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum LocalShellPayload {
    ExecCommand(ExecCommandArgs),
    WriteStdin(WriteStdinArgs),
}

#[derive(Debug, Clone)]
pub enum ToolPayload {
    Function {
        arguments: JsonValue,
    },
    Mcp {
        server: String,
        tool: String,
        arguments: JsonValue,
    },
    LocalShell(LocalShellPayload),
    ToolSearch {
        query: String,
        limit: Option<usize>,
        include_hidden: Option<bool>,
    },
    Custom {
        input: String,
    },
}

impl ToolPayload {
    pub fn log_payload(&self) -> String {
        match self {
            Self::Function { arguments } => arguments.to_string(),
            Self::Mcp {
                server,
                tool,
                arguments,
            } => {
                format!("{server}:{tool} {arguments}")
            }
            Self::LocalShell(LocalShellPayload::ExecCommand(args)) => args
                .command
                .as_ref()
                .map(|command| command.join(" "))
                .unwrap_or_default(),
            Self::LocalShell(LocalShellPayload::WriteStdin(args)) => {
                format!(
                    "session={} chars={}",
                    args.session_id,
                    args.chars.clone().unwrap_or_default()
                )
            }
            Self::ToolSearch { query, .. } => query.clone(),
            Self::Custom { input } => input.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ToolInvocation {
    pub call_id: String,
    pub tool_name: String,
    pub source: ToolCallSource,
    pub payload: ToolPayload,
    pub workdir: PathBuf,
    pub environment: BTreeMap<String, String>,
    pub attempt_id: u32,
    pub idempotency_key: Option<String>,
    pub recovery: ToolRecoveryMetadata,
    pub cancellation: CancellationToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallSource {
    Model,
    NestedTool,
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolOutcomeStatus {
    Ok,
    RecoverableError,
    FatalError,
    PartialSuccess,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolErrorClass {
    InvalidArguments,
    NotFound,
    PermissionDenied,
    CommandNotFound,
    Timeout,
    Cancelled,
    ExecutionFailed,
    NeedsNarrowing,
    Internal,
    OutputTruncated,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolOutcome {
    pub status: ToolOutcomeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_class: Option<ToolErrorClass>,
    pub should_retry: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_hint: Option<String>,
    pub incomplete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete_reason: Option<String>,
}

impl ToolOutcome {
    pub fn ok() -> Self {
        Self {
            status: ToolOutcomeStatus::Ok,
            error_class: None,
            should_retry: false,
            retry_hint: None,
            incomplete: false,
            incomplete_reason: None,
        }
    }

    pub fn partial(reason: impl Into<String>, retry_hint: impl Into<String>) -> Self {
        Self {
            status: ToolOutcomeStatus::PartialSuccess,
            error_class: Some(ToolErrorClass::OutputTruncated),
            should_retry: true,
            retry_hint: Some(retry_hint.into()),
            incomplete: true,
            incomplete_reason: Some(reason.into()),
        }
    }

    pub fn recoverable(
        class: ToolErrorClass,
        retry_hint: impl Into<String>,
        incomplete: bool,
        incomplete_reason: Option<String>,
    ) -> Self {
        Self {
            status: ToolOutcomeStatus::RecoverableError,
            error_class: Some(class),
            should_retry: true,
            retry_hint: Some(retry_hint.into()),
            incomplete,
            incomplete_reason,
        }
    }

    pub fn fatal(class: ToolErrorClass, retry_hint: Option<String>) -> Self {
        Self {
            status: ToolOutcomeStatus::FatalError,
            error_class: Some(class),
            should_retry: false,
            retry_hint,
            incomplete: false,
            incomplete_reason: None,
        }
    }
}

impl Default for ToolOutcome {
    fn default() -> Self {
        Self::ok()
    }
}

pub trait ToolOutput: Send + Sync {
    fn success(&self) -> bool;
    fn raw_text(&self) -> String;
    fn raw_json(&self) -> JsonValue;
    fn to_model_input_item(&self, call_id: &str, tool_name: &str) -> ModelInputItem;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionToolOutput {
    pub text: String,
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<JsonValue>,
}

impl FunctionToolOutput {
    pub fn new(text: impl Into<String>, success: bool) -> Self {
        Self {
            text: text.into(),
            success,
            payload: None,
        }
    }

    pub fn with_payload(text: impl Into<String>, success: bool, payload: JsonValue) -> Self {
        Self {
            text: text.into(),
            success,
            payload: Some(payload),
        }
    }
}

impl ToolOutput for FunctionToolOutput {
    fn success(&self) -> bool {
        self.success
    }

    fn raw_text(&self) -> String {
        self.text.clone()
    }

    fn raw_json(&self) -> JsonValue {
        self.payload.clone().unwrap_or_else(|| {
            serde_json::json!({
                "success": self.success,
                "output": self.text,
            })
        })
    }

    fn to_model_input_item(&self, call_id: &str, tool_name: &str) -> ModelInputItem {
        ModelInputItem::tool_result(
            call_id.to_owned(),
            tool_name.to_owned(),
            self.text.clone(),
            Some(self.raw_json()),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallToolResult {
    pub content: String,
    pub is_error: bool,
}

impl ToolOutput for CallToolResult {
    fn success(&self) -> bool {
        !self.is_error
    }

    fn raw_text(&self) -> String {
        self.content.clone()
    }

    fn raw_json(&self) -> JsonValue {
        serde_json::json!({
            "content": self.content,
            "is_error": self.is_error,
        })
    }

    fn to_model_input_item(&self, call_id: &str, tool_name: &str) -> ModelInputItem {
        ModelInputItem::tool_result(
            call_id.to_owned(),
            tool_name.to_owned(),
            self.content.clone(),
            Some(self.raw_json()),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSearchResultTool {
    pub name: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSearchOutput {
    pub tools: Vec<ToolSearchResultTool>,
}

impl ToolOutput for ToolSearchOutput {
    fn success(&self) -> bool {
        true
    }

    fn raw_text(&self) -> String {
        serde_json::to_string_pretty(&self.tools).unwrap_or_else(|_| "[]".to_owned())
    }

    fn raw_json(&self) -> JsonValue {
        serde_json::to_value(self).unwrap_or_else(|_| JsonValue::Array(Vec::new()))
    }

    fn to_model_input_item(&self, call_id: &str, tool_name: &str) -> ModelInputItem {
        ModelInputItem::tool_result(
            call_id.to_owned(),
            tool_name.to_owned(),
            self.raw_text(),
            Some(self.raw_json()),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSuggestOutput {
    pub tools: Vec<ToolSearchResultTool>,
}

impl ToolOutput for ToolSuggestOutput {
    fn success(&self) -> bool {
        true
    }

    fn raw_text(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{\"tools\":[]}".to_owned())
    }

    fn raw_json(&self) -> JsonValue {
        serde_json::to_value(self).unwrap_or_else(|_| serde_json::json!({ "tools": [] }))
    }

    fn to_model_input_item(&self, call_id: &str, tool_name: &str) -> ModelInputItem {
        ModelInputItem::tool_result(
            call_id.to_owned(),
            tool_name.to_owned(),
            self.raw_text(),
            Some(self.raw_json()),
        )
    }
}

pub struct AnyToolResult {
    pub call_id: String,
    pub tool_name: String,
    pub payload: ToolPayload,
    pub output: Box<dyn ToolOutput>,
    pub outcome: ToolOutcome,
    pub(crate) projection: Option<ToolResultEnvelope>,
}

impl AnyToolResult {
    pub fn success(&self) -> bool {
        self.output.success()
    }

    pub fn raw_output_text(&self) -> String {
        self.output.raw_text()
    }

    pub fn raw_output_json(&self) -> JsonValue {
        self.output.raw_json()
    }

    pub fn projection(&self) -> Option<&ToolResultEnvelope> {
        self.projection.as_ref()
    }

    pub fn set_projection(&mut self, projection: ToolResultEnvelope) {
        self.projection = Some(projection);
    }

    pub fn model_visible_text(&self) -> String {
        self.projection
            .as_ref()
            .map(ToolResultEnvelope::llm_text)
            .unwrap_or_default()
    }

    pub fn set_outcome(&mut self, outcome: ToolOutcome) {
        self.outcome = outcome;
    }

    pub fn to_model_input_item(&self) -> ModelInputItem {
        if let Some(projection) = self.projection.as_ref() {
            return projection.to_model_input_item(self.call_id.as_str(), self.tool_name.as_str());
        }

        let payload = serde_json::json!({
            "error": "tool result projection missing",
            "tool_outcome": self.outcome,
            "partial_output": {
                "is_partial": true,
                "reason": "tool result projection missing",
                "continuation_available": false,
                "truncated": true,
            },
        });
        ModelInputItem::tool_result(
            self.call_id.clone(),
            self.tool_name.clone(),
            "tool result projection missing".to_owned(),
            Some(payload),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output_policy::{
        ToolDisplayPayload, ToolOutputPolicySnapshot, ToolResultView, ToolStoragePayload,
    };
    use pioneer_provider::{AttachmentDataSource, MessageContentPart, Role};

    fn test_projection(
        tool_name: &str,
        value: JsonValue,
        outcome: ToolOutcome,
    ) -> ToolResultEnvelope {
        ToolResultEnvelope {
            llm_view: ToolResultView::Json {
                value,
                truncated: false,
            },
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::None,
            recovery: None,
            success: matches!(
                outcome.status,
                ToolOutcomeStatus::Ok | ToolOutcomeStatus::PartialSuccess
            ),
            outcome,
            output_policy: ToolOutputPolicySnapshot::for_tool_name(tool_name),
        }
    }

    #[test]
    fn model_payload_always_contains_partial_output_contract() {
        let outcome = ToolOutcome::ok();
        let result = AnyToolResult {
            call_id: "call_1".to_owned(),
            tool_name: "read_file".to_owned(),
            payload: ToolPayload::Function {
                arguments: serde_json::json!({}),
            },
            output: Box::new(FunctionToolOutput::with_payload(
                "ok",
                true,
                serde_json::json!({ "output": "ok", "truncated": false }),
            )),
            outcome: outcome.clone(),
            projection: Some(test_projection(
                "read_file",
                serde_json::json!({ "output": "ok", "truncated": false }),
                outcome,
            )),
        };

        let item = result.to_model_input_item();
        let payload = match item {
            ModelInputItem::ToolResult { payload, .. } => payload.expect("payload is required"),
            ModelInputItem::Message { .. } => panic!("expected tool result"),
        };

        assert!(payload.get("tool_outcome").is_some());
        assert!(payload.get("partial_output").is_some());
    }

    #[test]
    fn llm_context_attachment_becomes_structured_tool_message() {
        let outcome = ToolOutcome::ok();
        let model_payload = serde_json::json!({
            "action": "snapshot",
            "llm_context": {
                "attachment": {
                    "path": "/tmp/session/snapshots/1.png",
                    "mime_type": "image/png"
                }
            }
        });
        let result = AnyToolResult {
            call_id: "call_42".to_owned(),
            tool_name: "computer_use".to_owned(),
            payload: ToolPayload::Function {
                arguments: serde_json::json!({}),
            },
            output: Box::new(FunctionToolOutput::with_payload(
                "snapshot ready",
                true,
                model_payload.clone(),
            )),
            outcome: outcome.clone(),
            projection: Some(test_projection("computer_use", model_payload, outcome)),
        };

        let item = result.to_model_input_item();
        let message = match item {
            ModelInputItem::Message { message } => message,
            ModelInputItem::ToolResult { .. } => {
                panic!("expected message with structured attachment")
            }
        };

        assert_eq!(message.role, Role::Tool);
        assert_eq!(message.tool_call_id.as_deref(), Some("call_42"));
        assert_eq!(message.name.as_deref(), Some("computer_use"));
        assert!(message.content.contains("\"llm_context\""));
        assert!(message.content.contains("\"partial_output\""));
        assert_eq!(message.content_parts.len(), 1);

        match &message.content_parts[0] {
            MessageContentPart::Image { image } => {
                assert_eq!(image.mime_type, "image/png");
                match &image.source {
                    AttachmentDataSource::Path { path } => {
                        assert_eq!(path, "/tmp/session/snapshots/1.png");
                    }
                    _ => panic!("expected attachment source path"),
                }
            }
            _ => panic!("expected image attachment"),
        }
    }
}
