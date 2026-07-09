use crate::thread::Thread;
use crate::turn::CLIAgentRuntimeKind;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct RuntimeSummary {
    pub runtime_id: String,
    pub kind: CLIAgentRuntimeKind,
    pub display_name: String,
    pub enabled: bool,
    pub status: RuntimeStatus,
    pub capabilities: RuntimeCapabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<RuntimeAccountSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_home_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub debug_native_events_enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub models_refreshed_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<RuntimeDiagnostic>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_stderr: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RuntimeStatus {
    Disabled,
    MissingBinary {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        binary_path: Option<String>,
    },
    SpawnFailed {
        message: String,
    },
    Initializing,
    NeedsAuth,
    Ready,
    Degraded {
        message: String,
    },
    UnsupportedVersion {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        minimum_version: Option<String>,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct RuntimeCapabilities {
    pub supports_threads: bool,
    pub supports_resume: bool,
    pub supports_fork: bool,
    pub supports_steer: bool,
    pub supports_interrupt: bool,
    pub supports_approvals: bool,
    pub supports_file_change_approvals: bool,
    pub supports_command_approvals: bool,
    pub supports_user_input_requests: bool,
    pub supports_model_list: bool,
    pub supports_apps: bool,
    pub supports_review: bool,
    pub supports_compaction: bool,
    pub supports_goal: bool,
    pub supports_diff_updates: bool,
    pub supports_history_read: bool,
    pub supports_thread_archive: bool,
    pub supports_auth_management: bool,
    pub supports_generated_schema_probe: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct RuntimeAccountSnapshot {
    pub authenticated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_method: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct RuntimeModelInfo {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    #[serde(default)]
    pub is_custom: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub effort_options: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_modalities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub output_modalities: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_reasoning: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_vision: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct RuntimeAppInfo {
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct RuntimeDiagnostic {
    pub level: RuntimeDiagnosticLevel,
    pub code: String,
    pub message: String,
}

pub const RUNTIME_DIAGNOSTIC_MAX_LINES: usize = 40;
pub const RUNTIME_DIAGNOSTIC_LINE_MAX_CHARS: usize = 500;

fn is_false(value: &bool) -> bool {
    !*value
}

pub fn sanitize_runtime_diagnostic_lines(lines: Vec<String>) -> Vec<String> {
    let mut lines = lines
        .into_iter()
        .rev()
        .take(RUNTIME_DIAGNOSTIC_MAX_LINES)
        .collect::<Vec<_>>();
    lines.reverse();
    lines
        .into_iter()
        .map(|line| sanitize_runtime_diagnostic_line(line.as_str()))
        .collect()
}

pub fn sanitize_runtime_diagnostic_line(line: &str) -> String {
    let without_control_chars = line
        .chars()
        .filter(|character| *character == '\t' || !character.is_control())
        .collect::<String>();
    let redacted = redact_runtime_diagnostic_secrets(without_control_chars.as_str());
    truncate_runtime_diagnostic_line(redacted.as_str())
}

fn redact_runtime_diagnostic_secrets(line: &str) -> String {
    const SECRET_KEYS: &[&str] = &[
        "authorization",
        "proxy-authorization",
        "api_key",
        "apikey",
        "api-token",
        "apitoken",
        "access_token",
        "refresh_token",
        "id_token",
        "token",
        "secret",
        "password",
        "session_key",
        "headers",
        "raw",
        "blob",
        "payload",
    ];

    let mut redacted = line.to_owned();
    for key in SECRET_KEYS {
        redacted = redact_runtime_diagnostic_key_values(
            redacted.as_str(),
            key,
            matches!(*key, "authorization" | "proxy-authorization"),
        );
    }
    redacted = redact_runtime_diagnostic_bearer_tokens(redacted.as_str());
    redact_runtime_diagnostic_prefixed_tokens(redacted.as_str())
}

fn redact_runtime_diagnostic_key_values(line: &str, key: &str, redact_to_eol: bool) -> String {
    let mut output = line.to_owned();
    let mut cursor = 0;
    loop {
        let lower = output.to_ascii_lowercase();
        let Some(relative_index) = lower[cursor..].find(key) else {
            break;
        };
        let key_index = cursor + relative_index;
        let after_key = key_index + key.len();
        let Some((start, end)) =
            runtime_diagnostic_value_span(output.as_str(), after_key, redact_to_eol)
        else {
            cursor = after_key;
            if cursor >= output.len() {
                break;
            }
            continue;
        };
        output.replace_range(start..end, "[REDACTED]");
        cursor = start + "[REDACTED]".len();
        if cursor >= output.len() {
            break;
        }
    }
    output
}

fn runtime_diagnostic_value_span(
    line: &str,
    after_key: usize,
    redact_to_eol: bool,
) -> Option<(usize, usize)> {
    let bytes = line.as_bytes();
    let mut cursor = after_key;
    while cursor < bytes.len()
        && (bytes[cursor].is_ascii_whitespace() || bytes[cursor] == b'"' || bytes[cursor] == b'\'')
    {
        cursor += 1;
    }
    if cursor >= bytes.len() || (bytes[cursor] != b':' && bytes[cursor] != b'=') {
        return None;
    }
    cursor += 1;
    while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
        cursor += 1;
    }
    let quote = if cursor < bytes.len() && (bytes[cursor] == b'"' || bytes[cursor] == b'\'') {
        let quote = bytes[cursor];
        cursor += 1;
        Some(quote)
    } else {
        None
    };
    let start = cursor;
    if start >= bytes.len() {
        return None;
    }
    let mut end = start;
    if redact_to_eol && quote.is_none() {
        while end < bytes.len()
            && !matches!(bytes[end], b'}' | b']' | b',' | b';' | b'&' | b'\r' | b'\n')
        {
            end += 1;
        }
        if end == start {
            end = bytes.len();
        }
    } else if let Some(quote) = quote {
        while end < bytes.len() && bytes[end] != quote {
            end += 1;
        }
    } else {
        while end < bytes.len()
            && !matches!(
                bytes[end],
                b' ' | b'\t' | b'\r' | b'\n' | b',' | b';' | b'&'
            )
        {
            end += 1;
        }
    }
    (end > start).then_some((start, end))
}

fn redact_runtime_diagnostic_bearer_tokens(line: &str) -> String {
    let mut output = line.to_owned();
    let mut cursor = 0;
    loop {
        let lower = output.to_ascii_lowercase();
        let Some(relative_index) = lower[cursor..].find("bearer") else {
            break;
        };
        let bearer_index = cursor + relative_index;
        let mut start = bearer_index + "bearer".len();
        let bytes = output.as_bytes();
        if start >= bytes.len() || !bytes[start].is_ascii_whitespace() {
            cursor = start;
            continue;
        }
        while start < bytes.len() && bytes[start].is_ascii_whitespace() {
            start += 1;
        }
        let mut end = start;
        while end < bytes.len()
            && !matches!(
                bytes[end],
                b' ' | b'\t' | b'\r' | b'\n' | b',' | b';' | b'&' | b'"' | b'\''
            )
        {
            end += 1;
        }
        if end > start {
            output.replace_range(start..end, "[REDACTED]");
            cursor = start + "[REDACTED]".len();
        } else {
            cursor = start;
        }
        if cursor >= output.len() {
            break;
        }
    }
    output
}

fn redact_runtime_diagnostic_prefixed_tokens(line: &str) -> String {
    let mut output = line.to_owned();
    for prefix in ["sk-proj-", "sk-", "sess-"] {
        let mut cursor = 0;
        loop {
            let lower = output.to_ascii_lowercase();
            let Some(relative_index) = lower[cursor..].find(prefix) else {
                break;
            };
            let start = cursor + relative_index;
            let mut end = start + prefix.len();
            let bytes = output.as_bytes();
            while end < bytes.len()
                && !matches!(
                    bytes[end],
                    b' ' | b'\t' | b'\r' | b'\n' | b',' | b';' | b'&' | b'"' | b'\''
                )
            {
                end += 1;
            }
            output.replace_range(start..end, "[REDACTED]");
            cursor = start + "[REDACTED]".len();
            if cursor >= output.len() {
                break;
            }
        }
    }
    output
}

fn truncate_runtime_diagnostic_line(line: &str) -> String {
    let mut truncated = String::new();
    let mut chars = line.chars();
    for _ in 0..RUNTIME_DIAGNOSTIC_LINE_MAX_CHARS {
        let Some(character) = chars.next() else {
            return line.to_owned();
        };
        truncated.push(character);
    }
    let remaining = chars.count();
    if remaining == 0 {
        line.to_owned()
    } else {
        truncated.push_str(format!(" [truncated {remaining} chars]").as_str());
        truncated
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeDiagnosticLevel {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeListParams {
    pub workspace_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimeListResponse {
    pub runtimes: Vec<RuntimeSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeGetParams {
    pub workspace_id: String,
    pub runtime_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimeGetResponse {
    pub runtime: RuntimeSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeStatusParams {
    pub workspace_id: String,
    pub runtime_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimeStatusResponse {
    pub runtime: RuntimeSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeRefreshParams {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimeRefreshResponse {
    pub runtimes: Vec<RuntimeSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeListModelsParams {
    pub workspace_id: String,
    pub runtime_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeListModelsResponse {
    pub runtime_id: String,
    pub models: Vec<RuntimeModelInfo>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<RuntimeDiagnostic>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refreshed_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeThreadBindingGetParams {
    pub workspace_id: String,
    pub thread_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeThreadBindingGetResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<CLIRuntimeThreadBinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeThreadBinding {
    pub workspace_id: String,
    pub thread_id: String,
    pub runtime_id: String,
    pub runtime_kind: CLIAgentRuntimeKind,
    pub native_thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_model: Option<String>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeThreadForkParams {
    pub workspace_id: String,
    pub runtime_id: String,
    pub source_thread_id: String,
    pub fork_thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimeThreadForkResponse {
    pub workspace_id: String,
    pub runtime_id: String,
    pub source_thread_id: String,
    pub thread: Thread,
    pub native_thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeThreadCompactParams {
    pub workspace_id: String,
    pub runtime_id: String,
    pub thread_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimeThreadCompactResponse {
    pub workspace_id: String,
    pub runtime_id: String,
    pub thread_id: String,
    pub native_thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeTurnSteerParams {
    pub workspace_id: String,
    pub runtime_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimeTurnSteerResponse {
    pub workspace_id: String,
    pub runtime_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub native_thread_id: String,
    pub native_turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<JsonValue>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CLIRuntimeReviewDelivery {
    Inline,
    Detached,
}

impl Default for CLIRuntimeReviewDelivery {
    fn default() -> Self {
        Self::Inline
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum CLIRuntimeReviewTarget {
    UncommittedChanges,
    BaseBranch {
        branch: String,
    },
    Commit {
        sha: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
    },
    Custom {
        instructions: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeReviewStartParams {
    pub workspace_id: String,
    pub runtime_id: String,
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default)]
    pub delivery: CLIRuntimeReviewDelivery,
    pub target: CLIRuntimeReviewTarget,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimeReviewStartResponse {
    pub workspace_id: String,
    pub runtime_id: String,
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    pub delivery: CLIRuntimeReviewDelivery,
    pub target: CLIRuntimeReviewTarget,
    pub native_thread_id: String,
    pub review_thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<JsonValue>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub enum CLIRuntimeLoginStartType {
    #[serde(rename = "chatgptDeviceCode")]
    ChatgptDeviceCode,
    #[serde(rename = "chatgpt")]
    Chatgpt,
}

impl Default for CLIRuntimeLoginStartType {
    fn default() -> Self {
        Self::ChatgptDeviceCode
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeLoginStartParams {
    pub workspace_id: String,
    pub runtime_id: String,
    #[serde(default)]
    pub login_type: CLIRuntimeLoginStartType,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimeLoginStartResponse {
    pub runtime_id: String,
    pub login_type: CLIRuntimeLoginStartType,
    pub status: RuntimeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub login_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeLoginCancelParams {
    pub workspace_id: String,
    pub runtime_id: String,
    pub login_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeLoginCancelResponse {
    pub runtime_id: String,
    pub login_id: String,
    pub cancelled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeProxySetParams {
    pub workspace_id: String,
    pub runtime_id: String,
    pub proxy_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeProxySetResponse {
    pub runtime_id: String,
    pub proxy_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeProxyDeleteParams {
    pub workspace_id: String,
    pub runtime_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CLIRuntimeProxyDeleteResponse {
    pub runtime_id: String,
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimeRequestRespondParams {
    pub workspace_id: String,
    pub runtime_id: String,
    pub request_id: String,
    pub resolution: CLIRuntimeRequestResolution,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimeRequestRespondResponse {
    pub workspace_id: String,
    pub runtime_id: String,
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    pub status: CLIRuntimePendingRequestStatus,
    pub resolution: CLIRuntimeRequestResolution,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimeStatusChangedNotification {
    pub workspace_id: String,
    pub runtime: RuntimeSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimeAccountUpdatedNotification {
    pub workspace_id: String,
    pub runtime_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<CLIAgentRuntimeKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<RuntimeAccountSnapshot>,
    pub status: RuntimeStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimeRequestOpenedNotification {
    pub workspace_id: String,
    pub runtime_id: String,
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    pub request: CLIRuntimePendingRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimeRequestResolvedNotification {
    pub workspace_id: String,
    pub runtime_id: String,
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    pub resolution: CLIRuntimeRequestResolution,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimePendingRequest {
    pub kind: CLIRuntimeRequestKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<JsonValue>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CLIRuntimeRequestKind {
    CommandApproval,
    FileChangeApproval,
    UserInput,
    Other,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CLIRuntimePendingRequestStatus {
    Pending,
    Answered,
    Resolved,
    Cancelled,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CLIRuntimeRequestResolution {
    Approved,
    Denied {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    Cancelled,
    Answered {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        response: Option<JsonValue>,
    },
    Expired,
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CLIRuntimeAppsChangedNotification {
    pub workspace_id: String,
    pub runtime_id: String,
    pub apps: Vec<RuntimeAppInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refreshed_at_unix_ms: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn runtime_status_snapshot_serializes_codex_ready_catalog() {
        let summary = RuntimeSummary {
            runtime_id: "codex_personal".to_owned(),
            kind: CLIAgentRuntimeKind::Codex,
            display_name: "Codex Personal".to_owned(),
            enabled: true,
            status: RuntimeStatus::Ready,
            capabilities: RuntimeCapabilities {
                supports_threads: true,
                supports_resume: true,
                supports_fork: true,
                supports_steer: true,
                supports_interrupt: true,
                supports_approvals: true,
                supports_file_change_approvals: true,
                supports_command_approvals: true,
                supports_user_input_requests: true,
                supports_model_list: true,
                supports_apps: false,
                supports_review: true,
                supports_compaction: true,
                supports_goal: true,
                supports_diff_updates: true,
                supports_history_read: true,
                supports_thread_archive: true,
                supports_auth_management: true,
                supports_generated_schema_probe: true,
            },
            account: Some(RuntimeAccountSnapshot {
                authenticated: true,
                account_id: Some("acct_123".to_owned()),
                email: Some("user@example.com".to_owned()),
                display_name: Some("User Example".to_owned()),
                plan: Some("ChatGPT Pro".to_owned()),
                auth_method: Some("chatgpt".to_owned()),
            }),
            version: Some("1.2.3".to_owned()),
            binary_path: Some("codex".to_owned()),
            home_path: Some("~/.codex".to_owned()),
            shadow_home_path: Some("~/.pioneer/codex/personal".to_owned()),
            proxy_url: Some("socks5://127.0.0.1:1080".to_owned()),
            debug_native_events_enabled: true,
            models_refreshed_at_unix_ms: Some(1_700_000_000_000),
            diagnostics: vec![RuntimeDiagnostic {
                level: RuntimeDiagnosticLevel::Info,
                code: "schema_probe_ok".to_owned(),
                message: "generated schema probe succeeded".to_owned(),
            }],
            recent_stderr: vec!["app-server ready".to_owned()],
        };

        let encoded = serde_json::to_value(&summary).expect("summary should serialize");

        assert_eq!(encoded["runtime_id"], "codex_personal");
        assert_eq!(encoded["kind"], "codex");
        assert_eq!(encoded["status"], json!({ "state": "ready" }));
        assert_eq!(encoded["account"]["email"], "user@example.com");
        assert_eq!(encoded["debug_native_events_enabled"], true);
        assert_eq!(encoded["recent_stderr"][0], "app-server ready");

        let decoded: RuntimeSummary =
            serde_json::from_value(encoded).expect("summary should deserialize");
        assert_eq!(decoded, summary);
    }

    #[test]
    fn runtime_diagnostic_sanitizer_redacts_headers_tokens_raw_blobs_and_large_lines() {
        let raw = "headers={Authorization: Bearer sk-proj-secret} OPENAI_API_KEY=sk-secret raw={secret} apiToken=\"secret\"";

        let sanitized = sanitize_runtime_diagnostic_line(raw);
        let large =
            sanitize_runtime_diagnostic_line(format!("stderr {}", "x".repeat(900)).as_str());

        assert!(sanitized.contains("[REDACTED]"));
        assert!(!sanitized.contains("sk-secret"));
        assert!(!sanitized.contains("sk-proj-secret"));
        assert!(!sanitized.contains("apiToken=\"secret\""));
        assert!(!sanitized.contains("raw={secret}"));
        assert!(large.contains("[truncated "));
    }

    #[test]
    fn schema_documents_include_phase35_cli_runtime_contracts() {
        let schema_names = crate::protocol_schema_documents()
            .into_iter()
            .map(|document| document.file_name)
            .collect::<Vec<_>>();

        for expected in [
            "agent_execution_backend.json",
            "cli_agent_runtime_kind.json",
            "cli_agent_runtime_sandbox_policy.json",
            "turn_cli_runtime_options.json",
            "runtime_summary.json",
            "runtime_status.json",
            "runtime_model_info.json",
            "runtime_app_info.json",
            "runtime_capabilities.json",
            "runtime_account_snapshot.json",
            "runtime_diagnostic.json",
            "runtime_diagnostic_level.json",
            "cli_runtime_list_params.json",
            "cli_runtime_list_response.json",
            "cli_runtime_get_params.json",
            "cli_runtime_get_response.json",
            "cli_runtime_status_params.json",
            "cli_runtime_status_response.json",
            "cli_runtime_refresh_params.json",
            "cli_runtime_refresh_response.json",
            "cli_runtime_list_models_params.json",
            "cli_runtime_list_models_response.json",
            "cli_runtime_thread_binding_get_params.json",
            "cli_runtime_thread_binding_get_response.json",
            "cli_runtime_thread_binding.json",
            "cli_runtime_thread_fork_params.json",
            "cli_runtime_thread_fork_response.json",
            "cli_runtime_thread_compact_params.json",
            "cli_runtime_thread_compact_response.json",
            "cli_runtime_turn_steer_params.json",
            "cli_runtime_turn_steer_response.json",
            "cli_runtime_review_delivery.json",
            "cli_runtime_review_target.json",
            "cli_runtime_review_start_params.json",
            "cli_runtime_review_start_response.json",
            "cli_runtime_login_start_type.json",
            "cli_runtime_login_start_params.json",
            "cli_runtime_login_start_response.json",
            "cli_runtime_login_cancel_params.json",
            "cli_runtime_login_cancel_response.json",
            "cli_runtime_pending_request.json",
            "cli_runtime_pending_request_status.json",
            "cli_runtime_request_respond_params.json",
            "cli_runtime_request_respond_response.json",
            "cli_runtime_request_kind.json",
            "cli_runtime_request_resolution.json",
            "cli_runtime_status_changed_notification.json",
            "cli_runtime_account_updated_notification.json",
            "cli_runtime_request_opened_notification.json",
            "cli_runtime_request_resolved_notification.json",
            "cli_runtime_apps_changed_notification.json",
        ] {
            assert!(
                schema_names.iter().any(|name| *name == expected),
                "missing schema document {expected}"
            );
        }
    }

    #[test]
    fn runtime_pending_request_respond_contract_round_trips() {
        let params = CLIRuntimeRequestRespondParams {
            workspace_id: "ws_1".to_owned(),
            runtime_id: "codex_personal".to_owned(),
            request_id: "req_1".to_owned(),
            resolution: CLIRuntimeRequestResolution::Answered {
                response: Some(json!({
                    "answer": "Use the existing implementation"
                })),
            },
        };
        let encoded = serde_json::to_value(&params).expect("params should serialize");
        assert_eq!(encoded["resolution"]["status"], "answered");

        let decoded: CLIRuntimeRequestRespondParams =
            serde_json::from_value(encoded).expect("params should deserialize");
        assert_eq!(decoded, params);

        let response = CLIRuntimeRequestRespondResponse {
            workspace_id: "ws_1".to_owned(),
            runtime_id: "codex_personal".to_owned(),
            request_id: "req_1".to_owned(),
            thread_id: Some("thread_1".to_owned()),
            turn_id: Some("turn_1".to_owned()),
            item_id: Some("item_1".to_owned()),
            status: CLIRuntimePendingRequestStatus::Answered,
            resolution: CLIRuntimeRequestResolution::Approved,
        };
        let encoded = serde_json::to_value(&response).expect("response should serialize");
        assert_eq!(encoded["status"], "answered");
        assert_eq!(encoded["resolution"], json!({ "status": "approved" }));

        let decoded: CLIRuntimeRequestRespondResponse =
            serde_json::from_value(encoded).expect("response should deserialize");
        assert_eq!(decoded, response);
    }
}
