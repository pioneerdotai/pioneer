use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::domain::{REQUEST_TOOLS_DOMAIN_VALUES, REQUEST_TOOLS_REASON_MAX_CHARS};
use crate::output_policy::{ToolOutputPolicy, ToolOutputProjectionKind, builtin_output_policy};

pub const REQUEST_TOOLS_TOOL_NAME: &str = "request_tools";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionClass {
    Shared,
    Exclusive,
    SessionScoped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadKind {
    Function,
    Mcp,
    LocalShell,
    ToolSearch,
    Custom,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ToolPayloadBinding {
    Function,
    Mcp {
        server_id: String,
        server_name: String,
        raw_tool_name: String,
        catalog_version: String,
        snapshot_version: u64,
    },
}

impl Default for ToolPayloadBinding {
    fn default() -> Self {
        Self::Function
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRetryClass {
    Never,
    Transient,
    Arguments,
    Session,
    Network,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolIdempotencyMode {
    None,
    Safe,
    RequiresKey,
    SessionBound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolRecoveryMetadata {
    pub retry_class: ToolRetryClass,
    pub idempotency_mode: ToolIdempotencyMode,
    pub max_attempts: u8,
    pub can_resume: bool,
}

impl Default for ToolRecoveryMetadata {
    fn default() -> Self {
        Self {
            retry_class: ToolRetryClass::Transient,
            idempotency_mode: ToolIdempotencyMode::None,
            max_attempts: 2,
            can_resume: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub parameters: JsonValue,
    pub payload_kind: PayloadKind,
    pub recovery: ToolRecoveryMetadata,
}

impl ToolSpec {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        parameters: JsonValue,
        payload_kind: PayloadKind,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            parameters,
            payload_kind,
            recovery: ToolRecoveryMetadata::default(),
        }
    }

    pub fn with_recovery(mut self, recovery: ToolRecoveryMetadata) -> Self {
        self.recovery = recovery;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfiguredToolSpec {
    pub spec: ToolSpec,
    pub execution_class: ExecutionClass,
    pub output_policy: ToolOutputPolicy,
    pub output_projection: ToolOutputProjectionKind,
    #[serde(default)]
    pub payload_binding: ToolPayloadBinding,
}

impl ConfiguredToolSpec {
    pub fn new(
        spec: ToolSpec,
        execution_class: ExecutionClass,
        output_policy: ToolOutputPolicy,
    ) -> Self {
        Self::with_output_projection(
            spec,
            execution_class,
            output_policy,
            ToolOutputProjectionKind::Builtin,
        )
    }

    pub fn with_output_projection(
        spec: ToolSpec,
        execution_class: ExecutionClass,
        output_policy: ToolOutputPolicy,
        output_projection: ToolOutputProjectionKind,
    ) -> Self {
        Self {
            spec,
            execution_class,
            output_policy,
            output_projection,
            payload_binding: ToolPayloadBinding::default(),
        }
    }

    pub fn with_payload_binding(mut self, payload_binding: ToolPayloadBinding) -> Self {
        self.payload_binding = payload_binding;
        self
    }
}

pub fn builtin_tool_specs() -> Vec<ConfiguredToolSpec> {
    vec![
        configured_builtin_spec(
            "exec_command",
            "Run a command (optionally interactive) and return output or a session id.",
            exec_command_schema(),
            PayloadKind::LocalShell,
            ExecutionClass::SessionScoped,
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Session,
                idempotency_mode: ToolIdempotencyMode::SessionBound,
                max_attempts: 3,
                can_resume: true,
            },
        ),
        configured_builtin_spec(
            "write_stdin",
            "Write bytes to an existing exec session stdin and read new output.",
            write_stdin_schema(),
            PayloadKind::LocalShell,
            ExecutionClass::SessionScoped,
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Session,
                idempotency_mode: ToolIdempotencyMode::SessionBound,
                max_attempts: 3,
                can_resume: true,
            },
        ),
        configured_builtin_spec(
            "read_file",
            "Read file contents from disk.",
            read_file_schema(),
            PayloadKind::Function,
            ExecutionClass::Shared,
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Arguments,
                idempotency_mode: ToolIdempotencyMode::Safe,
                max_attempts: 2,
                can_resume: false,
            },
        ),
        configured_builtin_spec(
            "list_dir",
            "List files/directories recursively.",
            list_dir_schema(),
            PayloadKind::Function,
            ExecutionClass::Shared,
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Arguments,
                idempotency_mode: ToolIdempotencyMode::Safe,
                max_attempts: 2,
                can_resume: false,
            },
        ),
        configured_builtin_spec(
            "grep_files",
            "Search file contents by regex/text pattern. Always pass the narrowest path and glob you can infer for codebase searches; do not repeat broad workspace searches after a needs_narrowing result.",
            grep_files_schema(),
            PayloadKind::Function,
            ExecutionClass::Shared,
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Arguments,
                idempotency_mode: ToolIdempotencyMode::Safe,
                max_attempts: 2,
                can_resume: false,
            },
        ),
        configured_builtin_spec(
            "apply_patch",
            "Apply patch in apply_patch format or pass JSON with input/patch string.",
            apply_patch_schema(),
            PayloadKind::Custom,
            ExecutionClass::Exclusive,
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Never,
                idempotency_mode: ToolIdempotencyMode::RequiresKey,
                max_attempts: 1,
                can_resume: false,
            },
        ),
        configured_builtin_spec(
            "web_search",
            "Search the web with DuckDuckGo and return ranked results.",
            web_search_schema(),
            PayloadKind::Function,
            ExecutionClass::Shared,
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Network,
                idempotency_mode: ToolIdempotencyMode::Safe,
                max_attempts: 3,
                can_resume: false,
            },
        ),
        configured_builtin_spec(
            "web_fetch",
            "Fetch a URL and extract content with mode markdown|text|raw|auto.",
            web_fetch_schema(),
            PayloadKind::Function,
            ExecutionClass::Shared,
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Network,
                idempotency_mode: ToolIdempotencyMode::Safe,
                max_attempts: 3,
                can_resume: true,
            },
        ),
        configured_builtin_spec(
            "download_url",
            "Download a URL to local disk and report file metadata.",
            download_url_schema(),
            PayloadKind::Function,
            ExecutionClass::SessionScoped,
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Network,
                idempotency_mode: ToolIdempotencyMode::Safe,
                max_attempts: 3,
                can_resume: true,
            },
        ),
        configured_builtin_spec(
            REQUEST_TOOLS_TOOL_NAME,
            "Request hidden builtin tool domains for the next provider round. Pass domain names only, not individual tool names.",
            request_tools_schema(),
            PayloadKind::Function,
            ExecutionClass::Shared,
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Arguments,
                idempotency_mode: ToolIdempotencyMode::Safe,
                max_attempts: 2,
                can_resume: false,
            },
        ),
    ]
}

pub fn builtin_tool_recovery_metadata(tool_name: &str) -> Option<ToolRecoveryMetadata> {
    builtin_tool_specs()
        .into_iter()
        .find(|configured| configured.spec.name == tool_name)
        .map(|configured| configured.spec.recovery)
}

fn configured_builtin_spec(
    name: impl Into<String>,
    description: impl Into<String>,
    parameters: JsonValue,
    payload_kind: PayloadKind,
    execution_class: ExecutionClass,
    recovery: ToolRecoveryMetadata,
) -> ConfiguredToolSpec {
    let name = name.into();
    ConfiguredToolSpec::new(
        ToolSpec::new(name.clone(), description, parameters, payload_kind).with_recovery(recovery),
        execution_class,
        builtin_output_policy(name.as_str()),
    )
}

#[cfg(feature = "computer-use")]
pub(crate) fn computer_use_configured_spec() -> ConfiguredToolSpec {
    configured_builtin_spec(
        "computer_use",
        "Remote desktop control loop. Required call shapes: start {action,goal}; snapshot {action,session_id}; act {action,session_id,act:{type,...}}; status {action,session_id}; stop {action,session_id}. For action=act, nested act object is mandatory.",
        computer_use_schema(),
        PayloadKind::Function,
        ExecutionClass::SessionScoped,
        ToolRecoveryMetadata {
            retry_class: ToolRetryClass::Session,
            idempotency_mode: ToolIdempotencyMode::SessionBound,
            max_attempts: 2,
            can_resume: true,
        },
    )
}

fn exec_command_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "description": "Execute a command by direct argv. For shell syntax such as pipes, redirects, globbing, or expansions, call an explicit shell in `command`, for example [\"/bin/sh\", \"-c\", \"printf ok | cat\"].",
        "properties": {
            "command": {
                "type": "array",
                "items": { "type": "string" },
                "minItems": 1,
                "description": "Command argv. The first item is the executable; remaining items are passed as arguments without implicit shell wrapping."
            },
            "workdir": { "type": "string" },
            "timeout_ms": { "type": "integer", "minimum": 1 },
            "max_output_tokens": { "type": "integer", "minimum": 1 },
            "yield_time_ms": { "type": "integer", "minimum": 0 },
            "tty": { "type": "boolean" }
        },
        "required": ["command"],
        "additionalProperties": false
    })
}

fn write_stdin_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "properties": {
            "session_id": { "type": "integer", "minimum": 1 },
            "chars": { "type": "string" },
            "yield_time_ms": { "type": "integer", "minimum": 0 },
            "max_output_tokens": { "type": "integer", "minimum": 1 }
        },
        "required": ["session_id"],
        "additionalProperties": false
    })
}

fn read_file_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "properties": {
            "path": { "type": "string" },
            "start_line": { "type": "integer", "minimum": 1 },
            "end_line": { "type": "integer", "minimum": 1 },
            "max_bytes": { "type": "integer", "minimum": 1 }
        },
        "required": ["path"],
        "additionalProperties": false
    })
}

fn list_dir_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "properties": {
            "path": { "type": "string" },
            "depth": { "type": "integer", "minimum": 0 },
            "limit": { "type": "integer", "minimum": 1 },
            "include_hidden": { "type": "boolean" }
        },
        "additionalProperties": false
    })
}

fn grep_files_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "properties": {
            "pattern": { "type": "string" },
            "path": { "type": "string" },
            "glob": { "type": "string" },
            "max_results": { "type": "integer", "minimum": 1 },
            "max_output_bytes": { "type": "integer", "minimum": 1 },
            "case_sensitive": { "type": "boolean" },
            "timeout_ms": { "type": "integer", "minimum": 1 }
        },
        "required": ["pattern"],
        "additionalProperties": false
    })
}

fn apply_patch_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "properties": {
            "input": { "type": "string", "description": "The full apply_patch payload" },
            "patch": { "type": "string", "description": "Alias of input" }
        },
        "additionalProperties": false
    })
}

fn web_search_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "properties": {
            "query": { "type": "string" },
            "q": { "type": "string" },
            "max_results": { "type": "integer", "minimum": 1, "maximum": 20 },
            "region": { "type": "string", "description": "DuckDuckGo region code like us-en, ru-ru." },
            "safesearch": {
                "type": "string",
                "enum": ["off", "moderate", "strict"],
                "description": "DuckDuckGo safesearch level."
            },
            "freshness": {
                "type": "string",
                "enum": ["any", "day", "week", "month", "year"],
                "description": "Time filter when available."
            },
            "max_snippet_chars": { "type": "integer", "minimum": 64, "maximum": 4096 }
        },
        "additionalProperties": false
    })
}

fn web_fetch_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "properties": {
            "url": { "type": "string", "format": "uri" },
            "extract_mode": {
                "type": "string",
                "enum": ["markdown", "text", "raw", "auto"],
                "description": "Content extraction mode."
            },
            "timeout_ms": { "type": "integer", "minimum": 1, "maximum": 120000 },
            "max_bytes": { "type": "integer", "minimum": 1024, "maximum": 8388608 },
            "include_headers": { "type": "boolean" },
            "follow_redirects": { "type": "boolean" }
        },
        "required": ["url"],
        "additionalProperties": false
    })
}

fn download_url_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "properties": {
            "url": { "type": "string", "format": "uri" },
            "destination": { "type": "string", "description": "Absolute or workdir-relative destination path." },
            "overwrite": { "type": "boolean" },
            "create_dirs": { "type": "boolean" },
            "timeout_ms": { "type": "integer", "minimum": 1, "maximum": 120000 },
            "max_bytes": { "type": "integer", "minimum": 1024, "maximum": 1073741824 },
            "follow_redirects": { "type": "boolean" }
        },
        "required": ["url"],
        "additionalProperties": false
    })
}

fn request_tools_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "properties": {
            "domains": {
                "type": "array",
                "description": "Hidden builtin domains to make available in the next provider round. Use domain enum values, not individual tool names.",
                "items": {
                    "type": "string",
                    "enum": REQUEST_TOOLS_DOMAIN_VALUES
                },
                "minItems": 1
            },
            "reason": {
                "type": "string",
                "description": "Short diagnostic reason for requesting hidden tool domains.",
                "minLength": 1,
                "maxLength": REQUEST_TOOLS_REASON_MAX_CHARS
            }
        },
        "required": ["domains", "reason"],
        "additionalProperties": false
    })
}

#[cfg(feature = "computer-use")]
fn computer_use_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "description": "Session-oriented remote computer control. When action is `act`, arguments MUST include both `session_id` and nested `act` object.",
        "properties": {
            "action": {
                "type": "string",
                "description": "Operation name. Use `act` only with nested `act` object.",
                "enum": [
                    "list_displays",
                    "start",
                    "snapshot",
                    "act",
                    "status",
                    "stop"
                ]
            },
            "session_id": { "type": "integer", "minimum": 1 },
            "goal": { "type": "string" },
            "max_steps": { "type": "integer", "minimum": 1, "maximum": 200 },
            "timeout_ms": { "type": "integer", "minimum": 1000, "maximum": 3600000 },
            "display_id": { "type": "integer", "minimum": 0 },
            "planner_provider": { "type": "string" },
            "planner_model": { "type": "string" },
            "snapshot_max_bytes": { "type": "integer", "minimum": 262144, "maximum": 67108864 },
            "snapshot_max_side_px": { "type": "integer", "minimum": 320, "maximum": 4096 },
            "screenshot_path": { "type": "string" },
            "recovery_attempt": { "type": "integer", "minimum": 0, "maximum": 100 },
            "expected_effect_mismatch": { "type": "boolean" },
            "failure_class": {
                "type": "string",
                "enum": [
                    "attachment_transport_failure",
                    "provider_timeout",
                    "provider_rate_limit",
                    "expected_effect_mismatch",
                    "policy_blocked",
                    "runtime_action_error",
                    "loop_guard_triggered",
                    "recovery_budget_exceeded"
                ]
            },
            "outcome": { "type": "string", "enum": ["stopped", "completed", "failed"] },
            "reason": { "type": "string" },
            "act": {
                "type": "object",
                "description": "Required when action=`act`. Nested action payload to execute on the desktop.",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": [
                            "click",
                            "double_click",
                            "right_click",
                            "move",
                            "scroll",
                            "type_text",
                            "hotkey",
                            "wait"
                        ]
                    },
                    "x_norm": { "type": "number", "minimum": 0, "maximum": 1 },
                    "y_norm": { "type": "number", "minimum": 0, "maximum": 1 },
                    "button": { "type": "string", "enum": ["left", "right", "middle"] },
                    "delta_x": { "type": "integer" },
                    "delta_y": { "type": "integer" },
                    "text": { "type": "string" },
                    "keys": {
                        "type": "array",
                        "items": { "type": "string" },
                        "minItems": 1,
                        "maxItems": 5
                    },
                    "wait_ms": { "type": "integer", "minimum": 1, "maximum": 60000 }
                },
                "required": ["type"],
                "additionalProperties": false
            }
        },
        "allOf": [
            {
                "if": { "properties": { "action": { "const": "start" } }, "required": ["action"] },
                "then": { "required": ["goal"] }
            },
            {
                "if": { "properties": { "action": { "const": "snapshot" } }, "required": ["action"] },
                "then": { "required": ["session_id"] }
            },
            {
                "if": { "properties": { "action": { "const": "act" } }, "required": ["action"] },
                "then": { "required": ["session_id", "act"] }
            },
            {
                "if": { "properties": { "action": { "const": "status" } }, "required": ["action"] },
                "then": { "required": ["session_id"] }
            },
            {
                "if": { "properties": { "action": { "const": "stop" } }, "required": ["action"] },
                "then": { "required": ["session_id"] }
            }
        ],
        "required": ["action"],
        "additionalProperties": false
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn builtin_specs_have_unique_names() {
        let specs = builtin_tool_specs();
        let mut names = HashSet::new();
        for configured in specs {
            assert!(
                names.insert(configured.spec.name.clone()),
                "duplicate builtin tool: {}",
                configured.spec.name
            );
        }
    }

    #[test]
    fn builtin_specs_include_expected_execution_classes() {
        let specs = builtin_tool_specs();
        let by_name = specs
            .into_iter()
            .map(|configured| (configured.spec.name, configured.execution_class))
            .collect::<std::collections::HashMap<_, _>>();

        assert_eq!(by_name.get("apply_patch"), Some(&ExecutionClass::Exclusive));
        assert_eq!(
            by_name.get("write_stdin"),
            Some(&ExecutionClass::SessionScoped)
        );
        assert_eq!(by_name.get("read_file"), Some(&ExecutionClass::Shared));
        assert_eq!(
            by_name.get(REQUEST_TOOLS_TOOL_NAME),
            Some(&ExecutionClass::Shared)
        );
    }

    #[test]
    fn exec_command_model_schema_requires_direct_argv() {
        let specs = builtin_tool_specs();
        let exec = specs
            .iter()
            .find(|configured| configured.spec.name == "exec_command")
            .expect("exec_command spec should exist");
        let properties = exec.spec.parameters["properties"]
            .as_object()
            .expect("properties should be an object");

        assert!(properties.contains_key("command"));
        assert!(!properties.contains_key("cmd"));
        assert!(!properties.contains_key("shell"));
        assert!(!properties.contains_key("login"));
        assert_eq!(
            exec.spec.parameters["required"],
            serde_json::json!(["command"])
        );
    }

    #[test]
    fn builtin_specs_define_recovery_metadata() {
        let specs = builtin_tool_specs();
        let by_name = specs
            .into_iter()
            .map(|configured| (configured.spec.name, configured.spec.recovery))
            .collect::<std::collections::HashMap<_, _>>();

        assert_eq!(
            by_name
                .get("apply_patch")
                .map(|metadata| metadata.max_attempts),
            Some(1)
        );
        assert_eq!(
            by_name
                .get("write_stdin")
                .map(|metadata| metadata.can_resume),
            Some(true)
        );
        assert_eq!(
            by_name
                .get("web_fetch")
                .map(|metadata| metadata.retry_class),
            Some(ToolRetryClass::Network)
        );
    }

    #[test]
    fn builtin_specs_define_explicit_output_policy() {
        for configured in builtin_tool_specs() {
            assert_eq!(
                configured.output_policy,
                builtin_output_policy(configured.spec.name.as_str()),
                "builtin tool {} must carry its explicit output policy",
                configured.spec.name
            );
        }
    }

    #[cfg(feature = "computer-use")]
    #[test]
    fn computer_use_configured_spec_preserves_runtime_contract() {
        let configured = computer_use_configured_spec();

        assert_eq!(configured.spec.name, "computer_use");
        assert_eq!(configured.spec.payload_kind, PayloadKind::Function);
        assert_eq!(configured.execution_class, ExecutionClass::SessionScoped);
        assert_eq!(
            configured.output_policy,
            builtin_output_policy("computer_use")
        );
        assert_eq!(
            configured.spec.recovery.retry_class,
            ToolRetryClass::Session
        );
        assert_eq!(
            configured.spec.recovery.idempotency_mode,
            ToolIdempotencyMode::SessionBound
        );
        assert_eq!(
            configured.spec.parameters["required"],
            serde_json::json!(["action"])
        );
    }

    #[test]
    fn request_tools_schema_defines_strict_domain_contract() {
        let specs = builtin_tool_specs();
        let request_tools = specs
            .iter()
            .find(|configured| configured.spec.name == REQUEST_TOOLS_TOOL_NAME)
            .expect("request_tools spec should exist");

        assert_eq!(request_tools.spec.payload_kind, PayloadKind::Function);
        assert_eq!(
            request_tools.spec.parameters["required"],
            serde_json::json!(["domains", "reason"])
        );
        assert_eq!(
            request_tools.spec.parameters["additionalProperties"],
            serde_json::json!(false)
        );
        assert_eq!(
            request_tools.spec.parameters["properties"]["domains"]["minItems"],
            serde_json::json!(1)
        );
        assert_eq!(
            request_tools.spec.parameters["properties"]["reason"]["minLength"],
            serde_json::json!(1)
        );
        assert_eq!(
            request_tools.spec.parameters["properties"]["reason"]["maxLength"],
            serde_json::json!(REQUEST_TOOLS_REASON_MAX_CHARS)
        );
        assert_eq!(
            request_tools.spec.parameters["properties"]["domains"]["items"]["enum"],
            serde_json::json!(REQUEST_TOOLS_DOMAIN_VALUES)
        );
        assert!(
            !request_tools.spec.parameters["properties"]["domains"]["items"]["enum"]
                .as_array()
                .expect("domain enum should be an array")
                .iter()
                .any(|value| value.as_str() == Some("task_create")),
            "request_tools must accept domains, not individual tool names"
        );
    }
}
