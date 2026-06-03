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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wall_clock_secs: Option<u64>,
}

impl Default for ToolRecoveryMetadata {
    fn default() -> Self {
        Self {
            retry_class: ToolRetryClass::Transient,
            idempotency_mode: ToolIdempotencyMode::None,
            max_attempts: 2,
            can_resume: false,
            max_wall_clock_secs: None,
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
                max_wall_clock_secs: None,
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
                max_wall_clock_secs: None,
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
                max_wall_clock_secs: None,
            },
        ),
        configured_builtin_spec(
            "write_file",
            "Create a file from complete UTF-8 content or fully overwrite an existing file after current-file state has been observed. Use this for writing complete file contents. Do not use exec_command, shell heredocs, or write_stdin to create ordinary files.",
            write_file_schema(),
            PayloadKind::Function,
            ExecutionClass::Exclusive,
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Arguments,
                idempotency_mode: ToolIdempotencyMode::RequiresKey,
                max_attempts: 2,
                can_resume: false,
                max_wall_clock_secs: None,
            },
        ),
        configured_builtin_spec(
            "edit_file",
            "Edit an existing file whose contents are valid UTF-8, such as source code, config, Markdown, JSON, or YAML, by replacing an exact old_string with new_string after current-file state has been observed. Use write_file for file creation or full-file rewrites, and apply_patch for coordinated diff-style or multi-file patches.",
            edit_file_schema(),
            PayloadKind::Function,
            ExecutionClass::Exclusive,
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Arguments,
                idempotency_mode: ToolIdempotencyMode::RequiresKey,
                max_attempts: 2,
                can_resume: false,
                max_wall_clock_secs: None,
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
                max_wall_clock_secs: None,
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
                max_wall_clock_secs: None,
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
                max_wall_clock_secs: None,
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
                max_wall_clock_secs: None,
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
                max_wall_clock_secs: None,
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
                max_wall_clock_secs: None,
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
                max_wall_clock_secs: None,
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
        "Remote desktop control loop for visible app/UI tasks. Use computer_use for desktop UI state, screenshots, accessibility actions, and explicit input simulation. exec_command may be used for diagnostics or app metadata lookup, but not as a replacement for UI clicking/typing/verification. Layered control order: top-level OS/session operations first (preflight, list_apps, start, snapshot, status, stop); high-level OS actions in action=act second (open_app, activate_app, open_path, reveal_path, open_url, select_menu_item, focus_window); semantic accessibility actions third (press/focus/set_value/type_text with node_id or selector); explicit input_* simulation fallback last. start requires explicit target: app_name/pid/identity_key/bundle_id/executable_path/active_app for app tasks, screen only for whole-desktop tasks. Do not invent unsupported nested act.type verbs such as open; use open_app/open_path/open_url. input_key sends exactly one key; input_chord sends a multi-key hotkey. The preflight action is a computer_use OS capability/permission check and is unrelated to gateway model/provider preflight. Do not use stop outcome=completed until computer_use verify has passed or the latest post-action snapshot returned completion_evidence.",
        computer_use_schema(),
        PayloadKind::Function,
        ExecutionClass::SessionScoped,
        ToolRecoveryMetadata {
            retry_class: ToolRetryClass::Session,
            idempotency_mode: ToolIdempotencyMode::SessionBound,
            max_attempts: 2,
            can_resume: true,
            max_wall_clock_secs: None,
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

fn write_file_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Absolute path, or path relative to the tool invocation workdir."
            },
            "content": {
                "type": "string",
                "description": "Complete UTF-8 file contents to write."
            },
            "create_dirs": {
                "type": "boolean",
                "description": "Create missing parent directories. Defaults to true."
            },
            "overwrite": {
                "type": "boolean",
                "description": "Allow replacing an existing file. Defaults to true."
            },
            "read_observation_id": {
                "type": "string",
                "description": "Optional id from a prior complete read_file result for the same target path."
            },
            "expected_sha256": {
                "type": "string",
                "description": "Optional current file SHA-256 precondition for stale-write protection."
            },
            "expected_mtime_ms": {
                "type": "integer",
                "description": "Optional current file mtime precondition in Unix epoch milliseconds."
            }
        },
        "required": ["path", "content"],
        "additionalProperties": false
    })
}

fn edit_file_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Absolute path, or path relative to the tool invocation workdir."
            },
            "old_string": {
                "type": "string",
                "description": "Exact current text to replace. Must be non-empty."
            },
            "new_string": {
                "type": "string",
                "description": "Replacement text. May be empty to delete old_string."
            },
            "replace_all": {
                "type": "boolean",
                "description": "Replace every occurrence of old_string. Defaults to false."
            },
            "read_observation_id": {
                "type": "string",
                "description": "Optional id from a prior complete read_file result for the same target path."
            },
            "expected_sha256": {
                "type": "string",
                "description": "Optional current file SHA-256 precondition for stale-edit protection."
            },
            "expected_mtime_ms": {
                "type": "integer",
                "description": "Optional current file mtime precondition in Unix epoch milliseconds."
            }
        },
        "required": ["path", "old_string", "new_string"],
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
        "description": "Session-oriented remote computer control for visible app/UI tasks. Use this tool for desktop UI state, screenshots, accessibility actions, and explicit input simulation. exec_command may be used for diagnostics or app metadata lookup, but not as a replacement for UI clicking/typing/verification. Layered control order: top-level OS/session operations first, high-level OS act.type actions second, semantic accessibility actions third, explicit input simulation fallback last. `preflight` checks OS desktop-control permissions/capabilities without creating a session. `start` requires explicit target: use app_name/pid/identity_key/bundle_id/executable_path/active_app for app tasks and screen only for whole-desktop tasks. When action is `act`, arguments MUST include both `session_id` and nested `act` object. Do not call snapshot/act/verify/status/stop until a successful start returns session_id. Do not use `stop` with `outcome=completed` until `verify` passes or the latest post-action snapshot includes `completion_evidence`.",
        "examples": [
            { "action": "preflight" },
            {
                "action": "start",
                "goal": "Open the requested application",
                "target": {
                    "type": "app_name",
                    "name": "ExampleApp",
                    "launch_if_missing": true
                }
            },
            { "action": "snapshot", "session_id": 1 },
            {
                "action": "act",
                "session_id": 1,
                "act": {
                    "type": "open_app",
                    "app": "ExampleApp"
                }
            },
            {
                "action": "act",
                "session_id": 1,
                "act": {
                    "type": "open_path",
                    "path": "/absolute/path"
                }
            },
            {
                "action": "act",
                "session_id": 1,
                "act": {
                    "type": "press",
                    "target": { "node_id": "n42", "snapshot_id": "s1-1" }
                }
            },
            {
                "action": "act",
                "session_id": 1,
                "act": {
                    "type": "input_chord",
                    "keys": ["meta", "space"]
                }
            },
            {
                "action": "act",
                "session_id": 1,
                "act": {
                    "type": "input_click",
                    "target": { "point": { "x": 100, "y": 200, "coordinate_space": "source_pixels" } }
                }
            },
            {
                "action": "verify",
                "session_id": 1,
                "expect": {
                    "app": "ExampleApp",
                    "window_title": "Example Window",
                    "visible_text": "Expected visible text"
                }
            }
        ],
        "properties": {
            "action": {
                "type": "string",
                "description": "Operation name. `preflight` is a computer_use desktop permission/capability check and does not require session_id. Use `act` only with nested `act` object.",
                "enum": [
                    "preflight",
                    "list_apps",
                    "list_displays",
                    "start",
                    "snapshot",
                    "act",
                    "verify",
                    "status",
                    "stop"
                ]
            },
            "session_id": { "type": "integer", "minimum": 1 },
            "goal": { "type": "string" },
            "target": {
                "type": "object",
                "description": "Required for action=start. Desktop target for a session. Prefer app_name, pid, identity_key, bundle_id, executable_path, or active_app for accessibility control; use screen only for whole-desktop tasks.",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["screen", "active_app", "app_name", "app", "pid", "identity_key", "bundle_id", "executable_path"]
                    },
                    "name": { "type": "string" },
                    "pid": { "type": "integer", "minimum": 1 },
                    "identity_key": { "type": "string" },
                    "bundle_id": { "type": "string" },
                    "executable_path": { "type": "string" },
                    "display_id": { "type": "integer", "minimum": 0 },
                    "launch_if_missing": { "type": "boolean" },
                    "launch_command": { "type": "string" },
                    "activation_timeout_ms": { "type": "integer", "minimum": 0, "maximum": 120000 },
                    "tree_max_depth": { "type": "integer", "minimum": 1, "maximum": 50 }
                },
                "required": ["type"],
                "additionalProperties": false
            },
            "max_steps": { "type": "integer", "minimum": 1, "maximum": 200 },
            "timeout_ms": { "type": "integer", "minimum": 1000, "maximum": 3600000 },
            "display_id": { "type": "integer", "minimum": 0 },
            "launch_if_missing": { "type": "boolean" },
            "launch_command": { "type": "string" },
            "activation_timeout_ms": { "type": "integer", "minimum": 0, "maximum": 120000 },
            "tree_max_depth": { "type": "integer", "minimum": 1, "maximum": 50 },
            "planner_provider": { "type": "string" },
            "planner_model": { "type": "string" },
            "snapshot_max_bytes": { "type": "integer", "minimum": 262144, "maximum": 67108864 },
            "snapshot_max_side_px": { "type": "integer", "minimum": 320, "maximum": 4096 },
            "screenshot_path": { "type": "string" },
            "recovery_attempt": { "type": "integer", "minimum": 0, "maximum": 100 },
            "failure_class": {
                "type": "string",
                "enum": [
                    "permission_denied",
                    "accessibility_unavailable",
                    "accessibility_not_enabled",
                    "app_not_found",
                    "element_not_found",
                    "element_stale",
                    "action_not_supported",
                    "input_simulation_unavailable",
                    "screenshot_unavailable",
                    "attachment_transport_failure",
                    "provider_timeout",
                    "provider_rate_limit",
                    "loop_guard_triggered",
                    "recovery_budget_exceeded",
                    "runtime_action_error"
                ]
            },
            "outcome": { "type": "string", "enum": ["stopped", "completed", "failed"] },
            "reason": { "type": "string" },
            "expect": computer_use_verify_expect_schema(),
            "act": computer_use_act_schema()
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
                "if": { "properties": { "action": { "const": "verify" } }, "required": ["action"] },
                "then": { "required": ["session_id", "expect"] }
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

#[cfg(feature = "computer-use")]
fn computer_use_verify_expect_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "description": "Deterministic verification expectations checked against the latest computer_use snapshot/accessibility tree. Does not call OCR/VLM/provider.",
        "properties": {
            "app": { "type": "string" },
            "window_title": { "type": "string" },
            "visible_text": { "type": "string" },
            "node": {
                "type": "object",
                "properties": {
                    "node_id": { "type": "string" },
                    "selector": { "type": "string" },
                    "role": { "type": "string" },
                    "name": { "type": "string" }
                },
                "additionalProperties": false
            },
            "snapshot_hash_changed": { "type": "boolean" }
        },
        "additionalProperties": false
    })
}

#[cfg(feature = "computer-use")]
fn computer_use_act_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "description": "Required when action=`act`. High-level OS actions are preferred for common desktop tasks; semantic accessibility actions are next; explicit input_* actions are fallback-only. Do not use unsupported high-level verbs such as open; use open_app/open_path/open_url. input_key sends exactly one key; input_chord sends a multi-key hotkey/chord.",
        "examples": [
            { "type": "open_app", "app": "ExampleApp" },
            { "type": "open_path", "path": "/absolute/path" },
            { "type": "select_menu_item", "app": "ExampleApp", "menu_path": ["File", "New Window"] },
            { "type": "focus_window", "app": "ExampleApp", "title": "Example Window" },
            { "type": "press", "target": { "node_id": "n42", "snapshot_id": "s1-1" } },
            { "type": "input_key", "keys": ["enter"] },
            { "type": "input_chord", "keys": ["meta", "space"] },
            { "type": "input_click", "target": { "point": { "x": 100, "y": 200, "coordinate_space": "source_pixels" } } }
        ],
        "properties": {
            "type": {
                "type": "string",
                "description": "OS actions operate on apps, paths, URLs, menus, or windows. Semantic actions target accessibility nodes. Explicit input_* actions use raw input simulation and should be used only when OS/semantic actions are insufficient. input_key requires exactly one key in keys; input_chord requires keys for a simultaneous hotkey/chord.",
                "enum": [
                    "open_app",
                    "activate_app",
                    "open_path",
                    "reveal_path",
                    "open_url",
                    "select_menu_item",
                    "focus_window",
                    "press",
                    "focus",
                    "blur",
                    "toggle",
                    "select",
                    "expand",
                    "collapse",
                    "show_menu",
                    "scroll_into_view",
                    "set_value",
                    "set_numeric_value",
                    "type_text",
                    "select_text",
                    "perform_action",
                    "wait_for",
                    "input_click",
                    "input_double_click",
                    "input_right_click",
                    "input_move",
                    "input_drag",
                    "input_scroll",
                    "input_key",
                    "input_chord",
                    "input_type_text",
                    "wait"
                ]
            },
            "target": computer_use_action_target_schema(true),
            "from": computer_use_action_target_schema(true),
            "to": computer_use_action_target_schema(true),
            "button": { "type": "string", "enum": ["left", "right", "middle"] },
            "delta_x": { "type": "integer" },
            "delta_y": { "type": "integer" },
            "text": { "type": "string" },
            "numeric_value": { "type": "number" },
            "action_name": { "type": "string" },
            "condition": { "type": "string" },
            "app": {
                "type": "string",
                "description": "Required for open_app, activate_app, select_menu_item, and focus_window."
            },
            "path": {
                "type": "string",
                "description": "Required for open_path and reveal_path."
            },
            "url": {
                "type": "string",
                "description": "Required for open_url."
            },
            "menu_path": {
                "type": "array",
                "description": "Required for select_menu_item; ordered menu labels such as [\"File\",\"New Window\"].",
                "items": { "type": "string", "minLength": 1 },
                "minItems": 1
            },
            "title": {
                "type": "string",
                "description": "Optional window title filter for focus_window."
            },
            "keys": {
                "type": "array",
                "description": "For input_key, provide exactly one key such as [\"enter\"]. For input_chord, provide a simultaneous hotkey/chord such as [\"meta\",\"space\"].",
                "items": { "type": "string" },
                "minItems": 1,
                "maxItems": 5
            },
            "wait_ms": { "type": "integer", "minimum": 1, "maximum": 60000 }
        },
        "required": ["type"],
        "additionalProperties": false
    })
}

#[cfg(feature = "computer-use")]
fn computer_use_action_target_schema(allow_point: bool) -> JsonValue {
    let mut properties = serde_json::json!({
        "node_id": { "type": "string" },
        "snapshot_id": {
            "type": "string",
            "description": "Required when using node_id; copy it from the latest computer_use snapshot response. Omit for selector/role/name targets."
        },
        "selector": { "type": "string" },
        "role": { "type": "string" },
        "name": { "type": "string" },
        "nth": { "type": "integer", "minimum": 1 },
        "bounds_anchor": {
            "type": "object",
            "properties": {
                "node_id": { "type": "string" },
                "snapshot_id": {
                    "type": "string",
                    "description": "Required with bounds_anchor.node_id; copy it from the latest snapshot."
                },
                "anchor": {
                    "type": "string",
                    "enum": ["center", "top_left", "top_right", "bottom_left", "bottom_right"]
                }
            },
            "required": ["node_id", "snapshot_id"],
            "additionalProperties": false
        }
    });
    if allow_point {
        if let Some(object) = properties.as_object_mut() {
            object.insert(
                "point".to_owned(),
                serde_json::json!({
                    "type": "object",
                    "description": "Explicit point target. coordinate_space is required in the tool schema to prevent mixing source screenshot pixels, downscaled transport pixels, accessibility logical coordinates, and native input coordinates. Runtime only defaults missing coordinate_space to source_pixels when the latest snapshot was not transformed.",
                    "properties": {
                        "x": { "type": "integer" },
                        "y": { "type": "integer" },
                        "coordinate_space": {
                            "type": "string",
                            "enum": ["source_pixels", "transport_pixels", "logical_screen", "native_input"],
                            "description": "source_pixels = original screenshot pixel coordinates; transport_pixels = downscaled image sent to the LLM; logical_screen = accessibility/display logical bounds; native_input = coordinates accepted by the input backend."
                        }
                    },
                    "required": ["x", "y", "coordinate_space"],
                    "additionalProperties": false
                }),
            );
        }
    }
    serde_json::json!({
        "type": "object",
        "description": if allow_point {
            "Action target. Point targets are valid only for explicit input_* actions and should include coordinate_space. node_id and bounds_anchor.node_id targets must include snapshot_id from the latest snapshot."
        } else {
            "Semantic accessibility target. Use node_id with snapshot_id from the latest snapshot when available; point coordinates are intentionally not accepted for semantic actions."
        },
        "properties": properties,
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
        assert_eq!(by_name.get("write_file"), Some(&ExecutionClass::Exclusive));
        assert_eq!(by_name.get("edit_file"), Some(&ExecutionClass::Exclusive));
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
    fn write_file_schema_matches_contract() {
        let schema = write_file_schema();
        let properties = schema["properties"]
            .as_object()
            .expect("write_file properties should be an object");
        let property_names = properties.keys().cloned().collect::<HashSet<_>>();

        assert_eq!(schema["type"], serde_json::json!("object"));
        assert_eq!(schema["required"], serde_json::json!(["path", "content"]));
        assert_eq!(schema["additionalProperties"], serde_json::json!(false));
        assert_eq!(
            property_names,
            HashSet::from([
                "path".to_owned(),
                "content".to_owned(),
                "create_dirs".to_owned(),
                "overwrite".to_owned(),
                "read_observation_id".to_owned(),
                "expected_sha256".to_owned(),
                "expected_mtime_ms".to_owned(),
            ])
        );
        assert_eq!(properties["path"]["type"], serde_json::json!("string"));
        assert_eq!(properties["content"]["type"], serde_json::json!("string"));
        assert_eq!(
            properties["create_dirs"]["type"],
            serde_json::json!("boolean")
        );
        assert_eq!(
            properties["overwrite"]["type"],
            serde_json::json!("boolean")
        );
        assert_eq!(
            properties["read_observation_id"]["type"],
            serde_json::json!("string")
        );
        assert_eq!(
            properties["expected_sha256"]["type"],
            serde_json::json!("string")
        );
        assert_eq!(
            properties["expected_mtime_ms"]["type"],
            serde_json::json!("integer")
        );
    }

    #[test]
    fn edit_file_schema_matches_contract() {
        let schema = edit_file_schema();
        let properties = schema["properties"]
            .as_object()
            .expect("edit_file properties should be an object");
        let property_names = properties.keys().cloned().collect::<HashSet<_>>();

        assert_eq!(schema["type"], serde_json::json!("object"));
        assert_eq!(
            schema["required"],
            serde_json::json!(["path", "old_string", "new_string"])
        );
        assert_eq!(schema["additionalProperties"], serde_json::json!(false));
        assert_eq!(
            property_names,
            HashSet::from([
                "path".to_owned(),
                "old_string".to_owned(),
                "new_string".to_owned(),
                "replace_all".to_owned(),
                "read_observation_id".to_owned(),
                "expected_sha256".to_owned(),
                "expected_mtime_ms".to_owned(),
            ])
        );
        assert_eq!(properties["path"]["type"], serde_json::json!("string"));
        assert_eq!(
            properties["old_string"]["type"],
            serde_json::json!("string")
        );
        assert_eq!(
            properties["new_string"]["type"],
            serde_json::json!("string")
        );
        assert_eq!(
            properties["replace_all"]["type"],
            serde_json::json!("boolean")
        );
        assert_eq!(
            properties["read_observation_id"]["type"],
            serde_json::json!("string")
        );
        assert_eq!(
            properties["expected_sha256"]["type"],
            serde_json::json!("string")
        );
        assert_eq!(
            properties["expected_mtime_ms"]["type"],
            serde_json::json!("integer")
        );
        assert!(
            properties["old_string"]["description"]
                .as_str()
                .unwrap_or_default()
                .contains("non-empty")
        );
    }

    #[test]
    fn write_file_builtin_spec_matches_contract() {
        let specs = builtin_tool_specs();
        let configured = specs
            .iter()
            .find(|configured| configured.spec.name == "write_file")
            .expect("write_file spec should exist");

        assert_eq!(configured.spec.payload_kind, PayloadKind::Function);
        assert_eq!(configured.execution_class, ExecutionClass::Exclusive);
        assert_eq!(
            configured.spec.recovery.retry_class,
            ToolRetryClass::Arguments
        );
        assert_eq!(
            configured.spec.recovery.idempotency_mode,
            ToolIdempotencyMode::RequiresKey
        );
        assert_eq!(configured.spec.recovery.max_attempts, 2);
        assert!(!configured.spec.recovery.can_resume);
        assert_eq!(configured.spec.recovery.max_wall_clock_secs, None);
        assert_eq!(configured.spec.parameters, write_file_schema());
    }

    #[test]
    fn edit_file_builtin_spec_matches_contract() {
        let specs = builtin_tool_specs();
        let configured = specs
            .iter()
            .find(|configured| configured.spec.name == "edit_file")
            .expect("edit_file spec should exist");

        assert_eq!(configured.spec.payload_kind, PayloadKind::Function);
        assert_eq!(configured.execution_class, ExecutionClass::Exclusive);
        assert_eq!(
            configured.spec.recovery.retry_class,
            ToolRetryClass::Arguments
        );
        assert_eq!(
            configured.spec.recovery.idempotency_mode,
            ToolIdempotencyMode::RequiresKey
        );
        assert_eq!(configured.spec.recovery.max_attempts, 2);
        assert!(!configured.spec.recovery.can_resume);
        assert_eq!(configured.spec.recovery.max_wall_clock_secs, None);
        assert_eq!(configured.spec.parameters, edit_file_schema());
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
        let actions = configured.spec.parameters["properties"]["action"]["enum"]
            .as_array()
            .expect("computer_use action enum");
        assert!(
            actions.contains(&serde_json::json!("preflight")),
            "computer_use must expose its own desktop permission preflight action"
        );
        assert!(
            configured.spec.description.contains("unrelated to gateway"),
            "computer_use preflight description must distinguish it from gateway model/provider preflight"
        );
        assert!(
            configured
                .spec
                .description
                .contains("Layered control order")
                && configured
                    .spec
                    .description
                    .contains("exec_command may be used for diagnostics"),
            "computer_use description must tell the model the desktop control policy"
        );
        assert!(
            configured
                .spec
                .description
                .contains("input_key sends exactly one key")
                && configured
                    .spec
                    .description
                    .contains("input_chord sends a multi-key hotkey"),
            "computer_use description must distinguish input_key from input_chord"
        );

        let parameters = &configured.spec.parameters;
        let examples = parameters["examples"]
            .as_array()
            .expect("computer_use schema examples");
        for required_example in [
            serde_json::json!({ "action": "preflight" }),
            serde_json::json!({
                "action": "snapshot",
                "session_id": 1
            }),
            serde_json::json!({
                "action": "act",
                "session_id": 1,
                "act": {
                    "type": "open_app",
                    "app": "ExampleApp"
                }
            }),
            serde_json::json!({
                "action": "act",
                "session_id": 1,
                "act": {
                    "type": "open_path",
                    "path": "/absolute/path"
                }
            }),
            serde_json::json!({
                "action": "act",
                "session_id": 1,
                "act": {
                    "type": "press",
                    "target": { "node_id": "n42", "snapshot_id": "s1-1" }
                }
            }),
            serde_json::json!({
                "action": "act",
                "session_id": 1,
                "act": {
                    "type": "input_chord",
                    "keys": ["meta", "space"]
                }
            }),
            serde_json::json!({
                "action": "act",
                "session_id": 1,
                "act": {
                    "type": "input_click",
                    "target": { "point": { "x": 100, "y": 200, "coordinate_space": "source_pixels" } }
                }
            }),
        ] {
            assert!(
                examples.contains(&required_example),
                "missing computer_use example: {required_example}"
            );
        }
        assert!(
            examples.iter().any(|example| {
                example.get("action").and_then(JsonValue::as_str) == Some("start")
                    && example.pointer("/target/type").and_then(JsonValue::as_str)
                        == Some("app_name")
            }),
            "computer_use schema must include start target.type=app_name example"
        );

        let act_enum = parameters["properties"]["act"]["properties"]["type"]["enum"]
            .as_array()
            .expect("computer_use act.type enum");
        assert!(
            !act_enum.contains(&serde_json::json!("open")),
            "computer_use act.type enum must not imply unsupported open action"
        );
        let act_schema_text =
            serde_json::to_string(&parameters["properties"]["act"]).expect("act schema JSON");
        assert!(
            act_schema_text.contains("input_key requires exactly one key")
                && act_schema_text.contains("input_chord requires keys"),
            "act schema must document input_key versus input_chord"
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
