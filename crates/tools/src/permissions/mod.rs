use crate::apply_patch::file_mutation::{
    PatchError, PatchErrorCode, PatchLimits, PatchRequest, PatchRequestSource, PatchStage,
    Retryability, TargetResolver, TargetRole,
};
use crate::apply_patch::{
    ExecutionReport, OperationKind, PrepareOptions, TelemetryStage, ValidatedPatchDocument, parse,
    patch_telemetry, resolve_patch, validate_guards,
};
use crate::context::ToolPayload;
use crate::context::{ApplyPatchPreflight, ExecCommandArgs, ToolInvocation, WriteStdinArgs};
use crate::domain::{ARTIFACT_DOMAIN_TOOL_NAMES, MEMORY_DOMAIN_TOOL_NAMES, TASK_DOMAIN_TOOL_NAMES};
use crate::handlers::apply_patch::extract_patch_input;
use crate::spec::DynamicSkillPermissionKind;
use crate::{FilePolicyChecker, FilePolicyDecision, FilePolicyDenyReason, FilePolicyOperation};
use async_trait::async_trait;
use pioneer_mcp::{McpToolPermissionClass, McpToolSafetyHints, classify_mcp_tool_policy};
use pioneer_protocol::{
    PermissionBehavior, ToolPermissionPolicySnapshot, TurnPermissionProfileSnapshot,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::path::{Component, Path, PathBuf};
use std::time::Instant;
use url::Url;

pub use pioneer_protocol::{
    TurnPermissionActionKind as PermissionActionKind,
    TurnPermissionDecisionReason as PermissionDecisionReason,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionEvaluationContext {
    pub workspace_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub permission_profile: TurnPermissionProfileSnapshot,
}

impl PermissionEvaluationContext {
    pub fn for_turn(
        workspace_id: impl Into<String>,
        thread_id: impl Into<String>,
        turn_id: impl Into<String>,
        permission_profile: TurnPermissionProfileSnapshot,
    ) -> Self {
        Self {
            workspace_id: Some(workspace_id.into()),
            thread_id: Some(thread_id.into()),
            turn_id: Some(turn_id.into()),
            permission_profile,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionIntent {
    pub action: PermissionActionKind,
    pub scope: PermissionRequestScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

impl PermissionIntent {
    pub fn new(action: PermissionActionKind, scope: PermissionRequestScope) -> Self {
        Self {
            action,
            scope,
            summary: None,
        }
    }

    pub fn generic_for_invocation(invocation: &ToolInvocation) -> Self {
        let scope = PermissionRequestScope::from_pairs([
            ("tool_name", invocation.tool_name.as_str()),
            ("source", invocation.source.as_str()),
        ]);
        let scope = mark_unknown_capability(
            scope,
            "no specific permission classifier matched this tool invocation",
        );
        Self {
            action: PermissionActionKind::Unknown,
            scope,
            summary: Some(format!("tool `{}`", invocation.tool_name)),
        }
    }

    pub fn request_key(
        &self,
        context: &PermissionEvaluationContext,
        invocation: &ToolInvocation,
    ) -> PermissionRequestKey {
        PermissionRequestKey {
            profile_mode: context.permission_profile.mode,
            tool_name: invocation.tool_name.clone(),
            action: self.action,
            normalized_scope_hash: self.scope.normalized_hash(),
            turn_id: context.turn_id.clone().unwrap_or_default(),
        }
    }

    pub fn is_unknown_capability(&self) -> bool {
        self.scope
            .entries
            .get("unknown_capability")
            .is_some_and(|value| value == "true")
    }
}

pub fn extract_permission_intent(invocation: &ToolInvocation) -> PermissionIntent {
    extract_permission_intent_with_preflight(invocation).0
}

pub(crate) fn extract_permission_intent_with_preflight(
    invocation: &ToolInvocation,
) -> (PermissionIntent, Option<ApplyPatchPreflight>) {
    if invocation.tool_name == "apply_patch" {
        return apply_patch_intent_with_preflight(invocation);
    }
    let intent = extract_shell_permission_intent(invocation)
        .or_else(|| extract_network_permission_intent(invocation))
        .or_else(|| extract_mcp_permission_intent(invocation))
        .or_else(|| extract_dynamic_skill_permission_intent(invocation))
        .or_else(|| extract_computer_use_permission_intent(invocation))
        .or_else(|| extract_agent_action_permission_intent(invocation))
        .or_else(|| extract_memory_permission_intent(invocation))
        .or_else(|| extract_task_permission_intent(invocation))
        .or_else(|| extract_internal_permission_intent(invocation))
        .or_else(|| extract_file_permission_intent(invocation))
        .unwrap_or_else(|| PermissionIntent::generic_for_invocation(invocation));
    (intent, None)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionRequestScope {
    pub entries: BTreeMap<String, String>,
}

impl PermissionRequestScope {
    pub fn empty() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub fn from_pairs<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        Self {
            entries: pairs
                .into_iter()
                .map(|(key, value)| (normalize_scope_key(key), normalize_scope_value(value)))
                .collect(),
        }
    }

    pub fn normalized_hash(&self) -> String {
        let json = serde_json::to_vec(&self.entries).unwrap_or_default();
        let digest = Sha256::digest(json.as_slice());
        hex::encode(digest)
    }
}

fn extract_file_permission_intent(invocation: &ToolInvocation) -> Option<PermissionIntent> {
    match invocation.tool_name.as_str() {
        "read_file" => Some(file_path_intent(
            invocation,
            PermissionActionKind::FileRead,
            "read_file",
            "read file",
            "path",
        )),
        "list_dir" => Some(file_path_intent(
            invocation,
            PermissionActionKind::FileRead,
            "list_dir",
            "list directory",
            "path",
        )),
        "grep_files" => Some(grep_files_intent(invocation)),
        "apply_patch" => Some(apply_patch_intent(invocation)),
        _ => None,
    }
}

fn extract_shell_permission_intent(invocation: &ToolInvocation) -> Option<PermissionIntent> {
    match invocation.tool_name.as_str() {
        "exec_command" => Some(exec_command_intent(invocation)),
        "write_stdin" => Some(write_stdin_intent(invocation)),
        _ => None,
    }
}

fn extract_network_permission_intent(invocation: &ToolInvocation) -> Option<PermissionIntent> {
    match invocation.tool_name.as_str() {
        "web_search" => Some(web_search_intent(invocation)),
        "web_fetch" => Some(network_url_intent(invocation, "web fetch", "GET")),
        "download_url" => Some(network_url_intent(invocation, "download url", "GET")),
        _ => None,
    }
}

fn extract_mcp_permission_intent(invocation: &ToolInvocation) -> Option<PermissionIntent> {
    let ToolPayload::Mcp {
        server,
        tool,
        arguments,
        read_only_hint,
        destructive_hint,
        open_world_hint,
        ..
    } = &invocation.payload
    else {
        return None;
    };

    let classification = classify_mcp_tool_policy(McpToolSafetyHints {
        read_only_hint: *read_only_hint,
        destructive_hint: *destructive_hint,
        open_world_hint: *open_world_hint,
    });
    let mut scope = base_scope(invocation);
    scope
        .entries
        .insert("operation".to_owned(), "mcp tool".to_owned());
    scope.entries.insert("server".to_owned(), server.clone());
    scope.entries.insert("tool".to_owned(), tool.clone());
    scope
        .entries
        .insert("callable_name".to_owned(), invocation.tool_name.clone());
    bind_json_payload(&mut scope, arguments);
    if let Some(value) = read_only_hint {
        scope
            .entries
            .insert("read_only_hint".to_owned(), value.to_string());
    }
    if let Some(value) = destructive_hint {
        scope
            .entries
            .insert("destructive_hint".to_owned(), value.to_string());
    }
    if let Some(value) = open_world_hint {
        scope
            .entries
            .insert("open_world_hint".to_owned(), value.to_string());
    }
    scope.entries.insert(
        "mcp_side_effect_class".to_owned(),
        classification.side_effect_class.as_str().to_owned(),
    );
    scope.entries.insert(
        "mcp_requires_network".to_owned(),
        classification.requires_network.to_string(),
    );
    if classification.side_effect_class == pioneer_mcp::McpToolSideEffectClass::Unknown {
        scope = mark_unknown_capability(scope, "mcp side effects are not classified");
    }

    Some(PermissionIntent {
        action: match classification.permission_class {
            McpToolPermissionClass::Read => PermissionActionKind::McpRead,
            McpToolPermissionClass::WriteOrUnknown => PermissionActionKind::McpWriteOrUnknown,
        },
        scope,
        summary: Some(format!("MCP `{server}` tool `{tool}`")),
    })
}

fn extract_dynamic_skill_permission_intent(
    invocation: &ToolInvocation,
) -> Option<PermissionIntent> {
    let metadata = invocation.permission_metadata.dynamic_skill.as_ref()?;
    let mut scope = base_scope(invocation);
    scope
        .entries
        .insert("operation".to_owned(), "dynamic skill tool".to_owned());
    scope
        .entries
        .insert("skill_id".to_owned(), metadata.skill_id.to_string());
    if let Some(owner) = metadata.skill_owner.as_ref() {
        scope
            .entries
            .insert("skill_owner".to_owned(), owner.clone());
    }
    scope
        .entries
        .insert("skill_slug".to_owned(), metadata.skill_slug.clone());
    scope.entries.insert(
        "skill_fingerprint".to_owned(),
        metadata.skill_fingerprint.clone(),
    );
    scope
        .entries
        .insert("source_kind".to_owned(), metadata.source_kind.clone());
    scope
        .entries
        .insert("trust_level".to_owned(), metadata.trust_level.clone());
    scope.entries.insert(
        "dynamic_skill_kind".to_owned(),
        match metadata.kind {
            DynamicSkillPermissionKind::Http => "http",
            DynamicSkillPermissionKind::Shell => "shell",
            DynamicSkillPermissionKind::FunctionProxy => "function_proxy",
        }
        .to_owned(),
    );
    if let Some(target_tool) = metadata.target_tool.as_deref() {
        scope
            .entries
            .insert("target_tool".to_owned(), target_tool.to_owned());
    }
    if matches!(metadata.kind, DynamicSkillPermissionKind::FunctionProxy)
        && metadata
            .target_tool
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
    {
        scope = mark_unknown_capability(
            scope,
            "dynamic skill function proxy target tool is unavailable",
        );
    }

    match metadata.kind {
        DynamicSkillPermissionKind::Http => {
            let args = function_arguments(invocation);
            if let Some(arguments) = args {
                bind_json_payload(&mut scope, arguments);
            }
            let method = args
                .and_then(|arguments| string_field(arguments, "method"))
                .or(metadata.configured_method.as_deref())
                .map(|method| method.trim().to_ascii_uppercase())
                .filter(|method| !method.is_empty())
                .unwrap_or_else(|| "GET".to_owned());
            scope.entries.insert("method".to_owned(), method.clone());
            if let Some(raw_url) = args
                .and_then(|arguments| string_field(arguments, "url"))
                .or(metadata.configured_url.as_deref())
            {
                apply_url_scope(&mut scope, raw_url);
            } else {
                scope
                    .entries
                    .insert("parse_status".to_owned(), "missing_url".to_owned());
            }
            Some(PermissionIntent {
                action: PermissionActionKind::Network,
                scope,
                summary: Some(format!(
                    "dynamic skill HTTP request via `{}`",
                    invocation.tool_name
                )),
            })
        }
        DynamicSkillPermissionKind::Shell => {
            if let Some(command) = function_arguments(invocation).and_then(command_array_field) {
                scope
                    .entries
                    .insert("command".to_owned(), command.join(" "));
                scope.entries.insert(
                    "argv".to_owned(),
                    serde_json::to_string(&command).unwrap_or_else(|_| "[]".to_owned()),
                );
            }
            Some(PermissionIntent {
                // The wrapper has no side effect of its own. Its nested
                // exec_command receives the real ShellCommand decision and
                // sandbox grants, with this skill attached as provenance.
                action: PermissionActionKind::Internal,
                scope,
                summary: Some(format!(
                    "dynamic skill shell tool `{}`",
                    invocation.tool_name
                )),
            })
        }
        DynamicSkillPermissionKind::FunctionProxy => Some(PermissionIntent {
            // The target invocation is permission-gated once using its real
            // semantic action. The nested skill origin tightens that decision
            // through `dynamic_skill_tool` policy.
            action: if metadata
                .target_tool
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_some()
            {
                PermissionActionKind::Internal
            } else {
                PermissionActionKind::DynamicSkillTool
            },
            scope,
            summary: Some(format!(
                "dynamic skill function proxy `{}`",
                invocation.tool_name
            )),
        }),
    }
}

fn extract_computer_use_permission_intent(invocation: &ToolInvocation) -> Option<PermissionIntent> {
    if invocation.tool_name != "computer_use" {
        return None;
    }

    let arguments = function_arguments(invocation);
    let action = arguments
        .and_then(|arguments| string_field(arguments, "action"))
        .map(|action| action.trim().to_ascii_lowercase())
        .filter(|action| !action.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());

    let mut scope = base_scope(invocation);
    scope
        .entries
        .insert("operation".to_owned(), "computer use".to_owned());
    scope.entries.insert("action".to_owned(), action.clone());

    match arguments {
        Some(arguments) => {
            scope
                .entries
                .insert("parse_status".to_owned(), "arguments_present".to_owned());
            // Turn-scoped approvals are cached by the normalized scope hash.
            // Bind that cache to the complete model-visible operation so an
            // approval for one app, URL, path or input target cannot authorize
            // a different desktop effect whose abbreviated UI fields happen
            // to share the same action/session pair.
            if let Ok(encoded) = serde_json::to_vec(arguments) {
                scope
                    .entries
                    .insert("payload_hash".to_owned(), sha256_hex(encoded.as_slice()));
            }
            if let Some(session_id) = u64_field(arguments, "session_id") {
                scope
                    .entries
                    .insert("session_id".to_owned(), session_id.to_string());
            }
            if let Some(target_type) = nested_string_field(arguments, "target", "type") {
                scope
                    .entries
                    .insert("target_type".to_owned(), target_type.to_owned());
            }
            for (field, scope_key) in [
                ("name", "target_name"),
                ("identity_key", "target_identity_key"),
                ("bundle_id", "target_bundle_id"),
                ("executable_path", "target_executable_path"),
            ] {
                if let Some(value) = nested_string_field(arguments, "target", field) {
                    scope
                        .entries
                        .insert(scope_key.to_owned(), normalize_scope_value(value));
                }
            }
            if let Some(pid) = arguments
                .get("target")
                .and_then(JsonValue::as_object)
                .and_then(|target| target.get("pid"))
                .and_then(JsonValue::as_u64)
            {
                scope
                    .entries
                    .insert("target_pid".to_owned(), pid.to_string());
            }
            if let Some(display_id) = arguments
                .get("target")
                .and_then(JsonValue::as_object)
                .and_then(|target| target.get("display_id"))
                .and_then(JsonValue::as_u64)
                .or_else(|| u64_field(arguments, "display_id"))
            {
                scope
                    .entries
                    .insert("display_id".to_owned(), display_id.to_string());
            }
            if let Some(launch_if_missing) = arguments
                .get("target")
                .and_then(JsonValue::as_object)
                .and_then(|target| target.get("launch_if_missing"))
                .and_then(JsonValue::as_bool)
                .or_else(|| bool_field(arguments, "launch_if_missing"))
            {
                scope.entries.insert(
                    "launch_if_missing".to_owned(),
                    launch_if_missing.to_string(),
                );
            }
            if let Some(command) = nested_string_field(arguments, "target", "launch_command")
                .or_else(|| string_field(arguments, "launch_command"))
            {
                scope
                    .entries
                    .insert("launch_command".to_owned(), normalize_scope_value(command));
            }
            if let Some(screenshot_path) = string_field(arguments, "screenshot_path") {
                scope.entries.insert(
                    "screenshot_path".to_owned(),
                    normalize_scope_value(screenshot_path),
                );
            }
            if let Some(act_type) = nested_string_field(arguments, "act", "type") {
                scope
                    .entries
                    .insert("act_type".to_owned(), act_type.to_owned());
            }
            if let Some(act) = arguments.get("act").and_then(JsonValue::as_object) {
                for (field, scope_key) in [
                    ("app", "act_app"),
                    ("path", "act_path"),
                    ("action_name", "act_action_name"),
                    ("condition", "act_condition"),
                    ("title", "act_title"),
                ] {
                    if let Some(value) = act.get(field).and_then(JsonValue::as_str) {
                        scope
                            .entries
                            .insert(scope_key.to_owned(), normalize_scope_value(value));
                    }
                }
                if let Some(url) = act.get("url").and_then(JsonValue::as_str) {
                    apply_url_scope(&mut scope, url);
                }
                if let Some(keys) = act.get("keys").and_then(JsonValue::as_array) {
                    scope.entries.insert(
                        "act_keys".to_owned(),
                        serde_json::to_string(keys).unwrap_or_else(|_| "[]".to_owned()),
                    );
                }
                if let Some(text) = act.get("text").and_then(JsonValue::as_str) {
                    scope
                        .entries
                        .insert("input_text_present".to_owned(), "true".to_owned());
                    scope
                        .entries
                        .insert("input_text_bytes".to_owned(), text.len().to_string());
                }
                for (field, scope_key) in [
                    ("target", "act_target"),
                    ("from", "act_from"),
                    ("to", "act_to"),
                    ("menu_path", "act_menu_path"),
                ] {
                    if let Some(value) = act.get(field) {
                        scope.entries.insert(
                            scope_key.to_owned(),
                            serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned()),
                        );
                    }
                }
            }
        }
        None => {
            scope.entries.insert(
                "parse_status".to_owned(),
                "missing_function_arguments".to_owned(),
            );
        }
    }

    Some(PermissionIntent {
        action: PermissionActionKind::ComputerUse,
        scope,
        summary: Some(format!("computer_use {action}")),
    })
}

fn extract_agent_action_permission_intent(invocation: &ToolInvocation) -> Option<PermissionIntent> {
    let (operation, mutation) = match invocation.tool_name.as_str() {
        "agent_start_options" => ("read agent start options", false),
        "thread_message_send" => ("send message to thread", true),
        "thread_create" => ("create thread", true),
        "agent_start" => ("start agent", true),
        _ => return None,
    };

    let mut scope = base_scope(invocation);
    scope
        .entries
        .insert("domain".to_owned(), "agent".to_owned());
    scope
        .entries
        .insert("operation".to_owned(), operation.to_owned());
    if let Some(arguments) = function_arguments(invocation) {
        for key in ["targetOptionId", "optionId"] {
            if let Some(value) = string_field(arguments, key) {
                scope
                    .entries
                    .insert(normalize_scope_key(key), normalize_scope_value(value));
            }
        }
        if mutation {
            bind_json_payload(&mut scope, arguments);
        }
    }

    Some(PermissionIntent {
        action: if mutation {
            PermissionActionKind::AgentAction
        } else {
            PermissionActionKind::Internal
        },
        scope,
        summary: Some(operation.to_owned()),
    })
}

fn extract_memory_permission_intent(invocation: &ToolInvocation) -> Option<PermissionIntent> {
    let operation = match invocation.tool_name.as_str() {
        "memory_remember" => "remember memory",
        "memory_forget" => "forget memory",
        _ => return None,
    };
    let arguments = function_arguments(invocation);
    let dry_run = arguments
        .and_then(|arguments| {
            bool_field(arguments, "dryRun").or_else(|| bool_field(arguments, "dry_run"))
        })
        .unwrap_or(false);

    let mut scope = base_scope(invocation);
    scope
        .entries
        .insert("domain".to_owned(), "memory".to_owned());
    scope
        .entries
        .insert("operation".to_owned(), operation.to_owned());
    if let Some(arguments) = arguments {
        for key in ["scope", "category", "namespace", "key", "supersedes"] {
            if let Some(value) = string_field(arguments, key) {
                scope
                    .entries
                    .insert(key.to_owned(), normalize_scope_value(value));
            }
        }
        if let Some(memory_id) = string_field(arguments, "memoryId")
            .or_else(|| string_field(arguments, "memory_id"))
            .or_else(|| nested_string_field(arguments, "target", "memory_id"))
            .or_else(|| nested_string_field(arguments, "target", "memoryId"))
        {
            scope
                .entries
                .insert("memory_id".to_owned(), normalize_scope_value(memory_id));
        }
        if let Some(target_key) = nested_string_field(arguments, "target", "key") {
            scope
                .entries
                .insert("key".to_owned(), normalize_scope_value(target_key));
        }
        if dry_run {
            scope
                .entries
                .insert("dry_run".to_owned(), "true".to_owned());
        }
        if !dry_run {
            bind_json_payload(&mut scope, arguments);
        }
    }

    Some(PermissionIntent {
        action: if dry_run {
            PermissionActionKind::Internal
        } else {
            PermissionActionKind::MemoryWrite
        },
        scope,
        summary: Some(if dry_run {
            "preview memory forget".to_owned()
        } else {
            operation.to_owned()
        }),
    })
}

fn extract_internal_permission_intent(invocation: &ToolInvocation) -> Option<PermissionIntent> {
    let tool_name = invocation.tool_name.as_str();
    let operation = match invocation.tool_name.as_str() {
        "request_tools" => Some("request tools"),
        "read_skill" => Some("read skill"),
        tool_name if MEMORY_DOMAIN_TOOL_NAMES.contains(&tool_name) => Some("memory bookkeeping"),
        tool_name if ARTIFACT_DOMAIN_TOOL_NAMES.contains(&tool_name) => {
            Some("artifact bookkeeping")
        }
        _ => match &invocation.payload {
            ToolPayload::ToolSearch { .. } => Some("tool search"),
            _ => None,
        },
    }?;

    let mut scope = base_scope(invocation);
    scope
        .entries
        .insert("operation".to_owned(), operation.to_owned());
    if let ToolPayload::ToolSearch {
        limit,
        include_hidden,
        ..
    } = &invocation.payload
    {
        if let Some(limit) = limit {
            scope.entries.insert("limit".to_owned(), limit.to_string());
        }
        if let Some(include_hidden) = include_hidden {
            scope
                .entries
                .insert("include_hidden".to_owned(), include_hidden.to_string());
        }
    }
    if MEMORY_DOMAIN_TOOL_NAMES.contains(&tool_name) {
        scope
            .entries
            .insert("domain".to_owned(), "memory".to_owned());
    } else if ARTIFACT_DOMAIN_TOOL_NAMES.contains(&tool_name) {
        scope
            .entries
            .insert("domain".to_owned(), "artifact".to_owned());
    }

    Some(PermissionIntent {
        action: PermissionActionKind::Internal,
        scope,
        summary: Some(operation.to_owned()),
    })
}

fn extract_task_permission_intent(invocation: &ToolInvocation) -> Option<PermissionIntent> {
    let tool_name = invocation.tool_name.as_str();
    if !TASK_DOMAIN_TOOL_NAMES.contains(&tool_name) {
        return None;
    }

    let operation = match tool_name {
        "task_create" => "create task/subagent",
        "task_wait" => "wait for task/subagent",
        "task_result" => "read task/subagent result",
        "task_accept" => "accept task/subagent result",
        "task_revise" => "revise task/subagent result",
        "task_cancel" => "cancel task/subagent",
        "task_update" => "update task/subagent",
        "task_detach" => "detach task/subagent",
        "task_list" => "list task/subagent state",
        "task_get" => "get task/subagent state",
        "task_reschedule" => "reschedule task/subagent",
        "task_pause" => "pause task/subagent",
        "task_resume" => "resume task/subagent",
        _ => "task/subagent action",
    };
    let mut scope = base_scope(invocation);
    scope.entries.insert("domain".to_owned(), "task".to_owned());
    scope
        .entries
        .insert("operation".to_owned(), operation.to_owned());
    if let Some(arguments) = function_arguments(invocation) {
        if let Some(task_id) =
            string_field(arguments, "taskId").or_else(|| string_field(arguments, "task_id"))
        {
            scope
                .entries
                .insert("task_id".to_owned(), task_id.to_owned());
        }
        if matches!(
            tool_name,
            "task_create"
                | "task_accept"
                | "task_revise"
                | "task_cancel"
                | "task_update"
                | "task_detach"
                | "task_reschedule"
                | "task_pause"
                | "task_resume"
        ) {
            bind_json_payload(&mut scope, arguments);
        }
    }

    Some(PermissionIntent {
        action: match tool_name {
            // Observation is non-effectful. Every task mutation, including
            // lifecycle/review control, consumes the profile's explicit
            // task/subagent authority; otherwise `Internal` would bypass
            // supervised consent for durable task graph changes.
            "task_create" | "task_accept" | "task_revise" | "task_cancel" | "task_update"
            | "task_detach" | "task_reschedule" | "task_pause" | "task_resume" => {
                PermissionActionKind::TaskSubagent
            }
            _ => PermissionActionKind::Internal,
        },
        scope,
        summary: Some(operation.to_owned()),
    })
}

fn command_array_field(value: &JsonValue) -> Option<Vec<String>> {
    value
        .as_object()?
        .get("command")?
        .as_array()?
        .iter()
        .map(|value| value.as_str().map(str::to_owned))
        .collect()
}

fn web_search_intent(invocation: &ToolInvocation) -> PermissionIntent {
    let args = function_arguments(invocation);
    let query_present = args
        .and_then(|arguments| {
            string_field(arguments, "query").or_else(|| string_field(arguments, "q"))
        })
        .map(|query| !query.trim().is_empty())
        .unwrap_or(false);
    let mut scope = base_scope(invocation);
    scope
        .entries
        .insert("operation".to_owned(), "web search".to_owned());
    scope
        .entries
        .insert("method".to_owned(), "SEARCH".to_owned());
    scope
        .entries
        .insert("query_present".to_owned(), query_present.to_string());
    if let Some(arguments) = args {
        bind_json_payload(&mut scope, arguments);
    }
    let mut targets = invocation
        .permission_metadata
        .network_targets
        .iter()
        .filter_map(|target| Url::parse(target).ok())
        .filter(|target| matches!(target.scheme(), "http" | "https"))
        .filter_map(|target| {
            target.host_str().map(|host| {
                (
                    target.origin().ascii_serialization(),
                    host.to_ascii_lowercase(),
                )
            })
        })
        .collect::<Vec<_>>();
    if targets.is_empty() {
        // Runtime materialization replaces these defaults from the normalized
        // WebToolsConfig. Keep directly constructed builtin specs safe and
        // functional without falling back to unrestricted network access.
        targets.extend([
            (
                "https://duckduckgo.com".to_owned(),
                "duckduckgo.com".to_owned(),
            ),
            (
                "https://api.duckduckgo.com".to_owned(),
                "api.duckduckgo.com".to_owned(),
            ),
        ]);
    }
    targets.sort();
    targets.dedup();
    let origins = targets
        .iter()
        .map(|(origin, _)| origin.clone())
        .collect::<Vec<_>>();
    let mut hosts = targets
        .into_iter()
        .map(|(_, host)| host)
        .collect::<Vec<_>>();
    hosts.sort();
    hosts.dedup();
    scope.entries.insert(
        "network_origins".to_owned(),
        serde_json::to_string(&origins).unwrap_or_else(|_| "[]".to_owned()),
    );
    scope.entries.insert(
        "network_hosts".to_owned(),
        serde_json::to_string(&hosts).unwrap_or_else(|_| "[]".to_owned()),
    );

    PermissionIntent {
        action: PermissionActionKind::Network,
        scope,
        summary: Some("perform web search".to_owned()),
    }
}

fn network_url_intent(
    invocation: &ToolInvocation,
    operation: &'static str,
    default_method: &'static str,
) -> PermissionIntent {
    let args = function_arguments(invocation);
    let mut scope = base_scope(invocation);
    scope
        .entries
        .insert("operation".to_owned(), operation.to_owned());
    let method = args
        .and_then(|arguments| string_field(arguments, "method"))
        .map(|method| method.trim().to_ascii_uppercase())
        .filter(|method| !method.is_empty())
        .unwrap_or_else(|| default_method.to_owned());
    scope.entries.insert("method".to_owned(), method.clone());

    let Some(raw_url) = args.and_then(|arguments| string_field(arguments, "url")) else {
        scope
            .entries
            .insert("parse_status".to_owned(), "missing_url".to_owned());
        return PermissionIntent {
            action: PermissionActionKind::Network,
            scope,
            summary: Some(format!("{method} network request")),
        };
    };

    apply_url_scope(&mut scope, raw_url);
    if let Some(arguments) = args {
        bind_json_payload(&mut scope, arguments);
    }
    if invocation.tool_name == "download_url" {
        if let Ok(destination) = crate::handlers::resolve_download_destination(
            invocation.workdir.as_path(),
            args.and_then(|arguments| string_field(arguments, "destination")),
            raw_url,
        ) {
            scope.entries.insert(
                "destination".to_owned(),
                normalize_path_lexically(&destination).display().to_string(),
            );
        } else {
            scope
                .entries
                .insert("destination_status".to_owned(), "invalid".to_owned());
        }
        for (key, default) in [
            ("overwrite", false),
            ("create_dirs", true),
            ("follow_redirects", true),
        ] {
            scope.entries.insert(
                key.to_owned(),
                args.and_then(|arguments| bool_field(arguments, key))
                    .unwrap_or(default)
                    .to_string(),
            );
        }
        if let Some(max_bytes) = args.and_then(|arguments| u64_field(arguments, "max_bytes")) {
            scope
                .entries
                .insert("max_bytes".to_owned(), max_bytes.to_string());
        }
    }

    let target = scope
        .entries
        .get("url_origin")
        .or_else(|| scope.entries.get("domain"))
        .cloned()
        .unwrap_or_else(|| "unknown URL".to_owned());

    PermissionIntent {
        action: PermissionActionKind::Network,
        scope,
        summary: Some(format!("{method} {target}")),
    }
}

fn apply_url_scope(scope: &mut PermissionRequestScope, raw_url: &str) {
    scope.entries.insert(
        "url_full_hash".to_owned(),
        sha256_hex(raw_url.trim().as_bytes()),
    );
    let Ok(parsed) = Url::parse(raw_url) else {
        scope
            .entries
            .insert("parse_status".to_owned(), "invalid_url".to_owned());
        return;
    };

    scope
        .entries
        .insert("parse_status".to_owned(), "valid_url".to_owned());
    scope
        .entries
        .insert("scheme".to_owned(), parsed.scheme().to_owned());
    if let Some(domain) = parsed.host_str() {
        scope.entries.insert("domain".to_owned(), domain.to_owned());
        scope.entries.insert(
            "destination_hint".to_owned(),
            destination_hint(domain).to_owned(),
        );
    }
    let mut origin = format!(
        "{}://{}",
        parsed.scheme(),
        parsed.host_str().unwrap_or("unknown-host")
    );
    if let Some(port) = parsed.port() {
        origin.push(':');
        origin.push_str(port.to_string().as_str());
        scope.entries.insert("port".to_owned(), port.to_string());
    }
    scope.entries.insert("url_origin".to_owned(), origin);
    scope
        .entries
        .insert("url_path".to_owned(), parsed.path().to_owned());
}

fn destination_hint(host: &str) -> &'static str {
    let normalized = host.trim().trim_matches(['[', ']']).to_ascii_lowercase();
    if normalized == "localhost" || normalized.ends_with(".localhost") {
        return "private_or_local";
    }
    if let Ok(ip) = normalized.parse::<IpAddr>() {
        return if ip_is_private_or_local(ip) {
            "private_or_local"
        } else {
            "public_or_unknown"
        };
    }
    "public_or_unknown"
}

fn ip_is_private_or_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_loopback() || ip.is_private() || ip.is_link_local() || ip.is_unspecified()
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || ((ip.segments()[0] & 0xfe00) == 0xfc00)
                || ((ip.segments()[0] & 0xffc0) == 0xfe80)
        }
    }
}

fn exec_command_intent(invocation: &ToolInvocation) -> PermissionIntent {
    let mut scope = base_scope(invocation);
    scope
        .entries
        .insert("operation".to_owned(), "exec command".to_owned());

    let Some(args) = exec_command_args(invocation) else {
        scope.entries.insert(
            "parse_status".to_owned(),
            "invalid_exec_command_args".to_owned(),
        );
        return PermissionIntent {
            action: PermissionActionKind::ShellCommand,
            scope,
            summary: Some("run shell command".to_owned()),
        };
    };

    let argv = args.command.clone().unwrap_or_default();
    let command = if argv.is_empty() {
        "<missing command>".to_owned()
    } else {
        argv.join(" ")
    };
    let cwd = resolve_requested_path(
        invocation.workdir.as_path(),
        args.workdir.as_deref().unwrap_or("."),
    );
    let env_keys = invocation.environment.keys().cloned().collect::<Vec<_>>();

    scope.entries.insert(
        "parse_status".to_owned(),
        "valid_exec_command_args".to_owned(),
    );
    scope.entries.insert("command".to_owned(), command.clone());
    scope.entries.insert(
        "argv".to_owned(),
        serde_json::to_string(&argv).unwrap_or_else(|_| "[]".to_owned()),
    );
    scope.entries.insert("cwd".to_owned(), cwd);
    scope
        .entries
        .insert("tty".to_owned(), args.tty.unwrap_or(false).to_string());
    if let Some(timeout_ms) = args.timeout_ms {
        scope
            .entries
            .insert("timeout_ms".to_owned(), timeout_ms.to_string());
    }
    scope.entries.insert(
        "env_keys".to_owned(),
        serde_json::to_string(&env_keys).unwrap_or_else(|_| "[]".to_owned()),
    );

    PermissionIntent {
        action: PermissionActionKind::ShellCommand,
        scope,
        summary: Some(format!("run `{command}`")),
    }
}

fn exec_command_args(invocation: &ToolInvocation) -> Option<ExecCommandArgs> {
    match &invocation.payload {
        ToolPayload::LocalShell(crate::context::LocalShellPayload::ExecCommand(args)) => {
            Some(args.clone())
        }
        ToolPayload::Function { arguments } => serde_json::from_value(arguments.clone()).ok(),
        _ => None,
    }
}

fn write_stdin_intent(invocation: &ToolInvocation) -> PermissionIntent {
    let mut scope = base_scope(invocation);
    scope
        .entries
        .insert("operation".to_owned(), "write stdin".to_owned());

    let Some(args) = write_stdin_args(invocation) else {
        scope.entries.insert(
            "parse_status".to_owned(),
            "invalid_write_stdin_args".to_owned(),
        );
        return PermissionIntent {
            action: PermissionActionKind::ShellCommand,
            scope,
            summary: Some("write to shell session stdin".to_owned()),
        };
    };

    scope.entries.insert(
        "parse_status".to_owned(),
        "valid_write_stdin_args".to_owned(),
    );
    scope
        .entries
        .insert("session_id".to_owned(), args.session_id.to_string());
    scope.entries.insert(
        "stdin_chars_present".to_owned(),
        args.chars.is_some().to_string(),
    );
    if let Some(chars) = args.chars.as_deref() {
        scope
            .entries
            .insert("stdin_bytes".to_owned(), chars.len().to_string());
    }

    PermissionIntent {
        action: PermissionActionKind::ShellCommand,
        scope,
        summary: Some(format!("write stdin to shell session {}", args.session_id)),
    }
}

pub fn write_stdin_session_id(invocation: &ToolInvocation) -> Option<u64> {
    write_stdin_args(invocation).map(|args| args.session_id)
}

fn write_stdin_args(invocation: &ToolInvocation) -> Option<WriteStdinArgs> {
    match &invocation.payload {
        ToolPayload::LocalShell(crate::context::LocalShellPayload::WriteStdin(args)) => {
            Some(args.clone())
        }
        ToolPayload::Function { arguments } => serde_json::from_value(arguments.clone()).ok(),
        _ => None,
    }
}

fn file_path_intent(
    invocation: &ToolInvocation,
    action: PermissionActionKind,
    tool_name: &'static str,
    operation: &'static str,
    path_field: &'static str,
) -> PermissionIntent {
    let args = function_arguments(invocation);
    let raw_path = args
        .and_then(|arguments| string_field(arguments, path_field))
        .unwrap_or(".");
    let mut scope = base_scope(invocation);
    scope
        .entries
        .insert("operation".to_owned(), operation.to_owned());
    scope.entries.insert(
        "path".to_owned(),
        resolve_requested_path(permission_resolution_cwd(invocation).as_path(), raw_path),
    );

    PermissionIntent {
        action,
        scope,
        summary: Some(format!("{operation} via `{tool_name}`")),
    }
}

fn grep_files_intent(invocation: &ToolInvocation) -> PermissionIntent {
    let args = function_arguments(invocation);
    let raw_path = args
        .and_then(|arguments| string_field(arguments, "path"))
        .unwrap_or(".");
    let mut scope = base_scope(invocation);
    scope
        .entries
        .insert("operation".to_owned(), "grep files".to_owned());
    scope.entries.insert(
        "path".to_owned(),
        resolve_requested_path(permission_resolution_cwd(invocation).as_path(), raw_path),
    );
    if let Some(glob) = args.and_then(|arguments| string_field(arguments, "glob")) {
        scope.entries.insert("glob".to_owned(), glob.to_owned());
    }

    PermissionIntent {
        action: PermissionActionKind::FileRead,
        scope,
        summary: Some("grep files via `grep_files`".to_owned()),
    }
}

fn permission_resolution_cwd(invocation: &ToolInvocation) -> PathBuf {
    invocation
        .execution_security_snapshot
        .as_ref()
        .map(|snapshot| PathBuf::from(snapshot.sandbox.cwd.as_str()))
        .unwrap_or_else(|| invocation.workdir.clone())
}

fn apply_patch_intent(invocation: &ToolInvocation) -> PermissionIntent {
    apply_patch_intent_with_preflight(invocation).0
}

fn apply_patch_intent_with_preflight(
    invocation: &ToolInvocation,
) -> (PermissionIntent, Option<ApplyPatchPreflight>) {
    let mut scope = base_scope(invocation);
    scope
        .entries
        .insert("operation".to_owned(), "apply patch".to_owned());

    let Ok(patch) = extract_patch_input(&invocation.payload) else {
        scope
            .entries
            .insert("parse_status".to_owned(), "missing_patch_input".to_owned());
        return (
            PermissionIntent {
                action: PermissionActionKind::FileWrite,
                scope,
                summary: Some("apply patch with unreadable patch input".to_owned()),
            },
            Some(ApplyPatchPreflight::Rejected(
                ExecutionReport::rejected_patch_error(&PatchError::new(
                    PatchStage::Normalize,
                    PatchErrorCode::InvalidPayload,
                    "apply_patch expects exactly one patch string property",
                    Retryability::Never,
                )),
            )),
        );
    };

    let source = match &invocation.payload {
        ToolPayload::Function { .. } => PatchRequestSource::NativeFunction,
        _ => PatchRequestSource::NativeFreeform,
    };
    let request = match PatchRequest::from_provider_text(patch, source, PatchLimits::default()) {
        Ok(request) => request,
        Err(error) => {
            scope.entries.insert(
                "parse_status".to_owned(),
                "invalid_patch_document".to_owned(),
            );
            return (
                invalid_patch_permission_intent(scope),
                Some(ApplyPatchPreflight::Rejected(
                    ExecutionReport::rejected_patch_error(&error),
                )),
            );
        }
    };
    let parse_started = Instant::now();
    let document = match parse(&request, PatchLimits::default()) {
        Ok(document) => document,
        Err(error) => {
            patch_telemetry().record_stage_latency(TelemetryStage::Parse, parse_started.elapsed());
            scope.entries.insert(
                "parse_status".to_owned(),
                "invalid_patch_document".to_owned(),
            );
            return (
                invalid_patch_permission_intent(scope),
                Some(ApplyPatchPreflight::Rejected(
                    ExecutionReport::rejected_parse_error(&error),
                )),
            );
        }
    };
    let validated = match validate_guards(document) {
        Ok(validated) => validated,
        Err(error) => {
            patch_telemetry().record_stage_latency(TelemetryStage::Parse, parse_started.elapsed());
            scope.entries.insert(
                "parse_status".to_owned(),
                "invalid_patch_document".to_owned(),
            );
            return (
                invalid_patch_permission_intent(scope),
                Some(ApplyPatchPreflight::Rejected(
                    ExecutionReport::rejected_guard_error(&error),
                )),
            );
        }
    };
    patch_telemetry().record_stage_latency(TelemetryStage::Parse, parse_started.elapsed());

    let validated_for_consent = validated.clone();
    let (validated, patch_root) = match authorize_and_normalize_patch_paths(invocation, validated) {
        Ok(result) => result,
        Err(error) => {
            if let Ok(validated) = authorize_patch_paths_with_maximum_consent_authority(
                invocation,
                validated_for_consent,
            ) {
                return deferred_apply_patch_intent(scope, &validated);
            }
            scope.entries.insert(
                "parse_status".to_owned(),
                "unauthorized_target_manifest".to_owned(),
            );
            return (
                invalid_patch_permission_intent(scope),
                Some(ApplyPatchPreflight::Rejected(
                    ExecutionReport::rejected_patch_error(&error),
                )),
            );
        }
    };

    let resolver = match TargetResolver::new(&patch_root) {
        Ok(resolver) => resolver.with_absolute_paths(true),
        Err(_) => {
            scope.entries.insert(
                "parse_status".to_owned(),
                "invalid_target_manifest".to_owned(),
            );
            return (
                invalid_patch_permission_intent(scope),
                Some(ApplyPatchPreflight::Rejected(
                    ExecutionReport::rejected_patch_error(&PatchError::new(
                        PatchStage::Resolve,
                        PatchErrorCode::InvalidPath,
                        format!(
                            "the patch execution root `{}` is invalid; use a path under the current working directory or another authorized root",
                            patch_root.display()
                        ),
                        Retryability::Never,
                    )),
                )),
            );
        }
    };
    let resolved = match resolve_patch(&validated, &resolver, PrepareOptions::default()) {
        Ok(resolved) => resolved,
        Err(error) => {
            scope.entries.insert(
                "parse_status".to_owned(),
                "invalid_target_manifest".to_owned(),
            );
            return (
                invalid_patch_permission_intent(scope),
                Some(ApplyPatchPreflight::Rejected(
                    ExecutionReport::rejected_resolve_error(&error),
                )),
            );
        }
    };

    let mut resolved_paths = resolved
        .target_manifest()
        .targets()
        .iter()
        .filter(|target| target.role != TargetRole::Parent)
        .map(|target| target.absolute().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let mut operations = BTreeMap::<&'static str, ()>::new();
    for operation in &resolved.document().operations {
        operations.insert(
            match operation.kind() {
                OperationKind::Add => "add",
                OperationKind::Replace => "replace",
                OperationKind::Update => "update",
                OperationKind::Delete => "delete",
            },
            (),
        );
        if operation.operation.move_to.is_some() {
            operations.insert("move", ());
        }
    }
    resolved_paths.sort();
    resolved_paths.dedup();
    let operations = operations
        .into_keys()
        .map(str::to_owned)
        .collect::<Vec<_>>();

    scope
        .entries
        .insert("parse_status".to_owned(), "valid_patch_document".to_owned());
    scope.entries.insert(
        "changed_path_count".to_owned(),
        resolved_paths.len().to_string(),
    );
    scope.entries.insert(
        "changed_paths".to_owned(),
        serde_json::to_string(&resolved_paths).unwrap_or_else(|_| "[]".to_owned()),
    );
    scope.entries.insert(
        "parser_schema_version".to_owned(),
        resolved.document().schema_version.to_string(),
    );
    scope.entries.insert(
        "payload_hash".to_owned(),
        hex::encode(resolved.document().payload_hash),
    );
    scope.entries.insert(
        "target_manifest".to_owned(),
        serde_json::to_string(
            &resolved
                .target_manifest()
                .targets()
                .iter()
                .map(|target| {
                    serde_json::json!({
                        "path": target.absolute().to_string_lossy(),
                        "role": target.role,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|_| "[]".to_owned()),
    );
    for (index, path) in resolved_paths.iter().take(20).enumerate() {
        scope.entries.insert(format!("path.{index}"), path.clone());
    }
    if !operations.is_empty() {
        scope.entries.insert(
            "operations".to_owned(),
            serde_json::to_string(&operations).unwrap_or_else(|_| "[]".to_owned()),
        );
    }

    (
        PermissionIntent {
            action: PermissionActionKind::FileWrite,
            scope,
            summary: Some(format!(
                "apply patch touching {} path(s)",
                resolved_paths.len()
            )),
        },
        Some(ApplyPatchPreflight::Ready(resolved)),
    )
}

fn authorize_patch_paths_with_maximum_consent_authority(
    invocation: &ToolInvocation,
    document: ValidatedPatchDocument,
) -> Result<ValidatedPatchDocument, PatchError> {
    let Some(snapshot) = invocation.execution_security_snapshot.as_ref() else {
        return authorize_and_normalize_patch_paths(invocation, document)
            .map(|(document, _)| document);
    };
    let mut authority_invocation = invocation.clone();
    let authority_snapshot = authority_invocation
        .execution_security_snapshot
        .as_mut()
        .expect("snapshot cloned from present invocation");
    authority_snapshot.sandbox.filesystem = snapshot.authority_cap.filesystem.clone();
    authorize_and_normalize_patch_paths(&authority_invocation, document)
        .map(|(document, _)| document)
}

fn deferred_apply_patch_intent(
    mut scope: PermissionRequestScope,
    document: &ValidatedPatchDocument,
) -> (PermissionIntent, Option<ApplyPatchPreflight>) {
    let mut resolved_paths = document
        .operations
        .iter()
        .flat_map(|operation| {
            std::iter::once(operation.operation.path.clone())
                .chain(operation.operation.move_to.iter().cloned())
        })
        .collect::<Vec<_>>();
    resolved_paths.sort();
    resolved_paths.dedup();

    let mut operations = BTreeMap::<&'static str, ()>::new();
    for operation in &document.operations {
        operations.insert(
            match operation.kind() {
                OperationKind::Add => "add",
                OperationKind::Replace => "replace",
                OperationKind::Update => "update",
                OperationKind::Delete => "delete",
            },
            (),
        );
        if operation.operation.move_to.is_some() {
            operations.insert("move", ());
        }
    }

    scope.entries.insert(
        "parse_status".to_owned(),
        "awaiting_filesystem_grant".to_owned(),
    );
    scope.entries.insert(
        "changed_path_count".to_owned(),
        resolved_paths.len().to_string(),
    );
    scope.entries.insert(
        "changed_paths".to_owned(),
        serde_json::to_string(&resolved_paths).unwrap_or_else(|_| "[]".to_owned()),
    );
    scope.entries.insert(
        "parser_schema_version".to_owned(),
        document.schema_version.to_string(),
    );
    scope.entries.insert(
        "payload_hash".to_owned(),
        hex::encode(document.payload_hash),
    );
    for (index, path) in resolved_paths.iter().take(20).enumerate() {
        scope.entries.insert(format!("path.{index}"), path.clone());
    }
    if !operations.is_empty() {
        scope.entries.insert(
            "operations".to_owned(),
            serde_json::to_string(&operations.into_keys().collect::<Vec<_>>())
                .unwrap_or_else(|_| "[]".to_owned()),
        );
    }

    (
        PermissionIntent {
            action: PermissionActionKind::FileWrite,
            scope,
            summary: Some(format!(
                "apply patch touching {} path(s)",
                resolved_paths.len()
            )),
        },
        None,
    )
}

fn invalid_patch_permission_intent(scope: PermissionRequestScope) -> PermissionIntent {
    PermissionIntent {
        action: PermissionActionKind::FileWrite,
        scope,
        summary: Some("apply malformed patch".to_owned()),
    }
}

#[derive(Debug)]
struct AuthorizedPatchPath {
    absolute: PathBuf,
    matched_root: Option<PathBuf>,
}

/// Resolve every model path through the immutable turn filesystem policy
/// before the patch engine sees it. Relative inputs use the Composer-selected
/// cwd; absolute inputs may target any writable root authorized for the turn.
/// The engine still operates below one technical root. For a restricted turn
/// that root is derived from the complete Composer-authorized writable set, so
/// it remains stable across every patch in the same security snapshot.
fn authorize_and_normalize_patch_paths(
    invocation: &ToolInvocation,
    mut document: ValidatedPatchDocument,
) -> Result<(ValidatedPatchDocument, PathBuf), PatchError> {
    let Some(snapshot) = invocation.execution_security_snapshot.as_ref() else {
        // Permission classification is also used before a runtime snapshot is
        // attached. Build a bounded provisional manifest from workdir so the
        // action remains FileWrite; the handler still fails closed if the
        // authoritative snapshot is absent at execution time.
        let cwd = std::fs::canonicalize(invocation.workdir.as_path())
            .unwrap_or_else(|_| normalize_path_lexically(invocation.workdir.as_path()));
        for (operation_index, operation) in document.operations.iter_mut().enumerate() {
            operation.operation.path = provisional_patch_path(
                cwd.as_path(),
                operation.operation.path.as_str(),
                operation_index,
            )?
            .to_string_lossy()
            .into_owned();
            if let Some(destination) = operation.operation.move_to.as_mut() {
                *destination =
                    provisional_patch_path(cwd.as_path(), destination.as_str(), operation_index)?
                        .to_string_lossy()
                        .into_owned();
            }
        }
        return Ok((document, cwd));
    };

    let mut authorized_paths = Vec::new();
    for (operation_index, operation) in document.operations.iter_mut().enumerate() {
        let source =
            authorize_patch_path(snapshot, operation.operation.path.as_str(), operation_index)?;
        operation.operation.path = source.absolute.to_string_lossy().into_owned();
        authorized_paths.push(source);

        if let Some(destination) = operation.operation.move_to.as_mut() {
            let authorized = authorize_patch_path(snapshot, destination.as_str(), operation_index)?;
            *destination = authorized.absolute.to_string_lossy().into_owned();
            authorized_paths.push(authorized);
        }
    }

    let root = select_patch_execution_root(snapshot, authorized_paths.as_slice()).ok_or_else(|| {
        PatchError::new(
            PatchStage::Resolve,
            PatchErrorCode::InvalidPath,
            "apply_patch could not select a common existing directory for the authorized targets; split targets on different volumes into separate patches",
            Retryability::Never,
        )
    })?;
    Ok((document, root))
}

fn provisional_patch_path(
    cwd: &Path,
    input: &str,
    operation_index: usize,
) -> Result<PathBuf, PatchError> {
    let input_path = Path::new(input);
    let joined = if input_path.is_absolute() {
        input_path.to_path_buf()
    } else {
        cwd.join(input_path)
    };
    let absolute = normalize_path_lexically(joined.as_path());
    if !absolute.starts_with(cwd) {
        let mut error = PatchError::new(
            PatchStage::Resolve,
            PatchErrorCode::PathOutsideAllowedRoot,
            format!(
                "relative patch path {input} resolves outside provisional working directory {}",
                cwd.display()
            ),
            Retryability::Never,
        );
        error.diagnostic.operation_index = u32::try_from(operation_index).ok();
        error.diagnostic.path = Some(absolute.to_string_lossy().into_owned());
        return Err(error);
    }
    Ok(absolute)
}

fn authorize_patch_path(
    snapshot: &pioneer_protocol::TurnExecutionSecuritySnapshot,
    input: &str,
    operation_index: usize,
) -> Result<AuthorizedPatchPath, PatchError> {
    match FilePolicyChecker::check_write(snapshot, Path::new(input)) {
        FilePolicyDecision::Allowed(grant) => Ok(AuthorizedPatchPath {
            absolute: grant.resolved_path,
            matched_root: grant.matched_root,
        }),
        FilePolicyDecision::Denied(deny) => {
            let resolved = deny
                .resolved_path
                .as_deref()
                .unwrap_or(deny.requested_path.as_path());
            let roots = FilePolicyChecker::allowed_roots(snapshot, FilePolicyOperation::Write)
                .into_iter()
                .map(|root| format!("`{}`", root.display()))
                .collect::<Vec<_>>();
            let code = match deny.reason {
                FilePolicyDenyReason::EmptyPath | FilePolicyDenyReason::MissingPath => {
                    PatchErrorCode::InvalidPath
                }
                FilePolicyDenyReason::OutsideAllowedRoots => PatchErrorCode::PathOutsideAllowedRoot,
                FilePolicyDenyReason::SymlinkEscape
                | FilePolicyDenyReason::WriteRequiresWritableRoot
                | FilePolicyDenyReason::NoUsableRoots
                | FilePolicyDenyReason::InvalidRoot => PatchErrorCode::PermissionDenied,
            };
            let mut error = PatchError::new(
                PatchStage::Authorize,
                code,
                format!(
                    "patch path `{input}` resolves to `{}` but is not writable: {}. Current working directory: `{}`. Writable roots: {}. Use a relative path from the current working directory or an absolute path under one of those roots",
                    resolved.display(),
                    deny.message,
                    snapshot.sandbox.cwd,
                    if roots.is_empty() {
                        "none".to_owned()
                    } else {
                        roots.join(", ")
                    }
                ),
                Retryability::Never,
            );
            error.diagnostic.operation_index = u32::try_from(operation_index).ok();
            error.diagnostic.path = Some(resolved.to_string_lossy().into_owned());
            Err(error)
        }
    }
}

fn select_patch_execution_root(
    snapshot: &pioneer_protocol::TurnExecutionSecuritySnapshot,
    paths: &[AuthorizedPatchPath],
) -> Option<PathBuf> {
    let cwd = std::fs::canonicalize(snapshot.sandbox.cwd.as_str())
        .unwrap_or_else(|_| normalize_path_lexically(Path::new(&snapshot.sandbox.cwd)));
    let candidates = if snapshot.sandbox.filesystem.kind
        == pioneer_protocol::TurnFilesystemSandboxKind::Unrestricted
    {
        // An unrestricted policy has no finite root list. Keep the namespace
        // stable per filesystem volume instead of narrowing it to whichever
        // directory happened to be touched by this patch.
        let mut anchors = paths
            .iter()
            .filter_map(|path| filesystem_anchor(path.absolute.as_path()))
            .collect::<Vec<_>>();
        if anchors.is_empty()
            && let Some(anchor) = filesystem_anchor(cwd.as_path())
        {
            anchors.push(anchor);
        }
        anchors
    } else {
        let authorized_roots =
            FilePolicyChecker::allowed_roots(snapshot, FilePolicyOperation::Write);
        if authorized_roots.is_empty() {
            // Defensive fallback: authorization normally cannot have produced
            // `paths` without at least one usable root.
            paths
                .iter()
                .filter_map(|path| {
                    path.matched_root
                        .clone()
                        .or_else(|| path.absolute.parent().map(Path::to_path_buf))
                })
                .collect()
        } else {
            authorized_roots
        }
    };
    common_ancestor(candidates.as_slice()).and_then(existing_directory)
}

fn filesystem_anchor(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    let mut anchor = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => anchor.push(prefix.as_os_str()),
            Component::RootDir => anchor.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => return None,
            Component::Normal(_) => break,
        }
    }
    (!anchor.as_os_str().is_empty()).then_some(anchor)
}

fn common_ancestor(paths: &[PathBuf]) -> Option<PathBuf> {
    let mut common = paths.first()?.clone();
    while !paths.iter().all(|path| path.starts_with(common.as_path())) {
        if !common.pop() {
            return None;
        }
    }
    Some(common)
}

fn existing_directory(mut path: PathBuf) -> Option<PathBuf> {
    loop {
        match std::fs::canonicalize(path.as_path()) {
            Ok(canonical) if canonical.is_dir() => return Some(canonical),
            _ if path.pop() => {}
            _ => return None,
        }
    }
}

fn base_scope(invocation: &ToolInvocation) -> PermissionRequestScope {
    let mut scope = PermissionRequestScope::from_pairs([
        ("tool_name", invocation.tool_name.as_str()),
        ("source", invocation.source.as_str()),
    ]);
    for (index, origin) in invocation
        .permission_metadata
        .nested_dynamic_skills
        .iter()
        .enumerate()
    {
        let prefix = format!("skill_origin_{index}");
        scope
            .entries
            .insert(format!("{prefix}_skill_id"), origin.skill_id.to_string());
        scope
            .entries
            .insert(format!("{prefix}_skill_slug"), origin.skill_slug.clone());
        scope.entries.insert(
            format!("{prefix}_skill_fingerprint"),
            origin.skill_fingerprint.clone(),
        );
        scope
            .entries
            .insert(format!("{prefix}_trust_level"), origin.trust_level.clone());
        if let Some(target) = origin.target_tool.as_ref() {
            scope
                .entries
                .insert(format!("{prefix}_declared_target"), target.clone());
        }
    }
    scope
}

fn function_arguments(invocation: &ToolInvocation) -> Option<&JsonValue> {
    match &invocation.payload {
        ToolPayload::Function { arguments } => Some(arguments),
        _ => None,
    }
}

fn string_field<'a>(value: &'a JsonValue, key: &str) -> Option<&'a str> {
    value.as_object()?.get(key)?.as_str()
}

fn u64_field(value: &JsonValue, key: &str) -> Option<u64> {
    value.as_object()?.get(key)?.as_u64()
}

fn bool_field(value: &JsonValue, key: &str) -> Option<bool> {
    value.as_object()?.get(key)?.as_bool()
}

fn nested_string_field<'a>(
    value: &'a JsonValue,
    object_key: &str,
    nested_key: &str,
) -> Option<&'a str> {
    value
        .as_object()?
        .get(object_key)?
        .as_object()?
        .get(nested_key)?
        .as_str()
}

fn resolve_requested_path(workdir: &Path, requested_path: &str) -> String {
    let requested = Path::new(requested_path);
    let joined = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        workdir.join(requested)
    };
    normalize_path_lexically(joined.as_path())
        .display()
        .to_string()
}

fn normalize_path_lexically(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !matches!(
                    normalized.components().next_back(),
                    Some(Component::RootDir | Component::Prefix(_))
                ) {
                    normalized.pop();
                }
            }
            Component::RootDir | Component::Prefix(_) | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        normalized
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PermissionRequestKey {
    pub profile_mode: pioneer_protocol::TurnPermissionMode,
    pub tool_name: String,
    pub action: PermissionActionKind,
    pub normalized_scope_hash: String,
    pub turn_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum PermissionDecision {
    Allow {
        reason: PermissionDecisionReason,
    },
    Ask {
        key: PermissionRequestKey,
        reason: PermissionDecisionReason,
    },
    Deny {
        reason: PermissionDecisionReason,
        message: String,
    },
}

pub trait ToolPermissionEvaluator: Send + Sync {
    fn evaluate(
        &self,
        context: &PermissionEvaluationContext,
        invocation: &ToolInvocation,
        intent: &PermissionIntent,
    ) -> PermissionDecision;
}

#[derive(Debug, Default)]
pub struct ProfileToolPermissionEvaluator;

impl ToolPermissionEvaluator for ProfileToolPermissionEvaluator {
    fn evaluate(
        &self,
        context: &PermissionEvaluationContext,
        invocation: &ToolInvocation,
        intent: &PermissionIntent,
    ) -> PermissionDecision {
        if let Some(message) = policy_denies_tool_name(
            &context.permission_profile.effective_policy,
            invocation.tool_name.as_str(),
        ) {
            return PermissionDecision::Deny {
                reason: PermissionDecisionReason::PolicyDeniesAction,
                message,
            };
        }
        if let Some(message) =
            policy_denies_intent_paths(&context.permission_profile.effective_policy, intent)
        {
            return PermissionDecision::Deny {
                reason: PermissionDecisionReason::PolicyDeniesAction,
                message,
            };
        }
        let policy = &context.permission_profile.effective_policy;
        let mut behavior = behavior_for_action(policy, intent.action);
        let defers_to_nested_target = intent.action == PermissionActionKind::Internal
            && invocation
                .permission_metadata
                .dynamic_skill
                .as_ref()
                .is_some_and(|metadata| {
                    matches!(
                        metadata.kind,
                        DynamicSkillPermissionKind::Shell
                            | DynamicSkillPermissionKind::FunctionProxy
                    )
                });
        if !invocation
            .permission_metadata
            .nested_dynamic_skills
            .is_empty()
            && !defers_to_nested_target
        {
            behavior = most_restrictive_behavior(behavior, policy.dynamic_skill_tool);
        }
        decision_from_behavior(behavior, context, invocation, intent)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum PermissionApprovalResolution {
    AllowOnce,
    AllowForTurn,
    Deny { message: String },
    Cancelled,
    Expired,
}

#[async_trait]
pub trait PermissionApprovalBroker: Send + Sync {
    /// Reprojects the immutable turn permission profile through the current
    /// server-owned authorization policy immediately before a tool side
    /// effect. Local/static brokers keep the admitted profile unchanged;
    /// Gateway brokers may only return an equal or narrower profile.
    async fn revalidate_permission_context(
        &self,
        context: &PermissionEvaluationContext,
        _invocation: &ToolInvocation,
    ) -> Result<PermissionEvaluationContext, String> {
        Ok(context.clone())
    }

    async fn request_approval(
        &self,
        context: &PermissionEvaluationContext,
        invocation: &ToolInvocation,
        intent: &PermissionIntent,
        key: &PermissionRequestKey,
        reason: PermissionDecisionReason,
    ) -> PermissionApprovalResolution;
}

#[derive(Debug, Clone)]
pub struct StaticPermissionApprovalBroker {
    resolution: PermissionApprovalResolution,
}

impl StaticPermissionApprovalBroker {
    pub fn new(resolution: PermissionApprovalResolution) -> Self {
        Self { resolution }
    }

    pub fn allow_once() -> Self {
        Self::new(PermissionApprovalResolution::AllowOnce)
    }

    pub fn allow_for_turn() -> Self {
        Self::new(PermissionApprovalResolution::AllowForTurn)
    }

    pub fn deny(message: impl Into<String>) -> Self {
        Self::new(PermissionApprovalResolution::Deny {
            message: message.into(),
        })
    }
}

impl Default for StaticPermissionApprovalBroker {
    fn default() -> Self {
        Self::deny("permission approval broker is not available")
    }
}

#[async_trait]
impl PermissionApprovalBroker for StaticPermissionApprovalBroker {
    async fn request_approval(
        &self,
        _context: &PermissionEvaluationContext,
        _invocation: &ToolInvocation,
        _intent: &PermissionIntent,
        _key: &PermissionRequestKey,
        _reason: PermissionDecisionReason,
    ) -> PermissionApprovalResolution {
        self.resolution.clone()
    }
}

fn behavior_for_action(
    policy: &ToolPermissionPolicySnapshot,
    action: PermissionActionKind,
) -> PermissionBehavior {
    match action {
        PermissionActionKind::FileRead => policy.file_read,
        PermissionActionKind::FileWrite => policy.file_write,
        PermissionActionKind::ShellCommand => policy.shell_command,
        PermissionActionKind::Network => policy.network,
        PermissionActionKind::McpRead => policy.mcp_read,
        PermissionActionKind::McpWriteOrUnknown => policy.mcp_write_or_unknown,
        PermissionActionKind::DynamicSkillTool => policy.dynamic_skill_tool,
        PermissionActionKind::ComputerUse => policy.computer_use,
        PermissionActionKind::TaskSubagent => policy.task_subagent,
        PermissionActionKind::MemoryWrite => policy.memory_write,
        PermissionActionKind::AgentAction => policy.agent_action,
        PermissionActionKind::Internal => PermissionBehavior::Allow,
        PermissionActionKind::Unknown => policy.default_behavior,
    }
}

fn most_restrictive_behavior(
    left: PermissionBehavior,
    right: PermissionBehavior,
) -> PermissionBehavior {
    match (left, right) {
        (PermissionBehavior::Deny, _) | (_, PermissionBehavior::Deny) => PermissionBehavior::Deny,
        (PermissionBehavior::Ask, _) | (_, PermissionBehavior::Ask) => PermissionBehavior::Ask,
        (PermissionBehavior::Allow, PermissionBehavior::Allow) => PermissionBehavior::Allow,
    }
}

fn decision_from_behavior(
    behavior: PermissionBehavior,
    context: &PermissionEvaluationContext,
    invocation: &ToolInvocation,
    intent: &PermissionIntent,
) -> PermissionDecision {
    let unknown_capability = intent.is_unknown_capability();
    match behavior {
        PermissionBehavior::Allow => PermissionDecision::Allow {
            reason: PermissionDecisionReason::PolicyAllowsAction,
        },
        PermissionBehavior::Ask => PermissionDecision::Ask {
            key: intent.request_key(context, invocation),
            reason: if unknown_capability {
                PermissionDecisionReason::UnknownActionDefault
            } else {
                PermissionDecisionReason::PolicyRequiresApproval
            },
        },
        PermissionBehavior::Deny => PermissionDecision::Deny {
            reason: if unknown_capability {
                PermissionDecisionReason::UnknownActionDefault
            } else {
                PermissionDecisionReason::PolicyDeniesAction
            },
            message: if unknown_capability {
                "unknown tool capability denied by turn permission profile".to_owned()
            } else {
                "tool action denied by turn permission profile".to_owned()
            },
        },
    }
}

fn mark_unknown_capability(
    mut scope: PermissionRequestScope,
    reason: impl Into<String>,
) -> PermissionRequestScope {
    scope
        .entries
        .insert("unknown_capability".to_owned(), "true".to_owned());
    scope
        .entries
        .insert("unknown_reason".to_owned(), reason.into());
    scope
}

fn policy_denies_tool_name(
    policy: &ToolPermissionPolicySnapshot,
    tool_name: &str,
) -> Option<String> {
    let normalized_tool_name = normalize_policy_value(tool_name);
    if policy
        .denied_tools
        .iter()
        .map(|value| normalize_policy_value(value))
        .any(|value| value == normalized_tool_name)
    {
        return Some(format!(
            "tool `{tool_name}` denied by turn permission profile"
        ));
    }
    if (policy.allowed_tools_restricted || !policy.allowed_tools.is_empty())
        && !policy
            .allowed_tools
            .iter()
            .map(|value| normalize_policy_value(value))
            .any(|value| value == normalized_tool_name)
    {
        return Some(format!(
            "tool `{tool_name}` is not allowed by turn permission profile"
        ));
    }
    None
}

fn policy_denies_intent_paths(
    policy: &ToolPermissionPolicySnapshot,
    intent: &PermissionIntent,
) -> Option<String> {
    if (!policy.allowed_paths_restricted && policy.allowed_paths.is_empty())
        || !matches!(
            intent.action,
            PermissionActionKind::FileRead | PermissionActionKind::FileWrite
        )
    {
        return None;
    }
    let requested_paths = intent_paths(intent);
    if requested_paths.is_empty() {
        return Some("file action denied because path scope is unavailable".to_owned());
    }
    let allowed_paths = policy
        .allowed_paths
        .iter()
        .map(|value| normalize_policy_path(value))
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if allowed_paths.is_empty() {
        return Some("file action denied because the allowed path set is empty".to_owned());
    }
    let all_paths_allowed = requested_paths.iter().all(|path| {
        let normalized = normalize_policy_path(path);
        allowed_paths
            .iter()
            .any(|allowed| path_is_within_allowed_scope(normalized.as_str(), allowed.as_str()))
    });
    if all_paths_allowed {
        None
    } else {
        Some("file path denied by turn permission profile".to_owned())
    }
}

fn intent_paths(intent: &PermissionIntent) -> Vec<String> {
    let mut paths = Vec::new();
    if let Some(path) = intent.scope.entries.get("path") {
        paths.push(path.clone());
    }
    for (key, value) in &intent.scope.entries {
        if key.starts_with("path.") {
            paths.push(value.clone());
        }
    }
    if let Some(changed_paths) = intent.scope.entries.get("changed_paths")
        && let Ok(decoded) = serde_json::from_str::<Vec<String>>(changed_paths)
    {
        paths.extend(decoded);
    }
    paths.sort();
    paths.dedup();
    paths
}

fn normalize_policy_value(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn normalize_policy_path(value: &str) -> String {
    normalize_path_lexically(Path::new(value.trim()))
        .to_string_lossy()
        .into_owned()
}

fn path_is_within_allowed_scope(path: &str, allowed: &str) -> bool {
    let path = Path::new(path);
    let allowed = Path::new(allowed);
    path == allowed || path.starts_with(allowed)
}

fn normalize_scope_key(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn normalize_scope_value(value: &str) -> String {
    value.trim().to_owned()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn bind_json_payload(scope: &mut PermissionRequestScope, payload: &JsonValue) {
    if let Ok(encoded) = serde_json::to_vec(payload) {
        scope
            .entries
            .insert("payload_hash".to_owned(), sha256_hex(encoded.as_slice()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{ToolCallSource, ToolPayload};
    use crate::spec::{
        DynamicSkillPermissionKind, DynamicSkillPermissionMetadata, ToolPermissionMetadata,
        ToolRecoveryMetadata,
    };
    use std::collections::BTreeSet;
    use std::path::PathBuf;
    use tokio_util::sync::CancellationToken;

    fn test_context(mode: pioneer_protocol::TurnPermissionMode) -> PermissionEvaluationContext {
        PermissionEvaluationContext::for_turn(
            "workspace_test",
            "thread_test",
            "turn_test",
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                mode,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            ),
        )
    }

    fn full_access_context() -> PermissionEvaluationContext {
        PermissionEvaluationContext::for_turn(
            "workspace_test",
            "thread_test",
            "turn_test",
            pioneer_protocol::default_turn_permission_profile_snapshot(),
        )
    }

    fn invocation() -> ToolInvocation {
        invocation_for_tool(
            "read_file",
            ToolPayload::Function {
                arguments: serde_json::json!({ "path": "src/lib.rs" }),
            },
        )
    }

    fn invocation_for_tool(tool_name: &str, payload: ToolPayload) -> ToolInvocation {
        ToolInvocation {
            call_id: "call_permission".to_owned(),
            tool_name: tool_name.to_owned(),
            source: ToolCallSource::Model,
            payload,
            workdir: PathBuf::from("/workspace"),
            environment: BTreeMap::new(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: ToolRecoveryMetadata::default(),
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            execution_security_snapshot: None,
            apply_patch_preflight: None,
            cancellation: CancellationToken::new(),
        }
    }

    #[test]
    fn permission_semantics_cover_every_registered_native_tool() {
        let mut registered = crate::builtin_tool_specs()
            .into_iter()
            .map(|configured| configured.spec.name)
            .collect::<BTreeSet<_>>();
        for domain in crate::BuiltinToolDomain::ALL {
            registered.extend(domain.tool_names().iter().map(|name| (*name).to_owned()));
        }
        registered.insert("read_skill".to_owned());
        registered.extend(
            [
                "agent_start_options",
                "thread_message_send",
                "thread_create",
                "agent_start",
            ]
            .map(str::to_owned),
        );

        assert_eq!(registered.len(), 37, "native tool inventory changed");
        let unknown = registered
            .iter()
            .filter(|name| {
                let invocation = invocation_for_tool(
                    name,
                    ToolPayload::Function {
                        arguments: serde_json::json!({}),
                    },
                );
                extract_permission_intent(&invocation).action == PermissionActionKind::Unknown
            })
            .cloned()
            .collect::<Vec<_>>();

        assert!(
            unknown.is_empty(),
            "registered native tools without semantic permission classification: {unknown:?}"
        );
    }

    #[test]
    fn every_registered_native_tool_has_the_expected_three_mode_permission_matrix() {
        let expected_actions = BTreeMap::from([
            ("exec_command", PermissionActionKind::ShellCommand),
            ("write_stdin", PermissionActionKind::ShellCommand),
            ("read_file", PermissionActionKind::FileRead),
            ("list_dir", PermissionActionKind::FileRead),
            ("grep_files", PermissionActionKind::FileRead),
            ("apply_patch", PermissionActionKind::FileWrite),
            ("web_search", PermissionActionKind::Network),
            ("web_fetch", PermissionActionKind::Network),
            ("download_url", PermissionActionKind::Network),
            ("request_tools", PermissionActionKind::Internal),
            ("memory_search", PermissionActionKind::Internal),
            ("memory_list", PermissionActionKind::Internal),
            ("memory_get", PermissionActionKind::Internal),
            ("memory_remember", PermissionActionKind::MemoryWrite),
            ("memory_forget", PermissionActionKind::MemoryWrite),
            ("task_create", PermissionActionKind::TaskSubagent),
            ("task_wait", PermissionActionKind::Internal),
            ("task_result", PermissionActionKind::Internal),
            ("task_accept", PermissionActionKind::TaskSubagent),
            ("task_revise", PermissionActionKind::TaskSubagent),
            ("task_cancel", PermissionActionKind::TaskSubagent),
            ("task_update", PermissionActionKind::TaskSubagent),
            ("task_detach", PermissionActionKind::TaskSubagent),
            ("task_list", PermissionActionKind::Internal),
            ("task_get", PermissionActionKind::Internal),
            ("task_reschedule", PermissionActionKind::TaskSubagent),
            ("task_pause", PermissionActionKind::TaskSubagent),
            ("task_resume", PermissionActionKind::TaskSubagent),
            ("artifact_prepare", PermissionActionKind::Internal),
            ("artifact_register", PermissionActionKind::Internal),
            ("artifact_read", PermissionActionKind::Internal),
            ("computer_use", PermissionActionKind::ComputerUse),
            ("read_skill", PermissionActionKind::Internal),
            ("agent_start_options", PermissionActionKind::Internal),
            ("thread_message_send", PermissionActionKind::AgentAction),
            ("thread_create", PermissionActionKind::AgentAction),
            ("agent_start", PermissionActionKind::AgentAction),
        ]);

        let mut registered = crate::builtin_tool_specs()
            .into_iter()
            .map(|configured| configured.spec.name)
            .collect::<BTreeSet<_>>();
        for domain in crate::BuiltinToolDomain::ALL {
            registered.extend(domain.tool_names().iter().map(|name| (*name).to_owned()));
        }
        registered.insert("read_skill".to_owned());
        registered.extend(
            [
                "agent_start_options",
                "thread_message_send",
                "thread_create",
                "agent_start",
            ]
            .map(str::to_owned),
        );

        assert_eq!(registered.len(), 37, "native tool inventory changed");
        assert_eq!(
            registered,
            expected_actions
                .keys()
                .map(|name| (*name).to_owned())
                .collect::<BTreeSet<_>>(),
            "the permission matrix must be updated atomically with the native tool registry"
        );

        for (tool_name, expected_action) in expected_actions {
            let invocation = invocation_for_tool(
                tool_name,
                ToolPayload::Function {
                    arguments: serde_json::json!({}),
                },
            );
            let intent = extract_permission_intent(&invocation);
            assert_eq!(intent.action, expected_action, "{tool_name} action");

            for (mode, expected_behavior) in [
                (
                    pioneer_protocol::TurnPermissionMode::FullAccess,
                    PermissionBehavior::Allow,
                ),
                (
                    pioneer_protocol::TurnPermissionMode::AutoAcceptEdits,
                    match expected_action {
                        PermissionActionKind::FileRead
                        | PermissionActionKind::FileWrite
                        | PermissionActionKind::McpRead
                        | PermissionActionKind::Internal => PermissionBehavior::Allow,
                        _ => PermissionBehavior::Ask,
                    },
                ),
                (
                    pioneer_protocol::TurnPermissionMode::Supervised,
                    match expected_action {
                        PermissionActionKind::FileRead
                        | PermissionActionKind::McpRead
                        | PermissionActionKind::Internal => PermissionBehavior::Allow,
                        _ => PermissionBehavior::Ask,
                    },
                ),
            ] {
                let context = test_context(mode);
                let decision =
                    ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent);
                assert!(
                    matches!(
                        (expected_behavior, decision),
                        (PermissionBehavior::Allow, PermissionDecision::Allow { .. })
                            | (PermissionBehavior::Ask, PermissionDecision::Ask { .. })
                            | (PermissionBehavior::Deny, PermissionDecision::Deny { .. })
                    ),
                    "unexpected {mode:?} decision for {tool_name} ({expected_action:?})"
                );
            }
        }
    }

    fn dynamic_skill_metadata(
        kind: DynamicSkillPermissionKind,
        target_tool: Option<&str>,
        configured_method: Option<&str>,
        configured_url: Option<&str>,
    ) -> ToolPermissionMetadata {
        ToolPermissionMetadata {
            dynamic_skill: Some(DynamicSkillPermissionMetadata {
                kind,
                skill_id: pioneer_protocol::SkillId::new("PPPPPPPPPPPPPPPPPPPPP")
                    .expect("valid permission test SkillId"),
                skill_owner: Some("workspace".to_owned()),
                skill_slug: "user:weather".to_owned(),
                skill_fingerprint: "weather-test-fingerprint".to_owned(),
                source_kind: "User".to_owned(),
                trust_level: "Trusted".to_owned(),
                target_tool: target_tool.map(str::to_owned),
                configured_method: configured_method.map(str::to_owned),
                configured_url: configured_url.map(str::to_owned),
            }),
            nested_dynamic_skills: Vec::new(),
            network_targets: Vec::new(),
        }
    }

    fn context_with_policy(policy: ToolPermissionPolicySnapshot) -> PermissionEvaluationContext {
        PermissionEvaluationContext {
            workspace_id: Some("workspace".to_owned()),
            thread_id: Some("thread".to_owned()),
            turn_id: Some("turn".to_owned()),
            permission_profile: pioneer_protocol::TurnPermissionProfileSnapshot {
                mode: pioneer_protocol::TurnPermissionMode::FullAccess,
                source: pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
                effective_policy: policy,
            },
        }
    }

    #[test]
    fn profile_evaluator_denies_task_policy_denied_tool() {
        let mut policy = ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow);
        policy.denied_tools = vec!["exec_command".to_owned()];
        let context = context_with_policy(policy);
        let invocation = invocation_for_tool(
            "exec_command",
            ToolPayload::Function {
                arguments: serde_json::json!({ "cmd": "pwd" }),
            },
        );
        let intent = extract_permission_intent(&invocation);
        let decision = ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent);

        assert!(matches!(
            decision,
            PermissionDecision::Deny {
                reason: PermissionDecisionReason::PolicyDeniesAction,
                ..
            }
        ));
    }

    #[test]
    fn profile_evaluator_allows_task_policy_allowed_tool() {
        let mut policy = ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow);
        policy.allowed_tools = vec!["read_file".to_owned()];
        let context = context_with_policy(policy);
        let invocation = invocation();
        let intent = extract_permission_intent(&invocation);
        let decision = ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent);

        assert!(matches!(decision, PermissionDecision::Allow { .. }));
    }

    #[test]
    fn profile_evaluator_denies_file_path_outside_task_policy_scope() {
        let mut policy = ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow);
        policy.allowed_paths = vec!["/workspace/src".to_owned()];
        let context = context_with_policy(policy);
        let invocation = invocation_for_tool(
            "read_file",
            ToolPayload::Function {
                arguments: serde_json::json!({ "path": "README.md" }),
            },
        );
        let intent = extract_permission_intent(&invocation);
        let decision = ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent);

        assert!(matches!(
            decision,
            PermissionDecision::Deny {
                reason: PermissionDecisionReason::PolicyDeniesAction,
                ..
            }
        ));
    }

    #[test]
    fn profile_evaluator_denies_every_tool_for_a_restricted_empty_tool_set() {
        let mut policy = ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow);
        policy.allowed_tools_restricted = true;
        let context = context_with_policy(policy);
        let invocation = invocation();
        let intent = extract_permission_intent(&invocation);

        let decision = ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent);

        assert!(matches!(
            decision,
            PermissionDecision::Deny {
                reason: PermissionDecisionReason::PolicyDeniesAction,
                ..
            }
        ));
    }

    #[test]
    fn profile_evaluator_denies_every_file_for_a_restricted_empty_path_set() {
        let mut policy = ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow);
        policy.allowed_paths_restricted = true;
        let context = context_with_policy(policy);
        let invocation = invocation();
        let intent = extract_permission_intent(&invocation);

        let decision = ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent);

        assert!(matches!(
            decision,
            PermissionDecision::Deny {
                reason: PermissionDecisionReason::PolicyDeniesAction,
                ..
            }
        ));
    }

    #[test]
    fn request_scope_hash_is_order_independent() {
        let first = PermissionRequestScope::from_pairs([("tool_name", "read_file"), ("path", "a")]);
        let second =
            PermissionRequestScope::from_pairs([("path", "a"), ("tool_name", "read_file")]);

        assert_eq!(first.normalized_hash(), second.normalized_hash());
    }

    #[test]
    fn permission_intent_request_key_uses_normalized_scope_hash() {
        let invocation = invocation_for_tool(
            "read_file",
            ToolPayload::Function {
                arguments: serde_json::json!({ "path": "src/lib.rs" }),
            },
        );
        let context = PermissionEvaluationContext::for_turn(
            "workspace",
            "thread",
            "turn",
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            ),
        );
        let intent = PermissionIntent::new(
            PermissionActionKind::FileRead,
            PermissionRequestScope::from_pairs([(" Path ", " src/lib.rs ")]),
        );
        let key = intent.request_key(&context, &invocation);

        assert_eq!(
            key.profile_mode,
            pioneer_protocol::TurnPermissionMode::Supervised
        );
        assert_eq!(key.tool_name, "read_file");
        assert_eq!(key.action, PermissionActionKind::FileRead);
        assert_eq!(key.normalized_scope_hash, intent.scope.normalized_hash());
        assert_eq!(key.turn_id, "turn");
        assert_eq!(
            intent.scope.entries.get("path"),
            Some(&"src/lib.rs".to_owned())
        );
    }

    #[test]
    fn profile_evaluator_uses_full_access_allow_policy() {
        let invocation = invocation();
        let intent = PermissionIntent::generic_for_invocation(&invocation);
        let decision =
            ProfileToolPermissionEvaluator.evaluate(&full_access_context(), &invocation, &intent);

        assert_eq!(
            decision,
            PermissionDecision::Allow {
                reason: PermissionDecisionReason::PolicyAllowsAction
            }
        );
    }

    #[test]
    fn profile_evaluator_asks_for_unknown_restricted_actions() {
        let invocation = invocation();
        let intent = PermissionIntent::generic_for_invocation(&invocation);
        let context = test_context(pioneer_protocol::TurnPermissionMode::Supervised);
        let decision = ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent);

        assert!(intent.is_unknown_capability());
        assert_eq!(
            intent.scope.entries.get("unknown_capability"),
            Some(&"true".to_owned())
        );
        assert!(matches!(
            decision,
            PermissionDecision::Ask {
                reason: PermissionDecisionReason::UnknownActionDefault,
                ..
            }
        ));
    }

    #[test]
    fn unknown_capability_denied_policy_returns_unknown_reason() {
        let invocation = invocation_for_tool(
            "mystery_tool",
            ToolPayload::Function {
                arguments: serde_json::json!({ "anything": true }),
            },
        );
        let intent = extract_permission_intent(&invocation);
        let context =
            context_with_policy(ToolPermissionPolicySnapshot::all(PermissionBehavior::Deny));

        let decision = ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent);

        assert_eq!(intent.action, PermissionActionKind::Unknown);
        assert!(intent.is_unknown_capability());
        assert_eq!(
            decision,
            PermissionDecision::Deny {
                reason: PermissionDecisionReason::UnknownActionDefault,
                message: "unknown tool capability denied by turn permission profile".to_owned()
            }
        );
    }

    #[test]
    fn extractor_classifies_read_file_as_file_read_with_resolved_path() {
        let intent = extract_permission_intent(&invocation());

        assert_eq!(intent.action, PermissionActionKind::FileRead);
        assert_eq!(
            intent.scope.entries.get("path"),
            Some(&"/workspace/src/lib.rs".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("operation"),
            Some(&"read file".to_owned())
        );
    }

    #[test]
    fn file_permission_scope_uses_the_dynamic_security_snapshot_cwd() {
        let dynamic_cwd = tempfile::tempdir().unwrap();
        let stale_workdir = tempfile::tempdir().unwrap();
        let mut invocation = invocation_for_tool(
            "read_file",
            ToolPayload::Function {
                arguments: serde_json::json!({ "path": "src/lib.rs" }),
            },
        );
        invocation.workdir = stale_workdir.path().to_path_buf();
        invocation.execution_security_snapshot = Some(
            pioneer_protocol::TurnExecutionSecuritySnapshot::unrestricted_full_access(
                dynamic_cwd.path().to_string_lossy(),
                1,
            ),
        );

        let intent = extract_permission_intent(&invocation);

        assert_eq!(
            intent.scope.entries.get("path"),
            Some(
                &dynamic_cwd
                    .path()
                    .join("src/lib.rs")
                    .to_string_lossy()
                    .into_owned()
            )
        );
    }

    #[test]
    fn extractor_classifies_task_create_as_subagent_launch() {
        let invocation = invocation_for_tool(
            "task_create",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "title": "Delegate",
                    "goal": "Do delegated work"
                }),
            },
        );

        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::TaskSubagent);
        assert_eq!(intent.scope.entries.get("domain"), Some(&"task".to_owned()));
        assert_eq!(
            intent.scope.entries.get("operation"),
            Some(&"create task/subagent".to_owned())
        );
    }

    #[test]
    fn extractor_classifies_list_dir_default_path_as_file_read() {
        let invocation = invocation_for_tool(
            "list_dir",
            ToolPayload::Function {
                arguments: serde_json::json!({}),
            },
        );
        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::FileRead);
        assert_eq!(
            intent.scope.entries.get("path"),
            Some(&"/workspace".to_owned())
        );
    }

    #[test]
    fn extractor_classifies_grep_files_with_path_and_glob_as_file_read() {
        let invocation = invocation_for_tool(
            "grep_files",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "pattern": "permission",
                    "path": "crates/tools",
                    "glob": "*.rs"
                }),
            },
        );
        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::FileRead);
        assert_eq!(
            intent.scope.entries.get("path"),
            Some(&"/workspace/crates/tools".to_owned())
        );
        assert_eq!(intent.scope.entries.get("glob"), Some(&"*.rs".to_owned()));
    }

    #[test]
    fn extractor_classifies_valid_apply_patch_as_file_write_with_changed_paths() {
        let patch = "\
*** Begin Patch
*** Add File: a.txt
+hello
*** Update File: src/lib.rs
@@
-old
+new
*** Delete File: old.txt
*** End Patch";
        let invocation = invocation_for_tool(
            "apply_patch",
            ToolPayload::Function {
                arguments: serde_json::json!({ "patch": patch }),
            },
        );

        let (intent, preflight) = extract_permission_intent_with_preflight(&invocation);

        assert_eq!(intent.action, PermissionActionKind::FileWrite);
        let preflight = preflight.expect("valid patch must carry its canonical preflight");
        let ApplyPatchPreflight::Ready(preflight) = preflight else {
            panic!("valid patch must not carry a rejection preflight");
        };
        let expected_payload_hash: [u8; 32] = Sha256::digest(patch.as_bytes()).into();
        assert_eq!(preflight.document().payload_hash, expected_payload_hash);
        assert_eq!(
            preflight.target_manifest().targets().len(),
            5,
            "three files, the nested src parent, and the workspace root are authorized"
        );
        assert_eq!(
            intent.scope.entries.get("parse_status"),
            Some(&"valid_patch_document".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("changed_path_count"),
            Some(&"3".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("path.0"),
            Some(&"/workspace/a.txt".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("path.1"),
            Some(&"/workspace/old.txt".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("path.2"),
            Some(&"/workspace/src/lib.rs".to_owned())
        );
        assert!(!intent.scope.entries.contains_key("path.3"));
        assert_eq!(
            intent.scope.entries.get("parser_schema_version"),
            Some(&"1".to_owned())
        );
        assert!(intent.scope.entries.contains_key("payload_hash"));
        assert!(intent.scope.entries.contains_key("target_manifest"));
        assert_eq!(
            intent.scope.entries.get("operations"),
            Some(&"[\"add\",\"delete\",\"update\"]".to_owned())
        );
    }

    #[test]
    fn apply_patch_defers_content_preflight_until_supervised_write_grant() {
        let root = tempfile::tempdir().expect("workspace should create");
        let target = root
            .path()
            .canonicalize()
            .expect("workspace should canonicalize")
            .join("approved.txt");
        let mut invocation = invocation_for_tool(
            "apply_patch",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "patch": "*** Begin Patch\n*** Add File: approved.txt\n+approved\n*** End Patch"
                }),
            },
        );
        invocation.workdir = root.path().to_path_buf();
        invocation.execution_security_snapshot =
            Some(pioneer_protocol::TurnExecutionSecuritySnapshot::read_only(
                pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                    pioneer_protocol::TurnPermissionMode::Supervised,
                    pioneer_protocol::TurnPermissionProfileSource::Composer,
                ),
                root.path().to_string_lossy(),
                vec![
                    pioneer_protocol::TurnFilesystemSandboxEntry::workspace_root(
                        pioneer_protocol::TurnFilesystemAccess::Read,
                        root.path().to_string_lossy(),
                    ),
                ],
                1,
            ));

        let (intent, preflight) = extract_permission_intent_with_preflight(&invocation);

        assert_eq!(intent.action, PermissionActionKind::FileWrite);
        assert!(
            preflight.is_none(),
            "content preflight must wait for consent"
        );
        assert_eq!(
            intent.scope.entries.get("parse_status"),
            Some(&"awaiting_filesystem_grant".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("changed_paths"),
            Some(&serde_json::to_string(&vec![target.to_string_lossy().into_owned()]).unwrap())
        );
    }

    #[test]
    fn extractor_keeps_malformed_apply_patch_in_the_known_file_write_capability() {
        let invocation = invocation_for_tool(
            "apply_patch",
            ToolPayload::Function {
                arguments: serde_json::json!({ "patch": "*** Add File: a.txt\n+hello" }),
            },
        );

        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::FileWrite);
        assert_eq!(
            intent.scope.entries.get("parse_status"),
            Some(&"invalid_patch_document".to_owned())
        );
    }

    #[test]
    fn auto_accept_edits_allows_valid_apply_patch() {
        let context = test_context(pioneer_protocol::TurnPermissionMode::AutoAcceptEdits);
        let patch = "\
*** Begin Patch
*** Add File: a.txt
+hello
*** End Patch";
        let invocation = invocation_for_tool(
            "apply_patch",
            ToolPayload::Custom {
                input: patch.to_owned(),
            },
        );
        let intent = extract_permission_intent(&invocation);

        assert_eq!(
            ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
            PermissionDecision::Allow {
                reason: PermissionDecisionReason::PolicyAllowsAction
            }
        );
    }

    #[test]
    fn supervised_profile_asks_before_apply_patch() {
        let context = test_context(pioneer_protocol::TurnPermissionMode::Supervised);
        let patch = "\
*** Begin Patch
*** Add File: a.txt
+hello
*** End Patch";
        let invocation = invocation_for_tool(
            "apply_patch",
            ToolPayload::Custom {
                input: patch.to_owned(),
            },
        );
        let intent = extract_permission_intent(&invocation);

        assert!(matches!(
            ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
            PermissionDecision::Ask {
                reason: PermissionDecisionReason::PolicyRequiresApproval,
                ..
            }
        ));
    }

    #[test]
    fn extractor_classifies_exec_command_with_command_cwd_timeout_and_tty() {
        let invocation = invocation_for_tool(
            "exec_command",
            ToolPayload::LocalShell(crate::context::LocalShellPayload::ExecCommand(
                ExecCommandArgs {
                    command: Some(vec!["cargo".to_owned(), "check".to_owned()]),
                    workdir: Some("crates/tools".to_owned()),
                    timeout_ms: Some(120_000),
                    max_output_tokens: None,
                    yield_time_ms: None,
                    tty: Some(true),
                },
            )),
        );

        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::ShellCommand);
        assert_eq!(
            intent.scope.entries.get("command"),
            Some(&"cargo check".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("argv"),
            Some(&"[\"cargo\",\"check\"]".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("cwd"),
            Some(&"/workspace/crates/tools".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("timeout_ms"),
            Some(&"120000".to_owned())
        );
        assert_eq!(intent.scope.entries.get("tty"), Some(&"true".to_owned()));
    }

    #[test]
    fn extractor_includes_shell_env_key_names_without_values() {
        let mut invocation = invocation_for_tool(
            "exec_command",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "command": ["printenv", "SECRET_TOKEN"],
                    "workdir": "."
                }),
            },
        );
        invocation
            .environment
            .insert("SECRET_TOKEN".to_owned(), "super-secret-value".to_owned());
        invocation
            .environment
            .insert("PATH".to_owned(), "/bin:/usr/bin".to_owned());

        let intent = extract_permission_intent(&invocation);
        let payload = serde_json::to_string(&intent.scope.entries).expect("scope serializes");

        assert_eq!(
            intent.scope.entries.get("env_keys"),
            Some(&"[\"PATH\",\"SECRET_TOKEN\"]".to_owned())
        );
        assert!(payload.contains("SECRET_TOKEN"));
        assert!(!payload.contains("super-secret-value"));
        assert!(!payload.contains("/bin:/usr/bin"));
    }

    #[test]
    fn restricted_profiles_ask_before_exec_command() {
        let invocation = invocation_for_tool(
            "exec_command",
            ToolPayload::LocalShell(crate::context::LocalShellPayload::ExecCommand(
                ExecCommandArgs {
                    command: Some(vec!["echo".to_owned(), "hello".to_owned()]),
                    workdir: None,
                    timeout_ms: None,
                    max_output_tokens: None,
                    yield_time_ms: None,
                    tty: None,
                },
            )),
        );
        let intent = extract_permission_intent(&invocation);

        for mode in [
            pioneer_protocol::TurnPermissionMode::AutoAcceptEdits,
            pioneer_protocol::TurnPermissionMode::Supervised,
        ] {
            let context = test_context(mode);
            assert!(matches!(
                ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
                PermissionDecision::Ask {
                    reason: PermissionDecisionReason::PolicyRequiresApproval,
                    ..
                }
            ));
        }
    }

    #[test]
    fn extractor_classifies_write_stdin_without_exposing_stdin_content() {
        let invocation = invocation_for_tool(
            "write_stdin",
            ToolPayload::LocalShell(crate::context::LocalShellPayload::WriteStdin(
                WriteStdinArgs {
                    session_id: 42,
                    chars: Some("password=secret\n".to_owned()),
                    yield_time_ms: None,
                    max_output_tokens: None,
                },
            )),
        );

        let intent = extract_permission_intent(&invocation);
        let payload = serde_json::to_string(&intent.scope.entries).expect("scope serializes");

        assert_eq!(intent.action, PermissionActionKind::ShellCommand);
        assert_eq!(
            intent.scope.entries.get("session_id"),
            Some(&"42".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("stdin_chars_present"),
            Some(&"true".to_owned())
        );
        assert!(intent.scope.entries.contains_key("stdin_bytes"));
        assert!(!payload.contains("password=secret"));
    }

    #[test]
    fn extractor_classifies_web_fetch_as_network_with_redacted_url_scope() {
        let invocation = invocation_for_tool(
            "web_fetch",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "url": "https://user:password@example.com:8443/path/to/page?token=secret#frag"
                }),
            },
        );

        let intent = extract_permission_intent(&invocation);
        let payload = serde_json::to_string(&intent.scope.entries).expect("scope serializes");

        assert_eq!(intent.action, PermissionActionKind::Network);
        assert_eq!(intent.scope.entries.get("method"), Some(&"GET".to_owned()));
        assert_eq!(
            intent.scope.entries.get("domain"),
            Some(&"example.com".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("url_origin"),
            Some(&"https://example.com:8443".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("url_path"),
            Some(&"/path/to/page".to_owned())
        );
        assert!(intent.scope.entries.contains_key("url_full_hash"));
        assert!(!payload.contains("user"));
        assert!(!payload.contains("password"));
        assert!(!payload.contains("token=secret"));
        assert!(!payload.contains("frag"));
    }

    #[test]
    fn extractor_marks_download_url_localhost_destination_hint() {
        let invocation = invocation_for_tool(
            "download_url",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "url": "http://127.0.0.1:8080/archive.tgz",
                    "destination": "downloads/archive.tgz"
                }),
            },
        );

        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::Network);
        assert_eq!(
            intent.scope.entries.get("destination_hint"),
            Some(&"private_or_local".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("destination"),
            Some(&"/workspace/downloads/archive.tgz".to_owned())
        );
    }

    #[test]
    fn extractor_marks_download_url_implicit_destination_under_workdir() {
        let invocation = invocation_for_tool(
            "download_url",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "url": "https://example.com/archive.tgz"
                }),
            },
        );

        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::Network);
        assert_eq!(
            intent.scope.entries.get("destination"),
            Some(&"/workspace/archive.tgz".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("overwrite"),
            Some(&"false".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("create_dirs"),
            Some(&"true".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("follow_redirects"),
            Some(&"true".to_owned())
        );
        assert!(intent.scope.entries.contains_key("payload_hash"));
    }

    #[test]
    fn restricted_profiles_ask_before_network_tools() {
        let invocation = invocation_for_tool(
            "web_search",
            ToolPayload::Function {
                arguments: serde_json::json!({ "query": "pioneer permissions" }),
            },
        );
        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::Network);
        for mode in [
            pioneer_protocol::TurnPermissionMode::AutoAcceptEdits,
            pioneer_protocol::TurnPermissionMode::Supervised,
        ] {
            let context = test_context(mode);
            assert!(matches!(
                ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
                PermissionDecision::Ask {
                    reason: PermissionDecisionReason::PolicyRequiresApproval,
                    ..
                }
            ));
        }
    }

    #[test]
    fn web_search_permission_scope_preserves_configured_origins() {
        let mut invocation = invocation_for_tool(
            "web_search",
            ToolPayload::Function {
                arguments: serde_json::json!({ "query": "pioneer permissions" }),
            },
        );
        invocation.permission_metadata.network_targets = vec![
            "https://search.example:8443/html/".to_owned(),
            "http://answers.example:8080/api".to_owned(),
        ];

        let intent = extract_permission_intent(&invocation);
        let origins = serde_json::from_str::<Vec<String>>(
            intent
                .scope
                .entries
                .get("network_origins")
                .expect("configured origins"),
        )
        .expect("origin list");

        assert_eq!(
            origins,
            vec![
                "http://answers.example:8080".to_owned(),
                "https://search.example:8443".to_owned(),
            ]
        );
    }

    #[test]
    fn full_access_allows_network_tools() {
        let invocation = invocation_for_tool(
            "web_fetch",
            ToolPayload::Function {
                arguments: serde_json::json!({ "url": "https://example.com" }),
            },
        );
        let intent = extract_permission_intent(&invocation);

        assert_eq!(
            ProfileToolPermissionEvaluator.evaluate(&full_access_context(), &invocation, &intent),
            PermissionDecision::Allow {
                reason: PermissionDecisionReason::PolicyAllowsAction
            }
        );
    }

    #[test]
    fn extractor_treats_server_declared_read_only_mcp_as_untrusted() {
        let invocation = invocation_for_tool(
            "mcp_docs_search",
            ToolPayload::Mcp {
                server: "srv_docs".to_owned(),
                tool: "search".to_owned(),
                arguments: serde_json::json!({ "q": "permissions" }),
                read_only_hint: Some(true),
                destructive_hint: Some(false),
                open_world_hint: Some(false),
            },
        );

        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::McpWriteOrUnknown);
        assert_eq!(
            intent.scope.entries.get("server"),
            Some(&"srv_docs".to_owned())
        );
        assert_eq!(intent.scope.entries.get("tool"), Some(&"search".to_owned()));
        assert_eq!(
            intent.scope.entries.get("mcp_side_effect_class"),
            Some(&"unknown".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("mcp_requires_network"),
            Some(&"true".to_owned())
        );
    }

    #[test]
    fn restricted_profiles_ask_for_server_declared_read_only_mcp() {
        let invocation = invocation_for_tool(
            "mcp_docs_search",
            ToolPayload::Mcp {
                server: "srv_docs".to_owned(),
                tool: "search".to_owned(),
                arguments: serde_json::json!({ "q": "permissions" }),
                read_only_hint: Some(true),
                destructive_hint: Some(false),
                open_world_hint: Some(false),
            },
        );
        let intent = extract_permission_intent(&invocation);
        let context = test_context(pioneer_protocol::TurnPermissionMode::Supervised);

        assert!(matches!(
            ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
            PermissionDecision::Ask {
                reason: PermissionDecisionReason::UnknownActionDefault,
                ..
            }
        ));
    }

    #[test]
    fn extractor_classifies_destructive_network_or_unknown_mcp_as_write_or_unknown() {
        for (read_only_hint, destructive_hint, open_world_hint, expected_class, requires_network) in [
            (Some(true), Some(true), Some(false), "write_like", "true"),
            (Some(false), Some(false), Some(true), "network_like", "true"),
            (None, None, None, "unknown", "true"),
            (Some(false), Some(false), Some(false), "unknown", "true"),
        ] {
            let invocation = invocation_for_tool(
                "mcp_repo_write",
                ToolPayload::Mcp {
                    server: "srv_repo".to_owned(),
                    tool: "repo_write".to_owned(),
                    arguments: serde_json::json!({ "path": "README.md" }),
                    read_only_hint,
                    destructive_hint,
                    open_world_hint,
                },
            );
            let intent = extract_permission_intent(&invocation);

            assert_eq!(intent.action, PermissionActionKind::McpWriteOrUnknown);
            assert_eq!(
                intent.scope.entries.get("mcp_side_effect_class"),
                Some(&expected_class.to_owned())
            );
            assert_eq!(
                intent.scope.entries.get("mcp_requires_network"),
                Some(&requires_network.to_owned())
            );
        }
    }

    #[test]
    fn restricted_profiles_ask_for_unknown_mcp_tools() {
        let invocation = invocation_for_tool(
            "mcp_repo_write",
            ToolPayload::Mcp {
                server: "srv_repo".to_owned(),
                tool: "repo_write".to_owned(),
                arguments: serde_json::json!({ "path": "README.md" }),
                read_only_hint: None,
                destructive_hint: None,
                open_world_hint: None,
            },
        );
        let intent = extract_permission_intent(&invocation);
        let context = test_context(pioneer_protocol::TurnPermissionMode::Supervised);

        assert!(intent.is_unknown_capability());
        assert_eq!(
            intent.scope.entries.get("unknown_reason"),
            Some(&"mcp side effects are not classified".to_owned())
        );
        assert!(matches!(
            ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
            PermissionDecision::Ask {
                reason: PermissionDecisionReason::UnknownActionDefault,
                ..
            }
        ));
    }

    #[test]
    fn unknown_capability_skill_function_proxy_without_target_requires_approval() {
        let mut invocation = invocation_for_tool(
            "skill_proxy_unknown",
            ToolPayload::Function {
                arguments: serde_json::json!({ "arguments": { "value": 1 } }),
            },
        );
        invocation.permission_metadata =
            dynamic_skill_metadata(DynamicSkillPermissionKind::FunctionProxy, None, None, None);
        let intent = extract_permission_intent(&invocation);
        let context = test_context(pioneer_protocol::TurnPermissionMode::Supervised);

        assert_eq!(intent.action, PermissionActionKind::DynamicSkillTool);
        assert!(intent.is_unknown_capability());
        assert_eq!(
            intent.scope.entries.get("unknown_reason"),
            Some(&"dynamic skill function proxy target tool is unavailable".to_owned())
        );
        assert!(matches!(
            ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
            PermissionDecision::Ask {
                reason: PermissionDecisionReason::UnknownActionDefault,
                ..
            }
        ));
    }

    #[test]
    fn extractor_classifies_dynamic_http_skill_as_network() {
        let mut invocation = invocation_for_tool(
            "skill_weather_http",
            ToolPayload::Function {
                arguments: serde_json::json!({ "city": "Paris" }),
            },
        );
        invocation.permission_metadata = dynamic_skill_metadata(
            DynamicSkillPermissionKind::Http,
            None,
            Some("POST"),
            Some("https://api.example.com/weather?token=secret"),
        );

        let intent = extract_permission_intent(&invocation);
        let payload = serde_json::to_string(&intent.scope.entries).expect("scope serializes");

        assert_eq!(intent.action, PermissionActionKind::Network);
        assert_eq!(
            intent.scope.entries.get("skill_id"),
            Some(&"PPPPPPPPPPPPPPPPPPPPP".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("skill_owner"),
            Some(&"workspace".to_owned())
        );
        assert_eq!(intent.scope.entries.get("method"), Some(&"POST".to_owned()));
        assert_eq!(
            intent.scope.entries.get("dynamic_skill_kind"),
            Some(&"http".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("domain"),
            Some(&"api.example.com".to_owned())
        );
        assert!(!payload.contains("token=secret"));
    }

    #[test]
    fn dynamic_shell_wrapper_defers_permission_to_nested_command() {
        let mut invocation = invocation_for_tool(
            "skill_shell",
            ToolPayload::Function {
                arguments: serde_json::json!({ "command": ["echo", "hello"] }),
            },
        );
        invocation.permission_metadata =
            dynamic_skill_metadata(DynamicSkillPermissionKind::Shell, None, None, None);

        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::Internal);
        assert_eq!(
            intent.scope.entries.get("dynamic_skill_kind"),
            Some(&"shell".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("command"),
            Some(&"echo hello".to_owned())
        );
    }

    #[test]
    fn dynamic_function_proxy_wrapper_defers_permission_to_target() {
        let mut invocation = invocation_for_tool(
            "skill_proxy",
            ToolPayload::Function {
                arguments: serde_json::json!({ "arguments": { "path": "README.md" } }),
            },
        );
        invocation.permission_metadata = dynamic_skill_metadata(
            DynamicSkillPermissionKind::FunctionProxy,
            Some("read_file"),
            None,
            None,
        );

        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::Internal);
        assert_eq!(
            intent.scope.entries.get("target_tool"),
            Some(&"read_file".to_owned())
        );
    }

    #[test]
    fn nested_dynamic_skill_target_receives_one_tightened_semantic_permission() {
        let mut invocation = invocation_for_tool(
            "skill_proxy",
            ToolPayload::Function {
                arguments: serde_json::json!({ "arguments": { "path": "README.md" } }),
            },
        );
        invocation.permission_metadata = dynamic_skill_metadata(
            DynamicSkillPermissionKind::FunctionProxy,
            Some("read_file"),
            None,
            None,
        );
        let origin = invocation
            .permission_metadata
            .dynamic_skill
            .take()
            .expect("proxy metadata");
        invocation.tool_name = "read_file".to_owned();
        invocation.source = ToolCallSource::NestedTool;
        invocation.payload = ToolPayload::Function {
            arguments: serde_json::json!({ "path": "README.md" }),
        };
        invocation
            .permission_metadata
            .nested_dynamic_skills
            .push(origin);
        let intent = extract_permission_intent(&invocation);
        let context = test_context(pioneer_protocol::TurnPermissionMode::Supervised);

        assert_eq!(intent.action, PermissionActionKind::FileRead);
        assert_eq!(
            intent.scope.entries.get("skill_origin_0_skill_slug"),
            Some(&"user:weather".to_owned())
        );
        assert!(matches!(
            ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
            PermissionDecision::Ask {
                reason: PermissionDecisionReason::PolicyRequiresApproval,
                ..
            }
        ));
    }

    #[test]
    fn extractor_requires_computer_use_permission_for_preflight() {
        let invocation = invocation_for_tool(
            "computer_use",
            ToolPayload::Function {
                arguments: serde_json::json!({ "action": "preflight" }),
            },
        );
        let intent = extract_permission_intent(&invocation);
        let context = test_context(pioneer_protocol::TurnPermissionMode::Supervised);

        assert_eq!(intent.action, PermissionActionKind::ComputerUse);
        assert_eq!(
            intent.scope.entries.get("action"),
            Some(&"preflight".to_owned())
        );
        assert!(matches!(
            ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
            PermissionDecision::Ask {
                reason: PermissionDecisionReason::PolicyRequiresApproval,
                ..
            }
        ));
    }

    #[test]
    fn restricted_profiles_ask_for_computer_use_desktop_actions() {
        let cases = [
            serde_json::json!({
                "action": "start",
                "goal": "Inspect app",
                "target": { "type": "active_app" }
            }),
            serde_json::json!({ "action": "snapshot", "session_id": 42 }),
            serde_json::json!({
                "action": "act",
                "session_id": 42,
                "act": { "type": "click", "target": { "node_id": "n1" } }
            }),
        ];
        let context = test_context(pioneer_protocol::TurnPermissionMode::Supervised);

        for arguments in cases {
            let invocation =
                invocation_for_tool("computer_use", ToolPayload::Function { arguments });
            let intent = extract_permission_intent(&invocation);

            assert_eq!(intent.action, PermissionActionKind::ComputerUse);
            assert!(matches!(
                ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
                PermissionDecision::Ask {
                    reason: PermissionDecisionReason::PolicyRequiresApproval,
                    ..
                }
            ));
        }
    }

    #[test]
    fn computer_use_permission_scope_binds_effective_launch_target_and_command() {
        let first = invocation_for_tool(
            "computer_use",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "action": "start",
                    "goal": "Inspect app",
                    "launch_command": "ignored-top-level-command",
                    "target": {
                        "type": "bundle_id",
                        "bundle_id": "com.example.First",
                        "launch_if_missing": true,
                        "launch_command": "open -b com.example.First"
                    }
                }),
            },
        );
        let second = invocation_for_tool(
            "computer_use",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "action": "start",
                    "goal": "Inspect app",
                    "target": {
                        "type": "bundle_id",
                        "bundle_id": "com.example.Second",
                        "launch_if_missing": true,
                        "launch_command": "open -b com.example.Second"
                    }
                }),
            },
        );

        let first_intent = extract_permission_intent(&first);
        let second_intent = extract_permission_intent(&second);

        assert_eq!(
            first_intent.scope.entries.get("target_bundle_id"),
            Some(&"com.example.First".to_owned())
        );
        assert_eq!(
            first_intent.scope.entries.get("launch_command"),
            Some(&"open -b com.example.First".to_owned()),
            "the nested target command is the effective runtime command"
        );
        assert_ne!(
            first_intent.scope.normalized_hash(),
            second_intent.scope.normalized_hash(),
            "turn approval for one desktop target must not authorize another"
        );
    }

    #[test]
    fn computer_use_permission_scope_binds_act_payload_without_exposing_typed_text() {
        let invocation = invocation_for_tool(
            "computer_use",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "action": "act",
                    "session_id": 42,
                    "act": {
                        "type": "input_type_text",
                        "target": { "node_id": "password-field", "snapshot_id": "s1" },
                        "text": "super-secret-password"
                    }
                }),
            },
        );

        let intent = extract_permission_intent(&invocation);
        let rendered_scope = serde_json::to_string(&intent.scope.entries).expect("scope JSON");

        assert_eq!(
            intent.scope.entries.get("act_type"),
            Some(&"input_type_text".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("input_text_present"),
            Some(&"true".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("input_text_bytes"),
            Some(&"21".to_owned())
        );
        assert!(!rendered_scope.contains("super-secret-password"));
        assert!(intent.scope.entries.contains_key("payload_hash"));
    }

    #[test]
    fn malformed_computer_use_is_still_computer_use() {
        let invocation = invocation_for_tool(
            "computer_use",
            ToolPayload::Function {
                arguments: serde_json::json!({ "session_id": 7 }),
            },
        );
        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::ComputerUse);
        assert_eq!(
            intent.scope.entries.get("action"),
            Some(&"unknown".to_owned())
        );
    }

    #[test]
    fn read_only_domain_tools_remain_allowed_but_domain_mutations_require_approval() {
        let context = test_context(pioneer_protocol::TurnPermissionMode::Supervised);
        let invocations = [
            invocation_for_tool(
                "request_tools",
                ToolPayload::Function {
                    arguments: serde_json::json!({
                        "domains": ["memory"],
                        "reason": "Need memory tools."
                    }),
                },
            ),
            invocation_for_tool(
                "read_skill",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "skill": "rust" }),
                },
            ),
            invocation_for_tool(
                "tool_search",
                ToolPayload::ToolSearch {
                    query: "memory".to_owned(),
                    limit: Some(8),
                    include_hidden: Some(false),
                },
            ),
            invocation_for_tool(
                "memory_remember",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "text": "User prefers concise answers." }),
                },
            ),
            invocation_for_tool(
                "memory_search",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "query": "preferences" }),
                },
            ),
            invocation_for_tool(
                "memory_list",
                ToolPayload::Function {
                    arguments: serde_json::json!({}),
                },
            ),
            invocation_for_tool(
                "memory_get",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "memoryId": "mem_a" }),
                },
            ),
            invocation_for_tool(
                "memory_forget",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "memoryId": "mem_a" }),
                },
            ),
            invocation_for_tool(
                "artifact_prepare",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "filename": "report.md" }),
                },
            ),
            invocation_for_tool(
                "artifact_register",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "artifact_id": "artifact_a" }),
                },
            ),
            invocation_for_tool(
                "artifact_read",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "artifactId": "artifact_a" }),
                },
            ),
            invocation_for_tool(
                "task_create",
                ToolPayload::Function {
                    arguments: serde_json::json!({
                        "title": "Follow up",
                        "goal": "Do work later"
                    }),
                },
            ),
            invocation_for_tool(
                "task_update",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "taskId": "task_a", "title": "Updated" }),
                },
            ),
            invocation_for_tool(
                "task_get",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "taskId": "task_a" }),
                },
            ),
            invocation_for_tool(
                "task_list",
                ToolPayload::Function {
                    arguments: serde_json::json!({}),
                },
            ),
            invocation_for_tool(
                "task_wait",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "taskIds": ["task_a"] }),
                },
            ),
            invocation_for_tool(
                "task_result",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "candidateId": "candidate_a" }),
                },
            ),
            invocation_for_tool(
                "task_accept",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "candidateId": "candidate_a" }),
                },
            ),
            invocation_for_tool(
                "task_revise",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "candidateId": "candidate_a" }),
                },
            ),
            invocation_for_tool(
                "task_cancel",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "taskId": "task_a" }),
                },
            ),
            invocation_for_tool(
                "task_detach",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "taskId": "task_a" }),
                },
            ),
            invocation_for_tool(
                "task_reschedule",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "taskId": "task_a" }),
                },
            ),
            invocation_for_tool(
                "task_pause",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "taskId": "task_a" }),
                },
            ),
            invocation_for_tool(
                "task_resume",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "taskId": "task_a" }),
                },
            ),
        ];

        for invocation in invocations {
            let intent = extract_permission_intent(&invocation);
            if matches!(
                invocation.tool_name.as_str(),
                "task_create"
                    | "task_accept"
                    | "task_revise"
                    | "task_cancel"
                    | "task_update"
                    | "task_detach"
                    | "task_reschedule"
                    | "task_pause"
                    | "task_resume"
            ) {
                assert_eq!(intent.action, PermissionActionKind::TaskSubagent);
                assert!(matches!(
                    ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
                    PermissionDecision::Ask {
                        reason: PermissionDecisionReason::PolicyRequiresApproval,
                        ..
                    }
                ));
            } else if matches!(
                invocation.tool_name.as_str(),
                "memory_remember" | "memory_forget"
            ) {
                assert_eq!(intent.action, PermissionActionKind::MemoryWrite);
                assert!(matches!(
                    ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
                    PermissionDecision::Ask {
                        reason: PermissionDecisionReason::PolicyRequiresApproval,
                        ..
                    }
                ));
            } else {
                assert_eq!(intent.action, PermissionActionKind::Internal);
                assert_eq!(
                    ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
                    PermissionDecision::Allow {
                        reason: PermissionDecisionReason::PolicyAllowsAction
                    }
                );
            }
        }
    }

    #[test]
    fn memory_forget_dry_run_is_read_only_but_mutations_require_approval() {
        let context = test_context(pioneer_protocol::TurnPermissionMode::Supervised);
        let preview = invocation_for_tool(
            "memory_forget",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "memoryId": "mem_a",
                    "dryRun": true
                }),
            },
        );
        let preview_intent = extract_permission_intent(&preview);
        assert_eq!(preview_intent.action, PermissionActionKind::Internal);
        assert_eq!(
            preview_intent.scope.entries.get("memory_id"),
            Some(&"mem_a".to_owned())
        );
        assert!(matches!(
            ProfileToolPermissionEvaluator.evaluate(&context, &preview, &preview_intent),
            PermissionDecision::Allow { .. }
        ));

        let remember = invocation_for_tool(
            "memory_remember",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "scope": "user",
                    "category": "preference",
                    "key": "tone",
                    "content": "concise"
                }),
            },
        );
        let remember_intent = extract_permission_intent(&remember);
        assert_eq!(remember_intent.action, PermissionActionKind::MemoryWrite);
        assert_eq!(
            remember_intent.scope.entries.get("key"),
            Some(&"tone".to_owned())
        );
        assert!(matches!(
            ProfileToolPermissionEvaluator.evaluate(&context, &remember, &remember_intent),
            PermissionDecision::Ask { .. }
        ));
    }

    #[test]
    fn agent_catalog_is_internal_but_agent_mutations_have_semantic_permissions() {
        let context = test_context(pioneer_protocol::TurnPermissionMode::Supervised);
        let options = invocation_for_tool(
            "agent_start_options",
            ToolPayload::Function {
                arguments: serde_json::json!({}),
            },
        );
        let options_intent = extract_permission_intent(&options);
        assert_eq!(options_intent.action, PermissionActionKind::Internal);
        assert!(matches!(
            ProfileToolPermissionEvaluator.evaluate(&context, &options, &options_intent),
            PermissionDecision::Allow { .. }
        ));

        for (name, arguments) in [
            (
                "thread_message_send",
                serde_json::json!({ "targetOptionId": "target_a", "input": {} }),
            ),
            (
                "thread_create",
                serde_json::json!({ "optionId": "create_a" }),
            ),
            (
                "agent_start",
                serde_json::json!({ "targetOptionId": "target_a", "input": {}, "launch": {} }),
            ),
        ] {
            let invocation = invocation_for_tool(name, ToolPayload::Function { arguments });
            let intent = extract_permission_intent(&invocation);
            assert_eq!(intent.action, PermissionActionKind::AgentAction);
            assert_eq!(
                intent.scope.entries.get("domain"),
                Some(&"agent".to_owned())
            );
            assert!(matches!(
                ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
                PermissionDecision::Ask {
                    reason: PermissionDecisionReason::PolicyRequiresApproval,
                    ..
                }
            ));
        }
    }

    #[test]
    fn turn_scoped_mutation_approvals_are_bound_to_complete_payloads() {
        let cases = [
            (
                "memory_remember",
                serde_json::json!({
                    "scope": "user",
                    "category": "preference",
                    "key": "editor",
                    "content": "first-secret-value"
                }),
                serde_json::json!({
                    "scope": "user",
                    "category": "preference",
                    "key": "editor",
                    "content": "second-secret-value"
                }),
            ),
            (
                "task_create",
                serde_json::json!({ "title": "First", "goal": "first-private-goal" }),
                serde_json::json!({ "title": "Second", "goal": "second-private-goal" }),
            ),
            (
                "agent_start",
                serde_json::json!({
                    "targetOptionId": "same-target",
                    "input": { "prompt": "first-private-agent-input" }
                }),
                serde_json::json!({
                    "targetOptionId": "same-target",
                    "input": { "prompt": "second-private-agent-input" }
                }),
            ),
            (
                "web_search",
                serde_json::json!({ "query": "first-private-query" }),
                serde_json::json!({ "query": "second-private-query" }),
            ),
        ];

        for (tool_name, first, second) in cases {
            let first = extract_permission_intent(&invocation_for_tool(
                tool_name,
                ToolPayload::Function { arguments: first },
            ));
            let second = extract_permission_intent(&invocation_for_tool(
                tool_name,
                ToolPayload::Function { arguments: second },
            ));
            assert!(
                first.scope.entries.contains_key("payload_hash"),
                "{tool_name}"
            );
            assert_ne!(
                first.scope.normalized_hash(),
                second.scope.normalized_hash(),
                "changed {tool_name} payload must require a distinct decision"
            );
            let rendered = serde_json::to_string(&first.scope.entries).expect("scope JSON");
            assert!(!rendered.contains("first-private"), "{tool_name}");
        }
    }

    #[test]
    fn mcp_approval_is_bound_to_arguments_without_exposing_them() {
        let invocation = |arguments| {
            invocation_for_tool(
                "mcp_repo_mutate",
                ToolPayload::Mcp {
                    server: "srv_repo".to_owned(),
                    tool: "mutate".to_owned(),
                    arguments,
                    read_only_hint: Some(false),
                    destructive_hint: Some(true),
                    open_world_hint: Some(false),
                },
            )
        };
        let first = extract_permission_intent(&invocation(serde_json::json!({
            "path": "first-private-path"
        })));
        let second = extract_permission_intent(&invocation(serde_json::json!({
            "path": "second-private-path"
        })));

        assert_ne!(
            first.scope.normalized_hash(),
            second.scope.normalized_hash()
        );
        let rendered = serde_json::to_string(&first.scope.entries).expect("scope JSON");
        assert!(!rendered.contains("first-private-path"));
        assert!(first.scope.entries.contains_key("payload_hash"));
    }

    #[test]
    fn dynamic_http_approval_is_bound_to_payload_and_selected_skill_revision() {
        let mut first = invocation_for_tool(
            "skill_http",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "url": "https://example.com/action",
                    "method": "POST",
                    "body": { "secret": "first-private-body" }
                }),
            },
        );
        first.permission_metadata = dynamic_skill_metadata(
            DynamicSkillPermissionKind::Http,
            None,
            Some("POST"),
            Some("https://example.com/action"),
        );
        let mut second = first.clone();
        second.payload = ToolPayload::Function {
            arguments: serde_json::json!({
                "url": "https://example.com/action",
                "method": "POST",
                "body": { "secret": "second-private-body" }
            }),
        };
        let first_intent = extract_permission_intent(&first);
        let second_intent = extract_permission_intent(&second);
        assert_ne!(
            first_intent.scope.normalized_hash(),
            second_intent.scope.normalized_hash()
        );

        let mut revised = first.clone();
        revised
            .permission_metadata
            .dynamic_skill
            .as_mut()
            .expect("skill metadata")
            .skill_fingerprint = "revised-fingerprint".to_owned();
        let revised_intent = extract_permission_intent(&revised);
        assert_ne!(
            first_intent.scope.normalized_hash(),
            revised_intent.scope.normalized_hash()
        );
        let rendered = serde_json::to_string(&first_intent.scope.entries).expect("scope JSON");
        assert!(!rendered.contains("first-private-body"));
    }
}
