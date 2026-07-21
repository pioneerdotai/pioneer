use crate::context::ToolPayload;
use crate::context::{ExecCommandArgs, ToolInvocation, WriteStdinArgs};
use crate::domain::{ARTIFACT_DOMAIN_TOOL_NAMES, MEMORY_DOMAIN_TOOL_NAMES, TASK_DOMAIN_TOOL_NAMES};
use crate::handlers::apply_patch::{extract_patch_input, validate_patch_document};
use crate::spec::DynamicSkillPermissionKind;
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
    extract_shell_permission_intent(invocation)
        .or_else(|| extract_network_permission_intent(invocation))
        .or_else(|| extract_mcp_permission_intent(invocation))
        .or_else(|| extract_dynamic_skill_permission_intent(invocation))
        .or_else(|| extract_computer_use_permission_intent(invocation))
        .or_else(|| extract_task_permission_intent(invocation))
        .or_else(|| extract_internal_permission_intent(invocation))
        .or_else(|| extract_file_permission_intent(invocation))
        .unwrap_or_else(|| PermissionIntent::generic_for_invocation(invocation))
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
        "write_file" => Some(file_write_intent(invocation, "write_file")),
        "edit_file" => Some(file_mutation_intent(invocation, "edit_file", "edit file")),
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
                action: PermissionActionKind::ShellCommand,
                scope,
                summary: Some(format!(
                    "dynamic skill shell tool `{}`",
                    invocation.tool_name
                )),
            })
        }
        DynamicSkillPermissionKind::FunctionProxy => Some(PermissionIntent {
            action: PermissionActionKind::DynamicSkillTool,
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
            if let Some(act_type) = nested_string_field(arguments, "act", "type") {
                scope
                    .entries
                    .insert("act_type".to_owned(), act_type.to_owned());
            }
        }
        None => {
            scope.entries.insert(
                "parse_status".to_owned(),
                "missing_function_arguments".to_owned(),
            );
        }
    }

    if action == "preflight" {
        return Some(PermissionIntent {
            action: PermissionActionKind::Internal,
            scope,
            summary: Some("computer_use preflight".to_owned()),
        });
    }

    Some(PermissionIntent {
        action: PermissionActionKind::ComputerUse,
        scope,
        summary: Some(format!("computer_use {action}")),
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
    }

    Some(PermissionIntent {
        action: PermissionActionKind::Internal,
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
    if invocation.tool_name == "download_url" {
        let destination = args
            .and_then(|arguments| string_field(arguments, "destination"))
            .map(|destination| resolve_requested_path(invocation.workdir.as_path(), destination))
            .unwrap_or_else(|| {
                invocation
                    .workdir
                    .join("__pioneer_download_destination__")
                    .display()
                    .to_string()
            });
        scope.entries.insert("destination".to_owned(), destination);
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
        resolve_requested_path(invocation.workdir.as_path(), raw_path),
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
        resolve_requested_path(invocation.workdir.as_path(), raw_path),
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

fn file_write_intent(invocation: &ToolInvocation, tool_name: &'static str) -> PermissionIntent {
    let args = function_arguments(invocation);
    let operation = match args.and_then(|arguments| bool_field(arguments, "overwrite")) {
        Some(true) => "overwrite file",
        Some(false) => "create file",
        None => "write file",
    };
    file_mutation_intent(invocation, tool_name, operation)
}

fn file_mutation_intent(
    invocation: &ToolInvocation,
    tool_name: &'static str,
    operation: &'static str,
) -> PermissionIntent {
    let args = function_arguments(invocation);
    let mut scope = base_scope(invocation);
    scope
        .entries
        .insert("operation".to_owned(), operation.to_owned());
    if let Some(raw_path) = args.and_then(|arguments| string_field(arguments, "path")) {
        scope.entries.insert(
            "path".to_owned(),
            resolve_requested_path(invocation.workdir.as_path(), raw_path),
        );
    }

    PermissionIntent {
        action: PermissionActionKind::FileWrite,
        scope,
        summary: Some(format!("{operation} via `{tool_name}`")),
    }
}

fn apply_patch_intent(invocation: &ToolInvocation) -> PermissionIntent {
    let mut scope = base_scope(invocation);
    scope
        .entries
        .insert("operation".to_owned(), "apply patch".to_owned());

    let Ok(patch) = extract_patch_input(&invocation.payload) else {
        scope
            .entries
            .insert("parse_status".to_owned(), "missing_patch_input".to_owned());
        return PermissionIntent {
            action: PermissionActionKind::Unknown,
            scope,
            summary: Some("apply patch with unreadable patch input".to_owned()),
        };
    };

    let Ok(changed_paths) = validate_patch_document(patch) else {
        scope.entries.insert(
            "parse_status".to_owned(),
            "invalid_patch_document".to_owned(),
        );
        return PermissionIntent {
            action: PermissionActionKind::Unknown,
            scope,
            summary: Some("apply malformed patch".to_owned()),
        };
    };

    let resolved_paths = changed_paths
        .iter()
        .map(|path| resolve_requested_path(invocation.workdir.as_path(), path))
        .collect::<Vec<_>>();
    let operations = patch_operation_kinds(patch);

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
    for (index, path) in resolved_paths.iter().take(20).enumerate() {
        scope.entries.insert(format!("path.{index}"), path.clone());
    }
    if !operations.is_empty() {
        scope.entries.insert(
            "operations".to_owned(),
            serde_json::to_string(&operations).unwrap_or_else(|_| "[]".to_owned()),
        );
    }

    PermissionIntent {
        action: PermissionActionKind::FileWrite,
        scope,
        summary: Some(format!(
            "apply patch touching {} path(s)",
            resolved_paths.len()
        )),
    }
}

fn patch_operation_kinds(patch: &str) -> Vec<String> {
    let mut operations = BTreeMap::<&'static str, ()>::new();
    for line in patch.lines() {
        if line.starts_with("*** Add File: ") {
            operations.insert("add", ());
        } else if line.starts_with("*** Update File: ") {
            operations.insert("update", ());
        } else if line.starts_with("*** Delete File: ") {
            operations.insert("delete", ());
        } else if line.starts_with("*** Move to: ") {
            operations.insert("move", ());
        }
    }
    operations
        .into_keys()
        .map(str::to_owned)
        .collect::<Vec<_>>()
}

fn base_scope(invocation: &ToolInvocation) -> PermissionRequestScope {
    PermissionRequestScope::from_pairs([
        ("tool_name", invocation.tool_name.as_str()),
        ("source", invocation.source.as_str()),
    ])
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

fn bool_field(value: &JsonValue, key: &str) -> Option<bool> {
    value.as_object()?.get(key)?.as_bool()
}

fn u64_field(value: &JsonValue, key: &str) -> Option<u64> {
    value.as_object()?.get(key)?.as_u64()
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
                normalized.pop();
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
        let behavior =
            behavior_for_action(&context.permission_profile.effective_policy, intent.action);
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
        PermissionActionKind::Internal => PermissionBehavior::Allow,
        PermissionActionKind::Unknown => policy.default_behavior,
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
    if !policy.allowed_tools.is_empty()
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
    if policy.allowed_paths.is_empty()
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
        return None;
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
    value.trim().trim_end_matches('/').to_owned()
}

fn path_is_within_allowed_scope(path: &str, allowed: &str) -> bool {
    path == allowed
        || path
            .strip_prefix(allowed)
            .is_some_and(|suffix| suffix.starts_with('/'))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{ToolCallSource, ToolPayload};
    use crate::spec::{
        DynamicSkillPermissionKind, DynamicSkillPermissionMetadata, ToolPermissionMetadata,
        ToolRecoveryMetadata,
    };
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
            cancellation: CancellationToken::new(),
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
                source_kind: "User".to_owned(),
                trust_level: "Trusted".to_owned(),
                target_tool: target_tool.map(str::to_owned),
                configured_method: configured_method.map(str::to_owned),
                configured_url: configured_url.map(str::to_owned),
            }),
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
    fn extractor_classifies_task_create_as_internal_task_management() {
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

        assert_eq!(intent.action, PermissionActionKind::Internal);
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
    fn extractor_classifies_write_file_operation_without_full_validation() {
        let invocation = invocation_for_tool(
            "write_file",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "path": "../README.md",
                    "content": "updated",
                    "overwrite": true
                }),
            },
        );
        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::FileWrite);
        assert_eq!(
            intent.scope.entries.get("path"),
            Some(&"/README.md".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("operation"),
            Some(&"overwrite file".to_owned())
        );
    }

    #[test]
    fn extractor_classifies_malformed_write_file_as_file_write() {
        let invocation = invocation_for_tool(
            "write_file",
            ToolPayload::Function {
                arguments: serde_json::json!({ "content": "missing path" }),
            },
        );
        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::FileWrite);
        assert!(!intent.scope.entries.contains_key("path"));
    }

    #[test]
    fn extractor_classifies_edit_file_as_file_write() {
        let invocation = invocation_for_tool(
            "edit_file",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "path": "src/lib.rs",
                    "old_string": "old",
                    "new_string": "new"
                }),
            },
        );
        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::FileWrite);
        assert_eq!(
            intent.scope.entries.get("operation"),
            Some(&"edit file".to_owned())
        );
    }

    #[test]
    fn supervised_profile_allows_file_read_and_asks_for_file_write() {
        let context = test_context(pioneer_protocol::TurnPermissionMode::Supervised);
        let read_invocation = invocation();
        let read_intent = extract_permission_intent(&read_invocation);
        let write_invocation = invocation_for_tool(
            "write_file",
            ToolPayload::Function {
                arguments: serde_json::json!({ "path": "src/lib.rs", "content": "updated" }),
            },
        );
        let write_intent = extract_permission_intent(&write_invocation);

        assert_eq!(
            ProfileToolPermissionEvaluator.evaluate(&context, &read_invocation, &read_intent),
            PermissionDecision::Allow {
                reason: PermissionDecisionReason::PolicyAllowsAction
            }
        );
        assert!(matches!(
            ProfileToolPermissionEvaluator.evaluate(&context, &write_invocation, &write_intent),
            PermissionDecision::Ask {
                reason: PermissionDecisionReason::PolicyRequiresApproval,
                ..
            }
        ));
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
                arguments: serde_json::json!({ "input": patch }),
            },
        );

        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::FileWrite);
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
        assert_eq!(
            intent.scope.entries.get("operations"),
            Some(&"[\"add\",\"delete\",\"update\"]".to_owned())
        );
    }

    #[test]
    fn extractor_keeps_malformed_apply_patch_approval_gated() {
        let invocation = invocation_for_tool(
            "apply_patch",
            ToolPayload::Function {
                arguments: serde_json::json!({ "input": "*** Add File: a.txt\n+hello" }),
            },
        );

        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::Unknown);
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
            Some(&"/workspace/__pioneer_download_destination__".to_owned())
        );
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
    fn extractor_classifies_read_only_mcp_hint_as_mcp_read() {
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

        assert_eq!(intent.action, PermissionActionKind::McpRead);
        assert_eq!(
            intent.scope.entries.get("server"),
            Some(&"srv_docs".to_owned())
        );
        assert_eq!(intent.scope.entries.get("tool"), Some(&"search".to_owned()));
        assert_eq!(
            intent.scope.entries.get("mcp_side_effect_class"),
            Some(&"read_only".to_owned())
        );
        assert_eq!(
            intent.scope.entries.get("mcp_requires_network"),
            Some(&"false".to_owned())
        );
    }

    #[test]
    fn restricted_profiles_allow_read_only_hint_mcp() {
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

        assert_eq!(
            ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
            PermissionDecision::Allow {
                reason: PermissionDecisionReason::PolicyAllowsAction
            }
        );
    }

    #[test]
    fn extractor_classifies_destructive_network_or_unknown_mcp_as_write_or_unknown() {
        for (read_only_hint, destructive_hint, open_world_hint, expected_class, requires_network) in [
            (Some(true), Some(true), Some(false), "write_like", "false"),
            (Some(false), Some(false), Some(true), "network_like", "true"),
            (None, None, None, "unknown", "true"),
            (Some(false), Some(false), Some(false), "unknown", "false"),
        ] {
            let invocation = invocation_for_tool(
                "mcp_repo_write",
                ToolPayload::Mcp {
                    server: "srv_repo".to_owned(),
                    tool: "write_file".to_owned(),
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
                tool: "write_file".to_owned(),
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
    fn extractor_classifies_dynamic_shell_skill_as_shell_command() {
        let mut invocation = invocation_for_tool(
            "skill_shell",
            ToolPayload::Function {
                arguments: serde_json::json!({ "command": ["echo", "hello"] }),
            },
        );
        invocation.permission_metadata =
            dynamic_skill_metadata(DynamicSkillPermissionKind::Shell, None, None, None);

        let intent = extract_permission_intent(&invocation);

        assert_eq!(intent.action, PermissionActionKind::ShellCommand);
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
    fn extractor_classifies_dynamic_function_proxy_as_dynamic_skill_tool() {
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

        assert_eq!(intent.action, PermissionActionKind::DynamicSkillTool);
        assert_eq!(
            intent.scope.entries.get("target_tool"),
            Some(&"read_file".to_owned())
        );
    }

    #[test]
    fn restricted_profiles_ask_for_dynamic_skill_function_proxy() {
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
        let context = test_context(pioneer_protocol::TurnPermissionMode::Supervised);

        assert!(matches!(
            ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
            PermissionDecision::Ask {
                reason: PermissionDecisionReason::PolicyRequiresApproval,
                ..
            }
        ));
    }

    #[test]
    fn extractor_allows_computer_use_preflight_as_internal() {
        let invocation = invocation_for_tool(
            "computer_use",
            ToolPayload::Function {
                arguments: serde_json::json!({ "action": "preflight" }),
            },
        );
        let intent = extract_permission_intent(&invocation);
        let context = test_context(pioneer_protocol::TurnPermissionMode::Supervised);

        assert_eq!(intent.action, PermissionActionKind::Internal);
        assert_eq!(
            intent.scope.entries.get("action"),
            Some(&"preflight".to_owned())
        );
        assert_eq!(
            ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
            PermissionDecision::Allow {
                reason: PermissionDecisionReason::PolicyAllowsAction
            }
        );
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
    fn internal_discovery_tools_are_allowed_in_restricted_profiles() {
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

            assert_eq!(intent.action, PermissionActionKind::Internal);
            assert_eq!(
                ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
                PermissionDecision::Allow {
                    reason: PermissionDecisionReason::PolicyAllowsAction
                }
            );
        }
    }

    #[test]
    fn system_source_file_writes_do_not_bypass_file_write_policy() {
        let mut invocation = invocation_for_tool(
            "write_file",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "path": "visible.txt",
                    "content": "visible write"
                }),
            },
        );
        invocation.source = ToolCallSource::System;
        let intent = extract_permission_intent(&invocation);
        let context = test_context(pioneer_protocol::TurnPermissionMode::Supervised);

        assert_eq!(intent.action, PermissionActionKind::FileWrite);
        assert!(matches!(
            ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
            PermissionDecision::Ask {
                reason: PermissionDecisionReason::PolicyRequiresApproval,
                ..
            }
        ));
    }
}
