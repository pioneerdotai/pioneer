use crate::context::{
    ExecCommandArgs, LocalShellPayload, ToolCallSource, ToolInvocation, ToolPayload, WriteStdinArgs,
};
use crate::domain::parse_request_tools_domains;
use crate::error::ToolError;
use crate::events::{ToolEventBus, ToolEventTrace};
use crate::normalize_tool_arguments_for_tool;
use crate::orchestrator::ToolOrchestrator;
use crate::output_policy::{ToolOutputPolicySnapshot, ToolOutputProjectionKind};
use crate::permissions::PermissionEvaluationContext;
use crate::registry::ToolRegistry;
use crate::spec::{
    ConfiguredToolSpec, ExecutionClass, PayloadKind, REQUEST_TOOLS_TOOL_NAME, ToolIdempotencyMode,
    ToolPayloadBinding, ToolPermissionMetadata, ToolRecoveryMetadata, ToolSpec,
};
use crate::tool_index::{PreflightToolIndex, build_preflight_tool_index};
use crate::visibility::{
    FinalToolVisibility, FinalToolVisibilityInput, ToolVisibilitySnapshot,
    compute_final_tool_visibility, materialized_dynamic_extension_tool_names,
};
use pioneer_protocol::TurnExecutionSecuritySnapshot;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct RawToolCall {
    pub call_id: String,
    pub tool_name: String,
    pub arguments: String,
}

#[derive(Debug, Clone)]
pub struct ToolCall {
    pub call_id: String,
    pub tool_name: String,
    pub payload: ToolPayload,
    pub execution_class: ExecutionClass,
    pub recovery: ToolRecoveryMetadata,
    pub permission_metadata: ToolPermissionMetadata,
    pub output_policy: ToolOutputPolicySnapshot,
    pub output_projection: ToolOutputProjectionKind,
    pub idempotency_key: Option<String>,
    pub trace_id: String,
    pub session_scope_key: Option<String>,
}

impl ToolCall {
    pub fn session_scope_key(&self) -> Option<String> {
        self.session_scope_key.clone()
    }
}

pub struct ToolRouter {
    registry: ToolRegistry,
    specs: HashMap<String, ConfiguredToolSpec>,
    visibility: ToolVisibilitySnapshot,
    event_bus: ToolEventBus,
    turn_id: String,
    blocked_tool_names: Arc<RwLock<BTreeMap<String, String>>>,
}

impl ToolRouter {
    pub fn new(
        specs: Vec<ConfiguredToolSpec>,
        registry: ToolRegistry,
        visibility: ToolVisibilitySnapshot,
        event_bus: ToolEventBus,
        turn_id: impl Into<String>,
    ) -> Self {
        Self::new_with_blocked_tool_names(
            specs,
            registry,
            visibility,
            event_bus,
            turn_id,
            Arc::new(RwLock::new(BTreeMap::new())),
        )
    }

    pub fn new_with_blocked_tool_names(
        specs: Vec<ConfiguredToolSpec>,
        registry: ToolRegistry,
        visibility: ToolVisibilitySnapshot,
        event_bus: ToolEventBus,
        turn_id: impl Into<String>,
        blocked_tool_names: Arc<RwLock<BTreeMap<String, String>>>,
    ) -> Self {
        let specs = specs
            .into_iter()
            .map(|spec| (spec.spec.name.clone(), spec))
            .collect();
        Self {
            registry,
            specs,
            visibility,
            event_bus,
            turn_id: turn_id.into(),
            blocked_tool_names,
        }
    }

    pub fn set_blocked_tool_names(
        &self,
        blocked_tool_names: impl IntoIterator<Item = (String, String)>,
    ) {
        *self
            .blocked_tool_names
            .write()
            .expect("tool router blocked tool map lock poisoned") =
            blocked_tool_names.into_iter().collect();
    }

    fn blocked_tool_names_snapshot(&self) -> BTreeMap<String, String> {
        self.blocked_tool_names
            .read()
            .expect("tool router blocked tool map lock poisoned")
            .clone()
    }

    pub fn has_handler(&self, tool_name: &str) -> bool {
        self.registry.has_handler(tool_name)
    }

    pub fn all_specs(&self) -> Vec<ToolSpec> {
        self.visibility.all_specs().to_vec()
    }

    pub fn preflight_tool_index(&self) -> PreflightToolIndex {
        let blocked_tool_names = self.blocked_tool_names_snapshot();
        build_preflight_tool_index(self.specs.values().filter(|configured| {
            self.has_handler(configured.spec.name.as_str())
                && !blocked_tool_names.contains_key(configured.spec.name.as_str())
        }))
    }

    pub async fn model_visible_specs(&self) -> Vec<ToolSpec> {
        self.visibility.get().await
    }

    pub async fn is_model_visible_tool(&self, tool_name: &str) -> bool {
        self.visibility.contains_name(tool_name).await
    }

    pub async fn set_model_visible_tools(&self, names: &[String]) {
        self.visibility.set_visible_by_name(names).await;
    }

    pub fn compute_final_visible_tools(
        &self,
        core_tools: &[String],
        preflight_visible_tools: &[String],
        current_visible_tools: &[String],
    ) -> FinalToolVisibility {
        let all_tool_names = self
            .visibility
            .all_specs()
            .iter()
            .map(|spec| spec.name.clone())
            .collect::<Vec<_>>();
        let available_tool_names = all_tool_names
            .iter()
            .filter(|name| self.has_handler(name.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let dynamic_tool_names =
            materialized_dynamic_extension_tool_names(all_tool_names.clone(), core_tools.to_vec());

        compute_final_tool_visibility(FinalToolVisibilityInput {
            core_tools: core_tools.to_vec(),
            current_visible_tools: current_visible_tools.to_vec(),
            preflight_visible_tools: preflight_visible_tools.to_vec(),
            dynamic_tool_names,
            registered_tool_names: all_tool_names,
            available_tool_names,
            blocked_tool_names: self.blocked_tool_names_snapshot(),
        })
    }

    pub async fn build_model_tool_call(&self, call: RawToolCall) -> Result<ToolCall, ToolError> {
        if self.find_spec(call.tool_name.as_str()).is_some()
            && !self.is_model_visible_tool(call.tool_name.as_str()).await
        {
            return Err(ToolError::not_visible(call.tool_name));
        }

        self.build_tool_call(call)
    }

    pub fn find_spec(&self, tool_name: &str) -> Option<&ConfiguredToolSpec> {
        self.specs.get(tool_name)
    }

    pub fn has_spec_name_with_prefix(&self, prefix: &str) -> bool {
        let prefix = prefix.trim();
        if prefix.is_empty() {
            return false;
        }
        self.specs
            .keys()
            .any(|name| name.as_str() != prefix && name.starts_with(prefix))
    }

    pub fn build_tool_call(&self, call: RawToolCall) -> Result<ToolCall, ToolError> {
        let trace_id = self.event_bus.new_trace_id();
        let trace = self.event_bus.trace_with_id(
            trace_id.clone(),
            self.turn_id.clone(),
            call.call_id.clone(),
            call.tool_name.clone(),
        );
        trace.emit_stage(
            1,
            "router.parse.started",
            None,
            Some(serde_json::json!({
                "arguments_length": call.arguments.len(),
            })),
        );

        let configured = match self.find_spec(call.tool_name.as_str()) {
            Some(spec) => spec.clone(),
            None => {
                let error = ToolError::NotFound(call.tool_name.clone());
                trace.emit_stage(
                    1,
                    "router.parse.failed",
                    Some(error.to_string()),
                    Some(serde_json::json!({
                        "reason": "unknown_tool",
                    })),
                );
                return Err(error);
            }
        };

        let (payload, argument_coercions) = match Self::parse_payload(
            &configured,
            call.tool_name.as_str(),
            call.arguments.as_str(),
        ) {
            Ok(parsed) => {
                trace.emit_stage(1, "router.parse.completed", None, None);
                parsed
            }
            Err(error) => {
                trace.emit_stage(1, "router.parse.failed", Some(error.to_string()), None);
                return Err(error);
            }
        };
        if !argument_coercions.is_empty() {
            trace.emit_stage(
                1,
                "router.arguments.normalized",
                None,
                Some(serde_json::json!({
                    "coercions": argument_coercions,
                })),
            );
        }

        let call_id = call.call_id;
        let tool_name = call.tool_name;

        let session_scope_key =
            Self::derive_session_scope_key(configured.spec.name.as_str(), &payload);
        let idempotency_key = Self::derive_idempotency_key(
            configured.spec.name.as_str(),
            &payload,
            configured.spec.recovery.idempotency_mode,
            call_id.as_str(),
        );

        Ok(ToolCall {
            call_id: call_id.clone(),
            tool_name: tool_name.clone(),
            session_scope_key,
            payload,
            execution_class: configured.execution_class,
            recovery: configured.spec.recovery,
            permission_metadata: configured.spec.permission_metadata,
            output_policy: configured.output_policy,
            output_projection: configured.output_projection,
            idempotency_key,
            trace_id,
        })
    }

    pub async fn dispatch(
        &self,
        orchestrator: &ToolOrchestrator,
        call: ToolCall,
        source: ToolCallSource,
        workdir: PathBuf,
        environment: BTreeMap<String, String>,
        permission_context: &PermissionEvaluationContext,
        execution_security_snapshot: Option<TurnExecutionSecuritySnapshot>,
        trace: &ToolEventTrace,
        cancellation: CancellationToken,
    ) -> Result<crate::context::AnyToolResult, ToolError> {
        let invocation = ToolInvocation {
            call_id: call.call_id,
            tool_name: call.tool_name,
            source,
            payload: call.payload,
            workdir,
            environment,
            attempt_id: 1,
            idempotency_key: call.idempotency_key,
            recovery: call.recovery,
            permission_metadata: call.permission_metadata,
            execution_security_snapshot,
            cancellation,
        };
        orchestrator
            .run_with_context(&self.registry, invocation, trace, permission_context)
            .await
    }

    fn parse_payload(
        configured: &ConfiguredToolSpec,
        tool_name: &str,
        arguments: &str,
    ) -> Result<(ToolPayload, Vec<crate::ToolArgumentCoercion>), ToolError> {
        match configured.spec.payload_kind {
            PayloadKind::Function => {
                let parsed = parse_json_arguments(arguments)?;
                let normalized = normalize_tool_arguments_for_tool(
                    tool_name,
                    parsed,
                    &configured.spec.parameters,
                )?;
                if tool_name == REQUEST_TOOLS_TOOL_NAME {
                    parse_request_tools_domains(&normalized.arguments)
                        .map_err(ToolError::invalid_arguments)?;
                }
                Ok((
                    ToolPayload::Function {
                        arguments: normalized.arguments,
                    },
                    normalized.coercions,
                ))
            }
            PayloadKind::Mcp => {
                let parsed = parse_json_arguments(arguments)?;
                let normalized = normalize_tool_arguments_for_tool(
                    tool_name,
                    parsed,
                    &configured.spec.parameters,
                )?;
                match &configured.payload_binding {
                    ToolPayloadBinding::Mcp {
                        server_id,
                        raw_tool_name,
                        read_only_hint,
                        destructive_hint,
                        open_world_hint,
                        ..
                    } => Ok((
                        ToolPayload::Mcp {
                            server: server_id.clone(),
                            tool: raw_tool_name.clone(),
                            arguments: normalized.arguments,
                            read_only_hint: *read_only_hint,
                            destructive_hint: *destructive_hint,
                            open_world_hint: *open_world_hint,
                        },
                        normalized.coercions,
                    )),
                    ToolPayloadBinding::Function => Err(ToolError::NotFound(format!(
                        "MCP tool `{tool_name}` has no materialized binding"
                    ))),
                }
            }
            PayloadKind::LocalShell => {
                let parsed = parse_json_arguments(arguments)?;
                if tool_name == "write_stdin" {
                    let args =
                        serde_json::from_value::<WriteStdinArgs>(parsed).map_err(|error| {
                            ToolError::invalid_arguments(format!(
                                "failed to parse write_stdin arguments: {error}"
                            ))
                        })?;
                    Ok((
                        ToolPayload::LocalShell(LocalShellPayload::WriteStdin(args)),
                        Vec::new(),
                    ))
                } else {
                    let args =
                        serde_json::from_value::<ExecCommandArgs>(parsed).map_err(|error| {
                            ToolError::invalid_arguments(format!(
                                "failed to parse {tool_name} arguments: {error}"
                            ))
                        })?;
                    Ok((
                        ToolPayload::LocalShell(LocalShellPayload::ExecCommand(args)),
                        Vec::new(),
                    ))
                }
            }
            PayloadKind::ToolSearch => {
                let parsed = parse_json_arguments(arguments)?;
                let query = parsed
                    .get("query")
                    .and_then(JsonValue::as_str)
                    .or_else(|| parsed.get("q").and_then(JsonValue::as_str))
                    .or_else(|| parsed.get("intent").and_then(JsonValue::as_str))
                    .unwrap_or_default()
                    .to_owned();
                let limit = parsed
                    .get("limit")
                    .and_then(JsonValue::as_u64)
                    .and_then(|value| usize::try_from(value).ok());
                let include_hidden = parsed.get("include_hidden").and_then(JsonValue::as_bool);
                Ok((
                    ToolPayload::ToolSearch {
                        query,
                        limit,
                        include_hidden,
                    },
                    Vec::new(),
                ))
            }
            PayloadKind::Custom => {
                let parsed = parse_json_arguments(arguments)
                    .unwrap_or_else(|_| JsonValue::String(arguments.to_owned()));
                if let Some(input) = parsed.get("input").and_then(JsonValue::as_str) {
                    return Ok((
                        ToolPayload::Custom {
                            input: input.to_owned(),
                        },
                        Vec::new(),
                    ));
                }
                if let Some(input) = parsed.get("patch").and_then(JsonValue::as_str) {
                    return Ok((
                        ToolPayload::Custom {
                            input: input.to_owned(),
                        },
                        Vec::new(),
                    ));
                }
                if let Some(input) = parsed.as_str() {
                    return Ok((
                        ToolPayload::Custom {
                            input: input.to_owned(),
                        },
                        Vec::new(),
                    ));
                }
                Ok((
                    ToolPayload::Custom {
                        input: arguments.to_owned(),
                    },
                    Vec::new(),
                ))
            }
        }
    }

    fn derive_session_scope_key(tool_name: &str, payload: &ToolPayload) -> Option<String> {
        match payload {
            ToolPayload::Mcp { server, .. } => Some(format!("mcp:{server}")),
            ToolPayload::LocalShell(LocalShellPayload::WriteStdin(args)) => {
                Some(format!("shell:{}", args.session_id))
            }
            ToolPayload::Function { arguments } if tool_name == "computer_use" => arguments
                .get("session_id")
                .and_then(JsonValue::as_u64)
                .map(|id| format!("computer_use:{id}")),
            ToolPayload::Function { arguments } if tool_name == "download_url" => arguments
                .get("destination")
                .and_then(JsonValue::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(|value| format!("download:{value}"))
                .or_else(|| {
                    arguments
                        .get("url")
                        .and_then(JsonValue::as_str)
                        .filter(|value| !value.trim().is_empty())
                        .map(|value| format!("download:{value}"))
                }),
            _ => None,
        }
    }

    fn derive_idempotency_key(
        tool_name: &str,
        payload: &ToolPayload,
        idempotency_mode: ToolIdempotencyMode,
        call_id: &str,
    ) -> Option<String> {
        let explicit = match payload {
            ToolPayload::Function { arguments } | ToolPayload::Mcp { arguments, .. } => arguments
                .get("idempotency_key")
                .or_else(|| arguments.get("idempotencyKey"))
                .and_then(JsonValue::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned),
            _ => None,
        };

        if explicit.is_some() {
            return explicit;
        }

        match idempotency_mode {
            ToolIdempotencyMode::None => None,
            ToolIdempotencyMode::Safe => Some(format!("{tool_name}:{call_id}")),
            // A provider call id is not a durable operation key: a replay can
            // receive a different call id while targeting the same side effect.
            // RequiresKey and SessionBound tools must supply a stable key or a
            // typed session identity; otherwise the orchestrator fails closed.
            ToolIdempotencyMode::RequiresKey => None,
            ToolIdempotencyMode::SessionBound => Self::derive_session_scope_key(tool_name, payload),
        }
    }
}

fn parse_json_arguments(arguments: &str) -> Result<JsonValue, ToolError> {
    let trimmed = arguments.trim();
    if trimmed.is_empty() {
        return Ok(JsonValue::Object(serde_json::Map::new()));
    }

    serde_json::from_str::<JsonValue>(trimmed).map_err(|error| {
        ToolError::invalid_arguments(format!("failed to parse tool arguments as JSON: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::ToolRegistry;
    use std::collections::HashMap;
    use tokio::time::{Duration, timeout};

    fn configured_spec(
        name: &str,
        payload_kind: PayloadKind,
        execution_class: ExecutionClass,
    ) -> ConfiguredToolSpec {
        ConfiguredToolSpec::new(
            ToolSpec::new(
                name,
                "test tool",
                serde_json::json!({"type":"object"}),
                payload_kind,
            ),
            execution_class,
            crate::dynamic_unknown_output_policy(),
        )
    }

    fn configured_function_spec_with_schema(name: &str, schema: JsonValue) -> ConfiguredToolSpec {
        ConfiguredToolSpec::new(
            ToolSpec::new(name, "test tool", schema, PayloadKind::Function),
            ExecutionClass::Shared,
            crate::dynamic_unknown_output_policy(),
        )
    }

    fn request_tools_configured_spec() -> ConfiguredToolSpec {
        crate::builtin_tool_specs()
            .into_iter()
            .find(|configured| configured.spec.name == REQUEST_TOOLS_TOOL_NAME)
            .expect("request_tools builtin spec should exist")
    }

    fn write_file_configured_spec() -> ConfiguredToolSpec {
        crate::builtin_tool_specs()
            .into_iter()
            .find(|configured| configured.spec.name == "write_file")
            .expect("write_file builtin spec should exist")
    }

    fn router_with_specs(specs: Vec<ConfiguredToolSpec>) -> ToolRouter {
        router_with_event_bus(specs, ToolEventBus::default())
    }

    fn router_with_event_bus(
        specs: Vec<ConfiguredToolSpec>,
        event_bus: ToolEventBus,
    ) -> ToolRouter {
        let visibility =
            ToolVisibilitySnapshot::new(specs.iter().map(|spec| spec.spec.clone()).collect());
        ToolRouter::new(
            specs,
            ToolRegistry::new(HashMap::new()),
            visibility,
            event_bus,
            "test_turn",
        )
    }

    #[test]
    fn tool_index_router_view_omits_specs_without_handlers() {
        let router = router_with_specs(vec![configured_spec(
            "memory_search",
            PayloadKind::Function,
            ExecutionClass::Shared,
        )]);

        let index = router.preflight_tool_index();

        assert!(index.core_tools.is_empty());
        assert!(index.candidate_tools.is_empty());
    }

    #[test]
    fn build_tool_call_parses_local_shell_write_stdin() {
        let router = router_with_specs(vec![configured_spec(
            "write_stdin",
            PayloadKind::LocalShell,
            ExecutionClass::SessionScoped,
        )]);

        let call = router
            .build_tool_call(RawToolCall {
                call_id: "call_1".to_owned(),
                tool_name: "write_stdin".to_owned(),
                arguments: r#"{"session_id":42,"chars":"echo hi\n"}"#.to_owned(),
            })
            .expect("tool call should parse");

        assert_eq!(call.execution_class, ExecutionClass::SessionScoped);
        assert!(!call.trace_id.is_empty());
        assert_eq!(call.session_scope_key(), Some("shell:42".to_owned()));
        match call.payload {
            ToolPayload::LocalShell(LocalShellPayload::WriteStdin(args)) => {
                assert_eq!(args.session_id, 42);
                assert_eq!(args.chars.as_deref(), Some("echo hi\n"));
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn build_tool_call_keeps_freeform_custom_payload() {
        let router = router_with_specs(vec![configured_spec(
            "apply_patch",
            PayloadKind::Custom,
            ExecutionClass::Exclusive,
        )]);
        let patch = "*** Begin Patch\n*** Add File: a.txt\n+hello\n*** End Patch";
        let call = router
            .build_tool_call(RawToolCall {
                call_id: "call_2".to_owned(),
                tool_name: "apply_patch".to_owned(),
                arguments: patch.to_owned(),
            })
            .expect("custom payload should parse");

        assert_eq!(call.execution_class, ExecutionClass::Exclusive);
        assert!(!call.trace_id.is_empty());
        match call.payload {
            ToolPayload::Custom { input } => assert_eq!(input, patch),
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn build_tool_call_normalizes_stringified_object_arguments_from_schema() {
        let router = router_with_specs(vec![configured_function_spec_with_schema(
            "schedule",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "trigger": {
                        "type": "object",
                        "properties": {
                            "kind": { "type": "string" }
                        }
                    }
                }
            }),
        )]);

        let call = router
            .build_tool_call(RawToolCall {
                call_id: "call_normalize".to_owned(),
                tool_name: "schedule".to_owned(),
                arguments: r#"{"trigger":"{\"kind\":\"cron\"}"}"#.to_owned(),
            })
            .expect("stringified object should normalize");

        match call.payload {
            ToolPayload::Function { arguments } => {
                assert_eq!(arguments["trigger"]["kind"], "cron");
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn build_tool_call_rejects_string_for_object_schema_field() {
        let router = router_with_specs(vec![configured_function_spec_with_schema(
            "schedule",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "trigger": { "type": "object" }
                }
            }),
        )]);

        let error = router
            .build_tool_call(RawToolCall {
                call_id: "call_bad_trigger".to_owned(),
                tool_name: "schedule".to_owned(),
                arguments: r#"{"trigger":"every 15 minutes"}"#.to_owned(),
            })
            .expect_err("plain string should be rejected for object field");

        assert!(error.to_string().contains("$.trigger"));
        assert!(error.to_string().contains("must be a JSON object"));
    }

    #[test]
    fn build_tool_call_normalizes_write_file_alias_arguments() {
        let router = router_with_specs(vec![write_file_configured_spec()]);

        let call = router
            .build_tool_call(RawToolCall {
                call_id: "call_write_alias".to_owned(),
                tool_name: "write_file".to_owned(),
                arguments: r#"{"file_path":"docs/example.md","contents":"hello"}"#.to_owned(),
            })
            .expect("write_file aliases should parse");

        match call.payload {
            ToolPayload::Function { arguments } => {
                assert_eq!(arguments["path"], "docs/example.md");
                assert_eq!(arguments["content"], "hello");
                assert!(arguments.get("file_path").is_none());
                assert!(arguments.get("contents").is_none());
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn build_tool_call_normalizes_write_file_filename_and_text_aliases() {
        let router = router_with_specs(vec![write_file_configured_spec()]);

        let call = router
            .build_tool_call(RawToolCall {
                call_id: "call_write_alias_2".to_owned(),
                tool_name: "write_file".to_owned(),
                arguments: r#"{"filename":"notes.txt","text":"hello"}"#.to_owned(),
            })
            .expect("write_file aliases should parse");

        match call.payload {
            ToolPayload::Function { arguments } => {
                assert_eq!(arguments["path"], "notes.txt");
                assert_eq!(arguments["content"], "hello");
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn build_tool_call_rejects_write_file_unknown_fields_after_alias_normalization() {
        let router = router_with_specs(vec![write_file_configured_spec()]);

        let error = router
            .build_tool_call(RawToolCall {
                call_id: "call_write_unknown".to_owned(),
                tool_name: "write_file".to_owned(),
                arguments: r#"{"file_path":"docs/example.md","contents":"hello","mode":"append"}"#
                    .to_owned(),
            })
            .expect_err("unknown write_file field should fail");

        assert!(matches!(error, ToolError::InvalidArguments(_)));
        assert!(error.to_string().contains("unknown field `mode`"));
    }

    #[test]
    fn build_tool_call_does_not_apply_write_file_aliases_to_other_tools() {
        let router = router_with_specs(vec![configured_function_spec_with_schema(
            "other_tool",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "content": { "type": "string" }
                }
            }),
        )]);

        let call = router
            .build_tool_call(RawToolCall {
                call_id: "call_other_alias".to_owned(),
                tool_name: "other_tool".to_owned(),
                arguments: r#"{"file_path":"docs/example.md","contents":"hello"}"#.to_owned(),
            })
            .expect("other tool should parse without write_file aliases");

        match call.payload {
            ToolPayload::Function { arguments } => {
                assert_eq!(arguments["file_path"], "docs/example.md");
                assert_eq!(arguments["contents"], "hello");
                assert!(arguments.get("path").is_none());
                assert!(arguments.get("content").is_none());
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn detects_partial_tool_name_prefixes_without_hardcoded_tool_names() {
        let router = router_with_specs(vec![
            configured_spec("task_create", PayloadKind::Function, ExecutionClass::Shared),
            configured_spec("read_file", PayloadKind::Function, ExecutionClass::Shared),
        ]);

        assert!(router.has_spec_name_with_prefix("tas"));
        assert!(router.has_spec_name_with_prefix("task_"));
        assert!(!router.has_spec_name_with_prefix("task_create"));
        assert!(!router.has_spec_name_with_prefix("unknown"));
        assert!(!router.has_spec_name_with_prefix(""));
    }

    #[tokio::test]
    async fn visibility_snapshot_can_hide_registered_specs() {
        let router = router_with_specs(vec![
            configured_spec("tool_a", PayloadKind::Function, ExecutionClass::Shared),
            configured_spec("tool_b", PayloadKind::Function, ExecutionClass::Shared),
        ]);

        router.set_model_visible_tools(&["tool_a".to_owned()]).await;
        let visible = router.model_visible_specs().await;
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].name, "tool_a");

        assert!(router.find_spec("tool_b").is_some());
    }

    #[tokio::test]
    async fn router_reports_only_currently_visible_tools() {
        let router = router_with_specs(vec![
            configured_spec("tool_a", PayloadKind::Function, ExecutionClass::Shared),
            configured_spec("tool_b", PayloadKind::Function, ExecutionClass::Shared),
        ]);

        router.set_model_visible_tools(&["tool_a".to_owned()]).await;

        assert!(router.is_model_visible_tool("tool_a").await);
        assert!(router.find_spec("tool_a").is_some());

        assert!(!router.is_model_visible_tool("tool_b").await);
        assert!(router.find_spec("tool_b").is_some());

        assert!(!router.is_model_visible_tool("unknown_tool").await);
        assert!(router.find_spec("unknown_tool").is_none());
    }

    #[tokio::test]
    async fn build_model_tool_call_rejects_hidden_registered_tool_before_argument_parsing() {
        let router = router_with_specs(vec![
            configured_spec(
                "visible_tool",
                PayloadKind::Function,
                ExecutionClass::Shared,
            ),
            configured_spec("hidden_tool", PayloadKind::Function, ExecutionClass::Shared),
        ]);

        router
            .set_model_visible_tools(&["visible_tool".to_owned()])
            .await;

        let error = router
            .build_model_tool_call(RawToolCall {
                call_id: "call_hidden".to_owned(),
                tool_name: "hidden_tool".to_owned(),
                arguments: "{not valid json".to_owned(),
            })
            .await
            .expect_err("hidden registered tool should be rejected before parsing args");

        assert!(matches!(error, ToolError::NotVisible(_)));
    }

    #[tokio::test]
    async fn build_model_tool_call_preserves_unknown_tool_not_found() {
        let router = router_with_specs(vec![configured_spec(
            "visible_tool",
            PayloadKind::Function,
            ExecutionClass::Shared,
        )]);

        router
            .set_model_visible_tools(&["visible_tool".to_owned()])
            .await;

        let error = router
            .build_model_tool_call(RawToolCall {
                call_id: "call_unknown".to_owned(),
                tool_name: "unknown_tool".to_owned(),
                arguments: "{}".to_owned(),
            })
            .await
            .expect_err("unknown tool should still be not found");

        assert!(matches!(error, ToolError::NotFound(_)));
    }

    #[tokio::test]
    async fn build_model_tool_call_preserves_visible_tool_invalid_arguments() {
        let router = router_with_specs(vec![configured_function_spec_with_schema(
            "visible_tool",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "trigger": { "type": "object" }
                }
            }),
        )]);

        router
            .set_model_visible_tools(&["visible_tool".to_owned()])
            .await;

        let error = router
            .build_model_tool_call(RawToolCall {
                call_id: "call_bad_args".to_owned(),
                tool_name: "visible_tool".to_owned(),
                arguments: r#"{"trigger":"bad"}"#.to_owned(),
            })
            .await
            .expect_err("visible tool should still validate arguments normally");

        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[test]
    fn request_tools_accepts_valid_domain_arguments() {
        let router = router_with_specs(vec![request_tools_configured_spec()]);

        let call = router
            .build_tool_call(RawToolCall {
                call_id: "call_request_tools".to_owned(),
                tool_name: REQUEST_TOOLS_TOOL_NAME.to_owned(),
                arguments: r#"{"domains":["memory","task"],"reason":"Need memory and subtasks."}"#
                    .to_owned(),
            })
            .expect("valid request_tools call should parse");

        match call.payload {
            ToolPayload::Function { arguments } => {
                assert_eq!(arguments["domains"][0], "memory");
                assert_eq!(arguments["domains"][1], "task");
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn request_tools_rejects_empty_domains() {
        let router = router_with_specs(vec![request_tools_configured_spec()]);

        let error = router
            .build_tool_call(RawToolCall {
                call_id: "call_request_tools_empty".to_owned(),
                tool_name: REQUEST_TOOLS_TOOL_NAME.to_owned(),
                arguments: r#"{"domains":[],"reason":"Need tools."}"#.to_owned(),
            })
            .expect_err("empty request_tools domains should fail");

        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[test]
    fn request_tools_rejects_unknown_domain_values() {
        let router = router_with_specs(vec![request_tools_configured_spec()]);

        let error = router
            .build_tool_call(RawToolCall {
                call_id: "call_request_tools_unknown".to_owned(),
                tool_name: REQUEST_TOOLS_TOOL_NAME.to_owned(),
                arguments: r#"{"domains":["calendar"],"reason":"Need calendar tools."}"#.to_owned(),
            })
            .expect_err("unknown request_tools domains should fail");

        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[test]
    fn request_tools_rejects_individual_tool_name_domains() {
        let router = router_with_specs(vec![request_tools_configured_spec()]);

        let error = router
            .build_tool_call(RawToolCall {
                call_id: "call_request_tools_name".to_owned(),
                tool_name: REQUEST_TOOLS_TOOL_NAME.to_owned(),
                arguments: r#"{"domains":["task_create"],"reason":"Need to create a task."}"#
                    .to_owned(),
            })
            .expect_err("request_tools must accept domains, not individual tool names");

        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[test]
    fn request_tools_rejects_missing_reason() {
        let router = router_with_specs(vec![request_tools_configured_spec()]);

        let error = router
            .build_tool_call(RawToolCall {
                call_id: "call_request_tools_missing_reason".to_owned(),
                tool_name: REQUEST_TOOLS_TOOL_NAME.to_owned(),
                arguments: r#"{"domains":["task"]}"#.to_owned(),
            })
            .expect_err("request_tools must require reason");

        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[test]
    fn request_tools_rejects_extra_properties() {
        let router = router_with_specs(vec![request_tools_configured_spec()]);

        let error = router
            .build_tool_call(RawToolCall {
                call_id: "call_request_tools_extra".to_owned(),
                tool_name: REQUEST_TOOLS_TOOL_NAME.to_owned(),
                arguments: r#"{"domains":["task"],"reason":"Need task tools.","toolNames":["task_create"]}"#.to_owned(),
            })
            .expect_err("request_tools must reject extra properties");

        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[test]
    fn request_tools_rejects_blank_reason() {
        let router = router_with_specs(vec![request_tools_configured_spec()]);

        let error = router
            .build_tool_call(RawToolCall {
                call_id: "call_request_tools_blank_reason".to_owned(),
                tool_name: REQUEST_TOOLS_TOOL_NAME.to_owned(),
                arguments: r#"{"domains":["task"],"reason":"   "}"#.to_owned(),
            })
            .expect_err("request_tools must reject blank reason");

        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[test]
    fn request_tools_rejects_overlong_reason() {
        let router = router_with_specs(vec![request_tools_configured_spec()]);
        let reason = "x".repeat(crate::domain::REQUEST_TOOLS_REASON_MAX_CHARS + 1);

        let error = router
            .build_tool_call(RawToolCall {
                call_id: "call_request_tools_overlong_reason".to_owned(),
                tool_name: REQUEST_TOOLS_TOOL_NAME.to_owned(),
                arguments: serde_json::json!({
                    "domains": ["task"],
                    "reason": reason
                })
                .to_string(),
            })
            .expect_err("request_tools must reject overlong reason");

        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[test]
    fn build_tool_call_returns_not_found_for_unknown_tool() {
        let router = router_with_specs(Vec::new());
        let error = router
            .build_tool_call(RawToolCall {
                call_id: "call_3".to_owned(),
                tool_name: "missing".to_owned(),
                arguments: "{}".to_owned(),
            })
            .expect_err("unknown tool should fail");
        assert!(matches!(error, ToolError::NotFound(_)));
    }

    #[tokio::test]
    async fn build_tool_call_failure_does_not_publish_internal_stage_events() {
        let event_bus = ToolEventBus::new(4);
        let mut events = event_bus.subscribe();
        let router = router_with_event_bus(Vec::new(), event_bus);

        let error = router
            .build_tool_call(RawToolCall {
                call_id: "call_no_stage".to_owned(),
                tool_name: "missing".to_owned(),
                arguments: "{}".to_owned(),
            })
            .expect_err("unknown tool should fail");

        assert!(matches!(error, ToolError::NotFound(_)));
        assert!(
            timeout(Duration::from_millis(25), events.recv())
                .await
                .is_err(),
            "router parse/build telemetry must not be published to ToolEventBus"
        );
    }

    #[test]
    fn computer_use_session_scope_key_is_derived_from_session_id_argument() {
        let router = router_with_specs(vec![configured_spec(
            "computer_use",
            PayloadKind::Function,
            ExecutionClass::SessionScoped,
        )]);

        let call = router
            .build_tool_call(RawToolCall {
                call_id: "call_1".to_owned(),
                tool_name: "computer_use".to_owned(),
                arguments: r#"{"action":"status","session_id":12}"#.to_owned(),
            })
            .expect("tool call should parse");

        assert_eq!(call.session_scope_key(), Some("computer_use:12".to_owned()));
    }

    #[test]
    fn download_scope_key_uses_destination_when_present() {
        let router = router_with_specs(vec![configured_spec(
            "download_url",
            PayloadKind::Function,
            ExecutionClass::SessionScoped,
        )]);

        let call = router
            .build_tool_call(RawToolCall {
                call_id: "call_1".to_owned(),
                tool_name: "download_url".to_owned(),
                arguments: r#"{"url":"https://example.com/a","destination":"/tmp/a.txt"}"#
                    .to_owned(),
            })
            .expect("tool call should parse");

        assert_eq!(
            call.session_scope_key(),
            Some("download:/tmp/a.txt".to_owned())
        );
    }

    #[test]
    fn mcp_payload_uses_materialized_binding() {
        let spec = configured_spec(
            "mcp_resend_send",
            PayloadKind::Mcp,
            ExecutionClass::SessionScoped,
        )
        .with_payload_binding(ToolPayloadBinding::Mcp {
            server_id: "srv_1".to_owned(),
            server_name: "resend".to_owned(),
            raw_tool_name: "send".to_owned(),
            catalog_version: "sha256:catalog".to_owned(),
            snapshot_version: 7,
            read_only_hint: Some(false),
            destructive_hint: Some(true),
            open_world_hint: Some(true),
        });
        let router = router_with_specs(vec![spec]);

        let call = router
            .build_tool_call(RawToolCall {
                call_id: "call_1".to_owned(),
                tool_name: "mcp_resend_send".to_owned(),
                arguments: r#"{"to":"a@example.com"}"#.to_owned(),
            })
            .expect("MCP tool call should parse");

        assert_eq!(call.session_scope_key(), Some("mcp:srv_1".to_owned()));
        match call.payload {
            ToolPayload::Mcp {
                server,
                tool,
                arguments,
                read_only_hint,
                destructive_hint,
                open_world_hint,
            } => {
                assert_eq!(server, "srv_1");
                assert_eq!(tool, "send");
                assert_eq!(arguments["to"], "a@example.com");
                assert_eq!(read_only_hint, Some(false));
                assert_eq!(destructive_hint, Some(true));
                assert_eq!(open_world_hint, Some(true));
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn mcp_payload_requires_materialized_binding() {
        let router = router_with_specs(vec![configured_spec(
            "mcp_missing_binding",
            PayloadKind::Mcp,
            ExecutionClass::SessionScoped,
        )]);

        let error = router
            .build_tool_call(RawToolCall {
                call_id: "call_1".to_owned(),
                tool_name: "mcp_missing_binding".to_owned(),
                arguments: "{}".to_owned(),
            })
            .expect_err("MCP tool without binding should fail");

        assert!(matches!(error, ToolError::NotFound(_)));
    }
}
