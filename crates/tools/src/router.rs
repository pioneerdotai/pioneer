use crate::context::{
    ExecCommandArgs, LocalShellPayload, ToolCallSource, ToolInvocation, ToolPayload, WriteStdinArgs,
};
use crate::error::ToolError;
use crate::events::{ToolEventBus, ToolEventTrace};
use crate::normalize_tool_arguments_from_schema;
use crate::orchestrator::ToolOrchestrator;
use crate::output_policy::{ToolOutputPolicySnapshot, ToolOutputProjectionKind};
use crate::registry::ToolRegistry;
use crate::spec::{
    ConfiguredToolSpec, ExecutionClass, PayloadKind, ToolIdempotencyMode, ToolPayloadBinding,
    ToolRecoveryMetadata, ToolSpec,
};
use crate::visibility::ToolVisibilitySnapshot;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
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
}

impl ToolRouter {
    pub fn new(
        specs: Vec<ConfiguredToolSpec>,
        registry: ToolRegistry,
        visibility: ToolVisibilitySnapshot,
        event_bus: ToolEventBus,
        turn_id: impl Into<String>,
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
        }
    }

    pub fn has_handler(&self, tool_name: &str) -> bool {
        self.registry.has_handler(tool_name)
    }

    pub fn all_specs(&self) -> Vec<ToolSpec> {
        self.specs.values().map(|spec| spec.spec.clone()).collect()
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
            cancellation,
        };
        orchestrator.run(&self.registry, invocation, trace).await
    }

    fn parse_payload(
        configured: &ConfiguredToolSpec,
        tool_name: &str,
        arguments: &str,
    ) -> Result<(ToolPayload, Vec<crate::ToolArgumentCoercion>), ToolError> {
        match configured.spec.payload_kind {
            PayloadKind::Function => {
                let parsed = parse_json_arguments(arguments)?;
                let normalized =
                    normalize_tool_arguments_from_schema(parsed, &configured.spec.parameters)?;
                Ok((
                    ToolPayload::Function {
                        arguments: normalized.arguments,
                    },
                    normalized.coercions,
                ))
            }
            PayloadKind::Mcp => {
                let parsed = parse_json_arguments(arguments)?;
                let normalized =
                    normalize_tool_arguments_from_schema(parsed, &configured.spec.parameters)?;
                match &configured.payload_binding {
                    ToolPayloadBinding::Mcp {
                        server_id,
                        raw_tool_name,
                        ..
                    } => Ok((
                        ToolPayload::Mcp {
                            server: server_id.clone(),
                            tool: raw_tool_name.clone(),
                            arguments: normalized.arguments,
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
            ToolIdempotencyMode::Safe | ToolIdempotencyMode::RequiresKey => {
                Some(format!("{tool_name}:{call_id}"))
            }
            ToolIdempotencyMode::SessionBound => Self::derive_session_scope_key(tool_name, payload)
                .or_else(|| Some(format!("{tool_name}:{call_id}"))),
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
            } => {
                assert_eq!(server, "srv_1");
                assert_eq!(tool, "send");
                assert_eq!(arguments["to"], "a@example.com");
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
