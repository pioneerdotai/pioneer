use crate::ToolExtensionBundle;
use crate::context::{FunctionToolOutput, ToolInvocation, ToolOutput, ToolPayload};
use crate::error::ToolError;
use crate::events::ToolEventTrace;
use crate::mcp_policy::{enforce_mcp_network_policy, mcp_policy_classification_metadata};
use crate::output_policy::{ToolOutputProjectionKind, mcp_output_policy};
use crate::registry::ToolHandler;
use crate::spec::{
    ConfiguredToolSpec, ExecutionClass, PayloadKind, ToolIdempotencyMode, ToolPayloadBinding,
    ToolRecoveryMetadata, ToolRetryClass, ToolSpec,
};
use async_trait::async_trait;
use pioneer_mcp::{McpToolSafetyHints, classify_mcp_tool_policy};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

const DEFAULT_MCP_TOOL_TIMEOUT_MS: u64 = 20_000;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpDynamicToolAnnotations {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only_hint: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destructive_hint: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotent_hint: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_world_hint: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct McpDynamicToolDescriptor {
    pub callable_name: String,
    pub workspace_id: String,
    pub server_id: String,
    pub server_name: String,
    pub raw_tool_name: String,
    pub catalog_version: String,
    pub fingerprint: String,
    pub snapshot_version: u64,
    pub description: String,
    pub parameters: JsonValue,
    pub annotations: McpDynamicToolAnnotations,
    pub timeout_ms: Option<u64>,
    /// Maximum serialized argument payload accepted at the MCP transport
    /// boundary for this materialized tool.
    pub max_arguments_bytes: usize,
    pub selection_reason: String,
    pub capability_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpDynamicToolBinding {
    pub server_installation_id: String,
    pub server_name: String,
    pub raw_tool_name: String,
    pub callable_name: String,
    pub catalog_version: String,
    pub fingerprint: String,
    pub selection_reason: String,
    pub capability_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExcludedMcpRuntimeTool {
    pub callable_name: String,
    pub server_name: String,
    pub raw_tool_name: String,
    pub reason: String,
}

#[derive(Clone, Default)]
pub struct McpRuntimeToolMaterialization {
    pub bundles: Vec<ToolExtensionBundle>,
    pub bindings: Vec<McpDynamicToolBinding>,
    pub excluded_tools: Vec<ExcludedMcpRuntimeTool>,
}

#[derive(Debug, Clone)]
pub struct McpToolCallRequest {
    pub workspace_id: String,
    pub turn_id: String,
    pub call_id: String,
    pub callable_name: String,
    pub server_id: String,
    pub server_name: String,
    pub raw_tool_name: String,
    pub catalog_version: String,
    pub arguments: JsonValue,
    pub timeout_ms: u64,
    pub max_arguments_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpToolCallOutput {
    #[serde(default)]
    pub content: JsonValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_content: Option<JsonValue>,
    pub is_error: bool,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonValue>,
}

#[async_trait]
pub trait McpToolExecutor: Send + Sync {
    async fn call_mcp_tool(
        &self,
        request: McpToolCallRequest,
        trace: ToolEventTrace,
        cancellation: CancellationToken,
    ) -> Result<McpToolCallOutput, ToolError>;
}

pub fn materialize_mcp_runtime_tools(
    descriptors: &[McpDynamicToolDescriptor],
    executor: Arc<dyn McpToolExecutor>,
) -> McpRuntimeToolMaterialization {
    let mut bundle = ToolExtensionBundle::default();
    let mut bindings = Vec::new();
    let mut excluded_tools = Vec::new();

    for descriptor in descriptors {
        if descriptor.callable_name.trim().is_empty() {
            excluded_tools.push(excluded(descriptor, "MCP callable name is empty"));
            continue;
        }
        if descriptor.server_id.trim().is_empty() {
            excluded_tools.push(excluded(descriptor, "MCP server id is empty"));
            continue;
        }
        if descriptor.raw_tool_name.trim().is_empty() {
            excluded_tools.push(excluded(descriptor, "MCP raw tool name is empty"));
            continue;
        }

        if !descriptor.parameters.is_object() {
            excluded_tools.push(excluded(
                descriptor,
                "MCP input schema must be a JSON object",
            ));
            continue;
        }
        let parameters = descriptor.parameters.clone();
        let recovery = recovery_for_annotations(&descriptor.annotations);
        let spec = ToolSpec::new(
            descriptor.callable_name.clone(),
            description_for_descriptor(descriptor),
            parameters,
            PayloadKind::Mcp,
        )
        .with_recovery(recovery);
        let configured = ConfiguredToolSpec::with_output_projection(
            spec,
            ExecutionClass::SessionScoped,
            mcp_output_policy(),
            ToolOutputProjectionKind::DynamicMcp,
        )
        .with_payload_binding(ToolPayloadBinding::Mcp {
            server_id: descriptor.server_id.clone(),
            server_name: descriptor.server_name.clone(),
            raw_tool_name: descriptor.raw_tool_name.clone(),
            catalog_version: descriptor.catalog_version.clone(),
            snapshot_version: descriptor.snapshot_version,
            read_only_hint: descriptor.annotations.read_only_hint,
            destructive_hint: descriptor.annotations.destructive_hint,
            open_world_hint: descriptor.annotations.open_world_hint,
        });

        bindings.push(McpDynamicToolBinding {
            server_installation_id: descriptor.server_id.clone(),
            server_name: descriptor.server_name.clone(),
            raw_tool_name: descriptor.raw_tool_name.clone(),
            callable_name: descriptor.callable_name.clone(),
            catalog_version: descriptor.catalog_version.clone(),
            fingerprint: descriptor.fingerprint.clone(),
            selection_reason: descriptor.selection_reason.clone(),
            capability_id: descriptor.capability_id.clone(),
        });
        bundle.specs.push(configured);
        bundle.handlers.push((
            descriptor.callable_name.clone(),
            Arc::new(McpToolHandler {
                descriptor: descriptor.clone(),
                executor: executor.clone(),
            }),
        ));
    }

    let bundles = if bundle.specs.is_empty() {
        Vec::new()
    } else {
        vec![bundle]
    };

    McpRuntimeToolMaterialization {
        bundles,
        bindings,
        excluded_tools,
    }
}

fn excluded(
    descriptor: &McpDynamicToolDescriptor,
    reason: impl Into<String>,
) -> ExcludedMcpRuntimeTool {
    ExcludedMcpRuntimeTool {
        callable_name: descriptor.callable_name.clone(),
        server_name: descriptor.server_name.clone(),
        raw_tool_name: descriptor.raw_tool_name.clone(),
        reason: reason.into(),
    }
}

fn description_for_descriptor(descriptor: &McpDynamicToolDescriptor) -> String {
    let mut parts = Vec::new();
    if let Some(title) = descriptor.annotations.title.as_deref() {
        let title = title.trim();
        if !title.is_empty() {
            parts.push(title.to_owned());
        }
    }
    let description = descriptor.description.trim();
    if !description.is_empty() && !parts.iter().any(|part| part == description) {
        parts.push(description.to_owned());
    }
    parts.push(format!(
        "MCP server `{}` raw tool `{}`.",
        descriptor.server_name, descriptor.raw_tool_name
    ));
    parts.join("\n")
}

fn recovery_for_annotations(annotations: &McpDynamicToolAnnotations) -> ToolRecoveryMetadata {
    if annotations.read_only_hint == Some(true) {
        return ToolRecoveryMetadata {
            retry_class: ToolRetryClass::Network,
            idempotency_mode: ToolIdempotencyMode::Safe,
            max_attempts: 2,
            can_resume: false,
            max_wall_clock_secs: None,
        };
    }

    if annotations.idempotent_hint == Some(true) {
        return ToolRecoveryMetadata {
            retry_class: ToolRetryClass::Network,
            idempotency_mode: ToolIdempotencyMode::Safe,
            max_attempts: 2,
            can_resume: false,
            max_wall_clock_secs: None,
        };
    }

    ToolRecoveryMetadata {
        retry_class: ToolRetryClass::Never,
        idempotency_mode: ToolIdempotencyMode::None,
        max_attempts: 1,
        can_resume: false,
        max_wall_clock_secs: None,
    }
}

#[derive(Clone)]
struct McpToolHandler {
    descriptor: McpDynamicToolDescriptor,
    executor: Arc<dyn McpToolExecutor>,
}

#[async_trait]
impl ToolHandler for McpToolHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        trace: ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let arguments = match &invocation.payload {
            ToolPayload::Mcp { arguments, .. } => arguments.clone(),
            _ => {
                return Err(ToolError::invalid_arguments(
                    "MCP handler received non-MCP payload",
                ));
            }
        };
        let classification = classify_mcp_tool_policy(McpToolSafetyHints {
            read_only_hint: self.descriptor.annotations.read_only_hint,
            destructive_hint: self.descriptor.annotations.destructive_hint,
            open_world_hint: self.descriptor.annotations.open_world_hint,
        });
        let stage_metadata = mcp_stage_metadata(&self.descriptor);
        trace.emit_stage(
            1,
            "mcp.policy.classified",
            None,
            Some(stage_metadata.clone()),
        );
        if let Err(error) = enforce_mcp_network_policy(
            invocation.execution_security_snapshot.as_ref(),
            &classification,
            self.descriptor.server_name.as_str(),
            self.descriptor.raw_tool_name.as_str(),
        ) {
            trace.emit_stage(
                1,
                "mcp.policy.denied",
                Some(error.to_string()),
                Some(stage_metadata),
            );
            return Err(error);
        }

        let stage_metadata = mcp_stage_metadata(&self.descriptor);
        trace.emit_stage(1, "mcp.call.prepare", None, Some(stage_metadata.clone()));
        trace.emit_stage(1, "mcp.call.started", None, Some(stage_metadata.clone()));

        let output = match self
            .executor
            .call_mcp_tool(
                mcp_call_request(
                    &self.descriptor,
                    trace.turn_id(),
                    invocation.call_id.as_str(),
                    arguments,
                ),
                trace.clone(),
                invocation.cancellation.clone(),
            )
            .await
        {
            Ok(output) => {
                trace.emit_stage(
                    1,
                    "mcp.call.completed",
                    None,
                    Some(mcp_completed_stage_metadata(&self.descriptor, &output)),
                );
                output
            }
            Err(error) => {
                trace.emit_stage(
                    1,
                    "mcp.call.failed",
                    Some(error.to_string()),
                    Some(stage_metadata),
                );
                return Err(error);
            }
        };

        let text = render_mcp_tool_text(&output);
        let payload = serde_json::json!({
            "mcp": {
                "serverId": self.descriptor.server_id,
                "serverName": self.descriptor.server_name,
                "rawToolName": self.descriptor.raw_tool_name,
                "callableName": self.descriptor.callable_name,
                "catalogVersion": self.descriptor.catalog_version,
                "snapshotVersion": self.descriptor.snapshot_version,
                "durationMs": output.duration_ms,
                "isError": output.is_error,
            },
            "content": output.content,
            "structuredContent": output.structured_content,
            "isError": output.is_error,
            "durationMs": output.duration_ms,
            "meta": output.meta,
        });

        Ok(Box::new(FunctionToolOutput::with_payload(
            text,
            !output.is_error,
            payload,
        )))
    }
}

fn mcp_call_request(
    descriptor: &McpDynamicToolDescriptor,
    turn_id: &str,
    call_id: &str,
    arguments: JsonValue,
) -> McpToolCallRequest {
    McpToolCallRequest {
        workspace_id: descriptor.workspace_id.clone(),
        turn_id: turn_id.to_owned(),
        call_id: call_id.to_owned(),
        callable_name: descriptor.callable_name.clone(),
        server_id: descriptor.server_id.clone(),
        server_name: descriptor.server_name.clone(),
        raw_tool_name: descriptor.raw_tool_name.clone(),
        catalog_version: descriptor.catalog_version.clone(),
        arguments,
        timeout_ms: descriptor
            .timeout_ms
            .unwrap_or(DEFAULT_MCP_TOOL_TIMEOUT_MS)
            .max(1),
        max_arguments_bytes: descriptor.max_arguments_bytes,
    }
}

fn mcp_stage_metadata(descriptor: &McpDynamicToolDescriptor) -> JsonValue {
    let classification = classify_mcp_tool_policy(McpToolSafetyHints {
        read_only_hint: descriptor.annotations.read_only_hint,
        destructive_hint: descriptor.annotations.destructive_hint,
        open_world_hint: descriptor.annotations.open_world_hint,
    });
    serde_json::json!({
        "source": "mcp",
        "mcp": {
            "serverId": descriptor.server_id,
            "serverName": descriptor.server_name,
            "rawToolName": descriptor.raw_tool_name,
            "callableName": descriptor.callable_name,
            "catalogVersion": descriptor.catalog_version,
            "snapshotVersion": descriptor.snapshot_version,
            "policy": mcp_policy_classification_metadata(&classification),
        }
    })
}

fn mcp_completed_stage_metadata(
    descriptor: &McpDynamicToolDescriptor,
    output: &McpToolCallOutput,
) -> JsonValue {
    let mut metadata = mcp_stage_metadata(descriptor);
    if let Some(mcp) = metadata
        .get_mut("mcp")
        .and_then(serde_json::Value::as_object_mut)
    {
        mcp.insert(
            "durationMs".to_owned(),
            serde_json::json!(output.duration_ms),
        );
        mcp.insert("isError".to_owned(), serde_json::json!(output.is_error));
    }
    metadata
}

fn render_mcp_tool_text(output: &McpToolCallOutput) -> String {
    if let Some(structured) = output.structured_content.as_ref() {
        return serde_json::to_string_pretty(structured).unwrap_or_else(|_| structured.to_string());
    }
    serde_json::to_string_pretty(&output.content).unwrap_or_else(|_| output.content.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{ToolIdempotencyMode, ToolPayloadBinding, ToolRetryClass};

    struct NoopExecutor;

    #[async_trait]
    impl McpToolExecutor for NoopExecutor {
        async fn call_mcp_tool(
            &self,
            _request: McpToolCallRequest,
            _trace: ToolEventTrace,
            _cancellation: CancellationToken,
        ) -> Result<McpToolCallOutput, ToolError> {
            unreachable!("materialization tests do not execute the handler")
        }
    }

    fn descriptor(parameters: JsonValue) -> McpDynamicToolDescriptor {
        McpDynamicToolDescriptor {
            callable_name: "mcp_resend_send".to_owned(),
            workspace_id: "workspace".to_owned(),
            server_id: "installation".to_owned(),
            server_name: "resend".to_owned(),
            raw_tool_name: "send".to_owned(),
            catalog_version: "catalog-v1".to_owned(),
            fingerprint: "installation-fingerprint".to_owned(),
            snapshot_version: 17,
            description: "Send an email".to_owned(),
            parameters,
            annotations: McpDynamicToolAnnotations {
                title: Some("Send".to_owned()),
                read_only_hint: Some(false),
                destructive_hint: Some(true),
                idempotent_hint: Some(true),
                open_world_hint: Some(true),
            },
            timeout_ms: Some(4_321),
            max_arguments_bytes: 128 * 1024,
            selection_reason: "explicit_composer_capability".to_owned(),
            capability_id: Some("mcp-tool:workspace:resend:send".to_owned()),
        }
    }

    #[test]
    fn mcp_api_materialization_preserves_projection_descriptor_exactly() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {"to": {"type": "string"}},
            "required": ["to"],
            "additionalProperties": false
        });
        let descriptor = descriptor(schema.clone());
        let materialized = materialize_mcp_runtime_tools(
            std::slice::from_ref(&descriptor),
            Arc::new(NoopExecutor),
        );

        assert!(materialized.excluded_tools.is_empty());
        assert_eq!(materialized.bundles.len(), 1);
        let configured = &materialized.bundles[0].specs[0];
        assert_eq!(configured.spec.name, descriptor.callable_name);
        assert_eq!(configured.spec.parameters, schema);
        assert_eq!(
            configured.spec.recovery.retry_class,
            ToolRetryClass::Network
        );
        assert_eq!(
            configured.spec.recovery.idempotency_mode,
            ToolIdempotencyMode::Safe
        );
        assert_eq!(
            configured.payload_binding,
            ToolPayloadBinding::Mcp {
                server_id: descriptor.server_id.clone(),
                server_name: descriptor.server_name.clone(),
                raw_tool_name: descriptor.raw_tool_name.clone(),
                catalog_version: descriptor.catalog_version.clone(),
                snapshot_version: descriptor.snapshot_version,
                read_only_hint: descriptor.annotations.read_only_hint,
                destructive_hint: descriptor.annotations.destructive_hint,
                open_world_hint: descriptor.annotations.open_world_hint,
            }
        );
        assert_eq!(materialized.bindings.len(), 1);
        assert_eq!(
            materialized.bindings[0].callable_name,
            descriptor.callable_name
        );
        assert_eq!(
            materialized.bindings[0].capability_id,
            descriptor.capability_id
        );

        let call = mcp_call_request(&descriptor, "turn", "call", serde_json::json!({"to":"a"}));
        assert_eq!(call.callable_name, descriptor.callable_name);
        assert_eq!(call.server_id, descriptor.server_id);
        assert_eq!(call.raw_tool_name, descriptor.raw_tool_name);
        assert_eq!(call.catalog_version, descriptor.catalog_version);
        assert_eq!(call.timeout_ms, 4_321);
    }

    #[test]
    fn mcp_api_materialization_rejects_schema_drift_instead_of_rewriting_it() {
        let descriptor = descriptor(serde_json::json!("not-an-object-schema"));
        let materialized = materialize_mcp_runtime_tools(
            std::slice::from_ref(&descriptor),
            Arc::new(NoopExecutor),
        );

        assert!(materialized.bundles.is_empty());
        assert!(materialized.bindings.is_empty());
        assert_eq!(materialized.excluded_tools.len(), 1);
        assert!(
            materialized.excluded_tools[0]
                .reason
                .contains("schema must be a JSON object")
        );
    }
}
