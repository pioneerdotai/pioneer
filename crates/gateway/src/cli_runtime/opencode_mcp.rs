//! OpenCode-facing projection of Pioneer's private per-turn MCP facade.

use crate::cli_runtime::codex_mcp::canonical_value_fingerprint;
use crate::cli_runtime::mcp::facade::{
    CliMcpFacadeBuildError, CliMcpFacadeProjection, CliMcpFacadeTool,
};
use crate::cli_runtime::mcp::limits::CliMcpFacadeProjectionLimits;
use crate::turn_mcp::projection::ResolvedMcpTurnProjection;
use pioneer_cli_agent_runtime::codex_attestation::sha256_json;
use pioneer_cli_agent_runtime::event::RuntimeEvent;
use serde_json::{Value as JsonValue, json};
use std::collections::HashSet;
use std::fmt;

pub(crate) const OPENCODE_MCP_SERVER_NAME: &str = "pioneer";
const OPENCODE_MCP_TOOL_PREFIX: &str = "pioneer_";

#[derive(Clone)]
pub(crate) struct OpenCodeMcpSessionLaunchProjection {
    pub(crate) canonical_projection: ResolvedMcpTurnProjection,
    pub(crate) provider_manifest_hash: String,
    pub(crate) provider_contract_fingerprint: String,
    semantic_restart_fingerprint: String,
}

impl fmt::Debug for OpenCodeMcpSessionLaunchProjection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenCodeMcpSessionLaunchProjection")
            .field(
                "canonical_manifest_hash",
                &self.canonical_projection.manifest_hash,
            )
            .field("provider_manifest_hash", &self.provider_manifest_hash)
            .field("tool_count", &self.canonical_projection.tools.len())
            .finish()
    }
}

impl PartialEq for OpenCodeMcpSessionLaunchProjection {
    fn eq(&self, other: &Self) -> bool {
        self.semantic_restart_fingerprint == other.semantic_restart_fingerprint
    }
}

impl Eq for OpenCodeMcpSessionLaunchProjection {}

impl OpenCodeMcpSessionLaunchProjection {
    pub(crate) fn semantic_restart_fingerprint(&self) -> &str {
        self.semantic_restart_fingerprint.as_str()
    }

    pub(crate) fn facade_projection(
        &self,
        limits: CliMcpFacadeProjectionLimits,
    ) -> Result<CliMcpFacadeProjection, CliMcpFacadeBuildError> {
        let tools = self
            .canonical_projection
            .tools
            .iter()
            .map(|tool| {
                CliMcpFacadeTool::new(
                    tool.canonical_callable_name.clone(),
                    tool.description.clone(),
                    tool.input_schema.clone(),
                    serde_json::to_value(tool.annotations.clone().unwrap_or_default())
                        .map_err(|_| CliMcpFacadeBuildError::Serialization)?,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        CliMcpFacadeProjection::new(tools, limits)
    }

    pub(crate) fn enrich_native_event(
        &self,
        runtime_id: &str,
        session_generation: u64,
        event: &mut RuntimeEvent,
    ) -> anyhow::Result<()> {
        let (native_thread_id, native_turn_id, native_item_id, item_kind, metadata) = match event {
            RuntimeEvent::ItemStarted(item) => (
                item.native_thread_id.as_deref(),
                item.native_turn_id.as_str(),
                item.native_item_id.as_str(),
                item.item_kind.as_str(),
                &mut item.metadata,
            ),
            RuntimeEvent::ItemCompleted(item) => (
                item.native_thread_id.as_deref(),
                item.native_turn_id.as_str(),
                item.native_item_id.as_str(),
                item.item_kind.as_str(),
                &mut item.metadata,
            ),
            RuntimeEvent::ItemDelta(item) => (
                item.native_thread_id.as_deref(),
                item.native_turn_id.as_str(),
                item.native_item_id.as_str(),
                item.item_kind.as_str(),
                &mut item.metadata,
            ),
            _ => return Ok(()),
        };
        if normalize_kind(item_kind) != "mcptoolcall" {
            return Ok(());
        }
        let object = metadata
            .get_or_insert_with(|| json!({}))
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("OpenCode MCP metadata must be an object"))?;
        let qualified = object
            .get("providerQualifiedName")
            .or_else(|| object.get("toolName"))
            .or_else(|| object.get("tool"))
            .and_then(JsonValue::as_str)
            .map(str::to_owned)
            .ok_or_else(|| anyhow::anyhow!("OpenCode MCP event is missing its tool name"))?;
        let canonical = qualified
            .strip_prefix(OPENCODE_MCP_TOOL_PREFIX)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!("OpenCode MCP event is outside the Pioneer namespace")
            })?;
        let tool = self
            .canonical_projection
            .tools
            .iter()
            .find(|tool| tool.canonical_callable_name == canonical)
            .ok_or_else(|| {
                anyhow::anyhow!("OpenCode MCP event is outside the frozen projection")
            })?;
        let arguments = object
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let arguments_fingerprint = canonical_value_fingerprint(&arguments)
            .map_err(|_| anyhow::anyhow!("OpenCode MCP arguments could not be fingerprinted"))?;
        let native_thread_id = native_thread_id
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow::anyhow!("OpenCode MCP event is missing session identity"))?;
        if runtime_id.is_empty() || native_turn_id.is_empty() || native_item_id.is_empty() {
            anyhow::bail!("OpenCode MCP event identity is incomplete");
        }
        let correlation = sha256_json(&json!({
            "runtimeId": runtime_id,
            "nativeThreadId": native_thread_id,
            "nativeTurnId": native_turn_id,
            "providerCallId": native_item_id,
        }))?;
        object.insert("tool".to_owned(), JsonValue::String(canonical.to_owned()));
        object.insert(
            "toolName".to_owned(),
            JsonValue::String(canonical.to_owned()),
        );
        object.insert(
            "providerQualifiedName".to_owned(),
            JsonValue::String(qualified.to_owned()),
        );
        object.insert(
            "canonicalCallableName".to_owned(),
            JsonValue::String(canonical.to_owned()),
        );
        object.insert(
            "serverInstallationId".to_owned(),
            JsonValue::String(tool.server_installation_id.clone()),
        );
        object.insert(
            "serverName".to_owned(),
            JsonValue::String(tool.server_name.clone()),
        );
        object.insert(
            "rawToolName".to_owned(),
            JsonValue::String(tool.raw_tool_name.clone()),
        );
        object.insert(
            "manifestHash".to_owned(),
            JsonValue::String(self.canonical_projection.manifest_hash.clone()),
        );
        object.insert(
            "providerManifestHash".to_owned(),
            JsonValue::String(self.provider_manifest_hash.clone()),
        );
        object.insert(
            "providerCallId".to_owned(),
            JsonValue::String(native_item_id.to_owned()),
        );
        object.insert(
            "invocationCorrelationId".to_owned(),
            JsonValue::String(correlation),
        );
        object.insert("sessionGeneration".to_owned(), session_generation.into());
        object.insert(
            "argumentsFingerprint".to_owned(),
            JsonValue::String(arguments_fingerprint),
        );
        Ok(())
    }
}

pub(crate) fn build_opencode_mcp_session_launch_projection(
    projection: ResolvedMcpTurnProjection,
    provider_contract_fingerprint: impl Into<String>,
) -> anyhow::Result<OpenCodeMcpSessionLaunchProjection> {
    let provider_contract_fingerprint = provider_contract_fingerprint.into();
    if !is_sha256_hex(projection.manifest_hash.as_str()) {
        anyhow::bail!("canonical OpenCode MCP projection is not finalized");
    }
    if !is_sha256_hex(provider_contract_fingerprint.as_str()) {
        anyhow::bail!("invalid OpenCode executable/contract fingerprint");
    }
    let mut names = HashSet::with_capacity(projection.tools.len());
    for tool in &projection.tools {
        if !names.insert(tool.canonical_callable_name.as_str()) {
            anyhow::bail!("duplicate OpenCode MCP callable name");
        }
        if !tool.input_schema.is_object() {
            anyhow::bail!("OpenCode MCP input schema must be an object");
        }
    }
    let provider_manifest_hash = sha256_json(&json!({
        "provider": "opencode",
        "canonicalManifestHash": projection.manifest_hash,
        "providerContractFingerprint": provider_contract_fingerprint,
        "serverName": OPENCODE_MCP_SERVER_NAME,
        "tools": projection.tools.iter().map(|tool| json!({
            "qualifiedName": format!("{OPENCODE_MCP_TOOL_PREFIX}{}", tool.canonical_callable_name),
            "schemaFingerprint": tool.schema_fingerprint,
        })).collect::<Vec<_>>(),
    }))?;
    let semantic_restart_fingerprint = sha256_json(&json!({
        "provider": "opencode",
        "providerManifestHash": provider_manifest_hash,
        "transport": "dynamic_local_stdio",
        "serverName": OPENCODE_MCP_SERVER_NAME,
    }))?;
    Ok(OpenCodeMcpSessionLaunchProjection {
        canonical_projection: projection,
        provider_manifest_hash,
        provider_contract_fingerprint,
        semantic_restart_fingerprint,
    })
}

fn normalize_kind(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turn_mcp::projection::{
        McpProjectionLimits, McpSelectionReason, ResolvedMcpTurnTool,
    };
    use pioneer_cli_agent_runtime::event::{RuntimeAgentMessagePhase, RuntimeItemStarted};

    fn projection(turn_id: &str) -> ResolvedMcpTurnProjection {
        let mut projection = ResolvedMcpTurnProjection::empty("workspace", turn_id);
        projection.tools.push(ResolvedMcpTurnTool {
            canonical_callable_name: String::new(),
            workspace_id: "workspace".to_owned(),
            server_installation_id: "installation".to_owned(),
            server_name: "Example MCP".to_owned(),
            raw_tool_name: "search".to_owned(),
            description: Some("Search the example service".to_owned()),
            input_schema: json!({
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"]
            }),
            annotations: None,
            timeout_ms: 20_000,
            catalog_version: "catalog".to_owned(),
            installation_fingerprint: "installation-fingerprint".to_owned(),
            schema_fingerprint: String::new(),
            runtime_generation: 1,
            selection_reason: McpSelectionReason::ExplicitTool,
            capability_id: Some("capability".to_owned()),
        });
        projection
            .finalize_identity(McpProjectionLimits::default())
            .expect("canonical projection");
        projection
    }

    #[test]
    fn launch_projection_is_semantic_across_turns_and_builds_canonical_facade() {
        let first =
            build_opencode_mcp_session_launch_projection(projection("turn-a"), "a".repeat(64))
                .expect("first projection");
        let second =
            build_opencode_mcp_session_launch_projection(projection("turn-b"), "a".repeat(64))
                .expect("second projection");

        assert_eq!(first, second);
        assert_ne!(
            first.canonical_projection.turn_id,
            second.canonical_projection.turn_id
        );
        let tool_name = first.canonical_projection.tools[0]
            .canonical_callable_name
            .clone();
        let facade = first
            .facade_projection(Default::default())
            .expect("facade projection");
        assert!(facade.contains_tool(tool_name.as_str()));
        assert!(!facade.contains_tool(format!("pioneer_{tool_name}").as_str()));
    }

    #[test]
    fn native_mcp_event_is_correlated_to_frozen_projection() {
        let projection =
            build_opencode_mcp_session_launch_projection(projection("turn-a"), "b".repeat(64))
                .expect("projection");
        let canonical = projection.canonical_projection.tools[0]
            .canonical_callable_name
            .clone();
        let qualified = format!("pioneer_{canonical}");
        let mut event = RuntimeEvent::ItemStarted(RuntimeItemStarted {
            native_thread_id: Some("session-1".to_owned()),
            native_turn_id: "turn-1".to_owned(),
            native_item_id: "call-1".to_owned(),
            item_kind: "mcpToolCall".to_owned(),
            title: None,
            phase: RuntimeAgentMessagePhase::FinalAnswer,
            metadata: Some(json!({
                "providerQualifiedName": qualified,
                "arguments": {"query": "pioneer"}
            })),
            native_item_redacted: None,
            native: None,
        });

        projection
            .enrich_native_event("opencode", 7, &mut event)
            .expect("event enrichment");

        let RuntimeEvent::ItemStarted(item) = event else {
            panic!("fixture should remain an item-start event");
        };
        let metadata = item.metadata.expect("enriched metadata");
        assert_eq!(
            metadata
                .get("canonicalCallableName")
                .and_then(JsonValue::as_str),
            Some(canonical.as_str())
        );
        assert_eq!(
            metadata
                .get("providerQualifiedName")
                .and_then(JsonValue::as_str),
            Some(format!("pioneer_{canonical}").as_str())
        );
        assert_eq!(
            metadata
                .get("serverInstallationId")
                .and_then(JsonValue::as_str),
            Some("installation")
        );
        assert_eq!(
            metadata
                .get("sessionGeneration")
                .and_then(JsonValue::as_u64),
            Some(7)
        );
        assert_eq!(
            metadata
                .get("argumentsFingerprint")
                .and_then(JsonValue::as_str)
                .map(str::len),
            Some(64)
        );
        assert_eq!(
            metadata
                .get("invocationCorrelationId")
                .and_then(JsonValue::as_str)
                .map(str::len),
            Some(64)
        );
    }

    #[test]
    fn launch_projection_rejects_unfinalized_identity() {
        let projection = ResolvedMcpTurnProjection::empty("workspace", "turn");
        let error = build_opencode_mcp_session_launch_projection(projection, "a".repeat(64))
            .expect_err("unfinalized projection must fail closed");
        assert!(error.to_string().contains("not finalized"));
    }
}
