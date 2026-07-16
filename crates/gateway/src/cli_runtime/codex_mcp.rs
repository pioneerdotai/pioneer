use crate::cli_runtime::mcp::facade::{
    CliMcpFacadeBuildError, CliMcpFacadeProjection, CliMcpFacadeProjectionLimits, CliMcpFacadeTool,
};
use crate::turn_mcp::projection::{ResolvedMcpTurnProjection, ResolvedMcpTurnTool};
use pioneer_cli_agent_runtime::codex::{
    CodexHomeOverlayPolicy, CodexManagedMcpConfigArtifact, CodexManagedMcpConfigError,
    CodexManagedMcpConfigInput, CodexManagedMcpSemanticInput, CodexManagedMcpToolIdentity,
    CodexMcpSchemaTransformer, CodexMcpSchemaTransformerError,
    codex_managed_mcp_semantic_restart_fingerprint, serialize_codex_managed_mcp_config,
};
use pioneer_cli_agent_runtime::event::RuntimeEvent;
use pioneer_cli_agent_runtime::mcp::{
    McpSchemaTransformError, TransformedMcpToolSchema, transform_mcp_tool_schema,
};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};

/// Exact Codex-facing schema projection produced before managed config is
/// serialized or an app-server process is allowed to start.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CodexMcpSchemaPreflight {
    pub(crate) canonical_manifest_hash: String,
    pub(crate) provider_manifest_hash: String,
    pub(crate) provider_contract_fingerprint: String,
    pub(crate) tools: Vec<CodexMcpTransformedTool>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CodexMcpTransformedTool {
    pub(crate) canonical_callable_name: String,
    pub(crate) canonical_schema_fingerprint: String,
    pub(crate) transformed_schema: JsonValue,
    pub(crate) transformed_schema_fingerprint: String,
    pub(crate) transform_contract_fingerprint: String,
    pub(crate) transformed_fingerprint: String,
}

#[derive(Clone)]
pub(crate) struct CodexMcpSessionLaunchProjection {
    pub(crate) canonical_projection: ResolvedMcpTurnProjection,
    pub(crate) preflight: CodexMcpSchemaPreflight,
    semantic_restart_fingerprint: String,
}

impl fmt::Debug for CodexMcpSessionLaunchProjection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexMcpSessionLaunchProjection")
            .field(
                "canonical_manifest_hash",
                &self.preflight.canonical_manifest_hash,
            )
            .field(
                "provider_manifest_hash",
                &self.preflight.provider_manifest_hash,
            )
            .field(
                "semantic_restart_fingerprint",
                &self.semantic_restart_fingerprint,
            )
            .field("tool_count", &self.preflight.tools.len())
            .finish()
    }
}

impl PartialEq for CodexMcpSessionLaunchProjection {
    fn eq(&self, other: &Self) -> bool {
        self.semantic_restart_fingerprint == other.semantic_restart_fingerprint
    }
}

impl Eq for CodexMcpSessionLaunchProjection {}

impl CodexMcpSessionLaunchProjection {
    pub(crate) fn semantic_restart_fingerprint(&self) -> &str {
        self.semantic_restart_fingerprint.as_str()
    }

    pub(crate) fn facade_projection(
        &self,
    ) -> Result<CliMcpFacadeProjection, CliMcpFacadeBuildError> {
        let tools = self
            .preflight
            .tools
            .iter()
            .map(|transformed| {
                let canonical = self
                    .canonical_projection
                    .tools
                    .iter()
                    .find(|tool| {
                        tool.canonical_callable_name == transformed.canonical_callable_name
                    })
                    .ok_or(CliMcpFacadeBuildError::InvalidToolName)?;
                CliMcpFacadeTool::new(
                    transformed.canonical_callable_name.clone(),
                    canonical.description.clone(),
                    transformed.transformed_schema.clone(),
                    serde_json::to_value(canonical.annotations.clone().unwrap_or_default())
                        .map_err(|_| CliMcpFacadeBuildError::Serialization)?,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        CliMcpFacadeProjection::new(tools, CliMcpFacadeProjectionLimits::default())
    }

    pub(crate) fn enrich_native_event(
        &self,
        runtime_id: &str,
        session_generation: u64,
        event: &mut RuntimeEvent,
    ) -> Result<Option<CodexNativeMcpItemBinding>, CodexNativeMcpEventError> {
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
            _ => return Ok(None),
        };
        if normalize_native_kind(item_kind) != "mcptoolcall" {
            return Ok(None);
        }
        let native_thread_id = native_thread_id
            .filter(|value| !value.trim().is_empty())
            .ok_or(CodexNativeMcpEventError::MissingIdentity)?;
        if native_turn_id.trim().is_empty() || native_item_id.trim().is_empty() {
            return Err(CodexNativeMcpEventError::MissingIdentity);
        }
        let object = metadata
            .get_or_insert_with(|| JsonValue::Object(Default::default()))
            .as_object_mut()
            .ok_or(CodexNativeMcpEventError::InvalidMetadata)?;
        let server = object
            .get("server")
            .and_then(JsonValue::as_str)
            .ok_or(CodexNativeMcpEventError::MissingServer)?;
        if server != "pioneer" {
            return Err(CodexNativeMcpEventError::UnmanagedServer);
        }
        let callable = object
            .get("tool")
            .or_else(|| object.get("toolName"))
            .and_then(JsonValue::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or(CodexNativeMcpEventError::MissingTool)?
            .to_owned();
        let arguments_fingerprint = canonical_value_fingerprint(
            object
                .get("arguments")
                .ok_or(CodexNativeMcpEventError::InvalidMetadata)?,
        )?;
        let tool = self
            .canonical_projection
            .tools
            .iter()
            .find(|tool| tool.canonical_callable_name == callable)
            .ok_or(CodexNativeMcpEventError::ToolOutsideProjection)?;
        self.enrich_native_metadata(
            runtime_id,
            session_generation,
            native_thread_id,
            native_turn_id,
            native_item_id,
            tool,
            object,
        )?;
        Ok(Some(CodexNativeMcpItemBinding {
            native_thread_id: native_thread_id.to_owned(),
            native_turn_id: native_turn_id.to_owned(),
            native_item_id: native_item_id.to_owned(),
            canonical_callable_name: callable,
            arguments_fingerprint,
        }))
    }

    pub(crate) fn enrich_native_progress(
        &self,
        runtime_id: &str,
        session_generation: u64,
        canonical_callable_name: &str,
        event: &mut RuntimeEvent,
    ) -> Result<(), CodexNativeMcpEventError> {
        let RuntimeEvent::ItemDelta(delta) = event else {
            return Err(CodexNativeMcpEventError::MissingIdentity);
        };
        if normalize_native_kind(delta.item_kind.as_str()) != "mcptoolcall" {
            return Err(CodexNativeMcpEventError::MissingIdentity);
        }
        let native_thread_id = delta
            .native_thread_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(CodexNativeMcpEventError::MissingIdentity)?;
        if delta.native_turn_id.trim().is_empty() || delta.native_item_id.trim().is_empty() {
            return Err(CodexNativeMcpEventError::MissingIdentity);
        }
        let tool = self
            .canonical_projection
            .tools
            .iter()
            .find(|tool| tool.canonical_callable_name == canonical_callable_name)
            .ok_or(CodexNativeMcpEventError::ToolOutsideProjection)?;
        let object = delta
            .metadata
            .get_or_insert_with(|| JsonValue::Object(Default::default()))
            .as_object_mut()
            .ok_or(CodexNativeMcpEventError::InvalidMetadata)?;
        self.enrich_native_metadata(
            runtime_id,
            session_generation,
            native_thread_id,
            delta.native_turn_id.as_str(),
            delta.native_item_id.as_str(),
            tool,
            object,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn enrich_native_metadata(
        &self,
        runtime_id: &str,
        session_generation: u64,
        native_thread_id: &str,
        native_turn_id: &str,
        native_item_id: &str,
        tool: &ResolvedMcpTurnTool,
        object: &mut serde_json::Map<String, JsonValue>,
    ) -> Result<(), CodexNativeMcpEventError> {
        let correlation_bytes = serde_json::to_vec(&serde_json::json!({
            "runtimeId": runtime_id,
            "nativeThreadId": native_thread_id,
            "nativeTurnId": native_turn_id,
            "providerCallId": native_item_id,
        }))
        .map_err(|_| CodexNativeMcpEventError::Serialization)?;
        let invocation_correlation_id = hex::encode(Sha256::digest(correlation_bytes));
        object.insert(
            "canonicalCallableName".to_owned(),
            JsonValue::String(tool.canonical_callable_name.clone()),
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
            JsonValue::String(self.preflight.canonical_manifest_hash.clone()),
        );
        object.insert(
            "providerManifestHash".to_owned(),
            JsonValue::String(self.preflight.provider_manifest_hash.clone()),
        );
        object.insert(
            "providerCallId".to_owned(),
            JsonValue::String(native_item_id.to_owned()),
        );
        object.insert(
            "invocationCorrelationId".to_owned(),
            JsonValue::String(invocation_correlation_id),
        );
        object.insert(
            "sessionGeneration".to_owned(),
            JsonValue::Number(session_generation.into()),
        );
        object.insert(
            "selectionReason".to_owned(),
            JsonValue::String(tool.selection_reason.legacy_binding_value().to_owned()),
        );
        if let Some(capability_id) = tool.capability_id.as_ref() {
            object.insert(
                "capabilityId".to_owned(),
                JsonValue::String(capability_id.clone()),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodexNativeMcpItemBinding {
    pub(crate) native_thread_id: String,
    pub(crate) native_turn_id: String,
    pub(crate) native_item_id: String,
    pub(crate) canonical_callable_name: String,
    pub(crate) arguments_fingerprint: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodexNativeMcpEventError {
    MissingIdentity,
    InvalidMetadata,
    MissingServer,
    UnmanagedServer,
    MissingTool,
    ToolOutsideProjection,
    Serialization,
}

impl fmt::Display for CodexNativeMcpEventError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::MissingIdentity => "Codex MCP item identity is incomplete",
            Self::InvalidMetadata => "Codex MCP item metadata is malformed",
            Self::MissingServer => "Codex MCP item is missing its server",
            Self::UnmanagedServer => "Codex MCP item came from an unmanaged server",
            Self::MissingTool => "Codex MCP item is missing its tool",
            Self::ToolOutsideProjection => "Codex MCP item tool is outside the frozen projection",
            Self::Serialization => "Codex MCP correlation identity could not be serialized",
        })
    }
}

impl Error for CodexNativeMcpEventError {}

fn normalize_native_kind(kind: &str) -> String {
    kind.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

pub(crate) fn canonical_value_fingerprint(
    value: &JsonValue,
) -> Result<String, CodexNativeMcpEventError> {
    let canonical = crate::turn_mcp::projection::canonical_json(value);
    let bytes =
        serde_json::to_vec(&canonical).map_err(|_| CodexNativeMcpEventError::Serialization)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

impl From<TransformedMcpToolSchema> for CodexMcpTransformedTool {
    fn from(schema: TransformedMcpToolSchema) -> Self {
        Self {
            canonical_callable_name: schema.canonical_callable_name,
            canonical_schema_fingerprint: schema.canonical_schema_fingerprint,
            transformed_schema: schema.transformed_schema,
            transformed_schema_fingerprint: schema.transformed_schema_fingerprint,
            transform_contract_fingerprint: schema.transform_contract_fingerprint,
            transformed_fingerprint: schema.transformed_fingerprint,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CodexMcpSchemaPreflightError {
    InvalidProviderContractFingerprint,
    CanonicalProjectionNotFinalized,
    DuplicateCallableName {
        callable_name: String,
    },
    IncompatibleTool {
        callable_name: String,
        error: McpSchemaTransformError,
    },
    FingerprintSerialization,
}

impl fmt::Display for CodexMcpSchemaPreflightError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProviderContractFingerprint => {
                formatter.write_str("invalid Codex provider contract fingerprint")
            }
            Self::CanonicalProjectionNotFinalized => {
                formatter.write_str("canonical MCP projection is not finalized")
            }
            Self::DuplicateCallableName { callable_name } => write!(
                formatter,
                "duplicate canonical MCP callable name `{callable_name}` in Codex preflight"
            ),
            Self::IncompatibleTool {
                callable_name,
                error,
            } => write!(
                formatter,
                "Codex schema preflight rejected `{callable_name}`: {error}"
            ),
            Self::FingerprintSerialization => {
                formatter.write_str("failed to fingerprint Codex MCP schema projection")
            }
        }
    }
}

impl Error for CodexMcpSchemaPreflightError {}

#[derive(Debug)]
pub(crate) enum CodexManagedMcpProjectionError {
    Schema(CodexMcpSchemaPreflightError),
    HelperResolution(anyhow::Error),
    Config(CodexManagedMcpConfigError),
}

impl fmt::Display for CodexManagedMcpProjectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schema(error) => write!(formatter, "Codex MCP schema preflight failed: {error}"),
            Self::HelperResolution(_) => {
                formatter.write_str("failed to resolve the signed Pioneer MCP helper")
            }
            Self::Config(error) => write!(formatter, "invalid managed Codex MCP config: {error}"),
        }
    }
}

impl Error for CodexManagedMcpProjectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Schema(error) => Some(error),
            Self::HelperResolution(error) => Some(error.as_ref()),
            Self::Config(error) => Some(error),
        }
    }
}

impl From<CodexManagedMcpConfigError> for CodexManagedMcpProjectionError {
    fn from(error: CodexManagedMcpConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<CodexMcpSchemaPreflightError> for CodexManagedMcpProjectionError {
    fn from(error: CodexMcpSchemaPreflightError) -> Self {
        Self::Schema(error)
    }
}

/// Materialize the provider schema identity from an already finalized,
/// provider-neutral projection. This function has no process or persistence
/// side effects, so every incompatibility is known before config/spawn.
pub(crate) fn preflight_codex_mcp_schemas(
    projection: &ResolvedMcpTurnProjection,
    provider_contract_fingerprint: impl Into<String>,
) -> Result<CodexMcpSchemaPreflight, CodexMcpSchemaPreflightError> {
    if projection.manifest_hash.len() != 64
        || !projection
            .manifest_hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CodexMcpSchemaPreflightError::CanonicalProjectionNotFinalized);
    }
    let transformer = CodexMcpSchemaTransformer::new(provider_contract_fingerprint.into())
        .map_err(
            |CodexMcpSchemaTransformerError::InvalidProviderContractFingerprint| {
                CodexMcpSchemaPreflightError::InvalidProviderContractFingerprint
            },
        )?;
    let mut names = HashSet::with_capacity(projection.tools.len());
    let mut tools = Vec::with_capacity(projection.tools.len());
    for tool in &projection.tools {
        if !names.insert(tool.canonical_callable_name.clone()) {
            return Err(CodexMcpSchemaPreflightError::DuplicateCallableName {
                callable_name: tool.canonical_callable_name.clone(),
            });
        }
        let transformed = transform_mcp_tool_schema(&tool.as_provider_schema_input(), &transformer)
            .map_err(|error| CodexMcpSchemaPreflightError::IncompatibleTool {
                callable_name: tool.canonical_callable_name.clone(),
                error,
            })?;
        tools.push(CodexMcpTransformedTool::from(transformed));
    }
    tools.sort_by(|left, right| {
        left.canonical_callable_name
            .cmp(&right.canonical_callable_name)
    });
    let provider_manifest_hash = provider_manifest_hash(
        projection.manifest_hash.as_str(),
        transformer.provider_contract_fingerprint(),
        tools.as_slice(),
    )?;
    Ok(CodexMcpSchemaPreflight {
        canonical_manifest_hash: projection.manifest_hash.clone(),
        provider_manifest_hash,
        provider_contract_fingerprint: transformer.provider_contract_fingerprint().to_owned(),
        tools,
    })
}

pub(crate) fn build_codex_mcp_session_launch_projection(
    projection: ResolvedMcpTurnProjection,
    provider_contract_fingerprint: impl Into<String>,
) -> Result<CodexMcpSessionLaunchProjection, CodexManagedMcpProjectionError> {
    let preflight = preflight_codex_mcp_schemas(&projection, provider_contract_fingerprint.into())?;
    let semantic_restart_fingerprint = codex_mcp_semantic_restart_fingerprint(&preflight)?;
    Ok(CodexMcpSessionLaunchProjection {
        canonical_projection: projection,
        preflight,
        semantic_restart_fingerprint,
    })
}

/// Build the exact config artifact for production. A non-empty projection can
/// only select the already-running Pioneer executable as its helper.
pub(crate) fn build_codex_managed_mcp_config(
    preflight: &CodexMcpSchemaPreflight,
    bootstrap_path: Option<&Path>,
) -> Result<CodexManagedMcpConfigArtifact, CodexManagedMcpProjectionError> {
    let helper_path = if preflight.tools.is_empty() {
        None
    } else {
        Some(
            crate::cli_runtime::config::resolve_current_pioneer_cli_mcp_helper()
                .map_err(CodexManagedMcpProjectionError::HelperResolution)?,
        )
    };
    build_codex_managed_mcp_config_with_helper(
        preflight,
        helper_path,
        bootstrap_path.map(Path::to_path_buf),
    )
}

fn build_codex_managed_mcp_config_with_helper(
    preflight: &CodexMcpSchemaPreflight,
    helper_path: Option<PathBuf>,
    bootstrap_path: Option<PathBuf>,
) -> Result<CodexManagedMcpConfigArtifact, CodexManagedMcpProjectionError> {
    let semantic = codex_managed_mcp_semantic_input(preflight);
    Ok(serialize_codex_managed_mcp_config(
        CodexManagedMcpConfigInput {
            semantic,
            helper_path,
            bootstrap_path,
        },
    )?)
}

fn codex_managed_mcp_semantic_input(
    preflight: &CodexMcpSchemaPreflight,
) -> CodexManagedMcpSemanticInput {
    CodexManagedMcpSemanticInput {
        canonical_manifest_hash: preflight.canonical_manifest_hash.clone(),
        provider_manifest_hash: preflight.provider_manifest_hash.clone(),
        provider_contract_fingerprint: preflight.provider_contract_fingerprint.clone(),
        overlay_policy_version: CodexHomeOverlayPolicy::v1().version,
        tools: preflight
            .tools
            .iter()
            .map(|tool| CodexManagedMcpToolIdentity {
                canonical_callable_name: tool.canonical_callable_name.clone(),
                canonical_schema_fingerprint: tool.canonical_schema_fingerprint.clone(),
                transformed_schema_fingerprint: tool.transformed_schema_fingerprint.clone(),
                transform_contract_fingerprint: tool.transform_contract_fingerprint.clone(),
                transformed_fingerprint: tool.transformed_fingerprint.clone(),
            })
            .collect(),
    }
}

pub(crate) fn codex_mcp_semantic_restart_fingerprint(
    preflight: &CodexMcpSchemaPreflight,
) -> Result<String, CodexManagedMcpProjectionError> {
    Ok(codex_managed_mcp_semantic_restart_fingerprint(
        codex_managed_mcp_semantic_input(preflight),
    )?)
}

fn provider_manifest_hash(
    canonical_manifest_hash: &str,
    provider_contract_fingerprint: &str,
    tools: &[CodexMcpTransformedTool],
) -> Result<String, CodexMcpSchemaPreflightError> {
    let tool_identities = tools
        .iter()
        .map(|tool| {
            serde_json::json!({
                "canonicalCallableName": tool.canonical_callable_name,
                "canonicalSchemaFingerprint": tool.canonical_schema_fingerprint,
                "transformedSchemaFingerprint": tool.transformed_schema_fingerprint,
                "transformContractFingerprint": tool.transform_contract_fingerprint,
                "transformedFingerprint": tool.transformed_fingerprint,
            })
        })
        .collect::<Vec<_>>();
    let encoded = serde_json::to_vec(&serde_json::json!({
        "provider": "codex",
        "canonicalManifestHash": canonical_manifest_hash,
        "providerContractFingerprint": provider_contract_fingerprint,
        "tools": tool_identities,
    }))
    .map_err(|_| CodexMcpSchemaPreflightError::FingerprintSerialization)?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turn_mcp::projection::{
        McpProjectionLimits, McpSelectionReason, ResolvedMcpTurnTool,
    };

    fn projection(schema: JsonValue) -> ResolvedMcpTurnProjection {
        projection_for_turn(schema, "turn")
    }

    fn projection_for_turn(schema: JsonValue, turn_id: &str) -> ResolvedMcpTurnProjection {
        let mut projection = ResolvedMcpTurnProjection::empty("workspace", turn_id);
        projection.tools.push(ResolvedMcpTurnTool {
            canonical_callable_name: String::new(),
            workspace_id: "workspace".to_owned(),
            server_installation_id: "installation".to_owned(),
            server_name: "server".to_owned(),
            raw_tool_name: "tool".to_owned(),
            description: Some("fixture".to_owned()),
            input_schema: schema,
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

    fn empty_projection() -> ResolvedMcpTurnProjection {
        let mut projection = ResolvedMcpTurnProjection::empty("workspace", "turn");
        projection
            .finalize_identity(McpProjectionLimits::default())
            .expect("empty canonical projection");
        projection
    }

    #[test]
    fn codex_schema_preflight_preserves_canonical_schema_opaquely() {
        let schema = serde_json::json!({
            "$schema": "https://example.test/custom-mcp-dialect",
            "type": "object",
            "properties": {
                "value": {
                    "oneOf": [{"type": "string"}, {"type": "integer"}],
                    "nullable": true
                },
                "metadata": {
                    "type": "object",
                    "patternProperties": {"^x-": false},
                    "unevaluatedProperties": true
                }
            },
            "x-provider-extension": {"arbitrary": [true, null]}
        });
        let projection = projection(schema.clone());
        let original = projection.clone();
        let preflight =
            preflight_codex_mcp_schemas(&projection, "a".repeat(64)).expect("Codex preflight");
        assert_eq!(projection, original);
        assert_eq!(preflight.canonical_manifest_hash, projection.manifest_hash);
        assert_ne!(preflight.provider_manifest_hash, projection.manifest_hash);
        assert_eq!(preflight.tools.len(), 1);
        assert_eq!(preflight.tools[0].transformed_schema, schema);
        assert_eq!(
            preflight.tools[0].canonical_schema_fingerprint,
            preflight.tools[0].transformed_schema_fingerprint
        );
    }

    #[test]
    fn provider_schema_preflight_accepts_unknown_keywords_before_materialization() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {"value": {"oneOf": [{"type": "string"}]}},
            "x-never-seen-before": {"nested": false}
        });
        let projection = projection(schema.clone());
        let preflight = preflight_codex_mcp_schemas(&projection, "a".repeat(64))
            .expect("opaque schema must reach provider");
        assert_eq!(preflight.tools[0].transformed_schema, schema);
    }

    #[test]
    fn codex_schema_provider_manifest_changes_with_exact_contract_fingerprint() {
        let projection = projection(serde_json::json!({"type": "object"}));
        let first = preflight_codex_mcp_schemas(&projection, "a".repeat(64)).expect("first");
        let second = preflight_codex_mcp_schemas(&projection, "b".repeat(64)).expect("second");
        assert_ne!(first.provider_manifest_hash, second.provider_manifest_hash);
        assert_ne!(
            first.tools[0].transformed_fingerprint,
            second.tools[0].transformed_fingerprint
        );
    }

    #[test]
    fn codex_mcp_concurrency_launch_equality_uses_only_semantic_projection_identity() {
        let schema = serde_json::json!({"type": "object"});
        let turn_a = build_codex_mcp_session_launch_projection(
            projection_for_turn(schema.clone(), "turn-a"),
            "a".repeat(64),
        )
        .expect("turn A launch projection");
        let turn_b = build_codex_mcp_session_launch_projection(
            projection_for_turn(schema.clone(), "turn-b"),
            "a".repeat(64),
        )
        .expect("turn B launch projection");
        let changed_contract = build_codex_mcp_session_launch_projection(
            projection_for_turn(schema.clone(), "turn-c"),
            "b".repeat(64),
        )
        .expect("changed-contract launch projection");
        let changed_schema = build_codex_mcp_session_launch_projection(
            projection_for_turn(
                serde_json::json!({
                    "type": "object",
                    "properties": {"value": {"type": "string"}}
                }),
                "turn-d",
            ),
            "a".repeat(64),
        )
        .expect("changed-schema launch projection");

        assert_ne!(
            turn_a.canonical_projection.turn_id,
            turn_b.canonical_projection.turn_id
        );
        assert_eq!(turn_a, turn_b, "turn-local identity must not restart Codex");
        assert_eq!(
            turn_a.semantic_restart_fingerprint(),
            turn_b.semantic_restart_fingerprint()
        );
        assert_ne!(turn_a, changed_contract);
        assert_ne!(turn_a, changed_schema);
    }

    #[test]
    fn codex_mcp_config_empty_projection_has_no_stale_server_or_paths() {
        let preflight = preflight_codex_mcp_schemas(&empty_projection(), "a".repeat(64))
            .expect("empty preflight");
        let artifact = build_codex_managed_mcp_config_with_helper(&preflight, None, None)
            .expect("empty managed config");
        assert!(artifact.enabled_tools.is_empty());
        assert!(!artifact.config_toml.contains("mcp_servers"));
        assert!(!artifact.config_toml.contains("__cli-mcp-stdio"));
    }

    #[test]
    fn codex_mcp_config_exact_projection_has_one_required_pioneer_server() {
        let preflight = preflight_codex_mcp_schemas(
            &projection(serde_json::json!({"type": "object"})),
            "a".repeat(64),
        )
        .expect("preflight");
        let artifact = build_codex_managed_mcp_config_with_helper(
            &preflight,
            Some(PathBuf::from("/opt/pioneer/pioneer")),
            Some(PathBuf::from("/private/pioneer/session/bootstrap")),
        )
        .expect("managed config");
        assert_eq!(artifact.enabled_tools.len(), 1);
        assert!(artifact.config_toml.contains("[mcp_servers.pioneer]"));
        assert!(artifact.config_toml.contains("required = true"));
        assert!(artifact.config_toml.contains("approval_mode = \"approve\""));
        assert!(!artifact.config_toml.contains("default_tools_approval_mode"));
        assert!(!artifact.config_toml.contains("disabled_tools"));
    }

    #[test]
    fn codex_config_secret_canary_never_enters_managed_config() {
        let canary = "proposal53-upstream-secret-canary";
        let mut preflight = preflight_codex_mcp_schemas(
            &projection(serde_json::json!({
                "type": "object",
                "description": canary
            })),
            "a".repeat(64),
        )
        .expect("preflight");
        assert!(
            preflight.tools[0]
                .transformed_schema
                .to_string()
                .contains(canary)
        );
        preflight.tools[0].transformed_schema["x-test-upstream-url"] =
            JsonValue::String(format!("https://user:{canary}@upstream.invalid"));
        let artifact = build_codex_managed_mcp_config_with_helper(
            &preflight,
            Some(PathBuf::from("/opt/pioneer/pioneer")),
            Some(PathBuf::from("/private/pioneer/session/bootstrap")),
        )
        .expect("managed config");
        assert!(!artifact.config_toml.contains(canary));
        for forbidden in ["url =", "env =", "http_headers", "bearer_token"] {
            assert!(!artifact.config_toml.contains(forbidden));
        }
    }
}
