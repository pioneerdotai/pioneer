//! Typed pre-spawn Claude MCP configuration boundary.

use crate::cli_runtime::config::resolve_current_pioneer_cli_mcp_helper;
use crate::cli_runtime::mcp::facade::{
    CliMcpFacadeBuildError, CliMcpFacadeProjection, CliMcpFacadeProjectionLimits, CliMcpFacadeTool,
};
use crate::turn_mcp::projection::ResolvedMcpTurnProjection;
use anyhow::{Context, Result};
use pioneer_cli_agent_runtime::claude::{
    ClaudeManagedMcpConfigDescriptor, ClaudeManagedMcpConfigIdentity, ClaudeManagedMcpConfigInput,
    ClaudeMcpSchemaTransformer, ClaudeMcpSchemaTransformerError,
    materialize_claude_managed_mcp_config, serialize_claude_managed_mcp_config,
};
use pioneer_cli_agent_runtime::mcp::{McpSchemaTransformError, transform_mcp_tool_schema};
use pioneer_protocol::ToolMetadata;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};

const CLAUDE_QUALIFIED_TOOL_PREFIX: &str = "mcp__pioneer__";
const CLAUDE_SYNTHETIC_SERVER_NAME: &str = "mcp__pioneer";
const CLAUDE_ALLOWED_TOOLS_FLAG: &str = "--allowedTools";
const CLAUDE_NON_EMPTY_CONFIG_UPPER_BOUND_BYTES: usize = 17_408;
const CLAUDE_EMPTY_CONFIG_BYTES: usize = 18;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ClaudeProjectionBudget {
    pub(crate) max_allowed_tools_argv_bytes: usize,
    pub(crate) max_managed_config_bytes: usize,
}

impl Default for ClaudeProjectionBudget {
    fn default() -> Self {
        #[cfg(windows)]
        let max_allowed_tools_argv_bytes = 24 * 1024;
        #[cfg(not(windows))]
        let max_allowed_tools_argv_bytes = 64 * 1024;
        Self {
            max_allowed_tools_argv_bytes,
            max_managed_config_bytes: 64 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ClaudeMcpSchemaPreflight {
    pub(crate) canonical_manifest_hash: String,
    pub(crate) provider_manifest_hash: String,
    pub(crate) provider_contract_fingerprint: String,
    pub(crate) tools: Vec<ClaudeMcpTransformedTool>,
    pub(crate) allowed_tool_names: Vec<String>,
    pub(crate) encoded_allowed_tools_argv_bytes: usize,
    pub(crate) encoded_managed_config_upper_bound: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ClaudeMcpTransformedTool {
    pub(crate) canonical_callable_name: String,
    pub(crate) canonical_schema_fingerprint: String,
    pub(crate) transformed_schema: JsonValue,
    pub(crate) transformed_schema_fingerprint: String,
    pub(crate) transform_contract_fingerprint: String,
    pub(crate) transformed_fingerprint: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ClaudeMcpSessionLaunchProjection {
    pub(crate) canonical_projection: ResolvedMcpTurnProjection,
    pub(crate) preflight: ClaudeMcpSchemaPreflight,
    semantic_restart_fingerprint: String,
}

impl PartialEq for ClaudeMcpSessionLaunchProjection {
    fn eq(&self, other: &Self) -> bool {
        self.semantic_restart_fingerprint == other.semantic_restart_fingerprint
    }
}

impl Eq for ClaudeMcpSessionLaunchProjection {}

impl ClaudeMcpSessionLaunchProjection {
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

    pub(crate) fn bind_native_tool_use(
        &self,
        runtime_id: &str,
        session_generation: u64,
        native_thread_id: &str,
        native_turn_id: &str,
        native_item_id: &str,
        qualified_tool_name: &str,
        arguments: &JsonValue,
    ) -> Result<ClaudeNativeMcpItemBinding, ClaudeNativeMcpEventError> {
        if [runtime_id, native_thread_id, native_turn_id, native_item_id]
            .into_iter()
            .any(|value| value.trim().is_empty())
            || session_generation == 0
        {
            return Err(ClaudeNativeMcpEventError::MissingIdentity);
        }
        let canonical_callable_name = qualified_tool_name
            .strip_prefix(CLAUDE_QUALIFIED_TOOL_PREFIX)
            .filter(|value| !value.trim().is_empty())
            .ok_or(ClaudeNativeMcpEventError::UnmanagedTool)?;
        let tool = self
            .canonical_projection
            .tools
            .iter()
            .find(|tool| tool.canonical_callable_name == canonical_callable_name)
            .ok_or(ClaudeNativeMcpEventError::ToolOutsideProjection)?;
        let arguments_fingerprint =
            crate::cli_runtime::codex_mcp::canonical_value_fingerprint(arguments)
                .map_err(|_| ClaudeNativeMcpEventError::Serialization)?;
        let correlation = serde_json::to_vec(&serde_json::json!({
            "runtimeId": runtime_id,
            "nativeThreadId": native_thread_id,
            "nativeTurnId": native_turn_id,
            "providerCallId": native_item_id,
        }))
        .map_err(|_| ClaudeNativeMcpEventError::Serialization)?;
        let invocation_correlation_id = hex::encode(Sha256::digest(correlation));
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "toolName".to_owned(),
            JsonValue::String(canonical_callable_name.to_owned()),
        );
        metadata.insert(
            "tool".to_owned(),
            JsonValue::String(canonical_callable_name.to_owned()),
        );
        metadata.insert(
            "providerQualifiedName".to_owned(),
            JsonValue::String(qualified_tool_name.to_owned()),
        );
        metadata.insert(
            "canonicalCallableName".to_owned(),
            JsonValue::String(tool.canonical_callable_name.clone()),
        );
        metadata.insert(
            "serverInstallationId".to_owned(),
            JsonValue::String(tool.server_installation_id.clone()),
        );
        metadata.insert(
            "serverName".to_owned(),
            JsonValue::String(tool.server_name.clone()),
        );
        metadata.insert(
            "rawToolName".to_owned(),
            JsonValue::String(tool.raw_tool_name.clone()),
        );
        metadata.insert(
            "manifestHash".to_owned(),
            JsonValue::String(self.preflight.canonical_manifest_hash.clone()),
        );
        metadata.insert(
            "providerManifestHash".to_owned(),
            JsonValue::String(self.preflight.provider_manifest_hash.clone()),
        );
        metadata.insert(
            "providerCallId".to_owned(),
            JsonValue::String(native_item_id.to_owned()),
        );
        metadata.insert(
            "invocationCorrelationId".to_owned(),
            JsonValue::String(invocation_correlation_id),
        );
        metadata.insert(
            "sessionGeneration".to_owned(),
            JsonValue::Number(session_generation.into()),
        );
        metadata.insert(
            "selectionReason".to_owned(),
            JsonValue::String(tool.selection_reason.legacy_binding_value().to_owned()),
        );
        metadata.insert("arguments".to_owned(), arguments.clone());
        metadata.insert(
            "status".to_owned(),
            JsonValue::String("inProgress".to_owned()),
        );
        if let Some(capability_id) = tool.capability_id.as_ref() {
            metadata.insert(
                "capabilityId".to_owned(),
                JsonValue::String(capability_id.clone()),
            );
        }
        Ok(ClaudeNativeMcpItemBinding {
            native_thread_id: native_thread_id.to_owned(),
            native_turn_id: native_turn_id.to_owned(),
            native_item_id: native_item_id.to_owned(),
            canonical_callable_name: canonical_callable_name.to_owned(),
            arguments_fingerprint,
            metadata: ToolMetadata::from_json(JsonValue::Object(metadata)),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ClaudeNativeMcpItemBinding {
    pub(crate) native_thread_id: String,
    pub(crate) native_turn_id: String,
    pub(crate) native_item_id: String,
    pub(crate) canonical_callable_name: String,
    pub(crate) arguments_fingerprint: String,
    pub(crate) metadata: ToolMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaudeNativeMcpEventError {
    MissingIdentity,
    UnmanagedTool,
    ToolOutsideProjection,
    Serialization,
}

impl fmt::Display for ClaudeNativeMcpEventError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::MissingIdentity => "Claude MCP item identity is incomplete",
            Self::UnmanagedTool => "Claude MCP item is outside the managed Pioneer namespace",
            Self::ToolOutsideProjection => "Claude MCP item tool is outside the frozen projection",
            Self::Serialization => "Claude MCP correlation identity could not be serialized",
        })
    }
}

impl Error for ClaudeNativeMcpEventError {}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ClaudeNativeMcpPermissionRequest {
    pub(crate) runtime_id: String,
    pub(crate) session_generation: u64,
    pub(crate) native_thread_id: String,
    pub(crate) native_turn_id: String,
    pub(crate) native_item_id: String,
    pub(crate) qualified_tool_name: String,
    pub(crate) canonical_callable_name: String,
    pub(crate) arguments: JsonValue,
    pub(crate) arguments_fingerprint: String,
    pub(crate) manifest_hash: String,
    pub(crate) provider_contract_fingerprint: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaudeNativeMcpPermissionParseError {
    NotSynthetic,
    InvalidShape,
    InvalidIdentity,
    WildcardOrInvalidName,
    Serialization,
}

impl fmt::Display for ClaudeNativeMcpPermissionParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotSynthetic => "Claude permission request is not for the Pioneer MCP server",
            Self::InvalidShape => "Claude Pioneer MCP permission request shape is invalid",
            Self::InvalidIdentity => "Claude Pioneer MCP permission request identity is invalid",
            Self::WildcardOrInvalidName => {
                "Claude Pioneer MCP permission request name is wildcarded or invalid"
            }
            Self::Serialization => {
                "Claude Pioneer MCP permission request arguments are not canonicalizable"
            }
        })
    }
}

impl Error for ClaudeNativeMcpPermissionParseError {}

pub(crate) fn is_claude_native_mcp_permission_candidate(request: &JsonValue) -> bool {
    request
        .get("tool_name")
        .and_then(JsonValue::as_str)
        .is_some_and(|name| {
            name == CLAUDE_SYNTHETIC_SERVER_NAME || name.starts_with(CLAUDE_QUALIFIED_TOOL_PREFIX)
        })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn parse_claude_native_mcp_permission_request(
    request: &JsonValue,
    runtime_id: &str,
    session_generation: u64,
    native_thread_id: &str,
    native_turn_id: &str,
    manifest_hash: &str,
    provider_contract_fingerprint: &str,
) -> Result<ClaudeNativeMcpPermissionRequest, ClaudeNativeMcpPermissionParseError> {
    if !is_claude_native_mcp_permission_candidate(request) {
        return Err(ClaudeNativeMcpPermissionParseError::NotSynthetic);
    }
    if request.get("subtype").and_then(JsonValue::as_str) != Some("can_use_tool") {
        return Err(ClaudeNativeMcpPermissionParseError::InvalidShape);
    }
    let qualified_tool_name = request
        .get("tool_name")
        .and_then(JsonValue::as_str)
        .ok_or(ClaudeNativeMcpPermissionParseError::InvalidShape)?;
    let canonical_callable_name = qualified_tool_name
        .strip_prefix(CLAUDE_QUALIFIED_TOOL_PREFIX)
        .filter(|name| !name.is_empty())
        .ok_or(ClaudeNativeMcpPermissionParseError::WildcardOrInvalidName)?;
    if qualified_tool_name.contains('*')
        || canonical_callable_name
            .bytes()
            .any(|byte| !(byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'))
    {
        return Err(ClaudeNativeMcpPermissionParseError::WildcardOrInvalidName);
    }
    let native_item_id = request
        .get("tool_use_id")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(ClaudeNativeMcpPermissionParseError::InvalidShape)?;
    let arguments = request
        .get("input")
        .filter(|value| value.is_object())
        .cloned()
        .ok_or(ClaudeNativeMcpPermissionParseError::InvalidShape)?;
    if runtime_id.trim().is_empty()
        || session_generation == 0
        || native_turn_id.trim().is_empty()
        || manifest_hash.len() != 64
        || provider_contract_fingerprint.len() != 64
        || uuid::Uuid::parse_str(native_thread_id)
            .ok()
            .is_none_or(|provider_session_id| provider_session_id.is_nil())
    {
        return Err(ClaudeNativeMcpPermissionParseError::InvalidIdentity);
    }
    let arguments_fingerprint =
        crate::cli_runtime::codex_mcp::canonical_value_fingerprint(&arguments)
            .map_err(|_| ClaudeNativeMcpPermissionParseError::Serialization)?;
    Ok(ClaudeNativeMcpPermissionRequest {
        runtime_id: runtime_id.to_owned(),
        session_generation,
        native_thread_id: native_thread_id.to_owned(),
        native_turn_id: native_turn_id.to_owned(),
        native_item_id: native_item_id.to_owned(),
        qualified_tool_name: qualified_tool_name.to_owned(),
        canonical_callable_name: canonical_callable_name.to_owned(),
        arguments,
        arguments_fingerprint,
        manifest_hash: manifest_hash.to_owned(),
        provider_contract_fingerprint: provider_contract_fingerprint.to_owned(),
    })
}

#[derive(Debug)]
pub(crate) enum ClaudeMcpPreflightError {
    CanonicalProjectionNotFinalized,
    InvalidProviderContractFingerprint,
    DuplicateCallableName {
        callable_name: String,
    },
    IncompatibleTool {
        callable_name: String,
        error: McpSchemaTransformError,
    },
    AllowedToolsBudgetExceeded {
        actual: usize,
        maximum: usize,
    },
    ManagedConfigBudgetExceeded {
        actual: usize,
        maximum: usize,
    },
    InvalidAllowedToolName {
        name: String,
    },
    ManagedConfigProjectionMismatch,
    FingerprintSerialization,
}

impl fmt::Display for ClaudeMcpPreflightError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CanonicalProjectionNotFinalized => {
                formatter.write_str("canonical Claude MCP projection is not finalized")
            }
            Self::InvalidProviderContractFingerprint => {
                formatter.write_str("invalid Claude executable/contract fingerprint")
            }
            Self::DuplicateCallableName { callable_name } => {
                write!(
                    formatter,
                    "duplicate Claude MCP callable name `{callable_name}`"
                )
            }
            Self::IncompatibleTool {
                callable_name,
                error,
            } => write!(
                formatter,
                "Claude MCP schema preflight rejected `{callable_name}`: {error}"
            ),
            Self::AllowedToolsBudgetExceeded { actual, maximum } => write!(
                formatter,
                "Claude exact allowed-tools argv uses {actual} encoded bytes; maximum is {maximum}"
            ),
            Self::ManagedConfigBudgetExceeded { actual, maximum } => write!(
                formatter,
                "Claude managed MCP config upper bound is {actual} bytes; maximum is {maximum}"
            ),
            Self::InvalidAllowedToolName { name } => {
                write!(formatter, "invalid Claude exact allowed tool name `{name}`")
            }
            Self::ManagedConfigProjectionMismatch => formatter.write_str(
                "Claude managed MCP config does not match the exact allowed-tool projection",
            ),
            Self::FingerprintSerialization => {
                formatter.write_str("failed to fingerprint Claude MCP provider projection")
            }
        }
    }
}

impl Error for ClaudeMcpPreflightError {}

impl From<pioneer_cli_agent_runtime::mcp::TransformedMcpToolSchema> for ClaudeMcpTransformedTool {
    fn from(schema: pioneer_cli_agent_runtime::mcp::TransformedMcpToolSchema) -> Self {
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

/// Build the entire Claude-visible MCP surface without touching the filesystem
/// or starting a provider process. Every later artifact and argv builder must
/// consume this result instead of independently deriving tool names.
pub(crate) fn preflight_claude_mcp_projection(
    projection: &ResolvedMcpTurnProjection,
    provider_contract_fingerprint: impl Into<String>,
) -> Result<ClaudeMcpSchemaPreflight, ClaudeMcpPreflightError> {
    preflight_claude_mcp_projection_with_budget(
        projection,
        provider_contract_fingerprint,
        ClaudeProjectionBudget::default(),
    )
}

fn preflight_claude_mcp_projection_with_budget(
    projection: &ResolvedMcpTurnProjection,
    provider_contract_fingerprint: impl Into<String>,
    budget: ClaudeProjectionBudget,
) -> Result<ClaudeMcpSchemaPreflight, ClaudeMcpPreflightError> {
    if !is_sha256_hex(projection.manifest_hash.as_str()) {
        return Err(ClaudeMcpPreflightError::CanonicalProjectionNotFinalized);
    }
    let provider_contract_fingerprint = provider_contract_fingerprint.into();
    if !is_sha256_hex(provider_contract_fingerprint.as_str()) {
        return Err(ClaudeMcpPreflightError::InvalidProviderContractFingerprint);
    }
    let transformer = ClaudeMcpSchemaTransformer::new(provider_contract_fingerprint.clone())
        .map_err(
            |ClaudeMcpSchemaTransformerError::InvalidProviderContractFingerprint| {
                ClaudeMcpPreflightError::InvalidProviderContractFingerprint
            },
        )?;
    let mut canonical_names = HashSet::with_capacity(projection.tools.len());
    let mut tools = Vec::with_capacity(projection.tools.len());
    for tool in &projection.tools {
        if !canonical_names.insert(tool.canonical_callable_name.clone()) {
            return Err(ClaudeMcpPreflightError::DuplicateCallableName {
                callable_name: tool.canonical_callable_name.clone(),
            });
        }
        let transformed = transform_mcp_tool_schema(&tool.as_provider_schema_input(), &transformer)
            .map_err(|error| ClaudeMcpPreflightError::IncompatibleTool {
                callable_name: tool.canonical_callable_name.clone(),
                error,
            })?;
        tools.push(ClaudeMcpTransformedTool::from(transformed));
    }
    tools.sort_by(|left, right| {
        left.canonical_callable_name
            .cmp(&right.canonical_callable_name)
    });

    let allowed_tool_names = tools
        .iter()
        .map(|tool| {
            format!(
                "{CLAUDE_QUALIFIED_TOOL_PREFIX}{}",
                tool.canonical_callable_name
            )
        })
        .collect::<Vec<_>>();
    validate_exact_allowed_tool_names(allowed_tool_names.as_slice())?;
    let encoded_allowed_tools_argv_bytes = if allowed_tool_names.is_empty() {
        0
    } else {
        encoded_argv_bytes(CLAUDE_ALLOWED_TOOLS_FLAG, allowed_tool_names.as_slice())
    };
    if encoded_allowed_tools_argv_bytes > budget.max_allowed_tools_argv_bytes {
        return Err(ClaudeMcpPreflightError::AllowedToolsBudgetExceeded {
            actual: encoded_allowed_tools_argv_bytes,
            maximum: budget.max_allowed_tools_argv_bytes,
        });
    }
    let encoded_managed_config_upper_bound = if tools.is_empty() {
        CLAUDE_EMPTY_CONFIG_BYTES
    } else {
        CLAUDE_NON_EMPTY_CONFIG_UPPER_BOUND_BYTES
    };
    if encoded_managed_config_upper_bound > budget.max_managed_config_bytes {
        return Err(ClaudeMcpPreflightError::ManagedConfigBudgetExceeded {
            actual: encoded_managed_config_upper_bound,
            maximum: budget.max_managed_config_bytes,
        });
    }

    let provider_manifest_hash = claude_provider_manifest_hash(
        projection.manifest_hash.as_str(),
        provider_contract_fingerprint.as_str(),
        tools.as_slice(),
        allowed_tool_names.as_slice(),
    )?;
    Ok(ClaudeMcpSchemaPreflight {
        canonical_manifest_hash: projection.manifest_hash.clone(),
        provider_manifest_hash,
        provider_contract_fingerprint,
        tools,
        allowed_tool_names,
        encoded_allowed_tools_argv_bytes,
        encoded_managed_config_upper_bound,
    })
}

pub(crate) fn build_claude_mcp_session_launch_projection(
    projection: ResolvedMcpTurnProjection,
    provider_contract_fingerprint: impl Into<String>,
) -> Result<ClaudeMcpSessionLaunchProjection, ClaudeMcpPreflightError> {
    let preflight =
        preflight_claude_mcp_projection(&projection, provider_contract_fingerprint.into())?;
    let semantic_restart_fingerprint = sha256_json(&serde_json::json!({
        "provider": "claude",
        "providerManifestHash": preflight.provider_manifest_hash,
        "allowedToolNames": preflight.allowed_tool_names,
        "managedConfigMode": if preflight.tools.is_empty() { "empty" } else { "pioneer" },
        "strictMcpConfig": true,
    }))?;
    Ok(ClaudeMcpSessionLaunchProjection {
        canonical_projection: projection,
        preflight,
        semantic_restart_fingerprint,
    })
}

/// Append the exact provider-side preallow. A caller cannot enable the
/// synthetic server without a non-empty exact list, or preallow names while
/// using the strict empty config.
pub(crate) fn append_claude_exact_allowed_tools(
    args: &mut Vec<String>,
    managed_mcp_config: &ClaudeManagedMcpConfigDescriptor,
    allowed_tool_names: &[String],
) -> Result<(), ClaudeMcpPreflightError> {
    if managed_mcp_config.has_pioneer_server != !allowed_tool_names.is_empty() {
        return Err(ClaudeMcpPreflightError::ManagedConfigProjectionMismatch);
    }
    validate_exact_allowed_tool_names(allowed_tool_names)?;
    if !allowed_tool_names.is_empty() {
        args.push(CLAUDE_ALLOWED_TOOLS_FLAG.to_owned());
        args.extend(allowed_tool_names.iter().cloned());
    }
    Ok(())
}

fn validate_exact_allowed_tool_names(
    allowed_tool_names: &[String],
) -> Result<(), ClaudeMcpPreflightError> {
    let mut previous: Option<&str> = None;
    for name in allowed_tool_names {
        let canonical_name = name
            .strip_prefix(CLAUDE_QUALIFIED_TOOL_PREFIX)
            .filter(|canonical_name| !canonical_name.is_empty())
            .ok_or_else(|| ClaudeMcpPreflightError::InvalidAllowedToolName {
                name: name.clone(),
            })?;
        if name.contains('*')
            || canonical_name
                .bytes()
                .any(|byte| !(byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'))
            || previous.is_some_and(|previous| previous >= name.as_str())
        {
            return Err(ClaudeMcpPreflightError::InvalidAllowedToolName { name: name.clone() });
        }
        previous = Some(name.as_str());
    }
    Ok(())
}

fn encoded_argv_bytes(flag: &str, values: &[String]) -> usize {
    std::iter::once(flag)
        .chain(values.iter().map(String::as_str))
        .map(encoded_argument_bytes)
        .fold(0_usize, usize::saturating_add)
}

#[cfg(windows)]
fn encoded_argument_bytes(argument: &str) -> usize {
    argument
        .encode_utf16()
        .count()
        .saturating_add(1)
        .saturating_mul(2)
}

#[cfg(not(windows))]
fn encoded_argument_bytes(argument: &str) -> usize {
    argument.len().saturating_add(1)
}

fn claude_provider_manifest_hash(
    canonical_manifest_hash: &str,
    provider_contract_fingerprint: &str,
    tools: &[ClaudeMcpTransformedTool],
    allowed_tool_names: &[String],
) -> Result<String, ClaudeMcpPreflightError> {
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
    sha256_json(&serde_json::json!({
        "provider": "claude",
        "canonicalManifestHash": canonical_manifest_hash,
        "providerContractFingerprint": provider_contract_fingerprint,
        "tools": tool_identities,
        "allowedToolNames": allowed_tool_names,
    }))
}

fn sha256_json(value: &JsonValue) -> Result<String, ClaudeMcpPreflightError> {
    let encoded =
        serde_json::to_vec(value).map_err(|_| ClaudeMcpPreflightError::FingerprintSerialization)?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClaudeManagedMcpLaunchMode {
    Empty,
    Pioneer { bootstrap_path: PathBuf },
}

pub(crate) fn materialize_claude_mcp_config(
    managed_root_path: &Path,
    identity: ClaudeManagedMcpConfigIdentity,
    mode: ClaudeManagedMcpLaunchMode,
) -> Result<ClaudeManagedMcpConfigDescriptor> {
    match mode {
        ClaudeManagedMcpLaunchMode::Empty => {
            materialize_claude_mcp_config_with_helper(managed_root_path, identity, None, None)
        }
        ClaudeManagedMcpLaunchMode::Pioneer { bootstrap_path } => {
            let helper = resolve_current_pioneer_cli_mcp_helper()
                .context("failed to resolve the signed Pioneer Claude MCP helper")?;
            materialize_claude_mcp_config_with_helper(
                managed_root_path,
                identity,
                Some(helper),
                Some(bootstrap_path),
            )
        }
    }
}

fn materialize_claude_mcp_config_with_helper(
    managed_root_path: &Path,
    identity: ClaudeManagedMcpConfigIdentity,
    helper_path: Option<PathBuf>,
    bootstrap_path: Option<PathBuf>,
) -> Result<ClaudeManagedMcpConfigDescriptor> {
    let input = match (helper_path, bootstrap_path) {
        (None, None) => ClaudeManagedMcpConfigInput::empty(),
        (Some(helper_path), Some(bootstrap_path)) => {
            ClaudeManagedMcpConfigInput::pioneer(helper_path, bootstrap_path)
        }
        _ => anyhow::bail!("Claude managed MCP launch requires a complete helper/bootstrap pair"),
    };
    let artifact = serialize_claude_managed_mcp_config(input)
        .context("failed to serialize strict Claude MCP config")?;
    materialize_claude_managed_mcp_config(managed_root_path, identity, &artifact)
        .context("failed to materialize strict Claude MCP config")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turn_mcp::projection::{
        McpProjectionLimits, McpSelectionReason, ResolvedMcpTurnTool,
    };
    use serde_json::json;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    fn identity(generation: u64) -> ClaudeManagedMcpConfigIdentity {
        ClaudeManagedMcpConfigIdentity::new(
            "workspace",
            "claude",
            "thread",
            "gateway-boot",
            generation,
        )
        .expect("identity")
    }

    fn projection_for_turn(
        turn_id: &str,
        tool_names: &[&str],
        schema: JsonValue,
    ) -> ResolvedMcpTurnProjection {
        let mut projection = ResolvedMcpTurnProjection::empty("workspace", turn_id);
        projection.tools = tool_names
            .iter()
            .enumerate()
            .map(|(index, raw_tool_name)| ResolvedMcpTurnTool {
                canonical_callable_name: String::new(),
                workspace_id: "workspace".to_owned(),
                server_installation_id: format!("installation-{index}"),
                server_name: "server".to_owned(),
                raw_tool_name: (*raw_tool_name).to_owned(),
                description: Some("fixture".to_owned()),
                input_schema: schema.clone(),
                annotations: None,
                timeout_ms: 20_000,
                catalog_version: "catalog".to_owned(),
                installation_fingerprint: format!("installation-fingerprint-{index}"),
                schema_fingerprint: String::new(),
                runtime_generation: 1,
                selection_reason: McpSelectionReason::ExplicitTool,
                capability_id: Some(format!("capability-{index}")),
            })
            .collect();
        projection
            .finalize_identity(McpProjectionLimits::default())
            .expect("canonical projection");
        projection
    }

    #[test]
    fn claude_mcp_permission_callback_fixture_is_typed_and_strict() {
        let fixture: JsonValue = serde_json::from_str(include_str!(
            "../../tests/fixtures/claude_mcp_permission_callbacks.json"
        ))
        .expect("Claude permission callback fixture");
        let provider_session_id = "01900000-0000-7000-8000-000000000031";
        let exact = parse_claude_native_mcp_permission_request(
            &fixture["exactDestructive"]["request"],
            "claude",
            7,
            provider_session_id,
            "provider-turn",
            "a".repeat(64).as_str(),
            "b".repeat(64).as_str(),
        )
        .expect("exact synthetic callback");
        assert_eq!(exact.native_item_id, "call-exact");
        assert_eq!(exact.canonical_callable_name, "mcp_server_tool_a");
        assert_eq!(exact.arguments["command"], json!("rm -rf ./generated"));
        assert!(
            is_claude_native_mcp_permission_candidate(&fixture["unknown"]["request"]),
            "unknown synthetic names still require a direct fail-closed response"
        );
        assert!(matches!(
            parse_claude_native_mcp_permission_request(
                &fixture["wildcard"]["request"],
                "claude",
                7,
                provider_session_id,
                "provider-turn",
                "a".repeat(64).as_str(),
                "b".repeat(64).as_str(),
            ),
            Err(ClaudeNativeMcpPermissionParseError::WildcardOrInvalidName)
        ));
        assert!(matches!(
            parse_claude_native_mcp_permission_request(
                &fixture["malformed"]["request"],
                "claude",
                7,
                provider_session_id,
                "provider-turn",
                "a".repeat(64).as_str(),
                "b".repeat(64).as_str(),
            ),
            Err(ClaudeNativeMcpPermissionParseError::InvalidShape)
        ));
        assert!(matches!(
            parse_claude_native_mcp_permission_request(
                &fixture["exactDestructive"]["request"],
                "claude",
                0,
                provider_session_id,
                "provider-turn",
                "a".repeat(64).as_str(),
                "b".repeat(64).as_str(),
            ),
            Err(ClaudeNativeMcpPermissionParseError::InvalidIdentity)
        ));
    }

    #[test]
    fn claude_allowed_tools_are_exact_sorted_and_never_wildcarded() {
        let projection = projection_for_turn(
            "turn",
            &["zeta", "alpha"],
            serde_json::json!({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "propertyNames": {"type": "string"},
                "additionalProperties": true
            }),
        );
        let preflight =
            preflight_claude_mcp_projection(&projection, "a".repeat(64)).expect("preflight");
        assert_eq!(
            preflight.allowed_tool_names,
            [
                "mcp__pioneer__mcp_server_alpha",
                "mcp__pioneer__mcp_server_zeta",
            ]
        );
        assert!(preflight.allowed_tool_names.iter().all(|name| {
            name.starts_with(CLAUDE_QUALIFIED_TOOL_PREFIX)
                && !name.contains('*')
                && name != "mcp__pioneer"
        }));
        assert_eq!(preflight.allowed_tool_names.len(), projection.tools.len());
    }

    #[test]
    fn claude_projection_budget_at_limit_and_over_limit_has_no_fs_side_effect() {
        let temp = tempfile::tempdir().expect("tempdir");
        let sentinel = temp.path().join("must-not-exist");
        let projection =
            projection_for_turn("turn", &["alpha"], serde_json::json!({"type": "object"}));
        let measured =
            preflight_claude_mcp_projection(&projection, "a".repeat(64)).expect("measured");
        let exact_budget = ClaudeProjectionBudget {
            max_allowed_tools_argv_bytes: measured.encoded_allowed_tools_argv_bytes,
            max_managed_config_bytes: measured.encoded_managed_config_upper_bound,
        };
        preflight_claude_mcp_projection_with_budget(&projection, "a".repeat(64), exact_budget)
            .expect("exact limit must succeed");

        let argv_error = preflight_claude_mcp_projection_with_budget(
            &projection,
            "a".repeat(64),
            ClaudeProjectionBudget {
                max_allowed_tools_argv_bytes: measured
                    .encoded_allowed_tools_argv_bytes
                    .saturating_sub(1),
                max_managed_config_bytes: measured.encoded_managed_config_upper_bound,
            },
        )
        .expect_err("argv over limit");
        assert!(matches!(
            argv_error,
            ClaudeMcpPreflightError::AllowedToolsBudgetExceeded { .. }
        ));

        let config_error = preflight_claude_mcp_projection_with_budget(
            &projection,
            "a".repeat(64),
            ClaudeProjectionBudget {
                max_allowed_tools_argv_bytes: measured.encoded_allowed_tools_argv_bytes,
                max_managed_config_bytes: measured
                    .encoded_managed_config_upper_bound
                    .saturating_sub(1),
            },
        )
        .expect_err("config over limit");
        assert!(matches!(
            config_error,
            ClaudeMcpPreflightError::ManagedConfigBudgetExceeded { .. }
        ));
        assert!(
            !sentinel.exists(),
            "provider preflight must not create filesystem artifacts"
        );
    }

    #[test]
    fn claude_provider_projection_passes_unknown_schema_keywords_through() {
        let schema = serde_json::json!({
            "$schema": "https://example.test/custom-mcp-dialect",
            "type": "object",
            "properties": {"value": {"oneOf": [{"type": "string"}]}},
            "x-never-seen-before": {"nested": false}
        });
        let projection = projection_for_turn("turn", &["alpha"], schema.clone());
        let preflight = preflight_claude_mcp_projection(&projection, "a".repeat(64))
            .expect("opaque schema must reach provider");
        assert_eq!(preflight.tools[0].transformed_schema, schema);
        assert_eq!(
            preflight.tools[0].canonical_schema_fingerprint,
            preflight.tools[0].transformed_schema_fingerprint
        );
    }

    #[test]
    fn claude_session_launch_equality_uses_only_semantic_projection_identity() {
        let schema = serde_json::json!({"type": "object"});
        let turn_a = build_claude_mcp_session_launch_projection(
            projection_for_turn("turn-a", &["alpha"], schema.clone()),
            "a".repeat(64),
        )
        .expect("turn A");
        let turn_b = build_claude_mcp_session_launch_projection(
            projection_for_turn("turn-b", &["alpha"], schema.clone()),
            "a".repeat(64),
        )
        .expect("turn B");
        let changed_contract = build_claude_mcp_session_launch_projection(
            projection_for_turn("turn-c", &["alpha"], schema.clone()),
            "b".repeat(64),
        )
        .expect("changed contract");
        let changed_schema = build_claude_mcp_session_launch_projection(
            projection_for_turn(
                "turn-d",
                &["alpha"],
                serde_json::json!({
                    "type": "object",
                    "properties": {"value": {"type": "string"}}
                }),
            ),
            "a".repeat(64),
        )
        .expect("changed schema");

        assert_ne!(
            turn_a.canonical_projection.turn_id,
            turn_b.canonical_projection.turn_id
        );
        assert_eq!(
            turn_a, turn_b,
            "turn-local identity must not restart Claude"
        );
        assert_eq!(
            turn_a.semantic_restart_fingerprint(),
            turn_b.semantic_restart_fingerprint()
        );
        assert_ne!(turn_a, changed_contract);
        assert_ne!(turn_a, changed_schema);
    }

    #[test]
    #[cfg(unix)]
    fn claude_mcp_config_is_strict_empty_or_exact_pioneer_only() {
        let temp = tempfile::tempdir().expect("tempdir");
        let helper = temp.path().join("pioneer");
        let bootstrap = temp.path().join("bootstrap.json");
        std::fs::write(helper.as_path(), b"pioneer fixture").expect("helper");
        std::fs::write(bootstrap.as_path(), b"{}").expect("bootstrap");
        std::fs::set_permissions(bootstrap.as_path(), std::fs::Permissions::from_mode(0o600))
            .expect("bootstrap permissions");
        let root = temp.path().join("managed");

        let empty =
            materialize_claude_mcp_config_with_helper(root.as_path(), identity(1), None, None)
                .expect("empty config");
        let exact = materialize_claude_mcp_config_with_helper(
            root.as_path(),
            identity(2),
            Some(helper.clone()),
            Some(bootstrap.clone()),
        )
        .expect("exact config");

        assert_eq!(
            std::fs::read_to_string(empty.config_path).expect("empty contents"),
            "{\"mcpServers\":{}}\n"
        );
        let document: serde_json::Value =
            serde_json::from_slice(&std::fs::read(exact.config_path).expect("exact contents"))
                .expect("exact json");
        assert_eq!(
            document["mcpServers"]
                .as_object()
                .expect("server map")
                .keys()
                .collect::<Vec<_>>(),
            ["pioneer"]
        );
        assert_eq!(document["mcpServers"]["pioneer"]["type"], "stdio");
        assert_eq!(
            document["mcpServers"]["pioneer"]["command"],
            helper.to_string_lossy().as_ref()
        );
        assert_eq!(
            document["mcpServers"]["pioneer"]["args"],
            serde_json::json!([
                "__cli-mcp-stdio",
                "--bootstrap-file",
                bootstrap.to_string_lossy()
            ])
        );
        let encoded = document.to_string();
        for forbidden in ["headers", "env", "token", "url", "upstream"] {
            assert!(!encoded.to_ascii_lowercase().contains(forbidden));
        }
    }

    #[test]
    fn claude_mcp_config_rejects_incomplete_or_relative_managed_paths() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("managed");
        assert!(
            materialize_claude_mcp_config_with_helper(
                root.as_path(),
                identity(1),
                Some(PathBuf::from("relative-pioneer")),
                Some(temp.path().join("bootstrap.json")),
            )
            .is_err()
        );
        assert!(
            materialize_claude_mcp_config_with_helper(
                root.as_path(),
                identity(2),
                Some(temp.path().join("pioneer")),
                None,
            )
            .is_err()
        );
        assert!(!root.exists(), "preflight failure must create no artifact");
    }

    #[test]
    #[cfg(unix)]
    fn claude_mcp_config_production_helper_is_the_running_pioneer_executable() {
        let temp = tempfile::tempdir().expect("tempdir");
        let bootstrap = temp.path().join("bootstrap.json");
        std::fs::write(bootstrap.as_path(), b"{}").expect("bootstrap");
        std::fs::set_permissions(bootstrap.as_path(), std::fs::Permissions::from_mode(0o600))
            .expect("bootstrap permissions");
        let descriptor = materialize_claude_mcp_config(
            temp.path().join("managed").as_path(),
            identity(1),
            ClaudeManagedMcpLaunchMode::Pioneer {
                bootstrap_path: bootstrap,
            },
        )
        .expect("production config");
        let document: serde_json::Value =
            serde_json::from_slice(&std::fs::read(descriptor.config_path).expect("config"))
                .expect("JSON");
        assert_eq!(
            document["mcpServers"]["pioneer"]["command"],
            resolve_current_pioneer_cli_mcp_helper()
                .expect("running helper")
                .to_string_lossy()
                .as_ref()
        );
    }
}
