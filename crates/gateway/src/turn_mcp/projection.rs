use pioneer_protocol::{TurnAcceptedCapability, TurnRejectedCapability};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;

pub(crate) const MCP_TURN_PROJECTION_VERSION: u32 = 1;
pub(crate) const DEFAULT_MCP_TURN_TOOL_TIMEOUT_MS: u64 = 20_000;
const DEFAULT_MAX_PROJECTION_TOOLS: usize = 128;
const DEFAULT_MAX_PROJECTION_SCHEMA_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_CALLABLE_NAME_BYTES: usize = 64;

/// Provider-neutral result of the authoritative workspace MCP resolver.
///
/// This type deliberately contains selection data only. Provider materialization,
/// persistence, and provider-specific schema adaptation are later boundaries.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedMcpTurnProjection {
    pub(crate) projection_version: u32,
    pub(crate) workspace_id: String,
    pub(crate) turn_id: String,
    pub(crate) tools: Vec<ResolvedMcpTurnTool>,
    pub(crate) accepted_capabilities: Vec<TurnAcceptedCapability>,
    pub(crate) rejected_capabilities: Vec<TurnRejectedCapability>,
    pub(crate) available_mcp: Vec<String>,
    pub(crate) blocked_mcp: Vec<String>,
    pub(crate) diagnostics: Vec<McpResolutionDiagnostic>,
    pub(crate) manifest_hash: String,
}

impl ResolvedMcpTurnProjection {
    pub(crate) fn empty(workspace_id: impl Into<String>, turn_id: impl Into<String>) -> Self {
        Self {
            projection_version: MCP_TURN_PROJECTION_VERSION,
            workspace_id: workspace_id.into(),
            turn_id: turn_id.into(),
            tools: Vec::new(),
            accepted_capabilities: Vec::new(),
            rejected_capabilities: Vec::new(),
            available_mcp: Vec::new(),
            blocked_mcp: Vec::new(),
            diagnostics: Vec::new(),
            manifest_hash: String::new(),
        }
    }

    pub(crate) fn finalize_identity(
        &mut self,
        limits: McpProjectionLimits,
    ) -> Result<(), McpProjectionBuildError> {
        if self.tools.len() > limits.max_tools {
            return Err(McpProjectionBuildError::TooManyTools {
                actual: self.tools.len(),
                maximum: limits.max_tools,
            });
        }

        self.tools
            .sort_by(|left, right| canonical_source_key(left).cmp(&canonical_source_key(right)));
        let mut identities = HashSet::with_capacity(self.tools.len());
        for tool in &self.tools {
            let identity = (
                tool.server_installation_id.clone(),
                tool.raw_tool_name.clone(),
            );
            if !identities.insert(identity) {
                return Err(McpProjectionBuildError::DuplicateToolIdentity {
                    installation_id: tool.server_installation_id.clone(),
                    raw_tool_name: tool.raw_tool_name.clone(),
                });
            }
        }

        let bases = self
            .tools
            .iter()
            .map(base_callable_name)
            .collect::<Vec<_>>();
        let mut base_counts = HashMap::<String, usize>::new();
        for base in &bases {
            *base_counts.entry(base.clone()).or_default() += 1;
        }
        let mut used_names = HashSet::with_capacity(self.tools.len());
        let mut total_schema_bytes = 0_usize;
        for (tool, base) in self.tools.iter_mut().zip(bases) {
            let unsuffixed =
                truncate_ascii_identifier(base.as_str(), limits.max_callable_name_bytes);
            let callable_name = if base_counts.get(base.as_str()).copied().unwrap_or_default() > 1
                || used_names.contains(unsuffixed.as_str())
            {
                collision_callable_name(
                    base.as_str(),
                    tool.server_installation_id.as_str(),
                    tool.raw_tool_name.as_str(),
                    limits.max_callable_name_bytes,
                    &used_names,
                )
                .ok_or_else(|| McpProjectionBuildError::CallableNameCollision {
                    callable_name: unsuffixed.clone(),
                })?
            } else {
                unsuffixed
            };
            validate_callable_name(callable_name.as_str(), limits.max_callable_name_bytes)?;
            if !used_names.insert(callable_name.clone()) {
                return Err(McpProjectionBuildError::CallableNameCollision { callable_name });
            }
            tool.canonical_callable_name = callable_name;

            let (schema, schema_fingerprint, schema_bytes) =
                canonical_schema_identity(&tool.input_schema)
                    .map_err(McpProjectionBuildError::Serialization)?;
            total_schema_bytes = total_schema_bytes.saturating_add(schema_bytes.len());
            if total_schema_bytes > limits.max_total_schema_bytes {
                return Err(McpProjectionBuildError::SchemaBytesExceeded {
                    actual: total_schema_bytes,
                    maximum: limits.max_total_schema_bytes,
                });
            }
            tool.input_schema = schema;
            tool.schema_fingerprint = schema_fingerprint;
        }

        self.manifest_hash = projection_manifest_hash(self)?;
        Ok(())
    }
}

/// Canonical MCP tool selected for one Pioneer turn before provider adaptation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedMcpTurnTool {
    pub(crate) canonical_callable_name: String,
    pub(crate) workspace_id: String,
    pub(crate) server_installation_id: String,
    pub(crate) server_name: String,
    pub(crate) raw_tool_name: String,
    pub(crate) description: Option<String>,
    pub(crate) input_schema: JsonValue,
    pub(crate) annotations: Option<pioneer_tools::McpDynamicToolAnnotations>,
    pub(crate) timeout_ms: u64,
    pub(crate) catalog_version: String,
    pub(crate) installation_fingerprint: String,
    pub(crate) schema_fingerprint: String,
    pub(crate) runtime_generation: u64,
    pub(crate) selection_reason: McpSelectionReason,
    pub(crate) capability_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct McpProjectionLimits {
    pub(crate) max_tools: usize,
    pub(crate) max_total_schema_bytes: usize,
    pub(crate) max_callable_name_bytes: usize,
}

impl Default for McpProjectionLimits {
    fn default() -> Self {
        Self {
            max_tools: DEFAULT_MAX_PROJECTION_TOOLS,
            max_total_schema_bytes: DEFAULT_MAX_PROJECTION_SCHEMA_BYTES,
            max_callable_name_bytes: DEFAULT_MAX_CALLABLE_NAME_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum McpProjectionBuildError {
    TooManyTools {
        actual: usize,
        maximum: usize,
    },
    SchemaBytesExceeded {
        actual: usize,
        maximum: usize,
    },
    InvalidCallableName {
        name: String,
        reason: &'static str,
    },
    CallableNameCollision {
        callable_name: String,
    },
    DuplicateToolIdentity {
        installation_id: String,
        raw_tool_name: String,
    },
    Serialization(String),
}

impl fmt::Display for McpProjectionBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyTools { actual, maximum } => write!(
                formatter,
                "MCP projection has {actual} tools; configured maximum is {maximum}"
            ),
            Self::SchemaBytesExceeded { actual, maximum } => write!(
                formatter,
                "MCP projection schemas use {actual} bytes; configured maximum is {maximum}"
            ),
            Self::InvalidCallableName { name, reason } => {
                write!(formatter, "invalid MCP callable name `{name}`: {reason}")
            }
            Self::CallableNameCollision { callable_name } => write!(
                formatter,
                "deterministic MCP callable name collision for `{callable_name}`"
            ),
            Self::DuplicateToolIdentity {
                installation_id,
                raw_tool_name,
            } => write!(
                formatter,
                "duplicate MCP tool identity `{installation_id}/{raw_tool_name}`"
            ),
            Self::Serialization(message) => {
                write!(
                    formatter,
                    "failed to encode MCP projection identity: {message}"
                )
            }
        }
    }
}

impl Error for McpProjectionBuildError {}

impl ResolvedMcpTurnTool {
    pub(crate) fn as_provider_schema_input(
        &self,
    ) -> pioneer_cli_agent_runtime::mcp::CanonicalMcpToolSchema {
        pioneer_cli_agent_runtime::mcp::CanonicalMcpToolSchema {
            canonical_callable_name: self.canonical_callable_name.clone(),
            canonical_schema: self.input_schema.clone(),
            canonical_schema_fingerprint: self.schema_fingerprint.clone(),
        }
    }

    pub(crate) fn as_dynamic_descriptor(&self) -> pioneer_tools::McpDynamicToolDescriptor {
        let canonical_schema = self.as_provider_schema_input();
        pioneer_tools::McpDynamicToolDescriptor {
            callable_name: canonical_schema.canonical_callable_name,
            workspace_id: self.workspace_id.clone(),
            server_id: self.server_installation_id.clone(),
            server_name: self.server_name.clone(),
            raw_tool_name: self.raw_tool_name.clone(),
            catalog_version: self.catalog_version.clone(),
            fingerprint: self.installation_fingerprint.clone(),
            snapshot_version: self.runtime_generation,
            description: self
                .description
                .clone()
                .unwrap_or_else(|| "MCP runtime tool".to_owned()),
            parameters: canonical_schema.canonical_schema,
            annotations: self.annotations.clone().unwrap_or_default(),
            timeout_ms: Some(self.timeout_ms),
            max_arguments_bytes: pioneer_protocol::McpInvocationResourceLimits::default()
                .max_arguments_bytes,
            selection_reason: self.selection_reason.legacy_binding_value().to_owned(),
            capability_id: self.capability_id.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum McpSelectionReason {
    ImplicitPolicy,
    ExplicitServer,
    ExplicitTool,
}

impl McpSelectionReason {
    pub(crate) const fn priority(self) -> u8 {
        match self {
            Self::ImplicitPolicy => 1,
            Self::ExplicitServer => 2,
            Self::ExplicitTool => 3,
        }
    }

    pub(crate) const fn legacy_binding_value(self) -> &'static str {
        match self {
            Self::ImplicitPolicy => "implicit_policy",
            Self::ExplicitServer | Self::ExplicitTool => "explicit_composer_capability",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct McpResolutionDiagnostic {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl McpResolutionDiagnostic {
    pub(crate) fn selection(message: impl Into<String>) -> Self {
        Self {
            code: "mcp.selection",
            message: message.into(),
        }
    }

    pub(crate) fn installation_unavailable(message: impl Into<String>) -> Self {
        Self {
            code: "mcp.resolution.installation_unavailable",
            message: message.into(),
        }
    }
}

fn canonical_source_key(tool: &ResolvedMcpTurnTool) -> (String, String, String) {
    (
        normalize_name(tool.server_name.as_str()),
        tool.raw_tool_name.clone(),
        tool.server_installation_id.clone(),
    )
}

fn normalize_name(value: &str) -> String {
    value.trim().to_lowercase()
}

fn base_callable_name(tool: &ResolvedMcpTurnTool) -> String {
    format!(
        "mcp_{}_{}",
        sanitize_callable_component(tool.server_name.as_str()),
        sanitize_callable_component(tool.raw_tool_name.as_str())
    )
}

fn sanitize_callable_component(value: &str) -> String {
    let mut output = String::new();
    let mut previous_separator = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            output.push(ch.to_ascii_lowercase());
            previous_separator = false;
        } else if !previous_separator && !output.is_empty() {
            output.push('_');
            previous_separator = true;
        }
    }
    while output.ends_with('_') {
        output.pop();
    }
    if output.is_empty() {
        "tool".to_owned()
    } else {
        output
    }
}

fn collision_callable_name(
    base: &str,
    installation_id: &str,
    raw_tool_name: &str,
    maximum_bytes: usize,
    used_names: &HashSet<String>,
) -> Option<String> {
    let digest = sha256_hex(format!("{installation_id}\0{raw_tool_name}").as_bytes());
    for suffix_len in [10_usize, 16, 24, 32, 48, 64] {
        let suffix = &digest[..suffix_len];
        let prefix_bytes = maximum_bytes.saturating_sub(suffix.len() + 1).max(1);
        let candidate = format!(
            "{}_{}",
            truncate_ascii_identifier(base, prefix_bytes),
            suffix
        );
        if !used_names.contains(candidate.as_str()) {
            return Some(candidate);
        }
    }
    None
}

fn truncate_ascii_identifier(value: &str, maximum_bytes: usize) -> String {
    value.chars().take(maximum_bytes).collect()
}

fn validate_callable_name(name: &str, maximum_bytes: usize) -> Result<(), McpProjectionBuildError> {
    if name.is_empty() {
        return Err(McpProjectionBuildError::InvalidCallableName {
            name: name.to_owned(),
            reason: "name is empty",
        });
    }
    if name.len() > maximum_bytes {
        return Err(McpProjectionBuildError::InvalidCallableName {
            name: name.to_owned(),
            reason: "name exceeds configured byte limit",
        });
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(McpProjectionBuildError::InvalidCallableName {
            name: name.to_owned(),
            reason: "name contains unsupported characters",
        });
    }
    Ok(())
}

pub(crate) fn canonical_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(object) => {
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort();
            let mut canonical = serde_json::Map::new();
            for key in keys {
                canonical.insert(key.clone(), canonical_json(&object[key]));
            }
            JsonValue::Object(canonical)
        }
        JsonValue::Array(values) => JsonValue::Array(values.iter().map(canonical_json).collect()),
        _ => value.clone(),
    }
}

pub(crate) fn canonical_schema_identity(
    schema: &JsonValue,
) -> Result<(JsonValue, String, Vec<u8>), String> {
    let canonical = canonical_json(schema);
    let bytes = serde_json::to_vec(&canonical)
        .map_err(|error| format!("failed to encode canonical MCP schema: {error}"))?;
    let fingerprint = sha256_hex(bytes.as_slice());
    Ok((canonical, fingerprint, bytes))
}

pub(crate) fn canonical_annotations_identity(
    annotations: &pioneer_tools::McpDynamicToolAnnotations,
) -> Result<(String, String), String> {
    let value = serde_json::to_value(annotations)
        .map_err(|error| format!("failed to encode MCP annotations: {error}"))?;
    let canonical = canonical_json(&value);
    let bytes = serde_json::to_vec(&canonical)
        .map_err(|error| format!("failed to canonicalize MCP annotations: {error}"))?;
    let json = String::from_utf8(bytes.clone())
        .map_err(|error| format!("canonical MCP annotations were not UTF-8: {error}"))?;
    Ok((json, sha256_hex(bytes.as_slice())))
}

fn projection_manifest_hash(
    projection: &ResolvedMcpTurnProjection,
) -> Result<String, McpProjectionBuildError> {
    let mut tools = Vec::with_capacity(projection.tools.len());
    for tool in &projection.tools {
        let annotations = tool
            .annotations
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| McpProjectionBuildError::Serialization(error.to_string()))?
            .map(|annotations| canonical_json(&annotations));
        tools.push(serde_json::json!({
            "annotations": annotations,
            "callable_name": tool.canonical_callable_name,
            "catalog_version": tool.catalog_version,
            "input_schema": tool.input_schema,
            "installation_fingerprint": tool.installation_fingerprint,
            "runtime_generation": tool.runtime_generation,
            "server_installation_id": tool.server_installation_id,
            "raw_tool_name": tool.raw_tool_name,
            "timeout_ms": tool.timeout_ms,
        }));
    }
    let manifest = canonical_json(&serde_json::json!({
        "projection_version": projection.projection_version,
        "tools": tools,
    }));
    let bytes = serde_json::to_vec(&manifest)
        .map_err(|error| McpProjectionBuildError::Serialization(error.to_string()))?;
    Ok(sha256_hex(bytes.as_slice()))
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestSchemaTransformer {
        reject: bool,
    }

    impl pioneer_cli_agent_runtime::mcp::McpSchemaTransformer for TestSchemaTransformer {
        fn contract(&self) -> pioneer_cli_agent_runtime::mcp::McpSchemaTransformContract {
            pioneer_cli_agent_runtime::mcp::McpSchemaTransformContract {
                transformer_id: "gateway-test".to_owned(),
                contract_version: 1,
                provider_contract_fingerprint: "gateway-test-provider-contract".to_owned(),
            }
        }

        fn transform(
            &self,
            canonical: &pioneer_cli_agent_runtime::mcp::CanonicalMcpToolSchema,
        ) -> Result<JsonValue, pioneer_cli_agent_runtime::mcp::McpSchemaIncompatibility> {
            if self.reject {
                return Err(
                    pioneer_cli_agent_runtime::mcp::McpSchemaIncompatibility::new(
                        "test.incompatible",
                        "schema cannot be represented",
                    ),
                );
            }
            Ok(canonical.canonical_schema.clone())
        }
    }

    fn tool(installation_id: &str, server_name: &str, raw_tool_name: &str) -> ResolvedMcpTurnTool {
        ResolvedMcpTurnTool {
            canonical_callable_name: String::new(),
            workspace_id: "workspace".to_owned(),
            server_installation_id: installation_id.to_owned(),
            server_name: server_name.to_owned(),
            raw_tool_name: raw_tool_name.to_owned(),
            description: Some("description".to_owned()),
            input_schema: serde_json::json!({
                "required": ["query"],
                "properties": {"query": {"type": "string"}},
                "type": "object"
            }),
            annotations: Some(pioneer_tools::McpDynamicToolAnnotations {
                read_only_hint: Some(true),
                ..Default::default()
            }),
            timeout_ms: 20_000,
            catalog_version: "catalog-v1".to_owned(),
            installation_fingerprint: "installation-v1".to_owned(),
            schema_fingerprint: String::new(),
            runtime_generation: 7,
            selection_reason: McpSelectionReason::ExplicitTool,
            capability_id: Some("capability".to_owned()),
        }
    }

    fn projection(turn_id: &str, tools: Vec<ResolvedMcpTurnTool>) -> ResolvedMcpTurnProjection {
        ResolvedMcpTurnProjection {
            projection_version: MCP_TURN_PROJECTION_VERSION,
            workspace_id: "workspace".to_owned(),
            turn_id: turn_id.to_owned(),
            tools,
            accepted_capabilities: Vec::new(),
            rejected_capabilities: Vec::new(),
            available_mcp: Vec::new(),
            blocked_mcp: Vec::new(),
            diagnostics: Vec::new(),
            manifest_hash: String::new(),
        }
    }

    #[test]
    fn turn_mcp_projection_empty_is_a_valid_explicit_result() {
        let projection = ResolvedMcpTurnProjection::empty("workspace", "turn");
        assert_eq!(projection.projection_version, MCP_TURN_PROJECTION_VERSION);
        assert_eq!(projection.workspace_id, "workspace");
        assert_eq!(projection.turn_id, "turn");
        assert!(projection.tools.is_empty());
        assert!(projection.accepted_capabilities.is_empty());
        assert!(projection.rejected_capabilities.is_empty());
    }

    #[test]
    fn turn_mcp_projection_selection_priority_is_explicit_tool_server_implicit() {
        assert!(
            McpSelectionReason::ExplicitTool.priority()
                > McpSelectionReason::ExplicitServer.priority()
        );
        assert!(
            McpSelectionReason::ExplicitServer.priority()
                > McpSelectionReason::ImplicitPolicy.priority()
        );
    }

    #[test]
    fn callable_name_sanitizes_and_prefixes_identity() {
        let mut projection = projection(
            "turn",
            vec![tool(
                "installation",
                "GitHub Enterprise",
                "search/issues.create",
            )],
        );
        projection
            .finalize_identity(McpProjectionLimits::default())
            .expect("projection identity should finalize");
        assert_eq!(
            projection.tools[0].canonical_callable_name,
            "mcp_github_enterprise_search_issues_create"
        );
    }

    #[test]
    fn callable_name_collisions_use_stable_identity_hashes() {
        let mut projection = projection(
            "turn",
            vec![
                tool("installation-b", "server_a", "tool"),
                tool("installation-a", "server-a", "tool"),
            ],
        );
        projection
            .finalize_identity(McpProjectionLimits::default())
            .expect("projection identity should finalize");
        assert_eq!(projection.tools.len(), 2);
        assert!(projection.tools.iter().all(|tool| {
            tool.canonical_callable_name
                .starts_with("mcp_server_a_tool_")
        }));
        assert_ne!(
            projection.tools[0].canonical_callable_name,
            projection.tools[1].canonical_callable_name
        );
    }

    #[test]
    fn turn_mcp_manifest_is_identical_for_shuffled_source_tools() {
        let tools = vec![
            tool("installation-c", "zeta", "read"),
            tool("installation-b", "server_a", "tool"),
            tool("installation-a", "server-a", "tool"),
        ];
        let orders = [
            [0_usize, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        let mut baseline: Option<ResolvedMcpTurnProjection> = None;
        for (index, order) in orders.into_iter().enumerate() {
            let mut candidate = projection(
                format!("turn-{index}").as_str(),
                order
                    .into_iter()
                    .map(|index| tools[index].clone())
                    .collect(),
            );
            candidate
                .finalize_identity(McpProjectionLimits::default())
                .expect("permuted projection should finalize");
            if let Some(baseline) = baseline.as_ref() {
                assert_eq!(baseline.tools, candidate.tools);
                assert_eq!(baseline.manifest_hash, candidate.manifest_hash);
            } else {
                baseline = Some(candidate);
            }
        }
    }

    #[test]
    fn turn_mcp_manifest_canonicalizes_schema_object_order() {
        let mut left_tool = tool("installation", "server", "tool");
        left_tool.input_schema = serde_json::from_str(
            r#"{"type":"object","properties":{"b":{"type":"number"},"a":{"type":"string"}}}"#,
        )
        .expect("left schema");
        let mut right_tool = left_tool.clone();
        right_tool.input_schema = serde_json::from_str(
            r#"{"properties":{"a":{"type":"string"},"b":{"type":"number"}},"type":"object"}"#,
        )
        .expect("right schema");
        let mut left = projection("turn-left", vec![left_tool]);
        let mut right = projection("turn-right", vec![right_tool]);
        left.finalize_identity(McpProjectionLimits::default())
            .expect("left projection should finalize");
        right
            .finalize_identity(McpProjectionLimits::default())
            .expect("right projection should finalize");

        assert_eq!(
            left.tools[0].schema_fingerprint,
            right.tools[0].schema_fingerprint
        );
        assert_eq!(left.manifest_hash, right.manifest_hash);
    }

    #[test]
    fn provider_schema_preflight_preserves_canonical_identity_and_types_incompatibility() {
        let mut projection = projection(
            "turn-provider-schema",
            vec![tool("installation", "server", "tool")],
        );
        projection
            .finalize_identity(McpProjectionLimits::default())
            .expect("projection should finalize before provider schema preflight");
        let canonical_input = projection.tools[0].as_provider_schema_input();
        let original_schema = canonical_input.canonical_schema.clone();

        let transformed = pioneer_cli_agent_runtime::mcp::transform_mcp_tool_schema(
            &canonical_input,
            &TestSchemaTransformer { reject: false },
        )
        .expect("provider-neutral fake transformer should accept schema");
        assert_eq!(canonical_input.canonical_schema, original_schema);
        assert_eq!(
            canonical_input.canonical_schema_fingerprint,
            projection.tools[0].schema_fingerprint
        );
        assert_eq!(
            transformed.canonical_callable_name,
            projection.tools[0].canonical_callable_name
        );
        assert_eq!(transformed.transformed_schema, original_schema);

        let error = pioneer_cli_agent_runtime::mcp::transform_mcp_tool_schema(
            &canonical_input,
            &TestSchemaTransformer { reject: true },
        )
        .expect_err("selected incompatible schema must fail before provider start");
        assert!(matches!(
            error,
            pioneer_cli_agent_runtime::mcp::McpSchemaTransformError::Incompatible(_)
        ));
    }

    #[test]
    fn turn_mcp_manifest_changes_for_every_semantic_identity_field() {
        let mut baseline = projection("turn-baseline", vec![tool("install", "server", "tool")]);
        baseline
            .finalize_identity(McpProjectionLimits::default())
            .expect("baseline should finalize");
        let baseline_hash = baseline.manifest_hash.clone();

        let mutations: Vec<Box<dyn Fn(&mut ResolvedMcpTurnTool)>> = vec![
            Box::new(|tool| tool.raw_tool_name.push_str("_changed")),
            Box::new(|tool| tool.server_installation_id.push_str("_changed")),
            Box::new(|tool| tool.input_schema = serde_json::json!({"type": "string"})),
            Box::new(|tool| tool.annotations = None),
            Box::new(|tool| tool.timeout_ms += 1),
            Box::new(|tool| tool.catalog_version.push_str("_changed")),
            Box::new(|tool| tool.installation_fingerprint.push_str("_changed")),
            Box::new(|tool| tool.runtime_generation += 1),
        ];
        for mutate in mutations {
            let mut changed = projection("turn-changed", vec![tool("install", "server", "tool")]);
            mutate(&mut changed.tools[0]);
            changed
                .finalize_identity(McpProjectionLimits::default())
                .expect("changed projection should finalize");
            assert_ne!(baseline_hash, changed.manifest_hash);
        }
    }

    #[test]
    fn turn_mcp_manifest_excludes_turn_identity_and_nonsemantic_description() {
        let mut left = projection("turn-left", vec![tool("install", "server", "tool")]);
        let mut changed_tool = tool("install", "server", "tool");
        changed_tool.description = Some("new description".to_owned());
        changed_tool.capability_id = Some("another-capability".to_owned());
        changed_tool.selection_reason = McpSelectionReason::ImplicitPolicy;
        let mut right = projection("turn-right", vec![changed_tool]);
        left.finalize_identity(McpProjectionLimits::default())
            .expect("left projection should finalize");
        right
            .finalize_identity(McpProjectionLimits::default())
            .expect("right projection should finalize");
        assert_eq!(left.manifest_hash, right.manifest_hash);
    }

    #[test]
    fn projection_limits_reject_without_truncating_tools_or_schema() {
        let mut too_many = projection(
            "turn-tools",
            vec![
                tool("installation-a", "server", "a"),
                tool("installation-b", "server", "b"),
            ],
        );
        let error = too_many
            .finalize_identity(McpProjectionLimits {
                max_tools: 1,
                ..McpProjectionLimits::default()
            })
            .expect_err("tool overflow should fail");
        assert!(matches!(
            error,
            McpProjectionBuildError::TooManyTools {
                actual: 2,
                maximum: 1
            }
        ));
        assert_eq!(too_many.tools.len(), 2);

        let mut schema_overflow =
            projection("turn-schema", vec![tool("installation", "server", "tool")]);
        let error = schema_overflow
            .finalize_identity(McpProjectionLimits {
                max_total_schema_bytes: 8,
                ..McpProjectionLimits::default()
            })
            .expect_err("schema overflow should fail");
        assert!(matches!(
            error,
            McpProjectionBuildError::SchemaBytesExceeded { maximum: 8, .. }
        ));
        assert_eq!(schema_overflow.tools.len(), 1);
    }
}
