use crate::mcp::{
    CanonicalMcpToolSchema, McpSchemaIncompatibility, McpSchemaTransformContract,
    McpSchemaTransformer,
};
use serde_json::Value as JsonValue;
use std::error::Error;
use std::fmt;

const TRANSFORMER_ID: &str = "codex.app-server.mcp-schema";
pub const CODEX_MCP_SCHEMA_TRANSFORM_CONTRACT_VERSION: u32 = 2;
const MAX_CALLABLE_NAME_BYTES: usize = 64;
const MAX_SCHEMA_BYTES: usize = 256 * 1024;
const MAX_SCHEMA_DEPTH: usize = 64;
const MAX_SCHEMA_NODES: usize = 4_096;

/// Opaque transport adapter for an exact Codex executable and protocol
/// contract. Pioneer does not interpret, normalize, or restrict JSON Schema
/// keywords. The provider is the authority on the schema dialect it accepts.
///
/// This boundary validates only wire identifiers and resource limits before
/// passing the canonical MCP schema through unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexMcpSchemaTransformer {
    provider_contract_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexMcpSchemaTransformerError {
    InvalidProviderContractFingerprint,
}

impl fmt::Display for CodexMcpSchemaTransformerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProviderContractFingerprint => {
                formatter.write_str("invalid Codex executable/contract fingerprint")
            }
        }
    }
}

impl Error for CodexMcpSchemaTransformerError {}

impl CodexMcpSchemaTransformer {
    pub fn new(
        provider_contract_fingerprint: impl Into<String>,
    ) -> Result<Self, CodexMcpSchemaTransformerError> {
        let provider_contract_fingerprint = provider_contract_fingerprint.into();
        if provider_contract_fingerprint.len() != 64
            || !provider_contract_fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(CodexMcpSchemaTransformerError::InvalidProviderContractFingerprint);
        }
        Ok(Self {
            provider_contract_fingerprint,
        })
    }

    pub fn provider_contract_fingerprint(&self) -> &str {
        self.provider_contract_fingerprint.as_str()
    }
}

impl McpSchemaTransformer for CodexMcpSchemaTransformer {
    fn contract(&self) -> McpSchemaTransformContract {
        McpSchemaTransformContract {
            transformer_id: TRANSFORMER_ID.to_owned(),
            contract_version: CODEX_MCP_SCHEMA_TRANSFORM_CONTRACT_VERSION,
            provider_contract_fingerprint: self.provider_contract_fingerprint.clone(),
        }
    }

    fn transform(
        &self,
        canonical: &CanonicalMcpToolSchema,
    ) -> Result<JsonValue, McpSchemaIncompatibility> {
        validate_callable_name(canonical.canonical_callable_name.as_str())?;
        let encoded = serde_json::to_vec(&canonical.canonical_schema).map_err(|_| {
            incompatible(
                "codex.schema.serialization",
                "canonical MCP schema could not be serialized",
            )
        })?;
        if encoded.len() > MAX_SCHEMA_BYTES {
            return Err(incompatible(
                "codex.schema.bytes_exceeded",
                "canonical MCP schema exceeds the adapter byte limit",
            ));
        }
        validate_resource_limits(&canonical.canonical_schema)?;
        Ok(canonical.canonical_schema.clone())
    }
}

fn validate_callable_name(name: &str) -> Result<(), McpSchemaIncompatibility> {
    if name.is_empty()
        || name.len() > MAX_CALLABLE_NAME_BYTES
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(incompatible(
            "codex.schema.invalid_callable_name",
            "canonical MCP callable name is not representable by Codex",
        ));
    }
    Ok(())
}

fn validate_resource_limits(schema: &JsonValue) -> Result<(), McpSchemaIncompatibility> {
    // Iterative traversal is intentional: even hostile input cannot consume
    // the Rust call stack while the depth limit is being checked.
    let mut pending = vec![(1_usize, schema)];
    let mut nodes = 0_usize;
    while let Some((depth, value)) = pending.pop() {
        if depth > MAX_SCHEMA_DEPTH {
            return Err(incompatible(
                "codex.schema.depth_exceeded",
                "canonical MCP schema exceeds the adapter nesting limit",
            ));
        }
        nodes = nodes.saturating_add(1);
        if nodes > MAX_SCHEMA_NODES {
            return Err(incompatible(
                "codex.schema.nodes_exceeded",
                "canonical MCP schema exceeds the adapter node limit",
            ));
        }
        match value {
            JsonValue::Object(object) => {
                pending.extend(object.values().map(|child| (depth + 1, child)));
            }
            JsonValue::Array(values) => {
                pending.extend(values.iter().map(|child| (depth + 1, child)));
            }
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {}
        }
    }
    Ok(())
}

fn incompatible(code: impl Into<String>, message: impl Into<String>) -> McpSchemaIncompatibility {
    McpSchemaIncompatibility::new(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::{canonical_mcp_schema_fingerprint, transform_mcp_tool_schema};

    fn input(name: &str, schema: JsonValue) -> CanonicalMcpToolSchema {
        CanonicalMcpToolSchema {
            canonical_callable_name: name.to_owned(),
            canonical_schema_fingerprint: canonical_mcp_schema_fingerprint(&schema)
                .expect("canonical fingerprint"),
            canonical_schema: schema,
        }
    }

    fn transform(schema: JsonValue) -> crate::mcp::TransformedMcpToolSchema {
        transform_mcp_tool_schema(
            &input("mcp_pioneer_fixture", schema),
            &CodexMcpSchemaTransformer::new("a".repeat(64)).expect("transformer"),
        )
        .expect("Codex schema pass-through")
    }

    #[test]
    fn codex_schema_passes_arbitrary_dialects_and_keywords_through_unchanged() {
        let schema = serde_json::json!({
            "$schema": "https://example.test/custom-mcp-dialect",
            "$id": "https://example.test/tool",
            "type": "object",
            "oneOf": [
                {
                    "properties": {
                        "value": {
                            "type": "string",
                            "nullable": true,
                            "x-provider-extension": {"anything": [true, false, null]}
                        }
                    }
                },
                false
            ],
            "patternProperties": {
                "^x-": {"$dynamicRef": "https://example.test/external-schema"}
            },
            "unevaluatedProperties": true
        });
        let first = transform(schema.clone());
        let second = transform(schema.clone());
        assert_eq!(first, second);
        assert_eq!(first.transformed_schema, schema);
        assert_eq!(
            first.canonical_schema_fingerprint,
            first.transformed_schema_fingerprint
        );

        let empty = transform(serde_json::json!({}));
        assert_eq!(empty.transformed_schema, serde_json::json!({}));
    }

    #[test]
    fn codex_schema_checks_only_wire_and_resource_limits() {
        let invalid_name = transform_mcp_tool_schema(
            &input("invalid name", serde_json::json!({})),
            &CodexMcpSchemaTransformer::new("a".repeat(64)).expect("transformer"),
        )
        .expect_err("invalid provider wire name");
        assert!(matches!(
            invalid_name,
            crate::mcp::McpSchemaTransformError::Incompatible(ref incompatibility)
                if incompatibility.code == "codex.schema.invalid_callable_name"
        ));

        let mut too_deep = JsonValue::Bool(true);
        for _ in 0..MAX_SCHEMA_DEPTH {
            too_deep = serde_json::json!({"arbitrary": too_deep});
        }
        let depth_error = transform_mcp_tool_schema(
            &input("mcp_pioneer_fixture", too_deep),
            &CodexMcpSchemaTransformer::new("a".repeat(64)).expect("transformer"),
        )
        .expect_err("depth limit");
        assert!(matches!(
            depth_error,
            crate::mcp::McpSchemaTransformError::Incompatible(ref incompatibility)
                if incompatibility.code == "codex.schema.depth_exceeded"
        ));

        let too_many_nodes = serde_json::json!({
            "arbitrary": vec![JsonValue::Null; MAX_SCHEMA_NODES]
        });
        let nodes_error = transform_mcp_tool_schema(
            &input("mcp_pioneer_fixture", too_many_nodes),
            &CodexMcpSchemaTransformer::new("a".repeat(64)).expect("transformer"),
        )
        .expect_err("node limit");
        assert!(matches!(
            nodes_error,
            crate::mcp::McpSchemaTransformError::Incompatible(ref incompatibility)
                if incompatibility.code == "codex.schema.nodes_exceeded"
        ));

        let too_large = serde_json::json!({"arbitrary": "x".repeat(MAX_SCHEMA_BYTES)});
        let bytes_error = transform_mcp_tool_schema(
            &input("mcp_pioneer_fixture", too_large),
            &CodexMcpSchemaTransformer::new("a".repeat(64)).expect("transformer"),
        )
        .expect_err("byte limit");
        assert!(matches!(
            bytes_error,
            crate::mcp::McpSchemaTransformError::Incompatible(ref incompatibility)
                if incompatibility.code == "codex.schema.bytes_exceeded"
        ));
    }

    #[test]
    fn codex_schema_contract_fingerprint_tracks_exact_provider_contract() {
        let schema = serde_json::json!({"type": "object", "x-custom": true});
        let input = input("mcp_pioneer_fixture", schema);
        let first = transform_mcp_tool_schema(
            &input,
            &CodexMcpSchemaTransformer::new("a".repeat(64)).expect("first"),
        )
        .expect("first transform");
        let second = transform_mcp_tool_schema(
            &input,
            &CodexMcpSchemaTransformer::new("b".repeat(64)).expect("second"),
        )
        .expect("second transform");
        assert_eq!(
            first.transformed_schema_fingerprint,
            second.transformed_schema_fingerprint
        );
        assert_ne!(
            first.transform_contract_fingerprint,
            second.transform_contract_fingerprint
        );
        assert_ne!(
            first.transformed_fingerprint,
            second.transformed_fingerprint
        );
    }

    #[test]
    fn codex_schema_generated_app_server_evidence_is_version_pinned() {
        let evidence: JsonValue = serde_json::from_str(include_str!(
            "../tests/fixtures/codex_app_server_0_144_1_schema_evidence.json"
        ))
        .expect("generated schema evidence");
        assert_eq!(evidence["codexVersion"], "0.144.1");
        assert_eq!(evidence["dynamicToolSpecInputSchema"], true);
        assert_eq!(evidence["mcpToolInputSchema"], true);
        for key in ["stableBundleSha256", "v2BundleSha256"] {
            assert_eq!(evidence[key].as_str().expect("hash").len(), 64);
        }
    }
}
