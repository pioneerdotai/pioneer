use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanonicalMcpToolSchema {
    pub canonical_callable_name: String,
    pub canonical_schema: JsonValue,
    pub canonical_schema_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpSchemaTransformContract {
    pub transformer_id: String,
    pub contract_version: u32,
    /// Fingerprint of the exact provider executable/protocol contract against
    /// which this transform was certified. This is deliberately distinct from
    /// a provider semantic version so readiness is invalidated by a changed
    /// build or generated contract even when the displayed version is equal.
    pub provider_contract_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransformedMcpToolSchema {
    pub canonical_callable_name: String,
    pub canonical_schema_fingerprint: String,
    pub transformed_schema: JsonValue,
    pub transformed_schema_fingerprint: String,
    pub transform_contract_fingerprint: String,
    pub transformed_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpSchemaIncompatibility {
    pub code: String,
    pub message: String,
}

impl McpSchemaIncompatibility {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

pub trait McpSchemaTransformer: Send + Sync {
    fn contract(&self) -> McpSchemaTransformContract;

    fn transform(
        &self,
        canonical: &CanonicalMcpToolSchema,
    ) -> Result<JsonValue, McpSchemaIncompatibility>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpSchemaTransformError {
    InvalidCallableName,
    CanonicalSchemaNotObject,
    CanonicalFingerprintMismatch { expected: String, actual: String },
    InvalidContract { message: String },
    Incompatible(McpSchemaIncompatibility),
    TransformedSchemaNotObject,
    Serialization { message: String },
}

impl fmt::Display for McpSchemaTransformError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCallableName => {
                formatter.write_str("canonical MCP callable name is empty")
            }
            Self::CanonicalSchemaNotObject => {
                formatter.write_str("canonical MCP input schema must be a JSON object")
            }
            Self::CanonicalFingerprintMismatch { expected, actual } => write!(
                formatter,
                "canonical MCP schema fingerprint mismatch: expected `{expected}`, got `{actual}`"
            ),
            Self::InvalidContract { message } => {
                write!(
                    formatter,
                    "invalid MCP schema transform contract: {message}"
                )
            }
            Self::Incompatible(incompatibility) => write!(
                formatter,
                "MCP schema is incompatible ({}): {}",
                incompatibility.code, incompatibility.message
            ),
            Self::TransformedSchemaNotObject => {
                formatter.write_str("transformed MCP input schema must be a JSON object")
            }
            Self::Serialization { message } => {
                write!(
                    formatter,
                    "failed to fingerprint MCP schema transform: {message}"
                )
            }
        }
    }
}

impl Error for McpSchemaTransformError {}

pub fn transform_mcp_tool_schema(
    input: &CanonicalMcpToolSchema,
    transformer: &dyn McpSchemaTransformer,
) -> Result<TransformedMcpToolSchema, McpSchemaTransformError> {
    if input.canonical_callable_name.trim().is_empty() {
        return Err(McpSchemaTransformError::InvalidCallableName);
    }
    if !input.canonical_schema.is_object() {
        return Err(McpSchemaTransformError::CanonicalSchemaNotObject);
    }

    let canonical_schema = canonical_json(&input.canonical_schema);
    let actual_canonical_fingerprint = fingerprint_json(&canonical_schema)?;
    if actual_canonical_fingerprint != input.canonical_schema_fingerprint {
        return Err(McpSchemaTransformError::CanonicalFingerprintMismatch {
            expected: input.canonical_schema_fingerprint.clone(),
            actual: actual_canonical_fingerprint,
        });
    }

    let contract = transformer.contract();
    validate_contract(&contract)?;
    let canonical_input = CanonicalMcpToolSchema {
        canonical_callable_name: input.canonical_callable_name.clone(),
        canonical_schema,
        canonical_schema_fingerprint: input.canonical_schema_fingerprint.clone(),
    };
    let transformed_schema = transformer
        .transform(&canonical_input)
        .map_err(McpSchemaTransformError::Incompatible)?;
    if !transformed_schema.is_object() {
        return Err(McpSchemaTransformError::TransformedSchemaNotObject);
    }
    let transformed_schema = canonical_json(&transformed_schema);
    let transformed_schema_fingerprint = fingerprint_json(&transformed_schema)?;
    let transform_contract_fingerprint =
        fingerprint_json(&serde_json::to_value(&contract).map_err(|error| {
            McpSchemaTransformError::Serialization {
                message: error.to_string(),
            }
        })?)?;
    let transformed_fingerprint = fingerprint_json(&serde_json::json!({
        "canonicalCallableName": canonical_input.canonical_callable_name,
        "canonicalSchemaFingerprint": canonical_input.canonical_schema_fingerprint,
        "transformedSchemaFingerprint": transformed_schema_fingerprint,
        "transformContractFingerprint": transform_contract_fingerprint,
    }))?;

    Ok(TransformedMcpToolSchema {
        canonical_callable_name: input.canonical_callable_name.clone(),
        canonical_schema_fingerprint: input.canonical_schema_fingerprint.clone(),
        transformed_schema,
        transformed_schema_fingerprint,
        transform_contract_fingerprint,
        transformed_fingerprint,
    })
}

pub fn canonical_mcp_schema_fingerprint(
    schema: &JsonValue,
) -> Result<String, McpSchemaTransformError> {
    fingerprint_json(&canonical_json(schema))
}

fn validate_contract(contract: &McpSchemaTransformContract) -> Result<(), McpSchemaTransformError> {
    if contract.transformer_id.trim().is_empty() {
        return Err(McpSchemaTransformError::InvalidContract {
            message: "transformer_id is empty".to_owned(),
        });
    }
    if contract.contract_version == 0 {
        return Err(McpSchemaTransformError::InvalidContract {
            message: "contract_version must be greater than zero".to_owned(),
        });
    }
    if contract.provider_contract_fingerprint.trim().is_empty()
        || contract.provider_contract_fingerprint.len() > 512
        || contract
            .provider_contract_fingerprint
            .chars()
            .any(char::is_control)
    {
        return Err(McpSchemaTransformError::InvalidContract {
            message: "provider_contract_fingerprint is invalid".to_owned(),
        });
    }
    Ok(())
}

fn canonical_json(value: &JsonValue) -> JsonValue {
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

fn fingerprint_json(value: &JsonValue) -> Result<String, McpSchemaTransformError> {
    let bytes = serde_json::to_vec(&canonical_json(value)).map_err(|error| {
        McpSchemaTransformError::Serialization {
            message: error.to_string(),
        }
    })?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeTransformer {
        contract_version: u32,
        reject: bool,
    }

    impl McpSchemaTransformer for FakeTransformer {
        fn contract(&self) -> McpSchemaTransformContract {
            McpSchemaTransformContract {
                transformer_id: "test.schema-transform".to_owned(),
                contract_version: self.contract_version,
                provider_contract_fingerprint: "test-provider-contract".to_owned(),
            }
        }

        fn transform(
            &self,
            canonical: &CanonicalMcpToolSchema,
        ) -> Result<JsonValue, McpSchemaIncompatibility> {
            if self.reject {
                return Err(McpSchemaIncompatibility::new(
                    "test.resource_limit",
                    "the fake adapter resource limit was exceeded",
                ));
            }
            Ok(canonical.canonical_schema.clone())
        }
    }

    fn input(schema: JsonValue) -> CanonicalMcpToolSchema {
        CanonicalMcpToolSchema {
            canonical_callable_name: "mcp_resend_send".to_owned(),
            canonical_schema_fingerprint: canonical_mcp_schema_fingerprint(&schema).unwrap(),
            canonical_schema: schema,
        }
    }

    #[test]
    fn mcp_schema_transport_is_canonical_deterministic_and_non_mutating() {
        let first_schema: JsonValue = serde_json::from_str(
            r#"{"type":"object","properties":{"to":{"type":"string"},"subject":{"type":"string"}}}"#,
        )
        .unwrap();
        let second_schema: JsonValue = serde_json::from_str(
            r#"{"properties":{"subject":{"type":"string"},"to":{"type":"string"}},"type":"object"}"#,
        )
        .unwrap();
        let first = input(first_schema);
        let second = input(second_schema);
        let original = first.clone();
        let transformer = FakeTransformer {
            contract_version: 1,
            reject: false,
        };

        let first_result = transform_mcp_tool_schema(&first, &transformer).unwrap();
        let second_result = transform_mcp_tool_schema(&second, &transformer).unwrap();

        assert_eq!(first, original, "transform must not mutate canonical input");
        assert_eq!(
            first_result.transformed_schema,
            second_result.transformed_schema
        );
        assert_eq!(
            first_result.transformed_fingerprint,
            second_result.transformed_fingerprint
        );
        assert_eq!(
            first_result.canonical_schema_fingerprint,
            first.canonical_schema_fingerprint
        );
        assert_eq!(
            first_result.transformed_schema_fingerprint,
            first_result.canonical_schema_fingerprint
        );
    }

    #[test]
    fn mcp_schema_transform_fingerprint_includes_contract_version() {
        let input = input(serde_json::json!({"type":"object"}));
        let first = transform_mcp_tool_schema(
            &input,
            &FakeTransformer {
                contract_version: 1,
                reject: false,
            },
        )
        .unwrap();
        let second = transform_mcp_tool_schema(
            &input,
            &FakeTransformer {
                contract_version: 2,
                reject: false,
            },
        )
        .unwrap();

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
    fn mcp_schema_transform_returns_typed_incompatibility_before_serialization() {
        let input = input(serde_json::json!({"type":"object"}));
        let error = transform_mcp_tool_schema(
            &input,
            &FakeTransformer {
                contract_version: 1,
                reject: true,
            },
        )
        .unwrap_err();

        assert_eq!(
            error,
            McpSchemaTransformError::Incompatible(McpSchemaIncompatibility::new(
                "test.resource_limit",
                "the fake adapter resource limit was exceeded",
            ))
        );
    }
}
