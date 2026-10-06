use crate::domain::{McpConfigValue, McpScopeKind, McpSecretRef};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpSecretMaterialization {
    pub ref_id: String,
    pub value: String,
}
impl std::fmt::Debug for McpSecretMaterialization {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpSecretMaterialization")
            .field("ref_id", &self.ref_id)
            .field("value", &"[redacted]")
            .finish()
    }
}

pub fn secret_ref_for(
    scope_kind: &McpScopeKind,
    scope_key: &str,
    server_name: &str,
    source: &str,
    key: &str,
    value: &str,
) -> (McpConfigValue, McpSecretRef, McpSecretMaterialization) {
    let normalized_source = source.trim().to_ascii_lowercase();
    let normalized_key = if normalized_source == "header" {
        key.trim().to_ascii_lowercase()
    } else {
        key.trim().to_owned()
    };
    let ref_id = format!(
        "gateway_settings:mcp:{}:{}:{}:{}:{}",
        scope_kind.as_str(),
        scope_key.trim(),
        server_name.trim(),
        normalized_source,
        normalized_key
    );

    (
        McpConfigValue::SecretRef {
            ref_id: ref_id.clone(),
        },
        McpSecretRef {
            ref_id: ref_id.clone(),
            name: normalized_key,
            source: normalized_source,
        },
        McpSecretMaterialization {
            ref_id,
            value: value.to_owned(),
        },
    )
}
