use anyhow::{Context, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpCatalogSnapshot {
    pub server_installation_id: String,
    pub catalog_version: String,
    pub server_info_json: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_instructions_hash: Option<String>,
    pub tools_json: String,
    pub resources_json: String,
    pub resource_templates_json: String,
    pub prompts_json: String,
    pub generated_at_unix: i64,
}

impl McpCatalogSnapshot {
    pub fn from_json_values(
        server_installation_id: String,
        server_info: Value,
        instructions: Option<&str>,
        tools: Value,
        resources: Value,
        resource_templates: Value,
        prompts: Value,
        generated_at_unix: i64,
    ) -> Result<Self> {
        let tools = ensure_array(tools);
        let resources = ensure_array(resources);
        let resource_templates = ensure_array(resource_templates);
        let prompts = ensure_array(prompts);

        let server_info_json = serde_json::to_string(&server_info)
            .context("failed to encode MCP server info catalog")?;
        let tools_json =
            serde_json::to_string(&tools).context("failed to encode MCP tools catalog")?;
        let resources_json =
            serde_json::to_string(&resources).context("failed to encode MCP resources catalog")?;
        let resource_templates_json = serde_json::to_string(&resource_templates)
            .context("failed to encode MCP resource templates catalog")?;
        let prompts_json =
            serde_json::to_string(&prompts).context("failed to encode MCP prompts catalog")?;

        let mut hasher = Sha256::new();
        hasher.update(server_info_json.as_bytes());
        hasher.update(b"\n");
        hasher.update(tools_json.as_bytes());
        hasher.update(b"\n");
        hasher.update(resources_json.as_bytes());
        hasher.update(b"\n");
        hasher.update(resource_templates_json.as_bytes());
        hasher.update(b"\n");
        hasher.update(prompts_json.as_bytes());
        let catalog_version = format!("sha256:{}", hex::encode(hasher.finalize()));

        Ok(Self {
            server_installation_id,
            catalog_version,
            server_info_json,
            server_instructions_hash: instructions.map(stable_hash),
            tools_json,
            resources_json,
            resource_templates_json,
            prompts_json,
            generated_at_unix,
        })
    }

    pub fn tools_count(&self) -> usize {
        count_json_array(self.tools_json.as_str())
    }

    pub fn resources_count(&self) -> usize {
        count_json_array(self.resources_json.as_str())
    }

    pub fn resource_templates_count(&self) -> usize {
        count_json_array(self.resource_templates_json.as_str())
    }

    pub fn prompts_count(&self) -> usize {
        count_json_array(self.prompts_json.as_str())
    }
}

fn ensure_array(value: Value) -> Value {
    if value.is_array() {
        value
    } else {
        Value::Array(Vec::new())
    }
}

fn count_json_array(value: &str) -> usize {
    serde_json::from_str::<Vec<Value>>(value)
        .map(|items| items.len())
        .unwrap_or(0)
}

fn stable_hash(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn catalog_version_is_stable_for_identical_content() {
        let left = McpCatalogSnapshot::from_json_values(
            "server".to_owned(),
            json!({"name":"test"}),
            Some("use carefully"),
            json!([{"name":"send"}]),
            json!([]),
            json!([]),
            json!([]),
            1,
        )
        .unwrap();
        let right = McpCatalogSnapshot::from_json_values(
            "server".to_owned(),
            json!({"name":"test"}),
            Some("use carefully"),
            json!([{"name":"send"}]),
            json!([]),
            json!([]),
            json!([]),
            2,
        )
        .unwrap();

        assert_eq!(left.catalog_version, right.catalog_version);
        assert_eq!(left.tools_count(), 1);
    }

    #[test]
    fn catalog_version_changes_when_tools_change() {
        let left = McpCatalogSnapshot::from_json_values(
            "server".to_owned(),
            json!({"name":"test"}),
            None,
            json!([{"name":"send"}]),
            json!([]),
            json!([]),
            json!([]),
            1,
        )
        .unwrap();
        let right = McpCatalogSnapshot::from_json_values(
            "server".to_owned(),
            json!({"name":"test"}),
            None,
            json!([{"name":"send"},{"name":"domains"}]),
            json!([]),
            json!([]),
            json!([]),
            1,
        )
        .unwrap();

        assert_ne!(left.catalog_version, right.catalog_version);
        assert_eq!(right.tools_count(), 2);
    }
}
