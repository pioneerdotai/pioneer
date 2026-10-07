//! Agent Plugin parent contracts. Capability expansion is server-derived only.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PluginOwner {
    pub plugin_id: String,
    pub member_key: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginsSourceParams {
    pub workspace_id: String,
    pub upload_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<PluginPreviewTarget>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginPreviewTarget {
    pub plugin_id: String,
    pub expected_revision: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginsInstallParams {
    pub workspace_id: String,
    pub upload_id: String,
    pub expected_fingerprint: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginsListParams {
    pub workspace_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginsDetailsParams {
    pub workspace_id: String,
    pub plugin_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PluginDiagnostic {
    pub code: String,
    pub path: String,
    pub message: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PluginComponentItem {
    pub kind: String,
    pub member_key: String,
    pub status: String,
    pub diagnostic: Option<String>,
    pub skill_id: Option<crate::SkillId>,
    pub mcp_installation_id: Option<String>,
    pub runtime_status: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PluginItem {
    pub id: String,
    pub name: String,
    pub version: Option<String>,
    pub enabled: bool,
    pub state: String,
    pub revision: i64,
    pub status: String,
    pub components: Vec<PluginComponentItem>,
    pub diagnostics: Vec<PluginDiagnostic>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PluginsListResponse {
    pub plugins: Vec<PluginItem>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PluginsPreviewResponse {
    pub name: String,
    pub version: Option<String>,
    pub fingerprint: String,
    pub components: Vec<PluginComponentItem>,
    pub diagnostics: Vec<PluginDiagnostic>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PluginsChangedNotification {
    pub workspace_id: String,
    pub plugin_id: String,
    pub revision: i64,
}

/// Persisted on the existing turn. This is never accepted in a client request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PluginSelectionSnapshot {
    pub parents: Vec<PluginSelectedParent>,
    pub children: Vec<PluginSelectedChild>,
    pub phase: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PluginSelectedParent {
    pub id: String,
    pub revision: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PluginSelectedChild {
    pub kind: String,
    pub id: String,
    pub parent_id: String,
}

#[cfg(test)]
mod tests {
    // Source regressions only. NOT_RUN / NOT_COMPILED.
    #[test]
    fn install_request_rejects_client_expansion_and_ownership() {
        let valid = serde_json::json!({"workspace_id":"ws", "upload_id":"upload", "expected_fingerprint":"digest"});
        assert!(serde_json::from_value::<super::PluginsInstallParams>(valid.clone()).is_ok());
        for field in ["children", "ownership", "plugin_selection_json"] {
            let mut forged = valid.clone();
            forged[field] = serde_json::json!([]);
            assert!(serde_json::from_value::<super::PluginsInstallParams>(forged).is_err());
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginsSetEnabledParams {
    pub workspace_id: String,
    pub plugin_id: String,
    pub expected_revision: i64,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginComponentKey {
    pub kind: String,
    pub member_key: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginsUpdatePreviewParams {
    pub workspace_id: String,
    pub plugin_id: String,
    pub expected_revision: i64,
    pub upload_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PluginsUpdatePreviewResponse {
    pub package: PluginsPreviewResponse,
    pub added: Vec<PluginComponentKey>,
    pub updated: Vec<PluginComponentKey>,
    pub identity_resets: Vec<PluginComponentKey>,
    pub removed: Vec<PluginComponentKey>,
    /// Conservative disclosure of changed MCP configurations that may need sign-in.
    pub authorization_changes: Vec<PluginComponentKey>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum PluginManagementIntent {
    Update {
        upload_id: String,
        expected_fingerprint: String,
        confirm_changes: bool,
    },
    Retry {
        components: Vec<PluginComponentKey>,
        restore_removed: bool,
    },
    Continue,
    Remove {
        purge_data: bool,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginsMutateParams {
    pub workspace_id: String,
    pub plugin_id: String,
    pub expected_revision: i64,
    pub intent: PluginManagementIntent,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PluginsMutationResponse {
    pub plugin: Option<PluginItem>,
    pub removed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginsUpdateParams {
    pub workspace_id: String,
    pub plugin_id: String,
    pub expected_revision: i64,
    pub upload_id: String,
    pub expected_fingerprint: String,
    pub confirm_changes: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginsRemoveParams {
    pub workspace_id: String,
    pub plugin_id: String,
    pub expected_revision: i64,
    pub purge_data: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginsRetryParams {
    pub workspace_id: String,
    pub plugin_id: String,
    pub expected_revision: i64,
    pub components: Option<Vec<PluginComponentKey>>,
    #[serde(default)]
    pub restore_removed: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginsContinueParams {
    pub workspace_id: String,
    pub plugin_id: String,
    pub expected_revision: i64,
}
