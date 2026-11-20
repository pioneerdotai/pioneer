//! Installed Apps are a product domain distinct from provider-facing RuntimeAppInfo.
/// Stable Apps admission failure for an unsupported Gateway host.
pub const APPS_UNSUPPORTED_HOST_PLATFORM: &str = "apps.unsupported_host_platform";

use crate::{
    PluginComponentItem, PluginComponentKey, PluginDiagnostic, PluginItem, PluginMetadata,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppsSourceParams {
    pub workspace_id: String,
    pub upload_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<AppPreviewTarget>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppPreviewTarget {
    pub app_id: String,
    pub expected_revision: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppsInstallParams {
    pub workspace_id: String,
    pub upload_id: String,
    pub expected_fingerprint: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppsListParams {
    pub workspace_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppsDetailsParams {
    pub workspace_id: String,
    pub app_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppsSetEnabledParams {
    pub workspace_id: String,
    pub app_id: String,
    pub expected_revision: i64,
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_implicit_invocation: Option<bool>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppsUpdateParams {
    pub workspace_id: String,
    pub app_id: String,
    pub expected_revision: i64,
    pub upload_id: String,
    pub expected_fingerprint: String,
    pub confirm_changes: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppsRemoveParams {
    pub workspace_id: String,
    pub app_id: String,
    pub expected_revision: i64,
    pub purge_data: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppsRetryParams {
    pub workspace_id: String,
    pub app_id: String,
    pub expected_revision: i64,
    pub components: Option<Vec<PluginComponentKey>>,
    #[serde(default)]
    pub restore_removed: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppsContinueParams {
    pub workspace_id: String,
    pub app_id: String,
    pub expected_revision: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct AppsPreviewResponse {
    pub name: String,
    pub version: Option<String>,
    /// Authored package description, bounded by the Gateway; absent for old peers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 2048))]
    pub description: Option<String>,
    pub fingerprint: String,
    pub components: Vec<PluginComponentItem>,
    pub diagnostics: Vec<PluginDiagnostic>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct AppsUpdatePreviewResponse {
    pub package: AppsPreviewResponse,
    /// Package bytes changed outside a changed member (for example shared assets).
    #[serde(default)]
    pub supporting_files_changed: bool,
    pub added: Vec<PluginComponentKey>,
    pub updated: Vec<PluginComponentKey>,
    pub identity_resets: Vec<PluginComponentKey>,
    pub removed: Vec<PluginComponentKey>,
    /// Conservative disclosure of changed MCP configurations that may need sign-in.
    pub authorization_changes: Vec<PluginComponentKey>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct AppItem {
    pub id: String,
    pub slug: String,
    pub name: String,
    pub version: Option<String>,
    /// Authored package description, bounded by the Gateway; absent for old peers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 2048))]
    pub description: Option<String>,
    #[serde(default)]
    pub metadata: PluginMetadata,
    pub enabled: bool,
    #[serde(default)]
    pub allow_implicit_invocation: bool,
    pub state: String,
    pub revision: i64,
    pub status: String,
    pub components: Vec<PluginComponentItem>,
    pub diagnostics: Vec<PluginDiagnostic>,
}

impl AppItem {
    pub fn from_package(summary: PluginItem) -> Self {
        Self {
            slug: summary.name.clone(),
            name: summary
                .metadata
                .display_name
                .clone()
                .unwrap_or_else(|| summary.name.clone()),
            id: summary.id,
            version: summary.version,
            description: summary.description,
            metadata: summary.metadata,
            enabled: summary.enabled,
            allow_implicit_invocation: summary.allow_implicit_invocation,
            state: summary.state,
            revision: summary.revision,
            status: summary.status,
            components: summary.components,
            diagnostics: summary.diagnostics,
        }
    }
    pub fn into_package(self) -> PluginItem {
        PluginItem {
            id: self.id,
            name: self.slug,
            version: self.version,
            description: self.description,
            metadata: self.metadata,
            enabled: self.enabled,
            allow_implicit_invocation: self.allow_implicit_invocation,
            state: self.state,
            revision: self.revision,
            status: self.status,
            components: self.components,
            diagnostics: self.diagnostics,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct AppsListResponse {
    pub apps: Vec<AppItem>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct AppsMutationResponse {
    pub app: Option<AppItem>,
    pub removed: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct AppsChangedNotification {
    #[serde(default)]
    pub runtime_only: bool,
    #[serde(default)]
    pub execution_id: Option<String>,
    pub workspace_id: String,
    pub app_id: String,
    pub revision: i64,
}

impl AppsPreviewResponse {
    pub fn into_package(self) -> crate::PluginsPreviewResponse {
        crate::PluginsPreviewResponse {
            name: self.name,
            version: self.version,
            description: self.description,
            fingerprint: self.fingerprint,
            components: self.components,
            diagnostics: self.diagnostics,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct AppThemedIcon {
    pub light: crate::ImageReference,
    pub dark: crate::ImageReference,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AppLocalization {
    pub name: Option<String>,
    pub short_description: Option<String>,
    pub long_description: Option<String>,
}
/// Verified presentation snapshot only; native authority remains in components.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct AppMetadata {
    #[serde(default)]
    pub has_ui: bool,
    #[serde(default)]
    pub icon: Option<AppThemedIcon>,
    pub composer_icon: Option<AppThemedIcon>,
    pub developer_logo: Option<AppThemedIcon>,
    pub developer_url: Option<String>,
    pub categories: Vec<String>,
    pub default_locale: Option<String>,
    pub localizations: std::collections::BTreeMap<String, AppLocalization>,
}
impl AppMetadata {
    pub fn localized(
        &self,
        locale: &str,
        field: fn(&AppLocalization) -> Option<&str>,
    ) -> Option<String> {
        let exact = canonical_app_locale(locale).ok();
        let base = exact.as_deref().and_then(|s| s.split('-').next());
        let default = self
            .default_locale
            .as_deref()
            .and_then(|s| canonical_app_locale(s).ok());
        exact
            .as_deref()
            .into_iter()
            .chain(base)
            .chain(default.as_deref())
            .find_map(|key| self.localizations.get(key).and_then(field))
            .map(str::to_owned)
    }
}

#[cfg(test)]
mod tests {
    // NOT_RUN / NOT_COMPILED. Parent-only public wire authority.
    #[test]
    fn app_identity_rejects_children_and_wrong_canonical_key() {
        let id = "A".repeat(21);
        let cap = crate::package_capability(crate::PackageKind::App, &id, 3);
        let value = serde_json::to_value(&cap).unwrap();
        assert_eq!(value["id"], format!("app:{id}"));
        assert_eq!(
            serde_json::from_value::<crate::TurnCapability>(value.clone()).unwrap(),
            cap
        );
        let mut injected = value.clone();
        injected["kind"]["children"] = serde_json::json!([]);
        assert!(serde_json::from_value::<crate::TurnCapability>(injected).is_err());
        let mut wrong = value;
        wrong["id"] = format!("plugin:{id}").into();
        assert!(serde_json::from_value::<crate::TurnCapability>(wrong).is_err());
    }
    #[test]
    fn app_receipts_cannot_be_reinterpreted_as_legacy_plugin_owner() {
        let app = crate::PackageIdentity {
            kind: crate::PackageKind::App,
            id: "A".repeat(21),
        };
        assert_eq!(
            crate::PackageIdentity::resolve(Some(&app), None).unwrap(),
            Some(app.clone())
        );
        assert!(crate::PackageIdentity::resolve(Some(&app), Some(&app.id)).is_err());
        assert_eq!(
            crate::PackageIdentity::resolve(None, Some(&app.id))
                .unwrap()
                .unwrap()
                .kind,
            crate::PackageKind::Plugin
        );
    }
}

/// One locale parser for App manifests, stored snapshots and client presentation.
pub fn canonical_app_locale(value: &str) -> Result<String, &'static str> {
    value
        .parse::<icu_locale_core::LanguageIdentifier>()
        .map(|locale| locale.to_string())
        .map_err(|_| "invalid_locale")
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppViewParams {
    pub workspace_id: String,
    pub app_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct AppViewBootstrap {
    #[serde(default)]
    pub development: bool,
    /// Verified local authoring URL. The relative URL remains a native-only
    /// lease handle for dev views and must never be loaded into their browser.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dev_url: Option<String>,
    pub package_revision: i64,
    pub relative_url: crate::AuthSecretString,
    pub expires_at: u64,
    pub execution_id: String,
    pub title: String,
}

/// Installation/component state remains on AppItem. This is live execution state.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct AppRuntimeInfo {
    pub state: String,
    pub execution_id: Option<String>,
    pub diagnostic: Option<String>,
    pub logs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppViewLeaseParams {
    pub relative_url: crate::AuthSecretString,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct AppViewLeaseResponse {
    pub expires_at: u64,
}

/// Local authoring connection. The Gateway reads project files on its host.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppsDevAddParams {
    pub workspace_id: String,
    pub project: String,
    pub url: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AppDevConnection {
    pub id: String,
    pub name: String,
    pub project: String,
    pub url: String,
    pub state: String,
    pub diagnostic: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", content = "params", rename_all = "snake_case")]
pub enum DevAppOperation {
    Add(AppsDevAddParams),
    List(AppsListParams),
    Remove(AppsDetailsParams),
}
