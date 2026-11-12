//! Transient joined package projection. This is not an Entity or a second lifecycle store.
//! Reads/writes continue through genuinely generated package/domain Entities.
use pioneer_entity::{package_installation, plugin_installation};
pub use pioneer_protocol::PackageKind;
use sea_orm::Set;
use sea_orm::prelude::DateTimeWithTimeZone;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageRecord {
    pub kind: PackageKind,
    /// Validated App metadata snapshot, interpreted outside database capacity.
    pub app_manifest_json: Option<String>,
    pub app_localizations_json: Option<String>,
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub version: Option<String>,
    pub display_name: Option<String>,
    pub short_description: Option<String>,
    pub description: Option<String>,
    pub long_description: Option<String>,
    pub developer_name: Option<String>,
    pub author_name: Option<String>,
    pub author_email: Option<String>,
    pub author_url: Option<String>,
    pub homepage: Option<String>,
    pub repository: Option<String>,
    pub documentation_url: Option<String>,
    pub support_url: Option<String>,
    pub privacy_policy_url: Option<String>,
    pub terms_of_service_url: Option<String>,
    pub license: Option<String>,
    pub category: Option<String>,
    pub brand_color: Option<String>,
    pub logo_path: Option<String>,
    pub logo_revision: Option<String>,
    pub composer_icon_path: Option<String>,
    pub composer_icon_revision: Option<String>,
    pub source_upload_id: String,
    pub package_path: String,
    pub data_path: String,
    pub package_fingerprint: String,
    pub enabled: bool,
    pub allow_implicit_invocation: bool,
    pub state: String,
    pub revision: i64,
    pub pending_json: Option<String>,
    pub last_error: Option<String>,
    /// Original structured diagnostic retained across unrelated lifecycle writes.
    pub operation_error_json: Option<String>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}
impl PackageRecord {
    /// Prepare before entering database capacity. The commit revalidates scope,
    /// kind, revision and unique identity through generated Entities and constraints.
    pub(crate) fn validate_domain(&self) -> anyhow::Result<()> {
        match self.kind {
            PackageKind::Plugin => anyhow::ensure!(
                self.app_manifest_json.is_none() && self.app_localizations_json.is_none(),
                "Plugin cannot contain App extension"
            ),
            PackageKind::App => {
                let bytes = self
                    .app_manifest_json
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("App manifest missing"))?;
                let manifest = pioneer_plugins::app::AppManifest::parse(bytes.as_bytes())?;
                anyhow::ensure!(
                    manifest.slug() == self.name,
                    "App slug does not match manifest"
                );
                let translations = self
                    .app_localizations_json
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("App translations missing"))?;
                manifest.validate_localizations_json(translations.as_bytes())?;
            }
        }
        Ok(())
    }
    pub(crate) fn project(
        parent: package_installation::Model,
        plugin: Option<plugin_installation::Model>,
        app: Option<pioneer_entity::app_installation::Model>,
    ) -> anyhow::Result<Self> {
        let kind = PackageKind::from_storage(&parent.kind).map_err(anyhow::Error::msg)?;
        anyhow::ensure!(
            match kind {
                PackageKind::Plugin => plugin.is_some() && app.is_none(),
                PackageKind::App => app.is_some() && plugin.is_none(),
            },
            "package extension mismatch"
        );
        let app_slug = app.as_ref().map(|a| a.slug.clone());
        let last_error = parent
            .last_error_json
            .as_deref()
            .map(decode_operation_error);
        let mut row = Self {
            kind,
            app_manifest_json: app.as_ref().map(|a| a.manifest_json.clone()),
            app_localizations_json: app.map(|a| a.localizations_json),
            id: parent.id,
            workspace_id: parent.workspace_id,
            source_upload_id: parent.source_upload_id,
            package_path: parent.package_path,
            data_path: parent.data_path,
            package_fingerprint: parent.package_fingerprint,
            enabled: parent.enabled,
            allow_implicit_invocation: parent.allow_implicit_invocation,
            state: parent.state,
            revision: parent.revision,
            pending_json: parent.pending_json,
            last_error: last_error,
            operation_error_json: parent.last_error_json,
            created_at: parent.created_at,
            updated_at: parent.updated_at,
            name: plugin.as_ref().map(|p| p.name.clone()).unwrap_or_default(),
            version: plugin.as_ref().and_then(|p| p.version.clone()),
            display_name: plugin.as_ref().and_then(|p| p.display_name.clone()),
            short_description: plugin.as_ref().and_then(|p| p.short_description.clone()),
            description: plugin.as_ref().and_then(|p| p.description.clone()),
            long_description: plugin.as_ref().and_then(|p| p.long_description.clone()),
            developer_name: plugin.as_ref().and_then(|p| p.developer_name.clone()),
            author_name: plugin.as_ref().and_then(|p| p.author_name.clone()),
            author_email: plugin.as_ref().and_then(|p| p.author_email.clone()),
            author_url: plugin.as_ref().and_then(|p| p.author_url.clone()),
            homepage: plugin.as_ref().and_then(|p| p.homepage.clone()),
            repository: plugin.as_ref().and_then(|p| p.repository.clone()),
            documentation_url: plugin.as_ref().and_then(|p| p.documentation_url.clone()),
            support_url: plugin.as_ref().and_then(|p| p.support_url.clone()),
            privacy_policy_url: plugin.as_ref().and_then(|p| p.privacy_policy_url.clone()),
            terms_of_service_url: plugin.as_ref().and_then(|p| p.terms_of_service_url.clone()),
            license: plugin.as_ref().and_then(|p| p.license.clone()),
            category: plugin.as_ref().and_then(|p| p.category.clone()),
            brand_color: plugin.as_ref().and_then(|p| p.brand_color.clone()),
            logo_path: plugin.as_ref().and_then(|p| p.logo_path.clone()),
            logo_revision: plugin.as_ref().and_then(|p| p.logo_revision.clone()),
            composer_icon_path: plugin.as_ref().and_then(|p| p.composer_icon_path.clone()),
            composer_icon_revision: plugin
                .as_ref()
                .and_then(|p| p.composer_icon_revision.clone()),
        };
        // App identity comes from the extension, without parsing JSON under a DB transaction.
        if let Some(slug) = app_slug {
            row.name = slug;
        }
        Ok(row)
    }
    pub(crate) fn operational(&self) -> package_installation::ActiveModel {
        package_installation::ActiveModel {
            kind: Set(self.kind.as_str().into()),
            id: Set(self.id.clone()),
            workspace_id: Set(self.workspace_id.clone()),
            source_upload_id: Set(self.source_upload_id.clone()),
            package_path: Set(self.package_path.clone()),
            data_path: Set(self.data_path.clone()),
            package_fingerprint: Set(self.package_fingerprint.clone()),
            enabled: Set(self.enabled.clone()),
            allow_implicit_invocation: Set(self.allow_implicit_invocation.clone()),
            state: Set(self.state.clone()),
            revision: Set(self.revision.clone()),
            pending_json: Set(self.pending_json.clone()),
            last_error_json: Set(self
                .operation_error_json
                .clone()
                .or_else(|| self.last_error.as_deref().map(encode_operation_error))),
            created_at: Set(self.created_at.clone()),
            updated_at: Set(self.updated_at.clone()),
            ..Default::default()
        }
    }
    pub(crate) fn plugin_metadata(&self) -> plugin_installation::ActiveModel {
        plugin_installation::ActiveModel {
            id: Set(self.id.clone()),
            name: Set(self.name.clone()),
            version: Set(self.version.clone()),
            display_name: Set(self.display_name.clone()),
            short_description: Set(self.short_description.clone()),
            description: Set(self.description.clone()),
            long_description: Set(self.long_description.clone()),
            developer_name: Set(self.developer_name.clone()),
            author_name: Set(self.author_name.clone()),
            author_email: Set(self.author_email.clone()),
            author_url: Set(self.author_url.clone()),
            homepage: Set(self.homepage.clone()),
            repository: Set(self.repository.clone()),
            documentation_url: Set(self.documentation_url.clone()),
            support_url: Set(self.support_url.clone()),
            privacy_policy_url: Set(self.privacy_policy_url.clone()),
            terms_of_service_url: Set(self.terms_of_service_url.clone()),
            license: Set(self.license.clone()),
            category: Set(self.category.clone()),
            brand_color: Set(self.brand_color.clone()),
            logo_path: Set(self.logo_path.clone()),
            logo_revision: Set(self.logo_revision.clone()),
            composer_icon_path: Set(self.composer_icon_path.clone()),
            composer_icon_revision: Set(self.composer_icon_revision.clone()),
            ..Default::default()
        }
    }
}
/// Preserve historical exact codes (including retained-data markers). Invalid
/// storage stays closed rather than erasing a diagnostic during projection.
pub(crate) fn decode_operation_error(value: &str) -> String {
    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|v| {
            v.get("legacy")
                .or_else(|| v.get("code"))
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "packages.invalid_operation_error".into())
}
pub(crate) fn encode_operation_error(value: &str) -> String {
    serde_json::json!({"code": value.chars().take(4096).collect::<String>()}).to_string()
}

/// Validated App publication prepared without holding database capacity.
#[derive(Clone, Debug)]
pub struct PreparedAppMetadata {
    pub(crate) slug: String,
    pub(crate) manifest_json: String,
    pub(crate) localizations_json: String,
    pub(crate) fingerprint: String,
}
impl PreparedAppMetadata {
    pub fn from_package(package: &pioneer_plugins::app::LoadedAppPackage) -> anyhow::Result<Self> {
        let manifest_json = serde_json::to_string(package.manifest.document())?;
        let localizations_json = serde_json::to_string(&package.localizations)?;
        package
            .manifest
            .validate_localizations_json(localizations_json.as_bytes())?;
        Ok(Self {
            slug: package.manifest.slug().into(),
            manifest_json,
            localizations_json,
            fingerprint: package.tree_digest.clone(),
        })
    }
}
