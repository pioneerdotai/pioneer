use std::{collections::BTreeSet, path::Path, sync::Arc};

use anyhow::{Context, Result, bail};
use pioneer_keystore::{
    DbKeyStore, DbKeyStoreConfig, SecretEntryMeta, SecretFilter, SecretId, SecretKind, SecretMeta,
    SecretStore,
};
use tracing::warn;

use crate::helpers::{decode_hex, encode_hex, unix_timestamp_secs};

const WORKSPACE_PROVIDER_API_KEY_PREFIX: &str = "workspace:";

#[derive(Clone)]
pub(crate) struct GatewaySecrets {
    store: Arc<dyn SecretStore>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct McpSecretDeleteReport {
    pub attempted: usize,
    pub deleted: usize,
    pub missing: usize,
    pub failed: Vec<McpSecretDeleteFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct McpSecretDeleteFailure {
    pub ref_id: String,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SuperuserJwtMaterialRotation {
    pub material_existed: bool,
    pub rotated_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProviderSecretWorkspaceMigrationReport {
    pub workspace_id: String,
    pub legacy_keys: usize,
    pub copied: usize,
    pub skipped_existing: usize,
    pub deleted_legacy: usize,
}

impl GatewaySecrets {
    pub(crate) fn open(runtime_home: &Path) -> Result<Self> {
        let store = DbKeyStore::open(DbKeyStoreConfig::for_runtime_home(runtime_home))
            .context("failed to open gateway keystore")?;
        Ok(Self::new(Arc::new(store)))
    }

    pub(crate) fn new(store: Arc<dyn SecretStore>) -> Self {
        Self { store }
    }

    pub(crate) fn normalize_provider_name(&self, provider: &str) -> Result<String> {
        Ok(SecretId::provider_api_key(provider)
            .context("invalid provider name")?
            .user()
            .to_owned())
    }

    pub(crate) fn get_provider_api_key(&self, provider: &str) -> Result<Option<String>> {
        let id = SecretId::provider_api_key(provider).context("invalid provider name")?;
        self.store
            .get_string(&id)
            .context("failed to read provider api key from keystore")
    }

    pub(crate) fn get_workspace_provider_api_key(
        &self,
        workspace_id: &str,
        provider: &str,
    ) -> Result<Option<String>> {
        let id = SecretId::workspace_provider_api_key(workspace_id, provider)
            .context("invalid workspace provider api key id")?;
        self.store
            .get_string(&id)
            .context("failed to read workspace provider api key from keystore")
    }

    #[allow(dead_code)]
    pub(crate) fn set_provider_api_key(&self, provider: &str, api_key: &str) -> Result<String> {
        if api_key.trim().is_empty() {
            bail!("provider api key must not be empty");
        }

        let id = SecretId::provider_api_key(provider).context("invalid provider name")?;
        let normalized_provider = id.user().to_owned();
        let now = current_unix_i64()?;
        let created_at = self
            .existing_provider_secret_meta(&id)?
            .and_then(|entry| entry.created_at_unix)
            .unwrap_or(now);

        self.store
            .put_string(
                &id,
                api_key,
                SecretMeta {
                    kind: SecretKind::ProviderApiKey,
                    label: Some(normalized_provider.clone()),
                    created_at_unix: created_at,
                    updated_at_unix: now,
                },
            )
            .context("failed to write provider api key to keystore")?;

        Ok(normalized_provider)
    }

    pub(crate) fn set_workspace_provider_api_key(
        &self,
        workspace_id: &str,
        provider: &str,
        api_key: &str,
    ) -> Result<String> {
        if api_key.trim().is_empty() {
            bail!("provider api key must not be empty");
        }

        let id = SecretId::workspace_provider_api_key(workspace_id, provider)
            .context("invalid workspace provider api key id")?;
        let normalized_provider = Self::provider_name_from_workspace_provider_id(&id)
            .unwrap_or_else(|| provider.trim().to_ascii_lowercase());
        let now = current_unix_i64()?;
        let created_at = self
            .existing_provider_secret_meta(&id)?
            .and_then(|entry| entry.created_at_unix)
            .unwrap_or(now);

        self.store
            .put_string(
                &id,
                api_key,
                SecretMeta {
                    kind: SecretKind::ProviderApiKey,
                    label: Some(normalized_provider.clone()),
                    created_at_unix: created_at,
                    updated_at_unix: now,
                },
            )
            .context("failed to write workspace provider api key to keystore")?;

        Ok(normalized_provider)
    }

    #[allow(dead_code)]
    pub(crate) fn delete_provider_api_key(&self, provider: &str) -> Result<(String, bool)> {
        let id = SecretId::provider_api_key(provider).context("invalid provider name")?;
        let normalized_provider = id.user().to_owned();
        let deleted = self
            .store
            .delete(&id)
            .context("failed to delete provider api key from keystore")?;
        Ok((normalized_provider, deleted))
    }

    pub(crate) fn delete_workspace_provider_api_key(
        &self,
        workspace_id: &str,
        provider: &str,
    ) -> Result<(String, bool)> {
        let id = SecretId::workspace_provider_api_key(workspace_id, provider)
            .context("invalid workspace provider api key id")?;
        let normalized_provider = Self::provider_name_from_workspace_provider_id(&id)
            .unwrap_or_else(|| provider.trim().to_ascii_lowercase());
        let deleted = self
            .store
            .delete(&id)
            .context("failed to delete workspace provider api key from keystore")?;
        Ok((normalized_provider, deleted))
    }

    #[allow(dead_code)]
    pub(crate) fn list_configured_provider_names(&self) -> Result<Vec<String>> {
        let entries = self
            .store
            .list(SecretFilter::Kind(SecretKind::ProviderApiKey))
            .context("failed to list provider api keys from keystore")?;

        let mut names = BTreeSet::new();
        for entry in entries {
            names.insert(entry.label.unwrap_or_else(|| entry.id.user().to_owned()));
        }

        Ok(names.into_iter().collect())
    }

    pub(crate) fn list_configured_workspace_provider_names(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<String>> {
        let prefix = Self::workspace_provider_secret_user_prefix(workspace_id)?;
        let entries = self
            .store
            .list(SecretFilter::Kind(SecretKind::ProviderApiKey))
            .context("failed to list provider api keys from keystore")?;

        let mut names = BTreeSet::new();
        for entry in entries {
            if !entry.id.user().starts_with(prefix.as_str()) {
                continue;
            }

            names.insert(
                Self::provider_name_from_workspace_provider_id(&entry.id)
                    .unwrap_or_else(|| entry.label.unwrap_or_else(|| entry.id.user().to_owned())),
            );
        }

        Ok(names.into_iter().collect())
    }

    pub(crate) fn resolve_provider_api_key(&self, provider_name: &str) -> String {
        if is_local_provider(provider_name) {
            return String::new();
        }

        match self.get_provider_api_key(provider_name) {
            Ok(Some(value)) => value,
            Ok(None) => String::new(),
            Err(error) => {
                warn!(
                    provider = provider_name,
                    error = %format!("{error:#}"),
                    "failed to resolve provider api key from keystore"
                );
                String::new()
            }
        }
    }

    pub(crate) fn resolve_workspace_provider_api_key(
        &self,
        workspace_id: &str,
        provider_name: &str,
    ) -> String {
        if is_local_provider(provider_name) {
            return String::new();
        }

        match self.get_workspace_provider_api_key(workspace_id, provider_name) {
            Ok(Some(value)) => value,
            Ok(None) => String::new(),
            Err(error) => {
                warn!(
                    workspace_id,
                    provider = provider_name,
                    error = %format!("{error:#}"),
                    "failed to resolve workspace provider api key from keystore"
                );
                String::new()
            }
        }
    }

    // TODO: Remove sometime
    pub(crate) fn migrate_legacy_provider_api_keys_to_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<ProviderSecretWorkspaceMigrationReport> {
        let workspace_id = workspace_id.trim().to_owned();
        SecretId::workspace_provider_api_key(workspace_id.as_str(), "validation")
            .context("invalid provider key migration workspace id")?;

        let entries = self
            .store
            .list(SecretFilter::Kind(SecretKind::ProviderApiKey))
            .context("failed to list provider api keys for workspace migration")?;
        let now = current_unix_i64()?;
        let mut report = ProviderSecretWorkspaceMigrationReport {
            workspace_id: workspace_id.clone(),
            legacy_keys: 0,
            copied: 0,
            skipped_existing: 0,
            deleted_legacy: 0,
        };

        for entry in entries {
            if Self::provider_name_from_workspace_provider_id(&entry.id).is_some() {
                continue;
            }

            let provider_id = SecretId::provider_api_key(entry.id.user())
                .with_context(|| format!("invalid legacy provider key `{}`", entry.id.user()))?;
            let provider_name = provider_id.user().to_owned();
            let target_id =
                SecretId::workspace_provider_api_key(workspace_id.as_str(), provider_name.as_str())
                    .context("invalid workspace provider key id")?;

            report.legacy_keys = report.legacy_keys.saturating_add(1);
            if self
                .store
                .exists(&target_id)
                .context("failed to check workspace provider api key during migration")?
            {
                report.skipped_existing = report.skipped_existing.saturating_add(1);
                if self
                    .store
                    .delete(&entry.id)
                    .context("failed to delete migrated legacy provider api key")?
                {
                    report.deleted_legacy = report.deleted_legacy.saturating_add(1);
                }
                continue;
            }

            let Some(api_key) = self
                .store
                .get_string(&entry.id)
                .context("failed to read legacy provider api key during migration")?
            else {
                if self
                    .store
                    .delete(&entry.id)
                    .context("failed to delete empty legacy provider api key")?
                {
                    report.deleted_legacy = report.deleted_legacy.saturating_add(1);
                }
                continue;
            };

            self.store
                .put_string(
                    &target_id,
                    api_key.as_str(),
                    SecretMeta {
                        kind: SecretKind::ProviderApiKey,
                        label: Some(provider_name),
                        created_at_unix: entry.created_at_unix.unwrap_or(now),
                        updated_at_unix: entry.updated_at_unix.unwrap_or(now),
                    },
                )
                .context("failed to write migrated workspace provider api key")?;
            report.copied = report.copied.saturating_add(1);
            if self
                .store
                .delete(&entry.id)
                .context("failed to delete migrated legacy provider api key")?
            {
                report.deleted_legacy = report.deleted_legacy.saturating_add(1);
            }
        }

        Ok(report)
    }

    fn workspace_provider_secret_user_prefix(workspace_id: &str) -> Result<String> {
        const SENTINEL_PROVIDER: &str = "validation";

        let id = SecretId::workspace_provider_api_key(workspace_id, SENTINEL_PROVIDER)
            .context("invalid provider workspace id")?;
        Ok(id
            .user()
            .strip_suffix(SENTINEL_PROVIDER)
            .unwrap_or(id.user())
            .to_owned())
    }

    fn provider_name_from_workspace_provider_id(id: &SecretId) -> Option<String> {
        id.user()
            .strip_prefix(WORKSPACE_PROVIDER_API_KEY_PREFIX)
            .and_then(|value| value.split_once(":provider:"))
            .map(|(_, provider)| provider.to_owned())
    }

    pub(crate) fn load_or_create_superuser_jwt_material(
        &self,
        size_bytes: usize,
    ) -> Result<Vec<u8>> {
        if size_bytes < 32 {
            bail!("gateway.auth.secret_size_bytes must be at least 32 bytes");
        }

        let id = SecretId::superuser_jwt_token();
        if let Some(stored_hex) = self
            .store
            .get_string(&id)
            .context("failed to read superuser jwt material from keystore")?
        {
            let material = decode_hex(stored_hex.trim())
                .context("failed to decode superuser jwt material from keystore")?;
            ensure_jwt_material_len(material.as_slice())?;
            return Ok(material);
        }

        let mut material = vec![0u8; size_bytes];
        rand::fill(material.as_mut_slice());

        let now = current_unix_i64()?;
        self.store
            .put_string(
                &id,
                encode_hex(material.as_slice()).as_str(),
                SecretMeta::new(
                    SecretKind::SuperuserJwtToken,
                    Some("superuser".to_owned()),
                    now,
                ),
            )
            .context("failed to write superuser jwt material to keystore")?;

        Ok(material)
    }

    pub(crate) fn rotate_superuser_jwt_material(
        &self,
        size_bytes: usize,
    ) -> Result<SuperuserJwtMaterialRotation> {
        if size_bytes < 32 {
            bail!("gateway.auth.secret_size_bytes must be at least 32 bytes");
        }

        let id = SecretId::superuser_jwt_token();
        let material_existed = self
            .store
            .exists(&id)
            .context("failed to check superuser jwt material in keystore")?;
        let now = current_unix_i64()?;
        let created_at = match self.existing_superuser_jwt_meta(&id) {
            Ok(Some(entry)) => entry.created_at_unix.unwrap_or(now),
            Ok(None) => now,
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "failed to read superuser jwt metadata before rotation; replacing metadata"
                );
                now
            }
        };

        let mut material = vec![0u8; size_bytes];
        rand::fill(material.as_mut_slice());

        self.store
            .put_string(
                &id,
                encode_hex(material.as_slice()).as_str(),
                SecretMeta {
                    kind: SecretKind::SuperuserJwtToken,
                    label: Some("superuser".to_owned()),
                    created_at_unix: created_at,
                    updated_at_unix: now.max(created_at),
                },
            )
            .context("failed to rotate superuser jwt material in keystore")?;

        Ok(SuperuserJwtMaterialRotation {
            material_existed,
            rotated_at_unix: now,
        })
    }

    pub(crate) fn list_secret_entries(&self) -> Result<Vec<SecretEntryMeta>> {
        self.store
            .list(SecretFilter::All)
            .context("failed to list secrets from keystore")
    }

    pub(crate) fn get_mcp_secret(&self, ref_id: &str) -> Result<Option<String>> {
        let id = SecretId::mcp_secret(ref_id).context("invalid MCP secret ref id")?;
        self.store
            .get_string(&id)
            .context("failed to read MCP secret from keystore")
    }

    pub(crate) fn put_mcp_secret(
        &self,
        ref_id: &str,
        value: &str,
        label: Option<String>,
    ) -> Result<()> {
        if value.is_empty() {
            bail!("MCP secret value must not be empty");
        }

        let id = SecretId::mcp_secret(ref_id).context("invalid MCP secret ref id")?;
        let now = current_unix_i64()?;
        let created_at = self
            .existing_mcp_secret_meta(&id)?
            .and_then(|entry| entry.created_at_unix)
            .unwrap_or(now);
        let label = label
            .and_then(|label| {
                let trimmed = label.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_owned())
            })
            .unwrap_or_else(|| id.user().to_owned());

        self.store
            .put_string(
                &id,
                value,
                SecretMeta {
                    kind: SecretKind::McpSecret,
                    label: Some(label),
                    created_at_unix: created_at,
                    updated_at_unix: now,
                },
            )
            .context("failed to write MCP secret to keystore")
    }

    pub(crate) fn delete_mcp_secret(&self, ref_id: &str) -> Result<bool> {
        let id = SecretId::mcp_secret(ref_id).context("invalid MCP secret ref id")?;
        self.store
            .delete(&id)
            .context("failed to delete MCP secret from keystore")
    }

    pub(crate) fn delete_mcp_secrets<'a>(
        &self,
        ref_ids: impl IntoIterator<Item = &'a str>,
    ) -> McpSecretDeleteReport {
        let mut report = McpSecretDeleteReport {
            attempted: 0,
            deleted: 0,
            missing: 0,
            failed: Vec::new(),
        };
        let mut seen = BTreeSet::new();

        for ref_id in ref_ids {
            if !seen.insert(ref_id.to_owned()) {
                continue;
            }
            report.attempted = report.attempted.saturating_add(1);
            match self.delete_mcp_secret(ref_id) {
                Ok(true) => report.deleted = report.deleted.saturating_add(1),
                Ok(false) => report.missing = report.missing.saturating_add(1),
                Err(error) => report.failed.push(McpSecretDeleteFailure {
                    ref_id: ref_id.to_owned(),
                    error: format!("{error:#}"),
                }),
            }
        }

        report
    }

    pub(crate) fn list_mcp_secret_refs(&self) -> Result<Vec<String>> {
        let entries = self
            .store
            .list(SecretFilter::Kind(SecretKind::McpSecret))
            .context("failed to list MCP secrets from keystore")?;

        let mut refs = entries
            .into_iter()
            .map(|entry| entry.id.user().to_owned())
            .collect::<Vec<_>>();
        refs.sort();
        refs.dedup();
        Ok(refs)
    }

    fn existing_provider_secret_meta(&self, id: &SecretId) -> Result<Option<SecretEntryMeta>> {
        let entries = self
            .store
            .list(SecretFilter::Kind(SecretKind::ProviderApiKey))
            .context("failed to read provider api key metadata from keystore")?;
        Ok(entries.into_iter().find(|entry| entry.id == *id))
    }

    fn existing_mcp_secret_meta(&self, id: &SecretId) -> Result<Option<SecretEntryMeta>> {
        let entries = self
            .store
            .list(SecretFilter::Kind(SecretKind::McpSecret))
            .context("failed to read MCP secret metadata from keystore")?;
        Ok(entries.into_iter().find(|entry| entry.id == *id))
    }

    fn existing_superuser_jwt_meta(&self, id: &SecretId) -> Result<Option<SecretEntryMeta>> {
        let entries = self
            .store
            .list(SecretFilter::Kind(SecretKind::SuperuserJwtToken))
            .context("failed to read superuser jwt metadata from keystore")?;
        Ok(entries.into_iter().find(|entry| entry.id == *id))
    }
}

pub(crate) fn ensure_jwt_material_len(jwt_material: &[u8]) -> Result<()> {
    if jwt_material.len() < 32 {
        bail!("jwt material is too short");
    }

    Ok(())
}

fn current_unix_i64() -> Result<i64> {
    i64::try_from(unix_timestamp_secs()?).context("current unix timestamp does not fit into i64")
}

fn is_local_provider(provider_name: &str) -> bool {
    matches!(
        provider_name,
        "ollama" | "lmstudio" | "llamacpp" | "sglang" | "vllm" | "osaurus" | "litellm"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_keystore::{
        KeystoreError, MemorySecretStore, Result as KeystoreResult, SecretEntryMeta,
    };

    #[test]
    fn provider_methods_write_list_read_and_delete_key() {
        let secrets = GatewaySecrets::new(Arc::new(MemorySecretStore::new()));

        let normalized = secrets
            .set_provider_api_key("  OpenRouter  ", "sk-test")
            .expect("set provider key");
        assert_eq!(normalized, "openrouter");

        assert_eq!(
            secrets
                .get_provider_api_key("openrouter")
                .expect("read key"),
            Some("sk-test".to_owned())
        );
        assert_eq!(
            secrets
                .list_configured_provider_names()
                .expect("list providers"),
            vec!["openrouter".to_owned()]
        );

        let (normalized, deleted) = secrets
            .delete_provider_api_key("OpenRouter")
            .expect("delete provider key");
        assert_eq!(normalized, "openrouter");
        assert!(deleted);
        assert_eq!(
            secrets
                .get_provider_api_key("openrouter")
                .expect("read deleted key"),
            None
        );
    }

    #[test]
    fn provider_overwrite_preserves_created_at() {
        let store = Arc::new(MemorySecretStore::new());
        let secrets = GatewaySecrets::new(store.clone());

        secrets
            .set_provider_api_key("openrouter", "first")
            .expect("set first");
        let first = store
            .list(SecretFilter::Kind(SecretKind::ProviderApiKey))
            .expect("list first")
            .pop()
            .expect("first meta");

        secrets
            .set_provider_api_key("openrouter", "second")
            .expect("set second");
        let second = store
            .list(SecretFilter::Kind(SecretKind::ProviderApiKey))
            .expect("list second")
            .pop()
            .expect("second meta");

        assert_eq!(first.created_at_unix, second.created_at_unix);
        assert_eq!(
            store
                .get_string(&SecretId::provider_api_key("openrouter").expect("id"))
                .expect("read second"),
            Some("second".to_owned())
        );
    }

    #[test]
    fn legacy_provider_keys_are_moved_to_workspace_scope() {
        let secrets = GatewaySecrets::new(Arc::new(MemorySecretStore::new()));

        secrets
            .set_provider_api_key("OpenRouter", "sk-legacy")
            .expect("set legacy provider key");

        let report = secrets
            .migrate_legacy_provider_api_keys_to_workspace("ws_default")
            .expect("migrate provider keys");
        assert_eq!(
            report,
            ProviderSecretWorkspaceMigrationReport {
                workspace_id: "ws_default".to_owned(),
                legacy_keys: 1,
                copied: 1,
                skipped_existing: 0,
                deleted_legacy: 1,
            }
        );
        assert_eq!(
            secrets
                .get_workspace_provider_api_key("ws_default", "openrouter")
                .expect("read migrated key"),
            Some("sk-legacy".to_owned())
        );
        assert_eq!(
            secrets
                .get_provider_api_key("openrouter")
                .expect("legacy key removed"),
            None
        );
        assert_eq!(
            secrets
                .list_configured_workspace_provider_names("ws_default")
                .expect("list migrated workspace providers"),
            vec!["openrouter".to_owned()]
        );
        assert!(
            secrets
                .list_configured_workspace_provider_names("ws_other")
                .expect("list other workspace providers")
                .is_empty()
        );

        let second_report = secrets
            .migrate_legacy_provider_api_keys_to_workspace("ws_default")
            .expect("rerun migration");
        assert_eq!(second_report.legacy_keys, 0);
        assert_eq!(second_report.copied, 0);
        assert_eq!(second_report.skipped_existing, 0);
        assert_eq!(second_report.deleted_legacy, 0);
    }

    #[test]
    fn legacy_provider_migration_does_not_overwrite_scoped_key() {
        let secrets = GatewaySecrets::new(Arc::new(MemorySecretStore::new()));

        secrets
            .set_provider_api_key("openrouter", "sk-legacy")
            .expect("set legacy provider key");
        secrets
            .set_workspace_provider_api_key("ws_default", "openrouter", "sk-scoped")
            .expect("set scoped provider key");

        let report = secrets
            .migrate_legacy_provider_api_keys_to_workspace("ws_default")
            .expect("migrate provider keys");

        assert_eq!(report.legacy_keys, 1);
        assert_eq!(report.copied, 0);
        assert_eq!(report.skipped_existing, 1);
        assert_eq!(report.deleted_legacy, 1);
        assert_eq!(
            secrets
                .get_workspace_provider_api_key("ws_default", "openrouter")
                .expect("read scoped key"),
            Some("sk-scoped".to_owned())
        );
        assert_eq!(
            secrets
                .get_provider_api_key("openrouter")
                .expect("legacy key removed"),
            None
        );
    }

    #[test]
    fn provider_methods_reject_empty_key() {
        let secrets = GatewaySecrets::new(Arc::new(MemorySecretStore::new()));

        let error = secrets
            .set_provider_api_key("openrouter", "   ")
            .expect_err("empty api key should fail");
        assert!(
            format!("{error:#}").contains("provider api key must not be empty"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn provider_resolver_does_not_touch_store_for_local_providers() {
        let secrets = GatewaySecrets::new(Arc::new(FailingSecretStore));

        assert_eq!(secrets.resolve_provider_api_key("ollama"), "");
    }

    #[test]
    fn mcp_methods_write_list_read_and_delete_secret() {
        let store = Arc::new(MemorySecretStore::new());
        let secrets = GatewaySecrets::new(store.clone());
        let ref_id = "gateway_settings:mcp:workspace:ws:resend:env:RESEND_API_KEY";

        secrets
            .put_mcp_secret(
                ref_id,
                "re_secret",
                Some("resend:env:RESEND_API_KEY".to_owned()),
            )
            .expect("put MCP secret");

        assert_eq!(
            store
                .get_string(&SecretId::mcp_secret(ref_id).expect("mcp id"))
                .expect("raw read"),
            Some("re_secret".to_owned())
        );
        assert_eq!(
            secrets.get_mcp_secret(ref_id).expect("read MCP secret"),
            Some("re_secret".to_owned())
        );
        assert_eq!(
            secrets.list_mcp_secret_refs().expect("list MCP refs"),
            vec![ref_id.to_owned()]
        );

        assert!(secrets.delete_mcp_secret(ref_id).expect("delete existing"));
        assert_eq!(
            secrets
                .get_mcp_secret(ref_id)
                .expect("read deleted MCP secret"),
            None
        );
        assert!(!secrets.delete_mcp_secret(ref_id).expect("delete missing"));
    }

    #[test]
    fn mcp_overwrite_preserves_created_at_and_updates_metadata() {
        let store = Arc::new(MemorySecretStore::new());
        let secrets = GatewaySecrets::new(store.clone());
        let ref_id = "gateway_settings:mcp:workspace:ws:resend:header:authorization";
        let id = SecretId::mcp_secret(ref_id).expect("mcp id");

        store
            .put_string(
                &id,
                "old",
                SecretMeta {
                    kind: SecretKind::McpSecret,
                    label: Some("old label".to_owned()),
                    created_at_unix: 123,
                    updated_at_unix: 456,
                },
            )
            .expect("seed MCP secret");

        secrets
            .put_mcp_secret(
                ref_id,
                "new",
                Some("resend:header:authorization".to_owned()),
            )
            .expect("overwrite MCP secret");

        let entry = store
            .list(SecretFilter::Kind(SecretKind::McpSecret))
            .expect("list metadata")
            .pop()
            .expect("metadata entry");
        assert_eq!(entry.created_at_unix, Some(123));
        assert!(
            entry.updated_at_unix.unwrap_or_default() >= 456,
            "updated_at should be refreshed"
        );
        assert_eq!(
            store.get_string(&id).expect("read overwritten secret"),
            Some("new".to_owned())
        );
    }

    #[test]
    fn mcp_bulk_delete_reports_deleted_missing_and_failed_refs() {
        let store = Arc::new(MemorySecretStore::new());
        let secrets = GatewaySecrets::new(store);

        secrets
            .put_mcp_secret("ref_existing", "secret", None)
            .expect("put existing");
        let report = secrets.delete_mcp_secrets(["ref_existing", "ref_existing", "ref_missing"]);

        assert_eq!(report.attempted, 2);
        assert_eq!(report.deleted, 1);
        assert_eq!(report.missing, 1);
        assert!(report.failed.is_empty());

        let failing = GatewaySecrets::new(Arc::new(FailingSecretStore));
        let report = failing.delete_mcp_secrets(["ref_failed"]);
        assert_eq!(report.attempted, 1);
        assert_eq!(report.deleted, 0);
        assert_eq!(report.missing, 0);
        assert_eq!(report.failed.len(), 1);
        assert_eq!(report.failed[0].ref_id, "ref_failed");
    }

    #[test]
    fn mcp_list_returns_refs_without_values() {
        let store = Arc::new(MemorySecretStore::new());
        let secrets = GatewaySecrets::new(store);

        secrets
            .put_mcp_secret("ref_alpha", "secret-alpha", Some("alpha".to_owned()))
            .expect("put alpha");
        secrets
            .put_mcp_secret("ref_beta", "secret-beta", Some("beta".to_owned()))
            .expect("put beta");
        secrets
            .set_provider_api_key("openrouter", "sk-provider")
            .expect("put provider");

        let refs = secrets.list_mcp_secret_refs().expect("list MCP refs");
        assert_eq!(refs, vec!["ref_alpha".to_owned(), "ref_beta".to_owned()]);
        assert!(!format!("{refs:?}").contains("secret-alpha"));
        assert!(!format!("{refs:?}").contains("secret-beta"));
        assert!(!format!("{refs:?}").contains("sk-provider"));
    }

    struct FailingSecretStore;

    impl SecretStore for FailingSecretStore {
        fn get_string(&self, _id: &SecretId) -> KeystoreResult<Option<String>> {
            Err(KeystoreError::ReadFailed("should not read".to_owned()))
        }

        fn put_string(
            &self,
            _id: &SecretId,
            _value: &str,
            _meta: SecretMeta,
        ) -> KeystoreResult<()> {
            Err(KeystoreError::WriteFailed("should not write".to_owned()))
        }

        fn delete(&self, _id: &SecretId) -> KeystoreResult<bool> {
            Err(KeystoreError::DeleteFailed("should not delete".to_owned()))
        }

        fn exists(&self, _id: &SecretId) -> KeystoreResult<bool> {
            Err(KeystoreError::ReadFailed(
                "should not check exists".to_owned(),
            ))
        }

        fn list(&self, _filter: SecretFilter) -> KeystoreResult<Vec<SecretEntryMeta>> {
            Err(KeystoreError::ListFailed("should not list".to_owned()))
        }
    }
}
