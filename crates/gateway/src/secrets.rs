use std::{collections::BTreeSet, path::Path, sync::Arc};

use anyhow::{Context, Result, bail};
use pioneer_keystore::{
    DbKeyStore, DbKeyStoreConfig, SecretEntryMeta, SecretFilter, SecretId, SecretKind, SecretMeta,
    SecretStore,
};
use rand::RngCore;
use rand::rngs::OsRng;
use tracing::warn;

use crate::helpers::{decode_hex, encode_hex, unix_timestamp_secs};

#[derive(Clone)]
pub(crate) struct GatewaySecrets {
    store: Arc<dyn SecretStore>,
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

    pub(crate) fn delete_provider_api_key(&self, provider: &str) -> Result<(String, bool)> {
        let id = SecretId::provider_api_key(provider).context("invalid provider name")?;
        let normalized_provider = id.user().to_owned();
        let deleted = self
            .store
            .delete(&id)
            .context("failed to delete provider api key from keystore")?;
        Ok((normalized_provider, deleted))
    }

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

    pub(crate) fn resolve_provider_api_key(&self, provider_name: &str) -> String {
        if is_local_or_cli_provider(provider_name) {
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
        OsRng.fill_bytes(material.as_mut_slice());

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

    fn existing_provider_secret_meta(&self, id: &SecretId) -> Result<Option<SecretEntryMeta>> {
        let entries = self
            .store
            .list(SecretFilter::Kind(SecretKind::ProviderApiKey))
            .context("failed to read provider api key metadata from keystore")?;
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

fn is_local_or_cli_provider(provider_name: &str) -> bool {
    matches!(
        provider_name,
        "ollama"
            | "lmstudio"
            | "llamacpp"
            | "sglang"
            | "vllm"
            | "osaurus"
            | "litellm"
            | "claude-code"
            | "gemini-cli"
            | "kilocli"
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
