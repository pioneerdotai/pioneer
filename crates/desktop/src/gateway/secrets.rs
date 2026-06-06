use std::{
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use pioneer_client::gateway::secrets::{GatewayAuthTokenRef, normalize_gateway_auth_token};
use pioneer_keystore::{
    DbKeyStore, DbKeyStoreConfig, SecretEntryMeta, SecretFilter, SecretId, SecretKind, SecretMeta,
    SecretStore,
};

#[derive(Clone)]
pub(crate) struct DesktopSecrets {
    store: Arc<dyn SecretStore>,
}

impl DesktopSecrets {
    pub(crate) fn open(runtime_home: &Path) -> Result<Self> {
        let store = DbKeyStore::open(DbKeyStoreConfig::for_runtime_home(runtime_home))
            .context("failed to open desktop keystore")?;
        Ok(Self::new(Arc::new(store)))
    }

    pub(crate) fn new(store: Arc<dyn SecretStore>) -> Self {
        Self { store }
    }

    pub(crate) fn get_gateway_auth_token(&self, token_ref: &str) -> Result<Option<String>> {
        let id = desktop_gateway_auth_secret_id(token_ref)?;
        self.store
            .get_string(&id)
            .context("failed to read desktop gateway auth token from keystore")
    }

    pub(crate) fn put_gateway_auth_token(
        &self,
        token_ref: &str,
        token: &str,
        label: Option<String>,
    ) -> Result<()> {
        let Some(token) = normalize_gateway_auth_token(token) else {
            bail!("desktop gateway auth token must not be empty");
        };

        let id = desktop_gateway_auth_secret_id(token_ref)?;
        let now = current_unix_i64()?;
        let created_at = self
            .existing_gateway_auth_token_meta(&id)?
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
                token.as_str(),
                SecretMeta {
                    kind: SecretKind::DesktopGatewayAuthToken,
                    label: Some(label),
                    created_at_unix: created_at,
                    updated_at_unix: now.max(created_at),
                },
            )
            .context("failed to write desktop gateway auth token to keystore")
    }

    pub(crate) fn delete_gateway_auth_token(&self, token_ref: &str) -> Result<bool> {
        let id = desktop_gateway_auth_secret_id(token_ref)?;
        self.store
            .delete(&id)
            .context("failed to delete desktop gateway auth token from keystore")
    }

    fn existing_gateway_auth_token_meta(&self, id: &SecretId) -> Result<Option<SecretEntryMeta>> {
        let entries = self
            .store
            .list(SecretFilter::Kind(SecretKind::DesktopGatewayAuthToken))
            .context("failed to list desktop gateway auth tokens from keystore")?;
        Ok(entries.into_iter().find(|entry| entry.id == *id))
    }
}

fn desktop_gateway_auth_secret_id(token_ref: &str) -> Result<SecretId> {
    let token_ref =
        GatewayAuthTokenRef::new(token_ref).context("invalid gateway auth token ref")?;
    SecretId::desktop_gateway_auth_token(token_ref.as_str())
        .context("invalid desktop gateway auth token ref")
}

fn current_unix_i64() -> Result<i64> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_secs();
    i64::try_from(timestamp).context("current unix timestamp does not fit in i64")
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::Arc,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use pioneer_keystore::{MemorySecretStore, SecretId, SecretStore};

    use super::*;

    fn desktop_secrets() -> (DesktopSecrets, Arc<MemorySecretStore>) {
        let store = Arc::new(MemorySecretStore::new());
        let secrets = DesktopSecrets::new(store.clone());
        (secrets, store)
    }

    #[test]
    fn gateway_auth_token_round_trips_through_desktop_secret_id() {
        let (secrets, store) = desktop_secrets();
        let id = SecretId::desktop_gateway_auth_token("remote-123").expect("desktop id");

        secrets
            .put_gateway_auth_token(
                "remote-123",
                "  bearer-token  ",
                Some("Remote Gateway".to_owned()),
            )
            .expect("put token");

        assert_eq!(
            secrets
                .get_gateway_auth_token("remote-123")
                .expect("read token"),
            Some("bearer-token".to_owned())
        );
        assert_eq!(
            store.get_string(&id).expect("read raw test store"),
            Some("bearer-token".to_owned())
        );
    }

    #[test]
    fn blank_gateway_auth_token_is_rejected() {
        let (secrets, _) = desktop_secrets();

        let error = secrets
            .put_gateway_auth_token("remote-123", "   ", None)
            .expect_err("blank token should be rejected");

        assert!(
            format!("{error:#}").contains("must not be empty"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn gateway_auth_token_delete_reports_existing_and_missing() {
        let (secrets, _) = desktop_secrets();

        secrets
            .put_gateway_auth_token("remote-123", "token", None)
            .expect("put token");

        assert!(
            secrets
                .delete_gateway_auth_token("remote-123")
                .expect("delete existing")
        );
        assert!(
            !secrets
                .delete_gateway_auth_token("remote-123")
                .expect("delete missing")
        );
    }

    #[test]
    fn gateway_auth_token_overwrite_preserves_created_at() {
        let (secrets, store) = desktop_secrets();
        let id = SecretId::desktop_gateway_auth_token("remote-123").expect("desktop id");

        secrets
            .put_gateway_auth_token("remote-123", "first", Some("first".to_owned()))
            .expect("put first");
        let first_meta = store
            .list(SecretFilter::Kind(SecretKind::DesktopGatewayAuthToken))
            .expect("list first")
            .into_iter()
            .find(|entry| entry.id == id)
            .expect("first metadata");

        secrets
            .put_gateway_auth_token(
                "remote-123",
                "second-secret-value",
                Some("updated label".to_owned()),
            )
            .expect("put second");
        let second_meta = store
            .list(SecretFilter::Kind(SecretKind::DesktopGatewayAuthToken))
            .expect("list second")
            .into_iter()
            .find(|entry| entry.id == id)
            .expect("second metadata");

        assert_eq!(second_meta.created_at_unix, first_meta.created_at_unix);
        assert_eq!(second_meta.label.as_deref(), Some("updated label"));
        assert_eq!(
            secrets
                .get_gateway_auth_token("remote-123")
                .expect("read second"),
            Some("second-secret-value".to_owned())
        );
        assert!(!format!("{second_meta:?}").contains("second-secret-value"));
    }

    #[test]
    fn open_uses_runtime_home_keystore_db() {
        let temp_dir = unique_temp_dir();

        let secrets = DesktopSecrets::open(&temp_dir).expect("open desktop secrets");
        secrets
            .put_gateway_auth_token("remote-123", "token", None)
            .expect("put token");

        assert!(temp_dir.join("keystore.db").exists());
        assert_eq!(
            secrets
                .get_gateway_auth_token("remote-123")
                .expect("read token"),
            Some("token".to_owned())
        );

        let _ = fs::remove_dir_all(&temp_dir);
    }

    fn unique_temp_dir() -> PathBuf {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        std::env::temp_dir().join(format!("pioneer-desktop-secrets-tests-{nanos}-{id}"))
    }
}
