use std::{collections::HashMap, path::PathBuf, sync::Arc};

use db_keystore::{DbKeyStore as RawDbKeyStore, DbKeyStoreConfig as RawDbKeyStoreConfig};
use keyring_core::api::CredentialStoreApi;
use serde::{Deserialize, Serialize};

use crate::{
    KeystoreError, Result, SecretEntryMeta, SecretFilter, SecretId, SecretKind, SecretMeta,
    SecretStore, ensure_keystore_sqlite_files, ensure_private_runtime_dir, id::filter_matches,
};

const METADATA_SCHEMA: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbKeyStoreConfig {
    pub path: PathBuf,
}

impl DbKeyStoreConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn for_runtime_home(runtime_home: impl Into<PathBuf>) -> Self {
        Self {
            path: runtime_home.into().join("keystore.db"),
        }
    }
}

#[derive(Clone)]
pub struct DbKeyStore {
    config: DbKeyStoreConfig,
}

impl DbKeyStore {
    pub fn open(config: DbKeyStoreConfig) -> Result<Self> {
        if let Some(parent) = config
            .path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            ensure_private_runtime_dir(parent)?;
        }

        let _db = open_raw_store(&config)?;

        ensure_keystore_sqlite_files(&config.path)?;

        Ok(Self { config })
    }

    fn raw_store(&self) -> Result<Arc<RawDbKeyStore>> {
        open_raw_store(&self.config)
    }

    fn read_entry(&self, db: &RawDbKeyStore, id: &SecretId) -> Result<keyring_core::Entry> {
        db.build(id.service(), id.user(), None)
            .map_err(|err| KeystoreError::ReadFailed(err.to_string()))
    }
}

fn open_raw_store(config: &DbKeyStoreConfig) -> Result<Arc<RawDbKeyStore>> {
    RawDbKeyStore::new(RawDbKeyStoreConfig {
        path: config.path.clone(),
        encryption_opts: None,
        allow_ambiguity: false,
        vfs: None,
        index_always: false,
    })
    .map_err(|err| KeystoreError::OpenFailed(err.to_string()))
}

impl SecretStore for DbKeyStore {
    fn get_string(&self, id: &SecretId) -> Result<Option<String>> {
        let db = self.raw_store()?;
        let entry = self.read_entry(&db, id)?;
        match entry.get_password() {
            Ok(value) => Ok(Some(value)),
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(KeystoreError::ReadFailed(err.to_string())),
        }
    }

    fn put_string(&self, id: &SecretId, value: &str, meta: SecretMeta) -> Result<()> {
        let db = self.raw_store()?;
        let comment = encode_meta(&meta)?;
        let entry = db
            .build(id.service(), id.user(), None)
            .map_err(|err| KeystoreError::WriteFailed(err.to_string()))?;

        entry
            .set_password(value)
            .map_err(|err| KeystoreError::WriteFailed(err.to_string()))?;

        let attrs = HashMap::from([("comment", comment.as_str())]);
        entry
            .update_attributes(&attrs)
            .map_err(|err| KeystoreError::WriteFailed(err.to_string()))
    }

    fn delete(&self, id: &SecretId) -> Result<bool> {
        let db = self.raw_store()?;
        let entry = db
            .build(id.service(), id.user(), None)
            .map_err(|err| KeystoreError::DeleteFailed(err.to_string()))?;

        match entry.delete_credential() {
            Ok(()) => Ok(true),
            Err(err) if is_not_found(&err) => Ok(false),
            Err(err) => Err(KeystoreError::DeleteFailed(err.to_string())),
        }
    }

    fn exists(&self, id: &SecretId) -> Result<bool> {
        let db = self.raw_store()?;
        let entry = self.read_entry(&db, id)?;
        match entry.get_attributes() {
            Ok(_) => Ok(true),
            Err(err) if is_not_found(&err) => Ok(false),
            Err(err) => Err(KeystoreError::ReadFailed(err.to_string())),
        }
    }

    fn list(&self, filter: SecretFilter) -> Result<Vec<SecretEntryMeta>> {
        let db = self.raw_store()?;
        let entries = db
            .search(&HashMap::new())
            .map_err(|err| KeystoreError::ListFailed(err.to_string()))?;

        let mut metas = Vec::new();
        for entry in entries {
            let Some((service, user)) = entry.get_specifiers() else {
                continue;
            };
            let id = SecretId::from_service_user(service, user)
                .map_err(|err| KeystoreError::ListFailed(err.to_string()))?;
            let attrs = entry
                .get_attributes()
                .map_err(|err| KeystoreError::ListFailed(err.to_string()))?;
            let meta = decode_meta(attrs.get("comment").map(String::as_str))?;
            let kind = meta
                .as_ref()
                .map(|meta| meta.kind)
                .or_else(|| SecretKind::from_service(id.service()));

            if !filter_matches(&id, kind, &filter) {
                continue;
            }

            metas.push(match meta {
                Some(meta) => SecretEntryMeta::from_meta(id, meta),
                None => SecretEntryMeta {
                    id,
                    kind,
                    label: None,
                    created_at_unix: None,
                    updated_at_unix: None,
                },
            });
        }

        metas.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(metas)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredSecretMeta {
    schema: u8,
    kind: SecretKind,
    label: Option<String>,
    created_at_unix: i64,
    updated_at_unix: i64,
}

#[derive(Debug, Deserialize)]
struct StoredSecretMetaWire {
    schema: u8,
    kind: String,
    label: Option<String>,
    created_at_unix: i64,
    updated_at_unix: i64,
}

impl From<SecretMeta> for StoredSecretMeta {
    fn from(value: SecretMeta) -> Self {
        Self {
            schema: METADATA_SCHEMA,
            kind: value.kind,
            label: value.label,
            created_at_unix: value.created_at_unix,
            updated_at_unix: value.updated_at_unix,
        }
    }
}

fn encode_meta(meta: &SecretMeta) -> Result<String> {
    serde_json::to_string(&StoredSecretMeta::from(meta.clone()))
        .map_err(|err| KeystoreError::MetadataDecodeFailed(err.to_string()))
}

fn decode_meta(comment: Option<&str>) -> Result<Option<SecretMeta>> {
    let Some(comment) = comment.filter(|comment| !comment.is_empty()) else {
        return Ok(None);
    };

    let meta = serde_json::from_str::<StoredSecretMetaWire>(comment)
        .map_err(|err| KeystoreError::MetadataDecodeFailed(err.to_string()))?;
    if meta.schema != METADATA_SCHEMA {
        return Err(KeystoreError::MetadataDecodeFailed(format!(
            "unsupported metadata schema {}",
            meta.schema
        )));
    }

    let Ok(kind) = serde_json::from_value::<SecretKind>(serde_json::Value::String(meta.kind))
    else {
        // Secret kinds can be retired while their credential entries still
        // exist in an older runtime home. Keep service-based enumeration
        // available so the owning upgrade path can delete those entries.
        return Ok(None);
    };

    Ok(Some(SecretMeta {
        kind,
        label: meta.label,
        created_at_unix: meta.created_at_unix,
        updated_at_unix: meta.updated_at_unix,
    }))
}

fn is_not_found(err: &keyring_core::Error) -> bool {
    matches!(err, keyring_core::Error::NoEntry)
}

#[cfg(test)]
mod tests {
    use crate::{SecretKind, SecretStore};

    use super::*;

    fn meta(kind: SecretKind, label: &str, created_at: i64, updated_at: i64) -> SecretMeta {
        SecretMeta {
            kind,
            label: Some(label.to_owned()),
            created_at_unix: created_at,
            updated_at_unix: updated_at,
        }
    }

    fn open_temp_store() -> (tempfile::TempDir, DbKeyStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store =
            DbKeyStore::open(DbKeyStoreConfig::for_runtime_home(dir.path())).expect("open store");
        (dir, store)
    }

    #[test]
    fn retired_secret_kind_metadata_is_treated_as_unavailable() {
        let decoded = decode_meta(Some(
            r#"{"schema":1,"kind":"desktop_gateway_auth_token","label":"Legacy Gateway","created_at_unix":1,"updated_at_unix":2}"#,
        ))
        .expect("retired metadata must remain enumerable");

        assert_eq!(decoded, None);
    }

    #[test]
    fn retired_secret_metadata_remains_listable_and_deletable_by_service() {
        let (_dir, store) = open_temp_store();
        let raw = store.raw_store().expect("open raw store");
        for (service, user, kind) in [
            (
                "pioneer.desktop.gateway_auth_token",
                "remote-123",
                "desktop_gateway_auth_token",
            ),
            (
                "pioneer.gateway.superuser_jwt_token",
                "superuser",
                "superuser_jwt_token",
            ),
        ] {
            let entry = raw.build(service, user, None).expect("build retired entry");
            entry
                .set_password("retired-secret")
                .expect("write retired entry");
            let comment = format!(
                r#"{{"schema":1,"kind":"{kind}","label":"Legacy Gateway","created_at_unix":1,"updated_at_unix":2}}"#
            );
            entry
                .update_attributes(&HashMap::from([("comment", comment.as_str())]))
                .expect("write retired metadata");

            let entries = store
                .list(SecretFilter::Service(service.to_owned()))
                .expect("list retired service");
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].id.service(), service);
            assert_eq!(entries[0].id.user(), user);
            assert_eq!(entries[0].kind, None);
            assert!(store.delete(&entries[0].id).expect("delete retired entry"));
            assert!(
                store
                    .list(SecretFilter::Service(service.to_owned()))
                    .expect("list deleted retired service")
                    .is_empty()
            );
        }
    }

    #[test]
    fn open_temp_keystore_db() {
        let (dir, _store) = open_temp_store();

        assert!(dir.path().join("keystore.db").exists());
    }

    #[test]
    fn live_handles_do_not_hold_the_keystore_file_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = DbKeyStoreConfig::for_runtime_home(dir.path());
        let first = DbKeyStore::open(config.clone()).expect("open first");
        let second = DbKeyStore::open(config).expect("open second");
        let id = SecretId::provider_api_key("openrouter").expect("id");

        first
            .put_string(
                &id,
                "shared-secret",
                meta(SecretKind::ProviderApiKey, "openrouter", 1, 1),
            )
            .expect("write through first handle");

        assert_eq!(
            second.get_string(&id).expect("read through second handle"),
            Some("shared-secret".to_owned())
        );
    }

    #[test]
    fn put_get_overwrite_and_exists() {
        let (_dir, store) = open_temp_store();
        let id = SecretId::provider_api_key("openrouter").expect("id");

        assert_eq!(store.get_string(&id).expect("read missing"), None);
        assert!(!store.exists(&id).expect("missing exists"));

        store
            .put_string(
                &id,
                "first-secret",
                meta(SecretKind::ProviderApiKey, "first", 1, 1),
            )
            .expect("put first");
        assert_eq!(
            store.get_string(&id).expect("read first"),
            Some("first-secret".to_owned())
        );
        assert!(store.exists(&id).expect("exists"));

        store
            .put_string(
                &id,
                "second-secret",
                meta(SecretKind::ProviderApiKey, "second", 1, 2),
            )
            .expect("put second");
        assert_eq!(
            store.get_string(&id).expect("read second"),
            Some("second-secret".to_owned())
        );

        let entries = store.list(SecretFilter::All).expect("list");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].label.as_deref(), Some("second"));
        assert_eq!(entries[0].updated_at_unix, Some(2));
    }

    #[test]
    fn delete_existing_and_missing() {
        let (_dir, store) = open_temp_store();
        let id = SecretId::gateway_access_jwt_signing_key();

        assert!(!store.delete(&id).expect("delete missing"));

        store
            .put_string(
                &id,
                "access-jwt-secret",
                meta(
                    SecretKind::GatewayAccessJwtSigningKey,
                    "Gateway access JWT",
                    1,
                    1,
                ),
            )
            .expect("put");

        assert!(store.delete(&id).expect("delete existing"));
        assert!(!store.delete(&id).expect("delete missing again"));
        assert_eq!(store.get_string(&id).expect("read deleted"), None);
    }

    #[test]
    fn list_round_trips_metadata_without_values() {
        let (_dir, store) = open_temp_store();
        let provider_id = SecretId::provider_api_key("openrouter").expect("provider id");
        let mcp_id = SecretId::mcp_secret("gateway_settings:mcp:server:token").expect("mcp id");

        store
            .put_string(
                &provider_id,
                "provider-secret-value",
                meta(SecretKind::ProviderApiKey, "openrouter", 10, 11),
            )
            .expect("put provider");
        store
            .put_string(
                &mcp_id,
                "mcp-secret-value",
                meta(SecretKind::McpSecret, "mcp token", 20, 21),
            )
            .expect("put mcp");

        let all = store.list(SecretFilter::All).expect("list all");
        assert_eq!(all.len(), 2);
        assert!(!format!("{all:?}").contains("provider-secret-value"));
        assert!(!format!("{all:?}").contains("mcp-secret-value"));

        let provider = store
            .list(SecretFilter::Kind(SecretKind::ProviderApiKey))
            .expect("list provider");
        assert_eq!(provider.len(), 1);
        assert_eq!(provider[0].id, provider_id);
        assert_eq!(provider[0].kind, Some(SecretKind::ProviderApiKey));
        assert_eq!(provider[0].label.as_deref(), Some("openrouter"));
        assert_eq!(provider[0].created_at_unix, Some(10));
        assert_eq!(provider[0].updated_at_unix, Some(11));

        let mcp = store
            .list(SecretFilter::Service(
                "pioneer.gateway.mcp_secret".to_owned(),
            ))
            .expect("list mcp service");
        assert_eq!(mcp.len(), 1);
        assert_eq!(mcp[0].id, mcp_id);
    }

    #[test]
    fn reopening_db_preserves_values() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = DbKeyStoreConfig::for_runtime_home(dir.path());
        let id = SecretId::desktop_gateway_session("endpoint").expect("id");

        {
            let store = DbKeyStore::open(config.clone()).expect("open first");
            store
                .put_string(
                    &id,
                    "desktop-session",
                    meta(SecretKind::DesktopGatewaySession, "endpoint", 30, 30),
                )
                .expect("put");
        }

        let reopened = DbKeyStore::open(config).expect("open second");
        assert_eq!(
            reopened.get_string(&id).expect("read reopened"),
            Some("desktop-session".to_owned())
        );
        let entries = reopened.list(SecretFilter::All).expect("list reopened");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].kind, Some(SecretKind::DesktopGatewaySession));
    }

    #[test]
    fn metadata_json_schema_round_trips() {
        let meta = meta(SecretKind::ProviderApiKey, "openrouter", 1, 2);
        let comment = encode_meta(&meta).expect("encode");

        assert!(comment.contains("\"schema\":1"));
        assert_eq!(decode_meta(Some(&comment)).expect("decode"), Some(meta));
    }
}
