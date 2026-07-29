use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use crate::{
    Result, SecretEntryMeta, SecretFilter, SecretId, SecretMeta, SecretStore, id::filter_matches,
};

#[derive(Debug, Default, Clone)]
pub struct MemorySecretStore {
    entries: Arc<RwLock<BTreeMap<SecretId, (String, SecretMeta)>>>,
}

impl MemorySecretStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SecretStore for MemorySecretStore {
    fn get_string(&self, id: &SecretId) -> Result<Option<String>> {
        let entries = self.entries.read().expect("memory keystore read lock");
        Ok(entries.get(id).map(|(value, _)| value.clone()))
    }

    fn put_string(&self, id: &SecretId, value: &str, meta: SecretMeta) -> Result<()> {
        let mut entries = self.entries.write().expect("memory keystore write lock");
        entries.insert(id.clone(), (value.to_owned(), meta));
        Ok(())
    }

    fn delete(&self, id: &SecretId) -> Result<bool> {
        let mut entries = self.entries.write().expect("memory keystore write lock");
        Ok(entries.remove(id).is_some())
    }

    fn exists(&self, id: &SecretId) -> Result<bool> {
        let entries = self.entries.read().expect("memory keystore read lock");
        Ok(entries.contains_key(id))
    }

    fn list(&self, filter: SecretFilter) -> Result<Vec<SecretEntryMeta>> {
        let entries = self.entries.read().expect("memory keystore read lock");
        Ok(entries
            .iter()
            .filter(|(id, (_, meta))| filter_matches(id, Some(meta.kind), &filter))
            .map(|(id, (_, meta))| SecretEntryMeta::from_meta(id.clone(), meta.clone()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use crate::{SecretKind, SecretStore};

    use super::*;

    fn meta(kind: SecretKind, label: &str, now: i64) -> SecretMeta {
        SecretMeta::new(kind, Some(label.to_owned()), now)
    }

    #[test]
    fn put_get_delete() {
        let store = MemorySecretStore::new();
        let id = SecretId::provider_api_key("openrouter").expect("id");

        assert_eq!(store.get_string(&id).expect("missing read"), None);
        assert!(!store.exists(&id).expect("missing exists"));

        store
            .put_string(
                &id,
                "sk-openrouter",
                meta(SecretKind::ProviderApiKey, "openrouter", 1),
            )
            .expect("put");

        assert_eq!(
            store.get_string(&id).expect("read"),
            Some("sk-openrouter".to_owned())
        );
        assert!(store.exists(&id).expect("exists"));
        assert!(store.delete(&id).expect("delete existing"));
        assert!(!store.delete(&id).expect("delete missing"));
        assert_eq!(store.get_string(&id).expect("read deleted"), None);
    }

    #[test]
    fn overwrite_same_id() {
        let store = MemorySecretStore::new();
        let id = SecretId::provider_api_key("openrouter").expect("id");

        store
            .put_string(&id, "first", meta(SecretKind::ProviderApiKey, "first", 1))
            .expect("put first");
        store
            .put_string(&id, "second", meta(SecretKind::ProviderApiKey, "second", 2))
            .expect("put second");

        assert_eq!(
            store.get_string(&id).expect("read"),
            Some("second".to_owned())
        );
        let entries = store.list(SecretFilter::All).expect("list");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].label.as_deref(), Some("second"));
        assert_eq!(entries[0].updated_at_unix, Some(2));
    }

    #[test]
    fn list_all_and_by_kind_without_values() {
        let store = MemorySecretStore::new();
        let provider_id = SecretId::provider_api_key("openrouter").expect("provider id");
        let jwt_id = SecretId::gateway_access_jwt_signing_key();

        store
            .put_string(
                &provider_id,
                "secret-provider-value",
                meta(SecretKind::ProviderApiKey, "openrouter", 1),
            )
            .expect("put provider");
        store
            .put_string(
                &jwt_id,
                "secret-jwt-value",
                meta(
                    SecretKind::GatewayAccessJwtSigningKey,
                    "Gateway access JWT",
                    2,
                ),
            )
            .expect("put jwt");

        let all = store.list(SecretFilter::All).expect("list all");
        assert_eq!(all.len(), 2);
        assert!(!format!("{all:?}").contains("secret-provider-value"));
        assert!(!format!("{all:?}").contains("secret-jwt-value"));

        let provider = store
            .list(SecretFilter::Kind(SecretKind::ProviderApiKey))
            .expect("list provider");
        assert_eq!(provider.len(), 1);
        assert_eq!(provider[0].id, provider_id);
        assert_eq!(provider[0].kind, Some(SecretKind::ProviderApiKey));
    }

    #[test]
    fn list_by_service() {
        let store = MemorySecretStore::new();
        let id = SecretId::desktop_gateway_session("endpoint").expect("desktop id");

        store
            .put_string(
                &id,
                "desktop-session",
                meta(SecretKind::DesktopGatewaySession, "endpoint", 1),
            )
            .expect("put");

        let entries = store
            .list(SecretFilter::Service(id.service().to_owned()))
            .expect("list service");

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, id);
    }
}
