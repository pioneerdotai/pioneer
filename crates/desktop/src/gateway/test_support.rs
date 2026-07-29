use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use pioneer_keystore::{
    KeystoreError, MemorySecretStore, Result, SecretEntryMeta, SecretFilter, SecretId, SecretMeta,
    SecretStore,
};

#[derive(Debug, Default)]
pub(crate) struct FailingDesktopSecretStore {
    inner: MemorySecretStore,
    fail_read: AtomicBool,
    fail_write: AtomicBool,
    fail_delete: AtomicBool,
}

impl FailingDesktopSecretStore {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn fail_next_read(&self) {
        self.fail_read.store(true, Ordering::SeqCst);
    }

    pub(crate) fn fail_next_write(&self) {
        self.fail_write.store(true, Ordering::SeqCst);
    }

    pub(crate) fn fail_next_delete(&self) {
        self.fail_delete.store(true, Ordering::SeqCst);
    }

    fn take(flag: &AtomicBool) -> bool {
        flag.swap(false, Ordering::SeqCst)
    }
}

impl SecretStore for FailingDesktopSecretStore {
    fn get_string(&self, id: &SecretId) -> Result<Option<String>> {
        if Self::take(&self.fail_read) {
            return Err(KeystoreError::ReadFailed(
                "injected test failure".to_owned(),
            ));
        }
        self.inner.get_string(id)
    }

    fn put_string(&self, id: &SecretId, value: &str, meta: SecretMeta) -> Result<()> {
        if Self::take(&self.fail_write) {
            return Err(KeystoreError::WriteFailed(
                "injected test failure".to_owned(),
            ));
        }
        self.inner.put_string(id, value, meta)
    }

    fn delete(&self, id: &SecretId) -> Result<bool> {
        if Self::take(&self.fail_delete) {
            return Err(KeystoreError::DeleteFailed(
                "injected test failure".to_owned(),
            ));
        }
        self.inner.delete(id)
    }

    fn exists(&self, id: &SecretId) -> Result<bool> {
        self.inner.exists(id)
    }

    fn list(&self, filter: SecretFilter) -> Result<Vec<SecretEntryMeta>> {
        self.inner.list(filter)
    }
}

#[cfg(test)]
mod tests {
    use pioneer_keystore::{SecretKind, SecretMeta};

    use super::*;

    #[test]
    fn secret_store_supports_missing_success_and_one_shot_failures() {
        let store = FailingDesktopSecretStore::new();
        let id = SecretId::desktop_gateway_session("fixture").expect("secret id");
        let meta = SecretMeta::new(SecretKind::DesktopGatewaySession, None, 1);

        assert_eq!(store.get_string(&id).expect("missing read"), None);
        store.fail_next_write();
        assert!(store.put_string(&id, "secret", meta.clone()).is_err());
        store
            .put_string(&id, "secret", meta)
            .expect("successful write");
        store.fail_next_read();
        assert!(store.get_string(&id).is_err());
        assert_eq!(
            store.get_string(&id).expect("successful read").as_deref(),
            Some("secret")
        );
        store.fail_next_delete();
        assert!(store.delete(&id).is_err());
        assert!(store.delete(&id).expect("successful delete"));
    }
}
