mod db;
mod error;
mod id;
mod memory;
mod permissions;

pub use crate::db::{DbKeyStore, DbKeyStoreConfig};
pub use crate::error::{KeystoreError, Result};
pub use crate::id::{SecretEntryMeta, SecretFilter, SecretId, SecretKind, SecretMeta};
pub use crate::memory::MemorySecretStore;
pub use crate::permissions::{
    ensure_keystore_sqlite_files, ensure_private_file, ensure_private_runtime_dir,
};

pub trait SecretStore: Send + Sync {
    fn get_string(&self, id: &SecretId) -> Result<Option<String>>;
    fn put_string(&self, id: &SecretId, value: &str, meta: SecretMeta) -> Result<()>;
    fn delete(&self, id: &SecretId) -> Result<bool>;
    fn exists(&self, id: &SecretId) -> Result<bool>;
    fn list(&self, filter: SecretFilter) -> Result<Vec<SecretEntryMeta>>;
}
