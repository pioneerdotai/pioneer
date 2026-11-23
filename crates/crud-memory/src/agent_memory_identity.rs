use crate::convention::memory_scope_kind_to_db;
use crate::identity::{self, IdentityKey};
use crate::memory::MemoryScopeResolution;
use anyhow::Result;
use sea_orm::{ConnectionTrait, DatabaseTransaction};

pub use crate::identity::transfer;

fn key<'a>(scope: &'a MemoryScopeResolution, namespace: &'a str, name: &'a str) -> IdentityKey<'a> {
    IdentityKey {
        scope_kind: memory_scope_kind_to_db(scope.scope.kind),
        scope_hash: &scope.scope_key_hash,
        namespace,
        key: name,
    }
}

pub async fn lookup<C: ConnectionTrait>(
    db: &C,
    scope: &MemoryScopeResolution,
    namespace: &str,
    canonical_key: &str,
) -> Result<Option<String>> {
    identity::lookup(db, &key(scope, namespace, canonical_key)).await
}

pub async fn bind(
    tx: &DatabaseTransaction,
    scope: &MemoryScopeResolution,
    namespace: &str,
    canonical_key: &str,
    memory_id: &str,
) -> Result<()> {
    identity::bind(tx, &key(scope, namespace, canonical_key), memory_id).await
}
