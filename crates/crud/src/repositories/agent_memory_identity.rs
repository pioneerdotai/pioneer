//! Explicit semantic identity links. All mutations run in the caller's
//! publication transaction; no payload I/O is performed here.
use crate::convention::memory_scope_kind_to_db;
use crate::memory::MemoryScopeResolution;
use anyhow::{Result, bail};
use sea_orm::{ConnectionTrait, DbBackend, Statement};

pub async fn lookup<C: ConnectionTrait>(
    db: &C,
    scope: &MemoryScopeResolution,
    namespace: &str,
    key: &str,
) -> Result<Option<String>> {
    let row = db.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "SELECT i.memory_id FROM agent_memory_identity i JOIN agent_memory m ON m.id=i.memory_id
         WHERE i.scope_kind=? AND i.scope_key_hash=? AND i.namespace=? AND i.canonical_key=? AND m.status='active'",
        [memory_scope_kind_to_db(scope.scope.kind).into(), scope.scope_key_hash.clone().into(), namespace.into(), key.into()])).await?;
    Ok(row.map(|row| row.try_get("", "memory_id")).transpose()?)
}

pub async fn bind<C: ConnectionTrait>(
    db: &C,
    scope: &MemoryScopeResolution,
    namespace: &str,
    key: &str,
    memory_id: &str,
) -> Result<()> {
    if let Some(existing) = lookup(db, scope, namespace, key).await? {
        if existing != memory_id {
            bail!("canonical memory identity already belongs to another active record");
        }
        return Ok(());
    }
    // Retired links are replaced only for this exact identity. No legacy key
    // or unrelated record is guessed, renamed or merged.
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "DELETE FROM agent_memory_identity WHERE scope_kind=? AND scope_key_hash=? AND namespace=? AND canonical_key=?",
        [memory_scope_kind_to_db(scope.scope.kind).into(), scope.scope_key_hash.clone().into(), namespace.into(), key.into()])).await?;
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO agent_memory_identity (scope_kind,scope_key_hash,namespace,canonical_key,memory_id) VALUES (?,?,?,?,?)",
        [memory_scope_kind_to_db(scope.scope.kind).into(), scope.scope_key_hash.clone().into(), namespace.into(), key.into(), memory_id.into()])).await?;
    Ok(())
}

pub async fn transfer<C: ConnectionTrait>(db: &C, old_id: &str, new_id: &str) -> Result<()> {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE agent_memory_identity SET memory_id=? WHERE memory_id=?",
        [new_id.into(), old_id.into()],
    ))
    .await?;
    Ok(())
}
