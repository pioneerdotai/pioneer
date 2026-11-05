//! Conservative rotating audit for unrecognized legacy occurrence producers.
//! This path deliberately retains historical coverage; verified typed mode
//! returns after one metadata observation and does no historical audit.
use crate::CrudStore;
use crate::repositories::{
    task_actor_contract as contracts, task_occurrence_producers as producers,
    task_run_occurrence_reconcile as parents,
};
use anyhow::Result;
use pioneer_entity::{
    task_occurrence_reconcile_pending as contract_pending, task_run,
    task_run_occurrence_reconcile_pending as parent_pending,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    Statement, TransactionTrait,
};
use sha2::{Digest, Sha256};

// Canonical metadata from the normal Migrator's released parent tracker
// (20261002_000001) and contract tracker (20261008_000003). Hashes attest exact whitespace-normalized SQL, not just names
// or counts. Verified using the isolated initial codegen schema, read-only.
async fn known_legacy_owner(db: &pioneer_sqlite::SqliteDatabase, contract: bool) -> Result<bool> {
    if db.get_database_backend() != DbBackend::Sqlite {
        return Ok(false);
    }
    let (pattern, count, expected) = if contract {
        (
            "task_occurrence_reconcile_*",
            15_usize,
            "6e0ebab9c6d5293764a0eb5b273b99976c23b5ea65c45a6fbc024bc704991b45",
        )
    } else {
        (
            "task_run_occurrence_reconcile_*",
            6_usize,
            "3260c19c4b3bedab8d9338e2d317dbe9788d197953abf3bb274025daf6a3272a",
        )
    };
    // Catalog metadata has no Entity. Fetch a bounded group, then release the
    // reader before normalizing/hashing. This never runs inside a transaction.
    let rows = db.query_all_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name GLOB ? ORDER BY name LIMIT 16", [pattern.into()])).await?;
    if rows.len() != count {
        return Ok(false);
    }
    let mut definitions = Vec::with_capacity(count);
    for row in rows {
        let sql = row.try_get::<String>("", "sql")?;
        if sql.len() > 64 * 1024 {
            return Ok(false);
        }
        definitions.push(
            sql.trim()
                .trim_end_matches(';')
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" "),
        );
    }
    Ok(hex::encode(Sha256::digest(definitions.join("\n").as_bytes())) == expected)
}

#[derive(Default)]
pub(crate) struct AuditCursor {
    after_id: Option<String>,
    upper_id: Option<String>,
}

impl AuditCursor {
    pub(crate) fn has_more(&self) -> bool {
        self.upper_id.is_some()
    }
}

impl CrudStore {
    pub(crate) async fn audit_legacy_occurrence_producers(&self, contract: bool) -> Result<()> {
        // Unknown metadata never verifies producer readiness. Continue this
        // conservative audit and the existing queue owner independently.
        let mut first_error = None;
        match producers::typed(&self.connection).await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => first_error = Some(error),
        }
        match known_legacy_owner(&self.connection, contract).await {
            Ok(true) => return first_error.map_or(Ok(()), Err),
            Ok(false) => {}
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        let index = usize::from(contract);
        let mut cursor = self.occurrence_compatibility_cursors[index].lock().await;
        // Keep a fixed upper bound for this owner's finite pass. The shared
        // mutex serializes scoped clones; cancellation releases it, and skipped
        // locators are revisited after the finite pass wraps.
        if cursor.upper_id.is_none() {
            cursor.upper_id = task_run::Entity::find()
                .select_only()
                .column(task_run::Column::Id)
                .order_by_desc(task_run::Column::Id)
                .limit(1)
                .into_tuple::<String>()
                .one(&self.connection)
                .await?;
        }
        let Some(upper) = cursor.upper_id.as_deref() else {
            return first_error.map_or(Ok(()), Err);
        };
        let mut query = task_run::Entity::find()
            .select_only()
            .column(task_run::Column::Id)
            .filter(task_run::Column::Id.lte(upper));
        if let Some(after) = cursor.after_id.as_deref() {
            query = query.filter(task_run::Column::Id.gt(after));
        }
        let ids = query
            .order_by_asc(task_run::Column::Id)
            .limit(32)
            .into_tuple::<String>()
            .all(&self.connection)
            .await?;
        if ids.len() < 32 || ids.last() == cursor.upper_id.as_ref() {
            *cursor = AuditCursor::default();
        } else {
            cursor.after_id = ids.last().cloned();
        }
        for id in ids {
            let inspected: Result<bool> = async {
                if contract {
                    if contract_pending::Entity::find_by_id(&id)
                        .one(&self.connection)
                        .await?
                        .is_some()
                    {
                        return Ok(false);
                    }
                    Ok(
                        contracts::load_terminal_occurrence_metadata(&self.connection, &id)
                            .await?
                            .is_mismatch(),
                    )
                } else {
                    if parent_pending::Entity::find_by_id(&id)
                        .one(&self.connection)
                        .await?
                        .is_some()
                    {
                        return Ok(false);
                    }
                    parents::is_mismatch(&self.connection, &id).await
                }
            }
            .await;
            match inspected {
                Ok(false) => continue,
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    continue;
                }
                Ok(true) => {}
            }
            let result = self
                .run_serialized_write(|| async {
                    let tx = self.connection.begin().await?;
                    if contract {
                        if contract_pending::Entity::find_by_id(&id)
                            .one(&tx)
                            .await?
                            .is_none()
                            && contracts::load_terminal_occurrence_metadata(&tx, &id)
                                .await?
                                .is_mismatch()
                        {
                            producers::contract_ticket(&tx, &id).await?;
                        }
                    } else if parent_pending::Entity::find_by_id(&id)
                        .one(&tx)
                        .await?
                        .is_none()
                        && parents::is_mismatch(&tx, &id).await?
                    {
                        producers::parent_ticket(&tx, &id).await?;
                    }
                    tx.commit().await?;
                    Ok(())
                })
                .await;
            if let Err(error) = result {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        if first_error.is_some() {
            // Unknown rows remain on the rotating audit. A failed locator must
            // not suppress processing independent tickets already in storage.
            tracing::warn!(
                failure_class = "task_occurrence_compatibility_audit_failed",
                "Occurrence compatibility audit retained failed locators for a later pass"
            );
        }
        first_error.map_or(Ok(()), Err)
    }
}
