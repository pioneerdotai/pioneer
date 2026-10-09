//! Storage ownership only. A use neither grants source authority nor expires.
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_compaction::frozen::FrozenHistoryRef;
use pioneer_entity::compaction_frozen_history;
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct Ownership {
    store: CrudStore,
    use_id: String,
    header: compaction_frozen_history::Model,
    owners: AtomicUsize,
    abandoned: AtomicBool,
    dependencies: Mutex<Vec<FrozenUseGuard>>,
}

/// Clones own one local share each. Cancellation leaves the durable use intact.
/// The last explicit close is the sole ordinary release owner, even when the
/// final two clones close concurrently. Drop never starts database work.
pub struct FrozenUseGuard {
    inner: Arc<Ownership>,
    owns: bool,
}
impl std::fmt::Debug for FrozenUseGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrozenUseGuard").finish_non_exhaustive()
    }
}
impl PartialEq for FrozenUseGuard {
    fn eq(&self, other: &Self) -> bool {
        self.inner.store.connection.runtime_identity()
            == other.inner.store.connection.runtime_identity()
            && self.inner.header.id == other.inner.header.id
            && self.inner.header.workspace_id == other.inner.header.workspace_id
            && self.inner.header.owner_thread == other.inner.header.owner_thread
            && self.inner.header.storage_generation == other.inner.header.storage_generation
            && self.inner.header.message_count == other.inner.header.message_count
            && self.inner.header.identity_sha256 == other.inner.header.identity_sha256
            && self.inner.header.import_count == other.inner.header.import_count
            && self.inner.header.imports_sha256 == other.inner.header.imports_sha256
    }
}
impl Eq for FrozenUseGuard {}
impl Clone for FrozenUseGuard {
    fn clone(&self) -> Self {
        assert!(self.owns);
        self.inner
            .owners
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .expect("frozen use ownership overflow");
        Self {
            inner: self.inner.clone(),
            owns: true,
        }
    }
}
impl Drop for FrozenUseGuard {
    fn drop(&mut self) {
        if self.owns {
            self.inner.abandoned.store(true, Ordering::Release);
            self.inner.owners.fetch_sub(1, Ordering::AcqRel);
        }
    }
}
impl FrozenUseGuard {
    pub fn descriptor(&self) -> FrozenHistoryRef {
        FrozenHistoryRef {
            format: 1,
            manifest_id: self.inner.header.id.clone(),
            messages: self.inner.header.message_count as u64,
            identity_sha256: self.inner.header.identity_sha256.clone(),
        }
    }
    pub fn workspace(&self) -> &str {
        &self.inner.header.workspace_id
    }
    pub fn owner(&self) -> &str {
        &self.inner.header.owner_thread
    }
    pub(crate) fn header(&self) -> &compaction_frozen_history::Model {
        &self.inner.header
    }
    pub(crate) fn token(&self) -> &str {
        &self.inner.use_id
    }
    pub(crate) fn retain_dependencies(
        &self,
        uses: impl IntoIterator<Item = FrozenUseGuard>,
    ) -> Result<()> {
        let uses: Vec<_> = uses.into_iter().collect();
        ensure!(
            uses.iter()
                .all(|guard| !Arc::ptr_eq(&self.inner, &guard.inner)),
            "frozen use cannot retain itself"
        );
        self.inner
            .dependencies
            .lock()
            .map_err(|_| anyhow::anyhow!("frozen dependencies poisoned"))?
            .extend(uses);
        Ok(())
    }
    pub(crate) fn validate_store(&self, store: &CrudStore) -> Result<()> {
        ensure!(
            self.inner.store.connection.runtime_identity() == store.connection.runtime_identity(),
            "frozen use belongs to another database"
        );
        Ok(())
    }
    pub async fn validate(&self) -> Result<()> {
        self.validate_in(&self.inner.store.connection, true).await
    }
    pub(crate) async fn validate_in<C: ConnectionTrait>(&self, db: &C, ready: bool) -> Result<()> {
        ensure!(self.owns, "frozen use is closed");
        let h = &self.inner.header;
        let row=db.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "SELECT h.id FROM compaction_frozen_history h JOIN compaction_frozen_use u ON u.manifest_id=h.id \
             JOIN thread t ON t.id=h.owner_thread AND t.workspace_id=h.workspace_id JOIN workspace w ON w.id=h.workspace_id \
             WHERE u.use_id=? AND h.id=? AND u.storage_generation=? AND h.storage_generation=? \
             AND u.workspace_id=? AND h.workspace_id=? AND h.owner_thread=? AND h.availability='resident' \
             AND h.identity_sha256=? AND h.message_count=? AND h.imports_sha256=? AND h.import_count=? \
             AND (?=0 OR (h.ready=1 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count))",
            [self.inner.use_id.clone().into(),h.id.clone().into(),h.storage_generation.into(),h.storage_generation.into(),
             h.workspace_id.clone().into(),h.workspace_id.clone().into(),h.owner_thread.clone().into(),h.identity_sha256.clone().into(),
             h.message_count.into(),h.imports_sha256.clone().into(),h.import_count.into(),i64::from(ready).into()])).await?;
        ensure!(row.is_some(), "frozen history use or domain is unavailable");
        Ok(())
    }
    pub async fn close(mut self) -> Result<()> {
        self.owns = false;
        if self.inner.owners.fetch_sub(1, Ordering::AcqRel) != 1 {
            return Ok(());
        }
        if self.inner.abandoned.load(Ordering::Acquire) {
            return Ok(());
        }
        let dependencies = std::mem::take(
            &mut *self
                .inner
                .dependencies
                .lock()
                .map_err(|_| anyhow::anyhow!("frozen dependencies poisoned"))?,
        );
        let mut dependency_result = Ok(());
        for guard in dependencies {
            dependency_result = Box::pin(guard.complete(dependency_result)).await;
        }
        // A failed or cancelled release leaves the row as conservative debt.
        let h = &self.inner.header;
        let result = self.inner.store.run_serialized_write(||async {
            let released = self.inner.store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "DELETE FROM compaction_frozen_use WHERE use_id=? AND manifest_id=? AND storage_generation=? AND workspace_id=? \
                 AND EXISTS(SELECT 1 FROM compaction_frozen_history h JOIN thread t ON t.id=h.owner_thread AND t.workspace_id=h.workspace_id \
                 JOIN workspace w ON w.id=h.workspace_id WHERE h.id=compaction_frozen_use.manifest_id \
                 AND h.storage_generation=compaction_frozen_use.storage_generation AND h.workspace_id=compaction_frozen_use.workspace_id)",
                [self.inner.use_id.clone().into(),h.id.clone().into(),h.storage_generation.into(),h.workspace_id.clone().into()])).await?;
            ensure!(released.rows_affected() == 1, "frozen use release domain is unavailable");
            Ok(())
        }).await;
        dependency_result.and(result)
    }
    /// Close even on a semantic error, retaining the original error chain.
    pub async fn complete<T>(self, result: Result<T>) -> Result<T> {
        match (result, self.close().await) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(release)) => {
                Err(error.context(format!("frozen use release failed: {release:#}")))
            }
        }
    }
}

pub(crate) async fn acquire_in<C: ConnectionTrait>(
    store: &CrudStore,
    db: &C,
    workspace: &str,
    descriptor: &FrozenHistoryRef,
    expected_owner: Option<&str>,
    purpose: &str,
    ready: bool,
    use_id: &str,
) -> Result<FrozenUseGuard> {
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    ensure!(descriptor.format == 1, "unsupported frozen history format");
    ensure!(
        matches!(purpose, "read" | "capture" | "layout" | "proof"),
        "invalid frozen use purpose"
    );
    let h = compaction_frozen_history::Entity::find_by_id(&descriptor.manifest_id)
        .filter(compaction_frozen_history::Column::WorkspaceId.eq(workspace))
        .filter(compaction_frozen_history::Column::Availability.eq("resident"))
        .filter(
            compaction_frozen_history::Column::MessageCount.eq(i64::try_from(descriptor.messages)?),
        )
        .filter(compaction_frozen_history::Column::IdentitySha256.eq(&descriptor.identity_sha256))
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("frozen history is unavailable"))?;
    ensure!(
        expected_owner.is_none_or(|owner| owner == h.owner_thread),
        "frozen owner changed"
    );
    ensure!(
        !ready
            || (h.ready == 1
                && h.next_ordinal == h.message_count
                && h.next_import == h.import_count),
        "frozen history is incomplete"
    );
    let inserted=db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_use(use_id,manifest_id,storage_generation,purpose,workspace_id) \
         SELECT ?,h.id,h.storage_generation,?,h.workspace_id FROM compaction_frozen_history h \
         JOIN thread t ON t.id=h.owner_thread AND t.workspace_id=h.workspace_id JOIN workspace w ON w.id=h.workspace_id \
         WHERE h.id=? AND h.storage_generation=? AND h.availability='resident'",
        [use_id.into(),purpose.into(),h.id.clone().into(),h.storage_generation.into()])).await?;
    ensure!(
        inserted.rows_affected() == 1,
        "frozen history domain is unavailable"
    );
    Ok(FrozenUseGuard {
        inner: Arc::new(Ownership {
            store: store.clone(),
            use_id: use_id.into(),
            header: h,
            owners: AtomicUsize::new(1),
            abandoned: AtomicBool::new(false),
            dependencies: Mutex::new(Vec::new()),
        }),
        owns: true,
    })
}

impl CrudStore {
    pub(crate) async fn compaction_acquire_frozen_builder_use(
        &self,
        workspace: &str,
        owner: &str,
        manifest: &str,
    ) -> Result<FrozenUseGuard> {
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
        let use_id = uuid::Uuid::new_v4().to_string();
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let h = compaction_frozen_history::Entity::find_by_id(manifest)
                .filter(compaction_frozen_history::Column::WorkspaceId.eq(workspace))
                .filter(compaction_frozen_history::Column::OwnerThread.eq(owner))
                .one(&tx)
                .await?
                .ok_or_else(|| anyhow::anyhow!("frozen history is unavailable"))?;
            let descriptor = FrozenHistoryRef {
                format: 1,
                manifest_id: h.id,
                messages: u64::try_from(h.message_count)?,
                identity_sha256: h.identity_sha256,
            };
            let guard = acquire_in(
                self,
                &tx,
                workspace,
                &descriptor,
                Some(owner),
                "capture",
                false,
                &use_id,
            )
            .await?;
            tx.commit().await?;
            Ok(guard)
        })
        .await
    }
    /// Callers retain this handle through all pages and dependent publications.
    pub async fn compaction_acquire_frozen_use(
        &self,
        workspace: &str,
        descriptor: &FrozenHistoryRef,
        expected_owner: Option<&str>,
    ) -> Result<FrozenUseGuard> {
        let use_id = uuid::Uuid::new_v4().to_string();
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let guard = acquire_in(
                self,
                &tx,
                workspace,
                descriptor,
                expected_owner,
                "read",
                true,
                &use_id,
            )
            .await?;
            tx.commit().await?;
            Ok(guard)
        })
        .await
    }
}
