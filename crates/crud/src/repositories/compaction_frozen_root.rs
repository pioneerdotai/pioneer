//! Carrier classification is CPU work performed before acquiring DB capacity.
//! Derived columns are published with the original literal carrier, never used
//! to rewrite it or to grant source authority.
use crate::{CrudStore, FrozenUseGuard};
use anyhow::{Result, ensure};
use pioneer_compaction::frozen::FrozenHistoryRef;
use sea_orm::ConnectionTrait;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FrozenRoot {
    None,
    Manifest(FrozenHistoryRef),
    Blocked,
}
impl FrozenRoot {
    pub(crate) fn history(json: &str) -> Self {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(json) else {
            return Self::Blocked;
        };
        if value.is_array() {
            return Self::None;
        }
        let Ok(descriptor) = serde_json::from_value::<FrozenHistoryRef>(value) else {
            return Self::Blocked;
        };
        if descriptor.format != 1
            || descriptor.manifest_id.is_empty()
            || i64::try_from(descriptor.messages).is_err()
            || descriptor.identity_sha256.len() != 64
            || !descriptor
                .identity_sha256
                .bytes()
                .all(|c| c.is_ascii_hexdigit())
        {
            return Self::Blocked;
        }
        Self::Manifest(descriptor)
    }
    pub(crate) fn id(&self) -> Option<String> {
        match self {
            Self::Manifest(h) => Some(h.manifest_id.clone()),
            _ => None,
        }
    }
    pub(crate) fn state(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Manifest(_) => "manifest",
            Self::Blocked => "blocked",
        }
    }
}

pub(crate) struct RootPublication {
    pub(crate) root: FrozenRoot,
    guard: Option<FrozenUseGuard>,
}
impl RootPublication {
    pub(crate) async fn prepare(
        store: &CrudStore,
        workspace: &str,
        owner: &str,
        json: &str,
    ) -> Result<Self> {
        let root = FrozenRoot::history(json);
        let guard = match &root {
            FrozenRoot::Manifest(descriptor) => Some(
                store
                    .compaction_acquire_frozen_use(workspace, descriptor, Some(owner))
                    .await?,
            ),
            _ => None,
        };
        Ok(Self { root, guard })
    }
    pub(crate) async fn validate_in<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace: &str,
        owner: &str,
    ) -> Result<()> {
        match (&self.root, &self.guard) {
            (FrozenRoot::Manifest(descriptor), Some(guard)) => {
                ensure!(
                    guard.workspace() == workspace
                        && guard.owner() == owner
                        && guard.descriptor() == *descriptor,
                    "frozen publication root scope mismatch"
                );
                guard.validate_in(db, true).await
            }
            (FrozenRoot::None | FrozenRoot::Blocked, None) => Ok(()),
            _ => anyhow::bail!("frozen publication root has no use"),
        }
    }
    pub(crate) async fn complete<T>(self, result: Result<T>) -> Result<T> {
        match self.guard {
            Some(guard) => guard.complete(result).await,
            None => result,
        }
    }
}

impl CrudStore {
    /// Pin the literal insert-if-absent winner after classifying it outside DB
    /// capacity. The writer revalidates the actual carrier and domain, even
    /// when the winner names the same manifest as the losing capture.
    pub async fn compaction_pin_task_snapshot(
        &self,
        snapshot: &crate::TaskRunConversationSnapshotRecord,
    ) -> Result<Option<FrozenUseGuard>> {
        let root = FrozenRoot::history(&snapshot.history_json);
        let FrozenRoot::Manifest(descriptor) = root else {
            ensure!(
                root == FrozenRoot::None,
                "accepted Task history carrier is malformed"
            );
            return Ok(None);
        };
        let use_id = uuid::Uuid::new_v4().to_string();
        use sea_orm::TransactionTrait;
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let actual = super::task_run_conversation_snapshot::find_by_run(&tx, &snapshot.run_id).await?
                .ok_or_else(|| anyhow::anyhow!("accepted Task snapshot is unavailable"))?;
            ensure!(actual == *snapshot, "accepted Task snapshot carrier changed");
            let domain = tx.query_one_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DbBackend::Sqlite,
                "SELECT r.id FROM task_run r JOIN task t ON t.id=r.task_id JOIN workspace w ON w.id=t.workspace_id \
                 JOIN thread c ON c.id=? AND c.workspace_id=t.workspace_id WHERE r.id=? AND r.task_id=? AND t.workspace_id=?",
                [snapshot.conversation_thread_id.clone().into(), snapshot.run_id.clone().into(), snapshot.task_id.clone().into(),
                 snapshot.workspace_id.clone().into()])).await?;
            ensure!(domain.is_some(), "accepted Task snapshot domain is unavailable");
            let guard = super::compaction_frozen_use::acquire_in(self, &tx, &snapshot.workspace_id, &descriptor,
                Some(&snapshot.conversation_thread_id), "read", true, &use_id).await?;
            tx.commit().await?;
            Ok(Some(guard))
        }).await
    }
    pub async fn compaction_pin_runtime_snapshot(
        &self,
        snapshot: &crate::TurnRuntimeSnapshotRecord,
    ) -> Result<Option<FrozenUseGuard>> {
        let root = FrozenRoot::history(&snapshot.history_json);
        let FrozenRoot::Manifest(descriptor) = root else {
            ensure!(
                root == FrozenRoot::None,
                "accepted runtime history carrier is malformed"
            );
            return Ok(None);
        };
        let use_id = uuid::Uuid::new_v4().to_string();
        use sea_orm::TransactionTrait;
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let actual = super::turn_runtime_snapshot::find_turn_runtime_snapshot(&tx, &snapshot.turn_id).await?
                .ok_or_else(|| anyhow::anyhow!("accepted runtime snapshot is unavailable"))?;
            ensure!(actual.turn_id == snapshot.turn_id && actual.thread_id == snapshot.thread_id
                && actual.workspace_id == snapshot.workspace_id && actual.history_json == snapshot.history_json
                && actual.hook_runtime_context_json == snapshot.hook_runtime_context_json,
                "accepted runtime snapshot carrier changed");
            let domain = tx.query_one_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DbBackend::Sqlite,
                "SELECT v.id FROM turn v JOIN thread t ON t.id=v.thread_id JOIN workspace w ON w.id=t.workspace_id \
                 WHERE v.id=? AND t.id=? AND t.workspace_id=?",
                [snapshot.turn_id.clone().into(), snapshot.thread_id.clone().into(), snapshot.workspace_id.clone().into()])).await?;
            ensure!(domain.is_some(), "accepted runtime snapshot domain is unavailable");
            let guard = super::compaction_frozen_use::acquire_in(self, &tx, &snapshot.workspace_id, &descriptor,
                Some(&snapshot.thread_id), "read", true, &use_id).await?;
            tx.commit().await?;
            Ok(Some(guard))
        }).await
    }
}

impl FrozenRoot {
    pub(crate) fn cli_thread(json: &str) -> (Self, Option<String>) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(json) else {
            return (Self::Blocked, None);
        };
        if !value.is_object() {
            return (Self::Blocked, None);
        }
        let Some(receipt) = value.get("pioneerContext") else {
            return (Self::None, None);
        };
        if receipt.get("version").and_then(serde_json::Value::as_u64) != Some(4) {
            return (Self::Blocked, None);
        }
        let Some(history) = receipt
            .get("contextHistoryJson")
            .and_then(serde_json::Value::as_str)
        else {
            return if receipt
                .get("contextHistoryJson")
                .is_none_or(serde_json::Value::is_null)
            {
                (Self::None, None)
            } else {
                (Self::Blocked, None)
            };
        };
        let owner = receipt
            .get("contextManifestOwnerThreadId")
            .and_then(serde_json::Value::as_str)
            .or_else(|| {
                receipt
                    .get("contextOwnerThreadId")
                    .and_then(serde_json::Value::as_str)
            });
        let root = Self::history(history);
        if matches!(root, Self::Manifest(_)) && owner.is_none_or(str::is_empty) {
            return (Self::Blocked, None);
        }
        (root, owner.map(str::to_owned))
    }
    pub(crate) fn cli_turn(json: &str) -> (Self, Option<String>) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(json) else {
            return (Self::Blocked, None);
        };
        if !value.is_object() {
            return (Self::Blocked, None);
        }
        let Some(basis) = value.get("pioneerContextBasis") else {
            return (Self::None, None);
        };
        let Some(history) = basis.get("historyJson").and_then(serde_json::Value::as_str) else {
            return (Self::Blocked, None);
        };
        let owner = basis
            .get("manifestOwnerThreadId")
            .and_then(serde_json::Value::as_str);
        if owner.is_none_or(str::is_empty)
            || basis
                .get("executionThreadId")
                .and_then(serde_json::Value::as_str)
                .is_none_or(str::is_empty)
        {
            return (Self::Blocked, None);
        }
        (Self::history(history), owner.map(str::to_owned))
    }
}
impl RootPublication {
    pub(crate) async fn prepare_classified(
        store: &CrudStore,
        workspace: &str,
        classified: (FrozenRoot, Option<String>),
    ) -> Result<Self> {
        let (root, owner) = classified;
        let guard = match &root {
            FrozenRoot::Manifest(descriptor) => Some(
                store
                    .compaction_acquire_frozen_use(workspace, descriptor, owner.as_deref())
                    .await?,
            ),
            _ => None,
        };
        Ok(Self { root, guard })
    }
    pub(crate) async fn validate_root_in<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace: &str,
    ) -> Result<()> {
        match &self.guard {
            Some(guard) => self.validate_in(db, workspace, guard.owner()).await,
            None => {
                ensure!(
                    !matches!(self.root, FrozenRoot::Manifest(_)),
                    "frozen root has no guard"
                );
                Ok(())
            }
        }
    }
    pub(crate) fn values(&self) -> (Option<String>, String) {
        (self.root.id(), self.root.state().into())
    }
}

impl CrudStore {
    pub(crate) async fn compaction_pin_checkpoint_projection(
        &self,
        checkpoint: &str,
        operation: &str,
        workspace: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<FrozenUseGuard> {
        let use_id = uuid::Uuid::new_v4().to_string();
        use sea_orm::TransactionTrait;
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let actual = tx.query_one_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DbBackend::Sqlite,
                "SELECT p.id FROM compaction_checkpoint p JOIN compaction_operation o ON o.id=p.operation_id AND o.owner=p.owner \
                 JOIN compaction_context c ON c.owner=p.owner JOIN thread t ON t.id=c.thread_id AND t.workspace_id=c.workspace_id \
                 JOIN workspace w ON w.id=c.workspace_id JOIN compaction_operation_projection x ON x.operation_id=o.id \
                 JOIN compaction_frozen_history h ON h.id=x.manifest_id AND h.workspace_id=c.workspace_id \
                 WHERE p.id=? AND o.id=? AND c.workspace_id=? AND x.storage_state='bound' AND h.id=? \
                 AND h.ready=1 AND h.availability='resident' AND h.identity_sha256=? AND h.message_count=? \
                 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count \
                 AND x.identity_sha256=h.identity_sha256 AND x.imports_sha256=h.imports_sha256 AND x.import_count=h.import_count",
                [checkpoint.into(), operation.into(), workspace.into(), descriptor.manifest_id.clone().into(),
                 descriptor.identity_sha256.clone().into(), i64::try_from(descriptor.messages)?.into()])).await?;
            ensure!(actual.is_some(), "checkpoint frozen projection is unavailable or changed");
            let guard = super::compaction_frozen_use::acquire_in(self, &tx, workspace, descriptor, None, "read", true, &use_id).await?;
            tx.commit().await?;
            Ok(guard)
        }).await
    }
}

impl CrudStore {
    pub async fn compaction_pin_cli_thread_binding(
        &self,
        binding: &crate::CliRuntimeThreadBindingRecord,
    ) -> Result<Option<FrozenUseGuard>> {
        let (root, owner) = FrozenRoot::cli_thread(&binding.resume_cursor_json);
        let FrozenRoot::Manifest(descriptor) = root else {
            return Ok(None);
        };
        let use_id = uuid::Uuid::new_v4().to_string();
        use sea_orm::TransactionTrait;
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let actual = tx.query_one_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DbBackend::Sqlite,
                "SELECT b.thread_id FROM thread_cli_runtime_binding b JOIN thread t ON t.id=b.thread_id AND t.workspace_id=b.workspace_id \
                 JOIN workspace w ON w.id=t.workspace_id WHERE b.thread_id=? AND b.workspace_id=? AND b.runtime_id=? \
                 AND b.native_thread_id=? AND b.resume_cursor_json=? AND b.status=?",
                [binding.thread_id.clone().into(), binding.workspace_id.clone().into(), binding.runtime_id.clone().into(),
                 binding.native_thread_id.clone().into(), binding.resume_cursor_json.clone().into(), binding.status.clone().into()])).await?;
            ensure!(actual.is_some(), "CLI context receipt carrier is unavailable or changed");
            let guard = super::compaction_frozen_use::acquire_in(self, &tx, &binding.workspace_id, &descriptor,
                owner.as_deref(), "read", true, &use_id).await?;
            tx.commit().await?;
            Ok(Some(guard))
        }).await
    }
    pub async fn compaction_pin_cli_turn_binding(
        &self,
        binding: &crate::CliRuntimeTurnBindingRecord,
    ) -> Result<Option<FrozenUseGuard>> {
        let (root, owner) = FrozenRoot::cli_turn(&binding.input_mapping_json);
        let FrozenRoot::Manifest(descriptor) = root else {
            return Ok(None);
        };
        let use_id = uuid::Uuid::new_v4().to_string();
        use sea_orm::TransactionTrait;
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let actual = tx.query_one_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DbBackend::Sqlite,
                "SELECT b.turn_id FROM turn_cli_runtime_binding b JOIN turn v ON v.id=b.turn_id AND v.thread_id=b.thread_id \
                 JOIN thread t ON t.id=b.thread_id AND t.workspace_id=b.workspace_id \
                 JOIN thread c ON c.id=b.continuation_thread_id AND c.workspace_id=b.workspace_id \
                 JOIN workspace w ON w.id=t.workspace_id WHERE b.turn_id=? AND b.thread_id=? AND b.workspace_id=? \
                 AND b.runtime_id=? AND b.native_thread_id=? AND b.input_mapping_json=? AND b.continuation_thread_id=?",
                [binding.turn_id.clone().into(), binding.thread_id.clone().into(), binding.workspace_id.clone().into(),
                 binding.runtime_id.clone().into(), binding.native_thread_id.clone().into(), binding.input_mapping_json.clone().into(),
                 binding.continuation_thread_id.clone().into()])).await?;
            ensure!(actual.is_some(), "CLI sent-basis carrier is unavailable or changed");
            let guard = super::compaction_frozen_use::acquire_in(self, &tx, &binding.workspace_id, &descriptor,
                owner.as_deref(), "read", true, &use_id).await?;
            tx.commit().await?;
            Ok(Some(guard))
        }).await
    }
}

impl CrudStore {
    pub(crate) async fn compaction_pin_delivery_output(
        &self,
        workspace: &str,
        snapshot: &crate::compaction::TaskDeliveryOutputSnapshot,
    ) -> Result<FrozenUseGuard> {
        let use_id = uuid::Uuid::new_v4().to_string();
        use sea_orm::TransactionTrait;
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let actual = super::compaction_task_output::compaction_delivery_output(
                &tx,
                workspace,
                &snapshot.delivery_id,
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("accepted output carrier is unavailable"))?;
            ensure!(actual == *snapshot, "accepted output carrier changed");
            let guard = super::compaction_frozen_use::acquire_in(
                self,
                &tx,
                workspace,
                &snapshot.output.history,
                Some(&snapshot.output.source_thread),
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
    pub(crate) async fn compaction_pin_task_basis(
        &self,
        workspace: &str,
        destination: &str,
        turn: &str,
        basis: &crate::compaction::AcceptedTaskBasis,
        descriptor: &FrozenHistoryRef,
    ) -> Result<FrozenUseGuard> {
        let use_id = uuid::Uuid::new_v4().to_string();
        use sea_orm::TransactionTrait;
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let actual = super::compaction_history::compaction_task_basis_snapshot(
                &tx,
                workspace,
                destination,
                turn,
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("accepted Task basis carrier is unavailable"))?;
            ensure!(
                actual.run_id == basis.run_id
                    && actual.parent_thread == basis.parent_thread
                    && actual.history_json == basis.history_json,
                "accepted Task basis carrier changed"
            );
            let guard = super::compaction_frozen_use::acquire_in(
                self,
                &tx,
                workspace,
                descriptor,
                Some(&basis.parent_thread),
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

/// Receipt rejection is authority, not a successful lifetime acquisition.
/// Only the read-time boolean predicate consumes this outcome as `false`.
pub(crate) enum OperationProjectionPin {
    Absent,
    Pinned(FrozenUseGuard),
    AuthorityRejected,
}

impl CrudStore {
    /// Strict consumers never turn an invalid receipt into an absent origin.
    pub(crate) async fn compaction_pin_operation_projection(
        &self,
        operation: &str,
    ) -> Result<Option<FrozenUseGuard>> {
        match self
            .compaction_pin_operation_projection_for_read(operation)
            .await?
        {
            OperationProjectionPin::Absent => Ok(None),
            OperationProjectionPin::Pinned(guard) => Ok(Some(guard)),
            OperationProjectionPin::AuthorityRejected => {
                anyhow::bail!("bound operation projection authority changed")
            }
        }
    }

    pub(crate) async fn compaction_pin_operation_projection_for_read(
        &self,
        operation: &str,
    ) -> Result<OperationProjectionPin> {
        use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};
        let use_id = uuid::Uuid::new_v4().to_string();
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            // Header/domain availability is independent of receipt equality.
            // This query never fetches a reference or an import body.
            let row = tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "SELECT p.manifest_id,p.identity_sha256,h.message_count,c.workspace_id, \
                 (h.workspace_id=c.workspace_id AND h.identity_sha256=p.identity_sha256 \
                  AND h.imports_sha256=p.imports_sha256 AND h.import_count=p.import_count) AS accepted \
                 FROM compaction_operation_projection p JOIN compaction_operation o ON o.id=p.operation_id \
                 JOIN compaction_context c ON c.owner=o.owner JOIN thread t ON t.id=c.thread_id AND t.workspace_id=c.workspace_id \
                 JOIN workspace w ON w.id=c.workspace_id JOIN compaction_frozen_history h ON h.id=p.manifest_id \
                 JOIN thread ht ON ht.id=h.owner_thread AND ht.workspace_id=h.workspace_id \
                 JOIN workspace hw ON hw.id=h.workspace_id \
                 WHERE p.operation_id=? AND p.storage_state='bound' AND h.availability='resident' \
                  AND h.ready=1 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count",
                 [operation.into()])).await?;
            let exists = tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "SELECT operation_id FROM compaction_operation_projection WHERE operation_id=?", [operation.into()])).await?;
            ensure!(exists.is_none() || row.is_some(), "bound operation projection or domain is unavailable");
            let outcome = match row {
                Some(row) if row.try_get::<i64>("", "accepted")? == 0 => OperationProjectionPin::AuthorityRejected,
                Some(row) => {
                    let descriptor = FrozenHistoryRef { format:1, manifest_id:row.try_get("", "manifest_id")?,
                        identity_sha256:row.try_get("", "identity_sha256")?, messages:u64::try_from(row.try_get::<i64>("", "message_count")?)? };
                    let workspace:String=row.try_get("", "workspace_id")?;
                    OperationProjectionPin::Pinned(super::compaction_frozen_use::acquire_in(self,&tx,&workspace,&descriptor,None,"read",true,&use_id).await?)
                }
                None => OperationProjectionPin::Absent,
            };
            tx.commit().await?;
            Ok(outcome)
        }).await
    }
}
