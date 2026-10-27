use anyhow::{Context, Result};
use pioneer_entity::task_run_conversation_snapshot;
use pioneer_observability::portable_payload as observe;
use sea_orm::sea_query::{Expr, ExprTrait, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, FromQueryResult, QueryFilter,
    QuerySelect, QueryTrait, Set,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTaskRunConversationSnapshot {
    pub run_id: String,
    pub task_id: String,
    pub workspace_id: String,
    pub conversation_thread_id: String,
    pub source_turn_id: Option<String>,
    pub history_json: String,
    pub created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
}

#[derive(Debug, Clone)]
pub struct TaskRunConversationSnapshotRecord {
    /// Physical provenance is excluded from logical record equality.
    #[doc(hidden)]
    pub history_json_payload_id: Option<String>,
    #[doc(hidden)]
    pub portable_revision: i64,
    #[doc(hidden)]
    pub portable_identity: String,
    pub run_id: String,
    pub task_id: String,
    pub workspace_id: String,
    pub conversation_thread_id: String,
    pub source_turn_id: Option<String>,
    pub history_json: String,
    pub created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
}

/// A captured immutable winner without its potentially large history. Only
/// hydrate_record may turn this into the public logical record after release.
#[derive(Debug, FromQueryResult)]
pub struct StoredTaskRunConversationSnapshot {
    #[doc(hidden)]
    #[sea_orm(skip)]
    pub history_json_read_hold: Option<super::portable_payload::PayloadReadHold>,
    pub run_id: String,
    pub task_id: String,
    pub workspace_id: String,
    pub conversation_thread_id: String,
    pub source_turn_id: Option<String>,
    pub created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    // Preserve the SQLite spelling for the source fence (legacy defaults can
    // use a space rather than SQLx's RFC3339 binding representation).
    pub created_at_text: String,
    pub history_json_payload_id: Option<String>,
    pub portable_revision: i64,
    pub portable_identity: String,
    pub history_bytes: i64,
}

// Scalar correlated metadata is not an Entity column. Missing headers remain
// NULL and fail typed decoding; they must never turn into an empty history.
const HISTORY_BYTES_SQL: &str = "CASE WHEN history_json_payload_id IS NULL THEN octet_length(history_json) ELSE (SELECT p.original_bytes FROM portable_payload p WHERE p.id=task_run_conversation_snapshot.history_json_payload_id) END";

pub async fn insert_if_absent<C: ConnectionTrait>(
    db: &C,
    snapshot: NewTaskRunConversationSnapshot,
    root: &crate::frozen_lifetime::PreparedFrozenRoot,
) -> Result<StoredTaskRunConversationSnapshot> {
    // A losing insert does not acquire or replace the immutable winner's root.
    if let Some(winner) = find_by_run(db, &snapshot.run_id).await? {
        return Ok(winner);
    }
    root.verify(db, &snapshot.workspace_id).await?;
    // Derived locator is not authority: bind the exact run/Task/workspace in
    // the same creation/retry writer, including direct repository callers.
    anyhow::ensure!(
        pioneer_entity::task::Entity::find_by_id(&snapshot.task_id)
            .filter(pioneer_entity::task::Column::WorkspaceId.eq(&snapshot.workspace_id))
            .one(db)
            .await?
            .is_some(),
        "Task snapshot workspace mismatch or missing Task"
    );
    anyhow::ensure!(
        pioneer_entity::task_run::Entity::find_by_id(&snapshot.run_id)
            .filter(pioneer_entity::task_run::Column::TaskId.eq(&snapshot.task_id))
            .one(db)
            .await?
            .is_some(),
        "Task snapshot run identity mismatch"
    );
    let run_id = snapshot.run_id.clone();
    task_run_conversation_snapshot::Entity::insert(task_run_conversation_snapshot::ActiveModel {
        run_id: Set(snapshot.run_id),
        task_id: Set(snapshot.task_id),
        workspace_id: Set(snapshot.workspace_id),
        conversation_thread_id: Set(snapshot.conversation_thread_id),
        source_turn_id: Set(snapshot.source_turn_id),
        history_json: Set(snapshot.history_json),
        frozen_manifest_id: Set(root.locator()),
        created_at: Set(snapshot.created_at),
        ..Default::default()
    })
    .on_conflict(
        OnConflict::column(task_run_conversation_snapshot::Column::RunId)
            .do_nothing()
            .to_owned(),
    )
    // The immutable winner is read below. Avoid RETURNING here because SeaORM
    // maps a legitimate insert-if-absent conflict to `RecordNotInserted`.
    .exec_without_returning(db)
    .await
    .context("failed to insert immutable task run conversation snapshot")?;

    find_by_run(db, run_id.as_str())
        .await?
        .context("task run conversation snapshot is missing after insert")
}

pub async fn find_by_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<Option<StoredTaskRunConversationSnapshot>> {
    use task_run_conversation_snapshot::Column as Col;
    let stored = task_run_conversation_snapshot::Entity::find_by_id(run_id.to_owned())
        .select_only()
        .columns([
            Col::RunId,
            Col::TaskId,
            Col::WorkspaceId,
            Col::ConversationThreadId,
            Col::SourceTurnId,
            Col::CreatedAt,
            Col::HistoryJsonPayloadId,
            Col::PortableRevision,
            Col::PortableIdentity,
        ])
        .expr_as(Expr::cust(HISTORY_BYTES_SQL), "history_bytes")
        .expr_as(Expr::cust("CAST(created_at AS TEXT)"), "created_at_text")
        .into_model::<StoredTaskRunConversationSnapshot>()
        .one(db)
        .await
        .context("failed to query task run conversation snapshot metadata")?;
    stored
        .map(|mut stored| {
            stored.history_json_read_hold = stored
                .history_json_payload_id
                .as_deref()
                .map(super::portable_payload::PayloadReadHold::capture)
                .transpose()?;
            Ok(stored)
        })
        .transpose()
}

pub async fn delete_by_run<C: ConnectionTrait>(db: &C, run_id: &str) -> Result<u64> {
    task_run_conversation_snapshot::Entity::delete_by_id(run_id.to_owned())
        .exec(db)
        .await
        .context("failed to delete task run conversation snapshot")
        .map(|result| result.rows_affected)
}

/// Read only the captured winner after the caller releases its transaction.
/// Every fragment fences identity/revision, owner and logical length. Physical
/// cutover may change the current reference while preserving the captured bytes.
pub(crate) async fn hydrate_record(
    db: &pioneer_sqlite::SqliteDatabase,
    stored: StoredTaskRunConversationSnapshot,
) -> Result<TaskRunConversationSnapshotRecord> {
    observe::scope_result(
        observe::Family::TaskHistory,
        observe::Mode::Full,
        hydrate_record_inner(db, stored),
    )
    .await
}
async fn hydrate_record_inner(
    db: &pioneer_sqlite::SqliteDatabase,
    stored: StoredTaskRunConversationSnapshot,
) -> Result<TaskRunConversationSnapshotRecord> {
    use task_run_conversation_snapshot::Column as Col;
    const BLOCK: usize = 64 * 1024;
    let size = usize::try_from(stored.history_bytes)?;
    let mut bytes = Vec::new();
    let mut original_digest = if let Some(id) = &stored.history_json_payload_id {
        let hold = stored
            .history_json_read_hold
            .as_ref()
            .context("Task snapshot payload hold missing")?;
        let meta = super::portable_payload::header_with_hold(
            db,
            id,
            super::portable_payload::Family::TaskHistory,
            hold,
        )
        .await?;
        anyhow::ensure!(
            meta.original_bytes == stored.history_bytes,
            "Task snapshot length changed"
        );
        if meta.raw == 0 {
            // The captured immutable winner remains readable after its source
            // row is deleted. Never reload a replacement to hydrate this result.
            bytes = super::portable_payload::hydrate_header(db, &meta, String::new())
                .await?
                .into_bytes();
        }
        Some(meta.sha256.clone())
    } else {
        None
    };
    if bytes.is_empty() {
        bytes.try_reserve(size)?;
    }
    while bytes.len() < size {
        let count = BLOCK.min(size - bytes.len());
        let query = task_run_conversation_snapshot::Entity::find_by_id(&stored.run_id)
            .select_only()
            .filter(Col::PortableIdentity.eq(&stored.portable_identity))
            .filter(Col::PortableRevision.eq(stored.portable_revision))
            .filter(Col::TaskId.eq(&stored.task_id))
            .filter(Col::WorkspaceId.eq(&stored.workspace_id))
            .filter(Col::ConversationThreadId.eq(&stored.conversation_thread_id))
            .filter(match &stored.source_turn_id {
                Some(source) => Col::SourceTurnId.eq(source),
                None => Col::SourceTurnId.is_null(),
            })
            .filter(Col::CreatedAt.eq(&stored.created_at_text))
            .filter(Expr::cust(HISTORY_BYTES_SQL).eq(stored.history_bytes));
        let current_ref = query
            .clone()
            .column(Col::HistoryJsonPayloadId)
            .into_tuple::<Option<String>>()
            .one(db)
            .await?
            .ok_or_else(|| {
                observe::outcome(observe::Outcome::Stale);
                super::portable_payload::PreflightRace
            })?;
        let meta = match &current_ref {
            Some(id) => Some(
                super::portable_payload::header(
                    db,
                    id,
                    super::portable_payload::Family::TaskHistory,
                )
                .await?,
            ),
            None => None,
        };
        if let Some(meta) = &meta {
            anyhow::ensure!(
                meta.original_bytes == stored.history_bytes,
                "Task snapshot length changed"
            );
            original_digest = Some(meta.sha256.clone());
        }
        let fragment = if let Some(meta) = meta.as_ref().filter(|meta| meta.raw == 0) {
            super::portable_payload::read_range(db, meta, bytes.len() as i64, count, false).await?
        } else {
            // Legacy/raw bytes use a native range. Pin the captured reference
            // as well as logical version: a concurrent physical cutover retries
            // this read rather than interpreting the small SQL projection.
            let guard = query
                .expr(Expr::cust("task_run_conversation_snapshot.rowid"))
                .filter(match &current_ref {
                    Some(id) => Col::HistoryJsonPayloadId.eq(id),
                    None => Col::HistoryJsonPayloadId.is_null(),
                })
                .limit(1)
                .build(DbBackend::Sqlite);
            let _read = observe::stage(observe::Stage::SourceRead);
            db.read_blob_range(
                pioneer_sqlite::SqliteBlobSource {
                    table: "task_run_conversation_snapshot",
                    column: "history_json",
                },
                guard,
                u64::try_from(bytes.len())?,
                count,
                u64::try_from(size)?,
            )
            .await?
            .ok_or_else(|| {
                observe::outcome(observe::Outcome::Stale);
                super::portable_payload::PreflightRace
            })?
        };
        anyhow::ensure!(fragment.len() == count, "Task snapshot fragment incomplete");
        if meta.as_ref().is_none_or(|meta| meta.raw != 0) {
            observe::bytes(observe::ByteKind::ReadOriginal, fragment.len() as u64);
        }
        let _assembly = observe::stage(observe::Stage::VerifyAssemble);
        bytes.extend(fragment);
    }
    let _verify = observe::stage(observe::Stage::VerifyAssemble);
    if let Some(digest) = original_digest {
        use sha2::{Digest, Sha256};
        anyhow::ensure!(
            hex::encode(Sha256::digest(&bytes)) == digest,
            "Task snapshot digest mismatch"
        );
    }
    Ok(record_from_bytes(stored, String::from_utf8(bytes)?))
}

/// Logical payload changes and owner changes bump the revision; an insert token
/// also fences delete/recreate ABA. Physical cutover may change the reference.
pub(crate) fn same_version(
    stored: &StoredTaskRunConversationSnapshot,
    expected: &TaskRunConversationSnapshotRecord,
) -> bool {
    stored.portable_identity == expected.portable_identity
        && stored.portable_revision == expected.portable_revision
        && stored.run_id == expected.run_id
        && stored.task_id == expected.task_id
        && stored.workspace_id == expected.workspace_id
        && stored.conversation_thread_id == expected.conversation_thread_id
        && stored.source_turn_id == expected.source_turn_id
        && stored.created_at == expected.created_at
}

impl PartialEq for TaskRunConversationSnapshotRecord {
    fn eq(&self, other: &Self) -> bool {
        self.run_id == other.run_id
            && self.task_id == other.task_id
            && self.workspace_id == other.workspace_id
            && self.conversation_thread_id == other.conversation_thread_id
            && self.source_turn_id == other.source_turn_id
            && self.history_json == other.history_json
            && self.created_at == other.created_at
    }
}
impl Eq for TaskRunConversationSnapshotRecord {}

fn record_from_bytes(
    stored: StoredTaskRunConversationSnapshot,
    history_json: String,
) -> TaskRunConversationSnapshotRecord {
    TaskRunConversationSnapshotRecord {
        run_id: stored.run_id,
        task_id: stored.task_id,
        workspace_id: stored.workspace_id,
        conversation_thread_id: stored.conversation_thread_id,
        source_turn_id: stored.source_turn_id,
        created_at: stored.created_at,
        history_json_payload_id: stored.history_json_payload_id,
        portable_revision: stored.portable_revision,
        portable_identity: stored.portable_identity,
        history_json,
    }
}
pub(crate) fn record_from_insert(
    stored: StoredTaskRunConversationSnapshot,
    history_json: String,
) -> Result<TaskRunConversationSnapshotRecord> {
    anyhow::ensure!(
        stored.history_json_payload_id.is_none()
            && stored.history_bytes == i64::try_from(history_json.len())?,
        "Task snapshot insert bytes changed"
    );
    Ok(record_from_bytes(stored, history_json))
}
