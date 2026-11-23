use anyhow::{Context, Result, ensure};
use pioneer_crud_payloads::storage::{self, CapturedPayload, PreparedPayload, Selection};
use pioneer_entity::{task, task_run, task_run_conversation_snapshot as snapshot};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, FromQueryResult, QueryFilter,
    QuerySelect, Set,
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

#[derive(Debug, FromQueryResult)]
pub struct StoredTaskRunConversationSnapshot {
    pub run_id: String,
    pub task_id: String,
    pub workspace_id: String,
    pub conversation_thread_id: String,
    pub source_turn_id: Option<String>,
    pub created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    pub history_json_payload_id: Option<String>,
    pub portable_revision: i64,
    pub portable_identity: String,
}

pub enum CapturedHistory {
    Frozen(String),
    Payload(CapturedPayload),
}

impl CapturedHistory {
    pub fn into_text(self) -> Result<String> {
        match self {
            Self::Frozen(locator) => Ok(locator),
            Self::Payload(payload) => payload.into_text(),
        }
    }
}

pub struct CapturedSnapshot {
    metadata: StoredTaskRunConversationSnapshot,
    history: CapturedHistory,
}

impl CapturedSnapshot {
    pub fn into_record(self) -> Result<TaskRunConversationSnapshotRecord> {
        let history_json = self.history.into_text()?;
        Ok(record_from_bytes(self.metadata, history_json))
    }
}

pub async fn insert_if_absent(
    tx: &DatabaseTransaction,
    input: &NewTaskRunConversationSnapshot,
    frozen_manifest_id: Option<String>,
    prepared: Option<&PreparedPayload>,
) -> Result<(StoredTaskRunConversationSnapshot, bool)> {
    ensure!(
        frozen_manifest_id.is_some() != prepared.is_some(),
        "expected either a frozen locator or a prepared Task history"
    );
    ensure!(
        task::Entity::find_by_id(&input.task_id)
            .filter(task::Column::WorkspaceId.eq(&input.workspace_id))
            .select_only()
            .column(task::Column::Id)
            .into_tuple::<String>()
            .one(tx)
            .await?
            .is_some(),
        "Task snapshot workspace mismatch or missing Task"
    );
    ensure!(
        task_run::Entity::find_by_id(&input.run_id)
            .filter(task_run::Column::TaskId.eq(&input.task_id))
            .select_only()
            .column(task_run::Column::Id)
            .lock_exclusive()
            .into_tuple::<String>()
            .one(tx)
            .await?
            .is_some(),
        "Task snapshot run identity mismatch"
    );
    // The run lock orders concurrent contenders even before a snapshot exists.
    if let Some(winner) = find_by_run(tx, &input.run_id).await? {
        return Ok((winner, false));
    }
    if let Some(prepared) = prepared {
        prepared.insert(tx).await?;
    }
    snapshot::Entity::insert(snapshot::ActiveModel {
        run_id: Set(input.run_id.clone()),
        task_id: Set(input.task_id.clone()),
        workspace_id: Set(input.workspace_id.clone()),
        conversation_thread_id: Set(input.conversation_thread_id.clone()),
        source_turn_id: Set(input.source_turn_id.clone()),
        history_json: Set(frozen_manifest_id
            .as_ref()
            .map(|_| input.history_json.clone())),
        history_json_payload_id: Set(prepared.map(|value| value.id().to_owned())),
        history_json_has_array_prefix: Set(
            prepared.is_some() && input.history_json.trim_start_matches(' ').starts_with('[')
        ),
        frozen_manifest_id: Set(frozen_manifest_id),
        created_at: Set(input.created_at),
        ..Default::default()
    })
    .exec_without_returning(tx)
    .await
    .context("failed to insert immutable Task run conversation snapshot")?;
    let stored = find_by_run(tx, &input.run_id)
        .await?
        .context("Task run conversation snapshot missing after insert")?;
    Ok((stored, true))
}

pub async fn find_by_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<Option<StoredTaskRunConversationSnapshot>> {
    use snapshot::Column as C;
    snapshot::Entity::find_by_id(run_id)
        .select_only()
        .columns([
            C::RunId,
            C::TaskId,
            C::WorkspaceId,
            C::ConversationThreadId,
            C::SourceTurnId,
            C::CreatedAt,
            C::HistoryJsonPayloadId,
            C::PortableRevision,
            C::PortableIdentity,
        ])
        .into_model()
        .one(db)
        .await
        .context("failed to query Task run conversation snapshot metadata")
}

/// Capture the row and payload from the caller's repeatable-read snapshot.
pub async fn capture_snapshot<S: ConnectionTrait>(
    tx: &S,
    run_id: &str,
) -> Result<Option<CapturedSnapshot>> {
    let Some(metadata) = find_by_run(tx, run_id).await? else {
        return Ok(None);
    };
    let history = capture_history(tx, run_id, metadata.history_json_payload_id.as_deref()).await?;
    Ok(Some(CapturedSnapshot { metadata, history }))
}

pub async fn capture_history<S: ConnectionTrait>(
    tx: &S,
    run_id: &str,
    payload_id: Option<&str>,
) -> Result<CapturedHistory> {
    if let Some(id) = payload_id {
        let payload = storage::capture(tx, id, Selection::Full)
            .await?
            .context("missing Task history payload")?;
        Ok(CapturedHistory::Payload(payload))
    } else {
        let locator = snapshot::Entity::find_by_id(run_id)
            .select_only()
            .column(snapshot::Column::HistoryJson)
            .into_tuple::<String>()
            .one(tx)
            .await?
            .context("missing frozen Task history locator")?;
        Ok(CapturedHistory::Frozen(locator))
    }
}

pub async fn delete_by_run<C: ConnectionTrait>(db: &C, run_id: &str) -> Result<u64> {
    snapshot::Entity::delete_by_id(run_id)
        .exec(db)
        .await
        .context("failed to delete Task run conversation snapshot")
        .map(|result| result.rows_affected)
}

/// Logical payload changes and owner changes bump the revision; an insert token
/// also fences delete/recreate ABA. Changing the payload advances the revision.
pub fn same_version(
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

pub fn record_from_bytes(
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

pub async fn get_scoped(
    connection: &pioneer_postgres_pools::PostgresDatabase,
    run_id: &str,
) -> Result<Option<TaskRunConversationSnapshotRecord>> {
    let reader = connection.begin_read().await?;
    let captured = capture_snapshot(&reader, run_id).await?;
    reader.commit().await?;
    captured.map(|capture| capture.into_record()).transpose()
}
