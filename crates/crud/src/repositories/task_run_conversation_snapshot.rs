use anyhow::{Context, Result};
use pioneer_entity::task_run_conversation_snapshot;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ConnectionTrait, EntityTrait, Set};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRunConversationSnapshotRecord {
    pub run_id: String,
    pub task_id: String,
    pub workspace_id: String,
    pub conversation_thread_id: String,
    pub source_turn_id: Option<String>,
    pub history_json: String,
    pub created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
}

pub(crate) async fn insert_if_absent<C: ConnectionTrait>(
    db: &C,
    snapshot: NewTaskRunConversationSnapshot,
    root: &super::compaction_frozen_root::RootPublication,
) -> Result<TaskRunConversationSnapshotRecord> {
    root.validate_in(db, &snapshot.workspace_id, &snapshot.conversation_thread_id)
        .await?;
    let domain = db.query_one_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DbBackend::Sqlite,
        "SELECT r.id FROM task_run r JOIN task t ON t.id=r.task_id JOIN workspace w ON w.id=t.workspace_id \
         JOIN thread c ON c.id=? AND c.workspace_id=t.workspace_id \
         WHERE r.id=? AND r.task_id=? AND t.workspace_id=? \
         AND (? IS NULL OR EXISTS(SELECT 1 FROM turn v WHERE v.id=? AND v.thread_id=c.id))",
        [snapshot.conversation_thread_id.clone().into(), snapshot.run_id.clone().into(), snapshot.task_id.clone().into(),
         snapshot.workspace_id.clone().into(), snapshot.source_turn_id.clone().into(), snapshot.source_turn_id.clone().into()])).await?;
    anyhow::ensure!(domain.is_some(), "Task snapshot domain is unavailable");
    let run_id = snapshot.run_id.clone();
    task_run_conversation_snapshot::Entity::insert(task_run_conversation_snapshot::ActiveModel {
        run_id: Set(snapshot.run_id),
        task_id: Set(snapshot.task_id),
        workspace_id: Set(snapshot.workspace_id),
        conversation_thread_id: Set(snapshot.conversation_thread_id),
        source_turn_id: Set(snapshot.source_turn_id),
        history_json: Set(snapshot.history_json),
        frozen_manifest_id: Set(root.root.id()),
        frozen_root_state: Set(root.root.state().to_owned()),
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
) -> Result<Option<TaskRunConversationSnapshotRecord>> {
    task_run_conversation_snapshot::Entity::find_by_id(run_id.to_owned())
        .one(db)
        .await
        .context("failed to query task run conversation snapshot")
        .map(|record| record.map(record_from_model))
}

pub async fn delete_by_run<C: ConnectionTrait>(db: &C, run_id: &str) -> Result<u64> {
    task_run_conversation_snapshot::Entity::delete_by_id(run_id.to_owned())
        .exec(db)
        .await
        .context("failed to delete task run conversation snapshot")
        .map(|result| result.rows_affected)
}

fn record_from_model(
    model: task_run_conversation_snapshot::Model,
) -> TaskRunConversationSnapshotRecord {
    TaskRunConversationSnapshotRecord {
        run_id: model.run_id,
        task_id: model.task_id,
        workspace_id: model.workspace_id,
        conversation_thread_id: model.conversation_thread_id,
        source_turn_id: model.source_turn_id,
        history_json: model.history_json,
        created_at: model.created_at,
    }
}
