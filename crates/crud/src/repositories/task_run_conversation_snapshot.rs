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

pub async fn insert_if_absent<C: ConnectionTrait>(
    db: &C,
    snapshot: NewTaskRunConversationSnapshot,
) -> Result<TaskRunConversationSnapshotRecord> {
    let run_id = snapshot.run_id.clone();
    task_run_conversation_snapshot::Entity::insert(task_run_conversation_snapshot::ActiveModel {
        run_id: Set(snapshot.run_id),
        task_id: Set(snapshot.task_id),
        workspace_id: Set(snapshot.workspace_id),
        conversation_thread_id: Set(snapshot.conversation_thread_id),
        source_turn_id: Set(snapshot.source_turn_id),
        history_json: Set(snapshot.history_json),
        created_at: Set(snapshot.created_at),
    })
    .on_conflict(
        OnConflict::column(task_run_conversation_snapshot::Column::RunId)
            .do_nothing()
            .to_owned(),
    )
    .exec(db)
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
