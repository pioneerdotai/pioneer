//! Durable revision of the tasks attached to one conversation turn.
use crate::{task as task_repository, task::task_from_db_model, task_run};
use anyhow::{Context, Result};
use pioneer_protocol::TaskAttachmentMode;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskFinalizationRevision {
    pub token: String,
    pub attached_task_count: usize,
}

pub async fn task_finalization_revision_for_connection<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<TaskFinalizationRevision> {
    let mut tasks =
        task_repository::list_tasks_by_creator_turns(db, thread_id, &[turn_id.to_owned()])
            .await?
            .into_iter()
            .filter_map(|model| match task_from_db_model(model.clone()) {
                Ok(task)
                    if task.lifecycle_policy.as_ref().is_some_and(|policy| {
                        policy.attachment == TaskAttachmentMode::Attached
                    }) =>
                {
                    Some(Ok(model))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>>>()?;
    tasks.sort_by(|left, right| left.id.cmp(&right.id));

    if tasks.is_empty() {
        return Ok(TaskFinalizationRevision {
            token: "tasks:0".to_owned(),
            attached_task_count: 0,
        });
    }

    let task_ids = tasks.iter().map(|task| task.id.clone()).collect::<Vec<_>>();
    let mut runs = task_run::list_runs_by_tasks(db, task_ids.as_slice()).await?;
    runs.sort_by(|left, right| left.id.cmp(&right.id));

    let mut executions = pioneer_entity::task_run_execution::Entity::find()
        .filter(pioneer_entity::task_run_execution::Column::TaskId.is_in(task_ids.clone()))
        .all(db)
        .await
        .context("failed to load task executions for finalization revision")?;
    executions.sort_by(|left, right| left.id.cmp(&right.id));

    let mut run_turns = pioneer_entity::task_run_turn::Entity::find()
        .filter(pioneer_entity::task_run_turn::Column::TaskId.is_in(task_ids.clone()))
        .all(db)
        .await
        .context("failed to load task run turns for finalization revision")?;
    run_turns.sort_by(|left, right| left.id.cmp(&right.id));

    let mut thread_bindings = pioneer_entity::task_run_thread_binding::Entity::find()
        .filter(pioneer_entity::task_run_thread_binding::Column::TaskId.is_in(task_ids.clone()))
        .all(db)
        .await
        .context("failed to load task thread bindings for finalization revision")?;
    thread_bindings.sort_by(|left, right| left.id.cmp(&right.id));

    let mut candidates = pioneer_entity::task_result_candidate::Entity::find()
        .filter(pioneer_entity::task_result_candidate::Column::TaskId.is_in(task_ids))
        .all(db)
        .await
        .context("failed to load task result candidates for finalization revision")?;
    candidates.sort_by(|left, right| left.id.cmp(&right.id));

    let encoded = serde_json::to_vec(&(
        &tasks,
        &runs,
        &executions,
        &run_turns,
        &thread_bindings,
        &candidates,
    ))
    .context("failed to encode attached-task finalization revision")?;
    let digest = hex::encode(Sha256::digest(encoded));
    Ok(TaskFinalizationRevision {
        token: format!("tasks:{}:{digest}", tasks.len()),
        attached_task_count: tasks.len(),
    })
}
