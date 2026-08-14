use anyhow::{Context, Result};
use pioneer_entity::task_execution_admission;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ConnectionTrait, EntityTrait, Set};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTaskExecutionAdmission {
    pub task_id: String,
    pub workspace_id: String,
    pub root_thread_id: String,
    pub initiating_principal_id: String,
    pub authorization_context_json: String,
    pub created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    pub execution_lease: Option<super::execution_admission_lease::NewExecutionAdmissionLease>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskExecutionAdmissionRecord {
    pub task_id: String,
    pub workspace_id: String,
    pub root_thread_id: String,
    pub initiating_principal_id: String,
    pub authorization_context_json: String,
    pub created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
}

pub async fn insert_immutable<C: ConnectionTrait>(
    db: &C,
    admission: NewTaskExecutionAdmission,
) -> Result<TaskExecutionAdmissionRecord> {
    let expected = admission.clone();
    let task_id = admission.task_id.clone();
    let execution_lease = admission.execution_lease.clone();
    let created_at = admission.created_at;
    task_execution_admission::Entity::insert(task_execution_admission::ActiveModel {
        task_id: Set(admission.task_id),
        workspace_id: Set(admission.workspace_id),
        root_thread_id: Set(admission.root_thread_id),
        initiating_principal_id: Set(admission.initiating_principal_id),
        authorization_context_json: Set(admission.authorization_context_json),
        created_at: Set(admission.created_at),
    })
    .on_conflict(
        OnConflict::column(task_execution_admission::Column::TaskId)
            .do_nothing()
            .to_owned(),
    )
    .exec(db)
    .await
    .context("failed to insert immutable Task execution admission")?;
    if let Some(execution_lease) = execution_lease {
        super::execution_admission_lease::reserve(db, execution_lease, created_at).await?;
    }

    let persisted = find_by_task(db, task_id.as_str())
        .await?
        .context("Task execution admission is missing after insert")?;
    if persisted.task_id != expected.task_id
        || persisted.workspace_id != expected.workspace_id
        || persisted.root_thread_id != expected.root_thread_id
        || persisted.initiating_principal_id != expected.initiating_principal_id
        || persisted.authorization_context_json != expected.authorization_context_json
    {
        anyhow::bail!("Task execution admission conflicts with its immutable persisted value");
    }
    Ok(persisted)
}

pub async fn find_by_task<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
) -> Result<Option<TaskExecutionAdmissionRecord>> {
    task_execution_admission::Entity::find_by_id(task_id.to_owned())
        .one(db)
        .await
        .context("failed to query Task execution admission")
        .map(|record| record.map(record_from_model))
}

fn record_from_model(model: task_execution_admission::Model) -> TaskExecutionAdmissionRecord {
    TaskExecutionAdmissionRecord {
        task_id: model.task_id,
        workspace_id: model.workspace_id,
        root_thread_id: model.root_thread_id,
        initiating_principal_id: model.initiating_principal_id,
        authorization_context_json: model.authorization_context_json,
        created_at: model.created_at,
    }
}
