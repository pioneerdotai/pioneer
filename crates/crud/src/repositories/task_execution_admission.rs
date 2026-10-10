use anyhow::{Context, Result};
use pioneer_entity::task_execution_admission;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ConnectionTrait, EntityTrait, Set, Statement};

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

impl NewTaskExecutionAdmission {
    pub(crate) fn validate_context_size(&self) -> Result<()> {
        anyhow::ensure!(
            self.authorization_context_json.len()
                <= pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES,
            "Task execution authorization context exceeds byte limit",
        );
        Ok(())
    }
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
    admission.validate_context_size()?;
    let expected = admission.clone();
    let task_id = admission.task_id.clone();
    let execution_lease = admission.execution_lease.clone();
    let created_at = admission.created_at;
    if let Some(persisted) = find_by_task(db, task_id.as_str()).await? {
        ensure_matches(&persisted, &expected)?;
        if let Some(execution_lease) = execution_lease {
            super::execution_admission_lease::reserve(db, execution_lease, created_at).await?;
        }
        return Ok(persisted);
    }
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
    ensure_matches(&persisted, &expected)?;
    Ok(persisted)
}

/// Persists (or verifies) the immutable Task authority and reacquires its
/// released quota lease for an explicit blocked-Task resume.
pub async fn readmit_immutable<C: ConnectionTrait>(
    db: &C,
    admission: NewTaskExecutionAdmission,
) -> Result<TaskExecutionAdmissionRecord> {
    let execution_lease = admission.execution_lease.clone();
    let reacquired_at = admission.created_at;
    let mut admission_without_lease = admission;
    admission_without_lease.execution_lease = None;
    let persisted = insert_immutable(db, admission_without_lease).await?;
    if let Some(execution_lease) = execution_lease {
        super::execution_admission_lease::reacquire(db, execution_lease, reacquired_at).await?;
    }
    Ok(persisted)
}

pub async fn find_by_task<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
) -> Result<Option<TaskExecutionAdmissionRecord>> {
    let limit = pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES as i64;
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "SELECT task_id, workspace_id, root_thread_id, initiating_principal_id, created_at, \
         CASE WHEN length(cast(authorization_context_json as blob)) <= ? \
         THEN authorization_context_json ELSE NULL END AS context_json, \
         length(cast(authorization_context_json as blob)) AS context_bytes \
         FROM task_execution_admission WHERE task_id=?",
            [limit.into(), task_id.into()],
        ))
        .await
        .context("failed to query bounded Task execution admission")?;
    let Some(row) = row else { return Ok(None) };
    anyhow::ensure!(
        row.try_get::<i64>("", "context_bytes")? <= limit,
        "Task execution authorization context exceeds byte limit"
    );
    Ok(Some(TaskExecutionAdmissionRecord {
        task_id: row.try_get("", "task_id")?,
        workspace_id: row.try_get("", "workspace_id")?,
        root_thread_id: row.try_get("", "root_thread_id")?,
        initiating_principal_id: row.try_get("", "initiating_principal_id")?,
        authorization_context_json: row.try_get("", "context_json")?,
        created_at: row.try_get("", "created_at")?,
    }))
}

fn ensure_matches(
    persisted: &TaskExecutionAdmissionRecord,
    expected: &NewTaskExecutionAdmission,
) -> Result<()> {
    if persisted.task_id != expected.task_id
        || persisted.workspace_id != expected.workspace_id
        || persisted.root_thread_id != expected.root_thread_id
        || persisted.initiating_principal_id != expected.initiating_principal_id
        || persisted.authorization_context_json != expected.authorization_context_json
    {
        anyhow::bail!("Task execution admission conflicts with its immutable persisted value");
    }
    Ok(())
}
