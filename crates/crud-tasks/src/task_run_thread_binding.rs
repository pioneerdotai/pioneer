use anyhow::{Context, Result};
use pioneer_entity::task_run_thread_binding;
use pioneer_protocol::{TaskRunThreadBinding, TaskRunThreadBindingKind};
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

use crate::status::{task_run_thread_binding_kind_from_db, task_run_thread_binding_kind_to_db};
use crate::values::unix_to_datetime;

pub struct NewTaskRunThreadBinding {
    pub id: String,
    pub task_id: String,
    pub run_id: String,
    pub execution_id: Option<String>,
    pub thread_id: String,
    pub binding_kind: TaskRunThreadBindingKind,
    pub created_at: i64,
}

pub async fn upsert_binding<C: ConnectionTrait>(
    db: &C,
    binding: NewTaskRunThreadBinding,
) -> Result<()> {
    task_run_thread_binding::Entity::insert(active_model_from_new_binding(binding))
        .on_conflict(
            OnConflict::column(task_run_thread_binding::Column::Id)
                .update_columns([
                    task_run_thread_binding::Column::TaskId,
                    task_run_thread_binding::Column::RunId,
                    task_run_thread_binding::Column::ExecutionId,
                    task_run_thread_binding::Column::ThreadId,
                    task_run_thread_binding::Column::BindingKind,
                ])
                .to_owned(),
        )
        .exec(db)
        .await
        .context("failed to upsert task run thread binding")?;
    Ok(())
}

pub async fn find_binding_by_id<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<task_run_thread_binding::Model>> {
    task_run_thread_binding::Entity::find_by_id(id.to_owned())
        .one(db)
        .await
        .context("failed to query task run thread binding by id")
}

pub async fn find_binding_by_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Option<task_run_thread_binding::Model>> {
    task_run_thread_binding::Entity::find()
        .filter(task_run_thread_binding::Column::ThreadId.eq(thread_id.to_owned()))
        .one(db)
        .await
        .context("failed to query task run thread binding by thread")
}

pub async fn find_binding_by_run_and_kind<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
    binding_kind: TaskRunThreadBindingKind,
) -> Result<Option<task_run_thread_binding::Model>> {
    task_run_thread_binding::Entity::find()
        .filter(task_run_thread_binding::Column::RunId.eq(run_id.to_owned()))
        .filter(
            task_run_thread_binding::Column::BindingKind
                .eq(task_run_thread_binding_kind_to_db(binding_kind)),
        )
        .order_by_asc(task_run_thread_binding::Column::CreatedAt)
        .one(db)
        .await
        .context("failed to query task run thread binding by run and kind")
}

pub async fn list_bindings_by_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<Vec<task_run_thread_binding::Model>> {
    task_run_thread_binding::Entity::find()
        .filter(task_run_thread_binding::Column::RunId.eq(run_id.to_owned()))
        .order_by_asc(task_run_thread_binding::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list task run thread bindings by run")
}

fn active_model_from_new_binding(
    binding: NewTaskRunThreadBinding,
) -> task_run_thread_binding::ActiveModel {
    task_run_thread_binding::ActiveModel {
        id: Set(binding.id),
        task_id: Set(binding.task_id),
        run_id: Set(binding.run_id),
        execution_id: Set(binding.execution_id),
        thread_id: Set(binding.thread_id),
        binding_kind: Set(task_run_thread_binding_kind_to_db(binding.binding_kind)),
        created_at: Set(unix_to_datetime(binding.created_at)),
    }
}

pub fn task_run_thread_binding_from_db_model(
    model: pioneer_entity::task_run_thread_binding::Model,
) -> Result<TaskRunThreadBinding> {
    Ok(TaskRunThreadBinding {
        id: model.id,
        task_id: model.task_id,
        run_id: model.run_id,
        execution_id: model.execution_id,
        thread_id: model.thread_id,
        binding_kind: task_run_thread_binding_kind_from_db(model.binding_kind.as_str())?,
        created_at: model.created_at.timestamp(),
    })
}
