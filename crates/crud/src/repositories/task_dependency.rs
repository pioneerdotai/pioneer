#![allow(dead_code)]

use anyhow::{Context, Result};
use pioneer_entity::task_dependency;
use pioneer_protocol::TaskDependency;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

use crate::util::{optional_typed_json_to_db, unix_to_datetime};

#[derive(Clone, Debug)]
pub struct PreparedTaskDependencyProjection(task_dependency::ActiveModel);

pub fn prepare_dependency_projection(
    dependency: &TaskDependency,
) -> Result<PreparedTaskDependencyProjection> {
    active_model_from_dependency(dependency).map(PreparedTaskDependencyProjection)
}

pub async fn upsert_dependency<C: ConnectionTrait>(
    db: &C,
    dependency: &TaskDependency,
) -> Result<()> {
    upsert_prepared_dependency(db, prepare_dependency_projection(dependency)?).await
}

pub async fn upsert_prepared_dependency<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTaskDependencyProjection,
) -> Result<()> {
    task_dependency::Entity::insert(prepared.0)
        .on_conflict(
            OnConflict::columns([
                task_dependency::Column::TaskId,
                task_dependency::Column::DependsOnTaskId,
                task_dependency::Column::Kind,
            ])
            .update_columns([task_dependency::Column::ConditionJson])
            .to_owned(),
        )
        .exec(db)
        .await
        .context("failed to upsert task dependency")?;
    Ok(())
}

pub async fn list_dependencies_for_task<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
) -> Result<Vec<task_dependency::Model>> {
    task_dependency::Entity::find()
        .filter(task_dependency::Column::TaskId.eq(task_id.to_owned()))
        .order_by_asc(task_dependency::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list task dependencies")
}

pub async fn list_tasks_blocked_by_dependency<C: ConnectionTrait>(
    db: &C,
    depends_on_task_id: &str,
) -> Result<Vec<task_dependency::Model>> {
    task_dependency::Entity::find()
        .filter(task_dependency::Column::DependsOnTaskId.eq(depends_on_task_id.to_owned()))
        .order_by_asc(task_dependency::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list tasks blocked by dependency")
}

fn active_model_from_dependency(
    dependency: &TaskDependency,
) -> Result<task_dependency::ActiveModel> {
    Ok(task_dependency::ActiveModel {
        id: Set(dependency.id.clone()),
        task_id: Set(dependency.task_id.clone()),
        depends_on_task_id: Set(dependency.depends_on_task_id.clone()),
        kind: Set(dependency.kind.clone()),
        condition_json: Set(optional_typed_json_to_db(&dependency.condition)?),
        created_at: Set(unix_to_datetime(dependency.created_at)),
    })
}
