#![allow(dead_code)]

use anyhow::{Context, Result};
use pioneer_entity::task_trigger;
use pioneer_protocol::{TaskTrigger, TaskTriggerStatus};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

use crate::convention::{task_trigger_kind_to_db, task_trigger_status_to_db};
use crate::util::unix_to_datetime;

pub async fn upsert_trigger<C: ConnectionTrait>(db: &C, trigger: &TaskTrigger) -> Result<()> {
    task_trigger::Entity::insert(active_model_from_trigger(trigger)?)
        .on_conflict(
            OnConflict::column(task_trigger::Column::Id)
                .update_columns([
                    task_trigger::Column::TaskId,
                    task_trigger::Column::Kind,
                    task_trigger::Column::Status,
                    task_trigger::Column::SpecJson,
                    task_trigger::Column::NextFireAt,
                    task_trigger::Column::LastFireAt,
                    task_trigger::Column::UpdatedAt,
                ])
                .to_owned(),
        )
        .exec(db)
        .await
        .context("failed to upsert task trigger")?;
    Ok(())
}

pub async fn list_triggers_by_task<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
) -> Result<Vec<task_trigger::Model>> {
    task_trigger::Entity::find()
        .filter(task_trigger::Column::TaskId.eq(task_id.to_owned()))
        .order_by_asc(task_trigger::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list task triggers")
}

pub async fn find_trigger_by_id<C: ConnectionTrait>(
    db: &C,
    trigger_id: &str,
) -> Result<Option<task_trigger::Model>> {
    task_trigger::Entity::find_by_id(trigger_id.to_owned())
        .one(db)
        .await
        .context("failed to query task trigger by id")
}

pub async fn list_due_active_triggers<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
) -> Result<Vec<task_trigger::Model>> {
    task_trigger::Entity::find()
        .filter(task_trigger::Column::Status.eq("active"))
        .filter(task_trigger::Column::NextFireAt.lte(now))
        .order_by_asc(task_trigger::Column::NextFireAt)
        .all(db)
        .await
        .context("failed to list due task triggers")
}

pub async fn list_active_triggers<C: ConnectionTrait>(db: &C) -> Result<Vec<task_trigger::Model>> {
    task_trigger::Entity::find()
        .filter(task_trigger::Column::Status.eq("active"))
        .order_by_asc(task_trigger::Column::NextFireAt)
        .all(db)
        .await
        .context("failed to list active task triggers")
}

pub async fn update_trigger_schedule<C: ConnectionTrait>(
    db: &C,
    trigger_id: &str,
    status: TaskTriggerStatus,
    next_fire_at: Option<DateTimeWithTimeZone>,
    last_fire_at: Option<DateTimeWithTimeZone>,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    task_trigger::Entity::update_many()
        .filter(task_trigger::Column::Id.eq(trigger_id.to_owned()))
        .col_expr(
            task_trigger::Column::Status,
            Expr::value(task_trigger_status_to_db(status)),
        )
        .col_expr(task_trigger::Column::NextFireAt, Expr::value(next_fire_at))
        .col_expr(task_trigger::Column::LastFireAt, Expr::value(last_fire_at))
        .col_expr(task_trigger::Column::UpdatedAt, Expr::value(updated_at))
        .exec(db)
        .await
        .context("failed to update task trigger schedule")?;
    Ok(())
}

fn active_model_from_trigger(trigger: &TaskTrigger) -> Result<task_trigger::ActiveModel> {
    let spec_json =
        serde_json::to_string(&trigger.spec).context("failed to serialize task trigger spec")?;

    Ok(task_trigger::ActiveModel {
        id: Set(trigger.id.clone()),
        task_id: Set(trigger.task_id.clone()),
        kind: Set(task_trigger_kind_to_db(trigger.kind()).to_owned()),
        status: Set(task_trigger_status_to_db(trigger.status).to_owned()),
        spec_json: Set(spec_json),
        next_fire_at: Set(trigger.next_fire_at.map(unix_to_datetime)),
        last_fire_at: Set(trigger.last_fire_at.map(unix_to_datetime)),
        created_at: Set(unix_to_datetime(trigger.created_at)),
        updated_at: Set(unix_to_datetime(trigger.updated_at)),
    })
}
