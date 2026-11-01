#![allow(dead_code)]

use anyhow::{Context, Result};
use pioneer_entity::task_trigger;
use pioneer_protocol::{TaskTrigger, TaskTriggerStatus};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::convention::{task_trigger_kind_to_db, task_trigger_status_to_db};
use crate::util::unix_to_datetime;

#[derive(Clone, Debug)]
pub struct PreparedTaskTriggerProjection(task_trigger::ActiveModel);

pub fn prepare_trigger_projection(trigger: &TaskTrigger) -> Result<PreparedTaskTriggerProjection> {
    active_model_from_trigger(trigger).map(PreparedTaskTriggerProjection)
}

pub async fn upsert_trigger<C: ConnectionTrait>(db: &C, trigger: &TaskTrigger) -> Result<()> {
    upsert_prepared_trigger(db, prepare_trigger_projection(trigger)?).await
}

pub async fn upsert_prepared_trigger<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTaskTriggerProjection,
) -> Result<()> {
    task_trigger::Entity::insert(prepared.0)
        .on_conflict(
            OnConflict::column(task_trigger::Column::Id)
                .update_columns([
                    task_trigger::Column::TaskId,
                    task_trigger::Column::WebhookSecretRef,
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

/// Bounded foreign-writer fallback; status/next_fire_at use idx_task_trigger_due.
pub async fn list_due_active_triggers_bounded<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    after: Option<(DateTimeWithTimeZone, &str)>,
    limit: u64,
) -> Result<Vec<task_trigger::Model>> {
    list_due_active_triggers_bounded_through(db, now, after, None, limit).await
}

pub async fn list_due_active_triggers_bounded_through<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    after: Option<(DateTimeWithTimeZone, &str)>,
    upper: Option<(DateTimeWithTimeZone, &str)>,
    limit: u64,
) -> Result<Vec<task_trigger::Model>> {
    let mut query = task_trigger::Entity::find()
        .filter(task_trigger::Column::Status.eq("active"))
        .filter(task_trigger::Column::NextFireAt.lte(now));
    if let Some((at, id)) = after {
        query = query.filter(
            Condition::any()
                .add(task_trigger::Column::NextFireAt.gt(at))
                .add(
                    Condition::all()
                        .add(task_trigger::Column::NextFireAt.eq(at))
                        .add(task_trigger::Column::Id.gt(id)),
                ),
        );
    }
    if let Some((at, id)) = upper {
        query = query.filter(
            Condition::any()
                .add(task_trigger::Column::NextFireAt.lt(at))
                .add(
                    Condition::all()
                        .add(task_trigger::Column::NextFireAt.eq(at))
                        .add(task_trigger::Column::Id.lte(id)),
                ),
        );
    }
    query
        .order_by_asc(task_trigger::Column::NextFireAt)
        .order_by_asc(task_trigger::Column::Id)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list bounded due task triggers")
}

/// NULL schedules are dormant, not absent. Cursor rotates the bounded audit
/// even if the first dormant row is permanently ineligible for recovery.
pub async fn list_dormant_active_triggers<C: ConnectionTrait>(
    db: &C,
    after_id: Option<&str>,
    limit: u64,
) -> Result<Vec<task_trigger::Model>> {
    list_dormant_active_triggers_through(db, after_id, None, limit).await
}

pub async fn list_dormant_active_triggers_through<C: ConnectionTrait>(
    db: &C,
    after_id: Option<&str>,
    upper: Option<&str>,
    limit: u64,
) -> Result<Vec<task_trigger::Model>> {
    let mut query = task_trigger::Entity::find()
        .filter(task_trigger::Column::Status.eq("active"))
        .filter(task_trigger::Column::NextFireAt.is_null());
    if let Some(after_id) = after_id {
        query = query.filter(task_trigger::Column::Id.gt(after_id));
    }
    if let Some(id) = upper {
        query = query.filter(task_trigger::Column::Id.lte(id));
    }
    query
        .order_by_asc(task_trigger::Column::Id)
        .limit(limit)
        .all(db)
        .await
        .context("failed to audit dormant task triggers")
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

/// Stops every schedulable trigger for a Task that entered a blocked state.
///
/// The caller supplies the same database transaction that projects the
/// `TaskBlocked` event, so the Task status and its trigger schedules cannot
/// become visible independently.
pub async fn pause_active_triggers_for_blocked_task<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    updated_at: DateTimeWithTimeZone,
) -> Result<u64> {
    let next_fire_at: Option<DateTimeWithTimeZone> = None;
    let result = task_trigger::Entity::update_many()
        .filter(task_trigger::Column::TaskId.eq(task_id.to_owned()))
        .filter(task_trigger::Column::Status.eq("active"))
        .col_expr(
            task_trigger::Column::Status,
            Expr::value(task_trigger_status_to_db(TaskTriggerStatus::Paused)),
        )
        .col_expr(task_trigger::Column::NextFireAt, Expr::value(next_fire_at))
        .col_expr(task_trigger::Column::UpdatedAt, Expr::value(updated_at))
        .exec(db)
        .await
        .context("failed to pause active triggers for blocked task")?;
    Ok(result.rows_affected)
}

fn active_model_from_trigger(trigger: &TaskTrigger) -> Result<task_trigger::ActiveModel> {
    let spec_json =
        serde_json::to_string(&trigger.spec).context("failed to serialize task trigger spec")?;

    Ok(task_trigger::ActiveModel {
        id: Set(trigger.id.clone()),
        task_id: Set(trigger.task_id.clone()),
        webhook_secret_ref: Set(trigger.webhook_secret_ref.clone()),
        kind: Set(task_trigger_kind_to_db(trigger.kind()).to_owned()),
        status: Set(task_trigger_status_to_db(trigger.status).to_owned()),
        spec_json: Set(spec_json),
        next_fire_at: Set(trigger.next_fire_at.map(unix_to_datetime)),
        last_fire_at: Set(trigger.last_fire_at.map(unix_to_datetime)),
        created_at: Set(unix_to_datetime(trigger.created_at)),
        updated_at: Set(unix_to_datetime(trigger.updated_at)),
    })
}

/// Addressed scheduling inputs exclude inactive historical metadata before decoding.
pub async fn list_scheduler_active_triggers<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
) -> Result<Vec<task_trigger::Model>> {
    task_trigger::Entity::find()
        .filter(task_trigger::Column::TaskId.eq(task_id))
        .filter(task_trigger::Column::Status.eq("active"))
        .order_by_asc(task_trigger::Column::Id)
        .all(db)
        .await
        .context("failed to list scheduler active triggers")
}

pub async fn scheduler_due_upper<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
) -> Result<Option<(DateTimeWithTimeZone, String)>> {
    task_trigger::Entity::find()
        .select_only()
        .column(task_trigger::Column::NextFireAt)
        .column(task_trigger::Column::Id)
        .filter(task_trigger::Column::Status.eq("active"))
        .filter(task_trigger::Column::NextFireAt.lte(now))
        .order_by_desc(task_trigger::Column::NextFireAt)
        .order_by_desc(task_trigger::Column::Id)
        .into_tuple::<(DateTimeWithTimeZone, String)>()
        .one(db)
        .await
        .context("failed to capture due trigger upper")
}
pub async fn scheduler_dormant_upper<C: ConnectionTrait>(db: &C) -> Result<Option<String>> {
    task_trigger::Entity::find()
        .select_only()
        .column(task_trigger::Column::Id)
        .filter(task_trigger::Column::Status.eq("active"))
        .filter(task_trigger::Column::NextFireAt.is_null())
        .order_by_desc(task_trigger::Column::Id)
        .into_tuple::<String>()
        .one(db)
        .await
        .context("failed to capture dormant trigger upper")
}
