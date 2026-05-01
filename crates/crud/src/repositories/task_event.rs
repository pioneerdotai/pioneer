#![allow(dead_code)]

use anyhow::{Context, Result};
use pioneer_entity::task_event;
use pioneer_protocol::{TaskEvent, generate_id};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Query};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder};

use crate::task_events::{AppendedTaskEvent, TaskEventPayload};

const DB_ID_LEN: usize = 21;

pub async fn append_event<C: ConnectionTrait>(
    db: &C,
    payload: &TaskEventPayload,
    created_at: DateTimeWithTimeZone,
) -> Result<AppendedTaskEvent> {
    let task_id = payload.task_id().to_owned();
    let run_id = payload.run_id().map(str::to_owned);
    let thread_id = payload.thread_id().map(str::to_owned);
    let turn_id = payload.turn_id().map(str::to_owned);
    let event_type = payload.event_type().to_owned();
    let payload_json =
        serde_json::to_string(payload).context("failed to serialize task event payload")?;
    let sequence = next_sequence_for_task(db, task_id.as_str()).await?;
    let id = generate_id(DB_ID_LEN);

    let mut insert = Query::insert();
    insert
        .into_table(Alias::new("task_event"))
        .columns([
            Alias::new("id"),
            Alias::new("task_id"),
            Alias::new("run_id"),
            Alias::new("thread_id"),
            Alias::new("turn_id"),
            Alias::new("sequence"),
            Alias::new("event_type"),
            Alias::new("payload_json"),
            Alias::new("created_at"),
        ])
        .values_panic([
            id.clone().into(),
            task_id.clone().into(),
            run_id.clone().into(),
            thread_id.clone().into(),
            turn_id.clone().into(),
            sequence.into(),
            event_type.clone().into(),
            payload_json.into(),
            created_at.into(),
        ]);

    db.execute(&insert)
        .await
        .context("failed to append task event")?;

    Ok(AppendedTaskEvent {
        id,
        task_id,
        run_id,
        thread_id,
        turn_id,
        sequence,
        event_type,
        payload: payload.clone(),
        workspace_id: None,
        root_task_id: None,
        parent_task_id: None,
        created_at,
    })
}

pub async fn list_events_for_task<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    after_sequence: Option<i64>,
) -> Result<Vec<task_event::Model>> {
    let mut query = task_event::Entity::find()
        .filter(task_event::Column::TaskId.eq(task_id.to_owned()))
        .order_by_asc(task_event::Column::Sequence);

    if let Some(after_sequence) = after_sequence {
        query = query.filter(task_event::Column::Sequence.gt(after_sequence));
    }

    query.all(db).await.context("failed to query task events")
}

pub async fn list_events_for_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<Vec<task_event::Model>> {
    task_event::Entity::find()
        .filter(task_event::Column::RunId.eq(run_id.to_owned()))
        .order_by_asc(task_event::Column::Sequence)
        .all(db)
        .await
        .context("failed to query task events for run")
}

pub async fn list_events_for_thread_turn<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: Option<&str>,
) -> Result<Vec<task_event::Model>> {
    let mut query = task_event::Entity::find()
        .filter(task_event::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_asc(task_event::Column::CreatedAt)
        .order_by_asc(task_event::Column::Sequence);

    if let Some(turn_id) = turn_id {
        query = query.filter(task_event::Column::TurnId.eq(turn_id.to_owned()));
    }

    query
        .all(db)
        .await
        .context("failed to query task events for thread/turn")
}

pub fn task_event_from_model(model: task_event::Model) -> Result<TaskEvent> {
    let payload: TaskEventPayload = serde_json::from_str(model.payload_json.as_str())
        .context("failed to decode task_event payload_json")?;
    Ok(TaskEvent {
        id: model.id,
        task_id: model.task_id,
        run_id: model.run_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        sequence: model.sequence,
        event_type: model.event_type,
        payload,
        created_at: model.created_at.timestamp(),
    })
}

async fn next_sequence_for_task<C: ConnectionTrait>(db: &C, task_id: &str) -> Result<i64> {
    let max_sequence = db
        .query_one(
            &Query::select()
                .expr_as(
                    Expr::cust("COALESCE(MAX(sequence), 0)"),
                    Alias::new("max_sequence"),
                )
                .from(Alias::new("task_event"))
                .and_where(Expr::col(Alias::new("task_id")).eq(task_id.to_owned()))
                .to_owned(),
        )
        .await
        .context("failed to query max task_event sequence")?
        .and_then(|row| {
            row.try_get::<Option<i64>>("", "max_sequence")
                .ok()
                .flatten()
        })
        .unwrap_or(0);

    Ok(max_sequence + 1)
}
