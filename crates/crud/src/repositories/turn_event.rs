use anyhow::{Context, Result};
use pioneer_entity::turn_event;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Query};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};

use crate::events::{AppendedTurnEvent, TurnEventPayload};
use pioneer_protocol::generate_id;

const DB_ID_LEN: usize = 21;

pub async fn append_event<C: ConnectionTrait>(
    db: &C,
    payload: &TurnEventPayload,
    created_at: DateTimeWithTimeZone,
) -> Result<AppendedTurnEvent> {
    let thread_id = payload.thread_id().to_owned();
    let turn_id = payload.turn_id().to_owned();

    let event_type = payload.event_type().to_owned();

    let payload_json =
        serde_json::to_string(payload).context("failed to serialize turn event payload")?;

    let sequence = next_sequence_for_turn(db, turn_id.as_str()).await?;

    let id = generate_id(DB_ID_LEN);

    let mut insert = Query::insert();

    insert
        .into_table(Alias::new("turn_event"))
        .columns([
            Alias::new("id"),
            Alias::new("thread_id"),
            Alias::new("turn_id"),
            Alias::new("sequence"),
            Alias::new("event_type"),
            Alias::new("payload"),
            Alias::new("created_at"),
        ])
        .values_panic([
            id.clone().into(),
            thread_id.clone().into(),
            turn_id.clone().into(),
            sequence.into(),
            event_type.clone().into(),
            payload_json.into(),
            created_at.into(),
        ]);

    db.execute(&insert)
        .await
        .context("failed to append turn event")?;

    Ok(AppendedTurnEvent {
        id,
        thread_id,
        turn_id,
        sequence,
        payload: payload.clone(),
        created_at,
    })
}

pub fn appended_event_from_model(model: turn_event::Model) -> Result<AppendedTurnEvent> {
    let payload: TurnEventPayload = serde_json::from_str(model.payload.as_str())
        .with_context(|| format!("failed to deserialize turn_event `{}` payload", model.id))?;

    Ok(AppendedTurnEvent {
        id: model.id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        sequence: model.sequence,
        payload,
        created_at: model.created_at,
    })
}

pub async fn find_event_by_id<C: ConnectionTrait>(
    db: &C,
    event_id: &str,
) -> Result<Option<AppendedTurnEvent>> {
    turn_event::Entity::find_by_id(event_id.to_owned())
        .one(db)
        .await
        .context("failed to query turn_event by id")?
        .map(appended_event_from_model)
        .transpose()
}

pub async fn latest_event_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<AppendedTurnEvent>> {
    turn_event::Entity::find()
        .filter(turn_event::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_desc(turn_event::Column::Sequence)
        .one(db)
        .await
        .context("failed to query latest turn_event")?
        .map(appended_event_from_model)
        .transpose()
}

async fn next_sequence_for_turn<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<i64> {
    let max_sequence = db
        .query_one(
            &Query::select()
                .expr_as(
                    Expr::cust("COALESCE(MAX(sequence), 0)"),
                    Alias::new("max_sequence"),
                )
                .from(Alias::new("turn_event"))
                .and_where(Expr::col(Alias::new("turn_id")).eq(turn_id.to_owned()))
                .to_owned(),
        )
        .await
        .context("failed to query max turn_event sequence")?
        .and_then(|row| {
            row.try_get::<Option<i64>>("", "max_sequence")
                .ok()
                .flatten()
        })
        .unwrap_or(0);

    Ok(max_sequence + 1)
}

pub async fn list_events_for_turn<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Vec<turn_event::Model>> {
    turn_event::Entity::find()
        .filter(turn_event::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn_event::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_asc(turn_event::Column::Sequence)
        .all(db)
        .await
        .context("failed to query turn events")
}

pub async fn list_events_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    limit: Option<u64>,
) -> Result<Vec<turn_event::Model>> {
    let mut query = turn_event::Entity::find()
        .filter(turn_event::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_desc(turn_event::Column::CreatedAt)
        .order_by_desc(turn_event::Column::TurnId)
        .order_by_desc(turn_event::Column::Sequence);

    if let Some(limit) = limit {
        query = query.limit(limit);
    }

    query
        .all(db)
        .await
        .map(|mut rows| {
            rows.reverse();
            rows
        })
        .context("failed to query thread history events")
}
