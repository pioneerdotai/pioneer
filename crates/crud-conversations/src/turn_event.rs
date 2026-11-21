use anyhow::{Context, Result};
use pioneer_crud_payloads::storage::PreparedPayload;
use pioneer_entity::turn_event as event;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, DbBackend, EntityTrait, QueryFilter,
    QuerySelect, Set, Statement, entity::prelude::DateTimeWithTimeZone,
};

const EVENT_STREAM_LOCK_SEED: i64 = 0x7069_6f6e_7465_7674;

/// Admission precedes the first event, when the turn row may not exist yet.
pub async fn lock_stream(tx: &DatabaseTransaction, turn_id: &str) -> Result<()> {
    // PostgreSQL advisory transaction locks are exposed as a query function.
    tx.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT pg_advisory_xact_lock(hashtextextended($1, $2))",
        [turn_id.into(), EVENT_STREAM_LOCK_SEED.into()],
    ))
    .await
    .context("lock canonical turn event stream")?;
    Ok(())
}

pub async fn max_sequence<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<i64> {
    Ok(event::Entity::find()
        .select_only()
        .column_as(event::Column::Sequence.max(), "last")
        .filter(event::Column::TurnId.eq(turn_id))
        .into_tuple::<Option<i64>>()
        .one(db)
        .await
        .context("failed to query max turn_event sequence")?
        .flatten()
        .unwrap_or(0))
}

pub async fn next_sequence(tx: &DatabaseTransaction, turn_id: &str) -> Result<i64> {
    max_sequence(tx, turn_id)
        .await?
        .checked_add(1)
        .context("turn event sequence overflow")
}

pub struct NewEvent<'a> {
    pub id: &'a str,
    pub thread_id: &'a str,
    pub turn_id: &'a str,
    pub sequence: i64,
    pub event_type: &'a str,
    pub idempotency_key: &'a str,
    pub created_at: DateTimeWithTimeZone,
}

/// Insert after acquiring the stream lock and checking idempotency.
pub async fn insert(
    tx: &DatabaseTransaction,
    event: NewEvent<'_>,
    body: &PreparedPayload,
) -> Result<()> {
    body.insert(tx).await?;
    event::Entity::insert(event::ActiveModel {
        id: Set(event.id.to_owned()),
        thread_id: Set(event.thread_id.to_owned()),
        turn_id: Set(event.turn_id.to_owned()),
        sequence: Set(event.sequence),
        event_type: Set(event.event_type.to_owned()),
        idempotency_key: Set(Some(event.idempotency_key.to_owned())),
        payload: Set(None),
        payload_payload_id: Set(Some(body.id().to_owned())),
        created_at: Set(event.created_at),
        ..Default::default()
    })
    .exec_without_returning(tx)
    .await
    .context("append canonical turn event")?;
    Ok(())
}
