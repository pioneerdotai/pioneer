use anyhow::{Context, Result, ensure};
use pioneer_crud_payloads::Frame;
use pioneer_entity::task_event;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

// PostgreSQL limits a variable-length value to 1 GiB. Use the same upper
// limit for the uncompressed representation as for a raw database value.
const MAX_EVENT_BYTES: usize = (1 << 30) - 1;

#[derive(Clone)]
pub struct EventPayload(Frame);

impl std::fmt::Debug for EventPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventPayload")
            .field("encoding", &self.0.encoding)
            .field("original_bytes", &self.0.original_bytes)
            .finish_non_exhaustive()
    }
}

impl EventPayload {
    pub fn prepare(json: &str) -> Result<Self> {
        ensure!(
            json.len() <= MAX_EVENT_BYTES,
            "task event exceeds its byte limit"
        );
        Frame::encode(json).map(Self)
    }

    pub fn read(row: &mut task_event::Model) -> Result<String> {
        Frame {
            encoding: row.payload_json_codec.try_into()?,
            bytes: std::mem::take(&mut row.payload_json),
            original_bytes: row.payload_json_bytes.try_into()?,
            original_chars: row.payload_json_chars.try_into()?,
            sha256: row.payload_json_sha256.as_slice().try_into()?,
        }
        .decode(MAX_EVENT_BYTES)
        .context("decode task event payload")
    }
}

/// The event log can precede creation of its task projection in the same
/// transaction. A row lock cannot protect the empty stream. This transaction
/// advisory lock serializes sequence and idempotency checks for one task.
pub async fn lock_task<C: ConnectionTrait>(transaction: &C, task_id: &str) -> Result<()> {
    let query = Query::select()
        .expr(Expr::cust_with_values(
            "pg_advisory_xact_lock(hashtextextended($1, $2))",
            [
                task_id.to_owned().into(),
                sea_orm::Value::from(848012464_i64),
            ],
        ))
        .to_owned();
    transaction.query_one(&query).await?;
    Ok(())
}

pub async fn next_sequence<C: ConnectionTrait>(transaction: &C, task_id: &str) -> Result<i64> {
    let latest = task_event::Entity::find()
        .select_only()
        .column(task_event::Column::Sequence)
        .filter(task_event::Column::TaskId.eq(task_id))
        .order_by_desc(task_event::Column::Sequence)
        .into_tuple::<i64>()
        .one(transaction)
        .await?
        .unwrap_or_default();
    latest
        .checked_add(1)
        .context("task event sequence exhausted")
}

pub struct NewEvent<'a> {
    pub id: &'a str,
    pub task_id: &'a str,
    pub run_id: Option<&'a str>,
    pub thread_id: Option<&'a str>,
    pub turn_id: Option<&'a str>,
    pub event_type: &'a str,
    pub idempotency_key: Option<&'a str>,
    pub sequence: i64,
    pub created_at: DateTimeWithTimeZone,
}

/// Append inside the transaction after acquiring the stream lock. The payload
/// is already encoded, so allocating a sequence never holds heavy CPU work.
pub async fn append<C: ConnectionTrait>(
    transaction: &C,
    event: NewEvent<'_>,
    payload: EventPayload,
) -> Result<()> {
    let frame = payload.0;
    task_event::Entity::insert(task_event::ActiveModel {
        id: Set(event.id.to_owned()),
        task_id: Set(event.task_id.to_owned()),
        run_id: Set(event.run_id.map(str::to_owned)),
        thread_id: Set(event.thread_id.map(str::to_owned)),
        turn_id: Set(event.turn_id.map(str::to_owned)),
        event_type: Set(event.event_type.to_owned()),
        idempotency_key: Set(event.idempotency_key.map(str::to_owned)),
        sequence: Set(event.sequence),
        created_at: Set(event.created_at),
        payload_json: Set(frame.bytes),
        payload_json_codec: Set(frame.encoding as i16),
        payload_json_bytes: Set(frame.original_bytes.try_into()?),
        payload_json_chars: Set(frame.original_chars.try_into()?),
        payload_json_sha256: Set(frame.sha256.to_vec()),
    })
    .exec_without_returning(transaction)
    .await
    .context("append task event")?;
    Ok(())
}
