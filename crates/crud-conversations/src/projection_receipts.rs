use anyhow::{Context, Result, ensure};
use pioneer_entity::{
    turn, turn_event as event, turn_event_projection_state as receipt,
    turn_event_projection_stream_state as stream,
};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Func, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, FromQueryResult, QueryFilter,
    QueryOrder, QuerySelect, QueryTrait,
};

pub const RECEIPT_CLEANUP_MAX_ROWS: u64 = 128;
pub const RECEIPT_CLEANUP_MAX_SOURCE_BYTES: i64 = 256 * 1024;

#[derive(Debug, Default)]
pub struct ProjectionReceiptCleanupOutcome {
    pub last_turn_id: Option<String>,
    /// Captured once for an addressed finite pass; late suffixes need a new wake.
    pub through_sequence: Option<i64>,
    pub rows_deleted: u64,
    pub source_bytes: u64,
    pub deferred: bool,
    pub failed: bool,
}

#[derive(Debug, Clone, FromQueryResult)]
pub struct CleanupStream {
    pub turn_id: String,
    thread_id: String,
    status: String,
    projected_through_sequence: i64,
    receipts_compacted_through_sequence: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, FromQueryResult)]
struct CleanupRow {
    event_id: String,
    sequence: i64,
    has_projected_receipt: bool,
    source_bytes: i64,
}

#[derive(Debug)]
pub struct PreparedCleanup {
    stream: CleanupStream,
    rows: Vec<CleanupRow>,
    pub source_bytes: u64,
    pub deferred: bool,
}

impl PreparedCleanup {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

// The partial index contains only streams with potential cleanup work. Fully
// compacted and quarantined history is excluded before the bounded keyset page;
// prepare still validates the receipt prefix of each discovered candidate.
pub async fn next_stream<C: ConnectionTrait>(
    db: &C,
    after_turn_id: Option<&str>,
    through_turn_id: Option<&str>,
) -> Result<Option<CleanupStream>> {
    discovery_query(after_turn_id, through_turn_id, false)
        .into_model::<CleanupStream>()
        .one(db)
        .await
        .context("failed to discover receipt cleanup stream")
}

pub async fn upper_bound<C: ConnectionTrait>(db: &C) -> Result<Option<String>> {
    discovery_query(None, None, true)
        .into_model::<CleanupStream>()
        .one(db)
        .await
        .map(|row| row.map(|row| row.turn_id))
        .context("failed to read receipt cleanup boundary")
}

pub fn discovery_query(
    after_turn_id: Option<&str>,
    through_turn_id: Option<&str>,
    reverse: bool,
) -> sea_orm::Select<stream::Entity> {
    let owner = Query::select()
        .expr(Expr::val(1))
        .from(turn::Entity)
        .and_where(
            Expr::col((turn::Entity, turn::Column::Id))
                .equals((stream::Entity, stream::Column::TurnId)),
        )
        .and_where(Expr::col((turn::Entity, turn::Column::Status)).is_in([
            "completed",
            "failed",
            "interrupted",
        ]))
        .to_owned();
    // Literal status and nonnegative floor match the partial work index,
    // including when PostgreSQL switches to a generic prepared plan.
    let mut query = stream_query()
        .filter(Expr::col(stream::Column::Status).eq(Expr::Constant("healthy".into())))
        .filter(
            Expr::col(stream::Column::ReceiptsCompactedThroughSequence)
                .gte(Expr::Constant(0_i64.into())),
        )
        .filter(
            Expr::col(stream::Column::ProjectedThroughSequence)
                .gt(Expr::col(stream::Column::ReceiptsCompactedThroughSequence)),
        )
        .filter(Expr::exists(owner));
    if let Some(after) = after_turn_id {
        query = query.filter(stream::Column::TurnId.gt(after));
    }
    if let Some(through) = through_turn_id {
        query = query.filter(stream::Column::TurnId.lte(through));
    }
    let order = if reverse {
        sea_orm::Order::Desc
    } else {
        sea_orm::Order::Asc
    };
    query.order_by(stream::Column::TurnId, order).limit(1)
}

fn stream_query() -> sea_orm::Select<stream::Entity> {
    stream::Entity::find().select_only().columns([
        stream::Column::TurnId,
        stream::Column::ThreadId,
        stream::Column::Status,
        stream::Column::ProjectedThroughSequence,
        stream::Column::ReceiptsCompactedThroughSequence,
    ])
}

pub async fn addressed_stream<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    through: Option<i64>,
) -> Result<Option<CleanupStream>> {
    let terminal = turn::Entity::find()
        .select_only()
        .column(turn::Column::Id)
        .filter(turn::Column::Id.eq(turn_id))
        .filter(turn::Column::Status.is_in(["completed", "failed", "interrupted"]))
        .into_query();
    let current = stream_query()
        .filter(stream::Column::TurnId.eq(turn_id))
        .filter(stream::Column::TurnId.in_subquery(terminal))
        .filter(stream::Column::Status.eq("healthy"))
        .into_model::<CleanupStream>()
        .one(db)
        .await?;
    Ok(current.and_then(|mut row| {
        if let Some(through) = through {
            row.projected_through_sequence = std::cmp::min(row.projected_through_sequence, through);
        }
        (row.receipts_compacted_through_sequence >= 0
            && row.projected_through_sequence > row.receipts_compacted_through_sequence)
            .then_some(row)
    }))
}

impl CleanupStream {
    pub fn through_sequence(&self) -> i64 {
        self.projected_through_sequence
    }
}

async fn load_rows<C: ConnectionTrait>(
    db: &C,
    stream: &CleanupStream,
    through: i64,
) -> Result<Vec<CleanupRow>> {
    // Only canonical keys and receipt lengths are projected.
    let event_col = |column| Expr::col((event::Entity, column));
    let receipt_col = |column| Expr::col((receipt::Entity, column));
    let byte_length = |column| -> Expr {
        Func::coalesce([
            Func::cust(Alias::new("octet_length"))
                .arg(receipt_col(column))
                .into(),
            Expr::val(0),
        ])
        .into()
    };
    let projected = receipt_col(receipt::Column::Status)
        .eq("projected")
        .and(receipt_col(receipt::Column::TurnId).eq(event_col(event::Column::TurnId)))
        .and(receipt_col(receipt::Column::ThreadId).eq(&stream.thread_id))
        .and(receipt_col(receipt::Column::ThreadId).eq(event_col(event::Column::ThreadId)))
        .and(receipt_col(receipt::Column::Sequence).eq(event_col(event::Column::Sequence)));
    let query = Query::select()
        .expr_as(event_col(event::Column::Id), Alias::new("event_id"))
        .column((event::Entity, event::Column::Sequence))
        .expr_as(
            Func::coalesce([projected, Expr::val(false)]),
            Alias::new("has_projected_receipt"),
        )
        .expr_as(
            byte_length(receipt::Column::ProjectionContextJson)
                .add(byte_length(receipt::Column::LastError))
                .add(512)
                .cast_as(Alias::new("bigint")),
            Alias::new("source_bytes"),
        )
        .from(event::Entity)
        .left_join(
            receipt::Entity,
            receipt_col(receipt::Column::EventId).eq(event_col(event::Column::Id)),
        )
        .and_where(event_col(event::Column::TurnId).eq(&stream.turn_id))
        .and_where(
            event_col(event::Column::Sequence).gt(stream.receipts_compacted_through_sequence),
        )
        .and_where(event_col(event::Column::Sequence).lte(through))
        .order_by(
            (event::Entity, event::Column::Sequence),
            sea_orm::Order::Asc,
        )
        .limit(RECEIPT_CLEANUP_MAX_ROWS)
        .to_owned();
    db.query_all(&query)
        .await
        .context("failed to read bounded receipt cleanup prefix")?
        .iter()
        .map(|row| CleanupRow::from_query_result(row, "").map_err(Into::into))
        .collect()
}

pub async fn prepare<C: ConnectionTrait>(db: &C, stream: CleanupStream) -> Result<PreparedCleanup> {
    let mut prepared = PreparedCleanup {
        stream,
        rows: Vec::new(),
        source_bytes: 0,
        deferred: false,
    };
    let floor = prepared.stream.receipts_compacted_through_sequence;
    let watermark = prepared.stream.projected_through_sequence;
    if prepared.stream.status != "healthy" || floor < 0 || watermark < floor {
        prepared.deferred = true;
        return Ok(prepared);
    }
    if watermark == floor {
        return Ok(prepared);
    }
    let rows = load_rows(db, &prepared.stream, watermark).await?;
    if rows.is_empty() {
        prepared.deferred = true;
    }
    for row in rows {
        let expected = floor + prepared.rows.len() as i64 + 1;
        if row.sequence != expected
            || !row.has_projected_receipt
            || row.source_bytes < 0
            || row.source_bytes as u64 + prepared.source_bytes
                > RECEIPT_CLEANUP_MAX_SOURCE_BYTES as u64
        {
            // A gap, malformed receipt or oversized row stops this stream,
            // not the keyset scan of other streams.
            prepared.deferred = true;
            break;
        }
        prepared.source_bytes += row.source_bytes as u64;
        prepared.rows.push(row);
    }
    Ok(prepared)
}

pub async fn apply(db: &DatabaseTransaction, prepared: &PreparedCleanup) -> Result<u64> {
    let Some(last) = prepared.rows.last() else {
        return Ok(0);
    };
    crate::turn_event::lock_stream(db, &prepared.stream.turn_id).await?;
    // Advisory discovery can race a resume. Check authoritative terminal state
    // again under the stream lock before deletion and boundary advance.
    if turn::Entity::find_by_id(prepared.stream.turn_id.clone())
        .select_only()
        .column(turn::Column::Id)
        .filter(turn::Column::Status.is_in(["completed", "failed", "interrupted"]))
        .lock_exclusive()
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_none()
    {
        return Ok(0);
    }
    let current = stream_query()
        .filter(stream::Column::TurnId.eq(prepared.stream.turn_id.clone()))
        .lock_exclusive()
        .into_model::<CleanupStream>()
        .one(db)
        .await
        .context("failed to revalidate receipt cleanup stream")?;
    let Some(current) = current else {
        return Ok(0);
    };
    if current.status != "healthy"
        || current.thread_id != prepared.stream.thread_id
        || current.receipts_compacted_through_sequence
            != prepared.stream.receipts_compacted_through_sequence
        || current.projected_through_sequence < last.sequence
    {
        return Ok(0);
    }

    // Revalidate the bounded prefix while holding the stream locks.
    // A changed status, owner, size or canonical key invalidates the plan.
    if load_rows(db, &prepared.stream, last.sequence).await? != prepared.rows {
        return Ok(0);
    }
    let deleted = receipt::Entity::delete_many()
        .filter(
            receipt::Column::EventId.is_in(prepared.rows.iter().map(|row| row.event_id.clone())),
        )
        .filter(receipt::Column::TurnId.eq(current.turn_id.clone()))
        .filter(receipt::Column::ThreadId.eq(current.thread_id))
        .filter(receipt::Column::Status.eq("projected"))
        .filter(receipt::Column::Sequence.gt(current.receipts_compacted_through_sequence))
        .filter(receipt::Column::Sequence.lte(last.sequence))
        .filter(receipt::Column::Sequence.lte(current.projected_through_sequence))
        .exec(db)
        .await
        .context("failed to delete bounded projection receipts")?
        .rows_affected;
    ensure!(
        deleted == prepared.rows.len() as u64,
        "projection receipt cleanup prefix changed during deletion"
    );
    let advanced = stream::Entity::update_many()
        .col_expr(
            stream::Column::ReceiptsCompactedThroughSequence,
            Expr::value(last.sequence),
        )
        .filter(stream::Column::TurnId.eq(current.turn_id))
        .filter(
            stream::Column::ReceiptsCompactedThroughSequence
                .eq(current.receipts_compacted_through_sequence),
        )
        .filter(stream::Column::ProjectedThroughSequence.gte(last.sequence))
        .exec(db)
        .await
        .context("failed to persist receipt compaction boundary")?
        .rows_affected;
    ensure!(
        advanced == 1,
        "projection receipt compaction boundary did not advance atomically"
    );
    Ok(deleted)
}
