use anyhow::{Context, Result, ensure};
use pioneer_entity::{
    turn_event_projection_state as receipt, turn_event_projection_stream_state as stream,
};
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, FromQueryResult, QueryFilter,
    QueryOrder, QuerySelect, Statement,
};

#[cfg(test)]
#[path = "projection_receipt_cleanup_tests.rs"]
mod tests;

pub const RECEIPT_CLEANUP_MAX_ROWS: u64 = 128;
pub const RECEIPT_CLEANUP_MAX_SOURCE_BYTES: i64 = 256 * 1024;

#[derive(Debug, Default)]
pub struct ProjectionReceiptCleanupOutcome {
    pub backfill_ready: bool,
    pub last_turn_id: Option<String>,
    pub rows_deleted: u64,
    pub source_bytes: u64,
    pub deferred: bool,
    pub failed: bool,
}

#[derive(Debug, Clone, FromQueryResult)]
pub(crate) struct CleanupStream {
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
pub(crate) struct PreparedCleanup {
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

pub(crate) async fn backfill_ready<C: ConnectionTrait>(db: &C) -> Result<bool> {
    let meta =
        crate::find_projection_meta(db, "turn_event_projection_stream_state_backfill").await?;
    Ok(meta.is_some_and(|meta| {
        meta.projection_version >= 3
            && meta.status == crate::PROJECTION_META_STATUS_COMPLETE
            && meta.last_error.is_none()
    }))
}

// Do not filter by eligibility before LIMIT: even a database full of protected
// streams must be scanned in bounded keyset quanta, without reserving a writer.
pub(crate) async fn next_stream<C: ConnectionTrait>(
    db: &C,
    after_turn_id: Option<&str>,
) -> Result<Option<CleanupStream>> {
    let mut query = stream_query();
    if let Some(after) = after_turn_id {
        query = query.filter(stream::Column::TurnId.gt(after.to_owned()));
    }
    query
        .order_by_asc(stream::Column::TurnId)
        .limit(1)
        .into_model::<CleanupStream>()
        .one(db)
        .await
        .context("failed to discover receipt cleanup stream")
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

async fn load_rows<C: ConnectionTrait>(
    db: &C,
    stream: &CleanupStream,
    through: i64,
) -> Result<Vec<CleanupRow>> {
    // Only canonical keys and receipt lengths are read. No JSON decoding or
    // decompression is needed, including when turn_event is a Zstd view.
    CleanupRow::find_by_statement(Statement::from_sql_and_values(
        db.get_database_backend(),
        r#"SELECT event.id AS event_id, event.sequence,
            COALESCE(projection.status = 'projected'
                AND projection.turn_id = event.turn_id
                AND projection.thread_id = ?
                AND projection.thread_id = event.thread_id
                AND projection.sequence = event.sequence, 0) AS has_projected_receipt,
            COALESCE(length(CAST(projection.projection_context_json AS BLOB)), 0)
                + COALESCE(length(CAST(projection.last_error AS BLOB)), 0) + 512 AS source_bytes
        FROM turn_event AS event
        LEFT JOIN turn_event_projection_state AS projection ON projection.event_id = event.id
        WHERE event.turn_id = ?
            AND event.sequence > ? AND event.sequence <= ?
        ORDER BY event.sequence LIMIT ?"#,
        [
            stream.thread_id.clone().into(),
            stream.turn_id.clone().into(),
            stream.receipts_compacted_through_sequence.into(),
            through.into(),
            RECEIPT_CLEANUP_MAX_ROWS.into(),
        ],
    ))
    .all(db)
    .await
    .context("failed to read bounded receipt cleanup prefix")
}

pub(crate) async fn prepare<C: ConnectionTrait>(
    db: &C,
    stream: CleanupStream,
) -> Result<PreparedCleanup> {
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

pub(crate) async fn apply(db: &DatabaseTransaction, prepared: &PreparedCleanup) -> Result<u64> {
    let Some(last) = prepared.rows.last() else {
        return Ok(0);
    };
    if !backfill_ready(db).await? {
        return Ok(0);
    }
    let current = stream_query()
        .filter(stream::Column::TurnId.eq(prepared.stream.turn_id.clone()))
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

    // Revalidate the bounded prefix after obtaining the serialized writer.
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
