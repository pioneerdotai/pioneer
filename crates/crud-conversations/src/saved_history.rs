//! Metadata-only keyset scan of the canonical saved conversation.
use anyhow::{Result, ensure};
use chrono::SecondsFormat;
use sea_orm::{
    ConnectionTrait, DatabaseBackend, QueryResult, Statement, entity::prelude::DateTimeWithTimeZone,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedHistoryKey {
    pub created_at: String,
    pub kind: String,
    pub id: String,
}

#[derive(Debug, Clone)]
pub struct SavedHistoryRow {
    pub key: SavedHistoryKey,
    pub turn_id: String,
    pub item_id: Option<String>,
}

/// Pin traversal without holding a reader. Sequences can be allocated before a
/// competing transaction commits, so also keep the original MVCC visibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedHistorySnapshot {
    pub turns: i64,
    pub inputs: i64,
    pub items: i64,
    pub children: i64,
    visibility: String,
}

pub async fn snapshot<C: ConnectionTrait>(db: &C, thread_id: &str) -> Result<SavedHistorySnapshot> {
    let row = db.query_one_raw(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "SELECT COALESCE((SELECT MAX(insertion_ordinal) FROM turn WHERE thread_id=$1),0) AS turns, \
        COALESCE((SELECT MAX(i.insertion_ordinal) FROM turn_input i JOIN turn t ON t.id=i.turn_id WHERE t.thread_id=$1),0) AS inputs, \
        COALESCE((SELECT MAX(i.insertion_ordinal) FROM turn_item i JOIN turn t ON t.id=i.turn_id WHERE t.thread_id=$1),0) AS items, \
        COALESCE((SELECT MAX(insertion_ordinal) FROM thread_lineage WHERE parent_thread_id=$1),0) AS children, \
        pg_current_snapshot()::text AS visibility",
        [thread_id.to_owned().into()])).await?.ok_or_else(|| anyhow::anyhow!("history source unavailable"))?;
    Ok(SavedHistorySnapshot {
        turns: row.try_get("", "turns")?,
        inputs: row.try_get("", "inputs")?,
        items: row.try_get("", "items")?,
        children: row.try_get("", "children")?,
        visibility: row.try_get("", "visibility")?,
    })
}

// No payloads are selected by the scan. Each payload is loaded only after the
// bounded metadata query releases its reader connection.
const SOURCE: &str = r#"
SELECT t.created_at, 'turn'::text COLLATE "C" AS kind, t.id COLLATE "C" AS id, t.id AS turn_id, NULL AS item_id
FROM turn t WHERE t.thread_id = $1 AND t.insertion_ordinal <= $9
AND pg_visible_in_snapshot(t.publication_xid::xid8, $13::text::pg_snapshot)
UNION ALL
SELECT i.created_at, 'input'::text COLLATE "C" AS kind, i.id COLLATE "C" AS id, t.id AS turn_id, NULL AS item_id
FROM turn_input i JOIN turn t ON t.id = i.turn_id
WHERE t.thread_id = $1 AND t.message_deleted_at IS NULL AND t.insertion_ordinal <= $9 AND i.insertion_ordinal <= $10
AND pg_visible_in_snapshot(t.publication_xid::xid8, $13::text::pg_snapshot)
AND pg_visible_in_snapshot(i.publication_xid::xid8, $13::text::pg_snapshot)
UNION ALL
SELECT i.created_at, 'item'::text COLLATE "C" AS kind, i.id COLLATE "C" AS id, t.id AS turn_id, i.item_id
FROM turn_item i JOIN turn t ON t.id = i.turn_id
WHERE t.thread_id = $1 AND t.message_deleted_at IS NULL AND t.insertion_ordinal <= $9 AND i.insertion_ordinal <= $11
AND pg_visible_in_snapshot(t.publication_xid::xid8, $13::text::pg_snapshot)
AND pg_visible_in_snapshot(i.publication_xid::xid8, $13::text::pg_snapshot)
UNION ALL
SELECT l.created_at, 'child'::text COLLATE "C" AS kind, l.child_thread_id COLLATE "C" AS id,
       CASE WHEN l.created_by_thread_id = $1 THEN COALESCE(l.created_by_turn_id, '') ELSE '' END AS turn_id, NULL AS item_id
FROM thread_lineage l JOIN thread p ON p.id = l.parent_thread_id
WHERE l.parent_thread_id = $1 AND l.insertion_ordinal <= $12
AND pg_visible_in_snapshot(l.publication_xid::xid8, $13::text::pg_snapshot)
"#;

pub async fn page<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    boundary: Option<&SavedHistoryKey>,
    before: Option<&SavedHistoryKey>,
    limit: u64,
    snapshot: &SavedHistorySnapshot,
) -> Result<Vec<SavedHistoryRow>> {
    page_direction(
        db, thread_id, boundary, before, false, false, limit, snapshot,
    )
    .await
}

/// The same frozen source and unique key order support both sides of an anchor.
/// Initial context edges and fragment resumes are inclusive; completed-record
/// continuation is exclusive.
pub async fn page_direction<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    boundary: Option<&SavedHistoryKey>,
    position: Option<&SavedHistoryKey>,
    inclusive: bool,
    newer: bool,
    limit: u64,
    snapshot: &SavedHistorySnapshot,
) -> Result<Vec<SavedHistoryRow>> {
    ensure!((1..=101).contains(&limit), "invalid history page limit");
    let comparison = match (newer, inclusive) {
        (true, true) => ">=",
        (true, false) => ">",
        (false, true) => "<=",
        (false, false) => "<",
    };
    let order = if newer { "ASC" } else { "DESC" };
    // SeaQuery has no composite range over an aliased UNION with collated
    // text keys. Keep the fixed operators in Rust and bind all cursor values.
    let sql = format!(
        "SELECT * FROM ({SOURCE}) AS history WHERE ($2::text IS NULL OR (created_at, kind, id) <= ($2::text::timestamptz, $3, $4)) AND ($5::text IS NULL OR (created_at, kind, id) {comparison} ($5::text::timestamptz, $6, $7)) ORDER BY created_at {order}, kind {order}, id {order} LIMIT $8"
    );
    let values = vec![
        thread_id.to_owned().into(),
        boundary.map(|k| k.created_at.clone()).into(),
        boundary.map(|k| k.kind.clone()).into(),
        boundary.map(|k| k.id.clone()).into(),
        position.map(|k| k.created_at.clone()).into(),
        position.map(|k| k.kind.clone()).into(),
        position.map(|k| k.id.clone()).into(),
        i64::try_from(limit)?.into(),
        snapshot.turns.into(),
        snapshot.inputs.into(),
        snapshot.items.into(),
        snapshot.children.into(),
        snapshot.visibility.clone().into(),
    ];
    db.query_all_raw(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        sql,
        values,
    ))
    .await?
    .into_iter()
    .map(decode_row)
    .collect()
}

/// Resolve only an exact canonical saved source owned by this thread. Deleted
/// turns are unavailable, including their turn-level anchors. No payload fetched.
pub async fn anchor<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    item_id: Option<&str>,
    snapshot: &SavedHistorySnapshot,
) -> Result<Option<SavedHistoryRow>> {
    let sql = if item_id.is_some() {
        "SELECT i.created_at, 'item' AS kind, i.id, t.id AS turn_id, i.item_id
        FROM turn_item i JOIN turn t ON t.id=i.turn_id WHERE t.thread_id=$1 AND t.id=$2 AND i.item_id=$3
        AND t.message_deleted_at IS NULL AND t.insertion_ordinal<=$4 AND i.insertion_ordinal<=$5
        AND pg_visible_in_snapshot(t.publication_xid::xid8, $6::text::pg_snapshot)
        AND pg_visible_in_snapshot(i.publication_xid::xid8, $6::text::pg_snapshot)"
    } else {
        "SELECT t.created_at, 'turn' AS kind, t.id, t.id AS turn_id, NULL::text AS item_id
        FROM turn t WHERE t.thread_id=$1 AND t.id=$2 AND $3::text IS NULL
        AND t.message_deleted_at IS NULL AND t.insertion_ordinal<=$4 AND $5::bigint>=0
        AND pg_visible_in_snapshot(t.publication_xid::xid8, $6::text::pg_snapshot)"
    };
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            sql,
            vec![
                thread_id.to_owned().into(),
                turn_id.to_owned().into(),
                item_id.map(str::to_owned).into(),
                snapshot.turns.into(),
                snapshot.items.into(),
                snapshot.visibility.clone().into(),
            ],
        ))
        .await?;
    row.map(decode_row).transpose()
}

fn decode_row(row: QueryResult) -> Result<SavedHistoryRow> {
    let created_at: DateTimeWithTimeZone = row.try_get("", "created_at")?;
    Ok(SavedHistoryRow {
        key: SavedHistoryKey {
            created_at: created_at
                .to_utc()
                .to_rfc3339_opts(SecondsFormat::Micros, true),
            kind: row.try_get("", "kind")?,
            id: row.try_get("", "id")?,
        },
        turn_id: row.try_get("", "turn_id")?,
        item_id: row.try_get("", "item_id")?,
    })
}

/// The receipt covers logical edits and deletions in the pinned set.
pub async fn revision<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    boundary: &SavedHistoryKey,
    snapshot: &SavedHistorySnapshot,
) -> Result<String> {
    let row = db.query_one_raw(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "WITH visible_turns AS (
            SELECT id, created_at, message_revision, message_deleted_at FROM turn
            WHERE thread_id=$1 AND insertion_ordinal<=$3
                AND pg_visible_in_snapshot(publication_xid::xid8, $7::text::pg_snapshot)
        ), items AS (
            SELECT COUNT(*) AS n, COALESCE(SUM(COALESCE(r.revision,1)),0)::bigint AS revisions,
                COALESCE(SUM(r.insertion_ordinal),0)::bigint AS registrations
            FROM turn_item i JOIN visible_turns t ON t.id=i.turn_id
            LEFT JOIN compaction_item_revision r ON r.source_id=i.id AND r.turn_id=i.turn_id
            WHERE i.insertion_ordinal<=$5
                AND pg_visible_in_snapshot(i.publication_xid::xid8, $7::text::pg_snapshot)
        ), inputs AS (
            SELECT COUNT(*) AS n, COALESCE(SUM(COALESCE(r.revision,1)),0)::bigint AS revisions,
                COALESCE(SUM(r.insertion_ordinal),0)::bigint AS registrations
            FROM turn_input i JOIN visible_turns t ON t.id=i.turn_id
            LEFT JOIN compaction_input_revision r ON r.source_id=i.id AND r.turn_id=i.turn_id
            WHERE i.insertion_ordinal<=$4
                AND pg_visible_in_snapshot(i.publication_xid::xid8, $7::text::pg_snapshot)
        ), turns AS (
            SELECT COUNT(*) AS n, COALESCE(SUM(message_revision),0)::bigint AS revisions,
                COUNT(*) FILTER (WHERE message_deleted_at IS NOT NULL) AS deleted
            FROM visible_turns WHERE created_at<=$2::text::timestamptz
        )
        SELECT turns.*, items.n AS items, items.revisions AS item_revisions,
            items.registrations AS item_registrations, inputs.n AS inputs,
            inputs.revisions AS input_revisions, inputs.registrations AS input_registrations,
            (SELECT COUNT(*) FROM thread_lineage l
                WHERE l.parent_thread_id=$1 AND l.created_at<=$2::text::timestamptz
                  AND l.insertion_ordinal<=$6
                  AND pg_visible_in_snapshot(l.publication_xid::xid8, $7::text::pg_snapshot)) AS children
        FROM turns CROSS JOIN items CROSS JOIN inputs",
        vec![thread_id.to_owned().into(), boundary.created_at.clone().into(), snapshot.turns.into(),
            snapshot.inputs.into(), snapshot.items.into(), snapshot.children.into(),
            snapshot.visibility.clone().into()])).await?
        .ok_or_else(|| anyhow::anyhow!("history revision unavailable"))?;
    let parts = [
        "n",
        "revisions",
        "deleted",
        "items",
        "item_revisions",
        "item_registrations",
        "inputs",
        "input_revisions",
        "input_registrations",
        "children",
    ]
    .into_iter()
    .map(|key| row.try_get::<i64>("", key).map(|v| v.to_string()))
    .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(parts.join(":"))
}
