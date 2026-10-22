//! Metadata-only keyset scan of the canonical saved conversation.
use anyhow::{Result, ensure};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
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

/// Per-source insertion watermarks pin traversal without holding a reader.
/// Inputs/items use the backing table's stable rowid, never a decompression
/// view's nonexistent rowid. sqlite-zstd renames storage without copying rows.
/// These values stay inside the encrypted version-2 tool cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedHistorySnapshot {
    pub turns: i64,
    pub inputs: i64,
    pub items: i64,
    pub children: i64,
}

// Same logical/backing-table distinction as the canonical storage migrations.
// Only these fixed names can reach SQL identifiers. Discovery releases its reader
// before the metadata query; a concurrent schema transition may fail that call
// safely, and the next call resolves the new backing table.
async fn storage_tables<C: ConnectionTrait>(db: &C) -> Result<(&'static str, &'static str)> {
    let rows = db.query_all_raw(Statement::from_string(DatabaseBackend::Sqlite,
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('turn_input','_turn_input_zstd','turn_item','_turn_item_zstd') LIMIT 4".to_owned())).await?;
    let names = rows
        .into_iter()
        .map(|r| r.try_get::<String>("", "name"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let input = if names.iter().any(|n| n == "_turn_input_zstd") {
        "_turn_input_zstd"
    } else {
        ensure!(
            names.iter().any(|n| n == "turn_input"),
            "history source unavailable"
        );
        "turn_input"
    };
    let item = if names.iter().any(|n| n == "_turn_item_zstd") {
        "_turn_item_zstd"
    } else {
        ensure!(
            names.iter().any(|n| n == "turn_item"),
            "history source unavailable"
        );
        "turn_item"
    };
    Ok((input, item))
}

pub async fn snapshot<C: ConnectionTrait>(db: &C, thread_id: &str) -> Result<SavedHistorySnapshot> {
    let (inputs, items) = storage_tables(db).await?;
    let row = db.query_one_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        format!("SELECT COALESCE((SELECT MAX(rowid) FROM turn WHERE thread_id=?1),0) AS turns, \
        COALESCE((SELECT MAX(i.rowid) FROM {inputs} i JOIN turn t ON t.id=i.turn_id WHERE t.thread_id=?1),0) AS inputs, \
        COALESCE((SELECT MAX(i.rowid) FROM {items} i JOIN turn t ON t.id=i.turn_id WHERE t.thread_id=?1),0) AS items, \
        COALESCE((SELECT MAX(rowid) FROM thread_lineage WHERE parent_thread_id=?1),0) AS children"),
        [thread_id.to_owned().into()])).await?.ok_or_else(|| anyhow::anyhow!("history source unavailable"))?;
    Ok(SavedHistorySnapshot {
        turns: row.try_get("", "turns")?,
        inputs: row.try_get("", "inputs")?,
        items: row.try_get("", "items")?,
        children: row.try_get("", "children")?,
    })
}

// No payloads are selected by the scan. Each payload is loaded only after the
// bounded metadata query releases its reader connection.
const SOURCE: &str = r#"
SELECT t.created_at, 'turn' AS kind, t.id AS id, t.id AS turn_id, NULL AS item_id
FROM turn t WHERE t.thread_id = ?1 AND t.rowid <= ?9
UNION ALL
SELECT i.created_at, 'input' AS kind, i.id AS id, t.id AS turn_id, NULL AS item_id
FROM __INPUT_STORAGE__ i JOIN turn t ON t.id = i.turn_id
WHERE t.thread_id = ?1 AND t.message_deleted_at IS NULL AND i.rowid <= ?10
UNION ALL
SELECT i.created_at, 'item' AS kind, i.id AS id, t.id AS turn_id, i.item_id
FROM __ITEM_STORAGE__ i JOIN turn t ON t.id = i.turn_id
WHERE t.thread_id = ?1 AND t.message_deleted_at IS NULL AND i.rowid <= ?11
UNION ALL
SELECT l.created_at, 'child' AS kind, l.child_thread_id AS id,
       CASE WHEN l.created_by_thread_id = ?1 THEN COALESCE(l.created_by_turn_id, '') ELSE '' END AS turn_id, NULL AS item_id
FROM thread_lineage l JOIN thread p ON p.id = l.parent_thread_id
WHERE l.parent_thread_id = ?1 AND l.rowid <= ?12
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
    let (inputs, items) = storage_tables(db).await?;
    let source = SOURCE
        .replace("__INPUT_STORAGE__", inputs)
        .replace("__ITEM_STORAGE__", items);
    let comparison = match (newer, inclusive) {
        (true, true) => ">=",
        (true, false) => ">",
        (false, true) => "<=",
        (false, false) => "<",
    };
    let order = if newer { "ASC" } else { "DESC" };
    // Operators and order are server-owned literals, never cursor SQL.
    let sql = format!(
        "SELECT * FROM ({source}) WHERE (?2 IS NULL OR (created_at, kind, id) <= (?2, ?3, ?4)) AND (?5 IS NULL OR (created_at, kind, id) {comparison} (?5, ?6, ?7)) ORDER BY created_at {order}, kind {order}, id {order} LIMIT ?8"
    );
    let values = vec![
        thread_id.to_owned().into(),
        boundary.map(|k| k.created_at.clone()).into(),
        boundary.map(|k| k.kind.clone()).into(),
        boundary.map(|k| k.id.clone()).into(),
        position.map(|k| k.created_at.clone()).into(),
        position.map(|k| k.kind.clone()).into(),
        position.map(|k| k.id.clone()).into(),
        limit.into(),
        snapshot.turns.into(),
        snapshot.inputs.into(),
        snapshot.items.into(),
        snapshot.children.into(),
    ];
    db.query_all_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        sql,
        values,
    ))
    .await?
    .into_iter()
    .map(|row| {
        Ok(SavedHistoryRow {
            key: SavedHistoryKey {
                created_at: row.try_get("", "created_at")?,
                kind: row.try_get("", "kind")?,
                id: row.try_get("", "id")?,
            },
            turn_id: row.try_get("", "turn_id")?,
            item_id: row.try_get("", "item_id")?,
        })
    })
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
    let (_, items) = storage_tables(db).await?;
    let sql = if item_id.is_some() {
        format!(
            "SELECT i.created_at, 'item' AS kind, i.id, t.id AS turn_id, i.item_id \
        FROM {items} i JOIN turn t ON t.id=i.turn_id WHERE t.thread_id=?1 AND t.id=?2 AND i.item_id=?3 \
        AND t.message_deleted_at IS NULL AND t.rowid<=?4 AND i.rowid<=?5 LIMIT 1"
        )
    } else {
        "SELECT t.created_at, 'turn' AS kind, t.id, t.id AS turn_id, NULL AS item_id \
        FROM turn t WHERE t.thread_id=?1 AND t.id=?2 AND ?3 IS NULL \
        AND t.message_deleted_at IS NULL AND t.rowid<=?4 AND ?5>=0 LIMIT 1"
            .to_owned()
    };
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            sql,
            vec![
                thread_id.to_owned().into(),
                turn_id.to_owned().into(),
                item_id.map(str::to_owned).into(),
                snapshot.turns.into(),
                snapshot.items.into(),
            ],
        ))
        .await?;
    row.map(|row| {
        Ok(SavedHistoryRow {
            key: SavedHistoryKey {
                created_at: row.try_get("", "created_at")?,
                kind: row.try_get("", "kind")?,
                id: row.try_get("", "id")?,
            },
            turn_id: row.try_get("", "turn_id")?,
            item_id: row.try_get("", "item_id")?,
        })
    })
    .transpose()
}

/// Logical edit/delete checks cover the frozen physical set, including legacy
/// rows without revision metadata. Registering legacy metadata may make a cursor
/// stale; physical compression changes neither these versions nor row identities.
pub async fn revision<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    boundary: &SavedHistoryKey,
    snapshot: &SavedHistorySnapshot,
) -> Result<String> {
    let (inputs, items) = storage_tables(db).await?;
    let row = db.query_one_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        format!("SELECT COUNT(*) AS n, COALESCE(SUM(message_revision),0) AS revisions, \
        COALESCE(SUM(message_deleted_at IS NOT NULL),0) AS deleted, \
        (SELECT COUNT(*) FROM {items} i JOIN turn p ON p.id=i.turn_id WHERE p.thread_id=?1 AND i.rowid<=?5) AS items, \
        (SELECT COALESCE(SUM(COALESCE(r.revision,1)),0) FROM {items} i JOIN turn p ON p.id=i.turn_id LEFT JOIN compaction_item_revision r ON r.source_id=i.id AND r.turn_id=i.turn_id WHERE p.thread_id=?1 AND i.rowid<=?5) AS item_revisions, \
        (SELECT COALESCE(SUM(COALESCE(r.rowid,0)),0) FROM {items} i JOIN turn p ON p.id=i.turn_id LEFT JOIN compaction_item_revision r ON r.source_id=i.id AND r.turn_id=i.turn_id WHERE p.thread_id=?1 AND i.rowid<=?5) AS item_registrations, \
        (SELECT COUNT(*) FROM {inputs} i JOIN turn p ON p.id=i.turn_id WHERE p.thread_id=?1 AND i.rowid<=?4) AS inputs, \
        (SELECT COALESCE(SUM(COALESCE(r.revision,1)),0) FROM {inputs} i JOIN turn p ON p.id=i.turn_id LEFT JOIN compaction_input_revision r ON r.source_id=i.id AND r.turn_id=i.turn_id WHERE p.thread_id=?1 AND i.rowid<=?4) AS input_revisions, \
        (SELECT COALESCE(SUM(COALESCE(r.rowid,0)),0) FROM {inputs} i JOIN turn p ON p.id=i.turn_id LEFT JOIN compaction_input_revision r ON r.source_id=i.id AND r.turn_id=i.turn_id WHERE p.thread_id=?1 AND i.rowid<=?4) AS input_registrations, \
        (SELECT COUNT(*) FROM thread_lineage l WHERE l.parent_thread_id=?1 AND l.created_at<=?2 AND l.rowid<=?6) AS children \
        FROM turn WHERE thread_id=?1 AND created_at<=?2 AND rowid<=?3"),
        vec![thread_id.to_owned().into(), boundary.created_at.clone().into(), snapshot.turns.into(),snapshot.inputs.into(),snapshot.items.into(),snapshot.children.into()])).await?
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
