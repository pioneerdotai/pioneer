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

/// Per-source insertion watermarks pin the traversal without holding a reader
/// snapshot across calls. These values stay inside an encrypted tool cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedHistorySnapshot {
    pub turns: i64,
    pub inputs: i64,
    pub items: i64,
    pub children: i64,
}

pub async fn snapshot<C: ConnectionTrait>(db: &C, thread_id: &str) -> Result<SavedHistorySnapshot> {
    let row = db.query_one_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "SELECT COALESCE((SELECT MAX(rowid) FROM turn WHERE thread_id=?1),0) AS turns, \
        COALESCE((SELECT MAX(i.rowid) FROM turn_input i JOIN turn t ON t.id=i.turn_id WHERE t.thread_id=?1),0) AS inputs, \
        COALESCE((SELECT MAX(i.rowid) FROM turn_item i JOIN turn t ON t.id=i.turn_id WHERE t.thread_id=?1),0) AS items, \
        COALESCE((SELECT MAX(rowid) FROM thread_lineage WHERE parent_thread_id=?1),0) AS children",
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
FROM turn_input i JOIN turn t ON t.id = i.turn_id
WHERE t.thread_id = ?1 AND t.message_deleted_at IS NULL AND i.rowid <= ?10
UNION ALL
SELECT i.created_at, 'item' AS kind, i.id AS id, t.id AS turn_id, i.item_id
FROM turn_item i JOIN turn t ON t.id = i.turn_id
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
    ensure!((1..=101).contains(&limit), "invalid history page limit");
    let sql = format!(
        "SELECT * FROM ({SOURCE}) WHERE (?2 IS NULL OR (created_at, kind, id) <= (?2, ?3, ?4)) AND (?5 IS NULL OR (created_at, kind, id) < (?5, ?6, ?7)) ORDER BY created_at DESC, kind DESC, id DESC LIMIT ?8"
    );
    let values = vec![
        thread_id.to_owned().into(),
        boundary.map(|k| k.created_at.clone()).into(),
        boundary.map(|k| k.kind.clone()).into(),
        boundary.map(|k| k.id.clone()).into(),
        before.map(|k| k.created_at.clone()).into(),
        before.map(|k| k.kind.clone()).into(),
        before.map(|k| k.id.clone()).into(),
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

/// Edits and deletes invalidate continuation even when their original position
/// is older than the current page. New turns after the boundary are excluded.
pub async fn revision<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    boundary: &SavedHistoryKey,
    snapshot: &SavedHistorySnapshot,
) -> Result<String> {
    let row = db.query_one_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "SELECT COUNT(*) AS n, COALESCE(SUM(message_revision),0) AS revisions, \
        COALESCE(SUM(message_deleted_at IS NOT NULL),0) AS deleted, \
        (SELECT COUNT(*) FROM turn_item i JOIN turn p ON p.id=i.turn_id WHERE p.thread_id=?1 AND i.created_at<=?2 AND i.rowid<=?5) AS items, \
        (SELECT COALESCE(SUM(r.revision),0) FROM turn_item i JOIN turn p ON p.id=i.turn_id LEFT JOIN compaction_item_revision r ON r.source_id=i.id AND r.turn_id=i.turn_id WHERE p.thread_id=?1 AND i.created_at<=?2 AND i.rowid<=?5) AS item_revisions, \
        (SELECT COUNT(*) FROM turn_input i JOIN turn p ON p.id=i.turn_id WHERE p.thread_id=?1 AND i.created_at<=?2 AND i.rowid<=?4) AS inputs, \
        (SELECT COALESCE(SUM(r.revision),0) FROM turn_input i JOIN turn p ON p.id=i.turn_id LEFT JOIN compaction_input_revision r ON r.source_id=i.id AND r.turn_id=i.turn_id WHERE p.thread_id=?1 AND i.created_at<=?2 AND i.rowid<=?4) AS input_revisions, \
        (SELECT COUNT(*) FROM thread_lineage l WHERE l.parent_thread_id=?1 AND l.created_at<=?2 AND l.rowid<=?6) AS children \
        FROM turn WHERE thread_id=?1 AND created_at<=?2 AND rowid<=?3",
        vec![thread_id.to_owned().into(), boundary.created_at.clone().into(), snapshot.turns.into(),snapshot.inputs.into(),snapshot.items.into(),snapshot.children.into()])).await?
        .ok_or_else(|| anyhow::anyhow!("history revision unavailable"))?;
    let parts = [
        "n",
        "revisions",
        "deleted",
        "items",
        "item_revisions",
        "inputs",
        "input_revisions",
        "children",
    ]
    .into_iter()
    .map(|key| row.try_get::<i64>("", key).map(|v| v.to_string()))
    .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(parts.join(":"))
}
