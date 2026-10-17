//! Receipt-gated continuation of the existing terminal-child owner.
use anyhow::{Context, Result};
use sea_orm::{ConnectionTrait, Statement};

// A terminal aggregate alone is not a receipt. The materialized parent must be
// closed and its authoritative watermark must cover its accepted terminal event.
// Use the watermark (retained across receipt compaction), not receipt row count.
pub(crate) const READY: &str = r#"
    trt.status <> 'blocked' AND t.status IN ('completed', 'failed', 'interrupted')
    AND EXISTS (
        SELECT 1 FROM thread_lineage l
        JOIN "turn" parent ON parent.id = l.created_by_turn_id
            AND parent.thread_id = COALESCE(l.created_by_thread_id, l.parent_thread_id)
        JOIN turn_event_projection_stream_state s ON s.turn_id = parent.id
        WHERE l.child_thread_id = t.thread_id AND parent.turn_kind = 'task_run'
            AND parent.status IN ('completed', 'failed', 'interrupted')
            AND s.status = 'healthy' AND s.accepted_terminal_sequence IS NOT NULL
            AND s.projected_through_sequence >= s.accepted_terminal_sequence
    )
"#;

/// None: the aggregate is still owned by ordinary child reconciliation.
/// Some(false): terminal, but cleanup is fenced until the parent's receipt.
pub(crate) async fn ready<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<bool>> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!(
                r#"SELECT CASE WHEN {READY} THEN 1 ELSE 0 END AS ready
            FROM task_run_turn trt JOIN task_run r ON r.id = trt.run_id
            JOIN "turn" t ON t.id = trt.turn_id AND t.thread_id = trt.thread_id
            WHERE t.thread_id = ? AND t.id = ?
              AND r.status IN ('succeeded', 'failed', 'cancelled', 'timed_out')"#
            ),
            [thread_id.into(), turn_id.into()],
        ))
        .await
        .context("failed to inspect terminal child cleanup receipt")?;
    row.map(|row| {
        row.try_get::<i64>("", "ready")
            .map(|n| n != 0)
            .map_err(Into::into)
    })
    .transpose()
}
