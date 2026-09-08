use anyhow::Result;
use sea_orm::{ConnectionTrait, FromQueryResult, Statement};

const PAGE_ROWS: u64 = 128;
const PAYLOAD_BUDGET: i64 = 256 * 1024;

#[derive(Debug, Default)]
pub struct NativeEventCleanupOutcome {
    pub last_rowid: Option<i64>,
    pub rows_scanned: u64,
    pub rows_deleted: u64,
}

#[derive(Debug, FromQueryResult)]
pub(crate) struct SourceRow {
    pub source_rowid: i64,
    id: String,
    payload_bytes: i64,
}

pub(crate) async fn prepare<C: ConnectionTrait>(db: &C, after: i64) -> Result<Vec<SourceRow>> {
    // Page the source before filtering: retained/orphan rows must not cause
    // an unbounded candidate search or prevent reaching later eligible rows.
    let rows = SourceRow::find_by_statement(Statement::from_sql_and_values(
        db.get_database_backend(),
        "SELECT rowid AS source_rowid, id, length(CAST(payload_redacted_json AS BLOB)) AS payload_bytes \
         FROM cli_runtime_native_event WHERE rowid > ? ORDER BY rowid LIMIT ?",
        [after.into(), PAGE_ROWS.into()],
    ))
    .all(db)
    .await?;
    let mut bytes = 0;
    let mut page = Vec::new();
    for row in rows {
        if !page.is_empty() && bytes + row.payload_bytes > PAYLOAD_BUDGET {
            break;
        }
        bytes += row.payload_bytes;
        page.push(row);
    }
    Ok(page)
}

pub(crate) async fn apply<C: ConnectionTrait>(db: &C, page: &[SourceRow]) -> Result<u64> {
    let mut remaining = PAYLOAD_BUDGET;
    let ids: Vec<_> = page
        .iter()
        .filter_map(|row| {
            if row.payload_bytes > remaining {
                return None;
            }
            remaining -= row.payload_bytes;
            Some(row.id.clone().into())
        })
        .collect();
    if ids.is_empty() {
        return Ok(0);
    }
    let placeholders = vec!["?"; ids.len()].join(",");
    // Keep lifecycle/error events. Only redundant item traffic is eligible,
    // after both canonical execution and runtime/recovery have settled.
    let sql = format!(
        r#"
        DELETE FROM cli_runtime_native_event AS e
        WHERE e.id IN ({placeholders})
          AND e.native_method IN (
            'item/started', 'item/completed', 'item/agentMessage/delta',
            'item/commandExecution/outputDelta', 'turn/diff/updated',
            'thread/tokenUsage/updated', 'account/rateLimits/updated'
          )
          AND EXISTS (SELECT 1 FROM "turn" t WHERE t.id = e.turn_id
              AND t.status IN ('completed', 'failed', 'interrupted'))
          AND EXISTS (SELECT 1 FROM turn_cli_runtime_binding b
              WHERE b.turn_id = e.turn_id AND b.runtime_id = e.runtime_id
                AND b.status IN ('completed', 'failed', 'interrupted'))
          AND NOT EXISTS (SELECT 1 FROM turn_cli_runtime_attempt a
              WHERE a.turn_id = e.turn_id AND a.status IN ('starting', 'running'))
          AND NOT EXISTS (SELECT 1 FROM turn_cli_runtime_execution_segment s
              WHERE s.turn_id = e.turn_id AND s.status = 'running')
          AND NOT EXISTS (SELECT 1 FROM recovery_job r WHERE r.turn_id = e.turn_id
              AND (r.status IN ('pending', 'active') OR r.resolution_pending = 1))
          AND EXISTS (SELECT 1 FROM turn_event_projection_stream_state p
              WHERE p.turn_id = e.turn_id AND p.status = 'healthy'
                AND p.projected_through_sequence > 0
                AND NOT EXISTS (SELECT 1 FROM turn_event_projection_state receipt
                    WHERE receipt.turn_id = p.turn_id
                      AND receipt.sequence > p.projected_through_sequence))
    "#
    );
    Ok(db
        .execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            ids,
        ))
        .await?
        .rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{Database, DatabaseConnection};

    const REDUNDANT_METHODS: [&str; 7] = [
        "item/started",
        "item/completed",
        "item/agentMessage/delta",
        "item/commandExecution/outputDelta",
        "turn/diff/updated",
        "thread/tokenUsage/updated",
        "account/rateLimits/updated",
    ];

    async fn database() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared(r#"
            CREATE TABLE "turn" (id TEXT PRIMARY KEY, status TEXT);
            CREATE TABLE turn_cli_runtime_binding (turn_id TEXT PRIMARY KEY, runtime_id TEXT, status TEXT);
            CREATE TABLE turn_cli_runtime_attempt (turn_id TEXT, status TEXT);
            CREATE TABLE turn_cli_runtime_execution_segment (turn_id TEXT, status TEXT);
            CREATE TABLE recovery_job (turn_id TEXT, status TEXT, resolution_pending INTEGER);
            CREATE TABLE turn_event_projection_stream_state (
                turn_id TEXT PRIMARY KEY, status TEXT, projected_through_sequence INTEGER);
            CREATE TABLE turn_event_projection_state (turn_id TEXT, sequence INTEGER, status TEXT);
            CREATE TABLE cli_runtime_native_event (
                id TEXT PRIMARY KEY, turn_id TEXT, runtime_id TEXT,
                native_method TEXT, payload_redacted_json TEXT);
            INSERT INTO "turn" VALUES ('done', 'completed');
            INSERT INTO turn_cli_runtime_binding VALUES ('done', 'codex', 'completed');
            INSERT INTO turn_event_projection_stream_state VALUES ('done', 'healthy', 10);
        "#).await.unwrap();
        db
    }

    async fn insert(
        db: &DatabaseConnection,
        id: &str,
        turn_id: Option<&str>,
        method: &str,
        bytes: usize,
    ) {
        db.execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "INSERT INTO cli_runtime_native_event VALUES (?, ?, 'codex', ?, ?)",
            [
                id.into(),
                turn_id.into(),
                method.into(),
                "x".repeat(bytes).into(),
            ],
        ))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn deletes_only_redundant_terminal_traffic_and_can_restart() {
        let db = database().await;
        for method in REDUNDANT_METHODS {
            insert(&db, method, Some("done"), method, 2).await;
        }
        for method in ["turn/completed", "error", "future/event"] {
            insert(&db, method, Some("done"), method, 2).await;
        }
        insert(&db, "orphan", Some("missing"), "item/completed", 2).await;
        insert(&db, "unbound", None, "item/completed", 2).await;
        let page = prepare(&db, 0).await.unwrap();
        assert_eq!(page.len(), 12);
        assert_eq!(apply(&db, &page).await.unwrap(), 7);
        assert_eq!(apply(&db, &page).await.unwrap(), 0);
        let restarted = prepare(&db, 0).await.unwrap();
        assert_eq!(restarted.len(), 5);
        assert_eq!(apply(&db, &restarted).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn revalidates_execution_recovery_and_projection_after_discovery() {
        for change in [
            "UPDATE \"turn\" SET status = 'in_progress'",
            "UPDATE \"turn\" SET status = 'blocked'",
            "UPDATE turn_cli_runtime_binding SET status = 'starting'",
            "UPDATE turn_cli_runtime_binding SET status = 'running'",
            "DELETE FROM turn_cli_runtime_binding",
            "INSERT INTO turn_cli_runtime_attempt VALUES ('done', 'starting')",
            "INSERT INTO turn_cli_runtime_attempt VALUES ('done', 'running')",
            "INSERT INTO turn_cli_runtime_execution_segment VALUES ('done', 'running')",
            "INSERT INTO recovery_job VALUES ('done', 'pending', 0)",
            "INSERT INTO recovery_job VALUES ('done', 'active', 0)",
            "INSERT INTO recovery_job VALUES ('done', 'exhausted', 1)",
            "UPDATE turn_event_projection_stream_state SET status = 'quarantined'",
            "DELETE FROM turn_event_projection_stream_state",
            "INSERT INTO turn_event_projection_state VALUES ('done', 11, 'pending')",
            "INSERT INTO turn_event_projection_state VALUES ('done', 11, 'exhausted')",
            "INSERT INTO turn_event_projection_state VALUES ('done', 11, 'projected')",
        ] {
            let db = database().await;
            for method in REDUNDANT_METHODS {
                insert(&db, method, Some("done"), method, 2).await;
            }
            let page = prepare(&db, 0).await.unwrap();
            db.execute_unprepared(change).await.unwrap();
            assert_eq!(apply(&db, &page).await.unwrap(), 0, "{change}");
            assert_eq!(
                prepare(&db, 0).await.unwrap().len(),
                REDUNDANT_METHODS.len()
            );
        }
    }

    #[tokio::test]
    async fn bounds_scan_and_bytes_without_getting_stuck_on_retained_rows() {
        let db = database().await;
        for id in 0..130 {
            insert(&db, &format!("orphan-{id}"), None, "item/completed", 1).await;
        }
        let first = prepare(&db, 0).await.unwrap();
        assert_eq!(first.len(), 128);
        assert_eq!(apply(&db, &first).await.unwrap(), 0);
        let second = prepare(&db, first.last().unwrap().source_rowid)
            .await
            .unwrap();
        assert_eq!(second.len(), 2);

        insert(
            &db,
            "oversized",
            Some("done"),
            "item/completed",
            PAYLOAD_BUDGET as usize + 1,
        )
        .await;
        insert(&db, "a", Some("done"), "item/completed", 180_000).await;
        insert(&db, "b", Some("done"), "item/completed", 180_000).await;
        let oversized = prepare(&db, second.last().unwrap().source_rowid)
            .await
            .unwrap();
        assert_eq!(oversized.len(), 1);
        assert_eq!(apply(&db, &oversized).await.unwrap(), 0);
        let a = prepare(&db, oversized[0].source_rowid).await.unwrap();
        assert_eq!(a.len(), 1);
        assert_eq!(apply(&db, &a).await.unwrap(), 1);
        let b = prepare(&db, a[0].source_rowid).await.unwrap();
        assert_eq!(b.len(), 1);
        assert_eq!(apply(&db, &b).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn cleanup_uses_the_current_schema_and_requires_maintenance_access() {
        use migration::{Migrator, MigratorTrait};
        let db = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        let absent = vec![SourceRow {
            source_rowid: 1,
            id: "missing".into(),
            payload_bytes: 0,
        }];
        assert_eq!(apply(&db, &absent).await.unwrap(), 0);
        let store = crate::CrudStore::new(db);
        assert!(store.cleanup_native_events_quantum(0).await.is_err());
        assert!(
            store
                .with_maintenance_access()
                .cleanup_native_events_quantum(0)
                .await
                .unwrap()
                .last_rowid
                .is_none()
        );
    }
}
