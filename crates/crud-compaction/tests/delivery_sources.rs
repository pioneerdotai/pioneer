use anyhow::{Context, Result};
use pioneer_crud_compaction::delivery_sources::{
    delivery_events, next_delivery_turn, recheck_delivery_events,
};
use pioneer_postgres_test::with_database;
use sea_orm::ConnectionTrait;
use serde_json::json;

#[tokio::test]
async fn delivery_discovery_and_exact_refresh_obey_the_event_endpoint_and_scope() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('workspace', 'Workspace');
             INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
             VALUES ('thread', 'workspace', '', 'agent', 'model', 'provider', 'active');
             INSERT INTO turn (id, thread_id, status, created_at)
             VALUES ('turn', 'thread', 'completed', '2026-11-19 00:00:00Z');
             INSERT INTO task_delivery (id, workspace_id, task_id, run_id, delivery_key, mode,
                 status, target_thread_id, delivered_turn_id)
             VALUES ('delivery', 'workspace', 'task', 'run', 'delivery-key', 'thread',
                     'delivered', 'thread', 'turn');
             INSERT INTO turn_event (id, turn_id, thread_id, sequence, event_type, payload, created_at)
             VALUES ('event-a', 'turn', 'thread', 1, 'completed', '{}', '2026-11-19 00:00:00Z'),
                    ('event-b', 'turn', 'thread', 2, 'completed', '{}', '2026-11-19 00:00:01Z');"
        ).await?;

        let entry = next_delivery_turn(db, "workspace", "thread", "", "turn", i64::MAX, None)
            .await?.context("missing delivered turn")?;
        assert_eq!(entry.turn_id, "turn");
        assert_eq!(entry.event_high_water, Some(2));
        assert!(next_delivery_turn(db, "other", "thread", "", "turn", i64::MAX, None).await?.is_none());
        assert!(next_delivery_turn(db, "workspace", "thread", "turn", "turn", i64::MAX, None).await?.is_none());

        let rows = delivery_events(db, "turn", "thread", "workspace", None, 1, i64::MAX,
            None, "result:", "completed").await?;
        assert_eq!(rows.len(), 1);
        let event = &rows[0];
        assert_eq!(event.event_id, "event-a");
        let key = json!([{
            "source": {"scope": "event:turn", "id": event.event_id, "version": format!("event-revision:{}", event.revision)},
            "sequence": event.sequence,
            "capture_order": event.capture_order
        }]);
        let refreshed = recheck_delivery_events(db, &key.to_string(), "workspace", "thread",
            i64::MAX, "result:", "completed").await?;
        assert_eq!(refreshed.len(), 1);
        assert_eq!(refreshed[0].capture_order, event.capture_order);
        let tail = delivery_events(db, "turn", "thread", "workspace", Some(1), 2, i64::MAX,
            None, "result:", "completed").await?;
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].event_id, "event-b");

        db.execute_unprepared("UPDATE turn_event SET payload = '{\"changed\":true}' WHERE id = 'event-a'").await?;
        assert!(recheck_delivery_events(db, &key.to_string(), "workspace", "thread", i64::MAX,
            "result:", "completed").await?.is_empty());
        Ok(())
    }).await
}

#[tokio::test]
async fn delivered_output_discovery_without_deliveries_ignores_foreign_history() -> Result<()> {
    use pioneer_crud_compaction::history_fence::compaction_history_read_fence;
    use pioneer_crud_compaction::task_output::{
        DeliveredTaskOutputCursor, compaction_delivered_output_page,
    };
    use sea_orm::{Statement, TransactionTrait};
    use std::sync::{Arc, Mutex};

    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES ('ws','Workspace'),('foreign-ws','Foreign');
             INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
             VALUES ('thread','ws','','agent','m','p','active'),
                    ('foreign-thread','foreign-ws','','agent','m','p','active');
             INSERT INTO turn(id,thread_id,status) VALUES ('turn','thread','completed'),
                                                        ('foreign-turn','foreign-thread','completed');
             INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload)
             SELECT 'irrelevant-'||value,'thread','turn',value,'item/completed','{}'
             FROM generate_series(1,260) AS value;
             INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload)
             SELECT 'foreign-'||value,'foreign-thread','foreign-turn',value,'item/completed','{}'
             FROM generate_series(1,4096) AS value;",
        ).await?;
        let fence = compaction_history_read_fence(db).await?;
        let statements = Arc::new(Mutex::new(Vec::<Statement>::new()));
        let mut reader = fixture.connect_read_only().await?;
        let recorded = statements.clone();
        reader.set_metric_callback(move |info| recorded.lock().unwrap().push(info.statement.clone()));
        let cursor = DeliveredTaskOutputCursor::default();
        let page = compaction_delivered_output_page(&reader, "ws", "thread", &cursor, &fence).await?;
        assert!(page.done);
        assert!(page.entries.is_empty() && page.unprojected_events.is_empty());
        assert_eq!(page.selected_turn_rows, 0);
        assert_eq!(page.selected_event_rows, 0);
        let mut statement = {
            let statements = statements.lock().unwrap();
            assert_eq!(statements.len(), 1);
            statements[0].clone()
        };
        assert!(statement.sql.starts_with("SELECT d.delivered_turn_id"));
        for forbidden in ["SELECT *", "compaction_event_revision WHERE", "rowid", "payload"] {
            assert!(!statement.sql.contains(forbidden));
        }
        let tx = db.begin().await?;
        tx.execute_unprepared("SET LOCAL enable_seqscan = off").await?;
        statement.sql = format!("EXPLAIN (COSTS OFF) {}", statement.sql);
        let plan = tx.query_all_raw(statement).await?
            .into_iter()
            .map(|row| row.try_get::<String>("", "QUERY PLAN"))
            .collect::<Result<Vec<_>, _>>()?;
        let uses = |index: &str| plan.iter().any(|detail| detail.contains("Index") && detail.contains(index));
        assert!(uses("compaction_delivery_turn") || uses("idx_task_delivery_inbox_result"), "{plan:#?}");
        assert!(uses("compaction_turn_creation_turn_id_key"), "{plan:#?}");
        assert!(uses("idx_turn_events_turn_id_sequence"), "{plan:#?}");
        assert!(!uses("compaction_event_revision_capture_order"), "{plan:#?}");
        tx.rollback().await?;

        db.execute_unprepared(
            "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload)
             VALUES ('late','thread','turn',261,'item/completed','{}')",
        ).await?;
        assert_eq!(compaction_delivered_output_page(&reader, "ws", "thread", &cursor, &fence).await?, page);
        reader.close().await?;
        Ok(())
    }).await
}
