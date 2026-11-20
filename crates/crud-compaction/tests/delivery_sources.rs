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
