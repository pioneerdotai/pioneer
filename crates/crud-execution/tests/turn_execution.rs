use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use pioneer_crud_execution::turn_execution::{find, heartbeat_owner};
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, TransactionTrait};

#[tokio::test]
async fn owner_heartbeat_updates_only_active_receipts_and_rolls_back_the_whole_lease() -> Result<()>
{
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                VALUES ('thread', 'w', '', 'agent', 'model', 'provider', 'active');
            INSERT INTO "turn" (id, thread_id, status)
                VALUES ('active', 'thread', 'in_progress'),
                       ('terminal', 'thread', 'completed'),
                       ('foreign', 'thread', 'in_progress');
            INSERT INTO turn_execution (turn_id, thread_id, workspace_id, executor_kind, status,
                owner_id, lease_until, heartbeat_at, completed_at)
                VALUES
                  ('active', 'thread', 'w', 'native_agent', 'queued',
                    'worker', '2026-11-15T20:00:30Z', '2026-11-15T20:00:00Z', NULL),
                  ('terminal', 'thread', 'w', 'native_agent', 'completed',
                    'worker', '2026-11-15T20:00:30Z', '2026-11-15T20:00:00Z', '2026-11-15T20:00:15Z'),
                  ('foreign', 'thread', 'w', 'native_agent', 'queued',
                    'other', '2026-11-15T20:00:30Z', '2026-11-15T20:00:00Z', NULL);
            "#,
        )
        .await?;
        let original = DateTime::parse_from_rfc3339("2026-11-15T20:00:00Z")?;
        let writer = fixture.connect().await?;
        let tx = writer.begin().await?;
        assert_eq!(heartbeat_owner(&tx, "worker", 45).await?, 1);
        let updated = find(&tx, "active").await?.context("missing active receipt")?;
        assert_eq!(updated.lease_until, updated.heartbeat_at + Duration::seconds(45));
        assert_eq!(updated.updated_at, updated.heartbeat_at);
        assert_eq!(find(db, "active").await?.context("missing active receipt")?.heartbeat_at, original);
        tx.rollback().await?;
        assert_eq!(find(db, "active").await?.context("missing active receipt")?.heartbeat_at, original);

        let before = Utc::now().timestamp();
        assert_eq!(heartbeat_owner(&writer, "worker", 30).await?, 1);
        let updated = find(db, "active").await?.context("missing active receipt")?;
        assert!((before..=Utc::now().timestamp()).contains(&updated.heartbeat_at.timestamp()));
        assert_eq!(updated.lease_until, updated.heartbeat_at + Duration::seconds(30));
        for id in ["terminal", "foreign"] {
            assert_eq!(find(db, id).await?.context("missing unchanged receipt")?.heartbeat_at, original);
        }
        writer.close().await?;
        Ok(())
    })
    .await
}
