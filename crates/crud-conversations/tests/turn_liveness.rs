use anyhow::{Context, Result};
use pioneer_crud_conversations::turn_liveness::{
    TurnLivenessObservation, find_by_turn_id, observe_activity,
};
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};
use std::time::Duration;

#[tokio::test]
async fn concurrent_first_observations_keep_the_later_sequence_in_both_orders() -> Result<()> {
    for (first_sequence, second_sequence) in [(11, 12), (12, 11)] {
        with_database(async |fixture| {
            let db = fixture.connection();
            db.execute_unprepared(
                r#"
                INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
                INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                    VALUES ('thread', 'w', '', 'chat', 'model', 'provider', 'active');
                INSERT INTO turn (id, thread_id, status) VALUES ('turn', 'thread', 'in_progress');
            "#,
            )
            .await?;
            let now = chrono::Utc::now().fixed_offset();
            let observation = |activity_sequence| TurnLivenessObservation {
                turn_id: "turn".into(),
                thread_id: "thread".into(),
                activity_kind: "item".into(),
                activity_sequence,
                item_id: Some(format!("item-{activity_sequence}")),
                item_type: None,
                observed_at: now,
            };

            let first = db.begin().await?;
            assert!(observe_activity(&first, observation(first_sequence)).await?);
            let second = db.begin().await?;
            let pid: i32 = second
                .query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT pg_backend_pid() AS pid",
                ))
                .await?
                .context("missing second backend PID")?
                .try_get("", "pid")?;
            let next = observation(second_sequence);
            let writer = tokio::spawn(async move {
                let applied = observe_activity(&second, next).await?;
                second.commit().await?;
                Ok::<_, anyhow::Error>(applied)
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let blocked: bool = db
                        .query_one_raw(Statement::from_sql_and_values(
                            DbBackend::Postgres,
                            "SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked",
                            [pid.into()],
                        ))
                        .await?
                        .context("no lock observation")?
                        .try_get("", "blocked")?;
                    if blocked {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("second observation did not wait for the first")??;
            first.commit().await?;
            assert_eq!(writer.await??, second_sequence > first_sequence);
            let committed = find_by_turn_id(db, "turn")
                .await?
                .context("missing liveness row")?;
            assert_eq!(committed.last_activity_sequence, 12);
            assert_eq!(committed.last_activity_item_id.as_deref(), Some("item-12"));
            Ok(())
        })
        .await?;
    }
    Ok(())
}
