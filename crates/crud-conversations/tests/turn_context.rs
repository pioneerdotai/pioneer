use anyhow::{Context, Result};
use pioneer_crud_conversations::turn_context::{
    NewTurnLlmContextEntry, append, list_turn_llm_context,
};
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};
use std::time::Duration;

const TURN: &str = "context-turn";

async fn seed(db: &impl ConnectionTrait) -> Result<()> {
    db.execute_unprepared(
        "INSERT INTO workspace(id,name) VALUES ('w','Workspace');
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
            VALUES ('thread','w','','agent','model','provider','active');
         INSERT INTO turn(id,thread_id,status) VALUES ('context-turn','thread','in_progress');",
    )
    .await?;
    Ok(())
}

fn entry() -> NewTurnLlmContextEntry {
    NewTurnLlmContextEntry {
        turn_id: TURN.to_owned(),
        item_id: Some("assistant_round_1".to_owned()),
        attempt_id: None,
        sequence: 0,
        source: "assistant_round".to_owned(),
        tool_name: None,
        payload: r#"{"round":1}"#.to_owned(),
        output_policy_snapshot: "{}".to_owned(),
        created_at: chrono::DateTime::from_timestamp(1_700_000_100, 0)
            .unwrap()
            .fixed_offset(),
        expires_at: None,
    }
}

#[tokio::test]
async fn native_transcript_sequence_and_delivery_identity_survive_store_restart() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        seed(db).await?;
        let original = entry();
        let tx = db.begin().await?;
        let first = append(&tx, original.clone(), "delivery_round_1").await?;
        tx.commit().await?;
        assert_eq!(first.entry.sequence, 1);
        assert!(!first.already_committed);

        let restarted = fixture.connect().await?;
        let tx = restarted.begin().await?;
        let duplicate = append(&tx, original.clone(), "delivery_round_1").await?;
        assert_eq!(duplicate.entry.id, first.entry.id);
        assert!(duplicate.already_committed);
        let second = append(
            &tx,
            NewTurnLlmContextEntry {
                item_id: Some("tool_result_1".to_owned()),
                source: "tool_result".to_owned(),
                payload: r#"{"result":true}"#.to_owned(),
                ..original.clone()
            },
            "delivery_tool_1",
        )
        .await?;
        tx.commit().await?;
        assert_eq!(second.entry.sequence, 2);
        assert_eq!(
            list_turn_llm_context(&restarted, TURN)
                .await?
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            [1, 2],
        );
        let tx = restarted.begin().await?;
        let collision = append(
            &tx,
            NewTurnLlmContextEntry {
                payload: r#"{"round":999}"#.to_owned(),
                ..original
            },
            "delivery_round_1",
        )
        .await
        .expect_err("a delivery key cannot alias different content");
        assert!(collision.to_string().contains("delivery key collision"));
        tx.rollback().await?;
        restarted.close().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn independent_context_writers_serialize_sequence_and_delivery_keys() -> Result<()> {
    for (commit_first, same_key) in [(true, false), (true, true), (false, true)] {
        with_database(async |fixture| {
            let db = fixture.connection();
            seed(db).await?;
            let first = db.begin().await?;
            let first_outcome = append(&first, entry(), "first").await?;
            let other = fixture.connect().await?;
            let second = other.begin().await?;
            let pid = second
                .query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT pg_backend_pid() AS pid",
                ))
                .await?
                .context("writer PID")?
                .try_get::<i32>("", "pid")?;
            let waiter = tokio::spawn(async move {
                let outcome =
                    append(&second, entry(), if same_key { "first" } else { "other" }).await?;
                second.commit().await?;
                Ok::<_, anyhow::Error>(outcome)
            });
            let blocked = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let blocked = db
                        .query_one_raw(Statement::from_sql_and_values(
                            DbBackend::Postgres,
                            "SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked",
                            [pid.into()],
                        ))
                        .await?
                        .context("writer lock state")?
                        .try_get::<bool>("", "blocked")?;
                    if blocked {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await;
            if commit_first {
                first.commit().await?;
            } else {
                first.rollback().await?;
            }
            let second_outcome = waiter.await??;
            blocked.context("second writer must wait on the context stream lock")??;
            assert_eq!(second_outcome.already_committed, commit_first && same_key);
            let expected_sequence = if commit_first && !same_key { 2 } else { 1 };
            assert_eq!(second_outcome.entry.sequence, expected_sequence);
            assert_eq!(
                first_outcome.entry.id == second_outcome.entry.id,
                commit_first && same_key
            );
            assert_eq!(
                list_turn_llm_context(db, TURN).await?.len(),
                expected_sequence as usize
            );
            other.close().await?;
            Ok(())
        })
        .await?;
    }
    Ok(())
}
