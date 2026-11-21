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

#[tokio::test]
async fn turn_llm_context_repository_round_trips_and_cleans_terminal_turns() {
    use pioneer_crud_conversations::context_cleanup::{
        delete_expired_turn_llm_context, delete_turn_llm_context_for_terminal_turns,
        delete_turn_llm_context_for_turn,
    };
    use pioneer_crud_conversations::turn_context::insert;
    use pioneer_protocol::ToolOutputPolicySnapshot;

    with_database(async |fixture| {
        let timestamp = chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .fixed_offset();
        let db = pioneer_sqlite::SqliteDatabase::new(
            fixture.connect_read_only().await?,
            fixture.connection().clone(),
        );
        seed(&db).await?;
        let active_turn_id = TURN;
        let terminal_turn_id = "turn_llm_terminal";
        db.execute_unprepared(
            "INSERT INTO turn(id,thread_id,status) VALUES ('turn_llm_terminal','thread','completed')",
        ).await?;

        let read_file_policy =
            serde_json::to_string(&ToolOutputPolicySnapshot::for_tool_name("read_file"))
                .expect("policy should serialize");
        let shell_policy =
            serde_json::to_string(&ToolOutputPolicySnapshot::for_tool_name("exec_command"))
                .expect("policy should serialize");
        let future_expiry = chrono::Utc::now().fixed_offset() + chrono::Duration::days(1);
        let expired_expiry = chrono::Utc::now().fixed_offset() - chrono::Duration::days(1);

        for (turn_id, sequence, tool_name, payload, policy) in [
            (
                active_turn_id,
                2,
                "read_file",
                r#"{"kind":"text","text":"later"}"#,
                read_file_policy.as_str(),
            ),
            (
                active_turn_id,
                1,
                "exec_command",
                r#"{"kind":"text","text":"earlier"}"#,
                shell_policy.as_str(),
            ),
            (
                terminal_turn_id,
                1,
                "web_fetch",
                r#"{"kind":"text","text":"temporary"}"#,
                read_file_policy.as_str(),
            ),
        ] {
            let tx = db.begin().await?;
            insert(&tx, NewTurnLlmContextEntry {
                    turn_id: turn_id.to_owned(),
                    item_id: Some(format!("item_{sequence}")),
                    attempt_id: Some(format!("attempt_{sequence}")),
                    sequence,
                    source: "tool_result".to_owned(),
                    tool_name: Some(tool_name.to_owned()),
                    payload: payload.to_owned(),
                    output_policy_snapshot: policy.to_owned(),
                    created_at: timestamp + chrono::Duration::seconds(sequence),
                    expires_at: Some(future_expiry),
                })
                .await
                .expect("llm context row should insert");
            tx.commit().await?;
        }
        let tx = db.begin().await?;
        insert(&tx, NewTurnLlmContextEntry {
                turn_id: active_turn_id.to_owned(),
                item_id: Some("item_expired".to_owned()),
                attempt_id: Some("attempt_expired".to_owned()),
                sequence: 3,
                source: "tool_result".to_owned(),
                tool_name: Some("read_file".to_owned()),
                payload: r#"{"kind":"text","text":"expired"}"#.to_owned(),
                output_policy_snapshot: read_file_policy.clone(),
                created_at: timestamp + chrono::Duration::seconds(3),
                expires_at: Some(expired_expiry),
            })
            .await
            .expect("expired llm context row should insert");
        tx.commit().await?;

        let active_rows = list_turn_llm_context(&db, active_turn_id)
            .await
            .expect("active context should list");
        assert_eq!(
            active_rows
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(active_rows[0].tool_name.as_deref(), Some("exec_command"));
        assert_eq!(active_rows[1].tool_name.as_deref(), Some("read_file"));
        assert_eq!(active_rows[2].item_id.as_deref(), Some("item_expired"));

        let deleted_expired = delete_expired_turn_llm_context(&db)
            .await
            .expect("expired cleanup should succeed");
        assert_eq!(deleted_expired, 1);
        assert_eq!(
            list_turn_llm_context(&db, active_turn_id)
                .await
                .expect("active context should survive expired cleanup")
                .len(),
            2
        );

        let deleted_terminal = delete_turn_llm_context_for_terminal_turns(&db)
            .await
            .expect("terminal cleanup should succeed");
        assert_eq!(deleted_terminal, 1);
        assert!(
            list_turn_llm_context(&db, terminal_turn_id)
                .await
                .expect("terminal context should list")
                .is_empty()
        );
        assert_eq!(
            list_turn_llm_context(&db, active_turn_id)
                .await
                .expect("active context should survive terminal cleanup")
                .len(),
            2
        );

        let deleted_active = delete_turn_llm_context_for_turn(&db, active_turn_id)
            .await
            .expect("turn cleanup should succeed");
        assert_eq!(deleted_active, 2);
        assert!(
            list_turn_llm_context(&db, active_turn_id)
                .await
                .expect("active context should list after delete")
                .is_empty()
        );
        Ok(())
    })
    .await
    .unwrap();
}
