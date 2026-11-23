use anyhow::{Context, Result};
use pioneer_compaction::SourceRef;
use pioneer_crud_compaction::task_basis;
use pioneer_crud_payloads::storage::{self, PreparedPayload};
use pioneer_crud_tasks::conversation_snapshot::{NewTaskRunConversationSnapshot, insert_if_absent};
use pioneer_entity::compaction_task_basis_revision;
use pioneer_postgres_test::with_database;
use sea_orm::{AccessMode, ConnectionTrait, EntityTrait, IsolationLevel, TransactionTrait};

#[tokio::test]
async fn compressed_task_basis_keeps_exact_fragments_and_the_first_snapshot_after_reopen()
-> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
             INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                 VALUES ('thread', 'w', '', 'chat', 'model', 'provider', 'active');
             INSERT INTO task (id, workspace_id, owner_kind, owner_id, executor_kind, status, title, goal)
                 VALUES ('task', 'w', 'thread', 'thread', 'agent', 'running', 'Task', 'test');
             INSERT INTO task_run (id, task_id, run_group_id, run_number, status, executor_kind)
                 VALUES ('run', 'task', 'run', 1, 'running', 'agent');",
        ).await?;
        let history = format!(
            "  [\n {{\"role\": \"user\", \"content\": {}}}\n]\n",
            serde_json::to_string(&"контекст🦀e\u{301}\"\n".repeat(16_000))?,
        );
        let mut input = NewTaskRunConversationSnapshot {
            run_id: "run".into(),
            task_id: "task".into(),
            workspace_id: "w".into(),
            conversation_thread_id: "thread".into(),
            source_turn_id: None,
            history_json: history.clone(),
            created_at: "2026-11-19T15:00:00Z".parse()?,
        };
        let payload = PreparedPayload::new(&history)?;
        let tx = db.begin().await?;
        let (first, published) = insert_if_absent(&tx, &input, None, Some(&payload)).await?;
        assert!(published);
        tx.commit().await?;
        let metadata = storage::metadata(db, payload.id()).await?.context("payload header")?;
        assert_eq!(metadata.original_bytes, history.len());
        assert_eq!(metadata.original_chars, history.chars().count());
        let revision = compaction_task_basis_revision::Entity::find_by_id("run")
            .one(db).await?.context("basis revision")?;
        assert_eq!(revision.revision, 1);
        let reference = SourceRef {
            scope: "task-basis:run".into(),
            id: "run".into(),
            version: format!("task-basis-revision:{}", revision.revision),
        };

        let independent = fixture.connect().await?;
        input.history_json = r#"[{"role":"assistant","content":"losing retry"}]"#.into();
        let losing = PreparedPayload::new(&input.history_json)?;
        let retry = independent.begin().await?;
        let (winner, inserted) = insert_if_absent(&retry, &input, None, Some(&losing)).await?;
        assert_eq!(winner.run_id, first.run_id);
        assert_eq!(winner.history_json_payload_id, first.history_json_payload_id);
        assert_eq!(winner.portable_identity, first.portable_identity);
        assert_eq!(winner.portable_revision, first.portable_revision);
        assert_eq!(winner.created_at, first.created_at);
        assert!(!inserted);
        retry.commit().await?;
        assert!(storage::metadata(&independent, losing.id()).await?.is_none());

        let mut rebuilt = String::new();
        let mut offset = 0u64;
        loop {
            let read = independent.begin_with_config(
                Some(IsolationLevel::RepeatableRead), Some(AccessMode::ReadOnly),
            ).await?;
            let captured = task_basis::capture(&read, "w", "thread", &reference, Some(offset))
                .await?.context("current Task basis fragment")?;
            read.commit().await?;
            let text = captured.body.into_text()?;
            assert_eq!(text, history.chars().skip(usize::try_from(offset)?).take(16_384).collect::<String>());
            rebuilt.push_str(&text);
            offset += text.chars().count() as u64;
            if let Some(next) = captured.next_character {
                assert_eq!(next, offset);
            } else {
                break;
            }
        }
        assert_eq!(rebuilt, history);

        let read = independent.begin_with_config(
            Some(IsolationLevel::RepeatableRead), Some(AccessMode::ReadOnly),
        ).await?;
        let empty_tail = task_basis::capture(&read, "w", "thread", &reference, Some(offset))
            .await?.context("EOF is a valid fragment")?;
        let whole = task_basis::capture(&read, "w", "thread", &reference, None)
            .await?.context("whole Task basis source")?;
        assert!(task_basis::capture(&read, "other", "thread", &reference, None).await?.is_none());
        read.commit().await?;
        assert!(empty_tail.next_character.is_none());
        assert!(empty_tail.body.into_text()?.is_empty());
        assert_eq!(whole.body.into_text()?, history);
        let unchanged = compaction_task_basis_revision::Entity::find_by_id("run")
            .one(db).await?.context("basis revision after retry")?;
        assert_eq!(unchanged.revision, revision.revision);
        independent.close().await?;
        Ok(())
    }).await
}
