use anyhow::{Context, Result};
use pioneer_crud_payloads::BLOCK_BYTES;
use pioneer_crud_payloads::storage::{PreparedPayload, metadata};
use pioneer_crud_tasks::conversation_snapshot::{
    NewTaskRunConversationSnapshot, capture_snapshot, delete_by_run, insert_if_absent,
};
use pioneer_entity::{portable_payload_part as part, task_run_conversation_snapshot as snapshot};
use pioneer_postgres_test::with_database;
use sea_orm::{
    AccessMode, ColumnTrait, ConnectionTrait, EntityTrait, IsolationLevel, PaginatorTrait,
    QueryFilter, TransactionTrait, sea_query::Expr,
};

#[tokio::test]
async fn snapshot_owns_a_blocked_value_and_readers_keep_their_committed_version() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                VALUES ('thread', 'w', '', 'chat', 'model', 'provider', 'active');
            INSERT INTO task (id, workspace_id, owner_kind, owner_id, executor_kind, status, title, goal)
                VALUES ('task', 'w', 'thread', 'thread', 'agent', 'running', 'Task', 'test');
            INSERT INTO task_run (id, task_id, run_group_id, run_number, status, executor_kind)
                VALUES ('run', 'task', 'run', 1, 'running', 'agent');
        "#).await?;
        let original = format!(r#"[ {{ "role": "user", "content": "{}\u0000" }} ]"#, "🙂é".repeat(BLOCK_BYTES / 4));
        let input = NewTaskRunConversationSnapshot {
            run_id: "run".into(),
            task_id: "task".into(),
            workspace_id: "w".into(),
            conversation_thread_id: "thread".into(),
            source_turn_id: None,
            history_json: original.clone(),
            created_at: chrono::Utc::now().fixed_offset(),
        };
        let first = PreparedPayload::new(&original)?;
        let second = PreparedPayload::new("[]")?;

        let tx = db.begin().await?;
        assert!(insert_if_absent(&tx, &input, None, Some(&first)).await?.1);
        tx.rollback().await?;
        assert!(metadata(db, first.id()).await?.is_none());

        let tx = db.begin().await?;
        let (saved, inserted) = insert_if_absent(&tx, &input, None, Some(&first)).await?;
        assert!(inserted);
        assert_eq!(saved.history_json_payload_id.as_deref(), Some(first.id()));
        assert!(!insert_if_absent(&tx, &input, None, Some(&second)).await?.1);
        tx.commit().await?;
        assert!(metadata(db, second.id()).await?.is_none());

        let reader = db.begin_with_config(Some(IsolationLevel::RepeatableRead), Some(AccessMode::ReadOnly)).await?;
        let read = capture_snapshot(&reader, "run").await?.context("missing original snapshot")?;
        let tx = db.begin().await?;
        second.insert(&tx).await?;
        snapshot::Entity::update_many()
            .col_expr(snapshot::Column::HistoryJsonPayloadId, Expr::val(second.id()))
            .filter(snapshot::Column::RunId.eq("run"))
            .exec(&tx).await?;
        tx.commit().await?;
        assert!(metadata(db, first.id()).await?.is_none());
        assert_eq!(part::Entity::find().filter(part::Column::PayloadId.eq(first.id())).count(db).await?, 0);
        let previous = capture_snapshot(&reader, "run").await?.context("missing snapshot in prior view")?;
        reader.commit().await?;
        assert_eq!(read.into_record()?.history_json, original);
        assert_eq!(previous.into_record()?.history_json, original);

        let tx = db.begin_with_config(Some(IsolationLevel::RepeatableRead), Some(AccessMode::ReadOnly)).await?;
        let replacement = capture_snapshot(&tx, "run").await?.context("missing replacement")?;
        tx.commit().await?;
        assert_eq!(replacement.into_record()?.history_json, "[]");
        let tx = db.begin().await?;
        assert_eq!(delete_by_run(&tx, "run").await?, 1);
        tx.commit().await?;
        assert!(metadata(db, second.id()).await?.is_none());
        Ok(())
    }).await
}
