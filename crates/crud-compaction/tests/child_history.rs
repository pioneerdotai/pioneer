use anyhow::Result;
use pioneer_crud_compaction::child_history::compaction_child_history_parent;
use pioneer_crud_compaction::input_copy_aliases::compaction_task_input_copy_aliases;
use pioneer_crud_payloads::storage::PreparedPayload;
use pioneer_crud_tasks::conversation_snapshot::{NewTaskRunConversationSnapshot, insert_if_absent};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement, TransactionTrait};

async fn seed_child_history(db: &sea_orm::DatabaseConnection) -> Result<()> {
    db.execute_unprepared(
        "INSERT INTO workspace(id,name) VALUES('ws-history','Fixture');
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
         VALUES('parent-history','ws-history','','agent','m','p','active'),
               ('child-history','ws-history','','agent','m','p','active');
         INSERT INTO turn(id,thread_id,status) VALUES
               ('source-history','parent-history','completed'),
               ('turn-history','child-history','completed');
         INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at)
         VALUES('child-history','parent-history','parent-history',1,CURRENT_TIMESTAMP);
         INSERT INTO task(id,workspace_id,owner_kind,owner_id,executor_kind,status,title,goal,
                          created_by_thread_id,created_by_turn_id,metadata_json)
         VALUES('task-history','ws-history','thread','parent-history','agent','running','Task','Fixture',
                'parent-history','source-history','{ \"z\": 1, \"a\": 2 }');
         INSERT INTO task_run(id,task_id,run_group_id,run_number,status,executor_kind)
         VALUES('run-history','task-history','run-history',1,'running','agent');
         INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status)
         VALUES('run-turn-history','task-history','run-history','child-history','turn-history',
                'initial',0,1,'completed');
         INSERT INTO task_agent_spec(id,task_id,run_id,prompt_json,context_policy_json,depth,max_depth)
         VALUES('task-policy','task-history',NULL,'{}','{ \"mode\": \"empty\" }',0,1),
               ('run-policy','task-history','run-history','{}','{ \"mode\": \"last_n_turns\" }',0,1);",
    ).await?;

    let snapshot = NewTaskRunConversationSnapshot {
        run_id: "run-history".into(),
        task_id: "task-history".into(),
        workspace_id: "ws-history".into(),
        conversation_thread_id: "parent-history".into(),
        source_turn_id: Some("source-history".into()),
        history_json: "[]".into(),
        created_at: sea_orm::prelude::DateTimeUtc::from_timestamp(1_700_000_000, 0)
            .expect("valid fixture timestamp")
            .fixed_offset(),
    };
    let prepared = PreparedPayload::new(&snapshot.history_json)?;
    let tx = db.begin().await?;
    assert!(
        insert_if_absent(&tx, &snapshot, None, Some(&prepared))
            .await?
            .1
    );
    tx.commit().await?;

    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn child_history_scope_preserves_exact_task_metadata_and_run_policy() -> Result<()> {
    let fixture = pioneer_postgres_test::scoped_database().await?;
    let db = fixture.connection();
    seed_child_history(db).await?;
    let parent = compaction_child_history_parent(db, "ws-history", "child-history")
        .await?
        .expect("exact saved child relationship");
    assert_eq!(parent.parent_thread, "parent-history");
    assert_eq!(parent.root_thread, "parent-history");
    assert_eq!(parent.source_turn.as_deref(), Some("source-history"));
    assert_eq!(parent.task_source_turn.as_deref(), Some("source-history"));
    assert_eq!(
        parent.task_metadata_json.as_deref(),
        Some(r#"{ "z": 1, "a": 2 }"#)
    );
    assert_eq!(
        parent.context_policy_json.as_deref(),
        Some(r#"{ "mode": "last_n_turns" }"#)
    );

    db.execute_unprepared("DELETE FROM task_agent_spec WHERE id='run-policy'")
        .await?;
    let fallback = compaction_child_history_parent(db, "ws-history", "child-history")
        .await?
        .expect("task policy applies when the run has no policy");
    assert_eq!(
        fallback.context_policy_json.as_deref(),
        Some(r#"{ "mode": "empty" }"#)
    );
    assert!(
        compaction_child_history_parent(db, "other-workspace", "child-history")
            .await
            .is_err()
    );
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn composer_alias_requires_exact_accepted_input_and_current_source() -> Result<()> {
    let fixture = pioneer_postgres_test::scoped_database().await?;
    let db = fixture.connection();
    seed_child_history(db).await?;
    let input = serde_json::to_value(pioneer_protocol::UserInput::Text {
        text: "original 雪 input".into(),
        text_elements: Vec::new(),
    })?;
    let metadata = serde_json::json!({
        "composerWork": {
            "version": pioneer_protocol::TASK_COMPOSER_WORK_VERSION,
            "launch": {
                "thread_id": "parent-history",
                "turn_id": "source-history",
                "input": [input],
            }
        }
    });
    db.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE task SET metadata_json=$1::text::json WHERE id='task-history'",
        [serde_json::to_string_pretty(&metadata)?.into()],
    ))
    .await?;
    db.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload)
         VALUES('original-input','source-history',0,'text','original 雪 input',$1),
               ('copied-input','turn-history',0,'text','original 雪 input',$1)",
        [serde_json::to_string(&input)?.into()],
    ))
    .await?;

    let ids = ["copied-input".to_owned()];
    let copies = compaction_task_input_copy_aliases(db, "ws-history", &ids).await?;
    assert_eq!(copies.len(), 1);
    assert_eq!(copies[0].original_id, "original-input");
    assert_eq!(copies[0].original_turn_id, "source-history");
    assert_eq!(copies[0].copy_id, "copied-input");
    assert_eq!(copies[0].copy_turn_id, "turn-history");
    assert!(
        compaction_task_input_copy_aliases(db, "other-workspace", &ids)
            .await?
            .is_empty()
    );
    db.execute_unprepared("UPDATE turn SET message_revision=1 WHERE id='source-history'")
        .await?;
    assert!(
        compaction_task_input_copy_aliases(db, "ws-history", &ids)
            .await?
            .is_empty()
    );
    Ok(())
}
