use anyhow::Result;
use pioneer_crud_execution::authority::{active_agent_tasks, active_turns};
use pioneer_postgres_test::with_database;
use sea_orm::ConnectionTrait;

#[tokio::test]
async fn authority_integrity_scan_pages_live_work_and_keeps_missing_grants_visible() -> Result<()> {
    with_database(async |database| {
        let db = database.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                VALUES ('thread', 'w', '', 'agent', 'model', 'provider', 'active');
            INSERT INTO "turn" (id, thread_id, status, execution_authorization_context_json)
                VALUES ('a', 'thread', 'in_progress', '{ "test": true }'),
                       ('b', 'thread', 'in_progress', NULL),
                       ('c', 'thread', 'completed', NULL);
            INSERT INTO task (id, workspace_id, owner_kind, executor_kind, status, title, goal)
                VALUES ('a', 'w', 'workspace', 'agent', 'running', 'Active', 'Goal'),
                       ('b', 'w', 'workspace', 'agent', 'completed', 'Completed', 'Goal'),
                       ('c', 'w', 'workspace', 'command', 'running', 'Command', 'Goal');
        "#,
        )
        .await?;
        let page = active_turns(db, None, 1).await?;
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].turn_id, "a");
        assert_eq!(
            page[0].authority_envelope_json.as_deref(),
            Some(r#"{ "test": true }"#)
        );
        let next = active_turns(db, Some("a"), 10).await?;
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].turn_id, "b");
        assert!(next[0].authority_envelope_json.is_none());

        let tasks = active_agent_tasks(db, None, 10).await?;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].task_id, "a");
        assert!(tasks[0].authority_envelope_json.is_none());
        assert!(tasks[0].principal_id.is_none());
        assert!(active_agent_tasks(db, Some("a"), 10).await?.is_empty());
        Ok(())
    })
    .await
}
