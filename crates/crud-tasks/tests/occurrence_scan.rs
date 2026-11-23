use anyhow::Result;
use pioneer_crud_tasks::occurrence_scan::scan_terminal_task_occurrence_mismatches;
use pioneer_postgres_test::with_database;
use sea_orm::ConnectionTrait;

#[tokio::test]
async fn occurrence_scan_pages_past_mismatched_execution_and_reads_exact_terminal_authority()
-> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO task (id, workspace_id, owner_kind, executor_kind, status, title, goal)
                VALUES ('task', 'w', 'workspace', 'system', 'running', 'Task', '');
            INSERT INTO task_run
                (id, task_id, run_group_id, run_number, status, executor_kind, completed_at)
                VALUES ('first', 'task', 'first', 1, 'succeeded', 'system', statement_timestamp()),
                       ('second', 'task', 'second', 2, 'succeeded', 'system', statement_timestamp());
            INSERT INTO task_run_execution
                (id, task_id, task_run_id, executor_kind, status, completed_at)
                VALUES ('first-execution', 'task', 'first', 'system', 'failed', statement_timestamp()),
                       ('second-execution', 'task', 'second', 'system', 'succeeded', statement_timestamp());
            INSERT INTO task_occurrence_contract
                (occurrence_id, task_id, run_id, occurrence_key, action_idempotency_key, execution_generation, status)
                VALUES ('first', 'task', 'first', 'manual:first', 'task:task:first', 1, 'running'),
                       ('second', 'task', 'second', 'manual:second', 'task:task:second', 2, 'running');
            "#,
        )
        .await?;
        let first = scan_terminal_task_occurrence_mismatches(db, None, 1).await?;
        assert_eq!(first.scanned_occurrences, 1);
        assert!(first.mismatches.is_empty());
        assert_eq!(first.next_cursor.as_deref(), Some("first"));
        let second = scan_terminal_task_occurrence_mismatches(db, first.next_cursor.as_deref(), 1)
            .await?;
        assert_eq!(second.scanned_occurrences, 1);
        assert!(second.next_cursor.is_none());
        assert_eq!(second.mismatches.len(), 1);
        assert_eq!(second.mismatches[0].run_id, "second");
        assert_eq!(second.mismatches[0].execution_id, "second-execution");
        assert_eq!(second.mismatches[0].expected_occurrence_status, "delivered");
        Ok(())
    })
    .await
}
