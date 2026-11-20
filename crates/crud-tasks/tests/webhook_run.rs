use anyhow::Result;
use pioneer_crud_tasks::{
    event_log::{self, EventPayload, NewEvent},
    webhook_run::{has_webhook_sibling_activity, is_webhook_queue_run},
};
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, TransactionTrait};

#[tokio::test]
async fn webhook_retry_proves_exact_input_and_last_activity_survives_sibling_completion()
-> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO task (id, workspace_id, owner_kind, executor_kind, status, title, goal)
                VALUES ('task', 'w', 'workspace', 'agent', 'running', 'Task', '');
            INSERT INTO task_trigger (id, task_id, kind, status, spec_json)
                VALUES ('hook', 'task', 'webhook', 'active', '{}');
            INSERT INTO task_run
                (id, task_id, trigger_id, run_group_id, run_number, status, executor_kind,
                 trigger_input_json, started_at, completed_at)
                VALUES ('original', 'task', 'hook', 'original', 1, 'failed', 'agent',
                    '{ "text": "\u0000" }', '2026-11-16T09:00:00Z', '2026-11-16T09:01:00Z');
            INSERT INTO task_run
                (id, task_id, trigger_id, run_group_id, run_number, status, executor_kind,
                 trigger_input_json, attempt_number, retry_of_run_id, parent_run_id)
                VALUES ('retry', 'task', 'hook', 'original', 2, 'queued', 'agent',
                    '{ "text": "\u0000" }', 2, 'original', 'original');
            INSERT INTO task_occurrence_contract
                (occurrence_id, task_id, run_id, trigger_id, occurrence_key, action_idempotency_key)
                VALUES ('original', 'task', 'original', 'hook', 'webhook:original', 'task:task:original');
            "#,
        )
        .await?;
        assert!(is_webhook_queue_run(db, "task", "hook", "completing", 1).await?);
        assert!(is_webhook_queue_run(db, "task", "hook", "completing", 2).await?);
        assert!(!is_webhook_queue_run(db, "task", "hook", "original", 2).await?);
        db.execute_unprepared(
            r#"UPDATE task_run SET trigger_input_json = '{"text":"\u0000"}' WHERE id = 'retry'"#,
        )
        .await?;
        assert!(!is_webhook_queue_run(db, "task", "hook", "completing", 2).await?);

        let now = chrono::Utc::now().fixed_offset();
        for (id, run_id, event_type, expected) in [
            ("started", Some("original"), "task/run/started", true),
            ("queued", None, "task/queued", false),
        ] {
            let tx = db.begin().await?;
            event_log::lock_task(&tx, "task").await?;
            event_log::append(
                &tx,
                NewEvent {
                    id,
                    task_id: "task",
                    run_id,
                    thread_id: None,
                    turn_id: None,
                    event_type,
                    idempotency_key: Some(id),
                    sequence: event_log::next_sequence(&tx, "task").await?,
                    created_at: now,
                },
                EventPayload::prepare("{}")?,
            )
            .await?;
            tx.commit().await?;
            assert_eq!(
                has_webhook_sibling_activity(db, "task", "hook", "completing", "running").await?,
                expected
            );
        }
        Ok(())
    })
    .await
}
