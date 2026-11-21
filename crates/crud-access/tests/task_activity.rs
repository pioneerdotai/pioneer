use pioneer_crud_access::authorization_scope::list_running_task_scopes;
use pioneer_protocol::PrincipalId;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::ConnectionTrait;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[tokio::test]
async fn tree_task_activity_is_a_bounded_read_without_history_or_conversation_status() {
    pioneer_postgres_test::with_database(async |fixture| {
        let thread_id = "thr_task".to_owned();
        let mut writer = fixture.connection().clone();
        writer
            .execute_unprepared(
                "INSERT INTO workspace (id, name, is_active, is_current)
                     VALUES ('ws_task', 'Tasks', TRUE, FALSE);
                 INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider,
                                     status, origin_kind, created_by_actor_kind)
                     VALUES ('thr_task', 'ws_task', '', 'agent', 'gpt-5.4', 'openai',
                             'idle', 'user', 'system');
                 INSERT INTO task (id, workspace_id, owner_kind, owner_id, created_by_thread_id,
                                   executor_kind, status, title, goal)
                     VALUES ('task_0000000000000001', 'ws_task', 'thread', 'thr_task', 'thr_task',
                             'agent', 'scheduled', 'Check weather', 'Send the daily weather summary');
                 INSERT INTO task_run (id, task_id, run_group_id, run_number, status, executor_kind)
                     VALUES ('run_00000000000000001', 'task_0000000000000001',
                             'run_00000000000000001', 1, 'running', 'agent')",
            )
            .await?;
        let mut reader = fixture.connect_read_only().await?;
        let counter = Arc::new(AtomicU64::new(0));
        for connection in [&mut writer, &mut reader] {
            let counter = Arc::clone(&counter);
            connection.set_metric_callback(move |_| {
                counter.fetch_add(1, Ordering::Relaxed);
            });
        }
        let db = SqliteDatabase::new(reader, writer);
        let principal = PrincipalId::new("P00000000000000000001").unwrap();
        counter.store(0, Ordering::Relaxed);
        let page = list_running_task_scopes(
            &db,
            "ws_task",
            &[thread_id.clone(), "missing-thread".into()],
            &principal,
            "",
        )
        .await
        .unwrap();
        assert_eq!(
            counter.load(Ordering::Relaxed),
            2,
            "thin discovery and one bounded scope SELECT"
        );
        let filtered = list_running_task_scopes(
            &db,
            "ws_task",
            &["missing-thread".into()],
            &principal,
            "",
        )
        .await
        .unwrap();
        assert!(filtered.scopes.is_empty());
        assert_eq!(
            filtered.after_task_id, page.after_task_id,
            "discovery must progress even when every scope is filtered"
        );
        assert_eq!(page.scopes.len(), 1);
        assert_eq!(
            page.scopes[0].0.root_thread_id.as_deref(),
            Some(thread_id.as_str())
        );
        assert!(
            list_running_task_scopes(
                &db.maintenance(),
                "ws_task",
                &[thread_id],
                &principal,
                page.after_task_id.as_deref().unwrap()
            )
            .await
            .unwrap()
            .scopes
            .is_empty()
        );
        Ok(())
    })
    .await
    .unwrap();
}
