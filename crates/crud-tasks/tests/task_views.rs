use anyhow::Result;
use pioneer_crud_tasks::task::TaskRootAccessFilter;
use pioneer_crud_tasks::task_trigger::upsert_trigger;
use pioneer_crud_tasks::task_views::{
    get_task_tree_bounded, list_task_agenda_scoped, list_tasks_scoped,
};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_postgres_test::with_database;
use pioneer_protocol::{
    TaskAgendaParams, TaskListParams, TaskStatus, TaskTrigger, TaskTriggerKind, TaskTriggerSpec,
    TaskTriggerStatus,
};
use sea_orm::ConnectionTrait;

#[tokio::test]
async fn agenda_filters_and_orders_before_applying_limit() -> Result<()> {
    with_database(async |fixture| {
        let writer = fixture.connection();
        writer.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('ws', 'Workspace');
             INSERT INTO task (id, workspace_id, owner_kind, owner_id, executor_kind, status, title, goal, updated_at) VALUES
               ('a-early', 'ws', 'thread', 'visible', 'agent', 'scheduled', 'Early', 'Earlier', '2000-01-01T00:00:00Z'),
               ('b-tie', 'ws', 'thread', 'visible', 'agent', 'scheduled', 'Tie', 'Same time', '2001-01-01T00:00:00Z'),
               ('c-late', 'ws', 'thread', 'visible', 'agent', 'scheduled', 'Later', 'Later', '2020-01-01T00:00:00Z'),
               ('d-paused', 'ws', 'thread', 'visible', 'agent', 'scheduled', 'Paused', 'Paused', '2021-01-01T00:00:00Z'),
               ('e-hidden', 'ws', 'thread', 'hidden', 'agent', 'scheduled', 'Hidden', 'Hidden', '2022-01-01T00:00:00Z'),
               ('f-done', 'ws', 'thread', 'visible', 'agent', 'completed', 'Done', 'Done', '2023-01-01T00:00:00Z');",
        ).await?;
        let now = 1_700_000_000;
        for (id, offset, status) in [
            ("a-early", 10, TaskTriggerStatus::Active),
            ("b-tie", 10, TaskTriggerStatus::Active),
            ("c-late", 30, TaskTriggerStatus::Active),
            ("d-paused", 0, TaskTriggerStatus::Paused),
            ("e-hidden", 1, TaskTriggerStatus::Active),
            ("f-done", 1, TaskTriggerStatus::Active),
        ] {
            upsert_trigger(writer, &TaskTrigger {
                id: format!("{id}-trigger"),
                task_id: id.into(),
                status,
                spec: TaskTriggerSpec::ScheduledAt { scheduled_at: now + offset, timezone: None, catch_up_policy: None },
                next_fire_at: Some(now + offset),
                last_fire_at: None,
                created_at: now,
                updated_at: now,
                webhook_secret_ref: None,
            }).await?;
        }
        let db = PostgresDatabase::new(fixture.connect_read_only().await?, writer.clone());
        let access = TaskRootAccessFilter { allowed_root_thread_ids: vec!["visible".to_owned()] };
        let params = TaskAgendaParams {
            workspace_id: "ws".to_owned(),
            statuses: vec![TaskStatus::Scheduled],
            trigger_kinds: vec![TaskTriggerKind::ScheduledAt],
            from: Some(now),
            to: Some(now + 20),
            limit: Some(1),
            ..Default::default()
        };
        let response = list_task_agenda_scoped(&db, params.clone(), Some(&access)).await?;
        assert_eq!(response.items.len(), 1);
        assert_eq!(response.items[0].task.id, "a-early");
        assert_eq!(response.items[0].next_fire_at, Some(now + 10));
        let response = list_task_agenda_scoped(
            &db, TaskAgendaParams { limit: Some(3), ..params }, Some(&access),
        ).await?;
        assert_eq!(response.items.iter().map(|item| item.task.id.as_str()).collect::<Vec<_>>(), ["a-early", "b-tie"]);
        Ok(())
    }).await
}

#[tokio::test]
async fn tree_limit_rejects_partial_results_and_keeps_exact_children() -> Result<()> {
    with_database(async |fixture| {
        let writer = fixture.connection();
        writer.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('ws', 'Workspace');
             INSERT INTO task (id, workspace_id, owner_kind, executor_kind, status, title, goal, parent_task_id, root_task_id) VALUES
               ('root', 'ws', 'workspace', 'agent', 'draft', 'Root', 'Root', NULL, NULL),
               ('child', 'ws', 'workspace', 'agent', 'draft', 'Child', 'Child', 'root', 'root');",
        ).await?;
        let db = PostgresDatabase::new(fixture.connect_read_only().await?, writer.clone());
        assert!(get_task_tree_bounded(&db, "root", 1).await.is_err());
        let root = get_task_tree_bounded(&db, "root", 2).await?.unwrap();
        assert_eq!(root.task.id, "root");
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].task.id, "child");
        assert!(root.children[0].children.is_empty());
        Ok(())
    }).await
}

#[tokio::test]
async fn task_list_pages_use_id_order_when_timestamps_match() -> Result<()> {
    with_database(async |fixture| {
        let writer = fixture.connection();
        writer.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('ws', 'Workspace');
             INSERT INTO task (id, workspace_id, owner_kind, executor_kind, status, title, goal, updated_at) VALUES
               ('b-tie', 'ws', 'workspace', 'agent', 'draft', 'B', 'B', '2020-01-01T00:00:00Z'),
               ('a-tie', 'ws', 'workspace', 'agent', 'draft', 'A', 'A', '2020-01-01T00:00:00Z'),
               ('z-newer', 'ws', 'workspace', 'agent', 'draft', 'Z', 'Z', '2021-01-01T00:00:00Z');",
        ).await?;
        let db = PostgresDatabase::new(fixture.connect_read_only().await?, writer.clone());
        for (offset, expected) in ["z-newer", "a-tie", "b-tie"].into_iter().enumerate() {
            let page = list_tasks_scoped(&db, TaskListParams {
                workspace_id: "ws".into(),
                limit: Some(1),
                cursor: Some(offset as u64),
                ..Default::default()
            }, None).await?;
            assert_eq!(page.len(), 1);
            assert_eq!(page[0].id, expected);
        }
        Ok(())
    }).await
}
