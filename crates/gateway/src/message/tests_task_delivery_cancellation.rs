use super::*;
use pioneer_crud::{TaskDeliveryCommitTestKind, TaskDeliveryTransitionOutcome};

const RACE_TIME: i64 = 4_300_000_000;

async fn race_processor() -> (tempfile::TempDir, Arc<MessageProcessor>, String) {
    let (directory, workspaces, store, workspace_id) = setup_pooled_file_workspace_manager().await;
    let sessions = Arc::new(SessionManager::new());
    let (sender, _receiver) = mpsc::channel(32);
    let connection_id = register_authenticated_test_connection(sessions.as_ref(), sender).await;
    let processor = Arc::new(MessageProcessor::with_agent_manager(
        Arc::new(ThreadManager::new("o4-mini", "openai")),
        Arc::new(AgentManager::new(test_provider(), test_tool_loop_config())),
        sessions,
        workspaces,
        store,
    ));
    processor
        .task_runtime
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;
    processor
        .thread_manager
        .thread_start_seeded(
            connection_id,
            workspace_id.clone(),
            ThreadStartParams {
                thread_id: "thr_race_delivery".to_owned(),
                workspace_id: workspace_id.clone(),
                name: Some("Delivery race".to_owned()),
                model: Some("o4-mini".to_owned()),
                model_provider: Some("openai".to_owned()),
                sandbox: Some(SandboxMode::FullAccess),
                mode: Some(ThreadMode::Message),
                origin_kind: None,
                sidebar_visibility: None,
                visibility: None,
                agent_nickname: None,
                agent_role: None,
            },
            None,
            None,
        )
        .await
        .unwrap();
    (directory, processor, workspace_id)
}

async fn queue_worker_delivery(
    processor: &MessageProcessor,
    workspace: &str,
    thread: &str,
    at: i64,
) -> pioneer_protocol::TaskDelivery {
    let task = processor
        .task_runtime
        .service()
        .create_task(
            pioneer_tasks::TaskCreateContext::default(),
            TaskCreateParams {
                workspace_id: workspace.to_owned(),
                owner_kind: TaskOwnerKind::Thread,
                owner_id: Some(thread.to_owned()),
                created_by_thread_id: Some(thread.to_owned()),
                created_by_turn_id: None,
                parent_task_id: None,
                executor_kind: TaskExecutorKind::System,
                title: "Delivery race".to_owned(),
                goal: "Deliver result".to_owned(),
                priority: 0,
                trigger: TaskTriggerInput {
                    spec: TaskTriggerSpec::Interval {
                        interval_seconds: 3600,
                        interval_anchor_at: Some(at),
                        catch_up_policy: None,
                    },
                },
                launch: None,
                agent_spec: None,
                lifecycle_policy: None,
                delivery_policy: Some(TaskDeliveryPolicy {
                    mode: TaskDeliveryMode::Thread,
                    thread_target: Some(pioneer_protocol::TaskDeliveryThreadTarget::ExactThread),
                    thread_id: Some(thread.to_owned()),
                    webhook_url: None,
                    include_result: true,
                    format: TaskDeliveryFormat::Summary,
                }),
                retry_policy: None,
                timeout_policy: None,
                concurrency_policy: None,
                metadata: None,
            },
        )
        .await
        .unwrap()
        .task;
    processor.task_runtime.process_due_once(at).await.unwrap();
    processor
        .crud_store
        .list_task_deliveries(TaskDeliveriesParams {
            workspace_id: workspace.to_owned(),
            task_id: Some(task.id),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .unwrap()
        .deliveries
        .remove(0)
}

async fn cancellation_worker_case(
    kind: TaskDeliveryCommitTestKind,
    execution_fails: bool,
    execution_times_out: bool,
) {
    let (_directory, processor, workspace) = race_processor().await;
    let first = queue_worker_delivery(
        &processor,
        &workspace,
        if execution_fails {
            "thr_missing_delivery"
        } else {
            "thr_race_delivery"
        },
        RACE_TIME,
    )
    .await;
    let second =
        queue_worker_delivery(&processor, &workspace, "thr_race_delivery", RACE_TIME + 1).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    processor
        .crud_store
        .set_delivery_commit_gate_for_test(kind, entered.clone(), release.clone());
    let worker = processor.clone();
    let pass =
        tokio::spawn(async move { worker.process_due_task_deliveries(RACE_TIME + 1, 10).await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    processor
        .task_runtime
        .service()
        .cancel_task(
            pioneer_tasks::TaskMutationContext::default(),
            pioneer_protocol::TaskCancelParams {
                task_id: first.task_id.clone(),
                reason: Some("cancel_worker_race".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .unwrap();
    let before = processor
        .crud_store
        .list_task_deliveries(TaskDeliveriesParams {
            workspace_id: workspace.clone(),
            task_id: Some(first.task_id.clone()),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .unwrap();
    let journal = processor
        .task_runtime
        .service()
        .get_task_events(pioneer_protocol::TaskEventsParams {
            task_id: first.task_id.clone(),
            after_sequence: None,
            limit: Some(100),
        })
        .await
        .unwrap()
        .events;
    let mut subscription =
        processor
            .task_runtime
            .event_bus()
            .subscribe(pioneer_tasks::TaskEventFilter {
                task_ids: vec![first.task_id.clone()],
                ..Default::default()
            });
    if execution_times_out {
        // The execution is held at an owned production completion boundary.
        // Advance its real worker deadline without a sleep or wall-clock wait.
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::time::resume();
    } else {
        release.notify_one();
    }
    tokio::time::timeout(Duration::from_secs(10), pass)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let after = processor
        .crud_store
        .list_task_deliveries(TaskDeliveriesParams {
            workspace_id: workspace,
            task_id: Some(first.task_id.clone()),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .unwrap();
    assert_eq!(after.deliveries, before.deliveries);
    assert_eq!(after.attempts, before.attempts);
    assert_eq!(after.deliveries[0].status, TaskDeliveryStatus::Cancelled);
    assert_eq!(
        processor
            .task_runtime
            .service()
            .get_task_events(pioneer_protocol::TaskEventsParams {
                task_id: first.task_id.clone(),
                after_sequence: None,
                limit: Some(100),
            })
            .await
            .unwrap()
            .events,
        journal
    );
    let next = processor
        .crud_store
        .get_task_delivery(&second.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        next.status,
        TaskDeliveryStatus::Delivered,
        "worker must continue after a superseded operation"
    );
    assert_eq!(next.attempt_count, 1);
    if kind == TaskDeliveryCommitTestKind::Start {
        assert!(after.attempts.is_empty());
        assert!(
            processor
                .crud_store
                .get_task_delivery(&first.id)
                .await
                .unwrap()
                .unwrap()
                .delivered_turn_id
                .is_none()
        );
        let turns = processor
            .crud_store
            .database_connection()
            .query_one_raw(sea_orm::Statement::from_sql_and_values(
                sea_orm::DbBackend::Sqlite,
                "SELECT COUNT(*) AS count FROM turn_item WHERE item_id=?",
                [pioneer_protocol::task_delivery_result_item_id(&first.id).into()],
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            turns.try_get::<i64>("", "count").unwrap(),
            0,
            "lost start must not execute externally"
        );
    }
    use std::future::Future;
    std::future::poll_fn(|cx| {
        let mut receive = Box::pin(subscription.recv());
        assert!(matches!(
            receive.as_mut().poll(cx),
            std::task::Poll::Pending
        ));
        std::task::Poll::Ready(())
    })
    .await;
    // Even an explicit late finalization uses the same typed losing contract.
    if let Some(attempt) = before.attempts.first() {
        let mut original = first;
        original.status = TaskDeliveryStatus::Delivering;
        original.attempt_count = attempt.attempt_number;
        original.next_attempt_at = None;
        let mut started = attempt.clone();
        started.status = pioneer_protocol::TaskDeliveryAttemptStatus::Started;
        started.completed_at = None;
        started.error = None;
        assert!(matches!(
            processor
                .task_runtime
                .background_control_service()
                .fail_delivery(
                    original,
                    started,
                    "task_delivery_execution_timed_out".to_owned(),
                    None,
                    None,
                    RACE_TIME + 2,
                )
                .await
                .unwrap(),
            TaskDeliveryTransitionOutcome::Superseded
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_delivery_cancellation_lost_start_skips_execution_and_continues_worker() {
    cancellation_worker_case(TaskDeliveryCommitTestKind::Start, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_delivery_cancellation_late_success_continues_worker_without_refinalization() {
    cancellation_worker_case(TaskDeliveryCommitTestKind::Finish, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_delivery_cancellation_outer_execution_failure_continues_worker() {
    cancellation_worker_case(TaskDeliveryCommitTestKind::Finish, true, false).await;
}

#[tokio::test]
async fn task_delivery_cancellation_execution_timeout_continues_worker_without_refinalization() {
    cancellation_worker_case(TaskDeliveryCommitTestKind::Finish, false, true).await;
}
