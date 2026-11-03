use super::*;
use pioneer_crud::{TaskDeliveryCommitTestKind, TaskDeliveryTransitionOutcome};

const RACE_TIME: i64 = 4_300_000_000;

async fn race_processor() -> (tempfile::TempDir, Arc<MessageProcessor>, String) {
    let (directory, processor, workspace, _) = clock_delivery_processor().await;
    (directory, processor, workspace)
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

#[tokio::test]
async fn recovery_partial_failure_does_not_hide_independent_pending_delivery() {
    let (_directory, processor, workspace) = race_processor().await;
    let poison =
        queue_worker_delivery(&processor, &workspace, "thr_race_delivery", RACE_TIME).await;
    let pending =
        queue_worker_delivery(&processor, &workspace, "thr_race_delivery", RACE_TIME + 1).await;
    let TaskDeliveryTransitionOutcome::Applied((_, attempt)) = processor
        .task_runtime
        .background_control_service()
        .start_delivery(&poison.id, RACE_TIME)
        .await
        .unwrap()
    else {
        panic!("fixture start lost authority")
    };
    let database = processor.crud_store.database_connection();
    database
        .execute_raw(sea_orm::Statement::from_sql_and_values(
            sea_orm::DbBackend::Sqlite,
            "DELETE FROM task_delivery_attempt WHERE id=?",
            [attempt.id.into()],
        ))
        .await
        .unwrap();
    // Failure is local to the row whose verified exact attempt is absent.
    database.execute_unprepared("CREATE TRIGGER reject_missing_recovery_retry BEFORE INSERT ON task_delivery_recovery_retry WHEN NEW.expected_attempt_id IS NULL BEGIN SELECT RAISE(ABORT,'test row-local failure'); END").await.unwrap();
    processor
        .process_due_task_deliveries(RACE_TIME + 301, 64)
        .await
        .unwrap();
    assert_eq!(
        processor
            .crud_store
            .get_task_delivery(&pending.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        pioneer_protocol::TaskDeliveryStatus::Delivered
    );
    let source = processor
        .crud_store
        .with_maintenance_access()
        .task_delivery_recovery_snapshot(&poison.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source.delivery.status, "delivering");
    assert!(source.attempt.is_none());
    assert!(
        source.retry.is_none(),
        "failed deferral must remain visible as undurable"
    );
    let report = processor
        .task_runtime
        .background_control_service()
        .recover_stuck_deliveries(RACE_TIME + 302, 64)
        .await
        .unwrap();
    assert!(report.cooling_down);
    assert_eq!(report.selected, 0);
}

async fn clock_delivery_processor() -> (
    tempfile::TempDir,
    Arc<MessageProcessor>,
    String,
    Arc<NativeSchedulingObserver>,
) {
    let observer = Arc::new(NativeSchedulingObserver::default());
    let (directory, workspaces, store, workspace_id) =
        setup_pooled_file_workspace_manager_with_observers(
            Some(observer.clone()),
            Some(observer.clone()),
        )
        .await;
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
    (directory, processor, workspace_id, observer)
}

// Keep the SQLite thread independent of virtual timer advancement.
fn hold_delivery_clock() -> tokio::task::JoinHandle<()> {
    tokio::time::pause();
    tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    })
}
async fn lifecycle_publication(processor: &MessageProcessor) {
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn lifecycle_owner_fractional_grace_matches_real_recovery_source() {
    use pioneer_entity::task_delivery as delivery;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, sea_query::Expr};
    for (fraction_ms, retry_delay) in [(0, 0), (500, 0), (500, 2)] {
        let (_directory, processor, workspace, observer) = clock_delivery_processor().await;
        let now = now_timestamp_secs();
        let created = now - 300;
        let row = queue_worker_delivery(&processor, &workspace, "thr_race_delivery", created).await;
        let TaskDeliveryTransitionOutcome::Applied((_, original)) = processor
            .task_runtime
            .background_control_service()
            .start_delivery(&row.id, created)
            .await
            .unwrap()
        else {
            panic!("typed start must own an actual attempt")
        };
        let db = processor.crud_store.database_connection();
        // Historical fractional persistence is the input under test; all
        // attempt/occurrence authority comes from start_delivery above.
        delivery::Entity::update_many()
            .col_expr(
                delivery::Column::UpdatedAt,
                Expr::val(
                    pioneer_crud::unix_to_datetime(created)
                        + chrono::Duration::milliseconds(fraction_ms),
                ),
            )
            .filter(delivery::Column::Id.eq(&row.id))
            .exec(&db)
            .await
            .unwrap();
        if retry_delay != 0 {
            let snapshot = processor
                .crud_store
                .with_maintenance_access()
                .task_delivery_recovery_snapshot(&row.id)
                .await
                .unwrap()
                .unwrap();
            // Supported exact-snapshot deferral, not a fabricated retry ticket.
            assert!(
                processor
                    .crud_store
                    .with_maintenance_access()
                    .defer_task_delivery_recovery(&snapshot, &|| now - 3)
                    .await
                    .unwrap()
            );
        }
        let due = now + i64::from(fraction_ms != 0).max(retry_delay);
        let wake = processor.crud_store.background_work("tasks.lifecycle");
        wake.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
        processor.arm_completed_history_preparation_barrier("__task_lifecycle_before_wait__");
        let worker = tokio::spawn(MessageProcessor::run_task_lifecycle_resilience_worker(
            Arc::downgrade(&processor),
        ));
        lifecycle_publication(&processor).await;
        if due > now {
            assert_eq!(
                processor
                    .crud_store
                    .get_task_delivery(&row.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                pioneer_protocol::TaskDeliveryStatus::Delivering
            );
            let deadlines = processor
                .crud_store
                .task_lifecycle_deadlines(now)
                .await
                .unwrap();
            assert_eq!(deadlines.delivery_recovery, Some(due));
            let guard = hold_delivery_clock();
            observer.reads.lock().unwrap().clear();
            observer.writes.lock().unwrap().clear();
            processor.release_completed_history_preparation_barrier();
            tokio::time::advance(Duration::from_millis(900)).await;
            for _ in 0..128 {
                tokio::task::yield_now().await;
            }
            assert!(
                observer.reads.lock().unwrap().is_empty(),
                "no successful-Empty SQL spin before authoritative grace"
            );
            assert!(observer.writes.lock().unwrap().is_empty());
            processor.arm_completed_history_preparation_barrier("__task_lifecycle_before_wait__");
            wake.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(due as u64));
            // Cross the actual wait's original deadline, without an input hint.
            tokio::time::advance(Duration::from_secs((due - now) as u64)).await;
            lifecycle_publication(&processor).await;
            guard.abort();
            tokio::time::resume();
        }
        assert_eq!(
            processor
                .crud_store
                .get_task_delivery(&row.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            pioneer_protocol::TaskDeliveryStatus::Delivered
        );
        let first = processor
            .crud_store
            .get_task_delivery_attempt(&row.id, original.attempt_number)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            first.status,
            pioneer_protocol::TaskDeliveryAttemptStatus::Failed
        );
        worker.abort();
        let _ = worker.await;
    }
}

#[tokio::test]
async fn lifecycle_owner_retains_real_delivery_cooldown_with_independent_wall_work() {
    use pioneer_entity::task_delivery as delivery;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, sea_query::Expr};
    for forward in [false, true] {
        let (_directory, processor, workspace, _observer) = clock_delivery_processor().await;
        let now = now_timestamp_secs();
        // Remains genuinely stale even after a one-hour wall rollback.
        let poison =
            queue_worker_delivery(&processor, &workspace, "thr_race_delivery", now - 7200).await;
        let pending =
            queue_worker_delivery(&processor, &workspace, "thr_race_delivery", now - 7199).await;
        let TaskDeliveryTransitionOutcome::Applied((_, attempt)) = processor
            .task_runtime
            .background_control_service()
            .start_delivery(&poison.id, now - 7200)
            .await
            .unwrap()
        else {
            panic!("actual start authority")
        };
        let db = processor.crud_store.database_connection();
        delivery::Entity::update_many()
            .col_expr(
                delivery::Column::NextAttemptAt,
                Expr::val(Some(pioneer_crud::unix_to_datetime(now + 2))),
            )
            .filter(delivery::Column::Id.eq(&pending.id))
            .exec(&db)
            .await
            .unwrap();
        // Real healthy attempt; fail its recovery transaction and the durable
        // error deferral once. Removing both faults restores useful recovery.
        db.execute_unprepared("CREATE TRIGGER clock_reject_attempt BEFORE UPDATE ON task_delivery_attempt BEGIN SELECT RAISE(ABORT,'clock recovery storage failure'); END").await.unwrap();
        db.execute_unprepared("CREATE TRIGGER clock_reject_retry BEFORE INSERT ON task_delivery_recovery_retry BEGIN SELECT RAISE(ABORT,'clock fixture storage failure'); END").await.unwrap();
        let wake = processor.crud_store.background_work("tasks.lifecycle");
        wake.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
        let guard = hold_delivery_clock();
        processor
            .process_due_task_deliveries(now, 64)
            .await
            .unwrap();
        assert!(
            processor
                .task_runtime
                .background_control_service()
                .delivery_recovery_retry_deadline()
                .is_some()
        );
        // Recover storage BEFORE the owner successfully publishes its cooling
        // quantum; no SQL or durable producer occurs after that publication.
        db.execute_unprepared("DROP TRIGGER clock_reject_attempt")
            .await
            .unwrap();
        db.execute_unprepared("DROP TRIGGER clock_reject_retry")
            .await
            .unwrap();
        processor.arm_completed_history_preparation_barrier("__task_lifecycle_before_wait__");
        let worker = tokio::spawn(MessageProcessor::run_task_lifecycle_resilience_worker(
            Arc::downgrade(&processor),
        ));
        lifecycle_publication(&processor).await;
        assert!(
            processor
                .task_runtime
                .background_control_service()
                .delivery_recovery_retry_deadline()
                .is_some()
        );
        assert_eq!(
            processor
                .crud_store
                .task_lifecycle_deadlines(now)
                .await
                .unwrap()
                .delivery_pending
                .unwrap()
                .timestamp(),
            now + 2
        );
        processor.release_completed_history_preparation_barrier();
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        let moved = if forward { now + 3600 } else { now - 3600 };
        wake.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(moved as u64));
        if forward {
            // Force Wake's pending wall wait to resample before the retry is due.
            // An original two-second wall deadline is its next sample.
            tokio::time::advance(Duration::from_secs(2)).await;
            for _ in 0..128 {
                if processor
                    .crud_store
                    .get_task_delivery(&pending.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status
                    == pioneer_protocol::TaskDeliveryStatus::Delivered
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(
                processor
                    .crud_store
                    .get_task_delivery(&pending.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                pioneer_protocol::TaskDeliveryStatus::Delivered
            );
        } else {
            tokio::time::advance(Duration::from_secs(5)).await;
            for _ in 0..128 {
                if processor
                    .crud_store
                    .get_task_delivery(&poison.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status
                    == pioneer_protocol::TaskDeliveryStatus::Delivered
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(
                processor
                    .crud_store
                    .get_task_delivery(&poison.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                pioneer_protocol::TaskDeliveryStatus::Delivered
            );
            assert_eq!(
                processor
                    .crud_store
                    .get_task_delivery_attempt(&poison.id, attempt.attempt_number)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                pioneer_protocol::TaskDeliveryAttemptStatus::Failed
            );
        }
        worker.abort();
        let _ = worker.await;
        guard.abort();
        tokio::time::resume();
    }
}

#[tokio::test]
async fn lifecycle_owner_reconstructs_future_delivery_pages_and_old_key() {
    let (_directory, processor, workspace, observer) = clock_delivery_processor().await;
    let now = now_timestamp_secs();
    let wake = processor.crud_store.background_work("tasks.lifecycle");
    wake.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
    let mut inputs = Vec::new();
    for index in 0..65 {
        let row =
            queue_worker_delivery(&processor, &workspace, "thr_race_delivery", now + index).await;
        inputs.push(row);
    }
    inputs.sort_by(|a, b| a.id.cmp(&b.id));
    for (index, row) in inputs.iter().enumerate() {
        // Real delivering attempt authority. The tail is stale, all preceding
        // raw keys have future grace; deferred inputs retain max(grace,retry).
        let at = if index == 64 {
            now - 300
        } else {
            now + 100 + index as i64
        };
        assert!(matches!(
            processor
                .task_runtime
                .background_control_service()
                .start_delivery(&row.id, at)
                .await
                .unwrap(),
            TaskDeliveryTransitionOutcome::Applied(_)
        ));
        if index % 2 == 0 && index != 64 {
            let snapshot = processor
                .crud_store
                .with_maintenance_access()
                .task_delivery_recovery_snapshot(&row.id)
                .await
                .unwrap()
                .unwrap();
            assert!(
                processor
                    .crud_store
                    .with_maintenance_access()
                    .defer_task_delivery_recovery(&snapshot, &|| now + 1000)
                    .await
                    .unwrap()
            );
        }
    }
    processor.arm_completed_history_preparation_barrier("__task_lifecycle_delivery_settled__");
    let worker = tokio::spawn(MessageProcessor::run_task_lifecycle_resilience_worker(
        Arc::downgrade(&processor),
    ));
    // This barrier is reached only after the complete raw pass and successful
    // generation-aware publication, never at an intermediate Runnable page.
    lifecycle_publication(&processor).await;
    assert_eq!(
        processor
            .crud_store
            .get_task_delivery(&inputs[64].id)
            .await
            .unwrap()
            .unwrap()
            .status,
        pioneer_protocol::TaskDeliveryStatus::Delivered
    );
    // Complete finite discovery before measuring quiet; nearest is odd-index
    // grace now+401, even-index retries are later (now+1005).
    let deadlines = processor
        .crud_store
        .task_lifecycle_deadlines(now)
        .await
        .unwrap();
    assert_eq!(deadlines.delivery_recovery, Some(now + 401));
    processor.release_completed_history_preparation_barrier();
    let guard = hold_delivery_clock();
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    tokio::time::advance(Duration::from_secs(100)).await;
    for _ in 0..128 {
        tokio::task::yield_now().await;
    }
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    // Supported failure/retry transition for an old key moves the nearest
    // clock EARLIER, from future grace/retry to pending now+60.
    processor.arm_completed_history_preparation_barrier("__task_lifecycle_delivery_settled__");
    let old = processor
        .crud_store
        .get_task_delivery(&inputs[0].id)
        .await
        .unwrap()
        .unwrap();
    let attempt = processor
        .crud_store
        .get_task_delivery_attempt(&old.id, 1)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        processor
            .task_runtime
            .background_control_service()
            .fail_delivery(old, attempt, "storage_error".to_owned(), None, None, now,)
            .await
            .unwrap(),
        TaskDeliveryTransitionOutcome::Applied(_)
    ));
    lifecycle_publication(&processor).await;
    let deadlines = processor
        .crud_store
        .task_lifecycle_deadlines(now)
        .await
        .unwrap();
    assert_eq!(
        deadlines.delivery_pending.map(|at| at.timestamp()),
        Some(now + 60)
    );
    processor.release_completed_history_preparation_barrier();
    processor.arm_completed_history_preparation_barrier("__task_lifecycle_delivery_settled__");
    wake.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs((now + 60) as u64));
    tokio::time::advance(Duration::from_secs(60)).await;
    lifecycle_publication(&processor).await;
    assert_eq!(
        processor
            .crud_store
            .get_task_delivery(&inputs[0].id)
            .await
            .unwrap()
            .unwrap()
            .status,
        pioneer_protocol::TaskDeliveryStatus::Delivered
    );
    processor.release_completed_history_preparation_barrier();
    processor.arm_completed_history_preparation_barrier("__task_lifecycle_delivery_settled__");
    wake.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs((now + 401) as u64));
    tokio::time::advance(Duration::from_secs(401)).await;
    lifecycle_publication(&processor).await;
    assert_eq!(
        processor
            .crud_store
            .get_task_delivery(&inputs[1].id)
            .await
            .unwrap()
            .unwrap()
            .status,
        pioneer_protocol::TaskDeliveryStatus::Delivered
    );
    guard.abort();
    worker.abort();
    let _ = worker.await;
    tokio::time::resume();
}
