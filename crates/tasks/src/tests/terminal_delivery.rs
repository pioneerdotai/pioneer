use super::*;
use pioneer_crud::{TaskTerminalCommitStatus, TaskTerminalConflict};
use pioneer_protocol::{TaskDelivery, TaskDeliveryAttempt, TaskDeliveryAttemptStatus};
use sea_orm::{ColumnTrait, QueryFilter, TransactionTrait};

async fn fixture_with_runtime(
    runtime: TaskRuntime,
) -> (TaskRuntime, TaskRun, TaskExecutionHandle, TaskResult) {
    let mut params = create_params(TaskTriggerSpec::Immediate);
    params.owner_kind = TaskOwnerKind::Thread;
    params.owner_id = Some("thr_terminal_delivery".into());
    params.created_by_thread_id = params.owner_id.clone();
    params.delivery_policy = Some(TaskDeliveryPolicy {
        mode: TaskDeliveryMode::Thread,
        thread_target: Some(pioneer_protocol::TaskDeliveryThreadTarget::OriginThread),
        thread_id: params.owner_id.clone(),
        webhook_url: None,
        include_result: true,
        format: pioneer_protocol::TaskDeliveryFormat::FullResult,
    });
    let response = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .unwrap();
    let run = response.run.unwrap();
    let store = runtime.service().store();
    store
        .claim_task_run_for_dispatch(&run.id, run.created_at)
        .await
        .unwrap()
        .unwrap();
    store
        .claim_task_run_execution_for_dispatch(
            &run.id,
            TaskExecutorKind::System,
            "terminal-test",
            run.created_at,
            run.created_at + 60,
        )
        .await
        .unwrap()
        .unwrap();
    let handle = TaskExecutionHandle::new(
        store,
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    );
    let result = TaskResult {
        summary: Some("same result".into()),
        data: Some(TaskValue::Integer(42)),
        artifacts: Vec::new(),
        completed_by_run_id: Some(run.id.clone()),
    };
    (runtime, run, handle, result)
}

async fn fixture() -> (TaskRuntime, TaskRun, TaskExecutionHandle, TaskResult) {
    fixture_with_runtime(runtime().await).await
}

fn completed(run: &TaskRun, result: &TaskResult, at: i64) -> TaskEventPayload {
    TaskEventPayload::RunCompleted {
        task_id: run.task_id.clone(),
        run_id: run.id.clone(),
        result: Some(result.clone()),
        completed_at: at,
    }
}

async fn deliveries(runtime: &TaskRuntime, run: &TaskRun) -> Vec<TaskDelivery> {
    runtime
        .service()
        .store()
        .list_task_deliveries(TaskDeliveriesParams {
            workspace_id: TEST_WORKSPACE_ID.into(),
            task_id: Some(run.task_id.clone()),
            run_id: Some(run.id.clone()),
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .unwrap()
        .deliveries
}

async fn events(runtime: &TaskRuntime, run: &TaskRun) -> Vec<pioneer_protocol::TaskEvent> {
    runtime
        .service()
        .store()
        .get_task_events(&run.task_id, None)
        .await
        .unwrap()
        .events
}

#[tokio::test]
async fn overlapping_preparations_converge_to_one_delivery_and_original_id() {
    let (runtime, run, mut first, result) = fixture().await;
    // Independently reconstructed handles share ONLY a test barrier. Each
    // reaches it after allocating a distinct delivery id and serializing its
    // full batch, before either can acquire the writer transaction.
    let barrier = Arc::new(Barrier::new(2));
    first.terminal_preparation_barrier = Some(barrier.clone());
    let mut second = TaskExecutionHandle::new(
        runtime.service().store(),
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    );
    second.terminal_preparation_barrier = Some(barrier);
    let at = run.created_at + 10;
    let (a, b) = timeout(Duration::from_secs(5), async {
        tokio::join!(
            first.complete_run(Some(result.clone()), at),
            second.complete_run(Some(result.clone()), at + 1)
        )
    })
    .await
    .expect("both preparations must reach the barrier");
    a.unwrap();
    b.unwrap();
    let rows = deliveries(&runtime, &run).await;
    assert_eq!(rows.len(), 1);
    let delivery = &rows[0];
    let log = events(&runtime, &run).await;
    let queued: Vec<_> = log
        .iter()
        .filter_map(|e| match &e.payload {
            TaskEventPayload::DeliveryQueued { delivery } => Some(delivery),
            _ => None,
        })
        .collect();
    assert_eq!(queued, vec![delivery]);
    assert_eq!(
        log.iter()
            .filter(|e| matches!(e.payload, TaskEventPayload::RunCompleted { .. }))
            .count(),
        1
    );
    let db = runtime.service().store().database_connection();
    let authorities = pioneer_entity::task_delivery_authority::Entity::find()
        .filter(pioneer_entity::task_delivery_authority::Column::RunId.eq(run.id.clone()))
        .all(&db)
        .await
        .unwrap();
    assert_eq!(authorities.len(), 1);
    assert_eq!(authorities[0].delivery_id, delivery.id);
    assert_eq!(authorities[0].idempotency_key, delivery.delivery_key);
    let execution = runtime
        .service()
        .store()
        .load_execution_for_run(&run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.status, TaskRunExecutionStatus::Succeeded);
    assert_eq!(execution.completed_at, Some(delivery.created_at));
    assert_eq!(
        runtime
            .service()
            .store()
            .get_task_occurrence_contract_by_run(&run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskOccurrenceStatus::Delivered
    );
}

#[tokio::test]
async fn reconstructed_runtime_replay_repairs_interrupted_post_commit_finalization() {
    let (runtime, run, _, result) = fixture().await;
    let store = runtime.service().store();
    let at = run.created_at + 10;
    // Seed a durable terminal batch, then reproduce the old version's partial
    // boundary using fixture-only execution/occurrence mutations. New commits
    // finalize all of these rows atomically.
    let prepared = store
        .prepare_task_terminal_transition(completed(&run, &result, at))
        .await
        .unwrap();
    let committed = store
        .commit_task_terminal_transition(prepared)
        .await
        .unwrap();
    assert_eq!(committed.status, TaskTerminalCommitStatus::Applied);
    let original = deliveries(&runtime, &run).await;
    let db = store.database_connection();
    db.execute(&Statement::from_sql_and_values(DatabaseBackend::Sqlite, "UPDATE task_run_execution SET status='running',result_json=NULL,error_json=NULL,completed_at=NULL WHERE task_run_id=?", [run.id.clone().into()])).await.unwrap();
    db.execute(&Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE task_occurrence_contract SET status='running' WHERE run_id=?",
        [run.id.clone().into()],
    ))
    .await
    .unwrap();
    assert!(
        !store
            .load_execution_for_run(&run.id)
            .await
            .unwrap()
            .unwrap()
            .status
            .is_terminal()
    );
    let recovered_runtime = TaskRuntime::new(store.clone());
    let handle = TaskExecutionHandle::new(
        store.clone(),
        recovered_runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    );
    handle.complete_run(Some(result), at + 100).await.unwrap();
    assert_eq!(deliveries(&recovered_runtime, &run).await, original);
    let execution = store
        .load_execution_for_run(&run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.status, TaskRunExecutionStatus::Succeeded);
    assert_eq!(execution.completed_at, Some(at));
    assert_eq!(
        store
            .get_task_occurrence_contract_by_run(&run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskOccurrenceStatus::Delivered
    );
    // Repair also runs when execution was already finalized but occurrence
    // was not: a second successful repeat is not an early terminal return.
    let before = events(&recovered_runtime, &run).await;
    store
        .database_connection()
        .execute(&Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE task_occurrence_contract SET status='running' WHERE run_id=?",
            [run.id.clone().into()],
        ))
        .await
        .unwrap();
    handle
        .complete_run(execution.result, at + 200)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_task_occurrence_contract_by_run(&run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskOccurrenceStatus::Delivered
    );
    assert_eq!(events(&recovered_runtime, &run).await, before);
}

#[tokio::test]
async fn replay_preserves_all_progressed_delivery_states_attempts_and_receipts() {
    for status in [
        TaskDeliveryStatus::Delivering,
        TaskDeliveryStatus::Delivered,
        TaskDeliveryStatus::Failed,
        TaskDeliveryStatus::Cancelled,
    ] {
        let (runtime, run, handle, result) = fixture().await;
        let at = run.created_at + 10;
        handle.complete_run(Some(result.clone()), at).await.unwrap();
        let store = runtime.service().store();
        let mut delivery = deliveries(&runtime, &run).await.remove(0);
        delivery.status = TaskDeliveryStatus::Delivering;
        delivery.attempt_count = 1;
        delivery.next_attempt_at = None;
        delivery.updated_at = at + 1;
        let mut attempt = TaskDeliveryAttempt {
            id: format!("attempt_{}", delivery.id),
            delivery_id: delivery.id.clone(),
            attempt_number: 1,
            status: TaskDeliveryAttemptStatus::Started,
            started_at: at + 1,
            completed_at: None,
            http_status: None,
            error: None,
            response_fingerprint: None,
        };
        store
            .append_task_event(
                TaskEventPayload::DeliveryStarted {
                    delivery: delivery.clone(),
                    attempt: attempt.clone(),
                },
                at + 1,
            )
            .await
            .unwrap();
        if status != TaskDeliveryStatus::Delivering {
            delivery.status = status;
            delivery.updated_at = at + 2;
            attempt.completed_at = Some(at + 2);
            let event = match status {
                TaskDeliveryStatus::Delivered => {
                    delivery.delivered_turn_id = Some("exact_destination_receipt".into());
                    delivery.delivered_at = Some(at + 2);
                    attempt.status = TaskDeliveryAttemptStatus::Delivered;
                    TaskEventPayload::DeliveryDelivered {
                        delivery: delivery.clone(),
                        attempt: attempt.clone(),
                    }
                }
                TaskDeliveryStatus::Failed => {
                    delivery.last_error = Some("permanent delivery error".into());
                    attempt.status = TaskDeliveryAttemptStatus::Failed;
                    attempt.error = delivery.last_error.clone();
                    TaskEventPayload::DeliveryFailed {
                        delivery: delivery.clone(),
                        attempt: attempt.clone(),
                    }
                }
                TaskDeliveryStatus::Cancelled => {
                    attempt.status = TaskDeliveryAttemptStatus::Failed;
                    attempt.error = Some("cancelled".into());
                    TaskEventPayload::DeliveryCancelled {
                        delivery: delivery.clone(),
                        attempt: Some(attempt.clone()),
                        reason: Some("cancelled".into()),
                    }
                }
                _ => unreachable!(),
            };
            store.append_task_event(event, at + 2).await.unwrap();
        }
        let db = store.database_connection();
        let authority_before =
            pioneer_entity::task_delivery_authority::Entity::find_by_id(delivery.id.clone())
                .one(&db)
                .await
                .unwrap()
                .unwrap();
        let attempts_before = pioneer_entity::task_delivery_attempt::Entity::find()
            .filter(
                pioneer_entity::task_delivery_attempt::Column::DeliveryId.eq(delivery.id.clone()),
            )
            .all(&db)
            .await
            .unwrap();
        let log_before = events(&runtime, &run).await;
        let fresh = TaskExecutionHandle::new(
            store,
            runtime.event_bus(),
            run.task_id.clone(),
            run.id.clone(),
        );
        fresh.complete_run(Some(result), at + 99).await.unwrap();
        assert_eq!(deliveries(&runtime, &run).await, vec![delivery]);
        assert_eq!(events(&runtime, &run).await, log_before);
        assert_eq!(
            pioneer_entity::task_delivery_authority::Entity::find_by_id(
                authority_before.delivery_id.clone()
            )
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
            authority_before
        );
        assert_eq!(
            pioneer_entity::task_delivery_attempt::Entity::find_by_id(attempt.id)
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            attempts_before[0]
        );
    }
}

#[tokio::test]
async fn different_result_or_terminal_outcome_is_a_conflict_without_partial_events() {
    let (runtime, run, mut first, result) = fixture().await;
    let barrier = Arc::new(Barrier::new(2));
    first.terminal_preparation_barrier = Some(barrier.clone());
    let mut second = TaskExecutionHandle::new(
        runtime.service().store(),
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    );
    second.terminal_preparation_barrier = Some(barrier);
    let mut changed = result.clone();
    changed.summary = Some("different result".into());
    let at = run.created_at + 10;
    let (a, b) = timeout(Duration::from_secs(5), async {
        tokio::join!(
            first.complete_run(Some(result.clone()), at),
            second.complete_run(Some(changed), at)
        )
    })
    .await
    .unwrap();
    assert_ne!(a.is_ok(), b.is_ok());
    assert!(
        (a.err().or(b.err()).unwrap())
            .downcast_ref::<TaskTerminalConflict>()
            .is_some()
    );
    assert_eq!(deliveries(&runtime, &run).await.len(), 1);
    let before = events(&runtime, &run).await;
    let fresh = TaskExecutionHandle::new(
        runtime.service().store(),
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    );
    let error = fresh
        .cancel_run(Some("incompatible cancellation".into()), at + 1)
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<TaskTerminalConflict>().is_some());
    assert_eq!(events(&runtime, &run).await, before);
}

#[tokio::test]
async fn incompatible_complete_and_block_compete_under_the_same_writer_fence() {
    let (runtime, run, mut first, result) = fixture().await;
    let barrier = Arc::new(Barrier::new(2));
    first.terminal_preparation_barrier = Some(barrier.clone());
    let mut second = TaskExecutionHandle::new(
        runtime.service().store(),
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    );
    second.terminal_preparation_barrier = Some(barrier);
    let at = run.created_at + 10;
    let (a, b) = timeout(Duration::from_secs(5), async {
        tokio::join!(
            first.complete_run(Some(result), at),
            second.block_run(None, at)
        )
    })
    .await
    .unwrap();
    assert_ne!(a.is_ok(), b.is_ok());
    assert!(
        a.err()
            .or(b.err())
            .unwrap()
            .downcast_ref::<TaskTerminalConflict>()
            .is_some()
    );
    let status = runtime
        .service()
        .store()
        .get_task_run(&run.id)
        .await
        .unwrap()
        .unwrap()
        .status;
    assert!(matches!(
        status,
        TaskRunStatus::Succeeded | TaskRunStatus::Blocked
    ));
    assert_eq!(deliveries(&runtime, &run).await.len(), 1);
    assert_eq!(
        events(&runtime, &run)
            .await
            .iter()
            .filter(|e| matches!(
                e.payload,
                TaskEventPayload::RunCompleted { .. } | TaskEventPayload::RunBlocked { .. }
            ))
            .count(),
        1
    );
}

#[tokio::test]
async fn stale_destination_authority_or_generation_rolls_back_the_terminal_batch() {
    for changed_fact in [
        "destination",
        "authority",
        "generation",
        "retry_generation",
        "trigger",
    ] {
        let (runtime, run, _, result) = fixture().await;
        let store = runtime.service().store();
        let prepared = store
            .prepare_task_terminal_transition(completed(&run, &result, run.created_at + 10))
            .await
            .unwrap();
        let before = events(&runtime, &run).await;
        let db = store.database_connection();
        // Fixture-only mutations force the exact read/write race, even for
        // facts whose normal repository update path is immutable.
        match changed_fact {
            "destination" => {
                let mut occurrence = store
                    .get_task_occurrence_contract_by_run(&run.id)
                    .await
                    .unwrap()
                    .unwrap();
                let plan = occurrence.delivery_plan.as_mut().unwrap();
                plan.policy.thread_id = Some("another_destination".into());
                plan.presentation_thread_id = plan.policy.thread_id.clone();
                db.execute(&Statement::from_sql_and_values(
                    DatabaseBackend::Sqlite,
                    "UPDATE task_occurrence_contract SET delivery_plan_json=? WHERE run_id=?",
                    [
                        serde_json::to_string(&occurrence.delivery_plan.unwrap())
                            .unwrap()
                            .into(),
                        run.id.clone().into(),
                    ],
                ))
                .await
                .unwrap();
            }
            "authority" => {
                let mut actor = store
                    .get_task_actor_contract(&run.task_id)
                    .await
                    .unwrap()
                    .unwrap();
                actor.delivery.disclosure_generation += 1;
                db.execute(&Statement::from_sql_and_values(
                    DatabaseBackend::Sqlite,
                    "UPDATE task_actor_contract SET delivery_json=? WHERE task_id=?",
                    [
                        serde_json::to_string(&actor.delivery).unwrap().into(),
                        run.task_id.clone().into(),
                    ],
                ))
                .await
                .unwrap();
            }
            "generation" => {
                db.execute(&Statement::from_sql_and_values(DatabaseBackend::Sqlite, "UPDATE task_occurrence_contract SET execution_generation=execution_generation+1 WHERE run_id=?", [run.id.clone().into()])).await.unwrap();
            }
            "retry_generation" => {
                db.execute(&Statement::from_sql_and_values(DatabaseBackend::Sqlite, "UPDATE task_occurrence_contract SET retry_attempt=retry_attempt+1 WHERE run_id=?", [run.id.clone().into()])).await.unwrap();
            }
            "trigger" => {
                db.execute(&Statement::from_sql_and_values(
                    DatabaseBackend::Sqlite,
                    "UPDATE task_trigger SET status='paused' WHERE id=?",
                    [run.trigger_id.clone().unwrap().into()],
                ))
                .await
                .unwrap();
            }
            _ => unreachable!(),
        }
        let error = store
            .commit_task_terminal_transition(prepared)
            .await
            .unwrap_err();
        assert!(
            error.downcast_ref::<TaskTerminalConflict>().is_some(),
            "{changed_fact}: {error:#}"
        );
        assert_eq!(events(&runtime, &run).await, before);
        assert!(deliveries(&runtime, &run).await.is_empty());
        assert!(
            !store
                .get_task_run(&run.id)
                .await
                .unwrap()
                .unwrap()
                .status
                .is_terminal()
        );
    }
}

#[tokio::test]
async fn authority_projection_failure_rolls_back_run_task_events_and_delivery() {
    let (runtime, run, handle, result) = fixture().await;
    let db = runtime.service().store().database_connection();
    let before = events(&runtime, &run).await;
    // Fail after the delivery INSERT but before its authority INSERT. The
    // entire terminal batch, including its earlier projections, must roll back.
    db.execute_unprepared("CREATE TRIGGER reject_terminal_authority BEFORE INSERT ON task_delivery_authority BEGIN SELECT RAISE(ABORT, 'injected authority failure'); END").await.unwrap();
    assert!(
        handle
            .complete_run(Some(result.clone()), run.created_at + 10)
            .await
            .is_err()
    );
    assert_eq!(events(&runtime, &run).await, before);
    assert!(deliveries(&runtime, &run).await.is_empty());
    assert!(
        !runtime
            .service()
            .store()
            .get_task_run(&run.id)
            .await
            .unwrap()
            .unwrap()
            .status
            .is_terminal()
    );
    assert!(
        !runtime
            .service()
            .store()
            .get_task(&run.task_id)
            .await
            .unwrap()
            .unwrap()
            .task
            .status
            .is_terminal()
    );
    assert!(
        pioneer_entity::task_delivery_authority::Entity::find()
            .filter(pioneer_entity::task_delivery_authority::Column::RunId.eq(run.id.clone()))
            .all(&db)
            .await
            .unwrap()
            .is_empty()
    );
    db.execute_unprepared("DROP TRIGGER reject_terminal_authority")
        .await
        .unwrap();
    handle
        .complete_run(Some(result), run.created_at + 11)
        .await
        .unwrap();
    assert_eq!(deliveries(&runtime, &run).await.len(), 1);
}

#[tokio::test]
async fn already_committed_delivery_rejects_changed_destination_and_authority() {
    for fact in ["destination", "authority"] {
        let (runtime, run, handle, result) = fixture().await;
        let at = run.created_at + 10;
        handle.complete_run(Some(result.clone()), at).await.unwrap();
        let store = runtime.service().store();
        let before = events(&runtime, &run).await;
        let delivery_before = deliveries(&runtime, &run).await;
        let db = store.database_connection();
        if fact == "destination" {
            let mut occurrence = store
                .get_task_occurrence_contract_by_run(&run.id)
                .await
                .unwrap()
                .unwrap();
            let plan = occurrence.delivery_plan.as_mut().unwrap();
            plan.policy.thread_id = Some("different_destination".into());
            plan.presentation_thread_id = plan.policy.thread_id.clone();
            db.execute(&Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "UPDATE task_occurrence_contract SET delivery_plan_json=? WHERE run_id=?",
                [
                    serde_json::to_string(plan).unwrap().into(),
                    run.id.clone().into(),
                ],
            ))
            .await
            .unwrap();
        } else {
            let mut actor = store
                .get_task_actor_contract(&run.task_id)
                .await
                .unwrap()
                .unwrap();
            actor.delivery.disclosure_generation += 1;
            db.execute(&Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "UPDATE task_actor_contract SET delivery_json=? WHERE task_id=?",
                [
                    serde_json::to_string(&actor.delivery).unwrap().into(),
                    run.task_id.clone().into(),
                ],
            ))
            .await
            .unwrap();
        }
        let fresh = TaskExecutionHandle::new(
            store,
            runtime.event_bus(),
            run.task_id.clone(),
            run.id.clone(),
        );
        assert!(
            fresh.complete_run(Some(result), at + 1).await.is_err(),
            "{fact}"
        );
        assert_eq!(events(&runtime, &run).await, before);
        assert_eq!(deliveries(&runtime, &run).await, delivery_before);
    }
}

#[tokio::test]
async fn concurrent_failure_reuses_one_retry_and_does_not_finalize_the_occurrence() {
    let (runtime, run, mut first, _) = fixture().await;
    let store = runtime.service().store();
    let policy = TaskRetryPolicy {
        max_attempts: 2,
        backoff: TaskRetryBackoffKind::Fixed,
        initial_delay_seconds: Some(5),
        max_delay_seconds: None,
        retry_on: vec![TaskErrorClass::Internal],
    };
    store
        .database_connection()
        .execute(&Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE task SET retry_policy_json=? WHERE id=?",
            [
                serde_json::to_string(&policy).unwrap().into(),
                run.task_id.clone().into(),
            ],
        ))
        .await
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    first.terminal_preparation_barrier = Some(barrier.clone());
    let mut second = TaskExecutionHandle::new(
        store.clone(),
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    );
    second.terminal_preparation_barrier = Some(barrier);
    let error = TaskError {
        recovery_diagnostic: None,
        code: "retryable".into(),
        message: "retryable failure".into(),
        class: TaskErrorClass::Internal,
        details: None,
        failed_run_id: Some(run.id.clone()),
    };
    let at = run.created_at + 10;
    let (a, b) = timeout(Duration::from_secs(5), async {
        tokio::join!(
            first.fail_run(Some(error.clone()), at),
            second.fail_run(Some(error.clone()), at + 1)
        )
    })
    .await
    .unwrap();
    a.unwrap();
    b.unwrap();
    let response = store.get_task(&run.task_id).await.unwrap().unwrap();
    assert_eq!(response.runs.len(), 2);
    assert_eq!(response.task.status, TaskStatus::Queued);
    let retry = response
        .runs
        .iter()
        .find(|r| r.retry_of_run_id.as_deref() == Some(run.id.as_str()))
        .unwrap();
    assert_eq!(retry.attempt_number, 2);
    assert!(deliveries(&runtime, &run).await.is_empty());
    assert!(
        !store
            .load_execution_for_run(&run.id)
            .await
            .unwrap()
            .unwrap()
            .status
            .is_terminal()
    );
    let before = events(&runtime, &run).await;
    TaskExecutionHandle::new(
        store,
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    )
    .fail_run(Some(error), at + 99)
    .await
    .unwrap();
    assert_eq!(events(&runtime, &run).await, before);
}

#[tokio::test]
async fn terminal_preparation_and_commit_keep_maintenance_reads_and_critical_writer_scope() {
    use pioneer_sqlite::{SqliteDatabase, SqliteReadClass, SqliteWriteClass, SqliteWriteExecutor};
    let connection = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&connection, None).await.unwrap();
    seed_task_test_workspace(&connection).await;
    let observer = Arc::new(DispatchDatabaseObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        connection.clone(),
        SqliteWriteExecutor::with_observer(connection, observer.clone()),
        observer.clone(),
    );
    let (runtime, run, _, result) =
        fixture_with_runtime(TaskRuntime::new(Arc::new(CrudStore::new(database)))).await;
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    let store = runtime
        .service()
        .store()
        .with_maintenance_reads_and_critical_writes();
    let prepared = store
        .prepare_task_terminal_transition(completed(&run, &result, run.created_at + 10))
        .await
        .unwrap();
    assert!(
        observer.writes.lock().unwrap().is_empty(),
        "preparation must use readers only"
    );
    store
        .commit_task_terminal_transition(prepared)
        .await
        .unwrap();
    assert_eq!(
        observer.writes.lock().unwrap().len(),
        1,
        "the whole batch must use one writer transaction"
    );
    observer.assert_scope(SqliteReadClass::Maintenance, SqliteWriteClass::Critical);
    let retry = store
        .prepare_task_terminal_transition(completed(&run, &result, run.created_at + 99))
        .await
        .unwrap();
    assert_eq!(
        store
            .commit_task_terminal_transition(retry)
            .await
            .unwrap()
            .status,
        TaskTerminalCommitStatus::Replayed
    );
    assert_eq!(
        observer.writes.lock().unwrap().len(),
        1,
        "the whole batch must use one writer transaction"
    );
    observer.assert_scope(SqliteReadClass::Maintenance, SqliteWriteClass::Critical);
}

#[derive(Default)]
struct TerminalQueueObserver {
    armed: std::sync::atomic::AtomicBool,
    enqueued: Notify,
    cancelled: Notify,
    cancelled_queue: std::sync::Mutex<Option<pioneer_sqlite::SqliteWriteQueueSnapshot>>,
}
impl pioneer_sqlite::SqliteWriteObserver for TerminalQueueObserver {
    fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        match event {
            pioneer_sqlite::SqliteWriteEvent::Enqueued { .. } => self.enqueued.notify_one(),
            pioneer_sqlite::SqliteWriteEvent::Cancelled { queue, .. } => {
                *self.cancelled_queue.lock().unwrap() = Some(queue);
                self.cancelled.notify_one();
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn cancelling_queued_terminal_commit_releases_capacity_without_partial_events() {
    use pioneer_sqlite::{SqliteDatabase, SqliteWriteExecutor, SqliteWriteQueueSnapshot};
    let connection = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&connection, None).await.unwrap();
    seed_task_test_workspace(&connection).await;
    let observer = Arc::new(TerminalQueueObserver::default());
    let database = SqliteDatabase::from_executor(
        connection.clone(),
        SqliteWriteExecutor::with_observer(connection, observer.clone()),
    );
    let (runtime, run, _, result) =
        fixture_with_runtime(TaskRuntime::new(Arc::new(CrudStore::new(database)))).await;
    let store = Arc::new(
        runtime
            .service()
            .store()
            .with_maintenance_reads_and_critical_writes(),
    );
    let prepared = store
        .prepare_task_terminal_transition(completed(&run, &result, run.created_at + 10))
        .await
        .unwrap();
    let before = events(&runtime, &run).await;
    let blocker = store.database_connection().begin().await.unwrap();
    observer.armed.store(true, Ordering::SeqCst);
    let worker = tokio::spawn({
        let store = store.clone();
        let prepared = prepared.clone();
        async move { store.commit_task_terminal_transition(prepared).await }
    });
    timeout(Duration::from_secs(5), observer.enqueued.notified())
        .await
        .unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    timeout(Duration::from_secs(5), observer.cancelled.notified())
        .await
        .unwrap();
    assert_eq!(
        *observer.cancelled_queue.lock().unwrap(),
        Some(SqliteWriteQueueSnapshot::default())
    );
    blocker.rollback().await.unwrap();
    assert_eq!(events(&runtime, &run).await, before);
    assert!(deliveries(&runtime, &run).await.is_empty());
    timeout(
        Duration::from_secs(5),
        store.commit_task_terminal_transition(prepared),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(deliveries(&runtime, &run).await.len(), 1);
}

#[tokio::test]
async fn finalization_failure_rolls_back_the_already_projected_terminal_batch() {
    let (runtime, run, handle, result) = fixture().await;
    let store = runtime.service().store();
    let db = store.database_connection();
    let before = events(&runtime, &run).await;
    db.execute_unprepared("CREATE TRIGGER reject_terminal_occurrence BEFORE UPDATE ON task_occurrence_contract WHEN NEW.status='delivered' BEGIN SELECT RAISE(ABORT, 'injected finalization failure'); END").await.unwrap();
    assert!(
        handle
            .complete_run(Some(result.clone()), run.created_at + 10)
            .await
            .is_err()
    );
    assert_eq!(events(&runtime, &run).await, before);
    assert!(deliveries(&runtime, &run).await.is_empty());
    assert!(
        !store
            .get_task_run(&run.id)
            .await
            .unwrap()
            .unwrap()
            .status
            .is_terminal()
    );
    assert!(
        !store
            .load_execution_for_run(&run.id)
            .await
            .unwrap()
            .unwrap()
            .status
            .is_terminal()
    );
    assert!(
        pioneer_entity::task_delivery_authority::Entity::find()
            .filter(pioneer_entity::task_delivery_authority::Column::RunId.eq(run.id.clone()))
            .all(&db)
            .await
            .unwrap()
            .is_empty()
    );
    db.execute_unprepared("DROP TRIGGER reject_terminal_occurrence")
        .await
        .unwrap();
    handle
        .complete_run(Some(result), run.created_at + 11)
        .await
        .unwrap();
}

#[tokio::test]
async fn delivery_output_binding_and_candidate_keep_the_original_delivery_identity() {
    let (runtime, run, handle, result) = fixture().await;
    let store = runtime.service().store();
    persist_test_agent_turn(
        &runtime,
        TEST_PARENT_EXECUTION_ID,
        TEST_PARENT_THREAD_ID,
        TEST_PARENT_TURN_ID,
        None,
    )
    .await;
    let execution = store
        .load_execution_for_run(&run.id)
        .await
        .unwrap()
        .unwrap();
    let run_turn = TaskRunTurn {
        id: format!("source_{}", run.id),
        task_id: run.task_id.clone(),
        run_id: run.id.clone(),
        execution_id: Some(execution.id),
        thread_id: TEST_PARENT_THREAD_ID.into(),
        turn_id: TEST_PARENT_TURN_ID.into(),
        kind: TaskRunTurnKind::Initial,
        round: 0,
        sequence: 0,
        status: TaskRunTurnStatus::InProgress,
        reviews_candidate_id: None,
        requested_by_candidate_id: None,
        requested_by_review_event_id: None,
        created_at: run.created_at,
        started_at: Some(run.created_at),
        completed_at: None,
    };
    store
        .append_task_event(
            TaskEventPayload::TaskRunTurnStarted {
                task_run_turn: run_turn.clone(),
            },
            run.created_at,
        )
        .await
        .unwrap();
    let db = store.database_connection();
    db.execute(&Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE turn SET status='completed' WHERE id=?",
        [TEST_PARENT_TURN_ID.into()],
    ))
    .await
    .unwrap();
    // Same empty ready manifest used by the existing CRUD compaction fixture.
    let manifest = format!("output_{}", run.id);
    let empty_digest = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    pioneer_entity::compaction_frozen_history::Entity::insert(
        pioneer_entity::compaction_frozen_history::ActiveModel {
            id: Set(manifest.clone()),
            workspace_id: Set(TEST_WORKSPACE_ID.into()),
            owner_thread: Set(TEST_PARENT_THREAD_ID.into()),
            identity_sha256: Set(empty_digest.into()),
            message_count: Set(0),
            next_ordinal: Set(0),
            import_count: Set(0),
            imports_sha256: Set(empty_digest.into()),
            next_import: Set(0),
            ready: Set(1),
            storage_registered: Set(0),
        },
    )
    .exec(&db)
    .await
    .unwrap();
    pioneer_entity::compaction_task_output::Entity::insert(
        pioneer_entity::compaction_task_output::ActiveModel {
            task_run_turn_id: Set(run_turn.id.clone()),
            task_id: Set(run.task_id.clone()),
            run_id: Set(run.id.clone()),
            workspace_id: Set(TEST_WORKSPACE_ID.into()),
            source_thread: Set(TEST_PARENT_THREAD_ID.into()),
            source_turn: Set(TEST_PARENT_TURN_ID.into()),
            manifest_id: Set(manifest),
        },
    )
    .exec(&db)
    .await
    .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut first = handle;
    first.terminal_preparation_barrier = Some(barrier.clone());
    let mut second = TaskExecutionHandle::new(
        store.clone(),
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    );
    second.terminal_preparation_barrier = Some(barrier);
    let at = run.created_at + 10;
    let (a, b) = timeout(Duration::from_secs(5), async {
        tokio::join!(
            first.complete_run(Some(result.clone()), at),
            second.complete_run(Some(result.clone()), at + 1)
        )
    })
    .await
    .unwrap();
    a.unwrap();
    b.unwrap();
    let delivery = deliveries(&runtime, &run).await.remove(0);
    let binding =
        pioneer_entity::compaction_delivery_output::Entity::find_by_id(delivery.id.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(binding.delivery_id, delivery.id);
    assert_eq!(binding.task_run_turn_id, run_turn.id);
    assert_eq!(binding.candidate_id, format!("trc_{}", run.id));
    let before = binding.clone();
    TaskExecutionHandle::new(
        store,
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    )
    .complete_run(Some(result), at + 99)
    .await
    .unwrap();
    assert_eq!(
        pioneer_entity::compaction_delivery_output::Entity::find_by_id(delivery.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn ordinary_delivery_update_still_rejects_rewritten_result_by_existing_id() {
    let (runtime, run, handle, result) = fixture().await;
    let at = run.created_at + 10;
    handle.complete_run(Some(result), at).await.unwrap();
    let mut delivery = deliveries(&runtime, &run).await.remove(0);
    let original = delivery.clone();
    let before = events(&runtime, &run).await;
    delivery.status = TaskDeliveryStatus::Delivering;
    delivery.attempt_count = 1;
    delivery.next_attempt_at = None;
    delivery.updated_at = at + 1;
    delivery.result_snapshot.as_mut().unwrap().summary = Some("replacement result".into());
    let attempt = TaskDeliveryAttempt {
        id: "rewritten_result_attempt".into(),
        delivery_id: delivery.id.clone(),
        attempt_number: 1,
        status: TaskDeliveryAttemptStatus::Started,
        started_at: at + 1,
        completed_at: None,
        http_status: None,
        error: None,
        response_fingerprint: None,
    };
    assert!(
        runtime
            .service()
            .store()
            .append_task_event(
                TaskEventPayload::DeliveryStarted { delivery, attempt },
                at + 1
            )
            .await
            .is_err()
    );
    assert_eq!(deliveries(&runtime, &run).await, vec![original]);
    assert_eq!(events(&runtime, &run).await, before);
}

#[tokio::test]
async fn implicit_result_replay_uses_the_original_delivery_snapshot() {
    let (runtime, run, handle, mut result) = fixture().await;
    let store = runtime.service().store();
    let db = store.database_connection();
    // The historical builder permits a None run result to fall back to the
    // previous Task result. TaskCompleted then replaces Task.result with None.
    db.execute(&Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE task SET result_json=? WHERE id=?",
        [
            serde_json::to_string(&result).unwrap().into(),
            run.task_id.clone().into(),
        ],
    ))
    .await
    .unwrap();
    let at = run.created_at + 10;
    handle.complete_run(None, at).await.unwrap();
    let original = deliveries(&runtime, &run).await;
    assert_eq!(original[0].result_snapshot, Some(result.clone()));
    result.summary = Some("later task result".into());
    db.execute(&Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE task SET result_json=? WHERE id=?",
        [
            serde_json::to_string(&result).unwrap().into(),
            run.task_id.clone().into(),
        ],
    ))
    .await
    .unwrap();
    TaskExecutionHandle::new(
        store,
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    )
    .complete_run(None, at + 99)
    .await
    .unwrap();
    assert_eq!(deliveries(&runtime, &run).await, original);
}
