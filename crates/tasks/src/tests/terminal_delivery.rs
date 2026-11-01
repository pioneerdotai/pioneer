use super::*;
use pioneer_crud::{TaskTerminalCommitStatus, TaskTerminalConflict};
use pioneer_protocol::{TaskDelivery, TaskDeliveryAttempt, TaskDeliveryAttemptStatus};
use sea_orm::{ColumnTrait, QueryFilter, TransactionTrait};

async fn fixture_with_runtime(
    runtime: TaskRuntime,
) -> (TaskRuntime, TaskRun, TaskExecutionHandle, TaskResult) {
    fixture_with_runtime_and_retry(runtime, None).await
}

async fn fixture_with_runtime_and_retry(
    runtime: TaskRuntime,
    retry_policy: Option<TaskRetryPolicy>,
) -> (TaskRuntime, TaskRun, TaskExecutionHandle, TaskResult) {
    let mut params = create_params(TaskTriggerSpec::Immediate);
    params.owner_kind = TaskOwnerKind::Thread;
    params.owner_id = Some("thr_terminal_delivery".into());
    params.created_by_thread_id = params.owner_id.clone();
    params.retry_policy = retry_policy;
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
    db.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite, "UPDATE task_run_execution SET status='running',result_json=NULL,error_json=NULL,completed_at=NULL WHERE task_run_id=?", [run.id.clone().into()])).await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(
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
        .execute_raw(Statement::from_sql_and_values(
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
    assert!(
        error
            .downcast_ref::<crate::executor::TerminalCommitPhase>()
            .is_some(),
        "terminal phase must survive alongside the original domain conflict"
    );
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
                db.execute_raw(Statement::from_sql_and_values(
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
                db.execute_raw(Statement::from_sql_and_values(
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
                db.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite, "UPDATE task_occurrence_contract SET execution_generation=execution_generation+1 WHERE run_id=?", [run.id.clone().into()])).await.unwrap();
            }
            "retry_generation" => {
                db.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite, "UPDATE task_occurrence_contract SET retry_attempt=retry_attempt+1 WHERE run_id=?", [run.id.clone().into()])).await.unwrap();
            }
            "trigger" => {
                db.execute_raw(Statement::from_sql_and_values(
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
            db.execute_raw(Statement::from_sql_and_values(
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
            db.execute_raw(Statement::from_sql_and_values(
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
        .execute_raw(Statement::from_sql_and_values(
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
    // Commit refreshes event preflight through a reader before queuing its
    // writer. A shared in-memory connection would stall that read behind the
    // blocker, so it could never exercise queued-writer cancellation.
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("terminal-cancellation.sqlite");
    let mut options = sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
    options
        .max_connections(1)
        .min_connections(1)
        .map_sqlx_sqlite_opts(|options| {
            options
                .pragma("journal_mode", "WAL")
                .pragma("foreign_keys", "OFF")
        });
    let connection = Database::connect(options).await.unwrap();
    Migrator::up(&connection, None).await.unwrap();
    seed_task_test_workspace(&connection).await;
    let mut options =
        sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
    options
        .max_connections(1)
        .min_connections(1)
        .map_sqlx_sqlite_opts(|options| {
            options
                .read_only(true)
                .create_if_missing(false)
                .pragma("query_only", "ON")
                .pragma("foreign_keys", "OFF")
        });
    let reader = Database::connect(options).await.unwrap();
    let observer = Arc::new(TerminalQueueObserver::default());
    let database = SqliteDatabase::from_executor(
        reader,
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
    let pending_before =
        pioneer_entity::task_occurrence_reconcile_pending::Entity::find_by_id(run.id.clone())
            .one(&db)
            .await
            .unwrap();
    let generation_before =
        pioneer_entity::task_occurrence_reconcile_sequence::Entity::find_by_id(1)
            .one(&db)
            .await
            .unwrap();
    db.execute_unprepared("CREATE TRIGGER reject_terminal_occurrence BEFORE UPDATE ON task_occurrence_contract WHEN NEW.status='delivered' BEGIN SELECT RAISE(ABORT, 'injected finalization failure'); END").await.unwrap();
    assert!(
        handle
            .complete_run(Some(result.clone()), run.created_at + 10)
            .await
            .is_err()
    );
    assert_eq!(events(&runtime, &run).await, before);
    assert_eq!(
        pioneer_entity::task_occurrence_reconcile_pending::Entity::find_by_id(run.id.clone())
            .one(&db)
            .await
            .unwrap(),
        pending_before
    );
    assert_eq!(
        pioneer_entity::task_occurrence_reconcile_sequence::Entity::find_by_id(1)
            .one(&db)
            .await
            .unwrap(),
        generation_before
    );
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
    db.execute_raw(Statement::from_sql_and_values(
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
            expired: Set(0),
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
    db.execute_raw(Statement::from_sql_and_values(
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
    db.execute_raw(Statement::from_sql_and_values(
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

async fn unadmitted_agent_fixture() -> (TaskRuntime, TaskRun, TaskExecutionHandle) {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut params);
    params.agent_spec = Some(agent_spec(3));
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
            TaskExecutorKind::Agent,
            "before-graph",
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
    (runtime, run, handle)
}

#[tokio::test]
async fn cancellation_before_agent_graph_admission_is_atomic_and_restart_safe() {
    let (runtime, run, handle) = unadmitted_agent_fixture().await;
    let store = runtime.service().store();
    let at = run.created_at + 10;
    handle
        .cancel_run(Some("cancel before graph".into()), at)
        .await
        .unwrap();
    let execution = store
        .load_execution_for_run(&run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.status, TaskRunExecutionStatus::Cancelled);
    assert!(
        pioneer_crud::load_agent_execution(&store.database_connection(), &execution.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .get_task_occurrence_contract_by_run(&run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskOccurrenceStatus::Cancelled
    );
    let before = events(&runtime, &run).await;
    let recovered = TaskExecutionHandle::new(
        store,
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    );
    recovered
        .cancel_run(Some("cancel before graph".into()), at + 20)
        .await
        .unwrap();
    assert_eq!(events(&runtime, &run).await, before);
    assert!(deliveries(&runtime, &run).await.is_empty());
}

#[tokio::test]
async fn cancellation_rejects_a_missing_previously_bound_agent_graph() {
    let (runtime, run, handle) = unadmitted_agent_fixture().await;
    let store = runtime.service().store();
    let execution = store
        .load_execution_for_run(&run.id)
        .await
        .unwrap()
        .unwrap();
    let mut occurrence = store
        .get_task_occurrence_contract_by_run(&run.id)
        .await
        .unwrap()
        .unwrap();
    // Inject the durable lineage of an admitted graph whose execution is lost.
    occurrence.agent_execution_id = Some(execution.id.clone());
    occurrence.work_graph_root_execution_id = Some(execution.id.clone());
    occurrence.root_resource_scope_id = Some(execution.id.clone());
    store
        .upsert_task_occurrence_contract(&occurrence, run.created_at)
        .await
        .unwrap();
    let before = events(&runtime, &run).await;
    let error = handle
        .cancel_run(Some("cancel".into()), run.created_at + 10)
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<TaskTerminalConflict>().is_some());
    assert_eq!(events(&runtime, &run).await, before);
    assert_eq!(
        store
            .load_execution_for_run(&run.id)
            .await
            .unwrap()
            .unwrap(),
        execution
    );
    assert_eq!(
        store
            .get_task_occurrence_contract_by_run(&run.id)
            .await
            .unwrap()
            .unwrap(),
        occurrence
    );
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

async fn cancelled_work_graph_fixture() -> (TaskRuntime, TaskRun, TaskExecutionHandle) {
    let (runtime, run, handle) = unadmitted_agent_fixture().await;
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
    persist_test_child_agent_execution(&runtime, &execution.id).await;
    let mut occurrence = store
        .get_task_occurrence_contract_by_run(&run.id)
        .await
        .unwrap()
        .unwrap();
    occurrence.agent_execution_id = Some(execution.id.clone());
    occurrence.work_graph_root_execution_id = Some(TEST_PARENT_EXECUTION_ID.into());
    occurrence.root_resource_scope_id = Some(TEST_PARENT_EXECUTION_ID.into());
    occurrence.status = TaskOccurrenceStatus::Cancelled;
    occurrence.terminal_reason = Some("graph fence".into());
    store
        .upsert_task_occurrence_contract(&occurrence, run.created_at + 5)
        .await
        .unwrap();
    let db = store.database_connection();
    // Reproduce cancel_agent_work_graph's pre-Task fence: it leaves payloads
    // untouched and records completion before the RunCancelled event exists.
    db.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "UPDATE task_run_execution SET status='cancelled',worker_id=NULL,lease_until=NULL,completed_at=updated_at WHERE id=?",
        [execution.id.clone().into()],
    )).await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "UPDATE agent_execution SET status='cancelled',finished_at=updated_at,parent_task_id=? WHERE id=?",
        [run.task_id.clone().into(), execution.id.clone().into()],
    )).await.unwrap();
    (runtime, run, handle)
}

#[tokio::test]
async fn task_cancellation_preserves_the_existing_work_graph_execution_fence() {
    let (runtime, run, handle) = cancelled_work_graph_fixture().await;
    let store = runtime.service().store();
    let occurrence = store
        .get_task_occurrence_contract_by_run(&run.id)
        .await
        .unwrap()
        .unwrap();
    let fenced = store
        .load_execution_for_run(&run.id)
        .await
        .unwrap()
        .unwrap();
    let before = events(&runtime, &run).await;
    assert!(
        handle
            .complete_run(None, run.created_at + 9)
            .await
            .unwrap_err()
            .downcast_ref::<TaskTerminalConflict>()
            .is_some()
    );
    assert_eq!(events(&runtime, &run).await, before);
    assert_eq!(
        store
            .load_execution_for_run(&run.id)
            .await
            .unwrap()
            .unwrap(),
        fenced
    );
    handle
        .cancel_run(Some("Task cancellation".into()), run.created_at + 10)
        .await
        .unwrap();
    assert_eq!(
        store
            .load_execution_for_run(&run.id)
            .await
            .unwrap()
            .unwrap(),
        fenced
    );
    assert_eq!(
        store
            .get_task_occurrence_contract_by_run(&run.id)
            .await
            .unwrap()
            .unwrap(),
        occurrence
    );
    assert_eq!(
        store.get_task_run(&run.id).await.unwrap().unwrap().status,
        TaskRunStatus::Cancelled
    );
    let before = events(&runtime, &run).await;
    handle
        .cancel_run(Some("Task cancellation".into()), run.created_at + 20)
        .await
        .unwrap();
    assert_eq!(events(&runtime, &run).await, before);
    assert!(
        handle
            .complete_run(None, run.created_at + 30)
            .await
            .unwrap_err()
            .downcast_ref::<TaskTerminalConflict>()
            .is_some()
    );
}

async fn persisted_graph_fence(
    runtime: &TaskRuntime,
    run: &TaskRun,
) -> (
    pioneer_entity::task_run_execution::Model,
    pioneer_entity::task_occurrence_contract::Model,
    pioneer_entity::agent_execution::Model,
) {
    let db = runtime.service().store().database_connection();
    let execution = pioneer_entity::task_run_execution::Entity::find()
        .filter(pioneer_entity::task_run_execution::Column::TaskRunId.eq(run.id.clone()))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let occurrence = pioneer_entity::task_occurrence_contract::Entity::find_by_id(run.id.clone())
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let agent = pioneer_entity::agent_execution::Entity::find_by_id(execution.id.clone())
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    (execution, occurrence, agent)
}

#[tokio::test]
async fn cancellation_without_reason_preserves_graph_fence_after_handle_reconstruction() {
    let (runtime, run, handle) = cancelled_work_graph_fixture().await;
    let store = runtime.service().store();
    let fence = persisted_graph_fence(&runtime, &run).await;
    assert_eq!(fence.1.terminal_reason.as_deref(), Some("graph fence"));
    assert!(fence.0.result_json.is_none() && fence.0.error_json.is_none());

    let incompatible = TaskResult {
        summary: Some("incompatible success".into()),
        data: Some(TaskValue::Integer(99)),
        artifacts: Vec::new(),
        completed_by_run_id: Some(run.id.clone()),
    };
    let before = events(&runtime, &run).await;
    let original_task = store.get_task(&run.task_id).await.unwrap().unwrap().task;
    let original_run = store.get_task_run(&run.id).await.unwrap().unwrap();
    let error = handle
        .complete_run(Some(incompatible.clone()), run.created_at + 9)
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<TaskTerminalConflict>().is_some());
    assert_eq!(events(&runtime, &run).await, before);
    assert_eq!(
        store.get_task(&run.task_id).await.unwrap().unwrap().task,
        original_task
    );
    assert_eq!(
        store.get_task_run(&run.id).await.unwrap().unwrap(),
        original_run
    );
    assert_eq!(persisted_graph_fence(&runtime, &run).await, fence);

    handle.cancel_run(None, run.created_at + 10).await.unwrap();
    assert_eq!(
        store.get_task_run(&run.id).await.unwrap().unwrap().status,
        TaskRunStatus::Cancelled
    );
    assert_eq!(
        store
            .get_task(&run.task_id)
            .await
            .unwrap()
            .unwrap()
            .task
            .status,
        TaskStatus::Cancelled
    );
    // Compare complete persisted rows, including created_at/updated_at and
    // completion facts, rather than just the protocol status projections.
    assert_eq!(persisted_graph_fence(&runtime, &run).await, fence);
    let before = events(&runtime, &run).await;
    let reconstructed = TaskExecutionHandle::new(
        store,
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    );
    reconstructed
        .cancel_run(None, run.created_at + 20)
        .await
        .unwrap();
    assert_eq!(events(&runtime, &run).await, before);
    assert_eq!(persisted_graph_fence(&runtime, &run).await, fence);
    assert!(deliveries(&runtime, &run).await.is_empty());

    let error = reconstructed
        .complete_run(Some(incompatible), run.created_at + 30)
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<TaskTerminalConflict>().is_some());
    assert_eq!(events(&runtime, &run).await, before);
    assert_eq!(persisted_graph_fence(&runtime, &run).await, fence);
}

#[tokio::test]
async fn cancellation_without_reason_rejects_unconfirmed_graph_facts_atomically() {
    for invalid_fact in [
        "binding",
        "generation",
        "parent_task",
        "root",
        "occurrence_status",
        "execution_completion",
        "agent_completion",
        "agent_status",
        "result",
    ] {
        let (runtime, run, handle) = cancelled_work_graph_fixture().await;
        let store = runtime.service().store();
        let db = store.database_connection();
        let execution = store
            .load_execution_for_run(&run.id)
            .await
            .unwrap()
            .unwrap();
        // Corrupt the fixture before preparation: commit must validate the
        // actual graph facts even when execution and RunCancelled payloads match.
        let (sql, id) = match invalid_fact {
            "binding" => (
                "UPDATE task_occurrence_contract SET agent_execution_id=work_graph_root_execution_id WHERE run_id=?",
                run.id.clone(),
            ),
            "generation" => (
                "UPDATE agent_execution SET execution_generation=execution_generation+1 WHERE id=?",
                execution.id.clone(),
            ),
            "parent_task" => (
                "UPDATE agent_execution SET parent_task_id=NULL WHERE id=?",
                execution.id.clone(),
            ),
            "root" => (
                "UPDATE task_occurrence_contract SET work_graph_root_execution_id=agent_execution_id,root_resource_scope_id=agent_execution_id WHERE run_id=?",
                run.id.clone(),
            ),
            "occurrence_status" => (
                "UPDATE task_occurrence_contract SET status='running' WHERE run_id=?",
                run.id.clone(),
            ),
            "execution_completion" => (
                "UPDATE task_run_execution SET completed_at=NULL WHERE id=?",
                execution.id.clone(),
            ),
            "agent_completion" => (
                "UPDATE agent_execution SET finished_at=NULL WHERE id=?",
                execution.id.clone(),
            ),
            "agent_status" => (
                "UPDATE agent_execution SET status='running' WHERE id=?",
                execution.id.clone(),
            ),
            "result" => (
                "UPDATE task_run_execution SET result_json='{}' WHERE id=?",
                execution.id.clone(),
            ),
            _ => unreachable!(),
        };
        db.execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            sql,
            [id.into()],
        ))
        .await
        .unwrap();
        let fence = persisted_graph_fence(&runtime, &run).await;
        let task = pioneer_entity::task::Entity::find_by_id(run.task_id.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let persisted_run = pioneer_entity::task_run::Entity::find_by_id(run.id.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let before = events(&runtime, &run).await;
        let error = handle
            .cancel_run(None, run.created_at + 10)
            .await
            .unwrap_err();
        assert!(
            error.downcast_ref::<TaskTerminalConflict>().is_some(),
            "{invalid_fact}: {error:#}"
        );
        assert_eq!(events(&runtime, &run).await, before, "{invalid_fact}");
        assert_eq!(
            persisted_graph_fence(&runtime, &run).await,
            fence,
            "{invalid_fact}"
        );
        assert_eq!(
            pioneer_entity::task::Entity::find_by_id(run.task_id.clone())
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            task,
            "{invalid_fact}"
        );
        assert_eq!(
            pioneer_entity::task_run::Entity::find_by_id(run.id.clone())
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            persisted_run,
            "{invalid_fact}"
        );
        assert!(
            deliveries(&runtime, &run).await.is_empty(),
            "{invalid_fact}"
        );
    }
}

struct RevisionTerminalFixture {
    _directory: tempfile::TempDir,
    runtime: TaskRuntime,
    run: TaskRun,
    handle: TaskExecutionHandle,
    revision: TaskRunTurn,
    observer: Arc<DispatchDatabaseObserver>,
}

async fn revision_terminal_fixture() -> RevisionTerminalFixture {
    revision_terminal_fixture_with_retry(None).await
}

async fn revision_terminal_fixture_with_retry(
    retry_policy: Option<TaskRetryPolicy>,
) -> RevisionTerminalFixture {
    use pioneer_sqlite::{SqliteDatabase, SqliteWriteExecutor};
    use sea_orm::ConnectOptions;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.sqlite");
    let mut writer_options = ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
    writer_options
        .max_connections(1)
        .sqlx_logging(false)
        .map_sqlx_sqlite_opts(|options| options.pragma("foreign_keys", "OFF"));
    let writer = Database::connect(writer_options).await.unwrap();
    pioneer_sqlite::apply_sqlite_pragmas(&writer).await.unwrap();
    let bootstrap = SqliteDatabase::from_single_connection(writer.clone());
    let migration = bootstrap.begin().await.unwrap();
    Migrator::up(&*migration, None).await.unwrap();
    migration.commit().await.unwrap();
    seed_task_test_workspace(&writer).await;
    let mut reader_options =
        ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
    reader_options
        .max_connections(2)
        .sqlx_logging(false)
        .map_sqlx_sqlite_opts(|options| {
            options
                .pragma("query_only", "ON")
                .pragma("foreign_keys", "OFF")
        });
    let reader = Database::connect(reader_options).await.unwrap();
    let observer = Arc::new(DispatchDatabaseObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    let (runtime, run, _, _) = fixture_with_runtime_and_retry(
        TaskRuntime::new(Arc::new(CrudStore::new(database))),
        retry_policy,
    )
    .await;
    let store = Arc::new(
        runtime
            .service()
            .store()
            .with_maintenance_reads_and_critical_writes(),
    );
    let execution = store
        .load_execution_for_run(&run.id)
        .await
        .unwrap()
        .unwrap();
    let revision = TaskRunTurn {
        id: format!("revision_{}", run.id),
        task_id: run.task_id.clone(),
        run_id: run.id.clone(),
        execution_id: Some(execution.id),
        thread_id: "reserved-revision-thread".into(),
        turn_id: "reserved-revision-turn".into(),
        kind: TaskRunTurnKind::Revision,
        round: 1,
        sequence: 1,
        status: TaskRunTurnStatus::InProgress,
        reviews_candidate_id: None,
        requested_by_candidate_id: Some("previous-candidate".into()),
        requested_by_review_event_id: Some("revision-request".into()),
        created_at: run.created_at,
        started_at: Some(run.created_at),
        completed_at: None,
    };
    store
        .append_task_event(
            TaskEventPayload::TaskRunTurnStarted {
                task_run_turn: revision.clone(),
            },
            run.created_at,
        )
        .await
        .unwrap();
    let handle = TaskExecutionHandle::new(
        store,
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    );
    RevisionTerminalFixture {
        _directory: directory,
        runtime,
        run,
        handle,
        revision,
        observer,
    }
}

fn revision_start_error(run: &TaskRun) -> TaskError {
    crate::TaskStartFailure::new(
        crate::TaskStartStage::CliAdmission,
        crate::TaskStartCause::Validation,
    )
    .with_reason(crate::TaskStartReason::CapabilityProjectionInvalid)
    .descriptor()
    .task_error(Some(run.id.clone()))
}

#[tokio::test]
async fn terminal_revision_rolls_back_with_run_then_replays_after_restart() {
    use pioneer_sqlite::{SqliteReadClass, SqliteWriteClass};
    for (failed, turn_status, run_status, execution_status) in [
        (
            false,
            TaskRunTurnStatus::Blocked,
            TaskRunStatus::Blocked,
            TaskRunExecutionStatus::Blocked,
        ),
        (
            true,
            TaskRunTurnStatus::Failed,
            TaskRunStatus::Failed,
            TaskRunExecutionStatus::Failed,
        ),
    ] {
        let fixture = revision_terminal_fixture().await;
        let RevisionTerminalFixture {
            runtime,
            run,
            handle,
            revision,
            observer,
            ..
        } = &fixture;
        let store = runtime.service().store();
        let db = store.database_connection();
        let before = events(runtime, run).await;
        let error = revision_start_error(run);
        // Abort after the revision/run events have been projected, during the
        // existing finalization. All events, delivery and domain rows must roll back.
        db.execute_unprepared("CREATE TRIGGER reject_revision_terminal BEFORE UPDATE ON task_occurrence_contract WHEN NEW.status='failed' BEGIN SELECT RAISE(ABORT, 'injected revision finalization failure'); END").await.unwrap();
        assert!(
            commit_revision_failure(
                handle,
                revision.clone(),
                error.clone(),
                run.created_at + 10,
                failed
            )
            .await
            .is_err()
        );
        assert_eq!(events(runtime, run).await, before);
        assert_eq!(
            store
                .get_task_run_turn(&revision.id)
                .await
                .unwrap()
                .as_ref(),
            Some(revision)
        );
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
        assert!(deliveries(runtime, run).await.is_empty());
        db.execute_unprepared("DROP TRIGGER reject_revision_terminal")
            .await
            .unwrap();
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        commit_revision_failure(
            handle,
            revision.clone(),
            error.clone(),
            run.created_at + 11,
            failed,
        )
        .await
        .unwrap();
        assert_eq!(observer.writes.lock().unwrap().len(), 1);
        observer.assert_scope(SqliteReadClass::Maintenance, SqliteWriteClass::Critical);
        let saved = store
            .get_task_run_turn(&revision.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.status, turn_status);
        assert_eq!(saved.completed_at, Some(run.created_at + 11));
        assert_eq!(
            store.get_task_run(&run.id).await.unwrap().unwrap().status,
            run_status
        );
        assert_eq!(
            store
                .get_task_run(&run.id)
                .await
                .unwrap()
                .unwrap()
                .error
                .as_ref(),
            Some(&error)
        );
        assert_eq!(
            store
                .load_execution_for_run(&run.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            execution_status
        );
        assert_eq!(
            store
                .get_task_occurrence_contract_by_run(&run.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            TaskOccurrenceStatus::Failed
        );
        assert!(
            store
                .get_turn(&revision.thread_id, &revision.turn_id)
                .await
                .unwrap()
                .is_none()
        );
        let committed = events(runtime, run).await;
        let revision_event = committed
            .iter()
            .position(|event| {
                if failed {
                    matches!(event.payload, TaskEventPayload::TaskRunTurnFailed { .. })
                } else {
                    matches!(event.payload, TaskEventPayload::TaskRunTurnBlocked { .. })
                }
            })
            .unwrap();
        let run_event = committed
            .iter()
            .position(|event| {
                if failed {
                    matches!(event.payload, TaskEventPayload::RunFailed { .. })
                } else {
                    matches!(event.payload, TaskEventPayload::RunBlocked { .. })
                }
            })
            .unwrap();
        assert!(revision_event < run_event);
        let queued = deliveries(runtime, run).await;
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].error_snapshot.as_ref(), Some(&error));
        let restarted = TaskRuntime::new(store.clone());
        let restarted_handle = TaskExecutionHandle::new(
            store.clone(),
            restarted.event_bus(),
            run.task_id.clone(),
            run.id.clone(),
        );
        commit_revision_failure(
            &restarted_handle,
            revision.clone(),
            error,
            run.created_at + 99,
            failed,
        )
        .await
        .unwrap();
        assert_eq!(events(&restarted, run).await, committed);
        assert_eq!(deliveries(&restarted, run).await, queued);
        assert_eq!(
            store
                .get_task_run_turn(&revision.id)
                .await
                .unwrap()
                .unwrap(),
            saved
        );
    }
}

#[tokio::test]
async fn prepared_revision_failure_cannot_overwrite_a_winner_or_a_new_revision() {
    for failed in [false, true] {
        for winner in ["cancel", "success", "revision", "turn_cancelled"] {
            let fixture = revision_terminal_fixture().await;
            let RevisionTerminalFixture {
                runtime,
                run,
                handle,
                revision,
                ..
            } = &fixture;
            let store = runtime.service().store();
            let prepared = store
                .prepare_task_terminal_transition(if failed {
                    TaskEventPayload::RunFailed {
                        task_id: run.task_id.clone(),
                        run_id: run.id.clone(),
                        error: Some(revision_start_error(run)),
                        completed_at: run.created_at + 20,
                    }
                } else {
                    TaskEventPayload::RunBlocked {
                        task_id: run.task_id.clone(),
                        run_id: run.id.clone(),
                        error: Some(revision_start_error(run)),
                        blocked_at: run.created_at + 20,
                    }
                })
                .await
                .unwrap();
            let prepared = store
                .prepare_task_terminal_with_revision(prepared, revision.clone())
                .await
                .unwrap();
            match winner {
                "cancel" => handle
                    .cancel_run(Some("cancelled".into()), run.created_at + 10)
                    .await
                    .unwrap(),
                "success" => handle
                    .complete_run(None, run.created_at + 10)
                    .await
                    .unwrap(),
                "revision" => {
                    let mut next = revision.clone();
                    next.id.push_str("_next");
                    next.turn_id.push_str("_next");
                    next.sequence += 1;
                    next.round += 1;
                    store
                        .append_task_event(
                            TaskEventPayload::TaskRunTurnStarted {
                                task_run_turn: next,
                            },
                            run.created_at + 10,
                        )
                        .await
                        .unwrap();
                }
                "turn_cancelled" => {
                    let mut cancelled = revision.clone();
                    cancelled.status = TaskRunTurnStatus::Cancelled;
                    cancelled.completed_at = Some(run.created_at + 10);
                    handle
                        .record_task_run_turn_failed(cancelled, None, run.created_at + 10)
                        .await
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let before = store.get_task(&run.task_id).await.unwrap().unwrap();
            let before_events = events(runtime, run).await;
            let before_deliveries = deliveries(runtime, run).await;
            let error = store
                .commit_task_terminal_transition(prepared)
                .await
                .unwrap_err();
            assert!(
                error.downcast_ref::<TaskTerminalConflict>().is_some(),
                "{winner}: {error:#}"
            );
            assert_eq!(store.get_task(&run.task_id).await.unwrap().unwrap(), before);
            assert_eq!(events(runtime, run).await, before_events);
            assert_eq!(deliveries(runtime, run).await, before_deliveries);
        }
    }
}

async fn commit_revision_failure(
    handle: &TaskExecutionHandle,
    revision: TaskRunTurn,
    error: TaskError,
    at: i64,
    failed: bool,
) -> TaskRuntimeResult<()> {
    if failed {
        handle.fail_run_with_revision(revision, error, at).await
    } else {
        handle.block_run_with_revision(revision, error, at).await
    }
}

#[tokio::test]
async fn failed_revision_preserves_policy_retry_and_replay() {
    for retry_policy_error in [false, true] {
        let fixture = revision_terminal_fixture_with_retry(Some(TaskRetryPolicy {
            max_attempts: 2,
            backoff: TaskRetryBackoffKind::Fixed,
            initial_delay_seconds: Some(5),
            max_delay_seconds: None,
            retry_on: vec![if retry_policy_error {
                TaskErrorClass::Policy
            } else {
                TaskErrorClass::Internal
            }],
        }))
        .await;
        let RevisionTerminalFixture {
            runtime,
            run,
            handle,
            revision,
            ..
        } = &fixture;
        let error = TaskError {
            recovery_diagnostic: None,
            code: "write_lock_conflict".to_owned(),
            message: "write scope conflicts with another active task run".to_owned(),
            class: TaskErrorClass::Policy,
            details: None,
            failed_run_id: Some(run.id.clone()),
        };
        let at = run.created_at + 10;
        handle
            .fail_run_with_revision(revision.clone(), error.clone(), at)
            .await
            .unwrap();
        let store = runtime.service().store();
        let response = store.get_task(&run.task_id).await.unwrap().unwrap();
        let source = response
            .runs
            .iter()
            .find(|source| source.id == run.id)
            .unwrap();
        assert_eq!(source.status, TaskRunStatus::Failed);
        assert_eq!(source.completed_at, Some(at));
        assert_eq!(source.error.as_ref(), Some(&error));
        assert_eq!(
            store
                .get_task_run_turn(&revision.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            TaskRunTurnStatus::Failed
        );
        let retries: Vec<_> = response
            .runs
            .iter()
            .filter(|candidate| candidate.retry_of_run_id.as_deref() == Some(run.id.as_str()))
            .collect();
        assert_eq!(retries.len(), usize::from(retry_policy_error));
        if retry_policy_error {
            assert_eq!(response.task.status, TaskStatus::Queued);
            assert_eq!(retries[0].attempt_number, 2);
            assert_eq!(retries[0].ready_at, Some(at + 5));
            assert_eq!(retries[0].status, TaskRunStatus::Queued);
            assert!(
                !store
                    .load_execution_for_run(&run.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status
                    .is_terminal()
            );
            assert!(deliveries(runtime, run).await.is_empty());
        } else {
            assert_eq!(response.task.status, TaskStatus::Failed);
            assert_eq!(
                store
                    .load_execution_for_run(&run.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                TaskRunExecutionStatus::Failed
            );
            assert_eq!(deliveries(runtime, run).await.len(), 1);
        }
        let before_events = events(runtime, run).await;
        let restarted = TaskRuntime::new(store.clone());
        TaskExecutionHandle::new(
            store.clone(),
            restarted.event_bus(),
            run.task_id.clone(),
            run.id.clone(),
        )
        .fail_run_with_revision(revision.clone(), error, at + 99)
        .await
        .unwrap();
        assert_eq!(events(&restarted, run).await, before_events);
        assert_eq!(
            store.get_task(&run.task_id).await.unwrap().unwrap(),
            response
        );
    }
}
