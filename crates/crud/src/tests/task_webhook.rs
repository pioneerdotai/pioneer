//! Webhook admission through the task and execution transaction coordinator.
use super::*;
use pioneer_protocol::{
    TaskActorContract, TaskDeliveryActorContract, TaskReviewerIntent, TaskRunInputEncoding,
    TaskRunTriggerInput,
};
use pioneer_sqlite::{
    SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteClass, SqliteWriteEvent,
    SqliteWriteObserver,
};
use std::sync::Mutex;
use std::time::Duration;

const AT: i64 = 1_700_000_000;

#[derive(Default)]
struct Routes {
    reads: Mutex<Vec<SqliteReadClass>>,
    writes: Mutex<Vec<SqliteWriteEvent>>,
    watch_interactive: std::sync::atomic::AtomicBool,
    interactive_queued: tokio::sync::Notify,
}
impl SqliteReadObserver for Routes {
    fn observe(&self, event: SqliteReadEvent) {
        if let SqliteReadEvent::OperationFinished { class, .. } = event {
            self.reads.lock().unwrap().push(class);
        }
    }
}
impl SqliteWriteObserver for Routes {
    fn observe(&self, event: SqliteWriteEvent) {
        self.writes.lock().unwrap().push(event);
        if self.watch_interactive.load(Ordering::SeqCst)
            && matches!(
                event,
                SqliteWriteEvent::Enqueued {
                    class: SqliteWriteClass::Interactive,
                    ..
                }
            )
        {
            self.interactive_queued.notify_one();
        }
    }
}

async fn observed_store(
    fixture: &pioneer_postgres_test::TestDatabase,
    routes: Arc<Routes>,
) -> CrudStore {
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let executor = pioneer_sqlite::SqliteWriteExecutor::with_observer(writer, routes.clone());
    CrudStore::new(
        pioneer_sqlite::SqliteDatabase::from_executor_with_read_observer(reader, executor, routes),
    )
}

async fn webhook_fixture(store: &CrudStore) -> (Task, TaskTrigger) {
    let mut task = sample_task(AT);
    task.status = TaskStatus::Waiting;
    let mut trigger = sample_task_trigger(AT);
    trigger.spec = TaskTriggerSpec::External {
        source: "webhook".into(),
        event_type: None,
        filter: None,
    };
    trigger.next_fire_at = None;
    trigger.webhook_secret_ref = Some("webhook-fixture-secret-ref".into());
    store
        .append_task_events(
            vec![
                TaskEventPayload::TaskCreated { task: task.clone() },
                TaskEventPayload::TriggerCreated {
                    trigger: trigger.clone(),
                },
            ],
            AT,
        )
        .await
        .unwrap();
    store
        .upsert_task_actor_contract(
            &TaskActorContract {
                task_id: task.id.clone(),
                workspace_id: task.workspace_id.clone(),
                creator: PersistedActorRef::System,
                creator_presentation_snapshot: None,
                reviewer: TaskReviewerIntent::RuntimeAuto,
                execution_destination_thread_id: None,
                execution_route_id: None,
                execution_route_receipt_json: None,
                execution_route_expires_at_millis: None,
                delivery: TaskDeliveryActorContract {
                    enabled: false,
                    destination_thread_id: None,
                    destination_user_id: None,
                    destination_webhook_url_fingerprint: None,
                    route_id: None,
                    return_route_id: None,
                    author_snapshot: None,
                    route_receipt_json: None,
                    disclosure_generation: 1,
                    route_expires_at_millis: None,
                },
                launch: None,
                requested_identity_json: None,
                resolved_identity_id: None,
                resolved_profile_id: None,
                source_config_fingerprint: None,
                derived_child_launch_grant_json: None,
                creator_work_graph_root_execution_id: None,
                work_graph_root_execution_id: None,
                root_resource_scope_id: None,
                accounting_attribution: None,
                controller_principal_id: None,
                revision: 1,
            },
            AT,
        )
        .await
        .unwrap();
    (
        store.get_task(&task.id).await.unwrap().unwrap().task,
        trigger,
    )
}

fn request(
    task: &Task,
    trigger: &TaskTrigger,
    number: i64,
) -> (TaskRun, TaskOccurrenceContract, Vec<TaskEventPayload>) {
    let mut run = sample_task_run(AT + number);
    run.id = format!("webhook_run_{number:08}");
    run.run_group_id = run.id.clone();
    run.task_id = task.id.clone();
    run.trigger_id = Some(trigger.id.clone());
    run.run_number = number;
    run.trigger_input = Some(TaskRunTriggerInput {
        content_type: Some("application/octet-stream".into()),
        encoding: TaskRunInputEncoding::Base64,
        body: "AP9cCg==".into(),
    });
    let occurrence = TaskOccurrenceContract {
        occurrence_id: run.id.clone(),
        task_id: task.id.clone(),
        run_id: run.id.clone(),
        trigger_id: Some(trigger.id.clone()),
        occurrence_key: format!("webhook:{}", run.id),
        execution_generation: number.try_into().unwrap(),
        agent_execution_id: None,
        work_graph_root_execution_id: None,
        root_resource_scope_id: None,
        status: TaskOccurrenceStatus::Queued,
        queue_position: Some(number.try_into().unwrap()),
        retry_attempt: 0,
        action_idempotency_key: format!("task:{}:{}", task.id, run.id),
        route_id: None,
        result_return_route_id: None,
        delivery_plan: None,
        terminal_reason: None,
    };
    let events = vec![
        TaskEventPayload::RunCreated {
            run: run.clone(),
            agent_spec: None,
        },
        TaskEventPayload::TaskQueued {
            task_id: task.id.clone(),
            run_id: Some(run.id.clone()),
        },
    ];
    (run, occurrence, events)
}

#[pioneer_postgres_test::postgres_test]
async fn webhook_admission_inherits_request_and_maintenance_classes_with_query_only_readers() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    postgres_store_with_workspace(&fixture, "ws_task").await;
    let routes = Arc::new(Routes::default());
    let store = observed_store(&fixture, routes.clone()).await;
    let (_, trigger) = webhook_fixture(&store).await;
    assert!(store.connection.reader_query_only_enabled().await.unwrap());
    let mut accepted = Vec::new();
    for (number, scoped, read_class, write_class) in [
        (
            1,
            store.clone(),
            SqliteReadClass::Interactive,
            SqliteWriteClass::Interactive,
        ),
        (
            2,
            store.with_maintenance_access(),
            SqliteReadClass::Maintenance,
            SqliteWriteClass::Maintenance,
        ),
    ] {
        routes.reads.lock().unwrap().clear();
        routes.writes.lock().unwrap().clear();
        let snapshot = scoped
            .get_task_webhook_admission_snapshot(&trigger.id)
            .await
            .unwrap()
            .unwrap();
        let trigger = snapshot.trigger;
        let task = snapshot.task;
        let (run, occurrence, events) = request(&task, &trigger, number);
        let appended = scoped
            .accept_task_webhook(&task, &trigger, &run, occurrence.clone(), events)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(appended.len(), 2);
        assert!(appended[0].sequence < appended[1].sequence);
        assert_eq!(
            scoped
                .get_task_run(&run.id)
                .await
                .unwrap()
                .unwrap()
                .trigger_input,
            run.trigger_input
        );
        assert_eq!(
            scoped
                .get_task_occurrence_contract_by_run(&run.id)
                .await
                .unwrap(),
            Some(occurrence.clone())
        );
        accepted.push((run, occurrence));
        let reads = routes.reads.lock().unwrap();
        assert!(!reads.is_empty());
        assert!(reads.iter().all(|class| *class == read_class));
        let writes = routes.writes.lock().unwrap();
        let enqueued = writes
            .iter()
            .filter_map(|event| match event {
                SqliteWriteEvent::Enqueued { class, .. } => Some(*class),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(!enqueued.is_empty());
        assert!(enqueued.iter().all(|class| *class == write_class));
    }
    assert!(store.connection.reader_query_only_enabled().await.unwrap());
    drop(store);
    // Open independent writer and reader pools on the same committed database.
    let reopened = observed_store(&fixture, Arc::new(Routes::default())).await;
    for (run, occurrence) in accepted {
        assert_eq!(
            reopened
                .get_task_run(&run.id)
                .await
                .unwrap()
                .unwrap()
                .trigger_input,
            run.trigger_input
        );
        assert_eq!(
            reopened
                .get_task_occurrence_contract_by_run(&run.id)
                .await
                .unwrap(),
            Some(occurrence)
        );
    }
    assert_eq!(
        reopened
            .get_webhook_task_trigger(&trigger.id)
            .await
            .unwrap()
            .unwrap()
            .webhook_secret_ref,
        trigger.webhook_secret_ref
    );
}

#[pioneer_postgres_test::postgres_test]
async fn webhook_occurrence_failure_rolls_back_payload_events_projection_and_fanout() {
    let store = test_store_with_workspace("ws_task").await;
    let (task, trigger) = webhook_fixture(&store).await;
    let (run, occurrence, events) = request(&task, &trigger, 1);
    let before_events = store.get_task_events(&task.id, None).await.unwrap();
    let before_pending = pioneer_entity::task_event_fanout_pending::Entity::find_by_id(&task.id)
        .one(&store.connection)
        .await
        .unwrap();
    store.connection.execute_unprepared("CREATE FUNCTION webhook_occurrence_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'reject webhook occurrence'; END; $$; CREATE TRIGGER webhook_occurrence_failure BEFORE INSERT ON task_occurrence_contract FOR EACH ROW EXECUTE FUNCTION webhook_occurrence_failure()").await.unwrap();
    assert!(
        store
            .accept_task_webhook(&task, &trigger, &run, occurrence.clone(), events.clone())
            .await
            .is_err()
    );
    assert!(store.get_task_run(&run.id).await.unwrap().is_none());
    assert!(
        store
            .get_task_occurrence_contract_by_run(&run.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(store.get_task(&task.id).await.unwrap().unwrap().task, task);
    assert_eq!(
        store.get_task_events(&task.id, None).await.unwrap().events,
        before_events.events
    );
    assert_eq!(
        pioneer_entity::task_event_fanout_pending::Entity::find_by_id(&task.id)
            .one(&store.connection)
            .await
            .unwrap(),
        before_pending
    );
    store
        .connection
        .execute_unprepared("DROP TRIGGER webhook_occurrence_failure ON task_occurrence_contract")
        .await
        .unwrap();
    assert!(
        store
            .accept_task_webhook(&task, &trigger, &run, occurrence, events)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store
            .get_task_run(&run.id)
            .await
            .unwrap()
            .unwrap()
            .trigger_input,
        run.trigger_input
    );
}

#[pioneer_postgres_test::postgres_test]
async fn webhook_stale_snapshot_rechecks_pause_cancel_and_finish_before_any_write() {
    for operation in ["pause", "cancel", "finish"] {
        let store = test_store_with_workspace("ws_task").await;
        let (task, trigger) = webhook_fixture(&store).await;
        let (active, occurrence, events) = request(&task, &trigger, 1);
        store
            .accept_task_webhook(&task, &trigger, &active, occurrence, events)
            .await
            .unwrap()
            .unwrap();
        store
            .append_task_event(
                TaskEventPayload::RunStarted {
                    task_id: task.id.clone(),
                    run_id: active.id,
                    started_at: AT + 2,
                },
                AT + 2,
            )
            .await
            .unwrap();
        let expected = store.get_task(&task.id).await.unwrap().unwrap().task;
        let (candidate, occurrence, events) = request(&expected, &trigger, 2);
        match operation {
            "pause" => {
                let mut paused = expected.clone();
                paused.status = TaskStatus::Waiting;
                paused.revision += 1;
                let mut paused_trigger = trigger.clone();
                paused_trigger.status = TaskTriggerStatus::Paused;
                store
                    .append_task_event(
                        TaskEventPayload::TaskPaused {
                            task: paused,
                            triggers: vec![paused_trigger],
                            reason: None,
                            paused_at: AT + 3,
                        },
                        AT + 3,
                    )
                    .await
                    .unwrap();
            }
            "cancel" => {
                store
                    .append_task_event(
                        TaskEventPayload::TaskCancelled {
                            task_id: task.id.clone(),
                            reason: None,
                            completed_at: AT + 3,
                        },
                        AT + 3,
                    )
                    .await
                    .unwrap();
            }
            "finish" => {
                store
                    .request_task_finish(&task.id, None, AT + 3, None)
                    .await
                    .unwrap();
                let stopped = store.get_task(&task.id).await.unwrap().unwrap().task;
                assert!(stopped.finish_request().is_some());
                assert!(
                    !stopped.status.is_terminal(),
                    "active admitted work remains unfinished"
                );
            }
            _ => unreachable!(),
        }
        let after_stop = store.get_task_events(&task.id, None).await.unwrap();
        assert!(
            store
                .accept_task_webhook(&expected, &trigger, &candidate, occurrence, events)
                .await
                .unwrap()
                .is_none(),
            "{operation}"
        );
        assert!(store.get_task_run(&candidate.id).await.unwrap().is_none());
        assert!(
            store
                .get_task_occurrence_contract_by_run(&candidate.id)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.get_task_events(&task.id, None).await.unwrap().events,
            after_stop.events
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn cancelled_webhook_waiter_releases_writer_queue_and_can_be_admitted_again() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    postgres_store_with_workspace(&fixture, "ws_task").await;
    let routes = Arc::new(Routes::default());
    let store = observed_store(&fixture, routes.clone()).await;
    let (task, trigger) = webhook_fixture(&store).await;
    let (run, occurrence, events) = request(&task, &trigger, 1);
    let hold = store
        .with_maintenance_access()
        .connection
        .begin()
        .await
        .unwrap();
    routes.watch_interactive.store(true, Ordering::SeqCst);
    let worker = store.clone();
    let expected_task = task.clone();
    let expected_trigger = trigger.clone();
    let pending_run = run.clone();
    let pending_occurrence = occurrence.clone();
    let pending_events = events.clone();
    let waiting = tokio::spawn(async move {
        worker
            .accept_task_webhook(
                &expected_task,
                &expected_trigger,
                &pending_run,
                pending_occurrence,
                pending_events,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), routes.interactive_queued.notified())
        .await
        .unwrap();
    assert!(
        store
            .get_webhook_task_trigger(&trigger.id)
            .await
            .unwrap()
            .is_some(),
        "query-only reads remain available while the writer is held"
    );
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    assert!(routes.writes.lock().unwrap().iter().any(|event| matches!(event,
        SqliteWriteEvent::Cancelled { class: SqliteWriteClass::Interactive, queue, .. } if queue.interactive == 0)));
    hold.rollback().await.unwrap();
    assert!(store.get_task_run(&run.id).await.unwrap().is_none());
    assert!(
        store
            .get_task_occurrence_contract_by_run(&run.id)
            .await
            .unwrap()
            .is_none()
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        store.accept_task_webhook(&task, &trigger, &run, occurrence, events),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn repeated_migration_up_preserves_saved_webhook_input_and_trigger_reference() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let store = postgres_store_with_workspace(&fixture, "ws_task").await;
    let (task, trigger) = webhook_fixture(&store).await;
    let (run, occurrence, events) = request(&task, &trigger, 1);
    store
        .accept_task_webhook(&task, &trigger, &run, occurrence, events)
        .await
        .unwrap()
        .unwrap();
    for _ in 0..2 {
        Migrator::up(fixture.connection(), None).await.unwrap();
    }
    assert_eq!(
        store
            .get_task_run(&run.id)
            .await
            .unwrap()
            .unwrap()
            .trigger_input,
        run.trigger_input
    );
    assert_eq!(
        store
            .get_webhook_task_trigger(&trigger.id)
            .await
            .unwrap()
            .unwrap()
            .webhook_secret_ref,
        trigger.webhook_secret_ref
    );
}
