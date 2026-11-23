use crate::event_store::TaskEventStore;
use crate::occurrence_repository::TaskOccurrenceRepository;
use crate::task_event_context::TaskEventContextRepository;
use crate::test_support::task_projection::NoFrozenOutput;
use crate::test_support::webhook::{AT, request, webhook_fixture};
use migration::{Migrator, MigratorTrait};
use pioneer_postgres_pools::{
    PostgresReadClass, PostgresReadEvent, PostgresReadObserver, PostgresWriteClass,
    PostgresWriteEvent, PostgresWriteObserver,
};
use pioneer_protocol::{TaskEventPayload, TaskOccurrenceContract, TaskStatus, TaskTriggerStatus};
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set, TransactionTrait};
use std::sync::{Arc, Mutex, atomic::Ordering};
use std::time::Duration;

#[derive(Default)]
struct Routes {
    reads: Mutex<Vec<PostgresReadClass>>,
    writes: Mutex<Vec<PostgresWriteEvent>>,
    watch_interactive: std::sync::atomic::AtomicBool,
    interactive_queued: tokio::sync::Notify,
}
impl PostgresReadObserver for Routes {
    fn observe(&self, event: PostgresReadEvent) {
        if let PostgresReadEvent::OperationFinished { class, .. } = event {
            self.reads.lock().unwrap().push(class);
        }
    }
}
impl PostgresWriteObserver for Routes {
    fn observe(&self, event: PostgresWriteEvent) {
        self.writes.lock().unwrap().push(event);
        if self.watch_interactive.load(Ordering::SeqCst)
            && matches!(
                event,
                PostgresWriteEvent::Enqueued {
                    class: PostgresWriteClass::Interactive,
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
) -> TaskEventStore<NoFrozenOutput> {
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let executor =
        pioneer_postgres_pools::PostgresWriteExecutor::with_observer(writer, routes.clone());
    TaskEventStore::new(
        pioneer_postgres_pools::PostgresDatabase::from_executor_with_read_observer(
            reader, executor, routes,
        ),
        NoFrozenOutput,
    )
}

async fn postgres_store_with_workspace(
    fixture: &pioneer_postgres_test::TestDatabase,
    workspace_id: &str,
) -> TaskEventStore<NoFrozenOutput> {
    let connection = fixture.connection();
    let tx = connection.begin().await.expect("begin workspace fixture");
    pioneer_entity::workspace::Entity::update_many()
        .col_expr(
            pioneer_entity::workspace::Column::IsCurrent,
            Expr::value(false),
        )
        .filter(pioneer_entity::workspace::Column::IsCurrent.eq(true))
        .exec(&tx)
        .await
        .expect("clear the seeded current workspace");
    let timestamp = crate::values::unix_to_datetime(1_700_000_000);
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set(workspace_id.to_owned()),
        name: Set("Test Workspace".to_owned()),
        is_active: Set(true),
        is_current: Set(true),
        created_at: Set(timestamp),
        updated_at: Set(timestamp),
    })
    .exec(&tx)
    .await
    .expect("insert workspace fixture");
    tx.commit().await.expect("commit workspace fixture");
    let reader = fixture
        .connect_read_only()
        .await
        .expect("read-only PostgreSQL pool");
    TaskEventStore::new(
        pioneer_postgres_pools::PostgresDatabase::new(reader, connection.clone()),
        NoFrozenOutput,
    )
}

async fn test_store_with_workspace(workspace_id: &str) -> TaskEventStore<NoFrozenOutput> {
    let fixture = pioneer_postgres_test::scoped_database()
        .await
        .expect("clone the PostgreSQL CRUD fixture database");
    postgres_store_with_workspace(&fixture, workspace_id).await
}

impl TaskEventStore<NoFrozenOutput> {
    async fn get_task_run(&self, id: &str) -> anyhow::Result<Option<pioneer_protocol::TaskRun>> {
        TaskEventContextRepository::new(&self.connection)
            .get_task_run(id)
            .await
    }

    async fn get_task_occurrence_contract_by_run(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<TaskOccurrenceContract>> {
        TaskOccurrenceRepository::new(self.connection.clone())
            .get_task_occurrence_contract_by_run(id)
            .await
    }

    async fn get_task_webhook_admission_snapshot(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<crate::webhook_admission::TaskWebhookAdmissionSnapshot>> {
        crate::webhook_admission::get_task_webhook_admission_snapshot(&self.connection, id).await
    }

    async fn get_webhook_task_trigger(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<pioneer_protocol::TaskTrigger>> {
        crate::webhook_admission::get_webhook_task_trigger(&self.connection, id).await
    }
}
#[pioneer_postgres_test::postgres_test]
async fn webhook_admission_inherits_request_and_maintenance_classes_with_read_only_readers() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    postgres_store_with_workspace(&fixture, "ws_task").await;
    let routes = Arc::new(Routes::default());
    let store = observed_store(&fixture, routes.clone()).await;
    let (_, trigger) = webhook_fixture(&store).await;
    assert!(store.connection.reader_is_read_only().await.unwrap());
    let mut accepted = Vec::new();
    for (number, scoped, read_class, write_class) in [
        (
            1,
            store.clone(),
            PostgresReadClass::Interactive,
            PostgresWriteClass::Interactive,
        ),
        (
            2,
            store.with_maintenance_access(),
            PostgresReadClass::Maintenance,
            PostgresWriteClass::Maintenance,
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
                PostgresWriteEvent::Enqueued { class, .. } => Some(*class),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(!enqueued.is_empty());
        assert!(enqueued.iter().all(|class| *class == write_class));
    }
    assert!(store.connection.reader_is_read_only().await.unwrap());
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
    assert_eq!(
        TaskEventContextRepository::new(&store.connection)
            .get_task_record(&task.id)
            .await
            .unwrap()
            .unwrap(),
        task
    );
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
        PostgresWriteEvent::Cancelled { class: PostgresWriteClass::Interactive, queue, .. } if queue.interactive == 0)));
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
        let expected = TaskEventContextRepository::new(&store.connection)
            .get_task_record(&task.id)
            .await
            .unwrap()
            .unwrap();
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
                    .request_task_finish(&task.id, None, AT + 3)
                    .await
                    .unwrap();
                let stopped = TaskEventContextRepository::new(&store.connection)
                    .get_task_record(&task.id)
                    .await
                    .unwrap()
                    .unwrap();
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
