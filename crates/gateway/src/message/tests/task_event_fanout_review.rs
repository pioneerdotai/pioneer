//! Review regressions. Compile only until orchestrator acceptance.
use super::*;
use crate::message::TaskEventFanoutSummary;
use pioneer_crud::{TASK_EVENT_FANOUT_BYTE_BUDGET as BYTES, TaskEventFanoutOutcome};
use pioneer_entity::{task_event, task_event_fanout_pending as pending};
use pioneer_sqlite::{SqliteWriteClass, SqliteWriteEvent, SqliteWriteObserver};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use std::sync::atomic::{AtomicI64, AtomicUsize};
const NOW: i64 = 4_000_000_000;

// The cutover fixture needs the cancellation-era schema for Gateway setup,
// but must not apply later irreversible migrations before its fanout rollback.
struct FanoutCutoverFixtureMigrator;
impl MigratorTrait for FanoutCutoverFixtureMigrator {
    fn migrations() -> Vec<Box<dyn migration::MigrationTrait>> {
        let mut migrations = Migrator::migrations();
        let target = migrations
            .iter()
            .position(|migration| {
                migration.name() == "m20261005_000001_native_cancellation_context"
            })
            .expect("cancellation migration remains registered");
        migrations.truncate(target + 1);
        migrations
    }
}

fn processor_with_store(
    manager: Arc<WorkspaceManager>,
    store: Arc<CrudStore>,
) -> Arc<MessageProcessor> {
    Arc::new(MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        test_provider(),
        Arc::new(SessionManager::new()),
        manager,
        store,
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    ))
}
async fn pending_row(processor: &MessageProcessor, id: &str) -> pending::Model {
    pending::Entity::find_by_id(id)
        .one(&processor.crud_store.database_connection())
        .await
        .unwrap()
        .unwrap()
}
async fn ready(processor: &MessageProcessor, id: &str, due: i64, attempts: i64) {
    pending::Entity::update_many()
        .col_expr(pending::Column::DueAt, sea_orm::sea_query::Expr::val(due))
        .col_expr(
            pending::Column::Attempts,
            sea_orm::sea_query::Expr::val(attempts),
        )
        .filter(pending::Column::TaskId.eq(id))
        .exec(
            &processor
                .crud_store
                .with_maintenance_access()
                .database_connection(),
        )
        .await
        .unwrap();
}
async fn create(processor: &MessageProcessor, workspace: &str, id: &str) {
    processor
        .crud_store
        .append_task_event(
            TaskEventPayload::TaskCreated {
                task: fanout_test_task(id, workspace),
            },
            NOW,
        )
        .await
        .unwrap();
}
async fn ack_created(processor: &MessageProcessor, id: &str) {
    processor
        .crud_store
        .with_maintenance_reads_and_critical_writes()
        .advance_task_event_fanout_cursor(id, 1)
        .await
        .unwrap();
}
fn assert_budget(summary: &TaskEventFanoutSummary) {
    assert!(summary.selected <= 64);
    assert!(summary.event_inputs <= 128);
    assert!(summary.emitted <= summary.event_inputs);
}

async fn competing_small_events(large_size: usize, prior_attempts: i64, restart: bool) {
    let (processor, workspace) = fanout_test_processor().await;
    create(&processor, &workspace, "budget-A").await;
    create(&processor, &workspace, "budget-B").await;
    ack_created(&processor, "budget-B").await;
    // A consumes more than half the full byte budget; B must be first later.
    fanout_test_append(&processor, "budget-A", &"a".repeat(BYTES * 3 / 4)).await;
    fanout_test_append(&processor, "budget-B", &"b".repeat(large_size)).await;
    let clock = AtomicI64::new(NOW);
    if prior_attempts > 0 {
        // Establish real delivery failures through the same dispatcher, rather
        // than only seeding an attempts number. The malformed stored payload
        // is a poison fixture, repaired below so byte admission can be tested.
        ready(&processor, "budget-A", NOW + 100, 0).await;
        ready(&processor, "budget-B", NOW - 201, 0).await;
        clock.store(NOW - 200, Ordering::SeqCst);
        let event = task_event::Entity::find()
            .filter(task_event::Column::TaskId.eq("budget-B"))
            .filter(task_event::Column::Sequence.eq(2_i64))
            .one(&processor.crud_store.database_connection())
            .await
            .unwrap()
            .unwrap();
        task_event::Entity::update_many()
            .col_expr(
                task_event::Column::PayloadJson,
                sea_orm::sea_query::Expr::val("{invalid"),
            )
            .filter(task_event::Column::Id.eq(event.id.clone()))
            .exec(&processor.crud_store.database_connection())
            .await
            .unwrap();
        for attempt in 1..=prior_attempts {
            let failed = processor
                .for_background_reconciliation()
                .task_event_fanout_quantum_with_clock(&|| clock.load(Ordering::SeqCst))
                .await
                .unwrap();
            assert_budget(&failed);
            assert_eq!(failed.errors, 1);
            assert_eq!(failed.emitted, 0);
            let retry = pending_row(&processor, "budget-B").await;
            assert_eq!(retry.attempts, attempt);
            assert!(retry.claim_token.is_none());
            clock.store(retry.due_at, Ordering::SeqCst);
        }
        task_event::Entity::update_many()
            .col_expr(
                task_event::Column::PayloadJson,
                sea_orm::sea_query::Expr::val(event.payload_json),
            )
            .filter(task_event::Column::Id.eq(event.id))
            .exec(&processor.crud_store.database_connection())
            .await
            .unwrap();
        clock.store(NOW, Ordering::SeqCst);
    }
    ready(&processor, "budget-A", NOW - 10, 0).await;
    ready(&processor, "budget-B", NOW - 9, prior_attempts).await;
    let original = pending_row(&processor, "budget-B").await;
    let first = processor
        .for_background_reconciliation()
        .task_event_fanout_quantum_with_clock(&|| clock.load(Ordering::SeqCst))
        .await
        .unwrap();
    assert_budget(&first);
    assert_eq!(first.errors, 0);
    assert_eq!(
        processor
            .crud_store
            .get_task_event_fanout_cursor("budget-B")
            .await
            .unwrap(),
        Some(1)
    );
    let deferred = pending_row(&processor, "budget-B").await;
    assert_eq!(
        deferred.due_at, original.due_at,
        "budget retains waiting priority"
    );
    assert_eq!(
        deferred.attempts, prior_attempts,
        "budget neither increments nor clears error history"
    );
    assert!(deferred.claim_token.is_none());
    // A keeps arriving before every periodic quantum, including this one.
    fanout_test_append(&processor, "budget-A", "new work before next tick").await;
    ready(&processor, "budget-A", NOW + 5, 0).await;
    clock.store(NOW + 5, Ordering::SeqCst);
    let processor = if restart {
        // Fresh dispatcher state, same durable frontier; no memory
        // replay cursor or fairness flag carried across restart.
        processor_with_store(
            processor.workspace_manager.clone(),
            processor.crud_store.clone(),
        )
    } else {
        processor
    };
    let second = processor
        .for_background_reconciliation()
        .task_event_fanout_quantum_with_clock(&|| clock.load(Ordering::SeqCst))
        .await
        .unwrap();
    assert_budget(&second);
    assert_eq!(second.errors, 0);
    assert_eq!(
        processor
            .crud_store
            .get_task_event_fanout_cursor("budget-B")
            .await
            .unwrap(),
        Some(2),
        "B gets the fresh budget despite fresh A work"
    );
    if large_size > BYTES {
        assert_eq!(
            second.emitted, 1,
            "one oversized event consumes this quantum's payload allowance"
        );
    }
    for tick in 2..5 {
        fanout_test_append(&processor, "budget-A", "continuous A arrivals").await;
        ready(&processor, "budget-A", NOW + tick * 5, 0).await;
        clock.store(NOW + tick * 5, Ordering::SeqCst);
        let summary = processor
            .for_background_reconciliation()
            .task_event_fanout_quantum_with_clock(&|| clock.load(Ordering::SeqCst))
            .await
            .unwrap();
        assert_budget(&summary);
        assert_eq!(summary.errors, 0);
    }
}
#[tokio::test]
async fn oversized_gets_full_quantum_despite_continuous_small_events_and_restart() {
    competing_small_events(BYTES + 4096, 0, true).await;
}
#[tokio::test]
async fn ordinary_full_budget_event_does_not_starve_on_remainder() {
    competing_small_events(BYTES / 2, 0, false).await;
}
#[tokio::test]
async fn budget_deferral_preserves_prior_error_history() {
    competing_small_events(BYTES + 4096, 4, false).await;
}

#[tokio::test]
async fn expired_budget_handler_cannot_change_replacement_holder_or_cursor_reset() {
    let (processor, workspace) = fanout_test_processor().await;
    create(&processor, &workspace, "holder-B").await;
    let clock = AtomicI64::new(NOW);
    ready(&processor, "holder-B", NOW - 1, 3).await;
    let store = processor.crud_store.with_maintenance_access();
    let old = store
        .claim_task_event_fanout(&pending_row(&processor, "holder-B").await, &|| {
            clock.load(Ordering::SeqCst)
        })
        .await
        .unwrap()
        .unwrap();
    clock.store(old.retry_at, Ordering::SeqCst);
    let new = store
        .claim_task_event_fanout(&pending_row(&processor, "holder-B").await, &|| {
            clock.load(Ordering::SeqCst)
        })
        .await
        .unwrap()
        .unwrap();
    let before = pending_row(&processor, "holder-B").await;
    store
        .release_task_event_fanout(&old, TaskEventFanoutOutcome::BudgetDeferred, &|| {
            clock.load(Ordering::SeqCst)
        })
        .await
        .unwrap();
    assert_eq!(pending_row(&processor, "holder-B").await, before);
    let reset = store.database_connection();
    pioneer_entity::task_event_fanout_cursor::Entity::delete_by_id("holder-B")
        .exec(&reset)
        .await
        .unwrap();
    let before = pending_row(&processor, "holder-B").await;
    store
        .release_task_event_fanout(&new, TaskEventFanoutOutcome::BudgetDeferred, &|| {
            clock.load(Ordering::SeqCst)
        })
        .await
        .unwrap();
    assert_eq!(pending_row(&processor, "holder-B").await, before);
    // The selected dispatcher restores a zero cursor in its known claim
    // commit, then emits and ACKs the first event above the saved floor.
    clock.store(NOW + 300, Ordering::SeqCst);
    let summary = processor
        .for_background_reconciliation()
        .task_event_fanout_quantum_with_clock(&|| clock.load(Ordering::SeqCst))
        .await
        .unwrap();
    assert_budget(&summary);
    assert_eq!(summary.errors, 0);
    assert_eq!(summary.emitted, 1);
    assert_eq!(summary.pending, Some(false));
    assert_eq!(
        processor
            .crud_store
            .get_task_event_fanout_cursor("holder-B")
            .await
            .unwrap(),
        Some(1)
    );
}

#[derive(Default)]
struct PanicObserver {
    maintenance_acquires: AtomicUsize,
    fail_released: AtomicUsize,
    events: std::sync::Mutex<Vec<SqliteWriteEvent>>,
}
impl SqliteWriteObserver for PanicObserver {
    fn observe(&self, event: SqliteWriteEvent) {
        // Never poison the observer mutex, and panic once. Released is called
        // after the executor drops admission capacity, but before commit's
        // caller learns its outcome: this exercises actual commit ambiguity.
        self.events.lock().unwrap().push(event);
        if matches!(
            event,
            SqliteWriteEvent::Acquired {
                class: SqliteWriteClass::Maintenance,
                ..
            }
        ) {
            self.maintenance_acquires.fetch_add(1, Ordering::SeqCst);
        }
        if matches!(
            event,
            SqliteWriteEvent::Released {
                class: SqliteWriteClass::Maintenance,
                ..
            }
        ) {
            let current = self.maintenance_acquires.load(Ordering::SeqCst);
            if self
                .fail_released
                .compare_exchange(current, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                panic!("injected synchronous writer-observer failure");
            }
        }
    }
}
async fn panic_candidate_lifecycle(mode: usize) {
    let observer = Arc::new(PanicObserver::default());
    let (directory, manager, store, workspace) =
        setup_pooled_file_workspace_manager_with_observer(Some(observer.clone())).await;
    let processor = processor_with_store(manager, store);
    // Append refreshes B's generation. Distinct due times keep A/B/C order
    // independent of that tie-breaker, so the fault targets B's intended phase.
    for (id, due) in [
        ("panic-A", NOW - 3),
        ("panic-B", NOW - 2),
        ("panic-C", NOW - 1),
    ] {
        create(&processor, &workspace, id).await;
        ready(&processor, id, due, 0).await;
    }
    if mode == 5 {
        // B's reservation update fails; its separate failure bookkeeping is
        // the next Maintenance transaction, and its commit observer panics.
        processor.crud_store.with_maintenance_access().database_connection().execute_unprepared("CREATE TRIGGER fail_B_claim BEFORE UPDATE OF claim_token ON task_event_fanout_pending WHEN NEW.task_id='panic-B' AND NEW.claim_token IS NOT NULL BEGIN SELECT RAISE(ABORT,'fixture'); END").await.unwrap();
    }
    if mode == 7 {
        fanout_test_append(&processor, "panic-B", "second confirmed prefix event").await;
    }
    assert_eq!(
        processor
            .crud_store
            .due_task_event_fanout(NOW, 64)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.task_id)
            .collect::<Vec<_>>(),
        vec!["panic-A", "panic-B", "panic-C"],
        "fixture must keep B between A and C after append refreshes generation"
    );
    observer.maintenance_acquires.store(0, Ordering::SeqCst);
    observer.events.lock().unwrap().clear();
    // A: claim, renewal, release. B: claim (#4), or failed-claim
    // bookkeeping (#5), or claim + two renewals + release (#7).
    observer.fail_released.store(mode, Ordering::SeqCst);
    let clock = AtomicI64::new(NOW);
    let summary = processor
        .for_background_reconciliation()
        .task_event_fanout_quantum_with_clock(&|| clock.load(Ordering::SeqCst))
        .await
        .unwrap();
    assert_budget(&summary);
    assert_eq!(summary.errors, 1);
    assert_eq!(summary.event_inputs, 128, "B's allocation is not refunded");
    assert_eq!(
        observer.maintenance_acquires.load(Ordering::SeqCst),
        match mode {
            4 => 7,
            5 => 8,
            7 => 10,
            _ => unreachable!(),
        },
        "no second reservation or blind compensation after the panic"
    );
    for id in ["panic-A", "panic-C"] {
        assert_eq!(
            processor
                .crud_store
                .get_task_event_fanout_cursor(id)
                .await
                .unwrap(),
            Some(1)
        );
    }
    assert_eq!(
        processor
            .crud_store
            .get_task_event_fanout_cursor("panic-B")
            .await
            .unwrap(),
        Some(if mode == 7 { 2 } else { 0 })
    );
    assert_eq!(summary.emitted, if mode == 7 { 4 } else { 2 });
    assert_eq!(
        observer.fail_released.load(Ordering::SeqCst),
        0,
        "fault exercised inside the real repository operation"
    );
    let events = observer.events.lock().unwrap().clone();
    assert!(events.iter().all(|e| !matches!(
        e,
        SqliteWriteEvent::Enqueued {
            class: SqliteWriteClass::Interactive,
            ..
        }
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        SqliteWriteEvent::Enqueued {
            class: SqliteWriteClass::Critical,
            ..
        }
    )));
    assert!(
        processor
            .crud_store
            .with_maintenance_access()
            .database_connection()
            .validate_reader()
            .await
            .is_ok()
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        assert!(
            processor
                .crud_store
                .get_task_record("panic-C")
                .await
                .unwrap()
                .is_some()
        );
        // Exercise serialized writer again after panic, then continue a real
        // quantum with the same owning dispatcher state.
        if mode == 5 {
            processor
                .crud_store
                .database_connection()
                .execute_unprepared("DROP TRIGGER fail_B_claim")
                .await
                .unwrap();
        }
        fanout_test_append(&processor, "panic-A", "next quantum A").await;
        fanout_test_append(&processor, "panic-C", "next quantum C").await;
        ready(&processor, "panic-A", NOW + 300, 0).await;
        ready(&processor, "panic-C", NOW + 300, 0).await;
        clock.store(NOW + 300, Ordering::SeqCst);
        let next = processor
            .for_background_reconciliation()
            .task_event_fanout_quantum_with_clock(&|| clock.load(Ordering::SeqCst))
            .await
            .unwrap();
        assert_budget(&next);
        assert_eq!(next.errors, 0);
        for id in ["panic-A", "panic-C"] {
            assert_eq!(
                processor
                    .crud_store
                    .get_task_event_fanout_cursor(id)
                    .await
                    .unwrap(),
                Some(2)
            );
        }
        assert_eq!(
            processor
                .crud_store
                .get_task_event_fanout_cursor("panic-B")
                .await
                .unwrap(),
            Some(if mode == 7 { 2 } else { 1 })
        );
    })
    .await
    .unwrap();
    processor
        .crud_store
        .database_connection()
        .close()
        .await
        .unwrap();
    drop(directory);
}
#[tokio::test]
async fn panic_unknown_claim_never_emits_b_and_preserves_a_c_progress() {
    panic_candidate_lifecycle(4).await;
}
#[tokio::test]
async fn panic_claim_failure_bookkeeping_preserves_other_candidates_and_capacity() {
    panic_candidate_lifecycle(5).await;
}
#[tokio::test]
async fn panic_release_preserves_confirmed_prefix_and_dispatcher_continues() {
    panic_candidate_lifecycle(7).await;
}

#[tokio::test]
async fn many_tasks_share_inputs_and_only_one_oversized_event_per_quantum() {
    let (processor, workspace) = fanout_test_processor().await;
    let clock = AtomicI64::new(NOW);
    for i in 0..70 {
        let id = format!("bounded-{i:03}");
        create(&processor, &workspace, &id).await;
        ack_created(&processor, &id).await;
        let payload = if i < 2 {
            "x".repeat(BYTES + 1024)
        } else {
            "small".to_owned()
        };
        fanout_test_append(&processor, &id, &payload).await;
        ready(&processor, &id, NOW - 70 + i, 0).await;
    }
    for quantum in 0..2 {
        let summary = processor
            .for_background_reconciliation()
            .task_event_fanout_quantum_with_clock(&|| clock.load(Ordering::SeqCst))
            .await
            .unwrap();
        assert_budget(&summary);
        assert_eq!(summary.selected, 64);
        assert_eq!(summary.emitted, 1);
        assert_eq!(summary.errors, 0);
        assert_eq!(
            processor
                .crud_store
                .get_task_event_fanout_cursor(&format!("bounded-{quantum:03}"))
                .await
                .unwrap(),
            Some(2)
        );
        assert_eq!(
            processor
                .crud_store
                .get_task_event_fanout_cursor("bounded-002")
                .await
                .unwrap(),
            Some(1)
        );
    }
    let summary = processor
        .for_background_reconciliation()
        .task_event_fanout_quantum_with_clock(&|| clock.load(Ordering::SeqCst))
        .await
        .unwrap();
    assert_budget(&summary);
    assert_eq!(summary.selected, 64);
    assert_eq!(summary.emitted, 64);
    assert_eq!(summary.event_inputs, 128);
    let last = processor
        .for_background_reconciliation()
        .task_event_fanout_quantum_with_clock(&|| clock.load(Ordering::SeqCst))
        .await
        .unwrap();
    assert_budget(&last);
    assert_eq!(last.emitted, 4);
    assert_eq!(last.pending, Some(false));
}

#[tokio::test]
async fn cutover_quantum_never_reads_history_and_only_delivers_post_install_events() {
    use pioneer_entity::task_event_fanout_cursor as cursor;
    use pioneer_sqlite::{SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteWriteExecutor};
    use sea_orm::{ConnectOptions, TransactionTrait};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cutover.sqlite");
    let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
    options.max_connections(1).sqlx_logging(false);
    let writer = Database::connect(options).await.unwrap();
    let (_, _, workspace) = setup_workspace_manager_with_connection_migrator::<
        FanoutCutoverFixtureMigrator,
    >(writer.clone())
    .await;
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
    options
        .max_connections(2)
        .sqlx_logging(false)
        .map_sqlx_sqlite_opts(|o| {
            o.read_only(true)
                .create_if_missing(false)
                .pragma("query_only", "ON")
        });
    let reader = Database::connect(options).await.unwrap();
    let observed = Arc::new(NativeSchedulingObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observed.clone()),
        observed.clone(),
    );
    let store = Arc::new(CrudStore::new(database.clone()));
    let sessions = Arc::new(SessionManager::new());
    let (tx, mut rx) = mpsc::channel(32);
    let connection = register_authenticated_test_connection(&sessions, tx).await;
    sessions
        .set_connection_workspace(connection, Some(workspace.clone()))
        .await;
    let make_processor = || {
        Arc::new(MessageProcessor::new(
            Arc::new(ThreadManager::new("test-model", "openai")),
            test_provider(),
            sessions.clone(),
            Arc::new(WorkspaceManager::new(database.clone())),
            store.clone(),
            test_gateway_secrets(),
            test_summary_config(),
            test_tool_loop_config(),
        ))
    };
    let processor = make_processor();
    let migrations = FanoutCutoverFixtureMigrator::migrations();
    let boundary = migrations
        .iter()
        .position(|m| m.name() == "m20261004_000008_task_event_fanout_pending")
        .unwrap();
    let tx = store
        .with_maintenance_access()
        .database_connection()
        .begin()
        .await
        .unwrap();
    FanoutCutoverFixtureMigrator::down(&*tx, Some((migrations.len() - boundary) as u32))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    create(&processor, &workspace, "cutover-old").await;
    for _ in 2..=100 {
        fanout_test_append(&processor, "cutover-old", "historical").await;
    }
    store
        .advance_task_event_fanout_cursor("cutover-old", 90)
        .await
        .unwrap();
    for n in 0..70 {
        create(&processor, &workspace, &format!("old-{n}")).await;
    }
    // Excluding this page must happen before size admission and JSON decode.
    task_event::Entity::update_many()
        .col_expr(
            task_event::Column::PayloadJson,
            sea_orm::sea_query::Expr::val("界".repeat(BYTES)),
        )
        .filter(task_event::Column::TaskId.eq("cutover-old"))
        .filter(task_event::Column::Sequence.eq(91_i64))
        .exec(&store.database_connection())
        .await
        .unwrap();
    task_event::Entity::update_many()
        .col_expr(
            task_event::Column::PayloadJson,
            sea_orm::sea_query::Expr::val("{invalid"),
        )
        .filter(task_event::Column::TaskId.eq("cutover-old"))
        .filter(task_event::Column::Sequence.eq(100_i64))
        .exec(&store.database_connection())
        .await
        .unwrap();
    let tx = store
        .with_maintenance_access()
        .database_connection()
        .begin()
        .await
        .unwrap();
    FanoutCutoverFixtureMigrator::up(&*tx, None).await.unwrap();
    tx.commit().await.unwrap();
    observed.reads.lock().unwrap().clear();
    observed.writes.lock().unwrap().clear();
    let empty = processor
        .for_background_reconciliation()
        .task_event_fanout_quantum_with_clock(&|| NOW)
        .await
        .unwrap();
    assert_eq!(
        (
            empty.selected,
            empty.event_inputs,
            empty.emitted,
            empty.errors
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(empty.pending, Some(false));
    assert!(observed.writes.lock().unwrap().is_empty());
    let reads = observed
        .reads
        .lock()
        .unwrap()
        .iter()
        .filter_map(|e| match e {
            SqliteReadEvent::OperationFinished { class, .. } => Some(*class),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        reads,
        vec![SqliteReadClass::Maintenance; 2],
        "only due pending and queue-state probes; no Task/cursor/event history reads"
    );
    assert!(rx.try_recv().is_err());
    fanout_test_append(&processor, "cutover-old", "new 101").await;
    fanout_test_append(&processor, "cutover-old", "new 102").await;
    assert_eq!(
        pending_row(&processor, "cutover-old").await.first_sequence,
        101
    );
    let second = task_event::Entity::find()
        .filter(task_event::Column::TaskId.eq("cutover-old"))
        .filter(task_event::Column::Sequence.eq(102_i64))
        .one(&store.database_connection())
        .await
        .unwrap()
        .unwrap();
    task_event::Entity::update_many()
        .col_expr(
            task_event::Column::PayloadJson,
            sea_orm::sea_query::Expr::val("{invalid-new"),
        )
        .filter(task_event::Column::Id.eq(second.id.clone()))
        .exec(&store.database_connection())
        .await
        .unwrap();
    // Restart before claim, keeping only the durable floor and reservation state.
    drop(processor);
    let processor = make_processor();
    let partial = processor
        .for_background_reconciliation()
        .task_event_fanout_quantum_with_clock(&|| NOW)
        .await
        .unwrap();
    assert_budget(&partial);
    assert_eq!((partial.emitted, partial.errors), (1, 1));
    let notification = recv_notification_by_method(&mut rx, events::TASK_PROGRESS).await;
    assert_eq!(notification.params.unwrap()["context"]["sequence"], 101);
    assert_eq!(
        store
            .get_task_event_fanout_cursor("cutover-old")
            .await
            .unwrap(),
        Some(101)
    );
    let retry = pending_row(&processor, "cutover-old").await;
    assert_eq!(retry.first_sequence, 101);
    assert!(retry.due_at > NOW);
    drop(processor);
    let processor = make_processor();
    let waiting = processor
        .for_background_reconciliation()
        .task_event_fanout_quantum_with_clock(&|| NOW)
        .await
        .unwrap();
    assert_eq!(waiting.selected, 0);
    assert_eq!(waiting.pending, Some(true));
    assert_eq!(pending_row(&processor, "cutover-old").await, retry);
    task_event::Entity::update_many()
        .col_expr(
            task_event::Column::PayloadJson,
            sea_orm::sea_query::Expr::val(second.payload_json),
        )
        .filter(task_event::Column::Id.eq(second.id))
        .exec(&store.database_connection())
        .await
        .unwrap();
    let delivered = processor
        .for_background_reconciliation()
        .task_event_fanout_quantum_with_clock(&|| retry.due_at)
        .await
        .unwrap();
    assert_budget(&delivered);
    assert_eq!((delivered.emitted, delivered.errors), (1, 0));
    let notification = recv_notification_by_method(&mut rx, events::TASK_PROGRESS).await;
    assert_eq!(notification.params.unwrap()["context"]["sequence"], 102);
    assert!(rx.try_recv().is_err());
    assert_eq!(
        store
            .get_task_event_fanout_cursor("cutover-old")
            .await
            .unwrap(),
        Some(102)
    );
    cursor::Entity::delete_by_id("cutover-old")
        .exec(&store.database_connection())
        .await
        .unwrap();
    let empty = processor
        .for_background_reconciliation()
        .task_event_fanout_quantum_with_clock(&|| NOW)
        .await
        .unwrap();
    assert_eq!((empty.selected, empty.errors), (0, 0));
    assert_eq!(empty.pending, Some(false));
    // A new INSERT after cursor deletion retains its own floor and gets a zero
    // cursor in the claim transaction, without reopening 1..102.
    fanout_test_append(&processor, "cutover-old", "new 103").await;
    cursor::Entity::delete_by_id("cutover-old")
        .exec(&store.database_connection())
        .await
        .unwrap();
    ready(&processor, "cutover-old", NOW, 0).await;
    let delivered = processor
        .for_background_reconciliation()
        .task_event_fanout_quantum_with_clock(&|| NOW)
        .await
        .unwrap();
    assert_eq!((delivered.emitted, delivered.errors), (1, 0));
    let notification = recv_notification_by_method(&mut rx, events::TASK_PROGRESS).await;
    assert_eq!(notification.params.unwrap()["context"]["sequence"], 103);
    assert!(rx.try_recv().is_err());
    // A new Task's first event also survives late cursor creation.
    create(&processor, &workspace, "cutover-new").await;
    fanout_test_append(&processor, "cutover-new", "second").await;
    cursor::Entity::delete_by_id("cutover-new")
        .exec(&store.database_connection())
        .await
        .unwrap();
    ready(&processor, "cutover-new", NOW, 0).await;
    let delivered = processor
        .for_background_reconciliation()
        .task_event_fanout_quantum_with_clock(&|| NOW)
        .await
        .unwrap();
    assert_eq!((delivered.emitted, delivered.errors), (2, 0));
    let created = recv_notification_by_method(&mut rx, events::TASK_CREATED).await;
    assert_eq!(created.params.unwrap()["context"]["sequence"], 1);
    let progress = recv_notification_by_method(&mut rx, events::TASK_PROGRESS).await;
    assert_eq!(progress.params.unwrap()["context"]["sequence"], 2);
    assert!(rx.try_recv().is_err());
}
