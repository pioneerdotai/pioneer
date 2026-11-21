use super::*;
use crate::event_store::TaskEventStore;
use crate::task_event_context::TaskEventContextRepository;
use crate::test_support::task_projection::{NoFrozenOutput, sample_task, sample_task_run};
use crate::values::unix_to_datetime;
use pioneer_entity::{
    task_run as runs, task_run_occurrence_reconcile_pending as pending,
    task_run_occurrence_reconcile_sequence as sequence, turn as turns,
};
use pioneer_protocol::{TaskEventPayload, TaskRun};
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, EntityTrait, IntoActiveModel, QueryFilter,
    QuerySelect, QueryTrait, Set, Statement, TransactionTrait,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

const NOW: i64 = 4_000_000_000;

#[derive(Default)]
struct Routes {
    maintenance_queued: tokio::sync::Notify,
    watch_claim: AtomicBool,
}
impl pioneer_sqlite::SqliteReadObserver for Routes {
    fn observe(&self, _: pioneer_sqlite::SqliteReadEvent) {}
}
impl pioneer_sqlite::SqliteWriteObserver for Routes {
    fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
        if self.watch_claim.load(Ordering::SeqCst)
            && matches!(
                event,
                pioneer_sqlite::SqliteWriteEvent::Enqueued {
                    class: pioneer_sqlite::SqliteWriteClass::Maintenance,
                    ..
                }
            )
        {
            self.maintenance_queued.notify_one();
        }
    }
}

async fn fixture_db() -> SqliteDatabase {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    SqliteDatabase::new(
        fixture.connect_read_only().await.unwrap(),
        fixture.connection().clone(),
    )
}

async fn terminal_fixture() -> (SqliteDatabase, TaskRun) {
    terminal_fixture_with_turn(true).await
}

async fn terminal_fixture_with_turn(include_occurrence: bool) -> (SqliteDatabase, TaskRun) {
    let db = fixture_db().await;
    let run = seed_pair_with_turn(&db, include_occurrence).await;
    (db, run)
}

async fn seed_pair(db: &SqliteDatabase) -> TaskRun {
    seed_pair_with_turn(db, true).await
}

async fn seed_pair_with_turn(db: &SqliteDatabase, include_occurrence: bool) -> TaskRun {
    db.execute_unprepared(
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws_task','Tracker',TRUE,FALSE);
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class)
             VALUES ('thr_task','ws_task','','agent','gpt-5.4','openai','active','user','workspace');
         INSERT INTO turn(id,thread_id,status,turn_kind,origin)
             VALUES ('turn_task','thr_task','completed','conversation','user');",
    ).await.unwrap();
    let timestamp = 1_700_000_000;
    let task = sample_task(timestamp);
    let mut run = sample_task_run(timestamp);
    run.trigger_id = None;
    TaskEventStore::new(db.clone(), NoFrozenOutput)
        .append_task_events(
            vec![
                TaskEventPayload::TaskCreated { task: task.clone() },
                TaskEventPayload::RunCreated {
                    run: run.clone(),
                    agent_spec: None,
                },
                TaskEventPayload::RunStarted {
                    task_id: task.id.clone(),
                    run_id: run.id.clone(),
                    started_at: timestamp + 1,
                },
                TaskEventPayload::RunCompleted {
                    task_id: task.id,
                    run_id: run.id.clone(),
                    result: None,
                    completed_at: timestamp + 2,
                },
            ],
            timestamp,
        )
        .await
        .unwrap();
    let run = TaskEventContextRepository::new(db)
        .get_task_run(&run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, TaskRunStatus::Succeeded);
    if include_occurrence {
        insert_occurrence(db, &run.id).await;
    }
    run
}

async fn insert_occurrence(db: &SqliteDatabase, run_id: &str) {
    insert_occurrence_with_kind(db, run_id, "task_run").await;
}

async fn insert_occurrence_with_kind(db: &SqliteDatabase, run_id: &str, kind: &str) {
    let tx = db.begin().await.unwrap();
    tx.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin) VALUES ($1,'thr_task','in_progress',$2,'scheduled_task')",
        [run_id.into(), kind.into()],
    )).await.unwrap();
    let turn = turns::Entity::find_by_id(run_id)
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    crate::task_occurrence_producers::turn_change(&tx, None, Some(&turn))
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

async fn observed_store(
    fixture: &pioneer_postgres_test::TestDatabase,
    routes: Arc<Routes>,
) -> SqliteDatabase {
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture
        .connect_with_options(|options| {
            options
                .max_connections(1)
                .map_sqlx_postgres_opts(|options| {
                    options.options([("default_transaction_read_only", "on")])
                });
        })
        .await
        .unwrap();
    let executor = pioneer_sqlite::SqliteWriteExecutor::with_observer(writer, routes.clone());
    SqliteDatabase::from_executor_with_read_observer(reader, executor, routes)
}

async fn row(store: &SqliteDatabase, id: &str) -> Option<pending::Model> {
    pending::Entity::find_by_id(id.to_owned())
        .one(store)
        .await
        .unwrap()
}

async fn generation(store: &SqliteDatabase) -> i64 {
    sequence::Entity::find_by_id(1)
        .one(store)
        .await
        .unwrap()
        .unwrap()
        .generation
}

async fn update_run_in_transaction(
    tx: &sea_orm::DatabaseTransaction,
    id: &str,
    update: sea_orm::UpdateMany<runs::Entity>,
) -> anyhow::Result<()> {
    let before = runs::Entity::find_by_id(id)
        .lock_exclusive()
        .one(tx)
        .await?;
    update.filter(runs::Column::Id.eq(id)).exec(tx).await?;
    let after = runs::Entity::find_by_id(id).one(tx).await?;
    crate::task_occurrence_producers::run_change(tx, before.as_ref(), after.as_ref()).await
}

#[pioneer_postgres_test::postgres_test]
async fn generation_overflow_and_missing_singleton_reject_source_write_atomically() {
    let (store, run) = terminal_fixture().await;
    sequence::Entity::update_many()
        .col_expr(sequence::Column::Generation, Expr::val(i64::MAX))
        .exec(&store)
        .await
        .unwrap();
    let before = row(&store, &run.id).await.unwrap();
    let tx = store.begin().await.unwrap();
    assert!(
        update_run_in_transaction(
            &tx,
            &run.id,
            runs::Entity::update_many().col_expr(
                runs::Column::CompletedAt,
                Expr::val(Some(unix_to_datetime(NOW))),
            ),
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    assert_eq!(generation(&store).await, i64::MAX);
    assert_eq!(row(&store, &run.id).await.unwrap(), before);
    sequence::Entity::delete_by_id(1)
        .exec(&store)
        .await
        .unwrap();
    let tx = store.begin().await.unwrap();
    // Changing to a different mismatched status still requires publication.
    assert!(
        update_run_in_transaction(
            &tx,
            &run.id,
            runs::Entity::update_many().col_expr(runs::Column::Status, Expr::val("failed")),
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    assert_eq!(row(&store, &run.id).await.unwrap(), before);
}

#[pioneer_postgres_test::postgres_test]
async fn due_plan_empty_deferred_and_due_backlog_use_covering_index_without_sort() {
    let store = fixture_db().await;
    for phase in ["empty", "deferred", "backlog"] {
        if phase != "empty" {
            // Service rows are fixture data, never a scan of domain history.
            for chunk in 0..32 {
                let values = (0..128)
                    .map(|offset| {
                        let id = chunk * 128 + offset;
                        pending::ActiveModel {
                            run_id: Set(format!("plan_{id:05}")),
                            generation: Set(id + 1),
                            next_attempt_at: Set(if phase == "deferred" {
                                NOW + 100
                            } else {
                                NOW - 100
                            }),
                            attempt_count: Set(0),
                            claim_token: Set(None),
                        }
                    })
                    .collect::<Vec<_>>();
                if phase == "deferred" {
                    pending::Entity::insert_many(values)
                        .exec(&store)
                        .await
                        .unwrap();
                }
            }
            if phase == "backlog" {
                pending::Entity::update_many()
                    .col_expr(pending::Column::NextAttemptAt, Expr::val(NOW - 100))
                    .exec(&store)
                    .await
                    .unwrap();
            }
        }
        let statement = queue::due_query(NOW, 64).build(DatabaseBackend::Postgres);
        assert!(!statement.sql.contains("task_run "));
        assert!(!statement.sql.contains("JOIN"));
        // EXPLAIN (COSTS OFF) has no SeaQuery builder; preserve builder values.
        let explain = Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            format!("EXPLAIN (COSTS OFF) {}", statement.sql),
            statement.values.unwrap().0,
        );
        let plan = store.query_all_raw(explain).await.unwrap();
        let details = plan
            .into_iter()
            .map(|r| r.try_get::<String>("", "QUERY PLAN").unwrap())
            .collect::<Vec<_>>();
        assert!(
            details
                .iter()
                .any(|d| d.contains("idx_task_run_occurrence_reconcile_due")),
            "{phase}: {details:?}"
        );
        assert!(
            details.iter().all(|d| !d.contains("Sort")),
            "{phase}: {details:?}"
        );
        assert_eq!(
            TaskRunOccurrenceQueue::new(&store)
                .discover_task_run_occurrence_reconcile(NOW, 10000)
                .await
                .unwrap()
                .len(),
            if phase == "backlog" { 64 } else { 0 }
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn attempt_count_saturates_but_poison_row_stays_retryable_forever() {
    let (store, run) = terminal_fixture().await;
    let candidate = TaskRunOccurrenceQueue::new(&store)
        .discover_task_run_occurrence_reconcile(NOW, 64)
        .await
        .unwrap()
        .remove(0);
    let mut now = NOW;
    for attempt in 1..=24 {
        let claim = TaskRunOccurrenceQueue::new(&store)
            .claim_task_run_occurrence_reconcile(&candidate, &|| now)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claim.attempt_count, std::cmp::min(attempt, 16));
        let expected_delay = [5, 10, 20, 40, 80, 160, 300][(attempt as usize - 1).min(6)];
        assert_eq!(claim.next_attempt_at, now + expected_delay);
        let stored = row(&store, &run.id).await.unwrap();
        assert_eq!(stored.next_attempt_at, now + expected_delay);
        assert_eq!(
            stored.claim_token.as_deref(),
            Some(claim.claim_token.as_str())
        );
        now = claim.next_attempt_at;
    }
}

#[pioneer_postgres_test::postgres_test]
async fn failed_claim_deferral_fences_generation_token_due_count_and_ambiguous_success() {
    use queue::{
        TaskRunOccurrenceClaimDeferral as Deferral, TaskRunOccurrenceClaimFailurePhase as Phase,
    };
    let (store, run) = terminal_fixture().await;
    let maintenance = store.maintenance();
    let candidate = TaskRunOccurrenceQueue::new(&store)
        .discover_task_run_occurrence_reconcile(NOW, 64)
        .await
        .unwrap()
        .remove(0);
    // The PostgreSQL fault trigger rejects only token updates,
    // preserving ordinary service bookkeeping writes for the same candidate.
    maintenance.execute_unprepared("CREATE FUNCTION reject_claim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'individual claim refusal'; END; $$; CREATE TRIGGER reject_claim BEFORE UPDATE OF claim_token ON task_run_occurrence_reconcile_pending FOR EACH ROW EXECUTE FUNCTION reject_claim()").await.unwrap();
    let failure = match queue::claim(&maintenance, &candidate, "rejected".into(), &|| NOW).await {
        Err(failure) => failure,
        _ => panic!("claim must be rejected"),
    };
    assert_eq!(failure.phase, Phase::Reservation);
    let snapshot = failure.snapshot.unwrap();
    assert_eq!(
        queue::defer_failed_claim(&maintenance, &snapshot, &|| NOW)
            .await
            .unwrap(),
        Deferral::Deferred
    );
    let deferred = row(&store, &run.id).await.unwrap();
    assert_eq!(deferred.attempt_count, 1);
    assert_eq!(deferred.next_attempt_at, NOW + 5);
    assert_eq!(deferred.claim_token, snapshot.claim_token);
    assert_eq!(
        queue::defer_failed_claim(&maintenance, &snapshot, &|| NOW + 100)
            .await
            .unwrap(),
        Deferral::StateChanged
    );
    assert_eq!(row(&store, &run.id).await.unwrap(), deferred);
    // Source refresh must also invalidate the token. Remove the injected token
    // write blocker before changing the source, while retaining the failed
    // attempt's exact snapshot to test its later stale deferral.
    maintenance
        .execute_unprepared("DROP TRIGGER reject_claim ON task_run_occurrence_reconcile_pending")
        .await
        .unwrap();
    // A late result of the same refusal cannot postpone a new generation.
    let tx = store.begin().await.unwrap();
    update_run_in_transaction(
        &tx,
        &run.id,
        runs::Entity::update_many().col_expr(
            runs::Column::ErrorJson,
            Expr::val(r#"{"message":"changed"}"#),
        ),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let updated = row(&store, &run.id).await.unwrap();
    assert_eq!(
        queue::defer_failed_claim(&maintenance, &deferred, &|| NOW + 100)
            .await
            .unwrap(),
        Deferral::StateChanged
    );
    assert_eq!(row(&store, &run.id).await.unwrap(), updated);
    let candidate = TaskRunOccurrenceReconcileCandidate {
        run_id: run.id.clone(),
        generation: updated.generation,
    };
    let first = TaskRunOccurrenceQueue::new(&store)
        .claim_task_run_occurrence_reconcile(&candidate, &|| NOW + 5)
        .await
        .unwrap()
        .unwrap();
    let old_holder_snapshot = row(&store, &run.id).await.unwrap();
    let second = TaskRunOccurrenceQueue::new(&store)
        .claim_task_run_occurrence_reconcile(&candidate, &|| first.next_attempt_at)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first.claim_token, second.claim_token);
    let current = row(&store, &run.id).await.unwrap();
    assert_eq!(
        queue::defer_failed_claim(&maintenance, &old_holder_snapshot, &|| NOW + 100)
            .await
            .unwrap(),
        Deferral::StateChanged
    );
    // Model a lost commit acknowledgement: the successful token/due/count are
    // durable, but the caller holds only its pre-commit snapshot for deferral.
    assert_eq!(
        queue::defer_failed_claim(&maintenance, &updated, &|| NOW + 100)
            .await
            .unwrap(),
        Deferral::StateChanged
    );
    assert_eq!(row(&store, &run.id).await.unwrap(), current);
}

#[pioneer_postgres_test::postgres_test]
async fn unavailable_bookkeeping_is_reported_without_claim_or_durable_backoff_promise() {
    use queue::{
        TaskRunOccurrenceClaimDeferral as Deferral, TaskRunOccurrenceClaimFailure,
        TaskRunOccurrenceClaimFailurePhase as Phase,
    };
    let (store, run) = terminal_fixture().await;
    let candidate = TaskRunOccurrenceQueue::new(&store)
        .discover_task_run_occurrence_reconcile(NOW, 64)
        .await
        .unwrap()
        .remove(0);
    let before = row(&store, &run.id).await.unwrap();
    // Reject every service update, including the fallback deferral. Trigger
    // This test installs the trigger only in its own database.
    store.execute_unprepared("CREATE FUNCTION reject_bookkeeping() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'bookkeeping unavailable'; END; $$; CREATE TRIGGER reject_bookkeeping BEFORE UPDATE ON task_run_occurrence_reconcile_pending FOR EACH ROW EXECUTE FUNCTION reject_bookkeeping()").await.unwrap();
    let failure = TaskRunOccurrenceQueue::new(&store)
        .claim_task_run_occurrence_reconcile(&candidate, &|| NOW)
        .await
        .unwrap_err()
        .downcast::<TaskRunOccurrenceClaimFailure>()
        .unwrap();
    assert_eq!(failure.phase, Phase::Reservation);
    assert_eq!(failure.deferral, Deferral::Failed);
    assert!(failure.deferral_error.is_some());
    assert_eq!(row(&store, &run.id).await.unwrap(), before);
    store.clone().close().await.unwrap();
    let failure = TaskRunOccurrenceQueue::new(&store)
        .claim_task_run_occurrence_reconcile(&candidate, &|| NOW)
        .await
        .unwrap_err()
        .downcast::<TaskRunOccurrenceClaimFailure>()
        .unwrap();
    assert_eq!(failure.phase, Phase::AdvisoryRead);
    assert_eq!(failure.deferral, Deferral::NoSnapshot);
}

#[pioneer_postgres_test::postgres_test]
async fn claim_uses_time_after_discovery_instead_of_the_advisory_clock() {
    let (store, _) = terminal_fixture().await;
    let clock = std::sync::atomic::AtomicI64::new(NOW);
    let candidate = TaskRunOccurrenceQueue::new(&store)
        .discover_task_run_occurrence_reconcile(clock.load(Ordering::SeqCst), 64)
        .await
        .unwrap()
        .remove(0);
    clock.store(NOW + 100, Ordering::SeqCst);
    let claim = TaskRunOccurrenceQueue::new(&store)
        .claim_task_run_occurrence_reconcile(&candidate, &|| clock.load(Ordering::SeqCst))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.next_attempt_at, NOW + 105);
    assert_eq!(
        row(&store, &candidate.run_id)
            .await
            .unwrap()
            .next_attempt_at,
        NOW + 105
    );
}

#[pioneer_postgres_test::postgres_test]
async fn claim_and_failed_claim_deferral_use_time_after_waiting_for_writer() {
    use std::time::Duration;
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let routes = Arc::new(Routes::default());
    let store = observed_store(&fixture, routes.clone()).await;
    let id = seed_pair(&store).await.id;
    let candidate = TaskRunOccurrenceQueue::new(&store)
        .discover_task_run_occurrence_reconcile(NOW, 64)
        .await
        .unwrap()
        .remove(0);
    let clock = Arc::new(std::sync::atomic::AtomicI64::new(NOW));
    let hold = store.begin().await.unwrap();
    routes.watch_claim.store(true, Ordering::SeqCst);
    let worker = store.clone();
    let c = candidate.clone();
    let time = clock.clone();
    let waiting = tokio::spawn(async move {
        TaskRunOccurrenceQueue::new(&worker)
            .claim_task_run_occurrence_reconcile(&c, &|| time.load(Ordering::SeqCst))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), routes.maintenance_queued.notified())
        .await
        .unwrap();
    clock.store(NOW + 100, Ordering::SeqCst);
    hold.rollback().await.unwrap();
    let claim = waiting.await.unwrap().unwrap().unwrap();
    assert_eq!(claim.next_attempt_at, NOW + 105);
    let snapshot = row(&store, &id).await.unwrap();
    clock.store(claim.next_attempt_at, Ordering::SeqCst);
    routes.watch_claim.store(false, Ordering::SeqCst);
    let hold = store.begin().await.unwrap();
    routes.watch_claim.store(true, Ordering::SeqCst);
    let worker = store.clone();
    let time = clock.clone();
    let waiting = tokio::spawn(async move {
        queue::defer_failed_claim(&worker.maintenance(), &snapshot, &|| {
            time.load(Ordering::SeqCst)
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), routes.maintenance_queued.notified())
        .await
        .unwrap();
    clock.store(NOW + 200, Ordering::SeqCst);
    hold.rollback().await.unwrap();
    assert_eq!(
        waiting.await.unwrap().unwrap(),
        queue::TaskRunOccurrenceClaimDeferral::Deferred
    );
    let row = row(&store, &id).await.unwrap();
    assert_eq!(row.attempt_count, 2);
    assert_eq!(row.next_attempt_at, NOW + 210);
    assert_eq!(row.claim_token.as_deref(), Some(claim.claim_token.as_str()));
    store.close().await.unwrap();
}

async fn claimed(store: &SqliteDatabase) -> TaskRunOccurrenceReconcileClaim {
    let queue = TaskRunOccurrenceQueue::new(store);
    let candidates = queue
        .discover_task_run_occurrence_reconcile(NOW, 64)
        .await
        .unwrap();
    assert_eq!(candidates.len(), 1);
    queue
        .claim_task_run_occurrence_reconcile(&candidates[0], &|| NOW)
        .await
        .unwrap()
        .unwrap()
}

async fn reconcile_without_publication(
    connection: &SqliteDatabase,
    claim: &TaskRunOccurrenceReconcileClaim,
) -> Result<TaskRunOccurrenceTerminalizationOutcome> {
    let scoped = connection.maintenance().with_critical_writes();
    TaskRunOccurrenceQueue::new(&scoped)
        .reconcile_claim(
            claim,
            || async {
                match OccurrenceSourceFacts::prepare(&scoped, claim.run_id.clone(), NOW, true)
                    .await?
                {
                    OccurrencePreparation::Unchanged(outcome) => {
                        Ok(OccurrenceRepairPreparation::Unchanged(outcome))
                    }
                    OccurrencePreparation::Ready { .. } => {
                        panic!("fixture requires no terminal publication")
                    }
                }
            },
            |()| async { panic!("fixture requires no terminal publication") },
        )
        .await
}

async fn remove_run(store: &SqliteDatabase, id: &str) -> runs::Model {
    let tx = store.begin().await.unwrap();
    let saved = runs::Entity::find_by_id(id)
        .lock_exclusive()
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    runs::Entity::delete_by_id(id).exec(&tx).await.unwrap();
    crate::task_occurrence_producers::run_change(&tx, Some(&saved), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    saved
}

async fn restore_run(store: &SqliteDatabase, saved: runs::Model) {
    let tx = store.begin().await.unwrap();
    let inserted = runs::Entity::insert(saved.into_active_model())
        .exec_with_returning(&tx)
        .await
        .unwrap();
    crate::task_occurrence_producers::run_change(&tx, None, Some(&inserted))
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

async fn run_status(store: &SqliteDatabase, id: &str, status: &str) {
    let tx = store.begin().await.unwrap();
    update_run_in_transaction(
        &tx,
        id,
        runs::Entity::update_many().col_expr(runs::Column::Status, Expr::val(status)),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}
async fn turn_status(store: &SqliteDatabase, id: &str, status: &str) {
    let tx = store.begin().await.unwrap();
    let before = turns::Entity::find_by_id(id)
        .lock_exclusive()
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    turns::Entity::update_many()
        .col_expr(turns::Column::Status, Expr::val(status))
        .filter(turns::Column::Id.eq(id))
        .exec(&tx)
        .await
        .unwrap();
    let after = turns::Entity::find_by_id(id)
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    crate::task_occurrence_producers::turn_change(&tx, Some(&before), Some(&after))
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn same_status_error_only_and_noncanonical_or_missing_pairs_do_not_enqueue() {
    let (store, run) = terminal_fixture().await;
    turn_status(&store, &run.id, "completed").await;
    runs::Entity::update_many()
        .col_expr(
            runs::Column::ErrorJson,
            Expr::val(Some("{\"message\":\"different\"}")),
        )
        .filter(runs::Column::Id.eq(&run.id))
        .exec(&store)
        .await
        .unwrap();
    assert!(row(&store, &run.id).await.is_none());
    turns::Entity::update_many()
        .col_expr(turns::Column::Status, Expr::val("failed"))
        .col_expr(turns::Column::TurnKind, Expr::val("conversation"))
        .filter(turns::Column::Id.eq(&run.id))
        .exec(&store)
        .await
        .unwrap();
    assert!(row(&store, &run.id).await.is_none());
    let (missing, run) = terminal_fixture_with_turn(false).await;
    // A terminal run may race the first Turn insert on another transaction.
    // An advisory claim must retire safely when its counterpart is absent.
    let claim = claimed(&missing).await;
    assert_eq!(claim.run_id, run.id);
    assert_eq!(
        reconcile_without_publication(&missing, &claim)
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::NotFound,
    );
    assert!(row(&missing, &run.id).await.is_none());
}

#[pioneer_postgres_test::postgres_test]
async fn both_late_inserts_deletes_and_id_moves_track_exact_current_pairs() {
    let (store, run) = terminal_fixture_with_turn(false).await;
    // Keep the fixture's identity separate from canonical event history, so DML
    // can move it while all PostgreSQL constraints remain active.
    insert_occurrence(&store, &run.id).await;
    let first = row(&store, &run.id).await.unwrap();
    let saved = remove_run(&store, &run.id).await;
    assert!(row(&store, &run.id).await.is_none());
    restore_run(&store, saved).await;
    let second = row(&store, &run.id).await.unwrap();
    assert!(
        second.generation > first.generation,
        "delete/reinsert must exclude ABA"
    );
    for (old, new) in [
        (run.id.as_str(), "moved_run"),
        ("moved_run", run.id.as_str()),
    ] {
        let tx = store.begin().await.unwrap();
        let before = runs::Entity::find_by_id(old)
            .lock_exclusive()
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        runs::Entity::update_many()
            .col_expr(runs::Column::Id, Expr::val(new))
            .filter(runs::Column::Id.eq(old))
            .exec(&tx)
            .await
            .unwrap();
        let after = runs::Entity::find_by_id(new)
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        crate::task_occurrence_producers::run_change(&tx, Some(&before), Some(&after))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        if new == "moved_run" {
            let claim = claimed(&store).await;
            assert_eq!(claim.run_id, new);
            assert_eq!(
                reconcile_without_publication(&store, &claim).await.unwrap(),
                TaskRunOccurrenceTerminalizationOutcome::NotFound,
            );
        }
        assert!(row(&store, "moved_run").await.is_none());
    }
    let third = row(&store, &run.id).await.unwrap();
    assert!(third.generation > second.generation);
    for (old, new) in [
        (run.id.as_str(), "moved_turn"),
        ("moved_turn", run.id.as_str()),
    ] {
        // INSERT always creates compaction bookkeeping with a non-cascading
        // identity FK. Move that dependent atomically with this physical DML
        // fixture, preserving its sequence and enforcing every FK throughout.
        use pioneer_entity::compaction_turn_creation as creation;
        let tx = store.begin().await.unwrap();
        let before = turns::Entity::find_by_id(old)
            .lock_exclusive()
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        let dependent = creation::Entity::find()
            .filter(creation::Column::TurnId.eq(old))
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        creation::Entity::delete_by_id(dependent.sequence)
            .exec(&tx)
            .await
            .unwrap();
        turns::Entity::update_many()
            .col_expr(turns::Column::Id, Expr::val(new))
            .filter(turns::Column::Id.eq(old))
            .exec(&tx)
            .await
            .unwrap();
        tx.execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "INSERT INTO compaction_turn_creation(sequence,turn_id) OVERRIDING SYSTEM VALUE VALUES ($1,$2)",
            [dependent.sequence.into(), new.into()],
        )).await.unwrap();
        let after = turns::Entity::find_by_id(new)
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        crate::task_occurrence_producers::turn_change(&tx, Some(&before), Some(&after))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        if new == "moved_turn" {
            let claim = claimed(&store).await;
            assert_eq!(claim.run_id, new);
            assert_eq!(
                reconcile_without_publication(&store, &claim).await.unwrap(),
                TaskRunOccurrenceTerminalizationOutcome::NotFound,
            );
        }
        assert!(row(&store, "moved_turn").await.is_none());
        if new == "moved_turn" {
            assert!(row(&store, &run.id).await.is_none());
        }
    }
    assert!(row(&store, &run.id).await.unwrap().generation > third.generation);
    let tx = store.begin().await.unwrap();
    let before = turns::Entity::find_by_id(&run.id)
        .lock_exclusive()
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    turns::Entity::delete_by_id(run.id.clone())
        .exec(&tx)
        .await
        .unwrap();
    crate::task_occurrence_producers::turn_change(&tx, Some(&before), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(row(&store, &run.id).await.is_none());
}

#[pioneer_postgres_test::postgres_test]
async fn trigger_updates_preserve_retry_and_ignore_noops_heartbeat_updated_at() {
    let (store, run) = terminal_fixture().await;
    let claim = claimed(&store).await;
    let before = row(&store, &run.id).await.unwrap();
    let tx = store.begin().await.unwrap();
    update_run_in_transaction(
        &tx,
        &run.id,
        runs::Entity::update_many()
            .col_expr(runs::Column::Status, Expr::val("succeeded"))
            .col_expr(
                runs::Column::HeartbeatAt,
                Expr::val(Some(unix_to_datetime(NOW))),
            )
            .col_expr(runs::Column::UpdatedAt, Expr::val(unix_to_datetime(NOW))),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(row(&store, &run.id).await.unwrap(), before);
    let tx = store.begin().await.unwrap();
    update_run_in_transaction(
        &tx,
        &run.id,
        runs::Entity::update_many().col_expr(
            runs::Column::CompletedAt,
            Expr::val(Some(unix_to_datetime(NOW - 1))),
        ),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let after = row(&store, &run.id).await.unwrap();
    assert!(after.generation > before.generation);
    assert_eq!(after.next_attempt_at, claim.next_attempt_at);
    assert_eq!(after.attempt_count, claim.attempt_count);
    assert!(after.claim_token.is_none());
    assert!(
        TaskRunOccurrenceQueue::new(&store)
            .discover_task_run_occurrence_reconcile(NOW, 64)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reconcile_without_publication(&store, &claim).await.unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::StaleClaim
    );
}

async fn prepared_facts(connection: &SqliteDatabase, run_id: &str) -> Box<OccurrenceSourceFacts> {
    let scoped = connection.maintenance().with_critical_writes();
    match OccurrenceSourceFacts::prepare(&scoped, run_id.to_owned(), NOW, true)
        .await
        .unwrap()
    {
        OccurrencePreparation::Ready { facts, .. } => facts,
        OccurrencePreparation::Unchanged(outcome) => {
            panic!("expected prepared repair, got {outcome:?}")
        }
    }
}

async fn revalidate_without_publication(
    connection: &SqliteDatabase,
    facts: &OccurrenceSourceFacts,
    claim: &TaskRunOccurrenceReconcileClaim,
) -> Result<TaskRunOccurrenceTerminalizationOutcome> {
    let scoped = connection.maintenance().with_critical_writes();
    let transaction = scoped.begin().await?;
    let outcome = facts
        .revalidate(&transaction, Some(claim))
        .await?
        .context("the obsolete source must fail before canonical publication")?;
    transaction.commit().await?;
    Ok(outcome)
}

#[pioneer_postgres_test::postgres_test]
async fn run_and_turn_freshness_ignores_equal_timestamps_and_checks_all_used_fields() {
    for change in [
        "completed_at",
        "error_json",
        "turn_error",
        "turn_prompt",
        "thread_id",
        "thread_workspace",
        "resume",
    ] {
        let (store, run) = terminal_fixture().await;
        let claim = claimed(&store).await;
        let prepared = prepared_facts(&store, &run.id).await;
        let run_time = runs::Entity::find_by_id(run.id.clone())
            .one(&store)
            .await
            .unwrap()
            .unwrap()
            .updated_at;
        let turn_time = turns::Entity::find_by_id(run.id.clone())
            .one(&store)
            .await
            .unwrap()
            .unwrap()
            .updated_at;
        match change {
            "completed_at" => {
                runs::Entity::update_many()
                    .col_expr(
                        runs::Column::CompletedAt,
                        Expr::val(Some(unix_to_datetime(NOW))),
                    )
                    .filter(runs::Column::Id.eq(&run.id))
                    .exec(&store)
                    .await
                    .unwrap();
            }
            "error_json" => {
                runs::Entity::update_many()
                    .col_expr(runs::Column::ErrorJson, Expr::val(Some("{}")))
                    .filter(runs::Column::Id.eq(&run.id))
                    .exec(&store)
                    .await
                    .unwrap();
            }
            "turn_error" => {
                turns::Entity::update_many()
                    .col_expr(turns::Column::Error, Expr::val(Some("changed")))
                    .filter(turns::Column::Id.eq(&run.id))
                    .exec(&store)
                    .await
                    .unwrap();
            }
            "turn_prompt" => {
                turns::Entity::update_many()
                    .col_expr(
                        turns::Column::PromptManifestJson,
                        Expr::val("{\"changed\":true}"),
                    )
                    .filter(turns::Column::Id.eq(&run.id))
                    .exec(&store)
                    .await
                    .unwrap();
            }
            "thread_id" => {
                turns::Entity::update_many()
                    .col_expr(turns::Column::ThreadId, Expr::val("other_thread"))
                    .filter(turns::Column::Id.eq(&run.id))
                    .exec(&store)
                    .await
                    .unwrap();
            }
            "thread_workspace" => {
                pioneer_entity::thread::Entity::update_many()
                    .col_expr(
                        pioneer_entity::thread::Column::WorkspaceId,
                        Expr::val("other_workspace"),
                    )
                    .filter(pioneer_entity::thread::Column::Id.eq("thr_task"))
                    .exec(&store)
                    .await
                    .unwrap();
            }
            _ => run_status(&store, &run.id, "running").await,
        }
        assert_eq!(
            runs::Entity::find_by_id(run.id.clone())
                .one(&store)
                .await
                .unwrap()
                .unwrap()
                .updated_at,
            run_time
        );
        assert_eq!(
            turns::Entity::find_by_id(run.id.clone())
                .one(&store)
                .await
                .unwrap()
                .unwrap()
                .updated_at,
            turn_time
        );
        assert_eq!(
            revalidate_without_publication(&store, &prepared, &claim)
                .await
                .unwrap(),
            TaskRunOccurrenceTerminalizationOutcome::StaleClaim,
            "{change}"
        );
        assert_eq!(
            turns::Entity::find_by_id(run.id.clone())
                .one(&store)
                .await
                .unwrap()
                .unwrap()
                .status,
            "in_progress"
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn changed_after_discovery_loses_claim_and_reincarnation_cannot_accept_old_result() {
    let (store, run) = terminal_fixture().await;
    let queue = TaskRunOccurrenceQueue::new(&store);
    let candidate = queue
        .discover_task_run_occurrence_reconcile(NOW, 64)
        .await
        .unwrap()
        .remove(0);
    turn_status(&store, &run.id, "failed").await;
    assert!(
        queue
            .claim_task_run_occurrence_reconcile(&candidate, &|| NOW)
            .await
            .unwrap()
            .is_none()
    );
    let claim = claimed(&store).await;
    let preparation = prepared_facts(&store, &run.id).await;
    let saved = remove_run(&store, &run.id).await;
    restore_run(&store, saved).await;
    let current = row(&store, &run.id).await.unwrap();
    assert!(current.generation > claim.generation);
    assert_eq!(
        revalidate_without_publication(&store, &preparation, &claim)
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::StaleClaim
    );
    assert_eq!(row(&store, &run.id).await.unwrap(), current);
}

#[pioneer_postgres_test::postgres_test]
async fn task_run_occurrence_terminalization_returns_not_found_for_missing_canonical_turn() {
    let (db, run) = terminal_fixture_with_turn(false).await;

    let prepared = OccurrenceSourceFacts::prepare(&db, run.id.clone(), 1_700_000_003, false)
        .await
        .expect("missing occurrence should be classified");
    let OccurrencePreparation::Unchanged(outcome) = prepared else {
        panic!("invalid occurrence was admitted for canonical publication");
    };
    assert_eq!(outcome, TaskRunOccurrenceTerminalizationOutcome::NotFound,);
}

#[pioneer_postgres_test::postgres_test]
async fn task_run_occurrence_terminalization_rejects_non_task_run_canonical_binding() {
    let (db, run) = terminal_fixture_with_turn(false).await;
    insert_occurrence_with_kind(&db, &run.id, "conversation").await;

    let prepared = OccurrenceSourceFacts::prepare(&db, run.id.clone(), 1_700_000_003, false)
        .await
        .expect("invalid occurrence binding should be classified");
    let OccurrencePreparation::Unchanged(outcome) = prepared else {
        panic!("invalid occurrence was admitted for canonical publication");
    };
    assert_eq!(
        outcome,
        TaskRunOccurrenceTerminalizationOutcome::InvalidBinding,
    );
    let occurrence = turn::find_turn_by_thread_and_id(&db, "thr_task", &run.id)
        .await
        .expect("occurrence should load")
        .expect("occurrence should remain present");
    assert_eq!(
        turn_status_from_db(&occurrence.status),
        Some(TurnStatus::InProgress)
    );
    assert_eq!(
        turn_kind_from_db(&occurrence.turn_kind),
        Some(TurnKind::Conversation)
    );
}
