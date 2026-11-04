//! Gateway orchestration regressions; compile only until review acceptance.
use super::*;
use pioneer_entity::{compaction_lifecycle_pending as pending, compaction_operation as operation};
use pioneer_sqlite::SqliteWriteClass;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, TransactionTrait};

async fn fixture() -> (
    tempfile::TempDir,
    Arc<MessageProcessor>,
    Arc<CrudStore>,
    Arc<NativeSchedulingObserver>,
) {
    let observer = Arc::new(NativeSchedulingObserver::default());
    let (directory, manager, store, workspace) =
        setup_pooled_file_workspace_manager_with_observer(Some(observer.clone())).await;
    let processor = Arc::new(MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        test_provider(),
        Arc::new(SessionManager::new()),
        manager,
        store.clone(),
        test_gateway_secrets(),
        phase_13_summary_config(),
        test_tool_loop_config(),
    ));
    processor
        .agent_manager
        .set_context_controller(Some(Arc::new(
            crate::compaction::GatewayNativeContextController::new(Arc::downgrade(&processor)),
        )))
        .await;
    let db = store.with_maintenance_access().database_connection();
    let history = Phase13CompactionHarness {
        processor: processor.clone(),
        crud_store: store.clone(),
        workspace_id: workspace.clone(),
    };
    super::root_conversation_compaction::human_input(
        &history,
        "poll-thread",
        "poll-turn",
        phase_13_now_secs(),
        "actual root question",
    )
    .await;
    super::root_conversation_compaction::answer(
        &history,
        "poll-thread",
        "poll-turn",
        phase_13_now_secs(),
        "actual root answer",
    )
    .await;
    db.execute_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DbBackend::Sqlite,
        "INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('poll-owner',?,'poll-thread',1)", [workspace.into()])).await.unwrap();
    for id in
        std::iter::once("a-poison".to_owned()).chain((0..11).map(|i| format!("b-healthy-{i:02}")))
    {
        db.execute_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DbBackend::Sqlite,
            "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,execution_turn,deadline_ms) VALUES (?,'poll-owner',?,'running','{}','poll-turn',0)", [id.clone().into(), id.into()])).await.unwrap();
    }
    (directory, processor, store, observer)
}

async fn failed_count(store: &CrudStore) -> u64 {
    use sea_orm::PaginatorTrait;
    operation::Entity::find()
        .filter(operation::Column::Status.eq("failed"))
        .count(&store.with_maintenance_access().database_connection())
        .await
        .unwrap()
}

#[tokio::test]
async fn lifecycle_claim_and_deferral_failure_keeps_batch_progress_history_poll_and_cooldown() {
    let (_directory, processor, store, observer) = fixture().await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("CREATE TRIGGER reject_poison_bookkeeping BEFORE UPDATE ON compaction_lifecycle_pending WHEN OLD.operation_id='a-poison' AND NEW.attempts>OLD.attempts BEGIN SELECT RAISE(ABORT,'fixture bookkeeping unavailable'); END").await.unwrap();
    // Complete expansion before selecting this batch so the first point
    // locator keeps its original generation/order. No seed rediscovery.
    db.execute_unprepared("DELETE FROM compaction_lifecycle_scope; UPDATE compaction_lifecycle_sequence SET seed_complete=1 WHERE singleton=1").await.unwrap();
    let before = pending::Entity::find_by_id("a-poison")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    // An independent history check must still be owned after lifecycle failure.
    assert!(
        store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .iter()
            .any(|row| row.turn_id == "poll-turn"),
        "actual answer must enqueue root work"
    );
    let started = std::time::Instant::now();
    let write_start = observer.writes.lock().unwrap().len();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        processor.poll_completed_history_checks(),
    )
    .await
    .expect("lifecycle storage failure must not sleep five seconds in the common poll");
    assert!(result.is_err(), "storage failure must remain visible");
    assert!(started.elapsed() < Duration::from_secs(2));
    // Exactly eight pending inputs: poison plus seven healthy. No rediscovery.
    assert_eq!(failed_count(&store).await, 7);
    assert_eq!(
        store
            .compaction_operation("b-healthy-00")
            .await
            .unwrap()
            .unwrap()
            .status,
        "failed"
    );
    assert_eq!(
        store
            .compaction_operation("a-poison")
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    let poison = pending::Entity::find_by_id("a-poison")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(poison.attempts, before.attempts);
    assert_eq!(poison.retry_not_before, before.retry_not_before);
    assert!(poison.claim_token.is_none());
    assert!(
        processor
            .completed_history_checks
            .lock()
            .await
            .keys()
            .any(|(_, thread)| thread == "poll-thread")
    );
    // The common resilience loop's next action can proceed without the cooldown.
    processor
        .crud_store
        .with_critical_writes()
        .heartbeat_turn_executions_owned_by(
            processor.turn_execution_owner_id.as_ref(),
            TURN_EXECUTION_OWNER_LEASE_SECONDS,
        )
        .await
        .unwrap();
    assert!(
        observer.writes.lock().unwrap()[write_start..]
            .iter()
            .any(|event| matches!(
                event,
                pioneer_sqlite::SqliteWriteEvent::Acquired {
                    class: SqliteWriteClass::Critical,
                    ..
                }
            ))
    );
    let scheduled = processor
        .compaction_lifecycle_not_before
        .read()
        .unwrap()
        .unwrap();
    assert!(scheduled > tokio::time::Instant::now());
    // Removing the fault does not bypass the next lifecycle poll's cooldown.
    db.execute_unprepared("DROP TRIGGER reject_poison_bookkeeping")
        .await
        .unwrap();
    processor.poll_completed_history_checks().await.unwrap();
    assert_eq!(failed_count(&store).await, 7);
    assert_eq!(
        pending::Entity::find_by_id("a-poison")
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        poison
    );
    // The independent root job may still own the same coordinator key. Join
    // its cancellation before checking lifecycle progress on that key.
    processor.suspend_completed_history_checks().await;
    // Move only this existing scheduling deadline to its expiry, without sleep.
    *processor.compaction_lifecycle_not_before.write().unwrap() = Some(tokio::time::Instant::now());
    processor.poll_completed_history_checks().await.unwrap();
    assert!(failed_count(&store).await > 7);
    processor.suspend_completed_history_checks().await;
    assert!(db.reader_query_only_enabled().await.unwrap());
}

#[tokio::test]
async fn lifecycle_scope_bookkeeping_failure_preserves_independent_pending_quota() {
    let (_directory, processor, store, _) = fixture().await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("CREATE TRIGGER reject_scope_bookkeeping BEFORE UPDATE ON compaction_lifecycle_scope WHEN NEW.attempts>OLD.attempts BEGIN SELECT RAISE(ABORT,'fixture scope unavailable'); END").await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        processor.poll_completed_history_checks(),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    assert_eq!(failed_count(&store).await, 8);
    assert!(
        pioneer_entity::compaction_lifecycle_scope::Entity::find_by_id((
            "seed".to_owned(),
            String::new(),
            String::new()
        ))
        .one(&db)
        .await
        .unwrap()
        .is_some()
    );
    assert!(
        processor
            .compaction_lifecycle_not_before
            .read()
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn lifecycle_readiness_failure_preserves_pending_quota_history_and_cooldown() {
    let (_directory, processor, store, _) = fixture().await;
    let db = store.with_maintenance_access().database_connection();
    // Ordinary scopes have no typeof CHECK on due_at. A restored text value
    // fails the grouped MIN decode but is excluded by the old due discovery.
    // Keep it the only scope so a healthy numeric MIN cannot mask the fault.
    db.execute_unprepared("DELETE FROM compaction_lifecycle_scope; UPDATE compaction_lifecycle_sequence SET seed_complete=1 WHERE singleton=1; INSERT INTO compaction_lifecycle_scope(kind,owner,turn_id,generation,due_at) VALUES('owner','restored-poison','',1,'invalid-clock')")
        .await
        .unwrap();
    processor.arm_completed_history_preparation_barrier("poll-turn");
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        processor.poll_completed_history_checks(),
    )
    .await
    .expect("readiness failure must schedule cooldown without sleeping in the poll");
    assert!(
        result.is_err(),
        "grouped observation error must survive successful independent repairs"
    );
    assert_eq!(
        failed_count(&store).await,
        8,
        "one pending input quota must run"
    );
    tokio::time::timeout(
        Duration::from_secs(2),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .expect("independent history owner must still start");
    let scheduled = processor
        .compaction_lifecycle_not_before
        .read()
        .unwrap()
        .unwrap();
    assert!(scheduled > tokio::time::Instant::now());
    // Join history cancellation before comparing lifecycle progress. Cancellation
    // is independent of this debt and does not wait for its five-second cooldown.
    processor.suspend_completed_history_checks().await;
    processor.poll_completed_history_checks().await.unwrap();
    assert_eq!(failed_count(&store).await, 8);
    assert_eq!(
        *processor.compaction_lifecycle_not_before.read().unwrap(),
        Some(scheduled),
        "another common poll must neither retry nor postpone lifecycle cooldown"
    );
    processor.suspend_completed_history_checks().await;
    // History bookkeeping can add ordinary numeric scopes. Leave the original
    // poison as the only scope again, without repairing or replacing that row.
    db.execute_unprepared("DELETE FROM compaction_lifecycle_scope WHERE owner<>'restored-poison'")
        .await
        .unwrap();
    *processor.compaction_lifecycle_not_before.write().unwrap() = Some(tokio::time::Instant::now());
    let result = processor.poll_completed_history_checks().await;
    assert!(
        result.is_err(),
        "persistent observation failure must remain visible"
    );
    assert_eq!(
        failed_count(&store).await,
        12,
        "remaining bounded inputs must progress after cooldown"
    );
    let poison = db.query_one_raw(sea_orm::Statement::from_string(
        sea_orm::DbBackend::Sqlite,
        "SELECT due_at FROM compaction_lifecycle_scope WHERE kind='owner' AND owner='restored-poison' AND turn_id=''".to_owned(),
    ))
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        poison.try_get::<String>("", "due_at").unwrap(),
        "invalid-clock"
    );
    processor.suspend_completed_history_checks().await;
    assert!(db.reader_query_only_enabled().await.unwrap());
}

#[tokio::test]
async fn lifecycle_gateway_poll_cancellation_releases_maintenance_reservation() {
    let (_directory, processor, store, observer) = fixture().await;
    let db = store.with_maintenance_access().database_connection();
    let held = db.begin().await.unwrap();
    let start = observer.writes.lock().unwrap().len();
    let owner = processor.clone();
    let poll = tokio::spawn(async move { owner.poll_completed_history_checks().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if observer.writes.lock().unwrap()[start..]
                .iter()
                .any(|event| {
                    matches!(
                        event,
                        pioneer_sqlite::SqliteWriteEvent::Enqueued {
                            class: SqliteWriteClass::Maintenance,
                            ..
                        }
                    )
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    poll.abort();
    assert!(poll.await.unwrap_err().is_cancelled());
    held.rollback().await.unwrap();
    assert_eq!(failed_count(&store).await, 0);
    assert!(
        pending::Entity::find_by_id("a-poison")
            .one(&db)
            .await
            .unwrap()
            .is_some()
    );
    assert!(observer.writes.lock().unwrap()[start..].iter().any(|event| matches!(event,
        pioneer_sqlite::SqliteWriteEvent::Cancelled { class: SqliteWriteClass::Maintenance, queue, .. } if queue.maintenance == 0)));
    processor.poll_completed_history_checks().await.unwrap();
    assert_eq!(failed_count(&store).await, 8);
    assert!(db.reader_query_only_enabled().await.unwrap());
}

#[tokio::test]
async fn lifecycle_owner_stays_dormant_and_finishes_a_new_operation_at_its_clock_deadline() {
    use pioneer_compaction::{
        CompactionMode, CompactionPlan, CompactionSettings, CoverageDomain, ModelSelection,
        OperationSnapshot, Transport,
    };
    let (_directory, processor, store, observer) = fixture().await;
    processor.arm_completed_history_preparation_barrier("__compaction_lifecycle_before_wait__");
    let worker = tokio::spawn(MessageProcessor::run_compaction_lifecycle_worker(
        Arc::downgrade(&processor),
    ));
    // The lifecycle owner itself finishes finite startup/restored debt.
    for _ in 0..16 {
        timeout(
            Duration::from_secs(10),
            processor.wait_for_completed_history_preparation_barrier(),
        )
        .await
        .unwrap();
        if store.compaction_lifecycle_deadlines().await.unwrap() == (None, None) {
            break;
        }
        processor.arm_completed_history_preparation_barrier("__compaction_lifecycle_before_wait__");
        processor.release_completed_history_preparation_barrier();
    }
    assert_eq!(
        store.compaction_lifecycle_deadlines().await.unwrap(),
        (None, None)
    );
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    processor.release_completed_history_preparation_barrier();
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();
    let db = store.with_maintenance_access().database_connection();
    let workspace = pioneer_entity::thread::Entity::find_by_id("poll-thread")
        .one(&db)
        .await
        .unwrap()
        .unwrap()
        .workspace_id;
    let selection = ModelSelection {
        transport: Transport::Api,
        instance: "test".into(),
        model: "test-model".into(),
        effort: None,
    };
    let now = processor.compaction_clock.now_ms();
    let mut admission = CompactionSettings::default()
        .admit(&selection, None, now)
        .unwrap();
    admission.deadline_ms = now + 3000;
    let snapshot = OperationSnapshot {
        id: "registered-clock-operation".into(),
        owner: "poll-owner".into(),
        expected_checkpoint: None,
        projection_version: store
            .compaction_projection_version(&workspace, "poll-thread")
            .await
            .unwrap(),
        source_epochs: BTreeMap::new(),
        admission,
        plan: CompactionPlan {
            mode: CompactionMode::Normal,
            coverage_domain: CoverageDomain::OwnContribution,
            compact: vec![],
            retain: vec![],
            coverage: vec![],
            fingerprint: "registered-clock-plan".into(),
        },
    };
    let lower = CrudStore::new(db);
    processor.arm_completed_history_preparation_barrier("__compaction_lifecycle_before_wait__");
    lower
        .compaction_admit_for_turn(&workspace, "poll-thread", &snapshot, Some("poll-turn"))
        .await
        .unwrap();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .compaction_operation(&snapshot.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    processor.arm_completed_history_preparation_barrier("__compaction_lifecycle_before_wait__");
    processor.release_completed_history_preparation_barrier();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .compaction_operation(&snapshot.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "failed",
        "the retained compaction deadline must fence the operation without a client/poll"
    );
    // The terminal update's existing trigger can create one more bounded
    // publication/ACK quantum. Drain it through the real owner, not a manual call.
    for _ in 0..16 {
        if store.compaction_lifecycle_deadlines().await.unwrap() == (None, None) {
            break;
        }
        processor.arm_completed_history_preparation_barrier("__compaction_lifecycle_before_wait__");
        processor.release_completed_history_preparation_barrier();
        timeout(
            Duration::from_secs(10),
            processor.wait_for_completed_history_preparation_barrier(),
        )
        .await
        .unwrap();
    }
    assert_eq!(
        store.compaction_lifecycle_deadlines().await.unwrap(),
        (None, None)
    );
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    processor.release_completed_history_preparation_barrier();
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert!(
        observer.reads.lock().unwrap().is_empty(),
        "completed history stays dormant"
    );
    assert!(observer.writes.lock().unwrap().is_empty());
    worker.abort();
    let _ = worker.await;
    tokio::time::resume();
    processor.suspend_completed_history_checks().await;
}

// The same physical-reader fixture covers the new history owner, raw parked
// traversal, dependency release, and the real hot-window deadline. It does not
// infer internal query work from the number of reader commands.
#[tokio::test]
async fn history_owner_bounds_parked_inputs_and_waits_for_release_or_actual_expiry() {
    use pioneer_entity::compaction_history_check as check;
    use sea_orm::Set;
    let (_directory, processor, store, observer) = fixture().await;
    let db = store.with_maintenance_access().database_connection();
    let workspace = pioneer_entity::thread::Entity::find_by_id("poll-thread")
        .one(&db)
        .await
        .unwrap()
        .unwrap()
        .workspace_id;
    let cancel = tokio_util::sync::CancellationToken::new();
    let lease = processor
        .compaction_coordinator
        .acquire(
            &workspace,
            "poll-thread",
            crate::compaction::ContextWorkPriority::Foreground,
            &cancel,
        )
        .await
        .unwrap()
        .unwrap();
    // Retained terminal metadata does not require a surviving canonical Turn.
    // FK=OFF is the production setting; none of these keys is eligible work.
    for n in 0..21 {
        check::Entity::insert(check::ActiveModel {
            turn_id: Set(format!("000-terminal-{n:02}")),
            state: Set("finished".into()),
            outcome: Set(Some("fits".into())),
            ..Default::default()
        })
        .exec(&db)
        .await
        .unwrap();
    }
    let worker = tokio::spawn(MessageProcessor::run_compaction_history_worker(
        Arc::downgrade(&processor),
    ));
    timeout(Duration::from_secs(10), async {
        loop {
            let row = check::Entity::find_by_id("poll-turn")
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            if row.outcome.as_deref() == Some("waiting_executor") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let terminal = check::Entity::find_by_id("000-terminal-00")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let (rows, after, upper, error) = store
        .compaction_parked_history_page(None, None, &[], 8, 4)
        .await
        .unwrap();
    assert!(error.is_none());
    assert!(rows.is_empty());
    assert_eq!(
        after.as_deref(),
        Some("000-terminal-07"),
        "one quantum spends eight RAW keys"
    );
    let mut after = after;
    let mut upper = upper;
    let mut selected = Vec::new();
    for _ in 0..4 {
        let (rows, next, bound, error) = store
            .compaction_parked_history_page(after.as_deref(), upper.as_deref(), &[], 8, 4)
            .await
            .unwrap();
        assert!(error.is_none());
        selected.extend(rows.into_iter().map(|row| row.turn_id));
        after = next;
        upper = bound;
        if after.is_none() {
            break;
        }
    }
    assert!(after.is_none());
    assert_eq!(selected, ["poll-turn"]);
    assert_eq!(
        check::Entity::find_by_id("000-terminal-00")
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        terminal
    );
    let waiting = check::Entity::find_by_id("poll-turn")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let activity = store
        .compaction_root_history_activity(&workspace, "poll-thread", "poll-turn")
        .await
        .unwrap()
        .unwrap();
    let window = processor
        .context_compaction_config
        .hot_conversation_window_secs
        * 1000;
    let mut after = None;
    let mut upper = None;
    let mut earliest = None::<i64>;
    loop {
        let (_, next, bound, error, deadline) = store
            .compaction_history_obligation_page(
                after.as_deref(),
                upper.as_deref(),
                &[],
                8,
                0,
                0,
                window as i64,
            )
            .await
            .unwrap();
        assert!(error.is_none());
        earliest = earliest.into_iter().chain(deadline).min();
        after = next;
        upper = bound;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(earliest, Some(activity + window as i64));
    assert!(
        waiting.next_attempt_ms < activity + window as i64,
        "synthetic Waiting retry is not the owner timer"
    );

    // Wait for collection as well as durable ACK before observing Dormant.
    for _ in 0..4 {
        processor.arm_completed_history_preparation_barrier("__compaction_history_before_wait__");
        store
            .background_work("compaction.history")
            .dependency_released();
        timeout(
            Duration::from_secs(10),
            processor.wait_for_completed_history_preparation_barrier(),
        )
        .await
        .unwrap();
        let idle = processor.completed_history_checks.lock().await.is_empty();
        processor.release_completed_history_preparation_barrier();
        if idle {
            break;
        }
    }
    assert!(processor.completed_history_checks.lock().await.is_empty());
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();

    // Releasing the real foreground lease must retry before any new commit or
    // the old 60s next_attempt. A second Waiting is allowed, but not a lost wake.
    drop(lease);
    timeout(Duration::from_secs(10), async {
        loop {
            let row = check::Entity::find_by_id("poll-turn")
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            if row.revision > waiting.revision && row.attempt_deadline_ms.is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    // Window expiry has useful terminal work even when catalog/configuration
    // still prevents optional preparation and there is no client/new commit.
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(window)).await;
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    tokio::time::resume();
    timeout(Duration::from_secs(10), async {
        loop {
            let row = check::Entity::find_by_id("poll-turn")
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            if row.state == "finished" {
                assert!(matches!(
                    row.outcome.as_deref(),
                    Some("cancelled" | "fits" | "compacted" | "failed")
                ));
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(db.reader_query_only_enabled().await.unwrap());
    processor.cli_history_shutdown.cancel();
    worker.await.unwrap();
    processor.suspend_completed_history_checks().await;
}

// Actual pending root metadata exceeds one raw page in BOTH lanes. No orphan
// locator stands in for a future/parked conversation.
#[tokio::test]
async fn history_owner_retains_future_and_parked_roots_across_pages_and_old_key_release() {
    use pioneer_crud::compaction::{HistoryCheckDiagnostic, HistoryCheckOutcome};
    use pioneer_entity::compaction_history_check as check;
    let (_directory, processor, store, observer) = fixture().await;
    let db = store.with_maintenance_access().database_connection();
    store
        .compaction_finish_history_check("poll-turn", "cancelled")
        .await
        .unwrap();
    let workspace = pioneer_entity::thread::Entity::find_by_id("poll-thread")
        .one(&db)
        .await
        .unwrap()
        .unwrap()
        .workspace_id;
    let harness = Phase13CompactionHarness {
        processor: processor.clone(),
        crud_store: store.clone(),
        workspace_id: workspace.clone(),
    };
    let now = processor.compaction_clock.now_ms() as i64;
    let window = processor
        .context_compaction_config
        .hot_conversation_window_secs
        * 1000;
    assert!(window > 300_000);
    let future = now + 300_000;
    let cancel = tokio_util::sync::CancellationToken::new();
    let mut leases = Vec::new();
    let mut seeded = Vec::new();
    for n in 0..34 {
        let turn = format!("bound-root-{n:02}");
        let thread = format!("thread-{turn}");
        super::root_conversation_compaction::human_input(
            &harness,
            &thread,
            &turn,
            now / 1000,
            "real bounded source question",
        )
        .await;
        super::root_conversation_compaction::answer(
            &harness,
            &thread,
            &turn,
            now / 1000,
            "real published source answer",
        )
        .await;
        let row = check::Entity::find_by_id(&turn)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let outcome = if n % 2 == 0 {
            HistoryCheckOutcome::Retryable
        } else {
            HistoryCheckOutcome::WaitingExecutor
        };
        // Genuine CAS result recording creates both scheduling states. Use the
        // second retry step so its deadline is later than a short idle sample.
        assert!(
            store
                .compaction_record_history_result(
                    &turn,
                    row.revision,
                    1,
                    outcome,
                    &HistoryCheckDiagnostic::new(
                        "fixture",
                        "retained_input",
                        "bounded source fixture"
                    ),
                    now,
                )
                .await
                .unwrap()
        );
        let row = check::Entity::find_by_id(&turn)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.state, "pending");
        if n % 2 == 0 {
            assert_eq!(row.next_attempt_ms, future);
        }
        leases.push(Some(
            processor
                .compaction_coordinator
                .acquire(
                    &workspace,
                    &thread,
                    crate::compaction::ContextWorkPriority::Foreground,
                    &cancel,
                )
                .await
                .unwrap()
                .unwrap(),
        ));
        seeded.push(row);
    }
    // A truly eligible tail after all 34 pending root keys must finish. Its
    // original user activity is expired, so the useful outcome is cancellation.
    let expired = now / 1000 - (window / 1000) as i64 - 2;
    super::root_conversation_compaction::human_input(
        &harness,
        "thread-zz-bound-tail",
        "zz-bound-tail",
        expired,
        "expired question",
    )
    .await;
    super::root_conversation_compaction::answer(
        &harness,
        "thread-zz-bound-tail",
        "zz-bound-tail",
        expired,
        "expired answer",
    )
    .await;
    let (rows, mut after, mut upper, error, deadline) = store
        .compaction_history_obligation_page(None, None, &[], 8, 0, now, window as i64)
        .await
        .unwrap();
    assert!(error.is_none());
    assert!(rows.is_empty());
    assert_eq!(
        after.as_deref(),
        Some("bound-root-07"),
        "eight actual raw root inputs"
    );
    assert_eq!(deadline, Some(future));
    let mut earliest = deadline;
    let mut tail_seen = false;
    while after.is_some() {
        let (page, next, bound, error, at) = store
            .compaction_history_obligation_page(
                after.as_deref(),
                upper.as_deref(),
                &[],
                8,
                0,
                now,
                window as i64,
            )
            .await
            .unwrap();
        assert!(error.is_none());
        tail_seen |= page.iter().any(|(row, _)| row.turn_id == "zz-bound-tail");
        earliest = earliest.into_iter().chain(at).min();
        after = next;
        upper = bound;
    }
    assert!(tail_seen, "bounded traversal reaches actual useful tail");
    assert_eq!(
        earliest,
        Some(future),
        "minimum retained across the complete finite pass"
    );

    processor.arm_completed_history_preparation_barrier("__compaction_history_before_wait__");
    let worker = tokio::spawn(MessageProcessor::run_compaction_history_worker(
        Arc::downgrade(&processor),
    ));
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    // The owner has already published continuation beyond root-01. Let its
    // independently owned job finish before releasing that real dependency.
    timeout(Duration::from_secs(10), async {
        loop {
            let row = check::Entity::find_by_id("bound-root-01")
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            if row.revision > seeded[1].revision
                && row.outcome.as_deref() == Some("waiting_executor")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let before = check::Entity::find_by_id("bound-root-01")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    drop(leases[1].take()); // real coordinator release; no manual wake or write
    processor.release_completed_history_preparation_barrier();
    timeout(
        Duration::from_secs(10),
        processor
            .completed_history_preparation_barrier
            .wait_for_history_idle(),
    )
    .await
    .unwrap();
    let tail = check::Entity::find_by_id("zz-bound-tail")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tail.state, "finished");
    assert_eq!(tail.outcome.as_deref(), Some("cancelled"));
    let old = check::Entity::find_by_id("bound-root-01")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert!(
        old.revision > before.revision,
        "release behind cursor must be revisited before EOF"
    );
    assert_ne!(old.outcome.as_deref(), Some("waiting_executor"));
    let future_before = check::Entity::find_by_id("bound-root-00")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        future_before.revision, seeded[0].revision,
        "future source was not admitted early"
    );
    // No queries used as a stand-in for physical SQL quietness. This fixture's
    // SystemCompactionClock advances with Tokio and is also used by admission.
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    tokio::time::pause();
    let delay = future.saturating_sub(processor.compaction_clock.now_ms() as i64);
    assert!(delay > 2);
    tokio::time::advance(Duration::from_millis(delay as u64 - 2)).await;
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::advance(Duration::from_millis(3)).await;
    // Resume before I/O waits so Tokio's automatic time advancement cannot
    // skip the next real hot-window expiry while waiting on a physical reader.
    tokio::time::resume();
    timeout(
        Duration::from_secs(10),
        processor
            .completed_history_preparation_barrier
            .wait_for_history_idle(),
    )
    .await
    .unwrap();
    let retried = check::Entity::find_by_id("bound-root-00")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert!(
        retried.revision > future_before.revision,
        "retained earliest deadline must service future input"
    );
    assert_eq!(retried.outcome.as_deref(), Some("waiting_executor"));
    assert!(db.reader_query_only_enabled().await.unwrap());
    processor.cli_history_shutdown.cancel();
    worker.await.unwrap();
    processor.suspend_completed_history_checks().await;
    drop(leases);
}
