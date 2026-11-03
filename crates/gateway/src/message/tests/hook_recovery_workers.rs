//! Real owner/reader evidence; not executed before external acceptance.
use super::*;
use pioneer_hooks::{HookContext, HookRunStatus};
use pioneer_sqlite::{SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteWriteExecutor};

#[tokio::test]
async fn hook_empty_wait_has_no_poll_and_scoped_producer_reaches_terminal_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("hook-worker.sqlite");
    let mut options = sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
    options.max_connections(1).sqlx_logging(false);
    let writer = Database::connect(options).await.unwrap();
    setup_workspace_manager_with_connection(writer.clone()).await;
    pioneer_sqlite::SqliteWriteExecutor::new(writer.clone())
        .apply_pragmas(pioneer_sqlite::SqliteWriteClass::Maintenance)
        .await
        .unwrap();
    let mut options =
        sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
    options
        .max_connections(2)
        .map_sqlx_sqlite_opts(|options| options.read_only(true).pragma("query_only", "ON"));
    let reader = Database::connect(options).await.unwrap();
    let observer = Arc::new(NativeSchedulingObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    let processor = Arc::new(MessageProcessor::with_agent_manager(
        Arc::new(ThreadManager::new("test-model", "openai")),
        Arc::new(AgentManager::new(test_provider(), test_tool_loop_config())),
        Arc::new(SessionManager::new()),
        Arc::new(WorkspaceManager::new(database.clone())),
        Arc::new(CrudStore::new(database.clone())),
    ));
    processor
        .set_hook_recovery_config(GatewayHookRecoveryConfig {
            enabled: true,
            startup_scan: true,
            poll_interval_ms: 250,
            batch_size: 2,
            max_concurrent: 1,
            stale_running_after_ms: 1_000,
            strict_debug: true,
        })
        .await;
    install_recoverable_test_hook_runtime(&processor, phase_13_empty_hook_runtime()).await;
    let reads = || {
        observer
            .reads
            .lock()
            .unwrap()
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    SqliteReadEvent::OperationFinished {
                        class: SqliteReadClass::Maintenance,
                        ..
                    }
                )
            })
            .count()
    };
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    processor.arm_completed_history_preparation_barrier("__hook_clock_settled__");
    processor.start_hook_recovery_worker().await;
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    processor.release_completed_history_preparation_barrier();
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    tokio::time::pause();
    let settled = reads();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        reads(),
        settled,
        "settled Empty owns no periodic domain SQL"
    );
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();
    // A separate store over the SAME runtime must address the waiting owner.
    // Missing handler exercises existing poison progress to a durable terminal
    // outcome; no provider or client is required for recovery.
    let producer = CrudStore::new(database.maintenance());
    let phase = pioneer_hooks::HookPhase::TurnPostTurn;
    let run = producer
        .create_hook_run(
            pioneer_crud::NewHookRunRecord {
                id: None,
                idempotency_key: pioneer_hooks::HookRunIdempotencyKey::new(
                    "event-driven-hook-poison",
                )
                .unwrap(),
                subscription_id: pioneer_hooks::HookSubscriptionId::new("missing-subscription")
                    .unwrap(),
                hook_id: pioneer_hooks::HookId::new("missing-handler").unwrap(),
                phase,
                status: HookRunStatus::Queued,
                scope: None,
                context: HookContext::default(),
                contribution_hashes: vec![],
                diagnostic_previews: vec![],
                error: None,
                queued_at: None,
                started_at: None,
                completed_at: None,
                deadline_at: None,
                resume_state: Some(pioneer_hooks::HookRunResumeState::input_snapshot(
                    pioneer_hooks::HookExecutionPolicy::default(),
                    pioneer_hooks::HookFailurePolicy::BestEffort,
                    pioneer_hooks::HookRetryPolicy::default(),
                    1,
                    1,
                    1,
                    pioneer_hooks::HookRunInputSnapshot::new(
                        phase,
                        HookContext::default(),
                        pioneer_hooks::HookInput::empty(pioneer_hooks::HookInputKind::TurnPostTurn),
                        pioneer_hooks::HookPolicySet::empty(),
                        pioneer_hooks::HookPromptContextSet::empty(),
                    ),
                )),
            },
            chrono::Utc::now().fixed_offset(),
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(10), async {
        loop {
            if producer
                .find_hook_run(&run.id)
                .await
                .unwrap()
                .unwrap()
                .status
                == HookRunStatus::Failed
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("committed resumable work must wake recovery without a tick");
    processor.shutdown_resilience_workers().await;
    assert!(
        processor
            .crud_store
            .background_work("hooks.recovery")
            .acquire()
            .is_some(),
        "shutdown released the unique owner"
    );
    // Lifecycle is closed by cancellation: start cannot create a late worker.
    processor.start_hook_recovery_worker().await;
    assert!(processor.hook_recovery_worker.lock().await.is_none());
    database.close().await.unwrap();
}

async fn finite_hook_fixture() -> (
    tempfile::TempDir,
    Arc<MessageProcessor>,
    Arc<NativeSchedulingObserver>,
    SqliteDatabase,
) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("hook-worker.sqlite");
    let mut options = sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
    options.max_connections(1).sqlx_logging(false);
    let writer = Database::connect(options).await.unwrap();
    setup_workspace_manager_with_connection(writer.clone()).await;
    pioneer_sqlite::SqliteWriteExecutor::new(writer.clone())
        .apply_pragmas(pioneer_sqlite::SqliteWriteClass::Maintenance)
        .await
        .unwrap();
    let mut options =
        sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
    options
        .max_connections(2)
        .map_sqlx_sqlite_opts(|options| options.read_only(true).pragma("query_only", "ON"));
    let reader = Database::connect(options).await.unwrap();
    let observer = Arc::new(NativeSchedulingObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    let processor = Arc::new(MessageProcessor::with_agent_manager(
        Arc::new(ThreadManager::new("test-model", "openai")),
        Arc::new(AgentManager::new(test_provider(), test_tool_loop_config())),
        Arc::new(SessionManager::new()),
        Arc::new(WorkspaceManager::new(database.clone())),
        Arc::new(CrudStore::new(database.clone())),
    ));
    processor
        .set_hook_recovery_config(GatewayHookRecoveryConfig {
            enabled: true,
            startup_scan: true,
            poll_interval_ms: 250,
            batch_size: 2,
            max_concurrent: 1,
            stale_running_after_ms: 1_000,
            strict_debug: true,
        })
        .await;
    install_recoverable_test_hook_runtime(&processor, phase_13_empty_hook_runtime()).await;
    (directory, processor, observer, database)
}

fn finite_hook_input(
    id: &str,
    status: HookRunStatus,
    queued: Option<i64>,
    started: Option<i64>,
    deadline: Option<i64>,
) -> pioneer_crud::NewHookRunRecord {
    let phase = pioneer_hooks::HookPhase::TurnPostTurn;
    pioneer_crud::NewHookRunRecord {
        id: Some(pioneer_hooks::HookRunId::new(id).unwrap()),
        idempotency_key: pioneer_hooks::HookRunIdempotencyKey::new(id).unwrap(),
        subscription_id: pioneer_hooks::HookSubscriptionId::new("missing-subscription").unwrap(),
        hook_id: pioneer_hooks::HookId::new("missing-handler").unwrap(),
        phase,
        status,
        scope: None,
        context: HookContext::default(),
        contribution_hashes: vec![],
        diagnostic_previews: vec![],
        error: None,
        queued_at: queued.map(|at| {
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(at)
                .unwrap()
                .fixed_offset()
        }),
        started_at: started.map(|at| {
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(at)
                .unwrap()
                .fixed_offset()
        }),
        completed_at: None,
        deadline_at: deadline.map(|at| {
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(at)
                .unwrap()
                .fixed_offset()
        }),
        resume_state: Some(pioneer_hooks::HookRunResumeState::input_snapshot(
            pioneer_hooks::HookExecutionPolicy::default(),
            pioneer_hooks::HookFailurePolicy::BestEffort,
            pioneer_hooks::HookRetryPolicy::default(),
            1,
            1,
            1,
            pioneer_hooks::HookRunInputSnapshot::new(
                phase,
                HookContext::default(),
                pioneer_hooks::HookInput::empty(pioneer_hooks::HookInputKind::TurnPostTurn),
                pioneer_hooks::HookPolicySet::empty(),
                pioneer_hooks::HookPromptContextSet::empty(),
            ),
        )),
    }
}

#[tokio::test]
async fn hook_clock_owner_finishes_future_raw_pages_and_revisits_an_earlier_old_key() {
    let (_directory, processor, observer, database) = finite_hook_fixture().await;
    let store = CrudStore::new(database.maintenance());
    let now = 1_800_000_000_000i64;
    let wall = processor.crud_store.background_work("hooks.recovery");
    wall.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_millis(now as u64));
    let at = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(now)
        .unwrap()
        .fixed_offset();
    for index in 0..129 {
        store
            .create_hook_run(
                finite_hook_input(
                    &format!("a-queued-{index:03}"),
                    HookRunStatus::Queued,
                    Some(now + 60_000),
                    None,
                    None,
                ),
                at,
            )
            .await
            .unwrap();
        store
            .create_hook_run(
                finite_hook_input(
                    &format!("b-running-{index:03}"),
                    HookRunStatus::Running,
                    None,
                    Some(if index == 0 {
                        now + 1_000
                    } else {
                        now + 120_000
                    }),
                    Some(now + 90_000),
                ),
                at,
            )
            .await
            .unwrap();
    }
    let tail = store
        .create_hook_run(
            finite_hook_input("z-due", HookRunStatus::Queued, None, None, None),
            at,
        )
        .await
        .unwrap();
    // Restored ordinary NULL is distinct from the create API's default(now).
    // Establish its persisted bytes before the owner starts; no fake wake.
    {
        use pioneer_entity::hook_run;
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, sea_query::Expr};
        hook_run::Entity::update_many()
            .col_expr(
                hook_run::Column::QueuedAt,
                Expr::value(None::<sea_orm::prelude::DateTimeWithTimeZone>),
            )
            .filter(hook_run::Column::Id.eq("z-due"))
            .exec(&database)
            .await
            .unwrap();
        assert!(
            hook_run::Entity::find_by_id("z-due")
                .one(&database)
                .await
                .unwrap()
                .unwrap()
                .queued_at
                .is_none()
        );
    }
    let label = "__hook_clock_settled__";
    processor.arm_completed_history_preparation_barrier(label);
    processor.start_hook_recovery_worker().await;
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        store.find_hook_run(&tail.id).await.unwrap().unwrap().status,
        HookRunStatus::Failed
    );
    for index in 0..129 {
        for (prefix, expected) in [
            ("a-queued", HookRunStatus::Queued),
            ("b-running", HookRunStatus::Running),
        ] {
            let id = pioneer_hooks::HookRunId::new(format!("{prefix}-{index:03}")).unwrap();
            assert_eq!(
                store.find_hook_run(&id).await.unwrap().unwrap().status,
                expected
            );
        }
    }
    assert_eq!(
        store.next_hook_recovery_deadline(1_000).await.unwrap(),
        Some(
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(now + 2_000)
                .unwrap()
                .fixed_offset()
        )
    );
    let reads = || observer.reads.lock().unwrap().len();
    let settled = reads();
    processor.release_completed_history_preparation_barrier();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(30)).await;
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        reads(),
        settled,
        "future population parks after finite raw reconstruction"
    );
    tokio::time::resume();
    let old = pioneer_hooks::HookRunId::new("a-queued-000").unwrap();
    processor.arm_completed_history_preparation_barrier(label);
    store
        .schedule_hook_run_retry(
            &old,
            pioneer_hooks::HookRetrySchedule {
                queued_at_unix_ms: now + 1_001,
                deadline_at_unix_ms: None,
                diagnostic_previews: vec![],
            },
            at,
        )
        .await
        .unwrap()
        .unwrap();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        store.find_hook_run(&old).await.unwrap().unwrap().status,
        HookRunStatus::Queued
    );
    assert_eq!(
        store.next_hook_recovery_deadline(1_000).await.unwrap(),
        Some(
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(now + 1_001)
                .unwrap()
                .fixed_offset()
        )
    );
    processor.release_completed_history_preparation_barrier();
    // No producer hint: the shared clock drives BOTH eligibility and waiting.
    wall.set_wall_clock_for_test(
        std::time::UNIX_EPOCH + Duration::from_millis((now + 1_000) as u64),
    );
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        store.find_hook_run(&old).await.unwrap().unwrap().status,
        HookRunStatus::Queued
    );
    processor.arm_completed_history_preparation_barrier(label);
    tokio::time::pause();
    wall.set_wall_clock_for_test(
        std::time::UNIX_EPOCH + Duration::from_millis((now + 1_001) as u64),
    );
    // Exercise Wake clock sampling without a producer or real30s wait.
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::time::resume();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        store.find_hook_run(&old).await.unwrap().unwrap().status,
        HookRunStatus::Failed
    );
    processor.release_completed_history_preparation_barrier();
    // Running stale clock wins over its later deadline; no producer kick.
    let running = pioneer_hooks::HookRunId::new("b-running-000").unwrap();
    processor.arm_completed_history_preparation_barrier(label);
    tokio::time::pause();
    wall.set_wall_clock_for_test(
        std::time::UNIX_EPOCH + Duration::from_millis((now + 2_000) as u64),
    );
    // Exercise Wake clock sampling without a producer or real30s wait.
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::time::resume();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert!(matches!(
        store.find_hook_run(&running).await.unwrap().unwrap().status,
        HookRunStatus::Failed | HookRunStatus::TimedOut
    ));
    processor.release_completed_history_preparation_barrier();
    // The other running rows become eligible by deadline before stale-start.
    processor.arm_completed_history_preparation_barrier(label);
    tokio::time::pause();
    wall.set_wall_clock_for_test(
        std::time::UNIX_EPOCH + Duration::from_millis((now + 90_000) as u64),
    );
    // Exercise Wake clock sampling without a producer or real30s wait.
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::time::resume();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    let running = pioneer_hooks::HookRunId::new("b-running-128").unwrap();
    assert!(matches!(
        store.find_hook_run(&running).await.unwrap().unwrap().status,
        HookRunStatus::Failed | HookRunStatus::TimedOut
    ));
    processor.release_completed_history_preparation_barrier();
    processor.shutdown_resilience_workers().await;
    database.close().await.unwrap();
}
