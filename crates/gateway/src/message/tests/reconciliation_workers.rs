use super::*;
use crate::message::reconciliation_diagnostics::{Operation, Reporter};
use sentry::SentryFutureExt;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;

#[tokio::test]
async fn native_turn_event_delivery_kick_uses_background_database_scope() {
    assert_native_kick_database_scope(false).await;
}

#[tokio::test]
async fn native_terminal_effect_kick_uses_background_database_scope() {
    assert_native_kick_database_scope(true).await;
}

#[tokio::test]
async fn native_terminal_effect_kick_defers_coalesced_retry_after_storage_failure() {
    // Observe the existing end-of-quantum warning, rather than assuming a
    // fixed number of scheduler yields means the worker has started its sleep.
    struct KickFailure(tokio::sync::mpsc::UnboundedSender<tokio::time::Instant>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for KickFailure {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let metadata = event.metadata();
            if metadata.target() == "pioneer_gateway::message::agent_runtime"
                && *metadata.level() == tracing::Level::WARN
                && metadata.fields().field("dispatched_count").is_some()
            {
                let _ = self.0.send(tokio::time::Instant::now());
            }
        }
    }

    let (processor, _, _, _, _, _) = setup_workspace_message_processor().await;
    processor
        .crud_store
        .database_connection()
        .close()
        .await
        .unwrap();
    let (failures, mut failures_rx) = tokio::sync::mpsc::unbounded_channel();
    // This current-thread test also polls the spawned kick under this dispatch.
    let _subscriber = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(KickFailure(failures)),
    );
    tokio::time::pause();
    processor.kick_native_terminal_effects();
    let first_failure = timeout(Duration::from_secs(10), failures_rx.recv())
        .await
        .expect("first quantum must report its storage failure")
        .unwrap();
    assert!(
        processor
            .native_terminal_effect_kick_running
            .load(Ordering::Acquire)
    );
    processor.kick_native_terminal_effects();
    tokio::time::advance(Duration::from_secs(4)).await;
    for _ in 0..128 {
        tokio::task::yield_now().await;
    }
    assert!(
        processor
            .native_terminal_effect_kick_pending
            .load(Ordering::Acquire)
    );
    assert!(failures_rx.try_recv().is_err(), "no retry before the delay");
    // Tokio's timer wheel rounds deadlines to milliseconds. Cross the exact
    // five-second boundary by one tick, then await the second completed quantum.
    tokio::time::advance(Duration::from_millis(1001)).await;
    let second_failure = timeout(Duration::from_millis(100), failures_rx.recv())
        .await
        .expect("coalesced kick must retry after the delay")
        .unwrap();
    let elapsed = second_failure.duration_since(first_failure);
    assert!(elapsed >= Duration::from_secs(5));
    assert!(elapsed <= Duration::from_millis(5101));
    assert!(
        !processor
            .native_terminal_effect_kick_pending
            .load(Ordering::Acquire)
    );
    assert!(
        processor
            .native_terminal_effect_kick_running
            .load(Ordering::Acquire)
    );
    tokio::time::advance(Duration::from_secs(4)).await;
    tokio::task::yield_now().await;
    assert!(
        processor
            .native_terminal_effect_kick_running
            .load(Ordering::Acquire)
    );
    tokio::time::advance(Duration::from_millis(1001)).await;
    timeout(Duration::from_millis(100), async {
        while processor
            .native_terminal_effect_kick_running
            .load(Ordering::Acquire)
        {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("kick must release running after the second delay");
    assert!(
        !processor
            .native_terminal_effect_kick_running
            .load(Ordering::Acquire)
    );
    assert!(failures_rx.try_recv().is_err(), "only two quanta may run");
}

async fn assert_native_kick_database_scope(terminal_effects: bool) {
    use pioneer_sqlite::{
        SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteWriteClass, SqliteWriteEvent,
        SqliteWriteExecutor,
    };
    use sea_orm::{ConnectOptions, DbBackend, Statement};

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("native-kick.sqlite");
    let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
    options.max_connections(1).sqlx_logging(false);
    let writer = Database::connect(options).await.unwrap();
    let (_, _, workspace) = setup_workspace_manager_with_connection(writer.clone()).await;
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
    options
        .max_connections(2)
        .sqlx_logging(false)
        .map_sqlx_sqlite_opts(|options| options.pragma("query_only", "ON"));
    let reader = Database::connect(options).await.unwrap();
    let observer = Arc::new(NativeSchedulingObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    let store = Arc::new(CrudStore::new(database.clone()));
    let processor = MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        test_provider(),
        Arc::new(SessionManager::new()),
        Arc::new(WorkspaceManager::new(database.clone())),
        store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    );
    if !terminal_effects {
        materialize_cli_runtime_turn_with_text(
            &store,
            &workspace,
            "kick-thread",
            "kick-turn",
            "input",
        )
        .await;
        let (_, mut turn) = store
            .get_turn("kick-thread", "kick-turn")
            .await
            .unwrap()
            .unwrap();
        turn.status = TurnStatus::Completed;
        store
            .materialize_native_agent_turn_event(
                pioneer_crud::CanonicalTurnEventPayload::TurnCompleted(TurnCompletedNotification {
                    workspace_id: workspace,
                    thread_id: "kick-thread".into(),
                    turn,
                }),
                now_timestamp_secs(),
                None,
            )
            .await
            .unwrap();
        let row = database
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT COUNT(*) AS count FROM turn_event_delivery \
             WHERE turn_id='kick-turn' AND consumer='live_notification' \
             AND status='pending'"
                    .to_owned(),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.try_get::<i64>("", "count").unwrap(), 1);
    }

    // Occupy the single maintenance slot while leaving another physical reader
    // free for foreground queries. A real kick must queue at this limiter.
    let held = database.maintenance().begin_read().await.unwrap();
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    let running = if terminal_effects {
        processor.kick_native_terminal_effects();
        &processor.native_terminal_effect_kick_running
    } else {
        processor.kick_native_turn_event_deliveries();
        &processor.native_turn_event_delivery_kick_running
    };
    timeout(Duration::from_secs(10), async {
        loop {
            if observer.reads.lock().unwrap().iter().any(|event| {
                matches!(
                    event,
                    SqliteReadEvent::AdmissionEnqueued {
                        class: SqliteReadClass::Maintenance,
                        ..
                    }
                )
            }) {
                break;
            }
            assert!(
                running.load(Ordering::Acquire),
                "background kick bypassed maintenance admission"
            );
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        observer.writes.lock().unwrap().is_empty(),
        "discovery must wait before writing"
    );
    let query_only = timeout(
        Duration::from_secs(10),
        database.reader_query_only_enabled(),
    )
    .await
    .expect("foreground read must proceed while the kick waits")
    .unwrap();
    assert!(query_only, "the physical reader must remain query-only");
    assert_eq!(
        processor.crud_store.database_connection().read_class(),
        SqliteReadClass::Interactive
    );
    observer.reads.lock().unwrap().clear();
    drop(held);
    timeout(Duration::from_secs(10), async {
        while running.load(Ordering::Acquire) {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background kick must drain after maintenance admission is released");

    let reads = observer.reads.lock().unwrap().clone();
    assert!(reads.iter().any(|event| matches!(
        event,
        SqliteReadEvent::OperationFinished {
            class: SqliteReadClass::Maintenance,
            ..
        }
    )));
    assert!(reads.iter().all(|event| !matches!(
        event,
        SqliteReadEvent::OperationFinished {
            class: SqliteReadClass::Interactive,
            ..
        }
    )));
    let writes = observer.writes.lock().unwrap().clone();
    if terminal_effects {
        // No probe/claim input: the removed global exhausted UPDATE must not
        // acquire a writer merely to perform an empty terminal-effect sweep.
        assert!(writes.is_empty());
    } else {
        assert!(writes.iter().any(|event| matches!(
            event,
            SqliteWriteEvent::Acquired {
                class: SqliteWriteClass::Critical,
                ..
            }
        )));
        assert!(writes.iter().all(|event| !matches!(event,
            SqliteWriteEvent::Acquired { class, .. } if *class != SqliteWriteClass::Critical
        )));
    }
    if !terminal_effects {
        let row = database
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT COUNT(*) AS count FROM turn_event_delivery \
                 WHERE turn_id='kick-turn' AND consumer='live_notification' \
                 AND status='delivered'"
                    .to_owned(),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.try_get::<i64>("", "count").unwrap(),
            1,
            "kick must acknowledge the committed live event"
        );
    }
}

pub(super) fn local_capture() -> (
    Arc<sentry::Hub>,
    Arc<sentry::test::TestTransport>,
    tracing::Dispatch,
) {
    let transport = sentry::test::TestTransport::new();
    let options = sentry::ClientOptions::new()
        .dsn("https://public@sentry.invalid/1")
        .transport(transport.clone());
    let hub = Arc::new(sentry::Hub::new(
        Some(Arc::new(options.into())),
        Arc::new(Default::default()),
    ));
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry().with(pioneer_observability::sentry_tracing_layer()),
    );
    (hub, transport, dispatch)
}

#[tokio::test]
async fn reconciliation_reporting_is_retained_in_both_real_worker_loops() {
    let (processor, _, _, _, _, _) = setup_workspace_message_processor().await;
    let processor = Arc::new(processor);
    // Closed pools supply a real typed storage failure without fixture hooks.
    processor
        .crud_store
        .database_connection()
        .close()
        .await
        .unwrap();
    // SQLite pool setup uses a real worker thread. Pause only after the fixture
    // is ready and closed, so Tokio cannot advance its acquisition timeout while
    // that thread is still opening the connection.
    tokio::time::pause();
    let (hub, transport, dispatch) = local_capture();
    let projection = tokio::spawn(
        MessageProcessor::run_projection_delivery_resilience_worker(Arc::downgrade(&processor))
            .with_subscriber(dispatch.clone())
            .bind_hub(hub.clone()),
    );
    let lifecycle = tokio::spawn(
        MessageProcessor::run_task_lifecycle_resilience_worker(Arc::downgrade(&processor))
            .with_subscriber(dispatch)
            .bind_hub(hub),
    );
    let mut events = Vec::new();
    // End-of-pass neighboring events provide a completion marker for each real
    // worker. Advance Tokio's clock only after both reached their existing sleep.
    for pass in 1..=3 {
        let mut completed = false;
        for _ in 0..1024 {
            tokio::task::yield_now().await;
            events.extend(transport.fetch_and_clear_events());
            let projection_passes = events
                .iter()
                .filter(|event| {
                    event.message.as_deref()
                        == Some("native turn-event optional delivery worker failed")
                })
                .count();
            let lifecycle_passes = events
                .iter()
                .filter(|event| event.message.as_deref() == Some("task delivery worker failed"))
                .count();
            if projection_passes >= pass && lifecycle_passes >= pass {
                completed = true;
                break;
            }
            // Existing retry policy also retries the ConnectionAcquire wrapper;
            // drive its backoff without changing production timing or sleeping.
            tokio::time::advance(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            completed,
            "both workers must finish their real reconciliation pass"
        );
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
    }
    projection.abort();
    lifecycle.abort();
    let _ = projection.await;
    let _ = lifecycle.await;
    events.extend(transport.fetch_and_clear_events());
    let diagnostics: Vec<_> = events
        .iter()
        .filter(|event| {
            matches!(
                event.message.as_deref(),
                Some(
                    "native Turn finalization reconciler failed"
                        | "task parent occurrence reconciler failed"
                )
            )
        })
        .collect();
    assert_eq!(
        diagnostics.len(),
        2,
        "each operation emits once across repeated passes"
    );
    for operation in ["native_turn_finalization", "task_run_occurrence"] {
        assert_eq!(
            diagnostics
                .iter()
                .filter(|event| {
                    let sentry::protocol::Context::Other(fields) =
                        &event.contexts["Rust Tracing Fields"]
                    else {
                        panic!()
                    };
                    fields["operation"] == operation && fields["cause"] == "pool_closed"
                })
                .count(),
            1
        );
    }
    assert!(
        events
            .iter()
            .filter(|event| {
                event.message.as_deref() == Some("task child terminal reconciler failed")
            })
            .count()
            >= 2,
        "neighbor ERROR is still emitted each pass"
    );
}

#[tokio::test]
async fn real_complete_reconciliation_results_recover_only_their_reporter() {
    let (processor, _, _, _, _, _) = setup_workspace_message_processor().await;
    let processor = Arc::new(processor);
    let background = processor.for_background_reconciliation();
    let mut finalization = Reporter::new(Operation::NativeFinalization);
    let mut occurrence = Reporter::new(Operation::TaskRunOccurrence);
    let (hub, transport, dispatch) = local_capture();
    async {
        let now = std::time::Instant::now();
        finalization.observe::<usize>(&Err(anyhow::anyhow!("fixture failure")), now);
        occurrence.observe::<usize>(&Err(anyhow::anyhow!("fixture failure")), now);
        let result = background
            .reconcile_prepared_native_turn_finalizations(now_timestamp_secs(), 64)
            .await;
        assert!(result.is_ok());
        finalization.observe(&result, now);
        tracing::error!("after finalization success");
        let result = background
            .reconcile_terminal_task_run_occurrence_turns(64)
            .await;
        assert_eq!(result.as_ref().unwrap().changed, 0);
        occurrence.observe_occurrences(&result, now);
        occurrence.observe_occurrences(&result, now);
        tracing::error!("after occurrence success");
    }
    .with_subscriber(dispatch)
    .bind_hub(hub)
    .await;
    let events = transport.fetch_and_clear_events();
    let recovered_operations = |event: &sentry::protocol::Event<'_>| {
        event
            .breadcrumbs
            .iter()
            .filter(|crumb| {
                crumb.message.as_deref() == Some("reconciliation recovered after observed failures")
            })
            .map(|crumb| crumb.data["operation"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        recovered_operations(&events[2]),
        ["native_turn_finalization"]
    );
    assert_eq!(
        recovered_operations(&events[3]),
        ["native_turn_finalization", "task_run_occurrence"]
    );
}

#[tokio::test]
async fn partial_durable_progress_followed_by_real_error_does_not_recover() {
    let (processor, store, workspace_id) =
        setup_execution_window_terminal_turn("diagnostic_thread", "diagnostic_turn").await;
    let processor = Arc::new(processor);
    store
        .prepare_turn_finalization(
            &ItemCompletedNotification {
                workspace_id,
                thread_id: "diagnostic_thread".to_owned(),
                turn_id: "diagnostic_turn".to_owned(),
                item: TurnItem::AgentMessage {
                    id: "diagnostic_item".to_owned(),
                    text: "prepared".to_owned(),
                    phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
                    markdown: None,
                    markdown_version: None,
                },
            },
            1,
            None,
            1_700_000_100,
        )
        .await
        .unwrap();
    // Real durable progress followed by a real synchronization error: the
    // loaded Turn disagrees with the prepared durable Completed transition.
    let (_, mut loaded) = processor
        .thread_manager
        .turn_get("diagnostic_thread", "diagnostic_turn")
        .await
        .unwrap();
    loaded.status = TurnStatus::Failed;
    processor
        .thread_manager
        .commit_terminal_turn("diagnostic_thread", &loaded)
        .await
        .unwrap();
    let (hub, transport, dispatch) = local_capture();
    async {
        let mut reporting = Reporter::new(Operation::NativeFinalization);
        let result = processor
            .for_background_reconciliation()
            .reconcile_prepared_native_turn_finalizations(1_700_000_101, 64)
            .await;
        assert!(result.is_err());
        let (_, durable) = store
            .get_turn("diagnostic_thread", "diagnostic_turn")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            durable.status,
            TurnStatus::Completed,
            "partial durable progress preceded Err"
        );
        reporting.observe(&result, std::time::Instant::now());
        tracing::error!("after partial reconciliation failure");
    }
    .with_subscriber(dispatch)
    .bind_hub(hub)
    .await;
    let events = transport.fetch_and_clear_events();
    assert_eq!(events.len(), 2);
    assert!(events[1].breadcrumbs.iter().all(|crumb| {
        crumb.message.as_deref() != Some("reconciliation recovered after observed failures")
    }));
}
