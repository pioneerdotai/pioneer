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
        &processor.native_turn_event_delivery_workers.live.running
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
        while running.load(Ordering::Acquire)
            || (!terminal_effects
                && !processor
                    .native_turn_event_delivery_workers
                    .tasks
                    .is_empty())
        {
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
    // Existing replay/lifecycle errors mark successive real recovery passes.
    // Delivery now runs in the independent, coalesced consumer owners.
    for pass in 1..=3 {
        let mut completed = false;
        for _ in 0..1024 {
            tokio::task::yield_now().await;
            events.extend(transport.fetch_and_clear_events());
            let projection_passes = events
                .iter()
                .filter(|event| {
                    event.message.as_deref() == Some("native projection/finalization recovery deferred")
                        && matches!(event.contexts.get("Rust Tracing Fields"),
                            Some(sentry::protocol::Context::Other(fields))
                            if fields.get("finalization").and_then(JsonValue::as_bool) == Some(true))
                })
                .count();
            let lifecycle_passes = events
                .iter()
                .filter(|event| {
                    matches!(
                        event.message.as_deref(),
                        Some(
                            "Task lifecycle deadline observation failed"
                                | "Task lifecycle change settlement failed"
                        )
                    )
                })
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
            "both workers must finish retry wave {pass}: {:?}",
            events
                .iter()
                .map(|event| (&event.message, &event.contexts))
                .collect::<Vec<_>>()
        );
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
    }
    projection.abort();
    lifecycle.abort();
    let _ = projection.await;
    let _ = lifecycle.await;
    processor.shutdown_resilience_workers().await;
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
        events.iter().any(|event| {
            event.message.as_deref() == Some("task child terminal reconciler failed")
        }),
        "the initial domain failure remains visible"
    );
    // Once the failed mutation leaves Unknown debt, subsequent waves must
    // report settlement failure rather than reentering domain reconciliation.
    assert!(
        events
            .iter()
            .filter(|event| matches!(
                event.message.as_deref(),
                Some(
                    "task child terminal reconciler failed"
                        | "Task lifecycle change settlement failed"
                )
            ))
            .count()
            >= 3,
        "neighbor errors remain visible through all three retry waves"
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
        assert_eq!(result.as_ref().unwrap().failures.len(), 1);
        assert_eq!(result.as_ref().unwrap().committed_count(), 1);
        let (_, durable) = store
            .get_turn("diagnostic_thread", "diagnostic_turn")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            durable.status,
            TurnStatus::Completed,
            "partial durable progress is retained with its synchronization failure"
        );
        reporting.observe_finalizations(&result, std::time::Instant::now());
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

#[tokio::test]
async fn queue_control_phase_preserves_parked_capacity_and_fractional_predicates() {
    use pioneer_entity::{
        agent_execution as execution, agent_execution_resource_state as resource,
        agent_running_permit as permit, agent_work_branch_schedule as branch,
        agent_work_queue as queue, agent_work_resource_scope as scope,
    };
    use pioneer_sqlite::{SqliteWriteClass, SqliteWriteEvent};
    use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set};

    let observer = Arc::new(NativeSchedulingObserver::default());
    let (_directory, manager, store, workspace) =
        setup_pooled_file_workspace_manager_with_observers(
            Some(observer.clone()),
            Some(observer.clone()),
        )
        .await;
    let processor = Arc::new(MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        test_provider(),
        Arc::new(SessionManager::new()),
        manager,
        store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    ));
    let worker = processor.for_background_reconciliation();
    let db = store.database_connection();
    let second = chrono::DateTime::from_timestamp(100, 0)
        .unwrap()
        .fixed_offset();
    let fractional_due = second + chrono::Duration::milliseconds(500);
    let promotion_now = second + chrono::Duration::milliseconds(800);
    // Historical scopes alone must not create root traversal. Each scope is
    // valid durable metadata; queue is still empty at this point.
    for i in 0..130 {
        execution::ActiveModel {
            id: Set(format!("History{i:014}")),
            workspace_id: Set(workspace.clone()),
            agent_identity_id: Set("Aidentity123456789012".into()),
            identity_source_revision: Set(1),
            identity_source_fingerprint: Set("source".into()),
            home_root_thread_id: Set("queue-thread".into()),
            work_graph_root_execution_id: Set(format!("History{i:014}")),
            requested_identity_selection_json: Set("{}".into()),
            requested_profile_selection_json: Set("{}".into()),
            authorization_context_fingerprint: Set("authorization".into()),
            execution_generation: Set(1),
            status: Set("completed".into()),
            created_at: Set(second),
            updated_at: Set(second),
            finished_at: Set(Some(second)),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        pioneer_crud::ensure_root_resource_scope(
            &db,
            &format!("History{i:014}"),
            1,
            8,
            8,
            8,
            64,
            second,
        )
        .await
        .unwrap();
    }
    let mut empty = pioneer_crud::ControlAdmissionPass::default();
    assert!(
        store.control_admission_quantum(&mut empty).await.unwrap(),
        "empty queue finishes in its first quantum regardless of historical scopes"
    );
    // Durable inputs only: none of these commits sends a Gateway queue kick.
    for root in [
        "Saturated000000000000",
        "Inactive0000000000000",
        "Runnable0000000000000",
        "NoSchedule00000000000",
    ] {
        execution::ActiveModel {
            id: Set(root.into()),
            workspace_id: Set(workspace.clone()),
            agent_identity_id: Set("Aidentity123456789012".into()),
            identity_source_revision: Set(1),
            identity_source_fingerprint: Set("source".into()),
            home_root_thread_id: Set("queue-thread".into()),
            work_graph_root_execution_id: Set(root.into()),
            requested_identity_selection_json: Set("{}".into()),
            requested_profile_selection_json: Set("{}".into()),
            authorization_context_fingerprint: Set("authorization".into()),
            execution_generation: Set(1),
            status: Set("queued".into()),
            created_at: Set(second),
            updated_at: Set(second),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        pioneer_crud::ensure_root_resource_scope(&db, root, 1, 8, 8, 8, 64, second)
            .await
            .unwrap();
        resource::ActiveModel {
            id: Set(format!("state-{root}")),
            execution_id: Set(root.into()),
            attempt_generation: Set(1),
            progress_sequence: Set(0),
            progress_frontier_json: Set("{}".into()),
            local_usage_json: Set("{}".into()),
            branch_key: Set(root.into()),
            fair_order: Set(1),
            status: Set("queued".into()),
            fencing_generation: Set(1),
            created_at: Set(second),
            updated_at: Set(second),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        pioneer_crud::enqueue_agent_execution(
            &db,
            &pioneer_crud::AgentQueueEntryInput {
                id: format!("queue-{root}"),
                root_execution_id: root.into(),
                execution_id: root.into(),
                attempt_generation: 1,
                branch_key: root.into(),
                enqueue_sequence: 1,
                eligible_at: (root != "NoSchedule00000000000").then_some(fractional_due),
                now: second,
            },
        )
        .await
        .unwrap();
    }
    // Several valid queue entries share the saturated root/branch. This
    // extends semantic root-set coverage, not a claim about SQL VM step counts.
    let original_execution = execution::Entity::find_by_id("Saturated000000000000")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let original_resource = resource::Entity::find_by_id("state-Saturated000000000000")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    for index in 1..=3 {
        let id = format!("SaturatedChild{index:07}");
        let mut child: execution::ActiveModel = original_execution.clone().into();
        child.id = Set(id.clone());
        child.insert(&db).await.unwrap();
        let mut state: resource::ActiveModel = original_resource.clone().into();
        state.id = Set(format!("state-{id}"));
        state.execution_id = Set(id.clone());
        state.insert(&db).await.unwrap();
        pioneer_crud::enqueue_agent_execution(
            &db,
            &pioneer_crud::AgentQueueEntryInput {
                id: format!("queue-{id}"),
                root_execution_id: "Saturated000000000000".into(),
                execution_id: id,
                attempt_generation: 1,
                branch_key: "Saturated000000000000".into(),
                enqueue_sequence: index + 1,
                eligible_at: Some(fractional_due),
                now: second,
            },
        )
        .await
        .unwrap();
    }
    // Active historical scopes without eligible queue work do not admit promotion.
    let mut history: execution::ActiveModel = original_execution.into();
    history.id = Set("HistoryOnly0000000000".into());
    history.work_graph_root_execution_id = Set("HistoryOnly0000000000".into());
    history.status = Set("completed".into());
    history.insert(&db).await.unwrap();
    pioneer_crud::ensure_root_resource_scope(&db, "HistoryOnly0000000000", 1, 8, 8, 8, 64, second)
        .await
        .unwrap();
    pioneer_crud::acquire_agent_running_permit(
        &db,
        "held",
        "Saturated000000000000",
        "old-attempt",
        1,
        second,
    )
    .await
    .unwrap();
    scope::Entity::update_many()
        .col_expr(
            scope::Column::Status,
            sea_orm::sea_query::Expr::value("closed"),
        )
        .filter(
            scope::Column::RootExecutionId
                .is_in(["Inactive0000000000000", "Runnable0000000000000"]),
        )
        .exec(&db)
        .await
        .unwrap();
    branch::Entity::delete_many()
        .filter(branch::Column::RootExecutionId.eq("NoSchedule00000000000"))
        .exec(&db)
        .await
        .unwrap();

    // The actual production control phase owns the skip; a repeated durable
    // parked snapshot must not enter the serialized writer at all.
    for _ in 0..2 {
        observer.writes.lock().unwrap().clear();
        let parked = worker
            .crud_store
            .recovery_admission("owner", 100)
            .await
            .unwrap();
        assert!(!parked.queue);
        assert!(
            worker
                .promote_admitted_agent_queue(Some(&parked), || promotion_now)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(observer.writes.lock().unwrap().is_empty());
    }
    scope::Entity::update_many()
        .col_expr(
            scope::Column::Status,
            sea_orm::sea_query::Expr::value("active"),
        )
        .filter(scope::Column::RootExecutionId.eq("Runnable0000000000000"))
        .exec(&db)
        .await
        .unwrap();
    // An exact next-second deadline is excluded; fractional deadlines
    // inside second 100 are admitted even though integer 100.0 precedes them.
    queue::Entity::update_many()
        .col_expr(
            queue::Column::EligibleAt,
            sea_orm::sea_query::Expr::value(Some(second + chrono::Duration::seconds(1))),
        )
        .filter(queue::Column::RootExecutionId.eq("Runnable0000000000000"))
        .exec(&db)
        .await
        .unwrap();
    assert!(
        !worker
            .crud_store
            .recovery_admission("owner", 100)
            .await
            .unwrap()
            .queue
    );
    assert!(
        worker
            .crud_store
            .recovery_admission("owner", 101)
            .await
            .unwrap()
            .queue
    );
    queue::Entity::update_many()
        .col_expr(
            queue::Column::EligibleAt,
            sea_orm::sea_query::Expr::value(Some(fractional_due)),
        )
        .filter(queue::Column::RootExecutionId.eq("Runnable0000000000000"))
        .exec(&db)
        .await
        .unwrap();
    assert!(
        !worker
            .crud_store
            .recovery_admission("owner", 99)
            .await
            .unwrap()
            .queue
    );
    let ready = worker
        .crud_store
        .recovery_admission("owner", 100)
        .await
        .unwrap();
    assert!(
        ready.queue,
        "runnable root is independent of saturated/inactive roots"
    );
    assert_eq!(ready.queue_next, Some(fractional_due));
    let mut fractional = pioneer_crud::ControlAdmissionPass::default();
    timeout(Duration::from_secs(10), async {
        while !store
            .control_admission_quantum(&mut fractional)
            .await
            .unwrap()
        {}
    })
    .await
    .unwrap();
    let mut exact = pioneer_crud::RecoveryAdmission::unknown();
    fractional.apply_at(&mut exact, second + chrono::Duration::milliseconds(200));
    assert!(
        !exact.queue,
        "retained owner does not admit a future fractional deadline"
    );
    assert_eq!(exact.control_deadline(), Some(fractional_due));
    fractional.apply_at(&mut exact, fractional_due);
    assert!(exact.queue, "full-resolution due admission preserves F1");
    observer.writes.lock().unwrap().clear();
    assert!(
        worker
            .promote_admitted_agent_queue(Some(&ready), || second
                + chrono::Duration::milliseconds(200))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        observer.writes.lock().unwrap().is_empty(),
        "conservative early admission waits for the exact fractional clock without a writer"
    );
    observer.writes.lock().unwrap().clear();
    let promoted = worker
        .promote_admitted_agent_queue(Some(&ready), || promotion_now)
        .await
        .unwrap();
    assert_eq!(
        promoted
            .iter()
            .map(|row| row.root_execution_id.as_str())
            .collect::<Vec<_>>(),
        vec!["Runnable0000000000000"]
    );
    assert!(observer.writes.lock().unwrap().iter().any(|event| matches!(
        event,
        SqliteWriteEvent::Enqueued {
            class: SqliteWriteClass::Critical,
            ..
        }
    )));

    // Predicate oracle: re-read after durable release/activation/reconstruction.
    // Raw fixture ORM writes are not supported online producer notifications;
    // the retained worker uses guarded production commits, not foreign polling.
    for root in [
        "Saturated000000000000",
        "Inactive0000000000000",
        "NoSchedule00000000000",
    ] {
        assert!(
            !worker
                .crud_store
                .recovery_admission("owner", 100)
                .await
                .unwrap()
                .queue
        );
        match root {
            "Saturated000000000000" => {
                permit::Entity::update_many()
                    .col_expr(
                        permit::Column::Status,
                        sea_orm::sea_query::Expr::value("released"),
                    )
                    .col_expr(
                        permit::Column::ReleasedAt,
                        sea_orm::sea_query::Expr::value(Some(promotion_now)),
                    )
                    .filter(permit::Column::Id.eq("held"))
                    .exec(&db)
                    .await
                    .unwrap();
            }
            "Inactive0000000000000" => {
                scope::Entity::update_many()
                    .col_expr(
                        scope::Column::Status,
                        sea_orm::sea_query::Expr::value("active"),
                    )
                    .filter(scope::Column::RootExecutionId.eq(root))
                    .exec(&db)
                    .await
                    .unwrap();
            }
            _ => {
                pioneer_crud::enqueue_agent_execution(
                    &db,
                    &pioneer_crud::AgentQueueEntryInput {
                        id: format!("queue-{root}"),
                        root_execution_id: root.into(),
                        execution_id: root.into(),
                        attempt_generation: 1,
                        branch_key: root.into(),
                        enqueue_sequence: 1,
                        eligible_at: None,
                        now: second,
                    },
                )
                .await
                .unwrap();
            }
        }
        let ready = worker
            .crud_store
            .recovery_admission("owner", 100)
            .await
            .unwrap();
        assert!(ready.queue);
        if root == "NoSchedule00000000000" {
            assert_eq!(
                ready.queue_next, None,
                "NULL is immediate, never a sampled future wall deadline"
            );
            assert!(ready.control_runnable());
        }
        let promoted = worker
            .promote_admitted_agent_queue(Some(&ready), || promotion_now)
            .await
            .unwrap();
        assert_eq!(promoted.len(), 1);
        assert_eq!(promoted[0].root_execution_id, root);
        assert_eq!(
            execution::Entity::find_by_id(root)
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .status,
            "running"
        );
    }
    assert!(
        !worker
            .crud_store
            .recovery_admission("owner", 101)
            .await
            .unwrap()
            .queue
    );
    // Extend this authority matrix with the actual retained control owner.
    // More than a raw page of future queue inputs shares one available root;
    // the old saturated root and its queued children remain independent.
    let now = pioneer_crud::utc_now();
    let future = now + chrono::Duration::hours(1) + chrono::Duration::nanoseconds(1);
    let mut root: execution::ActiveModel = execution::Entity::find_by_id("Runnable0000000000000")
        .one(&db)
        .await
        .unwrap()
        .unwrap()
        .into();
    root.id = Set("F05FutureRoot00000000".into());
    root.work_graph_root_execution_id = Set("F05FutureRoot00000000".into());
    root.status = Set("queued".into());
    root.insert(&db).await.unwrap();
    pioneer_crud::ensure_root_resource_scope(
        &db,
        "F05FutureRoot00000000",
        2,
        128,
        8,
        128,
        128,
        now,
    )
    .await
    .unwrap();
    for i in 0..66 {
        let id = format!("F05Queue{i:013}");
        let mut child: execution::ActiveModel =
            execution::Entity::find_by_id("F05FutureRoot00000000")
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .into();
        child.id = Set(id.clone());
        child.insert(&db).await.unwrap();
        let mut state: resource::ActiveModel =
            resource::Entity::find_by_id("state-Runnable0000000000000")
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .into();
        state.id = Set(format!("state-{id}"));
        state.execution_id = Set(id.clone());
        state.status = Set("queued".into());
        state.branch_key = Set("F05FutureRoot00000000".into());
        state.permit_id = Set(None);
        state.last_heartbeat_at = Set(None);
        state.idle_deadline = Set(None);
        state.hard_deadline = Set(None);
        state.insert(&db).await.unwrap();
        pioneer_crud::enqueue_agent_execution(
            &db,
            &pioneer_crud::AgentQueueEntryInput {
                id: format!("f05-queue-{i:03}"),
                root_execution_id: "F05FutureRoot00000000".into(),
                execution_id: id,
                attempt_generation: 1,
                branch_key: "F05FutureRoot00000000".into(),
                enqueue_sequence: i + 1,
                eligible_at: Some(if i == 65 {
                    now - chrono::Duration::seconds(1)
                } else {
                    future
                }),
                now,
            },
        )
        .await
        .unwrap();
    }
    processor.arm_completed_history_preparation_barrier("__control_admission_raw_page__");
    let stop = tokio_util::sync::CancellationToken::new();
    let control = tokio::spawn(MessageProcessor::run_recovery_control_worker(
        Arc::downgrade(&processor),
        stop.clone(),
    ));
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        execution::Entity::find_by_id("F05Queue0000000000065")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .status,
        "queued"
    );
    processor.arm_completed_history_preparation_barrier("__control_owner_settled__");
    processor.release_completed_history_preparation_barrier();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        execution::Entity::find_by_id("F05Queue0000000000065")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    let mut pass = pioneer_crud::ControlAdmissionPass::default();
    timeout(Duration::from_secs(10), async {
        while !store.control_admission_quantum(&mut pass).await.unwrap() {}
    })
    .await
    .unwrap();
    let mut nearest = pioneer_crud::RecoveryAdmission::unknown();
    pass.apply(&mut nearest, now.timestamp());
    assert_eq!(nearest.queue_next, Some(future));
    observer.writes.lock().unwrap().clear();
    processor.release_completed_history_preparation_barrier();
    tokio::task::yield_now().await;
    // Old-key source change behind EOF, with ordinary guarded Unknown/commit
    // publication. The real fair promotion still owns the permit/CAS decision.
    let change = store.background_work("recovery.control").changing();
    processor.arm_completed_history_preparation_barrier("__control_owner_settled__");
    queue::Entity::update_many()
        .col_expr(
            queue::Column::EligibleAt,
            sea_orm::sea_query::Expr::value(Some(now - chrono::Duration::seconds(1))),
        )
        .filter(queue::Column::Id.eq("f05-queue-000"))
        .exec(&db)
        .await
        .unwrap();
    change.finish(true);
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        execution::Entity::find_by_id("F05Queue0000000000000")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    processor.release_completed_history_preparation_barrier();
    tokio::task::yield_now().await;
    let ticker = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    ticker.abort();
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();
    // Ordinary lower finalization publishes actual capacity release. It must
    // reintroduce the future clock even though every queue key is behind EOF.
    processor.arm_completed_history_preparation_barrier("__control_owner_settled__");
    assert!(
        store
            .finalize_agent_execution(
                "F05Queue0000000000065",
                "completed",
                pioneer_crud::utc_now()
            )
            .await
            .unwrap()
    );
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    let mut released = pioneer_crud::ControlAdmissionPass::default();
    timeout(Duration::from_secs(10), async {
        while !store
            .control_admission_quantum(&mut released)
            .await
            .unwrap()
        {}
    })
    .await
    .unwrap();
    let mut snapshot = pioneer_crud::RecoveryAdmission::unknown();
    released.apply(&mut snapshot, now.timestamp());
    assert_eq!(snapshot.queue_next, Some(future));
    processor.release_completed_history_preparation_barrier();
    stop.cancel();
    timeout(Duration::from_secs(10), control)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn task_bus_retains_committed_handoff_and_coalesces_empty_hints_without_writes() {
    use pioneer_entity::task_child_reconcile_scope as scope;
    use pioneer_sqlite::{
        SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadOutcome, SqliteWriteExecutor,
    };
    use sea_orm::{ConnectOptions, EntityTrait};
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("task-bus-handoff.sqlite");
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
        .map_sqlx_sqlite_opts(|opts| opts.pragma("query_only", "ON"));
    let reader = Database::connect(options).await.unwrap();
    let observer = Arc::new(NativeSchedulingObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    let store = Arc::new(CrudStore::new(database.clone()));
    let processor = Arc::new(MessageProcessor::with_agent_manager(
        Arc::new(ThreadManager::new("test-model", "openai")),
        Arc::new(AgentManager::new(test_provider(), test_tool_loop_config())),
        Arc::new(SessionManager::new()),
        Arc::new(WorkspaceManager::new(database)),
        store.clone(),
    ));
    // Complete startup reconstruction before testing the normal Empty handoff.
    while store
        .with_maintenance_access()
        .expand_child_scope(now_timestamp_secs(), true)
        .await
        .unwrap()
    {}
    processor.arm_completed_history_preparation_barrier("__task_lifecycle_before_wait__");
    let worker = tokio::spawn(MessageProcessor::run_task_lifecycle_resilience_worker(
        Arc::downgrade(&processor),
    ));
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert!(
        store
            .task_lifecycle_deadlines(now_timestamp_secs())
            .await
            .unwrap()
            .child_scope
            .is_none()
    );
    processor.release_completed_history_preparation_barrier();
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert!(
        observer.reads.lock().unwrap().is_empty(),
        "cold settled lifecycle has no discovery timer"
    );
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();
    // Establish another actual Empty-read -> wait handoff after observing the
    // cold dormant interval. This process-local hint carries no business event.
    processor.arm_completed_history_preparation_barrier("__task_lifecycle_before_wait__");
    store.background_work("tasks.lifecycle").invalidate();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let scenario = async {
        // The real worker observed Empty with a generation fence, but has not registered
        // its wait. The transaction creates a real typed-producer scope ticket.
        let event = store
            .append_task_event(
                TaskEventPayload::TaskCreated {
                    task: fanout_test_task("task_bus_handoff", &workspace),
                },
                now_timestamp_secs(),
            )
            .await
            .unwrap();
        let key = ("task".to_owned(), "task_bus_handoff".to_owned());
        assert!(
            scope::Entity::find_by_id(key.clone())
                .one(&store.database_connection())
                .await
                .unwrap()
                .is_some()
        );
        let bus = processor.task_runtime.event_bus();
        for _ in 0..128 {
            bus.publish(event.clone()).await;
        }
        observer.writes.lock().unwrap().clear();
        processor.arm_completed_history_preparation_barrier("__task_lifecycle_before_wait__");
        processor.release_completed_history_preparation_barrier();
        processor
            .wait_for_completed_history_preparation_barrier()
            .await;
        assert_eq!(
            tokio::time::Instant::now(),
            started,
            "retained commit generation must run without a fallback timer"
        );
        assert!(
            scope::Entity::find_by_id(key)
                .one(&store.database_connection())
                .await
                .unwrap()
                .is_none(),
            "real child scope expansion must ACK the committed ticket"
        );
        assert!(
            !observer.writes.lock().unwrap().is_empty(),
            "useful work must reach its authoritative writer"
        );

        // Business bus replay carries no new durable invalidation. The real
        // lower commit already advanced the independent registry generation.
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        for _ in 0..1000 {
            bus.publish(event.clone()).await;
        }
        processor.release_completed_history_preparation_barrier();
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(300)).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            observer
                .reads
                .lock()
                .unwrap()
                .iter()
                .filter(|event| matches!(
                    event,
                    SqliteReadEvent::OperationFinished {
                        class: SqliteReadClass::Maintenance,
                        outcome: SqliteReadOutcome::Ok,
                        ..
                    }
                ))
                .count(),
            0,
            "settled cold/history lifecycle has no repeated Empty discovery"
        );
        assert!(observer.writes.lock().unwrap().is_empty());
    };
    tokio::pin!(scenario);
    // Keep this current-thread runtime runnable during SQLite's real worker
    // I/O. Virtual time moves only at the explicit advance above, not because
    // an otherwise idle runtime auto-advanced its pool/fallback timeout.
    let wall = std::time::Instant::now();
    loop {
        if futures_util::poll!(&mut scenario).is_ready() {
            break;
        }
        assert!(
            wall.elapsed() < Duration::from_secs(10),
            "bus scenario must complete without advancing a missing wake's fallback"
        );
        tokio::task::yield_now().await;
    }
    worker.abort();
    let _ = worker.await;
    tokio::time::resume();
    processor.shutdown_resilience_workers().await;
}

#[tokio::test]
async fn control_owner_parks_cold_and_terminal_history_and_handles_lower_committed_outbox() {
    use pioneer_entity::recovery_terminalization_outbox as terminal;
    use sea_orm::sea_query::Expr;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    let observer = Arc::new(NativeSchedulingObserver::default());
    let (_directory, manager, store, workspace) =
        setup_pooled_file_workspace_manager_with_observers(
            Some(observer.clone()),
            Some(observer.clone()),
        )
        .await;
    let processor = Arc::new(MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        test_provider(),
        Arc::new(SessionManager::new()),
        manager,
        store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    ));
    // Recovery terminalization requires the same durable Task cleanup adapter
    // installed during Gateway startup. Keep that authority in this fixture.
    processor.bind_task_bridge().await;
    let cancellation = tokio_util::sync::CancellationToken::new();
    let label = "__control_owner_settled__";
    processor.arm_completed_history_preparation_barrier(label);
    // The production startup calls this same independent owner alongside its
    // siblings. Isolate its physical SQL here; sibling owners own other tests.
    let worker = tokio::spawn(MessageProcessor::run_recovery_control_worker(
        Arc::downgrade(&processor),
        cancellation.clone(),
    ));
    timeout(
        Duration::from_secs(5),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert!(
        observer.writes.lock().unwrap().is_empty(),
        "cold observation cannot admit an empty promotion writer"
    );
    processor.release_completed_history_preparation_barrier();
    tokio::task::yield_now().await;
    observer.reads.lock().unwrap().clear();
    let settled_ticker = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert!(
        observer.reads.lock().unwrap().is_empty(),
        "no two-second admission or independent empty scans"
    );
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();
    settled_ticker.abort();

    // Preserve the initial genuinely cold/empty startup above. Hold the real
    // settled owner before restoring covered history, so none can dispatch early.
    processor.arm_completed_history_preparation_barrier(label);
    store.background_work("recovery.control").invalidate();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    let restoration = store.background_work("recovery.control").changing();
    // F06-3: genuine terminal unresolved jobs in distinct materialized Turns.
    // Normal transition creates each outbox; only its supported retry/claim
    // state is restored below. No cloned job or attempt identity.
    let covered_now = now_timestamp_secs();
    let covered_deadline =
        pioneer_crud::unix_to_datetime(covered_now + 7200) + chrono::Duration::milliseconds(123);
    let mut covered = Vec::new();
    for index in 0..130 {
        let turn = format!("covered-terminal-{index:03}");
        materialize_cli_runtime_turn_with_text(&store, &workspace, &turn, &turn, "terminal").await;
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace.clone(),
                    thread_id: turn.clone(),
                    turn_id: turn.clone(),
                    item: web_fetch_item("covered-item"),
                },
                covered_now,
            )
            .await
            .unwrap();
        let job = store
            .enqueue_recovery_job(
                turn.clone(),
                "covered-item".into(),
                TurnItemType::WebFetch,
                None,
                RecoveryTrigger::Timeout,
                RecoveryAction::RetryWithBackoff,
                Some("expired".into()),
                None,
                None,
                Some(60_000),
                1,
                2,
                serde_json::json!({}),
                serde_json::json!({}),
                covered_now,
            )
            .await
            .unwrap();
        store
            .mark_recovery_job_terminal(
                &job.id,
                RecoveryJobStatus::Exhausted,
                Some("exhausted".into()),
                covered_now,
            )
            .await
            .unwrap();
        assert!(
            store
                .get_recovery_job(&job.id)
                .await
                .unwrap()
                .unwrap()
                .resolution_pending
        );
        terminal::Entity::update_many()
            .col_expr(
                terminal::Column::Status,
                Expr::value(if index % 2 == 0 {
                    "failed"
                } else {
                    "delivering"
                }),
            )
            .col_expr(terminal::Column::NextRunAt, Expr::value(covered_deadline))
            .col_expr(
                terminal::Column::ClaimExpiresAt,
                Expr::value(if index % 2 == 0 {
                    None
                } else {
                    Some(covered_deadline)
                }),
            )
            .col_expr(
                terminal::Column::ClaimToken,
                Expr::value(if index % 2 == 0 {
                    None
                } else {
                    Some("covered_claim_token01".to_owned())
                }),
            )
            .filter(terminal::Column::RecoveryJobId.eq(&job.id))
            .exec(&store.database_connection())
            .await
            .unwrap();
        covered.push((job.id, turn));
    }
    covered.sort_by(|a, b| a.0.cmp(&b.0));
    // Fixture setup writes are not owner work. The all-covered finite EOF below
    // must park without acquiring a writer for absence of missing outboxes.
    observer.writes.lock().unwrap().clear();
    restoration.finish(true);
    processor.arm_completed_history_preparation_barrier(label);
    processor.release_completed_history_preparation_barrier();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert!(
        observer.writes.lock().unwrap().is_empty(),
        "covered future outboxes need no audit writer"
    );
    // All-covered successful raw EOF retains only concrete future clocks.
    processor.release_completed_history_preparation_barrier();
    tokio::task::yield_now().await;
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    let covered_ticker = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();
    covered_ticker.abort();

    // Restore missing-outbox history after all-covered EOF: first the last raw
    // key (healthy tail), then an early key after a further completed pass.
    // Explicit invalidation models the supported restoration publication;
    // these direct fixture deletes are not uncooperative online writers.
    for (job_id, turn) in [covered.last().unwrap(), covered.first().unwrap()] {
        processor.arm_completed_history_preparation_barrier(label);
        let wake = store.background_work("recovery.control");
        let restoration = wake.changing();
        terminal::Entity::delete_by_id(job_id.clone())
            .exec(&store.database_connection())
            .await
            .unwrap();
        restoration.finish(true);
        let result = timeout(Duration::from_secs(10), async {
            loop {
                processor
                    .wait_for_completed_history_preparation_barrier()
                    .await;
                let row = terminal::Entity::find_by_id(job_id.clone())
                    .one(&store.database_connection())
                    .await
                    .unwrap();
                if row.as_ref().is_some_and(|row| row.status == "delivered") {
                    break;
                }
                processor.arm_completed_history_preparation_barrier(label);
                processor.release_completed_history_preparation_barrier();
            }
        })
        .await;
        assert!(
            result.is_ok(),
            "restored outbox must deliver: row={:?}; job={:?}; turn={:?}",
            terminal::Entity::find_by_id(job_id.clone())
                .one(&store.database_connection())
                .await,
            store.get_recovery_job(job_id).await,
            store.get_turn(turn, turn).await
        );
        assert_eq!(
            store.get_turn(turn, turn).await.unwrap().unwrap().1.status,
            TurnStatus::Failed
        );
        let clock = store
            .recovery_admission(processor.turn_execution_owner_id.as_ref(), covered_now)
            .await
            .unwrap();
        assert_eq!(
            clock.terminal_delivery_next,
            Some(pioneer_crud::unix_to_datetime(covered_now + 7201)),
            "untouched future retry/leased outboxes keep their exact ceiling"
        );
        processor.release_completed_history_preparation_barrier();
        tokio::task::yield_now().await;
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        let quiet_ticker = tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(300)).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert!(observer.reads.lock().unwrap().is_empty());
        assert!(observer.writes.lock().unwrap().is_empty());
        tokio::time::resume();
        quiet_ticker.abort();
    }

    // Commit before wait registration and during its previous Empty are both
    // retained by lower CrudStore sources, never an explicit processor kick.
    let lower = CrudStore::new(store.database_connection());
    materialize_cli_runtime_turn_with_text(
        &lower,
        &workspace,
        "control-input",
        "control-turn",
        "finish",
    )
    .await;
    let now = now_timestamp_secs();
    lower
        .materialize_item_started(
            ItemStartedNotification {
                workspace_id: workspace.clone(),
                thread_id: "control-input".into(),
                turn_id: "control-turn".into(),
                item: web_fetch_item("control-item"),
            },
            now,
        )
        .await
        .unwrap();
    let job = lower
        .enqueue_recovery_job(
            "control-turn".into(),
            "control-item".into(),
            TurnItemType::WebFetch,
            None,
            RecoveryTrigger::Timeout,
            RecoveryAction::RetryWithBackoff,
            Some("expired".into()),
            None,
            None,
            Some(60_000),
            1,
            2,
            serde_json::json!({}),
            serde_json::json!({}),
            now,
        )
        .await
        .unwrap();
    lower
        .mark_recovery_job_terminal(
            &job.id,
            RecoveryJobStatus::Exhausted,
            Some("exhausted".into()),
            now,
        )
        .await
        .unwrap();
    // A barrier may first observe an older pass whose generation was rejected.
    // Fence the actual durable outcome, then the successful terminal-history
    // publication. No wall-time polling is used to make the outcome arrive.
    timeout(Duration::from_secs(5), async {
        loop {
            if lower
                .get_turn("control-input", "control-turn")
                .await
                .unwrap()
                .unwrap()
                .1
                .status
                == TurnStatus::Failed
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    processor.arm_completed_history_preparation_barrier(label);
    lower.background_work("recovery.control").invalidate();
    timeout(
        Duration::from_secs(5),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    let outbox = pioneer_entity::recovery_terminalization_outbox::Entity::find_by_id(job.id)
        .one(&store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outbox.status, "delivered");
    assert!(outbox.delivered_at.is_some());
    assert!(
        observer.writes.lock().unwrap().iter().any(|event| matches!(
            event,
            pioneer_sqlite::SqliteWriteEvent::Acquired {
                class: pioneer_sqlite::SqliteWriteClass::Critical,
                ..
            }
        )),
        "useful terminalization preserves the control commit class"
    );
    processor.release_completed_history_preparation_barrier();
    tokio::task::yield_now().await;
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    let settled_ticker = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert!(
        observer.reads.lock().unwrap().is_empty(),
        "settled terminal history must not reopen control discovery"
    );
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();
    settled_ticker.abort();
    // F08-2: many unrelated Active Turns, plus a restored legacy Pending
    // repair sharing a Turn with a genuine current Active episode.
    processor.arm_completed_history_preparation_barrier(label);
    store.background_work("recovery.control").invalidate();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    let now = now_timestamp_secs();
    let wake = store.background_work("recovery.control");
    wake.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
    let restoration = wake.changing();
    let mut active_jobs = Vec::new();
    let mut repair = None;
    let mut guarded = None;
    for index in 0..132 {
        let turn = format!("terminal-repair-population-{index:03}");
        let item = format!("terminal-repair-item-{index:03}");
        materialize_cli_runtime_turn_with_text(&store, &workspace, &turn, &turn, "repair").await;
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace.clone(),
                    thread_id: turn.clone(),
                    turn_id: turn.clone(),
                    item: web_fetch_item(&item),
                },
                now,
            )
            .await
            .unwrap();
        let job = store
            .enqueue_recovery_job(
                turn.clone(),
                item.clone(),
                TurnItemType::WebFetch,
                None,
                RecoveryTrigger::Timeout,
                RecoveryAction::MarkFailed,
                None,
                None,
                None,
                None,
                0,
                1,
                serde_json::json!({}),
                serde_json::json!({}),
                now,
            )
            .await
            .unwrap();
        if index == 130 {
            repair = Some((job, turn));
            continue;
        }
        // Two ordinary unresolved episodes in one Turn are prohibited by
        // uq_recovery_job_unresolved_turn. Retire the old episode normally,
        // then enqueue/claim/activate its genuine replacement before restoring
        // only the old status. No fake Active row, claim or disabled index.
        let (job, restored) = if index == 131 {
            assert!(
                store
                    .mark_recovery_job_terminal(&job.id, RecoveryJobStatus::Cancelled, None, now,)
                    .await
                    .unwrap()
            );
            let retired = pioneer_entity::recovery_job::Entity::find_by_id(job.id.clone())
                .one(&store.database_connection())
                .await
                .unwrap()
                .unwrap();
            assert!(!retired.resolution_pending);
            let replacement = store
                .enqueue_recovery_job(
                    turn.clone(),
                    item,
                    TurnItemType::WebFetch,
                    None,
                    RecoveryTrigger::Timeout,
                    RecoveryAction::MarkFailed,
                    None,
                    None,
                    None,
                    None,
                    0,
                    1,
                    serde_json::json!({}),
                    serde_json::json!({}),
                    now,
                )
                .await
                .unwrap();
            assert_ne!(replacement.id, job.id);
            (replacement, Some(job))
        } else {
            (job, None)
        };
        let claimed = store
            .claim_due_recovery_job_candidate(now, 45, &job.id)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        let attempt = pioneer_protocol::generate_id(21);
        assert!(matches!(
            store
                .mark_claimed_recovery_job_active(
                    &job.id,
                    claimed[0].claim_token.as_deref().unwrap(),
                    &attempt,
                    now,
                )
                .await
                .unwrap(),
            pioneer_crud::ClaimedRecoveryActivation::Activated
        ));
        let active = store.get_recovery_job(&job.id).await.unwrap().unwrap();
        assert_eq!(active.status, RecoveryJobStatus::Active);
        assert_eq!(active.active_attempt_id.as_deref(), Some(attempt.as_str()));
        assert!(store.get_turn_liveness(&turn).await.unwrap().is_some());
        if let Some(old) = restored {
            // Explicit restored legacy input: Pending with resolution_pending
            // false is NOT represented as normal producer provenance. Restore
            // status alone, leaving claim/attempt IDs and both UNIQUE guards.
            use pioneer_entity::recovery_job as row;
            let restored = row::Entity::update_many()
                .col_expr(row::Column::Status, Expr::value("pending"))
                .filter(row::Column::Id.eq(&old.id))
                .filter(row::Column::Status.eq("cancelled"))
                .filter(row::Column::ResolutionPending.eq(false))
                .exec(&store.database_connection())
                .await
                .unwrap();
            assert_eq!(restored.rows_affected, 1);
            let restored = row::Entity::find_by_id(old.id.clone())
                .one(&store.database_connection())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(restored.status, "pending");
            assert!(!restored.resolution_pending);
            guarded = Some((old, active, turn, attempt));
        } else {
            active_jobs.push(active);
        }
    }
    let (repair, repair_turn) = repair.unwrap();
    let (legacy, active, guarded_turn, attempt) = guarded.unwrap();
    assert_eq!(active_jobs.len(), 130);
    let selected = store
        .list_due_pending_recovery_jobs_by_action(RecoveryAction::MarkFailed, now, 64)
        .await
        .unwrap();
    assert!(selected.iter().any(|job| job.id == repair.id));
    assert!(!selected.iter().any(|job| job.id == legacy.id));
    // Reader exclusion is advisory; the actual writer also rejects repair.
    assert!(
        !store
            .mark_due_pending_recovery_job_terminal_if_turn_idle(
                &legacy.id,
                RecoveryAction::MarkFailed,
                RecoveryJobStatus::Failed,
                None,
                now,
            )
            .await
            .unwrap()
    );
    assert!(
        processor
            .recovery_coordinator
            .terminalize_pending_mark_failed_job(legacy.clone(), now,)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        !store
            .mark_recovery_job_terminal_after_attempt(
                &active.id,
                "stale-attempt",
                RecoveryJobStatus::Cancelled,
                None,
                now,
            )
            .await
            .unwrap()
    );
    restoration.finish(true);
    processor.arm_completed_history_preparation_barrier(label);
    processor.release_completed_history_preparation_barrier();
    timeout(Duration::from_secs(10), async {
        loop {
            processor
                .wait_for_completed_history_preparation_barrier()
                .await;
            if store
                .get_turn(&repair_turn, &repair_turn)
                .await
                .unwrap()
                .unwrap()
                .1
                .status
                == TurnStatus::Failed
            {
                break;
            }
            processor.arm_completed_history_preparation_barrier(label);
            processor.release_completed_history_preparation_barrier();
        }
    })
    .await
    .unwrap();
    let repaired = store.get_recovery_job(&repair.id).await.unwrap().unwrap();
    assert_eq!(repaired.status, RecoveryJobStatus::Failed);
    assert_eq!(repaired.run_count, 0);
    assert!(repaired.claim_token.is_none());
    assert_eq!(
        store
            .get_recovery_job(&legacy.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RecoveryJobStatus::Pending
    );
    assert_eq!(
        store
            .get_recovery_job(&active.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RecoveryJobStatus::Active
    );
    assert_eq!(
        store
            .get_turn(&guarded_turn, &guarded_turn)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::InProgress
    );
    processor.arm_completed_history_preparation_barrier(label);
    // Actual attempt-owned release cancels the current episode and frees the
    // unresolved slot. Defer-to-Pending would keep it and conflict with repair.
    assert!(
        store
            .mark_recovery_job_terminal_after_attempt(
                &active.id,
                &attempt,
                RecoveryJobStatus::Cancelled,
                None,
                now,
            )
            .await
            .unwrap()
    );
    processor.release_completed_history_preparation_barrier();
    timeout(Duration::from_secs(10), async {
        loop {
            processor
                .wait_for_completed_history_preparation_barrier()
                .await;
            if store
                .get_turn(&guarded_turn, &guarded_turn)
                .await
                .unwrap()
                .unwrap()
                .1
                .status
                == TurnStatus::Failed
            {
                break;
            }
            processor.arm_completed_history_preparation_barrier(label);
            processor.release_completed_history_preparation_barrier();
        }
    })
    .await
    .unwrap();
    let repaired_legacy = store.get_recovery_job(&legacy.id).await.unwrap().unwrap();
    assert_eq!(repaired_legacy.status, RecoveryJobStatus::Failed);
    assert_eq!(repaired_legacy.run_count, 0);
    assert!(repaired_legacy.claim_token.is_none());
    let released = store.get_recovery_job(&active.id).await.unwrap().unwrap();
    assert_eq!(released.status, RecoveryJobStatus::Cancelled);
    assert_eq!(released.run_count, active.run_count + 1);
    assert_eq!(
        released.last_failure_attempt_id.as_deref(),
        Some(attempt.as_str())
    );
    assert!(released.active_attempt_id.is_none());
    assert!(released.claim_token.is_none());
    for job in active_jobs {
        assert_eq!(
            store
                .get_recovery_job(&job.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            RecoveryJobStatus::Active
        );
    }
    processor.release_completed_history_preparation_barrier();
    tokio::task::yield_now().await;
    let ticker = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..128 {
        tokio::task::yield_now().await;
    }
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();
    ticker.abort();
    cancellation.cancel();
    worker.await.unwrap();
    processor.shutdown_resilience_workers().await;
}

#[tokio::test]
async fn transient_recovery_admission_failure_does_not_delay_late_terminalization() {
    // Capture the existing diagnostic at the real control owner. Other phase
    // diagnostics would also be retained here, rather than hidden by the fault.
    #[derive(Clone, Default)]
    struct PollErrors(Arc<std::sync::Mutex<Vec<String>>>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for PollErrors {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Operation(Option<String>);
            impl tracing::field::Visit for Operation {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    if field.name() == "operation" {
                        self.0 = Some(value.to_owned());
                    }
                }
                fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
            }
            if event.metadata().target() == "pioneer_gateway::message"
                && matches!(
                    *event.metadata().level(),
                    tracing::Level::WARN | tracing::Level::ERROR
                )
            {
                let mut operation = Operation(None);
                event.record(&mut operation);
                if let Some(operation) = operation.0 {
                    self.0.lock().unwrap().push(operation);
                }
            }
        }
    }

    let (processor, _, _, _, _, workspace) = setup_workspace_message_processor().await;
    let processor = Arc::new(processor);
    processor.bind_task_bridge().await;
    let store = processor.crud_store.clone();
    let errors = PollErrors::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(errors.clone()));
    // The reader observer cannot return failures. Use the existing private
    // one-shot typed-error pattern at only this observation boundary; it never
    // closes the DB or changes any subsequent authoritative operation.
    let failure = anyhow::Error::new(sea_orm::DbErr::ConnectionAcquire(
        sea_orm::ConnAcquireErr::Timeout,
    ));
    assert!(pioneer_sqlite::is_anyhow_sqlite_transient_access(&failure));
    *processor.recovery_admission_failure.lock().unwrap() = Some(failure);
    processor.arm_completed_history_preparation_barrier("__resilience_before_history_poll__");
    // Keep durable deadlines <= the timestamp captured by this control pass.
    let now = now_timestamp_secs();
    // The same production control body, isolated from sibling owners so an
    // unrelated provider/history diagnostic cannot obscure this error path.
    let worker = tokio::spawn(MessageProcessor::run_recovery_control_worker(
        Arc::downgrade(&processor),
        processor.cli_history_shutdown.clone(),
    ));
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .expect("real control pass must observe the one-shot failure before its history boundary");
    assert!(
        processor
            .recovery_admission_failure
            .lock()
            .unwrap()
            .is_none()
    );
    assert_eq!(*errors.0.lock().unwrap(), vec!["recovery admission"]);

    // Create useful late-phase input after startup and admission, while the
    // actual control pass is held. A future retry keeps the independent due-job
    // consumer from taking the pending job before it is marked terminal.
    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let scenario = async {
        materialize_cli_runtime_turn_with_text(
            &store,
            &workspace,
            "unknown-thread",
            "unknown-turn",
            "recover me",
        )
        .await;
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace.clone(),
                    thread_id: "unknown-thread".into(),
                    turn_id: "unknown-turn".into(),
                    item: web_fetch_item("unknown-item"),
                },
                now,
            )
            .await
            .unwrap();
        let job = store
            .enqueue_recovery_job(
                "unknown-turn".into(),
                "unknown-item".into(),
                TurnItemType::WebFetch,
                None,
                RecoveryTrigger::Timeout,
                RecoveryAction::RetryWithBackoff,
                Some("tool timed out".into()),
                None,
                None,
                Some(60_000),
                1,
                2,
                serde_json::json!({}),
                serde_json::json!({}),
                now,
            )
            .await
            .unwrap();
        assert!(
            store
                .mark_recovery_job_terminal(
                    &job.id,
                    RecoveryJobStatus::Exhausted,
                    Some("recovery budget exhausted".into()),
                    now,
                )
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .get_turn("unknown-thread", "unknown-turn")
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::InProgress
        );
        use sea_orm::EntityTrait;
        let pending =
            pioneer_entity::recovery_terminalization_outbox::Entity::find_by_id(job.id.clone())
                .one(&store.database_connection())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(pending.status, "pending");
        processor.release_completed_history_preparation_barrier();
        loop {
            let (_, turn) = store
                .get_turn("unknown-thread", "unknown-turn")
                .await
                .unwrap()
                .unwrap();
            if turn.status == TurnStatus::Failed {
                break;
            }
            tokio::task::yield_now().await;
        }
        let outbox = pioneer_entity::recovery_terminalization_outbox::Entity::find_by_id(job.id)
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outbox.status, "delivered");
        assert!(outbox.delivered_at.is_some());
        assert_eq!(
            tokio::time::Instant::now(),
            started,
            "advisory failure must not put late terminalization behind the 60-second backoff"
        );
        assert_eq!(
            *errors.0.lock().unwrap(),
            vec!["recovery admission"],
            "the admission diagnostic survives while authoritative phases stay healthy"
        );
        processor.shutdown_resilience_workers().await;
    };
    tokio::pin!(scenario);
    // SQLite uses real worker IO. Keep this current-thread runtime runnable so
    // paused time cannot auto-advance the old 60-second sleep into a false pass.
    let wall = std::time::Instant::now();
    loop {
        if futures_util::poll!(&mut scenario).is_ready() {
            break;
        }
        assert!(
            wall.elapsed() < Duration::from_secs(10),
            "late authoritative terminalization must finish without advancing the backoff"
        );
        tokio::task::yield_now().await;
    }
    worker.await.unwrap();
    tokio::time::resume();
}

#[tokio::test]
async fn timeout_owner_dormant_reader_and_lower_future_attempt_reaches_authoritative_outcome() {
    let observer = Arc::new(NativeSchedulingObserver::default());
    let (_directory, manager, store, workspace) =
        setup_pooled_file_workspace_manager_with_observers(
            Some(observer.clone()),
            Some(observer.clone()),
        )
        .await;
    let processor = Arc::new(MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        test_provider(),
        Arc::new(SessionManager::new()),
        manager,
        store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    ));
    let label = "__timeout_owner_settled__";
    processor.arm_completed_history_preparation_barrier(label);
    let worker = tokio::spawn(MessageProcessor::run_execution_timeout_worker(
        Arc::downgrade(&processor),
    ));
    timeout(
        Duration::from_secs(5),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert!(
        observer.writes.lock().unwrap().is_empty(),
        "empty timeout observation cannot admit writer"
    );
    processor.release_completed_history_preparation_barrier();
    tokio::task::yield_now().await;
    observer.reads.lock().unwrap().clear();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    tokio::task::yield_now().await;
    assert!(
        observer.reads.lock().unwrap().is_empty(),
        "successful Empty has no two-second scan"
    );
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();
    let lower = CrudStore::new(store.database_connection());
    processor.arm_completed_history_preparation_barrier(label);
    materialize_cli_runtime_turn_with_text(
        &lower,
        &workspace,
        "timeout-clock",
        "timeout-clock",
        "future timeout",
    )
    .await;
    timeout(
        Duration::from_secs(5),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    let now = now_timestamp_secs();
    processor.arm_completed_history_preparation_barrier(label);
    lower
        .materialize_item_started_with_attempt_deadlines(
            ItemStartedNotification {
                workspace_id: workspace,
                thread_id: "timeout-clock".into(),
                turn_id: "timeout-clock".into(),
                item: web_fetch_item("timed-fetch"),
            },
            now,
            TurnItemAttemptDeadlines {
                lease_expires_at_unix: Some(now + 60),
                idle_deadline_at_unix: Some(now + 60),
                hard_deadline_at_unix: Some(now + 3),
            },
        )
        .await
        .unwrap();
    // Restored legacy/terminal history is excluded by the point predicates,
    // but must consume a bounded raw page for BOTH supervision consumers.
    use pioneer_entity::turn_item_attempt as attempt;
    use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set};
    let db = lower.database_connection();
    let source = attempt::Entity::find()
        .filter(attempt::Column::TurnId.eq("timeout-clock"))
        .filter(attempt::Column::ItemId.eq("timed-fetch"))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    for index in 0..65 {
        let mut historical: attempt::ActiveModel = source.clone().into();
        historical.id = Set(format!("!timeout-history-{index:03}"));
        historical.item_id = Set(format!("history-{index:03}"));
        if index % 2 == 0 {
            historical.status = Set("completed".into());
        } else {
            historical.execution_class = Set(None);
        }
        historical.insert(&db).await.unwrap();
    }
    let upper = lower.timeout_supervision_upper().await.unwrap().unwrap();
    let page = lower
        .timeout_supervision_page(None, &upper, now)
        .await
        .unwrap();
    assert_eq!(page.after.as_deref(), Some("!timeout-history-063"));
    assert!(
        page.rows.is_empty(),
        "excluded raw keys advance without timeout authority"
    );
    let upper = lower.command_supervision_upper().await.unwrap().unwrap();
    let (after, rows) = lower.command_supervision_page(None, &upper).await.unwrap();
    assert_eq!(after.as_deref(), Some("!timeout-history-063"));
    assert!(
        rows.is_empty(),
        "command type is revalidated after the raw page"
    );
    // The real lower commit happened while the owner was in its Empty
    // handoff. No manual dirty flag or synthetic dispatcher supplies the wake.
    processor.release_completed_history_preparation_barrier();
    timeout(
        Duration::from_secs(5),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert!(
        lower
            .get_running_turn_item_attempt("timeout-clock", "timed-fetch")
            .await
            .unwrap()
            .is_some(),
        "future deadline is not a timeout claim"
    );
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    // Re-arm while the prior barrier owns the worker. Only a real wall-clock
    // deadline, with no client or later commit, can complete the next outcome.
    processor.arm_completed_history_preparation_barrier(label);
    processor.release_completed_history_preparation_barrier();
    timeout(
        Duration::from_secs(5),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert!(observer.reads.lock().unwrap().iter().all(|event| !matches!(event,
        pioneer_sqlite::SqliteReadEvent::AdmissionEnqueued { class, .. } if *class != pioneer_sqlite::SqliteReadClass::Maintenance)));
    assert!(
        lower
            .get_running_turn_item_attempt("timeout-clock", "timed-fetch")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        lower
            .count_recovery_jobs_for_turn("timeout-clock")
            .await
            .unwrap()
            > 0,
        "timeout reached durable recovery authority"
    );
    assert!(observer.writes.lock().unwrap().iter().any(|event| matches!(
        event,
        pioneer_sqlite::SqliteWriteEvent::Acquired {
            class: pioneer_sqlite::SqliteWriteClass::Critical,
            ..
        }
    )));

    processor.release_completed_history_preparation_barrier();
    tokio::task::yield_now().await;
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    tokio::task::yield_now().await;
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    processor.cli_history_shutdown.cancel();
    worker.await.unwrap();
}

#[tokio::test]
async fn timeout_owner_point_decode_retry_survives_rollback_and_keeps_wall_forward_deadline() {
    use pioneer_entity::turn_item_attempt as attempt;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, sea_query::Expr};
    for forward in [false, true] {
        let observer = Arc::new(NativeSchedulingObserver::default());
        let (_directory, manager, store, workspace) =
            setup_pooled_file_workspace_manager_with_observers(
                Some(observer.clone()),
                Some(observer.clone()),
            )
            .await;
        let processor = Arc::new(MessageProcessor::new(
            Arc::new(ThreadManager::new("test-model", "openai")),
            test_provider(),
            Arc::new(SessionManager::new()),
            manager,
            store.clone(),
            test_gateway_secrets(),
            test_summary_config(),
            test_tool_loop_config(),
        ));
        let lower = CrudStore::new(store.database_connection());
        let now = now_timestamp_secs();
        for (turn, hard) in [("timeout-point", now - 7200), ("timeout-wall", now + 100)] {
            materialize_cli_runtime_turn_with_text(&lower, &workspace, turn, turn, "point fault")
                .await;
            lower
                .materialize_item_started_with_attempt_deadlines(
                    ItemStartedNotification {
                        workspace_id: workspace.clone(),
                        thread_id: turn.into(),
                        turn_id: turn.into(),
                        item: web_fetch_item("timed-fetch"),
                    },
                    now - 7300,
                    TurnItemAttemptDeadlines {
                        lease_expires_at_unix: Some(now + 1000),
                        idle_deadline_at_unix: Some(now + 1000),
                        hard_deadline_at_unix: Some(hard),
                    },
                )
                .await
                .unwrap();
        }
        // These fixtures restore old hard deadlines after initial text activity.
        // A current heartbeat keeps that earlier text from legitimately
        // suppressing recovery as post-attempt progress.
        for turn in ["timeout-point", "timeout-wall"] {
            assert!(
                lower
                    .heartbeat_turn_item_attempt(
                        turn,
                        "timed-fetch",
                        TurnItemType::WebFetch,
                        now_timestamp_secs(),
                        Some(now + 1000),
                        Some(now + 1000),
                        None,
                    )
                    .await
                    .unwrap()
            );
        }
        assert!(
            lower
                .get_running_turn_item_attempt("timeout-point", "timed-fetch")
                .await
                .unwrap()
                .is_some()
        );
        let db = lower.database_connection();
        // An actual ORM point-read decode failure in an otherwise successful
        // raw page. No fake page result or mutation/Unknown supplies retry debt.
        attempt::Entity::update_many()
            .col_expr(attempt::Column::StartedAt, Expr::value("invalid-date"))
            .filter(attempt::Column::TurnId.eq("timeout-point"))
            .filter(attempt::Column::ItemId.eq("timed-fetch"))
            .exec(&db)
            .await
            .unwrap();
        let wake = lower.background_work("recovery.timeouts");
        wake.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
        let label = "__timeout_owner_settled__";
        processor.arm_completed_history_preparation_barrier(label);
        let worker = tokio::spawn(MessageProcessor::run_execution_timeout_worker(
            Arc::downgrade(&processor),
        ));
        timeout(
            Duration::from_secs(5),
            processor.wait_for_completed_history_preparation_barrier(),
        )
        .await
        .unwrap();
        // Repair storage while the successful-page retry publication is held.
        // This direct fixture repair emits no producer wake.
        attempt::Entity::update_many()
            .col_expr(
                attempt::Column::StartedAt,
                Expr::value(pioneer_crud::unix_to_datetime(now - 7300)),
            )
            .filter(attempt::Column::TurnId.eq("timeout-point"))
            .filter(attempt::Column::ItemId.eq("timed-fetch"))
            .exec(&db)
            .await
            .unwrap();
        assert!(
            lower
                .get_running_turn_item_attempt("timeout-point", "timed-fetch")
                .await
                .unwrap()
                .is_some()
        );
        wake.set_wall_clock_for_test(
            std::time::UNIX_EPOCH
                + Duration::from_secs((if forward { now + 200 } else { now - 3600 }) as u64),
        );
        // Keep paused-time auto-advance from racing the publication barrier.
        let ticker = tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });
        tokio::time::pause();
        processor.arm_completed_history_preparation_barrier(label);
        processor.release_completed_history_preparation_barrier();
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::time::resume();
        timeout(
            Duration::from_secs(5),
            processor.wait_for_completed_history_preparation_barrier(),
        )
        .await
        .unwrap();
        assert!(
            lower
                .get_running_turn_item_attempt("timeout-point", "timed-fetch")
                .await
                .unwrap()
                .is_none(),
            "recovered point read reaches real timeout authority after a monotonic retry"
        );
        assert!(
            lower
                .count_recovery_jobs_for_turn("timeout-point")
                .await
                .unwrap()
                > 0
        );
        assert_eq!(
            lower
                .get_running_turn_item_attempt("timeout-wall", "timed-fetch")
                .await
                .unwrap()
                .is_none(),
            forward,
            "independent genuine wall eligibility is retained"
        );
        processor.cli_history_shutdown.cancel();
        processor.release_completed_history_preparation_barrier();
        worker.await.unwrap();
        ticker.abort();
    }
}

#[tokio::test]
async fn recovery_due_clock_owner_retains_max_claim_retry_and_wakes_an_earlier_old_key() {
    let observer = Arc::new(NativeSchedulingObserver::default());
    let (_directory, manager, store, workspace) =
        setup_pooled_file_workspace_manager_with_observers(
            Some(observer.clone()),
            Some(observer.clone()),
        )
        .await;
    let processor = Arc::new(MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        test_provider(),
        Arc::new(SessionManager::new()),
        manager,
        store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    ));
    let now = 1_800_000_000i64;
    let wall = store.background_work("recovery.due");
    wall.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
    for index in 0..129 {
        let turn = format!("finite-recovery-turn-{index}");
        materialize_cli_runtime_turn_with_text(
            &store,
            &workspace,
            &format!("finite-recovery-thread-{index}"),
            &turn,
            "input",
        )
        .await;
        let item = format!("finite-recovery-item-{index}");
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace.clone(),
                    thread_id: format!("finite-recovery-thread-{index}"),
                    turn_id: turn.clone(),
                    item: web_fetch_item(&item),
                },
                now,
            )
            .await
            .unwrap();
        store
            .enqueue_recovery_job(
                turn,
                item,
                TurnItemType::WebFetch,
                None,
                RecoveryTrigger::Timeout,
                RecoveryAction::MarkFailed,
                None,
                None,
                None,
                Some(0),
                0,
                0,
                serde_json::json!({}),
                serde_json::json!({}),
                now,
            )
            .await
            .unwrap();
    }
    let mut claims = store.claim_due_recovery_jobs(now, 105, 129).await.unwrap();
    assert_eq!(claims.len(), 129);
    assert!(claims.iter().all(|job| job.next_run_at_unix <= now));
    // Every excluded prefix row is retry-due NOW, but leased until105.
    // A limit on filtered matches cannot establish a finite physical quantum.
    claims.sort_by(|a, b| a.id.cmp(&b.id));
    let tail = claims.last().unwrap().clone();
    assert!(
        store
            .release_claimed_recovery_job(
                &tail.id,
                tail.claim_token.as_deref().unwrap(),
                now,
                None,
                now
            )
            .await
            .unwrap()
    );
    let label = "__recovery_clock_settled__";
    processor.arm_completed_history_preparation_barrier(label);
    let worker = tokio::spawn(MessageProcessor::run_recovery_due_worker(Arc::downgrade(
        &processor,
    )));
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .get_recovery_job(&tail.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RecoveryJobStatus::Failed
    );
    assert_eq!(
        store.next_recovery_job_deadline().await.unwrap(),
        Some(pioneer_crud::unix_to_datetime(now + 105)),
        "per-row claim dominates retry"
    );
    let reads = || observer.reads.lock().unwrap().len();
    let settled = reads();
    processor.release_completed_history_preparation_barrier();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        reads(),
        settled,
        "future claimed population has no periodic SQL"
    );
    tokio::time::resume();
    let old = claims.first().unwrap();
    processor.arm_completed_history_preparation_barrier(label);
    assert!(
        store
            .release_claimed_recovery_job(
                &old.id,
                old.claim_token.as_deref().unwrap(),
                now + 2,
                None,
                now
            )
            .await
            .unwrap()
    );
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .get_recovery_job(&old.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RecoveryJobStatus::Pending
    );
    assert_eq!(
        store.next_recovery_job_deadline().await.unwrap(),
        Some(pioneer_crud::unix_to_datetime(now + 2)),
        "old-key reschedule moves nearest earlier"
    );
    processor.release_completed_history_preparation_barrier();
    wall.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs((now + 1) as u64));
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        store
            .get_recovery_job(&old.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RecoveryJobStatus::Pending
    );
    processor.arm_completed_history_preparation_barrier(label);
    tokio::time::pause();
    wall.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs((now + 2) as u64));
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
        store
            .get_recovery_job(&old.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RecoveryJobStatus::Failed
    );
    processor.release_completed_history_preparation_barrier();
    // The untouched rows retain claim105 although their retry is already due. The
    // planner must not wake at the earlier component of the per-row maximum.
    let settled = reads();
    wall.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs((now + 104) as u64));
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(30)).await;
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert_eq!(reads(), settled, "claim expiry dominates earlier retry");
    tokio::time::resume();
    processor.arm_completed_history_preparation_barrier(label);
    tokio::time::pause();
    wall.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs((now + 105) as u64));
    // Exercise Wake clock sampling without a producer or real30s wait.
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::time::resume();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    for job in &claims[1..claims.len() - 1] {
        assert_eq!(
            store
                .get_recovery_job(&job.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            RecoveryJobStatus::Failed
        );
    }
    processor.release_completed_history_preparation_barrier();
    processor.cli_history_shutdown.cancel();
    worker.await.unwrap();
}

// F05-1: only the lower reactivation producer may reopen the parked owner.
#[tokio::test]
async fn parked_timeout_owner_reactivation_committed_noop_and_lost_commit_outcome() {
    use pioneer_entity::turn_item_attempt as attempt;
    use sea_orm::EntityTrait;
    use std::sync::{
        Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    };

    #[derive(Default)]
    struct CommitBarrier {
        armed: AtomicBool,
        entered: tokio::sync::Notify,
        released: Mutex<bool>,
        condition: Condvar,
    }
    struct ReleaseCommit(Arc<CommitBarrier>);
    impl Drop for ReleaseCommit {
        fn drop(&mut self) {
            *self.0.released.lock().unwrap() = true;
            self.0.condition.notify_all();
        }
    }
    for lost_outcome in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("reactivation.sqlite");
        let mut options =
            sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
        options.max_connections(1).sqlx_logging(false);
        let writer = Database::connect(options).await.unwrap();
        let (_, _, workspace) = setup_workspace_manager_with_connection(writer.clone()).await;
        pioneer_sqlite::SqliteWriteExecutor::new(writer.clone())
            .apply_pragmas(pioneer_sqlite::SqliteWriteClass::Maintenance)
            .await
            .unwrap();
        let pool = writer.get_sqlite_connection_pool().clone();
        let mut options =
            sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
        options.max_connections(4).map_sqlx_sqlite_opts(|options| {
            options
                .read_only(true)
                .create_if_missing(false)
                .pragma("query_only", "ON")
        });
        let reader = Database::connect(options).await.unwrap();
        let observer = Arc::new(NativeSchedulingObserver::default());
        let db = pioneer_sqlite::SqliteDatabase::from_executor_with_read_observer(
            reader,
            pioneer_sqlite::SqliteWriteExecutor::with_observer(writer, observer.clone()),
            observer.clone(),
        );
        let store = Arc::new(CrudStore::new(db.clone()));
        let lower = Arc::new(CrudStore::new(db.clone()));
        let now = now_timestamp_secs();
        materialize_cli_runtime_turn_with_text(
            &lower,
            &workspace,
            "reactivated",
            "reactivated",
            "timeout",
        )
        .await;
        lower
            .materialize_item_started_with_attempt_deadlines(
                ItemStartedNotification {
                    workspace_id: workspace,
                    thread_id: "reactivated".into(),
                    turn_id: "reactivated".into(),
                    item: web_fetch_item("fetch"),
                },
                now - 10,
                TurnItemAttemptDeadlines {
                    lease_expires_at_unix: Some(now - 1),
                    idle_deadline_at_unix: Some(now - 1),
                    hard_deadline_at_unix: Some(now - 1),
                },
            )
            .await
            .unwrap();
        let candidate = lower
            .list_timeout_candidates(now, 8)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(
            lower
                .transition_timeout_candidate(&candidate, now)
                .await
                .unwrap()
        );
        let processor = Arc::new(MessageProcessor::new(
            Arc::new(ThreadManager::new("test-model", "openai")),
            test_provider(),
            Arc::new(SessionManager::new()),
            Arc::new(WorkspaceManager::new(db.clone())),
            store,
            test_gateway_secrets(),
            test_summary_config(),
            test_tool_loop_config(),
        ));
        let wake = lower.background_work("recovery.timeouts");
        wake.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
        let label = "__timeout_owner_settled__";
        processor.arm_completed_history_preparation_barrier(label);
        let worker = tokio::spawn(MessageProcessor::run_execution_timeout_worker(
            Arc::downgrade(&processor),
        ));
        timeout(
            Duration::from_secs(5),
            processor.wait_for_completed_history_preparation_barrier(),
        )
        .await
        .unwrap();
        processor.release_completed_history_preparation_barrier();
        // A ready ticker prevents paused-time auto-advance past our assertions.
        let ticker = tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });
        tokio::time::pause();
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        tokio::time::advance(Duration::from_secs(300)).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert!(observer.reads.lock().unwrap().is_empty());
        assert!(observer.writes.lock().unwrap().is_empty());
        let deadlines = TurnItemAttemptDeadlines {
            lease_expires_at_unix: Some(now + 100),
            idle_deadline_at_unix: Some(now + 100),
            hard_deadline_at_unix: Some(now + 2),
        };
        // A known no-op discharges its guard, and cannot leave unknown debt.
        assert!(
            !lower
                .reactivate_timed_out_attempt("absent", now, deadlines.clone())
                .await
                .unwrap()
        );
        assert!(!wake.requires_settlement());
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        tokio::time::advance(Duration::from_secs(300)).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert!(
            observer.reads.lock().unwrap().is_empty(),
            "no-op must remain dormant"
        );
        assert!(observer.writes.lock().unwrap().is_empty());
        tokio::time::resume();
        processor.arm_completed_history_preparation_barrier(label);
        if lost_outcome {
            let barrier = Arc::new(CommitBarrier::default());
            let release = ReleaseCommit(barrier.clone());
            {
                let mut connection = pool.acquire().await.unwrap();
                let mut handle = connection.lock_handle().await.unwrap();
                let barrier = barrier.clone();
                // Same physical sent-COMMIT seam as cancelled_sent_acquisition_commit_settles_before_owner_can_park.
                handle.set_commit_hook(move || {
                    if barrier.armed.swap(false, Ordering::SeqCst) {
                        barrier.entered.notify_one();
                        let mut released = barrier.released.lock().unwrap();
                        while !*released {
                            released = barrier.condition.wait(released).unwrap();
                        }
                    }
                    true
                });
            }
            barrier.armed.store(true, Ordering::SeqCst);
            let mutation_store = lower.clone();
            let id = candidate.attempt_id.clone();
            let mutation = tokio::spawn(async move {
                mutation_store
                    .reactivate_timed_out_attempt(&id, now, deadlines)
                    .await
            });
            timeout(Duration::from_secs(5), barrier.entered.notified())
                .await
                .unwrap();
            mutation.abort();
            assert!(mutation.await.unwrap_err().is_cancelled());
            assert!(
                wake.requires_settlement(),
                "sent COMMIT without Rust reply retains Unknown"
            );
            let before = attempt::Entity::find_by_id(candidate.attempt_id.clone())
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                before.status, "timed_out",
                "reader cannot yet see the sent COMMIT"
            );
            drop(release);
        } else {
            assert!(
                lower
                    .reactivate_timed_out_attempt(&candidate.attempt_id, now, deadlines)
                    .await
                    .unwrap()
            );
        }
        // No heartbeat, observation mutation, manual invalidate or synthetic kick.
        timeout(
            Duration::from_secs(5),
            processor.wait_for_completed_history_preparation_barrier(),
        )
        .await
        .unwrap();
        assert!(!wake.requires_settlement());
        let running = lower
            .get_running_turn_item_attempt("reactivated", "fetch")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(running.turn_id, "reactivated");
        let restored = attempt::Entity::find_by_id(candidate.attempt_id.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restored.status, "running");
        assert_eq!(restored.hard_deadline_at.unwrap().timestamp(), now + 2);
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        tokio::time::pause();
        processor.arm_completed_history_preparation_barrier(label);
        processor.release_completed_history_preparation_barrier();
        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert!(
            observer.reads.lock().unwrap().is_empty(),
            "future timeout must remain SQL-free"
        );
        assert!(observer.writes.lock().unwrap().is_empty());
        wake.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs((now + 3) as u64));
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::time::resume();
        timeout(
            Duration::from_secs(5),
            processor.wait_for_completed_history_preparation_barrier(),
        )
        .await
        .unwrap();
        assert!(
            observer
                .reads
                .lock()
                .unwrap()
                .iter()
                .all(|event| !matches!(event,
                    pioneer_sqlite::SqliteReadEvent::AdmissionEnqueued { class, .. }
                        if *class != pioneer_sqlite::SqliteReadClass::Maintenance
                ))
        );
        assert!(
            lower
                .get_running_turn_item_attempt("reactivated", "fetch")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            lower
                .count_recovery_jobs_for_turn("reactivated")
                .await
                .unwrap()
                > 0
        );
        assert!(observer.writes.lock().unwrap().iter().any(|event| matches!(
            event,
            pioneer_sqlite::SqliteWriteEvent::Acquired {
                class: pioneer_sqlite::SqliteWriteClass::Critical,
                ..
            }
        )));
        processor.cli_history_shutdown.cancel();
        processor.release_completed_history_preparation_barrier();
        worker.await.unwrap();
        ticker.abort();
    }
}

// Added to message/tests/reconciliation_workers.rs: exercise the actual control
// owner, including its settled publication boundary and physical observer.
#[tokio::test]
async fn control_orphan_raw_history_and_point_retry_survive_wall_rollback() {
    use pioneer_entity::turn_execution as execution;
    use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set};
    // Each error lane is tested separately so the other lane's genuine wall
    // deadline must survive the failing lane's monotonic retry budget.
    // The third case fails earlier than liveness observation: typed job
    // hydration itself must not discard a healthy raw page or its frontier.
    for (active_fault, decode_fault) in [(false, false), (true, false), (true, true)] {
        let observer = Arc::new(NativeSchedulingObserver::default());
        let (_dir, manager, store, workspace) = setup_pooled_file_workspace_manager_with_observers(
            Some(observer.clone()),
            Some(observer.clone()),
        )
        .await;
        let processor = Arc::new(MessageProcessor::new(
            Arc::new(ThreadManager::new("test-model", "openai")),
            test_provider(),
            Arc::new(SessionManager::new()),
            manager,
            store.clone(),
            test_gateway_secrets(),
            test_summary_config(),
            test_tool_loop_config(),
        ));
        let now = 1_800_000_000i64;
        let wake = store.background_work("recovery.control");
        wake.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
        // Two pages excluded by current-owner/terminal policy. Raw traversal must
        // still advance; no filtered MAX/PK-order scan is an acceptable oracle.
        for index in 0..130 {
            execution::ActiveModel {
                turn_id: Set(format!("a-raw-orphan-{index:03}")),
                thread_id: Set("excluded-thread".into()),
                workspace_id: Set(workspace.clone()),
                executor_kind: Set("cli_runtime".into()),
                executor_key: Set(None),
                // F06-2: more than a full page of active expired rows
                // excluded solely by current owner, ahead of foreign future tail.
                status: Set("starting".into()),
                owner_id: Set(processor.turn_execution_owner_id.to_string()),
                owner_generation: Set(1),
                lease_until: Set(pioneer_crud::unix_to_datetime(now - 20)),
                heartbeat_at: Set(pioneer_crud::unix_to_datetime(now - 30)),
                started_at: Set(None),
                completed_at: Set(None),
                created_at: Set(pioneer_crud::unix_to_datetime(now - 30)),
                updated_at: Set(pioneer_crud::unix_to_datetime(now - 30)),
            }
            .insert(&store.database_connection())
            .await
            .unwrap();
        }
        // All-excluded EOF must park in the real owner as well as advancing its
        // raw cursor. No mutation or retry budget exists in this first stage.
        let cold_cancel = tokio_util::sync::CancellationToken::new();
        processor.arm_completed_history_preparation_barrier("__control_owner_settled__");
        let cold_worker = tokio::spawn(MessageProcessor::run_recovery_control_worker(
            Arc::downgrade(&processor),
            cold_cancel.clone(),
        ));
        timeout(
            Duration::from_secs(5),
            processor.wait_for_completed_history_preparation_barrier(),
        )
        .await
        .unwrap();
        processor.release_completed_history_preparation_barrier();
        tokio::task::yield_now().await;
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        let ticker = tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(300)).await;
        tokio::task::yield_now().await;
        assert!(observer.reads.lock().unwrap().is_empty());
        assert!(observer.writes.lock().unwrap().is_empty());
        tokio::time::resume();
        cold_cancel.cancel();
        cold_worker.await.unwrap();
        for (id, foreign, poison) in [
            ("m-owned-point", false, !active_fault),
            ("z-foreign-tail", true, false),
            ("zz-future", true, false),
        ] {
            materialize_cli_runtime_turn_with_text(
                &store,
                &workspace,
                &format!("thread-{id}"),
                id,
                "input",
            )
            .await;
            execution::ActiveModel {
                turn_id: Set(id.into()),
                thread_id: Set(format!("thread-{id}")),
                workspace_id: Set(workspace.clone()),
                executor_kind: Set("cli_runtime".into()),
                executor_key: Set(None),
                status: Set(if foreign { "starting" } else { "recovering" }.into()),
                owner_id: Set(if foreign {
                    "replaced-owner".into()
                } else {
                    processor.turn_execution_owner_id.to_string()
                }),
                owner_generation: Set(1),
                lease_until: Set(if id == "zz-future" {
                    pioneer_crud::unix_to_datetime(now + 10) + chrono::Duration::nanoseconds(1)
                } else {
                    pioneer_crud::unix_to_datetime(now - 7200)
                }),
                heartbeat_at: Set(pioneer_crud::unix_to_datetime(now - 7201)),
                started_at: Set(None),
                completed_at: Set(None),
                created_at: Set(pioneer_crud::unix_to_datetime(now - 7201)),
                updated_at: Set(pioneer_crud::unix_to_datetime(now - 7201)),
            }
            .insert(&store.database_connection())
            .await
            .unwrap();
            if poison {
                execution::Entity::update_many()
                    .col_expr(
                        execution::Column::CreatedAt,
                        sea_orm::sea_query::Expr::val("invalid-point-timestamp"),
                    )
                    .filter(execution::Column::TurnId.eq(id))
                    .exec(&store.database_connection())
                    .await
                    .unwrap();
            }
        }
        // Raw APIs expose the excluded first page as progress, with no matches.
        let upper = store
            .owned_recovering_execution_upper(processor.turn_execution_owner_id.as_ref())
            .await
            .unwrap()
            .unwrap();
        let (last, rows) = store
            .owned_recovering_execution_page(
                processor.turn_execution_owner_id.as_ref(),
                None,
                &upper,
                64,
            )
            .await
            .unwrap();
        assert_eq!(last.as_deref(), Some("a-raw-orphan-063"));
        assert!(rows.is_empty());
        // Existing active-point coverage corrupts liveness after job hydration;
        // decode_fault instead corrupts ScheduledAt after choosing actual keys.
        let active_started = if active_fault { now - 8000 } else { now - 59 };
        let active_turn = "active-point-fault";
        materialize_cli_runtime_turn_with_text(
            &store,
            &workspace,
            "active-point-thread",
            active_turn,
            "input",
        )
        .await;
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace.clone(),
                    thread_id: "active-point-thread".into(),
                    turn_id: active_turn.into(),
                    item: web_fetch_item("active-point-item"),
                },
                active_started,
            )
            .await
            .unwrap();
        let mut active_job = store
            .enqueue_recovery_job(
                active_turn.into(),
                "active-point-item".into(),
                TurnItemType::WebFetch,
                None,
                RecoveryTrigger::Timeout,
                RecoveryAction::RetryWithBackoff,
                Some("expired".into()),
                None,
                None,
                None,
                1,
                2,
                serde_json::json!({}),
                serde_json::json!({}),
                active_started,
            )
            .await
            .unwrap();
        let claimed = store
            .claim_due_recovery_job_candidate(active_started, 45, &active_job.id)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert!(matches!(
            store
                .mark_claimed_recovery_job_active(
                    &active_job.id,
                    claimed[0].claim_token.as_deref().unwrap(),
                    "active-point-attempt",
                    active_started
                )
                .await
                .unwrap(),
            pioneer_crud::ClaimedRecoveryActivation::Activated
        ));
        let mut active_jobs = vec![active_job.clone()];
        let other_started = if decode_fault {
            active_started
        } else {
            now + 2000
        };
        // Real active jobs: each owns its own persisted Turn, item,
        // liveness, job claim and active-attempt identity. No cloned receipts.
        for index in 0..65 {
            let turn_id = format!("future-active-{index}");
            let thread_id = format!("future-active-thread-{index}");
            let item_id = format!("future-active-item-{index}");
            materialize_cli_runtime_turn_with_text(
                &store, &workspace, &thread_id, &turn_id, "future",
            )
            .await;
            store
                .materialize_item_started(
                    ItemStartedNotification {
                        workspace_id: workspace.clone(),
                        thread_id,
                        turn_id: turn_id.clone(),
                        item: web_fetch_item(&item_id),
                    },
                    other_started,
                )
                .await
                .unwrap();
            let job = store
                .enqueue_recovery_job(
                    turn_id.clone(),
                    item_id,
                    TurnItemType::WebFetch,
                    None,
                    RecoveryTrigger::Timeout,
                    RecoveryAction::RetryWithBackoff,
                    Some("future supervision budget".into()),
                    None,
                    None,
                    None,
                    1,
                    2,
                    serde_json::json!({}),
                    serde_json::json!({}),
                    other_started,
                )
                .await
                .unwrap();
            let claim = store
                .claim_due_recovery_job_candidate(other_started, 45, &job.id)
                .await
                .unwrap();
            assert_eq!(claim.len(), 1);
            let attempt_id = format!("future-active-attempt-{index}");
            assert!(matches!(
                store
                    .mark_claimed_recovery_job_active(
                        &job.id,
                        claim[0].claim_token.as_deref().unwrap(),
                        &attempt_id,
                        other_started
                    )
                    .await
                    .unwrap(),
                pioneer_crud::ClaimedRecoveryActivation::Activated
            ));
            let persisted = store.get_recovery_job(&job.id).await.unwrap().unwrap();
            assert_eq!(persisted.status, RecoveryJobStatus::Active);
            assert_eq!(
                persisted.active_attempt_id.as_deref(),
                Some(attempt_id.as_str())
            );
            assert_eq!(
                persisted.active_attempt_started_at_unix,
                Some(other_started)
            );
            assert!(store.get_turn_liveness(&turn_id).await.unwrap().is_some());
            active_jobs.push(persisted);
        }
        use pioneer_entity::turn_liveness as liveness;
        let healthy_decode_ids = if decode_fault {
            active_jobs.sort_by(|a, b| a.id.cmp(&b.id));
            assert!(active_jobs.len() > 64);
            active_job = active_jobs[0].clone();
            // Only a typed DateTime is corrupt; raw ID/status selection and all
            // real claim/attempt identities remain intact. Use actual PK order.
            pioneer_entity::recovery_job::Entity::update_many()
                .col_expr(
                    pioneer_entity::recovery_job::Column::ScheduledAt,
                    sea_orm::sea_query::Expr::val("malformed-date"),
                )
                .filter(pioneer_entity::recovery_job::Column::Id.eq(&active_job.id))
                .exec(&store.database_connection())
                .await
                .unwrap();
            assert!(store.get_recovery_job(&active_job.id).await.is_err());
            Some((
                active_jobs[1].id.clone(),
                active_jobs.last().unwrap().id.clone(),
            ))
        } else {
            None
        };
        if active_fault && !decode_fault {
            liveness::Entity::update_many()
                .col_expr(
                    liveness::Column::LastActivityAt,
                    sea_orm::sea_query::Expr::val("malformed-date"),
                )
                .filter(liveness::Column::TurnId.eq(active_turn))
                .exec(&store.database_connection())
                .await
                .unwrap();
        }
        let cancellation = tokio_util::sync::CancellationToken::new();
        processor.arm_completed_history_preparation_barrier("__control_owner_settled__");
        let worker = tokio::spawn(MessageProcessor::run_recovery_control_worker(
            Arc::downgrade(&processor),
            cancellation.clone(),
        ));
        // Time remains resumed during all bounded DB/barrier rendezvous.
        // Error publication can precede EOF. Finish the healthy tail before repair;
        // the real conditional takeover must happen despite the owned point fault.
        let mut tail_complete = false;
        for _ in 0..6 {
            timeout(
                Duration::from_secs(5),
                processor.wait_for_completed_history_preparation_barrier(),
            )
            .await
            .unwrap();
            if !processor.recovery_coordinator.orphan_supervision_more()
                && !processor
                    .recovery_coordinator
                    .active_supervision_schedule()
                    .0
            {
                tail_complete = true;
                break;
            }
            processor.release_completed_history_preparation_barrier();
            processor.arm_completed_history_preparation_barrier("__control_owner_settled__");
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(2)).await;
            tokio::time::resume();
        }
        assert!(
            tail_complete,
            "raw healthy tail must finish within six finite quanta"
        );
        if let Some((same_page, later_page)) = &healthy_decode_ids {
            for id in [same_page, later_page] {
                let job = store.get_recovery_job(id).await.unwrap().unwrap();
                assert_eq!(job.status, RecoveryJobStatus::Exhausted);
                assert!(
                    job.last_error
                        .as_deref()
                        .unwrap()
                        .contains("wall-clock budget")
                );
            }
            // The malformed job remains present and unacknowledged at EOF.
            // Its decode error must still own the monotonic retry below.
            use sea_orm::QuerySelect;
            let status = pioneer_entity::recovery_job::Entity::find_by_id(&active_job.id)
                .select_only()
                .column(pioneer_entity::recovery_job::Column::Status)
                .into_tuple::<String>()
                .one(&store.database_connection())
                .await
                .unwrap();
            assert_eq!(status.as_deref(), Some("active"));
            assert!(store.get_recovery_job(&active_job.id).await.is_err());
        }
        // This is the completed finite EOF publication with actual error debt.
        // Keep the owner at that publication while repairing the two points.
        let retry = processor
            .recovery_coordinator
            .supervision_retry_deadline()
            .expect("actual point-error debt");
        assert!(
            retry > tokio::time::Instant::now() + Duration::from_secs(5),
            "fixture must repair before the authoritative cooldown has elapsed"
        );
        let independent_wall = if active_fault {
            processor.recovery_coordinator.orphan_supervision_deadline()
        } else {
            processor
                .recovery_coordinator
                .active_supervision_schedule()
                .1
        };
        assert_eq!(
            independent_wall,
            Some(if active_fault { now + 11 } else { now + 2 }),
            "the healthy lane's earlier real wall obligation survives the other lane's retry"
        );
        assert_eq!(
            store
                .get_turn_execution("z-foreign-tail")
                .await
                .unwrap()
                .unwrap()
                .owner_generation,
            2
        );
        assert!(
            store
                .count_recovery_jobs_for_turn("z-foreign-tail")
                .await
                .unwrap()
                > 0
        );
        // Repair is deliberately below the producer wrapper; neither a kick nor a
        // heartbeat nor a fresh generation is allowed to rescue this retry.
        let repair_observation = wake.observe();
        if decode_fault {
            pioneer_entity::recovery_job::Entity::update_many()
                .col_expr(
                    pioneer_entity::recovery_job::Column::ScheduledAt,
                    sea_orm::sea_query::Expr::val(pioneer_crud::unix_to_datetime(active_started)),
                )
                .filter(pioneer_entity::recovery_job::Column::Id.eq(&active_job.id))
                .exec(&store.database_connection())
                .await
                .unwrap();
        }
        execution::Entity::update_many()
            .col_expr(
                execution::Column::CreatedAt,
                sea_orm::sea_query::Expr::val(pioneer_crud::unix_to_datetime(now - 7201)),
            )
            .filter(execution::Column::TurnId.eq("m-owned-point"))
            .exec(&store.database_connection())
            .await
            .unwrap();
        liveness::Entity::update_many()
            .col_expr(
                liveness::Column::LastActivityAt,
                sea_orm::sea_query::Expr::val(pioneer_crud::unix_to_datetime(active_started)),
            )
            .filter(liveness::Column::TurnId.eq(active_turn))
            .exec(&store.database_connection())
            .await
            .unwrap();
        assert_eq!(
            store
                .get_recovery_job(&active_job.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            RecoveryJobStatus::Active
        );
        assert!(
            retry > tokio::time::Instant::now(),
            "repair completed before retry became eligible"
        );
        assert_eq!(
            wake.observe(),
            repair_observation,
            "fixture repair publishes no producer wake"
        );
        wake.set_wall_clock_for_test(
            std::time::UNIX_EPOCH + Duration::from_secs((now - 3600) as u64),
        );
        processor.release_completed_history_preparation_barrier();
        processor.arm_completed_history_preparation_barrier("__control_owner_settled__");
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::time::resume();
        let mut repaired = false;
        for _ in 0..8 {
            timeout(
                Duration::from_secs(5),
                processor.wait_for_completed_history_preparation_barrier(),
            )
            .await
            .unwrap();
            if store
                .count_recovery_jobs_for_turn("m-owned-point")
                .await
                .unwrap()
                > 0
                && (if active_fault {
                    store
                        .get_recovery_job(&active_job.id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status
                        != RecoveryJobStatus::Active
                } else {
                    store
                        .get_recovery_job(&active_job.id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status
                        == RecoveryJobStatus::Active
                })
                && processor
                    .recovery_coordinator
                    .supervision_retry_deadline()
                    .is_none()
            {
                repaired = true;
                break;
            }
            processor.release_completed_history_preparation_barrier();
            processor.arm_completed_history_preparation_barrier("__control_owner_settled__");
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(2)).await;
            tokio::time::resume();
        }
        assert!(
            repaired,
            "both repaired point sources must recover within eight finite quanta"
        );
        if decode_fault {
            assert_eq!(
                store
                    .get_recovery_job(&active_job.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                RecoveryJobStatus::Exhausted,
                "the repaired job reaches the guarded watchdog outcome on its retained retry"
            );
        }
        // Change an old raw key after EOF, with a real lower semantic producer.
        // Its new owned-recovering obligation must not require a newly appended ID.
        let old = "a-raw-orphan-000";
        materialize_cli_runtime_turn_with_text(
            &store,
            &workspace,
            "excluded-thread",
            old,
            "old-key",
        )
        .await;
        execution::Entity::update_many()
            .col_expr(
                execution::Column::Status,
                sea_orm::sea_query::Expr::val("recovering"),
            )
            .filter(execution::Column::TurnId.eq(old))
            .exec(&store.database_connection())
            .await
            .unwrap();
        assert!(
            store
                .observe_turn_runtime_activity(old, "Runtime/activity", now + 1)
                .await
                .unwrap()
        );
        processor.release_completed_history_preparation_barrier();
        processor.arm_completed_history_preparation_barrier("__control_owner_settled__");
        let mut old_key_recovered = false;
        for _ in 0..8 {
            timeout(
                Duration::from_secs(5),
                processor.wait_for_completed_history_preparation_barrier(),
            )
            .await
            .unwrap();
            if store.count_recovery_jobs_for_turn(old).await.unwrap() > 0 {
                old_key_recovered = true;
                break;
            }
            processor.release_completed_history_preparation_barrier();
            processor.arm_completed_history_preparation_barrier("__control_owner_settled__");
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(2)).await;
            tokio::time::resume();
        }
        assert!(
            old_key_recovered,
            "old-key mutation must reopen the finite frontier"
        );
        // A fractional independent lease is still a wall obligation. Forward
        // movement is detected by SQL-free sampling, without any producer change.
        assert_eq!(
            store
                .get_turn_execution("zz-future")
                .await
                .unwrap()
                .unwrap()
                .owner_generation,
            1
        );
        wake.set_wall_clock_for_test(
            std::time::UNIX_EPOCH + Duration::from_secs((now + 11) as u64),
        );
        processor.release_completed_history_preparation_barrier();
        processor.arm_completed_history_preparation_barrier("__control_owner_settled__");
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::time::resume();
        let mut lease_recovered = false;
        for _ in 0..8 {
            timeout(
                Duration::from_secs(5),
                processor.wait_for_completed_history_preparation_barrier(),
            )
            .await
            .unwrap();
            if store
                .count_recovery_jobs_for_turn("zz-future")
                .await
                .unwrap()
                > 0
            {
                lease_recovered = true;
                break;
            }
            processor.release_completed_history_preparation_barrier();
            processor.arm_completed_history_preparation_barrier("__control_owner_settled__");
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(2)).await;
            tokio::time::resume();
        }
        assert!(
            lease_recovered,
            "fractional foreign lease must produce a durable takeover"
        );
        processor.release_completed_history_preparation_barrier();
        tokio::task::yield_now().await;
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(300)).await;
        tokio::task::yield_now().await;
        assert!(observer.reads.lock().unwrap().is_empty());
        assert!(observer.writes.lock().unwrap().is_empty());
        tokio::time::resume();
        cancellation.cancel();
        worker.await.unwrap();
        ticker.abort();
    }
}

// F07-2: the actual pooled control owner must traverse processed timeout history,
// retain failed point debt, and reopen an old attempt identity after EOF.
#[tokio::test]
async fn control_backfill_processed_history_tail_and_old_key_retry() {
    use pioneer_entity::turn_item_attempt as attempt;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, sea_query::Expr};

    let observer = Arc::new(NativeSchedulingObserver::default());
    let (_dir, manager, store, workspace) = setup_pooled_file_workspace_manager_with_observers(
        Some(observer.clone()),
        Some(observer.clone()),
    )
    .await;
    let processor = Arc::new(MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        test_provider(),
        Arc::new(SessionManager::new()),
        manager,
        store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    ));
    // Recovery terminalization requires the same durable Task cleanup adapter
    // installed during Gateway startup. Keep that authority in this fixture.
    processor.bind_task_bridge().await;
    // Keep source liveness behind the controlled eligibility clock even while
    // normal Turn materialization uses UTC during this large setup.
    let now = now_timestamp_secs() + 86_400;
    let wake = store.background_work("recovery.control");
    wake.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
    let db = store.database_connection();
    // This control-only matrix exercises terminal repair. Missing snapshots
    // intentionally choose BlockResumable and belong to the independent due
    // owner; declare a real non-retryable tool policy instead.
    let terminal_item = || {
        let mut item = web_fetch_item("fetch");
        if let TurnItem::WebFetch {
            recovery_policy, ..
        } = &mut item
        {
            *recovery_policy = Some(pioneer_protocol::ToolRecoveryPolicySnapshot {
                retry_class: pioneer_protocol::ToolRecoveryRetryClass::Never,
                idempotency_mode: pioneer_protocol::ToolRecoveryIdempotencyMode::None,
                max_attempts: 1,
                can_resume: false,
                resolved_action: pioneer_protocol::RecoveryAction::MarkFailed,
                base_backoff_secs: 1,
                max_wall_clock_secs: 240,
                no_progress_limit: 1,
            });
        }
        item
    };
    let mut processed = Vec::new();
    for index in 0..130 {
        let id = format!("a-backfill-{index:03}");
        materialize_cli_runtime_turn_with_text(&store, &workspace, &id, &id, "input").await;
        store
            .materialize_item_started_with_attempt_deadlines(
                ItemStartedNotification {
                    workspace_id: workspace.clone(),
                    thread_id: id.clone(),
                    turn_id: id.clone(),
                    item: terminal_item(),
                },
                now - 20,
                TurnItemAttemptDeadlines {
                    lease_expires_at_unix: Some(now - 1),
                    idle_deadline_at_unix: Some(now - 1),
                    hard_deadline_at_unix: Some(now - 1),
                },
            )
            .await
            .unwrap();
        let original = attempt::Entity::find()
            .filter(attempt::Column::TurnId.eq(&id))
            .filter(attempt::Column::Status.eq("running"))
            .filter(attempt::Column::ItemId.eq("fetch"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        // Fixture ordering only; each row still comes from normal materialization.
        attempt::Entity::update_many()
            .filter(attempt::Column::Id.eq(&original.id))
            .col_expr(attempt::Column::Id, Expr::value(id.clone()))
            .exec(&db)
            .await
            .unwrap();
        // Keep the existing item/attempt identity pair intact before its real
        // guarded timeout transition; only pre-owner fixture keys are ordered.
        pioneer_entity::turn_item::Entity::update_many()
            .filter(pioneer_entity::turn_item::Column::TurnId.eq(&id))
            .filter(pioneer_entity::turn_item::Column::ItemId.eq("fetch"))
            .filter(pioneer_entity::turn_item::Column::ActiveAttemptId.eq(&original.id))
            .col_expr(
                pioneer_entity::turn_item::Column::ActiveAttemptId,
                Expr::value(Some(id.clone())),
            )
            .exec(&db)
            .await
            .unwrap();
        let candidate = store
            .list_timeout_candidates(now, 8)
            .await
            .unwrap()
            .into_iter()
            .find(|row| row.attempt_id == id)
            .unwrap();
        assert!(
            store
                .transition_timeout_candidate(&candidate, now)
                .await
                .unwrap()
        );
        assert!(
            store
                .update_turn_status(&id, &id, TurnStatus::Completed, None, now)
                .await
                .unwrap()
        );
        assert!(
            processor
                .recovery_coordinator
                .suppress_timeout_recovery_if_turn_not_in_progress(&candidate, now)
                .await
                .unwrap()
        );
        let saved = attempt::Entity::find_by_id(id.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.status, "timed_out");
        assert!(saved.recovery_action.is_some());
        assert!(saved.execution_class.is_some());
        assert!(saved.recovery_suppressed_reason.is_none());
        processed.push(saved);
    }
    let cancel = tokio_util::sync::CancellationToken::new();
    let label = "__control_owner_settled__";
    processor.arm_completed_history_preparation_barrier(label);
    observer.writes.lock().unwrap().clear();
    let worker = tokio::spawn(MessageProcessor::run_recovery_control_worker(
        Arc::downgrade(&processor),
        cancel.clone(),
    ));
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert!(
        observer.writes.lock().unwrap().is_empty(),
        "all-processed bootstrap is read-only"
    );
    processor.release_completed_history_preparation_barrier();
    let ticker = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    tokio::time::pause();
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();

    cancel.cancel();
    worker.await.unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();

    // A new unqueued tail behind >2 pages of processed inputs. A poisoned early
    // identity has valid advisory predicates but fails the real Model mapper.
    let bad = &processed[0];
    assert!(
        store
            .update_turn_status(
                &bad.turn_id,
                &bad.turn_id,
                TurnStatus::InProgress,
                None,
                now
            )
            .await
            .unwrap()
    );
    let deadlines = TurnItemAttemptDeadlines {
        lease_expires_at_unix: Some(now - 1),
        idle_deadline_at_unix: Some(now - 1),
        hard_deadline_at_unix: Some(now - 1),
    };
    assert!(
        store
            .reactivate_timed_out_attempt(&bad.id, now - 20, deadlines.clone())
            .await
            .unwrap()
    );
    let candidate = store
        .list_timeout_candidates(now, 8)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.attempt_id == bad.id)
        .unwrap();
    assert!(
        store
            .transition_timeout_candidate(&candidate, now)
            .await
            .unwrap()
    );
    attempt::Entity::update_many()
        .filter(attempt::Column::Id.eq(&bad.id))
        .col_expr(
            attempt::Column::ExecutionClass,
            Expr::value("invalid-backfill-class"),
        )
        .exec(&db)
        .await
        .unwrap();
    let tail = "z-backfill-tail";
    materialize_cli_runtime_turn_with_text(&store, &workspace, tail, tail, "input").await;
    store
        .materialize_item_started_with_attempt_deadlines(
            ItemStartedNotification {
                workspace_id: workspace.clone(),
                thread_id: tail.into(),
                turn_id: tail.into(),
                item: terminal_item(),
            },
            now - 20,
            deadlines.clone(),
        )
        .await
        .unwrap();
    let original = attempt::Entity::find()
        .filter(attempt::Column::TurnId.eq(tail))
        .filter(attempt::Column::Status.eq("running"))
        .filter(attempt::Column::ItemId.eq("fetch"))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    attempt::Entity::update_many()
        .filter(attempt::Column::Id.eq(&original.id))
        .col_expr(attempt::Column::Id, Expr::value(tail))
        .exec(&db)
        .await
        .unwrap();
    pioneer_entity::turn_item::Entity::update_many()
        .filter(pioneer_entity::turn_item::Column::TurnId.eq(tail))
        .filter(pioneer_entity::turn_item::Column::ItemId.eq("fetch"))
        .filter(pioneer_entity::turn_item::Column::ActiveAttemptId.eq(&original.id))
        .col_expr(
            pioneer_entity::turn_item::Column::ActiveAttemptId,
            Expr::value(Some(tail.to_owned())),
        )
        .exec(&db)
        .await
        .unwrap();
    let candidate = store
        .list_timeout_candidates(now, 8)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.attempt_id == tail)
        .unwrap();
    assert!(
        store
            .transition_timeout_candidate(&candidate, now)
            .await
            .unwrap()
    );
    processor.arm_completed_history_preparation_barrier(label);
    let worker = tokio::spawn(MessageProcessor::run_recovery_control_worker(
        Arc::downgrade(&processor),
        cancel.clone(),
    ));
    let mut tail_seen = false;
    for _ in 0..8 {
        timeout(
            Duration::from_secs(10),
            processor.wait_for_completed_history_preparation_barrier(),
        )
        .await
        .unwrap();
        if store.count_recovery_jobs_for_turn(tail).await.unwrap() > 0 {
            tail_seen = true;
            break;
        }
        processor.arm_completed_history_preparation_barrier(label);
        processor.release_completed_history_preparation_barrier();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::resume();
    }
    assert!(
        tail_seen,
        "poisoned first page must not hide the useful raw tail"
    );
    assert!(
        attempt::Entity::find_by_id(&bad.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .recovery_action
            .is_none()
    );
    assert!(
        store
            .unqueued_timeout_candidate_by_id(&bad.id)
            .await
            .is_err(),
        "failure reaches the actual authoritative backfill decode"
    );
    // Repair emits no producer. Observe after all useful-tail commits and while
    // owner is held at its published retry boundary; rollback the same Wake.
    let before = wake.observe();
    attempt::Entity::update_many()
        .filter(attempt::Column::Id.eq(&bad.id))
        .col_expr(
            attempt::Column::ExecutionClass,
            Expr::value(bad.execution_class.clone()),
        )
        .exec(&db)
        .await
        .unwrap();
    assert_eq!(
        wake.observe(),
        before,
        "repair deliberately has no semantic producer"
    );
    wake.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs((now - 3600) as u64));
    processor.arm_completed_history_preparation_barrier(label);
    processor.release_completed_history_preparation_barrier();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::time::resume();
    let mut repaired = false;
    for _ in 0..8 {
        timeout(
            Duration::from_secs(10),
            processor.wait_for_completed_history_preparation_barrier(),
        )
        .await
        .unwrap();
        if store
            .count_recovery_jobs_for_turn(&bad.turn_id)
            .await
            .unwrap()
            > 0
        {
            repaired = true;
            break;
        }
        processor.arm_completed_history_preparation_barrier(label);
        processor.release_completed_history_preparation_barrier();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::resume();
    }
    assert!(
        repaired,
        "retained monotonic retry must repair without a producer after rollback"
    );
    // Another already-visited key changes through the normal reactivation/timeout
    // wrappers after EOF; no newly appended attempt can rescue the traversal.
    let old = &processed[1];
    assert!(
        store
            .update_turn_status(
                &old.turn_id,
                &old.turn_id,
                TurnStatus::InProgress,
                None,
                now
            )
            .await
            .unwrap()
    );
    assert!(
        store
            .reactivate_timed_out_attempt(&old.id, now - 20, deadlines)
            .await
            .unwrap()
    );
    let candidate = store
        .list_timeout_candidates(now, 8)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.attempt_id == old.id)
        .unwrap();
    assert!(
        store
            .transition_timeout_candidate(&candidate, now)
            .await
            .unwrap()
    );
    processor.arm_completed_history_preparation_barrier(label);
    processor.release_completed_history_preparation_barrier();
    let mut old_seen = false;
    for _ in 0..8 {
        timeout(
            Duration::from_secs(10),
            processor.wait_for_completed_history_preparation_barrier(),
        )
        .await
        .unwrap();
        if store
            .count_recovery_jobs_for_turn(&old.turn_id)
            .await
            .unwrap()
            > 0
        {
            old_seen = true;
            break;
        }
        processor.arm_completed_history_preparation_barrier(label);
        processor.release_completed_history_preparation_barrier();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::resume();
    }
    assert!(
        old_seen,
        "old-key producer reopens the completed raw frontier"
    );
    assert!(
        attempt::Entity::find_by_id(tail)
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .recovery_action
            .is_some()
    );
    assert!(
        attempt::Entity::find_by_id(&bad.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .recovery_action
            .is_some()
    );
    assert!(
        attempt::Entity::find_by_id(&old.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .recovery_action
            .is_some()
    );
    // The rollback experiment above proves the local retry independently.
    // Restore business time before waiting for jobs whose genuine next_run_at
    // was captured before that rollback to finish terminalization.
    wake.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
    // A job's existence alone can precede its mark-failed repair. Wait at
    // actual published boundaries until all useful obligations are terminal.
    let mut terminal = false;
    for _ in 0..12 {
        let mut all_failed = true;
        for id in [tail, bad.turn_id.as_str(), old.turn_id.as_str()] {
            all_failed &=
                store.get_turn(id, id).await.unwrap().unwrap().1.status == TurnStatus::Failed;
        }
        if all_failed {
            terminal = true;
            break;
        }
        processor.arm_completed_history_preparation_barrier(label);
        processor.release_completed_history_preparation_barrier();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::resume();
        let settled = timeout(
            Duration::from_secs(10),
            processor.wait_for_completed_history_preparation_barrier(),
        )
        .await;
        assert!(
            settled.is_ok(),
            "terminal retry must publish: tail={:?}; bad={:?}; old={:?}",
            store.get_turn(tail, tail).await,
            store.get_turn(&bad.turn_id, &bad.turn_id).await,
            store.get_turn(&old.turn_id, &old.turn_id).await
        );
    }
    assert!(
        terminal,
        "normal conservative timeout recovery must finish all three Turns"
    );
    processor.release_completed_history_preparation_barrier();
    for _ in 0..128 {
        tokio::task::yield_now().await;
    }
    tokio::time::pause();
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..128 {
        tokio::task::yield_now().await;
    }
    assert!(
        observer.reads.lock().unwrap().is_empty(),
        "new completed EOF stays SQL quiet"
    );
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();
    cancel.cancel();
    worker.await.unwrap();
    ticker.abort();
}
