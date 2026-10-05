use crate::{repositories, turn_event_was_appended_before_error};
use anyhow::Result;
use pioneer_protocol::TurnBlockedNotification;
use sea_orm::ActiveModelTrait;

const OWNER: &str = "native-cancellation-owner";
const NOW: i64 = 1_700_000_100;

async fn fixture(
    suffix: &str,
) -> (
    CrudStore,
    Turn,
    pioneer_protocol::NativeTerminalEffectPreparation,
) {
    let ws = format!("ws_cancel_{suffix}");
    let thread = format!("thread_cancel_{suffix}");
    let id = format!("turn_cancel_{suffix}");
    let (store, _, turn) = test_store_with_started_turn(&ws, &thread, &id).await;
    let mut plan = cleanup_effect_preparation(&ws, &thread, &id, "original");
    plan.effects[0].effect_id = format!("{id}:cancellation-effect:attached-task-cleanup");
    store
        .persist_native_cancellation_context(plan.clone(), OWNER, NOW, true)
        .await
        .unwrap();
    (store, turn, plan)
}

fn interrupted(
    turn: &Turn,
    plan: &pioneer_protocol::NativeTerminalEffectPreparation,
    reason: &str,
) -> CanonicalTurnEventPayload {
    let mut turn = turn.clone();
    turn.status = TurnStatus::Interrupted;
    turn.error = Some(reason.to_owned());
    CanonicalTurnEventPayload::TurnFailed(TurnFailedNotification {
        workspace_id: plan.workspace_id.clone(),
        thread_id: plan.thread_id.clone(),
        turn,
    })
}

async fn cancel(
    store: &CrudStore,
    turn: &Turn,
    plan: &pioneer_protocol::NativeTerminalEffectPreparation,
    reason: &str,
) -> Result<()> {
    let context = store.native_cancellation_context(&turn.id).await?.unwrap();
    store
        .materialize_native_cancellation_owned(
            interrupted(turn, plan, reason),
            NOW + 1,
            context,
            plan.clone(),
            OWNER,
        )
        .await
}

async fn effect_row(
    store: &CrudStore,
    id: &str,
) -> pioneer_entity::native_terminal_effect_outbox::Model {
    pioneer_entity::native_terminal_effect_outbox::Entity::find_by_id(id.to_owned())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn native_cancellation_original_context_cleanup_and_lost_ack_replay() {
    let (store, turn, plan) = fixture("immutable").await;
    let mut changed = plan.clone();
    changed.batch_id.push_str("-changed");
    changed.runtime_generation = 2;
    changed.effects.clear();
    // A restart/new configuration cannot replace the originating description.
    store
        .persist_native_cancellation_context(changed, OWNER, NOW + 1, false)
        .await
        .unwrap();
    assert_eq!(
        store
            .native_cancellation_context(&turn.id)
            .await
            .unwrap()
            .unwrap()
            .preparation,
        plan
    );
    cancel(&store, &turn, &plan, "first accepted reason")
        .await
        .unwrap();
    let first = effect_row(&store, &plan.effects[0].effect_id).await;
    let receipt = store
        .native_cancellation_receipt_owned(&turn.id, OWNER)
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .native_cancellation_receipt_owned(&turn.id, "stale-owner")
            .await
            .unwrap()
            .is_none()
    );
    let restarted = CrudStore::new(store.database_connection());
    cancel(&restarted, &turn, &plan, "different retry payload")
        .await
        .unwrap();
    let second = effect_row(&restarted, &plan.effects[0].effect_id).await;
    assert_eq!(
        first.payload_identity_sha256,
        second.payload_identity_sha256
    );
    assert_eq!(first.terminal_committed_at, second.terminal_committed_at);
    assert_eq!(first.attempt_count, second.attempt_count);
    let accepted = turn_event::find_event_by_id(&store.connection, &receipt.canonical_event_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(accepted.payload, CanonicalTurnEventPayload::TurnFailed(n)
        if n.turn.error.as_deref() == Some("first accepted reason"))
    );
    let claims = restarted
        .claim_due_native_terminal_effects(NOW + 2, 30, 2)
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].payload, plan.effects[0].payload);
}

#[tokio::test]
async fn native_cancellation_cannot_drop_obligations_from_original_context() {
    let (store, turn, mut plan) = fixture("empty").await;
    plan.effects.clear();
    // The fixture's original context intentionally has cleanup; passing an empty
    // replacement must not be accepted as that context's cancellation plan.
    let context = store
        .native_cancellation_context(&turn.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .materialize_native_cancellation_owned(
                interrupted(&turn, &plan, "cancel"),
                NOW,
                context,
                plan,
                OWNER
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn native_cancellation_append_failure_rolls_back_effects_and_receipt() {
    let (store, turn, plan) = fixture("rollback").await;
    store.connection.execute_unprepared(
        "CREATE TRIGGER reject_cancel_receipt BEFORE UPDATE OF accepted_event_id ON native_cancellation_context
         BEGIN SELECT RAISE(ABORT, 'injected cancellation receipt failure'); END"
    ).await.unwrap();
    assert!(cancel(&store, &turn, &plan, "cancel").await.is_err());
    assert!(
        !store
            .native_cancellation_was_accepted(&turn.id)
            .await
            .unwrap()
    );
    assert!(
        store
            .native_terminal_effect_status(&plan.effects[0].effect_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !repositories::turn_event_projection_stream_state::has_accepted_terminal(
            &store.connection,
            &turn.id
        )
        .await
        .unwrap()
    );
    store
        .connection
        .execute_unprepared("DROP TRIGGER reject_cancel_receipt")
        .await
        .unwrap();
    cancel(&store, &turn, &plan, "cancel").await.unwrap();
}

#[tokio::test]
async fn native_cancellation_accepted_append_recovers_projection_and_fences_old_preparation() {
    let (store, turn, plan) = fixture("projection").await;
    store.connection.execute_unprepared(
        "CREATE TRIGGER reject_cancel_projection BEFORE UPDATE OF status ON turn
         WHEN NEW.status = 'interrupted' BEGIN SELECT RAISE(ABORT, 'injected projection failure'); END"
    ).await.unwrap();
    let error = cancel(&store, &turn, &plan, "cancel").await.unwrap_err();
    assert!(turn_event_was_appended_before_error(&error));
    assert!(
        store
            .native_cancellation_was_accepted(&turn.id)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .native_terminal_effect_status(&plan.effects[0].effect_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "prepared"
    );
    let mut old_success = plan.clone();
    old_success.batch_id.push_str("-success");
    assert!(
        store
            .prepare_native_terminal_effects(old_success, NOW + 2)
            .await
            .is_err()
    );
    // Lost ACK is accepted even while the projection remains deferred.
    cancel(&store, &turn, &plan, "retry").await.unwrap();
    store
        .connection
        .execute_unprepared("DROP TRIGGER reject_cancel_projection")
        .await
        .unwrap();
    let restarted = CrudStore::new(store.database_connection());
    restarted
        .with_maintenance_access()
        .replay_due_turn_event_projections(NOW + 100, 10)
        .await
        .unwrap();
    assert_eq!(
        restarted
            .native_terminal_effect_status(&plan.effects[0].effect_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "ready"
    );
}

#[tokio::test]
async fn native_cancellation_cannot_overlay_completed_failed_or_blocked_results() {
    for (suffix, status) in [
        ("completed", TurnStatus::Completed),
        ("failed", TurnStatus::Failed),
        ("blocked", TurnStatus::Blocked),
    ] {
        let (store, mut turn, plan) = fixture(suffix).await;
        turn.status = status;
        let event = match status {
            TurnStatus::Completed => {
                CanonicalTurnEventPayload::TurnCompleted(TurnCompletedNotification {
                    workspace_id: plan.workspace_id.clone(),
                    thread_id: plan.thread_id.clone(),
                    turn: turn.clone(),
                })
            }
            TurnStatus::Failed => CanonicalTurnEventPayload::TurnFailed(TurnFailedNotification {
                workspace_id: plan.workspace_id.clone(),
                thread_id: plan.thread_id.clone(),
                turn: turn.clone(),
            }),
            TurnStatus::Blocked => {
                CanonicalTurnEventPayload::TurnBlocked(TurnBlockedNotification {
                    workspace_id: plan.workspace_id.clone(),
                    thread_id: plan.thread_id.clone(),
                    turn: turn.clone(),
                    resume: None,
                })
            }
            _ => unreachable!(),
        };
        store
            .materialize_native_agent_turn_event_owned(event, NOW + 1, None, OWNER)
            .await
            .unwrap();
        assert!(cancel(&store, &turn, &plan, "late cancel").await.is_err());
        assert!(
            !store
                .native_cancellation_was_accepted(&turn.id)
                .await
                .unwrap()
        );
    }
}

#[tokio::test]
async fn native_cancellation_replaces_uncommitted_success_plan_and_rejects_later_success() {
    let (store, mut turn, plan) = fixture("success_race").await;
    let mut success_plan = plan.clone();
    success_plan.batch_id.push_str("-success");
    success_plan.effects[0].payload =
        pioneer_protocol::NativeTerminalEffectPayload::AttachedTaskCleanup {
            reason: "success".to_owned(),
            runtime_contract: "success-contract".to_owned(),
        };
    store
        .prepare_native_terminal_effects(success_plan, NOW)
        .await
        .unwrap();
    cancel(&store, &turn, &plan, "cancel").await.unwrap();
    let row = effect_row(&store, &plan.effects[0].effect_id).await;
    assert_eq!(
        serde_json::from_str::<pioneer_protocol::NativeTerminalEffectPayload>(&row.payload_json)
            .unwrap(),
        plan.effects[0].payload
    );
    turn.status = TurnStatus::Completed;
    assert!(
        store
            .materialize_native_agent_turn_event_owned(
                CanonicalTurnEventPayload::TurnCompleted(TurnCompletedNotification {
                    workspace_id: plan.workspace_id,
                    thread_id: plan.thread_id,
                    turn,
                }),
                NOW + 2,
                None,
                OWNER
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn native_cancellation_empty_originating_plan_is_accepted_and_replayed() {
    let (store, _, turn) = test_store_with_started_turn(
        "ws_empty_cancel",
        "thread_empty_cancel",
        "turn_empty_cancel",
    )
    .await;
    let mut plan =
        cleanup_effect_preparation("ws_empty_cancel", "thread_empty_cancel", &turn.id, "empty");
    plan.effects.clear();
    store
        .persist_native_cancellation_context(plan.clone(), OWNER, NOW, true)
        .await
        .unwrap();
    cancel(&store, &turn, &plan, "cancel").await.unwrap();
    cancel(&store, &turn, &plan, "retry").await.unwrap();
    assert!(
        store
            .native_cancellation_was_accepted(&turn.id)
            .await
            .unwrap()
    );
    assert_eq!(store.native_terminal_effect_stats().await.unwrap().ready, 0);
}

#[tokio::test]
async fn native_cancellation_inherits_operation_scope_and_cancelled_writer_wait_releases_capacity()
{
    let (store, turn, plan) = fixture("scope").await;
    let maintenance = store.with_maintenance_access();
    assert_eq!(
        maintenance.database_connection().read_class(),
        pioneer_sqlite::SqliteReadClass::Maintenance
    );
    assert_eq!(
        maintenance.database_connection().write_class(),
        pioneer_sqlite::SqliteWriteClass::Maintenance
    );
    let context = store
        .native_cancellation_context(&turn.id)
        .await
        .unwrap()
        .unwrap();
    let tx = store.connection.begin().await.unwrap();
    let mut future = Box::pin(store.materialize_native_cancellation_owned(
        interrupted(&turn, &plan, "cancel"),
        NOW,
        context,
        plan.clone(),
        OWNER,
    ));
    let mut poll_context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(std::future::Future::poll(future.as_mut(), &mut poll_context).is_pending());
    // Drop the queued operation before releasing the occupied writer.
    drop(future);
    tx.rollback().await.unwrap();
    cancel(&maintenance, &turn, &plan, "cancel").await.unwrap();
    assert!(
        store
            .native_cancellation_was_accepted(&turn.id)
            .await
            .unwrap()
    );
}

#[derive(Default)]
struct CancellationRoutes {
    reads: std::sync::Mutex<Vec<pioneer_sqlite::SqliteReadClass>>,
    writes: std::sync::Mutex<Vec<pioneer_sqlite::SqliteWriteEvent>>,
    changed: tokio::sync::Notify,
}
impl pioneer_sqlite::SqliteReadObserver for CancellationRoutes {
    fn observe(&self, event: pioneer_sqlite::SqliteReadEvent) {
        if let pioneer_sqlite::SqliteReadEvent::OperationFinished { class, .. } = event {
            self.reads.lock().unwrap().push(class);
        }
    }
}
impl pioneer_sqlite::SqliteWriteObserver for CancellationRoutes {
    fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
        self.writes.lock().unwrap().push(event);
        self.changed.notify_one();
    }
}
struct CancellationDatabasePath(std::path::PathBuf);
impl Drop for CancellationDatabasePath {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

#[tokio::test]
async fn native_cancellation_physical_routes_atomic_event_order_and_cancelled_reservation() {
    use pioneer_sqlite::{
        SqliteDatabase, SqliteReadClass, SqliteWriteClass, SqliteWriteEvent, SqliteWriteExecutor,
    };
    use sea_orm::{ConnectOptions, IntoActiveModel};
    let path = CancellationDatabasePath(std::env::temp_dir().join(format!(
        "pioneer-native-cancellation-{}.sqlite",
        uuid::Uuid::new_v4()
    )));
    let mut writer_options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.0.display()));
    writer_options.max_connections(1);
    let writer = Database::connect(writer_options).await.unwrap();
    Migrator::up(&writer, None).await.unwrap();
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let mut reader_options =
        ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path.0));
    reader_options.max_connections(2);
    reader_options.map_sqlx_sqlite_opts(|o| {
        o.read_only(true)
            .create_if_missing(false)
            .pragma("query_only", "ON")
    });
    let reader = Database::connect(reader_options).await.unwrap();
    let routes = Arc::new(CancellationRoutes::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, routes.clone()),
        routes.clone(),
    );
    let store = CrudStore::new(database.clone());
    let (seed, thread, turn) = test_store_with_started_turn(
        "ws_cancel_routes",
        "thread_cancel_routes",
        "turn_cancel_routes",
    )
    .await;
    pioneer_entity::workspace::Entity::find_by_id(thread.workspace_id.clone())
        .one(&seed.connection)
        .await
        .unwrap()
        .unwrap()
        .into_active_model()
        .insert(&store.connection)
        .await
        .unwrap();
    store
        .upsert_thread_model(&thread, pioneer_protocol::PersistedActorRef::System)
        .await
        .unwrap();
    store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &turn,
            &[],
            pioneer_protocol::PersistedActorRef::System,
        )
        .await
        .unwrap();
    let plan = cleanup_effect_preparation(&thread.workspace_id, &thread.id, &turn.id, "routes");
    let maintenance = store.with_maintenance_access();
    maintenance
        .persist_native_cancellation_context(plan.clone(), OWNER, NOW, true)
        .await
        .unwrap();
    let context = maintenance
        .native_cancellation_context(&turn.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        routes
            .reads
            .lock()
            .unwrap()
            .contains(&SqliteReadClass::Maintenance)
    );
    assert!(routes.writes.lock().unwrap().iter().any(|e| matches!(
        e,
        SqliteWriteEvent::Acquired {
            class: SqliteWriteClass::Maintenance,
            ..
        }
    )));
    let blocker = database.begin().await.unwrap();
    let before = routes.writes.lock().unwrap().len();
    let queued = {
        let maintenance = maintenance.clone();
        let turn = turn.clone();
        let plan = plan.clone();
        tokio::spawn(async move {
            maintenance
                .materialize_native_cancellation_owned(
                    interrupted(&turn, &plan, "cancel"),
                    NOW + 1,
                    context,
                    plan,
                    OWNER,
                )
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if routes.writes.lock().unwrap()[before..].iter().any(|e| {
                matches!(
                    e,
                    SqliteWriteEvent::Enqueued {
                        class: SqliteWriteClass::Maintenance,
                        ..
                    }
                )
            }) {
                break;
            }
            routes.changed.notified().await;
        }
    })
    .await
    .unwrap();
    queued.abort();
    assert!(queued.await.unwrap_err().is_cancelled());
    assert!(routes.writes.lock().unwrap()[before..].iter().any(|e| matches!(e,
        SqliteWriteEvent::Cancelled { class: SqliteWriteClass::Maintenance, queue, .. } if queue.maintenance == 0)));
    blocker.rollback().await.unwrap();
    let (interactive, background) = tokio::join!(
        cancel(&store, &turn, &plan, "cancel"),
        cancel(&maintenance, &turn, &plan, "cancel")
    );
    interactive.unwrap();
    background.unwrap();
    let receipt = store
        .native_cancellation_receipt_owned(&turn.id, OWNER)
        .await
        .unwrap()
        .unwrap();
    let accepted = turn_event::find_event_by_id(&store.connection, &receipt.canonical_event_id)
        .await
        .unwrap()
        .unwrap();
    assert!(accepted.sequence > 1);
    assert_eq!(store.native_terminal_effect_stats().await.unwrap().ready, 1);
}

#[tokio::test]
async fn native_cancellation_recovery_cannot_substitute_current_configuration_for_missing_history()
{
    let (store, _, turn) = test_store_with_started_turn(
        "ws_missing_cancel",
        "thread_missing_cancel",
        "turn_missing_cancel",
    )
    .await;
    let plan = cleanup_effect_preparation(
        "ws_missing_cancel",
        "thread_missing_cancel",
        &turn.id,
        "current-settings",
    );
    assert!(
        store
            .persist_native_cancellation_context(plan, OWNER, NOW, false)
            .await
            .is_err()
    );
    assert!(
        store
            .native_cancellation_context(&turn.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !store
            .native_cancellation_was_accepted(&turn.id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn native_cancellation_revalidates_execution_owner_and_immutable_plan_before_append() {
    let (store, turn, mut plan) = fixture("revalidation").await;
    let context = store
        .native_cancellation_context(&turn.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .materialize_native_cancellation_owned(
                interrupted(&turn, &plan, "cancel"),
                NOW,
                context.clone(),
                plan.clone(),
                "stale-owner"
            )
            .await
            .is_err()
    );
    plan.runtime_generation += 1;
    assert!(
        store
            .materialize_native_cancellation_owned(
                interrupted(&turn, &plan, "cancel"),
                NOW,
                context,
                plan,
                OWNER
            )
            .await
            .is_err()
    );
    assert!(
        !store
            .native_cancellation_was_accepted(&turn.id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn native_cancellation_digest_is_checked_outside_writer_and_fence_has_no_json() {
    use pioneer_entity::native_cancellation_context as entity;
    let (store, turn, _) = fixture("digest").await;
    let context = store
        .native_cancellation_context(&turn.id)
        .await
        .unwrap()
        .unwrap();
    let fence = repositories::native_cancellation_context::prepare_revalidation(&context, OWNER);
    let debug = format!("{fence:?}");
    assert!(debug.contains("context_sha256"));
    assert!(!debug.contains("context_json"));
    assert!(debug.len() < 4096);
    let row = entity::Entity::find_by_id(turn.id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.context_sha256,
        repositories::native_cancellation_context::digest(&row.context_json)
    );
    let _: sea_orm::prelude::DateTimeWithTimeZone = row.created_at;
    entity::Entity::update_many()
        .col_expr(entity::Column::ContextJson, Expr::value("{}"))
        .filter(entity::Column::TurnId.eq(turn.id.clone()))
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(store.native_cancellation_context(&turn.id).await.is_err());
    // The damaged JSON cannot be parsed/admitted even though it is valid JSON.
    assert!(
        !store
            .native_cancellation_was_accepted(&turn.id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn native_cancellation_competing_terminal_append_before_projection_is_fenced() {
    let (store, mut turn, plan) = fixture("terminal_gap").await;
    store.connection.execute_unprepared("CREATE TRIGGER reject_completed_projection BEFORE UPDATE OF status ON turn WHEN NEW.status = 'completed' BEGIN SELECT RAISE(ABORT, 'injected projection failure'); END").await.unwrap();
    turn.status = TurnStatus::Completed;
    let event = CanonicalTurnEventPayload::TurnCompleted(TurnCompletedNotification {
        workspace_id: plan.workspace_id.clone(),
        thread_id: plan.thread_id.clone(),
        turn: turn.clone(),
    });
    let error = store
        .materialize_native_agent_turn_event_owned(event, NOW, None, OWNER)
        .await
        .unwrap_err();
    assert!(turn_event_was_appended_before_error(&error));
    let marker =
        repositories::turn_event_projection_stream_state::find(&store.connection, &turn.id)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        marker.accepted_terminal_event_type.as_deref(),
        Some("turn/completed")
    );
    assert!(marker.accepted_terminal_sequence.unwrap() > marker.projected_through_sequence);
    turn.status = TurnStatus::InProgress;
    assert!(cancel(&store, &turn, &plan, "too late").await.is_err());
    assert!(
        !store
            .native_cancellation_was_accepted(&turn.id)
            .await
            .unwrap()
    );
    assert!(
        store
            .native_terminal_effect_status(&plan.effects[0].effect_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn native_cancellation_marker_and_receipt_fence_both_append_paths_and_replay() {
    let (store, mut turn, plan) = fixture("both_paths").await;
    cancel(&store, &turn, &plan, "cancel").await.unwrap();
    let original =
        repositories::turn_event_projection_stream_state::find(&store.connection, &turn.id)
            .await
            .unwrap()
            .unwrap();
    let receipt = store
        .native_cancellation_receipt_owned(&turn.id, OWNER)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        original.accepted_terminal_event_id.as_deref(),
        Some(receipt.canonical_event_id.as_str())
    );
    turn.status = TurnStatus::Completed;
    let event = CanonicalTurnEventPayload::TurnCompleted(TurnCompletedNotification {
        workspace_id: plan.workspace_id.clone(),
        thread_id: plan.thread_id.clone(),
        turn: turn.clone(),
    });
    assert!(
        store
            .materialize_native_agent_turn_event_owned(event.clone(), NOW, None, OWNER)
            .await
            .is_err()
    );
    assert!(
        store
            .materialize_turn_events_atomically(vec![event], NOW)
            .await
            .is_err()
    );
    cancel(&store, &turn, &plan, "different retry")
        .await
        .unwrap();
    let after = repositories::turn_event_projection_stream_state::find(&store.connection, &turn.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        original.accepted_terminal_event_id,
        after.accepted_terminal_event_id
    );
    assert_eq!(
        original.accepted_terminal_sequence,
        after.accepted_terminal_sequence
    );
    assert_eq!(
        receipt,
        store
            .native_cancellation_receipt_owned(&turn.id, OWNER)
            .await
            .unwrap()
            .unwrap()
    );
}

#[tokio::test]
async fn native_cancellation_marker_failure_rolls_back_entire_append_boundary() {
    let (store, turn, plan) = fixture("marker_rollback").await;
    let before_sequence = turn_event::max_sequence_for_turn(&store.connection, &turn.id)
        .await
        .unwrap();
    store.connection.execute_unprepared("CREATE TRIGGER reject_marker BEFORE UPDATE OF accepted_terminal_event_id ON turn_event_projection_stream_state BEGIN SELECT RAISE(ABORT, 'injected marker failure'); END").await.unwrap();
    assert!(cancel(&store, &turn, &plan, "cancel").await.is_err());
    assert!(
        !store
            .native_cancellation_was_accepted(&turn.id)
            .await
            .unwrap()
    );
    assert!(
        !repositories::turn_event_projection_stream_state::has_accepted_terminal(
            &store.connection,
            &turn.id
        )
        .await
        .unwrap()
    );
    assert!(
        store
            .native_terminal_effect_status(&plan.effects[0].effect_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        before_sequence,
        turn_event::max_sequence_for_turn(&store.connection, &turn.id)
            .await
            .unwrap()
    );
    let (_, persisted) = store
        .get_turn(&plan.thread_id, &turn.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.status, TurnStatus::InProgress);
    store
        .connection
        .execute_unprepared("DROP TRIGGER reject_marker")
        .await
        .unwrap();
    cancel(&store, &turn, &plan, "cancel").await.unwrap();
}

#[tokio::test]
async fn native_cancellation_blocked_lawful_resume_keeps_original_context_and_allows_interruption()
{
    use pioneer_protocol::{
        NativeTerminalEffectGate, NativeTerminalEffectKind, NativeTerminalEffectPayload,
        NativeTerminalEffectSpec,
    };
    let ws = "ws_blocked_obligations";
    let thread = "thread_blocked_obligations";
    let id = "turn_blocked_obligations";
    let (store, _, mut turn) = test_store_with_started_turn(ws, thread, id).await;
    let mut plan = cleanup_effect_preparation(ws, thread, id, "original");
    plan.effects[0].effect_id = format!("{id}:cancellation-effect:attached-task-cleanup");
    plan.effects.push(NativeTerminalEffectSpec {
        effect_id: format!("{id}:cancellation-effect:post-turn"),
        effect_kind: NativeTerminalEffectKind::PostTurnHook,
        gate: NativeTerminalEffectGate::TerminalCommit,
        payload: NativeTerminalEffectPayload::PostTurnHook {
            request: serde_json::json!({"input":{"payload":{"value":{"status":"interrupted"}}}}),
            runtime_snapshot: serde_json::json!({}),
        },
        max_attempts: 3,
    });
    store
        .persist_native_cancellation_context(plan.clone(), OWNER, NOW, true)
        .await
        .unwrap();
    let mut blocked_plan = plan.clone();
    blocked_plan.batch_id.push_str(":blocked");
    for effect in &mut blocked_plan.effects {
        effect.effect_id = effect
            .effect_id
            .replace(":cancellation-effect:", ":terminal-effect:");
        match &mut effect.payload {
            NativeTerminalEffectPayload::AttachedTaskCleanup { reason, .. } => {
                *reason = "parent turn blocked".into()
            }
            NativeTerminalEffectPayload::PostTurnHook { request, .. } => {
                request["input"]["payload"]["value"]["status"] = serde_json::json!("blocked")
            }
            _ => unreachable!(),
        }
    }
    store
        .prepare_native_terminal_effects(blocked_plan.clone(), NOW)
        .await
        .unwrap();
    turn.status = TurnStatus::Blocked;
    let block = CanonicalTurnEventPayload::TurnBlocked(TurnBlockedNotification {
        workspace_id: plan.workspace_id.clone(),
        thread_id: plan.thread_id.clone(),
        turn: turn.clone(),
        resume: None,
    });
    store
        .materialize_native_agent_turn_event_owned(block, NOW, None, OWNER)
        .await
        .unwrap();
    assert_eq!(store.native_terminal_effect_stats().await.unwrap().ready, 2);
    // Claim old obligations and publish a real immutable handler checkpoint.
    // Cancellation must preserve even in-flight worker identity and lease state.
    let old_claims = store
        .claim_due_native_terminal_effects(NOW, 30, 2)
        .await
        .unwrap();
    assert_eq!(old_claims.len(), 2);
    for claim in &old_claims {
        if claim.effect_id.ends_with(":post-turn") {
            store
                .store_native_terminal_effect_handler_checkpoint(
                    &claim.effect_id,
                    &claim.claim_token,
                    "{\"checkpoint\":true}",
                    NOW,
                )
                .await
                .unwrap();
        }
    }
    let mut old_rows = Vec::new();
    for effect in &blocked_plan.effects {
        old_rows.push(effect_row(&store, &effect.effect_id).await);
    }
    // An unconfirmed resume cannot bypass durable Blocked acceptance.
    assert!(
        cancel(&store, &turn, &plan, "without lawful resume")
            .await
            .is_err()
    );
    let job = store
        .enqueue_recovery_job(
            turn.id.clone(),
            "reasoning_resume".to_owned(),
            TurnItemType::Reasoning,
            None,
            RecoveryTrigger::Timeout,
            RecoveryAction::BlockResumable,
            None,
            None,
            None,
            None,
            0,
            0,
            serde_json::json!({}),
            serde_json::json!({"base_backoff_secs":0,"max_wall_clock_secs":60}),
            NOW,
        )
        .await
        .unwrap();
    store
        .mark_recovery_job_terminal(&job.id, RecoveryJobStatus::Blocked, None, NOW)
        .await
        .unwrap();
    assert!(matches!(
        store
            .resume_blocked_turn_recovery(
                &plan.thread_id,
                &turn.id,
                Some(&job.id),
                NOW + 1,
                OWNER,
                NOW + 100
            )
            .await
            .unwrap(),
        BlockedTurnRecoveryResumeOutcome::Resumed(_)
    ));
    assert!(
        !repositories::turn_event_projection_stream_state::has_accepted_terminal(
            &store.connection,
            &turn.id
        )
        .await
        .unwrap()
    );
    assert_eq!(
        store
            .native_cancellation_context(&turn.id)
            .await
            .unwrap()
            .unwrap()
            .preparation,
        plan
    );
    turn.status = TurnStatus::InProgress;
    cancel(&store, &turn, &plan, "after lawful resume")
        .await
        .unwrap();
    assert_eq!(store.native_terminal_effect_stats().await.unwrap().ready, 2);
    for before in &old_rows {
        assert_eq!(&effect_row(&store, &before.effect_id).await, before);
    }
    let cancellation_claims = store
        .claim_due_native_terminal_effects(NOW + 2, 30, 2)
        .await
        .unwrap();
    assert_eq!(cancellation_claims.len(), 2);
    assert!(
        cancellation_claims
            .iter()
            .all(|c| c.effect_id.contains(":cancellation-effect:"))
    );
    let mut cancellation_rows = Vec::new();
    for claim in &cancellation_claims {
        cancellation_rows.push(effect_row(&store, &claim.effect_id).await);
    }
    let restarted = CrudStore::new(store.database_connection());
    cancel(&restarted, &turn, &plan, "different replay reason")
        .await
        .unwrap();
    for before in old_rows.iter().chain(&cancellation_rows) {
        assert_eq!(&effect_row(&restarted, &before.effect_id).await, before);
    }
    assert_eq!(
        restarted
            .claim_due_native_terminal_effects(NOW + 3, 30, 2)
            .await
            .unwrap()
            .len(),
        0
    );
    // Complete the old worker wave through its real claim-token fence. Changes
    // made by its own completion are legal; cancellation replay changes nothing.
    for claim in &old_claims {
        assert!(
            restarted
                .complete_native_terminal_effect(&claim.effect_id, &claim.claim_token, NOW + 3)
                .await
                .unwrap()
        );
    }
    let mut completed_old = Vec::new();
    for before in &old_rows {
        let after = effect_row(&restarted, &before.effect_id).await;
        assert_eq!(after.status, "succeeded");
        assert_eq!(
            after.payload_identity_sha256,
            before.payload_identity_sha256
        );
        assert_eq!(after.gate_kind, before.gate_kind);
        assert_eq!(after.attempt_count, before.attempt_count);
        assert_eq!(after.terminal_committed_at, before.terminal_committed_at);
        completed_old.push(after);
    }
    cancel(&restarted, &turn, &plan, "retry after old worker")
        .await
        .unwrap();
    for before in completed_old.iter().chain(&cancellation_rows) {
        assert_eq!(&effect_row(&restarted, &before.effect_id).await, before);
    }
    assert_eq!(
        pioneer_entity::native_terminal_effect_outbox::Entity::find()
            .filter(pioneer_entity::native_terminal_effect_outbox::Column::TurnId.eq(id))
            .all(&store.connection)
            .await
            .unwrap()
            .len(),
        4
    );
    turn.status = TurnStatus::Completed;
    assert!(
        restarted
            .materialize_native_agent_turn_event_owned(
                CanonicalTurnEventPayload::TurnCompleted(TurnCompletedNotification {
                    workspace_id: ws.into(),
                    thread_id: thread.into(),
                    turn
                }),
                NOW + 4,
                None,
                OWNER
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn native_cancellation_owner_change_and_health_restore_cannot_clear_fence() {
    use pioneer_entity::turn_execution as execution;
    let (store, turn, plan) = fixture("owner_change").await;
    repositories::turn_execution::insert_immutable(
        &store.connection,
        crate::NewTurnExecution {
            turn_id: turn.id.clone(),
            thread_id: plan.thread_id.clone(),
            workspace_id: plan.workspace_id.clone(),
            executor_kind: crate::TurnExecutorKind::NativeAgent,
            executor_key: Some("openai".into()),
            status: crate::TurnExecutionStatus::Running,
            owner_id: OWNER.into(),
            lease_until: unix_to_datetime(NOW + 100),
            created_at: unix_to_datetime(NOW),
        },
    )
    .await
    .unwrap();
    cancel(&store, &turn, &plan, "cancel").await.unwrap();
    let before =
        repositories::turn_event_projection_stream_state::find(&store.connection, &turn.id)
            .await
            .unwrap()
            .unwrap();
    execution::Entity::update_many()
        .col_expr(execution::Column::OwnerId, Expr::value("replacement-owner"))
        .filter(execution::Column::TurnId.eq(turn.id.clone()))
        .exec(&store.connection)
        .await
        .unwrap();
    repositories::turn_event_projection_stream_state::ensure_healthy(
        &store.connection,
        &plan.thread_id,
        &turn.id,
        unix_to_datetime(NOW + 1),
    )
    .await
    .unwrap();
    assert!(
        store
            .native_cancellation_was_accepted(&turn.id)
            .await
            .unwrap()
    );
    assert!(
        store
            .prepare_native_terminal_effects(plan.clone(), NOW + 2)
            .await
            .is_ok()
    ); // Exact immutable terminal replay.
    let after = repositories::turn_event_projection_stream_state::find(&store.connection, &turn.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        before.accepted_terminal_event_id,
        after.accepted_terminal_event_id
    );
    assert!(
        repositories::turn_event_projection_stream_state::clear_confirmed_blocked_for_resume(
            &store.connection,
            &plan.thread_id,
            &turn.id,
            unix_to_datetime(NOW + 2),
            &pioneer_entity::turn::Entity::find_by_id(turn.id.clone())
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap()
        )
        .await
        .is_err()
    );
}

// Isolate the cancellation migration's up/down boundary while keeping the
// full registered schema. Later unrelated migrations must not become the
// target of these guards merely because they were appended to the registry.
// This ordering is fixture-only; the production migrator keeps chronological order.
struct CancellationFixtureMigrator;

impl MigratorTrait for CancellationFixtureMigrator {
    fn migrations() -> Vec<Box<dyn migration::MigrationTrait>> {
        let mut migrations = Migrator::migrations();
        let index = migrations
            .iter()
            .position(|migration| {
                migration.name() == "m20261001_000001_native_cancellation_context"
            })
            .expect("native cancellation migration must remain registered");
        let cancellation = migrations.remove(index);
        migrations.push(cancellation);
        migrations
    }
}

// Use the scoped writer transaction's SeaORM executor without exposing a pool.
// Both successful migration writes and failed down guards release the reservation
// before the caller inspects durable state.
async fn migrate_fixture(
    db: &pioneer_sqlite::SqliteDatabase,
    steps: Option<u32>,
    down: bool,
) -> std::result::Result<(), sea_orm::DbErr> {
    let transaction = db.begin().await?;
    let result = if down {
        CancellationFixtureMigrator::down(&*transaction, steps).await
    } else {
        CancellationFixtureMigrator::up(&*transaction, steps).await
    };
    match result {
        Ok(()) => transaction.commit().await,
        Err(error) => {
            transaction.rollback().await?;
            Err(error)
        }
    }
}

#[tokio::test]
async fn native_cancellation_migration_plain_and_zstd_use_entity_schema_without_event_index() {
    use migration::SchemaManager;
    for compressed in [false, true] {
        if compressed {
            pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
        }
        let db = pioneer_sqlite::SqliteDatabase::from_single_connection(
            Database::connect("sqlite::memory:").await.unwrap(),
        );
        // Apply the pre-change schema, optionally install the real logical view,
        // then traverse the new migration. No migration runs in this work session.
        migrate_fixture(
            &db,
            Some((CancellationFixtureMigrator::migrations().len() - 1) as u32),
            false,
        )
        .await
        .unwrap();
        let transaction = db.begin().await.unwrap();
        assert!(
            SchemaManager::new(&*transaction)
                .has_index(
                    "native_terminal_effect_outbox",
                    "uidx_native_terminal_effect_turn_kind"
                )
                .await
                .unwrap()
        );
        transaction.rollback().await.unwrap();
        if compressed {
            let config = serde_json::json!({"table":"turn_event","column":"payload","compression_level":3,"dict_chooser":"'[nodict]'"});
            db.query_one_write_raw(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT zstd_enable_transparent(?) AS value",
                [config.to_string().into()],
            ))
            .await
            .unwrap();
        }
        migrate_fixture(&db, None, false).await.unwrap();
        let transaction = db.begin().await.unwrap();
        let schema = SchemaManager::new(&*transaction);
        for column in [
            "context_json",
            "context_sha256",
            "created_at",
            "accepted_event_id",
        ] {
            assert!(
                schema
                    .has_column("native_cancellation_context", column)
                    .await
                    .unwrap()
            );
        }
        for column in [
            "accepted_terminal_event_id",
            "accepted_terminal_event_type",
            "accepted_terminal_sequence",
        ] {
            assert!(
                schema
                    .has_column("turn_event_projection_stream_state", column)
                    .await
                    .unwrap()
            );
        }
        assert!(
            !schema
                .has_index("turn_event", "idx_turn_event_terminal_fence")
                .await
                .unwrap()
        );
        assert!(
            !schema
                .has_index("_turn_event_zstd", "idx_turn_event_terminal_fence")
                .await
                .unwrap()
        );
        assert!(
            pioneer_entity::native_cancellation_context::Entity::find()
                .all(&*transaction)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            !schema
                .has_index(
                    "native_terminal_effect_outbox",
                    "uidx_native_terminal_effect_turn_kind"
                )
                .await
                .unwrap()
        );
        assert!(
            schema
                .has_index(
                    "native_terminal_effect_outbox",
                    "idx_native_terminal_effect_turn"
                )
                .await
                .unwrap()
        );
        transaction.rollback().await.unwrap();
        // Empty down is safe and retryable; durable rows/markers are covered separately.
        migrate_fixture(&db, Some(1), true).await.unwrap();
        let transaction = db.begin().await.unwrap();
        assert!(
            SchemaManager::new(&*transaction)
                .has_index(
                    "native_terminal_effect_outbox",
                    "uidx_native_terminal_effect_turn_kind"
                )
                .await
                .unwrap()
        );
        transaction.rollback().await.unwrap();
        migrate_fixture(&db, None, false).await.unwrap();
    }
}

#[tokio::test]
async fn native_cancellation_migration_down_preserves_context_and_terminal_markers() {
    let (store, turn, plan) = fixture("down_guard").await;
    assert!(
        migrate_fixture(&store.connection, Some(1), true)
            .await
            .is_err()
    );
    cancel(&store, &turn, &plan, "cancel").await.unwrap();
    assert!(
        migrate_fixture(&store.connection, Some(1), true)
            .await
            .is_err()
    );
    assert!(
        store
            .native_cancellation_was_accepted(&turn.id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn native_cancellation_task_resume_clears_only_confirmed_blocked_and_rolls_back_conflict() {
    use pioneer_entity::turn_event_projection_stream_state as stream;
    for conflict in [false, true] {
        let suffix = if conflict {
            "marker_conflict"
        } else {
            "marker_resume"
        };
        let ws = format!("ws_task_{suffix}");
        let (store, task, _, job, thread_id, turn_id) =
            task_owned_resume_conflict_fixture(&ws, suffix).await;
        // Repair the older aggregate-only fixture with its real Thread owner so
        // canonical projection and its marker obey production scope checks.
        let (_, mut thread, _) =
            test_store_with_started_turn(&ws, "fixture_thread", "fixture_turn").await;
        thread.id = thread_id.clone();
        store
            .upsert_thread_model(&thread, pioneer_protocol::PersistedActorRef::System)
            .await
            .unwrap();
        let (_, turn) = store.get_turn(&thread_id, &turn_id).await.unwrap().unwrap();
        store
            .materialize_turn_events_atomically(
                vec![CanonicalTurnEventPayload::TurnBlocked(
                    TurnBlockedNotification {
                        workspace_id: ws.clone(),
                        thread_id: thread_id.clone(),
                        turn,
                        resume: None,
                    },
                )],
                NOW,
            )
            .await
            .unwrap();
        let before =
            repositories::turn_event_projection_stream_state::find(&store.connection, &turn_id)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            before.accepted_terminal_event_type.as_deref(),
            Some("turn/blocked")
        );
        if conflict {
            stream::Entity::update_many()
                .col_expr(
                    stream::Column::AcceptedTerminalEventType,
                    Expr::value("turn/failed"),
                )
                .filter(stream::Column::TurnId.eq(turn_id.clone()))
                .exec(&store.connection)
                .await
                .unwrap();
        }
        let result = store
            .resume_task_owned_turn(
                &thread_id,
                &turn_id,
                Some(&job.id),
                NOW + 1,
                OWNER,
                NOW + 100,
            )
            .await;
        if conflict {
            assert!(result.is_err());
            assert_eq!(
                store
                    .get_turn(&thread_id, &turn_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .1
                    .status,
                TurnStatus::Blocked
            );
            assert_eq!(
                store.get_task(&task.id).await.unwrap().unwrap().task.status,
                TaskStatus::Blocked
            );
            assert_eq!(
                store
                    .get_recovery_job(&job.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                RecoveryJobStatus::Blocked
            );
            assert_eq!(
                repositories::turn_event_projection_stream_state::find(&store.connection, &turn_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .accepted_terminal_event_id,
                before.accepted_terminal_event_id
            );
        } else {
            assert!(matches!(
                result.unwrap(),
                Some(TaskOwnedTurnResumeOutcome::Resumed { .. })
            ));
            assert!(
                !repositories::turn_event_projection_stream_state::has_accepted_terminal(
                    &store.connection,
                    &turn_id
                )
                .await
                .unwrap()
            );
        }
    }
}

#[tokio::test]
async fn native_cancellation_migration_down_rejects_marker_without_context() {
    let (store, thread, mut turn) =
        test_store_with_started_turn("ws_marker_down", "thread_marker_down", "turn_marker_down")
            .await;
    turn.status = TurnStatus::Completed;
    store
        .materialize_turn_events_atomically(
            vec![CanonicalTurnEventPayload::TurnCompleted(
                TurnCompletedNotification {
                    workspace_id: thread.workspace_id,
                    thread_id: thread.id,
                    turn: turn.clone(),
                },
            )],
            NOW,
        )
        .await
        .unwrap();
    assert!(
        store
            .native_cancellation_context(&turn.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        migrate_fixture(&store.connection, Some(1), true)
            .await
            .is_err()
    );
    assert!(
        repositories::turn_event_projection_stream_state::has_accepted_terminal(
            &store.connection,
            &turn.id
        )
        .await
        .unwrap()
    );
}
