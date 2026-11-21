use super::*;
use crate::cli_runtime_store::CliRuntimeStore;
use crate::gateway_control::GatewayControlWake;
use crate::native_events::{self, NativeEventRepository};
use crate::turn_execution::NewTurnExecution;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, EntityTrait, TransactionTrait};

use crate::test_fixture::InlineInput;

async fn native_delivery_fixture() -> (
    CliRuntimeStore,
    pioneer_protocol::CliRuntimeBlockedTurnGuard,
    NewCliRuntimeNativeEvent,
) {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = SqliteDatabase::new(
        fixture.connect_read_only().await.unwrap(),
        fixture.connection().clone(),
    );
    db.execute_unprepared(
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws_cli_guard','CLI',TRUE,FALSE);
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class)
             VALUES ('thr_cli_guard','ws_cli_guard','','agent','gpt-5.4','openai','active','user','workspace');
         INSERT INTO turn(id,thread_id,status,turn_kind,origin)
             VALUES ('turn_cli_guard','thr_cli_guard','in_progress','conversation','user');",
    ).await.unwrap();
    let timestamp = crate::time::unix_to_datetime(1_700_000_000);
    let tx = db.begin().await.unwrap();
    prepare_initial_attempt(
        &tx,
        &NewCliRuntimeTurnBinding {
            turn_id: "turn_cli_guard".into(),
            thread_id: "thr_cli_guard".into(),
            continuation_thread_id: "thr_cli_guard".into(),
            workspace_id: "ws_cli_guard".into(),
            runtime_id: "codex".into(),
            runtime_kind: "codex".into(),
            native_thread_id: "native_thread".into(),
            native_turn_id: None,
            request_id: None,
            status: "starting".into(),
            model: None,
            cwd: None,
            sandbox_json: None,
            approval_policy: None,
            input_mapping_json: "{}".into(),
            created_at: timestamp,
            updated_at: timestamp,
        },
        "attempt_guard_1",
        1,
        InputMapping::prepare("{}", false).unwrap(),
        &InlineInput,
    )
    .await
    .unwrap();
    let ((binding, _), _, _, _) = activate_attempt(
        &tx,
        "turn_cli_guard",
        "attempt_guard_1",
        "native-delivery-A",
        None,
        timestamp,
        None,
        None,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let store = CliRuntimeStore::new(db).maintenance();
    let source = store
        .cli_runtime_terminal_event_source(&binding, "native-delivery-A")
        .await
        .unwrap()
        .unwrap();
    let event = NewCliRuntimeNativeEvent {
        id: "native-terminal-delivery-fixture".into(),
        runtime_id: source.runtime_id.clone(),
        runtime_kind: source.runtime_kind.clone(),
        turn_id: Some(source.turn_id.clone()),
        thread_id: Some(source.thread_id.clone()),
        workspace_id: Some(source.workspace_id.clone()),
        native_thread_id: Some(source.native_thread_id.clone()),
        native_turn_id: Some("native-delivery-A".into()),
        native_method: "gateway/terminal_delivery".into(),
        payload_redacted_json: "{\"outcome\":\"failed\"}".into(),
        sequence: 1,
        created_at: binding.updated_at,
    };
    (store, source, event)
}

#[pioneer_postgres_test::postgres_test]
async fn cli_native_terminal_source_write_preserves_first_outcome_and_rolls_back_on_storage_failure()
 {
    let (store, source, event) = native_delivery_fixture().await;
    let binding = store
        .get_cli_runtime_turn_binding(&source.turn_id)
        .await
        .unwrap()
        .unwrap();
    let before = store
        .cli_runtime_turn_terminal_guard(&binding)
        .await
        .unwrap();
    store.database_connection().execute_unprepared(
        "CREATE FUNCTION reject_native_terminal_delivery() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected delivery failure'; END $$; CREATE TRIGGER reject_native_terminal_delivery BEFORE INSERT ON cli_runtime_native_event FOR EACH ROW WHEN (NEW.native_method = 'gateway/terminal_delivery') EXECUTE FUNCTION reject_native_terminal_delivery()"
    ).await.unwrap();
    assert!(
        store
            .persist_cli_runtime_terminal_event(event.clone(), &source, "native-delivery-owner")
            .await
            .is_err()
    );
    assert!(
        native_events::find_native_event(&store.database_connection(), &event.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .cli_runtime_turn_terminal_guard(&binding)
            .await
            .unwrap(),
        before
    );
    store
        .database_connection()
        .execute_unprepared("DROP TRIGGER reject_native_terminal_delivery ON cli_runtime_native_event; DROP FUNCTION reject_native_terminal_delivery()")
        .await
        .unwrap();
    assert!(
        store
            .persist_cli_runtime_terminal_event(event.clone(), &source, "native-delivery-owner")
            .await
            .unwrap()
    );
    let mut later = event.clone();
    later.payload_redacted_json = "{\"outcome\":\"different\"}".into();
    assert!(
        store
            .persist_cli_runtime_terminal_event(later, &source, "native-delivery-owner")
            .await
            .unwrap()
    );
    assert_eq!(
        native_events::find_native_event(&store.database_connection(), &event.id)
            .await
            .unwrap()
            .unwrap()
            .payload_redacted_json,
        event.payload_redacted_json,
        "a retransmission cannot replace the accepted native outcome"
    );
    assert_eq!(
        store
            .cli_runtime_turn_terminal_guard(&binding)
            .await
            .unwrap(),
        before
    );
}

#[pioneer_postgres_test::postgres_test]
async fn cli_native_terminal_source_write_rejects_owner_aba_even_with_same_binding_and_timestamp() {
    let (store, _, event) = native_delivery_fixture().await;
    let timestamp = event.created_at;
    // A live lease makes rejection depend on the generation, not expiration.
    let lease_until = chrono::Utc::now().fixed_offset() + chrono::Duration::hours(1);
    let db = store.database_connection();
    let owner = turn_execution::insert_immutable(
        &db,
        NewTurnExecution {
            turn_id: event.turn_id.clone().unwrap(),
            thread_id: event.thread_id.clone().unwrap(),
            workspace_id: event.workspace_id.clone().unwrap(),
            executor_kind: TurnExecutorKind::CliRuntime,
            executor_key: Some("codex".into()),
            status: TurnExecutionStatus::Starting,
            owner_id: "native-delivery-owner".into(),
            lease_until,
            created_at: timestamp,
        },
    )
    .await
    .unwrap();
    let binding = store
        .get_cli_runtime_turn_binding(&owner.turn_id)
        .await
        .unwrap()
        .unwrap();
    let source = store
        .cli_runtime_terminal_event_source(&binding, "native-delivery-A")
        .await
        .unwrap()
        .unwrap();
    assert!(
        turn_execution::mark_terminal(
            &db,
            &owner.turn_id,
            TurnExecutionStatus::Blocked,
            timestamp,
        )
        .await
        .unwrap()
    );
    assert!(
        turn_execution::reacquire_blocked(
            &db,
            &owner.turn_id,
            &owner.owner_id,
            timestamp,
            lease_until
        )
        .await
        .unwrap()
    );
    let mut resumed_owner = owner.clone();
    resumed_owner.owner_generation += 1;
    assert_eq!(
        turn_execution::find(&db, &owner.turn_id).await.unwrap(),
        Some(resumed_owner),
        "only the owner generation changes across the ABA transition"
    );
    assert!(
        !store
            .persist_cli_runtime_terminal_event(event.clone(), &source, "native-delivery-owner")
            .await
            .unwrap()
    );
    assert!(
        native_events::find_native_event(&store.database_connection(), &event.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .get_cli_runtime_turn_binding(&owner.turn_id)
            .await
            .unwrap()
            .unwrap(),
        binding
    );
    assert!(
        store
            .latest_cli_runtime_turn_attempt(&owner.turn_id)
            .await
            .unwrap()
            .unwrap()
            .status
            .is_active()
    );
    let fresh_source = store
        .cli_runtime_terminal_event_source(&binding, "native-delivery-A")
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .persist_cli_runtime_terminal_event(event, &fresh_source, "native-delivery-owner")
            .await
            .unwrap(),
        "the current generation can still persist its outcome"
    );
}

#[pioneer_postgres_test::postgres_test]
async fn cli_accepted_outcome_wins_recovery_preparation_writer_and_preserves_sources() {
    let (store, source, mut event) = native_delivery_fixture().await;
    event.id = source.terminal_delivery_id();
    assert!(
        store
            .persist_cli_runtime_terminal_event(event.clone(), &source, "native-delivery-owner")
            .await
            .unwrap()
    );
    assert!(
        store
            .has_pending_cli_runtime_terminal_event(&source.turn_id)
            .await
            .unwrap()
    );
    let before = store
        .cli_runtime_turn_terminal_guard_by_id(&source.turn_id)
        .await
        .unwrap()
        .unwrap();
    let tx = store.database_connection().begin().await.unwrap();
    // The final writer check is necessary even when a coordinator's earlier
    // read saw no journal row and already dispatched its recovery request.
    assert!(
        prepare_recovery_attempt(
            &tx,
            source.turn_id.clone(),
            "receipt-recovery-attempt".into(),
            "receipt-recovery-job".into(),
            "receipt-recovery-run".into(),
            2,
            "restart must wait".into(),
            event.created_at,
            Some((
                source.clone(),
                event.id.clone(),
                "native-delivery-owner".into()
            )),
            "receipt-legacy-attempt".into(),
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        store
            .cli_runtime_turn_terminal_guard_by_id(&source.turn_id)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    assert!(
        store
            .get_cli_runtime_turn_attempt("receipt-recovery-attempt")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        native_events::find_native_event(&store.database_connection(), &event.id)
            .await
            .unwrap()
            .unwrap()
            .payload_redacted_json,
        event.payload_redacted_json
    );
}

#[pioneer_postgres_test::postgres_test]
async fn native_ack_replay_keeps_control_quiet_and_reinsert_wakes_its_turn() {
    let (store, source, event) = native_delivery_fixture().await;
    let database = store.database_connection();
    let mut changes = database.work_signal("gateway.control").subscribe();
    changes.begin_keyed_pass(&database).await.unwrap();
    let first = NativeEventRepository::new(&database)
        .append_cli_runtime_native_event_if_absent(event.clone())
        .await
        .unwrap();
    assert_eq!(
        changes.begin_keyed_pass(&database).await.unwrap(),
        Some(vec![
            GatewayControlWake::Activity(source.turn_id.clone()).key()
        ])
    );
    let duplicate = NativeEventRepository::new(&database)
        .append_cli_runtime_native_event_if_absent(event.clone())
        .await
        .unwrap();
    assert_eq!(duplicate, first);
    assert_eq!(
        changes.begin_keyed_pass(&database).await.unwrap(),
        Some(Vec::new())
    );
    pioneer_entity::cli_runtime_native_event::Entity::delete_by_id(event.id.clone())
        .exec(&database)
        .await
        .unwrap();
    NativeEventRepository::new(&database)
        .append_cli_runtime_native_event_if_absent(event)
        .await
        .unwrap();
    assert_eq!(
        changes.begin_keyed_pass(&database).await.unwrap(),
        Some(vec![GatewayControlWake::Activity(source.turn_id).key()])
    );
}
