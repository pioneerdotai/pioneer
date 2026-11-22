//! Recovery and publication behavior against the Pioneer PostgreSQL schema.
use pioneer_compaction::{
    ModelBudget,
    runner::{FailureKind, RunnerPhase, RunnerState},
};
use pioneer_crud::{CanonicalTurnEventPayload as Event, CrudStore, compaction::*};
use pioneer_entity::{
    compaction_lifecycle_pending as pending, compaction_lifecycle_scope as scope,
    compaction_lifecycle_sequence as sequence, compaction_runner_state as runner,
};
use pioneer_postgres_pools::{PostgresDatabase, PostgresWriteExecutor};
use pioneer_postgres_test::TestDatabase;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter, Statement, TransactionTrait,
};
use std::{sync::Arc, time::Duration};
const NOW: i64 = 2_000_000_000_000;

async fn open(fixture: &TestDatabase) -> (CrudStore, PostgresWriteExecutor) {
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let executor = PostgresWriteExecutor::new(writer);
    let reader = fixture.connect_read_only().await.unwrap();
    let db = PostgresDatabase::from_executor(reader, executor.clone());
    assert!(db.reader_is_read_only().await.unwrap());
    (CrudStore::new(db), executor)
}

async fn fixture() -> (Arc<TestDatabase>, CrudStore, PostgresWriteExecutor) {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let (store, writer) = open(&fixture).await;
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',TRUE,FALSE)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES ('owner','ws','thread',1)",
    ] {
        store
            .with_maintenance_access()
            .database_connection()
            .execute_unprepared(sql)
            .await
            .unwrap();
    }
    (fixture, store, writer)
}
async fn operation(store: &CrudStore, id: &str, status: &str, deadline: i64) {
    store.with_maintenance_access().database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,execution_turn,deadline_ms) VALUES($1,'owner',$2,$3, '{}','turn',$4)",
        [id.into(),id.into(),status.into(),deadline.into()])).await.unwrap();
}
async fn state(store: &CrudStore, id: &str) -> RunnerState {
    let state = RunnerState::new(
        NOW as u64,
        &ModelBudget::new(Some(128000), None, Some(16384)),
        1000,
        None,
    )
    .unwrap();
    store
        .with_maintenance_access()
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO compaction_runner_state(operation_id,generation,state) VALUES ($1,$2,$3::text::json)",
            [
                id.into(),
                (state.generation as i64).into(),
                serde_json::to_string(&state).unwrap().into(),
            ],
        ))
        .await
        .unwrap();
    state
}
async fn row(store: &CrudStore, id: &str) -> Option<pending::Model> {
    pending::Entity::find_by_id(id)
        .one(&store.with_maintenance_access().database_connection())
        .await
        .unwrap()
}
async fn scalar(store: &CrudStore, sql: &str) -> i64 {
    store
        .with_maintenance_access()
        .database_connection()
        .query_one_raw(Statement::from_string(DbBackend::Postgres, sql))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}
async fn claim(store: &CrudStore, id: &str, now: i64) -> CompactionLifecycleClaim {
    let candidate = row(store, id).await.unwrap();
    store
        .compaction_claim_lifecycle(&candidate, &|| now)
        .await
        .unwrap()
        .unwrap()
}
fn lifecycle_event(prepared: &PreparedCompactionLifecycle) -> Option<Event> {
    if !prepared.needs_publication() {
        return None;
    }
    let state = prepared.state().unwrap();
    let status = if matches!(state.phase, RunnerPhase::Applied { .. }) {
        "completed"
    } else if matches!(
        state.phase,
        RunnerPhase::Failed {
            kind: FailureKind::Cancelled
        }
    ) {
        "cancelled"
    } else {
        "failed"
    };
    let (workspace, thread, turn) = prepared.scope().unwrap();
    Some(Event::ItemCompleted(
        pioneer_protocol::ItemCompletedNotification {
            workspace_id: workspace.into(),
            thread_id: thread.into(),
            turn_id: turn.into(),
            item: pioneer_protocol::TurnItem::SystemEvent {
                id: format!("compaction:{}", prepared.operation_id()),
                code: Some("agent_context_compaction".into()),
                message: status.into(),
                level: pioneer_protocol::SystemEventLevel::Info,
                details: Some(serde_json::json!({"status":status})),
            },
        },
    ))
}
async fn apply(store: &CrudStore, id: &str, now: i64) -> bool {
    let claimed = claim(store, id, now).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, now)
        .await
        .unwrap()
        .unwrap();
    let event = lifecycle_event(&prepared);
    store
        .compaction_repair_lifecycle(prepared, event, now / 1000)
        .await
        .unwrap()
}
async fn expand_all(store: &CrudStore, now: i64) {
    for _ in 0..100 {
        let seed = store
            .compaction_expand_lifecycle_scope(true, &|| now)
            .await
            .unwrap();
        let scope = store
            .compaction_expand_lifecycle_scope(false, &|| now)
            .await
            .unwrap();
        assert!(seed + scope <= 8);
        if seed + scope == 0 {
            return;
        }
    }
    panic!("bounded fixture scopes must drain");
}
async fn replace_item_with_started_payload(
    db: &PostgresDatabase,
    compressed: bool,
    updated_at: Option<&str>,
) {
    let payload = if compressed {
        serde_json::json!({"details":{"status":"started"},"padding":"retained".repeat(4_096)})
    } else {
        serde_json::json!({"details":{"status":"started"}})
    };
    let prepared = compressed.then(|| {
        pioneer_crud_payloads::storage::PreparedPayload::new(&payload.to_string()).unwrap()
    });
    let tx = db.begin().await.unwrap();
    if let Some(prepared) = &prepared {
        prepared.insert(&tx).await.unwrap();
    }
    let result = tx
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE turn_item SET payload=$1::text::json, payload_payload_id=$2,
         updated_at=COALESCE($3::text::timestamptz,updated_at)
         WHERE item_id='compaction:op'",
            [
                (!compressed).then(|| payload.to_string()).into(),
                prepared
                    .as_ref()
                    .map(|payload| payload.id().to_owned())
                    .into(),
                updated_at.map(str::to_owned).into(),
            ],
        ))
        .await
        .unwrap();
    assert_eq!(result.rows_affected(), 1);
    tx.commit().await.unwrap();
}

async fn terminal_item(store: &CrudStore) -> pioneer_protocol::TurnItem {
    let item = store
        .get_turn_item("turn", "compaction:op")
        .await
        .unwrap()
        .unwrap();
    let pioneer_protocol::TurnItem::SystemEvent {
        details: Some(details),
        ..
    } = &item
    else {
        panic!("unexpected compaction projection: {item:?}");
    };
    assert_eq!(details["status"], "failed");
    item
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_stop_after_finish_preserves_terminal_deadline_classification() {
    let (_file, store, _) = fixture().await;
    operation(&store, "op", "running", NOW).await;
    state(&store, "op").await;
    assert!(apply(&store, "op", NOW).await); // finish before runner/publication
    store
        .compaction_stop_execution("ws", "thread", "owner", "turn")
        .await
        .unwrap();
    expand_all(&store, NOW).await;
    assert!(apply(&store, "op", NOW).await);
    assert!(row(&store, "op").await.is_none());
    assert_eq!(scalar(&store, "SELECT count(*) n FROM compaction_operation WHERE status='failed' AND outcome='deadline'").await, 1);
    assert!(matches!(
        store
            .compaction_runner_state("op")
            .await
            .unwrap()
            .unwrap()
            .phase,
        RunnerPhase::Failed {
            kind: FailureKind::Deadline
        }
    ));
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM turn_item WHERE item_id='compaction:op' AND status='completed'"
        )
        .await,
        1
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM turn WHERE status='completed'"
        )
        .await,
        1
    );
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_finish_poison_runner_and_restart_publication_pipeline() {
    let (file, store, _) = fixture().await;
    operation(&store, "poison", "running", NOW).await;
    let initial = state(&store, "poison").await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared(
        "UPDATE compaction_runner_state SET state=to_json('broken'::text) WHERE operation_id='poison'",
    )
    .await
    .unwrap();
    assert!(
        apply(&store, "poison", NOW).await,
        "poison runner cannot prevent timeout"
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_operation WHERE status='failed'"
        )
        .await,
        1
    );
    let claimed = claim(&store, "poison", NOW).await;
    assert!(
        store
            .compaction_prepare_lifecycle(&claimed, NOW)
            .await
            .is_err()
    );
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE compaction_runner_state SET state=$1::text::json WHERE operation_id='poison'",
        [serde_json::to_string(&initial).unwrap().into()],
    ))
    .await
    .unwrap();
    assert_eq!(row(&store, "poison").await.unwrap().due_at, NOW + 5_000);
    drop(db);
    drop(store);
    let (store, _) = open(&file).await;
    assert!(apply(&store, "poison", NOW + 5_000).await);
    assert!(row(&store, "poison").await.is_none());
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM turn_item WHERE item_id='compaction:poison'"
        )
        .await,
        1
    );
    assert!(matches!(
        store
            .compaction_runner_state("poison")
            .await
            .unwrap()
            .unwrap()
            .phase,
        RunnerPhase::Failed {
            kind: FailureKind::Deadline
        }
    ));
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_heartbeat_during_preparation_does_not_fence_equal_inputs() {
    let (_file, store, _) = fixture().await;
    operation(&store, "op", "failed", NOW).await;
    state(&store, "op").await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,active_attempt_number) VALUES('item','turn','compaction:op','system_event','in_progress','{\"details\":{\"status\":\"started\"}}',0)").await.unwrap();
    let claimed = claim(&store, "op", NOW).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW)
        .await
        .unwrap()
        .unwrap();
    let event = lifecycle_event(&prepared);
    let before = row(&store, "op").await.unwrap();
    for sql in [
        "UPDATE turn SET updated_at=CURRENT_TIMESTAMP + INTERVAL '1 second' WHERE id='turn'",
        "UPDATE thread SET preview='heartbeat',updated_at=CURRENT_TIMESTAMP + INTERVAL '1 second' WHERE id='thread'",
        "UPDATE turn_item SET last_heartbeat_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP + INTERVAL '1 second' WHERE id='item'",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    assert_eq!(row(&store, "op").await.unwrap(), before);
    assert!(
        store
            .compaction_repair_lifecycle(prepared, event, NOW / 1000)
            .await
            .unwrap()
    );
    assert!(row(&store, "op").await.is_none());
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM turn_item WHERE item_id='compaction:op' AND status='completed'"
        )
        .await,
        1
    );
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_publication_projection_ack_and_post_trigger_generation_are_atomic() {
    let (_file, store, _) = fixture().await;
    operation(&store, "op", "failed", NOW).await;
    let initial = state(&store, "op").await;
    let claimed = claim(&store, "op", NOW).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW)
        .await
        .unwrap()
        .unwrap();
    let event = lifecycle_event(&prepared);
    let before = row(&store, "op").await.unwrap();
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("CREATE FUNCTION reject_lifecycle_projection() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'projection rollback'; END; $$;
        CREATE TRIGGER reject_lifecycle_projection BEFORE INSERT ON turn_item FOR EACH ROW WHEN (NEW.item_id='compaction:op') EXECUTE FUNCTION reject_lifecycle_projection()").await.unwrap();
    assert!(
        store
            .compaction_repair_lifecycle(prepared, event, NOW / 1000)
            .await
            .is_err()
    );
    assert_eq!(row(&store, "op").await.unwrap(), before);
    assert_eq!(
        runner::Entity::find_by_id("op")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .generation,
        initial.generation as i64
    );
    for table in [
        "turn_event",
        "turn_event_projection_state",
        "turn_event_delivery",
    ] {
        assert_eq!(
            scalar(&store, &format!("SELECT count(*) n FROM {table}")).await,
            0
        );
    }
    db.execute_unprepared("DROP TRIGGER reject_lifecycle_projection ON turn_item")
        .await
        .unwrap();
    assert!(apply(&store, "op", NOW + 5_000).await);
    assert!(
        row(&store, "op").await.is_none(),
        "own runner/item triggers must not strand post-repair debt"
    );
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 1);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM turn_event_projection_state WHERE status='projected'"
        )
        .await,
        1
    );
    // Retried old handler must not wipe any later generation.
    db.execute_unprepared("DELETE FROM turn_item WHERE item_id='compaction:op'")
        .await
        .unwrap();
    let new = row(&store, "op").await.unwrap();
    assert!(new.generation > before.generation);
    assert!(
        store
            .compaction_prepare_lifecycle(&claimed, NOW + 5_000)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(row(&store, "op").await.unwrap(), new);
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_event_driven_publication_overtakes_worker_with_fresh_atomic_ack() {
    let (_file, store, _) = fixture().await;
    operation(&store, "op", "failed", NOW).await;
    state(&store, "op").await;
    store.compaction_reconcile_runner_state("op").await.unwrap();
    let claimed = claim(&store, "op", NOW).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW)
        .await
        .unwrap()
        .unwrap();
    let event = lifecycle_event(&prepared).unwrap();
    let generation = prepared.state().unwrap().generation;
    store
        .compaction_materialize_lifecycle("op", generation, event.clone(), NOW / 1000)
        .await
        .unwrap();
    assert!(row(&store, "op").await.is_none());
    assert!(
        !store
            .compaction_repair_lifecycle(prepared, Some(event), NOW / 1000)
            .await
            .unwrap()
    );
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 1);
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_physical_plain_compressed_delete_replay_and_storage_switch() {
    for compressed in [false, true] {
        let (_fixture, store, _) = fixture().await;
        operation(&store, "op", "failed", NOW).await;
        state(&store, "op").await;
        let db = store.with_maintenance_access().database_connection();
        db.execute_unprepared("INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,active_attempt_number,created_at,updated_at) VALUES ('item','turn','compaction:op','system_event','in_progress','{\"details\":{\"status\":\"started\"}}',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        assert!(apply(&store, "op", NOW).await);
        assert!(row(&store, "op").await.is_none());

        let event_count = scalar(&store, "SELECT count(*) n FROM turn_event").await;
        replace_item_with_started_payload(&db, compressed, None).await;
        let compressed_items = scalar(&store,
            "SELECT count(*) n FROM turn_item i JOIN portable_payload_value p ON p.id=i.payload_payload_id WHERE i.item_id='compaction:op' AND p.codec=1"
        ).await;
        assert_eq!(compressed_items, i64::from(compressed));
        assert!(row(&store, "op").await.is_some());
        assert!(apply(&store, "op", NOW).await);
        assert_eq!(
            scalar(&store, "SELECT count(*) n FROM turn_event").await,
            event_count,
            "replay repairs projections without duplicate append"
        );
        let repaired = terminal_item(&store).await;
        db.execute_unprepared("DELETE FROM turn_item WHERE item_id='compaction:op'")
            .await
            .unwrap();
        assert!(row(&store, "op").await.is_some());
        assert!(apply(&store, "op", NOW).await);
        assert_eq!(
            scalar(
                &store,
                "SELECT count(*) n FROM turn_item WHERE item_id='compaction:op'"
            )
            .await,
            1
        );
        assert_eq!(terminal_item(&store).await, repaired);
        assert_eq!(
            scalar(&store, "SELECT count(*) n FROM turn_event").await,
            event_count
        );
        let deliveries = scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await;
        assert!(row(&store, "op").await.is_none());
        replace_item_with_started_payload(&db, !compressed, None).await;
        assert!(apply(&store, "op", NOW).await);
        assert_eq!(
            scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await,
            deliveries
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_late_runner_context_turn_and_source_deletion() {
    let (_file, store, _) = fixture().await;
    operation(&store, "op", "failed", NOW).await;
    assert!(apply(&store, "op", NOW).await);
    assert!(row(&store, "op").await.is_none());
    state(&store, "op").await;
    assert!(
        row(&store, "op").await.is_some(),
        "late runner is exact INSERT coverage"
    );
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("DELETE FROM compaction_context WHERE owner='owner'")
        .await
        .unwrap();
    assert!(row(&store, "op").await.is_none());
    assert_eq!(
        scalar(&store, "SELECT count(*) n FROM compaction_operation").await,
        0
    );
    assert_eq!(
        scalar(&store, "SELECT count(*) n FROM compaction_runner_state").await,
        0
    );
    db.execute_unprepared("INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('owner','ws','thread',1)").await.unwrap();
    operation(&store, "op", "failed", NOW).await;
    state(&store, "op").await;
    expand_all(&store, NOW).await;
    db.execute_unprepared("DELETE FROM turn WHERE id='turn'")
        .await
        .unwrap();
    assert!(apply(&store, "op", NOW).await);
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn").await, 0);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_turn_creation WHERE turn_id='turn'"
        )
        .await,
        0
    );
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    expand_all(&store, NOW).await;
    assert!(apply(&store, "op", NOW).await);
    assert!(row(&store, "op").await.is_none());
    db.execute_unprepared("DELETE FROM compaction_runner_state WHERE operation_id='op'")
        .await
        .unwrap();
    assert!(row(&store, "op").await.is_some());
    assert!(apply(&store, "op", NOW).await);
    db.execute_unprepared("DELETE FROM compaction_operation WHERE id='op'")
        .await
        .unwrap();
    assert!(row(&store, "op").await.is_none());
    operation(&store, "op", "failed", NOW).await;
    let new = row(&store, "op").await.unwrap();
    assert!(
        new.generation > 1,
        "delete/reinsert cannot ABA the sequence"
    );
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_scope_pages_are_restart_safe() {
    let (fixture, store, writer) = fixture().await;
    for i in 0..19 {
        operation(&store, &format!("active-{i:02}"), "running", NOW + 60_000).await;
        let id = format!("terminal-{i:02}");
        operation(&store, &id, "failed", NOW).await;
        state(&store, &id).await;
    }
    expand_all(&store, NOW).await;
    let mut seen = std::collections::HashSet::new();
    loop {
        let batch = store.compaction_due_lifecycle(NOW).await.unwrap();
        assert!(batch.len() <= 8);
        if batch.is_empty() {
            break;
        }
        for candidate in batch {
            assert!(seen.insert(candidate.operation_id.clone()));
            assert!(apply(&store, &candidate.operation_id, NOW).await);
        }
    }
    assert_eq!(seen.len(), 38);
    drop(store);
    drop(writer);
    let (store, _) = open(&fixture).await;
    assert!(
        store
            .compaction_due_lifecycle(NOW)
            .await
            .unwrap()
            .is_empty()
    );
    // One Turn change creates one coalesced scope; no foreground fanout.
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("UPDATE turn SET status='interrupted' WHERE id='turn'")
        .await
        .unwrap();
    let before = scalar(
        &store,
        "SELECT generation n FROM compaction_lifecycle_sequence",
    )
    .await;
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_lifecycle_scope WHERE kind='turn'"
        )
        .await,
        1
    );
    assert!(
        store
            .compaction_due_lifecycle(NOW)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .compaction_expand_lifecycle_scope(false, &|| NOW)
            .await
            .unwrap(),
        4
    );
    let old = scope::Entity::find()
        .filter(scope::Column::Kind.eq("turn".to_owned()))
        .filter(scope::Column::Owner.eq(String::new()))
        .filter(scope::Column::TurnId.eq("turn".to_owned()))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    db.execute_unprepared(
        "UPDATE turn SET status='cancelled',updated_at=updated_at WHERE id='turn'",
    )
    .await
    .unwrap();
    let fresh = scope::Entity::find()
        .filter(scope::Column::Kind.eq("turn".to_owned()))
        .filter(scope::Column::Owner.eq(String::new()))
        .filter(scope::Column::TurnId.eq("turn".to_owned()))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert!(fresh.generation > old.generation && fresh.generation > before);
    assert!(fresh.cursor_id.is_none() && fresh.upper_id.is_none());
    expand_all(&store, NOW).await;
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_lifecycle_pending WHERE eligible_at=0"
        )
        .await,
        38
    );
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_stop_rebind_covers_old_and_new_scope_and_turn_binding_fences_commit() {
    let (_file, store, _) = fixture().await;
    operation(&store, "op", "running", NOW + 60_000).await;
    expand_all(&store, NOW).await;
    assert!(apply(&store, "op", NOW).await);
    let db = store.with_maintenance_access().database_connection();
    // Direct physical DML must cover replay without Gateway hooks.
    db.execute_unprepared(
        "INSERT INTO compaction_execution_stop(owner,turn_id) VALUES('owner','turn')",
    )
    .await
    .unwrap();
    expand_all(&store, NOW).await;
    let claimed = claim(&store, "op", NOW).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW)
        .await
        .unwrap()
        .unwrap();
    db.execute_unprepared("INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('other-owner','ws','thread',1)").await.unwrap();
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin) VALUES('other-turn','thread','completed','conversation','user')").await.unwrap();
    db.execute_unprepared("UPDATE compaction_execution_stop SET owner='other-owner',turn_id='other-turn' WHERE owner='owner' AND turn_id='turn'").await.unwrap();
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_lifecycle_scope WHERE kind='owner_turn'"
        )
        .await,
        2
    );
    assert!(
        !store
            .compaction_repair_lifecycle(prepared, None, NOW / 1000)
            .await
            .unwrap()
    );
    expand_all(&store, NOW).await;
    assert_eq!(
        row(&store, "op").await.unwrap().retry_not_before,
        NOW + 5_000
    );
    assert!(apply(&store, "op", NOW + 5_000).await);
    assert_eq!(row(&store, "op").await.unwrap().due_at, NOW + 60_000);
    db.execute_unprepared("UPDATE turn SET status='interrupted' WHERE id='turn'")
        .await
        .unwrap();
    expand_all(&store, NOW + 5_000).await;
    let claimed = claim(&store, "op", NOW + 5_000).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW + 5_000)
        .await
        .unwrap()
        .unwrap();
    db.execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class) VALUES('rebound','ws','','agent','m','p','active','user','workspace')").await.unwrap();
    db.execute_unprepared("UPDATE turn SET thread_id='rebound' WHERE id='turn'")
        .await
        .unwrap();
    assert!(
        !store
            .compaction_repair_lifecycle(prepared, None, NOW / 1000)
            .await
            .unwrap()
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_operation WHERE status='running'"
        )
        .await,
        1
    );
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 0);
    // Binding changes coalesce, and a missing matching context never gets
    // fabricated by recovery. Its next claim remains durable retry work.
    expand_all(&store, NOW + 10_000).await;
    let claimed = claim(&store, "op", NOW + 10_000).await;
    assert!(
        store
            .compaction_prepare_lifecycle(&claimed, NOW + 10_000)
            .await
            .is_err()
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_context WHERE thread_id='rebound'"
        )
        .await,
        0
    );
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_repeated_started_keeps_one_attempt_and_completed_finishes_it() {
    use pioneer_entity::{turn_item, turn_item_attempt};
    use sea_orm::{ColumnTrait, QueryFilter};
    let (_file, store, _) = fixture().await;
    operation(&store, "op", "running", NOW + 60_000).await;
    let running = state(&store, "op").await;
    use futures_util::FutureExt;
    let signal_database = store.database_connection();
    let mut consumers = ["projection", "maintenance.zstd", "maintenance.receipts"]
        .map(|domain| signal_database.work_signal(domain).subscribe());
    for consumer in &mut consumers {
        consumer.begin_pass(&signal_database).await.unwrap();
    }
    let started = Event::ItemStarted(pioneer_protocol::ItemStartedNotification {
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item: pioneer_protocol::TurnItem::SystemEvent {
            id: "compaction:op".into(),
            code: Some("agent_context_compaction".into()),
            message: "started".into(),
            level: pioneer_protocol::SystemEventLevel::Info,
            details: Some(serde_json::json!({"status":"started"})),
        },
    });
    store
        .compaction_materialize_lifecycle("op", running.generation, started.clone(), NOW / 1000)
        .await
        .unwrap();
    for consumer in &mut consumers {
        assert!(
            consumer.changed().now_or_never().is_some(),
            "published service item wakes durable event owners"
        );
        consumer.begin_pass(&signal_database).await.unwrap();
    }
    let db = store.with_maintenance_access().database_connection();
    let first = turn_item::Entity::find()
        .filter(turn_item::Column::ItemId.eq("compaction:op"))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let deliveries = scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await;
    assert!(deliveries > 0);
    store
        .compaction_materialize_lifecycle("op", running.generation, started, NOW / 1000 + 100)
        .await
        .unwrap();
    for consumer in &mut consumers {
        assert!(
            consumer.changed().now_or_never().is_none(),
            "idempotent started replay has no new event work"
        );
    }
    let repeated = turn_item::Entity::find_by_id(&first.id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repeated, first);
    assert_eq!(repeated.active_attempt_number, 1);
    assert!(repeated.active_attempt_id.is_some());
    assert_eq!(
        scalar(&store, "SELECT count(*) n FROM turn_item_attempt").await,
        1
    );
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 1);
    assert_eq!(
        scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await,
        deliveries
    );
    store
        .compaction_finish("op", "failed", "deadline")
        .await
        .unwrap();
    assert!(apply(&store, "op", NOW + 100_000).await);
    let finished = turn_item_attempt::Entity::find_by_id(first.active_attempt_id.unwrap())
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    // SystemEvent completion uses the existing completed attempt semantics.
    assert_eq!(finished.status, "completed");
    assert_eq!(
        scalar(&store, "SELECT count(*) n FROM turn_item_attempt").await,
        1
    );
    let deliveries = scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await;
    db.execute_unprepared("DELETE FROM turn_item WHERE item_id='compaction:op'")
        .await
        .unwrap();
    assert!(apply(&store, "op", NOW + 200_000).await);
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 2);
    assert_eq!(
        scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await,
        deliveries
    );
    assert_eq!(
        scalar(&store, "SELECT count(*) n FROM turn_item_attempt").await,
        1
    );
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_terminal_repair_uses_saved_event_time_and_matches_canonical_replay() {
    use pioneer_entity::{turn_event, turn_item};
    use sea_orm::{ColumnTrait, QueryFilter};
    for compressed in [false, true] {
        let (_file, store, _) = fixture().await;
        operation(&store, "op", "failed", NOW).await;
        state(&store, "op").await;
        let t1 = NOW;
        let t2 = NOW + 600_000;
        assert!(apply(&store, "op", t1).await);
        let db = store.with_maintenance_access().database_connection();
        let saved = turn_event::Entity::find().one(&db).await.unwrap().unwrap();
        assert_eq!(saved.created_at.timestamp(), t1 / 1000);
        let original = turn_item::Entity::find()
            .filter(turn_item::Column::ItemId.eq("compaction:op"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original.created_at, saved.created_at);
        assert_eq!(original.updated_at, saved.created_at);
        let deliveries = scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await;
        let event: Event = if let Some(locator) = saved.payload_payload_id.as_deref() {
            use pioneer_crud_payloads::storage::{Selection, capture};

            let payload = capture(&db, locator, Selection::Full)
                .await
                .unwrap()
                .expect("lifecycle event payload");
            serde_json::from_slice(&payload.into_bytes().unwrap()).unwrap()
        } else {
            serde_json::from_value(
                saved
                    .payload
                    .clone()
                    .expect("inline lifecycle event payload"),
            )
            .unwrap()
        };
        let generation = store
            .compaction_runner_state("op")
            .await
            .unwrap()
            .unwrap()
            .generation;
        store
            .compaction_materialize_lifecycle("op", generation, event, t2 / 1000)
            .await
            .unwrap();
        assert_eq!(
            turn_item::Entity::find_by_id(&original.id)
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            original
        );
        let expected_body = terminal_item(&store).await;
        // Damage both the item data and projection time, then repair at T2.
        replace_item_with_started_payload(&db, compressed, Some("2090-01-01 00:00:00+00:00")).await;
        assert!(apply(&store, "op", t2).await);
        let repaired = turn_item::Entity::find_by_id(&original.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(repaired.portable_revision > original.portable_revision);
        assert_eq!(
            repaired,
            turn_item::Model {
                portable_revision: repaired.portable_revision,
                payload_payload_id: repaired.payload_payload_id.clone(),
                ..original.clone()
            }
        );
        assert_eq!(terminal_item(&store).await, expected_body);
        db.execute_unprepared("DELETE FROM turn_item WHERE item_id='compaction:op'")
            .await
            .unwrap();
        assert!(apply(&store, "op", t2 + 60_000).await);
        let restored = turn_item::Entity::find()
            .filter(turn_item::Column::ItemId.eq("compaction:op"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal_item(&store).await, expected_body);
        assert_eq!(restored.status, original.status);
        assert_eq!(restored.created_at, saved.created_at);
        assert_eq!(restored.updated_at, saved.created_at);
        assert_eq!(
            turn_event::Entity::find_by_id(&saved.id)
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            saved
        );
        assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 1);
        assert_eq!(
            scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await,
            deliveries
        );
        // Rebuild through the ordinary event replay path, which uses the saved
        // event's time. Compare the actual projection, excluding random row ID.
        db.execute_unprepared("DELETE FROM turn_item WHERE item_id='compaction:op'")
            .await
            .unwrap();
        db.execute_unprepared("UPDATE turn_event_projection_state SET status='pending',claim_token=NULL,claim_expires_at=NULL WHERE turn_id='turn'; UPDATE turn_event_projection_stream_state SET projected_through_sequence=0 WHERE turn_id='turn'").await.unwrap();
        let replay = store
            .replay_due_turn_event_projections(t2 / 1000 + 120, 1)
            .await
            .unwrap();
        assert_eq!(replay.claimed, 1);
        assert_eq!(replay.projected, 1);
        let canonical = turn_item::Entity::find()
            .filter(turn_item::Column::ItemId.eq("compaction:op"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal_item(&store).await, expected_body);
        assert_eq!(canonical.status, restored.status);
        assert_eq!(canonical.created_at, restored.created_at);
        assert_eq!(canonical.updated_at, restored.updated_at);
        assert_eq!(
            scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await,
            deliveries
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_terminal_repair_fences_canonical_event_snapshot_and_rolls_back() {
    use pioneer_entity::turn_event;
    let (_file, store, _) = fixture().await;
    operation(&store, "op", "failed", NOW).await;
    let running = state(&store, "op").await;
    assert!(apply(&store, "op", NOW).await);
    let db = store.with_maintenance_access().database_connection();
    let saved = turn_event::Entity::find().one(&db).await.unwrap().unwrap();
    // Reconcile will update runner inside the domain transaction. Simulate an
    // event replacement after projection preparation but before publication.
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE compaction_runner_state SET generation=$1,state=$2::text::json WHERE operation_id='op'",
        [
            (running.generation as i64).into(),
            serde_json::to_string(&running).unwrap().into(),
        ],
    ))
    .await
    .unwrap();
    db.execute_unprepared("DELETE FROM turn_item WHERE item_id='compaction:op'")
        .await
        .unwrap();
    let claimed = claim(&store, "op", NOW + 600_000).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW + 600_000)
        .await
        .unwrap()
        .unwrap();
    let event = lifecycle_event(&prepared);
    let before = row(&store, "op").await.unwrap();
    let runner_before = runner::Entity::find_by_id("op")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    db.execute_unprepared("CREATE FUNCTION change_event_during_repair() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN UPDATE turn_event SET created_at='2090-01-01 00:00:00+00:00'; RETURN NEW; END; $$;
        CREATE TRIGGER change_event_during_repair AFTER UPDATE ON compaction_runner_state FOR EACH ROW EXECUTE FUNCTION change_event_during_repair()").await.unwrap();
    assert!(
        store
            .compaction_repair_lifecycle(prepared, event, NOW / 1000 + 600)
            .await
            .is_err()
    );
    assert_eq!(
        turn_event::Entity::find_by_id(&saved.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        saved
    );
    assert_eq!(
        runner::Entity::find_by_id("op")
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        runner_before
    );
    assert_eq!(row(&store, "op").await.unwrap(), before);
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_item").await, 0);
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 1);
    db.execute_unprepared("DROP TRIGGER change_event_during_repair ON compaction_runner_state")
        .await
        .unwrap();
    assert!(apply(&store, "op", before.retry_not_before).await);
}
