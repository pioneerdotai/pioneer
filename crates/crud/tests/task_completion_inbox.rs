//! CRUD integration coverage for the completed-run inbox and access boundaries.
use pioneer_crud::{
    CrudStore, TaskCompletionInboxAccess, count_unread_task_completion_inbox,
    find_task_completion_inbox_entry, list_task_completion_inbox, mark_task_completion_inbox_read,
    resolve_task_authorization_scope,
};
use pioneer_postgres_test::{TestDatabase, scoped_database};
use pioneer_sqlite::{SqliteDatabase, SqliteWriteExecutor};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter, Statement, TransactionTrait,
};
use std::sync::Arc;

const A: &str = "P0000000000000000000A";
const B: &str = "P0000000000000000000B";

async fn open(fixture: &TestDatabase) -> (CrudStore, SqliteWriteExecutor) {
    let writer = SqliteWriteExecutor::new(fixture.connect_with_limit(1).await.unwrap());
    let reader = fixture.connect_read_only().await.unwrap();
    let db = SqliteDatabase::from_executor(reader, writer.clone());
    assert!(db.reader_query_only_enabled().await.unwrap());
    (CrudStore::new(db), writer)
}

async fn fixture() -> (Arc<TestDatabase>, CrudStore, SqliteWriteExecutor) {
    let fixture = scoped_database().await.unwrap();
    let (store, writer) = open(&fixture).await;
    let db = store.with_maintenance_access().database_connection();
    pioneer_crud_tasks::test_support::completion_inbox::seed(&db).await;
    (fixture, store, writer)
}
fn access(principal: &str) -> TaskCompletionInboxAccess<'_> {
    TaskCompletionInboxAccess {
        workspace_id: "W0000000000000000000I",
        principal_id: principal,
        absolute: false,
    }
}
async fn task(
    store: &CrudStore,
    id: &str,
    thread: Option<&str>,
    root: Option<&str>,
    executor: &str,
    mode: &str,
    attachment: &str,
) {
    let db = store.with_maintenance_access().database_connection();
    pioneer_crud_tasks::test_support::completion_inbox::task(
        &db, id, thread, root, executor, mode, attachment,
    )
    .await;
}

async fn run(store: &CrudStore, id: &str, task: &str, status: &str, number: i64) {
    let db = store.with_maintenance_access().database_connection();
    pioneer_crud_tasks::test_support::completion_inbox::run(&db, id, task, status, number).await;
}

async fn unread(store: &CrudStore, principal: &str) -> u64 {
    count_unread_task_completion_inbox(&store.database_connection(), &access(principal))
        .await
        .unwrap()
}
async fn read(store: &CrudStore, principal: &str, run: &str) {
    mark_task_completion_inbox_read(
        &store.database_connection(),
        &access(principal),
        run,
        "2026-10-16 11:00:00 +00:00",
    )
    .await
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn internal_origin_turn_provenance_and_admission_use_the_same_task_scope() {
    let (_dir, store, _writer) = fixture().await;
    let db = store.database_connection();
    db.execute_unprepared("INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES('T0000000000000000000I','T0000000000000000000V','T0000000000000000000V',1,CURRENT_TIMESTAMP)").await.unwrap();
    task(
        &store,
        "causal",
        Some("T0000000000000000000I"),
        None,
        "system",
        "none",
        "detached",
    )
    .await;
    run(&store, "causal-run", "causal", "cancelled", 1).await;
    let scope =
        resolve_task_authorization_scope(&db, "causal", Some("W0000000000000000000I"), None)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        scope.root_thread_id.as_deref(),
        Some("T0000000000000000000V")
    );
    assert_eq!(
        find_task_completion_inbox_entry(&db, &access(A), "causal-run")
            .await
            .unwrap()
            .unwrap()
            .thread_id,
        "T0000000000000000000V"
    );
    db.execute_unprepared("INSERT INTO task_execution_admission(task_id,workspace_id,root_thread_id,initiating_principal_id,authorization_context_json,created_at) VALUES('causal','W0000000000000000000I','T0000000000000000000P','P0000000000000000000A','{}',CURRENT_TIMESTAMP)").await.unwrap();
    assert!(
        resolve_task_authorization_scope(&db, "causal", None, None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        find_task_completion_inbox_entry(&db, &access(A), "causal-run")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(unread(&store, A).await, 0);
    db.execute_unprepared("DELETE FROM task_execution_admission WHERE task_id='causal'")
        .await
        .unwrap();
    // A creator turn is authoritative even without created_by_thread_id, and
    // contradicting that turn cannot redirect the inbox to a public thread.
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('creator','T0000000000000000000V','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    task(&store, "turn-only", None, None, "tool", "none", "attached").await;
    db.execute_unprepared("UPDATE task SET created_by_turn_id='creator' WHERE id='turn-only'")
        .await
        .unwrap();
    run(&store, "turn-run", "turn-only", "failed", 1).await;
    assert_eq!(
        find_task_completion_inbox_entry(&db, &access(B), "turn-run")
            .await
            .unwrap()
            .unwrap()
            .thread_id,
        "T0000000000000000000V"
    );
    db.execute_unprepared(
        "UPDATE task SET created_by_thread_id='T0000000000000000000P' WHERE id='turn-only'",
    )
    .await
    .unwrap();
    assert!(
        resolve_task_authorization_scope(&db, "turn-only", None, None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        find_task_completion_inbox_entry(&db, &access(A), "turn-run")
            .await
            .unwrap()
            .is_none()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn cancellation_receipt_tracks_actor_exact_transition_and_rolls_back_with_projection() {
    use pioneer_protocol::TaskEventPayload;
    let (_dir, store, _writer) = fixture().await;
    task(
        &store,
        "cancel",
        Some("T0000000000000000000P"),
        None,
        "tool",
        "none",
        "detached",
    )
    .await;
    store
        .database_connection()
        .execute_unprepared("UPDATE task SET status='running' WHERE id='cancel'")
        .await
        .unwrap();
    run(&store, "previous", "cancel", "succeeded", 1).await;
    let cancel = |id: &str| TaskEventPayload::RunCancelled {
        task_id: "cancel".into(),
        run_id: id.into(),
        reason: None,
        cancelled_at: 1792144800,
    };
    for (id, number, actor) in [
        ("own", 2, Some(A)),
        ("other", 3, Some(B)),
        ("system", 4, None),
    ] {
        run(&store, id, "cancel", "running", number).await;
        store
            .append_task_cancellation_events_as(
                vec![cancel(id)],
                1792144800,
                None,
                actor.map(str::to_owned),
            )
            .await
            .unwrap();
    }
    assert_eq!(unread(&store, A).await, 3);
    assert_eq!(unread(&store, B).await, 3);
    // Replaying another actor's cancellation must not suppress it for A.
    store
        .append_task_cancellation_events_as(vec![cancel("other")], 1792144800, None, Some(A.into()))
        .await
        .unwrap();
    assert_eq!(unread(&store, A).await, 3);
    run(&store, "rollback", "cancel", "running", 5).await;
    let db = store.database_connection();
    db.execute_unprepared("CREATE FUNCTION reject_inbox_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture receipt failure'; END; $$; CREATE TRIGGER reject_inbox_receipt BEFORE INSERT ON task_run_read FOR EACH ROW EXECUTE FUNCTION reject_inbox_receipt()").await.unwrap();
    assert!(
        store
            .append_task_cancellation_events_as(
                vec![cancel("rollback")],
                1792144800,
                None,
                Some(A.into())
            )
            .await
            .is_err()
    );
    let row = pioneer_entity::task_run::Entity::find_by_id("rollback")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, "running");
    let clock = pioneer_entity::task_inbox_activation::Entity::find_by_id(1)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        clock.completion_sequence, 3,
        "failed Stop receipt rolls back its completion watermark"
    );
    db.execute_unprepared("DROP TRIGGER reject_inbox_receipt ON task_run_read")
        .await
        .unwrap();
    store
        .append_task_cancellation_events_as(
            vec![cancel("rollback")],
            1792144800,
            None,
            Some(A.into()),
        )
        .await
        .unwrap();
    assert_eq!(unread(&store, A).await, 3);
    assert_eq!(unread(&store, B).await, 4);
    run(&store, "future-repeat", "cancel", "succeeded", 6).await;
    assert_eq!(
        unread(&store, A).await,
        4,
        "cancellation cannot pre-read future repeats"
    );
}

async fn saved_result_item(store: &CrudStore, turn: &str, item: &str) {
    store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,active_attempt_number,created_at,updated_at) VALUES($1,$2,$3,'agent_message','completed','{}',1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [format!("saved-{turn}-{item}").into(), turn.into(), item.into()])).await.unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn own_cancellation_receipt_remains_personal_when_a_presentation_root_is_added_later() {
    let (_dir, store, _writer) = fixture().await;
    task(
        &store,
        "no-root-cancel",
        None,
        None,
        "system",
        "none",
        "detached",
    )
    .await;
    store
        .database_connection()
        .execute_unprepared("UPDATE task SET status='running' WHERE id='no-root-cancel'")
        .await
        .unwrap();
    run(&store, "no-root-run", "no-root-cancel", "running", 1).await;
    store
        .append_task_cancellation_events_as(
            vec![pioneer_protocol::TaskEventPayload::RunCancelled {
                task_id: "no-root-cancel".into(),
                run_id: "no-root-run".into(),
                reason: None,
                cancelled_at: 1792144800,
            }],
            1792144800,
            None,
            Some(A.into()),
        )
        .await
        .unwrap();
    let db = store.database_connection();
    assert!(
        pioneer_entity::task_run_read::Entity::find()
            .filter(pioneer_entity::task_run_read::Column::PrincipalId.eq(A))
            .filter(pioneer_entity::task_run_read::Column::RunId.eq("no-root-run"))
            .one(&db)
            .await
            .unwrap()
            .is_some()
    );
    db.execute_unprepared(
        "UPDATE task SET created_by_thread_id='T0000000000000000000P' WHERE id='no-root-cancel'",
    )
    .await
    .unwrap();
    assert_eq!(unread(&store, A).await, 0);
    assert_eq!(unread(&store, B).await, 1);
}

#[pioneer_postgres_test::postgres_test]
async fn ordinary_negative_batches_keep_viewport_scope_checks_and_direct_run_evidence() {
    use pioneer_crud::mark_viewed_task_completion_inbox_read as viewed;
    let (_dir, store, _writer) = fixture().await;
    task(
        &store,
        "batch",
        Some("T0000000000000000000P"),
        None,
        "system",
        "none",
        "detached",
    )
    .await;
    run(&store, "batch-run", "batch", "succeeded", 1).await;
    let db = store.database_connection();
    for ix in 0..100 {
        let turn = format!("ordinary-{ix}");
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO turn(id,thread_id,status,prompt_manifest_json,turn_kind,origin,created_at,updated_at) VALUES($1,'T0000000000000000000P','completed','{}','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            [turn.clone().into()])).await.unwrap();
        saved_result_item(&store, &turn, "final").await;
    }
    let mut params = pioneer_protocol::TaskCompletionInboxViewedParams {
        workspace_id: access(A).workspace_id.into(),
        thread_id: "T0000000000000000000P".into(),
        run_ids: vec![],
        result_turn_ids: vec![],
        result_items: (0..100)
            .map(|i| pioneer_protocol::TaskCompletionInboxViewedResult {
                turn_id: format!("ordinary-{i}"),
                item_id: "final".into(),
            })
            .collect(),
    };
    let at = "2026-10-18T12:00:00+00:00";
    assert!(
        viewed(&db, &access(A), &params, at)
            .await
            .unwrap()
            .is_empty()
    );
    params.result_turn_ids = params
        .result_items
        .drain(..)
        .map(|item| item.turn_id)
        .collect();
    assert!(
        viewed(&db, &access(A), &params, at)
            .await
            .unwrap()
            .is_empty()
    );
    params.thread_id = "nonexistent".into();
    assert!(
        viewed(&db, &access(A), &params, at).await.is_err(),
        "negative prefilter cannot bypass real viewport-thread validation"
    );
    params.thread_id = "T0000000000000000000P".into();
    params.result_turn_ids.pop();
    params.run_ids.push("batch-run".into());
    assert_eq!(
        viewed(&db, &access(A), &params, at).await.unwrap(),
        ["run:batch-run"]
    );
    assert_eq!(unread(&store, A).await, 0);
    assert_eq!(unread(&store, B).await, 1);
}

/// The three released identities survive source consolidation. Upgrade each
/// possible released prefix, including no inbox migration, without replaying it.

const ACCESS_SECOND: i64 = 1792144800;
fn grant_time() -> chrono::DateTime<chrono::FixedOffset> {
    chrono::DateTime::parse_from_rfc3339("2026-10-16T10:00:00.999999999Z").unwrap()
}
async fn grant_workspace(store: &CrudStore) -> pioneer_entity::workspace_membership::Model {
    let db = store.database_connection();
    let tx = db.begin().await.unwrap();
    let grant = pioneer_crud::insert_workspace_membership(
        &tx,
        &pioneer_crud::NewWorkspaceMembership {
            gateway_id: pioneer_protocol::GatewayId::new("G00000000000000000001").unwrap(),
            principal_id: pioneer_protocol::PrincipalId::new(A).unwrap(),
            workspace_id: access(A).workspace_id.into(),
            granted_by: pioneer_protocol::PersistedActorRef::System,
            now: grant_time(),
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    grant
}
async fn revoke_workspace(store: &CrudStore) {
    let db = store.database_connection();
    let tx = db.begin().await.unwrap();
    pioneer_crud::delete_workspace_membership(
        &tx,
        &pioneer_protocol::GatewayId::new("G00000000000000000001").unwrap(),
        &pioneer_protocol::PrincipalId::new(A).unwrap(),
        access(A).workspace_id,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}
async fn complete_for_access(store: &CrudStore, id: &str, task_id: &str, at: i64) {
    store
        .append_task_event(
            pioneer_protocol::TaskEventPayload::RunCompleted {
                task_id: task_id.into(),
                run_id: id.into(),
                result: None,
                completed_at: at,
            },
            at,
        )
        .await
        .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn current_workspace_grant_orders_same_second_completions_and_survives_devices_regrant() {
    let (dir, store, writer) = fixture().await;
    task(
        &store,
        "access",
        Some("T0000000000000000000P"),
        None,
        "tool",
        "none",
        "detached",
    )
    .await;
    let db = store.database_connection();
    db.execute_unprepared("UPDATE task SET status='running' WHERE id='access'")
        .await
        .unwrap();
    run(&store, "before", "access", "running", 1).await;
    complete_for_access(&store, "before", "access", ACCESS_SECOND).await;
    // It was running before grant; only the later terminal commit matters.
    run(&store, "was-running", "access", "running", 2).await;
    revoke_workspace(&store).await;
    let grant = grant_workspace(&store).await;
    assert_eq!(grant.inbox_after_sequence, Some(1));
    assert_eq!(unread(&store, A).await, 0);
    complete_for_access(&store, "was-running", "access", ACCESS_SECOND).await;
    run(&store, "after", "access", "running", 3).await;
    complete_for_access(&store, "after", "access", ACCESS_SECOND + 5).await;
    assert_eq!(
        unread(&store, A).await,
        2,
        "no first-open baseline; retain completion in the grant second"
    );
    assert_eq!(
        grant_workspace(&store).await,
        grant,
        "idempotent grant must retain its interval"
    );
    assert!(
        find_task_completion_inbox_entry(&db, &access(A), "before")
            .await
            .unwrap()
            .is_some(),
        "history TaskRead remains available"
    );
    assert!(
        pioneer_entity::task_run_read::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .is_empty()
    );
    drop(db);
    drop(store);
    drop(writer);
    let (store, _writer) = open(&dir).await;
    assert_eq!(
        unread(&store, A).await,
        2,
        "restart / another device uses persisted grant"
    );
    revoke_workspace(&store).await;
    assert_eq!(unread(&store, A).await, 0);
    grant_workspace(&store).await;
    assert_eq!(
        unread(&store, A).await,
        0,
        "regrant cannot resurrect the prior interval"
    );
    run(&store, "repeat", "access", "running", 4).await;
    complete_for_access(&store, "repeat", "access", ACCESS_SECOND + 6).await;
    assert_eq!(unread(&store, A).await, 1);
    assert_eq!(unread(&store, B).await, 4, "other participant is unchanged");
}

#[pioneer_postgres_test::postgres_test]
async fn private_grant_visibility_routes_and_absolute_start_are_personal() {
    let (_dir, store, _writer) = fixture().await;
    task(
        &store,
        "private-access",
        Some("T0000000000000000000V"),
        None,
        "system",
        "webhook",
        "attached",
    )
    .await;
    let db = store.database_connection();
    db.execute_unprepared("UPDATE task SET status='running' WHERE id='private-access'")
        .await
        .unwrap();
    run(&store, "private-old", "private-access", "running", 1).await;
    complete_for_access(&store, "private-old", "private-access", ACCESS_SECOND).await;
    let gateway = pioneer_protocol::GatewayId::new("G00000000000000000001").unwrap();
    let principal = pioneer_protocol::PrincipalId::new(A).unwrap();
    let tx = db.begin().await.unwrap();
    pioneer_crud::delete_thread_membership(
        &tx,
        &gateway,
        access(A).workspace_id,
        "T0000000000000000000V",
        &principal,
    )
    .await
    .unwrap();
    let grant = pioneer_crud::insert_thread_membership(
        &tx,
        &pioneer_crud::NewThreadMembership {
            gateway_id: gateway,
            principal_id: principal,
            workspace_id: access(A).workspace_id.into(),
            thread_id: "T0000000000000000000V".into(),
            added_by: pioneer_protocol::PersistedActorRef::System,
            now: grant_time(),
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(grant.inbox_after_sequence, Some(1));
    run(&store, "private-new", "private-access", "running", 2).await;
    complete_for_access(&store, "private-new", "private-access", ACCESS_SECOND).await;
    assert_eq!(unread(&store, A).await, 1);
    assert_eq!(unread(&store, B).await, 2);
    // Public route starts now; B's continuous private route retains both runs.
    store
        .update_user_thread_management(
            access(A).workspace_id,
            "T0000000000000000000V",
            None,
            None,
            Some(pioneer_crud::PersistedThreadAccessClass::Workspace),
            None,
        )
        .await
        .unwrap();
    assert_eq!(unread(&store, A).await, 1);
    assert_eq!(unread(&store, B).await, 2);
    db.execute_unprepared(
        "DELETE FROM thread_membership WHERE principal_id='P0000000000000000000A'",
    )
    .await
    .unwrap();
    assert_eq!(unread(&store, A).await, 0);
    // An actual absolute principal needs no membership, and its creation time
    // does not move when profiles/sessions change.
    let tx = db.begin().await.unwrap();
    let owner = pioneer_protocol::PrincipalId::new("P0000000000000000000O").unwrap();
    pioneer_crud::create_superuser(
        &tx,
        &owner,
        &pioneer_protocol::GatewayId::new("G00000000000000000001").unwrap(),
        "Owner",
        "owner",
        "owner",
        grant_time(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let absolute = TaskCompletionInboxAccess {
        absolute: true,
        principal_id: owner.as_str(),
        ..access(A)
    };
    assert_eq!(
        count_unread_task_completion_inbox(&db, &absolute)
            .await
            .unwrap(),
        0
    );
    run(&store, "owner-new", "private-access", "running", 3).await;
    complete_for_access(&store, "owner-new", "private-access", ACCESS_SECOND).await;
    assert_eq!(
        count_unread_task_completion_inbox(&db, &absolute)
            .await
            .unwrap(),
        1
    );
    let missing = TaskCompletionInboxAccess {
        principal_id: "missing",
        ..absolute
    };
    assert_eq!(
        count_unread_task_completion_inbox(&db, &missing)
            .await
            .unwrap(),
        0
    );
}

#[pioneer_postgres_test::postgres_test]
async fn access_start_list_count_keyset_and_read_all_share_unloaded_eligibility() {
    let (_dir, store, _writer) = fixture().await;
    task(
        &store,
        "access-pages",
        Some("T0000000000000000000P"),
        None,
        "workflow",
        "none",
        "detached",
    )
    .await;
    let db = store.database_connection();
    db.execute_unprepared("UPDATE task SET status='running' WHERE id='access-pages'")
        .await
        .unwrap();
    for ix in 0..8 {
        run(
            &store,
            &format!("pre-{ix}"),
            "access-pages",
            "succeeded",
            ix + 1,
        )
        .await;
    }
    revoke_workspace(&store).await;
    grant_workspace(&store).await;
    // The timestamp alone cannot distinguish these legacy pre-grant commits.
    for ix in 0..35 {
        let id = format!("post-{ix:03}");
        run(&store, &id, "access-pages", "running", ix + 9).await;
        complete_for_access(&store, &id, "access-pages", ACCESS_SECOND).await;
    }
    assert_eq!(unread(&store, A).await, 35);
    let first = list_task_completion_inbox(&db, &access(A), None, 30)
        .await
        .unwrap();
    let end = first.last().unwrap();
    let next =
        list_task_completion_inbox(&db, &access(A), Some((&end.completed_key, &end.run_id)), 30)
            .await
            .unwrap();
    assert_eq!(first.len(), 30);
    assert_eq!(next.len(), 5);
    assert!(
        first
            .iter()
            .chain(&next)
            .all(|v| v.run_id.starts_with("post-"))
    );
    pioneer_crud::mark_all_task_completion_inbox_read(&db, &access(A), "2026-10-16T10:00:00Z")
        .await
        .unwrap();
    assert_eq!(unread(&store, A).await, 0);
    assert_eq!(
        pioneer_entity::task_run_read::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .len(),
        35
    );
    run(&store, "later-same-time", "access-pages", "running", 44).await;
    complete_for_access(&store, "later-same-time", "access-pages", ACCESS_SECOND).await;
    assert_eq!(
        unread(&store, A).await,
        1,
        "later terminal commit escapes read-all despite equal timestamp"
    );
}

#[pioneer_postgres_test::postgres_test]
async fn grant_and_completion_race_uses_durable_writer_order_not_request_clock() {
    let (_dir, store, _writer) = fixture().await;
    task(
        &store,
        "racing-access",
        Some("T0000000000000000000P"),
        None,
        "tool",
        "none",
        "detached",
    )
    .await;
    let db = store.database_connection();
    db.execute_unprepared("UPDATE task SET status='running' WHERE id='racing-access'")
        .await
        .unwrap();
    run(&store, "racing-access-run", "racing-access", "running", 1).await;
    revoke_workspace(&store).await;
    let (grant, ()) = tokio::join!(
        grant_workspace(&store),
        complete_for_access(&store, "racing-access-run", "racing-access", ACCESS_SECOND)
    );
    let run = pioneer_entity::task_run::Entity::find_by_id("racing-access-run")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let expected = u64::from(run.inbox_sequence.unwrap() > grant.inbox_after_sequence.unwrap());
    assert_eq!(unread(&store, A).await, expected);
    assert_eq!(unread(&store, B).await, 1);
    // A rolled-back regrant never becomes the start of a different interval.
    let tx = db.begin().await.unwrap();
    pioneer_crud::delete_workspace_membership(
        &tx,
        &pioneer_protocol::GatewayId::new("G00000000000000000001").unwrap(),
        &pioneer_protocol::PrincipalId::new(A).unwrap(),
        access(A).workspace_id,
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(grant_workspace(&store).await, grant);
    assert_eq!(unread(&store, A).await, expected);
}

#[pioneer_postgres_test::postgres_test]
async fn publication_intervals_use_operation_time_and_preserve_continuous_participants() {
    let (_dir, store, _writer) = fixture().await;
    task(
        &store,
        "publication",
        Some("T0000000000000000000V"),
        None,
        "tool",
        "none",
        "detached",
    )
    .await;
    let db = store.database_connection();
    db.execute_unprepared("UPDATE task SET status='running' WHERE id='publication'")
        .await
        .unwrap();
    db.execute_unprepared(
        "UPDATE thread SET updated_at='2099-01-01T00:00:00Z' WHERE id='T0000000000000000000V'",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "DELETE FROM thread_membership WHERE principal_id='P0000000000000000000A'",
    )
    .await
    .unwrap();
    run(&store, "published-before", "publication", "running", 1).await;
    let now = chrono::Utc::now().timestamp();
    complete_for_access(&store, "published-before", "publication", now).await;
    let patch = |class| {
        store.update_user_thread_management(
            access(A).workspace_id,
            "T0000000000000000000V",
            None,
            None,
            Some(class),
            None,
        )
    };
    patch(pioneer_crud::PersistedThreadAccessClass::Workspace)
        .await
        .unwrap();
    run(&store, "published-after", "publication", "running", 2).await;
    complete_for_access(
        &store,
        "published-after",
        "publication",
        chrono::Utc::now().timestamp(),
    )
    .await;
    assert_eq!(
        unread(&store, A).await,
        1,
        "new public interval excludes an earlier commit even in the same second"
    );
    assert_eq!(
        unread(&store, B).await,
        2,
        "existing private participant has continuous access"
    );
    let saved = pioneer_entity::thread::Entity::find_by_id("T0000000000000000000V")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert!(
        saved.inbox_workspace_started_at.unwrap().timestamp() <= chrono::Utc::now().timestamp()
    );
    let watermark = saved.inbox_workspace_after_sequence;
    patch(pioneer_crud::PersistedThreadAccessClass::Workspace)
        .await
        .unwrap();
    assert_eq!(
        pioneer_entity::thread::Entity::find_by_id("T0000000000000000000V")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .inbox_workspace_after_sequence,
        watermark,
        "same visibility does not reset its interval"
    );
    patch(pioneer_crud::PersistedThreadAccessClass::Private)
        .await
        .unwrap();
    assert_eq!(unread(&store, A).await, 0);
    patch(pioneer_crud::PersistedThreadAccessClass::Workspace)
        .await
        .unwrap();
    assert_eq!(
        unread(&store, A).await,
        0,
        "republication cannot resurrect an earlier public interval"
    );
    assert_eq!(unread(&store, B).await, 2);
}

#[pioneer_postgres_test::postgres_test]
async fn activity_discovery_pages_survive_filtered_scopes_and_duplicate_active_runs() {
    let (_dir, store, _writer) = fixture().await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared(
        "WITH RECURSIVE ids(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM ids WHERE n<513)
         INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,executor_kind,status,title,goal,priority,delivery_policy_json,lifecycle_policy_json,revision,created_at,updated_at)
         SELECT 'active-'||lpad(n::text,4,'0'),'W0000000000000000000I','workspace','W0000000000000000000I','T0000000000000000000P','system','running','Task','Goal',0,'{}'::json,'{}'::jsonb,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM ids",
    ).await.unwrap();
    for (suffix, status) in [("-starting", "starting"), ("-running", "running")] {
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind,created_at,updated_at)
             SELECT id || $1,id,id || $1,1,$3,$2,'system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM task",
            [suffix.into(), status.into(), (if status == "starting" { 1_i64 } else { 2_i64 }).into()],
        )).await.unwrap();
    }
    let principal = pioneer_protocol::PrincipalId::new(A).unwrap();
    let first = pioneer_crud::list_running_task_scopes(
        &db,
        access(A).workspace_id,
        &["T0000000000000000000P".into()],
        &principal,
        "",
    )
    .await
    .unwrap();
    assert_eq!(
        first.scopes.len(),
        512,
        "all four scope batches must survive LIMIT"
    );
    assert_eq!(first.after_task_id.as_deref(), Some("active-0512"));
    let filtered = pioneer_crud::list_running_task_scopes(
        &db,
        access(A).workspace_id,
        &["missing".into()],
        &principal,
        "",
    )
    .await
    .unwrap();
    assert!(filtered.scopes.is_empty());
    assert_eq!(filtered.after_task_id, first.after_task_id);
    let second = pioneer_crud::list_running_task_scopes(
        &db,
        access(A).workspace_id,
        &["T0000000000000000000P".into()],
        &principal,
        filtered.after_task_id.as_deref().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(second.scopes.len(), 1);
    assert_eq!(second.scopes[0].0.task_id, "active-0513");
}

// Durable facts are deliberately seeded independently of current Task policy.
// Keep valid JSON while exercising unusable linkage or recipient evidence.
async fn inbox_contracts(
    store: &CrudStore,
    task_id: &str,
    run_id: &str,
    creator: &str,
    plan: Option<&str>,
    generation: i64,
) {
    let db = store.with_maintenance_access().database_connection();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO task_actor_contract(task_id,workspace_id,creator_json,reviewer_json,delivery_json,revision,created_at,updated_at) VALUES($1,'W0000000000000000000I',$2,'\"runtime_auto\"','{}',1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP) ON CONFLICT (task_id) DO NOTHING",
        [task_id.into(), creator.into()])).await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO task_occurrence_contract(occurrence_id,task_id,run_id,occurrence_key,execution_generation,status,retry_attempt,action_idempotency_key,delivery_plan_json,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'delivered',0,$6,$7,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [run_id.into(), task_id.into(), run_id.into(), run_id.into(), generation.into(), run_id.into(), plan.into()])).await.unwrap();
}

fn inbox_plan(mode: pioneer_protocol::TaskDeliveryMode, target: Option<&str>) -> String {
    use pioneer_protocol::{
        TaskDeliveryFormat, TaskDeliveryPolicy, TaskDeliveryThreadTarget,
        TaskOccurrenceDeliveryPlan,
    };
    serde_json::to_string(&TaskOccurrenceDeliveryPlan::from_normalized_policy(
        TaskDeliveryPolicy {
            mode,
            thread_target: target.map(|_| TaskDeliveryThreadTarget::ExactThread),
            thread_id: target.map(str::to_owned),
            webhook_url: (mode == pioneer_protocol::TaskDeliveryMode::Webhook)
                .then(|| "https://example.test/hook".to_owned()),
            include_result: true,
            format: TaskDeliveryFormat::Summary,
        },
    ))
    .unwrap()
}

#[pioneer_postgres_test::postgres_test]
async fn durable_internal_destinations_share_inbox_membership_across_statuses_and_reads() {
    use pioneer_protocol::{AgentExecutionId, PersistedActorRef, TaskDeliveryMode as M};
    let (_dir, store, _writer) = fixture().await;
    let agent = serde_json::to_string(&PersistedActorRef::AgentExecution(
        AgentExecutionId::new("E00000000000000000001").unwrap(),
    ))
    .unwrap();
    let human = serde_json::to_string(&PersistedActorRef::Principal(
        pioneer_protocol::PrincipalId::new(A).unwrap(),
    ))
    .unwrap();
    let system = serde_json::to_string(&PersistedActorRef::System).unwrap();
    let mut hidden = Vec::new();
    let mut visible = Vec::new();
    for (i, status) in ["succeeded", "failed", "blocked", "cancelled", "timed_out"]
        .into_iter()
        .enumerate()
    {
        for (j, (creator, mode, target, included)) in [
            (&agent, M::None, None, false),
            (&agent, M::Thread, Some("T0000000000000000000I"), false),
            // Detached Composer/background/scheduled output to a user thread.
            (&agent, M::Thread, Some("T0000000000000000000P"), true),
            (&agent, M::UserNotification, None, true),
            (&agent, M::Webhook, None, true),
            (&human, M::None, None, true),
            (&human, M::Thread, Some("T0000000000000000000I"), true),
            (&system, M::None, None, true),
            (&system, M::Thread, Some("T0000000000000000000I"), true),
        ]
        .into_iter()
        .enumerate()
        {
            let task_id = format!("contract-task-{i}-{j}");
            let run_id = format!("contract-run-{i}-{j}");
            task(
                &store,
                &task_id,
                Some("T0000000000000000000P"),
                None,
                "agent",
                "none",
                "detached",
            )
            .await;
            run(&store, &run_id, &task_id, status, 1).await;
            inbox_contracts(
                &store,
                &task_id,
                &run_id,
                creator,
                Some(&inbox_plan(mode, target)),
                1,
            )
            .await;
            if included {
                visible.push(run_id);
            } else {
                hidden.push(run_id);
            }
        }
    }
    let db = store.database_connection();
    // Opposite current policy cannot rewrite old occurrences' classification.
    db.execute_unprepared("UPDATE task SET delivery_policy_json='{\"mode\":\"user_notification\",\"includeResult\":true,\"format\":\"summary\"}'").await.unwrap();
    assert_eq!(unread(&store, A).await, visible.len() as u64);
    let page = list_task_completion_inbox(&db, &access(A), None, 3)
        .await
        .unwrap();
    assert_eq!(page.len(), 3, "internal rows must be filtered before LIMIT");
    let mut paged = Vec::new();
    let mut cursor: Option<(String, String)> = None;
    loop {
        let page = list_task_completion_inbox(
            &db,
            &access(A),
            cursor.as_ref().map(|(at, id)| (at.as_str(), id.as_str())),
            3,
        )
        .await
        .unwrap();
        if page.is_empty() {
            break;
        }
        cursor = page
            .last()
            .map(|row| (row.completed_key.clone(), row.run_id.clone()));
        paged.extend(page.into_iter().map(|row| row.run_id));
    }
    paged.sort();
    visible.sort();
    assert_eq!(paged, visible);
    let ids: Vec<_> = visible.iter().chain(&hidden).cloned().collect();
    let (entries, total) = store
        .task_completion_inbox_patch(&access(A), &ids)
        .await
        .unwrap();
    assert_eq!(total, visible.len() as u64);
    assert_eq!(entries.len(), visible.len());
    assert!(entries.iter().all(|row| visible.contains(&row.run_id)));
    for id in &hidden {
        assert!(
            find_task_completion_inbox_entry(&db, &access(A), id)
                .await
                .unwrap()
                .is_none()
        );
        read(&store, A, id).await;
    }
    let params = pioneer_protocol::TaskCompletionInboxViewedParams {
        workspace_id: access(A).workspace_id.to_owned(),
        thread_id: "T0000000000000000000P".to_owned(),
        run_ids: hidden.clone(),
        result_turn_ids: vec![],
        result_items: vec![],
    };
    assert!(
        pioneer_crud::mark_viewed_task_completion_inbox_read(
            &db,
            &access(A),
            &params,
            "2026-10-16T11:00:00Z"
        )
        .await
        .unwrap()
        .is_empty()
    );
    pioneer_crud::mark_all_task_completion_inbox_read(&db, &access(A), "2026-10-16T11:00:00Z")
        .await
        .unwrap();
    assert_eq!(unread(&store, A).await, 0);
    assert_eq!(unread(&store, B).await, visible.len() as u64);
    let receipts = pioneer_entity::task_run_read::Entity::find()
        .all(&db)
        .await
        .unwrap();
    assert_eq!(receipts.len(), visible.len());
    assert!(receipts.iter().all(|row| visible.contains(&row.run_id)));
}

#[pioneer_postgres_test::postgres_test]
async fn occurrence_evidence_is_per_run_and_untrusted_legacy_links_remain_visible() {
    use pioneer_protocol::TaskDeliveryMode as M;
    let (_dir, store, _writer) = fixture().await;
    let agent = r#"{"kind":"agent_execution","id":"E00000000000000000001"}"#;
    task(
        &store,
        "history",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "attached",
    )
    .await;
    let none = inbox_plan(M::None, None);
    let user = inbox_plan(M::Thread, Some("T0000000000000000000P"));
    let unknown_version = none.replace("\"routingVersion\":1", "\"routingVersion\":99");
    let wrong_boolean = none.replace("\"includeResult\":true", "\"includeResult\":\"true\"");
    let unknown_format = none.replace("\"summary\"", "\"future_format\"");
    let mismatched_target = inbox_plan(M::Thread, Some("T0000000000000000000I")).replace(
        "\"presentationThreadId\":\"T0000000000000000000I\"",
        "\"presentationThreadId\":\"T0000000000000000000P\"",
    );
    for (i, plan) in [
        Some(none.as_str()),
        Some(user.as_str()),
        None,
        Some("null"),
        Some(r#""invalid-shape""#),
        Some("{}"),
        Some(unknown_version.as_str()),
        Some(wrong_boolean.as_str()),
        Some(unknown_format.as_str()),
        Some(mismatched_target.as_str()),
    ]
    .into_iter()
    .enumerate()
    {
        let id = format!("history-{i}");
        run(&store, &id, "history", "succeeded", i as i64 + 1).await;
        inbox_contracts(&store, "history", &id, agent, plan, i as i64 + 1).await;
    }
    for (i, corruption) in [
        "actor-workspace",
        "occurrence-task",
        "target-workspace",
        "invalid-creator",
        "missing-actor",
    ]
    .into_iter()
    .enumerate()
    {
        task(
            &store,
            corruption,
            Some("T0000000000000000000P"),
            None,
            "agent",
            "none",
            "detached",
        )
        .await;
        run(&store, corruption, corruption, "succeeded", 1).await;
        let plan = if i == 2 {
            inbox_plan(M::Thread, Some("foreign-internal"))
        } else {
            none.clone()
        };
        inbox_contracts(&store, corruption, corruption, agent, Some(&plan), 1).await;
    }
    let db = store.database_connection();
    for sql in [
        "UPDATE task_actor_contract SET workspace_id='W0000000000000000000O' WHERE task_id='actor-workspace'",
        "UPDATE task_occurrence_contract SET task_id='other-task' WHERE run_id='occurrence-task'",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class) VALUES('foreign-internal','W0000000000000000000O','','agent','m','p','active','task_run','internal')",
        "UPDATE task_actor_contract SET creator_json='{\"kind\":\"agent_execution\",\"id\":7}' WHERE task_id='invalid-creator'",
        "DELETE FROM task_actor_contract WHERE task_id='missing-actor'",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    assert_eq!(unread(&store, A).await, 14);
    assert!(
        find_task_completion_inbox_entry(&db, &access(A), "history-0")
            .await
            .unwrap()
            .is_none()
    );
    let ids = [
        "history-0",
        "history-1",
        "actor-workspace",
        "occurrence-task",
        "target-workspace",
        "invalid-creator",
        "missing-actor",
    ]
    .map(str::to_owned);
    let (entries, total) = store
        .task_completion_inbox_patch(&access(A), &ids)
        .await
        .unwrap();
    assert_eq!(total, 14);
    assert_eq!(entries.len(), 6);
}

#[pioneer_postgres_test::postgres_test]
async fn retry_rebinding_keeps_all_internal_attempts_out_of_the_inbox() {
    use pioneer_protocol::{TaskDeliveryMode as M, TaskOccurrenceStatus};
    for (mode, target) in [(M::None, None), (M::Thread, Some("T0000000000000000000I"))] {
        let (_dir, store, _writer) = fixture().await;
        let db = store.with_maintenance_access().database_connection();
        task(
            &store,
            "retry-task",
            Some("T0000000000000000000P"),
            None,
            "agent",
            "none",
            "detached",
        )
        .await;
        run(&store, "attempt-1", "retry-task", "failed", 1).await;
        inbox_contracts(
            &store,
            "retry-task",
            "attempt-1",
            r#"{"kind":"agent_execution","id":"E00000000000000000001"}"#,
            Some(&inbox_plan(mode, target)),
            1,
        )
        .await;
        db.execute_unprepared("UPDATE task SET status='active' WHERE id='retry-task'")
            .await
            .unwrap();
        db.execute_unprepared("UPDATE task_occurrence_contract SET status='failed',action_idempotency_key='task:retry-task:attempt-1' WHERE occurrence_id='attempt-1'").await.unwrap();
        // A separate logical occurrence of the SAME Task delivers to the user.
        // A recurrence may point parent_run_id at the preceding group's run.
        run(&store, "user-occurrence", "retry-task", "succeeded", 2).await;
        db.execute_unprepared(
            "UPDATE task_run SET parent_run_id='attempt-1' WHERE id='user-occurrence'",
        )
        .await
        .unwrap();
        inbox_contracts(
            &store,
            "retry-task",
            "user-occurrence",
            r#"{"kind":"agent_execution","id":"E00000000000000000001"}"#,
            Some(&inbox_plan(M::Thread, Some("T0000000000000000000P"))),
            2,
        )
        .await;
        db.execute_unprepared("UPDATE task SET delivery_policy_json='{\"mode\":\"thread\",\"threadTarget\":\"exact_thread\",\"threadId\":\"T0000000000000000000P\",\"includeResult\":true,\"format\":\"summary\"}' WHERE id='retry-task'").await.unwrap();
        let mut attempts = Vec::new();
        for attempt in 1..=3 {
            let id = format!("attempt-{attempt}");
            if attempt > 1 {
                let previous_id = format!("attempt-{}", attempt - 1);
                // Storage side of task_terminal::retry_run: one group, sequential
                // attempts, both source links, monotonically increasing run number.
                run(&store, &id, "retry-task", "queued", attempt + 1).await;
                db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                    "UPDATE task_run SET run_group_id='attempt-1',attempt_number=$1,retry_of_run_id=$2,parent_run_id=$3,completed_at=NULL,result_json=NULL WHERE id=$4",
                    [attempt.into(), previous_id.clone().into(), previous_id.clone().into(), id.clone().into()])).await.unwrap();
                let previous = store
                    .get_task_occurrence_contract_by_run(&previous_id)
                    .await
                    .unwrap()
                    .unwrap();
                let mut next = previous.retry(attempt as u32).unwrap();
                next.run_id = id.clone();
                // Same repository upsert used by retry dispatch; this updates
                // one occurrence_id, preserving its plan, generation and key.
                store
                    .upsert_task_occurrence_contract(&next, 1792144800 + attempt)
                    .await
                    .unwrap();
                assert!(
                    store
                        .get_task_occurrence_contract_by_run(&previous_id)
                        .await
                        .unwrap()
                        .is_none()
                );
                db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                    "UPDATE task_run SET status='failed',completed_at='2026-10-16 10:00:00 +00:00' WHERE id=$1",
                    [id.clone().into()])).await.unwrap();
                next.status = TaskOccurrenceStatus::Failed;
                store
                    .upsert_task_occurrence_contract(&next, 1792144801 + attempt)
                    .await
                    .unwrap();
            }
            attempts.push(id);
            assert_eq!(unread(&store, A).await, 1, "{mode:?}, attempt {attempt}");
            let page = list_task_completion_inbox(&db, &access(A), None, 1)
                .await
                .unwrap();
            assert_eq!(
                page.iter().map(|r| r.run_id.as_str()).collect::<Vec<_>>(),
                ["user-occurrence"]
            );
            let ids = attempts
                .iter()
                .cloned()
                .chain(["user-occurrence".to_owned()])
                .collect::<Vec<_>>();
            let (entries, total) = store
                .task_completion_inbox_patch(&access(A), &ids)
                .await
                .unwrap();
            assert_eq!(total, 1);
            assert_eq!(
                entries
                    .iter()
                    .map(|r| r.run_id.as_str())
                    .collect::<Vec<_>>(),
                ["user-occurrence"]
            );
            for hidden in &attempts {
                assert!(
                    find_task_completion_inbox_entry(&db, &access(A), hidden)
                        .await
                        .unwrap()
                        .is_none()
                );
                read(&store, A, hidden).await;
            }
            pioneer_crud::mark_all_task_completion_inbox_read(
                &db,
                &access(B),
                "2026-10-16T11:00:00Z",
            )
            .await
            .unwrap();
            let receipts = pioneer_entity::task_run_read::Entity::find()
                .all(&db)
                .await
                .unwrap();
            assert_eq!(receipts.len(), 1);
            assert_eq!(receipts[0].run_id, "user-occurrence");
            let occurrences = pioneer_entity::task_occurrence_contract::Entity::find()
                .all(&db)
                .await
                .unwrap();
            assert_eq!(
                occurrences.len(),
                2,
                "one row per logical occurrence, not per attempt"
            );
        }
        // Corrupt links cannot borrow the old group's durable plan. The current
        // attempt still has its direct contract; these probes address older ones.
        for (damage, restore, visible) in [
            (
                "UPDATE task_run SET run_group_id='foreign-group' WHERE id='attempt-3'",
                "UPDATE task_run SET run_group_id='attempt-1' WHERE id='attempt-3'",
                "attempt-1",
            ),
            (
                "UPDATE task_run SET task_id='foreign-task' WHERE id='attempt-3'",
                "UPDATE task_run SET task_id='retry-task' WHERE id='attempt-3'",
                "attempt-1",
            ),
            (
                "UPDATE task_occurrence_contract SET occurrence_id='unrelated-occurrence' WHERE occurrence_id='attempt-1'",
                "UPDATE task_occurrence_contract SET occurrence_id='attempt-1' WHERE occurrence_id='unrelated-occurrence'",
                "attempt-2",
            ),
            (
                "UPDATE task_occurrence_contract SET task_id='foreign-task' WHERE occurrence_id='attempt-1'",
                "UPDATE task_occurrence_contract SET task_id='retry-task' WHERE occurrence_id='attempt-1'",
                "attempt-1",
            ),
            (
                "UPDATE task_run SET retry_of_run_id='user-occurrence',parent_run_id='user-occurrence' WHERE id='attempt-2'",
                "UPDATE task_run SET retry_of_run_id='attempt-1',parent_run_id='attempt-1' WHERE id='attempt-2'",
                "attempt-2",
            ),
            (
                "UPDATE task_run SET attempt_number=4 WHERE id='attempt-3'",
                "UPDATE task_run SET attempt_number=3 WHERE id='attempt-3'",
                "attempt-1",
            ),
            (
                "UPDATE task_run SET attempt_number=9 WHERE id='attempt-1'",
                "UPDATE task_run SET attempt_number=1 WHERE id='attempt-1'",
                "attempt-2",
            ),
        ] {
            db.execute_unprepared(damage).await.unwrap();
            assert!(
                find_task_completion_inbox_entry(&db, &access(A), visible)
                    .await
                    .unwrap()
                    .is_some(),
                "{damage}"
            );
            db.execute_unprepared(restore).await.unwrap();
            assert_eq!(unread(&store, A).await, 1);
        }
        db.execute_unprepared(
            "DELETE FROM task_occurrence_contract WHERE occurrence_id='attempt-1'",
        )
        .await
        .unwrap();
        assert_eq!(
            unread(&store, A).await,
            4,
            "missing durable evidence retains legacy eligibility"
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn same_run_recovery_preserves_internal_retry_history_membership() {
    use pioneer_protocol::{TaskDeliveryMode as M, TaskOccurrenceStatus as O};
    for (mode, target) in [(M::None, None), (M::Thread, Some("T0000000000000000000I"))] {
        let (_dir, store, _writer) = fixture().await;
        let db = store.with_maintenance_access().database_connection();
        task(
            &store,
            "resume-task",
            Some("T0000000000000000000P"),
            None,
            "agent",
            "none",
            "detached",
        )
        .await;
        run(&store, "resume-first", "resume-task", "failed", 1).await;
        inbox_contracts(
            &store,
            "resume-task",
            "resume-first",
            r#"{"kind":"agent_execution","id":"E00000000000000000001"}"#,
            Some(&inbox_plan(mode, target)),
            1,
        )
        .await;
        db.execute_unprepared("UPDATE task_occurrence_contract SET status='failed',action_idempotency_key='task:resume-task:resume-first' WHERE occurrence_id='resume-first'").await.unwrap();
        run(&store, "resume-second", "resume-task", "blocked", 2).await;
        db.execute_unprepared("UPDATE task_run SET run_group_id='resume-first',attempt_number=2,retry_of_run_id='resume-first',parent_run_id='resume-first' WHERE id='resume-second'").await.unwrap();
        let original = store
            .get_task_occurrence_contract_by_run("resume-first")
            .await
            .unwrap()
            .unwrap();
        let mut bound = original.retry(2).unwrap();
        bound.run_id = "resume-second".to_owned();
        bound.status = O::Failed; // Blocked TaskRuns map to Failed occurrences.
        store
            .upsert_task_occurrence_contract(&bound, 1792144800)
            .await
            .unwrap();
        run(&store, "resume-user", "resume-task", "succeeded", 3).await;
        inbox_contracts(
            &store,
            "resume-task",
            "resume-user",
            r#"{"kind":"agent_execution","id":"E00000000000000000001"}"#,
            Some(&inbox_plan(M::Thread, Some("T0000000000000000000P"))),
            2,
        )
        .await;
        db.execute_unprepared("UPDATE task SET status='blocked' WHERE id='resume-task'")
            .await
            .unwrap();
        let ids = ["resume-first", "resume-second", "resume-user"].map(str::to_owned);
        for stage in 0..3 {
            if stage == 1 {
                // Contract side of resume_task_owned_turn: checked generation
                // increment + retry(), Recovering, immutable same run binding.
                // Use public protocol/repository APIs without new test seams.
                let generation = bound.retry_attempt.checked_add(1).unwrap();
                let mut resumed = bound.retry(generation).unwrap();
                resumed.status = O::Recovering;
                assert_eq!(resumed.run_id, bound.run_id);
                store
                    .upsert_task_occurrence_contract(&resumed, 1792144801)
                    .await
                    .unwrap();
                bound = resumed;
                db.execute_unprepared("UPDATE task SET status='running' WHERE id='resume-task'")
                    .await
                    .unwrap();
                // Storage side of reopening the same blocked run. Neither its
                // identity nor attempt_number is changed by this transition.
                db.execute_unprepared("UPDATE task_run SET status='running',completed_at=NULL,result_json=NULL,error_json=NULL WHERE id='resume-second'").await.unwrap();
            } else if stage == 2 {
                db.execute_unprepared("UPDATE task_run SET status='succeeded',completed_at='2026-10-16 10:00:00 +00:00' WHERE id='resume-second'").await.unwrap();
                bound.status = O::Delivered;
                store
                    .upsert_task_occurrence_contract(&bound, 1792144802)
                    .await
                    .unwrap();
            }
            let saved = store
                .get_task_occurrence_contract_by_run("resume-second")
                .await
                .unwrap()
                .unwrap();
            let run = pioneer_entity::task_run::Entity::find_by_id("resume-second")
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(saved.occurrence_id, "resume-first");
            assert_eq!(saved.run_id, run.id);
            assert_eq!(run.id, "resume-second");
            assert_eq!(run.attempt_number, 2);
            assert_eq!(saved.retry_attempt, if stage == 0 { 2 } else { 3 });
            assert_eq!(saved.delivery_plan, original.delivery_plan);
            assert_eq!(unread(&store, A).await, 1, "{mode:?} stage {stage}");
            let page = list_task_completion_inbox(&db, &access(A), None, 1)
                .await
                .unwrap();
            assert_eq!(
                page.iter().map(|r| r.run_id.as_str()).collect::<Vec<_>>(),
                ["resume-user"]
            );
            let (entries, total) = store
                .task_completion_inbox_patch(&access(A), &ids)
                .await
                .unwrap();
            assert_eq!(total, 1);
            assert_eq!(
                entries
                    .iter()
                    .map(|r| r.run_id.as_str())
                    .collect::<Vec<_>>(),
                ["resume-user"]
            );
            for hidden in &ids[..2] {
                assert!(
                    find_task_completion_inbox_entry(&db, &access(A), hidden)
                        .await
                        .unwrap()
                        .is_none()
                );
                read(&store, A, hidden).await;
            }
            pioneer_crud::mark_all_task_completion_inbox_read(
                &db,
                &access(B),
                "2026-10-16T11:00:00Z",
            )
            .await
            .unwrap();
            let receipts = pioneer_entity::task_run_read::Entity::find()
                .all(&db)
                .await
                .unwrap();
            assert_eq!(receipts.len(), 1);
            assert_eq!(receipts[0].run_id, "resume-user");
            let occurrences = pioneer_entity::task_occurrence_contract::Entity::find()
                .all(&db)
                .await
                .unwrap();
            assert_eq!(occurrences.len(), 2);
        }
    }
}
