//! CRUD integration coverage for the completed-run inbox and access boundaries.
use pioneer_crud::{CrudStore, TaskCompletionInboxAccess, count_unread_task_completion_inbox};
use pioneer_postgres_pools::{PostgresDatabase, PostgresWriteExecutor};
use pioneer_postgres_test::{TestDatabase, scoped_database};
use sea_orm::{ConnectionTrait, EntityTrait, TransactionTrait};
use std::sync::Arc;

const A: &str = "P0000000000000000000A";
const B: &str = "P0000000000000000000B";

async fn open(fixture: &TestDatabase) -> (CrudStore, PostgresWriteExecutor) {
    let writer = PostgresWriteExecutor::new(fixture.connect_with_limit(1).await.unwrap());
    let reader = fixture.connect_read_only().await.unwrap();
    let db = PostgresDatabase::from_executor(reader, writer.clone());
    assert!(db.reader_is_read_only().await.unwrap());
    (CrudStore::new(db), writer)
}

async fn fixture() -> (Arc<TestDatabase>, CrudStore, PostgresWriteExecutor) {
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
