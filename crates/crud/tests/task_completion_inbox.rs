//! Completed-run inbox regression coverage. Do not execute before external review.
use migration::{Migrator, MigratorTrait};
use pioneer_crud::{
    CrudStore, TaskCompletionInboxAccess, count_unread_task_completion_inbox,
    find_task_completion_inbox_entry, list_task_completion_inbox, mark_task_completion_inbox_read,
    resolve_task_authorization_scope,
};
use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DbBackend, EntityTrait, QueryFilter,
    Statement, TransactionTrait,
};

const A: &str = "P0000000000000000000A";
const B: &str = "P0000000000000000000B";
const MIGRATION: &str = "m20261016_000001_task_completion_inbox";

// Same file-backed reader / serialized writer setup as the existing CRUD
// migration suites. Fixture writes never go through the query-only reader.
async fn open(path: &std::path::Path) -> (CrudStore, SqliteWriteExecutor) {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let mut opts = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    opts.max_connections(1)
        .min_connections(1)
        .map_sqlx_sqlite_opts(|o| o.pragma("foreign_keys", "OFF"));
    let connection = Database::connect(opts).await.unwrap();
    let writer = SqliteWriteExecutor::new(connection.clone());
    SqliteDatabase::from_executor(connection, writer.clone())
        .maintenance()
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let mut opts = ConnectOptions::new(format!("sqlite://{}?mode=ro", path.display()));
    opts.max_connections(2)
        .min_connections(1)
        .map_sqlx_sqlite_opts(|o| o.pragma("query_only", "ON"));
    let db = SqliteDatabase::from_executor(Database::connect(opts).await.unwrap(), writer.clone());
    assert!(db.reader_query_only_enabled().await.unwrap());
    (CrudStore::new(db), writer)
}
async fn fixture(before: bool) -> (tempfile::TempDir, CrudStore, SqliteWriteExecutor) {
    let dir = tempfile::tempdir().unwrap();
    let (store, writer) = open(&dir.path().join("inbox.sqlite")).await;
    let count = before.then(|| {
        Migrator::migrations()
            .iter()
            .position(|m| m.name() == MIGRATION)
            .unwrap() as u32
    });
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, count)
        .await
        .unwrap();
    let db = store.with_maintenance_access().database_connection();
    if !before {
        db.execute_unprepared("UPDATE task_inbox_activation SET started_at=1792108800 WHERE id=1")
            .await
            .unwrap();
    }
    // ACL membership tables enforce 21-character alphanumeric IDs. Keep the
    // fixture relationships valid without relaxing the migrated schema.
    for sql in [
        "INSERT INTO gateway_identity(id,singleton_key,identity_bootstrap_version,created_at,updated_at) VALUES('G00000000000000000001',1,1,'2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00')",
        "INSERT INTO gateway_principal(id,gateway_id,kind,role_key,status,display_name,nickname,nickname_key,created_at,updated_at) VALUES('P0000000000000000000A','G00000000000000000001','user','member','active','A','a','a','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00'),('P0000000000000000000B','G00000000000000000001','user','member','active','B','b','b','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00')",
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('W0000000000000000000I','Inbox',1,1),('W0000000000000000000O','Other',1,0)",
        "INSERT INTO thread(id,workspace_id,name,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('T0000000000000000000P','W0000000000000000000I','Public thread','','agent','m','p','active','user','workspace','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00'),('T0000000000000000000V','W0000000000000000000I','Private thread','','agent','m','p','active','user','private','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00'),('T0000000000000000000I','W0000000000000000000I',NULL,'','agent','m','p','active','task_run','internal','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00')",
        "INSERT INTO workspace_membership(principal_id,workspace_id,granted_by_actor_kind,created_at,updated_at) VALUES('P0000000000000000000A','W0000000000000000000I','system','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00'),('P0000000000000000000B','W0000000000000000000I','system','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00')",
        "INSERT INTO thread_membership(principal_id,thread_id,added_by_actor_kind,created_at,updated_at) VALUES('P0000000000000000000A','T0000000000000000000V','system','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00'),('P0000000000000000000B','T0000000000000000000V','system','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00')",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    (dir, store, writer)
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
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,root_task_id,executor_kind,status,title,goal,priority,delivery_policy_json,lifecycle_policy_json,revision,created_at,updated_at) VALUES(?,'W0000000000000000000I','workspace','W0000000000000000000I',?,?,?,'completed','Task title','Goal',0,?,?,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [id.into(),thread.into(),root.into(),executor.into(),serde_json::json!({"mode":mode,"includeResult":true,"format":"summary"}).to_string().into(),serde_json::json!({"attachment":attachment,"onParentCancel":"cancel","onParentFailure":"cancel","completion":"complete_on_terminal_run"}).to_string().into()])).await.unwrap();
}
async fn run(store: &CrudStore, id: &str, task: &str, status: &str, number: i64) {
    store.with_maintenance_access().database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind,completed_at,result_json,created_at,updated_at) VALUES(?1,?2,?3,1,?4,?5,(SELECT executor_kind FROM task WHERE id=?2),'2026-10-16 10:00:00 +00:00',?6,CURRENT_TIMESTAMP,'2099-01-01 00:00:00 +00:00')",
        [id.into(),task.into(),id.into(),number.into(),status.into(),serde_json::json!({"summary":"summary","data":{"html":"not for the list"},"artifacts":[]}).to_string().into()])).await.unwrap();
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

#[tokio::test]
async fn terminal_statuses_policies_executors_attachment_and_recurring_runs_share_one_source() {
    let (_dir, store, _writer) = fixture(false).await;
    let mut expected = 0;
    for (ix, status) in [
        "succeeded",
        "failed",
        "blocked",
        "cancelled",
        "timed_out",
        "queued",
        "starting",
        "running",
        "waiting",
        "waiting_review",
    ]
    .iter()
    .enumerate()
    {
        for (jx, mode) in ["none", "thread", "user_notification", "webhook"]
            .iter()
            .enumerate()
        {
            let id = format!("task-{ix}-{jx}");
            task(
                &store,
                &id,
                Some("T0000000000000000000P"),
                None,
                ["agent", "tool", "workflow", "webhook", "system"][ix % 5],
                mode,
                if jx % 2 == 0 { "attached" } else { "detached" },
            )
            .await;
            run(&store, &format!("run-{ix}-{jx}"), &id, status, 1).await;
            if ix < 5 {
                expected += 1;
            }
        }
    }
    // A service System task has no accessible collaboration root. A System
    // task attached to a public root was included by the matrix above.
    task(&store, "service", None, None, "system", "none", "detached").await;
    run(&store, "service-run", "service", "succeeded", 1).await;
    run(&store, "second-occurrence", "task-0-0", "succeeded", 2).await;
    expected += 1;
    assert_eq!(unread(&store, A).await, expected);
    let page = list_task_completion_inbox(&store.database_connection(), &access(A), None, 100)
        .await
        .unwrap();
    assert_eq!(page.len() as u64, expected);
    assert!(page.iter().all(|e| e.thread_id == "T0000000000000000000P"
        && e.summary.as_deref() == Some("summary")
        && e.completed_at == 1792144800));
    assert_eq!(page.iter().filter(|e| e.task_id == "task-0-0").count(), 2);
    let absolute = TaskCompletionInboxAccess {
        absolute: true,
        ..access(A)
    };
    assert_eq!(
        count_unread_task_completion_inbox(&store.database_connection(), &absolute)
            .await
            .unwrap(),
        expected,
        "absolute access still excludes service tasks without a user thread"
    );
    // Replaying deliveries and re-reading events cannot insert inbox rows.
    let again = list_task_completion_inbox(&store.database_connection(), &access(A), None, 100)
        .await
        .unwrap();
    assert_eq!(
        page.iter().map(|e| &e.run_id).collect::<Vec<_>>(),
        again.iter().map(|e| &e.run_id).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn personal_reads_survive_reopen_and_acl_revocation_removes_list_count_get_and_write() {
    let (dir, store, writer) = fixture(false).await;
    task(
        &store,
        "root",
        Some("T0000000000000000000V"),
        None,
        "agent",
        "none",
        "attached",
    )
    .await;
    task(
        &store,
        "child",
        Some("T0000000000000000000P"),
        Some("root"),
        "system",
        "webhook",
        "detached",
    )
    .await;
    store
        .database_connection()
        .execute_unprepared(
            "UPDATE task SET owner_kind='user',owner_id='P0000000000000000000A' WHERE id='root'",
        )
        .await
        .unwrap();
    run(&store, "run", "child", "blocked", 1).await;
    assert_eq!(unread(&store, A).await, 1);
    assert_eq!(unread(&store, B).await, 1);
    let scope = resolve_task_authorization_scope(
        &store.database_connection(),
        "child",
        Some("W0000000000000000000I"),
        None,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        scope.root_thread_id.as_deref(),
        Some("T0000000000000000000V")
    );
    read(&store, A, "run").await;
    read(&store, A, "run").await;
    assert_eq!(unread(&store, A).await, 0);
    assert_eq!(unread(&store, B).await, 1);
    assert!(
        find_task_completion_inbox_entry(&store.database_connection(), &access(A), "run")
            .await
            .unwrap()
            .unwrap()
            .read_at
            .is_some()
    );
    store.database_connection().close().await.unwrap();
    drop(store);
    drop(writer);
    let (store, _writer) = open(&dir.path().join("inbox.sqlite")).await;
    assert_eq!(unread(&store, A).await, 0);
    // Another device reloads the same durable receipt, not a local copy.
    assert!(
        find_task_completion_inbox_entry(&store.database_connection(), &access(A), "run")
            .await
            .unwrap()
            .unwrap()
            .read_at
            .is_some()
    );
    store.database_connection().execute_unprepared("DELETE FROM thread_membership WHERE principal_id='P0000000000000000000B' AND thread_id='T0000000000000000000V'").await.unwrap();
    assert_eq!(unread(&store, B).await, 0);
    assert!(
        list_task_completion_inbox(&store.database_connection(), &access(B), None, 30)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        find_task_completion_inbox_entry(&store.database_connection(), &access(B), "run")
            .await
            .unwrap()
            .is_none()
    );
    read(&store, B, "run").await;
    let receipts = store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT COUNT(*) n FROM task_run_read WHERE principal_id='P0000000000000000000B'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipts.try_get::<i64>("", "n").unwrap(), 0);
    store
        .database_connection()
        .execute_unprepared(
            "DELETE FROM workspace_membership WHERE principal_id='P0000000000000000000A'",
        )
        .await
        .unwrap();
    assert!(
        find_task_completion_inbox_entry(&store.database_connection(), &access(A), "run")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn time_and_id_seek_pages_are_stable_and_total_unread_is_not_page_size() {
    let (_dir, store, _writer) = fixture(false).await;
    task(
        &store,
        "recurring",
        Some("T0000000000000000000P"),
        None,
        "tool",
        "none",
        "detached",
    )
    .await;
    for ix in 0..65 {
        run(
            &store,
            &format!("run-{ix:03}"),
            "recurring",
            "succeeded",
            ix + 1,
        )
        .await;
    }
    assert_eq!(unread(&store, A).await, 65);
    let first = list_task_completion_inbox(&store.database_connection(), &access(A), None, 30)
        .await
        .unwrap();
    assert_eq!(first.len(), 30);
    assert_eq!(first[0].run_id, "run-064");
    let anchor = first.last().unwrap();
    read(&store, A, &anchor.run_id).await;
    // A new occurrence with the same completion time cannot shift the seek.
    run(&store, "run-999", "recurring", "succeeded", 66).await;
    let second = list_task_completion_inbox(
        &store.database_connection(),
        &access(A),
        Some((&anchor.completed_key, &anchor.run_id)),
        30,
    )
    .await
    .unwrap();
    assert_eq!(second[0].run_id, "run-034");
    assert!(
        second
            .iter()
            .all(|e| !first.iter().any(|f| f.run_id == e.run_id))
    );
    let anchor = second.last().unwrap();
    let third = list_task_completion_inbox(
        &store.database_connection(),
        &access(A),
        Some((&anchor.completed_key, &anchor.run_id)),
        30,
    )
    .await
    .unwrap();
    assert_eq!(third.len(), 5);
    assert_eq!(unread(&store, A).await, 65);
    let foreign = TaskCompletionInboxAccess {
        workspace_id: "W0000000000000000000O",
        principal_id: A,
        absolute: true,
    };
    assert_eq!(
        count_unread_task_completion_inbox(&store.database_connection(), &foreign)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn migration_preserves_legacy_acknowledgement_only_for_its_exact_recipient() {
    let (_dir, store, writer) = fixture(true).await;
    task(
        &store,
        "task",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "user_notification",
        "detached",
    )
    .await;
    run(&store, "run", "task", "succeeded", 1).await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_user_id,status,attempt_count,max_attempts,created_at,updated_at) VALUES('delivery','W0000000000000000000I','task','run','delivery-key','user_notification','P0000000000000000000A','delivered',1,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO user_notification_outbox(id,task_delivery_id,workspace_id,recipient_principal_id,task_id,run_id,payload_json,status,created_at,delivered_at,acknowledged_at) VALUES('legacy','delivery','W0000000000000000000I','P0000000000000000000A','task','run','{}','acknowledged',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    // A second delivery/outbox record for the same run is not a second card,
    // and its missing acknowledgement cannot undo an exact saved receipt.
    db.execute_unprepared("INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_user_id,status,attempt_count,max_attempts,created_at,updated_at) VALUES('delivery-duplicate','W0000000000000000000I','task','run','duplicate-key','user_notification','P0000000000000000000A','delivered',1,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO user_notification_outbox(id,task_delivery_id,workspace_id,recipient_principal_id,task_id,run_id,payload_json,status,created_at,delivered_at) VALUES('unread-duplicate','delivery-duplicate','W0000000000000000000I','P0000000000000000000A','task','run','{}','delivered',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    // FK OFF permits inconsistent historical rows. None may become B's read
    // receipt by merely pointing at the same run ID or an orphaned parent.
    task(
        &store,
        "other-task",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "attached",
    )
    .await;
    run(&store, "orphan-run", "other-task", "succeeded", 1).await;
    db.execute_unprepared("UPDATE task_run SET task_id='missing-task' WHERE id='orphan-run'")
        .await
        .unwrap();
    task(
        &store,
        "orphan-workspace-task",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "attached",
    )
    .await;
    run(
        &store,
        "orphan-workspace-run",
        "orphan-workspace-task",
        "succeeded",
        1,
    )
    .await;
    db.execute_unprepared(
        "UPDATE task SET workspace_id='missing-workspace' WHERE id='orphan-workspace-task'",
    )
    .await
    .unwrap();
    for (id, task_id, run_id, workspace_id) in [
        (
            "missing-run-ack",
            "task",
            "missing-run",
            "W0000000000000000000I",
        ),
        (
            "wrong-task-ack",
            "other-task",
            "run",
            "W0000000000000000000I",
        ),
        (
            "wrong-workspace-ack",
            "task",
            "run",
            "W0000000000000000000O",
        ),
        (
            "missing-task-ack",
            "missing-task",
            "orphan-run",
            "W0000000000000000000I",
        ),
        (
            "missing-workspace-ack",
            "orphan-workspace-task",
            "orphan-workspace-run",
            "missing-workspace",
        ),
    ] {
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO user_notification_outbox(id,task_delivery_id,workspace_id,recipient_principal_id,task_id,run_id,payload_json,status,created_at,delivered_at,acknowledged_at) VALUES(?1,?1,?2,?3,?4,?5,'{}','acknowledged',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            [id.into(),workspace_id.into(),B.into(),task_id.into(),run_id.into()])).await.unwrap();
    }
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, Some(1))
        .await
        .unwrap();
    db.execute_unprepared("UPDATE seaql_migrations SET applied_at=1792108800 WHERE version='m20261016_000001_task_completion_inbox'").await.unwrap();
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    let receipts = pioneer_entity::task_run_read::Entity::find()
        .all(&db)
        .await
        .unwrap();
    assert_eq!(
        receipts.len(),
        1,
        "backfill explicitly validates run/task/workspace existence and belonging without FK enforcement"
    );
    assert_eq!(receipts[0].principal_id, A);
    assert_eq!(receipts[0].run_id, "run");
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    assert_eq!(unread(&store, A).await, 0);
    assert_eq!(unread(&store, B).await, 1);
    assert_eq!(
        list_task_completion_inbox(&db, &access(A), None, 30)
            .await
            .unwrap()
            .len(),
        0
    );
    let legacy=db.query_one_raw(Statement::from_string(DbBackend::Sqlite,"SELECT n.payload_json,d.status FROM user_notification_outbox n JOIN task_delivery d ON d.id=n.task_delivery_id WHERE n.id='legacy'")).await.unwrap().unwrap();
    assert_eq!(legacy.try_get::<String>("", "payload_json").unwrap(), "{}");
    assert_eq!(legacy.try_get::<String>("", "status").unwrap(), "delivered");
    // Data is copied, not borrowed from the old inbox. Removing that receipt
    // does not undo migrated read state or remove the saved run.
    db.execute_unprepared("DELETE FROM user_notification_outbox WHERE id='legacy'")
        .await
        .unwrap();
    assert_eq!(unread(&store, A).await, 0);
    // Older devices acknowledging after migration are accounted for too.
    db.execute_unprepared("INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_user_id,status,attempt_count,max_attempts,created_at,updated_at) VALUES('delivery-b','W0000000000000000000I','task','run','delivery-key-b','user_notification','P0000000000000000000B','delivered',1,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO user_notification_outbox(id,task_delivery_id,workspace_id,recipient_principal_id,task_id,run_id,payload_json,status,created_at,delivered_at,acknowledged_at) VALUES('late','delivery-b','W0000000000000000000I','P0000000000000000000B','task','run','{}','acknowledged',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    assert_eq!(unread(&store, B).await, 0);
}

#[tokio::test]
async fn internal_origin_turn_provenance_and_admission_use_the_same_task_scope() {
    let (_dir, store, _writer) = fixture(false).await;
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

#[tokio::test]
async fn scheduled_and_recurring_trigger_history_does_not_depend_on_current_task_state() {
    let (_dir, store, _writer) = fixture(false).await;
    task(
        &store,
        "recurring",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "detached",
    )
    .await;
    let db = store.database_connection();
    for (ix, kind) in [
        "immediate",
        "scheduled_at",
        "interval",
        "cron",
        "manual",
        "external",
        "dependency",
    ]
    .iter()
    .enumerate()
    {
        let trigger = format!("trigger-{ix}");
        let run_id = format!("run-{ix}");
        // The list deliberately does not parse trigger configuration or join
        // an execution/timeline subscription to discover saved completions.
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO task_trigger(id,task_id,kind,status,spec_json,created_at,updated_at) VALUES(?,'recurring',?,'paused','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",[trigger.clone().into(),(*kind).into()])).await.unwrap();
        run(&store, &run_id, "recurring", "succeeded", ix as i64 + 1).await;
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE task_run SET trigger_id=? WHERE id=?",
            [trigger.into(), run_id.into()],
        ))
        .await
        .unwrap();
    }
    db.execute_unprepared(
        "UPDATE task SET status='scheduled',completed_at=NULL WHERE id='recurring'",
    )
    .await
    .unwrap();
    assert_eq!(unread(&store, A).await, 7);
    assert_eq!(
        list_task_completion_inbox(&db, &access(A), None, 30)
            .await
            .unwrap()
            .len(),
        7
    );
}

#[tokio::test]
async fn same_run_updates_preserve_personal_read_and_list_projection_is_bounded() {
    let (_dir, store, _writer) = fixture(false).await;
    task(
        &store,
        "task",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "attached",
    )
    .await;
    run(&store, "run", "task", "blocked", 1).await;
    read(&store, A, "run").await;
    let db = store.database_connection();
    let result=serde_json::json!({"summary":"界".repeat(2048),"data":{"html":"x".repeat(65536)},"artifacts":[]}).to_string();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE task_run SET status='succeeded',result_json=?,error_json=NULL WHERE id='run'",
        [result.into()],
    ))
    .await
    .unwrap();
    let rows = list_task_completion_inbox(&db, &access(A), None, 30)
        .await
        .unwrap();
    assert!(rows.is_empty());
    let row = find_task_completion_inbox_entry(&db, &access(A), "run")
        .await
        .unwrap()
        .unwrap();
    assert!(row.read_at.is_some());
    assert_eq!(row.summary.as_ref().unwrap().chars().count(), 1024);
    assert_eq!(unread(&store, A).await, 0);
    let failure=serde_json::json!({"code":"provider-error","message":"m".repeat(4096),"class":"provider","details":{"secret":"not for the list"}}).to_string();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE task_run SET status='failed',result_json=NULL,error_json=? WHERE id='run'",
        [failure.into()],
    ))
    .await
    .unwrap();
    let row = find_task_completion_inbox_entry(&db, &access(A), "run")
        .await
        .unwrap()
        .unwrap();
    let error = row.error_json.unwrap();
    assert!(error.len() < 1536);
    assert!(!error.contains("not for the list"));
    assert!(row.read_at.is_some());
    // A reopened blocked run is no longer a completed entry despite its stale
    // saved completion timestamp. The later terminal state reuses its run ID.
    db.execute_unprepared("UPDATE task_run SET status='running' WHERE id='run'")
        .await
        .unwrap();
    assert!(
        find_task_completion_inbox_entry(&db, &access(A), "run")
            .await
            .unwrap()
            .is_none()
    );
    db.execute_unprepared("UPDATE task_run SET status='succeeded',error_json=NULL WHERE id='run'")
        .await
        .unwrap();
    // Reopening the same run does not revoke its personal read receipt.
    assert!(
        list_task_completion_inbox(&db, &access(A), None, 30)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(unread(&store, A).await, 0);
    assert!(
        find_task_completion_inbox_entry(&db, &access(A), "run")
            .await
            .unwrap()
            .unwrap()
            .read_at
            .is_some()
    );
}

#[tokio::test]
async fn an_unloaded_old_payload_is_not_parsed_to_return_a_metadata_page_or_total() {
    let (_dir, store, _writer) = fixture(false).await;
    task(
        &store,
        "task",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "detached",
    )
    .await;
    for ix in 0..31 {
        run(&store, &format!("run-{ix:03}"), "task", "succeeded", ix + 1).await;
    }
    // Deliberately poison an older payload to prove it is outside the page's
    // projection boundary. No schema/production invariant is relaxed.
    store
        .database_connection()
        .execute_unprepared(
            "UPDATE task_run SET result_json='unloaded legacy payload' WHERE id='run-000'",
        )
        .await
        .unwrap();
    let page = list_task_completion_inbox(&store.database_connection(), &access(A), None, 30)
        .await
        .unwrap();
    assert_eq!(page.len(), 30);
    assert_eq!(page.last().unwrap().run_id, "run-001");
    assert_eq!(unread(&store, A).await, 31);
}

#[tokio::test]
async fn empty_and_small_workspaces_page_independently_of_large_foreign_history() {
    let (_dir, store, _writer) = fixture(false).await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("INSERT INTO task(id,workspace_id,owner_kind,owner_id,executor_kind,status,title,goal,priority,revision,created_at,updated_at) VALUES('foreign-task','W0000000000000000000O','workspace','W0000000000000000000O','system','completed','Foreign','Goal',0,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<4096) INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind,completed_at,result_json,created_at,updated_at) SELECT printf('foreign-%05d',n),'foreign-task',printf('foreign-%05d',n),1,n,'succeeded','system','2099-01-01 00:00:00 +00:00','foreign payload must not be decoded',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM numbers").await.unwrap();
    // No tasks in the requested workspace: the outer workspace index seek
    // yields no run lookups, regardless of newer completions elsewhere.
    assert!(
        list_task_completion_inbox(&db, &access(A), None, 2)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        count_unread_task_completion_inbox(&db, &access(A))
            .await
            .unwrap(),
        0
    );
    task(
        &store,
        "local",
        Some("T0000000000000000000P"),
        None,
        "tool",
        "none",
        "attached",
    )
    .await;
    run(&store, "local-pending", "local", "running", 6).await;
    assert!(
        list_task_completion_inbox(&db, &access(A), None, 2)
            .await
            .unwrap()
            .is_empty()
    );
    for ix in 0..5 {
        run(&store, &format!("local-{ix}"), "local", "succeeded", ix + 1).await;
    }
    let mut cursor: Option<(String, String)> = None;
    let mut ids = Vec::new();
    loop {
        let page = list_task_completion_inbox(
            &db,
            &access(A),
            cursor.as_ref().map(|(at, id)| (at.as_str(), id.as_str())),
            2,
        )
        .await
        .unwrap();
        assert_eq!(
            count_unread_task_completion_inbox(&db, &access(A))
                .await
                .unwrap(),
            5
        );
        assert!(
            page.iter()
                .all(|row| row.task_id == "local" && row.summary.as_deref() == Some("summary"))
        );
        if page.is_empty() {
            break;
        }
        let last = page.last().unwrap();
        cursor = Some((last.completed_key.clone(), last.run_id.clone()));
        ids.extend(page.into_iter().map(|row| row.run_id));
    }
    assert_eq!(ids, ["local-4", "local-3", "local-2", "local-1", "local-0"]);
    // Replaying the loaded prefix after an ACL loss and a resumed run must
    // remove both rows. Continuation starts after the new, authorized page.
    task(
        &store,
        "hidden",
        Some("T0000000000000000000V"),
        None,
        "agent",
        "thread",
        "detached",
    )
    .await;
    run(&store, "private-run", "hidden", "succeeded", 1).await;
    read(&store, A, "local-2").await;
    db.execute_unprepared("DELETE FROM thread_membership WHERE principal_id='P0000000000000000000A' AND thread_id='T0000000000000000000V'").await.unwrap();
    db.execute_unprepared("UPDATE task_run SET status='running' WHERE id='local-3'")
        .await
        .unwrap();
    let page = list_task_completion_inbox(&db, &access(A), None, 3)
        .await
        .unwrap();
    assert_eq!(
        page.iter()
            .map(|row| row.run_id.as_str())
            .collect::<Vec<_>>(),
        ["local-4", "local-1", "local-0"]
    );
    assert!(page.iter().all(|row| row.read_at.is_none()));
    assert_eq!(
        count_unread_task_completion_inbox(&db, &access(A))
            .await
            .unwrap(),
        3
    );
    let last = page.last().unwrap();
    let next = list_task_completion_inbox(
        &db,
        &access(A),
        Some((&last.completed_key, &last.run_id)),
        3,
    )
    .await
    .unwrap();
    assert_eq!(
        next.iter()
            .map(|row| row.run_id.as_str())
            .collect::<Vec<_>>(),
        Vec::<&str>::new()
    );
}

#[tokio::test]
async fn read_entity_preserves_personal_keys_and_orphans_do_not_create_cards_with_fk_off() {
    use pioneer_entity::{task_run, task_run_read};
    let (_dir, store, _writer) = fixture(false).await;
    task(
        &store,
        "task",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "attached",
    )
    .await;
    run(&store, "first", "task", "succeeded", 1).await;
    run(&store, "second", "task", "succeeded", 2).await;
    read(&store, A, "first").await;
    read(&store, B, "first").await;
    read(&store, A, "second").await;
    let db = store.with_maintenance_access().database_connection();
    let receipt = task_run_read::Entity::find_by_id((A.to_owned(), "first".to_owned()))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.read_at, "2026-10-16 11:00:00 +00:00");
    assert_eq!(receipt.principal_id, A);
    assert_eq!(receipt.run_id, "first");
    mark_task_completion_inbox_read(&db, &access(A), "first", "2026-10-17 11:00:00 +00:00")
        .await
        .unwrap();
    assert_eq!(
        task_run_read::Entity::find_by_id((A.to_owned(), "first".to_owned()))
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .read_at,
        receipt.read_at,
        "a repeated read keeps the original personal receipt"
    );
    assert_eq!(
        task_run_read::Entity::find()
            .filter(task_run_read::Column::RunId.eq("first"))
            .all(&db)
            .await
            .unwrap()
            .len(),
        2,
        "the composite key keeps one receipt per principal and run"
    );
    // Fixture-only physical deletion: Gateway does not expose a run deletion
    // operation. OFF leaves orphan receipts; they must never drive the inbox.
    task_run::Entity::delete_by_id("first")
        .exec(&db)
        .await
        .unwrap();
    assert_eq!(
        task_run_read::Entity::find()
            .filter(task_run_read::Column::RunId.eq("first"))
            .all(&db)
            .await
            .unwrap()
            .len(),
        2,
        "declarative ON DELETE CASCADE does not clean up with FK enforcement OFF"
    );
    assert!(
        task_run_read::Entity::find_by_id((A.to_owned(), "second".to_owned()))
            .one(&db)
            .await
            .unwrap()
            .is_some()
    );
    for (principal, expected_unread) in [(A, 0), (B, 1)] {
        let page = list_task_completion_inbox(&db, &access(principal), None, 30)
            .await
            .unwrap();
        assert_eq!(page.len() as u64, expected_unread);
        if expected_unread > 0 {
            assert_eq!(page[0].run_id, "second");
        }
        assert_eq!(unread(&store, principal).await, expected_unread);
        assert!(
            find_task_completion_inbox_entry(&db, &access(principal), "first")
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn read_requires_an_existing_authorized_terminal_run_with_fk_off() {
    use pioneer_entity::task_run_read;
    let (_dir, store, _writer) = fixture(false).await;
    task(
        &store,
        "private",
        Some("T0000000000000000000V"),
        None,
        "agent",
        "none",
        "attached",
    )
    .await;
    run(&store, "private-run", "private", "succeeded", 1).await;
    run(&store, "running-run", "private", "running", 2).await;
    run(&store, "orphan-run", "private", "succeeded", 3).await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("DELETE FROM thread_membership WHERE principal_id='P0000000000000000000B' AND thread_id='T0000000000000000000V'").await.unwrap();
    db.execute_unprepared("UPDATE task_run SET task_id='missing-task' WHERE id='orphan-run'")
        .await
        .unwrap();
    for run_id in ["missing-run", "private-run", "running-run", "orphan-run"] {
        read(&store, B, run_id).await;
    }
    // Absolute TaskRead still cannot substitute a different workspace or skip
    // run/task existence and terminal checks in the atomic INSERT SELECT.
    let wrong_workspace = TaskCompletionInboxAccess {
        workspace_id: "W0000000000000000000O",
        principal_id: A,
        absolute: true,
    };
    mark_task_completion_inbox_read(
        &db,
        &wrong_workspace,
        "private-run",
        "2026-10-17 11:00:00 +00:00",
    )
    .await
    .unwrap();
    let absolute = TaskCompletionInboxAccess {
        absolute: true,
        ..access(A)
    };
    for run_id in ["missing-run", "running-run", "orphan-run"] {
        mark_task_completion_inbox_read(&db, &absolute, run_id, "2026-10-17 11:00:00 +00:00")
            .await
            .unwrap();
    }
    assert!(
        task_run_read::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .is_empty()
    );
    read(&store, A, "private-run").await;
    let receipts = task_run_read::Entity::find().all(&db).await.unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].principal_id, A);
    assert_eq!(receipts[0].run_id, "private-run");
}

#[tokio::test]
async fn released_upgrade_uses_original_activation_and_survives_reopen_without_receipt_backfill() {
    let (dir, store, writer) = fixture(true).await;
    task(
        &store,
        "upgrade",
        Some("T0000000000000000000P"),
        None,
        "tool",
        "none",
        "detached",
    )
    .await;
    for (id, number) in [("old", 1), ("activation-second", 2), ("new", 3)] {
        run(&store, id, "upgrade", "succeeded", number).await;
    }
    let db = store.database_connection();
    db.execute_unprepared(
        "UPDATE task_run SET completed_at='2026-10-16 09:59:59 +00:00' WHERE id='old'",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE task_run SET completed_at='2026-10-16 10:01:00 +00:00' WHERE id='new'",
    )
    .await
    .unwrap();
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, Some(1))
        .await
        .unwrap();
    db.execute_unprepared("UPDATE seaql_migrations SET applied_at=1792144800 WHERE version='m20261016_000001_task_completion_inbox'").await.unwrap();
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    assert_eq!(
        unread(&store, A).await,
        2,
        "retain the entire activation second and later results"
    );
    let page = list_task_completion_inbox(&db, &access(A), None, 30)
        .await
        .unwrap();
    assert!(page.iter().all(|row| row.run_id != "old"));
    assert!(
        find_task_completion_inbox_entry(&db, &access(A), "old")
            .await
            .unwrap()
            .unwrap()
            .read_at
            .is_none(),
        "activation is not a fabricated personal viewing date"
    );
    assert!(
        pioneer_entity::task_run_read::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .is_empty()
    );
    let baseline = pioneer_entity::task_inbox_activation::Entity::find_by_id(1)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(baseline.started_at, 1792144800);
    drop(db);
    drop(store);
    drop(writer);
    let (reopened, writer) = open(&dir.path().join("inbox.sqlite")).await;
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    assert_eq!(unread(&reopened, A).await, 2);
    assert_eq!(unread(&reopened, B).await, 2);
}

#[tokio::test]
async fn read_all_covers_unloaded_pages_and_leaves_later_commits_at_the_same_time_unread() {
    use pioneer_crud::mark_all_task_completion_inbox_read;
    let (_dir, store, _writer) = fixture(false).await;
    task(
        &store,
        "bulk",
        Some("T0000000000000000000P"),
        None,
        "tool",
        "none",
        "detached",
    )
    .await;
    task(
        &store,
        "hidden-bulk",
        Some("T0000000000000000000V"),
        None,
        "tool",
        "none",
        "detached",
    )
    .await;
    for ix in 0..65 {
        run(
            &store,
            &format!("bulk-{ix:03}"),
            "bulk",
            "succeeded",
            ix + 1,
        )
        .await;
    }
    run(&store, "hidden-bulk-run", "hidden-bulk", "failed", 1).await;
    let db = store.database_connection();
    db.execute_unprepared("DELETE FROM thread_membership WHERE principal_id='P0000000000000000000A' AND thread_id='T0000000000000000000V'").await.unwrap();
    assert_eq!(
        list_task_completion_inbox(&db, &access(A), None, 30)
            .await
            .unwrap()
            .len(),
        30
    );
    mark_all_task_completion_inbox_read(&db, &access(A), "2026-10-16 10:00:00 +00:00")
        .await
        .unwrap();
    assert_eq!(unread(&store, A).await, 0);
    assert_eq!(unread(&store, B).await, 66);
    assert!(
        list_task_completion_inbox(&db, &access(A), None, 30)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        pioneer_entity::task_run_read::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .len(),
        65
    );
    // Writer order, not a lossy time watermark or lexicographic run ID, defines inclusion.
    run(&store, "aaa-later", "bulk", "succeeded", 66).await;
    assert_eq!(unread(&store, A).await, 1);
    assert_eq!(
        list_task_completion_inbox(&db, &access(A), None, 30)
            .await
            .unwrap()[0]
            .run_id,
        "aaa-later"
    );
    let foreign = TaskCompletionInboxAccess {
        workspace_id: "W0000000000000000000O",
        ..access(A)
    };
    mark_all_task_completion_inbox_read(&db, &foreign, "2026-10-16 10:00:00 +00:00")
        .await
        .unwrap();
    assert_eq!(unread(&store, A).await, 1);
    let principal_access = access(A);
    let (bulk_result, ()) = tokio::join!(
        mark_all_task_completion_inbox_read(&db, &principal_access, "2026-10-16 10:00:00 +00:00"),
        run(&store, "racing-completion", "bulk", "succeeded", 67),
    );
    bulk_result.unwrap();
    let included = pioneer_entity::task_run_read::Entity::find_by_id((
        A.to_owned(),
        "racing-completion".to_owned(),
    ))
    .one(&db)
    .await
    .unwrap()
    .is_some();
    let remaining = u64::from(!included);
    assert_eq!(
        unread(&store, A).await,
        remaining,
        "the serialized writer defines which concurrent commit belongs to the operation"
    );
    run(&store, "strictly-after-race", "bulk", "succeeded", 68).await;
    assert_eq!(unread(&store, A).await, remaining + 1);
}

#[tokio::test]
async fn unread_filter_precedes_limit_and_keyset_cursor() {
    let (_dir, store, _writer) = fixture(false).await;
    task(
        &store,
        "pages",
        Some("T0000000000000000000P"),
        None,
        "tool",
        "none",
        "detached",
    )
    .await;
    for ix in 0..80 {
        let id = format!("page-{ix:03}");
        run(&store, &id, "pages", "succeeded", ix + 1).await;
        if ix >= 35 {
            read(&store, A, &id).await;
        }
    }
    let db = store.database_connection();
    let first = list_task_completion_inbox(&db, &access(A), None, 30)
        .await
        .unwrap();
    assert_eq!(first.len(), 30);
    assert_eq!(first[0].run_id, "page-034");
    let anchor = first.last().unwrap();
    let next = list_task_completion_inbox(
        &db,
        &access(A),
        Some((&anchor.completed_key, &anchor.run_id)),
        30,
    )
    .await
    .unwrap();
    assert_eq!(next.len(), 5);
    assert_eq!(unread(&store, A).await, 35);
}

#[tokio::test]
async fn cancellation_receipt_tracks_actor_exact_transition_and_rolls_back_with_projection() {
    use pioneer_protocol::TaskEventPayload;
    let (_dir, store, _writer) = fixture(false).await;
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
    db.execute_unprepared("CREATE TRIGGER reject_inbox_receipt BEFORE INSERT ON task_run_read BEGIN SELECT RAISE(ABORT, 'fixture receipt failure'); END").await.unwrap();
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
    db.execute_unprepared("DROP TRIGGER reject_inbox_receipt")
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
    store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,active_attempt_number,created_at,updated_at) VALUES(?1,?2,?3,'agent_message','completed','{}',1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [format!("saved-{turn}-{item}").into(), turn.into(), item.into()])).await.unwrap();
}

#[tokio::test]
async fn visible_results_resolve_exact_occurrence_and_delivery_with_personal_cross_device_receipts()
{
    use pioneer_crud::mark_viewed_task_completion_inbox_read;
    use pioneer_protocol::TaskCompletionInboxViewedParams;
    let (_dir, store, _writer) = fixture(false).await;
    task(
        &store,
        "visible",
        Some("T0000000000000000000P"),
        None,
        "tool",
        "thread",
        "detached",
    )
    .await;
    for (id, number) in [
        ("first-visible", 1),
        ("second-visible", 2),
        ("third-visible", 3),
    ] {
        run(&store, id, "visible", "succeeded", number).await;
    }
    let db = store.database_connection();
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,prompt_manifest_json,turn_kind,origin,created_at,updated_at) VALUES('child-answer','T0000000000000000000I','completed','{}','task_run','attached_task',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('delivered-answer','T0000000000000000000P','completed','{}','task_run','attached_task',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES('binding','visible','first-visible','T0000000000000000000I','child-answer','initial',1,1,'candidate_created',CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id,created_at,updated_at) VALUES('visible-delivery','W0000000000000000000I','visible','second-visible','visible-key','thread','T0000000000000000000P','delivered',1,1,'delivered-answer',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    saved_result_item(
        &store,
        "delivered-answer",
        &pioneer_protocol::task_delivery_result_item_id("visible-delivery"),
    )
    .await;
    // Two child deliveries share the same occurrence Turn. Only the viewed
    // canonical result item may acknowledge its own run.
    db.execute_unprepared("INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id,created_at,updated_at) VALUES('neighbour-delivery','W0000000000000000000I','visible','third-visible','neighbour-key','thread','T0000000000000000000P','delivered',1,1,'delivered-answer',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    let mut params = TaskCompletionInboxViewedParams {
        workspace_id: access(A).workspace_id.into(),
        thread_id: "T0000000000000000000I".into(),
        run_ids: vec![],
        result_turn_ids: vec!["child-answer".into()],
        result_items: vec![],
    };
    let at = "2026-10-16 10:00:00 +00:00";
    assert_eq!(
        mark_viewed_task_completion_inbox_read(&db, &access(A), &params, at)
            .await
            .unwrap(),
        ["turn:child-answer"]
    );
    assert_eq!(unread(&store, A).await, 2);
    assert_eq!(unread(&store, B).await, 3);
    // Same receipt is visible through a second independent reader and idempotent.
    mark_viewed_task_completion_inbox_read(&db, &access(A), &params, at)
        .await
        .unwrap();
    params.thread_id = "T0000000000000000000P".into();
    params.result_turn_ids = vec!["child-answer".into()];
    assert!(
        mark_viewed_task_completion_inbox_read(&db, &access(A), &params, at)
            .await
            .unwrap()
            .is_empty()
    );
    params.result_turn_ids = vec!["delivered-answer".into()];
    assert!(
        mark_viewed_task_completion_inbox_read(&db, &access(A), &params, at)
            .await
            .unwrap()
            .is_empty(),
        "a whole parent Turn cannot read its child deliveries"
    );
    params.result_turn_ids.clear();
    params.result_items = vec![pioneer_protocol::TaskCompletionInboxViewedResult {
        turn_id: "delivered-answer".into(),
        item_id: "unrelated-parent-answer".into(),
    }];
    assert!(
        mark_viewed_task_completion_inbox_read(&db, &access(A), &params, at)
            .await
            .unwrap()
            .is_empty()
    );
    params.result_items = vec![pioneer_protocol::TaskCompletionInboxViewedResult {
        turn_id: "delivered-answer".into(),
        item_id: pioneer_protocol::task_delivery_result_item_id("visible-delivery"),
    }];
    mark_viewed_task_completion_inbox_read(&db, &access(A), &params, at)
        .await
        .unwrap();
    assert_eq!(
        unread(&store, A).await,
        1,
        "another repeat of the same task stays unread"
    );
    assert_eq!(unread(&store, B).await, 3);
    let receipts = pioneer_entity::task_run_read::Entity::find()
        .all(&db)
        .await
        .unwrap();
    assert_eq!(receipts.len(), 2);
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.run_id != "third-visible")
    );
}

#[tokio::test]
async fn own_cancellation_receipt_remains_personal_when_a_presentation_root_is_added_later() {
    let (_dir, store, _writer) = fixture(false).await;
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
        pioneer_entity::task_run_read::Entity::find_by_id((A.to_owned(), "no-root-run".to_owned()))
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

#[tokio::test]
async fn failed_dedicated_delivery_status_uses_its_exact_canonical_turn_link() {
    use pioneer_crud::mark_viewed_task_completion_inbox_read;
    let (_dir, store, _writer) = fixture(false).await;
    task(
        &store,
        "failure-result",
        Some("T0000000000000000000P"),
        None,
        "system",
        "thread",
        "detached",
    )
    .await;
    run(&store, "failure-result-run", "failure-result", "failed", 1).await;
    let turn_id =
        pioneer_crud::canonical_agent_id('T', "task-delivery-turn\0failure-result-delivery");
    let db = store.database_connection();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO turn(id,thread_id,status,prompt_manifest_json,turn_kind,origin,created_at,updated_at) VALUES(?,'T0000000000000000000P','failed','{}','conversation','task_delivery',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [turn_id.clone().into()])).await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id,created_at,updated_at) VALUES('failure-result-delivery','W0000000000000000000I','failure-result','failure-result-run','failure-result-key','thread','T0000000000000000000P','delivered',1,1,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [turn_id.clone().into()])).await.unwrap();
    let params = pioneer_protocol::TaskCompletionInboxViewedParams {
        workspace_id: access(A).workspace_id.into(),
        thread_id: "T0000000000000000000P".into(),
        run_ids: vec![],
        result_turn_ids: vec![turn_id],
        result_items: vec![],
    };
    assert_eq!(
        mark_viewed_task_completion_inbox_read(
            &db,
            &access(A),
            &params,
            "2026-10-16 10:00:00 +00:00"
        )
        .await
        .unwrap()
        .len(),
        1
    );
    assert_eq!(unread(&store, A).await, 0);
    assert_eq!(unread(&store, B).await, 1);
}

#[tokio::test]
async fn child_delivery_item_does_not_read_the_parent_execution_run_sharing_its_turn() {
    use pioneer_crud::mark_viewed_task_completion_inbox_read;
    let (_dir, store, _writer) = fixture(false).await;
    task(
        &store,
        "parent-visible-task",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "attached",
    )
    .await;
    task(
        &store,
        "child-visible-task",
        Some("T0000000000000000000I"),
        Some("parent-visible-task"),
        "tool",
        "thread",
        "attached",
    )
    .await;
    run(
        &store,
        "parent-visible-run",
        "parent-visible-task",
        "succeeded",
        1,
    )
    .await;
    run(
        &store,
        "child-visible-run",
        "child-visible-task",
        "succeeded",
        1,
    )
    .await;
    let db = store.database_connection();
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,prompt_manifest_json,turn_kind,origin,created_at,updated_at) VALUES('shared-execution-turn','T0000000000000000000I','completed','{}','task_run','attached_task',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES('parent-visible-binding','parent-visible-task','parent-visible-run','T0000000000000000000I','shared-execution-turn','initial',1,1,'candidate_created',CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id,created_at,updated_at) VALUES('exact-child-delivery','W0000000000000000000I','child-visible-task','child-visible-run','exact-child-key','thread','T0000000000000000000I','delivered',1,1,'shared-execution-turn',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    // Both children have finished; their parent is still working. Looking at
    // one result cannot acknowledge its neighbour or the parent's own run.
    db.execute_unprepared("UPDATE turn SET status='in_progress' WHERE id='shared-execution-turn'")
        .await
        .unwrap();
    run(
        &store,
        "second-child-run",
        "child-visible-task",
        "succeeded",
        2,
    )
    .await;
    db.execute_unprepared("INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id,created_at,updated_at) VALUES('second-child-delivery','W0000000000000000000I','child-visible-task','second-child-run','second-child-key','thread','T0000000000000000000I','delivered',1,1,'shared-execution-turn',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    saved_result_item(
        &store,
        "shared-execution-turn",
        &pioneer_protocol::task_delivery_result_item_id("exact-child-delivery"),
    )
    .await;
    saved_result_item(
        &store,
        "shared-execution-turn",
        &pioneer_protocol::task_delivery_result_item_id("second-child-delivery"),
    )
    .await;
    let params = pioneer_protocol::TaskCompletionInboxViewedParams {
        workspace_id: access(A).workspace_id.into(),
        thread_id: "T0000000000000000000I".into(),
        run_ids: vec![],
        result_turn_ids: vec![],
        result_items: vec![pioneer_protocol::TaskCompletionInboxViewedResult {
            turn_id: "shared-execution-turn".into(),
            item_id: pioneer_protocol::task_delivery_result_item_id("exact-child-delivery"),
        }],
    };
    mark_viewed_task_completion_inbox_read(&db, &access(A), &params, "2026-10-16 10:00:00 +00:00")
        .await
        .unwrap();
    assert_eq!(unread(&store, A).await, 2);
    assert_eq!(unread(&store, B).await, 3);
    assert!(
        find_task_completion_inbox_entry(&db, &access(A), "parent-visible-run")
            .await
            .unwrap()
            .unwrap()
            .read_at
            .is_none()
    );
    assert!(
        find_task_completion_inbox_entry(&db, &access(A), "child-visible-run")
            .await
            .unwrap()
            .unwrap()
            .read_at
            .is_some()
    );
    // Leaving the viewport performs no undo. Finishing the containing parent
    // later cannot revoke the already persisted child's personal receipt.
    db.execute_unprepared("UPDATE turn SET status='completed' WHERE id='shared-execution-turn'")
        .await
        .unwrap();
    assert_eq!(unread(&store, A).await, 2);
    assert!(
        find_task_completion_inbox_entry(&db, &access(A), "second-child-run")
            .await
            .unwrap()
            .unwrap()
            .read_at
            .is_none()
    );
}

#[tokio::test]
async fn reviewed_run_requires_accepted_candidate_not_old_answer_or_reviewer() {
    use pioneer_crud::mark_viewed_task_completion_inbox_read as viewed;
    let (_dir, store, _writer) = fixture(false).await;
    task(
        &store,
        "reviewed",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "detached",
    )
    .await;
    run(&store, "reviewed-run", "reviewed", "waiting_review", 1).await;
    let db = store.database_connection();
    for (turn, binding, kind, status, sequence) in [
        (
            "old-answer",
            "old-binding",
            "initial",
            "candidate_created",
            1,
        ),
        (
            "accepted-answer",
            "accepted-binding",
            "revision",
            "candidate_created",
            2,
        ),
        (
            "reviewer-answer",
            "reviewer-binding",
            "review",
            "review_recorded",
            3,
        ),
    ] {
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO turn(id,thread_id,status,prompt_manifest_json,turn_kind,origin,created_at,updated_at) VALUES(?,'T0000000000000000000I','completed','{}','task_run','attached_task',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",[turn.into()])).await.unwrap();
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES(?,'reviewed','reviewed-run','T0000000000000000000I',?,?,?, ?,?,CURRENT_TIMESTAMP)",
            [binding.into(),turn.into(),kind.into(),sequence.into(),sequence.into(),status.into()])).await.unwrap();
        saved_result_item(&store, turn, "final-answer").await;
    }
    for (candidate, binding, turn, round, status) in [
        (
            "old-candidate",
            "old-binding",
            "old-answer",
            1i64,
            "superseded",
        ),
        (
            "accepted-candidate",
            "accepted-binding",
            "accepted-answer",
            2,
            "accepted",
        ),
    ] {
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO task_result_candidate(id,task_id,run_id,task_run_turn_id,thread_id,turn_id,round,status,created_at,updated_at) VALUES(?,'reviewed','reviewed-run',?,'T0000000000000000000I',?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            [candidate.into(),binding.into(),turn.into(),round.into(),status.into()])).await.unwrap();
    }
    let proof = |turn: &str, status_only: bool| pioneer_protocol::TaskCompletionInboxViewedParams {
        workspace_id: access(A).workspace_id.into(),
        thread_id: "T0000000000000000000I".into(),
        run_ids: vec![],
        result_turn_ids: if status_only {
            vec![turn.into()]
        } else {
            vec![]
        },
        result_items: if status_only {
            vec![]
        } else {
            vec![pioneer_protocol::TaskCompletionInboxViewedResult {
                turn_id: turn.into(),
                item_id: "final-answer".into(),
            }]
        },
    };
    let at = "2026-10-16 10:00:00 +00:00";
    assert!(
        viewed(&db, &access(A), &proof("old-answer", false), at)
            .await
            .unwrap()
            .is_empty()
    );
    db.execute_unprepared("UPDATE task_run SET status='succeeded' WHERE id='reviewed-run'")
        .await
        .unwrap();
    for candidate_state in ["rejected", "superseded"] {
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE task_result_candidate SET status=? WHERE id='old-candidate'",
            [candidate_state.into()],
        ))
        .await
        .unwrap();
        // Re-evaluation after completion invalidation, with the old row still
        // visible, must not acknowledge the subsequently accepted result.
        for old in ["old-answer", "reviewer-answer"] {
            for status_only in [false, true] {
                assert!(
                    viewed(&db, &access(A), &proof(old, status_only), at)
                        .await
                        .unwrap()
                        .is_empty()
                );
            }
        }
        assert_eq!(unread(&store, A).await, 1);
    }
    assert!(
        !viewed(&db, &access(A), &proof("accepted-answer", false), at)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(unread(&store, A).await, 0);
    assert_eq!(unread(&store, B).await, 1);
}

#[tokio::test]
async fn terminal_errors_without_candidates_require_the_matching_work_frontier() {
    use pioneer_crud::mark_viewed_task_completion_inbox_read as viewed;
    let (_dir, store, _writer) = fixture(false).await;
    task(
        &store,
        "errors",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "detached",
    )
    .await;
    let db = store.database_connection();
    for (ix, (run_status, turn_status, work_status)) in [
        ("failed", "failed", "failed"),
        ("blocked", "blocked", "blocked"),
        ("cancelled", "interrupted", "cancelled"),
        ("timed_out", "failed", "failed"),
        ("failed", "interrupted", "interrupted"),
    ]
    .into_iter()
    .enumerate()
    {
        let run_id = format!("error-run-{ix}");
        let turn_id = format!("error-turn-{ix}");
        run(&store, &run_id, "errors", run_status, ix as i64 + 1).await;
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO turn(id,thread_id,status,prompt_manifest_json,turn_kind,origin,created_at,updated_at) VALUES(?,'T0000000000000000000I',?,'{}','task_run','attached_task',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", [turn_id.clone().into(), turn_status.into()])).await.unwrap();
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES(?,'errors',?,'T0000000000000000000I',?,'recovery',1,1,?,CURRENT_TIMESTAMP)",
            [format!("error-binding-{ix}").into(),run_id.into(),turn_id.clone().into(),work_status.into()])).await.unwrap();
        let params = pioneer_protocol::TaskCompletionInboxViewedParams {
            workspace_id: access(A).workspace_id.into(),
            thread_id: "T0000000000000000000I".into(),
            run_ids: vec![],
            result_turn_ids: vec![turn_id],
            result_items: vec![],
        };
        assert!(
            !viewed(&db, &access(A), &params, "2026-10-16 10:00:00 +00:00")
                .await
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(unread(&store, A).await, 0);
    assert_eq!(unread(&store, B).await, 5);
}

#[tokio::test]
async fn ordinary_negative_batches_keep_viewport_scope_checks_and_direct_run_evidence() {
    use pioneer_crud::mark_viewed_task_completion_inbox_read as viewed;
    let (_dir, store, _writer) = fixture(false).await;
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
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO turn(id,thread_id,status,prompt_manifest_json,turn_kind,origin,created_at,updated_at) VALUES(?,'T0000000000000000000P','completed','{}','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
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
#[tokio::test]
async fn consolidated_inbox_migrations_preserve_released_prefixes_and_schema() {
    const NAMES: [&str; 3] = [
        "m20261016_000001_task_completion_inbox",
        "m20261017_000001_task_inbox_activation",
        "m20261018_000002_task_inbox_query_indexes",
    ];
    let names = Migrator::migrations()
        .iter()
        .map(|m| m.name().to_owned())
        .collect::<Vec<_>>();
    let positions = NAMES.map(|n| names.iter().position(|v| v == n).unwrap());
    assert!(positions[0] < positions[1] && positions[1] < positions[2]);
    assert_eq!(
        names[positions[0] + 1],
        "m20261015_000001_html_preview_retention"
    );
    assert_eq!(
        names[positions[1] + 1],
        "m20261018_000001_task_cancellation_actor"
    );
    let mut expected_schema = None;
    for stage in 0..=3 {
        let (_dir, store, writer) = fixture(true).await;
        let db = store.database_connection();
        task(
            &store,
            "compat",
            Some("T0000000000000000000P"),
            None,
            "tool",
            "none",
            "detached",
        )
        .await;
        run(&store, "compat-late", "compat", "succeeded", 1).await;
        if stage > 0 {
            writer
                .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, Some(1))
                .await
                .unwrap();
            db.execute_unprepared("UPDATE seaql_migrations SET applied_at=1792108800 WHERE version='m20261016_000001_task_completion_inbox'").await.unwrap();
            // A receipt must survive, while a late legacy ack must NOT trigger
            // re-execution of the already applied initial backfill.
            db.execute_unprepared("INSERT INTO task_run_read(principal_id,run_id,read_at) VALUES('reader','retained','2026-10-16 12:34:56+00:00')").await.unwrap();
            if stage >= 2 {
                let end = positions[stage - 1];
                writer
                    .run_migrations::<Migrator>(
                        SqliteWriteClass::Maintenance,
                        Some((end - positions[0]) as u32),
                    )
                    .await
                    .unwrap();
            }
        }
        db.execute_unprepared("INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_user_id,status,attempt_count,max_attempts,created_at,updated_at) VALUES('compat-delivery','W0000000000000000000I','compat','compat-late','compat-key','user_notification','P0000000000000000000B','delivered',1,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        db.execute_unprepared("INSERT INTO user_notification_outbox(id,task_delivery_id,workspace_id,recipient_principal_id,task_id,run_id,payload_json,status,created_at,acknowledged_at) VALUES('compat-legacy','compat-delivery','W0000000000000000000I','P0000000000000000000B','compat','compat-late','{}','acknowledged',CURRENT_TIMESTAMP,'2026-10-16T12:34:56Z')").await.unwrap();
        let saved_activation = if stage >= 2 {
            Some(
                db.query_one_raw(Statement::from_string(
                    DbBackend::Sqlite,
                    "SELECT started_at FROM task_inbox_activation WHERE id=1",
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get::<i64>("", "started_at")
                .unwrap(),
            )
        } else {
            None
        };
        writer
            .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
            .await
            .unwrap();
        let row = db.query_one_raw(Statement::from_string(DbBackend::Sqlite,
            "SELECT a.started_at,m.applied_at,a.completion_sequence FROM task_inbox_activation a JOIN seaql_migrations m ON m.version='m20261016_000001_task_completion_inbox' WHERE a.id=1")).await.unwrap().unwrap();
        assert_eq!(
            row.try_get::<i64>("", "started_at").unwrap(),
            row.try_get::<i64>("", "applied_at").unwrap()
        );
        if let Some(at) = saved_activation {
            assert_eq!(row.try_get::<i64>("", "started_at").unwrap(), at);
        }
        assert_eq!(row.try_get::<i64>("", "completion_sequence").unwrap(), 0);
        if stage > 0 {
            let receipt = db.query_one_raw(Statement::from_string(DbBackend::Sqlite,"SELECT read_at FROM task_run_read WHERE principal_id='reader' AND run_id='retained'")).await.unwrap().unwrap();
            assert_eq!(
                receipt.try_get::<String>("", "read_at").unwrap(),
                "2026-10-16 12:34:56+00:00"
            );
        }
        let late_receipt = pioneer_entity::task_run_read::Entity::find_by_id((
            B.to_owned(),
            "compat-late".to_owned(),
        ))
        .one(&db)
        .await
        .unwrap();
        assert_eq!(
            late_receipt.is_some(),
            stage == 0,
            "initial backfill runs exactly once, before a later acknowledgement"
        );
        let schema = db
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name",
            ))
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                (
                    row.try_get::<String>("", "name").unwrap(),
                    row.try_get::<Option<String>>("", "sql").unwrap(),
                )
            })
            .collect::<Vec<_>>();
        if let Some(expected) = &expected_schema {
            assert_eq!(&schema, expected);
        } else {
            expected_schema = Some(schema);
        }
        // Running the chain again is a no-op, including activation and receipts.
        writer
            .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
            .await
            .unwrap();
    }
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

#[tokio::test]
async fn current_workspace_grant_orders_same_second_completions_and_survives_devices_regrant() {
    let (dir, store, writer) = fixture(false).await;
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
    let (store, _writer) = open(&dir.path().join("inbox.sqlite")).await;
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

#[tokio::test]
async fn private_grant_visibility_routes_and_absolute_start_are_personal() {
    let (_dir, store, _writer) = fixture(false).await;
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

#[tokio::test]
async fn access_start_list_count_keyset_and_read_all_share_unloaded_eligibility() {
    let (_dir, store, _writer) = fixture(false).await;
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

#[tokio::test]
async fn grant_and_completion_race_uses_durable_writer_order_not_request_clock() {
    let (_dir, store, _writer) = fixture(false).await;
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

#[tokio::test]
async fn publication_intervals_use_operation_time_and_preserve_continuous_participants() {
    let (_dir, store, _writer) = fixture(false).await;
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

#[tokio::test]
async fn legacy_grant_uses_epoch_and_retains_the_ambiguous_second_without_receipts() {
    let (_dir, store, _writer) = fixture(false).await;
    task(
        &store,
        "legacy-access",
        Some("T0000000000000000000P"),
        None,
        "webhook",
        "none",
        "detached",
    )
    .await;
    let db = store.database_connection();
    db.execute_unprepared("UPDATE workspace_membership SET created_at='2026-10-16 13:00:00.999999999 +03:00' WHERE principal_id='P0000000000000000000A'").await.unwrap();
    for (i, id, at) in [
        (1, "legacy-pre", "2026-10-16T09:59:59Z"),
        (2, "legacy-tie", "2026-10-16 10:00:00 +00:00"),
        (3, "legacy-post", "2026-10-16T12:00:01+02:00"),
    ] {
        run(&store, id, "legacy-access", "succeeded", i).await;
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE task_run SET completed_at=? WHERE id=?",
            [at.into(), id.into()],
        ))
        .await
        .unwrap();
    }
    assert_eq!(
        unread(&store, A).await,
        2,
        "legacy completion seconds cannot prove subsecond order; do not discard legitimate tied completions"
    );
    assert!(
        pioneer_entity::task_run_read::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(unread(&store, B).await, 3);
}
