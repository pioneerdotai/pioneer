//! Completed-run inbox regression coverage. Do not execute before external review.
use migration::{Migrator, MigratorTrait};
use pioneer_crud::{
    CrudStore, TaskCompletionInboxAccess, count_unread_task_completion_inbox,
    find_task_completion_inbox_entry, list_task_completion_inbox, mark_task_completion_inbox_read,
    resolve_task_authorization_scope,
};
use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, Statement};

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
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','Inbox',1,1),('other','Other',1,0)",
        "INSERT INTO thread(id,workspace_id,name,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('public','ws','Public thread','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('private','ws','Private thread','','agent','m','p','active','user','private',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('internal','ws',NULL,'','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO workspace_membership(principal_id,workspace_id,granted_by_actor_kind,created_at,updated_at) VALUES('P0000000000000000000A','ws','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('P0000000000000000000B','ws','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread_membership(principal_id,thread_id,workspace_id,added_by_actor_kind,created_at,updated_at) VALUES('P0000000000000000000A','private','ws','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('P0000000000000000000B','private','ws','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    (dir, store, writer)
}
fn access(principal: &str) -> TaskCompletionInboxAccess<'_> {
    TaskCompletionInboxAccess {
        workspace_id: "ws",
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
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,root_task_id,executor_kind,status,title,goal,priority,delivery_policy_json,lifecycle_policy_json,revision,created_at,updated_at) VALUES(?,'ws','workspace','ws',?,?,?,'completed','Task title','Goal',0,?,?,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
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
                Some("public"),
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
    assert!(page.iter().all(|e| e.thread_id == "public"
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
        Some("private"),
        None,
        "agent",
        "none",
        "attached",
    )
    .await;
    task(
        &store,
        "child",
        Some("public"),
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
    let scope =
        resolve_task_authorization_scope(&store.database_connection(), "child", Some("ws"), None)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(scope.root_thread_id.as_deref(), Some("private"));
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
    store.database_connection().execute_unprepared("DELETE FROM thread_membership WHERE principal_id='P0000000000000000000B' AND thread_id='private'").await.unwrap();
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
        Some("public"),
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
        workspace_id: "other",
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
        Some("public"),
        None,
        "agent",
        "user_notification",
        "detached",
    )
    .await;
    run(&store, "run", "task", "succeeded", 1).await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_user_id,status,attempt_count,max_attempts,created_at,updated_at) VALUES('delivery','ws','task','run','delivery-key','user_notification','P0000000000000000000A','delivered',1,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO user_notification_outbox(id,task_delivery_id,workspace_id,recipient_principal_id,task_id,run_id,payload_json,status,created_at,delivered_at,acknowledged_at) VALUES('legacy','delivery','ws','P0000000000000000000A','task','run','{}','acknowledged',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    // A second delivery/outbox record for the same run is not a second card,
    // and its missing acknowledgement cannot undo an exact saved receipt.
    db.execute_unprepared("INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_user_id,status,attempt_count,max_attempts,created_at,updated_at) VALUES('delivery-duplicate','ws','task','run','duplicate-key','user_notification','P0000000000000000000A','delivered',1,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO user_notification_outbox(id,task_delivery_id,workspace_id,recipient_principal_id,task_id,run_id,payload_json,status,created_at,delivered_at) VALUES('unread-duplicate','delivery-duplicate','ws','P0000000000000000000A','task','run','{}','delivered',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
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
        1
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
    db.execute_unprepared("INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_user_id,status,attempt_count,max_attempts,created_at,updated_at) VALUES('delivery-b','ws','task','run','delivery-key-b','user_notification','P0000000000000000000B','delivered',1,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO user_notification_outbox(id,task_delivery_id,workspace_id,recipient_principal_id,task_id,run_id,payload_json,status,created_at,delivered_at,acknowledged_at) VALUES('late','delivery-b','ws','P0000000000000000000B','task','run','{}','acknowledged',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    assert_eq!(unread(&store, B).await, 0);
}

#[tokio::test]
async fn internal_origin_turn_provenance_and_admission_use_the_same_task_scope() {
    let (_dir, store, _writer) = fixture(false).await;
    let db = store.database_connection();
    db.execute_unprepared("INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES('internal','private','private',1,CURRENT_TIMESTAMP)").await.unwrap();
    task(
        &store,
        "causal",
        Some("internal"),
        None,
        "system",
        "none",
        "detached",
    )
    .await;
    run(&store, "causal-run", "causal", "cancelled", 1).await;
    let scope = resolve_task_authorization_scope(&db, "causal", Some("ws"), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(scope.root_thread_id.as_deref(), Some("private"));
    assert_eq!(
        find_task_completion_inbox_entry(&db, &access(A), "causal-run")
            .await
            .unwrap()
            .unwrap()
            .thread_id,
        "private"
    );
    db.execute_unprepared("INSERT INTO task_execution_admission(task_id,workspace_id,root_thread_id,initiating_principal_id,authorization_context_json,created_at) VALUES('causal','ws','public','P0000000000000000000A','{}',CURRENT_TIMESTAMP)").await.unwrap();
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
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('creator','private','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
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
        "private"
    );
    db.execute_unprepared("UPDATE task SET created_by_thread_id='public' WHERE id='turn-only'")
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
        Some("public"),
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
        Some("public"),
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
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].run_id, "run");
    assert!(rows[0].read_at.is_some());
    assert_eq!(rows[0].summary.as_ref().unwrap().chars().count(), 1024);
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
    assert_eq!(
        list_task_completion_inbox(&db, &access(A), None, 30)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn an_unloaded_old_payload_is_not_parsed_to_return_a_metadata_page_or_total() {
    let (_dir, store, _writer) = fixture(false).await;
    task(
        &store,
        "task",
        Some("public"),
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
    db.execute_unprepared("INSERT INTO task(id,workspace_id,owner_kind,owner_id,executor_kind,status,title,goal,priority,revision,created_at,updated_at) VALUES('foreign-task','other','workspace','other','system','completed','Foreign','Goal',0,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
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
        Some("public"),
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
        Some("private"),
        None,
        "agent",
        "thread",
        "detached",
    )
    .await;
    run(&store, "private-run", "hidden", "succeeded", 1).await;
    read(&store, A, "local-2").await;
    db.execute_unprepared("DELETE FROM thread_membership WHERE principal_id='P0000000000000000000A' AND thread_id='private'").await.unwrap();
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
        ["local-4", "local-2", "local-1"]
    );
    assert!(page[1].read_at.is_some());
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
        ["local-0"]
    );
}
