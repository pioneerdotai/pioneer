//! Isolated future fixtures only. Do not run/compile test targets before review.
use migration::{Migrator, MigratorTrait};
use pioneer_compaction::frozen::FrozenHistoryRef;
use pioneer_crud::{CrudStore, FrozenStorageLifetimeProgress};
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};

async fn db_fixture(legacy: bool) -> sea_orm::DatabaseConnection {
    let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
    options.max_connections(1).min_connections(1);
    let db = sea_orm::Database::connect(options).await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    let pragma: i64 = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA foreign_keys",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "foreign_keys")
        .unwrap();
    assert_eq!(pragma, 0, "production single writer uses FK OFF");
    let last = Migrator::migrations().len() - 1;
    Migrator::up(&db, if legacy { Some(last as u32) } else { None })
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',1,1),('other','other',1,0)").await.unwrap();
    db.execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('other-thread','other','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db
}
async fn fixture() -> CrudStore {
    CrudStore::new(db_fixture(false).await)
}
async fn sql(store: &CrudStore, sql: &str, values: Vec<sea_orm::Value>) {
    store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            sql,
            values,
        ))
        .await
        .unwrap();
}
async fn n(store: &CrudStore, sql: &str, id: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            sql,
            [id.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}
async fn header(
    store: &CrudStore,
    id: &str,
    workspace: &str,
    owner: &str,
    messages: i64,
    imports: i64,
) -> FrozenHistoryRef {
    sql(store,"INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,next_import,imports_sha256,ready) VALUES(?1,?2,?3,?4,?5,?5,?6,?6,?4,1)",vec![id.into(),workspace.into(),owner.into(),"a".repeat(64).into(),messages.into(),imports.into()]).await;
    for ordinal in 0..messages {
        sql(store,"INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES(?1,?2,'{}',2)",vec![id.into(),ordinal.into()]).await;
    }
    for ordinal in 0..imports {
        sql(store,"INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES(?1,?2,0,'event:turn','e','event-revision:1',?3,'{}',2)",vec![id.into(),ordinal.into(),owner.into()]).await;
    }
    FrozenHistoryRef {
        format: 1,
        manifest_id: id.into(),
        messages: messages as u64,
        identity_sha256: "a".repeat(64),
    }
}
async fn drive(
    store: &CrudStore,
    progress: &mut FrozenStorageLifetimeProgress,
    quanta: usize,
) -> usize {
    let mut failures = 0;
    for _ in 0..quanta {
        if progress.quantum(store).await.is_err() {
            failures += 1;
        }
    }
    failures
}
async fn expired(store: &CrudStore, id: &str) -> i64 {
    n(
        store,
        "SELECT expired AS n FROM compaction_frozen_history WHERE id=?1",
        id,
    )
    .await
}
async fn physical(store: &CrudStore, id: &str) -> i64 {
    n(store,"SELECT (SELECT count(*) FROM compaction_frozen_message_data WHERE manifest_id=?1)+(SELECT count(*) FROM compaction_frozen_import_data WHERE manifest_id=?1) AS n",id).await
}
async fn task(store: &CrudStore, id: &str, status: &str) {
    sql(store,"INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES(?1,'ws','thread','thread','thread','turn','agent',?2,'fixture','fixture')",vec![id.into(),status.into()]).await;
}
async fn task_input(store: &CrudStore, task: &str, run: &str, history: &FrozenHistoryRef) {
    sql(store,"INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES(?1,?2,?1,1,1,'succeeded','agent')",vec![run.into(),task.into()]).await;
    store
        .insert_task_run_conversation_snapshot_if_absent(
            pioneer_crud::NewTaskRunConversationSnapshot {
                run_id: run.into(),
                task_id: task.into(),
                workspace_id: "ws".into(),
                conversation_thread_id: "thread".into(),
                source_turn_id: None,
                history_json: serde_json::to_string(history).unwrap(),
                created_at: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .unwrap();
}
fn binding(json: String) -> pioneer_crud::NewCliRuntimeThreadBinding {
    let now = chrono::Utc::now().fixed_offset();
    pioneer_crud::NewCliRuntimeThreadBinding {
        thread_id: "thread".into(),
        workspace_id: "ws".into(),
        runtime_id: "runtime".into(),
        runtime_kind: "claude".into(),
        native_thread_id: "native".into(),
        native_session_id: None,
        native_root_thread_id: None,
        native_cwd: None,
        native_model: None,
        resume_cursor_json: json,
        status: "active".into(),
        created_at: now,
        updated_at: now,
    }
}
fn cursor(h: &FrozenHistoryRef) -> String {
    serde_json::json!({"pioneerContext":{"version":4,"nativeThreadId":"native","acceptedTurnId":"turn","acceptedTurnRevision":1,"acceptedTurnDeleted":false,"contextOwnerThreadId":"thread","contextHistoryJson":serde_json::to_string(h).unwrap()}}).to_string()
}

#[tokio::test]
async fn task_status_not_completed_retains_every_run_even_when_runs_succeeded() {
    for status in [
        "failed",
        "cancelled",
        "blocked",
        "running",
        "waiting_review",
    ] {
        let store = fixture().await;
        task(&store, "task", status).await;
        for id in ["first", "retry"] {
            let h = header(&store, id, "ws", "thread", 1, 1).await;
            task_input(&store, "task", id, &h).await;
        }
        let mut p = FrozenStorageLifetimeProgress::default();
        drive(&store, &mut p, 2500).await;
        for id in ["first", "retry"] {
            assert_eq!(expired(&store, id).await, 0, "{status}");
            assert_eq!(physical(&store, id).await, 2);
        }
        sql(
            &store,
            "UPDATE task SET status='completed' WHERE id='task'",
            vec![],
        )
        .await;
        drive(&store, &mut p, 2500).await;
        for id in ["first", "retry"] {
            assert_eq!(expired(&store, id).await, 1);
            assert_eq!(physical(&store, id).await, 0);
        }
        assert_eq!(
            n(
                &store,
                "SELECT count(*) AS n FROM task_run_conversation_snapshot WHERE task_id=?1",
                "task"
            )
            .await,
            2,
            "historical descriptors survive"
        );
    }
}
#[tokio::test]
async fn reader_acquire_root_delete_drop_and_expiry_first_share_scoped_counter() {
    let store = fixture().await;
    let h = header(&store, "input", "ws", "thread", 1, 1).await;
    store
        .upsert_cli_runtime_thread_binding(binding(cursor(&h)))
        .await
        .unwrap();
    let foreground = store.clone();
    let maintenance = store.with_maintenance_access();
    let hold = foreground
        .compaction_acquire_frozen_history("ws", &h)
        .await
        .unwrap();
    store
        .upsert_cli_runtime_thread_binding(binding("{}".into()))
        .await
        .unwrap();
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&maintenance, &mut p, 1500).await;
    assert_eq!(expired(&store, "input").await, 0);
    assert_eq!(physical(&store, "input").await, 2);
    drop(hold);
    drive(&maintenance, &mut p, 1500).await;
    assert_eq!(expired(&store, "input").await, 1);
    assert_eq!(physical(&store, "input").await, 0);
    assert!(
        foreground
            .compaction_acquire_frozen_history("ws", &h)
            .await
            .is_err()
    );
    assert!(
        store
            .upsert_cli_runtime_thread_binding(binding(cursor(&h)))
            .await
            .is_err()
    );
    assert_eq!(
        store
            .get_cli_runtime_thread_binding("thread")
            .await
            .unwrap()
            .unwrap()
            .resume_cursor_json,
        "{}"
    );
}
#[tokio::test]
async fn cancellation_and_panic_drop_readers_without_async_cleanup() {
    let store = fixture().await;
    let h = header(&store, "input", "ws", "thread", 1, 0).await;
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let s = store.clone();
    let d = h.clone();
    let b = barrier.clone();
    let task = tokio::spawn(async move {
        let _hold = s.compaction_acquire_frozen_history("ws", &d).await.unwrap();
        b.wait().await;
        std::future::pending::<()>().await;
    });
    barrier.wait().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let s = store.clone();
    let panic = tokio::spawn(async move {
        let _hold = s.compaction_acquire_frozen_history("ws", &h).await.unwrap();
        panic!("fixture panic after acquisition");
    });
    assert!(panic.await.unwrap_err().is_panic());
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 1500).await;
    assert_eq!(expired(&store, "input").await, 1);
}
#[tokio::test]
async fn root_insert_first_protects_discovered_expiry_and_expiry_first_rejects_root() {
    let store = fixture().await;
    let h = header(&store, "input", "ws", "thread", 1, 0).await;
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 7).await;
    let hold = store
        .compaction_acquire_frozen_history("ws", &h)
        .await
        .unwrap();
    store
        .upsert_cli_runtime_thread_binding(binding(cursor(&h)))
        .await
        .unwrap();
    drop(hold);
    drive(&store, &mut p, 1500).await;
    assert_eq!(expired(&store, "input").await, 0);
    store
        .upsert_cli_runtime_thread_binding(binding("{}".into()))
        .await
        .unwrap();
    drive(&store, &mut p, 1500).await;
    assert_eq!(expired(&store, "input").await, 1);
    assert!(
        store
            .upsert_cli_runtime_thread_binding(binding(cursor(&h)))
            .await
            .is_err()
    );
}
#[tokio::test]
async fn root_write_rollback_preserves_locator_and_completed_task_cannot_reopen_expired_input() {
    let store = fixture().await;
    let h = header(&store, "input", "ws", "thread", 1, 0).await;
    store
        .upsert_cli_runtime_thread_binding(binding(cursor(&h)))
        .await
        .unwrap();
    let db = store.database_connection();
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared("UPDATE thread_cli_runtime_binding SET resume_cursor_json='{}',frozen_manifest_id=NULL WHERE thread_id='thread'").await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        store
            .get_cli_runtime_thread_binding("thread")
            .await
            .unwrap()
            .unwrap()
            .resume_cursor_json,
        cursor(&h)
    );
    task(&store, "task", "completed").await;
    task_input(&store, "task", "run", &h).await;
    store
        .upsert_cli_runtime_thread_binding(binding("{}".into()))
        .await
        .unwrap();
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 1500).await;
    assert_eq!(expired(&store, "input").await, 1);
    assert!(
        db.execute_unprepared("UPDATE task SET status='failed' WHERE id='task'")
            .await
            .is_err()
    );
    assert_eq!(
        n(
            &store,
            "SELECT count(*) AS n FROM task WHERE id=?1 AND status='completed'",
            "task"
        )
        .await,
        1
    );
}

async fn operation(
    store: &CrudStore,
    id: &str,
    status: &str,
    h: &FrozenHistoryRef,
    phase: pioneer_compaction::runner::RunnerPhase,
) {
    let db = store.database_connection();
    db.execute_unprepared("INSERT OR IGNORE INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('owner','ws','thread',1)").await.unwrap();
    sql(store,"INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms,attempts,transient_retries,correction,next_portion) VALUES(?1,'owner',?1,?2,'{}',1000,0,0,0,0)",vec![id.into(),status.into()]).await;
    let state = pioneer_compaction::runner::RunnerState {
        generation: 0,
        deadline_ms: 1000,
        attempts: 0,
        retries: 0,
        corrections: 0,
        target_tokens: 10,
        source_text_projection_version: 0,
        cursor: Default::default(),
        previous_checkpoint: None,
        phase,
        resume_phase: None,
        observation: None,
        diagnostic: None,
    };
    sql(
        store,
        "INSERT INTO compaction_runner_state(operation_id,generation,state) VALUES(?1,0,?2)",
        vec![id.into(), serde_json::to_string(&state).unwrap().into()],
    )
    .await;
    sql(store,"INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) SELECT ?1,id,identity_sha256,imports_sha256,import_count FROM compaction_frozen_history WHERE id=?2",vec![id.into(),h.manifest_id.clone().into()]).await;
}
#[tokio::test]
async fn every_proven_terminal_projection_can_expire_unknown_or_mismatched_runner_cannot() {
    use pioneer_compaction::runner::{FailureKind, RunnerPhase};
    for (status, phase, can_expire) in [
        (
            "completed",
            RunnerPhase::Applied {
                checkpoint: "cp".into(),
            },
            true,
        ),
        (
            "failed",
            RunnerPhase::Failed {
                kind: FailureKind::Deadline,
            },
            true,
        ),
        (
            "cancelled",
            RunnerPhase::Failed {
                kind: FailureKind::Cancelled,
            },
            true,
        ),
        (
            "stale",
            RunnerPhase::Failed {
                kind: FailureKind::Permanent,
            },
            true,
        ),
        (
            "stale",
            RunnerPhase::Commit {
                checkpoint: "cp".into(),
            },
            false,
        ),
        (
            "completed",
            RunnerPhase::Failed {
                kind: FailureKind::Deadline,
            },
            false,
        ),
        (
            "running",
            RunnerPhase::Commit {
                checkpoint: "cp".into(),
            },
            false,
        ),
    ] {
        let store = fixture().await;
        let h = header(&store, "origin", "ws", "thread", 1, 1).await;
        operation(&store, "op", status, &h, phase).await;
        let mut p = FrozenStorageLifetimeProgress::default();
        drive(&store, &mut p, 2500).await;
        assert_eq!(
            expired(&store, "origin").await,
            i64::from(can_expire),
            "{status}"
        );
        assert_eq!(
            physical(&store, "origin").await,
            if can_expire { 0 } else { 2 }
        );
        assert_eq!(
            n(
                &store,
                "SELECT count(*) AS n FROM compaction_operation_projection WHERE operation_id=?1",
                "op"
            )
            .await,
            1
        );
    }
}
#[tokio::test]
async fn mutation_invalidates_derived_terminal_generation_before_resume_or_state_change() {
    use pioneer_compaction::runner::{FailureKind, RunnerPhase};
    let store = fixture().await;
    let h = header(&store, "origin", "ws", "thread", 1, 0).await;
    operation(
        &store,
        "op",
        "failed",
        &h,
        RunnerPhase::Failed {
            kind: FailureKind::Deadline,
        },
    )
    .await;
    store
        .upsert_cli_runtime_thread_binding(binding(cursor(&h)))
        .await
        .unwrap();
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 1500).await;
    assert_eq!(n(&store,"SELECT terminal_generation AS n FROM compaction_operation_projection WHERE operation_id=?1","op").await,0);
    sql(
        &store,
        "UPDATE compaction_runner_state SET generation=1 WHERE operation_id='op'",
        vec![],
    )
    .await;
    assert_eq!(n(&store,"SELECT count(*) AS n FROM compaction_operation_projection WHERE operation_id=?1 AND terminal_generation IS NULL","op").await,1);
    store
        .upsert_cli_runtime_thread_binding(binding("{}".into()))
        .await
        .unwrap();
    drive(&store, &mut p, 1500).await;
    assert_eq!(
        expired(&store, "origin").await,
        0,
        "contradictory generation is not terminal"
    );
}

#[tokio::test]
async fn expired_backing_preserves_foreign_messages_and_distinct_import_intervals() {
    let store = fixture().await;
    let p = header(&store, "p", "ws", "thread", 2, 1).await;
    let h = header(&store, "h", "ws", "thread", 5, 3).await;
    for ordinal in 2..7 {
        sql(&store,"INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES('p',?1,'{}',2)",vec![ordinal.into()]).await;
    }
    for ordinal in 1..5 {
        sql(&store,"INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES('p',?1,0,'event:turn','e','event-revision:1','thread','{}',2)",vec![ordinal.into()]).await;
    }
    sql(&store,"INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES('h',0,0,1),('h',1,0,1)",vec![]).await;
    sql(&store,"INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('h',0,0,5,'p'),('h',1,0,3,'p')",vec![]).await;
    sql(
        &store,
        "UPDATE compaction_frozen_layout SET active=1,pending=0 WHERE manifest_id='h'",
        vec![],
    )
    .await;
    store
        .upsert_cli_runtime_thread_binding(binding(cursor(&h)))
        .await
        .unwrap();
    let mut q = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut q, 6000).await;
    assert_eq!(expired(&store, "p").await, 1);
    assert_eq!(expired(&store, "h").await, 0);
    assert_eq!(
        n(
            &store,
            "SELECT count(*) AS n FROM compaction_frozen_message_data WHERE manifest_id=?1",
            "p"
        )
        .await,
        5
    );
    assert_eq!(
        n(
            &store,
            "SELECT count(*) AS n FROM compaction_frozen_import_data WHERE manifest_id=?1",
            "p"
        )
        .await,
        3
    );
    assert_eq!(
        n(
            &store,
            "SELECT count(*) AS n FROM compaction_frozen_message WHERE manifest_id=?1",
            "h"
        )
        .await,
        5
    );
    assert_eq!(
        n(
            &store,
            "SELECT count(*) AS n FROM compaction_frozen_import WHERE manifest_id=?1",
            "h"
        )
        .await,
        3
    );
    assert!(
        store
            .compaction_acquire_frozen_history("ws", &p)
            .await
            .is_err()
    );
    let hold = store
        .compaction_acquire_frozen_history("ws", &h)
        .await
        .unwrap();
    store
        .upsert_cli_runtime_thread_binding(binding("{}".into()))
        .await
        .unwrap();
    drive(&store, &mut q, 1500).await;
    assert_eq!(expired(&store, "h").await, 0);
    drop(hold);
    // Simulated process restart discards only private cursors/pins.
    drop(q);
    let mut q = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut q, 6000).await;
    assert_eq!(expired(&store, "h").await, 1);
    assert_eq!(physical(&store, "p").await, 0);
    assert_eq!(
        n(
            &store,
            "SELECT count(*) AS n FROM compaction_frozen_span WHERE manifest_id=?1",
            "h"
        )
        .await,
        2,
        "lineage/layout headers are not GC targets"
    );
}
#[tokio::test]
async fn available_container_keeps_own_prefix_but_not_unreferenced_physical_tail() {
    let store = fixture().await;
    let h = header(&store, "p", "ws", "thread", 2, 1).await;
    sql(&store,"INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES('p',2,'{}',2)",vec![]).await;
    sql(&store,"INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES('p',1,0,'event:turn','e','event-revision:1','thread','{}',2)",vec![]).await;
    store
        .upsert_cli_runtime_thread_binding(binding(cursor(&h)))
        .await
        .unwrap();
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 2000).await;
    assert_eq!(expired(&store, "p").await, 0);
    assert_eq!(physical(&store, "p").await, 3);
}
#[tokio::test]
async fn pending_failed_candidate_holds_known_backing_and_does_not_block_another_workspace() {
    let store = fixture().await;
    header(&store, "base", "ws", "thread", 2, 1).await;
    header(&store, "pending", "ws", "thread", 2, 1).await;
    header(&store, "free", "other", "other-thread", 1, 0).await;
    sql(&store,"INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending,candidate,failed) VALUES('pending',0,0,1,'base',1)",vec![]).await;
    let mut p = FrozenStorageLifetimeProgress::default();
    assert!(drive(&store, &mut p, 2500).await > 0);
    assert_eq!(expired(&store, "pending").await, 0);
    assert_eq!(expired(&store, "base").await, 0);
    assert_eq!(physical(&store, "base").await, 3);
    assert_eq!(expired(&store, "free").await, 1);
    assert_eq!(physical(&store, "free").await, 0);
}
#[tokio::test]
async fn healthy_incomplete_capture_holds_only_committed_kind_prefix_and_finish_refuses_gap() {
    let store = fixture().await;
    sql(&store,"INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,next_import,imports_sha256,ready) VALUES('capture','ws','thread',?1,3,1,2,1,?1,0)",vec!["a".repeat(64).into()]).await;
    for ordinal in 0..3 {
        sql(&store,"INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES('capture',?1,'{}',2)",vec![ordinal.into()]).await;
    }
    for ordinal in 0..2 {
        sql(&store,"INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES('capture',?1,0,'event:turn','e','event-revision:1','thread','{}',2)",vec![ordinal.into()]).await;
    }
    let h = FrozenHistoryRef {
        format: 1,
        manifest_id: "capture".into(),
        messages: 3,
        identity_sha256: "a".repeat(64),
    };
    assert!(
        store
            .compaction_finish_frozen_history_held("ws", "thread", &h)
            .await
            .is_err()
    );
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 2000).await;
    assert_eq!(expired(&store, "capture").await, 0);
    assert_eq!(physical(&store, "capture").await, 2);
    assert_eq!(
        n(
            &store,
            "SELECT next_ordinal AS n FROM compaction_frozen_history WHERE id=?1",
            "capture"
        )
        .await,
        1
    );
}
#[tokio::test]
async fn understated_payload_bytes_and_early_empty_pages_fail_before_decode() {
    let store = fixture().await;
    header(&store, "corrupt", "ws", "thread", 1, 1).await;
    let oversized = serde_json::json!({"padding":"x".repeat(300_000)}).to_string();
    for (table, field, imports) in [
        ("compaction_frozen_message_data", "reference_json", false),
        ("compaction_frozen_import_data", "proof_json", true),
    ] {
        sql(
            &store,
            &format!("UPDATE {table} SET {field}=?1 WHERE manifest_id='corrupt'"),
            vec![oversized.clone().into()],
        )
        .await;
        let error = if imports {
            store
                .compaction_frozen_import_page("ws", "thread", "corrupt", 0)
                .await
                .unwrap_err()
        } else {
            store
                .compaction_frozen_history_page("ws", "thread", "corrupt", 0)
                .await
                .unwrap_err()
        };
        assert!(
            error.to_string().contains("bounded readback incomplete"),
            "{error}"
        );
        sql(
            &store,
            &format!("DELETE FROM {table} WHERE manifest_id='corrupt'"),
            vec![],
        )
        .await;
        let error = if imports {
            store
                .compaction_frozen_import_page("ws", "thread", "corrupt", 0)
                .await
                .unwrap_err()
        } else {
            store
                .compaction_frozen_history_page("ws", "thread", "corrupt", 0)
                .await
                .unwrap_err()
        };
        assert!(
            error
                .to_string()
                .contains("page missing before declared count"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn malformed_physical_keys_are_not_delete_permission() {
    let store = fixture().await;
    header(&store, "p", "ws", "thread", 1, 0).await;
    sql(&store,"INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES('p',0,-1,'event:turn','e','event-revision:1','thread','{}',2)",vec![]).await;
    let mut p = FrozenStorageLifetimeProgress::default();
    assert!(drive(&store, &mut p, 2000).await > 0);
    assert_eq!(
        n(
            &store,
            "SELECT count(*) AS n FROM compaction_frozen_import_data WHERE manifest_id=?1",
            "p"
        )
        .await,
        1
    );
}
#[tokio::test]
async fn expiry_rollback_then_restart_sweep_keeps_identity_headers_and_monotonic_tombstone() {
    let store = fixture().await;
    let h = header(&store, "p", "ws", "thread", 1, 1).await;
    sql(&store,"CREATE TRIGGER fixture_expiry_fault BEFORE UPDATE OF expired ON compaction_frozen_history BEGIN SELECT RAISE(ABORT,'fixture commit fault'); END",vec![]).await;
    let mut p = FrozenStorageLifetimeProgress::default();
    assert!(drive(&store, &mut p, 1500).await > 0);
    assert_eq!(expired(&store, "p").await, 0);
    assert_eq!(physical(&store, "p").await, 2);
    sql(&store, "DROP TRIGGER fixture_expiry_fault", vec![]).await;
    drop(p);
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 2000).await;
    assert_eq!(expired(&store, "p").await, 1);
    assert_eq!(physical(&store, "p").await, 0);
    assert_eq!(
        n(
            &store,
            "SELECT message_count AS n FROM compaction_frozen_history WHERE id=?1",
            "p"
        )
        .await,
        h.messages as i64
    );
    assert!(
        store
            .compaction_begin_frozen_history("ws", "thread", &h)
            .await
            .is_err()
    );
    assert!(
        store
            .database_connection()
            .execute_unprepared("UPDATE compaction_frozen_history SET expired=0 WHERE id='p'")
            .await
            .is_err()
    );
}

async fn legacy_binding(json: &str) -> CrudStore {
    let db = db_fixture(true).await;
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO thread_cli_runtime_binding(thread_id,workspace_id,runtime_id,runtime_kind,native_thread_id,resume_cursor_json,status) VALUES('thread','ws','runtime','claude','native',?1,'active')",[json.into()])).await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    CrudStore::new(db)
}
#[tokio::test]
async fn legacy_fragment_cas_replacement_metadata_only_and_delete_recreate_same_timestamp() {
    let legacy = serde_json::json!({"provider":"claude","padding":"x".repeat(150_000)}).to_string();
    for replacement in ["metadata", "replace", "recreate"] {
        let store = legacy_binding(&legacy).await;
        let mut p = FrozenStorageLifetimeProgress::default();
        drive(&store, &mut p, 13).await;
        match replacement {
            "metadata"=>sql(&store,"UPDATE thread_cli_runtime_binding SET status='active',updated_at=created_at WHERE thread_id='thread'",vec![]).await,
            "replace"=>{store.upsert_cli_runtime_thread_binding(binding("{}".into())).await.unwrap();},
            _=>{sql(&store,"DELETE FROM thread_cli_runtime_binding WHERE thread_id='thread'",vec![]).await;store.upsert_cli_runtime_thread_binding(binding("{}".into())).await.unwrap();}
        }
        drive(&store, &mut p, 500).await;
        assert_eq!(n(&store,"SELECT count(*) AS n FROM thread_cli_runtime_binding WHERE thread_id=?1 AND frozen_manifest_id IS NULL","thread").await,1);
        assert_eq!(
            store
                .get_cli_runtime_thread_binding("thread")
                .await
                .unwrap()
                .unwrap()
                .resume_cursor_json,
            if replacement == "metadata" {
                legacy.clone()
            } else {
                "{}".into()
            }
        );
    }
}
#[tokio::test]
async fn malformed_legacy_locator_is_scoped_and_reconciliation_continues_after_repair() {
    let store = legacy_binding("{bad").await;
    header(&store, "blocked", "ws", "thread", 1, 0).await;
    header(&store, "free", "other", "other-thread", 1, 0).await;
    let mut p = FrozenStorageLifetimeProgress::default();
    assert!(drive(&store, &mut p, 2500).await > 0);
    assert_eq!(expired(&store, "blocked").await, 0);
    assert_eq!(expired(&store, "free").await, 1);
    store
        .upsert_cli_runtime_thread_binding(binding("{}".into()))
        .await
        .unwrap();
    drive(&store, &mut p, 2000).await;
    assert_eq!(expired(&store, "blocked").await, 1);
}

#[tokio::test]
async fn oversized_legacy_locator_keeps_unknown_scope_without_starving_other_workspace() {
    // This is a pre-cutover legacy record, not a new authoritative writer.
    // Its size exceeds the bounded reconciliation limit; guessing NULL would
    // allow cleanup of a descriptor hidden in the uninspected remainder.
    let legacy =
        serde_json::json!({"provider":"claude","padding":"x".repeat(9 * 1024 * 1024)}).to_string();
    let store = legacy_binding(&legacy).await;
    header(&store, "blocked", "ws", "thread", 1, 0).await;
    header(&store, "free", "other", "other-thread", 1, 0).await;
    let mut progress = FrozenStorageLifetimeProgress::default();
    assert!(drive(&store, &mut progress, 2500).await > 0);
    assert_eq!(n(&store, "SELECT count(*) AS n FROM thread_cli_runtime_binding WHERE thread_id=?1 AND frozen_manifest_id='';", "thread").await, 1);
    assert_eq!(expired(&store, "blocked").await, 0);
    assert_eq!(physical(&store, "blocked").await, 1);
    assert_eq!(expired(&store, "free").await, 1);
    assert_eq!(physical(&store, "free").await, 0);
    store
        .upsert_cli_runtime_thread_binding(binding("{}".into()))
        .await
        .unwrap();
    drive(&store, &mut progress, 2000).await;
    assert_eq!(expired(&store, "blocked").await, 1);
    assert_eq!(physical(&store, "blocked").await, 0);
}

#[tokio::test]
async fn unknown_completed_task_is_not_a_workspace_root() {
    let db = db_fixture(true).await;
    db.execute_unprepared("INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES('task','ws','thread','thread','thread','turn','agent','completed','fixture','fixture')").await.unwrap();
    db.execute_unprepared("INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES('run','task','run',1,1,'succeeded','agent')").await.unwrap();
    db.execute_unprepared("INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) VALUES('run','task','ws','thread','{bad',CURRENT_TIMESTAMP)").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    let store = CrudStore::new(db);
    header(&store, "free", "ws", "thread", 1, 0).await;
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 2000).await;
    assert_eq!(expired(&store, "free").await, 1);
}
#[tokio::test]
async fn completed_task_legacy_scope_mismatch_is_not_an_independent_input_root() {
    let db = db_fixture(true).await;
    db.execute_unprepared("INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES('task','other','thread','thread','thread','turn','agent','completed','fixture','fixture')").await.unwrap();
    db.execute_unprepared("INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES('run','task','run',1,1,'succeeded','agent')").await.unwrap();
    db.execute_unprepared("INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) VALUES('run','task','ws','thread','{bad',CURRENT_TIMESTAMP)").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    let store = CrudStore::new(db);
    header(&store, "free", "ws", "thread", 1, 0).await;
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 2000).await;
    assert_eq!(expired(&store, "free").await, 1);
}

#[tokio::test]
async fn dense_shared_range_and_reverse_root_query_plans_use_required_indexes() {
    // Future EXPLAIN only on this isolated fixture. Index selection does not
    // assert runtime latency; dense fan-out timing remains a rollout check.
    let store = fixture().await;
    header(&store, "physical", "ws", "thread", 1, 0).await;
    for i in 0..512 {
        let id = format!("logical-{i:04}");
        header(&store, &id, "ws", "thread", 1, 0).await;
        sql(&store,"INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES(?1,0,0,1)",vec![id.clone().into()]).await;
        sql(&store,"INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES(?1,0,0,1,'physical')",vec![id.clone().into()]).await;
        sql(
            &store,
            "UPDATE compaction_frozen_layout SET active=1,pending=0 WHERE manifest_id=?1",
            vec![id.into()],
        )
        .await;
    }
    for (query, index) in [
        (
            "SELECT start,manifest_id FROM compaction_frozen_span WHERE source_manifest='physical' AND kind=0 AND start>=0 ORDER BY start,manifest_id LIMIT 64",
            "frozen_span_source",
        ),
        (
            "SELECT run_id FROM task_run_conversation_snapshot WHERE frozen_manifest_id='physical' AND workspace_id='ws'",
            "task_run_conversation_snapshot_frozen_root",
        ),
        (
            "SELECT turn_id FROM turn_runtime_snapshot WHERE frozen_manifest_id='physical' AND workspace_id='ws'",
            "turn_runtime_snapshot_frozen_root",
        ),
        (
            "SELECT thread_id FROM thread_cli_runtime_binding WHERE frozen_manifest_id='physical' AND workspace_id='ws'",
            "thread_cli_runtime_binding_frozen_root",
        ),
        (
            "SELECT turn_id FROM turn_cli_runtime_binding WHERE frozen_manifest_id='physical' AND workspace_id='ws'",
            "turn_cli_runtime_binding_frozen_root",
        ),
        (
            "SELECT manifest_id FROM compaction_frozen_layout WHERE candidate='physical' AND kind=0",
            "frozen_layout_candidate",
        ),
        (
            "SELECT id FROM compaction_checkpoint WHERE previous='cp' ORDER BY id LIMIT 1",
            "checkpoint_incoming_previous",
        ),
        (
            "SELECT checkpoint_id FROM compaction_coverage WHERE source_id='cp' AND source_scope LIKE 'checkpoint:%'",
            "coverage_incoming_checkpoint",
        ),
    ] {
        let plan = store
            .database_connection()
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                format!("EXPLAIN QUERY PLAN {query}"),
            ))
            .await
            .unwrap();
        assert!(
            plan.iter()
                .any(|r| r.try_get::<String>("", "detail").unwrap().contains(index)),
            "expected indexed reverse lookup {index}"
        );
    }
    let hold = store
        .compaction_acquire_frozen_history(
            "ws",
            &FrozenHistoryRef {
                format: 1,
                manifest_id: "logical-0000".into(),
                messages: 1,
                identity_sha256: "a".repeat(64),
            },
        )
        .await
        .unwrap();
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 100_000).await;
    assert_eq!(expired(&store, "physical").await, 1);
    assert_eq!(expired(&store, "logical-0000").await, 0);
    assert_eq!(physical(&store, "physical").await, 1);
    drop(hold);
    drive(&store, &mut p, 100_000).await;
    assert_eq!(expired(&store, "logical-0000").await, 1);
    assert_eq!(physical(&store, "physical").await, 0);
}

#[tokio::test]
async fn runtime_root_and_undelivered_task_output_remain_independent_of_completed_inputs() {
    let store = fixture().await;
    let h = header(&store, "output", "ws", "thread", 1, 0).await;
    task(&store, "task", "completed").await;
    task_input(&store, "task", "run", &h).await;
    sql(&store,"INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",vec![]).await;
    sql(&store,"INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES('rt','task','run','thread','turn','initial',0,1,'completed',CURRENT_TIMESTAMP)",vec![]).await;
    store
        .compaction_record_task_output("ws", "rt", &h)
        .await
        .unwrap();
    let now = chrono::Utc::now().fixed_offset();
    store
        .upsert_turn_runtime_snapshot(pioneer_crud::NewTurnRuntimeSnapshot {
            turn_id: "turn".into(),
            thread_id: "thread".into(),
            workspace_id: "ws".into(),
            mode_json: "\"Agent\"".into(),
            model: "m".into(),
            provider_name: "p".into(),
            reasoning_effort: None,
            agent_skill_versions_json: None,
            hook_runtime_context_json: "{}".into(),
            workspace_skill_policies_json: "[]".into(),
            input_json: "[]".into(),
            capabilities_json: "[]".into(),
            resolved_artifacts_json: "[]".into(),
            runtime_environment_json: "{}".into(),
            history_json: serde_json::to_string(&h).unwrap(),
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 1500).await;
    assert_eq!(expired(&store, "output").await, 0);
    store.delete_turn_runtime_snapshot("turn").await.unwrap();
    drive(&store, &mut p, 1500).await;
    assert_eq!(
        expired(&store, "output").await,
        0,
        "undelivered/unreviewed output is always its own root"
    );
    sql(
        &store,
        "DELETE FROM compaction_task_output WHERE task_run_turn_id='rt'",
        vec![],
    )
    .await;
    drive(&store, &mut p, 1500).await;
    assert_eq!(expired(&store, "output").await, 1);
}
#[tokio::test]
async fn retry_conflict_winner_keeps_exact_descriptor_and_losing_capture_is_not_a_root() {
    let store = fixture().await;
    task(&store, "task", "failed").await;
    let first = header(&store, "first", "ws", "thread", 1, 0).await;
    let losing = header(&store, "losing", "ws", "thread", 1, 0).await;
    task_input(&store, "task", "run", &first).await;
    let winner = store
        .insert_task_run_conversation_snapshot_if_absent(
            pioneer_crud::NewTaskRunConversationSnapshot {
                run_id: "run".into(),
                task_id: "task".into(),
                workspace_id: "ws".into(),
                conversation_thread_id: "thread".into(),
                source_turn_id: None,
                history_json: serde_json::to_string(&losing).unwrap(),
                created_at: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .unwrap();
    assert_eq!(winner.history_json, serde_json::to_string(&first).unwrap());
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 2000).await;
    assert_eq!(expired(&store, "first").await, 0);
    assert_eq!(expired(&store, "losing").await, 1);
}
#[tokio::test]
async fn wholly_unscoped_legacy_operation_blocks_cleanup_but_locator_reconciliation_continues() {
    let store = legacy_binding("{}").await;
    header(&store, "free", "ws", "thread", 1, 0).await;
    sql(&store,"INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('unknown','', 'thread',1)",vec![]).await;
    sql(&store,"INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms,attempts,transient_retries,correction,next_portion) VALUES('unscoped','unknown','unscoped','failed','{}',1,0,0,0,0)",vec![]).await;
    let mut p = FrozenStorageLifetimeProgress::default();
    assert!(drive(&store, &mut p, 1500).await > 0);
    assert_eq!(expired(&store, "free").await, 0);
    assert_eq!(n(&store,"SELECT count(*) AS n FROM thread_cli_runtime_binding WHERE thread_id=?1 AND frozen_manifest_id IS NULL","thread").await,1);
    sql(
        &store,
        "UPDATE compaction_context SET workspace_id='ws' WHERE owner='unknown'",
        vec![],
    )
    .await;
    drive(&store, &mut p, 1500).await;
    assert_eq!(expired(&store, "free").await, 1);
}
