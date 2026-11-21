//! Frozen storage retention and publication on PostgreSQL.
use pioneer_compaction::frozen::FrozenHistoryRef;
use pioneer_crud::{CrudStore, FrozenStorageLifetimeProgress};
use sea_orm::{
    ConnectionTrait, DbBackend, EntityTrait, IntoActiveModel, Set, Statement, TransactionTrait,
};

async fn fixture() -> CrudStore {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connection().clone();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',TRUE,FALSE),('other','other',TRUE,FALSE)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('other-thread','other','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        writer.execute_unprepared(sql).await.unwrap();
    }
    let reader = fixture.connect_read_only().await.unwrap();
    CrudStore::new(pioneer_sqlite::SqliteDatabase::new(reader, writer))
}
async fn sql(store: &CrudStore, sql: &str, values: Vec<sea_orm::Value>) {
    store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
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
            DbBackend::Postgres,
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
    sql(store,"INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,next_import,imports_sha256,ready) VALUES($1,$2,$3,$4,$5,$5,$6,$6,$4,1)",vec![id.into(),workspace.into(),owner.into(),"a".repeat(64).into(),messages.into(),imports.into()]).await;
    for ordinal in 0..messages {
        sql(store,"INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES($1,$2,'{}',2)",vec![id.into(),ordinal.into()]).await;
    }
    for ordinal in 0..imports {
        sql(store,"INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES($1,$2,0,'event:turn','e-'||$2::bigint,'event-revision:1',$3,'{}',2)",vec![id.into(),ordinal.into(),owner.into()]).await;
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
        match progress.quantum(store).await {
            Ok(true) => {}
            Ok(false) => break,
            Err(error) => {
                if failures == 0 {
                    eprintln!("frozen storage test quantum failed: {error:#}");
                }
                failures += 1;
            }
        }
    }
    failures
}
async fn expired(store: &CrudStore, id: &str) -> i64 {
    n(
        store,
        "SELECT expired AS n FROM compaction_frozen_history WHERE id=$1",
        id,
    )
    .await
}
async fn physical(store: &CrudStore, id: &str) -> i64 {
    n(store,"SELECT (SELECT count(*) FROM compaction_frozen_message_data WHERE manifest_id=$1)+(SELECT count(*) FROM compaction_frozen_import_data WHERE manifest_id=$1) AS n",id).await
}
async fn task(store: &CrudStore, id: &str, status: &str) {
    sql(store,"INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES($1,'ws','thread','thread','thread','turn','agent',$2,'fixture','fixture')",vec![id.into(),status.into()]).await;
}
async fn task_input(store: &CrudStore, task: &str, run: &str, history: &FrozenHistoryRef) {
    sql(store,"INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES($1,$2,$1,1,(SELECT COALESCE(MAX(run_number),0)+1 FROM task_run WHERE task_id=$2),'succeeded','agent')",vec![run.into(),task.into()]).await;
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

#[pioneer_postgres_test::postgres_test]
async fn oversized_cursor_publishes_descriptor_in_tail_without_retaining_other_workspace() {
    let store = fixture().await;
    let retained = header(&store, "large-cursor-root", "ws", "thread", 1, 0).await;
    let free = header(&store, "other-cursor-root", "other", "other-thread", 1, 0).await;
    let receipt = cursor(&retained);
    let mut json = String::with_capacity(9 * 1024 * 1024 + receipt.len() + 16);
    json.push_str(r#"{"padding":""#);
    json.extend(std::iter::repeat_n('x', 9 * 1024 * 1024));
    json.push_str(r#"","#);
    json.push_str(&receipt[1..]);
    store
        .upsert_cli_runtime_thread_binding(binding(json.clone()))
        .await
        .unwrap();

    assert_eq!(
        n(&store, "SELECT count(*) AS n FROM thread_cli_runtime_binding WHERE thread_id='thread' AND frozen_manifest_id=$1", &retained.manifest_id).await,
        1,
        "the write must publish the root before background cleanup can run",
    );
    assert_eq!(
        store
            .get_cli_runtime_thread_binding("thread")
            .await
            .unwrap()
            .unwrap()
            .resume_cursor_json,
        json,
    );

    let mut progress = FrozenStorageLifetimeProgress::default();
    assert_eq!(drive(&store, &mut progress, 2_500).await, 0);
    assert_eq!(expired(&store, &retained.manifest_id).await, 0);
    assert_eq!(physical(&store, &retained.manifest_id).await, 1);
    assert_eq!(expired(&store, &free.manifest_id).await, 1);
    assert_eq!(physical(&store, &free.manifest_id).await, 0);

    store
        .upsert_cli_runtime_thread_binding(binding("{}".to_owned()))
        .await
        .unwrap();
    assert_eq!(drive(&store, &mut progress, 2_000).await, 0);
    assert_eq!(expired(&store, &retained.manifest_id).await, 1);
    assert_eq!(physical(&store, &retained.manifest_id).await, 0);
}

#[pioneer_postgres_test::postgres_test]
async fn task_status_controls_every_accepted_run_input_independently_of_run_status() {
    for status in [
        "failed",
        "cancelled",
        "blocked",
        "running",
        "waiting_review",
        "queued",
        "waiting",
        "unknown_future_status",
        "completed",
    ] {
        let releasing = matches!(status, "completed" | "failed" | "cancelled");
        let store = fixture().await;
        task(&store, "task", status).await;
        for id in ["first", "retry"] {
            let h = header(&store, id, "ws", "thread", 1, 1).await;
            task_input(&store, "task", id, &h).await;
        }
        let mut p = FrozenStorageLifetimeProgress::default();
        if status == "blocked" {
            let db = store.database_connection();
            assert!(
                db.execute_unprepared("UPDATE task SET workspace_id='other' WHERE id='task'")
                    .await
                    .is_err()
            );
            let mut moved = pioneer_entity::task::Entity::find_by_id("task")
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .into_active_model();
            moved.workspace_id = Set("other".into());
            assert!(
                pioneer_entity::task::Entity::insert(moved)
                    .on_conflict(
                        sea_orm::sea_query::OnConflict::column(pioneer_entity::task::Column::Id)
                            .update_column(pioneer_entity::task::Column::WorkspaceId)
                            .to_owned()
                    )
                    .exec(&db)
                    .await
                    .is_err()
            );
        }
        if status == "unknown_future_status" {
            sql(&store, "DELETE FROM task WHERE id='task'", vec![]).await;
            drive(&store, &mut p, 2500).await;
            for id in ["first", "retry"] {
                assert_eq!(
                    expired(&store, id).await,
                    0,
                    "missing Task retains typed inputs"
                );
                assert_eq!(physical(&store, id).await, 2);
            }
            task(&store, "task", status).await;
        }
        drive(&store, &mut p, 2500).await;
        for id in ["first", "retry"] {
            assert_eq!(expired(&store, id).await, i64::from(releasing), "{status}");
            assert_eq!(physical(&store, id).await, if releasing { 0 } else { 2 });
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
                "SELECT count(*) AS n FROM task_run_conversation_snapshot WHERE task_id=$1",
                "task"
            )
            .await,
            2,
            "historical descriptors survive"
        );
    }
}
#[pioneer_postgres_test::postgres_test]
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
#[pioneer_postgres_test::postgres_test]
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
#[pioneer_postgres_test::postgres_test]
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
#[pioneer_postgres_test::postgres_test]
async fn root_write_rollback_and_terminal_tasks_cannot_reopen_expired_input() {
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
    assert_eq!(physical(&store, "input").await, 0);
    let original = pioneer_entity::task::Entity::find_by_id("task")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    for status in ["completed", "failed", "cancelled"] {
        // Neither terminal-to-terminal updates nor projection upserts acquire a root.
        sql(
            &store,
            "UPDATE task SET status=$1 WHERE id='task'",
            vec![status.into()],
        )
        .await;
        let mut projection = original.clone().into_active_model();
        projection.status = Set(status.into());
        pioneer_entity::task::Entity::insert(projection.clone())
            .on_conflict(
                sea_orm::sea_query::OnConflict::column(pioneer_entity::task::Column::Id)
                    .update_columns([
                        pioneer_entity::task::Column::Status,
                        pioneer_entity::task::Column::WorkspaceId,
                    ])
                    .to_owned(),
            )
            .exec(&db)
            .await
            .unwrap();
        for retaining in ["running", "blocked"] {
            assert!(
                db.execute_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "UPDATE task SET status=$1 WHERE id='task'",
                    [retaining.into()]
                ))
                .await
                .is_err()
            );
            let mut reopened = projection.clone();
            reopened.status = Set(retaining.into());
            assert!(
                pioneer_entity::task::Entity::insert(reopened.clone())
                    .on_conflict(
                        sea_orm::sea_query::OnConflict::column(pioneer_entity::task::Column::Id)
                            .update_columns([
                                pioneer_entity::task::Column::Status,
                                pioneer_entity::task::Column::WorkspaceId
                            ])
                            .to_owned()
                    )
                    .exec(&db)
                    .await
                    .is_err()
            );
            // FK OFF leaves the immutable input behind across delete/recreate.
            sql(&store, "DELETE FROM task WHERE id='task'", vec![]).await;
            assert!(
                pioneer_entity::task::Entity::insert(reopened)
                    .exec(&db)
                    .await
                    .is_err()
            );
            pioneer_entity::task::Entity::insert(projection.clone())
                .exec(&db)
                .await
                .unwrap();
        }
        let current = pioneer_entity::task::Entity::find_by_id("task")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, status);
        assert_eq!(current.workspace_id, "ws");
    }
    assert_eq!(expired(&store, "input").await, 1);
    assert!(
        store
            .compaction_acquire_frozen_history("ws", &h)
            .await
            .is_err()
    );
    // Snapshot guards remain unconditional, even for a releasing parent Task.
    assert!(
        db.execute_unprepared(
            "UPDATE task_run_conversation_snapshot SET history_json=history_json WHERE run_id='run'"
        )
        .await
        .is_err()
    );
    // This unsealed fixture may lose its header with FK OFF. Missing payload
    // authority must also reject readmission; no guard is disabled for the fixture.
    sql(
        &store,
        "DELETE FROM compaction_frozen_history WHERE id='input'",
        vec![],
    )
    .await;
    assert!(
        db.execute_unprepared("UPDATE task SET status='blocked' WHERE id='task'")
            .await
            .is_err()
    );
    assert!(
        store
            .compaction_acquire_frozen_history("ws", &h)
            .await
            .is_err()
    );
    assert_eq!(n(&store, "SELECT count(*) AS n FROM task_run_conversation_snapshot WHERE run_id=$1 AND frozen_manifest_id='input'", "run").await, 1);
}

async fn operation(
    store: &CrudStore,
    id: &str,
    status: &str,
    h: &FrozenHistoryRef,
    phase: pioneer_compaction::runner::RunnerPhase,
) {
    let db = store.database_connection();
    db.execute_unprepared("INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('owner','ws','thread',1) ON CONFLICT (owner) DO NOTHING").await.unwrap();
    sql(store,"INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms,attempts,transient_retries,correction,next_portion) VALUES($1,'owner',$1,$2,'{}',1000,0,0,0,0)",vec![id.into(),status.into()]).await;
    let state = pioneer_compaction::runner::RunnerState {
        generation: 0,
        deadline_ms: 1000,
        attempts: 0,
        retries: 0,
        portion_attempts: 0,
        overflow_rejections: 0,
        overflow_input_ceiling: None,
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
        "INSERT INTO compaction_runner_state(operation_id,generation,state) VALUES($1,0,$2::text::json)",
        vec![id.into(), serde_json::to_string(&state).unwrap().into()],
    )
    .await;
    sql(store,"INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) SELECT $1,id,identity_sha256,imports_sha256,import_count FROM compaction_frozen_history WHERE id=$2",vec![id.into(),h.manifest_id.clone().into()]).await;
}
#[pioneer_postgres_test::postgres_test]
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
                "SELECT count(*) AS n FROM compaction_operation_projection WHERE operation_id=$1",
                "op"
            )
            .await,
            1
        );
    }
}
#[pioneer_postgres_test::postgres_test]
async fn pre_runner_failures_expire_through_existing_sweep_unless_counter_or_cli_root_holds() {
    use pioneer_compaction::runner::{FailureKind, RunnerPhase};
    for outcome in ["invalid_source_manifest", "deadline"] {
        for retention in ["none", "attempt", "cli"] {
            let store = fixture().await;
            let h = header(&store, "origin", "ws", "thread", 1, 1).await;
            // Cross the former 64-binding boundary in one existing positive
            // case, without multiplying the outcome/retention matrix.
            let bindings = if outcome == "deadline" && retention == "none" {
                65
            } else {
                1
            };
            for i in 0..bindings {
                let id = if i == 0 {
                    "op".to_owned()
                } else {
                    format!("op-{i:02}")
                };
                operation(
                    &store,
                    &id,
                    "failed",
                    &h,
                    RunnerPhase::Failed {
                        kind: FailureKind::Deadline,
                    },
                )
                .await;
                // Model accumulated admission failures at worker startup.
                sql(
                    &store,
                    "DELETE FROM compaction_runner_state WHERE operation_id=$1",
                    vec![id.clone().into()],
                )
                .await;
                sql(&store, "INSERT INTO compaction_runner_plan(operation_id,source_count,reference_count,descriptor,ready) VALUES($1,1,0,'{}',0)", vec![id.clone().into()]).await;
                sql(
                    &store,
                    "UPDATE compaction_operation SET outcome=$1,attempts=$2 WHERE id=$3",
                    vec![
                        outcome.into(),
                        i64::from(retention == "attempt").into(),
                        id.into(),
                    ],
                )
                .await;
            }
            assert_eq!(n(&store, "SELECT count(*) AS n FROM compaction_operation_projection WHERE manifest_id=$1", "origin").await, bindings);
            if retention == "cli" {
                store
                    .upsert_cli_runtime_thread_binding(binding(cursor(&h)))
                    .await
                    .unwrap();
            }
            let mut progress = FrozenStorageLifetimeProgress::default();
            assert_eq!(drive(&store, &mut progress, 2500).await, 0);
            let releases = retention == "none";
            assert_eq!(
                expired(&store, "origin").await,
                i64::from(releases),
                "{outcome}/{retention}"
            );
            assert_eq!(
                physical(&store, "origin").await,
                if releases { 0 } else { 2 },
                "{outcome}/{retention}"
            );
            assert_eq!(
                n(
                    &store,
                    "SELECT count(*) AS n FROM compaction_runner_state WHERE operation_id=$1",
                    "op"
                )
                .await,
                0
            );
            assert_eq!(n(&store,"SELECT count(*) AS n FROM compaction_operation_projection WHERE operation_id=$1 AND terminal_generation IS NULL","op").await,1);
        }
    }
}

#[pioneer_postgres_test::postgres_test]
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
    // A direct root skips terminal preparation. A reader hold lets preparation
    // derive the marker, then prevents expiry at the final writer guard.
    let hold = store
        .compaction_acquire_frozen_history("ws", &h)
        .await
        .unwrap();
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 1500).await;
    assert_eq!(n(&store,"SELECT terminal_generation AS n FROM compaction_operation_projection WHERE operation_id=$1","op").await,0);
    assert_eq!(expired(&store, "origin").await, 0);
    sql(
        &store,
        "UPDATE compaction_runner_state SET generation=1 WHERE operation_id='op'",
        vec![],
    )
    .await;
    assert_eq!(n(&store,"SELECT count(*) AS n FROM compaction_operation_projection WHERE operation_id=$1 AND terminal_generation IS NULL","op").await,1);
    drop(hold);
    drive(&store, &mut p, 1500).await;
    assert_eq!(
        expired(&store, "origin").await,
        0,
        "contradictory generation is not terminal"
    );
}

#[pioneer_postgres_test::postgres_test]
async fn expired_backing_preserves_foreign_messages_and_distinct_import_intervals() {
    let store = fixture().await;
    let p = header(&store, "p", "ws", "thread", 2, 1).await;
    let h = header(&store, "h", "ws", "thread", 5, 3).await;
    task(&store, "task", "cancelled").await;
    task_input(&store, "task", "run", &p).await;
    for ordinal in 2..7 {
        sql(&store,"INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES('p',$1,'{}',2)",vec![ordinal.into()]).await;
    }
    for ordinal in 1..5 {
        sql(&store,"INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES('p',$1,0,'event:turn','e-'||$1,'event-revision:1','thread','{}',2)",vec![ordinal.into()]).await;
    }
    sql(&store,"INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES('h',0,0,1),('h',1,0,1)",vec![]).await;
    sql(&store,"INSERT INTO compaction_frozen_span(manifest_id,kind,start,\"end\",source_manifest) VALUES('h',0,0,5,'p'),('h',1,0,3,'p')",vec![]).await;
    sql(
        &store,
        "UPDATE compaction_frozen_layout AS layout SET active=1,pending=0,verified_span_revision=(SELECT c.revision FROM compaction_frozen_sweep_consumer c WHERE c.manifest_id=layout.manifest_id AND c.kind=layout.kind) WHERE layout.manifest_id='h'",
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
            "SELECT count(*) AS n FROM compaction_frozen_message_data WHERE manifest_id=$1",
            "p"
        )
        .await,
        5
    );
    assert_eq!(
        n(
            &store,
            "SELECT count(*) AS n FROM compaction_frozen_import_data WHERE manifest_id=$1",
            "p"
        )
        .await,
        3
    );
    assert_eq!(
        n(
            &store,
            "SELECT count(*) AS n FROM compaction_frozen_message WHERE manifest_id=$1",
            "h"
        )
        .await,
        5
    );
    assert_eq!(
        n(
            &store,
            "SELECT count(*) AS n FROM compaction_frozen_import WHERE manifest_id=$1",
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
            "SELECT count(*) AS n FROM compaction_frozen_span WHERE manifest_id=$1",
            "h"
        )
        .await,
        2,
        "lineage/layout headers are not GC targets"
    );
}
#[pioneer_postgres_test::postgres_test]
async fn available_container_keeps_own_prefix_but_not_unreferenced_physical_tail() {
    let store = fixture().await;
    let h = header(&store, "p", "ws", "thread", 2, 1).await;
    sql(&store,"INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES('p',2,'{}',2)",vec![]).await;
    sql(&store,"INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES('p',1,0,'event:turn','e-1','event-revision:1','thread','{}',2)",vec![]).await;
    store
        .upsert_cli_runtime_thread_binding(binding(cursor(&h)))
        .await
        .unwrap();
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 2000).await;
    assert_eq!(expired(&store, "p").await, 0);
    assert_eq!(physical(&store, "p").await, 3);
}
#[pioneer_postgres_test::postgres_test]
async fn pending_failed_candidate_holds_known_backing_and_does_not_block_another_workspace() {
    let store = fixture().await;
    header(&store, "base", "ws", "thread", 2, 1).await;
    header(&store, "pending", "ws", "thread", 2, 1).await;
    header(&store, "free", "other", "other-thread", 1, 0).await;
    sql(&store,"INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending,candidate,failed) VALUES('pending',0,0,1,'base',1)",vec![]).await;
    let mut p = FrozenStorageLifetimeProgress::default();
    assert_eq!(
        drive(&store, &mut p, 2500).await,
        0,
        "a retained failed dependency must not trigger worker backoff"
    );
    assert_eq!(expired(&store, "pending").await, 0);
    assert_eq!(expired(&store, "base").await, 0);
    assert_eq!(physical(&store, "base").await, 3);
    assert_eq!(physical(&store, "pending").await, 3);
    assert_eq!(
        n(
            &store,
            "SELECT failed AS n FROM compaction_frozen_layout WHERE manifest_id=$1 AND kind=0",
            "pending"
        )
        .await,
        1,
        "maintenance must preserve existing failed markers"
    );
    assert_eq!(expired(&store, "free").await, 1);
    assert_eq!(physical(&store, "free").await, 0);
}
#[pioneer_postgres_test::postgres_test]
async fn healthy_incomplete_capture_holds_only_committed_kind_prefix_and_finish_refuses_gap() {
    let store = fixture().await;
    sql(&store,"INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,next_import,imports_sha256,ready) VALUES('capture','ws','thread',$1,3,1,2,1,$1,0)",vec!["a".repeat(64).into()]).await;
    for ordinal in 0..3 {
        sql(&store,"INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES('capture',$1,'{}',2)",vec![ordinal.into()]).await;
    }
    for ordinal in 0..2 {
        sql(&store,"INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES('capture',$1,0,'event:turn','e-'||$1,'event-revision:1','thread','{}',2)",vec![ordinal.into()]).await;
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
            "SELECT next_ordinal AS n FROM compaction_frozen_history WHERE id=$1",
            "capture"
        )
        .await,
        1
    );
}
#[pioneer_postgres_test::postgres_test]
async fn understated_payload_bytes_and_early_empty_pages_fail_before_decode() {
    let store = fixture().await;
    header(&store, "corrupt", "ws", "thread", 1, 1).await;
    let oversized = serde_json::json!({"padding":"x".repeat(300_000)}).to_string();
    for (table, field, imports) in [
        ("compaction_frozen_message_data", "reference_json", false),
        ("compaction_frozen_import_data", "proof_json", true),
    ] {
        // Inject pre-existing corruption only while seeding this isolated DB.
        // Restore CHECK enforcement before exercising the bounded reader.
        let tx = store.database_connection().begin().await.unwrap();
        tx.execute_unprepared(&format!(
            "ALTER TABLE {table} DROP CONSTRAINT {table}_check"
        ))
        .await
        .unwrap();
        tx.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            format!("UPDATE {table} SET {field}=$1 WHERE manifest_id='corrupt'"),
            [oversized.clone().into()],
        ))
        .await
        .unwrap();
        tx.execute_unprepared(&format!(
            "ALTER TABLE {table} ADD CONSTRAINT {table}_check CHECK (bytes BETWEEN 0 AND 262144 AND octet_length({field})=bytes) NOT VALID"
        ))
            .await
            .unwrap();
        tx.commit().await.unwrap();
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

#[pioneer_postgres_test::postgres_test]
async fn malformed_physical_keys_are_not_delete_permission() {
    let store = fixture().await;
    header(&store, "p", "ws", "thread", 1, 0).await;
    sql(&store,"INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES('p',0,-1,'event:turn','e','event-revision:1','thread','{}',2)",vec![]).await;
    // A valid physical tail after the poison key must still be swept.
    sql(&store,"INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES('p',1,0,'event:turn','healthy','event-revision:1','thread','{}',2)",vec![]).await;
    let mut p = FrozenStorageLifetimeProgress::default();
    assert_eq!(
        drive(&store, &mut p, 2000).await,
        0,
        "a poison physical key must not trigger worker backoff"
    );
    assert_eq!(expired(&store, "p").await, 1);
    assert_eq!(physical(&store, "p").await, 1);
    assert_eq!(
        n(&store, "SELECT message_ordinal AS n FROM compaction_frozen_import_data WHERE manifest_id=$1 AND ordinal=0", "p").await,
        -1,
        "the malformed key must stay intact while healthy rows are deleted"
    );
    assert_eq!(
        n(
            &store,
            "SELECT count(*) AS n FROM compaction_frozen_import_data WHERE manifest_id=$1",
            "p"
        )
        .await,
        1
    );
}
#[pioneer_postgres_test::postgres_test]
async fn expiry_rollback_then_restart_sweep_keeps_identity_headers_and_monotonic_tombstone() {
    let store = fixture().await;
    let h = header(&store, "p", "ws", "thread", 1, 1).await;
    store.database_connection().execute_unprepared("CREATE FUNCTION fixture_expiry_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture commit fault'; END; $$; CREATE TRIGGER fixture_expiry_fault BEFORE UPDATE OF expired ON compaction_frozen_history FOR EACH ROW EXECUTE FUNCTION fixture_expiry_fault()").await.unwrap();
    let mut p = FrozenStorageLifetimeProgress::default();
    assert!(drive(&store, &mut p, 1500).await > 0);
    assert_eq!(expired(&store, "p").await, 0);
    assert_eq!(physical(&store, "p").await, 2);
    sql(
        &store,
        "DROP TRIGGER fixture_expiry_fault ON compaction_frozen_history",
        vec![],
    )
    .await;
    drop(p);
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 2000).await;
    assert_eq!(expired(&store, "p").await, 1);
    assert_eq!(physical(&store, "p").await, 0);
    assert_eq!(
        n(
            &store,
            "SELECT message_count AS n FROM compaction_frozen_history WHERE id=$1",
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

#[pioneer_postgres_test::postgres_test]
async fn dense_shared_range_and_reverse_root_query_plans_use_required_indexes() {
    // Future EXPLAIN only on this isolated fixture. Index selection does not
    // assert runtime latency; dense fan-out timing remains a rollout check.
    let store = fixture().await;
    header(&store, "physical", "ws", "thread", 1, 0).await;
    for i in 0..512 {
        let id = format!("logical-{i:04}");
        header(&store, &id, "ws", "thread", 1, 0).await;
        sql(&store,"INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES($1,0,0,1)",vec![id.clone().into()]).await;
        sql(&store,"INSERT INTO compaction_frozen_span(manifest_id,kind,start,\"end\",source_manifest) VALUES($1,0,0,1,'physical')",vec![id.clone().into()]).await;
        sql(
            &store,
            "UPDATE compaction_frozen_layout AS layout SET active=1,pending=0,verified_span_revision=(SELECT c.revision FROM compaction_frozen_sweep_consumer c WHERE c.manifest_id=layout.manifest_id AND c.kind=layout.kind) WHERE layout.manifest_id=$1",
            vec![id.into()],
        )
        .await;
    }
    let db = store.database_connection();
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared("SET LOCAL enable_seqscan = off")
        .await
        .unwrap();
    for (query, index) in [
        (
            "SELECT start,manifest_id FROM compaction_frozen_span WHERE source_manifest='physical' AND kind=0 AND start>=0 ORDER BY start,manifest_id LIMIT 64",
            "frozen_span_source",
        ),
        (
            "SELECT task_id FROM task_run_conversation_snapshot WHERE frozen_manifest_id='physical' AND workspace_id='ws' ORDER BY task_id LIMIT 1",
            "task_run_conversation_snapshot_frozen_root",
        ),
        (
            "SELECT 1 FROM turn_runtime_snapshot WHERE frozen_manifest_id='physical' AND workspace_id='ws' LIMIT 1",
            "turn_runtime_snapshot_frozen_root",
        ),
        (
            "SELECT 1 FROM thread_cli_runtime_binding WHERE frozen_manifest_id='physical' AND workspace_id='ws' LIMIT 1",
            "thread_cli_runtime_binding_frozen_root",
        ),
        (
            "SELECT 1 FROM turn_cli_runtime_binding WHERE frozen_manifest_id='physical' AND workspace_id='ws' LIMIT 1",
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
        let plan = tx
            .query_all_raw(Statement::from_string(
                DbBackend::Postgres,
                format!("EXPLAIN (COSTS OFF) {query}"),
            ))
            .await
            .unwrap();
        let steps: Vec<String> = plan
            .iter()
            .map(|row| row.try_get("", "QUERY PLAN").unwrap())
            .collect();
        let locator_index = index
            .strip_suffix("_frozen_root")
            .map(|table| format!("{table}_frozen_locator_discovery"));
        let indexed = steps.iter().any(|step| {
            step.contains(index)
                || locator_index
                    .as_ref()
                    .is_some_and(|locator| step.contains(locator))
        });
        assert!(
            indexed,
            "expected indexed reverse lookup {index}: {steps:#?}"
        );
        if locator_index.is_some() {
            assert!(
                steps.iter().any(|step| step.contains("Index Cond:") && step.contains("frozen_manifest_id")),
                "reverse lookup must start from the exact manifest: {steps:#?}"
            );
        }
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
    tx.rollback().await.unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn runtime_root_and_undelivered_task_output_remain_independent_of_terminal_inputs() {
    let store = fixture().await;
    let h = header(&store, "output", "ws", "thread", 1, 0).await;
    task(&store, "task", "failed").await;
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
            mode: pioneer_protocol::ThreadMode::Agent,
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
#[pioneer_postgres_test::postgres_test]
async fn retry_conflict_winner_keeps_exact_descriptor_and_losing_capture_is_not_a_root() {
    let store = fixture().await;
    task(&store, "task", "running").await;
    let first = header(&store, "first", "ws", "thread", 1, 0).await;
    let losing = header(&store, "losing", "ws", "thread", 1, 0).await;
    task_input(&store, "task", "run", &first).await;
    sql(
        &store,
        "UPDATE task_run SET status='failed' WHERE id='run'",
        vec![],
    )
    .await;
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 2000).await;
    assert_eq!(
        expired(&store, "first").await,
        0,
        "failed run in an active Task is retryable"
    );
    sql(&store, "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind,retry_of_run_id) VALUES('retry','task','run',2,2,'queued','agent','run')", vec![]).await;
    let accepted = store
        .get_task_run_conversation_snapshot("run")
        .await
        .unwrap()
        .unwrap();
    let retried = store
        .insert_task_run_conversation_snapshot_if_absent(
            pioneer_crud::NewTaskRunConversationSnapshot {
                run_id: "retry".into(),
                task_id: "task".into(),
                workspace_id: "ws".into(),
                conversation_thread_id: "thread".into(),
                source_turn_id: None,
                history_json: accepted.history_json.clone(),
                created_at: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .unwrap();
    assert_eq!(retried.history_json, accepted.history_json);
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
    assert_eq!(
        winner, accepted,
        "conflict preserves the exact immutable record"
    );
    let mut p = FrozenStorageLifetimeProgress::default();
    drive(&store, &mut p, 2000).await;
    assert_eq!(expired(&store, "first").await, 0);
    assert_eq!(physical(&store, "first").await, 1);
    assert_eq!(expired(&store, "losing").await, 1);
}
