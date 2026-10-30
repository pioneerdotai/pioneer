use super::*;
use pioneer_sqlite::{
    SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteReadOutcome,
    SqliteWriteClass, SqliteWriteEvent, SqliteWriteExecutor, SqliteWriteObserver,
};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Routes {
    reads: Mutex<Vec<SqliteReadEvent>>,
    writes: Mutex<Vec<SqliteWriteEvent>>,
    blocked: tokio::sync::Notify,
}
impl SqliteReadObserver for Routes {
    fn observe(&self, event: SqliteReadEvent) {
        if matches!(event, SqliteReadEvent::AdmissionEnqueued { active: 1, .. }) {
            self.blocked.notify_one();
        }
        self.reads.lock().unwrap().push(event);
    }
}
impl SqliteWriteObserver for Routes {
    fn observe(&self, event: SqliteWriteEvent) {
        self.writes.lock().unwrap().push(event);
    }
}
impl Routes {
    fn clear(&self) {
        self.reads.lock().unwrap().clear();
        self.writes.lock().unwrap().clear();
    }
    fn assert_single_read(&self, outcome: SqliteReadOutcome) {
        let reads = self.reads.lock().unwrap();
        let operations: Vec<_> = reads
            .iter()
            .filter_map(|event| match event {
                SqliteReadEvent::OperationFinished { class, outcome, .. } => {
                    Some((*class, *outcome))
                }
                _ => None,
            })
            .collect();
        assert_eq!(operations, [(SqliteReadClass::Maintenance, outcome)]);
        assert!(reads.iter().any(|event| matches!(
            event,
            SqliteReadEvent::AdmissionReleased {
                active: 0,
                queue_depth: 0,
                ..
            }
        )));
        assert!(
            self.writes.lock().unwrap().is_empty(),
            "preflight must not request a writer"
        );
    }
}

// A real query_only reader and separate single writer. No operation projection
// or checkpoint exists: an empty, finished history can lawfully expire directly.
async fn observed_fixture() -> (tempfile::TempDir, CrudStore, Arc<Routes>, FrozenHistoryRef) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("expiry.db");
    let mut options = sea_orm::ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false);
    let writer = Database::connect(options).await.unwrap();
    writer
        .execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    Migrator::up(&writer, None).await.unwrap();
    let mut options = sea_orm::ConnectOptions::new(format!("sqlite://{}?mode=ro", path.display()));
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false)
        .map_sqlx_sqlite_opts(|options| options.pragma("query_only", "ON"));
    let reader = Database::connect(options).await.unwrap();
    let routes = Arc::new(Routes::default());
    let store = CrudStore::new(SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, routes.clone()),
        routes.clone(),
    ));
    assert!(store.connection.reader_query_only_enabled().await.unwrap());
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,created_at,updated_at) VALUES('turn','thread','failed',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,executor_kind,status,title,goal) VALUES('task','ws','thread','thread','agent','failed','fixture','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES('run','task','run',1,1,'failed','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status) VALUES('occurrence','task','run','thread','turn','initial',0,0,'failed')",
    ] {
        store.connection.execute_unprepared(sql).await.unwrap();
    }
    let descriptor = FrozenHistoryRef {
        format: 1,
        manifest_id: "origin".into(),
        messages: 0,
        identity_sha256: hex::encode(Sha256::digest([])),
    };
    store
        .compaction_begin_frozen_history("ws", "thread", &descriptor)
        .await
        .unwrap();
    let hold = store
        .compaction_finish_frozen_history_held("ws", "thread", &descriptor)
        .await
        .unwrap();
    drop(hold);
    routes.clear();
    (directory, store, routes, descriptor)
}

async fn insert_root(store: &CrudStore, descriptor: &FrozenHistoryRef, kind: usize) {
    let history = serde_json::to_string(descriptor).unwrap();
    let json = match kind {
        2 => serde_json::json!({"pioneerContext": {
            "version": 4, "nativeThreadId": "native", "acceptedTurnId": "turn",
            "acceptedTurnRevision": 1, "acceptedTurnDeleted": false,
            "contextOwnerThreadId": "thread", "contextManifestOwnerThreadId": "thread",
            "contextHistoryJson": history,
        }})
        .to_string(),
        3 => serde_json::json!({"pioneerContextBasis": {
            "executionThreadId": "thread",
            "pendingTurn": {"turnId": "turn", "messageRevision": 1, "messageDeleted": false},
            "manifestOwnerThreadId": "thread", "historyJson": history,
        }})
        .to_string(),
        _ => history,
    };
    let prepared = match kind {
        0 | 1 => Some(PreparedFrozenRoot::history(&json).unwrap()),
        2 | 3 => Some(PreparedFrozenRoot::cli(&json, kind == 2).unwrap()),
        _ => None,
    };
    if let Some(prepared) = prepared {
        assert_eq!(prepared.locator(), Some(descriptor.manifest_id.clone()));
        prepared.verify(&store.connection, "ws").await.unwrap();
    }
    let sql = match kind {
        0 => {
            "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,frozen_manifest_id) VALUES('run','task','ws','thread',?1,?2)"
        }
        1 => {
            "INSERT INTO turn_runtime_snapshot(turn_id,thread_id,workspace_id,mode_json,model,provider_name,hook_runtime_context_json,workspace_skill_policies_json,input_json,capabilities_json,resolved_artifacts_json,runtime_environment_json,history_json,frozen_manifest_id) VALUES('turn','thread','ws','{}','m','p','{}','{}','[]','{}','[]','{}',?1,?2)"
        }
        2 => {
            "INSERT INTO thread_cli_runtime_binding(thread_id,workspace_id,runtime_id,runtime_kind,native_thread_id,resume_cursor_json,status,frozen_manifest_id) VALUES('thread','ws','runtime','claude','native',?1,'active',?2)"
        }
        3 => {
            "INSERT INTO turn_cli_runtime_binding(turn_id,thread_id,continuation_thread_id,workspace_id,runtime_id,runtime_kind,native_thread_id,input_mapping_json,status,frozen_manifest_id) VALUES('turn','thread','thread','ws','runtime','claude','native',?1,'active',?2)"
        }
        4 => {
            "INSERT INTO compaction_task_output(task_run_turn_id,task_id,run_id,workspace_id,source_thread,source_turn,manifest_id) SELECT 'occurrence','task','run','ws','thread','turn',?2 WHERE ?1 IS NOT NULL"
        }
        _ => unreachable!(),
    };
    store
        .connection
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            sql,
            [json.into(), descriptor.manifest_id.clone().into()],
        ))
        .await
        .unwrap();
}

fn assert_unprepared(preparation: &ExpiryPreparation) {
    assert_eq!(preparation.layout_kind, 0);
    assert!(preparation.layout.is_none());
    assert!(!preparation.operations_done && preparation.operations_after.is_empty());
    assert!(!preparation.checkpoints_done && preparation.checkpoint_after.is_empty());
    assert!(preparation.reverse.is_none());
}

#[tokio::test]
async fn direct_roots_and_task_release_set_match_final_guard_without_preparation_or_writer() {
    for (kind, status, missing, held) in [
        (0, "blocked", false, true),
        (0, "running", false, true),
        (0, "interrupted", false, true),
        (0, "completed", false, false),
        (0, "failed", false, false),
        (0, "cancelled", false, false),
        (0, "failed", true, true),
        (1, "failed", false, true),
        (2, "failed", false, true),
        (3, "failed", false, true),
        (4, "failed", false, true),
    ] {
        let (_directory, store, routes, descriptor) = observed_fixture().await;
        insert_root(&store, &descriptor, kind).await;
        if kind == 4 {
            // FK OFF can retain inconsistent legacy ownership metadata. The
            // exact direct relation remains a hold in both checks.
            store.connection.execute_unprepared("UPDATE compaction_task_output SET workspace_id='other',source_thread='other' WHERE task_run_turn_id='occurrence'").await.unwrap();
        }
        store
            .connection
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE task SET status=?1 WHERE id='task'",
                [status.into()],
            ))
            .await
            .unwrap();
        if missing {
            // Deliberate FK-OFF orphan: the existing LEFT JOIN retains it.
            store
                .connection
                .execute_unprepared("DELETE FROM task WHERE id='task'")
                .await
                .unwrap();
        }
        let header = history::Entity::find_by_id("origin")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        let mut preparation = ExpiryPreparation::new(header.clone());
        routes.clear();
        let mut accounting = SweepAccounting::default();
        assert_eq!(
            preparation
                .step(&store.with_maintenance_access(), &mut accounting)
                .await
                .unwrap(),
            held,
            "kind={kind}, status={status}, missing={missing}"
        );
        assert!(preparation.roots_checked);
        assert_unprepared(&preparation);
        assert_eq!(accounting.held, u64::from(held));
        assert_eq!(accounting.expired + accounting.refused, 0);
        routes.assert_single_read(SqliteReadOutcome::Ok);
        // Independently execute the unchanged final writer guard. Terminal
        // Tasks release an otherwise unreferenced, mature empty history.
        assert_eq!(
            expire(&store.with_maintenance_access(), &header)
                .await
                .unwrap(),
            !held
        );
    }
}

#[tokio::test]
async fn negative_preflight_runs_once_and_final_writer_observes_late_root() {
    for late_root in [false, true] {
        let (_directory, store, routes, descriptor) = observed_fixture().await;
        let store = store.with_maintenance_access();
        let mut preparation = prepared_expiry(&store).await;
        assert!(preparation.roots_checked);
        assert_eq!(preparation.layout_kind, 2);
        assert!(preparation.operations_done && preparation.checkpoints_done);
        if late_root {
            insert_root(&store, &descriptor, 1).await;
        }
        // Returning an early hold here would incorrectly bypass this writer.
        routes.clear();
        let mut accounting = SweepAccounting::default();
        assert!(preparation.step(&store, &mut accounting).await.unwrap());
        assert_eq!(accounting.expired, u64::from(!late_root));
        assert_eq!(accounting.held, u64::from(late_root));
        assert!(routes.reads.lock().unwrap().is_empty());
        assert!(routes.writes.lock().unwrap().iter().any(|event| matches!(
            event,
            SqliteWriteEvent::Acquired {
                class: SqliteWriteClass::Maintenance,
                ..
            }
        )));
        assert_eq!(
            history::Entity::find_by_id("origin")
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap()
                .expired,
            i64::from(!late_root)
        );
    }
}

#[tokio::test]
async fn negative_preflight_does_not_bypass_active_reader_hold() {
    let (_directory, store, routes, descriptor) = observed_fixture().await;
    let store = store.with_maintenance_access();
    let mut preparation = prepared_expiry(&store).await;
    let hold = store
        .compaction_acquire_frozen_history("ws", &descriptor)
        .await
        .unwrap();
    routes.clear();
    let mut accounting = SweepAccounting::default();
    assert!(preparation.step(&store, &mut accounting).await.unwrap());
    assert_eq!(accounting.held, 1);
    assert_eq!(accounting.expired, 0);
    assert!(routes.writes.lock().unwrap().iter().any(|event| matches!(
        event,
        SqliteWriteEvent::Acquired {
            class: SqliteWriteClass::Maintenance,
            ..
        }
    )));
    drop(hold);
    let mut preparation = prepared_expiry(&store).await;
    assert!(preparation.step(&store, &mut accounting).await.unwrap());
    assert_eq!(accounting.expired, 1);
    assert!(store.frozen_readers.lock().unwrap().is_empty());
}

#[tokio::test]
async fn positive_visit_keeps_cursor_accounting_and_periodic_revisit_after_root_release() {
    let (_directory, store, routes, descriptor) = observed_fixture().await;
    insert_root(&store, &descriptor, 1).await;
    let mut progress = FrozenStorageLifetimeProgress::default();
    progress.scope.done = true;
    progress.pass_done = [true, false, false, true, true, true];
    progress.phase = 2;
    routes.clear();
    assert!(progress.quantum(&store).await.unwrap());
    // Discovery returns the complete lightweight header in one reader call;
    // the next quantum still owns the independent root/hold preflight.
    routes.assert_single_read(SqliteReadOutcome::Ok);
    assert_eq!(
        progress.expiry.as_ref().unwrap().header.id,
        descriptor.manifest_id
    );
    assert!(!progress.expiry.as_ref().unwrap().roots_checked);
    assert_eq!(progress.expiry_after, "origin");
    routes.clear();
    progress.phase = 2;
    assert!(progress.quantum(&store).await.unwrap());
    assert!(progress.expiry.is_none() && !progress.pass_done[2]);
    assert_eq!(progress.expiry_after, "origin");
    assert_eq!(progress.accounting.held, 1);
    assert_eq!(progress.accounting.expired, 0);
    routes.assert_single_read(SqliteReadOutcome::Ok);
    progress.phase = 2;
    assert!(progress.quantum(&store).await.unwrap());
    assert!(progress.pass_done[2] && progress.expiry_after.is_empty());
    store
        .connection
        .execute_unprepared("DELETE FROM turn_runtime_snapshot WHERE turn_id='turn'")
        .await
        .unwrap();
    let now = Instant::now();
    let interval = Duration::from_secs(60);
    progress.revisit_completed_cleanup(now, interval);
    progress.revisit_completed_cleanup(now + interval - Duration::from_secs(1), interval);
    assert!(progress.pass_done[2]);
    progress.revisit_completed_cleanup(now + interval, interval);
    assert!(!progress.pass_done[2]);
    progress.phase = 2;
    assert!(progress.quantum(&store).await.unwrap());
    assert!(!progress.expiry.as_ref().unwrap().roots_checked);
    for _ in 0..64 {
        if progress.expiry.is_none() {
            break;
        }
        progress
            .expiry_quantum(&store.with_maintenance_access())
            .await
            .unwrap();
    }
    assert!(progress.expiry.is_none());
    assert_eq!(progress.accounting.expired, 1);
    assert_eq!(progress.accounting.held, 1);
    assert_eq!(
        history::Entity::find_by_id("origin")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap()
            .expired,
        1
    );
}

#[tokio::test]
async fn preflight_db_error_preserves_visit_and_retries_before_preparation() {
    let (_directory, store, routes, descriptor) = observed_fixture().await;
    let mut progress = FrozenStorageLifetimeProgress::default();
    progress.scope.done = true;
    assert!(progress.expiry_quantum(&store).await.unwrap());
    store
        .connection
        .execute_unprepared("ALTER TABLE compaction_task_output RENAME TO unavailable_output")
        .await
        .unwrap();
    routes.clear();
    progress.phase = 2;
    let error = progress.quantum(&store).await.unwrap_err();
    assert!(error.downcast_ref::<sea_orm::DbErr>().is_some());
    assert_eq!(progress.expiry_after, "origin");
    assert!(!progress.pass_done[2]);
    let retry = progress.expiry.as_ref().unwrap();
    assert_eq!(retry.header.id, "origin");
    assert!(!retry.roots_checked);
    assert_unprepared(retry);
    assert_eq!(
        progress.accounting.held + progress.accounting.expired + progress.accounting.refused,
        0
    );
    routes.assert_single_read(SqliteReadOutcome::Error);
    store
        .connection
        .execute_unprepared("ALTER TABLE unavailable_output RENAME TO compaction_task_output")
        .await
        .unwrap();
    insert_root(&store, &descriptor, 4).await;
    routes.clear();
    progress.phase = 2;
    assert!(progress.quantum(&store).await.unwrap());
    assert!(progress.expiry.is_none() && !progress.pass_done[2]);
    assert_eq!(progress.accounting.held, 1);
    assert_eq!(progress.expiry_after, "origin");
    routes.assert_single_read(SqliteReadOutcome::Ok);
    assert!(store.frozen_readers.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancelled_preflight_wait_preserves_attempt_and_restart_releases_capacity() {
    let (_directory, store, routes, _descriptor) = observed_fixture().await;
    let mut progress = FrozenStorageLifetimeProgress::default();
    progress.scope.done = true;
    assert!(progress.expiry_quantum(&store).await.unwrap());
    let maintenance = store.with_maintenance_access();
    let reader = maintenance.connection.begin_read().await.unwrap();
    routes.clear();
    progress.phase = 2;
    let mut pending = Box::pin(progress.quantum(&store));
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            result = &mut pending => panic!("preflight should wait: {result:?}"),
            _ = routes.blocked.notified() => {}
        }
    })
    .await
    .unwrap();
    drop(pending);
    assert_eq!(progress.expiry_after, "origin");
    assert!(!progress.pass_done[2]);
    let attempt = progress.expiry.as_ref().unwrap();
    assert!(!attempt.roots_checked);
    assert_unprepared(attempt);
    assert_eq!(
        progress.accounting.held + progress.accounting.expired + progress.accounting.refused,
        0
    );
    assert!(routes.writes.lock().unwrap().is_empty());
    assert!(routes.reads.lock().unwrap().iter().any(|event| matches!(
        event,
        SqliteReadEvent::AdmissionCancelled {
            active: 1,
            queue_depth: 0,
            ..
        }
    )));
    assert!(routes.reads.lock().unwrap().iter().any(|event| matches!(
        event,
        SqliteReadEvent::OperationFinished {
            class: SqliteReadClass::Maintenance,
            outcome: SqliteReadOutcome::Cancelled,
            ..
        }
    )));
    reader.commit().await.unwrap();
    assert!(routes.reads.lock().unwrap().iter().any(|event| matches!(
        event,
        SqliteReadEvent::AdmissionReleased {
            active: 0,
            queue_depth: 0,
            ..
        }
    )));
    drop(progress);
    let mut restart = FrozenStorageLifetimeProgress::default();
    assert!(restart.expiry_quantum(&maintenance).await.unwrap());
    for _ in 0..64 {
        if restart.expiry.is_none() {
            break;
        }
        restart.expiry_quantum(&maintenance).await.unwrap();
    }
    assert!(restart.expiry.is_none());
    assert_eq!(restart.accounting.expired, 1);
    assert!(!restart.expiry_quantum(&maintenance).await.unwrap());
    assert!(restart.expiry_after.is_empty());
    assert!(store.frozen_readers.lock().unwrap().is_empty());
}

#[tokio::test]
async fn negative_preflight_keeps_unknown_legacy_and_concurrent_expiry_availability_guards() {
    let (store, descriptor) = legacy_locator_fixture().await;
    let header = history::Entity::find_by_id(&descriptor.manifest_id)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    let mut preparation = ExpiryPreparation::new(header);
    let mut accounting = SweepAccounting::default();
    assert!(!preparation.step(&store, &mut accounting).await.unwrap());
    assert!(preparation.roots_checked);
    for _ in 0..64 {
        if preparation.step(&store, &mut accounting).await.unwrap() {
            break;
        }
    }
    assert_eq!(accounting.held, 1);
    assert_eq!(accounting.expired, 0);
    assert_eq!(
        history::Entity::find_by_id(&descriptor.manifest_id)
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap()
            .expired,
        0
    );

    let (_directory, store, _routes, _descriptor) = observed_fixture().await;
    let store = store.with_maintenance_access();
    let mut preparation = prepared_expiry(&store).await;
    // A legal concurrent expiry fails availability revalidation in logical_bounds,
    // before the final writer can compare the current and captured headers.
    store
        .connection
        .execute_unprepared("UPDATE compaction_frozen_history SET expired=1 WHERE id='origin'")
        .await
        .unwrap();
    let mut accounting = SweepAccounting::default();
    let error = preparation.step(&store, &mut accounting).await.unwrap_err();
    assert!(error.to_string().starts_with("frozen_input_expired:"));
    assert_eq!(
        (accounting.expired, accounting.held, accounting.refused),
        (0, 0, 0)
    );
    store
        .connection
        .begin()
        .await
        .unwrap()
        .rollback()
        .await
        .unwrap();
    assert!(store.frozen_readers.lock().unwrap().is_empty());
}
