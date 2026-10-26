// Focused static-stage regressions; intentionally not executed before review.
use super::*;
use pioneer_sqlite::{
    SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteReadOutcome,
    SqliteWriteClass, SqliteWriteEvent, SqliteWriteExecutor, SqliteWriteObserver,
};
use std::sync::{Arc, Mutex};

async fn empty_history(store: &CrudStore, id: &str) {
    let descriptor = FrozenHistoryRef {
        format: 1,
        manifest_id: id.into(),
        messages: 0,
        identity_sha256: hex::encode(Sha256::digest([])),
    };
    store
        .compaction_begin_frozen_history("ws", "thread", &descriptor)
        .await
        .unwrap();
    drop(
        store
            .compaction_finish_frozen_history_held("ws", "thread", &descriptor)
            .await
            .unwrap(),
    );
}

async fn ready(store: &CrudStore) {
    checkpoint_proofs::prepare_graph(store, "cp-1")
        .await
        .unwrap();
    assert!(
        prepared_expiry(store)
            .await
            .step(store, &mut SweepAccounting::default())
            .await
            .unwrap()
    );
}

// Reuse the original terminal runner and staged input, with an independent,
// unsealed operation/binding so ordinary schema guards allow rebinding it.
async fn later_candidate(store: &CrudStore) {
    empty_history(store, "late-origin").await;
    for sql in [
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) SELECT 'later-op',owner,'later-op',status,snapshot,deadline_ms FROM compaction_operation WHERE id='op'",
        "INSERT INTO compaction_runner_state(operation_id,generation,state) SELECT 'later-op',generation,state FROM compaction_runner_state WHERE operation_id='op'",
        "INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) SELECT 'later-op',id,identity_sha256,imports_sha256,import_count FROM compaction_frozen_history WHERE id='late-origin'",
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,previous,portion,summary,identity_sha256,selection,projection_version,format_version,status) SELECT 'cp-2','later-op',owner,NULL,2,summary,identity_sha256,selection,projection_version,format_version,'candidate' FROM compaction_checkpoint WHERE id='cp-1'",
        "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) SELECT 'cp-2',source_scope,source_id,source_version FROM compaction_coverage WHERE checkpoint_id='cp-1'",
        "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) SELECT 'later-op',ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version FROM compaction_manifest WHERE operation_id='op'",
        "INSERT INTO compaction_checkpoint_event_input(checkpoint_id,source_thread,source_scope,source_id,source_version,role) SELECT 'cp-2',source_thread,source_scope,source_id,source_version,role FROM compaction_checkpoint_event_input WHERE checkpoint_id='cp-1'",
        "UPDATE compaction_frozen_history SET expired=1 WHERE id='late-origin'",
    ] {
        store.connection.execute_unprepared(sql).await.unwrap();
    }
    let row = checkpoint::Entity::find_by_id("cp-2")
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    let sources = coverage::Entity::find()
        .filter(coverage::Column::CheckpointId.eq("cp-2"))
        .all(&store.connection)
        .await
        .unwrap();
    let cp = Checkpoint {
        id: row.id,
        operation_id: row.operation_id,
        owner: row.owner,
        previous: row.previous,
        summary: row.summary,
        selection: serde_json::from_str(&row.selection).unwrap(),
        coverage: sources
            .into_iter()
            .map(|s| SourceRef {
                scope: s.source_scope,
                id: s.source_id,
                version: s.source_version,
            })
            .collect(),
        projection_version: row.projection_version.try_into().unwrap(),
        format_version: row.format_version.try_into().unwrap(),
    };
    store
        .connection
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE compaction_checkpoint SET identity_sha256=?1 WHERE id='cp-2'",
            [compaction::checkpoint_identity(&cp).unwrap().into()],
        ))
        .await
        .unwrap();
    assert!(discard_preflight(store, "cp-2").await.unwrap());
}

#[tokio::test]
async fn unusable_runner_refuses_one_visit_without_hiding_later_work() {
    for bad in ["missing", "oversize", "invalid", "invalid_utf8"] {
        let (store, _) = fixture(false).await;
        ready(&store).await;
        later_candidate(&store).await;
        let state = runner::Entity::find_by_id("op")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        match bad {
            "missing" => {
                store
                    .connection
                    .execute_unprepared(
                        "DELETE FROM compaction_runner_state WHERE operation_id='op'",
                    )
                    .await
                    .unwrap();
            }
            "invalid_utf8" => {
                store.connection.execute_unprepared("UPDATE compaction_runner_state SET state=CAST(X'80' AS TEXT) WHERE operation_id='op'").await.unwrap();
            }
            "oversize" | "invalid" => {
                // Character length is below 256 KiB; actual UTF-8 bytes exceed it.
                let json = if bad == "oversize" {
                    let mut terminal_state: RunnerState =
                        serde_json::from_str(&state.state).unwrap();
                    terminal_state.diagnostic =
                        Some(pioneer_compaction::runner::FailureDiagnostic::new(
                            "fixture",
                            "fixture",
                            &"雪".repeat(100_000),
                        ));
                    let json = serde_json::to_string(&terminal_state).unwrap();
                    assert!(json.chars().count() < 256 * 1024 && json.len() > 256 * 1024);
                    json
                } else {
                    "{".into()
                };
                store
                    .connection
                    .execute_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        "UPDATE compaction_runner_state SET state=?1 WHERE operation_id='op'",
                        [json.into()],
                    ))
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let mut progress = FrozenStorageLifetimeProgress::default();
        progress.discard_after = "cp-0".into();
        assert!(progress.discard_quantum(&store).await.unwrap(), "{bad}");
        assert_eq!(progress.discard_after, "cp-1", "refusal is not EOF: {bad}");
        assert!(progress.discard.is_none(), "{bad}");
        assert_eq!(
            value(&store, "compaction_checkpoint_event_input", "cp-1").await,
            1
        );
        assert_eq!(
            compaction::checkpoint_row(&store.connection, "cp-1")
                .await
                .unwrap()
                .unwrap()
                .proof_version,
            1
        );
        assert!(progress.discard_quantum(&store).await.unwrap());
        assert_eq!(progress.discard.as_ref().unwrap().id, "cp-2");
        for _ in 0..64 {
            if progress.discard.is_none() {
                break;
            }
            progress.discard_quantum(&store).await.unwrap();
        }
        assert!(progress.discard.is_none());
        assert_eq!(
            value(&store, "compaction_checkpoint_event_input", "cp-2").await,
            0
        );
        // A later pass can reconsider repaired data; the refusal isn't cached.
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_runner_state(operation_id,generation,state) VALUES('op',?1,?2) ON CONFLICT(operation_id) DO UPDATE SET generation=excluded.generation,state=excluded.state",
            [state.generation.into(),state.state.into()])).await.unwrap();
        let mut restart = FrozenStorageLifetimeProgress::default();
        restart.discard_after = "cp-0".into();
        assert!(restart.discard_quantum(&store).await.unwrap());
        assert_eq!(restart.discard.as_ref().unwrap().id, "cp-1");
    }
}

#[tokio::test]
async fn late_read_uses_current_status_generation_and_binding_and_writer_rechecks_binding() {
    // Existing writer race regression owns incoming/hold/status/generation.
    // Here only binding adds a new writer race; other cases target the new read.
    for change in [
        "running",
        "generation",
        "generation_mismatch",
        "binding",
        "writer_binding",
    ] {
        let (store, _) = fixture(false).await;
        ready(&store).await;
        later_candidate(&store).await;
        assert!(discard_preflight(&store, "cp-2").await.unwrap());
        let prepared = if change == "writer_binding" {
            Some(
                prepare_discard_candidate(&store, "cp-2")
                    .await
                    .unwrap()
                    .unwrap(),
            )
        } else {
            None
        };
        match change {
            "running" => {
                store
                    .connection
                    .execute_unprepared(
                        "UPDATE compaction_operation SET status='running' WHERE id='later-op'",
                    )
                    .await
                    .unwrap();
            }
            "generation" | "generation_mismatch" => {
                let row = runner::Entity::find_by_id("later-op")
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .unwrap();
                let mut state: RunnerState = serde_json::from_str(&row.state).unwrap();
                if change == "generation" {
                    state.generation += 1;
                }
                store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                    "UPDATE compaction_runner_state SET generation=generation+1,state=?1 WHERE operation_id='later-op'",
                    [serde_json::to_string(&state).unwrap().into()])).await.unwrap();
            }
            "binding" | "writer_binding" => {
                empty_history(&store, "replacement").await;
                store.connection.execute_unprepared("UPDATE compaction_operation_projection SET manifest_id='replacement' WHERE operation_id='later-op'").await.unwrap();
                if change == "writer_binding" {
                    // Rebind while available, then isolate the writer's manifest
                    // fence by making the current history eligible for cleanup.
                    store
                        .connection
                        .execute_unprepared(
                            "UPDATE compaction_frozen_history SET expired=1 WHERE id='replacement'",
                        )
                        .await
                        .unwrap();
                }
            }
            _ => unreachable!(),
        }
        if let Some(mut prepared) = prepared {
            assert_eq!(prepared.manifest, "late-origin");
            assert_eq!(prepared.generation, Some(0));
            let current = prepare_discard_candidate(&store, "cp-2")
                .await
                .unwrap()
                .expect("current expired replacement must admit a fresh preparation");
            assert_eq!(current.manifest, "replacement");
            assert_eq!(current.generation, Some(0));
            while !prepared.reverse.done() {
                prepared.reverse.step(&store).await.unwrap();
            }
            assert!(!prepared.reverse.reachable);
            prepared.cleanup(&store).await.unwrap();
        } else {
            let current = prepare_discard_candidate(&store, "cp-2").await.unwrap();
            if change == "generation" {
                // Preflight has no terminal authority. A coherent new terminal
                // observation is allowed and must carry the *current* generation.
                assert_eq!(current.unwrap().generation, Some(1));
            } else {
                assert!(current.is_none(), "{change}");
            }
        }
        assert_eq!(
            value(&store, "compaction_checkpoint_event_input", "cp-2").await,
            1,
            "{change}"
        );
    }
}

#[derive(Default)]
struct Routes {
    reads: Mutex<Vec<SqliteReadEvent>>,
    writes: Mutex<Vec<SqliteWriteEvent>>,
    queued: tokio::sync::Notify,
}
impl SqliteReadObserver for Routes {
    fn observe(&self, event: SqliteReadEvent) {
        if matches!(event, SqliteReadEvent::AdmissionEnqueued { active: 1, .. }) {
            self.queued.notify_one();
        }
        self.reads.lock().unwrap().push(event);
    }
}
impl SqliteWriteObserver for Routes {
    fn observe(&self, event: SqliteWriteEvent) {
        self.writes.lock().unwrap().push(event);
    }
}

#[tokio::test]
async fn cancelled_discovery_or_late_read_releases_maintenance_capacity_for_retry_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("discard.db");
    let mut options = sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
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
    let mut options =
        sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false)
        .map_sqlx_sqlite_opts(|o| o.pragma("query_only", "ON"));
    let reader = Database::connect(options).await.unwrap();
    let routes = Arc::new(Routes::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, routes.clone()),
        routes.clone(),
    );
    let (store, _) = fixture_in_store(
        CrudStore::new(database).with_maintenance_access(),
        false,
        ["cp-0", "cp-1"],
    )
    .await;
    assert!(store.connection.reader_query_only_enabled().await.unwrap());
    ready(&store).await;
    for late in [false, true] {
        let mut progress = FrozenStorageLifetimeProgress::default();
        progress.discard_after = "cp-0".into();
        assert!(discard_preflight(&store, "cp-1").await.unwrap());
        let reader = store.connection.begin_read().await.unwrap();
        routes.reads.lock().unwrap().clear();
        routes.writes.lock().unwrap().clear();
        let mut pending = Box::pin(async {
            if late {
                prepare_discard_candidate(&store, "cp-1").await.map(|_| ())
            } else {
                progress.discard_quantum(&store).await.map(|_| ())
            }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                result = &mut pending => panic!("reader must queue: {result:?}"),
                _ = routes.queued.notified() => {}
            }
        })
        .await
        .unwrap();
        drop(pending);
        assert_eq!(progress.discard_after, "cp-0");
        assert!(progress.discard.is_none());
        assert!(routes.writes.lock().unwrap().is_empty());
        assert!(routes.reads.lock().unwrap().iter().any(|e| matches!(
            e,
            SqliteReadEvent::AdmissionCancelled {
                class: SqliteReadClass::Maintenance,
                active: 1,
                queue_depth: 0,
                ..
            }
        )));
        reader.commit().await.unwrap();
        assert!(progress.discard_quantum(&store).await.unwrap());
        assert_eq!(progress.discard.as_ref().unwrap().id, "cp-1");
        // Drop an admitted preparation, then rediscover via the normal keyset.
        drop(progress);
        assert!(store.frozen_readers.lock().unwrap().is_empty());
        let mut restart = FrozenStorageLifetimeProgress::default();
        for _ in 0..2 {
            restart.discard_quantum(&store).await.unwrap();
        }
        assert_eq!(restart.discard.as_ref().unwrap().id, "cp-1");
        assert!(routes.reads.lock().unwrap().iter().all(|e| !matches!(
            e,
            SqliteReadEvent::OperationFinished {
                class: SqliteReadClass::Interactive,
                ..
            }
        )));
        assert!(routes.reads.lock().unwrap().iter().any(|e| matches!(
            e,
            SqliteReadEvent::OperationFinished {
                outcome: SqliteReadOutcome::Cancelled,
                ..
            }
        )));
        assert!(routes.reads.lock().unwrap().iter().any(|e| matches!(
            e,
            SqliteReadEvent::AdmissionReleased {
                active: 0,
                queue_depth: 0,
                ..
            }
        )));
    }
    // Positive cleanup still routes its one atomic write through Maintenance.
    let mut progress = FrozenStorageLifetimeProgress::default();
    progress.discard_after = "cp-0".into();
    for _ in 0..64 {
        progress.discard_quantum(&store).await.unwrap();
        if progress.discard.is_none() {
            break;
        }
    }
    assert_eq!(
        value(&store, "compaction_checkpoint_event_input", "cp-1").await,
        0
    );
    assert!(routes.writes.lock().unwrap().iter().any(|e| matches!(
        e,
        SqliteWriteEvent::Acquired {
            class: SqliteWriteClass::Maintenance,
            ..
        }
    )));
    assert!(routes.writes.lock().unwrap().iter().all(|e| !matches!(
        e,
        SqliteWriteEvent::Acquired {
            class: SqliteWriteClass::Interactive | SqliteWriteClass::Critical,
            ..
        }
    )));
}
