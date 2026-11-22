use super::*;
use pioneer_postgres_pools::{
    PostgresDatabase, PostgresReadClass, PostgresReadEvent, PostgresReadObserver, PostgresReadOutcome,
    PostgresWriteClass, PostgresWriteEvent, PostgresWriteExecutor, PostgresWriteObserver,
};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Routes {
    reads: Mutex<Vec<PostgresReadEvent>>,
    writes: Mutex<Vec<PostgresWriteEvent>>,
    blocked: tokio::sync::Notify,
}
impl PostgresReadObserver for Routes {
    fn observe(&self, event: PostgresReadEvent) {
        if matches!(event, PostgresReadEvent::AdmissionEnqueued { active: 1, .. }) {
            self.blocked.notify_one();
        }
        self.reads.lock().unwrap().push(event);
    }
}
impl PostgresWriteObserver for Routes {
    fn observe(&self, event: PostgresWriteEvent) {
        self.writes.lock().unwrap().push(event);
    }
}
impl Routes {
    fn clear(&self) {
        self.reads.lock().unwrap().clear();
        self.writes.lock().unwrap().clear();
    }
    fn assert_reads(&self, outcome: PostgresReadOutcome, count: usize) {
        self.assert_read_outcomes(&vec![outcome; count]);
    }
    fn assert_read_outcomes(&self, outcomes: &[PostgresReadOutcome]) {
        let reads = self.reads.lock().unwrap();
        let operations: Vec<_> = reads
            .iter()
            .filter_map(|event| match event {
                PostgresReadEvent::OperationFinished { class, outcome, .. } => {
                    Some((*class, *outcome))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            operations,
            outcomes
                .iter()
                .map(|outcome| (PostgresReadClass::Maintenance, *outcome))
                .collect::<Vec<_>>()
        );
        assert!(reads.iter().any(|event| matches!(
            event,
            PostgresReadEvent::AdmissionReleased {
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

// A read-only PostgreSQL pool and a writer constrained to one connection
// expose the admission and release boundaries. The history has no checkpoint.
async fn observed_fixture() -> (CompactionStore, Arc<Routes>, FrozenHistoryRef) {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture
        .connect_with_options(|options| {
            options
                .max_connections(1)
                .map_sqlx_postgres_opts(|options| {
                    options.options([("default_transaction_read_only", "on")])
                });
        })
        .await
        .unwrap();
    let routes = Arc::new(Routes::default());
    let store = CompactionStore::for_test(PostgresDatabase::from_executor_with_read_observer(
        reader,
        PostgresWriteExecutor::with_observer(writer, routes.clone()),
        routes.clone(),
    ));
    assert!(store.connection.reader_is_read_only().await.unwrap());
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',TRUE,FALSE)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('turn','thread','failed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
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
    (store, routes, descriptor)
}

async fn insert_root(store: &CompactionStore, descriptor: &FrozenHistoryRef, kind: usize) {
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
    if let Some(prepared) = &prepared {
        assert_eq!(prepared.locator(), Some(descriptor.manifest_id.clone()));
    }
    if kind == 3 {
        let now = chrono::Utc::now().fixed_offset();
        store
            .upsert_cli_runtime_turn_binding(
                pioneer_crud_execution::cli_runtime_binding::NewCliRuntimeTurnBinding {
                    turn_id: "turn".into(),
                    thread_id: "thread".into(),
                    continuation_thread_id: "thread".into(),
                    workspace_id: "ws".into(),
                    runtime_id: "runtime".into(),
                    runtime_kind: "claude".into(),
                    native_thread_id: "native".into(),
                    native_turn_id: None,
                    request_id: None,
                    status: "active".into(),
                    model: None,
                    cwd: None,
                    sandbox_json: None,
                    approval_policy: None,
                    input_mapping_json: json,
                    created_at: now,
                    updated_at: now,
                },
            )
            .await
            .unwrap();
        return;
    }
    let sql = match kind {
        0 => {
            "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,frozen_manifest_id) VALUES('run','task','ws','thread',$1,$2)"
        }
        1 => {
            "INSERT INTO turn_runtime_snapshot(turn_id,thread_id,workspace_id,mode,model,provider_name,hook_runtime_context_json,workspace_skill_policies_json,input_json,capabilities_json,resolved_artifacts_json,runtime_environment_json,history_json,frozen_manifest_id) VALUES('turn','thread','ws','agent','m','p','{}','{}','[]','{}','[]','{}',$1::json,$2)"
        }
        2 => {
            "INSERT INTO thread_cli_runtime_binding(thread_id,workspace_id,runtime_id,runtime_kind,native_thread_id,resume_cursor_json,status,frozen_manifest_id) VALUES('thread','ws','runtime','claude','native',$1,'active',$2)"
        }
        4 => {
            "INSERT INTO compaction_task_output(task_run_turn_id,task_id,run_id,workspace_id,source_thread,source_turn,manifest_id) SELECT 'occurrence','task','run','ws','thread','turn',$2 WHERE $1::text IS NOT NULL"
        }
        _ => unreachable!(),
    };
    let transaction = store.connection.begin().await.unwrap();
    if let Some(prepared) = prepared {
        prepared.verify(&transaction, "ws").await.unwrap();
    }
    transaction
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            [json.into(), descriptor.manifest_id.clone().into()],
        ))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
}

fn assert_unprepared(preparation: &ExpiryPreparation) {
    assert!(preparation.is_unprepared());
}

#[pioneer_postgres_test::postgres_test]
async fn direct_roots_and_task_release_set_match_final_guard_without_preparation_or_writer() {
    for (kind, status, held) in [
        (0, "blocked", true),
        (0, "running", true),
        (0, "interrupted", true),
        (0, "completed", false),
        (0, "failed", false),
        (0, "cancelled", false),
        (1, "failed", true),
        (2, "failed", true),
        (3, "failed", true),
        (4, "failed", true),
    ] {
        let (store, routes, descriptor) = observed_fixture().await;
        insert_root(&store, &descriptor, kind).await;
        if kind == 4 {
            assert!(store.connection.execute_unprepared(
                "UPDATE compaction_task_output SET workspace_id='missing',source_thread='missing' WHERE task_run_turn_id='occurrence'"
            ).await.is_err());
        }
        store
            .connection
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE task SET status=$1 WHERE id='task'",
                [status.into()],
            ))
            .await
            .unwrap();
        let header = history::Entity::find_by_id("origin")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        let mut preparation = ExpiryPreparation::new(header.clone());
        routes.clear();
        let mut accounting = SweepAccounting::default();
        assert_eq!(
            expiry_step(
                &mut preparation,
                &store.with_maintenance_access(),
                &mut accounting
            )
            .await
            .unwrap(),
            held,
            "kind={kind}, status={status}"
        );
        assert!(preparation.roots_checked());
        assert_unprepared(&preparation);
        assert_eq!(accounting.held, u64::from(held));
        assert_eq!(accounting.expired + accounting.refused, 0);
        routes.assert_reads(PostgresReadOutcome::Ok, 1);
        let page =
            read_queries::expiry_page(&store.with_maintenance_access().connection, "", "origin")
                .await
                .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(
            page[0].held, held,
            "paged admission: kind={kind}, status={status}"
        );
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

#[pioneer_postgres_test::postgres_test]
async fn negative_page_admission_keeps_final_writer_check_for_late_roots() {
    for late_root in [false, true] {
        let (store, routes, descriptor) = observed_fixture().await;
        let store = store.with_maintenance_access();
        let mut progress = FrozenStorageLifetimeProgress::default();
        assert!(progress.expiry_quantum(&store).await.unwrap());
        for _ in 0..64 {
            let preparation = progress.expiry.as_ref().unwrap();
            if preparation.completed_layouts() == 2
                && preparation.operations_checked()
                && preparation.checkpoints_checked()
            {
                break;
            }
            progress.expiry_quantum(&store).await.unwrap();
        }
        let mut preparation = progress.expiry.take().unwrap();
        assert!(preparation.roots_checked());
        assert_eq!(preparation.completed_layouts(), 2);
        assert!(preparation.operations_checked() && preparation.checkpoints_checked());
        if late_root {
            insert_root(&store, &descriptor, 1).await;
        }
        // Returning an early hold here would incorrectly bypass this writer.
        routes.clear();
        let mut accounting = SweepAccounting::default();
        assert!(
            expiry_step(&mut preparation, &store, &mut accounting)
                .await
                .unwrap()
        );
        assert_eq!(accounting.expired, u64::from(!late_root));
        assert_eq!(accounting.held, u64::from(late_root));
        assert!(routes.reads.lock().unwrap().is_empty());
        assert!(routes.writes.lock().unwrap().iter().any(|event| matches!(
            event,
            PostgresWriteEvent::Acquired {
                class: PostgresWriteClass::Maintenance,
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

#[pioneer_postgres_test::postgres_test]
async fn negative_preflight_does_not_bypass_active_reader_hold() {
    use futures_util::FutureExt;
    let (store, routes, descriptor) = observed_fixture().await;
    let store = store.with_maintenance_access();
    let mut preparation = prepared_expiry(&store).await;
    let hold = store
        .compaction_acquire_frozen_history("ws", &descriptor)
        .await
        .unwrap();
    let (_, internal_hold) = store
        .acquire_frozen_header("ws", &descriptor)
        .await
        .unwrap();
    let database = store.database_connection();
    let mut released = database
        .work_signal("maintenance.frozen.release")
        .subscribe();
    let mut html = database.work_signal("maintenance.html.release").subscribe();
    routes.clear();
    let mut accounting = SweepAccounting::default();
    assert!(
        expiry_step(&mut preparation, &store, &mut accounting)
            .await
            .unwrap()
    );
    assert_eq!(accounting.held, 1);
    assert_eq!(accounting.expired, 0);
    assert!(routes.writes.lock().unwrap().iter().any(|event| matches!(
        event,
        PostgresWriteEvent::Acquired {
            class: PostgresWriteClass::Maintenance,
            ..
        }
    )));
    drop(hold);
    assert!(released.changed().now_or_never().is_none());
    drop(internal_hold);
    assert!(released.changed().now_or_never().is_some());
    assert!(
        html.changed().now_or_never().is_none(),
        "HTML did not wait on this hold"
    );
    let mut preparation = prepared_expiry(&store).await;
    assert!(
        expiry_step(&mut preparation, &store, &mut accounting)
            .await
            .unwrap()
    );
    assert_eq!(accounting.expired, 1);
    assert!(store.frozen_readers.lock().unwrap().is_empty());
}

#[pioneer_postgres_test::postgres_test]
async fn positive_visit_keeps_cursor_accounting_and_periodic_revisit_after_root_release() {
    let (store, routes, descriptor) = observed_fixture().await;
    insert_root(&store, &descriptor, 1).await;
    let mut progress = FrozenStorageLifetimeProgress::default();

    progress.pass_done = [false, false, true, true];
    progress.phase = 1;
    routes.clear();
    assert!(progress.quantum(&store).await.unwrap());
    // Discovery and root admission share a page. A hold needs no individual
    // preparation or a later root-query quantum.
    routes.assert_reads(PostgresReadOutcome::Ok, 2);
    assert!(progress.expiry.is_none());
    assert_eq!(progress.expiry_after, "origin");
    assert_eq!(progress.accounting.held, 1);
    assert_eq!(progress.accounting.expired, 0);
    progress.phase = 1;
    assert!(progress.quantum(&store).await.unwrap());
    assert!(progress.pass_done[1] && progress.expiry_after.is_empty());
    store
        .connection
        .execute_unprepared("DELETE FROM turn_runtime_snapshot WHERE turn_id='turn'")
        .await
        .unwrap();
    let now = Instant::now();
    let interval = Duration::from_secs(60);
    progress.revisit_completed_cleanup(now, interval);
    progress.revisit_completed_cleanup(now + interval - Duration::from_secs(1), interval);
    assert!(progress.pass_done[1]);
    progress.revisit_completed_cleanup(now + interval, interval);
    assert!(!progress.pass_done[1]);
    progress.phase = 1;
    assert!(progress.quantum(&store).await.unwrap());
    assert!(progress.expiry.as_ref().unwrap().roots_checked());
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

#[pioneer_postgres_test::postgres_test]
async fn page_root_error_preserves_cursor_and_retries_before_preparation() {
    let (store, routes, descriptor) = observed_fixture().await;
    let mut progress = FrozenStorageLifetimeProgress::default();

    store
        .connection
        .execute_unprepared("ALTER TABLE compaction_task_output RENAME TO unavailable_output")
        .await
        .unwrap();
    routes.clear();
    progress.phase = 1;
    let error = progress.quantum(&store).await.unwrap_err();
    assert!(error.downcast_ref::<sea_orm::DbErr>().is_some());
    assert!(progress.expiry_after.is_empty());
    assert!(!progress.pass_done[1]);
    assert!(progress.expiry.is_none() && progress.expiry_pending.is_empty());
    assert_eq!(
        progress.accounting.held + progress.accounting.expired + progress.accounting.refused,
        0
    );
    routes.assert_read_outcomes(&[PostgresReadOutcome::Ok, PostgresReadOutcome::Error]);
    store
        .connection
        .execute_unprepared("ALTER TABLE unavailable_output RENAME TO compaction_task_output")
        .await
        .unwrap();
    insert_root(&store, &descriptor, 4).await;
    routes.clear();
    progress.phase = 1;
    assert!(progress.quantum(&store).await.unwrap());
    assert!(progress.expiry.is_none() && !progress.pass_done[1]);
    assert_eq!(progress.accounting.held, 1);
    assert_eq!(progress.expiry_after, "origin");
    routes.assert_reads(PostgresReadOutcome::Ok, 1);
    assert!(store.frozen_readers.lock().unwrap().is_empty());
}

#[pioneer_postgres_test::postgres_test]
async fn cancelled_paged_preparation_preserves_attempt_and_restart_releases_capacity() {
    let (store, routes, _descriptor) = observed_fixture().await;
    let mut progress = FrozenStorageLifetimeProgress::default();

    assert!(progress.expiry_quantum(&store).await.unwrap());
    let maintenance = store.with_maintenance_access();
    let reader = maintenance.connection.begin_read().await.unwrap();
    routes.clear();
    progress.phase = 1;
    let mut pending = Box::pin(progress.quantum(&store));
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            result = &mut pending => panic!("preparation should wait: {result:?}"),
            _ = routes.blocked.notified() => {}
        }
    })
    .await
    .unwrap();
    drop(pending);
    assert_eq!(progress.expiry_after, "origin");
    assert!(!progress.pass_done[1]);
    let attempt = progress.expiry.as_ref().unwrap();
    assert!(attempt.roots_checked());
    assert_unprepared(attempt);
    assert_eq!(
        progress.accounting.held + progress.accounting.expired + progress.accounting.refused,
        0
    );
    assert!(routes.writes.lock().unwrap().is_empty());
    assert!(routes.reads.lock().unwrap().iter().any(|event| matches!(
        event,
        PostgresReadEvent::AdmissionCancelled {
            active: 1,
            queue_depth: 0,
            ..
        }
    )));
    assert!(routes.reads.lock().unwrap().iter().any(|event| matches!(
        event,
        PostgresReadEvent::OperationFinished {
            class: PostgresReadClass::Maintenance,
            outcome: PostgresReadOutcome::Cancelled,
            ..
        }
    )));
    reader.commit().await.unwrap();
    assert!(routes.reads.lock().unwrap().iter().any(|event| matches!(
        event,
        PostgresReadEvent::AdmissionReleased {
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

#[pioneer_postgres_test::postgres_test]
async fn negative_preflight_keeps_concurrent_expiry_availability_guard() {
    let (store, _routes, _descriptor) = observed_fixture().await;
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
    let error = expiry_step(&mut preparation, &store, &mut accounting)
        .await
        .unwrap_err();
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

#[pioneer_postgres_test::postgres_test]
async fn expiry_prefetched_stale_header_cannot_authorize_expiration() {
    use sea_orm::{IntoActiveModel, Set};

    let (store, _routes, _descriptor) = observed_fixture().await;
    let store = store.with_maintenance_access();
    let mut progress = FrozenStorageLifetimeProgress::default();
    assert!(progress.expiry_quantum(&store).await.unwrap());
    // Completed identities cannot be updated. Recreate this unreferenced empty
    // history atomically after discovery; the captured header must not authorize
    // expiration of the replacement owner under the same locator.
    let tx = store.connection.begin().await.unwrap();
    let mut replacement = history::Entity::find_by_id("origin")
        .one(&tx)
        .await
        .unwrap()
        .unwrap()
        .into_active_model();
    history::Entity::delete_by_id("origin")
        .exec(&tx)
        .await
        .unwrap();
    tx.execute_unprepared(
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,created_at,updated_at)
         VALUES('replacement-owner','ws','','agent','m','p','active',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"
    ).await.unwrap();
    replacement.owner_thread = Set("replacement-owner".into());
    history::Entity::insert(replacement)
        .exec(&tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut refused = false;
    for _ in 0..100 {
        match progress.expiry_quantum(&store).await {
            Err(error) => {
                assert_eq!(error.to_string(), "expiry header changed");
                refused = true;
                break;
            }
            Ok(_) if progress.expiry.is_none() => break,
            Ok(_) => {}
        }
    }
    assert!(refused, "stale header must refuse before deletion");
    let current = history::Entity::find_by_id("origin")
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.expired, 0);
    assert_eq!(current.owner_thread, "replacement-owner");
}

#[pioneer_postgres_test::postgres_test]
async fn expiry_pages_skip_holds_without_per_header_work_or_writer() {
    let (store, routes, _descriptor) = observed_fixture().await;
    let store = store.with_maintenance_access();
    for index in 0..17 {
        let id = format!("page-{index:02}");
        store.connection.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,next_import,imports_sha256,ready) SELECT $1,workspace_id,owner_thread,identity_sha256,0,0,0,0,imports_sha256,1 FROM compaction_frozen_history WHERE id='origin'",
            [id.clone().into()],
        )).await.unwrap();
        store
            .connection
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at)
             VALUES($1,'thread','failed','task_run','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
                [id.clone().into()],
            ))
            .await
            .unwrap();
        let sequence = i64::from(index + 1);
        store.connection.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status)
             VALUES($1,'task','run','thread',$1,'revision',$2,$2,'failed')",
            [id.clone().into(), sequence.into()],
        )).await.unwrap();
        store.connection.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO compaction_task_output(task_run_turn_id,task_id,run_id,workspace_id,source_thread,source_turn,manifest_id) VALUES($1,'task','run','ws','thread',$1,$1)",
            [id.into()],
        )).await.unwrap();
    }
    // Leave only the seventeen rooted headers in this expiry discovery pass.
    store
        .connection
        .execute_unprepared("UPDATE compaction_frozen_history SET expired=1 WHERE id='origin'")
        .await
        .unwrap();
    routes.clear();
    let mut progress = FrozenStorageLifetimeProgress::default();
    for _ in 0..2 {
        assert!(progress.expiry_quantum(&store).await.unwrap());
        assert!(
            progress.expiry.is_none(),
            "held pages must not allocate preparations"
        );
    }
    assert_eq!(progress.accounting.held, 17);
    assert!(routes.writes.lock().unwrap().is_empty());
    let reads = routes
        .reads
        .lock()
        .unwrap()
        .iter()
        .filter(|event| matches!(event, PostgresReadEvent::OperationFinished { .. }))
        .count();
    // One frontier and two pages, including root checks. No per-header query.
    assert_eq!(reads, 3);
    assert!(!progress.expiry_quantum(&store).await.unwrap());
    assert_eq!(
        history::Entity::find()
            .filter(history::Column::Expired.eq(0_i64))
            .all(&store.connection)
            .await
            .unwrap()
            .len(),
        17
    );
}
