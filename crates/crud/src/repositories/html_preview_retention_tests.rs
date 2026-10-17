//! Isolated owner regressions. Written for the closed test gate; not executed.
use super::super::compaction::FrozenImportRecord;
use super::*;
use futures_util::FutureExt;
use migration::Migrator;
use pioneer_compaction::{SourceRef, frozen::FrozenMessageRef};
use pioneer_sqlite::{
    SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteClass,
    SqliteWriteEvent, SqliteWriteExecutor, SqliteWriteObserver,
};
use sea_orm::{ConnectOptions, Database, DatabaseConnection, DbBackend, Statement};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Routes {
    reads: Mutex<Vec<SqliteReadEvent>>,
    writes: Mutex<Vec<SqliteWriteEvent>>,
    queued: tokio::sync::Notify,
    writer_queued: tokio::sync::Notify,
}
impl SqliteReadObserver for Routes {
    fn observe(&self, event: SqliteReadEvent) {
        if matches!(
            event,
            SqliteReadEvent::AdmissionEnqueued {
                queue_depth: 1,
                active: 1,
                ..
            }
        ) {
            self.queued.notify_one();
        }
        self.reads.lock().unwrap().push(event);
    }
}
impl SqliteWriteObserver for Routes {
    fn observe(&self, event: SqliteWriteEvent) {
        if matches!(event, SqliteWriteEvent::Enqueued { queue, .. } if queue.maintenance == 1) {
            self.writer_queued.notify_one();
        }
        self.writes.lock().unwrap().push(event);
    }
}
impl Routes {
    fn reset(&self) {
        self.reads.lock().unwrap().clear();
        self.writes.lock().unwrap().clear();
    }
    fn counts(&self) -> (usize, usize) {
        let reads = self.reads.lock().unwrap();
        let writes = self.writes.lock().unwrap();
        assert!(reads.iter().all(|e| !matches!(
            e,
            SqliteReadEvent::OperationFinished {
                class: SqliteReadClass::Interactive,
                ..
            }
        )));
        assert!(writes.iter().all(|e| !matches!(
            e,
            SqliteWriteEvent::Acquired {
                class: SqliteWriteClass::Interactive | SqliteWriteClass::Critical,
                ..
            }
        )));
        (
            reads
                .iter()
                .filter(|e| matches!(e, SqliteReadEvent::OperationFinished { .. }))
                .count(),
            writes
                .iter()
                .filter(|e| matches!(e, SqliteWriteEvent::Acquired { .. }))
                .count(),
        )
    }
}
struct Fixture {
    store: CrudStore,
    reader: DatabaseConnection,
    writer: SqliteWriteExecutor,
    routes: Arc<Routes>,
    directory: tempfile::TempDir,
}
impl Fixture {
    async fn reader(directory: &std::path::Path) -> DatabaseConnection {
        let mut options = ConnectOptions::new(format!(
            "sqlite://{}?mode=ro",
            directory.join("index.sqlite").display()
        ));
        options
            .max_connections(2)
            .min_connections(1)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(|o| o.pragma("query_only", "ON"));
        Database::connect(options).await.unwrap()
    }
    async fn reopen_reader(&mut self) {
        self.reader = Self::reader(self.directory.path()).await;
        self.store = CrudStore::new(SqliteDatabase::from_executor_with_read_observer(
            self.reader.clone(),
            self.writer.clone(),
            self.routes.clone(),
        ))
        .with_maintenance_access();
    }
    async fn sql(&self, sql: &str) {
        self.store.connection.execute_unprepared(sql).await.unwrap();
    }
    async fn marker(&self, id: &str) -> frozen_index::Model {
        frozen_index::Entity::find_by_id(id)
            .one(&self.store.connection)
            .await
            .unwrap()
            .unwrap()
    }
    async fn step(&self, after: &mut String, progress: &mut Option<PreviewImportIndex>) {
        assert!(index_quantum(&self.store, after, progress).await.unwrap());
    }
}
async fn fixture() -> Fixture {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut options = ConnectOptions::new(format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("index.sqlite").display()
    ));
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false);
    let connection = Database::connect(options).await.unwrap();
    connection
        .execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    let routes = Arc::new(Routes::default());
    let writer = SqliteWriteExecutor::with_observer(connection, routes.clone());
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    let reader = Fixture::reader(directory.path()).await;
    let store = CrudStore::new(SqliteDatabase::from_executor_with_read_observer(
        reader.clone(),
        writer.clone(),
        routes.clone(),
    ))
    .with_maintenance_access();
    let f = Fixture {
        store,
        reader,
        writer,
        routes,
        directory,
    };
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('parent','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('child','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('parent-turn','parent','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('child-turn','child','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('ack-turn','parent','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES('task','ws','thread','parent','parent','parent-turn','agent','running','Task','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES('run','task','run',1,1,'succeeded','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES('rt','task','run','child','child-turn','initial',0,1,'completed',CURRENT_TIMESTAMP)",
        "INSERT INTO task_result_candidate(id,task_id,run_id,task_run_turn_id,thread_id,turn_id,round,status,created_at,updated_at) VALUES('candidate','task','run','rt','child','child-turn',0,'accepted',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,thread_target,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id) VALUES('delivery','ws','task','run','key','thread','origin_thread','parent','delivered',1,1,'ack-turn')",
    ] {
        f.sql(sql).await;
    }
    f.store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "parent".into(),
                turn_id: "ack-turn".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: pioneer_protocol::task_delivery_result_item_id("delivery"),
                    text: "delivered".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            1,
        )
        .await
        .unwrap();
    assert!(
        f.store
            .connection
            .reader_query_only_enabled()
            .await
            .unwrap()
    );
    f
}
fn reference(source: SourceRef, thread: &str, context: Option<&str>) -> FrozenMessageRef {
    serde_json::from_value(json!({
        "source_thread":thread,"context_thread":context,"unit_id":"fixture-unit",
        "sources":[source],"inherited":false,"complete":true,"protected_input":false,
        "wire_sha256":"a".repeat(64),"replay_source":null,"tool_call_id":null,"tool_name":null
    }))
    .unwrap()
}
fn import_digest(proofs: &[FrozenImportRecord]) -> Sha256 {
    let mut digest = Sha256::new();
    for proof in proofs {
        let bytes = serde_json::to_vec(proof).unwrap();
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(bytes);
    }
    digest
}
async fn manifest(
    f: &Fixture,
    id: &str,
    owner: &str,
    messages: &[FrozenMessageRef],
    proofs: &[FrozenImportRecord],
    declared: usize,
    expected: Option<&str>,
) {
    let mut digest = Sha256::new();
    for message in messages {
        let bytes = serde_json::to_vec(message).unwrap();
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    f.store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,next_import,imports_sha256,ready) VALUES(?1,'ws',?2,?3,?4,?4,?5,?5,?6,1)",
        [id.into(),owner.into(),hex::encode(digest.finalize()).into(),(messages.len() as i64).into(),(declared as i64).into(),expected.map(str::to_owned).unwrap_or_else(||hex::encode(import_digest(proofs).finalize())).into()]
    )).await.unwrap();
    for (ordinal, message) in messages.iter().enumerate() {
        let json = serde_json::to_string(message).unwrap();
        f.store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES(?1,?2,?3,?4)",
            [id.into(),(ordinal as i64).into(),json.clone().into(),(json.len() as i64).into()]
        )).await.unwrap();
    }
    for (ordinal, proof) in proofs.iter().enumerate() {
        let json = serde_json::to_string(proof).unwrap();
        f.store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_thread,source_scope,source_id,source_version,proof_json,bytes) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            [id.into(),(ordinal as i64).into(),(proof.message_ordinal as i64).into(),proof.source_thread.clone().into(),proof.source.scope.clone().into(),proof.source.id.clone().into(),proof.source.version.clone().into(),json.clone().into(),(json.len() as i64).into()]
        )).await.unwrap();
    }
}
async fn seed(
    f: &Fixture,
    n: usize,
    padding: usize,
    checkpoint: bool,
) -> (Vec<FrozenMessageRef>, Vec<FrozenImportRecord>) {
    let ack = f.store.connection.query_one_raw(Statement::from_string(DbBackend::Sqlite,
        "SELECT source_id,revision FROM compaction_event_revision WHERE turn_id='ack-turn' AND item_id IS NOT NULL"
    )).await.unwrap().unwrap();
    let acknowledgement = SourceRef {
        scope: "event:ack-turn".into(),
        id: ack.try_get("", "source_id").unwrap(),
        version: format!(
            "event-revision:{}",
            ack.try_get::<i64>("", "revision").unwrap()
        ),
    };
    let mut originals = Vec::new();
    let mut targets = Vec::new();
    let mut proofs = Vec::new();
    if checkpoint {
        for sql in [
            "INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('cp-owner','ws','parent',1)",
            "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES('cp-op','cp-owner','cp','completed','{}',1)",
            "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES('cp','cp-op','cp-owner',0,'fixture','cp-v1','{}',0,1,'applied')",
        ] {
            f.sql(sql).await;
        }
    }
    for ordinal in 0..n {
        let source = SourceRef {
            scope: "event:child-turn".into(),
            id: format!("source-{ordinal}-{}", "x".repeat(padding)),
            version: "event-revision:1".into(),
        };
        originals.push(reference(source.clone(), "child", None));
        targets.push(if checkpoint {
            reference(
                SourceRef {
                    scope: "checkpoint:cp-owner".into(),
                    id: "cp".into(),
                    version: "cp-v1".into(),
                },
                "parent",
                None,
            )
        } else {
            reference(source.clone(), "child", Some("parent"))
        });
        if checkpoint {
            for sql in [
                "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES('cp',?1,?2,?3)",
                "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES('cp-op',?4,0,0,'child',?1,?2,?3)",
            ] {
                let values = if sql.contains("?4") {
                    vec![
                        source.scope.clone().into(),
                        source.id.clone().into(),
                        source.version.clone().into(),
                        (ordinal as i64).into(),
                    ]
                } else {
                    vec![
                        source.scope.clone().into(),
                        source.id.clone().into(),
                        source.version.clone().into(),
                    ]
                };
                f.store
                    .connection
                    .execute_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        sql,
                        values,
                    ))
                    .await
                    .unwrap();
            }
        }
        proofs.push(FrozenImportRecord {
            message_ordinal: ordinal as u64,
            source_thread: "child".into(),
            source,
            delivery_id: "delivery".into(),
            candidate_id: "candidate".into(),
            output_manifest: "zz-output".into(),
            output_ordinal: ordinal as u64,
            acknowledgement: acknowledgement.clone(),
            preview_archive: None,
        });
    }
    if checkpoint {
        f.sql("UPDATE compaction_operation SET next_portion=1 WHERE id='cp-op'")
            .await;
    }
    manifest(f, "zz-output", "child", &originals, &[], 0, None).await;
    f.sql("INSERT INTO compaction_task_output(task_run_turn_id,task_id,run_id,workspace_id,source_thread,source_turn,manifest_id) VALUES('rt','task','run','ws','child','child-turn','zz-output')").await;
    f.sql("INSERT INTO compaction_delivery_output(delivery_id,candidate_id,task_run_turn_id) VALUES('delivery','candidate','rt')").await;
    (targets, proofs)
}
async fn start_imports(
    f: &Fixture,
    targets: &[FrozenMessageRef],
    proofs: &[FrozenImportRecord],
    declared: usize,
    expected: Option<&str>,
) {
    manifest(f, "a", "parent", targets, proofs, declared, expected).await;
    f.sql(&format!(
        "UPDATE html_preview_frozen_index SET next_ordinal={} WHERE manifest_id='a'",
        targets.len()
    ))
    .await;
}
async fn no_intermediate_writes(f: &Fixture) {
    f.sql("CREATE TRIGGER reject_repeat_index_insert BEFORE INSERT ON html_preview_frozen_index BEGIN SELECT RAISE(ABORT,'unexpected marker insert'); END").await;
    f.sql("CREATE TRIGGER reject_intermediate_index_update BEFORE UPDATE ON html_preview_frozen_index WHEN NEW.complete=0 AND NEW.unknown=0 BEGIN SELECT RAISE(ABORT,'unexpected RAM-only marker update'); END").await;
}

#[tokio::test]
async fn imports_consume_each_row_and_byte_bounded_page_once_in_order() {
    for (n, padding, page_len) in [(130, 0, 128), (3, 140_000, 1)] {
        let f = fixture().await;
        let (targets, proofs) = seed(&f, n, padding, false).await;
        start_imports(&f, &targets, &proofs, n, None).await;
        no_intermediate_writes(&f).await;
        let mut after = String::new();
        let mut progress = None;
        for ordinal in 0..n {
            f.routes.reset();
            f.step(&mut after, &mut progress).await;
            // Three exact proof reads; four helper reads only at page EOF;
            // discovery once. Existing low-cardinality observer sees routes.
            let page_reads = if ordinal % page_len == 0 { 4 } else { 0 };
            assert_eq!(
                f.routes.counts(),
                (
                    3 + page_reads + usize::from(ordinal == 0),
                    usize::from(ordinal + 1 == n)
                )
            );
            if ordinal + 1 < n {
                let state = progress.as_ref().unwrap();
                assert_eq!(state.next, (ordinal + 1) as u64);
                assert_eq!(
                    hex::encode(state.digest.clone().finalize()),
                    hex::encode(import_digest(&proofs[..=ordinal]).finalize())
                );
                let end = ((ordinal / page_len + 1) * page_len).min(n);
                assert_eq!(state.page.len(), end - ordinal - 1);
                assert!(after.is_empty());
            }
        }
        assert!(progress.is_none());
        assert_eq!(after, "a");
        assert_eq!(f.marker("a").await.complete, 1);
    }
}

async fn finish_member(f: &Fixture, after: &mut String, progress: &mut Option<PreviewImportIndex>) {
    for _ in 0..64 {
        if progress
            .as_ref()
            .unwrap()
            .pending
            .as_ref()
            .unwrap()
            .1
            .done()
        {
            return;
        }
        f.step(after, progress).await;
    }
    panic!("fixture member did not finish its full traversal");
}
#[tokio::test]
async fn historical_member_keeps_the_current_proof_and_rest_of_page_without_writes() {
    let f = fixture().await;
    let (targets, proofs) = seed(&f, 2, 0, true).await;
    start_imports(&f, &targets, &proofs, 2, None).await;
    no_intermediate_writes(&f).await;
    let mut after = String::new();
    let mut progress = None;
    f.step(&mut after, &mut progress).await;
    assert_eq!(progress.as_ref().unwrap().page.len(), 1);
    // Enter root; coverage page/EOF; ownership page/EOF; two leaves; exit.
    // Exact observer counts allow the walker's SQL, while detecting an extra
    // discovery SELECT even on steps which legitimately access the database.
    for reads in [1, 3, 3, 3, 3, 0, 0, 0] {
        assert!(
            !progress
                .as_ref()
                .unwrap()
                .pending
                .as_ref()
                .unwrap()
                .1
                .done()
        );
        f.routes.reset();
        f.step(&mut after, &mut progress).await;
        assert_eq!(f.routes.counts(), (reads, 0));
        assert_eq!(progress.as_ref().unwrap().next, 0);
        assert_eq!(progress.as_ref().unwrap().page.len(), 1);
    }
    assert!(
        progress
            .as_ref()
            .unwrap()
            .pending
            .as_ref()
            .unwrap()
            .1
            .done()
    );
    assert_eq!(progress.as_ref().unwrap().next, 0);
    assert_eq!(
        progress.as_ref().unwrap().pending.as_ref().unwrap().0,
        proofs[0]
    );
    f.routes.reset();
    f.step(&mut after, &mut progress).await;
    assert_eq!(
        f.routes.counts(),
        (0, 0),
        "completed proof hashes in RAM without discovery"
    );
    assert_eq!(progress.as_ref().unwrap().next, 1);
    assert_eq!(progress.as_ref().unwrap().page.len(), 1);
    f.routes.reset();
    f.step(&mut after, &mut progress).await;
    assert_eq!(
        f.routes.counts(),
        (4, 0),
        "next proof uses buffered page, exact reads and new member"
    );
    finish_member(&f, &mut after, &mut progress).await;
    f.routes.reset();
    f.step(&mut after, &mut progress).await;
    assert_eq!(f.routes.counts(), (0, 1));
    assert_eq!(f.marker("a").await.complete, 1);
    assert!(progress.is_none());
}

#[tokio::test]
async fn invalid_count_digest_binding_and_target_remain_retaining_and_advance() {
    for fault in ["count", "digest", "binding", "target", "gap", "length"] {
        let f = fixture().await;
        let (mut targets, mut proofs) = seed(&f, 2, 0, false).await;
        if fault == "binding" {
            proofs[1].candidate_id = "wrong-candidate".into();
        }
        if fault == "target" {
            targets[1].sources[0].version = "event-revision:2".into();
        }
        start_imports(
            &f,
            &targets,
            &proofs,
            if fault == "count" { 3 } else { 2 },
            if fault == "digest" {
                Some("wrong-digest")
            } else {
                None
            },
        )
        .await;
        if fault == "gap" {
            f.sql("DELETE FROM compaction_frozen_import_data WHERE manifest_id='a' AND ordinal=0")
                .await;
        }
        if fault == "length" {
            // Legacy corruption only: restore normal constraints before owner reads.
            let tx = f.store.connection.begin().await.unwrap();
            tx.execute_unprepared("PRAGMA ignore_check_constraints=ON")
                .await
                .unwrap();
            tx.execute_unprepared("UPDATE compaction_frozen_import_data SET bytes=bytes+1 WHERE manifest_id='a' AND ordinal=0").await.unwrap();
            tx.execute_unprepared("PRAGMA ignore_check_constraints=OFF")
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
        manifest(&f, "b", "parent", &[], &[], 0, None).await;
        let mut after = String::new();
        let mut progress = None;
        for _ in 0..4 {
            f.step(&mut after, &mut progress).await;
            if after == "a" {
                break;
            }
        }
        assert_eq!(after, "a", "{fault}");
        let marker = f.marker("a").await;
        assert_eq!((marker.complete, marker.unknown), (0, 1), "{fault}");
        assert!(progress.is_none());
        f.step(&mut after, &mut progress).await;
        assert_eq!(after, "b");
    }
}

#[tokio::test]
async fn pending_terminal_write_revalidates_mutable_marker_and_header_and_advances() {
    for mutation in [
        "UPDATE compaction_frozen_history SET expired=1 WHERE id='a'",
        "DELETE FROM compaction_frozen_history WHERE id='a'",
        "UPDATE html_preview_frozen_index SET unknown=1 WHERE manifest_id='a'",
        "UPDATE html_preview_frozen_index SET identity_sha256='other-version' WHERE manifest_id='a'",
        "UPDATE html_preview_frozen_index SET workspace_id='other-workspace' WHERE manifest_id='a'",
        "UPDATE html_preview_frozen_index SET next_ordinal=0 WHERE manifest_id='a'",
        "UPDATE html_preview_frozen_index SET message_count=3 WHERE manifest_id='a'",
        "UPDATE html_preview_frozen_index SET ready=0 WHERE manifest_id='a'",
        "UPDATE html_preview_frozen_index SET expired=1 WHERE manifest_id='a'",
    ] {
        let f = fixture().await;
        let (targets, proofs) = seed(&f, 2, 0, false).await;
        start_imports(&f, &targets, &proofs, 2, None).await;
        manifest(&f, "b", "parent", &[], &[], 0, None).await;
        let mut after = String::new();
        let mut progress = None;
        f.step(&mut after, &mut progress).await;
        assert_eq!(progress.as_ref().unwrap().next, 1);
        // Between quanta models a concurrent serialized owner. Buffered proof
        // can complete in RAM, but the old snapshot cannot authorize commit.
        f.sql(mutation).await;
        f.step(&mut after, &mut progress).await;
        assert!(progress.is_none());
        assert_eq!(after, "a");
        assert_eq!(f.marker("a").await.complete, 0, "{mutation}");
        f.step(&mut after, &mut progress).await;
        assert_eq!(after, "b");
    }
}

#[tokio::test]
async fn writer_failure_and_restart_replay_the_entire_stream() {
    let f = fixture().await;
    let (targets, proofs) = seed(&f, 2, 0, false).await;
    start_imports(&f, &targets, &proofs, 2, None).await;
    let mut after = String::new();
    let mut progress = None;
    f.step(&mut after, &mut progress).await;
    f.sql("CREATE TRIGGER fail_index_commit BEFORE UPDATE ON html_preview_frozen_index BEGIN SELECT RAISE(ABORT,'fixture writer failure'); END").await;
    let error = index_quantum(&f.store, &mut after, &mut progress)
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<sea_orm::DbErr>().is_some());
    assert!(progress.is_none());
    assert!(after.is_empty());
    assert_eq!(f.marker("a").await.complete, 0);
    f.sql("DROP TRIGGER fail_index_commit").await;
    f.step(&mut after, &mut progress).await;
    assert_eq!(progress.as_ref().unwrap().next, 1);
    assert_eq!(
        hex::encode(progress.as_ref().unwrap().digest.clone().finalize()),
        hex::encode(import_digest(&proofs[..1]).finalize())
    );
    // Process restart loses all RAM progress; durable marker has no import offset.
    drop(progress.take());
    after.clear();
    f.step(&mut after, &mut progress).await;
    assert_eq!(progress.as_ref().unwrap().next, 1);
    f.step(&mut after, &mut progress).await;
    assert_eq!(f.marker("a").await.complete, 1);
}

#[tokio::test]
async fn walker_db_error_and_cancelled_read_discard_partial_ram_before_retry() {
    for cancel in [false, true] {
        let mut f = fixture().await;
        let (targets, proofs) = seed(&f, 2, 0, true).await;
        start_imports(&f, &targets, &proofs, 2, None).await;
        let mut after = String::new();
        let mut progress = None;
        f.step(&mut after, &mut progress).await; // Pending walker, buffered proof 1.
        if cancel {
            let held = f.store.connection.begin_read().await.unwrap();
            {
                let step = index_quantum(&f.store, &mut after, &mut progress);
                tokio::pin!(step);
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    tokio::select! {
                        result = &mut step => panic!("read was not queued: {result:?}"),
                        _ = f.routes.queued.notified() => {},
                    }
                })
                .await
                .expect("fixture read did not reach admission");
                // Drop future after HistoricalMember popped/activated its root.
            }
            drop(held);
            assert!(f.routes.reads.lock().unwrap().iter().any(|e| matches!(
                e,
                SqliteReadEvent::AdmissionCancelled { queue_depth: 0, .. }
            )));
        } else {
            // Close only this isolated physical reader to fail the graph read
            // after it has changed RAM. Reopen on the same temporary fixture.
            f.reader.clone().close().await.unwrap();
            let error = index_quantum(&f.store, &mut after, &mut progress)
                .await
                .unwrap_err();
            assert!(error.downcast_ref::<sea_orm::DbErr>().is_some());
            f.reopen_reader().await;
        }
        assert!(progress.is_none());
        assert!(after.is_empty());
        assert_eq!(f.marker("a").await.complete, 0);
        f.step(&mut after, &mut progress).await;
        assert_eq!(progress.as_ref().unwrap().next, 0);
        assert_eq!(progress.as_ref().unwrap().page.len(), 1);
        finish_member(&f, &mut after, &mut progress).await;
        f.step(&mut after, &mut progress).await;
        f.step(&mut after, &mut progress).await;
        finish_member(&f, &mut after, &mut progress).await;
        f.step(&mut after, &mut progress).await;
        assert_eq!(f.marker("a").await.complete, 1);
    }
}

#[tokio::test]
async fn missing_legacy_marker_messages_and_backfill_gate_remain_supported() {
    let f = fixture().await;
    // Model a header predating the HTML migration using its exact insert
    // trigger DDL; restore before the production owner runs.
    let trigger:String = f.store.connection.query_one_raw(Statement::from_string(DbBackend::Sqlite,
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='html_preview_header_insert'"
    )).await.unwrap().unwrap().try_get("","sql").unwrap();
    f.sql("DROP TRIGGER html_preview_header_insert").await;
    let message = reference(
        SourceRef {
            scope: "event:parent-turn".into(),
            id: "legacy-source".into(),
            version: "event-revision:1".into(),
        },
        "parent",
        None,
    );
    manifest(&f, "a", "parent", &[message], &[], 0, None).await;
    f.sql(&trigger).await;
    let mut after = String::new();
    let mut progress = None;
    f.routes.reset();
    f.step(&mut after, &mut progress).await;
    assert_eq!(
        f.routes.counts().1,
        2,
        "one legacy INSERT and one message transaction"
    );
    let marker = f.marker("a").await;
    assert_eq!(
        (marker.next_ordinal, marker.complete, marker.unknown),
        (1, 1, 0)
    );
    assert_eq!(
        backfill::Entity::find_by_id(1_i64)
            .one(&f.store.connection)
            .await
            .unwrap()
            .unwrap()
            .complete,
        0
    );
    assert!(
        !index_quantum(&f.store, &mut after, &mut progress)
            .await
            .unwrap()
    );
    assert!(after.is_empty());
    assert_eq!(
        backfill::Entity::find_by_id(1_i64)
            .one(&f.store.connection)
            .await
            .unwrap()
            .unwrap()
            .complete,
        1
    );
    no_intermediate_writes(&f).await;
    f.routes.reset();
    f.step(&mut after, &mut progress).await; // Existing complete marker: read only.
    assert_eq!(f.routes.counts(), (1, 0));
}

#[tokio::test]
async fn full_member_without_the_wanted_leaf_cannot_publish_safe() {
    let f = fixture().await;
    let (mut targets, proofs) = seed(&f, 2, 0, false).await;
    for sql in [
        "INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('empty-owner','ws','parent',1)",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms,next_portion) VALUES('empty-op','empty-owner','empty','completed','{}',1,0)",
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES('empty-cp','empty-op','empty-owner',0,'fixture','empty-v1','{}',0,1,'applied')",
        "UPDATE compaction_operation SET next_portion=1 WHERE id='empty-op'",
    ] {
        f.sql(sql).await;
    }
    targets[0] = reference(
        SourceRef {
            scope: "checkpoint:empty-owner".into(),
            id: "empty-cp".into(),
            version: "empty-v1".into(),
        },
        "parent",
        None,
    );
    start_imports(&f, &targets, &proofs, 2, None).await;
    let mut after = String::new();
    let mut progress = None;
    f.step(&mut after, &mut progress).await;
    finish_member(&f, &mut after, &mut progress).await;
    assert!(
        !progress
            .as_ref()
            .unwrap()
            .pending
            .as_ref()
            .unwrap()
            .1
            .found()
    );
    f.step(&mut after, &mut progress).await;
    let marker = f.marker("a").await;
    assert_eq!((marker.complete, marker.unknown), (0, 1));
    assert_eq!(after, "a");
    assert!(progress.is_none());
}

#[tokio::test]
async fn concurrent_progress_owners_do_not_mix_versions_or_publish_twice() {
    let f = fixture().await;
    let (targets, proofs) = seed(&f, 2, 0, false).await;
    start_imports(&f, &targets, &proofs, 2, None).await;
    let mut left_after = String::new();
    let mut right_after = String::new();
    let mut left = None;
    let mut right = None;
    // Interleave independent owners at the actual await/quantum boundary.
    f.step(&mut left_after, &mut left).await;
    f.step(&mut right_after, &mut right).await;
    for assignment in [
        "owner_thread='child'",
        "identity_sha256='different-version'",
        "import_count=3",
        "imports_sha256='different-stream'",
    ] {
        assert!(
            f.store
                .connection
                .execute_unprepared(&format!(
                    "UPDATE compaction_frozen_history SET {assignment} WHERE id='a'"
                ))
                .await
                .is_err()
        );
    }
    f.step(&mut left_after, &mut left).await;
    assert_eq!(f.marker("a").await.complete, 1);
    // Reject any actual second publication while allowing a zero-row CAS.
    f.sql("CREATE TRIGGER reject_second_publication BEFORE UPDATE ON html_preview_frozen_index WHEN OLD.complete=1 AND NEW.complete=1 BEGIN SELECT RAISE(ABORT,'duplicate publication'); END").await;
    f.step(&mut right_after, &mut right).await;
    assert!(left.is_none() && right.is_none());
    assert_eq!((left_after.as_str(), right_after.as_str()), ("a", "a"));
    assert_eq!(f.marker("a").await.complete, 1);
}

#[tokio::test]
async fn cancelled_terminal_writer_discards_digest_and_replays_without_publication() {
    let f = fixture().await;
    let (targets, proofs) = seed(&f, 2, 0, false).await;
    start_imports(&f, &targets, &proofs, 2, None).await;
    let mut after = String::new();
    let mut progress = None;
    f.step(&mut after, &mut progress).await;
    let held = f.store.connection.begin().await.unwrap();
    // Enqueued also observes immediately granted fixture writes. Consume the
    // stored notification before waiting for this blocked terminal statement.
    let _ = f.routes.writer_queued.notified().now_or_never();
    {
        let step = index_quantum(&f.store, &mut after, &mut progress);
        tokio::pin!(step);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::select! {
                result = &mut step => panic!("terminal writer was not queued: {result:?}"),
                _ = f.routes.writer_queued.notified() => {},
            }
        })
        .await
        .expect("fixture write did not reach admission");
    }
    held.rollback().await.unwrap();
    assert!(progress.is_none());
    assert!(after.is_empty());
    assert!(f.routes.writes.lock().unwrap().iter().any(|e| matches!(
        e,
        SqliteWriteEvent::Cancelled {
            class: SqliteWriteClass::Maintenance,
            ..
        }
    )));
    assert_eq!(f.marker("a").await.complete, 0);
    f.step(&mut after, &mut progress).await;
    assert_eq!(progress.as_ref().unwrap().next, 1);
    f.step(&mut after, &mut progress).await;
    assert_eq!(f.marker("a").await.complete, 1);
}
