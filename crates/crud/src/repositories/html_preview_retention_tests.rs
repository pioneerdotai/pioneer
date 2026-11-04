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
        assert!(
            index_quantum(&self.store, after, progress, "\u{10ffff}")
                .await
                .unwrap()
        );
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
    // Exercise import continuation and all publication guards through the
    // post-backfill work selection path. Legacy discovery is covered separately.
    f.sql("UPDATE html_preview_index_backfill SET complete=1 WHERE singleton=1")
        .await;
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
        let database = f.store.database_connection();
        let mut wake = database.work_signal("maintenance.html").subscribe();
        wake.begin_pass(&database).await.unwrap();
        let mut after = String::new();
        let mut progress = None;
        for ordinal in 0..n {
            f.routes.reset();
            f.step(&mut after, &mut progress).await;
            // Three exact proof reads; four helper reads only at page EOF;
            // discovery and durable gate once. Observer sees physical routes.
            let page_reads = if ordinal % page_len == 0 { 4 } else { 0 };
            assert_eq!(
                f.routes.counts(),
                (
                    3 + page_reads + 2 * usize::from(ordinal == 0),
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
        assert!(
            wake.wait(None).now_or_never().is_some(),
            "verified index completion must revisit retirement"
        );
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
    let error = index_quantum(&f.store, &mut after, &mut progress, "\u{10ffff}")
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
                let step = index_quantum(&f.store, &mut after, &mut progress, "\u{10ffff}");
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
            let error = index_quantum(&f.store, &mut after, &mut progress, "\u{10ffff}")
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
        !index_quantum(&f.store, &mut after, &mut progress, "\u{10ffff}")
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
    assert!(
        !index_quantum(&f.store, &mut after, &mut progress, "\u{10ffff}")
            .await
            .unwrap()
    ); // Complete markers are no longer enumerated.
    assert_eq!(f.routes.counts(), (2, 0));
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
        let step = index_quantum(&f.store, &mut after, &mut progress, "\u{10ffff}");
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

#[tokio::test]
async fn complete_gate_selects_only_live_ready_work_and_unknown_does_not_starve() {
    let f = fixture().await;
    for id in [
        "a-complete",
        "b-expired",
        "d-unknown",
        "e-work",
        "f-unknown-complete",
    ] {
        manifest(&f, id, "parent", &[], &[], 0, None).await;
    }
    f.sql("INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,ready) VALUES('c-unready','ws','parent','e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',0,0)").await;
    f.sql("UPDATE html_preview_frozen_index SET complete=1 WHERE manifest_id IN ('a-complete','f-unknown-complete')").await;
    f.sql("UPDATE compaction_frozen_history SET expired=1 WHERE id='b-expired'")
        .await;
    f.sql("UPDATE html_preview_frozen_index SET unknown=1 WHERE manifest_id IN ('d-unknown','f-unknown-complete')").await;
    f.sql("UPDATE html_preview_index_backfill SET complete=1 WHERE singleton=1")
        .await;
    let mut after = String::new();
    let mut progress = None;
    for expected in [
        &["d-unknown", "e-work", "f-unknown-complete"][..],
        &["d-unknown", "f-unknown-complete"][..],
    ] {
        for &id in expected {
            f.step(&mut after, &mut progress).await;
            assert_eq!(after, id);
            assert!(progress.is_none());
        }
        f.routes.reset();
        assert!(
            !index_quantum(&f.store, &mut after, &mut progress, "\u{10ffff}")
                .await
                .unwrap()
        );
        assert!(after.is_empty());
        assert_eq!(f.routes.counts(), (2, 0));
    }
    assert_eq!(f.marker("c-unready").await.complete, 0);
    assert_eq!(f.marker("e-work").await.complete, 1);
}

#[tokio::test]
async fn work_cursor_wrap_and_restart_find_new_and_newly_ready_headers_behind_cursor() {
    let f = fixture().await;
    manifest(&f, "m", "parent", &[], &[], 0, None).await;
    f.sql("INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,ready) VALUES('b','ws','parent','e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',0,0)").await;
    f.sql("UPDATE html_preview_index_backfill SET complete=1 WHERE singleton=1")
        .await;
    let mut after = String::new();
    let mut progress = None;
    f.step(&mut after, &mut progress).await;
    assert_eq!(after, "m");
    manifest(&f, "a", "parent", &[], &[], 0, None).await;
    f.sql("UPDATE compaction_frozen_history SET ready=1 WHERE id='b'")
        .await;
    assert!(
        !index_quantum(&f.store, &mut after, &mut progress, "\u{10ffff}")
            .await
            .unwrap()
    );
    assert!(after.is_empty());
    for id in ["a", "b"] {
        f.step(&mut after, &mut progress).await;
        assert_eq!(after, id);
        assert_eq!(f.marker(id).await.complete, 1);
    }
    manifest(&f, "0-restart", "parent", &[], &[], 0, None).await;
    after = String::new(); // New process/owner: no cached inventory.
    f.step(&mut after, &mut progress).await;
    assert_eq!(after, "0-restart");
    for _ in 0..2 {
        f.routes.reset();
        assert!(
            !index_quantum(&f.store, &mut after, &mut progress, "\u{10ffff}")
                .await
                .unwrap()
        );
        assert_eq!(
            f.routes.counts(),
            (2, 0),
            "quiet pass skips all completed headers"
        );
    }
    // Deletion/expiry triggers remove unsafe work even with fixture FK OFF.
    manifest(&f, "x-delete", "parent", &[], &[], 0, None).await;
    manifest(&f, "y-expire", "parent", &[], &[], 0, None).await;
    f.sql("DELETE FROM compaction_frozen_history WHERE id='x-delete'")
        .await;
    f.sql("UPDATE compaction_frozen_history SET expired=1 WHERE id='y-expire'")
        .await;
    assert!(
        !index_quantum(&f.store, &mut after, &mut progress, "\u{10ffff}")
            .await
            .unwrap()
    );
    assert_eq!(f.marker("x-delete").await.expired, 1);
    assert_eq!(f.marker("y-expire").await.expired, 1);
}

#[tokio::test]
async fn missing_gate_still_backfills_a_legacy_header_without_marker() {
    let f = fixture().await;
    f.sql("DELETE FROM html_preview_index_backfill WHERE singleton=1")
        .await;
    let trigger: String = f.store.connection.query_one_raw(Statement::from_string(
        DbBackend::Sqlite,
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='html_preview_header_insert'",
    )).await.unwrap().unwrap().try_get("", "sql").unwrap();
    f.sql("DROP TRIGGER html_preview_header_insert").await;
    manifest(&f, "legacy", "parent", &[], &[], 0, None).await;
    f.sql(&trigger).await; // Restore the production trigger before maintenance.
    let mut after = String::new();
    let mut progress = None;
    f.step(&mut after, &mut progress).await;
    assert_eq!(f.marker("legacy").await.complete, 1);
    assert!(
        !index_quantum(&f.store, &mut after, &mut progress, "\u{10ffff}")
            .await
            .unwrap()
    );
    f.step(&mut after, &mut progress).await; // Missing gate remains conservative.
    assert_eq!(after, "legacy");
    assert!(
        backfill::Entity::find_by_id(1_i64)
            .one(&f.store.connection)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn completed_unknown_import_is_discoverable_and_rechecked_after_restart() {
    let f = fixture().await;
    let (targets, proofs) = seed(&f, 2, 0, false).await;
    start_imports(&f, &targets, &proofs, 2, None).await;
    for _ in 0..2 {
        f.sql("UPDATE html_preview_frozen_index SET complete=1,unknown=1,next_ordinal=2 WHERE manifest_id='a'").await;
        let mut after = String::new();
        let mut progress = None;
        f.step(&mut after, &mut progress).await; // Legacy marker reset.
        assert!(after.is_empty());
        assert_eq!(
            (f.marker("a").await.complete, f.marker("a").await.unknown),
            (0, 0)
        );
        f.step(&mut after, &mut progress).await;
        assert!(after.is_empty()); // Message page keeps cursor before imports.
        assert_eq!(f.marker("a").await.next_ordinal, 2);
        // Revisit from the pre-manifest cursor to start import verification.
        f.step(&mut after, &mut progress).await;
        f.step(&mut after, &mut progress).await;
        assert_eq!(
            (f.marker("a").await.complete, f.marker("a").await.unknown),
            (1, 0)
        );
        assert!(progress.is_none());
    }
}

#[tokio::test]
async fn failed_completion_is_not_published_and_remains_discoverable_after_wrap() {
    let f = fixture().await;
    manifest(&f, "a", "parent", &[], &[], 0, None).await;
    f.sql("UPDATE html_preview_index_backfill SET complete=1 WHERE singleton=1")
        .await;
    f.sql("CREATE TRIGGER fail_completion BEFORE UPDATE ON html_preview_frozen_index WHEN NEW.complete=1 BEGIN SELECT RAISE(ABORT,'fixture completion failure'); END").await;
    let mut after = String::new();
    let mut progress = None;
    assert!(
        index_quantum(&f.store, &mut after, &mut progress, "\u{10ffff}")
            .await
            .is_err()
    );
    assert_eq!(f.marker("a").await.complete, 0);
    f.sql("DROP TRIGGER fail_completion").await;
    assert!(
        !index_quantum(&f.store, &mut after, &mut progress, "\u{10ffff}")
            .await
            .unwrap()
    );
    f.step(&mut after, &mut progress).await;
    assert_eq!(f.marker("a").await.complete, 1);
}

#[tokio::test]
async fn input_gate_is_conservative_until_complete_and_quiet_after_completion() {
    for missing in [false, true] {
        let f = fixture().await;
        if missing {
            f.sql("DELETE FROM html_preview_index_backfill WHERE singleton=1")
                .await;
        }
        f.sql(r#"INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES('old','parent-turn',0,'text','legacy','{"type":"text","text":"legacy"}',CURRENT_TIMESTAMP)"#).await;
        let mut after = String::new();
        f.routes.reset();
        assert!(
            input_adoption_quantum(&f.store, &mut after, "\u{10ffff}")
                .await
                .unwrap()
        );
        assert_eq!(after, "old");
        assert_eq!(f.routes.counts(), (2, 0));
        assert!(
            !input_adoption_quantum(&f.store, &mut after, "\u{10ffff}")
                .await
                .unwrap()
        );
        if missing {
            assert!(
                input_adoption_quantum(&f.store, &mut after, "\u{10ffff}")
                    .await
                    .unwrap()
            );
            f.sql("INSERT INTO html_preview_index_backfill(singleton,complete,inputs_complete) VALUES(1,0,1)").await;
        }
        for _ in 0..2 {
            f.routes.reset();
            assert!(
                !input_adoption_quantum(&f.store, &mut after, "\u{10ffff}")
                    .await
                    .unwrap()
            );
            assert!(after.is_empty());
            assert_eq!(f.routes.counts(), (1, 0));
        }
        // A future legacy importer can explicitly reopen the durable gate.
        f.sql("UPDATE html_preview_index_backfill SET inputs_complete=0 WHERE singleton=1")
            .await;
        assert!(
            input_adoption_quantum(&f.store, &mut after, "\u{10ffff}")
                .await
                .unwrap()
        );
        assert_eq!(after, "old");
    }
}

async fn preview_input_fixture(f: &Fixture) {
    // Real artifact/version/blob prerequisites; two versions share the same blob.
    for sql in [
        "INSERT INTO artifact_blob(id,workspace_id,sha256,size_bytes,storage_backend,storage_key) VALUES('blob','ws','fixture-sha',1,'local','fixture-key')",
        r#"INSERT INTO artifact(id,workspace_id,primary_thread_id,display_name,kind,mime_type,status,created_by_kind,created_by_actor_id,metadata_json) VALUES('preview','ws','parent','Preview','screenshot','image/png','ready','tool','html_preview','{"htmlTool":{"version":1,"publication":false,"turnId":"parent-turn","callId":"preview-call"}}')"#,
        "INSERT INTO artifact_version(id,workspace_id,artifact_id,version_number,blob_id) VALUES('v1','ws','preview',1,'blob'),('v2','ws','preview',2,'blob')",
        "UPDATE artifact SET current_version_id='v1' WHERE id='preview'",
        "INSERT INTO artifact_binding(id,artifact_id,artifact_version_id,workspace_id,thread_id,turn_id,tool_call_id,binding_kind,direction,role) VALUES('output','preview','v1','ws','parent','parent-turn','preview-call','tool_output','output','tool')",
    ] {
        f.sql(sql).await;
    }
    assert!(preview_is_temporary(f).await);
}
async fn preview_is_temporary(f: &Fixture) -> bool {
    let query = Query::select()
        .expr_as(temporary_preview_predicate("a"), "temporary")
        .from_as(artifact::Entity, "a")
        .and_where(col("a", artifact::Column::Id).eq("preview"))
        .to_owned();
    f.store
        .connection
        .query_one(&query)
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "temporary")
        .unwrap()
        == 1
}

#[tokio::test]
async fn new_inputs_after_complete_gate_are_adopted_atomically_and_rollback_preserves_old_input() {
    for failure in ["binding", "input"] {
        let f = fixture().await;
        preview_input_fixture(&f).await;
        f.sql("UPDATE html_preview_index_backfill SET inputs_complete=1 WHERE singleton=1")
            .await;
        f.sql(r#"INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload) VALUES('old','parent-turn',0,'text','old','{"type":"text","text":"old"}')"#).await;
        let input = pioneer_protocol::UserInput::Artifact {
            artifact_id: "preview".into(),
            version_id: Some("v1".into()),
        };
        f.sql(if failure == "binding" {
            "CREATE TRIGGER fail_adoption BEFORE INSERT ON artifact_binding WHEN NEW.binding_kind='user_input' BEGIN SELECT RAISE(ABORT,'fixture binding failure'); END"
        } else {
            "CREATE TRIGGER fail_adoption BEFORE INSERT ON turn_input BEGIN SELECT RAISE(ABORT,'fixture input failure'); END"
        }).await;
        assert!(
            f.store
                .replace_turn_input_projection_for_test(
                    "parent-turn",
                    &[input.clone()],
                    1,
                    1_700_000_000
                )
                .await
                .is_err()
        );
        f.sql("DROP TRIGGER fail_adoption").await;
        assert!(preview_is_temporary(&f).await);
        assert!(
            turn_input::Entity::find_by_id("old")
                .one(&f.store.connection)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            artifact_binding::Entity::find()
                .filter(artifact_binding::Column::BindingKind.eq("user_input"))
                .all(&f.store.connection)
                .await
                .unwrap()
                .is_empty()
        );
        f.routes.reset();
        f.store
            .replace_turn_input_projection_for_test("parent-turn", &[input], 1, 1_700_000_000)
            .await
            .unwrap();
        assert_eq!(
            f.routes.counts(),
            (0, 1),
            "canonical adoption is one Maintenance transaction"
        );
        let bindings = artifact_binding::Entity::find()
            .filter(artifact_binding::Column::BindingKind.eq("user_input"))
            .all(&f.store.connection)
            .await
            .unwrap();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].artifact_version_id.as_deref(), Some("v1"));
        let rows = turn_input::Entity::find()
            .filter(turn_input::Column::TurnId.eq("parent-turn"))
            .all(&f.store.connection)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].input_type, "artifact");
        assert!(rows[0].id.as_str() < "~"); // Behind the previous legacy cursor.
        f.sql("UPDATE artifact SET current_version_id='v2' WHERE id='preview'")
            .await;
        assert!(!preview_is_temporary(&f).await);
        // Another version still owns the shared blob; ownership query is unchanged.
        let owners = Query::select()
            .expr_as(queries::blob_has_owners("b"), "owned")
            .from_as(artifact_blob::Entity, "b")
            .and_where(col("b", artifact_blob::Column::Id).eq("blob"))
            .to_owned();
        assert_eq!(
            f.store
                .connection
                .query_one(&owners)
                .await
                .unwrap()
                .unwrap()
                .try_get::<i64>("", "owned")
                .unwrap(),
            1
        );
        let mut after = "~".to_owned();
        f.routes.reset();
        assert!(
            !input_adoption_quantum(&f.store, &mut after, "\u{10ffff}")
                .await
                .unwrap()
        );
        assert_eq!(f.routes.counts(), (1, 0));
    }
}

#[tokio::test]
async fn legacy_versionless_input_is_adopted_once_and_protects_later_current_version() {
    let f = fixture().await;
    preview_input_fixture(&f).await;
    let input = pioneer_protocol::UserInput::Artifact {
        artifact_id: "preview".into(),
        version_id: None,
    };
    // Legacy row predating synchronous adoption, not a supported new writer.
    f.store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,payload) VALUES('legacy','parent-turn',0,'artifact',?)",
        [serde_json::to_string(&input).unwrap().into()],
    )).await.unwrap();
    let mut after = String::new();
    assert!(
        input_adoption_quantum(&f.store, &mut after, "\u{10ffff}")
            .await
            .unwrap()
    );
    assert!(!preview_is_temporary(&f).await);
    let binding = artifact_binding::Entity::find()
        .filter(artifact_binding::Column::BindingKind.eq("user_input"))
        .one(&f.store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(binding.artifact_version_id.as_deref(), Some("v1"));
    assert!(
        !input_adoption_quantum(&f.store, &mut after, "\u{10ffff}")
            .await
            .unwrap()
    );
    f.sql("UPDATE artifact SET current_version_id='v2' WHERE id='preview'")
        .await;
    assert!(!preview_is_temporary(&f).await);
    f.routes.reset();
    assert!(
        !input_adoption_quantum(&f.store, &mut after, "\u{10ffff}")
            .await
            .unwrap()
    );
    assert_eq!(f.routes.counts(), (1, 0));
    let bindings = artifact_binding::Entity::find()
        .filter(artifact_binding::Column::BindingKind.eq("user_input"))
        .all(&f.store.connection)
        .await
        .unwrap();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].artifact_version_id.as_deref(), Some("v1"));
}

#[tokio::test]
async fn concurrent_reader_sees_new_input_and_adoption_only_after_the_same_commit() {
    for commit in [false, true] {
        let f = fixture().await;
        preview_input_fixture(&f).await;
        f.sql("UPDATE html_preview_index_backfill SET inputs_complete=1 WHERE singleton=1")
            .await;
        let mut after = "~".to_owned();
        assert!(
            !input_adoption_quantum(&f.store, &mut after, "\u{10ffff}")
                .await
                .unwrap()
        );
        let prepared = super::super::turn::prepare_turn_input_projection(
            "parent-turn",
            &[pioneer_protocol::UserInput::Artifact {
                artifact_id: "preview".into(),
                version_id: Some("v1".into()),
            }],
            chrono::Utc::now().fixed_offset(),
        )
        .unwrap();
        let tx = f.store.connection.begin().await.unwrap();
        super::super::turn::replace_prepared_turn_input(&tx, prepared)
            .await
            .unwrap();
        // Query-only reader runs concurrently with the uncommitted write set.
        assert!(
            turn_input::Entity::find()
                .filter(turn_input::Column::TurnId.eq("parent-turn"))
                .all(&f.store.connection)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(preview_is_temporary(&f).await);
        if commit {
            tx.commit().await.unwrap();
        } else {
            tx.rollback().await.unwrap();
        }
        let rows = turn_input::Entity::find()
            .filter(turn_input::Column::TurnId.eq("parent-turn"))
            .all(&f.store.connection)
            .await
            .unwrap();
        assert_eq!(rows.len(), usize::from(commit));
        assert_eq!(preview_is_temporary(&f).await, !commit);
        let bindings = artifact_binding::Entity::find()
            .filter(artifact_binding::Column::BindingKind.eq("user_input"))
            .all(&f.store.connection)
            .await
            .unwrap();
        assert_eq!(bindings.len(), usize::from(commit));
    }
}

#[tokio::test]
async fn retirement_page_keeps_individual_metadata_budget_and_empty_reader_only_eof() {
    let f = fixture().await;
    for index in 0..17 {
        // Oversized metadata must never enter preparation even inside page16.
        let oversized = index == 8;
        f.store.connection.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO artifact(id,workspace_id,display_name,kind,mime_type,status,created_by_kind,created_by_actor_id,current_version_id,metadata_json) VALUES(?1,'ws','preview','screenshot','image/png','ready','tool','html_preview',?2,?3)",
            [format!("page-{index:02}").into(), if oversized {Some("advisory-version".to_owned())} else {None}.into(), if oversized {"x".repeat(1048577)} else {"{}".to_owned()}.into()],
        )).await.unwrap();
    }
    let mut progress = HtmlPreviewRetentionProgress::default();
    progress.upper[2] = Some(f.store.html_preview_artifact_frontier().await.unwrap());
    f.routes.reset();
    for _ in 0..17 {
        assert!(progress.artifact_quantum(&f.store).await.unwrap());
    }
    // Two compact discovery pages; no metadata fetch or writer reservation.
    assert_eq!(f.routes.counts(), (2, 0));
    f.routes.reset();
    assert!(!progress.artifact_quantum(&f.store).await.unwrap());
    assert_eq!(f.routes.counts(), (1, 0));
}

// Closed test gate: these exercise the real read-only/serialized-writer fixture.
fn io_queue_key(id: &str) -> String {
    super::super::html_preview_io_work::HtmlPreviewIoLocator::new(id).key
}
#[tokio::test]
async fn io_enqueue_rollback_and_idle_poll_never_claim_writer() {
    use super::super::html_preview_io_work::{HtmlPreviewIoFamily, request};
    use pioneer_entity::html_preview_io_work as work;
    let f = fixture().await;
    let tx = f.store.connection.begin().await.unwrap();
    request(&tx, HtmlPreviewIoFamily::Copies, "rollback-copies", 0, "ws")
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(
        work::Entity::find_by_id(("copies".to_owned(), io_queue_key("rollback-copies"), 0_i64))
            .one(&f.store.connection)
            .await
            .unwrap()
            .is_none()
    );
    f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
        .await;
    f.routes.reset();
    assert!(
        f.store
            .claim_html_preview_io_work(0, 60_000)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(f.routes.counts(), (1, 0));
}

#[tokio::test]
async fn io_takeover_and_reinsert_reject_stale_worker() {
    use super::super::html_preview_io_work::{HtmlPreviewIoFamily, ack, request};
    use pioneer_entity::html_preview_io_work as work;
    let f = fixture().await;
    f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
        .await;
    let tx = f.store.connection.begin().await.unwrap();
    request(&tx, HtmlPreviewIoFamily::Copies, "missing", 0, "ws")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let old = f
        .store
        .claim_html_preview_io_work(0, 10)
        .await
        .unwrap()
        .unwrap();
    let owner = f
        .store
        .claim_html_preview_io_work(10, 10)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(owner.row.claim_token, old.row.claim_token);
    let tx = f.store.connection.begin().await.unwrap();
    assert!(!ack(&tx, &old).await.unwrap());
    assert!(ack(&tx, &owner).await.unwrap());
    tx.commit().await.unwrap();
    let tx = f.store.connection.begin().await.unwrap();
    request(&tx, HtmlPreviewIoFamily::Copies, "missing", 0, "ws")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let next = f
        .store
        .claim_html_preview_io_work(20, 10)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(owner.row.incarnation, next.row.incarnation);
    let tx = f.store.connection.begin().await.unwrap();
    assert!(!ack(&tx, &owner).await.unwrap());
    tx.rollback().await.unwrap();
    assert!(
        work::Entity::find_by_id(("copies".to_owned(), io_queue_key("missing"), 0_i64))
            .one(&f.store.connection)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn io_producer_during_claim_preserves_pass_and_ack_keeps_new_request() {
    use super::super::html_preview_io_work::{HtmlPreviewIoFamily, ack, request};
    use pioneer_entity::html_preview_io_work as work;
    let f = fixture().await;
    f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
        .await;
    let tx = f.store.connection.begin().await.unwrap();
    request(&tx, HtmlPreviewIoFamily::Copies, "late", 0, "ws")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let old = f
        .store
        .claim_html_preview_io_work(0, 10)
        .await
        .unwrap()
        .unwrap();
    let tx = f.store.connection.begin().await.unwrap();
    request(&tx, HtmlPreviewIoFamily::Copies, "late", 0, "ws")
        .await
        .unwrap();
    let active = work::Entity::find_by_id(("copies".to_owned(), io_queue_key("late"), 0_i64))
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.claim_token, old.row.claim_token);
    assert_eq!(active.due_at, old.row.due_at);
    assert_eq!(active.pass_revision, old.row.pass_revision);
    assert_ne!(active.requested_revision, old.row.requested_revision);
    assert!(ack(&tx, &old).await.unwrap());
    tx.commit().await.unwrap();
    let next = f
        .store
        .claim_html_preview_io_work(chrono::Utc::now().timestamp_millis(), 10)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.row.pass_revision, active.requested_revision);
}

#[tokio::test]
async fn io_seed_completion_is_durable_across_restart() {
    let f = fixture().await;
    for _ in 0..256 {
        if !f
            .store
            .html_preview_io_seed_startup_quantum()
            .await
            .unwrap()
        {
            break;
        }
    }
    assert!(f.store.html_preview_io_seed_complete().await.unwrap());
    let restarted = f.store.clone();
    f.routes.reset();
    assert!(
        !restarted
            .html_preview_io_seed_startup_quantum()
            .await
            .unwrap()
    );
    assert_eq!(
        f.routes.counts().1,
        0,
        "completed seeds do not reopen at startup"
    );
}

#[tokio::test]
async fn io_zero_row_copy_cas_keeps_claim_durable() {
    use super::super::html_preview_io_work::{HtmlPreviewIoFamily, request};
    use pioneer_entity::html_preview_io_work as work;
    let f = fixture().await;
    f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
        .await;
    let tx = f.store.connection.begin().await.unwrap();
    request(&tx, HtmlPreviewIoFamily::Copies, "missing-source", 0, "ws")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let claim = f
        .store
        .claim_html_preview_io_work(0, 60_000)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !f.store
            .finish_html_preview_copy_cleanup_claimed(&claim, "stale", "prepared")
            .await
            .unwrap()
    );
    let row =
        work::Entity::find_by_id(("copies".to_owned(), io_queue_key("missing-source"), 0_i64))
            .one(&f.store.connection)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(row.state, "running");
    assert_eq!(row.claim_token, claim.row.claim_token);
}

async fn io_copy_source(f: &Fixture, id: &str, metadata: &str) {
    f.store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO artifact(id,workspace_id,display_name,kind,mime_type,status,created_by_kind,created_by_actor_id,created_at,updated_at,metadata_json) VALUES(?1,'ws','preview','image','image/png','deleted','tool','html_preview',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?2)",
        [id.into(),metadata.into()])).await.unwrap();
}

#[tokio::test]
async fn io_seed_continuous_resync_keeps_frontier_and_late_producer() {
    use super::super::html_preview_io_work::{HtmlPreviewIoFamily, request};
    use pioneer_entity::html_preview_io_work as work;
    let f = fixture().await;
    let metadata = r#"{"htmlTool":{"retention":"released","releasedCopies":["x"]}}"#;
    for id in ["a", "b", "c"] {
        io_copy_source(&f, id, metadata).await;
    }
    f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1 WHERE family='seed_witness'").await;
    assert!(
        f.store
            .html_preview_io_seed_startup_quantum()
            .await
            .unwrap()
    );
    let first = work::Entity::find_by_id(("seed_copies".to_owned(), String::new(), 0_i64))
        .one(&f.store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.cursor_id.as_deref(), Some("a"));
    io_copy_source(&f, "0-behind", metadata).await;
    let tx = f.store.connection.begin().await.unwrap();
    request(&tx, HtmlPreviewIoFamily::Copies, "0-behind", 0, "ws")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    for expected in ["b", "c"] {
        let tx = f.store.connection.begin().await.unwrap();
        request(&tx, HtmlPreviewIoFamily::SeedCopies, "", 0, "")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(
            f.store
                .html_preview_io_seed_startup_quantum()
                .await
                .unwrap()
        );
        let row = work::Entity::find_by_id(("seed_copies".to_owned(), String::new(), 0_i64))
            .one(&f.store.connection)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.cursor_id.as_deref(),
            Some(expected),
            "new requests cannot restart an active pass"
        );
        assert_eq!(row.upper_id, first.upper_id);
        assert_eq!(row.pass_revision, first.pass_revision);
    }
    assert!(
        work::Entity::find_by_id(("copies".to_owned(), io_queue_key("0-behind"), 0_i64))
            .one(&f.store.connection)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn io_seed_only_enqueues_real_copies() {
    use pioneer_entity::html_preview_io_work as work;
    let f = fixture().await;
    io_copy_source(
        &f,
        "empty",
        r#"{"htmlTool":{"retention":"released","releasedCopies":[]}}"#,
    )
    .await;
    io_copy_source(
        &f,
        "real",
        r#"{"htmlTool":{"retention":"released","releasedCopies":["x"]}}"#,
    )
    .await;
    for _ in 0..16 {
        if !f
            .store
            .html_preview_io_seed_startup_quantum()
            .await
            .unwrap()
        {
            break;
        }
    }
    assert!(
        work::Entity::find_by_id(("copies".to_owned(), io_queue_key("empty"), 0_i64))
            .one(&f.store.connection)
            .await
            .unwrap()
            .is_none()
    );
    let claim = f
        .store
        .claim_html_preview_io_work(chrono::Utc::now().timestamp_millis(), 60_000)
        .await
        .unwrap()
        .unwrap();
    let crate::HtmlPreviewIoSource::Ready((_workspace, metadata)) =
        f.store.load_html_preview_io_copies(&claim).await.unwrap()
    else {
        panic!("real copies")
    };
    assert!(!metadata.is_empty());
}

#[tokio::test]
async fn io_poison_quarantine_does_not_block_later_work_and_unknown_outcome_is_durable() {
    use super::super::html_preview_io_work::{HtmlPreviewIoFamily, request};
    use pioneer_entity::html_preview_io_work as work;
    let f = fixture().await;
    f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
        .await;
    let tx = f.store.connection.begin().await.unwrap();
    request(&tx, HtmlPreviewIoFamily::Witness, "a-poison", 0, "ws")
        .await
        .unwrap();
    request(&tx, HtmlPreviewIoFamily::Witness, "b-later", 0, "ws")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    work::Entity::update_many()
        .col_expr(work::Column::DueAt, Expr::val(-1_i64))
        .filter(work::Column::ObjectId.eq(io_queue_key("a-poison")))
        .exec(&f.store.connection)
        .await
        .unwrap();
    let claim = f
        .store
        .claim_html_preview_io_work(0, 10)
        .await
        .unwrap()
        .unwrap();
    assert!(
        f.store
            .defer_html_preview_io_work(&claim, crate::HtmlPreviewIoOutcome::Quarantine, 0)
            .await
            .unwrap()
    );
    let later = f
        .store
        .claim_html_preview_io_work(0, 10)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(later.source_id(), "b-later");
    // Discard the successful claim response as a transport could: durable row
    // must remain recoverable; no empty/finished marker may be inferred.
    drop(later);
    let takeover = f
        .store
        .claim_html_preview_io_work(10, 10)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(takeover.source_id(), "b-later");
    let poison = work::Entity::find_by_id(("witness".to_owned(), io_queue_key("a-poison"), 0_i64))
        .one(&f.store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(poison.state, "quarantine");
    assert!(poison.due_at.is_none());
}

#[tokio::test]
async fn io_witness_zero_cas_corrupt_expired_and_unavailable_are_distinct() {
    use super::super::html_preview_io_work::{HtmlPreviewIoFamily, request};
    use pioneer_entity::html_preview_io_work as work;
    for mode in [
        "corrupt",
        "empty-unit",
        "invalid-digest",
        "expired",
        "unavailable",
        "missing-header",
        "body-restored",
    ] {
        let f = fixture().await;
        f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
            .await;
        if mode != "missing-header" {
            manifest(&f, "w", "parent", &[], &[], 0, None).await;
        }
        let mut reference = reference(
            SourceRef {
                scope: "context:parent-turn".into(),
                id: "missing-source".into(),
                version: "revision:1".into(),
            },
            "parent",
            None,
        );
        let semantic_invalid = matches!(mode, "empty-unit" | "invalid-digest");
        let healthy_reference = reference.clone();
        if mode == "empty-unit" {
            reference.unit_id.clear();
        } else if mode == "invalid-digest" {
            reference.wire_sha256 = "invalid".into();
        }
        if semantic_invalid {
            // A real canonical source makes the old serde-only loader return
            // Ready: absence/provenance guards must not mask this regression.
            f.sql("INSERT INTO turn_llm_context(id,turn_id,sequence,source,tool_name,payload,output_policy_snapshot,created_at) VALUES('missing-source','parent-turn',999,'tool_result_v2','html_preview','{}','{}',CURRENT_TIMESTAMP)").await;
            f.sql("UPDATE compaction_source_revision SET revision=1,present=1 WHERE source_id='missing-source'").await;
        }
        let reference_json = if mode == "corrupt" {
            "invalid".into()
        } else {
            serde_json::to_string(&reference).unwrap()
        };
        frozen_result::Entity::insert(frozen_result::ActiveModel {
            manifest_id: Set("w".into()),
            ordinal: Set(0),
            reference_json: Set(reference_json),
            turn_id: Set("parent-turn".into()),
            source_id: Set("missing-source".into()),
            source_version: Set("revision:1".into()),
            archive_wire_sha256: Set(None),
        })
        .exec_without_returning(&f.store.connection)
        .await
        .unwrap();
        if mode == "expired" {
            f.sql("UPDATE compaction_frozen_history SET expired=1 WHERE id='w'")
                .await;
        }
        if mode == "body-restored" {
            f.sql("INSERT INTO compaction_source_revision(source_id,turn_id,revision,present,capture_order) VALUES('missing-source','parent-turn',1,1,9999)").await;
        }
        let tx = f.store.connection.begin().await.unwrap();
        request(&tx, HtmlPreviewIoFamily::Witness, "w", 0, "ws")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let claim = f
            .store
            .claim_html_preview_io_work(0, 60_000)
            .await
            .unwrap()
            .unwrap();
        let outcome = f.store.load_html_preview_io_witness(&claim).await.unwrap();
        match mode {
            "corrupt" | "empty-unit" | "invalid-digest" => {
                assert!(matches!(outcome, crate::HtmlPreviewIoSource::Quarantine))
            }
            "expired" => assert!(matches!(outcome, crate::HtmlPreviewIoSource::Obsolete)),
            _ => assert!(matches!(outcome, crate::HtmlPreviewIoSource::Retry)),
        }
        if semantic_invalid {
            assert!(
                f.store
                    .defer_html_preview_io_work(&claim, crate::HtmlPreviewIoOutcome::Quarantine, 0)
                    .await
                    .unwrap()
            );
            let poison = work::Entity::find_by_id(("witness".to_owned(), io_queue_key("w"), 0_i64))
                .one(&f.store.connection)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(poison.state, "quarantine");
            assert!(poison.due_at.is_none());
            let retained = frozen_result::Entity::find_by_id(("w".to_owned(), 0_i64))
                .one(&f.store.connection)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::from_str::<FrozenMessageRef>(&retained.reference_json).unwrap(),
                reference
            );
            assert!(retained.archive_wire_sha256.is_none());
            frozen_result::Entity::insert(frozen_result::ActiveModel {
                manifest_id: Set("w".into()),
                ordinal: Set(1),
                reference_json: Set(serde_json::to_string(&healthy_reference).unwrap()),
                turn_id: Set("parent-turn".into()),
                source_id: Set("missing-source".into()),
                source_version: Set("revision:1".into()),
                archive_wire_sha256: Set(None),
            })
            .exec_without_returning(&f.store.connection)
            .await
            .unwrap();
            let tx = f.store.connection.begin().await.unwrap();
            request(&tx, HtmlPreviewIoFamily::Witness, "w", 1, "ws")
                .await
                .unwrap();
            tx.commit().await.unwrap();
            let healthy = f
                .store
                .claim_html_preview_io_work(0, 60_000)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(healthy.row.ordinal, 1);
            assert!(matches!(
                f.store
                    .load_html_preview_io_witness(&healthy)
                    .await
                    .unwrap(),
                crate::HtmlPreviewIoSource::Ready(_)
            ));
            continue;
        }
        if mode == "body-restored" {
            // Pending work remains addressably timed, and restores without
            // changing its source identity/revision or requiring a resync.
            assert!(
                f.store
                    .defer_html_preview_io_work(&claim, crate::HtmlPreviewIoOutcome::Retry, 0)
                    .await
                    .unwrap()
            );
            let pending =
                work::Entity::find_by_id(("witness".to_owned(), io_queue_key("w"), 0_i64))
                    .one(&f.store.connection)
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(pending.state, "timed");
            assert_eq!(pending.due_at, Some(60_000));
            assert!(
                f.store
                    .claim_html_preview_io_work(59_999, 60_000)
                    .await
                    .unwrap()
                    .is_none()
            );
            let claim = f
                .store
                .claim_html_preview_io_work(60_000, 60_000)
                .await
                .unwrap()
                .unwrap();
            f.sql("INSERT INTO turn_llm_context(id,turn_id,sequence,source,tool_name,payload,output_policy_snapshot,created_at) VALUES('missing-source','parent-turn',999,'tool_result_v2','html_preview','{}','{}',CURRENT_TIMESTAMP)").await;
            // Historical restoration preserves its original revision identity.
            f.sql("UPDATE compaction_source_revision SET revision=1,present=1 WHERE source_id='missing-source'").await;
            let crate::HtmlPreviewIoSource::Ready(witness) =
                f.store.load_html_preview_io_witness(&claim).await.unwrap()
            else {
                panic!("restored body must become ready");
            };
            let hash = "a".repeat(64);
            let database = f.store.database_connection();
            let mut wake = database.work_signal("maintenance.html").subscribe();
            wake.begin_pass(&database).await.unwrap();
            f.store
                .record_html_preview_archive_witness(&witness, &hash)
                .await
                .unwrap();
            assert!(
                wake.wait(None).now_or_never().is_some(),
                "new witness must revisit retirement"
            );
            wake.begin_pass(&database).await.unwrap();
            let tx = f.store.connection.begin().await.unwrap();
            request(&tx, HtmlPreviewIoFamily::Witness, "w", 0, "ws")
                .await
                .unwrap();
            tx.commit().await.unwrap();
            let repeated = f
                .store
                .claim_html_preview_io_work(0, 60_000)
                .await
                .unwrap()
                .unwrap();
            assert!(
                f.store
                    .record_html_preview_archive_witness_claimed(&repeated, &witness, &hash)
                    .await
                    .unwrap(),
                "zero-row publication requires verified reread before ACK"
            );
            assert!(
                wake.wait(None).now_or_never().is_none(),
                "verified replay ACK must stay quiet"
            );
            assert!(
                work::Entity::find_by_id(("witness".to_owned(), io_queue_key("w"), 0_i64))
                    .one(&f.store.connection)
                    .await
                    .unwrap()
                    .is_none()
            );
            continue;
        }
        let witness = HtmlPreviewArchiveWitness {
            manifest: "w".into(),
            ordinal: 0,
            workspace: "ws".into(),
            owner_thread: "parent".into(),
            reference,
            payload: "{}".into(),
        };
        assert!(
            !f.store
                .record_html_preview_archive_witness_claimed(&claim, &witness, &"a".repeat(64))
                .await
                .unwrap(),
            "refused source cannot ACK"
        );
        assert!(
            f.store
                .record_html_preview_archive_witness(&witness, &"a".repeat(64))
                .await
                .is_err(),
            "legacy callback cannot report zero-row CAS success"
        );
        assert!(
            work::Entity::find_by_id(("witness".to_owned(), io_queue_key("w"), 0_i64))
                .one(&f.store.connection)
                .await
                .unwrap()
                .is_some()
        );
        if matches!(mode, "unavailable" | "missing-header") {
            assert!(
                f.store
                    .defer_html_preview_io_work(&claim, crate::HtmlPreviewIoOutcome::Retry, 0)
                    .await
                    .unwrap()
            );
            let pending =
                work::Entity::find_by_id(("witness".to_owned(), io_queue_key("w"), 0_i64))
                    .one(&f.store.connection)
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(pending.state, "timed");
            assert_eq!(pending.due_at, Some(60_000));
        }
    }
}

#[tokio::test]
async fn io_repair_updates_workspace_and_reopens_quarantine_without_losing_active_claim() {
    use super::super::html_preview_io_work::{HtmlPreviewIoFamily, ack, request};
    use pioneer_entity::html_preview_io_work as work;
    let f = fixture().await;
    f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
        .await;
    let tx = f.store.connection.begin().await.unwrap();
    request(&tx, HtmlPreviewIoFamily::Copies, "repair", 0, "old")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let old = f
        .store
        .claim_html_preview_io_work(0, 10)
        .await
        .unwrap()
        .unwrap();
    let tx = f.store.connection.begin().await.unwrap();
    request(&tx, HtmlPreviewIoFamily::Copies, "repair", 0, "ws")
        .await
        .unwrap();
    assert!(ack(&tx, &old).await.unwrap());
    tx.commit().await.unwrap();
    let current = work::Entity::find_by_id(("copies".to_owned(), io_queue_key("repair"), 0_i64))
        .one(&f.store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.workspace_id, "ws");
    assert_eq!(current.state, "ready");
}

#[tokio::test]
async fn io_copy_cleanup_refuses_sql_nocase_match_after_cooperative_resync() {
    use super::super::html_preview_io_work::{HtmlPreviewIoFamily, HtmlPreviewIoLocator, request};
    use pioneer_entity::html_preview_io_work as work;
    for claimed in [true, false] {
        let f = fixture().await;
        // Vary only the empty artifact table from the normal Migrator fixture.
        // SQLite cannot ALTER COLUMN collation. Reuse its actual migrated DDL
        // and restore its actual indexes/triggers; no handwritten Entity/schema.
        assert!(
            artifact::Entity::find()
                .one(&f.store.connection)
                .await
                .unwrap()
                .is_none()
        );
        let ddl = f
            .store
            .connection
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT sql FROM sqlite_schema WHERE type='table' AND name='artifact'",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<String>("", "sql")
            .unwrap();
        let nocase_ddl = ddl.replacen(
            "\"metadata_json\" text",
            "\"metadata_json\" text COLLATE NOCASE",
            1,
        );
        assert_ne!(
            ddl, nocase_ddl,
            "fixture must actually install NOCASE on the metadata column"
        );
        let dependents = f.store.connection.query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT sql FROM sqlite_schema WHERE tbl_name='artifact' AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY type,name",
        )).await.unwrap().into_iter().map(|row| row.try_get::<String>("", "sql").unwrap()).collect::<Vec<_>>();
        let tx = f.store.connection.begin().await.unwrap();
        tx.execute_unprepared("DROP TABLE artifact").await.unwrap();
        tx.execute_unprepared(&nocase_ddl).await.unwrap();
        for ddl in dependents {
            tx.execute_unprepared(&ddl).await.unwrap();
        }
        tx.commit().await.unwrap();
        f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
            .await;

        let previous = r#"{"htmlTool":{"retention":"released","releasedCopies":["A.html"]}}"#;
        let repaired = r#"{"htmlTool":{"retention":"released","releasedCopies":["a.html"]}}"#;
        let cleaned = r#"{"htmlTool":{"retention":"released","releasedCopies":[]}}"#;
        io_copy_source(&f, "nocase", previous).await;
        let tx = f.store.connection.begin().await.unwrap();
        request(&tx, HtmlPreviewIoFamily::Copies, "nocase", 0, "ws")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let now = chrono::Utc::now().timestamp_millis();
        let old = f
            .store
            .claim_html_preview_io_work(now, 300_000)
            .await
            .unwrap()
            .unwrap();
        let crate::HtmlPreviewIoSource::Ready((workspace, prepared_from)) =
            f.store.load_html_preview_io_copies(&old).await.unwrap()
        else {
            panic!("old source must be ready")
        };
        assert_eq!(workspace, "ws");
        assert_eq!(prepared_from, previous);
        // Cleanup preparation has finished from A.html before a cooperative
        // repair publishes a.html and its durable scoped resync atomically.
        let prepared_cleanup = cleaned.to_owned();
        let scope = HtmlPreviewIoLocator::new("ws");
        let tx = f.store.connection.begin().await.unwrap();
        assert_eq!(
            artifact::Entity::update_many()
                .col_expr(artifact::Column::MetadataJson, Expr::val(repaired))
                .filter(artifact::Column::Id.eq("nocase"))
                .exec(&tx)
                .await
                .unwrap()
                .rows_affected,
            1
        );
        f.store
            .request_html_preview_io_resync(&tx, &scope, &[HtmlPreviewIoFamily::Copies])
            .await
            .unwrap();
        tx.commit().await.unwrap();
        f.store.notify_html_preview_io_work();

        // This is the actual old SQL source fence, with column collation (no
        // explicit COLLATE override). A binary-only fixture cannot pass here.
        let sql_equal = artifact::Entity::find_by_id("nocase")
            .filter(artifact::Column::CurrentVersionId.is_null())
            .filter(artifact::Column::MetadataJson.eq(&prepared_from))
            .one(&f.store.connection)
            .await
            .unwrap()
            .expect("ordinary SQL '=' must falsely equate A.html and a.html");
        assert_eq!(sql_equal.metadata_json, repaired);
        assert_ne!(sql_equal.metadata_json, prepared_from);
        let scope_key = ("seed_copies".to_owned(), scope.key.clone(), 0_i64);
        assert!(
            work::Entity::find_by_id(scope_key.clone())
                .one(&f.store.connection)
                .await
                .unwrap()
                .is_some()
        );
        // Process this scoped pass before the stale callback: its newer point
        // request must survive as well as the repaired source metadata.
        for _ in 0..2 {
            let seed = f
                .store
                .claim_html_preview_io_work(chrono::Utc::now().timestamp_millis(), 300_000)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(seed.row.family, "seed_copies");
            assert!(f.store.html_preview_io_seed_quantum(&seed).await.unwrap());
        }
        let key = ("copies".to_owned(), io_queue_key("nocase"), 0_i64);
        let renewed = work::Entity::find_by_id(key.clone())
            .one(&f.store.connection)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(renewed.requested_revision, old.row.requested_revision);
        assert_eq!(renewed.claim_token, old.row.claim_token);
        assert_eq!(renewed.pass_revision, old.row.pass_revision);
        if claimed {
            assert!(
                !f.store
                    .finish_html_preview_copy_cleanup_claimed(
                        &old,
                        &prepared_from,
                        &prepared_cleanup
                    )
                    .await
                    .unwrap()
            );
        } else {
            assert!(
                f.store
                    .finish_html_preview_copy_cleanup("nocase", &prepared_from, &prepared_cleanup)
                    .await
                    .is_err()
            );
        }
        let preserved = work::Entity::find_by_id(key.clone())
            .one(&f.store.connection)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            preserved, renewed,
            "refusal must neither ACK nor change the durable obligation"
        );
        assert_eq!(
            artifact::Entity::find_by_id("nocase")
                .one(&f.store.connection)
                .await
                .unwrap()
                .unwrap()
                .metadata_json,
            repaired
        );

        assert!(
            !f.store
                .defer_html_preview_io_work(&old, crate::HtmlPreviewIoOutcome::Retry, now)
                .await
                .unwrap()
        );
        let next = f
            .store
            .claim_html_preview_io_work(chrono::Utc::now().timestamp_millis() + 60_000, 300_000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next.row.family, "copies");
        let crate::HtmlPreviewIoSource::Ready((workspace, previous)) =
            f.store.load_html_preview_io_copies(&next).await.unwrap()
        else {
            panic!("repaired copies must remain discoverable")
        };
        assert_eq!(workspace, "ws");
        assert_eq!(previous, repaired);
        assert!(
            f.store
                .finish_html_preview_copy_cleanup_claimed(&next, &previous, cleaned)
                .await
                .unwrap()
        );
        assert!(
            work::Entity::find_by_id(key)
                .one(&f.store.connection)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            artifact::Entity::find_by_id("nocase")
                .one(&f.store.connection)
                .await
                .unwrap()
                .unwrap()
                .metadata_json,
            cleaned
        );
    }
}

#[tokio::test]
async fn io_legacy_copy_callback_commits_source_and_ack_atomically() {
    use super::super::html_preview_io_work::{HtmlPreviewIoFamily, request};
    use pioneer_entity::html_preview_io_work as work;
    let f = fixture().await;
    let previous = r#"{"htmlTool":{"retention":"released","releasedCopies":["x"]}}"#;
    io_copy_source(&f, "callback", previous).await;
    let tx = f.store.connection.begin().await.unwrap();
    request(&tx, HtmlPreviewIoFamily::Copies, "callback", 0, "ws")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(
        f.store
            .finish_html_preview_copy_cleanup("callback", "stale", "{}")
            .await
            .is_err()
    );
    assert!(
        work::Entity::find_by_id(("copies".to_owned(), io_queue_key("callback"), 0_i64))
            .one(&f.store.connection)
            .await
            .unwrap()
            .is_some()
    );
    f.store
        .finish_html_preview_copy_cleanup("callback", previous, "{}")
        .await
        .unwrap();
    assert!(
        work::Entity::find_by_id(("copies".to_owned(), io_queue_key("callback"), 0_i64))
            .one(&f.store.connection)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        artifact::Entity::find_by_id("callback")
            .one(&f.store.connection)
            .await
            .unwrap()
            .unwrap()
            .metadata_json,
        "{}"
    );
}

#[tokio::test]
async fn gc_deadline_crossing_behind_a_finished_page_is_retained_for_the_next_pass() {
    let f = fixture().await;
    f.sql(r#"INSERT INTO artifact_blob(id,workspace_id,sha256,size_bytes,storage_backend,storage_key,created_at,metadata_json)
        VALUES('deadline-blob','ws','deadline-sha',1,'local','deadline-key','2020-01-01 00:00:00 +00:00','{"htmlToolTemporary":1}')"#).await;
    let created_ms = 1_577_836_800_000_i64;
    let grace_ms = 60_000_i64;
    let cutoff = created_ms - 1;
    let (after, candidates) = f.store.html_preview_gc_blob_page("", cutoff).await.unwrap();
    assert!(candidates.is_empty());
    assert!(
        f.store
            .html_preview_gc_blob_page(&after, cutoff)
            .await
            .unwrap()
            .0
            .is_empty()
    );
    // The real clock is already later than this fixture's grace deadline.
    // Recomputing a "future only" cutoff at EOF would silently lose it.
    assert_eq!(
        f.store
            .html_preview_gc_deadline(grace_ms, cutoff)
            .await
            .unwrap(),
        Some(1_577_836_860)
    );
    let (_, candidates) = f
        .store
        .html_preview_gc_blob_page("", created_ms)
        .await
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].1.blob_id, "deadline-blob");
    assert_eq!(
        f.store
            .html_preview_gc_deadline(grace_ms, created_ms)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn io_released_import_producer_rolls_back_domain_and_obligation_together() {
    use super::super::artifact::{IngestArtifactMetadataRecord, NewArtifactBlobRecord};
    use sea_orm::PaginatorTrait;
    let f = fixture().await;
    f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
        .await;
    let blob = NewArtifactBlobRecord {
        workspace_id: "ws".into(),
        sha256: "a".repeat(64),
        size_bytes: 1,
        mime_type: Some("image/png".into()),
        storage_backend: "local".into(),
        storage_key: "restore".into(),
        metadata: Default::default(),
    };
    let request = IngestArtifactMetadataRecord {
        workspace_id: "ws".into(),
        primary_thread_id: None,
        display_name: "released".into(),
        kind: pioneer_protocol::ArtifactKind::Image,
        mime_type: Some("image/png".into()),
        created_by_kind: pioneer_protocol::ArtifactCreatedByKind::Tool,
        created_by_actor_id: Some("html_preview".into()),
        metadata: std::collections::BTreeMap::from([(
            "htmlTool".into(),
            json!({"retention":"released","releasedCopies":["x"]}),
        )]),
    };
    let before = (
        artifact::Entity::find()
            .count(&f.store.connection)
            .await
            .unwrap(),
        artifact_blob::Entity::find()
            .count(&f.store.connection)
            .await
            .unwrap(),
        artifact_version::Entity::find()
            .count(&f.store.connection)
            .await
            .unwrap(),
    );
    f.sql("CREATE TRIGGER io_fail_enqueue BEFORE INSERT ON html_preview_io_work WHEN NEW.family='copies' BEGIN SELECT RAISE(ABORT,'injected enqueue failure'); END").await;
    assert!(
        f.store
            .ingest_artifact_metadata_with_identity(
                blob.clone(),
                request.clone(),
                None,
                Default::default(),
                None,
                None,
                None
            )
            .await
            .is_err()
    );
    assert_eq!(
        before,
        (
            artifact::Entity::find()
                .count(&f.store.connection)
                .await
                .unwrap(),
            artifact_blob::Entity::find()
                .count(&f.store.connection)
                .await
                .unwrap(),
            artifact_version::Entity::find()
                .count(&f.store.connection)
                .await
                .unwrap()
        )
    );
    f.sql("DROP TRIGGER io_fail_enqueue").await;
    let imported = f
        .store
        .ingest_artifact_metadata_with_identity(
            blob,
            request,
            None,
            Default::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert!(imported.artifact.current_version_id.is_none());
    let claim = f
        .store
        .claim_html_preview_io_work(chrono::Utc::now().timestamp_millis(), 60_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.source_id(), imported.artifact.id);
    assert!(matches!(
        f.store.load_html_preview_io_copies(&claim).await.unwrap(),
        crate::HtmlPreviewIoSource::Ready(_)
    ));
}

#[tokio::test]
async fn io_startup_does_not_spin_while_runtime_owns_seed_claims() {
    let f = fixture().await;
    let now = chrono::Utc::now().timestamp_millis();
    let first = f
        .store
        .claim_html_preview_io_work(now, 300_000)
        .await
        .unwrap()
        .unwrap();
    let second = f
        .store
        .claim_html_preview_io_work(now, 300_000)
        .await
        .unwrap()
        .unwrap();
    assert!(first.row.family.starts_with("seed_") && second.row.family.starts_with("seed_"));
    f.routes.reset();
    assert!(
        !f.store
            .html_preview_io_seed_startup_quantum()
            .await
            .unwrap()
    );
    assert_eq!(
        f.routes.counts().1,
        0,
        "occupied durable seeds do not cause startup claim writes"
    );
}

#[tokio::test]
async fn io_long_source_locator_seed_and_claim_are_lossless_and_collision_checked() {
    use super::super::html_preview_io_work::{
        HtmlPreviewIoFamily, HtmlPreviewIoLocator, request, request_prepared,
    };
    let f = fixture().await;
    let id = "legacy-source-".repeat(1024);
    let metadata = r#"{"htmlTool":{"retention":"released","releasedCopies":["x"]}}"#;
    io_copy_source(&f, &id, metadata).await;
    for _ in 0..16 {
        if !f
            .store
            .html_preview_io_seed_startup_quantum()
            .await
            .unwrap()
        {
            break;
        }
    }
    let claim = f
        .store
        .claim_html_preview_io_work(chrono::Utc::now().timestamp_millis(), 60_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.row.object_id.len(), 64);
    assert_eq!(claim.source_id(), id);
    assert!(matches!(
        f.store.load_html_preview_io_copies(&claim).await.unwrap(),
        crate::HtmlPreviewIoSource::Ready(_)
    ));
    let tx = f.store.connection.begin().await.unwrap();
    request(&tx, HtmlPreviewIoFamily::Copies, &id, 0, "ws")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(
        f.store
            .finish_html_preview_copy_cleanup_claimed(&claim, metadata, "{}")
            .await
            .unwrap()
    );
    let next = f
        .store
        .claim_html_preview_io_work(chrono::Utc::now().timestamp_millis(), 60_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        next.source_id(),
        id,
        "new request must preserve full point locator"
    );
    let tx = f.store.connection.begin().await.unwrap();
    let collision = HtmlPreviewIoLocator {
        source_id: "different source".into(),
        key: claim.row.object_id,
    };
    assert!(
        request_prepared(&tx, HtmlPreviewIoFamily::Copies, &collision, 0, "ws")
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn io_witness_seed_ignores_verified_and_expired_history() {
    use pioneer_entity::html_preview_io_work as work;
    let f = fixture().await;
    for (id, expired, hash) in [
        ("pending", false, None),
        ("verified", false, Some("a".repeat(64))),
        ("expired", true, None),
    ] {
        manifest(&f, id, "parent", &[], &[], 0, None).await;
        frozen_result::Entity::insert(frozen_result::ActiveModel {
            manifest_id: Set(id.into()),
            ordinal: Set(0),
            reference_json: Set("{}".into()),
            turn_id: Set("parent-turn".into()),
            source_id: Set("missing-source".into()),
            source_version: Set("revision:1".into()),
            archive_wire_sha256: Set(hash),
        })
        .exec_without_returning(&f.store.connection)
        .await
        .unwrap();
        if expired {
            history::Entity::update_many()
                .col_expr(history::Column::Expired, Expr::val(1))
                .filter(history::Column::Id.eq(id))
                .exec(&f.store.connection)
                .await
                .unwrap();
        }
    }
    for _ in 0..16 {
        if !f
            .store
            .html_preview_io_seed_startup_quantum()
            .await
            .unwrap()
        {
            break;
        }
    }
    for (id, expected) in [("pending", true), ("verified", false), ("expired", false)] {
        assert_eq!(
            work::Entity::find_by_id(("witness".to_owned(), io_queue_key(id), 0_i64))
                .one(&f.store.connection)
                .await
                .unwrap()
                .is_some(),
            expected
        );
    }
}

#[tokio::test]
async fn io_malformed_point_locator_quarantines_and_next_ticket_progresses() {
    use super::super::html_preview_io_work::{HtmlPreviewIoFamily, request};
    use pioneer_entity::html_preview_io_work as work;
    let f = fixture().await;
    f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
        .await;
    io_copy_source(
        &f,
        "good",
        r#"{"htmlTool":{"retention":"released","releasedCopies":["x"]}}"#,
    )
    .await;
    let tx = f.store.connection.begin().await.unwrap();
    request(&tx, HtmlPreviewIoFamily::Copies, "bad", 0, "ws")
        .await
        .unwrap();
    request(&tx, HtmlPreviewIoFamily::Copies, "good", 0, "ws")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    work::Entity::update_many()
        .filter(work::Column::ObjectId.eq(io_queue_key("bad")))
        .col_expr(work::Column::CursorId, Expr::val(None::<String>))
        .col_expr(work::Column::DueAt, Expr::val(-1_i64))
        .exec(&f.store.connection)
        .await
        .unwrap();
    let bad = f
        .store
        .claim_html_preview_io_work(0, 60_000)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        f.store.load_html_preview_io_copies(&bad).await.unwrap(),
        crate::HtmlPreviewIoSource::Quarantine
    ));
    assert!(
        f.store
            .defer_html_preview_io_work(&bad, crate::HtmlPreviewIoOutcome::Quarantine, 0)
            .await
            .unwrap()
    );
    let good = f
        .store
        .claim_html_preview_io_work(0, 60_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(good.source_id(), "good");
    assert!(matches!(
        f.store.load_html_preview_io_copies(&good).await.unwrap(),
        crate::HtmlPreviewIoSource::Ready(_)
    ));
}

#[tokio::test]
async fn io_generic_claim_admits_point_work_during_continuous_resync() {
    use super::super::html_preview_io_work::{
        HtmlPreviewIoFamily, HtmlPreviewIoLocator, request_at, request_prepared,
    };
    let f = fixture().await;
    f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
        .await;
    let metadata = r#"{"htmlTool":{"retention":"released","releasedCopies":["x"]}}"#;
    for id in ["a", "b", "c"] {
        io_copy_source(&f, id, metadata).await;
    }
    let workspace = HtmlPreviewIoLocator::new("ws");
    let point = HtmlPreviewIoLocator::new("point");
    let tx = f.store.connection.begin().await.unwrap();
    request_at(&tx, HtmlPreviewIoFamily::SeedCopies, &workspace, 0, "ws", 0)
        .await
        .unwrap();
    request_at(&tx, HtmlPreviewIoFamily::Witness, &point, 0, "ws", 0)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut point_seen = false;
    let mut seed_quanta = 0;
    for _ in 0..32 {
        let now = chrono::Utc::now().timestamp_millis();
        let claim = f
            .store
            .claim_html_preview_io_work(now, 60_000)
            .await
            .unwrap()
            .unwrap();
        match claim.row.family.as_str() {
            "seed_copies" => {
                assert!(f.store.html_preview_io_seed_quantum(&claim).await.unwrap());
                seed_quanta += 1;
            }
            "witness" => {
                point_seen = true;
                assert!(
                    f.store
                        .defer_html_preview_io_work(&claim, crate::HtmlPreviewIoOutcome::Done, now)
                        .await
                        .unwrap()
                );
            }
            "copies" => {
                assert!(
                    f.store
                        .finish_html_preview_copy_cleanup_claimed(&claim, metadata, "{}")
                        .await
                        .unwrap()
                );
            }
            family => panic!("unexpected family {family}"),
        }
        let tx = f.store.connection.begin().await.unwrap();
        request_prepared(&tx, HtmlPreviewIoFamily::SeedCopies, &workspace, 0, "ws")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        if point_seen && seed_quanta >= 3 {
            break;
        }
    }
    assert!(point_seen, "resync must not starve point witnesses");
    assert!(
        seed_quanta >= 3,
        "point admission must preserve seed progress"
    );
}

#[tokio::test]
async fn io_index_enqueue_failure_rolls_back_result_and_progress() {
    use pioneer_entity::html_preview_io_work as work;
    let f = fixture().await;
    let mut message = reference(
        SourceRef {
            scope: "context:parent-turn".into(),
            id: "context".into(),
            version: "revision:1".into(),
        },
        "parent",
        None,
    );
    message.tool_name = Some("html_preview".into());
    manifest(&f, "index-obligation", "parent", &[message], &[], 0, None).await;
    let before = f.marker("index-obligation").await;
    f.sql("CREATE TRIGGER reject_io_enqueue BEFORE INSERT ON html_preview_io_work WHEN NEW.family='witness' BEGIN SELECT RAISE(ABORT,'fixture enqueue failure'); END").await;
    let mut after = String::new();
    let mut imports = None;
    assert!(
        index_quantum(&f.store, &mut after, &mut imports, "\u{10ffff}")
            .await
            .is_err()
    );
    assert_eq!(f.marker("index-obligation").await, before);
    assert!(
        frozen_result::Entity::find_by_id(("index-obligation".to_owned(), 0_i64))
            .one(&f.store.connection)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        work::Entity::find_by_id((
            "witness".to_owned(),
            io_queue_key("index-obligation"),
            0_i64
        ))
        .one(&f.store.connection)
        .await
        .unwrap()
        .is_none()
    );
    f.sql("DROP TRIGGER reject_io_enqueue").await;
    after.clear();
    assert!(
        index_quantum(&f.store, &mut after, &mut imports, "\u{10ffff}")
            .await
            .unwrap()
    );
    assert!(
        frozen_result::Entity::find_by_id(("index-obligation".to_owned(), 0_i64))
            .one(&f.store.connection)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        work::Entity::find_by_id((
            "witness".to_owned(),
            io_queue_key("index-obligation"),
            0_i64
        ))
        .one(&f.store.connection)
        .await
        .unwrap()
        .is_some()
    );
}

#[tokio::test]
async fn io_lost_source_ack_response_recovers_newer_request_after_restart() {
    use super::super::html_preview_io_work::{HtmlPreviewIoFamily, request};
    let f = fixture().await;
    f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
        .await;
    let metadata = r#"{"htmlTool":{"retention":"released","releasedCopies":["x"]}}"#;
    io_copy_source(&f, "lost-ack", metadata).await;
    let tx = f.store.connection.begin().await.unwrap();
    request(&tx, HtmlPreviewIoFamily::Copies, "lost-ack", 0, "ws")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let old = f
        .store
        .claim_html_preview_io_work(0, 60_000)
        .await
        .unwrap()
        .unwrap();
    // Source preparation precedes a producer's newer requested revision.
    let tx = f.store.connection.begin().await.unwrap();
    request(&tx, HtmlPreviewIoFamily::Copies, "lost-ack", 0, "ws")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    // Deliberately lose the response of the actual source+ACK API, rather than
    // pretending that a failed driver response establishes rollback or empty.
    drop(
        f.store
            .finish_html_preview_copy_cleanup_claimed(&old, metadata, "{}")
            .await,
    );
    let restarted = CrudStore::new(f.store.database_connection()).with_maintenance_access();
    assert_eq!(
        artifact::Entity::find_by_id("lost-ack")
            .one(&restarted.connection)
            .await
            .unwrap()
            .unwrap()
            .metadata_json,
        "{}"
    );
    let now = chrono::Utc::now().timestamp_millis();
    let newer = restarted
        .claim_html_preview_io_work(now, 60_000)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(newer.row.pass_revision, old.row.pass_revision);
    assert!(matches!(
        restarted.load_html_preview_io_copies(&newer).await.unwrap(),
        crate::HtmlPreviewIoSource::Obsolete
    ));
    assert!(
        restarted
            .defer_html_preview_io_work(&newer, crate::HtmlPreviewIoOutcome::Done, now)
            .await
            .unwrap()
    );
    assert!(
        restarted
            .claim_html_preview_io_work(now, 60_000)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn io_seed_revision_and_frontier_capture_the_same_committed_request() {
    use super::super::html_preview_io_work::{HtmlPreviewIoFamily, request};
    use pioneer_entity::html_preview_io_work as work;
    let f = fixture().await;
    f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1 WHERE family='seed_witness'").await;
    let metadata = r#"{"htmlTool":{"retention":"released","releasedCopies":["x"]}}"#;
    io_copy_source(&f, "a", metadata).await;
    let claim = f
        .store
        .claim_html_preview_io_work(0, 60_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.row.family, "seed_copies");
    // A repair arrives after claim but before the pass has a frontier.
    io_copy_source(&f, "b", metadata).await;
    let tx = f.store.connection.begin().await.unwrap();
    request(&tx, HtmlPreviewIoFamily::SeedCopies, "", 0, "")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(f.store.html_preview_io_seed_quantum(&claim).await.unwrap());
    let row = work::Entity::find_by_id(("seed_copies".to_owned(), String::new(), 0_i64))
        .one(&f.store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(row.requested_revision, claim.row.requested_revision);
    assert_eq!(row.pass_revision, row.requested_revision);
    assert_eq!(row.upper_id.as_deref(), Some("b"));
    assert_eq!(row.cursor_id.as_deref(), Some("a"));
}

#[tokio::test]
async fn io_business_retry_preserves_cas_class_through_artifact_error_boundary() {
    use super::super::artifact::ArtifactCrudError;
    use super::super::html_preview_io_work::{CasRace, into_database_error};
    for conflict in [true, false] {
        let f = fixture().await;
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let store = &f.store;
        let result: Result<()> = store
            .run_serialized_write(|| {
                let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move {
                    let tx = store.connection.begin().await?;
                    if attempt == 0 {
                        tx.rollback().await?;
                        let error: anyhow::Error = if conflict {
                            CasRace.into()
                        } else {
                            anyhow::anyhow!("unknown outcome")
                        };
                        return Err(ArtifactCrudError::Database {
                            message: "enqueue failed".into(),
                            source: into_database_error(error),
                        }
                        .into());
                    }
                    tx.commit().await?;
                    Ok(())
                }
            })
            .await;
        assert_eq!(result.is_ok(), conflict);
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            if conflict { 2 } else { 1 }
        );
        let tx = f.store.connection.begin().await.unwrap();
        tx.rollback().await.unwrap();
    }
}

#[tokio::test]
async fn retention_pass_finishes_with_a_continuously_growing_artifact_suffix() {
    let f = fixture().await;
    io_copy_source(&f, "a", "{}").await;
    io_copy_source(&f, "m", "{}").await;
    let mut progress = HtmlPreviewRetentionProgress::default();
    // Visit every family once, so each has captured its initial frontier.
    for _ in 0..3 {
        assert!(progress.quantum(&f.store).await.unwrap());
    }
    let mut finished = false;
    for suffix in 0..12 {
        io_copy_source(&f, &format!("z-{suffix:02}"), "{}").await;
        if !progress.quantum(&f.store).await.unwrap() {
            finished = true;
            break;
        }
    }
    assert!(
        finished,
        "new suffix rows must not extend an already captured pass"
    );
}

#[tokio::test]
async fn finite_copy_and_gc_pages_revisit_older_candidates_despite_new_suffix_rows() {
    let f = fixture().await;
    for id in ["a", "m"] {
        io_copy_source(&f, id, "{}").await;
    }
    let upper = f.store.html_preview_artifact_frontier().await.unwrap();
    let (mut after, _) = f
        .store
        .html_preview_released_copies_page_through("", &upper)
        .await
        .unwrap();
    let metadata = r#"{"htmlTool":{"retention":"released","releasedCopies":["old.html"]}}"#;
    f.store
        .connection
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE artifact SET metadata_json=? WHERE id='a'",
            [metadata.into()],
        ))
        .await
        .unwrap();
    let mut finished = false;
    for suffix in 0..4 {
        io_copy_source(&f, &format!("z-{suffix}"), "{}").await;
        let (next, _) = f
            .store
            .html_preview_released_copies_page_through(&after, &upper)
            .await
            .unwrap();
        after = next;
        if after.is_empty() {
            finished = true;
            break;
        }
    }
    assert!(finished);
    let next_upper = f.store.html_preview_artifact_frontier().await.unwrap();
    let (id, cleanup) = f
        .store
        .html_preview_released_copies_page_through("", &next_upper)
        .await
        .unwrap();
    assert_eq!(id, "a");
    assert_eq!(cleanup, Some(("ws".to_owned(), metadata.to_owned())));

    f.sql(r#"INSERT INTO artifact_blob(id,workspace_id,sha256,size_bytes,storage_backend,storage_key,created_at,metadata_json)
        VALUES('a','ws','a',1,'local','a','2020-01-01 00:00:00 +00:00','{}')"#).await;
    let upper = f.store.html_preview_blob_frontier().await.unwrap();
    let cutoff = 1_577_836_800_000_i64;
    let (after, candidates) = f
        .store
        .html_preview_gc_blob_page_through("", cutoff, &upper)
        .await
        .unwrap();
    assert!(candidates.is_empty());
    f.sql(r#"UPDATE artifact_blob SET metadata_json='{"htmlToolTemporary":1}' WHERE id='a'"#)
        .await;
    f.sql("INSERT INTO artifact_blob(id,workspace_id,sha256,size_bytes,storage_backend,storage_key) VALUES('z','ws','z',1,'local','z')").await;
    assert!(
        f.store
            .html_preview_gc_blob_page_through(&after, cutoff, &upper)
            .await
            .unwrap()
            .0
            .is_empty()
    );
    let next_upper = f.store.html_preview_blob_frontier().await.unwrap();
    let (_, candidates) = f
        .store
        .html_preview_gc_blob_page_through("", cutoff, &next_upper)
        .await
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].1.blob_id, "a");
}

#[tokio::test]
async fn blocked_io_recovery_revisits_an_older_row_with_a_growing_suffix() {
    use super::super::html_preview_io_work::{HtmlPreviewIoFamily, request};
    use pioneer_entity::html_preview_io_work as work;
    let f = fixture().await;
    f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
        .await;
    let tx = f.store.connection.begin().await.unwrap();
    for i in 0..17 {
        request(
            &tx,
            HtmlPreviewIoFamily::Copies,
            &format!("copy-{i}"),
            0,
            "ws",
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    f.sql("UPDATE html_preview_io_work SET state='blocked' WHERE family='copies'")
        .await;
    let upper = f.store.html_preview_io_recovery_frontier().await.unwrap();
    let mut after = f
        .store
        .recover_html_preview_io_blocked_through(&Default::default(), &upper)
        .await
        .unwrap()
        .unwrap();
    let first = work::Entity::find()
        .filter(work::Column::Family.eq("copies"))
        .order_by_asc(work::Column::ObjectId)
        .one(&f.store.connection)
        .await
        .unwrap()
        .unwrap();
    f.store
        .connection
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE html_preview_io_work SET state='blocked' WHERE family='copies' AND object_id=?",
            [first.object_id.clone().into()],
        ))
        .await
        .unwrap();
    let mut finished = false;
    for i in 0..4 {
        let tx = f.store.connection.begin().await.unwrap();
        request(
            &tx,
            HtmlPreviewIoFamily::Witness,
            &format!("new-{i}"),
            0,
            "ws",
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        match f
            .store
            .recover_html_preview_io_blocked_through(&after, &upper)
            .await
            .unwrap()
        {
            Some(next) => after = next,
            None => {
                finished = true;
                break;
            }
        }
    }
    assert!(finished);
    let upper = f.store.html_preview_io_recovery_frontier().await.unwrap();
    f.store
        .recover_html_preview_io_blocked_through(&Default::default(), &upper)
        .await
        .unwrap();
    let row = work::Entity::find_by_id((first.family, first.object_id, first.ordinal))
        .one(&f.store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.state, "ready");
}
