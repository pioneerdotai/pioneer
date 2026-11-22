//! PostgreSQL regressions for preview retention and its I/O queue.
use super::*;
use crate::frozen_import::FrozenImportRecord;
use futures_util::FutureExt;
use pioneer_compaction::{SourceRef, frozen::FrozenMessageRef};
use pioneer_crud_artifacts::html_preview::find_frozen_result;
use pioneer_crud_artifacts::html_preview_io::{HtmlPreviewIoLocator, find_work};
use pioneer_postgres_pools::{
    PostgresDatabase, PostgresReadClass, PostgresReadEvent, PostgresReadObserver,
    PostgresWriteClass, PostgresWriteEvent, PostgresWriteExecutor, PostgresWriteObserver,
};
use pioneer_postgres_test::TestDatabase;
use sea_orm::{DatabaseConnection, DbBackend, Statement};
use serde_json::json;
use std::sync::{Arc, Mutex};

// A finite upper bound beyond every ASCII manifest ID in these fixtures.
const TEST_INDEX_FRONTIER: &str = "zzzzzzzzzz";

#[derive(Default)]
struct Routes {
    reads: Mutex<Vec<PostgresReadEvent>>,
    writes: Mutex<Vec<PostgresWriteEvent>>,
    queued: tokio::sync::Notify,
    writer_queued: tokio::sync::Notify,
}
impl PostgresReadObserver for Routes {
    fn observe(&self, event: PostgresReadEvent) {
        if matches!(
            event,
            PostgresReadEvent::AdmissionEnqueued {
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
impl PostgresWriteObserver for Routes {
    fn observe(&self, event: PostgresWriteEvent) {
        if matches!(event, PostgresWriteEvent::Enqueued { queue, .. } if queue.maintenance == 1) {
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
            PostgresReadEvent::OperationFinished {
                class: PostgresReadClass::Interactive,
                ..
            }
        )));
        assert!(writes.iter().all(|e| !matches!(
            e,
            PostgresWriteEvent::Acquired {
                class: PostgresWriteClass::Interactive | PostgresWriteClass::Critical,
                ..
            }
        )));
        (
            reads
                .iter()
                .filter(|e| matches!(e, PostgresReadEvent::OperationFinished { .. }))
                .count(),
            writes
                .iter()
                .filter(|e| matches!(e, PostgresWriteEvent::Acquired { .. }))
                .count(),
        )
    }
}
struct Fixture<'db> {
    store: CompactionStore,
    io_wake: tokio::sync::Notify,
    reader: DatabaseConnection,
    writer: PostgresWriteExecutor,
    routes: Arc<Routes>,
    database: &'db TestDatabase,
}
impl Fixture<'_> {
    async fn reopen_reader(&mut self) {
        self.reader = self.database.connect_read_only().await.unwrap();
        self.store = CompactionStore::for_test(PostgresDatabase::from_executor_with_read_observer(
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
            index_quantum(
                &self.store,
                &self.io_wake,
                after,
                progress,
                TEST_INDEX_FRONTIER
            )
            .await
            .unwrap()
        );
    }
}
async fn fixture(database: &TestDatabase) -> Fixture<'_> {
    let connection = database.connect_with_limit(1).await.unwrap();
    let routes = Arc::new(Routes::default());
    let writer = PostgresWriteExecutor::with_observer(connection, routes.clone());
    let reader = database.connect_read_only().await.unwrap();
    let store = CompactionStore::for_test(PostgresDatabase::from_executor_with_read_observer(
        reader.clone(),
        writer.clone(),
        routes.clone(),
    ))
    .with_maintenance_access();
    let f = Fixture {
        store,
        io_wake: Default::default(),
        reader,
        writer,
        routes,
        database,
    };
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',TRUE,FALSE)",
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
    let payload = pioneer_crud_conversations::events::TurnEventPayload::ItemCompleted(
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
    );
    let prepared = pioneer_crud_execution::event_append::PreparedTurnEvent::prepare(payload)
        .unwrap()
        .preflight_idempotency(&f.store.connection)
        .await
        .unwrap();
    let tx = f.store.connection.begin().await.unwrap();
    pioneer_crud_execution::event_append::append_prepared_event(
        &tx,
        prepared,
        chrono::DateTime::from_timestamp(1, 0)
            .unwrap()
            .fixed_offset(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(f.store.connection.reader_is_read_only().await.unwrap());
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
    f: &Fixture<'_>,
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
    f.store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,next_import,imports_sha256,ready) VALUES($1,'ws',$2,$3,$4,$4,$5,$5,$6,1)",
        [id.into(),owner.into(),hex::encode(digest.finalize()).into(),(messages.len() as i64).into(),(declared as i64).into(),expected.map(str::to_owned).unwrap_or_else(||hex::encode(import_digest(proofs).finalize())).into()]
    )).await.unwrap();
    for (ordinal, message) in messages.iter().enumerate() {
        let json = serde_json::to_string(message).unwrap();
        f.store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES($1,$2,$3,$4)",
            [id.into(),(ordinal as i64).into(),json.clone().into(),(json.len() as i64).into()]
        )).await.unwrap();
    }
    for (ordinal, proof) in proofs.iter().enumerate() {
        let json = serde_json::to_string(proof).unwrap();
        f.store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_thread,source_scope,source_id,source_version,proof_json,bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            [id.into(),(ordinal as i64).into(),(proof.message_ordinal as i64).into(),proof.source_thread.clone().into(),proof.source.scope.clone().into(),proof.source.id.clone().into(),proof.source.version.clone().into(),json.clone().into(),(json.len() as i64).into()]
        )).await.unwrap();
    }
}
async fn seed(
    f: &Fixture<'_>,
    n: usize,
    padding: usize,
    checkpoint: bool,
) -> (Vec<FrozenMessageRef>, Vec<FrozenImportRecord>) {
    let ack = f.store.connection.query_one_raw(Statement::from_string(DbBackend::Postgres,
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
                "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES('cp',$1,$2,$3)",
                "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES('cp-op',$4,0,0,'child',$1,$2,$3)",
            ] {
                let values = if sql.contains("$4") {
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
                        DbBackend::Postgres,
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
    f: &Fixture<'_>,
    targets: &[FrozenMessageRef],
    proofs: &[FrozenImportRecord],
    declared: usize,
    expected: Option<&str>,
) {
    manifest(f, "a", "parent", targets, proofs, declared, expected).await;
    // Exercise import continuation and all publication guards through the

    f.sql(&format!(
        "UPDATE html_preview_frozen_index SET next_ordinal={} WHERE manifest_id='a'",
        targets.len()
    ))
    .await;
}
async fn no_intermediate_writes(f: &Fixture<'_>) {
    f.sql("CREATE OR REPLACE FUNCTION reject_repeat_index_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'unexpected marker insert'; END $$; CREATE TRIGGER reject_repeat_index_insert BEFORE INSERT ON html_preview_frozen_index FOR EACH ROW EXECUTE FUNCTION reject_repeat_index_insert()").await;
    f.sql("CREATE OR REPLACE FUNCTION reject_intermediate_index_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'unexpected RAM-only marker update'; END $$; CREATE TRIGGER reject_intermediate_index_update BEFORE UPDATE ON html_preview_frozen_index FOR EACH ROW WHEN (NEW.complete=0 AND NEW.unknown=0) EXECUTE FUNCTION reject_intermediate_index_update()").await;
}

#[tokio::test]
async fn imports_consume_each_row_and_byte_bounded_page_once_in_order() {
    for (n, padding, page_len) in [(130, 0, 128), (3, 140_000, 1)] {
        pioneer_postgres_test::with_database(async |database| {
            let f = fixture(database).await;
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
            assert!(
                wake.wait(None).now_or_never().is_some(),
                "verified index completion must revisit retirement"
            );
            assert!(progress.is_none());
            assert_eq!(after, "a");
            assert_eq!(f.marker("a").await.complete, 1);
            Ok(())
        })
        .await
        .unwrap();
    }
}

async fn finish_member(
    f: &Fixture<'_>,
    after: &mut String,
    progress: &mut Option<PreviewImportIndex>,
) {
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
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn invalid_count_digest_binding_and_target_remain_retaining_and_advance() {
    for fault in ["count", "digest", "binding", "target", "gap", "length"] {
        pioneer_postgres_test::with_database(async |database| {
            let f = fixture(database).await;
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
                // The current schema rejects a physical proof with a false byte count.
                let error = f.store.connection.execute_unprepared(
                    "UPDATE compaction_frozen_import_data SET bytes=bytes+1 WHERE manifest_id='a' AND ordinal=0",
                ).await.expect_err("frozen import byte count must be enforced");
                assert!(error.to_string().contains("compaction_frozen_import_data_check"));
                return Ok(());
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
            Ok(())
        })
        .await
        .unwrap();
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
        pioneer_postgres_test::with_database(async |database| {
            let f = fixture(database).await;
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
            Ok(())
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn writer_failure_and_restart_replay_the_entire_stream() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
        let (targets, proofs) = seed(&f, 2, 0, false).await;
        start_imports(&f, &targets, &proofs, 2, None).await;
        let mut after = String::new();
        let mut progress = None;
        f.step(&mut after, &mut progress).await;
        f.sql("CREATE OR REPLACE FUNCTION fail_index_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture writer failure'; END $$; CREATE TRIGGER fail_index_commit BEFORE UPDATE ON html_preview_frozen_index FOR EACH ROW EXECUTE FUNCTION fail_index_commit()").await;
        let error = index_quantum(&f.store, &f.io_wake, &mut after, &mut progress, TEST_INDEX_FRONTIER)
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<sea_orm::DbErr>().is_some());
        assert!(progress.is_none());
        assert!(after.is_empty());
        assert_eq!(f.marker("a").await.complete, 0);
        f.sql("DROP TRIGGER fail_index_commit ON html_preview_frozen_index").await;
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn walker_db_error_and_cancelled_read_discard_partial_ram_before_retry() {
    for cancel in [false, true] {
        pioneer_postgres_test::with_database(async |database| {
            let mut f = fixture(database).await;
            let (targets, proofs) = seed(&f, 2, 0, true).await;
            start_imports(&f, &targets, &proofs, 2, None).await;
            let mut after = String::new();
            let mut progress = None;
            f.step(&mut after, &mut progress).await; // Pending walker, buffered proof 1.
            if cancel {
                let held = f.store.connection.begin_read().await.unwrap();
                {
                    let step = index_quantum(
                        &f.store,
                        &f.io_wake,
                        &mut after,
                        &mut progress,
                        TEST_INDEX_FRONTIER,
                    );
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
                    PostgresReadEvent::AdmissionCancelled { queue_depth: 0, .. }
                )));
            } else {
                // Close only this isolated physical reader to fail the graph read
                // after it has changed RAM. Reopen on the same temporary fixture.
                f.reader.clone().close().await.unwrap();
                let error = index_quantum(
                    &f.store,
                    &f.io_wake,
                    &mut after,
                    &mut progress,
                    TEST_INDEX_FRONTIER,
                )
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
            Ok(())
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn full_member_without_the_wanted_leaf_cannot_publish_safe() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn concurrent_progress_owners_do_not_mix_versions_or_publish_twice() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
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
        f.sql("CREATE OR REPLACE FUNCTION reject_second_publication() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'duplicate publication'; END $$; CREATE TRIGGER reject_second_publication BEFORE UPDATE ON html_preview_frozen_index FOR EACH ROW WHEN (OLD.complete=1 AND NEW.complete=1) EXECUTE FUNCTION reject_second_publication()").await;
        f.step(&mut right_after, &mut right).await;
        assert!(left.is_none() && right.is_none());
        assert_eq!((left_after.as_str(), right_after.as_str()), ("a", "a"));
        assert_eq!(f.marker("a").await.complete, 1);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelled_terminal_writer_discards_digest_and_replays_without_publication() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
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
            let step = index_quantum(
                &f.store,
                &f.io_wake,
                &mut after,
                &mut progress,
                TEST_INDEX_FRONTIER,
            );
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
            PostgresWriteEvent::Cancelled {
                class: PostgresWriteClass::Maintenance,
                ..
            }
        )));
        assert_eq!(f.marker("a").await.complete, 0);
        f.step(&mut after, &mut progress).await;
        assert_eq!(progress.as_ref().unwrap().next, 1);
        f.step(&mut after, &mut progress).await;
        assert_eq!(f.marker("a").await.complete, 1);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn complete_gate_selects_only_live_ready_work_and_unknown_does_not_starve() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
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
                !index_quantum(&f.store, &f.io_wake, &mut after, &mut progress, TEST_INDEX_FRONTIER)
                    .await
                    .unwrap()
            );
            assert!(after.is_empty());
            assert_eq!(f.routes.counts(), (1, 0));
        }
        assert_eq!(f.marker("c-unready").await.complete, 0);
        assert_eq!(f.marker("e-work").await.complete, 1);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn work_cursor_wrap_and_restart_find_new_and_newly_ready_headers_behind_cursor() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
        manifest(&f, "m", "parent", &[], &[], 0, None).await;
        f.sql("INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,ready) VALUES('b','ws','parent','e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',0,0)").await;

        let mut after = String::new();
        let mut progress = None;
        f.step(&mut after, &mut progress).await;
        assert_eq!(after, "m");
        manifest(&f, "a", "parent", &[], &[], 0, None).await;
        f.sql("UPDATE compaction_frozen_history SET ready=1 WHERE id='b'")
            .await;
        assert!(
            !index_quantum(&f.store, &f.io_wake, &mut after, &mut progress, TEST_INDEX_FRONTIER)
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
                !index_quantum(&f.store, &f.io_wake, &mut after, &mut progress, TEST_INDEX_FRONTIER)
                    .await
                    .unwrap()
            );
            assert_eq!(
                f.routes.counts(),
                (1, 0),
                "quiet pass skips all completed headers"
            );
        }
        // Deletion and expiry triggers stop retaining work for dead manifests.
        manifest(&f, "x-delete", "parent", &[], &[], 0, None).await;
        manifest(&f, "y-expire", "parent", &[], &[], 0, None).await;
        f.sql("DELETE FROM compaction_frozen_history WHERE id='x-delete'")
            .await;
        f.sql("UPDATE compaction_frozen_history SET expired=1 WHERE id='y-expire'")
            .await;
        assert!(
            !index_quantum(&f.store, &f.io_wake, &mut after, &mut progress, TEST_INDEX_FRONTIER)
                .await
                .unwrap()
        );
        assert_eq!(f.marker("x-delete").await.expired, 1);
        assert_eq!(f.marker("y-expire").await.expired, 1);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn failed_index_read_or_completion_remains_discoverable_after_wrap() {
    for reader_failure in [false, true] {
        pioneer_postgres_test::with_database(async |database| {
            let f = fixture(database).await;
            manifest(&f, "a", "parent", &[], &[], 0, None).await;

            let repair = if reader_failure {
                // Fail the physical reader's bounded page query after the index
                // header advanced the keyset. Restore only this fixture's view.
                f.sql("ALTER VIEW compaction_frozen_message RENAME TO preview_fixture_message").await;
                "ALTER VIEW preview_fixture_message RENAME TO compaction_frozen_message".to_owned()
            } else {
                f.sql("CREATE OR REPLACE FUNCTION fail_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture completion failure'; END $$; CREATE TRIGGER fail_completion BEFORE UPDATE ON html_preview_frozen_index FOR EACH ROW WHEN (NEW.complete=1) EXECUTE FUNCTION fail_completion()").await;
                "DROP TRIGGER fail_completion ON html_preview_frozen_index".into()
            };
            let mut after = String::new();
            let mut progress = None;
            let error = index_quantum(&f.store, &f.io_wake, &mut after, &mut progress, TEST_INDEX_FRONTIER)
                .await
                .unwrap_err();
            assert!(error.downcast_ref::<sea_orm::DbErr>().is_some());
            assert_eq!(f.marker("a").await.complete, 0);
            f.sql(&repair).await;
            assert!(
                !index_quantum(&f.store, &f.io_wake, &mut after, &mut progress, TEST_INDEX_FRONTIER)
                    .await
                    .unwrap()
            );
            // No domain wake/restart: the same cursor wraps to the retained row.
            f.step(&mut after, &mut progress).await;
            assert_eq!(f.marker("a").await.complete, 1);
            Ok(())
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn io_index_enqueue_failure_rolls_back_result_and_progress() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
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
        f.sql("CREATE OR REPLACE FUNCTION reject_io_enqueue() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture enqueue failure'; END $$; CREATE TRIGGER reject_io_enqueue BEFORE INSERT ON html_preview_io_work FOR EACH ROW WHEN (NEW.family='witness') EXECUTE FUNCTION reject_io_enqueue()").await;
        let mut after = String::new();
        let mut imports = None;
        assert!(
            index_quantum(&f.store, &f.io_wake, &mut after, &mut imports, TEST_INDEX_FRONTIER)
                .await
                .is_err()
        );
        assert_eq!(f.marker("index-obligation").await, before);
        assert!(
            find_frozen_result("index-obligation".to_owned(), 0_i64)
                .one(&f.store.connection)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            find_work(
                "witness".to_owned(),
                HtmlPreviewIoLocator::new("index-obligation").key().to_owned(),
                0_i64
            )
            .one(&f.store.connection)
            .await
            .unwrap()
            .is_none()
        );
        f.sql("DROP TRIGGER reject_io_enqueue ON html_preview_io_work").await;
        after.clear();
        assert!(
            index_quantum(&f.store, &f.io_wake, &mut after, &mut imports, TEST_INDEX_FRONTIER)
                .await
                .unwrap()
        );
        assert!(
            find_frozen_result("index-obligation".to_owned(), 0_i64)
                .one(&f.store.connection)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            find_work(
                "witness".to_owned(),
                HtmlPreviewIoLocator::new("index-obligation").key().to_owned(),
                0_i64
            )
            .one(&f.store.connection)
            .await
            .unwrap()
            .is_some()
        );
        Ok(())
    })
    .await
    .unwrap();
}
