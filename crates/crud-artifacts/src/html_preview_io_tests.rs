//! PostgreSQL regressions for the durable HTML preview I/O queue.
use super::*;
use crate::html_preview::find_frozen_result;
use crate::html_preview_receipts::{HtmlPreviewArchiveWitness, HtmlPreviewReceipts};
use futures_util::FutureExt;
use pioneer_compaction::{SourceRef, frozen::FrozenMessageRef};
use pioneer_entity::{
    artifact, compaction_frozen_history as history, html_preview_frozen_result as frozen_result,
};
use pioneer_postgres_test::TestDatabase;
use pioneer_sqlite::{
    SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteClass, SqliteWriteEvent,
    SqliteWriteExecutor, SqliteWriteObserver,
};
use sea_orm::{DbBackend, Statement};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Routes {
    reads: Mutex<Vec<SqliteReadEvent>>,
    writes: Mutex<Vec<SqliteWriteEvent>>,
}
impl SqliteReadObserver for Routes {
    fn observe(&self, event: SqliteReadEvent) {
        self.reads.lock().unwrap().push(event);
    }
}
impl SqliteWriteObserver for Routes {
    fn observe(&self, event: SqliteWriteEvent) {
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

struct Fixture<'a> {
    db: SqliteDatabase,
    writer: SqliteWriteExecutor,
    routes: Arc<Routes>,
    database: &'a TestDatabase,
    wake: tokio::sync::Notify,
}

impl Fixture<'_> {
    fn queue(&self) -> HtmlPreviewIoQueue<'_> {
        HtmlPreviewIoQueue::new(&self.db)
    }

    fn receipts(&self) -> HtmlPreviewReceipts<'_> {
        HtmlPreviewReceipts::new(&self.db, &self.wake)
    }

    async fn request_repair(&self, families: &[HtmlPreviewIoFamily]) {
        let transaction = self.db.begin().await.unwrap();
        self.queue()
            .request_html_preview_io_resync(
                &transaction,
                &HtmlPreviewIoLocator::new("ws"),
                families,
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();
    }

    async fn reopen_reader(&mut self) {
        self.db = SqliteDatabase::from_executor_with_read_observer(
            self.database.connect_read_only().await.unwrap(),
            self.writer.clone(),
            self.routes.clone(),
        )
        .maintenance();
    }

    async fn sql(&self, sql: &str) {
        self.db.execute_unprepared(sql).await.unwrap();
    }
}

async fn fixture(database: &TestDatabase) -> Fixture<'_> {
    let connection = database.connect_with_limit(1).await.unwrap();
    let routes = Arc::new(Routes::default());
    let writer = SqliteWriteExecutor::with_observer(connection, routes.clone());
    let reader = database.connect_read_only().await.unwrap();
    let db =
        SqliteDatabase::from_executor_with_read_observer(reader, writer.clone(), routes.clone())
            .maintenance();
    assert!(db.reader_query_only_enabled().await.unwrap());
    let f = Fixture {
        db,
        writer,
        routes,
        database,
        wake: Default::default(),
    };
    f.sql("INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',TRUE,FALSE);
        INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class)
            VALUES('parent','ws','','agent','m','p','active','user','workspace');
        INSERT INTO turn(id,thread_id,status,turn_kind,origin)
            VALUES('parent-turn','parent','completed','conversation','user');").await;
    f
}

async fn repair_quantum(queue: &HtmlPreviewIoQueue<'_>) -> Result<bool> {
    let Some(claim) = queue
        .claim_html_preview_io_work_kind(chrono::Utc::now().timestamp_millis(), 300_000, true)
        .await?
    else {
        return Ok(false);
    };
    queue.html_preview_io_seed_quantum(&claim).await
}

async fn request(
    db: &impl ConnectionTrait,
    family: HtmlPreviewIoFamily,
    id: &str,
    ordinal: i64,
    workspace: &str,
) -> Result<()> {
    request_at(
        db,
        family,
        &HtmlPreviewIoLocator::new(id),
        ordinal,
        workspace,
        0,
    )
    .await
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
    f.db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,next_import,imports_sha256,ready) VALUES($1,'ws',$2,$3,$4,$4,$5,$5,$6,1)",
        [id.into(),owner.into(),hex::encode(digest.finalize()).into(),(messages.len() as i64).into(),(declared as i64).into(),expected.map(str::to_owned).unwrap_or_else(||hex::encode(import_digest(proofs).finalize())).into()]
    )).await.unwrap();
    for (ordinal, message) in messages.iter().enumerate() {
        let json = serde_json::to_string(message).unwrap();
        f.db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES($1,$2,$3,$4)",
            [id.into(),(ordinal as i64).into(),json.clone().into(),(json.len() as i64).into()]
        )).await.unwrap();
    }
    for (ordinal, proof) in proofs.iter().enumerate() {
        let json = serde_json::to_string(proof).unwrap();
        f.db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_thread,source_scope,source_id,source_version,proof_json,bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            [id.into(),(ordinal as i64).into(),(proof.message_ordinal as i64).into(),proof.source_thread.clone().into(),proof.source.scope.clone().into(),proof.source.id.clone().into(),proof.source.version.clone().into(),json.clone().into(),(json.len() as i64).into()]
        )).await.unwrap();
    }
}

fn io_queue_key(id: &str) -> String {
    HtmlPreviewIoLocator::new(id).key().to_owned()
}

async fn io_copy_source(f: &Fixture<'_>, id: &str, metadata: &str) {
    f.db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO artifact(id,workspace_id,display_name,kind,mime_type,status,created_by_kind,created_by_actor_id,created_at,updated_at,metadata_json) VALUES($1,'ws','preview','image','image/png','deleted','tool','html_preview',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,$2)",
        [id.into(),metadata.into()])).await.unwrap();
}

#[tokio::test]
async fn io_scoped_repair_completion_is_durable_across_restart() {
    pioneer_postgres_test::with_database(async |database| {
        let mut f = fixture(database).await;
        f.request_repair(&[HtmlPreviewIoFamily::Witness, HtmlPreviewIoFamily::Copies])
            .await;
        for _ in 0..256 {
            if !repair_quantum(&f.queue()).await.unwrap() {
                break;
            }
        }
        for family in ["seed_witness", "seed_copies"] {
            let row = find_work(family.to_owned(), io_queue_key("ws"), 0)
                .one(&f.db)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.state, "done");
        }
        f.reopen_reader().await;
        f.routes.reset();
        assert!(!repair_quantum(&f.queue()).await.unwrap());
        assert_eq!(
            f.routes.counts().1,
            0,
            "completed seeds do not reopen at startup"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_resync_keeps_frontier_and_late_producer() {
    pioneer_postgres_test::with_database(async |database| {
        use pioneer_entity::html_preview_io_work as work;
        let f = fixture(database).await;
        let metadata = r#"{"htmlTool":{"retention":"released","releasedCopies":["x"]}}"#;
        for id in ["a", "b", "c"] {
            io_copy_source(&f, id, metadata).await;
        }
        f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1 WHERE family='seed_witness'").await;
        f.request_repair(&[HtmlPreviewIoFamily::Copies]).await;
        assert!(
            repair_quantum(&f.queue())
                .await
                .unwrap()
        );
        let first = find_work("seed_copies".to_owned(), io_queue_key("ws"), 0_i64)
            .one(&f.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.cursor_id.as_deref(), Some("a"));
        io_copy_source(&f, "0-behind", metadata).await;
        let tx = f.db.begin().await.unwrap();
        request(&tx, HtmlPreviewIoFamily::Copies, "0-behind", 0, "ws")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        for expected in ["b", "c"] {
            let tx = f.db.begin().await.unwrap();
            f.queue().request_html_preview_io_resync(&tx, &HtmlPreviewIoLocator::new("ws"), &[HtmlPreviewIoFamily::Copies])
                .await
                .unwrap();
            tx.commit().await.unwrap();
            assert!(
                repair_quantum(&f.queue())
                    .await
                    .unwrap()
            );
            let row = find_work("seed_copies".to_owned(), io_queue_key("ws"), 0_i64)
                .one(&f.db)
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
            find_work("copies".to_owned(), io_queue_key("0-behind"), 0_i64)
                .one(&f.db)
                .await
                .unwrap()
                .is_some()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_scoped_repair_only_enqueues_real_copies() {
    pioneer_postgres_test::with_database(async |database| {
        use pioneer_entity::html_preview_io_work as work;
        let f = fixture(database).await;
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
        f.request_repair(&[HtmlPreviewIoFamily::Copies]).await;
        for _ in 0..16 {
            if !repair_quantum(&f.queue()).await.unwrap() {
                break;
            }
        }
        assert!(
            find_work("copies".to_owned(), io_queue_key("empty"), 0_i64)
                .one(&f.db)
                .await
                .unwrap()
                .is_none()
        );
        let claim = f
            .queue()
            .claim_html_preview_io_work(chrono::Utc::now().timestamp_millis(), 60_000)
            .await
            .unwrap()
            .unwrap();
        let HtmlPreviewIoSource::Ready((_workspace, metadata)) = f
            .receipts()
            .load_html_preview_io_copies(&claim)
            .await
            .unwrap()
        else {
            panic!("real copies")
        };
        assert!(!metadata.is_empty());
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_poison_quarantine_does_not_block_later_work_and_unknown_outcome_is_durable() {
    pioneer_postgres_test::with_database(async |database| {
        use pioneer_entity::html_preview_io_work as work;
        let f = fixture(database).await;
        f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
            .await;
        let tx = f.db.begin().await.unwrap();
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
            .exec(&f.db)
            .await
            .unwrap();
        let claim = f
            .queue()
            .claim_html_preview_io_work(0, 10)
            .await
            .unwrap()
            .unwrap();
        assert!(
            f.queue()
                .defer_html_preview_io_work(&claim, HtmlPreviewIoOutcome::Quarantine, 0)
                .await
                .unwrap()
        );
        let later = f
            .queue()
            .claim_html_preview_io_work(0, 10)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(later.source_id(), "b-later");
        // Discard the successful claim response as a transport could: durable row
        // must remain recoverable; no empty/finished marker may be inferred.
        drop(later);
        let takeover = f
            .queue()
            .claim_html_preview_io_work(10, 10)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(takeover.source_id(), "b-later");
        let poison = find_work("witness".to_owned(), io_queue_key("a-poison"), 0_i64)
            .one(&f.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(poison.state, "quarantine");
        assert!(poison.due_at.is_none());
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_witness_zero_cas_corrupt_expired_and_unavailable_are_distinct() {
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
        pioneer_postgres_test::with_database(async |database| {
            let f = fixture(database).await;
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
            .exec_without_returning(&f.db)
            .await
            .unwrap();
            if mode == "expired" {
                f.sql("UPDATE compaction_frozen_history SET expired=1 WHERE id='w'")
                    .await;
            }
            if mode == "body-restored" {
                f.sql("INSERT INTO compaction_source_revision(source_id,turn_id,revision,present,capture_order) VALUES('missing-source','parent-turn',1,1,9999)").await;
            }
            let tx = f.db.begin().await.unwrap();
            request(&tx, HtmlPreviewIoFamily::Witness, "w", 0, "ws")
                .await
                .unwrap();
            tx.commit().await.unwrap();
            let claim = f.queue().claim_html_preview_io_work(0, 60_000)
                .await
                .unwrap()
                .unwrap();
            let outcome = f.receipts().load_html_preview_io_witness(&claim).await.unwrap();
            match mode {
                "corrupt" | "empty-unit" | "invalid-digest" => {
                    assert!(matches!(outcome, HtmlPreviewIoSource::Quarantine))
                }
                "expired" => assert!(matches!(outcome, HtmlPreviewIoSource::Obsolete)),
                _ => assert!(matches!(outcome, HtmlPreviewIoSource::Retry)),
            }
            if semantic_invalid {
                assert!(
                    f.queue().defer_html_preview_io_work(&claim, HtmlPreviewIoOutcome::Quarantine, 0)
                        .await
                        .unwrap()
                );
                let poison = find_work("witness".to_owned(), io_queue_key("w"), 0_i64)
                    .one(&f.db)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(poison.state, "quarantine");
                assert!(poison.due_at.is_none());
                let retained = find_frozen_result("w".to_owned(), 0_i64)
                    .one(&f.db)
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
                .exec_without_returning(&f.db)
                .await
                .unwrap();
                let tx = f.db.begin().await.unwrap();
                request(&tx, HtmlPreviewIoFamily::Witness, "w", 1, "ws")
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
                let healthy = f.queue().claim_html_preview_io_work(0, 60_000)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(healthy.row.ordinal, 1);
                assert!(matches!(
                    f.receipts().load_html_preview_io_witness(&healthy)
                        .await
                        .unwrap(),
                    HtmlPreviewIoSource::Ready(_)
                ));
                return Ok(());
            }
            if mode == "body-restored" {
                // Pending work remains addressably timed, and restores without
                // changing its source identity/revision or requiring a resync.
                assert!(
                    f.queue().defer_html_preview_io_work(&claim, HtmlPreviewIoOutcome::Retry, 0)
                        .await
                        .unwrap()
                );
                let pending = find_work("witness".to_owned(), io_queue_key("w"), 0_i64)
                    .one(&f.db)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(pending.state, "timed");
                assert_eq!(pending.due_at, Some(60_000));
                assert!(
                    f.queue().claim_html_preview_io_work(59_999, 60_000)
                        .await
                        .unwrap()
                        .is_none()
                );
                let claim = f.queue().claim_html_preview_io_work(60_000, 60_000)
                    .await
                    .unwrap()
                    .unwrap();
                f.sql("INSERT INTO turn_llm_context(id,turn_id,sequence,source,tool_name,payload,output_policy_snapshot,created_at) VALUES('missing-source','parent-turn',999,'tool_result_v2','html_preview','{}','{}',CURRENT_TIMESTAMP)").await;
                // Historical restoration preserves its original revision identity.
                f.sql("UPDATE compaction_source_revision SET revision=1,present=1 WHERE source_id='missing-source'").await;
                let HtmlPreviewIoSource::Ready(witness) =
                    f.receipts().load_html_preview_io_witness(&claim).await.unwrap()
                else {
                    panic!("restored body must become ready");
                };
                let hash = "a".repeat(64);
                let database = f.db.clone();
                let mut wake = database.work_signal("maintenance.html").subscribe();
                wake.begin_pass(&database).await.unwrap();
                f.receipts().record_html_preview_archive_witness(&witness, &hash)
                    .await
                    .unwrap();
                assert!(
                    wake.wait(None).now_or_never().is_some(),
                    "new witness must revisit retirement"
                );
                wake.begin_pass(&database).await.unwrap();
                let tx = f.db.begin().await.unwrap();
                request(&tx, HtmlPreviewIoFamily::Witness, "w", 0, "ws")
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
                let repeated = f.queue().claim_html_preview_io_work(0, 60_000)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    f.receipts().record_html_preview_archive_witness_claimed(&repeated, &witness, &hash)
                        .await
                        .unwrap(),
                    "zero-row publication requires verified reread before ACK"
                );
                assert!(
                    wake.wait(None).now_or_never().is_none(),
                    "verified replay ACK must stay quiet"
                );
                assert!(
                    find_work("witness".to_owned(), io_queue_key("w"), 0_i64)
                        .one(&f.db)
                        .await
                        .unwrap()
                        .is_none()
                );
                return Ok(());
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
                !f.receipts().record_html_preview_archive_witness_claimed(&claim, &witness, &"a".repeat(64))
                    .await
                    .unwrap(),
                "refused source cannot ACK"
            );
            assert!(
                f.receipts().record_html_preview_archive_witness(&witness, &"a".repeat(64))
                    .await
                    .is_err(),
                "legacy callback cannot report zero-row CAS success"
            );
            assert!(
                find_work("witness".to_owned(), io_queue_key("w"), 0_i64)
                    .one(&f.db)
                    .await
                    .unwrap()
                    .is_some()
            );
            if matches!(mode, "unavailable" | "missing-header") {
                assert!(
                    f.queue().defer_html_preview_io_work(&claim, HtmlPreviewIoOutcome::Retry, 0)
                        .await
                        .unwrap()
                );
                let pending = find_work("witness".to_owned(), io_queue_key("w"), 0_i64)
                    .one(&f.db)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(pending.state, "timed");
                assert_eq!(pending.due_at, Some(60_000));
            }
            Ok(())
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn io_repair_updates_workspace_and_reopens_quarantine_without_losing_active_claim() {
    pioneer_postgres_test::with_database(async |database| {
        use pioneer_entity::html_preview_io_work as work;
        let f = fixture(database).await;
        f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
            .await;
        let tx = f.db.begin().await.unwrap();
        request(&tx, HtmlPreviewIoFamily::Copies, "repair", 0, "old")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let old = f
            .queue()
            .claim_html_preview_io_work(0, 10)
            .await
            .unwrap()
            .unwrap();
        let tx = f.db.begin().await.unwrap();
        request(&tx, HtmlPreviewIoFamily::Copies, "repair", 0, "ws")
            .await
            .unwrap();
        assert!(ack(&tx, &old).await.unwrap());
        tx.commit().await.unwrap();
        let current = find_work("copies".to_owned(), io_queue_key("repair"), 0_i64)
            .one(&f.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.workspace_id, "ws");
        assert_eq!(current.state, "ready");
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_copy_cleanup_refuses_case_insensitive_match_after_cooperative_resync() {
    use pioneer_entity::html_preview_io_work as work;
    for claimed in [true, false] {
        pioneer_postgres_test::with_database(async |database| {
            let f = fixture(database).await;
            // Exercise the byte-exact source fence with a case-insensitive
            // PostgreSQL column collation while preserving the migrated table.
            f.sql("CREATE COLLATION preview_test_nocase (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
                   ALTER TABLE artifact ALTER COLUMN metadata_json TYPE text COLLATE preview_test_nocase").await;
            f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
                .await;

            let previous = r#"{"htmlTool":{"retention":"released","releasedCopies":["A.html"]}}"#;
            let repaired = r#"{"htmlTool":{"retention":"released","releasedCopies":["a.html"]}}"#;
            let cleaned = r#"{"htmlTool":{"retention":"released","releasedCopies":[]}}"#;
            io_copy_source(&f, "nocase", previous).await;
            let tx = f.db.begin().await.unwrap();
            request(&tx, HtmlPreviewIoFamily::Copies, "nocase", 0, "ws")
                .await
                .unwrap();
            tx.commit().await.unwrap();
            let now = chrono::Utc::now().timestamp_millis();
            let old = f.queue().claim_html_preview_io_work(now, 300_000)
                .await
                .unwrap()
                .unwrap();
            let HtmlPreviewIoSource::Ready((workspace, prepared_from)) =
                f.receipts().load_html_preview_io_copies(&old).await.unwrap()
            else {
                panic!("old source must be ready")
            };
            assert_eq!(workspace, "ws");
            assert_eq!(prepared_from, previous);
            // Cleanup preparation has finished from A.html before a cooperative
            // repair publishes a.html and its durable scoped resync atomically.
            let prepared_cleanup = cleaned.to_owned();
            let scope = HtmlPreviewIoLocator::new("ws");
            let tx = f.db.begin().await.unwrap();
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
            f.queue().request_html_preview_io_resync(&tx, &scope, &[HtmlPreviewIoFamily::Copies])
                .await
                .unwrap();
            tx.commit().await.unwrap();
            f.wake.notify_one();
            f.db.work_signal("maintenance.html").notify();

            // This is the actual old SQL source fence, with column collation (no
            // explicit COLLATE override). A binary-only fixture cannot pass here.
            let sql_equal = artifact::Entity::find_by_id("nocase")
                .filter(artifact::Column::CurrentVersionId.is_null())
                .filter(artifact::Column::MetadataJson.eq(&prepared_from))
                .one(&f.db)
                .await
                .unwrap()
                .expect("ordinary SQL '=' must falsely equate A.html and a.html");
            assert_eq!(sql_equal.metadata_json, repaired);
            assert_ne!(sql_equal.metadata_json, prepared_from);
            let scope_key = ("seed_copies".to_owned(), scope.key().to_owned(), 0_i64);
            assert!(
                find_work(&scope_key.0, &scope_key.1, scope_key.2)
                    .one(&f.db)
                    .await
                    .unwrap()
                    .is_some()
            );
            // Process this scoped pass before the stale callback: its newer point
            // request must survive as well as the repaired source metadata.
            for _ in 0..2 {
                let seed = f.queue().claim_html_preview_io_work(chrono::Utc::now().timestamp_millis(), 300_000)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(seed.row.family, "seed_copies");
                assert!(f.queue().html_preview_io_seed_quantum(&seed).await.unwrap());
            }
            let key = ("copies".to_owned(), io_queue_key("nocase"), 0_i64);
            let renewed = find_work(&key.0, &key.1, key.2)
                .one(&f.db)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(renewed.requested_revision, old.row.requested_revision);
            assert_eq!(renewed.claim_token, old.row.claim_token);
            assert_eq!(renewed.pass_revision, old.row.pass_revision);
            if claimed {
                assert!(
                    !f.receipts().finish_html_preview_copy_cleanup_claimed(
                            &old,
                            &prepared_from,
                            &prepared_cleanup
                        )
                        .await
                        .unwrap()
                );
            } else {
                assert!(
                    f.receipts().finish_html_preview_copy_cleanup("nocase", &prepared_from, &prepared_cleanup)
                        .await
                        .is_err()
                );
            }
            let preserved = find_work(&key.0, &key.1, key.2)
                .one(&f.db)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                preserved, renewed,
                "refusal must neither ACK nor change the durable obligation"
            );
            assert_eq!(
                artifact::Entity::find_by_id("nocase")
                    .one(&f.db)
                    .await
                    .unwrap()
                    .unwrap()
                    .metadata_json,
                repaired
            );

            assert!(
                !f.queue().defer_html_preview_io_work(&old, HtmlPreviewIoOutcome::Retry, now)
                    .await
                    .unwrap()
            );
            let next = f.queue().claim_html_preview_io_work(chrono::Utc::now().timestamp_millis() + 60_000, 300_000)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(next.row.family, "copies");
            let HtmlPreviewIoSource::Ready((workspace, previous)) =
                f.receipts().load_html_preview_io_copies(&next).await.unwrap()
            else {
                panic!("repaired copies must remain discoverable")
            };
            assert_eq!(workspace, "ws");
            assert_eq!(previous, repaired);
            assert!(
                f.receipts().finish_html_preview_copy_cleanup_claimed(&next, &previous, cleaned)
                    .await
                    .unwrap()
            );
            assert!(
                find_work(&key.0, &key.1, key.2)
                    .one(&f.db)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                artifact::Entity::find_by_id("nocase")
                    .one(&f.db)
                    .await
                    .unwrap()
                    .unwrap()
                    .metadata_json,
                cleaned
            );
            Ok(())
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn io_scoped_repairs_do_not_reclaim_active_leases() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
        f.request_repair(&[HtmlPreviewIoFamily::Witness, HtmlPreviewIoFamily::Copies])
            .await;
        let now = chrono::Utc::now().timestamp_millis();
        let first = f
            .queue()
            .claim_html_preview_io_work(now, 300_000)
            .await
            .unwrap()
            .unwrap();
        let second = f
            .queue()
            .claim_html_preview_io_work(now, 300_000)
            .await
            .unwrap()
            .unwrap();
        assert!(first.row.family.starts_with("seed_") && second.row.family.starts_with("seed_"));
        f.routes.reset();
        assert!(!repair_quantum(&f.queue()).await.unwrap());
        assert_eq!(
            f.routes.counts().1,
            0,
            "occupied durable seeds do not cause startup claim writes"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_long_source_locator_seed_and_claim_are_lossless_and_collision_checked() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
        let id = "legacy-source-".repeat(1024);
        let metadata = r#"{"htmlTool":{"retention":"released","releasedCopies":["x"]}}"#;
        io_copy_source(&f, &id, metadata).await;
        f.request_repair(&[HtmlPreviewIoFamily::Copies]).await;
        for _ in 0..16 {
            if !repair_quantum(&f.queue()).await.unwrap() {
                break;
            }
        }
        let claim = f.queue().claim_html_preview_io_work(chrono::Utc::now().timestamp_millis(), 60_000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claim.row.object_id.len(), 64);
        assert_eq!(claim.source_id(), id);
        assert!(matches!(
            f.receipts().load_html_preview_io_copies(&claim).await.unwrap(),
            HtmlPreviewIoSource::Ready(_)
        ));
        let tx = f.db.begin().await.unwrap();
        request(&tx, HtmlPreviewIoFamily::Copies, &id, 0, "ws")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(
            f.receipts().finish_html_preview_copy_cleanup_claimed(&claim, metadata, "{}")
                .await
                .unwrap()
        );
        let next = f.queue().claim_html_preview_io_work(chrono::Utc::now().timestamp_millis(), 60_000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            next.source_id(),
            id,
            "new request must preserve full point locator"
        );
        let tx = f.db.begin().await.unwrap();
        // Emulate a collision by changing the stored full identity while
        // leaving the hashed queue key and all other admission facts intact.
        tx.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE html_preview_io_work SET cursor_id = $1 WHERE family='copies' AND object_id=$2 AND ordinal=0",
            ["different source".into(), claim.row.object_id.into()],
        ))
        .await
        .unwrap();
        let locator = HtmlPreviewIoLocator::new(&id);
        assert!(
            request_prepared(&tx, HtmlPreviewIoFamily::Copies, &locator, 0, "ws")
                .await
                .is_err()
        );
        tx.rollback().await.unwrap();
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_witness_repair_ignores_verified_and_expired_history() {
    pioneer_postgres_test::with_database(async |database| {
        use pioneer_entity::html_preview_io_work as work;
        let f = fixture(database).await;
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
            .exec_without_returning(&f.db)
            .await
            .unwrap();
            if expired {
                history::Entity::update_many()
                    .col_expr(history::Column::Expired, Expr::val(1))
                    .filter(history::Column::Id.eq(id))
                    .exec(&f.db)
                    .await
                    .unwrap();
            }
        }
        f.request_repair(&[HtmlPreviewIoFamily::Witness]).await;
        for _ in 0..16 {
            if !repair_quantum(&f.queue()).await.unwrap() {
                break;
            }
        }
        for (id, expected) in [("pending", true), ("verified", false), ("expired", false)] {
            assert_eq!(
                find_work("witness".to_owned(), io_queue_key(id), 0_i64)
                    .one(&f.db)
                    .await
                    .unwrap()
                    .is_some(),
                expected
            );
        }
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_malformed_point_locator_quarantines_and_next_ticket_progresses() {
    pioneer_postgres_test::with_database(async |database| {
        use pioneer_entity::html_preview_io_work as work;
        let f = fixture(database).await;
        f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
            .await;
        io_copy_source(
            &f,
            "good",
            r#"{"htmlTool":{"retention":"released","releasedCopies":["x"]}}"#,
        )
        .await;
        let tx = f.db.begin().await.unwrap();
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
            .exec(&f.db)
            .await
            .unwrap();
        let bad = f
            .queue()
            .claim_html_preview_io_work(0, 60_000)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            f.receipts()
                .load_html_preview_io_copies(&bad)
                .await
                .unwrap(),
            HtmlPreviewIoSource::Quarantine
        ));
        assert!(
            f.queue()
                .defer_html_preview_io_work(&bad, HtmlPreviewIoOutcome::Quarantine, 0)
                .await
                .unwrap()
        );
        let good = f
            .queue()
            .claim_html_preview_io_work(0, 60_000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(good.source_id(), "good");
        assert!(matches!(
            f.receipts()
                .load_html_preview_io_copies(&good)
                .await
                .unwrap(),
            HtmlPreviewIoSource::Ready(_)
        ));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_generic_claim_admits_point_work_during_continuous_resync() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
        f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
            .await;
        let metadata = r#"{"htmlTool":{"retention":"released","releasedCopies":["x"]}}"#;
        for id in ["a", "b", "c"] {
            io_copy_source(&f, id, metadata).await;
        }
        let workspace = HtmlPreviewIoLocator::new("ws");
        let point = HtmlPreviewIoLocator::new("point");
        let tx = f.db.begin().await.unwrap();
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
                .queue()
                .claim_html_preview_io_work(now, 60_000)
                .await
                .unwrap()
                .unwrap();
            match claim.row.family.as_str() {
                "seed_copies" => {
                    assert!(
                        f.queue()
                            .html_preview_io_seed_quantum(&claim)
                            .await
                            .unwrap()
                    );
                    seed_quanta += 1;
                }
                "witness" => {
                    point_seen = true;
                    assert!(
                        f.queue()
                            .defer_html_preview_io_work(&claim, HtmlPreviewIoOutcome::Done, now)
                            .await
                            .unwrap()
                    );
                }
                "copies" => {
                    assert!(
                        f.receipts()
                            .finish_html_preview_copy_cleanup_claimed(&claim, metadata, "{}")
                            .await
                            .unwrap()
                    );
                }
                family => panic!("unexpected family {family}"),
            }
            let tx = f.db.begin().await.unwrap();
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_lost_source_ack_response_recovers_newer_request_after_restart() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
        f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
            .await;
        let metadata = r#"{"htmlTool":{"retention":"released","releasedCopies":["x"]}}"#;
        io_copy_source(&f, "lost-ack", metadata).await;
        let tx = f.db.begin().await.unwrap();
        request(&tx, HtmlPreviewIoFamily::Copies, "lost-ack", 0, "ws")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let old = f
            .queue()
            .claim_html_preview_io_work(0, 60_000)
            .await
            .unwrap()
            .unwrap();
        // Source preparation precedes a producer's newer requested revision.
        let tx = f.db.begin().await.unwrap();
        request(&tx, HtmlPreviewIoFamily::Copies, "lost-ack", 0, "ws")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        // Deliberately lose the response of the actual source+ACK API, rather than
        // pretending that a failed driver response establishes rollback or empty.
        drop(
            f.receipts()
                .finish_html_preview_copy_cleanup_claimed(&old, metadata, "{}")
                .await,
        );
        let reopened = f.db.maintenance();
        let restarted = HtmlPreviewIoQueue::new(&reopened);
        let receipts = HtmlPreviewReceipts::new(&reopened, &f.wake);
        assert_eq!(
            artifact::Entity::find_by_id("lost-ack")
                .one(&reopened)
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
            receipts.load_html_preview_io_copies(&newer).await.unwrap(),
            HtmlPreviewIoSource::Obsolete
        ));
        assert!(
            restarted
                .defer_html_preview_io_work(&newer, HtmlPreviewIoOutcome::Done, now)
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_repair_revision_and_frontier_capture_the_same_committed_request() {
    pioneer_postgres_test::with_database(async |database| {
        use pioneer_entity::html_preview_io_work as work;
        let f = fixture(database).await;
        f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1 WHERE family='seed_witness'").await;
        let metadata = r#"{"htmlTool":{"retention":"released","releasedCopies":["x"]}}"#;
        io_copy_source(&f, "a", metadata).await;
        f.request_repair(&[HtmlPreviewIoFamily::Copies]).await;
        let claim = f.queue().claim_html_preview_io_work(chrono::Utc::now().timestamp_millis(), 60_000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claim.row.family, "seed_copies");
        // A repair arrives after claim but before the pass has a frontier.
        io_copy_source(&f, "b", metadata).await;
        let tx = f.db.begin().await.unwrap();
        f.queue().request_html_preview_io_resync(&tx, &HtmlPreviewIoLocator::new("ws"), &[HtmlPreviewIoFamily::Copies])
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(f.queue().html_preview_io_seed_quantum(&claim).await.unwrap());
        let row = find_work("seed_copies".to_owned(), io_queue_key("ws"), 0_i64)
            .one(&f.db)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(row.requested_revision, claim.row.requested_revision);
        assert_eq!(row.pass_revision, row.requested_revision);
        assert_eq!(row.upper_id.as_deref(), Some("b"));
        assert_eq!(row.cursor_id.as_deref(), Some("a"));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn blocked_io_recovery_revisits_an_older_row_with_a_growing_suffix() {
    pioneer_postgres_test::with_database(async |database| {
        use pioneer_entity::html_preview_io_work as work;
        let f = fixture(database).await;
        f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
            .await;
        let tx = f.db.begin().await.unwrap();
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
        f.sql("UPDATE html_preview_io_work SET state='blocked',due_at=NULL WHERE family='copies'")
            .await;
        let upper = f.queue().html_preview_io_recovery_frontier().await.unwrap();
        let mut after = f.queue().recover_html_preview_io_blocked_through(&Default::default(), &upper)
            .await
            .unwrap()
            .unwrap();
        let first = work::Entity::find()
            .filter(work::Column::Family.eq("copies"))
            .order_by_asc(work::Column::ObjectId)
            .one(&f.db)
            .await
            .unwrap()
            .unwrap();
        f.db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE html_preview_io_work SET state='blocked',due_at=NULL WHERE family='copies' AND object_id=$1",
                [first.object_id.clone().into()],
            ))
            .await
            .unwrap();
        let mut finished = false;
        for i in 0..4 {
            let tx = f.db.begin().await.unwrap();
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
            match f.queue().recover_html_preview_io_blocked_through(&after, &upper)
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
        let upper = f.queue().html_preview_io_recovery_frontier().await.unwrap();
        f.queue().recover_html_preview_io_blocked_through(&Default::default(), &upper)
            .await
            .unwrap();
        let row = find_work(first.family, first.object_id, first.ordinal)
            .one(&f.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.state, "ready");
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn retirement_page_keeps_individual_metadata_budget_and_empty_reader_only_eof() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
        for index in 0..17 {
            // Oversized metadata must never enter preparation even inside page16.
            let oversized = index == 8;
            f.db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO artifact(id,workspace_id,display_name,kind,mime_type,status,created_by_kind,created_by_actor_id,current_version_id,metadata_json) VALUES($1,'ws','preview','screenshot','image/png','ready','tool','html_preview',$2,$3)",
                [format!("page-{index:02}").into(), if oversized {Some("advisory-version".to_owned())} else {None}.into(), if oversized {serde_json::json!({"padding": "x".repeat(1048577)}).to_string()} else {"{}".to_owned()}.into()],
            )).await.unwrap();
        }
        use crate::preview_retirement::{PreviewArtifactScan, PreviewCandidate};
        let mut progress = PreviewArtifactScan::default();
        let upper = f.receipts().html_preview_artifact_frontier().await.unwrap();
        f.routes.reset();
        for _ in 0..17 {
            assert!(matches!(progress.quantum(&f.db, &upper).await.unwrap(), PreviewCandidate::Skipped));
        }
        // Two compact discovery pages; no metadata fetch or writer reservation.
        assert_eq!(f.routes.counts(), (2, 0));
        f.routes.reset();
        assert!(matches!(progress.quantum(&f.db, &upper).await.unwrap(), PreviewCandidate::Exhausted));
        assert_eq!(f.routes.counts(), (1, 0));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_released_import_producer_rolls_back_domain_and_obligation_together() {
    pioneer_postgres_test::with_database(async |database| {
        use crate::artifact::{IngestArtifactMetadataRecord, NewArtifactBlobRecord};
        use sea_orm::PaginatorTrait;
        use pioneer_entity::{artifact_blob, artifact_version};
        let f = fixture(database).await;
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
        let store = crate::ingest::ArtifactIngestStore::new(&f.db, &f.wake);
        let before = (
            artifact::Entity::find()
                .count(&f.db)
                .await
                .unwrap(),
            artifact_blob::Entity::find()
                .count(&f.db)
                .await
                .unwrap(),
            artifact_version::Entity::find()
                .count(&f.db)
                .await
                .unwrap(),
        );
        f.sql("CREATE OR REPLACE FUNCTION io_fail_enqueue() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected enqueue failure'; END $$; CREATE TRIGGER io_fail_enqueue BEFORE INSERT ON html_preview_io_work FOR EACH ROW WHEN (NEW.family='copies') EXECUTE FUNCTION io_fail_enqueue()").await;
        assert!(
            store.commit_without_publication(
                crate::ingest::PreparedArtifactIngest::new(blob.clone(), request.clone(),
                    None, Default::default(), None, None).unwrap(), None,
                )
                .await
                .is_err()
        );
        assert_eq!(
            before,
            (
                artifact::Entity::find()
                    .count(&f.db)
                    .await
                    .unwrap(),
                artifact_blob::Entity::find()
                    .count(&f.db)
                    .await
                    .unwrap(),
                artifact_version::Entity::find()
                    .count(&f.db)
                    .await
                    .unwrap()
            )
        );
        f.sql("DROP TRIGGER io_fail_enqueue ON html_preview_io_work").await;
        let imported = store.commit_without_publication(
            crate::ingest::PreparedArtifactIngest::new(blob, request,
                None, Default::default(), None, None).unwrap(), None,
            )
            .await
            .unwrap();
        assert!(imported.artifact.current_version_id.is_none());
        let claim = f.queue().claim_html_preview_io_work(chrono::Utc::now().timestamp_millis(), 60_000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claim.source_id(), imported.artifact.id);
        assert!(matches!(
            f.receipts().load_html_preview_io_copies(&claim).await.unwrap(),
            HtmlPreviewIoSource::Ready(_)
        ));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_business_retry_preserves_cas_class_through_artifact_error_boundary() {
    use crate::artifact::ArtifactCrudError;
    for conflict in [true, false] {
        pioneer_postgres_test::with_database(async |database| {
            let f = fixture(database).await;
            let attempts = std::sync::atomic::AtomicUsize::new(0);
            let store = f.queue();
            let connection = &f.db;
            let result: Result<()> = store
                .run_serialized_write(|| {
                    let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    async move {
                        let tx = connection.begin().await?;
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
            let tx = f.db.begin().await.unwrap();
            tx.rollback().await.unwrap();
            Ok(())
        })
        .await
        .unwrap();
    }
}
