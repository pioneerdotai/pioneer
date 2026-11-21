//! PostgreSQL regressions for preview retention and its I/O queue.
use super::{CrudStore, HtmlPreviewRetentionProgress, index_quantum};
use futures_util::FutureExt;
use pioneer_compaction::SourceRef;
use pioneer_compaction::frozen::FrozenMessageRef;
use pioneer_crud_artifacts::html_preview::find_frozen_result;
use pioneer_crud_artifacts::html_preview_io::{
    HtmlPreviewIoFamily, HtmlPreviewIoLocator, find_work,
};
use pioneer_crud_artifacts::html_preview_retention_queries as queries;
use pioneer_crud_artifacts::html_preview_retention_queries::col;
use pioneer_crud_artifacts::visibility::temporary_preview_predicate;
use pioneer_crud_compaction::accepted_import::FrozenImportRecord;
use pioneer_crud_compaction::preview_frozen_index::unowned;
use pioneer_entity::{
    artifact, artifact_binding, artifact_blob, html_preview_frozen_index as frozen_index,
    turn_input,
};
use pioneer_postgres_test::TestDatabase;
use pioneer_sqlite::{
    SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteClass,
    SqliteWriteEvent, SqliteWriteExecutor, SqliteWriteObserver,
};
use sea_orm::sea_query::{ExprTrait, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait, QueryFilter,
    Statement, TransactionTrait,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};

// A finite upper bound beyond every ASCII manifest ID in these fixtures.
const TEST_INDEX_FRONTIER: &str = "zzzzzzzzzz";

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
struct Fixture<'db> {
    store: CrudStore,
    reader: DatabaseConnection,
    writer: SqliteWriteExecutor,
    routes: Arc<Routes>,
    database: &'db TestDatabase,
}
impl Fixture<'_> {
    async fn request_repair(&self, families: &[HtmlPreviewIoFamily]) {
        let transaction = self.store.connection.begin().await.unwrap();
        self.store
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
        self.reader = self.database.connect_read_only().await.unwrap();
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
}
async fn fixture(database: &TestDatabase) -> Fixture<'_> {
    let connection = database.connect_with_limit(1).await.unwrap();
    let routes = Arc::new(Routes::default());
    let writer = SqliteWriteExecutor::with_observer(connection, routes.clone());
    let reader = database.connect_read_only().await.unwrap();
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

async fn preview_input_fixture(f: &Fixture<'_>) {
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
async fn preview_is_temporary(f: &Fixture<'_>) -> bool {
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
        .try_get::<bool>("", "temporary")
        .unwrap()
}

#[tokio::test]
async fn new_inputs_are_adopted_atomically_and_rollback_preserves_old_input() {
    for failure in ["binding", "input"] {
        pioneer_postgres_test::with_database(async |database| {
            let f = fixture(database).await;
            preview_input_fixture(&f).await;

            f.sql(r#"INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload) VALUES('old','parent-turn',0,'text','old','{"type":"text","text":"old"}')"#).await;
            let input = pioneer_protocol::UserInput::Artifact {
                artifact_id: "preview".into(),
                version_id: Some("v1".into()),
            };
            f.sql(if failure == "binding" {
                "CREATE OR REPLACE FUNCTION fail_adoption() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture binding failure'; END $$; CREATE TRIGGER fail_adoption BEFORE INSERT ON artifact_binding FOR EACH ROW WHEN (NEW.binding_kind='user_input') EXECUTE FUNCTION fail_adoption()"
            } else {
                "CREATE OR REPLACE FUNCTION fail_adoption() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture input failure'; END $$; CREATE TRIGGER fail_adoption BEFORE INSERT ON turn_input FOR EACH ROW EXECUTE FUNCTION fail_adoption()"
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
            let table = if failure == "binding" { "artifact_binding" } else { "turn_input" };
            f.sql(&format!("DROP TRIGGER fail_adoption ON {table}")).await;
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
                    .try_get::<bool>("", "owned")
                    .unwrap(),
                true
            );
            Ok(())
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn concurrent_reader_sees_new_input_and_adoption_only_after_the_same_commit() {
    for commit in [false, true] {
        pioneer_postgres_test::with_database(async |database| {
            let f = fixture(database).await;
            preview_input_fixture(&f).await;

            let prepared = crate::store::conversations::turn_writes::prepare_turn_input_projection(
                "parent-turn",
                &[pioneer_protocol::UserInput::Artifact {
                    artifact_id: "preview".into(),
                    version_id: Some("v1".into()),
                }],
                chrono::Utc::now().fixed_offset(),
            )
            .unwrap();
            let tx = f.store.connection.begin().await.unwrap();
            crate::store::conversations::turn_writes::replace_prepared_turn_input(&tx, prepared)
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
            Ok(())
        })
        .await
        .unwrap();
    }
}

// Closed test gate: these exercise the real read-only/serialized-writer fixture.
fn io_queue_key(id: &str) -> String {
    pioneer_crud_artifacts::html_preview_io::HtmlPreviewIoLocator::new(id)
        .key()
        .to_owned()
}

async fn io_copy_source(f: &Fixture<'_>, id: &str, metadata: &str) {
    f.store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO artifact(id,workspace_id,display_name,kind,mime_type,status,created_by_kind,created_by_actor_id,created_at,updated_at,metadata_json) VALUES($1,'ws','preview','image','image/png','deleted','tool','html_preview',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,$2)",
        [id.into(),metadata.into()])).await.unwrap();
}

#[tokio::test]
async fn retention_pass_finishes_with_a_continuously_growing_artifact_suffix() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
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
        Ok(())
    })
    .await
    .unwrap();
}

// An error behind the input cursor must not make the startup gate terminal.
// The service owner retries the finite traversal after EOF/backoff.
// The cap is an actual retirement hold even when none of the held histories
// contains this turn. Its release must reach HTML without waking frozen work.
#[tokio::test]
async fn html_reader_limit_release_wakes_only_the_waiting_owner() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
        for index in 0..65 {
            manifest(&f, &format!("held-{index:02}"), "parent", &[], &[], 0, None).await;
        }
        let mut after = String::new();
        let mut progress = None;
        for _ in 0..512 {
            if !index_quantum(&f.store, &mut after, &mut progress, TEST_INDEX_FRONTIER)
                .await
                .unwrap()
            {
                break;
            }
        }

        let root = crate::canonical_agent_id('E', "root-agent-turn\0parent-turn");
        let database = f.store.database_connection();
        let released = database.work_signal("maintenance.html.release");
        let mut html = released.subscribe();
        let mut frozen = database
            .work_signal("maintenance.frozen.release")
            .subscribe();
        assert!(
            unowned(
                &f.store.connection,
                "ws",
                "parent",
                "parent-turn",
                &root,
                &f.store.frozen_readers,
                &released
            )
            .await
            .unwrap()
        );
        let mut holds = Vec::new();
        for index in 0..65 {
            holds.push(
                f.store
                    .compaction_acquire_frozen_history(
                        "ws",
                        &pioneer_compaction::frozen::FrozenHistoryRef {
                            format: 1,
                            manifest_id: format!("held-{index:02}"),
                            messages: 0,
                            identity_sha256:
                                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                                    .into(),
                        },
                    )
                    .await
                    .unwrap(),
            );
        }
        assert!(
            !unowned(
                &f.store.connection,
                "ws",
                "parent",
                "parent-turn",
                &root,
                &f.store.frozen_readers,
                &released
            )
            .await
            .unwrap()
        );
        assert!(html.changed().now_or_never().is_none());
        drop(holds.pop().unwrap());
        assert!(html.changed().now_or_never().is_some());
        assert!(frozen.changed().now_or_never().is_none());
        assert!(
            unowned(
                &f.store.connection,
                "ws",
                "parent",
                "parent-turn",
                &root,
                &f.store.frozen_readers,
                &released
            )
            .await
            .unwrap()
        );
        drop(holds);
        html.begin_pass(&database).await.unwrap();
        let ordinary = f
            .store
            .compaction_acquire_frozen_history(
                "ws",
                &pioneer_compaction::frozen::FrozenHistoryRef {
                    format: 1,
                    manifest_id: "held-00".into(),
                    messages: 0,
                    identity_sha256:
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
                },
            )
            .await
            .unwrap();
        drop(ordinary);
        assert!(html.changed().now_or_never().is_none());
        assert!(frozen.changed().now_or_never().is_none());
        Ok(())
    })
    .await
    .unwrap();
}
