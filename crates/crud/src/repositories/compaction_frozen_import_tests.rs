use super::*;
use crate::CrudStore;
use migration::Migrator;
use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, Statement};
use std::path::{Path, PathBuf};

const LEGACY_ACCEPTED_IMPORT_CURRENT_SQL: &str = r#"
SELECT 1
FROM task_run_turn execution
JOIN task_run_conversation_snapshot snapshot
  ON snapshot.run_id=execution.run_id AND snapshot.task_id=execution.task_id
JOIN thread_lineage lineage
  ON lineage.child_thread_id=execution.thread_id
 AND lineage.parent_thread_id=snapshot.conversation_thread_id
JOIN thread child
  ON child.id=execution.thread_id AND child.workspace_id=snapshot.workspace_id
JOIN compaction_frozen_history h
  ON h.id=?
 AND h.owner_thread=snapshot.conversation_thread_id
 AND h.workspace_id=snapshot.workspace_id
JOIN compaction_frozen_import i
  ON i.manifest_id=h.id AND i.ordinal=?
JOIN compaction_live_sources source
  ON source.workspace_id=h.workspace_id
 AND source.thread_id=i.source_thread
 AND source.source_scope=i.source_scope
 AND source.source_id=i.source_id
 AND source.source_version=i.source_version
WHERE execution.thread_id=?
  AND execution.turn_id=?
  AND snapshot.workspace_id=?
  AND snapshot.history_json=?
  AND h.ready=1
  AND h.identity_sha256=?
  AND h.next_import=h.import_count
  AND i.proof_json=?
  AND h.imports_sha256=?
  AND h.import_count=?
LIMIT 1
"#;

struct TestFile(PathBuf);

impl Drop for TestFile {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

struct Fixture {
    store: CrudStore,
    _file: TestFile,
}

impl Fixture {
    fn db(&self) -> SqliteDatabase {
        self.store.database_connection()
    }
}

async fn open(path: &Path) -> (CrudStore, SqliteWriteExecutor) {
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    options.max_connections(1).min_connections(1);
    let writer_connection = Database::connect(options).await.unwrap();
    let writer = SqliteWriteExecutor::new(writer_connection.clone());
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();

    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=ro", path.display()));
    options.max_connections(1).min_connections(1);
    let reader = Database::connect(options).await.unwrap();
    reader
        .execute_unprepared("PRAGMA query_only=ON")
        .await
        .unwrap();
    let database = SqliteDatabase::from_executor(reader, writer.clone());
    assert!(database.reader_query_only_enabled().await.unwrap());
    (CrudStore::new(database).with_maintenance_access(), writer)
}

async fn fixture() -> Fixture {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let file = TestFile(std::env::temp_dir().join(format!(
        "pioneer-accepted-import-{}.sqlite",
        uuid::Uuid::new_v4()
    )));
    let (store, _writer) = open(&file.0).await;
    let db = store.database_connection();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',1,1)",
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('other','other',1,0)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('parent','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('child','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('source-thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('parent-turn','parent','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('child-turn','child','in_progress','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('source-turn','source-thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('other-source-turn','source-thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','parent','parent','parent-turn','agent','running','Task','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run','task','run',1,1,'running','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('execution','task','run','child','child-turn','initial',0,1,'running',CURRENT_TIMESTAMP)",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('child','parent','parent',1,CURRENT_TIMESTAMP)",
        "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) VALUES ('run','task','ws','parent','[\"accepted\"]',CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    drop(db);
    Fixture { store, _file: file }
}

#[derive(Clone)]
struct Branch {
    name: &'static str,
    source: SourceRef,
}

fn branches() -> Vec<Branch> {
    vec![
        Branch {
            name: "context",
            source: SourceRef {
                scope: "context:source-turn".into(),
                id: "context-source".into(),
                version: "revision:1".into(),
            },
        },
        Branch {
            name: "item",
            source: SourceRef {
                scope: "item:source-turn".into(),
                id: "item-source".into(),
                version: "item-revision:1".into(),
            },
        },
        Branch {
            name: "event",
            source: SourceRef {
                scope: "event:source-turn".into(),
                id: "event-source".into(),
                version: "event-revision:1".into(),
            },
        },
        Branch {
            name: "input",
            source: SourceRef {
                scope: "input:source-turn".into(),
                id: "input-source".into(),
                version: "input-revision:1".into(),
            },
        },
        Branch {
            name: "checkpoint",
            source: SourceRef {
                scope: "checkpoint:checkpoint-owner".into(),
                id: "checkpoint-source".into(),
                version: "checkpoint-version".into(),
            },
        },
        Branch {
            name: "task-basis",
            source: SourceRef {
                scope: "task-basis:basis-run".into(),
                id: "basis-run".into(),
                version: "task-basis-revision:1".into(),
            },
        },
    ]
}

struct CanonicalBranch {
    branch: Branch,
    table: &'static str,
    revision_table: &'static str,
    insert_sql: &'static str,
}

fn canonical_branches() -> Vec<CanonicalBranch> {
    let mut all = branches().into_iter();
    vec![
        CanonicalBranch {
            branch: all.next().unwrap(),
            table: "turn_llm_context",
            revision_table: "compaction_source_revision",
            insert_sql: "INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,created_at) VALUES ('context-source','source-turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
        },
        CanonicalBranch {
            branch: all.next().unwrap(),
            table: "turn_item",
            revision_table: "compaction_item_revision",
            insert_sql: "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,created_at,updated_at) VALUES ('item-source','source-turn','item-source','command_execution','completed','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        },
        CanonicalBranch {
            branch: all.next().unwrap(),
            table: "turn_event",
            revision_table: "compaction_event_revision",
            insert_sql: "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('event-source','source-thread','source-turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
        },
        CanonicalBranch {
            branch: all.next().unwrap(),
            table: "turn_input",
            revision_table: "compaction_input_revision",
            insert_sql: "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('input-source','source-turn',0,'text','fixture','{}',CURRENT_TIMESTAMP)",
        },
    ]
}

async fn install_canonical_sources(db: &SqliteDatabase) {
    for source in canonical_branches() {
        db.execute_unprepared(source.insert_sql).await.unwrap();
    }
}

async fn install_checkpoint_and_task_basis(db: &SqliteDatabase) {
    for sql in [
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('basis-run','task','basis-run',1,2,'succeeded','agent')",
        "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) VALUES ('basis-run','task','ws','source-thread','[]',CURRENT_TIMESTAMP)",
        "INSERT INTO compaction_context(workspace_id,thread_id,owner,format_version) VALUES ('ws','source-thread','checkpoint-owner',1)",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES ('checkpoint-operation','checkpoint-owner','checkpoint-fixture','completed','{}',1)",
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES ('checkpoint-source','checkpoint-operation','checkpoint-owner',0,'fixture','checkpoint-version','{}',0,1,'applied')",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    db.execute_unprepared("DELETE FROM compaction_task_basis_revision WHERE run_id='basis-run'")
        .await
        .unwrap();
}

fn prepared(manifest: &str, source: SourceRef) -> PreparedFrozenImport {
    PreparedFrozenImport {
        workspace: "ws".into(),
        destination: "child".into(),
        output_digest: String::new(),
        original_json: String::new(),
        record: FrozenImportRecord {
            message_ordinal: 0,
            source_thread: "source-thread".into(),
            source,
            delivery_id: "delivery".into(),
            candidate_id: "candidate".into(),
            output_manifest: "output".into(),
            output_ordinal: 0,
            acknowledgement: SourceRef {
                scope: "event:parent-turn".into(),
                id: "acknowledgement".into(),
                version: "event-revision:1".into(),
            },
        },
        accepted_basis: Some(AcceptedImportBasis {
            turn: "child-turn".into(),
            history_json: "[\"accepted\"]".into(),
            manifest: manifest.into(),
            digest: format!("digest-{manifest}"),
            imports_digest: format!("imports-{manifest}"),
            import_count: 1,
            ordinal: 0,
            proof_json: "{}".into(),
        }),
        target_checkpoint: None,
    }
}

async fn install_manifest(
    db: &SqliteDatabase,
    manifest: &str,
    source: &SourceRef,
) -> PreparedFrozenImport {
    let prepared = prepared(manifest, source.clone());
    let basis = prepared.accepted_basis.as_ref().unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES (?,?,?,?,0,0,1,?,1,1)",
        [
            manifest.into(),
            "ws".into(),
            "parent".into(),
            basis.digest.clone().into(),
            basis.imports_digest.clone().into(),
        ],
    ))
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES (?,0,0,?,?,?,?, '{}',2)",
        [
            manifest.into(),
            source.scope.clone().into(),
            source.id.clone().into(),
            source.version.clone().into(),
            "source-thread".into(),
        ],
    ))
    .await
    .unwrap();
    prepared
}

async fn update_import_field(db: &SqliteDatabase, manifest: &str, column: &str, value: &str) {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        format!("UPDATE compaction_frozen_import_data SET {column}=? WHERE manifest_id=?"),
        [value.into(), manifest.into()],
    ))
    .await
    .unwrap();
}

async fn assert_matches_oracle(
    db: &SqliteDatabase,
    prepared: &PreparedFrozenImport,
    expected: bool,
    case: &str,
) {
    let snapshot = db.begin_read().await.unwrap();
    let actual = accepted_import_current(&snapshot, prepared).await.unwrap();
    if !prepared.record.source.scope.starts_with("checkpoint:") {
        let legacy = snapshot
            .query_one_raw(
                accepted_import_current_statement(LEGACY_ACCEPTED_IMPORT_CURRENT_SQL, prepared)
                    .unwrap(),
            )
            .await
            .unwrap()
            .is_some();
        assert_eq!(
            actual, legacy,
            "new query diverged from legacy oracle: {case}"
        );
    }
    assert_eq!(actual, expected, "unexpected fixture result: {case}");
    snapshot.rollback().await.unwrap();
}

async fn set_branch_workspace(db: &SqliteDatabase, branch: &Branch, workspace: &str) {
    match branch.name {
        "context" | "item" | "event" | "input" => {
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE thread SET workspace_id=? WHERE id='source-thread'",
                [workspace.into()],
            ))
            .await
            .unwrap();
        }
        "checkpoint" => {
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE compaction_context SET workspace_id=? WHERE owner='checkpoint-owner'",
                [workspace.into()],
            ))
            .await
            .unwrap();
        }
        "task-basis" => {
            for statement in [
                "UPDATE thread SET workspace_id=? WHERE id='source-thread'",
                "UPDATE task_run_conversation_snapshot SET workspace_id=? WHERE run_id='basis-run'",
            ] {
                db.execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    statement,
                    [workspace.into()],
                ))
                .await
                .unwrap();
            }
            db.execute_unprepared(
                "DELETE FROM compaction_task_basis_revision WHERE run_id='basis-run'",
            )
            .await
            .unwrap();
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn accepted_import_current_checks_every_identity_predicate_for_all_six_branches() {
    let fixture = fixture().await;
    let db = fixture.db();
    install_canonical_sources(&db).await;
    install_checkpoint_and_task_basis(&db).await;
    for branch in branches() {
        let manifest = format!("identity-{}", branch.name);
        let import = install_manifest(&db, &manifest, &branch.source).await;
        assert_matches_oracle(&db, &import, true, &format!("{} positive", branch.name)).await;

        for (column, wrong, original) in [
            ("source_scope", "wrong:scope", branch.source.scope.as_str()),
            ("source_id", "wrong-source", branch.source.id.as_str()),
            (
                "source_version",
                "wrong-version",
                branch.source.version.as_str(),
            ),
            ("source_thread", "parent", "source-thread"),
        ] {
            update_import_field(&db, &manifest, column, wrong).await;
            assert_matches_oracle(
                &db,
                &import,
                false,
                &format!("{} wrong {column}", branch.name),
            )
            .await;
            update_import_field(&db, &manifest, column, original).await;
        }

        set_branch_workspace(&db, &branch, "other").await;
        assert_matches_oracle(
            &db,
            &import,
            false,
            &format!("{} wrong workspace", branch.name),
        )
        .await;
        set_branch_workspace(&db, &branch, "ws").await;
        assert_matches_oracle(&db, &import, true, &format!("{} restored", branch.name)).await;
    }
}

#[tokio::test]
async fn accepted_import_current_checks_each_canonical_liveness_predicate_independently() {
    let fixture = fixture().await;
    let db = fixture.db();
    install_canonical_sources(&db).await;
    for canonical in canonical_branches() {
        let manifest = format!("canonical-{}", canonical.branch.name);
        let import = install_manifest(&db, &manifest, &canonical.branch.source).await;
        assert_matches_oracle(
            &db,
            &import,
            true,
            &format!("{} canonical positive", canonical.branch.name),
        )
        .await;

        db.execute_unprepared(&format!(
            "UPDATE {} SET present=0 WHERE source_id='{}'",
            canonical.revision_table, canonical.branch.source.id
        ))
        .await
        .unwrap();
        assert_matches_oracle(
            &db,
            &import,
            false,
            &format!("{} present=0", canonical.branch.name),
        )
        .await;
        db.execute_unprepared(&format!(
            "UPDATE {} SET present=1 WHERE source_id='{}'",
            canonical.revision_table, canonical.branch.source.id
        ))
        .await
        .unwrap();

        db.execute_unprepared(&format!(
            "UPDATE {} SET revision=2 WHERE source_id='{}'",
            canonical.revision_table, canonical.branch.source.id
        ))
        .await
        .unwrap();
        assert_matches_oracle(
            &db,
            &import,
            false,
            &format!("{} revision mismatch", canonical.branch.name),
        )
        .await;
        db.execute_unprepared(&format!(
            "UPDATE {} SET revision=1 WHERE source_id='{}'",
            canonical.revision_table, canonical.branch.source.id
        ))
        .await
        .unwrap();

        db.execute_unprepared(&format!(
            "DELETE FROM {} WHERE id='{}'",
            canonical.table, canonical.branch.source.id
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "UPDATE {} SET revision=1,present=1,turn_id='source-turn' WHERE source_id='{}'",
            canonical.revision_table, canonical.branch.source.id
        ))
        .await
        .unwrap();
        assert_matches_oracle(
            &db,
            &import,
            false,
            &format!("{} canonical row absent", canonical.branch.name),
        )
        .await;
        db.execute_unprepared(canonical.insert_sql).await.unwrap();
        db.execute_unprepared(&format!(
            "UPDATE {} SET revision=1,present=1,turn_id='source-turn' WHERE source_id='{}'",
            canonical.revision_table, canonical.branch.source.id
        ))
        .await
        .unwrap();

        db.execute_unprepared(&format!(
            "UPDATE {} SET turn_id='other-source-turn' WHERE source_id='{}'",
            canonical.revision_table, canonical.branch.source.id
        ))
        .await
        .unwrap();
        let other_source_scope = canonical
            .branch
            .source
            .scope
            .strip_suffix("source-turn")
            .expect("canonical source scope must end in the fixture turn id")
            .to_owned()
            + "other-source-turn";
        update_import_field(&db, &manifest, "source_scope", &other_source_scope).await;
        assert_matches_oracle(
            &db,
            &import,
            false,
            &format!("{} canonical/revision turn mismatch", canonical.branch.name),
        )
        .await;
        db.execute_unprepared(&format!(
            "UPDATE {} SET turn_id='source-turn' WHERE source_id='{}'",
            canonical.revision_table, canonical.branch.source.id
        ))
        .await
        .unwrap();
        update_import_field(
            &db,
            &manifest,
            "source_scope",
            &canonical.branch.source.scope,
        )
        .await;
        assert_matches_oracle(
            &db,
            &import,
            true,
            &format!("{} restored canonical turn", canonical.branch.name),
        )
        .await;
    }
}

#[tokio::test]
async fn accepted_import_current_checks_task_basis_revision_and_legacy_json_independently() {
    let fixture = fixture().await;
    let db = fixture.db();
    install_checkpoint_and_task_basis(&db).await;
    let branch = branches().remove(5);
    let import = install_manifest(&db, "task-basis", &branch.source).await;
    assert_matches_oracle(&db, &import, true, "missing revision falls back to one").await;

    db.execute_unprepared(
        "INSERT INTO compaction_task_basis_revision(run_id,revision) VALUES ('basis-run',2)",
    )
    .await
    .unwrap();
    update_import_field(&db, "task-basis", "source_version", "task-basis-revision:2").await;
    assert_matches_oracle(&db, &import, true, "explicit revision two").await;
    update_import_field(&db, "task-basis", "source_version", "task-basis-revision:1").await;
    assert_matches_oracle(&db, &import, false, "stale task-basis revision").await;

    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE task_run_conversation_snapshot SET history_json=? WHERE run_id='basis-run'",
        ["  {}".into()],
    ))
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE compaction_task_basis_revision SET revision=3 WHERE run_id='basis-run'",
    )
    .await
    .unwrap();
    update_import_field(&db, "task-basis", "source_version", "task-basis-revision:3").await;
    assert_matches_oracle(&db, &import, false, "non-array JSON with matching revision").await;

    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE task_run_conversation_snapshot SET history_json=? WHERE run_id='basis-run'",
        [" \t[1]".into()],
    ))
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE compaction_task_basis_revision SET revision=4 WHERE run_id='basis-run'",
    )
    .await
    .unwrap();
    update_import_field(&db, "task-basis", "source_version", "task-basis-revision:4").await;
    assert_matches_oracle(&db, &import, false, "ltrim does not remove a tab").await;

    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE task_run_conversation_snapshot SET history_json=? WHERE run_id='basis-run'",
        ["   [1]".into()],
    ))
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE compaction_task_basis_revision SET revision=5 WHERE run_id='basis-run'",
    )
    .await
    .unwrap();
    update_import_field(&db, "task-basis", "source_version", "task-basis-revision:5").await;
    assert_matches_oracle(
        &db,
        &import,
        true,
        "ltrim removes spaces before a JSON array",
    )
    .await;
}

#[tokio::test]
async fn accepted_import_current_checks_checkpoint_status_format_and_epoch_independence() {
    let fixture = fixture().await;
    let db = fixture.db();
    install_checkpoint_and_task_basis(&db).await;
    let branch = branches().remove(4);
    let import = install_manifest(&db, "checkpoint", &branch.source).await;
    db.execute_unprepared(
        "DELETE FROM compaction_projection_epoch WHERE thread_id='source-thread'",
    )
    .await
    .unwrap();
    assert_matches_oracle(
        &db,
        &import,
        true,
        "applied checkpoint with epoch fallback zero",
    )
    .await;

    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='retained' WHERE id='checkpoint-source'",
    )
    .await
    .unwrap();
    assert_matches_oracle(
        &db,
        &import,
        true,
        "retained checkpoint with completed operation",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_operation SET status='running' WHERE id='checkpoint-operation'",
    )
    .await
    .unwrap();
    assert_matches_oracle(
        &db,
        &import,
        false,
        "retained checkpoint with unfinished operation",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_operation SET status='completed' WHERE id='checkpoint-operation'",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='pending' WHERE id='checkpoint-source'",
    )
    .await
    .unwrap();
    assert_matches_oracle(&db, &import, false, "checkpoint with invalid status").await;

    db.execute_unprepared("UPDATE compaction_checkpoint SET status='applied',projection_version=7 WHERE id='checkpoint-source'")
        .await
        .unwrap();
    db.execute_unprepared(
        "INSERT INTO compaction_projection_epoch(thread_id,version) VALUES ('source-thread',7)",
    )
    .await
    .unwrap();
    assert_matches_oracle(&db, &import, true, "matching nonzero checkpoint epoch").await;
    db.execute_unprepared(
        "UPDATE compaction_projection_epoch SET version=8 WHERE thread_id='source-thread'",
    )
    .await
    .unwrap();
    assert_matches_oracle(
        &db,
        &import,
        true,
        "published checkpoint is independent of the current projection epoch",
    )
    .await;

    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET format_version=2 WHERE id='checkpoint-source'",
    )
    .await
    .unwrap();
    assert_matches_oracle(&db, &import, false, "unsupported checkpoint format").await;
}

#[tokio::test]
async fn accepted_import_current_preserves_every_outer_binding() {
    let fixture = fixture().await;
    let db = fixture.db();
    install_canonical_sources(&db).await;
    let source = branches().remove(2).source;
    let import = install_manifest(&db, "outer-bindings", &source).await;
    assert_matches_oracle(&db, &import, true, "outer baseline").await;

    let mut cases = Vec::new();
    let mut changed = import.clone();
    changed.workspace = "other".into();
    cases.push(("workspace", changed));
    let mut changed = import.clone();
    changed.destination = "parent".into();
    cases.push(("destination thread", changed));
    let mut changed = import.clone();
    changed.accepted_basis.as_mut().unwrap().turn = "parent-turn".into();
    cases.push(("execution turn", changed));
    let mut changed = import.clone();
    changed.accepted_basis.as_mut().unwrap().history_json = "[]".into();
    cases.push(("snapshot", changed));
    let mut changed = import.clone();
    changed.accepted_basis.as_mut().unwrap().manifest = "missing".into();
    cases.push(("manifest", changed));
    let mut changed = import.clone();
    changed.accepted_basis.as_mut().unwrap().digest = "wrong".into();
    cases.push(("identity digest", changed));
    let mut changed = import.clone();
    changed.accepted_basis.as_mut().unwrap().imports_digest = "wrong".into();
    cases.push(("imports digest", changed));
    let mut changed = import.clone();
    changed.accepted_basis.as_mut().unwrap().import_count = 2;
    cases.push(("import count", changed));
    let mut changed = import.clone();
    changed.accepted_basis.as_mut().unwrap().ordinal = 1;
    cases.push(("ordinal", changed));
    let mut changed = import.clone();
    changed.accepted_basis.as_mut().unwrap().proof_json = "{\"wrong\":true}".into();
    cases.push(("proof", changed));
    for (case, changed) in cases {
        assert_matches_oracle(&db, &changed, false, case).await;
    }

    db.execute_unprepared(
        "UPDATE thread_lineage SET parent_thread_id='child' WHERE child_thread_id='child'",
    )
    .await
    .unwrap();
    assert_matches_oracle(&db, &import, false, "parent lineage").await;
    db.execute_unprepared(
        "UPDATE thread_lineage SET parent_thread_id='parent' WHERE child_thread_id='child'",
    )
    .await
    .unwrap();
    db.execute_unprepared("UPDATE compaction_frozen_history SET ready=0 WHERE id='outer-bindings'")
        .await
        .unwrap();
    assert_matches_oracle(&db, &import, false, "not ready").await;
    db.execute_unprepared(
        "UPDATE compaction_frozen_history SET ready=1,next_import=0 WHERE id='outer-bindings'",
    )
    .await
    .unwrap();
    assert_matches_oracle(&db, &import, false, "incomplete import cursor").await;
}

async fn install_shared_manifest(db: &SqliteDatabase, source: &SourceRef) -> PreparedFrozenImport {
    let _physical = install_manifest(db, "shared-physical", source).await;
    let logical = prepared("shared-logical", source.clone());
    let basis = logical.accepted_basis.as_ref().unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES (?,?,?,?,0,0,1,?,1,1)",
        [
            "shared-logical".into(),
            "ws".into(),
            "parent".into(),
            basis.digest.clone().into(),
            basis.imports_digest.clone().into(),
        ],
    ))
    .await
    .unwrap();
    for sql in [
        "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES ('shared-logical',1,1,0)",
        "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES ('shared-logical',1,0,1,'shared-physical')",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    logical
}

#[tokio::test]
async fn accepted_import_current_reads_shared_range_imports_through_the_logical_view() {
    let fixture = fixture().await;
    let db = fixture.db();
    install_canonical_sources(&db).await;
    let source = branches().remove(2).source;
    let import = install_shared_manifest(&db, &source).await;
    assert_matches_oracle(&db, &import, true, "shared import range").await;
    let physical_rows = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT COUNT(*) AS count FROM compaction_frozen_import_data".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "count")
        .unwrap();
    assert_eq!(physical_rows, 1);
}

async fn enable_zstd(db: &SqliteDatabase) {
    for table in ["turn_llm_context", "turn_item", "turn_event", "turn_input"] {
        db.query_one_write_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT zstd_enable_transparent(?)",
            [serde_json::json!({
                "table": table,
                "column": "payload",
                "compression_level": 3,
                "dict_chooser": "'[nodict]'"
            })
            .to_string()
            .into()],
        ))
        .await
        .unwrap();
    }
}

async fn compress_canonical_payloads(db: &SqliteDatabase) {
    for table in ["turn_llm_context", "turn_item", "turn_event", "turn_input"] {
        let payload = db
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                format!("SELECT payload FROM _{table}_zstd WHERE _payload_dict IS NULL LIMIT 1"),
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<String>("", "payload")
            .unwrap();
        let compressed =
            pioneer_sqlite::zstd::compress_column_value(payload.as_bytes(), 3, None).unwrap();
        assert_eq!(
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                format!(
                    "UPDATE _{table}_zstd SET payload=?,_payload_dict=-1 WHERE _payload_dict IS NULL AND payload=?"
                ),
                [compressed.into(), payload.into()],
            ))
            .await
            .unwrap()
            .rows_affected(),
            1
        );
    }
}

#[tokio::test]
async fn accepted_import_current_uses_canonical_sources_with_zstd_storage() {
    let fixture = fixture().await;
    let db = fixture.db();
    install_canonical_sources(&db).await;
    enable_zstd(&db).await;
    compress_canonical_payloads(&db).await;
    for branch in branches().into_iter().take(4) {
        let import = install_manifest(&db, &format!("zstd-{}", branch.name), &branch.source).await;
        assert_matches_oracle(&db, &import, true, &format!("zstd {}", branch.name)).await;
    }
}

fn plan_operation_targets_subject(detail: &str, operation: &str, subject: &str) -> bool {
    let prefix = format!("{operation} {subject}");
    detail.strip_prefix(&prefix).is_some_and(|suffix| {
        suffix.is_empty() || suffix.chars().next().is_some_and(char::is_whitespace)
    })
}

fn search_has_exact_equality(detail: &str, column: &str) -> bool {
    let Some((_, constraints)) = detail.rsplit_once('(') else {
        return false;
    };
    let Some(constraints) = constraints.trim_end().strip_suffix(')') else {
        return false;
    };

    constraints.split("AND").any(|constraint| {
        let Some((lhs, rhs)) = constraint.split_once('=') else {
            return false;
        };
        if rhs.trim() != "?" {
            return false;
        }

        let lhs = lhs
            .trim()
            .rsplit('.')
            .next()
            .unwrap_or_default()
            .trim_matches(|character| matches!(character, '"' | '`' | '[' | ']'));
        lhs == column
    })
}

fn plan_has_exact_search(plan: &[String], subjects: &[&str], column: &str) -> bool {
    plan.iter().any(|detail| {
        subjects.iter().any(|subject| {
            plan_operation_targets_subject(detail, "SEARCH", subject)
                && search_has_exact_equality(detail, column)
        })
    })
}

fn plan_has_subject_scan(plan: &[String], subjects: &[&str]) -> bool {
    plan.iter().any(|detail| {
        subjects
            .iter()
            .any(|subject| plan_operation_targets_subject(detail, "SCAN", subject))
    })
}

fn assert_exact_search(plan: &[String], subjects: &[&str], column: &str) {
    assert!(
        plan_has_exact_search(plan, subjects, column),
        "expected SEARCH for {subjects:?} with exact {column}=? equality: {plan:#?}"
    );
    assert!(
        !plan_has_subject_scan(plan, subjects),
        "unexpected full scan of {subjects:?}: {plan:#?}"
    );
}

#[test]
fn query_plan_key_matching_requires_the_exact_column_name() {
    let composite =
        vec!["SEARCH canonical_source USING INDEX fixture (turn_id = ? AND id = ?)".to_string()];
    assert!(plan_has_exact_search(
        &composite,
        &["canonical_source"],
        "id"
    ));
    assert!(plan_has_exact_search(
        &composite,
        &["canonical_source"],
        "turn_id"
    ));

    let suffixes = vec![
        "SEARCH canonical_source USING INDEX fixture (turn_id=? AND owner_id=? AND source_id=?)"
            .to_string(),
    ];
    assert!(!plan_has_exact_search(
        &suffixes,
        &["canonical_source"],
        "id"
    ));
    assert!(plan_has_exact_search(
        &suffixes,
        &["canonical_source"],
        "source_id"
    ));

    let scan = vec!["SCAN canonical_source".to_string()];
    assert!(plan_has_subject_scan(&scan, &["canonical_source"]));
    assert!(!plan_has_exact_search(&scan, &["canonical_source"], "id"));
}

async fn assert_production_plan(compressed: bool) {
    let fixture = fixture().await;
    let db = fixture.db();
    install_canonical_sources(&db).await;
    if compressed {
        enable_zstd(&db).await;
        compress_canonical_payloads(&db).await;
    }
    install_checkpoint_and_task_basis(&db).await;
    let source = branches().remove(2).source;
    let import = install_manifest(&db, "plan", &source).await;
    let mut statement =
        accepted_import_current_statement(ACCEPTED_IMPORT_CURRENT_SQL, &import).unwrap();
    assert!(!statement.sql.contains("compaction_live_sources"));
    statement.sql = format!("EXPLAIN QUERY PLAN {}", statement.sql);
    let plan = db
        .query_all_raw(statement)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String>("", "detail").unwrap())
        .collect::<Vec<_>>();

    for alias in [
        "context_revision",
        "item_revision",
        "event_revision",
        "input_revision",
    ] {
        assert_exact_search(&plan, &[alias], "source_id");
    }
    if compressed {
        for subjects in [
            &["context_source", "_turn_llm_context_zstd"][..],
            &["item_source", "_turn_item_zstd"][..],
            &["event_source", "_turn_event_zstd"][..],
            &["input_source", "_turn_input_zstd"][..],
        ] {
            assert_exact_search(&plan, subjects, "id");
        }
    } else {
        for alias in [
            "context_source",
            "item_source",
            "event_source",
            "input_source",
        ] {
            assert_exact_search(&plan, &[alias], "id");
        }
    }
    assert_exact_search(&plan, &["checkpoint"], "id");
    assert_exact_search(&plan, &["basis"], "run_id");
}

#[tokio::test]
async fn accepted_import_current_plan_keeps_exact_source_key_lookups() {
    assert_production_plan(false).await;
    assert_production_plan(true).await;
}
