use super::*;
use crate::CrudStore;
use migration::Migrator;
use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, Statement};
use std::path::{Path, PathBuf};

const LEGACY_SOURCES_CURRENT_SQL: &str = "SELECT COUNT(*) AS matched FROM json_each(?) wanted WHERE EXISTS (SELECT 1 FROM compaction_live_sources s WHERE s.workspace_id=? AND s.thread_id=? AND s.source_scope=json_extract(wanted.value,'$.scope') AND s.source_id=json_extract(wanted.value,'$.id') AND s.source_version=json_extract(wanted.value,'$.version') UNION ALL SELECT 1 FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner WHERE c.workspace_id=? AND c.thread_id=? AND 'checkpoint:'||p.owner=json_extract(wanted.value,'$.scope') AND p.id=json_extract(wanted.value,'$.id') AND p.identity_sha256=json_extract(wanted.value,'$.version') AND p.format_version=1 AND (p.status='applied' OR (p.status='retained' AND EXISTS(SELECT 1 FROM compaction_operation o WHERE o.id=p.operation_id AND o.status='completed'))))";

const LEGACY_CHECKPOINT_SOURCE_SQL: &str = r#"WITH RECURSIVE graph(source_scope,source_id,source_version) AS (
 SELECT 'checkpoint:'||p.owner,p.id,p.identity_sha256
 FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner
 WHERE p.id=? AND c.workspace_id=? AND c.thread_id=?
  AND p.format_version=1
  AND (p.status='applied' OR (p.status='retained' AND EXISTS(
   SELECT 1 FROM compaction_operation o WHERE o.id=p.operation_id AND o.status='completed')))
 UNION
 SELECT v.source_scope,v.source_id,v.source_version
 FROM graph g JOIN compaction_coverage v ON v.checkpoint_id=g.source_id
 WHERE g.source_scope LIKE 'checkpoint:%'
 UNION
 SELECT 'checkpoint:'||COALESCE(previous.owner,''),node.previous,COALESCE(previous.identity_sha256,'')
 FROM graph g JOIN compaction_checkpoint node ON node.id=g.source_id
 LEFT JOIN compaction_checkpoint previous ON previous.id=node.previous
 WHERE g.source_scope LIKE 'checkpoint:%' AND node.previous IS NOT NULL
 LIMIT 65537
)
SELECT root.source_scope,root.source_id,root.source_version
FROM graph root
WHERE root.source_scope LIKE 'checkpoint:%' AND root.source_id=?
 AND (SELECT COUNT(*) FROM graph)<65537
 AND EXISTS(SELECT 1 FROM graph leaf WHERE leaf.source_scope NOT LIKE 'checkpoint:%')
 AND NOT EXISTS(SELECT 1 FROM graph g WHERE NOT (
  (g.source_scope NOT LIKE 'checkpoint:%' AND EXISTS(
   SELECT 1 FROM compaction_live_sources s
   WHERE s.source_scope=g.source_scope AND s.source_id=g.source_id AND s.source_version=g.source_version
    AND s.workspace_id=?
  )) OR (g.source_scope LIKE 'checkpoint:%' AND EXISTS(
   SELECT 1 FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner
   WHERE p.id=g.source_id AND 'checkpoint:'||p.owner=g.source_scope
    AND p.identity_sha256=g.source_version AND p.format_version=1 AND c.workspace_id=?
    AND (p.status='applied' OR (p.status='retained' AND EXISTS(
     SELECT 1 FROM compaction_operation o WHERE o.id=p.operation_id AND o.status='completed')))
  ))
 )) LIMIT 1"#;

const LEGACY_CHECKPOINT_PAYLOAD_SQL: &str = r#"WITH RECURSIVE graph(source_scope,source_id,source_version) AS (
 SELECT 'checkpoint:'||p.owner,p.id,p.identity_sha256
 FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner
 WHERE p.id=? AND 'checkpoint:'||p.owner=? AND p.identity_sha256=?
  AND p.format_version=1 AND c.workspace_id=? AND c.thread_id=?
  AND (p.status='applied' OR (p.status='retained' AND EXISTS(
   SELECT 1 FROM compaction_operation o WHERE o.id=p.operation_id AND o.status='completed')))
 UNION
 SELECT v.source_scope,v.source_id,v.source_version
 FROM graph g JOIN compaction_coverage v ON v.checkpoint_id=g.source_id
 WHERE g.source_scope LIKE 'checkpoint:%'
 UNION
 SELECT 'checkpoint:'||COALESCE(previous.owner,''),node.previous,COALESCE(previous.identity_sha256,'')
 FROM graph g JOIN compaction_checkpoint node ON node.id=g.source_id
 LEFT JOIN compaction_checkpoint previous ON previous.id=node.previous
 WHERE g.source_scope LIKE 'checkpoint:%' AND node.previous IS NOT NULL
 LIMIT 65537
)
SELECT 1 AS revision,root.summary AS fragment,length(root.summary) AS characters
FROM compaction_checkpoint root
WHERE root.id=? AND (SELECT COUNT(*) FROM graph)<65537
 AND EXISTS(SELECT 1 FROM graph leaf WHERE leaf.source_scope NOT LIKE 'checkpoint:%')
 AND NOT EXISTS(SELECT 1 FROM graph g WHERE NOT (
  (g.source_scope NOT LIKE 'checkpoint:%' AND EXISTS(
   SELECT 1 FROM compaction_live_sources s
   WHERE s.workspace_id=? AND s.source_scope=g.source_scope
    AND s.source_id=g.source_id AND s.source_version=g.source_version
  )) OR (g.source_scope LIKE 'checkpoint:%' AND EXISTS(
   SELECT 1 FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner
   WHERE p.id=g.source_id AND 'checkpoint:'||p.owner=g.source_scope
    AND p.identity_sha256=g.source_version AND p.format_version=1 AND c.workspace_id=?
    AND (p.status='applied' OR (p.status='retained' AND EXISTS(
     SELECT 1 FROM compaction_operation o WHERE o.id=p.operation_id AND o.status='completed')))
  ))
 )) LIMIT 1"#;

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
        "pioneer-source-lookups-{}.sqlite",
        uuid::Uuid::new_v4()
    )));
    let (store, _writer) = open(&file.0).await;
    let db = store.database_connection();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',1,1)",
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('other','other',1,0)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('root-thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('source-thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('foreign-thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('other-thread','other','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('root-turn','root-thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('source-turn','source-thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('other-source-turn','source-thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('foreign-turn','foreign-thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('other-turn','other-thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','root-thread','root-thread','root-turn','agent','running','Task','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('basis-run','task','basis-run',1,1,'succeeded','agent')",
        "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) VALUES ('basis-run','task','ws','source-thread','[]',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,created_at) VALUES ('context-source','source-turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,created_at,updated_at) VALUES ('item-source','source-turn','item-source','command_execution','completed','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('event-source','source-thread','source-turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('input-source','source-turn',0,'text','fixture','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('foreign-event','foreign-thread','foreign-turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('other-event','other-thread','other-turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO compaction_context(workspace_id,thread_id,owner,format_version) VALUES ('ws','root-thread','root-owner',1)",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES ('root-operation','root-owner','root-fixture','completed','{}',1)",
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES ('root-checkpoint','root-operation','root-owner',0,'root summary','root-version','{}',0,1,'applied')",
        "INSERT INTO compaction_context(workspace_id,thread_id,owner,format_version) VALUES ('ws','source-thread','source-owner',1)",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES ('source-operation','source-owner','source-fixture','completed','{}',1)",
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status) SELECT 'source-checkpoint','source-operation','source-owner',0,'source summary','source-version','{}',COALESCE((SELECT version FROM compaction_projection_epoch WHERE thread_id='source-thread'),0),1,'applied'",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    db.execute_unprepared("DELETE FROM compaction_task_basis_revision WHERE run_id='basis-run'")
        .await
        .unwrap();
    set_root_leaf(&db, &canonical_branches()[2].branch.source).await;
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
                scope: "checkpoint:source-owner".into(),
                id: "source-checkpoint".into(),
                version: "source-version".into(),
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

fn root_reference() -> SourceRef {
    SourceRef {
        scope: "checkpoint:root-owner".into(),
        id: "root-checkpoint".into(),
        version: "root-version".into(),
    }
}

async fn set_root_leaf(db: &SqliteDatabase, source: &SourceRef) {
    db.execute_unprepared("DELETE FROM compaction_coverage WHERE checkpoint_id='root-checkpoint'")
        .await
        .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('root-checkpoint',?,?,?)",
        [
            source.scope.clone().into(),
            source.id.clone().into(),
            source.version.clone().into(),
        ],
    ))
    .await
    .unwrap();
}

fn legacy_sources_statement(payload: String, workspace: &str, thread: &str) -> Statement {
    sqlite_specific_sql(
        LEGACY_SOURCES_CURRENT_SQL,
        [
            payload.into(),
            workspace.into(),
            thread.into(),
            workspace.into(),
            thread.into(),
        ],
    )
}

fn legacy_checkpoint_source_statement(workspace: &str, thread: &str) -> Statement {
    sqlite_specific_sql(
        LEGACY_CHECKPOINT_SOURCE_SQL,
        [
            "root-checkpoint".into(),
            workspace.into(),
            thread.into(),
            "root-checkpoint".into(),
            workspace.into(),
            workspace.into(),
        ],
    )
}

fn legacy_checkpoint_payload_statement(
    workspace: &str,
    thread: &str,
    reference: &SourceRef,
) -> Statement {
    sqlite_specific_sql(
        LEGACY_CHECKPOINT_PAYLOAD_SQL,
        [
            reference.id.clone().into(),
            reference.scope.clone().into(),
            reference.version.clone().into(),
            workspace.into(),
            thread.into(),
            reference.id.clone().into(),
            workspace.into(),
            workspace.into(),
        ],
    )
}

async fn assert_sources(
    db: &SqliteDatabase,
    workspace: &str,
    thread: &str,
    sources: &[SourceRef],
    expected: bool,
    case: &str,
) {
    let payload = serde_json::to_string(sources).unwrap();
    let snapshot = db.begin_read().await.unwrap();
    let actual_matched = snapshot
        .query_one_raw(compaction_sources_current_statement(
            payload.clone(),
            workspace,
            thread,
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "matched")
        .unwrap();
    let actual = compaction_sources_current(&snapshot, workspace, thread, sources)
        .await
        .unwrap();
    let legacy_matched = snapshot
        .query_one_raw(legacy_sources_statement(payload, workspace, thread))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "matched")
        .unwrap();
    let legacy = legacy_matched == sources.len() as i64;
    assert_eq!(
        actual_matched, legacy_matched,
        "matched count diverged from oracle: {case}"
    );
    assert_eq!(actual, legacy, "sources query diverged from oracle: {case}");
    assert_eq!(actual, expected, "unexpected sources result: {case}");
    snapshot.rollback().await.unwrap();
}

async fn assert_graph_results(
    db: &SqliteDatabase,
    workspace: &str,
    thread: &str,
    reference: &SourceRef,
    expected_source: bool,
    expected_payload: bool,
    case: &str,
) {
    let snapshot = db.begin_read().await.unwrap();
    let actual_source =
        compaction_checkpoint_source(&snapshot, workspace, thread, "root-checkpoint")
            .await
            .unwrap();
    let legacy_source = snapshot
        .query_one_raw(legacy_checkpoint_source_statement(workspace, thread))
        .await
        .unwrap()
        .map(|row| SourceRef {
            scope: row.try_get("", "source_scope").unwrap(),
            id: row.try_get("", "source_id").unwrap(),
            version: row.try_get("", "source_version").unwrap(),
        });
    assert_eq!(
        actual_source, legacy_source,
        "source oracle mismatch: {case}"
    );

    let actual_payload = snapshot
        .query_one_raw(compaction_reference_checkpoint_payload_statement(
            workspace, thread, reference,
        ))
        .await
        .unwrap()
        .map(|row| row.try_get::<String>("", "fragment").unwrap());
    let legacy_payload = snapshot
        .query_one_raw(legacy_checkpoint_payload_statement(
            workspace, thread, reference,
        ))
        .await
        .unwrap()
        .map(|row| row.try_get::<String>("", "fragment").unwrap());
    assert_eq!(
        actual_payload, legacy_payload,
        "payload oracle mismatch: {case}"
    );
    assert_eq!(
        actual_source.is_some(),
        expected_source,
        "unexpected source result: {case}"
    );
    if expected_source {
        assert_eq!(
            actual_source.as_ref(),
            Some(&root_reference()),
            "unexpected returned checkpoint reference: {case}"
        );
    }
    assert_eq!(
        actual_payload.as_deref(),
        expected_payload.then_some("root summary"),
        "unexpected payload result: {case}"
    );
    snapshot.rollback().await.unwrap();
}

async fn assert_graph(
    db: &SqliteDatabase,
    workspace: &str,
    thread: &str,
    reference: &SourceRef,
    expected: bool,
    case: &str,
) {
    assert_graph_results(db, workspace, thread, reference, expected, expected, case).await;
}

async fn assert_all(db: &SqliteDatabase, source: &SourceRef, expected: bool, case: &str) {
    set_root_leaf(db, source).await;
    assert_sources(
        db,
        "ws",
        "source-thread",
        std::slice::from_ref(source),
        expected,
        case,
    )
    .await;
    assert_graph(db, "ws", "root-thread", &root_reference(), expected, case).await;
}

#[tokio::test]
async fn source_lookup_queries_match_legacy_for_all_source_types_and_identity_fields() {
    let fixture = fixture().await;
    let db = fixture.db();
    assert_sources(&db, "ws", "source-thread", &[], true, "empty batch").await;

    for branch in branches() {
        assert_sources(
            &db,
            "ws",
            "source-thread",
            std::slice::from_ref(&branch.source),
            true,
            branch.name,
        )
        .await;
        assert_sources(
            &db,
            "ws",
            "source-thread",
            &[branch.source.clone(), branch.source.clone()],
            true,
            &format!("{} duplicate", branch.name),
        )
        .await;
        for (field, wrong) in [
            ("scope", "wrong:scope"),
            ("id", "wrong-id"),
            ("version", "wrong-version"),
        ] {
            let mut changed = branch.source.clone();
            match field {
                "scope" => changed.scope = wrong.into(),
                "id" => changed.id = wrong.into(),
                "version" => changed.version = wrong.into(),
                _ => unreachable!(),
            }
            assert_sources(
                &db,
                "ws",
                "source-thread",
                &[changed],
                false,
                &format!("{} wrong {field}", branch.name),
            )
            .await;
        }
        assert_sources(
            &db,
            "other",
            "source-thread",
            std::slice::from_ref(&branch.source),
            false,
            &format!("{} wrong workspace", branch.name),
        )
        .await;
        assert_sources(
            &db,
            "ws",
            "root-thread",
            std::slice::from_ref(&branch.source),
            false,
            &format!("{} wrong thread", branch.name),
        )
        .await;
    }

    let mut partial = branches()
        .into_iter()
        .map(|branch| branch.source)
        .collect::<Vec<_>>();
    partial[2].version = "event-revision:99".into();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        &partial,
        false,
        "partially stale batch",
    )
    .await;

    let too_many =
        vec![canonical_branches().remove(2).branch.source; SOURCE_PAGE_ROWS as usize + 1];
    assert!(
        compaction_sources_current(&db, "ws", "source-thread", &too_many)
            .await
            .is_err()
    );
    let oversized = SourceRef {
        scope: "event:source-turn".into(),
        id: "event-source".into(),
        version: "x".repeat(SOURCE_PAGE_BYTES),
    };
    assert!(
        compaction_sources_current(&db, "ws", "source-thread", &[oversized])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn checkpoint_graph_leaf_identity_fields_match_legacy_for_all_five_leaf_types() {
    for branch in branches()
        .into_iter()
        .filter(|branch| branch.name != "checkpoint")
    {
        let fixture = fixture().await;
        let db = fixture.db();
        assert_all(
            &db,
            &branch.source,
            true,
            &format!("{} graph baseline", branch.name),
        )
        .await;
        for field in ["scope", "id", "version"] {
            let mut changed = branch.source.clone();
            match field {
                "scope" => changed.scope = "wrong:scope".into(),
                "id" => changed.id = "wrong-id".into(),
                "version" => changed.version = "wrong-version".into(),
                _ => unreachable!(),
            }
            set_root_leaf(&db, &changed).await;
            assert_graph(
                &db,
                "ws",
                "root-thread",
                &root_reference(),
                false,
                &format!("{} graph wrong {field}", branch.name),
            )
            .await;
        }
    }
}

#[tokio::test]
async fn checkpoint_graph_leaf_workspace_is_independent_for_all_five_leaf_types() {
    for branch in branches()
        .into_iter()
        .filter(|branch| branch.name != "checkpoint")
    {
        let fixture = fixture().await;
        let db = fixture.db();
        set_root_leaf(&db, &branch.source).await;
        assert_graph(
            &db,
            "ws",
            "root-thread",
            &root_reference(),
            true,
            &format!(
                "{} leaf in another thread of the root workspace",
                branch.name
            ),
        )
        .await;

        db.execute_unprepared("UPDATE thread SET workspace_id='other' WHERE id='source-thread'")
            .await
            .unwrap();
        if branch.name == "task-basis" {
            let snapshot_workspace = db
                .query_one_raw(Statement::from_string(
                    DbBackend::Sqlite,
                    "SELECT workspace_id FROM task_run_conversation_snapshot WHERE run_id='basis-run'",
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get::<String>("", "workspace_id")
                .unwrap();
            assert_eq!(snapshot_workspace, "ws");
        }
        assert_graph(
            &db,
            "ws",
            "root-thread",
            &root_reference(),
            false,
            &format!("{} leaf thread belongs to another workspace", branch.name),
        )
        .await;

        db.execute_unprepared("UPDATE thread SET workspace_id='ws' WHERE id='source-thread'")
            .await
            .unwrap();
        assert_graph(
            &db,
            "ws",
            "root-thread",
            &root_reference(),
            true,
            &format!("{} leaf workspace restored", branch.name),
        )
        .await;
    }
}

#[tokio::test]
async fn canonical_liveness_predicates_are_independent_in_batch_and_graph_queries() {
    for canonical in canonical_branches() {
        let fixture = fixture().await;
        let db = fixture.db();
        assert_all(
            &db,
            &canonical.branch.source,
            true,
            &format!("{} baseline", canonical.branch.name),
        )
        .await;

        db.execute_unprepared(&format!(
            "UPDATE {} SET present=0 WHERE source_id='{}'",
            canonical.revision_table, canonical.branch.source.id
        ))
        .await
        .unwrap();
        assert_all(
            &db,
            &canonical.branch.source,
            false,
            &format!("{} present", canonical.branch.name),
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
        assert_all(
            &db,
            &canonical.branch.source,
            false,
            &format!("{} revision", canonical.branch.name),
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
        assert_all(
            &db,
            &canonical.branch.source,
            false,
            &format!("{} physical row", canonical.branch.name),
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
        let mut mismatched = canonical.branch.source.clone();
        mismatched.scope = mismatched
            .scope
            .strip_suffix("source-turn")
            .unwrap()
            .to_owned()
            + "other-source-turn";
        assert_all(
            &db,
            &mismatched,
            false,
            &format!("{} canonical/revision turn", canonical.branch.name),
        )
        .await;
        db.execute_unprepared(&format!(
            "UPDATE {} SET turn_id='source-turn' WHERE source_id='{}'",
            canonical.revision_table, canonical.branch.source.id
        ))
        .await
        .unwrap();
        assert_all(
            &db,
            &canonical.branch.source,
            true,
            &format!("{} restored", canonical.branch.name),
        )
        .await;
    }
}

async fn update_task_basis_history(
    db: &SqliteDatabase,
    basis: &mut SourceRef,
    history_json: &str,
    expected_revision: i64,
) {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE task_run_conversation_snapshot SET history_json=? WHERE run_id='basis-run'",
        [history_json.into()],
    ))
    .await
    .unwrap();
    let revision = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT revision FROM compaction_task_basis_revision WHERE run_id='basis-run'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "revision")
        .unwrap();
    assert_eq!(revision, expected_revision);
    basis.version = format!("task-basis-revision:{revision}");
}

#[tokio::test]
async fn task_basis_revision_and_original_json_array_semantics_match_legacy() {
    let fixture = fixture().await;
    let db = fixture.db();
    let mut basis = branches().remove(5).source;
    assert_all(&db, &basis, true, "fallback revision one").await;
    db.execute_unprepared(
        "INSERT INTO compaction_task_basis_revision(run_id,revision) VALUES ('basis-run',2)",
    )
    .await
    .unwrap();
    basis.version = "task-basis-revision:2".into();
    assert_all(&db, &basis, true, "explicit revision").await;
    let mut stale = basis.clone();
    stale.version = "task-basis-revision:1".into();
    assert_all(&db, &stale, false, "stale revision").await;

    update_task_basis_history(&db, &mut basis, "{}", 3).await;
    assert_all(&db, &basis, false, "non-array JSON").await;
    update_task_basis_history(&db, &mut basis, "   []", 4).await;
    assert_all(&db, &basis, true, "spaces are trimmed").await;
    update_task_basis_history(&db, &mut basis, "\t[]", 5).await;
    assert_all(&db, &basis, false, "tab is not trimmed").await;
}

async fn projection_epoch(db: &SqliteDatabase) -> i64 {
    db.query_one_raw(Statement::from_string(
        DbBackend::Sqlite,
        "SELECT COALESCE((SELECT version FROM compaction_projection_epoch WHERE thread_id='source-thread'),0) AS version",
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get("", "version")
    .unwrap()
}

#[tokio::test]
async fn sources_current_preserves_the_two_distinct_checkpoint_predicates() {
    let fixture = fixture().await;
    let db = fixture.db();
    let checkpoint = branches().remove(4).source;
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        true,
        "baseline checkpoint",
    )
    .await;

    let epoch = projection_epoch(&db).await;
    db.execute_unprepared(
        "DELETE FROM compaction_projection_epoch WHERE thread_id='source-thread'",
    )
    .await
    .unwrap();
    db.execute_unprepared("UPDATE compaction_checkpoint SET format_version=2,projection_version=0 WHERE id='source-checkpoint'").await.unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        true,
        "live branch uses the absent-epoch fallback zero",
    )
    .await;
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_projection_epoch(thread_id,version) VALUES ('source-thread',?)",
        [epoch.into()],
    ))
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite, "UPDATE compaction_checkpoint SET format_version=2,projection_version=? WHERE id='source-checkpoint'", [epoch.into()])).await.unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        true,
        "live branch does not require format one",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='retained' WHERE id='source-checkpoint'",
    )
    .await
    .unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        true,
        "live retained checkpoint with completed operation",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_operation SET status='running' WHERE id='source-operation'",
    )
    .await
    .unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        false,
        "live retained checkpoint with unfinished operation",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_operation SET status='completed' WHERE id='source-operation'",
    )
    .await
    .unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        true,
        "live retained checkpoint restored to completed",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='pending' WHERE id='source-checkpoint'",
    )
    .await
    .unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        false,
        "live checkpoint with invalid status",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='retained' WHERE id='source-checkpoint'",
    )
    .await
    .unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        true,
        "live retained checkpoint restored after invalid status",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_operation SET status='running' WHERE id='source-operation'",
    )
    .await
    .unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        false,
        "live retained checkpoint is rejected before applied transition",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='applied' WHERE id='source-checkpoint'",
    )
    .await
    .unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        true,
        "live applied checkpoint ignores unfinished operation",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_operation SET status='completed' WHERE id='source-operation'",
    )
    .await
    .unwrap();

    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite, "INSERT INTO compaction_projection_epoch(thread_id,version) VALUES ('source-thread',?) ON CONFLICT(thread_id) DO UPDATE SET version=excluded.version", [(epoch + 1).into()])).await.unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        false,
        "format two with stale epoch",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET format_version=1 WHERE id='source-checkpoint'",
    )
    .await
    .unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        true,
        "immutable applied checkpoint survives epoch change",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='retained' WHERE id='source-checkpoint'",
    )
    .await
    .unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        true,
        "immutable retained checkpoint with completed operation",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_operation SET status='running' WHERE id='source-operation'",
    )
    .await
    .unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        false,
        "immutable retained checkpoint with unfinished operation",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='applied' WHERE id='source-checkpoint'",
    )
    .await
    .unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        true,
        "immutable applied checkpoint ignores unfinished operation",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_operation SET status='completed' WHERE id='source-operation'",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='retained' WHERE id='source-checkpoint'",
    )
    .await
    .unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        true,
        "immutable retained checkpoint restored to completed",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='pending' WHERE id='source-checkpoint'",
    )
    .await
    .unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        false,
        "immutable checkpoint with invalid status",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='retained' WHERE id='source-checkpoint'",
    )
    .await
    .unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        true,
        "immutable retained checkpoint restored after invalid status",
    )
    .await;
    db.execute_unprepared("DELETE FROM compaction_operation WHERE id='source-operation'")
        .await
        .unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        false,
        "retained checkpoint is absent after its operation is removed",
    )
    .await;
}

async fn install_intermediate(db: &SqliteDatabase) {
    for sql in [
        "INSERT INTO compaction_context(workspace_id,thread_id,owner,format_version) VALUES ('ws','foreign-thread','mid-owner',1)",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES ('mid-operation','mid-owner','mid-fixture','completed','{}',1)",
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES ('mid-checkpoint','mid-operation','mid-owner',0,'mid summary','mid-version','{}',0,1,'applied')",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
}

fn mid_reference() -> SourceRef {
    SourceRef {
        scope: "checkpoint:mid-owner".into(),
        id: "mid-checkpoint".into(),
        version: "mid-version".into(),
    }
}

#[tokio::test]
async fn checkpoint_graph_traversal_status_identity_previous_cycles_and_foreign_leaves_match_legacy()
 {
    let fixture = fixture().await;
    let db = fixture.db();
    let event = canonical_branches().remove(2).branch.source;
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        true,
        "baseline",
    )
    .await;
    assert_graph(
        &db,
        "other",
        "root-thread",
        &root_reference(),
        false,
        "root workspace",
    )
    .await;
    assert_graph(
        &db,
        "ws",
        "source-thread",
        &root_reference(),
        false,
        "root thread",
    )
    .await;
    let mut wrong_root = root_reference();
    wrong_root.scope = "checkpoint:wrong".into();
    assert_graph_results(
        &db,
        "ws",
        "root-thread",
        &wrong_root,
        true,
        false,
        "root scope",
    )
    .await;
    let mut wrong_root = root_reference();
    wrong_root.version = "wrong".into();
    assert_graph_results(
        &db,
        "ws",
        "root-thread",
        &wrong_root,
        true,
        false,
        "root version",
    )
    .await;
    let mut wrong_root = root_reference();
    wrong_root.id = "wrong".into();
    assert_graph_results(
        &db,
        "ws",
        "root-thread",
        &wrong_root,
        true,
        false,
        "root id",
    )
    .await;

    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='retained' WHERE id='root-checkpoint'",
    )
    .await
    .unwrap();
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        true,
        "retained completed",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_operation SET status='running' WHERE id='root-operation'",
    )
    .await
    .unwrap();
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        false,
        "retained unfinished",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='pending' WHERE id='root-checkpoint'",
    )
    .await
    .unwrap();
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        false,
        "invalid root status",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_operation SET status='completed' WHERE id='root-operation'",
    )
    .await
    .unwrap();
    db.execute_unprepared("UPDATE compaction_checkpoint SET status='applied',format_version=2 WHERE id='root-checkpoint'").await.unwrap();
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        false,
        "root format",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET format_version=1 WHERE id='root-checkpoint'",
    )
    .await
    .unwrap();

    install_intermediate(&db).await;
    set_root_leaf(&db, &mid_reference()).await;
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite, "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('mid-checkpoint',?,?,?)", [event.scope.clone().into(), event.id.clone().into(), event.version.clone().into()])).await.unwrap();
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        true,
        "multi-level foreign-thread leaf",
    )
    .await;
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite, "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('root-checkpoint',?,?,?)", [event.scope.clone().into(), event.id.clone().into(), event.version.clone().into()])).await.unwrap();
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        true,
        "shared dependency deduplicated",
    )
    .await;

    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='pending' WHERE id='mid-checkpoint'",
    )
    .await
    .unwrap();
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        false,
        "changed intermediate status",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='applied' WHERE id='mid-checkpoint'",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET format_version=2 WHERE id='mid-checkpoint'",
    )
    .await
    .unwrap();
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        false,
        "changed intermediate format",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET format_version=1 WHERE id='mid-checkpoint'",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET identity_sha256='changed' WHERE id='mid-checkpoint'",
    )
    .await
    .unwrap();
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        false,
        "changed intermediate identity",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET identity_sha256='mid-version' WHERE id='mid-checkpoint'",
    )
    .await
    .unwrap();

    db.execute_unprepared("DELETE FROM compaction_coverage WHERE checkpoint_id='root-checkpoint'")
        .await
        .unwrap();
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET previous='mid-checkpoint' WHERE id='root-checkpoint'",
    )
    .await
    .unwrap();
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        true,
        "previous-only ancestry",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET previous='root-checkpoint' WHERE id='mid-checkpoint'",
    )
    .await
    .unwrap();
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        true,
        "cycle terminates by UNION deduplication",
    )
    .await;
    db.execute_unprepared("DELETE FROM compaction_coverage WHERE checkpoint_id='mid-checkpoint'")
        .await
        .unwrap();
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        false,
        "graph without canonical leaf",
    )
    .await;

    db.execute_unprepared("UPDATE compaction_checkpoint SET previous=NULL WHERE id IN ('root-checkpoint','mid-checkpoint')").await.unwrap();
    set_root_leaf(
        &db,
        &SourceRef {
            scope: "event:foreign-turn".into(),
            id: "foreign-event".into(),
            version: "event-revision:1".into(),
        },
    )
    .await;
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        true,
        "foreign thread in same workspace",
    )
    .await;
    set_root_leaf(
        &db,
        &SourceRef {
            scope: "event:other-turn".into(),
            id: "other-event".into(),
            version: "event-revision:1".into(),
        },
    )
    .await;
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        false,
        "leaf in other workspace",
    )
    .await;

    set_root_leaf(&db, &mid_reference()).await;
    db.execute_unprepared("DELETE FROM compaction_checkpoint WHERE id='mid-checkpoint'")
        .await
        .unwrap();
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        false,
        "missing intermediate checkpoint",
    )
    .await;
}

#[tokio::test]
async fn checkpoint_graph_preserves_the_65536_65537_boundary() {
    let fixture = fixture().await;
    let db = fixture.db();
    db.execute_unprepared(
        r#"WITH RECURSIVE sequence(n) AS (
             SELECT 1 UNION ALL SELECT n+1 FROM sequence WHERE n<65535
           )
           INSERT INTO compaction_checkpoint(id,operation_id,owner,previous,portion,summary,identity_sha256,selection,projection_version,format_version,status)
           SELECT printf('limit-%05d',n),'root-operation','root-owner',
                  CASE WHEN n<65535 THEN printf('limit-%05d',n+1) END,
                  n,'limit',printf('limit-version-%05d',n),'{}',0,1,'applied'
           FROM sequence"#,
    )
    .await
    .unwrap();
    let leaf = canonical_branches().remove(2).branch.source;
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('limit-65535',?,?,?)",
        [leaf.scope.into(), leaf.id.into(), leaf.version.into()],
    ))
    .await
    .unwrap();
    db.execute_unprepared("DELETE FROM compaction_coverage WHERE checkpoint_id='root-checkpoint'")
        .await
        .unwrap();
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET previous='limit-00002' WHERE id='root-checkpoint'",
    )
    .await
    .unwrap();
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        true,
        "65536 graph rows",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET previous='limit-00001' WHERE id='root-checkpoint'",
    )
    .await
    .unwrap();
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        false,
        "65537 graph rows",
    )
    .await;
}

async fn enable_zstd(db: &SqliteDatabase) {
    for table in ["turn_llm_context", "turn_item", "turn_event", "turn_input"] {
        db.query_one_write_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT zstd_enable_transparent(?)",
            [serde_json::json!({"table":table,"column":"payload","compression_level":3,"dict_chooser":"'[nodict]'"}).to_string().into()],
        ))
        .await
        .unwrap();
    }
}

async fn compress_canonical_payloads(db: &SqliteDatabase) {
    for table in ["turn_llm_context", "turn_item", "turn_event", "turn_input"] {
        let rows = db
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                format!("SELECT id,payload FROM _{table}_zstd WHERE _payload_dict IS NULL"),
            ))
            .await
            .unwrap();
        let values = rows
            .into_iter()
            .map(|row| {
                (
                    row.try_get::<String>("", "id").unwrap(),
                    row.try_get::<String>("", "payload").unwrap(),
                )
            })
            .collect::<Vec<_>>();
        for (id, payload) in values {
            let compressed =
                pioneer_sqlite::zstd::compress_column_value(payload.as_bytes(), 3, None).unwrap();
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite, format!("UPDATE _{table}_zstd SET payload=?,_payload_dict=-1 WHERE id=? AND _payload_dict IS NULL"), [compressed.into(), id.into()])).await.unwrap();
        }
    }
}

#[tokio::test]
async fn source_lookup_queries_match_legacy_with_real_zstd_payload_rows() {
    let fixture = fixture().await;
    let db = fixture.db();
    enable_zstd(&db).await;
    compress_canonical_payloads(&db).await;
    for canonical in canonical_branches() {
        assert_all(
            &db,
            &canonical.branch.source,
            true,
            &format!("zstd {}", canonical.branch.name),
        )
        .await;
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
        rhs.trim() == "?"
            && lhs
                .trim()
                .rsplit('.')
                .next()
                .unwrap_or_default()
                .trim_matches(|character| matches!(character, '"' | '`' | '[' | ']'))
                == column
    })
}

fn assert_exact_search(plan: &[String], subjects: &[&str], column: &str) {
    assert!(
        plan.iter().any(|detail| subjects
            .iter()
            .any(
                |subject| plan_operation_targets_subject(detail, "SEARCH", subject)
                    && search_has_exact_equality(detail, column)
            )),
        "expected exact {column} lookup for {subjects:?}: {plan:#?}"
    );
    assert!(
        !plan.iter().any(|detail| subjects
            .iter()
            .any(|subject| plan_operation_targets_subject(detail, "SCAN", subject))),
        "unexpected scan of {subjects:?}: {plan:#?}"
    );
}

async fn explain(db: &SqliteDatabase, mut statement: Statement) -> Vec<String> {
    assert!(!statement.sql.contains("compaction_live_sources"));
    statement.sql = format!("EXPLAIN QUERY PLAN {}", statement.sql);
    db.query_all_raw(statement)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get("", "detail").unwrap())
        .collect()
}

async fn assert_production_plans(compressed: bool) {
    let fixture = fixture().await;
    let db = fixture.db();
    if compressed {
        enable_zstd(&db).await;
        compress_canonical_payloads(&db).await;
    }
    let sources = branches()
        .into_iter()
        .map(|branch| branch.source)
        .collect::<Vec<_>>();
    let plans = [
        explain(
            &db,
            compaction_sources_current_statement(
                serde_json::to_string(&sources).unwrap(),
                "ws",
                "source-thread",
            ),
        )
        .await,
        explain(
            &db,
            compaction_checkpoint_source_statement("ws", "root-thread", "root-checkpoint"),
        )
        .await,
        explain(
            &db,
            compaction_reference_checkpoint_payload_statement(
                "ws",
                "root-thread",
                &root_reference(),
            ),
        )
        .await,
    ];
    for plan in plans {
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
        assert_exact_search(&plan, &["basis"], "run_id");
    }
}

#[test]
fn query_plan_key_recognition_does_not_confuse_id_suffixes_or_scans() {
    assert!(search_has_exact_equality(
        "SEARCH source USING INDEX x (turn_id=? AND id=?)",
        "id"
    ));
    assert!(!search_has_exact_equality(
        "SEARCH source USING INDEX x (turn_id=? AND owner_id=? AND source_id=?)",
        "id"
    ));
    assert!(search_has_exact_equality(
        "SEARCH source USING INDEX x (source_id=?)",
        "source_id"
    ));
    assert!(plan_operation_targets_subject(
        "SCAN source",
        "SCAN",
        "source"
    ));
}

#[tokio::test]
async fn source_lookup_plans_use_exact_keys_for_plain_and_zstd_storage() {
    assert_production_plans(false).await;
    assert_production_plans(true).await;
}

#[tokio::test]
async fn checkpoint_payload_public_path_returns_the_exact_summary() {
    let fixture = fixture().await;
    assert_eq!(
        compaction_reference_payload(&fixture.store, "ws", "root-thread", &root_reference())
            .await
            .unwrap()
            .as_deref(),
        Some("root summary")
    );
}
