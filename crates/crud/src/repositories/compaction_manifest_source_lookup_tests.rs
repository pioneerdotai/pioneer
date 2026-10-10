use super::*;
use crate::CrudStore;
use crate::repositories::compaction::{
    arm_checkpoint_projection_page_test_hook, checkpoint_projection_page_sizes_statement,
    checkpoint_projection_page_statement, checkpoint_projection_page_test_pause,
    observe_checkpoint_projection_page_test_reads,
};
use migration::{Migrator, MigratorTrait};
use pioneer_compaction::runner::{RunnerPhase, RunnerState, SourceCursor};
use pioneer_compaction::{ModelSelection, Transport};
use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DbBackend, Statement, TransactionTrait, Value,
};
use std::path::{Path, PathBuf};
use std::time::Duration;

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
    writer: SqliteWriteExecutor,
    _file: TestFile,
}

impl Fixture {
    fn db(&self) -> SqliteDatabase {
        self.store.database_connection()
    }

    fn arm_publication_hook(
        &self,
        operation: &str,
        phase: PublicationTestPause,
    ) -> PublicationTestHookHandle {
        arm_publication_test_hook(&self.store, operation, phase)
    }
}

async fn open(path: &Path) -> (CrudStore, SqliteWriteExecutor) {
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    options.max_connections(1).min_connections(1);
    let writer_connection = Database::connect(options).await.unwrap();
    let writer = SqliteWriteExecutor::new(writer_connection);
    writer
        .apply_pragmas(SqliteWriteClass::Maintenance)
        .await
        .unwrap();
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();

    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=ro", path.display()));
    options.max_connections(1).min_connections(1);
    let reader = Database::connect(options).await.unwrap();
    let journal_mode: String = reader
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA journal_mode".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "journal_mode")
        .unwrap();
    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    reader
        .execute_unprepared("PRAGMA query_only=ON")
        .await
        .unwrap();
    let store = CrudStore::new(SqliteDatabase::from_executor(reader, writer.clone()))
        .with_maintenance_access();
    // `apply_pragmas` is the supported way to put this fixture in WAL mode,
    // but it also applies the production `foreign_keys=OFF` setting. Restore
    // the fixture's pre-existing FK semantics so cascade assertions keep
    // exercising the same database behavior they did before WAL was explicit.
    store
        .database_connection()
        .execute_unprepared("PRAGMA foreign_keys=ON")
        .await
        .unwrap();
    (store, writer)
}

async fn fixture() -> Fixture {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let file = TestFile(std::env::temp_dir().join(format!(
        "pioneer-manifest-source-lookups-{}.sqlite",
        uuid::Uuid::new_v4()
    )));
    let (store, writer) = open(&file.0).await;
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
        "WITH frozen_root_fixture(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) AS (VALUES ('basis-run','task','ws','source-thread','[]',CURRENT_TIMESTAMP)) INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,frozen_manifest_id) SELECT run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,CASE WHEN json_valid(history_json) THEN CASE WHEN json_type(history_json)='object' THEN json_extract(history_json,'$.manifest_id') ELSE NULL END ELSE NULL END FROM frozen_root_fixture",
        "INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,created_at) VALUES ('context-source','source-turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,created_at,updated_at) VALUES ('item-source','source-turn','item-source','command_execution','completed','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('event-source','source-thread','source-turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('input-source','source-turn',0,'text','fixture','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('foreign-event','foreign-thread','foreign-turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('other-event','other-thread','other-turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO compaction_context(workspace_id,thread_id,owner,format_version) VALUES ('ws','root-thread','root-owner',1)",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES ('manifest-operation','root-owner','manifest-fixture','running','{\"plan\":{\"coverage_domain\":\"own_contribution\"},\"source_epochs\":{\"root-thread\":0,\"source-thread\":0,\"foreign-thread\":0}}',1)",
        "INSERT INTO compaction_context(workspace_id,thread_id,owner,format_version) VALUES ('ws','source-thread','source-owner',1)",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES ('source-operation','source-owner','source-fixture','completed','{}',1)",
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES ('source-checkpoint','source-operation','source-owner',0,'source summary','source-version','{}',0,1,'applied')",
        "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('source-checkpoint','event:source-turn','event-source','event-revision:1')",
        "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES ('source-operation',0,0,0,'source-thread','event:source-turn','event-source','event-revision:1')",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    db.execute_unprepared("DELETE FROM compaction_task_basis_revision WHERE run_id='basis-run'")
        .await
        .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_operation SET snapshot=?1 WHERE id='source-operation'",
        [raw_fixture_snapshot(
            "source-operation",
            "source-owner",
            source_cases()
                .into_iter()
                .filter(|case| case.name != "checkpoint")
                .map(|case| SourceRef {
                    scope: case.scope.into(),
                    id: case.id.into(),
                    version: case.version.into(),
                })
                .collect(),
        )
        .into()],
    ))
    .await
    .unwrap();
    Fixture {
        store,
        writer,
        _file: file,
    }
}

#[derive(Clone)]
struct SourceCase {
    name: &'static str,
    thread: &'static str,
    scope: &'static str,
    id: &'static str,
    version: &'static str,
}

struct CanonicalCase {
    source: SourceCase,
    canonical_table: &'static str,
    revision_table: &'static str,
}

#[tokio::test]
async fn checkpoint_edges_require_exact_unambiguous_historical_ownership() {
    let fixture = fixture().await;
    let db = fixture.db();
    assert!(
        fixture
            .store
            .compaction_checkpoint_edges("source-checkpoint")
            .await
            .unwrap()
            .is_some()
    );
    db.execute_unprepared("INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES ('source-operation',1,1,0,'foreign-thread','event:source-turn','event-source','event-revision:1')")
        .await
        .unwrap();
    assert!(
        fixture
            .store
            .compaction_checkpoint_edges("source-checkpoint")
            .await
            .is_err(),
        "equal row counts must not hide ambiguous historical ownership"
    );
}

fn source_cases() -> [SourceCase; 6] {
    [
        SourceCase {
            name: "context",
            thread: "source-thread",
            scope: "context:source-turn",
            id: "context-source",
            version: "revision:1",
        },
        SourceCase {
            name: "item",
            thread: "source-thread",
            scope: "item:source-turn",
            id: "item-source",
            version: "item-revision:1",
        },
        SourceCase {
            name: "event",
            thread: "source-thread",
            scope: "event:source-turn",
            id: "event-source",
            version: "event-revision:1",
        },
        SourceCase {
            name: "input",
            thread: "source-thread",
            scope: "input:source-turn",
            id: "input-source",
            version: "input-revision:1",
        },
        SourceCase {
            name: "checkpoint",
            thread: "source-thread",
            scope: "checkpoint:source-owner",
            id: "source-checkpoint",
            version: "source-version",
        },
        SourceCase {
            name: "task-basis",
            thread: "source-thread",
            scope: "task-basis:basis-run",
            id: "basis-run",
            version: "task-basis-revision:1",
        },
    ]
}

fn canonical_cases() -> [CanonicalCase; 4] {
    let [context, item, event, input, _, _] = source_cases();
    [
        CanonicalCase {
            source: context,
            canonical_table: "turn_llm_context",
            revision_table: "compaction_source_revision",
        },
        CanonicalCase {
            source: item,
            canonical_table: "turn_item",
            revision_table: "compaction_item_revision",
        },
        CanonicalCase {
            source: event,
            canonical_table: "turn_event",
            revision_table: "compaction_event_revision",
        },
        CanonicalCase {
            source: input,
            canonical_table: "turn_input",
            revision_table: "compaction_input_revision",
        },
    ]
}

async fn set_manifest(db: &SqliteDatabase, source: &SourceCase, reference_only: bool) {
    db.execute_unprepared(
        "DELETE FROM compaction_manifest WHERE operation_id='manifest-operation'",
    )
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES ('manifest-operation',0,0,?,?,?,?,?)",
        [
            (if reference_only { 1_i64 } else { 0_i64 }).into(),
            source.thread.into(),
            source.scope.into(),
            source.id.into(),
            source.version.into(),
        ],
    ))
    .await
    .unwrap();
}

async fn assert_manifest_current(db: &SqliteDatabase, operation: &str, expected: bool, case: &str) {
    let actual = compaction_manifest_sources_current(db, operation)
        .await
        .unwrap();
    assert_eq!(actual, expected, "unexpected result: {case}");
}

#[tokio::test]
async fn manifest_source_lookup_checks_all_source_types() {
    let fixture = fixture().await;
    let db = fixture.db();

    assert_manifest_current(&db, "missing-operation", true, "missing operation").await;
    assert_manifest_current(&db, "manifest-operation", true, "empty manifest").await;

    for source in source_cases() {
        set_manifest(&db, &source, true).await;
        assert_manifest_current(&db, "manifest-operation", true, source.name).await;
        for (column, value) in [
            ("source_scope", "wrong:scope"),
            ("source_id", "wrong-id"),
            ("source_version", "wrong-version"),
            ("source_thread", "foreign-thread"),
        ] {
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                format!("UPDATE compaction_manifest SET {column}=? WHERE operation_id='manifest-operation'"),
                [value.into()],
            ))
            .await
            .unwrap();
            assert_manifest_current(
                &db,
                "manifest-operation",
                false,
                &format!("{} wrong {column}", source.name),
            )
            .await;
            set_manifest(&db, &source, true).await;
        }
    }

    let event = source_cases()[2].clone();
    set_manifest(&db, &event, true).await;
    db.execute_unprepared(
        "UPDATE compaction_event_revision SET present=0 WHERE source_id='event-source'",
    )
    .await
    .unwrap();
    assert_manifest_current(&db, "manifest-operation", false, "present=0").await;
    db.execute_unprepared(
        "UPDATE compaction_event_revision SET present=1,revision=2 WHERE source_id='event-source'",
    )
    .await
    .unwrap();
    assert_manifest_current(&db, "manifest-operation", false, "stale revision").await;
    db.execute_unprepared("UPDATE compaction_event_revision SET revision=1,turn_id='other-source-turn' WHERE source_id='event-source'")
        .await
        .unwrap();
    db.execute_unprepared("UPDATE compaction_manifest SET source_scope='event:other-source-turn' WHERE operation_id='manifest-operation'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "canonical revision turn mismatch",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_event_revision SET turn_id='source-turn' WHERE source_id='event-source'",
    )
    .await
    .unwrap();

    let checkpoint = source_cases()[4].clone();
    db.execute_unprepared("UPDATE compaction_operation SET snapshot='{\"plan\":{\"coverage_domain\":\"own_contribution\"},\"source_epochs\":{\"root-thread\":0,\"source-thread\":0,\"foreign-thread\":0}}' WHERE id='manifest-operation'")
        .await
        .unwrap();
    set_manifest(&db, &checkpoint, true).await;
    db.execute_unprepared("INSERT INTO compaction_projection_epoch(thread_id,version) VALUES ('source-thread',1) ON CONFLICT(thread_id) DO UPDATE SET version=version+1")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "immutable manifest checkpoint does not require projection epoch equality",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET format_version=2 WHERE id='source-checkpoint'",
    )
    .await
    .unwrap();
    assert_manifest_current(&db, "manifest-operation", false, "checkpoint format").await;
    db.execute_unprepared("UPDATE compaction_checkpoint SET format_version=1,status='pending' WHERE id='source-checkpoint'")
        .await
        .unwrap();
    assert_manifest_current(&db, "manifest-operation", false, "checkpoint status").await;

    let basis = source_cases()[5].clone();
    set_manifest(&db, &basis, true).await;
    db.execute_unprepared(
        "UPDATE task_run_conversation_snapshot SET history_json='  []',frozen_manifest_id=NULL WHERE run_id='basis-run'",
    )
    .await
    .unwrap();
    db.execute_unprepared("DELETE FROM compaction_task_basis_revision WHERE run_id='basis-run'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "task basis whitespace and fallback",
    )
    .await;
    db.execute_unprepared(
        "UPDATE task_run_conversation_snapshot SET history_json=' {}',frozen_manifest_id=NULL WHERE run_id='basis-run'",
    )
    .await
    .unwrap();
    db.execute_unprepared("DELETE FROM compaction_task_basis_revision WHERE run_id='basis-run'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "task basis requires JSON array",
    )
    .await;

    set_manifest(&db, &event, true).await;
    db.execute_unprepared("DELETE FROM turn_event WHERE id='event-source'")
        .await
        .unwrap();
    db.execute_unprepared("UPDATE compaction_event_revision SET turn_id='source-turn',revision=1,present=1 WHERE source_id='event-source'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "physical canonical row is required independently of present revision",
    )
    .await;

    let other_workspace = SourceCase {
        name: "other workspace",
        thread: "other-thread",
        scope: "event:other-turn",
        id: "other-event",
        version: "event-revision:1",
    };
    set_manifest(&db, &other_workspace, true).await;
    assert_manifest_current(&db, "manifest-operation", false, "workspace isolation").await;
}

#[tokio::test]
async fn every_canonical_branch_independently_checks_revision_and_physical_identity() {
    let fixture = fixture().await;
    let db = fixture.db();

    for canonical in canonical_cases() {
        set_manifest(&db, &canonical.source, true).await;
        db.execute_unprepared(&format!(
            "UPDATE {} SET present=0 WHERE source_id='{}'",
            canonical.revision_table, canonical.source.id
        ))
        .await
        .unwrap();
        assert_manifest_current(
            &db,
            "manifest-operation",
            false,
            &format!("{} present=0", canonical.source.name),
        )
        .await;
        db.execute_unprepared(&format!(
            "UPDATE {} SET present=1 WHERE source_id='{}'",
            canonical.revision_table, canonical.source.id
        ))
        .await
        .unwrap();

        db.execute_unprepared(&format!(
            "UPDATE {} SET revision=2 WHERE source_id='{}'",
            canonical.revision_table, canonical.source.id
        ))
        .await
        .unwrap();
        assert_manifest_current(
            &db,
            "manifest-operation",
            false,
            &format!("{} stale revision", canonical.source.name),
        )
        .await;
        db.execute_unprepared(&format!(
            "UPDATE {} SET revision=1 WHERE source_id='{}'",
            canonical.revision_table, canonical.source.id
        ))
        .await
        .unwrap();

        let changed_scope = format!("{}:other-source-turn", canonical.source.name);
        db.execute_unprepared(&format!(
            "UPDATE {} SET turn_id='other-source-turn' WHERE source_id='{}'",
            canonical.revision_table, canonical.source.id
        ))
        .await
        .unwrap();
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE compaction_manifest SET source_scope=? WHERE operation_id='manifest-operation'",
            [changed_scope.into()],
        ))
        .await
        .unwrap();
        assert_manifest_current(
            &db,
            "manifest-operation",
            false,
            &format!("{} canonical/revision turn mismatch", canonical.source.name),
        )
        .await;
        db.execute_unprepared(&format!(
            "UPDATE {} SET turn_id='source-turn' WHERE source_id='{}'",
            canonical.revision_table, canonical.source.id
        ))
        .await
        .unwrap();
        set_manifest(&db, &canonical.source, true).await;

        db.execute_unprepared(&format!(
            "DELETE FROM {} WHERE id='{}'",
            canonical.canonical_table, canonical.source.id
        ))
        .await
        .unwrap();
        let trigger_revision = db
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                format!(
                    "SELECT revision,present FROM {} WHERE source_id='{}'",
                    canonical.revision_table, canonical.source.id
                ),
            ))
            .await
            .unwrap()
            .unwrap();
        assert!(trigger_revision.try_get::<i64>("", "revision").unwrap() > 1);
        assert_eq!(trigger_revision.try_get::<i64>("", "present").unwrap(), 0);
        db.execute_unprepared(&format!(
            "UPDATE {} SET turn_id='source-turn',revision=1,present=1 WHERE source_id='{}'",
            canonical.revision_table, canonical.source.id
        ))
        .await
        .unwrap();
        assert_manifest_current(
            &db,
            "manifest-operation",
            false,
            &format!("{} missing physical canonical row", canonical.source.name),
        )
        .await;
    }
}

#[tokio::test]
async fn every_source_branch_independently_checks_operation_workspace() {
    let fixture = fixture().await;
    let db = fixture.db();
    db.execute_unprepared("UPDATE compaction_operation SET snapshot='{\"plan\":{\"coverage_domain\":\"own_contribution\"},\"source_epochs\":{\"root-thread\":0,\"foreign-thread\":0}}' WHERE id='manifest-operation'")
        .await
        .unwrap();

    for source in source_cases().into_iter().take(4) {
        set_manifest(&db, &source, true).await;
        db.execute_unprepared("UPDATE thread SET workspace_id='other' WHERE id='source-thread'")
            .await
            .unwrap();
        assert_manifest_current(
            &db,
            "manifest-operation",
            false,
            &format!("{} workspace", source.name),
        )
        .await;
        db.execute_unprepared("UPDATE thread SET workspace_id='ws' WHERE id='source-thread'")
            .await
            .unwrap();
    }

    let checkpoint = source_cases()[4].clone();
    db.execute_unprepared(
        "UPDATE compaction_operation SET snapshot='{\"plan\":{\"coverage_domain\":\"own_contribution\"},\"source_epochs\":{\"root-thread\":0,\"source-thread\":0,\"foreign-thread\":0}}' WHERE id='manifest-operation'",
    )
    .await
    .unwrap();
    set_manifest(&db, &checkpoint, true).await;
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "checkpoint workspace baseline",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_context SET workspace_id='other' WHERE owner='source-owner'",
    )
    .await
    .unwrap();
    assert_manifest_current(&db, "manifest-operation", false, "checkpoint workspace").await;
    db.execute_unprepared(
        "UPDATE compaction_context SET workspace_id='ws' WHERE owner='source-owner'",
    )
    .await
    .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "checkpoint workspace restored",
    )
    .await;

    let basis = source_cases()[5].clone();
    db.execute_unprepared("UPDATE compaction_operation SET snapshot='{\"plan\":{\"coverage_domain\":\"own_contribution\"},\"source_epochs\":{\"root-thread\":0,\"foreign-thread\":0}}' WHERE id='manifest-operation'")
        .await
        .unwrap();
    set_manifest(&db, &basis, true).await;
    db.execute_unprepared("UPDATE thread SET workspace_id='other' WHERE id='source-thread'")
        .await
        .unwrap();
    db.execute_unprepared(
        "UPDATE task_run_conversation_snapshot SET workspace_id='other' WHERE run_id='basis-run'",
    )
    .await
    .unwrap();
    db.execute_unprepared("DELETE FROM compaction_task_basis_revision WHERE run_id='basis-run'")
        .await
        .unwrap();
    assert_manifest_current(&db, "manifest-operation", false, "task basis workspace").await;
}

#[tokio::test]
async fn task_basis_revision_and_ltrim_semantics_are_preserved() {
    let fixture = fixture().await;
    let db = fixture.db();
    let basis = source_cases()[5].clone();
    set_manifest(&db, &basis, true).await;

    db.execute_unprepared(
        "UPDATE task_run_conversation_snapshot SET history_json='[]',frozen_manifest_id=NULL WHERE run_id='basis-run'",
    )
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
    assert!(revision > 1);
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_manifest SET source_version=? WHERE operation_id='manifest-operation'",
        [format!("task-basis-revision:{revision}").into()],
    ))
    .await
    .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "current task basis revision",
    )
    .await;
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_manifest SET source_version=? WHERE operation_id='manifest-operation'",
        [format!("task-basis-revision:{}", revision - 1).into()],
    ))
    .await
    .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "stale task basis revision",
    )
    .await;
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_manifest SET source_version=? WHERE operation_id='manifest-operation'",
        [format!("task-basis-revision:{revision}").into()],
    ))
    .await
    .unwrap();

    for (history, expected, case) in [
        ("   []", true, "spaces before task basis array"),
        ("\t[]", false, "tab before task basis array"),
        ("   {}", false, "task basis non-array"),
    ] {
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "WITH root_replacement(history_json) AS (VALUES (?)) UPDATE task_run_conversation_snapshot SET history_json=(SELECT history_json FROM root_replacement),frozen_manifest_id=(SELECT CASE WHEN json_valid(history_json) THEN CASE WHEN json_type(history_json)='object' THEN json_extract(history_json,'$.manifest_id') ELSE NULL END ELSE NULL END FROM root_replacement) WHERE run_id='basis-run'",
            [history.into()],
        ))
        .await
        .unwrap();
        let current = db
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT revision FROM compaction_task_basis_revision WHERE run_id='basis-run'",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<i64>("", "revision")
            .unwrap();
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE compaction_manifest SET source_version=? WHERE operation_id='manifest-operation'",
            [format!("task-basis-revision:{current}").into()],
        ))
        .await
        .unwrap();
        assert_manifest_current(&db, "manifest-operation", expected, case).await;
    }
}

#[tokio::test]
async fn checkpoint_statuses_and_operation_liveness_are_independent() {
    let fixture = fixture().await;
    let db = fixture.db();
    let checkpoint = source_cases()[4].clone();
    set_manifest(&db, &checkpoint, true).await;

    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='retained' WHERE id='source-checkpoint'",
    )
    .await
    .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "retained completed checkpoint",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_operation SET status='running' WHERE id='source-operation'",
    )
    .await
    .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "retained unfinished checkpoint",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='applied' WHERE id='source-checkpoint'",
    )
    .await
    .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "applied unfinished checkpoint",
    )
    .await;
    db.execute_unprepared("DELETE FROM compaction_operation WHERE id='source-operation'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "checkpoint operation cascade",
    )
    .await;
}

#[tokio::test]
async fn published_checkpoint_ignores_historical_leaf_revision_presence_and_storage() {
    let fixture = fixture().await;
    let db = fixture.db();
    let checkpoint = source_cases()[4].clone();
    set_manifest(&db, &checkpoint, true).await;

    db.execute_unprepared(
        "UPDATE compaction_event_revision SET revision=2 WHERE source_id='event-source'",
    )
    .await
    .unwrap();
    assert_manifest_current(&db, "manifest-operation", true, "historical revision").await;

    db.execute_unprepared(
        "UPDATE compaction_event_revision SET present=0 WHERE source_id='event-source'",
    )
    .await
    .unwrap();
    assert_manifest_current(&db, "manifest-operation", true, "historical present flag").await;

    db.execute_unprepared("DELETE FROM turn_event WHERE id='event-source'")
        .await
        .unwrap();
    assert_manifest_current(&db, "manifest-operation", true, "historical physical row").await;
}

#[tokio::test]
async fn accepted_imports_require_the_bound_ready_manifest_and_exact_metadata() {
    let fixture = fixture().await;
    let db = fixture.db();
    let foreign = SourceCase {
        name: "foreign import",
        thread: "foreign-thread",
        scope: "event:foreign-turn",
        id: "foreign-event",
        version: "event-revision:1",
    };
    set_manifest(&db, &foreign, false).await;
    for sql in [
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES ('accepted-manifest','ws','root-thread','identity',1,1,1,'imports',1,1)",
        "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES ('accepted-manifest',0,'{}',2)",
        "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES ('accepted-manifest',0,0,'event:foreign-turn','foreign-event','event-revision:1','foreign-thread','{}',2)",
        "INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) VALUES ('manifest-operation','accepted-manifest','identity','imports',1)",
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES ('unrelated-manifest','ws','root-thread','identity',1,1,1,'imports',1,1)",
        "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES ('unrelated-manifest',0,'{}',2)",
        "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES ('unrelated-manifest',0,0,'event:foreign-turn','foreign-event','event-revision:1','foreign-thread','{}',2)",
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES ('empty-import-manifest','ws','root-thread','identity',1,1,1,'imports',1,1)",
        "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES ('empty-import-manifest',0,'{}',2)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    assert_manifest_current(&db, "manifest-operation", true, "accepted import").await;
    db.execute_unprepared("UPDATE compaction_operation_projection SET manifest_id='empty-import-manifest' WHERE operation_id='manifest-operation'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "ordinary import cannot leak from another manifest",
    )
    .await;
    db.execute_unprepared("UPDATE compaction_operation_projection SET manifest_id='accepted-manifest' WHERE operation_id='manifest-operation'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "ordinary import binding restored",
    )
    .await;
    for sql in [
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES ('import-storage','ws','root-thread','storage',1,1,1,'storage',1,1)",
        "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) SELECT 'import-storage',ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes FROM compaction_frozen_import_data WHERE manifest_id='accepted-manifest'",
        "DELETE FROM compaction_frozen_import_data WHERE manifest_id='accepted-manifest'",
        "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES ('accepted-manifest',1,0,1)",
        "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES ('accepted-manifest',1,0,1,'import-storage')",
        "UPDATE compaction_frozen_layout SET active=1,pending=0 WHERE manifest_id='accepted-manifest' AND kind=1",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "shared-range accepted import",
    )
    .await;
    db.execute_unprepared("UPDATE compaction_operation_projection SET manifest_id='empty-import-manifest' WHERE operation_id='manifest-operation'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "shared import cannot leak from another manifest",
    )
    .await;
    db.execute_unprepared("UPDATE compaction_operation_projection SET manifest_id='accepted-manifest' WHERE operation_id='manifest-operation'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "shared import binding restored",
    )
    .await;

    for (column, invalid, valid) in [
        ("ready", "0", "1"),
        ("identity_sha256", "'wrong'", "'identity'"),
        ("imports_sha256", "'wrong'", "'imports'"),
        ("import_count", "2", "1"),
        ("next_import", "0", "1"),
    ] {
        db.execute_unprepared(&format!(
            "UPDATE compaction_frozen_history SET {column}={invalid} WHERE id='accepted-manifest'"
        ))
        .await
        .unwrap();
        assert_manifest_current(
            &db,
            "manifest-operation",
            false,
            &format!("invalid frozen {column}"),
        )
        .await;
        db.execute_unprepared(&format!(
            "UPDATE compaction_frozen_history SET {column}={valid} WHERE id='accepted-manifest'"
        ))
        .await
        .unwrap();
    }
    db.execute_unprepared("UPDATE compaction_operation_projection SET manifest_id='empty-import-manifest' WHERE operation_id='manifest-operation'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "wrong manifest binding cannot borrow matching import",
    )
    .await;
}

async fn install_projection(db: &SqliteDatabase, manifest: &str, message_json: &str) {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES (?,'ws','root-thread','identity',1,1,0,'imports',0,1)",
        [manifest.into()],
    ))
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES (?,0,?,length(CAST(? AS BLOB)))",
        [manifest.into(), message_json.into(), message_json.into()],
    ))
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) VALUES ('manifest-operation',?,'identity','imports',0)",
        [manifest.into()],
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn inherited_input_alias_admits_only_exact_historical_checkpoint_coverage() {
    let f = fixture().await;
    let db = f.db();
    for sql in [
        "UPDATE compaction_operation SET snapshot='{\"plan\":{\"coverage_domain\":\"working_context\"},\"source_epochs\":{\"root-thread\":0,\"source-thread\":0,\"foreign-thread\":0}}' WHERE id='manifest-operation'",
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('original-input','foreign-turn',0,'text','fixture','{}',CURRENT_TIMESTAMP)",
        "UPDATE compaction_coverage SET source_scope='input:source-turn',source_id='input-source',source_version='input-revision:1' WHERE checkpoint_id='source-checkpoint'",
        "UPDATE compaction_manifest SET source_scope='input:source-turn',source_id='input-source',source_version='input-revision:1' WHERE operation_id='source-operation'",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    let checkpoint = source_cases()[4].clone();
    set_manifest(&db, &checkpoint, false).await;
    let alias = serde_json::json!({
        "represented_thread": "foreign-thread",
        "represented_source": {"scope":"input:foreign-turn","id":"original-input","version":"input-revision:1"},
        "source_thread": "source-thread",
        "source": {"scope":"input:source-turn","id":"input-source","version":"input-revision:1"}
    });
    let mut reference = serde_json::json!({
        "inherited": true, "complete": true, "protected_input": false,
        "source_thread": "foreign-thread",
        "unit_id": "foreign-turn:original-input",
        "wire_sha256": "a".repeat(64),
        "replay_source": null, "tool_call_id": null, "tool_name": null,
        "sources": [{"scope":"input:foreign-turn","id":"original-input","version":"input-revision:1"}]
    });
    serde_json::from_value::<pioneer_compaction::frozen::FrozenMessageRef>(reference.clone())
        .unwrap()
        .validate()
        .unwrap();
    install_projection(&db, "input-alias-basis", &reference.to_string()).await;
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "copy is not a primary source",
    )
    .await;
    reference["source_aliases"] = serde_json::json!([alias.clone()]);
    let update = |reference: serde_json::Value| {
        let db = db.clone();
        async move {
            let json = reference.to_string();
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE compaction_frozen_message_data SET reference_json=?1,bytes=length(CAST(?1 AS BLOB)) WHERE manifest_id='input-alias-basis' AND ordinal=0",
                [json.into()],
            )).await.unwrap();
        }
    };
    update(reference.clone()).await;
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "published copy checkpoint is represented by an exact alias",
    )
    .await;

    for (path, value) in [
        (
            "/source_aliases/0/source/version",
            serde_json::json!("input-revision:2"),
        ),
        (
            "/source_aliases/0/source_thread",
            serde_json::json!("other-thread"),
        ),
        (
            "/source_aliases/0/represented_source/id",
            serde_json::json!("unrepresented"),
        ),
        (
            "/source_aliases/0/represented_source/scope",
            serde_json::json!("event:foreign-turn"),
        ),
        ("/inherited", serde_json::json!(false)),
        ("/complete", serde_json::json!(false)),
        ("/protected_input", serde_json::json!(true)),
    ] {
        let mut invalid = reference.clone();
        *invalid.pointer_mut(path).unwrap() = value;
        update(invalid).await;
        assert_manifest_current(&db, "manifest-operation", false, path).await;
    }
    let mut ambiguous = reference.clone();
    ambiguous["ambiguous_input_aliases"] = serde_json::json!([{
        "source_thread": "source-thread",
        "source": {"scope":"input:source-turn","id":"input-source","version":"input-revision:1"}
    }]);
    update(ambiguous).await;
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "sticky conflict blocks copy proof",
    )
    .await;
    let mut competing = reference.clone();
    let mut other = alias;
    other["represented_source"]["id"] = serde_json::json!("different-original");
    competing["source_aliases"]
        .as_array_mut()
        .unwrap()
        .push(other);
    update(competing).await;
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "competing owners block copy proof",
    )
    .await;
    update(reference.clone()).await;

    let raw = source_cases()[3].clone();
    set_manifest(&db, &raw, false).await;
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "alias grants no direct raw access",
    )
    .await;
    set_manifest(&db, &checkpoint, false).await;
    db.execute_unprepared("UPDATE compaction_operation SET snapshot=json_set(snapshot,'$.plan.coverage_domain','own_contribution') WHERE id='manifest-operation'").await.unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "alias is not an OWN import",
    )
    .await;
    db.execute_unprepared("UPDATE compaction_operation SET snapshot=json_set(snapshot,'$.plan.coverage_domain','working_context') WHERE id='manifest-operation'").await.unwrap();
    db.execute_unprepared("INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('source-checkpoint','event:source-turn','event-source','event-revision:1'); INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES ('source-operation',1,1,0,'source-thread','event:source-turn','event-source','event-revision:1')").await.unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "alias cannot admit unrelated checkpoint leaves",
    )
    .await;
    db.execute_unprepared("DELETE FROM compaction_coverage WHERE checkpoint_id='source-checkpoint' AND source_id='event-source'; DELETE FROM compaction_manifest WHERE operation_id='source-operation' AND ordinal=1; DELETE FROM turn_input WHERE id='input-source'").await.unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "old checkpoint survives removed copy payload",
    )
    .await;

    // The representative can itself be an already published summary.
    for sql in [
        "INSERT INTO compaction_context(workspace_id,thread_id,owner,format_version) VALUES ('ws','foreign-thread','original-owner',1)",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES ('original-operation','original-owner','original','completed','{}',1)",
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES ('original-summary','original-operation','original-owner',0,'original summary','original-version','{}',0,1,'applied')",
        "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('original-summary','input:foreign-turn','original-input','input-revision:1')",
        "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES ('original-operation',0,0,0,'foreign-thread','input:foreign-turn','original-input','input-revision:1')",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    reference["sources"] = serde_json::json!([{"scope":"checkpoint:original-owner","id":"original-summary","version":"original-version"}]);
    serde_json::from_value::<pioneer_compaction::frozen::FrozenMessageRef>(reference.clone())
        .unwrap()
        .validate()
        .unwrap();
    update(reference.clone()).await;
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "summary carrier proves its exact historical input alias",
    )
    .await;
    let selection = serde_json::to_string(&ModelSelection {
        transport: Transport::Api,
        instance: "publication-fixture".into(),
        model: "publication-model".into(),
        effort: None,
    })
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_checkpoint SET selection=? WHERE id IN ('source-checkpoint','original-summary')",
        [selection.into()],
    ))
    .await
    .unwrap();
    let original_source = SourceRef {
        scope: "input:foreign-turn".into(),
        id: "original-input".into(),
        version: "input-revision:1".into(),
    };
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_operation SET snapshot=?1 WHERE id='original-operation'",
        [raw_fixture_snapshot(
            "original-operation",
            "original-owner",
            vec![original_source],
        )
        .into()],
    ))
    .await
    .unwrap();
    let original_identity = finish_checkpoint_fixture(&f, "original-summary").await;
    reference["sources"][0]["version"] = serde_json::json!(original_identity);
    let source_identity = finish_checkpoint_fixture(&f, "source-checkpoint").await;
    let old = f
        .store
        .compaction_checkpoint("source-checkpoint")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.summary, "source summary");
    assert_eq!(old.coverage.len(), 1);
    assert_eq!(old.coverage[0].id, "input-source");

    let state = publication_candidate(&f, "alias-publication", 1).await;
    for sql in [
        "UPDATE compaction_operation SET snapshot=json_set(snapshot,'$.plan.coverage_domain','working_context') WHERE id='alias-publication'",
        "UPDATE compaction_manifest SET source_thread='source-thread',source_scope='checkpoint:source-owner',source_id='source-checkpoint',source_version='source-version' WHERE operation_id='alias-publication'",
        "UPDATE compaction_coverage SET source_scope='checkpoint:source-owner',source_id='source-checkpoint',source_version='source-version' WHERE checkpoint_id='alias-publication-checkpoint'",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    for sql in [
        "UPDATE compaction_manifest SET source_version=?1 WHERE operation_id='alias-publication'",
        "UPDATE compaction_coverage SET source_version=?1 WHERE checkpoint_id='alias-publication-checkpoint'",
    ] {
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            sql,
            [source_identity.clone().into()],
        ))
        .await
        .unwrap();
    }
    install_verified_projection(&f, "alias-publication", &reference).await;
    finish_checkpoint_fixture(&f, "alias-publication-checkpoint").await;
    assert_positive_publication_preflight(&f, "alias-publication", &state).await;
    assert_eq!(
        f.store
            .compaction_apply_runner("alias-publication", &state, None)
            .await
            .unwrap(),
        crate::compaction::CommitOutcome::Applied
    );
    let next = f
        .store
        .compaction_checkpoint("alias-publication-checkpoint")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.coverage.len(), 1);
    assert_eq!(
        next.coverage[0].id, old.id,
        "new summary keeps the old checkpoint atomic"
    );
    let preserved = f
        .store
        .compaction_checkpoint(&old.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(preserved.summary, old.summary);
    assert_eq!(preserved.coverage, old.coverage);
}

#[tokio::test]
async fn inherited_checkpoint_basis_is_an_atomic_accepted_reference() {
    let fixture = fixture().await;
    let db = fixture.db();
    for sql in [
        "INSERT INTO compaction_context(workspace_id,thread_id,owner,format_version) VALUES ('ws','foreign-thread','basis-owner',1)",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES ('basis-operation','basis-owner','basis','completed','{}',1)",
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,previous,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES ('basis-mid','basis-operation','basis-owner',NULL,0,'mid','mid-version','{}',0,1,'applied')",
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,previous,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES ('basis-root','basis-operation','basis-owner','basis-mid',1,'root','basis-version','{}',0,1,'applied')",
        "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('basis-mid','event:foreign-turn','foreign-event','event-revision:1')",
        "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES ('basis-operation',0,0,0,'foreign-thread','event:foreign-turn','foreign-event','event-revision:1')",
        "UPDATE compaction_operation SET snapshot='{\"plan\":{\"coverage_domain\":\"working_context\"},\"source_epochs\":{\"root-thread\":0,\"foreign-thread\":0}}' WHERE id='manifest-operation'",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    let root = SourceCase {
        name: "basis root",
        thread: "foreign-thread",
        scope: "checkpoint:basis-owner",
        id: "basis-root",
        version: "basis-version",
    };
    set_manifest(&db, &root, false).await;
    db.execute_unprepared("INSERT INTO compaction_checkpoint(id,operation_id,owner,previous,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES ('basis-sibling','basis-operation','basis-owner','basis-mid',2,'sibling','sibling-version','{}',0,1,'applied')")
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES ('manifest-operation',1,1,1,'foreign-thread','checkpoint:basis-owner','basis-sibling','sibling-version')")
        .await
        .unwrap();
    let reference = serde_json::json!({
        "inherited": true,
        "source_thread": "foreign-thread",
        "sources": [{"scope":"checkpoint:basis-owner","id":"basis-root","version":"basis-version"}]
    })
    .to_string();
    install_projection(&db, "basis-manifest", &reference).await;
    for sql in [
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES ('empty-basis-manifest','ws','root-thread','identity',1,1,0,'imports',0,1)",
        "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES ('empty-basis-manifest',0,'{}',2)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }

    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "valid nonempty basis coverage",
    )
    .await;
    db.execute_unprepared("UPDATE compaction_operation_projection SET manifest_id='empty-basis-manifest' WHERE operation_id='manifest-operation'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "ordinary basis cannot leak from another manifest",
    )
    .await;
    db.execute_unprepared("UPDATE compaction_operation_projection SET manifest_id='basis-manifest' WHERE operation_id='manifest-operation'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "ordinary basis binding restored",
    )
    .await;
    db.execute_unprepared("INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES ('basis-storage','ws','root-thread','storage',1,1,0,'storage',0,1)")
        .await
        .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES ('basis-storage',0,?,length(CAST(? AS BLOB)))",
        [reference.clone().into(), reference.clone().into()],
    ))
    .await
    .unwrap();
    for sql in [
        "DELETE FROM compaction_frozen_message_data WHERE manifest_id='basis-manifest'",
        "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES ('basis-manifest',0,0,1)",
        "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES ('basis-manifest',0,0,1,'basis-storage')",
        "UPDATE compaction_frozen_layout SET active=1,pending=0 WHERE manifest_id='basis-manifest' AND kind=0",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "shared-range basis message",
    )
    .await;
    db.execute_unprepared("UPDATE compaction_operation_projection SET manifest_id='empty-basis-manifest' WHERE operation_id='manifest-operation'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "shared basis cannot leak from another manifest",
    )
    .await;
    db.execute_unprepared("UPDATE compaction_operation_projection SET manifest_id='basis-manifest' WHERE operation_id='manifest-operation'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "shared basis binding restored",
    )
    .await;
    crate::repositories::compaction::seed_legacy_frozen_header(
        &db,
        "UPDATE compaction_frozen_history SET next_ordinal=0 WHERE id='basis-manifest'",
    )
    .await;
    assert_manifest_current(&db, "manifest-operation", false, "basis message cursor").await;
    crate::repositories::compaction::seed_legacy_frozen_header(&db, "UPDATE compaction_frozen_history SET next_ordinal=1,message_count=2 WHERE id='basis-manifest'").await;
    assert_manifest_current(&db, "manifest-operation", false, "basis message count").await;
    crate::repositories::compaction::seed_legacy_frozen_header(
        &db,
        "UPDATE compaction_frozen_history SET message_count=1 WHERE id='basis-manifest'",
    )
    .await;
    db.execute_unprepared("UPDATE compaction_operation SET snapshot='{\"plan\":{\"coverage_domain\":\"own_contribution\"},\"source_epochs\":{\"root-thread\":0,\"foreign-thread\":0}}' WHERE id='manifest-operation'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "own contribution cannot use basis grant",
    )
    .await;
    db.execute_unprepared("UPDATE compaction_operation SET snapshot='{\"plan\":{\"coverage_domain\":\"working_context\"},\"source_epochs\":{\"root-thread\":0}}' WHERE id='manifest-operation'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "accepted checkpoint is independent of historical source epoch membership",
    )
    .await;
    db.execute_unprepared("UPDATE compaction_operation SET snapshot='{\"plan\":{\"coverage_domain\":\"working_context\"},\"source_epochs\":{\"root-thread\":0,\"foreign-thread\":0}}' WHERE id='manifest-operation'")
        .await
        .unwrap();
    let raw_basis = serde_json::json!({
        "inherited": true,
        "source_thread": "foreign-thread",
        "sources": [{"scope":"event:foreign-turn","id":"foreign-event","version":"event-revision:1"}]
    })
    .to_string();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_frozen_message_data SET reference_json=?,bytes=length(CAST(? AS BLOB)) WHERE manifest_id='basis-storage' AND ordinal=0",
        [raw_basis.clone().into(), raw_basis.into()],
    ))
    .await
    .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "historical coverage may derive a checkpoint grant from the accepted raw leaf",
    )
    .await;
    db.execute_unprepared(
        "DELETE FROM compaction_manifest WHERE operation_id='basis-operation' AND source_id='foreign-event'",
    )
    .await
    .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "missing historical ownership cannot truncate an unauthorized coverage branch",
    )
    .await;
    db.execute_unprepared("INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES ('basis-operation',0,0,0,'foreign-thread','event:foreign-turn','foreign-event','event-revision:1')")
        .await
        .unwrap();
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET previous='missing-basis-mid' WHERE id='basis-root'",
    )
    .await
    .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "missing historical predecessor cannot truncate an unauthorized coverage branch",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET previous='basis-mid' WHERE id='basis-root'",
    )
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_frozen_message_data SET reference_json=?,bytes=length(CAST(? AS BLOB)) WHERE manifest_id='basis-storage' AND ordinal=0",
        [reference.clone().into(), reference.clone().into()],
    ))
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET previous='basis-root' WHERE id='basis-mid'",
    )
    .await
    .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "historical checkpoint cycle does not alter the accepted root",
    )
    .await;
    db.execute_unprepared("DELETE FROM compaction_coverage WHERE checkpoint_id='basis-mid'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "accepted root does not require a canonical historical leaf",
    )
    .await;
    db.execute_unprepared("INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('basis-mid','event:foreign-turn','foreign-event','event-revision:1')")
        .await
        .unwrap();
    db.execute_unprepared("UPDATE compaction_checkpoint SET previous=NULL WHERE id='basis-mid'")
        .await
        .unwrap();
    db.execute_unprepared(
        "UPDATE compaction_event_revision SET revision=2 WHERE source_id='foreign-event'",
    )
    .await
    .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "historical leaf revision is not root liveness",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_event_revision SET revision=1 WHERE source_id='foreign-event'",
    )
    .await
    .unwrap();
    db.execute_unprepared("DELETE FROM compaction_checkpoint WHERE id='basis-mid'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "accepted root survives a missing historical intermediate",
    )
    .await;
}

#[tokio::test]
async fn checkpoint_basis_does_not_grant_direct_access_to_historical_raw_leaves() {
    let fixture = fixture().await;
    let db = fixture.db();
    for sql in [
        "INSERT INTO compaction_context(workspace_id,thread_id,owner,format_version) VALUES ('ws','foreign-thread','basis-only-owner',1)",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES ('basis-only-operation','basis-only-owner','basis-only','completed','{}',1)",
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,previous,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES ('basis-only-mid','basis-only-operation','basis-only-owner',NULL,0,'mid','basis-only-mid-version','{}',0,1,'applied')",
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,previous,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES ('basis-only-root','basis-only-operation','basis-only-owner','basis-only-mid',1,'root','basis-only-root-version','{}',0,1,'applied')",
        "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('basis-only-mid','event:foreign-turn','foreign-event','event-revision:1')",
        "UPDATE compaction_operation SET snapshot='{\"plan\":{\"coverage_domain\":\"working_context\"},\"source_epochs\":{\"root-thread\":0,\"foreign-thread\":0}}' WHERE id='manifest-operation'",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    let foreign = SourceCase {
        name: "basis-only leaf",
        thread: "foreign-thread",
        scope: "event:foreign-turn",
        id: "foreign-event",
        version: "event-revision:1",
    };
    set_manifest(&db, &foreign, false).await;
    let reference = serde_json::json!({
        "inherited": true,
        "source_thread": "foreign-thread",
        "sources": [{
            "scope":"checkpoint:basis-only-owner",
            "id":"basis-only-root",
            "version":"basis-only-root-version"
        }]
    })
    .to_string();
    install_projection(&db, "basis-only-manifest", &reference).await;

    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "summary coverage is not a grant to its raw leaf",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='pending' WHERE id='basis-only-mid'",
    )
    .await
    .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "unpublished ancestor still does not grant raw access",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='applied' WHERE id='basis-only-mid'",
    )
    .await
    .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "restored ancestor still does not grant raw access",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_event_revision SET revision=2 WHERE source_id='foreign-event'",
    )
    .await
    .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "edited historical leaf is still not directly granted",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_event_revision SET revision=1 WHERE source_id='foreign-event'",
    )
    .await
    .unwrap();
    db.execute_unprepared("DELETE FROM compaction_checkpoint WHERE id='basis-only-mid'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "missing historical checkpoint still does not grant raw access",
    )
    .await;
}

#[tokio::test]
async fn accepted_checkpoint_import_is_an_atomic_grant_for_a_later_checkpoint() {
    let fixture = fixture().await;
    let db = fixture.db();
    for sql in [
        "INSERT INTO compaction_context(workspace_id,thread_id,owner,format_version) VALUES ('ws','foreign-thread','atomic-target-owner',1)",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES ('atomic-target-operation','atomic-target-owner','atomic-target','completed','{}',1)",
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES ('atomic-target','atomic-target-operation','atomic-target-owner',0,'target','atomic-target-version','{}',0,1,'applied')",
        "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('atomic-target','checkpoint:source-owner','source-checkpoint','source-version')",
        "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('atomic-target','event:foreign-turn','foreign-event','event-revision:1')",
        "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES ('atomic-target-operation',0,0,0,'source-thread','checkpoint:source-owner','source-checkpoint','source-version')",
        "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES ('atomic-target-operation',1,1,0,'foreign-thread','event:foreign-turn','foreign-event','event-revision:1')",
        "UPDATE compaction_operation SET snapshot='{\"plan\":{\"coverage_domain\":\"own_contribution\"},\"source_epochs\":{\"root-thread\":0,\"source-thread\":0,\"foreign-thread\":0}}' WHERE id='manifest-operation'",
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES ('atomic-import-manifest','ws','root-thread','atomic-import-identity',1,1,1,'atomic-imports',1,1)",
        "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES ('atomic-import-manifest',0,0,'checkpoint:source-owner','source-checkpoint','source-version','source-thread','{}',2)",
        "INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) VALUES ('manifest-operation','atomic-import-manifest','atomic-import-identity','atomic-imports',1)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    let target = SourceCase {
        name: "atomic target",
        thread: "foreign-thread",
        scope: "checkpoint:atomic-target-owner",
        id: "atomic-target",
        version: "atomic-target-version",
    };
    set_manifest(&db, &target, false).await;
    let reference = serde_json::json!({
        "inherited": false,
        "source_thread": "foreign-thread",
        "context_thread": "root-thread",
        "sources": [{
            "scope":"checkpoint:atomic-target-owner",
            "id":"atomic-target",
            "version":"atomic-target-version"
        }]
    })
    .to_string();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES ('atomic-import-manifest',0,?,length(CAST(? AS BLOB)))",
        [reference.clone().into(), reference.into()],
    ))
    .await
    .unwrap();

    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "accepted S cannot cover the additional unaccepted X input of T",
    )
    .await;
    crate::repositories::compaction::seed_legacy_frozen_header(&db, "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES ('atomic-import-manifest',1,0,'event:foreign-turn','foreign-event','event-revision:1','foreign-thread','{}',2); UPDATE compaction_frozen_history SET import_count=2,next_import=2 WHERE id='atomic-import-manifest'; UPDATE compaction_operation_projection SET import_count=2 WHERE operation_id='manifest-operation'").await;
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "accepted S plus accepted X authorizes T without expanding S",
    )
    .await;

    let raw = SourceCase {
        name: "raw source behind S",
        thread: "source-thread",
        scope: "event:source-turn",
        id: "event-source",
        version: "event-revision:1",
    };
    let raw_ref = SourceRef {
        scope: raw.scope.into(),
        id: raw.id.into(),
        version: raw.version.into(),
    };
    assert!(
        fixture
            .store
            .compaction_sources_current("ws", "source-thread", std::slice::from_ref(&raw_ref))
            .await
            .unwrap(),
        "the raw authority check requires an existing exact-current A"
    );
    set_manifest(&db, &raw, false).await;
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "an atomic grant on S does not grant direct access to current raw A",
    )
    .await;
    crate::repositories::compaction::seed_legacy_frozen_header(&db, "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES ('atomic-import-manifest',2,0,'event:source-turn','event-source','event-revision:1','source-thread','{}',2); UPDATE compaction_frozen_history SET import_count=3,next_import=3 WHERE id='atomic-import-manifest'; UPDATE compaction_operation_projection SET import_count=3 WHERE operation_id='manifest-operation'").await;
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "a separate exact direct grant authorizes current raw A",
    )
    .await;

    crate::repositories::compaction::seed_legacy_frozen_header(&db, "DELETE FROM compaction_frozen_import_data WHERE manifest_id='atomic-import-manifest' AND ordinal=2; UPDATE compaction_frozen_history SET import_count=2,next_import=2 WHERE id='atomic-import-manifest'; UPDATE compaction_operation_projection SET import_count=2 WHERE operation_id='manifest-operation'").await;
    set_manifest(&db, &target, false).await;
    db.execute_unprepared("DELETE FROM turn_event WHERE id='event-source'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "deleting the historical raw leaf behind accepted S does not invalidate restored T",
    )
    .await;
}

async fn enable_and_physically_compress_canonical_payloads(db: &SqliteDatabase) {
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
        let rows = db
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                format!("SELECT id,payload FROM _{table}_zstd WHERE _payload_dict IS NULL"),
            ))
            .await
            .unwrap();
        for row in rows {
            let id = row.try_get::<String>("", "id").unwrap();
            let payload = row.try_get::<String>("", "payload").unwrap();
            let compressed =
                pioneer_sqlite::zstd::compress_column_value(payload.as_bytes(), 3, None).unwrap();
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                format!("UPDATE _{table}_zstd SET payload=?,_payload_dict=-1 WHERE id=?"),
                [compressed.into(), id.into()],
            ))
            .await
            .unwrap();
        }
    }
}

#[tokio::test]
async fn manifest_lookup_checks_physically_compressed_canonical_rows() {
    let fixture = fixture().await;
    let db = fixture.db();
    enable_and_physically_compress_canonical_payloads(&db).await;
    for source in source_cases().into_iter().take(4) {
        set_manifest(&db, &source, true).await;
        assert_manifest_current(&db, "manifest-operation", true, source.name).await;
    }
}

#[tokio::test]
async fn reference_only_checkpoint_validation_does_not_walk_historical_ancestry() {
    let fixture = fixture().await;
    let db = fixture.db();
    db.execute_unprepared("INSERT INTO compaction_context(workspace_id,thread_id,owner,format_version) VALUES ('ws','source-thread','limit-owner',1)")
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES ('limit-operation','limit-owner','limit','completed','{}',1)")
        .await
        .unwrap();
    db.execute_unprepared(
        r#"WITH RECURSIVE n(value) AS (
 SELECT 0
 UNION ALL
 SELECT value+1 FROM n WHERE value<65535
)
INSERT INTO compaction_checkpoint(
 id,operation_id,owner,previous,portion,summary,identity_sha256,
 selection,projection_version,format_version,status
)
SELECT 'limit-'||value,'limit-operation','limit-owner',
 CASE WHEN value=0 THEN NULL ELSE 'limit-'||(value-1) END,
 value,'','limit-version-'||value,'{}',0,1,'applied'
FROM n"#,
    )
    .await
    .unwrap();
    db.execute_unprepared("INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('limit-0','event:source-turn','event-source','event-revision:1')")
        .await
        .unwrap();

    for (root, case) in [
        (65534_i64, "65536 historical graph rows"),
        (65535_i64, "65537 historical graph rows"),
    ] {
        db.execute_unprepared(
            "DELETE FROM compaction_manifest WHERE operation_id='manifest-operation'",
        )
        .await
        .unwrap();
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES ('manifest-operation',0,0,1,'source-thread','checkpoint:limit-owner',?,?)",
            [
                format!("limit-{root}").into(),
                format!("limit-version-{root}").into(),
            ],
        ))
        .await
        .unwrap();
        assert_manifest_current(&db, "manifest-operation", true, case).await;
    }
}

#[tokio::test]
async fn accepted_basis_preserves_the_65536_65537_boundary() {
    let fixture = fixture().await;
    let db = fixture.db();
    db.execute_unprepared("UPDATE compaction_operation SET snapshot='{\"plan\":{\"coverage_domain\":\"working_context\"},\"source_epochs\":{\"root-thread\":0,\"foreign-thread\":0}}' WHERE id='manifest-operation'")
        .await
        .unwrap();
    let foreign = SourceCase {
        name: "accepted basis boundary leaf",
        thread: "foreign-thread",
        scope: "event:foreign-turn",
        id: "foreign-event",
        version: "event-revision:1",
    };
    set_manifest(&db, &foreign, false).await;
    let reference = serde_json::json!({
        "inherited": true,
        "source_thread": "foreign-thread",
        "sources": [{"scope":"event:foreign-turn","id":"foreign-event","version":"event-revision:1"}]
    })
    .to_string();
    db.execute_unprepared("INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES ('accepted-basis-boundary','ws','root-thread','identity',65536,65536,0,'imports',0,1)")
        .await
        .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        r#"WITH RECURSIVE n(value) AS (
 SELECT 0
 UNION ALL
 SELECT value+1 FROM n WHERE value<65535
)
INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes)
SELECT 'accepted-basis-boundary',value,?1,length(CAST(?1 AS BLOB)) FROM n"#,
        [reference.clone().into()],
    ))
    .await
    .unwrap();
    db.execute_unprepared("INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) VALUES ('manifest-operation','accepted-basis-boundary','identity','imports',0)")
        .await
        .unwrap();
    assert_manifest_current(&db, "manifest-operation", true, "65536 accepted basis rows").await;

    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES ('accepted-basis-boundary',65536,?1,length(CAST(?1 AS BLOB)))",
        [reference.into()],
    ))
    .await
    .unwrap();
    crate::repositories::compaction::seed_legacy_frozen_header(&db, "UPDATE compaction_frozen_history SET message_count=65537,next_ordinal=65537 WHERE id='accepted-basis-boundary'").await;
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "65537 accepted basis rows",
    )
    .await;

    db.execute_unprepared(
        "DELETE FROM compaction_frozen_message_data WHERE manifest_id='accepted-basis-boundary' AND ordinal=65536; \
         UPDATE compaction_frozen_history SET message_count=65536,next_ordinal=65536 WHERE id='accepted-basis-boundary'; \
         UPDATE compaction_operation SET snapshot='{\"plan\":{\"coverage_domain\":\"working_context\"},\"source_epochs\":{\"root-thread\":0,\"source-thread\":0}}' WHERE id='manifest-operation'; \
         DELETE FROM compaction_manifest WHERE operation_id='manifest-operation'; \
         INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES ('manifest-operation',0,0,0,'source-thread','checkpoint:source-owner','source-checkpoint','source-version')",
    )
    .await
    .unwrap();
    let checkpoint_reference = serde_json::json!({
        "inherited": true,
        "source_thread": "source-thread",
        "sources": [{
            "scope":"checkpoint:source-owner",
            "id":"source-checkpoint",
            "version":"source-version"
        }]
    })
    .to_string();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_frozen_message_data SET reference_json=?1,bytes=length(CAST(?1 AS BLOB)) WHERE manifest_id='accepted-basis-boundary'",
        [checkpoint_reference.clone().into()],
    ))
    .await
    .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "65536 accepted checkpoint basis rows",
    )
    .await;
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES ('accepted-basis-boundary',65536,?1,length(CAST(?1 AS BLOB)))",
        [checkpoint_reference.into()],
    ))
    .await
    .unwrap();
    crate::repositories::compaction::seed_legacy_frozen_header(&db, "UPDATE compaction_frozen_history SET message_count=65537,next_ordinal=65537 WHERE id='accepted-basis-boundary'").await;
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "65537 accepted checkpoint basis rows",
    )
    .await;
}

#[tokio::test]
async fn historical_basis_coverage_does_not_grant_its_raw_leaf() {
    let fixture = fixture().await;
    let db = fixture.db();
    for sql in [
        "INSERT INTO compaction_context(workspace_id,thread_id,owner,format_version) VALUES ('ws','foreign-thread','basis-limit-owner',1)",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES ('basis-limit-operation','basis-limit-owner','basis-limit','completed','{}',1)",
        "UPDATE compaction_operation SET snapshot='{\"plan\":{\"coverage_domain\":\"working_context\"},\"source_epochs\":{\"root-thread\":0,\"foreign-thread\":0}}' WHERE id='manifest-operation'",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    db.execute_unprepared(
        r#"WITH RECURSIVE n(value) AS (
 SELECT 0
 UNION ALL
 SELECT value+1 FROM n WHERE value<65535
)
INSERT INTO compaction_checkpoint(
 id,operation_id,owner,previous,portion,summary,identity_sha256,
 selection,projection_version,format_version,status
)
SELECT 'basis-limit-'||value,'basis-limit-operation','basis-limit-owner',
 CASE WHEN value=0 THEN NULL ELSE 'basis-limit-'||(value-1) END,
 value,'','basis-limit-version-'||value,'{}',0,1,'applied'
FROM n"#,
    )
    .await
    .unwrap();
    db.execute_unprepared("INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('basis-limit-0','event:foreign-turn','foreign-event','event-revision:1')")
        .await
        .unwrap();
    let foreign = SourceCase {
        name: "basis coverage boundary leaf",
        thread: "foreign-thread",
        scope: "event:foreign-turn",
        id: "foreign-event",
        version: "event-revision:1",
    };
    set_manifest(&db, &foreign, false).await;
    let reference = |root: i64| {
        serde_json::json!({
            "inherited": true,
            "source_thread": "foreign-thread",
            "sources": [{
                "scope":"checkpoint:basis-limit-owner",
                "id":format!("basis-limit-{root}"),
                "version":format!("basis-limit-version-{root}")
            }]
        })
        .to_string()
    };
    let within = reference(65534);
    install_projection(&db, "basis-coverage-boundary", &within).await;
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "65536 historical basis rows are not a raw grant",
    )
    .await;

    let over = reference(65535);
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_frozen_message_data SET reference_json=?,bytes=length(CAST(? AS BLOB)) WHERE manifest_id='basis-coverage-boundary' AND ordinal=0",
        [over.clone().into(), over.into()],
    ))
    .await
    .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "65537 historical basis rows are not a raw grant",
    )
    .await;
}

#[derive(Clone, Debug)]
struct PlanNode {
    id: i64,
    parent: i64,
    detail: String,
}

fn plan_subject(detail: &str, operation: &str, subject: &str) -> bool {
    let prefix = format!("{operation} {subject}");
    detail.strip_prefix(&prefix).is_some_and(|suffix| {
        suffix.is_empty() || suffix.chars().next().is_some_and(char::is_whitespace)
    })
}

fn constraint_parts(detail: &str) -> Vec<&str> {
    detail
        .rsplit_once('(')
        .map(|(_, constraints)| constraints.trim_end_matches(')').split("AND").collect())
        .unwrap_or_default()
}

fn constraint_column(lhs: &str) -> &str {
    lhs.trim()
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .trim_matches(|character| matches!(character, '"' | '`' | '[' | ']'))
}

fn has_constraint(detail: &str, column: &str, operator: &str) -> bool {
    constraint_parts(detail).into_iter().any(|constraint| {
        constraint
            .split_once(operator)
            .is_some_and(|(lhs, rhs)| constraint_column(lhs) == column && rhs.trim() == "?")
    })
}

fn subtree<'a>(plan: &'a [PlanNode], root: i64) -> Vec<&'a PlanNode> {
    let mut ids = vec![root];
    let mut result = Vec::new();
    for node in plan {
        if node.id == root {
            result.push(node);
        }
    }
    let mut cursor = 0;
    while cursor < ids.len() {
        let parent = ids[cursor];
        for node in plan {
            if node.parent == parent && !ids.contains(&node.id) {
                ids.push(node.id);
                result.push(node);
            }
        }
        cursor += 1;
    }
    result
}

fn unique_subtree<'a>(plan: &'a [PlanNode], root_detail: &str) -> Vec<&'a PlanNode> {
    let roots = plan
        .iter()
        .filter(|node| node.detail == root_detail)
        .collect::<Vec<_>>();
    assert_eq!(
        roots.len(),
        1,
        "expected one {root_detail:?} node: {plan:#?}"
    );
    subtree(plan, roots[0].id)
}

fn assert_exact_branch_search(branch: &[&PlanNode], subjects: &[&str], column: &str, label: &str) {
    assert!(
        branch.iter().any(|node| {
            subjects
                .iter()
                .any(|subject| plan_subject(&node.detail, "SEARCH", subject))
                && has_constraint(&node.detail, column, "=")
        }),
        "missing exact {column} lookup for {label} ({subjects:?}): {branch:#?}"
    );
    assert!(
        !branch
            .iter()
            .any(|node| subjects
                .iter()
                .any(|subject| plan_subject(&node.detail, "SCAN", subject))),
        "unexpected scan for {label} ({subjects:?}): {branch:#?}"
    );
}

#[derive(Clone, Copy, Debug)]
enum ProjectionPageBounds {
    Sizes { start: i64 },
    Data { start: i64, end: i64 },
}

fn assert_projection_page_statement(
    statement: &Statement,
    manifest: &str,
    bounds: ProjectionPageBounds,
) {
    let (sql, expected_values) = match bounds {
        ProjectionPageBounds::Sizes { start } => (
            "SELECT d.ordinal,d.bytes FROM compaction_frozen_message_data d \
             WHERE d.manifest_id=? AND d.ordinal>=? \
               AND NOT EXISTS (SELECT 1 FROM compaction_frozen_layout l \
                               WHERE l.manifest_id=d.manifest_id AND l.kind=0 AND l.active=1) \
             UNION ALL \
             SELECT d.ordinal,d.bytes FROM compaction_frozen_span s \
             JOIN compaction_frozen_message_data d \
               ON d.manifest_id=s.source_manifest AND d.ordinal>=s.start AND d.ordinal<s.end \
             JOIN compaction_frozen_layout l \
               ON l.manifest_id=s.manifest_id AND l.kind=s.kind AND l.active=1 \
             WHERE s.manifest_id=? AND s.kind=0 AND d.ordinal>=? \
             ORDER BY ordinal LIMIT ?",
            vec![
                Value::from(manifest),
                Value::from(start),
                Value::from(manifest),
                Value::from(start),
                Value::from(SOURCE_PAGE_ROWS as i64),
            ],
        ),
        ProjectionPageBounds::Data { start, end } => (
            "SELECT d.ordinal,d.reference_json,d.bytes FROM compaction_frozen_message_data d \
             WHERE d.manifest_id=? AND d.ordinal>=? AND d.ordinal<? \
               AND NOT EXISTS (SELECT 1 FROM compaction_frozen_layout l \
                               WHERE l.manifest_id=d.manifest_id AND l.kind=0 AND l.active=1) \
             UNION ALL \
             SELECT d.ordinal,d.reference_json,d.bytes FROM compaction_frozen_span s \
             JOIN compaction_frozen_message_data d \
               ON d.manifest_id=s.source_manifest AND d.ordinal>=s.start AND d.ordinal<s.end \
             JOIN compaction_frozen_layout l \
               ON l.manifest_id=s.manifest_id AND l.kind=s.kind AND l.active=1 \
             WHERE s.manifest_id=? AND s.kind=0 AND d.ordinal>=? AND d.ordinal<? \
             ORDER BY ordinal",
            vec![
                Value::from(manifest),
                Value::from(start),
                Value::from(end),
                Value::from(manifest),
                Value::from(start),
                Value::from(end),
            ],
        ),
    };
    assert_eq!(statement.sql, sql, "production page SQL lost its bounds");
    assert_eq!(
        statement
            .values
            .as_ref()
            .expect("projection page statement must bind its scope")
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        expected_values,
        "production page SQL bound the wrong manifest or ordinal range"
    );
}

fn assert_data_search_page_bounds(branch: &[&PlanNode], bounds: ProjectionPageBounds, label: &str) {
    let searches = branch
        .iter()
        .filter(|node| {
            plan_subject(&node.detail, "SEARCH", "d")
                && has_constraint(&node.detail, "manifest_id", "=")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        searches.len(),
        1,
        "expected one manifest-scoped data lookup for {label}: {branch:#?}"
    );
    let detail = &searches[0].detail;
    assert!(
        has_constraint(detail, "ordinal", ">"),
        "{label} data lookup lost its lower page boundary: {detail}"
    );
    if matches!(bounds, ProjectionPageBounds::Data { .. }) {
        assert!(
            has_constraint(detail, "ordinal", "<"),
            "{label} data lookup lost its upper page boundary: {detail}"
        );
    }
}

fn projection_page_branches(plan: &[PlanNode]) -> (Vec<&PlanNode>, Vec<&PlanNode>) {
    let compound = plan
        .iter()
        .filter(|node| node.detail == "LEFT-MOST SUBQUERY")
        .count();
    if compound == 1 {
        let plan = plan.iter().collect::<Vec<_>>();
        return (
            unique_detail_subtree(&plan, "LEFT-MOST SUBQUERY", "projection page"),
            unique_detail_subtree(&plan, "UNION ALL", "projection page"),
        );
    }
    assert_eq!(
        compound, 0,
        "ambiguous projection-page compound plan: {plan:#?}"
    );
    let merged = unique_subtree(plan, "MERGE (UNION ALL)");
    (
        unique_detail_subtree(&merged, "LEFT", "projection page merge"),
        unique_detail_subtree(&merged, "RIGHT", "projection page merge"),
    )
}

fn assert_projection_page_plan(plan: &[PlanNode], bounds: ProjectionPageBounds) {
    let (ordinary, shared) = projection_page_branches(plan);
    assert_exact_branch_search(&ordinary, &["d"], "manifest_id", "ordinary message data");
    assert_exact_branch_search(&shared, &["d"], "manifest_id", "shared message data");
    assert_exact_branch_search(&shared, &["s"], "manifest_id", "shared message span");
    assert!(
        shared.iter().any(|node| {
            plan_subject(&node.detail, "SEARCH", "s")
                && has_constraint(&node.detail, "manifest_id", "=")
                && has_constraint(&node.detail, "kind", "=")
        }),
        "shared span lookup must constrain manifest and kind: {shared:#?}"
    );

    assert_data_search_page_bounds(&ordinary, bounds, "ordinary");
    // Shared storage already has a span range. This assertion is deliberately
    // scoped to its data node; the exact production statement/binds prove that
    // the requested page range was also supplied rather than borrowing the
    // span's range as page evidence.
    assert_data_search_page_bounds(&shared, bounds, "shared");
}

fn frozen_view_branch<'a>(plan: &'a [PlanNode], view: &str) -> Vec<&'a PlanNode> {
    let producer_details = [format!("CO-ROUTINE {view}"), format!("MATERIALIZE {view}")];
    let producers = plan
        .iter()
        .filter(|node| producer_details.contains(&node.detail))
        .collect::<Vec<_>>();
    assert!(
        producers.len() <= 1,
        "expected at most one producer for {view:?}: {plan:#?}"
    );
    if let Some(root) = producers.first() {
        return subtree(plan, root.id);
    }

    // SQLite may inline a view into its sole consumer. These consumers are
    // disjoint in the production query, so their subtrees retain ownership of
    // otherwise repeated d/l/s aliases.
    let consumer = match view {
        "compaction_frozen_import" => "accepted_imports",
        "compaction_frozen_message" => "accepted_basis",
        _ => panic!("unknown frozen view {view:?}"),
    };
    let consumer_details = [
        format!("CO-ROUTINE {consumer}"),
        format!("MATERIALIZE {consumer}"),
    ];
    let consumers = plan
        .iter()
        .filter(|node| consumer_details.contains(&node.detail))
        .collect::<Vec<_>>();
    assert_eq!(
        consumers.len(),
        1,
        "inlined {view:?} must have one identifiable {consumer:?} consumer: {plan:#?}"
    );
    subtree(plan, consumers[0].id)
}

fn unique_detail_subtree<'a>(
    branch: &[&'a PlanNode],
    detail: &str,
    view: &str,
) -> Vec<&'a PlanNode> {
    let roots = branch
        .iter()
        .copied()
        .filter(|node| node.detail == detail)
        .collect::<Vec<_>>();
    assert_eq!(
        roots.len(),
        1,
        "expected one {detail:?} branch for {view:?}: {branch:#?}"
    );
    let mut ids = vec![roots[0].id];
    let mut result = vec![roots[0]];
    let mut cursor = 0;
    while cursor < ids.len() {
        let parent = ids[cursor];
        for node in branch.iter().copied() {
            if node.parent == parent && !ids.contains(&node.id) {
                ids.push(node.id);
                result.push(node);
            }
        }
        cursor += 1;
    }
    result
}

fn assert_frozen_view_plan(plan: &[PlanNode], view: &str) {
    let view_branch = frozen_view_branch(plan, view);
    let ordinary = unique_detail_subtree(&view_branch, "LEFT-MOST SUBQUERY", view);
    let shared = unique_detail_subtree(&view_branch, "UNION ALL", view);
    assert_exact_branch_search(
        &ordinary,
        &["d"],
        "manifest_id",
        &format!("ordinary {view} data"),
    );
    assert_exact_branch_search(
        &ordinary,
        &["l"],
        "manifest_id",
        &format!("ordinary {view} layout"),
    );
    assert!(
        ordinary.iter().any(|node| {
            plan_subject(&node.detail, "SEARCH", "d")
                && has_constraint(&node.detail, "manifest_id", "=")
                && !has_constraint(&node.detail, "ordinal", ">")
                && !has_constraint(&node.detail, "ordinal", "<")
        }),
        "ordinary {view} data lookup must not require an ordinal range: {ordinary:#?}"
    );
    assert!(
        ordinary.iter().any(|node| {
            plan_subject(&node.detail, "SEARCH", "l")
                && has_constraint(&node.detail, "manifest_id", "=")
                && has_constraint(&node.detail, "kind", "=")
        }),
        "ordinary {view} layout must constrain manifest_id and kind: {ordinary:#?}"
    );

    assert_exact_branch_search(
        &shared,
        &["d"],
        "manifest_id",
        &format!("shared {view} data"),
    );
    assert_exact_branch_search(
        &shared,
        &["l"],
        "manifest_id",
        &format!("shared {view} layout"),
    );
    assert_exact_branch_search(
        &shared,
        &["s"],
        "manifest_id",
        &format!("shared {view} span"),
    );
    assert!(
        shared.iter().any(|node| {
            plan_subject(&node.detail, "SEARCH", "l")
                && has_constraint(&node.detail, "manifest_id", "=")
                && has_constraint(&node.detail, "kind", "=")
        }),
        "shared {view} layout must constrain manifest_id and kind: {shared:#?}"
    );
    assert!(
        shared.iter().any(|node| {
            plan_subject(&node.detail, "SEARCH", "s")
                && has_constraint(&node.detail, "manifest_id", "=")
                && has_constraint(&node.detail, "kind", "=")
        }),
        "shared {view} span must constrain manifest_id and kind: {shared:#?}"
    );
    assert!(
        shared.iter().any(|node| {
            plan_subject(&node.detail, "SEARCH", "d")
                && has_constraint(&node.detail, "manifest_id", "=")
                && has_constraint(&node.detail, "ordinal", ">")
                && has_constraint(&node.detail, "ordinal", "<")
        }),
        "shared-range {view} data lookup must constrain manifest and ordinal range: {shared:#?}"
    );
}

#[test]
fn plan_recognizer_distinguishes_aliases_columns_scans_and_subtrees() {
    assert!(plan_subject(
        "SEARCH context_revision USING INDEX x (source_id=?)",
        "SEARCH",
        "context_revision"
    ));
    assert!(!plan_subject(
        "SEARCH other_context_revision USING INDEX x (source_id=?)",
        "SEARCH",
        "context_revision"
    ));
    assert!(has_constraint(
        "SEARCH r USING INDEX x (source_id=?)",
        "source_id",
        "="
    ));
    assert!(!has_constraint(
        "SEARCH r USING INDEX x (other_source_id=?)",
        "source_id",
        "="
    ));

    let plan = vec![
        PlanNode {
            id: 1,
            parent: 0,
            detail: "MATERIALIZE current_sources".into(),
        },
        PlanNode {
            id: 2,
            parent: 1,
            detail: "SCAN context_revision".into(),
        },
        PlanNode {
            id: 10,
            parent: 0,
            detail: "MATERIALIZE unrelated".into(),
        },
        PlanNode {
            id: 11,
            parent: 10,
            detail: "SEARCH context_revision USING INDEX x (source_id=?)".into(),
        },
    ];
    let branch = unique_subtree(&plan, "MATERIALIZE current_sources");
    assert!(
        branch
            .iter()
            .any(|node| plan_subject(&node.detail, "SCAN", "context_revision"))
    );
    assert!(!branch.iter().any(|node| {
        plan_subject(&node.detail, "SEARCH", "context_revision")
            && has_constraint(&node.detail, "source_id", "=")
    }));
}

fn synthetic_frozen_view_plan(
    view: &str,
    root: i64,
    shared_has_ordinal_range: bool,
) -> Vec<PlanNode> {
    let shared_data = if shared_has_ordinal_range {
        "SEARCH d USING INDEX frozen_data (manifest_id=? AND ordinal>? AND ordinal<?)"
    } else {
        "SEARCH d USING INDEX frozen_data (manifest_id=?)"
    };
    vec![
        PlanNode {
            id: root,
            parent: 0,
            detail: format!("MATERIALIZE {view}"),
        },
        PlanNode {
            id: root + 1,
            parent: root,
            detail: "COMPOUND QUERY".into(),
        },
        PlanNode {
            id: root + 2,
            parent: root + 1,
            detail: "LEFT-MOST SUBQUERY".into(),
        },
        PlanNode {
            id: root + 3,
            parent: root + 2,
            detail: "SEARCH d USING INDEX frozen_data (manifest_id=?)".into(),
        },
        PlanNode {
            id: root + 4,
            parent: root + 2,
            detail: "SEARCH l USING INDEX frozen_layout (manifest_id=? AND kind=?)".into(),
        },
        PlanNode {
            id: root + 5,
            parent: root + 1,
            detail: "UNION ALL".into(),
        },
        PlanNode {
            id: root + 6,
            parent: root + 5,
            detail: "SEARCH s USING INDEX frozen_span (manifest_id=? AND kind=?)".into(),
        },
        PlanNode {
            id: root + 7,
            parent: root + 5,
            detail: shared_data.into(),
        },
        PlanNode {
            id: root + 8,
            parent: root + 5,
            detail: "SEARCH l USING INDEX frozen_layout (manifest_id=? AND kind=?)".into(),
        },
    ]
}

#[test]
fn frozen_plan_recognizer_never_borrows_evidence_from_another_view_or_branch() {
    let inlined_import = synthetic_frozen_view_plan("accepted_imports", 10, true);
    assert_frozen_view_plan(&inlined_import, "compaction_frozen_import");
    let inlined_message = synthetic_frozen_view_plan("accepted_basis", 20, true);
    assert_frozen_view_plan(&inlined_message, "compaction_frozen_message");

    let import_only = synthetic_frozen_view_plan("compaction_frozen_import", 100, true);
    assert!(
        std::panic::catch_unwind(|| {
            assert_frozen_view_plan(&import_only, "compaction_frozen_message")
        })
        .is_err(),
        "a neighboring frozen view cannot satisfy a missing view"
    );

    let mut one_broken_shared = import_only.clone();
    one_broken_shared.extend(synthetic_frozen_view_plan(
        "compaction_frozen_message",
        200,
        false,
    ));
    assert_frozen_view_plan(&one_broken_shared, "compaction_frozen_import");
    assert!(
        std::panic::catch_unwind(|| {
            assert_frozen_view_plan(&one_broken_shared, "compaction_frozen_message")
        })
        .is_err(),
        "another view's ordinal range cannot repair the checked shared branch"
    );

    let mut missing_shared = synthetic_frozen_view_plan("accepted_basis", 300, true);
    missing_shared.retain(|node| node.id < 305);
    missing_shared.extend(import_only);
    assert!(
        std::panic::catch_unwind(|| {
            assert_frozen_view_plan(&missing_shared, "compaction_frozen_message")
        })
        .is_err(),
        "unrelated plan nodes cannot replace a missing checked branch"
    );
}

async fn seed_plan_noise(db: &SqliteDatabase) {
    for index in 0..16 {
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES (?,'other-thread','other-turn',?,'noise','{}',CURRENT_TIMESTAMP)",
            [format!("noise-event-{index}").into(), (index as i64 + 10).into()],
        ))
        .await
        .unwrap();
    }
    for sql in [
        "INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,created_at) VALUES ('noise-context','other-turn',10,'noise','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,created_at,updated_at) VALUES ('noise-item','other-turn','noise-item','command_execution','completed','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('noise-input','other-turn',10,'text','noise','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES ('plan-manifest','ws','root-thread','identity',1,1,1,'imports',1,1)",
        "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES ('plan-manifest',0,'{}',2)",
        "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES ('plan-manifest',0,0,'event:foreign-turn','foreign-event','event-revision:1','foreign-thread','{}',2)",
        "INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) VALUES ('manifest-operation','plan-manifest','identity','imports',1)",
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES ('noise-storage','ws','root-thread','storage',1,1,1,'storage',1,1)",
        "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES ('noise-storage',0,'{}',2)",
        "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES ('noise-storage',0,0,'event:foreign-turn','foreign-event','event-revision:1','foreign-thread','{}',2)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    for index in 0..12 {
        let id = format!("ordinary-noise-{index}");
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES (?,'ws','root-thread','noise',1,1,1,'noise',1,1)",
            [id.clone().into()],
        ))
        .await
        .unwrap();
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES (?,0,'{}',2)",
            [id.clone().into()],
        ))
        .await
        .unwrap();
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES (?,0,0,'event:foreign-turn','foreign-event','event-revision:1','foreign-thread','{}',2)",
            [id.into()],
        ))
        .await
        .unwrap();
    }
    for index in 0..12 {
        let id = format!("shared-noise-{index}");
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES (?,'ws','root-thread','noise',1,1,1,'noise',1,1)",
            [id.clone().into()],
        ))
        .await
        .unwrap();
        for kind in [0_i64, 1_i64] {
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES (?,?,0,1)",
                [id.clone().into(), kind.into()],
            ))
            .await
            .unwrap();
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES (?,?,0,1,'noise-storage')",
                [id.clone().into(), kind.into()],
            ))
            .await
            .unwrap();
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"UPDATE compaction_frozen_layout SET active=1,pending=0 WHERE manifest_id=?1 AND kind=?2",[id.clone().into(),kind.into()])).await.unwrap();
        }
    }
}

async fn production_plan(compressed: bool) -> Vec<PlanNode> {
    let fixture = fixture().await;
    let db = fixture.db();
    for (ordinal, source) in source_cases().into_iter().enumerate() {
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES ('manifest-operation',?,?,1,?,?,?,?)",
            [
                (ordinal as i64).into(),
                (ordinal as i64).into(),
                source.thread.into(),
                source.scope.into(),
                source.id.into(),
                source.version.into(),
            ],
        ))
        .await
        .unwrap();
    }
    seed_plan_noise(&db).await;
    if compressed {
        enable_and_physically_compress_canonical_payloads(&db).await;
    }
    let mut statement = compaction_manifest_sources_current_statement("manifest-operation");
    statement.sql = format!("EXPLAIN QUERY PLAN {}", statement.sql);
    db.query_all_raw(statement)
        .await
        .unwrap()
        .into_iter()
        .map(|row| PlanNode {
            id: row.try_get("", "id").unwrap(),
            parent: row.try_get("", "parent").unwrap(),
            detail: row.try_get("", "detail").unwrap(),
        })
        .collect()
}

fn assert_production_plan(plan: &[PlanNode], compressed: bool) {
    let current_roots = plan
        .iter()
        .filter(|node| {
            matches!(
                node.detail.as_str(),
                "MATERIALIZE current_sources" | "CO-ROUTINE current_sources"
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        current_roots.len(),
        1,
        "expected one current_sources producer: {plan:#?}"
    );
    let current = subtree(plan, current_roots[0].id);
    for (revision, canonical, key) in [
        ("context_revision", "context_source", "source_id"),
        ("item_revision", "item_source", "source_id"),
        ("event_revision", "event_source", "source_id"),
        ("input_revision", "input_source", "source_id"),
    ] {
        assert_exact_branch_search(&current, &[revision], key, revision);
        let physical = if compressed {
            match canonical {
                "context_source" => &["context_source", "_turn_llm_context_zstd"][..],
                "item_source" => &["item_source", "_turn_item_zstd"][..],
                "event_source" => &["event_source", "_turn_event_zstd"][..],
                "input_source" => &["input_source", "_turn_input_zstd"][..],
                _ => unreachable!(),
            }
        } else {
            std::slice::from_ref(&canonical)
        };
        assert_exact_branch_search(&current, physical, "id", canonical);
    }
    assert_exact_branch_search(&current, &["checkpoint_source"], "id", "checkpoint source");
    assert_exact_branch_search(&current, &["basis_source"], "run_id", "task basis source");
    assert_frozen_view_plan(plan, "compaction_frozen_import");
    assert_frozen_view_plan(plan, "compaction_frozen_message");
}

#[tokio::test]
async fn production_plan_scopes_plain_source_and_logical_frozen_view_lookups() {
    let plan = production_plan(false).await;
    assert_production_plan(&plan, false);
}

#[tokio::test]
async fn production_plan_scopes_zstd_source_and_logical_frozen_view_lookups() {
    let plan = production_plan(true).await;
    assert_production_plan(&plan, true);
}

fn raw_fixture_snapshot(operation: &str, owner: &str, coverage: Vec<SourceRef>) -> String {
    let selection = ModelSelection {
        transport: Transport::Api,
        instance: "publication-fixture".into(),
        model: "publication-model".into(),
        effort: None,
    };
    serde_json::to_string(&pioneer_compaction::OperationSnapshot {
        id: operation.into(),
        owner: owner.into(),
        expected_checkpoint: None,
        projection_version: 0,
        source_epochs: std::collections::BTreeMap::new(),
        admission: pioneer_compaction::CompactionSettings::default()
            .admit(&selection, None, 0)
            .unwrap(),
        plan: pioneer_compaction::CompactionPlan {
            mode: pioneer_compaction::CompactionMode::Normal,
            coverage_domain: pioneer_compaction::CoverageDomain::WorkingContext,
            compact: (0..coverage.len()).collect(),
            retain: vec![],
            coverage,
            fingerprint: operation.into(),
        },
    })
    .unwrap()
}

async fn publication_candidate(fixture: &Fixture, operation: &str, sources: usize) -> RunnerState {
    assert!(sources > 0);
    let db = fixture.db();
    for ordinal in 0..sources {
        let id = format!("{operation}-source-{ordinal}");
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES (?1,'root-thread','root-turn',?2,'fixture','{}',CURRENT_TIMESTAMP)",
            [id.into(),(10_000_i64+ordinal as i64).into()])).await.unwrap();
    }
    let coverage = (0..sources)
        .map(|ordinal| SourceRef {
            scope: "event:root-turn".into(),
            id: format!("{operation}-source-{ordinal}"),
            version: "event-revision:1".into(),
        })
        .collect::<Vec<_>>();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES (?1,'root-owner',?1,'running',?2,900000)",
        [operation.into(),raw_fixture_snapshot(operation,"root-owner",coverage.clone()).into()])).await.unwrap();
    for ordinal in 0..sources {
        let id = format!("{operation}-source-{ordinal}");
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO compaction_manifest(\
                operation_id,ordinal,unit_ordinal,reference_only,source_thread,\
                source_scope,source_id,source_version) \
             VALUES (?,?,?,?,'root-thread','event:root-turn',?,'event-revision:1')",
            [
                operation.into(),
                (ordinal as i64).into(),
                (ordinal as i64).into(),
                0_i64.into(),
                id.into(),
            ],
        ))
        .await
        .unwrap();
    }
    let checkpoint = format!("{operation}-checkpoint");
    let selection = serde_json::to_string(&ModelSelection {
        transport: Transport::Api,
        instance: "publication-fixture".into(),
        model: "publication-model".into(),
        effort: None,
    })
    .unwrap();
    let exact_candidate = pioneer_compaction::Checkpoint {
        id: checkpoint.clone(),
        operation_id: operation.into(),
        owner: "root-owner".into(),
        previous: None,
        coverage: coverage.clone(),
        summary: "prepared summary".into(),
        selection: serde_json::from_str(&selection).unwrap(),
        projection_version: 0,
        format_version: 1,
    };
    let identity = checkpoint_identity(&exact_candidate).unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_checkpoint(\
            id,operation_id,owner,portion,summary,identity_sha256,selection,\
            projection_version,format_version,status) \
         VALUES (?,?,'root-owner',0,'prepared summary',?,?,0,1,'candidate')",
        [
            checkpoint.clone().into(),
            operation.into(),
            identity.into(),
            selection.into(),
        ],
    ))
    .await
    .unwrap();
    for ordinal in 0..sources {
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO compaction_coverage(\
                checkpoint_id,source_scope,source_id,source_version) \
             VALUES (?,'event:root-turn',?,'event-revision:1')",
            [
                checkpoint.clone().into(),
                format!("{operation}-source-{ordinal}").into(),
            ],
        ))
        .await
        .unwrap();
    }
    let state = RunnerState {
        resume_phase: None,
        generation: 7,
        deadline_ms: 900_000,
        attempts: 1,
        retries: 0,
        corrections: 0,
        target_tokens: 100,
        source_text_projection_version: 1,
        cursor: SourceCursor {
            unit: sources as u64,
            ..Default::default()
        },
        previous_checkpoint: Some(checkpoint.clone()),
        phase: RunnerPhase::Commit { checkpoint },
        observation: None,
        diagnostic: None,
    };
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_runner_state(operation_id,generation,state) VALUES (?,?,?)",
        [
            operation.into(),
            i64::try_from(state.generation).unwrap().into(),
            serde_json::to_string(&state).unwrap().into(),
        ],
    ))
    .await
    .unwrap();
    state
}

async fn publication_preflight(
    fixture: &Fixture,
    operation: &str,
    state: &RunnerState,
) -> PreparedRunnerPublication {
    let RunnerPhase::Commit { checkpoint } = &state.phase else {
        panic!("publication fixture is not in Commit")
    };
    fixture
        .store
        .prepare_checkpoint_ancestry(operation, checkpoint, state.generation)
        .await
        .unwrap();
    prepare_runner_publication(
        &fixture.store,
        operation,
        checkpoint,
        state.generation,
        None,
    )
    .await
    .unwrap()
}

async fn assert_positive_publication_preflight(
    fixture: &Fixture,
    operation: &str,
    state: &RunnerState,
) {
    let prepared = publication_preflight(fixture, operation, state).await;
    assert!(
        prepared.identity_current,
        "{operation} identity is not current"
    );
    assert!(
        prepared.coverage_exact,
        "{operation} saved coverage is incomplete"
    );
}

// Finish intentionally mutable marker-0 fixture metadata before publication.
async fn finish_checkpoint_fixture(fixture: &Fixture, id: &str) -> String {
    let checkpoint = fixture
        .store
        .compaction_checkpoint(id)
        .await
        .unwrap()
        .unwrap();
    let identity = crate::repositories::compaction::checkpoint_identity(&checkpoint).unwrap();
    fixture
        .db()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE compaction_checkpoint SET identity_sha256=?1 WHERE id=?2 AND proof_version=0",
            [identity.clone().into(), id.into()],
        ))
        .await
        .unwrap();
    identity
}

async fn install_verified_projection(
    fixture: &Fixture,
    operation: &str,
    reference: &serde_json::Value,
) {
    use sha2::{Digest, Sha256};
    let typed: pioneer_compaction::frozen::FrozenMessageRef =
        serde_json::from_value(reference.clone()).unwrap();
    typed.validate().unwrap();
    let canonical = serde_json::to_vec(&typed).unwrap();
    let mut digest = Sha256::new();
    digest.update((canonical.len() as u64).to_be_bytes());
    digest.update(&canonical);
    let identity = hex::encode(digest.finalize());
    let json = serde_json::to_string(&typed).unwrap();
    let manifest = format!("{operation}-verified-origin");
    let db = fixture.db();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,import_count,imports_sha256,ready,next_ordinal,next_import) VALUES (?1,'ws','root-thread',?2,1,0,?3,1,1,0)",
        [manifest.clone().into(),identity.clone().into(),hex::encode(Sha256::digest([])).into()])).await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES (?1,0,?2,?3)",
        [manifest.clone().into(),json.clone().into(),(json.len() as i64).into()])).await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) VALUES (?1,?2,?3,?4,0)",
        [operation.into(),manifest.into(),identity.into(),hex::encode(Sha256::digest([])).into()])).await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO compaction_runner_plan(operation_id,source_count,reference_count,descriptor,ready) VALUES (?1,1,0,'fixture ready plan',1)",
        [operation.into()])).await.unwrap();
}

async fn bind_reference_checkpoint_chain(fixture: &Fixture, operation: &str, nodes: usize) {
    assert!(nodes > 0);
    let db = fixture.db();
    let mut previous = "source-checkpoint".to_owned();
    for ordinal in 0..nodes {
        let id = format!("{operation}-dag-{ordinal}");
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO compaction_checkpoint(\
              id,operation_id,owner,previous,portion,summary,identity_sha256,selection,\
              projection_version,format_version,status) \
             VALUES (?,'source-operation','source-owner',?,?, 'dag',?,'{}',0,1,'applied')",
            [
                id.clone().into(),
                previous.into(),
                (ordinal as i64 + 1).into(),
                format!("{operation}-dag-version-{ordinal}").into(),
            ],
        ))
        .await
        .unwrap();
        previous = id;
    }
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_manifest SET reference_only=1,source_thread='source-thread',\
          source_scope='checkpoint:source-owner',source_id=?,source_version=? \
         WHERE operation_id=? AND ordinal=0",
        [
            previous.into(),
            format!("{operation}-dag-version-{}", nodes - 1).into(),
            operation.into(),
        ],
    ))
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "DELETE FROM compaction_coverage WHERE checkpoint_id=? AND source_id=?",
        [
            format!("{operation}-checkpoint").into(),
            format!("{operation}-source-0").into(),
        ],
    ))
    .await
    .unwrap();
    finish_checkpoint_fixture(fixture, &format!("{operation}-checkpoint")).await;
}

#[tokio::test]
async fn completed_summary_publishes_after_selected_source_edits_and_deletes() {
    use crate::repositories::compaction::CommitOutcome;

    for mutation in [
        "UPDATE turn_event SET payload='{\"edited\":true}' WHERE id='publication-snapshot-source-0'",
        "DELETE FROM turn_event WHERE id='publication-snapshot-source-0'",
        "DELETE FROM turn_event WHERE id='publication-snapshot-source-0'; INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('publication-snapshot-source-0','root-thread','root-turn',10000,'fixture','replacement',CURRENT_TIMESTAMP)",
    ] {
        for phase in [
            None,
            Some(PublicationTestPause::ReaderPreflight),
            Some(PublicationTestPause::BeforeWriter),
        ] {
            let fixture = fixture().await;
            let operation = "publication-snapshot";
            let state = publication_candidate(&fixture, operation, 1).await;
            let outcome = if let Some(phase) = phase {
                let mut hook = fixture.arm_publication_hook(operation, phase);
                let store = fixture.store.clone();
                let captured = state.clone();
                let task = tokio::spawn(async move {
                    store
                        .compaction_apply_runner(operation, &captured, None)
                        .await
                        .unwrap()
                });
                hook.reached().await;
                tokio::time::timeout(
                    Duration::from_secs(1),
                    fixture.db().execute_unprepared(mutation),
                )
                .await
                .expect("publication retained database capacity before its writer")
                .unwrap();
                hook.release();
                task.await.unwrap()
            } else {
                fixture.db().execute_unprepared(mutation).await.unwrap();
                fixture
                    .store
                    .compaction_apply_runner(operation, &state, None)
                    .await
                    .unwrap()
            };
            assert_eq!(outcome, CommitOutcome::Applied);
            let checkpoint = fixture
                .store
                .compaction_checkpoint("publication-snapshot-checkpoint")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(checkpoint.summary, "prepared summary");
            assert_eq!(checkpoint.coverage[0].version, "event-revision:1");
            let applied = fixture
                .store
                .compaction_runner_state(operation)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(applied.attempts, state.attempts);
            assert_eq!(applied.retries, state.retries);
            assert!(matches!(applied.phase, RunnerPhase::Applied { .. }));
            assert_eq!(
                fixture
                    .store
                    .compaction_apply_runner(operation, &state, None)
                    .await
                    .unwrap(),
                CommitOutcome::AlreadyApplied
            );
        }
    }
}

#[tokio::test]
async fn unrelated_streaming_and_compaction_work_does_not_delay_publication() {
    use crate::repositories::compaction::CommitOutcome;
    let fixture = fixture().await;
    let operation = "publication-unrelated-work";
    let state = publication_candidate(&fixture, operation, 1).await;
    let mut hook = fixture.arm_publication_hook(operation, PublicationTestPause::BeforeWriter);
    let store = fixture.store.clone();
    let captured = state.clone();
    let task = tokio::spawn(async move {
        store
            .compaction_apply_runner(operation, &captured, None)
            .await
            .unwrap()
    });
    hook.reached().await;
    for _ in 0..20 {
        fixture.db().execute_unprepared(
            "UPDATE turn_llm_context SET payload=payload||' ' WHERE id='context-source'; UPDATE compaction_checkpoint SET status=status WHERE id='source-checkpoint'"
        ).await.unwrap();
    }
    hook.release();
    assert_eq!(task.await.unwrap(), CommitOutcome::Applied);
    assert_eq!(
        fixture
            .store
            .compaction_runner_state(operation)
            .await
            .unwrap()
            .unwrap()
            .attempts,
        state.attempts
    );
}

#[tokio::test]
async fn publication_keeps_atomic_cancellation_generation_identity_and_head_guards() {
    use crate::repositories::compaction::CommitOutcome;
    for (mutation, expected) in [
        (
            "UPDATE compaction_operation SET status='cancelled' WHERE id='publication-control'",
            CommitOutcome::Cancelled,
        ),
        (
            "INSERT INTO compaction_execution_stop(owner,turn_id) VALUES ('root-owner','root-turn')",
            CommitOutcome::Cancelled,
        ),
        (
            "UPDATE turn SET status='interrupted' WHERE id='root-turn'",
            CommitOutcome::Cancelled,
        ),
        (
            "UPDATE compaction_runner_state SET generation=generation+1 WHERE operation_id='publication-control'",
            CommitOutcome::Stale,
        ),
        (
            "UPDATE compaction_operation SET expected_head='source-checkpoint' WHERE id='publication-control'",
            CommitOutcome::Stale,
        ),
        (
            "UPDATE compaction_context SET head='source-checkpoint' WHERE owner='root-owner'",
            CommitOutcome::Stale,
        ),
    ] {
        let fixture = fixture().await;
        let operation = "publication-control";
        let state = publication_candidate(&fixture, operation, 1).await;
        fixture.db().execute_unprepared("UPDATE compaction_operation SET execution_turn='root-turn' WHERE id='publication-control'").await.unwrap();
        let mut hook = fixture.arm_publication_hook(operation, PublicationTestPause::BeforeWriter);
        let store = fixture.store.clone();
        let task = tokio::spawn(async move {
            store
                .compaction_apply_runner(operation, &state, None)
                .await
                .unwrap()
        });
        hook.reached().await;
        fixture.db().execute_unprepared(mutation).await.unwrap();
        hook.release();
        assert_eq!(task.await.unwrap(), expected, "{mutation}");
        let candidate = fixture
            .store
            .compaction_checkpoint("publication-control-checkpoint")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(candidate.summary, "prepared summary");
        assert_ne!(
            fixture
                .store
                .compaction_operation(operation)
                .await
                .unwrap()
                .unwrap()
                .status,
            "completed"
        );
    }
}

#[tokio::test]
async fn publication_rejects_changed_candidate_owner_before_seal_and_guard_blocks_it_after_seal() {
    use crate::repositories::compaction::CommitOutcome;
    let fixture = fixture().await;
    let operation = "publication-owner-before";
    let state = publication_candidate(&fixture, operation, 1).await;
    fixture.db().execute_unprepared("UPDATE compaction_checkpoint SET owner='source-owner' WHERE id='publication-owner-before-checkpoint'").await.unwrap();
    assert_eq!(
        fixture
            .store
            .compaction_apply_runner(operation, &state, None)
            .await
            .unwrap(),
        CommitOutcome::Stale
    );
    assert_ne!(
        fixture
            .store
            .compaction_operation(operation)
            .await
            .unwrap()
            .unwrap()
            .status,
        "completed"
    );

    let fixture = self::fixture().await;
    let operation = "publication-owner-after";
    let state = publication_candidate(&fixture, operation, 1).await;
    assert_positive_publication_preflight(&fixture, operation, &state).await;
    let error = fixture.db().execute_unprepared("UPDATE compaction_checkpoint SET owner='source-owner' WHERE id='publication-owner-after-checkpoint'").await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("sealed checkpoint identity immutable")
    );
    assert_eq!(
        fixture
            .store
            .compaction_apply_runner(operation, &state, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
}

#[tokio::test]
async fn publication_rejects_incomplete_saved_coverage_without_reading_live_sources() {
    use crate::repositories::compaction::CommitOutcome;
    let fixture = fixture().await;
    let operation = "publication-incomplete";
    let state = publication_candidate(&fixture, operation, 2).await;
    fixture.db().execute_unprepared("DELETE FROM compaction_coverage WHERE checkpoint_id='publication-incomplete-checkpoint' AND source_id='publication-incomplete-source-1'").await.unwrap();
    assert_eq!(
        fixture
            .store
            .compaction_apply_runner(operation, &state, None)
            .await
            .unwrap(),
        CommitOutcome::Stale
    );
    assert!(
        fixture
            .store
            .compaction_head("root-owner")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fixture
            .store
            .compaction_runner_state(operation)
            .await
            .unwrap()
            .unwrap(),
        state
    );
}

#[tokio::test]
async fn concurrent_success_is_already_applied() {
    use crate::repositories::compaction::CommitOutcome;

    let fixture = fixture().await;
    let operation = "publication-concurrent-success";
    let state = publication_candidate(&fixture, operation, 1).await;
    assert_positive_publication_preflight(&fixture, operation, &state).await;
    let checkpoint = format!("{operation}-checkpoint");
    let applied_state = state.applied(&checkpoint).unwrap();
    let mut hook = fixture.arm_publication_hook(operation, PublicationTestPause::BeforeWriter);
    let store = fixture.store.clone();
    let state_for_task = state.clone();
    let apply = tokio::spawn(async move {
        store
            .compaction_apply_runner(operation, &state_for_task, None)
            .await
            .unwrap()
    });
    hook.reached().await;

    let txn = fixture.db().begin().await.unwrap();
    txn.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_context SET head=? WHERE owner='root-owner' AND head IS NULL",
        [checkpoint.clone().into()],
    ))
    .await
    .unwrap();
    txn.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_checkpoint SET status='applied' WHERE id=?",
        [checkpoint.clone().into()],
    ))
    .await
    .unwrap();
    txn.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_operation SET status='completed',outcome='applied' WHERE id=?",
        [operation.into()],
    ))
    .await
    .unwrap();
    txn.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_runner_state SET generation=?,state=? WHERE operation_id=?",
        [
            i64::try_from(applied_state.generation).unwrap().into(),
            serde_json::to_string(&applied_state).unwrap().into(),
            operation.into(),
        ],
    ))
    .await
    .unwrap();
    txn.commit().await.unwrap();

    hook.release();
    assert_eq!(apply.await.unwrap(), CommitOutcome::AlreadyApplied);
}

#[tokio::test]
async fn publication_hook_is_scoped_to_one_database_with_reused_operation_id() {
    use crate::repositories::compaction::CommitOutcome;

    let first = fixture().await;
    let second = fixture().await;
    let operation = "publication-shared-operation-id";
    let first_state = publication_candidate(&first, operation, 1).await;
    let second_state = publication_candidate(&second, operation, 1).await;
    let mut hook = first.arm_publication_hook(operation, PublicationTestPause::BeforeWriter);

    let second_outcome = tokio::time::timeout(
        Duration::from_secs(10),
        second
            .store
            .compaction_apply_runner(operation, &second_state, None),
    )
    .await
    .expect("a hook from another database intercepted publication")
    .unwrap();
    assert_eq!(second_outcome, CommitOutcome::Applied);

    let first_store = first.store.clone();
    let first_apply = tokio::spawn(async move {
        first_store
            .compaction_apply_runner(operation, &first_state, None)
            .await
            .unwrap()
    });
    hook.reached().await;
    hook.release();
    assert_eq!(first_apply.await.unwrap(), CommitOutcome::Applied);
}

#[tokio::test]
async fn publication_hook_is_consumed_by_only_one_concurrent_publication() {
    let fixture = fixture().await;
    let operation = "publication-single-consumer-hook";
    let mut hook = fixture.arm_publication_hook(operation, PublicationTestPause::BeforeWriter);
    let (completed, mut received) = tokio::sync::mpsc::unbounded_channel();
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let store = fixture.store.clone();
        let completed = completed.clone();
        tasks.push(tokio::spawn(async move {
            trigger_publication_test_hook(&store, operation, PublicationTestPause::BeforeWriter)
                .await;
            completed.send(()).unwrap();
        }));
    }
    drop(completed);
    hook.reached().await;

    tokio::time::timeout(Duration::from_secs(10), received.recv())
        .await
        .expect("both concurrent callers consumed one hook")
        .expect("unpaused hook caller ended without reporting completion");
    let mut replacement =
        fixture.arm_publication_hook(operation, PublicationTestPause::BeforeWriter);
    hook.release();
    tokio::time::timeout(Duration::from_secs(10), received.recv())
        .await
        .expect("the paused hook caller did not resume")
        .expect("paused hook caller ended without reporting completion");
    for task in tasks {
        task.await.unwrap();
    }

    let store = fixture.store.clone();
    let replacement_task = tokio::spawn(async move {
        trigger_publication_test_hook(&store, operation, PublicationTestPause::BeforeWriter).await;
    });
    replacement.reached().await;
    replacement.release();
    replacement_task.await.unwrap();
}

#[tokio::test]
async fn aborted_publication_hook_can_be_rearmed_without_leaking_registration() {
    use crate::repositories::compaction::CommitOutcome;

    let fixture = fixture().await;
    let operation = "publication-aborted-hook";
    let state = publication_candidate(&fixture, operation, 1).await;
    let cancelled = fixture.arm_publication_hook(operation, PublicationTestPause::BeforeWriter);
    drop(cancelled);
    let mut aborted_hook =
        fixture.arm_publication_hook(operation, PublicationTestPause::BeforeWriter);
    let store = fixture.store.clone();
    let aborted_state = state.clone();
    let aborted = tokio::spawn(async move {
        store
            .compaction_apply_runner(operation, &aborted_state, None)
            .await
    });
    aborted_hook.reached().await;
    aborted.abort();
    assert!(aborted.await.unwrap_err().is_cancelled());
    drop(aborted_hook);

    let mut replacement =
        fixture.arm_publication_hook(operation, PublicationTestPause::BeforeWriter);
    let store = fixture.store.clone();
    let replacement_state = state.clone();
    let apply = tokio::spawn(async move {
        store
            .compaction_apply_runner(operation, &replacement_state, None)
            .await
            .unwrap()
    });
    replacement.reached().await;
    replacement.release();
    assert_eq!(apply.await.unwrap(), CommitOutcome::Applied);
}

#[tokio::test]
async fn writer_publication_boundary_has_no_heavy_checks_as_manifest_and_dag_grow() {
    use crate::repositories::compaction::CommitOutcome;

    for (operation, sources) in [("publication-small", 1), ("publication-large", 255)] {
        let fixture = fixture().await;
        let state = publication_candidate(&fixture, operation, sources).await;
        reset_publication_test_metrics(operation);
        assert_eq!(
            fixture
                .store
                .compaction_apply_runner(operation, &state, None)
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
        let metrics = publication_test_metrics(operation);
        assert_eq!(metrics.coverage_checks, 1);
        assert_eq!(metrics.manifest_checks, 0);
        assert_eq!(metrics.heavy_checks_while_writer, 0);
        assert_eq!(metrics.proof_seal_writer_entries, 1);
        assert!(metrics.topology_pages > 0 && metrics.dependency_checks > 0);
        assert_eq!(
            metrics.writer_entries,
            metrics.proof_seal_writer_entries + 1
        );
    }

    for (operation, nodes) in [("publication-dag-small", 1), ("publication-dag-large", 257)] {
        let fixture = fixture().await;
        let state = publication_candidate(&fixture, operation, 2).await;
        bind_reference_checkpoint_chain(&fixture, operation, nodes).await;
        reset_publication_test_metrics(operation);
        assert_eq!(
            fixture
                .store
                .compaction_apply_runner(operation, &state, None)
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
        let metrics = publication_test_metrics(operation);
        assert_eq!(metrics.coverage_checks, 1);
        assert_eq!(metrics.manifest_checks, 0);
        assert_eq!(metrics.heavy_checks_while_writer, 0);
        assert_eq!(
            metrics.proof_seal_writer_entries, 1,
            "actual new seal must be observed"
        );
        assert_eq!(
            metrics.writer_entries,
            metrics.proof_seal_writer_entries + 1,
            "writer traversed {nodes} DAG nodes"
        );
    }
}

#[tokio::test]
async fn retired_publication_fence_triggers_are_removed_and_migration_is_idempotent() {
    async fn trigger_count(db: &SqliteDatabase) -> i64 {
        db.query_one_raw(Statement::from_string(DbBackend::Sqlite,
            "SELECT count(*) AS n FROM sqlite_schema WHERE type='trigger' AND name GLOB 'compaction_publication_*'".to_owned()))
            .await.unwrap().unwrap().try_get("", "n").unwrap()
    }
    async fn generation(db: &SqliteDatabase) -> i64 {
        db.query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT structural_generation FROM compaction_publication_fence WHERE singleton=1"
                .to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "structural_generation")
        .unwrap()
    }
    for compressed in [false, true] {
        let fixture = fixture().await;
        let db = fixture.db();
        if compressed {
            enable_and_physically_compress_canonical_payloads(&db).await;
        }
        assert_eq!(trigger_count(&db).await, 0);
        let before = generation(&db).await;
        let table = if compressed {
            "_turn_event_zstd"
        } else {
            "turn_event"
        };
        db.execute_unprepared(&format!("CREATE TRIGGER compaction_publication_test AFTER UPDATE ON {table} BEGIN UPDATE compaction_publication_fence SET structural_generation=structural_generation+1 WHERE singleton=1; END")).await.unwrap();
        let migration = Migrator::migrations()
            .into_iter()
            .find(|migration| {
                migration.name() == "m20261008_000009_retire_compaction_publication_fences"
            })
            .expect("publication fence retirement migration is registered");
        for _ in 0..2 {
            // Reapply through the supported serialized migration executor.
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "DELETE FROM seaql_migrations WHERE version=?",
                [migration.name().into()],
            ))
            .await
            .unwrap();
            fixture
                .writer
                .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
                .await
                .unwrap();
        }
        assert_eq!(trigger_count(&db).await, 0);
        db.execute_unprepared("UPDATE turn_event SET payload='edited' WHERE id='event-source'")
            .await
            .unwrap();
        assert_eq!(
            generation(&db).await,
            before,
            "retired counters must no longer add write work"
        );
    }
}

#[tokio::test]
async fn checkpoint_projection_metadata_queries_seek_one_manifest_and_ordinal_page() {
    let fixture = fixture().await;
    let db = fixture.db();
    seed_plan_noise(&db).await;
    for (mut statement, bounds) in [
        (
            checkpoint_projection_page_sizes_statement("plan-manifest", 7),
            ProjectionPageBounds::Sizes { start: 7 },
        ),
        (
            checkpoint_projection_page_statement("plan-manifest", 7, 128),
            ProjectionPageBounds::Data { start: 7, end: 128 },
        ),
    ] {
        assert_projection_page_statement(&statement, "plan-manifest", bounds);
        statement.sql = format!("EXPLAIN QUERY PLAN {}", statement.sql);
        let plan = db
            .query_all_raw(statement)
            .await
            .unwrap()
            .into_iter()
            .map(|row| PlanNode {
                id: row.try_get("", "id").unwrap(),
                parent: row.try_get("", "parent").unwrap(),
                detail: row.try_get("", "detail").unwrap(),
            })
            .collect::<Vec<_>>();
        assert_projection_page_plan(&plan, bounds);
    }
}

#[test]
fn projection_page_plan_checks_do_not_borrow_missing_bounds_from_other_nodes() {
    let ordinary_manifest_only = synthetic_frozen_view_plan("projection-page", 100, true);
    assert!(
        std::panic::catch_unwind(|| assert_projection_page_plan(
            &ordinary_manifest_only,
            ProjectionPageBounds::Data { start: 7, end: 128 }
        ))
        .is_err(),
        "a manifest-only ordinary SEARCH must fail even when shared is fully bounded"
    );

    let mut missing_lower = synthetic_frozen_view_plan("projection-page", 200, true);
    missing_lower
        .iter_mut()
        .find(|node| node.id == 203)
        .unwrap()
        .detail =
        "SEARCH d USING INDEX frozen_data (manifest_id=? AND ordinal>? AND ordinal<?)".into();
    missing_lower
        .iter_mut()
        .find(|node| node.id == 207)
        .unwrap()
        .detail = "SEARCH d USING INDEX frozen_data (manifest_id=? AND ordinal<?)".into();
    assert!(
        std::panic::catch_unwind(|| assert_projection_page_plan(
            &missing_lower,
            ProjectionPageBounds::Data { start: 7, end: 128 }
        ))
        .is_err(),
        "a data page missing its lower boundary must be rejected"
    );

    let mut missing_upper = synthetic_frozen_view_plan("projection-page", 300, true);
    missing_upper
        .iter_mut()
        .find(|node| node.id == 303)
        .unwrap()
        .detail =
        "SEARCH d USING INDEX frozen_data (manifest_id=? AND ordinal>? AND ordinal<?)".into();
    missing_upper
        .iter_mut()
        .find(|node| node.id == 307)
        .unwrap()
        .detail = "SEARCH d USING INDEX frozen_data (manifest_id=? AND ordinal>?)".into();
    assert!(
        std::panic::catch_unwind(|| assert_projection_page_plan(
            &missing_upper,
            ProjectionPageBounds::Data { start: 7, end: 128 }
        ))
        .is_err(),
        "a data page missing its upper boundary must be rejected"
    );

    let mut neighboring_range = synthetic_frozen_view_plan("projection-page", 400, true);
    neighboring_range.push(PlanNode {
        id: 409,
        parent: 402,
        detail: "SEARCH other USING INDEX unrelated (ordinal>? AND ordinal<?)".into(),
    });
    assert!(
        std::panic::catch_unwind(|| assert_projection_page_plan(
            &neighboring_range,
            ProjectionPageBounds::Data { start: 7, end: 128 }
        ))
        .is_err(),
        "ranges in shared or unrelated nodes must not satisfy ordinary data lookup"
    );

    let missing_size_bound = Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "SELECT ordinal,bytes FROM compaction_frozen_message WHERE manifest_id=? ORDER BY ordinal LIMIT ?",
        [
            Value::from("plan-manifest"),
            Value::from(SOURCE_PAGE_ROWS as i64),
        ],
    );
    assert!(
        std::panic::catch_unwind(|| assert_projection_page_statement(
            &missing_size_bound,
            "plan-manifest",
            ProjectionPageBounds::Sizes { start: 7 }
        ))
        .is_err()
    );
    let missing_data_upper = Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "SELECT ordinal,reference_json,bytes FROM compaction_frozen_message WHERE manifest_id=? AND ordinal>=? ORDER BY ordinal",
        [Value::from("plan-manifest"), Value::from(7_i64)],
    );
    assert!(
        std::panic::catch_unwind(|| assert_projection_page_statement(
            &missing_data_upper,
            "plan-manifest",
            ProjectionPageBounds::Data { start: 7, end: 128 }
        ))
        .is_err()
    );
}

#[test]
fn projection_page_plan_recognizes_bounded_compound_and_merge_forms() {
    let mut compound = synthetic_frozen_view_plan("projection-page", 500, true);
    compound
        .iter_mut()
        .find(|node| node.id == 503)
        .unwrap()
        .detail =
        "SEARCH d USING INDEX frozen_data (manifest_id=? AND ordinal>? AND ordinal<?)".into();
    let merged = vec![
        PlanNode {
            id: 600,
            parent: 0,
            detail: "MERGE (UNION ALL)".into(),
        },
        PlanNode {
            id: 601,
            parent: 600,
            detail: "LEFT".into(),
        },
        PlanNode {
            id: 602,
            parent: 601,
            detail: "SEARCH d USING INDEX frozen_data (manifest_id=? AND ordinal>? AND ordinal<?)"
                .into(),
        },
        PlanNode {
            id: 603,
            parent: 601,
            detail: "SEARCH l USING INDEX frozen_layout (manifest_id=? AND kind=?)".into(),
        },
        PlanNode {
            id: 604,
            parent: 600,
            detail: "RIGHT".into(),
        },
        PlanNode {
            id: 605,
            parent: 604,
            detail: "SEARCH s USING INDEX frozen_span (manifest_id=? AND kind=?)".into(),
        },
        PlanNode {
            id: 606,
            parent: 604,
            detail: "SEARCH d USING INDEX frozen_data (manifest_id=? AND ordinal>? AND ordinal<?)"
                .into(),
        },
        PlanNode {
            id: 607,
            parent: 604,
            detail: "SEARCH l USING INDEX frozen_layout (manifest_id=? AND kind=?)".into(),
        },
    ];

    for plan in [&compound, &merged] {
        assert_projection_page_plan(plan, ProjectionPageBounds::Sizes { start: 7 });
        assert_projection_page_plan(plan, ProjectionPageBounds::Data { start: 7, end: 128 });
    }
}

struct AbortJoinOnDrop<T> {
    handle: Option<tokio::task::JoinHandle<T>>,
}

impl<T> AbortJoinOnDrop<T> {
    fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    fn handle(&mut self) -> &mut tokio::task::JoinHandle<T> {
        self.handle.as_mut().expect("lookup task already consumed")
    }

    async fn finish(mut self, diagnostic: &'static str) -> T {
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), self.handle())
            .await
            .expect(diagnostic)
            .expect("projection lookup task panicked");
        self.handle.take();
        result
    }
}

impl<T> Drop for AbortJoinOnDrop<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

#[tokio::test]
async fn checkpoint_projection_page_hook_is_token_owned_and_cancellation_safe() {
    let primary_fixture = fixture().await;
    let db = primary_fixture.db();

    let dropped = arm_checkpoint_projection_page_test_hook(&db, "drop-before-capture");
    drop(dropped);
    drop(arm_checkpoint_projection_page_test_hook(
        &db,
        "drop-before-capture",
    ));

    let secondary_fixture = fixture().await;
    let other_db = secondary_fixture.db();
    let first_database = arm_checkpoint_projection_page_test_hook(&db, "same-manifest");
    let second_database = arm_checkpoint_projection_page_test_hook(&other_db, "same-manifest");
    drop(first_database);
    drop(second_database);

    let mut duplicate_owner = arm_checkpoint_projection_page_test_hook(&db, "duplicate");
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            arm_checkpoint_projection_page_test_hook(&db, "duplicate")
        }))
        .is_err(),
        "duplicate arm must fail without replacing the first registration"
    );
    let duplicate_db = db.clone();
    let mut duplicate_participant = AbortJoinOnDrop::new(tokio::spawn(async move {
        checkpoint_projection_page_test_pause(&duplicate_db, "duplicate").await;
    }));
    tokio::select! {
        () = duplicate_owner.reached() => {}
        result = duplicate_participant.handle() => {
            panic!("duplicate owner registration disappeared before its barrier: {result:?}");
        }
    }
    drop(duplicate_owner);
    duplicate_participant
        .finish("participant was not released when its hook owner was dropped")
        .await;

    let mut old = arm_checkpoint_projection_page_test_hook(&db, "token-reuse");
    let old_db = db.clone();
    let mut old_participant = AbortJoinOnDrop::new(tokio::spawn(async move {
        checkpoint_projection_page_test_pause(&old_db, "token-reuse").await;
    }));
    tokio::select! {
        () = old.reached() => {}
        result = old_participant.handle() => {
            panic!("old participant ended before reaching its barrier: {result:?}");
        }
    }
    let mut replacement = arm_checkpoint_projection_page_test_hook(&db, "token-reuse");
    drop(old);
    let replacement_db = db.clone();
    let mut replacement_participant = AbortJoinOnDrop::new(tokio::spawn(async move {
        checkpoint_projection_page_test_pause(&replacement_db, "token-reuse").await;
    }));
    tokio::select! {
        () = replacement.reached() => {}
        result = replacement_participant.handle() => {
            panic!("old handle removed the replacement registration: {result:?}");
        }
    }
    drop(replacement);
    old_participant
        .finish("old participant remained blocked after owner drop")
        .await;
    replacement_participant
        .finish("replacement participant remained blocked after owner drop")
        .await;

    let waiting = arm_checkpoint_projection_page_test_hook(&db, "early-error");
    let store = primary_fixture.store.clone();
    let mut failed_participant = AbortJoinOnDrop::new(tokio::spawn(async move {
        store
            .compaction_checkpoint_edges("checkpoint-that-does-not-exist")
            .await
    }));
    let missing = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        failed_participant.handle(),
    )
    .await
    .expect("lookup that ended before the hook did not terminate before the diagnostic deadline")
    .expect("early-error participant panicked")
    .expect("early-error lookup returned a database error");
    assert!(
        missing.is_none(),
        "missing checkpoint unexpectedly reached the hook"
    );
    drop(waiting);
    drop(arm_checkpoint_projection_page_test_hook(&db, "early-error"));
}

#[tokio::test]
async fn checkpoint_projection_metadata_is_paged_deduplicated_and_releases_each_reader() {
    let fixture = fixture().await;
    let db = fixture.db();
    let count = SOURCE_PAGE_ROWS as i64 * 2 + 2;
    db.execute_raw(sqlite_specific_sql(
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES ('projection-pages','ws','source-thread','preparing',?,?,0,'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',0,0)",
        [count.into(), count.into()],
    ))
    .await
    .unwrap();
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    for ordinal in 0..count {
        let reference = serde_json::json!({
            "source_thread": "source-thread",
            "unit_id": if ordinal < SOURCE_PAGE_ROWS as i64 + 1 { "unit".to_owned() } else { "unit".repeat(2048) },
            "sources": [{"scope":"event:source-turn","id":"event-source","version":"event-revision:1"}],
            "event_input_role": "authoritative",
            "inherited": false,
            "complete": true,
            "protected_input": false,
            "wire_sha256": "a".repeat(64),
            "replay_source": {
                "scope":"item:source-turn",
                "id": if ordinal == count - 1 { "last" } else { "first" },
                "version":"item-revision:1"
            },
            "tool_item_id": "tool",
            "tool_call_id": null,
            "tool_name": null
        })
        .to_string();
        let typed: pioneer_compaction::frozen::FrozenMessageRef =
            serde_json::from_str(&reference).unwrap();
        typed.validate().unwrap();
        let canonical = serde_json::to_vec(&typed).unwrap();
        digest.update((canonical.len() as u64).to_be_bytes());
        digest.update(canonical);
        db.execute_raw(sqlite_specific_sql(
            "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES ('projection-pages',?,?,?)",
            [ordinal.into(), reference.clone().into(), (reference.len() as i64).into()],
        ))
        .await
        .unwrap();
    }

    let identity = hex::encode(digest.finalize());
    db.execute_raw(sqlite_specific_sql("UPDATE compaction_frozen_history SET identity_sha256=?,ready=1 WHERE id='projection-pages'", [identity.clone().into()])).await.unwrap();
    db.execute_raw(sqlite_specific_sql("INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) VALUES ('source-operation','projection-pages',?,'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',0)", [identity.into()])).await.unwrap();

    for shared in [false, true] {
        let observer = observe_checkpoint_projection_page_test_reads(&db, "projection-pages");
        let mut hook = arm_checkpoint_projection_page_test_hook(&db, "projection-pages");
        let store = fixture.store.clone();
        let mut lookup = AbortJoinOnDrop::new(tokio::spawn(async move {
            store.compaction_checkpoint_edges("source-checkpoint").await
        }));
        tokio::select! {
            () = hook.reached() => {}
            result = lookup.handle() => {
                panic!("projection lookup ended before the page barrier: {result:?}");
            }
        }
        let unrelated: i64 = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            db.query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT count(*) AS n FROM thread".to_owned(),
            )),
        )
        .await
        .expect("unrelated read remained blocked after the page query completed")
        .expect("unrelated read failed while projection lookup was paused")
        .expect("a completed metadata page must release the sole reader")
        .try_get("", "n")
        .unwrap();
        assert!(unrelated > 0);
        hook.release();
        let edges = lookup
            .finish("projection lookup did not finish after page-hook release")
            .await
            .expect("projection metadata lookup failed")
            .expect("source checkpoint disappeared");
        assert_eq!(edges.replay_aliases.len(), 2);
        assert_eq!(edges.replay_aliases[0].replay.source.id, "first");
        assert_eq!(edges.replay_aliases[1].replay.source.id, "last");
        assert_eq!(edges.event_input_evidence.len(), 1);
        assert!(edges.event_input_evidence.iter().all(|item| {
            item.source.source.id == "event-source" && item.source.source_thread == "source-thread"
        }));
        assert_eq!(edges.coverage.len(), 1);
        let reads = observer.reads();
        assert!(
            reads.len() >= 3,
            "expected several bounded pages: {reads:?}"
        );
        assert_eq!(reads.first().unwrap().start, 0);
        assert_eq!(reads.last().unwrap().end, count);
        assert!(reads.windows(2).all(|pair| pair[0].end == pair[1].start));
        assert!(reads.iter().all(|page| {
            page.rows <= SOURCE_PAGE_ROWS as usize && page.bytes <= SOURCE_PAGE_BYTES
        }));
        assert!(
            reads[..reads.len() - 1]
                .iter()
                .any(|page| page.rows < SOURCE_PAGE_ROWS as usize),
            "long references must exercise the byte quantum before the row quantum: {reads:?}"
        );
        drop(observer);

        if !shared {
            for sql in [
                "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) SELECT 'projection-storage',workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready FROM compaction_frozen_history WHERE id='projection-pages'",
                "INSERT INTO compaction_frozen_message_data SELECT 'projection-storage',ordinal,reference_json,bytes FROM compaction_frozen_message_data WHERE manifest_id='projection-pages'",
                "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES ('projection-pages',0,0,1)",
                "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) SELECT 'projection-pages',0,0,message_count,'projection-storage' FROM compaction_frozen_history WHERE id='projection-pages'",
                "UPDATE compaction_frozen_layout SET active=1,pending=0 WHERE manifest_id='projection-pages' AND kind=0",
                "DELETE FROM compaction_frozen_message_data WHERE manifest_id='projection-pages'",
            ] {
                db.execute_unprepared(sql).await.unwrap();
            }
        }
    }
}

// Published legacy foreign nodes, with one exact historical raw leaf each.
// The root is a saved raw assertion candidate with selected (not reference-only)
// foreign sources. Large aggregate strings fit individually in a quantum.
async fn publication_foreign_fanout(f: &Fixture, operation: &str, n: usize) -> RunnerState {
    let mut state = publication_candidate(f, operation, 1).await;
    let root = format!("{operation}-checkpoint");
    let mut checkpoint = f.store.compaction_checkpoint(&root).await.unwrap().unwrap();
    let leaf = SourceRef {
        scope: "event:source-turn".into(),
        id: "event-source".into(),
        version: "event-revision:1".into(),
    };
    let db = f.db();
    let mut sources = Vec::new();
    for i in 0..n {
        let owner = format!("{operation}-foreign-{i:04}-{}", "x".repeat(1024));
        let op = format!("{operation}-dependency-{i:04}");
        let cp = pioneer_compaction::Checkpoint {
            id: format!("{op}-cp"),
            operation_id: op.clone(),
            owner: owner.clone(),
            previous: None,
            summary: "foreign historical summary".into(),
            selection: checkpoint.selection.clone(),
            coverage: vec![leaf.clone()],
            projection_version: 0,
            format_version: 1,
        };
        let identity = checkpoint_identity(&cp).unwrap();
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES(?1,'ws','source-thread',1)",[owner.clone().into()])).await.unwrap();
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES(?1,?2,?1,'completed',?3,900000)",
            [op.clone().into(),owner.clone().into(),raw_fixture_snapshot(&op,&owner,vec![leaf.clone()]).into()])).await.unwrap();
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,selection,projection_version,format_version,identity_sha256,status) VALUES(?1,?2,?3,0,?4,?5,0,1,?6,'applied')",
            [cp.id.clone().into(),op.clone().into(),owner.clone().into(),cp.summary.clone().into(),serde_json::to_string(&cp.selection).unwrap().into(),identity.clone().into()])).await.unwrap();
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES(?1,?2,?3,?4)",
            [cp.id.clone().into(),leaf.scope.clone().into(),leaf.id.clone().into(),leaf.version.clone().into()])).await.unwrap();
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES(?1,0,0,0,'source-thread',?2,?3,?4)",
            [op.into(),leaf.scope.clone().into(),leaf.id.clone().into(),leaf.version.clone().into()])).await.unwrap();
        sources.push(SourceRef {
            scope: format!("checkpoint:{owner}"),
            id: cp.id,
            version: identity,
        });
    }
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_operation SET snapshot=?2 WHERE id=?1",
        [
            operation.into(),
            raw_fixture_snapshot(operation, "root-owner", sources.clone()).into(),
        ],
    ))
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "DELETE FROM compaction_manifest WHERE operation_id=?1",
        [operation.into()],
    ))
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "DELETE FROM compaction_coverage WHERE checkpoint_id=?1",
        [root.clone().into()],
    ))
    .await
    .unwrap();
    for (ordinal, r) in sources.iter().enumerate() {
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES(?1,?2,?2,0,'source-thread',?3,?4,?5)",
            [operation.into(),(ordinal as i64).into(),r.scope.clone().into(),r.id.clone().into(),r.version.clone().into()])).await.unwrap();
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES(?1,?2,?3,?4)",
            [root.clone().into(),r.scope.clone().into(),r.id.clone().into(),r.version.clone().into()])).await.unwrap();
    }
    checkpoint.coverage = sources;
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_checkpoint SET identity_sha256=?2 WHERE id=?1",
        [
            root.into(),
            checkpoint_identity(&checkpoint).unwrap().into(),
        ],
    ))
    .await
    .unwrap();
    state.cursor.unit = n as u64;
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_runner_state SET state=?2 WHERE operation_id=?1",
        [
            operation.into(),
            serde_json::to_string(&state).unwrap().into(),
        ],
    ))
    .await
    .unwrap();
    state
}
#[tokio::test]
async fn large_selected_foreign_fanout_pages_every_set_and_observes_constant_actual_seal_writer() {
    use crate::repositories::compaction::CommitOutcome;
    for n in [2, 260] {
        let f = fixture().await;
        let operation = format!("seal-fanout-{n}");
        let state = publication_foreign_fanout(&f, &operation, n).await;
        reset_publication_test_metrics(&operation);
        assert_eq!(
            f.store
                .compaction_apply_runner(&operation, &state, None)
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
        let metrics = publication_test_metrics(&operation);
        assert_eq!(
            metrics.proof_seal_writer_entries, 1,
            "root's real seal must be cold, not precredited"
        );
        assert_eq!(
            metrics.writer_entries, 2,
            "one short root seal and one final publication"
        );
        assert_eq!(metrics.heavy_checks_while_writer, 0);
        assert!(metrics.topology_pages >= 4 && metrics.dependency_checks >= n);
        assert!(metrics.topology_page_rows <= SOURCE_PAGE_ROWS as usize);
        assert!(metrics.topology_page_bytes <= SOURCE_PAGE_BYTES);
        let root = format!("{operation}-checkpoint");
        let edges = f
            .store
            .compaction_checkpoint_edges(&root)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(edges.coverage.len(), n);
        let sealed:i64=f.db().query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "SELECT count(*) AS n FROM compaction_checkpoint WHERE id LIKE ?1 AND proof_version=1",[format!("{operation}-dependency-%").into()])).await.unwrap().unwrap().try_get("","n").unwrap();
        assert_eq!(
            sealed, n as i64,
            "all foreign dependencies were prepared, not just a final summary"
        );
    }
}
#[tokio::test]
async fn actual_proof_seal_boundary_rejects_mutation_and_stop_but_allows_historical_source_edits() {
    use crate::repositories::compaction::CommitOutcome;
    for before in [false, true] {
        let f = fixture().await;
        let operation = "seal-source-edit";
        let state = publication_candidate(&f, operation, 130).await;
        let sql = "UPDATE turn_event SET payload='changed after summary' WHERE id='seal-source-edit-source-0'";
        if before {
            f.db().execute_unprepared(sql).await.unwrap();
        }
        let mut hook =
            arm_publication_test_hook(&f.store, operation, PublicationTestPause::ProofSeal);
        let store = f.store.clone();
        let task =
            tokio::spawn(
                async move { store.compaction_apply_runner(operation, &state, None).await },
            );
        hook.reached().await;
        for sql in [
            "DELETE FROM compaction_coverage WHERE checkpoint_id='seal-source-edit-checkpoint'",
            "UPDATE compaction_coverage SET source_version='lost' WHERE checkpoint_id='seal-source-edit-checkpoint'",
            "UPDATE compaction_manifest SET source_thread='foreign-thread' WHERE operation_id='seal-source-edit'",
            "UPDATE compaction_operation SET snapshot='{}' WHERE id='seal-source-edit'",
            "UPDATE compaction_operation SET next_portion=0 WHERE id='seal-source-edit'",
        ] {
            assert!(
                f.db().execute_unprepared(sql).await.is_err(),
                "prepared sets must be immutable"
            );
        }
        if !before {
            f.db().execute_unprepared(sql).await.unwrap();
        }
        hook.release();
        assert_eq!(task.await.unwrap().unwrap(), CommitOutcome::Applied);
    }
    for cancel_first in [false, true] {
        let f = fixture().await;
        let operation = "seal-stop";
        let state = publication_candidate(&f, operation, 130).await;
        if cancel_first {
            f.db()
                .execute_unprepared(
                    "UPDATE compaction_operation SET status='cancelled' WHERE id='seal-stop'",
                )
                .await
                .unwrap();
            assert!(
                f.store
                    .compaction_apply_runner(operation, &state, None)
                    .await
                    .is_err()
            );
        } else {
            let mut hook =
                arm_publication_test_hook(&f.store, operation, PublicationTestPause::ProofSeal);
            let store = f.store.clone();
            let task = tokio::spawn(async move {
                store.compaction_apply_runner(operation, &state, None).await
            });
            hook.reached().await;
            f.db()
                .execute_unprepared(
                    "UPDATE compaction_operation SET status='cancelled' WHERE id='seal-stop'",
                )
                .await
                .unwrap();
            hook.release();
            assert!(task.await.unwrap().is_err());
        }
        assert_eq!(f.store.compaction_head("root-owner").await.unwrap(), None);
        let version: i64 = f
            .db()
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT proof_version FROM compaction_checkpoint WHERE id='seal-stop-checkpoint'",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "proof_version")
            .unwrap();
        assert_eq!(version, 0, "Stop must not seal a stale preparation");
    }
}
#[tokio::test]
async fn proof_seal_cancelled_preparation_and_marker_rollback_restart_exactly() {
    use crate::repositories::compaction::CommitOutcome;
    for rollback in [false, true] {
        let f = fixture().await;
        let operation = "seal-restart";
        let state = publication_candidate(&f, operation, 130).await;
        if rollback {
            f.db().execute_unprepared("CREATE TRIGGER fixture_seal_fault BEFORE UPDATE OF proof_version ON compaction_checkpoint WHEN NEW.id='seal-restart-checkpoint' AND NEW.proof_version=1 BEGIN SELECT RAISE(ABORT,'seal fixture fault'); END").await.unwrap();
            assert!(
                f.store
                    .compaction_apply_runner(operation, &state, None)
                    .await
                    .is_err()
            );
            f.db()
                .execute_unprepared("DROP TRIGGER fixture_seal_fault")
                .await
                .unwrap();
        } else {
            let mut hook =
                arm_publication_test_hook(&f.store, operation, PublicationTestPause::ProofSeal);
            let store = f.store.clone();
            let saved = state.clone();
            let task = tokio::spawn(async move {
                store.compaction_apply_runner(operation, &saved, None).await
            });
            hook.reached().await;
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            drop(hook);
        }
        let marker:i64=f.db().query_one_raw(Statement::from_string(DbBackend::Sqlite,"SELECT proof_version FROM compaction_checkpoint WHERE id='seal-restart-checkpoint'")).await.unwrap().unwrap().try_get("","proof_version").unwrap();
        assert_eq!(marker, 0);
        assert_eq!(f.store.compaction_head("root-owner").await.unwrap(), None);
        assert_eq!(
            f.store
                .compaction_apply_runner(operation, &state, None)
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
    }
}

#[tokio::test]
async fn topology_pages_keep_domain_bounds_and_reject_single_poison_or_ambiguous_ownership() {
    for poison in ["source", "owner", "ambiguous", "version", "missing"] {
        let f = fixture().await;
        let operation = format!("topology-poison-{poison}");
        let state = publication_candidate(&f, &operation, 1).await;
        let root = format!("{operation}-checkpoint");
        let source = format!("{operation}-source-0");
        match poison {
            "source" => {
                f.db()
                    .execute_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        "UPDATE compaction_coverage SET source_id=?2 WHERE checkpoint_id=?1",
                        [
                            root.clone().into(),
                            "x".repeat(SOURCE_PAGE_BYTES + 1).into(),
                        ],
                    ))
                    .await
                    .unwrap();
            }
            "owner" => {
                f.db()
                    .execute_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        "UPDATE compaction_manifest SET source_thread=?2 WHERE operation_id=?1",
                        [
                            operation.clone().into(),
                            "x".repeat(SOURCE_PAGE_BYTES + 1).into(),
                        ],
                    ))
                    .await
                    .unwrap();
            }
            "ambiguous" => {
                f.db().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                    "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES(?1,1,0,0,'foreign-thread','event:root-turn',?2,'event-revision:1')",[operation.clone().into(),source.into()])).await.unwrap();
            }
            "version" => {
                f.db().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                    "UPDATE compaction_coverage SET source_version='lost-version' WHERE checkpoint_id=?1",[root.clone().into()])).await.unwrap();
            }
            "missing" => {
                f.db()
                    .execute_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        "DELETE FROM compaction_manifest WHERE operation_id=?1",
                        [operation.clone().into()],
                    ))
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        // Marker 0 + open portion represent an already corrupt legacy fixture.
        // New candidate writers never permit these changes after their commit.
        reset_publication_test_metrics(&operation);
        let error =
            crate::repositories::compaction::checkpoint_topology(&f.store.connection, &root)
                .await
                .err()
                .expect("poison must not become an empty set");
        if matches!(poison, "source" | "owner") {
            assert!(format!("{error:#}").contains("byte quantum"));
        }
        let result = f
            .store
            .compaction_apply_runner(&operation, &state, None)
            .await;
        assert!(
            !matches!(
                result,
                Ok(crate::repositories::compaction::CommitOutcome::Applied
                    | crate::repositories::compaction::CommitOutcome::AlreadyApplied)
            ),
            "poison must refuse publication"
        );
        let marker: i64 = f
            .db()
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT proof_version FROM compaction_checkpoint WHERE id=?1",
                [root.into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "proof_version")
            .unwrap();
        assert_eq!(marker, 0);
        assert_eq!(f.store.compaction_head("root-owner").await.unwrap(), None);
        let metrics = publication_test_metrics(&operation);
        assert!(
            metrics.topology_page_rows <= SOURCE_PAGE_ROWS as usize
                && metrics.topology_page_bytes <= SOURCE_PAGE_BYTES
        );
    }
}
#[tokio::test]
async fn legacy_topology_replacement_lost_coverage_and_version_between_pages_are_consistency_failures()
 {
    use crate::repositories::compaction::CheckpointTopologyRead;
    for mutation in [
        "header",
        "coverage",
        "version",
        "coverage-close",
        "version-close",
        "ownership-close",
        "status",
        "format",
        "unsupported-proof",
    ] {
        let f = fixture().await;
        let operation = format!("topology-page-{mutation}");
        let _ = publication_candidate(&f, &operation, 130).await;
        let root = format!("{operation}-checkpoint");
        let mut read = CheckpointTopologyRead::new(&f.store.connection, &root)
            .await
            .unwrap()
            .unwrap();
        read.step(&f.store.connection).await.unwrap();
        assert!(
            !read.done,
            "one worker step must not read the whole checkpoint"
        );
        if mutation == "ownership-close" {
            // Observe actual owners before they are changed in the open legacy
            // portion. Close must recheck these, not just the coverage keys.
            for _ in 0..3 {
                read.step(&f.store.connection).await.unwrap();
            }
        }
        let first = format!("{operation}-source-0");
        let sql = match mutation {
            "header" => {
                "UPDATE compaction_checkpoint SET identity_sha256='replaced' WHERE id=?1 AND ?2 IS NOT NULL"
            }
            "coverage" | "coverage-close" => {
                "DELETE FROM compaction_coverage WHERE checkpoint_id=?1 AND source_id=?2"
            }
            "version" | "version-close" => {
                "UPDATE compaction_coverage SET source_version='changed' WHERE checkpoint_id=?1 AND source_id=?2"
            }
            "ownership-close" => {
                "UPDATE compaction_manifest SET source_thread='source-thread' WHERE operation_id=(SELECT operation_id FROM compaction_checkpoint WHERE id=?1) AND source_id=?2"
            }
            "status" => {
                "UPDATE compaction_checkpoint SET status='stale' WHERE id=?1 AND ?2 IS NOT NULL"
            }
            "format" => {
                "UPDATE compaction_checkpoint SET format_version=2 WHERE id=?1 AND ?2 IS NOT NULL"
            }
            "unsupported-proof" => {
                "UPDATE compaction_checkpoint SET proof_version=2 WHERE id=?1 AND ?2 IS NOT NULL"
            }
            _ => unreachable!(),
        };
        if mutation == "unsupported-proof" {
            let mut invalid = read.row.clone();
            invalid.proof_version = 2;
            assert!(
                crate::repositories::compaction::refresh_checkpoint_row(
                    &f.store.connection,
                    &invalid
                )
                .await
                .is_err()
            );
            assert!(
                f.db()
                    .execute_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        sql,
                        [root.clone().into(), first.into()]
                    ))
                    .await
                    .is_err(),
                "schema also rejects unsupported durable marker"
            );
            continue;
        }
        f.db()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                sql,
                [root.clone().into(), first.into()],
            ))
            .await
            .unwrap();
        if mutation.ends_with("-close") {
            let mut preparation =
                crate::repositories::checkpoint_proofs::GraphPreparation::new(&root);
            preparation.step(&f.store).await.unwrap(); // real freeze, guards live
        }
        let mut failed = false;
        for _ in 0..10 {
            match read.step(&f.store.connection).await {
                Err(_) => {
                    failed = true;
                    break;
                }
                Ok(()) => {
                    if read.done {
                        break;
                    }
                }
            }
        }
        assert!(
            failed,
            "{mutation} cannot silently finish a mixed legacy topology"
        );
    }
}

#[tokio::test]
async fn prepared_unsealed_parent_edges_prevent_dependency_demotion_before_actual_seal() {
    use crate::repositories::compaction::CommitOutcome;
    let f = fixture().await;
    let operation = "seal-incoming";
    let state = publication_foreign_fanout(&f, operation, 2).await;
    let mut hook = arm_publication_test_hook(&f.store, operation, PublicationTestPause::ProofSeal);
    let store = f.store.clone();
    let task =
        tokio::spawn(async move { store.compaction_apply_runner(operation, &state, None).await });
    hook.reached().await;
    // Remove other published-root predicates in this isolated fixture, so the
    // guard is proved specifically by incoming coverage from a marker-0 node.
    f.db().execute_unprepared("UPDATE compaction_checkpoint SET status='candidate' WHERE id='seal-incoming-dependency-0000-cp'").await.unwrap();
    f.db().execute_unprepared("UPDATE compaction_operation SET status='failed' WHERE id='seal-incoming-dependency-0000'").await.unwrap();
    let version: i64 = f
        .db()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT proof_version FROM compaction_checkpoint WHERE id='seal-incoming-checkpoint'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "proof_version")
        .unwrap();
    assert_eq!(version, 0);
    assert!(f.db().execute_unprepared("UPDATE compaction_checkpoint SET proof_version=0 WHERE id='seal-incoming-dependency-0000-cp'").await.is_err());
    f.db().execute_unprepared("UPDATE compaction_operation SET status='completed' WHERE id='seal-incoming-dependency-0000'").await.unwrap();
    f.db().execute_unprepared("UPDATE compaction_checkpoint SET status='applied' WHERE id='seal-incoming-dependency-0000-cp'").await.unwrap();
    hook.release();
    assert_eq!(task.await.unwrap().unwrap(), CommitOutcome::Applied);
}

#[tokio::test]
async fn dense_selected_ownership_page_plan_seeks_thread_extrema_without_duplicate_scan() {
    use crate::repositories::compaction::CheckpointTopologyRead;
    let f = fixture().await;
    let operation = "topology-plan";
    publication_candidate(&f, operation, 130).await;
    let db = f.db();
    // Many duplicate ordinals must not make a single ownership lookup a full
    // range DISTINCT/sort. The sole thread is checked by indexed extrema.
    db.execute_unprepared("WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<2047) INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) SELECT 'topology-plan',1000+i,1000+i,0,'root-thread','event:root-turn','topology-plan-source-0','event-revision:1' FROM n").await.unwrap();
    let mut read = CheckpointTopologyRead::new(&f.store.connection, "topology-plan-checkpoint")
        .await
        .unwrap()
        .unwrap();
    // Three steps exhaust coverage and enter ownership while retaining progress.
    for _ in 0..3 {
        read.step(&f.store.connection).await.unwrap();
    }
    let mut statement = read.page_statement().unwrap();
    statement.sql = format!("EXPLAIN QUERY PLAN {}", statement.sql);
    let plan = db
        .query_all_raw(statement)
        .await
        .unwrap()
        .into_iter()
        .map(|row| PlanNode {
            id: row.try_get("", "id").unwrap(),
            parent: row.try_get("", "parent").unwrap(),
            detail: row.try_get("", "detail").unwrap(),
        })
        .collect::<Vec<_>>();
    let seeks = plan
        .iter()
        .filter(|node| {
            node.detail
                .starts_with("SEARCH m USING COVERING INDEX compaction_manifest_source")
                && node.detail.contains("operation_id=?")
                && node.detail.contains("reference_only=?")
                && node.detail.contains("source_scope=?")
                && node.detail.contains("source_id=?")
                && node.detail.contains("source_version=?")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        seeks.len(),
        2,
        "both ascending and descending owner extrema need exact indexed bounds: {plan:?}"
    );
    assert!(
        !plan.iter().any(|node| node.detail.starts_with("SCAN m")),
        "no duplicate-range scan: {plan:?}"
    );
    while !read.done {
        read.step(&f.store.connection).await.unwrap();
    }
    assert_eq!(read.finish().unwrap().ownership.len(), 130);
}

#[tokio::test]
async fn actual_freeze_and_seal_between_topology_pages_preserve_exact_edges_with_one_restart() {
    use crate::repositories::{
        checkpoint_proofs::{GraphPreparation, prepare_graph},
        compaction::{CheckpointTopologyRead, checkpoint_topology},
    };
    for (transition, observed_steps) in [
        ("freeze", 1),
        ("freeze", 4),
        ("seal", 1),
        ("seal", 4),
        ("seal-closed", 1),
        ("seal-closed", 4),
    ] {
        let f = fixture().await;
        let operation = format!("benign-{transition}-{observed_steps}");
        publication_candidate(&f, &operation, 130).await;
        let root = format!("{operation}-checkpoint");
        let original = checkpoint_topology(&f.store.connection, &root)
            .await
            .unwrap()
            .unwrap();
        if transition == "seal-closed" {
            GraphPreparation::new(&root).step(&f.store).await.unwrap();
        }
        let mut read = CheckpointTopologyRead::new(&f.store.connection, &root)
            .await
            .unwrap()
            .unwrap();
        for _ in 0..observed_steps {
            read.step(&f.store.connection).await.unwrap();
        }
        assert!(!read.done);
        let mut preparation = GraphPreparation::new(&root);
        preparation.step(&f.store).await.unwrap(); // existing serialized freeze
        if transition.starts_with("seal") {
            prepare_graph(&f.store, &root).await.unwrap();
        }
        let mut steps = 0;
        while !read.done && steps < 12 {
            read.step(&f.store.connection).await.unwrap();
            steps += 1;
        }
        assert!(
            read.done,
            "one monotonic close cannot cause endless restart"
        );
        if transition == "seal-closed" {
            assert!(
                steps <= 6 - observed_steps,
                "seal does not restart an already immutable cursor"
            );
        }
        let completed = read.finish().unwrap();
        assert_eq!(completed.ownership, original.ownership);
        assert_eq!(completed.row.identity_sha256, original.row.identity_sha256);
        assert_eq!(completed.row.coverage_closed, 1);
        assert_eq!(
            completed.row.proof_version,
            if transition.starts_with("seal") { 1 } else { 0 }
        );
        if transition == "freeze" {
            prepare_graph(&f.store, &root).await.unwrap();
        }
        // Foreground wrappers consume the same complete exact sources.
        let checkpoint = f.store.compaction_checkpoint(&root).await.unwrap().unwrap();
        let edges = f
            .store
            .compaction_checkpoint_edges(&root)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.coverage.len(), 130);
        assert_eq!(edges.coverage.len(), 130);
        assert_eq!(edges.identity_sha256, original.row.identity_sha256);
        assert_eq!(
            edges
                .coverage
                .iter()
                .map(|s| s.source.clone())
                .collect::<std::collections::BTreeSet<_>>(),
            original.ownership.keys().cloned().collect()
        );
        assert!(
            edges
                .coverage
                .iter()
                .all(|s| s.source_thread == "root-thread")
        );
        // A sealed observation accepts no demotion or scope/owner rewrite.
        assert!(f.db().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "UPDATE compaction_manifest SET source_thread='source-thread' WHERE operation_id=?1 AND ordinal=0",[operation.clone().into()])).await.is_err());
    }
}

#[tokio::test]
async fn coverage_eof_restart_rejects_extra_key_after_observed_maximum() {
    use crate::repositories::{
        checkpoint_proofs::GraphPreparation,
        compaction::{CheckpointTopologyRead, checkpoint_source_owner, checkpoint_topology},
    };
    let f = fixture().await;
    let operation = "topology-eof-extra";
    publication_candidate(&f, operation, 130).await;
    let root = format!("{operation}-checkpoint");
    let original = checkpoint_topology(&f.store.connection, &root)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.ownership.len(), 130);
    let mut read = CheckpointTopologyRead::new(&f.store.connection, &root)
        .await
        .unwrap()
        .unwrap();
    // Two coverage pages (128 + 2), coverage EOF, then the first ownership
    // page. Restart must preserve the observed full set, not just its prefix.
    for _ in 0..4 {
        read.step(&f.store.connection).await.unwrap();
    }
    assert!(!read.done);
    assert_eq!(read.row.coverage_closed, 0);
    let extra = SourceRef {
        scope: "event:root-turn".into(),
        id: format!("{operation}-source-zz-extra"),
        version: "event-revision:1".into(),
    };
    assert!(&extra > original.ownership.keys().next_back().unwrap());
    // Corrupt the still-open legacy sets without rewriting checkpoint identity
    // or disabling guards. Give the extra key a valid single historical owner
    // so current coverage/ownership equality cannot detect this discrepancy.
    f.db().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES (?1,130,130,0,'root-thread',?2,?3,?4)",
        [operation.into(),extra.scope.clone().into(),extra.id.clone().into(),extra.version.clone().into()])).await.unwrap();
    f.db().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES (?1,?2,?3,?4)",
        [root.clone().into(),extra.scope.clone().into(),extra.id.clone().into(),extra.version.clone().into()])).await.unwrap();
    assert_eq!(
        checkpoint_source_owner(&f.store.connection, &original.row, &extra)
            .await
            .unwrap(),
        "root-thread"
    );
    // Only the real freeze step runs: a later graph identity check would mask
    // the reader defect by independently rejecting the corrupted full hash.
    GraphPreparation::new(&root).step(&f.store).await.unwrap();
    read.step(&f.store.connection).await.unwrap(); // observes close; one restart
    assert_eq!(read.row.coverage_closed, 1);
    let mut identity = read.row.clone();
    identity.coverage_closed = original.row.coverage_closed;
    assert_eq!(identity, original.row);
    let mut failure = None;
    for _ in 0..12 {
        match read.step(&f.store.connection).await {
            Err(error) => {
                failure = Some(error);
                break;
            }
            Ok(()) if read.done => break,
            Ok(()) => {}
        }
    }
    let failure = failure.expect("observed coverage EOF must reject an extra key after close");
    assert!(
        failure
            .to_string()
            .contains("checkpoint coverage changed at close"),
        "the replay must reject the extra key itself: {failure}"
    );
    assert!(!read.done, "corrupt topology cannot reach finish");
}
