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
        "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) VALUES ('basis-run','task','ws','source-thread','[]',CURRENT_TIMESTAMP)",
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
        "UPDATE task_run_conversation_snapshot SET history_json='  []' WHERE run_id='basis-run'",
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
        "UPDATE task_run_conversation_snapshot SET history_json=' {}' WHERE run_id='basis-run'",
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
        "UPDATE task_run_conversation_snapshot SET history_json='[]' WHERE run_id='basis-run'",
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
            "UPDATE task_run_conversation_snapshot SET history_json=? WHERE run_id='basis-run'",
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
        "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES ('accepted-manifest',1,1,0)",
        "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES ('accepted-manifest',1,0,1,'import-storage')",
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
        "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES ('basis-manifest',0,1,0)",
        "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES ('basis-manifest',0,0,1,'basis-storage')",
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
    db.execute_unprepared(
        "UPDATE compaction_frozen_history SET next_ordinal=0 WHERE id='basis-manifest'",
    )
    .await
    .unwrap();
    assert_manifest_current(&db, "manifest-operation", false, "basis message cursor").await;
    db.execute_unprepared("UPDATE compaction_frozen_history SET next_ordinal=1,message_count=2 WHERE id='basis-manifest'")
        .await
        .unwrap();
    assert_manifest_current(&db, "manifest-operation", false, "basis message count").await;
    db.execute_unprepared(
        "UPDATE compaction_frozen_history SET message_count=1 WHERE id='basis-manifest'",
    )
    .await
    .unwrap();
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
    db.execute_unprepared("INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES ('atomic-import-manifest',1,0,'event:foreign-turn','foreign-event','event-revision:1','foreign-thread','{}',2); UPDATE compaction_frozen_history SET import_count=2,next_import=2 WHERE id='atomic-import-manifest'; UPDATE compaction_operation_projection SET import_count=2 WHERE operation_id='manifest-operation'")
        .await
        .unwrap();
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
    db.execute_unprepared("INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES ('atomic-import-manifest',2,0,'event:source-turn','event-source','event-revision:1','source-thread','{}',2); UPDATE compaction_frozen_history SET import_count=3,next_import=3 WHERE id='atomic-import-manifest'; UPDATE compaction_operation_projection SET import_count=3 WHERE operation_id='manifest-operation'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        true,
        "a separate exact direct grant authorizes current raw A",
    )
    .await;

    db.execute_unprepared("DELETE FROM compaction_frozen_import_data WHERE manifest_id='atomic-import-manifest' AND ordinal=2; UPDATE compaction_frozen_history SET import_count=2,next_import=2 WHERE id='atomic-import-manifest'; UPDATE compaction_operation_projection SET import_count=2 WHERE operation_id='manifest-operation'")
        .await
        .unwrap();
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
    db.execute_unprepared("UPDATE compaction_frozen_history SET message_count=65537,next_ordinal=65537 WHERE id='accepted-basis-boundary'")
        .await
        .unwrap();
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
    db.execute_unprepared("UPDATE compaction_frozen_history SET message_count=65537,next_ordinal=65537 WHERE id='accepted-basis-boundary'")
        .await
        .unwrap();
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
                "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES (?,?,1,0)",
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

async fn publication_candidate(fixture: &Fixture, operation: &str, sources: usize) -> RunnerState {
    assert!(sources > 0);
    let db = fixture.db();
    for ordinal in 0..sources {
        let id = format!("{operation}-source-{ordinal}");
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO turn_event(\
                id,thread_id,turn_id,sequence,event_type,payload,created_at) \
             VALUES (?,'root-thread','root-turn',?,'fixture','{}',CURRENT_TIMESTAMP)",
            [id.into(), (10_000_i64 + ordinal as i64).into()],
        ))
        .await
        .unwrap();
    }
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_operation(\
            id,owner,fingerprint,status,snapshot,deadline_ms) \
         VALUES (?,'root-owner',?,'running',\
          '{\"plan\":{\"coverage_domain\":\"own_contribution\"},\"source_epochs\":{\"root-thread\":0,\"source-thread\":0,\"foreign-thread\":0}}',900000)",
        [operation.into(), operation.into()],
    ))
    .await
    .unwrap();
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
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_checkpoint(\
            id,operation_id,owner,portion,summary,identity_sha256,selection,\
            projection_version,format_version,status) \
         VALUES (?,?,'root-owner',0,'prepared summary','candidate-identity',?,0,1,'candidate')",
        [
            checkpoint.clone().into(),
            operation.into(),
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
        prepared.sources_current,
        "{operation} fixture is stale before its race mutation"
    );
}

async fn source_fence(db: &SqliteDatabase, workspace: &str) -> (i64, i64) {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT mutation_generation,insert_generation \
             FROM compaction_publication_source_fence WHERE workspace_id=?",
            [workspace.into()],
        ))
        .await
        .unwrap()
        .unwrap();
    (
        row.try_get("", "mutation_generation").unwrap(),
        row.try_get("", "insert_generation").unwrap(),
    )
}

async fn structural_fence(db: &SqliteDatabase) -> i64 {
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

async fn bind_reference_checkpoint_dag(fixture: &Fixture, operation: &str, retained_middle: bool) {
    let middle_status = if retained_middle {
        "retained"
    } else {
        "applied"
    };
    let db = fixture.db();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_checkpoint(\
          id,operation_id,owner,previous,portion,summary,identity_sha256,selection,\
          projection_version,format_version,status) \
         VALUES ('publication-dag-mid','source-operation','source-owner','source-checkpoint',1,\
          'mid','publication-dag-mid-version','{}',0,1,?)",
        [middle_status.into()],
    ))
    .await
    .unwrap();
    db.execute_unprepared(
        "INSERT INTO compaction_checkpoint(\
          id,operation_id,owner,previous,portion,summary,identity_sha256,selection,\
          projection_version,format_version,status) \
         VALUES ('publication-dag-root','source-operation','source-owner','publication-dag-mid',2,\
          'root','publication-dag-root-version','{}',0,1,'applied')",
    )
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_manifest SET reference_only=1,source_thread='source-thread',\
          source_scope='checkpoint:source-owner',source_id='publication-dag-root',\
          source_version='publication-dag-root-version' \
          WHERE operation_id=? AND ordinal=0",
        [operation.into()],
    ))
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "DELETE FROM compaction_coverage WHERE checkpoint_id=?",
        [format!("{operation}-checkpoint").into()],
    ))
    .await
    .unwrap();
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
        "DELETE FROM compaction_coverage WHERE checkpoint_id=?",
        [format!("{operation}-checkpoint").into()],
    ))
    .await
    .unwrap();
}

async fn bind_publication_foreign_import(fixture: &Fixture, operation: &str, shared_range: bool) {
    let db = fixture.db();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_manifest SET reference_only=0,source_thread='foreign-thread',\
          source_scope='event:foreign-turn',source_id='foreign-event',\
          source_version='event-revision:1' WHERE operation_id=? AND ordinal=0",
        [operation.into()],
    ))
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_coverage SET source_scope='event:foreign-turn',\
          source_id='foreign-event',source_version='event-revision:1' \
          WHERE checkpoint_id=?",
        [format!("{operation}-checkpoint").into()],
    ))
    .await
    .unwrap();
    db.execute_unprepared(
        "INSERT INTO compaction_frozen_history(\
          id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,\
          import_count,imports_sha256,next_import,ready) \
         VALUES ('publication-bound-manifest','ws','root-thread','bound-identity',0,0,1,\
          'bound-imports',1,1)",
    )
    .await
    .unwrap();
    let data_manifest = if shared_range {
        db.execute_unprepared(
            "INSERT INTO compaction_frozen_history(\
              id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,\
              import_count,imports_sha256,next_import,ready) \
             VALUES ('publication-import-storage','ws','root-thread','storage-identity',0,0,1,\
              'storage-imports',1,1); \
             INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) \
              VALUES ('publication-bound-manifest',1,1,0); \
             INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) \
              VALUES ('publication-bound-manifest',1,0,1,'publication-import-storage')",
        )
        .await
        .unwrap();
        "publication-import-storage"
    } else {
        "publication-bound-manifest"
    };
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_import_data(\
          manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,\
          source_thread,proof_json,bytes) \
         VALUES (?,0,0,'event:foreign-turn','foreign-event','event-revision:1',\
          'foreign-thread','{}',2)",
        [data_manifest.into()],
    ))
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_operation_projection(\
          operation_id,manifest_id,identity_sha256,imports_sha256,import_count) \
         VALUES (?,'publication-bound-manifest','bound-identity','bound-imports',1)",
        [operation.into()],
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn publication_fence_mismatch_retries_without_losing_candidate_or_runner_budget() {
    use crate::repositories::compaction::CommitOutcome;

    let fixture = fixture().await;
    let operation = "publication-retry";
    let state = publication_candidate(&fixture, operation, 1).await;
    assert_positive_publication_preflight(&fixture, operation, &state).await;
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
    tokio::time::timeout(Duration::from_secs(1), async {
        fixture
            .db()
            .begin_read()
            .await
            .unwrap()
            .rollback()
            .await
            .unwrap();
    })
    .await
    .expect("reader preflight was still reserved before writer publication");
    fixture
        .db()
        .execute_unprepared(
            "UPDATE turn_llm_context SET payload='{\"changed\":true}' \
             WHERE id='context-source'",
        )
        .await
        .unwrap();
    hook.release();
    assert_eq!(apply.await.unwrap(), CommitOutcome::RetryValidation);

    let candidate = fixture
        .store
        .compaction_checkpoint(&format!("{operation}-checkpoint"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(candidate.summary, "prepared summary");
    let durable = fixture
        .store
        .compaction_runner_state(operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(durable, state, "validation retry consumed runner budget");

    assert_eq!(
        fixture
            .store
            .compaction_apply_runner(operation, &state, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    fixture
        .db()
        .execute_unprepared(
            "UPDATE turn_llm_context SET payload='{\"changed_again\":true}' \
             WHERE id='context-source'",
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .store
            .compaction_apply_runner(operation, &state, None)
            .await
            .unwrap(),
        CommitOutcome::AlreadyApplied,
        "a successful retry must remain idempotent after later source edits"
    );
}

#[tokio::test]
async fn concurrent_success_is_already_applied_even_after_its_fence_bumps() {
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
async fn reader_preflight_is_one_snapshot_and_does_not_reserve_the_writer() {
    use crate::repositories::compaction::CommitOutcome;

    let fixture = fixture().await;
    let operation = "publication-reader-race";
    let state = publication_candidate(&fixture, operation, 1).await;
    assert_positive_publication_preflight(&fixture, operation, &state).await;
    let mut hook = fixture.arm_publication_hook(operation, PublicationTestPause::ReaderPreflight);
    let store = fixture.store.clone();
    let state_for_task = state.clone();
    let apply = tokio::spawn(async move {
        store
            .compaction_apply_runner(operation, &state_for_task, None)
            .await
            .unwrap()
    });
    hook.reached().await;

    // The first fence/identity reads have fixed the reader snapshot, but its
    // heavy predicates have not run. A separate interactive write must finish
    // before the test releases that reader barrier.
    tokio::time::timeout(
        Duration::from_secs(1),
        fixture
            .store
            .with_interactive_writes()
            .database_connection()
            .execute_unprepared(
                "UPDATE turn_event SET payload='{\"raced\":true}' \
             WHERE id='publication-reader-race-source-0'",
            ),
    )
    .await
    .expect("interactive write waited for reader preflight")
    .unwrap();
    hook.release();
    assert_eq!(apply.await.unwrap(), CommitOutcome::RetryValidation);
    assert_eq!(
        fixture
            .store
            .compaction_apply_runner(operation, &state, None)
            .await
            .unwrap(),
        CommitOutcome::Stale,
        "the retry must see the source edit made after the first snapshot"
    );
}

#[tokio::test]
async fn validation_fence_and_predicates_are_captured_from_the_same_reader_snapshot() {
    let fixture = fixture().await;
    let operation = "publication-single-snapshot";
    let state = publication_candidate(&fixture, operation, 1).await;
    assert_positive_publication_preflight(&fixture, operation, &state).await;
    let checkpoint = format!("{operation}-checkpoint");
    let before_mutation = source_fence(&fixture.db(), "ws").await;
    let mut hook = fixture.arm_publication_hook(operation, PublicationTestPause::ReaderPreflight);
    let store = fixture.store.clone();
    let prepare = tokio::spawn(async move {
        prepare_runner_publication(&store, operation, &checkpoint, state.generation, None)
            .await
            .unwrap()
    });
    hook.reached().await;
    fixture
        .store
        .with_interactive_writes()
        .database_connection()
        .execute_unprepared(
            "UPDATE turn_event SET payload='{\"new_snapshot\":true}' \
             WHERE id='publication-single-snapshot-source-0'",
        )
        .await
        .unwrap();
    hook.release();
    let prepared = prepare.await.unwrap();
    assert!(
        prepared.sources_current,
        "predicate escaped the snapshot that supplied its old fence"
    );
    assert_eq!(prepared.source_mutation_generation, Some(before_mutation.0));
    assert!(source_fence(&fixture.db(), "ws").await.0 > before_mutation.0);
}

#[tokio::test]
async fn delete_reinsert_task_basis_and_coverage_races_retry_then_revalidate() {
    use crate::repositories::compaction::CommitOutcome;

    for (operation, mutation) in [
        (
            "publication-delete",
            "DELETE FROM turn_event WHERE id='publication-delete-source-0'",
        ),
        (
            "publication-reinsert",
            "DELETE FROM turn_event WHERE id='publication-reinsert-source-0'; \
             INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) \
             VALUES ('publication-reinsert-source-0','root-thread','root-turn',10000,\
              'fixture','{}',CURRENT_TIMESTAMP)",
        ),
        (
            "publication-manifest-race",
            "UPDATE compaction_manifest SET source_version='event-revision:99' \
             WHERE operation_id='publication-manifest-race' AND ordinal=0",
        ),
    ] {
        let case_fixture = fixture().await;
        let state = publication_candidate(&case_fixture, operation, 1).await;
        assert_positive_publication_preflight(&case_fixture, operation, &state).await;
        let mut hook =
            case_fixture.arm_publication_hook(operation, PublicationTestPause::BeforeWriter);
        let store = case_fixture.store.clone();
        let state_for_task = state.clone();
        let apply = tokio::spawn(async move {
            store
                .compaction_apply_runner(operation, &state_for_task, None)
                .await
                .unwrap()
        });
        hook.reached().await;
        case_fixture
            .db()
            .execute_unprepared(mutation)
            .await
            .unwrap();
        hook.release();
        assert_eq!(apply.await.unwrap(), CommitOutcome::RetryValidation);
        assert_eq!(
            case_fixture
                .store
                .compaction_apply_runner(operation, &state, None)
                .await
                .unwrap(),
            CommitOutcome::Stale,
            "{operation} did not revalidate its changed dependency"
        );
    }

    let basis_shape_fixture = fixture().await;
    let operation = "publication-task-basis-race";
    let state = publication_candidate(&basis_shape_fixture, operation, 1).await;
    basis_shape_fixture
        .db()
        .execute_unprepared(
            "UPDATE compaction_manifest SET reference_only=1,source_thread='source-thread',\
          source_scope='task-basis:basis-run',source_id='basis-run',\
          source_version='task-basis-revision:1' \
         WHERE operation_id='publication-task-basis-race' AND ordinal=0; \
         DELETE FROM compaction_coverage \
          WHERE checkpoint_id='publication-task-basis-race-checkpoint'",
        )
        .await
        .unwrap();
    assert_positive_publication_preflight(&basis_shape_fixture, operation, &state).await;
    let mut hook =
        basis_shape_fixture.arm_publication_hook(operation, PublicationTestPause::BeforeWriter);
    let store = basis_shape_fixture.store.clone();
    let state_for_task = state.clone();
    let apply = tokio::spawn(async move {
        store
            .compaction_apply_runner(operation, &state_for_task, None)
            .await
            .unwrap()
    });
    hook.reached().await;
    basis_shape_fixture
        .db()
        .execute_unprepared(
            "UPDATE task_run_conversation_snapshot SET history_json='{}' WHERE run_id='basis-run'",
        )
        .await
        .unwrap();
    hook.release();
    assert_eq!(apply.await.unwrap(), CommitOutcome::RetryValidation);
    assert_eq!(
        basis_shape_fixture
            .store
            .compaction_apply_runner(operation, &state, None)
            .await
            .unwrap(),
        CommitOutcome::Stale
    );

    let basis_revision_fixture = fixture().await;
    let operation = "publication-task-basis-revision-race";
    let state = publication_candidate(&basis_revision_fixture, operation, 1).await;
    basis_revision_fixture
        .db()
        .execute_unprepared(
            "UPDATE compaction_manifest SET reference_only=1,source_thread='source-thread',\
          source_scope='task-basis:basis-run',source_id='basis-run',\
          source_version='task-basis-revision:1' \
         WHERE operation_id='publication-task-basis-revision-race' AND ordinal=0; \
         DELETE FROM compaction_coverage \
          WHERE checkpoint_id='publication-task-basis-revision-race-checkpoint'",
        )
        .await
        .unwrap();
    assert_positive_publication_preflight(&basis_revision_fixture, operation, &state).await;
    let mut hook =
        basis_revision_fixture.arm_publication_hook(operation, PublicationTestPause::BeforeWriter);
    let store = basis_revision_fixture.store.clone();
    let state_for_task = state.clone();
    let apply = tokio::spawn(async move {
        store
            .compaction_apply_runner(operation, &state_for_task, None)
            .await
            .unwrap()
    });
    hook.reached().await;
    basis_revision_fixture
        .db()
        .execute_unprepared(
            "INSERT INTO compaction_task_basis_revision(run_id,revision) VALUES ('basis-run',2)",
        )
        .await
        .unwrap();
    hook.release();
    assert_eq!(apply.await.unwrap(), CommitOutcome::RetryValidation);
    assert_eq!(
        basis_revision_fixture
            .store
            .compaction_apply_runner(operation, &state, None)
            .await
            .unwrap(),
        CommitOutcome::Stale,
        "an explicit task-basis revision reused the fallback-revision proof"
    );

    let coverage_fixture = fixture().await;
    let operation = "publication-coverage-race";
    let state = publication_candidate(&coverage_fixture, operation, 1).await;
    assert_positive_publication_preflight(&coverage_fixture, operation, &state).await;
    let mut hook =
        coverage_fixture.arm_publication_hook(operation, PublicationTestPause::BeforeWriter);
    let store = coverage_fixture.store.clone();
    let state_for_task = state.clone();
    let apply = tokio::spawn(async move {
        store
            .compaction_apply_runner(operation, &state_for_task, None)
            .await
            .unwrap()
    });
    hook.reached().await;
    coverage_fixture
        .db()
        .execute_unprepared(
            "INSERT INTO compaction_coverage(\
          checkpoint_id,source_scope,source_id,source_version) \
         VALUES ('publication-coverage-race-checkpoint','context:source-turn',\
          'context-source','revision:1')",
        )
        .await
        .unwrap();
    hook.release();
    assert_eq!(apply.await.unwrap(), CommitOutcome::RetryValidation);
    assert_eq!(
        coverage_fixture
            .store
            .compaction_apply_runner(operation, &state, None)
            .await
            .unwrap(),
        CommitOutcome::Stale
    );
}

#[tokio::test]
async fn projection_and_frozen_storage_races_retry_then_revalidate() {
    use crate::repositories::compaction::CommitOutcome;

    for (operation, shared_range, mutation) in [
        (
            "publication-projection-race",
            false,
            "UPDATE compaction_operation_projection SET identity_sha256='wrong' \
             WHERE operation_id='publication-projection-race'",
        ),
        (
            "publication-frozen-metadata-race",
            false,
            "UPDATE compaction_frozen_history SET ready=0 \
             WHERE id='publication-bound-manifest'",
        ),
        (
            "publication-ordinary-frozen-race",
            false,
            "UPDATE compaction_frozen_import_data SET source_version='event-revision:2' \
             WHERE manifest_id='publication-bound-manifest' AND ordinal=0",
        ),
        (
            "publication-shared-frozen-race",
            true,
            "UPDATE compaction_frozen_import_data SET source_version='event-revision:2' \
             WHERE manifest_id='publication-import-storage' AND ordinal=0",
        ),
    ] {
        let fixture = fixture().await;
        let state = publication_candidate(&fixture, operation, 1).await;
        bind_publication_foreign_import(&fixture, operation, shared_range).await;
        assert_positive_publication_preflight(&fixture, operation, &state).await;
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
        fixture.db().execute_unprepared(mutation).await.unwrap();
        hook.release();
        assert_eq!(apply.await.unwrap(), CommitOutcome::RetryValidation);
        assert_eq!(
            fixture
                .store
                .compaction_apply_runner(operation, &state, None)
                .await
                .unwrap(),
            CommitOutcome::Stale,
            "{operation} reused a changed projection/frozen proof"
        );
    }
}

#[tokio::test]
async fn historical_checkpoint_mutations_retry_fence_without_staling_published_root() {
    use crate::repositories::compaction::CommitOutcome;

    for (operation, retained, mutation) in [
        (
            "publication-dag-status",
            false,
            "UPDATE compaction_checkpoint SET status='failed' \
             WHERE id='publication-dag-mid'",
        ),
        (
            "publication-dag-delete",
            false,
            "DELETE FROM compaction_checkpoint WHERE id='publication-dag-mid'",
        ),
        (
            "publication-dag-operation",
            true,
            "UPDATE compaction_operation SET status='running' WHERE id='source-operation'",
        ),
    ] {
        let fixture = fixture().await;
        let state = publication_candidate(&fixture, operation, 1).await;
        bind_reference_checkpoint_dag(&fixture, operation, retained).await;
        assert_positive_publication_preflight(&fixture, operation, &state).await;
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
        fixture.db().execute_unprepared(mutation).await.unwrap();
        hook.release();
        assert_eq!(apply.await.unwrap(), CommitOutcome::RetryValidation);
        assert_eq!(
            fixture
                .store
                .compaction_apply_runner(operation, &state, None)
                .await
                .unwrap(),
            CommitOutcome::Applied,
            "{operation} treated historical checkpoint metadata as root liveness"
        );
    }
}

#[tokio::test]
async fn positive_preflight_ignores_pure_append_but_retries_create_then_complete() {
    use crate::repositories::compaction::CommitOutcome;

    let append_fixture = fixture().await;
    let operation = "publication-append";
    let state = publication_candidate(&append_fixture, operation, 1).await;
    assert_positive_publication_preflight(&append_fixture, operation, &state).await;
    let mut hook =
        append_fixture.arm_publication_hook(operation, PublicationTestPause::BeforeWriter);
    let store = append_fixture.store.clone();
    let state_for_task = state.clone();
    let apply = tokio::spawn(async move {
        store
            .compaction_apply_runner(operation, &state_for_task, None)
            .await
            .unwrap()
    });
    hook.reached().await;
    append_fixture.db().execute_unprepared(
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) \
         VALUES ('post-proof-append','root-thread','root-turn',20000,'fixture','{}',CURRENT_TIMESTAMP)",
    ).await.unwrap();
    hook.release();
    assert_eq!(apply.await.unwrap(), CommitOutcome::Applied);

    let streaming_fixture = fixture().await;
    let operation = "publication-stream-complete";
    let state = publication_candidate(&streaming_fixture, operation, 1).await;
    assert_positive_publication_preflight(&streaming_fixture, operation, &state).await;
    let mut hook =
        streaming_fixture.arm_publication_hook(operation, PublicationTestPause::BeforeWriter);
    let store = streaming_fixture.store.clone();
    let state_for_task = state.clone();
    let apply = tokio::spawn(async move {
        store
            .compaction_apply_runner(operation, &state_for_task, None)
            .await
            .unwrap()
    });
    hook.reached().await;
    streaming_fixture
        .db()
        .execute_unprepared(
            "INSERT INTO turn_item(\
          id,turn_id,item_id,item_type,status,payload,created_at,updated_at) \
         VALUES ('streaming-user','root-turn','streaming-user','user_message',NULL,'{}',\
          CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); \
         UPDATE turn_item SET status='completed',payload='{\"done\":true}' \
          WHERE id='streaming-user'",
        )
        .await
        .unwrap();
    hook.release();
    assert_eq!(apply.await.unwrap(), CommitOutcome::RetryValidation);
    assert_eq!(
        streaming_fixture
            .store
            .compaction_apply_runner(operation, &state, None)
            .await
            .unwrap(),
        CommitOutcome::Applied,
        "finite streaming churn must still make publication progress"
    );
}

#[tokio::test]
async fn negative_preflight_retries_when_missing_source_is_inserted() {
    use crate::repositories::compaction::CommitOutcome;

    let fixture = fixture().await;
    let operation = "publication-negative-insert";
    let state = publication_candidate(&fixture, operation, 1).await;
    fixture
        .db()
        .execute_unprepared(
            "UPDATE compaction_manifest SET source_id='publication-late-source' \
              WHERE operation_id='publication-negative-insert'; \
             UPDATE compaction_coverage SET source_id='publication-late-source' \
              WHERE checkpoint_id='publication-negative-insert-checkpoint'",
        )
        .await
        .unwrap();
    let prepared = publication_preflight(&fixture, operation, &state).await;
    assert!(prepared.identity_current);
    assert!(!prepared.sources_current);
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
    let source_before = source_fence(&fixture.db(), "ws").await;
    let structural_before = structural_fence(&fixture.db()).await;
    fixture.db().execute_unprepared(
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) \
         VALUES ('publication-late-source','root-thread','root-turn',30001,'fixture','{}',CURRENT_TIMESTAMP)",
    ).await.unwrap();
    let source_after = source_fence(&fixture.db(), "ws").await;
    assert_eq!(source_after.0, source_before.0);
    assert!(source_after.1 > source_before.1);
    assert_eq!(structural_fence(&fixture.db()).await, structural_before);
    hook.release();
    assert_eq!(apply.await.unwrap(), CommitOutcome::RetryValidation);
    assert_positive_publication_preflight(&fixture, operation, &state).await;
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
async fn stable_negative_preflight_is_final_stale() {
    use crate::repositories::compaction::CommitOutcome;

    let fixture = fixture().await;
    let operation = "publication-stable-negative";
    let state = publication_candidate(&fixture, operation, 1).await;
    fixture
        .db()
        .execute_unprepared(
            "UPDATE compaction_manifest SET source_id='publication-never-created' \
              WHERE operation_id='publication-stable-negative'; \
             UPDATE compaction_coverage SET source_id='publication-never-created' \
              WHERE checkpoint_id='publication-stable-negative-checkpoint'",
        )
        .await
        .unwrap();
    let prepared = publication_preflight(&fixture, operation, &state).await;
    assert!(prepared.identity_current);
    assert!(!prepared.sources_current);
    assert_eq!(
        fixture
            .store
            .compaction_apply_runner(operation, &state, None)
            .await
            .unwrap(),
        CommitOutcome::Stale
    );
}

#[tokio::test]
async fn repeated_streaming_mutations_retry_without_consuming_commit_state_then_progress() {
    use crate::repositories::compaction::CommitOutcome;

    let fixture = fixture().await;
    let operation = "publication-stream-churn";
    let state = publication_candidate(&fixture, operation, 1).await;
    assert_positive_publication_preflight(&fixture, operation, &state).await;
    for revision in 0..4 {
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
        fixture
            .db()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE turn_llm_context SET payload=? WHERE id='context-source'",
                [format!("{{\"stream_revision\":{revision}}}").into()],
            ))
            .await
            .unwrap();
        hook.release();
        assert_eq!(apply.await.unwrap(), CommitOutcome::RetryValidation);
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
    assert_eq!(
        fixture
            .store
            .compaction_apply_runner(operation, &state, None)
            .await
            .unwrap(),
        CommitOutcome::Applied,
        "publication did not progress after streaming reached a stable window"
    );
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
        assert_eq!(publication_writer_fence_checks(operation), 0);
        assert_eq!(
            fixture
                .store
                .compaction_apply_runner(operation, &state, None)
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
        assert_eq!(
            publication_writer_fence_checks(operation),
            1,
            "writer fence work grew for {sources} manifest/DAG leaves"
        );
        let metrics = publication_test_metrics(operation);
        assert_eq!(metrics.coverage_checks, 1);
        assert_eq!(metrics.manifest_checks, 1);
        assert_eq!(metrics.heavy_checks_while_writer, 0);
        assert_eq!(metrics.writer_entries, 1);
    }

    for (operation, nodes) in [("publication-dag-small", 1), ("publication-dag-large", 257)] {
        let fixture = fixture().await;
        let state = publication_candidate(&fixture, operation, 1).await;
        bind_reference_checkpoint_chain(&fixture, operation, nodes).await;
        assert_positive_publication_preflight(&fixture, operation, &state).await;
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
        assert_eq!(metrics.manifest_checks, 1);
        assert_eq!(metrics.heavy_checks_while_writer, 0);
        assert_eq!(
            metrics.writer_entries, 1,
            "writer traversed {nodes} DAG nodes"
        );
    }
}

#[tokio::test]
async fn publication_triggers_cover_logical_sources_topology_and_frozen_storage() {
    let fixture = fixture().await;
    let db = fixture.db();

    let before = source_fence(&db, "ws").await;
    db.execute_unprepared(
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) \
         VALUES ('fence-append','source-thread','source-turn',30000,'fixture','{}',CURRENT_TIMESTAMP)",
    ).await.unwrap();
    let appended = source_fence(&db, "ws").await;
    assert_eq!(appended.0, before.0);
    assert!(appended.1 > before.1);

    db.execute_unprepared(
        "UPDATE turn_event SET payload='{\"edited\":true}' WHERE id='fence-append'",
    )
    .await
    .unwrap();
    let edited = source_fence(&db, "ws").await;
    assert!(edited.0 > appended.0);
    db.execute_unprepared(
        "DELETE FROM turn_event WHERE id='fence-append'; \
         INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) \
         VALUES ('fence-append','source-thread','source-turn',30000,'fixture','{}',CURRENT_TIMESTAMP)",
    ).await.unwrap();
    let recreated = source_fence(&db, "ws").await;
    assert!(
        recreated.0 > edited.0,
        "delete/reinsert lost its tombstone generation"
    );
    assert!(recreated.1 > edited.1);
    db.execute_unprepared(
        "UPDATE turn_event SET id='fence-append-renamed' WHERE id='fence-append'",
    )
    .await
    .unwrap();
    let renamed = source_fence(&db, "ws").await;
    assert!(
        renamed.0 > recreated.0,
        "changing a canonical source key did not invalidate the old identity"
    );

    db.execute_unprepared(
        "INSERT INTO task_run(\
          id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) \
         VALUES ('basis-append','task','basis-append',1,2,'succeeded','agent'); \
         INSERT INTO task_run_conversation_snapshot(\
          run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) \
         VALUES ('basis-append','task','ws','source-thread','[]',CURRENT_TIMESTAMP)",
    )
    .await
    .unwrap();
    let basis_appended = source_fence(&db, "ws").await;
    assert_eq!(
        basis_appended.0, renamed.0,
        "a new task-basis plus its fallback-equivalent revision caused mutation invalidation"
    );
    assert!(basis_appended.1 > renamed.1);

    let basis_before = basis_appended;
    db.execute_unprepared(
        "INSERT INTO compaction_task_basis_revision(run_id,revision) VALUES ('basis-run',2)",
    )
    .await
    .unwrap();
    let basis_revision = source_fence(&db, "ws").await;
    assert!(
        basis_revision.0 > basis_before.0,
        "inserting an explicit revision did not invalidate fallback revision 1"
    );
    db.execute_unprepared(
        "UPDATE task_run_conversation_snapshot SET history_json=' {not-an-array}' \
         WHERE run_id='basis-run'",
    )
    .await
    .unwrap();
    assert!(source_fence(&db, "ws").await.0 > basis_revision.0);

    db.execute_unprepared(
        "INSERT INTO turn_item(\
          id,turn_id,item_id,item_type,status,payload,created_at,updated_at) \
         VALUES ('non-epoch-item','source-turn','non-epoch-item','assistant_message','completed','{}',\
          CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ).await.unwrap();
    let epoch_before = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT COALESCE(version,0) AS version FROM compaction_projection_epoch \
         WHERE thread_id='source-thread'"
                .to_owned(),
        ))
        .await
        .unwrap()
        .map(|row| row.try_get::<i64>("", "version").unwrap())
        .unwrap_or(0);
    let logical_before = source_fence(&db, "ws").await;
    db.execute_unprepared(
        "UPDATE turn_item SET payload='{\"logical\":true}' WHERE id='non-epoch-item'",
    )
    .await
    .unwrap();
    let epoch_after = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT COALESCE(version,0) AS version FROM compaction_projection_epoch \
         WHERE thread_id='source-thread'"
                .to_owned(),
        ))
        .await
        .unwrap()
        .map(|row| row.try_get::<i64>("", "version").unwrap())
        .unwrap_or(0);
    assert_eq!(epoch_after, epoch_before);
    assert!(source_fence(&db, "ws").await.0 > logical_before.0);

    db.execute_unprepared(
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) \
         VALUES ('moved-thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); \
         INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) \
         VALUES ('moved-turn','moved-thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ).await.unwrap();
    let ownership_before = structural_fence(&db).await;
    db.execute_unprepared("UPDATE turn SET thread_id='source-thread' WHERE id='moved-turn'")
        .await
        .unwrap();
    assert!(structural_fence(&db).await > ownership_before);

    let canonical_revision_before = source_fence(&db, "ws").await;
    db.execute_unprepared(
        "UPDATE compaction_event_revision SET revision=revision+1 WHERE source_id='event-source'",
    )
    .await
    .unwrap();
    assert!(source_fence(&db, "ws").await.0 > canonical_revision_before.0);

    let canonical_present_before = source_fence(&db, "ws").await;
    db.execute_unprepared(
        "UPDATE compaction_event_revision SET present=0 WHERE source_id='event-source'",
    )
    .await
    .unwrap();
    assert!(source_fence(&db, "ws").await.0 > canonical_present_before.0);

    db.execute_unprepared(
        "INSERT INTO compaction_frozen_history(\
          id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,\
          import_count,imports_sha256,next_import,ready) \
         VALUES ('fence-storage','ws','root-thread','storage',1,1,0,'imports',0,1); \
         INSERT INTO compaction_frozen_history(\
          id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,\
          import_count,imports_sha256,next_import,ready) \
         VALUES ('fence-view','ws','root-thread','view',1,1,0,'imports',0,1); \
         INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) \
          VALUES ('fence-storage',0,'{}',2); \
         INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) \
          VALUES ('fence-view',0,1,0); \
         INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) \
          VALUES ('fence-view',0,0,1,'fence-storage')",
    )
    .await
    .unwrap();

    let inherited_before = structural_fence(&db).await;
    db.execute_unprepared(
        "UPDATE compaction_frozen_message_data SET reference_json='{\"inherited\":false}',bytes=19 \
          WHERE manifest_id='fence-storage' AND ordinal=0",
    )
    .await
    .unwrap();
    assert!(structural_fence(&db).await > inherited_before);

    let layout_before = structural_fence(&db).await;
    db.execute_unprepared(
        "UPDATE compaction_frozen_layout SET active=0 WHERE manifest_id='fence-view' AND kind=0",
    )
    .await
    .unwrap();
    assert!(structural_fence(&db).await > layout_before);

    let span_range_before = structural_fence(&db).await;
    db.execute_unprepared(
        "UPDATE compaction_frozen_span SET end=2 WHERE manifest_id='fence-view' AND kind=0",
    )
    .await
    .unwrap();
    assert!(structural_fence(&db).await > span_range_before);

    let span_source_before = structural_fence(&db).await;
    db.execute_unprepared(
        "UPDATE compaction_frozen_span SET source_manifest='fence-view' \
          WHERE manifest_id='fence-view' AND kind=0",
    )
    .await
    .unwrap();
    assert!(structural_fence(&db).await > span_source_before);

    db.execute_unprepared(
        "INSERT INTO compaction_frozen_import_data(\
          manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,\
          source_thread,proof_json,bytes) \
         VALUES ('fence-storage',0,0,'event:source-turn','event-source','event-revision:1',\
          'source-thread','{}',2)",
    )
    .await
    .unwrap();
    let physical_frozen_before = structural_fence(&db).await;
    db.execute_unprepared(
        "UPDATE compaction_frozen_import_data SET proof_json='{\"changed\":true}',bytes=16 \
          WHERE manifest_id='fence-storage' AND ordinal=0",
    )
    .await
    .unwrap();
    assert!(structural_fence(&db).await > physical_frozen_before);

    let binding_before = structural_fence(&db).await;
    db.execute_unprepared(
        "INSERT INTO compaction_operation_projection(\
          operation_id,manifest_id,identity_sha256,imports_sha256,import_count) \
         VALUES ('manifest-operation','fence-view','view','imports',0)",
    )
    .await
    .unwrap();
    assert!(structural_fence(&db).await > binding_before);

    let ready_before = structural_fence(&db).await;
    db.execute_unprepared("UPDATE compaction_frozen_history SET ready=0 WHERE id='fence-view'")
        .await
        .unwrap();
    assert!(structural_fence(&db).await > ready_before);

    let previous_before = structural_fence(&db).await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET previous='source-checkpoint' WHERE id='source-checkpoint'",
    )
    .await
    .unwrap();
    assert!(structural_fence(&db).await > previous_before);

    let coverage_before = structural_fence(&db).await;
    db.execute_unprepared(
        "UPDATE compaction_coverage SET source_version='event-revision:2' \
          WHERE checkpoint_id='source-checkpoint' AND source_id='event-source'",
    )
    .await
    .unwrap();
    assert!(structural_fence(&db).await > coverage_before);

    let dependency_status_before = structural_fence(&db).await;
    db.execute_unprepared(
        "UPDATE compaction_operation SET status='running' WHERE id='source-operation'",
    )
    .await
    .unwrap();
    assert!(structural_fence(&db).await > dependency_status_before);

    let snapshot_before = structural_fence(&db).await;
    db.execute_unprepared(
        "UPDATE compaction_operation SET snapshot='{\"plan\":{\"coverage_domain\":\"all\"}}' \
          WHERE id='manifest-operation'",
    )
    .await
    .unwrap();
    assert!(structural_fence(&db).await > snapshot_before);

    let manifest_before = structural_fence(&db).await;
    db.execute_unprepared(
        "INSERT INTO compaction_manifest(\
          operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) \
         VALUES ('manifest-operation',999,999,1,'root-thread','event:root-turn',\
          'event-source','event-revision:1')",
    )
    .await
    .unwrap();
    assert!(structural_fence(&db).await > manifest_before);
}

#[tokio::test]
async fn zstd_physical_rewrite_does_not_look_like_a_logical_source_edit() {
    let fixture = fixture().await;
    let db = fixture.db();
    let before = source_fence(&db, "ws").await;
    enable_and_physically_compress_canonical_payloads(&db).await;
    assert_eq!(
        source_fence(&db, "ws").await,
        before,
        "physical zstd storage maintenance changed the logical source fence"
    );
    db.execute_unprepared(
        "UPDATE turn_event SET payload='{\"logical_after_zstd\":true}' WHERE id='event-source'",
    )
    .await
    .unwrap();
    assert!(source_fence(&db, "ws").await.0 > before.0);
}

#[tokio::test]
async fn publication_migration_installs_source_fence_on_existing_zstd_storage() {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let file = TestFile(std::env::temp_dir().join(format!(
        "pioneer-publication-existing-zstd-{}.sqlite",
        uuid::Uuid::new_v4()
    )));
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", file.0.display()));
    options.max_connections(1).min_connections(1);
    let writer_connection = Database::connect(options).await.unwrap();
    let writer = SqliteWriteExecutor::new(writer_connection);
    writer
        .run_migrations::<Migrator>(
            SqliteWriteClass::Maintenance,
            Some((Migrator::migrations().len() - 1) as u32),
        )
        .await
        .unwrap();
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=ro", file.0.display()));
    options.max_connections(1).min_connections(1);
    let reader = Database::connect(options).await.unwrap();
    reader
        .execute_unprepared("PRAGMA query_only=ON")
        .await
        .unwrap();
    let db = SqliteDatabase::from_executor(reader, writer.clone()).maintenance();
    db.execute_unprepared(
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('zstd-ws','zstd',1,1); \
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) \
          VALUES ('zstd-thread','zstd-ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); \
         INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) \
          VALUES ('zstd-turn','zstd-thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); \
         INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) \
          VALUES ('zstd-event','zstd-thread','zstd-turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
    ).await.unwrap();
    let config = serde_json::json!({
        "table": "turn_event",
        "column": "payload",
        "compression_level": 3,
        "dict_chooser": "'[nodict]'",
    });
    db.query_one_write_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "SELECT zstd_enable_transparent(?)",
        [config.to_string().into()],
    ))
    .await
    .unwrap();
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();

    let before = source_fence(&db, "zstd-ws").await;
    let compressed = pioneer_sqlite::zstd::compress_column_value(b"{}", 3, None).unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE _turn_event_zstd SET payload=?,_payload_dict=-1 WHERE id='zstd-event'",
        [compressed.into()],
    ))
    .await
    .unwrap();
    assert_eq!(source_fence(&db, "zstd-ws").await, before);
    db.execute_unprepared(
        "UPDATE turn_event SET payload='{\"logical\":true}' WHERE id='zstd-event'",
    )
    .await
    .unwrap();
    assert!(source_fence(&db, "zstd-ws").await.0 > before.0);
}

#[tokio::test]
async fn publication_generations_fail_closed_on_overflow_and_workspace_recreation() {
    let fixture = fixture().await;
    let db = fixture.db();
    db.execute_unprepared(
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('aba-workspace','aba',1,0)",
    )
    .await
    .unwrap();
    let before = source_fence(&db, "aba-workspace").await;
    db.execute_unprepared(
        "DELETE FROM workspace WHERE id='aba-workspace'; \
         INSERT INTO workspace(id,name,is_active,is_current) VALUES ('aba-workspace','aba',1,0)",
    )
    .await
    .unwrap();
    let recreated = source_fence(&db, "aba-workspace").await;
    assert!(recreated.0 > before.0 && recreated.1 > before.1);

    db.execute_unprepared(
        "UPDATE compaction_publication_source_fence \
         SET mutation_generation=9223372036854775807 WHERE workspace_id='ws'",
    )
    .await
    .unwrap();
    let update = db
        .execute_unprepared(
            "UPDATE turn_event SET payload='{\"must_rollback\":true}' WHERE id='event-source'",
        )
        .await;
    assert!(update.is_err(), "generation overflow was silently reused");
    let payload: String = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT payload FROM turn_event WHERE id='event-source'".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "payload")
        .unwrap();
    assert_eq!(payload, "{}");
}

#[tokio::test]
async fn publication_migration_is_idempotent_without_resetting_generations() {
    let fixture = fixture().await;
    let db = fixture.db();
    db.execute_unprepared(
        "UPDATE turn_event SET payload='{\"idempotent\":true}' WHERE id='event-source'",
    )
    .await
    .unwrap();
    let before = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT database_id,structural_generation FROM compaction_publication_fence \
             WHERE singleton=1"
                .to_owned(),
        ))
        .await
        .unwrap()
        .unwrap();
    let database_id: String = before.try_get("", "database_id").unwrap();
    let structural: i64 = before.try_get("", "structural_generation").unwrap();
    let source = source_fence(&db, "ws").await;

    fixture
        .writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();

    let after = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT database_id,structural_generation FROM compaction_publication_fence \
             WHERE singleton=1"
                .to_owned(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.try_get::<String>("", "database_id").unwrap(),
        database_id
    );
    assert_eq!(
        after.try_get::<i64>("", "structural_generation").unwrap(),
        structural
    );
    assert_eq!(source_fence(&db, "ws").await, source);
}

#[tokio::test]
async fn publication_generations_survive_database_reopen() {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let file = TestFile(std::env::temp_dir().join(format!(
        "pioneer-publication-reopen-{}.sqlite",
        uuid::Uuid::new_v4()
    )));
    let connection = Database::connect(format!("sqlite://{}?mode=rwc", file.0.display()))
        .await
        .unwrap();
    Migrator::up(&connection, None).await.unwrap();
    connection
        .execute_unprepared(
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('restart-ws','restart',1,1); \
             UPDATE compaction_publication_fence SET structural_generation=7 WHERE singleton=1; \
             UPDATE compaction_publication_source_fence \
              SET mutation_generation=5,insert_generation=9 WHERE workspace_id='restart-ws'",
        )
        .await
        .unwrap();
    let database_id: String = connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT database_id FROM compaction_publication_fence WHERE singleton=1".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "database_id")
        .unwrap();
    connection.close().await.unwrap();

    let reopened = Database::connect(format!("sqlite://{}?mode=rw", file.0.display()))
        .await
        .unwrap();
    let row = reopened
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT database_id,structural_generation FROM compaction_publication_fence \
             WHERE singleton=1"
                .to_owned(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.try_get::<String>("", "database_id").unwrap(),
        database_id
    );
    assert_eq!(row.try_get::<i64>("", "structural_generation").unwrap(), 7);
    let source = reopened
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT mutation_generation,insert_generation \
             FROM compaction_publication_source_fence WHERE workspace_id='restart-ws'"
                .to_owned(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source.try_get::<i64>("", "mutation_generation").unwrap(), 5);
    assert_eq!(source.try_get::<i64>("", "insert_generation").unwrap(), 9);
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn publication_migration_failure_rolls_back_schema_triggers_and_marker() {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let connection = Database::connect("sqlite::memory:").await.unwrap();
    let migration = "m20260919_000002_compaction_publication_fence";
    Migrator::up(&connection, Some((Migrator::migrations().len() - 2) as u32))
        .await
        .unwrap();
    connection
        .execute_unprepared(&format!(
            "CREATE TRIGGER reject_publication_migration BEFORE INSERT ON seaql_migrations \
         WHEN NEW.version='{migration}' BEGIN SELECT RAISE(ABORT,'fixture marker failure'); END"
        ))
        .await
        .unwrap();
    assert!(Migrator::up(&connection, None).await.is_err());
    let objects: i64 = connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT count(*) AS n FROM sqlite_master \
             WHERE name LIKE 'compaction_publication_%' \
              AND name<>'reject_publication_migration'"
                .to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert_eq!(objects, 0);
    let marker: i64 = connection
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT count(*) AS n FROM seaql_migrations WHERE version=?",
            [migration.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert_eq!(marker, 0);
    connection
        .execute_unprepared("DROP TRIGGER reject_publication_migration")
        .await
        .unwrap();
    Migrator::up(&connection, None).await.unwrap();
    let installed: i64 = connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT count(*) AS n FROM compaction_publication_fence".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert_eq!(installed, 1);
}

#[tokio::test]
async fn rolled_back_domain_mutation_rolls_back_publication_fence_bump() {
    let fixture = fixture().await;
    let db = fixture.db();
    let before = source_fence(&db, "ws").await;
    let txn = db.begin().await.unwrap();
    txn.execute_unprepared(
        "UPDATE turn_event SET payload='{\"rolled_back\":true}' WHERE id='event-source'",
    )
    .await
    .unwrap();
    let inside: i64 = txn
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT mutation_generation FROM compaction_publication_source_fence \
             WHERE workspace_id='ws'"
                .to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "mutation_generation")
        .unwrap();
    assert!(inside > before.0);
    txn.rollback().await.unwrap();
    assert_eq!(source_fence(&db, "ws").await, before);
    let payload: String = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT payload FROM turn_event WHERE id='event-source'".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "payload")
        .unwrap();
    assert_eq!(payload, "{}");
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
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES ('projection-pages','ws','source-thread','identity',?,?,0,'imports',0,1)",
        [count.into(), count.into()],
    ))
    .await
    .unwrap();
    db.execute_unprepared("INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) VALUES ('source-operation','projection-pages','identity','imports',0)")
        .await
        .unwrap();
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
        serde_json::from_str::<pioneer_compaction::frozen::FrozenMessageRef>(&reference)
            .unwrap()
            .validate()
            .unwrap();
        db.execute_raw(sqlite_specific_sql(
            "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES ('projection-pages',?,?,?)",
            [ordinal.into(), reference.clone().into(), (reference.len() as i64).into()],
        ))
        .await
        .unwrap();
    }

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
                "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES ('projection-pages',0,1,0)",
                "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) SELECT 'projection-pages',0,0,message_count,'projection-storage' FROM compaction_frozen_history WHERE id='projection-pages'",
                "DELETE FROM compaction_frozen_message_data WHERE manifest_id='projection-pages'",
            ] {
                db.execute_unprepared(sql).await.unwrap();
            }
        }
    }
}
