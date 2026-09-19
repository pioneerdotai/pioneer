use super::*;
use crate::CrudStore;
use migration::Migrator;
use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, Statement};
use std::path::{Path, PathBuf};

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

async fn open(path: &Path) -> CrudStore {
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    options.max_connections(1).min_connections(1);
    let writer_connection = Database::connect(options).await.unwrap();
    let writer = SqliteWriteExecutor::new(writer_connection);
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
    CrudStore::new(SqliteDatabase::from_executor(reader, writer)).with_maintenance_access()
}

async fn fixture() -> Fixture {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let file = TestFile(std::env::temp_dir().join(format!(
        "pioneer-manifest-source-lookups-{}.sqlite",
        uuid::Uuid::new_v4()
    )));
    let store = open(&file.0).await;
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
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    db.execute_unprepared("DELETE FROM compaction_task_basis_revision WHERE run_id='basis-run'")
        .await
        .unwrap();
    Fixture { store, _file: file }
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
async fn inherited_checkpoint_basis_keeps_multilevel_dependencies_in_needed_refs() {
    let fixture = fixture().await;
    let db = fixture.db();
    for sql in [
        "INSERT INTO compaction_context(workspace_id,thread_id,owner,format_version) VALUES ('ws','foreign-thread','basis-owner',1)",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES ('basis-operation','basis-owner','basis','completed','{}',1)",
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,previous,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES ('basis-mid','basis-operation','basis-owner',NULL,0,'mid','mid-version','{}',0,1,'applied')",
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,previous,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES ('basis-root','basis-operation','basis-owner','basis-mid',1,'root','basis-version','{}',0,1,'applied')",
        "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('basis-mid','event:foreign-turn','foreign-event','event-revision:1')",
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
        false,
        "basis source epoch membership",
    )
    .await;
    db.execute_unprepared("UPDATE compaction_operation SET snapshot='{\"plan\":{\"coverage_domain\":\"working_context\"},\"source_epochs\":{\"root-thread\":0,\"foreign-thread\":0}}' WHERE id='manifest-operation'")
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
        "deduplicated checkpoint cycle with leaf",
    )
    .await;
    db.execute_unprepared("DELETE FROM compaction_coverage WHERE checkpoint_id='basis-mid'")
        .await
        .unwrap();
    assert_manifest_current(
        &db,
        "manifest-operation",
        false,
        "checkpoint cycle without canonical leaf",
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
    assert_manifest_current(&db, "manifest-operation", false, "stale inherited leaf").await;
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
        false,
        "missing inherited intermediate",
    )
    .await;
}

#[tokio::test]
async fn basis_only_checkpoint_dependencies_are_required_by_needed_refs() {
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
        true,
        "basis-only dependency graph",
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
        "stale basis-only checkpoint",
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
        true,
        "basis-only checkpoint restored",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_event_revision SET revision=2 WHERE source_id='foreign-event'",
    )
    .await
    .unwrap();
    assert_manifest_current(&db, "manifest-operation", false, "stale basis-only leaf").await;
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
        "missing basis-only checkpoint",
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
async fn manifest_checkpoint_dag_preserves_the_65536_65537_boundary() {
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

    for (root, expected, case) in [
        (65534_i64, true, "65536 graph rows"),
        (65535_i64, false, "65537 graph rows"),
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
        assert_manifest_current(&db, "manifest-operation", expected, case).await;
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
}

#[tokio::test]
async fn basis_coverage_preserves_the_65536_65537_boundary() {
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
    assert_manifest_current(&db, "manifest-operation", true, "65536 basis coverage rows").await;

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
        "65537 basis coverage rows",
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

fn assert_frozen_view_plan(plan: &[PlanNode], view: &str) {
    let branch = unique_subtree(plan, &format!("CO-ROUTINE {view}"));
    assert_exact_branch_search(&branch, &["d"], "manifest_id", &format!("{view} data"));
    assert_exact_branch_search(&branch, &["l"], "manifest_id", &format!("{view} layout"));
    assert_exact_branch_search(&branch, &["s"], "manifest_id", &format!("{view} span"));
    assert!(
        branch.iter().any(|node| {
            plan_subject(&node.detail, "SEARCH", "d")
                && has_constraint(&node.detail, "manifest_id", "=")
                && !has_constraint(&node.detail, "ordinal", ">")
                && !has_constraint(&node.detail, "ordinal", "<")
        }),
        "ordinary {view} data lookup must constrain manifest_id: {branch:#?}"
    );
    let layout_lookups = branch
        .iter()
        .filter(|node| {
            plan_subject(&node.detail, "SEARCH", "l")
                && has_constraint(&node.detail, "manifest_id", "=")
                && has_constraint(&node.detail, "kind", "=")
        })
        .count();
    assert!(
        layout_lookups >= 2,
        "ordinary and shared {view} layout branches must both be manifest-scoped: {branch:#?}"
    );
    assert!(
        branch.iter().any(|node| {
            plan_subject(&node.detail, "SEARCH", "s")
                && has_constraint(&node.detail, "manifest_id", "=")
                && has_constraint(&node.detail, "kind", "=")
        }),
        "shared {view} span must constrain manifest_id and kind: {branch:#?}"
    );
    assert!(
        branch.iter().any(|node| {
            plan_subject(&node.detail, "SEARCH", "d")
                && has_constraint(&node.detail, "manifest_id", "=")
                && has_constraint(&node.detail, "ordinal", ">")
                && has_constraint(&node.detail, "ordinal", "<")
        }),
        "shared-range {view} data lookup must constrain manifest and ordinal range: {branch:#?}"
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
    let current = unique_subtree(plan, "MATERIALIZE current_sources");
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
