use super::*;
use crate::plan_test_support::{
    assert_exact_branch_search, explain, replace_task_basis, seed_canonical_plan_noise,
    seed_compressed_sources, seed_task_basis,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, DbBackend, Statement};
use sea_orm::{QueryTrait, TransactionTrait};

const RAW_SOURCE_ORACLE_SQL: &str = "SELECT count(*) AS matched
    FROM json_array_elements($1::text::json) AS wanted(value)
    WHERE EXISTS (
        SELECT 1 FROM compaction_live_sources s
          WHERE s.workspace_id=$2 AND s.thread_id=$3
            AND s.source_scope=wanted.value->>'scope'
            AND s.source_id=wanted.value->>'id'
            AND s.source_version=wanted.value->>'version'
    )";

async fn fixture() -> SqliteDatabase {
    fixture_with_portable_sources(false).await
}

async fn fixture_with_portable_sources(portable: bool) -> SqliteDatabase {
    let database = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = database.connection().clone();
    let reader = database.connect_read_only().await.unwrap();
    let db = SqliteDatabase::new(reader, writer).maintenance();
    assert!(db.reader_query_only_enabled().await.unwrap());
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',TRUE,FALSE)",
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('other','other',TRUE,FALSE)",
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
        "INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,created_at) VALUES ('context-source','source-turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
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
    if portable {
        seed_compressed_sources(&db).await;
    } else {
        for sql in [
            "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,created_at,updated_at) VALUES ('item-source','source-turn','item-source','command_execution','completed','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('event-source','source-thread','source-turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
        ] {
            db.execute_unprepared(sql).await.unwrap();
        }
    }
    seed_task_basis(&db).await;
    db.execute_unprepared("DELETE FROM compaction_task_basis_revision WHERE run_id='basis-run'")
        .await
        .unwrap();
    set_root_leaf(&db, &canonical_branches()[2].branch.source).await;
    db
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
        DbBackend::Postgres,
        "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('root-checkpoint',$1,$2,$3)",
        [
            source.scope.clone().into(),
            source.id.clone().into(),
            source.version.clone().into(),
        ],
    ))
    .await
    .unwrap();
}

fn raw_source_oracle(payload: String, workspace: &str, thread: &str) -> Statement {
    Statement::from_sql_and_values(
        DbBackend::Postgres,
        RAW_SOURCE_ORACLE_SQL,
        [payload.into(), workspace.into(), thread.into()],
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
    // The legacy view remains a useful oracle for unchanged raw/task-basis
    // branches. Checkpoint validity intentionally changed and must be asserted
    // from the published-object contract instead of oracle equality.
    if sources
        .iter()
        .all(|source| !source.scope.starts_with("checkpoint:"))
    {
        let legacy_matched = snapshot
            .query_one_raw(raw_source_oracle(payload, workspace, thread))
            .await
            .unwrap()
            .unwrap()
            .try_get::<i64>("", "matched")
            .unwrap();
        let legacy = legacy_matched == sources.len() as i64;
        assert_eq!(
            actual_matched, legacy_matched,
            "matched count diverged from raw-source oracle: {case}"
        );
        assert_eq!(
            actual, legacy,
            "sources query diverged from raw-source oracle: {case}"
        );
    }
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
    let actual_payload =
        compaction_reference_checkpoint_payload(&snapshot, workspace, thread, reference)
            .await
            .unwrap();
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
    assert_graph(db, "ws", "root-thread", &root_reference(), true, case).await;
}

#[pioneer_postgres_test::postgres_test]
async fn source_lookup_queries_match_legacy_for_all_source_types_and_identity_fields() {
    let db = fixture().await;
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

#[pioneer_postgres_test::postgres_test]
async fn published_checkpoint_is_independent_of_historical_leaf_identity_fields() {
    for branch in branches()
        .into_iter()
        .filter(|branch| branch.name != "checkpoint")
    {
        let db = fixture().await;
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
                true,
                &format!("{} graph wrong {field}", branch.name),
            )
            .await;
        }
    }
}

#[pioneer_postgres_test::postgres_test]
async fn published_checkpoint_is_independent_of_historical_leaf_workspace() {
    for branch in branches()
        .into_iter()
        .filter(|branch| branch.name != "checkpoint")
    {
        let db = fixture().await;
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
                    DbBackend::Postgres,
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
            true,
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

#[pioneer_postgres_test::postgres_test]
async fn canonical_liveness_changes_raw_lookup_but_not_published_checkpoint_lookup() {
    for canonical in canonical_branches() {
        let db = fixture().await;
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
    replace_task_basis(db, history_json).await;
    let revision = db
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
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

#[pioneer_postgres_test::postgres_test]
async fn task_basis_revision_and_original_json_array_prefix() {
    let db = fixture().await;
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
        DbBackend::Postgres,
        "SELECT COALESCE((SELECT version FROM compaction_projection_epoch WHERE thread_id='source-thread'),0) AS version",
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get("", "version")
    .unwrap()
}

#[pioneer_postgres_test::postgres_test]
async fn sources_current_treats_checkpoint_as_epoch_independent_published_object() {
    let db = fixture().await;
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
        false,
        "published checkpoint still requires format one",
    )
    .await;
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO compaction_projection_epoch(thread_id,version) VALUES ('source-thread',$1)",
        [epoch.into()],
    ))
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres, "UPDATE compaction_checkpoint SET format_version=2,projection_version=$1 WHERE id='source-checkpoint'", [epoch.into()])).await.unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        false,
        "checkpoint format is part of published identity",
    )
    .await;
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET format_version=1 WHERE id='source-checkpoint'",
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

    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres, "INSERT INTO compaction_projection_epoch(thread_id,version) VALUES ('source-thread',$1) ON CONFLICT(thread_id) DO UPDATE SET version=excluded.version", [(epoch + 1).into()])).await.unwrap();
    assert_sources(
        &db,
        "ws",
        "source-thread",
        std::slice::from_ref(&checkpoint),
        true,
        "published checkpoint survives projection epoch change",
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

#[pioneer_postgres_test::postgres_test]
async fn published_root_lookup_ignores_historical_ancestry_liveness() {
    let db = fixture().await;
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
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres, "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('mid-checkpoint',$1,$2,$3)", [event.scope.clone().into(), event.id.clone().into(), event.version.clone().into()])).await.unwrap();
    assert_graph(
        &db,
        "ws",
        "root-thread",
        &root_reference(),
        true,
        "multi-level foreign-thread leaf",
    )
    .await;
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres, "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('root-checkpoint',$1,$2,$3)", [event.scope.clone().into(), event.id.clone().into(), event.version.clone().into()])).await.unwrap();
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
        true,
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
        true,
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
        true,
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
        true,
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
        true,
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
        true,
        "missing intermediate checkpoint",
    )
    .await;
}

#[pioneer_postgres_test::postgres_test]
async fn published_checkpoint_lookup_does_not_traverse_large_historical_graph() {
    let db = fixture().await;
    for (start, end) in [(1_i64, 1_024_i64), (1_025, 65_535)] {
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO compaction_checkpoint(id,operation_id,owner,previous,portion,summary,identity_sha256,selection,projection_version,format_version,status)
             SELECT 'limit-'||lpad(n::text,5,'0'),'root-operation','root-owner',
                    CASE WHEN n<65535 THEN 'limit-'||lpad((n+1)::text,5,'0') END,
                    n,'limit','limit-version-'||lpad(n::text,5,'0'),'{}',0,1,'applied'
             FROM generate_series($1::bigint,$2::bigint) AS series(n)",
            [start.into(), end.into()],
        ))
        .await
        .unwrap();
        if start == 1 {
            // Let the per-row guard choose its exact (operation, portion)
            // index for the large suffix. On an empty heap it caches a scan.
            db.execute_unprepared("ANALYZE compaction_checkpoint")
                .await
                .unwrap();
        }
    }
    let leaf = canonical_branches().remove(2).branch.source;
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('limit-65535',$1,$2,$3)",
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
        true,
        "published root is independent of 65537 historical graph rows",
    )
    .await;
}

#[pioneer_postgres_test::postgres_test]
async fn source_lookup_queries_match_indexed_sources_with_compressed_payloads() {
    let db = fixture_with_portable_sources(true).await;
    let count = db
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT count(DISTINCT payload_id) AS n FROM portable_payload_part WHERE codec=1",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count.try_get::<i64>("", "n").unwrap(), 2);
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

async fn assert_production_plans(compressed: bool) {
    let db = fixture_with_portable_sources(compressed).await;
    seed_canonical_plan_noise(&db).await;
    let sources = branches()
        .into_iter()
        .map(|branch| branch.source)
        .collect::<Vec<_>>();
    let source_plan = explain(
        &db,
        compaction_sources_current_statement(
            serde_json::to_string(&sources).unwrap(),
            "ws",
            "source-thread",
        ),
    )
    .await;
    for (revision, canonical) in [
        ("context_revision", "context_source"),
        ("item_revision", "item_source"),
        ("event_revision", "event_source"),
        ("input_revision", "input_source"),
    ] {
        assert_exact_branch_search(&source_plan, revision, "source_id", true);
        assert_exact_branch_search(&source_plan, canonical, "id", true);
    }
    assert_exact_branch_search(&source_plan, "basis", "run_id", true);

    for query in [
        checkpoint_source_query("ws", "root-thread", "root-checkpoint").build(DbBackend::Postgres),
        checkpoint_payload_query("ws", "root-thread", &root_reference()).build(DbBackend::Postgres),
    ] {
        let plan = explain(&db, query).await;
        assert_exact_branch_search(&plan, "compaction_checkpoint", "id", false);
        assert!(
            plan.walk().into_iter().all(|node| {
                ![
                    "compaction_source_revision",
                    "compaction_item_revision",
                    "compaction_event_revision",
                    "compaction_input_revision",
                ]
                .iter()
                .any(|table| node.relation.as_deref() == Some(table))
            }),
            "checkpoint lookup unexpectedly reads canonical leaves: {plan:#?}"
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn source_lookup_plans_use_exact_keys_for_plain_and_zstd_storage() {
    assert_production_plans(false).await;
    assert_production_plans(true).await;
}
