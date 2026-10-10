use migration::{Migrator, MigratorTrait};
use pioneer_compaction::*;
use pioneer_crud::{CrudStore, compaction::*};
use sea_orm::{ConnectionTrait, Database, DbBackend, Statement, TransactionTrait};

async fn store() -> CrudStore {
    store_recording_statements(None).await
}

type RecordedStatements = std::sync::Arc<std::sync::Mutex<Vec<Statement>>>;

async fn store_recording_statements(statements: Option<RecordedStatements>) -> CrudStore {
    let mut db = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    if let Some(statements) = statements {
        db.set_metric_callback(move |info| {
            statements.lock().unwrap().push(info.statement.clone());
        });
    }
    let store = CrudStore::new(db).with_maintenance_access();
    let db = store.database_connection();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    store
}

#[tokio::test]
async fn exact_delivery_source_uses_event_keys_instead_of_scanning_revision_history() {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    for compressed in [false, true] {
        let statements = RecordedStatements::default();
        let store = store_recording_statements(Some(statements.clone())).await;
        let db = store.database_connection();
        if compressed {
            db.query_one_write_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT zstd_enable_transparent(?)",
                [serde_json::json!({
                    "table": "turn_event", "column": "payload",
                    "compression_level": 3, "dict_chooser": "'[nodict]'"
                })
                .to_string()
                .into()],
            ))
            .await
            .unwrap();
        }
        source(&store, "ordinary", 1, "ordinary history").await;
        statements.lock().unwrap().clear();
        assert!(
            store
                .compaction_task_delivery_command(
                    "ws",
                    "thread",
                    &SourceRef {
                        scope: "event:turn".into(),
                        id: "ordinary".into(),
                        version: "event-revision:1".into(),
                    }
                )
                .await
                .unwrap()
                .is_none()
        );
        // Inspect the actual repository statement, including its bound values.
        // This guards query work independently of machine speed and row count.
        let mut statement = statements
            .lock()
            .unwrap()
            .iter()
            .find(|statement| {
                statement.sql.contains("EXISTS") && statement.sql.contains("\"outcome\"")
            })
            .expect("failed-delivery fallback must be exercised")
            .clone();
        statement.sql = format!("EXPLAIN QUERY PLAN {}", statement.sql);
        let plan = db
            .query_all_raw(statement)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get::<String>("", "detail").unwrap())
            .collect::<Vec<_>>();
        assert!(
            plan.iter()
                .any(|detail| detail.contains("SEARCH r ") && detail.contains("source_id=?")),
            "{plan:#?}"
        );
        let event_table = if compressed { "_turn_event_zstd" } else { "e" };
        assert!(
            plan.iter().any(
                |detail| detail.starts_with(&format!("SEARCH {event_table} "))
                    && detail.contains("(id=?)")
            ),
            "exact source lookup must not scan its entire thread: {plan:#?}"
        );
        assert!(
            !plan
                .iter()
                .any(|detail| detail.contains("compaction_event_revision_capture_order")),
            "{plan:#?}"
        );
    }
}

#[tokio::test]
async fn history_capture_is_identical_before_and_after_transparent_compression() {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let store = store().await;
    let db = store.database_connection();
    source(&store, "first", 1, "retained original").await;
    let first = store
        .compaction_source_metadata_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries
        .remove(0)
        .reference;
    let fence = store.compaction_history_read_fence().await.unwrap();
    let before = store
        .compaction_history_turn_page("ws", "thread", "", &fence)
        .await
        .unwrap();
    assert_eq!(before.len(), 1);
    for table in ["turn_item", "turn_event"] {
        let config = serde_json::json!({
            "table": table, "column": "payload", "compression_level": 3,
            "dict_chooser": "'[nodict]'"
        });
        db.query_one_write_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT zstd_enable_transparent(?)",
            [config.to_string().into()],
        ))
        .await
        .unwrap();
    }
    source(&store, "later", 2, "after the captured fence").await;
    let after = store
        .compaction_history_turn_page("ws", "thread", "", &fence)
        .await
        .unwrap();
    assert_eq!(after.len(), before.len());
    let (before, after) = (&before[0], &after[0]);
    assert_eq!(after.id, before.id);
    assert_eq!(after.created_at, before.created_at);
    assert_eq!(after.creation_order, before.creation_order);
    assert_eq!(after.legacy_creation_order, before.legacy_creation_order);
    assert_eq!(after.status, before.status);
    assert_eq!(after.turn_kind, before.turn_kind);
    assert_eq!(after.send_mode, before.send_mode);
    assert_eq!(after.input_high_water, before.input_high_water);
    assert_eq!(after.event_high_water, before.event_high_water);
    assert_eq!(after.context_high_water, before.context_high_water);
    assert_eq!(after.event_high_water, 1);
    let next_fence = store.compaction_history_read_fence().await.unwrap();
    assert_eq!(
        store
            .compaction_history_turn_page("ws", "thread", "", &next_fence)
            .await
            .unwrap()[0]
            .event_high_water,
        2
    );
    assert!(
        store
            .compaction_history_turn_page("other", "thread", "", &fence)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .compaction_history_turn_page("ws", "thread", &after.id, &fence)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .compaction_reference_fragment("ws", "thread", &first, 0)
            .await
            .unwrap()
            .unwrap()
            .text,
        "retained original"
    );
}

#[tokio::test]
async fn accepted_turn_metadata_is_selected_before_boundary_subqueries() {
    let recorded = RecordedStatements::default();
    let store = store_recording_statements(Some(recorded.clone())).await;
    let db = store.database_connection();
    db.execute_unprepared(
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) \
         VALUES ('other-thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    )
    .await
    .unwrap();
    for index in 0..32 {
        for thread in ["thread", "other-thread"] {
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) \
                 VALUES (?,?,'completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
                [format!("{thread}-tail-{index:02}").into(), thread.into()],
            ))
            .await
            .unwrap();
        }
    }
    for _ in 0..16 {
        if store
            .compaction_prepare_history_quantum("ws", "thread")
            .await
            .unwrap()
        {
            break;
        }
    }
    assert!(
        store
            .compaction_history_prepared("ws", "thread")
            .await
            .unwrap()
    );
    let fence = store.compaction_history_read_fence().await.unwrap();
    recorded.lock().unwrap().clear();
    let page = store
        .compaction_history_selected_turn_page("ws", "thread", &["turn".into()], &fence)
        .await
        .unwrap();
    assert_eq!(
        page.iter().map(|turn| turn.id.as_str()).collect::<Vec<_>>(),
        ["turn"]
    );
    let statement = recorded
        .lock()
        .unwrap()
        .iter()
        .find(|statement| statement.sql.contains("input_high_water"))
        .cloned()
        .expect("selected metadata query must be recorded");
    assert!(statement.sql.contains(" IN "), "{statement:?}");
    let mut plan_statement = statement;
    plan_statement.sql = format!("EXPLAIN QUERY PLAN {}", plan_statement.sql);
    let plan = db
        .query_all_raw(plan_statement)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String>("", "detail").unwrap())
        .collect::<Vec<_>>();
    assert!(
        plan.iter()
            .any(|detail| detail.starts_with("SEARCH turn USING") && detail.contains("(id=?)")),
        "selected turn metadata must use its exact ID lookup: {plan:#?}"
    );
    assert!(
        store
            .compaction_history_selected_turn_page("other", "thread", &["turn".into()], &fence)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .compaction_history_selected_turn_page(
                "ws",
                "thread",
                &vec!["turn".to_owned(); 65],
                &fence,
            )
            .await
            .is_err(),
        "the selected metadata query must stay below SQLite parameter limits"
    );
    assert!(
        store
            .compaction_history_selected_turn_page(
                "ws",
                "thread",
                &["x".repeat(SOURCE_PAGE_BYTES + 1)],
                &fence,
            )
            .await
            .is_err(),
        "metadata IDs must respect the same byte quantum as source pages"
    );
}

#[tokio::test]
async fn exact_tool_item_reads_preserve_legacy_rows_and_revisions_with_compression() {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    for compressed in [false, true] {
        let store = store().await;
        let db = store.database_connection();
        for id in ["untracked", "seeded"] {
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                r#"INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,created_at,updated_at) VALUES (?,'turn',?,'command_execution','completed','{"storage":{"kind":"shell"},"output":"retained result"}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"#,
                [id.into(), id.into()])).await.unwrap();
        }
        // Both pre-migration states must remain readable by exact reference:
        // missing revision metadata and an already seeded revision. Fragment
        // reads remain physically read-only; legacy registration is explicit.
        db.execute_unprepared("DELETE FROM compaction_item_revision")
            .await
            .unwrap();
        db.execute_unprepared("INSERT INTO compaction_item_revision(source_id,turn_id,revision,present) VALUES ('seeded','turn',1,1)").await.unwrap();
        if compressed {
            let config = serde_json::json!({"table":"turn_item", "column":"payload", "compression_level":3, "dict_chooser":"'[nodict]'"});
            db.query_one_write_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT zstd_enable_transparent(?)",
                [config.to_string().into()],
            ))
            .await
            .unwrap();
        }
        for id in ["untracked", "seeded"] {
            let reference = store
                .compaction_tool_item_reference("ws", "thread", "turn", id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(reference.version, "item-revision:1");
            if id == "untracked" {
                assert!(
                    store
                        .compaction_reference_fragment("ws", "thread", &reference, 0)
                        .await
                        .unwrap()
                        .is_none()
                );
                let revisions = db
                    .query_one_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        "SELECT count(*) AS n FROM compaction_item_revision WHERE source_id=?",
                        [id.into()],
                    ))
                    .await
                    .unwrap()
                    .unwrap()
                    .try_get::<i64>("", "n")
                    .unwrap();
                assert_eq!(
                    revisions, 0,
                    "read-only fragment lookup registered legacy revision"
                );
            }
            store
                .compaction_prepare_references("ws", "thread", std::slice::from_ref(&reference))
                .await
                .unwrap();
            let fragment = store
                .compaction_reference_fragment("ws", "thread", &reference, 0)
                .await
                .unwrap()
                .unwrap();
            assert!(fragment.text.contains("retained result"));
            assert_eq!(fragment.reference, reference);
            assert_eq!(
                store
                    .compaction_replay_item_id("ws", "thread", &reference)
                    .await
                    .unwrap()
                    .as_deref(),
                Some(id)
            );
            for (workspace, thread) in [("other", "thread"), ("ws", "other")] {
                assert!(
                    store
                        .compaction_reference_fragment(workspace, thread, &reference, 0)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(
                    store
                        .compaction_replay_item_id(workspace, thread, &reference)
                        .await
                        .unwrap()
                        .is_none()
                );
            }
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "UPDATE turn_item SET payload=json_set(payload,'$.output','changed result') WHERE id=?", [id.into()]
            )).await.unwrap();
            assert!(
                store
                    .compaction_reference_fragment("ws", "thread", &reference, 0)
                    .await
                    .is_err()
            );
            assert!(
                store
                    .compaction_replay_item_id("ws", "thread", &reference)
                    .await
                    .unwrap()
                    .is_none()
            );
            let current = store
                .compaction_tool_result_fragment("ws", "thread", "turn", id, None, 0)
                .await
                .unwrap()
                .unwrap();
            assert!(current.text.contains("changed result"));
            assert_ne!(current.reference.version, reference.version);
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "DELETE FROM turn_item WHERE id=?",
                [id.into()],
            ))
            .await
            .unwrap();
            assert!(
                store
                    .compaction_reference_fragment("ws", "thread", &current.reference, 0)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }
}

#[tokio::test]
async fn history_capture_after_upgrading_an_already_compressed_database() {
    use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let connection = Database::connect("sqlite::memory:").await.unwrap();
    let writer = SqliteWriteExecutor::new(connection.clone());
    let before_compaction = Migrator::migrations()
        .iter()
        .position(|m| m.name() == "m20260910_000001_context_compaction")
        .unwrap();
    writer
        .run_migrations::<Migrator>(
            SqliteWriteClass::Maintenance,
            Some(before_compaction as u32),
        )
        .await
        .unwrap();
    let store = CrudStore::new(SqliteDatabase::from_executor(connection, writer.clone()))
        .with_maintenance_access();
    let db = store.database_connection();
    let config = serde_json::json!({"table":"turn_item", "column":"payload", "compression_level":3, "dict_chooser":"'[nodict]'"});
    db.query_one_write_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "SELECT zstd_enable_transparent(?)",
        [config.to_string().into()],
    ))
    .await
    .unwrap();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        r#"INSERT INTO turn_item(id,turn_id,item_id,item_type,status,active_attempt_number,payload,created_at,updated_at) VALUES ('legacy','turn','legacy','command_execution','completed',0,'{"storage":{"kind":"shell"},"output":"retained old result"}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"#,
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO turn_item(id,turn_id,item_id,item_type,status,active_attempt_number,payload,created_at,updated_at) VALUES ('item','turn','item','command_execution','completed',0,'{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    // Metadata discovery stays read-only after upgrading an already compressed
    // database. The pre-migration source is unavailable to fragment reads until
    // the explicit bounded legacy preparation phase registers its revision.
    let legacy_reference = store
        .compaction_tool_item_reference("ws", "thread", "turn", "legacy")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(legacy_reference.version, "item-revision:1");
    assert!(
        store
            .compaction_reference_fragment("ws", "thread", &legacy_reference, 0)
            .await
            .unwrap()
            .is_none()
    );
    let legacy_revisions = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT count(*) AS n FROM compaction_item_revision WHERE source_id='legacy'"
                .to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "n")
        .unwrap();
    assert_eq!(legacy_revisions, 0);
    store
        .compaction_prepare_references("ws", "thread", std::slice::from_ref(&legacy_reference))
        .await
        .unwrap();
    let legacy = store
        .compaction_reference_fragment("ws", "thread", &legacy_reference, 0)
        .await
        .unwrap()
        .unwrap();
    assert!(legacy.text.contains("retained old result"));
    assert_eq!(legacy.reference, legacy_reference);
    let old_fence = store.compaction_history_read_fence().await.unwrap();
    assert!(
        store
            .compaction_history_turn_page("ws", "thread", "", &old_fence)
            .await
            .is_err()
    );
    while !store
        .compaction_prepare_history_quantum("ws", "thread")
        .await
        .unwrap()
    {}
    let fence = store.compaction_history_read_fence().await.unwrap();
    for _ in 0..2 {
        let history = store
            .compaction_history_turn_page("ws", "thread", "", &fence)
            .await
            .unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].id, "turn");
        assert_eq!(history[0].event_high_water, 0);
        assert_eq!(history[0].context_high_water, 0);
        assert_eq!(history[0].input_high_water, 0);
        writer
            .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
            .await
            .unwrap();
    }
}
async fn source(store: &CrudStore, id: &str, sequence: i64, payload: &str) -> SourceAssertion {
    store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES (?,'thread','turn',?,'fixture',?,CURRENT_TIMESTAMP)",
        [id.into(), sequence.into(), payload.into()])).await.unwrap();
    let revision = store
        .database_connection()
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT revision FROM compaction_event_revision WHERE source_id=?1",
            [id.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "revision")
        .unwrap();
    SourceAssertion {
        revision: Some(revision),
        kind: CanonicalSource::Event,
        turn_id: "turn".into(),
        id: id.into(),
        payload: payload.into(),
    }
}
async fn candidate(
    store: &CrudStore,
    op: &str,
    expected: Option<&str>,
    assertion: &SourceAssertion,
) -> Checkpoint {
    candidate_with_epochs(
        store,
        op,
        expected,
        assertion,
        std::collections::BTreeMap::new(),
    )
    .await
}
async fn candidate_with_epochs(
    store: &CrudStore,
    op: &str,
    expected: Option<&str>,
    assertion: &SourceAssertion,
    source_epochs: std::collections::BTreeMap<String, u64>,
) -> Checkpoint {
    candidate_fixture(store, op, expected, assertion, source_epochs, false).await
}

async fn candidate_with_manifest(
    store: &CrudStore,
    op: &str,
    expected: Option<&str>,
    assertion: &SourceAssertion,
) -> Checkpoint {
    candidate_fixture(
        store,
        op,
        expected,
        assertion,
        std::collections::BTreeMap::new(),
        true,
    )
    .await
}

async fn candidate_fixture(
    store: &CrudStore,
    op: &str,
    expected: Option<&str>,
    assertion: &SourceAssertion,
    source_epochs: std::collections::BTreeMap<String, u64>,
    prepare_manifest: bool,
) -> Checkpoint {
    let checkpoint = candidate_admission_fixture(
        store,
        op,
        expected,
        assertion,
        source_epochs,
        prepare_manifest,
    )
    .await;
    store
        .compaction_save_candidate(&checkpoint, 0)
        .await
        .unwrap();
    checkpoint
}

async fn candidate_admission_fixture(
    store: &CrudStore,
    op: &str,
    expected: Option<&str>,
    assertion: &SourceAssertion,
    source_epochs: std::collections::BTreeMap<String, u64>,
    prepare_manifest: bool,
) -> Checkpoint {
    let selection = ModelSelection {
        transport: Transport::Api,
        instance: "p".into(),
        model: "m".into(),
        effort: None,
    };
    let snapshot = OperationSnapshot {
        id: op.into(),
        owner: "owner".into(),
        expected_checkpoint: expected.map(str::to_owned),
        projection_version: 1,
        source_epochs,
        admission: CompactionSettings::default()
            .admit(&selection, None, 10)
            .unwrap(),
        plan: CompactionPlan {
            mode: CompactionMode::Normal,
            coverage_domain: pioneer_compaction::CoverageDomain::OwnContribution,
            compact: vec![0],
            retain: vec![],
            coverage: vec![assertion.reference()],
            fingerprint: op.into(),
        },
    };
    store
        .compaction_admit("ws", "thread", &snapshot)
        .await
        .unwrap();
    if prepare_manifest {
        store
            .compaction_prepare_runner(op, &ModelBudget::new(None, None, None), 1, 0)
            .await
            .unwrap();
        store
            .compaction_append_manifest(
                op,
                &[ManifestEntry {
                    ordinal: 0,
                    unit: 0,
                    reference_only: false,
                    thread_id: "thread".into(),
                    source: assertion.reference(),
                }],
            )
            .await
            .unwrap();
    }
    let checkpoint = Checkpoint {
        id: format!("cp-{op}"),
        operation_id: op.into(),
        format_version: 1,
        owner: "owner".into(),
        previous: expected.map(str::to_owned),
        coverage: vec![assertion.reference()],
        summary: "A saved summary".into(),
        selection,
        projection_version: 1,
    };
    checkpoint
}
#[tokio::test]
async fn append_survives_atomic_apply_and_restart_does_not_regenerate_or_reapply() {
    let store = store().await;
    let first = source(&store, "source-a", 1, "original one").await;
    let cp = candidate(&store, "op-a", None, &first).await;
    assert!(
        store
            .compaction_manifest_page("op-a", false, 0, 0)
            .await
            .unwrap()
            .len()
            == 1,
        "raw assertion publication saves exact historical ownership"
    );
    source(&store, "source-b", 2, "appended after snapshot").await;
    assert_eq!(
        store
            .compaction_apply(&cp, None, &[first.clone()])
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let restarted = CrudStore::new(store.database_connection());
    assert_eq!(
        restarted.compaction_head("owner").await.unwrap().as_deref(),
        Some(cp.id.as_str())
    );
    assert_eq!(
        restarted
            .compaction_apply(&cp, None, &[first])
            .await
            .unwrap(),
        CommitOutcome::AlreadyApplied
    );
    let page = restarted
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 2);
    assert!(page.entries.iter().all(|s| !s.incomplete));
    assert_eq!(
        restarted
            .compaction_checkpoint(&cp.id)
            .await
            .unwrap()
            .unwrap()
            .coverage
            .len(),
        1
    );
}
#[tokio::test]
async fn edits_competing_head_and_stop_reject_candidates_without_losing_summaries() {
    let store = store().await;
    let s = source(&store, "source", 1, "original").await;
    let a = candidate(&store, "a", None, &s).await;
    let b = candidate(&store, "b", None, &s).await;
    assert_eq!(
        store
            .compaction_apply(&a, None, &[s.clone()])
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(
        store
            .compaction_apply(&b, None, &[s.clone()])
            .await
            .unwrap(),
        CommitOutcome::Stale
    );
    let c = candidate(&store, "c", Some(&a.id), &s).await;
    store
        .database_connection()
        .execute_unprepared("UPDATE turn_event SET payload='changed' WHERE id='source'")
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_apply(&c, Some(&a.id), &[s.clone()])
            .await
            .unwrap(),
        CommitOutcome::Stale
    );
    store
        .compaction_finish("c", "cancelled", "user_stop")
        .await
        .unwrap();
    assert_eq!(
        store.compaction_apply(&c, Some(&a.id), &[s]).await.unwrap(),
        CommitOutcome::Cancelled
    );
    for cp in [&a, &b, &c] {
        assert!(store.compaction_checkpoint(&cp.id).await.unwrap().is_some());
    }
    assert_eq!(
        store.compaction_head("owner").await.unwrap().as_deref(),
        Some(a.id.as_str())
    );
}

#[tokio::test]
async fn legacy_apply_ignores_coarse_epoch_changes_behind_previous_checkpoint() {
    for delete in [false, true] {
        let store = store().await;
        let a = source(&store, "independent-a", 1, "A").await;
        let s1 = candidate(&store, "independent-s1", None, &a).await;
        assert_eq!(
            store.compaction_apply(&s1, None, &[a]).await.unwrap(),
            CommitOutcome::Applied
        );

        let b = source(&store, "independent-b", 2, "B").await;
        let epoch = store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap();
        let s2 = candidate_with_epochs(
            &store,
            "independent-s2",
            Some(&s1.id),
            &b,
            std::collections::BTreeMap::from([("thread".into(), epoch)]),
        )
        .await;
        store
            .database_connection()
            .execute_unprepared(if delete {
                "DELETE FROM turn_event WHERE id='independent-a'"
            } else {
                "UPDATE turn_event SET payload='edited A' WHERE id='independent-a'"
            })
            .await
            .unwrap();

        assert_eq!(
            store
                .compaction_apply(&s2, Some(&s1.id), &[b])
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
    }
}

#[tokio::test]
async fn durable_retry_budget_and_exact_candidate_idempotency() {
    let store = store().await;
    let s = source(&store, "source", 1, "original").await;
    let cp = candidate(&store, "op", None, &s).await;
    store.compaction_save_candidate(&cp, 0).await.unwrap();
    let mut forged = cp.clone();
    forged.summary = "different result".into();
    assert!(store.compaction_save_candidate(&forged, 0).await.is_err());
    assert!(store.compaction_apply(&forged, None, &[s]).await.is_err());
    assert!(
        store
            .compaction_claim_attempt("op", 0, 20, false, false)
            .await
            .unwrap()
    );
    assert!(
        !store
            .compaction_claim_attempt("op", 0, 20, false, false)
            .await
            .unwrap()
    );
    assert!(
        store
            .compaction_claim_attempt("op", 1, 20, true, false)
            .await
            .unwrap()
    );
    let restarted = CrudStore::new(store.database_connection());
    assert!(
        restarted
            .compaction_claim_attempt("op", 2, 20, true, true)
            .await
            .unwrap()
    );
    assert!(
        !restarted
            .compaction_claim_attempt("op", 3, 20, true, false)
            .await
            .unwrap()
    );
    assert!(
        !restarted
            .compaction_claim_attempt("op", 3, 20, false, true)
            .await
            .unwrap()
    );
    assert!(
        !restarted
            .compaction_claim_attempt("op", 3, (10 + OPERATION_MILLIS) as i64, false, false)
            .await
            .unwrap()
    );
}
#[tokio::test]
async fn poison_source_progress_and_scope_are_bounded() {
    let store = store().await;
    source(&store, "huge", 1, &"x".repeat(SOURCE_PAGE_BYTES + 1)).await;
    source(&store, "next", 2, "small").await;
    let page = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    assert!(page.entries[0].incomplete);
    assert_eq!(page.entries[1].payload.as_deref(), Some("small"));
    assert_eq!(page.next_sequence, 2);
    assert!(
        store
            .compaction_source_page("other", "thread", "turn", PagedSource::Event, 0)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    assert_eq!(
        store.database_connection().read_class(),
        pioneer_sqlite::SqliteReadClass::Maintenance
    );
    assert_eq!(
        store.database_connection().write_class(),
        pioneer_sqlite::SqliteWriteClass::Maintenance
    );
}

#[tokio::test]
async fn cancellation_releases_writer_and_reader_is_physically_read_only() {
    use sea_orm::{ConnectOptions, TransactionTrait};
    use std::time::Duration;
    let directory = std::env::current_dir()
        .unwrap()
        .join("target/compaction-tests");
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join(format!("{}.sqlite", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let mut options = ConnectOptions::new(url);
    options.max_connections(1).min_connections(1);
    let writer = Database::connect(options).await.unwrap();
    Migrator::up(&writer, None).await.unwrap();
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=ro", path.display()));
    options.max_connections(1).min_connections(1);
    let reader = Database::connect(options).await.unwrap();
    reader
        .execute_unprepared("PRAGMA query_only=ON")
        .await
        .unwrap();
    let database = pioneer_sqlite::SqliteDatabase::new(reader, writer);
    assert!(database.reader_query_only_enabled().await.unwrap());
    let store = CrudStore::new(database.clone()).with_maintenance_access();
    let hold = database.begin().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), store.compaction_head("none"))
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    let waiting = tokio::spawn({
        let store = store.clone();
        async move { store.compaction_finish("none", "cancelled", "stop").await }
    });
    tokio::task::yield_now().await;
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    hold.rollback().await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        store.compaction_finish("none", "failed", "done"),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(database.reader_query_only_enabled().await.unwrap());
    drop(store);
    drop(database);
    // This file belongs exclusively to this test. No application configuration is used.
    std::fs::remove_file(&path).unwrap();
    for suffix in ["-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test]
async fn large_canonical_source_reads_are_version_bound_and_cleanup_preserves_original() {
    let store = store().await;
    let text = "🌍漢字abcdef".repeat(40_000);
    store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES ('large','turn',1,'tool_result_v2',?,'{}',CURRENT_TIMESTAMP)", [text.clone().into()])).await.unwrap();
    let mut offset = 0;
    let mut rebuilt = String::new();
    let mut revision = None;
    loop {
        let fragment = store
            .compaction_source_fragment(
                "ws",
                "thread",
                "turn",
                "large",
                revision.as_deref(),
                offset,
            )
            .await
            .unwrap()
            .unwrap();
        assert!(fragment.text.len() <= 64 * 1024);
        revision = Some(fragment.reference.version);
        rebuilt.push_str(&fragment.text);
        let Some(next) = fragment.next_character else {
            break;
        };
        offset = next;
    }
    assert_eq!(rebuilt, text);
    assert_eq!(
        store
            .delete_turn_llm_context_for_terminal_turns()
            .await
            .unwrap(),
        0
    );
    let source = SourceAssertion {
        revision: Some(1),
        kind: CanonicalSource::ProviderContext,
        turn_id: "turn".into(),
        id: "large".into(),
        payload: String::new(),
    };
    let cp = candidate(&store, "large-op", None, &source).await;
    store
        .database_connection()
        .execute_unprepared("UPDATE turn_llm_context SET payload='changed' WHERE id='large'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_source_fragment("ws", "thread", "turn", "large", revision.as_deref(), 0)
            .await
            .is_err()
    );
    assert_eq!(
        store.compaction_apply(&cp, None, &[source]).await.unwrap(),
        CommitOutcome::Stale
    );
}

#[tokio::test]
async fn tool_result_lookup_never_crosses_workspace_thread_or_turn() {
    let store = store().await;
    store.database_connection().execute_unprepared("INSERT INTO turn_llm_context(id,turn_id,item_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES ('result','turn','call-item',1,'tool_result_v2','{}','{}',CURRENT_TIMESTAMP)").await.unwrap();
    assert_eq!(
        store
            .compaction_tool_result_id("ws", "thread", "turn", "call-item")
            .await
            .unwrap()
            .as_deref(),
        Some("result")
    );
    for (workspace, thread, turn) in [
        ("other", "thread", "turn"),
        ("ws", "other", "turn"),
        ("ws", "thread", "other"),
    ] {
        assert!(
            store
                .compaction_tool_result_id(workspace, thread, turn, "call-item")
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn full_shell_result_reuses_terminal_item_and_binds_revision() {
    let store = store().await;
    let payload = serde_json::json!({"storage":{"kind":"shell","stdout":"🌍漢字".repeat(20_000),"truncated":true}}).to_string();
    store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,payload,created_at,updated_at) VALUES ('shell-row','turn','shell-item','command_execution',?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", [payload.clone().into()])).await.unwrap();
    assert!(
        store
            .compaction_tool_result_id("ws", "thread", "turn", "shell-item")
            .await
            .unwrap()
            .is_none()
    );
    let first = store
        .compaction_tool_result_fragment("ws", "thread", "turn", "shell-item", None, 0)
        .await
        .unwrap()
        .unwrap();
    let version = first.reference.version.clone();
    assert!(version.starts_with("item-revision:"));
    let mut restored = first.text;
    let mut next = first.next_character;
    while let Some(offset) = next {
        let fragment = store
            .compaction_tool_result_fragment(
                "ws",
                "thread",
                "turn",
                "shell-item",
                Some(&version),
                offset,
            )
            .await
            .unwrap()
            .unwrap();
        restored.push_str(&fragment.text);
        next = fragment.next_character;
    }
    assert_eq!(restored, payload);
    assert!(
        store
            .compaction_tool_result_fragment("other", "thread", "turn", "shell-item", None, 0)
            .await
            .unwrap()
            .is_none()
    );
    let assertion = SourceAssertion {
        kind: CanonicalSource::ToolItem,
        turn_id: "turn".into(),
        id: "shell-row".into(),
        payload: String::new(),
        revision: Some(
            version
                .strip_prefix("item-revision:")
                .unwrap()
                .parse()
                .unwrap(),
        ),
    };
    let checkpoint = candidate(&store, "shell-op", None, &assertion).await;
    store.database_connection().execute_unprepared("UPDATE turn_item SET payload=json_set(payload,'$.storage.truncated',0) WHERE id='shell-row'").await.unwrap();
    assert_eq!(
        store
            .compaction_apply(&checkpoint, None, &[assertion])
            .await
            .unwrap(),
        CommitOutcome::Stale
    );
    assert!(
        store
            .compaction_tool_result_fragment(
                "ws",
                "thread",
                "turn",
                "shell-item",
                Some(&version),
                0
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn durable_runner_manifest_candidate_and_state_commit_together() {
    verify_runner_commit_dependency(false).await;
}
#[tokio::test]
async fn selected_source_edit_does_not_invalidate_completed_runner_summary() {
    verify_runner_commit_dependency(true).await;
}
async fn verify_runner_commit_dependency(edit_parent: bool) {
    use pioneer_compaction::runner::{RunnerState, SourceCursor};
    let store = store().await;
    store.database_connection().execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('parent','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    store.database_connection().execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('parent-turn','parent','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    store.database_connection().execute_unprepared("INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('parent-source','parent','parent-turn',1,'fixture','reference fact',CURRENT_TIMESTAMP)").await.unwrap();
    source(&store, "runner-source", 1, "original source").await;
    // Simulate a source predating the migration: admission must seed revision
    // metadata even if this source is never read by the service materializer.
    store
        .database_connection()
        .execute_unprepared("DELETE FROM compaction_event_revision WHERE source_id='runner-source'")
        .await
        .unwrap();
    let page = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    let reference = page.entries[0].reference.clone();
    assert_eq!(reference.version, "event-revision:1");
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        0
    );
    let snapshot = OperationSnapshot {
        id: "runner".into(),
        owner: "runner-owner".into(),
        expected_checkpoint: None,
        projection_version: 0,
        source_epochs: std::collections::BTreeMap::from([("parent".into(), 0)]),
        admission: CompactionSettings::default()
            .admit(
                &ModelSelection {
                    transport: Transport::Api,
                    instance: "p".into(),
                    model: "m".into(),
                    effort: None,
                },
                None,
                0,
            )
            .unwrap(),
        plan: CompactionPlan {
            mode: CompactionMode::Normal,
            coverage_domain: pioneer_compaction::CoverageDomain::OwnContribution,
            compact: vec![0],
            retain: vec![],
            coverage: vec![reference.clone()],
            fingerprint: "runner-plan".into(),
        },
    };
    store
        .compaction_admit("ws", "thread", &snapshot)
        .await
        .unwrap();
    let budget = ModelBudget::new(None, None, None);
    store
        .compaction_prepare_runner("runner", &budget, 1, 0)
        .await
        .unwrap();
    let initial = RunnerState::new(snapshot.admission.deadline_ms, &budget, 1000, None).unwrap();
    assert!(
        store
            .compaction_activate_runner("runner", &initial)
            .await
            .is_err()
    );
    let manifest = ManifestEntry {
        ordinal: 0,
        unit: 0,
        reference_only: false,
        thread_id: "thread".into(),
        source: reference.clone(),
    };
    store
        .compaction_append_manifest("runner", &[manifest.clone()])
        .await
        .unwrap();
    store
        .compaction_append_manifest("runner", &[manifest])
        .await
        .unwrap();
    store
        .compaction_activate_runner("runner", &initial)
        .await
        .unwrap();
    let attempt = initial.claim(1).unwrap();
    assert!(
        store
            .compaction_runner_transition("runner", initial.generation, &attempt, None)
            .await
            .unwrap()
    );
    assert!(
        !store
            .compaction_runner_transition("runner", initial.generation, &attempt, None)
            .await
            .unwrap()
    );
    let next = attempt
        .candidate(
            1,
            "runner-candidate".into(),
            SourceCursor {
                unit: 1,
                ..Default::default()
            },
            true,
            2,
        )
        .unwrap();
    let checkpoint = Checkpoint {
        id: "runner-candidate".into(),
        operation_id: "runner".into(),
        format_version: 1,
        owner: snapshot.owner.clone(),
        previous: None,
        coverage: vec![reference],
        summary: "saved complete candidate".into(),
        selection: snapshot.admission.selection.clone(),
        projection_version: 0,
    };
    let mut wrong_selection = checkpoint.clone();
    wrong_selection.selection.model = "changed-after-admission".into();
    assert!(
        store
            .compaction_runner_transition(
                "runner",
                attempt.generation,
                &next,
                Some(&wrong_selection)
            )
            .await
            .is_err()
    );
    assert_eq!(
        store
            .compaction_runner_state("runner")
            .await
            .unwrap()
            .unwrap(),
        attempt
    );
    let mut wrong_basis = checkpoint.clone();
    wrong_basis.previous = Some("unrelated-checkpoint".into());
    assert!(
        !store
            .compaction_runner_transition("runner", attempt.generation, &next, Some(&wrong_basis))
            .await
            .unwrap()
    );
    assert!(
        store
            .compaction_checkpoint("runner-candidate")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_runner_transition("runner", attempt.generation, &next, Some(&checkpoint))
            .await
            .unwrap()
    );
    let restored = store
        .compaction_runner_state("runner")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored, next);
    assert_eq!(
        store
            .compaction_checkpoint("runner-candidate")
            .await
            .unwrap()
            .unwrap()
            .summary,
        "saved complete candidate"
    );
    let ready = restored.candidate_checked(true).unwrap();
    store
        .compaction_runner_transition("runner", restored.generation, &ready, None)
        .await
        .unwrap();
    if edit_parent {
        store
            .database_connection()
            .execute_unprepared(
                "UPDATE turn_event SET payload='edited selected source' WHERE id='runner-source'",
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .compaction_apply_runner("runner", &ready, None)
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
        assert_eq!(
            store
                .compaction_head("runner-owner")
                .await
                .unwrap()
                .as_deref(),
            Some("runner-candidate")
        );
        assert_eq!(
            store
                .compaction_checkpoint("runner-candidate")
                .await
                .unwrap()
                .unwrap()
                .summary,
            "saved complete candidate"
        );
        return;
    }
    // The real message materializer first creates and then completes the next
    // user message. That update advances the parent's broad epoch, but the new
    // item was never part of this operation's immutable manifest or coverage.
    let parent = store.get_thread_model("parent").await.unwrap().unwrap();
    let (_, mut next_turn) = store
        .get_turn("parent", "parent-turn")
        .await
        .unwrap()
        .unwrap();
    next_turn.id = "next-parent-turn".into();
    next_turn.reply_to_turn_id = None;
    store
        .materialize_turn_start(
            &parent,
            pioneer_protocol::SandboxMode::FullAccess,
            &next_turn,
            &[],
            pioneer_protocol::PersistedActorRef::System,
        )
        .await
        .unwrap();
    let message = pioneer_protocol::TurnItem::UserMessage {
        id: "next-parent-message".into(),
        text: "new unselected work".into(),
        attachments: vec![],
    };
    store
        .materialize_item_started(
            pioneer_protocol::ItemStartedNotification {
                workspace_id: "ws".into(),
                thread_id: "parent".into(),
                turn_id: next_turn.id.clone(),
                item: message.clone(),
            },
            3,
        )
        .await
        .unwrap();
    store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "parent".into(),
                turn_id: next_turn.id,
                item: message,
            },
            4,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "parent")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .compaction_apply_runner("runner", &ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(
        store
            .compaction_apply_runner("runner", &ready, None)
            .await
            .unwrap(),
        CommitOutcome::AlreadyApplied
    );
    let derived = store
        .compaction_checkpoint_source("ws", "thread", "runner-candidate")
        .await
        .unwrap()
        .unwrap();
    let coverage = store
        .compaction_checkpoint("runner-candidate")
        .await
        .unwrap()
        .unwrap()
        .coverage;
    assert_eq!(coverage.len(), 1);
    assert!(
        coverage
            .iter()
            .all(|source| !source.scope.ends_with(":next-parent-turn"))
    );
    assert_eq!(
        store
            .compaction_reference_fragment("ws", "thread", &derived, 0)
            .await
            .unwrap()
            .unwrap()
            .text,
        "saved complete candidate"
    );
    store
        .database_connection()
        .execute_unprepared("UPDATE turn_event SET payload='edited' WHERE id='runner-source'")
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        1
    );
    assert!(
        store
            .compaction_checkpoint_source("ws", "thread", "runner-candidate")
            .await
            .unwrap()
            .is_some(),
        "published checkpoint must survive an edit to its historical leaf"
    );
    // FK-on cascade must not bypass permanent checkpoint identity/ownership
    // guards. The rejected delete rolls back the entire thread cascade.
    assert!(
        store
            .database_connection()
            .execute_unprepared("DELETE FROM thread WHERE id='thread'")
            .await
            .is_err()
    );
    assert!(store.get_thread_model("thread").await.unwrap().is_some());
    assert_eq!(
        store.compaction_head(&checkpoint.owner).await.unwrap(),
        Some(checkpoint.id.clone())
    );
}

#[tokio::test]
async fn projection_epoch_changes_for_history_edits_without_invalidating_active_parent_appends() {
    let store = store().await;
    let db = store.database_connection();
    db.execute_unprepared("INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,created_at,updated_at) VALUES ('live-row','turn','live-item','command_execution','in_progress','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared(
        "UPDATE turn_item SET payload='{\"stdout\":\"progress\"}' WHERE id='live-row'",
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        0
    );
    db.execute_unprepared("UPDATE turn_item SET status='completed',payload='{\"stdout\":\"finished\"}' WHERE id='live-row'").await.unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        0
    );
    source(&store, "appended-parent-event", 1, "completed new work").await;
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        0
    );
    db.execute_unprepared("UPDATE turn_item SET payload='{\"stdout\":\"edited historical result\"}' WHERE id='live-row'").await.unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn user_input_revisions_bind_edits_and_deletes_without_invalidating_appends() {
    let store = store().await;
    let db = store.database_connection();
    db.execute_unprepared("INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('original-input','turn',0,'text','original','{\"type\":\"text\",\"text\":\"original\"}',CURRENT_TIMESTAMP)").await.unwrap();
    let page = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Input, 0)
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    let original = page.entries[0].reference.clone();
    assert_eq!(original.version, "input-revision:1");
    assert!(
        store
            .compaction_reference_fragment("ws", "thread", &original, 0)
            .await
            .unwrap()
            .unwrap()
            .text
            .contains("original")
    );
    db.execute_unprepared("INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('new-input','turn',1,'text','new','{\"type\":\"text\",\"text\":\"new\"}',CURRENT_TIMESTAMP)").await.unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        0
    );
    db.execute_unprepared("UPDATE turn_input SET text='edited',payload='{\"type\":\"text\",\"text\":\"edited\"}' WHERE id='original-input'").await.unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        1
    );
    assert!(
        store
            .compaction_reference_fragment("ws", "thread", &original, 0)
            .await
            .is_err()
    );
    let changed = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Input, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    assert_eq!(changed.version, "input-revision:2");
    assert!(
        store
            .compaction_reference_fragment("other", "thread", &changed, 0)
            .await
            .unwrap()
            .is_none()
    );
    db.execute_unprepared("DELETE FROM turn_input WHERE id='original-input'")
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        2
    );
    assert!(
        store
            .compaction_reference_fragment("ws", "thread", &changed, 0)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn source_high_water_is_scoped_and_append_does_not_move_a_captured_boundary() {
    let store = store().await;
    source(&store, "first", 1, "first payload").await;
    let high_water = store
        .compaction_source_high_water("ws", "thread", "turn", PagedSource::Event)
        .await
        .unwrap();
    assert_eq!(high_water, 1);
    assert_eq!(
        store
            .compaction_source_high_water("other", "thread", "turn", PagedSource::Event)
            .await
            .unwrap(),
        0
    );
    source(&store, "appended", 2, "later payload").await;
    let page = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    let pinned: Vec<_> = page
        .entries
        .into_iter()
        .filter(|row| row.sequence <= high_water)
        .collect();
    assert_eq!(pinned.len(), 1);
    assert_eq!(pinned[0].reference.id, "first");
    assert_eq!(pinned[0].payload.as_deref(), Some("first payload"));
    assert_eq!(
        store
            .compaction_source_high_water("ws", "thread", "turn", PagedSource::Event)
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn history_fence_excludes_late_completion_and_reused_canonical_rowid() {
    let store = store().await;
    source(&store, "before", 1, "before snapshot").await;
    let fence = store.compaction_history_read_fence().await.unwrap();
    source(&store, "late", 2, "completed after snapshot").await;
    let page = store
        .compaction_history_turn_page("ws", "thread", "", &fence)
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].event_high_water, 1);
    assert!(
        store
            .compaction_history_turn_page("other", "thread", "", &fence)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .compaction_history_turn_page("ws", "thread", &page[0].id, &fence)
            .await
            .unwrap()
            .is_empty()
    );
    let next = store.compaction_history_read_fence().await.unwrap();
    assert_eq!(
        store
            .compaction_history_turn_page("ws", "thread", "", &next)
            .await
            .unwrap()[0]
            .event_high_water,
        2
    );
    // Deleting the largest canonical row permits SQLite to reuse its rowid.
    // Retained revision metadata must still classify the replacement as new.
    store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_event WHERE id='late'")
        .await
        .unwrap();
    source(&store, "replacement", 3, "new after deletion").await;
    assert_eq!(
        store
            .compaction_history_turn_page("ws", "thread", "", &next)
            .await
            .unwrap()[0]
            .event_high_water,
        1
    );
}

#[tokio::test]
async fn metadata_projection_keeps_exact_sources_without_loading_covered_payloads() {
    let store = store().await;
    source(&store, "small-metadata", 1, "small body").await;
    source(
        &store,
        "large-metadata",
        2,
        &"x".repeat(SOURCE_PAGE_BYTES + 1),
    )
    .await;
    let metadata = store
        .compaction_source_metadata_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    assert_eq!(metadata.entries.len(), 2);
    assert_eq!(metadata.next_sequence, 2);
    assert!(metadata.entries.iter().all(|row| row.payload.is_none()));
    let materialized = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    assert_eq!(
        metadata.entries[0].reference,
        materialized.entries[0].reference
    );
    assert_eq!(
        metadata.entries[1].reference,
        materialized.entries[1].reference
    );
    assert_eq!(
        materialized.entries[0].payload.as_deref(),
        Some("small body")
    );
    assert!(materialized.entries[1].payload.is_none());
    assert!(
        store
            .compaction_source_metadata_page("other", "thread", "turn", PagedSource::Event, 0)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
}

#[tokio::test]
async fn fitting_request_validation_rejects_edited_deleted_and_cross_scope_sources() {
    let store = store().await;
    source(&store, "current-source", 1, "original").await;
    let reference = store
        .compaction_source_metadata_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    assert!(
        store
            .compaction_sources_current("ws", "thread", &[reference.clone()])
            .await
            .unwrap()
    );
    assert!(
        !store
            .compaction_sources_current("other", "thread", &[reference.clone()])
            .await
            .unwrap()
    );
    store
        .database_connection()
        .execute_unprepared("UPDATE turn_event SET payload='edited' WHERE id='current-source'")
        .await
        .unwrap();
    assert!(
        !store
            .compaction_sources_current("ws", "thread", &[reference])
            .await
            .unwrap()
    );
    let updated = store
        .compaction_source_metadata_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    assert!(
        store
            .compaction_sources_current("ws", "thread", &[updated.clone()])
            .await
            .unwrap()
    );
    store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_event WHERE id='current-source'")
        .await
        .unwrap();
    assert!(
        !store
            .compaction_sources_current("ws", "thread", &[updated])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn canonical_event_metadata_uses_typed_identity_and_invalidates_on_edit() {
    let store = store().await;
    for item in [
        pioneer_protocol::TurnItem::Reasoning {
            id: "reasoning-source".into(),
            summary: vec![],
            content: vec!["equal text".into()],
        },
        pioneer_protocol::TurnItem::AgentMessage {
            id: "answer-source".into(),
            text: "equal text".into(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
    ] {
        store
            .materialize_item_completed(
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: "turn".into(),
                    item,
                },
                chrono::Utc::now().timestamp(),
            )
            .await
            .unwrap();
    }
    let metadata = store
        .compaction_source_metadata_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    let reasoning = metadata
        .entries
        .iter()
        .find(|row| row.item_id.as_deref() == Some("reasoning-source"))
        .unwrap();
    let answer = metadata
        .entries
        .iter()
        .find(|row| row.item_id.as_deref() == Some("answer-source"))
        .unwrap();
    assert_eq!(reasoning.projection_kind.as_deref(), Some("reasoning"));
    assert_eq!(answer.projection_kind.as_deref(), Some("assistant"));
    assert_ne!(reasoning.reference, answer.reference);
    store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "UPDATE turn_event SET payload=replace(payload,'equal text','edited content') WHERE id=?", [reasoning.reference.id.clone().into()])).await.unwrap();
    let edited = store
        .compaction_source_metadata_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    let invalidated = edited
        .entries
        .iter()
        .find(|row| row.reference.id == reasoning.reference.id)
        .unwrap();
    assert_eq!(invalidated.projection_kind, None);
    assert_eq!(invalidated.item_id, None);
    assert_ne!(invalidated.reference.version, reasoning.reference.version);
}

#[tokio::test]
async fn input_projection_uses_accepted_input_order_instead_of_insertion_order() {
    let store = store().await;
    let mut insertion_fence = None;
    for (id, index) in [("later-input", 1_i64), ("first-input", 0_i64)] {
        store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES (?,'turn',?,'text','fixture','{}',CURRENT_TIMESTAMP)", [id.into(),index.into()])).await.unwrap();
        if index == 1 {
            insertion_fence = Some(store.compaction_history_read_fence().await.unwrap());
        }
    }
    let frozen = store
        .compaction_source_metadata_page_at_fence(
            "ws",
            "thread",
            "turn",
            PagedSource::Input,
            0,
            insertion_fence.unwrap().input_order,
        )
        .await
        .unwrap();
    assert_eq!(
        frozen.entries.len(),
        1,
        "a late lower input index must not enter the captured snapshot"
    );
    assert_eq!(frozen.entries[0].reference.id, "later-input");
    let page = store
        .compaction_source_metadata_page("ws", "thread", "turn", PagedSource::Input, 0)
        .await
        .unwrap();
    assert_eq!(
        page.entries
            .iter()
            .map(|row| row.reference.id.as_str())
            .collect::<Vec<_>>(),
        vec!["first-input", "later-input"]
    );
    let next = store
        .compaction_source_metadata_page(
            "ws",
            "thread",
            "turn",
            PagedSource::Input,
            page.entries[0].sequence,
        )
        .await
        .unwrap();
    assert_eq!(next.entries.len(), 1);
    assert_eq!(next.entries[0].reference.id, "later-input");
}

#[tokio::test]
async fn frozen_history_is_paged_immutable_scoped_and_restart_safe() {
    use pioneer_compaction::frozen::{FrozenHistoryRef, FrozenMessageRef};
    let store = store().await;
    let descriptor = FrozenHistoryRef {
        format: 1,
        manifest_id: "frozen-manifest".into(),
        messages: 130,
        identity_sha256: "a".repeat(64),
    };
    let messages = (0..130)
        .map(|i| FrozenMessageRef {
            logical_turn_id: Some("command-turn".into()),
            context_thread: None,
            source_thread: "thread".into(),
            unit_id: format!("unit-{i}"),
            sources: vec![SourceRef {
                scope: "event:turn".into(),
                id: format!("event-{i}"),
                version: "event-revision:1".into(),
            }],
            event_input_role: None,
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
            publication_aliases: None,
            inherited: false,
            complete: true,
            protected_input: false,
            wire_sha256: "b".repeat(64),
            replay_source: None,
            tool_item_id: None,
            tool_call_id: None,
            tool_name: None,
        })
        .collect::<Vec<_>>();
    store
        .compaction_begin_frozen_history("ws", "thread", &descriptor)
        .await
        .unwrap();
    assert!(
        !store
            .compaction_finish_frozen_history("ws", "thread", &descriptor)
            .await
            .unwrap()
    );
    store
        .compaction_append_frozen_history(
            "ws",
            "thread",
            &descriptor.manifest_id,
            0,
            &messages[..128],
        )
        .await
        .unwrap();
    // R1: the committed prefix is visible in the logical view before ready;
    // physical allocation beyond next is not part of the logical stream.
    let visible = store
        .database_connection()
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT COUNT(*) AS n FROM compaction_frozen_message WHERE manifest_id=?",
            [descriptor.manifest_id.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "n")
        .unwrap();
    assert_eq!(visible, 128);
    assert!(
        store
            .compaction_frozen_history_owner("ws", &descriptor)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_frozen_history_page("ws", "thread", &descriptor.manifest_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
    // Restart retries the same immutable metadata, then finishes the remaining quantum.
    store
        .compaction_begin_frozen_history("ws", "thread", &descriptor)
        .await
        .unwrap();
    store
        .compaction_append_frozen_history(
            "ws",
            "thread",
            &descriptor.manifest_id,
            0,
            &messages[..128],
        )
        .await
        .unwrap();
    store
        .compaction_append_frozen_history(
            "ws",
            "thread",
            &descriptor.manifest_id,
            128,
            &messages[128..],
        )
        .await
        .unwrap();
    assert!(
        store
            .compaction_finish_frozen_history("ws", "thread", &descriptor)
            .await
            .unwrap()
    );
    assert!(
        store
            .compaction_finish_frozen_history("ws", "thread", &descriptor)
            .await
            .unwrap()
    );
    let page = store
        .compaction_frozen_history_page("ws", "thread", &descriptor.manifest_id, 0)
        .await
        .unwrap();
    assert_eq!(page, messages[..128]);
    assert_eq!(
        store
            .compaction_frozen_history_page("ws", "thread", &descriptor.manifest_id, 128)
            .await
            .unwrap(),
        messages[128..]
    );
    assert!(
        store
            .compaction_frozen_history_page("other", "thread", &descriptor.manifest_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .compaction_frozen_history_owner("other", &descriptor)
            .await
            .unwrap()
            .is_none()
    );
    let mut changed = messages[0].clone();
    changed.wire_sha256 = "c".repeat(64);
    assert!(
        store
            .compaction_append_frozen_history(
                "ws",
                "thread",
                &descriptor.manifest_id,
                0,
                &[changed]
            )
            .await
            .is_err()
    );
    assert_eq!(
        store
            .compaction_frozen_history_page("ws", "thread", &descriptor.manifest_id, 0)
            .await
            .unwrap(),
        page
    );
    let mut collision = descriptor.clone();
    collision.messages = 1;
    assert!(
        store
            .compaction_begin_frozen_history("ws", "thread", &collision)
            .await
            .is_err()
    );
    assert!(
        store
            .compaction_append_frozen_history(
                "ws",
                "thread",
                &descriptor.manifest_id,
                130,
                &messages[..1]
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn exact_reference_scope_requires_workspace_and_current_revision() {
    let store = store().await;
    source(&store, "lookup-source", 1, "original").await;
    let reference = store
        .compaction_source_metadata_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    assert_eq!(
        store
            .compaction_reference_thread("ws", &reference)
            .await
            .unwrap()
            .as_deref(),
        Some("thread")
    );
    assert!(
        store
            .compaction_reference_thread("other-workspace", &reference)
            .await
            .unwrap()
            .is_none()
    );
    store
        .database_connection()
        .execute_unprepared("UPDATE turn_event SET payload='edited' WHERE id='lookup-source'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_reference_thread("ws", &reference)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn inherited_source_epoch_fences_commit_but_parent_append_does_not() {
    for edited in [false, true] {
        let store = store().await;
        let db = store.database_connection();
        db.execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('parent','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('parent-turn','parent','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        db.execute_unprepared("INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('parent-source','parent','parent-turn',1,'fixture','reference fact',CURRENT_TIMESTAMP)").await.unwrap();
        let own = source(&store, "own", 1, "own completed work").await;
        let parent_epoch = store
            .compaction_projection_version("ws", "parent")
            .await
            .unwrap();
        let cp = candidate_with_epochs(
            &store,
            "guarded",
            None,
            &own,
            std::collections::BTreeMap::from([("parent".into(), parent_epoch)]),
        )
        .await;
        if edited {
            db.execute_unprepared(
                "UPDATE turn_event SET payload='edited fact' WHERE id='parent-source'",
            )
            .await
            .unwrap();
        } else {
            db.execute_unprepared("INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('parent-append','parent','parent-turn',2,'fixture','later work',CURRENT_TIMESTAMP)").await.unwrap();
        }
        let result = store.compaction_apply(&cp, None, &[own]).await.unwrap();
        assert_eq!(
            result,
            if edited {
                CommitOutcome::Stale
            } else {
                CommitOutcome::Applied
            }
        );
        assert_eq!(
            store.compaction_head("owner").await.unwrap().is_some(),
            !edited
        );
        assert!(store.compaction_checkpoint(&cp.id).await.unwrap().is_some());
    }
}

#[tokio::test]
async fn source_epoch_admission_rejects_changed_missing_and_foreign_scope() {
    let store = store().await;
    let own = source(&store, "own", 1, "completed work").await;
    candidate(&store, "original", None, &own).await;
    store
        .database_connection()
        .execute_unprepared(
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('other','other',1,0)",
        )
        .await
        .unwrap();
    store.database_connection().execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('foreign','other','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    let original: OperationSnapshot = serde_json::from_str(
        &store
            .compaction_operation("original")
            .await
            .unwrap()
            .unwrap()
            .snapshot,
    )
    .unwrap();
    for (index, (thread, epoch)) in [("missing", 0), ("thread", 99), ("foreign", 0)]
        .into_iter()
        .enumerate()
    {
        let mut snapshot = original.clone();
        snapshot.id = format!("invalid-{index}");
        snapshot.plan.fingerprint = snapshot.id.clone();
        snapshot.source_epochs = std::collections::BTreeMap::from([(thread.into(), epoch)]);
        assert!(
            store
                .compaction_admit("ws", "thread", &snapshot)
                .await
                .is_err()
        );
        assert!(
            store
                .compaction_operation(&snapshot.id)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn causal_task_boundary_requires_identified_delivery_inside_capture_fence() {
    let store = store().await;
    let db = store.database_connection();
    for sql in [
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run','task','run',1,1,'running','agent')",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('run','thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,thread_target,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id) VALUES ('delivery','ws','task','run','key','thread','origin_thread','thread','delivered',1,1,'run')",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    let empty = store.compaction_history_read_fence().await.unwrap();
    let boundary = store
        .compaction_history_causal_boundary("ws", "thread", "turn", &empty)
        .await
        .unwrap();
    assert!(boundary.delegated_command);
    assert!(
        !boundary.delivered_outcome,
        "delivery status without an acknowledged payload is not closure"
    );
    assert!(
        store
            .compaction_history_causal_boundary("ws", "thread", "run", &empty)
            .await
            .unwrap()
            .task_transport
    );
    store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "run".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: "unrelated-response".into(),
                    text: "not the Task result".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
    let unrelated = store.compaction_history_read_fence().await.unwrap();
    assert!(
        !store
            .compaction_history_causal_boundary("ws", "thread", "turn", &unrelated)
            .await
            .unwrap()
            .delivered_outcome
    );
    store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "run".into(),
                item: pioneer_protocol::TurnItem::SystemEvent {
                    id: pioneer_protocol::task_delivery_result_item_id("delivery"),
                    level: pioneer_protocol::SystemEventLevel::Error,
                    message: "acknowledged failed outcome".into(),
                    code: Some("unavailable".into()),
                    details: None,
                },
            },
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
    let completed = store.compaction_history_read_fence().await.unwrap();
    assert!(
        store
            .compaction_history_causal_boundary("ws", "thread", "turn", &completed)
            .await
            .unwrap()
            .delivered_outcome
    );
    assert!(
        !store
            .compaction_history_causal_boundary("ws", "thread", "turn", &unrelated)
            .await
            .unwrap()
            .delivered_outcome,
        "later delivery must not cross the accepted snapshot fence"
    );
    assert!(
        store
            .compaction_history_causal_boundary("other", "thread", "turn", &completed)
            .await
            .is_err()
    );
    let entries = store
        .compaction_source_page("ws", "thread", "run", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries;
    let result = entries
        .iter()
        .find(|entry| {
            entry
                .payload
                .as_deref()
                .is_some_and(|p| p.contains("acknowledged failed outcome"))
        })
        .unwrap()
        .reference
        .clone();
    assert_eq!(
        store
            .compaction_task_delivery_command("ws", "thread", &result)
            .await
            .unwrap()
            .as_deref(),
        Some("turn")
    );
    let unrelated_source = entries
        .iter()
        .find(|entry| {
            entry
                .payload
                .as_deref()
                .is_some_and(|p| p.contains("unrelated-response"))
        })
        .unwrap()
        .reference
        .clone();
    assert!(
        store
            .compaction_task_delivery_command("ws", "thread", &unrelated_source)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_task_delivery_command("other", "thread", &result)
            .await
            .unwrap()
            .is_none()
    );
    let mut stale_result = result.clone();
    stale_result.version = "event-revision:999".into();
    assert!(
        store
            .compaction_task_delivery_command("ws", "thread", &stale_result)
            .await
            .unwrap()
            .is_none()
    );
    let event: pioneer_crud::CanonicalTurnEventPayload = serde_json::from_str(
        entries
            .iter()
            .find(|entry| entry.reference == result)
            .unwrap()
            .payload
            .as_deref()
            .unwrap(),
    )
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_event_revision SET projection_revision=0 WHERE source_id=?",
        [result.id.clone().into()],
    ))
    .await
    .unwrap();
    assert!(
        store
            .compaction_task_delivery_command("ws", "thread", &result)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !store
            .compaction_record_event_projection("ws", "thread", &stale_result, &event)
            .await
            .unwrap()
    );
    assert!(
        store
            .compaction_record_event_projection("other", "thread", &result, &event)
            .await
            .is_err()
    );
    assert!(
        store
            .compaction_record_event_projection("ws", "thread", &result, &event)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .compaction_task_delivery_command("ws", "thread", &result)
            .await
            .unwrap()
            .as_deref(),
        Some("turn")
    );
    db.execute_unprepared("UPDATE task_delivery SET status='delivering' WHERE id='delivery'")
        .await
        .unwrap();
    assert!(
        !store
            .compaction_history_causal_boundary("ws", "thread", "turn", &completed)
            .await
            .unwrap()
            .delivered_outcome
    );
    assert!(
        store
            .compaction_task_delivery_command("ws", "thread", &result)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn task_occurrence_failures_close_command_only_after_canonical_acknowledgement() {
    for status in ["failed", "blocked", "cancelled"] {
        let store = store().await;
        let db = store.database_connection();
        for sql in [
            "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
            "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run','task','run',1,1,'running','agent')",
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('run','thread','in_progress','task_run','scheduled_task',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        ] {
            db.execute_unprepared(sql).await.unwrap();
        }
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE task_run SET status=? WHERE id='run'",
            [status.into()],
        ))
        .await
        .unwrap();
        let before = store.compaction_history_read_fence().await.unwrap();
        assert!(
            !store
                .compaction_history_causal_boundary("ws", "thread", "turn", &before)
                .await
                .unwrap()
                .delivered_outcome,
            "mutable {status} run without a canonical event is not closure"
        );
        assert_eq!(
            store
                .compare_and_materialize_task_run_occurrence_terminal(
                    "run",
                    chrono::Utc::now().timestamp()
                )
                .await
                .unwrap(),
            pioneer_crud::TaskRunOccurrenceTerminalizationOutcome::Changed
        );
        let after = store.compaction_history_read_fence().await.unwrap();
        assert!(
            store
                .compaction_history_causal_boundary("ws", "thread", "turn", &after)
                .await
                .unwrap()
                .delivered_outcome,
            "{status}"
        );
        assert!(
            !store
                .compaction_history_causal_boundary("ws", "thread", "turn", &before)
                .await
                .unwrap()
                .delivered_outcome
        );
        let entries = store
            .compaction_source_page("ws", "thread", "run", PagedSource::Event, 0)
            .await
            .unwrap()
            .entries;
        assert_eq!(entries.len(), 1, "terminal occurrence has no result item");
        let source = &entries[0].reference;
        assert_eq!(
            store
                .compaction_task_delivery_command("ws", "thread", source)
                .await
                .unwrap()
                .as_deref(),
            Some("turn")
        );
        assert!(
            store
                .compaction_task_delivery_command("other", "thread", source)
                .await
                .unwrap()
                .is_none()
        );
        let mut stale = source.clone();
        stale.version = "event-revision:999".into();
        assert!(
            store
                .compaction_task_delivery_command("ws", "thread", &stale)
                .await
                .unwrap()
                .is_none()
        );
        db.execute_unprepared("UPDATE turn SET turn_kind='conversation' WHERE id='run'")
            .await
            .unwrap();
        assert!(
            !store
                .compaction_history_causal_boundary("ws", "thread", "turn", &after)
                .await
                .unwrap()
                .delivered_outcome
        );
        assert!(
            store
                .compaction_task_delivery_command("ws", "thread", source)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn generic_failed_task_delivery_requires_exact_turn_identity_and_event_fence() {
    let store = store().await;
    let db = store.database_connection();
    for sql in [
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run','task','run',1,1,'failed','agent')",
        "INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,thread_target,target_thread_id,status,attempt_count,max_attempts) VALUES ('delivery','ws','task','run','key','thread','origin_thread','thread','delivered',1,1)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    let thread = store.get_thread_model("thread").await.unwrap().unwrap();
    let template = store.get_turn("thread", "turn").await.unwrap().unwrap().1;
    let before = store.compaction_history_read_fence().await.unwrap();
    for (id, expected) in [
        ("unrelated-failed-turn".to_owned(), false),
        (
            pioneer_crud::canonical_agent_id('T', "task-delivery-turn\0delivery"),
            true,
        ),
    ] {
        let mut started = template.clone();
        started.id = id.clone();
        started.status = pioneer_protocol::TurnStatus::InProgress;
        started.mode = pioneer_protocol::ThreadMode::Chat;
        started.origin = pioneer_protocol::TurnOrigin::TaskDelivery;
        started.author = None;
        let mut failed = started.clone();
        failed.status = pioneer_protocol::TurnStatus::Failed;
        failed.error = Some("fixture failure".into());
        store
            .materialize_failed_task_delivery_turn(pioneer_crud::FailedTaskDeliveryTurnWrite {
                thread: &thread,
                sandbox_mode: pioneer_protocol::SandboxMode::FullAccess,
                started_turn: &started,
                actor: pioneer_protocol::PersistedActorRef::System,
                audit_event: pioneer_protocol::TurnPermissionAuditEvent {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: id.clone(),
                    event_kind: pioneer_protocol::TurnPermissionAuditEventKind::ProfileSelected,
                    profile_mode: pioneer_protocol::TurnPermissionMode::FullAccess,
                    profile_source: pioneer_protocol::TurnPermissionProfileSource::System,
                    security_snapshot_id: None,
                    security_snapshot_version: None,
                    security_reason_code: None,
                    security_capability: None,
                    item_id: None,
                    tool_call_id: None,
                    tool_name: None,
                    action_kind: None,
                    request_key: None,
                    decision: None,
                    reason: None,
                    cached: false,
                },
                failed: pioneer_protocol::TurnFailedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn: failed,
                },
                agent_action: None,
            })
            .await
            .unwrap();
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE task_delivery SET delivered_turn_id=? WHERE id='delivery'",
            [id.clone().into()],
        ))
        .await
        .unwrap();
        let after = store.compaction_history_read_fence().await.unwrap();
        assert_eq!(
            store
                .compaction_history_causal_boundary("ws", "thread", "turn", &after)
                .await
                .unwrap()
                .delivered_outcome,
            expected
        );
        assert!(
            !store
                .compaction_history_causal_boundary("ws", "thread", "turn", &before)
                .await
                .unwrap()
                .delivered_outcome
        );
        let entries = store
            .compaction_source_page("ws", "thread", &id, PagedSource::Event, 0)
            .await
            .unwrap()
            .entries;
        for entry in entries {
            let is_failure = entry
                .payload
                .as_deref()
                .is_some_and(|p| p.contains("fixture failure"));
            assert_eq!(
                store
                    .compaction_task_delivery_command("ws", "thread", &entry.reference)
                    .await
                    .unwrap()
                    .as_deref(),
                (expected && is_failure).then_some("turn")
            );
            assert!(
                store
                    .compaction_task_delivery_command("other", "thread", &entry.reference)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }
}

#[tokio::test]
async fn task_basis_scope_requires_exact_execution_snapshot_and_lineage() {
    let store = store().await;
    let db = store.database_connection();
    for sql in [
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('child','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run','task','run',1,1,'running','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('rt','task','run','child','child-turn','initial',0,1,'in_progress',CURRENT_TIMESTAMP)",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('child','thread','thread',1,CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    assert_eq!(
        store
            .compaction_task_basis_thread("ws", "child", "child-turn")
            .await
            .unwrap(),
        None
    );
    db.execute_unprepared("WITH frozen_root_fixture(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) AS (VALUES ('run','task','ws','thread','not read by metadata lookup',CURRENT_TIMESTAMP)) INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,frozen_manifest_id) SELECT run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,CASE WHEN json_valid(history_json) THEN CASE WHEN json_type(history_json)='object' THEN json_extract(history_json,'$.manifest_id') ELSE NULL END ELSE NULL END FROM frozen_root_fixture").await.unwrap();
    assert_eq!(
        store
            .compaction_task_basis_thread("ws", "child", "child-turn")
            .await
            .unwrap()
            .as_deref(),
        Some("thread")
    );
    let before_input = store.compaction_history_read_fence().await.unwrap();
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('child-turn','child','in_progress','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('child-input','child-turn',0,'text','own','{}',CURRENT_TIMESTAMP)").await.unwrap();
    let after_input = store.compaction_history_read_fence().await.unwrap();
    assert!(
        store
            .compaction_latest_task_basis_turn("ws", "child", &before_input)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .compaction_latest_task_basis_turn("ws", "child", &after_input)
            .await
            .unwrap()
            .as_deref(),
        Some("child-turn")
    );
    assert!(
        store
            .compaction_latest_task_basis_turn("other", "child", &after_input)
            .await
            .unwrap()
            .is_none()
    );
    let basis = store
        .compaction_task_basis_snapshot("ws", "child", "child-turn")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(basis.parent_thread, "thread");
    assert_eq!(basis.run_id, "run");
    assert_eq!(basis.history_json, "not read by metadata lookup");
    // A UTF-8 scalar crosses the 256KiB fragment boundary; decode only after
    // releasing the reader and joining complete bounded byte fragments.
    let large = format!(
        "{}🦀{}",
        "x".repeat(SOURCE_PAGE_BYTES - 1),
        "история".repeat(50_000)
    );
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "WITH root_replacement(history_json) AS (VALUES (?)) UPDATE task_run_conversation_snapshot SET history_json=(SELECT history_json FROM root_replacement),frozen_manifest_id=(SELECT CASE WHEN json_valid(history_json) THEN CASE WHEN json_type(history_json)='object' THEN json_extract(history_json,'$.manifest_id') ELSE NULL END ELSE NULL END FROM root_replacement) WHERE run_id='run'",
        [large.clone().into()],
    ))
    .await
    .unwrap();
    assert_eq!(
        store
            .compaction_task_basis_snapshot("ws", "child", "child-turn")
            .await
            .unwrap()
            .unwrap()
            .history_json,
        large
    );
    for (workspace, thread, turn) in [
        ("other", "child", "child-turn"),
        ("ws", "thread", "child-turn"),
        ("ws", "child", "other-turn"),
    ] {
        assert!(
            store
                .compaction_task_basis_snapshot(workspace, thread, turn)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .compaction_task_basis_thread(workspace, thread, turn)
                .await
                .unwrap(),
            None
        );
    }
    db.execute_unprepared(
        "UPDATE thread_lineage SET parent_thread_id='child' WHERE child_thread_id='child'",
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .compaction_task_basis_thread("ws", "child", "child-turn")
            .await
            .unwrap(),
        None
    );
    db.execute_unprepared(
        "UPDATE thread_lineage SET parent_thread_id='thread' WHERE child_thread_id='child'",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE task_run_conversation_snapshot SET task_id='unrelated' WHERE run_id='run'",
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .compaction_task_basis_thread("ws", "child", "child-turn")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn task_output_snapshot_is_bound_to_completed_turn_and_never_recaptured() {
    let store = store().await;
    let db = store.database_connection();
    for sql in [
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run','task','run',1,1,'running','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('rt','task','run','thread','turn','initial',0,1,'in_progress',CURRENT_TIMESTAMP)",
        "UPDATE turn SET status='in_progress' WHERE id='turn'",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    assert_eq!(
        store.get_task_run_turn("rt").await.unwrap().unwrap().kind,
        pioneer_protocol::TaskRunTurnKind::Initial
    );
    let history = pioneer_compaction::frozen::FrozenHistoryRef {
        format: 1,
        manifest_id: "output-history".into(),
        messages: 0,
        identity_sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
    };
    store
        .compaction_begin_frozen_history("ws", "thread", &history)
        .await
        .unwrap();
    store
        .compaction_finish_frozen_history("ws", "thread", &history)
        .await
        .unwrap();
    assert!(
        store
            .compaction_record_task_output("ws", "rt", &history)
            .await
            .is_err()
    );
    db.execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_record_task_output("other", "rt", &history)
            .await
            .is_err()
    );
    let output = store
        .compaction_record_task_output("ws", "rt", &history)
        .await
        .unwrap();
    assert_eq!(output.task_id, "task");
    assert_eq!(output.run_id, "run");
    assert_eq!(output.source_thread, "thread");
    assert_eq!(output.source_turn, "turn");
    assert_eq!(output.history, history);
    let other = pioneer_compaction::frozen::FrozenHistoryRef {
        manifest_id: "later-history".into(),
        ..history.clone()
    };
    store
        .compaction_begin_frozen_history("ws", "thread", &other)
        .await
        .unwrap();
    store
        .compaction_finish_frozen_history("ws", "thread", &other)
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_record_task_output("ws", "rt", &other)
            .await
            .unwrap(),
        output
    );
    assert!(
        store
            .compaction_task_output("other", "rt")
            .await
            .unwrap()
            .is_none()
    );
    db.execute_unprepared("UPDATE task_run_turn SET turn_id='other-turn' WHERE id='rt'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_task_output("ws", "rt")
            .await
            .unwrap()
            .is_none()
    );
}

// Exact canonical delivery/output fixture; no mutable Task result payload is
// needed for metadata discovery. Every output has a completed TaskRunTurn and
// a ready frozen manifest, as in the production queue binding.
async fn delivered_output_fixture(store: &CrudStore, delivery: &str, delivery_turn: &str) {
    let db = store.database_connection();
    let task = format!("task-{delivery}");
    let run = format!("run-{delivery}");
    let run_turn = format!("rt-{delivery}");
    let source_turn = format!("output-turn-{delivery}");
    for (sql, values) in [
        (
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES (?,'thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            vec![source_turn.clone().into()],
        ),
        (
            "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES (?,'ws','thread','thread','thread','turn','agent','completed','Task','fixture')",
            vec![task.clone().into()],
        ),
        (
            "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES (?,?,?,1,1,'succeeded','agent')",
            vec![run.clone().into(), task.clone().into(), run.clone().into()],
        ),
        (
            "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES (?,?,?,'thread',?,'initial',0,1,'completed',CURRENT_TIMESTAMP)",
            vec![
                run_turn.clone().into(),
                task.clone().into(),
                run.clone().into(),
                source_turn.clone().into(),
            ],
        ),
        (
            "INSERT INTO task_result_candidate(id,task_id,run_id,task_run_turn_id,thread_id,turn_id,round,status,created_at,updated_at) VALUES (?,?,?,?,'thread',?,0,'accepted',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            vec![
                delivery.into(),
                task.clone().into(),
                run.clone().into(),
                run_turn.clone().into(),
                source_turn.clone().into(),
            ],
        ),
        (
            "INSERT OR IGNORE INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES (?,'thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            vec![delivery_turn.into()],
        ),
        (
            "INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,thread_target,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id) VALUES (?,'ws',?,?,?,'thread','origin_thread','thread','delivered',1,1,?)",
            vec![
                delivery.into(),
                task.into(),
                run.into(),
                delivery.into(),
                delivery_turn.into(),
            ],
        ),
    ] {
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            sql,
            values,
        ))
        .await
        .unwrap();
    }
    let history = pioneer_compaction::frozen::FrozenHistoryRef {
        format: 1,
        manifest_id: format!("output-{delivery}"),
        messages: 0,
        identity_sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
    };
    store
        .compaction_begin_frozen_history("ws", "thread", &history)
        .await
        .unwrap();
    assert!(
        store
            .compaction_finish_frozen_history("ws", "thread", &history)
            .await
            .unwrap()
    );
    store
        .compaction_record_task_output("ws", &run_turn, &history)
        .await
        .unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO compaction_delivery_output(delivery_id,candidate_id,task_run_turn_id) VALUES (?,?,?)",
        [delivery.into(),delivery.into(),run_turn.into()],
    )).await.unwrap();
}

async fn delivered_ack(store: &CrudStore, delivery: &str, turn: &str) {
    store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: turn.into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: pioneer_protocol::task_delivery_result_item_id(delivery),
                    text: format!("result-{delivery}"),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
}

async fn repeat_delivered_ack(
    store: &CrudStore,
    delivery: &str,
    turn: &str,
    id: &str,
    sequence: i64,
) {
    store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) SELECT ?,e.thread_id,e.turn_id,?,e.event_type,e.payload,CURRENT_TIMESTAMP FROM turn_event e JOIN compaction_event_revision r ON r.source_id=e.id WHERE e.turn_id=? AND r.item_id=? ORDER BY r.capture_order LIMIT 1",
        [id.into(),sequence.into(),turn.into(),pioneer_protocol::task_delivery_result_item_id(delivery).into()],
    )).await.unwrap();
    let source = SourceRef {
        scope: format!("event:{turn}"),
        id: id.into(),
        version: "event-revision:1".into(),
    };
    let payload = store
        .compaction_reference_payload("ws", "thread", &source)
        .await
        .unwrap()
        .unwrap();
    let event = serde_json::from_str(&payload).unwrap();
    assert!(
        store
            .compaction_record_event_projection("ws", "thread", &source, &event)
            .await
            .unwrap()
    );
}

async fn output_metadata_padding(store: &CrudStore, turn: &str, tag: &str, start: i64, count: i64) {
    for sequence in start..start + count {
        store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES (?,'thread',?,?,'fixture','{}',CURRENT_TIMESTAMP)",
            [format!("{tag}-{sequence}").into(),turn.into(),sequence.into()],
        )).await.unwrap();
    }
}

async fn compress_output_events(store: &CrudStore) {
    store.database_connection().query_one_write_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "SELECT zstd_enable_transparent(?)",
        [serde_json::json!({"table":"turn_event","column":"payload","compression_level":3,"dict_chooser":"'[nodict]'"}).to_string().into()],
    )).await.unwrap();
}

async fn refresh_output_page(store: &CrudStore, page: &DeliveredTaskOutputPage) {
    for source in &page.unprojected_events {
        let payload = store
            .compaction_reference_payload("ws", "thread", source)
            .await
            .unwrap()
            .unwrap();
        let event = serde_json::from_str(&payload).unwrap();
        assert!(
            store
                .compaction_record_event_projection("ws", "thread", source, &event)
                .await
                .unwrap()
        );
    }
}

async fn foreign_output_history(store: &CrudStore) {
    let db = store.database_connection();
    for sql in [
        "INSERT OR IGNORE INTO workspace(id,name,is_active,is_current) VALUES ('foreign-ws','foreign',1,0)",
        "INSERT OR IGNORE INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('foreign-thread','foreign-ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT OR IGNORE INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('foreign-turn','foreign-thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<4096) INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) SELECT 'foreign-'||x,'foreign-thread','foreign-turn',x,'item/completed','{}',CURRENT_TIMESTAMP FROM n",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
}

async fn discover_output_metadata(
    store: &CrudStore,
    fence: &HistoryReadFence,
) -> (Vec<DeliveredTaskOutputRef>, usize, usize) {
    let mut cursor = DeliveredTaskOutputCursor::default();
    let mut entries = Vec::new();
    let mut pages = 0;
    let mut event_rows = 0;
    loop {
        let page = store
            .compaction_delivered_output_page("ws", "thread", &cursor, fence)
            .await
            .unwrap();
        assert!(page.selected_turn_rows + page.selected_event_rows <= 128);
        assert!(page.unprojected_events.is_empty());
        pages += 1;
        assert!(pages <= 20, "local keyset cursor must terminate");
        event_rows += page.selected_event_rows;
        entries.extend(page.entries);
        if page.done {
            break;
        }
        assert_ne!(page.next_cursor, cursor);
        cursor = page.next_cursor;
    }
    entries.sort_by_key(|entry| entry.capture_order);
    (entries, pages, event_rows)
}

// Inspect the actual bound repository statements. Returned row counts alone
// cannot detect a hidden global revision/delivery scan inside a SQL plan.
async fn assert_output_discovery_plans(store: &CrudStore, statements: &RecordedStatements) {
    let statements = statements.lock().unwrap().clone();
    let discovery = statements
        .into_iter()
        .filter(|statement| {
            statement.sql.starts_with("SELECT d.delivered_turn_id")
                || statement.sql.contains("WITH quantum AS MATERIALIZED")
        })
        .collect::<Vec<_>>();
    assert!(!discovery.is_empty());
    for mut statement in discovery {
        let event_page = statement.sql.contains("WITH quantum AS MATERIALIZED");
        let resumed_events = statement.sql.contains("AND e.sequence>?");
        let exact_recheck = statement.sql.contains("FROM json_each(?) w");
        assert!(!statement.sql.contains("SELECT *"));
        assert!(!statement.sql.contains("compaction_event_revision WHERE"));
        assert!(!statement.sql.contains("rowid"));
        assert!(!statement.sql.contains("payload"));
        statement.sql = format!("EXPLAIN QUERY PLAN {}", statement.sql);
        let plan = store
            .database_connection()
            .query_all_raw(statement)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get::<String>("", "detail").unwrap())
            .collect::<Vec<_>>();
        if event_page {
            if exact_recheck {
                assert!(
                    plan.iter().any(|detail| detail.starts_with("SEARCH ")
                        && (detail.starts_with("SEARCH e ") || detail.contains("turn_event"))
                        && detail.contains("(id=?)")),
                    "{plan:#?}"
                );
            } else {
                assert!(
                    plan.iter().any(|detail| detail.starts_with("SEARCH ")
                        && detail.contains("idx_turn_events_turn_id_sequence")
                        && detail.contains("(turn_id=?")
                        && detail.contains("sequence<?")
                        && (!resumed_events || detail.contains("sequence>?"))),
                    "{plan:#?}"
                );
            }
            assert!(
                plan.iter()
                    .any(|detail| detail.starts_with("SEARCH r ")
                        && detail.contains("(source_id=?)")),
                "{plan:#?}"
            );
            assert!(
                plan.iter()
                    .any(|detail| detail.starts_with("SEARCH d ") && detail.contains("(id=?)")),
                "{plan:#?}"
            );
            assert!(
                !plan.iter().any(|detail| detail.starts_with("SCAN ")
                    && (detail.contains("turn_event")
                        || detail.starts_with("SCAN e ")
                        || detail.starts_with("SCAN r ")
                        || detail.starts_with("SCAN d "))),
                "{plan:#?}"
            );
        } else {
            assert!(
                plan.iter().any(|detail| detail
                    .contains("SEARCH d USING INDEX compaction_delivery_turn")
                    && detail
                        .contains("workspace_id=? AND target_thread_id=? AND delivered_turn_id>?")
                    && detail.contains("delivered_turn_id<?")),
                "{plan:#?}"
            );
            assert!(
                !plan.iter().any(|detail| detail.contains("SCAN d ")),
                "{plan:#?}"
            );
            assert!(
                plan.iter().any(
                    |detail| detail.starts_with("SEARCH ct ") && detail.contains("(turn_id=?)")
                ),
                "Turn fence uses the existing unique locator: {plan:#?}"
            );
            assert!(
                plan.iter().any(|detail| detail.starts_with("SEARCH ")
                    && detail.contains("idx_turn_events_turn_id_sequence")
                    && detail.contains("(turn_id=?)")),
                "sequence endpoint uses the local index: {plan:#?}"
            );
        }
        assert!(
            !plan
                .iter()
                .any(|detail| detail.contains("compaction_event_revision_capture_order")),
            "{plan:#?}"
        );
    }
}

#[tokio::test]
async fn delivered_output_discovery_without_deliveries_ignores_foreign_history() {
    let statements = RecordedStatements::default();
    let store = store_recording_statements(Some(statements.clone())).await;
    // Include same-workspace ordinary events as well: absent deliveries require
    // no event/revision query, irrespective of any history high-water mark.
    for n in 1..=260 {
        source(&store, &format!("irrelevant-{n}"), n, "{}").await;
    }
    foreign_output_history(&store).await;
    let fence = store.compaction_history_read_fence().await.unwrap();
    statements.lock().unwrap().clear();
    let page = store
        .compaction_delivered_output_page(
            "ws",
            "thread",
            &DeliveredTaskOutputCursor::default(),
            &fence,
        )
        .await
        .unwrap();
    assert!(page.done);
    assert!(page.entries.is_empty() && page.unprojected_events.is_empty());
    assert_eq!(page.selected_turn_rows, 0);
    assert_eq!(page.selected_event_rows, 0);
    assert_eq!(statements.lock().unwrap().len(), 1);
    assert_output_discovery_plans(&store, &statements).await;
    source(&store, "late", 261, "{}").await;
    assert_eq!(
        store
            .compaction_delivered_output_page(
                "ws",
                "thread",
                &DeliveredTaskOutputCursor::default(),
                &fence
            )
            .await
            .unwrap(),
        page
    );
}

#[tokio::test]
async fn delivered_output_local_keysets_preserve_sparse_outputs_and_repeated_acks() {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    for compressed in [false, true] {
        let statements = RecordedStatements::default();
        let store = store_recording_statements(Some(statements.clone())).await;
        if compressed {
            store.database_connection().query_one_write_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "SELECT zstd_enable_transparent(?)", [serde_json::json!({"table":"turn_event","column":"payload","compression_level":3,"dict_chooser":"'[nodict]'"}).to_string().into()],
            )).await.unwrap();
        }
        for (delivery, turn) in [
            ("first", "z-turn"),
            ("second", "a-turn"),
            ("shared", "a-turn"),
            ("third", "m-turn"),
        ] {
            delivered_output_fixture(&store, delivery, turn).await;
        }
        let mut canonical_outputs = std::collections::BTreeSet::new();
        for delivery in ["first", "second", "shared", "third"] {
            let snapshot = store
                .compaction_delivery_output("ws", delivery)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                snapshot.output.source_turn,
                format!("output-turn-{delivery}")
            );
            assert_eq!(snapshot.output.task_run_turn_id, format!("rt-{delivery}"));
            assert_eq!(snapshot.candidate_id, delivery);
            assert_eq!(
                snapshot.output.history.manifest_id,
                format!("output-{delivery}")
            );
            assert!(canonical_outputs.insert(snapshot.output.source_turn));
        }
        assert_eq!(canonical_outputs.len(), 4);
        let before = store.compaction_history_read_fence().await.unwrap();
        delivered_ack(&store, "first", "z-turn").await;
        foreign_output_history(&store).await;
        delivered_ack(&store, "second", "a-turn").await;
        delivered_ack(&store, "shared", "a-turn").await;
        delivered_ack(&store, "third", "m-turn").await;
        // Put a replay on a different local page; sequence order deliberately
        // disagrees with capture order so first-encountered is invalid.
        for n in 2..=130 {
            store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES (?,'thread','z-turn',?,'fixture','{}',CURRENT_TIMESTAMP)",
                [format!("padding-{n}").into(),n.into()],
            )).await.unwrap();
        }
        repeat_delivered_ack(&store, "first", "z-turn", "repeated-first", -1).await;
        repeat_delivered_ack(&store, "first", "z-turn", "repeated-first-late", 131).await;
        let fence = store.compaction_history_read_fence().await.unwrap();
        statements.lock().unwrap().clear();
        let (entries, pages, rows) = discover_output_metadata(&store, &fence).await;
        assert_eq!(pages, 5); // a, m, two z pages, local exhaustion
        assert_eq!(rows, 135);
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.delivery_id.as_str())
                .collect::<Vec<_>>(),
            ["first", "second", "shared", "third", "first", "first"]
        );
        assert!(
            entries
                .windows(2)
                .all(|pair| pair[0].capture_order < pair[1].capture_order)
        );
        assert_eq!(entries[0].acknowledgement.version, "event-revision:1");
        assert_eq!(entries[4].acknowledgement.id, "repeated-first");
        assert_ne!(entries[0].acknowledgement.id, "repeated-first");
        assert_output_discovery_plans(&store, &statements).await;
        store.database_connection().execute_unprepared(
            "WITH RECURSIVE n(x) AS (VALUES(4097) UNION ALL SELECT x+1 FROM n WHERE x<8192) INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) SELECT 'foreign-'||x,'foreign-thread','foreign-turn',x,'item/completed','{}',CURRENT_TIMESTAMP FROM n",
        ).await.unwrap();
        let larger_fence = store.compaction_history_read_fence().await.unwrap();
        statements.lock().unwrap().clear();
        let (same, same_pages, same_rows) = discover_output_metadata(&store, &larger_fence).await;
        assert_eq!(same, entries);
        assert_eq!((same_pages, same_rows), (pages, rows));
        // Three local Turn seeks plus exhaustion, and four event pages.
        assert_eq!(statements.lock().unwrap().len(), 8);

        // Append during an existing fenced request. Old discovery neither
        // includes the new acknowledgement nor traverses foreign revisions.
        let first_page = store
            .compaction_delivered_output_page(
                "ws",
                "thread",
                &DeliveredTaskOutputCursor::default(),
                &fence,
            )
            .await
            .unwrap();
        assert!(!first_page.done);
        repeat_delivered_ack(&store, "third", "m-turn", "late-third", 2).await;
        let mut during_append = first_page.entries;
        let mut cursor = first_page.next_cursor;
        for n in 0..20 {
            let page = store
                .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
                .await
                .unwrap();
            assert!(page.unprojected_events.is_empty());
            during_append.extend(page.entries);
            if page.done {
                break;
            }
            assert!(n < 19, "append must not prevent local exhaustion");
            assert_ne!(page.next_cursor, cursor);
            cursor = page.next_cursor;
        }
        during_append.sort_by_key(|entry| entry.capture_order);
        assert_eq!(during_append, entries);
        assert_eq!(discover_output_metadata(&store, &fence).await.0, entries);
        assert!(discover_output_metadata(&store, &before).await.0.is_empty());
        let after = store.compaction_history_read_fence().await.unwrap();
        assert_eq!(discover_output_metadata(&store, &after).await.0.len(), 7);
        for (workspace, thread) in [("foreign-ws", "thread"), ("ws", "foreign-thread")] {
            let page = store
                .compaction_delivered_output_page(
                    workspace,
                    thread,
                    &DeliveredTaskOutputCursor::default(),
                    &after,
                )
                .await
                .unwrap();
            assert!(page.done && page.entries.is_empty());
            assert_eq!(page.selected_event_rows, 0);
        }
        assert_eq!(
            store.database_connection().read_class(),
            pioneer_sqlite::SqliteReadClass::Maintenance
        );
        assert_eq!(
            store.database_connection().write_class(),
            pioneer_sqlite::SqliteWriteClass::Maintenance
        );
    }
}

#[tokio::test]
async fn delivered_output_exact_full_page_exhausts_without_a_revision_range_seek() {
    let store = store().await;
    delivered_output_fixture(&store, "delivery", "delivery-turn").await;
    delivered_ack(&store, "delivery", "delivery-turn").await;
    for sequence in 2..=127 {
        store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES (?,'thread','delivery-turn',?,'fixture','{}',CURRENT_TIMESTAMP)",
            [format!("exact-padding-{sequence}").into(),sequence.into()],
        )).await.unwrap();
    }
    let fence = store.compaction_history_read_fence().await.unwrap();
    let first = store
        .compaction_delivered_output_page(
            "ws",
            "thread",
            &DeliveredTaskOutputCursor::default(),
            &fence,
        )
        .await
        .unwrap();
    assert_eq!(first.selected_event_rows, 127);
    assert_eq!(first.entries.len(), 1);
    assert!(!first.done);
    assert_eq!(first.next_cursor.after_sequence, Some(127));
    let empty_tail = store
        .compaction_delivered_output_page("ws", "thread", &first.next_cursor, &fence)
        .await
        .unwrap();
    assert_eq!(
        empty_tail.selected_turn_rows + empty_tail.selected_event_rows,
        0
    );
    assert!(!empty_tail.done);
    assert_ne!(empty_tail.next_cursor, first.next_cursor);
    assert_eq!(empty_tail.next_cursor.after_turn, "delivery-turn");
    let last = store
        .compaction_delivered_output_page("ws", "thread", &empty_tail.next_cursor, &fence)
        .await
        .unwrap();
    assert!(last.done && last.entries.is_empty());
    assert_eq!(last.selected_turn_rows + last.selected_event_rows, 0);
}

#[tokio::test]
async fn delivered_output_cold_metadata_uses_scoped_revision_cas_and_source_freshness() {
    let store = store().await;
    delivered_output_fixture(&store, "delivery", "delivery-turn").await;
    delivered_ack(&store, "delivery", "delivery-turn").await;
    let fence = store.compaction_history_read_fence().await.unwrap();
    let cursor = DeliveredTaskOutputCursor::default();
    let warm = store
        .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
        .await
        .unwrap();
    assert_eq!(warm.entries.len(), 1);
    let acknowledgement = &warm.entries[0].acknowledgement;
    let payload = store
        .compaction_reference_payload("ws", "thread", acknowledgement)
        .await
        .unwrap()
        .unwrap();
    let event: pioneer_crud::CanonicalTurnEventPayload = serde_json::from_str(&payload).unwrap();
    for projection in ["NULL", "revision-1"] {
        store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            format!("UPDATE compaction_event_revision SET projection_revision={projection},item_id=NULL,projection_kind=NULL WHERE source_id=?"),
            [acknowledgement.id.clone().into()],
        )).await.unwrap();
        let cold = store
            .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
            .await
            .unwrap();
        assert!(cold.entries.is_empty());
        assert_eq!(cold.unprojected_events, vec![acknowledgement.clone()]);
        assert!(
            store
                .compaction_reference_payload("foreign-ws", "thread", acknowledgement)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .compaction_record_event_projection("ws", "thread", acknowledgement, &event)
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
                .await
                .unwrap(),
            warm
        );
    }
    foreign_output_history(&store).await;
    store
        .database_connection()
        .execute_unprepared(
            "UPDATE task_delivery SET delivered_turn_id='foreign-turn' WHERE id='delivery'",
        )
        .await
        .unwrap();
    let foreign_locator = store
        .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
        .await
        .unwrap();
    assert!(foreign_locator.done && foreign_locator.entries.is_empty());
    assert_eq!(foreign_locator.selected_event_rows, 0);
    store
        .database_connection()
        .execute_unprepared(
            "UPDATE task_delivery SET delivered_turn_id='delivery-turn' WHERE id='delivery'",
        )
        .await
        .unwrap();
    let epoch = store
        .compaction_projection_version("ws", "thread")
        .await
        .unwrap();
    store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE turn_event SET payload=payload||' ' WHERE id=?",
            [acknowledgement.id.clone().into()],
        ))
        .await
        .unwrap();
    assert_ne!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        epoch
    );
    assert!(
        !store
            .compaction_record_event_projection("ws", "thread", acknowledgement, &event)
            .await
            .unwrap()
    );
    assert!(
        store
            .compaction_reference_payload("ws", "thread", acknowledgement)
            .await
            .unwrap()
            .is_none()
    );
    let stale = store
        .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
        .await
        .unwrap();
    assert!(stale.entries.is_empty());
    assert_eq!(stale.unprojected_events.len(), 1);
    assert_eq!(stale.unprojected_events[0].version, "event-revision:2");
    store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "DELETE FROM turn_event WHERE id=?",
            [acknowledgement.id.clone().into()],
        ))
        .await
        .unwrap();
    assert!(
        !store
            .compaction_record_event_projection("ws", "thread", acknowledgement, &event)
            .await
            .unwrap()
    );
    let deleted = store
        .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
        .await
        .unwrap();
    assert!(deleted.entries.is_empty() && deleted.unprojected_events.is_empty());
    assert_eq!(deleted.selected_event_rows, 0);
}

#[tokio::test]
async fn delivered_output_fence_terminates_despite_continuous_append_and_new_turns() {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    for compressed in [false, true] {
        let statements = RecordedStatements::default();
        let store = store_recording_statements(Some(statements.clone())).await;
        if compressed {
            compress_output_events(&store).await;
        }
        delivered_output_fixture(&store, "first", "a-old").await;
        delivered_output_fixture(&store, "second", "z-old").await;
        delivered_ack(&store, "first", "a-old").await;
        delivered_ack(&store, "second", "z-old").await;
        // A valid old acknowledgement can have a sequence far above the
        // global capture order. An event_order-as-sequence cutoff would lose it.
        let mut second_source = store
            .compaction_source_metadata_page("ws", "thread", "z-old", PagedSource::Event, 0)
            .await
            .unwrap()
            .entries[0]
            .reference
            .clone();
        store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE turn_event SET sequence=10000 WHERE id=?",
                [second_source.id.clone().into()],
            ))
            .await
            .unwrap();
        second_source.version = "event-revision:2".into();
        let payload = store
            .compaction_reference_payload("ws", "thread", &second_source)
            .await
            .unwrap()
            .unwrap();
        let event = serde_json::from_str(&payload).unwrap();
        assert!(
            store
                .compaction_record_event_projection("ws", "thread", &second_source, &event)
                .await
                .unwrap()
        );
        output_metadata_padding(&store, "a-old", "before", 2, 259).await;
        let fence = store.compaction_history_read_fence().await.unwrap();
        assert!(fence.event_order < 10000);
        let (expected, expected_pages, expected_rows) =
            discover_output_metadata(&store, &fence).await;
        assert_eq!((expected_pages, expected_rows), (5, 261));
        let mut cursor = DeliveredTaskOutputCursor::default();
        let mut entries = Vec::new();
        let mut sources = std::collections::BTreeSet::new();
        let mut rows = 0;
        let mut pages = 0_usize;
        loop {
            // A producer appends a full quantum before EVERY continuation,
            // including the would-be terminating request. New Turn IDs are
            // inside the fence's lexical range, so creation_order is essential.
            if pages > 0 {
                if pages == 1 {
                    // Late rows BEFORE an unvisited old acknowledgement must
                    // not fill its quantum or move the admitted continuation.
                    output_metadata_padding(&store, "z-old", "late-interleaved", 1, 127).await;
                }
                output_metadata_padding(
                    &store,
                    "a-old",
                    &format!("late-{pages}"),
                    261 + (pages as i64 - 1) * 127,
                    127,
                )
                .await;
                repeat_delivered_ack(
                    &store,
                    "first",
                    "a-old",
                    &format!("late-low-{pages}"),
                    -(pages as i64),
                )
                .await;
                let delivery = format!("late-delivery-{pages}");
                let turn = format!("b-new-{pages:02}");
                delivered_output_fixture(&store, &delivery, &turn).await;
                delivered_ack(&store, &delivery, &turn).await;
            }
            statements.lock().unwrap().clear();
            let page = store
                .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
                .await
                .unwrap();
            assert!(page.selected_turn_rows + page.selected_event_rows <= 128);
            for selected in &page.selected_events {
                assert!(selected.capture_order <= fence.event_order);
                assert!(
                    sources.insert(selected.source.clone()),
                    "no source is selected twice"
                );
            }
            rows += page.selected_event_rows;
            entries.extend(page.entries);
            pages += 1;
            assert!(
                pages <= expected_pages,
                "post-fence appends must not extend discovery"
            );
            assert_output_discovery_plans(&store, &statements).await;
            if page.done {
                break;
            }
            if let Some(upper) = cursor.event_high_water {
                if page.next_cursor.active_turn == cursor.active_turn {
                    assert_eq!(page.next_cursor.event_high_water, Some(upper));
                }
            }
            assert_ne!(page.next_cursor, cursor);
            cursor = page.next_cursor;
        }
        entries.sort_by_key(|entry| entry.capture_order);
        assert_eq!(entries, expected);
        assert_eq!((pages, rows), (expected_pages, expected_rows));
        assert_eq!(sources.len(), expected_rows);
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.delivery_id.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
    }
}

#[tokio::test]
async fn delivered_output_cold_page_refresh_keeps_exact_sources_across_append_boundaries() {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    for compressed in [false, true] {
        for count in [126, 127, 128] {
            let statements = RecordedStatements::default();
            let store = store_recording_statements(Some(statements.clone())).await;
            if compressed {
                compress_output_events(&store).await;
            }
            delivered_output_fixture(&store, "delivery", "delivery-turn").await;
            delivered_ack(&store, "delivery", "delivery-turn").await;
            output_metadata_padding(&store, "delivery-turn", "before", 2, count - 1).await;
            let fence = store.compaction_history_read_fence().await.unwrap();
            let warm = store
                .compaction_delivered_output_page(
                    "ws",
                    "thread",
                    &DeliveredTaskOutputCursor::default(),
                    &fence,
                )
                .await
                .unwrap();
            let ack = warm.entries[0].acknowledgement.clone();
            store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "UPDATE compaction_event_revision SET projection_revision=NULL,item_id=NULL,projection_kind=NULL WHERE source_id=?", [ack.id.clone().into()],
            )).await.unwrap();
            let cold = store
                .compaction_delivered_output_page(
                    "ws",
                    "thread",
                    &DeliveredTaskOutputCursor::default(),
                    &fence,
                )
                .await
                .unwrap();
            assert_eq!(cold.selected_event_rows, std::cmp::min(count, 127) as usize);
            assert_eq!(cold.selected_events, warm.selected_events);
            assert_eq!(cold.unprojected_events, [ack.clone()]);
            // Append between selection and CAS, then again between CAS and
            // exact recheck. Both high and negative sequences are after fence.
            output_metadata_padding(&store, "delivery-turn", "after-selection", count + 1, 127)
                .await;
            // Use the known canonical source key: its item cache is deliberately
            // NULL until CAS, so the replay fixture cannot locate it via cache.
            store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) SELECT 'negative-late',thread_id,turn_id,-1,event_type,payload,CURRENT_TIMESTAMP FROM turn_event WHERE id=?",
                [ack.id.clone().into()],
            )).await.unwrap();
            refresh_output_page(&store, &cold).await;
            output_metadata_padding(&store, "delivery-turn", "after-cas", count + 128, 127).await;
            delivered_output_fixture(&store, "late", "late-turn").await;
            delivered_ack(&store, "late", "late-turn").await;
            statements.lock().unwrap().clear();
            let refreshed = store
                .compaction_recheck_delivered_output_page("ws", "thread", &cold, &fence)
                .await
                .unwrap();
            assert_eq!(refreshed, warm);
            assert_eq!(
                statements.lock().unwrap().len(),
                1,
                "refresh uses one exact-key query"
            );
            assert_output_discovery_plans(&store, &statements).await;
            assert!(
                store
                    .compaction_recheck_delivered_output_page("foreign-ws", "thread", &cold, &fence)
                    .await
                    .is_err()
            );
            let mut selected = refreshed
                .selected_events
                .into_iter()
                .map(|event| event.source)
                .collect::<std::collections::BTreeSet<_>>();
            let mut entries = refreshed.entries;
            let mut cursor = refreshed.next_cursor;
            loop {
                let tail = store
                    .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
                    .await
                    .unwrap();
                assert!(tail.selected_turn_rows + tail.selected_event_rows <= 128);
                for event in tail.selected_events {
                    assert!(selected.insert(event.source));
                }
                entries.extend(tail.entries);
                if tail.done {
                    break;
                }
                assert_ne!(tail.next_cursor, cursor);
                cursor = tail.next_cursor;
            }
            assert_eq!(selected.len(), count as usize);
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].acknowledgement, ack);
        }
    }
}

#[tokio::test]
async fn delivered_output_exact_refresh_rejects_source_and_known_binding_changes() {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    for compressed in [false, true] {
        for mutation in ["edit", "delete", "scope", "binding"] {
            let store = store().await;
            if compressed {
                compress_output_events(&store).await;
            }
            delivered_output_fixture(&store, "first", "shared-turn").await;
            delivered_output_fixture(&store, "second", "shared-turn").await;
            delivered_ack(&store, "first", "shared-turn").await;
            delivered_ack(&store, "second", "shared-turn").await;
            let fence = store.compaction_history_read_fence().await.unwrap();
            let warm = store
                .compaction_delivered_output_page(
                    "ws",
                    "thread",
                    &DeliveredTaskOutputCursor::default(),
                    &fence,
                )
                .await
                .unwrap();
            let first = warm.entries[0].acknowledgement.clone();
            store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "UPDATE compaction_event_revision SET projection_revision=NULL,item_id=NULL,projection_kind=NULL WHERE source_id=?", [first.id.clone().into()],
            )).await.unwrap();
            let cold = store
                .compaction_delivered_output_page(
                    "ws",
                    "thread",
                    &DeliveredTaskOutputCursor::default(),
                    &fence,
                )
                .await
                .unwrap();
            assert_eq!(
                cold.entries.len(),
                1,
                "a warm binding is retained alongside the cold source"
            );
            refresh_output_page(&store, &cold).await;
            assert_eq!(
                store
                    .compaction_recheck_delivered_output_page("ws", "thread", &cold, &fence)
                    .await
                    .unwrap(),
                warm
            );
            let db = store.database_connection();
            if mutation == "binding" {
                db.execute_unprepared("UPDATE compaction_delivery_output SET candidate_id='first' WHERE delivery_id='second'").await.unwrap();
            } else {
                let sql = match mutation {
                    "edit" => "UPDATE turn_event SET payload=payload||' ' WHERE id=?",
                    "delete" => "DELETE FROM turn_event WHERE id=?",
                    "scope" => "UPDATE turn_event SET thread_id='other-thread' WHERE id=?",
                    _ => unreachable!(),
                };
                db.execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    sql,
                    [first.id.clone().into()],
                ))
                .await
                .unwrap();
            }
            assert!(
                store
                    .compaction_recheck_delivered_output_page("ws", "thread", &cold, &fence)
                    .await
                    .is_err(),
                "{mutation} must invalidate the selected page"
            );
        }
    }
}

#[tokio::test]
async fn delivered_output_discovery_preserves_routes_and_cancels_queued_reads() {
    use pioneer_sqlite::{
        SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteReadOutcome, SqliteWriteEvent,
        SqliteWriteObserver,
    };
    use sea_orm::{ConnectOptions, StreamTrait};
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };
    #[derive(Default)]
    struct Observer {
        reads: Mutex<Vec<SqliteReadEvent>>,
        writes: Mutex<Vec<SqliteWriteEvent>>,
        queued: tokio::sync::Notify,
    }
    impl SqliteReadObserver for Observer {
        fn observe(&self, event: SqliteReadEvent) {
            self.reads.lock().unwrap().push(event);
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
        }
    }
    impl SqliteWriteObserver for Observer {
        fn observe(&self, event: SqliteWriteEvent) {
            self.writes.lock().unwrap().push(event);
        }
    }
    let directory = std::env::current_dir()
        .unwrap()
        .join("target/compaction-tests");
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join(format!("{}.sqlite", uuid::Uuid::new_v4()));
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    options.max_connections(1).min_connections(1);
    let writer = Database::connect(options).await.unwrap();
    Migrator::up(&writer, None).await.unwrap();
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        writer.execute_unprepared(sql).await.unwrap();
    }
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=ro", path.display()));
    options.max_connections(2).min_connections(2);
    let reader = Database::connect(options).await.unwrap();
    reader
        .execute_unprepared("PRAGMA query_only=ON")
        .await
        .unwrap();
    let reader_proof = reader.clone();
    let observer = Arc::new(Observer::default());
    let database = pioneer_sqlite::SqliteDatabase::from_executor_with_read_observer(
        reader,
        pioneer_sqlite::SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    let interactive = CrudStore::new(database.clone());
    delivered_output_fixture(&interactive, "delivery", "delivery-turn").await;
    delivered_ack(&interactive, "delivery", "delivery-turn").await;
    let fence = interactive.compaction_history_read_fence().await.unwrap();
    let maintenance = interactive.with_maintenance_access();
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    let held = maintenance
        .database_connection()
        .stream_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT 1".to_owned(),
        ))
        .await
        .unwrap();
    let waiting = tokio::spawn({
        let maintenance = maintenance.clone();
        let fence = fence.clone();
        async move {
            maintenance
                .compaction_delivered_output_page(
                    "ws",
                    "thread",
                    &DeliveredTaskOutputCursor::default(),
                    &fence,
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), observer.queued.notified())
        .await
        .unwrap();
    // Interactive discovery remains on its reader route while maintenance is
    // queued. Both locator and bounded event query inherit the scoped class.
    let page = tokio::time::timeout(
        Duration::from_secs(2),
        interactive.compaction_delivered_output_page(
            "ws",
            "thread",
            &DeliveredTaskOutputCursor::default(),
            &fence,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(page.entries.len(), 1);
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    assert!(observer.reads.lock().unwrap().iter().any(|event| matches!(
        event,
        SqliteReadEvent::AdmissionCancelled {
            queue_depth: 0,
            active: 1,
            ..
        }
    )));
    drop(held);
    let resumed = tokio::time::timeout(
        Duration::from_secs(2),
        maintenance.compaction_delivered_output_page(
            "ws",
            "thread",
            &DeliveredTaskOutputCursor::default(),
            &fence,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(resumed, page);
    let reads = observer.reads.lock().unwrap().clone();
    for class in [SqliteReadClass::Interactive, SqliteReadClass::Maintenance] {
        assert_eq!(reads.iter().filter(|event| matches!(event,
            SqliteReadEvent::OperationFinished { class: actual, outcome: SqliteReadOutcome::Ok, .. } if *actual == class
        )).count(), 2);
    }
    assert!(reads.iter().any(|event| matches!(
        event,
        SqliteReadEvent::AdmissionReleased {
            queue_depth: 0,
            active: 0,
            ..
        }
    )));
    assert!(
        observer.writes.lock().unwrap().is_empty(),
        "metadata discovery must stay on the physical reader"
    );
    assert!(
        reader_proof
            .execute_unprepared("UPDATE task_delivery SET status='queued'")
            .await
            .is_err(),
        "the independently opened discovery reader must reject physical writes"
    );
}

#[tokio::test]
async fn frozen_own_imports_require_exact_output_membership_and_atomic_publication() {
    let mut frozen_conversion_progress = pioneer_crud::FrozenStorageLifetimeProgress::default();
    use pioneer_compaction::frozen::{FrozenHistoryRef, FrozenMessageRef};
    use sha2::{Digest, Sha256};
    fn descriptor(id: &str, messages: &[FrozenMessageRef]) -> FrozenHistoryRef {
        let mut digest = Sha256::new();
        for message in messages {
            let bytes = serde_json::to_vec(message).unwrap();
            digest.update((bytes.len() as u64).to_be_bytes());
            digest.update(bytes);
        }
        FrozenHistoryRef {
            format: 1,
            manifest_id: id.into(),
            messages: messages.len() as u64,
            identity_sha256: hex::encode(digest.finalize()),
        }
    }
    let store = store().await;
    let db = store.database_connection();
    source(&store, "basis-source", 1, "{}").await;
    let inherited = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    for statement in [
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('child','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('child-turn','child','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('child-source','child','child-turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run','task','run',1,1,'succeeded','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('rt','task','run','child','child-turn','initial',0,1,'completed',CURRENT_TIMESTAMP)",
        "INSERT INTO task_result_candidate(id,task_id,run_id,task_run_turn_id,thread_id,turn_id,round,status,created_at,updated_at) VALUES ('candidate','task','run','rt','child','child-turn',0,'accepted',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('delivery-turn','thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,thread_target,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id) VALUES ('delivery','ws','task','run','key','thread','origin_thread','thread','delivered',1,1,'delivery-turn')",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
    let own_source = SourceRef {
        scope: "event:child-turn".into(),
        id: "child-source".into(),
        version: "event-revision:1".into(),
    };
    let own = FrozenMessageRef {
        logical_turn_id: None,
        context_thread: None,
        source_thread: "child".into(),
        unit_id: "child-unit".into(),
        sources: vec![own_source.clone()],
        event_input_role: None,
        source_aliases: vec![],
        ambiguous_input_aliases: vec![],
        publication_aliases: None,
        inherited: false,
        complete: true,
        protected_input: false,
        wire_sha256: "a".repeat(64),
        replay_source: None,
        tool_item_id: None,
        tool_call_id: None,
        tool_name: None,
    };
    let basis = FrozenMessageRef {
        source_thread: "thread".into(),
        sources: vec![inherited.clone()],
        inherited: true,
        ..own.clone()
    };
    let output = descriptor("output-with-basis", &[own.clone(), basis.clone()]);
    store
        .compaction_begin_frozen_history("ws", "child", &output)
        .await
        .unwrap();
    store
        .compaction_append_frozen_history(
            "ws",
            "child",
            &output.manifest_id,
            0,
            &[own.clone(), basis.clone()],
        )
        .await
        .unwrap();
    assert!(
        store
            .compaction_finish_frozen_history("ws", "child", &output)
            .await
            .unwrap()
    );
    store
        .compaction_record_task_output("ws", "rt", &output)
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO compaction_delivery_output(delivery_id,candidate_id,task_run_turn_id) VALUES ('delivery','candidate','rt')").await.unwrap();
    store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "delivery-turn".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: pioneer_protocol::task_delivery_result_item_id("delivery"),
                    text: "delivered".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
    let acknowledgement = store
        .compaction_source_page("ws", "thread", "delivery-turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    let prepared = store
        .compaction_prepare_frozen_import(
            "ws",
            "thread",
            "delivery",
            &acknowledgement,
            0,
            "child",
            &own_source,
        )
        .await
        .unwrap();
    assert!(
        store
            .compaction_prepare_frozen_import(
                "ws",
                "thread",
                "delivery",
                &acknowledgement,
                1,
                "thread",
                &inherited
            )
            .await
            .is_err(),
        "H cannot become own work"
    );
    assert!(
        store
            .compaction_prepare_frozen_import(
                "ws",
                "thread",
                "delivery",
                &acknowledgement,
                0,
                "thread",
                &inherited
            )
            .await
            .is_err(),
        "an unrelated same-workspace source is not an output member"
    );
    assert!(
        store
            .compaction_prepare_frozen_import(
                "other",
                "thread",
                "delivery",
                &acknowledgement,
                0,
                "child",
                &own_source
            )
            .await
            .is_err()
    );
    let mut stale_ack = acknowledgement.clone();
    stale_ack.version = "event-revision:999".into();
    assert!(
        store
            .compaction_prepare_frozen_import(
                "ws",
                "thread",
                "delivery",
                &stale_ack,
                0,
                "child",
                &own_source
            )
            .await
            .is_err()
    );
    let target = FrozenMessageRef {
        context_thread: Some("thread".into()),
        ..own
    };
    // H is already represented by a published checkpoint S in the accepted
    // snapshot. The checkpoint is an atomic accepted-basis source; its saved
    // coverage is used only for boundary/grant membership.
    let s_operation = admit_import_operation(&store, "basis-s", "thread", "turn").await;
    let s_ready = ready_import_operation(&store, &s_operation, "thread", &inherited).await;
    assert_eq!(
        store
            .compaction_apply_runner(&s_operation.id, &s_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let s_source = store
        .compaction_checkpoint_source("ws", "thread", "checkpoint-basis-s")
        .await
        .unwrap()
        .unwrap();
    let checkpoint_basis = FrozenMessageRef {
        sources: vec![s_source.clone()],
        wire_sha256: "c".repeat(64),
        inherited: true,
        ..basis.clone()
    };
    let context_messages = vec![checkpoint_basis, target.clone()];
    let context = descriptor("assembled", &context_messages);
    let imports = vec![(1, prepared.clone())];
    let import_digest = frozen_import_identity(&imports).unwrap();
    store
        .compaction_begin_frozen_history_with_imports("ws", "thread", &context, 1, &import_digest)
        .await
        .unwrap();
    store
        .compaction_append_frozen_history(
            "ws",
            "thread",
            &context.manifest_id,
            0,
            &context_messages,
        )
        .await
        .unwrap();
    assert!(
        !store
            .compaction_finish_frozen_history("ws", "thread", &context)
            .await
            .unwrap()
    );
    assert!(
        store
            .compaction_frozen_history_owner("ws", &context)
            .await
            .unwrap()
            .is_none()
    );
    db.execute_unprepared("DELETE FROM compaction_delivery_output WHERE delivery_id='delivery'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_append_frozen_imports("ws", "thread", &context.manifest_id, 0, &imports)
            .await
            .is_err(),
        "binding is revalidated after preparation"
    );
    db.execute_unprepared("INSERT INTO compaction_delivery_output(delivery_id,candidate_id,task_run_turn_id) VALUES ('delivery','candidate','rt')").await.unwrap();
    db.execute_unprepared("CREATE TEMP TRIGGER abort_frozen_import AFTER INSERT ON compaction_frozen_import_data BEGIN SELECT RAISE(ABORT,'fixture import rollback'); END").await.unwrap();
    assert!(
        store
            .compaction_append_frozen_imports("ws", "thread", &context.manifest_id, 0, &imports)
            .await
            .is_err()
    );
    assert!(
        !store
            .compaction_finish_frozen_history("ws", "thread", &context)
            .await
            .unwrap()
    );
    db.execute_unprepared("DROP TRIGGER abort_frozen_import")
        .await
        .unwrap();
    store
        .compaction_append_frozen_imports("ws", "thread", &context.manifest_id, 0, &imports)
        .await
        .unwrap();
    store
        .compaction_append_frozen_imports("ws", "thread", &context.manifest_id, 0, &imports)
        .await
        .unwrap();
    assert!(
        store
            .compaction_finish_frozen_history("ws", "thread", &context)
            .await
            .unwrap()
    );
    store
        .compaction_append_frozen_imports("ws", "thread", &context.manifest_id, 0, &imports)
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_frozen_import_state("ws", "thread", &context.manifest_id)
            .await
            .unwrap(),
        Some((1, import_digest.clone()))
    );
    let records = store
        .compaction_frozen_import_page("ws", "thread", &context.manifest_id, 0)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].source, own_source);
    assert_eq!(records[0].delivery_id, "delivery");
    assert!(
        store
            .compaction_frozen_import_page("other", "thread", &context.manifest_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
    for _ in 0..100 {
        if !store
            .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
            .await
            .unwrap()
        {
            break;
        }
    }
    let shared = FrozenHistoryRef {
        manifest_id: "assembled-shared".into(),
        ..context.clone()
    };
    store
        .compaction_begin_frozen_history_with_imports("ws", "thread", &shared, 1, &import_digest)
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_share_frozen_prefix(
                "ws",
                "thread",
                &shared.manifest_id,
                &context_messages,
                &imports
            )
            .await
            .unwrap(),
        (2, 1)
    );
    assert!(
        store
            .compaction_finish_frozen_history("ws", "thread", &shared)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .compaction_frozen_import_page("ws", "thread", &shared.manifest_id, 0)
            .await
            .unwrap(),
        records
    );
    assert_eq!(
        frozen_count(&store, "compaction_frozen_import_data").await,
        1
    );
    // The import still matches, but its target is not stored yet when the
    // message prefix diverges. Prefix preparation (including retry) must let
    // the normal message-then-import append path complete the capture.
    let mut divergent_messages = context_messages.clone();
    divergent_messages[0].wire_sha256 = "d".repeat(64);
    let divergent = descriptor("assembled-divergent", &divergent_messages);
    store
        .compaction_begin_frozen_history_with_imports("ws", "thread", &divergent, 1, &import_digest)
        .await
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            store
                .compaction_share_frozen_prefix(
                    "ws",
                    "thread",
                    &divergent.manifest_id,
                    &divergent_messages,
                    &imports,
                )
                .await
                .unwrap(),
            (0, 0)
        );
    }
    store
        .compaction_append_frozen_history(
            "ws",
            "thread",
            &divergent.manifest_id,
            0,
            &divergent_messages,
        )
        .await
        .unwrap();
    store
        .compaction_append_frozen_imports("ws", "thread", &divergent.manifest_id, 0, &imports)
        .await
        .unwrap();
    assert!(
        store
            .compaction_finish_frozen_history("ws", "thread", &divergent)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .compaction_frozen_import_page("ws", "thread", &divergent.manifest_id, 0)
            .await
            .unwrap(),
        records
    );
    // A new execution can adopt this metadata only through its exact TaskRun
    // snapshot; sharing a workspace or parent thread is insufficient.
    for statement in [
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('context-c','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('context-c','thread','thread',1,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn-c','context-c','in_progress','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run-c','task','run-c',1,2,'running','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('rt-c','task','run-c','context-c','turn-c','initial',0,1,'running',CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
    let operation = admit_import_operation(&store, "own-c", "context-c", "turn-c").await;
    assert!(
        store
            .compaction_bind_source_projection(&operation.id, &context)
            .await
            .is_err(),
        "an arbitrary parent manifest is not an accepted Task basis"
    );
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "WITH frozen_root_fixture(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) AS (VALUES ('run-c','task','ws','thread',?,CURRENT_TIMESTAMP)) INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,frozen_manifest_id) SELECT run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,CASE WHEN json_valid(history_json) THEN CASE WHEN json_type(history_json)='object' THEN json_extract(history_json,'$.manifest_id') ELSE NULL END ELSE NULL END FROM frozen_root_fixture",
        [serde_json::to_string(&context).unwrap().into()])).await.unwrap();
    assert!(
        store
            .compaction_bind_source_projection(&operation.id, &output)
            .await
            .is_err()
    );
    db.execute_unprepared("CREATE TEMP TRIGGER abort_projection AFTER INSERT ON compaction_operation_projection BEGIN SELECT RAISE(ABORT,'fixture binding rollback'); END").await.unwrap();
    assert!(
        store
            .compaction_bind_source_projection(&operation.id, &context)
            .await
            .is_err()
    );
    db.execute_unprepared("DROP TRIGGER abort_projection")
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&operation.id, &context)
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&operation.id, &context)
        .await
        .unwrap();
    // A completed child's summary is created AFTER its immutable raw output.
    // C may reuse that summary only if every leaf has a grant in C's accepted
    // basis; the output manifest itself is never rewritten to add the summary.
    let a_operation = admit_import_operation(&store, "summary-a", "child", "child-turn").await;
    let a_ready = ready_import_operation(&store, &a_operation, "child", &own_source).await;
    assert_eq!(
        store
            .compaction_apply_runner(&a_operation.id, &a_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let a_summary = store
        .compaction_checkpoint_source("ws", "child", "checkpoint-summary-a")
        .await
        .unwrap()
        .unwrap();
    let mut projected_operation = operation.clone();
    projected_operation.id = "projected-c".into();
    projected_operation.owner = "owner-projected-c".into();
    projected_operation.plan.fingerprint = "projected-c".into();
    for scope in ["child", "context-c", "thread"] {
        projected_operation.source_epochs.insert(
            scope.into(),
            store
                .compaction_projection_version("ws", scope)
                .await
                .unwrap(),
        );
    }
    store
        .compaction_admit("ws", "context-c", &projected_operation)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(&projected_operation.id, "turn-c")
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&projected_operation.id, &context)
        .await
        .unwrap();
    let projected_ready =
        ready_import_operation(&store, &projected_operation, "child", &a_summary).await;
    assert!(
        store
            .compaction_manifest_sources_current(&projected_operation.id)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .compaction_apply_runner(&projected_operation.id, &projected_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    // An accessible checkpoint must not launder H into accepted OWN work.
    // Model an older mixed checkpoint: only one of its two leaves was delivered.
    let mut mixed_op = admit_import_operation(&store, "mixed-summary", "child", "child-turn").await;
    // This legacy raw checkpoint originally admitted both leaves. It still
    // must not grant a leaf that was never accepted in the delivered output.
    mixed_op.plan.compact = vec![0, 1];
    mixed_op.plan.coverage = vec![own_source.clone(), inherited.clone()];
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_operation SET snapshot=? WHERE id=?",
        [
            serde_json::to_string(&mixed_op).unwrap().into(),
            mixed_op.id.clone().into(),
        ],
    ))
    .await
    .unwrap();
    let mixed = Checkpoint {
        id: "mixed-checkpoint".into(),
        operation_id: mixed_op.id.clone(),
        owner: mixed_op.owner.clone(),
        previous: None,
        format_version: 1,
        coverage: vec![own_source.clone(), inherited.clone()],
        summary: "mixed H and A".into(),
        selection: mixed_op.admission.selection.clone(),
        projection_version: mixed_op.projection_version,
    };
    store.compaction_save_candidate(&mixed, 0).await.unwrap();
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='applied' WHERE id='mixed-checkpoint'",
    )
    .await
    .unwrap();
    let mixed_source = store
        .compaction_checkpoint_source("ws", "child", &mixed.id)
        .await
        .unwrap()
        .unwrap();
    let mut mixed_target = projected_operation.clone();
    mixed_target.id = "mixed-target".into();
    mixed_target.owner = "owner-mixed-target".into();
    mixed_target.plan.fingerprint = "mixed-target".into();
    store
        .compaction_admit("ws", "context-c", &mixed_target)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(&mixed_target.id, "turn-c")
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&mixed_target.id, &context)
        .await
        .unwrap();
    let _mixed_ready = ready_import_operation(&store, &mixed_target, "child", &mixed_source).await;
    assert!(
        !store
            .compaction_manifest_sources_current(&mixed_target.id)
            .await
            .unwrap()
    );
    // The manifest check above rejects this input before summarization.
    // Publication trusts the versions consumed by a valid runner.
    // Publish K through the real runner path. K covers accepted S + imported A;
    // its H leaves are not present directly in the accepted frozen messages.
    let mut working_op = projected_operation.clone();
    working_op.id = "working-summary-operation".into();
    working_op.owner = "owner-working-summary".into();
    working_op.plan.coverage_domain = pioneer_compaction::CoverageDomain::WorkingContext;
    working_op.plan.fingerprint = working_op.id.clone();
    store
        .compaction_admit("ws", "context-c", &working_op)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(&working_op.id, "turn-c")
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&working_op.id, &context)
        .await
        .unwrap();
    let k_ready = ready_operation(
        &store,
        &working_op,
        &[
            ("thread".into(), s_source.clone()),
            ("child".into(), own_source.clone()),
        ],
    )
    .await;
    assert_eq!(
        store
            .compaction_apply_runner(&working_op.id, &k_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let k = store
        .compaction_checkpoint("checkpoint-working-summary-operation")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(k.coverage, vec![s_source.clone(), own_source.clone()]);
    let working_source = store
        .compaction_checkpoint_source("ws", "context-c", "checkpoint-working-summary-operation")
        .await
        .unwrap()
        .unwrap();

    // A following child accepts the same S + raw A snapshot. Its compaction may
    // consume K as WorkingContext, but the identical grants must not authorize
    // K for an OwnContribution operation.
    for statement in [
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('context-d','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('context-d','thread','thread',1,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn-d','context-d','in_progress','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task-d','ws','thread','thread','thread','turn','agent','running','Task D','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run-d','task-d','run-d',1,1,'running','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('rt-d','task-d','run-d','context-d','turn-d','initial',0,1,'running',CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "WITH frozen_root_fixture(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) AS (VALUES ('run-d','task-d','ws','thread',?,CURRENT_TIMESTAMP)) INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,frozen_manifest_id) SELECT run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,CASE WHEN json_valid(history_json) THEN CASE WHEN json_type(history_json)='object' THEN json_extract(history_json,'$.manifest_id') ELSE NULL END ELSE NULL END FROM frozen_root_fixture",
        [serde_json::to_string(&context).unwrap().into()],
    ))
    .await
    .unwrap();
    let mut working_target = projected_operation.clone();
    working_target.plan.coverage_domain = pioneer_compaction::CoverageDomain::WorkingContext;
    working_target.projection_version = store
        .compaction_projection_version("ws", "context-d")
        .await
        .unwrap();
    working_target.source_epochs.clear();
    for scope in ["thread", "child", "context-c", "context-d"] {
        working_target.source_epochs.insert(
            scope.into(),
            store
                .compaction_projection_version("ws", scope)
                .await
                .unwrap(),
        );
    }
    working_target.id = "working-target-admitted".into();
    working_target.owner = "owner-working-target-admitted".into();
    working_target.plan.fingerprint = working_target.id.clone();
    store
        .compaction_admit("ws", "context-d", &working_target)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(&working_target.id, "turn-d")
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&working_target.id, &context)
        .await
        .unwrap();
    let working_ready =
        ready_import_operation(&store, &working_target, "context-c", &working_source).await;
    assert!(
        store
            .compaction_manifest_sources_current(&working_target.id)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .compaction_apply_runner(&working_target.id, &working_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let mut own_target = working_target.clone();
    own_target.id = "own-target".into();
    own_target.owner = "owner-own-target".into();
    own_target.plan.coverage_domain = pioneer_compaction::CoverageDomain::OwnContribution;
    own_target.plan.fingerprint = own_target.id.clone();
    store
        .compaction_admit("ws", "context-d", &own_target)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(&own_target.id, "turn-d")
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&own_target.id, &context)
        .await
        .unwrap();
    let _own_ready =
        ready_import_operation(&store, &own_target, "context-c", &working_source).await;
    assert!(
        !store
            .compaction_manifest_sources_current(&own_target.id)
            .await
            .unwrap()
    );
    // The manifest check above rejects this input before summarization.
    // Publication trusts the versions consumed by a valid runner.
    let denied = admit_import_operation(&store, "unaccepted-summary", "context-c", "turn-c").await;
    let _denied_ready = ready_import_operation(&store, &denied, "child", &a_summary).await;
    assert!(
        !store
            .compaction_manifest_sources_current(&denied.id)
            .await
            .unwrap()
    );
    // The manifest check above rejects this input before summarization.
    // Publication trusts the versions consumed by a valid runner.
    assert_eq!(
        store
            .compaction_frozen_history_page("ws", "thread", &context.manifest_id, 0)
            .await
            .unwrap(),
        context_messages
    );

    // Production path: the accepted parent basis is recaptured by the child.
    // Ownership evidence must survive that capture and authorize final commit.
    let maintenance = store.with_maintenance_access();
    let forwarded = maintenance
        .compaction_prepare_accepted_import("ws", "context-c", "turn-c", 0)
        .await
        .unwrap();
    assert!(
        maintenance
            .compaction_prepare_accepted_import("ws", "child", "turn-c", 0)
            .await
            .is_err()
    );
    let child_target = FrozenMessageRef {
        context_thread: Some("context-c".into()),
        ..target.clone()
    };
    let child_context = descriptor("recaptured-c", std::slice::from_ref(&child_target));
    let forwarded_imports = vec![(0, forwarded)];
    let forwarded_digest =
        pioneer_crud::compaction::frozen_import_identity(&forwarded_imports).unwrap();
    maintenance
        .compaction_begin_frozen_history_with_imports(
            "ws",
            "context-c",
            &child_context,
            1,
            &forwarded_digest,
        )
        .await
        .unwrap();
    maintenance
        .compaction_append_frozen_history(
            "ws",
            "context-c",
            &child_context.manifest_id,
            0,
            std::slice::from_ref(&child_target),
        )
        .await
        .unwrap();
    db.execute_unprepared(
        "UPDATE task_run_conversation_snapshot SET history_json='[]',frozen_manifest_id=NULL WHERE run_id='run-c'",
    )
    .await
    .unwrap();
    assert!(
        maintenance
            .compaction_append_frozen_imports(
                "ws",
                "context-c",
                &child_context.manifest_id,
                0,
                &forwarded_imports
            )
            .await
            .is_err()
    );
    assert!(
        !maintenance
            .compaction_finish_frozen_history("ws", "context-c", &child_context)
            .await
            .unwrap()
    );
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "WITH root_replacement(history_json) AS (VALUES (?)) UPDATE task_run_conversation_snapshot SET history_json=(SELECT history_json FROM root_replacement),frozen_manifest_id=(SELECT CASE WHEN json_valid(history_json) THEN CASE WHEN json_type(history_json)='object' THEN json_extract(history_json,'$.manifest_id') ELSE NULL END ELSE NULL END FROM root_replacement) WHERE run_id='run-c'",
        [serde_json::to_string(&context).unwrap().into()],
    ))
    .await
    .unwrap();
    db.execute_unprepared("CREATE TEMP TRIGGER abort_forwarded_import AFTER INSERT ON compaction_frozen_import_data BEGIN SELECT RAISE(ABORT,'fixture forward rollback'); END").await.unwrap();
    assert!(
        maintenance
            .compaction_append_frozen_imports(
                "ws",
                "context-c",
                &child_context.manifest_id,
                0,
                &forwarded_imports
            )
            .await
            .is_err()
    );
    assert!(
        !maintenance
            .compaction_finish_frozen_history("ws", "context-c", &child_context)
            .await
            .unwrap()
    );
    db.execute_unprepared("DROP TRIGGER abort_forwarded_import")
        .await
        .unwrap();
    for _ in 0..2 {
        maintenance
            .compaction_append_frozen_imports(
                "ws",
                "context-c",
                &child_context.manifest_id,
                0,
                &forwarded_imports,
            )
            .await
            .unwrap();
    }
    assert!(
        maintenance
            .compaction_finish_frozen_history("ws", "context-c", &child_context)
            .await
            .unwrap()
    );
    let carried = maintenance
        .compaction_prepare_accepted_checkpoint_import(
            "ws",
            "context-c",
            "turn-c",
            0,
            "child",
            &a_summary,
        )
        .await
        .unwrap();
    let summary_target = FrozenMessageRef {
        sources: vec![a_summary.clone()],
        wire_sha256: "d".repeat(64),
        ..child_target.clone()
    };
    let projected_context = descriptor(
        "recaptured-checkpoint-c",
        std::slice::from_ref(&summary_target),
    );
    let carried_imports = vec![(0, carried)];
    let carried_digest = frozen_import_identity(&carried_imports).unwrap();
    maintenance
        .compaction_begin_frozen_history_with_imports(
            "ws",
            "context-c",
            &projected_context,
            1,
            &carried_digest,
        )
        .await
        .unwrap();
    maintenance
        .compaction_append_frozen_history(
            "ws",
            "context-c",
            &projected_context.manifest_id,
            0,
            std::slice::from_ref(&summary_target),
        )
        .await
        .unwrap();
    maintenance
        .compaction_append_frozen_imports(
            "ws",
            "context-c",
            &projected_context.manifest_id,
            0,
            &carried_imports,
        )
        .await
        .unwrap();
    assert!(
        maintenance
            .compaction_finish_frozen_history("ws", "context-c", &projected_context)
            .await
            .unwrap()
    );
    let projected_capture_operation = admit_import_operation(
        &maintenance,
        "projected-capture-operation",
        "context-c",
        "turn-c",
    )
    .await;
    maintenance
        .compaction_bind_source_projection(&projected_capture_operation.id, &projected_context)
        .await
        .unwrap();
    let projected_capture_ready = ready_import_operation(
        &maintenance,
        &projected_capture_operation,
        "child",
        &a_summary,
    )
    .await;
    assert_eq!(
        maintenance
            .compaction_apply_runner(
                &projected_capture_operation.id,
                &projected_capture_ready,
                None,
            )
            .await
            .unwrap(),
        CommitOutcome::Applied,
        "a carried raw grant must authorize the exact foreign OWN summary that replaced it"
    );
    let recaptured_operation =
        admit_import_operation(&maintenance, "recaptured-operation", "context-c", "turn-c").await;
    maintenance
        .compaction_bind_source_projection(&recaptured_operation.id, &child_context)
        .await
        .unwrap();
    let recaptured_ready =
        ready_import_operation(&maintenance, &recaptured_operation, "child", &own_source).await;
    assert_eq!(
        maintenance
            .compaction_apply_runner(&recaptured_operation.id, &recaptured_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(
        maintenance
            .compaction_apply_runner(&recaptured_operation.id, &recaptured_ready, None)
            .await
            .unwrap(),
        CommitOutcome::AlreadyApplied
    );

    let ready = ready_import_operation(&store, &operation, "child", &own_source).await;
    store
        .compaction_bind_source_projection(&operation.id, &context)
        .await
        .unwrap();
    // Complete identity is immutable now: an attempted rewrite is rejected
    // before it can widen the operation's read-time authority. Corrupt legacy
    // headers remain covered by the predicate/oracle fixtures.
    assert!(
        db.execute_unprepared(
            "UPDATE compaction_frozen_history SET imports_sha256='changed' WHERE id='assembled'",
        )
        .await
        .is_err()
    );
    let pinned: String = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT imports_sha256 FROM compaction_frozen_history WHERE id='assembled'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "imports_sha256")
        .unwrap();
    assert_eq!(pinned, import_digest);
    assert!(
        store
            .compaction_manifest_sources_current(&operation.id)
            .await
            .unwrap()
    );
    db.execute_unprepared("CREATE TEMP TRIGGER abort_import_commit AFTER UPDATE OF head ON compaction_context BEGIN SELECT RAISE(ABORT,'fixture imported commit rollback'); END").await.unwrap();
    assert!(
        store
            .compaction_apply_runner(&operation.id, &ready, None)
            .await
            .is_err()
    );
    assert_eq!(store.compaction_head(&operation.owner).await.unwrap(), None);
    db.execute_unprepared("DROP TRIGGER abort_import_commit")
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_apply_runner(&operation.id, &ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(
        store
            .compaction_apply_runner(&operation.id, &ready, None)
            .await
            .unwrap(),
        CommitOutcome::AlreadyApplied
    );

    let unbound = admit_import_operation(&store, "unbound-c", "context-c", "turn-c").await;
    let _unbound_ready = ready_import_operation(&store, &unbound, "child", &own_source).await;
    assert!(
        !store
            .compaction_manifest_sources_current(&unbound.id)
            .await
            .unwrap(),
        "unauthorized sources must be rejected before provider execution"
    );
    assert!(
        store
            .compaction_bind_source_projection(&unbound.id, &context)
            .await
            .is_err(),
        "a ready runner cannot gain new ownership after provider execution"
    );
    let h_operation = admit_import_operation(&store, "h-c", "context-c", "turn-c").await;
    store
        .compaction_bind_source_projection(&h_operation.id, &context)
        .await
        .unwrap();
    let _h_ready = ready_import_operation(&store, &h_operation, "thread", &inherited).await;
    assert!(
        !store
            .compaction_manifest_sources_current(&h_operation.id)
            .await
            .unwrap(),
        "unauthorized sources must be rejected before provider execution"
    );
    let edited = admit_import_operation(&store, "edited-c", "context-c", "turn-c").await;
    store
        .compaction_bind_source_projection(&edited.id, &context)
        .await
        .unwrap();
    let edited_ready = ready_import_operation(&store, &edited, "child", &own_source).await;
    let mut imported_checkpoint_edited = projected_operation.clone();
    imported_checkpoint_edited.id = "edited-imported-checkpoint".into();
    imported_checkpoint_edited.owner = "owner-edited-imported-checkpoint".into();
    imported_checkpoint_edited.plan.fingerprint = imported_checkpoint_edited.id.clone();
    imported_checkpoint_edited.projection_version = store
        .compaction_projection_version("ws", "context-c")
        .await
        .unwrap();
    imported_checkpoint_edited.source_epochs.clear();
    for scope in ["child", "context-c", "thread"] {
        imported_checkpoint_edited.source_epochs.insert(
            scope.into(),
            store
                .compaction_projection_version("ws", scope)
                .await
                .unwrap(),
        );
    }
    store
        .compaction_admit("ws", "context-c", &imported_checkpoint_edited)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(&imported_checkpoint_edited.id, "turn-c")
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&imported_checkpoint_edited.id, &context)
        .await
        .unwrap();
    let imported_checkpoint_ready =
        ready_import_operation(&store, &imported_checkpoint_edited, "child", &a_summary).await;
    let stale_forwarded = maintenance
        .compaction_prepare_accepted_import("ws", "context-c", "turn-c", 0)
        .await
        .unwrap();
    let stale_forwarded_imports = vec![(0, stale_forwarded)];
    let stale_forwarded_digest = frozen_import_identity(&stale_forwarded_imports).unwrap();
    let stale_forwarded_context =
        descriptor("stale-forwarded", std::slice::from_ref(&child_target));
    maintenance
        .compaction_begin_frozen_history_with_imports(
            "ws",
            "context-c",
            &stale_forwarded_context,
            1,
            &stale_forwarded_digest,
        )
        .await
        .unwrap();
    maintenance
        .compaction_append_frozen_history(
            "ws",
            "context-c",
            &stale_forwarded_context.manifest_id,
            0,
            std::slice::from_ref(&child_target),
        )
        .await
        .unwrap();
    let stale = descriptor("stale-assembled", std::slice::from_ref(&target));
    store
        .compaction_begin_frozen_history_with_imports("ws", "thread", &stale, 1, &import_digest)
        .await
        .unwrap();
    store
        .compaction_append_frozen_history("ws", "thread", &stale.manifest_id, 0, &[target])
        .await
        .unwrap();
    db.execute_unprepared(
        "UPDATE turn_event SET payload='{\"changed\":true}' WHERE id='child-source'",
    )
    .await
    .unwrap();
    assert!(
        maintenance
            .compaction_append_frozen_imports(
                "ws",
                "context-c",
                &stale_forwarded_context.manifest_id,
                0,
                &stale_forwarded_imports,
            )
            .await
            .is_err(),
        "accepted source revision is revalidated in the writer transaction"
    );
    let stale_forwarded_state = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT h.next_import,(SELECT COUNT(*) FROM compaction_frozen_import i WHERE i.manifest_id=h.id) AS stored FROM compaction_frozen_history h WHERE h.id=?",
            [stale_forwarded_context.manifest_id.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stale_forwarded_state
            .try_get::<i64>("", "next_import")
            .unwrap(),
        0,
        "rejected accepted import must not advance its cursor"
    );
    assert_eq!(
        stale_forwarded_state.try_get::<i64>("", "stored").unwrap(),
        0,
        "rejected accepted import must not write metadata"
    );
    assert!(
        store
            .compaction_append_frozen_imports("ws", "thread", &stale.manifest_id, 0, &imports)
            .await
            .is_err(),
        "source revision is revalidated in the writer transaction"
    );
    assert_eq!(
        store
            .compaction_apply_runner(&edited.id, &edited_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(
        store
            .compaction_apply_runner(
                &imported_checkpoint_edited.id,
                &imported_checkpoint_ready,
                None
            )
            .await
            .unwrap(),
        CommitOutcome::Applied,
        "an edited historical leaf invalidated an accepted published checkpoint"
    );
    db.execute_unprepared("DELETE FROM turn_event WHERE id='child-source'")
        .await
        .unwrap();
    let carried_after_delete = maintenance
        .compaction_prepare_accepted_checkpoint_import(
            "ws",
            "context-c",
            "turn-c",
            0,
            "child",
            &a_summary,
        )
        .await
        .expect("carrying a grant through its summary must not read the deleted raw payload");
    let projected_after_delete = descriptor(
        "recaptured-checkpoint-after-delete",
        std::slice::from_ref(&summary_target),
    );
    let carried_after_delete = vec![(0, carried_after_delete)];
    let carried_after_delete_digest = frozen_import_identity(&carried_after_delete).unwrap();
    maintenance
        .compaction_begin_frozen_history_with_imports(
            "ws",
            "context-c",
            &projected_after_delete,
            1,
            &carried_after_delete_digest,
        )
        .await
        .unwrap();
    maintenance
        .compaction_append_frozen_history(
            "ws",
            "context-c",
            &projected_after_delete.manifest_id,
            0,
            std::slice::from_ref(&summary_target),
        )
        .await
        .unwrap();
    maintenance
        .compaction_append_frozen_imports(
            "ws",
            "context-c",
            &projected_after_delete.manifest_id,
            0,
            &carried_after_delete,
        )
        .await
        .unwrap();
    assert!(
        maintenance
            .compaction_finish_frozen_history("ws", "context-c", &projected_after_delete)
            .await
            .unwrap()
    );
    let mut imported_checkpoint_deleted = projected_operation.clone();
    imported_checkpoint_deleted.id = "deleted-imported-checkpoint".into();
    imported_checkpoint_deleted.owner = "owner-deleted-imported-checkpoint".into();
    imported_checkpoint_deleted.plan.fingerprint = imported_checkpoint_deleted.id.clone();
    imported_checkpoint_deleted.projection_version = store
        .compaction_projection_version("ws", "context-c")
        .await
        .unwrap();
    imported_checkpoint_deleted.source_epochs.clear();
    for scope in ["child", "context-c", "thread"] {
        imported_checkpoint_deleted.source_epochs.insert(
            scope.into(),
            store
                .compaction_projection_version("ws", scope)
                .await
                .unwrap(),
        );
    }
    store
        .compaction_admit("ws", "context-c", &imported_checkpoint_deleted)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(&imported_checkpoint_deleted.id, "turn-c")
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&imported_checkpoint_deleted.id, &projected_after_delete)
        .await
        .unwrap();
    let imported_checkpoint_deleted_ready =
        ready_import_operation(&store, &imported_checkpoint_deleted, "child", &a_summary).await;
    assert!(
        store
            .compaction_manifest_sources_current(&imported_checkpoint_deleted.id)
            .await
            .unwrap(),
        "a deleted historical OWN leaf invalidated its accepted later summary"
    );
    assert_eq!(
        store
            .compaction_apply_runner(
                &imported_checkpoint_deleted.id,
                &imported_checkpoint_deleted_ready,
                None
            )
            .await
            .unwrap(),
        CommitOutcome::Applied
    );

    assert!(
        !store
            .compaction_finish_frozen_history("ws", "thread", &stale)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .compaction_frozen_import_page("ws", "thread", &context.manifest_id, 0)
            .await
            .unwrap(),
        records,
        "accepted metadata is retained"
    );
}

#[tokio::test]
async fn frozen_own_import_treats_published_summary_as_atomic_output() {
    use pioneer_compaction::frozen::{FrozenHistoryRef, FrozenMessageRef};
    use pioneer_compaction::runner::{RunnerState, SourceCursor};
    use sha2::{Digest, Sha256};

    fn descriptor(id: &str, messages: &[FrozenMessageRef]) -> FrozenHistoryRef {
        let mut digest = Sha256::new();
        for message in messages {
            let bytes = serde_json::to_vec(message).unwrap();
            digest.update((bytes.len() as u64).to_be_bytes());
            digest.update(bytes);
        }
        FrozenHistoryRef {
            format: 1,
            manifest_id: id.into(),
            messages: messages.len() as u64,
            identity_sha256: hex::encode(digest.finalize()),
        }
    }

    for delete_leaf in [false, true] {
        let store = store().await;
        let db = store.database_connection();
        for statement in [
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('portion-child','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('portion-turn','portion-child','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('portion-h','portion-child','portion-turn',1,'fixture','accepted H',CURRENT_TIMESTAMP)",
            "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('portion-task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
            "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('portion-run','portion-task','portion-run',1,1,'succeeded','agent')",
            "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('portion-rt','portion-task','portion-run','portion-child','portion-turn','initial',0,1,'completed',CURRENT_TIMESTAMP)",
            "INSERT INTO task_result_candidate(id,task_id,run_id,task_run_turn_id,thread_id,turn_id,round,status,created_at,updated_at) VALUES ('portion-candidate','portion-task','portion-run','portion-rt','portion-child','portion-turn',0,'accepted',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('portion-delivery-turn','thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,thread_target,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id) VALUES ('portion-delivery','ws','portion-task','portion-run','portion-key','thread','origin_thread','thread','delivered',1,1,'portion-delivery-turn')",
        ] {
            db.execute_unprepared(statement).await.unwrap();
        }
        let h_assertion = SourceAssertion {
            revision: Some(1),
            kind: CanonicalSource::Event,
            turn_id: "portion-turn".into(),
            id: "portion-h".into(),
            payload: "accepted H".into(),
        };
        let h = h_assertion.reference();
        let operation =
            admit_import_operation(&store, "portion-output", "portion-child", "portion-turn").await;
        let mut raw = operation.clone();
        raw.plan.compact = vec![0];
        raw.plan.coverage = vec![h.clone()];
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE compaction_operation SET snapshot=?2 WHERE id=?1",
            [
                operation.id.clone().into(),
                serde_json::to_string(&raw).unwrap().into(),
            ],
        ))
        .await
        .unwrap();
        let budget = ModelBudget::new(None, None, None);
        store
            .compaction_prepare_runner(&operation.id, &budget, 1, 0)
            .await
            .unwrap();
        store
            .compaction_append_manifest(
                &operation.id,
                &[ManifestEntry {
                    ordinal: 0,
                    unit: 0,
                    reference_only: false,
                    thread_id: "portion-child".into(),
                    source: h.clone(),
                }],
            )
            .await
            .unwrap();
        let initial =
            RunnerState::new(operation.admission.deadline_ms, &budget, 1000, None).unwrap();
        store
            .compaction_activate_runner(&operation.id, &initial)
            .await
            .unwrap();
        let first_attempt = initial.claim(1).unwrap();
        assert!(
            store
                .compaction_runner_transition(
                    &operation.id,
                    initial.generation,
                    &first_attempt,
                    None,
                )
                .await
                .unwrap()
        );
        let p = Checkpoint {
            id: "portion-p".into(),
            operation_id: operation.id.clone(),
            format_version: 1,
            owner: operation.owner.clone(),
            previous: None,
            coverage: vec![],
            summary: "partial prefix".into(),
            selection: operation.admission.selection.clone(),
            projection_version: operation.projection_version,
        };
        let p_state = first_attempt
            .candidate(
                1,
                p.id.clone(),
                SourceCursor {
                    character: 1,
                    ..Default::default()
                },
                false,
                2,
            )
            .unwrap();
        assert!(
            store
                .compaction_runner_transition(
                    &operation.id,
                    first_attempt.generation,
                    &p_state,
                    Some(&p),
                )
                .await
                .unwrap()
        );
        let next = p_state.candidate_checked(true).unwrap();
        assert!(
            store
                .compaction_runner_transition(&operation.id, p_state.generation, &next, None)
                .await
                .unwrap()
        );
        let second_attempt = next.claim(3).unwrap();
        assert!(
            store
                .compaction_runner_transition(
                    &operation.id,
                    next.generation,
                    &second_attempt,
                    None,
                )
                .await
                .unwrap()
        );
        let k = Checkpoint {
            id: "portion-k".into(),
            operation_id: operation.id.clone(),
            format_version: 1,
            owner: operation.owner.clone(),
            previous: Some(p.id.clone()),
            coverage: vec![h.clone()],
            summary: "complete H".into(),
            selection: operation.admission.selection.clone(),
            projection_version: operation.projection_version,
        };
        let k_state = second_attempt
            .candidate(
                2,
                k.id.clone(),
                SourceCursor {
                    unit: 1,
                    ..Default::default()
                },
                true,
                4,
            )
            .unwrap();
        assert!(
            store
                .compaction_runner_transition(
                    &operation.id,
                    second_attempt.generation,
                    &k_state,
                    Some(&k),
                )
                .await
                .unwrap()
        );
        let ready = k_state.candidate_checked(true).unwrap();
        assert!(
            store
                .compaction_runner_transition(&operation.id, k_state.generation, &ready, None)
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .compaction_apply_runner(&operation.id, &ready, None)
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
        let p_status = db
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT status FROM compaction_checkpoint WHERE id=?",
                [p.id.clone().into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<String>("", "status")
            .unwrap();
        assert_eq!(p_status, "retained");
        assert!(
            store
                .compaction_checkpoint_edges(&p.id)
                .await
                .unwrap()
                .unwrap()
                .coverage
                .is_empty()
        );
        let k_source = store
            .compaction_checkpoint_source("ws", "portion-child", &k.id)
            .await
            .unwrap()
            .unwrap();

        let output_message = FrozenMessageRef {
            logical_turn_id: None,
            context_thread: None,
            source_thread: "portion-child".into(),
            unit_id: "portion-output-unit".into(),
            sources: vec![k_source.clone()],
            event_input_role: None,
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
            publication_aliases: None,
            inherited: false,
            complete: true,
            protected_input: false,
            wire_sha256: "d".repeat(64),
            replay_source: None,
            tool_item_id: None,
            tool_call_id: None,
            tool_name: None,
        };
        let output = descriptor(
            "portion-output-manifest",
            std::slice::from_ref(&output_message),
        );
        store
            .compaction_begin_frozen_history("ws", "portion-child", &output)
            .await
            .unwrap();
        store
            .compaction_append_frozen_history(
                "ws",
                "portion-child",
                &output.manifest_id,
                0,
                std::slice::from_ref(&output_message),
            )
            .await
            .unwrap();
        assert!(
            store
                .compaction_finish_frozen_history("ws", "portion-child", &output)
                .await
                .unwrap()
        );
        store
            .compaction_record_task_output("ws", "portion-rt", &output)
            .await
            .unwrap();
        db.execute_unprepared("INSERT INTO compaction_delivery_output(delivery_id,candidate_id,task_run_turn_id) VALUES ('portion-delivery','portion-candidate','portion-rt')").await.unwrap();
        store
            .materialize_item_completed(
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: "portion-delivery-turn".into(),
                    item: pioneer_protocol::TurnItem::AgentMessage {
                        id: pioneer_protocol::task_delivery_result_item_id("portion-delivery"),
                        text: "delivered".into(),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                },
                chrono::Utc::now().timestamp(),
            )
            .await
            .unwrap();
        let acknowledgement = store
            .compaction_source_page(
                "ws",
                "thread",
                "portion-delivery-turn",
                PagedSource::Event,
                0,
            )
            .await
            .unwrap()
            .entries[0]
            .reference
            .clone();
        let prepared = store
            .compaction_prepare_frozen_import(
                "ws",
                "thread",
                "portion-delivery",
                &acknowledgement,
                0,
                "portion-child",
                &k_source,
            )
            .await
            .unwrap();
        assert_eq!(prepared.source(), &k_source);
        let imported_message = FrozenMessageRef {
            context_thread: Some("thread".into()),
            sources: vec![k_source.clone()],
            ..output_message.clone()
        };
        let imported = descriptor(
            "portion-import-manifest",
            std::slice::from_ref(&imported_message),
        );
        let imports = vec![(0, prepared)];
        let imports_digest = frozen_import_identity(&imports).unwrap();
        store
            .compaction_begin_frozen_history_with_imports(
                "ws",
                "thread",
                &imported,
                1,
                &imports_digest,
            )
            .await
            .unwrap();
        store
            .compaction_append_frozen_history(
                "ws",
                "thread",
                &imported.manifest_id,
                0,
                std::slice::from_ref(&imported_message),
            )
            .await
            .unwrap();
        store
            .compaction_append_frozen_imports("ws", "thread", &imported.manifest_id, 0, &imports)
            .await
            .unwrap();
        assert!(
            store
                .compaction_finish_frozen_history("ws", "thread", &imported)
                .await
                .unwrap()
        );

        db.execute_unprepared(
            "UPDATE compaction_checkpoint SET status='candidate' WHERE id='portion-p'",
        )
        .await
        .unwrap();
        let prepared_again = store
            .compaction_prepare_frozen_import(
                "ws",
                "thread",
                "portion-delivery",
                &acknowledgement,
                0,
                "portion-child",
                &k_source,
            )
            .await
            .unwrap();
        assert_eq!(
            prepared_again.source(),
            &k_source,
            "an unpublished predecessor invalidated an already published output summary"
        );
        let raw_error = store
            .compaction_prepare_frozen_import(
                "ws",
                "thread",
                "portion-delivery",
                &acknowledgement,
                0,
                "portion-child",
                &h,
            )
            .await
            .unwrap_err();
        assert_eq!(
            raw_error.to_string(),
            "source is outside the accepted own output message",
            "summary coverage granted direct access to its historical raw leaf"
        );
        db.execute_unprepared(if delete_leaf {
            "DELETE FROM turn_event WHERE id='portion-h'"
        } else {
            "UPDATE turn_event SET payload='changed H' WHERE id='portion-h'"
        })
        .await
        .unwrap();
        let prepared_after_leaf_change = store
            .compaction_prepare_frozen_import(
                "ws",
                "thread",
                "portion-delivery",
                &acknowledgement,
                0,
                "portion-child",
                &k_source,
            )
            .await
            .unwrap();
        assert_eq!(
            prepared_after_leaf_change.source(),
            &k_source,
            "a changed or deleted historical leaf invalidated the output summary"
        );

        // A later TaskRun accepts the descriptor containing S=K itself. Carry
        // that atomic grant onto a distinct T whose historical input is K.
        for statement in [
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('portion-target-thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('portion-consumer','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('portion-consumer','thread','thread',1,CURRENT_TIMESTAMP)",
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('portion-consumer-turn','portion-consumer','in_progress','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('portion-consumer-task','ws','thread','thread','thread','turn','agent','running','Consumer','fixture')",
            "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('portion-consumer-run','portion-consumer-task','portion-consumer-run',1,1,'running','agent')",
            "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('portion-consumer-rt','portion-consumer-task','portion-consumer-run','portion-consumer','portion-consumer-turn','initial',0,1,'running',CURRENT_TIMESTAMP)",
        ] {
            db.execute_unprepared(statement).await.unwrap();
        }
        db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('portion-target-turn','portion-target-thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        let mut target_op = admit_import_operation(
            &store,
            "portion-target-operation",
            "portion-target-thread",
            "portion-target-turn",
        )
        .await;
        target_op.plan.compact = vec![0];
        target_op.plan.coverage = vec![k_source.clone()];
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE compaction_operation SET snapshot=? WHERE id=?",
            [
                serde_json::to_string(&target_op).unwrap().into(),
                target_op.id.clone().into(),
            ],
        ))
        .await
        .unwrap();
        let target_checkpoint = Checkpoint {
            id: "portion-t".into(),
            operation_id: target_op.id.clone(),
            owner: target_op.owner.clone(),
            previous: None,
            coverage: vec![k_source.clone()],
            summary: "target T".into(),
            selection: target_op.admission.selection.clone(),
            projection_version: target_op.projection_version,
            format_version: 1,
        };
        store
            .compaction_save_candidate(&target_checkpoint, 0)
            .await
            .unwrap();
        // Model a pre-proof published legacy node with a real original raw
        // contract, exact identity and ownership, rather than malformed '{}'.
        db.execute_unprepared(
            "UPDATE compaction_checkpoint SET status='applied' WHERE id='portion-t'",
        )
        .await
        .unwrap();
        db.execute_unprepared("UPDATE compaction_operation SET status='completed' WHERE id='portion-target-operation'").await.unwrap();
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "WITH frozen_root_fixture(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) AS (VALUES ('portion-consumer-run','portion-consumer-task','ws','thread',?,CURRENT_TIMESTAMP)) INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,frozen_manifest_id) SELECT run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,CASE WHEN json_valid(history_json) THEN CASE WHEN json_type(history_json)='object' THEN json_extract(history_json,'$.manifest_id') ELSE NULL END ELSE NULL END FROM frozen_root_fixture",
            [serde_json::to_string(&imported).unwrap().into()],
        ))
        .await
        .unwrap();
        let t_source = store
            .compaction_checkpoint_source("ws", "portion-target-thread", "portion-t")
            .await
            .unwrap()
            .unwrap();
        let target_message = FrozenMessageRef {
            logical_turn_id: None,
            source_thread: "portion-target-thread".into(),
            context_thread: Some("portion-consumer".into()),
            unit_id: "portion-target-unit".into(),
            sources: vec![t_source.clone()],
            event_input_role: None,
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
            publication_aliases: None,
            inherited: false,
            complete: true,
            protected_input: false,
            wire_sha256: "e".repeat(64),
            replay_source: None,
            tool_item_id: None,
            tool_call_id: None,
            tool_name: None,
        };

        async fn begin_target(
            store: &CrudStore,
            id: &str,
            message: &FrozenMessageRef,
            prepared: PreparedFrozenImport,
        ) -> (FrozenHistoryRef, Vec<(u64, PreparedFrozenImport)>) {
            let target = descriptor(id, std::slice::from_ref(message));
            let imports = vec![(0, prepared)];
            let digest = frozen_import_identity(&imports).unwrap();
            store
                .compaction_begin_frozen_history_with_imports(
                    "ws",
                    "portion-consumer",
                    &target,
                    1,
                    &digest,
                )
                .await
                .unwrap();
            store
                .compaction_append_frozen_history(
                    "ws",
                    "portion-consumer",
                    &target.manifest_id,
                    0,
                    std::slice::from_ref(message),
                )
                .await
                .unwrap();
            (target, imports)
        }

        let unchanged = store
            .compaction_prepare_accepted_checkpoint_import(
                "ws",
                "portion-consumer",
                "portion-consumer-turn",
                0,
                "portion-target-thread",
                &t_source,
            )
            .await
            .expect("raw leaf mutation must not invalidate atomic S evidence");
        let (unchanged_target, unchanged_imports) =
            begin_target(&store, "portion-target-success", &target_message, unchanged).await;
        store
            .compaction_append_frozen_imports(
                "ws",
                "portion-consumer",
                &unchanged_target.manifest_id,
                0,
                &unchanged_imports,
            )
            .await
            .expect("an unchanged published S must permit the bounded append");

        let raced = store
            .compaction_prepare_accepted_checkpoint_import(
                "ws",
                "portion-consumer",
                "portion-consumer-turn",
                0,
                "portion-target-thread",
                &t_source,
            )
            .await
            .unwrap();
        let (raced_target, raced_imports) =
            begin_target(&store, "portion-target-raced", &target_message, raced).await;
        if delete_leaf {
            assert!(
                db.execute_unprepared("DELETE FROM compaction_checkpoint WHERE id='portion-k'")
                    .await
                    .is_err(),
                "sealed original grant evidence cannot be deleted"
            );
            // The accepted binding itself can disappear after preparation;
            // retain the immutable checkpoint and test that real writer race.
            db.execute_unprepared(
                "DELETE FROM task_run_conversation_snapshot WHERE run_id='portion-consumer-run'",
            )
            .await
            .unwrap();
        } else {
            db.execute_unprepared(
                "UPDATE compaction_checkpoint SET status='candidate' WHERE id='portion-k'",
            )
            .await
            .unwrap();
        }
        assert!(
            store
                .compaction_append_frozen_imports(
                    "ws",
                    "portion-consumer",
                    &raced_target.manifest_id,
                    0,
                    &raced_imports,
                )
                .await
                .is_err(),
            "writer accepted a checkpoint grant that changed after preparation"
        );
        let raced_state = db
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT next_import,(SELECT COUNT(*) FROM compaction_frozen_import i WHERE i.manifest_id=h.id) AS stored FROM compaction_frozen_history h WHERE h.id=?",
                [raced_target.manifest_id.into()],
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(raced_state.try_get::<i64>("", "next_import").unwrap(), 0);
        assert_eq!(raced_state.try_get::<i64>("", "stored").unwrap(), 0);
    }
}

async fn admit_import_operation(
    store: &CrudStore,
    id: &str,
    thread: &str,
    turn: &str,
) -> OperationSnapshot {
    let snapshot = OperationSnapshot {
        id: id.into(),
        owner: format!("owner-{id}"),
        expected_checkpoint: None,
        projection_version: store
            .compaction_projection_version("ws", thread)
            .await
            .unwrap(),
        source_epochs: std::collections::BTreeMap::new(),
        admission: CompactionSettings::default()
            .admit(
                &ModelSelection {
                    transport: Transport::Api,
                    instance: "p".into(),
                    model: "m".into(),
                    effort: None,
                },
                None,
                0,
            )
            .unwrap(),
        plan: CompactionPlan {
            mode: CompactionMode::Normal,
            coverage_domain: pioneer_compaction::CoverageDomain::OwnContribution,
            compact: vec![],
            retain: vec![],
            coverage: vec![],
            fingerprint: id.into(),
        },
    };
    store
        .compaction_admit("ws", thread, &snapshot)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(id, turn)
        .await
        .unwrap();
    snapshot
}

async fn ready_import_operation(
    store: &CrudStore,
    snapshot: &OperationSnapshot,
    source_thread: &str,
    source: &SourceRef,
) -> pioneer_compaction::runner::RunnerState {
    ready_operation(
        store,
        snapshot,
        &[(source_thread.to_owned(), source.clone())],
    )
    .await
}

async fn ready_operation(
    store: &CrudStore,
    snapshot: &OperationSnapshot,
    sources: &[(String, SourceRef)],
) -> pioneer_compaction::runner::RunnerState {
    ready_operation_with_references(store, snapshot, sources, &[]).await
}

async fn ready_operation_with_references(
    store: &CrudStore,
    snapshot: &OperationSnapshot,
    sources: &[(String, SourceRef)],
    references: &[(String, SourceRef)],
) -> pioneer_compaction::runner::RunnerState {
    use pioneer_compaction::runner::{RunnerState, SourceCursor};
    let op = &snapshot.id;
    // Seed the exact original legacy raw plan before runner execution. An
    // empty frozen-style plan is not a valid missing-binding contract.
    if !sources.is_empty()
        && snapshot.plan.compact.is_empty()
        && store
            .compaction_bound_source_projection(op)
            .await
            .unwrap()
            .is_none()
    {
        let mut raw = snapshot.clone();
        raw.plan.compact = (0..sources.len()).collect();
        raw.plan.coverage = sources.iter().map(|(_, source)| source.clone()).collect();
        store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE compaction_operation SET snapshot=?2 WHERE id=?1",
                [
                    op.clone().into(),
                    serde_json::to_string(&raw).unwrap().into(),
                ],
            ))
            .await
            .unwrap();
    }
    let budget = ModelBudget::new(None, None, None);
    store
        .compaction_prepare_runner(op, &budget, sources.len() as u64, references.len() as u64)
        .await
        .unwrap();
    let manifest = sources
        .iter()
        .map(|source| (false, source))
        .chain(references.iter().map(|source| (true, source)))
        .enumerate()
        .map(
            |(ordinal, (reference_only, (thread_id, source)))| ManifestEntry {
                ordinal: ordinal as u64,
                unit: ordinal as u64,
                reference_only,
                thread_id: thread_id.clone(),
                source: source.clone(),
            },
        )
        .collect::<Vec<_>>();
    store
        .compaction_append_manifest(op, &manifest)
        .await
        .unwrap();
    let initial = RunnerState::new(snapshot.admission.deadline_ms, &budget, 1000, None).unwrap();
    store
        .compaction_activate_runner(op, &initial)
        .await
        .unwrap();
    let attempt = initial.claim(1).unwrap();
    assert!(
        store
            .compaction_runner_transition(op, initial.generation, &attempt, None)
            .await
            .unwrap()
    );
    let checkpoint = Checkpoint {
        id: format!("checkpoint-{op}"),
        operation_id: op.clone(),
        format_version: 1,
        owner: snapshot.owner.clone(),
        previous: None,
        coverage: sources.iter().map(|(_, source)| source.clone()).collect(),
        summary: "fixture summary".into(),
        selection: snapshot.admission.selection.clone(),
        projection_version: snapshot.projection_version,
    };
    let next = attempt
        .candidate(
            1,
            checkpoint.id.clone(),
            SourceCursor {
                unit: sources.len() as u64,
                ..Default::default()
            },
            true,
            2,
        )
        .unwrap();
    assert!(
        store
            .compaction_runner_transition(op, attempt.generation, &next, Some(&checkpoint))
            .await
            .unwrap()
    );
    let ready = next.candidate_checked(true).unwrap();
    assert!(
        store
            .compaction_runner_transition(op, next.generation, &ready, None)
            .await
            .unwrap()
    );
    ready
}

#[tokio::test]
async fn runner_commit_treats_published_checkpoint_as_independent_after_admission() {
    for delete in [false, true] {
        let store = store().await;
        let mut leaf = source(&store, "checkpoint-leaf", 1, "accepted H").await;
        leaf.revision = Some(1);
        let checkpoint = candidate_with_manifest(&store, "source-checkpoint", None, &leaf).await;
        assert_eq!(
            store
                .compaction_apply(&checkpoint, None, std::slice::from_ref(&leaf))
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
        let mut later_leaf =
            source(&store, "later-checkpoint-leaf", 2, "later accepted work").await;
        later_leaf.revision = Some(1);
        let head = candidate_with_manifest(
            &store,
            "source-checkpoint-head",
            Some(&checkpoint.id),
            &later_leaf,
        )
        .await;
        assert_eq!(
            store
                .compaction_apply(
                    &head,
                    Some(&checkpoint.id),
                    std::slice::from_ref(&later_leaf)
                )
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
        let checkpoint_source = store
            .compaction_checkpoint_source("ws", "thread", &head.id)
            .await
            .unwrap()
            .unwrap();
        let operation =
            admit_import_operation(&store, "own-checkpoint-target", "thread", "turn").await;
        let ready = ready_import_operation(&store, &operation, "thread", &checkpoint_source).await;

        store
            .database_connection()
            .execute_unprepared(if delete {
                "DELETE FROM turn_event WHERE id='checkpoint-leaf'"
            } else {
                "UPDATE turn_event SET payload='changed H' WHERE id='checkpoint-leaf'"
            })
            .await
            .unwrap();
        assert!(
            store
                .compaction_manifest_sources_current(&operation.id)
                .await
                .unwrap(),
            "a published checkpoint must not depend on its historical leaf"
        );
        assert_eq!(
            store
                .compaction_apply_runner(&operation.id, &ready, None)
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
        let third = store
            .compaction_checkpoint(&format!("checkpoint-{}", operation.id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            third.coverage,
            vec![checkpoint_source],
            "S3 must record S2 as its atomic direct input"
        );
        let s3_edges = store
            .compaction_checkpoint_edges(&third.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(s3_edges.coverage.len(), 1);
        assert_eq!(s3_edges.coverage[0].source.id, head.id);
        assert_eq!(
            store
                .compaction_checkpoint_edges(&head.id)
                .await
                .unwrap()
                .unwrap()
                .previous
                .as_deref(),
            Some(checkpoint.id.as_str())
        );
        assert!(
            store
                .compaction_checkpoint_source("ws", "thread", &third.id)
                .await
                .unwrap()
                .is_some(),
            "S3 was not published after its predecessor's old leaf changed"
        );
    }
}

#[tokio::test]
async fn reference_only_checkpoint_does_not_revalidate_historical_leaves() {
    let store = store().await;
    let mut leaf = source(&store, "reference-leaf", 1, "reference H").await;
    leaf.revision = Some(1);
    let checkpoint = candidate(&store, "reference-checkpoint", None, &leaf).await;
    assert_eq!(
        store
            .compaction_apply(&checkpoint, None, std::slice::from_ref(&leaf))
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let checkpoint_source = store
        .compaction_checkpoint_source("ws", "thread", &checkpoint.id)
        .await
        .unwrap()
        .unwrap();
    let mut selected = source(&store, "selected-leaf", 2, "selected work").await;
    selected.revision = Some(1);
    let selected_source = selected.reference();
    let operation = admit_import_operation(&store, "reference-target", "thread", "turn").await;
    let ready = ready_operation_with_references(
        &store,
        &operation,
        &[("thread".into(), selected_source.clone())],
        &[("thread".into(), checkpoint_source)],
    )
    .await;
    assert!(
        store
            .compaction_manifest_sources_current(&operation.id)
            .await
            .unwrap(),
        "valid selected work plus a current reference-only checkpoint must reach commit validation"
    );
    let candidate = store
        .compaction_checkpoint(&format!("checkpoint-{}", operation.id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(candidate.coverage, vec![selected_source]);
    store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_event WHERE id='reference-leaf'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_manifest_sources_current(&operation.id)
            .await
            .unwrap(),
        "reference-only published checkpoint must survive historical deletion"
    );
    assert_eq!(
        store
            .compaction_apply_runner(&operation.id, &ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
}

#[tokio::test]
async fn compaction_lifecycle_after_terminal_turn_requires_exact_operation_and_generation() {
    use pioneer_crud::CanonicalTurnEventPayload as Event;
    let store = store().await;
    store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: "lifecycle-source".into(),
                    text: "original".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            1,
        )
        .await
        .unwrap();
    let reference = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    let op = admit_import_operation(&store, "lifecycle", "thread", "turn").await;
    let ready = ready_import_operation(&store, &op, "thread", &reference).await;
    let item = pioneer_protocol::TurnItem::SystemEvent {
        id: "compaction:lifecycle".into(),
        level: pioneer_protocol::SystemEventLevel::Info,
        message: "Context compaction started".into(),
        code: Some("agent_context_compaction".into()),
        details: None,
    };
    let started = pioneer_protocol::ItemStartedNotification {
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item: item.clone(),
    };
    let completed = pioneer_protocol::ItemCompletedNotification {
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item,
    };
    assert!(
        store
            .compaction_materialize_lifecycle(
                &op.id,
                ready.generation + 1,
                Event::ItemStarted(started.clone()),
                1
            )
            .await
            .is_err()
    );
    let mut wrong_scope = started.clone();
    wrong_scope.workspace_id = "other".into();
    assert!(
        store
            .compaction_materialize_lifecycle(
                &op.id,
                ready.generation,
                Event::ItemStarted(wrong_scope),
                1
            )
            .await
            .is_err()
    );
    let mut forged = started.clone();
    forged.item = pioneer_protocol::TurnItem::AgentMessage {
        id: "compaction:lifecycle".into(),
        text: "not a service event".into(),
        phase: Default::default(),
        markdown: None,
        markdown_version: None,
    };
    assert!(
        store
            .compaction_materialize_lifecycle(
                &op.id,
                ready.generation,
                Event::ItemStarted(forged),
                1
            )
            .await
            .is_err()
    );
    assert!(
        store
            .compaction_materialize_lifecycle(
                &op.id,
                ready.generation,
                Event::ItemCompleted(completed.clone()),
                1
            )
            .await
            .is_err(),
        "running operation cannot claim a terminal lifecycle"
    );
    let db = store.database_connection();
    db.execute_unprepared("CREATE TEMP TRIGGER abort_compaction_lifecycle BEFORE INSERT ON turn_event WHEN NEW.event_type='item/started' BEGIN SELECT RAISE(ABORT,'lifecycle append rollback'); END").await.unwrap();
    assert!(
        store
            .compaction_materialize_lifecycle(
                &op.id,
                ready.generation,
                Event::ItemStarted(started.clone()),
                1
            )
            .await
            .is_err()
    );
    db.execute_unprepared("DROP TRIGGER abort_compaction_lifecycle")
        .await
        .unwrap();
    // The fixture Turn is already completed. Only the operation-owned service
    // event, not an ordinary provider callback, is admitted by this API.
    store
        .compaction_materialize_lifecycle(
            &op.id,
            ready.generation,
            Event::ItemStarted(started.clone()),
            1,
        )
        .await
        .unwrap();
    store
        .compaction_materialize_lifecycle(
            &op.id,
            ready.generation,
            Event::ItemStarted(started.clone()),
            2,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_apply_runner(&op.id, &ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let applied = store
        .compaction_runner_state(&op.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .compaction_materialize_lifecycle(
                &op.id,
                applied.generation,
                Event::ItemStarted(started),
                3
            )
            .await
            .is_err()
    );
    assert!(
        store
            .compaction_materialize_lifecycle(
                &op.id,
                ready.generation,
                Event::ItemCompleted(completed.clone()),
                3
            )
            .await
            .is_err()
    );
    store
        .compaction_materialize_lifecycle(
            &op.id,
            applied.generation,
            Event::ItemCompleted(completed.clone()),
            3,
        )
        .await
        .unwrap();
    store
        .compaction_materialize_lifecycle(
            &op.id,
            applied.generation,
            Event::ItemCompleted(completed),
            4,
        )
        .await
        .unwrap();
    let row = db.query_one_raw(Statement::from_string(DbBackend::Sqlite,"SELECT count(*) AS n FROM turn_event WHERE turn_id='turn' AND event_type IN ('item/started','item/completed')".to_owned())).await.unwrap().unwrap();
    assert_eq!(
        row.try_get::<i64>("", "n").unwrap(),
        3,
        "baseline plus exactly two lifecycle events; retries add no duplicates"
    );
}

#[tokio::test]
async fn compaction_terminal_fence_reconciliation_persists_once_and_rejects_late_attempt() {
    use pioneer_compaction::runner::{FailureKind, RunnerPhase};
    let store = store().await;
    store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: "terminal-source".into(),
                    text: "original".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            1,
        )
        .await
        .unwrap();
    let reference = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    for (id, status, outcome, kind) in [
        ("stop", "cancelled", "cancelled", FailureKind::Cancelled),
        ("deadline", "failed", "deadline", FailureKind::Deadline),
    ] {
        let op = admit_import_operation(&store, id, "thread", "turn").await;
        let initial = ready_import_operation(&store, &op, "thread", &reference).await;
        store.compaction_finish(id, status, outcome).await.unwrap();
        let db = store.database_connection();
        db.execute_unprepared("CREATE TEMP TRIGGER abort_terminal_state BEFORE UPDATE ON compaction_runner_state BEGIN SELECT RAISE(ABORT,'terminal state rollback'); END").await.unwrap();
        assert!(store.compaction_reconcile_runner_state(id).await.is_err());
        assert_eq!(
            store
                .compaction_runner_state(id)
                .await
                .unwrap()
                .unwrap()
                .generation,
            initial.generation
        );
        db.execute_unprepared("DROP TRIGGER abort_terminal_state")
            .await
            .unwrap();
        let (a, b) = tokio::join!(
            store.compaction_reconcile_runner_state(id),
            store.compaction_reconcile_runner_state(id)
        );
        for state in [a.unwrap().unwrap(), b.unwrap().unwrap()] {
            assert_eq!(state.generation, initial.generation + 1);
            assert_eq!(state.phase, RunnerPhase::Failed { kind: kind.clone() });
        }
        let again = store
            .compaction_reconcile_runner_state(id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(again.generation, initial.generation + 1);
        assert!(
            !store
                .compaction_runner_transition(
                    id,
                    initial.generation,
                    &initial.terminate(FailureKind::Permanent).unwrap(),
                    None
                )
                .await
                .unwrap()
        );
        let event = pioneer_crud::CanonicalTurnEventPayload::ItemCompleted(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item: pioneer_protocol::TurnItem::SystemEvent {
                    id: format!("compaction:{id}"),
                    level: pioneer_protocol::SystemEventLevel::Info,
                    message: "Compaction finished".into(),
                    code: Some("agent_context_compaction".into()),
                    details: None,
                },
            },
        );
        assert!(
            store
                .compaction_materialize_lifecycle(id, initial.generation, event.clone(), 2)
                .await
                .is_err()
        );
        store
            .compaction_materialize_lifecycle(id, again.generation, event.clone(), 2)
            .await
            .unwrap();
        store
            .compaction_materialize_lifecycle(id, again.generation, event, 3)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn compaction_successful_apply_fences_prepared_lifecycle_timeout() {
    use pioneer_entity::compaction_lifecycle_pending as pending;
    use sea_orm::EntityTrait;
    let store = store().await;
    source(&store, "apply-race-source", 1, "original source").await;
    let reference = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    let op = admit_import_operation(&store, "apply-race", "thread", "turn").await;
    let ready = ready_import_operation(&store, &op, "thread", &reference).await;
    let candidate = pending::Entity::find_by_id(&op.id)
        .one(&store.database_connection())
        .await
        .unwrap()
        .unwrap();
    let now = i64::try_from(op.admission.deadline_ms).unwrap();
    let claim = store
        .compaction_claim_lifecycle(&candidate, &|| now)
        .await
        .unwrap()
        .unwrap();
    let timeout = store
        .compaction_prepare_lifecycle(&claim, now)
        .await
        .unwrap()
        .unwrap();
    assert!(!timeout.needs_publication());
    // Interleave the real atomic apply after timeout preparation, before its
    // writer admission. The old timeout must neither finish nor ACK this apply.
    assert_eq!(
        store
            .compaction_apply_runner(&op.id, &ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let after_apply = pending::Entity::find_by_id(&op.id)
        .one(&store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !store
            .compaction_repair_lifecycle(timeout, None, now / 1000)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .compaction_operation(&op.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "completed"
    );
    assert_eq!(
        store.compaction_head(&op.owner).await.unwrap().as_deref(),
        Some("checkpoint-apply-race")
    );
    assert_eq!(
        pending::Entity::find_by_id(&op.id)
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap(),
        after_apply
    );
    assert!(matches!(
        store
            .compaction_runner_state(&op.id)
            .await
            .unwrap()
            .unwrap()
            .phase,
        pioneer_compaction::runner::RunnerPhase::Applied { .. }
    ));
}

#[tokio::test]
async fn compaction_post_terminal_stop_survives_worker_loss_and_fences_new_admission() {
    let store = store().await;
    source(&store, "stop-source", 1, "original source").await;
    let reference = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    let op = admit_import_operation(&store, "durable-stop", "thread", "turn").await;
    let ready = ready_import_operation(&store, &op, "thread", &reference).await;
    let db = store.database_connection();
    assert!(
        store
            .compaction_stop_execution("wrong-workspace", "thread", &op.owner, "turn")
            .await
            .is_err()
    );
    db.execute_unprepared("CREATE TEMP TRIGGER abort_context_stop BEFORE INSERT ON compaction_execution_stop BEGIN SELECT RAISE(ABORT,'Stop rollback'); END").await.unwrap();
    assert!(
        store
            .compaction_stop_execution("ws", "thread", &op.owner, "turn")
            .await
            .is_err()
    );
    assert!(!store.compaction_execution_cancelled(&op.id).await.unwrap());
    db.execute_unprepared("DROP TRIGGER abort_context_stop")
        .await
        .unwrap();
    store
        .compaction_stop_execution("ws", "thread", &op.owner, "turn")
        .await
        .unwrap();
    store
        .compaction_stop_execution("ws", "thread", &op.owner, "turn")
        .await
        .unwrap();
    // No service reconciliation runs: model a lost worker with a ready candidate.
    assert!(store.compaction_execution_cancelled(&op.id).await.unwrap());
    assert_eq!(
        store
            .compaction_operation(&op.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    assert_eq!(
        store
            .compaction_apply_runner(&op.id, &ready, None)
            .await
            .unwrap(),
        CommitOutcome::Cancelled
    );
    assert!(store.compaction_head(&op.owner).await.unwrap().is_none());
    assert_eq!(
        store
            .get_turn("thread", "turn")
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        pioneer_protocol::TurnStatus::Completed
    );
    let mut replacement = op.clone();
    replacement.id = "after-stop-new-fingerprint".into();
    replacement.plan.fingerprint = replacement.id.clone();
    assert!(
        store
            .compaction_admit_for_turn("ws", "thread", &replacement, Some("turn"))
            .await
            .is_err()
    );
    assert!(
        store
            .compaction_operation(&replacement.id)
            .await
            .unwrap()
            .is_none()
    );
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('next-turn','thread','in_progress','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    store
        .compaction_admit_for_turn("ws", "thread", &replacement, Some("next-turn"))
        .await
        .unwrap();
    store
        .compaction_stop_execution("ws", "thread", &op.owner, "next-turn")
        .await
        .unwrap();
    assert!(
        store.compaction_execution_cancelled(&op.id).await.unwrap(),
        "later Stop must not erase an earlier stopped execution"
    );
    assert!(
        store
            .compaction_execution_cancelled(&replacement.id)
            .await
            .unwrap()
    );
    // Stop before any admission is also durable and cannot leave a running row.
    let mut unborn = replacement.clone();
    unborn.id = "never-admitted".into();
    unborn.owner = "never-admitted-owner".into();
    unborn.plan.fingerprint = unborn.id.clone();
    store
        .compaction_stop_execution("ws", "thread", &unborn.owner, "turn")
        .await
        .unwrap();
    assert!(
        store
            .compaction_admit_for_turn("ws", "thread", &unborn, Some("turn"))
            .await
            .is_err()
    );
    assert!(
        store
            .compaction_operation(&unborn.id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn legacy_task_snapshot_reference_is_bounded_scoped_and_revision_guarded() {
    let store = store().await;
    let db = store.database_connection();
    for sql in [
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run','task','run',1,1,'running','agent')",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    let body = serde_json::to_string(&vec!["память🦀".repeat(9000)]).unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "WITH frozen_root_fixture(run_id,task_id,workspace_id,conversation_thread_id,source_turn_id,history_json,created_at) AS (VALUES ('run','task','ws','thread','turn',?,CURRENT_TIMESTAMP)) INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,source_turn_id,history_json,created_at,frozen_manifest_id) SELECT run_id,task_id,workspace_id,conversation_thread_id,source_turn_id,history_json,created_at,CASE WHEN json_valid(history_json) THEN CASE WHEN json_type(history_json)='object' THEN json_extract(history_json,'$.manifest_id') ELSE NULL END ELSE NULL END FROM frozen_root_fixture", [body.clone().into()])).await.unwrap();
    let source = store
        .compaction_legacy_task_basis_source("ws", "thread", "run")
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .compaction_legacy_task_basis_source("other", "thread", "run")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_reference_fragment("ws", "other", &source, 0)
            .await
            .unwrap()
            .is_none()
    );
    let mut restored = String::new();
    let mut offset = 0;
    loop {
        let fragment = store
            .compaction_reference_fragment("ws", "thread", &source, offset)
            .await
            .unwrap()
            .unwrap();
        assert!(fragment.text.len() <= 64 * 1024);
        restored.push_str(&fragment.text);
        match fragment.next_character {
            Some(next) => offset = next,
            None => break,
        }
    }
    assert_eq!(restored, body);
    let epoch = store
        .compaction_projection_version("ws", "thread")
        .await
        .unwrap();
    db.execute_unprepared("UPDATE task_run_conversation_snapshot SET history_json=history_json||' ' WHERE run_id='run'").await.unwrap();
    assert!(
        store
            .compaction_reference_fragment("ws", "thread", &source, 0)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap()
            > epoch
    );
    let current = store
        .compaction_legacy_task_basis_source("ws", "thread", "run")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(current.version, source.version);
    db.execute_unprepared("DELETE FROM task_run_conversation_snapshot WHERE run_id='run'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_reference_thread("ws", &current)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn completed_cli_check_is_atomic_bounded_and_captures_settings_once() {
    let store = store().await;
    let db = store.database_connection();
    db.execute_unprepared("INSERT INTO turn_cli_runtime_binding(turn_id,thread_id,continuation_thread_id,workspace_id,runtime_id,runtime_kind,native_thread_id,status,model,frozen_manifest_id) VALUES('turn','thread','thread','ws','claude','claude','native','running','sonnet',NULL)").await.unwrap();
    assert!(
        store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty()
    );
    db.execute_unprepared(
        "UPDATE turn SET status='in_progress',reasoning_effort='high' WHERE id='turn'",
    )
    .await
    .unwrap();
    {
        use sea_orm::TransactionTrait;
        let tx = db.begin().await.unwrap();
        tx.execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn'")
            .await
            .unwrap();
        tx.rollback().await.unwrap();
    }
    assert!(
        store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty()
    );
    db.execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn'")
        .await
        .unwrap();
    let pending = store.compaction_pending_history_checks().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert!(
        store
            .compaction_history_check_is_current("turn")
            .await
            .unwrap()
    );
    assert_eq!(pending[0].reasoning_effort.as_deref(), Some("high"));
    assert_eq!(pending[0].model.as_deref(), Some("sonnet"));
    assert_eq!(
        store
            .compaction_capture_history_check("turn", "original selection and deadline")
            .await
            .unwrap()
            .as_deref(),
        Some("original selection and deadline")
    );
    assert_eq!(
        store
            .compaction_capture_history_check("turn", "changed settings after restart")
            .await
            .unwrap()
            .as_deref(),
        Some("original selection and deadline")
    );
    assert!(
        store
            .compaction_capture_history_check("turn", &"x".repeat(16385))
            .await
            .is_err()
    );
    store
        .compaction_stop_execution("ws", "thread", "owner", "turn")
        .await
        .unwrap();
    db.execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .compaction_capture_history_check("turn", "restart")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn compaction_timeline_persists_explicit_terminal_status_with_one_item() {
    let store = store().await;
    for terminal in ["completed", "failed", "cancelled"] {
        let id = format!("compaction:{terminal}");
        let item = |status: &str| pioneer_protocol::TurnItem::SystemEvent {
            id: id.clone(),
            level: pioneer_protocol::SystemEventLevel::Info,
            message: status.into(),
            code: Some("agent_context_compaction".into()),
            details: Some(serde_json::json!({"status":status})),
        };
        store
            .materialize_item_started(
                pioneer_protocol::ItemStartedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: "turn".into(),
                    item: item("started"),
                },
                1,
            )
            .await
            .unwrap();
        let query = || {
            Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT status FROM turn_work_item_projection WHERE item_id=?",
                vec![id.clone().into()],
            )
        };
        let row = store
            .database_connection()
            .query_one_raw(query())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.try_get::<String>("", "status").unwrap(), "running");
        for _ in 0..2 {
            store
                .materialize_item_completed(
                    pioneer_protocol::ItemCompletedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "thread".into(),
                        turn_id: "turn".into(),
                        item: item(terminal),
                    },
                    2,
                )
                .await
                .unwrap();
        }
        let rows = store
            .database_connection()
            .query_all_raw(query())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].try_get::<String>("", "status").unwrap(), terminal);
    }
}

#[tokio::test]
async fn compaction_migration_resumes_partial_ddl_and_preserves_legacy_data_on_reapply() {
    use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
    let connection = Database::connect("sqlite::memory:").await.unwrap();
    let writer = SqliteWriteExecutor::new(connection.clone());
    let migrations = Migrator::migrations();
    let name = "m20260910_000001_context_compaction";
    let before = migrations.iter().position(|m| m.name() == name).unwrap();
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, Some(before as u32))
        .await
        .unwrap();
    let store = CrudStore::new(SqliteDatabase::from_executor(connection, writer.clone()))
        .with_maintenance_access();
    let db = store.database_connection();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        // Durable prefix of an interrupted metadata migration, before its marker.
        "CREATE TABLE compaction_history_check (turn_id TEXT PRIMARY KEY NOT NULL REFERENCES turn(id) ON DELETE CASCADE, state TEXT NOT NULL DEFAULT 'pending', descriptor TEXT, outcome TEXT)",
        "INSERT INTO compaction_history_check(turn_id,state,outcome) VALUES ('turn','done','retained-before-retry')",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    store
        .update_thread_summary("thread", "retained old summary", 42)
        .await
        .unwrap();
    for _ in 0..2 {
        writer
            .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, Some(1))
            .await
            .unwrap();
        assert_eq!(
            store.get_thread_summary("thread").await.unwrap(),
            Some(("retained old summary".into(), 42))
        );
        let row = db
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT state,outcome FROM compaction_history_check WHERE turn_id='turn'",
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.try_get::<String>("", "state").unwrap(), "done");
        assert_eq!(
            row.try_get::<String>("", "outcome").unwrap(),
            "retained-before-retry"
        );
        // Re-execute the actual migration, not just Migrator's marker fast path.
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "DELETE FROM seaql_migrations WHERE version=?",
            [name.into()],
        ))
        .await
        .unwrap();
    }
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    source(&store, "after-migration", 1, "retained original").await;
    let page = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 1);
}

#[tokio::test]
async fn independent_summary_migration_reuses_existing_checkpoint_without_rewrite() {
    use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    for compressed in [false, true] {
        let connection = Database::connect("sqlite::memory:").await.unwrap();
        let writer = SqliteWriteExecutor::new(connection.clone());
        let migrations = Migrator::migrations();
        let name = "m20260920_000001_independent_compaction_summaries";
        let before = migrations
            .iter()
            .position(|migration| migration.name() == name)
            .unwrap();
        writer
            .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, Some(before as u32))
            .await
            .unwrap();
        let store = CrudStore::new(SqliteDatabase::from_executor(connection, writer.clone()))
            .with_maintenance_access();
        let db = store.database_connection();
        for sql in [
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('upgrade-ws','fixture',1,1)",
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('upgrade-thread','upgrade-ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('upgrade-turn','upgrade-thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('upgrade-leaf','upgrade-thread','upgrade-turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
            "INSERT INTO compaction_context(workspace_id,thread_id,owner,format_version) VALUES ('upgrade-ws','upgrade-thread','upgrade-owner',1)",
            "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES ('upgrade-operation','upgrade-owner','upgrade','completed','{}',1)",
            "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES ('upgrade-operation',0,0,0,'upgrade-thread','event:upgrade-turn','upgrade-leaf','event-revision:1')",
            "INSERT INTO compaction_projection_epoch(thread_id,version) VALUES ('upgrade-thread',7) ON CONFLICT(thread_id) DO UPDATE SET version=7",
        ] {
            db.execute_unprepared(sql).await.unwrap();
        }
        let selection = serde_json::to_string(&ModelSelection {
            transport: Transport::Api,
            instance: "upgrade-instance".into(),
            model: "upgrade-model".into(),
            effort: None,
        })
        .unwrap();
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES ('upgrade-summary','upgrade-operation','upgrade-owner',0,'saved before upgrade','stable-identity',?,0,1,'applied')",
            [selection.into()],
        ))
        .await
        .unwrap();
        db.execute_unprepared("INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES ('upgrade-summary','event:upgrade-turn','upgrade-leaf','event-revision:1')")
            .await
            .unwrap();
        let raw = OperationSnapshot {
            id: "upgrade-operation".into(),
            owner: "upgrade-owner".into(),
            expected_checkpoint: None,
            projection_version: 0,
            source_epochs: Default::default(),
            admission: CompactionSettings::default()
                .admit(
                    &ModelSelection {
                        transport: Transport::Api,
                        instance: "upgrade-instance".into(),
                        model: "upgrade-model".into(),
                        effort: None,
                    },
                    None,
                    0,
                )
                .unwrap(),
            plan: CompactionPlan {
                mode: CompactionMode::Normal,
                coverage_domain: CoverageDomain::OwnContribution,
                compact: vec![0],
                retain: vec![],
                coverage: vec![SourceRef {
                    scope: "event:upgrade-turn".into(),
                    id: "upgrade-leaf".into(),
                    version: "event-revision:1".into(),
                }],
                fingerprint: "upgrade".into(),
            },
        };
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE compaction_operation SET snapshot=?1 WHERE id='upgrade-operation'",
            [serde_json::to_string(&raw).unwrap().into()],
        ))
        .await
        .unwrap();
        if compressed {
            let config = serde_json::json!({
                "table":"turn_event",
                "column":"payload",
                "compression_level":3,
                "dict_chooser":"'[nodict]'"
            });
            db.query_one_write_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT zstd_enable_transparent(?)",
                [config.to_string().into()],
            ))
            .await
            .unwrap();
            let payload = b"{}";
            let payload = pioneer_sqlite::zstd::compress_column_value(payload, 3, None).unwrap();
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE _turn_event_zstd SET payload=?,_payload_dict=-1 WHERE id='upgrade-leaf'",
                [payload.into()],
            ))
            .await
            .unwrap();
        }
        db.execute_unprepared("DELETE FROM turn_event WHERE id='upgrade-leaf'")
            .await
            .unwrap();
        let reference = SourceRef {
            scope: "checkpoint:upgrade-owner".into(),
            id: "upgrade-summary".into(),
            version: "stable-identity".into(),
        };
        let live_before = db
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT count(*) AS n FROM compaction_live_sources WHERE source_id='upgrade-summary'"
                    .to_owned(),
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<i64>("", "n")
            .unwrap();
        assert_eq!(live_before, 0);

        writer
            .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
            .await
            .unwrap();

        let live_after = db
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT count(*) AS n FROM compaction_live_sources WHERE source_id='upgrade-summary'"
                    .to_owned(),
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<i64>("", "n")
            .unwrap();
        assert_eq!(live_after, 1);
        assert!(
            store
                .compaction_sources_current(
                    "upgrade-ws",
                    "upgrade-thread",
                    std::slice::from_ref(&reference),
                )
                .await
                .unwrap()
        );
        let checkpoint = store
            .compaction_checkpoint("upgrade-summary")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.summary, "saved before upgrade");
        assert_eq!(
            checkpoint.coverage,
            vec![SourceRef {
                scope: "event:upgrade-turn".into(),
                id: "upgrade-leaf".into(),
                version: "event-revision:1".into(),
            }]
        );
        assert_eq!(reference.version, "stable-identity");
        let edges = store
            .compaction_checkpoint_edges("upgrade-summary")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(edges.coverage.len(), 1);
        assert_eq!(edges.coverage[0].source_thread, "upgrade-thread");
        assert_eq!(edges.coverage[0].source.id, "upgrade-leaf");
    }
}

#[tokio::test]
async fn compaction_schema_preserves_byte_checks_keys_and_creation_sequence() {
    let store = store().await;
    let db = store.database_connection();
    db.execute_unprepared(
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count) VALUES ('manifest','ws','thread','fixture',1)",
    ).await.unwrap();
    // The CHECK measures UTF-8 bytes, not characters, and enforces the upper bound.
    for (table, columns, values) in [
        (
            "compaction_frozen_message_data",
            "manifest_id,ordinal,reference_json,bytes",
            "'manifest',0,?,?",
        ),
        (
            "compaction_frozen_import_data",
            "manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes",
            "'manifest',0,0,'scope','source','v1','thread',?,?",
        ),
    ] {
        let insert = format!("INSERT INTO {table}({columns}) VALUES ({values})");
        for (payload, bytes) in [
            ("я".to_owned(), 1_i64),
            ("".into(), -1),
            ("x".repeat(262145), 262145),
        ] {
            assert!(
                db.execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    insert.clone(),
                    [payload.into(), bytes.into()],
                ))
                .await
                .is_err()
            );
        }
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            insert.clone(),
            ["я".into(), 2_i64.into()],
        ))
        .await
        .unwrap();
        assert!(
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                insert,
                ["я".into(), 2_i64.into()],
            ))
            .await
            .is_err(),
            "composite primary key must reject duplicate ordinals"
        );
    }
    assert!(db.execute_unprepared("INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES ('manifest',1,0,'scope','source','v1','thread','{}',2)").await.is_err(), "source identity must remain unique within the manifest message");
    assert!(db.execute_unprepared("INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES ('missing',0,'{}',2)").await.is_err());
    db.execute_unprepared("DELETE FROM compaction_frozen_history WHERE id='manifest'")
        .await
        .unwrap();
    for table in [
        "compaction_frozen_message_data",
        "compaction_frozen_import_data",
    ] {
        let row = db
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                format!("SELECT count(*) AS n FROM {table}"),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.try_get::<i64>("", "n").unwrap(),
            0,
            "manifest deletion must cascade"
        );
    }
    let previous = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT sequence FROM compaction_turn_creation WHERE turn_id='turn'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "sequence")
        .unwrap();
    db.execute_unprepared("DELETE FROM compaction_turn_creation WHERE turn_id='turn'")
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO compaction_turn_creation(turn_id) VALUES ('turn')")
        .await
        .unwrap();
    let next = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT sequence FROM compaction_turn_creation WHERE turn_id='turn'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "sequence")
        .unwrap();
    assert!(
        next > previous,
        "AUTOINCREMENT must not reuse deleted creation ordinals"
    );
}

#[tokio::test]
async fn old_summary_is_not_a_canonical_source_or_projection_dependency() {
    let store = store().await;
    let epoch = store
        .compaction_projection_version("ws", "thread")
        .await
        .unwrap();
    store
        .update_thread_summary("thread", "obsolete text", 900)
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        epoch
    );
    let row = store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT count(*) AS n FROM compaction_live_sources WHERE source_scope LIKE 'legacy:%'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
    let source = SourceRef {
        scope: "legacy:thread".into(),
        id: "thread".into(),
        version: "legacy-revision:1".into(),
    };
    assert!(
        store
            .compaction_reference_fragment("ws", "thread", &source, 0)
            .await
            .is_err()
    );
}

fn shared_refs(count: usize) -> Vec<pioneer_compaction::frozen::FrozenMessageRef> {
    (0..count)
        .map(|i| pioneer_compaction::frozen::FrozenMessageRef {
            logical_turn_id: Some("turn".into()),
            context_thread: None,
            source_thread: "thread".into(),
            unit_id: format!("u{i}"),
            sources: vec![SourceRef {
                scope: "event:turn".into(),
                id: format!("e{i}"),
                version: "event-revision:1".into(),
            }],
            event_input_role: None,
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
            publication_aliases: None,
            inherited: false,
            complete: true,
            protected_input: false,
            wire_sha256: "a".repeat(64),
            replay_source: None,
            tool_item_id: None,
            tool_call_id: None,
            tool_name: None,
        })
        .collect()
}
fn shared_descriptor(
    id: &str,
    refs: &[pioneer_compaction::frozen::FrozenMessageRef],
) -> pioneer_compaction::frozen::FrozenHistoryRef {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    for r in refs {
        let b = serde_json::to_vec(r).unwrap();
        digest.update((b.len() as u64).to_be_bytes());
        digest.update(b);
    }
    pioneer_compaction::frozen::FrozenHistoryRef {
        format: 1,
        manifest_id: id.into(),
        messages: refs.len() as u64,
        identity_sha256: hex::encode(digest.finalize()),
    }
}
async fn shared_capture(
    store: &CrudStore,
    id: &str,
    refs: &[pioneer_compaction::frozen::FrozenMessageRef],
    shared: bool,
) -> pioneer_compaction::frozen::FrozenHistoryRef {
    let d = shared_descriptor(id, refs);
    store
        .compaction_begin_frozen_history("ws", "thread", &d)
        .await
        .unwrap();
    let start = if shared {
        store
            .compaction_share_frozen_prefix("ws", "thread", id, refs, &[])
            .await
            .unwrap()
            .0 as usize
    } else {
        0
    };
    for (i, page) in refs[start..].chunks(128).enumerate() {
        store
            .compaction_append_frozen_history("ws", "thread", id, (start + i * 128) as u64, page)
            .await
            .unwrap();
    }
    assert!(
        store
            .compaction_finish_frozen_history("ws", "thread", &d)
            .await
            .unwrap()
    );
    d
}
async fn shared_read(
    store: &CrudStore,
    id: &str,
) -> Vec<pioneer_compaction::frozen::FrozenMessageRef> {
    let mut result = Vec::new();
    loop {
        let page = store
            .compaction_frozen_history_page("ws", "thread", id, result.len() as u64)
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        result.extend(page);
    }
    result
}
async fn frozen_count(store: &CrudStore, table: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            format!("SELECT count(*) AS n FROM {table}"),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}
#[tokio::test]
async fn shared_frozen_growth_is_linear_and_old_revisions_are_unchanged() {
    let store = store().await;
    let refs = shared_refs(400);
    for n in (10..=400).step_by(10) {
        shared_capture(&store, &format!("s{n:04}"), &refs[..n], true).await;
    }
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        400
    );
    assert_eq!(frozen_count(&store, "compaction_frozen_span").await, 40);
    assert_eq!(shared_read(&store, "s0010").await, refs[..10]);
    assert_eq!(shared_read(&store, "s0400").await, refs);
    let plan = store.database_connection().query_all_raw(Statement::from_string(DbBackend::Sqlite,
        "EXPLAIN QUERY PLAN SELECT reference_json FROM compaction_frozen_message WHERE manifest_id='s0400' AND ordinal>=200 AND ordinal<210 ORDER BY ordinal"))
        .await.unwrap().into_iter().map(|row|row.try_get::<String>("","detail").unwrap()).collect::<Vec<_>>();
    assert!(
        !plan.iter().any(|line| line.starts_with("SCAN d")),
        "range read must not scan the full payload table: {plan:?}"
    );

    let d = shared_descriptor("unused", &refs);
    let found = store
        .compaction_equivalent_frozen_history("ws", "thread", &d, 0, EMPTY_FROZEN_IMPORT_SHA256)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.manifest_id, "s0400");
    assert!(
        store
            .compaction_equivalent_frozen_history(
                "foreign",
                "thread",
                &d,
                0,
                EMPTY_FROZEN_IMPORT_SHA256
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_equivalent_frozen_history("ws", "thread", &d, 1, &"b".repeat(64))
            .await
            .unwrap()
            .is_none()
    );
    let mut changed = refs.clone();
    changed[200].wire_sha256 = "b".repeat(64);
    shared_capture(&store, "branch", &changed, true).await;
    assert_eq!(shared_read(&store, "branch").await, changed);
    assert_eq!(shared_read(&store, "s0400").await, refs);
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        600
    );
    store
        .database_connection()
        .execute_unprepared("DELETE FROM thread WHERE id='thread'")
        .await
        .unwrap();
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        0
    );
    assert_eq!(frozen_count(&store, "compaction_frozen_span").await, 0);
}
#[tokio::test]
async fn legacy_frozen_duplicates_convert_incrementally_without_changing_ids_or_reads() {
    let mut frozen_conversion_progress = pioneer_crud::FrozenStorageLifetimeProgress::default();
    let store = store().await;
    let refs = shared_refs(300);
    for id in ["a", "b", "c"] {
        shared_capture(&store, id, &refs, false).await;
    }
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        900
    );
    // Reusing a published legacy capture must not hide it from conversion.
    assert_eq!(
        store
            .compaction_share_frozen_prefix("ws", "thread", "b", &refs, &[])
            .await
            .unwrap(),
        (300, 0)
    );

    let mut quanta = 0;
    while store
        .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
        .await
        .unwrap()
    {
        quanta += 1;
        assert!(quanta < 512);
        for id in ["a", "b", "c"] {
            assert_eq!(shared_read(&store, id).await, refs);
        }
    }
    assert!(quanta > 10);
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        300
    );
    assert_eq!(frozen_count(&store, "compaction_frozen_history").await, 3);
    let descriptor = shared_descriptor("b", &refs);
    assert_eq!(
        store
            .compaction_frozen_history_owner("ws", &descriptor)
            .await
            .unwrap(),
        Some("thread".into())
    );
    assert!(
        !store
            .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn old_layout_sizes_and_new_shared_payload_keep_exact_logical_stream() {
    let store = store().await;
    let refs = shared_refs(260);
    shared_capture(&store, "base", &refs, true).await;
    let descriptor = shared_capture(&store, "legacy", &refs, false).await;
    let sizes = store.database_connection().query_all_raw(Statement::from_string(DbBackend::Sqlite,"SELECT ordinal,bytes FROM compaction_frozen_message WHERE manifest_id='legacy' ORDER BY ordinal")).await.unwrap();
    assert_eq!(sizes.len(), refs.len());
    let mut progress = pioneer_crud::FrozenStorageLifetimeProgress::default();
    for _ in 0..512 {
        store
            .compact_frozen_storage_quantum(&mut progress)
            .await
            .unwrap();
    }
    assert_eq!(shared_read(&store, "legacy").await, refs);
    let after = store.database_connection().query_all_raw(Statement::from_string(DbBackend::Sqlite,"SELECT ordinal,bytes FROM compaction_frozen_message WHERE manifest_id='legacy' ORDER BY ordinal")).await.unwrap();
    assert_eq!(
        sizes
            .iter()
            .map(|r| (
                r.try_get::<i64>("", "ordinal").unwrap(),
                r.try_get::<i64>("", "bytes").unwrap()
            ))
            .collect::<Vec<_>>(),
        after
            .iter()
            .map(|r| (
                r.try_get::<i64>("", "ordinal").unwrap(),
                r.try_get::<i64>("", "bytes").unwrap()
            ))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        260
    );
    assert_eq!(
        store
            .compaction_frozen_history_owner("ws", &descriptor)
            .await
            .unwrap(),
        Some("thread".into())
    );
}

#[tokio::test]
async fn duplicate_cleanup_rejects_understated_physical_bytes_after_layout_switch() {
    let store = store().await;
    let refs = shared_refs(2);
    shared_capture(&store, "base", &refs, true).await;
    shared_capture(&store, "legacy", &refs, false).await;
    let mut progress = pioneer_crud::FrozenStorageLifetimeProgress::default();
    let mut activated = false;
    for _ in 0..128 {
        store
            .compact_frozen_storage_quantum(&mut progress)
            .await
            .unwrap();
        let row=store.database_connection().query_one_raw(Statement::from_string(DbBackend::Sqlite,"SELECT active,cleanup_next,cleanup_to FROM compaction_frozen_layout WHERE manifest_id='legacy' AND kind=0")).await.unwrap();
        if let Some(row) = row {
            if row.try_get::<i64>("", "active").unwrap() == 1
                && row.try_get::<i64>("", "cleanup_next").unwrap() == 0
                && row.try_get::<i64>("", "cleanup_to").unwrap() == 2
            {
                activated = true;
                break;
            }
        }
    }
    assert!(
        activated,
        "fixture must pause after real activation and before cleanup"
    );
    // Seed legacy corruption only; tested cleanup runs with CHECKs restored.
    let oversized = serde_json::json!({"padding":"x".repeat(300_000)}).to_string();
    let tx = store.database_connection().begin().await.unwrap();
    tx.execute_unprepared("PRAGMA ignore_check_constraints=ON")
        .await
        .unwrap();
    tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"UPDATE compaction_frozen_message_data SET reference_json=?1,bytes=2 WHERE manifest_id='legacy' AND ordinal=0",[oversized.into()])).await.unwrap();
    tx.execute_unprepared("PRAGMA ignore_check_constraints=OFF")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(
        store
            .compact_frozen_storage_quantum(&mut progress)
            .await
            .is_err()
    );
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        4
    );
    assert_eq!(shared_read(&store, "base").await, refs);
}

#[tokio::test]
async fn restart_staged_copy_next_does_not_publish_wrong_backing() {
    let store = store().await;
    let refs = shared_refs(2);
    shared_capture(&store, "base", &refs, true).await;
    shared_capture(&store, "staged", &refs, false).await;
    store.database_connection().execute_unprepared("INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending,candidate,compared,copy_to,copy_next) VALUES('staged',0,0,1,'base',2,2,2)").await.unwrap();
    // Plausible old progress with corrupt physical source. Direct rows still
    // exist, so rejecting activation must keep the old logical layout usable.
    store.database_connection().execute_unprepared("INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('staged',0,0,2,'staged')").await.unwrap();
    let mut progress = pioneer_crud::FrozenStorageLifetimeProgress::default();
    let mut rejected = false;
    for _ in 0..128 {
        if store
            .compact_frozen_storage_quantum(&mut progress)
            .await
            .is_err()
        {
            rejected = true;
            break;
        }
    }
    assert!(rejected);
    let state=store.database_connection().query_one_raw(Statement::from_string(DbBackend::Sqlite,"SELECT active,failed FROM compaction_frozen_layout WHERE manifest_id='staged' AND kind=0")).await.unwrap().unwrap();
    assert_eq!(state.try_get::<i64>("", "active").unwrap(), 0);
    assert_eq!(state.try_get::<i64>("", "failed").unwrap(), 1);
    assert!(
        store
            .compaction_frozen_history_page("ws", "thread", "staged", 0)
            .await
            .is_err(),
        "corrupt layout is explicit, not empty history"
    );
    store.database_connection().execute_unprepared(
        "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending,failed) VALUES('staged',1,0,1,1)"
    ).await.unwrap();
    assert!(
        store
            .compaction_frozen_import_page("ws", "thread", "staged", 0)
            .await
            .is_err(),
        "a failed import layout is explicit even when its stream is empty"
    );
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        4
    );
    assert_eq!(shared_read(&store, "base").await, refs);
}

#[tokio::test]
async fn shared_frozen_append_rollback_and_concurrent_retry_preserve_one_sequence() {
    let store = store().await;
    let refs = shared_refs(25);
    let d = shared_descriptor("concurrent", &refs);
    store
        .compaction_begin_frozen_history("ws", "thread", &d)
        .await
        .unwrap();
    store
        .compaction_share_frozen_prefix("ws", "thread", &d.manifest_id, &refs, &[])
        .await
        .unwrap();
    let db = store.database_connection();
    db.execute_unprepared("CREATE TEMP TRIGGER reject_shared_append BEFORE INSERT ON compaction_frozen_message_data BEGIN SELECT RAISE(ABORT,'fixture rollback'); END").await.unwrap();
    assert!(
        store
            .compaction_append_frozen_history("ws", "thread", &d.manifest_id, 0, &refs)
            .await
            .is_err()
    );
    assert_eq!(frozen_count(&store, "compaction_frozen_span").await, 0);
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        0
    );
    assert!(
        !store
            .compaction_finish_frozen_history("ws", "thread", &d)
            .await
            .unwrap()
    );
    db.execute_unprepared("DROP TRIGGER reject_shared_append")
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        shared_capture(&store, "concurrent", &refs, true),
        shared_capture(&store, "concurrent", &refs, true)
    );
    assert_eq!(a, b);
    assert_eq!(shared_read(&store, "concurrent").await, refs);
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        25
    );
    assert_eq!(frozen_count(&store, "compaction_frozen_span").await, 1);
}

#[tokio::test]
async fn shared_frozen_cleanup_rollback_and_poison_row_do_not_lose_other_history() {
    let mut frozen_conversion_progress = pioneer_crud::FrozenStorageLifetimeProgress::default();
    let store = store().await;
    let refs = shared_refs(20);
    shared_capture(&store, "a", &refs, true).await;
    shared_capture(&store, "b", &refs, false).await;
    let db = store.database_connection();
    db.execute_unprepared("CREATE TEMP TRIGGER reject_shared_cleanup BEFORE DELETE ON compaction_frozen_message_data BEGIN SELECT RAISE(ABORT,'fixture cleanup rollback'); END").await.unwrap();
    let mut failed = false;
    for _ in 0..128 {
        if store
            .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
            .await
            .is_err()
        {
            failed = true;
            break;
        }
    }
    assert!(failed);
    assert_eq!(shared_read(&store, "b").await, refs);
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        40
    );
    db.execute_unprepared("DROP TRIGGER reject_shared_cleanup")
        .await
        .unwrap();
    for _ in 0..128 {
        if !store
            .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
            .await
            .unwrap()
        {
            break;
        }
    }
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        20
    );
    shared_capture(&store, "c-broken", &refs, false).await;
    shared_capture(&store, "d-good", &refs, false).await;
    db.execute_unprepared(
        "DELETE FROM compaction_frozen_message_data WHERE manifest_id='c-broken' AND ordinal=3",
    )
    .await
    .unwrap();
    let mut rejected = 0;
    for _ in 0..128 {
        match store
            .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
            .await
        {
            Ok(false) => break,
            Ok(true) => {}
            Err(_) => rejected += 1,
        }
    }
    assert_eq!(rejected, 1);
    assert_eq!(shared_read(&store, "d-good").await, refs);
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        39
    );
}

#[tokio::test]
async fn history_check_retries_are_durable_bounded_and_cas_protected() {
    use pioneer_crud::compaction::{HistoryCheckDiagnostic as D, HistoryCheckOutcome as O};
    use pioneer_entity::compaction_history_check as check;
    use sea_orm::EntityTrait;
    let store = store().await.with_maintenance_access();
    store
        .database_connection()
        .execute_unprepared("UPDATE turn SET status='in_progress' WHERE id='turn'")
        .await
        .unwrap();
    store
        .compaction_enqueue_native_history_check("ws", "thread", "turn", "{}")
        .await
        .unwrap();
    store
        .database_connection()
        .execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn'")
        .await
        .unwrap();
    let mut now = 1000;
    for (failure, delay) in [60_000, 300_000, 900_000, 0].into_iter().enumerate() {
        let page = store.compaction_due_history_checks(now).await.unwrap();
        assert_eq!(page.len(), 1);
        let claim = store
            .compaction_claim_history_check("turn", page[0].revision, now)
            .await
            .unwrap()
            .unwrap();
        assert!(
            store
                .compaction_claim_history_check("turn", page[0].revision, now)
                .await
                .unwrap()
                .is_none()
        );
        let deadline = store
            .compaction_begin_history_attempt("turn", claim.revision, "{}", "hash", now)
            .await
            .unwrap()
            .unwrap();
        // A new worker after shutdown inherits the exact attempt deadline.
        let restarted = store
            .compaction_claim_history_check("turn", claim.revision, now + 10)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .compaction_begin_history_attempt(
                    "turn",
                    restarted.revision,
                    "{}",
                    "hash",
                    now + 10
                )
                .await
                .unwrap(),
            Some(deadline)
        );
        let mut d = D::new(
            "history_capture",
            "database_error",
            "Temporary database failure",
        );
        d.observed_ms = now as u64;
        assert!(
            !store
                .compaction_record_history_result(
                    "turn",
                    claim.revision,
                    failure as i64,
                    O::Retryable,
                    &d,
                    now
                )
                .await
                .unwrap()
        );
        assert!(
            store
                .compaction_record_history_result(
                    "turn",
                    restarted.revision,
                    failure as i64,
                    O::Retryable,
                    &d,
                    now
                )
                .await
                .unwrap()
        );
        assert!(
            !store
                .compaction_record_history_result(
                    "turn",
                    restarted.revision,
                    failure as i64,
                    O::Retryable,
                    &d,
                    now
                )
                .await
                .unwrap()
        );
        let saved = check::Entity::find_by_id("turn")
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.failures, failure as i64 + 1);
        assert_eq!(
            serde_json::from_str::<D>(saved.diagnostic.as_ref().unwrap()).unwrap(),
            d
        );
        assert!(saved.attempt_deadline_ms.is_none());
        if delay == 0 {
            assert_eq!(saved.state, "finished");
            assert_eq!(saved.outcome.as_deref(), Some("failed"));
            assert!(
                store
                    .compaction_due_history_checks(i64::MAX)
                    .await
                    .unwrap()
                    .is_empty()
            );
        } else {
            assert_eq!(saved.next_attempt_ms, now + delay);
            assert!(
                store
                    .compaction_due_history_checks(now + delay - 1)
                    .await
                    .unwrap()
                    .is_empty()
            );
            now += delay;
        }
    }
}

#[tokio::test]
async fn history_check_waits_do_not_spend_retries_and_stop_wins_over_stale_results() {
    use pioneer_crud::compaction::{HistoryCheckDiagnostic as D, HistoryCheckOutcome as O};
    use pioneer_entity::compaction_history_check as check;
    use sea_orm::EntityTrait;
    let store = store().await.with_maintenance_access();
    store
        .database_connection()
        .execute_unprepared("UPDATE turn SET status='in_progress' WHERE id='turn'")
        .await
        .unwrap();
    store
        .compaction_enqueue_native_history_check("ws", "thread", "turn", "{}")
        .await
        .unwrap();
    store
        .database_connection()
        .execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn'")
        .await
        .unwrap();
    let mut now = 0;
    for outcome in [
        O::Preparing,
        O::WaitingCatalog,
        O::WaitingExecutor,
        O::WaitingSettings,
    ] {
        let row = store
            .compaction_due_history_checks(now)
            .await
            .unwrap()
            .remove(0);
        let claim = store
            .compaction_claim_history_check("turn", row.revision, now)
            .await
            .unwrap()
            .unwrap();
        store
            .compaction_begin_history_attempt("turn", claim.revision, "{}", "original", now)
            .await
            .unwrap();
        let d = D::new("preparation", outcome.as_str(), "Waiting for readiness");
        store
            .compaction_record_history_result("turn", claim.revision, 0, outcome, &d, now)
            .await
            .unwrap();
        let row = check::Entity::find_by_id("turn")
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.failures, 0);
        assert_eq!(row.config_hash.as_deref(), Some("original"));
        assert!(
            store
                .compaction_due_history_checks(now + 59_999)
                .await
                .unwrap()
                .is_empty()
        );
        now += 60_000;
    }
    let row = store
        .compaction_due_history_checks(now)
        .await
        .unwrap()
        .remove(0);
    let claim = store
        .compaction_claim_history_check("turn", row.revision, now)
        .await
        .unwrap()
        .unwrap();
    store
        .compaction_stop_execution("ws", "thread", "owner", "turn")
        .await
        .unwrap();
    assert!(
        !store
            .compaction_record_history_result(
                "turn",
                claim.revision,
                0,
                O::Retryable,
                &D::default(),
                now
            )
            .await
            .unwrap()
    );
    assert!(
        store
            .compaction_due_history_checks(i64::MAX)
            .await
            .unwrap()
            .is_empty()
    );
    let row = check::Entity::find_by_id("turn")
        .one(&store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.outcome.as_deref(), Some("cancelled"));
}

#[tokio::test]
async fn history_check_legacy_failures_are_reconciled_once_without_reviving_cancelled_jobs() {
    use pioneer_crud::compaction::{HistoryCheckDiagnostic as D, HistoryCheckOutcome as O};
    let store = store().await.with_maintenance_access();
    let db = store.database_connection();
    db.execute_unprepared("UPDATE turn SET status='in_progress' WHERE id='turn'")
        .await
        .unwrap();
    store
        .compaction_enqueue_native_history_check("ws", "thread", "turn", "{}")
        .await
        .unwrap();
    db.execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn'")
        .await
        .unwrap();
    db.execute_unprepared("UPDATE compaction_history_check SET state='finished',outcome='failed' WHERE turn_id='turn'").await.unwrap();
    let row = store
        .compaction_due_history_checks(0)
        .await
        .unwrap()
        .remove(0);
    let claim = store
        .compaction_claim_history_check("turn", row.revision, 0)
        .await
        .unwrap()
        .unwrap();
    assert!(
        serde_json::from_str::<D>(claim.diagnostic.as_ref().unwrap())
            .unwrap()
            .legacy_reason_unknown,
        "legacy marker must survive a crash before the result is recorded"
    );
    let d = D {
        legacy_reason_unknown: true,
        ..D::new("budget", "history_fits", "No summary needed")
    };
    store
        .compaction_record_history_result("turn", claim.revision, 0, O::Fits, &d, 0)
        .await
        .unwrap();
    assert!(
        store
            .compaction_due_history_checks(i64::MAX)
            .await
            .unwrap()
            .is_empty()
    );
    db.execute_unprepared("UPDATE compaction_history_check SET managed=0,state='finished',outcome='cancelled' WHERE turn_id='turn'").await.unwrap();
    assert!(
        store
            .compaction_due_history_checks(i64::MAX)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn history_check_legacy_pages_discard_superseded_turns_and_make_progress() {
    let store = store().await.with_maintenance_access();
    let db = store.database_connection();
    for n in 0..33 {
        db.execute_unprepared(&format!("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('legacy-{n:02}','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)")).await.unwrap();
        db.execute_unprepared(&format!("INSERT INTO compaction_history_check(turn_id,state,outcome) VALUES ('legacy-{n:02}','finished','failed')")).await.unwrap();
    }
    for expected in [16, 16, 1] {
        let page = store.compaction_due_history_checks(0).await.unwrap();
        assert_eq!(page.len(), expected);
        for row in page {
            let claim = store
                .compaction_claim_history_check(&row.turn_id, row.revision, 0)
                .await
                .unwrap();
            if row.turn_id == "legacy-32" {
                let claim = claim.unwrap();
                store
                    .compaction_record_history_result(
                        &row.turn_id,
                        claim.revision,
                        0,
                        HistoryCheckOutcome::Failed,
                        &HistoryCheckDiagnostic::new(
                            "descriptor",
                            "invalid_descriptor",
                            "Malformed metadata quarantined",
                        ),
                        0,
                    )
                    .await
                    .unwrap();
            } else {
                assert!(
                    claim.is_none(),
                    "newer turn must invalidate an old failed check"
                );
            }
        }
    }
    assert!(
        store
            .compaction_due_history_checks(i64::MAX)
            .await
            .unwrap()
            .is_empty()
    );
}

// Frozen history lifetime regressions.
async fn checkpoint_proof_version(store: &CrudStore, id: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT proof_version FROM compaction_checkpoint WHERE id=?1",
            [id.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "proof_version")
        .unwrap()
}
#[tokio::test]
async fn raw_assertion_publication_seals_exact_empty_proofs_and_immutable_ownership() {
    let store = store().await;
    let assertion = source(&store, "raw-proof-source", 1, "original").await;
    let cp = candidate(&store, "raw-proof-op", None, &assertion).await;
    assert_eq!(checkpoint_proof_version(&store, &cp.id).await, 0);
    let ownership = store
        .compaction_manifest_page(&cp.operation_id, false, 0, 0)
        .await
        .unwrap();
    assert_eq!(ownership.len(), 1);
    assert_eq!(ownership[0].source, assertion.reference());
    assert_eq!(ownership[0].thread_id, "thread");
    assert_eq!(
        store
            .compaction_apply(&cp, None, &[assertion.clone()])
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(checkpoint_proof_version(&store, &cp.id).await, 1);
    let db = store.database_connection();
    for table in [
        "compaction_checkpoint_replay_alias",
        "compaction_checkpoint_event_input",
        "compaction_checkpoint_import",
    ] {
        let n: i64 = db
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                format!("SELECT count(*) AS n FROM {table} WHERE checkpoint_id=?1"),
                [cp.id.clone().into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap();
        assert_eq!(n, 0, "empty set is proven by marker, not row presence");
    }
    for sql in [
        "UPDATE compaction_checkpoint SET proof_version=0 WHERE id=?1",
        "UPDATE compaction_checkpoint SET previous='forged' WHERE id=?1",
        "DELETE FROM compaction_coverage WHERE checkpoint_id=?1",
        "INSERT INTO compaction_checkpoint_event_input(checkpoint_id,source_thread,source_scope,source_id,source_version,role) VALUES(?1,'thread','event:turn','forged','event-revision:1','deleted')",
    ] {
        assert!(
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                sql,
                [cp.id.clone().into()]
            ))
            .await
            .is_err(),
            "sealed evidence/links must be immutable"
        );
    }
    assert!(
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "DELETE FROM compaction_manifest WHERE operation_id=?1",
            [cp.operation_id.clone().into()]
        ))
        .await
        .is_err()
    );
    assert_eq!(
        store
            .compaction_apply(&cp, None, &[assertion])
            .await
            .unwrap(),
        CommitOutcome::AlreadyApplied
    );
}
#[tokio::test]
async fn raw_assertion_missing_historical_ownership_is_not_empty_proofs() {
    let store = store().await;
    let assertion = source(&store, "lost-owner-source", 1, "original").await;
    // Model an unsealed legacy checkpoint whose ownership was already lost,
    // before the new candidate writers' atomic immutable portion boundary.
    // A post-commit DELETE is now correctly rejected by the production guard.
    let cp = candidate_admission_fixture(
        &store,
        "lost-owner-op",
        None,
        &assertion,
        Default::default(),
        false,
    )
    .await;
    use sha2::{Digest, Sha256};
    let identity = hex::encode(Sha256::digest(serde_json::to_vec(&cp).unwrap()));
    let db = store.database_connection();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES(?1,?2,?3,0,?4,?5,?6,?7,1,'candidate')",
        [cp.id.clone().into(),cp.operation_id.clone().into(),cp.owner.clone().into(),cp.summary.clone().into(),identity.into(),serde_json::to_string(&cp.selection).unwrap().into(),(cp.projection_version as i64).into()])).await.unwrap();
    let r = assertion.reference();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES(?1,?2,?3,?4)",
        [cp.id.clone().into(),r.scope.into(),r.id.into(),r.version.into()])).await.unwrap();
    assert!(
        store
            .compaction_apply(&cp, None, &[assertion])
            .await
            .is_err()
    );
    assert_eq!(checkpoint_proof_version(&store, &cp.id).await, 0);
    assert_eq!(store.compaction_head("owner").await.unwrap(), None);
}
#[tokio::test]
async fn extra_staging_evidence_prevents_seal_and_publication_without_overwrite() {
    let store = store().await;
    let assertion = source(&store, "staging-source", 1, "original").await;
    let cp = candidate(&store, "staging-op", None, &assertion).await;
    store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO compaction_checkpoint_event_input(checkpoint_id,source_thread,source_scope,source_id,source_version,role) VALUES(?1,'thread','event:turn','poison','event-revision:1','deleted')",[cp.id.clone().into()])).await.unwrap();
    for _ in 0..2 {
        assert!(
            store
                .compaction_apply(&cp, None, &[assertion.clone()])
                .await
                .is_err()
        );
        assert_eq!(checkpoint_proof_version(&store, &cp.id).await, 0);
    }
    assert_eq!(store.compaction_head("owner").await.unwrap(), None);
    let n: i64 = store
        .database_connection()
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT count(*) AS n FROM compaction_checkpoint_event_input WHERE checkpoint_id=?1",
            [cp.id.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert_eq!(n, 1, "conflicting staging is not silently repaired");
}
#[tokio::test]
async fn checkpoint_alias_null_identity_is_distinct_from_empty_tool_identity() {
    let store = store().await;
    let assertion = source(&store, "alias-source", 1, "original").await;
    let cp = candidate(&store, "alias-op", None, &assertion).await;
    let sql = "INSERT INTO compaction_checkpoint_replay_alias(checkpoint_id,covered_thread,covered_scope,covered_id,covered_version,replay_thread,replay_scope,replay_id,replay_version,tool_item_id) VALUES(?1,'thread','event:turn','source','event-revision:1','thread','context:turn','replay','revision:1',?2)";
    for tool in [None, Some(String::new())] {
        store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                sql,
                [cp.id.clone().into(), tool.into()],
            ))
            .await
            .unwrap();
    }
    assert!(
        store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                sql,
                [cp.id.clone().into(), Option::<String>::None.into()]
            ))
            .await
            .is_err()
    );
    let n: i64 = store
        .database_connection()
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT count(*) AS n FROM compaction_checkpoint_replay_alias WHERE checkpoint_id=?1",
            [cp.id.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert_eq!(n, 2);
}

#[tokio::test]
async fn deadline_saved_ready_candidate_commit_expiry_refuses_resume_without_changing_generation() {
    use pioneer_compaction::runner::{AttemptPurpose, FailureKind, RunnerPhase, RunnerState};
    for phase_name in ["Ready", "Candidate", "Commit"] {
        let store = store().await;
        let assertion = source(&store, "resume-source", 1, "original").await;
        let cp = candidate_fixture(
            &store,
            "resume-op",
            None,
            &assertion,
            std::collections::BTreeMap::new(),
            true,
        )
        .await;
        let descriptor = shared_descriptor("resume-origin", &[]);
        store
            .compaction_begin_frozen_history("ws", "thread", &descriptor)
            .await
            .unwrap();
        let hold = store
            .compaction_finish_frozen_history_held("ws", "thread", &descriptor)
            .await
            .unwrap();
        let db = store.database_connection();
        db.execute_unprepared("INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) SELECT 'resume-op',id,identity_sha256,imports_sha256,import_count FROM compaction_frozen_history WHERE id='resume-origin'").await.unwrap();
        let phase = match phase_name {
            "Ready" => RunnerPhase::Ready {
                purpose: AttemptPurpose::Portion,
            },
            "Candidate" => RunnerPhase::Candidate {
                checkpoint: cp.id.clone(),
                final_portion: true,
            },
            _ => RunnerPhase::Commit {
                checkpoint: cp.id.clone(),
            },
        };
        let state = RunnerState {
            generation: 0,
            deadline_ms: 1000,
            attempts: 1,
            retries: 0,
            corrections: 0,
            target_tokens: 10,
            source_text_projection_version: 0,
            cursor: pioneer_compaction::runner::SourceCursor {
                unit: 1,
                ..Default::default()
            },
            previous_checkpoint: Some(cp.id.clone()),
            phase: RunnerPhase::Failed {
                kind: FailureKind::Deadline,
            },
            resume_phase: Some(phase),
            observation: None,
            diagnostic: None,
        };
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO compaction_runner_state(operation_id,generation,state) VALUES('resume-op',0,?1)",[serde_json::to_string(&state).unwrap().into()])).await.unwrap();
        db.execute_unprepared("UPDATE compaction_operation SET status='failed',outcome='deadline',deadline_ms=1000 WHERE id='resume-op'").await.unwrap();
        drop(hold);
        if phase_name == "Commit" {
            assert!(
                store
                    .compaction_resume_deadline("resume-op", "turn", 2000)
                    .await
                    .unwrap(),
                "resume first creates a running origin root"
            );
            let mut p = pioneer_crud::FrozenStorageLifetimeProgress::default();
            for _ in 0..500 {
                let _ = p.quantum(&store).await;
            }
            assert!(
                store
                    .compaction_bound_source_projection("resume-op")
                    .await
                    .unwrap()
                    .is_some()
            );
            // Return to proven deadline terminal state at the new generation.
            let mut terminal = store
                .compaction_runner_state("resume-op")
                .await
                .unwrap()
                .unwrap();
            terminal.generation += 1;
            terminal.phase = RunnerPhase::Failed {
                kind: FailureKind::Deadline,
            };
            terminal.resume_phase = Some(RunnerPhase::Commit {
                checkpoint: cp.id.clone(),
            });
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"UPDATE compaction_runner_state SET generation=?1,state=?2 WHERE operation_id='resume-op'",[(terminal.generation as i64).into(),serde_json::to_string(&terminal).unwrap().into()])).await.unwrap();
            db.execute_unprepared("UPDATE compaction_operation SET status='failed',outcome='deadline' WHERE id='resume-op'").await.unwrap();
        }
        // Isolated fixture expiry models the durable winner before dispatch.
        db.execute_unprepared(
            "UPDATE compaction_frozen_history SET expired=1 WHERE id='resume-origin'",
        )
        .await
        .unwrap();
        let before = store
            .compaction_runner_state("resume-op")
            .await
            .unwrap()
            .unwrap();
        let error = store
            .compaction_resume_deadline("resume-op", "turn", 4000)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("expired"), "{phase_name}");
        assert_eq!(
            store
                .compaction_runner_state("resume-op")
                .await
                .unwrap()
                .unwrap(),
            before
        );
        assert_eq!(
            store
                .compaction_operation("resume-op")
                .await
                .unwrap()
                .unwrap()
                .status,
            "failed"
        );
    }
}

#[tokio::test]
async fn legacy_large_raw_admission_and_coverage_are_paged_not_poisoned_by_aggregate_bytes() {
    let recorded: RecordedStatements = Default::default();
    let store = store_recording_statements(Some(recorded.clone())).await;
    let mut assertions = Vec::new();
    for i in 0..128 {
        assertions.push(
            source(
                &store,
                &format!("raw-{i:04}-{}", "x".repeat(3072)),
                i + 1,
                "{}",
            )
            .await,
        );
    }
    let mut cp = candidate_admission_fixture(
        &store,
        "raw-large-op",
        None,
        &assertions[0],
        Default::default(),
        false,
    )
    .await;
    cp.coverage = assertions.iter().map(SourceAssertion::reference).collect();
    let db = store.database_connection();
    let row = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT snapshot FROM compaction_operation WHERE id='raw-large-op'",
        ))
        .await
        .unwrap()
        .unwrap();
    let mut snapshot: OperationSnapshot =
        serde_json::from_str(&row.try_get::<String>("", "snapshot").unwrap()).unwrap();
    snapshot.plan.coverage = cp.coverage.clone();
    snapshot.plan.compact = (0..cp.coverage.len()).collect();
    let json = serde_json::to_string(&snapshot).unwrap();
    assert!(json.len() > SOURCE_PAGE_BYTES);
    // Original legacy admission predates the small current admission envelope.
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_operation SET snapshot=?1 WHERE id='raw-large-op'",
        [json.into()],
    ))
    .await
    .unwrap();
    recorded.lock().unwrap().clear();
    store.compaction_save_candidate(&cp, 0).await.unwrap();
    assert_eq!(
        store
            .compaction_checkpoint(&cp.id)
            .await
            .unwrap()
            .unwrap()
            .coverage,
        cp.coverage
    );
    assert_eq!(
        store
            .compaction_checkpoint_edges(&cp.id)
            .await
            .unwrap()
            .unwrap()
            .coverage
            .len(),
        128
    );
    assert_eq!(
        store
            .compaction_apply(&cp, None, &assertions)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(checkpoint_proof_version(&store, &cp.id).await, 1);
    let statements = recorded.lock().unwrap();
    let fragments = statements
        .iter()
        .filter(|s| s.sql.contains("substr(CAST(o.snapshot AS BLOB)"))
        .count();
    assert!(
        fragments >= 4,
        "both raw preparation and proof extraction must read multiple fragments"
    );
    assert!(
        !statements
            .iter()
            .any(|s| s.sql.starts_with("SELECT owner,snapshot")),
        "no full snapshot payload read"
    );
}
