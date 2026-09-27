use migration::{Migrator, MigratorTrait};
use pioneer_compaction::*;
use pioneer_crud::{CrudStore, compaction::*};
use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};

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
    SourceAssertion {
        revision: None,
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
    store
        .compaction_save_candidate(&checkpoint, 0)
        .await
        .unwrap();
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
            .is_empty(),
        "legacy assertion publication has no manifest"
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
async fn selected_source_edit_fences_runner_commit() {
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
            compact: vec![],
            retain: vec![],
            coverage: vec![],
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
            CommitOutcome::Stale
        );
        assert!(
            store
                .compaction_head("runner-owner")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .compaction_checkpoint("runner-candidate")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .compaction_checkpoint_source("ws", "thread", "runner-candidate")
                .await
                .unwrap()
                .is_none(),
            "ancestry prepared before a failed CAS must not become a live checkpoint"
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
    // Normal thread deletion must keep its existing cascade contract.
    store
        .database_connection()
        .execute_unprepared("DELETE FROM thread WHERE id='thread'")
        .await
        .unwrap();
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
    db.execute_unprepared("INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) VALUES ('run','task','ws','thread','not read by metadata lookup',CURRENT_TIMESTAMP)").await.unwrap();
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
        "UPDATE task_run_conversation_snapshot SET history_json=? WHERE run_id='run'",
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

#[tokio::test]
async fn delivered_output_discovery_advances_empty_bounded_quanta() {
    let store = store().await;
    for n in 1..=260 {
        source(&store, &format!("irrelevant-{n}"), n, "{}").await;
    }
    let fence = store.compaction_history_read_fence().await.unwrap();
    let first = store
        .compaction_delivered_output_page("ws", "thread", 0, &fence)
        .await
        .unwrap();
    assert!(first.entries.is_empty());
    assert_eq!(first.scanned_through, 128);
    assert!(!first.done);
    let second = store
        .compaction_delivered_output_page("ws", "thread", first.scanned_through, &fence)
        .await
        .unwrap();
    assert!(second.entries.is_empty());
    assert_eq!(second.scanned_through, 256);
    assert!(!second.done);
    source(&store, "late", 261, "{}").await;
    let last = store
        .compaction_delivered_output_page("ws", "thread", second.scanned_through, &fence)
        .await
        .unwrap();
    assert!(last.entries.is_empty());
    assert_eq!(last.scanned_through, fence.event_order);
    assert!(last.done);
}

#[tokio::test]
async fn frozen_own_imports_require_exact_output_membership_and_atomic_publication() {
    use pioneer_compaction::frozen::{FrozenHistoryRef, FrozenMessageRef};
    use sha2::{Digest, Sha256};
    fn descriptor(id: &str, messages: &[FrozenMessageRef]) -> FrozenHistoryRef {
        let mut digest = Sha256::new();
        for message in messages {
            let bytes = serde_json::to_vec(message).unwrap();
            digest.update((bytes.len() as u64).to_le_bytes());
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
        if !store.compact_frozen_storage_quantum().await.unwrap() {
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
        "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) VALUES ('run-c','task','ws','thread',?,CURRENT_TIMESTAMP)",
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
    let mixed_op = admit_import_operation(&store, "mixed-summary", "child", "child-turn").await;
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
    let mixed_ready = ready_import_operation(&store, &mixed_target, "child", &mixed_source).await;
    assert!(
        !store
            .compaction_manifest_sources_current(&mixed_target.id)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .compaction_apply_runner(&mixed_target.id, &mixed_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Stale
    );
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
        "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) VALUES ('run-d','task-d','ws','thread',?,CURRENT_TIMESTAMP)",
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
    let own_ready = ready_import_operation(&store, &own_target, "context-c", &working_source).await;
    assert!(
        !store
            .compaction_manifest_sources_current(&own_target.id)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .compaction_apply_runner(&own_target.id, &own_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Stale
    );
    let denied = admit_import_operation(&store, "unaccepted-summary", "context-c", "turn-c").await;
    let denied_ready = ready_import_operation(&store, &denied, "child", &a_summary).await;
    assert!(
        !store
            .compaction_manifest_sources_current(&denied.id)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .compaction_apply_runner(&denied.id, &denied_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Stale
    );
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
        "UPDATE task_run_conversation_snapshot SET history_json='[]' WHERE run_id='run-c'",
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
        "UPDATE task_run_conversation_snapshot SET history_json=? WHERE run_id='run-c'",
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
    // Header identity is pinned with the operation, so mutation cannot widen
    // its authority between admission and final publication.
    db.execute_unprepared(
        "UPDATE compaction_frozen_history SET imports_sha256='changed' WHERE id='assembled'",
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .compaction_apply_runner(&operation.id, &ready, None)
            .await
            .unwrap(),
        CommitOutcome::Stale
    );
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_frozen_history SET imports_sha256=? WHERE id='assembled'",
        [import_digest.clone().into()],
    ))
    .await
    .unwrap();
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
    let unbound_ready = ready_import_operation(&store, &unbound, "child", &own_source).await;
    assert_eq!(
        store
            .compaction_apply_runner(&unbound.id, &unbound_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Stale
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
    let h_ready = ready_import_operation(&store, &h_operation, "thread", &inherited).await;
    assert_eq!(
        store
            .compaction_apply_runner(&h_operation.id, &h_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Stale,
        "accepted H remains reference-only, even in the same accepted manifest"
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
        CommitOutcome::Stale
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
            digest.update((bytes.len() as u64).to_le_bytes());
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
            "INSERT INTO compaction_context(workspace_id,thread_id,owner,format_version) VALUES ('ws','portion-target-thread','portion-target-owner',1)",
            "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES ('portion-target-operation','portion-target-owner','portion-target','completed','{}',1)",
            "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES ('portion-t','portion-target-operation','portion-target-owner',0,'target T','portion-t-version','{}',0,1,'applied')",
            "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) SELECT 'portion-t',source_scope,source_id,source_version FROM compaction_live_sources WHERE source_id='portion-k'",
            "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) SELECT 'portion-target-operation',0,0,0,'portion-child',source_scope,source_id,source_version FROM compaction_live_sources WHERE source_id='portion-k'",
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('portion-consumer','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('portion-consumer','thread','thread',1,CURRENT_TIMESTAMP)",
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('portion-consumer-turn','portion-consumer','in_progress','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('portion-consumer-task','ws','thread','thread','thread','turn','agent','running','Consumer','fixture')",
            "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('portion-consumer-run','portion-consumer-task','portion-consumer-run',1,1,'running','agent')",
            "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('portion-consumer-rt','portion-consumer-task','portion-consumer-run','portion-consumer','portion-consumer-turn','initial',0,1,'running',CURRENT_TIMESTAMP)",
        ] {
            db.execute_unprepared(statement).await.unwrap();
        }
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) VALUES ('portion-consumer-run','portion-consumer-task','ws','thread',?,CURRENT_TIMESTAMP)",
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
        db.execute_unprepared(if delete_leaf {
            "DELETE FROM compaction_checkpoint WHERE id='portion-k'"
        } else {
            "UPDATE compaction_checkpoint SET status='candidate' WHERE id='portion-k'"
        })
        .await
        .unwrap();
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
        "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,source_turn_id,history_json,created_at) VALUES ('run','task','ws','thread','turn',?,CURRENT_TIMESTAMP)", [body.clone().into()])).await.unwrap();
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
    db.execute_unprepared("INSERT INTO turn_cli_runtime_binding(turn_id,thread_id,continuation_thread_id,workspace_id,runtime_id,runtime_kind,native_thread_id,status,model) VALUES('turn','thread','thread','ws','claude','claude','native','running','sonnet')").await.unwrap();
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
async fn compaction_recovery_metadata_pages_do_not_starve_later_operations() {
    let store = store().await;
    let assertion = source(&store, "recovery-source", 1, "canonical source").await;
    for index in 0..17 {
        let id = format!("recovery-{index:02}");
        candidate(&store, &id, None, &assertion).await;
        store
            .compaction_bind_execution_turn(&id, "turn")
            .await
            .unwrap();
    }
    assert!(
        store
            .compaction_lifecycle_recovery(11, "")
            .await
            .unwrap()
            .is_empty()
    );
    let after_deadline = 10 + OPERATION_MILLIS;
    let first = store
        .compaction_lifecycle_recovery(after_deadline, "")
        .await
        .unwrap();
    assert_eq!(first.len(), 16);
    let last = first.last().unwrap().id.clone();
    let second = store
        .compaction_lifecycle_recovery(after_deadline, &last)
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].id, "recovery-16");
    assert!(
        store
            .compaction_lifecycle_recovery(after_deadline, &second[0].id)
            .await
            .unwrap()
            .is_empty()
    );
    store
        .compaction_stop_execution("ws", "thread", "owner", "turn")
        .await
        .unwrap();
    assert!(
        store
            .compaction_lifecycle_recovery(11, "")
            .await
            .unwrap()
            .iter()
            .all(|row| row.cancelled)
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
        digest.update((b.len() as u64).to_le_bytes());
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
    while store.compact_frozen_storage_quantum().await.unwrap() {
        quanta += 1;
        assert!(quanta < 100);
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
    assert!(!store.compact_frozen_storage_quantum().await.unwrap());
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
    let store = store().await;
    let refs = shared_refs(20);
    shared_capture(&store, "a", &refs, true).await;
    shared_capture(&store, "b", &refs, false).await;
    let db = store.database_connection();
    db.execute_unprepared("CREATE TEMP TRIGGER reject_shared_cleanup BEFORE DELETE ON compaction_frozen_message_data BEGIN SELECT RAISE(ABORT,'fixture cleanup rollback'); END").await.unwrap();
    let mut failed = false;
    for _ in 0..20 {
        if store.compact_frozen_storage_quantum().await.is_err() {
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
    for _ in 0..20 {
        if !store.compact_frozen_storage_quantum().await.unwrap() {
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
    for _ in 0..40 {
        match store.compact_frozen_storage_quantum().await {
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
