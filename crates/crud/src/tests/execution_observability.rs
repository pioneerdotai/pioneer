//! Prepared regressions only. Use the real Migrator and scoped SQLite access.
use super::*;
use crate::ExecutionObservationInput;
use pioneer_entity::{execution_observation, execution_tariff};

async fn fixture() -> CrudStore {
    let store = test_store_with_workspace("ws_observe").await;
    store
        .database_connection()
        .execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    store
        .materialize_turn_start(
            &sample_thread("ws_observe", "thread_observe", 1_700_000_000),
            SandboxMode::FullAccess,
            &sample_turn("turn_observe"),
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    store
}

async fn file_store(path: &std::path::Path, install: bool) -> CrudStore {
    use pioneer_sqlite::{
        SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor, sqlite_read_only_connection_url,
    };
    use sea_orm::ConnectOptions;
    let mut writer_options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    writer_options
        .max_connections(1)
        .map_sqlx_sqlite_opts(|options| {
            options
                .pragma("foreign_keys", "OFF")
                .pragma("journal_mode", "WAL")
        });
    let writer = Database::connect(writer_options).await.unwrap();
    let executor = SqliteWriteExecutor::new(writer);
    if install {
        executor
            .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
            .await
            .unwrap();
    }
    let mut reader_options = ConnectOptions::new(sqlite_read_only_connection_url(path));
    reader_options
        .max_connections(2)
        .map_sqlx_sqlite_opts(|options| {
            options
                .read_only(true)
                .create_if_missing(false)
                .pragma("query_only", "ON")
                .pragma("foreign_keys", "OFF")
        });
    let reader = Database::connect(reader_options).await.unwrap();
    let db = SqliteDatabase::from_executor(reader, executor);
    db.validate_reader().await.unwrap();
    let store = CrudStore::new(db);
    if install {
        let timestamp = unix_to_datetime(1_700_000_000);
        pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
            id: Set("ws_observe".into()),
            name: Set("Observation fixture".into()),
            is_active: Set(true),
            is_current: Set(true),
            created_at: Set(timestamp),
            updated_at: Set(timestamp),
        })
        .exec(&store.connection)
        .await
        .unwrap();
        store
            .materialize_turn_start(
                &sample_thread("ws_observe", "thread_observe", 1_700_000_000),
                SandboxMode::FullAccess,
                &sample_turn("turn_observe"),
                &[],
                PersistedActorRef::System,
            )
            .await
            .unwrap();
    }
    store
}

fn attempt(id: &str, revision: i64, rate: &str) -> ExecutionObservationInput {
    ExecutionObservationInput {
        workspace_id: "ws_observe".into(),
        operation_id: id.into(),
        revision,
        thread_id: Some("thread_observe".into()),
        turn_id: Some("turn_observe".into()),
        owner_kind: "turn".into(),
        owner_id: "turn_observe".into(),
        parent_operation_id: Some("turn:turn_observe".into()),
        kind: "provider_attempt".into(),
        purpose: "main".into(),
        provider: Some("openai".into()),
        model: Some("fixture".into()),
        status: if revision == 0 {
            "started"
        } else {
            "completed"
        }
        .into(),
        recorded_at_ms: 1_700_000_000_000 + revision,
        facts: serde_json::json!({"usage":{"physical_attempt_id":id,"input_tokens":if revision==0{serde_json::Value::Null}else{serde_json::json!(10)}}}),
        tariff_evidence: Some(
            serde_json::json!({"cost":{"input":rate},"units":"USD_per_million_tokens","source_effective_from":null}),
        ),
    }
}

#[tokio::test]
async fn immutable_frontier_preserves_old_revision_during_append_and_tariff_refresh() {
    let path = OptionalDeliveryDatabasePath(std::env::temp_dir().join(format!(
        "pioneer-observation-{}.sqlite",
        uuid::Uuid::new_v4()
    )));
    let store = file_store(&path.0, true).await;
    store
        .record_execution_observation(attempt("A", 0, "2").prepare().unwrap())
        .await
        .unwrap();
    store
        .record_execution_observation(attempt("A", 0, "2").prepare().unwrap())
        .await
        .unwrap();
    let frontier = store
        .execution_observation_frontier("ws_observe", "thread_observe")
        .await
        .unwrap();
    let initial = store
        .execution_observation_page("ws_observe", "thread_observe", 0, frontier, 1, true)
        .await
        .unwrap();
    assert_eq!(initial.len(), 1);
    let old_hash = initial[0].tariff_hash.clone().unwrap();
    store
        .record_execution_observation(attempt("B", 0, "4").prepare().unwrap())
        .await
        .unwrap();
    store
        .record_execution_observation(attempt("A", 1, "2").prepare().unwrap())
        .await
        .unwrap();
    // Close both physical pools, then reopen through the actual reader/writer
    // runtime settings. Tariff lookup must not depend on a live catalog/cache.
    let database = store.database_connection();
    drop(store);
    database.close().await.unwrap();
    let reopened = file_store(&path.0, false).await;
    let old = reopened
        .execution_observation_page("ws_observe", "thread_observe", 0, frontier, 100, true)
        .await
        .unwrap();
    assert_eq!(old.len(), 1);
    assert_eq!(old[0].revision, 0);
    assert_eq!(old[0].tariff_hash.as_deref(), Some(old_hash.as_str()));
    let evidence = reopened
        .execution_tariff("ws_observe", &old_hash)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&evidence.evidence_json).unwrap()["cost"]["input"],
        "2"
    );
    assert_eq!(
        execution_tariff::Entity::find()
            .count(&reopened.connection)
            .await
            .unwrap(),
        2
    );
    let frontier = reopened
        .execution_observation_frontier("ws_observe", "thread_observe")
        .await
        .unwrap();
    let first = reopened
        .execution_observation_page("ws_observe", "thread_observe", 0, frontier, 1, true)
        .await
        .unwrap();
    reopened
        .record_execution_observation(attempt("C", 0, "100").prepare().unwrap())
        .await
        .unwrap();
    let second = reopened
        .execution_observation_page(
            "ws_observe",
            "thread_observe",
            first[0].sequence,
            frontier,
            1,
            true,
        )
        .await
        .unwrap();
    assert_eq!(first.len() + second.len(), 2);
    assert_ne!(first[0].operation_id, second[0].operation_id);
    assert!(first.iter().chain(&second).all(|r| r.operation_id != "C"));
    let database = reopened.database_connection();
    drop(reopened);
    database.close().await.unwrap();
}

#[tokio::test]
async fn owner_conflict_rolls_back_tariff_and_fk_off_thread_cleanup_removes_facts() {
    let store = fixture().await;
    let mut wrong = attempt("wrong", 0, "99");
    wrong.owner_id = "another_turn".into();
    assert!(
        store
            .record_execution_observation(wrong.prepare().unwrap())
            .await
            .is_err()
    );
    assert_eq!(
        execution_tariff::Entity::find()
            .count(&store.connection)
            .await
            .unwrap(),
        0
    );
    store
        .record_execution_observation(attempt("A", 0, "2").prepare().unwrap())
        .await
        .unwrap();
    let mut conflict = attempt("A", 1, "4");
    conflict.parent_operation_id = Some("forged-parent".into());
    assert!(
        store
            .record_execution_observation(conflict.prepare().unwrap())
            .await
            .is_err()
    );
    assert_eq!(
        execution_tariff::Entity::find()
            .count(&store.connection)
            .await
            .unwrap(),
        1
    );
    pioneer_entity::thread::Entity::delete_by_id("thread_observe")
        .exec(&store.connection)
        .await
        .unwrap();
    assert_eq!(
        execution_observation::Entity::find()
            .count(&store.connection)
            .await
            .unwrap(),
        0
    );
    assert!(
        store
            .record_execution_observation(attempt("late", 1, "2").prepare().unwrap())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn observation_schema_down_and_reapply_uses_registered_migration() {
    let store = fixture().await;
    let migration = Migrator::migrations()
        .into_iter()
        .find(|m| m.name() == "m20261109_000001_execution_observability")
        .unwrap();
    let tx = store.connection.begin().await.unwrap();
    let manager = migration::SchemaManager::new(&*tx);
    migration.down(&manager).await.unwrap();
    migration.up(&manager).await.unwrap();
    tx.commit().await.unwrap();
    store
        .record_execution_observation(attempt("after_reapply", 0, "2").prepare().unwrap())
        .await
        .unwrap();
    assert_eq!(
        store
            .execution_observation_page(
                "ws_observe",
                "thread_observe",
                0,
                store
                    .execution_observation_frontier("ws_observe", "thread_observe")
                    .await
                    .unwrap(),
                100,
                true
            )
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn native_cumulative_redelivery_is_one_leaf_and_cannot_roll_back_same_second_usage() {
    let store = fixture().await;
    let operation = "provider:turn_observe:native_item";
    store
        .record_execution_observation(attempt(operation, 0, "2").prepare().unwrap())
        .await
        .unwrap();
    let snapshot = |input| serde_json::json!({"physical_attempt_id":operation,"input_tokens":input,"output_tokens":0});
    for (revision, input) in [(2, 20), (2, 20), (1, 10)] {
        store
            .record_native_provider_usage(
                "ws_observe",
                "thread_observe",
                "turn_observe",
                "native_item",
                revision,
                snapshot(input),
            )
            .await
            .unwrap();
    }
    let current = store
        .execution_live_usage("ws_observe", "thread_observe", vec!["native_item".into()])
        .await
        .unwrap();
    assert_eq!(current.len(), 1);
    let value: serde_json::Value = serde_json::from_str(&current[0].usage_json).unwrap();
    assert_eq!(value["input_tokens"], 20);
    assert_eq!(value["_pioneer_observation"]["revision"], 2);
    let frontier = store
        .execution_observation_frontier("ws_observe", "thread_observe")
        .await
        .unwrap();
    let leaves = store
        .execution_observation_page("ws_observe", "thread_observe", 0, frontier, 100, true)
        .await
        .unwrap();
    assert_eq!(
        leaves.len(),
        1,
        "cumulative updates must not create journal charges"
    );
    assert!(
        store
            .record_native_provider_usage(
                "ws_observe",
                "thread_observe",
                "turn_observe",
                "native_item",
                2,
                snapshot(21)
            )
            .await
            .is_err()
    );
    pioneer_entity::thread::Entity::delete_by_id("thread_observe")
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(
        store
            .record_native_provider_usage(
                "ws_observe",
                "thread_observe",
                "turn_observe",
                "native_item",
                3,
                snapshot(30)
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn legacy_auxiliary_backfill_advances_past_corrupt_rows_and_reopen_is_idempotent() {
    let store = fixture().await.with_maintenance_access();
    for (id, body) in [
        (
            "legacy_a",
            serde_json::json!({"input_tokens":7,"output_tokens":0}).to_string(),
        ),
        (
            "legacy_b",
            serde_json::json!({"input_tokens":9,"output_tokens":0}).to_string(),
        ),
    ] {
        store
            .record_provider_usage(crate::ProviderUsageObservation {
                id: id.into(),
                workspace_id: "ws_observe".into(),
                owner_id: "thread_observe".into(),
                operation_kind: "title".into(),
                status: "completed".into(),
                usage_json: body,
                started_at: 1_700_000_000,
                updated_at: 1_700_000_001,
            })
            .await
            .unwrap();
    }
    // Fault injection into an old source, never a substitute schema/connection.
    pioneer_entity::provider_usage_observation::Entity::update_many()
        .col_expr(
            pioneer_entity::provider_usage_observation::Column::UsageJson,
            sea_orm::sea_query::Expr::value("broken-json"),
        )
        .filter(pioneer_entity::provider_usage_observation::Column::Id.eq("legacy_a"))
        .exec(&store.connection)
        .await
        .unwrap();
    let progress = store
        .backfill_aux_execution_observations_quantum()
        .await
        .unwrap();
    assert!(progress.complete);
    assert_eq!(progress.unavailable, 1);
    let frontier = store
        .execution_observation_frontier("ws_observe", "thread_observe")
        .await
        .unwrap();
    let rows = store
        .execution_observation_page("ws_observe", "thread_observe", 0, frontier, 100, true)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].operation_id, "provider:journal:legacy_b");
    assert_eq!(
        rows[0].end_wall_ms, None,
        "updated_at cannot supply legacy finish"
    );
    let reopened = CrudStore::new(store.database_connection()).with_maintenance_access();
    assert!(
        reopened
            .backfill_aux_execution_observations_quantum()
            .await
            .unwrap()
            .complete
    );
    assert_eq!(
        reopened
            .execution_observation_frontier("ws_observe", "thread_observe")
            .await
            .unwrap(),
        frontier
    );
}

#[tokio::test]
async fn monotonic_tool_duration_requires_the_same_process_incarnation() {
    let store = fixture().await;
    for (id, finish_run, expected) in [
        ("same", "process_A", Some(80)),
        ("restart", "process_B", None),
    ] {
        let mut start = attempt(id, 0, "2");
        start.kind = "tool_invocation".into();
        start.status = "running".into();
        start.tariff_evidence = None;
        start.facts = serde_json::json!({"observation":{"processRunId":"process_A","tsUnixMs":1000,"monoNs":100}});
        store
            .record_execution_observation(start.clone().prepare().unwrap())
            .await
            .unwrap();
        start.revision = 1;
        start.status = "settled".into();
        start.facts = serde_json::json!({"observation":{"processRunId":finish_run,"tsUnixMs":990,"monoNs":180}});
        store
            .record_execution_observation(start.prepare().unwrap())
            .await
            .unwrap();
        let row = store
            .execution_observation_latest("ws_observe", "thread_observe", id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.elapsed_ns, expected);
        assert_eq!(
            row.end_wall_ms,
            Some(990),
            "clock anomaly must remain visible"
        );
    }
}

#[tokio::test]
async fn reconciliation_revision_does_not_consume_the_next_producer_revision() {
    let store = fixture().await;
    let initial = attempt("revision_domains", 0, "2");
    store
        .record_execution_observation(initial.clone().prepare().unwrap())
        .await
        .unwrap();
    let first = store
        .execution_observation_latest("ws_observe", "thread_observe", "revision_domains")
        .await
        .unwrap()
        .unwrap();
    let mut resolution = initial.clone();
    resolution.revision = 1;
    resolution.facts["historical_reconciliation"] = serde_json::json!({"version":1});
    store
        .record_execution_observation(
            resolution
                .prepare()
                .unwrap()
                .expecting_previous_sequence(first.sequence),
        )
        .await
        .unwrap();
    // Re-delivery still matches the original producer fact after recalculation.
    store
        .record_execution_observation(initial.prepare().unwrap())
        .await
        .unwrap();
    store
        .record_execution_observation(attempt("revision_domains", 1, "2").prepare().unwrap())
        .await
        .unwrap();
    let latest = store
        .execution_observation_latest("ws_observe", "thread_observe", "revision_domains")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.status, "completed");
    assert_eq!(latest.source_revision, 1);
    assert_eq!(latest.revision, 2);
    assert_eq!(latest.resolution_origin, "producer");
    assert!(
        store
            .record_execution_observation(
                attempt("revision_domains", 2, "2")
                    .prepare()
                    .unwrap()
                    .expecting_previous_sequence(first.sequence)
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn journal_projection_failure_is_atomic_and_redelivery_survives_source_cleanup() {
    let store = fixture().await;
    let record=crate::ProviderUsageObservation {
        id:"aux_ack_identity".into(),workspace_id:"ws_observe".into(),owner_id:"thread_observe".into(),operation_kind:"title".into(),
        status:"completed".into(),started_at:1_700_000_000,updated_at:1_700_000_001,
        usage_json:serde_json::json!({"physical_attempt_id":"physical_aux","input_tokens":10,"output_tokens":0,
            "accounting":{"pricing":{"basis":"observed_billing_identity","version":1,"units":"USD_per_million_tokens","cost":{"input":"2"}}},
            "_pioneer_observation":{"revision":0,"bound_thread":true,"context_manifest":{"entries":[],"coverage":"fixture_content_free"}}}).to_string(),
    };
    // Inject failure after journal/tariff writes, at the atomic projection fence.
    store.connection.execute_unprepared("CREATE TRIGGER observation_fixture_fail BEFORE INSERT ON execution_observation BEGIN SELECT RAISE(ABORT,'fixture projection failure'); END").await.unwrap();
    assert!(store.record_provider_usage(record.clone()).await.is_err());
    assert!(
        store
            .provider_usage_page("ws_observe", "thread_observe", "", 100)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        execution_tariff::Entity::find()
            .count(&store.connection)
            .await
            .unwrap(),
        0
    );
    store
        .connection
        .execute_unprepared("DROP TRIGGER observation_fixture_fail")
        .await
        .unwrap();
    store.record_provider_usage(record.clone()).await.unwrap();
    // The caller may have lost its reply. A reopened owner redelivers the same
    // source identity; this cannot create another immutable charge leaf.
    let reopened = CrudStore::new(store.database_connection());
    reopened.record_provider_usage(record).await.unwrap();
    let before = reopened
        .execution_observation_latest(
            "ws_observe",
            "thread_observe",
            "provider:journal:aux_ack_identity",
        )
        .await
        .unwrap()
        .unwrap();
    let hash = before.tariff_hash.clone().unwrap();
    // Reclaim detailed transcript sources using the project connection. Numeric
    // facts and tariff evidence have independent lifetime, no prompt archive.
    pioneer_entity::turn_event::Entity::delete_many()
        .filter(pioneer_entity::turn_event::Column::TurnId.eq("turn_observe"))
        .exec(&store.connection)
        .await
        .unwrap();
    let after = reopened
        .execution_observation_latest(
            "ws_observe",
            "thread_observe",
            "provider:journal:aux_ack_identity",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.sequence, after.sequence);
    assert!(
        reopened
            .execution_tariff("ws_observe", &hash)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        execution_observation::Entity::find()
            .count(&store.connection)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn retained_tool_size_commits_with_source_and_survives_source_cleanup() {
    let store = fixture().await;
    let payload = serde_json::json!({"type":"json","value":{"content":"source 日本語","private":"not in facts"}}).to_string();
    let entry = crate::NewTurnLlmContextEntry {
        turn_id: "turn_observe".into(),
        item_id: Some("tool1".into()),
        attempt_id: None,
        sequence: 0,
        source: "tool_result_v2".into(),
        tool_name: Some("fixture_tool".into()),
        payload: payload.clone(),
        output_policy_snapshot: "{}".into(),
        created_at: chrono::Utc::now().fixed_offset(),
        expires_at: None,
    };
    store.database_connection().execute_unprepared("CREATE TRIGGER reject_source_size BEFORE INSERT ON execution_observation WHEN NEW.kind='tool_result_retention' BEGIN SELECT RAISE(ABORT,'fixture'); END").await.unwrap();
    assert!(
        store
            .append_turn_llm_context(entry.clone(), "retained-delivery")
            .await
            .is_err()
    );
    assert!(
        store
            .list_turn_llm_context("turn_observe")
            .await
            .unwrap()
            .is_empty()
    );
    store
        .database_connection()
        .execute_unprepared("DROP TRIGGER reject_source_size")
        .await
        .unwrap();
    assert!(
        !store
            .append_turn_llm_context(entry.clone(), "retained-delivery")
            .await
            .unwrap()
            .already_committed
    );
    assert!(
        store
            .append_turn_llm_context(entry, "retained-delivery")
            .await
            .unwrap()
            .already_committed
    );
    store
        .delete_turn_llm_context_for_turn("turn_observe")
        .await
        .unwrap();
    let rows = execution_observation::Entity::find()
        .filter(execution_observation::Column::Kind.eq("tool_result_retention"))
        .all(&store.connection)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let fact: serde_json::Value = serde_json::from_str(&rows[0].facts_json).unwrap();
    assert_eq!(fact["retained_source_serialized_bytes"], payload.len());
    assert_eq!(fact["source_item_id"], "tool1");
    assert!(!rows[0].facts_json.contains("not in facts"));
    assert!(!rows[0].facts_json.contains("source 日本語"));
}
