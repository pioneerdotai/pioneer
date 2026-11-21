//! Execution revision, tariff, journal and restart regressions on PostgreSQL.
use migration::{Migrator, MigratorTrait};
use pioneer_crud_execution::execution_observability::{
    ExecutionObservationInput, ExecutionObservationRepository,
};
use pioneer_crud_execution::provider_usage::ProviderUsageObservation;
use pioneer_entity::{execution_observation, execution_tariff};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter};

async fn open_store(
    database: &pioneer_postgres_test::TestDatabase,
    seed: bool,
) -> pioneer_sqlite::SqliteDatabase {
    let writer = database.connect_with_limit(1).await.unwrap();
    let reader = database.connect_read_only().await.unwrap();
    let db = pioneer_sqlite::SqliteDatabase::new(reader, writer);
    db.validate_reader().await.unwrap();
    if seed {
        db.execute_unprepared(
            "INSERT INTO workspace (id,name) VALUES ('ws_observe','Observation fixture');
             INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
             VALUES ('thread_observe','ws_observe','','agent','model','provider','active');
             INSERT INTO turn(id,thread_id,status) VALUES ('turn_observe','thread_observe','in_progress');",
        )
        .await
        .unwrap();
    }
    db
}

async fn fixture() -> pioneer_sqlite::SqliteDatabase {
    let database = pioneer_postgres_test::scoped_database().await.unwrap();
    open_store(&database, true).await
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

#[pioneer_postgres_test::postgres_test]
async fn immutable_frontier_preserves_old_revision_during_append_and_tariff_refresh() {
    let database = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = open_store(&database, true).await;
    let store = ExecutionObservationRepository::new(&db);
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
    db.close().await.unwrap();
    let db = open_store(&database, false).await;
    let reopened = ExecutionObservationRepository::new(&db);
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
        execution_tariff::Entity::find().count(&db).await.unwrap(),
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
    db.close().await.unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn owner_conflict_rolls_back_tariff_and_thread_cleanup_removes_facts() {
    let db = fixture().await;
    let store = ExecutionObservationRepository::new(&db);
    let mut wrong = attempt("wrong", 0, "99");
    wrong.owner_id = "another_turn".into();
    assert!(
        store
            .record_execution_observation(wrong.prepare().unwrap())
            .await
            .is_err()
    );
    assert_eq!(
        execution_tariff::Entity::find().count(&db).await.unwrap(),
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
        execution_tariff::Entity::find().count(&db).await.unwrap(),
        1
    );
    pioneer_entity::thread::Entity::delete_by_id("thread_observe")
        .exec(&db)
        .await
        .unwrap();
    assert_eq!(
        execution_observation::Entity::find()
            .count(&db)
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
    pioneer_postgres_test::with_empty_database(async |database| {
        let connection = database.connection();
        Migrator::up(connection, None).await?;
        Migrator::reset(connection).await?;
        Migrator::up(connection, None).await?;
        let db = open_store(database, true).await;
        let store = ExecutionObservationRepository::new(&db);
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
        Ok(())
    })
    .await
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn native_cumulative_redelivery_is_one_leaf_and_cannot_roll_back_same_second_usage() {
    let db = fixture().await;
    let store = ExecutionObservationRepository::new(&db);
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
        .exec(&db)
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

#[pioneer_postgres_test::postgres_test]
async fn monotonic_tool_duration_requires_the_same_process_incarnation() {
    let db = fixture().await;
    let store = ExecutionObservationRepository::new(&db);
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

#[pioneer_postgres_test::postgres_test]
async fn reconciliation_revision_does_not_consume_the_next_producer_revision() {
    let db = fixture().await;
    let store = ExecutionObservationRepository::new(&db);
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

#[pioneer_postgres_test::postgres_test]
async fn journal_projection_failure_is_atomic_and_redelivery_survives_source_cleanup() {
    let database = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = open_store(&database, true).await;
    let store = ExecutionObservationRepository::new(&db);
    let record=ProviderUsageObservation {
        id:"aux_ack_identity".into(),workspace_id:"ws_observe".into(),owner_id:"thread_observe".into(),operation_kind:"title".into(),
        status:"completed".into(),started_at:1_700_000_000,updated_at:1_700_000_001,
        usage_json:serde_json::json!({"physical_attempt_id":"physical_aux","input_tokens":10,"output_tokens":0,
            "accounting":{"pricing":{"basis":"observed_billing_identity","version":1,"units":"USD_per_million_tokens","cost":{"input":"2"}}},
            "_pioneer_observation":{"revision":0,"bound_thread":true,"context_manifest":{"entries":[],"coverage":"fixture_content_free"}}}).to_string(),
    };
    // Inject failure after journal/tariff writes, at the atomic projection fence.
    db.execute_unprepared("CREATE FUNCTION observation_fixture_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture projection failure'; END $$; CREATE TRIGGER observation_fixture_fail BEFORE INSERT ON execution_observation FOR EACH ROW EXECUTE FUNCTION observation_fixture_fail()").await.unwrap();
    assert!(store.record_provider_usage(record.clone()).await.is_err());
    assert!(
        store
            .provider_usage_page("ws_observe", "thread_observe", "", 100)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        execution_tariff::Entity::find().count(&db).await.unwrap(),
        0
    );
    db.execute_unprepared("DROP TRIGGER observation_fixture_fail ON execution_observation; DROP FUNCTION observation_fixture_fail()")
        .await
        .unwrap();
    store.record_provider_usage(record.clone()).await.unwrap();
    // The caller may have lost its reply. A reopened owner redelivers the same
    // source identity; this cannot create another immutable charge leaf.
    let reopened_db = open_store(&database, false).await;
    let reopened = ExecutionObservationRepository::new(&reopened_db);
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
        .exec(&db)
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
            .count(&db)
            .await
            .unwrap(),
        1
    );
}
