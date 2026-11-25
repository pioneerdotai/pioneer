//! Execution revision, tariff, journal and restart regressions on PostgreSQL.
use migration::{Migrator, MigratorTrait};
use pioneer_crud_conversations::events::TurnEventPayload;
use pioneer_crud_execution::execution_observability::{
    ExecutionObservationInput, ExecutionObservationRepository, prepare_event,
};
use pioneer_crud_execution::provider_usage::ProviderUsageObservation;
use pioneer_entity::{execution_observation, execution_tariff};
use pioneer_protocol::{
    ItemRecoveryOpenedNotification, ItemStartedNotification, RecoveryAction, RecoveryTrigger,
    SystemEventLevel, ToolCallStatus, ToolDisplayPayload, ToolObservation,
    ToolOutputPolicySnapshot, ToolStoragePayload, TurnItem, TurnItemType,
};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, Set};

async fn open_store(
    database: &pioneer_postgres_test::TestDatabase,
    seed: bool,
) -> pioneer_postgres_pools::PostgresDatabase {
    let writer = database.connect_with_limit(1).await.unwrap();
    let reader = database.connect_read_only().await.unwrap();
    let db = pioneer_postgres_pools::PostgresDatabase::new(reader, writer);
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

async fn fixture() -> pioneer_postgres_pools::PostgresDatabase {
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

async fn insert_observation_turn(db: &pioneer_postgres_pools::PostgresDatabase, turn: &str) {
    pioneer_entity::turn::Entity::insert(pioneer_entity::turn::ActiveModel {
        id: Set(turn.to_owned()),
        thread_id: Set("thread_observe".into()),
        status: Set("in_progress".into()),
        ..Default::default()
    })
    .exec(db)
    .await
    .unwrap();
}

fn start_observation_item(turn: &str, item: TurnItem) -> TurnEventPayload {
    TurnEventPayload::ItemStarted(ItemStartedNotification {
        workspace_id: "ws_observe".into(),
        thread_id: "thread_observe".into(),
        turn_id: turn.into(),
        item,
    })
}

fn system_observation_item(id: &str, code: &str, details: serde_json::Value) -> TurnItem {
    TurnItem::SystemEvent {
        id: id.into(),
        level: SystemEventLevel::Info,
        message: String::new(),
        code: Some(code.into()),
        details: Some(details),
    }
}

#[pioneer_postgres_test::postgres_test]
async fn compact_item_and_tool_parents_are_addressable_and_legacy_phase_can_replay() {
    let db = fixture().await;
    let store = ExecutionObservationRepository::new(&db);
    let turn = "t".repeat(126);
    let item = "i".repeat(130);
    insert_observation_turn(&db, &turn).await;
    let tool = |observation| TurnItem::DynamicToolCall {
        id: item.clone(),
        tool_name: "test_tool".into(),
        arguments: serde_json::json!({}),
        status: ToolCallStatus::InProgress,
        execution_class: Default::default(),
        recovery_policy: None,
        output_policy: ToolOutputPolicySnapshot::for_tool_name("test_tool"),
        display: ToolDisplayPayload::Hidden,
        storage: ToolStoragePayload::None,
        recovery: None,
        success: None,
        outcome: None,
        observation,
    };
    let item_parent = start_observation_item(&turn, tool(None));
    let tool_parent = start_observation_item(
        &turn,
        tool(Some(ToolObservation {
            parent_operation_id: None,
            phase_boundaries: Vec::new(),
            omitted_phase_boundaries: 0,
            output_sizes: None,
            process_run_id: None,
            trace_id: "trace".into(),
            turn_id: turn.clone(),
            tool_call_id: item.clone(),
            attempt_id: 1,
            pipeline_stage: "dispatch".into(),
            ts_unix_ms: 1_700_000_000_000,
            mono_ns: 1,
            event_seq: 1,
        })),
    );
    let raw_item_id = format!("item:{turn}:{item}");
    let raw_tool_id = format!("tool:{turn}:{item}:1");
    assert!(raw_item_id.len() > 256 && raw_tool_id.len() > 256);
    let details = serde_json::json!({"phase":"tool_dispatched","parent_operation_id":raw_tool_id});
    let tool_phase = |id| {
        start_observation_item(
            &turn,
            system_observation_item(id, "runtime_phase", details.clone()),
        )
    };
    let recovery = TurnEventPayload::ItemRecoveryOpened(ItemRecoveryOpenedNotification {
        workspace_id: "ws_observe".into(),
        thread_id: "thread_observe".into(),
        turn_id: turn.clone(),
        item_id: item,
        item_type: TurnItemType::DynamicToolCall,
        recovery_job_id: "recovery".into(),
        trigger: RecoveryTrigger::Timeout,
        action: RecoveryAction::RetryWithBackoff,
        attempt_number: 1,
    });
    for payload in [item_parent, tool_parent, recovery, tool_phase("new_phase")] {
        store
            .record_execution_observation(prepare_event(&payload).unwrap().unwrap())
            .await
            .unwrap();
    }

    // Recreate a durable producer fact written under the old raw parent spelling.
    let legacy_phase_id = format!("item:{turn}:legacy_phase");
    let mut legacy = attempt(&legacy_phase_id, 0, "2");
    legacy.turn_id = Some(turn.clone());
    legacy.owner_id = turn.clone();
    legacy.parent_operation_id = Some(raw_tool_id.clone());
    legacy.kind = "runtime_phase".into();
    legacy.purpose = "wait".into();
    legacy.provider = None;
    legacy.model = None;
    legacy.status = "running".into();
    legacy.facts = details.clone();
    legacy.tariff_evidence = None;
    store
        .record_execution_observation(legacy.prepare().unwrap())
        .await
        .unwrap();
    store
        .record_execution_observation(prepare_event(&tool_phase("legacy_phase")).unwrap().unwrap())
        .await
        .unwrap();
    let frontier = store
        .execution_observation_frontier("ws_observe", "thread_observe")
        .await
        .unwrap();
    for (raw, expected) in [
        (raw_item_id, "recovery_opened"),
        (raw_tool_id.clone(), "tool_dispatched"),
    ] {
        let parent = ExecutionObservationInput::bounded_operation_id(raw);
        let fact = store
            .execution_observation_latest("ws_observe", "thread_observe", &parent)
            .await
            .unwrap()
            .unwrap();
        let children = store
            .execution_children_page(
                "ws_observe",
                Some("thread_observe"),
                &fact.operation_id,
                0,
                frontier,
                32,
            )
            .await
            .unwrap();
        assert_eq!(children.len(), 1);
        let facts: serde_json::Value = serde_json::from_str(&children[0].facts_json).unwrap();
        assert_eq!(facts.get("details").unwrap_or(&facts)["phase"], expected);
    }
    let historical = store
        .execution_observation_latest("ws_observe", "thread_observe", &legacy_phase_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        historical.parent_operation_id.as_deref(),
        Some(raw_tool_id.as_str())
    );
    assert_eq!(historical.revision, 0);
}

#[tokio::test]
async fn upgraded_turn_cleanup_deletes_compact_native_usage_only_for_the_deleted_source() {
    pioneer_postgres_test::with_empty_database(async |database| {
        let connection = database.connection();
        let preceding = Migrator::migrations()
            .iter()
            .position(|migration| {
                migration.name() == "m20261125_000013_compact_observation_turn_cleanup"
            })
            .expect("compact observation cleanup migration is registered");
        Migrator::up(connection, Some(u32::try_from(preceding)?)).await?;
        let db = open_store(database, true).await;
        let store = ExecutionObservationRepository::new(&db);
        let turn = "t".repeat(230);
        insert_observation_turn(&db, &turn).await;
        let compact_item = "p".repeat(21);
        let other_item = "provider_other";
        assert!(format!("provider:{turn}:{compact_item}").len() > 256);

        // Persist a legacy journal entry before upgrading the existing database.
        let record = |turn: &str, id: &str| {
            let usage = serde_json::json!({"physical_attempt_id":id,"input_tokens":10});
            (
                start_observation_item(
                    turn,
                    system_observation_item(
                        id,
                        "provider_usage",
                        serde_json::json!({"provider":"openai","usage":usage.clone()}),
                    ),
                ),
                usage,
            )
        };
        let (legacy, other_usage) = record("turn_observe", other_item);
        store
            .record_execution_observation(prepare_event(&legacy)?.unwrap())
            .await?;
        store
            .record_native_provider_usage(
                "ws_observe",
                "thread_observe",
                "turn_observe",
                other_item,
                1,
                other_usage,
            )
            .await?;
        Migrator::up(connection, Some(1)).await?;
        Migrator::down(connection, Some(1)).await?;
        Migrator::up(connection, None).await?;

        let (payload, usage) = record(&turn, &compact_item);
        store
            .record_execution_observation(prepare_event(&payload)?.unwrap())
            .await?;
        store
            .record_native_provider_usage(
                "ws_observe",
                "thread_observe",
                &turn,
                &compact_item,
                1,
                usage,
            )
            .await?;
        let ids = vec![compact_item.clone(), other_item.into()];
        assert_eq!(
            store
                .execution_live_usage("ws_observe", "thread_observe", ids.clone())
                .await?
                .len(),
            2
        );

        // A compact fact that names another journal must not delete that journal.
        let mut decoy = attempt("obs1_decoy", 0, "2");
        decoy.turn_id = Some(turn.clone());
        decoy.owner_id = turn.clone();
        decoy.parent_operation_id = Some(format!("turn:{turn}"));
        decoy.facts = serde_json::json!({"provider_source_item_id":other_item});
        store.record_execution_observation(decoy.prepare()?).await?;
        pioneer_entity::turn::Entity::delete_by_id(turn.clone())
            .exec(&db)
            .await?;
        let remaining = store
            .execution_live_usage("ws_observe", "thread_observe", ids)
            .await?;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].id, other_item);
        assert_eq!(
            execution_observation::Entity::find()
                .filter(execution_observation::Column::TurnId.eq(turn))
                .count(&db)
                .await?,
            0
        );
        assert!(
            pioneer_entity::thread::Entity::find_by_id("thread_observe")
                .one(&db)
                .await?
                .is_some()
        );

        // The legacy operation format still participates in Turn cleanup.
        pioneer_entity::turn::Entity::delete_by_id("turn_observe")
            .exec(&db)
            .await?;
        assert!(
            store
                .execution_live_usage("ws_observe", "thread_observe", vec![other_item.into()])
                .await?
                .is_empty()
        );
        Ok(())
    })
    .await
    .unwrap();
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
