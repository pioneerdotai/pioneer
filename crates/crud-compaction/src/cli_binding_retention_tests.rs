use crate::{frozen_lifetime::PreparedFrozenRoot, store::CompactionStore};
use pioneer_crud_execution::{
    cli_attempt_commits::{activate_attempt, prepare_initial_attempt},
    cli_runtime_binding::{
        CliRuntimeActiveTurnBindingStatus, CliRuntimeExecutionSegmentStatus,
        CliRuntimeTurnBindingListFilter, NewCliRuntimeTurnBinding,
    },
    cli_runtime_store::CliRuntimeStore,
    input_mapping::InputMapping,
};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_postgres_test::with_database;
use sea_orm::{
    ConnectionTrait, DbBackend, Statement, TransactionTrait, prelude::DateTimeWithTimeZone,
};

async fn scalar(db: &PostgresDatabase, sql: &str) -> i64 {
    db.query_one_raw(Statement::from_string(DbBackend::Postgres, sql))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

async fn set_status(
    db: &PostgresDatabase,
    status: pioneer_protocol::TurnStatus,
    at: DateTimeWithTimeZone,
) -> anyhow::Result<()> {
    let tx = db.begin().await?;
    pioneer_crud_conversations::turn_writes::update_turn_status(
        &tx, "thread", "turn", status, None, at,
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

#[tokio::test]
async fn portable_cli_mapping_preserves_sent_bytes_control_updates_and_upsert() {
    with_database(async |db| {
    crate::plan_test_support::seed_payload_sources(db.connection()).await;
    let connection = PostgresDatabase::new(
        db.connect_read_only().await.unwrap(),
        db.connect().await.unwrap(),
    ).maintenance();
    let store = CliRuntimeStore::new(connection.clone());
    let compaction = CompactionStore::for_test(connection.clone());
    let at = chrono::Utc::now().fixed_offset();
    let history = serde_json::to_string(
        &serde_json::json!([{"role":"user","content":"контекст🦀".repeat(12000)}]),
    )
    .unwrap();
    let mut original = format!(
        "  {}\n",
        serde_json::json!({"pioneerContextBasis":{"historyJson":history,"executionThreadId":"thread","pendingTurn":{"turnId":"turn","messageRevision":0,"messageDeleted":false}},"input":"original sent bytes"})
    );
    let mut binding = NewCliRuntimeTurnBinding {
        turn_id: "turn".into(),
        thread_id: "thread".into(),
        continuation_thread_id: "thread".into(),
        workspace_id: "ws".into(),
        runtime_id: "codex".into(),
        runtime_kind: "codex".into(),
        native_thread_id: "native".into(),
        native_turn_id: None,
        request_id: Some("portable-request".into()),
        status: "starting".into(),
        model: None,
        cwd: None,
        sandbox_json: None,
        approval_policy: None,
        input_mapping_json: original.clone(),
        created_at: at,
        updated_at: at,
    };
    set_status(&connection, pioneer_protocol::TurnStatus::InProgress, at).await?;
    let root = PreparedFrozenRoot::cli(&binding.input_mapping_json, false)?;
    let mapping = InputMapping::prepare(&binding.input_mapping_json, root.locator().is_some())?;
    let tx = connection.begin().await?;
    prepare_initial_attempt(&tx, &binding, "mapping-attempt", 1, mapping, &root).await?;
    tx.commit().await?;
    let tx = connection.begin().await?;
    activate_attempt(
        &tx, "turn", "mapping-attempt", "native-control-1", None, at, None, None,
    ).await?;
    tx.commit().await?;
    store
        .register_cli_runtime_control_execution_segment("turn", "native", "native-control-1", at)
        .await
        .unwrap();
    binding.status = "running".into();
    binding.native_turn_id = Some("native-control-1".into());
    // Keep the mapping binding active after the owning Turn is sealed.
    set_status(&connection, pioneer_protocol::TurnStatus::Completed, at).await?;
    // Insignificant JSON whitespace is part of the original sent bytes.
    original.push_str(" \n");
    binding.input_mapping_json = original.clone();
    compaction.upsert_cli_runtime_turn_binding(binding.clone()).await.unwrap();
    assert_eq!(scalar(&connection,
        "SELECT count(*) n FROM turn_cli_runtime_binding WHERE turn_id='turn' AND input_mapping_codec=1 AND input_mapping_bytes>131072 AND octet_length(input_mapping_json)<input_mapping_bytes AND input_mapping_has_inline_history").await, 1);
    assert_eq!(
        store
            .get_cli_runtime_turn_binding_by_request("portable-request")
            .await
            .unwrap()
            .unwrap()
            .input_mapping_json,
        original
    );
    assert_eq!(
        store
            .set_cli_runtime_turn_mcp_metadata("turn", None)
            .await
            .unwrap()
            .input_mapping_json,
        original
    );
    // Invalid frame bytes must fail content reads while metadata stays usable.
    connection.execute_unprepared(
        "UPDATE turn_cli_runtime_binding SET input_mapping_json=decode('ff','hex') WHERE turn_id='turn'"
    ).await.unwrap();
    let compressed_guard = store.cli_runtime_turn_terminal_control_guard_by_id("turn").await.unwrap().unwrap();
    let metadata = store
        .get_cli_runtime_turn_binding_control("turn")
        .await
        .unwrap()
        .unwrap();
    assert!(metadata.input_mapping_json.is_empty());
    let frontier = store
        .active_cli_runtime_turn_binding_frontier(
            CliRuntimeActiveTurnBindingStatus::Running,
        )
        .await
        .unwrap()
        .unwrap();
    let page = store
        .list_active_cli_runtime_turn_binding_control_page_through(
            CliRuntimeActiveTurnBindingStatus::Running,
            None,
            &frontier,
        )
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0], metadata);
    assert_eq!(
        store
            .cli_runtime_turn_terminal_control_guard(&metadata)
            .await
            .unwrap()
            .unwrap(),
        compressed_guard
    );
    assert!(store.get_cli_runtime_turn_binding("turn").await.is_err());
    assert!(
        store
            .cli_runtime_turn_terminal_guard_by_id("turn")
            .await
            .is_err()
    );
    assert!(
        store
            .load_cli_runtime_control_mapping(&metadata)
            .await
            .is_err()
    );
    let owner = store
        .resolve_cli_runtime_native_turn_control_owner("codex", "native-control-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(owner.attempt.id, "mapping-attempt");
    assert_eq!(owner.binding, metadata);
    assert!(
        store
            .resolve_cli_runtime_native_turn_owner("codex", "native-control-1")
            .await
            .is_err()
    );
    let terminal_segment = store
        .terminalize_cli_runtime_control_execution_segment(
            "codex",
            "native-control-1",
            CliRuntimeExecutionSegmentStatus::Completed,
            None,
            at,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(terminal_segment.0.input_mapping_json.is_empty());
    assert_eq!(
        terminal_segment.2.status,
        CliRuntimeExecutionSegmentStatus::Completed
    );
    let next_segment = store
        .register_cli_runtime_control_execution_segment("turn", "native", "native-control-2", at)
        .await
        .unwrap();
    assert!(next_segment.0.input_mapping_json.is_empty());
    assert_eq!(
        next_segment.2.segment_index,
        terminal_segment.2.segment_index + 1
    );
    let selected = store
        .cli_runtime_turn_terminal_control_guard_by_id("turn")
        .await
        .unwrap()
        .unwrap();
    // A sealed Turn fences late Goal events even when the control path can read metadata.
    assert!(
        store
            .set_cli_runtime_turn_native_goal_control_state(
                "turn",
                Some("active".into()),
                Some("native-control-2".into()),
                at,
                Some(selected),
                "fixture-owner",
            )
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .get_cli_runtime_turn_binding_control("turn")
            .await
            .unwrap()
            .unwrap(),
        next_segment.0
    );
    let metadata = store
        .get_cli_runtime_turn_binding_control("turn")
        .await
        .unwrap()
        .unwrap();
    let listed = store
        .list_cli_runtime_turn_binding_controls(CliRuntimeTurnBindingListFilter {
            workspace_id: Some("ws".into()),
            runtime_id: Some("codex".into()),
            statuses: vec!["running".into()],
            limit: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(listed, vec![metadata.clone()]);
    assert!(
        store
            .load_cli_runtime_control_mapping(&metadata)
            .await
            .is_err()
    );
    binding.input_mapping_json =
        r#"{ "pioneerContextBasis": {"historyJson":"[]","executionThreadId":"thread","pendingTurn":{"turnId":"turn","messageRevision":0,"messageDeleted":false}}, "input":"replacement" }"#.into();
    let replacement = binding.input_mapping_json.clone();
    assert_eq!(
        compaction
            .upsert_cli_runtime_turn_binding(binding)
            .await
            .unwrap()
            .input_mapping_json,
        replacement
    );
    assert!(
        store
            .cli_runtime_turn_terminal_control_guard(&metadata)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .load_cli_runtime_control_mapping(&metadata)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .get_cli_runtime_turn_binding("turn")
            .await
            .unwrap()
            .unwrap()
            .input_mapping_json,
        replacement
    );
    assert_eq!(scalar(&connection, "SELECT count(*) n FROM turn_cli_runtime_binding WHERE turn_id='turn' AND NOT input_mapping_has_inline_history").await, 1);
        Ok(())
    }).await.unwrap();
}
