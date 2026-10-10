use super::*;

#[tokio::test]
async fn snapshot_write_and_physical_read_reject_aggregate_oversize_instead_of_missing_state() {
    let workspace = "ws_snapshot_bounds";
    let store = test_store_with_workspace(workspace).await;
    let now = unix_to_datetime(1_700_000_000);
    let snapshot = NewTurnRuntimeSnapshot {
        turn_id: "snapshot_bounds_turn".into(),
        thread_id: "snapshot_bounds_thread".into(),
        workspace_id: workspace.into(),
        mode_json: "\"agent\"".into(),
        model: "model".into(),
        provider_name: "fixture".into(),
        reasoning_effort: None,
        agent_skill_versions_json: None,
        hook_runtime_context_json: "{}".into(),
        workspace_skill_policies_json: "[]".into(),
        input_json: "[]".into(),
        capabilities_json: "[]".into(),
        resolved_artifacts_json: "[]".into(),
        runtime_environment_json: "{}".into(),
        history_json: "[]".into(),
        created_at: now,
        updated_at: now,
    };
    let limit = pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES;
    let mut oversized = snapshot.clone();
    oversized.runtime_environment_json = "x".repeat(limit / 2);
    oversized.workspace_skill_policies_json = "x".repeat(limit / 2);
    assert!(store.upsert_turn_runtime_snapshot(oversized).await.is_err());
    assert!(
        store
            .get_turn_runtime_snapshot(&snapshot.turn_id)
            .await
            .unwrap()
            .is_none()
    );
    store
        .upsert_turn_runtime_snapshot(snapshot.clone())
        .await
        .unwrap();
    assert!(
        store
            .get_turn_runtime_snapshot(&snapshot.turn_id)
            .await
            .unwrap()
            .is_some()
    );
    // Simulate oversized legacy/corrupt storage outside the guarded production
    // writer. The reader must report an explicit error, never successful None.
    store
        .connection
        .execute_raw(sea_orm::Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            "UPDATE turn_runtime_snapshot SET runtime_environment_json=? WHERE turn_id=?",
            [
                "é".repeat(limit / 2).into(),
                snapshot.turn_id.clone().into(),
            ],
        ))
        .await
        .unwrap();
    let error = store
        .get_turn_runtime_snapshot(&snapshot.turn_id)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("byte limit"));
    store
        .connection
        .execute_raw(sea_orm::Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            "UPDATE turn_runtime_snapshot SET runtime_environment_json='{}' WHERE turn_id=?",
            [snapshot.turn_id.clone().into()],
        ))
        .await
        .unwrap();
    assert!(
        store
            .get_turn_runtime_snapshot(&snapshot.turn_id)
            .await
            .unwrap()
            .is_some()
    );
}
