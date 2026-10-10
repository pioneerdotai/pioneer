use super::*;
use crate::{NewTurnExecution, TurnExecutionStatus, TurnExecutorKind};

#[tokio::test]
async fn snapshot_write_and_physical_read_reject_aggregate_oversize_instead_of_missing_state() {
    let workspace = "ws_snapshot_bounds";
    let store = test_store_with_workspace(workspace).await;
    let now = unix_to_datetime(1_700_000_000);
    let snapshot = candidate_snapshot(workspace, now);
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

fn candidate_snapshot(
    workspace: &str,
    now: sea_orm::entity::prelude::DateTimeWithTimeZone,
) -> NewTurnRuntimeSnapshot {
    NewTurnRuntimeSnapshot {
        turn_id: "snapshot_bounds_turn".into(),
        thread_id: "snapshot_bounds_thread".into(),
        workspace_id: workspace.into(),
        mode_json: "\"agent\"".into(),
        model: "model".into(),
        provider_name: "fixture".into(),
        reasoning_effort: None,
        agent_skill_versions_json: None,
        ordinary_skill_catalog_json: None,
        hook_runtime_context_json: "{}".into(),
        workspace_skill_policies_json: "[]".into(),
        input_json: "[]".into(),
        capabilities_json: "[]".into(),
        resolved_artifacts_json: "[]".into(),
        runtime_environment_json: "{}".into(),
        history_json: "[]".into(),
        created_at: now,
        updated_at: now,
    }
}

async fn seed_native_owner(store: &CrudStore, snapshot: &NewTurnRuntimeSnapshot) {
    let mut thread = sample_thread(&snapshot.workspace_id, &snapshot.thread_id, 1_700_000_000);
    thread.model = snapshot.model.clone();
    thread.model_provider = snapshot.provider_name.clone();
    let turn = sample_turn(&snapshot.turn_id);
    store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &turn,
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    crate::repositories::turn_execution::insert_immutable(
        &store.connection,
        NewTurnExecution {
            turn_id: snapshot.turn_id.clone(),
            thread_id: snapshot.thread_id.clone(),
            workspace_id: snapshot.workspace_id.clone(),
            executor_kind: TurnExecutorKind::NativeAgent,
            executor_key: None,
            status: TurnExecutionStatus::Starting,
            owner_id: "snapshot-owner".into(),
            lease_until: unix_to_datetime(1_700_000_045),
            created_at: snapshot.created_at,
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn native_snapshot_is_idempotent_by_semantic_payload_and_rejects_replacement() {
    let store = test_store_with_workspace("ws_snapshot_immutable").await;
    let mut snapshot = candidate_snapshot("ws_snapshot_immutable", unix_to_datetime(1_700_000_000));
    snapshot.runtime_environment_json = r#"{"a":"original","b":"value"}"#.into();
    seed_native_owner(&store, &snapshot).await;
    let accepted = store
        .insert_native_turn_runtime_snapshot(snapshot.clone())
        .await
        .unwrap();
    let mut retry = snapshot.clone();
    retry.runtime_environment_json = r#"{"b":"value","a":"original"}"#.into();
    retry.created_at = unix_to_datetime(1_700_000_020);
    retry.updated_at = retry.created_at;
    assert_eq!(
        store
            .insert_native_turn_runtime_snapshot(retry)
            .await
            .unwrap(),
        accepted
    );
    for replacement in [r#"{"a":"changed","b":"value"}"#, "SECRET malformed JSON"] {
        let mut changed = snapshot.clone();
        changed.runtime_environment_json = replacement.into();
        let err = store
            .insert_native_turn_runtime_snapshot(changed)
            .await
            .unwrap_err();
        assert!(!format!("{err:#}").contains("SECRET"));
        assert_eq!(
            store
                .get_turn_runtime_snapshot(&snapshot.turn_id)
                .await
                .unwrap()
                .unwrap(),
            accepted
        );
    }
    let mut changed = snapshot.clone();
    changed.ordinary_skill_catalog_json =
        Some(r#"{"schema_version":1,"catalog":{"skills":["changed"]}}"#.into());
    assert!(
        store
            .insert_native_turn_runtime_snapshot(changed)
            .await
            .is_err()
    );
    let mut foreign = snapshot.clone();
    foreign.workspace_id = "other-workspace".into();
    assert!(
        store
            .insert_native_turn_runtime_snapshot(foreign)
            .await
            .is_err()
    );
    assert_eq!(
        store
            .get_turn_runtime_snapshot(&snapshot.turn_id)
            .await
            .unwrap()
            .unwrap(),
        accepted
    );
    store
        .connection
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE \"turn\" SET status='interrupted' WHERE id=?",
            [snapshot.turn_id.clone().into()],
        ))
        .await
        .unwrap();
    assert!(
        store
            .insert_native_turn_runtime_snapshot(snapshot.clone())
            .await
            .is_err()
    );
    assert_eq!(
        store
            .get_turn_runtime_snapshot(&snapshot.turn_id)
            .await
            .unwrap()
            .unwrap(),
        accepted
    );
}

#[tokio::test]
async fn native_snapshot_has_no_orphan_and_rolls_back_generation_change_at_commit() {
    let store = test_store_with_workspace("ws_snapshot_owner").await;
    let snapshot = candidate_snapshot("ws_snapshot_owner", unix_to_datetime(1_700_000_000));
    assert!(
        store
            .insert_native_turn_runtime_snapshot(snapshot.clone())
            .await
            .is_err()
    );
    assert!(
        store
            .get_turn_runtime_snapshot(&snapshot.turn_id)
            .await
            .unwrap()
            .is_none()
    );
    seed_native_owner(&store, &snapshot).await;
    // Inject a generation change between validation and commit, within the
    // same transaction. Both the snapshot and trigger effect must roll back.
    store.connection.execute_unprepared("CREATE TRIGGER rotate_snapshot_owner BEFORE INSERT ON turn_runtime_snapshot BEGIN UPDATE turn_execution SET owner_generation=owner_generation+1 WHERE turn_id=NEW.turn_id; END").await.unwrap();
    assert!(
        store
            .insert_native_turn_runtime_snapshot(snapshot.clone())
            .await
            .is_err()
    );
    assert!(
        store
            .get_turn_runtime_snapshot(&snapshot.turn_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .get_turn_execution(&snapshot.turn_id)
            .await
            .unwrap()
            .unwrap()
            .owner_generation,
        1
    );
    store
        .connection
        .execute_unprepared("DROP TRIGGER rotate_snapshot_owner")
        .await
        .unwrap();
    store
        .insert_native_turn_runtime_snapshot(snapshot)
        .await
        .unwrap();
}
