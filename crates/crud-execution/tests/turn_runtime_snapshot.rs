use anyhow::Result;
use pioneer_crud_execution::turn_runtime_snapshot::{
    NewTurnRuntimeSnapshot, delete_turn_runtime_snapshots_for_closed_turns,
    find_turn_runtime_snapshot, upsert_turn_runtime_snapshot,
};
use sea_orm::{ConnectionTrait, EntityTrait, Set};

#[pioneer_postgres_test::postgres_test]
async fn turn_runtime_snapshot_cleanup_removes_closed_turns_only() -> Result<()> {
    let fixture = pioneer_postgres_test::scoped_database().await?;
    let db = fixture.connect_with_limit(1).await?;
    db.execute_unprepared(
        "INSERT INTO workspace(id,name) VALUES('ws_turn_runtime_snapshot_cleanup','Fixture');
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,created_at,updated_at)
         VALUES('thread_runtime_snapshot_cleanup','ws_turn_runtime_snapshot_cleanup','','agent','m','p','active',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"
    ).await?;
    let now = chrono::DateTime::from_timestamp(1_700_000_000, 0)
        .unwrap()
        .fixed_offset();
    for (turn_id, status) in [
        ("completed_turn", "completed"),
        ("failed_turn", "failed"),
        ("interrupted_turn", "interrupted"),
        ("blocked_turn", "blocked"),
        ("active_turn", "in_progress"),
    ] {
        pioneer_entity::turn::Entity::insert(pioneer_entity::turn::ActiveModel {
            id: Set(turn_id.to_owned()),
            thread_id: Set("thread_runtime_snapshot_cleanup".to_owned()),
            status: Set(status.to_owned()),
            turn_kind: Set("conversation".to_owned()),
            origin: Set("user".to_owned()),
            error: Set(None),
            prompt_manifest_json: Set("{}".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        })
        .exec(&db)
        .await
        .expect("turn insert should succeed");
    }

    for turn_id in [
        "completed_turn",
        "failed_turn",
        "interrupted_turn",
        "blocked_turn",
        "active_turn",
    ] {
        upsert_turn_runtime_snapshot(
            &db,
            NewTurnRuntimeSnapshot {
                turn_id: turn_id.to_owned(),
                thread_id: "thread_runtime_snapshot_cleanup".to_owned(),
                workspace_id: "ws_turn_runtime_snapshot_cleanup".to_owned(),
                mode: pioneer_protocol::ThreadMode::Agent,
                model: "model-a".to_owned(),
                provider_name: "provider-a".to_owned(),
                reasoning_effort: None,
                agent_skill_versions_json: None,
                hook_runtime_context_json: r#"{"mode":"agent","actor_kind":"agent"}"#.to_owned(),
                workspace_skill_policies_json: "[]".to_owned(),
                input_json: "[]".to_owned(),
                capabilities_json: "[]".to_owned(),
                resolved_artifacts_json: "[]".to_owned(),
                runtime_environment_json: "{}".to_owned(),
                history_json: "[]".to_owned(),
                created_at: now,
                updated_at: now,
            },
            None,
        )
        .await
        .expect("runtime snapshot insert should succeed");
    }

    let deleted = delete_turn_runtime_snapshots_for_closed_turns(&db)
        .await
        .expect("runtime snapshot cleanup should succeed");
    assert_eq!(deleted, 3);

    for turn_id in ["completed_turn", "failed_turn", "interrupted_turn"] {
        assert!(
            find_turn_runtime_snapshot(&db, turn_id)
                .await
                .expect("runtime snapshot read should succeed")
                .is_none(),
            "{turn_id} snapshot should be removed"
        );
    }
    for turn_id in ["blocked_turn", "active_turn"] {
        assert!(
            find_turn_runtime_snapshot(&db, turn_id)
                .await
                .expect("runtime snapshot read should succeed")
                .is_some(),
            "{turn_id} snapshot should be retained"
        );
    }
    db.close().await?;
    Ok(())
}
