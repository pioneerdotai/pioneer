//! Execution observation regressions on the migrated PostgreSQL schema.
use super::*;
use pioneer_entity::execution_observation;

async fn fixture() -> CrudStore {
    let store = test_store_with_workspace("ws_observe").await;
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

#[pioneer_postgres_test::postgres_test]
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
    store.database_connection().execute_unprepared("CREATE FUNCTION reject_source_size() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER reject_source_size BEFORE INSERT ON execution_observation FOR EACH ROW WHEN (NEW.kind='tool_result_retention') EXECUTE FUNCTION reject_source_size()").await.unwrap();
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
        .execute_unprepared("DROP TRIGGER reject_source_size ON execution_observation; DROP FUNCTION reject_source_size()")
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
