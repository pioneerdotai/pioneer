use pioneer_crud_execution::native_events::{
    CliRuntimeNativeEventListFilter, NewCliRuntimeNativeEvent, append_native_event,
    append_native_event_if_absent, deserialize_cli_runtime_json, latest_native_event,
    list_native_events, serialize_cli_runtime_json,
};
use pioneer_crud_execution::time::unix_to_datetime;

#[tokio::test]
async fn cli_runtime_json_helpers_and_native_events_round_trip() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = fixture.connection();
        let created_at = unix_to_datetime(1_700_030_000);
        let payload = serde_json::json!({
            "method": "item/completed",
            "redacted": true
        });
        let encoded =
            serialize_cli_runtime_json(&payload).expect("native event payload should serialize");
        let decoded: serde_json::Value = deserialize_cli_runtime_json(encoded.as_str())
            .expect("native event payload should deserialize");
        assert_eq!(decoded, payload);

        for (id, sequence, method) in [
            ("native-event-1", 1_i64, "thread/start"),
            ("native-event-2", 2_i64, "item/completed"),
        ] {
            append_native_event(
                db,
                NewCliRuntimeNativeEvent {
                    id: id.to_owned(),
                    runtime_id: "codex".to_owned(),
                    runtime_kind: "codex".to_owned(),
                    workspace_id: Some("ws_cli_native".to_owned()),
                    thread_id: Some("thread_cli_native".to_owned()),
                    turn_id: Some("turn_cli_native".to_owned()),
                    native_thread_id: Some("codex-thread-native".to_owned()),
                    native_turn_id: Some("codex-turn-native".to_owned()),
                    native_method: method.to_owned(),
                    payload_redacted_json: encoded.clone(),
                    sequence,
                    created_at,
                },
            )
            .await
            .expect("native event append should succeed");
        }

        let events = list_native_events(
            db,
            CliRuntimeNativeEventListFilter {
                runtime_id: Some("codex".to_owned()),
                thread_id: Some("thread_cli_native".to_owned()),
                turn_id: Some("turn_cli_native".to_owned()),
                native_thread_id: Some("codex-thread-native".to_owned()),
                ..Default::default()
            },
        )
        .await
        .expect("native event list should succeed");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, 1);
        assert_eq!(events[1].native_method, "item/completed");

        let latest = latest_native_event(
            db,
            CliRuntimeNativeEventListFilter {
                runtime_id: Some("codex".to_owned()),
                thread_id: Some("thread_cli_native".to_owned()),
                turn_id: Some("turn_cli_native".to_owned()),
                native_thread_id: Some("codex-thread-native".to_owned()),
                native_turn_id: Some("codex-turn-native".to_owned()),
                ..Default::default()
            },
        )
        .await
        .expect("latest native event query should succeed")
        .expect("latest native event should exist");
        assert_eq!(latest.sequence, 2);
        assert_eq!(latest.native_method, "item/completed");
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn claude_boundary_event_is_idempotent_under_concurrent_replay() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = fixture.connection();
        let event = NewCliRuntimeNativeEvent {
            id: "deterministic-boundary".to_owned(),
            runtime_id: "claude".to_owned(),
            runtime_kind: "claude".to_owned(),
            workspace_id: Some("ws_boundary_replay".to_owned()),
            thread_id: Some("child".to_owned()),
            turn_id: Some("answer".to_owned()),
            native_thread_id: Some("session".to_owned()),
            native_turn_id: Some("synthetic-provider-turn".to_owned()),
            native_method: "claude/assistant_record_boundary".to_owned(),
            payload_redacted_json:
                serde_json::json!({"messageUuid":"01900000-0000-7000-8000-000000000001"})
                    .to_string(),
            sequence: 1,
            created_at: unix_to_datetime(1_700_030_000),
        };
        let concurrent = fixture.connect().await?;
        let (first, second) = tokio::join!(
            append_native_event_if_absent(db, event.clone()),
            append_native_event_if_absent(&concurrent, event.clone()),
        );
        assert_eq!(first.unwrap().id, event.id);
        assert_eq!(second.unwrap().id, event.id);
        let rows = list_native_events(
            db,
            CliRuntimeNativeEventListFilter {
                native_method: Some(event.native_method.clone()),
                turn_id: event.turn_id.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), 1);
        let mut conflicting = event;
        conflicting.payload_redacted_json =
            serde_json::json!({"messageUuid":"01900000-0000-7000-8000-000000000002"}).to_string();
        assert!(
            append_native_event_if_absent(db, conflicting)
                .await
                .is_err()
        );
        concurrent.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}
