use super::*;

#[tokio::test]
async fn structured_action_trace_reports_target_resolution_failure() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;
    let stale_snapshot_id = "s999-1";

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "press",
                "target": { "node_id": "n2", "snapshot_id": stale_snapshot_id }
            }
        }),
    )
    .await;

    assert_eq!(
        result
            .pointer("/trace/session_id")
            .and_then(JsonValue::as_u64),
        Some(session_id)
    );
    assert_eq!(
        result
            .pointer("/trace/action_kind")
            .and_then(JsonValue::as_str),
        Some("semantic")
    );
    assert_eq!(
        result
            .pointer("/trace/action_type")
            .and_then(JsonValue::as_str),
        Some("press")
    );
    assert_eq!(
        result
            .pointer("/trace/failure_class")
            .and_then(JsonValue::as_str),
        Some("element_stale")
    );
    assert!(result.pointer("/trace/target_before_resolution").is_some());
    assert!(result.pointer("/trace/resolved_target").is_some());
    assert!(
        result
            .pointer("/trace/suggested_fallbacks")
            .and_then(JsonValue::as_array)
            .is_some()
    );
    assert_eq!(
        result
            .pointer("/result/trace/failure_class")
            .and_then(JsonValue::as_str),
        Some("element_stale")
    );
}

#[tokio::test]
async fn structured_action_trace_snapshot_reports_progress_and_transport() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "trace snapshot",
            "target": { "type": "screen" }
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");

    let snapshot = invoke(
        &handler,
        serde_json::json!({ "action": "snapshot", "session_id": session_id }),
    )
    .await;

    assert_eq!(
        snapshot
            .pointer("/trace/session_id")
            .and_then(JsonValue::as_u64),
        Some(session_id)
    );
    assert!(
        snapshot
            .pointer("/trace/snapshot_id")
            .and_then(JsonValue::as_str)
            .is_some()
    );
    assert!(
        snapshot
            .pointer("/trace/transport/source_width_px")
            .and_then(JsonValue::as_u64)
            .is_some()
    );
    assert!(snapshot.pointer("/trace/progress_signals").is_some());
    assert_eq!(
        snapshot
            .pointer("/llm_context/trace/session_id")
            .and_then(JsonValue::as_u64),
        Some(session_id)
    );
}
