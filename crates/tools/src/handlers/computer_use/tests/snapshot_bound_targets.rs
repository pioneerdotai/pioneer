use super::*;

#[tokio::test]
async fn snapshot_bound_targets_latest_snapshot_id_allows_node_action() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;
    let snapshot_id = latest_snapshot_id(&handler, session_id).await;

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "press",
                "target": { "node_id": "n2", "snapshot_id": snapshot_id }
            }
        }),
    )
    .await;

    assert_eq!(
        result.pointer("/result/status").and_then(JsonValue::as_str),
        Some("ok")
    );
}

#[tokio::test]
async fn snapshot_bound_targets_missing_snapshot_id_is_recoverable_element_stale() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "press",
                "target": { "node_id": "n2" }
            }
        }),
    )
    .await;

    assert_eq!(
        result.get("status").and_then(JsonValue::as_str),
        Some("running")
    );
    assert_eq!(
        result.get("failure_class").and_then(JsonValue::as_str),
        Some("element_stale")
    );
    assert!(
        result
            .pointer("/result/message")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .contains("snapshot_id_required"),
        "{result}"
    );
    assert_eq!(
        result
            .pointer("/next_call/arguments/action")
            .and_then(JsonValue::as_str),
        Some("snapshot")
    );
}

#[tokio::test]
async fn snapshot_bound_targets_stale_snapshot_id_is_recoverable_element_stale() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;
    let stale_snapshot_id = latest_snapshot_id(&handler, session_id).await;

    let fresh_snapshot = invoke(
        &handler,
        serde_json::json!({
            "action": "snapshot",
            "session_id": session_id
        }),
    )
    .await;
    let fresh_snapshot_id = fresh_snapshot
        .pointer("/snapshot/snapshot_id")
        .and_then(JsonValue::as_str)
        .expect("fresh snapshot id");
    assert_ne!(stale_snapshot_id, fresh_snapshot_id);

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
        result.get("status").and_then(JsonValue::as_str),
        Some("running")
    );
    assert_eq!(
        result.get("failure_class").and_then(JsonValue::as_str),
        Some("element_stale")
    );
    assert!(
        result
            .pointer("/result/message")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .contains("snapshot_id_stale"),
        "{result}"
    );
    assert_eq!(
        result
            .pointer("/next_call/arguments/action")
            .and_then(JsonValue::as_str),
        Some("snapshot")
    );
}

#[tokio::test]
async fn snapshot_bound_targets_selector_target_does_not_require_snapshot_id() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "press",
                "target": { "selector": "button[stable_id=\"mock-ok-button\"]" }
            }
        }),
    )
    .await;

    assert_eq!(
        result.pointer("/result/status").and_then(JsonValue::as_str),
        Some("ok")
    );
}
