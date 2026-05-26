use super::*;

#[tokio::test]
async fn fallback_suggestions_do_not_parse_goal_for_app_or_path_actions() {
    let backend = MockComputerUseBackend::default().with_unsupported_semantic_actions(["toggle"]);
    let (handler, _) = test_handler_with_backend(backend);

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "open ExampleApp and /tmp/example-path after the next UI step",
            "target": { "type": "app_name", "name": "MockApp" }
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");
    invoke(
        &handler,
        serde_json::json!({
            "action": "snapshot",
            "session_id": session_id
        }),
    )
    .await;
    let snapshot_id = latest_snapshot_id(&handler, session_id).await;

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "toggle",
                "target": { "node_id": "n2", "snapshot_id": snapshot_id }
            }
        }),
    )
    .await;

    let suggestions = result
        .get("suggested_fallbacks")
        .and_then(JsonValue::as_array)
        .expect("suggested fallbacks");
    assert!(
        suggestions.iter().all(|item| {
            !matches!(
                item.get("type").and_then(JsonValue::as_str),
                Some("open_app" | "open_path" | "reveal_path")
            ) && item.get("app").is_none()
                && item.get("path").is_none()
        }),
        "{result}"
    );
}

#[tokio::test]
async fn fallback_suggestions_action_not_supported_include_supported_node_action_and_input_click() {
    let backend = MockComputerUseBackend::default().with_unsupported_semantic_actions(["toggle"]);
    let (handler, _) = test_handler_with_backend(backend);
    let session_id = start_app_session_with_snapshot(&handler).await;
    let snapshot_id = latest_snapshot_id(&handler, session_id).await;

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "toggle",
                "target": { "node_id": "n2", "snapshot_id": snapshot_id }
            }
        }),
    )
    .await;

    assert_eq!(
        result.get("failure_class").and_then(JsonValue::as_str),
        Some("action_not_supported")
    );
    let suggestions = result
        .get("suggested_fallbacks")
        .and_then(JsonValue::as_array)
        .expect("suggested fallbacks");
    assert!(
        suggestions
            .iter()
            .any(|item| item.get("type").and_then(JsonValue::as_str) == Some("press")),
        "{result}"
    );
    assert!(
        suggestions
            .iter()
            .any(|item| item.get("type").and_then(JsonValue::as_str) == Some("input_click")),
        "{result}"
    );
    assert!(
        suggestions
            .iter()
            .all(|item| item.get("type").and_then(JsonValue::as_str) != Some("toggle")),
        "{result}"
    );
}

#[tokio::test]
async fn fallback_suggestions_element_stale_include_latest_snapshot_target() {
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
                "target": { "node_id": "n2" }
            }
        }),
    )
    .await;

    assert_eq!(
        result.get("failure_class").and_then(JsonValue::as_str),
        Some("element_stale")
    );
    let suggestions = result
        .get("suggested_fallbacks")
        .and_then(JsonValue::as_array)
        .expect("suggested fallbacks");
    assert!(
        suggestions.iter().any(|item| {
            item.get("type").and_then(JsonValue::as_str) == Some("press")
                && item
                    .pointer("/target/snapshot_id")
                    .and_then(JsonValue::as_str)
                    == Some(snapshot_id.as_str())
        }),
        "{result}"
    );
}

#[tokio::test]
async fn fallback_suggestions_ambiguous_target_include_nth_disambiguation() {
    let backend = MockComputerUseBackend::default().with_extra_button();
    let (handler, _) = test_handler_with_backend(backend);
    let session_id = start_app_session_with_snapshot(&handler).await;

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "press",
                "target": { "role": "button" }
            }
        }),
    )
    .await;

    assert_eq!(
        result.get("failure_class").and_then(JsonValue::as_str),
        Some("runtime_action_error")
    );
    let suggestions = result
        .get("suggested_fallbacks")
        .and_then(JsonValue::as_array)
        .expect("suggested fallbacks");
    assert!(
        suggestions.iter().any(|item| {
            item.get("type").and_then(JsonValue::as_str) == Some("press")
                && item.pointer("/target/role").and_then(JsonValue::as_str) == Some("button")
                && item.pointer("/target/nth").and_then(JsonValue::as_u64) == Some(1)
        }),
        "{result}"
    );
    assert!(
        suggestions.iter().any(|item| {
            item.get("type").and_then(JsonValue::as_str) == Some("press")
                && item.pointer("/target/role").and_then(JsonValue::as_str) == Some("button")
                && item.pointer("/target/nth").and_then(JsonValue::as_u64) == Some(2)
        }),
        "{result}"
    );
}
