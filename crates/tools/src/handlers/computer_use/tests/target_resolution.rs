use super::*;

#[tokio::test]
async fn target_resolution_ambiguous_role_includes_candidate_summaries() {
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

    let diagnostics = result
        .pointer("/result/execution/details/target_resolution/diagnostics")
        .expect("target resolution diagnostics");
    assert_eq!(
        diagnostics
            .pointer("/attempted/role")
            .and_then(JsonValue::as_str),
        Some("button")
    );
    assert_eq!(
        diagnostics
            .get("candidate_count")
            .and_then(JsonValue::as_u64),
        Some(2)
    );
    assert!(
        diagnostics
            .get("candidates")
            .and_then(JsonValue::as_array)
            .is_some_and(|items| items.len() == 2),
        "{result}"
    );
    assert_eq!(
        diagnostics
            .get("recommended_next_call")
            .and_then(JsonValue::as_str),
        Some("act_with_nth")
    );
}

#[tokio::test]
async fn target_resolution_stale_node_requests_snapshot() {
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
                "target": { "node_id": "missing-node", "snapshot_id": snapshot_id }
            }
        }),
    )
    .await;

    let diagnostics = result
        .pointer("/result/execution/details/target_resolution/diagnostics")
        .expect("target resolution diagnostics");
    assert_eq!(
        diagnostics.get("node_id").and_then(JsonValue::as_str),
        Some("missing-node")
    );
    assert_eq!(
        diagnostics
            .get("recommended_next_call")
            .and_then(JsonValue::as_str),
        Some("snapshot")
    );
    assert_eq!(
        result
            .pointer("/next_call/arguments/action")
            .and_then(JsonValue::as_str),
        Some("snapshot")
    );
}

#[tokio::test]
async fn target_resolution_missing_role_name_reports_attempt() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "press",
                "target": { "role": "checkbox", "name": "Subscribe" }
            }
        }),
    )
    .await;

    let diagnostics = result
        .pointer("/result/execution/details/target_resolution/diagnostics")
        .expect("target resolution diagnostics");
    assert_eq!(
        diagnostics
            .pointer("/attempted/role")
            .and_then(JsonValue::as_str),
        Some("checkbox")
    );
    assert_eq!(
        diagnostics
            .pointer("/attempted/name")
            .and_then(JsonValue::as_str),
        Some("Subscribe")
    );
    assert_eq!(
        diagnostics
            .get("candidate_count")
            .and_then(JsonValue::as_u64),
        Some(0)
    );
}
