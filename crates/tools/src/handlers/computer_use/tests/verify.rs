use super::*;

#[tokio::test]
async fn verify_action_visible_text_passes_from_latest_tree() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "verify",
            "session_id": session_id,
            "expect": { "visible_text": "OK" }
        }),
    )
    .await;

    assert_eq!(
        result
            .pointer("/verification/status")
            .and_then(JsonValue::as_str),
        Some("passed")
    );
    assert!(
        result
            .pointer("/verification/evidence/0/matches")
            .and_then(JsonValue::as_array)
            .is_some_and(|matches| !matches.is_empty())
    );

    let status = invoke(
        &handler,
        serde_json::json!({
            "action": "status",
            "session_id": session_id
        }),
    )
    .await;
    assert_eq!(
        status
            .pointer("/last_verification/status")
            .and_then(JsonValue::as_str),
        Some("passed")
    );
}

#[tokio::test]
async fn verify_action_node_presence_passes_from_latest_tree() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "verify",
            "session_id": session_id,
            "expect": {
                "node": { "role": "button", "name": "OK" }
            }
        }),
    )
    .await;

    assert_eq!(
        result
            .pointer("/verification/status")
            .and_then(JsonValue::as_str),
        Some("passed")
    );
}

#[tokio::test]
async fn verify_action_missing_snapshot_returns_needs_snapshot() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "verify without snapshot",
            "target": { "type": "screen" }
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "verify",
            "session_id": session_id,
            "expect": { "visible_text": "anything" }
        }),
    )
    .await;

    assert_eq!(
        result
            .pointer("/verification/status")
            .and_then(JsonValue::as_str),
        Some("needs_snapshot")
    );
    assert_eq!(
        result
            .pointer("/next_call/arguments/action")
            .and_then(JsonValue::as_str),
        Some("snapshot")
    );
}

#[tokio::test]
async fn verify_action_failed_expectation_returns_evidence() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "verify",
            "session_id": session_id,
            "expect": { "visible_text": "Definitely Missing Text" }
        }),
    )
    .await;

    assert_eq!(
        result
            .pointer("/verification/status")
            .and_then(JsonValue::as_str),
        Some("failed")
    );
    assert_eq!(
        result
            .pointer("/verification/evidence/0/passed")
            .and_then(JsonValue::as_bool),
        Some(false)
    );
}

#[tokio::test]
async fn completion_evidence_verify_pass_allows_completed_stop() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    let verification = invoke(
        &handler,
        serde_json::json!({
            "action": "verify",
            "session_id": session_id,
            "expect": { "visible_text": "OK" }
        }),
    )
    .await;

    assert_eq!(
        verification
            .pointer("/completion_evidence/accepted")
            .and_then(JsonValue::as_bool),
        Some(true)
    );

    let stopped = invoke(
        &handler,
        serde_json::json!({
            "action": "stop",
            "session_id": session_id,
            "outcome": "completed",
            "reason": "goal_completed"
        }),
    )
    .await;

    assert_eq!(
        stopped.get("loop_state").and_then(JsonValue::as_str),
        Some("completed")
    );
    assert_eq!(
        stopped
            .pointer("/completion_evidence/source")
            .and_then(JsonValue::as_str),
        Some("verify")
    );
}

#[tokio::test]
async fn completion_evidence_stale_after_new_action_blocks_completed_stop() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    invoke(
        &handler,
        serde_json::json!({
            "action": "verify",
            "session_id": session_id,
            "expect": { "visible_text": "OK" }
        }),
    )
    .await;
    invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": { "type": "wait", "wait_ms": 1 }
        }),
    )
    .await;

    let error = invoke_result(
        &handler,
        serde_json::json!({
            "action": "stop",
            "session_id": session_id,
            "outcome": "completed"
        }),
    )
    .await
    .expect_err("stale completion evidence must block completed stop");

    assert!(
        error.to_string().contains("completion_evidence_required"),
        "{error}"
    );
}
