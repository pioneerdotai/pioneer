use super::*;

#[tokio::test]
async fn computer_use_integration_smoke_mock_desktop_contract() {
    let (handler, _) = test_handler();

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "mock desktop smoke",
            "target": { "type": "app_name", "name": "ExampleApp", "launch_if_missing": true }
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");
    assert_eq!(
        started.pointer("/target/type").and_then(JsonValue::as_str),
        Some("app")
    );

    let snapshot = invoke(
        &handler,
        serde_json::json!({ "action": "snapshot", "session_id": session_id }),
    )
    .await;
    assert!(
        snapshot
            .pointer("/llm_context/attachment/path")
            .and_then(JsonValue::as_str)
            .is_some()
    );
    assert!(
        snapshot
            .pointer("/accessibility_tree/nodes")
            .and_then(JsonValue::as_array)
            .is_some_and(|nodes| !nodes.is_empty())
    );

    let open_path = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "open_path",
                "path": std::env::temp_dir().display().to_string()
            }
        }),
    )
    .await;
    assert_eq!(
        open_path
            .pointer("/result/status")
            .and_then(JsonValue::as_str),
        Some("ok")
    );
    assert!(
        open_path
            .pointer("/result/execution/details/verification_hint")
            .and_then(JsonValue::as_str)
            .is_some()
    );

    let malformed = invoke_result(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": { "type": "open" }
        }),
    )
    .await
    .expect_err("unsupported action must be rejected before execution");
    assert!(malformed.to_string().contains("unsupported act.type"));

    let failed = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "press",
                "target": { "node_id": "missing", "snapshot_id": "s0-0" }
            }
        }),
    )
    .await;
    assert_eq!(
        failed.get("status").and_then(JsonValue::as_str),
        Some("running"),
        "non-fatal failed actions must keep the session alive for recovery"
    );
    assert!(
        failed
            .pointer("/trace/failure_class")
            .and_then(JsonValue::as_str)
            .is_some()
    );

    let premature_stop = invoke_result(
        &handler,
        serde_json::json!({
            "action": "stop",
            "session_id": session_id,
            "outcome": "completed"
        }),
    )
    .await
    .expect_err("completion requires evidence");
    assert!(
        premature_stop
            .to_string()
            .contains("completion_evidence_required")
    );

    let _snapshot_after_action = invoke(
        &handler,
        serde_json::json!({ "action": "snapshot", "session_id": session_id }),
    )
    .await;
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
            .pointer("/verification/status")
            .and_then(JsonValue::as_str),
        Some("passed")
    );
    assert!(
        verification
            .pointer("/completion_evidence/accepted")
            .and_then(JsonValue::as_bool)
            == Some(true),
        "verify must provide completion evidence before completed stop"
    );

    let stopped = invoke(
        &handler,
        serde_json::json!({
            "action": "stop",
            "session_id": session_id,
            "outcome": "completed"
        }),
    )
    .await;
    assert_eq!(
        stopped.get("loop_state").and_then(JsonValue::as_str),
        Some("completed")
    );
}
