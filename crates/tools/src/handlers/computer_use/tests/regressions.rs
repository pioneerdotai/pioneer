use super::*;

async fn session_status(handler: &ComputerUseHandler, session_id: u64) -> JsonValue {
    invoke(
        handler,
        serde_json::json!({
            "action": "status",
            "session_id": session_id,
        }),
    )
    .await
}

async fn assert_invalid_act_fixture_does_not_mutate_session(
    handler: &ComputerUseHandler,
    session_id: u64,
    act: JsonValue,
    expected_error: &str,
) {
    let before = session_status(handler, session_id).await;

    let error = invoke_result(
        handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": act,
        }),
    )
    .await
    .expect_err("malformed action fixture must fail");
    assert!(
        error.to_string().contains(expected_error),
        "expected error containing `{expected_error}`, got `{error}`"
    );

    let after = session_status(handler, session_id).await;
    for pointer in [
        "/status",
        "/loop_state",
        "/step_count",
        "/last_action",
        "/recovery/attempts_current_step",
        "/recovery/attempts_run",
    ] {
        assert_eq!(
            before.pointer(pointer),
            after.pointer(pointer),
            "malformed action fixture mutated session field {pointer}"
        );
    }
}

#[tokio::test]
async fn computer_use_regression_unsupported_open_action_is_rejected_without_session_mutation() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    // Generic class: LLM invents a high-level OS action instead of using start/list_apps/semantic actions.
    assert_invalid_act_fixture_does_not_mutate_session(
        &handler,
        session_id,
        serde_json::json!({
            "type": "open",
            "target": { "node_id": "n2" }
        }),
        "unsupported act.type `open`",
    )
    .await;
}

#[tokio::test]
async fn computer_use_regression_input_key_multiple_keys_is_rejected_without_session_mutation() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    // Generic class: LLM sends a chord through the single-key action shape.
    assert_invalid_act_fixture_does_not_mutate_session(
        &handler,
        session_id,
        serde_json::json!({
            "type": "input_key",
            "keys": ["meta", "space"]
        }),
        "input_key requires exactly one key",
    )
    .await;
}

#[tokio::test]
async fn computer_use_regression_nested_input_chord_action_is_rejected_without_session_mutation() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    // Generic class: LLM nests another action object inside act instead of using flat schema fields.
    assert_invalid_act_fixture_does_not_mutate_session(
        &handler,
        session_id,
        serde_json::json!({
            "type": "input_chord",
            "action": {
                "type": "input_chord",
                "keys": ["meta", "space"]
            }
        }),
        "unknown field `action`",
    )
    .await;
}

#[tokio::test]
async fn computer_use_regression_direct_input_click_point_is_rejected_without_session_mutation() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    // Generic class: LLM places coordinates directly on act; valid shape is target.point.
    assert_invalid_act_fixture_does_not_mutate_session(
        &handler,
        session_id,
        serde_json::json!({
            "type": "input_click",
            "point": { "x": 745, "y": 2060 }
        }),
        "unknown field `point`",
    )
    .await;
}

#[tokio::test]
async fn computer_use_regression_action_failure_returns_actionable_diagnostics() {
    let config = ComputerUseToolsConfig {
        runtime_home_dir: std::env::temp_dir().join(format!(
            "pioneer-computer-use-regression-input-disabled-{}",
            chrono::Utc::now().timestamp_millis()
        )),
        artifacts_subdir: "tools/computer_use".to_owned(),
        input_simulation_enabled: false,
        ..ComputerUseToolsConfig::default()
    };
    let handler =
        ComputerUseHandler::with_backend(config, Arc::new(MockComputerUseBackend::default()));
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "diagnose action failure",
            "target": { "type": "screen" }
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");

    // Generic class: failed execution must tell the LLM what not to retry and what class failed.
    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "input_click",
                "target": { "point": { "x": 10, "y": 20 } }
            }
        }),
    )
    .await;

    assert_eq!(
        result.get("failure_class").and_then(JsonValue::as_str),
        Some("input_simulation_unavailable")
    );
    assert_eq!(
        result.pointer("/result/status").and_then(JsonValue::as_str),
        Some("failed")
    );
    let instruction = result
        .pointer("/llm_context/instruction")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    assert!(instruction.contains("Do not retry input_* actions"));
}

#[tokio::test]
async fn computer_use_regression_start_rejects_unknown_top_level_active_app_type() {
    let (handler, _) = test_handler();

    // Generic class: LLM puts target.type at the top level; this must not be silently ignored.
    let error = invoke_result(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "use the active app",
            "type": "active_app"
        }),
    )
    .await
    .expect_err("unknown top-level type must be rejected");

    assert!(error.to_string().contains("unknown field `type`"));
    assert!(error.to_string().contains("$.type"));
    assert!(error.to_string().contains("Example:"));
}

#[tokio::test]
async fn computer_use_regression_start_rejects_unknown_target_fields() {
    let (handler, _) = test_handler();

    // Generic class: LLM invents target fields; strict contract must fail before start mutates state.
    let error = invoke_result(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "open app",
            "target": {
                "type": "app_name",
                "name": "MockApp",
                "unexpected_identity_hint": "com.example.MockApp"
            }
        }),
    )
    .await
    .expect_err("unknown target field must be rejected");

    assert!(
        error
            .to_string()
            .contains("$.target.unexpected_identity_hint")
    );
    assert!(error.to_string().contains("Accepted shape:"));
}

#[tokio::test]
async fn computer_use_regression_start_requires_non_empty_goal() {
    let (handler, _) = test_handler();

    let error = invoke_result(
        &handler,
        serde_json::json!({
            "action": "start",
            "target": { "type": "screen" }
        }),
    )
    .await
    .expect_err("start without goal must be rejected");

    assert!(error.to_string().contains("$.goal"));
    assert!(error.to_string().contains("start requires non-empty goal"));
}

#[tokio::test]
async fn computer_use_regression_snapshot_rejects_stray_act_field() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "snapshot contract test",
            "target": { "type": "screen" }
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");

    let error = invoke_result(
        &handler,
        serde_json::json!({
            "action": "snapshot",
            "session_id": session_id,
            "act": { "type": "wait", "wait_ms": 1 }
        }),
    )
    .await
    .expect_err("snapshot must reject act field");

    assert!(error.to_string().contains("$.act"));
    assert!(
        error
            .to_string()
            .contains("field is not accepted for action `snapshot`")
    );
}

#[tokio::test]
async fn computer_use_regression_preflight_rejects_session_fields() {
    let (handler, _) = test_handler();

    let error = invoke_result(
        &handler,
        serde_json::json!({
            "action": "preflight",
            "session_id": 1
        }),
    )
    .await
    .expect_err("preflight must reject session_id");

    assert!(error.to_string().contains("$.session_id"));
    assert!(
        error
            .to_string()
            .contains("field is not accepted for action `preflight`")
    );
}

#[tokio::test]
#[ignore = "WP-27 app identity resolution must reject ambiguous/localized name mismatches with a targeted diagnostic"]
async fn computer_use_regression_app_name_mismatch_is_diagnostic_not_generic_not_found() {
    let (handler, _) = test_handler();

    // Generic class: requested app name differs from real/localized identity and needs actionable remediation.
    let error = invoke_result(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "open calculator",
            "target": {
                "type": "app_name",
                "name": "OtherExampleApp"
            }
        }),
    )
    .await
    .expect_err("app identity mismatch must fail diagnostically");

    assert!(error.to_string().contains("app_identity_mismatch"));
}

#[tokio::test]
async fn computer_use_regression_completed_stop_requires_final_evidence() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    // Generic class: the loop must not report success without post-action evidence that the goal is done.
    let error = invoke_result(
        &handler,
        serde_json::json!({
            "action": "stop",
            "session_id": session_id,
            "outcome": "completed",
            "reason": "goal_completed"
        }),
    )
    .await
    .expect_err("completion without evidence must be rejected");

    assert!(error.to_string().contains("completion_evidence_required"));
}
