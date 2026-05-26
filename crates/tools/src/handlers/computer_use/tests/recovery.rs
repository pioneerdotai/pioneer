use super::*;

#[tokio::test]
async fn computer_use_start_app_not_found_reports_failure_class() {
    let (handler, _) = test_handler();

    let error = invoke_result(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "missing app",
            "target": {
                "type": "app_name",
                "name": "MissingApp"
            }
        }),
    )
    .await
    .expect_err("missing app must fail");

    assert!(
        error.to_string().contains("failure_class=app_not_found"),
        "{error}"
    );
}

#[tokio::test]
async fn session_id_continues_after_handler_restart() {
    let unique = NEXT_TEST_ROOT_ID.fetch_add(1, Ordering::SeqCst);
    let root = std::env::temp_dir().join(format!(
        "pioneer-computer-use-remote-only-restart-{}-{}",
        chrono::Utc::now().timestamp_millis(),
        unique
    ));

    let config = ComputerUseToolsConfig {
        runtime_home_dir: root.clone(),
        artifacts_subdir: "tools/computer_use".to_owned(),
        ..ComputerUseToolsConfig::default()
    };

    let handler_a = ComputerUseHandler::with_backend(
        config.clone(),
        Arc::new(MockComputerUseBackend::default()),
    );
    let started_a = invoke(
        &handler_a,
        serde_json::json!({"action": "start", "goal": "a", "target": { "type": "screen" }}),
    )
    .await;
    let session_a = started_a
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session a");

    let handler_b =
        ComputerUseHandler::with_backend(config, Arc::new(MockComputerUseBackend::default()));
    let started_b = invoke(
        &handler_b,
        serde_json::json!({"action": "start", "goal": "b", "target": { "type": "screen" }}),
    )
    .await;
    let session_b = started_b
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session b");

    assert!(session_b > session_a);
}

#[tokio::test]
async fn repeated_snapshot_hash_guard_stops_session() {
    let (handler, _) = test_handler();

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "loop guard test",
            "target": { "type": "screen" }
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");

    let mut last = serde_json::json!({});
    for _ in 0..7 {
        last = invoke(
            &handler,
            serde_json::json!({
                "action": "snapshot",
                "session_id": session_id,
            }),
        )
        .await;
    }

    assert_eq!(
        last.get("status").and_then(JsonValue::as_str),
        Some("stopped")
    );
    assert!(
        last.get("stop_reason")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .contains("loop_guard")
    );
    assert_eq!(
        last.get("failure_class").and_then(JsonValue::as_str),
        Some("loop_guard_triggered")
    );
}

#[tokio::test]
async fn repeated_action_signature_guard_stops_session() {
    let (handler, _) = test_handler();

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "action guard test",
            "target": { "type": "app_name", "name": "MockApp" }
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");

    let _ = invoke(
        &handler,
        serde_json::json!({
            "action": "snapshot",
            "session_id": session_id,
        }),
    )
    .await;
    let snapshot_id = latest_snapshot_id(&handler, session_id).await;

    let mut last = serde_json::json!({});
    for _ in 0..9 {
        last = invoke(
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
        if last.get("status").and_then(JsonValue::as_str) == Some("stopped") {
            break;
        }
    }

    assert_eq!(
        last.get("status").and_then(JsonValue::as_str),
        Some("stopped")
    );
    assert_eq!(
        last.get("failure_class").and_then(JsonValue::as_str),
        Some("loop_guard_triggered")
    );
}

#[tokio::test]
async fn recovery_budget_guard_stops_session() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "recovery budget test",
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
            "action": "act",
            "session_id": session_id,
            "recovery_attempt": 3,
            "failure_class": "element_stale",
            "act": {
                "type": "wait",
                "wait_ms": 1
            }
        }),
    )
    .await;

    assert_eq!(
        result.get("status").and_then(JsonValue::as_str),
        Some("stopped")
    );
    assert_eq!(
        result.get("failure_class").and_then(JsonValue::as_str),
        Some("recovery_budget_exceeded")
    );
}

#[tokio::test]
async fn non_retryable_failure_class_stops_immediately() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "non retryable failure class test",
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
            "action": "act",
            "session_id": session_id,
            "failure_class": "action_not_supported",
            "recovery_attempt": 1,
            "act": {
                "type": "wait",
                "wait_ms": 1
            }
        }),
    )
    .await;

    assert_eq!(
        result.get("status").and_then(JsonValue::as_str),
        Some("stopped")
    );
    assert_eq!(
        result.get("loop_state").and_then(JsonValue::as_str),
        Some("failed")
    );
    assert_eq!(
        result.get("failure_class").and_then(JsonValue::as_str),
        Some("action_not_supported")
    );
}

#[tokio::test]
async fn stop_with_completed_outcome_sets_completed_state() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "complete this test",
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
            "session_id": session_id,
        }),
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
        stopped.get("status").and_then(JsonValue::as_str),
        Some("stopped")
    );
    assert_eq!(
        stopped.get("loop_state").and_then(JsonValue::as_str),
        Some("completed")
    );
    assert_eq!(stopped.get("failure_class"), Some(&JsonValue::Null));
}

#[tokio::test]
async fn long_snapshot_act_series_does_not_trigger_loop_guards() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "long run",
            "target": { "type": "app_name", "name": "MockApp" },
            "max_steps": 80
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");

    for step in 0..40 {
        let snap = invoke(
            &handler,
            serde_json::json!({
                "action": "snapshot",
                "session_id": session_id
            }),
        )
        .await;
        assert_eq!(
            snap.get("status").and_then(JsonValue::as_str),
            Some("running")
        );
        let snapshot_id = snap
            .pointer("/snapshot/snapshot_id")
            .and_then(JsonValue::as_str)
            .expect("snapshot id");

        let action_type = if step % 2 == 0 { "press" } else { "focus" };
        let act = invoke(
            &handler,
            serde_json::json!({
                "action": "act",
                "session_id": session_id,
                "act": {
                    "type": action_type,
                    "target": { "node_id": "n2", "snapshot_id": snapshot_id }
                }
            }),
        )
        .await;
        assert_eq!(
            act.get("status").and_then(JsonValue::as_str),
            Some("running")
        );
    }

    let status = invoke(
        &handler,
        serde_json::json!({
            "action": "status",
            "session_id": session_id
        }),
    )
    .await;
    assert_eq!(
        status.get("step_count").and_then(JsonValue::as_u64),
        Some(40)
    );
    assert_eq!(
        status.get("status").and_then(JsonValue::as_str),
        Some("running")
    );
}

#[tokio::test]
async fn computer_use_recovery_nonfatal_action_failure_keeps_session_running_and_allows_snapshot() {
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

    assert_eq!(
        result.get("status").and_then(JsonValue::as_str),
        Some("running")
    );
    assert_eq!(
        result.pointer("/result/status").and_then(JsonValue::as_str),
        Some("failed")
    );
    assert!(
        matches!(
            result.get("failure_class").and_then(JsonValue::as_str),
            Some("element_not_found" | "element_stale")
        ),
        "{result}"
    );
    assert_eq!(
        result
            .pointer("/next_call/arguments/action")
            .and_then(JsonValue::as_str),
        Some("snapshot")
    );

    let snapshot = invoke(
        &handler,
        serde_json::json!({
            "action": "snapshot",
            "session_id": session_id
        }),
    )
    .await;
    assert_eq!(
        snapshot.get("status").and_then(JsonValue::as_str),
        Some("running")
    );
}

#[tokio::test]
async fn computer_use_recovery_nonfatal_action_failure_repetition_still_hits_loop_guard() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;
    let snapshot_id = latest_snapshot_id(&handler, session_id).await;
    let mut last = serde_json::json!({});

    for _ in 0..9 {
        last = invoke(
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
        if last.get("status").and_then(JsonValue::as_str) == Some("stopped") {
            break;
        }
    }

    assert_eq!(
        last.get("status").and_then(JsonValue::as_str),
        Some("stopped")
    );
    assert_eq!(
        last.get("failure_class").and_then(JsonValue::as_str),
        Some("loop_guard_triggered")
    );
}
