use super::*;

#[tokio::test]
async fn computer_use_tree_is_included_for_app_targeted_snapshot() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "snapshot app tree",
            "target": {
                "type": "app_name",
                "name": "MockApp"
            }
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");

    let snap = invoke(
        &handler,
        serde_json::json!({
            "action": "snapshot",
            "session_id": session_id
        }),
    )
    .await;

    assert_eq!(
        snap.pointer("/accessibility_tree/status")
            .and_then(JsonValue::as_str),
        Some("ok")
    );
    assert_eq!(
        snap.pointer("/accessibility_tree/nodes/1/role")
            .and_then(JsonValue::as_str),
        Some("button")
    );
    assert!(
        snap.pointer("/accessibility_tree/nodes/1/supported_act_types")
            .and_then(JsonValue::as_array)
            .is_some_and(|values| values.iter().any(|value| value.as_str() == Some("press")))
    );
    assert!(
        snap.pointer("/llm_context/instruction")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .contains("supported_act_types")
    );
    assert!(
        snap.pointer("/accessibility_tree/nodes/1/selector_hints")
            .and_then(JsonValue::as_array)
            .is_some_and(|hints| hints
                .iter()
                .any(|hint| hint.as_str() == Some("button[stable_id=\"mock-ok-button\"]")))
    );
    assert_eq!(
        snap.pointer("/llm_context/accessibility_tree/status")
            .and_then(JsonValue::as_str),
        Some("ok")
    );
    assert_eq!(
        snap.pointer("/progress_signals/target_exists")
            .and_then(JsonValue::as_bool),
        Some(true)
    );
    assert_eq!(
        snap.pointer("/progress_signals/tree_hash_changed")
            .and_then(JsonValue::as_bool),
        Some(false)
    );
    assert_eq!(
        snap.pointer("/llm_context/progress_signals/target_exists")
            .and_then(JsonValue::as_bool),
        Some(true)
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
            .pointer("/last_accessibility_tree/status")
            .and_then(JsonValue::as_str),
        Some("ok")
    );
    assert_eq!(
        status
            .pointer("/last_node_ref_count")
            .and_then(JsonValue::as_u64),
        Some(2)
    );
    assert_eq!(
        status
            .pointer("/last_progress_signals/target_exists")
            .and_then(JsonValue::as_bool),
        Some(true)
    );
}

#[tokio::test]
async fn computer_use_tree_is_absent_with_reason_for_screen_target_snapshot() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "screen no tree",
            "target": { "type": "screen" }
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");

    let snap = invoke(
        &handler,
        serde_json::json!({
            "action": "snapshot",
            "session_id": session_id
        }),
    )
    .await;

    assert_eq!(
        snap.pointer("/accessibility_tree/status")
            .and_then(JsonValue::as_str),
        Some("absent")
    );
    assert!(
        snap.pointer("/accessibility_tree/reason")
            .and_then(JsonValue::as_str)
            .is_some_and(|reason| reason.contains("screen target"))
    );
}

#[tokio::test]
async fn progress_signals_detect_accessibility_state_change() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    let action = invoke(
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
        action.get("status").and_then(JsonValue::as_str),
        Some("running")
    );

    let snap = invoke(
        &handler,
        serde_json::json!({
            "action": "snapshot",
            "session_id": session_id
        }),
    )
    .await;

    assert_eq!(
        snap.pointer("/progress_signals/no_progress")
            .and_then(JsonValue::as_bool),
        Some(false)
    );
    assert_eq!(
        snap.pointer("/progress_signals/tree_hash_changed")
            .and_then(JsonValue::as_bool),
        Some(true)
    );
    assert_eq!(
        snap.pointer("/progress_signals/focused_node_changed")
            .and_then(JsonValue::as_bool),
        Some(true)
    );
    assert_eq!(
        snap.pointer("/progress_signals/selected_node_changed")
            .and_then(JsonValue::as_bool),
        Some(true)
    );
    assert!(
        snap.pointer("/progress_signals/changed_signals")
            .and_then(JsonValue::as_array)
            .is_some_and(|signals| signals
                .iter()
                .any(|signal| signal.as_str() == Some("target_node")))
    );
}

#[tokio::test]
async fn progress_signals_feed_no_progress_loop_guard_for_noop_action() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": { "type": "wait", "wait_ms": 1 }
        }),
    )
    .await;
    let snap = invoke(
        &handler,
        serde_json::json!({
            "action": "snapshot",
            "session_id": session_id
        }),
    )
    .await;

    assert_eq!(
        snap.pointer("/progress_signals/no_progress")
            .and_then(JsonValue::as_bool),
        Some(true)
    );
    assert_eq!(
        snap.pointer("/loop_guard/consecutive_no_progress_steps")
            .and_then(JsonValue::as_u64),
        Some(1)
    );
}

#[tokio::test]
async fn progress_signals_track_verification_failed_to_passed_transition() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    invoke(
        &handler,
        serde_json::json!({
            "action": "verify",
            "session_id": session_id,
            "expect": { "visible_text": "Missing Text" }
        }),
    )
    .await;
    invoke(
        &handler,
        serde_json::json!({
            "action": "verify",
            "session_id": session_id,
            "expect": { "visible_text": "OK" }
        }),
    )
    .await;

    let snap = invoke(
        &handler,
        serde_json::json!({
            "action": "snapshot",
            "session_id": session_id
        }),
    )
    .await;

    assert_eq!(
        snap.pointer("/progress_signals/verification_failed_to_passed")
            .and_then(JsonValue::as_bool),
        Some(true)
    );
    assert_eq!(
        snap.pointer("/progress_signals/no_progress")
            .and_then(JsonValue::as_bool),
        Some(false)
    );
}
