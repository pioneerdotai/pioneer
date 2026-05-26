use super::*;

#[tokio::test]
async fn computer_use_semantic_action_press_resolves_node_target() {
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
    assert_eq!(
        result
            .pointer("/result/execution/action_type")
            .and_then(JsonValue::as_str),
        Some("press")
    );
    assert_eq!(
        result
            .pointer("/result/target/selector")
            .and_then(JsonValue::as_str),
        Some(r#"button[stable_id="mock-ok-button"]"#)
    );
}

#[tokio::test]
async fn computer_use_semantic_action_focus_set_value_and_type_text_parse() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;
    let snapshot_id = latest_snapshot_id(&handler, session_id).await;

    for act in [
        serde_json::json!({ "type": "focus", "target": { "node_id": "n2", "snapshot_id": snapshot_id.clone() } }),
        serde_json::json!({ "type": "set_value", "target": { "node_id": "n2", "snapshot_id": snapshot_id.clone() }, "text": "hello" }),
        serde_json::json!({ "type": "type_text", "target": { "node_id": "n2", "snapshot_id": snapshot_id.clone() }, "text": " world" }),
    ] {
        let result = invoke(
            &handler,
            serde_json::json!({
                "action": "act",
                "session_id": session_id,
                "act": act,
            }),
        )
        .await;
        assert_eq!(
            result.pointer("/result/status").and_then(JsonValue::as_str),
            Some("ok")
        );
    }
}

#[tokio::test]
async fn computer_use_os_action_open_app_uses_backend_dispatch() {
    let backend = Arc::new(MockComputerUseBackend::default());
    let unique = NEXT_TEST_ROOT_ID.fetch_add(1, Ordering::SeqCst);
    let root = std::env::temp_dir().join(format!(
        "pioneer-computer-use-os-action-{}-{}",
        chrono::Utc::now().timestamp_millis(),
        unique
    ));
    let config = ComputerUseToolsConfig {
        runtime_home_dir: root,
        artifacts_subdir: "tools/computer_use".to_owned(),
        ..ComputerUseToolsConfig::default()
    };
    let handler = ComputerUseHandler::with_backend(config, backend.clone());
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "open app using OS action",
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
            "act": {
                "type": "open_app",
                "app": "ExampleApp"
            }
        }),
    )
    .await;

    assert_eq!(
        result.pointer("/result/status").and_then(JsonValue::as_str),
        Some("ok")
    );
    assert_eq!(
        result
            .pointer("/result/execution/action_type")
            .and_then(JsonValue::as_str),
        Some("open_app")
    );
    assert_eq!(backend.os_action_types(), vec!["open_app".to_owned()]);

    let status = invoke(
        &handler,
        serde_json::json!({
            "action": "status",
            "session_id": session_id
        }),
    )
    .await;
    assert_eq!(
        status.pointer("/target/type").and_then(JsonValue::as_str),
        Some("app")
    );
    assert_eq!(
        status
            .pointer("/target/app/name")
            .and_then(JsonValue::as_str),
        Some("ExampleApp")
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
        snapshot
            .pointer("/accessibility_tree/status")
            .and_then(JsonValue::as_str),
        Some("ok")
    );
}

#[tokio::test]
async fn activate_app_updates_session_target_without_input_simulation() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "activate app using OS action",
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
            "act": {
                "type": "activate_app",
                "app": "MockApp"
            }
        }),
    )
    .await;

    assert_eq!(
        result.pointer("/result/status").and_then(JsonValue::as_str),
        Some("ok")
    );
    assert_eq!(
        result
            .pointer("/result/execution/app_after/name")
            .and_then(JsonValue::as_str),
        Some("MockApp")
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
        status.pointer("/target/type").and_then(JsonValue::as_str),
        Some("app")
    );
}

#[tokio::test]
async fn open_app_failure_reports_structured_diagnostic_without_target_corruption() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "open missing app",
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
            "act": {
                "type": "open_app",
                "app": "UnlaunchableApp"
            }
        }),
    )
    .await;

    assert_eq!(
        result.get("failure_class").and_then(JsonValue::as_str),
        Some("app_not_found")
    );
    assert_eq!(
        result
            .pointer("/result/execution/app_after")
            .and_then(JsonValue::as_object),
        None
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
        status.pointer("/target/type").and_then(JsonValue::as_str),
        Some("screen")
    );
}

#[tokio::test]
async fn open_path_returns_verification_hints() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "open temp directory",
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
            "act": {
                "type": "open_path",
                "path": std::env::temp_dir().display().to_string()
            }
        }),
    )
    .await;

    assert_eq!(
        result.pointer("/result/status").and_then(JsonValue::as_str),
        Some("ok")
    );
    assert!(
        result
            .pointer("/result/execution/details/verification_hint")
            .and_then(JsonValue::as_str)
            .is_some()
    );
}

#[tokio::test]
async fn reveal_path_returns_verification_hints() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "reveal temp directory",
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
            "act": {
                "type": "reveal_path",
                "path": std::env::temp_dir().display().to_string()
            }
        }),
    )
    .await;

    assert_eq!(
        result.pointer("/result/status").and_then(JsonValue::as_str),
        Some("ok")
    );
    assert_eq!(
        result
            .pointer("/result/execution/details/expected_app")
            .and_then(JsonValue::as_str),
        Some("system_file_manager")
    );
}

#[tokio::test]
async fn open_url_returns_verification_hints() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "open url",
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
            "act": {
                "type": "open_url",
                "url": "https://example.com"
            }
        }),
    )
    .await;

    assert_eq!(
        result.pointer("/result/status").and_then(JsonValue::as_str),
        Some("ok")
    );
    assert_eq!(
        result
            .pointer("/result/execution/details/expected_app")
            .and_then(JsonValue::as_str),
        Some("system_url_handler")
    );
}

#[tokio::test]
async fn menu_action_select_menu_item_returns_expected_after_hint() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "select app menu item",
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
            "act": {
                "type": "select_menu_item",
                "app": "MockApp",
                "menu_path": ["File", "New Window"]
            }
        }),
    )
    .await;

    assert_eq!(
        result.pointer("/result/status").and_then(JsonValue::as_str),
        Some("ok")
    );
    assert_eq!(
        result
            .pointer("/result/execution/details/expected_after/state")
            .and_then(JsonValue::as_str),
        Some("menu_item_selected")
    );
}

#[tokio::test]
async fn focus_window_updates_app_target_and_returns_expected_after_hint() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "focus a window",
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
            "act": {
                "type": "focus_window",
                "app": "MockApp",
                "title": "Mock"
            }
        }),
    )
    .await;

    assert_eq!(
        result.pointer("/result/status").and_then(JsonValue::as_str),
        Some("ok")
    );
    assert_eq!(
        result
            .pointer("/result/execution/details/expected_after/state")
            .and_then(JsonValue::as_str),
        Some("window_focused")
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
        status.pointer("/target/type").and_then(JsonValue::as_str),
        Some("app")
    );
}
