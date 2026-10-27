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

    assert_eq!(
        result
            .pointer("/result/target/kind")
            .and_then(JsonValue::as_str),
        Some("locator")
    );

    assert_eq!(
        result
            .pointer("/trace/action_type")
            .and_then(JsonValue::as_str),
        Some("press")
    );
}

#[tokio::test]
async fn computer_use_semantic_action_focus_set_value_and_type_text_parse() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;
    let snapshot_id = latest_snapshot_id(&handler, session_id).await;

    for (act, expected_action_type) in [
        (
            serde_json::json!({ "type": "focus", "target": { "node_id": "n2", "snapshot_id": snapshot_id.clone() } }),
            "focus",
        ),
        (
            serde_json::json!({ "type": "set_value", "target": { "node_id": "n2", "snapshot_id": snapshot_id.clone() }, "text": "hello" }),
            "set_value",
        ),
        (
            serde_json::json!({ "type": "type_text", "target": { "node_id": "n2", "snapshot_id": snapshot_id.clone() }, "text": " world" }),
            "type_text",
        ),
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
        assert_eq!(
            result
                .pointer("/trace/action_type")
                .and_then(JsonValue::as_str),
            Some(expected_action_type)
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
    assert_eq!(
        result.get("status").and_then(JsonValue::as_str),
        Some("running")
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

    assert_eq!(
        result
            .pointer("/trace/action_type")
            .and_then(JsonValue::as_str),
        Some("open_app")
    );
}

#[test]
fn app_target_os_actions_preserve_app_metadata_and_verification_hints() {
    for (goal, act, expected_state) in [
        (
            "activate app using OS action",
            serde_json::json!({ "type": "activate_app", "app": "MockApp" }),
            "app_active",
        ),
        (
            "focus a window",
            serde_json::json!({ "type": "focus_window", "app": "MockApp", "title": "Mock" }),
            "window_focused",
        ),
    ] {
        let body = async {
            let (handler, _) = test_handler();
            let started = invoke(
                &handler,
                serde_json::json!({
                    "action": "start",
                    "goal": goal,
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
                    "act": act
                }),
            )
            .await;

            assert_eq!(
                result.pointer("/result/status").and_then(JsonValue::as_str),
                Some("ok"),
                "{goal}"
            );
            assert_eq!(
                result
                    .pointer("/result/execution/app_after/name")
                    .and_then(JsonValue::as_str),
                Some("MockApp"),
                "{goal}"
            );
            assert_eq!(
                result
                    .pointer("/result/execution/details/expected_after/state")
                    .and_then(JsonValue::as_str),
                Some(expected_state),
                "{goal}"
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
                Some("app"),
                "{goal}"
            );
        };
        tokio::pin!(body);
        let body: std::pin::Pin<&mut dyn std::future::Future<Output = ()>> = body;
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed building the Runtime")
            .block_on(body);
    }
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

    assert_eq!(
        result
            .pointer("/trace/action_type")
            .and_then(JsonValue::as_str),
        Some("select_menu_item")
    );
}

#[tokio::test]
async fn external_os_actions_return_verification_hints() {
    for (goal, act, expected_app, expected_action_type) in [
        (
            "reveal temp directory",
            serde_json::json!({
                "type": "reveal_path",
                "path": std::env::temp_dir().display().to_string()
            }),
            "system_file_manager",
            "reveal_path",
        ),
        (
            "open url",
            serde_json::json!({
                "type": "open_url",
                "url": "https://example.com"
            }),
            "system_url_handler",
            "open_url",
        ),
    ] {
        let (handler, _) = test_handler();
        let started = invoke(
            &handler,
            serde_json::json!({
                "action": "start",
                "goal": goal,
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
                "act": act
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
            Some(expected_app)
        );
        assert_eq!(
            result
                .pointer("/trace/action_type")
                .and_then(JsonValue::as_str),
            Some(expected_action_type)
        );
    }
}
