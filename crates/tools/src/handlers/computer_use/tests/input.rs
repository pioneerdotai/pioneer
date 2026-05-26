use super::*;

#[tokio::test]
async fn computer_use_input_action_click_point() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "input click point",
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
                "type": "input_click",
                "target": {
                        "point": {
                            "x": 10,
                            "y": 20,
                            "coordinate_space": "native_input"
                        }
                }
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
            .pointer("/result/input_targets/target/point/x")
            .and_then(JsonValue::as_i64),
        Some(10)
    );
    assert_eq!(
        result
            .pointer("/result/input_targets/target/point/coordinate_space")
            .and_then(JsonValue::as_str),
        Some("native_input")
    );
}

#[tokio::test]
async fn computer_use_input_action_click_element_anchor() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;
    let snapshot_id = latest_snapshot_id(&handler, session_id).await;

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "input_click",
                "target": {
                    "bounds_anchor": { "node_id": "n2", "snapshot_id": snapshot_id, "anchor": "center" }
                }
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
            .pointer("/result/input_targets/target/point/x")
            .and_then(JsonValue::as_i64),
        Some(50)
    );
    assert_eq!(
        result
            .pointer("/result/input_targets/target/point/y")
            .and_then(JsonValue::as_i64),
        Some(36)
    );
    assert_eq!(
        result
            .pointer("/result/input_targets/target/point/coordinate_space")
            .and_then(JsonValue::as_str),
        Some("native_input")
    );
}

#[tokio::test]
async fn computer_use_input_action_chord_and_scroll_parse() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "input chord and scroll",
            "target": { "type": "screen" }
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");

    for act in [
        serde_json::json!({ "type": "input_chord", "keys": ["meta", "a"] }),
        serde_json::json!({ "type": "input_scroll", "target": { "point": { "x": 1, "y": 2, "coordinate_space": "native_input" } }, "delta_y": -3 }),
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
async fn coordinate_space_defaults_untransformed_bare_point_to_source_pixels() {
    let (handler, _) =
        test_handler_with_backend(MockComputerUseBackend::default().with_scale_factor(1.0));
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "default coordinate space",
            "target": { "type": "screen" }
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
        result
            .pointer("/result/input_targets/target/point/coordinate_space")
            .and_then(JsonValue::as_str),
        Some("native_input")
    );
    assert_eq!(
        result
            .pointer("/result/input_targets/target/requested_point/coordinate_space")
            .and_then(JsonValue::as_str),
        Some("source_pixels")
    );
}

#[tokio::test]
async fn coordinate_space_rejects_bare_point_after_transformed_snapshot() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "transformed coordinate space",
            "target": { "type": "screen" },
            "snapshot_max_side_px": 320
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");
    let snapshot = invoke(
        &handler,
        serde_json::json!({
            "action": "snapshot",
            "session_id": session_id
        }),
    )
    .await;
    assert_ne!(
        snapshot
            .pointer("/snapshot/transport_width_px")
            .and_then(JsonValue::as_u64),
        snapshot
            .pointer("/snapshot/width_px")
            .and_then(JsonValue::as_u64)
    );

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
        result.pointer("/result/status").and_then(JsonValue::as_str),
        Some("failed")
    );
    assert!(
        result
            .pointer("/result/execution/message")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .contains("coordinate_space_required")
    );
}

#[tokio::test]
async fn coordinate_conversion_converts_transport_point_to_native_input() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "input_click",
                "target": {
                    "point": {
                        "x": 100,
                        "y": 50,
                        "coordinate_space": "transport_pixels"
                    }
                }
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
            .pointer("/result/input_targets/target/requested_point/coordinate_space")
            .and_then(JsonValue::as_str),
        Some("transport_pixels")
    );
    assert_eq!(
        result
            .pointer("/result/input_targets/target/point/coordinate_space")
            .and_then(JsonValue::as_str),
        Some("native_input")
    );
    assert_eq!(
        result
            .pointer("/result/input_targets/target/point/x")
            .and_then(JsonValue::as_i64),
        Some(100)
    );
    assert_eq!(
        result
            .pointer("/result/input_targets/target/point/y")
            .and_then(JsonValue::as_i64),
        Some(50)
    );
    assert_eq!(
        result
            .pointer("/result/coordinate_observability/validation_status")
            .and_then(JsonValue::as_str),
        Some("ok")
    );
    assert_eq!(
        result
            .pointer("/result/coordinate_observability/slots/target/requested_space")
            .and_then(JsonValue::as_str),
        Some("transport_pixels")
    );
    assert_eq!(
        result
            .pointer("/result/coordinate_observability/slots/target/converted_space")
            .and_then(JsonValue::as_str),
        Some("native_input")
    );
    assert_eq!(
        result
            .pointer("/result/coordinate_observability/display_bounds/native_input/width")
            .and_then(JsonValue::as_u64),
        Some(320)
    );
}

#[tokio::test]
async fn coordinate_observability_reports_failed_coordinate_validation() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "input_click",
                "target": {
                    "point": {
                        "x": 640,
                        "y": 10,
                        "coordinate_space": "source_pixels"
                    }
                }
            }
        }),
    )
    .await;

    assert_eq!(
        result
            .pointer("/result/coordinate_observability/validation_status")
            .and_then(JsonValue::as_str),
        Some("failed")
    );
    assert_eq!(
        result
            .pointer("/result/coordinate_observability/slots/target/requested_space")
            .and_then(JsonValue::as_str),
        Some("source_pixels")
    );
    assert!(
        result
            .pointer("/result/coordinate_observability/slots/target/diagnostic")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .contains("coordinate_out_of_bounds")
    );
}

#[tokio::test]
async fn coordinate_conversion_converts_source_point_by_scale_factor() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "input_click",
                "target": {
                    "point": {
                        "x": 20,
                        "y": 40,
                        "coordinate_space": "source_pixels"
                    }
                }
            }
        }),
    )
    .await;

    assert_eq!(
        result
            .pointer("/result/input_targets/target/point/x")
            .and_then(JsonValue::as_i64),
        Some(10)
    );
    assert_eq!(
        result
            .pointer("/result/input_targets/target/point/y")
            .and_then(JsonValue::as_i64),
        Some(20)
    );
}

#[tokio::test]
async fn coordinate_conversion_rejects_out_of_bounds_point() {
    let (handler, _) = test_handler();
    let session_id = start_app_session_with_snapshot(&handler).await;

    let result = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "input_click",
                "target": {
                    "point": {
                        "x": 640,
                        "y": 10,
                        "coordinate_space": "source_pixels"
                    }
                }
            }
        }),
    )
    .await;

    assert_eq!(
        result.pointer("/result/status").and_then(JsonValue::as_str),
        Some("failed")
    );
    assert!(
        result
            .pointer("/result/execution/message")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .contains("coordinate_out_of_bounds")
    );
}

#[tokio::test]
async fn coordinate_conversion_rejects_source_point_without_snapshot_metadata() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "missing snapshot metadata",
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
                "type": "input_click",
                "target": {
                    "point": {
                        "x": 10,
                        "y": 20,
                        "coordinate_space": "source_pixels"
                    }
                }
            }
        }),
    )
    .await;

    assert_eq!(
        result.pointer("/result/status").and_then(JsonValue::as_str),
        Some("failed")
    );
    assert!(
        result
            .pointer("/result/execution/message")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .contains("coordinate_snapshot_required")
    );
}

#[tokio::test]
async fn computer_use_macos_launch_missing_app_when_allowed_with_mock_launcher() {
    let (handler, _) = test_handler();

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "launch missing app",
            "target": {
                "type": "app_name",
                "name": "MissingApp",
                "launch_if_missing": true
            }
        }),
    )
    .await;

    assert_eq!(
        started.pointer("/target/type").and_then(JsonValue::as_str),
        Some("app")
    );
    assert_eq!(
        started
            .pointer("/target/app/name")
            .and_then(JsonValue::as_str),
        Some("MissingApp")
    );
}

#[tokio::test]
async fn computer_use_launch_command_allowlist_blocks_unlisted_command() {
    let config = ComputerUseToolsConfig {
        runtime_home_dir: std::env::temp_dir().join(format!(
            "pioneer-computer-use-launch-allowlist-{}",
            chrono::Utc::now().timestamp_millis()
        )),
        artifacts_subdir: "tools/computer_use".to_owned(),
        allowed_launch_commands: vec!["open -a ExampleApp".to_owned()],
        ..ComputerUseToolsConfig::default()
    };
    let handler =
        ComputerUseHandler::with_backend(config, Arc::new(MockComputerUseBackend::default()));

    let error = invoke_result(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "launch command allowlist",
            "target": {
                "type": "app_name",
                "name": "MissingApp",
                "launch_if_missing": true,
                "launch_command": "open -a OtherExampleApp"
            }
        }),
    )
    .await
    .expect_err("unlisted launch command must fail");

    assert!(error.to_string().contains("allowed_launch_commands"));
}

#[tokio::test]
async fn computer_use_input_simulation_config_blocks_input_actions() {
    let config = ComputerUseToolsConfig {
        runtime_home_dir: std::env::temp_dir().join(format!(
            "pioneer-computer-use-input-disabled-{}",
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
            "goal": "input disabled",
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
                "type": "input_click",
                "target": {
                    "point": {
                        "x": 10,
                        "y": 20,
                        "coordinate_space": "native_input"
                    }
                }
            }
        }),
    )
    .await;

    assert_eq!(
        result.pointer("/failure_class").and_then(JsonValue::as_str),
        Some("input_simulation_unavailable")
    );
}
