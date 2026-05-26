use super::*;

async fn exercise_safe_session_tail(handler: &ComputerUseHandler, session_id: u64) {
    let first_snapshot = invoke(
        handler,
        serde_json::json!({ "action": "snapshot", "session_id": session_id }),
    )
    .await;
    assert!(
        first_snapshot
            .pointer("/llm_context/attachment/path")
            .is_some()
    );

    let act = invoke(
        handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": { "type": "wait", "wait_ms": 10 }
        }),
    )
    .await;
    assert_eq!(act.get("action").and_then(JsonValue::as_str), Some("act"));

    let second_snapshot = invoke(
        handler,
        serde_json::json!({ "action": "snapshot", "session_id": session_id }),
    )
    .await;
    assert!(
        second_snapshot
            .pointer("/llm_context/attachment/path")
            .is_some()
    );

    let status = invoke(
        handler,
        serde_json::json!({ "action": "status", "session_id": session_id }),
    )
    .await;
    assert_eq!(
        status.get("session_id").and_then(JsonValue::as_u64),
        Some(session_id)
    );

    let stopped = invoke(
        handler,
        serde_json::json!({ "action": "stop", "session_id": session_id }),
    )
    .await;
    assert_eq!(
        stopped.get("status").and_then(JsonValue::as_str),
        Some("stopped")
    );
}

#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "real macOS desktop smoke test; requires Accessibility and Screen Recording permissions"]
async fn computer_use_macos_real_desktop_smoke_test() {
    let unique = NEXT_TEST_ROOT_ID.fetch_add(1, Ordering::SeqCst);
    let root = std::env::temp_dir().join(format!(
        "pioneer-computer-use-real-macos-{}-{}",
        chrono::Utc::now().timestamp_millis(),
        unique
    ));
    let handler = ComputerUseHandler::new(ComputerUseToolsConfig {
        runtime_home_dir: root,
        artifacts_subdir: "tools/computer_use".to_owned(),
        ..ComputerUseToolsConfig::default()
    });
    let preflight = invoke(&handler, serde_json::json!({ "action": "preflight" })).await;
    if preflight.get("status").and_then(JsonValue::as_str) == Some("blocked") {
        eprintln!(
            "computer_use macOS smoke preflight is blocked. Enable Accessibility and Screen Recording for the running binary, then rerun with --ignored."
        );
    }
    assert!(
        preflight
            .get("status")
            .and_then(JsonValue::as_str)
            .is_some()
    );

    let apps = invoke(&handler, serde_json::json!({ "action": "list_apps" })).await;
    assert!(apps.get("apps").and_then(JsonValue::as_array).is_some());

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "safe macOS smoke test",
            // Ignored manual smoke fixture: this real app name is not product behavior.
            "target": { "type": "app_name", "name": "Finder", "launch_if_missing": true }
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");
    if let Some(home) = std::env::var_os("HOME") {
        // Ignored manual smoke fixture: this real folder is only for local macOS QA.
        let downloads = std::path::PathBuf::from(home).join("Downloads");
        if downloads.exists() {
            let open_downloads = invoke(
                &handler,
                serde_json::json!({
                    "action": "act",
                    "session_id": session_id,
                    "act": { "type": "open_path", "path": downloads }
                }),
            )
            .await;
            assert_eq!(
                open_downloads.get("action").and_then(JsonValue::as_str),
                Some("act")
            );
        }
    }
    let open_calculator = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            // Ignored manual smoke fixture: this real app name is not product behavior.
            "act": { "type": "open_app", "app": "Calculator" }
        }),
    )
    .await;
    assert_eq!(
        open_calculator.get("action").and_then(JsonValue::as_str),
        Some("act")
    );
    exercise_safe_session_tail(&handler, session_id).await;
}

#[cfg(target_os = "windows")]
#[tokio::test]
#[ignore = "real Windows desktop smoke test; requires UI Automation desktop session"]
async fn computer_use_windows_real_desktop_smoke_test() {
    let unique = NEXT_TEST_ROOT_ID.fetch_add(1, Ordering::SeqCst);
    let root = std::env::temp_dir().join(format!(
        "pioneer-computer-use-real-windows-{}-{}",
        chrono::Utc::now().timestamp_millis(),
        unique
    ));
    let handler = ComputerUseHandler::new(ComputerUseToolsConfig {
        runtime_home_dir: root,
        artifacts_subdir: "tools/computer_use".to_owned(),
        ..ComputerUseToolsConfig::default()
    });
    let preflight = invoke(&handler, serde_json::json!({ "action": "preflight" })).await;
    if preflight.get("status").and_then(JsonValue::as_str) == Some("blocked") {
        eprintln!(
            "computer_use Windows smoke preflight is blocked. Ensure an interactive desktop session and UI Automation/screenshot permissions, then rerun with --ignored."
        );
    }
    assert!(
        preflight
            .get("status")
            .and_then(JsonValue::as_str)
            .is_some()
    );
    let apps = invoke(&handler, serde_json::json!({ "action": "list_apps" })).await;
    assert!(apps.get("apps").and_then(JsonValue::as_array).is_some());

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "safe Windows active app snapshot smoke test",
            "target": { "type": "active_app" }
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");
    exercise_safe_session_tail(&handler, session_id).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "real Linux desktop smoke test; requires AT-SPI desktop session and app accessibility bridge"]
async fn computer_use_linux_real_desktop_smoke_test() {
    let unique = NEXT_TEST_ROOT_ID.fetch_add(1, Ordering::SeqCst);
    let root = std::env::temp_dir().join(format!(
        "pioneer-computer-use-real-linux-{}-{}",
        chrono::Utc::now().timestamp_millis(),
        unique
    ));
    let handler = ComputerUseHandler::new(ComputerUseToolsConfig {
        runtime_home_dir: root,
        artifacts_subdir: "tools/computer_use".to_owned(),
        ..ComputerUseToolsConfig::default()
    });
    let preflight = invoke(&handler, serde_json::json!({ "action": "preflight" })).await;
    if preflight.get("status").and_then(JsonValue::as_str) == Some("blocked") {
        eprintln!(
            "computer_use Linux smoke preflight is blocked. Ensure AT-SPI is available and screenshot portal/desktop permissions are granted, then rerun with --ignored."
        );
    }
    assert!(
        preflight
            .get("status")
            .and_then(JsonValue::as_str)
            .is_some()
    );
    let apps = invoke(&handler, serde_json::json!({ "action": "list_apps" })).await;
    assert!(apps.get("apps").and_then(JsonValue::as_array).is_some());

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "safe Linux active app snapshot smoke test",
            "target": { "type": "active_app" }
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");
    exercise_safe_session_tail(&handler, session_id).await;
}
