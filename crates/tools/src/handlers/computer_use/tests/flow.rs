use super::*;

#[tokio::test]
async fn remote_start_snapshot_act_status_stop_flow() {
    let (handler, root) = test_handler();

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "Open app and check state",
            "target": { "type": "app_name", "name": "MockApp" }
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

    let snap = invoke(
        &handler,
        serde_json::json!({
            "action": "snapshot",
            "session_id": session_id,
        }),
    )
    .await;
    let snapshot_path = snap
        .get("snapshot")
        .and_then(|value| value.get("path"))
        .and_then(JsonValue::as_str)
        .expect("snapshot path");
    assert!(std::fs::metadata(snapshot_path).is_ok());
    assert!(
        snap.get("llm_context")
            .and_then(|value| value.get("attachment"))
            .and_then(|value| value.get("path"))
            .and_then(JsonValue::as_str)
            .is_some()
    );
    let snapshot_id = snap
        .pointer("/snapshot/snapshot_id")
        .and_then(JsonValue::as_str)
        .expect("snapshot id");

    let acted = invoke(
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
        acted.get("status").and_then(JsonValue::as_str),
        Some("running")
    );

    let status = invoke(
        &handler,
        serde_json::json!({
            "action": "status",
            "session_id": session_id,
        }),
    )
    .await;
    assert_eq!(
        status.get("mode").and_then(JsonValue::as_str),
        Some("remote")
    );
    assert_eq!(
        status.get("step_count").and_then(JsonValue::as_u64),
        Some(1)
    );
    assert_eq!(
        status.get("snapshot_count").and_then(JsonValue::as_u64),
        Some(1)
    );

    let stopped = invoke(
        &handler,
        serde_json::json!({
            "action": "stop",
            "session_id": session_id,
        }),
    )
    .await;
    assert_eq!(
        stopped.get("status").and_then(JsonValue::as_str),
        Some("stopped")
    );

    let session_dir = root
        .join("tools")
        .join("computer_use")
        .join(session_id.to_string());
    assert!(std::fs::metadata(&session_dir).is_ok());
}

#[tokio::test]
async fn computer_use_start_by_app_name_stores_target_metadata() {
    let (handler, _) = test_handler();

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "use mock app",
            "target": {
                "type": "app_name",
                "name": "MockApp",
                "tree_max_depth": 4
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
        Some("MockApp")
    );
    assert_eq!(
        started
            .pointer("/target/tree_max_depth")
            .and_then(JsonValue::as_u64),
        Some(4)
    );
}

#[tokio::test]
async fn computer_use_start_by_pid_stores_target_metadata() {
    let (handler, _) = test_handler();

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "use mock pid",
            "target": {
                "type": "pid",
                "pid": 42
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
            .pointer("/target/app/pid")
            .and_then(JsonValue::as_u64),
        Some(42)
    );
}

#[tokio::test]
async fn computer_use_start_screen_target_stores_display_metadata() {
    let (handler, _) = test_handler();

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "screen task",
            "target": {
                "type": "screen",
                "display_id": 1
            }
        }),
    )
    .await;

    assert_eq!(
        started.pointer("/target/type").and_then(JsonValue::as_str),
        Some("screen")
    );
    assert_eq!(
        started
            .pointer("/target/display/display_id")
            .and_then(JsonValue::as_u64),
        Some(1)
    );
}

#[tokio::test]
async fn computer_use_list_apps_returns_app_metadata() {
    let (handler, _) = test_handler();

    let listed = invoke(
        &handler,
        serde_json::json!({
            "action": "list_apps"
        }),
    )
    .await;

    assert_eq!(
        listed.pointer("/apps/0/name").and_then(JsonValue::as_str),
        Some("MockApp")
    );
    assert_eq!(
        listed
            .pointer("/apps/0/window_title")
            .and_then(JsonValue::as_str),
        Some("Mock Window")
    );
}
