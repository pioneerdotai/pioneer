use super::*;

#[tokio::test]
async fn session_target_policy_rejects_start_without_explicit_target() {
    let (handler, _) = test_handler();

    let error = invoke_result(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "open requested location"
        }),
    )
    .await
    .expect_err("start without target must fail");

    assert!(
        error.to_string().contains("requires explicit target"),
        "{error}"
    );
    assert!(error.to_string().contains("app_name"), "{error}");
    assert!(error.to_string().contains("screen"), "{error}");
}

#[tokio::test]
async fn session_target_policy_allows_explicit_screen_target() {
    let (handler, _) = test_handler();

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "whole desktop task",
            "target": { "type": "screen" }
        }),
    )
    .await;

    assert_eq!(
        started.pointer("/target/type").and_then(JsonValue::as_str),
        Some("screen")
    );
}

#[tokio::test]
async fn session_target_policy_os_open_app_updates_screen_session_to_app_target() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "open app from screen session",
            "target": { "type": "screen" }
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");

    let acted = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": { "type": "open_app", "app": "MockApp" }
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
        Some("MockApp")
    );
}
