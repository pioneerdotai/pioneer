use super::*;

#[tokio::test]
async fn computer_use_snapshot_writes_png_and_emits_coordinate_metadata() {
    let (handler, _) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "snapshot metadata",
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
    let path = snap
        .pointer("/snapshot/path")
        .and_then(JsonValue::as_str)
        .expect("snapshot path");
    assert!(std::fs::metadata(path).is_ok());
    assert_eq!(snap.get("step").and_then(JsonValue::as_u64), Some(1));
    assert_eq!(
        snap.pointer("/snapshot/width_px")
            .and_then(JsonValue::as_u64),
        Some(640)
    );
    assert_eq!(
        snap.pointer("/snapshot/width").and_then(JsonValue::as_u64),
        Some(640)
    );
    assert_eq!(
        snap.pointer("/snapshot/height_px")
            .and_then(JsonValue::as_u64),
        Some(360)
    );
    assert_eq!(
        snap.pointer("/snapshot/height").and_then(JsonValue::as_u64),
        Some(360)
    );
    assert_eq!(
        snap.pointer("/snapshot/transport_width_px")
            .and_then(JsonValue::as_u64),
        Some(320)
    );
    assert_eq!(
        snap.pointer("/snapshot/transport_height_px")
            .and_then(JsonValue::as_u64),
        Some(180)
    );
    assert_eq!(
        snap.pointer("/snapshot/transport_size/width_px")
            .and_then(JsonValue::as_u64),
        Some(320)
    );
    assert!(
        snap.pointer("/snapshot/timestamp")
            .and_then(JsonValue::as_i64)
            .is_some()
    );
    assert_eq!(
        snap.pointer("/llm_context/attachment/path")
            .and_then(JsonValue::as_str),
        Some(path)
    );
    assert_eq!(
        snap.pointer("/llm_context/coordinate_space/source_width_px")
            .and_then(JsonValue::as_u64),
        Some(640)
    );
    assert_eq!(
        snap.pointer("/llm_context/coordinate_space/transport_width_px")
            .and_then(JsonValue::as_u64),
        Some(320)
    );
}

#[cfg(unix)]
#[tokio::test]
async fn computer_use_snapshot_refuses_symlink_destination_without_touching_target() {
    use std::os::unix::fs::symlink;

    let (handler, root) = test_handler();
    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "secure snapshot",
            "target": { "type": "screen" }
        }),
    )
    .await;
    let session_id = started["session_id"].as_u64().expect("session id");
    let outside = root.join("outside.png");
    std::fs::write(&outside, b"keep-me").expect("outside file");
    let link = root
        .join("tools/computer_use")
        .join(session_id.to_string())
        .join("snapshots/escape.png");
    symlink(&outside, &link).expect("snapshot symlink");

    let error = invoke_result(
        &handler,
        serde_json::json!({
            "action": "snapshot",
            "session_id": session_id,
            "screenshot_path": "escape.png"
        }),
    )
    .await
    .expect_err("symlink destination must be rejected");

    assert!(error.to_string().contains("unsafe computer_use snapshot"));
    assert_eq!(std::fs::read(&outside).expect("outside file"), b"keep-me");
}
