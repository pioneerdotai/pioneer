use super::backend::ComputerUseBackend;
use super::handler::ComputerUseHandler;
use super::model::{CapturedFrame, DisplayMeta, ResolvedAction, SnapshotBudget};
use super::util::resolve_absolute_coordinates;
use crate::ComputerUseToolsConfig;
use crate::context::{ToolCallSource, ToolInvocation, ToolPayload};
use crate::error::ToolError;
use crate::events::ToolEventBus;
use crate::registry::ToolHandler;
use crate::spec::ToolRecoveryMetadata;
use serde_json::Value as JsonValue;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use xcap::image::{DynamicImage, ImageFormat};

static NEXT_TEST_ROOT_ID: AtomicUsize = AtomicUsize::new(1);

#[derive(Default)]
struct MockBackend {
    action_count: AtomicUsize,
}

impl MockBackend {
    fn display() -> DisplayMeta {
        DisplayMeta {
            display_id: 1,
            width_px: 1920,
            height_px: 1080,
            scale_factor: 2.0,
            origin_x: 0,
            origin_y: 0,
            is_primary: true,
        }
    }

    fn png_bytes(seed: u8) -> Vec<u8> {
        let image = xcap::image::RgbaImage::from_pixel(
            2,
            2,
            xcap::image::Rgba([seed, seed.saturating_add(1), seed.saturating_add(2), 255]),
        );
        let mut cursor = Cursor::new(Vec::<u8>::new());
        DynamicImage::ImageRgba8(image)
            .write_to(&mut cursor, ImageFormat::Png)
            .expect("encode png");
        cursor.into_inner()
    }
}

impl ComputerUseBackend for MockBackend {
    fn list_displays(&self) -> Result<Vec<DisplayMeta>, ToolError> {
        Ok(vec![Self::display()])
    }

    fn capture_display(
        &self,
        _display_id: u32,
        _snapshot_budget: &SnapshotBudget,
    ) -> Result<CapturedFrame, ToolError> {
        let seed = self.action_count.load(Ordering::SeqCst) as u8;
        Ok(CapturedFrame {
            width_px: 1920,
            height_px: 1080,
            scale_factor: 2.0,
            png_bytes: Self::png_bytes(seed),
            resize_passes: 0,
        })
    }

    fn perform_action(&self, _action: &ResolvedAction) -> Result<String, ToolError> {
        let count = self.action_count.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(format!("mock action {}", count))
    }
}

fn test_handler() -> (ComputerUseHandler, PathBuf) {
    let unique = NEXT_TEST_ROOT_ID.fetch_add(1, Ordering::SeqCst);
    let root = std::env::temp_dir().join(format!(
        "pioneer-computer-use-remote-only-tests-{}-{}",
        chrono::Utc::now().timestamp_millis(),
        unique
    ));
    let config = ComputerUseToolsConfig {
        runtime_home_dir: root.clone(),
        artifacts_subdir: "tools/computer_use".to_owned(),
        ..ComputerUseToolsConfig::default()
    };
    (
        ComputerUseHandler::with_backend(config, Arc::new(MockBackend::default())),
        root,
    )
}

fn invocation(payload: JsonValue) -> ToolInvocation {
    ToolInvocation {
        call_id: "call_1".to_owned(),
        tool_name: "computer_use".to_owned(),
        source: ToolCallSource::Model,
        payload: ToolPayload::Function { arguments: payload },
        workdir: PathBuf::from("."),
        attempt_id: 1,
        idempotency_key: None,
        recovery: ToolRecoveryMetadata::default(),
        cancellation: tokio_util::sync::CancellationToken::new(),
    }
}

async fn invoke(handler: &ComputerUseHandler, payload: JsonValue) -> JsonValue {
    let trace = ToolEventBus::default().start_trace("turn", "call", "computer_use");
    let output = handler
        .handle(invocation(payload), trace)
        .await
        .expect("tool call must succeed");
    output.raw_json()
}

#[tokio::test]
async fn remote_start_snapshot_act_status_stop_flow() {
    let (handler, root) = test_handler();

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "Open app and check state",
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

    let acted = invoke(
        &handler,
        serde_json::json!({
            "action": "act",
            "session_id": session_id,
            "act": {
                "type": "click",
                "x_norm": 0.5,
                "y_norm": 0.5,
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

    let handler_a =
        ComputerUseHandler::with_backend(config.clone(), Arc::new(MockBackend::default()));
    let started_a = invoke(
        &handler_a,
        serde_json::json!({"action": "start", "goal": "a"}),
    )
    .await;
    let session_a = started_a
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session a");

    let handler_b = ComputerUseHandler::with_backend(config, Arc::new(MockBackend::default()));
    let started_b = invoke(
        &handler_b,
        serde_json::json!({"action": "start", "goal": "b"}),
    )
    .await;
    let session_b = started_b
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session b");

    assert!(session_b > session_a);
}

#[test]
fn coordinate_conversion_uses_normalized_values() {
    let display = DisplayMeta {
        display_id: 1,
        width_px: 100,
        height_px: 50,
        scale_factor: 2.0,
        origin_x: 10,
        origin_y: 20,
        is_primary: true,
    };

    let (x, y) = resolve_absolute_coordinates(&display, Some(0.5), Some(0.5))
        .expect("coordinates should resolve");
    assert_eq!(x, 60);
    assert_eq!(y, 45);
}

#[tokio::test]
async fn repeated_snapshot_hash_guard_stops_session() {
    let (handler, _) = test_handler();

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "loop guard test",
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

    let mut last = serde_json::json!({});
    for _ in 0..9 {
        last = invoke(
            &handler,
            serde_json::json!({
                "action": "act",
                "session_id": session_id,
                "act": {
                    "type": "click",
                    "x_norm": 0.5,
                    "y_norm": 0.5
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
            "expected_effect_mismatch": true,
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
            "failure_class": "policy_blocked",
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
        Some("policy_blocked")
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
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");

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

        let (x_norm, y_norm) = if step % 2 == 0 {
            (0.25, 0.25)
        } else {
            (0.75, 0.75)
        };
        let act = invoke(
            &handler,
            serde_json::json!({
                "action": "act",
                "session_id": session_id,
                "act": {
                    "type": "click",
                    "x_norm": x_norm,
                    "y_norm": y_norm
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
