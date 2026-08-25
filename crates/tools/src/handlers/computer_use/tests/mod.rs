use super::backend::MockComputerUseBackend;
use super::handler::ComputerUseHandler;
use crate::ComputerUseToolsConfig;
use crate::context::{ToolCallSource, ToolInvocation, ToolPayload};
use crate::events::ToolEventBus;
use crate::registry::ToolHandler;
use crate::spec::ToolRecoveryMetadata;
use serde_json::Value as JsonValue;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_TEST_ROOT_ID: AtomicUsize = AtomicUsize::new(1);

fn test_handler() -> (ComputerUseHandler, PathBuf) {
    test_handler_with_backend(MockComputerUseBackend::default())
}

fn test_handler_with_backend(backend: MockComputerUseBackend) -> (ComputerUseHandler, PathBuf) {
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
        ComputerUseHandler::with_backend(config, Arc::new(backend)),
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
        environment: Default::default(),
        attempt_id: 1,
        idempotency_key: None,
        recovery: ToolRecoveryMetadata::default(),
        permission_metadata: crate::spec::ToolPermissionMetadata::default(),
        execution_security_snapshot: None,
        apply_patch_preflight: None,
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

async fn invoke_result(
    handler: &ComputerUseHandler,
    payload: JsonValue,
) -> Result<JsonValue, crate::error::ToolError> {
    let trace = ToolEventBus::default().start_trace("turn", "call", "computer_use");
    handler
        .handle(invocation(payload), trace)
        .await
        .map(|output| output.raw_json())
}

async fn latest_snapshot_id(handler: &ComputerUseHandler, session_id: u64) -> String {
    let status = invoke(
        handler,
        serde_json::json!({
            "action": "status",
            "session_id": session_id
        }),
    )
    .await;
    status
        .pointer("/last_snapshot/snapshot_id")
        .and_then(JsonValue::as_str)
        .expect("latest snapshot id")
        .to_owned()
}

mod actions;
mod active_app;
mod app_identity;
mod artifacts;
mod fallback_suggestions;
mod flow;
mod input;
mod integration_smoke;
mod regressions;
mod session_target_policy;
mod snapshot_bound_targets;
mod structured_action_trace;
mod target_resolution;
async fn start_app_session_with_snapshot(handler: &ComputerUseHandler) -> u64 {
    let started = invoke(
        handler,
        serde_json::json!({
            "action": "start",
            "goal": "semantic action test",
            "target": { "type": "app_name", "name": "MockApp" }
        }),
    )
    .await;
    let session_id = started
        .get("session_id")
        .and_then(JsonValue::as_u64)
        .expect("session id");
    let snap = invoke(
        handler,
        serde_json::json!({
            "action": "snapshot",
            "session_id": session_id,
        }),
    )
    .await;
    assert_eq!(
        snap.get("status").and_then(JsonValue::as_str),
        Some("running")
    );
    session_id
}

mod preflight;
mod real_desktop;
mod recovery;
mod tree;
mod verify;
