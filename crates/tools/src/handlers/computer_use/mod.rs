//! Gateway-owned computer use. This module never launches or connects to a driver.
mod browser_catalog;
mod catalog;
mod images;
pub use images::{ComputerUseImageAdmission, ComputerUseImageSink};

use crate::context::{
    FunctionToolOutput, ToolDeliveryReceipt, ToolInvocation, ToolOutput, ToolPayload,
};
use crate::error::ToolError;
use crate::events::ToolEventTrace;
use crate::registry::ToolHandler;
use crate::{ComputerUseToolsConfig, ToolExtensionBundle};
use async_trait::async_trait;
pub use catalog::TOOL_NAMES;
pub use catalog::upstream_name;
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub fn is_computer_use_tool(name: &str) -> bool {
    upstream_name(name).is_some()
}

pub fn computer_use_tool_specs() -> Vec<crate::ConfiguredToolSpec> {
    catalog::configured_specs()
}

/// Trusted Gateway ingress. `name` is the unprefixed upstream allowlisted operation.
/// Implementations must retire unsafe generations on cancellation or unknown outcomes.
#[async_trait]
pub trait ComputerUseBackend: Send + Sync + std::fmt::Debug {
    async fn execute(
        &self,
        owner: &str,
        name: &str,
        arguments: Value,
        cancellation: CancellationToken,
    ) -> Result<Value, ToolError>;
    /// Called only by the trusted typed result receipt after delivery commits.
    /// False means intervening input/cleanup invalidated this observation.
    fn observation_delivered(&self, owner: &str, context: &Value) -> bool;
    fn release_owner(&self, owner: &str);
}

struct OwnerLease {
    owner: String,
    backend: Option<Arc<dyn ComputerUseBackend>>,
    gate: Arc<tokio::sync::Mutex<ObservationGate>>,
}
impl Drop for OwnerLease {
    fn drop(&mut self) {
        if let Some(backend) = &self.backend {
            backend.release_owner(&self.owner);
        }
    }
}
struct ComputerUseHandler {
    lease: Arc<OwnerLease>,
    config: ComputerUseToolsConfig,
}

#[derive(Default)]
struct ObservationGate {
    observed: bool,
}

// Local authority held through artifact admission and the outer result's durable
// publication (CLI: through final frame encoding). This is never serialized.
struct DeliveryReceipt {
    backend: Arc<dyn ComputerUseBackend>,
    owner: String,
    cancellation: CancellationToken,
    guard: std::sync::Mutex<Option<tokio::sync::OwnedMutexGuard<ObservationGate>>>,
    observation: std::sync::Mutex<Option<Value>>,
}
impl ToolDeliveryReceipt for DeliveryReceipt {
    fn commit(&self) {
        let Some(mut guard) = self.guard.lock().unwrap().take() else {
            return;
        };
        if self.cancellation.is_cancelled() {
            guard.observed = false;
            self.backend.release_owner(&self.owner);
        } else if let Some(context) = self.observation.lock().unwrap().take() {
            guard.observed = self.backend.observation_delivered(&self.owner, &context);
        }
    }
}
impl Drop for DeliveryReceipt {
    fn drop(&mut self) {
        if let Some(mut guard) = self.guard.lock().unwrap().take() {
            guard.observed = false;
            self.backend.release_owner(&self.owner);
        }
    }
}

pub fn is_passive_operation(name: &str) -> bool {
    matches!(
        name,
        "get_window_state"
            | "get_desktop_state"
            | "get_browser_state"
            | "list_apps"
            | "list_windows"
            | "get_screen_size"
            | "get_cursor_position"
            | "clipboard_read"
            | "verify_state"
    )
}
fn is_capture(name: &str) -> bool {
    matches!(
        name,
        "get_window_state" | "get_desktop_state" | "get_browser_state"
    )
}

pub fn materialize_computer_use_domain_bundle(
    config: ComputerUseToolsConfig,
    owner: String,
) -> ToolExtensionBundle {
    let lease = Arc::new(OwnerLease {
        owner,
        backend: config.backend.clone(),
        gate: Arc::default(),
    });
    let handler: Arc<dyn ToolHandler> = Arc::new(ComputerUseHandler { lease, config });
    let specs = catalog::configured_specs();
    let handlers = specs
        .iter()
        .map(|spec| (spec.spec.name.clone(), handler.clone()))
        .collect();
    ToolExtensionBundle { specs, handlers }
}

#[async_trait]
impl ToolHandler for ComputerUseHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let name = upstream_name(&invocation.tool_name)
            .ok_or_else(|| ToolError::invalid_arguments("unknown CUA operation"))?;
        let ToolPayload::Function { arguments } = invocation.payload else {
            return Err(ToolError::invalid_arguments(
                "CUA requires function arguments",
            ));
        };
        if !arguments.is_object() {
            return Err(ToolError::invalid_arguments(
                "CUA arguments must be an object",
            ));
        }
        let backend = self.lease.backend.as_ref().ok_or_else(|| ToolError::Rejected("Computer use is unavailable on this Gateway: its packaged local driver is not configured.".to_owned()))?;
        let mut guard = tokio::select! {
            biased;
            _ = invocation.cancellation.cancelled() => return Err(ToolError::Cancelled("CUA delivery cancelled before owner admission".into())),
            guard = self.lease.gate.clone().lock_owned() => guard,
        };
        if !is_passive_operation(name) && !guard.observed {
            return Err(ToolError::Rejected(
                "A successfully delivered fresh CUA observation is required before input.".into(),
            ));
        }
        if is_capture(name) || !is_passive_operation(name) {
            guard.observed = false;
        }
        let delivery = Arc::new(DeliveryReceipt {
            backend: backend.clone(),
            owner: self.lease.owner.clone(),
            cancellation: invocation.cancellation.clone(),
            guard: std::sync::Mutex::new(Some(guard)),
            observation: std::sync::Mutex::new(None),
        });
        let result = backend
            .execute(
                &self.lease.owner,
                name,
                arguments,
                invocation.cancellation.clone(),
            )
            .await?;
        // This result came only from the injected Gateway backend, never an external MCP/proxy payload.
        let success = result.get("isError").and_then(Value::as_bool) != Some(true)
            && result
                .pointer("/structuredContent/status")
                .and_then(Value::as_str)
                != Some("refused");
        if is_capture(name) && success {
            // Only the actual injected backend's result supplies this context.
            // External JSON/name matching never reaches this receipt constructor.
            *delivery.observation.lock().unwrap() = Some(
                result
                    .get("pioneer_cua_context")
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }
        // The private acknowledgement never enters model/history/telemetry.
        // Public generation/session capture metadata remains unchanged.
        let mut result = result;
        if let Some(context) = result
            .get_mut("pioneer_cua_context")
            .and_then(Value::as_object_mut)
        {
            context.remove("delivery_nonce");
        }
        let text = result
            .get("content")
            .and_then(Value::as_array)
            .map(|parts| {
                parts
                    .iter()
                    .filter_map(|part| part.get("text").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default();
        images::trusted_output(
            result,
            success,
            text,
            &self.config,
            &self.lease.owner,
            &invocation.call_id,
            invocation.cancellation,
            Some(delivery),
        )
        .await
    }
}
