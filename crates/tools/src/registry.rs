use crate::context::{AnyToolResult, ToolInvocation};
use crate::error::ToolError;
use crate::events::ToolEventTrace;
use crate::spec::ConfiguredToolSpec;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

#[async_trait]
pub trait ToolHandler: Send + Sync {
    /// The concrete handler performs only reads. Class names and remote hints
    /// are not sufficient proof for native batch concurrency.
    fn supports_parallel_reads(&self) -> bool {
        false
    }

    /// A transparent composition performs bounded local preparation/projection
    /// and delegates all work to exactly one owned nested dispatch. Its child
    /// owns locks and the finite attempt budget after fresh authorization.
    /// Never enable this for a handler that also performs external work.
    fn delegates_runtime_execution(&self) -> bool {
        false
    }

    /// One execution attempt, after authorization. Concrete handlers may use
    /// their frozen workload policy instead of the native default.
    fn attempt_work_budget(
        &self,
        invocation: &ToolInvocation,
    ) -> Result<std::time::Duration, ToolError> {
        crate::orchestrator::tool_attempt_work_budget(invocation)
    }

    async fn handle(
        &self,
        invocation: ToolInvocation,
        trace: ToolEventTrace,
    ) -> Result<Box<dyn crate::context::ToolOutput>, ToolError>;
}

pub struct ToolRegistry {
    handlers: HashMap<String, Arc<dyn ToolHandler>>,
}

impl ToolRegistry {
    pub fn new(handlers: HashMap<String, Arc<dyn ToolHandler>>) -> Self {
        Self { handlers }
    }

    pub fn has_handler(&self, tool_name: &str) -> bool {
        self.handlers.contains_key(tool_name)
    }

    pub(crate) fn supports_parallel_reads(&self, tool_name: &str) -> bool {
        self.handlers.get(tool_name).is_some_and(|handler| {
            handler.supports_parallel_reads() && !handler.delegates_runtime_execution()
        })
    }

    pub(crate) fn delegates_runtime_execution(&self, tool_name: &str) -> bool {
        self.handlers
            .get(tool_name)
            .is_some_and(|handler| handler.delegates_runtime_execution())
    }

    pub(crate) fn attempt_work_budget(
        &self,
        invocation: &ToolInvocation,
    ) -> Result<std::time::Duration, ToolError> {
        self.handlers
            .get(invocation.tool_name.as_str())
            .ok_or_else(|| ToolError::NotFound(invocation.tool_name.clone()))?
            .attempt_work_budget(invocation)
    }

    pub async fn dispatch(
        &self,
        invocation: ToolInvocation,
        trace: &ToolEventTrace,
    ) -> Result<AnyToolResult, ToolError> {
        let handler = self
            .handlers
            .get(invocation.tool_name.as_str())
            .ok_or_else(|| ToolError::NotFound(invocation.tool_name.clone()))?
            .clone();

        trace.emit_stage(
            invocation.attempt_id,
            "handler.execute.started",
            None,
            Some(serde_json::json!({
                "tool": invocation.tool_name,
            })),
        );

        let call_id = invocation.call_id.clone();
        let tool_name = invocation.tool_name.clone();
        let payload = invocation.payload.clone();
        let output = match handler.handle(invocation.clone(), trace.clone()).await {
            Ok(output) => {
                trace.emit_stage(
                    invocation.attempt_id,
                    "handler.execute.completed",
                    None,
                    None,
                );
                output
            }
            Err(error) => {
                trace.emit_stage(
                    invocation.attempt_id,
                    "handler.execute.failed",
                    Some(error.to_string()),
                    None,
                );
                return Err(error);
            }
        };

        Ok(AnyToolResult {
            call_id,
            tool_name,
            payload,
            output,
            outcome: crate::context::ToolOutcome::ok(),
            projection: None,
        })
    }
}

pub struct ToolRegistryBuilder {
    specs: Vec<ConfiguredToolSpec>,
    handlers: HashMap<String, Arc<dyn ToolHandler>>,
}

impl ToolRegistryBuilder {
    pub fn new() -> Self {
        Self {
            specs: Vec::new(),
            handlers: HashMap::new(),
        }
    }

    pub fn push_configured_spec(&mut self, spec: ConfiguredToolSpec) {
        self.specs.push(spec);
    }

    pub fn register_handler<H>(&mut self, tool_name: impl Into<String>, handler: Arc<H>)
    where
        H: ToolHandler + 'static,
    {
        self.handlers.insert(tool_name.into(), handler);
    }

    pub fn register_dyn_handler(
        &mut self,
        tool_name: impl Into<String>,
        handler: Arc<dyn ToolHandler>,
    ) {
        self.handlers.insert(tool_name.into(), handler);
    }

    pub fn build(self) -> (Vec<ConfiguredToolSpec>, ToolRegistry) {
        (self.specs, ToolRegistry::new(self.handlers))
    }
}

impl Default for ToolRegistryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{FunctionToolOutput, ToolCallSource, ToolPayload};
    use crate::spec::{ExecutionClass, PayloadKind, ToolSpec};
    use std::path::PathBuf;

    struct FixedHandler {
        text: &'static str,
        error: Option<ToolError>,
    }

    #[async_trait]
    impl ToolHandler for FixedHandler {
        async fn handle(
            &self,
            _invocation: ToolInvocation,
            _trace: ToolEventTrace,
        ) -> Result<Box<dyn crate::context::ToolOutput>, ToolError> {
            if let Some(error) = self.error.clone() {
                return Err(error);
            }
            Ok(Box::new(FunctionToolOutput::new(self.text, true)))
        }
    }

    fn invocation(tool_name: &str) -> ToolInvocation {
        ToolInvocation {
            call_id: "call_1".to_owned(),
            tool_name: tool_name.to_owned(),
            source: ToolCallSource::Model,
            payload: ToolPayload::Function {
                arguments: serde_json::json!({}),
            },
            workdir: PathBuf::from("."),
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            execution_security_snapshot: None,
            apply_patch_preflight: None,
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn builder_keeps_specs_and_handlers_separate() {
        let spec = ToolSpec::new(
            "read_file",
            "Read file",
            serde_json::json!({ "type": "object" }),
            PayloadKind::Function,
        );

        let mut builder = ToolRegistryBuilder::new();
        builder.push_configured_spec(ConfiguredToolSpec::with_output_projection(
            spec.clone(),
            ExecutionClass::Shared,
            crate::output_policy::dynamic_unknown_output_policy(),
            crate::output_policy::ToolOutputProjectionKind::DynamicGeneric,
        ));
        let (specs, registry) = builder.build();

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].spec.name, "read_file");
        assert!(!registry.has_handler("read_file"));
    }

    #[tokio::test]
    async fn dispatch_returns_not_found_when_handler_missing() {
        let registry = ToolRegistry::new(HashMap::new());
        let trace =
            crate::events::ToolEventBus::default().start_trace("turn", "call_1", "missing_tool");
        let result = registry.dispatch(invocation("missing_tool"), &trace).await;
        match result {
            Ok(_) => panic!("expected missing handler error"),
            Err(error) => assert!(matches!(error, ToolError::NotFound(_))),
        }
    }

    #[tokio::test]
    async fn dispatch_returns_handler_output() {
        let mut handlers: HashMap<String, Arc<dyn ToolHandler>> = HashMap::new();
        handlers.insert(
            "echo".to_owned(),
            Arc::new(FixedHandler {
                text: "ok",
                error: None,
            }),
        );
        let registry = ToolRegistry::new(handlers);

        let result = registry
            .dispatch(
                invocation("echo"),
                &crate::events::ToolEventBus::default().start_trace("turn", "call_1", "echo"),
            )
            .await
            .expect("handler should succeed");
        assert_eq!(result.call_id, "call_1");
        assert_eq!(result.tool_name, "echo");
        assert_eq!(result.raw_output_text(), "ok");
    }
}
