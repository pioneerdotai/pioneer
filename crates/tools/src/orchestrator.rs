use crate::classifier::{DefaultErrorClassifier, ErrorClassifier};
use crate::context::ToolOutcome;
use crate::context::{AnyToolResult, ToolInvocation};
use crate::error::ToolError;
use crate::events::ToolEventTrace;
use crate::registry::ToolRegistry;
use crate::spec::ToolIdempotencyMode;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalState {
    Approved,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxTarget {
    Default,
    Escalated,
}

#[derive(Debug, Clone)]
pub struct OrchestratorPolicy {
    pub allow_all: bool,
    pub retry_with_escalated_sandbox: bool,
}

impl Default for OrchestratorPolicy {
    fn default() -> Self {
        Self {
            allow_all: true,
            retry_with_escalated_sandbox: true,
        }
    }
}

#[derive(Clone)]
pub struct PostExecutionPolicy {
    classifier: Arc<dyn ErrorClassifier>,
}

impl Default for PostExecutionPolicy {
    fn default() -> Self {
        Self {
            classifier: Arc::new(DefaultErrorClassifier),
        }
    }
}

impl PostExecutionPolicy {
    pub fn new(classifier: Arc<dyn ErrorClassifier>) -> Self {
        Self { classifier }
    }

    fn apply(&self, invocation: &ToolInvocation, result: &mut AnyToolResult) {
        let raw_output_json = result.raw_output_json();
        let outcome =
            self.classifier
                .classify_result(invocation, &raw_output_json, result.success());
        result.set_outcome(outcome);
    }

    pub fn classify_error(&self, invocation: &ToolInvocation, error: &ToolError) -> ToolOutcome {
        self.classifier.classify_error(invocation, error)
    }
}

#[derive(Clone)]
pub struct ToolOrchestrator {
    policy: OrchestratorPolicy,
    post_policy: PostExecutionPolicy,
}

impl Default for ToolOrchestrator {
    fn default() -> Self {
        Self {
            policy: OrchestratorPolicy::default(),
            post_policy: PostExecutionPolicy::default(),
        }
    }
}

impl ToolOrchestrator {
    pub fn new(policy: OrchestratorPolicy) -> Self {
        Self {
            policy,
            post_policy: PostExecutionPolicy::default(),
        }
    }

    pub fn with_post_policy(policy: OrchestratorPolicy, post_policy: PostExecutionPolicy) -> Self {
        Self {
            policy,
            post_policy,
        }
    }

    pub async fn run(
        &self,
        registry: &ToolRegistry,
        mut invocation: ToolInvocation,
        trace: &ToolEventTrace,
    ) -> Result<AnyToolResult, ToolError> {
        self.enforce_idempotency_contract(&invocation)?;

        invocation.attempt_id = 1;

        trace.emit_stage(1, "orchestrator.approval.started", None, None);

        let approval = self.request_approval(&invocation).await;

        if matches!(approval, ApprovalState::Rejected) {
            trace.emit_stage(
                1,
                "orchestrator.approval.failed",
                Some("request denied by policy".to_owned()),
                None,
            );
            return Err(ToolError::Rejected("request denied by policy".to_owned()));
        }

        trace.emit_stage(1, "orchestrator.approval.completed", None, None);

        let first_attempt = self
            .run_in_sandbox(registry, invocation.clone(), SandboxTarget::Default, trace)
            .await;

        match first_attempt {
            Ok(mut result) => {
                trace.emit_stage(
                    1,
                    "retry.skipped",
                    None,
                    Some(serde_json::json!({
                        "reason": "first_attempt_succeeded",
                    })),
                );
                self.post_policy.apply(&invocation, &mut result);
                Ok(result)
            }
            Err(error)
                if self.policy.retry_with_escalated_sandbox
                    && self.should_retry_with_escalated_sandbox(&error) =>
            {
                trace.emit_stage(
                    1,
                    "retry.scheduled",
                    Some(error.to_string()),
                    Some(serde_json::json!({
                        "reason": "permission_or_sandbox_error",
                    })),
                );
                trace.emit_stage(2, "retry.started", None, None);
                let mut retry_invocation = invocation.clone();
                retry_invocation.attempt_id = 2;
                let mut result = self
                    .run_in_sandbox(
                        registry,
                        retry_invocation.clone(),
                        SandboxTarget::Escalated,
                        trace,
                    )
                    .await?;
                self.post_policy.apply(&retry_invocation, &mut result);
                Ok(result)
            }
            Err(error) => {
                trace.emit_stage(
                    1,
                    "retry.skipped",
                    Some(error.to_string()),
                    Some(serde_json::json!({
                        "retry_with_escalated_sandbox": self.policy.retry_with_escalated_sandbox,
                    })),
                );
                Err(error)
            }
        }
    }

    pub fn classify_error_outcome(
        &self,
        invocation: &ToolInvocation,
        error: &ToolError,
    ) -> ToolOutcome {
        self.post_policy.classify_error(invocation, error)
    }

    async fn request_approval(&self, _invocation: &ToolInvocation) -> ApprovalState {
        if self.policy.allow_all {
            ApprovalState::Approved
        } else {
            ApprovalState::Rejected
        }
    }

    async fn run_in_sandbox(
        &self,
        registry: &ToolRegistry,
        invocation: ToolInvocation,
        target: SandboxTarget,
        trace: &ToolEventTrace,
    ) -> Result<AnyToolResult, ToolError> {
        trace.emit_stage(
            invocation.attempt_id,
            "orchestrator.sandbox.started",
            None,
            Some(serde_json::json!({
                "target": match target {
                    SandboxTarget::Default => "default",
                    SandboxTarget::Escalated => "escalated",
                }
            })),
        );
        // Foundation for sandbox selection is present. Current behavior is allow-all.
        let dispatched = registry.dispatch(invocation.clone(), trace).await;
        match dispatched {
            Ok(result) => {
                trace.emit_stage(
                    invocation.attempt_id,
                    "orchestrator.sandbox.completed",
                    None,
                    None,
                );
                Ok(result)
            }
            Err(error) => {
                trace.emit_stage(
                    invocation.attempt_id,
                    "orchestrator.sandbox.failed",
                    Some(error.to_string()),
                    None,
                );
                Err(error)
            }
        }
    }

    fn should_retry_with_escalated_sandbox(&self, error: &ToolError) -> bool {
        match error {
            ToolError::ExecutionFailed(message) | ToolError::Internal(message) => {
                let lower = message.to_lowercase();
                lower.contains("permission denied") || lower.contains("operation not permitted")
            }
            ToolError::InvalidArguments(_)
            | ToolError::NotFound(_)
            | ToolError::Rejected(_)
            | ToolError::Cancelled(_) => false,
        }
    }

    fn enforce_idempotency_contract(&self, invocation: &ToolInvocation) -> Result<(), ToolError> {
        match invocation.recovery.idempotency_mode {
            ToolIdempotencyMode::None => Ok(()),
            ToolIdempotencyMode::Safe
            | ToolIdempotencyMode::RequiresKey
            | ToolIdempotencyMode::SessionBound => {
                let has_key = invocation
                    .idempotency_key
                    .as_ref()
                    .map(|key| !key.trim().is_empty())
                    .unwrap_or(false);
                if has_key {
                    Ok(())
                } else {
                    Err(ToolError::invalid_arguments(format!(
                        "tool `{}` requires idempotency_key for recovery-safe execution",
                        invocation.tool_name
                    )))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{FunctionToolOutput, ToolCallSource, ToolInvocation, ToolPayload};
    use crate::registry::{ToolHandler, ToolRegistry};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingHandler {
        calls: Arc<AtomicUsize>,
        first_error: Option<ToolError>,
        success_text: &'static str,
    }

    #[async_trait]
    impl ToolHandler for CountingHandler {
        async fn handle(
            &self,
            _invocation: ToolInvocation,
            _trace: crate::events::ToolEventTrace,
        ) -> Result<Box<dyn crate::context::ToolOutput>, ToolError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0
                && let Some(error) = self.first_error.clone()
            {
                return Err(error);
            }
            Ok(Box::new(FunctionToolOutput::new(self.success_text, true)))
        }
    }

    fn invocation() -> ToolInvocation {
        ToolInvocation {
            call_id: "call_1".to_owned(),
            tool_name: "tool".to_owned(),
            source: ToolCallSource::Model,
            payload: ToolPayload::Function {
                arguments: serde_json::json!({}),
            },
            workdir: PathBuf::from("."),
            attempt_id: 1,
            idempotency_key: None,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn registry_with_handler(handler: Arc<dyn ToolHandler>) -> ToolRegistry {
        let mut handlers: HashMap<String, Arc<dyn ToolHandler>> = HashMap::new();
        handlers.insert("tool".to_owned(), handler);
        ToolRegistry::new(handlers)
    }

    #[tokio::test]
    async fn retries_once_with_escalated_sandbox_on_permission_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(CountingHandler {
            calls: calls.clone(),
            first_error: Some(ToolError::execution_failed("Permission denied")),
            success_text: "ok",
        });
        let registry = registry_with_handler(handler);
        let orchestrator = ToolOrchestrator::new(OrchestratorPolicy {
            allow_all: true,
            retry_with_escalated_sandbox: true,
        });

        let trace = crate::events::ToolEventBus::default().start_trace("turn", "call_1", "tool");
        let result = orchestrator.run(&registry, invocation(), &trace).await;
        assert!(result.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn does_not_retry_on_invalid_arguments() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(CountingHandler {
            calls: calls.clone(),
            first_error: Some(ToolError::invalid_arguments("bad request")),
            success_text: "ok",
        });
        let registry = registry_with_handler(handler);
        let orchestrator = ToolOrchestrator::new(OrchestratorPolicy {
            allow_all: true,
            retry_with_escalated_sandbox: true,
        });

        let trace = crate::events::ToolEventBus::default().start_trace("turn", "call_1", "tool");
        let result = orchestrator.run(&registry, invocation(), &trace).await;
        match result {
            Ok(_) => panic!("invalid args should fail"),
            Err(error) => assert!(matches!(error, ToolError::InvalidArguments(_))),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rejects_when_policy_disallows_all() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(CountingHandler {
            calls: calls.clone(),
            first_error: None,
            success_text: "ok",
        });
        let registry = registry_with_handler(handler);
        let orchestrator = ToolOrchestrator::new(OrchestratorPolicy {
            allow_all: false,
            retry_with_escalated_sandbox: true,
        });

        let trace = crate::events::ToolEventBus::default().start_trace("turn", "call_1", "tool");
        let result = orchestrator.run(&registry, invocation(), &trace).await;
        match result {
            Ok(_) => panic!("request should be rejected"),
            Err(error) => assert!(matches!(error, ToolError::Rejected(_))),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn post_execution_policy_sets_outcome_on_success() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(CountingHandler {
            calls: calls.clone(),
            first_error: None,
            success_text: "ok",
        });
        let registry = registry_with_handler(handler);
        let orchestrator = ToolOrchestrator::default();

        let trace = crate::events::ToolEventBus::default().start_trace("turn", "call_1", "tool");
        let result = orchestrator
            .run(&registry, invocation(), &trace)
            .await
            .expect("run should succeed");

        assert_eq!(result.outcome.status, crate::context::ToolOutcomeStatus::Ok);
    }

    #[test]
    fn classify_error_outcome_uses_classifier() {
        let orchestrator = ToolOrchestrator::default();
        let mut invocation = invocation();
        invocation.tool_name = "exec_command".to_owned();
        let outcome = orchestrator.classify_error_outcome(
            &invocation,
            &ToolError::execution_failed("command not found"),
        );
        assert_eq!(
            outcome.error_class,
            Some(crate::context::ToolErrorClass::CommandNotFound)
        );
        assert!(outcome.should_retry);
    }
}
