use crate::{
    HookContext, HookContribution, HookDiagnostic, HookDiagnosticCode, HookDiagnosticMessage,
    HookDiagnosticSeverity, HookError, HookFailurePolicy, HookHandlerRequest, HookHandlerResponse,
    HookId, HookInput, HookMetadata, HookPhase, HookRegistry, HookRegistryError,
    HookSubscriptionId, HookSubscriptionRegistry,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

pub type HookRuntimeResult<T> = Result<T, HookRuntimeError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookRuntimeError {
    Registry(HookRegistryError),
    MissingHandler {
        subscription_id: HookSubscriptionId,
        hook_id: HookId,
        phase: HookPhase,
    },
    HookFailed {
        subscription_id: HookSubscriptionId,
        hook_id: HookId,
        phase: HookPhase,
        error: HookError,
    },
}

impl fmt::Display for HookRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registry(error) => write!(formatter, "hook registry error: {}", error),
            Self::MissingHandler {
                subscription_id,
                hook_id,
                phase,
            } => write!(
                formatter,
                "hook subscription `{}` references missing handler `{}` for phase `{}`",
                subscription_id, hook_id, phase
            ),
            Self::HookFailed {
                subscription_id,
                hook_id,
                phase,
                error,
            } => write!(
                formatter,
                "hook subscription `{}` handler `{}` failed for phase `{}`: {}",
                subscription_id, hook_id, phase, error
            ),
        }
    }
}

impl std::error::Error for HookRuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Registry(error) => Some(error),
            Self::HookFailed { error, .. } => Some(error),
            Self::MissingHandler { .. } => None,
        }
    }
}

impl From<HookRegistryError> for HookRuntimeError {
    fn from(error: HookRegistryError) -> Self {
        Self::Registry(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookRunStatus {
    Succeeded,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookRunErrorSummary {
    pub code: HookDiagnosticCode,
    pub message: HookDiagnosticMessage,
    pub retryable: bool,
    pub safe_for_user: bool,
}

impl From<&HookError> for HookRunErrorSummary {
    fn from(error: &HookError) -> Self {
        Self {
            code: error.code.clone(),
            message: error.message.clone(),
            retryable: error.retryable,
            safe_for_user: error.safe_for_user,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookRunSummary {
    pub subscription_id: HookSubscriptionId,
    pub hook_id: HookId,
    pub phase: HookPhase,
    pub status: HookRunStatus,
    pub attempt_count: u16,
    pub contribution_count: usize,
    pub diagnostic_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<HookRunErrorSummary>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookPhaseRequest {
    pub phase: HookPhase,
    pub context: HookContext,
    pub input: HookInput,
}

impl HookPhaseRequest {
    pub fn new(phase: HookPhase, context: HookContext, input: HookInput) -> Self {
        Self {
            phase,
            context,
            input,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HookPhaseResponse {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contributions: Vec<HookContribution>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runs: Vec<HookRunSummary>,
}

#[derive(Clone)]
pub struct HookRuntime {
    handlers: Arc<HookRegistry>,
    subscriptions: Arc<HookSubscriptionRegistry>,
}

impl HookRuntime {
    pub fn new(handlers: Arc<HookRegistry>, subscriptions: Arc<HookSubscriptionRegistry>) -> Self {
        Self {
            handlers,
            subscriptions,
        }
    }

    pub fn handlers(&self) -> &Arc<HookRegistry> {
        &self.handlers
    }

    pub fn subscriptions(&self) -> &Arc<HookSubscriptionRegistry> {
        &self.subscriptions
    }

    pub async fn run_phase(
        &self,
        request: HookPhaseRequest,
    ) -> HookRuntimeResult<HookPhaseResponse> {
        let subscriptions = self.subscriptions.subscriptions_for_phase(request.phase)?;
        let mut response = HookPhaseResponse::default();

        for subscription in subscriptions {
            let handler = self
                .handlers
                .get_handler(&subscription.hook_id)?
                .ok_or_else(|| HookRuntimeError::MissingHandler {
                    subscription_id: subscription.subscription_id.clone(),
                    hook_id: subscription.hook_id.clone(),
                    phase: request.phase,
                })?;
            let handler_request = HookHandlerRequest {
                hook_id: subscription.hook_id.clone(),
                phase: request.phase,
                context: request.context.clone(),
                input: request.input.clone(),
            };

            match handler.execute(handler_request).await {
                Ok(handler_response) => {
                    append_success(
                        &mut response,
                        subscription.subscription_id,
                        subscription.hook_id,
                        request.phase,
                        handler_response,
                    );
                }
                Err(error) if subscription.failure_policy == HookFailurePolicy::BestEffort => {
                    append_best_effort_failure(
                        &mut response,
                        subscription.subscription_id,
                        subscription.hook_id,
                        request.phase,
                        error,
                    );
                }
                Err(error) => {
                    return Err(HookRuntimeError::HookFailed {
                        subscription_id: subscription.subscription_id,
                        hook_id: subscription.hook_id,
                        phase: request.phase,
                        error,
                    });
                }
            }
        }

        Ok(response)
    }
}

fn append_success(
    phase_response: &mut HookPhaseResponse,
    subscription_id: HookSubscriptionId,
    hook_id: HookId,
    phase: HookPhase,
    handler_response: HookHandlerResponse,
) {
    let contribution_count = handler_response.contributions.len();
    let diagnostic_count = handler_response.diagnostics.len();
    phase_response
        .contributions
        .extend(handler_response.contributions);
    phase_response
        .diagnostics
        .extend(handler_response.diagnostics);
    phase_response.runs.push(HookRunSummary {
        subscription_id,
        hook_id,
        phase,
        status: HookRunStatus::Succeeded,
        attempt_count: 1,
        contribution_count,
        diagnostic_count,
        error: None,
    });
}

fn append_best_effort_failure(
    phase_response: &mut HookPhaseResponse,
    subscription_id: HookSubscriptionId,
    hook_id: HookId,
    phase: HookPhase,
    error: HookError,
) {
    let diagnostic = HookDiagnostic {
        code: error.code.clone(),
        message: error.message.clone(),
        severity: HookDiagnosticSeverity::Warning,
        safe_for_user: error.safe_for_user,
        metadata: HookMetadata::default(),
    };
    phase_response.diagnostics.push(diagnostic);
    phase_response.runs.push(HookRunSummary {
        subscription_id,
        hook_id,
        phase,
        status: HookRunStatus::Failed,
        attempt_count: 1,
        contribution_count: 0,
        diagnostic_count: 1,
        error: Some(HookRunErrorSummary::from(&error)),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        HookDiagnosticMessage, HookDomain, HookError, HookFilterKey, HookHandler, HookKind,
        HookPromptContent, HookPromptSectionTitle, HookResult, HookSectionId, HookSubscription,
        HookSubscriptionDependencies, HookValue,
    };
    use async_trait::async_trait;
    use std::collections::BTreeMap;
    use std::collections::VecDeque;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    struct RecordingHookHandler {
        id: HookId,
        phases: Vec<HookPhase>,
        calls: Arc<Mutex<Vec<HookId>>>,
        responses: Mutex<VecDeque<HookResult<HookHandlerResponse>>>,
    }

    #[async_trait]
    impl HookHandler for RecordingHookHandler {
        fn id(&self) -> HookId {
            self.id.clone()
        }

        fn kind(&self) -> HookKind {
            HookKind::new("test").expect("valid hook kind")
        }

        fn supported_phases(&self) -> Vec<HookPhase> {
            self.phases.clone()
        }

        async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
            assert_eq!(request.hook_id, self.id);
            assert_eq!(request.phase, HookPhase::TurnPrePromptCompile);
            assert_eq!(request.context, HookContext::default());
            assert_eq!(
                request.input.kind,
                crate::HookInputKind::TurnPrePromptCompile
            );
            self.calls.lock().expect("calls lock").push(self.id.clone());
            self.responses
                .lock()
                .expect("responses lock")
                .pop_front()
                .expect("test response exists")
        }
    }

    fn block_on_ready<F>(future: F) -> F::Output
    where
        F: Future,
    {
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        match Pin::as_mut(&mut future).poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("test future unexpectedly pending"),
        }
    }

    fn noop_waker() -> Waker {
        unsafe fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        unsafe fn wake(_: *const ()) {}
        unsafe fn wake_by_ref(_: *const ()) {}
        unsafe fn drop(_: *const ()) {}

        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
        let raw = RawWaker::new(std::ptr::null(), &VTABLE);
        unsafe { Waker::from_raw(raw) }
    }

    fn hook_id(value: &str) -> HookId {
        HookId::new(value).expect("valid hook id")
    }

    fn subscription_id(value: &str) -> HookSubscriptionId {
        HookSubscriptionId::new(value).expect("valid subscription id")
    }

    fn phase_request() -> HookPhaseRequest {
        HookPhaseRequest::new(
            HookPhase::TurnPrePromptCompile,
            HookContext::default(),
            HookInput {
                kind: crate::HookInputKind::TurnPrePromptCompile,
                payload: crate::HookValue::Null,
            },
        )
    }

    fn contribution(section_id: &str, content: &str) -> HookContribution {
        HookContribution::PromptSection(crate::PromptSectionContribution {
            section_id: HookSectionId::new(section_id).expect("valid section id"),
            title: Some(HookPromptSectionTitle::new("Test").expect("valid title")),
            domain: HookDomain::new("test").expect("valid domain"),
            priority: 0,
            content: HookPromptContent::new(content).expect("valid content"),
            max_chars: None,
            diagnostics: Vec::new(),
            truncated: false,
        })
    }

    fn diagnostic(code: &str, message: &str) -> HookDiagnostic {
        HookDiagnostic {
            code: HookDiagnosticCode::new(code).expect("valid code"),
            message: HookDiagnosticMessage::new(message).expect("valid message"),
            severity: HookDiagnosticSeverity::Info,
            safe_for_user: false,
            metadata: HookMetadata::default(),
        }
    }

    fn hook_error(code: &str, message: &str) -> HookError {
        HookError::new(
            HookDiagnosticCode::new(code).expect("valid code"),
            HookDiagnosticMessage::new(message).expect("valid message"),
        )
    }

    fn handler(
        id: &str,
        calls: Arc<Mutex<Vec<HookId>>>,
        responses: Vec<HookResult<HookHandlerResponse>>,
    ) -> Arc<dyn HookHandler> {
        Arc::new(RecordingHookHandler {
            id: hook_id(id),
            phases: vec![HookPhase::TurnPrePromptCompile],
            calls,
            responses: Mutex::new(VecDeque::from(responses)),
        })
    }

    fn register_handler(
        registry: &HookRegistry,
        id: &str,
        calls: Arc<Mutex<Vec<HookId>>>,
        responses: Vec<HookResult<HookHandlerResponse>>,
    ) {
        registry
            .register_handler(handler(id, calls, responses))
            .expect("handler registers");
    }

    fn register_subscription(
        handlers: &HookRegistry,
        subscriptions: &HookSubscriptionRegistry,
        subscription_id: &str,
        hook_id: &str,
        priority: i32,
        failure_policy: HookFailurePolicy,
    ) {
        subscriptions
            .register_subscription(
                handlers,
                HookSubscription::new(
                    self::subscription_id(subscription_id),
                    self::hook_id(hook_id),
                    HookPhase::TurnPrePromptCompile,
                )
                .with_priority(priority)
                .with_failure_policy(failure_policy),
            )
            .expect("subscription registers");
    }

    fn runtime(
        handlers: Arc<HookRegistry>,
        subscriptions: Arc<HookSubscriptionRegistry>,
    ) -> HookRuntime {
        HookRuntime::new(handlers, subscriptions)
    }

    #[test]
    fn no_subscriptions_returns_empty_response() {
        let runtime = runtime(
            Arc::new(HookRegistry::new()),
            Arc::new(HookSubscriptionRegistry::new()),
        );

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response, HookPhaseResponse::default());
    }

    #[test]
    fn one_hook_returns_contribution() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.one",
            calls.clone(),
            vec![Ok(HookHandlerResponse {
                contributions: vec![contribution("section.one", "context")],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.one",
            "test.one",
            0,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response.contributions.len(), 1);
        assert!(response.diagnostics.is_empty());
        assert_eq!(response.runs.len(), 1);
        assert_eq!(response.runs[0].status, HookRunStatus::Succeeded);
        assert_eq!(response.runs[0].contribution_count, 1);
        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![hook_id("test.one")]
        );
    }

    #[test]
    fn multiple_hooks_run_in_deterministic_order() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        for id in ["test.c", "test.b", "test.a"] {
            register_handler(
                &handlers,
                id,
                calls.clone(),
                vec![Ok(HookHandlerResponse {
                    contributions: vec![contribution(id, id)],
                    diagnostics: Vec::new(),
                    metadata: HookMetadata::default(),
                })],
            );
        }
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.c",
            "test.c",
            20,
            HookFailurePolicy::BestEffort,
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.b",
            "test.b",
            10,
            HookFailurePolicy::BestEffort,
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.a",
            "test.a",
            10,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![hook_id("test.a"), hook_id("test.b"), hook_id("test.c")]
        );
        assert_eq!(
            response
                .runs
                .into_iter()
                .map(|run| run.subscription_id)
                .collect::<Vec<_>>(),
            vec![
                subscription_id("sub.a"),
                subscription_id("sub.b"),
                subscription_id("sub.c")
            ]
        );
    }

    #[test]
    fn best_effort_failure_records_diagnostic_and_continues() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.fail",
            calls.clone(),
            vec![Err(
                hook_error("hook.failed", "failed").with_safe_for_user(true)
            )],
        );
        register_handler(
            &handlers,
            "test.ok",
            calls.clone(),
            vec![Ok(HookHandlerResponse {
                contributions: vec![contribution("section.ok", "ok")],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.fail",
            "test.fail",
            0,
            HookFailurePolicy::BestEffort,
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.ok",
            "test.ok",
            1,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response = block_on_ready(runtime.run_phase(phase_request()))
            .expect("best effort failure continues");

        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![hook_id("test.fail"), hook_id("test.ok")]
        );
        assert_eq!(response.contributions.len(), 1);
        assert_eq!(response.diagnostics.len(), 1);
        assert_eq!(response.diagnostics[0].code.as_str(), "hook.failed");
        assert_eq!(
            response.diagnostics[0].severity,
            HookDiagnosticSeverity::Warning
        );
        assert!(response.diagnostics[0].safe_for_user);
        assert_eq!(response.runs[0].status, HookRunStatus::Failed);
        assert_eq!(response.runs[0].diagnostic_count, 1);
        assert_eq!(response.runs[1].status, HookRunStatus::Succeeded);
    }

    #[test]
    fn non_best_effort_failure_returns_runtime_error() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.fail",
            calls.clone(),
            vec![Err(hook_error("hook.failed", "failed"))],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.fail",
            "test.fail",
            0,
            HookFailurePolicy::Required,
        );
        let runtime = runtime(handlers, subscriptions);

        let error = block_on_ready(runtime.run_phase(phase_request())).expect_err("runtime fails");

        assert!(matches!(
            error,
            HookRuntimeError::HookFailed { subscription_id, hook_id, phase, .. }
                if subscription_id == self::subscription_id("sub.fail")
                    && hook_id == self::hook_id("test.fail")
                    && phase == HookPhase::TurnPrePromptCompile
        ));
    }

    #[test]
    fn disabled_subscription_is_not_executed() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.one",
            calls.clone(),
            vec![Ok(HookHandlerResponse::default())],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.one",
            "test.one",
            0,
            HookFailurePolicy::BestEffort,
        );
        subscriptions
            .disable_subscription(&subscription_id("sub.one"))
            .expect("disable succeeds");
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response, HookPhaseResponse::default());
        assert!(calls.lock().expect("calls lock").is_empty());
    }

    #[test]
    fn runtime_uses_subscription_lookup_order_for_runs() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        for id in ["test.second", "test.first"] {
            register_handler(
                &handlers,
                id,
                calls.clone(),
                vec![Ok(HookHandlerResponse::default())],
            );
        }
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.second",
            "test.second",
            2,
            HookFailurePolicy::BestEffort,
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.first",
            "test.first",
            1,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(
            response
                .runs
                .into_iter()
                .map(|run| run.subscription_id)
                .collect::<Vec<_>>(),
            vec![subscription_id("sub.first"), subscription_id("sub.second")]
        );
    }

    #[test]
    fn missing_handler_during_runtime_returns_error() {
        let registration_handlers = Arc::new(HookRegistry::new());
        let runtime_handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &registration_handlers,
            "test.missing",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse::default())],
        );
        register_subscription(
            &registration_handlers,
            &subscriptions,
            "sub.missing",
            "test.missing",
            0,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(runtime_handlers, subscriptions);

        let error =
            block_on_ready(runtime.run_phase(phase_request())).expect_err("handler missing");

        assert!(matches!(
            error,
            HookRuntimeError::MissingHandler { subscription_id, hook_id, phase }
                if subscription_id == self::subscription_id("sub.missing")
                    && hook_id == self::hook_id("test.missing")
                    && phase == HookPhase::TurnPrePromptCompile
        ));
    }

    #[test]
    fn handler_response_diagnostics_are_aggregated() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.one",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse {
                contributions: Vec::new(),
                diagnostics: vec![diagnostic("diag.one", "diagnostic")],
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.one",
            "test.one",
            0,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response.diagnostics.len(), 1);
        assert_eq!(response.runs[0].diagnostic_count, 1);
    }

    #[test]
    fn phase_request_roundtrips() {
        let request = phase_request();
        let value = serde_json::to_value(&request).expect("request serializes");
        let decoded: HookPhaseRequest =
            serde_json::from_value(value).expect("request deserializes");

        assert_eq!(decoded, request);
    }

    #[test]
    fn phase_response_default_is_empty() {
        assert_eq!(HookPhaseResponse::default().runs.len(), 0);
        assert!(HookPhaseResponse::default().contributions.is_empty());
        assert!(HookPhaseResponse::default().diagnostics.is_empty());
    }

    #[test]
    fn run_status_serializes_stably() {
        assert_eq!(
            serde_json::to_value(HookRunStatus::Succeeded).expect("status serializes"),
            serde_json::json!("succeeded")
        );
    }

    #[test]
    fn runtime_error_display_is_stable() {
        let error = HookRuntimeError::MissingHandler {
            subscription_id: subscription_id("sub.missing"),
            hook_id: hook_id("test.missing"),
            phase: HookPhase::TurnPrePromptCompile,
        };

        assert_eq!(
            error.to_string(),
            "hook subscription `sub.missing` references missing handler `test.missing` for phase `turn.pre_prompt_compile`"
        );
    }

    #[test]
    fn filters_are_not_evaluated_in_phase_03() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.filtered",
            calls.clone(),
            vec![Ok(HookHandlerResponse::default())],
        );
        subscriptions
            .register_subscription(
                &handlers,
                HookSubscription::new(
                    subscription_id("sub.filtered"),
                    hook_id("test.filtered"),
                    HookPhase::TurnPrePromptCompile,
                )
                .with_filters(BTreeMap::from([(
                    HookFilterKey::new("would.not.match").expect("valid filter key"),
                    HookValue::Bool(false),
                )])),
            )
            .expect("subscription registers");
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response.runs.len(), 1);
        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![hook_id("test.filtered")]
        );
    }

    #[test]
    fn dependencies_do_not_change_order_in_phase_03() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        for id in ["test.early", "test.late"] {
            register_handler(
                &handlers,
                id,
                calls.clone(),
                vec![Ok(HookHandlerResponse::default())],
            );
        }
        subscriptions
            .register_subscription(
                &handlers,
                HookSubscription::new(
                    subscription_id("sub.early"),
                    hook_id("test.early"),
                    HookPhase::TurnPrePromptCompile,
                )
                .with_priority(0)
                .with_dependencies(HookSubscriptionDependencies::new(
                    [subscription_id("sub.late")],
                    [],
                )),
            )
            .expect("subscription registers");
        subscriptions
            .register_subscription(
                &handlers,
                HookSubscription::new(
                    subscription_id("sub.late"),
                    hook_id("test.late"),
                    HookPhase::TurnPrePromptCompile,
                )
                .with_priority(1),
            )
            .expect("subscription registers");
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(
            response
                .runs
                .into_iter()
                .map(|run| run.subscription_id)
                .collect::<Vec<_>>(),
            vec![subscription_id("sub.early"), subscription_id("sub.late")]
        );
    }
}
