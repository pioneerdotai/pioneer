use crate::classifier::{DefaultErrorClassifier, ErrorClassifier};
use crate::context::ToolOutcome;
use crate::context::{AnyToolResult, ToolInvocation};
use crate::error::ToolError;
use crate::events::ToolEventTrace;
use crate::permissions::{
    PermissionApprovalBroker, PermissionApprovalResolution, PermissionDecision,
    PermissionDecisionReason, PermissionEvaluationContext, PermissionIntent,
    ProfileToolPermissionEvaluator, StaticPermissionApprovalBroker, ToolPermissionEvaluator,
    extract_permission_intent, write_stdin_session_id,
};
use crate::registry::ToolRegistry;
use crate::spec::ToolIdempotencyMode;
use pioneer_protocol::{
    TurnExecutionSecuritySnapshot, TurnPermissionAuditDecision, TurnPermissionAuditEvent,
    TurnPermissionAuditEventKind, TurnPermissionAuditRequestKey, TurnPermissionMode,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

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
    pub retry_with_escalated_sandbox: bool,
}

impl Default for OrchestratorPolicy {
    fn default() -> Self {
        Self {
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

fn permission_decision_trace_payload(
    decision: &PermissionDecision,
    intent: &PermissionIntent,
) -> serde_json::Value {
    let mut payload = match decision {
        PermissionDecision::Allow { reason } => serde_json::json!({
            "decision": "allow",
            "reason": reason,
        }),
        PermissionDecision::Ask { key, reason } => serde_json::json!({
            "decision": "ask",
            "reason": reason,
            "action": key.action,
            "scope_hash": key.normalized_scope_hash,
        }),
        PermissionDecision::Deny { reason, .. } => serde_json::json!({
            "decision": "deny",
            "reason": reason,
        }),
    };

    if intent.is_unknown_capability()
        && let Some(map) = payload.as_object_mut()
    {
        map.insert("unknown_capability".to_owned(), serde_json::json!(true));
        if let Some(reason) = intent.scope.entries.get("unknown_reason") {
            map.insert("unknown_reason".to_owned(), serde_json::json!(reason));
        }
    }
    payload
}

fn shell_result_session_id(raw: &serde_json::Value) -> Option<u64> {
    raw.get("session_id").and_then(serde_json::Value::as_u64)
}

fn shell_result_is_finished(raw: &serde_json::Value) -> bool {
    raw.get("exit_code").is_some_and(|value| !value.is_null())
        || raw
            .get("timed_out")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
}

#[derive(Debug, Clone)]
struct PermissionEvaluationGrant {
    intent: PermissionIntent,
    request_key: Option<crate::PermissionRequestKey>,
}

#[derive(Debug, Clone)]
struct ShellSessionPermission {
    intent: PermissionIntent,
    request_key: crate::PermissionRequestKey,
}

fn security_snapshot_audit_fields(
    turn_id: &str,
    snapshot: Option<&TurnExecutionSecuritySnapshot>,
) -> (Option<String>, Option<u32>) {
    snapshot
        .map(|snapshot| (Some(snapshot.audit_id(turn_id)), Some(snapshot.version)))
        .unwrap_or((None, None))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ShellSessionPermissionKey {
    turn_id: String,
    profile_mode: TurnPermissionMode,
    session_id: u64,
}

#[derive(Clone)]
pub struct ToolOrchestrator {
    policy: OrchestratorPolicy,
    post_policy: PostExecutionPolicy,
    permission_evaluator: Arc<dyn ToolPermissionEvaluator>,
    approval_broker: Arc<dyn PermissionApprovalBroker>,
    approval_cache: Arc<Mutex<HashSet<crate::PermissionRequestKey>>>,
    shell_session_permissions:
        Arc<Mutex<HashMap<ShellSessionPermissionKey, ShellSessionPermission>>>,
}

impl Default for ToolOrchestrator {
    fn default() -> Self {
        Self {
            policy: OrchestratorPolicy::default(),
            post_policy: PostExecutionPolicy::default(),
            permission_evaluator: Arc::new(ProfileToolPermissionEvaluator),
            approval_broker: Arc::new(StaticPermissionApprovalBroker::default()),
            approval_cache: Arc::new(Mutex::new(HashSet::new())),
            shell_session_permissions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl ToolOrchestrator {
    pub fn new(policy: OrchestratorPolicy) -> Self {
        Self {
            policy,
            post_policy: PostExecutionPolicy::default(),
            permission_evaluator: Arc::new(ProfileToolPermissionEvaluator),
            approval_broker: Arc::new(StaticPermissionApprovalBroker::default()),
            approval_cache: Arc::new(Mutex::new(HashSet::new())),
            shell_session_permissions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_post_policy(policy: OrchestratorPolicy, post_policy: PostExecutionPolicy) -> Self {
        Self {
            policy,
            post_policy,
            permission_evaluator: Arc::new(ProfileToolPermissionEvaluator),
            approval_broker: Arc::new(StaticPermissionApprovalBroker::default()),
            approval_cache: Arc::new(Mutex::new(HashSet::new())),
            shell_session_permissions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_permission_evaluator(
        policy: OrchestratorPolicy,
        post_policy: PostExecutionPolicy,
        permission_evaluator: Arc<dyn ToolPermissionEvaluator>,
    ) -> Self {
        Self {
            policy,
            post_policy,
            permission_evaluator,
            approval_broker: Arc::new(StaticPermissionApprovalBroker::default()),
            approval_cache: Arc::new(Mutex::new(HashSet::new())),
            shell_session_permissions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_permission_evaluator_and_approval_broker(
        policy: OrchestratorPolicy,
        post_policy: PostExecutionPolicy,
        permission_evaluator: Arc<dyn ToolPermissionEvaluator>,
        approval_broker: Arc<dyn PermissionApprovalBroker>,
    ) -> Self {
        Self {
            policy,
            post_policy,
            permission_evaluator,
            approval_broker,
            approval_cache: Arc::new(Mutex::new(HashSet::new())),
            shell_session_permissions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_approval_broker(
        policy: OrchestratorPolicy,
        approval_broker: Arc<dyn PermissionApprovalBroker>,
    ) -> Self {
        Self {
            policy,
            post_policy: PostExecutionPolicy::default(),
            permission_evaluator: Arc::new(ProfileToolPermissionEvaluator),
            approval_broker,
            approval_cache: Arc::new(Mutex::new(HashSet::new())),
            shell_session_permissions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn run_with_context(
        &self,
        registry: &ToolRegistry,
        mut invocation: ToolInvocation,
        trace: &ToolEventTrace,
        permission_context: &PermissionEvaluationContext,
    ) -> Result<AnyToolResult, ToolError> {
        self.enforce_idempotency_contract(&invocation)?;

        invocation.attempt_id = 1;

        let permission_grant = self
            .evaluate_permission(&invocation, permission_context, trace)
            .await?;

        let first_attempt = self
            .run_in_sandbox(registry, invocation.clone(), SandboxTarget::Default, trace)
            .await;

        match first_attempt {
            Ok(mut result) => {
                self.update_shell_session_permission(
                    &invocation,
                    permission_context,
                    &permission_grant,
                    &result,
                );
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
                self.update_shell_session_permission(
                    &retry_invocation,
                    permission_context,
                    &permission_grant,
                    &result,
                );
                self.post_policy.apply(&retry_invocation, &mut result);
                Ok(result)
            }
            Err(error) => {
                self.cleanup_shell_session_permission_after_error(
                    &invocation,
                    permission_context,
                    &error,
                );
                if matches!(error, ToolError::Rejected(_)) {
                    self.emit_permission_audit(
                        trace,
                        &invocation,
                        permission_context,
                        &permission_grant.intent,
                        TurnPermissionAuditEventKind::DecisionDenied,
                        Some(TurnPermissionAuditDecision::Deny),
                        permission_grant.request_key.as_ref(),
                        Some(PermissionDecisionReason::SandboxDenied),
                        false,
                    );
                }
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

    fn emit_permission_audit(
        &self,
        trace: &ToolEventTrace,
        invocation: &ToolInvocation,
        permission_context: &PermissionEvaluationContext,
        intent: &PermissionIntent,
        event_kind: TurnPermissionAuditEventKind,
        decision: Option<TurnPermissionAuditDecision>,
        request_key: Option<&crate::PermissionRequestKey>,
        reason: Option<PermissionDecisionReason>,
        cached: bool,
    ) {
        let turn_id = permission_context
            .turn_id
            .clone()
            .unwrap_or_else(|| trace.turn_id().to_owned());
        let (security_snapshot_id, security_snapshot_version) = security_snapshot_audit_fields(
            turn_id.as_str(),
            invocation.execution_security_snapshot.as_ref(),
        );
        trace.emit_permission_audit(
            invocation.attempt_id,
            TurnPermissionAuditEvent {
                workspace_id: permission_context.workspace_id.clone().unwrap_or_default(),
                thread_id: permission_context.thread_id.clone().unwrap_or_default(),
                turn_id,
                event_kind,
                profile_mode: permission_context.permission_profile.mode,
                profile_source: permission_context.permission_profile.source,
                security_snapshot_id,
                security_snapshot_version,
                security_reason_code: None,
                security_capability: None,
                item_id: Some(invocation.call_id.clone()),
                tool_call_id: Some(invocation.call_id.clone()),
                tool_name: Some(invocation.tool_name.clone()),
                action_kind: Some(intent.action),
                request_key: request_key.map(|key| TurnPermissionAuditRequestKey {
                    action_kind: key.action,
                    scope_hash: key.normalized_scope_hash.clone(),
                }),
                decision,
                reason,
                cached,
            },
        );
    }

    async fn evaluate_permission(
        &self,
        invocation: &ToolInvocation,
        permission_context: &PermissionEvaluationContext,
        trace: &ToolEventTrace,
    ) -> Result<PermissionEvaluationGrant, ToolError> {
        trace.emit_stage(
            invocation.attempt_id,
            "permission.evaluate.started",
            None,
            Some(serde_json::json!({
                "profile_mode": permission_context.permission_profile.mode.as_str(),
                "profile_source": permission_context.permission_profile.source.as_str(),
            })),
        );

        let inherited_session_permission =
            self.inherited_shell_session_permission(invocation, permission_context);
        let intent = inherited_session_permission
            .as_ref()
            .map(|permission| permission.intent.clone())
            .unwrap_or_else(|| extract_permission_intent(invocation));
        let mut decision =
            self.permission_evaluator
                .evaluate(permission_context, invocation, &intent);
        if let (Some(permission), PermissionDecision::Ask { key: _, reason }) =
            (&inherited_session_permission, decision.clone())
        {
            decision = PermissionDecision::Ask {
                key: permission.request_key.clone(),
                reason,
            };
        }

        trace.emit_stage(
            invocation.attempt_id,
            "permission.evaluate.completed",
            None,
            Some(permission_decision_trace_payload(&decision, &intent)),
        );

        match decision {
            PermissionDecision::Allow { reason } => {
                self.emit_permission_audit(
                    trace,
                    invocation,
                    permission_context,
                    &intent,
                    TurnPermissionAuditEventKind::DecisionAllowed,
                    Some(TurnPermissionAuditDecision::Allow),
                    None,
                    Some(reason),
                    false,
                );
                Ok(PermissionEvaluationGrant {
                    intent,
                    request_key: None,
                })
            }
            PermissionDecision::Deny { reason, message } => {
                self.emit_permission_audit(
                    trace,
                    invocation,
                    permission_context,
                    &intent,
                    TurnPermissionAuditEventKind::DecisionDenied,
                    Some(TurnPermissionAuditDecision::Deny),
                    None,
                    Some(reason),
                    false,
                );
                trace.emit_stage(
                    invocation.attempt_id,
                    "permission.evaluate.failed",
                    Some(message.clone()),
                    None,
                );
                Err(ToolError::Rejected(message))
            }
            PermissionDecision::Ask { key, reason } => {
                if self
                    .approval_cache
                    .lock()
                    .map(|cache| cache.contains(&key))
                    .unwrap_or(false)
                {
                    trace.emit_stage(
                        invocation.attempt_id,
                        "permission.evaluate.cached",
                        None,
                        Some(serde_json::json!({
                                "action": key.action,
                                "scope_hash": key.normalized_scope_hash.as_str(),
                        })),
                    );
                    self.emit_permission_audit(
                        trace,
                        invocation,
                        permission_context,
                        &intent,
                        TurnPermissionAuditEventKind::DecisionAllowed,
                        Some(TurnPermissionAuditDecision::Allow),
                        Some(&key),
                        Some(PermissionDecisionReason::CachedApproval),
                        true,
                    );
                    return Ok(PermissionEvaluationGrant {
                        intent,
                        request_key: Some(key),
                    });
                }

                self.emit_permission_audit(
                    trace,
                    invocation,
                    permission_context,
                    &intent,
                    TurnPermissionAuditEventKind::ApprovalRequested,
                    Some(TurnPermissionAuditDecision::Ask),
                    Some(&key),
                    Some(reason),
                    false,
                );

                let resolution = self
                    .approval_broker
                    .request_approval(permission_context, invocation, &intent, &key, reason)
                    .await;
                match resolution {
                    PermissionApprovalResolution::AllowOnce => {
                        self.emit_permission_audit(
                            trace,
                            invocation,
                            permission_context,
                            &intent,
                            TurnPermissionAuditEventKind::ApprovalResolved,
                            Some(TurnPermissionAuditDecision::AllowOnce),
                            Some(&key),
                            Some(PermissionDecisionReason::UserApproved),
                            false,
                        );
                        Ok(PermissionEvaluationGrant {
                            intent,
                            request_key: Some(key),
                        })
                    }
                    PermissionApprovalResolution::AllowForTurn => {
                        self.emit_permission_audit(
                            trace,
                            invocation,
                            permission_context,
                            &intent,
                            TurnPermissionAuditEventKind::ApprovalResolved,
                            Some(TurnPermissionAuditDecision::AllowForTurn),
                            Some(&key),
                            Some(PermissionDecisionReason::UserApproved),
                            false,
                        );
                        if let Ok(mut cache) = self.approval_cache.lock() {
                            cache.insert(key.clone());
                        }
                        Ok(PermissionEvaluationGrant {
                            intent,
                            request_key: Some(key),
                        })
                    }
                    PermissionApprovalResolution::Deny { message } => {
                        self.emit_permission_audit(
                            trace,
                            invocation,
                            permission_context,
                            &intent,
                            TurnPermissionAuditEventKind::ApprovalResolved,
                            Some(TurnPermissionAuditDecision::Deny),
                            Some(&key),
                            Some(PermissionDecisionReason::UserDenied),
                            false,
                        );
                        trace.emit_stage(
                            invocation.attempt_id,
                            "permission.evaluate.failed",
                            Some(message.clone()),
                            None,
                        );
                        Err(ToolError::Rejected(message))
                    }
                    PermissionApprovalResolution::Cancelled => {
                        self.emit_permission_audit(
                            trace,
                            invocation,
                            permission_context,
                            &intent,
                            TurnPermissionAuditEventKind::ApprovalResolved,
                            Some(TurnPermissionAuditDecision::Cancelled),
                            Some(&key),
                            Some(PermissionDecisionReason::Cancelled),
                            false,
                        );
                        Err(ToolError::cancelled("permission approval cancelled"))
                    }
                    PermissionApprovalResolution::Expired => {
                        self.emit_permission_audit(
                            trace,
                            invocation,
                            permission_context,
                            &intent,
                            TurnPermissionAuditEventKind::ApprovalResolved,
                            Some(TurnPermissionAuditDecision::Expired),
                            Some(&key),
                            Some(PermissionDecisionReason::Expired),
                            false,
                        );
                        Err(ToolError::Rejected(
                            "permission approval expired".to_owned(),
                        ))
                    }
                }
            }
        }
    }

    fn inherited_shell_session_permission(
        &self,
        invocation: &ToolInvocation,
        permission_context: &PermissionEvaluationContext,
    ) -> Option<ShellSessionPermission> {
        let key = self
            .shell_session_permission_key(write_stdin_session_id(invocation)?, permission_context);
        self.shell_session_permissions
            .lock()
            .ok()
            .and_then(|sessions| sessions.get(&key).cloned())
    }

    fn update_shell_session_permission(
        &self,
        invocation: &ToolInvocation,
        permission_context: &PermissionEvaluationContext,
        grant: &PermissionEvaluationGrant,
        result: &AnyToolResult,
    ) {
        let raw = result.raw_output_json();
        match invocation.tool_name.as_str() {
            "exec_command" => {
                if let Some(session_id) = shell_result_session_id(&raw) {
                    let key = self.shell_session_permission_key(session_id, permission_context);
                    if shell_result_is_finished(&raw) {
                        self.remove_shell_session_permission(&key);
                    } else if let Some(request_key) = grant.request_key.clone() {
                        self.store_shell_session_permission(
                            key,
                            ShellSessionPermission {
                                intent: grant.intent.clone(),
                                request_key,
                            },
                        );
                    }
                }
            }
            "write_stdin" => {
                if let Some(session_id) = shell_result_session_id(&raw)
                    && shell_result_is_finished(&raw)
                {
                    let key = self.shell_session_permission_key(session_id, permission_context);
                    self.remove_shell_session_permission(&key);
                }
            }
            _ => {}
        }
    }

    fn cleanup_shell_session_permission_after_error(
        &self,
        invocation: &ToolInvocation,
        permission_context: &PermissionEvaluationContext,
        error: &ToolError,
    ) {
        if invocation.tool_name == "write_stdin"
            && matches!(error, ToolError::NotFound(_))
            && let Some(session_id) = write_stdin_session_id(invocation)
        {
            let key = self.shell_session_permission_key(session_id, permission_context);
            self.remove_shell_session_permission(&key);
        }
    }

    fn shell_session_permission_key(
        &self,
        session_id: u64,
        permission_context: &PermissionEvaluationContext,
    ) -> ShellSessionPermissionKey {
        ShellSessionPermissionKey {
            turn_id: permission_context.turn_id.clone().unwrap_or_default(),
            profile_mode: permission_context.permission_profile.mode,
            session_id,
        }
    }

    fn store_shell_session_permission(
        &self,
        key: ShellSessionPermissionKey,
        permission: ShellSessionPermission,
    ) {
        if let Ok(mut sessions) = self.shell_session_permissions.lock() {
            sessions.insert(key, permission);
        }
    }

    fn remove_shell_session_permission(&self, key: &ShellSessionPermissionKey) {
        if let Ok(mut sessions) = self.shell_session_permissions.lock() {
            sessions.remove(key);
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
            | ToolError::NotVisible(_)
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
    use crate::context::{
        ExecCommandArgs, FunctionToolOutput, LocalShellPayload, ToolCallSource, ToolInvocation,
        ToolPayload, WriteStdinArgs,
    };
    use crate::permissions::{
        PermissionActionKind, PermissionApprovalBroker, PermissionApprovalResolution,
        PermissionDecision, PermissionDecisionReason, PermissionEvaluationContext,
        PermissionIntent, PermissionRequestKey, PermissionRequestScope,
        StaticPermissionApprovalBroker, ToolPermissionEvaluator,
    };
    use crate::registry::{ToolHandler, ToolRegistry};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct CountingHandler {
        calls: Arc<AtomicUsize>,
        first_error: Option<ToolError>,
        success_text: &'static str,
    }

    struct StaticEvaluator {
        decision: PermissionDecision,
    }

    struct CountingApprovalBroker {
        calls: Arc<AtomicUsize>,
        resolution: PermissionApprovalResolution,
    }

    struct StaticJsonHandler {
        calls: Arc<AtomicUsize>,
        payload: serde_json::Value,
    }

    impl ToolPermissionEvaluator for StaticEvaluator {
        fn evaluate(
            &self,
            _context: &crate::PermissionEvaluationContext,
            _invocation: &ToolInvocation,
            _intent: &PermissionIntent,
        ) -> PermissionDecision {
            self.decision.clone()
        }
    }

    #[async_trait]
    impl ToolHandler for StaticJsonHandler {
        async fn handle(
            &self,
            _invocation: ToolInvocation,
            _trace: crate::events::ToolEventTrace,
        ) -> Result<Box<dyn crate::context::ToolOutput>, ToolError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(FunctionToolOutput::with_payload(
                self.payload.to_string(),
                true,
                self.payload.clone(),
            )))
        }
    }

    #[async_trait]
    impl PermissionApprovalBroker for CountingApprovalBroker {
        async fn request_approval(
            &self,
            _context: &PermissionEvaluationContext,
            _invocation: &ToolInvocation,
            _intent: &PermissionIntent,
            _key: &PermissionRequestKey,
            _reason: PermissionDecisionReason,
        ) -> PermissionApprovalResolution {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.resolution.clone()
        }
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
        invocation_for_tool(
            "tool",
            ToolPayload::Function {
                arguments: serde_json::json!({}),
            },
        )
    }

    fn invocation_for_tool(tool_name: &str, payload: ToolPayload) -> ToolInvocation {
        ToolInvocation {
            call_id: "call_1".to_owned(),
            tool_name: tool_name.to_owned(),
            source: ToolCallSource::Model,
            payload,
            workdir: PathBuf::from("."),
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            execution_security_snapshot: None,
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn registry_with_handler(handler: Arc<dyn ToolHandler>) -> ToolRegistry {
        let mut handlers: HashMap<String, Arc<dyn ToolHandler>> = HashMap::new();
        handlers.insert("tool".to_owned(), handler);
        ToolRegistry::new(handlers)
    }

    fn registry_with_named_handlers(
        handlers: impl IntoIterator<Item = (&'static str, Arc<dyn ToolHandler>)>,
    ) -> ToolRegistry {
        ToolRegistry::new(
            handlers
                .into_iter()
                .map(|(name, handler)| (name.to_owned(), handler))
                .collect(),
        )
    }

    fn default_test_permission_context() -> PermissionEvaluationContext {
        PermissionEvaluationContext::for_turn(
            "workspace_test",
            "thread_test",
            "turn",
            pioneer_protocol::default_turn_permission_profile_snapshot(),
        )
    }

    fn default_test_request_key(intent: &PermissionIntent) -> PermissionRequestKey {
        intent.request_key(&default_test_permission_context(), &invocation())
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
            retry_with_escalated_sandbox: true,
        });

        let trace = crate::events::ToolEventBus::default().start_trace("turn", "call_1", "tool");
        let context = default_test_permission_context();
        let result = orchestrator
            .run_with_context(&registry, invocation(), &trace, &context)
            .await;
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
            retry_with_escalated_sandbox: true,
        });

        let trace = crate::events::ToolEventBus::default().start_trace("turn", "call_1", "tool");
        let context = default_test_permission_context();
        let result = orchestrator
            .run_with_context(&registry, invocation(), &trace, &context)
            .await;
        match result {
            Ok(_) => panic!("invalid args should fail"),
            Err(error) => assert!(matches!(error, ToolError::InvalidArguments(_))),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn permission_deny_never_dispatches() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler: Arc<dyn ToolHandler> = Arc::new(CountingHandler {
            calls: calls.clone(),
            first_error: None,
            success_text: "ok",
        });
        let registry = registry_with_handler(handler);
        let orchestrator = ToolOrchestrator::with_permission_evaluator(
            OrchestratorPolicy::default(),
            PostExecutionPolicy::default(),
            Arc::new(StaticEvaluator {
                decision: PermissionDecision::Deny {
                    reason: PermissionDecisionReason::PolicyDeniesAction,
                    message: "denied for test".to_owned(),
                },
            }),
        );

        let trace = crate::events::ToolEventBus::default().start_trace("turn", "call_1", "tool");
        let context = default_test_permission_context();
        let result = orchestrator
            .run_with_context(&registry, invocation(), &trace, &context)
            .await;
        assert!(matches!(result, Err(ToolError::Rejected(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn permission_audit_event_redacts_scope_values() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler: Arc<dyn ToolHandler> = Arc::new(CountingHandler {
            calls: calls.clone(),
            first_error: None,
            success_text: "ok",
        });
        let registry = registry_with_named_handlers([("read_file", handler)]);
        let request_key = PermissionRequestKey {
            profile_mode: pioneer_protocol::TurnPermissionMode::Supervised,
            tool_name: "read_file".to_owned(),
            action: PermissionActionKind::FileRead,
            normalized_scope_hash: "scope_hash_1".to_owned(),
            turn_id: "turn_1".to_owned(),
        };
        let broker_calls = Arc::new(AtomicUsize::new(0));
        let orchestrator = ToolOrchestrator::with_permission_evaluator_and_approval_broker(
            OrchestratorPolicy::default(),
            PostExecutionPolicy::default(),
            Arc::new(StaticEvaluator {
                decision: PermissionDecision::Ask {
                    key: request_key.clone(),
                    reason: PermissionDecisionReason::PolicyRequiresApproval,
                },
            }),
            Arc::new(CountingApprovalBroker {
                calls: broker_calls.clone(),
                resolution: PermissionApprovalResolution::AllowOnce,
            }),
        );
        let event_bus = crate::events::ToolEventBus::new(8);
        let mut events = event_bus.subscribe();
        let trace = event_bus.start_trace("turn_1", "call_1", "read_file");
        let context = PermissionEvaluationContext::for_turn(
            "ws_1",
            "thread_1",
            "turn_1",
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            ),
        );
        let result = orchestrator
            .run_with_context(
                &registry,
                invocation_for_tool(
                    "read_file",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "path": "/Users/alexander/.ssh/id_rsa",
                        }),
                    },
                ),
                &trace,
                &context,
            )
            .await;

        assert!(result.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(broker_calls.load(Ordering::SeqCst), 1);

        let mut audit_payloads = Vec::new();
        for _ in 0..8 {
            let event = match tokio::time::timeout(Duration::from_millis(250), events.recv()).await
            {
                Ok(Ok(event)) => event,
                _ => break,
            };
            if let crate::events::ToolEventPayload::PermissionAudit(audit) = event.payload {
                audit_payloads.push(serde_json::to_value(audit).expect("audit serializes"));
            }
        }
        assert!(
            !audit_payloads.is_empty(),
            "expected at least one permission audit event"
        );

        assert!(
            audit_payloads.iter().any(|payload| {
                payload.get("eventKind").and_then(serde_json::Value::as_str)
                    == Some("approval_requested")
                    && payload
                        .pointer("/requestKey/scopeHash")
                        .and_then(serde_json::Value::as_str)
                        == Some(request_key.normalized_scope_hash.as_str())
            }),
            "approval_requested audit should contain request scope hash"
        );
        let combined = serde_json::to_string(&audit_payloads).expect("audit payloads stringify");
        assert!(!combined.contains(".ssh"));
        assert!(!combined.contains("id_rsa"));
    }

    #[tokio::test]
    async fn permission_ask_dispatches_after_approval() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler: Arc<dyn ToolHandler> = Arc::new(CountingHandler {
            calls: calls.clone(),
            first_error: None,
            success_text: "ok",
        });
        let registry = registry_with_handler(handler);
        let intent = PermissionIntent::new(
            PermissionActionKind::ShellCommand,
            PermissionRequestScope::from_pairs([("tool_name", "tool")]),
        );
        let orchestrator = ToolOrchestrator::with_permission_evaluator_and_approval_broker(
            OrchestratorPolicy::default(),
            PostExecutionPolicy::default(),
            Arc::new(StaticEvaluator {
                decision: PermissionDecision::Ask {
                    key: default_test_request_key(&intent),
                    reason: PermissionDecisionReason::PolicyRequiresApproval,
                },
            }),
            Arc::new(StaticPermissionApprovalBroker::allow_once()),
        );

        let trace = crate::events::ToolEventBus::default().start_trace("turn", "call_1", "tool");
        let context = default_test_permission_context();
        let result = orchestrator
            .run_with_context(&registry, invocation(), &trace, &context)
            .await;
        assert!(result.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn permission_ask_denied_does_not_dispatch() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler: Arc<dyn ToolHandler> = Arc::new(CountingHandler {
            calls: calls.clone(),
            first_error: None,
            success_text: "ok",
        });
        let registry = registry_with_handler(handler);
        let intent = PermissionIntent::new(
            PermissionActionKind::ShellCommand,
            PermissionRequestScope::from_pairs([("tool_name", "tool")]),
        );
        let orchestrator = ToolOrchestrator::with_permission_evaluator_and_approval_broker(
            OrchestratorPolicy::default(),
            PostExecutionPolicy::default(),
            Arc::new(StaticEvaluator {
                decision: PermissionDecision::Ask {
                    key: default_test_request_key(&intent),
                    reason: PermissionDecisionReason::PolicyRequiresApproval,
                },
            }),
            Arc::new(StaticPermissionApprovalBroker::deny("denied for test")),
        );

        let trace = crate::events::ToolEventBus::default().start_trace("turn", "call_1", "tool");
        let context = default_test_permission_context();
        let result = orchestrator
            .run_with_context(&registry, invocation(), &trace, &context)
            .await;
        assert!(matches!(result, Err(ToolError::Rejected(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unknown_capability_user_denial_does_not_dispatch() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler: Arc<dyn ToolHandler> = Arc::new(CountingHandler {
            calls: calls.clone(),
            first_error: None,
            success_text: "ok",
        });
        let registry = registry_with_named_handlers([("mystery_tool", handler)]);
        let orchestrator = ToolOrchestrator::with_permission_evaluator_and_approval_broker(
            OrchestratorPolicy::default(),
            PostExecutionPolicy::default(),
            Arc::new(ProfileToolPermissionEvaluator),
            Arc::new(StaticPermissionApprovalBroker::deny(
                "unknown capability denied",
            )),
        );
        let invocation = invocation_for_tool(
            "mystery_tool",
            ToolPayload::Function {
                arguments: serde_json::json!({ "value": true }),
            },
        );
        let trace =
            crate::events::ToolEventBus::default().start_trace("turn", "call_1", "mystery_tool");
        let context = PermissionEvaluationContext::for_turn(
            "workspace_test",
            "thread_test",
            "turn",
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            ),
        );

        let result = orchestrator
            .run_with_context(&registry, invocation, &trace, &context)
            .await;

        assert!(matches!(result, Err(ToolError::Rejected(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn permission_allow_for_turn_reuses_matching_request_key() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(CountingHandler {
            calls: calls.clone(),
            first_error: None,
            success_text: "ok",
        });
        let registry = registry_with_handler(handler);
        let intent = PermissionIntent::new(
            PermissionActionKind::ShellCommand,
            PermissionRequestScope::from_pairs([("tool_name", "tool")]),
        );
        let broker_calls = Arc::new(AtomicUsize::new(0));
        let orchestrator = ToolOrchestrator::with_permission_evaluator_and_approval_broker(
            OrchestratorPolicy::default(),
            PostExecutionPolicy::default(),
            Arc::new(StaticEvaluator {
                decision: PermissionDecision::Ask {
                    key: default_test_request_key(&intent),
                    reason: PermissionDecisionReason::PolicyRequiresApproval,
                },
            }),
            Arc::new(CountingApprovalBroker {
                calls: broker_calls.clone(),
                resolution: PermissionApprovalResolution::AllowForTurn,
            }),
        );

        let trace = crate::events::ToolEventBus::default().start_trace("turn", "call_1", "tool");
        let context = default_test_permission_context();
        let first = orchestrator
            .run_with_context(&registry, invocation(), &trace, &context)
            .await;
        assert!(first.is_ok());
        let trace = crate::events::ToolEventBus::default().start_trace("turn", "call_2", "tool");
        let second = orchestrator
            .run_with_context(&registry, invocation(), &trace, &context)
            .await;
        assert!(second.is_ok());

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(broker_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn permission_allow_once_does_not_populate_turn_cache() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(CountingHandler {
            calls: calls.clone(),
            first_error: None,
            success_text: "ok",
        });
        let registry = registry_with_handler(handler);
        let intent = PermissionIntent::new(
            PermissionActionKind::ShellCommand,
            PermissionRequestScope::from_pairs([("tool_name", "tool")]),
        );
        let broker_calls = Arc::new(AtomicUsize::new(0));
        let orchestrator = ToolOrchestrator::with_permission_evaluator_and_approval_broker(
            OrchestratorPolicy::default(),
            PostExecutionPolicy::default(),
            Arc::new(StaticEvaluator {
                decision: PermissionDecision::Ask {
                    key: default_test_request_key(&intent),
                    reason: PermissionDecisionReason::PolicyRequiresApproval,
                },
            }),
            Arc::new(CountingApprovalBroker {
                calls: broker_calls.clone(),
                resolution: PermissionApprovalResolution::AllowOnce,
            }),
        );

        let trace = crate::events::ToolEventBus::default().start_trace("turn", "call_1", "tool");
        let context = default_test_permission_context();
        let first = orchestrator
            .run_with_context(&registry, invocation(), &trace, &context)
            .await;
        assert!(first.is_ok());
        let trace = crate::events::ToolEventBus::default().start_trace("turn", "call_2", "tool");
        let second = orchestrator
            .run_with_context(&registry, invocation(), &trace, &context)
            .await;
        assert!(second.is_ok());

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(broker_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn write_stdin_reuses_approved_exec_command_session_scope_for_turn() {
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(StaticJsonHandler {
            calls: handler_calls.clone(),
            payload: serde_json::json!({
                "exit_code": null,
                "timed_out": false,
                "duration_ms": 1,
                "stdout": "",
                "stderr": "",
                "aggregated_output": "",
                "truncated": {
                    "stdout": false,
                    "stderr": false,
                    "aggregated_output": false
                },
                "session_id": 42,
                "command": ["bash"]
            }),
        });
        let registry = registry_with_named_handlers([
            ("exec_command", handler.clone() as Arc<dyn ToolHandler>),
            ("write_stdin", handler.clone() as Arc<dyn ToolHandler>),
        ]);
        let broker_calls = Arc::new(AtomicUsize::new(0));
        let orchestrator = ToolOrchestrator::with_approval_broker(
            OrchestratorPolicy::default(),
            Arc::new(CountingApprovalBroker {
                calls: broker_calls.clone(),
                resolution: PermissionApprovalResolution::AllowForTurn,
            }),
        );
        let context = PermissionEvaluationContext::for_turn(
            "workspace_test",
            "thread_test",
            "turn",
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            ),
        );

        let exec_invocation = invocation_for_tool(
            "exec_command",
            ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                command: Some(vec!["bash".to_owned()]),
                workdir: None,
                timeout_ms: None,
                max_output_tokens: None,
                yield_time_ms: None,
                tty: Some(true),
            })),
        );
        let trace =
            crate::events::ToolEventBus::default().start_trace("turn", "call_1", "exec_command");
        orchestrator
            .run_with_context(&registry, exec_invocation, &trace, &context)
            .await
            .expect("approved exec_command should dispatch");

        let write_invocation = invocation_for_tool(
            "write_stdin",
            ToolPayload::LocalShell(LocalShellPayload::WriteStdin(WriteStdinArgs {
                session_id: 42,
                chars: Some("echo hi\n".to_owned()),
                yield_time_ms: None,
                max_output_tokens: None,
            })),
        );
        let trace =
            crate::events::ToolEventBus::default().start_trace("turn", "call_2", "write_stdin");
        orchestrator
            .run_with_context(&registry, write_invocation, &trace, &context)
            .await
            .expect("known write_stdin should dispatch through cached approval");

        assert_eq!(handler_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            broker_calls.load(Ordering::SeqCst),
            1,
            "write_stdin should reuse the exec_command allow-for-turn request key"
        );
    }

    #[tokio::test]
    async fn write_stdin_session_approval_does_not_cross_turns() {
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(StaticJsonHandler {
            calls: handler_calls.clone(),
            payload: serde_json::json!({
                "exit_code": null,
                "timed_out": false,
                "duration_ms": 1,
                "stdout": "",
                "stderr": "",
                "aggregated_output": "",
                "truncated": {
                    "stdout": false,
                    "stderr": false,
                    "aggregated_output": false
                },
                "session_id": 42,
                "command": ["bash"]
            }),
        });
        let registry = registry_with_named_handlers([
            ("exec_command", handler.clone() as Arc<dyn ToolHandler>),
            ("write_stdin", handler.clone() as Arc<dyn ToolHandler>),
        ]);
        let broker_calls = Arc::new(AtomicUsize::new(0));
        let orchestrator = ToolOrchestrator::with_approval_broker(
            OrchestratorPolicy::default(),
            Arc::new(CountingApprovalBroker {
                calls: broker_calls.clone(),
                resolution: PermissionApprovalResolution::AllowForTurn,
            }),
        );
        let profile = pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
            pioneer_protocol::TurnPermissionMode::Supervised,
            pioneer_protocol::TurnPermissionProfileSource::Composer,
        );
        let first_context = PermissionEvaluationContext::for_turn(
            "workspace_test",
            "thread_test",
            "turn_1",
            profile.clone(),
        );
        let second_context = PermissionEvaluationContext::for_turn(
            "workspace_test",
            "thread_test",
            "turn_2",
            profile,
        );

        let exec_invocation = invocation_for_tool(
            "exec_command",
            ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                command: Some(vec!["bash".to_owned()]),
                workdir: None,
                timeout_ms: None,
                max_output_tokens: None,
                yield_time_ms: None,
                tty: Some(true),
            })),
        );
        let trace =
            crate::events::ToolEventBus::default().start_trace("turn_1", "call_1", "exec_command");
        orchestrator
            .run_with_context(&registry, exec_invocation, &trace, &first_context)
            .await
            .expect("approved exec_command should dispatch");

        let write_invocation = invocation_for_tool(
            "write_stdin",
            ToolPayload::LocalShell(LocalShellPayload::WriteStdin(WriteStdinArgs {
                session_id: 42,
                chars: Some("echo hi\n".to_owned()),
                yield_time_ms: None,
                max_output_tokens: None,
            })),
        );
        let trace =
            crate::events::ToolEventBus::default().start_trace("turn_2", "call_2", "write_stdin");
        orchestrator
            .run_with_context(&registry, write_invocation, &trace, &second_context)
            .await
            .expect("same session id in another turn should still ask and dispatch after approval");

        assert_eq!(handler_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            broker_calls.load(Ordering::SeqCst),
            2,
            "session approval must be scoped by turn/profile, not only shell session id"
        );
    }

    #[tokio::test]
    async fn write_stdin_unknown_session_still_asks_in_restricted_mode() {
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(StaticJsonHandler {
            calls: handler_calls.clone(),
            payload: serde_json::json!({
                "exit_code": null,
                "timed_out": false,
                "duration_ms": 1,
                "stdout": "",
                "stderr": "",
                "aggregated_output": "",
                "truncated": {
                    "stdout": false,
                    "stderr": false,
                    "aggregated_output": false
                },
                "session_id": 404,
                "command": ["bash"]
            }),
        });
        let registry =
            registry_with_named_handlers([("write_stdin", handler as Arc<dyn ToolHandler>)]);
        let broker_calls = Arc::new(AtomicUsize::new(0));
        let orchestrator = ToolOrchestrator::with_approval_broker(
            OrchestratorPolicy::default(),
            Arc::new(CountingApprovalBroker {
                calls: broker_calls.clone(),
                resolution: PermissionApprovalResolution::AllowOnce,
            }),
        );
        let context = PermissionEvaluationContext::for_turn(
            "workspace_test",
            "thread_test",
            "turn",
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            ),
        );
        let invocation = invocation_for_tool(
            "write_stdin",
            ToolPayload::LocalShell(LocalShellPayload::WriteStdin(WriteStdinArgs {
                session_id: 404,
                chars: Some("echo hi\n".to_owned()),
                yield_time_ms: None,
                max_output_tokens: None,
            })),
        );

        let trace =
            crate::events::ToolEventBus::default().start_trace("turn", "call_1", "write_stdin");
        orchestrator
            .run_with_context(&registry, invocation, &trace, &context)
            .await
            .expect("approved unknown session request should dispatch to handler");

        assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
        assert_eq!(broker_calls.load(Ordering::SeqCst), 1);
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
        let context = default_test_permission_context();
        let result = orchestrator
            .run_with_context(&registry, invocation(), &trace, &context)
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
