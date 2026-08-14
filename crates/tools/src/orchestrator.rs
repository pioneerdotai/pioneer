use crate::classifier::{DefaultErrorClassifier, ErrorClassifier};
use crate::context::ToolOutcome;
use crate::context::{AnyToolResult, ToolInvocation, ToolPayload};
use crate::error::ToolError;
use crate::events::ToolEventTrace;
use crate::mcp_policy::enforce_mcp_network_policy;
use crate::network_policy::{NetworkPolicyChecker, NetworkPolicyDenyReason};
use crate::permissions::{
    PermissionActionKind, PermissionApprovalBroker, PermissionApprovalResolution,
    PermissionDecision, PermissionDecisionReason, PermissionEvaluationContext, PermissionIntent,
    PermissionRequestScope, ProfileToolPermissionEvaluator, StaticPermissionApprovalBroker,
    ToolPermissionEvaluator, extract_permission_intent, write_stdin_session_id,
};
use crate::registry::ToolRegistry;
use crate::spec::ToolIdempotencyMode;
use crate::{FilePolicyChecker, FilePolicyDecision, FilePolicyDenyReason, FilePolicyOperation};
use pioneer_mcp::{McpToolSafetyHints, classify_mcp_tool_policy};
use pioneer_protocol::{
    TurnExecutionSecuritySnapshot, TurnFilesystemAccess, TurnFilesystemSandboxEntry,
    TurnFilesystemSandboxKind, TurnFilesystemSandboxPath, TurnNetworkMode,
    TurnNetworkPolicySnapshot, TurnPermissionAuditDecision, TurnPermissionAuditEvent,
    TurnPermissionAuditEventKind, TurnPermissionAuditRequestKey, TurnPermissionMode,
    TurnPermissionProfileSnapshot, TurnSecurityRuleProvenance,
};
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use url::Url;

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
    approval_scope: PermissionApprovalGrantScope,
}

#[derive(Debug, Clone)]
struct ShellSessionPermission {
    intent: PermissionIntent,
    request_key: crate::PermissionRequestKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PermissionApprovalGrantScope {
    None,
    Once,
    Turn,
}

impl PermissionApprovalGrantScope {
    fn can_apply_sandbox_grants(self) -> bool {
        matches!(self, Self::Once | Self::Turn)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FilesystemAccessRequirement {
    requested_path: PathBuf,
    grant_root: PathBuf,
    operation: FilePolicyOperation,
    grant_access: TurnFilesystemAccess,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FilesystemAccessGrant {
    root: PathBuf,
    access: TurnFilesystemAccess,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NetworkAccessRequirement {
    Enabled,
    Host { url: String, host: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NetworkAccessGrant {
    Enabled,
    Host(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FilesystemGrantCacheKey {
    workspace_id: String,
    turn_id: String,
    profile_mode: TurnPermissionMode,
    authority_binding_id: String,
    authority_binding_revision: u64,
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
    effective_permission_profile: Arc<Mutex<Option<TurnPermissionProfileSnapshot>>>,
    approval_cache: Arc<Mutex<HashSet<crate::PermissionRequestKey>>>,
    filesystem_grants: Arc<Mutex<HashMap<FilesystemGrantCacheKey, Vec<FilesystemAccessGrant>>>>,
    network_grants: Arc<Mutex<HashMap<FilesystemGrantCacheKey, Vec<NetworkAccessGrant>>>>,
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
            effective_permission_profile: Arc::new(Mutex::new(None)),
            approval_cache: Arc::new(Mutex::new(HashSet::new())),
            filesystem_grants: Arc::new(Mutex::new(HashMap::new())),
            network_grants: Arc::new(Mutex::new(HashMap::new())),
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
            effective_permission_profile: Arc::new(Mutex::new(None)),
            approval_cache: Arc::new(Mutex::new(HashSet::new())),
            filesystem_grants: Arc::new(Mutex::new(HashMap::new())),
            network_grants: Arc::new(Mutex::new(HashMap::new())),
            shell_session_permissions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_post_policy(policy: OrchestratorPolicy, post_policy: PostExecutionPolicy) -> Self {
        Self {
            policy,
            post_policy,
            permission_evaluator: Arc::new(ProfileToolPermissionEvaluator),
            approval_broker: Arc::new(StaticPermissionApprovalBroker::default()),
            effective_permission_profile: Arc::new(Mutex::new(None)),
            approval_cache: Arc::new(Mutex::new(HashSet::new())),
            filesystem_grants: Arc::new(Mutex::new(HashMap::new())),
            network_grants: Arc::new(Mutex::new(HashMap::new())),
            shell_session_permissions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn request_approval_or_cancel(
        &self,
        permission_context: &PermissionEvaluationContext,
        invocation: &ToolInvocation,
        intent: &PermissionIntent,
        key: &crate::PermissionRequestKey,
        reason: PermissionDecisionReason,
    ) -> PermissionApprovalResolution {
        tokio::select! {
            biased;
            _ = invocation.cancellation.cancelled() => {
                PermissionApprovalResolution::Cancelled
            }
            resolution = self.approval_broker.request_approval(
                permission_context,
                invocation,
                intent,
                key,
                reason,
            ) => resolution,
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
            effective_permission_profile: Arc::new(Mutex::new(None)),
            approval_cache: Arc::new(Mutex::new(HashSet::new())),
            filesystem_grants: Arc::new(Mutex::new(HashMap::new())),
            network_grants: Arc::new(Mutex::new(HashMap::new())),
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
            effective_permission_profile: Arc::new(Mutex::new(None)),
            approval_cache: Arc::new(Mutex::new(HashSet::new())),
            filesystem_grants: Arc::new(Mutex::new(HashMap::new())),
            network_grants: Arc::new(Mutex::new(HashMap::new())),
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
            effective_permission_profile: Arc::new(Mutex::new(None)),
            approval_cache: Arc::new(Mutex::new(HashSet::new())),
            filesystem_grants: Arc::new(Mutex::new(HashMap::new())),
            network_grants: Arc::new(Mutex::new(HashMap::new())),
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
        enforce_non_escalatable_mcp_network_policy(&invocation)?;

        let effective_permission_context = tokio::select! {
            biased;
            _ = invocation.cancellation.cancelled() => {
                return Err(ToolError::Cancelled("tool invocation cancelled".to_owned()));
            }
            result = self.approval_broker.revalidate_permission_context(
                permission_context,
                &invocation,
            ) => result.map_err(|_| {
                ToolError::Rejected("tool permission authority is no longer available".to_owned())
            })?,
        };
        if effective_permission_context.workspace_id != permission_context.workspace_id
            || effective_permission_context.thread_id != permission_context.thread_id
            || effective_permission_context.turn_id != permission_context.turn_id
        {
            return Err(ToolError::Rejected(
                "tool permission authority changed execution identity".to_owned(),
            ));
        }
        self.apply_effective_permission_profile(&effective_permission_context.permission_profile)?;

        let permission_grant = self
            .evaluate_permission(&invocation, &effective_permission_context, trace)
            .await?;
        self.ensure_filesystem_access(
            &mut invocation,
            &effective_permission_context,
            &permission_grant,
            trace,
        )
        .await?;
        self.ensure_network_access(
            &mut invocation,
            &effective_permission_context,
            &permission_grant,
            trace,
        )
        .await?;

        let first_attempt = self
            .run_in_sandbox(registry, invocation.clone(), SandboxTarget::Default, trace)
            .await;

        match first_attempt {
            Ok(mut result) => {
                self.update_shell_session_permission(
                    &invocation,
                    &effective_permission_context,
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
                    && self.can_retry_invocation(&invocation)
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
                    &effective_permission_context,
                    &permission_grant,
                    &result,
                );
                self.post_policy.apply(&retry_invocation, &mut result);
                Ok(result)
            }
            Err(error) => {
                self.cleanup_shell_session_permission_after_error(
                    &invocation,
                    &effective_permission_context,
                    &error,
                );
                if matches!(error, ToolError::Rejected(_)) {
                    self.emit_permission_audit(
                        trace,
                        &invocation,
                        &effective_permission_context,
                        &permission_grant.intent,
                        TurnPermissionAuditEventKind::DecisionDenied,
                        Some(TurnPermissionAuditDecision::Deny),
                        permission_grant.request_key.as_ref(),
                        Some(PermissionDecisionReason::SandboxDenied),
                        false,
                    )
                    .await?;
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

    fn apply_effective_permission_profile(
        &self,
        profile: &TurnPermissionProfileSnapshot,
    ) -> Result<(), ToolError> {
        let changed = {
            let mut current = self.effective_permission_profile.lock().map_err(|_| {
                ToolError::Rejected("tool permission projection is unavailable".to_owned())
            })?;
            let changed = current.as_ref().is_some_and(|current| current != profile);
            *current = Some(profile.clone());
            changed
        };
        if !changed {
            return Ok(());
        }

        self.approval_cache
            .lock()
            .map_err(|_| ToolError::Rejected("tool approval cache is unavailable".to_owned()))?
            .clear();
        self.filesystem_grants
            .lock()
            .map_err(|_| {
                ToolError::Rejected("tool filesystem grant cache is unavailable".to_owned())
            })?
            .clear();
        self.network_grants
            .lock()
            .map_err(|_| ToolError::Rejected("tool network grant cache is unavailable".to_owned()))?
            .clear();
        self.shell_session_permissions
            .lock()
            .map_err(|_| ToolError::Rejected("tool session grant cache is unavailable".to_owned()))?
            .clear();
        Ok(())
    }

    pub fn classify_error_outcome(
        &self,
        invocation: &ToolInvocation,
        error: &ToolError,
    ) -> ToolOutcome {
        self.post_policy.classify_error(invocation, error)
    }

    async fn emit_permission_audit(
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
    ) -> Result<(), ToolError> {
        let turn_id = permission_context
            .turn_id
            .clone()
            .unwrap_or_else(|| trace.turn_id().to_owned());
        let (security_snapshot_id, security_snapshot_version) = security_snapshot_audit_fields(
            turn_id.as_str(),
            invocation.execution_security_snapshot.as_ref(),
        );
        trace
            .emit_permission_audit(
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
            )
            .await
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
                )
                .await?;
                Ok(PermissionEvaluationGrant {
                    intent,
                    request_key: None,
                    approval_scope: PermissionApprovalGrantScope::None,
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
                )
                .await?;
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
                    )
                    .await?;
                    return Ok(PermissionEvaluationGrant {
                        intent,
                        request_key: Some(key),
                        approval_scope: PermissionApprovalGrantScope::Turn,
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
                )
                .await?;

                let resolution = self
                    .request_approval_or_cancel(
                        permission_context,
                        invocation,
                        &intent,
                        &key,
                        reason,
                    )
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
                        )
                        .await?;
                        Ok(PermissionEvaluationGrant {
                            intent,
                            request_key: Some(key),
                            approval_scope: PermissionApprovalGrantScope::Once,
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
                        )
                        .await?;
                        if let Ok(mut cache) = self.approval_cache.lock() {
                            cache.insert(key.clone());
                        }
                        Ok(PermissionEvaluationGrant {
                            intent,
                            request_key: Some(key),
                            approval_scope: PermissionApprovalGrantScope::Turn,
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
                        )
                        .await?;
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
                        )
                        .await?;
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
                        )
                        .await?;
                        Err(ToolError::Rejected(
                            "permission approval expired".to_owned(),
                        ))
                    }
                }
            }
        }
    }

    async fn ensure_filesystem_access(
        &self,
        invocation: &mut ToolInvocation,
        permission_context: &PermissionEvaluationContext,
        permission_grant: &PermissionEvaluationGrant,
        trace: &ToolEventTrace,
    ) -> Result<(), ToolError> {
        self.apply_cached_filesystem_grants(invocation, permission_context);

        let requirements = filesystem_access_requirements(invocation, &permission_grant.intent);
        if requirements.is_empty() {
            return Ok(());
        }

        let missing = missing_filesystem_access_grants(
            invocation.execution_security_snapshot.as_ref(),
            requirements.as_slice(),
        );
        if missing.is_empty() {
            return Ok(());
        }
        if !filesystem_grants_within_authority_cap(
            invocation.execution_security_snapshot.as_ref(),
            missing.as_slice(),
        ) {
            return Err(ToolError::Rejected(
                "requested filesystem access is outside the immutable execution authority"
                    .to_owned(),
            ));
        }

        if permission_grant.approval_scope.can_apply_sandbox_grants() {
            self.apply_filesystem_grants(
                invocation,
                permission_context,
                missing,
                permission_grant.approval_scope,
                trace,
                "covered_by_tool_approval",
            );
            return Ok(());
        }

        let intent =
            filesystem_access_permission_intent(invocation, &permission_grant.intent, &missing);
        let key = intent.request_key(permission_context, invocation);
        self.emit_permission_audit(
            trace,
            invocation,
            permission_context,
            &intent,
            TurnPermissionAuditEventKind::ApprovalRequested,
            Some(TurnPermissionAuditDecision::Ask),
            Some(&key),
            Some(PermissionDecisionReason::SandboxDenied),
            false,
        )
        .await?;

        let resolution = self
            .request_approval_or_cancel(
                permission_context,
                invocation,
                &intent,
                &key,
                PermissionDecisionReason::SandboxDenied,
            )
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
                )
                .await?;
                self.apply_filesystem_grants(
                    invocation,
                    permission_context,
                    missing,
                    PermissionApprovalGrantScope::Once,
                    trace,
                    "approved_filesystem_grant_once",
                );
                Ok(())
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
                )
                .await?;
                self.apply_filesystem_grants(
                    invocation,
                    permission_context,
                    missing,
                    PermissionApprovalGrantScope::Turn,
                    trace,
                    "approved_filesystem_grant_for_turn",
                );
                Ok(())
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
                )
                .await?;
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
                )
                .await?;
                Err(ToolError::cancelled("filesystem access approval cancelled"))
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
                )
                .await?;
                Err(ToolError::Rejected(
                    "filesystem access approval expired".to_owned(),
                ))
            }
        }
    }

    async fn ensure_network_access(
        &self,
        invocation: &mut ToolInvocation,
        permission_context: &PermissionEvaluationContext,
        permission_grant: &PermissionEvaluationGrant,
        trace: &ToolEventTrace,
    ) -> Result<(), ToolError> {
        self.apply_cached_network_grants(invocation, permission_context);

        let requirements = network_access_requirements(invocation, &permission_grant.intent);
        if requirements.is_empty() {
            return Ok(());
        }

        let missing = missing_network_access_grants(
            invocation.execution_security_snapshot.as_ref(),
            requirements.as_slice(),
        );
        if missing.is_empty() {
            return Ok(());
        }
        if !network_grants_within_authority_cap(
            invocation.execution_security_snapshot.as_ref(),
            missing.as_slice(),
        ) {
            return Err(ToolError::Rejected(
                "requested network access is outside the immutable execution authority".to_owned(),
            ));
        }

        if permission_grant.approval_scope.can_apply_sandbox_grants() {
            self.apply_network_grants(
                invocation,
                permission_context,
                missing,
                permission_grant.approval_scope,
                trace,
                "covered_by_tool_approval",
            );
            return Ok(());
        }

        let intent =
            network_access_permission_intent(invocation, &permission_grant.intent, &missing);
        let key = intent.request_key(permission_context, invocation);
        self.emit_permission_audit(
            trace,
            invocation,
            permission_context,
            &intent,
            TurnPermissionAuditEventKind::ApprovalRequested,
            Some(TurnPermissionAuditDecision::Ask),
            Some(&key),
            Some(PermissionDecisionReason::SandboxDenied),
            false,
        )
        .await?;

        let resolution = self
            .request_approval_or_cancel(
                permission_context,
                invocation,
                &intent,
                &key,
                PermissionDecisionReason::SandboxDenied,
            )
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
                )
                .await?;
                self.apply_network_grants(
                    invocation,
                    permission_context,
                    missing,
                    PermissionApprovalGrantScope::Once,
                    trace,
                    "approved_network_grant_once",
                );
                Ok(())
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
                )
                .await?;
                self.apply_network_grants(
                    invocation,
                    permission_context,
                    missing,
                    PermissionApprovalGrantScope::Turn,
                    trace,
                    "approved_network_grant_for_turn",
                );
                Ok(())
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
                )
                .await?;
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
                )
                .await?;
                Err(ToolError::cancelled("network access approval cancelled"))
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
                )
                .await?;
                Err(ToolError::Rejected(
                    "network access approval expired".to_owned(),
                ))
            }
        }
    }

    fn apply_cached_filesystem_grants(
        &self,
        invocation: &mut ToolInvocation,
        permission_context: &PermissionEvaluationContext,
    ) {
        let Some(cache_key) = filesystem_grant_cache_key(
            permission_context,
            invocation.execution_security_snapshot.as_ref(),
        ) else {
            return;
        };
        let mut grants = self
            .filesystem_grants
            .lock()
            .ok()
            .and_then(|cache| cache.get(&cache_key).cloned())
            .unwrap_or_default();
        grants.retain(|grant| {
            filesystem_grants_within_authority_cap(
                invocation.execution_security_snapshot.as_ref(),
                std::slice::from_ref(grant),
            )
        });
        apply_filesystem_grants_to_invocation(invocation, grants.as_slice());
    }

    fn apply_filesystem_grants(
        &self,
        invocation: &mut ToolInvocation,
        permission_context: &PermissionEvaluationContext,
        grants: Vec<FilesystemAccessGrant>,
        scope: PermissionApprovalGrantScope,
        trace: &ToolEventTrace,
        reason: &str,
    ) {
        apply_filesystem_grants_to_invocation(invocation, grants.as_slice());
        if scope == PermissionApprovalGrantScope::Turn
            && let Some(cache_key) = filesystem_grant_cache_key(
                permission_context,
                invocation.execution_security_snapshot.as_ref(),
            )
            && let Ok(mut cache) = self.filesystem_grants.lock()
        {
            let cached = cache.entry(cache_key).or_default();
            merge_filesystem_grants(cached, grants.as_slice());
        }
        trace.emit_stage(
            invocation.attempt_id,
            "permission.filesystem_grant.applied",
            None,
            Some(serde_json::json!({
                "reason": reason,
                "grant_count": grants.len(),
                "scope": match scope {
                    PermissionApprovalGrantScope::None => "none",
                    PermissionApprovalGrantScope::Once => "once",
                    PermissionApprovalGrantScope::Turn => "turn",
                },
            })),
        );
    }

    fn apply_cached_network_grants(
        &self,
        invocation: &mut ToolInvocation,
        permission_context: &PermissionEvaluationContext,
    ) {
        let Some(cache_key) = filesystem_grant_cache_key(
            permission_context,
            invocation.execution_security_snapshot.as_ref(),
        ) else {
            return;
        };
        let mut grants = self
            .network_grants
            .lock()
            .ok()
            .and_then(|cache| cache.get(&cache_key).cloned())
            .unwrap_or_default();
        grants.retain(|grant| {
            network_grants_within_authority_cap(
                invocation.execution_security_snapshot.as_ref(),
                std::slice::from_ref(grant),
            )
        });
        apply_network_grants_to_invocation(invocation, grants.as_slice());
    }

    fn apply_network_grants(
        &self,
        invocation: &mut ToolInvocation,
        permission_context: &PermissionEvaluationContext,
        grants: Vec<NetworkAccessGrant>,
        scope: PermissionApprovalGrantScope,
        trace: &ToolEventTrace,
        reason: &str,
    ) {
        apply_network_grants_to_invocation(invocation, grants.as_slice());
        if scope == PermissionApprovalGrantScope::Turn
            && let Some(cache_key) = filesystem_grant_cache_key(
                permission_context,
                invocation.execution_security_snapshot.as_ref(),
            )
            && let Ok(mut cache) = self.network_grants.lock()
        {
            let cached = cache.entry(cache_key).or_default();
            merge_network_grants(cached, grants.as_slice());
        }
        trace.emit_stage(
            invocation.attempt_id,
            "permission.network_grant.applied",
            None,
            Some(serde_json::json!({
                "reason": reason,
                "grant_count": grants.len(),
                "scope": match scope {
                    PermissionApprovalGrantScope::None => "none",
                    PermissionApprovalGrantScope::Once => "once",
                    PermissionApprovalGrantScope::Turn => "turn",
                },
            })),
        );
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
            ToolIdempotencyMode::None | ToolIdempotencyMode::Safe => Ok(()),
            ToolIdempotencyMode::RequiresKey | ToolIdempotencyMode::SessionBound => {
                // The first delivery is allowed to establish the operation.
                // A key by itself is only correlation metadata until a
                // verified operation ledger/backend is present, so automatic
                // retries for these modes are rejected below instead of
                // replaying an ambiguous side effect.
                if invocation.attempt_id <= 1 {
                    return Ok(());
                }
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

    fn can_retry_invocation(&self, invocation: &ToolInvocation) -> bool {
        matches!(
            invocation.recovery.idempotency_mode,
            ToolIdempotencyMode::None | ToolIdempotencyMode::Safe
        )
    }
}

fn enforce_non_escalatable_mcp_network_policy(
    invocation: &ToolInvocation,
) -> Result<(), ToolError> {
    let ToolPayload::Mcp {
        server,
        tool,
        read_only_hint,
        destructive_hint,
        open_world_hint,
        ..
    } = &invocation.payload
    else {
        return Ok(());
    };

    let classification = classify_mcp_tool_policy(McpToolSafetyHints {
        read_only_hint: *read_only_hint,
        destructive_hint: *destructive_hint,
        open_world_hint: *open_world_hint,
    });
    enforce_mcp_network_policy(
        invocation.execution_security_snapshot.as_ref(),
        &classification,
        server,
        tool,
    )
}

fn filesystem_grant_cache_key(
    permission_context: &PermissionEvaluationContext,
    snapshot: Option<&TurnExecutionSecuritySnapshot>,
) -> Option<FilesystemGrantCacheKey> {
    let authority_cap = &snapshot?.authority_cap;
    Some(FilesystemGrantCacheKey {
        workspace_id: permission_context.workspace_id.clone()?,
        turn_id: permission_context.turn_id.clone()?,
        profile_mode: permission_context.permission_profile.mode,
        authority_binding_id: authority_cap.resource_binding_id.clone(),
        authority_binding_revision: authority_cap.resource_binding_revision,
    })
}

fn filesystem_grants_within_authority_cap(
    snapshot: Option<&TurnExecutionSecuritySnapshot>,
    grants: &[FilesystemAccessGrant],
) -> bool {
    let Some(cap) = snapshot.map(|snapshot| &snapshot.authority_cap) else {
        return grants.is_empty();
    };
    if cap.filesystem.kind == TurnFilesystemSandboxKind::Unrestricted {
        return true;
    }
    grants.iter().all(|grant| {
        let grant_root = normalize_path_lexically(grant.root.clone());
        cap.filesystem.entries.iter().any(|entry| {
            let Some(cap_root) = filesystem_entry_root(entry) else {
                return false;
            };
            grant_root.starts_with(cap_root)
                && access_rank(grant.access) <= access_rank(entry.access)
        })
    })
}

fn network_grants_within_authority_cap(
    snapshot: Option<&TurnExecutionSecuritySnapshot>,
    grants: &[NetworkAccessGrant],
) -> bool {
    let Some(snapshot) = snapshot else {
        return grants.is_empty();
    };
    let cap = &snapshot.authority_cap;
    grants.iter().all(|grant| match grant {
        NetworkAccessGrant::Enabled => cap.network.mode == TurnNetworkMode::Enabled,
        NetworkAccessGrant::Host(host) => {
            let mut cap_snapshot = snapshot.clone();
            cap_snapshot.network = cap.network.clone();
            cap_snapshot.sandbox.network = cap.network.clone();
            let url = if host.contains(':') && !host.starts_with('[') {
                format!("https://[{host}]/")
            } else {
                format!("https://{host}/")
            };
            matches!(
                NetworkPolicyChecker::check_url(&cap_snapshot, url.as_str(), "authority cap"),
                crate::network_policy::NetworkPolicyDecision::Allowed(_)
            )
        }
    })
}

fn filesystem_entry_root(entry: &TurnFilesystemSandboxEntry) -> Option<PathBuf> {
    entry
        .resolved_path
        .as_deref()
        .map(PathBuf::from)
        .or_else(|| match &entry.path {
            TurnFilesystemSandboxPath::ExplicitPath { path } => Some(PathBuf::from(path)),
            _ => None,
        })
        .map(normalize_path_lexically)
}

fn filesystem_access_requirements(
    invocation: &ToolInvocation,
    intent: &PermissionIntent,
) -> Vec<FilesystemAccessRequirement> {
    match intent.action {
        PermissionActionKind::FileRead => intent_paths(intent)
            .into_iter()
            .map(|path| {
                let grant_root = match invocation.tool_name.as_str() {
                    "list_dir" | "grep_files" => path.clone(),
                    _ => path.clone(),
                };
                FilesystemAccessRequirement {
                    requested_path: path,
                    grant_root,
                    operation: FilePolicyOperation::Read,
                    grant_access: TurnFilesystemAccess::Read,
                }
            })
            .collect(),
        PermissionActionKind::FileWrite => intent_paths(intent)
            .into_iter()
            .map(|path| FilesystemAccessRequirement {
                grant_root: writable_parent_root(path.as_path()),
                requested_path: path,
                operation: FilePolicyOperation::Write,
                grant_access: TurnFilesystemAccess::Write,
            })
            .collect(),
        PermissionActionKind::ShellCommand
            if invocation.tool_name == "exec_command"
                && matches!(
                    &invocation.payload,
                    crate::context::ToolPayload::LocalShell(
                        crate::context::LocalShellPayload::ExecCommand(args)
                    ) if args.workdir.as_deref().is_some_and(|workdir| !workdir.trim().is_empty())
                ) =>
        {
            intent
                .scope
                .entries
                .get("cwd")
                .map(|cwd| {
                    let cwd = normalize_path_lexically(PathBuf::from(cwd));
                    FilesystemAccessRequirement {
                        requested_path: cwd.clone(),
                        grant_root: cwd,
                        operation: FilePolicyOperation::Write,
                        grant_access: TurnFilesystemAccess::Write,
                    }
                })
                .into_iter()
                .collect()
        }
        PermissionActionKind::Network if invocation.tool_name == "download_url" => intent
            .scope
            .entries
            .get("destination")
            .map(|destination| {
                let destination = normalize_path_lexically(PathBuf::from(destination));
                FilesystemAccessRequirement {
                    grant_root: writable_parent_root(destination.as_path()),
                    requested_path: destination,
                    operation: FilePolicyOperation::Write,
                    grant_access: TurnFilesystemAccess::Write,
                }
            })
            .into_iter()
            .collect(),
        _ => Vec::new(),
    }
}

fn network_access_requirements(
    invocation: &ToolInvocation,
    intent: &PermissionIntent,
) -> Vec<NetworkAccessRequirement> {
    if intent.action == PermissionActionKind::Network {
        if invocation.tool_name == "web_search" {
            return vec![NetworkAccessRequirement::Enabled];
        }
        if let Some(url) = network_url_from_intent(intent)
            && let Ok(parsed) = Url::parse(url.as_str())
            && let Some(host) = parsed.host_str().map(|host| host.to_ascii_lowercase())
        {
            return vec![NetworkAccessRequirement::Host { url, host }];
        }
    }

    if intent
        .scope
        .entries
        .get("mcp_requires_network")
        .is_some_and(|value| value == "true")
    {
        return vec![NetworkAccessRequirement::Enabled];
    }

    Vec::new()
}

fn network_url_from_intent(intent: &PermissionIntent) -> Option<String> {
    if let Some(origin) = intent.scope.entries.get("url_origin") {
        let path = intent
            .scope
            .entries
            .get("url_path")
            .map(String::as_str)
            .unwrap_or("/");
        return Some(format!("{origin}{path}"));
    }
    intent
        .scope
        .entries
        .get("domain")
        .map(|domain| format!("https://{domain}/"))
}

fn missing_network_access_grants(
    snapshot: Option<&TurnExecutionSecuritySnapshot>,
    requirements: &[NetworkAccessRequirement],
) -> Vec<NetworkAccessGrant> {
    let Some(snapshot) = snapshot else {
        return Vec::new();
    };

    let mut missing = Vec::new();
    for requirement in requirements {
        match requirement {
            NetworkAccessRequirement::Enabled => {
                if snapshot.network.mode != TurnNetworkMode::Enabled {
                    missing.push(NetworkAccessGrant::Enabled);
                }
            }
            NetworkAccessRequirement::Host { url, host } => {
                match NetworkPolicyChecker::check_url(snapshot, url.as_str(), "network grant") {
                    crate::network_policy::NetworkPolicyDecision::Allowed(_) => {}
                    crate::network_policy::NetworkPolicyDecision::Denied(deny)
                        if matches!(
                            deny.reason,
                            NetworkPolicyDenyReason::NetworkDisabled
                                | NetworkPolicyDenyReason::HostNotAllowed
                        ) =>
                    {
                        missing.push(NetworkAccessGrant::Host(host.clone()));
                    }
                    crate::network_policy::NetworkPolicyDecision::Denied(_) => {}
                }
            }
        }
    }

    let mut deduped = Vec::new();
    merge_network_grants(&mut deduped, missing.as_slice());
    deduped
}

fn network_access_permission_intent(
    invocation: &ToolInvocation,
    original_intent: &PermissionIntent,
    grants: &[NetworkAccessGrant],
) -> PermissionIntent {
    let broad = grants
        .iter()
        .any(|grant| matches!(grant, NetworkAccessGrant::Enabled));
    let hosts = grants
        .iter()
        .filter_map(|grant| match grant {
            NetworkAccessGrant::Host(host) => Some(host.clone()),
            NetworkAccessGrant::Enabled => None,
        })
        .collect::<Vec<_>>();

    let mut scope = PermissionRequestScope::from_pairs([
        ("tool_name", invocation.tool_name.as_str()),
        ("source", invocation.source.as_str()),
        ("operation", "network access grant"),
        ("network_mode", if broad { "enabled" } else { "restricted" }),
    ]);
    for (key, value) in &original_intent.scope.entries {
        if matches!(
            key.as_str(),
            "url_origin" | "domain" | "method" | "server" | "tool"
        ) {
            scope.entries.insert(key.clone(), value.clone());
        }
    }
    if !hosts.is_empty() {
        scope.entries.insert(
            "network_hosts".to_owned(),
            serde_json::to_string(&hosts).unwrap_or_else(|_| "[]".to_owned()),
        );
        if let Some(first) = hosts.first() {
            scope
                .entries
                .insert("network_host".to_owned(), first.clone());
        }
    }

    PermissionIntent {
        action: PermissionActionKind::Network,
        scope,
        summary: Some(format!(
            "grant {} network access for `{}`",
            if broad { "enabled" } else { "host-scoped" },
            invocation.tool_name
        )),
    }
}

fn intent_paths(intent: &PermissionIntent) -> Vec<PathBuf> {
    let mut paths = Vec::<PathBuf>::new();
    if let Some(path) = intent.scope.entries.get("path") {
        paths.push(PathBuf::from(path));
    }
    for index in 0..20 {
        if let Some(path) = intent.scope.entries.get(format!("path.{index}").as_str()) {
            paths.push(PathBuf::from(path));
        }
    }
    if let Some(changed_paths) = intent.scope.entries.get("changed_paths")
        && let Ok(decoded) = serde_json::from_str::<Vec<String>>(changed_paths)
    {
        paths.extend(decoded.into_iter().map(PathBuf::from));
    }
    dedupe_paths(paths)
}

fn missing_filesystem_access_grants(
    snapshot: Option<&TurnExecutionSecuritySnapshot>,
    requirements: &[FilesystemAccessRequirement],
) -> Vec<FilesystemAccessGrant> {
    let Some(snapshot) = snapshot else {
        return Vec::new();
    };
    if snapshot.sandbox.filesystem.kind == TurnFilesystemSandboxKind::Unrestricted {
        return Vec::new();
    }

    let mut missing = Vec::new();
    for requirement in requirements {
        match FilePolicyChecker::check(
            snapshot,
            requirement.operation,
            requirement.requested_path.as_path(),
        ) {
            FilePolicyDecision::Allowed(_) => {}
            FilePolicyDecision::Denied(deny) if filesystem_deny_can_request_grant(deny.reason) => {
                let grant_root = match deny.reason {
                    FilePolicyDenyReason::SymlinkEscape => deny
                        .resolved_path
                        .as_deref()
                        .map(|path| match requirement.operation {
                            FilePolicyOperation::Read => path.to_path_buf(),
                            FilePolicyOperation::Write => writable_parent_root(path),
                        })
                        .unwrap_or_else(|| requirement.grant_root.clone()),
                    _ => requirement.grant_root.clone(),
                };
                missing.push(FilesystemAccessGrant {
                    root: normalize_path_lexically(grant_root),
                    access: requirement.grant_access,
                });
            }
            FilePolicyDecision::Denied(_) => {}
        }
    }
    let mut deduped = Vec::new();
    merge_filesystem_grants(&mut deduped, missing.as_slice());
    deduped
}

fn filesystem_deny_can_request_grant(reason: FilePolicyDenyReason) -> bool {
    matches!(
        reason,
        FilePolicyDenyReason::OutsideAllowedRoots
            | FilePolicyDenyReason::SymlinkEscape
            | FilePolicyDenyReason::WriteRequiresWritableRoot
            | FilePolicyDenyReason::NoUsableRoots
            | FilePolicyDenyReason::InvalidRoot
    )
}

fn filesystem_access_permission_intent(
    invocation: &ToolInvocation,
    original_intent: &PermissionIntent,
    grants: &[FilesystemAccessGrant],
) -> PermissionIntent {
    let access = if grants
        .iter()
        .any(|grant| grant.access == TurnFilesystemAccess::Write)
    {
        TurnFilesystemAccess::Write
    } else {
        TurnFilesystemAccess::Read
    };
    let mut scope = PermissionRequestScope::from_pairs([
        ("tool_name", invocation.tool_name.as_str()),
        ("source", invocation.source.as_str()),
        ("operation", "filesystem access grant"),
        (
            "grant_access",
            match access {
                TurnFilesystemAccess::None => "none",
                TurnFilesystemAccess::Read => "read",
                TurnFilesystemAccess::Write => "write",
            },
        ),
    ]);
    for (key, value) in &original_intent.scope.entries {
        if matches!(key.as_str(), "path" | "cwd" | "command" | "argv") {
            scope.entries.insert(key.clone(), value.clone());
        }
    }
    scope.entries.insert(
        "grant_roots".to_owned(),
        serde_json::to_string(
            &grants
                .iter()
                .map(|grant| grant.root.display().to_string())
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|_| "[]".to_owned()),
    );
    if let Some(first) = grants.first() {
        scope
            .entries
            .insert("grant_root".to_owned(), first.root.display().to_string());
    }

    PermissionIntent {
        action: match access {
            TurnFilesystemAccess::Write => PermissionActionKind::FileWrite,
            TurnFilesystemAccess::Read | TurnFilesystemAccess::None => {
                PermissionActionKind::FileRead
            }
        },
        scope,
        summary: Some(format!(
            "grant {} filesystem access for `{}`",
            match access {
                TurnFilesystemAccess::None => "no",
                TurnFilesystemAccess::Read => "read",
                TurnFilesystemAccess::Write => "write",
            },
            invocation.tool_name
        )),
    }
}

fn apply_filesystem_grants_to_invocation(
    invocation: &mut ToolInvocation,
    grants: &[FilesystemAccessGrant],
) {
    let Some(snapshot) = invocation.execution_security_snapshot.as_mut() else {
        return;
    };
    if grants.is_empty()
        || snapshot.sandbox.filesystem.kind == TurnFilesystemSandboxKind::Unrestricted
    {
        return;
    }
    let grants = grants
        .iter()
        .filter(|grant| {
            filesystem_grants_within_authority_cap(Some(snapshot), std::slice::from_ref(*grant))
        })
        .cloned()
        .collect::<Vec<_>>();
    for grant in &grants {
        upsert_filesystem_grant(snapshot, grant);
    }
}

fn apply_network_grants_to_invocation(
    invocation: &mut ToolInvocation,
    grants: &[NetworkAccessGrant],
) {
    let Some(snapshot) = invocation.execution_security_snapshot.as_mut() else {
        return;
    };
    if grants.is_empty() || snapshot.network.mode == TurnNetworkMode::Enabled {
        return;
    }

    let grants = grants
        .iter()
        .filter(|grant| {
            network_grants_within_authority_cap(Some(snapshot), std::slice::from_ref(*grant))
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut network = snapshot.network.clone();
    merge_network_policy_grants(&mut network, grants.as_slice());
    snapshot.network = network.clone();
    snapshot.sandbox.network = network;
}

fn upsert_filesystem_grant(
    snapshot: &mut TurnExecutionSecuritySnapshot,
    grant: &FilesystemAccessGrant,
) {
    let root = normalize_path_lexically(grant.root.clone());
    let root_string = root.display().to_string();
    if let Some(existing) = snapshot
        .sandbox
        .filesystem
        .entries
        .iter_mut()
        .find(|entry| entry.resolved_path.as_deref() == Some(root_string.as_str()))
    {
        if access_rank(grant.access) > access_rank(existing.access) {
            existing.access = grant.access;
        }
        return;
    }

    snapshot
        .sandbox
        .filesystem
        .entries
        .push(TurnFilesystemSandboxEntry {
            path: TurnFilesystemSandboxPath::ExplicitPath {
                path: root_string.clone(),
            },
            access: grant.access,
            provenance: TurnSecurityRuleProvenance::Runtime,
            resolved_path: Some(root_string),
        });
}

fn merge_network_policy_grants(
    policy: &mut TurnNetworkPolicySnapshot,
    grants: &[NetworkAccessGrant],
) {
    if grants
        .iter()
        .any(|grant| matches!(grant, NetworkAccessGrant::Enabled))
    {
        *policy = TurnNetworkPolicySnapshot::enabled();
        return;
    }

    if policy.mode == TurnNetworkMode::Disabled {
        policy.mode = TurnNetworkMode::Restricted;
    }
    if policy.mode != TurnNetworkMode::Restricted {
        return;
    }

    for grant in grants {
        let NetworkAccessGrant::Host(host) = grant else {
            continue;
        };
        if is_localhost_network_host(host.as_str()) {
            policy.allow_localhost = true;
        } else if !policy
            .allowed_domains
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(host))
        {
            policy.allowed_domains.push(host.clone());
        }
    }
}

fn merge_filesystem_grants(
    existing: &mut Vec<FilesystemAccessGrant>,
    incoming: &[FilesystemAccessGrant],
) {
    for grant in incoming {
        let root = normalize_path_lexically(grant.root.clone());
        if let Some(existing_grant) = existing.iter_mut().find(|item| item.root == root) {
            if access_rank(grant.access) > access_rank(existing_grant.access) {
                existing_grant.access = grant.access;
            }
        } else {
            existing.push(FilesystemAccessGrant {
                root,
                access: grant.access,
            });
        }
    }
}

fn merge_network_grants(existing: &mut Vec<NetworkAccessGrant>, incoming: &[NetworkAccessGrant]) {
    if existing
        .iter()
        .any(|grant| matches!(grant, NetworkAccessGrant::Enabled))
    {
        return;
    }
    if incoming
        .iter()
        .any(|grant| matches!(grant, NetworkAccessGrant::Enabled))
    {
        existing.clear();
        existing.push(NetworkAccessGrant::Enabled);
        return;
    }
    for grant in incoming {
        let NetworkAccessGrant::Host(host) = grant else {
            continue;
        };
        if !existing.iter().any(|existing| match existing {
            NetworkAccessGrant::Host(existing_host) => existing_host.eq_ignore_ascii_case(host),
            NetworkAccessGrant::Enabled => true,
        }) {
            existing.push(NetworkAccessGrant::Host(host.clone()));
        }
    }
}

fn is_localhost_network_host(host: &str) -> bool {
    let normalized = host.trim().trim_matches(['[', ']']).to_ascii_lowercase();
    normalized == "localhost"
        || normalized.ends_with(".localhost")
        || normalized.parse::<std::net::IpAddr>().is_ok_and(|ip| {
            ip.is_loopback()
                || ip.is_unspecified()
                || match ip {
                    std::net::IpAddr::V4(ip) => ip.is_private() || ip.is_link_local(),
                    std::net::IpAddr::V6(ip) => {
                        ((ip.segments()[0] & 0xfe00) == 0xfc00)
                            || ((ip.segments()[0] & 0xffc0) == 0xfe80)
                    }
                }
        })
}

fn access_rank(access: TurnFilesystemAccess) -> u8 {
    match access {
        TurnFilesystemAccess::None => 0,
        TurnFilesystemAccess::Read => 1,
        TurnFilesystemAccess::Write => 2,
    }
}

fn writable_parent_root(path: &Path) -> PathBuf {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| path.to_path_buf())
}

fn dedupe_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for path in paths {
        let path = normalize_path_lexically(path);
        if seen.insert(path.clone()) {
            deduped.push(path);
        }
    }
    deduped
}

fn normalize_path_lexically(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(value) => normalized.push(value),
            Component::RootDir | Component::Prefix(_) => normalized.push(component.as_os_str()),
        }
    }
    normalized
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
    use std::path::{Path, PathBuf};
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

    struct NarrowingApprovalBroker {
        revalidations: Arc<AtomicUsize>,
        approvals: Arc<AtomicUsize>,
        permission_profile: pioneer_protocol::TurnPermissionProfileSnapshot,
    }

    struct StaticJsonHandler {
        calls: Arc<AtomicUsize>,
        payload: serde_json::Value,
    }

    struct SnapshotAssertHandler {
        calls: Arc<AtomicUsize>,
        required_path: PathBuf,
        operation: FilePolicyOperation,
    }

    struct NetworkSnapshotAssertHandler {
        calls: Arc<AtomicUsize>,
        required_url: Option<&'static str>,
        expect_enabled: bool,
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
    impl ToolHandler for SnapshotAssertHandler {
        async fn handle(
            &self,
            invocation: ToolInvocation,
            _trace: crate::events::ToolEventTrace,
        ) -> Result<Box<dyn crate::context::ToolOutput>, ToolError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let snapshot = invocation
                .execution_security_snapshot
                .as_ref()
                .expect("snapshot should be present");
            match FilePolicyChecker::check(snapshot, self.operation, self.required_path.as_path()) {
                FilePolicyDecision::Allowed(_) => {}
                FilePolicyDecision::Denied(deny) => {
                    panic!("expected filesystem grant to allow path: {deny:?}");
                }
            }
            Ok(Box::new(FunctionToolOutput::new("ok", true)))
        }
    }

    #[async_trait]
    impl ToolHandler for NetworkSnapshotAssertHandler {
        async fn handle(
            &self,
            invocation: ToolInvocation,
            _trace: crate::events::ToolEventTrace,
        ) -> Result<Box<dyn crate::context::ToolOutput>, ToolError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let snapshot = invocation
                .execution_security_snapshot
                .as_ref()
                .expect("snapshot should be present");
            if self.expect_enabled {
                assert_eq!(snapshot.network.mode, TurnNetworkMode::Enabled);
                assert_eq!(snapshot.sandbox.network.mode, TurnNetworkMode::Enabled);
            }
            if let Some(url) = self.required_url {
                match NetworkPolicyChecker::check_url(snapshot, url, "test") {
                    crate::network_policy::NetworkPolicyDecision::Allowed(_) => {}
                    crate::network_policy::NetworkPolicyDecision::Denied(deny) => {
                        panic!("expected network grant to allow url: {deny:?}");
                    }
                }
            }
            Ok(Box::new(FunctionToolOutput::new("ok", true)))
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
    impl PermissionApprovalBroker for NarrowingApprovalBroker {
        async fn revalidate_permission_context(
            &self,
            context: &PermissionEvaluationContext,
            _invocation: &ToolInvocation,
        ) -> Result<PermissionEvaluationContext, String> {
            self.revalidations.fetch_add(1, Ordering::SeqCst);
            let mut effective = context.clone();
            effective.permission_profile = self.permission_profile.clone();
            Ok(effective)
        }

        async fn request_approval(
            &self,
            _context: &PermissionEvaluationContext,
            _invocation: &ToolInvocation,
            _intent: &PermissionIntent,
            _key: &PermissionRequestKey,
            _reason: PermissionDecisionReason,
        ) -> PermissionApprovalResolution {
            self.approvals.fetch_add(1, Ordering::SeqCst);
            PermissionApprovalResolution::AllowOnce
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

    fn with_execution_security_snapshot(
        mut invocation: ToolInvocation,
        snapshot: TurnExecutionSecuritySnapshot,
    ) -> ToolInvocation {
        invocation.execution_security_snapshot = Some(snapshot);
        invocation
    }

    fn read_only_snapshot(root: &Path) -> TurnExecutionSecuritySnapshot {
        TurnExecutionSecuritySnapshot::read_only(
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            ),
            root.to_string_lossy(),
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Read,
                root.to_string_lossy(),
            )],
            1,
        )
    }

    fn temp_path(name: &str) -> PathBuf {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "pioneer-orchestrator-{name}-{}-{suffix}",
            std::process::id()
        ))
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
    async fn current_permission_projection_narrows_before_tool_side_effect() {
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let registry = registry_with_handler(Arc::new(CountingHandler {
            calls: handler_calls.clone(),
            first_error: None,
            success_text: "unexpected",
        }));
        let revalidations = Arc::new(AtomicUsize::new(0));
        let approvals = Arc::new(AtomicUsize::new(0));
        let orchestrator = ToolOrchestrator::with_approval_broker(
            OrchestratorPolicy::default(),
            Arc::new(NarrowingApprovalBroker {
                revalidations: revalidations.clone(),
                approvals: approvals.clone(),
                permission_profile: pioneer_protocol::TurnPermissionProfileSnapshot {
                    mode: pioneer_protocol::TurnPermissionMode::Supervised,
                    source: pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
                    effective_policy: pioneer_protocol::ToolPermissionPolicySnapshot::all(
                        pioneer_protocol::PermissionBehavior::Deny,
                    ),
                },
            }),
        );
        let context = default_test_permission_context();
        let trace = crate::events::ToolEventBus::default().start_trace("turn", "call_1", "tool");
        let error = orchestrator
            .run_with_context(&registry, invocation(), &trace, &context)
            .await
            .err()
            .expect("current role deny must precede the handler side effect");
        assert!(matches!(error, ToolError::Rejected(_)));
        assert_eq!(revalidations.load(Ordering::SeqCst), 1);
        assert_eq!(approvals.load(Ordering::SeqCst), 0);
        assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn file_read_approval_cannot_widen_immutable_filesystem_authority() {
        let base = temp_path("file-read-grant");
        let workspace = base.join("backend");
        let frontend = base.join("frontend");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should create");
        std::fs::create_dir_all(frontend.as_path()).expect("frontend should create");
        let file = frontend.join("package.json");
        std::fs::write(file.as_path(), "{}").expect("outside file should write");

        let handler_calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(SnapshotAssertHandler {
            calls: handler_calls.clone(),
            required_path: file.clone(),
            operation: FilePolicyOperation::Read,
        });
        let registry =
            registry_with_named_handlers([("read_file", handler as Arc<dyn ToolHandler>)]);
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
        let invocation = with_execution_security_snapshot(
            invocation_for_tool(
                "read_file",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "path": file.display().to_string() }),
                },
            ),
            read_only_snapshot(workspace.as_path()),
        );

        let trace =
            crate::events::ToolEventBus::default().start_trace("turn", "call_1", "read_file");
        let error = orchestrator
            .run_with_context(&registry, invocation, &trace, &context)
            .await
            .err()
            .expect("approval must not widen filesystem authority");

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("outside the immutable execution authority"))
        );
        assert_eq!(broker_calls.load(Ordering::SeqCst), 0);
        assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
        let _ = std::fs::remove_dir_all(base);
    }

    #[tokio::test]
    async fn file_write_approval_cannot_widen_immutable_filesystem_authority() {
        let base = temp_path("file-write-grant");
        let workspace = base.join("backend");
        let frontend = base.join("frontend");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should create");
        std::fs::create_dir_all(frontend.as_path()).expect("frontend should create");
        let file = frontend.join("package.json");

        let handler_calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(SnapshotAssertHandler {
            calls: handler_calls.clone(),
            required_path: file.clone(),
            operation: FilePolicyOperation::Write,
        });
        let registry =
            registry_with_named_handlers([("write_file", handler as Arc<dyn ToolHandler>)]);
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
        let invocation = with_execution_security_snapshot(
            invocation_for_tool(
                "write_file",
                ToolPayload::Function {
                    arguments: serde_json::json!({
                        "path": file.display().to_string(),
                        "content": "{}",
                    }),
                },
            ),
            read_only_snapshot(workspace.as_path()),
        );

        let trace =
            crate::events::ToolEventBus::default().start_trace("turn", "call_1", "write_file");
        let error = orchestrator
            .run_with_context(&registry, invocation, &trace, &context)
            .await
            .err()
            .expect("approval must not widen filesystem authority");

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("outside the immutable execution authority"))
        );
        assert_eq!(
            broker_calls.load(Ordering::SeqCst),
            1,
            "file write approval must still remain bounded by immutable filesystem authority"
        );
        assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
        let _ = std::fs::remove_dir_all(base);
    }

    #[tokio::test]
    async fn shell_approval_cannot_widen_immutable_cwd_authority() {
        let base = temp_path("shell-cwd-grant");
        let workspace = base.join("backend");
        let frontend = base.join("frontend");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should create");
        std::fs::create_dir_all(frontend.as_path()).expect("frontend should create");

        let handler_calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(SnapshotAssertHandler {
            calls: handler_calls.clone(),
            required_path: frontend.clone(),
            operation: FilePolicyOperation::Write,
        });
        let registry =
            registry_with_named_handlers([("exec_command", handler as Arc<dyn ToolHandler>)]);
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
        let mut invocation = invocation_for_tool(
            "exec_command",
            ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                command: Some(vec!["npm".to_owned(), "run".to_owned(), "build".to_owned()]),
                workdir: Some(frontend.display().to_string()),
                timeout_ms: None,
                max_output_tokens: None,
                yield_time_ms: None,
                tty: None,
            })),
        );
        invocation.workdir = workspace.clone();
        let invocation =
            with_execution_security_snapshot(invocation, read_only_snapshot(workspace.as_path()));

        let trace =
            crate::events::ToolEventBus::default().start_trace("turn", "call_1", "exec_command");
        let error = orchestrator
            .run_with_context(&registry, invocation, &trace, &context)
            .await
            .err()
            .expect("approval must not widen cwd authority");

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("outside the immutable execution authority"))
        );
        assert_eq!(
            broker_calls.load(Ordering::SeqCst),
            1,
            "shell action approval should also cover the cwd filesystem grant"
        );
        assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn shell_without_explicit_workdir_does_not_expand_filesystem_write_access() {
        let mut invocation = invocation_for_tool(
            "exec_command",
            ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                command: Some(vec!["pwd".to_owned()]),
                workdir: None,
                timeout_ms: None,
                max_output_tokens: None,
                yield_time_ms: None,
                tty: None,
            })),
        );
        invocation.workdir = PathBuf::from("/");
        let intent = extract_permission_intent(&invocation);

        assert!(filesystem_access_requirements(&invocation, &intent).is_empty());
    }

    #[tokio::test]
    async fn download_approval_cannot_widen_immutable_destination_authority() {
        let base = temp_path("download-destination-grant");
        let workspace = base.join("backend");
        let downloads = base.join("downloads");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should create");
        std::fs::create_dir_all(downloads.as_path()).expect("downloads should create");
        let destination = downloads.join("archive.tgz");

        let handler_calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(SnapshotAssertHandler {
            calls: handler_calls.clone(),
            required_path: destination.clone(),
            operation: FilePolicyOperation::Write,
        });
        let registry =
            registry_with_named_handlers([("download_url", handler as Arc<dyn ToolHandler>)]);
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
        let mut invocation = invocation_for_tool(
            "download_url",
            ToolPayload::Function {
                arguments: serde_json::json!({
                    "url": "https://example.com/archive.tgz",
                    "destination": destination.display().to_string(),
                }),
            },
        );
        invocation.workdir = workspace.clone();
        let invocation =
            with_execution_security_snapshot(invocation, read_only_snapshot(workspace.as_path()));

        let trace =
            crate::events::ToolEventBus::default().start_trace("turn", "call_1", "download_url");
        let error = orchestrator
            .run_with_context(&registry, invocation, &trace, &context)
            .await
            .err()
            .expect("approval must not widen destination authority");

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("outside the immutable execution authority"))
        );
        assert_eq!(
            broker_calls.load(Ordering::SeqCst),
            1,
            "network approval should also cover the destination filesystem grant"
        );
        assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
        let _ = std::fs::remove_dir_all(base);
    }

    #[tokio::test]
    async fn web_fetch_approval_cannot_widen_immutable_network_authority() {
        let workspace = temp_path("web-fetch-network-grant");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should create");

        let handler_calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(NetworkSnapshotAssertHandler {
            calls: handler_calls.clone(),
            required_url: Some("https://example.com/docs"),
            expect_enabled: false,
        });
        let registry =
            registry_with_named_handlers([("web_fetch", handler as Arc<dyn ToolHandler>)]);
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
        let mut invocation = invocation_for_tool(
            "web_fetch",
            ToolPayload::Function {
                arguments: serde_json::json!({ "url": "https://example.com/docs" }),
            },
        );
        invocation.workdir = workspace.clone();
        let invocation =
            with_execution_security_snapshot(invocation, read_only_snapshot(workspace.as_path()));

        let trace =
            crate::events::ToolEventBus::default().start_trace("turn", "call_1", "web_fetch");
        let error = orchestrator
            .run_with_context(&registry, invocation, &trace, &context)
            .await
            .err()
            .expect("approval must not widen network authority");

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("outside the immutable execution authority"))
        );
        assert_eq!(broker_calls.load(Ordering::SeqCst), 1);
        assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn web_search_approval_cannot_widen_immutable_network_authority() {
        let workspace = temp_path("web-search-network-grant");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should create");

        let handler_calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(NetworkSnapshotAssertHandler {
            calls: handler_calls.clone(),
            required_url: None,
            expect_enabled: true,
        });
        let registry =
            registry_with_named_handlers([("web_search", handler as Arc<dyn ToolHandler>)]);
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
        let mut invocation = invocation_for_tool(
            "web_search",
            ToolPayload::Function {
                arguments: serde_json::json!({ "query": "pioneer permissions" }),
            },
        );
        invocation.workdir = workspace.clone();
        let invocation =
            with_execution_security_snapshot(invocation, read_only_snapshot(workspace.as_path()));

        let trace =
            crate::events::ToolEventBus::default().start_trace("turn", "call_1", "web_search");
        let error = orchestrator
            .run_with_context(&registry, invocation, &trace, &context)
            .await
            .err()
            .expect("approval must not widen network authority");

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("outside the immutable execution authority"))
        );
        assert_eq!(broker_calls.load(Ordering::SeqCst), 1);
        assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn mcp_network_policy_denial_precedes_approval_and_does_not_widen_sandbox() {
        let workspace = temp_path("mcp-network-grant");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should create");

        let handler_calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(NetworkSnapshotAssertHandler {
            calls: handler_calls.clone(),
            required_url: None,
            expect_enabled: false,
        });
        let registry =
            registry_with_named_handlers([("mcp.example", handler as Arc<dyn ToolHandler>)]);
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
        let mut invocation = invocation_for_tool(
            "mcp.example",
            ToolPayload::Mcp {
                server: "example".to_owned(),
                tool: "send_email".to_owned(),
                arguments: serde_json::json!({}),
                read_only_hint: Some(false),
                destructive_hint: Some(false),
                open_world_hint: Some(true),
            },
        );
        invocation.workdir = workspace.clone();
        let invocation =
            with_execution_security_snapshot(invocation, read_only_snapshot(workspace.as_path()));

        let trace =
            crate::events::ToolEventBus::default().start_trace("turn", "call_1", "mcp.example");
        let result = orchestrator
            .run_with_context(&registry, invocation, &trace, &context)
            .await;
        let error = match result {
            Ok(_) => panic!("MCP approval must not widen the frozen network sandbox"),
            Err(error) => error,
        };

        assert!(
            matches!(error, ToolError::Rejected(ref message) if message.contains("network is disabled")),
            "unexpected error: {error}"
        );
        assert_eq!(broker_calls.load(Ordering::SeqCst), 0);
        assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn cached_turn_approval_cannot_widen_immutable_filesystem_authority() {
        let base = temp_path("file-read-grant-cache");
        let workspace = base.join("backend");
        let frontend = base.join("frontend");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should create");
        std::fs::create_dir_all(frontend.as_path()).expect("frontend should create");
        let file = frontend.join("package.json");
        std::fs::write(file.as_path(), "{}").expect("outside file should write");

        let handler_calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(SnapshotAssertHandler {
            calls: handler_calls.clone(),
            required_path: file.clone(),
            operation: FilePolicyOperation::Read,
        });
        let registry =
            registry_with_named_handlers([("read_file", handler as Arc<dyn ToolHandler>)]);
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

        for call_id in ["call_1", "call_2"] {
            let mut invocation = invocation_for_tool(
                "read_file",
                ToolPayload::Function {
                    arguments: serde_json::json!({ "path": file.display().to_string() }),
                },
            );
            invocation.call_id = call_id.to_owned();
            let invocation = with_execution_security_snapshot(
                invocation,
                read_only_snapshot(workspace.as_path()),
            );
            let trace =
                crate::events::ToolEventBus::default().start_trace("turn", call_id, "read_file");
            let error = orchestrator
                .run_with_context(&registry, invocation, &trace, &context)
                .await
                .err()
                .expect("cached approval must not widen filesystem authority");
            assert!(
                matches!(error, ToolError::Rejected(message) if message.contains("outside the immutable execution authority"))
            );
        }

        assert_eq!(broker_calls.load(Ordering::SeqCst), 0);
        assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
        let _ = std::fs::remove_dir_all(base);
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
