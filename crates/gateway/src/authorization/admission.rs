use std::sync::atomic::{AtomicU64, Ordering};

use pioneer_protocol::{
    AUTHENTICATION_TERMINAL_CODE, FORBIDDEN_CODE, INVALID_PARAMS_CODE, JsonRpcError,
    JsonRpcErrorResponse, NOT_FOUND_CODE, RequestId,
};
use serde_json::json;

use super::{
    AuthorizationDecision, BinaryAuthorizationEntry, DenyReason, DisclosurePolicy,
    MethodAuthorizationEntry, ResourceAction,
};

const AUTHORIZATION_UNAVAILABLE_CODE: i64 = -32603;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AuthorizationExternalError {
    NotFound,
    Forbidden,
    AuthenticationTerminal,
    Validation,
    Unavailable,
}

impl AuthorizationExternalError {
    const fn code(self) -> i64 {
        match self {
            Self::NotFound => NOT_FOUND_CODE,
            Self::Forbidden => FORBIDDEN_CODE,
            Self::AuthenticationTerminal => AUTHENTICATION_TERMINAL_CODE,
            Self::Validation => INVALID_PARAMS_CODE,
            Self::Unavailable => AUTHORIZATION_UNAVAILABLE_CODE,
        }
    }

    const fn safe_code(self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::Forbidden => "forbidden",
            Self::AuthenticationTerminal => "authentication_terminal",
            Self::Validation => "invalid_params",
            Self::Unavailable => "authorization_unavailable",
        }
    }

    pub(crate) fn response(self, request_id: RequestId) -> JsonRpcErrorResponse {
        let safe_code = self.safe_code();
        JsonRpcErrorResponse {
            jsonrpc: pioneer_protocol::JSONRPC_VERSION.to_owned(),
            id: Some(request_id),
            error: JsonRpcError {
                code: self.code(),
                message: safe_code.to_owned(),
                data: Some(json!({ "code": safe_code })),
            },
        }
    }
}

pub(crate) fn external_error_for_decision(
    decision: &AuthorizationDecision,
) -> Option<AuthorizationExternalError> {
    let AuthorizationDecision::Deny { disclosure, .. } = decision else {
        return None;
    };
    Some(match disclosure {
        DisclosurePolicy::NotFound => AuthorizationExternalError::NotFound,
        DisclosurePolicy::Forbidden => AuthorizationExternalError::Forbidden,
        DisclosurePolicy::AuthenticationTerminal => {
            AuthorizationExternalError::AuthenticationTerminal
        }
        DisclosurePolicy::Validation => AuthorizationExternalError::Validation,
    })
}

#[derive(Default)]
struct AuthorizationCounters {
    allowed: AtomicU64,
    denied: AtomicU64,
    inaccessible: AtomicU64,
    forbidden: AtomicU64,
    authentication_terminal: AtomicU64,
    stale: AtomicU64,
    capability_filtered: AtomicU64,
    subscription_evicted: AtomicU64,
    private_source_rejected: AtomicU64,
    unavailable: AtomicU64,
}

static AUTHORIZATION_COUNTERS: AuthorizationCounters = AuthorizationCounters {
    allowed: AtomicU64::new(0),
    denied: AtomicU64::new(0),
    inaccessible: AtomicU64::new(0),
    forbidden: AtomicU64::new(0),
    authentication_terminal: AtomicU64::new(0),
    stale: AtomicU64::new(0),
    capability_filtered: AtomicU64::new(0),
    subscription_evicted: AtomicU64::new(0),
    private_source_rejected: AtomicU64::new(0),
    unavailable: AtomicU64::new(0),
};

trait AuthorizationAuditEntry {
    fn action_name(&self) -> &'static str;
    fn resource_kind_name(&self) -> &'static str;
    fn audit_class_name(&self) -> &'static str;
}

impl AuthorizationAuditEntry for MethodAuthorizationEntry {
    fn action_name(&self) -> &'static str {
        self.action.safe_name()
    }

    fn resource_kind_name(&self) -> &'static str {
        self.resolver.safe_name()
    }

    fn audit_class_name(&self) -> &'static str {
        self.audit.safe_name()
    }
}

impl AuthorizationAuditEntry for BinaryAuthorizationEntry {
    fn action_name(&self) -> &'static str {
        self.action.safe_name()
    }

    fn resource_kind_name(&self) -> &'static str {
        self.resolver.safe_name()
    }

    fn audit_class_name(&self) -> &'static str {
        self.audit.safe_name()
    }
}

pub(crate) fn record_method_decision(
    entry: &MethodAuthorizationEntry,
    decision: &AuthorizationDecision,
) {
    record_decision(entry, decision);
}

pub(crate) fn record_method_decision_for_action(
    entry: &MethodAuthorizationEntry,
    action: ResourceAction,
    decision: &AuthorizationDecision,
) {
    record_decision_fields(
        action.safe_name(),
        entry.resolver.safe_name(),
        entry.audit.safe_name(),
        decision,
    );
}

pub(crate) fn record_binary_decision(
    entry: &BinaryAuthorizationEntry,
    decision: &AuthorizationDecision,
) {
    record_decision(entry, decision);
}

pub(crate) fn record_workspace_notification_decision(
    action: ResourceAction,
    decision: &AuthorizationDecision,
) {
    record_decision_fields(action.safe_name(), "workspace", "notification", decision);
}

pub(crate) fn record_thread_notification_decision(
    action: ResourceAction,
    decision: &AuthorizationDecision,
) {
    record_decision_fields(action.safe_name(), "thread", "notification", decision);
}

pub(crate) fn record_task_notification_decision(
    action: ResourceAction,
    decision: &AuthorizationDecision,
) {
    record_decision_fields(action.safe_name(), "task", "notification", decision);
}

pub(crate) fn record_task_tool_decision(action: ResourceAction, decision: &AuthorizationDecision) {
    record_tool_decision(action, "task", decision);
}

pub(crate) fn record_tool_decision(
    action: ResourceAction,
    resource_kind: &'static str,
    decision: &AuthorizationDecision,
) {
    record_decision_fields(action.safe_name(), resource_kind, "tool", decision);
}

fn record_decision(entry: &impl AuthorizationAuditEntry, decision: &AuthorizationDecision) {
    record_decision_fields(
        entry.action_name(),
        entry.resource_kind_name(),
        entry.audit_class_name(),
        decision,
    );
}

fn record_decision_fields(
    action_name: &'static str,
    resource_kind_name: &'static str,
    audit_class_name: &'static str,
    decision: &AuthorizationDecision,
) {
    let (decision_name, reason_name) = match decision {
        AuthorizationDecision::AllowSuperuser => ("allow", "absolute_superuser"),
        AuthorizationDecision::AllowPolicy { reason, .. } => ("allow", reason.safe_name()),
        AuthorizationDecision::Deny { reason, disclosure } => {
            AUTHORIZATION_COUNTERS
                .denied
                .fetch_add(1, Ordering::Relaxed);
            match disclosure {
                DisclosurePolicy::NotFound => {
                    AUTHORIZATION_COUNTERS
                        .inaccessible
                        .fetch_add(1, Ordering::Relaxed);
                }
                DisclosurePolicy::Forbidden => {
                    AUTHORIZATION_COUNTERS
                        .forbidden
                        .fetch_add(1, Ordering::Relaxed);
                }
                DisclosurePolicy::AuthenticationTerminal => {
                    AUTHORIZATION_COUNTERS
                        .authentication_terminal
                        .fetch_add(1, Ordering::Relaxed);
                }
                DisclosurePolicy::Validation => {}
            }
            if *reason == DenyReason::StaleAuthorizationRevision {
                AUTHORIZATION_COUNTERS.stale.fetch_add(1, Ordering::Relaxed);
            }
            if *reason == DenyReason::CapabilityDisabled {
                AUTHORIZATION_COUNTERS
                    .capability_filtered
                    .fetch_add(1, Ordering::Relaxed);
            }
            ("deny", reason.safe_name())
        }
    };
    if decision.is_allowed() {
        AUTHORIZATION_COUNTERS
            .allowed
            .fetch_add(1, Ordering::Relaxed);
    }

    tracing::debug!(
        authorization_action = action_name,
        authorization_resource_kind = resource_kind_name,
        authorization_decision = decision_name,
        authorization_reason = reason_name,
        authorization_audit_class = audit_class_name,
        "authorization audit decision"
    );
}

pub(crate) fn record_authorization_unavailable(
    action_name: &'static str,
    resource_kind_name: &'static str,
    audit_class_name: &'static str,
) {
    AUTHORIZATION_COUNTERS
        .unavailable
        .fetch_add(1, Ordering::Relaxed);
    tracing::warn!(
        authorization_action = action_name,
        authorization_resource_kind = resource_kind_name,
        authorization_decision = "deny",
        authorization_reason = "authorization_unavailable",
        authorization_audit_class = audit_class_name,
        "authorization audit decision"
    );
}

pub(crate) fn record_subscription_evictions(count: usize) {
    if count == 0 {
        return;
    }
    AUTHORIZATION_COUNTERS
        .subscription_evicted
        .fetch_add(u64::try_from(count).unwrap_or(u64::MAX), Ordering::Relaxed);
    tracing::debug!(
        authorization_action = "subscription_evict",
        authorization_resource_kind = "thread_subscription",
        authorization_decision = "evict",
        authorization_reason = "access_revoked",
        authorization_audit_class = "lifecycle",
        authorization_eviction_count = count,
        "authorization lifecycle metric"
    );
}

pub(crate) fn record_stale_policy_revision() {
    AUTHORIZATION_COUNTERS.stale.fetch_add(1, Ordering::Relaxed);
    tracing::debug!(
        authorization_action = "execution_revalidate",
        authorization_resource_kind = "execution_authorization_context",
        authorization_decision = "revalidate",
        authorization_reason = "stale_authorization_revision",
        authorization_audit_class = "lifecycle",
        "authorization lifecycle metric"
    );
}

pub(crate) fn record_private_self_improvement_source_rejection() {
    AUTHORIZATION_COUNTERS
        .private_source_rejected
        .fetch_add(1, Ordering::Relaxed);
    tracing::debug!(
        authorization_action = "self_improvement_overlay_project",
        authorization_resource_kind = "learned_skill_source",
        authorization_decision = "filter",
        authorization_reason = "source_not_workspace_visible",
        authorization_audit_class = "derived_data",
        "authorization lifecycle metric"
    );
}

#[cfg(test)]
pub(crate) fn authorization_counter_snapshot() -> [u64; 10] {
    [
        AUTHORIZATION_COUNTERS.allowed.load(Ordering::Relaxed),
        AUTHORIZATION_COUNTERS.denied.load(Ordering::Relaxed),
        AUTHORIZATION_COUNTERS.inaccessible.load(Ordering::Relaxed),
        AUTHORIZATION_COUNTERS.forbidden.load(Ordering::Relaxed),
        AUTHORIZATION_COUNTERS
            .authentication_terminal
            .load(Ordering::Relaxed),
        AUTHORIZATION_COUNTERS.stale.load(Ordering::Relaxed),
        AUTHORIZATION_COUNTERS
            .capability_filtered
            .load(Ordering::Relaxed),
        AUTHORIZATION_COUNTERS
            .subscription_evicted
            .load(Ordering::Relaxed),
        AUTHORIZATION_COUNTERS
            .private_source_rejected
            .load(Ordering::Relaxed),
        AUTHORIZATION_COUNTERS.unavailable.load(Ordering::Relaxed),
    ]
}

#[cfg(test)]
mod tests {
    use pioneer_protocol::{PrincipalKind, RoleKey};

    use super::*;
    use crate::authorization::{AuthorizationService, ResourceAction, normal_method_entry};

    #[test]
    fn external_mapping_never_exposes_internal_deny_reason() {
        let decision = AuthorizationDecision::Deny {
            reason: DenyReason::NoPrivateThreadMembership,
            disclosure: DisclosurePolicy::NotFound,
        };
        let response = external_error_for_decision(&decision)
            .expect("denial mapping")
            .response(RequestId::new("R".repeat(21)).expect("request id"));
        let serialized = serde_json::to_string(&response).expect("serialize");

        assert!(serialized.contains("not_found"));
        assert!(!serialized.contains("no_private_thread_membership"));
    }

    #[test]
    fn canonical_audit_counter_records_bounded_decisions() {
        let entry = normal_method_entry(pioneer_protocol::constants::methods::WORKSPACE_LIST)
            .expect("registered method");
        let service = AuthorizationService::new();
        let gate = service.authorize_action(
            PrincipalKind::User,
            Some(&RoleKey::member()),
            ResourceAction::WorkspaceList,
        );
        let decision = service.authorize_resource(
            &gate,
            ResourceAction::WorkspaceList,
            crate::authorization::ResolvedResourceAccess::WorkspaceCollection,
        );
        let before = authorization_counter_snapshot();
        record_method_decision(entry, &decision);
        let after = authorization_counter_snapshot();

        assert_eq!(after[0], before[0] + 1);
        assert_eq!(after[1], before[1]);
    }

    #[test]
    fn lifecycle_counters_cover_subscription_and_private_source_rejections() {
        let before = authorization_counter_snapshot();
        record_subscription_evictions(2);
        record_stale_policy_revision();
        record_private_self_improvement_source_rejection();
        let after = authorization_counter_snapshot();

        assert_eq!(after[5], before[5] + 1);
        assert_eq!(after[7], before[7] + 2);
        assert_eq!(after[8], before[8] + 1);
    }
}
