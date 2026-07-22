use crate::{
    ItemCompletedNotification, ItemDeltaNotification, ItemDeltaStream, ItemStartedNotification,
    ItemToolRetryExhaustedNotification, ItemToolRetryResolvedNotification,
    ItemToolRetryScheduledNotification, McpTurnBindingSummary, ProviderFailureDetails, SkillId,
    TaskEvent, TaskEventPayload, ThreadLineage, ToolOutputPolicySnapshot, TurnCapabilityKind,
    TurnExecutionWindowBlockedNotification, TurnExecutionWindowCheckpointedNotification,
    TurnExecutionWindowContinuedNotification, TurnExecutionWindowExhaustedNotification,
    TurnExecutionWindowStartedNotification, TurnItemType, TurnPermissionActionKind,
    TurnPermissionDecisionReason, TurnPermissionMode, TurnPermissionProfileSource,
    TurnToolLoopBudgetExceededNotification,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// High-level delivery class for protocol events that are relevant to agent
/// execution.
///
/// `InternalTelemetry` is not a bus lane and must not become a JSON-RPC event.
/// Internal runtime details belong in structured logs, for example
/// `tracing::debug!(target: "pioneer.tools.pipeline", ...)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProtocolEventClass {
    Durable,
    Progress,
    InternalTelemetry,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentDurableEvent {
    PromptManifestCompiled {
        thread_id: String,
        turn_id: String,
        manifest: crate::PromptManifest,
    },
    TurnSkillsResolved {
        thread_id: String,
        turn_id: String,
        bindings: Vec<TurnSkillBinding>,
    },
    TurnCapabilitiesResolved {
        thread_id: String,
        turn_id: String,
        accepted: Vec<TurnAcceptedCapability>,
        rejected: Vec<TurnRejectedCapability>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        mcp_bindings: Vec<McpTurnBindingSummary>,
    },
    SkillAuditEvents {
        thread_id: String,
        turn_id: String,
        events: Vec<SkillAuditEvent>,
    },
    TurnPermissionAudit {
        event: TurnPermissionAuditEvent,
    },
    TurnLlmContextAppended {
        thread_id: String,
        turn_id: String,
        item_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attempt_id: Option<String>,
        sequence: i64,
        source: String,
        tool_name: String,
        payload: ToolResultView,
        output_policy_snapshot: ToolOutputPolicySnapshot,
    },
    ItemStarted {
        notification: ItemStartedNotification,
    },
    ItemCompleted {
        notification: ItemCompletedNotification,
    },
    ItemToolRetryScheduled {
        notification: ItemToolRetryScheduledNotification,
    },
    ItemToolRetryResolved {
        notification: ItemToolRetryResolvedNotification,
    },
    ItemToolRetryExhausted {
        notification: ItemToolRetryExhaustedNotification,
    },
    TurnToolLoopBudgetExceeded {
        notification: TurnToolLoopBudgetExceededNotification,
    },
    TurnExecutionWindowStarted {
        notification: TurnExecutionWindowStartedNotification,
    },
    TurnExecutionWindowExhausted {
        notification: TurnExecutionWindowExhaustedNotification,
    },
    TurnExecutionWindowCheckpointed {
        notification: TurnExecutionWindowCheckpointedNotification,
        payload: crate::ExecutionCheckpointPayload,
    },
    TurnExecutionWindowContinued {
        notification: TurnExecutionWindowContinuedNotification,
    },
    TurnExecutionWindowBlocked {
        notification: TurnExecutionWindowBlockedNotification,
    },
    ProviderFailureDetected {
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        failure: ProviderFailureDetails,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<RecoveryAttemptContext>,
    },
    RecoveryAttemptSucceeded {
        thread_id: String,
        turn_id: String,
        recovery: RecoveryAttemptContext,
    },
    TurnCompleted {
        thread_id: String,
        turn_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<RecoveryAttemptContext>,
    },
    TurnFailed {
        thread_id: String,
        turn_id: String,
        error: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<RecoveryAttemptContext>,
    },
    TurnBlocked {
        thread_id: String,
        turn_id: String,
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<RecoveryAttemptContext>,
    },
    TurnInterrupted {
        thread_id: String,
        turn_id: String,
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<RecoveryAttemptContext>,
    },
    TaskEvent {
        event: TaskEvent,
    },
    ThreadLineageCreated {
        lineage: ThreadLineage,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum DurableEventCausalityKey {
    Turn { turn_id: String },
    Task { task_id: String },
    TaskRun { task_run_id: String },
    ThreadLineage { child_thread_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TurnSkillBinding {
    pub skill_id: SkillId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_owner: Option<String>,
    pub skill_slug: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_version: Option<String>,
    pub fingerprint: String,
    pub source_kind: String,
    pub resolved_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TurnAcceptedCapability {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub kind: TurnCapabilityKind,
    pub reason: TurnCapabilityAcceptedReason,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TurnCapabilityAcceptedReason {
    ExplicitComposerCapability,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TurnRejectedCapability {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub kind: TurnCapabilityKind,
    pub reason: TurnCapabilityRejectedReason,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TurnCapabilityRejectedReason {
    InvalidInput,
    Duplicate,
    NotFound,
    DisabledByPolicy,
    ValidationRejected,
    SecurityBlocked,
    DependencyMissing,
    Unavailable,
    CatalogMissing,
    ToolMissing,
    ProviderUnsupported,
    MaterializationFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SkillAuditEvent {
    pub skill_id: SkillId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_owner: Option<String>,
    pub skill_slug: String,
    pub source_kind: String,
    pub action: String,
    pub decision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
    #[serde(default)]
    pub details: JsonValue,
    pub created_at_unix: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurnPermissionAuditEvent {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub event_kind: TurnPermissionAuditEventKind,
    pub profile_mode: TurnPermissionMode,
    pub profile_source: TurnPermissionProfileSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_snapshot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_snapshot_version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_reason_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_capability: Option<crate::TurnSecurityCapabilityKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_kind: Option<TurnPermissionActionKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_key: Option<TurnPermissionAuditRequestKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision: Option<TurnPermissionAuditDecision>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<TurnPermissionDecisionReason>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub cached: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TurnPermissionAuditEventKind {
    ProfileSelected,
    SecuritySnapshotResolved,
    SecuritySandboxDegraded,
    SecuritySandboxUnavailable,
    ApprovalRequested,
    ApprovalResolved,
    DecisionAllowed,
    DecisionDenied,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TurnPermissionAuditDecision {
    Allow,
    Ask,
    Deny,
    AllowOnce,
    AllowForTurn,
    Cancelled,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurnPermissionAuditRequestKey {
    pub action_kind: TurnPermissionActionKind,
    pub scope_hash: String,
}

const fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct RecoveryAttemptContext {
    pub job_id: String,
    pub attempt_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ToolResultView {
    Text { text: String, truncated: bool },
    Json { value: JsonValue, truncated: bool },
    Empty,
}

impl AgentDurableEvent {
    pub fn causality_key(&self) -> DurableEventCausalityKey {
        match self {
            Self::PromptManifestCompiled { turn_id, .. }
            | Self::TurnSkillsResolved { turn_id, .. }
            | Self::TurnCapabilitiesResolved { turn_id, .. }
            | Self::SkillAuditEvents { turn_id, .. }
            | Self::TurnLlmContextAppended { turn_id, .. }
            | Self::ProviderFailureDetected { turn_id, .. }
            | Self::RecoveryAttemptSucceeded { turn_id, .. }
            | Self::TurnCompleted { turn_id, .. }
            | Self::TurnFailed { turn_id, .. }
            | Self::TurnBlocked { turn_id, .. }
            | Self::TurnInterrupted { turn_id, .. } => DurableEventCausalityKey::Turn {
                turn_id: turn_id.clone(),
            },
            Self::TurnPermissionAudit { event } => DurableEventCausalityKey::Turn {
                turn_id: event.turn_id.clone(),
            },
            Self::ItemStarted { notification } => DurableEventCausalityKey::Turn {
                turn_id: notification.turn_id.clone(),
            },
            Self::ItemCompleted { notification } => DurableEventCausalityKey::Turn {
                turn_id: notification.turn_id.clone(),
            },
            Self::ItemToolRetryScheduled { notification } => DurableEventCausalityKey::Turn {
                turn_id: notification.turn_id.clone(),
            },
            Self::ItemToolRetryResolved { notification } => DurableEventCausalityKey::Turn {
                turn_id: notification.turn_id.clone(),
            },
            Self::ItemToolRetryExhausted { notification } => DurableEventCausalityKey::Turn {
                turn_id: notification.turn_id.clone(),
            },
            Self::TurnToolLoopBudgetExceeded { notification } => DurableEventCausalityKey::Turn {
                turn_id: notification.turn_id.clone(),
            },
            Self::TurnExecutionWindowStarted { notification } => DurableEventCausalityKey::Turn {
                turn_id: notification.turn_id.clone(),
            },
            Self::TurnExecutionWindowExhausted { notification } => DurableEventCausalityKey::Turn {
                turn_id: notification.turn_id.clone(),
            },
            Self::TurnExecutionWindowCheckpointed { notification, .. } => {
                DurableEventCausalityKey::Turn {
                    turn_id: notification.turn_id.clone(),
                }
            }
            Self::TurnExecutionWindowContinued { notification } => DurableEventCausalityKey::Turn {
                turn_id: notification.turn_id.clone(),
            },
            Self::TurnExecutionWindowBlocked { notification } => DurableEventCausalityKey::Turn {
                turn_id: notification.turn_id.clone(),
            },
            Self::TaskEvent { event } => event.causality_key(),
            Self::ThreadLineageCreated { lineage } => DurableEventCausalityKey::ThreadLineage {
                child_thread_id: lineage.child_thread_id.clone(),
            },
        }
    }

    pub fn is_terminal(&self) -> bool {
        match self {
            Self::TurnCompleted { .. }
            | Self::TurnFailed { .. }
            | Self::TurnBlocked { .. }
            | Self::TurnInterrupted { .. }
            | Self::ItemCompleted { .. }
            | Self::ItemToolRetryExhausted { .. } => true,
            Self::TaskEvent { event } => event.is_terminal(),
            Self::PromptManifestCompiled { .. }
            | Self::TurnSkillsResolved { .. }
            | Self::TurnCapabilitiesResolved { .. }
            | Self::SkillAuditEvents { .. }
            | Self::TurnPermissionAudit { .. }
            | Self::TurnLlmContextAppended { .. }
            | Self::ItemStarted { .. }
            | Self::ItemToolRetryScheduled { .. }
            | Self::ItemToolRetryResolved { .. }
            | Self::TurnToolLoopBudgetExceeded { .. }
            | Self::TurnExecutionWindowStarted { .. }
            | Self::TurnExecutionWindowExhausted { .. }
            | Self::TurnExecutionWindowCheckpointed { .. }
            | Self::TurnExecutionWindowContinued { .. }
            | Self::TurnExecutionWindowBlocked { .. }
            | Self::ProviderFailureDetected { .. }
            | Self::RecoveryAttemptSucceeded { .. }
            | Self::ThreadLineageCreated { .. } => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentProgressEvent {
    ItemDelta {
        notification: ItemDeltaNotification,
    },
    ItemHeartbeat {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
    },
    ToolOutputDelta {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        stream: ItemDeltaStream,
        delta: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<JsonValue>,
    },
    TaskProgress {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        task_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run_id: Option<String>,
        summary: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash)]
pub struct ProgressCoalescingKey {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub stream: ItemDeltaStream,
}

impl AgentProgressEvent {
    pub fn coalescing_key(&self) -> ProgressCoalescingKey {
        match self {
            Self::ItemDelta { notification } => ProgressCoalescingKey {
                workspace_id: notification.workspace_id.clone(),
                thread_id: notification.thread_id.clone(),
                turn_id: notification.turn_id.clone(),
                item_id: notification.item_id.clone(),
                stream: notification.stream.unwrap_or(ItemDeltaStream::Generic),
            },
            Self::ItemHeartbeat {
                workspace_id,
                thread_id,
                turn_id,
                item_id,
                ..
            } => ProgressCoalescingKey {
                workspace_id: workspace_id.clone(),
                thread_id: thread_id.clone(),
                turn_id: turn_id.clone(),
                item_id: item_id.clone(),
                stream: ItemDeltaStream::Generic,
            },
            Self::ToolOutputDelta {
                workspace_id,
                thread_id,
                turn_id,
                item_id,
                stream,
                ..
            } => ProgressCoalescingKey {
                workspace_id: workspace_id.clone(),
                thread_id: thread_id.clone(),
                turn_id: turn_id.clone(),
                item_id: item_id.clone(),
                stream: *stream,
            },
            Self::TaskProgress {
                workspace_id,
                thread_id,
                turn_id,
                item_id,
                ..
            } => ProgressCoalescingKey {
                workspace_id: workspace_id.clone(),
                thread_id: thread_id.clone(),
                turn_id: turn_id.clone(),
                item_id: item_id.clone(),
                stream: ItemDeltaStream::ToolProgress,
            },
        }
    }
}

impl TaskEvent {
    pub fn event_class(&self) -> ProtocolEventClass {
        self.payload.event_class()
    }

    pub fn causality_key(&self) -> DurableEventCausalityKey {
        self.payload
            .run_id()
            .map(|task_run_id| DurableEventCausalityKey::TaskRun {
                task_run_id: task_run_id.to_owned(),
            })
            .unwrap_or_else(|| DurableEventCausalityKey::Task {
                task_id: self.payload.task_id().to_owned(),
            })
    }

    pub fn is_terminal(&self) -> bool {
        self.payload.is_terminal()
    }
}

impl TaskEventPayload {
    pub fn event_class(&self) -> ProtocolEventClass {
        match self {
            Self::Progress { .. } => ProtocolEventClass::Progress,
            Self::TaskCreated { .. }
            | Self::TriggerCreated { .. }
            | Self::DependencyCreated { .. }
            | Self::AgentSpecCreated { .. }
            | Self::TaskScheduled { .. }
            | Self::TaskQueued { .. }
            | Self::RunCreated { .. }
            | Self::RunStarted { .. }
            | Self::RunCompleted { .. }
            | Self::RunFailed { .. }
            | Self::RunBlocked { .. }
            | Self::RunRetryScheduled { .. }
            | Self::RunRetryExhausted { .. }
            | Self::RunCancelled { .. }
            | Self::TaskCompleted { .. }
            | Self::TaskFailed { .. }
            | Self::TaskBlocked { .. }
            | Self::TaskCancelled { .. }
            | Self::TaskDetached { .. }
            | Self::TaskUpdated { .. }
            | Self::TaskRescheduled { .. }
            | Self::TaskPaused { .. }
            | Self::TaskResumed { .. }
            | Self::TaskRecovered { .. }
            | Self::ChildThreadLinked { .. }
            | Self::TaskThreadLineageCreated { .. }
            | Self::TaskRunThreadBindingCreated { .. }
            | Self::TaskRunTurnStarted { .. }
            | Self::TaskRunTurnCompleted { .. }
            | Self::TaskRunTurnFailed { .. }
            | Self::TaskRunTurnBlocked { .. }
            | Self::TaskResultCandidateCreated { .. }
            | Self::TaskResultReviewEventRecorded { .. }
            | Self::TaskResultCandidateAccepted { .. }
            | Self::TaskResultCandidateRejected { .. }
            | Self::TaskResultCandidateCancelled { .. }
            | Self::TaskRevisionRequested { .. }
            | Self::TaskRunEnteredReview { .. }
            | Self::DepthLimitExceeded { .. }
            | Self::DeliveryQueued { .. }
            | Self::DeliveryStarted { .. }
            | Self::DeliveryDelivered { .. }
            | Self::DeliveryFailed { .. }
            | Self::DeliveryCancelled { .. }
            | Self::WriteLockAcquired { .. }
            | Self::WriteLockExtended { .. }
            | Self::WriteLockReleased { .. }
            | Self::WriteLockBlocked { .. }
            | Self::WriteLockExpired { .. } => ProtocolEventClass::Durable,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::RunCompleted { .. }
                | Self::RunFailed { .. }
                | Self::RunBlocked { .. }
                | Self::RunRetryExhausted { .. }
                | Self::RunCancelled { .. }
                | Self::TaskCompleted { .. }
                | Self::TaskFailed { .. }
                | Self::TaskBlocked { .. }
                | Self::TaskCancelled { .. }
                | Self::DepthLimitExceeded { .. }
                | Self::DeliveryDelivered { .. }
                | Self::DeliveryFailed { .. }
                | Self::DeliveryCancelled { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TaskErrorClass, TurnItem};

    fn task_event(payload: TaskEventPayload) -> TaskEvent {
        TaskEvent {
            id: "event_1".to_owned(),
            task_id: payload.task_id().to_owned(),
            run_id: payload.run_id().map(str::to_owned),
            thread_id: None,
            turn_id: None,
            sequence: 1,
            event_type: payload.event_type().to_owned(),
            idempotency_key: payload.idempotency_key(),
            payload,
            created_at: 10,
        }
    }

    #[test]
    fn durable_turn_item_causality_key_uses_turn_id() {
        let event = AgentDurableEvent::ItemStarted {
            notification: ItemStartedNotification {
                workspace_id: "ws_1".to_owned(),
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
                item: TurnItem::Reasoning {
                    id: "item_1".to_owned(),
                    summary: Vec::new(),
                    content: Vec::new(),
                },
            },
        };

        assert_eq!(
            event.causality_key(),
            DurableEventCausalityKey::Turn {
                turn_id: "turn_1".to_owned()
            }
        );
        assert!(!event.is_terminal());
    }

    #[test]
    fn durable_turn_interrupted_is_terminal_turn_event() {
        let event = AgentDurableEvent::TurnInterrupted {
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            reason: "user clicked stop".to_owned(),
            recovery: None,
        };

        assert_eq!(
            event.causality_key(),
            DurableEventCausalityKey::Turn {
                turn_id: "turn_1".to_owned()
            }
        );
        assert!(event.is_terminal());
    }

    #[test]
    fn security_audit_event_carries_snapshot_reference_and_reason_code() {
        let event = TurnPermissionAuditEvent {
            workspace_id: "ws_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            event_kind: TurnPermissionAuditEventKind::SecuritySandboxDegraded,
            profile_mode: TurnPermissionMode::AutoAcceptEdits,
            profile_source: TurnPermissionProfileSource::Composer,
            security_snapshot_id: Some("turn_1:security:v1".to_owned()),
            security_snapshot_version: Some(1),
            security_reason_code: Some("sandbox_degraded".to_owned()),
            security_capability: Some(crate::TurnSecurityCapabilityKind::Filesystem),
            item_id: None,
            tool_call_id: None,
            tool_name: None,
            action_kind: None,
            request_key: None,
            decision: None,
            reason: None,
            cached: false,
        };

        let encoded = serde_json::to_value(&event).expect("audit event should encode");
        assert_eq!(encoded["eventKind"], "security_sandbox_degraded");
        assert_eq!(encoded["securitySnapshotId"], "turn_1:security:v1");
        assert_eq!(encoded["securitySnapshotVersion"], 1);
        assert_eq!(encoded["securityReasonCode"], "sandbox_degraded");
        assert_eq!(encoded["securityCapability"], "filesystem");
        assert!(encoded.get("details").is_none());

        let decoded: TurnPermissionAuditEvent =
            serde_json::from_value(encoded).expect("audit event should decode");
        assert_eq!(decoded, event);
    }

    #[test]
    fn skill_binding_and_audit_use_exact_id_with_presentation_snapshots() {
        let binding: TurnSkillBinding = serde_json::from_value(serde_json::json!({
            "skill_id": "mvg02zVNGWuw5z5C4nYDo",
            "skill_owner": "pioneer",
            "skill_slug": "browser",
            "skill_version": "1.0.0",
            "fingerprint": "sha256:revision",
            "source_kind": "system",
            "resolved_reason": "explicit"
        }))
        .expect("binding should decode");
        assert_eq!(binding.skill_id.as_ref(), "mvg02zVNGWuw5z5C4nYDo");
        assert_eq!(binding.skill_owner.as_deref(), Some("pioneer"));
        assert_eq!(binding.skill_slug, "browser");

        let audit: SkillAuditEvent = serde_json::from_value(serde_json::json!({
            "skill_id": "mvg02zVNGWuw5z5C4nYDo",
            "skill_slug": "browser",
            "source_kind": "system",
            "action": "resolve",
            "decision": "accepted",
            "created_at_unix": 10
        }))
        .expect("audit should decode without an owner snapshot");
        assert_eq!(audit.skill_id, binding.skill_id);
        assert_eq!(audit.skill_owner, None);
    }

    #[test]
    fn skill_binding_and_audit_require_exact_id() {
        let binding_without_id = serde_json::from_value::<TurnSkillBinding>(serde_json::json!({
            "skill_owner": "pioneer",
            "skill_slug": "browser",
            "fingerprint": "sha256:revision",
            "source_kind": "system",
            "resolved_reason": "explicit"
        }));
        assert!(binding_without_id.is_err());

        let audit_without_id = serde_json::from_value::<SkillAuditEvent>(serde_json::json!({
            "skill_owner": "pioneer",
            "skill_slug": "browser",
            "source_kind": "system",
            "action": "resolve",
            "decision": "accepted",
            "created_at_unix": 10
        }));
        assert!(audit_without_id.is_err());

        let binding_schema = serde_json::to_value(schemars::schema_for!(TurnSkillBinding))
            .expect("binding schema should encode")
            .to_string();
        assert!(binding_schema.contains("skill_id"));
        assert!(binding_schema.contains("skill_owner"));
        assert!(binding_schema.contains("^[A-Za-z0-9]{21}$"));
        assert!(!binding_schema.contains("pack_id"));
        assert!(!binding_schema.contains("packId"));
    }

    #[test]
    fn durable_task_run_causality_key_uses_run_id_when_available() {
        let event = AgentDurableEvent::TaskEvent {
            event: task_event(TaskEventPayload::RunFailed {
                task_id: "task_1".to_owned(),
                run_id: "run_1".to_owned(),
                error: Some(crate::TaskError {
                    code: "failed".to_owned(),
                    message: "failed".to_owned(),
                    class: TaskErrorClass::Internal,
                    details: None,
                    failed_run_id: Some("run_1".to_owned()),
                }),
                completed_at: 20,
            }),
        };

        assert_eq!(
            event.causality_key(),
            DurableEventCausalityKey::TaskRun {
                task_run_id: "run_1".to_owned()
            }
        );
        assert!(event.is_terminal());
    }

    #[test]
    fn task_progress_payload_classifies_as_progress() {
        let event = task_event(TaskEventPayload::Progress {
            task_id: "task_1".to_owned(),
            run_id: Some("run_1".to_owned()),
            message: "working".to_owned(),
            details: None,
        });

        assert_eq!(event.event_class(), ProtocolEventClass::Progress);
        assert_eq!(
            event.causality_key(),
            DurableEventCausalityKey::TaskRun {
                task_run_id: "run_1".to_owned()
            }
        );
        assert!(!event.is_terminal());
    }

    #[test]
    fn progress_item_delta_key_uses_delta_stream() {
        let event = AgentProgressEvent::ItemDelta {
            notification: ItemDeltaNotification {
                workspace_id: "ws_1".to_owned(),
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
                item_id: "item_1".to_owned(),
                delta: "hello".to_owned(),
                stream: Some(ItemDeltaStream::AgentMessage),
                payload: None,
                markdown: None,
                markdown_version: None,
            },
        };

        assert_eq!(
            event.coalescing_key(),
            ProgressCoalescingKey {
                workspace_id: "ws_1".to_owned(),
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
                item_id: "item_1".to_owned(),
                stream: ItemDeltaStream::AgentMessage,
            }
        );
    }

    #[test]
    fn generated_schema_documents_include_agent_event_lane_contracts() {
        let schema_names = crate::protocol_schema_documents()
            .into_iter()
            .map(|document| document.file_name)
            .collect::<Vec<_>>();

        for expected in [
            "agent_durable_event.json",
            "agent_progress_event.json",
            "durable_event_causality_key.json",
            "progress_coalescing_key.json",
        ] {
            assert!(
                schema_names.iter().any(|name| *name == expected),
                "missing schema document {expected}"
            );
        }
    }
}
