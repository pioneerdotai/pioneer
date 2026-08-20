//! Canonical, runtime-neutral agent mutation intents.
//!
//! These values contain opaque server-projected selections only.  They do not
//! carry actor/session credentials, provider/model choices, raw ACLs or host
//! paths.  Adapters build an intent; Gateway authorization and commit code
//! remains the only place that resolves or writes the domain mutation.

use crate::{
    AgentActionId, AgentAuthoredInput, AgentDelegationRouteId, AgentExecutionId,
    AgentExecutionProfileId, AgentLaunchSelection, AgentStartTarget,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentThreadAudienceTemplate {
    HomeCapsule,
    RootDelegation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentThreadCreationOption {
    pub option_id: String,
    pub audience: AgentThreadAudienceTemplate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentBoundRuntimeSelection {
    pub profile_id: AgentExecutionProfileId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentTaskActionSelection {
    pub task_template_id: String,
    pub launch: AgentLaunchSelection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentTaskControl {
    Cancel,
    Resume,
    Detach,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentReviewDecision {
    Accept,
    Reject,
    RequestChanges,
    Abstain,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", tag = "kind", deny_unknown_fields)]
pub enum AgentActionIntent {
    SendMessage {
        action_id: AgentActionId,
        execution_id: AgentExecutionId,
        target: AgentStartTarget,
        input: AgentAuthoredInput,
        idempotency_key: String,
    },
    CreateThread {
        action_id: AgentActionId,
        execution_id: AgentExecutionId,
        option: AgentThreadCreationOption,
        idempotency_key: String,
    },
    StartAgent {
        action_id: AgentActionId,
        execution_id: AgentExecutionId,
        start: crate::StartAgentIntent,
        idempotency_key: String,
    },
    CreateTask {
        action_id: AgentActionId,
        execution_id: AgentExecutionId,
        target: AgentStartTarget,
        selection: AgentTaskActionSelection,
        idempotency_key: String,
    },
    ScheduleTask {
        action_id: AgentActionId,
        execution_id: AgentExecutionId,
        target: AgentStartTarget,
        selection: AgentTaskActionSelection,
        schedule_option_id: String,
        idempotency_key: String,
    },
    ReviewTaskResult {
        action_id: AgentActionId,
        execution_id: AgentExecutionId,
        task_id: String,
        decision: AgentReviewDecision,
        idempotency_key: String,
    },
    ControlTask {
        action_id: AgentActionId,
        execution_id: AgentExecutionId,
        task_id: String,
        control: AgentTaskControl,
        idempotency_key: String,
    },
    DeliverResult {
        action_id: AgentActionId,
        execution_id: AgentExecutionId,
        route_id: Option<AgentDelegationRouteId>,
        target: AgentStartTarget,
        result_reference: String,
        idempotency_key: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentActionKind {
    SendMessage,
    CreateThread,
    StartAgent,
    CreateTask,
    ScheduleTask,
    ReviewTaskResult,
    ControlTask,
    DeliverResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedAgentAction {
    pub action_id: AgentActionId,
    pub execution_id: AgentExecutionId,
    pub kind: AgentActionKind,
    pub target: Option<AgentStartTarget>,
    pub idempotency_key: String,
    pub launch: Option<AgentLaunchSelection>,
    pub route_id: Option<AgentDelegationRouteId>,
    pub opaque_option_id: Option<String>,
    pub opaque_resource_id: Option<String>,
    /// Digest of the complete typed intent. The normalized projection omits
    /// visible content and detailed control payloads, so this fence is what
    /// makes same-key/different-payload retries conflict instead of silently
    /// replaying the first write.
    pub payload_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentActionNormalizationError {
    EmptyIdempotencyKey,
    EmptyOpaqueSelection,
    InvalidTarget,
    UnsafeVisibleInput,
    PayloadLimitExceeded,
}

impl AgentActionIntent {
    pub fn normalize(&self) -> Result<NormalizedAgentAction, AgentActionNormalizationError> {
        match self {
            Self::SendMessage { input, .. }
            | Self::StartAgent {
                start: crate::StartAgentIntent { input, .. },
                ..
            } => match input.validate_visible() {
                Ok(()) => {}
                Err(crate::AgentAuthoredInputError::PayloadLimitExceeded) => {
                    return Err(AgentActionNormalizationError::PayloadLimitExceeded);
                }
                Err(_) => return Err(AgentActionNormalizationError::UnsafeVisibleInput),
            },
            _ => {}
        }
        let mut payload_hasher = BoundedActionPayloadHasher::new();
        serde_json::to_writer(&mut payload_hasher, self)
            .map_err(|_| AgentActionNormalizationError::PayloadLimitExceeded)?;
        let payload_fingerprint = payload_hasher.finish();
        let (
            action_id,
            execution_id,
            kind,
            target,
            idempotency_key,
            launch,
            route_id,
            opaque_option_id,
            opaque_resource_id,
        ) = match self {
            Self::SendMessage {
                action_id,
                execution_id,
                target,
                idempotency_key,
                ..
            } => (
                action_id.clone(),
                execution_id.clone(),
                AgentActionKind::SendMessage,
                Some(target.clone()),
                idempotency_key.clone(),
                None,
                None,
                None,
                None,
            ),
            Self::CreateThread {
                action_id,
                execution_id,
                option,
                idempotency_key,
            } => (
                action_id.clone(),
                execution_id.clone(),
                AgentActionKind::CreateThread,
                None,
                idempotency_key.clone(),
                None,
                None,
                Some(option.option_id.clone()),
                None,
            ),
            Self::StartAgent {
                action_id,
                execution_id,
                start,
                idempotency_key,
            } => (
                action_id.clone(),
                execution_id.clone(),
                AgentActionKind::StartAgent,
                Some(start.target.clone()),
                idempotency_key.clone(),
                Some(start.launch.clone()),
                None,
                None,
                None,
            ),
            Self::CreateTask {
                action_id,
                execution_id,
                target,
                selection,
                idempotency_key,
            } => (
                action_id.clone(),
                execution_id.clone(),
                AgentActionKind::CreateTask,
                Some(target.clone()),
                idempotency_key.clone(),
                Some(selection.launch.clone()),
                None,
                Some(selection.task_template_id.clone()),
                None,
            ),
            Self::ScheduleTask {
                action_id,
                execution_id,
                target,
                selection,
                schedule_option_id,
                idempotency_key,
            } => (
                action_id.clone(),
                execution_id.clone(),
                AgentActionKind::ScheduleTask,
                Some(target.clone()),
                idempotency_key.clone(),
                Some(selection.launch.clone()),
                None,
                Some(schedule_option_id.clone()),
                Some(selection.task_template_id.clone()),
            ),
            Self::ReviewTaskResult {
                action_id,
                execution_id,
                task_id,
                idempotency_key,
                ..
            } => (
                action_id.clone(),
                execution_id.clone(),
                AgentActionKind::ReviewTaskResult,
                None,
                idempotency_key.clone(),
                None,
                None,
                None,
                Some(task_id.clone()),
            ),
            Self::ControlTask {
                action_id,
                execution_id,
                task_id,
                idempotency_key,
                ..
            } => (
                action_id.clone(),
                execution_id.clone(),
                AgentActionKind::ControlTask,
                None,
                idempotency_key.clone(),
                None,
                None,
                None,
                Some(task_id.clone()),
            ),
            Self::DeliverResult {
                action_id,
                execution_id,
                route_id,
                target,
                result_reference,
                idempotency_key,
            } => (
                action_id.clone(),
                execution_id.clone(),
                AgentActionKind::DeliverResult,
                Some(target.clone()),
                idempotency_key.clone(),
                None,
                route_id.clone(),
                None,
                Some(result_reference.clone()),
            ),
        };
        if idempotency_key.trim().is_empty() {
            return Err(AgentActionNormalizationError::EmptyIdempotencyKey);
        }
        if opaque_option_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
            || opaque_resource_id
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
        {
            return Err(AgentActionNormalizationError::EmptyOpaqueSelection);
        }
        if let Some(target) = target.as_ref() {
            let valid_target = match target {
                AgentStartTarget::CurrentThread => true,
                AgentStartTarget::SameCapsuleThread { thread_id }
                | AgentStartTarget::RoutedThread { thread_id, .. } => !thread_id.trim().is_empty(),
            };
            if !valid_target {
                return Err(AgentActionNormalizationError::InvalidTarget);
            }
            if let AgentStartTarget::RoutedThread {
                route_id: target_route,
                ..
            } = target
            {
                if matches!(kind, AgentActionKind::DeliverResult)
                    && route_id.as_ref() != Some(target_route)
                {
                    return Err(AgentActionNormalizationError::InvalidTarget);
                }
                if route_id.is_none() && matches!(kind, AgentActionKind::DeliverResult) {
                    return Err(AgentActionNormalizationError::InvalidTarget);
                }
            }
        }
        Ok(NormalizedAgentAction {
            action_id,
            execution_id,
            kind,
            target,
            idempotency_key,
            launch,
            route_id,
            opaque_option_id,
            opaque_resource_id,
            payload_fingerprint,
        })
    }

    pub fn action_id(&self) -> &AgentActionId {
        match self {
            Self::SendMessage { action_id, .. }
            | Self::CreateThread { action_id, .. }
            | Self::StartAgent { action_id, .. }
            | Self::CreateTask { action_id, .. }
            | Self::ScheduleTask { action_id, .. }
            | Self::ReviewTaskResult { action_id, .. }
            | Self::ControlTask { action_id, .. }
            | Self::DeliverResult { action_id, .. } => action_id,
        }
    }
}

struct BoundedActionPayloadHasher {
    hasher: Sha256,
    bytes: usize,
}

impl BoundedActionPayloadHasher {
    fn new() -> Self {
        Self {
            hasher: Sha256::new(),
            bytes: 0,
        }
    }

    fn finish(self) -> String {
        hex::encode(self.hasher.finalize())
    }
}

impl Write for BoundedActionPayloadHasher {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.bytes = self.bytes.saturating_add(buffer.len());
        if self.bytes > crate::TURN_EXECUTION_REQUEST_MAX_BYTES {
            return Err(std::io::Error::other("Agent action payload is too large"));
        }
        self.hasher.update(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentActionId, AgentExecutionId, AgentStartTarget};

    fn ids() -> (AgentActionId, AgentExecutionId) {
        (
            AgentActionId::new("X12345678901234567890").unwrap(),
            AgentExecutionId::new("E12345678901234567890").unwrap(),
        )
    }

    #[test]
    fn normalization_contains_no_actor_or_runtime_fields() {
        let (action_id, execution_id) = ids();
        let intent = AgentActionIntent::CreateThread {
            action_id,
            execution_id,
            option: AgentThreadCreationOption {
                option_id: "home-thread".to_owned(),
                audience: AgentThreadAudienceTemplate::HomeCapsule,
            },
            idempotency_key: "create-1".to_owned(),
        };
        let normalized = intent.normalize().unwrap();
        assert_eq!(normalized.kind, AgentActionKind::CreateThread);
        assert_eq!(normalized.opaque_option_id.as_deref(), Some("home-thread"));
        let encoded = serde_json::to_string(&intent).unwrap();
        assert!(!encoded.contains("provider"));
        assert!(!encoded.contains("credential"));
        assert!(!encoded.contains("session_id"));
    }

    #[test]
    fn routed_delivery_requires_a_route_id() {
        let (action_id, execution_id) = ids();
        let intent = AgentActionIntent::DeliverResult {
            action_id,
            execution_id,
            route_id: None,
            target: AgentStartTarget::RoutedThread {
                route_id: crate::AgentDelegationRouteId::new("R12345678901234567890").unwrap(),
                thread_id: "T12345678901234567890".to_owned(),
            },
            result_reference: "R12345678901234567890".to_owned(),
            idempotency_key: "deliver-1".to_owned(),
        };
        assert_eq!(
            intent.normalize(),
            Err(AgentActionNormalizationError::InvalidTarget)
        );
    }

    #[test]
    fn target_normalization_rejects_empty_thread_and_mismatched_route() {
        let (action_id, execution_id) = ids();
        let empty_target = AgentActionIntent::SendMessage {
            action_id: action_id.clone(),
            execution_id: execution_id.clone(),
            target: AgentStartTarget::SameCapsuleThread {
                thread_id: "  ".to_owned(),
            },
            input: AgentAuthoredInput::default(),
            idempotency_key: "send-1".to_owned(),
        };
        assert_eq!(
            empty_target.normalize(),
            Err(AgentActionNormalizationError::InvalidTarget)
        );
        let target_route = crate::AgentDelegationRouteId::new("R12345678901234567890").unwrap();
        let supplied_route = crate::AgentDelegationRouteId::new("R12345678901234567891").unwrap();
        let mismatched = AgentActionIntent::DeliverResult {
            action_id,
            execution_id,
            route_id: Some(supplied_route),
            target: AgentStartTarget::RoutedThread {
                route_id: target_route,
                thread_id: "T12345678901234567890".to_owned(),
            },
            result_reference: "result-1".to_owned(),
            idempotency_key: "deliver-1".to_owned(),
        };
        assert_eq!(
            mismatched.normalize(),
            Err(AgentActionNormalizationError::InvalidTarget)
        );
    }

    #[test]
    fn normalization_rejects_oversized_authored_payload_before_fingerprinting() {
        let (action_id, execution_id) = ids();
        let intent = AgentActionIntent::SendMessage {
            action_id,
            execution_id,
            target: AgentStartTarget::CurrentThread,
            input: AgentAuthoredInput::from(vec![crate::UserInput::Text {
                text: "x".repeat(crate::TURN_EXECUTION_INPUT_MAX_BYTES + 1),
                text_elements: Vec::new(),
            }]),
            idempotency_key: "oversized".to_owned(),
        };
        assert_eq!(
            intent.normalize(),
            Err(AgentActionNormalizationError::PayloadLimitExceeded)
        );
    }

    #[test]
    fn normalization_rejects_runtime_or_unresolved_authored_input() {
        let (action_id, execution_id) = ids();
        let intent = AgentActionIntent::SendMessage {
            action_id,
            execution_id,
            target: AgentStartTarget::CurrentThread,
            input: AgentAuthoredInput::from(vec![crate::UserInput::LocalFile {
                path: "/private/secret".to_owned(),
            }]),
            idempotency_key: "send-unsafe".to_owned(),
        };
        assert_eq!(
            intent.normalize(),
            Err(AgentActionNormalizationError::UnsafeVisibleInput)
        );
    }
}
