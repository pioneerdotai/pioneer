//! Exact actor projections for same-capsule authored work.
//!
//! These values separate the actor that authored a conversation mutation from
//! the human or collaborator that supplied the authority to execute it.
//! Nothing in this module infers an actor from the current session, Task owner,
//! provider, or runtime process.

use crate::{
    AgentActionId, AgentAuthoredInput, AgentExecutionId, AgentPresentationSnapshot,
    AgentReviewDecision, PersistedActorRef, PrincipalId, TaskResultReviewerRef, ThreadMode,
    TurnAuthorSnapshot,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentAuthoredTurnProjection {
    pub action_id: AgentActionId,
    pub execution_id: AgentExecutionId,
    pub mode: ThreadMode,
    pub author: TurnAuthorSnapshot,
    pub input: AgentAuthoredInput,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller_principal_id: Option<PrincipalId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentAuthoredTaskProjection {
    pub action_id: AgentActionId,
    pub task_id: String,
    pub execution_id: AgentExecutionId,
    pub author: TurnAuthorSnapshot,
    pub prompt: AgentAuthoredInput,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller_principal_id: Option<PrincipalId>,
}

impl AgentAuthoredTaskProjection {
    pub fn new(
        action_id: AgentActionId,
        task_id: impl Into<String>,
        snapshot: &AgentPresentationSnapshot,
        prompt: AgentAuthoredInput,
        controller_principal_id: Option<PrincipalId>,
    ) -> Result<Self, AgentAuthoredProjectionError> {
        let task_id = task_id.into();
        if task_id.trim().is_empty() {
            return Err(AgentAuthoredProjectionError::MissingTask);
        }
        prompt
            .validate_visible()
            .map_err(|_| AgentAuthoredProjectionError::UnsafeVisibleInput)?;
        Ok(Self {
            action_id,
            task_id,
            execution_id: snapshot.agent_execution_id.clone(),
            author: snapshot.to_turn_author_snapshot(),
            prompt,
            controller_principal_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentTaskReviewProjection {
    pub action_id: AgentActionId,
    pub task_id: String,
    pub reviewer: TaskResultReviewerRef,
    pub decision: AgentReviewDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback: Option<AgentAuthoredInput>,
}

impl AgentTaskReviewProjection {
    pub fn new(
        action_id: AgentActionId,
        task_id: impl Into<String>,
        reviewer: TaskResultReviewerRef,
        decision: AgentReviewDecision,
        feedback: Option<AgentAuthoredInput>,
    ) -> Result<Self, AgentAuthoredProjectionError> {
        let task_id = task_id.into();
        if task_id.trim().is_empty() {
            return Err(AgentAuthoredProjectionError::MissingTask);
        }
        if let Some(feedback) = feedback.as_ref() {
            feedback
                .validate_visible()
                .map_err(|_| AgentAuthoredProjectionError::UnsafeVisibleInput)?;
        }
        Ok(Self {
            action_id,
            task_id,
            reviewer,
            decision,
            feedback,
        })
    }
}

impl AgentAuthoredTurnProjection {
    pub fn new(
        action_id: AgentActionId,
        snapshot: &AgentPresentationSnapshot,
        mode: ThreadMode,
        input: AgentAuthoredInput,
        controller_principal_id: Option<PrincipalId>,
    ) -> Result<Self, AgentAuthoredProjectionError> {
        input
            .validate_visible()
            .map_err(|_| AgentAuthoredProjectionError::UnsafeVisibleInput)?;
        if snapshot.agent_execution_id.as_str().trim().is_empty() {
            return Err(AgentAuthoredProjectionError::MissingExecution);
        }
        let author = snapshot.to_turn_author_snapshot();
        if !matches!(author.actor, PersistedActorRef::AgentExecution(_)) {
            return Err(AgentAuthoredProjectionError::WrongActorKind);
        }
        Ok(Self {
            action_id,
            execution_id: snapshot.agent_execution_id.clone(),
            mode,
            author,
            input,
            controller_principal_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentAuthoredProjectionError {
    MissingExecution,
    MissingTask,
    WrongActorKind,
    UnsafeVisibleInput,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentIdentityId, AgentIdentitySourceKind};

    fn snapshot() -> AgentPresentationSnapshot {
        AgentPresentationSnapshot {
            agent_identity_id: AgentIdentityId::new("A12345678901234567890").unwrap(),
            agent_execution_id: AgentExecutionId::new("E12345678901234567890").unwrap(),
            identity_source_kind: AgentIdentitySourceKind::NativeAgent,
            identity_source_revision: 7,
            display_name: "Worker".to_owned(),
            nickname: "worker".to_owned(),
            avatar_revision: None,
            role_label: Some("Agent".to_owned()),
        }
    }

    #[test]
    fn authored_projection_uses_exact_agent_not_current_user() {
        let projection = AgentAuthoredTurnProjection::new(
            AgentActionId::new("X12345678901234567890").unwrap(),
            &snapshot(),
            ThreadMode::Chat,
            AgentAuthoredInput::from(vec![crate::UserInput::Text {
                text: "hello".to_owned(),
                text_elements: Vec::new(),
            }]),
            Some(PrincipalId::new("P12345678901234567890").unwrap()),
        )
        .unwrap();
        assert_eq!(
            projection.author.actor,
            PersistedActorRef::AgentExecution(projection.execution_id.clone())
        );
        assert_eq!(
            projection.controller_principal_id.unwrap().as_str(),
            "P12345678901234567890"
        );
    }

    #[test]
    fn runtime_inputs_are_not_visible_authored_content() {
        let result = AgentAuthoredTurnProjection::new(
            AgentActionId::new("X12345678901234567890").unwrap(),
            &snapshot(),
            ThreadMode::Agent,
            AgentAuthoredInput::from(vec![crate::UserInput::LocalFile {
                path: "/private/secret".to_owned(),
            }]),
            None,
        );
        assert_eq!(
            result,
            Err(AgentAuthoredProjectionError::UnsafeVisibleInput)
        );
    }

    #[test]
    fn visible_projection_has_no_runtime_wrapper_or_authorization_fields() {
        let projection = AgentAuthoredTurnProjection::new(
            AgentActionId::new("X12345678901234567890").unwrap(),
            &snapshot(),
            ThreadMode::Agent,
            AgentAuthoredInput::default(),
            None,
        )
        .unwrap();
        let encoded = serde_json::to_string(&projection).unwrap();
        for forbidden in [
            "system_prompt",
            "authorization_envelope",
            "tool_schema",
            "host_path",
            "credential",
            "provider",
            "model",
        ] {
            assert!(!encoded.contains(forbidden), "forbidden field {forbidden}");
        }
    }

    #[test]
    fn task_and_review_projections_keep_exact_actor_kinds() {
        let snapshot = snapshot();
        let task = AgentAuthoredTaskProjection::new(
            AgentActionId::new("X12345678901234567890").unwrap(),
            "T12345678901234567890",
            &snapshot,
            AgentAuthoredInput::default(),
            None,
        )
        .unwrap();
        assert_eq!(
            task.author.actor,
            PersistedActorRef::AgentExecution(snapshot.agent_execution_id.clone())
        );
        let review = AgentTaskReviewProjection::new(
            AgentActionId::new("X12345678901234567891").unwrap(),
            "T12345678901234567890",
            TaskResultReviewerRef::AgentExecution(snapshot.agent_execution_id),
            AgentReviewDecision::Accept,
            None,
        )
        .unwrap();
        assert!(matches!(
            review.reviewer,
            TaskResultReviewerRef::AgentExecution(_)
        ));
    }
}
