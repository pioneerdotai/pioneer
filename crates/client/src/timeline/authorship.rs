//! Shell-neutral authorship and work-graph projections.
//!
//! This module is deliberately a thin semantic boundary over the protocol
//! projection.  Desktop and Mobile consume the same values and do not infer
//! identity from task role, provider, model, or current-user state.

use pioneer_protocol::{
    AgentAuthoredMessage, AgentAuthoredMessageState, AgentIdentityStatus,
    AgentPresentationSnapshot, PersistedActorRef, SafeRouteProvenance, TurnAuthorSnapshot,
    project_agent_authored_message,
};
use serde::{Deserialize, Serialize};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimelineAuthorAlignment {
    CurrentPrincipal,
    OtherPrincipal,
    Agent,
    System,
    Unknown,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SemanticAgentAuthoredMessageRow {
    pub message: AgentAuthoredMessage,
    pub alignment: TimelineAuthorAlignment,
    pub accessible_to_human: bool,
}

pub fn project_agent_authored_message_row(
    message_id: impl Into<String>,
    turn_id: impl Into<String>,
    author: &TurnAuthorSnapshot,
    agent: Option<(&AgentPresentationSnapshot, AgentIdentityStatus)>,
    state: AgentAuthoredMessageState,
    route: Option<SafeRouteProvenance>,
) -> Result<SemanticAgentAuthoredMessageRow, pioneer_protocol::ClientAuthorProjectionError> {
    let message = project_agent_authored_message(message_id, turn_id, author, agent, state, route)?;
    Ok(SemanticAgentAuthoredMessageRow {
        message,
        alignment: TimelineAuthorAlignment::Agent,
        accessible_to_human: true,
    })
}

pub fn author_alignment(
    author: Option<&TurnAuthorSnapshot>,
    current_principal_id: Option<&str>,
) -> TimelineAuthorAlignment {
    match author.map(|author| &author.actor) {
        Some(PersistedActorRef::Principal(id)) if current_principal_id == Some(id.as_str()) => {
            TimelineAuthorAlignment::CurrentPrincipal
        }
        Some(PersistedActorRef::Principal(_)) => TimelineAuthorAlignment::OtherPrincipal,
        Some(PersistedActorRef::AgentExecution(_)) => TimelineAuthorAlignment::Agent,
        Some(PersistedActorRef::System) => TimelineAuthorAlignment::System,
        None => TimelineAuthorAlignment::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{AgentExecutionId, AgentIdentityId, AgentIdentitySourceKind};

    fn agent() -> AgentPresentationSnapshot {
        AgentPresentationSnapshot {
            agent_identity_id: AgentIdentityId::new("A12345678901234567890").unwrap(),
            agent_execution_id: AgentExecutionId::new("E12345678901234567890").unwrap(),
            identity_source_kind: AgentIdentitySourceKind::NativeAgent,
            identity_source_revision: 1,
            display_name: "Worker".to_owned(),
            nickname: "worker".to_owned(),
            avatar_revision: None,
            role_label: Some("Agent".to_owned()),
        }
    }

    fn author(actor: PersistedActorRef) -> TurnAuthorSnapshot {
        TurnAuthorSnapshot {
            actor,
            display_name: "Worker".to_owned(),
            nickname: "worker".to_owned(),
            avatar_revision: None,
            agent: None,
        }
    }

    #[test]
    fn semantic_row_preserves_agent_alignment_and_never_current_user() {
        let agent = agent();
        let row = project_agent_authored_message_row(
            "message",
            "turn",
            &author(PersistedActorRef::AgentExecution(
                agent.agent_execution_id.clone(),
            )),
            Some((&agent, AgentIdentityStatus::Active)),
            AgentAuthoredMessageState::Completed,
            None,
        )
        .unwrap();
        assert_eq!(row.alignment, TimelineAuthorAlignment::Agent);
        assert!(row.accessible_to_human);
    }

    #[test]
    fn alignment_is_explicit_for_system_and_unknown() {
        assert_eq!(
            author_alignment(None, None),
            TimelineAuthorAlignment::Unknown
        );
        assert_eq!(
            author_alignment(Some(&author(PersistedActorRef::System)), None),
            TimelineAuthorAlignment::System
        );
    }
}
