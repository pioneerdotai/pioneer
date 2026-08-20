//! Server-owned semantic projections shared by every Pioneer client.
//!
//! The client shells must render these values as received.  In particular, an
//! agent author is never inferred from a task, turn origin, provider, model, or
//! the currently signed-in principal.

use crate::{
    AgentDelegationRouteProjection, AgentExecutionId, AgentExecutionProfileProjection,
    AgentIdentityId, AgentIdentitySourceKind, AgentIdentityStatus, AgentRouteAction,
    AgentRouteDisclosurePolicy, PersistedActorRef, PrincipalId, TurnAuthorSnapshot,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PrincipalAuthorSnapshot {
    pub principal_id: PrincipalId,
    pub display_name: String,
    pub nickname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_revision: Option<String>,
}

impl PrincipalAuthorSnapshot {
    pub fn from_turn_author(author: &TurnAuthorSnapshot, principal_id: PrincipalId) -> Self {
        Self {
            principal_id,
            display_name: author.display_name.clone(),
            nickname: author.nickname.clone(),
            avatar_revision: author.avatar_revision.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClientAgentPresentationSnapshot {
    pub agent_identity_id: AgentIdentityId,
    pub agent_execution_id: AgentExecutionId,
    pub source_kind: AgentIdentitySourceKind,
    pub source_revision: u64,
    pub display_name: String,
    pub nickname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_label: Option<String>,
    pub status: AgentIdentityStatus,
}

impl ClientAgentPresentationSnapshot {
    pub fn from_agent(
        agent: &crate::AgentPresentationSnapshot,
        status: AgentIdentityStatus,
    ) -> Self {
        Self {
            agent_identity_id: agent.agent_identity_id.clone(),
            agent_execution_id: agent.agent_execution_id.clone(),
            source_kind: agent.identity_source_kind,
            source_revision: agent.identity_source_revision,
            display_name: agent.display_name.clone(),
            nickname: agent.nickname.clone(),
            avatar_revision: agent.avatar_revision.clone(),
            role_label: agent.role_label.clone(),
            status,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum ConversationAuthorPresentation {
    Principal(PrincipalAuthorSnapshot),
    Agent(ClientAgentPresentationSnapshot),
    System { label: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CrossThreadSourceVisibility {
    Accessible,
    Inaccessible,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SafeRouteProvenance {
    pub action: AgentRouteAction,
    pub visibility: CrossThreadSourceVisibility,
    /// Present only when the viewer has source read authority.  Source title,
    /// participants, prompts, and raw identifiers are never sent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_thread_label: Option<String>,
    pub disclosure: AgentRouteDisclosurePolicy,
}

impl SafeRouteProvenance {
    pub const fn delegated_action(action: AgentRouteAction) -> Self {
        Self {
            action,
            visibility: CrossThreadSourceVisibility::Inaccessible,
            source_thread_label: None,
            disclosure: AgentRouteDisclosurePolicy {
                text: false,
                artifacts: false,
                context: false,
                user_input: false,
                result_return: crate::AgentResultReturnPolicy::None,
            },
        }
    }

    pub fn from_route(
        route: &AgentDelegationRouteProjection,
        action: AgentRouteAction,
        source_read_authorized: bool,
        source_thread_label: Option<String>,
    ) -> Self {
        let accessible = source_read_authorized && route.permits(action, None);
        Self {
            action,
            visibility: if accessible {
                CrossThreadSourceVisibility::Accessible
            } else {
                CrossThreadSourceVisibility::Inaccessible
            },
            source_thread_label: accessible.then_some(source_thread_label).flatten(),
            disclosure: if accessible {
                route.disclosure
            } else {
                AgentRouteDisclosurePolicy::default()
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentAuthoredMessageState {
    Streaming,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentAuthoredMessage {
    pub message_id: String,
    pub turn_id: String,
    pub author: ClientAgentPresentationSnapshot,
    pub state: AgentAuthoredMessageState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<SafeRouteProvenance>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentWorkNodeState {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentWorkNodeProjection {
    pub execution_id: AgentExecutionId,
    pub state: AgentWorkNodeState,
    pub progress_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentWorkGraphProjection {
    pub root_execution_id: AgentExecutionId,
    /// Monotonic persisted resource-state timestamp used only to reject stale
    /// graph projections racing with live queue/promotion/finalization events.
    pub updated_at_unix_micros: i64,
    pub queued_count: u64,
    pub running_count: u64,
    pub terminal_count: u64,
    /// True while one or more authorized nodes are durably queued for
    /// server-owned capacity. This is resource state, not an authorization
    /// failure and never implies that the whole graph is blocked.
    pub saturated: bool,
    /// Stable, bounded nodes in the exact root graph. No prompt, provider,
    /// model, thread title, runtime path, or other private payload is exposed.
    pub nodes: Vec<AgentWorkNodeProjection>,
}

/// The profile data a product surface may explain without exposing provider,
/// model, credentials, runtime IDs, or local paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SafeExecutionProfileMetadata {
    pub profile_id: String,
    pub backend: String,
    pub provider_label: Option<String>,
    pub model_label: Option<String>,
}

impl SafeExecutionProfileMetadata {
    pub fn from_projection(profile: &AgentExecutionProfileProjection) -> Self {
        let (backend, provider_label, model_label) = match &profile.backend {
            crate::AgentExecutionProfileBackend::ApiProvider => (
                "api".to_owned(),
                Some(profile.provider_display_name.clone()),
                Some(profile.model_display_name.clone()),
            ),
            crate::AgentExecutionProfileBackend::CliRuntime { .. } => {
                ("cli".to_owned(), None, None)
            }
            crate::AgentExecutionProfileBackend::AcpAgentRuntime { .. } => {
                ("acp".to_owned(), None, None)
            }
        };
        Self {
            profile_id: profile.id.as_str().to_owned(),
            backend,
            provider_label,
            model_label,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientAuthorProjectionError {
    MissingAgentSnapshot,
    WrongActorKind,
    AgentSnapshotMismatch,
}

fn agent_snapshot_matches_author(
    author: &TurnAuthorSnapshot,
    snapshot: &crate::AgentPresentationSnapshot,
) -> bool {
    matches!(
        &author.actor,
        PersistedActorRef::AgentExecution(execution_id)
            if execution_id == &snapshot.agent_execution_id
                && author.display_name == snapshot.display_name
                && author.nickname == snapshot.nickname
                && author.avatar_revision == snapshot.avatar_revision
    )
}

pub fn project_conversation_author(
    author: &TurnAuthorSnapshot,
    agent: Option<(&crate::AgentPresentationSnapshot, AgentIdentityStatus)>,
) -> Result<ConversationAuthorPresentation, ClientAuthorProjectionError> {
    match &author.actor {
        PersistedActorRef::Principal(principal_id) => {
            Ok(ConversationAuthorPresentation::Principal(
                PrincipalAuthorSnapshot::from_turn_author(author, principal_id.clone()),
            ))
        }
        PersistedActorRef::AgentExecution(_) => agent
            .or_else(|| {
                author
                    .agent
                    .as_ref()
                    .map(|snapshot| (snapshot, AgentIdentityStatus::Active))
            })
            .filter(|(snapshot, _)| agent_snapshot_matches_author(author, snapshot))
            .map(|(snapshot, status)| {
                ConversationAuthorPresentation::Agent(ClientAgentPresentationSnapshot::from_agent(
                    snapshot, status,
                ))
            })
            .ok_or(ClientAuthorProjectionError::MissingAgentSnapshot),
        PersistedActorRef::System => Ok(ConversationAuthorPresentation::System {
            label: "System".to_owned(),
        }),
    }
}

pub fn project_agent_authored_message(
    message_id: impl Into<String>,
    turn_id: impl Into<String>,
    author: &TurnAuthorSnapshot,
    agent: Option<(&crate::AgentPresentationSnapshot, AgentIdentityStatus)>,
    state: AgentAuthoredMessageState,
    route: Option<SafeRouteProvenance>,
) -> Result<AgentAuthoredMessage, ClientAuthorProjectionError> {
    if !matches!(author.actor, PersistedActorRef::AgentExecution(_)) {
        return Err(ClientAuthorProjectionError::WrongActorKind);
    }
    let agent = agent.or_else(|| {
        author
            .agent
            .as_ref()
            .map(|snapshot| (snapshot, AgentIdentityStatus::Active))
    });
    let Some((snapshot, status)) = agent else {
        return Err(ClientAuthorProjectionError::MissingAgentSnapshot);
    };
    if !agent_snapshot_matches_author(author, snapshot) {
        return Err(ClientAuthorProjectionError::AgentSnapshotMismatch);
    }
    Ok(AgentAuthoredMessage {
        message_id: message_id.into(),
        turn_id: turn_id.into(),
        author: ClientAgentPresentationSnapshot::from_agent(snapshot, status),
        state,
        route,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentExecutionId, AgentIdentityId, AgentPresentationSnapshot};

    fn agent() -> AgentPresentationSnapshot {
        AgentPresentationSnapshot {
            agent_identity_id: AgentIdentityId::new("A12345678901234567890").unwrap(),
            agent_execution_id: AgentExecutionId::new("E12345678901234567890").unwrap(),
            identity_source_kind: AgentIdentitySourceKind::NativeAgent,
            identity_source_revision: 4,
            display_name: "Worker".to_owned(),
            nickname: "worker".to_owned(),
            avatar_revision: Some("avatar-4".to_owned()),
            role_label: Some("Agent".to_owned()),
        }
    }

    fn author(actor: PersistedActorRef) -> TurnAuthorSnapshot {
        TurnAuthorSnapshot {
            actor,
            display_name: "Worker".to_owned(),
            nickname: "worker".to_owned(),
            avatar_revision: Some("avatar-4".to_owned()),
            agent: None,
        }
    }

    #[test]
    fn author_union_requires_an_exact_agent_snapshot() {
        let error = project_conversation_author(
            &author(PersistedActorRef::AgentExecution(
                agent().agent_execution_id.clone(),
            )),
            None,
        )
        .expect_err("agent author without an exact snapshot must fail");
        assert_eq!(error, ClientAuthorProjectionError::MissingAgentSnapshot);
    }

    #[test]
    fn agent_message_requires_exact_agent_snapshot() {
        let a = agent();
        let author = author(PersistedActorRef::AgentExecution(
            a.agent_execution_id.clone(),
        ));
        let message = project_agent_authored_message(
            "message",
            "turn",
            &author,
            Some((&a, AgentIdentityStatus::Active)),
            AgentAuthoredMessageState::Completed,
            Some(SafeRouteProvenance::delegated_action(
                AgentRouteAction::SendMessage,
            )),
        )
        .unwrap();
        assert_eq!(message.author.nickname, "worker");
        assert_eq!(
            message.route.unwrap().visibility,
            CrossThreadSourceVisibility::Inaccessible
        );
    }

    #[test]
    fn embedded_immutable_agent_snapshot_is_sufficient_for_clients() {
        let a = agent();
        let author = a.to_turn_author_snapshot();
        let projected = project_conversation_author(&author, None)
            .expect("embedded exact snapshot should project");
        assert!(matches!(
            projected,
            ConversationAuthorPresentation::Agent(ClientAgentPresentationSnapshot {
                source_revision: 4,
                ..
            })
        ));
        let message = project_agent_authored_message(
            "message",
            "turn",
            &author,
            None,
            AgentAuthoredMessageState::Completed,
            None,
        )
        .expect("embedded exact snapshot should project");
        assert_eq!(message.author.role_label.as_deref(), Some("Agent"));
    }

    #[test]
    fn inaccessible_route_has_no_source_label_or_disclosure() {
        let route = SafeRouteProvenance::delegated_action(AgentRouteAction::SendMessage);
        assert!(route.source_thread_label.is_none());
        assert!(!route.disclosure.allows_anything());
    }
}
