//! Durable, bounded cross-capsule route contract.
//!
//! A route is an explicit delegation edge between two threads.  It is not a
//! copy of the initiating user's permissions: the action subset, disclosure
//! policy, identity/profile constraints and policy generations are all pinned
//! facts that must be revalidated before a write.

use crate::{AgentDelegationRouteId, AgentExecutionId, AgentExecutionProfileId, AgentIdentityId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentRouteKind {
    ExecutionBound,
    IdentityBound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentRouteStatus {
    Prepared,
    Active,
    Expired,
    Revoked,
}

impl AgentRouteStatus {
    pub const fn is_live(self) -> bool {
        matches!(self, Self::Active)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentRouteAction {
    SendMessage,
    StartAgent,
    CreateTask,
    ScheduleTask,
    ReviewTaskResult,
    DeliverResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRouteDisclosurePolicy {
    /// Explicit text authored for the routed operation.
    #[serde(default)]
    pub text: bool,
    /// Exact, already-authorized artifact handles. Raw files and paths are
    /// never represented by this flag.
    #[serde(default)]
    pub artifacts: bool,
    /// Bounded conversation excerpts or summaries selected by server policy.
    #[serde(default)]
    pub context: bool,
    /// Explicit user-provided Task inputs. This is deliberately independent
    /// from Agent-authored text and inherited conversation context.
    #[serde(default)]
    pub user_input: bool,
    /// Result return is a separate disclosure class: allowing ordinary text
    /// must never imply that a full Task result can cross a capsule boundary.
    #[serde(default)]
    pub result_return: AgentResultReturnPolicy,
}

impl AgentRouteDisclosurePolicy {
    pub const fn allows_anything(self) -> bool {
        self.text
            || self.artifacts
            || self.context
            || self.user_input
            || !matches!(self.result_return, AgentResultReturnPolicy::None)
    }

    pub fn permits_authored_input(&self, input: &crate::AgentAuthoredInput) -> bool {
        input.as_slice().iter().all(|item| match item {
            crate::UserInput::Text { .. } => self.text,
            crate::UserInput::Artifact { .. } => self.artifacts,
            crate::UserInput::Image { .. }
            | crate::UserInput::LocalImage { .. }
            | crate::UserInput::File { .. }
            | crate::UserInput::LocalFile { .. }
            | crate::UserInput::Audio { .. }
            | crate::UserInput::LocalAudio { .. }
            | crate::UserInput::Video { .. }
            | crate::UserInput::LocalVideo { .. }
            | crate::UserInput::Mention { .. } => false,
        })
    }

    pub const fn permits_result_format(self, format: crate::TaskDeliveryFormat) -> bool {
        match (self.result_return, format) {
            (AgentResultReturnPolicy::FullResult, _) => true,
            (AgentResultReturnPolicy::SummaryOnly, crate::TaskDeliveryFormat::Summary) => true,
            (AgentResultReturnPolicy::None, _)
            | (AgentResultReturnPolicy::SummaryOnly, crate::TaskDeliveryFormat::FullResult) => {
                false
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum AgentResultReturnPolicy {
    #[default]
    None,
    SummaryOnly,
    FullResult,
}

/// Safe route projection returned to an authorized caller. It intentionally
/// contains no participant list, prompt, credential, provider session or raw
/// ACL data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentDelegationRouteProjection {
    pub id: AgentDelegationRouteId,
    pub source_execution_id: AgentExecutionId,
    #[serde(default)]
    pub source_capsule_id: String,
    pub destination_thread_id: String,
    pub destination_capsule_id: String,
    pub kind: AgentRouteKind,
    pub status: AgentRouteStatus,
    pub allowed_actions: Vec<AgentRouteAction>,
    pub disclosure: AgentRouteDisclosurePolicy,
    pub source_agent_identity_id: AgentIdentityId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_agent_identity_id: Option<AgentIdentityId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_profile_id: Option<String>,
    pub source_workspace_id: String,
    pub destination_workspace_id: String,
    pub source_gateway_id: String,
    pub destination_gateway_id: String,
    pub generation: u64,
    pub source_policy_generation: u64,
    pub destination_policy_generation: u64,
    pub hop_count: u16,
    pub max_hops: u16,
    pub grant_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub return_route_id: Option<AgentDelegationRouteId>,
}

/// Authenticated, structured request for a persistent route authority
/// expansion. Free-form text, participants and ACLs are intentionally absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentDelegationRouteCreateParams {
    pub source_execution_id: AgentExecutionId,
    pub destination_thread_id: String,
    pub kind: AgentRouteKind,
    pub allowed_actions: Vec<AgentRouteAction>,
    pub disclosure: AgentRouteDisclosurePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_agent_identity_id: Option<AgentIdentityId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_profile_id: Option<AgentExecutionProfileId>,
    pub expires_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub return_route_id: Option<AgentDelegationRouteId>,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentDelegationRouteListParams {
    pub source_execution_id: AgentExecutionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentDelegationRouteListResponse {
    pub routes: Vec<AgentDelegationRouteProjection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentDelegationRouteRevokeParams {
    pub route_id: AgentDelegationRouteId,
    pub expected_generation: u64,
    pub idempotency_key: String,
}

/// Optional typed extension accepted by `turn/start` for automatic,
/// short-lived root-execution delegation. It is deliberately separate from
/// the prompt and has no participant/role/ACL fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRootDelegationRequest {
    pub destination_thread_id: String,
    pub allowed_actions: Vec<AgentRouteAction>,
    pub disclosure: AgentRouteDisclosurePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_agent_identity_id: Option<AgentIdentityId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_profile_id: Option<AgentExecutionProfileId>,
    pub expires_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub return_route_id: Option<AgentDelegationRouteId>,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentRouteValidationError {
    EmptyIdentifier,
    NoActions,
    DuplicateAction,
    InvalidGeneration,
    InvalidHopBoundary,
    CrossWorkspace,
    CrossGateway,
    DisclosureRequired,
    InvalidResultReturnPolicy,
    InvalidExpiry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentRouteGraphValidationError {
    TooManyEdges,
    Cycle,
    HopBoundary,
}

/// Validate the explicit capsule route graph. Edges remain independently
/// authorized; this check only prevents cycles and unbounded topology from
/// being persisted as a latent transitive escalation surface.
pub fn validate_agent_route_graph(
    edges: &[(String, String)],
    max_edges: usize,
    max_hops: usize,
) -> Result<(), AgentRouteGraphValidationError> {
    if edges.len() > max_edges {
        return Err(AgentRouteGraphValidationError::TooManyEdges);
    }
    let mut adjacency: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (source, destination) in edges {
        if source == destination {
            continue;
        }
        adjacency
            .entry(source.as_str())
            .or_default()
            .push(destination.as_str());
    }
    fn longest_path_from<'a>(
        node: &'a str,
        adjacency: &BTreeMap<&'a str, Vec<&'a str>>,
        visiting: &mut BTreeSet<&'a str>,
        longest_paths: &mut BTreeMap<&'a str, usize>,
        max_hops: usize,
    ) -> Result<usize, AgentRouteGraphValidationError> {
        if !visiting.insert(node) {
            return Err(AgentRouteGraphValidationError::Cycle);
        }
        if let Some(longest) = longest_paths.get(node).copied() {
            visiting.remove(node);
            return Ok(longest);
        }
        let mut longest = 0usize;
        for next in adjacency.get(node).into_iter().flatten().copied() {
            let candidate = longest_path_from(next, adjacency, visiting, longest_paths, max_hops)?
                .saturating_add(1);
            if candidate > max_hops {
                return Err(AgentRouteGraphValidationError::HopBoundary);
            }
            longest = longest.max(candidate);
        }
        visiting.remove(node);
        longest_paths.insert(node, longest);
        Ok(longest)
    }

    let mut visiting = BTreeSet::new();
    let mut longest_paths = BTreeMap::new();
    for source in adjacency.keys().copied() {
        longest_path_from(
            source,
            &adjacency,
            &mut visiting,
            &mut longest_paths,
            max_hops,
        )?;
    }
    Ok(())
}

impl AgentDelegationRouteProjection {
    pub fn validate(&self, now_millis: Option<i64>) -> Result<(), AgentRouteValidationError> {
        if self.source_capsule_id.trim().is_empty()
            || self.destination_thread_id.trim().is_empty()
            || self.destination_capsule_id.trim().is_empty()
            || self.source_workspace_id.trim().is_empty()
            || self.destination_workspace_id.trim().is_empty()
            || self.source_gateway_id.trim().is_empty()
            || self.destination_gateway_id.trim().is_empty()
            || self.grant_fingerprint.trim().is_empty()
        {
            return Err(AgentRouteValidationError::EmptyIdentifier);
        }
        if self.allowed_actions.is_empty() {
            return Err(AgentRouteValidationError::NoActions);
        }
        for (index, action) in self.allowed_actions.iter().enumerate() {
            if self.allowed_actions[..index].contains(action) {
                return Err(AgentRouteValidationError::DuplicateAction);
            }
        }
        if self.generation == 0
            || self.source_policy_generation == 0
            || self.destination_policy_generation == 0
        {
            return Err(AgentRouteValidationError::InvalidGeneration);
        }
        if self.max_hops == 0 || self.hop_count > self.max_hops {
            return Err(AgentRouteValidationError::InvalidHopBoundary);
        }
        if self.source_workspace_id != self.destination_workspace_id {
            return Err(AgentRouteValidationError::CrossWorkspace);
        }
        if self.source_gateway_id != self.destination_gateway_id {
            return Err(AgentRouteValidationError::CrossGateway);
        }
        if !self.disclosure.allows_anything() {
            return Err(AgentRouteValidationError::DisclosureRequired);
        }
        let delivers_result = self
            .allowed_actions
            .contains(&AgentRouteAction::DeliverResult);
        if delivers_result
            != !matches!(self.disclosure.result_return, AgentResultReturnPolicy::None)
        {
            return Err(AgentRouteValidationError::InvalidResultReturnPolicy);
        }
        if let (Some(expires_at), Some(now)) = (self.expires_at, now_millis)
            && expires_at <= now
            && matches!(
                self.status,
                AgentRouteStatus::Prepared | AgentRouteStatus::Active
            )
        {
            return Err(AgentRouteValidationError::InvalidExpiry);
        }
        Ok(())
    }

    pub fn permits(&self, action: AgentRouteAction, now_millis: Option<i64>) -> bool {
        self.status.is_live()
            && self
                .expires_at
                .zip(now_millis)
                .is_none_or(|(expires_at, now)| expires_at > now)
            && self.allowed_actions.contains(&action)
    }

    pub fn can_return_result_via(&self, return_route: &Self, now_millis: Option<i64>) -> bool {
        self.return_route_id.as_ref() == Some(&return_route.id)
            && return_route.source_capsule_id == self.destination_capsule_id
            && return_route.destination_capsule_id == self.source_capsule_id
            && return_route.source_workspace_id == self.destination_workspace_id
            && return_route.destination_workspace_id == self.source_workspace_id
            && return_route.source_gateway_id == self.destination_gateway_id
            && return_route.destination_gateway_id == self.source_gateway_id
            && return_route.permits(AgentRouteAction::DeliverResult, now_millis)
            && !matches!(
                return_route.disclosure.result_return,
                AgentResultReturnPolicy::None
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route() -> AgentDelegationRouteProjection {
        AgentDelegationRouteProjection {
            id: AgentDelegationRouteId::new("R12345678901234567890").unwrap(),
            source_execution_id: AgentExecutionId::new("E12345678901234567890").unwrap(),
            source_capsule_id: "T12345678901234567891".to_owned(),
            destination_thread_id: "T12345678901234567890".to_owned(),
            destination_capsule_id: "T12345678901234567891".to_owned(),
            kind: AgentRouteKind::ExecutionBound,
            status: AgentRouteStatus::Active,
            allowed_actions: vec![AgentRouteAction::SendMessage],
            disclosure: AgentRouteDisclosurePolicy {
                text: true,
                ..Default::default()
            },
            source_agent_identity_id: AgentIdentityId::new("A12345678901234567890").unwrap(),
            destination_agent_identity_id: None,
            destination_profile_id: None,
            source_workspace_id: "W12345678901234567890".to_owned(),
            destination_workspace_id: "W12345678901234567890".to_owned(),
            source_gateway_id: "G12345678901234567890".to_owned(),
            destination_gateway_id: "G12345678901234567890".to_owned(),
            generation: 1,
            source_policy_generation: 1,
            destination_policy_generation: 1,
            hop_count: 1,
            max_hops: 8,
            grant_fingerprint: "g".repeat(64),
            expires_at: None,
            return_route_id: None,
        }
    }

    #[test]
    fn route_is_bounded_and_action_scoped() {
        let route = route();
        route.validate(None).unwrap();
        assert!(route.permits(AgentRouteAction::SendMessage, None));
        assert!(!route.permits(AgentRouteAction::StartAgent, None));
    }

    #[test]
    fn revoked_or_expired_route_cannot_be_used() {
        let mut route = route();
        route.status = AgentRouteStatus::Revoked;
        assert!(!route.permits(AgentRouteAction::SendMessage, None));
        route.status = AgentRouteStatus::Active;
        route.expires_at = Some(10);
        assert!(!route.permits(AgentRouteAction::SendMessage, Some(10)));
    }

    #[test]
    fn ordinary_text_authority_does_not_imply_result_return_authority() {
        let mut route = route();
        route.allowed_actions = vec![AgentRouteAction::DeliverResult];
        assert_eq!(
            route.validate(None),
            Err(AgentRouteValidationError::InvalidResultReturnPolicy)
        );
        route.disclosure.result_return = AgentResultReturnPolicy::SummaryOnly;
        assert!(route.validate(None).is_ok());
        assert!(
            route
                .disclosure
                .permits_result_format(crate::TaskDeliveryFormat::Summary)
        );
        assert!(
            !route
                .disclosure
                .permits_result_format(crate::TaskDeliveryFormat::FullResult)
        );
    }

    #[test]
    fn routed_authored_input_requires_each_exact_disclosure_class() {
        let input = crate::AgentAuthoredInput::new(vec![
            crate::UserInput::Text {
                text: "hello".to_owned(),
                text_elements: Vec::new(),
            },
            crate::UserInput::Artifact {
                artifact_id: "artifact-1".to_owned(),
                version_id: Some("version-1".to_owned()),
            },
        ]);
        let mut disclosure = AgentRouteDisclosurePolicy {
            text: true,
            ..Default::default()
        };
        assert!(!disclosure.permits_authored_input(&input));
        disclosure.artifacts = true;
        assert!(disclosure.permits_authored_input(&input));
    }

    #[test]
    fn prepared_route_cannot_authorize_a_write() {
        let mut route = route();
        route.status = AgentRouteStatus::Prepared;
        assert!(!route.permits(AgentRouteAction::SendMessage, None));
    }

    #[test]
    fn route_rejects_cross_workspace_and_gateway() {
        let mut route = route();
        route.destination_workspace_id = "W12345678901234567891".to_owned();
        assert_eq!(
            route.validate(None),
            Err(AgentRouteValidationError::CrossWorkspace)
        );
        route.destination_workspace_id = route.source_workspace_id.clone();
        route.destination_gateway_id = "G12345678901234567891".to_owned();
        assert_eq!(
            route.validate(None),
            Err(AgentRouteValidationError::CrossGateway)
        );
    }

    #[test]
    fn route_graph_checks_the_longest_dag_path() {
        let edges = vec![
            ("a".to_owned(), "d".to_owned()),
            ("a".to_owned(), "b".to_owned()),
            ("b".to_owned(), "c".to_owned()),
            ("c".to_owned(), "d".to_owned()),
            ("d".to_owned(), "e".to_owned()),
        ];
        assert_eq!(
            validate_agent_route_graph(&edges, 8, 3),
            Err(AgentRouteGraphValidationError::HopBoundary)
        );
        assert!(validate_agent_route_graph(&edges, 8, 4).is_ok());
    }

    #[test]
    fn prepared_route_must_not_be_created_already_expired() {
        let mut route = route();
        route.status = AgentRouteStatus::Prepared;
        route.expires_at = Some(10);
        assert_eq!(
            route.validate(Some(10)),
            Err(AgentRouteValidationError::InvalidExpiry)
        );
    }
}
