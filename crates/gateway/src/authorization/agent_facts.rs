//! Subject-aware authorization facts for agent domain.
//!
//! This module is intentionally pure.  It translates durable identity and
//! route facts into bounded authorization/options projections; it never
//! looks at permits, queue depth, provider credentials or UI state.

use pioneer_protocol::{
    AgentActionKind, AgentDelegationRouteProjection, AgentExecutionProfileId,
    AgentExecutionProfileProjection, AgentIdentityId, AgentIdentityProjection, AgentIdentityStatus,
    AgentLaunchSelection, AgentRouteAction, AgentRouteKind, AgentStartTarget,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

use super::{ResourceAction, RoleDefinitionRegistry};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AgentRouteFacts {
    pub(crate) source_execution_id: String,
    pub(crate) source_capsule_id: String,
    pub(crate) kind: AgentRouteKind,
    pub(crate) same_capsule: bool,
    pub(crate) destination_thread_id: String,
    pub(crate) destination_capsule_id: String,
    pub(crate) route_id: String,
    pub(crate) active: bool,
    pub(crate) disclosure: pioneer_protocol::AgentRouteDisclosurePolicy,
    pub(crate) same_capsule_thread_ids: Vec<String>,
    pub(crate) same_workspace: bool,
    pub(crate) same_gateway: bool,
    pub(crate) allowed_actions: Vec<AgentRouteAction>,
    pub(crate) generation: u64,
    pub(crate) source_policy_generation: u64,
    pub(crate) destination_policy_generation: u64,
    pub(crate) source_identity_id: Option<AgentIdentityId>,
    pub(crate) destination_identity_id: Option<AgentIdentityId>,
    pub(crate) destination_profile_id: Option<AgentExecutionProfileId>,
    pub(crate) hop_count: u16,
    pub(crate) max_hops: u16,
    pub(crate) expires_at_millis: Option<i64>,
    pub(crate) return_route_id: Option<String>,
    pub(crate) grant_fingerprint: String,
}

impl AgentRouteFacts {
    pub(crate) fn from_projection(
        projection: &AgentDelegationRouteProjection,
    ) -> Result<Self, &'static str> {
        projection
            .validate(None)
            .map_err(|_| "route projection is invalid")?;
        let same_capsule = projection.source_capsule_id == projection.destination_capsule_id;
        Ok(Self {
            source_execution_id: projection.source_execution_id.to_string(),
            source_capsule_id: projection.source_capsule_id.clone(),
            kind: projection.kind,
            same_capsule,
            destination_thread_id: projection.destination_thread_id.clone(),
            destination_capsule_id: projection.destination_capsule_id.clone(),
            route_id: projection.id.to_string(),
            active: projection.status.is_live(),
            disclosure: projection.disclosure,
            // Same-capsule targets come only from exact durable lineage in the
            // runtime adapter. Delegation routes are authority edges between
            // different collaboration capsules and never alias the root.
            same_capsule_thread_ids: Vec::new(),
            same_workspace: projection.source_workspace_id == projection.destination_workspace_id,
            same_gateway: projection.source_gateway_id == projection.destination_gateway_id,
            allowed_actions: projection.allowed_actions.clone(),
            generation: projection.generation,
            source_policy_generation: projection.source_policy_generation,
            destination_policy_generation: projection.destination_policy_generation,
            source_identity_id: Some(projection.source_agent_identity_id.clone()),
            destination_identity_id: projection.destination_agent_identity_id.clone(),
            destination_profile_id: projection
                .destination_profile_id
                .as_deref()
                .map(|id| AgentExecutionProfileId::new(id.to_owned()))
                .transpose()
                .map_err(|_| "route projection has invalid destination profile")?,
            hop_count: projection.hop_count,
            max_hops: projection.max_hops,
            expires_at_millis: projection.expires_at,
            return_route_id: projection.return_route_id.as_ref().map(ToString::to_string),
            grant_fingerprint: projection.grant_fingerprint.clone(),
        })
    }

    pub(crate) fn permits_target(&self, target: &AgentStartTarget) -> bool {
        match target {
            AgentStartTarget::CurrentThread => true,
            AgentStartTarget::SameCapsuleThread { thread_id } => {
                self.active
                    && self.disclosure.allows_anything()
                    && self
                        .same_capsule_thread_ids
                        .iter()
                        .any(|allowed| allowed == thread_id)
            }
            AgentStartTarget::RoutedThread {
                route_id,
                thread_id,
            } => {
                self.active
                    && self.disclosure.allows_anything()
                    && route_id.as_str() == self.route_id
                    && thread_id == &self.destination_thread_id
            }
        }
    }

    pub(crate) fn permits_action(&self, action: AgentActionKind) -> bool {
        let required = match action {
            AgentActionKind::SendMessage => AgentRouteAction::SendMessage,
            AgentActionKind::StartAgent => AgentRouteAction::StartAgent,
            AgentActionKind::CreateTask => AgentRouteAction::CreateTask,
            AgentActionKind::ScheduleTask => AgentRouteAction::ScheduleTask,
            AgentActionKind::ReviewTaskResult => AgentRouteAction::ReviewTaskResult,
            AgentActionKind::DeliverResult => AgentRouteAction::DeliverResult,
            AgentActionKind::CreateThread | AgentActionKind::ControlTask => return false,
        };
        self.active
            && self.same_workspace
            && self.same_gateway
            && self.hop_count <= self.max_hops
            && self.allowed_actions.contains(&required)
    }

    pub(crate) fn generations_match(
        &self,
        source_policy_generation: u64,
        destination_policy_generation: u64,
    ) -> bool {
        self.generation > 0
            && self.source_policy_generation == source_policy_generation
            && self.destination_policy_generation == destination_policy_generation
    }

    pub(crate) fn permits_identity(&self, identity_id: &AgentIdentityId) -> bool {
        self.destination_identity_id
            .as_ref()
            .is_none_or(|allowed| allowed == identity_id)
    }

    pub(crate) fn permits_profile(&self, profile_id: &AgentExecutionProfileId) -> bool {
        self.destination_profile_id
            .as_ref()
            .is_none_or(|allowed| allowed == profile_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AgentSecurityEnvelope {
    pub(crate) role_key: String,
    pub(crate) root_capsule_id: String,
    pub(crate) allowed_actions: BTreeSet<ResourceAction>,
    pub(crate) identity_id: AgentIdentityId,
    pub(crate) policy_generation: u64,
    pub(crate) fingerprint: String,
}

impl AgentSecurityEnvelope {
    pub(crate) fn allows(&self, action: ResourceAction) -> bool {
        self.allowed_actions.contains(&action)
    }

    pub(crate) fn allowed_action_names(&self) -> Vec<String> {
        let mut names = self
            .allowed_actions
            .iter()
            .map(|action| action.safe_name().to_owned())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    pub(crate) fn constrain_to_persisted_actions(
        mut self,
        action_names: &[String],
        persisted_policy_generation: u64,
        expected_fingerprint: &str,
    ) -> Option<Self> {
        if action_names.is_empty() || persisted_policy_generation == 0 {
            return None;
        }
        let mut persisted_actions = BTreeSet::new();
        for name in action_names {
            let action = ResourceAction::ALL
                .into_iter()
                .find(|action| action.safe_name() == name)?;
            if !persisted_actions.insert(action) {
                return None;
            }
        }
        let persisted_fingerprint = agent_security_envelope_fingerprint(
            &self.identity_id,
            self.role_key.as_str(),
            self.root_capsule_id.as_str(),
            persisted_policy_generation,
            &persisted_actions,
        );
        if persisted_fingerprint != expected_fingerprint {
            return None;
        }
        self.allowed_actions = self
            .allowed_actions
            .intersection(&persisted_actions)
            .copied()
            .collect();
        if self.allowed_actions.is_empty() {
            return None;
        }
        self.fingerprint = agent_security_envelope_fingerprint(
            &self.identity_id,
            self.role_key.as_str(),
            self.root_capsule_id.as_str(),
            self.policy_generation,
            &self.allowed_actions,
        );
        Some(self)
    }
}

fn agent_security_envelope_fingerprint(
    identity_id: &AgentIdentityId,
    role_key: &str,
    root_capsule_id: &str,
    policy_generation: u64,
    allowed_actions: &BTreeSet<ResourceAction>,
) -> String {
    let mut digest = Sha256::new();
    digest.update(identity_id.as_str().as_bytes());
    digest.update(role_key.as_bytes());
    digest.update(root_capsule_id.as_bytes());
    digest.update(policy_generation.to_be_bytes());
    let mut action_names = allowed_actions
        .iter()
        .map(|action| action.safe_name())
        .collect::<Vec<_>>();
    action_names.sort_unstable();
    for action_name in action_names {
        digest.update(action_name.as_bytes());
        digest.update([0]);
    }
    hex::encode(digest.finalize())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AgentStartFacts {
    pub(crate) target: AgentStartTarget,
    pub(crate) route: Option<AgentRouteFacts>,
    pub(crate) envelope: AgentSecurityEnvelope,
    pub(crate) inherited_profile: Option<AgentExecutionProfileId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AgentWorkResourcePolicy {
    /// Server-owned resource governance.  This value is never passed to an
    /// allow/deny decision and is not serialized into child configuration.
    pub(crate) max_concurrency: u32,
    pub(crate) max_queue_depth: u32,
    pub(crate) max_depth: u16,
    pub(crate) max_fan_out: u16,
    pub(crate) max_total_nodes: u32,
    /// Server-owned liveness windows. These are resource-governance facts,
    /// never user-provided authority or recursively shrinking child budgets.
    pub(crate) idle_timeout_secs: u64,
    pub(crate) hard_timeout_secs: u64,
}

impl Default for AgentWorkResourcePolicy {
    fn default() -> Self {
        let timeout = pioneer_config::GatewayProviderStreamItemTimeoutConfig::default();
        Self {
            max_concurrency: 16,
            max_queue_depth: 2048,
            max_depth: 64,
            max_fan_out: 128,
            max_total_nodes: 4_096,
            idle_timeout_secs: timeout.idle_secs,
            hard_timeout_secs: timeout.hard_secs,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AgentAuthorizationFacts {
    pub(crate) identity_id: AgentIdentityId,
    pub(crate) identity_status: AgentIdentityStatus,
    pub(crate) role_key: String,
    pub(crate) root_capsule_id: String,
    pub(crate) parent_envelope: Option<AgentSecurityEnvelope>,
    pub(crate) policy_generation: u64,
}

impl AgentAuthorizationFacts {
    pub(crate) fn derive_envelope(
        &self,
        registry: RoleDefinitionRegistry,
    ) -> Option<AgentSecurityEnvelope> {
        if self.identity_status != AgentIdentityStatus::Active {
            return None;
        }
        let role = registry.resolve_agent_role(&self.role_key)?;
        let allowed_actions = ResourceAction::ALL
            .into_iter()
            .filter(|action| role.actions.allows(*action))
            .filter(|action| {
                self.parent_envelope
                    .as_ref()
                    .is_none_or(|parent| parent.allows(*action))
            })
            .collect::<BTreeSet<_>>();
        let fingerprint = agent_security_envelope_fingerprint(
            &self.identity_id,
            self.role_key.as_str(),
            self.root_capsule_id.as_str(),
            self.policy_generation,
            &allowed_actions,
        );
        Some(AgentSecurityEnvelope {
            role_key: self.role_key.clone(),
            root_capsule_id: self.root_capsule_id.clone(),
            allowed_actions,
            identity_id: self.identity_id.clone(),
            policy_generation: self.policy_generation,
            fingerprint,
        })
    }
}

pub(crate) fn project_bounded_start_options(
    identities: Vec<AgentIdentityProjection>,
    profiles: Vec<AgentExecutionProfileProjection>,
    envelope: &AgentSecurityEnvelope,
    policy_generation: u64,
) -> pioneer_protocol::AgentStartOptionsProjection {
    let mut visible_identities = identities;
    visible_identities.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
    let visible_identity_ids = visible_identities
        .iter()
        .map(|identity| identity.id.clone())
        .collect::<BTreeSet<_>>();
    let visible_profiles = profiles
        .into_iter()
        .filter(|profile| {
            profile.policy_generation <= policy_generation
                && profile
                    .compatible_agent_identity_ids
                    .iter()
                    .any(|id| visible_identity_ids.contains(id))
        })
        .collect::<Vec<_>>();
    let mut digest = Sha256::new();
    digest.update(envelope.fingerprint.as_bytes());
    digest.update(policy_generation.to_be_bytes());
    for identity in &visible_identities {
        digest.update(identity.id.as_str().as_bytes());
        digest.update(identity.source_fingerprint.as_bytes());
    }
    for profile in &visible_profiles {
        digest.update(profile.id.as_str().as_bytes());
        digest.update(profile.fingerprint.as_bytes());
    }
    pioneer_protocol::AgentStartOptionsProjection {
        agents: visible_identities,
        inherit_parent_agent_available: true,
        derived_ephemeral_available: envelope.allows(ResourceAction::ChildStart),
        profiles: visible_profiles,
        inherit_parent_profile_available: true,
        allowed_skill_ids: Vec::new(),
        allowed_mcp_server_ids: Vec::new(),
        max_permission_profile: pioneer_protocol::task_permission_cap_for_mode(
            pioneer_protocol::TurnPermissionMode::Supervised,
        ),
        generation_fingerprint: hex::encode(digest.finalize()),
    }
}

pub(crate) fn validate_agent_start_facts(
    selection: &AgentLaunchSelection,
    facts: &AgentStartFacts,
) -> Result<(), &'static str> {
    if !facts.envelope.allows(ResourceAction::ChildStart) {
        return Err("agent execution is not allowed to start children");
    }
    if let Some(route) = &facts.route {
        if !route.permits_target(&facts.target) {
            return Err("agent target is not permitted by the durable route");
        }
    } else if matches!(facts.target, AgentStartTarget::RoutedThread { .. }) {
        return Err("routed target requires an active durable route");
    }
    selection
        .validate(facts.inherited_profile.as_ref())
        .map_err(|_| "an exact execution profile is required")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{AgentIdentityId, AgentIdentityStatus};

    fn security_decision_is_capacity_independent(
        envelope: &AgentSecurityEnvelope,
        action: ResourceAction,
        _resource_policy: &AgentWorkResourcePolicy,
        _resource_state: &str,
    ) -> bool {
        envelope.allows(action)
    }

    fn envelope() -> AgentSecurityEnvelope {
        let facts = AgentAuthorizationFacts {
            identity_id: AgentIdentityId::new("A12345678901234567890").unwrap(),
            identity_status: AgentIdentityStatus::Active,
            role_key: "thread_agent".to_owned(),
            root_capsule_id: "T12345678901234567890".to_owned(),
            parent_envelope: None,
            policy_generation: 1,
        };
        facts
            .derive_envelope(RoleDefinitionRegistry::new())
            .expect("thread agent role is registered")
    }

    #[test]
    fn capacity_state_does_not_change_security() {
        let envelope = envelope();
        let policy = AgentWorkResourcePolicy::default();
        let action = ResourceAction::ChildStart;
        assert_eq!(
            security_decision_is_capacity_independent(&envelope, action, &policy, "available"),
            security_decision_is_capacity_independent(&envelope, action, &policy, "queued")
        );
        assert_eq!(
            security_decision_is_capacity_independent(&envelope, action, &policy, "saturated"),
            true
        );
    }

    #[test]
    fn child_envelope_cannot_widen_parent() {
        let mut parent = envelope();
        parent
            .allowed_actions
            .remove(&ResourceAction::AgentRouteCreate);
        let facts = AgentAuthorizationFacts {
            identity_id: AgentIdentityId::new("A12345678901234567891").unwrap(),
            identity_status: AgentIdentityStatus::Active,
            role_key: "thread_agent".to_owned(),
            root_capsule_id: "T12345678901234567890".to_owned(),
            parent_envelope: Some(parent),
            policy_generation: 1,
        };
        let child = facts
            .derive_envelope(RoleDefinitionRegistry::new())
            .unwrap();
        assert!(!child.allows(ResourceAction::AgentRouteCreate));
    }

    #[test]
    fn persisted_action_ceiling_is_nonempty_and_fingerprint_exact() {
        let current = envelope();
        let action_names = current
            .allowed_actions
            .iter()
            .map(|action| action.safe_name().to_owned())
            .collect::<Vec<_>>();
        assert!(
            current
                .clone()
                .constrain_to_persisted_actions(
                    &action_names,
                    current.policy_generation,
                    &current.fingerprint,
                )
                .is_some()
        );
        assert!(
            current
                .clone()
                .constrain_to_persisted_actions(
                    &[],
                    current.policy_generation,
                    &current.fingerprint,
                )
                .is_none()
        );
        assert!(
            current
                .clone()
                .constrain_to_persisted_actions(
                    &action_names[..action_names.len().saturating_sub(1)],
                    current.policy_generation,
                    &current.fingerprint,
                )
                .is_none()
        );
        assert!(
            current
                .clone()
                .constrain_to_persisted_actions(
                    &action_names,
                    current.policy_generation,
                    &"0".repeat(64),
                )
                .is_none()
        );

        let original = envelope();
        let mut narrowed = original.clone();
        let removed = *narrowed.allowed_actions.iter().next().unwrap();
        narrowed.allowed_actions.remove(&removed);
        narrowed.policy_generation += 1;
        narrowed.fingerprint = agent_security_envelope_fingerprint(
            &narrowed.identity_id,
            narrowed.role_key.as_str(),
            narrowed.root_capsule_id.as_str(),
            narrowed.policy_generation,
            &narrowed.allowed_actions,
        );
        let effective = narrowed
            .constrain_to_persisted_actions(
                &action_names,
                original.policy_generation,
                &original.fingerprint,
            )
            .unwrap();
        assert!(!effective.allowed_actions.contains(&removed));
        assert_eq!(effective.policy_generation, original.policy_generation + 1);
    }

    #[test]
    fn routed_target_requires_active_disclosure_grant_and_exact_destination() {
        let route_id = "R12345678901234567890".to_owned();
        let target = AgentStartTarget::RoutedThread {
            route_id: pioneer_protocol::AgentDelegationRouteId::new(route_id.clone()).unwrap(),
            thread_id: "T12345678901234567891".to_owned(),
        };
        let route = AgentRouteFacts {
            source_execution_id: "E12345678901234567890".to_owned(),
            source_capsule_id: "T12345678901234567890".to_owned(),
            kind: pioneer_protocol::AgentRouteKind::ExecutionBound,
            same_capsule: false,
            destination_thread_id: "T12345678901234567891".to_owned(),
            destination_capsule_id: "T12345678901234567892".to_owned(),
            route_id,
            active: true,
            disclosure: pioneer_protocol::AgentRouteDisclosurePolicy::default(),
            same_capsule_thread_ids: Vec::new(),
            same_workspace: true,
            same_gateway: true,
            allowed_actions: vec![pioneer_protocol::AgentRouteAction::StartAgent],
            generation: 1,
            source_policy_generation: 1,
            destination_policy_generation: 1,
            source_identity_id: None,
            destination_identity_id: None,
            destination_profile_id: None,
            hop_count: 1,
            max_hops: 8,
            expires_at_millis: None,
            return_route_id: None,
            grant_fingerprint: "a".repeat(64),
        };
        assert!(!route.permits_target(&target));
        let mut disclosed = route;
        disclosed.disclosure.text = true;
        assert!(disclosed.permits_target(&target));
    }
}
