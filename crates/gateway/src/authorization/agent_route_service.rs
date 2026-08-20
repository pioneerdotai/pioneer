//! Composite authorization for cross-capsule agent routes.
//!
//! Route presence is never sufficient for a write. This module keeps source
//! export, route action subset, destination policy, disclosure and generation
//! checks in one side-effect-free decision so handlers cannot accidentally
//! implement an ambient-access shortcut.

use super::{AgentRouteFacts, AgentSecurityEnvelope};
use pioneer_protocol::{
    AgentActionKind, AgentExecutionId, AgentExecutionProfileId, AgentIdentityId, AgentRouteKind,
    AgentStartTarget,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouteDenyReason {
    NotFoundOrNotAuthorized,
    SourceExport,
    RouteAction,
    DestinationPolicy,
    Disclosure,
    Generation,
    IdentityConstraint,
    ProfileConstraint,
    HopBoundary,
    TargetMismatch,
}

impl RouteDenyReason {
    /// Deliberately stable and non-disclosing: private target existence and
    /// route state are not distinguishable to an unauthorized caller.
    pub(crate) const fn public_message(self) -> &'static str {
        "routed agent operation is not authorized"
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RouteAuthorizationRequest<'a> {
    pub(crate) route: &'a AgentRouteFacts,
    pub(crate) source: &'a AgentSecurityEnvelope,
    pub(crate) source_execution_id: &'a AgentExecutionId,
    pub(crate) action: AgentActionKind,
    pub(crate) target: &'a AgentStartTarget,
    pub(crate) source_export_allowed: bool,
    pub(crate) destination_action_allowed: bool,
    pub(crate) disclosure_allowed: bool,
    pub(crate) source_policy_generation: u64,
    pub(crate) destination_policy_generation: u64,
    pub(crate) requested_identity: Option<&'a AgentIdentityId>,
    pub(crate) requested_profile: Option<&'a AgentExecutionProfileId>,
    pub(crate) now_millis: Option<i64>,
}

pub(crate) fn authorize_route(
    request: &RouteAuthorizationRequest<'_>,
) -> Result<(), RouteDenyReason> {
    let route = request.route;
    if !route.active || !route.same_workspace || !route.same_gateway {
        return Err(RouteDenyReason::NotFoundOrNotAuthorized);
    }
    if route
        .expires_at_millis
        .is_some_and(|expires_at| request.now_millis.is_some_and(|now| expires_at <= now))
    {
        return Err(RouteDenyReason::NotFoundOrNotAuthorized);
    }
    if !request.source_export_allowed {
        return Err(RouteDenyReason::SourceExport);
    }
    if route.source_capsule_id != request.source.root_capsule_id {
        return Err(RouteDenyReason::NotFoundOrNotAuthorized);
    }
    if !route.permits_action(request.action) {
        return Err(RouteDenyReason::RouteAction);
    }
    if !request.destination_action_allowed {
        return Err(RouteDenyReason::DestinationPolicy);
    }
    if !request.disclosure_allowed || !route.disclosure.allows_anything() {
        return Err(RouteDenyReason::Disclosure);
    }
    if !route.generations_match(
        request.source_policy_generation,
        request.destination_policy_generation,
    ) {
        return Err(RouteDenyReason::Generation);
    }
    match route.kind {
        AgentRouteKind::ExecutionBound
            if route.source_execution_id != request.source_execution_id.as_str() =>
        {
            return Err(RouteDenyReason::NotFoundOrNotAuthorized);
        }
        // Identity-bound routes are capsule policy for the exact source
        // identity. Their creation execution remains audit provenance, not a
        // bearer fence that would make the route unusable by the next
        // occurrence of that same pinned identity.
        AgentRouteKind::IdentityBound => {}
        AgentRouteKind::ExecutionBound => {}
    }
    if route.source_identity_id.as_ref() != Some(&request.source.identity_id) {
        return Err(RouteDenyReason::IdentityConstraint);
    }
    if let Some(identity) = request.requested_identity
        && !route.permits_identity(identity)
    {
        return Err(RouteDenyReason::IdentityConstraint);
    }
    if request.requested_identity.is_none() && route.destination_identity_id.is_some() {
        return Err(RouteDenyReason::IdentityConstraint);
    }
    if let Some(profile) = request.requested_profile
        && !route.permits_profile(profile)
    {
        return Err(RouteDenyReason::ProfileConstraint);
    }
    if request.requested_profile.is_none() && route.destination_profile_id.is_some() {
        return Err(RouteDenyReason::ProfileConstraint);
    }
    if route.hop_count > route.max_hops {
        return Err(RouteDenyReason::HopBoundary);
    }
    if !route.permits_target(request.target) {
        return Err(RouteDenyReason::TargetMismatch);
    }
    Ok(())
}

pub(crate) fn safe_route_receipt(route: &AgentRouteFacts, action: AgentActionKind) -> String {
    let action = match action {
        AgentActionKind::SendMessage => "send_message",
        AgentActionKind::CreateThread => "create_thread",
        AgentActionKind::StartAgent => "start_agent",
        AgentActionKind::CreateTask => "create_task",
        AgentActionKind::ScheduleTask => "schedule_task",
        AgentActionKind::ReviewTaskResult => "review_task_result",
        AgentActionKind::ControlTask => "control_task",
        AgentActionKind::DeliverResult => "deliver_result",
    };
    serde_json::json!({
        "routeId": route.route_id,
        "routeGeneration": route.generation,
        "sourcePolicyGeneration": route.source_policy_generation,
        "destinationPolicyGeneration": route.destination_policy_generation,
        "action": action,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{AgentActionKind, AgentIdentityId, AgentRouteAction, AgentStartTarget};
    use std::collections::BTreeSet;

    fn envelope() -> AgentSecurityEnvelope {
        AgentSecurityEnvelope {
            role_key: "thread_agent".to_owned(),
            root_capsule_id: "T12345678901234567890".to_owned(),
            allowed_actions: BTreeSet::new(),
            identity_id: AgentIdentityId::new("A12345678901234567890").unwrap(),
            policy_generation: 1,
            fingerprint: "F".repeat(64),
        }
    }

    fn route() -> AgentRouteFacts {
        AgentRouteFacts {
            source_execution_id: "E12345678901234567890".to_owned(),
            source_capsule_id: "T12345678901234567890".to_owned(),
            kind: pioneer_protocol::AgentRouteKind::ExecutionBound,
            same_capsule: false,
            destination_thread_id: "T12345678901234567891".to_owned(),
            destination_capsule_id: "T12345678901234567892".to_owned(),
            route_id: "R12345678901234567890".to_owned(),
            active: true,
            disclosure: pioneer_protocol::AgentRouteDisclosurePolicy {
                text: true,
                ..Default::default()
            },
            same_capsule_thread_ids: Vec::new(),
            same_workspace: true,
            same_gateway: true,
            allowed_actions: vec![AgentRouteAction::SendMessage],
            generation: 1,
            source_policy_generation: 1,
            destination_policy_generation: 1,
            source_identity_id: Some(AgentIdentityId::new("A12345678901234567890").unwrap()),
            destination_identity_id: None,
            destination_profile_id: None,
            hop_count: 1,
            max_hops: 8,
            expires_at_millis: None,
            return_route_id: None,
            grant_fingerprint: "G".repeat(64),
        }
    }

    fn request<'a>(
        route: &'a AgentRouteFacts,
        source: &'a AgentSecurityEnvelope,
        source_execution_id: &'a AgentExecutionId,
        target: &'a AgentStartTarget,
    ) -> RouteAuthorizationRequest<'a> {
        RouteAuthorizationRequest {
            route,
            source,
            source_execution_id,
            action: AgentActionKind::SendMessage,
            target,
            source_export_allowed: true,
            destination_action_allowed: true,
            disclosure_allowed: true,
            source_policy_generation: 1,
            destination_policy_generation: 1,
            requested_identity: None,
            requested_profile: None,
            now_millis: None,
        }
    }

    #[test]
    fn route_requires_all_composite_facts() {
        let source = envelope();
        let route = route();
        let execution_id = AgentExecutionId::new("E12345678901234567890").unwrap();
        let target = AgentStartTarget::RoutedThread {
            route_id: pioneer_protocol::AgentDelegationRouteId::new("R12345678901234567890")
                .unwrap(),
            thread_id: "T12345678901234567891".to_owned(),
        };
        assert!(authorize_route(&request(&route, &source, &execution_id, &target)).is_ok());
        let mut denied = request(&route, &source, &execution_id, &target);
        denied.source_export_allowed = false;
        assert_eq!(authorize_route(&denied), Err(RouteDenyReason::SourceExport));
        denied = request(&route, &source, &execution_id, &target);
        denied.destination_action_allowed = false;
        assert_eq!(
            authorize_route(&denied),
            Err(RouteDenyReason::DestinationPolicy)
        );
    }

    #[test]
    fn route_action_subset_blocks_agent_start() {
        let source = envelope();
        let route = route();
        let execution_id = AgentExecutionId::new("E12345678901234567890").unwrap();
        let target = AgentStartTarget::RoutedThread {
            route_id: pioneer_protocol::AgentDelegationRouteId::new("R12345678901234567890")
                .unwrap(),
            thread_id: "T12345678901234567891".to_owned(),
        };
        let mut denied = request(&route, &source, &execution_id, &target);
        denied.action = AgentActionKind::StartAgent;
        assert_eq!(authorize_route(&denied), Err(RouteDenyReason::RouteAction));
    }

    #[test]
    fn identity_route_rebinds_only_within_its_exact_source_capsule() {
        let source = envelope();
        let mut route = route();
        route.kind = pioneer_protocol::AgentRouteKind::IdentityBound;
        let later_execution = AgentExecutionId::new("E12345678901234567891").unwrap();
        let target = AgentStartTarget::RoutedThread {
            route_id: pioneer_protocol::AgentDelegationRouteId::new("R12345678901234567890")
                .unwrap(),
            thread_id: "T12345678901234567891".to_owned(),
        };
        assert!(authorize_route(&request(&route, &source, &later_execution, &target)).is_ok());

        let mut other_capsule = source;
        other_capsule.root_capsule_id = "T12345678901234567893".to_owned();
        assert_eq!(
            authorize_route(&request(&route, &other_capsule, &later_execution, &target,)),
            Err(RouteDenyReason::NotFoundOrNotAuthorized)
        );
    }

    #[test]
    fn cross_capsule_route_accepts_an_authorized_same_workspace_edge() {
        let source = envelope();
        let mut route = route();
        route.same_capsule = false;
        let execution_id = AgentExecutionId::new("E12345678901234567890").unwrap();
        let target = AgentStartTarget::RoutedThread {
            route_id: pioneer_protocol::AgentDelegationRouteId::new("R12345678901234567890")
                .unwrap(),
            thread_id: "T12345678901234567891".to_owned(),
        };
        // Composite route authorization accepts a same-workspace
        // cross-capsule edge and the commit boundary revalidates its durable
        // policy and route generations.
        assert!(authorize_route(&request(&route, &source, &execution_id, &target)).is_ok());
    }

    #[test]
    fn route_denials_have_same_public_message() {
        assert_eq!(
            RouteDenyReason::SourceExport.public_message(),
            RouteDenyReason::NotFoundOrNotAuthorized.public_message()
        );
    }

    #[test]
    fn route_graph_rejects_cycles_and_excessive_hops() {
        let cycle = vec![
            ("A".to_owned(), "B".to_owned()),
            ("B".to_owned(), "A".to_owned()),
        ];
        assert_eq!(
            pioneer_protocol::validate_agent_route_graph(&cycle, 8, 8),
            Err(pioneer_protocol::AgentRouteGraphValidationError::Cycle)
        );
        let long = vec![
            ("A".to_owned(), "B".to_owned()),
            ("B".to_owned(), "C".to_owned()),
        ];
        assert_eq!(
            pioneer_protocol::validate_agent_route_graph(&long, 8, 1),
            Err(pioneer_protocol::AgentRouteGraphValidationError::HopBoundary)
        );
    }

    #[test]
    fn route_receipt_contains_only_safe_provenance() {
        let route = route();
        let receipt = safe_route_receipt(&route, AgentActionKind::SendMessage);
        for forbidden in ["provider", "model", "credential", "session", "prompt"] {
            assert!(!receipt.contains(forbidden));
        }
        assert!(!receipt.contains(route.destination_thread_id.as_str()));
    }
}
