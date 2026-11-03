use crate::quality_gate::MemoryQualityGateInput;
use pioneer_protocol::{
    MemoryOwnershipClass, MemoryQualityAction, MemoryQualityDecision, MemorySemanticWriteRoute,
    MemoryWriteRelation,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemoryOwnershipRouteInput {
    pub action: MemoryQualityAction,
    pub target_ownership: MemoryOwnershipClass,
    pub ownership_class: MemoryOwnershipClass,
    pub relation: MemoryWriteRelation,
    pub canonical_key: Option<String>,
    pub thread_id: Option<String>,
    pub source_turn_id: Option<String>,
    pub source_item_id: Option<String>,
}

impl MemoryOwnershipRouteInput {
    pub(crate) fn from_quality(
        quality_input: &MemoryQualityGateInput,
        quality_decision: &MemoryQualityDecision,
    ) -> Self {
        Self {
            action: quality_decision.action,
            target_ownership: quality_decision.target_ownership,
            ownership_class: quality_input.ownership_class,
            relation: quality_input.relation,
            canonical_key: quality_input.canonical_key.clone(),
            thread_id: quality_input.source_thread_id.clone(),
            source_turn_id: quality_input.source_turn_id.clone(),
            source_item_id: quality_input.source_item_id.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemoryOwnershipRouteDestination {
    DurableControlPlane,
    ThreadEpisodic,
    TaskState,
    DomainState,
    AuditOnly,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemoryOwnershipRouteReason {
    DurableCandidate,
    ThreadEpisodic,
    TaskState,
    DomainState,
    QuarantineAuditOnly,
    ForceRejected,
    InvalidCandidateOwnership,
    InvalidRouteOwnership,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemoryOwnershipRoute {
    pub destination: MemoryOwnershipRouteDestination,
    pub target_ownership: MemoryOwnershipClass,
    pub reason: MemoryOwnershipRouteReason,
    pub relation: MemoryWriteRelation,
    pub canonical_key: Option<String>,
    pub thread_id: Option<String>,
    pub source_turn_id: Option<String>,
    pub source_item_id: Option<String>,
}

impl MemoryOwnershipRoute {
    pub(crate) fn permits_durable_control_plane(&self) -> bool {
        self.destination == MemoryOwnershipRouteDestination::DurableControlPlane
    }

    pub(crate) fn permits_candidate_policy(&self) -> bool {
        self.permits_durable_control_plane()
    }

    pub(crate) fn is_terminal_non_memory_route(&self) -> bool {
        !self.permits_durable_control_plane()
    }

    pub(crate) fn semantic_write_route(&self) -> MemorySemanticWriteRoute {
        match self.destination {
            MemoryOwnershipRouteDestination::DurableControlPlane => {
                MemorySemanticWriteRoute::DurableControlPlane
            }
            MemoryOwnershipRouteDestination::ThreadEpisodic => {
                MemorySemanticWriteRoute::ThreadEpisodicDeferred
            }
            MemoryOwnershipRouteDestination::TaskState => {
                MemorySemanticWriteRoute::TaskStateDeferred
            }
            MemoryOwnershipRouteDestination::DomainState => {
                MemorySemanticWriteRoute::DomainStateDeferred
            }
            MemoryOwnershipRouteDestination::AuditOnly => MemorySemanticWriteRoute::AuditOnly,
            MemoryOwnershipRouteDestination::Rejected => MemorySemanticWriteRoute::Rejected,
        }
    }
}

pub(crate) fn resolve_memory_ownership_route(
    input: MemoryOwnershipRouteInput,
) -> MemoryOwnershipRoute {
    let (destination, reason) = match input.action {
        MemoryQualityAction::CandidatePolicy => {
            if is_durable_memory_ownership(input.target_ownership)
                && input.target_ownership == input.ownership_class
            {
                (
                    MemoryOwnershipRouteDestination::DurableControlPlane,
                    MemoryOwnershipRouteReason::DurableCandidate,
                )
            } else {
                (
                    MemoryOwnershipRouteDestination::AuditOnly,
                    MemoryOwnershipRouteReason::InvalidCandidateOwnership,
                )
            }
        }
        MemoryQualityAction::RouteToThreadEpisodic => {
            if input.target_ownership == MemoryOwnershipClass::ThreadEpisodicContext
                && input.ownership_class == MemoryOwnershipClass::ThreadEpisodicContext
            {
                (
                    MemoryOwnershipRouteDestination::ThreadEpisodic,
                    MemoryOwnershipRouteReason::ThreadEpisodic,
                )
            } else {
                (
                    MemoryOwnershipRouteDestination::AuditOnly,
                    MemoryOwnershipRouteReason::InvalidRouteOwnership,
                )
            }
        }
        MemoryQualityAction::RouteToTaskState => {
            if input.target_ownership == MemoryOwnershipClass::TaskRuntimeState
                && input.ownership_class == MemoryOwnershipClass::TaskRuntimeState
            {
                (
                    MemoryOwnershipRouteDestination::TaskState,
                    MemoryOwnershipRouteReason::TaskState,
                )
            } else {
                (
                    MemoryOwnershipRouteDestination::AuditOnly,
                    MemoryOwnershipRouteReason::InvalidRouteOwnership,
                )
            }
        }
        MemoryQualityAction::RouteToDomainState => {
            if input.target_ownership == MemoryOwnershipClass::DomainRuntimeState
                && input.ownership_class == MemoryOwnershipClass::DomainRuntimeState
            {
                (
                    MemoryOwnershipRouteDestination::DomainState,
                    MemoryOwnershipRouteReason::DomainState,
                )
            } else {
                (
                    MemoryOwnershipRouteDestination::AuditOnly,
                    MemoryOwnershipRouteReason::InvalidRouteOwnership,
                )
            }
        }
        MemoryQualityAction::Quarantine => (
            MemoryOwnershipRouteDestination::AuditOnly,
            MemoryOwnershipRouteReason::QuarantineAuditOnly,
        ),
        MemoryQualityAction::ForceReject => (
            MemoryOwnershipRouteDestination::Rejected,
            MemoryOwnershipRouteReason::ForceRejected,
        ),
    };

    MemoryOwnershipRoute {
        destination,
        target_ownership: input.target_ownership,
        reason,
        relation: input.relation,
        canonical_key: input.canonical_key,
        thread_id: input.thread_id,
        source_turn_id: input.source_turn_id,
        source_item_id: input.source_item_id,
    }
}

fn is_durable_memory_ownership(ownership_class: MemoryOwnershipClass) -> bool {
    matches!(
        ownership_class,
        MemoryOwnershipClass::DurableUserMemory
            | MemoryOwnershipClass::DurableWorkspaceMemory
            | MemoryOwnershipClass::DurableAgentMemory
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route_input(
        action: MemoryQualityAction,
        target_ownership: MemoryOwnershipClass,
        ownership_class: MemoryOwnershipClass,
    ) -> MemoryOwnershipRouteInput {
        MemoryOwnershipRouteInput {
            action,
            target_ownership,
            ownership_class,
            relation: MemoryWriteRelation::Novel,
            canonical_key: Some("identity:name".to_owned()),
            thread_id: Some("thread-1".to_owned()),
            source_turn_id: Some("turn-1".to_owned()),
            source_item_id: Some("item-1".to_owned()),
        }
    }

    #[test]
    fn ownership_route_decision_matrix() {
        use MemoryOwnershipClass as Ownership;
        use MemoryOwnershipRouteDestination as Destination;
        use MemoryOwnershipRouteReason as Reason;
        use MemoryQualityAction as Action;
        use MemorySemanticWriteRoute as Semantic;
        let cases = [
            (
                Action::CandidatePolicy,
                Ownership::DurableUserMemory,
                Ownership::DurableUserMemory,
                Destination::DurableControlPlane,
                Reason::DurableCandidate,
                Semantic::DurableControlPlane,
                true,
            ),
            (
                Action::CandidatePolicy,
                Ownership::DurableWorkspaceMemory,
                Ownership::DurableWorkspaceMemory,
                Destination::DurableControlPlane,
                Reason::DurableCandidate,
                Semantic::DurableControlPlane,
                true,
            ),
            (
                Action::CandidatePolicy,
                Ownership::DurableAgentMemory,
                Ownership::DurableAgentMemory,
                Destination::DurableControlPlane,
                Reason::DurableCandidate,
                Semantic::DurableControlPlane,
                true,
            ),
            (
                Action::RouteToThreadEpisodic,
                Ownership::ThreadEpisodicContext,
                Ownership::ThreadEpisodicContext,
                Destination::ThreadEpisodic,
                Reason::ThreadEpisodic,
                Semantic::ThreadEpisodicDeferred,
                false,
            ),
            (
                Action::RouteToTaskState,
                Ownership::TaskRuntimeState,
                Ownership::TaskRuntimeState,
                Destination::TaskState,
                Reason::TaskState,
                Semantic::TaskStateDeferred,
                false,
            ),
            (
                Action::RouteToDomainState,
                Ownership::DomainRuntimeState,
                Ownership::DomainRuntimeState,
                Destination::DomainState,
                Reason::DomainState,
                Semantic::DomainStateDeferred,
                false,
            ),
            (
                Action::Quarantine,
                Ownership::AuditOnly,
                Ownership::AuditOnly,
                Destination::AuditOnly,
                Reason::QuarantineAuditOnly,
                Semantic::AuditOnly,
                false,
            ),
            (
                Action::ForceReject,
                Ownership::Reject,
                Ownership::Reject,
                Destination::Rejected,
                Reason::ForceRejected,
                Semantic::Rejected,
                false,
            ),
            (
                Action::CandidatePolicy,
                Ownership::AuditOnly,
                Ownership::AuditOnly,
                Destination::AuditOnly,
                Reason::InvalidCandidateOwnership,
                Semantic::AuditOnly,
                false,
            ),
            (
                Action::RouteToThreadEpisodic,
                Ownership::ThreadEpisodicContext,
                Ownership::DurableUserMemory,
                Destination::AuditOnly,
                Reason::InvalidRouteOwnership,
                Semantic::AuditOnly,
                false,
            ),
        ];
        for (action, target, owning, destination, reason, semantic, permits_candidate) in cases {
            let route = resolve_memory_ownership_route(route_input(action, target, owning));
            assert_eq!(
                route.destination, destination,
                "{action:?}, {target:?}, {owning:?}"
            );
            assert_eq!(route.reason, reason);
            assert_eq!(route.semantic_write_route(), semantic);
            assert_eq!(route.permits_candidate_policy(), permits_candidate);
            assert_eq!(route.is_terminal_non_memory_route(), !permits_candidate);
            assert_eq!(route.thread_id.as_deref(), Some("thread-1"));
            assert_eq!(route.source_turn_id.as_deref(), Some("turn-1"));
            assert_eq!(route.source_item_id.as_deref(), Some("item-1"));
        }
    }
}
