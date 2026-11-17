use crate::extractor_ontology::{
    MemoryExtractorOntologyProposal, MemoryExtractorOntologyProposalComparison,
    extractor_ontology_proposal_from_metadata,
};
use crate::quality::{MemoryOntologyClassification, MemorySourceContextClassification};
use pioneer_protocol::{
    MemoryEvidenceClass, MemoryFactClass, MemoryLifetimeClass, MemoryOwnershipClass,
    MemoryQualityAction, MemoryQualityDecision, MemoryQualityReasonCode, MemoryScope,
    MemorySemanticWriteParams, MemorySensitivity, MemorySourceContextKind, MemoryWriteRelation,
};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct MemoryQualityGateInput {
    pub source_context_kind: MemorySourceContextKind,
    pub fact_class: MemoryFactClass,
    pub lifetime_class: MemoryLifetimeClass,
    pub ownership_class: MemoryOwnershipClass,
    pub evidence_class: MemoryEvidenceClass,
    pub sensitivity: MemorySensitivity,
    pub relation: MemoryWriteRelation,
    pub scope: MemoryScope,
    pub canonical_key: Option<String>,
    pub memory_write_disabled_for_turn: bool,
    pub explicit_user_approval: bool,
    pub sensitive_memory_policy_allowed: bool,
    pub source_thread_id: Option<String>,
    pub source_turn_id: Option<String>,
    pub source_item_id: Option<String>,
    pub task_id: Option<String>,
    pub workspace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extractor_ontology_proposal: Option<MemoryExtractorOntologyProposal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extractor_ontology_proposal_comparison: Option<MemoryExtractorOntologyProposalComparison>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MemoryQualityGate;

impl MemoryQualityGate {
    pub(crate) fn decide(input: &MemoryQualityGateInput) -> MemoryQualityDecision {
        if let Some(decision) = hard_reject_decision(input) {
            return decision;
        }
        if let Some(decision) = non_durable_routing_decision(input) {
            return decision;
        }
        if let Some(decision) = durable_candidate_allow_decision(input) {
            return relation_adjusted_candidate_decision(input, decision);
        }
        if let Some(decision) = default_quarantine_decision(input) {
            return decision;
        }

        quarantine(
            MemoryOwnershipClass::AuditOnly,
            vec![MemoryQualityReasonCode::NoQualityAllowRule],
        )
    }
}

fn hard_reject_decision(input: &MemoryQualityGateInput) -> Option<MemoryQualityDecision> {
    if input.memory_write_disabled_for_turn {
        return Some(force_reject(
            MemoryOwnershipClass::Reject,
            vec![MemoryQualityReasonCode::MemoryWriteDisabledForTurn],
        ));
    }

    if input.fact_class == MemoryFactClass::SecretOrCredential {
        return Some(force_reject(
            MemoryOwnershipClass::Reject,
            vec![MemoryQualityReasonCode::SecretOrCredential],
        ));
    }

    if input.fact_class == MemoryFactClass::RegulatedSensitiveFact
        && !input.explicit_user_approval
        && !input.sensitive_memory_policy_allowed
    {
        return Some(force_reject(
            MemoryOwnershipClass::Reject,
            vec![MemoryQualityReasonCode::RegulatedSensitiveWithoutUserApproval],
        ));
    }

    if matches!(
        input.source_context_kind,
        MemorySourceContextKind::SystemRuntime | MemorySourceContextKind::DeveloperInstruction
    ) && input.evidence_class == MemoryEvidenceClass::SystemObservation
        && is_durable_memory_ownership(input.ownership_class)
    {
        return Some(force_reject(
            MemoryOwnershipClass::AuditOnly,
            vec![
                MemoryQualityReasonCode::SystemOwnedStateNotMemory,
                MemoryQualityReasonCode::SourceNotAuthoritativeForDurableMemory,
            ],
        ));
    }

    if input.source_context_kind == MemorySourceContextKind::TaskRuntime
        && input.evidence_class == MemoryEvidenceClass::TaskRuntimeObservation
        && is_durable_memory_ownership(input.ownership_class)
    {
        return Some(force_reject(
            MemoryOwnershipClass::TaskRuntimeState,
            vec![
                MemoryQualityReasonCode::TaskStateNotUserMemory,
                MemoryQualityReasonCode::OwnershipMismatch,
            ],
        ));
    }

    if input.source_context_kind == MemorySourceContextKind::ToolResult
        && input.evidence_class == MemoryEvidenceClass::ToolObservation
        && is_durable_memory_ownership(input.ownership_class)
    {
        return Some(force_reject(
            MemoryOwnershipClass::DomainRuntimeState,
            vec![
                MemoryQualityReasonCode::ToolResultNotUserMemory,
                MemoryQualityReasonCode::OwnershipMismatch,
            ],
        ));
    }

    if input.source_context_kind == MemorySourceContextKind::AssistantResponse
        && input.evidence_class == MemoryEvidenceClass::AssistantInference
        && matches!(
            input.ownership_class,
            MemoryOwnershipClass::DurableUserMemory | MemoryOwnershipClass::DurableWorkspaceMemory
        )
    {
        return Some(force_reject(
            MemoryOwnershipClass::AuditOnly,
            vec![
                MemoryQualityReasonCode::AssistantInferenceNotDurableEvidence,
                MemoryQualityReasonCode::SourceNotAuthoritativeForDurableMemory,
            ],
        ));
    }

    if matches!(
        input.lifetime_class,
        MemoryLifetimeClass::Instantaneous | MemoryLifetimeClass::SessionOnly
    ) && is_durable_memory_ownership(input.ownership_class)
    {
        return Some(force_reject(
            MemoryOwnershipClass::Reject,
            vec![MemoryQualityReasonCode::NonDurableLifetime],
        ));
    }

    if input.evidence_class == MemoryEvidenceClass::MissingOrWeak {
        return Some(force_reject(
            MemoryOwnershipClass::Reject,
            vec![MemoryQualityReasonCode::WeakOrMissingEvidence],
        ));
    }

    if matches!(
        input.relation,
        MemoryWriteRelation::Duplicate | MemoryWriteRelation::SuppressedByRejection
    ) {
        return Some(force_reject(
            MemoryOwnershipClass::AuditOnly,
            vec![MemoryQualityReasonCode::DuplicateExistingMemory],
        ));
    }

    None
}

fn force_reject(
    target_ownership: MemoryOwnershipClass,
    reason_codes: Vec<MemoryQualityReasonCode>,
) -> MemoryQualityDecision {
    MemoryQualityDecision {
        action: MemoryQualityAction::ForceReject,
        target_ownership,
        reason_codes,
        candidate_auto_approve_allowed: false,
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

fn non_durable_routing_decision(input: &MemoryQualityGateInput) -> Option<MemoryQualityDecision> {
    if matches!(
        input.source_context_kind,
        MemorySourceContextKind::DirectUserConversation
            | MemorySourceContextKind::AssistantResponse
            | MemorySourceContextKind::GeneratedSummary
    ) && input.fact_class == MemoryFactClass::ThreadLocalState
        && matches!(
            input.lifetime_class,
            MemoryLifetimeClass::ThreadLifetime | MemoryLifetimeClass::SessionOnly
        )
        && evidence_is_not_weak(input.evidence_class)
    {
        return Some(route(
            MemoryQualityAction::RouteToThreadEpisodic,
            MemoryOwnershipClass::ThreadEpisodicContext,
            vec![
                MemoryQualityReasonCode::RouteThreadEpisodic,
                MemoryQualityReasonCode::NonDurableLifetime,
            ],
        ));
    }

    if matches!(
        input.source_context_kind,
        MemorySourceContextKind::TaskRuntime | MemorySourceContextKind::DirectUserConversation
    ) && input.fact_class == MemoryFactClass::TaskLifecycleState
        && input.lifetime_class == MemoryLifetimeClass::TaskLifetime
        && evidence_is_not_weak(input.evidence_class)
    {
        return Some(route(
            MemoryQualityAction::RouteToTaskState,
            MemoryOwnershipClass::TaskRuntimeState,
            vec![
                MemoryQualityReasonCode::RouteTaskState,
                MemoryQualityReasonCode::TaskLifetime,
            ],
        ));
    }

    if input.source_context_kind == MemorySourceContextKind::ToolResult
        && matches!(
            input.fact_class,
            MemoryFactClass::ToolResultFact | MemoryFactClass::OperationalObservation
        )
        && matches!(
            input.lifetime_class,
            MemoryLifetimeClass::TaskLifetime
                | MemoryLifetimeClass::ThreadLifetime
                | MemoryLifetimeClass::NaturallyExpiring
        )
        && input.evidence_class == MemoryEvidenceClass::ToolObservation
    {
        return Some(route(
            MemoryQualityAction::RouteToDomainState,
            MemoryOwnershipClass::DomainRuntimeState,
            vec![
                MemoryQualityReasonCode::RouteDomainState,
                MemoryQualityReasonCode::ToolOwnedState,
            ],
        ));
    }

    if input.source_context_kind == MemorySourceContextKind::SystemRuntime
        && matches!(
            input.fact_class,
            MemoryFactClass::DomainOwnedState | MemoryFactClass::OperationalObservation
        )
        && !is_durable_lifetime(input.lifetime_class)
        && input.evidence_class == MemoryEvidenceClass::SystemObservation
    {
        return Some(route(
            MemoryQualityAction::RouteToDomainState,
            MemoryOwnershipClass::DomainRuntimeState,
            vec![
                MemoryQualityReasonCode::RouteDomainState,
                MemoryQualityReasonCode::SystemOwnedStateNotMemory,
            ],
        ));
    }

    if input.source_context_kind == MemorySourceContextKind::GeneratedSummary
        && input.fact_class == MemoryFactClass::GeneratedSummaryFact
        && matches!(
            input.lifetime_class,
            MemoryLifetimeClass::ThreadLifetime
                | MemoryLifetimeClass::SessionOnly
                | MemoryLifetimeClass::Unknown
        )
        && input.evidence_class == MemoryEvidenceClass::GeneratedSummary
    {
        return Some(route(
            MemoryQualityAction::RouteToThreadEpisodic,
            MemoryOwnershipClass::ThreadEpisodicContext,
            vec![
                MemoryQualityReasonCode::GeneratedSummaryNotDurableMemory,
                MemoryQualityReasonCode::RouteThreadEpisodic,
            ],
        ));
    }

    None
}

fn default_quarantine_decision(input: &MemoryQualityGateInput) -> Option<MemoryQualityDecision> {
    if input.source_context_kind == MemorySourceContextKind::Unknown {
        return Some(quarantine(
            MemoryOwnershipClass::AuditOnly,
            vec![MemoryQualityReasonCode::UnknownSourceContext],
        ));
    }

    if input.fact_class == MemoryFactClass::Unknown {
        return Some(quarantine(
            MemoryOwnershipClass::AuditOnly,
            vec![MemoryQualityReasonCode::UnknownFactClass],
        ));
    }

    if input.lifetime_class == MemoryLifetimeClass::Unknown
        && is_durable_memory_ownership(input.ownership_class)
    {
        return Some(quarantine(
            MemoryOwnershipClass::AuditOnly,
            vec![MemoryQualityReasonCode::UnknownLifetime],
        ));
    }

    if !ownership_fits_fact_class(input.ownership_class, input.fact_class) {
        return Some(quarantine(
            MemoryOwnershipClass::AuditOnly,
            vec![MemoryQualityReasonCode::OwnershipMismatch],
        ));
    }

    if matches!(
        input.source_context_kind,
        MemorySourceContextKind::ConnectorContent | MemorySourceContextKind::ImportedDocument
    ) {
        return Some(quarantine(
            MemoryOwnershipClass::AuditOnly,
            vec![MemoryQualityReasonCode::SourcePolicyMissing],
        ));
    }

    None
}

fn durable_candidate_allow_decision(
    input: &MemoryQualityGateInput,
) -> Option<MemoryQualityDecision> {
    if input.source_context_kind != MemorySourceContextKind::DirectUserConversation {
        return None;
    }
    if !evidence_is_user_owned(input.evidence_class) {
        return None;
    }

    if input.fact_class == MemoryFactClass::UserIdentity
        && input.lifetime_class == MemoryLifetimeClass::LongLived
        && input.ownership_class == MemoryOwnershipClass::DurableUserMemory
    {
        return Some(candidate_policy(
            input.ownership_class,
            vec![
                MemoryQualityReasonCode::CandidatePolicyAllowed,
                MemoryQualityReasonCode::DurableUserIdentity,
            ],
        ));
    }

    if matches!(
        input.fact_class,
        MemoryFactClass::UserBiography | MemoryFactClass::UserRelationship
    ) && input.lifetime_class == MemoryLifetimeClass::LongLived
        && input.ownership_class == MemoryOwnershipClass::DurableUserMemory
    {
        return Some(candidate_policy(
            input.ownership_class,
            vec![
                MemoryQualityReasonCode::CandidatePolicyAllowed,
                MemoryQualityReasonCode::DurableUserProfile,
            ],
        ));
    }

    if matches!(
        input.fact_class,
        MemoryFactClass::StableUserPreference | MemoryFactClass::CommunicationPreference
    ) && input.lifetime_class == MemoryLifetimeClass::LongLived
        && input.ownership_class == MemoryOwnershipClass::DurableUserMemory
    {
        return Some(candidate_policy(
            input.ownership_class,
            vec![
                MemoryQualityReasonCode::CandidatePolicyAllowed,
                MemoryQualityReasonCode::DurableUserPreference,
            ],
        ));
    }

    if input.fact_class == MemoryFactClass::RecurringUserInstruction
        && input.lifetime_class == MemoryLifetimeClass::LongLived
        && matches!(
            input.ownership_class,
            MemoryOwnershipClass::DurableUserMemory | MemoryOwnershipClass::DurableWorkspaceMemory
        )
    {
        return Some(candidate_policy(
            input.ownership_class,
            vec![
                MemoryQualityReasonCode::CandidatePolicyAllowed,
                MemoryQualityReasonCode::DurableRecurringInstruction,
            ],
        ));
    }

    if matches!(
        input.fact_class,
        MemoryFactClass::ProjectPolicy
            | MemoryFactClass::ProjectDecision
            | MemoryFactClass::ProjectProcedure
            | MemoryFactClass::ProjectConstraint
    ) && matches!(
        input.lifetime_class,
        MemoryLifetimeClass::ProjectLifetime | MemoryLifetimeClass::LongLived
    ) && input.ownership_class == MemoryOwnershipClass::DurableWorkspaceMemory
    {
        return Some(candidate_policy(
            input.ownership_class,
            vec![
                MemoryQualityReasonCode::CandidatePolicyAllowed,
                MemoryQualityReasonCode::DurableProjectMemory,
            ],
        ));
    }

    if input.fact_class == MemoryFactClass::AssistantSelfDescription
        && input.lifetime_class == MemoryLifetimeClass::LongLived
        && input.ownership_class == MemoryOwnershipClass::DurableAgentMemory
    {
        return Some(candidate_policy(
            input.ownership_class,
            vec![
                MemoryQualityReasonCode::CandidatePolicyAllowed,
                MemoryQualityReasonCode::UserConfirmedAgentMemory,
            ],
        ));
    }

    None
}

fn relation_adjusted_candidate_decision(
    input: &MemoryQualityGateInput,
    mut decision: MemoryQualityDecision,
) -> MemoryQualityDecision {
    match input.relation {
        MemoryWriteRelation::Duplicate | MemoryWriteRelation::SuppressedByRejection => {
            force_reject(
                MemoryOwnershipClass::AuditOnly,
                vec![MemoryQualityReasonCode::DuplicateExistingMemory],
            )
        }
        MemoryWriteRelation::CompatibleUpdate => {
            decision
                .reason_codes
                .push(MemoryQualityReasonCode::CompatibleUpdate);
            decision
        }
        MemoryWriteRelation::Contradiction => {
            decision
                .reason_codes
                .push(MemoryQualityReasonCode::ContradictsExistingMemory);
            decision
                .reason_codes
                .push(MemoryQualityReasonCode::RequiresResolution);
            decision.candidate_auto_approve_allowed = false;
            decision
        }
        MemoryWriteRelation::Novel => {
            decision
                .reason_codes
                .push(MemoryQualityReasonCode::NovelCandidate);
            decision
        }
    }
}

fn route(
    action: MemoryQualityAction,
    target_ownership: MemoryOwnershipClass,
    reason_codes: Vec<MemoryQualityReasonCode>,
) -> MemoryQualityDecision {
    MemoryQualityDecision {
        action,
        target_ownership,
        reason_codes,
        candidate_auto_approve_allowed: false,
    }
}

fn quarantine(
    target_ownership: MemoryOwnershipClass,
    reason_codes: Vec<MemoryQualityReasonCode>,
) -> MemoryQualityDecision {
    MemoryQualityDecision {
        action: MemoryQualityAction::Quarantine,
        target_ownership,
        reason_codes,
        candidate_auto_approve_allowed: false,
    }
}

fn candidate_policy(
    target_ownership: MemoryOwnershipClass,
    reason_codes: Vec<MemoryQualityReasonCode>,
) -> MemoryQualityDecision {
    MemoryQualityDecision {
        action: MemoryQualityAction::CandidatePolicy,
        target_ownership,
        reason_codes,
        candidate_auto_approve_allowed: true,
    }
}

fn evidence_is_not_weak(evidence_class: MemoryEvidenceClass) -> bool {
    evidence_class != MemoryEvidenceClass::MissingOrWeak
}

fn evidence_is_user_owned(evidence_class: MemoryEvidenceClass) -> bool {
    matches!(
        evidence_class,
        MemoryEvidenceClass::DirectUserAssertion
            | MemoryEvidenceClass::UserCorrection
            | MemoryEvidenceClass::UserApproval
    )
}

fn is_durable_lifetime(lifetime_class: MemoryLifetimeClass) -> bool {
    matches!(
        lifetime_class,
        MemoryLifetimeClass::LongLived | MemoryLifetimeClass::ProjectLifetime
    )
}

fn ownership_fits_fact_class(
    ownership_class: MemoryOwnershipClass,
    fact_class: MemoryFactClass,
) -> bool {
    match ownership_class {
        MemoryOwnershipClass::DurableUserMemory => matches!(
            fact_class,
            MemoryFactClass::UserIdentity
                | MemoryFactClass::UserBiography
                | MemoryFactClass::UserRelationship
                | MemoryFactClass::StableUserPreference
                | MemoryFactClass::CommunicationPreference
                | MemoryFactClass::RecurringUserInstruction
        ),
        MemoryOwnershipClass::DurableWorkspaceMemory => matches!(
            fact_class,
            MemoryFactClass::RecurringUserInstruction
                | MemoryFactClass::ProjectPolicy
                | MemoryFactClass::ProjectDecision
                | MemoryFactClass::ProjectProcedure
                | MemoryFactClass::ProjectConstraint
        ),
        MemoryOwnershipClass::DurableAgentMemory => {
            fact_class == MemoryFactClass::AssistantSelfDescription
        }
        MemoryOwnershipClass::ThreadEpisodicContext => matches!(
            fact_class,
            MemoryFactClass::ThreadLocalState | MemoryFactClass::GeneratedSummaryFact
        ),
        MemoryOwnershipClass::TaskRuntimeState => fact_class == MemoryFactClass::TaskLifecycleState,
        MemoryOwnershipClass::DomainRuntimeState => matches!(
            fact_class,
            MemoryFactClass::OperationalObservation
                | MemoryFactClass::ToolResultFact
                | MemoryFactClass::DomainOwnedState
                | MemoryFactClass::GeneratedSummaryFact
        ),
        MemoryOwnershipClass::AuditOnly | MemoryOwnershipClass::Reject => true,
    }
}

pub(crate) fn memory_quality_gate_input_from_semantic_write(
    params: &MemorySemanticWriteParams,
    relation: MemoryWriteRelation,
    source_context: &MemorySourceContextClassification,
    ontology: MemoryOntologyClassification,
    sensitivity: MemorySensitivity,
    canonical_key: Option<String>,
    memory_write_disabled_for_turn: bool,
) -> MemoryQualityGateInput {
    let ontology = source_context_adjusted_ontology(source_context, ontology);
    let extractor_ontology_proposal = extractor_ontology_proposal_from_metadata(&params.metadata);
    let extractor_ontology_proposal_comparison = extractor_ontology_proposal.map(|proposal| {
        proposal.compare_to_service_classification(&ontology, source_context.evidence_class)
    });
    MemoryQualityGateInput {
        source_context_kind: source_context.context_kind,
        fact_class: ontology.fact_class,
        lifetime_class: ontology.lifetime_class,
        ownership_class: ontology.proposed_ownership_class,
        evidence_class: source_context.evidence_class,
        sensitivity,
        relation,
        scope: params.scope.clone(),
        canonical_key,
        memory_write_disabled_for_turn,
        explicit_user_approval: source_context.evidence_class == MemoryEvidenceClass::UserApproval,
        sensitive_memory_policy_allowed: false,
        source_thread_id: source_context.thread_id.clone(),
        source_turn_id: source_context.turn_id.clone(),
        source_item_id: source_context.item_id.clone(),
        task_id: source_context.task_id.clone(),
        workspace_id: source_context.workspace_id.clone(),
        extractor_ontology_proposal,
        extractor_ontology_proposal_comparison,
    }
}

fn source_context_adjusted_ontology(
    source_context: &MemorySourceContextClassification,
    ontology: MemoryOntologyClassification,
) -> MemoryOntologyClassification {
    if source_context.context_kind == MemorySourceContextKind::GeneratedSummary
        && ontology.fact_class == MemoryFactClass::Unknown
    {
        return MemoryOntologyClassification {
            fact_class: MemoryFactClass::GeneratedSummaryFact,
            lifetime_class: MemoryLifetimeClass::Unknown,
            proposed_ownership_class: MemoryOwnershipClass::ThreadEpisodicContext,
        };
    }

    ontology
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extractor_ontology::{
        MemoryExtractorOntologyProposal, insert_extractor_ontology_proposal_metadata,
    };
    use pioneer_protocol::{
        MemoryAttribute, MemoryCategory, MemoryDurability, MemoryExplicitness,
        MemoryExtractorCertainty, MemoryIntent, MemoryScopeHint, MemoryScopeKind,
        MemorySemanticFields, MemorySensitivityHint, MemorySubject,
    };

    #[test]
    fn semantic_write_input_builder_uses_typed_fields_only() {
        let params = MemorySemanticWriteParams {
            scope: MemoryScope {
                kind: MemoryScopeKind::User,
                key: "user:default".to_owned(),
            },
            semantic: user_identity_semantic(),
            content: "irrelevant test content".to_owned(),
            value: Some("Alexander".to_owned()),
            evidence: None,
            provenance: None,
            source_context_kind: Some(MemorySourceContextKind::DirectUserConversation),
            disposition: None,
            client_provided_key: None,
            target_memory_id: None,
            confidence: None,
            importance: None,
            metadata: Default::default(),
        };
        let source_context = MemorySourceContextClassification {
            context_kind: MemorySourceContextKind::DirectUserConversation,
            actor_role: pioneer_protocol::MemoryEvidenceActorRole::User,
            evidence_class: MemoryEvidenceClass::DirectUserAssertion,
            source_is_user_assertion: true,
            source_is_system_owned_state: false,
            thread_id: Some("thread-1".to_owned()),
            turn_id: Some("turn-1".to_owned()),
            item_id: Some("item-1".to_owned()),
            task_id: None,
            workspace_id: None,
        };
        let ontology = MemoryOntologyClassification {
            fact_class: MemoryFactClass::UserIdentity,
            lifetime_class: MemoryLifetimeClass::LongLived,
            proposed_ownership_class: MemoryOwnershipClass::DurableUserMemory,
        };

        let input = memory_quality_gate_input_from_semantic_write(
            &params,
            MemoryWriteRelation::Novel,
            &source_context,
            ontology,
            MemorySensitivity::Personal,
            Some("user:identity:name".to_owned()),
            false,
        );

        assert_eq!(
            input.source_context_kind,
            MemorySourceContextKind::DirectUserConversation
        );
        assert_eq!(input.fact_class, MemoryFactClass::UserIdentity);
        assert_eq!(input.lifetime_class, MemoryLifetimeClass::LongLived);
        assert_eq!(
            input.evidence_class,
            MemoryEvidenceClass::DirectUserAssertion
        );
        assert_eq!(input.canonical_key.as_deref(), Some("user:identity:name"));
        assert_eq!(input.source_thread_id.as_deref(), Some("thread-1"));
    }

    #[test]
    fn semantic_write_input_builder_keeps_extractor_proposal_advisory() {
        let mut params = MemorySemanticWriteParams {
            scope: MemoryScope {
                kind: MemoryScopeKind::User,
                key: "user:default".to_owned(),
            },
            semantic: user_identity_semantic(),
            content: "irrelevant test content".to_owned(),
            value: Some("Alexander".to_owned()),
            evidence: None,
            provenance: None,
            source_context_kind: Some(MemorySourceContextKind::DirectUserConversation),
            disposition: None,
            client_provided_key: None,
            target_memory_id: None,
            confidence: None,
            importance: None,
            metadata: Default::default(),
        };
        let extractor_proposal = MemoryExtractorOntologyProposal {
            fact_class: MemoryFactClass::ToolResultFact,
            lifetime_class: MemoryLifetimeClass::NaturallyExpiring,
            evidence_class: MemoryEvidenceClass::ToolObservation,
            proposed_ownership_class: MemoryOwnershipClass::DomainRuntimeState,
        };
        insert_extractor_ontology_proposal_metadata(&mut params.metadata, extractor_proposal);

        let source_context = MemorySourceContextClassification {
            context_kind: MemorySourceContextKind::DirectUserConversation,
            actor_role: pioneer_protocol::MemoryEvidenceActorRole::User,
            evidence_class: MemoryEvidenceClass::DirectUserAssertion,
            source_is_user_assertion: true,
            source_is_system_owned_state: false,
            thread_id: None,
            turn_id: None,
            item_id: None,
            task_id: None,
            workspace_id: None,
        };
        let service_ontology = MemoryOntologyClassification {
            fact_class: MemoryFactClass::UserIdentity,
            lifetime_class: MemoryLifetimeClass::LongLived,
            proposed_ownership_class: MemoryOwnershipClass::DurableUserMemory,
        };

        let input = memory_quality_gate_input_from_semantic_write(
            &params,
            MemoryWriteRelation::Novel,
            &source_context,
            service_ontology,
            MemorySensitivity::Personal,
            Some("user:identity:name".to_owned()),
            false,
        );

        assert_eq!(input.extractor_ontology_proposal, Some(extractor_proposal));
        let comparison = input
            .extractor_ontology_proposal_comparison
            .expect("proposal comparison is computed");
        assert!(!comparison.all_match);
        assert!(!comparison.fact_class_matches);
        assert_eq!(input.fact_class, MemoryFactClass::UserIdentity);
        assert_eq!(
            input.evidence_class,
            MemoryEvidenceClass::DirectUserAssertion
        );
        assert_eq!(
            input.ownership_class,
            MemoryOwnershipClass::DurableUserMemory
        );
    }

    #[test]
    fn hard_reject_opt_out_beats_secret_reason() {
        let mut input = base_input();
        input.memory_write_disabled_for_turn = true;
        input.fact_class = MemoryFactClass::SecretOrCredential;

        let decision = MemoryQualityGate::decide(&input);

        assert_eq!(
            decision.reason_codes,
            vec![MemoryQualityReasonCode::MemoryWriteDisabledForTurn]
        );
    }

    #[test]
    fn hard_reject_still_wins_over_routing() {
        let mut input = base_input();
        input.memory_write_disabled_for_turn = true;
        input.fact_class = MemoryFactClass::ThreadLocalState;
        input.lifetime_class = MemoryLifetimeClass::ThreadLifetime;
        input.ownership_class = MemoryOwnershipClass::ThreadEpisodicContext;

        assert_force_reject(
            MemoryQualityGate::decide(&input),
            MemoryOwnershipClass::Reject,
            &[MemoryQualityReasonCode::MemoryWriteDisabledForTurn],
        );
    }

    fn base_input() -> MemoryQualityGateInput {
        MemoryQualityGateInput {
            source_context_kind: MemorySourceContextKind::DirectUserConversation,
            fact_class: MemoryFactClass::UserIdentity,
            lifetime_class: MemoryLifetimeClass::LongLived,
            ownership_class: MemoryOwnershipClass::DurableUserMemory,
            evidence_class: MemoryEvidenceClass::DirectUserAssertion,
            sensitivity: MemorySensitivity::Personal,
            relation: MemoryWriteRelation::Novel,
            scope: MemoryScope {
                kind: MemoryScopeKind::User,
                key: "user:default".to_owned(),
            },
            canonical_key: Some("user:identity:name".to_owned()),
            memory_write_disabled_for_turn: false,
            explicit_user_approval: false,
            sensitive_memory_policy_allowed: false,
            source_thread_id: None,
            source_turn_id: None,
            source_item_id: None,
            task_id: None,
            workspace_id: None,
            extractor_ontology_proposal: None,
            extractor_ontology_proposal_comparison: None,
        }
    }

    fn user_identity_semantic() -> MemorySemanticFields {
        MemorySemanticFields {
            intent: MemoryIntent::ExplicitStore,
            explicitness: MemoryExplicitness::Explicit,
            category: MemoryCategory::Identity,
            subject: MemorySubject::CurrentUser,
            attribute: MemoryAttribute::Name,
            subject_key: None,
            custom_subject: None,
            custom_attribute: None,
            scope_hint: MemoryScopeHint::UserGlobal,
            durability: MemoryDurability::LongLived,
            sensitivity: MemorySensitivityHint::Personal,
            certainty: MemoryExtractorCertainty::High,
        }
    }

    fn assert_force_reject(
        decision: MemoryQualityDecision,
        target_ownership: MemoryOwnershipClass,
        reason_codes: &[MemoryQualityReasonCode],
    ) {
        assert_eq!(decision.action, MemoryQualityAction::ForceReject);
        assert_eq!(decision.target_ownership, target_ownership);
        assert_eq!(decision.reason_codes, reason_codes);
        assert!(!decision.candidate_auto_approve_allowed);
    }

    fn assert_quarantine(
        decision: MemoryQualityDecision,
        reason_codes: &[MemoryQualityReasonCode],
    ) {
        assert_decision(
            decision,
            MemoryQualityAction::Quarantine,
            MemoryOwnershipClass::AuditOnly,
            reason_codes,
        );
    }

    fn assert_decision(
        decision: MemoryQualityDecision,
        action: MemoryQualityAction,
        target_ownership: MemoryOwnershipClass,
        reason_codes: &[MemoryQualityReasonCode],
    ) {
        assert_eq!(decision.action, action);
        assert_eq!(decision.target_ownership, target_ownership);
        assert_eq!(decision.reason_codes, reason_codes);
        assert!(!decision.candidate_auto_approve_allowed);
    }

    fn assert_candidate_policy(
        decision: MemoryQualityDecision,
        target_ownership: MemoryOwnershipClass,
        reason_codes: &[MemoryQualityReasonCode],
        candidate_auto_approve_allowed: bool,
    ) {
        assert_eq!(decision.action, MemoryQualityAction::CandidatePolicy);
        assert_eq!(decision.target_ownership, target_ownership);
        assert_eq!(decision.reason_codes, reason_codes);
        assert_eq!(
            decision.candidate_auto_approve_allowed,
            candidate_auto_approve_allowed
        );
    }

    #[test]
    fn hard_rejection_reasons_preserve_source_ownership_and_evidence_boundaries() {
        type QualityCase = (
            &'static str,
            fn(&mut MemoryQualityGateInput),
            MemoryOwnershipClass,
            &'static [MemoryQualityReasonCode],
        );
        let cases: &[QualityCase] = &[
            (
                "hard_rejects_when_turn_disables_memory_writes",
                |input| {
                    input.memory_write_disabled_for_turn = true;
                },
                MemoryOwnershipClass::Reject,
                &[MemoryQualityReasonCode::MemoryWriteDisabledForTurn],
            ),
            (
                "hard_rejects_secret_or_credential_facts",
                |input| {
                    input.fact_class = MemoryFactClass::SecretOrCredential;
                },
                MemoryOwnershipClass::Reject,
                &[MemoryQualityReasonCode::SecretOrCredential],
            ),
            (
                "hard_rejects_regulated_sensitive_fact_without_user_approval_or_policy",
                |input| {
                    input.fact_class = MemoryFactClass::RegulatedSensitiveFact;
                    input.explicit_user_approval = false;
                    input.sensitive_memory_policy_allowed = false;
                },
                MemoryOwnershipClass::Reject,
                &[MemoryQualityReasonCode::RegulatedSensitiveWithoutUserApproval],
            ),
            (
                "hard_rejects_system_owned_state_as_durable_memory",
                |input| {
                    input.source_context_kind = MemorySourceContextKind::SystemRuntime;
                    input.evidence_class = MemoryEvidenceClass::SystemObservation;
                    input.ownership_class = MemoryOwnershipClass::DurableWorkspaceMemory;
                },
                MemoryOwnershipClass::AuditOnly,
                &[
                    MemoryQualityReasonCode::SystemOwnedStateNotMemory,
                    MemoryQualityReasonCode::SourceNotAuthoritativeForDurableMemory,
                ],
            ),
            (
                "hard_rejects_task_runtime_as_any_durable_memory:DurableUserMemory",
                |input| {
                    input.source_context_kind = MemorySourceContextKind::TaskRuntime;
                    input.evidence_class = MemoryEvidenceClass::TaskRuntimeObservation;
                    input.ownership_class = MemoryOwnershipClass::DurableUserMemory;
                },
                MemoryOwnershipClass::TaskRuntimeState,
                &[
                    MemoryQualityReasonCode::TaskStateNotUserMemory,
                    MemoryQualityReasonCode::OwnershipMismatch,
                ],
            ),
            (
                "hard_rejects_task_runtime_as_any_durable_memory:DurableWorkspaceMemory",
                |input| {
                    input.source_context_kind = MemorySourceContextKind::TaskRuntime;
                    input.evidence_class = MemoryEvidenceClass::TaskRuntimeObservation;
                    input.ownership_class = MemoryOwnershipClass::DurableWorkspaceMemory;
                },
                MemoryOwnershipClass::TaskRuntimeState,
                &[
                    MemoryQualityReasonCode::TaskStateNotUserMemory,
                    MemoryQualityReasonCode::OwnershipMismatch,
                ],
            ),
            (
                "hard_rejects_task_runtime_as_any_durable_memory:DurableAgentMemory",
                |input| {
                    input.source_context_kind = MemorySourceContextKind::TaskRuntime;
                    input.evidence_class = MemoryEvidenceClass::TaskRuntimeObservation;
                    input.ownership_class = MemoryOwnershipClass::DurableAgentMemory;
                },
                MemoryOwnershipClass::TaskRuntimeState,
                &[
                    MemoryQualityReasonCode::TaskStateNotUserMemory,
                    MemoryQualityReasonCode::OwnershipMismatch,
                ],
            ),
            (
                "hard_rejects_tool_result_as_any_durable_memory:DurableUserMemory",
                |input| {
                    input.source_context_kind = MemorySourceContextKind::ToolResult;
                    input.evidence_class = MemoryEvidenceClass::ToolObservation;
                    input.ownership_class = MemoryOwnershipClass::DurableUserMemory;
                },
                MemoryOwnershipClass::DomainRuntimeState,
                &[
                    MemoryQualityReasonCode::ToolResultNotUserMemory,
                    MemoryQualityReasonCode::OwnershipMismatch,
                ],
            ),
            (
                "hard_rejects_tool_result_as_any_durable_memory:DurableWorkspaceMemory",
                |input| {
                    input.source_context_kind = MemorySourceContextKind::ToolResult;
                    input.evidence_class = MemoryEvidenceClass::ToolObservation;
                    input.ownership_class = MemoryOwnershipClass::DurableWorkspaceMemory;
                },
                MemoryOwnershipClass::DomainRuntimeState,
                &[
                    MemoryQualityReasonCode::ToolResultNotUserMemory,
                    MemoryQualityReasonCode::OwnershipMismatch,
                ],
            ),
            (
                "hard_rejects_tool_result_as_any_durable_memory:DurableAgentMemory",
                |input| {
                    input.source_context_kind = MemorySourceContextKind::ToolResult;
                    input.evidence_class = MemoryEvidenceClass::ToolObservation;
                    input.ownership_class = MemoryOwnershipClass::DurableAgentMemory;
                },
                MemoryOwnershipClass::DomainRuntimeState,
                &[
                    MemoryQualityReasonCode::ToolResultNotUserMemory,
                    MemoryQualityReasonCode::OwnershipMismatch,
                ],
            ),
            (
                "hard_rejects_assistant_inference_as_user_or_workspace_memory",
                |input| {
                    input.source_context_kind = MemorySourceContextKind::AssistantResponse;
                    input.evidence_class = MemoryEvidenceClass::AssistantInference;
                    input.ownership_class = MemoryOwnershipClass::DurableUserMemory;
                },
                MemoryOwnershipClass::AuditOnly,
                &[
                    MemoryQualityReasonCode::AssistantInferenceNotDurableEvidence,
                    MemoryQualityReasonCode::SourceNotAuthoritativeForDurableMemory,
                ],
            ),
            (
                "hard_rejects_non_durable_lifetime_for_durable_memory",
                |input| {
                    input.lifetime_class = MemoryLifetimeClass::SessionOnly;
                    input.ownership_class = MemoryOwnershipClass::DurableUserMemory;
                },
                MemoryOwnershipClass::Reject,
                &[MemoryQualityReasonCode::NonDurableLifetime],
            ),
            (
                "hard_rejects_missing_or_weak_evidence",
                |input| {
                    input.evidence_class = MemoryEvidenceClass::MissingOrWeak;
                },
                MemoryOwnershipClass::Reject,
                &[MemoryQualityReasonCode::WeakOrMissingEvidence],
            ),
            (
                "hard_rejects_duplicate_or_suppressed_duplicate_relation",
                |input| {
                    input.relation = MemoryWriteRelation::SuppressedByRejection;
                },
                MemoryOwnershipClass::AuditOnly,
                &[MemoryQualityReasonCode::DuplicateExistingMemory],
            ),
            (
                "duplicate_never_reaches_candidate_policy",
                |input| {
                    input.relation = MemoryWriteRelation::Duplicate;
                },
                MemoryOwnershipClass::AuditOnly,
                &[MemoryQualityReasonCode::DuplicateExistingMemory],
            ),
        ];
        for &(case, configure, ownership, reasons) in cases {
            let mut input = base_input();
            configure(&mut input);
            eprintln!("quality gate case: {case}");
            assert_force_reject(MemoryQualityGate::decide(&input), ownership, reasons);
        }
    }

    #[test]
    fn non_durable_inputs_route_to_their_owning_state() {
        type QualityCase = (
            &'static str,
            fn(&mut MemoryQualityGateInput),
            MemoryQualityAction,
            MemoryOwnershipClass,
            &'static [MemoryQualityReasonCode],
        );
        let cases: &[QualityCase] = &[
            (
                "routes_thread_local_state_to_thread_episodic_context",
                |input| {
                    input.fact_class = MemoryFactClass::ThreadLocalState;
                    input.lifetime_class = MemoryLifetimeClass::ThreadLifetime;
                    input.ownership_class = MemoryOwnershipClass::ThreadEpisodicContext;
                },
                MemoryQualityAction::RouteToThreadEpisodic,
                MemoryOwnershipClass::ThreadEpisodicContext,
                &[
                    MemoryQualityReasonCode::RouteThreadEpisodic,
                    MemoryQualityReasonCode::NonDurableLifetime,
                ],
            ),
            (
                "routes_task_lifecycle_state_to_task_runtime_state",
                |input| {
                    input.source_context_kind = MemorySourceContextKind::TaskRuntime;
                    input.evidence_class = MemoryEvidenceClass::TaskRuntimeObservation;
                    input.fact_class = MemoryFactClass::TaskLifecycleState;
                    input.lifetime_class = MemoryLifetimeClass::TaskLifetime;
                    input.ownership_class = MemoryOwnershipClass::TaskRuntimeState;
                },
                MemoryQualityAction::RouteToTaskState,
                MemoryOwnershipClass::TaskRuntimeState,
                &[
                    MemoryQualityReasonCode::RouteTaskState,
                    MemoryQualityReasonCode::TaskLifetime,
                ],
            ),
            (
                "routes_tool_owned_observation_to_domain_state",
                |input| {
                    input.source_context_kind = MemorySourceContextKind::ToolResult;
                    input.evidence_class = MemoryEvidenceClass::ToolObservation;
                    input.fact_class = MemoryFactClass::ToolResultFact;
                    input.lifetime_class = MemoryLifetimeClass::NaturallyExpiring;
                    input.ownership_class = MemoryOwnershipClass::DomainRuntimeState;
                },
                MemoryQualityAction::RouteToDomainState,
                MemoryOwnershipClass::DomainRuntimeState,
                &[
                    MemoryQualityReasonCode::RouteDomainState,
                    MemoryQualityReasonCode::ToolOwnedState,
                ],
            ),
            (
                "routes_system_owned_observation_to_domain_state",
                |input| {
                    input.source_context_kind = MemorySourceContextKind::SystemRuntime;
                    input.evidence_class = MemoryEvidenceClass::SystemObservation;
                    input.fact_class = MemoryFactClass::DomainOwnedState;
                    input.lifetime_class = MemoryLifetimeClass::NaturallyExpiring;
                    input.ownership_class = MemoryOwnershipClass::DomainRuntimeState;
                },
                MemoryQualityAction::RouteToDomainState,
                MemoryOwnershipClass::DomainRuntimeState,
                &[
                    MemoryQualityReasonCode::RouteDomainState,
                    MemoryQualityReasonCode::SystemOwnedStateNotMemory,
                ],
            ),
            (
                "routes_generated_summary_to_thread_episodic_context",
                |input| {
                    input.source_context_kind = MemorySourceContextKind::GeneratedSummary;
                    input.evidence_class = MemoryEvidenceClass::GeneratedSummary;
                    input.fact_class = MemoryFactClass::GeneratedSummaryFact;
                    input.lifetime_class = MemoryLifetimeClass::Unknown;
                    input.ownership_class = MemoryOwnershipClass::ThreadEpisodicContext;
                },
                MemoryQualityAction::RouteToThreadEpisodic,
                MemoryOwnershipClass::ThreadEpisodicContext,
                &[
                    MemoryQualityReasonCode::GeneratedSummaryNotDurableMemory,
                    MemoryQualityReasonCode::RouteThreadEpisodic,
                ],
            ),
        ];
        for &(case, configure, action, ownership, reasons) in cases {
            let mut input = base_input();
            configure(&mut input);
            eprintln!("quality gate case: {case}");
            assert_decision(
                MemoryQualityGate::decide(&input),
                action,
                ownership,
                reasons,
            );
        }
    }

    #[test]
    fn untrusted_or_unclassified_inputs_remain_quarantined() {
        type QualityCase = (
            &'static str,
            fn(&mut MemoryQualityGateInput),
            &'static [MemoryQualityReasonCode],
        );
        let cases: &[QualityCase] = &[
            (
                "quarantines_unknown_source_context",
                |input| {
                    input.source_context_kind = MemorySourceContextKind::Unknown;
                },
                &[MemoryQualityReasonCode::UnknownSourceContext],
            ),
            (
                "quarantines_unknown_fact_class",
                |input| {
                    input.fact_class = MemoryFactClass::Unknown;
                },
                &[MemoryQualityReasonCode::UnknownFactClass],
            ),
            (
                "quarantines_unknown_lifetime_for_durable_memory",
                |input| {
                    input.lifetime_class = MemoryLifetimeClass::Unknown;
                    input.ownership_class = MemoryOwnershipClass::DurableUserMemory;
                },
                &[MemoryQualityReasonCode::UnknownLifetime],
            ),
            (
                "quarantines_ownership_mismatch",
                |input| {
                    input.fact_class = MemoryFactClass::ProjectDecision;
                    input.ownership_class = MemoryOwnershipClass::DurableUserMemory;
                },
                &[MemoryQualityReasonCode::OwnershipMismatch],
            ),
            (
                "quarantines_imported_content_without_source_policy",
                |input| {
                    input.source_context_kind = MemorySourceContextKind::ImportedDocument;
                },
                &[MemoryQualityReasonCode::SourcePolicyMissing],
            ),
            (
                "quarantines_when_no_positive_allow_rule_matches",
                |input| {
                    input.source_context_kind = MemorySourceContextKind::DirectUserConversation;
                    input.fact_class = MemoryFactClass::UserIdentity;
                    input.lifetime_class = MemoryLifetimeClass::LongLived;
                    input.ownership_class = MemoryOwnershipClass::DurableUserMemory;
                    input.evidence_class = MemoryEvidenceClass::AssistantInference;
                },
                &[MemoryQualityReasonCode::NoQualityAllowRule],
            ),
            (
                "assistant_originated_self_description_does_not_reach_candidate_policy",
                |input| {
                    input.source_context_kind = MemorySourceContextKind::AssistantResponse;
                    input.evidence_class = MemoryEvidenceClass::AssistantInference;
                    input.fact_class = MemoryFactClass::AssistantSelfDescription;
                    input.ownership_class = MemoryOwnershipClass::DurableAgentMemory;
                },
                &[MemoryQualityReasonCode::NoQualityAllowRule],
            ),
        ];
        for &(case, configure, reasons) in cases {
            let mut input = base_input();
            configure(&mut input);
            eprintln!("quality gate case: {case}");
            assert_quarantine(MemoryQualityGate::decide(&input), reasons);
        }
    }

    #[test]
    fn user_owned_durable_facts_preserve_candidate_policy_and_reasons() {
        type QualityCase = (
            &'static str,
            fn(&mut MemoryQualityGateInput),
            MemoryOwnershipClass,
            &'static [MemoryQualityReasonCode],
            bool,
        );
        let cases: &[QualityCase] = &[
            (
                "allows_direct_user_identity_candidate",
                |input| {
                    input.fact_class = MemoryFactClass::UserIdentity;
                    input.ownership_class = MemoryOwnershipClass::DurableUserMemory;
                },
                MemoryOwnershipClass::DurableUserMemory,
                &[
                    MemoryQualityReasonCode::CandidatePolicyAllowed,
                    MemoryQualityReasonCode::DurableUserIdentity,
                    MemoryQualityReasonCode::NovelCandidate,
                ],
                true,
            ),
            (
                "allows_direct_user_profile_candidate",
                |input| {
                    input.fact_class = MemoryFactClass::UserBiography;
                    input.ownership_class = MemoryOwnershipClass::DurableUserMemory;
                },
                MemoryOwnershipClass::DurableUserMemory,
                &[
                    MemoryQualityReasonCode::CandidatePolicyAllowed,
                    MemoryQualityReasonCode::DurableUserProfile,
                    MemoryQualityReasonCode::NovelCandidate,
                ],
                true,
            ),
            (
                "allows_direct_user_preference_candidate",
                |input| {
                    input.fact_class = MemoryFactClass::CommunicationPreference;
                    input.ownership_class = MemoryOwnershipClass::DurableUserMemory;
                },
                MemoryOwnershipClass::DurableUserMemory,
                &[
                    MemoryQualityReasonCode::CandidatePolicyAllowed,
                    MemoryQualityReasonCode::DurableUserPreference,
                    MemoryQualityReasonCode::NovelCandidate,
                ],
                true,
            ),
            (
                "allows_recurring_instruction_for_user_or_workspace_candidate",
                |input| {
                    input.fact_class = MemoryFactClass::RecurringUserInstruction;
                    input.ownership_class = MemoryOwnershipClass::DurableWorkspaceMemory;
                },
                MemoryOwnershipClass::DurableWorkspaceMemory,
                &[
                    MemoryQualityReasonCode::CandidatePolicyAllowed,
                    MemoryQualityReasonCode::DurableRecurringInstruction,
                    MemoryQualityReasonCode::NovelCandidate,
                ],
                true,
            ),
            (
                "allows_project_decision_candidate",
                |input| {
                    input.fact_class = MemoryFactClass::ProjectDecision;
                    input.lifetime_class = MemoryLifetimeClass::ProjectLifetime;
                    input.ownership_class = MemoryOwnershipClass::DurableWorkspaceMemory;
                },
                MemoryOwnershipClass::DurableWorkspaceMemory,
                &[
                    MemoryQualityReasonCode::CandidatePolicyAllowed,
                    MemoryQualityReasonCode::DurableProjectMemory,
                    MemoryQualityReasonCode::NovelCandidate,
                ],
                true,
            ),
            (
                "allows_user_confirmed_agent_memory_candidate",
                |input| {
                    input.fact_class = MemoryFactClass::AssistantSelfDescription;
                    input.evidence_class = MemoryEvidenceClass::UserApproval;
                    input.ownership_class = MemoryOwnershipClass::DurableAgentMemory;
                },
                MemoryOwnershipClass::DurableAgentMemory,
                &[
                    MemoryQualityReasonCode::CandidatePolicyAllowed,
                    MemoryQualityReasonCode::UserConfirmedAgentMemory,
                    MemoryQualityReasonCode::NovelCandidate,
                ],
                true,
            ),
        ];
        for &(case, configure, ownership, reasons, auto_approve) in cases {
            let mut input = base_input();
            configure(&mut input);
            eprintln!("quality gate case: {case}");
            assert_candidate_policy(
                MemoryQualityGate::decide(&input),
                ownership,
                reasons,
                auto_approve,
            );
        }
    }

    #[test]
    fn candidate_relations_control_reasons_and_auto_approval() {
        type QualityCase = (
            &'static str,
            fn(&mut MemoryQualityGateInput),
            MemoryOwnershipClass,
            &'static [MemoryQualityReasonCode],
            bool,
        );
        let cases: &[QualityCase] = &[
            (
                "contradiction_can_reach_candidate_policy_but_cannot_auto_approve",
                |input| {
                    input.relation = MemoryWriteRelation::Contradiction;
                },
                MemoryOwnershipClass::DurableUserMemory,
                &[
                    MemoryQualityReasonCode::CandidatePolicyAllowed,
                    MemoryQualityReasonCode::DurableUserIdentity,
                    MemoryQualityReasonCode::ContradictsExistingMemory,
                    MemoryQualityReasonCode::RequiresResolution,
                ],
                false,
            ),
            (
                "compatible_update_reaches_candidate_policy_with_relation_reason",
                |input| {
                    input.relation = MemoryWriteRelation::CompatibleUpdate;
                },
                MemoryOwnershipClass::DurableUserMemory,
                &[
                    MemoryQualityReasonCode::CandidatePolicyAllowed,
                    MemoryQualityReasonCode::DurableUserIdentity,
                    MemoryQualityReasonCode::CompatibleUpdate,
                ],
                true,
            ),
        ];
        for &(case, configure, ownership, reasons, auto_approve) in cases {
            let mut input = base_input();
            configure(&mut input);
            eprintln!("quality gate case: {case}");
            assert_candidate_policy(
                MemoryQualityGate::decide(&input),
                ownership,
                reasons,
                auto_approve,
            );
        }
    }
}
