use crate::config::MemoryCandidatePolicyConfig;
use pioneer_protocol::{
    MemoryCandidatePolicyDecision, MemoryCandidatePolicyInput, MemoryCandidatePolicyOutput,
    MemoryCandidateScore, MemoryCandidateScoreBucket, MemoryCandidateStatus, MemoryDurability,
    MemoryEvidenceClass, MemoryExplicitness, MemoryFactClass, MemoryIntent, MemoryLifetimeClass,
    MemoryOwnershipClass, MemoryQualityAction, MemoryScopeClarity, MemorySensitivity,
    MemorySensitivityHint, MemorySourceContextKind, MemoryWriteRelation,
};

#[derive(Debug, Clone)]
pub(crate) struct MemoryCandidatePolicyEngine {
    config: MemoryCandidatePolicyConfig,
}

impl MemoryCandidatePolicyEngine {
    pub(crate) fn new(config: MemoryCandidatePolicyConfig) -> Self {
        Self { config }
    }

    pub(crate) fn decide(&self, input: MemoryCandidatePolicyInput) -> MemoryCandidatePolicyOutput {
        let forced_reject_reason = forced_reject_reason(&input);
        let mut score = score_candidate(&input);
        if let Some(reason) = forced_reject_reason {
            score.bucket = MemoryCandidateScoreBucket::ExtremelyLow;
            score.total_score = 0.0;
            score.reasons.push(reason.to_owned());
            return MemoryCandidatePolicyOutput {
                input,
                score,
                decision: MemoryCandidatePolicyDecision::AutoReject,
                status: MemoryCandidateStatus::AutoRejected,
                reason_code: reason.to_owned(),
                reason: Some(reason.to_owned()),
            };
        }

        score.bucket = if score.total_score >= self.config.auto_approve_min_score {
            MemoryCandidateScoreBucket::High
        } else if score.total_score <= self.config.auto_reject_max_score {
            MemoryCandidateScoreBucket::ExtremelyLow
        } else {
            MemoryCandidateScoreBucket::Middle
        };

        if input.has_contradiction || input.relation == MemoryWriteRelation::Contradiction {
            return if self.config.review_enabled {
                MemoryCandidatePolicyOutput {
                    input,
                    score,
                    decision: MemoryCandidatePolicyDecision::AskOnUse,
                    status: MemoryCandidateStatus::AskOnUse,
                    reason_code: "review_enabled_contradiction".to_owned(),
                    reason: Some(
                        "contradictory memory candidate needs explicit resolution".to_owned(),
                    ),
                }
            } else {
                MemoryCandidatePolicyOutput {
                    input,
                    score,
                    decision: MemoryCandidatePolicyDecision::RejectReviewDisabled,
                    status: MemoryCandidateStatus::ReviewDisabledRejected,
                    reason_code: "review_disabled_contradiction".to_owned(),
                    reason: Some(
                        "contradictory memory candidate suppressed while review is disabled"
                            .to_owned(),
                    ),
                }
            };
        }

        match score.bucket {
            MemoryCandidateScoreBucket::High => self.decide_high(input, score),
            MemoryCandidateScoreBucket::Middle => self.decide_middle(input, score),
            MemoryCandidateScoreBucket::ExtremelyLow => MemoryCandidatePolicyOutput {
                input,
                score,
                decision: MemoryCandidatePolicyDecision::AutoReject,
                status: MemoryCandidateStatus::AutoRejected,
                reason_code: "extremely_low_confidence".to_owned(),
                reason: Some(
                    "memory candidate score is below the auto-reject threshold".to_owned(),
                ),
            },
        }
    }

    fn decide_high(
        &self,
        input: MemoryCandidatePolicyInput,
        score: MemoryCandidateScore,
    ) -> MemoryCandidatePolicyOutput {
        let explicit = input.semantic.explicitness == MemoryExplicitness::Explicit
            || input.semantic.intent == MemoryIntent::ExplicitStore;
        let implicit = input.semantic.explicitness == MemoryExplicitness::Implicit
            || input.semantic.intent == MemoryIntent::ImplicitCandidate;
        if !input.quality_candidate_auto_approve_allowed {
            return self.high_without_auto_approve(
                input,
                score,
                "quality_auto_approve_disabled",
                "quality decision allowed candidate policy but disabled automatic approval",
            );
        }

        if (explicit && self.config.allow_explicit_auto_approve)
            || (implicit && self.config.allow_implicit_auto_approve)
        {
            return MemoryCandidatePolicyOutput {
                input,
                score,
                decision: MemoryCandidatePolicyDecision::AutoApprove,
                status: MemoryCandidateStatus::Approved,
                reason_code: "high_confidence_durable_fact".to_owned(),
                reason: Some("high-confidence durable memory candidate auto-approved".to_owned()),
            };
        }

        let reason_code = if implicit && !self.config.allow_implicit_auto_approve {
            "implicit_auto_approve_disabled"
        } else {
            "auto_approve_not_allowed"
        };
        let reason = if implicit && !self.config.allow_implicit_auto_approve {
            "implicit memory candidate suppressed because proactive writes are disabled"
        } else {
            "high-score candidate cannot auto-approve under current policy"
        };

        self.high_without_auto_approve(input, score, reason_code, reason)
    }

    fn high_without_auto_approve(
        &self,
        input: MemoryCandidatePolicyInput,
        score: MemoryCandidateScore,
        reason_code: &str,
        reason: &str,
    ) -> MemoryCandidatePolicyOutput {
        if self.config.review_enabled {
            MemoryCandidatePolicyOutput {
                input,
                score,
                decision: MemoryCandidatePolicyDecision::NeedsReview,
                status: MemoryCandidateStatus::NeedsReview,
                reason_code: reason_code.to_owned(),
                reason: Some(reason.to_owned()),
            }
        } else {
            MemoryCandidatePolicyOutput {
                input,
                score,
                decision: MemoryCandidatePolicyDecision::RejectReviewDisabled,
                status: MemoryCandidateStatus::ReviewDisabledRejected,
                reason_code: reason_code.to_owned(),
                reason: Some(reason.to_owned()),
            }
        }
    }

    fn decide_middle(
        &self,
        input: MemoryCandidatePolicyInput,
        score: MemoryCandidateScore,
    ) -> MemoryCandidatePolicyOutput {
        if self.config.review_enabled {
            return MemoryCandidatePolicyOutput {
                input,
                score,
                decision: MemoryCandidatePolicyDecision::PendingSilent,
                status: MemoryCandidateStatus::PendingSilent,
                reason_code: "review_enabled_middle_confidence".to_owned(),
                reason: Some(
                    "middle-confidence memory candidate routed to dormant review".to_owned(),
                ),
            };
        }

        MemoryCandidatePolicyOutput {
            input,
            score,
            decision: MemoryCandidatePolicyDecision::RejectReviewDisabled,
            status: MemoryCandidateStatus::ReviewDisabledRejected,
            reason_code: "review_disabled_middle_confidence".to_owned(),
            reason: Some(
                "middle-confidence memory candidate suppressed while review is disabled".to_owned(),
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct MemoryCandidateScoreComponents {
    explicitness_score: f32,
    source_trust_score: f32,
    evidence_score: f32,
    fact_class_score: f32,
    lifetime_fit_score: f32,
    scope_score: f32,
    ownership_fit_score: f32,
    relation_score: f32,
    sensitivity_score: f32,
    certainty_score: f32,
    penalty_score: f32,
}

fn forced_reject_reason(input: &MemoryCandidatePolicyInput) -> Option<&'static str> {
    if input.quality_action != MemoryQualityAction::CandidatePolicy {
        return Some("quality_action_not_candidate_policy");
    }
    if input.active_no_memory_policy || input.semantic.intent == MemoryIntent::ExplicitNoMemory {
        return Some("memory_write_disabled_for_turn");
    }
    if matches!(
        input.semantic.sensitivity,
        MemorySensitivityHint::Secret | MemorySensitivityHint::Regulated
    ) || matches!(
        input.sensitivity,
        MemorySensitivity::SecretLike | MemorySensitivity::Regulated
    ) {
        return Some("secret_like_or_regulated");
    }
    if matches!(
        input.semantic.durability,
        MemoryDurability::Transient | MemoryDurability::SessionOnly
    ) {
        return Some("transient_or_session_only");
    }
    if input.has_rejected_duplicate || input.relation == MemoryWriteRelation::SuppressedByRejection
    {
        return Some("suppressed_duplicate");
    }
    None
}

fn score_candidate(input: &MemoryCandidatePolicyInput) -> MemoryCandidateScore {
    let components = score_components(input);

    let total_score: f32 = (components.source_trust_score
        + components.evidence_score
        + components.fact_class_score
        + components.lifetime_fit_score
        + components.scope_score
        + components.ownership_fit_score
        + components.relation_score
        + components.sensitivity_score
        + components.certainty_score
        - components.penalty_score)
        .clamp(0.0, 1.0);
    let mut reasons = Vec::new();
    push_reason(
        &mut reasons,
        components.explicitness_score,
        explicitness_reason(input),
    );
    push_reason(
        &mut reasons,
        components.source_trust_score,
        source_trust_reason(input),
    );
    push_reason(
        &mut reasons,
        components.evidence_score,
        evidence_reason(input),
    );
    push_reason(
        &mut reasons,
        components.fact_class_score,
        fact_class_reason(input),
    );
    push_reason(
        &mut reasons,
        components.lifetime_fit_score,
        lifetime_reason(input),
    );
    push_reason(&mut reasons, components.scope_score, scope_reason(input));
    push_reason(
        &mut reasons,
        components.ownership_fit_score,
        ownership_reason(input),
    );
    push_reason(
        &mut reasons,
        components.relation_score,
        relation_reason(input),
    );
    push_reason(
        &mut reasons,
        components.sensitivity_score,
        sensitivity_reason(input),
    );
    push_reason(
        &mut reasons,
        components.certainty_score,
        certainty_reason(input),
    );
    if components.penalty_score > 0.0 {
        reasons.push(format!("penalty:{:.2}", components.penalty_score));
    }
    if input.has_contradiction {
        reasons.push("contradiction".to_owned());
    }
    if input.has_duplicate {
        reasons.push("duplicate".to_owned());
    }

    MemoryCandidateScore {
        score_version: "quality_v1".to_owned(),
        total_score,
        bucket: MemoryCandidateScoreBucket::Middle,
        explicitness_score: components.explicitness_score,
        durability_score: components.lifetime_fit_score,
        source_trust_score: components.source_trust_score,
        fact_class_score: components.fact_class_score,
        lifetime_fit_score: components.lifetime_fit_score,
        scope_score: components.scope_score,
        ownership_fit_score: components.ownership_fit_score,
        evidence_score: components.evidence_score,
        certainty_score: components.certainty_score,
        sensitivity_score: components.sensitivity_score,
        relation_score: components.relation_score,
        penalty_score: components.penalty_score,
        reasons,
    }
}

fn score_components(input: &MemoryCandidatePolicyInput) -> MemoryCandidateScoreComponents {
    MemoryCandidateScoreComponents {
        explicitness_score: explicitness_support_score(input),
        source_trust_score: source_trust_score(input),
        evidence_score: evidence_score(input),
        fact_class_score: fact_class_score(input),
        lifetime_fit_score: lifetime_fit_score(input),
        scope_score: scope_fit_score(input),
        ownership_fit_score: ownership_fit_score(input),
        relation_score: relation_score(input),
        sensitivity_score: sensitivity_score(input),
        certainty_score: extractor_certainty_score(input),
        penalty_score: penalty_score(input),
    }
}

fn explicitness_support_score(input: &MemoryCandidatePolicyInput) -> f32 {
    match input.semantic.explicitness {
        MemoryExplicitness::Explicit => 0.03,
        MemoryExplicitness::Implicit => 0.02,
        MemoryExplicitness::Unclear | MemoryExplicitness::None => 0.0,
    }
}

fn source_trust_score(input: &MemoryCandidatePolicyInput) -> f32 {
    match (input.source_context_kind, input.evidence_class) {
        (
            MemorySourceContextKind::DirectUserConversation,
            MemoryEvidenceClass::DirectUserAssertion
            | MemoryEvidenceClass::UserCorrection
            | MemoryEvidenceClass::UserApproval,
        ) => 0.18,
        (MemorySourceContextKind::GeneratedSummary, MemoryEvidenceClass::GeneratedSummary) => 0.08,
        _ => 0.0,
    }
}

fn evidence_score(input: &MemoryCandidatePolicyInput) -> f32 {
    let base: f32 = match input.evidence_class {
        MemoryEvidenceClass::UserApproval | MemoryEvidenceClass::UserCorrection => 0.14,
        MemoryEvidenceClass::DirectUserAssertion => 0.12,
        MemoryEvidenceClass::GeneratedSummary => 0.06,
        MemoryEvidenceClass::AssistantInference
        | MemoryEvidenceClass::ToolObservation
        | MemoryEvidenceClass::TaskRuntimeObservation
        | MemoryEvidenceClass::SystemObservation
        | MemoryEvidenceClass::MissingOrWeak => 0.0,
    };
    let repetition_bonus: f32 = match input.evidence_count {
        0 | 1 => 0.0,
        2 => 0.02,
        _ => 0.04,
    };
    (base + repetition_bonus).min(0.16)
}

fn fact_class_score(input: &MemoryCandidatePolicyInput) -> f32 {
    match input.fact_class {
        MemoryFactClass::UserIdentity => 0.14,
        MemoryFactClass::UserBiography
        | MemoryFactClass::UserRelationship
        | MemoryFactClass::StableUserPreference
        | MemoryFactClass::CommunicationPreference
        | MemoryFactClass::RecurringUserInstruction
        | MemoryFactClass::ProjectPolicy
        | MemoryFactClass::ProjectDecision
        | MemoryFactClass::ProjectProcedure
        | MemoryFactClass::ProjectConstraint
        | MemoryFactClass::AssistantSelfDescription => 0.13,
        MemoryFactClass::ThreadLocalState
        | MemoryFactClass::TaskLifecycleState
        | MemoryFactClass::ToolResultFact
        | MemoryFactClass::DomainOwnedState
        | MemoryFactClass::OperationalObservation
        | MemoryFactClass::GeneratedSummaryFact
        | MemoryFactClass::SecretOrCredential
        | MemoryFactClass::RegulatedSensitiveFact
        | MemoryFactClass::Unknown => 0.0,
    }
}

fn lifetime_fit_score(input: &MemoryCandidatePolicyInput) -> f32 {
    match (input.lifetime_class, input.ownership_class) {
        (
            MemoryLifetimeClass::LongLived,
            MemoryOwnershipClass::DurableUserMemory | MemoryOwnershipClass::DurableAgentMemory,
        ) => 0.12,
        (
            MemoryLifetimeClass::LongLived | MemoryLifetimeClass::ProjectLifetime,
            MemoryOwnershipClass::DurableWorkspaceMemory,
        ) => 0.12,
        _ => 0.0,
    }
}

fn scope_fit_score(input: &MemoryCandidatePolicyInput) -> f32 {
    match input.scope_clarity {
        MemoryScopeClarity::Clear => 0.10,
        MemoryScopeClarity::Inferred => 0.06,
        MemoryScopeClarity::Unclear => 0.0,
    }
}

fn ownership_fit_score(input: &MemoryCandidatePolicyInput) -> f32 {
    if input.ownership_class == input.quality_target_ownership
        && matches!(
            input.ownership_class,
            MemoryOwnershipClass::DurableUserMemory
                | MemoryOwnershipClass::DurableWorkspaceMemory
                | MemoryOwnershipClass::DurableAgentMemory
        )
    {
        return 0.16;
    }
    0.0
}

fn relation_score(input: &MemoryCandidatePolicyInput) -> f32 {
    match input.relation {
        MemoryWriteRelation::Novel => 0.06,
        MemoryWriteRelation::CompatibleUpdate => 0.05,
        MemoryWriteRelation::Contradiction
        | MemoryWriteRelation::Duplicate
        | MemoryWriteRelation::SuppressedByRejection => 0.0,
    }
}

fn sensitivity_score(input: &MemoryCandidatePolicyInput) -> f32 {
    match input.sensitivity {
        MemorySensitivity::Normal => 0.08,
        MemorySensitivity::Personal => 0.04,
        MemorySensitivity::SecretLike | MemorySensitivity::Regulated => 0.0,
    }
}

fn extractor_certainty_score(input: &MemoryCandidatePolicyInput) -> f32 {
    match input.semantic.certainty {
        pioneer_protocol::MemoryExtractorCertainty::High => 0.10,
        pioneer_protocol::MemoryExtractorCertainty::Medium => 0.04,
        pioneer_protocol::MemoryExtractorCertainty::Low => 0.0,
    }
}

fn penalty_score(input: &MemoryCandidatePolicyInput) -> f32 {
    let mut penalty: f32 = 0.0;
    if input.has_contradiction || input.relation == MemoryWriteRelation::Contradiction {
        penalty += 0.30;
    }
    if input.has_duplicate
        || input.has_rejected_duplicate
        || matches!(
            input.relation,
            MemoryWriteRelation::Duplicate | MemoryWriteRelation::SuppressedByRejection
        )
    {
        penalty += 0.40;
    }
    if input.scope_clarity == MemoryScopeClarity::Unclear {
        penalty += 0.10;
    }
    if input.semantic.certainty == pioneer_protocol::MemoryExtractorCertainty::Low {
        penalty += 0.08;
    }
    if matches!(
        input.semantic.explicitness,
        MemoryExplicitness::Unclear | MemoryExplicitness::None
    ) {
        penalty += 0.18;
    }
    penalty.min(1.0)
}

fn explicitness_reason(input: &MemoryCandidatePolicyInput) -> &'static str {
    match input.semantic.explicitness {
        MemoryExplicitness::Explicit => "explicitness:explicit",
        MemoryExplicitness::Implicit => "explicitness:implicit",
        MemoryExplicitness::Unclear => "explicitness:unclear",
        MemoryExplicitness::None => "explicitness:none",
    }
}

fn source_trust_reason(input: &MemoryCandidatePolicyInput) -> &'static str {
    match input.source_context_kind {
        MemorySourceContextKind::DirectUserConversation => "source_trust:direct_user",
        MemorySourceContextKind::AssistantResponse => "source_trust:assistant",
        MemorySourceContextKind::ToolResult => "source_trust:tool",
        MemorySourceContextKind::TaskRuntime => "source_trust:task",
        MemorySourceContextKind::SystemRuntime | MemorySourceContextKind::DeveloperInstruction => {
            "source_trust:system"
        }
        MemorySourceContextKind::ConnectorContent | MemorySourceContextKind::ImportedDocument => {
            "source_trust:connector"
        }
        MemorySourceContextKind::GeneratedSummary => "source_trust:generated_summary",
        MemorySourceContextKind::Unknown => "source_trust:unknown",
    }
}

fn evidence_reason(input: &MemoryCandidatePolicyInput) -> &'static str {
    match input.evidence_class {
        MemoryEvidenceClass::DirectUserAssertion => "evidence:user_assertion",
        MemoryEvidenceClass::UserCorrection => "evidence:user_correction",
        MemoryEvidenceClass::UserApproval => "evidence:user_approval",
        MemoryEvidenceClass::AssistantInference => "evidence:assistant_inference",
        MemoryEvidenceClass::ToolObservation => "evidence:tool_observation",
        MemoryEvidenceClass::TaskRuntimeObservation => "evidence:task_runtime",
        MemoryEvidenceClass::SystemObservation => "evidence:system",
        MemoryEvidenceClass::GeneratedSummary => "evidence:generated_summary",
        MemoryEvidenceClass::MissingOrWeak => "evidence:missing_or_weak",
    }
}

fn fact_class_reason(input: &MemoryCandidatePolicyInput) -> &'static str {
    match input.fact_class {
        MemoryFactClass::UserIdentity => "fact_class:user_identity",
        MemoryFactClass::UserBiography => "fact_class:user_biography",
        MemoryFactClass::UserRelationship => "fact_class:user_relationship",
        MemoryFactClass::StableUserPreference => "fact_class:stable_user_preference",
        MemoryFactClass::CommunicationPreference => "fact_class:communication_preference",
        MemoryFactClass::RecurringUserInstruction => "fact_class:recurring_user_instruction",
        MemoryFactClass::ProjectPolicy => "fact_class:project_policy",
        MemoryFactClass::ProjectDecision => "fact_class:project_decision",
        MemoryFactClass::ProjectProcedure => "fact_class:project_procedure",
        MemoryFactClass::ProjectConstraint => "fact_class:project_constraint",
        MemoryFactClass::AssistantSelfDescription => "fact_class:assistant_self_description",
        MemoryFactClass::ThreadLocalState => "fact_class:thread_local_state",
        MemoryFactClass::TaskLifecycleState => "fact_class:task_lifecycle_state",
        MemoryFactClass::ToolResultFact => "fact_class:tool_result",
        MemoryFactClass::DomainOwnedState => "fact_class:domain_owned_state",
        MemoryFactClass::OperationalObservation => "fact_class:operational_observation",
        MemoryFactClass::GeneratedSummaryFact => "fact_class:generated_summary",
        MemoryFactClass::SecretOrCredential => "fact_class:secret_or_credential",
        MemoryFactClass::RegulatedSensitiveFact => "fact_class:regulated_sensitive",
        MemoryFactClass::Unknown => "fact_class:unknown",
    }
}

fn lifetime_reason(input: &MemoryCandidatePolicyInput) -> &'static str {
    match input.lifetime_class {
        MemoryLifetimeClass::LongLived => "lifetime:long_lived",
        MemoryLifetimeClass::ProjectLifetime => "lifetime:project_lifetime",
        MemoryLifetimeClass::ThreadLifetime => "lifetime:thread_lifetime",
        MemoryLifetimeClass::TaskLifetime => "lifetime:task_lifetime",
        MemoryLifetimeClass::SessionOnly => "lifetime:session_only",
        MemoryLifetimeClass::Instantaneous => "lifetime:instantaneous",
        MemoryLifetimeClass::NaturallyExpiring => "lifetime:naturally_expiring",
        MemoryLifetimeClass::Unknown => "lifetime:unknown",
    }
}

fn scope_reason(input: &MemoryCandidatePolicyInput) -> &'static str {
    match input.scope_clarity {
        MemoryScopeClarity::Clear => "scope_fit:clear",
        MemoryScopeClarity::Inferred => "scope_fit:inferred",
        MemoryScopeClarity::Unclear => "scope_fit:unclear",
    }
}

fn ownership_reason(input: &MemoryCandidatePolicyInput) -> &'static str {
    match input.ownership_class {
        MemoryOwnershipClass::DurableUserMemory => "ownership_fit:durable_user_memory",
        MemoryOwnershipClass::DurableWorkspaceMemory => "ownership_fit:durable_workspace_memory",
        MemoryOwnershipClass::DurableAgentMemory => "ownership_fit:durable_agent_memory",
        MemoryOwnershipClass::ThreadEpisodicContext => "ownership_fit:thread_episodic_context",
        MemoryOwnershipClass::TaskRuntimeState => "ownership_fit:task_runtime_state",
        MemoryOwnershipClass::DomainRuntimeState => "ownership_fit:domain_runtime_state",
        MemoryOwnershipClass::AuditOnly => "ownership_fit:audit_only",
        MemoryOwnershipClass::Reject => "ownership_fit:reject",
    }
}

fn relation_reason(input: &MemoryCandidatePolicyInput) -> &'static str {
    match input.relation {
        MemoryWriteRelation::Novel => "relation:novel",
        MemoryWriteRelation::CompatibleUpdate => "relation:compatible_update",
        MemoryWriteRelation::Contradiction => "relation:contradiction",
        MemoryWriteRelation::Duplicate => "relation:duplicate",
        MemoryWriteRelation::SuppressedByRejection => "relation:suppressed_by_rejection",
    }
}

fn sensitivity_reason(input: &MemoryCandidatePolicyInput) -> &'static str {
    match input.sensitivity {
        MemorySensitivity::Normal => "sensitivity:normal",
        MemorySensitivity::Personal => "sensitivity:personal",
        MemorySensitivity::SecretLike => "sensitivity:secret_like",
        MemorySensitivity::Regulated => "sensitivity:regulated",
    }
}

fn certainty_reason(input: &MemoryCandidatePolicyInput) -> &'static str {
    match input.semantic.certainty {
        pioneer_protocol::MemoryExtractorCertainty::High => "extractor_certainty:high",
        pioneer_protocol::MemoryExtractorCertainty::Medium => "extractor_certainty:medium",
        pioneer_protocol::MemoryExtractorCertainty::Low => "extractor_certainty:low",
    }
}

fn push_reason(reasons: &mut Vec<String>, score: f32, label: &str) {
    if score > 0.0 {
        reasons.push(label.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        MemoryAttribute, MemoryCategory, MemoryExtractorCertainty, MemoryQualityReasonCode,
        MemoryScope, MemoryScopeHint, MemoryScopeKind, MemorySemanticFields, MemorySourceKind,
        MemorySubject,
    };

    fn base_input() -> MemoryCandidatePolicyInput {
        MemoryCandidatePolicyInput {
            semantic: MemorySemanticFields {
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
                sensitivity: MemorySensitivityHint::None,
                certainty: MemoryExtractorCertainty::High,
            },
            relation: MemoryWriteRelation::Novel,
            scope: MemoryScope {
                kind: MemoryScopeKind::User,
                key: "default".to_owned(),
            },
            scope_clarity: MemoryScopeClarity::Clear,
            evidence_count: 1,
            has_contradiction: false,
            has_duplicate: false,
            has_rejected_duplicate: false,
            sensitivity: MemorySensitivity::Normal,
            active_no_memory_policy: false,
            source_kind: MemorySourceKind::ExplicitUserRequest,
            quality_action: MemoryQualityAction::CandidatePolicy,
            quality_target_ownership: MemoryOwnershipClass::DurableUserMemory,
            quality_reason_codes: vec![MemoryQualityReasonCode::CandidatePolicyAllowed],
            quality_candidate_auto_approve_allowed: true,
            source_context_kind: MemorySourceContextKind::DirectUserConversation,
            fact_class: MemoryFactClass::UserIdentity,
            lifetime_class: MemoryLifetimeClass::LongLived,
            ownership_class: MemoryOwnershipClass::DurableUserMemory,
            evidence_class: MemoryEvidenceClass::DirectUserAssertion,
            hook_run_id: None,
        }
    }

    fn assert_high_auto_approve(output: MemoryCandidatePolicyOutput) {
        assert_eq!(output.score.bucket, MemoryCandidateScoreBucket::High);
        assert_eq!(output.decision, MemoryCandidatePolicyDecision::AutoApprove);
        assert_eq!(output.status, MemoryCandidateStatus::Approved);
        assert_eq!(output.reason_code, "high_confidence_durable_fact");
        assert_eq!(output.score.score_version, "quality_v1");
        assert!(output.score.source_trust_score > 0.0);
        assert!(output.score.evidence_score > 0.0);
        assert!(output.score.ownership_fit_score > 0.0);
    }

    #[test]
    fn explicit_durable_candidate_scores_high_and_auto_approves() {
        let output = MemoryCandidatePolicyEngine::new(MemoryCandidatePolicyConfig::default())
            .decide(base_input());
        assert_eq!(output.score.bucket, MemoryCandidateScoreBucket::High);
        assert_eq!(output.decision, MemoryCandidatePolicyDecision::AutoApprove);
        assert_eq!(output.status, MemoryCandidateStatus::Approved);
        assert_eq!(output.score.score_version, "quality_v1");
        assert!(output.score.source_trust_score > 0.0);
        assert!(output.score.fact_class_score > 0.0);
        assert!(output.score.lifetime_fit_score > 0.0);
        assert!(output.score.ownership_fit_score > 0.0);
        assert_eq!(output.score.penalty_score, 0.0);
        assert!(
            output
                .score
                .reasons
                .iter()
                .any(|reason| { reason == "source_trust:direct_user" })
        );
        assert!(
            output
                .score
                .reasons
                .iter()
                .any(|reason| { reason == "ownership_fit:durable_user_memory" })
        );
    }

    #[test]
    fn score_components_are_typed_and_deterministic() {
        let components = score_components(&base_input());

        assert_eq!(components.source_trust_score, 0.18);
        assert_eq!(components.evidence_score, 0.12);
        assert_eq!(components.fact_class_score, 0.14);
        assert_eq!(components.lifetime_fit_score, 0.12);
        assert_eq!(components.scope_score, 0.10);
        assert_eq!(components.ownership_fit_score, 0.16);
        assert_eq!(components.relation_score, 0.06);
        assert_eq!(components.sensitivity_score, 0.08);
        assert_eq!(components.certainty_score, 0.10);
        assert_eq!(components.penalty_score, 0.0);
    }

    #[test]
    fn class_based_golden_scores_cover_durable_allow_rows() {
        let engine = MemoryCandidatePolicyEngine::new(MemoryCandidatePolicyConfig::default());

        let mut personal_preference = base_input();
        personal_preference.semantic.category = MemoryCategory::Preference;
        personal_preference.semantic.attribute = MemoryAttribute::CommunicationStyle;
        personal_preference.semantic.sensitivity = MemorySensitivityHint::Personal;
        personal_preference.sensitivity = MemorySensitivity::Personal;
        personal_preference.fact_class = MemoryFactClass::CommunicationPreference;
        assert_high_auto_approve(engine.decide(personal_preference));

        let mut project_decision = base_input();
        project_decision.semantic.category = MemoryCategory::ProjectDecision;
        project_decision.semantic.subject = MemorySubject::Workspace;
        project_decision.semantic.attribute = MemoryAttribute::MigrationPolicy;
        project_decision.semantic.scope_hint = MemoryScopeHint::ProjectWorkspace;
        project_decision.semantic.durability = MemoryDurability::ProjectLifetime;
        project_decision.scope = MemoryScope {
            kind: MemoryScopeKind::Workspace,
            key: "workspace-1".to_owned(),
        };
        project_decision.quality_target_ownership = MemoryOwnershipClass::DurableWorkspaceMemory;
        project_decision.fact_class = MemoryFactClass::ProjectDecision;
        project_decision.lifetime_class = MemoryLifetimeClass::ProjectLifetime;
        project_decision.ownership_class = MemoryOwnershipClass::DurableWorkspaceMemory;
        assert_high_auto_approve(engine.decide(project_decision));

        let mut user_instruction = base_input();
        user_instruction.semantic.category = MemoryCategory::RecurringInstruction;
        user_instruction.semantic.attribute = MemoryAttribute::ReviewStyle;
        user_instruction.fact_class = MemoryFactClass::RecurringUserInstruction;
        assert_high_auto_approve(engine.decide(user_instruction));

        let mut workspace_instruction = base_input();
        workspace_instruction.semantic.category = MemoryCategory::RecurringInstruction;
        workspace_instruction.semantic.subject = MemorySubject::Workspace;
        workspace_instruction.semantic.scope_hint = MemoryScopeHint::ProjectWorkspace;
        workspace_instruction.scope = MemoryScope {
            kind: MemoryScopeKind::Workspace,
            key: "workspace-1".to_owned(),
        };
        workspace_instruction.quality_target_ownership =
            MemoryOwnershipClass::DurableWorkspaceMemory;
        workspace_instruction.fact_class = MemoryFactClass::RecurringUserInstruction;
        workspace_instruction.ownership_class = MemoryOwnershipClass::DurableWorkspaceMemory;
        assert_high_auto_approve(engine.decide(workspace_instruction));

        let mut agent_self_description = base_input();
        agent_self_description.semantic.category = MemoryCategory::Identity;
        agent_self_description.semantic.subject = MemorySubject::CurrentAgent;
        agent_self_description.semantic.attribute = MemoryAttribute::Name;
        agent_self_description.semantic.scope_hint = MemoryScopeHint::AgentGlobal;
        agent_self_description.scope = MemoryScope {
            kind: MemoryScopeKind::Agent,
            key: "agent-1".to_owned(),
        };
        agent_self_description.quality_target_ownership = MemoryOwnershipClass::DurableAgentMemory;
        agent_self_description.source_context_kind =
            MemorySourceContextKind::DirectUserConversation;
        agent_self_description.evidence_class = MemoryEvidenceClass::UserApproval;
        agent_self_description.fact_class = MemoryFactClass::AssistantSelfDescription;
        agent_self_description.ownership_class = MemoryOwnershipClass::DurableAgentMemory;
        assert_high_auto_approve(engine.decide(agent_self_description));
    }

    #[test]
    fn class_based_golden_scores_cover_relation_and_penalty_paths() {
        let engine = MemoryCandidatePolicyEngine::new(MemoryCandidatePolicyConfig::default());

        let mut compatible = base_input();
        compatible.relation = MemoryWriteRelation::CompatibleUpdate;
        let compatible_output = engine.decide(compatible);
        assert_eq!(
            compatible_output.score.bucket,
            MemoryCandidateScoreBucket::High
        );
        assert_eq!(
            compatible_output.decision,
            MemoryCandidatePolicyDecision::AutoApprove
        );
        assert!(
            compatible_output
                .score
                .reasons
                .iter()
                .any(|reason| reason == "relation:compatible_update")
        );

        let mut low_certainty = base_input();
        low_certainty.semantic.certainty = MemoryExtractorCertainty::Low;
        let low_certainty_output = engine.decide(low_certainty);
        assert_eq!(
            low_certainty_output.score.bucket,
            MemoryCandidateScoreBucket::Middle
        );
        assert_eq!(
            low_certainty_output.reason_code,
            "review_disabled_middle_confidence"
        );
        assert!(low_certainty_output.score.penalty_score > 0.0);

        let mut unclear_scope = base_input();
        unclear_scope.scope_clarity = MemoryScopeClarity::Unclear;
        let unclear_scope_output = engine.decide(unclear_scope);
        assert_eq!(
            unclear_scope_output.score.bucket,
            MemoryCandidateScoreBucket::Middle
        );
        assert_eq!(unclear_scope_output.score.scope_score, 0.0);
        assert!(unclear_scope_output.score.penalty_score > 0.0);
    }

    #[test]
    fn implicit_candidate_auto_approves_only_when_enabled() {
        let mut input = base_input();
        input.semantic.intent = MemoryIntent::ImplicitCandidate;
        input.semantic.explicitness = MemoryExplicitness::Implicit;

        let disabled = MemoryCandidatePolicyEngine::new(MemoryCandidatePolicyConfig::default())
            .decide(input.clone());
        assert_eq!(
            disabled.decision,
            MemoryCandidatePolicyDecision::RejectReviewDisabled
        );
        assert_eq!(disabled.reason_code, "implicit_auto_approve_disabled");

        let mut config = MemoryCandidatePolicyConfig::default();
        config.allow_implicit_auto_approve = true;
        let enabled = MemoryCandidatePolicyEngine::new(config).decide(input);
        assert_eq!(enabled.decision, MemoryCandidatePolicyDecision::AutoApprove);
    }

    #[test]
    fn quality_decision_can_disable_auto_approve_for_high_explicit_candidate() {
        let mut input = base_input();
        input.quality_candidate_auto_approve_allowed = false;

        let disabled = MemoryCandidatePolicyEngine::new(MemoryCandidatePolicyConfig::default())
            .decide(input.clone());
        assert_eq!(disabled.score.bucket, MemoryCandidateScoreBucket::High);
        assert_eq!(
            disabled.decision,
            MemoryCandidatePolicyDecision::RejectReviewDisabled
        );
        assert_eq!(disabled.reason_code, "quality_auto_approve_disabled");

        let mut config = MemoryCandidatePolicyConfig::default();
        config.review_enabled = true;
        let review = MemoryCandidatePolicyEngine::new(config).decide(input);
        assert_eq!(review.decision, MemoryCandidatePolicyDecision::NeedsReview);
        assert_eq!(review.reason_code, "quality_auto_approve_disabled");
    }

    #[test]
    fn secret_and_transient_candidates_force_auto_reject() {
        let mut secret = base_input();
        secret.semantic.sensitivity = MemorySensitivityHint::Secret;
        let secret_output =
            MemoryCandidatePolicyEngine::new(MemoryCandidatePolicyConfig::default()).decide(secret);
        assert_eq!(
            secret_output.score.bucket,
            MemoryCandidateScoreBucket::ExtremelyLow
        );
        assert_eq!(
            secret_output.decision,
            MemoryCandidatePolicyDecision::AutoReject
        );

        let mut transient = base_input();
        transient.semantic.durability = MemoryDurability::Transient;
        let transient_output =
            MemoryCandidatePolicyEngine::new(MemoryCandidatePolicyConfig::default())
                .decide(transient);
        assert_eq!(transient_output.reason_code, "transient_or_session_only");
    }

    #[test]
    fn non_candidate_policy_quality_action_is_defensively_rejected() {
        let mut input = base_input();
        input.quality_action = MemoryQualityAction::Quarantine;
        input.quality_candidate_auto_approve_allowed = false;

        let output =
            MemoryCandidatePolicyEngine::new(MemoryCandidatePolicyConfig::default()).decide(input);

        assert_eq!(
            output.score.bucket,
            MemoryCandidateScoreBucket::ExtremelyLow
        );
        assert_eq!(output.score.total_score, 0.0);
        assert_eq!(output.decision, MemoryCandidatePolicyDecision::AutoReject);
        assert_eq!(output.reason_code, "quality_action_not_candidate_policy");
    }

    #[test]
    fn raw_extractor_labels_do_not_create_high_score_without_quality_fit() {
        let mut input = base_input();
        input.semantic.explicitness = MemoryExplicitness::Explicit;
        input.semantic.certainty = MemoryExtractorCertainty::High;
        input.semantic.durability = MemoryDurability::LongLived;
        input.source_context_kind = MemorySourceContextKind::Unknown;
        input.evidence_class = MemoryEvidenceClass::MissingOrWeak;
        input.ownership_class = MemoryOwnershipClass::AuditOnly;
        input.quality_target_ownership = MemoryOwnershipClass::AuditOnly;
        input.scope_clarity = MemoryScopeClarity::Clear;

        let output =
            MemoryCandidatePolicyEngine::new(MemoryCandidatePolicyConfig::default()).decide(input);

        assert_ne!(output.score.bucket, MemoryCandidateScoreBucket::High);
        assert!(output.score.source_trust_score == 0.0);
        assert!(output.score.evidence_score == 0.0);
        assert!(output.score.ownership_fit_score == 0.0);
    }

    #[test]
    fn contradiction_never_auto_approves_even_with_high_score() {
        let mut input = base_input();
        input.relation = MemoryWriteRelation::Contradiction;
        input.has_contradiction = true;
        input.quality_candidate_auto_approve_allowed = false;

        let disabled = MemoryCandidatePolicyEngine::new(MemoryCandidatePolicyConfig::default())
            .decide(input.clone());
        assert_eq!(
            disabled.decision,
            MemoryCandidatePolicyDecision::RejectReviewDisabled
        );
        assert_eq!(disabled.reason_code, "review_disabled_contradiction");

        let mut config = MemoryCandidatePolicyConfig::default();
        config.review_enabled = true;
        let review = MemoryCandidatePolicyEngine::new(config).decide(input);
        assert_eq!(review.decision, MemoryCandidatePolicyDecision::AskOnUse);
        assert_eq!(review.reason_code, "review_enabled_contradiction");
    }

    #[test]
    fn middle_candidate_routes_to_review_only_when_enabled() {
        let mut input = base_input();
        input.semantic.explicitness = MemoryExplicitness::Unclear;
        input.semantic.intent = MemoryIntent::ImplicitCandidate;
        input.semantic.certainty = MemoryExtractorCertainty::Medium;
        input.semantic.durability = MemoryDurability::Unknown;
        input.evidence_count = 1;
        input.scope_clarity = MemoryScopeClarity::Inferred;

        let disabled = MemoryCandidatePolicyEngine::new(MemoryCandidatePolicyConfig::default())
            .decide(input.clone());
        assert_eq!(
            disabled.decision,
            MemoryCandidatePolicyDecision::RejectReviewDisabled
        );
        assert_eq!(
            disabled.status,
            MemoryCandidateStatus::ReviewDisabledRejected
        );
        assert_eq!(disabled.reason_code, "review_disabled_middle_confidence");

        let mut config = MemoryCandidatePolicyConfig::default();
        config.review_enabled = true;
        let enabled = MemoryCandidatePolicyEngine::new(config).decide(input);
        assert_eq!(
            enabled.decision,
            MemoryCandidatePolicyDecision::PendingSilent
        );
        assert_eq!(enabled.status, MemoryCandidateStatus::PendingSilent);
    }
}
