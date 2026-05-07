use crate::config::MemoryCandidatePolicyConfig;
use pioneer_protocol::{
    MemoryCandidatePolicyDecision, MemoryCandidatePolicyInput, MemoryCandidatePolicyOutput,
    MemoryCandidateScore, MemoryCandidateScoreBucket, MemoryCandidateStatus, MemoryDurability,
    MemoryExplicitness, MemoryIntent, MemoryScopeClarity, MemorySensitivity, MemorySensitivityHint,
    MemoryWriteRelation,
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

        if self.config.review_enabled {
            MemoryCandidatePolicyOutput {
                input,
                score,
                decision: MemoryCandidatePolicyDecision::NeedsReview,
                status: MemoryCandidateStatus::NeedsReview,
                reason_code: "auto_approve_not_allowed".to_owned(),
                reason: Some(
                    "high-score candidate cannot auto-approve under current policy".to_owned(),
                ),
            }
        } else {
            MemoryCandidatePolicyOutput {
                input,
                score,
                decision: MemoryCandidatePolicyDecision::RejectReviewDisabled,
                status: MemoryCandidateStatus::ReviewDisabledRejected,
                reason_code: "implicit_auto_approve_disabled".to_owned(),
                reason: Some(
                    "implicit memory candidate suppressed because proactive writes are disabled"
                        .to_owned(),
                ),
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

fn forced_reject_reason(input: &MemoryCandidatePolicyInput) -> Option<&'static str> {
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
    let explicitness_score: f32 = match input.semantic.explicitness {
        MemoryExplicitness::Explicit => 0.25,
        MemoryExplicitness::Implicit => 0.18,
        MemoryExplicitness::Unclear => 0.06,
        MemoryExplicitness::None => 0.0,
    };
    let durability_score: f32 = match input.semantic.durability {
        MemoryDurability::LongLived | MemoryDurability::ProjectLifetime => 0.20,
        MemoryDurability::Unknown => 0.07,
        MemoryDurability::SessionOnly | MemoryDurability::Transient => 0.0,
    };
    let scope_score: f32 = match input.scope_clarity {
        MemoryScopeClarity::Clear => 0.15,
        MemoryScopeClarity::Inferred => 0.10,
        MemoryScopeClarity::Unclear => 0.0,
    };
    let evidence_score: f32 = match input.evidence_count {
        0 => 0.0,
        1 => 0.10,
        2 => 0.15,
        _ => 0.18,
    };
    let certainty_score: f32 = match input.semantic.certainty {
        pioneer_protocol::MemoryExtractorCertainty::High => 0.15,
        pioneer_protocol::MemoryExtractorCertainty::Medium => 0.08,
        pioneer_protocol::MemoryExtractorCertainty::Low => 0.0,
    };
    let sensitivity_score: f32 = match input.sensitivity {
        MemorySensitivity::Normal => 0.10,
        MemorySensitivity::Personal => 0.04,
        MemorySensitivity::SecretLike | MemorySensitivity::Regulated => 0.0,
    };
    let relation_score: f32 = match input.relation {
        MemoryWriteRelation::Novel => 0.05,
        MemoryWriteRelation::CompatibleUpdate => 0.03,
        MemoryWriteRelation::Duplicate
        | MemoryWriteRelation::Contradiction
        | MemoryWriteRelation::SuppressedByRejection => 0.0,
    };

    let total_score: f32 = (explicitness_score
        + durability_score
        + scope_score
        + evidence_score
        + certainty_score
        + sensitivity_score
        + relation_score)
        .clamp(0.0_f32, 1.0_f32);
    let mut reasons = Vec::new();
    push_reason(&mut reasons, explicitness_score, "explicitness");
    push_reason(&mut reasons, durability_score, "durability");
    push_reason(&mut reasons, scope_score, "scope_clarity");
    push_reason(&mut reasons, evidence_score, "evidence");
    push_reason(&mut reasons, certainty_score, "extractor_certainty");
    push_reason(&mut reasons, sensitivity_score, "sensitivity");
    push_reason(&mut reasons, relation_score, "relation");
    if input.has_contradiction {
        reasons.push("contradiction".to_owned());
    }
    if input.has_duplicate {
        reasons.push("duplicate".to_owned());
    }

    MemoryCandidateScore {
        total_score,
        bucket: MemoryCandidateScoreBucket::Middle,
        explicitness_score,
        durability_score,
        scope_score,
        evidence_score,
        certainty_score,
        sensitivity_score,
        relation_score,
        reasons,
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
        MemoryAttribute, MemoryCategory, MemoryExtractorCertainty, MemoryScope, MemoryScopeHint,
        MemoryScopeKind, MemorySemanticFields, MemorySourceKind, MemorySubject,
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
            hook_run_id: None,
        }
    }

    #[test]
    fn explicit_durable_candidate_scores_high_and_auto_approves() {
        let output = MemoryCandidatePolicyEngine::new(MemoryCandidatePolicyConfig::default())
            .decide(base_input());
        assert_eq!(output.score.bucket, MemoryCandidateScoreBucket::High);
        assert_eq!(output.decision, MemoryCandidatePolicyDecision::AutoApprove);
        assert_eq!(output.status, MemoryCandidateStatus::Approved);
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
