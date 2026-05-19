use crate::config::MemoryRankingConfig;
use crate::context::MemoryActiveScopes;
use crate::recall_visibility::MemoryRecallQualitySignals;
use pioneer_protocol::{
    MemoryCategory, MemoryEvidenceClass, MemoryFactClass, MemoryOwnershipClass, MemorySearchHit,
    MemorySourceContextKind,
};
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub struct MemoryRankingCandidate {
    pub hit: MemorySearchHit,
    pub backend_score: Option<f32>,
    pub recency_anchor_unix: i64,
    pub quality: MemoryRecallQualitySignals,
}

#[derive(Debug)]
struct RankedMemorySearchHit {
    hit: MemorySearchHit,
    final_score: f32,
    exact_key_match: bool,
    scope_rank: Option<u32>,
    importance: f32,
    confidence: f32,
    recency_anchor_unix: i64,
    quality_penalty_applied: bool,
    low_source_context_penalty_applied: bool,
    rejected_related_penalty_applied: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemoryRankingDiagnostics {
    pub exact_key_boost_count: usize,
    pub quality_penalty_applied_count: usize,
    pub low_source_context_penalty_count: usize,
    pub rejected_related_penalty_count: usize,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MemoryRankingResult {
    pub hits: Vec<MemorySearchHit>,
    pub diagnostics: MemoryRankingDiagnostics,
}

#[cfg(test)]
pub fn rank_memory_search_hits(
    candidates: Vec<MemoryRankingCandidate>,
    query: &str,
    requested_categories: &[MemoryCategory],
    active_scopes: &MemoryActiveScopes,
    config: &MemoryRankingConfig,
    now_unix: i64,
    limit: u32,
) -> Vec<MemorySearchHit> {
    rank_memory_search_hits_with_diagnostics(
        candidates,
        query,
        requested_categories,
        active_scopes,
        config,
        now_unix,
        limit,
    )
    .hits
}

pub fn rank_memory_search_hits_with_diagnostics(
    candidates: Vec<MemoryRankingCandidate>,
    query: &str,
    requested_categories: &[MemoryCategory],
    active_scopes: &MemoryActiveScopes,
    config: &MemoryRankingConfig,
    now_unix: i64,
    limit: u32,
) -> MemoryRankingResult {
    let normalized_query = normalize_memory_query(query);
    let mut ranked = candidates
        .into_iter()
        .map(|candidate| {
            rank_candidate(
                candidate,
                normalized_query.as_str(),
                requested_categories,
                active_scopes,
                config,
                now_unix,
            )
        })
        .collect::<Vec<_>>();

    ranked.sort_by(compare_ranked_hits);
    ranked.truncate(limit as usize);

    let diagnostics = ranking_diagnostics(&ranked);
    let hits = ranked
        .into_iter()
        .map(|ranked| {
            let mut hit = ranked.hit;
            hit.score = Some(ranked.final_score);
            hit
        })
        .collect();

    MemoryRankingResult { hits, diagnostics }
}

fn rank_candidate(
    candidate: MemoryRankingCandidate,
    normalized_query: &str,
    requested_categories: &[MemoryCategory],
    active_scopes: &MemoryActiveScopes,
    config: &MemoryRankingConfig,
    now_unix: i64,
) -> RankedMemorySearchHit {
    let record = &candidate.hit.record;
    let backend_score = normalized_backend_score(candidate.backend_score);
    let exact_key_match =
        exact_key_matches(normalized_query, record.key.as_deref(), record.category);
    let category_match =
        !requested_categories.is_empty() && requested_categories.contains(&record.category);
    let primary_scope_match = active_scopes
        .primary_scope
        .as_ref()
        .is_some_and(|scope| scope == &record.scope);
    let scope_rank = active_scopes.rank_for(&record.scope);
    let recency_boost = recency_boost(candidate.recency_anchor_unix, now_unix, config);
    let importance = clamp_unit(record.importance);
    let confidence = clamp_unit(record.confidence);
    let quality_adjustment = quality_adjustment(
        &candidate.quality,
        record.category,
        requested_categories,
        config,
    );

    let final_score = backend_score * config.backend_score_weight.max(0.0)
        + if exact_key_match {
            config.exact_key_boost.max(0.0)
        } else {
            0.0
        }
        + if category_match {
            config.category_match_boost.max(0.0)
        } else {
            0.0
        }
        + if primary_scope_match {
            config.primary_scope_boost.max(0.0)
        } else {
            0.0
        }
        + scope_rank
            .map(|rank| config.scope_rank_boost.max(0.0) / rank.max(1) as f32)
            .unwrap_or(0.0)
        + recency_boost
        + importance * config.importance_weight.max(0.0)
        + confidence * config.confidence_weight.max(0.0)
        + quality_adjustment.score_delta;

    RankedMemorySearchHit {
        hit: candidate.hit,
        final_score: finite_score(final_score),
        exact_key_match,
        scope_rank,
        importance,
        confidence,
        recency_anchor_unix: candidate.recency_anchor_unix,
        quality_penalty_applied: quality_adjustment.penalty_applied,
        low_source_context_penalty_applied: quality_adjustment.low_source_context_penalty_applied,
        rejected_related_penalty_applied: quality_adjustment.rejected_related_penalty_applied,
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct QualityAdjustment {
    score_delta: f32,
    penalty_applied: bool,
    low_source_context_penalty_applied: bool,
    rejected_related_penalty_applied: bool,
}

fn quality_adjustment(
    quality: &MemoryRecallQualitySignals,
    category: MemoryCategory,
    requested_categories: &[MemoryCategory],
    config: &MemoryRankingConfig,
) -> QualityAdjustment {
    let mut adjustment = QualityAdjustment::default();
    let has_typed_quality = has_typed_quality(quality);

    if has_typed_quality && is_direct_user_source(quality) {
        adjustment.score_delta += config.direct_user_source_boost.max(0.0);
    }
    if has_typed_quality && is_durable_ownership(quality.ownership_class) {
        adjustment.score_delta += config.durable_ownership_boost.max(0.0);
    }
    if has_typed_quality
        && category_matches_fact_class(category, quality.fact_class)
        && (requested_categories.is_empty() || requested_categories.contains(&category))
    {
        adjustment.score_delta += config.matching_fact_class_boost.max(0.0);
    }
    if quality.is_low_quality_source_context() {
        let penalty = config.low_evidence_source_penalty.max(0.0);
        adjustment.score_delta -= penalty;
        adjustment.penalty_applied |= penalty > 0.0;
        adjustment.low_source_context_penalty_applied |= penalty > 0.0;
    }
    if has_typed_quality && is_assistant_or_generated_source(quality.source_context_kind) {
        let penalty = config.assistant_or_generated_source_penalty.max(0.0);
        adjustment.score_delta -= penalty;
        adjustment.penalty_applied |= penalty > 0.0;
    }
    if quality.is_rejected_related() {
        let penalty = config.rejected_related_penalty.max(0.0);
        adjustment.score_delta -= penalty;
        adjustment.penalty_applied |= penalty > 0.0;
        adjustment.rejected_related_penalty_applied |= penalty > 0.0;
    }
    if has_typed_quality
        && (quality.fact_class == MemoryFactClass::Unknown
            || matches!(
                quality.ownership_class,
                MemoryOwnershipClass::AuditOnly | MemoryOwnershipClass::Reject
            ))
    {
        let penalty = config.ownership_ambiguity_penalty.max(0.0);
        adjustment.score_delta -= penalty;
        adjustment.penalty_applied |= penalty > 0.0;
    }

    adjustment
}

fn has_typed_quality(quality: &MemoryRecallQualitySignals) -> bool {
    quality.quality_action.is_some()
        || !quality.quality_reason_codes.is_empty()
        || quality.fact_class != MemoryFactClass::Unknown
        || quality.ownership_class != MemoryOwnershipClass::AuditOnly
}

fn ranking_diagnostics(ranked: &[RankedMemorySearchHit]) -> MemoryRankingDiagnostics {
    MemoryRankingDiagnostics {
        exact_key_boost_count: ranked.iter().filter(|hit| hit.exact_key_match).count(),
        quality_penalty_applied_count: ranked
            .iter()
            .filter(|hit| hit.quality_penalty_applied)
            .count(),
        low_source_context_penalty_count: ranked
            .iter()
            .filter(|hit| hit.low_source_context_penalty_applied)
            .count(),
        rejected_related_penalty_count: ranked
            .iter()
            .filter(|hit| hit.rejected_related_penalty_applied)
            .count(),
    }
}

fn is_direct_user_source(quality: &MemoryRecallQualitySignals) -> bool {
    quality.source_context_kind == MemorySourceContextKind::DirectUserConversation
        && matches!(
            quality.evidence_class,
            MemoryEvidenceClass::DirectUserAssertion
                | MemoryEvidenceClass::UserCorrection
                | MemoryEvidenceClass::UserApproval
        )
}

fn is_durable_ownership(ownership_class: MemoryOwnershipClass) -> bool {
    matches!(
        ownership_class,
        MemoryOwnershipClass::DurableUserMemory
            | MemoryOwnershipClass::DurableWorkspaceMemory
            | MemoryOwnershipClass::DurableAgentMemory
    )
}

fn is_assistant_or_generated_source(source_context_kind: MemorySourceContextKind) -> bool {
    matches!(
        source_context_kind,
        MemorySourceContextKind::AssistantResponse | MemorySourceContextKind::GeneratedSummary
    )
}

fn category_matches_fact_class(category: MemoryCategory, fact_class: MemoryFactClass) -> bool {
    matches!(
        (category, fact_class),
        (MemoryCategory::Identity, MemoryFactClass::UserIdentity)
            | (
                MemoryCategory::Identity,
                MemoryFactClass::AssistantSelfDescription
            )
            | (
                MemoryCategory::Preference,
                MemoryFactClass::StableUserPreference
            )
            | (
                MemoryCategory::CommunicationStyle,
                MemoryFactClass::CommunicationPreference
            )
            | (
                MemoryCategory::RecurringInstruction,
                MemoryFactClass::RecurringUserInstruction
            )
            | (MemoryCategory::Biography, MemoryFactClass::UserBiography)
            | (
                MemoryCategory::Relationship,
                MemoryFactClass::UserRelationship
            )
            | (
                MemoryCategory::ProjectPolicy,
                MemoryFactClass::ProjectPolicy
            )
            | (
                MemoryCategory::ProjectDecision,
                MemoryFactClass::ProjectDecision
            )
            | (MemoryCategory::Procedure, MemoryFactClass::ProjectProcedure)
            | (
                MemoryCategory::Constraint,
                MemoryFactClass::ProjectConstraint
            )
            | (
                MemoryCategory::ProjectFact,
                MemoryFactClass::OperationalObservation
            )
            | (MemoryCategory::ProjectFact, MemoryFactClass::ToolResultFact)
    )
}

fn compare_ranked_hits(left: &RankedMemorySearchHit, right: &RankedMemorySearchHit) -> Ordering {
    compare_f32_desc(left.final_score, right.final_score)
        .then_with(|| right.exact_key_match.cmp(&left.exact_key_match))
        .then_with(|| {
            left.scope_rank
                .unwrap_or(u32::MAX)
                .cmp(&right.scope_rank.unwrap_or(u32::MAX))
        })
        .then_with(|| compare_f32_desc(left.importance, right.importance))
        .then_with(|| compare_f32_desc(left.confidence, right.confidence))
        .then_with(|| right.recency_anchor_unix.cmp(&left.recency_anchor_unix))
        .then_with(|| right.hit.record.updated_at.cmp(&left.hit.record.updated_at))
        .then_with(|| left.hit.record.id.cmp(&right.hit.record.id))
}

fn compare_f32_desc(left: f32, right: f32) -> Ordering {
    right.partial_cmp(&left).unwrap_or(Ordering::Equal)
}

fn normalized_backend_score(score: Option<f32>) -> f32 {
    score
        .filter(|score| score.is_finite())
        .map(|score| score.clamp(0.0, 1.0))
        .unwrap_or(0.0)
}

fn finite_score(score: f32) -> f32 {
    if score.is_finite() { score } else { 0.0 }
}

fn recency_boost(anchor_unix: i64, now_unix: i64, config: &MemoryRankingConfig) -> f32 {
    let age_secs = now_unix.saturating_sub(anchor_unix).max(0) as f32;
    let half_life_secs = config.recency_half_life_secs.max(1) as f32;
    config.recency_boost_max.max(0.0) / (1.0 + age_secs / half_life_secs)
}

fn exact_key_matches(normalized_query: &str, key: Option<&str>, category: MemoryCategory) -> bool {
    let Some(key) = key else {
        return false;
    };
    let normalized_key = normalize_memory_query(key);
    if normalized_key.is_empty() {
        return false;
    }

    normalized_query == normalized_key
        || normalized_query
            .split_whitespace()
            .any(|token| token == normalized_key)
        || normalized_query == format!("{}:{normalized_key}", category_label(category))
}

fn normalize_memory_query(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn category_label(category: MemoryCategory) -> &'static str {
    match category {
        MemoryCategory::Identity => "identity",
        MemoryCategory::Preference => "preference",
        MemoryCategory::Biography => "biography",
        MemoryCategory::Relationship => "relationship",
        MemoryCategory::RecurringInstruction => "recurring_instruction",
        MemoryCategory::ProjectPolicy => "project_policy",
        MemoryCategory::ProjectFact => "project_fact",
        MemoryCategory::ProjectDecision => "project_decision",
        MemoryCategory::Procedure => "procedure",
        MemoryCategory::Todo => "todo",
        MemoryCategory::Constraint => "constraint",
        MemoryCategory::CommunicationStyle => "communication_style",
        MemoryCategory::Custom => "custom",
    }
}

fn clamp_unit(value: f32) -> f32 {
    if value.is_finite() {
        value.clamp(0.0, 1.0)
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        MemoryProvenance, MemoryScope, MemoryScopeKind, MemorySensitivity, MemoryStatus,
    };
    use std::collections::BTreeMap;

    fn active_scopes() -> MemoryActiveScopes {
        let scope = MemoryScope {
            kind: MemoryScopeKind::User,
            key: "default".to_owned(),
        };
        MemoryActiveScopes {
            scopes: vec![scope.clone()],
            primary_scope: Some(scope.clone()),
            priorities: vec![crate::MemoryScopePriority { scope, rank: 1 }],
            explicit: false,
        }
    }

    fn record(
        id: &str,
        key: Option<&str>,
        category: MemoryCategory,
        updated_at: i64,
    ) -> MemorySearchHit {
        MemorySearchHit {
            record: pioneer_protocol::MemoryRecord {
                id: id.to_owned(),
                scope: MemoryScope {
                    kind: MemoryScopeKind::User,
                    key: "default".to_owned(),
                },
                namespace: None,
                category,
                key: key.map(str::to_owned),
                content: format!("content {id}"),
                status: MemoryStatus::Active,
                confidence: 0.8,
                importance: 0.6,
                sensitivity: MemorySensitivity::Normal,
                provenance: MemoryProvenance {
                    source_thread_id: None,
                    source_turn_id: None,
                    source_item_id: None,
                    created_by: None,
                },
                source_context_kind: Some(MemorySourceContextKind::DirectUserConversation),
                created_at: updated_at,
                updated_at,
                expires_at: None,
                last_accessed_at: None,
                access_count: 0,
                superseded_by: None,
                deleted_at: None,
                delete_reason: None,
                metadata: BTreeMap::new(),
            },
            score: None,
            snippet: None,
            matched_terms: Vec::new(),
        }
    }

    fn durable_user_quality() -> MemoryRecallQualitySignals {
        let mut quality = MemoryRecallQualitySignals::direct_user_default();
        quality.fact_class = MemoryFactClass::UserIdentity;
        quality.ownership_class = MemoryOwnershipClass::DurableUserMemory;
        quality
    }

    fn assistant_quality() -> MemoryRecallQualitySignals {
        let mut quality = durable_user_quality();
        quality.source_context_kind = MemorySourceContextKind::AssistantResponse;
        quality.evidence_class = MemoryEvidenceClass::AssistantInference;
        quality
    }

    #[test]
    fn ranking_quality_source_beats_low_quality_source_with_similar_backend_score() {
        let result = rank_memory_search_hits_with_diagnostics(
            vec![
                MemoryRankingCandidate {
                    hit: record("low", None, MemoryCategory::Identity, 10),
                    backend_score: Some(0.55),
                    recency_anchor_unix: 10,
                    quality: assistant_quality(),
                },
                MemoryRankingCandidate {
                    hit: record("high", None, MemoryCategory::Identity, 9),
                    backend_score: Some(0.45),
                    recency_anchor_unix: 9,
                    quality: durable_user_quality(),
                },
            ],
            "identity",
            &[MemoryCategory::Identity],
            &active_scopes(),
            &MemoryRankingConfig::default(),
            10,
            10,
        );

        assert_eq!(result.hits[0].record.id, "high");
        assert_eq!(result.diagnostics.quality_penalty_applied_count, 1);
    }

    #[test]
    fn ranking_exact_key_still_wins_among_visible_records() {
        let result = rank_memory_search_hits(
            vec![
                MemoryRankingCandidate {
                    hit: record("generic", None, MemoryCategory::Identity, 20),
                    backend_score: Some(1.0),
                    recency_anchor_unix: 20,
                    quality: durable_user_quality(),
                },
                MemoryRankingCandidate {
                    hit: record("exact", Some("preferred_name"), MemoryCategory::Identity, 1),
                    backend_score: Some(0.1),
                    recency_anchor_unix: 1,
                    quality: assistant_quality(),
                },
            ],
            "preferred_name",
            &[MemoryCategory::Identity],
            &active_scopes(),
            &MemoryRankingConfig::default(),
            20,
            10,
        );

        assert_eq!(result[0].record.id, "exact");
    }

    #[test]
    fn ranking_soft_penalties_do_not_suppress_records() {
        let result = rank_memory_search_hits_with_diagnostics(
            vec![MemoryRankingCandidate {
                hit: record("low", None, MemoryCategory::Identity, 10),
                backend_score: Some(0.5),
                recency_anchor_unix: 10,
                quality: assistant_quality(),
            }],
            "identity",
            &[MemoryCategory::Identity],
            &active_scopes(),
            &MemoryRankingConfig::default(),
            10,
            10,
        );

        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.diagnostics.quality_penalty_applied_count, 1);
    }

    #[test]
    fn ranking_tie_breaking_is_deterministic() {
        let result = rank_memory_search_hits(
            vec![
                MemoryRankingCandidate {
                    hit: record("b", None, MemoryCategory::Identity, 10),
                    backend_score: Some(0.5),
                    recency_anchor_unix: 10,
                    quality: durable_user_quality(),
                },
                MemoryRankingCandidate {
                    hit: record("a", None, MemoryCategory::Identity, 10),
                    backend_score: Some(0.5),
                    recency_anchor_unix: 10,
                    quality: durable_user_quality(),
                },
            ],
            "identity",
            &[MemoryCategory::Identity],
            &active_scopes(),
            &MemoryRankingConfig::default(),
            10,
            10,
        );

        assert_eq!(
            result
                .into_iter()
                .map(|hit| hit.record.id)
                .collect::<Vec<_>>(),
            vec!["a".to_owned(), "b".to_owned()]
        );
    }
}
