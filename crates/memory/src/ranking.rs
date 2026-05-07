use crate::config::MemoryRankingConfig;
use crate::context::MemoryActiveScopes;
use pioneer_protocol::{MemoryCategory, MemorySearchHit};
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub struct MemoryRankingCandidate {
    pub hit: MemorySearchHit,
    pub backend_score: Option<f32>,
    pub recency_anchor_unix: i64,
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
}

pub fn rank_memory_search_hits(
    candidates: Vec<MemoryRankingCandidate>,
    query: &str,
    requested_categories: &[MemoryCategory],
    active_scopes: &MemoryActiveScopes,
    config: &MemoryRankingConfig,
    now_unix: i64,
    limit: u32,
) -> Vec<MemorySearchHit> {
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

    ranked
        .into_iter()
        .map(|ranked| {
            let mut hit = ranked.hit;
            hit.score = Some(ranked.final_score);
            hit
        })
        .collect()
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
        + confidence * config.confidence_weight.max(0.0);

    RankedMemorySearchHit {
        hit: candidate.hit,
        final_score: finite_score(final_score),
        exact_key_match,
        scope_rank,
        importance,
        confidence,
        recency_anchor_unix: candidate.recency_anchor_unix,
    }
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
