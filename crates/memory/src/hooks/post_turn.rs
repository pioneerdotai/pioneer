use super::*;
use crate::extractor_ontology::{
    MemoryExtractorOntologyProposal, insert_extractor_ontology_proposal_metadata,
    proposal_has_unknown_class,
};
use pioneer_protocol::{
    MemoryAttribute, MemoryEvidenceClass, MemoryExtractorCertainty, MemoryOwnershipClass,
    MemorySubject,
};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

pub(super) fn memory_post_turn_extractor_context_from_turn(
    context: &MemoryTurnContext,
    model: Option<String>,
    model_provider: Option<String>,
) -> MemoryPostTurnExtractorContext {
    MemoryPostTurnExtractorContext {
        workspace_id: context.workspace_id.clone(),
        thread_id: context.thread_id.clone(),
        turn_id: context.turn_id.clone(),
        mode: context.mode,
        model,
        model_provider,
    }
}

pub(super) fn memory_post_turn_extractor_request_from_input(
    input: &TurnPostTurnHookInput,
    manifest: MemoryManifest,
    config: &MemoryPostTurnExtractorConfig,
) -> MemoryPostTurnExtractorRequest {
    MemoryPostTurnExtractorRequest {
        user_text: input
            .user_text
            .as_ref()
            .map(|text| truncate_chars(text.text.as_str(), config.max_input_chars))
            .unwrap_or_default(),
        assistant_text: input
            .assistant_text
            .as_ref()
            .map(|text| truncate_chars(text.text.as_str(), config.max_input_chars))
            .unwrap_or_default(),
        tool_events_summary: turn_post_tool_events_summary(&input.tool_events),
        domain_events_summary: turn_post_domain_events_summary(&input.domain_events),
        manifest,
        max_facts: config.max_facts_per_turn,
    }
}

pub(super) fn memory_turn_context_from_post_turn_request(
    request: &HookHandlerRequest,
    input: &TurnPostTurnHookInput,
    config: &MemoryPostTurnExtractorConfig,
) -> HookResult<MemoryTurnContext> {
    let workspace_id = required_context_id(
        request.context.workspace_id.as_ref().map(|id| id.as_str()),
        "workspace_id",
    )?;
    let thread_id = required_context_id(
        request.context.thread_id.as_ref().map(|id| id.as_str()),
        "thread_id",
    )?;
    let turn_id = required_context_id(
        request.context.turn_id.as_ref().map(|id| id.as_str()),
        "turn_id",
    )?;
    Ok(MemoryTurnContext {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        mode: ThreadMode::Agent,
        input_text: input
            .user_text
            .as_ref()
            .map(|text| truncate_chars(text.text.as_str(), config.max_input_chars))
            .unwrap_or_default(),
        task_id: request
            .context
            .task_id
            .as_ref()
            .map(|id| id.as_str().to_owned()),
        agent_id: request
            .context
            .agent_id
            .as_ref()
            .map(|id| id.as_str().to_owned()),
    })
}

pub(super) fn turn_post_tool_events_summary(
    events: &[pioneer_hooks::TurnPostTurnToolEventSummary],
) -> String {
    if events.is_empty() {
        return String::new();
    }
    events
        .iter()
        .map(|event| {
            format!(
                "{} status={:?} outcome={:?} error={:?}",
                event.tool_name, event.status, event.outcome_status, event.error_class
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub(super) fn turn_post_domain_events_summary(
    events: &[pioneer_hooks::TurnPostTurnDomainEventSummary],
) -> String {
    if events.is_empty() {
        return String::new();
    }
    events
        .iter()
        .map(|event| {
            format!(
                "domain={:?} code={} item={} message={}",
                event.domain,
                event.code.as_deref().unwrap_or("none"),
                event.item_id.as_deref().unwrap_or("none"),
                event
                    .message
                    .as_ref()
                    .map(|message| message.text.as_str())
                    .unwrap_or("")
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct MemoryPostTurnParsedFacts {
    pub(super) facts: Vec<MemoryPostTurnExtractedFact>,
    pub(super) raw_fact_count: usize,
    pub(super) validation_rejected_count: usize,
    pub(super) diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct MemoryPostTurnExtractedFact {
    pub(super) semantic: MemorySemanticFields,
    pub(super) ontology_proposal: Option<MemoryExtractorOntologyProposal>,
    pub(super) content: String,
    pub(super) value: Option<String>,
    pub(super) evidence: MemoryWriteEvidence,
    pub(super) confidence: Option<f32>,
    pub(super) importance: Option<f32>,
}

#[derive(Debug, Deserialize)]
pub(super) struct MemoryPostTurnExtractorJson {
    #[serde(default)]
    pub(super) facts: Vec<MemoryPostTurnExtractedFactJson>,
}

#[derive(Debug, Deserialize)]
pub(super) struct MemoryPostTurnExtractedFactJson {
    semantic: MemorySemanticFields,
    #[serde(default)]
    ontology: Option<Value>,
    #[serde(default)]
    fact_class: Option<Value>,
    #[serde(default)]
    lifetime_class: Option<Value>,
    #[serde(default)]
    evidence_class: Option<Value>,
    #[serde(default)]
    proposed_ownership_class: Option<Value>,
    content: String,
    #[serde(default)]
    value: Option<String>,
    evidence: MemoryWriteEvidence,
}

pub(super) fn parse_memory_post_turn_extractor_json(
    raw: &str,
    config: &MemoryPostTurnExtractorConfig,
) -> Result<MemoryPostTurnParsedFacts, String> {
    let parsed = serde_json::from_str::<MemoryPostTurnExtractorJson>(raw.trim())
        .map_err(|error| error.to_string())?;
    let raw_fact_count = parsed.facts.len();
    let mut validation_rejected_count = 0;
    let mut diagnostics = Vec::new();
    let mut facts = Vec::new();
    for (index, fact) in parsed
        .facts
        .into_iter()
        .take(config.max_facts_per_turn)
        .enumerate()
    {
        match validate_memory_post_turn_fact(fact, config) {
            Ok(fact) => {
                if fact.ontology_proposal.is_none() {
                    diagnostics.push(format!(
                        "memory.post_turn_extractor.ontology_proposal_missing: index={index}"
                    ));
                }
                facts.push(fact);
            }
            Err(error) => {
                validation_rejected_count += 1;
                diagnostics.push(format!(
                    "memory.post_turn_extractor.fact_rejected: index={index} reason={error}"
                ));
            }
        }
    }
    if raw_fact_count > config.max_facts_per_turn {
        diagnostics.push(format!(
            "memory.post_turn_extractor.fact_limit: raw={} kept={}",
            raw_fact_count, config.max_facts_per_turn
        ));
    }
    Ok(MemoryPostTurnParsedFacts {
        facts,
        raw_fact_count,
        validation_rejected_count,
        diagnostics,
    })
}

pub(super) fn validate_memory_post_turn_fact(
    fact: MemoryPostTurnExtractedFactJson,
    config: &MemoryPostTurnExtractorConfig,
) -> Result<MemoryPostTurnExtractedFact, &'static str> {
    if !matches!(
        fact.semantic.intent,
        MemoryIntent::ExplicitStore | MemoryIntent::ImplicitCandidate
    ) {
        return Err("unsupported_intent");
    }
    if fact.semantic.explicitness == MemoryExplicitness::None {
        return Err("missing_explicitness");
    }
    if matches!(
        fact.semantic.sensitivity,
        MemorySensitivityHint::Secret | MemorySensitivityHint::Regulated
    ) {
        return Err("secret_or_regulated");
    }
    if fact.semantic.subject == MemorySubject::CurrentAgent
        && fact
            .evidence
            .source_ref
            .as_deref()
            .map(is_assistant_post_turn_source_ref)
            .unwrap_or(false)
    {
        return Err("assistant_self_description");
    }
    if fact.semantic.subject == MemorySubject::CurrentUser
        && fact
            .evidence
            .source_ref
            .as_deref()
            .map(is_assistant_post_turn_source_ref)
            .unwrap_or(false)
    {
        return Err("assistant_inference_about_user");
    }
    let ontology_proposal = parse_memory_post_turn_ontology_proposal(&fact)?;
    validate_ontology_proposal_sufficient(ontology_proposal.as_ref(), &fact)?;
    if matches!(
        fact.semantic.durability,
        MemoryDurability::Transient | MemoryDurability::SessionOnly
    ) && !ontology_proposal_allows_non_durable_route(ontology_proposal.as_ref())
    {
        return Err("transient_or_session_only");
    }
    let semantic = normalized_post_turn_fact_semantic(fact.semantic);
    let Some(content) = bounded_nonempty_text(fact.content.as_str(), config.max_fact_content_chars)
    else {
        return Err("empty_content");
    };
    let quote = fact
        .evidence
        .quote_or_span
        .as_deref()
        .unwrap_or("")
        .trim()
        .to_owned();
    if quote.is_empty() {
        return Err("missing_evidence_quote");
    }
    let mut evidence = fact.evidence;
    evidence.quote_or_span = bounded_nonempty_text(quote.as_str(), config.max_evidence_chars);
    evidence.extractor_reason = evidence
        .extractor_reason
        .as_deref()
        .and_then(|reason| bounded_nonempty_text(reason, 240));
    let confidence = Some(computed_post_turn_fact_confidence(&semantic));
    let importance = Some(computed_post_turn_fact_importance(&semantic));
    Ok(MemoryPostTurnExtractedFact {
        semantic,
        ontology_proposal,
        content,
        value: fact
            .value
            .as_deref()
            .and_then(|value| bounded_nonempty_text(value, config.max_fact_content_chars)),
        evidence,
        confidence,
        importance,
    })
}

fn validate_ontology_proposal_sufficient(
    proposal: Option<&MemoryExtractorOntologyProposal>,
    fact: &MemoryPostTurnExtractedFactJson,
) -> Result<(), &'static str> {
    let Some(proposal) = proposal else {
        return Ok(());
    };
    if fact
        .evidence
        .source_ref
        .as_deref()
        .map(|source_ref| source_ref.trim().is_empty())
        .unwrap_or(true)
    {
        return Err("missing_evidence_source_ref");
    }
    if proposal.evidence_class == MemoryEvidenceClass::MissingOrWeak {
        return Err("weak_evidence_class");
    }
    if matches!(
        proposal.proposed_ownership_class,
        MemoryOwnershipClass::AuditOnly | MemoryOwnershipClass::Reject
    ) {
        return Err("unclear_or_rejected_ownership");
    }
    Ok(())
}

fn ontology_proposal_allows_non_durable_route(
    proposal: Option<&MemoryExtractorOntologyProposal>,
) -> bool {
    proposal
        .map(|proposal| {
            matches!(
                proposal.proposed_ownership_class,
                MemoryOwnershipClass::ThreadEpisodicContext
                    | MemoryOwnershipClass::TaskRuntimeState
                    | MemoryOwnershipClass::DomainRuntimeState
            )
        })
        .unwrap_or(false)
}

fn parse_memory_post_turn_ontology_proposal(
    fact: &MemoryPostTurnExtractedFactJson,
) -> Result<Option<MemoryExtractorOntologyProposal>, &'static str> {
    let nested = match fact.ontology.as_ref() {
        Some(Value::Object(map)) => Some(map),
        Some(_) => return Err("invalid_ontology_proposal"),
        None => None,
    };
    let has_flat = fact.fact_class.is_some()
        || fact.lifetime_class.is_some()
        || fact.evidence_class.is_some()
        || fact.proposed_ownership_class.is_some();
    if nested.is_none() && !has_flat {
        return Ok(None);
    }

    let fact_class = proposal_value(nested, &fact.fact_class, "fact_class")
        .ok_or("partial_ontology_proposal")?;
    let lifetime_class = proposal_value(nested, &fact.lifetime_class, "lifetime_class")
        .ok_or("partial_ontology_proposal")?;
    let evidence_class = proposal_value(nested, &fact.evidence_class, "evidence_class")
        .ok_or("partial_ontology_proposal")?;
    let proposed_ownership_class = proposal_value(
        nested,
        &fact.proposed_ownership_class,
        "proposed_ownership_class",
    )
    .ok_or("partial_ontology_proposal")?;

    let proposal = MemoryExtractorOntologyProposal {
        fact_class: parse_proposal_enum(fact_class)?,
        lifetime_class: parse_proposal_enum(lifetime_class)?,
        evidence_class: parse_proposal_enum(evidence_class)?,
        proposed_ownership_class: parse_proposal_enum(proposed_ownership_class)?,
    };
    if proposal_has_unknown_class(&proposal) {
        return Err("unknown_ontology_proposal");
    }

    Ok(Some(proposal))
}

fn proposal_value<'a>(
    nested: Option<&'a Map<String, Value>>,
    flat: &'a Option<Value>,
    key: &str,
) -> Option<&'a Value> {
    nested.and_then(|map| map.get(key)).or(flat.as_ref())
}

fn parse_proposal_enum<T>(value: &Value) -> Result<T, &'static str>
where
    T: DeserializeOwned,
{
    serde_json::from_value::<T>(value.clone()).map_err(|_| "invalid_ontology_proposal_enum")
}

fn normalized_post_turn_fact_semantic(mut semantic: MemorySemanticFields) -> MemorySemanticFields {
    if should_force_personal_sensitivity(&semantic) {
        semantic.sensitivity = MemorySensitivityHint::Personal;
    }
    semantic
}

fn should_force_personal_sensitivity(semantic: &MemorySemanticFields) -> bool {
    semantic.subject == MemorySubject::CurrentUser
        && semantic.category == MemoryCategory::Identity
        && matches!(
            semantic.attribute,
            MemoryAttribute::Name | MemoryAttribute::Birthday
        )
}

fn computed_post_turn_fact_confidence(semantic: &MemorySemanticFields) -> f32 {
    let certainty_score: f32 = match semantic.certainty {
        MemoryExtractorCertainty::High => 0.82,
        MemoryExtractorCertainty::Medium => 0.62,
        MemoryExtractorCertainty::Low => 0.35,
    };
    let explicitness_score: f32 = match semantic.explicitness {
        MemoryExplicitness::Explicit => 0.08,
        MemoryExplicitness::Implicit => 0.02,
        MemoryExplicitness::Unclear => -0.10,
        MemoryExplicitness::None => -0.25,
    };
    let intent_score: f32 = match semantic.intent {
        MemoryIntent::ExplicitStore => 0.05,
        MemoryIntent::ImplicitCandidate => 0.0,
        MemoryIntent::ExplicitForget | MemoryIntent::ExplicitNoMemory | MemoryIntent::None => -0.25,
    };
    let durability_score: f32 = match semantic.durability {
        MemoryDurability::LongLived | MemoryDurability::ProjectLifetime => 0.03,
        MemoryDurability::Unknown => -0.08,
        MemoryDurability::SessionOnly | MemoryDurability::Transient => -0.25,
    };
    let scope_score: f32 = match semantic.scope_hint {
        MemoryScopeHint::Unknown => -0.08,
        MemoryScopeHint::UserGlobal
        | MemoryScopeHint::UserWorkspace
        | MemoryScopeHint::AgentGlobal
        | MemoryScopeHint::AgentWorkspace
        | MemoryScopeHint::ProjectWorkspace => 0.02,
    };
    let sensitivity_score: f32 = match semantic.sensitivity {
        MemorySensitivityHint::None | MemorySensitivityHint::Low => 0.02,
        MemorySensitivityHint::Personal => -0.03,
        MemorySensitivityHint::Unknown => -0.05,
        MemorySensitivityHint::Secret | MemorySensitivityHint::Regulated => -0.25,
    };

    (certainty_score
        + explicitness_score
        + intent_score
        + durability_score
        + scope_score
        + sensitivity_score)
        .clamp(0.0_f32, 1.0_f32)
}

fn computed_post_turn_fact_importance(semantic: &MemorySemanticFields) -> f32 {
    let category_score: f32 = match semantic.category {
        MemoryCategory::Identity | MemoryCategory::Biography | MemoryCategory::Relationship => 0.72,
        MemoryCategory::Preference
        | MemoryCategory::RecurringInstruction
        | MemoryCategory::ProjectPolicy
        | MemoryCategory::ProjectDecision
        | MemoryCategory::Constraint
        | MemoryCategory::CommunicationStyle => 0.68,
        MemoryCategory::Procedure => 0.62,
        MemoryCategory::ProjectFact | MemoryCategory::Custom => 0.56,
        MemoryCategory::Todo => 0.48,
    };
    let attribute_score: f32 = match semantic.attribute {
        MemoryAttribute::Name | MemoryAttribute::Birthday | MemoryAttribute::PreferredLanguage => {
            0.10
        }
        MemoryAttribute::CommunicationStyle
        | MemoryAttribute::MigrationPolicy
        | MemoryAttribute::ReviewStyle
        | MemoryAttribute::PhaseNaming => 0.06,
        MemoryAttribute::Custom => 0.0,
    };
    let durability_score: f32 = match semantic.durability {
        MemoryDurability::LongLived | MemoryDurability::ProjectLifetime => 0.06,
        MemoryDurability::Unknown => -0.08,
        MemoryDurability::SessionOnly | MemoryDurability::Transient => -0.25,
    };
    let certainty_score: f32 = match semantic.certainty {
        MemoryExtractorCertainty::High => 0.04,
        MemoryExtractorCertainty::Medium => 0.0,
        MemoryExtractorCertainty::Low => -0.18,
    };

    (category_score + attribute_score + durability_score + certainty_score).clamp(0.0_f32, 1.0_f32)
}

fn is_assistant_post_turn_source_ref(source_ref: &str) -> bool {
    source_ref.trim().starts_with("turn.post_turn:assistant")
}

pub(super) fn memory_semantic_write_params_from_extracted_fact(
    index: usize,
    fact: MemoryPostTurnExtractedFact,
    context: &MemoryTurnContext,
    policy: &MemoryTurnPolicy,
    config: &MemoryPostTurnExtractorConfig,
    source_context_kind: MemorySourceContextKind,
    model: Option<&str>,
    model_provider: Option<&str>,
) -> Option<MemorySemanticWriteParams> {
    if fact.semantic.intent == MemoryIntent::ImplicitCandidate && !config.proactive_writes_enabled {
        return None;
    }
    let scope = memory_scope_from_semantic_hint(context, fact.semantic.scope_hint)?;
    let mut evidence = fact.evidence;
    evidence.source_thread_id = evidence
        .source_thread_id
        .filter(|value| !value.trim().is_empty())
        .or_else(|| Some(context.thread_id.clone()));
    evidence.source_turn_id = evidence
        .source_turn_id
        .filter(|value| !value.trim().is_empty())
        .or_else(|| Some(context.turn_id.clone()));
    evidence.source_ref = evidence
        .source_ref
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            Some(format!(
                "turn.post_turn:{}:{}:fact:{index}",
                context.thread_id, context.turn_id
            ))
        });

    let provenance = MemoryProvenance {
        source_thread_id: Some(context.thread_id.clone()),
        source_turn_id: Some(context.turn_id.clone()),
        source_item_id: None,
        created_by: Some(MemoryActor {
            kind: MemoryActorKind::Extractor,
            id: Some(MEMORY_POST_TURN_EXTRACTOR_HOOK_ID.to_owned()),
        }),
    };
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "hook_id".to_owned(),
        serde_json::json!(MEMORY_POST_TURN_EXTRACTOR_HOOK_ID),
    );
    metadata.insert(
        "source_phase".to_owned(),
        serde_json::json!(HookPhase::TurnPostTurn.as_str()),
    );
    metadata.insert(
        "extractor_version".to_owned(),
        serde_json::json!(MEMORY_POST_TURN_EXTRACTOR_VERSION),
    );
    metadata.insert("fact_index".to_owned(), serde_json::json!(index));
    metadata.insert(
        "policy_source".to_owned(),
        serde_json::json!(policy.source.as_str()),
    );
    metadata.insert(
        "policy_reason_code".to_owned(),
        serde_json::json!(policy.reason_code.as_str()),
    );
    metadata.insert(
        "proactive_writes_enabled".to_owned(),
        serde_json::json!(config.proactive_writes_enabled),
    );
    if let Some(proposal) = fact.ontology_proposal {
        insert_extractor_ontology_proposal_metadata(&mut metadata, proposal);
    }
    if let Some(model) = bounded_nonempty_text(model.unwrap_or_default(), 160) {
        metadata.insert("model".to_owned(), serde_json::json!(model));
    }
    if let Some(model_provider) = bounded_nonempty_text(model_provider.unwrap_or_default(), 80) {
        metadata.insert(
            "model_provider".to_owned(),
            serde_json::json!(model_provider),
        );
    }

    Some(MemorySemanticWriteParams {
        scope,
        semantic: fact.semantic,
        content: fact.content,
        value: fact.value,
        evidence: Some(evidence),
        provenance: Some(provenance),
        source_context_kind: Some(source_context_kind),
        disposition: Some(MemorySemanticWriteDisposition::RouteToCandidatePolicy),
        client_provided_key: None,
        confidence: fact.confidence,
        importance: fact.importance,
        metadata,
    })
}

pub(super) fn memory_scope_from_semantic_hint(
    context: &MemoryTurnContext,
    hint: MemoryScopeHint,
) -> Option<MemoryScope> {
    match hint {
        MemoryScopeHint::UserGlobal | MemoryScopeHint::UserWorkspace => Some(MemoryScope {
            kind: MemoryScopeKind::User,
            key: MEMORY_DEFAULT_USER_SCOPE_KEY.to_owned(),
        }),
        MemoryScopeHint::ProjectWorkspace | MemoryScopeHint::Unknown => Some(MemoryScope {
            kind: MemoryScopeKind::Workspace,
            key: context.workspace_id.clone(),
        }),
        MemoryScopeHint::AgentWorkspace => context.agent_id.as_ref().map(|agent_id| MemoryScope {
            kind: MemoryScopeKind::Agent,
            key: workspace_agent_memory_scope_key(context.workspace_id.as_str(), agent_id),
        }),
        MemoryScopeHint::AgentGlobal => context.agent_id.as_ref().map(|agent_id| MemoryScope {
            kind: MemoryScopeKind::Agent,
            key: format!("global:agent:{}", agent_id.trim()),
        }),
    }
}

pub(super) fn workspace_agent_memory_scope_key(workspace_id: &str, agent_id: &str) -> String {
    format!(
        "workspace:{}:agent:{}",
        workspace_id.trim(),
        agent_id.trim()
    )
}

pub(super) fn post_turn_policy_allows_any_extraction(policy: &MemoryTurnPolicy) -> bool {
    policy.post_turn_extraction == MemoryExtractionPolicy::Allow || policy.explicit_remember
}

pub(super) fn post_turn_policy_allows_fact(
    policy: &MemoryTurnPolicy,
    semantic: &MemorySemanticFields,
) -> bool {
    if semantic.intent == MemoryIntent::ExplicitStore
        && semantic.explicitness == MemoryExplicitness::Explicit
        && policy.explicit_remember
    {
        return true;
    }
    policy.post_turn_extraction == MemoryExtractionPolicy::Allow
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct MemoryPostTurnExtractorStats {
    pub(super) raw_fact_count: usize,
    pub(super) validation_rejected_count: usize,
    pub(super) policy_rejected_count: usize,
    pub(super) write_attempt_count: usize,
    pub(super) write_success_count: usize,
    pub(super) write_failure_count: usize,
    pub(super) auto_approved_count: usize,
    pub(super) rejected_or_suppressed_count: usize,
    pub(super) duplicate_or_merged_count: usize,
}

impl MemoryPostTurnExtractorStats {
    pub(super) fn observe_write_response(&mut self, response: &MemorySemanticWriteResponse) {
        if response.record.is_some() {
            self.auto_approved_count += 1;
        }
        if response.evidence_merged || response.relation == MemoryWriteRelation::Duplicate {
            self.duplicate_or_merged_count += 1;
        }
        if matches!(
            response.relation,
            MemoryWriteRelation::SuppressedByRejection | MemoryWriteRelation::Contradiction
        ) {
            self.rejected_or_suppressed_count += 1;
        }
        if let Some(candidate) = &response.candidate
            && matches!(
                candidate.status,
                MemoryCandidateStatus::Rejected
                    | MemoryCandidateStatus::AutoRejected
                    | MemoryCandidateStatus::ReviewDisabledRejected
                    | MemoryCandidateStatus::MergedDuplicate
                    | MemoryCandidateStatus::Superseded
            )
        {
            self.rejected_or_suppressed_count += 1;
        }
    }
}

pub(super) fn memory_post_turn_stats_diagnostic(
    stats: &MemoryPostTurnExtractorStats,
) -> HookDiagnostic {
    let mut diagnostic = memory_safe_info_diagnostic(
        "memory.post_turn_extractor.completed",
        format!(
            "memory post-turn extractor completed: raw_facts={} validation_rejected={} policy_rejected={} write_attempts={} write_successes={} write_failures={} auto_approved={} rejected_or_suppressed={} duplicate_or_merged={}",
            stats.raw_fact_count,
            stats.validation_rejected_count,
            stats.policy_rejected_count,
            stats.write_attempt_count,
            stats.write_success_count,
            stats.write_failure_count,
            stats.auto_approved_count,
            stats.rejected_or_suppressed_count,
            stats.duplicate_or_merged_count
        ),
    );
    for (key, value) in [
        ("raw_fact_count", stats.raw_fact_count),
        ("validation_rejected_count", stats.validation_rejected_count),
        ("policy_rejected_count", stats.policy_rejected_count),
        ("write_attempt_count", stats.write_attempt_count),
        ("write_success_count", stats.write_success_count),
        ("write_failure_count", stats.write_failure_count),
        ("auto_approved_count", stats.auto_approved_count),
        (
            "rejected_or_suppressed_count",
            stats.rejected_or_suppressed_count,
        ),
        ("duplicate_or_merged_count", stats.duplicate_or_merged_count),
    ] {
        insert_usize_metadata(&mut diagnostic.metadata, key, value);
    }
    diagnostic
}

pub(super) fn memory_post_turn_stats_metadata(
    stats: &MemoryPostTurnExtractorStats,
) -> HookMetadata {
    let mut metadata = HookMetadata::default();
    for (key, value) in [
        ("post_turn_extractor.raw_fact_count", stats.raw_fact_count),
        (
            "post_turn_extractor.validation_rejected_count",
            stats.validation_rejected_count,
        ),
        (
            "post_turn_extractor.policy_rejected_count",
            stats.policy_rejected_count,
        ),
        (
            "post_turn_extractor.write_attempt_count",
            stats.write_attempt_count,
        ),
        (
            "post_turn_extractor.write_success_count",
            stats.write_success_count,
        ),
        (
            "post_turn_extractor.write_failure_count",
            stats.write_failure_count,
        ),
        (
            "post_turn_extractor.auto_approved_count",
            stats.auto_approved_count,
        ),
        (
            "post_turn_extractor.rejected_or_suppressed_count",
            stats.rejected_or_suppressed_count,
        ),
        (
            "post_turn_extractor.duplicate_or_merged_count",
            stats.duplicate_or_merged_count,
        ),
    ] {
        insert_usize_metadata(&mut metadata, key, value);
    }
    metadata
}

pub(super) fn memory_post_turn_skip_diagnostic(
    reason: MemoryPostTurnEligibilitySkipReason,
    status: TurnPostTurnStatus,
    policy: Option<&MemoryTurnPolicy>,
) -> HookDiagnostic {
    let mut diagnostic = memory_safe_info_diagnostic(reason.diagnostic_code(), reason.message());
    diagnostic.metadata.insert(
        hook_metadata_key("skip_reason"),
        HookValue::Text(reason.as_str().to_owned()),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("turn_status"),
        HookValue::Text(turn_post_turn_status_label(status).to_owned()),
    );
    if let Some(policy) = policy {
        diagnostic.metadata.insert(
            hook_metadata_key("policy_source"),
            HookValue::Text(policy.source.as_str().to_owned()),
        );
        diagnostic.metadata.insert(
            hook_metadata_key("policy_reason_code"),
            HookValue::Text(policy.reason_code.as_str().to_owned()),
        );
    }
    diagnostic
}

pub(super) fn memory_post_turn_provider_skip_diagnostic(
    reason: &'static str,
    message: impl Into<String>,
) -> HookDiagnostic {
    let mut diagnostic = memory_safe_info_diagnostic("memory.post_turn_extractor.skipped", message);
    diagnostic.metadata.insert(
        hook_metadata_key("skip_reason"),
        HookValue::Text(reason.to_owned()),
    );
    diagnostic
}
