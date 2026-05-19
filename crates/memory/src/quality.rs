use crate::write::{EVIDENCE_METADATA_KEY, SEMANTIC_METADATA_KEY};
use pioneer_protocol::{
    MemoryAttribute, MemoryCandidate, MemoryCandidateScore, MemoryCandidateStatus, MemoryCategory,
    MemoryDurability, MemoryEvidenceActorRole, MemoryEvidenceClass, MemoryFactClass,
    MemoryLifetimeClass, MemoryOwnershipClass, MemoryRecord, MemoryScope, MemoryScopeHint,
    MemoryScopeKind, MemorySemanticFields, MemorySemanticWriteParams, MemorySensitivityHint,
    MemorySourceContextKind, MemoryStatus, MemorySubject, MemoryWriteEvidence,
};
use serde_json::Value;
use std::collections::BTreeMap;

const POST_TURN_USER_SOURCE_REF: &str = "turn.post_turn:user";
const POST_TURN_ASSISTANT_SOURCE_REF: &str = "turn.post_turn:assistant";
const POST_TURN_TOOL_SOURCE_REF: &str = "turn.post_turn:tool";
const TOOL_SOURCE_REF_PREFIX: &str = "tool:";
const TASK_SOURCE_REF_PREFIX: &str = "task:";
const SYSTEM_SOURCE_REF_PREFIX: &str = "system:";
const GENERATED_SUMMARY_SOURCE_REF_PREFIX: &str = "generated_summary:";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryOntologyClassification {
    pub fact_class: MemoryFactClass,
    pub lifetime_class: MemoryLifetimeClass,
    pub proposed_ownership_class: MemoryOwnershipClass,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MemorySourceContextInput<'a> {
    pub workspace_id: Option<&'a str>,
    pub thread_id: Option<&'a str>,
    pub turn_id: Option<&'a str>,
    pub task_id: Option<&'a str>,
    pub has_user_text: bool,
    pub has_assistant_text: bool,
    pub has_tool_events: bool,
    pub has_domain_events: bool,
    pub evidence: Option<&'a MemoryWriteEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySourceContextClassification {
    pub context_kind: MemorySourceContextKind,
    pub actor_role: MemoryEvidenceActorRole,
    pub evidence_class: MemoryEvidenceClass,
    pub source_is_user_assertion: bool,
    pub source_is_system_owned_state: bool,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub task_id: Option<String>,
    pub workspace_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryQualityAuditItemKind {
    Record,
    Candidate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryQualityAuditStatus {
    Memory(MemoryStatus),
    Candidate(MemoryCandidateStatus),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryQualityAuditRecord {
    pub item_kind: MemoryQualityAuditItemKind,
    pub id: String,
    pub status: MemoryQualityAuditStatus,
    pub scope: MemoryScope,
    pub category: MemoryCategory,
    pub key: Option<String>,
    pub active_key: Option<String>,
    pub content_preview: Option<String>,
    pub confidence: f32,
    pub importance: f32,
    pub source_context_kind: MemorySourceContextKind,
    pub fact_class: MemoryFactClass,
    pub lifetime_class: MemoryLifetimeClass,
    pub ownership_class: MemoryOwnershipClass,
    pub evidence_class: MemoryEvidenceClass,
    pub candidate_score: Option<f32>,
    pub source_thread_id: Option<String>,
    pub source_turn_id: Option<String>,
    pub source_item_id: Option<String>,
}

pub fn classify_semantic_memory_fact(
    semantic: &MemorySemanticFields,
    scope: Option<&MemoryScope>,
) -> MemoryOntologyClassification {
    let fact_class = classify_fact_class(semantic, scope);
    let lifetime_class = classify_lifetime_class(semantic, fact_class);
    let proposed_ownership_class = classify_ownership_class(semantic, scope, fact_class);

    MemoryOntologyClassification {
        fact_class,
        lifetime_class,
        proposed_ownership_class,
    }
}

pub fn classify_memory_source_context(
    input: MemorySourceContextInput<'_>,
) -> MemorySourceContextClassification {
    let evidence_source_ref = input
        .evidence
        .and_then(|evidence| evidence.source_ref.as_deref())
        .map(str::trim)
        .filter(|source_ref| !source_ref.is_empty());
    let source_thread_id = input
        .evidence
        .and_then(|evidence| evidence.source_thread_id.as_deref())
        .or(input.thread_id);
    let source_turn_id = input
        .evidence
        .and_then(|evidence| evidence.source_turn_id.as_deref())
        .or(input.turn_id);
    let source_item_id = input
        .evidence
        .and_then(|evidence| evidence.source_item_id.as_deref());

    let mut classification = if input.task_id.is_some()
        || source_ref_has_prefix(evidence_source_ref, TASK_SOURCE_REF_PREFIX)
    {
        source_context(
            MemorySourceContextKind::TaskRuntime,
            MemoryEvidenceActorRole::Task,
            MemoryEvidenceClass::TaskRuntimeObservation,
            false,
            true,
        )
    } else if source_ref_is(evidence_source_ref, POST_TURN_USER_SOURCE_REF) {
        source_context(
            MemorySourceContextKind::DirectUserConversation,
            MemoryEvidenceActorRole::User,
            MemoryEvidenceClass::DirectUserAssertion,
            true,
            false,
        )
    } else if source_ref_is(evidence_source_ref, POST_TURN_ASSISTANT_SOURCE_REF) {
        source_context(
            MemorySourceContextKind::AssistantResponse,
            MemoryEvidenceActorRole::Assistant,
            MemoryEvidenceClass::AssistantInference,
            false,
            false,
        )
    } else if source_ref_is(evidence_source_ref, POST_TURN_TOOL_SOURCE_REF)
        || source_ref_has_prefix(evidence_source_ref, TOOL_SOURCE_REF_PREFIX)
        || input.has_tool_events
    {
        source_context(
            MemorySourceContextKind::ToolResult,
            MemoryEvidenceActorRole::Tool,
            MemoryEvidenceClass::ToolObservation,
            false,
            true,
        )
    } else if source_ref_has_prefix(evidence_source_ref, GENERATED_SUMMARY_SOURCE_REF_PREFIX) {
        source_context(
            MemorySourceContextKind::GeneratedSummary,
            MemoryEvidenceActorRole::System,
            MemoryEvidenceClass::GeneratedSummary,
            false,
            true,
        )
    } else if source_ref_has_prefix(evidence_source_ref, SYSTEM_SOURCE_REF_PREFIX)
        || input.has_domain_events
    {
        source_context(
            MemorySourceContextKind::SystemRuntime,
            MemoryEvidenceActorRole::System,
            MemoryEvidenceClass::SystemObservation,
            false,
            true,
        )
    } else if input.has_user_text {
        source_context(
            MemorySourceContextKind::DirectUserConversation,
            MemoryEvidenceActorRole::User,
            MemoryEvidenceClass::MissingOrWeak,
            false,
            false,
        )
    } else if input.has_assistant_text {
        source_context(
            MemorySourceContextKind::AssistantResponse,
            MemoryEvidenceActorRole::Assistant,
            MemoryEvidenceClass::AssistantInference,
            false,
            false,
        )
    } else {
        source_context(
            MemorySourceContextKind::Unknown,
            MemoryEvidenceActorRole::Unknown,
            MemoryEvidenceClass::MissingOrWeak,
            false,
            false,
        )
    };

    classification.thread_id = source_thread_id.map(str::to_owned);
    classification.turn_id = source_turn_id.map(str::to_owned);
    classification.item_id = source_item_id.map(str::to_owned);
    classification.task_id = input.task_id.map(str::to_owned);
    classification.workspace_id = input.workspace_id.map(str::to_owned);
    classification
}

pub fn resolve_semantic_write_source_context(
    params: &MemorySemanticWriteParams,
) -> MemorySourceContextClassification {
    let provenance = params.provenance.as_ref();
    let evidence = params.evidence.as_ref();
    let source_thread_id = evidence
        .and_then(|evidence| evidence.source_thread_id.as_deref())
        .or_else(|| provenance.and_then(|provenance| provenance.source_thread_id.as_deref()));
    let source_turn_id = evidence
        .and_then(|evidence| evidence.source_turn_id.as_deref())
        .or_else(|| provenance.and_then(|provenance| provenance.source_turn_id.as_deref()));
    let source_item_id = evidence
        .and_then(|evidence| evidence.source_item_id.as_deref())
        .or_else(|| provenance.and_then(|provenance| provenance.source_item_id.as_deref()));

    if let Some(source_context_kind) = params.source_context_kind {
        let mut classification = classify_source_context_kind(source_context_kind);
        classification.thread_id = source_thread_id.map(str::to_owned);
        classification.turn_id = source_turn_id.map(str::to_owned);
        classification.item_id = source_item_id.map(str::to_owned);
        return classification;
    }

    classify_memory_source_context(MemorySourceContextInput {
        thread_id: provenance.and_then(|provenance| provenance.source_thread_id.as_deref()),
        turn_id: provenance.and_then(|provenance| provenance.source_turn_id.as_deref()),
        evidence,
        ..MemorySourceContextInput::default()
    })
}

pub fn audit_memory_record_quality(record: &MemoryRecord) -> MemoryQualityAuditRecord {
    let semantic = semantic_fields_from_metadata(&record.metadata);
    let ontology = semantic
        .as_ref()
        .map(|semantic| classify_semantic_memory_fact(semantic, Some(&record.scope)));
    let evidence = latest_evidence_from_metadata(&record.metadata);
    let source_context = record
        .source_context_kind
        .map(|source_context_kind| {
            persisted_source_context(
                source_context_kind,
                workspace_id_from_scope(&record.scope),
                record.provenance.source_thread_id.as_deref(),
                record.provenance.source_turn_id.as_deref(),
                record.provenance.source_item_id.as_deref(),
            )
        })
        .unwrap_or_else(|| {
            classify_memory_source_context(MemorySourceContextInput {
                workspace_id: workspace_id_from_scope(&record.scope),
                thread_id: record.provenance.source_thread_id.as_deref(),
                turn_id: record.provenance.source_turn_id.as_deref(),
                task_id: None,
                evidence: evidence.as_ref(),
                ..MemorySourceContextInput::default()
            })
        });

    MemoryQualityAuditRecord {
        item_kind: MemoryQualityAuditItemKind::Record,
        id: record.id.clone(),
        status: MemoryQualityAuditStatus::Memory(record.status),
        scope: record.scope.clone(),
        category: record.category,
        key: record.key.clone(),
        active_key: active_key_from_metadata(&record.metadata).or_else(|| record.key.clone()),
        content_preview: preview_text(record.content.as_str()),
        confidence: record.confidence,
        importance: record.importance,
        source_context_kind: source_context.context_kind,
        fact_class: ontology
            .map(|classification| classification.fact_class)
            .unwrap_or(MemoryFactClass::Unknown),
        lifetime_class: ontology
            .map(|classification| classification.lifetime_class)
            .unwrap_or(MemoryLifetimeClass::Unknown),
        ownership_class: ontology
            .map(|classification| classification.proposed_ownership_class)
            .unwrap_or(MemoryOwnershipClass::AuditOnly),
        evidence_class: source_context.evidence_class,
        candidate_score: candidate_score_from_metadata(&record.metadata),
        source_thread_id: source_context.thread_id,
        source_turn_id: source_context.turn_id,
        source_item_id: source_context.item_id,
    }
}

pub fn audit_memory_candidate_quality(candidate: &MemoryCandidate) -> MemoryQualityAuditRecord {
    let semantic = semantic_fields_from_metadata(&candidate.metadata);
    let ontology = semantic
        .as_ref()
        .map(|semantic| classify_semantic_memory_fact(semantic, Some(&candidate.scope)));
    let evidence = latest_evidence_from_metadata(&candidate.metadata);
    let source_context = candidate
        .source_context_kind
        .map(|source_context_kind| {
            persisted_source_context(
                source_context_kind,
                workspace_id_from_scope(&candidate.scope),
                candidate.provenance.source_thread_id.as_deref(),
                candidate.provenance.source_turn_id.as_deref(),
                candidate.provenance.source_item_id.as_deref(),
            )
        })
        .unwrap_or_else(|| {
            classify_memory_source_context(MemorySourceContextInput {
                workspace_id: workspace_id_from_scope(&candidate.scope),
                thread_id: candidate.provenance.source_thread_id.as_deref(),
                turn_id: candidate.provenance.source_turn_id.as_deref(),
                task_id: None,
                evidence: evidence.as_ref(),
                ..MemorySourceContextInput::default()
            })
        });

    MemoryQualityAuditRecord {
        item_kind: MemoryQualityAuditItemKind::Candidate,
        id: candidate.id.clone(),
        status: MemoryQualityAuditStatus::Candidate(candidate.status),
        scope: candidate.scope.clone(),
        category: candidate.category,
        key: candidate.key.clone(),
        active_key: active_key_from_metadata(&candidate.metadata).or_else(|| candidate.key.clone()),
        content_preview: preview_text(candidate.candidate_text.as_str()),
        confidence: candidate.confidence,
        importance: 0.0,
        source_context_kind: source_context.context_kind,
        fact_class: ontology
            .map(|classification| classification.fact_class)
            .unwrap_or(MemoryFactClass::Unknown),
        lifetime_class: ontology
            .map(|classification| classification.lifetime_class)
            .unwrap_or(MemoryLifetimeClass::Unknown),
        ownership_class: ontology
            .map(|classification| classification.proposed_ownership_class)
            .unwrap_or(MemoryOwnershipClass::AuditOnly),
        evidence_class: source_context.evidence_class,
        candidate_score: candidate_score_from_metadata(&candidate.metadata),
        source_thread_id: source_context.thread_id,
        source_turn_id: source_context.turn_id,
        source_item_id: source_context.item_id,
    }
}

fn source_context(
    context_kind: MemorySourceContextKind,
    actor_role: MemoryEvidenceActorRole,
    evidence_class: MemoryEvidenceClass,
    source_is_user_assertion: bool,
    source_is_system_owned_state: bool,
) -> MemorySourceContextClassification {
    MemorySourceContextClassification {
        context_kind,
        actor_role,
        evidence_class,
        source_is_user_assertion,
        source_is_system_owned_state,
        thread_id: None,
        turn_id: None,
        item_id: None,
        task_id: None,
        workspace_id: None,
    }
}

fn classify_source_context_kind(
    source_context_kind: MemorySourceContextKind,
) -> MemorySourceContextClassification {
    match source_context_kind {
        MemorySourceContextKind::DirectUserConversation => source_context(
            MemorySourceContextKind::DirectUserConversation,
            MemoryEvidenceActorRole::User,
            MemoryEvidenceClass::DirectUserAssertion,
            true,
            false,
        ),
        MemorySourceContextKind::AssistantResponse => source_context(
            MemorySourceContextKind::AssistantResponse,
            MemoryEvidenceActorRole::Assistant,
            MemoryEvidenceClass::AssistantInference,
            false,
            false,
        ),
        MemorySourceContextKind::ToolResult => source_context(
            MemorySourceContextKind::ToolResult,
            MemoryEvidenceActorRole::Tool,
            MemoryEvidenceClass::ToolObservation,
            false,
            true,
        ),
        MemorySourceContextKind::TaskRuntime => source_context(
            MemorySourceContextKind::TaskRuntime,
            MemoryEvidenceActorRole::Task,
            MemoryEvidenceClass::TaskRuntimeObservation,
            false,
            true,
        ),
        MemorySourceContextKind::SystemRuntime => source_context(
            MemorySourceContextKind::SystemRuntime,
            MemoryEvidenceActorRole::System,
            MemoryEvidenceClass::SystemObservation,
            false,
            true,
        ),
        MemorySourceContextKind::DeveloperInstruction => source_context(
            MemorySourceContextKind::DeveloperInstruction,
            MemoryEvidenceActorRole::Developer,
            MemoryEvidenceClass::SystemObservation,
            false,
            true,
        ),
        MemorySourceContextKind::ConnectorContent => source_context(
            MemorySourceContextKind::ConnectorContent,
            MemoryEvidenceActorRole::Connector,
            MemoryEvidenceClass::SystemObservation,
            false,
            true,
        ),
        MemorySourceContextKind::ImportedDocument => source_context(
            MemorySourceContextKind::ImportedDocument,
            MemoryEvidenceActorRole::Connector,
            MemoryEvidenceClass::SystemObservation,
            false,
            true,
        ),
        MemorySourceContextKind::GeneratedSummary => source_context(
            MemorySourceContextKind::GeneratedSummary,
            MemoryEvidenceActorRole::System,
            MemoryEvidenceClass::GeneratedSummary,
            false,
            true,
        ),
        MemorySourceContextKind::Unknown => source_context(
            MemorySourceContextKind::Unknown,
            MemoryEvidenceActorRole::Unknown,
            MemoryEvidenceClass::MissingOrWeak,
            false,
            false,
        ),
    }
}

fn persisted_source_context(
    source_context_kind: MemorySourceContextKind,
    workspace_id: Option<&str>,
    thread_id: Option<&str>,
    turn_id: Option<&str>,
    item_id: Option<&str>,
) -> MemorySourceContextClassification {
    let mut classification = classify_source_context_kind(source_context_kind);
    classification.workspace_id = workspace_id.map(str::to_owned);
    classification.thread_id = thread_id.map(str::to_owned);
    classification.turn_id = turn_id.map(str::to_owned);
    classification.item_id = item_id.map(str::to_owned);
    classification
}

fn source_ref_is(source_ref: Option<&str>, expected: &str) -> bool {
    source_ref
        .map(|source_ref| source_ref == expected || source_ref.starts_with(&format!("{expected}:")))
        .unwrap_or(false)
}

fn source_ref_has_prefix(source_ref: Option<&str>, prefix: &str) -> bool {
    source_ref
        .map(|source_ref| source_ref.starts_with(prefix))
        .unwrap_or(false)
}

fn semantic_fields_from_metadata(
    metadata: &BTreeMap<String, Value>,
) -> Option<MemorySemanticFields> {
    let semantic = metadata.get(SEMANTIC_METADATA_KEY)?;
    let fields = semantic.get("fields").unwrap_or(semantic);
    serde_json::from_value(fields.clone()).ok()
}

fn active_key_from_metadata(metadata: &BTreeMap<String, Value>) -> Option<String> {
    let semantic = metadata.get(SEMANTIC_METADATA_KEY)?;
    semantic
        .get("canonical_key")
        .and_then(Value::as_str)
        .or_else(|| {
            semantic
                .get("canonical")
                .and_then(|canonical| canonical.get("key"))
                .and_then(Value::as_str)
        })
        .map(str::to_owned)
}

fn candidate_score_from_metadata(metadata: &BTreeMap<String, Value>) -> Option<f32> {
    metadata
        .get("candidate_score")
        .and_then(candidate_score_value)
        .or_else(|| {
            metadata
                .get("candidate_policy")
                .and_then(|policy| policy.get("score"))
                .and_then(candidate_score_value)
        })
}

fn candidate_score_value(value: &Value) -> Option<f32> {
    serde_json::from_value::<MemoryCandidateScore>(value.clone())
        .ok()
        .map(|score| score.total_score)
        .or_else(|| {
            value
                .get("total_score")
                .and_then(Value::as_f64)
                .map(|value| value as f32)
        })
}

fn latest_evidence_from_metadata(
    metadata: &BTreeMap<String, Value>,
) -> Option<MemoryWriteEvidence> {
    let sources = metadata
        .get(EVIDENCE_METADATA_KEY)?
        .get("sources")?
        .as_array()?;
    sources
        .iter()
        .rev()
        .find_map(|source| serde_json::from_value::<MemoryWriteEvidence>(source.clone()).ok())
}

fn workspace_id_from_scope(scope: &MemoryScope) -> Option<&str> {
    (scope.kind == MemoryScopeKind::Workspace).then_some(scope.key.as_str())
}

fn preview_text(value: &str) -> Option<String> {
    const MAX_PREVIEW_CHARS: usize = 160;
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    Some(value.chars().take(MAX_PREVIEW_CHARS).collect())
}

fn classify_fact_class(
    semantic: &MemorySemanticFields,
    scope: Option<&MemoryScope>,
) -> MemoryFactClass {
    match semantic.sensitivity {
        MemorySensitivityHint::Secret => return MemoryFactClass::SecretOrCredential,
        MemorySensitivityHint::Regulated => return MemoryFactClass::RegulatedSensitiveFact,
        MemorySensitivityHint::None
        | MemorySensitivityHint::Low
        | MemorySensitivityHint::Personal
        | MemorySensitivityHint::Unknown => {}
    }

    if semantic.subject == MemorySubject::CurrentAgent {
        return MemoryFactClass::AssistantSelfDescription;
    }

    match semantic.category {
        MemoryCategory::Identity if semantic.subject == MemorySubject::CurrentUser => {
            MemoryFactClass::UserIdentity
        }
        MemoryCategory::Identity => MemoryFactClass::DomainOwnedState,
        MemoryCategory::Biography if semantic.subject == MemorySubject::CurrentUser => {
            MemoryFactClass::UserBiography
        }
        MemoryCategory::Biography => MemoryFactClass::DomainOwnedState,
        MemoryCategory::Relationship => MemoryFactClass::UserRelationship,
        MemoryCategory::Preference if semantic.attribute == MemoryAttribute::CommunicationStyle => {
            MemoryFactClass::CommunicationPreference
        }
        MemoryCategory::Preference => MemoryFactClass::StableUserPreference,
        MemoryCategory::CommunicationStyle => MemoryFactClass::CommunicationPreference,
        MemoryCategory::RecurringInstruction => MemoryFactClass::RecurringUserInstruction,
        MemoryCategory::ProjectPolicy => MemoryFactClass::ProjectPolicy,
        MemoryCategory::ProjectDecision => MemoryFactClass::ProjectDecision,
        MemoryCategory::Procedure => MemoryFactClass::ProjectProcedure,
        MemoryCategory::Constraint => MemoryFactClass::ProjectConstraint,
        MemoryCategory::Todo
            if matches!(
                semantic.subject,
                MemorySubject::Workspace | MemorySubject::Project | MemorySubject::Artifact
            ) =>
        {
            MemoryFactClass::TaskLifecycleState
        }
        MemoryCategory::Todo => MemoryFactClass::ThreadLocalState,
        MemoryCategory::ProjectFact if semantic.subject == MemorySubject::Artifact => {
            MemoryFactClass::ToolResultFact
        }
        MemoryCategory::ProjectFact if semantic.durability == MemoryDurability::Transient => {
            MemoryFactClass::OperationalObservation
        }
        MemoryCategory::ProjectFact if has_workspace_or_project_context(semantic, scope) => {
            MemoryFactClass::DomainOwnedState
        }
        MemoryCategory::ProjectFact | MemoryCategory::Custom => MemoryFactClass::Unknown,
    }
}

fn classify_lifetime_class(
    semantic: &MemorySemanticFields,
    fact_class: MemoryFactClass,
) -> MemoryLifetimeClass {
    match fact_class {
        MemoryFactClass::TaskLifecycleState => return MemoryLifetimeClass::TaskLifetime,
        MemoryFactClass::ThreadLocalState => return MemoryLifetimeClass::ThreadLifetime,
        MemoryFactClass::OperationalObservation | MemoryFactClass::ToolResultFact => {
            return MemoryLifetimeClass::NaturallyExpiring;
        }
        _ => {}
    }

    match semantic.durability {
        MemoryDurability::LongLived => MemoryLifetimeClass::LongLived,
        MemoryDurability::ProjectLifetime => MemoryLifetimeClass::ProjectLifetime,
        MemoryDurability::SessionOnly => MemoryLifetimeClass::SessionOnly,
        MemoryDurability::Transient => MemoryLifetimeClass::Instantaneous,
        MemoryDurability::Unknown => MemoryLifetimeClass::Unknown,
    }
}

fn classify_ownership_class(
    semantic: &MemorySemanticFields,
    scope: Option<&MemoryScope>,
    fact_class: MemoryFactClass,
) -> MemoryOwnershipClass {
    if matches!(
        fact_class,
        MemoryFactClass::SecretOrCredential | MemoryFactClass::RegulatedSensitiveFact
    ) {
        return MemoryOwnershipClass::Reject;
    }
    if fact_class == MemoryFactClass::ThreadLocalState {
        return MemoryOwnershipClass::ThreadEpisodicContext;
    }
    if fact_class == MemoryFactClass::TaskLifecycleState {
        return MemoryOwnershipClass::TaskRuntimeState;
    }
    if matches!(
        fact_class,
        MemoryFactClass::OperationalObservation
            | MemoryFactClass::ToolResultFact
            | MemoryFactClass::GeneratedSummaryFact
    ) {
        return MemoryOwnershipClass::DomainRuntimeState;
    }

    match semantic.scope_hint {
        MemoryScopeHint::UserGlobal | MemoryScopeHint::UserWorkspace
            if is_user_fact(fact_class) =>
        {
            MemoryOwnershipClass::DurableUserMemory
        }
        MemoryScopeHint::AgentGlobal | MemoryScopeHint::AgentWorkspace => {
            MemoryOwnershipClass::DurableAgentMemory
        }
        MemoryScopeHint::ProjectWorkspace
            if is_workspace_fact(fact_class)
                || has_workspace_or_project_context(semantic, scope) =>
        {
            MemoryOwnershipClass::DurableWorkspaceMemory
        }
        MemoryScopeHint::UserGlobal
        | MemoryScopeHint::UserWorkspace
        | MemoryScopeHint::ProjectWorkspace
        | MemoryScopeHint::Unknown => MemoryOwnershipClass::AuditOnly,
    }
}

fn is_user_fact(fact_class: MemoryFactClass) -> bool {
    matches!(
        fact_class,
        MemoryFactClass::UserIdentity
            | MemoryFactClass::UserBiography
            | MemoryFactClass::UserRelationship
            | MemoryFactClass::StableUserPreference
            | MemoryFactClass::CommunicationPreference
            | MemoryFactClass::RecurringUserInstruction
    )
}

fn is_workspace_fact(fact_class: MemoryFactClass) -> bool {
    matches!(
        fact_class,
        MemoryFactClass::ProjectPolicy
            | MemoryFactClass::ProjectDecision
            | MemoryFactClass::ProjectProcedure
            | MemoryFactClass::ProjectConstraint
            | MemoryFactClass::DomainOwnedState
    )
}

fn has_workspace_or_project_context(
    semantic: &MemorySemanticFields,
    scope: Option<&MemoryScope>,
) -> bool {
    semantic.scope_hint == MemoryScopeHint::ProjectWorkspace
        || matches!(
            semantic.subject,
            MemorySubject::Workspace | MemorySubject::Project | MemorySubject::Artifact
        )
        || scope
            .map(|scope| scope.kind == MemoryScopeKind::Workspace)
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        MemoryExplicitness, MemoryExtractorCertainty, MemoryIntent, MemoryProvenance,
        MemoryScopeKind, MemorySemanticWriteDisposition,
    };

    fn semantic(
        category: MemoryCategory,
        subject: MemorySubject,
        attribute: MemoryAttribute,
        scope_hint: MemoryScopeHint,
        durability: MemoryDurability,
    ) -> MemorySemanticFields {
        MemorySemanticFields {
            intent: MemoryIntent::ImplicitCandidate,
            explicitness: MemoryExplicitness::Implicit,
            category,
            subject,
            attribute,
            subject_key: None,
            custom_subject: None,
            custom_attribute: None,
            scope_hint,
            durability,
            sensitivity: MemorySensitivityHint::Low,
            certainty: MemoryExtractorCertainty::High,
        }
    }

    #[test]
    fn maps_user_identity_to_durable_user_memory() {
        let semantic = semantic(
            MemoryCategory::Identity,
            MemorySubject::CurrentUser,
            MemoryAttribute::Name,
            MemoryScopeHint::UserGlobal,
            MemoryDurability::LongLived,
        );

        let classification = classify_semantic_memory_fact(&semantic, None);

        assert_eq!(classification.fact_class, MemoryFactClass::UserIdentity);
        assert_eq!(
            classification.lifetime_class,
            MemoryLifetimeClass::LongLived
        );
        assert_eq!(
            classification.proposed_ownership_class,
            MemoryOwnershipClass::DurableUserMemory
        );
    }

    #[test]
    fn maps_communication_preference_structurally() {
        let semantic = semantic(
            MemoryCategory::Preference,
            MemorySubject::CurrentUser,
            MemoryAttribute::CommunicationStyle,
            MemoryScopeHint::UserWorkspace,
            MemoryDurability::LongLived,
        );

        let classification = classify_semantic_memory_fact(&semantic, None);

        assert_eq!(
            classification.fact_class,
            MemoryFactClass::CommunicationPreference
        );
        assert_eq!(
            classification.proposed_ownership_class,
            MemoryOwnershipClass::DurableUserMemory
        );
    }

    #[test]
    fn maps_project_decision_to_workspace_memory() {
        let semantic = semantic(
            MemoryCategory::ProjectDecision,
            MemorySubject::Project,
            MemoryAttribute::PhaseNaming,
            MemoryScopeHint::ProjectWorkspace,
            MemoryDurability::ProjectLifetime,
        );

        let classification = classify_semantic_memory_fact(&semantic, None);

        assert_eq!(classification.fact_class, MemoryFactClass::ProjectDecision);
        assert_eq!(
            classification.lifetime_class,
            MemoryLifetimeClass::ProjectLifetime
        );
        assert_eq!(
            classification.proposed_ownership_class,
            MemoryOwnershipClass::DurableWorkspaceMemory
        );
    }

    #[test]
    fn keeps_unknown_scope_hint_audit_only() {
        let semantic = semantic(
            MemoryCategory::Identity,
            MemorySubject::CurrentUser,
            MemoryAttribute::Name,
            MemoryScopeHint::Unknown,
            MemoryDurability::LongLived,
        );

        let classification = classify_semantic_memory_fact(&semantic, None);

        assert_eq!(classification.fact_class, MemoryFactClass::UserIdentity);
        assert_eq!(
            classification.proposed_ownership_class,
            MemoryOwnershipClass::AuditOnly
        );
    }

    #[test]
    fn maps_todo_to_thread_local_thread_lifetime_context() {
        let semantic = semantic(
            MemoryCategory::Todo,
            MemorySubject::CurrentUser,
            MemoryAttribute::Custom,
            MemoryScopeHint::UserWorkspace,
            MemoryDurability::Transient,
        );

        let classification = classify_semantic_memory_fact(&semantic, None);

        assert_eq!(classification.fact_class, MemoryFactClass::ThreadLocalState);
        assert_eq!(
            classification.lifetime_class,
            MemoryLifetimeClass::ThreadLifetime
        );
        assert_eq!(
            classification.proposed_ownership_class,
            MemoryOwnershipClass::ThreadEpisodicContext
        );
    }

    #[test]
    fn maps_project_todo_to_task_lifecycle_state() {
        let semantic = semantic(
            MemoryCategory::Todo,
            MemorySubject::Project,
            MemoryAttribute::Custom,
            MemoryScopeHint::ProjectWorkspace,
            MemoryDurability::SessionOnly,
        );

        let classification = classify_semantic_memory_fact(&semantic, None);

        assert_eq!(
            classification.fact_class,
            MemoryFactClass::TaskLifecycleState
        );
        assert_eq!(
            classification.lifetime_class,
            MemoryLifetimeClass::TaskLifetime
        );
        assert_eq!(
            classification.proposed_ownership_class,
            MemoryOwnershipClass::TaskRuntimeState
        );
    }

    #[test]
    fn maps_operational_observation_to_domain_runtime_state() {
        let semantic = semantic(
            MemoryCategory::ProjectFact,
            MemorySubject::Workspace,
            MemoryAttribute::Custom,
            MemoryScopeHint::ProjectWorkspace,
            MemoryDurability::Transient,
        );

        let classification = classify_semantic_memory_fact(&semantic, None);

        assert_eq!(
            classification.fact_class,
            MemoryFactClass::OperationalObservation
        );
        assert_eq!(
            classification.lifetime_class,
            MemoryLifetimeClass::NaturallyExpiring
        );
        assert_eq!(
            classification.proposed_ownership_class,
            MemoryOwnershipClass::DomainRuntimeState
        );
    }

    #[test]
    fn maps_artifact_project_fact_to_tool_result_fact() {
        let semantic = semantic(
            MemoryCategory::ProjectFact,
            MemorySubject::Artifact,
            MemoryAttribute::Custom,
            MemoryScopeHint::ProjectWorkspace,
            MemoryDurability::Transient,
        );

        let classification = classify_semantic_memory_fact(&semantic, None);

        assert_eq!(classification.fact_class, MemoryFactClass::ToolResultFact);
        assert_eq!(
            classification.lifetime_class,
            MemoryLifetimeClass::NaturallyExpiring
        );
        assert_eq!(
            classification.proposed_ownership_class,
            MemoryOwnershipClass::DomainRuntimeState
        );
    }

    #[test]
    fn maps_secret_semantic_to_reject_ownership_class() {
        let mut semantic = semantic(
            MemoryCategory::Identity,
            MemorySubject::CurrentUser,
            MemoryAttribute::Custom,
            MemoryScopeHint::UserGlobal,
            MemoryDurability::LongLived,
        );
        semantic.sensitivity = MemorySensitivityHint::Secret;

        let classification = classify_semantic_memory_fact(&semantic, None);

        assert_eq!(
            classification.fact_class,
            MemoryFactClass::SecretOrCredential
        );
        assert_eq!(
            classification.proposed_ownership_class,
            MemoryOwnershipClass::Reject
        );
    }

    #[test]
    fn maps_project_fact_only_when_context_is_structurally_clear() {
        let semantic_without_context = semantic(
            MemoryCategory::ProjectFact,
            MemorySubject::CurrentUser,
            MemoryAttribute::Custom,
            MemoryScopeHint::Unknown,
            MemoryDurability::ProjectLifetime,
        );
        let workspace_scope = MemoryScope {
            kind: MemoryScopeKind::Workspace,
            key: "workspace-1".to_owned(),
        };
        let semantic_with_context = semantic(
            MemoryCategory::ProjectFact,
            MemorySubject::Project,
            MemoryAttribute::Custom,
            MemoryScopeHint::ProjectWorkspace,
            MemoryDurability::ProjectLifetime,
        );

        assert_eq!(
            classify_semantic_memory_fact(&semantic_without_context, None).fact_class,
            MemoryFactClass::Unknown
        );
        assert_eq!(
            classify_semantic_memory_fact(&semantic_with_context, Some(&workspace_scope))
                .fact_class,
            MemoryFactClass::DomainOwnedState
        );
    }

    #[test]
    fn source_context_classifies_direct_user_evidence() {
        let evidence = evidence_with_source_ref("turn.post_turn:user");

        let classification = classify_memory_source_context(MemorySourceContextInput {
            workspace_id: Some("workspace-1"),
            thread_id: Some("thread-1"),
            turn_id: Some("turn-1"),
            has_user_text: true,
            evidence: Some(&evidence),
            ..MemorySourceContextInput::default()
        });

        assert_eq!(
            classification.context_kind,
            MemorySourceContextKind::DirectUserConversation
        );
        assert_eq!(classification.actor_role, MemoryEvidenceActorRole::User);
        assert_eq!(
            classification.evidence_class,
            MemoryEvidenceClass::DirectUserAssertion
        );
        assert!(classification.source_is_user_assertion);
        assert!(!classification.source_is_system_owned_state);
        assert_eq!(classification.thread_id.as_deref(), Some("thread-1"));
        assert_eq!(classification.turn_id.as_deref(), Some("turn-1"));
        assert_eq!(classification.workspace_id.as_deref(), Some("workspace-1"));
    }

    #[test]
    fn source_context_classifies_assistant_evidence() {
        let evidence = evidence_with_source_ref("turn.post_turn:assistant");

        let classification = classify_memory_source_context(MemorySourceContextInput {
            has_assistant_text: true,
            evidence: Some(&evidence),
            ..MemorySourceContextInput::default()
        });

        assert_eq!(
            classification.context_kind,
            MemorySourceContextKind::AssistantResponse
        );
        assert_eq!(
            classification.actor_role,
            MemoryEvidenceActorRole::Assistant
        );
        assert_eq!(
            classification.evidence_class,
            MemoryEvidenceClass::AssistantInference
        );
        assert!(!classification.source_is_user_assertion);
    }

    #[test]
    fn source_context_classifies_tool_evidence_without_text_matching() {
        let evidence = evidence_with_source_ref("tool:read_file");

        let classification = classify_memory_source_context(MemorySourceContextInput {
            has_tool_events: true,
            evidence: Some(&evidence),
            ..MemorySourceContextInput::default()
        });

        assert_eq!(
            classification.context_kind,
            MemorySourceContextKind::ToolResult
        );
        assert_eq!(classification.actor_role, MemoryEvidenceActorRole::Tool);
        assert_eq!(
            classification.evidence_class,
            MemoryEvidenceClass::ToolObservation
        );
        assert!(classification.source_is_system_owned_state);
    }

    #[test]
    fn source_context_classifies_task_runtime_as_system_owned() {
        let evidence = evidence_with_source_ref("turn.post_turn:user");

        let classification = classify_memory_source_context(MemorySourceContextInput {
            task_id: Some("task-1"),
            has_user_text: true,
            evidence: Some(&evidence),
            ..MemorySourceContextInput::default()
        });

        assert_eq!(
            classification.context_kind,
            MemorySourceContextKind::TaskRuntime
        );
        assert_eq!(classification.actor_role, MemoryEvidenceActorRole::Task);
        assert_eq!(
            classification.evidence_class,
            MemoryEvidenceClass::TaskRuntimeObservation
        );
        assert!(!classification.source_is_user_assertion);
        assert!(classification.source_is_system_owned_state);
        assert_eq!(classification.task_id.as_deref(), Some("task-1"));
    }

    #[test]
    fn semantic_write_source_context_prefers_explicit_typed_context() {
        let mut params = semantic_write_params_for_source_context();
        params.source_context_kind = Some(MemorySourceContextKind::ToolResult);
        params.provenance = Some(MemoryProvenance {
            source_thread_id: Some("thread-provenance".to_owned()),
            source_turn_id: Some("turn-provenance".to_owned()),
            source_item_id: Some("item-provenance".to_owned()),
            created_by: None,
        });
        params.evidence = Some(MemoryWriteEvidence {
            source_thread_id: Some("thread-evidence".to_owned()),
            source_turn_id: Some("turn-evidence".to_owned()),
            source_item_id: Some("item-evidence".to_owned()),
            source_ref: Some("turn.post_turn:user".to_owned()),
            quote_or_span: Some("structured evidence".to_owned()),
            extractor_reason: None,
        });

        let classification = resolve_semantic_write_source_context(&params);

        assert_eq!(
            classification.context_kind,
            MemorySourceContextKind::ToolResult
        );
        assert_eq!(classification.actor_role, MemoryEvidenceActorRole::Tool);
        assert!(classification.source_is_system_owned_state);
        assert_eq!(classification.thread_id.as_deref(), Some("thread-evidence"));
        assert_eq!(classification.turn_id.as_deref(), Some("turn-evidence"));
        assert_eq!(classification.item_id.as_deref(), Some("item-evidence"));
    }

    #[test]
    fn semantic_write_source_context_falls_back_to_structured_evidence() {
        let mut params = semantic_write_params_for_source_context();
        params.source_context_kind = None;
        params.provenance = Some(MemoryProvenance {
            source_thread_id: Some("thread-provenance".to_owned()),
            source_turn_id: Some("turn-provenance".to_owned()),
            source_item_id: Some("item-provenance".to_owned()),
            created_by: None,
        });
        params.evidence = Some(MemoryWriteEvidence {
            source_thread_id: None,
            source_turn_id: None,
            source_item_id: None,
            source_ref: Some("tool:read_file".to_owned()),
            quote_or_span: None,
            extractor_reason: None,
        });

        let classification = resolve_semantic_write_source_context(&params);

        assert_eq!(
            classification.context_kind,
            MemorySourceContextKind::ToolResult
        );
        assert_eq!(classification.actor_role, MemoryEvidenceActorRole::Tool);
        assert_eq!(
            classification.thread_id.as_deref(),
            Some("thread-provenance")
        );
        assert_eq!(classification.turn_id.as_deref(), Some("turn-provenance"));
    }

    #[test]
    fn source_context_does_not_mark_user_text_as_assertion_without_evidence_ref() {
        let classification = classify_memory_source_context(MemorySourceContextInput {
            has_user_text: true,
            ..MemorySourceContextInput::default()
        });

        assert_eq!(
            classification.context_kind,
            MemorySourceContextKind::DirectUserConversation
        );
        assert_eq!(classification.actor_role, MemoryEvidenceActorRole::User);
        assert_eq!(
            classification.evidence_class,
            MemoryEvidenceClass::MissingOrWeak
        );
        assert!(!classification.source_is_user_assertion);
    }

    #[test]
    fn source_context_classifies_domain_events_as_system_runtime() {
        let classification = classify_memory_source_context(MemorySourceContextInput {
            has_domain_events: true,
            ..MemorySourceContextInput::default()
        });

        assert_eq!(
            classification.context_kind,
            MemorySourceContextKind::SystemRuntime
        );
        assert_eq!(classification.actor_role, MemoryEvidenceActorRole::System);
        assert_eq!(
            classification.evidence_class,
            MemoryEvidenceClass::SystemObservation
        );
        assert!(classification.source_is_system_owned_state);
    }

    fn evidence_with_source_ref(source_ref: &str) -> MemoryWriteEvidence {
        MemoryWriteEvidence {
            source_thread_id: None,
            source_turn_id: None,
            source_item_id: None,
            source_ref: Some(source_ref.to_owned()),
            quote_or_span: Some("structured evidence".to_owned()),
            extractor_reason: None,
        }
    }

    fn semantic_write_params_for_source_context() -> MemorySemanticWriteParams {
        MemorySemanticWriteParams {
            scope: MemoryScope {
                kind: MemoryScopeKind::User,
                key: "default".to_owned(),
            },
            semantic: semantic(
                MemoryCategory::Identity,
                MemorySubject::CurrentUser,
                MemoryAttribute::Name,
                MemoryScopeHint::UserGlobal,
                MemoryDurability::LongLived,
            ),
            content: "Имя пользователя: Александр".to_owned(),
            value: Some("Александр".to_owned()),
            evidence: None,
            provenance: None,
            source_context_kind: None,
            disposition: Some(MemorySemanticWriteDisposition::AcceptActive),
            client_provided_key: None,
            confidence: Some(0.95),
            importance: Some(0.7),
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn quality_audit_record_decodes_typed_dimensions_without_mutating_record() {
        let record = memory_record_with_metadata(semantic_metadata_value(
            MemoryCategory::Identity,
            MemorySubject::CurrentUser,
            MemoryAttribute::Name,
            MemoryScopeHint::UserGlobal,
            MemoryDurability::LongLived,
        ));
        let before = record.clone();

        let audit = audit_memory_record_quality(&record);

        assert_eq!(record, before);
        assert_eq!(audit.item_kind, MemoryQualityAuditItemKind::Record);
        assert_eq!(
            audit.status,
            MemoryQualityAuditStatus::Memory(MemoryStatus::Active)
        );
        assert_eq!(audit.fact_class, MemoryFactClass::UserIdentity);
        assert_eq!(audit.lifetime_class, MemoryLifetimeClass::LongLived);
        assert_eq!(
            audit.ownership_class,
            MemoryOwnershipClass::DurableUserMemory
        );
        assert_eq!(
            audit.source_context_kind,
            MemorySourceContextKind::DirectUserConversation
        );
        assert_eq!(
            audit.evidence_class,
            MemoryEvidenceClass::DirectUserAssertion
        );
        assert_eq!(
            audit.active_key.as_deref(),
            Some("user/global:identity:self:name")
        );
    }

    #[test]
    fn quality_audit_record_prefers_persisted_source_context() {
        let mut record = memory_record_with_metadata(semantic_metadata_value(
            MemoryCategory::Identity,
            MemorySubject::CurrentUser,
            MemoryAttribute::Name,
            MemoryScopeHint::UserGlobal,
            MemoryDurability::LongLived,
        ));
        record.source_context_kind = Some(MemorySourceContextKind::ToolResult);

        let audit = audit_memory_record_quality(&record);

        assert_eq!(
            audit.source_context_kind,
            MemorySourceContextKind::ToolResult
        );
        assert_eq!(audit.evidence_class, MemoryEvidenceClass::ToolObservation);
        assert_eq!(audit.source_thread_id.as_deref(), Some("thread-1"));
        assert_eq!(audit.source_turn_id.as_deref(), Some("turn-1"));
    }

    #[test]
    fn quality_audit_candidate_includes_candidate_policy_score() {
        let candidate = memory_candidate_with_metadata(candidate_metadata_with_score(0.91));
        let before = candidate.clone();

        let audit = audit_memory_candidate_quality(&candidate);

        assert_eq!(candidate, before);
        assert_eq!(audit.item_kind, MemoryQualityAuditItemKind::Candidate);
        assert_eq!(
            audit.status,
            MemoryQualityAuditStatus::Candidate(MemoryCandidateStatus::PendingSilent)
        );
        assert_eq!(audit.candidate_score, Some(0.91));
        assert_eq!(audit.fact_class, MemoryFactClass::ProjectDecision);
        assert_eq!(
            audit.ownership_class,
            MemoryOwnershipClass::DurableWorkspaceMemory
        );
    }

    #[test]
    fn quality_audit_candidate_prefers_persisted_source_context() {
        let mut candidate = memory_candidate_with_metadata(candidate_metadata_with_score(0.91));
        candidate.source_context_kind = Some(MemorySourceContextKind::TaskRuntime);

        let audit = audit_memory_candidate_quality(&candidate);

        assert_eq!(
            audit.source_context_kind,
            MemorySourceContextKind::TaskRuntime
        );
        assert_eq!(
            audit.evidence_class,
            MemoryEvidenceClass::TaskRuntimeObservation
        );
        assert_eq!(audit.source_thread_id.as_deref(), Some("thread-2"));
        assert_eq!(audit.source_turn_id.as_deref(), Some("turn-2"));
    }

    #[test]
    fn quality_audit_missing_legacy_metadata_falls_back_without_panic() {
        let record = memory_record_with_metadata(BTreeMap::new());

        let audit = audit_memory_record_quality(&record);

        assert_eq!(audit.fact_class, MemoryFactClass::Unknown);
        assert_eq!(audit.lifetime_class, MemoryLifetimeClass::Unknown);
        assert_eq!(audit.ownership_class, MemoryOwnershipClass::AuditOnly);
        assert_eq!(audit.candidate_score, None);
    }

    fn memory_record_with_metadata(metadata: BTreeMap<String, Value>) -> MemoryRecord {
        MemoryRecord {
            id: "memory-1".to_owned(),
            scope: MemoryScope {
                kind: MemoryScopeKind::User,
                key: "user-1".to_owned(),
            },
            namespace: Some("default".to_owned()),
            category: MemoryCategory::Identity,
            key: Some("user/global:identity:self:name".to_owned()),
            content: "Имя пользователя: Александр".to_owned(),
            status: MemoryStatus::Active,
            confidence: 0.95,
            importance: 0.82,
            sensitivity: pioneer_protocol::MemorySensitivity::Personal,
            provenance: pioneer_protocol::MemoryProvenance {
                source_thread_id: Some("thread-1".to_owned()),
                source_turn_id: Some("turn-1".to_owned()),
                source_item_id: Some("item-1".to_owned()),
                created_by: None,
            },
            source_context_kind: None,
            created_at: 1,
            updated_at: 1,
            expires_at: None,
            last_accessed_at: None,
            access_count: 0,
            superseded_by: None,
            deleted_at: None,
            delete_reason: None,
            metadata,
        }
    }

    fn memory_candidate_with_metadata(metadata: BTreeMap<String, Value>) -> MemoryCandidate {
        MemoryCandidate {
            id: "candidate-1".to_owned(),
            scope: MemoryScope {
                kind: MemoryScopeKind::Workspace,
                key: "workspace-1".to_owned(),
            },
            category: MemoryCategory::ProjectDecision,
            key: Some("workspace/project:project_decision:self:phase_naming".to_owned()),
            candidate_text: "Проект использует наименование Phase.".to_owned(),
            confidence: 0.74,
            reason: "candidate_policy".to_owned(),
            provenance: pioneer_protocol::MemoryProvenance {
                source_thread_id: Some("thread-2".to_owned()),
                source_turn_id: Some("turn-2".to_owned()),
                source_item_id: None,
                created_by: None,
            },
            source_context_kind: None,
            status: MemoryCandidateStatus::PendingSilent,
            created_at: 2,
            decided_at: None,
            decision_reason: None,
            metadata,
        }
    }

    fn semantic_metadata_value(
        category: MemoryCategory,
        subject: MemorySubject,
        attribute: MemoryAttribute,
        scope_hint: MemoryScopeHint,
        durability: MemoryDurability,
    ) -> BTreeMap<String, Value> {
        let fields = semantic(category, subject, attribute, scope_hint, durability);
        let mut metadata = BTreeMap::new();
        metadata.insert(
            SEMANTIC_METADATA_KEY.to_owned(),
            serde_json::json!({
                "canonical_key": "user/global:identity:self:name",
                "fields": fields,
            }),
        );
        metadata.insert(
            EVIDENCE_METADATA_KEY.to_owned(),
            serde_json::json!({
                "sources": [
                    {
                        "source_thread_id": "thread-1",
                        "source_turn_id": "turn-1",
                        "source_item_id": "item-1",
                        "source_ref": "turn.post_turn:user",
                        "quote_or_span": "structured evidence",
                        "extractor_reason": null
                    }
                ]
            }),
        );
        metadata
    }

    fn candidate_metadata_with_score(score: f32) -> BTreeMap<String, Value> {
        let mut metadata = semantic_metadata_value(
            MemoryCategory::ProjectDecision,
            MemorySubject::Project,
            MemoryAttribute::PhaseNaming,
            MemoryScopeHint::ProjectWorkspace,
            MemoryDurability::ProjectLifetime,
        );
        metadata.insert(
            SEMANTIC_METADATA_KEY.to_owned(),
            serde_json::json!({
                "canonical_key": "workspace/project:project_decision:self:phase_naming",
                "fields": semantic(
                    MemoryCategory::ProjectDecision,
                    MemorySubject::Project,
                    MemoryAttribute::PhaseNaming,
                    MemoryScopeHint::ProjectWorkspace,
                    MemoryDurability::ProjectLifetime,
                ),
            }),
        );
        metadata.insert(
            "candidate_score".to_owned(),
            serde_json::json!({
                "total_score": score,
                "bucket": "high",
                "explicitness_score": 0.2,
                "durability_score": 0.2,
                "scope_score": 0.15,
                "evidence_score": 0.1,
                "certainty_score": 0.15,
                "sensitivity_score": 0.1,
                "relation_score": 0.01,
                "reasons": []
            }),
        );
        metadata
    }
}
