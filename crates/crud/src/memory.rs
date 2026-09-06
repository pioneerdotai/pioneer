use anyhow::{Context, Result, bail};
use pioneer_protocol::{
    MemoryActorKind, MemoryCandidateDecision, MemoryCandidateStatus, MemoryCategory,
    MemoryEvidenceClass, MemoryFactClass, MemoryLifecycleActorKind, MemoryLifecycleReasonCode,
    MemoryLifetimeClass, MemoryOwnershipClass, MemoryQualityAction, MemoryQualityReasonCode,
    MemoryScope, MemoryScopeKind, MemorySensitivity, MemorySourceContextKind, MemoryStatus,
    MemoryWriteRelation,
};
use sha2::{Digest, Sha256};

use crate::convention::{
    MEMORY_NAMESPACE_DEFAULT, memory_actor_kind_from_db, memory_actor_kind_to_db,
    memory_candidate_status_from_db, memory_category_from_db, memory_evidence_class_from_db,
    memory_fact_class_from_db, memory_lifecycle_actor_kind_from_db,
    memory_lifecycle_actor_kind_to_db, memory_lifecycle_reason_code_from_db,
    memory_lifetime_class_from_db, memory_ownership_class_from_db, memory_quality_action_from_db,
    memory_scope_kind_from_db, memory_scope_kind_to_db, memory_sensitivity_from_db,
    memory_source_context_kind_from_db, memory_status_from_db, memory_write_relation_from_db,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryScopeResolution {
    pub scope: MemoryScope,
    pub scope_key_hash: String,
    pub workspace_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryWorkspaceGuard {
    pub workspace_id: String,
    pub allow_global_user: bool,
    pub allow_global_agent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryActorRecord {
    pub kind: MemoryActorKind,
    pub id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryLifecycleActorRecord {
    pub kind: MemoryLifecycleActorKind,
    pub id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentMemoryControlRecord {
    pub id: String,
    pub scope: MemoryScope,
    pub scope_key_hash: String,
    pub workspace_id: Option<String>,
    pub namespace: String,
    pub category: MemoryCategory,
    pub key: Option<String>,
    pub active_key: Option<String>,
    pub status: MemoryStatus,
    pub sensitivity: MemorySensitivity,
    pub confidence: f64,
    pub importance: f64,
    pub content_preview: Option<String>,
    pub capsule_id: Option<String>,
    pub capsule_ref: Option<String>,
    pub frame_id: Option<i64>,
    pub frame_uri: Option<String>,
    pub frame_version: i64,
    pub source_context_kind: Option<MemorySourceContextKind>,
    pub source_thread_id: Option<String>,
    pub source_turn_id: Option<String>,
    pub source_item_id: Option<String>,
    pub created_by: Option<MemoryActorRecord>,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
    pub last_accessed_at_unix: Option<i64>,
    pub access_count: i64,
    pub expires_at_unix: Option<i64>,
    pub superseded_by: Option<String>,
    pub deleted_at_unix: Option<i64>,
    pub deleted_by: Option<MemoryActorRecord>,
    pub delete_reason: Option<String>,
    pub policy_version: Option<String>,
    pub repair_status: String,
    pub metadata_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewAgentMemoryControlRecord {
    pub id: Option<String>,
    pub scope: MemoryScope,
    pub namespace: Option<String>,
    pub category: MemoryCategory,
    pub key: Option<String>,
    pub sensitivity: MemorySensitivity,
    pub confidence: f64,
    pub importance: f64,
    pub content_preview: Option<String>,
    pub capsule_id: Option<String>,
    pub capsule_ref: Option<String>,
    pub frame_id: Option<i64>,
    pub frame_uri: Option<String>,
    pub frame_version: i64,
    pub source_context_kind: Option<MemorySourceContextKind>,
    pub source_thread_id: Option<String>,
    pub source_turn_id: Option<String>,
    pub source_item_id: Option<String>,
    pub created_by: Option<MemoryActorRecord>,
    pub expires_at_unix: Option<i64>,
    pub policy_version: Option<String>,
    pub metadata_json: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentMemoryListFilter {
    pub scopes: Vec<MemoryScope>,
    pub workspace_guard: Option<MemoryWorkspaceGuard>,
    pub namespace: Option<String>,
    pub key: Option<String>,
    pub categories: Vec<MemoryCategory>,
    pub statuses: Vec<MemoryStatus>,
    pub include_expired: bool,
    pub include_deleted: bool,
    pub include_superseded: bool,
    pub allowed_source_thread_ids: Option<Vec<String>>,
    pub owned_scopes: Vec<MemoryScope>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAgentMemoryEvent {
    pub memory_id: Option<String>,
    pub candidate_id: Option<String>,
    pub workspace_id: Option<String>,
    pub event_kind: String,
    pub actor: Option<MemoryActorRecord>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub details_json: Option<String>,
    pub created_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemoryEventRecord {
    pub id: String,
    pub memory_id: Option<String>,
    pub candidate_id: Option<String>,
    pub workspace_id: Option<String>,
    pub event_kind: String,
    pub actor: Option<MemoryActorRecord>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub details_json: Option<String>,
    pub created_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAgentMemoryQuarantine {
    pub id: Option<String>,
    pub memory_id: String,
    pub workspace_id: Option<String>,
    pub reason_code: MemoryLifecycleReasonCode,
    pub actor: MemoryLifecycleActorRecord,
    pub details_json: Option<String>,
    pub created_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveAgentMemoryQuarantine {
    pub memory_id: String,
    pub reason_code: MemoryLifecycleReasonCode,
    pub actor: MemoryLifecycleActorRecord,
    pub resolved_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemoryQuarantineRecord {
    pub id: String,
    pub memory_id: String,
    pub workspace_id: Option<String>,
    pub reason_code: MemoryLifecycleReasonCode,
    pub actor: MemoryLifecycleActorRecord,
    pub created_at_unix: i64,
    pub resolved_at_unix: Option<i64>,
    pub resolved_reason_code: Option<MemoryLifecycleReasonCode>,
    pub resolved_actor: Option<MemoryLifecycleActorRecord>,
    pub details_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewAgentMemoryCandidate {
    pub id: Option<String>,
    pub scope: MemoryScope,
    pub namespace: Option<String>,
    pub category: MemoryCategory,
    pub key: Option<String>,
    pub status: Option<MemoryCandidateStatus>,
    pub candidate_text: String,
    pub confidence: f64,
    pub reason: String,
    pub source_context_kind: Option<MemorySourceContextKind>,
    pub source_thread_id: Option<String>,
    pub source_turn_id: Option<String>,
    pub source_item_id: Option<String>,
    pub created_by: Option<MemoryActorRecord>,
    pub dedupe_key: Option<String>,
    pub metadata_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemoryCandidateDecisionRecord {
    pub candidate_id: String,
    pub decision: MemoryCandidateDecision,
    pub decided_by: Option<MemoryActorRecord>,
    pub decision_reason: Option<String>,
    pub promoted_memory_id: Option<String>,
    pub decided_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemoryCandidateStatusUpdateRecord {
    pub candidate_id: String,
    pub status: MemoryCandidateStatus,
    pub decided_by: Option<MemoryActorRecord>,
    pub decision_reason: Option<String>,
    pub promoted_memory_id: Option<String>,
    pub metadata_json: Option<String>,
    pub decided_at_unix: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentMemoryCandidateListFilter {
    pub scopes: Vec<MemoryScope>,
    pub workspace_guard: Option<MemoryWorkspaceGuard>,
    pub categories: Vec<MemoryCategory>,
    pub statuses: Vec<MemoryCandidateStatus>,
    pub allowed_source_thread_ids: Option<Vec<String>>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentMemoryCandidateRecord {
    pub id: String,
    pub scope: MemoryScope,
    pub scope_key_hash: String,
    pub workspace_id: Option<String>,
    pub namespace: String,
    pub category: MemoryCategory,
    pub key: Option<String>,
    pub candidate_text: String,
    pub confidence: f64,
    pub reason: String,
    pub source_context_kind: Option<MemorySourceContextKind>,
    pub source_thread_id: Option<String>,
    pub source_turn_id: Option<String>,
    pub source_item_id: Option<String>,
    pub created_by: Option<MemoryActorRecord>,
    pub status: MemoryCandidateStatus,
    pub dedupe_key: Option<String>,
    pub created_at_unix: i64,
    pub decided_at_unix: Option<i64>,
    pub decided_by: Option<MemoryActorRecord>,
    pub decision_reason: Option<String>,
    pub promoted_memory_id: Option<String>,
    pub metadata_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemoryCapsuleRecord {
    pub id: Option<String>,
    pub scope: MemoryScope,
    pub scope_key_hash: Option<String>,
    pub workspace_id: Option<String>,
    pub scope_slot: Option<String>,
    pub capsule_ref: String,
    pub storage_uri: String,
    pub backend: String,
    pub format: String,
    pub encrypted: bool,
    pub status: String,
    pub repair_status: String,
    pub content_hash: Option<String>,
    pub active_record_count: i64,
    pub metadata_json: Option<String>,
    pub created_at_unix: Option<i64>,
    pub updated_at_unix: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAgentMemoryPolicyDecision {
    pub memory_id: Option<String>,
    pub candidate_id: Option<String>,
    pub workspace_id: Option<String>,
    pub action: String,
    pub decision: String,
    pub reason_code: Option<String>,
    pub reason: Option<String>,
    pub policy_version: String,
    pub actor: Option<MemoryActorRecord>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub details_json: Option<String>,
    pub created_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemoryPolicyDecisionRecord {
    pub id: String,
    pub memory_id: Option<String>,
    pub candidate_id: Option<String>,
    pub workspace_id: Option<String>,
    pub action: String,
    pub decision: String,
    pub reason_code: Option<String>,
    pub reason: Option<String>,
    pub policy_version: String,
    pub actor: Option<MemoryActorRecord>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub details_json: Option<String>,
    pub created_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAgentMemoryQualityDecision {
    pub workspace_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub task_id: Option<String>,
    pub memory_id: Option<String>,
    pub candidate_id: Option<String>,
    pub canonical_key: Option<String>,
    pub action: MemoryQualityAction,
    pub target_ownership: MemoryOwnershipClass,
    pub source_context_kind: MemorySourceContextKind,
    pub fact_class: MemoryFactClass,
    pub lifetime_class: MemoryLifetimeClass,
    pub ownership_class: MemoryOwnershipClass,
    pub evidence_class: MemoryEvidenceClass,
    pub relation: MemoryWriteRelation,
    pub reason_codes: Vec<MemoryQualityReasonCode>,
    pub input_snapshot_json: Option<String>,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemoryQualityDecisionRecord {
    pub id: String,
    pub workspace_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub task_id: Option<String>,
    pub memory_id: Option<String>,
    pub candidate_id: Option<String>,
    pub canonical_key: Option<String>,
    pub action: MemoryQualityAction,
    pub target_ownership: MemoryOwnershipClass,
    pub source_context_kind: MemorySourceContextKind,
    pub fact_class: MemoryFactClass,
    pub lifetime_class: MemoryLifetimeClass,
    pub ownership_class: MemoryOwnershipClass,
    pub evidence_class: MemoryEvidenceClass,
    pub relation: MemoryWriteRelation,
    pub reason_codes: Vec<MemoryQualityReasonCode>,
    pub input_snapshot_json: Option<String>,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAgentMemoryRepairJob {
    pub job_kind: String,
    pub workspace_id: Option<String>,
    pub scope_kind: Option<MemoryScopeKind>,
    pub scope_key_hash: Option<String>,
    pub memory_id: Option<String>,
    pub capsule_id: Option<String>,
    pub priority: i64,
    pub max_attempts: i32,
    pub scheduled_at_unix: i64,
    pub payload_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemoryRepairJobRecord {
    pub id: String,
    pub job_kind: String,
    pub status: String,
    pub workspace_id: Option<String>,
    pub scope_kind: Option<MemoryScopeKind>,
    pub scope_key_hash: Option<String>,
    pub memory_id: Option<String>,
    pub capsule_id: Option<String>,
    pub priority: i64,
    pub attempts: i64,
    pub max_attempts: i64,
    pub locked_by: Option<String>,
    pub lock_expires_at_unix: Option<i64>,
    pub scheduled_at_unix: i64,
    pub started_at_unix: Option<i64>,
    pub completed_at_unix: Option<i64>,
    pub last_error: Option<String>,
    pub payload_json: Option<String>,
    pub result_json: Option<String>,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
}

pub fn normalized_memory_namespace(namespace: Option<&str>) -> Result<String> {
    match namespace {
        Some(namespace) => {
            let namespace = namespace.trim();
            if namespace.is_empty() {
                bail!("memory namespace cannot be empty");
            }
            Ok(namespace.to_owned())
        }
        None => Ok(MEMORY_NAMESPACE_DEFAULT.to_owned()),
    }
}

pub fn normalized_memory_key(key: &str) -> Result<String> {
    let key = key.trim();
    if key.is_empty() {
        bail!("memory key cannot be empty");
    }
    Ok(key.to_owned())
}

pub fn normalized_optional_memory_key(key: Option<String>) -> Result<Option<String>> {
    key.map(|key| normalized_memory_key(key.as_str()))
        .transpose()
}

pub fn memory_scope_key_hash(kind: MemoryScopeKind, key: &str) -> Result<String> {
    let normalized_key = normalized_scope_key(key)?;
    let mut hasher = Sha256::new();
    hasher.update(memory_scope_kind_to_db(kind).as_bytes());
    hasher.update([0]);
    hasher.update(normalized_key.as_bytes());
    Ok(hex::encode(hasher.finalize()))
}

pub fn workspace_agent_memory_scope_key(workspace_id: &str, agent_id: &str) -> String {
    format!(
        "workspace:{}:agent:{}",
        workspace_id.trim(),
        agent_id.trim()
    )
}

pub fn global_agent_memory_scope_key(agent_id: &str) -> String {
    format!("global:agent:{}", agent_id.trim())
}

pub fn normalized_scope_key(key: &str) -> Result<String> {
    let key = key.trim();
    if key.is_empty() {
        bail!("memory scope key cannot be empty");
    }
    Ok(key.to_owned())
}

pub fn parse_workspace_agent_scope_key(key: &str) -> Option<String> {
    let key = key.trim();
    let rest = key.strip_prefix("workspace:")?;
    let (workspace_id, agent_part) = rest.split_once(":agent:")?;
    if workspace_id.is_empty() || agent_part.is_empty() {
        return None;
    }
    Some(workspace_id.to_owned())
}

pub fn is_global_agent_scope_key(key: &str) -> bool {
    key.trim()
        .strip_prefix("global:agent:")
        .is_some_and(|agent_id| !agent_id.is_empty())
}

pub(crate) fn actor_kind_to_db(actor: &Option<MemoryActorRecord>) -> Option<String> {
    actor
        .as_ref()
        .map(|actor| memory_actor_kind_to_db(actor.kind).to_owned())
}

pub(crate) fn actor_id_to_db(actor: &Option<MemoryActorRecord>) -> Option<String> {
    actor.as_ref().and_then(|actor| actor.id.clone())
}

pub(crate) fn actor_from_db(
    kind: Option<String>,
    id: Option<String>,
) -> Result<Option<MemoryActorRecord>> {
    Ok(match kind {
        Some(kind) => Some(MemoryActorRecord {
            kind: memory_actor_kind_from_db(kind.as_str())?,
            id,
        }),
        None => None,
    })
}

pub(crate) fn lifecycle_actor_kind_to_db(actor: &MemoryLifecycleActorRecord) -> String {
    memory_lifecycle_actor_kind_to_db(actor.kind)
}

pub(crate) fn lifecycle_actor_id_to_db(actor: &MemoryLifecycleActorRecord) -> Option<String> {
    actor.id.clone()
}

pub(crate) fn lifecycle_actor_from_db(
    kind: String,
    id: Option<String>,
) -> Result<MemoryLifecycleActorRecord> {
    Ok(MemoryLifecycleActorRecord {
        kind: memory_lifecycle_actor_kind_from_db(kind.as_str())?,
        id,
    })
}

pub(crate) fn optional_lifecycle_actor_from_db(
    kind: Option<String>,
    id: Option<String>,
) -> Result<Option<MemoryLifecycleActorRecord>> {
    Ok(match kind {
        Some(kind) => Some(MemoryLifecycleActorRecord {
            kind: memory_lifecycle_actor_kind_from_db(kind.as_str())?,
            id,
        }),
        None => None,
    })
}

pub(crate) fn workspace_allowed_by_guard(
    scope_kind: MemoryScopeKind,
    workspace_id: &Option<String>,
    guard: &MemoryWorkspaceGuard,
) -> bool {
    match workspace_id.as_deref() {
        Some(workspace_id) => workspace_id == guard.workspace_id,
        None => match scope_kind {
            MemoryScopeKind::User => guard.allow_global_user,
            MemoryScopeKind::Agent => guard.allow_global_agent,
            _ => false,
        },
    }
}

pub(crate) fn agent_memory_control_record_from_model(
    model: pioneer_entity::agent_memory::Model,
) -> Result<AgentMemoryControlRecord> {
    let scope_kind = memory_scope_kind_from_db(model.scope_kind.as_str())?;
    Ok(AgentMemoryControlRecord {
        id: model.id,
        scope: MemoryScope {
            kind: scope_kind,
            key: model.scope_key,
        },
        scope_key_hash: model.scope_key_hash,
        workspace_id: model.workspace_id,
        namespace: model.namespace,
        category: memory_category_from_db(model.category.as_str())?,
        key: model.key,
        active_key: model.active_key,
        status: memory_status_from_db(model.status.as_str())?,
        sensitivity: memory_sensitivity_from_db(model.sensitivity.as_str())?,
        confidence: model.confidence,
        importance: model.importance,
        content_preview: model.content_preview,
        capsule_id: model.capsule_id,
        capsule_ref: model.capsule_ref,
        frame_id: model.frame_id,
        frame_uri: model.frame_uri,
        frame_version: model.frame_version,
        source_context_kind: model
            .source_context_kind
            .as_deref()
            .map(memory_source_context_kind_from_db),
        source_thread_id: model.source_thread_id,
        source_turn_id: model.source_turn_id,
        source_item_id: model.source_item_id,
        created_by: actor_from_db(model.created_by_kind, model.created_by_id)?,
        created_at_unix: model.created_at.timestamp(),
        updated_at_unix: model.updated_at.timestamp(),
        last_accessed_at_unix: model
            .last_accessed_at
            .map(|timestamp| timestamp.timestamp()),
        access_count: model.access_count,
        expires_at_unix: model.expires_at.map(|timestamp| timestamp.timestamp()),
        superseded_by: model.superseded_by,
        deleted_at_unix: model.deleted_at.map(|timestamp| timestamp.timestamp()),
        deleted_by: actor_from_db(model.deleted_by_kind, model.deleted_by_id)?,
        delete_reason: model.delete_reason,
        policy_version: model.policy_version,
        repair_status: model.repair_status,
        metadata_json: model.metadata_json,
    })
}

pub(crate) fn agent_memory_event_record_from_model(
    model: pioneer_entity::agent_memory_event::Model,
) -> Result<AgentMemoryEventRecord> {
    Ok(AgentMemoryEventRecord {
        id: model.id,
        memory_id: model.memory_id,
        candidate_id: model.candidate_id,
        workspace_id: model.workspace_id,
        event_kind: model.event_kind,
        actor: actor_from_db(model.actor_kind, model.actor_id)?,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        item_id: model.item_id,
        details_json: model.details_json,
        created_at_unix: model.created_at.timestamp(),
    })
}

pub(crate) fn agent_memory_quarantine_record_from_model(
    model: pioneer_entity::agent_memory_quarantine::Model,
) -> Result<AgentMemoryQuarantineRecord> {
    let created_at_unix = model
        .created_at
        .as_ref()
        .with_context(|| format!("quarantine marker `{}` is missing created_at", model.id))?
        .timestamp();

    Ok(AgentMemoryQuarantineRecord {
        id: model.id,
        memory_id: model.memory_id,
        workspace_id: model.workspace_id,
        reason_code: memory_lifecycle_reason_code_from_db(model.reason_code.as_str())?,
        actor: lifecycle_actor_from_db(model.actor_kind, model.actor_id)?,
        created_at_unix,
        resolved_at_unix: model.resolved_at.map(|timestamp| timestamp.timestamp()),
        resolved_reason_code: model
            .resolved_reason_code
            .as_deref()
            .map(memory_lifecycle_reason_code_from_db)
            .transpose()?,
        resolved_actor: optional_lifecycle_actor_from_db(
            model.resolved_actor_kind,
            model.resolved_actor_id,
        )?,
        details_json: model.details_json,
    })
}

pub(crate) fn agent_memory_candidate_record_from_model(
    model: pioneer_entity::agent_memory_candidate::Model,
) -> Result<AgentMemoryCandidateRecord> {
    let scope_kind = memory_scope_kind_from_db(model.scope_kind.as_str())?;
    Ok(AgentMemoryCandidateRecord {
        id: model.id,
        scope: MemoryScope {
            kind: scope_kind,
            key: model.scope_key,
        },
        scope_key_hash: model.scope_key_hash,
        workspace_id: model.workspace_id,
        namespace: model.namespace,
        category: memory_category_from_db(model.category.as_str())?,
        key: model.key,
        candidate_text: model.candidate_text,
        confidence: model.confidence,
        reason: model.reason,
        source_context_kind: model
            .source_context_kind
            .as_deref()
            .map(memory_source_context_kind_from_db),
        source_thread_id: model.source_thread_id,
        source_turn_id: model.source_turn_id,
        source_item_id: model.source_item_id,
        created_by: actor_from_db(model.created_by_kind, model.created_by_id)?,
        status: memory_candidate_status_from_db(model.status.as_str())?,
        dedupe_key: model.dedupe_key,
        created_at_unix: model.created_at.timestamp(),
        decided_at_unix: model.decided_at.map(|timestamp| timestamp.timestamp()),
        decided_by: actor_from_db(model.decided_by_kind, model.decided_by_id)?,
        decision_reason: model.decision_reason,
        promoted_memory_id: model.promoted_memory_id,
        metadata_json: model.metadata_json,
    })
}

pub(crate) fn agent_memory_capsule_record_from_model(
    model: pioneer_entity::agent_memory_capsule::Model,
) -> Result<AgentMemoryCapsuleRecord> {
    let scope_kind = memory_scope_kind_from_db(model.scope_kind.as_str())?;
    Ok(AgentMemoryCapsuleRecord {
        id: Some(model.id),
        scope: MemoryScope {
            kind: scope_kind,
            key: model.scope_key,
        },
        scope_key_hash: Some(model.scope_key_hash),
        workspace_id: model.workspace_id,
        scope_slot: Some(model.scope_slot),
        capsule_ref: model.capsule_ref,
        storage_uri: model.storage_uri,
        backend: model.backend,
        format: model.format,
        encrypted: model.encrypted,
        status: model.status,
        repair_status: model.repair_status,
        content_hash: model.content_hash,
        active_record_count: model.active_record_count,
        metadata_json: model.metadata_json,
        created_at_unix: Some(model.created_at.timestamp()),
        updated_at_unix: Some(model.updated_at.timestamp()),
        last_error: model.last_error,
    })
}

pub(crate) fn agent_memory_policy_decision_record_from_model(
    model: pioneer_entity::agent_memory_policy_decision::Model,
) -> Result<AgentMemoryPolicyDecisionRecord> {
    Ok(AgentMemoryPolicyDecisionRecord {
        id: model.id,
        memory_id: model.memory_id,
        candidate_id: model.candidate_id,
        workspace_id: model.workspace_id,
        action: model.action,
        decision: model.decision,
        reason_code: model.reason_code,
        reason: model.reason,
        policy_version: model.policy_version,
        actor: actor_from_db(model.actor_kind, model.actor_id)?,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        item_id: model.item_id,
        details_json: model.details_json,
        created_at_unix: model.created_at.timestamp(),
    })
}

pub(crate) fn agent_memory_quality_decision_record_from_model(
    model: pioneer_entity::agent_memory_quality_decision::Model,
) -> Result<AgentMemoryQualityDecisionRecord> {
    Ok(AgentMemoryQualityDecisionRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        item_id: model.item_id,
        task_id: model.task_id,
        memory_id: model.memory_id,
        candidate_id: model.candidate_id,
        canonical_key: model.canonical_key,
        action: memory_quality_action_from_db(model.action.as_str())?,
        target_ownership: memory_ownership_class_from_db(model.target_ownership.as_str())?,
        source_context_kind: memory_source_context_kind_from_db(model.source_context_kind.as_str()),
        fact_class: memory_fact_class_from_db(model.fact_class.as_str())?,
        lifetime_class: memory_lifetime_class_from_db(model.lifetime_class.as_str())?,
        ownership_class: memory_ownership_class_from_db(model.ownership_class.as_str())?,
        evidence_class: memory_evidence_class_from_db(model.evidence_class.as_str())?,
        relation: memory_write_relation_from_db(model.relation.as_str())?,
        reason_codes: serde_json::from_str(model.reason_codes_json.as_str())?,
        input_snapshot_json: model.input_snapshot_json,
        created_at_unix: model.created_at.timestamp(),
        updated_at_unix: model.updated_at.timestamp(),
    })
}

pub(crate) fn agent_memory_repair_job_record_from_model(
    model: pioneer_entity::agent_memory_repair_job::Model,
) -> Result<AgentMemoryRepairJobRecord> {
    Ok(AgentMemoryRepairJobRecord {
        id: model.id,
        job_kind: model.job_kind,
        status: model.status,
        workspace_id: model.workspace_id,
        scope_kind: model
            .scope_kind
            .as_deref()
            .map(memory_scope_kind_from_db)
            .transpose()?,
        scope_key_hash: model.scope_key_hash,
        memory_id: model.memory_id,
        capsule_id: model.capsule_id,
        priority: model.priority,
        attempts: model.attempts,
        max_attempts: model.max_attempts,
        locked_by: model.locked_by,
        lock_expires_at_unix: model.lock_expires_at.map(|timestamp| timestamp.timestamp()),
        scheduled_at_unix: model.scheduled_at.timestamp(),
        started_at_unix: model.started_at.map(|timestamp| timestamp.timestamp()),
        completed_at_unix: model.completed_at.map(|timestamp| timestamp.timestamp()),
        last_error: model.last_error,
        payload_json: model.payload_json,
        result_json: model.result_json,
        created_at_unix: model.created_at.timestamp(),
        updated_at_unix: model.updated_at.timestamp(),
    })
}
