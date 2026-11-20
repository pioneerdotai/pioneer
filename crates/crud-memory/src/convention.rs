use anyhow::{Result, bail};
use pioneer_protocol::{
    MemoryActorKind, MemoryCandidateStatus, MemoryCategory, MemoryEvidenceClass, MemoryFactClass,
    MemoryLifecycleActorKind, MemoryLifecycleReasonCode, MemoryLifetimeClass, MemoryOwnershipClass,
    MemoryQualityAction, MemoryScopeKind, MemorySensitivity, MemorySourceContextKind, MemoryStatus,
    MemoryWriteRelation,
};
use serde::{Serialize, de::DeserializeOwned};

pub(crate) const DB_ID_LEN: usize = 21;

pub const MEMORY_NAMESPACE_DEFAULT: &str = "default";
pub const MEMORY_REPAIR_STATUS_OK: &str = "ok";

pub const MEMORY_EVENT_CREATED: &str = "created";
pub const MEMORY_EVENT_UPDATED: &str = "updated";
pub const MEMORY_EVENT_FORGOTTEN: &str = "forgotten";
pub const MEMORY_EVENT_SUPERSEDED: &str = "superseded";
pub const MEMORY_EVENT_EXPIRED: &str = "expired";
pub const MEMORY_EVENT_ACCESSED: &str = "accessed";
pub const MEMORY_EVENT_REPAIR_STATUS_CHANGED: &str = "repair_status_changed";
pub const MEMORY_EVENT_QUARANTINED: &str = "quarantined";
pub const MEMORY_EVENT_RESTORED: &str = "restored";
pub const MEMORY_EVENT_CANDIDATE_CREATED: &str = "candidate_created";
pub const MEMORY_EVENT_CANDIDATE_APPROVED: &str = "candidate_approved";
pub const MEMORY_EVENT_CANDIDATE_REJECTED: &str = "candidate_rejected";
pub const MEMORY_EVENT_CANDIDATE_EXPIRED: &str = "candidate_expired";

pub const MEMORY_REPAIR_JOB_STATUS_PENDING: &str = "pending";
pub const MEMORY_REPAIR_JOB_STATUS_RUNNING: &str = "running";
pub const MEMORY_REPAIR_JOB_STATUS_COMPLETED: &str = "completed";
pub const MEMORY_REPAIR_JOB_STATUS_FAILED: &str = "failed";

pub fn memory_scope_kind_to_db(kind: MemoryScopeKind) -> &'static str {
    match kind {
        MemoryScopeKind::User => "user",
        MemoryScopeKind::Workspace => "workspace",
        MemoryScopeKind::Thread => "thread",
        MemoryScopeKind::Agent => "agent",
        MemoryScopeKind::Task => "task",
    }
}

pub fn memory_scope_kind_from_db(value: &str) -> Result<MemoryScopeKind> {
    match value {
        "user" => Ok(MemoryScopeKind::User),
        "workspace" => Ok(MemoryScopeKind::Workspace),
        "thread" => Ok(MemoryScopeKind::Thread),
        "agent" => Ok(MemoryScopeKind::Agent),
        "task" => Ok(MemoryScopeKind::Task),
        _ => bail!("unknown memory scope kind `{value}`"),
    }
}

pub fn memory_category_to_db(category: MemoryCategory) -> &'static str {
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

pub fn memory_category_from_db(value: &str) -> Result<MemoryCategory> {
    match value {
        "identity" => Ok(MemoryCategory::Identity),
        "preference" => Ok(MemoryCategory::Preference),
        "biography" => Ok(MemoryCategory::Biography),
        "relationship" => Ok(MemoryCategory::Relationship),
        "recurring_instruction" => Ok(MemoryCategory::RecurringInstruction),
        "project_policy" => Ok(MemoryCategory::ProjectPolicy),
        "project_fact" => Ok(MemoryCategory::ProjectFact),
        "project_decision" => Ok(MemoryCategory::ProjectDecision),
        "procedure" => Ok(MemoryCategory::Procedure),
        "todo" => Ok(MemoryCategory::Todo),
        "constraint" => Ok(MemoryCategory::Constraint),
        "communication_style" => Ok(MemoryCategory::CommunicationStyle),
        "custom" => Ok(MemoryCategory::Custom),
        _ => bail!("unknown memory category `{value}`"),
    }
}

pub fn memory_status_to_db(status: MemoryStatus) -> &'static str {
    match status {
        MemoryStatus::Active => "active",
        MemoryStatus::Superseded => "superseded",
        MemoryStatus::Deleted => "deleted",
        MemoryStatus::Expired => "expired",
    }
}

pub fn memory_status_from_db(value: &str) -> Result<MemoryStatus> {
    match value {
        "active" => Ok(MemoryStatus::Active),
        "superseded" => Ok(MemoryStatus::Superseded),
        "deleted" => Ok(MemoryStatus::Deleted),
        "expired" => Ok(MemoryStatus::Expired),
        _ => bail!("unknown memory status `{value}`"),
    }
}

pub fn memory_sensitivity_to_db(sensitivity: MemorySensitivity) -> &'static str {
    match sensitivity {
        MemorySensitivity::Normal => "normal",
        MemorySensitivity::Personal => "personal",
        MemorySensitivity::SecretLike => "secret_like",
        MemorySensitivity::Regulated => "regulated",
    }
}

pub fn memory_sensitivity_from_db(value: &str) -> Result<MemorySensitivity> {
    match value {
        "normal" => Ok(MemorySensitivity::Normal),
        "personal" => Ok(MemorySensitivity::Personal),
        "secret_like" => Ok(MemorySensitivity::SecretLike),
        "regulated" => Ok(MemorySensitivity::Regulated),
        _ => bail!("unknown memory sensitivity `{value}`"),
    }
}

pub fn memory_source_context_kind_to_db(kind: MemorySourceContextKind) -> &'static str {
    match kind {
        MemorySourceContextKind::DirectUserConversation => "direct_user_conversation",
        MemorySourceContextKind::AssistantResponse => "assistant_response",
        MemorySourceContextKind::ToolResult => "tool_result",
        MemorySourceContextKind::TaskRuntime => "task_runtime",
        MemorySourceContextKind::SystemRuntime => "system_runtime",
        MemorySourceContextKind::DeveloperInstruction => "developer_instruction",
        MemorySourceContextKind::ConnectorContent => "connector_content",
        MemorySourceContextKind::ImportedDocument => "imported_document",
        MemorySourceContextKind::GeneratedSummary => "generated_summary",
        MemorySourceContextKind::Unknown => "unknown",
    }
}

pub fn memory_source_context_kind_from_db(value: &str) -> MemorySourceContextKind {
    match value {
        "direct_user_conversation" => MemorySourceContextKind::DirectUserConversation,
        "assistant_response" => MemorySourceContextKind::AssistantResponse,
        "tool_result" => MemorySourceContextKind::ToolResult,
        "task_runtime" => MemorySourceContextKind::TaskRuntime,
        "system_runtime" => MemorySourceContextKind::SystemRuntime,
        "developer_instruction" => MemorySourceContextKind::DeveloperInstruction,
        "connector_content" => MemorySourceContextKind::ConnectorContent,
        "imported_document" => MemorySourceContextKind::ImportedDocument,
        "generated_summary" => MemorySourceContextKind::GeneratedSummary,
        "unknown" => MemorySourceContextKind::Unknown,
        _ => MemorySourceContextKind::Unknown,
    }
}

pub fn memory_quality_action_to_db(action: MemoryQualityAction) -> String {
    enum_to_snake_string(action)
}

pub fn memory_quality_action_from_db(value: &str) -> Result<MemoryQualityAction> {
    enum_from_snake_string(value, "memory quality action")
}

pub fn memory_fact_class_to_db(fact_class: MemoryFactClass) -> String {
    enum_to_snake_string(fact_class)
}

pub fn memory_fact_class_from_db(value: &str) -> Result<MemoryFactClass> {
    enum_from_snake_string(value, "memory fact class")
}

pub fn memory_lifetime_class_to_db(lifetime_class: MemoryLifetimeClass) -> String {
    enum_to_snake_string(lifetime_class)
}

pub fn memory_lifetime_class_from_db(value: &str) -> Result<MemoryLifetimeClass> {
    enum_from_snake_string(value, "memory lifetime class")
}

pub fn memory_ownership_class_to_db(ownership_class: MemoryOwnershipClass) -> String {
    enum_to_snake_string(ownership_class)
}

pub fn memory_ownership_class_from_db(value: &str) -> Result<MemoryOwnershipClass> {
    enum_from_snake_string(value, "memory ownership class")
}

pub fn memory_evidence_class_to_db(evidence_class: MemoryEvidenceClass) -> String {
    enum_to_snake_string(evidence_class)
}

pub fn memory_evidence_class_from_db(value: &str) -> Result<MemoryEvidenceClass> {
    enum_from_snake_string(value, "memory evidence class")
}

pub fn memory_write_relation_to_db(relation: MemoryWriteRelation) -> String {
    enum_to_snake_string(relation)
}

pub fn memory_write_relation_from_db(value: &str) -> Result<MemoryWriteRelation> {
    enum_from_snake_string(value, "memory write relation")
}

fn enum_to_snake_string<T: Serialize>(value: T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .expect("protocol enum should serialize as a string")
}

fn enum_from_snake_string<T: DeserializeOwned>(value: &str, type_name: &str) -> Result<T> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|err| anyhow::anyhow!("unknown {type_name} `{value}`: {err}"))
}

pub fn memory_actor_kind_to_db(kind: MemoryActorKind) -> &'static str {
    match kind {
        MemoryActorKind::User => "user",
        MemoryActorKind::Assistant => "assistant",
        MemoryActorKind::Extractor => "extractor",
        MemoryActorKind::System => "system",
        MemoryActorKind::Tool => "tool",
    }
}

pub fn memory_actor_kind_from_db(value: &str) -> Result<MemoryActorKind> {
    match value {
        "user" => Ok(MemoryActorKind::User),
        "assistant" => Ok(MemoryActorKind::Assistant),
        "extractor" => Ok(MemoryActorKind::Extractor),
        "system" => Ok(MemoryActorKind::System),
        "tool" => Ok(MemoryActorKind::Tool),
        _ => bail!("unknown memory actor kind `{value}`"),
    }
}

pub fn memory_lifecycle_actor_kind_to_db(kind: MemoryLifecycleActorKind) -> String {
    enum_to_snake_string(kind)
}

pub fn memory_lifecycle_actor_kind_from_db(value: &str) -> Result<MemoryLifecycleActorKind> {
    enum_from_snake_string(value, "memory lifecycle actor kind")
}

pub fn memory_lifecycle_reason_code_to_db(reason: MemoryLifecycleReasonCode) -> String {
    enum_to_snake_string(reason)
}

pub fn memory_lifecycle_reason_code_from_db(value: &str) -> Result<MemoryLifecycleReasonCode> {
    // Preserve historical audit/quarantine records without exposing an obsolete
    // engine-specific reason in the current product protocol.
    if value == "memvid_stale_vector" {
        return Ok(MemoryLifecycleReasonCode::StaleEmbeddingProjection);
    }
    enum_from_snake_string(value, "memory lifecycle reason code")
}

pub fn memory_candidate_status_to_db(status: MemoryCandidateStatus) -> &'static str {
    match status {
        MemoryCandidateStatus::Pending => "pending",
        MemoryCandidateStatus::PendingSilent => "pending_silent",
        MemoryCandidateStatus::AskOnUse => "ask_on_use",
        MemoryCandidateStatus::NeedsReview => "needs_review",
        MemoryCandidateStatus::Approved => "approved",
        MemoryCandidateStatus::Rejected => "rejected",
        MemoryCandidateStatus::AutoRejected => "auto_rejected",
        MemoryCandidateStatus::ReviewDisabledRejected => "review_disabled_rejected",
        MemoryCandidateStatus::Superseded => "superseded",
        MemoryCandidateStatus::MergedDuplicate => "merged_duplicate",
        MemoryCandidateStatus::Expired => "expired",
    }
}

pub fn memory_candidate_status_from_db(value: &str) -> Result<MemoryCandidateStatus> {
    match value {
        "pending" => Ok(MemoryCandidateStatus::Pending),
        "pending_silent" => Ok(MemoryCandidateStatus::PendingSilent),
        "ask_on_use" => Ok(MemoryCandidateStatus::AskOnUse),
        "needs_review" => Ok(MemoryCandidateStatus::NeedsReview),
        "approved" => Ok(MemoryCandidateStatus::Approved),
        "rejected" => Ok(MemoryCandidateStatus::Rejected),
        "auto_rejected" => Ok(MemoryCandidateStatus::AutoRejected),
        "review_disabled_rejected" => Ok(MemoryCandidateStatus::ReviewDisabledRejected),
        "superseded" => Ok(MemoryCandidateStatus::Superseded),
        "merged_duplicate" => Ok(MemoryCandidateStatus::MergedDuplicate),
        "expired" => Ok(MemoryCandidateStatus::Expired),
        _ => bail!("unknown memory candidate status `{value}`"),
    }
}
