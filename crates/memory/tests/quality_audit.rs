use pioneer_memory::{
    MemoryQualityAuditItemKind, MemoryQualityAuditStatus, audit_memory_candidate_quality,
    audit_memory_record_quality,
};
use pioneer_protocol::{
    MemoryActor, MemoryActorKind, MemoryAttribute, MemoryCandidate, MemoryCandidateStatus,
    MemoryCategory, MemoryDurability, MemoryEvidenceClass, MemoryExplicitness,
    MemoryExtractorCertainty, MemoryFactClass, MemoryIntent, MemoryLifetimeClass,
    MemoryOwnershipClass, MemoryProvenance, MemoryRecord, MemoryScope, MemoryScopeHint,
    MemoryScopeKind, MemorySemanticFields, MemorySensitivity, MemorySensitivityHint,
    MemorySourceContextKind, MemoryStatus, MemorySubject,
};
use serde_json::Value;
use std::collections::BTreeMap;

#[test]
fn quality_audit_is_read_only_for_records_and_candidates() {
    let record = representative_memory_record();
    let candidate = representative_memory_candidate();
    let record_before = record.clone();
    let candidate_before = candidate.clone();

    let record_audit = audit_memory_record_quality(&record);
    let candidate_audit = audit_memory_candidate_quality(&candidate);

    assert_eq!(record, record_before);
    assert_eq!(candidate, candidate_before);

    assert_eq!(record_audit.item_kind, MemoryQualityAuditItemKind::Record);
    assert_eq!(
        record_audit.status,
        MemoryQualityAuditStatus::Memory(MemoryStatus::Active)
    );
    assert_eq!(record_audit.fact_class, MemoryFactClass::UserIdentity);
    assert_eq!(record_audit.lifetime_class, MemoryLifetimeClass::LongLived);
    assert_eq!(
        record_audit.ownership_class,
        MemoryOwnershipClass::DurableUserMemory
    );
    assert_eq!(
        record_audit.source_context_kind,
        MemorySourceContextKind::DirectUserConversation
    );
    assert_eq!(
        record_audit.evidence_class,
        MemoryEvidenceClass::DirectUserAssertion
    );

    assert_eq!(
        candidate_audit.item_kind,
        MemoryQualityAuditItemKind::Candidate
    );
    assert_eq!(
        candidate_audit.status,
        MemoryQualityAuditStatus::Candidate(MemoryCandidateStatus::PendingSilent)
    );
    assert_eq!(candidate_audit.fact_class, MemoryFactClass::ProjectDecision);
    assert_eq!(
        candidate_audit.lifetime_class,
        MemoryLifetimeClass::ProjectLifetime
    );
    assert_eq!(
        candidate_audit.ownership_class,
        MemoryOwnershipClass::DurableWorkspaceMemory
    );
    assert_eq!(candidate_audit.candidate_score, Some(0.88));
    assert_eq!(
        candidate_audit.source_thread_id.as_deref(),
        Some("thread-2")
    );
}

fn representative_memory_record() -> MemoryRecord {
    MemoryRecord {
        id: "memory-1".to_owned(),
        scope: scope(MemoryScopeKind::User, "user-1"),
        namespace: Some("default".to_owned()),
        category: MemoryCategory::Identity,
        key: Some("user/global:identity:self:name".to_owned()),
        content: "User name is Alexander.".to_owned(),
        status: MemoryStatus::Active,
        confidence: 0.96,
        importance: 0.82,
        sensitivity: MemorySensitivity::Personal,
        provenance: provenance("thread-1", "turn-1"),
        source_context_kind: None,
        created_at: 1,
        updated_at: 1,
        expires_at: None,
        last_accessed_at: None,
        access_count: 0,
        superseded_by: None,
        deleted_at: None,
        delete_reason: None,
        metadata: metadata_with_semantic_and_evidence(
            "user/global:identity:self:name",
            semantic(
                MemoryCategory::Identity,
                MemorySubject::CurrentUser,
                MemoryAttribute::Name,
                MemoryScopeHint::UserGlobal,
                MemoryDurability::LongLived,
            ),
            "turn.post_turn:user",
            "thread-1",
            "turn-1",
        ),
    }
}

fn representative_memory_candidate() -> MemoryCandidate {
    let mut metadata = metadata_with_semantic_and_evidence(
        "workspace/project:project_decision:self:phase_naming",
        semantic(
            MemoryCategory::ProjectDecision,
            MemorySubject::Project,
            MemoryAttribute::PhaseNaming,
            MemoryScopeHint::ProjectWorkspace,
            MemoryDurability::ProjectLifetime,
        ),
        "turn.post_turn:user",
        "thread-2",
        "turn-2",
    );
    metadata.insert(
        "candidate_score".to_owned(),
        serde_json::json!({
            "total_score": 0.88,
            "bucket": "high",
            "explicitness_score": 0.2,
            "durability_score": 0.2,
            "scope_score": 0.15,
            "evidence_score": 0.1,
            "certainty_score": 0.15,
            "sensitivity_score": 0.07,
            "relation_score": 0.01,
            "reasons": []
        }),
    );

    MemoryCandidate {
        id: "candidate-1".to_owned(),
        scope: scope(MemoryScopeKind::Workspace, "workspace-1"),
        category: MemoryCategory::ProjectDecision,
        key: Some("workspace/project:project_decision:self:phase_naming".to_owned()),
        candidate_text: "Project uses Phase naming.".to_owned(),
        confidence: 0.74,
        reason: "candidate_policy".to_owned(),
        provenance: provenance("thread-2", "turn-2"),
        source_context_kind: None,
        status: MemoryCandidateStatus::PendingSilent,
        created_at: 2,
        decided_at: None,
        decision_reason: None,
        metadata,
    }
}

fn metadata_with_semantic_and_evidence(
    canonical_key: &str,
    semantic: MemorySemanticFields,
    source_ref: &str,
    thread_id: &str,
    turn_id: &str,
) -> BTreeMap<String, Value> {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "semantic".to_owned(),
        serde_json::json!({
            "canonical_key": canonical_key,
            "fields": semantic,
        }),
    );
    metadata.insert(
        "evidence".to_owned(),
        serde_json::json!({
            "sources": [
                {
                    "source_thread_id": thread_id,
                    "source_turn_id": turn_id,
                    "source_item_id": null,
                    "source_ref": source_ref,
                    "quote_or_span": "structured evidence",
                    "extractor_reason": null
                }
            ]
        }),
    );
    metadata
}

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

fn scope(kind: MemoryScopeKind, key: &str) -> MemoryScope {
    MemoryScope {
        kind,
        key: key.to_owned(),
    }
}

fn provenance(thread_id: &str, turn_id: &str) -> MemoryProvenance {
    MemoryProvenance {
        source_thread_id: Some(thread_id.to_owned()),
        source_turn_id: Some(turn_id.to_owned()),
        source_item_id: None,
        created_by: Some(MemoryActor {
            kind: MemoryActorKind::Extractor,
            id: Some("memory.post_turn_extractor".to_owned()),
        }),
    }
}
