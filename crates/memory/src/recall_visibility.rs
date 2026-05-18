use crate::MemoryReadPolicy;
use crate::convert::parse_metadata_json;
use crate::quality::audit_memory_record_quality;
use pioneer_crud::{AgentMemoryControlRecord, AgentMemoryQualityDecisionRecord, MemoryActorRecord};
use pioneer_protocol::{
    MemoryActor, MemoryEvidenceClass, MemoryFactClass, MemoryLifetimeClass, MemoryOwnershipClass,
    MemoryProvenance, MemoryQualityAction, MemoryQualityReasonCode, MemoryRecord,
    MemorySensitivity, MemorySourceContextKind, MemoryStatus, MemoryWriteRelation,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum MemoryRecallVisibility {
    Visible,
    SuppressDeleted,
    SuppressSuperseded,
    SuppressExpired,
    SuppressWorkspaceMismatch,
    SuppressSensitivity,
    SuppressOwnershipMismatch,
    SuppressLowQualitySourceContext,
    SuppressRejectedRelated,
    SuppressQuarantinedOrAuditOnly,
    SuppressBackendRepairRequired,
}

impl MemoryRecallVisibility {
    pub(crate) fn is_visible(self) -> bool {
        self == Self::Visible
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Visible => "visible",
            Self::SuppressDeleted => "suppress_deleted",
            Self::SuppressSuperseded => "suppress_superseded",
            Self::SuppressExpired => "suppress_expired",
            Self::SuppressWorkspaceMismatch => "suppress_workspace_mismatch",
            Self::SuppressSensitivity => "suppress_sensitivity",
            Self::SuppressOwnershipMismatch => "suppress_ownership_mismatch",
            Self::SuppressLowQualitySourceContext => "suppress_low_quality_source_context",
            Self::SuppressRejectedRelated => "suppress_rejected_related",
            Self::SuppressQuarantinedOrAuditOnly => "suppress_quarantined_or_audit_only",
            Self::SuppressBackendRepairRequired => "suppress_backend_repair_required",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MemoryRecallQualitySignals {
    pub(crate) source_context_kind: MemorySourceContextKind,
    pub(crate) evidence_class: MemoryEvidenceClass,
    pub(crate) fact_class: MemoryFactClass,
    pub(crate) lifetime_class: MemoryLifetimeClass,
    pub(crate) ownership_class: MemoryOwnershipClass,
    pub(crate) quality_action: Option<MemoryQualityAction>,
    pub(crate) quality_target_ownership: Option<MemoryOwnershipClass>,
    pub(crate) quality_reason_codes: Vec<MemoryQualityReasonCode>,
    pub(crate) write_relation: Option<MemoryWriteRelation>,
    pub(crate) metadata_valid: bool,
}

impl MemoryRecallQualitySignals {
    #[cfg(test)]
    pub(crate) fn direct_user_default() -> Self {
        Self {
            source_context_kind: MemorySourceContextKind::DirectUserConversation,
            evidence_class: MemoryEvidenceClass::DirectUserAssertion,
            fact_class: MemoryFactClass::Unknown,
            lifetime_class: MemoryLifetimeClass::Unknown,
            ownership_class: MemoryOwnershipClass::AuditOnly,
            quality_action: None,
            quality_target_ownership: None,
            quality_reason_codes: Vec::new(),
            write_relation: None,
            metadata_valid: true,
        }
    }

    pub(crate) fn has_reason(&self, reason: MemoryQualityReasonCode) -> bool {
        self.quality_reason_codes.contains(&reason)
    }

    pub(crate) fn is_rejected_related(&self) -> bool {
        matches!(
            self.write_relation,
            Some(MemoryWriteRelation::SuppressedByRejection)
        )
    }

    pub(crate) fn is_low_quality_source_context(&self) -> bool {
        !self.metadata_valid
            || self.source_context_kind == MemorySourceContextKind::Unknown
            || self.evidence_class == MemoryEvidenceClass::MissingOrWeak
            || self.has_reason(MemoryQualityReasonCode::SourceIneligible)
            || self.has_reason(MemoryQualityReasonCode::WeakEvidence)
            || self.has_reason(MemoryQualityReasonCode::WeakOrMissingEvidence)
            || self.has_reason(MemoryQualityReasonCode::UnknownSourceContext)
            || self.has_reason(MemoryQualityReasonCode::SourcePolicyMissing)
            || self.has_reason(MemoryQualityReasonCode::SourceNotAuthoritativeForDurableMemory)
            || self.has_reason(MemoryQualityReasonCode::AssistantInferenceNotDurableEvidence)
    }

    pub(crate) fn is_quality_terminal(&self) -> bool {
        if self.is_duplicate_write_attempt() {
            return false;
        }
        matches!(
            self.quality_action,
            Some(
                MemoryQualityAction::ForceReject
                    | MemoryQualityAction::Quarantine
                    | MemoryQualityAction::RouteToThreadEpisodic
                    | MemoryQualityAction::RouteToTaskState
                    | MemoryQualityAction::RouteToDomainState
            )
        )
    }

    pub(crate) fn is_audit_or_reject_ownership(&self) -> bool {
        if self.is_duplicate_write_attempt() {
            return false;
        }
        matches!(
            self.quality_target_ownership,
            Some(MemoryOwnershipClass::AuditOnly | MemoryOwnershipClass::Reject)
        ) || (self.fact_class != MemoryFactClass::Unknown
            && matches!(
                self.ownership_class,
                MemoryOwnershipClass::AuditOnly | MemoryOwnershipClass::Reject
            ))
    }

    pub(crate) fn has_ownership_mismatch(&self) -> bool {
        self.has_reason(MemoryQualityReasonCode::OwnershipMismatch)
            || (self.fact_class != MemoryFactClass::Unknown
                && !ownership_fits_fact_class(self.ownership_class, self.fact_class))
    }

    fn is_duplicate_write_attempt(&self) -> bool {
        matches!(self.write_relation, Some(MemoryWriteRelation::Duplicate))
            || self.has_reason(MemoryQualityReasonCode::Duplicate)
            || self.has_reason(MemoryQualityReasonCode::DuplicateExistingMemory)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MemoryRecallVisibilityInput<'a> {
    pub(crate) status: MemoryStatus,
    pub(crate) deleted: bool,
    pub(crate) superseded: bool,
    pub(crate) expires_at_unix: Option<i64>,
    pub(crate) allowed_statuses: &'a [MemoryStatus],
    pub(crate) now_unix: i64,
    pub(crate) repair_ok: bool,
    pub(crate) sensitivity: MemorySensitivity,
    pub(crate) read_policy: &'a MemoryReadPolicy,
    pub(crate) workspace_visible: bool,
    pub(crate) quality: &'a MemoryRecallQualitySignals,
}

pub(crate) fn decide_memory_recall_visibility(
    input: &MemoryRecallVisibilityInput<'_>,
) -> MemoryRecallVisibility {
    if !status_allowed(input.status, input.allowed_statuses) {
        return status_suppression(input.status);
    }
    if input.deleted && !input.allowed_statuses.contains(&MemoryStatus::Deleted) {
        return MemoryRecallVisibility::SuppressDeleted;
    }
    if input.superseded && !input.allowed_statuses.contains(&MemoryStatus::Superseded) {
        return MemoryRecallVisibility::SuppressSuperseded;
    }
    if input
        .expires_at_unix
        .is_some_and(|expires_at| expires_at <= input.now_unix)
        && !input.allowed_statuses.contains(&MemoryStatus::Expired)
    {
        return MemoryRecallVisibility::SuppressExpired;
    }
    if !input.repair_ok {
        return MemoryRecallVisibility::SuppressBackendRepairRequired;
    }
    if !input.read_policy.allows(input.sensitivity) {
        return MemoryRecallVisibility::SuppressSensitivity;
    }
    if !input.workspace_visible {
        return MemoryRecallVisibility::SuppressWorkspaceMismatch;
    }
    if input.quality.is_rejected_related() {
        return MemoryRecallVisibility::SuppressRejectedRelated;
    }
    if input.quality.has_ownership_mismatch() {
        return MemoryRecallVisibility::SuppressOwnershipMismatch;
    }
    if input.quality.is_low_quality_source_context() {
        return MemoryRecallVisibility::SuppressLowQualitySourceContext;
    }
    if input.quality.is_quality_terminal() || input.quality.is_audit_or_reject_ownership() {
        return MemoryRecallVisibility::SuppressQuarantinedOrAuditOnly;
    }

    MemoryRecallVisibility::Visible
}

pub(crate) fn memory_recall_quality_signals_for_row(
    row: &AgentMemoryControlRecord,
    latest_quality_decision: Option<&AgentMemoryQualityDecisionRecord>,
) -> MemoryRecallQualitySignals {
    let (metadata, metadata_valid) = match parse_metadata_json(row.metadata_json.as_deref()) {
        Ok(metadata) => (metadata, true),
        Err(_) => (Default::default(), false),
    };
    let audit = audit_memory_record_quality(&MemoryRecord {
        id: row.id.clone(),
        scope: row.scope.clone(),
        namespace: Some(row.namespace.clone()),
        category: row.category,
        key: row.key.clone(),
        content: row.content_preview.clone().unwrap_or_default(),
        status: row.status,
        confidence: checked_f32(row.confidence),
        importance: checked_f32(row.importance),
        sensitivity: row.sensitivity,
        provenance: MemoryProvenance {
            source_kind: row.source_kind,
            source_thread_id: row.source_thread_id.clone(),
            source_turn_id: row.source_turn_id.clone(),
            source_item_id: row.source_item_id.clone(),
            created_by: actor_to_protocol(row.created_by.clone()),
        },
        source_context_kind: row.source_context_kind,
        created_at: row.created_at_unix,
        updated_at: row.updated_at_unix,
        expires_at: row.expires_at_unix,
        last_accessed_at: row.last_accessed_at_unix,
        access_count: row.access_count.max(0) as u64,
        superseded_by: row.superseded_by.clone(),
        deleted_at: row.deleted_at_unix,
        delete_reason: row.delete_reason.clone(),
        metadata,
    });

    let mut signals = MemoryRecallQualitySignals {
        source_context_kind: audit.source_context_kind,
        evidence_class: audit.evidence_class,
        fact_class: audit.fact_class,
        lifetime_class: audit.lifetime_class,
        ownership_class: audit.ownership_class,
        quality_action: None,
        quality_target_ownership: None,
        quality_reason_codes: Vec::new(),
        write_relation: None,
        metadata_valid,
    };

    if let Some(decision) = latest_quality_decision {
        signals.quality_action = Some(decision.action);
        signals.quality_target_ownership = Some(decision.target_ownership);
        signals.quality_reason_codes = decision.reason_codes.clone();
        signals.write_relation = Some(decision.relation);
        signals.source_context_kind = decision.source_context_kind;
        signals.evidence_class = decision.evidence_class;
        signals.fact_class = decision.fact_class;
        signals.lifetime_class = decision.lifetime_class;
        signals.ownership_class = decision.ownership_class;
    }

    signals
}

pub(crate) fn memory_recall_visibility_input_for_row<'a>(
    row: &AgentMemoryControlRecord,
    allowed_statuses: &'a [MemoryStatus],
    now_unix: i64,
    repair_ok: bool,
    read_policy: &'a MemoryReadPolicy,
    workspace_visible: bool,
    quality: &'a MemoryRecallQualitySignals,
) -> MemoryRecallVisibilityInput<'a> {
    MemoryRecallVisibilityInput {
        status: row.status,
        deleted: row.deleted_at_unix.is_some(),
        superseded: row.superseded_by.is_some(),
        expires_at_unix: row.expires_at_unix,
        allowed_statuses,
        now_unix,
        repair_ok,
        sensitivity: row.sensitivity,
        read_policy,
        workspace_visible,
        quality,
    }
}

fn status_allowed(status: MemoryStatus, allowed_statuses: &[MemoryStatus]) -> bool {
    if allowed_statuses.is_empty() {
        status == MemoryStatus::Active
    } else {
        allowed_statuses.contains(&status)
    }
}

fn status_suppression(status: MemoryStatus) -> MemoryRecallVisibility {
    match status {
        MemoryStatus::Active => MemoryRecallVisibility::Visible,
        MemoryStatus::Deleted => MemoryRecallVisibility::SuppressDeleted,
        MemoryStatus::Superseded => MemoryRecallVisibility::SuppressSuperseded,
        MemoryStatus::Expired => MemoryRecallVisibility::SuppressExpired,
    }
}

fn actor_to_protocol(actor: Option<MemoryActorRecord>) -> Option<MemoryActor> {
    actor.map(|actor| MemoryActor {
        kind: actor.kind,
        id: actor.id,
    })
}

fn checked_f32(value: f64) -> f32 {
    if value.is_finite() {
        value.clamp(f32::MIN as f64, f32::MAX as f64) as f32
    } else {
        0.0
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_READ_POLICY: MemoryReadPolicy = MemoryReadPolicy {
        allow_normal: true,
        allow_personal: true,
        allow_secret_like: false,
        allow_regulated: false,
    };

    fn visibility_input<'a>(
        quality: &'a MemoryRecallQualitySignals,
    ) -> MemoryRecallVisibilityInput<'a> {
        MemoryRecallVisibilityInput {
            status: MemoryStatus::Active,
            deleted: false,
            superseded: false,
            expires_at_unix: None,
            allowed_statuses: &[],
            now_unix: 100,
            repair_ok: true,
            sensitivity: MemorySensitivity::Normal,
            read_policy: &TEST_READ_POLICY,
            workspace_visible: true,
            quality,
        }
    }

    #[test]
    fn recall_visibility_suppresses_terminal_statuses() {
        let quality = MemoryRecallQualitySignals::direct_user_default();
        for (status, expected) in [
            (
                MemoryStatus::Deleted,
                MemoryRecallVisibility::SuppressDeleted,
            ),
            (
                MemoryStatus::Superseded,
                MemoryRecallVisibility::SuppressSuperseded,
            ),
            (
                MemoryStatus::Expired,
                MemoryRecallVisibility::SuppressExpired,
            ),
        ] {
            let mut input = visibility_input(&quality);
            input.status = status;
            assert_eq!(decide_memory_recall_visibility(&input), expected);
        }
    }

    #[test]
    fn recall_visibility_allows_explicit_admin_statuses() {
        let quality = MemoryRecallQualitySignals::direct_user_default();
        let allowed = [MemoryStatus::Deleted];
        let mut input = visibility_input(&quality);
        input.status = MemoryStatus::Deleted;
        input.deleted = true;
        input.allowed_statuses = &allowed;
        assert_eq!(
            decide_memory_recall_visibility(&input),
            MemoryRecallVisibility::Visible
        );
    }

    #[test]
    fn recall_visibility_suppresses_workspace_and_sensitivity() {
        let quality = MemoryRecallQualitySignals::direct_user_default();
        let mut input = visibility_input(&quality);
        input.workspace_visible = false;
        assert_eq!(
            decide_memory_recall_visibility(&input),
            MemoryRecallVisibility::SuppressWorkspaceMismatch
        );

        let mut input = visibility_input(&quality);
        input.sensitivity = MemorySensitivity::SecretLike;
        assert_eq!(
            decide_memory_recall_visibility(&input),
            MemoryRecallVisibility::SuppressSensitivity
        );
    }

    #[test]
    fn recall_visibility_suppresses_quality_terminal_records() {
        let mut quality = MemoryRecallQualitySignals::direct_user_default();
        quality.quality_action = Some(MemoryQualityAction::Quarantine);
        assert_eq!(
            decide_memory_recall_visibility(&visibility_input(&quality)),
            MemoryRecallVisibility::SuppressQuarantinedOrAuditOnly
        );

        let mut quality = MemoryRecallQualitySignals::direct_user_default();
        quality.write_relation = Some(MemoryWriteRelation::SuppressedByRejection);
        assert_eq!(
            decide_memory_recall_visibility(&visibility_input(&quality)),
            MemoryRecallVisibility::SuppressRejectedRelated
        );
    }

    #[test]
    fn recall_visibility_suppresses_low_quality_source_context() {
        let mut quality = MemoryRecallQualitySignals::direct_user_default();
        quality.source_context_kind = MemorySourceContextKind::Unknown;
        quality.evidence_class = MemoryEvidenceClass::MissingOrWeak;
        assert_eq!(
            decide_memory_recall_visibility(&visibility_input(&quality)),
            MemoryRecallVisibility::SuppressLowQualitySourceContext
        );
    }

    #[test]
    fn recall_visibility_suppresses_ownership_mismatch() {
        let mut quality = MemoryRecallQualitySignals::direct_user_default();
        quality.fact_class = MemoryFactClass::UserIdentity;
        quality.ownership_class = MemoryOwnershipClass::DurableWorkspaceMemory;
        assert_eq!(
            decide_memory_recall_visibility(&visibility_input(&quality)),
            MemoryRecallVisibility::SuppressOwnershipMismatch
        );
    }
}
