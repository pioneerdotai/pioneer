use pioneer_crud::{AgentMemoryQuarantineRecord, AgentMemoryRepairJobRecord};
use pioneer_protocol::{
    MemoryLifecycleActor, MemoryLifecycleActorKind, MemoryLifecycleReasonCode,
    MemoryLifecycleTransitionKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryLifecycleSubjectKind {
    Memory,
    Candidate,
    RepairJob,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryLifecycleTransition {
    pub subject_kind: MemoryLifecycleSubjectKind,
    pub kind: MemoryLifecycleTransitionKind,
    pub reason: MemoryLifecycleReasonCode,
    pub actor: MemoryLifecycleActor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryQuarantineRequest {
    pub memory_id: String,
    pub reason_code: MemoryLifecycleReasonCode,
    pub actor: Option<MemoryLifecycleActor>,
    pub details_json: Option<String>,
    pub schedule_backend_cleanup: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRestoreRequest {
    pub memory_id: String,
    pub actor: Option<MemoryLifecycleActor>,
    pub schedule_backend_reindex: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryQuarantineResponse {
    pub quarantine: AgentMemoryQuarantineRecord,
    pub repair_job: Option<AgentMemoryRepairJobRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRestoreResponse {
    pub quarantine: Option<AgentMemoryQuarantineRecord>,
    pub repair_job: Option<AgentMemoryRepairJobRecord>,
}

impl MemoryLifecycleTransition {
    pub fn new(
        subject_kind: MemoryLifecycleSubjectKind,
        kind: MemoryLifecycleTransitionKind,
        reason: MemoryLifecycleReasonCode,
        actor: MemoryLifecycleActor,
    ) -> Result<Self, MemoryLifecycleInvariantError> {
        let transition = Self {
            subject_kind,
            kind,
            reason,
            actor,
        };
        validate_lifecycle_transition(&transition)?;
        Ok(transition)
    }

    pub fn service(
        subject_kind: MemoryLifecycleSubjectKind,
        kind: MemoryLifecycleTransitionKind,
        reason: MemoryLifecycleReasonCode,
    ) -> Result<Self, MemoryLifecycleInvariantError> {
        Self::new(
            subject_kind,
            kind,
            reason,
            MemoryLifecycleActor {
                kind: MemoryLifecycleActorKind::Service,
                id: None,
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryLifecycleInvariantError {
    MemoryTransitionMustTargetMemory(MemoryLifecycleTransitionKind),
    CandidateTransitionMustTargetCandidate(MemoryLifecycleTransitionKind),
    RepairTransitionMustTargetRepairJob(MemoryLifecycleTransitionKind),
    QuarantineMustUseQuarantineReason(MemoryLifecycleReasonCode),
    RestoreMustUseExplicitRestoreReason(MemoryLifecycleReasonCode),
}

pub fn validate_lifecycle_transition(
    transition: &MemoryLifecycleTransition,
) -> Result<(), MemoryLifecycleInvariantError> {
    match transition.kind {
        MemoryLifecycleTransitionKind::Quarantined | MemoryLifecycleTransitionKind::Restored => {
            if transition.subject_kind != MemoryLifecycleSubjectKind::Memory {
                return Err(
                    MemoryLifecycleInvariantError::MemoryTransitionMustTargetMemory(
                        transition.kind,
                    ),
                );
            }
        }
        MemoryLifecycleTransitionKind::Rejected | MemoryLifecycleTransitionKind::Approved => {
            if transition.subject_kind != MemoryLifecycleSubjectKind::Candidate {
                return Err(
                    MemoryLifecycleInvariantError::CandidateTransitionMustTargetCandidate(
                        transition.kind,
                    ),
                );
            }
        }
        MemoryLifecycleTransitionKind::RepairScheduled
        | MemoryLifecycleTransitionKind::RepairCompleted
        | MemoryLifecycleTransitionKind::RepairFailed => {
            if transition.subject_kind != MemoryLifecycleSubjectKind::RepairJob {
                return Err(
                    MemoryLifecycleInvariantError::RepairTransitionMustTargetRepairJob(
                        transition.kind,
                    ),
                );
            }
        }
    }

    if transition.kind == MemoryLifecycleTransitionKind::Quarantined
        && !is_quarantine_reason(transition.reason)
    {
        return Err(
            MemoryLifecycleInvariantError::QuarantineMustUseQuarantineReason(transition.reason),
        );
    }

    if transition.kind == MemoryLifecycleTransitionKind::Restored
        && transition.reason != MemoryLifecycleReasonCode::ExplicitRestore
    {
        return Err(
            MemoryLifecycleInvariantError::RestoreMustUseExplicitRestoreReason(transition.reason),
        );
    }

    Ok(())
}

pub fn memory_transition_removes_product_visibility(
    transition: MemoryLifecycleTransitionKind,
) -> bool {
    transition == MemoryLifecycleTransitionKind::Quarantined
}

pub fn memory_transition_restores_product_eligibility(
    transition: MemoryLifecycleTransitionKind,
) -> bool {
    transition == MemoryLifecycleTransitionKind::Restored
}

pub fn candidate_transition_is_candidate_scoped(transition: MemoryLifecycleTransitionKind) -> bool {
    matches!(
        transition,
        MemoryLifecycleTransitionKind::Rejected | MemoryLifecycleTransitionKind::Approved
    )
}

fn is_quarantine_reason(reason: MemoryLifecycleReasonCode) -> bool {
    matches!(
        reason,
        MemoryLifecycleReasonCode::VisibilityRankingSuppression
            | MemoryLifecycleReasonCode::QualityTerminalDecision
            | MemoryLifecycleReasonCode::BackendPayloadStaleOrMissing
            | MemoryLifecycleReasonCode::MemvidStaleVector
            | MemoryLifecycleReasonCode::ManualDeveloperAdminQuarantine
            | MemoryLifecycleReasonCode::RepairReconciliation
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quarantine_is_memory_scoped_and_removes_visibility() {
        let transition = MemoryLifecycleTransition::service(
            MemoryLifecycleSubjectKind::Memory,
            MemoryLifecycleTransitionKind::Quarantined,
            MemoryLifecycleReasonCode::ManualDeveloperAdminQuarantine,
        )
        .expect("valid quarantine transition");

        assert!(memory_transition_removes_product_visibility(
            transition.kind
        ));
        assert!(!memory_transition_restores_product_eligibility(
            transition.kind
        ));
    }

    #[test]
    fn restore_requires_explicit_restore_reason() {
        let error = MemoryLifecycleTransition::service(
            MemoryLifecycleSubjectKind::Memory,
            MemoryLifecycleTransitionKind::Restored,
            MemoryLifecycleReasonCode::RepairReconciliation,
        )
        .expect_err("restore must be explicit");

        assert_eq!(
            error,
            MemoryLifecycleInvariantError::RestoreMustUseExplicitRestoreReason(
                MemoryLifecycleReasonCode::RepairReconciliation
            )
        );
    }

    #[test]
    fn duplicate_candidate_rejection_is_candidate_scoped() {
        let transition = MemoryLifecycleTransition::service(
            MemoryLifecycleSubjectKind::Candidate,
            MemoryLifecycleTransitionKind::Rejected,
            MemoryLifecycleReasonCode::CandidateRejected,
        )
        .expect("valid candidate rejection");

        assert!(candidate_transition_is_candidate_scoped(transition.kind));
        assert!(!memory_transition_removes_product_visibility(
            transition.kind
        ));
    }

    #[test]
    fn repair_transitions_do_not_restore_memory_visibility() {
        let transition = MemoryLifecycleTransition::service(
            MemoryLifecycleSubjectKind::RepairJob,
            MemoryLifecycleTransitionKind::RepairCompleted,
            MemoryLifecycleReasonCode::RepairReconciliation,
        )
        .expect("valid repair transition");

        assert!(!memory_transition_removes_product_visibility(
            transition.kind
        ));
        assert!(!memory_transition_restores_product_eligibility(
            transition.kind
        ));
    }
}
