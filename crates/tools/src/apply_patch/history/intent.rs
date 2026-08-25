use crate::apply_patch::history::{
    AppliedPatchLog, AppliedPatchRecord, AppliedPatchRecordOutcome, CommitOrdinal,
    CommittedPatchChange, DurablePatchChange, InvocationIdentity, PatchRecoveryPlan,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum IntentStatus {
    Pending,
    Promoted,
    AppliedNoChange,
    FailedNoChange,
    Rejected,
    Gap,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PatchCommitIntent {
    pub identity: InvocationIdentity,
    pub commit_ordinal: CommitOrdinal,
    pub plan_fingerprint: [u8; 32],
    pub planned_operation_fingerprints: Vec<[u8; 32]>,
    pub recovery_plan: Option<PatchRecoveryPlan>,
    pub committed_changes: Vec<CommittedPatchChange>,
    pub status: IntentStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IntentError {
    ConflictingDuplicate,
    UnknownIntent,
    Closed,
    Poisoned,
}

impl std::fmt::Display for IntentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ConflictingDuplicate => "commit intent was reused for a different plan",
            Self::UnknownIntent => "commit intent does not exist",
            Self::Closed => "commit intent is already terminal",
            Self::Poisoned => "commit intent journal lock is poisoned",
        })
    }
}

impl std::error::Error for IntentError {}

#[derive(Clone, Debug, Default)]
pub struct CommitIntentJournal {
    state: Arc<Mutex<HashMap<(String, String, String), PatchCommitIntent>>>,
}

impl CommitIntentJournal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin(
        &self,
        identity: InvocationIdentity,
        commit_ordinal: CommitOrdinal,
        plan_fingerprint: [u8; 32],
        planned_operation_fingerprints: Vec<[u8; 32]>,
        recovery_plan: Option<PatchRecoveryPlan>,
    ) -> Result<PatchCommitIntent, IntentError> {
        let mut state = self.state.lock().map_err(|_| IntentError::Poisoned)?;
        let key = identity_key(&identity);
        if let Some(existing) = state.get(&key) {
            if existing.plan_fingerprint == plan_fingerprint
                && existing.planned_operation_fingerprints == planned_operation_fingerprints
                && existing.recovery_plan == recovery_plan
            {
                return Ok(existing.clone());
            }
            return Err(IntentError::ConflictingDuplicate);
        }
        let intent = PatchCommitIntent {
            identity,
            commit_ordinal,
            plan_fingerprint,
            planned_operation_fingerprints,
            recovery_plan,
            committed_changes: Vec::new(),
            status: IntentStatus::Pending,
        };
        state.insert(key, intent.clone());
        Ok(intent)
    }

    pub fn append_change(
        &self,
        identity: &InvocationIdentity,
        commit_ordinal: CommitOrdinal,
        change: CommittedPatchChange,
    ) -> Result<PatchCommitIntent, IntentError> {
        let mut state = self.state.lock().map_err(|_| IntentError::Poisoned)?;
        let intent = state
            .get_mut(&identity_key(identity))
            .ok_or(IntentError::UnknownIntent)?;
        if intent.commit_ordinal != commit_ordinal {
            return Err(IntentError::ConflictingDuplicate);
        }
        if intent.status != IntentStatus::Pending {
            return Err(IntentError::Closed);
        }
        if let Some(existing) = intent.committed_changes.get(change.sequence as usize) {
            if existing == &change {
                return Ok(intent.clone());
            }
            return Err(IntentError::ConflictingDuplicate);
        }
        let expected_sequence = intent.committed_changes.len() as u32;
        if change.sequence != expected_sequence {
            return Err(IntentError::ConflictingDuplicate);
        }
        let mut change = change;
        change.sequence = intent.committed_changes.len() as u32;
        change.commit_step = u16::try_from(change.sequence).unwrap_or(u16::MAX);
        intent.committed_changes.push(change);
        Ok(intent.clone())
    }

    pub fn mark_gap(
        &self,
        identity: &InvocationIdentity,
        commit_ordinal: CommitOrdinal,
        _reason: impl Into<String>,
    ) -> Result<PatchCommitIntent, IntentError> {
        let mut state = self.state.lock().map_err(|_| IntentError::Poisoned)?;
        let intent = state
            .get_mut(&identity_key(identity))
            .ok_or(IntentError::UnknownIntent)?;
        if intent.commit_ordinal != commit_ordinal {
            return Err(IntentError::ConflictingDuplicate);
        }
        if !matches!(intent.status, IntentStatus::Pending) {
            return Ok(intent.clone());
        }
        intent.status = IntentStatus::Gap;
        Ok(intent.clone())
    }

    pub fn promote(
        &self,
        identity: &InvocationIdentity,
        commit_ordinal: CommitOrdinal,
        outcome: AppliedPatchRecordOutcome,
        log: &AppliedPatchLog,
    ) -> Result<AppliedPatchRecord, IntentError> {
        self.promote_with_side_effects(
            identity,
            commit_ordinal,
            outcome,
            crate::apply_patch::history::PatchSideEffects::default(),
            log,
        )
    }

    pub fn promote_with_side_effects(
        &self,
        identity: &InvocationIdentity,
        commit_ordinal: CommitOrdinal,
        outcome: AppliedPatchRecordOutcome,
        side_effects: crate::apply_patch::history::PatchSideEffects,
        log: &AppliedPatchLog,
    ) -> Result<AppliedPatchRecord, IntentError> {
        let mut state = self.state.lock().map_err(|_| IntentError::Poisoned)?;
        let intent = state
            .get_mut(&identity_key(identity))
            .ok_or(IntentError::UnknownIntent)?;
        if intent.commit_ordinal != commit_ordinal {
            return Err(IntentError::ConflictingDuplicate);
        }
        if intent.status == IntentStatus::Promoted {
            return Ok(log
                .get(identity)
                .map_err(|_| IntentError::Poisoned)?
                .map(|stored| stored.record)
                .ok_or(IntentError::UnknownIntent)?);
        }
        let mut record = AppliedPatchRecord::new(
            intent.identity.clone(),
            intent.commit_ordinal,
            outcome,
            intent
                .committed_changes
                .iter()
                .map(DurablePatchChange::from)
                .collect(),
        );
        record.side_effects = side_effects;
        if !record.side_effects.exact {
            record.exactness = crate::apply_patch::history::PatchRecordExactness::Uncertain;
        }
        match log.insert(record.clone(), intent.plan_fingerprint) {
            Ok(_) => {
                intent.status = IntentStatus::Promoted;
                Ok(record)
            }
            Err(_) => Err(IntentError::ConflictingDuplicate),
        }
    }

    pub fn reject(
        &self,
        identity: &InvocationIdentity,
        commit_ordinal: CommitOrdinal,
    ) -> Result<(), IntentError> {
        let mut state = self.state.lock().map_err(|_| IntentError::Poisoned)?;
        let intent = state
            .get_mut(&identity_key(identity))
            .ok_or(IntentError::UnknownIntent)?;
        if intent.commit_ordinal != commit_ordinal {
            return Err(IntentError::ConflictingDuplicate);
        }
        if matches!(intent.status, IntentStatus::Pending) {
            intent.status = IntentStatus::Rejected;
        }
        Ok(())
    }

    pub fn mark_applied_no_change(
        &self,
        identity: &InvocationIdentity,
        commit_ordinal: CommitOrdinal,
    ) -> Result<(), IntentError> {
        let mut state = self.state.lock().map_err(|_| IntentError::Poisoned)?;
        let intent = state
            .get_mut(&identity_key(identity))
            .ok_or(IntentError::UnknownIntent)?;
        if intent.commit_ordinal != commit_ordinal {
            return Err(IntentError::ConflictingDuplicate);
        }
        if matches!(intent.status, IntentStatus::Pending) {
            intent.status = IntentStatus::AppliedNoChange;
        }
        Ok(())
    }

    /// Preserve a pre-commit failure for exactly-once replay without creating
    /// an AppliedPatchRecord. Applied records represent committed effects (or
    /// an explicit filesystem uncertainty), so an empty failed execution is a
    /// terminal intent marker only.
    pub fn mark_failed_no_change(
        &self,
        identity: &InvocationIdentity,
        commit_ordinal: CommitOrdinal,
    ) -> Result<(), IntentError> {
        let mut state = self.state.lock().map_err(|_| IntentError::Poisoned)?;
        let intent = state
            .get_mut(&identity_key(identity))
            .ok_or(IntentError::UnknownIntent)?;
        if intent.commit_ordinal != commit_ordinal {
            return Err(IntentError::ConflictingDuplicate);
        }
        if matches!(intent.status, IntentStatus::Pending) {
            intent.status = IntentStatus::FailedNoChange;
        }
        Ok(())
    }

    pub fn get(
        &self,
        identity: &InvocationIdentity,
    ) -> Result<Option<PatchCommitIntent>, IntentError> {
        let state = self.state.lock().map_err(|_| IntentError::Poisoned)?;
        Ok(state.get(&identity_key(identity)).cloned())
    }

    pub fn pending(&self) -> Result<Vec<PatchCommitIntent>, IntentError> {
        let state = self.state.lock().map_err(|_| IntentError::Poisoned)?;
        Ok(state
            .values()
            .filter(|intent| matches!(intent.status, IntentStatus::Pending))
            .cloned()
            .collect())
    }
}

fn identity_key(identity: &InvocationIdentity) -> (String, String, String) {
    (
        identity.thread_id.clone(),
        identity.turn_id.clone(),
        identity.invocation_id.clone(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::history::{AppliedPatchRecordOutcome, ChangeKind, PatchSideEffects};

    fn identity() -> InvocationIdentity {
        InvocationIdentity::new("thread", "turn", "call").unwrap()
    }

    fn change() -> CommittedPatchChange {
        CommittedPatchChange {
            operation_index: 0,
            commit_step: 0,
            sequence: 0,
            kind: ChangeKind::Update,
            source_path: "a.txt".to_owned(),
            destination_path: None,
            before: None,
            after: None,
            overwritten_destination: None,
            side_effects: PatchSideEffects::default(),
        }
    }

    #[test]
    fn intent_progress_and_promotion_are_idempotent() {
        let journal = CommitIntentJournal::new();
        let log = AppliedPatchLog::new();
        let id = identity();
        journal
            .begin(id.clone(), CommitOrdinal(0), [1; 32], vec![], None)
            .unwrap();
        journal
            .append_change(&id, CommitOrdinal(0), change())
            .unwrap();
        let record = journal
            .promote(
                &id,
                CommitOrdinal(0),
                AppliedPatchRecordOutcome::Applied,
                &log,
            )
            .unwrap();
        let replay = journal
            .promote(
                &id,
                CommitOrdinal(0),
                AppliedPatchRecordOutcome::Applied,
                &log,
            )
            .unwrap();
        assert_eq!(record, replay);
        assert_eq!(log.len().unwrap(), 1);
    }

    #[test]
    fn unresolved_gap_does_not_invent_a_file_change() {
        let journal = CommitIntentJournal::new();
        let id = identity();
        journal
            .begin(id.clone(), CommitOrdinal(0), [1; 32], vec![], None)
            .unwrap();

        let gap = journal
            .mark_gap(&id, CommitOrdinal(0), "crash boundary")
            .unwrap();

        assert_eq!(gap.status, IntentStatus::Gap);
        assert!(gap.committed_changes.is_empty());
    }

    #[test]
    fn pending_intent_is_explicitly_recoverable() {
        let journal = CommitIntentJournal::new();
        let id = identity();
        journal
            .begin(id, CommitOrdinal(0), [1; 32], vec![[2; 32]], None)
            .unwrap();
        assert_eq!(journal.pending().unwrap().len(), 1);
    }
}
