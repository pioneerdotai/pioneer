use crate::apply_patch::history::{
    AppliedPatchLog, AppliedPatchRecordOutcome, CommitIntentJournal, ContentAddressedSnapshotRef,
    ContentAddressedSnapshotStore, PatchCommitIntent, SnapshotDomain, TurnDiffProjectionStore,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryResolution {
    Promote(AppliedPatchRecordOutcome),
    Gap(String),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecoveryReport {
    pub inspected: u64,
    pub promoted: u64,
    pub gaps: u64,
    pub errors: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RetentionReport {
    pub records_deleted: u64,
    pub projections_deleted: u64,
    pub snapshots_released: u64,
    pub snapshots_collected: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetentionError {
    Store(String),
    Snapshot(String),
    Intent(String),
    Poisoned,
}

impl std::fmt::Display for RetentionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(message) => write!(f, "retention record store failed: {message}"),
            Self::Snapshot(message) => write!(f, "retention snapshot store failed: {message}"),
            Self::Intent(message) => write!(f, "retention intent journal failed: {message}"),
            Self::Poisoned => f.write_str("retention reference registry is poisoned"),
        }
    }
}

impl std::error::Error for RetentionError {}

#[derive(Clone, Debug)]
pub struct HistoryRetention {
    pub records: AppliedPatchLog,
    pub intents: CommitIntentJournal,
    pub projections: TurnDiffProjectionStore,
    pub snapshots: ContentAddressedSnapshotStore,
    references: Arc<Mutex<HashMap<String, Vec<(SnapshotDomain, ContentAddressedSnapshotRef)>>>>,
}

impl Default for HistoryRetention {
    fn default() -> Self {
        Self {
            records: AppliedPatchLog::new(),
            intents: CommitIntentJournal::new(),
            projections: TurnDiffProjectionStore::new(),
            snapshots: ContentAddressedSnapshotStore::new(Default::default()),
            references: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl HistoryRetention {
    pub fn register_reference(
        &self,
        thread_id: &str,
        domain: SnapshotDomain,
        reference: ContentAddressedSnapshotRef,
    ) -> Result<(), RetentionError> {
        let mut references = self
            .references
            .lock()
            .map_err(|_| RetentionError::Poisoned)?;
        references
            .entry(thread_id.to_owned())
            .or_default()
            .push((domain, reference));
        Ok(())
    }

    pub fn delete_thread(&self, thread_id: &str) -> Result<RetentionReport, RetentionError> {
        let removed = self
            .records
            .delete_thread(thread_id)
            .map_err(|error| RetentionError::Store(error.to_string()))?;
        let projections_deleted = self
            .projections
            .remove_thread(thread_id)
            .map_err(|error| RetentionError::Store(error.to_string()))?;
        let refs = self
            .references
            .lock()
            .map_err(|_| RetentionError::Poisoned)?
            .remove(thread_id)
            .unwrap_or_default();
        let mut report = RetentionReport {
            records_deleted: removed.len() as u64,
            projections_deleted: projections_deleted as u64,
            ..Default::default()
        };
        for (_, reference) in refs {
            if self
                .snapshots
                .release(&reference)
                .map_err(|error| RetentionError::Snapshot(error.to_string()))?
            {
                report.snapshots_collected = report.snapshots_collected.saturating_add(1);
            }
            report.snapshots_released = report.snapshots_released.saturating_add(1);
        }
        Ok(report)
    }

    pub fn reconcile_pending<F>(&self, mut resolve: F) -> Result<RecoveryReport, RetentionError>
    where
        F: FnMut(&PatchCommitIntent) -> RecoveryResolution,
    {
        let pending = self
            .intents
            .pending()
            .map_err(|error| RetentionError::Intent(error.to_string()))?;
        let mut report = RecoveryReport {
            inspected: pending.len() as u64,
            ..Default::default()
        };
        for intent in pending {
            match resolve(&intent) {
                RecoveryResolution::Promote(outcome) => {
                    if self
                        .intents
                        .promote(
                            &intent.identity,
                            intent.commit_ordinal,
                            outcome,
                            &self.records,
                        )
                        .is_ok()
                    {
                        report.promoted = report.promoted.saturating_add(1);
                    } else {
                        report.errors = report.errors.saturating_add(1);
                    }
                }
                RecoveryResolution::Gap(reason) => {
                    if self
                        .intents
                        .mark_gap(&intent.identity, intent.commit_ordinal, reason)
                        .is_ok()
                    {
                        report.gaps = report.gaps.saturating_add(1);
                    } else {
                        report.errors = report.errors.saturating_add(1);
                    }
                }
            }
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::history::{
        CommittedTextSnapshot, LineEnding, LineEndingMetadata, TextEncoding,
    };

    #[test]
    fn deleting_one_thread_does_not_collect_another_threads_reference() {
        let retention = HistoryRetention::default();
        let domain = SnapshotDomain::new("private", "none", "thread");
        let snapshot = CommittedTextSnapshot::from_bytes(
            b"shared".to_vec(),
            TextEncoding::Utf8,
            LineEndingMetadata {
                dominant: LineEnding::None,
                mixed: false,
                final_newline: false,
            },
        );
        let first = retention.snapshots.put(&domain, &snapshot).unwrap();
        let second = retention.snapshots.put(&domain, &snapshot).unwrap();
        retention
            .register_reference("thread-a", domain.clone(), first)
            .unwrap();
        retention
            .register_reference("thread-b", domain, second)
            .unwrap();
        let report = retention.delete_thread("thread-a").unwrap();
        assert_eq!(report.snapshots_collected, 0);
        assert_eq!(retention.snapshots.metrics().unwrap().blobs, 1);
    }

    #[test]
    fn unresolved_intent_becomes_explicit_gap_without_mutating_workspace() {
        let retention = HistoryRetention::default();
        let identity =
            crate::apply_patch::history::InvocationIdentity::new("thread", "turn", "call").unwrap();
        retention
            .intents
            .begin(
                identity,
                crate::apply_patch::history::CommitOrdinal(0),
                [1; 32],
                vec![],
                None,
            )
            .unwrap();
        let report = retention
            .reconcile_pending(|_| RecoveryResolution::Gap("crash boundary".to_owned()))
            .unwrap();
        assert_eq!(report.gaps, 1);
        assert!(retention.intents.pending().unwrap().is_empty());
    }
}
