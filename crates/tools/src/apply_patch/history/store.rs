use crate::apply_patch::history::{
    AppliedPatchRecord, AppliedPatchRecordOutcome, CommitOrdinal, InvocationIdentity,
    PatchRecordExactness, TextSnapshotRef,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

// Durable records are produced by bounded patch execution, but they are also
// decoded from persistent storage during replay.  Keep the storage boundary
// bounded independently of the in-memory collection implementation so a
// malformed/corrupt record cannot turn recovery into an unbounded allocation.
const MAX_PERSISTED_RECORD_CHANGES: usize = 256;
const MAX_PERSISTED_ID_BYTES: usize = 4096;
const MAX_PERSISTED_ENVIRONMENT_BYTES: usize = 4096;
const MAX_PERSISTED_PATH_BYTES: usize = 4096;
const MAX_PERSISTED_SIDE_EFFECT_ENTRIES: usize = 1024;
const MAX_PERSISTED_SIDE_EFFECT_STRING_BYTES: usize = 4096;
const MAX_PERSISTED_SNAPSHOT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_PERSISTED_TOTAL_SNAPSHOT_BYTES: u64 = 256 * 1024 * 1024;
const MAX_PERSISTED_GAP_REASON_BYTES: usize = 4096;
/// Serialized records are decoded from SQLite before their typed bounds can
/// be checked. Keep that decode itself bounded so a corrupt row cannot cause
/// an attacker-controlled allocation during replay/startup.
pub(crate) const MAX_PERSISTED_RECORD_JSON_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredPatchRecord {
    pub record: AppliedPatchRecord,
    pub plan_fingerprint: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InsertedPatchRecord {
    Inserted(StoredPatchRecord),
    Existing(StoredPatchRecord),
}

impl InsertedPatchRecord {
    pub fn record(&self) -> &StoredPatchRecord {
        match self {
            Self::Inserted(record) | Self::Existing(record) => record,
        }
    }

    pub fn was_inserted(&self) -> bool {
        matches!(self, Self::Inserted(_))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecordStoreError {
    InvalidRecord(String),
    ConflictingDuplicate {
        identity: InvocationIdentity,
    },
    ConflictingOrdinal {
        thread_id: String,
        turn_id: String,
        ordinal: CommitOrdinal,
    },
    Poisoned,
}

impl std::fmt::Display for RecordStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRecord(message) => f.write_str(message),
            Self::ConflictingDuplicate { identity } => write!(
                f,
                "patch invocation {}:{}:{} already has a different immutable record",
                identity.thread_id, identity.turn_id, identity.invocation_id
            ),
            Self::ConflictingOrdinal {
                thread_id,
                turn_id,
                ordinal,
            } => write!(
                f,
                "commit ordinal {} is already occupied in turn {}:{}",
                ordinal.0, thread_id, turn_id
            ),
            Self::Poisoned => f.write_str("patch history store lock is poisoned"),
        }
    }
}

impl std::error::Error for RecordStoreError {}

#[derive(Clone, Debug, Default)]
pub struct AppliedPatchLog {
    state: Arc<Mutex<LogState>>,
}

#[derive(Debug, Default)]
struct LogState {
    by_identity: HashMap<(String, String, String), StoredPatchRecord>,
    by_turn_ordinal: HashMap<(String, String, u64), (String, String, String)>,
    next_by_turn: HashMap<(String, String), u64>,
    records: BTreeMap<(String, String, u64), StoredPatchRecord>,
}

impl AppliedPatchLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocates a stable ordinal for a turn. The allocation is monotonic and
    /// remains inside the same mutex as insertion, so concurrent invocations
    /// cannot observe or reuse an ordinal that is not committed.
    pub fn allocate_ordinal(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<CommitOrdinal, RecordStoreError> {
        if thread_id.trim().is_empty() || turn_id.trim().is_empty() {
            return Err(RecordStoreError::InvalidRecord(
                "thread and turn ids are required".to_owned(),
            ));
        }
        let mut state = self.state.lock().map_err(|_| RecordStoreError::Poisoned)?;
        let key = (thread_id.to_owned(), turn_id.to_owned());
        let ordinal = state.next_by_turn.entry(key).or_insert(0);
        let result = CommitOrdinal(*ordinal);
        let next = (*ordinal).checked_add(1).ok_or_else(|| {
            RecordStoreError::InvalidRecord(
                "patch commit ordinal space is exhausted for this turn".to_owned(),
            )
        })?;
        *ordinal = next;
        Ok(result)
    }

    pub fn insert(
        &self,
        record: AppliedPatchRecord,
        plan_fingerprint: [u8; 32],
    ) -> Result<InsertedPatchRecord, RecordStoreError> {
        validate_record(&record)?;
        let identity_key = record.identity.uniqueness_key();
        let identity_key = (
            identity_key.0.to_owned(),
            identity_key.1.to_owned(),
            identity_key.2.to_owned(),
        );
        let turn_key = (
            record.identity.thread_id.clone(),
            record.identity.turn_id.clone(),
            record.commit_ordinal.0,
        );
        let mut state = self.state.lock().map_err(|_| RecordStoreError::Poisoned)?;
        if let Some(existing) = state.by_identity.get(&identity_key) {
            if existing.plan_fingerprint == plan_fingerprint && existing.record == record {
                return Ok(InsertedPatchRecord::Existing(existing.clone()));
            }
            return Err(RecordStoreError::ConflictingDuplicate {
                identity: record.identity,
            });
        }
        if let Some(existing_identity) = state.by_turn_ordinal.get(&turn_key)
            && existing_identity != &identity_key
        {
            return Err(RecordStoreError::ConflictingOrdinal {
                thread_id: turn_key.0,
                turn_id: turn_key.1,
                ordinal: record.commit_ordinal,
            });
        }
        let stored = StoredPatchRecord {
            record,
            plan_fingerprint,
        };
        state
            .by_turn_ordinal
            .insert(turn_key.clone(), identity_key.clone());
        state
            .next_by_turn
            .entry((turn_key.0.clone(), turn_key.1.clone()))
            .and_modify(|next| *next = (*next).max(turn_key.2.saturating_add(1)))
            .or_insert(turn_key.2.saturating_add(1));
        state.by_identity.insert(identity_key, stored.clone());
        state.records.insert(turn_key, stored.clone());
        Ok(InsertedPatchRecord::Inserted(stored))
    }

    pub fn get(
        &self,
        identity: &InvocationIdentity,
    ) -> Result<Option<StoredPatchRecord>, RecordStoreError> {
        let state = self.state.lock().map_err(|_| RecordStoreError::Poisoned)?;
        let key = (
            identity.thread_id.clone(),
            identity.turn_id.clone(),
            identity.invocation_id.clone(),
        );
        Ok(state.by_identity.get(&key).cloned())
    }

    pub fn records_for_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Vec<StoredPatchRecord>, RecordStoreError> {
        let state = self.state.lock().map_err(|_| RecordStoreError::Poisoned)?;
        Ok(state
            .records
            .range(
                (thread_id.to_owned(), turn_id.to_owned(), 0)
                    ..=(thread_id.to_owned(), turn_id.to_owned(), u64::MAX),
            )
            .map(|(_, record)| record.clone())
            .collect())
    }

    pub fn records_for_thread(
        &self,
        thread_id: &str,
    ) -> Result<Vec<StoredPatchRecord>, RecordStoreError> {
        let state = self.state.lock().map_err(|_| RecordStoreError::Poisoned)?;
        Ok(state
            .records
            .iter()
            .filter(|((thread, _, _), _)| thread == thread_id)
            .map(|(_, record)| record.clone())
            .collect())
    }

    pub fn len(&self) -> Result<usize, RecordStoreError> {
        let state = self.state.lock().map_err(|_| RecordStoreError::Poisoned)?;
        Ok(state.records.len())
    }

    pub fn is_empty(&self) -> Result<bool, RecordStoreError> {
        Ok(self.len()? == 0)
    }

    pub fn delete_thread(
        &self,
        thread_id: &str,
    ) -> Result<Vec<StoredPatchRecord>, RecordStoreError> {
        let mut state = self.state.lock().map_err(|_| RecordStoreError::Poisoned)?;
        let keys = state
            .records
            .keys()
            .filter(|(thread, _, _)| thread == thread_id)
            .cloned()
            .collect::<Vec<_>>();
        let mut removed = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(record) = state.records.remove(&key) {
                let identity_key = record.record.identity.uniqueness_key();
                state.by_identity.remove(&(
                    identity_key.0.to_owned(),
                    identity_key.1.to_owned(),
                    identity_key.2.to_owned(),
                ));
                state.by_turn_ordinal.remove(&key);
                removed.push(record);
            }
        }
        state
            .next_by_turn
            .retain(|(thread, _), _| thread != thread_id);
        Ok(removed)
    }
}

pub(crate) fn validate_record(record: &AppliedPatchRecord) -> Result<(), RecordStoreError> {
    if record.schema_version != crate::apply_patch::history::APPLIED_PATCH_RECORD_SCHEMA_VERSION {
        return Err(RecordStoreError::InvalidRecord(format!(
            "unsupported applied patch record schema version {}",
            record.schema_version
        )));
    }
    if record.identity.thread_id.trim().is_empty()
        || record.identity.turn_id.trim().is_empty()
        || record.identity.invocation_id.trim().is_empty()
        || record.identity.thread_id.len() > MAX_PERSISTED_ID_BYTES
        || record.identity.turn_id.len() > MAX_PERSISTED_ID_BYTES
        || record.identity.invocation_id.len() > MAX_PERSISTED_ID_BYTES
    {
        return Err(RecordStoreError::InvalidRecord(
            "record identity components must be non-empty and bounded".to_owned(),
        ));
    }
    if record.environment_id.len() > MAX_PERSISTED_ENVIRONMENT_BYTES {
        return Err(RecordStoreError::InvalidRecord(
            "record environment id exceeds the persisted bound".to_owned(),
        ));
    }
    if let AppliedPatchRecordOutcome::Gap { reason } = &record.outcome {
        if reason.len() > MAX_PERSISTED_GAP_REASON_BYTES {
            return Err(RecordStoreError::InvalidRecord(
                "record gap reason exceeds the persisted bound".to_owned(),
            ));
        }
    }
    let mut expected_exactness = match &record.outcome {
        AppliedPatchRecordOutcome::Applied => PatchRecordExactness::Exact,
        AppliedPatchRecordOutcome::Partial { .. } => PatchRecordExactness::Partial,
        AppliedPatchRecordOutcome::CommitStateUncertain | AppliedPatchRecordOutcome::Gap { .. } => {
            PatchRecordExactness::Uncertain
        }
    };
    if !record.side_effects.exact {
        expected_exactness = PatchRecordExactness::Uncertain;
    }
    if record.exactness != expected_exactness {
        return Err(RecordStoreError::InvalidRecord(
            "record exactness does not match its outcome".to_owned(),
        ));
    }
    if record.changes.len() > MAX_PERSISTED_RECORD_CHANGES {
        return Err(RecordStoreError::InvalidRecord(
            "record contains more changes than the persisted bound".to_owned(),
        ));
    }
    validate_side_effects(&record.side_effects)?;
    let mut previous_operation_index = None;
    let mut total_snapshot_bytes = 0u64;
    for (index, change) in record.changes.iter().enumerate() {
        let expected_sequence = u32::try_from(index).map_err(|_| {
            RecordStoreError::InvalidRecord("record contains too many changes".to_owned())
        })?;
        let expected_commit_step = u16::try_from(index).map_err(|_| {
            RecordStoreError::InvalidRecord("record contains too many changes".to_owned())
        })?;
        let operation_order_regressed =
            previous_operation_index.is_some_and(|previous| change.operation_index < previous);
        previous_operation_index = Some(change.operation_index);
        if operation_order_regressed
            || change.sequence != expected_sequence
            || change.commit_step != expected_commit_step
            || change.source_path.trim().is_empty()
            || change.source_path.len() > MAX_PERSISTED_PATH_BYTES
            || change
                .destination_path
                .as_deref()
                .is_some_and(|path| path.trim().is_empty() || path.len() > MAX_PERSISTED_PATH_BYTES)
        {
            return Err(RecordStoreError::InvalidRecord(
                "record changes must be ordered by operation/sequence/commit step and have non-empty paths"
                .to_owned(),
            ));
        }
        if (change.kind == crate::apply_patch::history::ChangeKind::Move)
            != change.destination_path.is_some()
        {
            return Err(RecordStoreError::InvalidRecord(
                "move changes must have a destination and non-move changes must not have one"
                    .to_owned(),
            ));
        }
        for (snapshot, field) in [
            (change.before.as_ref(), "before"),
            (change.after.as_ref(), "after"),
            (
                change.overwritten_destination.as_ref(),
                "overwritten destination",
            ),
        ] {
            if let Some(snapshot) = snapshot {
                validate_snapshot_ref(Some(snapshot), field)?;
                total_snapshot_bytes = total_snapshot_bytes
                    .checked_add(snapshot.byte_len)
                    .ok_or_else(|| {
                        RecordStoreError::InvalidRecord(
                            "record snapshot byte count overflow".to_owned(),
                        )
                    })?;
                if total_snapshot_bytes > MAX_PERSISTED_TOTAL_SNAPSHOT_BYTES {
                    return Err(RecordStoreError::InvalidRecord(
                        "record snapshots exceed the persisted aggregate byte bound".to_owned(),
                    ));
                }
            }
        }
        validate_side_effects(&change.side_effects)?;
    }
    Ok(())
}

fn validate_snapshot_ref(
    snapshot: Option<&TextSnapshotRef>,
    field: &str,
) -> Result<(), RecordStoreError> {
    if let Some(snapshot) = snapshot {
        if snapshot.schema_version != crate::apply_patch::history::SNAPSHOT_REF_SCHEMA_VERSION {
            return Err(RecordStoreError::InvalidRecord(format!(
                "{field} snapshot reference has an unsupported schema version"
            )));
        }
        if snapshot.byte_len > MAX_PERSISTED_SNAPSHOT_BYTES {
            return Err(RecordStoreError::InvalidRecord(format!(
                "{field} snapshot reference exceeds the persisted byte bound"
            )));
        }
    }
    Ok(())
}

fn validate_side_effects(
    side_effects: &crate::apply_patch::history::PatchSideEffects,
) -> Result<(), RecordStoreError> {
    let total_entries = side_effects
        .created_directories
        .len()
        .saturating_add(side_effects.residual_directories.len())
        .saturating_add(side_effects.metadata_warnings.len());
    if total_entries > MAX_PERSISTED_SIDE_EFFECT_ENTRIES {
        return Err(RecordStoreError::InvalidRecord(
            "record side effects exceed the persisted entry bound".to_owned(),
        ));
    }
    if side_effects
        .created_directories
        .iter()
        .chain(side_effects.residual_directories.iter())
        .any(|value| value.len() > MAX_PERSISTED_PATH_BYTES)
        || side_effects
            .metadata_warnings
            .iter()
            .any(|value| value.len() > MAX_PERSISTED_SIDE_EFFECT_STRING_BYTES)
    {
        return Err(RecordStoreError::InvalidRecord(
            "record side-effect text exceeds the persisted bound".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::history::{
        AppliedPatchRecord, AppliedPatchRecordOutcome, ChangeKind, DurablePatchChange,
        InvocationIdentity, PatchSideEffects,
    };

    fn record(invocation_id: &str, ordinal: u64) -> AppliedPatchRecord {
        AppliedPatchRecord::new(
            InvocationIdentity::new("thread", "turn", invocation_id).unwrap(),
            CommitOrdinal(ordinal),
            AppliedPatchRecordOutcome::Applied,
            vec![DurablePatchChange {
                operation_index: 0,
                commit_step: 0,
                sequence: 0,
                kind: ChangeKind::Update,
                source_path: "file.txt".to_owned(),
                destination_path: None,
                before: None,
                after: None,
                overwritten_destination: None,
                side_effects: PatchSideEffects::default(),
            }],
        )
    }

    #[test]
    fn identical_insert_is_idempotent_and_conflict_is_rejected() {
        let log = AppliedPatchLog::new();
        let first = log.insert(record("call", 0), [1; 32]).unwrap();
        assert!(first.was_inserted());
        let replay = log.insert(record("call", 0), [1; 32]).unwrap();
        assert!(!replay.was_inserted());
        let error = log.insert(record("call", 0), [2; 32]).unwrap_err();
        assert!(matches!(
            error,
            RecordStoreError::ConflictingDuplicate { .. }
        ));
    }

    #[test]
    fn ordinal_allocation_is_monotonic_per_turn() {
        let log = AppliedPatchLog::new();
        assert_eq!(
            log.allocate_ordinal("thread", "turn").unwrap(),
            CommitOrdinal(0)
        );
        assert_eq!(
            log.allocate_ordinal("thread", "turn").unwrap(),
            CommitOrdinal(1)
        );
        assert_eq!(
            log.allocate_ordinal("thread", "other").unwrap(),
            CommitOrdinal(0)
        );
    }

    #[test]
    fn records_are_returned_in_commit_order() {
        let log = AppliedPatchLog::new();
        log.insert(record("second", 1), [2; 32]).unwrap();
        log.insert(record("first", 0), [1; 32]).unwrap();
        let records = log.records_for_turn("thread", "turn").unwrap();
        assert_eq!(records[0].record.commit_ordinal, CommitOrdinal(0));
        assert_eq!(records[1].record.commit_ordinal, CommitOrdinal(1));
    }

    #[test]
    fn record_validation_rejects_schema_exactness_and_snapshot_mismatches() {
        let log = AppliedPatchLog::new();

        let mut wrong_schema = record("schema", 0);
        wrong_schema.schema_version += 1;
        assert!(matches!(
            log.insert(wrong_schema, [1; 32]),
            Err(RecordStoreError::InvalidRecord(_))
        ));

        let mut wrong_exactness = record("exactness", 0);
        wrong_exactness.exactness = PatchRecordExactness::Partial;
        assert!(matches!(
            log.insert(wrong_exactness, [2; 32]),
            Err(RecordStoreError::InvalidRecord(_))
        ));

        let mut wrong_snapshot = record("snapshot", 0);
        wrong_snapshot.changes[0]
            .before
            .get_or_insert_with(|| TextSnapshotRef {
                schema_version: crate::apply_patch::history::SNAPSHOT_REF_SCHEMA_VERSION + 1,
                content_hash: [0; 32],
                byte_len: 0,
                encoding: crate::apply_patch::history::TextEncoding::Utf8,
                line_endings: crate::apply_patch::history::LineEndingMetadata {
                    dominant: crate::apply_patch::history::LineEnding::Lf,
                    mixed: false,
                    final_newline: false,
                },
            });
        assert!(matches!(
            log.insert(wrong_snapshot, [3; 32]),
            Err(RecordStoreError::InvalidRecord(_))
        ));
    }

    #[test]
    fn record_validation_rejects_unbounded_persisted_fields() {
        let log = AppliedPatchLog::new();

        let mut too_many_changes = record("too-many", 0);
        too_many_changes.changes = vec![too_many_changes.changes[0].clone(); 257];
        assert!(matches!(
            log.insert(too_many_changes, [4; 32]),
            Err(RecordStoreError::InvalidRecord(_))
        ));

        let mut oversized_path = record("path", 0);
        oversized_path.changes[0].source_path = "x".repeat(MAX_PERSISTED_PATH_BYTES + 1);
        assert!(matches!(
            log.insert(oversized_path, [5; 32]),
            Err(RecordStoreError::InvalidRecord(_))
        ));

        let mut oversized_snapshot = record("bytes", 0);
        oversized_snapshot.changes[0].before = Some(TextSnapshotRef {
            schema_version: crate::apply_patch::history::SNAPSHOT_REF_SCHEMA_VERSION,
            content_hash: [0; 32],
            byte_len: MAX_PERSISTED_SNAPSHOT_BYTES + 1,
            encoding: crate::apply_patch::history::TextEncoding::Utf8,
            line_endings: crate::apply_patch::history::LineEndingMetadata::default(),
        });
        assert!(matches!(
            log.insert(oversized_snapshot, [6; 32]),
            Err(RecordStoreError::InvalidRecord(_))
        ));

        let mut oversized_side_effects = record("effects", 0);
        oversized_side_effects.changes[0]
            .side_effects
            .created_directories = vec!["dir".to_owned(); MAX_PERSISTED_SIDE_EFFECT_ENTRIES + 1];
        assert!(matches!(
            log.insert(oversized_side_effects, [7; 32]),
            Err(RecordStoreError::InvalidRecord(_))
        ));
    }
}
