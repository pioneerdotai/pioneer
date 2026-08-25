use crate::apply_patch::history::{
    AggregateFileChange, AggregateProjectionError, AppliedPatchLog, ChangeKind, CommitOrdinal,
    PatchHistoryCoverage, StoredPatchRecord, TextSnapshotRef, TurnAggregate, TurnDiffAuthority,
    TurnDiffExactness, project_turn_records,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub const TURN_DIFF_STATE_SCHEMA_VERSION: u16 = 1;
const MAX_PROJECTION_ID_BYTES: usize = 4096;
const MAX_PROJECTION_PATH_BYTES: usize = 4096;
const MAX_PROJECTION_ENVIRONMENT_BYTES: usize = 4096;
const MAX_PROJECTION_REASON_BYTES: usize = 4096;
const MAX_PROJECTION_CHANGES: usize = 4096;
const MAX_PROJECTION_SNAPSHOT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_PROJECTION_TOTAL_SNAPSHOT_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TurnDiffState {
    pub schema_version: u16,
    pub thread_id: String,
    pub turn_id: String,
    pub revision: u64,
    pub authority: TurnDiffAuthority,
    pub exactness: TurnDiffExactness,
    /// Derived machine-friendly flag validated against canonical `exactness`.
    pub exact: bool,
    pub coverage: PatchHistoryCoverage,
    pub applied_through_ordinal: Option<CommitOrdinal>,
    pub record_count: u64,
    pub final_state: bool,
    pub aggregate: TurnAggregate,
}

impl TurnDiffState {
    pub fn from_aggregate(
        aggregate: TurnAggregate,
        authority: TurnDiffAuthority,
        revision: u64,
        final_state: bool,
    ) -> Self {
        Self {
            schema_version: TURN_DIFF_STATE_SCHEMA_VERSION,
            thread_id: aggregate.thread_id.clone(),
            turn_id: aggregate.turn_id.clone(),
            revision,
            authority,
            exactness: TurnDiffExactness::from_coverage(aggregate.exact, &aggregate.coverage),
            exact: aggregate.exact,
            coverage: aggregate.coverage.clone(),
            applied_through_ordinal: aggregate.applied_through,
            record_count: aggregate.record_count,
            final_state,
            aggregate,
        }
    }
}

/// Validate the typed aggregate before it crosses either the in-memory or
/// SQLite projection boundary.  The aggregate is normally produced by the
/// bounded projector, but recovery and provider adapters also deserialize it
/// from durable input; those paths must not be able to smuggle an unbounded or
/// internally inconsistent state into the projection.
pub(crate) fn validate_turn_diff_state(state: &TurnDiffState) -> Result<(), String> {
    if state.schema_version != TURN_DIFF_STATE_SCHEMA_VERSION {
        return Err("turn diff state schema version is unsupported".to_owned());
    }
    validate_non_empty_bounded(&state.thread_id, MAX_PROJECTION_ID_BYTES, "thread id")?;
    validate_non_empty_bounded(&state.turn_id, MAX_PROJECTION_ID_BYTES, "turn id")?;
    if state.revision > i64::MAX as u64 {
        return Err("turn diff revision exceeds SQLite integer range".to_owned());
    }
    if state.record_count > i64::MAX as u64 {
        return Err("turn diff record count exceeds SQLite integer range".to_owned());
    }
    if let Some(ordinal) = state.applied_through_ordinal
        && ordinal.0 > i64::MAX as u64
    {
        return Err("turn diff applied ordinal exceeds SQLite integer range".to_owned());
    }
    if state.aggregate.thread_id != state.thread_id || state.aggregate.turn_id != state.turn_id {
        return Err("turn diff aggregate identity disagrees with state identity".to_owned());
    }
    if state.exactness.is_exact() != state.exact {
        return Err("turn diff exactness provenance disagrees with exact flag".to_owned());
    }
    validate_exactness(&state.exactness)?;
    if state.aggregate.exact != state.exact
        || state.aggregate.coverage != state.coverage
        || state.aggregate.applied_through != state.applied_through_ordinal
        || state.aggregate.record_count != state.record_count
    {
        return Err("turn diff denormalized fields disagree with aggregate".to_owned());
    }
    validate_coverage(&state.coverage)?;
    if state.aggregate.changes.len() > MAX_PROJECTION_CHANGES {
        return Err("turn diff aggregate contains too many changes".to_owned());
    }
    let mut total_snapshot_bytes = 0u64;
    for change in &state.aggregate.changes {
        validate_aggregate_change(change, &mut total_snapshot_bytes)?;
    }
    Ok(())
}

fn validate_non_empty_bounded(value: &str, maximum: usize, label: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > maximum {
        return Err(format!("turn diff {label} is empty or exceeds its bound"));
    }
    Ok(())
}

fn validate_optional_bounded(
    value: Option<&str>,
    maximum: usize,
    label: &str,
) -> Result<(), String> {
    if value.is_some_and(|value| value.trim().is_empty() || value.len() > maximum) {
        return Err(format!("turn diff {label} is empty or exceeds its bound"));
    }
    Ok(())
}

fn validate_coverage(coverage: &PatchHistoryCoverage) -> Result<(), String> {
    match coverage {
        PatchHistoryCoverage::EngineVerifiedSteps => Ok(()),
        PatchHistoryCoverage::ProviderReportedSteps { provider, protocol }
        | PatchHistoryCoverage::AggregateOnly { provider, protocol } => {
            validate_non_empty_bounded(provider, MAX_PROJECTION_ID_BYTES, "coverage provider")?;
            validate_non_empty_bounded(protocol, MAX_PROJECTION_REASON_BYTES, "coverage protocol")
        }
        PatchHistoryCoverage::Incomplete { reason }
        | PatchHistoryCoverage::Untracked { reason } => {
            if reason.trim().is_empty() || reason.len() > MAX_PROJECTION_REASON_BYTES {
                Err("turn diff coverage reason is empty or exceeds its bound".to_owned())
            } else {
                Ok(())
            }
        }
    }
}

fn validate_exactness(exactness: &TurnDiffExactness) -> Result<(), String> {
    match exactness {
        TurnDiffExactness::EngineVerified => Ok(()),
        TurnDiffExactness::ProviderReported { provider, protocol } => {
            validate_non_empty_bounded(provider, MAX_PROJECTION_ID_BYTES, "exactness provider")?;
            validate_non_empty_bounded(protocol, MAX_PROJECTION_REASON_BYTES, "exactness protocol")
        }
        TurnDiffExactness::Incomplete { reason } => {
            validate_non_empty_bounded(reason, MAX_PROJECTION_REASON_BYTES, "exactness reason")
        }
    }
}

fn validate_aggregate_change(
    change: &AggregateFileChange,
    total_snapshot_bytes: &mut u64,
) -> Result<(), String> {
    validate_non_empty_bounded(
        &change.source_path,
        MAX_PROJECTION_PATH_BYTES,
        "source path",
    )?;
    validate_optional_bounded(
        change.destination_path.as_deref(),
        MAX_PROJECTION_PATH_BYTES,
        "destination path",
    )?;
    if change.kind == ChangeKind::Move && change.destination_path.is_none() {
        return Err("turn diff move is missing its destination path".to_owned());
    }
    if change.kind != ChangeKind::Move && change.destination_path.is_some() {
        return Err("non-move turn diff change has a destination path".to_owned());
    }
    if change.environment_id.len() > MAX_PROJECTION_ENVIRONMENT_BYTES {
        return Err("turn diff environment id exceeds its bound".to_owned());
    }
    for (snapshot, label) in [
        (change.before.as_ref(), "before"),
        (change.after.as_ref(), "after"),
        (
            change.overwritten_destination.as_ref(),
            "overwritten destination",
        ),
    ] {
        if let Some(snapshot) = snapshot {
            validate_snapshot_ref(snapshot, label, total_snapshot_bytes)?;
        }
    }
    Ok(())
}

fn validate_snapshot_ref(
    snapshot: &TextSnapshotRef,
    label: &str,
    total_snapshot_bytes: &mut u64,
) -> Result<(), String> {
    if snapshot.schema_version != crate::apply_patch::history::SNAPSHOT_REF_SCHEMA_VERSION {
        return Err(format!(
            "turn diff {label} snapshot schema version is unsupported"
        ));
    }
    if snapshot.byte_len > MAX_PROJECTION_SNAPSHOT_BYTES {
        return Err(format!("turn diff {label} snapshot exceeds its byte bound"));
    }
    *total_snapshot_bytes = total_snapshot_bytes
        .checked_add(snapshot.byte_len)
        .ok_or_else(|| "turn diff snapshot byte count overflow".to_owned())?;
    if *total_snapshot_bytes > MAX_PROJECTION_TOTAL_SNAPSHOT_BYTES {
        return Err("turn diff snapshots exceed their aggregate byte bound".to_owned());
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectionError {
    StaleRevision,
    FinalStateImmutable,
    InvalidState(String),
    Aggregate(AggregateProjectionError),
    Poisoned,
}

impl std::fmt::Display for ProjectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StaleRevision => f.write_str("projection revision is stale or duplicated"),
            Self::FinalStateImmutable => f.write_str("final turn diff state is immutable"),
            Self::InvalidState(message) => write!(f, "invalid turn diff state: {message}"),
            Self::Aggregate(error) => error.fmt(f),
            Self::Poisoned => f.write_str("turn diff projection lock is poisoned"),
        }
    }
}

impl std::error::Error for ProjectionError {}

#[derive(Clone, Debug, Default)]
pub struct TurnDiffProjectionStore {
    states: Arc<RwLock<HashMap<(String, String), TurnDiffState>>>,
}

impl TurnDiffProjectionStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update_live(&self, state: TurnDiffState) -> Result<bool, ProjectionError> {
        let mut states = self.states.write().map_err(|_| ProjectionError::Poisoned)?;
        let key = (state.thread_id.clone(), state.turn_id.clone());
        if let Some(existing) = states.get(&key) {
            if existing.final_state {
                if existing == &state {
                    return Ok(false);
                }
                return Err(ProjectionError::FinalStateImmutable);
            }
            if state.revision < existing.revision {
                return Err(ProjectionError::StaleRevision);
            }
            if state.revision == existing.revision {
                if existing == &state {
                    return Ok(false);
                }
                if state.final_state
                    && !existing.final_state
                    && same_projection_ignoring_final(existing, &state)
                {
                    validate_turn_diff_state(&state).map_err(ProjectionError::InvalidState)?;
                    states.insert(key, state);
                    return Ok(true);
                }
                return Err(ProjectionError::StaleRevision);
            }
        }
        validate_turn_diff_state(&state).map_err(ProjectionError::InvalidState)?;
        states.insert(key, state);
        Ok(true)
    }

    pub fn finalize(&self, state: TurnDiffState) -> Result<bool, ProjectionError> {
        let mut state = state;
        state.final_state = true;
        self.update_live(state)
    }

    pub fn get(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<TurnDiffState>, ProjectionError> {
        let states = self.states.read().map_err(|_| ProjectionError::Poisoned)?;
        Ok(states
            .get(&(thread_id.to_owned(), turn_id.to_owned()))
            .cloned())
    }

    pub fn rebuild(
        &self,
        thread_id: &str,
        turn_id: &str,
        records: &[StoredPatchRecord],
        authority: TurnDiffAuthority,
        revision: u64,
    ) -> Result<TurnDiffState, ProjectionError> {
        let aggregate = project_turn_records(thread_id, turn_id, records)
            .map_err(ProjectionError::Aggregate)?;
        let state = TurnDiffState::from_aggregate(aggregate, authority, revision, false);
        self.update_live(state.clone())?;
        Ok(state)
    }

    pub fn rebuild_from_log(
        &self,
        log: &AppliedPatchLog,
        thread_id: &str,
        turn_id: &str,
        authority: TurnDiffAuthority,
        revision: u64,
    ) -> Result<TurnDiffState, ProjectionError> {
        let records = log.records_for_turn(thread_id, turn_id).map_err(|error| {
            ProjectionError::Aggregate(AggregateProjectionError::InvalidRecord(error.to_string()))
        })?;
        self.rebuild(thread_id, turn_id, records.as_slice(), authority, revision)
    }

    pub fn len(&self) -> Result<usize, ProjectionError> {
        let states = self.states.read().map_err(|_| ProjectionError::Poisoned)?;
        Ok(states.len())
    }

    pub fn remove_thread(&self, thread_id: &str) -> Result<usize, ProjectionError> {
        let mut states = self.states.write().map_err(|_| ProjectionError::Poisoned)?;
        let keys = states
            .keys()
            .filter(|(thread, _)| thread == thread_id)
            .cloned()
            .collect::<Vec<_>>();
        let count = keys.len();
        for key in keys {
            states.remove(&key);
        }
        Ok(count)
    }
}

fn same_projection_ignoring_final(left: &TurnDiffState, right: &TurnDiffState) -> bool {
    let mut left = left.clone();
    left.final_state = right.final_state;
    left == *right
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentDiffUpdatedProjection {
    pub thread_id: String,
    pub turn_id: String,
    pub revision: u64,
    pub final_state: bool,
    pub exactness: TurnDiffExactness,
    /// Derived machine-friendly flag validated against canonical `exactness`.
    pub exact: bool,
    pub coverage: PatchHistoryCoverage,
    pub changes: Vec<crate::apply_patch::history::AggregateFileChange>,
}

impl From<&TurnDiffState> for AgentDiffUpdatedProjection {
    fn from(state: &TurnDiffState) -> Self {
        Self {
            thread_id: state.thread_id.clone(),
            turn_id: state.turn_id.clone(),
            revision: state.revision,
            final_state: state.final_state,
            exactness: state.exactness.clone(),
            exact: state.exact,
            coverage: state.coverage.clone(),
            changes: state.aggregate.changes.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::history::{
        AppliedPatchRecord, AppliedPatchRecordOutcome, ChangeKind, DurablePatchChange,
        InvocationIdentity, PatchSideEffects,
    };

    fn record(ordinal: u64) -> StoredPatchRecord {
        StoredPatchRecord {
            record: AppliedPatchRecord::new(
                InvocationIdentity::new("thread", "turn", format!("call-{ordinal}")).unwrap(),
                CommitOrdinal(ordinal),
                AppliedPatchRecordOutcome::Applied,
                vec![DurablePatchChange {
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
                }],
            ),
            plan_fingerprint: [ordinal as u8 + 1; 32],
        }
    }

    #[test]
    fn duplicate_revision_is_idempotent_but_conflicting_revision_is_rejected() {
        let store = TurnDiffProjectionStore::new();
        let aggregate = project_turn_records("thread", "turn", &[record(0)]).unwrap();
        let state = TurnDiffState::from_aggregate(
            aggregate,
            TurnDiffAuthority::NativePatchEngine,
            1,
            false,
        );
        assert!(store.update_live(state.clone()).unwrap());
        assert!(!store.update_live(state.clone()).unwrap());
        let mut conflicting = state;
        conflicting.exact = !conflicting.exact;
        assert_eq!(
            store.update_live(conflicting),
            Err(ProjectionError::StaleRevision)
        );
    }

    #[test]
    fn final_state_cannot_be_overwritten() {
        let store = TurnDiffProjectionStore::new();
        let aggregate = project_turn_records("thread", "turn", &[record(0)]).unwrap();
        let final_state = TurnDiffState::from_aggregate(
            aggregate.clone(),
            TurnDiffAuthority::NativePatchEngine,
            1,
            true,
        );
        assert!(store.finalize(final_state).unwrap());
        let next = TurnDiffState::from_aggregate(
            aggregate,
            TurnDiffAuthority::NativePatchEngine,
            2,
            false,
        );
        assert_eq!(
            store.update_live(next),
            Err(ProjectionError::FinalStateImmutable)
        );
    }

    #[test]
    fn live_projection_can_be_finalized_at_the_same_revision() {
        let store = TurnDiffProjectionStore::new();
        let aggregate = project_turn_records("thread", "turn", &[record(0)]).unwrap();
        let live = TurnDiffState::from_aggregate(
            aggregate.clone(),
            TurnDiffAuthority::NativePatchEngine,
            1,
            false,
        );
        assert!(store.update_live(live).unwrap());
        let final_state =
            TurnDiffState::from_aggregate(aggregate, TurnDiffAuthority::NativePatchEngine, 1, true);
        assert!(store.finalize(final_state.clone()).unwrap());
        assert_eq!(store.get("thread", "turn").unwrap(), Some(final_state));
    }

    #[test]
    fn rebuild_from_log_is_deterministic() {
        let log = AppliedPatchLog::new();
        log.insert(record(0).record, [1; 32]).unwrap();
        let store = TurnDiffProjectionStore::new();
        let first = store
            .rebuild_from_log(
                &log,
                "thread",
                "turn",
                TurnDiffAuthority::NativePatchEngine,
                1,
            )
            .unwrap();
        let second = project_turn_records(
            "thread",
            "turn",
            &log.records_for_turn("thread", "turn").unwrap().as_slice(),
        )
        .unwrap();
        assert_eq!(first.aggregate, second);
    }
}
