use crate::apply_patch::history::{
    AggregateFileChange, AppliedPatchLog, CommitOrdinal, ContentAddressedSnapshotRef,
    PatchHistoryCoverage, StoredPatchRecord, TextSnapshotRef,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoryQueryLimits {
    pub max_page_records: usize,
    pub max_page_bytes: usize,
    pub max_decompressed_bytes: usize,
}

impl Default for HistoryQueryLimits {
    fn default() -> Self {
        Self {
            max_page_records: 100,
            max_page_bytes: 4 * 1024 * 1024,
            max_decompressed_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HistoryCoverage {
    pub exact: bool,
    pub coverage: PatchHistoryCoverage,
    pub first_missing_ordinal: Option<CommitOrdinal>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HistoryRenderedDiff {
    pub unified_patch: String,
    pub exactness: crate::apply_patch::history::TurnDiffExactness,
    pub coverage: PatchHistoryCoverage,
    pub records_rendered: u32,
    pub after_ordinal: Option<CommitOrdinal>,
    pub through_ordinal: Option<CommitOrdinal>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AppliedStep {
    pub record: StoredPatchRecord,
    pub coverage: HistoryCoverage,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HistoryPage<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<CommitOrdinal>,
    /// Thread history ordinals restart at every turn, so a thread cursor
    /// carries the turn identity as well as the per-turn ordinal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_thread_cursor: Option<ThreadHistoryCursor>,
    /// File-history pages can split one AppliedPatchRecord into multiple
    /// entries.  An ordinal-only cursor would skip the remaining changes in
    /// that record, so file queries use this sequence-aware continuation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_file_cursor: Option<FileHistoryCursor>,
    pub coverage: HistoryCoverage,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FileHistoryCursor {
    pub environment_id: String,
    pub turn_id: String,
    pub ordinal: CommitOrdinal,
    pub sequence: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadHistoryCursor {
    pub turn_id: String,
    pub ordinal: CommitOrdinal,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FileHistoryEntry {
    pub environment_id: String,
    pub turn_id: String,
    pub ordinal: CommitOrdinal,
    pub invocation_id: String,
    pub change: crate::apply_patch::history::DurablePatchChange,
    pub before: Option<TextSnapshotRef>,
    pub after: Option<TextSnapshotRef>,
    pub overwritten_destination: Option<TextSnapshotRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HistoryQueryError {
    InvalidLimit,
    InvalidArgument,
    PageTooLarge,
    Store(String),
}

impl std::fmt::Display for HistoryQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLimit => f.write_str("history query limit is invalid"),
            Self::InvalidArgument => f.write_str("history query argument is invalid"),
            Self::PageTooLarge => f.write_str("history page exceeds the configured byte limit"),
            Self::Store(message) => write!(f, "history store query failed: {message}"),
        }
    }
}

impl std::error::Error for HistoryQueryError {}

pub fn query_turn_steps(
    log: &AppliedPatchLog,
    thread_id: &str,
    turn_id: &str,
    cursor: Option<CommitOrdinal>,
    limits: HistoryQueryLimits,
) -> Result<HistoryPage<AppliedStep>, HistoryQueryError> {
    validate_limits(limits)?;
    validate_query_id(thread_id)?;
    validate_query_id(turn_id)?;
    if let Some(cursor) = cursor {
        validate_query_ordinal_cursor(cursor)?;
    }
    let records = log
        .records_for_turn(thread_id, turn_id)
        .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
    let coverage = coverage_for_records(&records);
    let mut items = Vec::new();
    let mut next_cursor = None;
    let mut page_bytes = 0usize;
    for stored in records {
        if cursor.is_some_and(|cursor| stored.record.commit_ordinal <= cursor) {
            continue;
        }
        if items.len() >= limits.max_page_records {
            next_cursor = items
                .last()
                .map(|item: &AppliedStep| item.record.record.commit_ordinal);
            break;
        }
        let item = AppliedStep {
            record: stored,
            coverage: coverage.clone(),
        };
        let item_bytes = serde_json::to_vec(&item)
            .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
        if item_bytes.len() > limits.max_page_bytes {
            return Err(HistoryQueryError::PageTooLarge);
        }
        if page_bytes.saturating_add(item_bytes.len()) > limits.max_page_bytes {
            next_cursor = items
                .last()
                .map(|item: &AppliedStep| item.record.record.commit_ordinal);
            if items.is_empty() {
                return Err(HistoryQueryError::PageTooLarge);
            }
            break;
        }
        page_bytes = page_bytes.saturating_add(item_bytes.len());
        items.push(item);
    }
    Ok(HistoryPage {
        items,
        next_cursor,
        next_thread_cursor: None,
        next_file_cursor: None,
        coverage,
    })
}

pub fn query_thread_steps(
    log: &AppliedPatchLog,
    thread_id: &str,
    cursor: Option<ThreadHistoryCursor>,
    limits: HistoryQueryLimits,
) -> Result<HistoryPage<AppliedStep>, HistoryQueryError> {
    validate_limits(limits)?;
    validate_query_id(thread_id)?;
    if let Some(cursor) = cursor.as_ref() {
        validate_query_id(&cursor.turn_id)?;
        validate_query_ordinal_cursor(cursor.ordinal)?;
    }
    let records = log
        .records_for_thread(thread_id)
        .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
    let coverage = coverage_for_records(&records);
    let mut items = Vec::new();
    let mut page_bytes = 0usize;
    for stored in records {
        if cursor.as_ref().is_some_and(|cursor| {
            stored.record.identity.turn_id < cursor.turn_id
                || (stored.record.identity.turn_id == cursor.turn_id
                    && stored.record.commit_ordinal <= cursor.ordinal)
        }) {
            continue;
        }
        if items.len() >= limits.max_page_records {
            let next_thread_cursor = items.last().map(|item: &AppliedStep| ThreadHistoryCursor {
                turn_id: item.record.record.identity.turn_id.clone(),
                ordinal: item.record.record.commit_ordinal,
            });
            return Ok(HistoryPage {
                items,
                next_cursor: None,
                next_thread_cursor,
                next_file_cursor: None,
                coverage,
            });
        }
        let item = AppliedStep {
            record: stored,
            coverage: coverage.clone(),
        };
        let item_bytes = serde_json::to_vec(&item)
            .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
        if item_bytes.len() > limits.max_page_bytes {
            return Err(HistoryQueryError::PageTooLarge);
        }
        if page_bytes.saturating_add(item_bytes.len()) > limits.max_page_bytes {
            let next_thread_cursor = items.last().map(|item: &AppliedStep| ThreadHistoryCursor {
                turn_id: item.record.record.identity.turn_id.clone(),
                ordinal: item.record.record.commit_ordinal,
            });
            if items.is_empty() {
                return Err(HistoryQueryError::PageTooLarge);
            }
            return Ok(HistoryPage {
                items,
                next_cursor: None,
                next_thread_cursor,
                next_file_cursor: None,
                coverage,
            });
        }
        page_bytes = page_bytes.saturating_add(item_bytes.len());
        items.push(item);
    }
    Ok(HistoryPage {
        items,
        next_cursor: None,
        next_thread_cursor: None,
        next_file_cursor: None,
        coverage,
    })
}

pub fn query_file_history(
    log: &AppliedPatchLog,
    thread_id: &str,
    path: &str,
    cursor: Option<FileHistoryCursor>,
    limits: HistoryQueryLimits,
) -> Result<HistoryPage<FileHistoryEntry>, HistoryQueryError> {
    validate_limits(limits)?;
    validate_query_id(thread_id)?;
    validate_query_path(path)?;
    if let Some(cursor) = cursor.as_ref() {
        validate_optional_query_id(&cursor.environment_id)?;
        validate_query_id(&cursor.turn_id)?;
        validate_query_ordinal_cursor(cursor.ordinal)?;
    }
    let records = log
        .records_for_thread(thread_id)
        .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
    let coverage = coverage_for_records(&records);
    let aliases = lineage_aliases_by_environment(&records, path);
    let mut entries = Vec::new();
    for stored in records {
        let ordinal = stored.record.commit_ordinal;
        for change in stored.record.changes {
            let environment_aliases = aliases.get(&stored.record.environment_id);
            let matches_source = environment_aliases
                .is_some_and(|aliases| aliases.iter().any(|alias| alias == &change.source_path));
            let matches_destination = change.destination_path.as_ref().is_some_and(|destination| {
                environment_aliases
                    .is_some_and(|aliases| aliases.iter().any(|alias| alias == destination))
            });
            if !matches_source && !matches_destination {
                continue;
            }
            let entry = FileHistoryEntry {
                environment_id: stored.record.environment_id.clone(),
                turn_id: stored.record.identity.turn_id.clone(),
                ordinal,
                invocation_id: stored.record.identity.invocation_id.clone(),
                before: change.before.clone(),
                after: change.after.clone(),
                overwritten_destination: change.overwritten_destination.clone(),
                change,
            };
            entries.push(entry);
        }
    }

    // Keep the in-memory implementation's order identical to the SQLite
    // index.  `records_for_thread` is ordered by turn/ordinal, whereas the
    // file cursor is explicitly environment/turn/ordinal/sequence.  Sorting
    // before applying the cursor prevents skipped or duplicated entries when
    // one thread contains more than one workspace environment.
    entries.sort_by(|left, right| {
        (
            left.environment_id.as_str(),
            left.turn_id.as_str(),
            left.ordinal,
            left.change.sequence,
        )
            .cmp(&(
                right.environment_id.as_str(),
                right.turn_id.as_str(),
                right.ordinal,
                right.change.sequence,
            ))
    });

    let mut paged_entries = Vec::new();
    let mut next_file_cursor = None;
    let mut page_bytes = 0usize;
    for entry in entries {
        if cursor.as_ref().is_some_and(|cursor| {
            (
                entry.environment_id.as_str(),
                entry.turn_id.as_str(),
                entry.ordinal,
                entry.change.sequence,
            ) <= (
                cursor.environment_id.as_str(),
                cursor.turn_id.as_str(),
                cursor.ordinal,
                cursor.sequence,
            )
        }) {
            continue;
        }
        if paged_entries.len() >= limits.max_page_records {
            next_file_cursor =
                paged_entries
                    .last()
                    .map(|entry: &FileHistoryEntry| FileHistoryCursor {
                        environment_id: entry.environment_id.clone(),
                        turn_id: entry.turn_id.clone(),
                        ordinal: entry.ordinal,
                        sequence: entry.change.sequence,
                    });
            break;
        }
        let entry_bytes = serde_json::to_vec(&entry)
            .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
        if entry_bytes.len() > limits.max_page_bytes {
            return Err(HistoryQueryError::PageTooLarge);
        }
        if page_bytes.saturating_add(entry_bytes.len()) > limits.max_page_bytes {
            if paged_entries.is_empty() {
                return Err(HistoryQueryError::PageTooLarge);
            }
            next_file_cursor =
                paged_entries
                    .last()
                    .map(|entry: &FileHistoryEntry| FileHistoryCursor {
                        environment_id: entry.environment_id.clone(),
                        turn_id: entry.turn_id.clone(),
                        ordinal: entry.ordinal,
                        sequence: entry.change.sequence,
                    });
            break;
        }
        page_bytes = page_bytes.saturating_add(entry_bytes.len());
        paged_entries.push(entry);
    }
    Ok(HistoryPage {
        items: paged_entries,
        next_cursor: None,
        next_thread_cursor: None,
        next_file_cursor,
        coverage,
    })
}

/// Resolve the logical file lineage before applying pagination.  A query for
/// the post-rename path must include edits made under the pre-rename path, so
/// aliases are discovered in reverse commit order and then used for the
/// forward history scan.  The result is deliberately path-only: snapshot
/// contents remain references and are never read from the workspace here.
fn lineage_aliases_by_environment(
    records: &[StoredPatchRecord],
    requested_path: &str,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut grouped = BTreeMap::<String, Vec<&StoredPatchRecord>>::new();
    for stored in records {
        grouped
            .entry(stored.record.environment_id.clone())
            .or_default()
            .push(stored);
    }
    grouped
        .into_iter()
        .map(|(environment_id, mut records)| {
            // Commit ordinals restart at every turn.  The file-history cursor
            // and the SQLite path-index use the deterministic
            // environment/turn/ordinal/sequence order, so lineage discovery
            // must use the same cross-turn order instead of sorting by the
            // ordinal alone.
            records.sort_by(|left, right| {
                left.record
                    .identity
                    .turn_id
                    .cmp(&right.record.identity.turn_id)
                    .then(left.record.commit_ordinal.cmp(&right.record.commit_ordinal))
                    .then_with(|| {
                        left.record
                            .changes
                            .first()
                            .map(|change| change.sequence)
                            .cmp(&right.record.changes.first().map(|change| change.sequence))
                    })
            });
            let mut aliases = BTreeSet::from([requested_path.to_owned()]);
            for stored in records.into_iter().rev() {
                for change in stored.record.changes.iter().rev() {
                    let Some(destination) = change.destination_path.as_deref() else {
                        continue;
                    };
                    if aliases.contains(destination) {
                        aliases.insert(change.source_path.clone());
                    }
                    if aliases.contains(&change.source_path) {
                        aliases.insert(destination.to_owned());
                    }
                }
            }
            (environment_id, aliases)
        })
        .collect()
}

fn validate_limits(limits: HistoryQueryLimits) -> Result<(), HistoryQueryError> {
    if limits.max_page_records == 0
        || limits.max_page_bytes == 0
        || limits.max_decompressed_bytes == 0
    {
        return Err(HistoryQueryError::InvalidLimit);
    }
    Ok(())
}

pub(crate) const MAX_HISTORY_QUERY_ID_BYTES: usize = 4096;
pub(crate) const MAX_HISTORY_QUERY_PATH_BYTES: usize = 4096;

pub(crate) fn validate_query_id(value: &str) -> Result<(), HistoryQueryError> {
    if value.trim().is_empty() || value.len() > MAX_HISTORY_QUERY_ID_BYTES {
        return Err(HistoryQueryError::InvalidArgument);
    }
    Ok(())
}

pub(crate) fn validate_optional_query_id(value: &str) -> Result<(), HistoryQueryError> {
    if value.is_empty() {
        return Ok(());
    }
    validate_query_id(value)
}

pub(crate) fn validate_query_path(value: &str) -> Result<(), HistoryQueryError> {
    if value.trim().is_empty() || value.len() > MAX_HISTORY_QUERY_PATH_BYTES {
        return Err(HistoryQueryError::InvalidArgument);
    }
    Ok(())
}

fn validate_query_ordinal_cursor(value: CommitOrdinal) -> Result<(), HistoryQueryError> {
    // The ordinal itself is bounded by the SQLite adapter when it becomes a
    // SQL parameter.  Rejecting values above that boundary here keeps the
    // in-memory and durable query contracts identical.
    if value.0 > i64::MAX as u64 {
        return Err(HistoryQueryError::InvalidArgument);
    }
    Ok(())
}

pub(crate) fn coverage_for_records(records: &[StoredPatchRecord]) -> HistoryCoverage {
    coverage_for_records_with_ordinal_status(records, &BTreeMap::new())
}

/// Streaming coverage fold used by the durable query adapter. It consumes the
/// record and intent-status streams in commit order and retains only a small
/// ordinal watermark; terminal markers are never collected into an unbounded
/// set just to answer a paginated history query.
pub(crate) struct HistoryCoverageAccumulator {
    expected: u64,
    exact: bool,
    first_missing_ordinal: Option<CommitOrdinal>,
}

impl HistoryCoverageAccumulator {
    pub(crate) fn new() -> Self {
        Self {
            expected: 0,
            exact: true,
            first_missing_ordinal: None,
        }
    }

    pub(crate) fn push(&mut self, stored: &StoredPatchRecord) {
        let ordinal = stored.record.commit_ordinal;
        if ordinal.0 != self.expected {
            self.exact = false;
            self.first_missing_ordinal
                .get_or_insert(CommitOrdinal(self.expected));
            if ordinal.0 < self.expected {
                return;
            }
        }
        if stored.record.outcome.is_uncertain() {
            self.exact = false;
        }
        self.expected = ordinal.0.saturating_add(1);
    }

    pub(crate) fn push_status(
        &mut self,
        ordinal: CommitOrdinal,
        status: crate::apply_patch::history::IntentStatus,
    ) {
        match status {
            crate::apply_patch::history::IntentStatus::AppliedNoChange
            | crate::apply_patch::history::IntentStatus::FailedNoChange
            | crate::apply_patch::history::IntentStatus::Rejected => {
                if ordinal.0 > self.expected {
                    self.exact = false;
                    self.first_missing_ordinal
                        .get_or_insert(CommitOrdinal(self.expected));
                    self.expected = ordinal.0.saturating_add(1);
                } else if ordinal.0 == self.expected {
                    self.expected = self.expected.saturating_add(1);
                }
            }
            crate::apply_patch::history::IntentStatus::Pending
            | crate::apply_patch::history::IntentStatus::Gap => {
                self.exact = false;
                self.first_missing_ordinal
                    .get_or_insert(if ordinal.0 > self.expected {
                        CommitOrdinal(self.expected)
                    } else {
                        ordinal
                    });
                if ordinal.0 >= self.expected {
                    self.expected = ordinal.0.saturating_add(1);
                }
            }
            crate::apply_patch::history::IntentStatus::Promoted => {}
        }
    }

    pub(crate) fn finish(self) -> HistoryCoverage {
        let coverage = if self.exact {
            PatchHistoryCoverage::EngineVerifiedSteps
        } else {
            PatchHistoryCoverage::Incomplete {
                reason: self
                    .first_missing_ordinal
                    .map(|ordinal| format!("missing commit ordinal {}", ordinal.0))
                    .unwrap_or_else(|| "one or more records are partial or uncertain".to_owned()),
            }
        };
        HistoryCoverage {
            exact: self.exact,
            coverage,
            first_missing_ordinal: self.first_missing_ordinal,
        }
    }
}

/// Computes coverage from both immutable applied records and the durable
/// intent statuses.  A rejected/no-change ordinal is a deliberate terminal
/// step and must advance the sequence without creating a false gap; pending
/// and gap intents remain explicitly incomplete even though they do not have
/// an AppliedPatchRecord yet.
pub(crate) fn coverage_for_records_with_ordinal_status(
    records: &[StoredPatchRecord],
    statuses: &BTreeMap<String, (BTreeSet<CommitOrdinal>, BTreeSet<CommitOrdinal>)>,
) -> HistoryCoverage {
    let mut exact = true;
    let mut first_missing_ordinal = None;
    let mut records_by_turn = BTreeMap::<&str, BTreeMap<u64, &StoredPatchRecord>>::new();
    for record in records {
        records_by_turn
            .entry(record.record.identity.turn_id.as_str())
            .or_default()
            .insert(record.record.commit_ordinal.0, record);
    }
    let mut turns = records_by_turn
        .keys()
        .map(|turn_id| (*turn_id).to_owned())
        .collect::<BTreeSet<_>>();
    turns.extend(statuses.keys().cloned());
    for turn_id in turns {
        let mut expected = 0u64;
        let (terminal_no_change, unresolved) = statuses.get(&turn_id).cloned().unwrap_or_default();
        let mut ordinals = records_by_turn
            .get(turn_id.as_str())
            .into_iter()
            .flat_map(|records| records.keys().copied())
            .collect::<BTreeSet<_>>();
        ordinals.extend(terminal_no_change.iter().map(|ordinal| ordinal.0));
        ordinals.extend(unresolved.iter().map(|ordinal| ordinal.0));
        for ordinal in ordinals {
            if ordinal != expected {
                if first_missing_ordinal.is_none() {
                    first_missing_ordinal = Some(CommitOrdinal(expected));
                }
                exact = false;
                if ordinal < expected {
                    continue;
                }
            }
            let ordinal = CommitOrdinal(ordinal);
            if unresolved.contains(&ordinal) {
                exact = false;
                if first_missing_ordinal.is_none() {
                    first_missing_ordinal = Some(ordinal);
                }
            } else if !terminal_no_change.contains(&ordinal)
                && !records_by_turn
                    .get(turn_id.as_str())
                    .and_then(|records| records.get(&ordinal.0))
                    .is_some_and(|record| record.record.exactness.is_exact())
            {
                exact = false;
            }
            expected = ordinal.0.saturating_add(1);
        }
    }
    let coverage = if exact {
        PatchHistoryCoverage::EngineVerifiedSteps
    } else {
        PatchHistoryCoverage::Incomplete {
            reason: first_missing_ordinal
                .map(|ordinal| format!("missing commit ordinal {}", ordinal.0))
                .unwrap_or_else(|| "one or more records are partial or uncertain".to_owned()),
        }
    };
    HistoryCoverage {
        exact,
        coverage,
        first_missing_ordinal,
    }
}

/// Proposal 64 semantic input: callers can expose a typed change list without
/// making the aggregate projection the source log.
pub fn aggregate_semantic_inputs(changes: &[AggregateFileChange]) -> Vec<serde_json::Value> {
    changes
        .iter()
        .map(|change| {
            serde_json::json!({
                "environmentId": change.environment_id,
                "kind": change.kind,
                "sourcePath": change.source_path,
                "destinationPath": change.destination_path,
                "before": change.before,
                "after": change.after,
                "overwrittenDestination": change.overwritten_destination,
            })
        })
        .collect()
}

/// A content-addressed reference is intentionally returned as data, not read
/// from the current workspace.  Authorization/redaction is performed by the
/// caller before resolving it through the snapshot store.
pub fn authorized_snapshot_reference(
    reference: &ContentAddressedSnapshotRef,
    allow: bool,
) -> Option<ContentAddressedSnapshotRef> {
    allow.then(|| reference.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::history::{
        AppliedPatchRecord, AppliedPatchRecordOutcome, ChangeKind, DurablePatchChange,
        InvocationIdentity, PatchSideEffects,
    };

    fn record(ordinal: u64, path: &str, destination: Option<&str>) -> StoredPatchRecord {
        StoredPatchRecord {
            record: AppliedPatchRecord::new(
                InvocationIdentity::new(
                    "thread",
                    format!("turn-{ordinal}"),
                    format!("call-{ordinal}"),
                )
                .unwrap(),
                CommitOrdinal(ordinal),
                AppliedPatchRecordOutcome::Applied,
                vec![DurablePatchChange {
                    operation_index: 0,
                    commit_step: 0,
                    sequence: 0,
                    kind: if destination.is_some() {
                        ChangeKind::Move
                    } else {
                        ChangeKind::Update
                    },
                    source_path: path.to_owned(),
                    destination_path: destination.map(str::to_owned),
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
    fn page_cursor_bounds_step_history() {
        let log = AppliedPatchLog::new();
        for ordinal in 0..3 {
            let item = record(ordinal, "a.txt", None);
            log.insert(item.record, [ordinal as u8 + 1; 32]).unwrap();
        }
        let first = query_thread_steps(
            &log,
            "thread",
            None,
            HistoryQueryLimits {
                max_page_records: 2,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(first.items.len(), 2);
        assert_eq!(
            first.next_thread_cursor,
            Some(ThreadHistoryCursor {
                turn_id: "turn-1".to_owned(),
                ordinal: CommitOrdinal(1),
            })
        );
        let second = query_thread_steps(
            &log,
            "thread",
            first.next_thread_cursor,
            HistoryQueryLimits::default(),
        )
        .unwrap();
        assert_eq!(second.items.len(), 1);
    }

    #[test]
    fn file_history_follows_rename_alias() {
        let log = AppliedPatchLog::new();
        let item = record(0, "old.txt", Some("new.txt"));
        log.insert(item.record, [1; 32]).unwrap();
        let page = query_file_history(
            &log,
            "thread",
            "new.txt",
            None,
            HistoryQueryLimits::default(),
        )
        .unwrap();
        assert_eq!(page.items.len(), 1);
    }

    #[test]
    fn file_history_follows_rename_alias_across_turns_with_reset_ordinals() {
        let log = AppliedPatchLog::new();
        let mut first = record(1, "old.txt", Some("middle.txt"));
        first.record.identity.turn_id = "turn-001".to_owned();
        log.insert(first.record, [1; 32]).unwrap();

        let mut second = record(0, "middle.txt", Some("new.txt"));
        second.record.identity.turn_id = "turn-002".to_owned();
        log.insert(second.record, [2; 32]).unwrap();

        let page = query_file_history(
            &log,
            "thread",
            "new.txt",
            None,
            HistoryQueryLimits::default(),
        )
        .unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].change.source_path, "old.txt");
        assert_eq!(page.items[1].change.source_path, "middle.txt");
    }

    #[test]
    fn file_history_does_not_cross_environment_rename_lineage() {
        let log = AppliedPatchLog::new();
        let mut first = record(0, "old.txt", Some("new.txt"));
        first.record.environment_id = "environment-a".to_owned();
        let mut second = record(1, "other.txt", Some("old.txt"));
        second.record.environment_id = "environment-b".to_owned();
        log.insert(first.record, [1; 32]).unwrap();
        log.insert(second.record, [2; 32]).unwrap();

        let page = query_file_history(
            &log,
            "thread",
            "new.txt",
            None,
            HistoryQueryLimits::default(),
        )
        .unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].environment_id, "environment-a");
    }

    #[test]
    fn file_history_cursor_does_not_skip_changes_within_one_record() {
        let log = AppliedPatchLog::new();
        let record = StoredPatchRecord {
            record: AppliedPatchRecord::new(
                InvocationIdentity::new("thread", "turn", "call").unwrap(),
                CommitOrdinal(0),
                AppliedPatchRecordOutcome::Applied,
                vec![
                    DurablePatchChange {
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
                    },
                    DurablePatchChange {
                        operation_index: 1,
                        commit_step: 1,
                        sequence: 1,
                        kind: ChangeKind::Update,
                        source_path: "a.txt".to_owned(),
                        destination_path: None,
                        before: None,
                        after: None,
                        overwritten_destination: None,
                        side_effects: PatchSideEffects::default(),
                    },
                ],
            ),
            plan_fingerprint: [9; 32],
        };
        log.insert(record.record, [9; 32]).unwrap();
        let first = query_file_history(
            &log,
            "thread",
            "a.txt",
            None,
            HistoryQueryLimits {
                max_page_records: 1,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(first.items.len(), 1);
        let cursor = first.next_file_cursor.expect("continuation cursor");
        let second = query_file_history(
            &log,
            "thread",
            "a.txt",
            Some(cursor),
            HistoryQueryLimits::default(),
        )
        .unwrap();
        assert_eq!(second.items.len(), 1);
        assert_eq!(second.items[0].change.sequence, 1);
    }

    #[test]
    fn thread_coverage_resets_commit_ordinals_for_each_turn() {
        let log = AppliedPatchLog::new();
        let first = record(0, "a.txt", None);
        let second = StoredPatchRecord {
            record: AppliedPatchRecord::new(
                InvocationIdentity::new("thread", "turn-b", "call-b").unwrap(),
                CommitOrdinal(0),
                AppliedPatchRecordOutcome::Applied,
                first.record.changes.clone(),
            ),
            plan_fingerprint: [2; 32],
        };
        log.insert(first.record, [1; 32]).unwrap();
        log.insert(second.record, [2; 32]).unwrap();
        let page = query_thread_steps(&log, "thread", None, HistoryQueryLimits::default()).unwrap();
        assert!(page.coverage.exact);
        assert_eq!(page.items.len(), 2);
    }

    #[test]
    fn exact_partial_step_does_not_downgrade_history_coverage() {
        let log = AppliedPatchLog::new();
        let mut partial = record(0, "a.txt", None);
        partial.record.outcome = AppliedPatchRecordOutcome::Partial {
            failed_stage: crate::apply_patch::file_mutation::PatchStage::Commit,
            error_code: crate::apply_patch::file_mutation::PatchErrorCode::Io,
        };
        partial.record.exactness = crate::apply_patch::history::PatchRecordExactness::Partial;
        log.insert(partial.record, [1; 32]).unwrap();

        let page = query_turn_steps(
            &log,
            "thread",
            "turn-1",
            None,
            HistoryQueryLimits::default(),
        )
        .unwrap();

        assert!(page.coverage.exact);
        assert_eq!(
            page.coverage.coverage,
            PatchHistoryCoverage::EngineVerifiedSteps
        );
    }
}
