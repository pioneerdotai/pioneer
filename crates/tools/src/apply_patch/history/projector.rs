use crate::apply_patch::history::{
    ChangeKind, CommitOrdinal, PatchHistoryCoverage, StoredPatchRecord, TextSnapshotRef,
};
use std::collections::{BTreeMap, BTreeSet};

const MAX_RENDERED_DIFF_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateFileChange {
    pub environment_id: String,
    pub kind: ChangeKind,
    pub source_path: String,
    pub destination_path: Option<String>,
    pub before: Option<TextSnapshotRef>,
    pub after: Option<TextSnapshotRef>,
    pub overwritten_destination: Option<TextSnapshotRef>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TurnAggregate {
    pub thread_id: String,
    pub turn_id: String,
    pub changes: Vec<AggregateFileChange>,
    pub exact: bool,
    pub coverage: PatchHistoryCoverage,
    pub applied_through: Option<CommitOrdinal>,
    pub record_count: u64,
}

impl TurnAggregate {
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn render_unified_diff<F>(&self, mut resolve: F) -> Result<String, AggregateProjectionError>
    where
        F: FnMut(&TextSnapshotRef) -> Result<Vec<u8>, AggregateProjectionError>,
    {
        let mut output = String::new();
        for change in &self.changes {
            let destination = change
                .destination_path
                .as_deref()
                .unwrap_or(change.source_path.as_str());
            output.push_str(&format!(
                "--- a/{}\n+++ b/{}\n",
                change.source_path, destination
            ));
            ensure_rendered_size(&output)?;
            let before = match &change.before {
                Some(reference) => resolve(reference)?,
                None => Vec::new(),
            };
            let after = match &change.after {
                Some(reference) => resolve(reference)?,
                None => Vec::new(),
            };
            let old_lines = text_line_count(&before)?;
            let new_lines = text_line_count(&after)?;
            if before != after && (old_lines != 0 || new_lines != 0) {
                output.push_str(&format!(
                    "@@ -{},{} +{},{} @@\n",
                    if old_lines == 0 { 0 } else { 1 },
                    old_lines,
                    if new_lines == 0 { 0 } else { 1 },
                    new_lines,
                ));
                ensure_rendered_size(&output)?;
                render_diff_side(&before, '-', &mut output)?;
                render_diff_side(&after, '+', &mut output)?;
            }
        }
        Ok(output)
    }
}

fn ensure_rendered_size(output: &str) -> Result<(), AggregateProjectionError> {
    if output.len() > MAX_RENDERED_DIFF_BYTES {
        return Err(AggregateProjectionError::Snapshot(
            "unified diff exceeds the rendering byte limit".to_owned(),
        ));
    }
    Ok(())
}

fn text_line_count(bytes: &[u8]) -> Result<usize, AggregateProjectionError> {
    let text = std::str::from_utf8(bytes).map_err(|error| {
        AggregateProjectionError::Snapshot(format!(
            "snapshot is not valid UTF-8 for unified rendering: {error}"
        ))
    })?;
    if text.is_empty() {
        Ok(0)
    } else {
        Ok(text.bytes().filter(|byte| *byte == b'\n').count() + usize::from(!text.ends_with('\n')))
    }
}

fn render_diff_side(
    bytes: &[u8],
    prefix: char,
    output: &mut String,
) -> Result<(), AggregateProjectionError> {
    let text = std::str::from_utf8(bytes).map_err(|error| {
        AggregateProjectionError::Snapshot(format!(
            "snapshot is not valid UTF-8 for unified rendering: {error}"
        ))
    })?;
    for line in text.split_inclusive('\n') {
        let has_newline = line.ends_with('\n');
        let content = line.strip_suffix('\n').unwrap_or(line);
        output.push(prefix);
        output.push_str(content);
        output.push('\n');
        ensure_rendered_size(output)?;
        if !has_newline {
            output.push_str("\\ No newline at end of file\n");
            ensure_rendered_size(output)?;
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AggregateProjectionError {
    ConflictingRecordOrder,
    InvalidRecord(String),
    Snapshot(String),
}

impl std::fmt::Display for AggregateProjectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConflictingRecordOrder => {
                f.write_str("records contain duplicate commit ordinals")
            }
            Self::InvalidRecord(message) => f.write_str(message),
            Self::Snapshot(message) => write!(f, "snapshot resolution failed: {message}"),
        }
    }
}

impl std::error::Error for AggregateProjectionError {}

#[derive(Clone, Debug)]
struct Lineage {
    environment_id: String,
    original_path: String,
    current_path: String,
    kind: ChangeKind,
    before: Option<TextSnapshotRef>,
    after: Option<TextSnapshotRef>,
    overwritten_destination: Option<TextSnapshotRef>,
}

/// Purely folds the immutable record sequence. It does not touch the
/// workspace, Git, clock or database and can therefore be used for rebuilds.
pub fn project_turn_records(
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
    records: &[StoredPatchRecord],
) -> Result<TurnAggregate, AggregateProjectionError> {
    project_turn_records_with_ordinal_status(
        thread_id,
        turn_id,
        records,
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
}

/// Fold records while accounting for durable commit-intent ordinals that are
/// known to have ended before any filesystem mutation. A terminal no-change
/// intent is not an applied record and therefore is skipped for continuity;
/// a pending/gap intent remains an explicit unresolved hole. Keeping this
/// status separate from the immutable record log preserves the rule that
/// rejected calls never become applied history while preventing their terminal
/// no-change ordinals from falsely making a later successful record incomplete.
pub fn project_turn_records_with_ordinal_status(
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
    records: &[StoredPatchRecord],
    terminal_no_change_ordinals: &BTreeSet<CommitOrdinal>,
    unresolved_ordinals: &BTreeSet<CommitOrdinal>,
) -> Result<TurnAggregate, AggregateProjectionError> {
    let thread_id = thread_id.into();
    let turn_id = turn_id.into();
    let mut ordered = records.to_vec();
    ordered.sort_by_key(|record| record.record.commit_ordinal);
    let mut projector = TurnRecordProjector::new(
        thread_id,
        turn_id,
        terminal_no_change_ordinals,
        unresolved_ordinals,
    );
    for stored in &ordered {
        projector.push(stored)?;
    }
    projector.finish()
}

/// Incremental, allocation-bounded record folder used by durable replay.
///
/// The projector retains only the current lineage map and coverage counters;
/// callers can feed records in commit order page by page without materializing
/// a complete turn or journal in memory.
pub struct TurnRecordProjector {
    thread_id: String,
    turn_id: String,
    terminal_no_change_ordinals: BTreeSet<CommitOrdinal>,
    first_unresolved_ordinal: Option<CommitOrdinal>,
    previous_ordinal: Option<CommitOrdinal>,
    expected: u64,
    applied_through: Option<CommitOrdinal>,
    contiguous: bool,
    exact: bool,
    coverage_reason: Option<String>,
    lineages: BTreeMap<(String, String), Lineage>,
    continuity_exact: bool,
    record_count: u64,
}

impl TurnRecordProjector {
    pub fn new(
        thread_id: impl Into<String>,
        turn_id: impl Into<String>,
        terminal_no_change_ordinals: &BTreeSet<CommitOrdinal>,
        unresolved_ordinals: &BTreeSet<CommitOrdinal>,
    ) -> Self {
        Self::new_at_ordinal(
            thread_id,
            turn_id,
            terminal_no_change_ordinals,
            unresolved_ordinals,
            CommitOrdinal(0),
        )
    }

    /// Start a projection at a caller-selected history boundary. This is used
    /// by persisted between-boundary diff queries; it does not relabel durable
    /// ordinals or manufacture coverage for earlier records.
    pub fn new_at_ordinal(
        thread_id: impl Into<String>,
        turn_id: impl Into<String>,
        terminal_no_change_ordinals: &BTreeSet<CommitOrdinal>,
        unresolved_ordinals: &BTreeSet<CommitOrdinal>,
        first_ordinal: CommitOrdinal,
    ) -> Self {
        Self {
            thread_id: thread_id.into(),
            turn_id: turn_id.into(),
            terminal_no_change_ordinals: terminal_no_change_ordinals.clone(),
            first_unresolved_ordinal: unresolved_ordinals.iter().next().copied(),
            previous_ordinal: None,
            expected: first_ordinal.0,
            applied_through: None,
            contiguous: true,
            exact: true,
            coverage_reason: None,
            lineages: BTreeMap::new(),
            continuity_exact: true,
            record_count: 0,
        }
    }

    /// Feed one durable terminal/no-change status while replaying a status
    /// stream in commit-ordinal order.  Once a coverage gap is observed, the
    /// projector deliberately discards later status rows: they cannot make
    /// an already-incomplete stream exact and retaining them would turn a
    /// bounded replay into an unbounded status set.
    pub fn push_terminal_no_change_ordinal(&mut self, ordinal: CommitOrdinal) {
        if !self.contiguous {
            return;
        }
        if ordinal.0 < self.expected {
            return;
        }
        if ordinal.0 > self.expected {
            self.contiguous = false;
            self.coverage_reason.get_or_insert_with(|| {
                format!(
                    "missing commit ordinal {} before terminal no-change {}",
                    self.expected, ordinal.0
                )
            });
            return;
        }
        self.expected = self.expected.saturating_add(1);
        while self
            .terminal_no_change_ordinals
            .remove(&CommitOrdinal(self.expected))
        {
            self.expected = self.expected.saturating_add(1);
        }
    }

    /// Feed one unresolved/pending status from a bounded durable replay page.
    pub fn push_unresolved_ordinal(&mut self, ordinal: CommitOrdinal) {
        if self
            .first_unresolved_ordinal
            .is_none_or(|first| ordinal < first)
        {
            self.first_unresolved_ordinal = Some(ordinal);
        }
        self.contiguous = false;
        self.coverage_reason
            .get_or_insert_with(|| format!("unresolved commit ordinal {}", ordinal.0));
    }

    /// Feed a decoded SQLite intent status. Promoted rows are intentionally
    /// ignored because their immutable applied record is the source log.
    pub fn push_ordinal_status(
        &mut self,
        ordinal: CommitOrdinal,
        status: crate::apply_patch::history::IntentStatus,
    ) {
        match status {
            crate::apply_patch::history::IntentStatus::AppliedNoChange
            | crate::apply_patch::history::IntentStatus::FailedNoChange
            | crate::apply_patch::history::IntentStatus::Rejected => {
                self.push_terminal_no_change_ordinal(ordinal)
            }
            crate::apply_patch::history::IntentStatus::Pending
            | crate::apply_patch::history::IntentStatus::Gap => {
                self.push_unresolved_ordinal(ordinal)
            }
            crate::apply_patch::history::IntentStatus::Promoted => {}
        }
    }

    pub fn push(&mut self, stored: &StoredPatchRecord) -> Result<(), AggregateProjectionError> {
        let record = &stored.record;
        if record.identity.thread_id != self.thread_id || record.identity.turn_id != self.turn_id {
            return Err(AggregateProjectionError::InvalidRecord(
                "record identity does not belong to the projected thread/turn".to_owned(),
            ));
        }
        if self
            .previous_ordinal
            .is_some_and(|previous| record.commit_ordinal <= previous)
        {
            return Err(AggregateProjectionError::ConflictingRecordOrder);
        }
        self.previous_ordinal = Some(record.commit_ordinal);
        while self
            .terminal_no_change_ordinals
            .contains(&CommitOrdinal(self.expected))
        {
            self.expected = self.expected.saturating_add(1);
        }
        if record.commit_ordinal.0 != self.expected {
            self.contiguous = false;
            self.coverage_reason.get_or_insert_with(|| {
                format!(
                    "missing commit ordinal {} before {}",
                    self.expected, record.commit_ordinal.0
                )
            });
        }
        if self.contiguous {
            self.applied_through = Some(record.commit_ordinal);
            self.expected = record.commit_ordinal.0.saturating_add(1);
        }
        if !record.exactness.is_exact() {
            self.exact = false;
            self.coverage_reason
                .get_or_insert_with(|| format!("record {} is not exact", record.commit_ordinal.0));
        }
        for change in &record.changes {
            if change.source_path.trim().is_empty() {
                return Err(AggregateProjectionError::InvalidRecord(
                    "record contains an empty source path".to_owned(),
                ));
            }
            if !apply_change(&mut self.lineages, &record.environment_id, change) {
                self.continuity_exact = false;
                self.coverage_reason.get_or_insert_with(|| {
                    "intervening_untracked_change: committed before-state does not match the tracked predecessor".to_owned()
                });
            }
        }
        self.record_count = self.record_count.saturating_add(1);
        Ok(())
    }

    pub fn finish(mut self) -> Result<TurnAggregate, AggregateProjectionError> {
        while self
            .terminal_no_change_ordinals
            .contains(&CommitOrdinal(self.expected))
        {
            self.expected = self.expected.saturating_add(1);
        }
        if let Some(next_terminal_no_change) = self
            .terminal_no_change_ordinals
            .iter()
            .find(|ordinal| ordinal.0 > self.expected)
        {
            self.contiguous = false;
            self.coverage_reason.get_or_insert_with(|| {
                format!(
                    "missing commit ordinal {} before terminal no-change {}",
                    self.expected, next_terminal_no_change.0
                )
            });
        }
        if let Some(unresolved) = self.first_unresolved_ordinal {
            self.contiguous = false;
            self.coverage_reason
                .get_or_insert_with(|| format!("unresolved commit ordinal {}", unresolved.0));
        }

        let mut changes = self
            .lineages
            .into_values()
            .filter_map(finalize_lineage)
            .collect::<Vec<_>>();
        changes.sort_by(|left, right| {
            left.source_path
                .cmp(&right.source_path)
                .then_with(|| left.destination_path.cmp(&right.destination_path))
        });
        let exact = self.exact && self.contiguous && self.continuity_exact;
        let coverage = if exact {
            PatchHistoryCoverage::EngineVerifiedSteps
        } else {
            PatchHistoryCoverage::Incomplete {
                reason: self
                    .coverage_reason
                    .unwrap_or_else(|| "record coverage is incomplete".to_owned()),
            }
        };
        Ok(TurnAggregate {
            thread_id: self.thread_id,
            turn_id: self.turn_id,
            changes,
            exact,
            coverage,
            applied_through: self.applied_through,
            record_count: self.record_count,
        })
    }
}

/// Return the next monotonic live-projection revision for a turn.  Revisions
/// represent the ordered intent stream, not merely the number of applied
/// records: a rejected/no-change, pending or gap intent still occupies an
/// ordinal and must advance the revision so a later repair cannot collide
/// with an earlier projection carrying the same record count.
pub fn next_turn_projection_revision(
    records: &[StoredPatchRecord],
    terminal_no_change_ordinals: &BTreeSet<CommitOrdinal>,
    unresolved_ordinals: &BTreeSet<CommitOrdinal>,
) -> u64 {
    records
        .iter()
        .map(|stored| stored.record.commit_ordinal.0)
        .chain(terminal_no_change_ordinals.iter().map(|ordinal| ordinal.0))
        .chain(unresolved_ordinals.iter().map(|ordinal| ordinal.0))
        .max()
        .map(|ordinal| ordinal.saturating_add(1))
        .unwrap_or(0)
}

fn apply_change(
    lineages: &mut BTreeMap<(String, String), Lineage>,
    environment_id: &str,
    change: &crate::apply_patch::history::DurablePatchChange,
) -> bool {
    let source_key = (environment_id.to_owned(), change.source_path.clone());
    let mut continuity = true;
    if change.kind == ChangeKind::Move {
        let destination = change
            .destination_path
            .clone()
            .unwrap_or_else(|| change.source_path.clone());
        let previous = lineages.remove(&source_key);
        let mut lineage = previous.clone().unwrap_or_else(|| Lineage {
            environment_id: environment_id.to_owned(),
            original_path: change.source_path.clone(),
            current_path: change.source_path.clone(),
            kind: ChangeKind::Move,
            before: change.before.clone(),
            after: None,
            overwritten_destination: None,
        });
        if previous.is_some_and(|previous| previous.after != change.before) {
            continuity = false;
            lineage.before = change.before.clone();
        }
        let destination_key = (environment_id.to_owned(), destination.clone());
        if let Some(overwritten) = lineages.remove(&destination_key) {
            if overwritten.after != change.overwritten_destination {
                continuity = false;
            }
            // The aggregate represents the complete turn baseline, not only
            // the bytes immediately preceding this move.  If the destination
            // already had a tracked lineage, its first before-state is the
            // destination baseline that the move ultimately overwrites.  It
            // may be `None` when that destination was created earlier in the
            // turn; retain that explicit absence instead of replacing it with
            // the intermediate after-state from the move record.
            lineage.overwritten_destination = overwritten.before;
        } else {
            lineage.overwritten_destination = change.overwritten_destination.clone();
        }
        lineage.current_path = destination.clone();
        lineage.kind = ChangeKind::Move;
        lineage.after = change.after.clone();
        lineages.insert(destination_key, lineage);
        return continuity;
    }

    let key = source_key;
    let existing = lineages.get(&key).cloned();
    let lineage = lineages.entry(key).or_insert_with(|| Lineage {
        environment_id: environment_id.to_owned(),
        original_path: change.source_path.clone(),
        current_path: change.source_path.clone(),
        kind: change.kind,
        before: change.before.clone(),
        after: None,
        overwritten_destination: change.overwritten_destination.clone(),
    });
    if let Some(previous) = existing
        && previous.after != change.before
    {
        continuity = false;
        lineage.before = change.before.clone();
    }
    lineage.after = change.after.clone();
    lineage.kind = change.kind;
    lineage.overwritten_destination = change
        .overwritten_destination
        .clone()
        .or(lineage.overwritten_destination.clone());
    continuity
}

fn finalize_lineage(lineage: Lineage) -> Option<AggregateFileChange> {
    if lineage.before.is_none()
        && lineage.after.is_none()
        && lineage.overwritten_destination.is_none()
    {
        return None;
    }
    if lineage.before == lineage.after
        && lineage.original_path == lineage.current_path
        && lineage.overwritten_destination.is_none()
    {
        return None;
    }
    let (kind, source_path, destination_path) = if lineage.before.is_none() {
        (ChangeKind::Add, lineage.current_path.clone(), None)
    } else if lineage.original_path != lineage.current_path {
        (
            ChangeKind::Move,
            lineage.original_path.clone(),
            Some(lineage.current_path.clone()),
        )
    } else if lineage.after.is_none() {
        (ChangeKind::Delete, lineage.original_path.clone(), None)
    } else {
        (
            match lineage.kind {
                ChangeKind::Add | ChangeKind::Delete | ChangeKind::Move => ChangeKind::Replace,
                other => other,
            },
            lineage.original_path.clone(),
            None,
        )
    };
    Some(AggregateFileChange {
        environment_id: lineage.environment_id,
        kind,
        source_path,
        destination_path,
        before: lineage.before,
        after: lineage.after,
        overwritten_destination: lineage.overwritten_destination,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::history::{
        AppliedPatchRecord, AppliedPatchRecordOutcome, ChangeKind, CommitOrdinal,
        DurablePatchChange, InvocationIdentity, PatchSideEffects, StoredPatchRecord,
    };

    fn snapshot(value: &[u8]) -> TextSnapshotRef {
        crate::apply_patch::history::TextSnapshotRef {
            schema_version: crate::apply_patch::history::SNAPSHOT_REF_SCHEMA_VERSION,
            content_hash: *crate::apply_patch::file_mutation::FileVersionToken::from_bytes(value)
                .digest(),
            byte_len: value.len() as u64,
            encoding: crate::apply_patch::history::TextEncoding::Utf8,
            line_endings: crate::apply_patch::history::LineEndingMetadata::default(),
        }
    }

    fn stored(
        ordinal: u64,
        invocation_id: &str,
        kind: ChangeKind,
        path: &str,
        before: Option<&[u8]>,
        after: Option<&[u8]>,
        destination: Option<&str>,
    ) -> StoredPatchRecord {
        StoredPatchRecord {
            record: AppliedPatchRecord::new(
                InvocationIdentity::new("thread", "turn", invocation_id).unwrap(),
                CommitOrdinal(ordinal),
                AppliedPatchRecordOutcome::Applied,
                vec![DurablePatchChange {
                    operation_index: 0,
                    commit_step: 0,
                    sequence: 0,
                    kind,
                    source_path: path.to_owned(),
                    destination_path: destination.map(str::to_owned),
                    before: before.map(snapshot),
                    after: after.map(snapshot),
                    overwritten_destination: None,
                    side_effects: PatchSideEffects::default(),
                }],
            ),
            plan_fingerprint: [ordinal as u8 + 1; 32],
        }
    }

    #[test]
    fn unified_renderer_emits_hunk_headers_and_no_final_newline_markers() {
        let before = snapshot(b"old\n");
        let after = snapshot(b"new");
        let aggregate = TurnAggregate {
            thread_id: "thread".to_owned(),
            turn_id: "turn".to_owned(),
            changes: vec![AggregateFileChange {
                environment_id: "env".to_owned(),
                kind: ChangeKind::Update,
                source_path: "file.txt".to_owned(),
                destination_path: None,
                before: Some(before.clone()),
                after: Some(after.clone()),
                overwritten_destination: None,
            }],
            exact: true,
            coverage: PatchHistoryCoverage::EngineVerifiedSteps,
            applied_through: Some(CommitOrdinal(0)),
            record_count: 1,
        };
        let rendered = aggregate
            .render_unified_diff(|reference| {
                if reference == &before {
                    Ok(b"old\n".to_vec())
                } else if reference == &after {
                    Ok(b"new".to_vec())
                } else {
                    Err(AggregateProjectionError::Snapshot(
                        "unexpected snapshot".to_owned(),
                    ))
                }
            })
            .unwrap();
        assert_eq!(
            rendered,
            "--- a/file.txt\n+++ b/file.txt\n@@ -1,1 +1,1 @@\n-old\n+new\n\\ No newline at end of file\n"
        );
    }

    #[test]
    fn unified_renderer_keeps_pure_rename_without_a_text_hunk() {
        let snapshot_ref = snapshot(b"same\n");
        let aggregate = TurnAggregate {
            thread_id: "thread".to_owned(),
            turn_id: "turn".to_owned(),
            changes: vec![AggregateFileChange {
                environment_id: "env".to_owned(),
                kind: ChangeKind::Move,
                source_path: "old.txt".to_owned(),
                destination_path: Some("new.txt".to_owned()),
                before: Some(snapshot_ref.clone()),
                after: Some(snapshot_ref.clone()),
                overwritten_destination: None,
            }],
            exact: true,
            coverage: PatchHistoryCoverage::EngineVerifiedSteps,
            applied_through: Some(CommitOrdinal(0)),
            record_count: 1,
        };
        let rendered = aggregate
            .render_unified_diff(|_| Ok(b"same\n".to_vec()))
            .unwrap();
        assert_eq!(rendered, "--- a/old.txt\n+++ b/new.txt\n");
    }

    #[test]
    fn repeated_edits_fold_to_first_before_and_last_after() {
        let records = vec![
            stored(
                0,
                "one",
                ChangeKind::Update,
                "a.txt",
                Some(b"a"),
                Some(b"b"),
                None,
            ),
            stored(
                1,
                "two",
                ChangeKind::Update,
                "a.txt",
                Some(b"b"),
                Some(b"c"),
                None,
            ),
        ];
        let aggregate = project_turn_records("thread", "turn", &records).unwrap();
        assert_eq!(aggregate.changes.len(), 1);
        assert_eq!(aggregate.changes[0].before, Some(snapshot(b"a")));
        assert_eq!(aggregate.changes[0].after, Some(snapshot(b"c")));
    }

    #[test]
    fn add_then_update_remains_one_add_with_final_bytes() {
        let records = vec![
            stored(0, "add", ChangeKind::Add, "a.txt", None, Some(b"a"), None),
            stored(
                1,
                "update",
                ChangeKind::Update,
                "a.txt",
                Some(b"a"),
                Some(b"b"),
                None,
            ),
        ];

        let aggregate = project_turn_records("thread", "turn", &records).unwrap();

        assert_eq!(aggregate.changes.len(), 1);
        assert_eq!(aggregate.changes[0].kind, ChangeKind::Add);
        assert_eq!(aggregate.changes[0].source_path, "a.txt");
        assert_eq!(aggregate.changes[0].before, None);
        assert_eq!(aggregate.changes[0].after, Some(snapshot(b"b")));
    }

    #[test]
    fn add_then_delete_is_net_zero() {
        let records = vec![
            stored(0, "add", ChangeKind::Add, "a.txt", None, Some(b"a"), None),
            stored(
                1,
                "delete",
                ChangeKind::Delete,
                "a.txt",
                Some(b"a"),
                None,
                None,
            ),
        ];

        let aggregate = project_turn_records("thread", "turn", &records).unwrap();

        assert!(aggregate.changes.is_empty());
        assert_eq!(aggregate.record_count, 2);
    }

    #[test]
    fn add_then_move_is_an_add_at_the_final_path() {
        let records = vec![
            stored(0, "add", ChangeKind::Add, "a.txt", None, Some(b"a"), None),
            stored(
                1,
                "move",
                ChangeKind::Move,
                "a.txt",
                Some(b"a"),
                Some(b"a"),
                Some("b.txt"),
            ),
        ];

        let aggregate = project_turn_records("thread", "turn", &records).unwrap();

        assert_eq!(aggregate.changes.len(), 1);
        assert_eq!(aggregate.changes[0].kind, ChangeKind::Add);
        assert_eq!(aggregate.changes[0].source_path, "b.txt");
        assert_eq!(aggregate.changes[0].destination_path, None);
        assert_eq!(aggregate.changes[0].before, None);
        assert_eq!(aggregate.changes[0].after, Some(snapshot(b"a")));
    }

    #[test]
    fn incremental_projector_matches_replay_of_page_concatenation() {
        let records = vec![
            stored(
                0,
                "one",
                ChangeKind::Update,
                "a.txt",
                Some(b"a"),
                Some(b"b"),
                None,
            ),
            stored(
                1,
                "two",
                ChangeKind::Move,
                "a.txt",
                Some(b"b"),
                Some(b"b"),
                Some("b.txt"),
            ),
            stored(
                2,
                "three",
                ChangeKind::Update,
                "b.txt",
                Some(b"b"),
                Some(b"c"),
                None,
            ),
        ];
        let expected = project_turn_records("thread", "turn", &records).unwrap();
        let mut projector =
            TurnRecordProjector::new("thread", "turn", &BTreeSet::new(), &BTreeSet::new());
        for page in records.chunks(1) {
            for record in page {
                projector.push(record).unwrap();
            }
        }
        assert_eq!(projector.finish().unwrap(), expected);
    }

    #[test]
    fn edit_revert_is_net_zero_but_records_remain_counted() {
        let records = vec![
            stored(
                0,
                "one",
                ChangeKind::Update,
                "a.txt",
                Some(b"a"),
                Some(b"b"),
                None,
            ),
            stored(
                1,
                "two",
                ChangeKind::Update,
                "a.txt",
                Some(b"b"),
                Some(b"a"),
                None,
            ),
        ];
        let aggregate = project_turn_records("thread", "turn", &records).unwrap();
        assert!(aggregate.changes.is_empty());
        assert_eq!(aggregate.record_count, 2);
    }

    #[test]
    fn exact_partial_record_keeps_engine_verified_aggregate_coverage() {
        let mut partial = stored(
            0,
            "partial",
            ChangeKind::Update,
            "a.txt",
            Some(b"a"),
            Some(b"b"),
            None,
        );
        partial.record.outcome = AppliedPatchRecordOutcome::Partial {
            failed_stage: crate::apply_patch::file_mutation::PatchStage::Commit,
            error_code: crate::apply_patch::file_mutation::PatchErrorCode::Io,
        };
        partial.record.exactness = crate::apply_patch::history::PatchRecordExactness::Partial;

        let aggregate = project_turn_records("thread", "turn", &[partial]).unwrap();

        assert!(aggregate.exact);
        assert_eq!(
            aggregate.coverage,
            PatchHistoryCoverage::EngineVerifiedSteps
        );
        assert_eq!(aggregate.changes.len(), 1);
    }

    #[test]
    fn empty_gap_marks_coverage_incomplete_without_inventing_a_path() {
        let record = StoredPatchRecord {
            record: AppliedPatchRecord::new(
                InvocationIdentity::new("thread", "turn", "gap").unwrap(),
                CommitOrdinal(0),
                AppliedPatchRecordOutcome::Gap {
                    reason: "unknown crash boundary".to_owned(),
                },
                Vec::new(),
            ),
            plan_fingerprint: [1; 32],
        };

        let aggregate = project_turn_records("thread", "turn", &[record]).unwrap();

        assert!(aggregate.changes.is_empty());
        assert!(!aggregate.exact);
        assert!(matches!(
            aggregate.coverage,
            PatchHistoryCoverage::Incomplete { .. }
        ));
        assert_eq!(aggregate.record_count, 1);
    }

    #[test]
    fn rename_chain_preserves_original_path_and_destination() {
        let records = vec![stored(
            0,
            "one",
            ChangeKind::Move,
            "a.txt",
            Some(b"a"),
            Some(b"a"),
            Some("b.txt"),
        )];
        let aggregate = project_turn_records("thread", "turn", &records).unwrap();
        assert_eq!(aggregate.changes[0].source_path, "a.txt");
        assert_eq!(
            aggregate.changes[0].destination_path.as_deref(),
            Some("b.txt")
        );
    }

    #[test]
    fn move_overwrite_preserves_destination_turn_baseline_after_prior_edit() {
        let records = vec![
            stored(
                0,
                "destination-edit",
                ChangeKind::Update,
                "b.txt",
                Some(b"b0"),
                Some(b"b1"),
                None,
            ),
            stored(
                1,
                "move",
                ChangeKind::Move,
                "a.txt",
                Some(b"a"),
                Some(b"a"),
                Some("b.txt"),
            ),
        ];
        let aggregate = project_turn_records("thread", "turn", &records).unwrap();
        assert_eq!(aggregate.changes.len(), 1);
        let change = &aggregate.changes[0];
        assert_eq!(change.source_path, "a.txt");
        assert_eq!(change.destination_path.as_deref(), Some("b.txt"));
        assert_eq!(change.overwritten_destination, Some(snapshot(b"b0")));
        assert_eq!(change.before, Some(snapshot(b"a")));
        assert_eq!(change.after, Some(snapshot(b"a")));
    }

    #[test]
    fn move_overwrite_of_destination_added_in_turn_keeps_absent_baseline() {
        let records = vec![
            stored(
                0,
                "destination-add",
                ChangeKind::Add,
                "b.txt",
                None,
                Some(b"b1"),
                None,
            ),
            stored(
                1,
                "move",
                ChangeKind::Move,
                "a.txt",
                Some(b"a"),
                Some(b"a"),
                Some("b.txt"),
            ),
        ];
        let aggregate = project_turn_records("thread", "turn", &records).unwrap();
        assert_eq!(aggregate.changes.len(), 1);
        assert_eq!(
            aggregate.changes[0].destination_path.as_deref(),
            Some("b.txt")
        );
        assert_eq!(aggregate.changes[0].overwritten_destination, None);
    }

    #[test]
    fn missing_ordinal_marks_incomplete_coverage() {
        let records = vec![stored(
            1,
            "one",
            ChangeKind::Update,
            "a.txt",
            None,
            Some(b"a"),
            None,
        )];
        let aggregate = project_turn_records("thread", "turn", &records).unwrap();
        assert!(!aggregate.exact);
        assert!(matches!(
            aggregate.coverage,
            PatchHistoryCoverage::Incomplete { .. }
        ));
    }

    #[test]
    fn terminal_no_change_ordinal_does_not_create_a_false_gap() {
        let records = vec![stored(
            1,
            "one",
            ChangeKind::Update,
            "a.txt",
            None,
            Some(b"a"),
            None,
        )];
        let aggregate = project_turn_records_with_ordinal_status(
            "thread",
            "turn",
            &records,
            &BTreeSet::from([CommitOrdinal(0)]),
            &BTreeSet::new(),
        )
        .unwrap();
        assert!(aggregate.exact);
        assert!(matches!(
            aggregate.coverage,
            PatchHistoryCoverage::EngineVerifiedSteps
        ));
    }

    #[test]
    fn isolated_late_terminal_no_change_is_an_incomplete_gap() {
        let aggregate = project_turn_records_with_ordinal_status(
            "thread",
            "turn",
            &[],
            &BTreeSet::from([CommitOrdinal(5)]),
            &BTreeSet::new(),
        )
        .unwrap();
        assert!(!aggregate.exact);
        assert!(matches!(
            aggregate.coverage,
            PatchHistoryCoverage::Incomplete { ref reason }
                if reason.contains("missing commit ordinal 0")
        ));
    }

    #[test]
    fn predecessor_mismatch_marks_aggregate_incomplete_without_claiming_continuity() {
        let records = vec![
            stored(
                0,
                "one",
                ChangeKind::Update,
                "a.txt",
                Some(b"a"),
                Some(b"b"),
                None,
            ),
            stored(
                1,
                "two",
                ChangeKind::Update,
                "a.txt",
                Some(b"external"),
                Some(b"c"),
                None,
            ),
        ];
        let aggregate = project_turn_records("thread", "turn", &records).unwrap();
        assert!(!aggregate.exact);
        assert!(matches!(
            aggregate.coverage,
            PatchHistoryCoverage::Incomplete { ref reason }
                if reason.contains("intervening_untracked_change")
        ));
        assert_eq!(aggregate.changes[0].before, Some(snapshot(b"external")));
    }

    #[test]
    fn same_display_path_in_different_environments_is_not_coalesced() {
        let mut first = stored(
            0,
            "one",
            ChangeKind::Update,
            "a.txt",
            Some(b"a"),
            Some(b"b"),
            None,
        );
        first.record.environment_id = "env-a".to_owned();
        let mut second = stored(
            1,
            "two",
            ChangeKind::Update,
            "a.txt",
            Some(b"x"),
            Some(b"y"),
            None,
        );
        second.record.environment_id = "env-b".to_owned();
        let aggregate = project_turn_records("thread", "turn", &[first, second]).unwrap();
        assert_eq!(aggregate.changes.len(), 2);
        assert_eq!(
            aggregate
                .changes
                .iter()
                .map(|change| change.environment_id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["env-a", "env-b"])
        );
    }
}
