use crate::apply_patch::file_mutation::{FileVersionToken, PatchLimits};
use crate::apply_patch::{
    DestinationGuard, MatchErrorCode, OperationBody, OperationKind, ValidatedOperation,
    ValidatedPatchDocument,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Identifies whether a virtual file came from the real workspace or from an
/// earlier operation in this patch. The distinction is what lets the planner
/// apply model guards only to the first real consumption of a file.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum VirtualFileOrigin {
    Real,
    Virtual {
        operation_index: usize,
        previous_path: Option<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VirtualFile {
    pub bytes: Vec<u8>,
    pub version: FileVersionToken,
    pub origin: VirtualFileOrigin,
}

impl VirtualFile {
    fn real(bytes: Vec<u8>) -> Self {
        Self {
            version: FileVersionToken::from_bytes(&bytes),
            bytes,
            origin: VirtualFileOrigin::Real,
        }
    }

    fn virtual_file(bytes: Vec<u8>, operation_index: usize, previous_path: Option<String>) -> Self {
        Self {
            version: FileVersionToken::from_bytes(&bytes),
            bytes,
            origin: VirtualFileOrigin::Virtual {
                operation_index,
                previous_path,
            },
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct VirtualWorkspace {
    files: BTreeMap<String, VirtualFile>,
}

impl VirtualWorkspace {
    pub fn from_files(files: BTreeMap<String, Vec<u8>>) -> Self {
        Self {
            files: files
                .into_iter()
                .map(|(path, bytes)| (path, VirtualFile::real(bytes)))
                .collect(),
        }
    }

    pub fn files(&self) -> &BTreeMap<String, VirtualFile> {
        &self.files
    }

    pub fn get(&self, path: &str) -> Option<&VirtualFile> {
        self.files.get(path)
    }

    pub fn contains(&self, path: &str) -> bool {
        self.files.contains_key(path)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PlannedSnapshot {
    pub bytes: Vec<u8>,
    pub version: FileVersionToken,
}

impl From<&VirtualFile> for PlannedSnapshot {
    fn from(file: &VirtualFile) -> Self {
        Self {
            bytes: file.bytes.clone(),
            version: file.version,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PlannedChange {
    pub operation_index: usize,
    pub kind: OperationKind,
    pub source: String,
    pub destination: Option<String>,
    pub before: Option<PlannedSnapshot>,
    pub after: Option<PlannedSnapshot>,
    pub overwritten_destination: Option<PlannedSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PlannedPatch {
    pub schema_version: u16,
    pub input_bytes: u64,
    pub payload_hash: [u8; 32],
    pub operations: Vec<PlannedChange>,
    pub workspace: VirtualWorkspace,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanErrorCode {
    SourceMissing,
    DestinationExists,
    DestinationMissing,
    MissingSourceGuard,
    StaleSource,
    StaleDestination,
    VirtualGuardOverride,
    SamePathMove,
    MoveCycle,
    ContextNotFound,
    AmbiguousContext,
    OverlappingHunks,
    UnsupportedContent,
    PathCollision,
    OutputTooLarge,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PlanError {
    pub code: PlanErrorCode,
    pub operation_index: usize,
    pub path: String,
    pub message: String,
}

impl PlanError {
    fn new(
        code: PlanErrorCode,
        operation_index: usize,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            operation_index,
            path: path.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "patch planning error at operation {} ({}): {}",
            self.operation_index, self.path, self.message
        )
    }
}

impl std::error::Error for PlanError {}

/// Plan a validated patch against an explicitly supplied workspace snapshot.
/// This function has no filesystem access and never rereads a path after an
/// operation has produced its virtual successor.
pub fn plan(
    document: &ValidatedPatchDocument,
    initial_files: BTreeMap<String, Vec<u8>>,
) -> Result<PlannedPatch, PlanError> {
    plan_with_limits(
        document,
        initial_files,
        PatchLimits::default().max_candidate_matches,
        PatchLimits::default().max_total_output_bytes,
    )
}

pub fn plan_with_candidate_limit(
    document: &ValidatedPatchDocument,
    initial_files: BTreeMap<String, Vec<u8>>,
    max_candidate_matches: u32,
) -> Result<PlannedPatch, PlanError> {
    plan_with_limits(
        document,
        initial_files,
        max_candidate_matches,
        PatchLimits::default().max_total_output_bytes,
    )
}

/// Plan a patch while enforcing the aggregate output budget as each operation
/// is materialized.  Keeping the budget in the pure planner is important: a
/// large sequence of updates must not first accumulate an unbounded vector of
/// `PlannedSnapshot`s and only then be rejected by the filesystem preparation
/// layer.  At most the snapshots for the single rejected operation are
/// transiently present; successful operations are retained only while the
/// aggregate remains within the caller's effective limit.
pub fn plan_with_limits(
    document: &ValidatedPatchDocument,
    initial_files: BTreeMap<String, Vec<u8>>,
    max_candidate_matches: u32,
    max_total_output_bytes: u64,
) -> Result<PlannedPatch, PlanError> {
    let mut workspace = VirtualWorkspace::from_files(initial_files);
    let mut operations = Vec::with_capacity(document.operations.len());
    let mut move_edges = BTreeMap::<String, String>::new();
    let mut total_output_bytes = 0u64;

    for (operation_index, operation) in document.operations.iter().enumerate() {
        let change = plan_operation(
            &mut workspace,
            &mut move_edges,
            operation_index,
            operation,
            max_candidate_matches,
        )?;
        let operation_output_bytes = change
            .after
            .as_ref()
            .map(|snapshot| snapshot.bytes.len() as u64)
            .unwrap_or(0);
        let next_total = total_output_bytes.checked_add(operation_output_bytes);
        let Some(next_total) = next_total else {
            return Err(PlanError::new(
                PlanErrorCode::OutputTooLarge,
                operation_index,
                change.source.clone(),
                "output byte count overflow",
            ));
        };
        if next_total > max_total_output_bytes {
            return Err(PlanError::new(
                PlanErrorCode::OutputTooLarge,
                operation_index,
                change.source.clone(),
                "total output limit exceeded",
            ));
        }
        total_output_bytes = next_total;
        operations.push(change);
    }

    Ok(PlannedPatch {
        schema_version: document.schema_version,
        input_bytes: document.input_bytes,
        payload_hash: document.payload_hash,
        operations,
        workspace,
    })
}

fn plan_operation(
    workspace: &mut VirtualWorkspace,
    move_edges: &mut BTreeMap<String, String>,
    operation_index: usize,
    operation: &ValidatedOperation,
    max_candidate_matches: u32,
) -> Result<PlannedChange, PlanError> {
    let path = operation.path().to_owned();
    match operation.kind() {
        OperationKind::Add => plan_add(workspace, move_edges, operation_index, operation, path),
        OperationKind::Replace => plan_replace(workspace, operation_index, operation, path),
        OperationKind::Update => plan_update(
            workspace,
            move_edges,
            operation_index,
            operation,
            path,
            max_candidate_matches,
        ),
        OperationKind::Delete => {
            plan_delete(workspace, move_edges, operation_index, operation, path)
        }
    }
}

fn plan_add(
    workspace: &mut VirtualWorkspace,
    move_edges: &mut BTreeMap<String, String>,
    operation_index: usize,
    operation: &ValidatedOperation,
    path: String,
) -> Result<PlannedChange, PlanError> {
    if workspace.contains(&path) {
        return Err(PlanError::new(
            PlanErrorCode::DestinationExists,
            operation_index,
            path,
            "Add File destination already exists",
        ));
    }
    let OperationBody::Add(file) = &operation.operation.body else {
        return Err(invalid_body(operation_index, &path));
    };
    let bytes = join_lines(&file.lines);
    let after = VirtualFile::virtual_file(bytes, operation_index, None);
    let snapshot = PlannedSnapshot::from(&after);
    invalidate_move_path(move_edges, &path);
    workspace.files.insert(path.clone(), after);
    Ok(PlannedChange {
        operation_index,
        kind: OperationKind::Add,
        source: path,
        destination: None,
        before: None,
        after: Some(snapshot),
        overwritten_destination: None,
    })
}

fn plan_replace(
    workspace: &mut VirtualWorkspace,
    operation_index: usize,
    operation: &ValidatedOperation,
    path: String,
) -> Result<PlannedChange, PlanError> {
    let before = consume_source(workspace, operation_index, operation, &path)?;
    let OperationBody::Replace(file) = &operation.operation.body else {
        return Err(invalid_body(operation_index, &path));
    };
    // A complete replacement changes the logical lines but keeps the existing
    // text-file framing unless the patch explicitly supplies a different
    // format.  In particular, do not silently turn a CRLF/BOM file into LF
    // without a BOM or drop its final newline.
    let after = VirtualFile::virtual_file(
        render_replacement(&file.lines, &before.bytes),
        operation_index,
        None,
    );
    let before_snapshot = PlannedSnapshot::from(&before);
    let after_snapshot = PlannedSnapshot::from(&after);
    workspace.files.insert(path.clone(), after);
    Ok(PlannedChange {
        operation_index,
        kind: OperationKind::Replace,
        source: path,
        destination: None,
        before: Some(before_snapshot),
        after: Some(after_snapshot),
        overwritten_destination: None,
    })
}

fn plan_delete(
    workspace: &mut VirtualWorkspace,
    move_edges: &mut BTreeMap<String, String>,
    operation_index: usize,
    operation: &ValidatedOperation,
    path: String,
) -> Result<PlannedChange, PlanError> {
    let before = consume_source(workspace, operation_index, operation, &path)?;
    if !matches!(&operation.operation.body, OperationBody::Delete) {
        return Err(invalid_body(operation_index, &path));
    }
    let before_snapshot = PlannedSnapshot::from(&before);
    invalidate_move_path(move_edges, &path);
    workspace.files.remove(&path);
    Ok(PlannedChange {
        operation_index,
        kind: OperationKind::Delete,
        source: path,
        destination: None,
        before: Some(before_snapshot),
        after: None,
        overwritten_destination: None,
    })
}

fn plan_update(
    workspace: &mut VirtualWorkspace,
    move_edges: &mut BTreeMap<String, String>,
    operation_index: usize,
    operation: &ValidatedOperation,
    path: String,
    max_candidate_matches: u32,
) -> Result<PlannedChange, PlanError> {
    let before = consume_source(workspace, operation_index, operation, &path)?;
    let OperationBody::Update(update) = &operation.operation.body else {
        return Err(invalid_body(operation_index, &path));
    };
    let destination = operation.operation.move_to.clone();
    // Detect move cycles before matching the update hunk.  The source may
    // already contain a virtual successor from an earlier move, so matching
    // against that successor first could otherwise mask the structural
    // cycle with an unrelated ContextNotFound error.
    if let Some(destination) = &destination {
        if destination == &path {
            return Err(PlanError::new(
                PlanErrorCode::SamePathMove,
                operation_index,
                path,
                "Move destination must differ from source",
            ));
        }
        if would_cycle(move_edges, &path, destination) {
            return Err(PlanError::new(
                PlanErrorCode::MoveCycle,
                operation_index,
                destination,
                "move chain would create a cycle",
            ));
        }
    }
    let text = String::from_utf8(before.bytes.clone()).map_err(|_| {
        PlanError::new(
            PlanErrorCode::UnsupportedContent,
            operation_index,
            &path,
            "Update File requires UTF-8 text",
        )
    })?;
    let matched = crate::apply_patch::matcher::apply_update_with_candidate_limit(
        &text,
        update,
        max_candidate_matches,
    )
    .map_err(|error| match_error(operation_index, &path, error))?;
    let after_bytes = matched.content.into_bytes();
    let before_snapshot = PlannedSnapshot::from(&before);
    let after = VirtualFile::virtual_file(
        after_bytes,
        operation_index,
        destination.as_ref().map(|_| path.clone()),
    );
    let after_snapshot = PlannedSnapshot::from(&after);

    if let Some(destination) = destination {
        let overwritten = match workspace.files.get(&destination) {
            Some(existing) => {
                if matches!(existing.origin, VirtualFileOrigin::Virtual { .. })
                    && matches!(
                        operation.destination_guard,
                        Some(DestinationGuard::Exact(_))
                    )
                {
                    return Err(PlanError::new(
                        PlanErrorCode::VirtualGuardOverride,
                        operation_index,
                        &destination,
                        "a virtual predecessor is guarded by the planner-derived version",
                    ));
                }
                match operation
                    .destination_guard
                    .unwrap_or(DestinationGuard::MustNotExist)
                {
                    DestinationGuard::MustNotExist => {
                        return Err(PlanError::new(
                            PlanErrorCode::DestinationExists,
                            operation_index,
                            &destination,
                            "move destination already exists; choose a different destination or delete the existing file explicitly before moving",
                        ));
                    }
                    DestinationGuard::Exact(expected) if expected != existing.version => {
                        return Err(PlanError::new(
                            PlanErrorCode::StaleDestination,
                            operation_index,
                            &destination,
                            "move destination version does not match",
                        ));
                    }
                    DestinationGuard::Exact(_) => {}
                }
                Some(PlannedSnapshot::from(existing))
            }
            None => {
                if matches!(
                    operation.destination_guard,
                    Some(DestinationGuard::Exact(_))
                ) {
                    return Err(PlanError::new(
                        PlanErrorCode::DestinationMissing,
                        operation_index,
                        &destination,
                        "exact destination guard requires an existing destination",
                    ));
                }
                None
            }
        };
        // An overwrite replaces the destination's current lineage. Keep the
        // source's incoming lineage so a legitimate rename chain remains
        // visible, but discard historical edges belonging to the destination.
        invalidate_move_path(move_edges, &destination);
        workspace.files.remove(&path);
        workspace.files.insert(destination.clone(), after);
        move_edges.insert(path.clone(), destination.clone());
        Ok(PlannedChange {
            operation_index,
            kind: OperationKind::Update,
            source: path,
            destination: Some(destination),
            before: Some(before_snapshot),
            after: Some(after_snapshot),
            overwritten_destination: overwritten,
        })
    } else {
        workspace.files.insert(path.clone(), after);
        Ok(PlannedChange {
            operation_index,
            kind: OperationKind::Update,
            source: path,
            destination: None,
            before: Some(before_snapshot),
            after: Some(after_snapshot),
            overwritten_destination: None,
        })
    }
}

fn consume_source<'a>(
    workspace: &'a VirtualWorkspace,
    operation_index: usize,
    operation: &ValidatedOperation,
    path: &str,
) -> Result<VirtualFile, PlanError> {
    let Some(source) = workspace.files.get(path) else {
        return Err(PlanError::new(
            PlanErrorCode::SourceMissing,
            operation_index,
            path,
            "source file does not exist in the virtual workspace",
        ));
    };
    let is_real = matches!(source.origin, VirtualFileOrigin::Real);
    if is_real {
        if let Some(expected) = operation.source_guard
            && expected != source.version
        {
            return Err(PlanError::new(
                PlanErrorCode::StaleSource,
                operation_index,
                path,
                "source version does not match",
            ));
        }
    } else if operation.source_guard.is_some() {
        return Err(PlanError::new(
            PlanErrorCode::VirtualGuardOverride,
            operation_index,
            path,
            "a virtual predecessor is guarded by the planner-derived version",
        ));
    }
    Ok(source.clone())
}

fn would_cycle(edges: &BTreeMap<String, String>, source: &str, destination: &str) -> bool {
    let mut current = destination;
    let mut visited = BTreeSet::new();
    while let Some(next) = edges.get(current) {
        if !visited.insert(current.to_owned()) {
            return true;
        }
        if next == source {
            return true;
        }
        current = next;
    }
    false
}

fn invalidate_move_path(edges: &mut BTreeMap<String, String>, path: &str) {
    edges.retain(|source, destination| source != path && destination != path);
}

fn join_lines(lines: &[String]) -> Vec<u8> {
    lines.join("\n").into_bytes()
}

fn render_replacement(lines: &[String], source: &[u8]) -> Vec<u8> {
    let has_bom = source.starts_with(&[0xef, 0xbb, 0xbf]);
    let raw = if has_bom { &source[3..] } else { source };
    let mut lf = 0usize;
    let mut crlf = 0usize;
    let mut lone_cr = 0usize;
    let mut index = 0usize;
    while index < raw.len() {
        match raw[index] {
            b'\r' if raw.get(index + 1) == Some(&b'\n') => {
                crlf = crlf.saturating_add(1);
                index += 2;
            }
            b'\r' => {
                lone_cr = lone_cr.saturating_add(1);
                index += 1;
            }
            b'\n' => {
                lf = lf.saturating_add(1);
                index += 1;
            }
            _ => index += 1,
        }
    }
    let ending = if crlf >= lf && crlf >= lone_cr && crlf > 0 {
        "\r\n"
    } else if lf >= lone_cr && lf > 0 {
        "\n"
    } else if lone_cr > 0 {
        "\r"
    } else {
        "\n"
    };
    let mut rendered = String::new();
    if has_bom {
        rendered.push('\u{feff}');
    }
    rendered.push_str(&lines.join(ending));
    if raw.ends_with(b"\n") || raw.ends_with(b"\r") {
        rendered.push_str(ending);
    }
    rendered.into_bytes()
}

fn invalid_body(operation_index: usize, path: &str) -> PlanError {
    PlanError::new(
        PlanErrorCode::PathCollision,
        operation_index,
        path,
        "operation body does not match its operation kind",
    )
}

fn match_error(
    operation_index: usize,
    path: &str,
    error: crate::apply_patch::MatchError,
) -> PlanError {
    let code = match error.code {
        MatchErrorCode::ContextNotFound => PlanErrorCode::ContextNotFound,
        MatchErrorCode::AmbiguousContext => PlanErrorCode::AmbiguousContext,
        MatchErrorCode::OverlappingHunks => PlanErrorCode::OverlappingHunks,
        MatchErrorCode::InvalidUtf8 | MatchErrorCode::InvalidHunk => {
            PlanErrorCode::UnsupportedContent
        }
    };
    PlanError::new(code, operation_index, path, error.message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::file_mutation::{PatchLimits, PatchRequest, PatchRequestSource};
    use crate::apply_patch::{parse, validate_guards};

    fn document(text: &str) -> ValidatedPatchDocument {
        let request = PatchRequest::from_provider_text(
            text,
            PatchRequestSource::NativeFreeform,
            PatchLimits::default(),
        )
        .unwrap();
        validate_guards(parse(&request, PatchLimits::default()).unwrap()).unwrap()
    }

    fn plan_text(text: &str, files: &[(&str, &str)]) -> Result<PlannedPatch, PlanError> {
        plan(
            &document(text),
            files
                .iter()
                .map(|(path, content)| ((*path).to_owned(), content.as_bytes().to_vec()))
                .collect(),
        )
    }

    #[test]
    fn add_then_update_consumes_virtual_predecessor() {
        let patch = "*** Begin Patch\n*** Add File: new.txt\n+old\n*** Update File: new.txt\n@@\n-old\n+new\n*** End Patch";
        let planned = plan_text(patch, &[]).unwrap();
        assert_eq!(planned.workspace.files()["new.txt"].bytes, b"new");
        assert_eq!(planned.operations.len(), 2);
    }

    #[test]
    fn add_then_replace_uses_planner_version_without_model_token() {
        let patch = "*** Begin Patch\n*** Add File: file.txt\n+middle\n*** Replace File: file.txt\n+final\n*** End Patch";
        let planned = plan_text(patch, &[]).unwrap();
        assert_eq!(planned.workspace.files()["file.txt"].bytes, b"final");
    }

    #[test]
    fn replace_preserves_bom_crlf_and_final_newline() {
        let token = FileVersionToken::from_bytes(b"\xef\xbb\xbfold\r\n");
        let patch = format!(
            "*** Begin Patch\n*** Replace File: file.txt\n*** If-Match: {token}\n+final\n*** End Patch"
        );
        let planned = plan_text(&patch, &[("file.txt", "\u{feff}old\r\n")]).unwrap();
        assert_eq!(
            planned.workspace.files()["file.txt"].bytes,
            b"\xef\xbb\xbffinal\r\n"
        );
    }

    #[test]
    fn delete_then_add_recreates_path() {
        let token = FileVersionToken::from_bytes(b"old");
        let patch = format!(
            "*** Begin Patch\n*** Delete File: file.txt\n*** If-Match: {token}\n*** Add File: file.txt\n+new\n*** End Patch"
        );
        let planned = plan_text(&patch, &[("file.txt", "old")]).unwrap();
        assert_eq!(planned.workspace.files()["file.txt"].bytes, b"new");
    }

    #[test]
    fn move_then_update_uses_destination() {
        let token = FileVersionToken::from_bytes(b"old");
        let patch = format!(
            "*** Begin Patch\n*** Update File: old.txt\n*** If-Match: {token}\n*** Move to: new.txt\n*** If-Destination: absent\n@@\n-old\n+moved\n*** Update File: new.txt\n@@\n-moved\n+final\n*** End Patch"
        );
        let planned = plan_text(&patch, &[("old.txt", "old")]).unwrap();
        assert!(!planned.workspace.contains("old.txt"));
        assert_eq!(planned.workspace.files()["new.txt"].bytes, b"final");
    }

    #[test]
    fn move_to_virtual_destination_rejects_model_exact_guard() {
        let source_token = FileVersionToken::from_bytes(b"source");
        let virtual_destination_token = FileVersionToken::from_bytes(b"destination");
        let patch = format!(
            "*** Begin Patch\n*** Add File: destination.txt\n+destination\n*** Update File: source.txt\n*** If-Match: {source_token}\n*** Move to: destination.txt\n*** If-Destination: {virtual_destination_token}\n@@\n-source\n+moved\n*** End Patch"
        );
        let error = plan_text(&patch, &[("source.txt", "source")]).unwrap_err();
        assert_eq!(error.code, PlanErrorCode::VirtualGuardOverride);
    }

    #[test]
    fn aggregate_output_limit_is_enforced_during_planning() {
        let patch = "*** Begin Patch\n*** Add File: first.txt\n+12\n*** Add File: second.txt\n+34\n*** End Patch";
        let error = plan_with_limits(
            &document(patch),
            BTreeMap::new(),
            PatchLimits::default().max_candidate_matches,
            3,
        )
        .unwrap_err();
        assert_eq!(error.code, PlanErrorCode::OutputTooLarge);
        assert_eq!(error.operation_index, 1);
        assert_eq!(error.path, "second.txt");
    }

    #[test]
    fn move_cycle_is_rejected_without_mutation() {
        let token_a = FileVersionToken::from_bytes(b"a");
        let token_b = FileVersionToken::from_bytes(b"b");
        let patch = format!(
            "*** Begin Patch\n*** Update File: a\n*** If-Match: {token_a}\n*** Move to: b\n*** If-Destination: {token_b}\n@@\n-a\n+a2\n*** Update File: b\n*** Move to: a\n*** If-Destination: absent\n@@\n-b\n+b2\n*** End Patch"
        );
        let error = plan_text(&patch, &[("a", "a"), ("b", "b")]).unwrap_err();
        assert_eq!(error.code, PlanErrorCode::MoveCycle);
    }

    #[test]
    fn reusing_a_deleted_path_does_not_keep_stale_move_cycle_edges() {
        let token = FileVersionToken::from_bytes(b"a");
        let patch = format!(
            "*** Begin Patch\n*** Update File: a\n*** If-Match: {token}\n*** Move to: b\n*** If-Destination: absent\n@@\n-a\n+b\n*** Delete File: b\n*** Add File: b\n+fresh\n*** Update File: b\n*** Move to: a\n*** If-Destination: absent\n@@\n-fresh\n+final\n*** End Patch"
        );
        let planned = plan_text(&patch, &[("a", "a")]).unwrap();
        assert_eq!(planned.workspace.files()["a"].bytes, b"final");
        assert!(!planned.workspace.contains("b"));
    }
}
