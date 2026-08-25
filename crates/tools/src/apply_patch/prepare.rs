use crate::apply_patch::file_mutation::{
    CanonicalTarget, PatchLimits, SnapshotErrorCode, SnapshotLimits, TargetExpectation, TargetKind,
    TargetManifest, TargetMetadataFingerprint, TargetResolutionError, TargetResolutionErrorCode,
    TargetResolver, TargetRole, TextSnapshot,
};
use crate::apply_patch::{
    OperationBody, OperationKind, PlanError, PlannedPatch, ValidatedPatchDocument, VirtualFile,
    VirtualWorkspace,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrepareOptions {
    pub patch_limits: PatchLimits,
    pub snapshot_limits: SnapshotLimits,
}

impl Default for PrepareOptions {
    fn default() -> Self {
        Self {
            patch_limits: PatchLimits::default(),
            snapshot_limits: SnapshotLimits::default(),
        }
    }
}

impl PrepareOptions {
    pub fn validate(&self) -> Result<(), PrepareError> {
        self.patch_limits.validate().map_err(|error| {
            PrepareError::new(PrepareErrorCode::InvalidLimits, 0, "", error.to_string())
        })?;
        self.snapshot_limits.validate().map_err(|_| {
            PrepareError::new(
                PrepareErrorCode::InvalidLimits,
                0,
                "",
                "snapshot limits are invalid",
            )
        })?;
        if self.snapshot_limits.max_file_bytes > self.patch_limits.max_file_bytes {
            return Err(PrepareError::new(
                PrepareErrorCode::InvalidLimits,
                0,
                "",
                "snapshot max_file_bytes exceeds patch max_file_bytes",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PreparedFileVersion {
    pub token: crate::apply_patch::file_mutation::FileVersionToken,
}

impl From<&VirtualFile> for PreparedFileVersion {
    fn from(file: &VirtualFile) -> Self {
        Self {
            token: file.version,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObservedTarget {
    pub target: CanonicalTarget,
    pub state: Option<PreparedFileVersion>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObservedDirectory {
    pub target: CanonicalTarget,
    pub existed: bool,
    pub fingerprint: TargetMetadataFingerprint,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PreparedPatch {
    pub parser_schema_version: u16,
    pub payload_hash: [u8; 32],
    pub payload_bytes: u64,
    pub snapshot_limits: SnapshotLimits,
    /// The aggregate source-snapshot budget is part of the immutable
    /// preparation contract.  Executor re-plans must enforce the same budget
    /// after an external change instead of silently allowing a larger current
    /// workspace snapshot into memory.
    pub max_total_snapshot_bytes: u64,
    pub max_total_output_bytes: u64,
    pub max_candidate_matches: u32,
    /// The validated, normalized patch document is retained so execution can
    /// re-plan optimistic Update operations against the current file contents
    /// while holding the complete target lock set. Strict guards are checked
    /// again by the pure planner during that re-plan.
    pub document: ValidatedPatchDocument,
    pub target_manifest: TargetManifest,
    pub planned: PlannedPatch,
    pub observed: BTreeMap<String, ObservedTarget>,
    pub observed_parents: BTreeMap<String, ObservedDirectory>,
    pub prepared: BTreeMap<String, PreparedFileVersion>,
    pub total_hunks: u64,
    pub total_output_bytes: u64,
    pub total_snapshot_bytes: u64,
    pub parent_targets: u64,
    pub fingerprint: [u8; 32],
}

impl PreparedPatch {
    pub fn workspace(&self) -> &VirtualWorkspace {
        &self.planned.workspace
    }

    pub fn is_immutable(&self) -> bool {
        self.fingerprint != [0; 32]
    }
}

/// Immutable, read-free permission handoff produced from the canonical parsed
/// patch model. It contains the exact resolved target manifest that approval
/// covers; source bytes are read only after this value has crossed the
/// permission boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedPatch {
    options: PrepareOptions,
    document: ValidatedPatchDocument,
    target_manifest: TargetManifest,
    source_targets: BTreeMap<String, CanonicalTarget>,
    parent_targets: BTreeMap<String, CanonicalTarget>,
    total_hunks: u64,
}

impl ResolvedPatch {
    pub fn document(&self) -> &ValidatedPatchDocument {
        &self.document
    }

    pub fn target_manifest(&self) -> &TargetManifest {
        &self.target_manifest
    }

    pub fn options(&self) -> PrepareOptions {
        self.options
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrepareErrorCode {
    InvalidLimits,
    PathTooLong,
    TargetResolution,
    TargetType,
    ParentType,
    Read,
    TooManyFiles,
    TooManyHunks,
    OutputTooLarge,
    SnapshotTooLarge,
    Planner,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrepareError {
    pub code: PrepareErrorCode,
    pub operation_index: usize,
    pub path: String,
    pub message: String,
    #[serde(skip)]
    plan_code: Option<crate::apply_patch::PlanErrorCode>,
    #[serde(skip)]
    snapshot_code: Option<SnapshotErrorCode>,
    #[serde(skip)]
    target_code: Option<TargetResolutionErrorCode>,
}

impl PrepareError {
    fn new(
        code: PrepareErrorCode,
        operation_index: usize,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            operation_index,
            path: path.into(),
            message: message.into(),
            plan_code: None,
            snapshot_code: None,
            target_code: None,
        }
    }

    pub const fn plan_code(&self) -> Option<crate::apply_patch::PlanErrorCode> {
        self.plan_code
    }

    pub const fn snapshot_code(&self) -> Option<SnapshotErrorCode> {
        self.snapshot_code
    }

    pub const fn target_code(&self) -> Option<TargetResolutionErrorCode> {
        self.target_code
    }

    fn with_snapshot_code(mut self, code: SnapshotErrorCode) -> Self {
        self.snapshot_code = Some(code);
        self
    }

    fn with_target_code(mut self, code: TargetResolutionErrorCode) -> Self {
        self.target_code = Some(code);
        self
    }
}

impl fmt::Display for PrepareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "patch preparation error at operation {} ({}): {}",
            self.operation_index, self.path, self.message
        )
    }
}

impl std::error::Error for PrepareError {}

/// Resolve, read and plan one complete patch. The only external effect is
/// bounded read access to the supplied workspace; no lock or mutation is done.
pub fn prepare(
    document: &ValidatedPatchDocument,
    resolver: &TargetResolver,
    options: PrepareOptions,
) -> Result<PreparedPatch, PrepareError> {
    prepare_resolved(resolve_patch(document, resolver, options)?)
}

/// Resolve the complete canonical permission manifest without reading source
/// file contents or mutating the filesystem.
pub fn resolve_patch(
    document: &ValidatedPatchDocument,
    resolver: &TargetResolver,
    options: PrepareOptions,
) -> Result<ResolvedPatch, PrepareError> {
    options.validate()?;
    validate_guard_limits(document, options.snapshot_limits)?;
    let mut targets = Vec::new();
    let mut normalized = document.clone();
    let mut source_targets = BTreeMap::<String, CanonicalTarget>::new();
    let mut parent_targets = BTreeMap::<String, CanonicalTarget>::new();

    for (operation_index, operation) in normalized.operations.iter_mut().enumerate() {
        check_path_limit(
            &operation.operation.path,
            operation_index,
            &options.patch_limits,
        )?;
        let source_role = if operation.kind() == OperationKind::Add {
            TargetRole::Destination
        } else {
            TargetRole::Source
        };
        let source = resolve_target(
            resolver,
            &operation.operation.path,
            source_role,
            TargetExpectation::ExistingOrMissing,
            operation_index,
        )?;
        add_target(&mut targets, &mut source_targets, source.clone());
        add_parent(
            resolver,
            &mut targets,
            &mut parent_targets,
            &source,
            operation_index,
        )?;
        operation.operation.path = relative_string(&source);

        if let Some(move_to) = operation.operation.move_to.as_mut() {
            check_path_limit(move_to, operation_index, &options.patch_limits)?;
            let destination = resolve_target(
                resolver,
                move_to,
                TargetRole::Destination,
                TargetExpectation::ExistingOrMissing,
                operation_index,
            )?;
            add_target(&mut targets, &mut source_targets, destination.clone());
            add_parent(
                resolver,
                &mut targets,
                &mut parent_targets,
                &destination,
                operation_index,
            )?;
            *move_to = relative_string(&destination);
        }
    }

    // The workspace root is an authorization boundary even when every
    // operation targets a file directly below it and therefore has no
    // lexical parent component in the relative path.  Lock and revalidate it
    // together with ordinary parent directories so a root replacement or
    // symlink swap cannot redirect a later filesystem primitive.
    let root_parent = resolver.root_parent_target();
    let root_key = relative_string(&root_parent);
    if parent_targets
        .insert(root_key, root_parent.clone())
        .is_none()
    {
        targets.push(root_parent);
    }

    if parent_targets.len() as u32 > options.patch_limits.max_parent_targets {
        return Err(PrepareError::new(
            PrepareErrorCode::TooManyFiles,
            0,
            "",
            "parent target limit exceeded",
        ));
    }
    let target_manifest = TargetManifest::new(targets).map_err(|error| {
        PrepareError::new(
            PrepareErrorCode::TargetResolution,
            0,
            "",
            target_error_message(&error),
        )
        .with_target_code(error.code)
    })?;
    if target_manifest.targets().len() as u32 > options.patch_limits.max_target_files {
        return Err(PrepareError::new(
            PrepareErrorCode::TooManyFiles,
            0,
            "",
            "target manifest limit exceeded",
        ));
    }

    let total_hunks = count_hunks(&normalized);
    if total_hunks > options.patch_limits.max_total_hunks as u64 {
        return Err(PrepareError::new(
            PrepareErrorCode::TooManyHunks,
            0,
            "",
            "total hunk limit exceeded",
        ));
    }

    Ok(ResolvedPatch {
        options,
        document: normalized,
        target_manifest,
        source_targets,
        parent_targets,
        total_hunks,
    })
}

/// Read and plan the exact manifest already handed to permission evaluation.
/// No path is parsed or resolved again in this stage.
pub fn prepare_resolved(resolved: ResolvedPatch) -> Result<PreparedPatch, PrepareError> {
    let ResolvedPatch {
        options,
        document: normalized,
        target_manifest,
        source_targets,
        parent_targets,
        total_hunks,
    } = resolved;

    let (observed, observed_parents, initial_files, total_snapshot_bytes) = read_observed_targets(
        &source_targets,
        &parent_targets,
        options.snapshot_limits,
        options.patch_limits.max_total_snapshot_bytes,
    )?;
    let planned = crate::apply_patch::planner::plan_with_limits(
        &normalized,
        initial_files,
        options.patch_limits.max_candidate_matches,
        options.patch_limits.max_total_output_bytes,
    )
    .map_err(plan_error)?;
    let total_output_bytes = planned
        .operations
        .iter()
        .filter_map(|change| change.after.as_ref())
        .try_fold(0u64, |sum, snapshot| {
            sum.checked_add(snapshot.bytes.len() as u64)
        })
        .ok_or_else(|| {
            PrepareError::new(
                PrepareErrorCode::OutputTooLarge,
                0,
                "",
                "output byte count overflow",
            )
        })?;
    if total_output_bytes > options.patch_limits.max_total_output_bytes {
        return Err(PrepareError::new(
            PrepareErrorCode::OutputTooLarge,
            0,
            "",
            "total output limit exceeded",
        ));
    }

    let prepared = planned
        .workspace
        .files()
        .iter()
        .map(|(path, file)| (path.clone(), PreparedFileVersion::from(file)))
        .collect::<BTreeMap<_, _>>();
    let fingerprint = fingerprint(
        &normalized,
        &target_manifest,
        &planned,
        options.patch_limits,
        options.snapshot_limits,
    );
    Ok(PreparedPatch {
        parser_schema_version: normalized.schema_version,
        payload_hash: normalized.payload_hash,
        payload_bytes: normalized.input_bytes,
        snapshot_limits: options.snapshot_limits,
        max_total_snapshot_bytes: options.patch_limits.max_total_snapshot_bytes,
        max_total_output_bytes: options.patch_limits.max_total_output_bytes,
        max_candidate_matches: options.patch_limits.max_candidate_matches,
        document: normalized,
        target_manifest,
        planned,
        observed,
        observed_parents,
        prepared,
        total_hunks,
        total_output_bytes,
        total_snapshot_bytes,
        parent_targets: parent_targets.len() as u64,
        fingerprint,
    })
}

fn validate_guard_limits(
    document: &ValidatedPatchDocument,
    limits: SnapshotLimits,
) -> Result<(), PrepareError> {
    for (operation_index, operation) in document.operations.iter().enumerate() {
        if operation
            .source_guard
            .is_some_and(|token| token.byte_len() > limits.max_file_bytes)
        {
            return Err(PrepareError::new(
                PrepareErrorCode::SnapshotTooLarge,
                operation_index,
                operation.path(),
                "If-Match version token exceeds the configured file-size limit",
            ));
        }
        if operation.destination_guard.is_some_and(|guard| {
            matches!(
                guard,
                crate::apply_patch::DestinationGuard::Exact(token)
                    if token.byte_len() > limits.max_file_bytes
            )
        }) {
            return Err(PrepareError::new(
                PrepareErrorCode::SnapshotTooLarge,
                operation_index,
                operation.path(),
                "If-Destination version token exceeds the configured file-size limit",
            ));
        }
    }
    Ok(())
}

fn read_observed_targets(
    source_targets: &BTreeMap<String, CanonicalTarget>,
    parent_targets: &BTreeMap<String, CanonicalTarget>,
    limits: SnapshotLimits,
    max_total_snapshot_bytes: u64,
) -> Result<
    (
        BTreeMap<String, ObservedTarget>,
        BTreeMap<String, ObservedDirectory>,
        BTreeMap<String, Vec<u8>>,
        u64,
    ),
    PrepareError,
> {
    let mut observed = BTreeMap::new();
    let mut observed_parents = BTreeMap::new();
    let mut initial_files = BTreeMap::new();
    let mut total_snapshot_bytes = 0u64;
    for (path, target) in source_targets {
        let kind = target.inspect_kind().map_err(|error| {
            PrepareError::new(
                PrepareErrorCode::TargetType,
                0,
                path,
                target_error_message(&error),
            )
            .with_target_code(error.code)
        })?;
        let state = match kind {
            TargetKind::Missing => None,
            TargetKind::RegularFile => {
                let snapshot =
                    TextSnapshot::from_file(target.absolute(), limits).map_err(|error| {
                        PrepareError::new(
                            PrepareErrorCode::Read,
                            0,
                            path,
                            snapshot_error_message(&error),
                        )
                        .with_snapshot_code(error.code)
                    })?;
                let snapshot_bytes = snapshot.version.token.byte_len();
                let next_total = total_snapshot_bytes
                    .checked_add(snapshot_bytes)
                    .ok_or_else(|| {
                        PrepareError::new(
                            PrepareErrorCode::SnapshotTooLarge,
                            0,
                            path,
                            "snapshot byte count overflow",
                        )
                    })?;
                if next_total > max_total_snapshot_bytes {
                    return Err(PrepareError::new(
                        PrepareErrorCode::SnapshotTooLarge,
                        0,
                        path,
                        "total observed snapshot limit exceeded",
                    ));
                }
                let bytes = snapshot.bytes().map_err(|error| {
                    PrepareError::new(
                        PrepareErrorCode::Read,
                        0,
                        path,
                        snapshot_error_message(&error),
                    )
                    .with_snapshot_code(error.code)
                })?;
                // This limit bounds the bytes retained by this preparation,
                // not the eventual content-addressed storage footprint. Two
                // different workspace files with identical content still
                // occupy two entries in `initial_files` and must count twice.
                debug_assert_eq!(bytes.len() as u64, snapshot_bytes);
                total_snapshot_bytes = next_total;
                initial_files.insert(path.clone(), bytes);
                Some(PreparedFileVersion {
                    token: snapshot.version.token,
                })
            }
            TargetKind::Directory | TargetKind::Symlink | TargetKind::Special => {
                return Err(PrepareError::new(
                    PrepareErrorCode::TargetType,
                    0,
                    path,
                    "patch target is not a regular text file",
                ));
            }
        };
        observed.insert(
            path.clone(),
            ObservedTarget {
                target: target.clone(),
                state,
            },
        );
    }
    for (path, target) in parent_targets {
        let kind = target.inspect_kind().map_err(|error| {
            PrepareError::new(
                PrepareErrorCode::ParentType,
                0,
                path,
                target_error_message(&error),
            )
            .with_target_code(error.code)
        })?;
        if !matches!(kind, TargetKind::Missing | TargetKind::Directory) {
            return Err(PrepareError::new(
                PrepareErrorCode::ParentType,
                0,
                path,
                "parent target is not a directory",
            ));
        }
        observed_parents.insert(
            path.clone(),
            ObservedDirectory {
                target: target.clone(),
                existed: kind == TargetKind::Directory,
                fingerprint: target.metadata_fingerprint().map_err(|error| {
                    PrepareError::new(
                        PrepareErrorCode::ParentType,
                        0,
                        path,
                        target_error_message(&error),
                    )
                    .with_target_code(error.code)
                })?,
            },
        );
    }
    Ok((
        observed,
        observed_parents,
        initial_files,
        total_snapshot_bytes,
    ))
}

fn resolve_target(
    resolver: &TargetResolver,
    path: &str,
    role: TargetRole,
    expectation: TargetExpectation,
    operation_index: usize,
) -> Result<CanonicalTarget, PrepareError> {
    resolver.resolve(path, role, expectation).map_err(|error| {
        PrepareError::new(
            PrepareErrorCode::TargetResolution,
            operation_index,
            path,
            target_error_message(&error),
        )
        .with_target_code(error.code)
    })
}

fn add_target(
    targets: &mut Vec<CanonicalTarget>,
    by_path: &mut BTreeMap<String, CanonicalTarget>,
    target: CanonicalTarget,
) {
    let path = relative_string(&target);
    if !by_path.contains_key(&path) {
        by_path.insert(path, target.clone());
        targets.push(target);
    }
}

fn add_parent(
    resolver: &TargetResolver,
    targets: &mut Vec<CanonicalTarget>,
    parents: &mut BTreeMap<String, CanonicalTarget>,
    target: &CanonicalTarget,
    operation_index: usize,
) -> Result<(), PrepareError> {
    let mut parent = target.relative().parent();
    while let Some(parent_path) = parent {
        if parent_path.as_os_str().is_empty() {
            break;
        }
        let parent_text = parent_path.to_string_lossy().into_owned();
        let parent_target = resolver
            .resolve(
                &parent_text,
                TargetRole::Parent,
                TargetExpectation::ParentDirectory,
            )
            .map_err(|error| {
                PrepareError::new(
                    PrepareErrorCode::TargetResolution,
                    operation_index,
                    parent_text.clone(),
                    target_error_message(&error),
                )
                .with_target_code(error.code)
            })?;
        if parents.insert(parent_text, parent_target.clone()).is_none() {
            targets.push(parent_target);
        }
        parent = parent_path.parent();
    }
    Ok(())
}

fn relative_string(target: &CanonicalTarget) -> String {
    target.relative().to_string_lossy().replace('\\', "/")
}

fn check_path_limit(
    path: &str,
    operation_index: usize,
    limits: &PatchLimits,
) -> Result<(), PrepareError> {
    if path.len() as u64 > limits.max_path_bytes {
        return Err(PrepareError::new(
            PrepareErrorCode::PathTooLong,
            operation_index,
            path,
            "path exceeds configured byte limit",
        ));
    }
    Ok(())
}

fn count_hunks(document: &ValidatedPatchDocument) -> u64 {
    document
        .operations
        .iter()
        .filter_map(|operation| match &operation.operation.body {
            OperationBody::Update(update) => Some(update.hunks.len() as u64),
            _ => None,
        })
        .sum()
}

fn plan_error(error: PlanError) -> PrepareError {
    let plan_code = error.code;
    let code = match error.code {
        crate::apply_patch::PlanErrorCode::OutputTooLarge => PrepareErrorCode::OutputTooLarge,
        _ => PrepareErrorCode::Planner,
    };
    let mut prepared = PrepareError::new(code, error.operation_index, error.path, error.message);
    prepared.plan_code = Some(plan_code);
    prepared
}

fn fingerprint(
    document: &ValidatedPatchDocument,
    manifest: &TargetManifest,
    planned: &PlannedPatch,
    patch_limits: PatchLimits,
    snapshot_limits: SnapshotLimits,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(document.schema_version.to_le_bytes());
    hasher.update(document.input_bytes.to_le_bytes());
    hasher.update(document.payload_hash);
    hasher.update(planned.input_bytes.to_le_bytes());
    hasher.update(patch_limits.schema_version.to_le_bytes());
    hasher.update(patch_limits.max_patch_bytes.to_le_bytes());
    hasher.update(patch_limits.max_file_bytes.to_le_bytes());
    hasher.update(patch_limits.max_total_output_bytes.to_le_bytes());
    hasher.update(patch_limits.max_total_snapshot_bytes.to_le_bytes());
    hasher.update(patch_limits.max_operations.to_le_bytes());
    hasher.update(patch_limits.max_chunks_per_update.to_le_bytes());
    hasher.update(patch_limits.max_total_hunks.to_le_bytes());
    hasher.update(patch_limits.max_target_files.to_le_bytes());
    hasher.update(patch_limits.max_path_bytes.to_le_bytes());
    hasher.update(patch_limits.max_parent_targets.to_le_bytes());
    hasher.update(patch_limits.max_candidate_matches.to_le_bytes());
    hasher.update(snapshot_limits.max_file_bytes.to_le_bytes());
    hasher.update(snapshot_limits.inline_threshold.to_le_bytes());
    for target in manifest.targets() {
        hasher.update(target.identity().as_bytes());
        hasher.update([0]);
    }
    for change in &planned.operations {
        hasher.update(change.operation_index.to_le_bytes());
        hasher.update([operation_kind_tag(change.kind)]);
        hasher.update(change.source.as_bytes());
        hasher.update([0]);
        if let Some(destination) = &change.destination {
            hasher.update(destination.as_bytes());
        }
        hasher.update([0]);
    }
    hasher.finalize().into()
}

/// The plan fingerprint identifies the immutable request shape and its target
/// set, not the workspace bytes observed while preparing it.  Contextual
/// `Update` operations are intentionally re-planned under the target locks;
/// unrelated edits must therefore not turn an otherwise identical admission
/// into a duplicate/rejection merely because the generated `after` snapshot
/// changed.
fn operation_kind_tag(kind: OperationKind) -> u8 {
    match kind {
        OperationKind::Add => 1,
        OperationKind::Replace => 2,
        OperationKind::Update => 3,
        OperationKind::Delete => 4,
    }
}

fn target_error_message(error: &TargetResolutionError) -> String {
    error.to_string()
}

fn snapshot_error_message(error: &crate::apply_patch::file_mutation::SnapshotError) -> String {
    match error.code {
        SnapshotErrorCode::BinaryContent => "target contains binary content".into(),
        SnapshotErrorCode::InvalidUtf8 => "target is not valid UTF-8".into(),
        SnapshotErrorCode::TooLarge => "target exceeds the file-size limit".into(),
        SnapshotErrorCode::InvalidLimits => "snapshot limits are invalid".into(),
        SnapshotErrorCode::Io
        | SnapshotErrorCode::SpoolUnavailable
        | SnapshotErrorCode::SpoolCorrupt => "target could not be read".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::file_mutation::{FileVersionToken, PatchRequest, PatchRequestSource};
    use crate::apply_patch::{parse, validate_guards};
    use std::fs;

    fn document(text: &str) -> ValidatedPatchDocument {
        let request = PatchRequest::from_provider_text(
            text,
            PatchRequestSource::NativeFreeform,
            PatchLimits::default(),
        )
        .unwrap();
        validate_guards(parse(&request, PatchLimits::default()).unwrap()).unwrap()
    }

    #[test]
    fn preparation_resolves_parents_and_captures_observed_and_prepared_versions() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join("src")).unwrap();
        let path = root.path().join("src/file.txt");
        fs::write(&path, b"old").unwrap();
        let token = FileVersionToken::from_bytes(b"old");
        let patch = format!(
            "*** Begin Patch\n*** Replace File: ./src/file.txt\n*** If-Match: {token}\n+new\n*** End Patch"
        );
        let prepared = prepare(
            &document(&patch),
            &TargetResolver::new(root.path()).unwrap(),
            PrepareOptions::default(),
        )
        .unwrap();
        assert!(prepared.observed.contains_key("src/file.txt"));
        assert!(prepared.prepared.contains_key("src/file.txt"));
        assert!(
            prepared
                .target_manifest
                .targets()
                .iter()
                .any(|target| target.relative() == std::path::Path::new("src"))
        );
        assert_ne!(
            prepared.observed["src/file.txt"]
                .state
                .as_ref()
                .unwrap()
                .token,
            prepared.prepared["src/file.txt"].token
        );
        assert!(prepared.is_immutable());
    }

    #[test]
    fn stale_observed_guard_fails_before_planning_and_disk_is_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file.txt");
        fs::write(&path, b"current").unwrap();
        let stale = FileVersionToken::from_bytes(b"old");
        let patch = format!(
            "*** Begin Patch\n*** Replace File: file.txt\n*** If-Match: {stale}\n+new\n*** End Patch"
        );
        let error = prepare(
            &document(&patch),
            &TargetResolver::new(root.path()).unwrap(),
            PrepareOptions::default(),
        )
        .unwrap_err();
        assert_eq!(error.code, PrepareErrorCode::Planner);
        assert_eq!(
            error.plan_code(),
            Some(crate::apply_patch::PlanErrorCode::StaleSource)
        );
        assert_eq!(fs::read(&path).unwrap(), b"current");
    }

    #[test]
    fn output_limit_is_checked_before_any_mutation() {
        let root = tempfile::tempdir().unwrap();
        let options = PrepareOptions {
            patch_limits: PatchLimits {
                max_total_output_bytes: 2,
                ..PatchLimits::default()
            },
            ..PrepareOptions::default()
        };
        let patch = "*** Begin Patch\n*** Add File: file.txt\n+long\n*** End Patch";
        let error = prepare(
            &document(patch),
            &TargetResolver::new(root.path()).unwrap(),
            options,
        )
        .unwrap_err();
        assert_eq!(error.code, PrepareErrorCode::OutputTooLarge);
        assert!(!root.path().join("file.txt").exists());
    }

    #[test]
    fn aggregate_snapshot_limit_is_enforced_before_copying_next_source() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("first.txt"), b"first\n").unwrap();
        fs::write(root.path().join("second.txt"), b"second\n").unwrap();
        let options = PrepareOptions {
            patch_limits: PatchLimits {
                max_total_snapshot_bytes: 7,
                ..PatchLimits::default()
            },
            ..PrepareOptions::default()
        };
        let patch = "*** Begin Patch\n*** Update File: first.txt\n@@\n-first\n+one\n*** Update File: second.txt\n@@\n-second\n+two\n*** End Patch";
        let error = prepare(
            &document(patch),
            &TargetResolver::new(root.path()).unwrap(),
            options,
        )
        .unwrap_err();
        assert_eq!(error.code, PrepareErrorCode::SnapshotTooLarge);
        assert_eq!(error.path, "second.txt");
    }
}
