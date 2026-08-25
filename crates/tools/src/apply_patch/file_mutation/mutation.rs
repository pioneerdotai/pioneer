use crate::apply_patch::file_mutation::secure_fs::{CreatedDirectories, StagedFile};
use crate::apply_patch::file_mutation::{
    CanonicalTarget, CasError, CasErrorCode, CasExpectation, CasRole, DurabilityOptions,
    FaultPoint, FileContentVersion, FileVersionToken, SnapshotEncoding, SnapshotError,
    SnapshotErrorCode, SnapshotLimits, SnapshotLineEndings, TargetLockGuard, TargetLockRegistry,
    TargetManifest, TextSnapshot, validate_cas, version_on_disk,
};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MutationOptions {
    pub lock_timeout: Duration,
    pub snapshot_limits: SnapshotLimits,
    pub durability: DurabilityOptions,
}

impl Default for MutationOptions {
    fn default() -> Self {
        Self {
            lock_timeout: Duration::from_secs(2),
            snapshot_limits: SnapshotLimits::default(),
            durability: DurabilityOptions::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationKind {
    Add,
    Replace,
    Delete,
    Move,
}

/// Metadata treatment frozen while replacement bytes are still private.
///
/// A patch executor can prepare every replacement before publishing any of
/// them.  Keeping the intent with the stage prevents the later commit loop
/// from silently choosing a different metadata policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StageMetadata {
    SafeAdd,
    PreserveSupportedMode {
        mode: Option<u32>,
        source_path: Option<PathBuf>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MutationSnapshot {
    pub version: FileContentVersion,
    pub bytes: Vec<u8>,
    pub encoding: SnapshotEncoding,
    pub line_endings: SnapshotLineEndings,
}

impl MutationSnapshot {
    fn from_text(snapshot: &TextSnapshot) -> Result<Self, MutationError> {
        Ok(Self {
            version: snapshot.version,
            bytes: snapshot.bytes().map_err(MutationError::snapshot)?.to_vec(),
            encoding: snapshot.encoding,
            line_endings: snapshot.line_endings,
        })
    }

    fn from_bytes(bytes: Vec<u8>, limits: SnapshotLimits) -> Result<Self, MutationError> {
        let snapshot = TextSnapshot::from_bytes(bytes, limits).map_err(MutationError::snapshot)?;
        Self::from_text(&snapshot)
    }
}

#[derive(Debug)]
pub struct MutationChange {
    pub kind: MutationKind,
    pub source: CanonicalTarget,
    pub destination: Option<CanonicalTarget>,
    pub before: Option<MutationSnapshot>,
    pub after: Option<MutationSnapshot>,
    pub overwritten_destination: Option<MutationSnapshot>,
    /// Directories created as part of this successful primitive.  They are
    /// intentionally carried with the change instead of being inferred by a
    /// later workspace scan: the patch journal must describe every filesystem
    /// side effect produced by the commit.
    pub side_effects: MutationSideEffects,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationSideEffects {
    pub created_directories: Vec<PathBuf>,
    pub residual_directories: Vec<PathBuf>,
    pub metadata_warnings: Vec<crate::apply_patch::file_mutation::MetadataWarning>,
    pub exact: bool,
}

impl MutationSideEffects {
    pub fn merge(&mut self, other: &Self) {
        self.exact &= other.exact;
        self.created_directories
            .extend(other.created_directories.iter().cloned());
        self.residual_directories
            .extend(other.residual_directories.iter().cloned());
        self.metadata_warnings
            .extend(other.metadata_warnings.iter().copied());
        self.created_directories.sort();
        self.created_directories.dedup();
        self.residual_directories.sort();
        self.residual_directories.dedup();
        self.metadata_warnings
            .sort_by_key(|warning| warning.as_str());
        self.metadata_warnings.dedup();
    }
}

impl Default for MutationSideEffects {
    fn default() -> Self {
        Self {
            created_directories: Vec::new(),
            residual_directories: Vec::new(),
            metadata_warnings: Vec::new(),
            exact: true,
        }
    }
}

#[derive(Debug)]
pub enum MutationOutcome {
    Applied(MutationChange),
    Failed {
        error: MutationError,
        committed: Option<MutationChange>,
    },
    Uncertain {
        error: MutationError,
        committed: Option<MutationChange>,
    },
}

impl MutationOutcome {
    pub fn committed(&self) -> Option<&MutationChange> {
        match self {
            Self::Applied(change)
            | Self::Failed {
                committed: Some(change),
                ..
            }
            | Self::Uncertain {
                committed: Some(change),
                ..
            } => Some(change),
            Self::Failed {
                committed: None, ..
            }
            | Self::Uncertain {
                committed: None, ..
            } => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationErrorCode {
    Lock,
    Cas,
    TargetMissing,
    TargetExists,
    CrossDevice,
    NotRegularFile,
    ParentCreation,
    StageCreate,
    StageWrite,
    Sync,
    Rename,
    Delete,
    Metadata,
    Uncertain,
    Snapshot,
}

#[derive(Debug)]
pub struct MutationError {
    pub code: MutationErrorCode,
    pub cas: Option<CasErrorCode>,
    pub source: Option<io::Error>,
    pub side_effects: MutationSideEffects,
}

impl MutationError {
    fn new(code: MutationErrorCode) -> Self {
        Self {
            code,
            cas: None,
            source: None,
            side_effects: MutationSideEffects::default(),
        }
    }

    fn cas(error: CasError) -> Self {
        Self {
            code: MutationErrorCode::Cas,
            cas: Some(error.code),
            source: None,
            side_effects: MutationSideEffects::default(),
        }
    }

    fn io(code: MutationErrorCode, source: io::Error) -> Self {
        Self {
            code,
            cas: None,
            source: Some(source),
            side_effects: MutationSideEffects::default(),
        }
    }

    fn snapshot(error: SnapshotError) -> Self {
        Self {
            code: MutationErrorCode::Snapshot,
            cas: matches!(
                error.code,
                SnapshotErrorCode::BinaryContent
                    | SnapshotErrorCode::InvalidUtf8
                    | SnapshotErrorCode::TooLarge
            )
            .then_some(CasErrorCode::UnsupportedContent),
            source: error.source,
            side_effects: MutationSideEffects::default(),
        }
    }

    fn with_side_effects(mut self, side_effects: MutationSideEffects) -> Self {
        self.side_effects = side_effects;
        self
    }
}

impl std::fmt::Display for MutationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mutation failed: {:?}", self.code)
    }
}

impl std::error::Error for MutationError {}

/// An opaque, same-directory replacement stage which has already been fully
/// written, metadata-adjusted and flushed.  Dropping an unused stage removes
/// its private name and any empty parents created solely for it.
#[derive(Debug)]
pub struct PreparedFileStage {
    target_identity: String,
    after: MutationSnapshot,
    staged: Option<StagedFile>,
    parents: Option<CreatedDirectories>,
    metadata_warnings: Vec<crate::apply_patch::file_mutation::MetadataWarning>,
    resulting_mode: Option<u32>,
    durability: DurabilityOptions,
}

impl PreparedFileStage {
    pub fn target_identity(&self) -> &str {
        &self.target_identity
    }

    /// Supported mode of the private stage after applying the frozen policy.
    /// This lets a sequential virtual plan carry mode semantics through an
    /// add/update/move chain before any stage is published.
    pub fn resulting_mode(&self) -> Option<u32> {
        self.resulting_mode
    }

    /// Explicit cleanup used by the batch executor so cleanup failures remain
    /// observable instead of being lost in `Drop`.
    pub fn abort(mut self) -> MutationSideEffects {
        self.abort_in_place()
    }

    fn abort_in_place(&mut self) -> MutationSideEffects {
        let mut side_effects = MutationSideEffects::default();
        if let Some(staged) = self.staged.take() {
            let inject_cleanup_failure = self.durability.faults.check(FaultPoint::Cleanup).is_err();
            if !staged.cleanup(inject_cleanup_failure) {
                push_cleanup_warning(&mut side_effects.metadata_warnings);
                side_effects.exact = false;
            }
        }
        if let Some(parents) = self.parents.take() {
            let (mut created, mut residual) = parents.cleanup();
            sort_deepest_first(&mut created);
            sort_deepest_first(&mut residual);
            side_effects.created_directories = created;
            side_effects.residual_directories = residual;
            side_effects.exact &= side_effects.residual_directories.is_empty();
        }
        side_effects
    }

    fn into_parts(mut self, target: &CanonicalTarget) -> Result<PreparedStageParts, MutationError> {
        if self.target_identity != target.identity() {
            let side_effects = self.abort_in_place();
            return Err(
                MutationError::new(MutationErrorCode::StageWrite).with_side_effects(side_effects)
            );
        }
        Ok(PreparedStageParts {
            after: self.after.clone(),
            staged: self
                .staged
                .take()
                .expect("prepared file stage retains its private file"),
            parents: self
                .parents
                .take()
                .expect("prepared file stage retains parent ownership"),
            metadata_warnings: std::mem::take(&mut self.metadata_warnings),
        })
    }
}

impl Drop for PreparedFileStage {
    fn drop(&mut self) {
        let _ = self.abort_in_place();
    }
}

struct PreparedStageParts {
    after: MutationSnapshot,
    staged: StagedFile,
    parents: CreatedDirectories,
    metadata_warnings: Vec<crate::apply_patch::file_mutation::MetadataWarning>,
}

#[derive(Clone, Debug)]
pub struct FileMutationEngine {
    locks: TargetLockRegistry,
    options: MutationOptions,
    stage_attempts: Arc<AtomicU32>,
}

impl FileMutationEngine {
    pub fn new(options: MutationOptions) -> Self {
        Self {
            locks: TargetLockRegistry::new(),
            options,
            stage_attempts: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn with_registry(options: MutationOptions, locks: TargetLockRegistry) -> Self {
        Self {
            locks,
            options,
            stage_attempts: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn lock_registry(&self) -> &TargetLockRegistry {
        &self.locks
    }

    /// Prepare one complete replacement stage while the caller holds the
    /// patch's full target lock set.  This performs every fallible private-file
    /// operation (create, write, metadata application and file sync) without
    /// publishing the destination name.
    pub fn prepare_file_stage_locked(
        &self,
        target: CanonicalTarget,
        bytes: Vec<u8>,
        metadata: StageMetadata,
        _lock: &TargetLockGuard,
    ) -> Result<PreparedFileStage, MutationError> {
        let stage_attempt = self.stage_attempts.fetch_add(1, Ordering::Relaxed) + 1;
        if self.options.durability.faults.fail_stage_attempt == Some(stage_attempt) {
            return Err(MutationError::io(
                MutationErrorCode::StageWrite,
                io::Error::other(format!(
                    "durability fault injected at private stage attempt {stage_attempt}"
                )),
            ));
        }
        let after = MutationSnapshot::from_bytes(bytes, self.options.snapshot_limits)?;
        let parents = ensure_parents(&target).map_err(|error| {
            MutationError::io(MutationErrorCode::ParentCreation, error.source)
                .with_side_effects(error.side_effects)
        })?;
        let staged = match stage_bytes(
            &target,
            &after.bytes,
            self.options.durability.faults,
            self.options.durability.sync_file,
        ) {
            Ok(staged) => staged,
            Err(error) => return Err(cleanup_error(error, parents)),
        };
        let (source_mode, source_path) = match metadata {
            StageMetadata::SafeAdd => (None, None),
            StageMetadata::PreserveSupportedMode { mode, source_path } => (mode, source_path),
        };
        let metadata_warnings = match apply_staged_metadata(
            &staged,
            source_path.as_deref(),
            source_mode,
            self.options.durability,
        ) {
            Ok(warnings) => warnings,
            Err(error) => {
                return Err(cleanup_error(
                    cleanup_staged_error(error, staged, self.options.durability),
                    parents,
                ));
            }
        };
        // Applying mode metadata dirties the private inode after the initial
        // content sync. Flush once more so "prepared" means both bytes and the
        // supported metadata are durable before the first visible publish.
        if self.options.durability.sync_file
            && let Err(error) = staged.sync_all()
        {
            return Err(cleanup_error(
                cleanup_staged_error(
                    MutationError::io(MutationErrorCode::Sync, error),
                    staged,
                    self.options.durability,
                ),
                parents,
            ));
        }
        let resulting_mode = match staged_supported_mode(&staged) {
            Ok(mode) => mode,
            Err(error) => {
                return Err(cleanup_error(
                    cleanup_staged_error(
                        MutationError::io(MutationErrorCode::Metadata, error),
                        staged,
                        self.options.durability,
                    ),
                    parents,
                ));
            }
        };
        Ok(PreparedFileStage {
            target_identity: target.identity().to_owned(),
            after,
            staged: Some(staged),
            parents: Some(parents),
            metadata_warnings,
            resulting_mode,
            durability: self.options.durability,
        })
    }

    pub fn create(&self, target: CanonicalTarget, bytes: Vec<u8>) -> MutationOutcome {
        let manifest = one_target_manifest(target.clone());
        let _lock = match self.locks.acquire(&manifest, self.options.lock_timeout) {
            Ok(lock) => lock,
            Err(_) => return failed(MutationError::new(MutationErrorCode::Lock)),
        };
        self.create_locked(target, bytes, &_lock)
    }

    /// Applies an add while the caller holds a lock covering `target`.
    /// Keeping this entry point separate prevents a complete patch lock set
    /// from being reacquired (and deadlocking) for every operation.
    pub fn create_locked(
        &self,
        target: CanonicalTarget,
        bytes: Vec<u8>,
        lock: &TargetLockGuard,
    ) -> MutationOutcome {
        let current = match version_on_disk(&target, self.options.snapshot_limits) {
            Ok(current) => current,
            Err(error) => return failed(MutationError::cas(error)),
        };
        if let Err(error) =
            validate_cas(CasExpectation::MustNotExist, current, CasRole::Destination)
        {
            return failed(MutationError::cas(error));
        }
        let prepared = match self.prepare_file_stage_locked(
            target.clone(),
            bytes,
            StageMetadata::SafeAdd,
            lock,
        ) {
            Ok(prepared) => prepared,
            Err(error) => return failed(error),
        };
        self.create_prepared_locked(target, prepared, lock)
    }

    /// Publish a previously prepared create stage.  The destination CAS is
    /// checked again at the actual commit boundary.
    pub fn create_prepared_locked(
        &self,
        target: CanonicalTarget,
        prepared: PreparedFileStage,
        _lock: &TargetLockGuard,
    ) -> MutationOutcome {
        let current = match version_on_disk(&target, self.options.snapshot_limits) {
            Ok(current) => current,
            Err(error) => return failed_with_aborted_stage(MutationError::cas(error), prepared),
        };
        if let Err(error) =
            validate_cas(CasExpectation::MustNotExist, current, CasRole::Destination)
        {
            return failed_with_aborted_stage(MutationError::cas(error), prepared);
        }
        let PreparedStageParts {
            after,
            staged,
            parents,
            mut metadata_warnings,
        } = match prepared.into_parts(&target) {
            Ok(parts) => parts,
            Err(error) => return failed(error),
        };
        if let Err(error) =
            persist_stage_no_replace(staged, self.options.durability, &mut metadata_warnings)
        {
            if error.code == MutationErrorCode::Uncertain {
                let created_directories = parents.paths();
                let error =
                    attach_metadata_warnings(cleanup_error(error, parents), &metadata_warnings);
                return uncertain(
                    error,
                    MutationChange {
                        kind: MutationKind::Add,
                        source: target.clone(),
                        destination: None,
                        before: None,
                        after: Some(after.clone()),
                        overwritten_destination: None,
                        side_effects: MutationSideEffects {
                            created_directories,
                            ..side_effects_with_metadata(&metadata_warnings)
                        },
                    },
                );
            }
            return failed(attach_metadata_warnings(
                cleanup_error(error, parents),
                &metadata_warnings,
            ));
        }
        applied(MutationChange {
            kind: MutationKind::Add,
            source: target,
            destination: None,
            before: None,
            after: Some(after),
            overwritten_destination: None,
            side_effects: MutationSideEffects {
                created_directories: parents.into_paths(),
                residual_directories: Vec::new(),
                exact: side_effects_exact(&metadata_warnings),
                metadata_warnings,
            },
        })
    }

    pub fn replace(
        &self,
        target: CanonicalTarget,
        expected: FileVersionToken,
        bytes: Vec<u8>,
    ) -> MutationOutcome {
        let manifest = one_target_manifest(target.clone());
        let _lock = match self.locks.acquire(&manifest, self.options.lock_timeout) {
            Ok(lock) => lock,
            Err(_) => return failed(MutationError::new(MutationErrorCode::Lock)),
        };
        self.replace_locked(target, expected, bytes, &_lock)
    }

    /// Applies a replacement while the caller holds a lock covering `target`.
    pub fn replace_locked(
        &self,
        target: CanonicalTarget,
        expected: FileVersionToken,
        bytes: Vec<u8>,
        lock: &TargetLockGuard,
    ) -> MutationOutcome {
        let before = match read_existing(&target, self.options.snapshot_limits) {
            Ok(before) => before,
            Err(error) => return failed(error),
        };
        let mode = match self.options.durability.metadata {
            crate::apply_patch::file_mutation::MetadataPolicy::PreserveSupportedMode => {
                match crate::apply_patch::file_mutation::supported_mode(target.absolute()) {
                    Ok(mode) => mode,
                    Err(error) => {
                        return failed(MutationError::io(MutationErrorCode::Metadata, error));
                    }
                }
            }
            crate::apply_patch::file_mutation::MetadataPolicy::SafeAddModeOnly => None,
        };
        if let Err(error) = validate_cas(
            CasExpectation::Exact(expected),
            Some(before.version.token),
            CasRole::Source,
        ) {
            return failed(MutationError::cas(error));
        }
        let prepared = match self.prepare_file_stage_locked(
            target.clone(),
            bytes,
            StageMetadata::PreserveSupportedMode {
                mode,
                source_path: Some(target.absolute().to_path_buf()),
            },
            lock,
        ) {
            Ok(prepared) => prepared,
            Err(error) => return failed(error),
        };
        self.replace_prepared_locked(target, expected, prepared, lock)
    }

    /// Publish a previously prepared replacement stage after a final content
    /// CAS against the actual destination.
    pub fn replace_prepared_locked(
        &self,
        target: CanonicalTarget,
        expected: FileVersionToken,
        prepared: PreparedFileStage,
        _lock: &TargetLockGuard,
    ) -> MutationOutcome {
        let before = match read_existing(&target, self.options.snapshot_limits) {
            Ok(before) => before,
            Err(error) => return failed_with_aborted_stage(error, prepared),
        };
        if let Err(error) = validate_cas(
            CasExpectation::Exact(expected),
            Some(before.version.token),
            CasRole::Source,
        ) {
            return failed_with_aborted_stage(MutationError::cas(error), prepared);
        }
        let PreparedStageParts {
            after,
            staged,
            parents,
            mut metadata_warnings,
        } = match prepared.into_parts(&target) {
            Ok(parts) => parts,
            Err(error) => return failed(error),
        };
        if let Err(error) = revalidate_exact(&target, expected, self.options.snapshot_limits) {
            return failed(cleanup_error(
                cleanup_staged_error(error, staged, self.options.durability),
                parents,
            ));
        }
        if let Err(error) = persist_stage(staged, self.options.durability, &mut metadata_warnings) {
            let change = MutationChange {
                kind: MutationKind::Replace,
                source: target.clone(),
                destination: None,
                before: Some(before.clone()),
                after: Some(after.clone()),
                overwritten_destination: None,
                side_effects: side_effects_with_metadata(&metadata_warnings),
            };
            let error = attach_metadata_warnings(cleanup_error(error, parents), &metadata_warnings);
            return if error.code == MutationErrorCode::Uncertain {
                uncertain(error, change)
            } else {
                failed(error)
            };
        }
        applied(MutationChange {
            kind: MutationKind::Replace,
            source: target,
            destination: None,
            before: Some(before),
            after: Some(after),
            overwritten_destination: None,
            side_effects: MutationSideEffects {
                created_directories: parents.into_paths(),
                exact: side_effects_exact(&metadata_warnings),
                metadata_warnings,
                ..MutationSideEffects::default()
            },
        })
    }

    pub fn delete(&self, target: CanonicalTarget, expected: FileVersionToken) -> MutationOutcome {
        let manifest = one_target_manifest(target.clone());
        let _lock = match self.locks.acquire(&manifest, self.options.lock_timeout) {
            Ok(lock) => lock,
            Err(_) => return failed(MutationError::new(MutationErrorCode::Lock)),
        };
        self.delete_locked(target, expected, &_lock)
    }

    /// Applies a deletion while the caller holds a lock covering `target`.
    pub fn delete_locked(
        &self,
        target: CanonicalTarget,
        expected: FileVersionToken,
        _lock: &TargetLockGuard,
    ) -> MutationOutcome {
        let metadata_warnings = crate::apply_patch::file_mutation::directory_durability_warnings();
        let before = match read_existing(&target, self.options.snapshot_limits) {
            Ok(before) => before,
            Err(error) => return failed(error),
        };
        if let Err(error) = validate_cas(
            CasExpectation::Exact(expected),
            Some(before.version.token),
            CasRole::Source,
        ) {
            return failed(MutationError::cas(error));
        }
        if let Err(error) = self.options.durability.faults.check(FaultPoint::Delete) {
            return failed(MutationError::io(MutationErrorCode::Delete, error.into()));
        }
        if let Err(error) = revalidate_exact(&target, expected, self.options.snapshot_limits) {
            return failed(error);
        }
        let removed_parent =
            match crate::apply_patch::file_mutation::secure_fs::remove_regular_file(&target) {
                Ok(parent) => parent,
                Err(error) => {
                    return failed(MutationError::io(MutationErrorCode::Delete, error));
                }
            };
        if self.options.durability.sync_parent {
            let sync_result = self
                .options
                .durability
                .faults
                .check(FaultPoint::ParentSync)
                .map_err(io::Error::from)
                .and_then(|_| removed_parent.sync_all());
            if let Err(error) = sync_result {
                return uncertain(
                    MutationError::io(MutationErrorCode::Delete, error),
                    MutationChange {
                        kind: MutationKind::Delete,
                        source: target,
                        destination: None,
                        before: Some(before),
                        after: None,
                        overwritten_destination: None,
                        side_effects: MutationSideEffects {
                            metadata_warnings: metadata_warnings.clone(),
                            ..MutationSideEffects::default()
                        },
                    },
                );
            }
        }
        applied(MutationChange {
            kind: MutationKind::Delete,
            source: target,
            destination: None,
            before: Some(before),
            after: None,
            overwritten_destination: None,
            side_effects: MutationSideEffects {
                metadata_warnings,
                ..MutationSideEffects::default()
            },
        })
    }

    pub fn move_file(
        &self,
        source: CanonicalTarget,
        expected_source: FileVersionToken,
        destination: CanonicalTarget,
        destination_expectation: CasExpectation,
        replacement_bytes: Option<Vec<u8>>,
    ) -> MutationOutcome {
        let manifest = match TargetManifest::new(vec![source.clone(), destination.clone()]) {
            Ok(manifest) => manifest,
            Err(_) => return failed(MutationError::new(MutationErrorCode::TargetExists)),
        };
        let _lock = match self.locks.acquire(&manifest, self.options.lock_timeout) {
            Ok(lock) => lock,
            Err(_) => return failed(MutationError::new(MutationErrorCode::Lock)),
        };
        self.move_file_locked(
            source,
            expected_source,
            destination,
            destination_expectation,
            replacement_bytes,
            &_lock,
        )
    }

    /// Applies a move while the caller holds a lock covering both paths.
    pub fn move_file_locked(
        &self,
        source: CanonicalTarget,
        expected_source: FileVersionToken,
        destination: CanonicalTarget,
        destination_expectation: CasExpectation,
        replacement_bytes: Option<Vec<u8>>,
        lock: &TargetLockGuard,
    ) -> MutationOutcome {
        if source.identity() == destination.identity() {
            return failed(MutationError::new(MutationErrorCode::TargetExists));
        }
        let before = match read_existing(&source, self.options.snapshot_limits) {
            Ok(before) => before,
            Err(error) => return failed(error),
        };
        if self
            .options
            .durability
            .faults
            .check(FaultPoint::CrossDevice)
            .is_err()
        {
            return failed(MutationError::new(MutationErrorCode::CrossDevice));
        }
        match same_filesystem(source.absolute(), destination.absolute()) {
            Ok(true) => {}
            Ok(false) => return failed(MutationError::new(MutationErrorCode::CrossDevice)),
            Err(error) => {
                return failed(MutationError::io(MutationErrorCode::Metadata, error));
            }
        }
        if let Err(error) = validate_cas(
            CasExpectation::Exact(expected_source),
            Some(before.version.token),
            CasRole::Source,
        ) {
            return failed(MutationError::cas(error));
        }
        let destination_version = match version_on_disk(&destination, self.options.snapshot_limits)
        {
            Ok(version) => version,
            Err(error) => return failed(MutationError::cas(error)),
        };
        if let Err(error) = validate_cas(
            destination_expectation,
            destination_version,
            CasRole::MoveDestination,
        ) {
            return failed(MutationError::cas(error));
        }
        let mode = match self.options.durability.metadata {
            crate::apply_patch::file_mutation::MetadataPolicy::PreserveSupportedMode => {
                match crate::apply_patch::file_mutation::supported_mode(source.absolute()) {
                    Ok(mode) => mode,
                    Err(error) => {
                        return failed(MutationError::io(MutationErrorCode::Metadata, error));
                    }
                }
            }
            crate::apply_patch::file_mutation::MetadataPolicy::SafeAddModeOnly => None,
        };
        let bytes = replacement_bytes.unwrap_or_else(|| before.bytes.clone());
        let prepared = match self.prepare_file_stage_locked(
            destination.clone(),
            bytes,
            StageMetadata::PreserveSupportedMode {
                mode,
                source_path: Some(source.absolute().to_path_buf()),
            },
            lock,
        ) {
            Ok(prepared) => prepared,
            Err(error) => return failed(error),
        };
        self.move_file_prepared_locked(
            source,
            expected_source,
            destination,
            destination_expectation,
            prepared,
            lock,
        )
    }

    /// Publish a previously prepared move destination stage. Source and
    /// destination CAS checks still run at the real commit boundary.
    pub fn move_file_prepared_locked(
        &self,
        source: CanonicalTarget,
        expected_source: FileVersionToken,
        destination: CanonicalTarget,
        destination_expectation: CasExpectation,
        prepared: PreparedFileStage,
        _lock: &TargetLockGuard,
    ) -> MutationOutcome {
        if source.identity() == destination.identity() {
            return failed_with_aborted_stage(
                MutationError::new(MutationErrorCode::TargetExists),
                prepared,
            );
        }
        // Classify a missing/non-regular source before the filesystem-device
        // probe.  Otherwise a missing source would surface as a generic
        // metadata I/O error instead of the typed target/CAS failure used by
        // every other primitive.
        let before = match read_existing(&source, self.options.snapshot_limits) {
            Ok(before) => before,
            Err(error) => return failed_with_aborted_stage(error, prepared),
        };
        if self
            .options
            .durability
            .faults
            .check(FaultPoint::CrossDevice)
            .is_err()
        {
            return failed_with_aborted_stage(
                MutationError::new(MutationErrorCode::CrossDevice),
                prepared,
            );
        }
        match same_filesystem(source.absolute(), destination.absolute()) {
            Ok(true) => {}
            Ok(false) => {
                return failed_with_aborted_stage(
                    MutationError::new(MutationErrorCode::CrossDevice),
                    prepared,
                );
            }
            Err(error) => {
                return failed_with_aborted_stage(
                    MutationError::io(MutationErrorCode::Metadata, error),
                    prepared,
                );
            }
        }
        if let Err(error) = validate_cas(
            CasExpectation::Exact(expected_source),
            Some(before.version.token),
            CasRole::Source,
        ) {
            return failed_with_aborted_stage(MutationError::cas(error), prepared);
        }
        let destination_exists = match fs::symlink_metadata(destination.absolute()) {
            Ok(_) => true,
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => {
                return failed_with_aborted_stage(
                    MutationError::io(MutationErrorCode::Metadata, error),
                    prepared,
                );
            }
        };
        let overwritten = if destination_exists {
            let snapshot = match read_existing(&destination, self.options.snapshot_limits) {
                Ok(snapshot) => snapshot,
                Err(error) => return failed_with_aborted_stage(error, prepared),
            };
            if let Err(error) = validate_cas(
                destination_expectation,
                Some(snapshot.version.token),
                CasRole::MoveDestination,
            ) {
                return failed_with_aborted_stage(MutationError::cas(error), prepared);
            }
            Some(snapshot)
        } else {
            if let Err(error) =
                validate_cas(destination_expectation, None, CasRole::MoveDestination)
            {
                return failed_with_aborted_stage(MutationError::cas(error), prepared);
            }
            None
        };
        let PreparedStageParts {
            after,
            staged,
            parents,
            mut metadata_warnings,
        } = match prepared.into_parts(&destination) {
            Ok(parts) => parts,
            Err(error) => return failed(error),
        };
        if let Err(error) = revalidate_exact(&source, expected_source, self.options.snapshot_limits)
        {
            return failed(cleanup_error(
                cleanup_staged_error(error, staged, self.options.durability),
                parents,
            ));
        }
        if let Err(error) = revalidate_destination(
            &destination,
            destination_expectation,
            self.options.snapshot_limits,
        ) {
            return failed(cleanup_error(
                cleanup_staged_error(error, staged, self.options.durability),
                parents,
            ));
        }
        let persist_result = if matches!(destination_expectation, CasExpectation::MustNotExist) {
            persist_stage_no_replace(staged, self.options.durability, &mut metadata_warnings)
        } else {
            persist_stage(staged, self.options.durability, &mut metadata_warnings)
        };
        if let Err(error) = persist_result {
            if error.code == MutationErrorCode::Uncertain {
                let created_directories = parents.paths();
                let mut error = cleanup_error(error, parents);
                error.side_effects.metadata_warnings = metadata_warnings.clone();
                return uncertain(
                    error,
                    published_move_destination_change(
                        &destination,
                        overwritten.as_ref(),
                        &after,
                        &created_directories,
                        &metadata_warnings,
                    ),
                );
            }
            return failed(attach_metadata_warnings(
                cleanup_error(error, parents),
                &metadata_warnings,
            ));
        }
        if revalidate_exact(&source, expected_source, self.options.snapshot_limits).is_err() {
            let created_directories = parents.paths();
            let mut error =
                cleanup_error(MutationError::new(MutationErrorCode::Uncertain), parents);
            error.side_effects.metadata_warnings = metadata_warnings.clone();
            // The destination publication is already visible, but the source
            // no longer matches the version that was prepared.  Reporting the
            // whole operation as Move would falsely claim that the source was
            // removed.  Preserve only the known destination side effect and
            // mark it inexact; recovery can then surface the unresolved source
            // state without inventing a completed rename.
            return uncertain(
                error,
                published_move_destination_change(
                    &destination,
                    overwritten.as_ref(),
                    &after,
                    &created_directories,
                    &metadata_warnings,
                ),
            );
        }
        let remove_source = self
            .options
            .durability
            .faults
            .check(FaultPoint::Delete)
            .map_err(io::Error::from)
            .and_then(|_| {
                crate::apply_patch::file_mutation::secure_fs::remove_regular_file(&source)
            });
        let removed_source_parent = match remove_source {
            Ok(parent) => parent,
            Err(error) => {
                let created_directories = parents.paths();
                let mut error = cleanup_error(
                    MutationError {
                        code: MutationErrorCode::Uncertain,
                        cas: None,
                        source: Some(error),
                        side_effects: MutationSideEffects::default(),
                    },
                    parents,
                );
                error.side_effects.metadata_warnings = metadata_warnings.clone();
                return uncertain(
                    error,
                    published_move_destination_change(
                        &destination,
                        overwritten.as_ref(),
                        &after,
                        &created_directories,
                        &metadata_warnings,
                    ),
                );
            }
        };
        if self.options.durability.sync_parent {
            if let Err(error) = self.options.durability.faults.check(FaultPoint::ParentSync) {
                let mut error = MutationError::io(MutationErrorCode::Uncertain, error.into());
                error.side_effects.metadata_warnings = metadata_warnings.clone();
                let created_directories = parents.paths();
                return uncertain(
                    error,
                    MutationChange {
                        kind: MutationKind::Move,
                        source: source.clone(),
                        destination: Some(destination.clone()),
                        before: Some(before.clone()),
                        after: Some(after.clone()),
                        overwritten_destination: overwritten.clone(),
                        side_effects: MutationSideEffects {
                            created_directories,
                            ..side_effects_with_metadata(&metadata_warnings)
                        },
                    },
                );
            }
            if let Err(error) = removed_source_parent.sync_all() {
                let mut mutation_error = MutationError::io(MutationErrorCode::Uncertain, error);
                mutation_error.side_effects.metadata_warnings = metadata_warnings.clone();
                let created_directories = parents.paths();
                return uncertain(
                    mutation_error,
                    MutationChange {
                        kind: MutationKind::Move,
                        source: source.clone(),
                        destination: Some(destination.clone()),
                        before: Some(before.clone()),
                        after: Some(after.clone()),
                        overwritten_destination: overwritten.clone(),
                        side_effects: MutationSideEffects {
                            created_directories,
                            ..side_effects_with_metadata(&metadata_warnings)
                        },
                    },
                );
            }
        }
        applied(MutationChange {
            kind: MutationKind::Move,
            source,
            destination: Some(destination),
            before: Some(before),
            after: Some(after),
            overwritten_destination: overwritten,
            side_effects: MutationSideEffects {
                created_directories: parents.into_paths(),
                residual_directories: Vec::new(),
                exact: side_effects_exact(&metadata_warnings),
                metadata_warnings,
            },
        })
    }
}

fn one_target_manifest(target: CanonicalTarget) -> TargetManifest {
    TargetManifest::new(vec![target]).expect("one canonical target always forms a manifest")
}

fn revalidate_exact(
    target: &CanonicalTarget,
    expected: FileVersionToken,
    limits: SnapshotLimits,
) -> Result<(), MutationError> {
    let current = version_on_disk(target, limits).map_err(MutationError::cas)?;
    validate_cas(CasExpectation::Exact(expected), current, CasRole::Source)
        .map_err(MutationError::cas)
}

fn revalidate_destination(
    target: &CanonicalTarget,
    expectation: CasExpectation,
    limits: SnapshotLimits,
) -> Result<(), MutationError> {
    let current = version_on_disk(target, limits).map_err(MutationError::cas)?;
    validate_cas(expectation, current, CasRole::MoveDestination).map_err(MutationError::cas)
}

fn read_existing(
    target: &CanonicalTarget,
    limits: SnapshotLimits,
) -> Result<MutationSnapshot, MutationError> {
    let kind = target
        .inspect_kind()
        .map_err(|_| MutationError::new(MutationErrorCode::TargetMissing))?;
    if !matches!(
        kind,
        crate::apply_patch::file_mutation::TargetKind::RegularFile
    ) {
        return Err(MutationError::new(match kind {
            crate::apply_patch::file_mutation::TargetKind::Missing => {
                MutationErrorCode::TargetMissing
            }
            crate::apply_patch::file_mutation::TargetKind::Directory
            | crate::apply_patch::file_mutation::TargetKind::Special
            | crate::apply_patch::file_mutation::TargetKind::Symlink => {
                MutationErrorCode::NotRegularFile
            }
            crate::apply_patch::file_mutation::TargetKind::RegularFile => {
                MutationErrorCode::NotRegularFile
            }
        }));
    }
    let snapshot =
        TextSnapshot::from_file(target.absolute(), limits).map_err(MutationError::snapshot)?;
    MutationSnapshot::from_text(&snapshot)
}

/// The first-release move contract is same-filesystem only.  Compare the
/// source file's device with the nearest existing destination parent before
/// creating parents or staging bytes, so a cross-device request fails before
/// any visible filesystem mutation.
fn same_filesystem(source: &Path, destination: &Path) -> io::Result<bool> {
    // Never follow a path that may have been replaced after target
    // resolution.  `read_existing` already rejected symlinks, but the
    // device-probe runs before the final CAS revalidation and must not turn a
    // concurrent symlink substitution into an outside-workspace metadata
    // read.
    let source_metadata = fs::symlink_metadata(source)?;
    if source_metadata.file_type().is_symlink() || !source_metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "move source is not a regular file",
        ));
    }
    let destination_parent =
        nearest_existing_directory(destination.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "destination has no parent")
        })?)?;
    let destination_metadata = fs::symlink_metadata(&destination_parent)?;
    if destination_metadata.file_type().is_symlink() || !destination_metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "move destination parent is not a real directory",
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(source_metadata.dev() == destination_metadata.dev())
    }
    #[cfg(windows)]
    {
        let _ = (source_metadata, destination_metadata);
        Ok(windows_volume_serial(source)? == windows_volume_serial(&destination_parent)?)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (source_metadata, destination_metadata);
        Ok(true)
    }
}

#[cfg(windows)]
fn windows_volume_serial(path: &Path) -> io::Result<u32> {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, GetFileInformationByHandle,
    };

    let file = OpenOptions::new()
        .access_mode(0)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    let mut information = BY_HANDLE_FILE_INFORMATION::default();
    if unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut information) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(information.dwVolumeSerialNumber)
}

fn nearest_existing_directory(path: &Path) -> io::Result<PathBuf> {
    let mut current = path;
    loop {
        match fs::symlink_metadata(current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(io::Error::new(
                        io::ErrorKind::NotADirectory,
                        "destination parent is not a real directory",
                    ));
                }
                return Ok(current.to_path_buf());
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                current = current.parent().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        "destination has no existing parent directory",
                    )
                })?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn stage_bytes(
    target: &CanonicalTarget,
    bytes: &[u8],
    faults: crate::apply_patch::file_mutation::FaultPlan,
    sync_file: bool,
) -> Result<StagedFile, MutationError> {
    faults
        .check(FaultPoint::StageCreate)
        .map_err(|error| MutationError::io(MutationErrorCode::StageCreate, error.into()))?;
    let mut staged = StagedFile::create(target)
        .map_err(|error| MutationError::io(MutationErrorCode::StageCreate, error))?;
    if let Err(error) = faults.check(FaultPoint::StageWrite) {
        return Err(cleanup_staged_error(
            MutationError::io(MutationErrorCode::StageWrite, error.into()),
            staged,
            crate::apply_patch::file_mutation::DurabilityOptions {
                faults,
                ..crate::apply_patch::file_mutation::DurabilityOptions::default()
            },
        ));
    }
    if let Err(error) = staged.write_all(bytes) {
        return Err(cleanup_staged_error(
            MutationError::io(MutationErrorCode::StageWrite, error),
            staged,
            crate::apply_patch::file_mutation::DurabilityOptions {
                faults,
                ..crate::apply_patch::file_mutation::DurabilityOptions::default()
            },
        ));
    }
    if sync_file {
        if let Err(error) = faults.check(FaultPoint::FileSync) {
            return Err(cleanup_staged_error(
                MutationError::io(MutationErrorCode::Sync, error.into()),
                staged,
                crate::apply_patch::file_mutation::DurabilityOptions {
                    faults,
                    ..crate::apply_patch::file_mutation::DurabilityOptions::default()
                },
            ));
        }
        if let Err(error) = staged.sync_all() {
            return Err(cleanup_staged_error(
                MutationError::io(MutationErrorCode::Sync, error),
                staged,
                crate::apply_patch::file_mutation::DurabilityOptions {
                    faults,
                    ..crate::apply_patch::file_mutation::DurabilityOptions::default()
                },
            ));
        }
    }
    Ok(staged)
}

/// Apply the frozen metadata policy while the replacement is still private.
/// Performing this before the visible rename keeps a metadata failure a clean
/// pre-commit failure instead of exposing a file whose mode is only partially
/// updated.  Added files have no source mode, so both policies use the safe
/// creation/umask behavior for them.
fn apply_staged_metadata(
    staged: &StagedFile,
    source_path: Option<&Path>,
    source_mode: Option<u32>,
    durability: crate::apply_patch::file_mutation::DurabilityOptions,
) -> Result<Vec<crate::apply_patch::file_mutation::MetadataWarning>, MutationError> {
    durability
        .faults
        .check(FaultPoint::Metadata)
        .map_err(|error| MutationError::io(MutationErrorCode::Metadata, error.into()))?;
    let result = match (durability.metadata, source_mode) {
        (crate::apply_patch::file_mutation::MetadataPolicy::PreserveSupportedMode, Some(mode)) => {
            crate::apply_patch::file_mutation::durability::apply_supported_mode_to_file(
                staged.file(),
                Some(mode),
            )
            .and_then(|_| {
                source_path.map_or_else(
                    || Ok(Vec::new()),
                    crate::apply_patch::file_mutation::durability::metadata_warnings_for_source,
                )
            })
        }
        (crate::apply_patch::file_mutation::MetadataPolicy::PreserveSupportedMode, None)
        | (crate::apply_patch::file_mutation::MetadataPolicy::SafeAddModeOnly, _) => {
            crate::apply_patch::file_mutation::durability::apply_safe_add_mode_to_file(
                staged.file(),
            )
        }
    };
    result.map_err(|error| MutationError::io(MutationErrorCode::Metadata, error))
}

fn staged_supported_mode(staged: &StagedFile) -> io::Result<Option<u32>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        return Ok(Some(staged.file().metadata()?.permissions().mode()));
    }
    #[cfg(not(unix))]
    {
        let _ = staged;
        Ok(None)
    }
}

fn persist_stage(
    stage: StagedFile,
    durability: crate::apply_patch::file_mutation::DurabilityOptions,
    warnings: &mut Vec<crate::apply_patch::file_mutation::MetadataWarning>,
) -> Result<(), MutationError> {
    if let Err(error) = durability.faults.check(FaultPoint::Rename) {
        return Err(cleanup_staged_error(
            MutationError::io(MutationErrorCode::Rename, error.into()),
            stage,
            durability,
        ));
    }
    let inject_cleanup_failure = durability.faults.check(FaultPoint::Cleanup).is_err();
    let published = stage
        .publish_replace(inject_cleanup_failure)
        .map_err(|error| publish_error(error, MutationErrorCode::Rename))?;
    if published.cleanup_failed {
        push_cleanup_warning(warnings);
    }
    if durability.sync_parent {
        if let Err(error) = durability.faults.check(FaultPoint::ParentSync) {
            return Err(attach_metadata_warnings(
                MutationError::io(MutationErrorCode::Uncertain, error.into()),
                warnings,
            ));
        }
        if let Err(error) = published.sync_all() {
            return Err(attach_metadata_warnings(
                MutationError::io(MutationErrorCode::Uncertain, error),
                warnings,
            ));
        }
    }
    Ok(())
}

/// Publish a staged file only when the destination is still absent. `rename`
/// is intentionally not used for this case because on Unix it replaces a
/// destination that may have appeared after the preflight CAS check. A hard
/// link gives us the required create-new commit primitive on the same
/// filesystem: linking is atomic with respect to the destination name, and
/// dropping the temporary link leaves the complete file at `destination`.
fn persist_stage_no_replace(
    stage: StagedFile,
    durability: crate::apply_patch::file_mutation::DurabilityOptions,
    warnings: &mut Vec<crate::apply_patch::file_mutation::MetadataWarning>,
) -> Result<(), MutationError> {
    if let Err(error) = durability.faults.check(FaultPoint::Rename) {
        return Err(cleanup_staged_error(
            MutationError::io(MutationErrorCode::Rename, error.into()),
            stage,
            durability,
        ));
    }
    let inject_cleanup_failure = durability.faults.check(FaultPoint::Cleanup).is_err();
    let published = stage
        .publish_no_replace(inject_cleanup_failure)
        .map_err(|error| {
            let code = if !error.published && error.source.kind() == io::ErrorKind::AlreadyExists {
                MutationErrorCode::TargetExists
            } else {
                MutationErrorCode::Rename
            };
            publish_error(error, code)
        })?;
    if published.cleanup_failed {
        push_cleanup_warning(warnings);
    }
    if durability.sync_parent {
        if let Err(error) = durability.faults.check(FaultPoint::ParentSync) {
            return Err(attach_metadata_warnings(
                MutationError::io(MutationErrorCode::Uncertain, error.into()),
                warnings,
            ));
        }
        if let Err(error) = published.sync_all() {
            return Err(attach_metadata_warnings(
                MutationError::io(MutationErrorCode::Uncertain, error),
                warnings,
            ));
        }
    }
    Ok(())
}

fn publish_error(
    error: crate::apply_patch::file_mutation::secure_fs::PublishError,
    pre_publish_code: MutationErrorCode,
) -> MutationError {
    let mut mutation = MutationError::io(
        if error.published {
            MutationErrorCode::Uncertain
        } else {
            pre_publish_code
        },
        error.source,
    );
    if error.cleanup_failed {
        push_cleanup_warning(&mut mutation.side_effects.metadata_warnings);
        mutation.side_effects.exact = false;
    }
    mutation
}

fn cleanup_staged_error(
    mut error: MutationError,
    staged: StagedFile,
    durability: crate::apply_patch::file_mutation::DurabilityOptions,
) -> MutationError {
    let inject_cleanup_failure = durability.faults.check(FaultPoint::Cleanup).is_err();
    if !staged.cleanup(inject_cleanup_failure) {
        push_cleanup_warning(&mut error.side_effects.metadata_warnings);
        error.side_effects.exact = false;
    }
    error
}

fn push_cleanup_warning(warnings: &mut Vec<crate::apply_patch::file_mutation::MetadataWarning>) {
    if !warnings
        .contains(&crate::apply_patch::file_mutation::MetadataWarning::TemporaryFileCleanupFailed)
    {
        warnings
            .push(crate::apply_patch::file_mutation::MetadataWarning::TemporaryFileCleanupFailed);
    }
}

#[derive(Debug)]
struct ParentCreationError {
    source: io::Error,
    side_effects: MutationSideEffects,
}

fn ensure_parents(target: &CanonicalTarget) -> Result<CreatedDirectories, ParentCreationError> {
    crate::apply_patch::file_mutation::secure_fs::ensure_parent_directories(target)
        .map_err(|failure| parent_creation_error(failure.source, failure.created))
}

fn parent_creation_error(source: io::Error, created: CreatedDirectories) -> ParentCreationError {
    let (mut created_directories, mut residual_directories) = created.cleanup();
    sort_deepest_first(&mut created_directories);
    sort_deepest_first(&mut residual_directories);
    ParentCreationError {
        source,
        side_effects: MutationSideEffects {
            exact: residual_directories.is_empty(),
            created_directories,
            residual_directories,
            ..MutationSideEffects::default()
        },
    }
}

fn sort_deepest_first(paths: &mut [PathBuf]) {
    paths.sort_by(|left, right| {
        left.components()
            .count()
            .cmp(&right.components().count())
            .reverse()
    });
}

fn cleanup_error(mut error: MutationError, parents: CreatedDirectories) -> MutationError {
    let (mut created_directories, mut residual_directories) = parents.cleanup();
    sort_deepest_first(&mut created_directories);
    sort_deepest_first(&mut residual_directories);
    error.side_effects.exact &= residual_directories.is_empty();
    error.side_effects.created_directories = created_directories;
    error.side_effects.residual_directories = residual_directories;
    error
}

fn attach_metadata_warnings(
    mut error: MutationError,
    warnings: &[crate::apply_patch::file_mutation::MetadataWarning],
) -> MutationError {
    error
        .side_effects
        .metadata_warnings
        .extend(warnings.iter().copied());
    error
        .side_effects
        .metadata_warnings
        .sort_by_key(|warning| warning.as_str());
    error.side_effects.metadata_warnings.dedup();
    error.side_effects.exact &= side_effects_exact(&error.side_effects.metadata_warnings);
    error
}

fn side_effects_with_metadata(
    warnings: &[crate::apply_patch::file_mutation::MetadataWarning],
) -> MutationSideEffects {
    MutationSideEffects {
        metadata_warnings: warnings.to_vec(),
        exact: side_effects_exact(warnings),
        ..MutationSideEffects::default()
    }
}

fn side_effects_exact(warnings: &[crate::apply_patch::file_mutation::MetadataWarning]) -> bool {
    !warnings
        .contains(&crate::apply_patch::file_mutation::MetadataWarning::TemporaryFileCleanupFailed)
}

/// A move publishes its destination before removing the source. If anything
/// fails in that interval, the only filesystem effect we can assert is the
/// destination add/replacement. Reporting a `Move` would falsely claim that
/// the source disappeared and would corrupt both recovery and file lineage.
fn published_move_destination_change(
    destination: &CanonicalTarget,
    overwritten: Option<&MutationSnapshot>,
    after: &MutationSnapshot,
    created_directories: &[PathBuf],
    metadata_warnings: &[crate::apply_patch::file_mutation::MetadataWarning],
) -> MutationChange {
    MutationChange {
        kind: if overwritten.is_some() {
            MutationKind::Replace
        } else {
            MutationKind::Add
        },
        source: destination.clone(),
        destination: None,
        before: overwritten.cloned(),
        after: Some(after.clone()),
        overwritten_destination: None,
        side_effects: MutationSideEffects {
            created_directories: created_directories.to_vec(),
            residual_directories: Vec::new(),
            metadata_warnings: metadata_warnings.to_vec(),
            exact: false,
        },
    }
}

fn applied(change: MutationChange) -> MutationOutcome {
    MutationOutcome::Applied(change)
}

fn failed_with_aborted_stage(
    mut error: MutationError,
    prepared: PreparedFileStage,
) -> MutationOutcome {
    error.side_effects.merge(&prepared.abort());
    failed(error)
}

fn failed(error: MutationError) -> MutationOutcome {
    MutationOutcome::Failed {
        error,
        committed: None,
    }
}

fn uncertain(mut error: MutationError, mut committed: MutationChange) -> MutationOutcome {
    // An uncertain primitive may have a known visible prefix, but by
    // definition we cannot claim complete knowledge of every durability or
    // residual side effect. Propagate that weakest exactness at the primitive
    // boundary so later delta/history folding cannot accidentally upgrade it.
    error.side_effects.exact = false;
    committed.side_effects.exact = false;
    MutationOutcome::Uncertain {
        error,
        committed: Some(committed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::file_mutation::{TargetExpectation, TargetResolver, TargetRole};
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn target(
        root: &std::path::Path,
        path: &str,
        role: TargetRole,
        expectation: TargetExpectation,
    ) -> CanonicalTarget {
        TargetResolver::new(root)
            .unwrap()
            .resolve(path, role, expectation)
            .unwrap()
    }

    #[test]
    fn create_requires_missing_target_and_returns_exact_after() {
        let root = tempfile::tempdir().unwrap();
        let file = target(
            root.path(),
            "new.txt",
            TargetRole::Destination,
            TargetExpectation::Missing,
        );
        let engine = FileMutationEngine::new(MutationOptions::default());
        let outcome = engine.create(file.clone(), b"hello\n".to_vec());
        let change = outcome.committed().unwrap();
        assert_eq!(change.kind, MutationKind::Add);
        assert_eq!(change.before, None);
        assert_eq!(change.after.as_ref().unwrap().bytes, b"hello\n");
        assert_eq!(std::fs::read(file.absolute()).unwrap(), b"hello\n");
    }

    #[cfg(unix)]
    #[test]
    fn replace_move_and_add_follow_the_frozen_unix_mode_policy() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let source_path = root.path().join("source.txt");
        std::fs::write(&source_path, b"old\n").unwrap();
        std::fs::set_permissions(&source_path, std::fs::Permissions::from_mode(0o751)).unwrap();
        let source = target(
            root.path(),
            "source.txt",
            TargetRole::Source,
            TargetExpectation::ExistingRegular,
        );
        let engine = FileMutationEngine::new(MutationOptions::default());
        let replaced = engine.replace(
            source.clone(),
            FileVersionToken::from_bytes(b"old\n"),
            b"replaced\n".to_vec(),
        );
        let MutationOutcome::Applied(replaced) = replaced else {
            panic!("mode-preserving replacement must apply");
        };
        assert_eq!(
            std::fs::metadata(&source_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o751
        );
        assert!(
            replaced
                .side_effects
                .metadata_warnings
                .contains(&crate::apply_patch::file_mutation::MetadataWarning::AclNotPreserved)
        );

        let destination = target(
            root.path(),
            "moved.txt",
            TargetRole::Destination,
            TargetExpectation::Missing,
        );
        let moved = engine.move_file(
            source,
            FileVersionToken::from_bytes(b"replaced\n"),
            destination,
            CasExpectation::MustNotExist,
            None,
        );
        assert!(matches!(moved, MutationOutcome::Applied(_)));
        assert_eq!(
            std::fs::metadata(root.path().join("moved.txt"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o751
        );

        let added = target(
            root.path(),
            "added.txt",
            TargetRole::Destination,
            TargetExpectation::Missing,
        );
        assert!(matches!(
            engine.create(added, b"added\n".to_vec()),
            MutationOutcome::Applied(_)
        ));
        let added_mode = std::fs::metadata(root.path().join("added.txt"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(added_mode & 0o077, 0, "new files must remain private");
    }

    #[test]
    fn create_reports_a_staging_cleanup_failure_without_hiding_the_commit() {
        let root = tempfile::tempdir().unwrap();
        let file = target(
            root.path(),
            "new.txt",
            TargetRole::Destination,
            TargetExpectation::Missing,
        );
        let mut options = MutationOptions::default();
        options.durability.faults =
            crate::apply_patch::file_mutation::FaultPlan::fail_at(FaultPoint::Cleanup);
        let engine = FileMutationEngine::new(options);

        let outcome = engine.create(file.clone(), b"hello\n".to_vec());
        let MutationOutcome::Applied(change) = outcome else {
            panic!("a residual private staging name must not hide the committed add");
        };
        assert_eq!(std::fs::read(file.absolute()).unwrap(), b"hello\n");
        assert!(!change.side_effects.exact);
        assert!(change.side_effects.metadata_warnings.contains(
            &crate::apply_patch::file_mutation::MetadataWarning::TemporaryFileCleanupFailed
        ));
        assert!(
            std::fs::read_dir(root.path())
                .unwrap()
                .filter_map(Result::ok)
                .any(|entry| entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".pioneer-patch-")),
            "the injected cleanup failure should leave an observable residual staging name"
        );
    }

    #[test]
    fn replace_rejects_stale_expected_version_without_mutating() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file.txt");
        std::fs::write(&path, b"old\n").unwrap();
        let file = target(
            root.path(),
            "file.txt",
            TargetRole::Source,
            TargetExpectation::ExistingRegular,
        );
        let engine = FileMutationEngine::new(MutationOptions::default());
        let stale = FileVersionToken::from_bytes(b"other\n");
        let outcome = engine.replace(file, stale, b"new\n".to_vec());
        assert!(matches!(outcome, MutationOutcome::Failed { .. }));
        assert_eq!(std::fs::read(path).unwrap(), b"old\n");
    }

    #[test]
    fn concurrent_replaces_on_one_target_commit_once_without_lost_update() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file.txt");
        std::fs::write(&path, b"old\n").unwrap();
        let file = target(
            root.path(),
            "file.txt",
            TargetRole::Source,
            TargetExpectation::ExistingRegular,
        );
        let engine = FileMutationEngine::new(MutationOptions::default());
        let expected = FileVersionToken::from_bytes(b"old\n");
        let barrier = Arc::new(Barrier::new(3));

        let spawn_replace = |bytes: &'static [u8]| {
            let engine = engine.clone();
            let file = file.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                engine.replace(file, expected, bytes.to_vec())
            })
        };
        let first = spawn_replace(b"first\n");
        let second = spawn_replace(b"second\n");
        barrier.wait();

        let outcomes = [first.join().unwrap(), second.join().unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, MutationOutcome::Applied(_)))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(
                    outcome,
                    MutationOutcome::Failed {
                        error: MutationError {
                            code: MutationErrorCode::Cas,
                            ..
                        },
                        committed: None,
                    }
                ))
                .count(),
            1
        );
        let final_bytes = std::fs::read(path).unwrap();
        assert!(final_bytes == b"first\n" || final_bytes == b"second\n");
        assert_eq!(engine.lock_registry().entry_count(), 0);
    }

    #[test]
    fn staging_write_sync_and_rename_failures_preserve_original_destination() {
        for fault in [
            FaultPoint::StageCreate,
            FaultPoint::StageWrite,
            FaultPoint::FileSync,
            FaultPoint::Metadata,
            FaultPoint::Rename,
        ] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("file.txt");
            std::fs::write(&path, b"old\n").unwrap();
            let file = target(
                root.path(),
                "file.txt",
                TargetRole::Source,
                TargetExpectation::ExistingRegular,
            );
            let mut options = MutationOptions::default();
            options.durability.faults =
                crate::apply_patch::file_mutation::FaultPlan::fail_at(fault);
            let outcome = FileMutationEngine::new(options).replace(
                file,
                FileVersionToken::from_bytes(b"old\n"),
                b"new\n".to_vec(),
            );

            let MutationOutcome::Failed {
                error,
                committed: None,
            } = outcome
            else {
                panic!("{fault:?} must fail before publishing the replacement");
            };
            let expected_code = match fault {
                FaultPoint::StageCreate => MutationErrorCode::StageCreate,
                FaultPoint::StageWrite => MutationErrorCode::StageWrite,
                FaultPoint::FileSync => MutationErrorCode::Sync,
                FaultPoint::Metadata => MutationErrorCode::Metadata,
                FaultPoint::Rename => MutationErrorCode::Rename,
                _ => unreachable!("fault list is exhaustive for this test"),
            };
            assert_eq!(error.code, expected_code, "fault {fault:?}");
            assert_eq!(std::fs::read(&path).unwrap(), b"old\n", "fault {fault:?}");
        }
    }

    #[test]
    fn parent_sync_failure_reports_committed_uncertainty_truthfully() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file.txt");
        std::fs::write(&path, b"old\n").unwrap();
        let file = target(
            root.path(),
            "file.txt",
            TargetRole::Source,
            TargetExpectation::ExistingRegular,
        );
        let mut options = MutationOptions::default();
        options.durability.faults =
            crate::apply_patch::file_mutation::FaultPlan::fail_at(FaultPoint::ParentSync);
        let outcome = FileMutationEngine::new(options).replace(
            file,
            FileVersionToken::from_bytes(b"old\n"),
            b"new\n".to_vec(),
        );

        let MutationOutcome::Uncertain {
            error,
            committed: Some(change),
        } = outcome
        else {
            panic!("post-rename parent sync failure must preserve the known committed change");
        };
        assert_eq!(error.code, MutationErrorCode::Uncertain);
        assert_eq!(change.after.as_ref().unwrap().bytes, b"new\n");
        assert!(!change.side_effects.exact);
        assert_eq!(std::fs::read(path).unwrap(), b"new\n");
    }

    #[test]
    fn cross_device_move_is_rejected_before_any_visible_mutation() {
        let root = tempfile::tempdir().unwrap();
        let source_path = root.path().join("old.txt");
        let destination_path = root.path().join("new.txt");
        std::fs::write(&source_path, b"old\n").unwrap();
        let source = target(
            root.path(),
            "old.txt",
            TargetRole::Source,
            TargetExpectation::ExistingRegular,
        );
        let destination = target(
            root.path(),
            "new.txt",
            TargetRole::Destination,
            TargetExpectation::Missing,
        );
        let mut options = MutationOptions::default();
        options.durability.faults =
            crate::apply_patch::file_mutation::FaultPlan::fail_at(FaultPoint::CrossDevice);
        let outcome = FileMutationEngine::new(options).move_file(
            source,
            FileVersionToken::from_bytes(b"old\n"),
            destination,
            CasExpectation::MustNotExist,
            None,
        );

        assert!(matches!(
            outcome,
            MutationOutcome::Failed {
                error: MutationError {
                    code: MutationErrorCode::CrossDevice,
                    ..
                },
                committed: None,
            }
        ));
        assert_eq!(std::fs::read(source_path).unwrap(), b"old\n");
        assert!(!destination_path.exists());
    }

    #[test]
    fn delete_and_move_preserve_before_after_and_overwrite() {
        let root = tempfile::tempdir().unwrap();
        let source_path = root.path().join("old.txt");
        let destination_path = root.path().join("new.txt");
        std::fs::write(&source_path, b"old\n").unwrap();
        std::fs::write(&destination_path, b"destination\n").unwrap();
        let engine = FileMutationEngine::new(MutationOptions::default());
        let source = target(
            root.path(),
            "old.txt",
            TargetRole::Source,
            TargetExpectation::ExistingRegular,
        );
        let destination = target(
            root.path(),
            "new.txt",
            TargetRole::Destination,
            TargetExpectation::ExistingRegular,
        );
        let destination_token = FileVersionToken::from_bytes(b"destination\n");
        let moved = engine.move_file(
            source,
            FileVersionToken::from_bytes(b"old\n"),
            destination,
            CasExpectation::Exact(destination_token),
            Some(b"updated\n".to_vec()),
        );
        let change = moved.committed().unwrap();
        assert_eq!(change.kind, MutationKind::Move);
        assert_eq!(
            change.overwritten_destination.as_ref().unwrap().bytes,
            b"destination\n"
        );
        assert_eq!(std::fs::read(destination_path).unwrap(), b"updated\n");
        assert!(!source_path.exists());
    }

    #[test]
    fn delete_returns_exact_prior_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("delete.txt");
        std::fs::write(&path, b"remove\n").unwrap();
        let target = target(
            root.path(),
            "delete.txt",
            TargetRole::Source,
            TargetExpectation::ExistingRegular,
        );
        let engine = FileMutationEngine::new(MutationOptions::default());
        let outcome = engine.delete(target, FileVersionToken::from_bytes(b"remove\n"));
        assert_eq!(
            outcome.committed().unwrap().before.as_ref().unwrap().bytes,
            b"remove\n"
        );
        assert!(!path.exists());
    }

    #[test]
    fn move_destination_sync_uncertainty_does_not_claim_source_removal() {
        let root = tempfile::tempdir().unwrap();
        let source_path = root.path().join("old.txt");
        let destination_path = root.path().join("nested/new.txt");
        std::fs::write(&source_path, b"old\n").unwrap();
        let source = target(
            root.path(),
            "old.txt",
            TargetRole::Source,
            TargetExpectation::ExistingRegular,
        );
        let destination = target(
            root.path(),
            "nested/new.txt",
            TargetRole::Destination,
            TargetExpectation::Missing,
        );
        let mut options = MutationOptions::default();
        options.durability.faults =
            crate::apply_patch::file_mutation::FaultPlan::fail_at(FaultPoint::ParentSync);
        let engine = FileMutationEngine::new(options);

        let outcome = engine.move_file(
            source,
            FileVersionToken::from_bytes(b"old\n"),
            destination,
            CasExpectation::MustNotExist,
            None,
        );

        let MutationOutcome::Uncertain {
            committed: Some(change),
            ..
        } = outcome
        else {
            panic!("destination parent-sync failure must be a committed uncertainty");
        };
        assert_eq!(change.kind, MutationKind::Add);
        assert_eq!(change.source.absolute(), destination_path);
        assert!(change.destination.is_none());
        assert!(!change.side_effects.exact);
        assert_eq!(std::fs::read(&source_path).unwrap(), b"old\n");
        assert_eq!(std::fs::read(&destination_path).unwrap(), b"old\n");
    }

    #[test]
    fn move_source_delete_failure_reports_only_destination_replacement() {
        let root = tempfile::tempdir().unwrap();
        let source_path = root.path().join("old.txt");
        let destination_path = root.path().join("new.txt");
        std::fs::write(&source_path, b"old\n").unwrap();
        std::fs::write(&destination_path, b"destination\n").unwrap();
        let source = target(
            root.path(),
            "old.txt",
            TargetRole::Source,
            TargetExpectation::ExistingRegular,
        );
        let destination = target(
            root.path(),
            "new.txt",
            TargetRole::Destination,
            TargetExpectation::ExistingRegular,
        );
        let mut options = MutationOptions::default();
        options.durability.faults =
            crate::apply_patch::file_mutation::FaultPlan::fail_at(FaultPoint::Delete);
        let engine = FileMutationEngine::new(options);

        let outcome = engine.move_file(
            source,
            FileVersionToken::from_bytes(b"old\n"),
            destination,
            CasExpectation::Exact(FileVersionToken::from_bytes(b"destination\n")),
            Some(b"updated\n".to_vec()),
        );

        let MutationOutcome::Uncertain {
            committed: Some(change),
            ..
        } = outcome
        else {
            panic!("source deletion failure must be a committed uncertainty");
        };
        assert_eq!(change.kind, MutationKind::Replace);
        assert_eq!(change.source.absolute(), destination_path);
        assert!(change.destination.is_none());
        assert_eq!(
            change
                .before
                .as_ref()
                .map(|snapshot| snapshot.bytes.as_slice()),
            Some(b"destination\n".as_slice())
        );
        assert!(!change.side_effects.exact);
        assert_eq!(std::fs::read(&source_path).unwrap(), b"old\n");
        assert_eq!(std::fs::read(&destination_path).unwrap(), b"updated\n");
    }
}
