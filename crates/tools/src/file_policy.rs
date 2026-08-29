use crate::apply_patch::file_mutation::{
    CanonicalTarget, TargetExpectation, TargetRole, open_directory, open_directory_at,
    open_regular_file, open_regular_file_at,
};
use pioneer_protocol::{
    TurnExecutionSecuritySnapshot, TurnFilesystemAccess, TurnFilesystemSandboxEntry,
    TurnFilesystemSandboxKind, TurnFilesystemSandboxPath,
};
use std::collections::BTreeSet;
use std::fs::{File, Metadata};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilePolicyOperation {
    Read,
    Write,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilePolicyDecision {
    Allowed(FilePolicyGrant),
    Denied(FilePolicyDeny),
}

impl FilePolicyDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed(_))
    }

    pub fn grant(&self) -> Option<&FilePolicyGrant> {
        match self {
            Self::Allowed(grant) => Some(grant),
            Self::Denied(_) => None,
        }
    }

    pub fn deny(&self) -> Option<&FilePolicyDeny> {
        match self {
            Self::Allowed(_) => None,
            Self::Denied(deny) => Some(deny),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilePolicyGrant {
    pub operation: FilePolicyOperation,
    pub requested_path: PathBuf,
    pub resolved_path: PathBuf,
    pub matched_root: Option<PathBuf>,
    pub access: TurnFilesystemAccess,
    pub(crate) capability: FilePolicyCapability,
}

impl PartialEq for FilePolicyGrant {
    fn eq(&self, other: &Self) -> bool {
        self.operation == other.operation
            && self.requested_path == other.requested_path
            && self.resolved_path == other.resolved_path
            && self.matched_root == other.matched_root
            && self.access == other.access
    }
}

impl Eq for FilePolicyGrant {}

/// An opaque filesystem authority captured at policy-check time.  Existing
/// targets carry the opened target object; write targets carry an opened
/// existing ancestor plus the normalized relative destination.  Consumers
/// must use these handles instead of reopening the checked pathname.
#[derive(Debug, Clone)]
pub(crate) struct FilePolicyCapability {
    target: Option<Arc<File>>,
    canonical_target: Option<CanonicalTarget>,
    anchor: Arc<File>,
    anchor_path: PathBuf,
    relative_path: PathBuf,
}

impl FilePolicyCapability {
    fn capture_read(path: &Path) -> std::io::Result<Self> {
        let metadata = std::fs::symlink_metadata(path)?;
        let target = if metadata.is_dir() {
            open_directory(path)?
        } else if metadata.is_file() {
            open_regular_file(path)?
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "filesystem policy target is not a regular file or directory",
            ));
        };
        if !same_filesystem_object(&metadata, &target.metadata()?) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "filesystem policy target changed while its capability was captured",
            ));
        }
        let anchor_path = if metadata.is_dir() {
            path.to_path_buf()
        } else {
            path.parent()
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "filesystem policy target has no parent",
                    )
                })?
                .to_path_buf()
        };
        let anchor = open_directory(anchor_path.as_path())?;
        let relative_path = path
            .strip_prefix(anchor_path.as_path())
            .map(Path::to_path_buf)
            .unwrap_or_else(|_| PathBuf::from("."));
        let canonical_target = Some(
            CanonicalTarget::from_authorized_path(
                anchor_path.as_path(),
                path.to_path_buf(),
                TargetRole::Source,
                if metadata.is_dir() {
                    TargetExpectation::ParentDirectory
                } else {
                    TargetExpectation::ExistingRegular
                },
            )
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "filesystem policy target could not be normalized",
                )
            })?,
        );
        Ok(Self {
            target: Some(Arc::new(target)),
            canonical_target,
            anchor: Arc::new(anchor),
            anchor_path,
            relative_path,
        })
    }

    fn capture_write(path: &Path) -> Result<Self, FilePolicyDenyReason> {
        let anchor_path = resolve_existing_write_anchor(path)?;
        let anchor_metadata = std::fs::symlink_metadata(anchor_path.as_path())
            .map_err(|_| FilePolicyDenyReason::InvalidRoot)?;
        let anchor =
            open_directory(anchor_path.as_path()).map_err(|_| FilePolicyDenyReason::InvalidRoot)?;
        if !same_filesystem_object(
            &anchor_metadata,
            &anchor
                .metadata()
                .map_err(|_| FilePolicyDenyReason::InvalidRoot)?,
        ) {
            return Err(FilePolicyDenyReason::InvalidRoot);
        }
        let relative_path = path
            .strip_prefix(anchor_path.as_path())
            .map(Path::to_path_buf)
            .map_err(|_| FilePolicyDenyReason::InvalidRoot)?;
        Ok(Self {
            target: None,
            canonical_target: None,
            anchor: Arc::new(anchor),
            anchor_path,
            relative_path,
        })
    }

    pub(crate) fn capture_unchecked(
        operation: FilePolicyOperation,
        path: &Path,
    ) -> Result<Self, FilePolicyDenyReason> {
        match operation {
            FilePolicyOperation::Read => {
                Self::capture_read(path).map_err(|_| FilePolicyDenyReason::InvalidRoot)
            }
            FilePolicyOperation::Write => Self::capture_write(path),
        }
    }

    pub(crate) fn open_target(&self) -> std::io::Result<File> {
        if let Some(target) = &self.target {
            return target.try_clone();
        }
        open_regular_file_at(self.anchor.as_ref(), self.relative_path.as_path())
    }

    pub(crate) fn canonical_target(&self) -> Option<&CanonicalTarget> {
        self.canonical_target.as_ref()
    }

    pub(crate) fn open_regular_file(&self) -> std::io::Result<File> {
        if let Some(target) = &self.target {
            return target.try_clone();
        }
        open_regular_file_at(self.anchor.as_ref(), self.relative_path.as_path())
    }

    pub(crate) fn open_directory(&self) -> std::io::Result<File> {
        if let Some(target) = &self.target {
            let clone = target.try_clone()?;
            if clone.metadata()?.is_dir() {
                return Ok(clone);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "filesystem policy target is not a directory",
            ));
        }
        open_directory_at(self.anchor.as_ref(), self.relative_path.as_path())
    }

    pub(crate) fn anchor(&self) -> std::io::Result<File> {
        self.anchor.try_clone()
    }

    pub(crate) fn anchor_path(&self) -> &Path {
        self.anchor_path.as_path()
    }

    pub(crate) fn relative_path(&self) -> &Path {
        self.relative_path.as_path()
    }
}

fn same_filesystem_object(expected: &Metadata, actual: &Metadata) -> bool {
    if expected.is_dir() != actual.is_dir() || expected.is_file() != actual.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        return expected.dev() == actual.dev() && expected.ino() == actual.ino();
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        return expected.volume_serial_number() == actual.volume_serial_number()
            && expected.file_index() == actual.file_index();
    }
    #[cfg(not(any(unix, windows)))]
    {
        expected.len() == actual.len() && expected.modified().ok() == actual.modified().ok()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilePolicyDeny {
    pub operation: FilePolicyOperation,
    pub requested_path: PathBuf,
    pub resolved_path: Option<PathBuf>,
    pub reason: FilePolicyDenyReason,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilePolicyDenyReason {
    EmptyPath,
    MissingPath,
    OutsideAllowedRoots,
    SymlinkEscape,
    WriteRequiresWritableRoot,
    NoUsableRoots,
    InvalidRoot,
}

pub struct FilePolicyChecker;

impl FilePolicyChecker {
    /// Returns the concrete roots authorized for the requested operation in
    /// this turn. Relative model paths always resolve from `sandbox.cwd`; the
    /// roots are the additional boundaries selected by Composer/security.
    pub fn allowed_roots(
        snapshot: &TurnExecutionSecuritySnapshot,
        operation: FilePolicyOperation,
    ) -> Vec<PathBuf> {
        if snapshot.sandbox.filesystem.kind == TurnFilesystemSandboxKind::Unrestricted {
            return vec![normalize_lexically(PathBuf::from(
                snapshot.sandbox.cwd.as_str(),
            ))];
        }

        let mut roots = BTreeSet::new();
        for entry in &snapshot.sandbox.filesystem.entries {
            if !access_allows(entry.access, operation) {
                continue;
            }
            let Some(path) = entry_root_path(snapshot, entry) else {
                continue;
            };
            let absolute = if path.is_absolute() {
                path
            } else {
                Path::new(snapshot.sandbox.cwd.as_str()).join(path)
            };
            let absolute = normalize_lexically(absolute);
            if let Ok(root) = resolve_policy_root(operation, absolute.as_path()) {
                roots.insert(root);
            }
        }
        roots.into_iter().collect()
    }

    /// Returns an already-existing directory from which a descriptor-anchored
    /// writer can securely reach an authorized destination. The policy root
    /// itself may be a future directory: exact write grants intentionally
    /// authorize tools such as Apply Patch and download_url to create it.
    pub fn write_anchor(grant: &FilePolicyGrant) -> Result<PathBuf, FilePolicyDenyReason> {
        if grant.operation != FilePolicyOperation::Write {
            return Err(FilePolicyDenyReason::InvalidRoot);
        }
        Ok(grant.capability.anchor_path().to_path_buf())
    }

    pub fn check_read(
        snapshot: &TurnExecutionSecuritySnapshot,
        requested_path: impl AsRef<Path>,
    ) -> FilePolicyDecision {
        Self::check(snapshot, FilePolicyOperation::Read, requested_path)
    }

    pub fn check_write(
        snapshot: &TurnExecutionSecuritySnapshot,
        requested_path: impl AsRef<Path>,
    ) -> FilePolicyDecision {
        Self::check(snapshot, FilePolicyOperation::Write, requested_path)
    }

    pub fn check(
        snapshot: &TurnExecutionSecuritySnapshot,
        operation: FilePolicyOperation,
        requested_path: impl AsRef<Path>,
    ) -> FilePolicyDecision {
        let requested_path = requested_path.as_ref();
        let absolute_path = match absolute_requested_path(snapshot, requested_path) {
            Ok(path) => path,
            Err(deny) => return deny_for(operation, requested_path.to_path_buf(), None, deny),
        };
        let resolved_path = match resolve_requested_path(operation, absolute_path.as_path()) {
            Ok(path) => path,
            Err(deny) => return deny_for(operation, absolute_path, None, deny),
        };

        if snapshot.sandbox.filesystem.kind == TurnFilesystemSandboxKind::Unrestricted {
            return allowed_grant(
                operation,
                absolute_path,
                resolved_path,
                None,
                TurnFilesystemAccess::Write,
            );
        }

        let mut usable_roots = 0usize;
        let mut literal_under_root = false;
        let mut read_only_match = None;
        let mut invalid_root = false;

        for entry in &snapshot.sandbox.filesystem.entries {
            let Some(root_path) = entry_root_path(snapshot, entry) else {
                invalid_root = true;
                continue;
            };
            let lexical_root_path = normalize_lexically(if root_path.is_absolute() {
                root_path.clone()
            } else {
                Path::new(snapshot.sandbox.cwd.as_str()).join(root_path.as_path())
            });
            let Ok(root_path) = resolve_policy_root(operation, lexical_root_path.as_path()) else {
                invalid_root = true;
                continue;
            };
            usable_roots += 1;
            if absolute_path.starts_with(lexical_root_path.as_path())
                || absolute_path.starts_with(root_path.as_path())
            {
                literal_under_root = true;
            }
            if !resolved_path.starts_with(root_path.as_path()) {
                continue;
            }
            if access_allows(entry.access, operation) {
                return allowed_grant(
                    operation,
                    absolute_path,
                    resolved_path,
                    Some(root_path),
                    entry.access,
                );
            }
            if operation == FilePolicyOperation::Write && entry.access == TurnFilesystemAccess::Read
            {
                read_only_match = Some(root_path);
            }
        }

        if let Some(root) = read_only_match {
            return deny_for(
                operation,
                absolute_path,
                Some(resolved_path),
                FilePolicyDenyReason::WriteRequiresWritableRoot,
            )
            .with_root(root);
        }
        if literal_under_root {
            return deny_for(
                operation,
                absolute_path,
                Some(resolved_path),
                FilePolicyDenyReason::SymlinkEscape,
            );
        }
        if usable_roots == 0 {
            return deny_for(
                operation,
                absolute_path,
                Some(resolved_path),
                if invalid_root {
                    FilePolicyDenyReason::InvalidRoot
                } else {
                    FilePolicyDenyReason::NoUsableRoots
                },
            );
        }
        deny_for(
            operation,
            absolute_path,
            Some(resolved_path),
            FilePolicyDenyReason::OutsideAllowedRoots,
        )
    }
}

fn allowed_grant(
    operation: FilePolicyOperation,
    requested_path: PathBuf,
    resolved_path: PathBuf,
    matched_root: Option<PathBuf>,
    access: TurnFilesystemAccess,
) -> FilePolicyDecision {
    match match operation {
        FilePolicyOperation::Read => FilePolicyCapability::capture_read(resolved_path.as_path())
            .map_err(|_| FilePolicyDenyReason::InvalidRoot),
        FilePolicyOperation::Write => FilePolicyCapability::capture_write(resolved_path.as_path()),
    } {
        Ok(capability) => FilePolicyDecision::Allowed(FilePolicyGrant {
            operation,
            requested_path,
            resolved_path,
            matched_root,
            access,
            capability,
        }),
        Err(reason) => deny_for(operation, requested_path, Some(resolved_path), reason),
    }
}

trait FilePolicyDecisionExt {
    fn with_root(self, root: PathBuf) -> Self;
}

impl FilePolicyDecisionExt for FilePolicyDecision {
    fn with_root(self, _root: PathBuf) -> Self {
        match self {
            Self::Denied(mut deny) => {
                // Canonical roots are internal paths and must not leak into
                // model-visible diagnostics.
                deny.message = format!("{}; matched read-only root", deny.message);
                Self::Denied(deny)
            }
            allowed => allowed,
        }
    }
}

fn absolute_requested_path(
    snapshot: &TurnExecutionSecuritySnapshot,
    requested_path: &Path,
) -> Result<PathBuf, FilePolicyDenyReason> {
    if requested_path.as_os_str().is_empty() {
        return Err(FilePolicyDenyReason::EmptyPath);
    }
    let path = if requested_path.is_absolute() {
        requested_path.to_path_buf()
    } else {
        Path::new(snapshot.sandbox.cwd.as_str()).join(requested_path)
    };
    Ok(normalize_lexically(path))
}

fn resolve_requested_path(
    operation: FilePolicyOperation,
    absolute_path: &Path,
) -> Result<PathBuf, FilePolicyDenyReason> {
    match std::fs::canonicalize(absolute_path) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if operation == FilePolicyOperation::Write {
                resolve_missing_write_path(absolute_path)
            } else {
                Err(FilePolicyDenyReason::MissingPath)
            }
        }
        Err(_) => Err(FilePolicyDenyReason::MissingPath),
    }
}

fn resolve_policy_root(
    operation: FilePolicyOperation,
    absolute_root: &Path,
) -> Result<PathBuf, FilePolicyDenyReason> {
    match std::fs::canonicalize(absolute_root) {
        Ok(path) => Ok(path),
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                && operation == FilePolicyOperation::Write =>
        {
            resolve_missing_write_path(absolute_root)
        }
        Err(_) => Err(FilePolicyDenyReason::InvalidRoot),
    }
}

fn resolve_missing_write_path(absolute_path: &Path) -> Result<PathBuf, FilePolicyDenyReason> {
    let mut missing_components = Vec::new();
    let mut current = absolute_path;
    loop {
        match std::fs::symlink_metadata(current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(FilePolicyDenyReason::SymlinkEscape);
                }
                if !metadata.is_dir() {
                    return Err(FilePolicyDenyReason::MissingPath);
                }
                let canonical = std::fs::canonicalize(current)
                    .map_err(|_| FilePolicyDenyReason::MissingPath)?;
                let mut resolved = canonical;
                for component in missing_components.iter().rev() {
                    resolved.push(component);
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let name = current
                    .file_name()
                    .ok_or(FilePolicyDenyReason::MissingPath)?;
                missing_components.push(name.to_owned());
                current = current.parent().ok_or(FilePolicyDenyReason::MissingPath)?;
            }
            Err(_) => return Err(FilePolicyDenyReason::MissingPath),
        }
    }
}

fn resolve_existing_write_anchor(
    resolved_destination: &Path,
) -> Result<PathBuf, FilePolicyDenyReason> {
    let mut current = resolved_destination;
    let mut is_destination = true;
    loop {
        match std::fs::symlink_metadata(current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(FilePolicyDenyReason::SymlinkEscape);
                }
                if metadata.is_dir() {
                    return std::fs::canonicalize(current)
                        .map_err(|_| FilePolicyDenyReason::InvalidRoot);
                }
                if !is_destination {
                    return Err(FilePolicyDenyReason::MissingPath);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(FilePolicyDenyReason::MissingPath),
        }
        current = current.parent().ok_or(FilePolicyDenyReason::MissingPath)?;
        is_destination = false;
    }
}

fn entry_root_path(
    snapshot: &TurnExecutionSecuritySnapshot,
    entry: &TurnFilesystemSandboxEntry,
) -> Option<PathBuf> {
    if let Some(path) = entry.resolved_path.as_deref() {
        return Some(PathBuf::from(path));
    }
    match &entry.path {
        TurnFilesystemSandboxPath::Root => {
            Some(PathBuf::from(std::path::MAIN_SEPARATOR.to_string()))
        }
        TurnFilesystemSandboxPath::CurrentWorkingDirectory
        | TurnFilesystemSandboxPath::WorkspaceRoot => {
            Some(PathBuf::from(snapshot.sandbox.cwd.as_str()))
        }
        TurnFilesystemSandboxPath::ExplicitPath { path } => Some(PathBuf::from(path)),
        TurnFilesystemSandboxPath::SlashTmp | TurnFilesystemSandboxPath::Tmpdir => {
            Some(std::env::temp_dir())
        }
        TurnFilesystemSandboxPath::ProjectRoot { .. } | TurnFilesystemSandboxPath::RuntimeHome => {
            None
        }
    }
}

fn access_allows(access: TurnFilesystemAccess, operation: FilePolicyOperation) -> bool {
    matches!(
        (access, operation),
        (TurnFilesystemAccess::Write, FilePolicyOperation::Read)
            | (TurnFilesystemAccess::Write, FilePolicyOperation::Write)
            | (TurnFilesystemAccess::Read, FilePolicyOperation::Read)
    )
}

fn normalize_lexically(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                // Never pop a platform root/prefix.  Allowing `/../x` to
                // become the relative path `x` would make the subsequent
                // canonicalize call resolve against the process cwd rather
                // than the sandbox cwd.
                let can_pop = !matches!(
                    normalized.components().next_back(),
                    Some(Component::RootDir | Component::Prefix(_))
                );
                if can_pop {
                    normalized.pop();
                }
            }
            Component::Normal(value) => normalized.push(value),
            Component::RootDir | Component::Prefix(_) => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn deny_for(
    operation: FilePolicyOperation,
    requested_path: PathBuf,
    resolved_path: Option<PathBuf>,
    reason: FilePolicyDenyReason,
) -> FilePolicyDecision {
    FilePolicyDecision::Denied(FilePolicyDeny {
        operation,
        requested_path,
        resolved_path,
        reason,
        message: deny_message(reason),
    })
}

fn deny_message(reason: FilePolicyDenyReason) -> String {
    match reason {
        FilePolicyDenyReason::EmptyPath => "filesystem path is empty".to_owned(),
        FilePolicyDenyReason::MissingPath => "filesystem path does not exist".to_owned(),
        FilePolicyDenyReason::OutsideAllowedRoots => {
            "filesystem path is outside the allowed sandbox roots".to_owned()
        }
        FilePolicyDenyReason::SymlinkEscape => {
            "filesystem path resolves outside an allowed root through a symlink".to_owned()
        }
        FilePolicyDenyReason::WriteRequiresWritableRoot => {
            "filesystem path is only covered by a read-only sandbox root".to_owned()
        }
        FilePolicyDenyReason::NoUsableRoots => {
            "filesystem sandbox has no usable roots for this path".to_owned()
        }
        FilePolicyDenyReason::InvalidRoot => {
            "filesystem sandbox roots could not be resolved".to_owned()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::file_mutation::{
        StagedFile, TargetExpectation, TargetResolver, TargetRole, open_regular_file_at,
    };
    use pioneer_protocol::{
        TurnExecutionSecuritySnapshot, TurnFilesystemSandboxPath, TurnPermissionMode,
        TurnPermissionProfileSnapshot, TurnPermissionProfileSource, TurnSecurityRuleProvenance,
    };
    use std::io::Read;

    fn workspace_write_snapshot(root: &Path) -> TurnExecutionSecuritySnapshot {
        TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            root.to_string_lossy(),
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Write,
                root.to_string_lossy(),
            )],
            1,
        )
    }

    #[test]
    fn file_policy_allows_path_inside_writable_root() {
        let temp = tempfile::tempdir().expect("tempdir");
        let file = temp.path().join("src.txt");
        std::fs::write(file.as_path(), "ok").expect("write test file");
        let snapshot = workspace_write_snapshot(temp.path());

        let decision = FilePolicyChecker::check_read(&snapshot, file.as_path());

        let grant = decision.grant().expect("path should be allowed");
        assert_eq!(grant.operation, FilePolicyOperation::Read);
        assert_eq!(grant.access, TurnFilesystemAccess::Write);
        assert_eq!(grant.resolved_path, std::fs::canonicalize(file).unwrap());
    }

    #[test]
    fn file_policy_denies_path_outside_root() {
        let root = tempfile::tempdir().expect("root tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let file = outside.path().join("outside.txt");
        std::fs::write(file.as_path(), "outside").expect("write outside file");
        let snapshot = workspace_write_snapshot(root.path());

        let decision = FilePolicyChecker::check_read(&snapshot, file.as_path());

        let deny = decision.deny().expect("outside path should be denied");
        assert_eq!(deny.reason, FilePolicyDenyReason::OutsideAllowedRoots);
    }

    #[test]
    fn file_policy_denies_missing_read_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let snapshot = workspace_write_snapshot(temp.path());

        let decision = FilePolicyChecker::check_read(&snapshot, temp.path().join("missing.txt"));

        let deny = decision.deny().expect("missing read should be denied");
        assert_eq!(deny.reason, FilePolicyDenyReason::MissingPath);
    }

    #[test]
    fn file_policy_allows_missing_write_path_under_writable_root() {
        let temp = tempfile::tempdir().expect("tempdir");
        let snapshot = workspace_write_snapshot(temp.path());

        let decision = FilePolicyChecker::check_write(&snapshot, temp.path().join("new.txt"));

        let grant = decision.grant().expect("new file write should be allowed");
        assert!(grant.resolved_path.ends_with("new.txt"));
    }

    #[test]
    fn file_policy_allows_write_when_the_exact_writable_root_is_not_created_yet() {
        let temp = tempfile::tempdir().expect("tempdir");
        let future_root = temp.path().join("new").join("nested");
        let target = future_root.join("created.txt");
        let sibling = temp.path().join("new").join("sibling.txt");
        let expected_root = temp
            .path()
            .canonicalize()
            .expect("canonical tempdir")
            .join("new")
            .join("nested");
        let expected_target = expected_root.join("created.txt");
        let snapshot = TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            temp.path().to_string_lossy(),
            vec![TurnFilesystemSandboxEntry {
                path: TurnFilesystemSandboxPath::ExplicitPath {
                    path: future_root.display().to_string(),
                },
                access: TurnFilesystemAccess::Write,
                provenance: TurnSecurityRuleProvenance::Runtime,
                resolved_path: Some(future_root.display().to_string()),
            }],
            1,
        );

        assert!(!future_root.exists(), "the grant root must start missing");
        let grant = FilePolicyChecker::check_write(&snapshot, target.as_path())
            .grant()
            .expect("an exact future write root should authorize its descendant")
            .clone();
        assert_eq!(grant.resolved_path, expected_target);
        assert_eq!(grant.matched_root.as_deref(), Some(expected_root.as_path()));
        assert_eq!(
            FilePolicyChecker::write_anchor(&grant).expect("existing secure anchor"),
            temp.path().canonicalize().expect("canonical tempdir")
        );
        assert_eq!(
            FilePolicyChecker::allowed_roots(&snapshot, FilePolicyOperation::Write),
            vec![expected_root]
        );

        let sibling_decision = FilePolicyChecker::check_write(&snapshot, sibling);
        let deny = sibling_decision
            .deny()
            .expect("a future root grant must not authorize its sibling");
        assert_eq!(deny.reason, FilePolicyDenyReason::OutsideAllowedRoots);
    }

    #[cfg(unix)]
    #[test]
    fn file_policy_denies_future_write_root_through_symlink_ancestor() {
        let temp = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let link = temp.path().join("linked");
        std::os::unix::fs::symlink(outside.path(), link.as_path()).expect("symlink");
        let future_root = link.join("new");
        let snapshot = TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            temp.path().to_string_lossy(),
            vec![TurnFilesystemSandboxEntry {
                path: TurnFilesystemSandboxPath::ExplicitPath {
                    path: future_root.display().to_string(),
                },
                access: TurnFilesystemAccess::Write,
                provenance: TurnSecurityRuleProvenance::Runtime,
                resolved_path: Some(future_root.display().to_string()),
            }],
            1,
        );

        let decision = FilePolicyChecker::check_write(&snapshot, future_root.join("escaped.txt"));
        let deny = decision
            .deny()
            .expect("a future root through a symlink must fail closed");
        assert_eq!(deny.reason, FilePolicyDenyReason::SymlinkEscape);
    }

    #[test]
    fn relative_composer_root_resolves_from_the_turn_cwd() {
        let temp = tempfile::tempdir().expect("tempdir");
        let allowed = temp.path().join("additional");
        std::fs::create_dir_all(&allowed).expect("create additional root");
        let file = allowed.join("file.txt");
        std::fs::write(&file, "ok").expect("write test file");
        let snapshot = TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            temp.path().to_string_lossy(),
            vec![TurnFilesystemSandboxEntry {
                path: TurnFilesystemSandboxPath::ExplicitPath {
                    path: "additional".to_owned(),
                },
                access: TurnFilesystemAccess::Write,
                provenance: TurnSecurityRuleProvenance::ComposerSelection,
                resolved_path: None,
            }],
            1,
        );

        let decision = FilePolicyChecker::check_read(&snapshot, "additional/file.txt");

        let grant = decision
            .grant()
            .expect("relative Composer root should be allowed");
        assert_eq!(grant.resolved_path, file.canonicalize().unwrap());
        assert_eq!(
            FilePolicyChecker::allowed_roots(&snapshot, FilePolicyOperation::Write),
            vec![allowed.canonicalize().unwrap()]
        );
    }

    #[test]
    fn file_policy_unrestricted_snapshot_allows_normal_existing_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let file = temp.path().join("file.txt");
        std::fs::write(file.as_path(), "ok").expect("write test file");
        let snapshot = TurnExecutionSecuritySnapshot::unrestricted_full_access(
            temp.path().to_string_lossy(),
            1,
        );

        let decision = FilePolicyChecker::check_read(&snapshot, file.as_path());

        assert!(decision.is_allowed());
    }

    #[test]
    fn file_policy_path_escape_denies_parent_traversal_read() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(root.as_path()).expect("create root");
        std::fs::create_dir_all(outside.as_path()).expect("create outside");
        let outside_file = outside.join("secret.txt");
        std::fs::write(outside_file.as_path(), "secret").expect("write outside file");
        let snapshot = workspace_write_snapshot(root.as_path());

        let decision = FilePolicyChecker::check_read(&snapshot, "../outside/secret.txt");

        let deny = decision
            .deny()
            .expect("parent traversal read should be denied");
        assert_eq!(deny.reason, FilePolicyDenyReason::OutsideAllowedRoots);
        let expected = std::fs::canonicalize(outside_file).unwrap();
        assert_eq!(deny.resolved_path.as_deref(), Some(expected.as_path()));
    }

    #[test]
    fn file_policy_path_escape_unrestricted_allows_parent_traversal_read() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(root.as_path()).expect("create root");
        std::fs::create_dir_all(outside.as_path()).expect("create outside");
        let outside_file = outside.join("allowed.txt");
        std::fs::write(outside_file.as_path(), "allowed").expect("write outside file");
        let snapshot =
            TurnExecutionSecuritySnapshot::unrestricted_full_access(root.to_string_lossy(), 1);

        let decision = FilePolicyChecker::check_read(&snapshot, "../outside/allowed.txt");

        assert!(decision.is_allowed());
        assert_eq!(
            decision
                .grant()
                .expect("full access should allow traversal")
                .resolved_path,
            std::fs::canonicalize(outside_file).unwrap()
        );
    }

    #[test]
    fn absolute_parent_traversal_keeps_the_path_absolute() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("workspace");
        std::fs::create_dir_all(root.as_path()).expect("create root");
        let snapshot =
            TurnExecutionSecuritySnapshot::unrestricted_full_access(root.to_string_lossy(), 1);

        let decision = FilePolicyChecker::check_read(&snapshot, Path::new("/../tmp"));

        assert_eq!(
            decision.grant().map(|grant| grant.requested_path.clone()),
            Some(PathBuf::from("/tmp"))
        );
    }

    #[cfg(unix)]
    #[test]
    fn file_policy_denies_symlink_escape() {
        let root = tempfile::tempdir().expect("root tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let outside_file = outside.path().join("outside.txt");
        std::fs::write(outside_file.as_path(), "outside").expect("write outside file");
        let link = root.path().join("link.txt");
        std::os::unix::fs::symlink(outside_file.as_path(), link.as_path()).expect("symlink");
        let snapshot = workspace_write_snapshot(root.path());

        let decision = FilePolicyChecker::check_read(&snapshot, link.as_path());

        let deny = decision.deny().expect("symlink escape should be denied");
        assert_eq!(deny.reason, FilePolicyDenyReason::SymlinkEscape);
    }

    #[cfg(unix)]
    #[test]
    fn file_policy_path_escape_denies_write_through_symlink_parent() {
        let root = tempfile::tempdir().expect("root tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let link = root.path().join("linked-dir");
        std::os::unix::fs::symlink(outside.path(), link.as_path()).expect("symlink");
        let snapshot = workspace_write_snapshot(root.path());

        let decision = FilePolicyChecker::check_write(&snapshot, link.join("created.txt"));

        let deny = decision
            .deny()
            .expect("write through symlinked parent should be denied");
        assert_eq!(deny.reason, FilePolicyDenyReason::SymlinkEscape);
    }

    #[cfg(unix)]
    #[test]
    fn file_policy_capability_pins_read_object_after_path_swap() {
        let root = tempfile::tempdir().expect("authorized root");
        let outside = tempfile::tempdir().expect("outside root");
        let checked = root.path().join("checked.txt");
        let moved = root.path().join("checked-original.txt");
        let outside_file = outside.path().join("secret.txt");
        std::fs::write(&checked, b"authorized-content").expect("checked file");
        std::fs::write(&outside_file, b"outside-content").expect("outside file");
        let snapshot = workspace_write_snapshot(root.path());

        let grant = match FilePolicyChecker::check_read(&snapshot, &checked) {
            FilePolicyDecision::Allowed(grant) => grant,
            decision => panic!("expected read grant, got {decision:?}"),
        };
        std::fs::rename(&checked, &moved).expect("move checked pathname");
        std::os::unix::fs::symlink(&outside_file, &checked).expect("replace with escape link");

        let mut opened = grant
            .capability
            .open_regular_file()
            .expect("grant should retain the originally checked object");
        let mut content = String::new();
        opened
            .read_to_string(&mut content)
            .expect("opened capability should be readable");
        assert_eq!(content, "authorized-content");
    }

    #[cfg(unix)]
    #[test]
    fn file_policy_capability_pins_write_anchor_after_path_swap() {
        let root = tempfile::tempdir().expect("policy root");
        let authorized = root.path().join("authorized");
        let outside = tempfile::tempdir().expect("outside root");
        std::fs::create_dir(&authorized).expect("authorized directory");
        let destination = authorized.join("created.txt");
        let resolver = TargetResolver::new(&authorized).expect("target resolver");
        let target = resolver
            .resolve(
                "created.txt",
                TargetRole::Destination,
                TargetExpectation::ExistingOrMissing,
            )
            .expect("destination target");
        let snapshot = workspace_write_snapshot(root.path());
        let grant = match FilePolicyChecker::check_write(&snapshot, &destination) {
            FilePolicyDecision::Allowed(grant) => grant,
            decision => panic!("expected write grant, got {decision:?}"),
        };

        let moved = root.path().join("authorized-original");
        std::fs::rename(&authorized, &moved).expect("move authorized pathname");
        std::os::unix::fs::symlink(outside.path(), &authorized).expect("replace root path");

        let anchor = grant.capability.anchor().expect("anchor handle");
        let mut staged =
            StagedFile::create_at(&anchor, grant.capability.relative_path(), &target, true)
                .expect("stage through the retained anchor");
        staged
            .write_all(b"written-through-capability")
            .expect("write stage");
        staged.sync_all().expect("sync stage");
        let published = staged.publish_replace(false).expect("publish stage");
        published.sync_all().expect("sync parent");

        assert_eq!(
            std::fs::read(moved.join("created.txt")).unwrap(),
            b"written-through-capability"
        );
        assert!(!outside.path().join("created.txt").exists());
    }

    #[cfg(unix)]
    #[test]
    fn descriptor_relative_open_uses_the_retained_directory_object() {
        let root = tempfile::tempdir().expect("root");
        let file = root.path().join("value.txt");
        std::fs::write(&file, b"value").expect("file");
        let directory = open_directory(&root.path()).expect("directory handle");
        let mut opened = open_regular_file_at(&directory, Path::new("value.txt"))
            .expect("descriptor-relative file");
        let mut content = String::new();
        opened.read_to_string(&mut content).expect("read value");
        assert_eq!(content, "value");
    }
}
