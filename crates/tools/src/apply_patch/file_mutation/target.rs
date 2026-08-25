//! Canonical target resolution for Apply Patch file operations.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::fs;
use std::path::{Component, Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetRole {
    Source,
    Destination,
    Parent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetExpectation {
    ExistingRegular,
    Missing,
    ExistingOrMissing,
    ParentDirectory,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    Missing,
    RegularFile,
    Directory,
    Symlink,
    Special,
}

/// Stable identity observed for a path while a patch is prepared.  The
/// identity is deliberately derived from `symlink_metadata`, so replacing a
/// parent directory with another directory at the same pathname is detected
/// even though both paths still have the same kind.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TargetMetadataFingerprint {
    pub kind: TargetKind,
    pub identity: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalTarget {
    relative: PathBuf,
    absolute: PathBuf,
    identity: String,
    pub role: TargetRole,
    pub expectation: TargetExpectation,
}

impl CanonicalTarget {
    pub fn relative(&self) -> &Path {
        &self.relative
    }

    pub fn absolute(&self) -> &Path {
        &self.absolute
    }

    pub fn identity(&self) -> &str {
        &self.identity
    }

    pub fn is_same_identity(&self, other: &Self) -> bool {
        self.identity == other.identity
    }

    pub fn inspect_kind(&self) -> Result<TargetKind, TargetResolutionError> {
        let metadata = match fs::symlink_metadata(&self.absolute) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(TargetKind::Missing);
            }
            Err(_) => {
                return Err(TargetResolutionError::new(
                    TargetResolutionErrorCode::MetadataUnavailable,
                ));
            }
        };
        let file_type = metadata.file_type();
        if file_type.is_symlink() {
            Ok(TargetKind::Symlink)
        } else if file_type.is_file() {
            Ok(TargetKind::RegularFile)
        } else if file_type.is_dir() {
            Ok(TargetKind::Directory)
        } else {
            Ok(TargetKind::Special)
        }
    }

    pub fn metadata_fingerprint(&self) -> Result<TargetMetadataFingerprint, TargetResolutionError> {
        metadata_fingerprint_for_path(&self.absolute)
    }
}

/// Capture a stable identity for an arbitrary already-resolved path.  Recovery
/// uses this instead of rebuilding a `CanonicalTarget` from a persisted string,
/// which would otherwise make a replaced workspace root or parent directory
/// indistinguishable from the originally authorized object.
pub fn metadata_fingerprint_for_path(
    path: &Path,
) -> Result<TargetMetadataFingerprint, TargetResolutionError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(TargetMetadataFingerprint {
                kind: TargetKind::Missing,
                identity: "missing".to_owned(),
            });
        }
        Err(_) => {
            return Err(TargetResolutionError::new(
                TargetResolutionErrorCode::MetadataUnavailable,
            ));
        }
    };
    let kind = if metadata.file_type().is_symlink() {
        TargetKind::Symlink
    } else if metadata.file_type().is_file() {
        TargetKind::RegularFile
    } else if metadata.file_type().is_dir() {
        TargetKind::Directory
    } else {
        TargetKind::Special
    };
    #[cfg(windows)]
    let identity = windows_filesystem_identity(path)
        .map_err(|_| TargetResolutionError::new(TargetResolutionErrorCode::MetadataUnavailable))?;
    #[cfg(not(windows))]
    let identity = filesystem_identity(&metadata);
    Ok(TargetMetadataFingerprint { kind, identity })
}

impl CanonicalTarget {
    pub fn validate_expectation(&self) -> Result<TargetKind, TargetResolutionError> {
        let kind = self.inspect_kind()?;
        let valid = match self.expectation {
            TargetExpectation::ExistingRegular => kind == TargetKind::RegularFile,
            TargetExpectation::Missing => kind == TargetKind::Missing,
            TargetExpectation::ExistingOrMissing => {
                matches!(kind, TargetKind::Missing | TargetKind::RegularFile)
            }
            TargetExpectation::ParentDirectory => kind == TargetKind::Directory,
        };
        if !valid {
            return Err(TargetResolutionError::for_kind(kind, self.expectation));
        }
        Ok(kind)
    }
}

#[derive(Clone, Debug)]
pub struct TargetResolver {
    root: PathBuf,
    allow_absolute: bool,
    case_sensitive: bool,
    deny_symlinks: bool,
}

impl TargetResolver {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, TargetResolutionError> {
        let root = normalize_absolute_root(&root.into())?;
        let case_sensitive = detect_case_sensitivity(&root);
        Ok(Self {
            root,
            allow_absolute: false,
            case_sensitive,
            deny_symlinks: true,
        })
    }

    pub fn with_absolute_paths(mut self, allow: bool) -> Self {
        self.allow_absolute = allow;
        self
    }

    pub fn with_case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    pub fn with_symlink_policy(mut self, deny_symlinks: bool) -> Self {
        self.deny_symlinks = deny_symlinks;
        self
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns the workspace root as a locked/revalidated parent target.
    ///
    /// A root-level file has no lexical parent component below `root`, but the
    /// root itself is still part of the authorization boundary.  Treating it
    /// as an explicit parent target prevents a root directory replacement (or
    /// symlink swap) from becoming invisible to the prepare-to-commit checks.
    pub fn root_parent_target(&self) -> CanonicalTarget {
        CanonicalTarget {
            relative: PathBuf::from("."),
            absolute: self.root.clone(),
            identity: identity_for(&self.root, self.case_sensitive),
            role: TargetRole::Parent,
            expectation: TargetExpectation::ParentDirectory,
        }
    }

    pub fn resolve(
        &self,
        input: &str,
        role: TargetRole,
        expectation: TargetExpectation,
    ) -> Result<CanonicalTarget, TargetResolutionError> {
        if input.is_empty() || input.contains('\0') {
            return Err(TargetResolutionError::new(
                TargetResolutionErrorCode::InvalidPath,
            ));
        }
        let path = Path::new(input);
        if path.is_absolute() && !self.allow_absolute {
            return Err(TargetResolutionError::new(
                TargetResolutionErrorCode::AbsolutePathDenied,
            ));
        }
        let absolute = if path.is_absolute() {
            normalize_absolute_path(path)?
        } else {
            join_relative(&self.root, path)?
        };
        if !absolute.starts_with(&self.root) {
            return Err(TargetResolutionError::new(
                TargetResolutionErrorCode::EscapesRoot,
            ));
        }
        let relative = absolute
            .strip_prefix(&self.root)
            .map_err(|_| TargetResolutionError::new(TargetResolutionErrorCode::EscapesRoot))?
            .to_path_buf();
        if relative.as_os_str().is_empty() {
            return Err(TargetResolutionError::new(
                TargetResolutionErrorCode::RootTargetDenied,
            ));
        }
        if self.deny_symlinks && has_symlink_component(&self.root, &relative) {
            return Err(TargetResolutionError::new(
                TargetResolutionErrorCode::SymlinkDenied,
            ));
        }
        let identity = identity_for(&absolute, self.case_sensitive);
        Ok(CanonicalTarget {
            relative,
            absolute,
            identity,
            role,
            expectation,
        })
    }

    pub fn manifest<'a>(
        &self,
        inputs: impl IntoIterator<Item = (&'a str, TargetRole, TargetExpectation)>,
    ) -> Result<TargetManifest, TargetResolutionError> {
        let targets = inputs
            .into_iter()
            .map(|(path, role, expectation)| self.resolve(path, role, expectation))
            .collect::<Result<Vec<_>, _>>()?;
        TargetManifest::new(targets)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TargetManifest {
    targets: Vec<CanonicalTarget>,
}

impl TargetManifest {
    pub fn new(mut targets: Vec<CanonicalTarget>) -> Result<Self, TargetResolutionError> {
        targets.sort_by(|left, right| {
            left.identity
                .cmp(&right.identity)
                .then_with(|| role_order(left.role).cmp(&role_order(right.role)))
        });
        let mut deduped: Vec<CanonicalTarget> = Vec::with_capacity(targets.len());
        for target in targets {
            match deduped.last() {
                Some(previous) if previous.identity == target.identity => {
                    if previous.absolute != target.absolute {
                        return Err(TargetResolutionError::new(
                            TargetResolutionErrorCode::IdentityCollision,
                        ));
                    }
                }
                _ => deduped.push(target),
            }
        }
        Ok(Self { targets: deduped })
    }

    pub fn targets(&self) -> &[CanonicalTarget] {
        &self.targets
    }

    pub fn identities(&self) -> impl Iterator<Item = &str> {
        self.targets.iter().map(CanonicalTarget::identity)
    }

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}

fn role_order(role: TargetRole) -> u8 {
    match role {
        TargetRole::Source => 0,
        TargetRole::Destination => 1,
        TargetRole::Parent => 2,
    }
}

fn identity_for(path: &Path, case_sensitive: bool) -> String {
    let value = path.to_string_lossy().replace('\\', "/");
    if case_sensitive {
        value
    } else {
        value.to_lowercase()
    }
}

/// Determine the path-name semantics of the workspace without creating or
/// modifying anything.  A compile-time Unix/Windows guess is wrong for
/// case-insensitive macOS volumes (and for mounted volumes with the opposite
/// policy), so probe an existing component using an alternate ASCII casing.
/// When the probe cannot decide, use the conservative platform default: reject
/// case aliases on macOS and preserve case on other Unix filesystems.
#[cfg(not(target_os = "windows"))]
fn detect_case_sensitivity(root: &Path) -> bool {
    let mut current = PathBuf::new();
    for component in root.components() {
        current.push(component.as_os_str());
        let Some(name) = component.as_os_str().to_str() else {
            continue;
        };
        let alternate = flip_ascii_case(name);
        if alternate == name {
            continue;
        }
        let Some(parent) = current.parent() else {
            continue;
        };
        let alternate_path = parent.join(alternate);
        let Ok(canonical_current) = fs::canonicalize(&current) else {
            continue;
        };
        match fs::canonicalize(&alternate_path) {
            Ok(canonical_alternate) => return canonical_current != canonical_alternate,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return true,
            Err(_) => continue,
        }
    }

    cfg!(not(target_os = "macos"))
}

#[cfg(target_os = "windows")]
fn detect_case_sensitivity(_root: &Path) -> bool {
    false
}

#[cfg(not(target_os = "windows"))]
fn flip_ascii_case(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_lowercase() {
                character.to_ascii_uppercase()
            } else if character.is_ascii_uppercase() {
                character.to_ascii_lowercase()
            } else {
                character
            }
        })
        .collect()
}

#[cfg(not(windows))]
fn filesystem_identity(metadata: &fs::Metadata) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        return format!("unix:{}:{}", metadata.dev(), metadata.ino());
    }
    #[cfg(not(unix))]
    {
        let modified = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|duration| duration.as_nanos().to_string())
            .unwrap_or_else(|| "unknown".to_owned());
        format!("metadata:{}:{}", metadata.len(), modified)
    }
}

#[cfg(windows)]
fn windows_filesystem_identity(path: &Path) -> std::io::Result<String> {
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
        return Err(std::io::Error::last_os_error());
    }
    let file_index =
        (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow);
    Ok(format!(
        "windows:{}:{}",
        information.dwVolumeSerialNumber, file_index
    ))
}

fn normalize_absolute_root(root: &Path) -> Result<PathBuf, TargetResolutionError> {
    if !root.is_absolute() {
        return Err(TargetResolutionError::new(
            TargetResolutionErrorCode::RootMustBeAbsolute,
        ));
    }
    normalize_absolute_path(root)
}

fn normalize_absolute_path(path: &Path) -> Result<PathBuf, TargetResolutionError> {
    let mut output = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => output.push(prefix.as_os_str()),
            Component::RootDir => output.push(Path::new(std::path::MAIN_SEPARATOR_STR)),
            Component::CurDir => {}
            Component::Normal(value) => output.push(value),
            Component::ParentDir => {
                if !output.pop() {
                    return Err(TargetResolutionError::new(
                        TargetResolutionErrorCode::EscapesRoot,
                    ));
                }
            }
        }
    }
    if !output.is_absolute() {
        return Err(TargetResolutionError::new(
            TargetResolutionErrorCode::InvalidPath,
        ));
    }
    Ok(output)
}

fn join_relative(root: &Path, relative: &Path) -> Result<PathBuf, TargetResolutionError> {
    let mut output = root.to_path_buf();
    for component in relative.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(value) => output.push(value),
            Component::ParentDir => {
                if !output.pop() || !output.starts_with(root) {
                    return Err(TargetResolutionError::new(
                        TargetResolutionErrorCode::EscapesRoot,
                    ));
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(TargetResolutionError::new(
                    TargetResolutionErrorCode::InvalidPath,
                ));
            }
        }
    }
    Ok(output)
}

fn has_symlink_component(root: &Path, relative: &Path) -> bool {
    let mut current = PathBuf::new();
    for component in root.components().chain(relative.components()) {
        if matches!(component, Component::CurDir) {
            continue;
        }
        current.push(component.as_os_str());
        let is_symlink = fs::symlink_metadata(&current)
            .map(|metadata| metadata.file_type().is_symlink())
            .unwrap_or(false);
        if is_symlink && !trusted_platform_root_symlink(&current) {
            return true;
        }
    }
    false
}

/// macOS exposes a few immutable compatibility aliases before every
/// application-owned path (notably `/var -> /private/var`).  They are safe to
/// traverse as platform roots; a configured workspace root or any descendant
/// symlink remains rejected by the resolver.
fn trusted_platform_root_symlink(path: &Path) -> bool {
    #[cfg(target_os = "macos")]
    {
        if !matches!(path.to_str(), Some("/var" | "/tmp" | "/etc")) {
            return false;
        }
        return fs::canonicalize(path)
            .ok()
            .and_then(|resolved| fs::metadata(resolved).ok())
            .is_some_and(|metadata| metadata.is_dir());
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = path;
        false
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetResolutionErrorCode {
    InvalidPath,
    RootMustBeAbsolute,
    AbsolutePathDenied,
    EscapesRoot,
    RootTargetDenied,
    SymlinkDenied,
    IdentityCollision,
    MetadataUnavailable,
    ExpectationMismatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TargetResolutionError {
    pub code: TargetResolutionErrorCode,
    pub actual: Option<TargetKind>,
    pub expected: Option<TargetExpectation>,
}

impl TargetResolutionError {
    pub const fn new(code: TargetResolutionErrorCode) -> Self {
        Self {
            code,
            actual: None,
            expected: None,
        }
    }

    pub const fn for_kind(actual: TargetKind, expected: TargetExpectation) -> Self {
        Self {
            code: TargetResolutionErrorCode::ExpectationMismatch,
            actual: Some(actual),
            expected: Some(expected),
        }
    }
}

impl fmt::Display for TargetResolutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "target resolution failed: {:?}", self.code)
    }
}

impl std::error::Error for TargetResolutionError {}

impl Ord for CanonicalTarget {
    fn cmp(&self, other: &Self) -> Ordering {
        self.identity.cmp(&other.identity)
    }
}

impl PartialOrd for CanonicalTarget {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn resolver_rejects_traversal_absolute_and_nul() {
        let root = tempdir().unwrap();
        let resolver = TargetResolver::new(root.path()).unwrap();
        assert_eq!(
            resolver
                .resolve(
                    "../escape.txt",
                    TargetRole::Source,
                    TargetExpectation::Missing
                )
                .unwrap_err()
                .code,
            TargetResolutionErrorCode::EscapesRoot
        );
        assert_eq!(
            resolver
                .resolve(
                    &root.path().join("outside.txt").to_string_lossy(),
                    TargetRole::Source,
                    TargetExpectation::Missing
                )
                .unwrap_err()
                .code,
            TargetResolutionErrorCode::AbsolutePathDenied
        );
        assert_eq!(
            resolver
                .resolve("a\0b", TargetRole::Source, TargetExpectation::Missing)
                .unwrap_err()
                .code,
            TargetResolutionErrorCode::InvalidPath
        );
    }

    #[test]
    fn manifest_is_sorted_and_deduplicated() {
        let root = tempdir().unwrap();
        let resolver = TargetResolver::new(root.path()).unwrap();
        let first = resolver
            .resolve("z.txt", TargetRole::Source, TargetExpectation::Missing)
            .unwrap();
        let second = resolver
            .resolve("a.txt", TargetRole::Destination, TargetExpectation::Missing)
            .unwrap();
        let duplicate = resolver
            .resolve("./a.txt", TargetRole::Source, TargetExpectation::Missing)
            .unwrap();
        let manifest = TargetManifest::new(vec![first, second, duplicate]).unwrap();
        assert_eq!(manifest.targets().len(), 2);
        assert_eq!(manifest.targets()[0].relative(), Path::new("a.txt"));
    }

    #[test]
    fn case_fold_collision_is_rejected() {
        let root = tempdir().unwrap();
        let resolver = TargetResolver::new(root.path())
            .unwrap()
            .with_case_sensitive(false);
        let upper = resolver
            .resolve("A.txt", TargetRole::Source, TargetExpectation::Missing)
            .unwrap();
        let lower = resolver
            .resolve("a.txt", TargetRole::Source, TargetExpectation::Missing)
            .unwrap();
        assert_eq!(
            TargetManifest::new(vec![upper, lower]).unwrap_err().code,
            TargetResolutionErrorCode::IdentityCollision
        );
    }

    #[test]
    fn case_only_rename_is_rejected_as_an_alias_on_case_insensitive_volumes() {
        let root = tempdir().unwrap();
        std::fs::write(root.path().join("Name.txt"), b"content\n").unwrap();
        let resolver = TargetResolver::new(root.path())
            .unwrap()
            .with_case_sensitive(false);
        let source = resolver
            .resolve(
                "Name.txt",
                TargetRole::Source,
                TargetExpectation::ExistingRegular,
            )
            .unwrap();
        let destination = resolver
            .resolve(
                "name.txt",
                TargetRole::Destination,
                TargetExpectation::ExistingOrMissing,
            )
            .unwrap();

        assert_eq!(source.identity(), destination.identity());
        assert_eq!(
            TargetManifest::new(vec![source, destination])
                .unwrap_err()
                .code,
            TargetResolutionErrorCode::IdentityCollision
        );
        assert_eq!(
            std::fs::read(root.path().join("Name.txt")).unwrap(),
            b"content\n"
        );
    }

    #[test]
    fn symlink_component_is_denied_without_following_it() {
        let root = tempdir().unwrap();
        let real = root.path().join("real");
        std::fs::create_dir(&real).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real, root.path().join("link")).unwrap();
        #[cfg(unix)]
        {
            let resolver = TargetResolver::new(root.path()).unwrap();
            assert_eq!(
                resolver
                    .resolve(
                        "link/file.txt",
                        TargetRole::Source,
                        TargetExpectation::Missing
                    )
                    .unwrap_err()
                    .code,
                TargetResolutionErrorCode::SymlinkDenied
            );
        }
    }

    #[test]
    fn symlinked_root_is_denied_without_following_it() {
        #[cfg(unix)]
        {
            let root = tempdir().unwrap();
            let real = root.path().join("real");
            std::fs::create_dir(&real).unwrap();
            let link = root.path().join("link");
            std::os::unix::fs::symlink(&real, &link).unwrap();
            let resolver = TargetResolver::new(&link).unwrap();
            assert_eq!(
                resolver
                    .resolve("file.txt", TargetRole::Source, TargetExpectation::Missing)
                    .unwrap_err()
                    .code,
                TargetResolutionErrorCode::SymlinkDenied
            );
        }
    }
}
