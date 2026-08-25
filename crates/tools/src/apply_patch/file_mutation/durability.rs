//! Durability helpers for Apply Patch filesystem mutations.

use std::fs;
use std::io;
use std::path::Path;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultPoint {
    CrossDevice,
    StageCreate,
    StageWrite,
    FileSync,
    Rename,
    ParentSync,
    Delete,
    Metadata,
    Cleanup,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub struct FaultPlan {
    pub fail_at: Option<FaultPoint>,
    /// Optional one-based private-stage attempt to fail. This is deterministic
    /// per `FileMutationEngine` and exercises multi-file pre-staging without
    /// relying on platform disk/permission behavior.
    pub fail_stage_attempt: Option<u32>,
}

impl FaultPlan {
    pub const fn none() -> Self {
        Self {
            fail_at: None,
            fail_stage_attempt: None,
        }
    }

    pub const fn fail_at(point: FaultPoint) -> Self {
        Self {
            fail_at: Some(point),
            fail_stage_attempt: None,
        }
    }

    pub const fn fail_stage_attempt(attempt: u32) -> Self {
        Self {
            fail_at: None,
            fail_stage_attempt: Some(attempt),
        }
    }

    pub fn check(&self, point: FaultPoint) -> Result<(), DurabilityError> {
        if self.fail_at == Some(point) {
            Err(DurabilityError { point })
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurabilityOptions {
    pub sync_file: bool,
    pub sync_parent: bool,
    pub metadata: MetadataPolicy,
    pub faults: FaultPlan,
}

impl Default for DurabilityOptions {
    fn default() -> Self {
        Self {
            sync_file: true,
            sync_parent: true,
            metadata: MetadataPolicy::PreserveSupportedMode,
            faults: FaultPlan::none(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataPolicy {
    PreserveSupportedMode,
    SafeAddModeOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataWarning {
    ExtendedAttributesNotPreserved,
    AclNotPreserved,
    WindowsAlternateStreamsNotPreserved,
    DirectoryDurabilityNotGuaranteed,
    /// A private staging name could not be removed. The requested destination
    /// outcome remains truthful, but operators must be able to observe and
    /// clean the residual file.
    TemporaryFileCleanupFailed,
}

impl MetadataWarning {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExtendedAttributesNotPreserved => "extended_attributes_not_preserved",
            Self::AclNotPreserved => "acl_not_preserved",
            Self::WindowsAlternateStreamsNotPreserved => "windows_alternate_streams_not_preserved",
            Self::DirectoryDurabilityNotGuaranteed => "directory_durability_not_guaranteed",
            Self::TemporaryFileCleanupFailed => "temporary_file_cleanup_failed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurabilityError {
    pub point: FaultPoint,
}

impl std::fmt::Display for DurabilityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "durability fault injected at {:?}", self.point)
    }
}

impl std::error::Error for DurabilityError {}

impl From<DurabilityError> for io::Error {
    fn from(error: DurabilityError) -> Self {
        io::Error::new(io::ErrorKind::Other, error)
    }
}

pub fn sync_parent_directory(path: &Path) -> Result<(), io::Error> {
    #[cfg(not(unix))]
    {
        // Directory fsync is not exposed portably by std on Windows and other
        // non-Unix targets.  The file flush/rename still runs; callers must
        // treat the platform's weaker directory-durability guarantee as a
        // metadata warning rather than turning every successful mutation into
        // a false commit-state uncertainty.
        let _ = path;
        return Ok(());
    }
    #[cfg(unix)]
    {
        let Some(parent) = path.parent() else {
            return Ok(());
        };
        let directory = fs::File::open(parent)?;
        directory.sync_all()
    }
}

/// Copies supported Unix mode bits. ACLs, xattrs and Windows alternate data
/// streams are intentionally not represented as exact text history.
pub fn preserve_supported_mode(
    source: &Path,
    destination: &Path,
) -> Result<Vec<MetadataWarning>, io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = fs::symlink_metadata(source)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "source mode target is not a regular file",
            ));
        }
        let mode = metadata.permissions().mode();
        fs::set_permissions(destination, fs::Permissions::from_mode(mode))?;
    }
    #[cfg(not(unix))]
    let _ = (source, destination);
    Ok(metadata_warnings())
}

/// Newly created temporary files remain private; this safe policy never
/// widens permissions beyond the process's secure creation mode.
pub fn apply_safe_add_mode(path: &Path) -> Result<Vec<MetadataWarning>, io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let current = fs::metadata(path)?.permissions().mode();
        fs::set_permissions(path, fs::Permissions::from_mode(current & 0o777))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(metadata_warnings())
}

pub(crate) fn apply_safe_add_mode_to_file(
    file: &fs::File,
) -> Result<Vec<MetadataWarning>, io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let current = file.metadata()?.permissions().mode();
        file.set_permissions(fs::Permissions::from_mode(current & 0o777))?;
    }
    #[cfg(not(unix))]
    let _ = file;
    Ok(metadata_warnings())
}

pub fn supported_mode(path: &Path) -> Result<Option<u32>, io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mode target is not a regular file",
            ));
        }
        return Ok(Some(metadata.permissions().mode()));
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(None)
    }
}

pub fn apply_supported_mode(path: &Path, mode: Option<u32>) -> Result<(), io::Error> {
    #[cfg(unix)]
    if let Some(mode) = mode {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    }
    #[cfg(not(unix))]
    let _ = (path, mode);
    Ok(())
}

pub(crate) fn apply_supported_mode_to_file(
    file: &fs::File,
    mode: Option<u32>,
) -> Result<(), io::Error> {
    #[cfg(unix)]
    if let Some(mode) = mode {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(mode))?;
    }
    #[cfg(not(unix))]
    let _ = (file, mode);
    Ok(())
}

fn metadata_warnings() -> Vec<MetadataWarning> {
    #[allow(unused_mut)]
    let mut warnings = vec![
        MetadataWarning::ExtendedAttributesNotPreserved,
        MetadataWarning::AclNotPreserved,
    ];
    #[cfg(windows)]
    warnings.push(MetadataWarning::WindowsAlternateStreamsNotPreserved);
    #[cfg(not(unix))]
    warnings.push(MetadataWarning::DirectoryDurabilityNotGuaranteed);
    warnings
}

/// Reports the platform boundary for directory-entry durability. Unix can
/// fsync the parent directory; Windows and other targets cannot express the
/// same guarantee through the portable standard-library API. Deletion has no
/// staged metadata pass, so it uses this focused warning directly.
pub fn directory_durability_warnings() -> Vec<MetadataWarning> {
    #[cfg(not(unix))]
    {
        vec![MetadataWarning::DirectoryDurabilityNotGuaranteed]
    }
    #[cfg(unix)]
    {
        Vec::new()
    }
}

pub fn unsupported_metadata_warnings() -> Vec<MetadataWarning> {
    metadata_warnings()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn fault_plan_is_deterministic() {
        let plan = FaultPlan::fail_at(FaultPoint::Rename);
        assert!(plan.check(FaultPoint::StageWrite).is_ok());
        assert_eq!(
            plan.check(FaultPoint::Rename).unwrap_err().point,
            FaultPoint::Rename
        );
    }

    #[test]
    fn parent_sync_and_metadata_helpers_are_callable() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        fs::write(&source, b"data").unwrap();
        fs::write(&destination, b"data").unwrap();
        sync_parent_directory(&destination).unwrap();
        let warnings = preserve_supported_mode(&source, &destination).unwrap();
        assert!(warnings.contains(&MetadataWarning::AclNotPreserved));
        let warnings = apply_safe_add_mode(&destination).unwrap();
        assert!(!warnings.is_empty());
    }
}
