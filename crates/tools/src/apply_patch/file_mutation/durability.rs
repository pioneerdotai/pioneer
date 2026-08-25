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
    let warnings = metadata_warnings_for_source(source)?;
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
    Ok(warnings)
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
    // A newly created file has no prior ACLs, xattrs, or alternate streams to
    // preserve. Reporting their absence as data-loss warnings is misleading.
    Ok(Vec::new())
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
    Ok(Vec::new())
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

pub fn metadata_warnings_for_source(path: &Path) -> Result<Vec<MetadataWarning>, io::Error> {
    #[allow(unused_mut)]
    let mut warnings = Vec::new();
    let presence = unsupported_metadata_presence(path)?;
    if presence.extended_attributes {
        warnings.push(MetadataWarning::ExtendedAttributesNotPreserved);
    }
    if presence.acl {
        warnings.push(MetadataWarning::AclNotPreserved);
    }
    #[cfg(not(unix))]
    warnings.push(MetadataWarning::DirectoryDurabilityNotGuaranteed);
    Ok(warnings)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct UnsupportedMetadataPresence {
    extended_attributes: bool,
    acl: bool,
}

fn unsupported_metadata_presence(path: &Path) -> Result<UnsupportedMetadataPresence, io::Error> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "metadata path contains NUL")
        })?;
        let names = list_extended_attributes(path.as_c_str())?;
        let acl_xattr = names.iter().any(|name| is_acl_xattr(name));
        return Ok(UnsupportedMetadataPresence {
            extended_attributes: names
                .iter()
                .any(|name| is_preservation_relevant_extended_attribute(name)),
            acl: acl_xattr || has_platform_acl(path.as_c_str())?,
        });
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = path;
        Ok(UnsupportedMetadataPresence::default())
    }
}

fn is_acl_xattr(name: &[u8]) -> bool {
    name == b"system.posix_acl_access" || name == b"system.posix_acl_default"
}

fn is_preservation_relevant_extended_attribute(name: &[u8]) -> bool {
    if is_acl_xattr(name) {
        return false;
    }
    #[cfg(target_os = "macos")]
    if name == b"com.apple.provenance" {
        // macOS attaches this process provenance marker to newly created
        // files and recreates it after removal. It is system-managed rather
        // than source content that Apply Patch could meaningfully preserve.
        return false;
    }
    true
}

#[cfg(target_os = "linux")]
fn list_extended_attributes(path: &std::ffi::CStr) -> Result<Vec<Vec<u8>>, io::Error> {
    let size = unsafe { libc::listxattr(path.as_ptr(), std::ptr::null_mut(), 0) };
    if size < 0 {
        return Err(io::Error::last_os_error());
    }
    if size == 0 {
        return Ok(Vec::new());
    }
    let mut buffer = vec![0_u8; usize::try_from(size).unwrap_or(0)];
    let written =
        unsafe { libc::listxattr(path.as_ptr(), buffer.as_mut_ptr().cast(), buffer.len()) };
    if written < 0 {
        return Err(io::Error::last_os_error());
    }
    buffer.truncate(usize::try_from(written).unwrap_or(0));
    Ok(buffer
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
        .map(<[u8]>::to_vec)
        .collect())
}

#[cfg(target_os = "macos")]
fn list_extended_attributes(path: &std::ffi::CStr) -> Result<Vec<Vec<u8>>, io::Error> {
    let size = unsafe { libc::listxattr(path.as_ptr(), std::ptr::null_mut(), 0, 0) };
    if size < 0 {
        return Err(io::Error::last_os_error());
    }
    if size == 0 {
        return Ok(Vec::new());
    }
    let mut buffer = vec![0_u8; usize::try_from(size).unwrap_or(0)];
    let written =
        unsafe { libc::listxattr(path.as_ptr(), buffer.as_mut_ptr().cast(), buffer.len(), 0) };
    if written < 0 {
        return Err(io::Error::last_os_error());
    }
    buffer.truncate(usize::try_from(written).unwrap_or(0));
    Ok(buffer
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
        .map(<[u8]>::to_vec)
        .collect())
}

#[cfg(target_os = "linux")]
fn has_platform_acl(_path: &std::ffi::CStr) -> Result<bool, io::Error> {
    Ok(false)
}

#[cfg(target_os = "macos")]
fn has_platform_acl(path: &std::ffi::CStr) -> Result<bool, io::Error> {
    const ACL_TYPE_EXTENDED: libc::c_int = 0x0000_0100;
    const ACL_FIRST_ENTRY: libc::c_int = 0;
    unsafe extern "C" {
        fn acl_get_file(path: *const libc::c_char, acl_type: libc::c_int) -> *mut libc::c_void;
        fn acl_get_entry(
            acl: *mut libc::c_void,
            entry_id: libc::c_int,
            entry: *mut *mut libc::c_void,
        ) -> libc::c_int;
        fn acl_free(object: *mut libc::c_void) -> libc::c_int;
    }

    let acl = unsafe { acl_get_file(path.as_ptr(), ACL_TYPE_EXTENDED) };
    if acl.is_null() {
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ENOENT) => Ok(false),
            _ => Err(error),
        };
    }
    let mut entry = std::ptr::null_mut();
    let result = unsafe { acl_get_entry(acl, ACL_FIRST_ENTRY, &mut entry) };
    let free_result = unsafe { acl_free(acl) };
    if free_result != 0 {
        return Err(io::Error::last_os_error());
    }
    match result {
        1 => Ok(true),
        0 => Ok(false),
        _ => Err(io::Error::last_os_error()),
    }
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
    Vec::new()
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
        assert!(warnings.is_empty());
        let warnings = apply_safe_add_mode(&destination).unwrap();
        assert!(warnings.is_empty());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn metadata_warnings_are_emitted_only_for_metadata_that_exists() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        fs::write(&source, b"data").unwrap();
        let path = CString::new(source.as_os_str().as_bytes()).unwrap();
        let initial_warnings = metadata_warnings_for_source(&source).unwrap();
        assert!(initial_warnings.is_empty(), "{initial_warnings:?}");

        #[cfg(target_os = "linux")]
        let name = CString::new("user.pioneer_metadata_test").unwrap();
        #[cfg(target_os = "macos")]
        let name = CString::new("com.pioneer.metadata-test").unwrap();
        let value = b"present";
        #[cfg(target_os = "linux")]
        let result = unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
            )
        };
        #[cfg(target_os = "macos")]
        let result = unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
                0,
            )
        };
        assert_eq!(result, 0, "set test xattr: {}", io::Error::last_os_error());
        assert_eq!(
            metadata_warnings_for_source(&source).unwrap(),
            vec![MetadataWarning::ExtendedAttributesNotPreserved]
        );
    }
}
