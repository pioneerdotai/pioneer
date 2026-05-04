use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::Serialize;

use crate::{KeystoreError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SecretPermissionHealthReport {
    pub path: PathBuf,
    pub target: String,
    pub status: SecretPermissionHealthStatus,
    pub expected: String,
    pub actual: Option<String>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretPermissionHealthStatus {
    Ok,
    Missing,
    MissingOptional,
    NotFile,
    NotDirectory,
    TooPermissive,
    Unknown,
    Error,
}

pub fn ensure_private_runtime_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path).map_err(|err| {
        KeystoreError::PermissionFailed(format!(
            "create runtime directory {}: {err}",
            path.display()
        ))
    })?;
    set_private_dir_permissions(path)
}

pub fn ensure_private_file(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    let metadata = fs::metadata(path).map_err(|err| {
        KeystoreError::PermissionFailed(format!("read metadata for {}: {err}", path.display()))
    })?;

    if !metadata.is_file() {
        return Err(KeystoreError::PermissionFailed(format!(
            "{} is not a file",
            path.display()
        )));
    }

    set_private_file_permissions(path)
}

pub fn ensure_keystore_sqlite_files(path: &Path) -> Result<()> {
    ensure_private_file(path)?;
    ensure_private_file(&sqlite_sidecar_path(path, "-wal")?)?;
    ensure_private_file(&sqlite_sidecar_path(path, "-shm")?)?;
    Ok(())
}

pub fn inspect_private_runtime_dir(path: &Path) -> SecretPermissionHealthReport {
    inspect_runtime_dir_target(path, "runtime_home")
}

pub fn inspect_private_file(path: &Path) -> SecretPermissionHealthReport {
    inspect_file_target(path, "keystore_db", "0600", true)
}

pub fn inspect_keystore_sqlite_files(path: &Path) -> Vec<SecretPermissionHealthReport> {
    vec![
        inspect_file_target(path, "keystore_db", "0600", true),
        match sqlite_sidecar_path(path, "-wal") {
            Ok(path) => inspect_file_target(path.as_path(), "keystore_wal", "0600", false),
            Err(error) => error_report(path, "keystore_wal", "0600", error.to_string()),
        },
        match sqlite_sidecar_path(path, "-shm") {
            Ok(path) => inspect_file_target(path.as_path(), "keystore_shm", "0600", false),
            Err(error) => error_report(path, "keystore_shm", "0600", error.to_string()),
        },
    ]
}

fn sqlite_sidecar_path(path: &Path, suffix: &str) -> Result<PathBuf> {
    let file_name = path.file_name().ok_or_else(|| {
        KeystoreError::PermissionFailed(format!(
            "keystore path {} has no file name",
            path.display()
        ))
    })?;
    Ok(path.with_file_name(format!("{}{}", file_name.to_string_lossy(), suffix)))
}

fn inspect_runtime_dir_target(path: &Path, target: &str) -> SecretPermissionHealthReport {
    let expected = "0700";
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return SecretPermissionHealthReport {
                path: path.to_path_buf(),
                target: target.to_owned(),
                status: SecretPermissionHealthStatus::Missing,
                expected: expected.to_owned(),
                actual: None,
                detail: None,
            };
        }
        Err(error) => {
            return error_report(
                path,
                target,
                expected,
                format!("read metadata failed: {error}"),
            );
        }
    };

    if !metadata.is_dir() {
        return SecretPermissionHealthReport {
            path: path.to_path_buf(),
            target: target.to_owned(),
            status: SecretPermissionHealthStatus::NotDirectory,
            expected: expected.to_owned(),
            actual: path_kind_actual(&metadata),
            detail: Some("path exists but is not a directory".to_owned()),
        };
    }

    inspect_platform_permissions(path, target, expected, &metadata)
}

fn inspect_file_target(
    path: &Path,
    target: &str,
    expected: &str,
    required: bool,
) -> SecretPermissionHealthReport {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return SecretPermissionHealthReport {
                path: path.to_path_buf(),
                target: target.to_owned(),
                status: if required {
                    SecretPermissionHealthStatus::Missing
                } else {
                    SecretPermissionHealthStatus::MissingOptional
                },
                expected: expected.to_owned(),
                actual: None,
                detail: None,
            };
        }
        Err(error) => {
            return error_report(
                path,
                target,
                expected,
                format!("read metadata failed: {error}"),
            );
        }
    };

    if !metadata.is_file() {
        return SecretPermissionHealthReport {
            path: path.to_path_buf(),
            target: target.to_owned(),
            status: SecretPermissionHealthStatus::NotFile,
            expected: expected.to_owned(),
            actual: path_kind_actual(&metadata),
            detail: Some("path exists but is not a file".to_owned()),
        };
    }

    inspect_platform_permissions(path, target, expected, &metadata)
}

fn error_report(
    path: &Path,
    target: &str,
    expected: &str,
    detail: String,
) -> SecretPermissionHealthReport {
    SecretPermissionHealthReport {
        path: path.to_path_buf(),
        target: target.to_owned(),
        status: SecretPermissionHealthStatus::Error,
        expected: expected.to_owned(),
        actual: None,
        detail: Some(detail),
    }
}

fn path_kind_actual(metadata: &fs::Metadata) -> Option<String> {
    if metadata.is_file() {
        Some("file".to_owned())
    } else if metadata.is_dir() {
        Some("directory".to_owned())
    } else {
        Some("other".to_owned())
    }
}

#[cfg(unix)]
fn inspect_platform_permissions(
    path: &Path,
    target: &str,
    expected: &str,
    metadata: &fs::Metadata,
) -> SecretPermissionHealthReport {
    use std::os::unix::fs::PermissionsExt;

    let mode = metadata.permissions().mode() & 0o777;
    let actual = format!("{mode:04o}");
    let status = if actual == expected {
        SecretPermissionHealthStatus::Ok
    } else if mode & 0o077 != 0 {
        SecretPermissionHealthStatus::TooPermissive
    } else {
        SecretPermissionHealthStatus::Unknown
    };
    let detail = match status {
        SecretPermissionHealthStatus::Ok => None,
        SecretPermissionHealthStatus::TooPermissive => {
            Some("group or other permission bits are set".to_owned())
        }
        _ => Some("mode differs from the expected private mode".to_owned()),
    };

    SecretPermissionHealthReport {
        path: path.to_path_buf(),
        target: target.to_owned(),
        status,
        expected: expected.to_owned(),
        actual: Some(actual),
        detail,
    }
}

#[cfg(windows)]
fn inspect_platform_permissions(
    path: &Path,
    target: &str,
    expected: &str,
    _metadata: &fs::Metadata,
) -> SecretPermissionHealthReport {
    match windows_acl::inspect_private_acl(path) {
        Ok(detail) => SecretPermissionHealthReport {
            path: path.to_path_buf(),
            target: target.to_owned(),
            status: SecretPermissionHealthStatus::Ok,
            expected: expected.to_owned(),
            actual: Some("private_dacl".to_owned()),
            detail: Some(detail),
        },
        Err(detail) => SecretPermissionHealthReport {
            path: path.to_path_buf(),
            target: target.to_owned(),
            status: SecretPermissionHealthStatus::Unknown,
            expected: expected.to_owned(),
            actual: None,
            detail: Some(detail),
        },
    }
}

#[cfg(all(not(unix), not(windows)))]
fn inspect_platform_permissions(
    path: &Path,
    target: &str,
    expected: &str,
    _metadata: &fs::Metadata,
) -> SecretPermissionHealthReport {
    SecretPermissionHealthReport {
        path: path.to_path_buf(),
        target: target.to_owned(),
        status: SecretPermissionHealthStatus::Unknown,
        expected: expected.to_owned(),
        actual: None,
        detail: Some(
            "private permission inspection is not implemented for this platform".to_owned(),
        ),
    }
}

#[cfg(unix)]
fn set_private_dir_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|err| {
        KeystoreError::PermissionFailed(format!(
            "set runtime directory mode 0700 on {}: {err}",
            path.display()
        ))
    })
}

#[cfg(windows)]
fn set_private_dir_permissions(path: &Path) -> Result<()> {
    windows_acl::set_private_acl(path, true)
}

#[cfg(all(not(unix), not(windows)))]
fn set_private_dir_permissions(path: &Path) -> Result<()> {
    let _ = fs::metadata(path).map_err(|err| {
        KeystoreError::PermissionFailed(format!("read metadata for {}: {err}", path.display()))
    })?;
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|err| {
        KeystoreError::PermissionFailed(format!("set file mode 0600 on {}: {err}", path.display()))
    })
}

#[cfg(windows)]
fn set_private_file_permissions(path: &Path) -> Result<()> {
    windows_acl::set_private_acl(path, false)
}

#[cfg(all(not(unix), not(windows)))]
fn set_private_file_permissions(path: &Path) -> Result<()> {
    let _ = fs::metadata(path).map_err(|err| {
        KeystoreError::PermissionFailed(format!("read metadata for {}: {err}", path.display()))
    })?;
    Ok(())
}

#[cfg(windows)]
mod windows_acl {
    use std::{ffi::c_void, mem, os::windows::ffi::OsStrExt, path::Path, ptr};

    use windows_sys::Win32::{
        Foundation::{
            CloseHandle, ERROR_INSUFFICIENT_BUFFER, ERROR_SUCCESS, GENERIC_ALL, GetLastError,
            HANDLE, HLOCAL, LocalFree,
        },
        Security::{
            ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, ACL_SIZE_INFORMATION, AclSizeInformation,
            Authorization::{
                ConvertStringSidToSidW, EXPLICIT_ACCESS_W, GetNamedSecurityInfoW,
                NO_MULTIPLE_TRUSTEE, SE_FILE_OBJECT, SET_ACCESS, SetEntriesInAclW,
                SetNamedSecurityInfoW, TRUSTEE_IS_ALIAS, TRUSTEE_IS_SID, TRUSTEE_IS_USER,
                TRUSTEE_IS_WELL_KNOWN_GROUP, TRUSTEE_W,
            },
            CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation,
            GetSecurityDescriptorControl, GetTokenInformation, NO_INHERITANCE, OBJECT_INHERIT_ACE,
            PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_PROTECTED,
            TOKEN_QUERY, TOKEN_USER, TokenUser,
        },
        System::Threading::{GetCurrentProcess, OpenProcessToken},
    };

    use crate::{KeystoreError, Result};

    const SYSTEM_SID: &str = "S-1-5-18";
    const ADMINISTRATORS_SID: &str = "S-1-5-32-544";
    const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;

    pub(super) fn inspect_private_acl(path: &Path) -> std::result::Result<String, String> {
        let path_wide = path_to_wide(path);
        let mut dacl = ptr::null_mut::<ACL>();
        let mut security_descriptor = ptr::null_mut::<c_void>();
        let status = unsafe {
            GetNamedSecurityInfoW(
                path_wide.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                ptr::null_mut(),
                ptr::null_mut(),
                &mut dacl,
                ptr::null_mut(),
                &mut security_descriptor,
            )
        };
        if status != ERROR_SUCCESS {
            return Err(format!("read DACL failed with Win32 error {status}"));
        }
        let _security_descriptor = LocalSecurityDescriptor(security_descriptor.cast());

        if dacl.is_null() {
            return Err("DACL is missing".to_owned());
        }

        let mut control = 0u16;
        let mut revision = 0u32;
        if unsafe {
            GetSecurityDescriptorControl(
                security_descriptor as PSECURITY_DESCRIPTOR,
                &mut control,
                &mut revision,
            )
        } == 0
        {
            return Err(format!(
                "read security descriptor control failed with Win32 error {}",
                unsafe { GetLastError() }
            ));
        }
        if control & SE_DACL_PROTECTED == 0 {
            return Err("DACL is not protected".to_owned());
        }

        let mut acl_info = ACL_SIZE_INFORMATION::default();
        if unsafe {
            GetAclInformation(
                dacl,
                (&mut acl_info as *mut ACL_SIZE_INFORMATION).cast(),
                mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
                AclSizeInformation,
            )
        } == 0
        {
            return Err(format!(
                "read ACL information failed with Win32 error {}",
                unsafe { GetLastError() }
            ));
        }

        let current_user = CurrentUserSid::load(path).map_err(|err| err.to_string())?;
        let system = LocalSid::from_string(SYSTEM_SID, path).map_err(|err| err.to_string())?;
        let administrators =
            LocalSid::from_string(ADMINISTRATORS_SID, path).map_err(|err| err.to_string())?;
        let expected_sids = [current_user.sid(), system.0, administrators.0];
        let mut seen_expected = [false; 3];

        for index in 0..acl_info.AceCount {
            let mut ace = ptr::null_mut::<c_void>();
            if unsafe { GetAce(dacl, index, &mut ace) } == 0 {
                return Err(format!(
                    "read ACE {index} failed with Win32 error {}",
                    unsafe { GetLastError() }
                ));
            }

            let header = unsafe { &*(ace as *const ACE_HEADER) };
            if header.AceType != ACCESS_ALLOWED_ACE_TYPE {
                return Err(format!(
                    "DACL contains unsupported ACE type {}",
                    header.AceType
                ));
            }

            let allowed = unsafe { &*(ace as *const ACCESS_ALLOWED_ACE) };
            let sid = (&allowed.SidStart as *const u32).cast::<c_void>() as PSID;
            let Some(position) = expected_sids
                .iter()
                .position(|expected| unsafe { EqualSid(sid, *expected) } != 0)
            else {
                return Err("DACL grants access to an unexpected SID".to_owned());
            };
            seen_expected[position] = true;
        }

        if seen_expected.iter().any(|seen| !*seen) {
            return Err("DACL is missing one or more expected private principals".to_owned());
        }

        Ok(format!(
            "protected DACL with {} allowed ACE(s)",
            acl_info.AceCount
        ))
    }

    pub(super) fn set_private_acl(path: &Path, directory: bool) -> Result<()> {
        let path_wide = path_to_wide(path);
        let current_user = CurrentUserSid::load(path)?;
        let system = LocalSid::from_string(SYSTEM_SID, path)?;
        let administrators = LocalSid::from_string(ADMINISTRATORS_SID, path)?;
        let inheritance = if directory {
            OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE
        } else {
            NO_INHERITANCE
        };

        let entries = [
            explicit_access(current_user.sid(), TRUSTEE_IS_USER, inheritance),
            explicit_access(system.0, TRUSTEE_IS_WELL_KNOWN_GROUP, inheritance),
            explicit_access(administrators.0, TRUSTEE_IS_ALIAS, inheritance),
        ];

        let mut acl = ptr::null_mut::<ACL>();
        let status = unsafe {
            SetEntriesInAclW(
                entries.len() as u32,
                entries.as_ptr(),
                ptr::null(),
                &mut acl,
            )
        };
        if status != ERROR_SUCCESS {
            return Err(windows_error(
                path,
                format!("build private DACL failed with Win32 error {status}"),
            ));
        }
        let acl = LocalAcl(acl);

        let status = unsafe {
            SetNamedSecurityInfoW(
                path_wide.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                ptr::null_mut(),
                ptr::null_mut(),
                acl.0,
                ptr::null(),
            )
        };
        if status != ERROR_SUCCESS {
            return Err(windows_error(
                path,
                format!("apply private DACL failed with Win32 error {status}"),
            ));
        }

        Ok(())
    }

    fn explicit_access(sid: PSID, trustee_type: i32, inheritance: u32) -> EXPLICIT_ACCESS_W {
        EXPLICIT_ACCESS_W {
            grfAccessPermissions: GENERIC_ALL,
            grfAccessMode: SET_ACCESS,
            grfInheritance: inheritance,
            Trustee: TRUSTEE_W {
                pMultipleTrustee: ptr::null_mut(),
                MultipleTrusteeOperation: NO_MULTIPLE_TRUSTEE,
                TrusteeForm: TRUSTEE_IS_SID,
                TrusteeType: trustee_type,
                ptstrName: sid.cast(),
            },
        }
    }

    fn path_to_wide(path: &Path) -> Vec<u16> {
        path.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect()
    }

    fn str_to_wide(value: &str) -> Vec<u16> {
        value.encode_utf16().chain(std::iter::once(0)).collect()
    }

    fn windows_error(path: &Path, message: String) -> KeystoreError {
        KeystoreError::PermissionFailed(format!("{}: {message}", path.display()))
    }

    struct CurrentUserSid {
        _buffer: Vec<usize>,
        sid: PSID,
    }

    impl CurrentUserSid {
        fn load(path: &Path) -> Result<Self> {
            let mut handle = ptr::null_mut::<c_void>();
            if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut handle) } == 0 {
                return Err(windows_error(
                    path,
                    format!("open process token failed with Win32 error {}", unsafe {
                        GetLastError()
                    }),
                ));
            }
            let token = TokenHandle(handle);

            let mut required_len = 0u32;
            let ok = unsafe {
                GetTokenInformation(token.0, TokenUser, ptr::null_mut(), 0, &mut required_len)
            };
            if ok != 0 || unsafe { GetLastError() } != ERROR_INSUFFICIENT_BUFFER {
                return Err(windows_error(
                    path,
                    format!("query token user size failed with Win32 error {}", unsafe {
                        GetLastError()
                    }),
                ));
            }

            let word_len = (required_len as usize).div_ceil(mem::size_of::<usize>());
            let mut buffer = vec![0usize; word_len];
            if unsafe {
                GetTokenInformation(
                    token.0,
                    TokenUser,
                    buffer.as_mut_ptr().cast(),
                    required_len,
                    &mut required_len,
                )
            } == 0
            {
                return Err(windows_error(
                    path,
                    format!("query token user failed with Win32 error {}", unsafe {
                        GetLastError()
                    }),
                ));
            }

            let token_user = unsafe { &*(buffer.as_ptr().cast::<TOKEN_USER>()) };
            let sid = token_user.User.Sid;
            if sid.is_null() {
                return Err(windows_error(path, "current user SID is null".to_owned()));
            }

            Ok(Self {
                _buffer: buffer,
                sid,
            })
        }

        fn sid(&self) -> PSID {
            self.sid
        }
    }

    struct TokenHandle(HANDLE);

    impl Drop for TokenHandle {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    CloseHandle(self.0);
                }
            }
        }
    }

    struct LocalSid(PSID);

    impl LocalSid {
        fn from_string(value: &str, path: &Path) -> Result<Self> {
            let wide = str_to_wide(value);
            let mut sid = ptr::null_mut::<c_void>();
            if unsafe { ConvertStringSidToSidW(wide.as_ptr(), &mut sid) } == 0 {
                return Err(windows_error(
                    path,
                    format!(
                        "convert well-known SID {value} failed with Win32 error {}",
                        unsafe { GetLastError() }
                    ),
                ));
            }
            Ok(Self(sid))
        }
    }

    impl Drop for LocalSid {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    LocalFree(self.0 as HLOCAL);
                }
            }
        }
    }

    struct LocalAcl(*mut ACL);

    impl Drop for LocalAcl {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    LocalFree(self.0 as HLOCAL);
                }
            }
        }
    }

    struct LocalSecurityDescriptor(PSECURITY_DESCRIPTOR);

    impl Drop for LocalSecurityDescriptor {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    LocalFree(self.0 as HLOCAL);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[cfg(unix)]
    fn mode(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;

        fs::metadata(path).expect("metadata").permissions().mode() & 0o777
    }

    #[cfg(unix)]
    #[test]
    fn unix_runtime_dir_mode_is_private() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755)).expect("loosen tempdir");

        ensure_private_runtime_dir(dir.path()).expect("harden dir");

        assert_eq!(mode(dir.path()), 0o700);
        let report = inspect_private_runtime_dir(dir.path());
        assert_eq!(report.status, SecretPermissionHealthStatus::Ok);
        assert_eq!(report.actual.as_deref(), Some("0700"));
    }

    #[cfg(unix)]
    #[test]
    fn unix_file_mode_is_private() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("keystore.db");
        let mut file = fs::File::create(&path).expect("create file");
        file.write_all(b"db").expect("write file");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("loosen file");

        ensure_private_file(&path).expect("harden file");

        assert_eq!(mode(&path), 0o600);
        let report = inspect_private_file(&path);
        assert_eq!(report.status, SecretPermissionHealthStatus::Ok);
        assert_eq!(report.actual.as_deref(), Some("0600"));
    }

    #[cfg(unix)]
    #[test]
    fn unix_inspection_reports_too_permissive_modes() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755)).expect("loosen dir");
        let path = dir.path().join("keystore.db");
        fs::File::create(&path).expect("create file");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("loosen file");

        let dir_report = inspect_private_runtime_dir(dir.path());
        let file_report = inspect_private_file(&path);

        assert_eq!(
            dir_report.status,
            SecretPermissionHealthStatus::TooPermissive
        );
        assert_eq!(
            file_report.status,
            SecretPermissionHealthStatus::TooPermissive
        );
    }

    #[test]
    fn missing_wal_and_shm_are_ignored() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("keystore.db");
        fs::File::create(&path).expect("create db");

        ensure_keystore_sqlite_files(&path).expect("harden sqlite files");
        let reports = inspect_keystore_sqlite_files(&path);
        assert_eq!(reports.len(), 3);
        assert_eq!(
            reports[1].status,
            SecretPermissionHealthStatus::MissingOptional
        );
        assert_eq!(
            reports[2].status,
            SecretPermissionHealthStatus::MissingOptional
        );
    }

    #[test]
    fn inspection_reports_missing_required_keystore_db() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("keystore.db");

        let report = inspect_private_file(&path);

        assert_eq!(report.status, SecretPermissionHealthStatus::Missing);
    }

    #[test]
    fn inspection_reports_wrong_path_kinds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dir_as_file = dir.path().join("keystore.db");
        fs::create_dir(&dir_as_file).expect("create directory at file path");
        let file_as_dir = dir.path().join("runtime_home");
        fs::File::create(&file_as_dir).expect("create file at dir path");

        assert_eq!(
            inspect_private_file(&dir_as_file).status,
            SecretPermissionHealthStatus::NotFile
        );
        assert_eq!(
            inspect_private_runtime_dir(&file_as_dir).status,
            SecretPermissionHealthStatus::NotDirectory
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_permission_helpers_apply_acl() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("keystore.db");
        fs::File::create(&path).expect("create db");

        ensure_private_runtime_dir(dir.path()).expect("dir helper");
        ensure_private_file(&path).expect("file helper");
        ensure_keystore_sqlite_files(&path).expect("sqlite helper");
    }
}
