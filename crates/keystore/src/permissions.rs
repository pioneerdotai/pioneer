use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::{KeystoreError, Result};

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

fn sqlite_sidecar_path(path: &Path, suffix: &str) -> Result<PathBuf> {
    let file_name = path.file_name().ok_or_else(|| {
        KeystoreError::PermissionFailed(format!(
            "keystore path {} has no file name",
            path.display()
        ))
    })?;
    Ok(path.with_file_name(format!("{}{}", file_name.to_string_lossy(), suffix)))
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
            ACL,
            Authorization::{
                ConvertStringSidToSidW, EXPLICIT_ACCESS_W, NO_MULTIPLE_TRUSTEE, SE_FILE_OBJECT,
                SET_ACCESS, SetEntriesInAclW, SetNamedSecurityInfoW, TRUSTEE_IS_ALIAS,
                TRUSTEE_IS_SID, TRUSTEE_IS_USER, TRUSTEE_IS_WELL_KNOWN_GROUP, TRUSTEE_W,
            },
            CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, GetTokenInformation, NO_INHERITANCE,
            OBJECT_INHERIT_ACE, PROTECTED_DACL_SECURITY_INFORMATION, PSID, TOKEN_QUERY, TOKEN_USER,
            TokenUser,
        },
        System::Threading::{GetCurrentProcess, OpenProcessToken},
    };

    use crate::{KeystoreError, Result};

    const SYSTEM_SID: &str = "S-1-5-18";
    const ADMINISTRATORS_SID: &str = "S-1-5-32-544";

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
    }

    #[test]
    fn missing_wal_and_shm_are_ignored() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("keystore.db");
        fs::File::create(&path).expect("create db");

        ensure_keystore_sqlite_files(&path).expect("harden sqlite files");
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
