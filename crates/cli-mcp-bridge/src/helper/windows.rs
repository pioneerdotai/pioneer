use super::HelperError;
use crate::bootstrap::MAX_BOOTSTRAP_BYTES;
use std::ffi::c_void;
use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::mem::{MaybeUninit, size_of};
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::AsRawHandle;
use std::path::{Component, Path, PathBuf};
use std::ptr;
use windows_sys::Win32::Foundation::{CloseHandle, GENERIC_READ, HANDLE, LocalFree};
use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};
use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, ACL_SIZE_INFORMATION, AclSizeInformation,
    DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation, GetSecurityDescriptorControl,
    GetTokenInformation, OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_PROTECTED,
    SECURITY_DESCRIPTOR_CONTROL, TOKEN_QUERY, TOKEN_USER, TokenUser,
};
use windows_sys::Win32::Storage::FileSystem::{
    BY_HANDLE_FILE_INFORMATION, DELETE, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT,
    FILE_DISPOSITION_INFO, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
    FILE_FLAG_SEQUENTIAL_SCAN, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    FileDispositionInfo, GetFileInformationByHandle, SetFileInformationByHandle,
};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

const ACCESS_ALLOWED_ACE_TYPE_VALUE: u8 = 0;
const INHERITED_ACE_FLAG: u8 = 0x10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    volume: u32,
    index: u64,
}

pub(super) struct OpenedBootstrap {
    file: Option<File>,
    _parent: File,
    path: PathBuf,
    identity: FileIdentity,
}

impl OpenedBootstrap {
    pub(super) fn open(path: &Path) -> Result<Self, HelperError> {
        let parent_path = path.parent().ok_or(HelperError::InvalidBootstrapPath)?;
        if !parent_path.is_absolute()
            || parent_path
                .components()
                .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
        {
            return Err(HelperError::InvalidBootstrapPath);
        }

        let parent = OpenOptions::new()
            .access_mode(GENERIC_READ)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(parent_path)?;
        let parent_info = file_information(&parent)?;
        if parent_info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY == 0
            || parent_info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
        {
            return Err(HelperError::InsecureBootstrap);
        }
        validate_owner_only_acl(&parent)?;

        // share_mode(0) is the one-use lock. DELETE access lets consume mark
        // this exact handle for deletion, avoiding path-based replacement.
        let file = OpenOptions::new()
            .access_mode(GENERIC_READ | DELETE)
            .share_mode(0)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_SEQUENTIAL_SCAN)
            .open(path)?;
        let info = file_information(&file)?;
        let size = (u64::from(info.nFileSizeHigh) << 32) | u64::from(info.nFileSizeLow);
        if info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT) != 0
            || info.nNumberOfLinks != 1
            || size > MAX_BOOTSTRAP_BYTES as u64
        {
            return Err(HelperError::InsecureBootstrap);
        }
        validate_owner_only_acl(&file)?;
        let identity = identity(&info);
        Ok(Self {
            file: Some(file),
            _parent: parent,
            path: path.to_path_buf(),
            identity,
        })
    }

    pub(super) fn read_bounded(&mut self) -> Result<Vec<u8>, HelperError> {
        let file = self.file.as_mut().ok_or(HelperError::InsecureBootstrap)?;
        let mut bytes = Vec::with_capacity(MAX_BOOTSTRAP_BYTES.min(4096));
        file.take((MAX_BOOTSTRAP_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_BOOTSTRAP_BYTES {
            return Err(HelperError::Bootstrap(
                crate::BootstrapDecodeError::TooLarge {
                    actual: bytes.len(),
                    max: MAX_BOOTSTRAP_BYTES,
                },
            ));
        }
        Ok(bytes)
    }

    pub(super) fn consume(mut self) -> Result<(), HelperError> {
        let file = self.file.as_ref().ok_or(HelperError::InsecureBootstrap)?;
        if identity(&file_information(file)?) != self.identity {
            return Err(HelperError::InsecureBootstrap);
        }
        let mut disposition = FILE_DISPOSITION_INFO { DeleteFile: true };
        // SAFETY: the handle has DELETE access and disposition points to a
        // correctly sized initialized structure.
        if unsafe {
            SetFileInformationByHandle(
                file.as_raw_handle() as HANDLE,
                FileDispositionInfo,
                (&mut disposition as *mut FILE_DISPOSITION_INFO).cast::<c_void>(),
                u32::try_from(size_of::<FILE_DISPOSITION_INFO>())
                    .map_err(|_| HelperError::InsecureBootstrap)?,
            )
        } == 0
        {
            return Err(io::Error::last_os_error().into());
        }
        self.file.take();
        match std::fs::symlink_metadata(&self.path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Ok(_) => Err(HelperError::InsecureBootstrap),
            Err(error) => Err(error.into()),
        }
    }
}

fn file_information(file: &File) -> Result<BY_HANDLE_FILE_INFORMATION, HelperError> {
    let mut info = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    // SAFETY: the file handle is live and info is writable.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle() as HANDLE, info.as_mut_ptr()) } == 0
    {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: Windows initialized info on success.
    Ok(unsafe { info.assume_init() })
}

fn identity(info: &BY_HANDLE_FILE_INFORMATION) -> FileIdentity {
    FileIdentity {
        volume: info.dwVolumeSerialNumber,
        index: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
    }
}

fn validate_owner_only_acl(file: &File) -> Result<(), HelperError> {
    let token = current_process_token()?;
    let mut required = 0_u32;
    unsafe { GetTokenInformation(token.0, TokenUser, ptr::null_mut(), 0, &mut required) };
    if required == 0 {
        return Err(HelperError::InsecureBootstrap);
    }
    let mut token_info = vec![0_u8; required as usize];
    // SAFETY: token_info has the exact size reported by Windows.
    if unsafe {
        GetTokenInformation(
            token.0,
            TokenUser,
            token_info.as_mut_ptr().cast(),
            required,
            &mut required,
        )
    } == 0
    {
        return Err(HelperError::InsecureBootstrap);
    }
    let current_user = unsafe { (*token_info.as_ptr().cast::<TOKEN_USER>()).User.Sid };

    let mut owner: PSID = ptr::null_mut();
    let mut dacl: *mut ACL = ptr::null_mut();
    let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
    // SAFETY: all outputs are writable; descriptor owns the returned owner and
    // DACL pointers until LocalFree below.
    let status = unsafe {
        GetSecurityInfo(
            file.as_raw_handle() as HANDLE,
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &mut owner,
            ptr::null_mut(),
            &mut dacl,
            ptr::null_mut(),
            &mut descriptor,
        )
    };
    if status != 0 || descriptor.is_null() {
        return Err(HelperError::InsecureBootstrap);
    }
    let result = validate_descriptor(descriptor, owner, dacl, current_user);
    // SAFETY: GetSecurityInfo allocated descriptor with LocalAlloc.
    unsafe { LocalFree(descriptor.cast()) };
    result
}

fn validate_descriptor(
    descriptor: PSECURITY_DESCRIPTOR,
    owner: PSID,
    dacl: *mut ACL,
    current_user: PSID,
) -> Result<(), HelperError> {
    if owner.is_null() || dacl.is_null() || unsafe { EqualSid(owner, current_user) } == 0 {
        return Err(HelperError::InsecureBootstrap);
    }
    let mut control: SECURITY_DESCRIPTOR_CONTROL = 0;
    let mut revision = 0_u32;
    // SAFETY: descriptor and outputs are valid for this function call.
    if unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) } == 0
        || control & SE_DACL_PROTECTED == 0
    {
        return Err(HelperError::InsecureBootstrap);
    }
    let mut acl_info = ACL_SIZE_INFORMATION::default();
    // SAFETY: dacl is part of descriptor and acl_info is writable.
    if unsafe {
        GetAclInformation(
            dacl,
            (&mut acl_info as *mut ACL_SIZE_INFORMATION).cast(),
            u32::try_from(size_of::<ACL_SIZE_INFORMATION>())
                .map_err(|_| HelperError::InsecureBootstrap)?,
            AclSizeInformation,
        )
    } == 0
        || acl_info.AceCount != 1
    {
        return Err(HelperError::InsecureBootstrap);
    }
    let mut raw_ace = ptr::null_mut();
    // SAFETY: the DACL reports exactly one ACE and raw_ace is writable.
    if unsafe { GetAce(dacl, 0, &mut raw_ace) } == 0 || raw_ace.is_null() {
        return Err(HelperError::InsecureBootstrap);
    }
    let header = unsafe { &*(raw_ace.cast::<ACE_HEADER>()) };
    if header.AceType != ACCESS_ALLOWED_ACE_TYPE_VALUE || header.AceFlags & INHERITED_ACE_FLAG != 0
    {
        return Err(HelperError::InsecureBootstrap);
    }
    let ace = unsafe { &*(raw_ace.cast::<ACCESS_ALLOWED_ACE>()) };
    let ace_sid = (&ace.SidStart as *const u32).cast_mut().cast::<c_void>();
    if ace.Mask == 0 || unsafe { EqualSid(ace_sid, current_user) } == 0 {
        return Err(HelperError::InsecureBootstrap);
    }
    Ok(())
}

struct OwnedHandle(HANDLE);

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: this wrapper exclusively owns the token handle.
            unsafe { CloseHandle(self.0) };
        }
    }
}

fn current_process_token() -> Result<OwnedHandle, HelperError> {
    let mut token = ptr::null_mut();
    // SAFETY: GetCurrentProcess returns a valid pseudo-handle and token is
    // writable.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(HelperError::InsecureBootstrap);
    }
    Ok(OwnedHandle(token))
}
