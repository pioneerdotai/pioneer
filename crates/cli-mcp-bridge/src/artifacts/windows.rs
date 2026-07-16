use super::{PrivateArtifactError, encode_bootstrap};
use crate::BootstrapDocument;
use std::ffi::{OsStr, c_void};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::mem::{MaybeUninit, size_of};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle};
use std::path::{Path, PathBuf};
use std::ptr;
use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_ALREADY_EXISTS, ERROR_FILE_EXISTS, ERROR_SHARING_VIOLATION, GENERIC_READ,
    GENERIC_WRITE, HANDLE, LocalFree,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, GetSecurityInfo,
    SDDL_REVISION_1, SE_FILE_OBJECT,
};
use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, ACL_SIZE_INFORMATION, AclSizeInformation,
    DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation, GetSecurityDescriptorControl,
    GetTokenInformation, OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_PROTECTED,
    SECURITY_ATTRIBUTES, SECURITY_DESCRIPTOR_CONTROL, TOKEN_QUERY, TOKEN_USER, TokenUser,
};
use windows_sys::Win32::Storage::FileSystem::{
    BY_HANDLE_FILE_INFORMATION, CREATE_NEW, CreateDirectoryW, CreateFileW, DELETE,
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT,
    FILE_DISPOSITION_INFO, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
    FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FileDispositionInfo,
    GetFileInformationByHandle, SetFileInformationByHandle,
};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};
use zeroize::Zeroize;

const ACCESS_ALLOWED_ACE_TYPE_VALUE: u8 = 0;
const INHERITED_ACE_FLAG: u8 = 0x10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Identity {
    volume: u32,
    index: u64,
}

#[derive(Debug)]
pub struct PrivateSessionDirectory {
    path: PathBuf,
    identity: Identity,
    cleanup_armed: bool,
}

#[derive(Debug)]
pub struct PrivateBootstrapArtifact {
    path: PathBuf,
    identity: Identity,
    cleanup_armed: bool,
}

pub(super) fn create(
    root: &Path,
    directory_name: &str,
) -> Result<PrivateSessionDirectory, PrivateArtifactError> {
    let parent = root.parent().ok_or(PrivateArtifactError::InvalidRoot)?;
    fs::create_dir_all(parent)?;
    create_or_validate_owner_directory(root, true)?;
    let path = root.join(directory_name);
    match create_owner_directory(&path) {
        Ok(()) => {}
        Err(PrivateArtifactError::AlreadyExists) => {
            return Err(PrivateArtifactError::AlreadyExists);
        }
        Err(error) => return Err(error),
    }
    let file = open_directory(&path, GENERIC_READ | DELETE)?;
    validate_owner_only_acl(&file)?;
    let info = file_information(&file)?;
    if info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY == 0
        || info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
    {
        return Err(PrivateArtifactError::InsecureRoot);
    }
    Ok(PrivateSessionDirectory {
        path,
        identity: identity(&info),
        cleanup_armed: true,
    })
}

impl PrivateSessionDirectory {
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn write_bootstrap(
        &self,
        document: &BootstrapDocument,
    ) -> Result<PrivateBootstrapArtifact, PrivateArtifactError> {
        validate_directory_identity(&self.path, self.identity)?;
        let path = self.path.join("bootstrap.json");
        let mut encoded = encode_bootstrap(document)?;
        let result = create_owner_file(&path, encoded.as_slice());
        encoded.zeroize();
        result?;
        let file = open_regular(&path, GENERIC_READ | DELETE)?;
        validate_owner_only_acl(&file)?;
        let info = file_information(&file)?;
        if info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT) != 0
            || info.nNumberOfLinks != 1
        {
            return Err(PrivateArtifactError::Replaced);
        }
        Ok(PrivateBootstrapArtifact {
            path,
            identity: identity(&info),
            cleanup_armed: true,
        })
    }

    pub fn cleanup(&mut self) -> Result<(), PrivateArtifactError> {
        if !self.cleanup_armed {
            return Ok(());
        }
        let file = open_directory(&self.path, GENERIC_READ | DELETE)?;
        validate_owner_only_acl(&file)?;
        if identity(&file_information(&file)?) != self.identity {
            return Err(PrivateArtifactError::Replaced);
        }
        delete_exact_handle(&file)?;
        drop(file);
        self.cleanup_armed = false;
        Ok(())
    }
}

impl Drop for PrivateSessionDirectory {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

impl PrivateBootstrapArtifact {
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn is_consumed(&self) -> Result<bool, PrivateArtifactError> {
        let file = match open_regular(&self.path, GENERIC_READ) {
            Ok(file) => file,
            Err(PrivateArtifactError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(true);
            }
            Err(PrivateArtifactError::Io(error))
                if error.raw_os_error() == Some(ERROR_SHARING_VIOLATION as i32) =>
            {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        validate_owner_only_acl(&file)?;
        if identity(&file_information(&file)?) != self.identity {
            return Err(PrivateArtifactError::Replaced);
        }
        Ok(false)
    }

    pub fn cleanup(&mut self) -> Result<(), PrivateArtifactError> {
        if !self.cleanup_armed {
            return Ok(());
        }
        let file = match open_regular(&self.path, GENERIC_READ | DELETE) {
            Ok(file) => file,
            Err(PrivateArtifactError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                self.cleanup_armed = false;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        validate_owner_only_acl(&file)?;
        if identity(&file_information(&file)?) != self.identity {
            return Err(PrivateArtifactError::Replaced);
        }
        delete_exact_handle(&file)?;
        drop(file);
        self.cleanup_armed = false;
        Ok(())
    }
}

impl Drop for PrivateBootstrapArtifact {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

fn create_or_validate_owner_directory(
    path: &Path,
    allow_existing: bool,
) -> Result<(), PrivateArtifactError> {
    match create_owner_directory(path) {
        Ok(()) => Ok(()),
        Err(PrivateArtifactError::AlreadyExists) if allow_existing => {
            let file = open_directory(path, GENERIC_READ)?;
            validate_owner_only_acl(&file)?;
            let info = file_information(&file)?;
            if info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY == 0
                || info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
            {
                return Err(PrivateArtifactError::InsecureRoot);
            }
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn create_owner_directory(path: &Path) -> Result<(), PrivateArtifactError> {
    let security = OwnedSecurityDescriptor::current_user_owner_only()?;
    let mut attributes = security.attributes()?;
    let path = wide(path);
    // SAFETY: path is NUL-terminated and attributes references a live descriptor.
    if unsafe { CreateDirectoryW(path.as_ptr(), &mut attributes) } == 0 {
        let error = io::Error::last_os_error();
        if matches!(error.raw_os_error(), Some(code) if code == ERROR_ALREADY_EXISTS as i32 || code == ERROR_FILE_EXISTS as i32)
        {
            return Err(PrivateArtifactError::AlreadyExists);
        }
        return Err(error.into());
    }
    Ok(())
}

fn create_owner_file(path: &Path, bytes: &[u8]) -> Result<(), PrivateArtifactError> {
    let security = OwnedSecurityDescriptor::current_user_owner_only()?;
    let mut attributes = security.attributes()?;
    let path_wide = wide(path);
    // SAFETY: path is NUL-terminated, attributes is live, and CREATE_NEW avoids replacement.
    let handle = unsafe {
        CreateFileW(
            path_wide.as_ptr(),
            GENERIC_READ | GENERIC_WRITE | DELETE,
            0,
            &mut attributes,
            CREATE_NEW,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
            ptr::null_mut(),
        )
    };
    if handle.is_null() || handle as isize == -1 {
        let error = io::Error::last_os_error();
        if matches!(error.raw_os_error(), Some(code) if code == ERROR_ALREADY_EXISTS as i32 || code == ERROR_FILE_EXISTS as i32)
        {
            return Err(PrivateArtifactError::AlreadyExists);
        }
        return Err(error.into());
    }
    // SAFETY: CreateFileW returned an exclusively owned live handle.
    let mut file = unsafe { File::from_raw_handle(handle.cast()) };
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn open_directory(path: &Path, access: u32) -> Result<File, PrivateArtifactError> {
    OpenOptions::new()
        .access_mode(access)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
        .map_err(Into::into)
}

fn open_regular(path: &Path, access: u32) -> Result<File, PrivateArtifactError> {
    OpenOptions::new()
        .access_mode(access)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
        .map_err(Into::into)
}

fn validate_directory_identity(
    path: &Path,
    expected: Identity,
) -> Result<(), PrivateArtifactError> {
    let file = open_directory(path, GENERIC_READ)?;
    validate_owner_only_acl(&file)?;
    let info = file_information(&file)?;
    if info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY == 0
        || info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
        || identity(&info) != expected
    {
        return Err(PrivateArtifactError::Replaced);
    }
    Ok(())
}

fn delete_exact_handle(file: &File) -> Result<(), PrivateArtifactError> {
    let mut disposition = FILE_DISPOSITION_INFO { DeleteFile: true };
    // SAFETY: file has DELETE access and disposition has the declared size.
    if unsafe {
        SetFileInformationByHandle(
            file.as_raw_handle() as HANDLE,
            FileDispositionInfo,
            (&mut disposition as *mut FILE_DISPOSITION_INFO).cast::<c_void>(),
            u32::try_from(size_of::<FILE_DISPOSITION_INFO>())
                .map_err(|_| PrivateArtifactError::Replaced)?,
        )
    } == 0
    {
        return Err(io::Error::last_os_error().into());
    }
    Ok(())
}

fn file_information(file: &File) -> Result<BY_HANDLE_FILE_INFORMATION, PrivateArtifactError> {
    let mut info = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    // SAFETY: file is live and info is writable.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle() as HANDLE, info.as_mut_ptr()) } == 0
    {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: Windows initialized info on success.
    Ok(unsafe { info.assume_init() })
}

fn identity(info: &BY_HANDLE_FILE_INFORMATION) -> Identity {
    Identity {
        volume: info.dwVolumeSerialNumber,
        index: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
    }
}

fn validate_owner_only_acl(file: &File) -> Result<(), PrivateArtifactError> {
    let current_user = current_user_sid()?;
    let mut owner: PSID = ptr::null_mut();
    let mut dacl: *mut ACL = ptr::null_mut();
    let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
    // SAFETY: outputs are writable and descriptor owns returned ACL/SID pointers.
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
    if status != 0 || descriptor.is_null() || owner.is_null() || dacl.is_null() {
        return Err(PrivateArtifactError::InsecureRoot);
    }
    let result = validate_descriptor(descriptor, owner, dacl, current_user.sid);
    // SAFETY: GetSecurityInfo allocated descriptor with LocalAlloc.
    unsafe { LocalFree(descriptor.cast()) };
    result
}

fn validate_descriptor(
    descriptor: PSECURITY_DESCRIPTOR,
    owner: PSID,
    dacl: *mut ACL,
    current_user: PSID,
) -> Result<(), PrivateArtifactError> {
    if unsafe { EqualSid(owner, current_user) } == 0 {
        return Err(PrivateArtifactError::InsecureRoot);
    }
    let mut control: SECURITY_DESCRIPTOR_CONTROL = 0;
    let mut revision = 0_u32;
    if unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) } == 0
        || control & SE_DACL_PROTECTED == 0
    {
        return Err(PrivateArtifactError::InsecureRoot);
    }
    let mut info = ACL_SIZE_INFORMATION::default();
    if unsafe {
        GetAclInformation(
            dacl,
            (&mut info as *mut ACL_SIZE_INFORMATION).cast(),
            u32::try_from(size_of::<ACL_SIZE_INFORMATION>())
                .map_err(|_| PrivateArtifactError::InsecureRoot)?,
            AclSizeInformation,
        )
    } == 0
        || info.AceCount != 1
    {
        return Err(PrivateArtifactError::InsecureRoot);
    }
    let mut raw_ace = ptr::null_mut();
    if unsafe { GetAce(dacl, 0, &mut raw_ace) } == 0 || raw_ace.is_null() {
        return Err(PrivateArtifactError::InsecureRoot);
    }
    let header = unsafe { &*raw_ace.cast::<ACE_HEADER>() };
    if header.AceType != ACCESS_ALLOWED_ACE_TYPE_VALUE || header.AceFlags & INHERITED_ACE_FLAG != 0
    {
        return Err(PrivateArtifactError::InsecureRoot);
    }
    let ace = unsafe { &*raw_ace.cast::<ACCESS_ALLOWED_ACE>() };
    let ace_sid = (&ace.SidStart as *const u32).cast_mut().cast::<c_void>();
    if ace.Mask == 0 || unsafe { EqualSid(ace_sid, current_user) } == 0 {
        return Err(PrivateArtifactError::InsecureRoot);
    }
    Ok(())
}

struct CurrentUserSid {
    _token: OwnedHandle,
    _bytes: Vec<u8>,
    sid: PSID,
}

fn current_user_sid() -> Result<CurrentUserSid, PrivateArtifactError> {
    let mut token = ptr::null_mut();
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error().into());
    }
    let token = OwnedHandle(token);
    let mut required = 0_u32;
    unsafe { GetTokenInformation(token.0, TokenUser, ptr::null_mut(), 0, &mut required) };
    if required == 0 {
        return Err(PrivateArtifactError::InsecureRoot);
    }
    let mut bytes = vec![0_u8; required as usize];
    if unsafe {
        GetTokenInformation(
            token.0,
            TokenUser,
            bytes.as_mut_ptr().cast(),
            required,
            &mut required,
        )
    } == 0
    {
        return Err(io::Error::last_os_error().into());
    }
    let sid = unsafe { (*bytes.as_ptr().cast::<TOKEN_USER>()).User.Sid };
    Ok(CurrentUserSid {
        _token: token,
        _bytes: bytes,
        sid,
    })
}

struct OwnedSecurityDescriptor(PSECURITY_DESCRIPTOR);

impl OwnedSecurityDescriptor {
    fn current_user_owner_only() -> Result<Self, PrivateArtifactError> {
        let current = current_user_sid()?;
        let mut sid_string = ptr::null_mut();
        if unsafe { ConvertSidToStringSidW(current.sid, &mut sid_string) } == 0
            || sid_string.is_null()
        {
            return Err(io::Error::last_os_error().into());
        }
        let sid = unsafe {
            let mut length = 0;
            while *sid_string.add(length) != 0 {
                length += 1;
            }
            String::from_utf16(&std::slice::from_raw_parts(sid_string, length))
        }
        .map_err(|_| PrivateArtifactError::InsecureRoot)?;
        unsafe { LocalFree(sid_string.cast()) };
        let sddl = format!("O:{sid}D:P(A;;FA;;;{sid})");
        let wide = OsStr::new(sddl.as_str())
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let mut descriptor = ptr::null_mut();
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                ptr::null_mut(),
            )
        } == 0
            || descriptor.is_null()
        {
            return Err(io::Error::last_os_error().into());
        }
        Ok(Self(descriptor))
    }

    fn attributes(&self) -> Result<SECURITY_ATTRIBUTES, PrivateArtifactError> {
        Ok(SECURITY_ATTRIBUTES {
            nLength: u32::try_from(size_of::<SECURITY_ATTRIBUTES>())
                .map_err(|_| PrivateArtifactError::InsecureRoot)?,
            lpSecurityDescriptor: self.0.cast(),
            bInheritHandle: 0,
        })
    }
}

impl Drop for OwnedSecurityDescriptor {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { LocalFree(self.0.cast()) };
        }
    }
}

struct OwnedHandle(HANDLE);

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { CloseHandle(self.0) };
        }
    }
}

fn wide(path: &Path) -> Vec<u16> {
    path.as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}
