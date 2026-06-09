use anyhow::Result;
use std::fs;
use std::path::Path;

#[cfg(unix)]
pub fn file_link_count(_path: &Path, metadata: &fs::Metadata) -> Result<u64> {
    use std::os::unix::fs::MetadataExt;

    Ok(metadata.nlink())
}

#[cfg(windows)]
pub fn file_link_count(path: &Path, _metadata: &fs::Metadata) -> Result<u64> {
    windows::file_link_count(path)
}

#[cfg(not(any(unix, windows)))]
pub fn file_link_count(_path: &Path, _metadata: &fs::Metadata) -> Result<u64> {
    Ok(1)
}

#[cfg(windows)]
mod windows {
    use anyhow::{Context, Result};
    use std::io;
    use std::mem::MaybeUninit;
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;
    use std::ptr;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, GetFileInformationByHandle, OPEN_EXISTING,
    };

    pub fn file_link_count(path: &Path) -> Result<u64> {
        let handle = FileHandle::open(path)?;
        let mut info = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
        // SAFETY: `handle` is a valid file handle owned by `FileHandle`, and `info`
        // points to writable storage for the OS to initialize.
        let result = unsafe { GetFileInformationByHandle(handle.raw(), info.as_mut_ptr()) };
        if result == 0 {
            return Err(io::Error::last_os_error()).with_context(|| {
                format!(
                    "failed to read file information for hardlink check `{}`",
                    path.display()
                )
            });
        }
        // SAFETY: `GetFileInformationByHandle` succeeded, so `info` was initialized.
        let info = unsafe { info.assume_init() };
        handle.close(path)?;
        Ok(u64::from(info.nNumberOfLinks))
    }

    struct FileHandle {
        handle: Option<HANDLE>,
    }

    impl FileHandle {
        fn open(path: &Path) -> Result<Self> {
            let wide_path = path
                .as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))
                .collect::<Vec<_>>();
            // SAFETY: `wide_path` is NUL-terminated and lives for the duration of
            // the call; null security/template pointers are accepted by CreateFileW.
            let handle = unsafe {
                CreateFileW(
                    wide_path.as_ptr(),
                    0,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                    ptr::null(),
                    OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL,
                    ptr::null_mut(),
                )
            };
            if handle == INVALID_HANDLE_VALUE {
                return Err(io::Error::last_os_error()).with_context(|| {
                    format!("failed to open `{}` for hardlink check", path.display())
                });
            }
            Ok(Self {
                handle: Some(handle),
            })
        }

        fn raw(&self) -> HANDLE {
            self.handle.expect("file handle must be open")
        }

        fn close(mut self, path: &Path) -> Result<()> {
            let Some(handle) = self.handle.take() else {
                return Ok(());
            };
            // SAFETY: this handle was returned by CreateFileW and has been removed
            // from `self`, so this is the only close path for this call.
            let result = unsafe { CloseHandle(handle) };
            if result == 0 {
                return Err(io::Error::last_os_error()).with_context(|| {
                    format!("failed to close `{}` after hardlink check", path.display())
                });
            }
            Ok(())
        }
    }

    impl Drop for FileHandle {
        fn drop(&mut self) {
            if let Some(handle) = self.handle.take() {
                // SAFETY: this is a best-effort cleanup for a handle still owned
                // by `FileHandle`; errors cannot be reported from Drop.
                let _ = unsafe { CloseHandle(handle) };
            }
        }
    }
}
