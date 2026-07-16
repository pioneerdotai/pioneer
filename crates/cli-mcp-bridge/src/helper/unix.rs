use super::HelperError;
use crate::bootstrap::MAX_BOOTSTRAP_BYTES;
use std::ffi::CString;
use std::fs::File;
use std::io::{self, Read};
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: libc::dev_t,
    inode: libc::ino_t,
    owner: libc::uid_t,
}

pub(super) struct OpenedBootstrap {
    file: Option<File>,
    parent: File,
    file_name: CString,
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
        let file_name = path.file_name().ok_or(HelperError::InvalidBootstrapPath)?;
        if file_name.as_bytes().contains(&b'/') {
            return Err(HelperError::InvalidBootstrapPath);
        }

        let parent_c = CString::new(parent_path.as_os_str().as_bytes())
            .map_err(|_| HelperError::InvalidBootstrapPath)?;
        // SAFETY: parent_c is NUL-terminated. O_NOFOLLOW rejects a substituted
        // final directory symlink and O_DIRECTORY rejects non-directories.
        let parent_fd = unsafe {
            libc::open(
                parent_c.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if parent_fd < 0 {
            return Err(io::Error::last_os_error().into());
        }
        // SAFETY: ownership of the newly opened descriptor is transferred.
        let parent = unsafe { File::from_raw_fd(parent_fd) };
        validate_parent(&parent)?;

        let file_name =
            CString::new(file_name.as_bytes()).map_err(|_| HelperError::InvalidBootstrapPath)?;
        // SAFETY: the directory fd is live and file_name contains one relative
        // component. O_NONBLOCK prevents a substituted FIFO from blocking.
        let file_fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                file_name.as_ptr(),
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
            )
        };
        if file_fd < 0 {
            return Err(io::Error::last_os_error().into());
        }
        // SAFETY: ownership of the newly opened descriptor is transferred.
        let file = unsafe { File::from_raw_fd(file_fd) };
        let identity = validate_file(&file)?;
        // Locking prevents a second helper from consuming the same live inode.
        // SAFETY: flock accepts a live file descriptor and no pointers.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(HelperError::InsecureBootstrap);
        }

        Ok(Self {
            file: Some(file),
            parent,
            file_name,
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
        let current = identity_at(self.parent.as_raw_fd(), &self.file_name)?;
        if current != self.identity {
            return Err(HelperError::InsecureBootstrap);
        }
        // SAFETY: unlinkat is anchored to the validated, still-open parent and
        // file_name has no path separators. It cannot traverse elsewhere.
        if unsafe { libc::unlinkat(self.parent.as_raw_fd(), self.file_name.as_ptr(), 0) } != 0 {
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

fn validate_parent(parent: &File) -> Result<(), HelperError> {
    let status = fstat(parent)?;
    if status.st_uid != current_uid()
        || status.st_mode & libc::S_IFMT != libc::S_IFDIR
        || status.st_mode & 0o777 != 0o700
    {
        return Err(HelperError::InsecureBootstrap);
    }
    Ok(())
}

fn validate_file(file: &File) -> Result<FileIdentity, HelperError> {
    let status = fstat(file)?;
    if status.st_uid != current_uid()
        || status.st_mode & libc::S_IFMT != libc::S_IFREG
        || status.st_mode & 0o777 != 0o600
        || status.st_nlink != 1
        || status.st_size < 0
        || status.st_size as usize > MAX_BOOTSTRAP_BYTES
    {
        return Err(HelperError::InsecureBootstrap);
    }
    Ok(identity(&status))
}

fn identity_at(parent_fd: i32, file_name: &CString) -> Result<FileIdentity, HelperError> {
    let mut status = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: status is writable and file_name is relative to a live directory
    // fd. AT_SYMLINK_NOFOLLOW rejects replacement links.
    if unsafe {
        libc::fstatat(
            parent_fd,
            file_name.as_ptr(),
            status.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: fstatat initialized status on success.
    let status = unsafe { status.assume_init() };
    if status.st_mode & libc::S_IFMT != libc::S_IFREG {
        return Err(HelperError::InsecureBootstrap);
    }
    Ok(identity(&status))
}

fn fstat(file: &File) -> Result<libc::stat, HelperError> {
    let mut status = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: status is writable and the descriptor is live.
    if unsafe { libc::fstat(file.as_raw_fd(), status.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: fstat initialized status on success.
    Ok(unsafe { status.assume_init() })
}

fn identity(status: &libc::stat) -> FileIdentity {
    FileIdentity {
        device: status.st_dev,
        inode: status.st_ino,
        owner: status.st_uid,
    }
}

fn current_uid() -> libc::uid_t {
    // SAFETY: getuid has no preconditions and cannot fail.
    unsafe { libc::getuid() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};

    #[test]
    fn helper_bootstrap_rejects_symlink_and_broad_permissions() {
        let directory = tempfile::tempdir().expect("directory");
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
            .expect("private directory");
        let bootstrap = directory.path().join("bootstrap");
        std::fs::write(&bootstrap, b"{}").expect("bootstrap");
        std::fs::set_permissions(&bootstrap, std::fs::Permissions::from_mode(0o644))
            .expect("broad file");
        assert!(matches!(
            OpenedBootstrap::open(&bootstrap),
            Err(HelperError::InsecureBootstrap)
        ));

        std::fs::set_permissions(&bootstrap, std::fs::Permissions::from_mode(0o600))
            .expect("private file");
        let link = directory.path().join("bootstrap-link");
        symlink(&bootstrap, &link).expect("symlink");
        assert!(OpenedBootstrap::open(&link).is_err());
    }

    #[test]
    fn helper_bootstrap_consume_is_one_use() {
        let directory = tempfile::tempdir().expect("directory");
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
            .expect("private directory");
        let bootstrap = directory.path().join("bootstrap");
        std::fs::write(&bootstrap, b"{}").expect("bootstrap");
        std::fs::set_permissions(&bootstrap, std::fs::Permissions::from_mode(0o600))
            .expect("private file");
        let opened = OpenedBootstrap::open(&bootstrap).expect("open");
        opened.consume().expect("consume");
        assert!(!bootstrap.exists());
        assert!(OpenedBootstrap::open(&bootstrap).is_err());
    }
}
