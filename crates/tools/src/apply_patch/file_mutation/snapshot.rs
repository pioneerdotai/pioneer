use crate::apply_patch::file_mutation::{FileContentVersion, FileVersionToken};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::fs::File;
#[cfg(not(unix))]
use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::path::Path;
use tempfile::{NamedTempFile, TempPath};

const COPY_BUFFER_BYTES: usize = 64 * 1024;

/// Open a workspace file without following symlink path components.
///
/// Target resolution and the post-read token check protect the path identity,
/// but they cannot prevent a path component from being swapped between those
/// checks.  This helper pins parent directories and rejects unsafe final
/// components where the platform exposes the required primitive, instead of
/// briefly consuming bytes from outside the workspace.
pub fn open_regular_file(path: impl AsRef<Path>) -> io::Result<File> {
    let path = trusted_platform_path(path.as_ref());
    #[cfg(unix)]
    let file = open_regular_file_unix(&path)?;
    #[cfg(windows)]
    let file = open_regular_file_windows(&path)?;
    #[cfg(not(any(unix, windows)))]
    let file = open_regular_file_portable(&path)?;

    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "workspace target is not a regular file",
        ));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes()
            & windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT
            != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "workspace target is a reparse point",
            ));
        }
    }
    Ok(file)
}

/// Open and pin a directory without following any application-controlled
/// symlink/reparse-point component. Callers can enumerate the returned handle
/// without re-opening a previously authorized pathname.
pub fn open_directory(path: impl AsRef<Path>) -> io::Result<File> {
    let path = trusted_platform_path(path.as_ref());
    #[cfg(unix)]
    let directory = open_directory_unix(&path)?;
    #[cfg(windows)]
    let directory = open_directory_windows(&path)?;
    #[cfg(not(any(unix, windows)))]
    let directory = File::open(&path)?;

    let metadata = directory.metadata()?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "workspace target is not a directory",
        ));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes()
            & windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT
            != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "workspace directory is a reparse point",
            ));
        }
    }
    Ok(directory)
}

/// Open a regular file relative to an already-authorized directory handle.
///
/// The relative path is deliberately interpreted only by descriptor-relative
/// operations on Unix.  This is the use-side half of a file-policy grant: a
/// pathname may be replaced after authorization, but the opened object and
/// every parent component remain anchored to the handle captured at check
/// time.
pub(crate) fn open_regular_file_at(parent: &File, relative: impl AsRef<Path>) -> io::Result<File> {
    let relative = relative.as_ref();
    #[cfg(unix)]
    let file = open_regular_file_at_unix(parent, relative)?;
    #[cfg(windows)]
    let file = open_regular_file_at_windows(parent, relative)?;
    #[cfg(not(any(unix, windows)))]
    let file = return Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "descriptor-relative file access is unavailable on this platform",
    ));

    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "workspace target is not a regular file",
        ));
    }
    Ok(file)
}

/// Open a directory relative to an already-authorized directory handle.
pub(crate) fn open_directory_at(parent: &File, relative: impl AsRef<Path>) -> io::Result<File> {
    let relative = relative.as_ref();
    #[cfg(unix)]
    let directory = open_directory_at_unix(parent, relative)?;
    #[cfg(windows)]
    let directory = open_directory_at_windows(parent, relative)?;
    #[cfg(not(any(unix, windows)))]
    let directory = return Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "descriptor-relative directory access is unavailable on this platform",
    ));

    let metadata = directory.metadata()?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "workspace target is not a directory",
        ));
    }
    Ok(directory)
}

/// Resolve only immutable macOS compatibility aliases that precede the
/// application-owned path.  Never canonicalize the workspace descendants:
/// doing so would follow an attacker-controlled symlink before `openat` gets
/// a chance to reject it with `O_NOFOLLOW`.
pub(crate) fn trusted_platform_path(path: &Path) -> std::borrow::Cow<'_, Path> {
    #[cfg(target_os = "macos")]
    {
        for prefix in [Path::new("/var"), Path::new("/tmp"), Path::new("/etc")] {
            if path == prefix || path.starts_with(prefix) {
                let Ok(resolved) = std::fs::canonicalize(prefix) else {
                    continue;
                };
                let suffix = path.strip_prefix(prefix).unwrap_or_else(|_| Path::new(""));
                return std::borrow::Cow::Owned(resolved.join(suffix));
            }
        }
    }
    std::borrow::Cow::Borrowed(path)
}

#[cfg(unix)]
fn open_regular_file_unix(path: &Path) -> io::Result<File> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::path::Component;

    let mut directory = if path.is_absolute() {
        File::open("/")?
    } else {
        File::open(".")?
    };
    let components = path.components().collect::<Vec<_>>();
    if components
        .iter()
        .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "workspace target must be normalized before secure open",
        ));
    }
    let mut normal_components = components.iter().filter_map(|component| match component {
        Component::Normal(value) => Some(*value),
        Component::CurDir | Component::RootDir => None,
        Component::ParentDir | Component::Prefix(_) => None,
    });
    let Some(final_component) = normal_components.next_back() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "workspace target has no file component",
        ));
    };

    for component in normal_components {
        let name = CString::new(component.as_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "workspace target contains NUL")
        })?;
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // The newly opened descriptor pins this directory even if an
        // external process swaps the pathname for a symlink immediately
        // afterwards.  The previous descriptor is closed on reassignment.
        directory = unsafe { File::from_raw_fd(fd) };
    }

    let name = CString::new(final_component.as_bytes()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "workspace target contains NUL")
    })?;
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            // A final FIFO opened read-only blocks until a writer appears.
            // Open non-blocking until `fstat` proves this is a regular file;
            // O_NONBLOCK has no data-path effect for ordinary regular files.
            libc::O_RDONLY | libc::O_NONBLOCK | libc::O_NOCTTY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(unix)]
fn open_directory_unix(path: &Path) -> io::Result<File> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::path::Component;

    let mut directory = if path.is_absolute() {
        File::open("/")?
    } else {
        File::open(".")?
    };
    for component in path.components() {
        let name = match component {
            Component::Normal(value) => CString::new(value.as_bytes()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "workspace directory contains NUL",
                )
            })?,
            Component::CurDir | Component::RootDir => continue,
            Component::ParentDir | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "workspace directory must be normalized before secure open",
                ));
            }
        };
        let descriptor = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0,
            )
        };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        directory = unsafe { File::from_raw_fd(descriptor) };
    }
    Ok(directory)
}

#[cfg(unix)]
fn relative_components(path: &Path) -> io::Result<Vec<&std::ffi::OsStr>> {
    use std::path::Component;

    if path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "descriptor-relative path must not be absolute",
        ));
    }
    path.components()
        .map(|component| match component {
            Component::Normal(value) => Ok(value),
            Component::CurDir => Ok(std::ffi::OsStr::new(".")),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "descriptor-relative path is not normalized",
                ))
            }
        })
        .filter(|component| !matches!(component, Ok(value) if *value == std::ffi::OsStr::new(".")))
        .collect()
}

#[cfg(unix)]
fn open_regular_file_at_unix(parent: &File, relative: &Path) -> io::Result<File> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;

    let components = relative_components(relative)?;
    let Some(final_component) = components.last() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "descriptor-relative file path has no file component",
        ));
    };
    let mut directory = parent.try_clone()?;
    for component in &components[..components.len() - 1] {
        let name = CString::new(component.as_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "workspace path contains NUL")
        })?;
        let descriptor = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0,
            )
        };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        directory = unsafe { File::from_raw_fd(descriptor) };
    }
    let name = CString::new(final_component.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "workspace path contains NUL"))?;
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NONBLOCK | libc::O_NOCTTY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0,
        )
    };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

#[cfg(unix)]
fn open_directory_at_unix(parent: &File, relative: &Path) -> io::Result<File> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;

    let components = relative_components(relative)?;
    let mut directory = parent.try_clone()?;
    for component in components {
        let name = CString::new(component.as_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "workspace path contains NUL")
        })?;
        let descriptor = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0,
            )
        };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        directory = unsafe { File::from_raw_fd(descriptor) };
    }
    Ok(directory)
}

#[cfg(windows)]
fn open_regular_file_at_windows(_parent: &File, _relative: &Path) -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "descriptor-relative file access is not implemented for Windows handles",
    ))
}

#[cfg(windows)]
fn open_directory_at_windows(_parent: &File, _relative: &Path) -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "descriptor-relative directory access is not implemented for Windows handles",
    ))
}

#[cfg(windows)]
fn open_regular_file_windows(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};
    // Omit FILE_SHARE_DELETE so the authorized name cannot be replaced while
    // this capability is live.
    options.share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE);
    options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT);
    let file = options.open(path)?;
    verify_windows_handle_path(&file, path)?;
    Ok(file)
}

#[cfg(windows)]
fn open_directory_windows(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};
    options.share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE);
    options.custom_flags(
        windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT
            | windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS,
    );
    let directory = options.open(path)?;
    verify_windows_handle_path(&directory, path)?;
    Ok(directory)
}

#[cfg(windows)]
pub(crate) fn verify_windows_handle_path(file: &File, requested: &Path) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;

    let mut buffer = vec![0u16; 32 * 1024];
    let length = unsafe {
        windows_sys::Win32::Storage::FileSystem::GetFinalPathNameByHandleW(
            file.as_raw_handle() as _,
            buffer.as_mut_ptr(),
            buffer.len() as u32,
            windows_sys::Win32::Storage::FileSystem::FILE_NAME_NORMALIZED,
        )
    };
    if length == 0 {
        return Err(io::Error::last_os_error());
    }
    if length as usize >= buffer.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "opened workspace path exceeds the Windows final-path bound",
        ));
    }
    let actual = String::from_utf16(&buffer[..length as usize]).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Windows returned a non-Unicode final workspace path",
        )
    })?;
    let expected = windows_lexical_path(requested)?;
    if normalize_windows_path(&actual) != normalize_windows_path(&expected) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "workspace path resolved through a reparse-point or changed parent",
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn windows_lexical_path(path: &Path) -> io::Result<String> {
    use std::path::Component;

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = std::path::PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::ParentDir => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "workspace path must be normalized before secure open",
                ));
            }
            Component::CurDir => {}
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir | Component::Normal(_) => normalized.push(component.as_os_str()),
        }
    }
    Ok(normalized.to_string_lossy().into_owned())
}

#[cfg(windows)]
fn normalize_windows_path(path: &str) -> String {
    let path = path.replace('/', "\\");
    let path = path
        .strip_prefix(r"\\?\UNC\")
        .map(|rest| format!(r"\\{rest}"))
        .or_else(|| path.strip_prefix(r"\\?\").map(str::to_owned))
        .unwrap_or(path);
    path.to_ascii_lowercase()
}

#[cfg(not(any(unix, windows)))]
fn open_regular_file_portable(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    options.open(path)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotLimits {
    pub max_file_bytes: u64,
    pub inline_threshold: u64,
}

impl Default for SnapshotLimits {
    fn default() -> Self {
        Self {
            max_file_bytes: 16 * 1024 * 1024,
            inline_threshold: 64 * 1024,
        }
    }
}

impl SnapshotLimits {
    pub fn validate(&self) -> Result<(), SnapshotError> {
        if self.max_file_bytes == 0 || self.inline_threshold > self.max_file_bytes {
            return Err(SnapshotError::new(SnapshotErrorCode::InvalidLimits));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotErrorCode {
    InvalidLimits,
    TooLarge,
    BinaryContent,
    InvalidUtf8,
    Io,
    SpoolUnavailable,
    SpoolCorrupt,
}

#[derive(Debug)]
pub struct SnapshotError {
    pub code: SnapshotErrorCode,
    pub source: Option<io::Error>,
}

impl SnapshotError {
    pub const fn new(code: SnapshotErrorCode) -> Self {
        Self { code, source: None }
    }

    fn io(source: io::Error) -> Self {
        Self {
            code: SnapshotErrorCode::Io,
            source: Some(source),
        }
    }
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(source) = &self.source {
            write!(f, "snapshot {:?}: {source}", self.code)
        } else {
            write!(f, "snapshot {:?}", self.code)
        }
    }
}

impl std::error::Error for SnapshotError {}

#[derive(Debug)]
pub enum SnapshotStorage {
    Inline(Vec<u8>),
    Spooled(TempPath),
}

impl SnapshotStorage {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Inline(_) => "inline",
            Self::Spooled(_) => "spooled",
        }
    }

    fn read_bytes(&self) -> Result<Vec<u8>, SnapshotError> {
        match self {
            Self::Inline(bytes) => Ok(bytes.clone()),
            Self::Spooled(path) => std::fs::read(path).map_err(SnapshotError::io),
        }
    }
}

/// An exact, bounded text snapshot. The storage owns a temporary spool path
/// when content exceeds the inline threshold; dropping the snapshot removes it.
#[derive(Debug)]
pub struct TextSnapshot {
    pub version: FileContentVersion,
    pub encoding: SnapshotEncoding,
    pub line_endings: SnapshotLineEndings,
    pub storage: SnapshotStorage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotEncoding {
    Utf8,
    Utf8Bom,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotLineEnding {
    Lf,
    Crlf,
    Mixed,
    None,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotLineEndings {
    pub dominant: SnapshotLineEnding,
    pub mixed: bool,
    pub final_newline: bool,
}

impl TextSnapshot {
    pub fn from_bytes(bytes: Vec<u8>, limits: SnapshotLimits) -> Result<Self, SnapshotError> {
        limits.validate()?;
        if bytes.len() as u64 > limits.max_file_bytes {
            return Err(SnapshotError::new(SnapshotErrorCode::TooLarge));
        }
        let metadata = inspect_text(&bytes)?;
        let version = FileContentVersion::new(FileVersionToken::from_bytes(&bytes));
        let storage = storage_for(bytes, limits.inline_threshold)?;
        Ok(Self {
            version,
            encoding: metadata.encoding,
            line_endings: metadata.line_endings,
            storage,
        })
    }

    pub fn from_reader<R: Read>(
        mut reader: R,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        limits.validate()?;
        // Stream the source once into a temporary spool while calculating the
        // exact hash and text metadata.  Only content below the inline
        // threshold is materialized back into memory; near-limit files never
        // exist simultaneously as both a full Vec and a spool.
        let mut spool = NamedTempFile::new()
            .map_err(|_| SnapshotError::new(SnapshotErrorCode::SpoolUnavailable))?;
        let mut hasher = Sha256::new();
        let mut byte_len = 0u64;
        let mut prefix = Vec::with_capacity(3);
        let mut last_byte = None;
        let mut previous_cr = false;
        let mut lf = 0u64;
        let mut crlf = 0u64;
        let mut lone_cr = 0u64;
        let mut utf8_tail = Vec::new();
        let mut has_nul = false;
        let mut buffer = [0u8; COPY_BUFFER_BYTES];
        loop {
            let count = reader.read(&mut buffer).map_err(SnapshotError::io)?;
            if count == 0 {
                break;
            }
            let next_len = byte_len.saturating_add(count as u64);
            if next_len > limits.max_file_bytes {
                return Err(SnapshotError::new(SnapshotErrorCode::TooLarge));
            }
            let chunk = &buffer[..count];
            spool.write_all(chunk).map_err(SnapshotError::io)?;
            hasher.update(chunk);
            byte_len = next_len;
            if prefix.len() < 3 {
                prefix.extend_from_slice(&chunk[..(3 - prefix.len()).min(chunk.len())]);
            }
            has_nul |= chunk.contains(&0);
            last_byte = chunk.last().copied().or(last_byte);
            update_line_endings(chunk, &mut previous_cr, &mut lf, &mut crlf, &mut lone_cr);
            validate_utf8_chunk(chunk, &mut utf8_tail)?;
        }
        if previous_cr {
            lone_cr = lone_cr.saturating_add(1);
        }
        if !utf8_tail.is_empty() {
            return Err(SnapshotError::new(SnapshotErrorCode::InvalidUtf8));
        }
        if has_nul {
            return Err(SnapshotError::new(SnapshotErrorCode::BinaryContent));
        }
        spool.as_file().sync_all().map_err(SnapshotError::io)?;
        let kinds = [lf > 0, crlf > 0, lone_cr > 0]
            .into_iter()
            .filter(|value| *value)
            .count();
        let line_endings = SnapshotLineEndings {
            dominant: if kinds == 0 {
                SnapshotLineEnding::None
            } else if crlf >= lf && crlf >= lone_cr {
                SnapshotLineEnding::Crlf
            } else if lf >= lone_cr {
                SnapshotLineEnding::Lf
            } else {
                SnapshotLineEnding::Mixed
            },
            mixed: kinds > 1,
            final_newline: matches!(last_byte, Some(b'\n' | b'\r')),
        };
        let digest = hasher.finalize();
        let mut digest_bytes = [0u8; 32];
        digest_bytes.copy_from_slice(&digest);
        let version = FileContentVersion::new(FileVersionToken::new(digest_bytes, byte_len));
        let encoding = if prefix == [0xef, 0xbb, 0xbf] {
            SnapshotEncoding::Utf8Bom
        } else {
            SnapshotEncoding::Utf8
        };
        let storage = if byte_len <= limits.inline_threshold {
            SnapshotStorage::Inline(std::fs::read(spool.path()).map_err(SnapshotError::io)?)
        } else {
            SnapshotStorage::Spooled(spool.into_temp_path())
        };
        Ok(Self {
            version,
            encoding,
            line_endings,
            storage,
        })
    }

    pub fn from_file(
        path: impl AsRef<Path>,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        let file = open_regular_file(path).map_err(SnapshotError::io)?;
        Self::from_reader(file, limits)
    }

    pub fn byte_len(&self) -> u64 {
        self.version.token.byte_len()
    }

    pub fn storage_kind(&self) -> &'static str {
        self.storage.kind()
    }

    pub fn bytes(&self) -> Result<Vec<u8>, SnapshotError> {
        let bytes = self.storage.read_bytes()?;
        let actual = FileVersionToken::from_bytes(&bytes);
        if actual != self.version.token {
            return Err(SnapshotError::new(SnapshotErrorCode::SpoolCorrupt));
        }
        Ok(bytes)
    }

    pub fn text(&self) -> Result<String, SnapshotError> {
        let bytes = self.bytes()?;
        String::from_utf8(strip_bom(&bytes).to_vec())
            .map_err(|_| SnapshotError::new(SnapshotErrorCode::InvalidUtf8))
    }
}

struct TextMetadata {
    encoding: SnapshotEncoding,
    line_endings: SnapshotLineEndings,
}

fn inspect_text(bytes: &[u8]) -> Result<TextMetadata, SnapshotError> {
    if bytes.contains(&0) {
        return Err(SnapshotError::new(SnapshotErrorCode::BinaryContent));
    }
    let has_bom = bytes.starts_with(&[0xef, 0xbb, 0xbf]);
    let text = std::str::from_utf8(strip_bom(bytes))
        .map_err(|_| SnapshotError::new(SnapshotErrorCode::InvalidUtf8))?;
    let mut lf = 0u64;
    let mut crlf = 0u64;
    let mut lone_cr = 0u64;
    let raw = text.as_bytes();
    let mut index = 0;
    while index < raw.len() {
        match raw[index] {
            b'\r' if raw.get(index + 1) == Some(&b'\n') => {
                crlf += 1;
                index += 2;
            }
            b'\r' => {
                lone_cr += 1;
                index += 1;
            }
            b'\n' => {
                lf += 1;
                index += 1;
            }
            _ => index += 1,
        }
    }
    let kinds = [lf > 0, crlf > 0, lone_cr > 0]
        .into_iter()
        .filter(|value| *value)
        .count();
    let dominant = if kinds == 0 {
        SnapshotLineEnding::None
    } else if crlf >= lf && crlf >= lone_cr {
        SnapshotLineEnding::Crlf
    } else if lf >= lone_cr {
        SnapshotLineEnding::Lf
    } else {
        SnapshotLineEnding::Mixed
    };
    Ok(TextMetadata {
        encoding: if has_bom {
            SnapshotEncoding::Utf8Bom
        } else {
            SnapshotEncoding::Utf8
        },
        line_endings: SnapshotLineEndings {
            dominant,
            mixed: kinds > 1,
            final_newline: raw.ends_with(b"\n") || raw.ends_with(b"\r"),
        },
    })
}

fn strip_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(bytes)
}

fn storage_for(bytes: Vec<u8>, inline_threshold: u64) -> Result<SnapshotStorage, SnapshotError> {
    if bytes.len() as u64 <= inline_threshold {
        return Ok(SnapshotStorage::Inline(bytes));
    }
    let mut file = NamedTempFile::new()
        .map_err(|_| SnapshotError::new(SnapshotErrorCode::SpoolUnavailable))?;
    file.write_all(&bytes)
        .and_then(|_| file.as_file().sync_all())
        .map_err(SnapshotError::io)?;
    Ok(SnapshotStorage::Spooled(file.into_temp_path()))
}

fn validate_utf8_chunk(chunk: &[u8], tail: &mut Vec<u8>) -> Result<(), SnapshotError> {
    if tail.is_empty() {
        match std::str::from_utf8(chunk) {
            Ok(_) => Ok(()),
            Err(error) if error.error_len().is_none() => {
                tail.extend_from_slice(&chunk[error.valid_up_to()..]);
                Ok(())
            }
            Err(_) => Err(SnapshotError::new(SnapshotErrorCode::InvalidUtf8)),
        }
    } else {
        let mut combined = std::mem::take(tail);
        combined.extend_from_slice(chunk);
        match std::str::from_utf8(&combined) {
            Ok(_) => Ok(()),
            Err(error) if error.error_len().is_none() => {
                tail.extend_from_slice(&combined[error.valid_up_to()..]);
                Ok(())
            }
            Err(_) => Err(SnapshotError::new(SnapshotErrorCode::InvalidUtf8)),
        }
    }
}

fn update_line_endings(
    chunk: &[u8],
    previous_cr: &mut bool,
    lf: &mut u64,
    crlf: &mut u64,
    lone_cr: &mut u64,
) {
    for byte in chunk {
        if *previous_cr {
            if *byte == b'\n' {
                *crlf = crlf.saturating_add(1);
                *previous_cr = false;
                continue;
            }
            *lone_cr = lone_cr.saturating_add(1);
            *previous_cr = false;
        }
        match byte {
            b'\r' => *previous_cr = true,
            b'\n' => *lf = lf.saturating_add(1),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[cfg(unix)]
    #[test]
    fn from_file_rejects_final_symlink() {
        let directory = tempfile::tempdir().unwrap();
        let outside = directory.path().join("outside.txt");
        let link = directory.path().join("link.txt");
        std::fs::write(&outside, b"outside\n").unwrap();
        std::os::unix::fs::symlink(&outside, &link).unwrap();

        let error = TextSnapshot::from_file(&link, SnapshotLimits::default()).unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::Io);
    }

    #[cfg(unix)]
    #[test]
    fn secure_open_rejects_symlinked_parent_component() {
        let directory = tempfile::tempdir().unwrap();
        let outside = directory.path().join("outside");
        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&outside).unwrap();
        std::fs::create_dir(&workspace).unwrap();
        std::fs::write(outside.join("secret.txt"), b"outside\n").unwrap();
        std::os::unix::fs::symlink(&outside, workspace.join("link")).unwrap();

        let error = open_regular_file(workspace.join("link/secret.txt")).unwrap_err();
        assert!(error.raw_os_error().is_some());
    }

    #[test]
    fn metadata_preserves_bom_crlf_and_final_newline() {
        let snapshot = TextSnapshot::from_bytes(
            b"\xef\xbb\xbffirst\r\nsecond\r\n".to_vec(),
            SnapshotLimits::default(),
        )
        .unwrap();
        assert_eq!(snapshot.encoding, SnapshotEncoding::Utf8Bom);
        assert_eq!(snapshot.line_endings.dominant, SnapshotLineEnding::Crlf);
        assert!(snapshot.line_endings.final_newline);
        assert_eq!(snapshot.text().unwrap(), "first\r\nsecond\r\n");
    }

    #[test]
    fn mixed_endings_are_explicit_and_no_final_newline_is_preserved() {
        let snapshot = TextSnapshot::from_reader(
            Cursor::new(b"one\ntwo\r\nthree".to_vec()),
            SnapshotLimits::default(),
        )
        .unwrap();
        assert!(snapshot.line_endings.mixed);
        assert!(!snapshot.line_endings.final_newline);
        assert_eq!(snapshot.line_endings.dominant, SnapshotLineEnding::Crlf);
    }

    #[test]
    fn oversize_and_binary_input_fail_before_unbounded_storage() {
        let limits = SnapshotLimits {
            max_file_bytes: 2,
            inline_threshold: 2,
        };
        assert_eq!(
            TextSnapshot::from_reader(Cursor::new(b"abc"), limits)
                .unwrap_err()
                .code,
            SnapshotErrorCode::TooLarge
        );
        assert_eq!(
            TextSnapshot::from_bytes(b"a\0b".to_vec(), SnapshotLimits::default())
                .unwrap_err()
                .code,
            SnapshotErrorCode::BinaryContent
        );
    }

    #[test]
    fn large_snapshot_spools_and_round_trips_with_hash_revalidation() {
        let bytes = b"line\n".repeat(32);
        let snapshot = TextSnapshot::from_bytes(
            bytes.clone(),
            SnapshotLimits {
                max_file_bytes: 1024,
                inline_threshold: 4,
            },
        )
        .unwrap();
        assert_eq!(snapshot.storage_kind(), "spooled");
        assert_eq!(snapshot.bytes().unwrap(), bytes);
    }

    #[test]
    fn equal_metadata_does_not_hide_changed_bytes() {
        let first =
            TextSnapshot::from_bytes(b"same\n".to_vec(), SnapshotLimits::default()).unwrap();
        let second =
            TextSnapshot::from_bytes(b"SAME\n".to_vec(), SnapshotLimits::default()).unwrap();
        assert_ne!(first.version.token, second.version.token);
        assert_ne!(first.version.token.byte_len(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn fifo_is_rejected_without_waiting_for_a_writer() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let workspace = tempfile::tempdir().unwrap();
        let fifo = workspace.path().join("pipe");
        let fifo_name = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_name.as_ptr(), 0o600) }, 0);

        let started = std::time::Instant::now();
        let error = open_regular_file(&fifo).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }
}
