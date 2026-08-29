//! Apply Patch filesystem mutations anchored to already-open directory descriptors.
//!
//! `TargetResolver` rejects symlinks during preparation, but a pathname can be
//! replaced after that check. On Unix every visible mutation in this module is
//! relative to a directory opened component-by-component with `O_NOFOLLOW`.
//! This prevents a swapped parent symlink from redirecting a write outside the
//! authorized workspace. Other platforms retain the portable implementation;
//! their callers still revalidate immediately before mutation and report the
//! weaker platform contract through the existing durability warnings.

use crate::apply_patch::file_mutation::CanonicalTarget;
#[cfg(unix)]
use std::ffi::CString;
#[cfg(any(not(unix), test))]
use std::fs;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(not(unix))]
use tempfile::NamedTempFile;

#[cfg(unix)]
static STAGE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub(crate) struct ParentCreationFailure {
    pub source: io::Error,
    pub created: CreatedDirectories,
}

#[derive(Debug, Default)]
pub(crate) struct CreatedDirectories {
    #[cfg(unix)]
    entries: Vec<CreatedDirectory>,
    #[cfg(not(unix))]
    paths: Vec<PathBuf>,
}

#[cfg(unix)]
#[derive(Debug)]
struct CreatedDirectory {
    parent: File,
    name: CString,
    path: PathBuf,
    device: libc::dev_t,
    inode: libc::ino_t,
}

impl CreatedDirectories {
    pub(crate) fn paths(&self) -> Vec<PathBuf> {
        #[cfg(unix)]
        {
            self.entries
                .iter()
                .map(|entry| entry.path.clone())
                .collect()
        }
        #[cfg(windows)]
        {
            self.paths.clone()
        }
        #[cfg(not(any(unix, windows)))]
        {
            self.paths.clone()
        }
    }

    pub(crate) fn into_paths(self) -> Vec<PathBuf> {
        let mut paths = self.paths();
        paths.sort_by(|left, right| {
            left.components()
                .count()
                .cmp(&right.components().count())
                .reverse()
        });
        paths
    }

    /// Remove only directories whose directory-entry identity still matches
    /// the object created by this invocation. A replaced parent or entry is
    /// retained and reported instead of risking deletion through an attacker-
    /// controlled path.
    pub(crate) fn cleanup(self) -> (Vec<PathBuf>, Vec<PathBuf>) {
        let created = self.paths();
        #[cfg(unix)]
        let residual = {
            let mut residual = Vec::new();
            for entry in self.entries.into_iter().rev() {
                let unchanged = match stat_at(entry.parent.as_raw_fd(), &entry.name) {
                    Ok(status) => {
                        status.st_dev == entry.device
                            && status.st_ino == entry.inode
                            && status.st_mode & libc::S_IFMT == libc::S_IFDIR
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(_) => false,
                };
                if !unchanged
                    || unsafe {
                        libc::unlinkat(
                            entry.parent.as_raw_fd(),
                            entry.name.as_ptr(),
                            libc::AT_REMOVEDIR,
                        )
                    } != 0
                {
                    residual.push(entry.path);
                }
            }
            residual
        };
        #[cfg(not(unix))]
        let residual = {
            let mut residual = Vec::new();
            for path in self.paths.into_iter().rev() {
                match fs::remove_dir(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(_) => residual.push(path),
                }
            }
            residual
        };
        (created, residual)
    }
}

pub(crate) fn ensure_parent_directories(
    target: &CanonicalTarget,
) -> Result<CreatedDirectories, ParentCreationFailure> {
    #[cfg(unix)]
    {
        ensure_parent_directories_unix(target)
    }
    #[cfg(windows)]
    {
        ensure_parent_directories_windows(target)
    }
    #[cfg(not(any(unix, windows)))]
    {
        ensure_parent_directories_portable(target)
    }
}

#[cfg(unix)]
fn ensure_parent_directories_unix(
    target: &CanonicalTarget,
) -> Result<CreatedDirectories, ParentCreationFailure> {
    let (root, parent_components, _) =
        target_parts(target).map_err(|source| ParentCreationFailure {
            source,
            created: CreatedDirectories::default(),
        })?;
    let mut current = open_directory_path(&root).map_err(|source| ParentCreationFailure {
        source,
        created: CreatedDirectories::default(),
    })?;
    let mut created = CreatedDirectories::default();
    let mut reported = workspace_root(target).unwrap_or_else(|_| root.clone());

    for component in parent_components {
        reported.push(&component);
        let name = match os_name(&component) {
            Ok(name) => name,
            Err(source) => return Err(ParentCreationFailure { source, created }),
        };
        match open_directory_at(&current, &name) {
            Ok(next) => {
                current = next;
                continue;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(source) => return Err(ParentCreationFailure { source, created }),
        }

        if unsafe { libc::mkdirat(current.as_raw_fd(), name.as_ptr(), 0o777) } != 0 {
            let source = io::Error::last_os_error();
            if source.kind() != io::ErrorKind::AlreadyExists {
                return Err(ParentCreationFailure { source, created });
            }
        }
        let next = match open_directory_at(&current, &name) {
            Ok(next) => next,
            Err(source) => return Err(ParentCreationFailure { source, created }),
        };
        let status = match fstat(next.as_raw_fd()) {
            Ok(status) => status,
            Err(source) => return Err(ParentCreationFailure { source, created }),
        };
        created.entries.push(CreatedDirectory {
            parent: match current.try_clone() {
                Ok(parent) => parent,
                Err(source) => return Err(ParentCreationFailure { source, created }),
            },
            name,
            path: reported.clone(),
            device: status.st_dev,
            inode: status.st_ino,
        });
        current = next;
    }
    Ok(created)
}

#[cfg(windows)]
fn ensure_parent_directories_windows(
    target: &CanonicalTarget,
) -> Result<CreatedDirectories, ParentCreationFailure> {
    let Some(parent) = target.absolute().parent() else {
        return Ok(CreatedDirectories::default());
    };
    let root = workspace_root(target).map_err(|source| ParentCreationFailure {
        source,
        created: CreatedDirectories::default(),
    })?;
    let relative_parent = parent
        .strip_prefix(&root)
        .map_err(|_| ParentCreationFailure {
            source: invalid_path("target parent is outside the workspace root"),
            created: CreatedDirectories::default(),
        })?;
    let root_guard = open_windows_directory(&root).map_err(|source| ParentCreationFailure {
        source,
        created: CreatedDirectories::default(),
    })?;
    let mut guards = vec![root_guard];
    let mut current = root;
    let mut created = CreatedDirectories::default();
    for component in relative_parent.components() {
        let Component::Normal(component) = component else {
            return Err(ParentCreationFailure {
                source: invalid_path("target parent is not normalized"),
                created,
            });
        };
        current.push(component);
        let next = match open_windows_directory(&current) {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if let Err(source) = fs::create_dir(&current) {
                    if source.kind() != io::ErrorKind::AlreadyExists {
                        return Err(ParentCreationFailure { source, created });
                    }
                } else {
                    created.paths.push(current.clone());
                }
                match open_windows_directory(&current) {
                    Ok(directory) => directory,
                    Err(source) => return Err(ParentCreationFailure { source, created }),
                }
            }
            Err(source) => return Err(ParentCreationFailure { source, created }),
        };
        // Every ancestor handle denies delete sharing. None of the already
        // traversed components can be swapped while a child is inspected or
        // created.
        guards.push(next);
    }
    drop(guards);
    Ok(created)
}

#[cfg(not(any(unix, windows)))]
fn ensure_parent_directories_portable(
    target: &CanonicalTarget,
) -> Result<CreatedDirectories, ParentCreationFailure> {
    let Some(parent) = target.absolute().parent() else {
        return Ok(CreatedDirectories::default());
    };
    let root = workspace_root(target).map_err(|source| ParentCreationFailure {
        source,
        created: CreatedDirectories::default(),
    })?;
    let relative_parent = parent
        .strip_prefix(&root)
        .map_err(|_| ParentCreationFailure {
            source: invalid_path("target parent is outside the workspace root"),
            created: CreatedDirectories::default(),
        })?;
    let mut current = root;
    let mut created = CreatedDirectories::default();
    for component in relative_parent.components() {
        let Component::Normal(component) = component else {
            return Err(ParentCreationFailure {
                source: invalid_path("target parent is not normalized"),
                created,
            });
        };
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(ParentCreationFailure {
                    source: io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "mutation parent is not a real directory",
                    ),
                    created,
                });
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if let Err(source) = fs::create_dir(&current) {
                    return Err(ParentCreationFailure { source, created });
                }
                created.paths.push(current.clone());
            }
            Err(source) => return Err(ParentCreationFailure { source, created }),
        }
    }
    Ok(created)
}

#[derive(Debug)]
pub(crate) struct StagedFile {
    #[cfg(unix)]
    file: Option<File>,
    #[cfg(unix)]
    parent: Option<File>,
    #[cfg(unix)]
    parent_identity: (libc::dev_t, libc::ino_t),
    #[cfg(unix)]
    temporary_name: CString,
    #[cfg(unix)]
    destination_name: CString,
    #[cfg(unix)]
    /// Pathname-bound staging retains the original target as a final binding
    /// fence. Descriptor-backed staging deliberately has no pathname fence:
    /// the captured directory capability is its authoritative destination.
    target: Option<CanonicalTarget>,
    #[cfg(unix)]
    active_name: bool,
    #[cfg(not(unix))]
    file: Option<NamedTempFile>,
    #[cfg(windows)]
    parent: Option<File>,
    #[cfg(windows)]
    destination_name: Vec<u16>,
    #[cfg(not(any(unix, windows)))]
    destination: PathBuf,
}

#[derive(Debug)]
pub(crate) struct PublishError {
    pub source: io::Error,
    pub cleanup_failed: bool,
    pub published: bool,
}

#[derive(Debug)]
pub(crate) struct PublishedParent {
    #[cfg(unix)]
    parent: File,
    pub cleanup_failed: bool,
}

impl PublishedParent {
    pub(crate) fn sync_all(&self) -> io::Result<()> {
        #[cfg(unix)]
        {
            self.parent.sync_all()
        }
        #[cfg(windows)]
        {
            Ok(())
        }
        #[cfg(not(any(unix, windows)))]
        {
            Ok(())
        }
    }
}

impl StagedFile {
    pub(crate) fn create(target: &CanonicalTarget) -> io::Result<Self> {
        #[cfg(unix)]
        {
            let (parent, destination_name) = open_target_parent(target)?;
            stage_in_parent(parent, destination_name, Some(target))
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            let parent_path = target
                .absolute()
                .parent()
                .ok_or_else(|| invalid_path("target has no parent"))?;
            let parent = open_windows_directory(parent_path)?;
            let file = NamedTempFile::new_in(parent_path)?;
            crate::apply_patch::file_mutation::snapshot::verify_windows_handle_path(
                file.as_file(),
                file.path(),
            )?;
            let destination_name = target
                .absolute()
                .file_name()
                .ok_or_else(|| invalid_path("target has no filename"))?
                .encode_wide()
                .collect();
            Ok(Self {
                file: Some(file),
                parent: Some(parent),
                destination_name,
            })
        }
        #[cfg(not(any(unix, windows)))]
        {
            let parent = target
                .absolute()
                .parent()
                .ok_or_else(|| invalid_path("target has no parent"))?;
            Ok(Self {
                file: Some(NamedTempFile::new_in(parent)?),
                destination: target.absolute().to_path_buf(),
            })
        }
    }

    /// Stage a file relative to an already-authorized directory descriptor.
    /// The destination pathname is never reopened to find its parent, so a
    /// replacement of an authorized root or ancestor cannot redirect the
    /// temporary file or its later atomic publish.
    pub(crate) fn create_at(
        anchor: &File,
        relative_destination: &Path,
        target: &CanonicalTarget,
        create_dirs: bool,
    ) -> io::Result<Self> {
        #[cfg(unix)]
        let _ = target;
        #[cfg(unix)]
        {
            let (parent, destination_name) =
                open_relative_target_parent(anchor, relative_destination, create_dirs)?;
            stage_in_parent(parent, destination_name, None)
        }
        #[cfg(windows)]
        {
            // Windows has no openat(2). The authorized anchor handle is kept
            // open while the path is materialized, and every opened parent is
            // verified by file identity/final handle path and opened without
            // delete sharing. That pins the destination parent against rename
            // before the temporary file is created and later renamed by
            // SetFileInformationByHandle relative to that parent handle.
            let _anchor_guard = anchor;
            if relative_destination.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            }) {
                return Err(invalid_path(
                    "relative destination escapes its authorized anchor",
                ));
            }
            if create_dirs {
                ensure_parent_directories(target).map_err(|failure| failure.source)?;
            }
            Self::create(target)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = (anchor, relative_destination, target, create_dirs);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "descriptor-relative staging is unavailable on this platform",
            ))
        }
    }

    pub(crate) fn file(&self) -> &File {
        #[cfg(unix)]
        {
            self.file.as_ref().expect("active staged file has a handle")
        }
        #[cfg(not(unix))]
        {
            self.file
                .as_ref()
                .expect("active staged file has a handle")
                .as_file()
        }
    }

    pub(crate) fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        #[cfg(unix)]
        {
            self.file
                .as_mut()
                .expect("active staged file has a handle")
                .write_all(bytes)
        }
        #[cfg(not(unix))]
        {
            self.file
                .as_mut()
                .expect("active staged file has a handle")
                .write_all(bytes)
        }
    }

    pub(crate) fn sync_all(&self) -> io::Result<()> {
        self.file().sync_all()
    }

    pub(crate) fn cleanup(mut self, inject_failure: bool) -> bool {
        if inject_failure {
            self.disarm_cleanup();
            return false;
        }
        #[cfg(unix)]
        {
            let result = self.unlink_temporary();
            if result.is_err() {
                self.disarm_cleanup();
            }
            result.is_ok()
        }
        #[cfg(not(unix))]
        {
            let Some(file) = self.file.take() else {
                return true;
            };
            file.close().is_ok()
        }
    }

    pub(crate) fn publish_replace(
        mut self,
        inject_cleanup_failure: bool,
    ) -> Result<PublishedParent, PublishError> {
        #[cfg(unix)]
        {
            if let Err(source) = self.verify_parent_binding() {
                let cleanup_failed = !self.cleanup(inject_cleanup_failure);
                return Err(PublishError {
                    source,
                    cleanup_failed,
                    published: false,
                });
            }
            let parent = self.parent.as_ref().expect("staged parent is present");
            if unsafe {
                libc::renameat(
                    parent.as_raw_fd(),
                    self.temporary_name.as_ptr(),
                    parent.as_raw_fd(),
                    self.destination_name.as_ptr(),
                )
            } != 0
            {
                let source = io::Error::last_os_error();
                let cleanup_failed = !self.cleanup(inject_cleanup_failure);
                return Err(PublishError {
                    source,
                    cleanup_failed,
                    published: false,
                });
            }
            self.active_name = false;
            self.file.take();
            let parent = self.parent.take().expect("staged parent is present");
            Ok(PublishedParent {
                parent,
                cleanup_failed: false,
            })
        }
        #[cfg(windows)]
        {
            let file = self.file.take().expect("active staged file is present");
            let parent = self.parent.take().expect("staged parent is present");
            publish_windows_stage(
                file,
                &parent,
                &self.destination_name,
                true,
                inject_cleanup_failure,
            )?;
            Ok(PublishedParent {
                cleanup_failed: false,
            })
        }
        #[cfg(not(any(unix, windows)))]
        {
            let file = self.file.take().expect("active staged file is present");
            let stage = file.into_temp_path();
            match replace_path(stage.as_ref(), &self.destination) {
                Ok(()) => Ok(PublishedParent {
                    cleanup_failed: false,
                }),
                Err(source) => {
                    let cleanup_failed = if inject_cleanup_failure {
                        let _ = stage.keep();
                        true
                    } else {
                        stage.close().is_err()
                    };
                    Err(PublishError {
                        source,
                        cleanup_failed,
                        published: false,
                    })
                }
            }
        }
    }

    #[allow(unused_mut)]
    pub(crate) fn publish_no_replace(
        mut self,
        inject_cleanup_failure: bool,
    ) -> Result<PublishedParent, PublishError> {
        #[cfg(unix)]
        {
            if let Err(source) = self.verify_parent_binding() {
                let cleanup_failed = !self.cleanup(inject_cleanup_failure);
                return Err(PublishError {
                    source,
                    cleanup_failed,
                    published: false,
                });
            }
            let parent = self.parent.as_ref().expect("staged parent is present");
            if unsafe {
                libc::linkat(
                    parent.as_raw_fd(),
                    self.temporary_name.as_ptr(),
                    parent.as_raw_fd(),
                    self.destination_name.as_ptr(),
                    0,
                )
            } != 0
            {
                let source = io::Error::last_os_error();
                let cleanup_failed = !self.cleanup(inject_cleanup_failure);
                return Err(PublishError {
                    source,
                    cleanup_failed,
                    published: false,
                });
            }
            let published_parent = match parent.try_clone() {
                Ok(parent) => parent,
                Err(source) => {
                    // The destination link is already visible. Preserve that
                    // truth and surface the inability to retain a sync handle
                    // as a post-publication error.
                    let cleanup_failed = !self.cleanup(inject_cleanup_failure);
                    return Err(PublishError {
                        source,
                        cleanup_failed,
                        published: true,
                    });
                }
            };
            let cleanup_failed = !self.cleanup(inject_cleanup_failure);
            Ok(PublishedParent {
                parent: published_parent,
                cleanup_failed,
            })
        }
        #[cfg(windows)]
        {
            let file = self.file.take().expect("active staged file is present");
            let parent = self.parent.take().expect("staged parent is present");
            publish_windows_stage(
                file,
                &parent,
                &self.destination_name,
                false,
                inject_cleanup_failure,
            )?;
            Ok(PublishedParent {
                cleanup_failed: false,
            })
        }
        #[cfg(not(any(unix, windows)))]
        {
            let file = self.file.take().expect("active staged file is present");
            let stage = file.into_temp_path();
            if let Err(source) = fs::hard_link(&stage, &self.destination) {
                let cleanup_failed = if inject_cleanup_failure {
                    let _ = stage.keep();
                    true
                } else {
                    stage.close().is_err()
                };
                return Err(PublishError {
                    source,
                    cleanup_failed,
                    published: false,
                });
            }
            let cleanup_failed = if inject_cleanup_failure {
                let _ = stage.keep();
                true
            } else {
                stage.close().is_err()
            };
            Ok(PublishedParent { cleanup_failed })
        }
    }

    #[cfg(unix)]
    fn verify_parent_binding(&self) -> io::Result<()> {
        // The descriptor is authoritative for the actual rename. Re-open the
        // original parent only as a fence: if the visible pathname was
        // replaced after staging, fail closed instead of publishing into a
        // now-hidden stale directory object.
        let Some(target) = self.target.as_ref() else {
            return Ok(());
        };
        let (current, current_name) = open_target_parent(target)?;
        let parent = self.parent.as_ref().expect("staged parent is present");
        let status = fstat(parent.as_raw_fd())?;
        if (status.st_dev, status.st_ino) != self.parent_identity
            || current_name != self.destination_name
            || fstat(current.as_raw_fd())
                .map(|status| (status.st_dev, status.st_ino))
                .ok()
                != Some(self.parent_identity)
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "mutation parent changed after staging",
            ));
        }
        Ok(())
    }

    #[cfg(unix)]
    fn unlink_temporary(&mut self) -> io::Result<()> {
        if !self.active_name {
            return Ok(());
        }
        let parent = self.parent.as_ref().expect("staged parent is present");
        if unsafe { libc::unlinkat(parent.as_raw_fd(), self.temporary_name.as_ptr(), 0) } != 0 {
            return Err(io::Error::last_os_error());
        }
        self.active_name = false;
        Ok(())
    }

    fn disarm_cleanup(&mut self) {
        #[cfg(unix)]
        {
            self.active_name = false;
        }
        #[cfg(not(unix))]
        if let Some(file) = self.file.take() {
            let _ = file.into_temp_path().keep();
        }
    }
}

#[cfg(unix)]
fn stage_in_parent(
    parent: File,
    destination_name: CString,
    target: Option<&CanonicalTarget>,
) -> io::Result<StagedFile> {
    let status = fstat(parent.as_raw_fd())?;
    for _ in 0..128 {
        let sequence = STAGE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary_name =
            CString::new(format!(".pioneer-patch-{}-{sequence}", std::process::id()))
                .map_err(|_| invalid_path("temporary filename contains NUL"))?;
        let descriptor = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                temporary_name.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600,
            )
        };
        if descriptor >= 0 {
            return Ok(StagedFile {
                file: Some(unsafe { File::from_raw_fd(descriptor) }),
                parent: Some(parent),
                parent_identity: (status.st_dev, status.st_ino),
                temporary_name,
                destination_name,
                target: target.cloned(),
                active_name: true,
            });
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::AlreadyExists {
            return Err(error);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a unique staging file",
    ))
}

#[cfg(unix)]
fn open_relative_target_parent(
    anchor: &File,
    relative_destination: &Path,
    create_dirs: bool,
) -> io::Result<(File, CString)> {
    let mut components = relative_destination.components().collect::<Vec<_>>();
    let Some(Component::Normal(final_component)) = components.pop() else {
        return Err(invalid_path(
            "staged destination has no normalized file component",
        ));
    };
    let mut current = anchor.try_clone()?;
    for component in components {
        let Component::Normal(component) = component else {
            return Err(invalid_path("staged destination is not normalized"));
        };
        let name = os_name(component)?;
        match open_directory_at(&current, &name) {
            Ok(next) => current = next,
            Err(error) if error.kind() == io::ErrorKind::NotFound && create_dirs => {
                if unsafe { libc::mkdirat(current.as_raw_fd(), name.as_ptr(), 0o777) } != 0 {
                    let mkdir_error = io::Error::last_os_error();
                    if mkdir_error.kind() != io::ErrorKind::AlreadyExists {
                        return Err(mkdir_error);
                    }
                }
                current = open_directory_at(&current, &name)?;
            }
            Err(error) => return Err(error),
        }
    }
    Ok((current, os_name(final_component)?))
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            let _ = self.unlink_temporary();
        }
    }
}

pub(crate) fn remove_regular_file(target: &CanonicalTarget) -> io::Result<PublishedParent> {
    #[cfg(unix)]
    {
        let (parent, name) = open_target_parent(target)?;
        let status = stat_at(parent.as_raw_fd(), &name)?;
        if status.st_mode & libc::S_IFMT != libc::S_IFREG {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mutation target is not a regular file",
            ));
        }
        if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(PublishedParent {
            parent,
            cleanup_failed: false,
        })
    }
    #[cfg(windows)]
    {
        remove_regular_file_windows(target)
    }
    #[cfg(not(any(unix, windows)))]
    {
        fs::remove_file(target.absolute())?;
        Ok(PublishedParent {
            cleanup_failed: false,
        })
    }
}

#[cfg(unix)]
fn open_target_parent(target: &CanonicalTarget) -> io::Result<(File, CString)> {
    let (root, parent_components, final_component) = target_parts(target)?;
    let mut current = open_directory_path(&root)?;
    for component in parent_components {
        current = open_directory_at(&current, &os_name(&component)?)?;
    }
    Ok((current, os_name(&final_component)?))
}

#[cfg(unix)]
fn target_parts(
    target: &CanonicalTarget,
) -> io::Result<(PathBuf, Vec<std::ffi::OsString>, std::ffi::OsString)> {
    let root = workspace_root(target)?;
    let mut components = target.relative().components().collect::<Vec<_>>();
    let Some(Component::Normal(final_component)) = components.pop() else {
        return Err(invalid_path(
            "mutation target has no normalized file component",
        ));
    };
    let mut parents = Vec::with_capacity(components.len());
    for component in components {
        let Component::Normal(component) = component else {
            return Err(invalid_path("mutation target is not normalized"));
        };
        parents.push(component.to_os_string());
    }
    Ok((
        crate::apply_patch::file_mutation::snapshot::trusted_platform_path(&root).into_owned(),
        parents,
        final_component.to_os_string(),
    ))
}

fn workspace_root(target: &CanonicalTarget) -> io::Result<PathBuf> {
    let mut root = target.absolute().to_path_buf();
    let component_count = target
        .relative()
        .components()
        .filter(|component| matches!(component, Component::Normal(_)))
        .count();
    for _ in 0..component_count {
        if !root.pop() {
            return Err(invalid_path(
                "mutation target is outside its workspace root",
            ));
        }
    }
    if root.join(target.relative()) != target.absolute() {
        return Err(invalid_path("mutation target root binding is inconsistent"));
    }
    Ok(root)
}

#[cfg(unix)]
fn open_directory_path(path: &Path) -> io::Result<File> {
    let mut directory = if path.is_absolute() {
        File::open("/")?
    } else {
        File::open(".")?
    };
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(component) => {
                directory = open_directory_at(&directory, &os_name(component)?)?;
            }
            Component::ParentDir | Component::Prefix(_) => {
                return Err(invalid_path("directory path is not normalized"));
            }
        }
    }
    Ok(directory)
}

#[cfg(unix)]
fn open_directory_at(parent: &File, name: &CString) -> io::Result<File> {
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0,
        )
    };
    if descriptor < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(unix)]
fn os_name(value: impl AsRef<std::ffi::OsStr>) -> io::Result<CString> {
    CString::new(value.as_ref().as_bytes())
        .map_err(|_| invalid_path("mutation path component contains NUL"))
}

#[cfg(unix)]
fn fstat(descriptor: libc::c_int) -> io::Result<libc::stat> {
    let mut status = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(descriptor, status.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { status.assume_init() })
}

#[cfg(unix)]
fn stat_at(parent: libc::c_int, name: &CString) -> io::Result<libc::stat> {
    let mut status = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            parent,
            name.as_ptr(),
            status.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { status.assume_init() })
}

fn invalid_path(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(windows)]
fn open_windows_directory(path: &Path) -> io::Result<File> {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_READ_ATTRIBUTES, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    // Deliberately omit FILE_SHARE_DELETE. Keeping this handle alive pins the
    // directory name against rename/removal while a stage is created and
    // published relative to the handle.
    let directory = OpenOptions::new()
        .access_mode(FILE_READ_ATTRIBUTES)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    crate::apply_patch::file_mutation::snapshot::verify_windows_handle_path(&directory, path)?;
    use std::os::windows::fs::MetadataExt;
    let metadata = directory.metadata()?;
    if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "mutation parent is not a real directory",
        ));
    }
    Ok(directory)
}

#[cfg(windows)]
fn publish_windows_stage(
    file: NamedTempFile,
    parent: &File,
    destination_name: &[u16],
    replace: bool,
    inject_cleanup_failure: bool,
) -> Result<(), PublishError> {
    let (file, path) = file.into_parts();
    match rename_windows_handle(&file, parent, destination_name, replace) {
        Ok(()) => {
            drop(file);
            drop(path);
            Ok(())
        }
        Err(source) => {
            drop(file);
            let cleanup_failed = if inject_cleanup_failure {
                let _ = path.keep();
                true
            } else {
                path.close().is_err()
            };
            Err(PublishError {
                source,
                cleanup_failed,
                published: false,
            })
        }
    }
}

#[cfg(windows)]
fn rename_windows_handle(
    file: &File,
    parent: &File,
    destination_name: &[u16],
    replace: bool,
) -> io::Result<()> {
    use std::mem::size_of;
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows_sys::Win32::Storage::FileSystem::{
        DELETE, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_RENAME_INFO, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, FileRenameInfo, ReOpenFile, SetFileInformationByHandle,
    };

    if destination_name.is_empty() {
        return Err(invalid_path("destination filename is empty"));
    }
    let reopened = unsafe {
        ReOpenFile(
            file.as_raw_handle() as _,
            FILE_GENERIC_READ | FILE_GENERIC_WRITE | DELETE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            0,
        )
    };
    if reopened == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    let reopened = unsafe { File::from_raw_handle(reopened as _) };

    let header_bytes = size_of::<FILE_RENAME_INFO>() - size_of::<u16>();
    let filename_bytes = destination_name
        .len()
        .checked_mul(size_of::<u16>())
        .ok_or_else(|| invalid_path("destination filename is too large"))?;
    let buffer_bytes = header_bytes
        .checked_add(filename_bytes)
        .ok_or_else(|| invalid_path("rename buffer is too large"))?;
    let units = buffer_bytes.div_ceil(size_of::<FILE_RENAME_INFO>());
    let mut buffer = vec![FILE_RENAME_INFO::default(); units.max(1)];
    let information = buffer.as_mut_ptr();
    unsafe {
        (*information).Anonymous.ReplaceIfExists = replace;
        (*information).RootDirectory = parent.as_raw_handle() as _;
        (*information).FileNameLength = u32::try_from(filename_bytes)
            .map_err(|_| invalid_path("destination filename is too large"))?;
        std::ptr::copy_nonoverlapping(
            destination_name.as_ptr(),
            std::ptr::addr_of_mut!((*information).FileName).cast::<u16>(),
            destination_name.len(),
        );
    }
    let result = unsafe {
        SetFileInformationByHandle(
            reopened.as_raw_handle() as _,
            FileRenameInfo,
            information.cast(),
            u32::try_from(buffer_bytes).map_err(|_| invalid_path("rename buffer is too large"))?,
        )
    };
    if result == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(windows)]
fn remove_regular_file_windows(target: &CanonicalTarget) -> io::Result<PublishedParent> {
    use std::fs::OpenOptions;
    use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        DELETE, FILE_ATTRIBUTE_REPARSE_POINT, FILE_DISPOSITION_INFO, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_READ_ATTRIBUTES, FILE_SHARE_READ, FILE_SHARE_WRITE, FileDispositionInfo,
        SetFileInformationByHandle,
    };

    let parent_path = target
        .absolute()
        .parent()
        .ok_or_else(|| invalid_path("target has no parent"))?;
    let _parent = open_windows_directory(parent_path)?;
    let file = OpenOptions::new()
        .access_mode(DELETE | FILE_READ_ATTRIBUTES)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(target.absolute())?;
    crate::apply_patch::file_mutation::snapshot::verify_windows_handle_path(
        &file,
        target.absolute(),
    )?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mutation target is not a regular file",
        ));
    }
    let information = FILE_DISPOSITION_INFO { DeleteFile: true };
    if unsafe {
        SetFileInformationByHandle(
            file.as_raw_handle() as _,
            FileDispositionInfo,
            std::ptr::addr_of!(information).cast(),
            u32::try_from(std::mem::size_of::<FILE_DISPOSITION_INFO>())
                .expect("Windows disposition structure size fits u32"),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    drop(file);
    Ok(PublishedParent {
        cleanup_failed: false,
    })
}

#[cfg(not(any(unix, windows)))]
fn replace_path(stage: &Path, destination: &Path) -> io::Result<()> {
    fs::rename(stage, destination)
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use super::*;
    #[cfg(unix)]
    use crate::apply_patch::file_mutation::{TargetExpectation, TargetResolver, TargetRole};

    #[cfg(unix)]
    fn target(root: &Path, path: &str, expectation: TargetExpectation) -> CanonicalTarget {
        TargetResolver::new(root)
            .unwrap()
            .resolve(path, TargetRole::Destination, expectation)
            .unwrap()
    }

    #[cfg(unix)]
    #[test]
    fn staged_publish_cannot_escape_through_swapped_parent_symlink() {
        use std::os::unix::fs::symlink;

        let sandbox = tempfile::tempdir().unwrap();
        let workspace = sandbox.path().join("workspace");
        let outside = sandbox.path().join("outside");
        fs::create_dir(&workspace).unwrap();
        fs::create_dir(&outside).unwrap();
        fs::create_dir(workspace.join("parent")).unwrap();
        let target = target(&workspace, "parent/file.txt", TargetExpectation::Missing);
        let mut staged = StagedFile::create(&target).unwrap();
        staged.write_all(b"inside\n").unwrap();

        fs::rename(workspace.join("parent"), workspace.join("old-parent")).unwrap();
        symlink(&outside, workspace.join("parent")).unwrap();

        let error = staged.publish_no_replace(false).unwrap_err();
        assert!(matches!(
            error.source.kind(),
            io::ErrorKind::PermissionDenied | io::ErrorKind::NotADirectory
        ));
        assert!(!outside.join("file.txt").exists());
        assert!(!workspace.join("old-parent/file.txt").exists());
    }

    #[cfg(unix)]
    #[test]
    fn parent_cleanup_does_not_follow_a_swapped_parent_symlink() {
        use std::os::unix::fs::symlink;

        let sandbox = tempfile::tempdir().unwrap();
        let workspace = sandbox.path().join("workspace");
        let outside = sandbox.path().join("outside");
        fs::create_dir(&workspace).unwrap();
        fs::create_dir(&outside).unwrap();
        fs::create_dir(outside.join("nested")).unwrap();
        let target = target(
            &workspace,
            "parent/nested/file.txt",
            TargetExpectation::Missing,
        );
        let created = ensure_parent_directories(&target).unwrap();
        fs::rename(workspace.join("parent"), workspace.join("old-parent")).unwrap();
        symlink(&outside, workspace.join("parent")).unwrap();

        let (_, residual) = created.cleanup();
        assert!(!residual.is_empty());
        assert!(outside.join("nested").is_dir());
    }
}
