//! Bounded byte export of an already resolved, authorized package. It neither
//! resolves a skill nor installs it in an external runtime. Capture open file
//! descriptors relative to a held directory, without following package links.
use anyhow::{Context, Result, ensure};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct SkillPackageFile {
    pub relative_path: String,
    pub bytes: Vec<u8>,
}

#[cfg(unix)]
pub fn snapshot_skill_package(
    root: &Path,
    max_files: usize,
    max_bytes: usize,
) -> Result<Vec<SkillPackageFile>> {
    use std::{
        fs::File,
        io::Read,
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::fs::MetadataExt,
        },
    };
    fn walk(
        directory: &File,
        prefix: &str,
        files: &mut Vec<SkillPackageFile>,
        remaining: &mut usize,
        max_files: usize,
        entries_left: &mut usize,
        depth: usize,
    ) -> Result<()> {
        ensure!(depth <= 32, "skill package exceeds directory depth budget");
        #[cfg(target_os = "linux")]
        let fd_path = format!("/proc/self/fd/{}", directory.as_raw_fd());
        #[cfg(not(target_os = "linux"))]
        let fd_path = format!("/dev/fd/{}", directory.as_raw_fd());
        let mut entries = std::fs::read_dir(fd_path)?
            .take(max_files.saturating_add(1))
            .collect::<std::io::Result<Vec<_>>>()?;
        ensure!(
            entries.len() <= max_files,
            "skill package exceeds entry budget"
        );
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            ensure!(*entries_left > 0, "skill package exceeds entry budget");
            *entries_left -= 1;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("skill package path must be UTF-8"))?;
            if matches!(
                name.as_str(),
                ".git" | "node_modules" | "__pycache__" | "__pypackages__" | "metadata.json"
            ) {
                continue;
            }
            let name_c = std::ffi::CString::new(name.as_bytes())?;
            // SAFETY: directory owns a live fd; name is a single directory
            // entry, NUL terminated. O_NOFOLLOW prevents link traversal and
            // O_NONBLOCK prevents a raced FIFO/device from blocking open.
            let fd = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    name_c.as_ptr(),
                    libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
                )
            };
            if fd < 0 {
                return Err(std::io::Error::last_os_error())
                    .context("cannot capture package entry without following links");
            }
            // SAFETY: successful openat returned a new owned descriptor.
            let file = unsafe { File::from_raw_fd(fd) };
            let metadata = file.metadata()?;
            let relative = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            if metadata.is_dir() {
                walk(
                    &file,
                    &relative,
                    files,
                    remaining,
                    max_files,
                    entries_left,
                    depth + 1,
                )?;
            } else {
                ensure!(
                    metadata.is_file() && metadata.nlink() == 1,
                    "skill support entry must be a single-link regular file"
                );
                ensure!(
                    files.len() < max_files && metadata.len() <= *remaining as u64,
                    "skill package exceeds export budget"
                );
                let mut bytes = Vec::with_capacity((metadata.len() as usize).min(*remaining));
                (&file)
                    .take(remaining.saturating_add(1) as u64)
                    .read_to_end(&mut bytes)?;
                ensure!(
                    bytes.len() <= *remaining && bytes.len() as u64 == metadata.len(),
                    "skill package changed or exceeds export budget"
                );
                let after = file.metadata()?;
                ensure!(
                    after.len() == metadata.len()
                        && after.modified()? == metadata.modified()?
                        && after.nlink() == 1,
                    "skill support file changed during capture"
                );
                *remaining -= bytes.len();
                files.push(SkillPackageFile {
                    relative_path: relative,
                    bytes,
                });
            }
        }
        Ok(())
    }
    ensure!(
        root.is_absolute() && max_files > 0 && max_bytes > 0,
        "invalid package export scope or budget"
    );
    // Anchor every component, not only the package's final directory. A
    // renamed ancestor must not redirect this export through a symlink.
    let mut directory = File::open("/")?;
    for component in root.components() {
        let std::path::Component::Normal(name) = component else {
            ensure!(
                matches!(component, std::path::Component::RootDir),
                "package path must be normalized"
            );
            continue;
        };
        use std::os::unix::ffi::OsStrExt;
        let name = std::ffi::CString::new(name.as_bytes())?;
        // SAFETY: held directory and one NUL-terminated normal component.
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        ensure!(
            fd >= 0,
            "package directory cannot be opened without following links"
        );
        // SAFETY: successful openat returned a new owned descriptor.
        directory = unsafe { File::from_raw_fd(fd) };
    }
    let root = directory;
    let mut files = Vec::new();
    walk(
        &root,
        "",
        &mut files,
        &mut { max_bytes },
        max_files,
        &mut { max_files.saturating_mul(2) },
        0,
    )?;
    Ok(files)
}

#[cfg(not(unix))]
pub fn snapshot_skill_package(
    _root: &Path,
    _max_files: usize,
    _max_bytes: usize,
) -> Result<Vec<SkillPackageFile>> {
    anyhow::bail!("secure skill package byte export is unavailable on this platform")
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    #[test]
    fn package_export_rejects_links_and_enforces_total_bytes() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        std::fs::write(root.join("SKILL.md"), "body").unwrap();
        assert!(snapshot_skill_package(&root, 8, 3).is_err());
        assert_eq!(
            snapshot_skill_package(&root, 8, 4).unwrap()[0].bytes,
            b"body"
        );
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("private"), "secret").unwrap();
        std::os::unix::fs::symlink(outside.path().join("private"), root.join("linked")).unwrap();
        assert!(snapshot_skill_package(&root, 8, 1024).is_err());
    }
}
