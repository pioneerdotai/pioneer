use anyhow::{Context, Result, anyhow, bail};
use flate2::{Compression, GzBuilder};
use sha2::{Digest, Sha256};
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use tar::{Builder, Header};

#[derive(Debug, Clone)]
pub(super) struct SkillUploadArchive {
    pub file_name: String,
    pub bytes: Vec<u8>,
    pub sha256: String,
    pub uncompressed_size_bytes: u64,
}

#[derive(Debug)]
struct ArchiveEntry {
    relative_path: String,
    source_path: PathBuf,
    is_dir: bool,
    size_bytes: u64,
    mode: u32,
}

pub(super) fn build_skill_upload_archive(source_path: &Path) -> Result<SkillUploadArchive> {
    let source_root = fs::canonicalize(source_path)
        .with_context(|| format!("failed to resolve `{}`", source_path.display()))?;
    let root_metadata = fs::symlink_metadata(source_root.as_path())
        .with_context(|| format!("failed to read metadata for `{}`", source_root.display()))?;
    if !root_metadata.file_type().is_dir() {
        bail!("selected skill source must be a directory");
    }
    if !source_root.join("SKILL.md").is_file() {
        bail!("selected skill directory is missing root SKILL.md");
    }

    let root_name = archive_root_name(source_root.as_path())?;
    let mut entries = Vec::new();
    collect_archive_entries(source_root.as_path(), source_root.as_path(), &mut entries)?;
    entries.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));

    let uncompressed_size_bytes =
        entries
            .iter()
            .filter(|entry| !entry.is_dir)
            .try_fold(0u64, |total, entry| {
                total
                    .checked_add(entry.size_bytes)
                    .ok_or_else(|| anyhow!("skill archive uncompressed size overflow"))
            })?;

    let encoder = GzBuilder::new()
        .mtime(0)
        .write(Vec::new(), Compression::default());
    let mut builder = Builder::new(encoder);

    append_directory(&mut builder, root_name.as_str())?;
    for entry in entries {
        let archive_path = format!("{}/{}", root_name, entry.relative_path);
        if entry.is_dir {
            append_directory(&mut builder, archive_path.as_str())?;
        } else {
            append_file(&mut builder, archive_path.as_str(), &entry)?;
        }
    }

    let encoder = builder
        .into_inner()
        .context("failed to finalize skill tar archive")?;
    let bytes = encoder
        .finish()
        .context("failed to finalize skill gzip archive")?;
    let sha256 = hex::encode(Sha256::digest(bytes.as_slice()));

    Ok(SkillUploadArchive {
        file_name: format!("{root_name}.tar.gz"),
        bytes,
        sha256,
        uncompressed_size_bytes,
    })
}

fn collect_archive_entries(
    root: &Path,
    current: &Path,
    entries: &mut Vec<ArchiveEntry>,
) -> Result<()> {
    let mut children = fs::read_dir(current)
        .with_context(|| format!("failed to read directory `{}`", current.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("failed to read directory entry in `{}`", current.display()))?;
    children.sort_by_key(|entry| entry.file_name());

    for child in children {
        let path = child.path();
        let metadata = fs::symlink_metadata(path.as_path())
            .with_context(|| format!("failed to read metadata for `{}`", path.display()))?;
        let file_type = metadata.file_type();
        let relative_path = normalize_relative_path(
            path.strip_prefix(root)
                .with_context(|| format!("failed to relativize `{}`", path.display()))?,
        )?;

        if file_type.is_symlink() {
            bail!("skill archive cannot include symlink `{relative_path}`");
        }
        if file_type.is_dir() {
            entries.push(ArchiveEntry {
                relative_path: relative_path.clone(),
                source_path: path.clone(),
                is_dir: true,
                size_bytes: 0,
                mode: 0o755,
            });
            collect_archive_entries(root, path.as_path(), entries)?;
            continue;
        }
        if !file_type.is_file() {
            bail!("skill archive cannot include special file `{relative_path}`");
        }
        reject_hardlinked_file(path.as_path(), &metadata, relative_path.as_str())?;

        entries.push(ArchiveEntry {
            relative_path,
            source_path: path,
            is_dir: false,
            size_bytes: metadata.len(),
            mode: normalized_file_mode(&metadata),
        });
    }

    Ok(())
}

fn archive_root_name(source_root: &Path) -> Result<String> {
    let name = source_root
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("skill");
    let sanitized = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches(['.', '-'])
        .to_owned();
    if sanitized.is_empty() {
        Ok("skill".to_owned())
    } else {
        Ok(sanitized)
    }
}

fn normalize_relative_path(path: &Path) -> Result<String> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => {
                let Some(part) = value.to_str() else {
                    bail!("skill archive path contains non-UTF-8 component");
                };
                parts.push(part.to_owned());
            }
            Component::CurDir => {}
            Component::ParentDir => bail!("skill archive path contains parent traversal"),
            Component::RootDir | Component::Prefix(_) => bail!("skill archive path is absolute"),
        }
    }
    if parts.is_empty() {
        bail!("skill archive entry has empty relative path");
    }
    Ok(parts.join("/"))
}

#[cfg(unix)]
fn reject_hardlinked_file(path: &Path, metadata: &fs::Metadata, relative_path: &str) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    if metadata.nlink() > 1 {
        bail!(
            "skill archive cannot include hardlinked file `{}` at `{}`",
            relative_path,
            path.display()
        );
    }
    Ok(())
}

#[cfg(windows)]
fn reject_hardlinked_file(
    path: &Path,
    _metadata: &fs::Metadata,
    relative_path: &str,
) -> Result<()> {
    if windows_file_link_count(path)? > 1 {
        bail!(
            "skill archive cannot include hardlinked file `{}` at `{}`",
            relative_path,
            path.display()
        );
    }
    Ok(())
}

#[cfg(windows)]
fn windows_file_link_count(path: &Path) -> Result<u32> {
    use std::mem::MaybeUninit;
    use std::os::windows::ffi::OsStrExt;
    use std::ptr;
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, GetFileInformationByHandle, OPEN_EXISTING,
    };

    let wide_path = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
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
        return Err(io::Error::last_os_error())
            .with_context(|| format!("failed to open `{}` for hardlink check", path.display()));
    }

    let mut info = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    let result = unsafe { GetFileInformationByHandle(handle, info.as_mut_ptr()) };
    let result_error = if result == 0 {
        Some(io::Error::last_os_error())
    } else {
        None
    };
    let close_result = unsafe { CloseHandle(handle) };
    if let Some(error) = result_error {
        return Err(error).with_context(|| {
            format!(
                "failed to read file information for hardlink check `{}`",
                path.display()
            )
        });
    }
    if close_result == 0 {
        return Err(io::Error::last_os_error())
            .with_context(|| format!("failed to close `{}` after hardlink check", path.display()));
    }

    Ok(unsafe { info.assume_init() }.nNumberOfLinks)
}

#[cfg(not(unix))]
#[cfg(not(windows))]
fn reject_hardlinked_file(
    _path: &Path,
    _metadata: &fs::Metadata,
    _relative_path: &str,
) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn normalized_file_mode(metadata: &fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o111 != 0 {
        0o755
    } else {
        0o644
    }
}

#[cfg(not(unix))]
fn normalized_file_mode(_metadata: &fs::Metadata) -> u32 {
    0o644
}

fn append_directory<W: io::Write>(builder: &mut Builder<W>, archive_path: &str) -> Result<()> {
    let mut header = Header::new_gnu();
    header.set_entry_type(tar::EntryType::Directory);
    header.set_size(0);
    header.set_mode(0o755);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_cksum();
    builder
        .append_data(&mut header, Path::new(archive_path), io::empty())
        .with_context(|| format!("failed to append archive directory `{archive_path}`"))?;
    Ok(())
}

fn append_file<W: io::Write>(
    builder: &mut Builder<W>,
    archive_path: &str,
    entry: &ArchiveEntry,
) -> Result<()> {
    let mut file = fs::File::open(entry.source_path.as_path())
        .with_context(|| format!("failed to open `{}`", entry.source_path.display()))?;
    let mut header = Header::new_gnu();
    header.set_entry_type(tar::EntryType::Regular);
    header.set_size(entry.size_bytes);
    header.set_mode(entry.mode);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_cksum();
    builder
        .append_data(&mut header, Path::new(archive_path), &mut file)
        .with_context(|| format!("failed to append archive file `{archive_path}`"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tar::Archive;

    #[test]
    fn build_skill_upload_archive_is_stable_for_unchanged_input() {
        let root = temp_case("stable");
        write_file(root.join("SKILL.md").as_path(), "# Stable\n");
        fs::create_dir_all(root.join(".hidden")).expect("create hidden dir");
        write_file(root.join(".hidden/config.json").as_path(), "{}\n");
        write_file(root.join("scripts/run.sh").as_path(), "#!/bin/sh\n");

        let first = build_skill_upload_archive(root.as_path()).expect("build archive");
        let second = build_skill_upload_archive(root.as_path()).expect("rebuild archive");

        assert_eq!(first.bytes, second.bytes);
        assert_eq!(first.sha256, second.sha256);
        assert!(archive_paths(first.bytes.as_slice()).contains(&format!(
            "{}/.hidden/config.json",
            root.file_name().unwrap().to_string_lossy()
        )));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn build_skill_upload_archive_rejects_missing_skill_md() {
        let root = temp_case("missing-skill-md");
        write_file(root.join("README.md").as_path(), "no skill\n");

        let error = build_skill_upload_archive(root.as_path())
            .expect_err("missing SKILL.md should fail")
            .to_string();
        assert!(error.contains("SKILL.md"));

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn build_skill_upload_archive_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let root = temp_case("symlink");
        write_file(root.join("SKILL.md").as_path(), "# Symlink\n");
        symlink("SKILL.md", root.join("linked.md")).expect("create symlink");

        let error = build_skill_upload_archive(root.as_path())
            .expect_err("symlink should fail")
            .to_string();
        assert!(error.contains("symlink"));

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn build_skill_upload_archive_rejects_hardlinks() {
        let root = temp_case("hardlink");
        write_file(root.join("SKILL.md").as_path(), "# Hardlink\n");
        write_file(root.join("original.txt").as_path(), "linked\n");
        fs::hard_link(root.join("original.txt"), root.join("linked.txt")).expect("create hardlink");

        let error = build_skill_upload_archive(root.as_path())
            .expect_err("hardlink should fail")
            .to_string();
        assert!(error.contains("hardlinked"));

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn build_skill_upload_archive_preserves_executable_bit() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_case("exec");
        write_file(root.join("SKILL.md").as_path(), "# Exec\n");
        write_file(root.join("run.sh").as_path(), "#!/bin/sh\n");
        fs::set_permissions(root.join("run.sh"), fs::Permissions::from_mode(0o755))
            .expect("chmod executable");

        let archive = build_skill_upload_archive(root.as_path()).expect("build archive");
        let modes = archive_modes(archive.bytes.as_slice());
        let script_path = format!("{}/run.sh", root.file_name().unwrap().to_string_lossy());
        assert_eq!(modes.get(script_path.as_str()).copied(), Some(0o755));

        let _ = fs::remove_dir_all(root);
    }

    fn archive_paths(bytes: &[u8]) -> std::collections::HashSet<String> {
        archive_modes(bytes).into_keys().collect()
    }

    fn archive_modes(bytes: &[u8]) -> std::collections::HashMap<String, u32> {
        let decoder = GzDecoder::new(bytes);
        let mut archive = Archive::new(decoder);
        let mut modes = std::collections::HashMap::new();
        for entry in archive.entries().expect("read archive") {
            let entry = entry.expect("read entry");
            let path = entry
                .path()
                .expect("entry path")
                .to_string_lossy()
                .to_string();
            modes.insert(path, entry.header().mode().expect("entry mode"));
        }
        modes
    }

    fn write_file(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create parent");
        }
        fs::write(path, contents).expect("write file");
    }

    fn temp_case(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pioneer-skill-archive-{name}-{nanos}"));
        fs::create_dir_all(path.as_path()).expect("create temp dir");
        path
    }
}
