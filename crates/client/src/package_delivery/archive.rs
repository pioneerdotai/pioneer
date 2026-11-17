use anyhow::{Context, Result, anyhow, bail};
use flate2::{Compression, GzBuilder};
use sha2::{Digest, Sha256};
use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use tar::{Builder, Header};

#[derive(Debug, Clone)]
pub struct SkillUploadArchive {
    pub file_name: String,
    pub bytes: Vec<u8>,
    pub sha256: String,
    pub uncompressed_size_bytes: u64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SkillUploadSourceKind {
    App,
    Plugin,
    Skill,
    Pack,
}
/// App directories are captured from declarations only. Ready archives retain
/// the existing upload limit and are independently checked at the Gateway.
pub fn build_app_upload_archive(
    source: &Path,
    cancelled: impl Fn() -> bool,
) -> Result<SkillUploadArchive> {
    let metadata = fs::symlink_metadata(source)?;
    anyhow::ensure!(
        !metadata.file_type().is_symlink(),
        "selected_symlink_denied"
    );
    if metadata.is_file() {
        use std::io::Read;
        const MAX_BYTES: usize = pioneer_apps::MAX_UPLOAD_ARCHIVE_BYTES;
        anyhow::ensure!(metadata.len() <= MAX_BYTES as u64, "archive_input_limit");
        let mut options = fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options.open(source)?;
        let opened = file.metadata()?;
        anyhow::ensure!(
            opened.is_file()
                && opened.len() == metadata.len()
                && opened.modified()? == metadata.modified()?,
            "source_changed"
        );
        let mut bytes = Vec::new();
        let mut reader = (&file).take((MAX_BYTES + 1) as u64);
        let mut chunk = [0u8; 65536];
        loop {
            anyhow::ensure!(!cancelled(), "cancelled");
            let n = reader.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..n]);
            anyhow::ensure!(bytes.len() <= MAX_BYTES, "archive_input_limit");
        }
        let after = file.metadata()?;
        let path_after = fs::symlink_metadata(source)?;
        anyhow::ensure!(
            after.len() == opened.len()
                && after.modified()? == opened.modified()?
                && !path_after.file_type().is_symlink()
                && path_after.len() == after.len()
                && path_after.modified()? == after.modified()?,
            "source_changed"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            anyhow::ensure!(
                opened.dev() == metadata.dev()
                    && opened.ino() == metadata.ino()
                    && path_after.dev() == opened.dev()
                    && path_after.ino() == opened.ino()
                    && after.ctime() == opened.ctime()
                    && after.ctime_nsec() == opened.ctime_nsec(),
                "source_changed"
            );
        }
        return Ok(SkillUploadArchive {
            file_name: source
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("app.tar.gz")
                .into(),
            sha256: hex::encode(Sha256::digest(&bytes)),
            bytes,
            uncompressed_size_bytes: 0,
        });
    }
    anyhow::ensure!(metadata.is_dir(), "source_kind");
    let (_, snapshot) =
        pioneer_apps::SelectionPlan::capture(source, pioneer_apps::PACKAGE_LIMITS, &cancelled)?;
    build_app_snapshot_archive(&snapshot, cancelled)
}

#[derive(Debug)]
struct ArchiveEntry {
    relative_path: String,
    source_path: PathBuf,
    is_dir: bool,
    size_bytes: u64,
    mode: u32,
}

/// Use the existing archive builder for folders. Ready archives are transported
/// unchanged; the Gateway owns archive and package validation.
pub fn build_plugin_upload_archive(source: &Path) -> Result<SkillUploadArchive> {
    if source.is_file() {
        let bytes = fs::read(source)?;
        return Ok(SkillUploadArchive {
            file_name: source
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("plugin.tar.gz")
                .into(),
            sha256: hex::encode(Sha256::digest(&bytes)),
            bytes,
            uncompressed_size_bytes: 0,
        });
    }
    let root = fs::canonicalize(source).context("source_unavailable")?;
    require_directory(&root, "selected plugin source")?;
    build_upload_archive(&root, "plugin".into())
}

pub fn build_skill_upload_archive(source_path: &Path) -> Result<SkillUploadArchive> {
    let source_root = fs::canonicalize(source_path)
        .with_context(|| format!("failed to resolve `{}`", source_path.display()))?;
    require_directory(source_root.as_path(), "selected skill source")?;
    if !source_root.join("SKILL.md").is_file() {
        bail!("selected skill directory is missing root SKILL.md");
    }

    let root_name = archive_root_name(source_root.as_path())?;
    build_upload_archive(source_root.as_path(), root_name)
}

pub fn build_skill_pack_upload_archive(source_path: &Path) -> Result<SkillUploadArchive> {
    let source_root = fs::canonicalize(source_path)
        .with_context(|| format!("failed to resolve `{}`", source_path.display()))?;
    require_directory(source_root.as_path(), "selected skill pack source")?;
    validate_skill_pack_shape(source_root.as_path())?;

    let root_name = archive_root_name_strict(source_root.as_path())?;
    build_upload_archive(source_root.as_path(), root_name)
}

pub fn classify_skill_upload_source(source_path: &Path) -> Result<SkillUploadSourceKind> {
    let source_root = fs::canonicalize(source_path)
        .with_context(|| format!("failed to resolve `{}`", source_path.display()))?;
    require_directory(source_root.as_path(), "selected skill source")?;

    if source_root.join("SKILL.md").is_file() {
        return Ok(SkillUploadSourceKind::Skill);
    }

    validate_skill_pack_shape(source_root.as_path())?;
    Ok(SkillUploadSourceKind::Pack)
}

fn require_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to read metadata for `{}`", path.display()))?;
    if !metadata.file_type().is_dir() {
        bail!("{label} must be a directory");
    }
    Ok(())
}

fn validate_skill_pack_shape(source_root: &Path) -> Result<()> {
    if source_root.join("SKILL.md").exists() {
        bail!("selected skill pack root must not contain SKILL.md");
    }

    let mut children = fs::read_dir(source_root)
        .with_context(|| format!("failed to read directory `{}`", source_root.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| {
            format!(
                "failed to read directory entry in `{}`",
                source_root.display()
            )
        })?;
    children.sort_by_key(|entry| entry.file_name());

    let mut member_count = 0usize;
    for child in children {
        let member_name = skill_pack_member_name(child.file_name())?;
        let path = child.path();
        let metadata = fs::symlink_metadata(path.as_path())
            .with_context(|| format!("failed to read metadata for `{}`", path.display()))?;
        let file_type = metadata.file_type();
        if file_type.is_symlink() {
            bail!("skill pack root cannot include symlink `{member_name}`");
        }
        if !file_type.is_dir() {
            bail!("skill pack root cannot include regular file `{member_name}`");
        }
        if !path.join("SKILL.md").is_file() {
            bail!("skill pack member `{member_name}` is missing direct SKILL.md");
        }
        member_count += 1;
    }

    if member_count == 0 {
        bail!("selected skill pack must contain at least one skill directory");
    }

    Ok(())
}

fn skill_pack_member_name(file_name: OsString) -> Result<String> {
    file_name
        .into_string()
        .map_err(|_| anyhow!("skill pack member name is not valid UTF-8"))
}

fn build_upload_archive(source_root: &Path, root_name: String) -> Result<SkillUploadArchive> {
    let mut entries = Vec::new();
    collect_archive_entries(source_root, source_root, &mut entries)?;
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

/// Archive immutable, selectively captured App bytes, never reread the source project.
/// The delivery controller retains these bytes through preview/install using an
/// App-purpose upload; Gateway independently validates the extracted selection.
pub fn build_app_snapshot_archive(
    snapshot: &pioneer_packages::Snapshot,
    cancelled: impl Fn() -> bool,
) -> Result<SkillUploadArchive> {
    use pioneer_apps::SelectionPlan;
    use pioneer_packages::Entry;
    let manifest = SelectionPlan::validate_package(snapshot)?;
    let encoder = GzBuilder::new()
        .mtime(0)
        .write(Vec::new(), Compression::default());
    let mut builder = Builder::new(encoder);
    let root = manifest.slug();
    let mut root_header = Header::new_gnu();
    root_header.set_entry_type(tar::EntryType::Directory);
    root_header.set_mode(snapshot.root_mode());
    root_header.set_size(0);
    root_header.set_mtime(0);
    root_header.set_cksum();
    builder.append_data(&mut root_header, root, io::empty())?;
    let mut total = 0u64;
    struct CancellableBytes<'a, F> {
        bytes: &'a [u8],
        cancelled: &'a F,
    }
    impl<F: Fn() -> bool> io::Read for CancellableBytes<'_, F> {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if (self.cancelled)() {
                return Err(io::Error::other("cancelled"));
            }
            let count = buffer.len().min(self.bytes.len()).min(64 * 1024);
            buffer[..count].copy_from_slice(&self.bytes[..count]);
            self.bytes = &self.bytes[count..];
            Ok(count)
        }
    }
    for (path, entry) in snapshot.entries() {
        if cancelled() {
            bail!("cancelled");
        }
        let mut header = Header::new_gnu();
        let bytes = match entry {
            Entry::Directory { mode } => {
                header.set_entry_type(tar::EntryType::Directory);
                header.set_mode(*mode);
                &[][..]
            }
            Entry::File { bytes, mode } => {
                header.set_entry_type(tar::EntryType::Regular);
                header.set_mode(*mode);
                total = total
                    .checked_add(bytes.len() as u64)
                    .context("expanded_limit")?;
                bytes.as_slice()
            }
            Entry::Denied => bail!("denied_entry"),
        };
        header.set_size(bytes.len() as u64);
        header.set_mtime(0);
        header.set_cksum();
        builder.append_data(
            &mut header,
            format!("{root}/{path}"),
            CancellableBytes {
                bytes,
                cancelled: &cancelled,
            },
        )?;
    }
    if cancelled() {
        bail!("cancelled");
    }
    let bytes = builder.into_inner()?.finish()?;
    anyhow::ensure!(
        bytes.len() <= pioneer_apps::MAX_UPLOAD_ARCHIVE_BYTES,
        "archive_input_limit"
    );
    Ok(SkillUploadArchive {
        file_name: format!("{root}.tar.gz"),
        sha256: hex::encode(Sha256::digest(&bytes)),
        bytes,
        uncompressed_size_bytes: total,
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

fn archive_root_name_strict(source_root: &Path) -> Result<String> {
    source_root
        .file_name()
        .and_then(|value| value.to_str())
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("skill pack root name is not valid UTF-8"))
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

#[cfg(any(unix, windows))]
fn reject_hardlinked_file(path: &Path, metadata: &fs::Metadata, relative_path: &str) -> Result<()> {
    if pioneer_skills::file_link_count(path, metadata)? > 1 {
        bail!(
            "skill archive cannot include hardlinked file `{}` at `{}`",
            relative_path,
            path.display()
        );
    }
    Ok(())
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
    fn app_folder_upload_accepts_a_prebuilt_library_larger_than_ten_mib() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join("runtime")).unwrap();
        let document = serde_json::json!({
            "slug": "backend-app", "name": "Backend", "version": "1.0.0",
            "shortDescription": "Prebuilt backend", "developer": {"name": "Author"},
            "icon": {"light": "icon.png", "dark": "icon.png"},
            "runtime": {
                "environment": "node", "cwd": "runtime", "command": ["node", "start.js"],
                "port": 3000, "readiness": {"path": "/healthz"}
            }
        });
        fs::write(root.path().join("app.json"), document.to_string()).unwrap();
        fs::write(root.path().join("icon.png"), hex::decode(
            "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4890000000b49444154789c636000020000050001a5f645400000000049454e44ae426082"
        ).unwrap()).unwrap();
        fs::write(
            root.path().join("runtime/start.js"),
            b"// Prebuilt entry point\n",
        )
        .unwrap();
        let library_bytes = 18 * 1024 * 1024;
        fs::File::create(root.path().join("runtime/library.so"))
            .unwrap()
            .set_len(library_bytes)
            .unwrap();

        let upload = build_app_upload_archive(root.path(), || false).unwrap();
        let mut archive = Archive::new(GzDecoder::new(upload.bytes.as_slice()));
        let mut library_found = false;
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            if entry.path().unwrap().as_ref() == Path::new("backend-app/runtime/library.so") {
                assert_eq!(
                    io::copy(&mut entry, &mut io::sink()).unwrap(),
                    library_bytes
                );
                library_found = true;
            }
        }
        assert!(library_found);
    }

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

    #[test]
    fn classify_skill_upload_source_distinguishes_valid_roots_and_rejects_mixed_root() {
        let skill = temp_case("classified-skill");
        write_file(skill.join("SKILL.md").as_path(), "# Skill\n");

        let pack = temp_case("classified-pack");
        write_file(pack.join("browser/SKILL.md").as_path(), "# Browser\n");
        write_file(pack.join("reviewer/SKILL.md").as_path(), "# Reviewer\n");

        let mixed = temp_case("classified-mixed");
        write_file(mixed.join("browser/SKILL.md").as_path(), "# Browser\n");
        write_file(mixed.join("README.md").as_path(), "not a pack\n");

        assert_eq!(
            classify_skill_upload_source(skill.as_path()).expect("classify skill"),
            SkillUploadSourceKind::Skill
        );
        assert_eq!(
            classify_skill_upload_source(pack.as_path()).expect("classify pack"),
            SkillUploadSourceKind::Pack
        );
        let error = classify_skill_upload_source(mixed.as_path())
            .expect_err("mixed root should fail")
            .to_string();
        assert!(error.contains("regular file `README.md`"));

        let _ = fs::remove_dir_all(skill);
        let _ = fs::remove_dir_all(pack);
        let _ = fs::remove_dir_all(mixed);
    }

    #[test]
    fn build_skill_pack_upload_archive_preserves_one_root_and_sorted_members() {
        let root = temp_case("Research Pack (v1)");
        write_file(root.join("reviewer/SKILL.md").as_path(), "# Reviewer\n");
        write_file(
            root.join("browser/references/search.md").as_path(),
            "search\n",
        );
        write_file(root.join("browser/SKILL.md").as_path(), "# Browser\n");

        let first = build_skill_pack_upload_archive(root.as_path()).expect("build pack archive");
        let second = build_skill_pack_upload_archive(root.as_path()).expect("rebuild pack archive");
        let root_name = root
            .file_name()
            .and_then(|name| name.to_str())
            .expect("UTF-8 pack root name")
            .to_owned();

        assert_eq!(
            archive_root_name_strict(root.as_path()).expect("strict archive root name"),
            root_name
        );

        assert_eq!(first.bytes, second.bytes);
        assert_eq!(first.sha256, second.sha256);
        assert_eq!(
            archive_paths_in_order(first.bytes.as_slice()),
            vec![
                root_name.clone(),
                format!("{root_name}/browser"),
                format!("{root_name}/browser/SKILL.md"),
                format!("{root_name}/browser/references"),
                format!("{root_name}/browser/references/search.md"),
                format!("{root_name}/reviewer"),
                format!("{root_name}/reviewer/SKILL.md"),
            ]
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn build_skill_pack_upload_archive_rejects_empty_or_mixed_root() {
        let empty = temp_case("empty-pack");
        let error = build_skill_pack_upload_archive(empty.as_path())
            .expect_err("empty pack should fail")
            .to_string();
        assert!(error.contains("at least one skill directory"));

        let root_skill = temp_case("pack-root-skill-md");
        write_file(root_skill.join("SKILL.md").as_path(), "# Not a pack\n");
        let error = build_skill_pack_upload_archive(root_skill.as_path())
            .expect_err("root SKILL.md should fail")
            .to_string();
        assert!(error.contains("must not contain SKILL.md"));

        let mixed = temp_case("mixed-pack");
        write_file(mixed.join("browser/SKILL.md").as_path(), "# Browser\n");
        write_file(mixed.join("README.md").as_path(), "read me\n");
        let error = build_skill_pack_upload_archive(mixed.as_path())
            .expect_err("root regular file should fail")
            .to_string();
        assert!(error.contains("regular file `README.md`"));

        let _ = fs::remove_dir_all(empty);
        let _ = fs::remove_dir_all(root_skill);
        let _ = fs::remove_dir_all(mixed);
    }

    #[test]
    fn build_skill_pack_upload_archive_rejects_member_without_direct_skill_md() {
        let root = temp_case("missing-member-skill-md");
        write_file(
            root.join("browser/nested/SKILL.md").as_path(),
            "# Too deep\n",
        );

        let error = build_skill_pack_upload_archive(root.as_path())
            .expect_err("member without direct SKILL.md should fail")
            .to_string();
        assert!(error.contains("member `browser`"));
        assert!(error.contains("direct SKILL.md"));

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn build_skill_pack_upload_archive_rejects_non_utf8_member_name() {
        use std::os::unix::ffi::OsStringExt;

        let error = skill_pack_member_name(OsString::from_vec(vec![b'm', 0x80]))
            .expect_err("non-UTF-8 member should fail")
            .to_string();
        assert!(error.contains("member name is not valid UTF-8"));
    }

    #[cfg(unix)]
    #[test]
    fn build_skill_pack_upload_archive_rejects_non_utf8_root_name() {
        use std::os::unix::ffi::OsStringExt;

        let root = PathBuf::from(OsString::from_vec(vec![b'p', 0x80]));

        let error = archive_root_name_strict(root.as_path())
            .expect_err("non-UTF-8 root should fail")
            .to_string();
        assert!(error.contains("root name is not valid UTF-8"));
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

    fn archive_paths_in_order(bytes: &[u8]) -> Vec<String> {
        let decoder = GzDecoder::new(bytes);
        let mut archive = Archive::new(decoder);
        archive
            .entries()
            .expect("read archive")
            .map(|entry| {
                entry
                    .expect("read entry")
                    .path()
                    .expect("entry path")
                    .to_string_lossy()
                    .trim_end_matches('/')
                    .to_owned()
            })
            .collect()
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
