use std::{
    env, fs,
    io::ErrorKind,
    path::{Component, Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use icu_collator::Collator;
use sha2::{Digest, Sha256};

pub fn sanitize_name(name: &str) -> String {
    let mut sanitized = String::new();
    let mut pending_hyphen = false;
    for character in name.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || matches!(character, '.' | '_')
        {
            if pending_hyphen && !sanitized.is_empty() {
                sanitized.push('-');
            }
            pending_hyphen = false;
            sanitized.push(character);
        } else {
            pending_hyphen = true;
        }
    }
    let trimmed = sanitized.trim_matches(['.', '-']);
    let truncated = &trimmed[..trimmed.len().min(255)];
    if truncated.is_empty() {
        "unnamed-skill".to_owned()
    } else {
        truncated.to_owned()
    }
}

pub(crate) fn normalize_external_runtime_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .context("failed to resolve current directory for external skill path")?
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::Normal(part) => normalized.push(part),
        }
    }
    Ok(normalized)
}

fn path_is_contained(base: &Path, target: &Path) -> Result<bool> {
    let base = normalize_external_runtime_path(base)?;
    let target = normalize_external_runtime_path(target)?;
    Ok(target == base || target.starts_with(base))
}

fn contained_destination(base: &Path, sanitized_name: &str) -> Result<PathBuf> {
    let base = normalize_external_runtime_path(base)?;
    let destination = normalize_external_runtime_path(&base.join(sanitized_name))?;
    if !path_is_contained(&base, &destination)? {
        bail!(
            "external skill destination `{}` escapes native skills root `{}`",
            destination.display(),
            base.display()
        );
    }
    Ok(destination)
}

fn paths_overlap(left: &Path, right: &Path) -> Result<bool> {
    Ok(path_is_contained(left, right)? || path_is_contained(right, left)?)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExternalRuntimeCopyResult {
    Changed,
    SkippedOverlap,
}

pub fn replace_external_runtime_skill(
    source: &Path,
    destination: &Path,
) -> Result<ExternalRuntimeCopyResult> {
    let destination = normalize_external_runtime_path(destination)?;
    let parent = destination
        .parent()
        .context("external skill destination must have a parent")?;
    let name = destination
        .file_name()
        .and_then(|v| v.to_str())
        .context("external skill destination must be UTF-8")?;
    let destination = contained_destination(parent, name)?;
    if paths_overlap(source, &destination)? {
        return Ok(ExternalRuntimeCopyResult::SkippedOverlap);
    }
    let _ = remove_any(&destination);
    fs::create_dir_all(&destination).with_context(|| {
        format!(
            "failed to create external skill destination `{}`",
            destination.display()
        )
    })?;
    copy_directory_filtered(source, &destination)?;
    Ok(ExternalRuntimeCopyResult::Changed)
}

fn remove_any(path: &Path) -> std::io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(m) if m.is_dir() && !m.file_type().is_symlink() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn copy_directory_filtered(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)?;
    for entry in
        fs::read_dir(source).with_context(|| format!("failed to read `{}`", source.display()))?
    {
        let entry = entry?;
        let ty = entry.file_type()?;
        let name = entry.file_name();
        let text = name.to_string_lossy();
        if text == "metadata.json"
            || (ty.is_dir() && matches!(text.as_ref(), ".git" | "__pycache__" | "__pypackages__"))
        {
            continue;
        }
        let src = entry.path();
        let dst = destination.join(name);
        if ty.is_dir() {
            copy_directory_filtered(&src, &dst)?;
        } else if let Err(error) = copy_dereferenced_entry(&src, &dst) {
            if ty.is_symlink()
                && error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == ErrorKind::NotFound)
            {
                continue;
            }
            return Err(error).with_context(|| {
                format!("failed to copy `{}` to `{}`", src.display(), dst.display())
            });
        }
    }
    Ok(())
}

fn copy_dereferenced_entry(source: &Path, destination: &Path) -> Result<()> {
    let metadata = fs::metadata(source)?;
    if metadata.is_dir() {
        copy_directory_unfiltered(source, destination)?;
        set_permissions(destination, &metadata)?;
    } else if metadata.is_file() {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(source, destination)?;
        set_permissions(destination, &metadata)?;
    }
    Ok(())
}

fn copy_directory_unfiltered(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        copy_dereferenced_entry(&entry.path(), &destination.join(entry.file_name()))?;
    }
    Ok(())
}

#[cfg(unix)]
fn set_permissions(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(
        path,
        fs::Permissions::from_mode(metadata.permissions().mode() & 0o777),
    )?;
    Ok(())
}
#[cfg(not(unix))]
fn set_permissions(_: &Path, _: &fs::Metadata) -> Result<()> {
    Ok(())
}

pub fn compute_skill_folder_hash(skill_directory: &Path) -> Result<String> {
    let mut files = Vec::new();
    collect_hash_files(skill_directory, skill_directory, &mut files)?;
    let collator = Collator::try_new(Default::default(), Default::default())
        .context("failed to initialize skill hash collation")?;
    files.sort_by(|left, right| collator.compare(&left.0, &right.0));
    let mut hash = Sha256::new();
    for (path, bytes) in files {
        hash.update(path.as_bytes());
        hash.update(bytes);
    }
    Ok(hex::encode(hash.finalize()))
}

fn collect_hash_files(
    base: &Path,
    current: &Path,
    files: &mut Vec<(String, Vec<u8>)>,
) -> Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            if matches!(entry.file_name().to_str(), Some(".git" | "node_modules")) {
                continue;
            }
            collect_hash_files(base, &entry.path(), files)?;
        } else if ty.is_file() {
            let path = entry.path();
            let relative = path
                .strip_prefix(base)?
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            files.push((relative, fs::read(path)?));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn external_runtime_installer_sanitize_and_paths_match_upstream() {
        assert_eq!(sanitize_name("My Skill"), "my-skill");
        assert_eq!(sanitize_name("../../secret"), "secret");
        assert_eq!(sanitize_name("日本語"), "unnamed-skill");
        assert_eq!(sanitize_name(&"A".repeat(300)), "a".repeat(255));
        let base = Path::new("/tmp/native-skills");
        assert!(path_is_contained(base, Path::new("/tmp/native-skills/one")).unwrap());
        assert!(!path_is_contained(base, Path::new("/tmp/native-skills-other")).unwrap());
        assert!(paths_overlap(base, Path::new("/tmp/native-skills/one")).unwrap());
        assert!(paths_overlap(Path::new("/tmp/native-skills/one"), base).unwrap());
        assert_eq!(
            contained_destination(base, "one").unwrap(),
            PathBuf::from("/tmp/native-skills/one")
        );
    }

    #[test]
    fn external_runtime_installer_replaces_stale_tree_and_raw_skill() {
        let root = tempfile::tempdir().unwrap();
        let src = root.path().join("source");
        let dst = root.path().join("dest");
        fs::create_dir_all(&src).unwrap();
        fs::create_dir_all(&dst).unwrap();
        fs::write(src.join("SKILL.md"), b"---\nextra: true\n---\nraw\n").unwrap();
        fs::write(src.join("metadata.json"), b"excluded").unwrap();
        fs::write(dst.join("stale"), b"stale").unwrap();
        assert_eq!(
            replace_external_runtime_skill(&src, &dst).unwrap(),
            ExternalRuntimeCopyResult::Changed
        );
        assert!(!dst.join("stale").exists());
        assert!(!dst.join("metadata.json").exists());
        assert_eq!(
            fs::read(src.join("SKILL.md")).unwrap(),
            fs::read(dst.join("SKILL.md")).unwrap()
        );
        assert_eq!(
            replace_external_runtime_skill(&src, &src).unwrap(),
            ExternalRuntimeCopyResult::SkippedOverlap
        );
    }

    #[test]
    fn external_runtime_installer_swallows_remove_error_before_mkdir_decides() {
        let root = tempfile::tempdir().unwrap();
        let src = root.path().join("source");
        let blocked_parent = root.path().join("blocked-parent");
        let dst = blocked_parent.join("skill");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("SKILL.md"), b"skill").unwrap();
        fs::write(&blocked_parent, b"not a directory").unwrap();

        let error = replace_external_runtime_skill(&src, &dst).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("failed to create external skill destination"),
            "the mkdir/copy step, not the swallowed remove attempt, must determine the error: {error:#}"
        );
    }

    #[test]
    fn external_runtime_folder_hash_uses_upstream_selectors() {
        let root = tempfile::tempdir().unwrap();
        let skill = root.path().join("skill");
        fs::create_dir_all(&skill).unwrap();
        fs::write(skill.join("SKILL.md"), b"one").unwrap();
        let base = compute_skill_folder_hash(&skill).unwrap();
        fs::create_dir_all(skill.join(".git")).unwrap();
        fs::write(skill.join(".git/HEAD"), b"ignored").unwrap();
        fs::create_dir_all(skill.join("node_modules/p")).unwrap();
        fs::write(skill.join("node_modules/p/i"), b"ignored").unwrap();
        assert_eq!(base, compute_skill_folder_hash(&skill).unwrap());
        fs::write(skill.join("metadata.json"), b"included").unwrap();
        assert_ne!(base, compute_skill_folder_hash(&skill).unwrap());
    }

    #[test]
    fn external_runtime_folder_hash_tracks_content_add_rename_and_nested_files() {
        let root = tempfile::tempdir().unwrap();
        let skill = root.path().join("skill");
        fs::create_dir_all(skill.join("nested")).unwrap();
        fs::write(skill.join("SKILL.md"), b"one").unwrap();

        let initial = compute_skill_folder_hash(&skill).unwrap();
        fs::write(skill.join("SKILL.md"), b"two").unwrap();
        let content_changed = compute_skill_folder_hash(&skill).unwrap();
        assert_ne!(initial, content_changed);

        fs::write(skill.join("nested/reference.md"), b"reference").unwrap();
        let nested_added = compute_skill_folder_hash(&skill).unwrap();
        assert_ne!(content_changed, nested_added);

        fs::rename(
            skill.join("nested/reference.md"),
            skill.join("nested/renamed.md"),
        )
        .unwrap();
        assert_ne!(nested_added, compute_skill_folder_hash(&skill).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn external_runtime_folder_hash_ignores_symlink_target_changes() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let skill = root.path().join("skill");
        let outside = root.path().join("outside");
        fs::create_dir_all(&skill).unwrap();
        fs::write(skill.join("SKILL.md"), b"skill").unwrap();
        fs::write(&outside, b"one").unwrap();
        symlink(&outside, skill.join("linked")).unwrap();

        let initial = compute_skill_folder_hash(&skill).unwrap();
        fs::write(&outside, b"two").unwrap();
        assert_eq!(initial, compute_skill_folder_hash(&skill).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn external_runtime_installer_dereferences_links_and_preserves_mode() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let root = tempfile::tempdir().unwrap();
        let src = root.path().join("source");
        let dst = root.path().join("dest");
        fs::create_dir_all(src.join("references")).unwrap();
        fs::create_dir_all(src.join("symlink-exclusions")).unwrap();
        fs::write(src.join("SKILL.md"), b"skill").unwrap();
        fs::write(src.join("references/guide"), b"guide").unwrap();
        fs::write(src.join("run.sh"), b"#!/bin/sh\n").unwrap();
        fs::set_permissions(src.join("run.sh"), fs::Permissions::from_mode(0o755)).unwrap();
        symlink("../references", src.join("symlink-exclusions/.git")).unwrap();
        symlink("missing", src.join("broken")).unwrap();
        replace_external_runtime_skill(&src, &dst).unwrap();
        assert_eq!(
            fs::read(dst.join("symlink-exclusions/.git/guide")).unwrap(),
            b"guide"
        );
        assert!(!dst.join("broken").exists());
        assert_eq!(
            fs::metadata(dst.join("run.sh"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
    }
}
