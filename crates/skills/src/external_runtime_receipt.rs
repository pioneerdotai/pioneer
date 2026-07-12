use std::{
    fs,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::external_runtime_installer::normalize_external_runtime_path;

pub const EXTERNAL_RUNTIME_RECEIPT_VERSION: u32 = 1;
pub const EXTERNAL_RUNTIME_RECEIPT_FILE_NAME: &str = "cli-runtime-skills-lock.json";

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalRuntimeSkillReceiptEntry {
    pub runtime_id: String,
    pub runtime_kind: String,
    pub native_skills_root: String,
    pub install_name: String,
    pub skill_slug: String,
    pub source_kind: String,
    pub source_folder_hash: String,
    pub install_path: String,
    pub installed_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalRuntimeSkillReceipt {
    pub version: u32,
    #[serde(default)]
    pub entries: Vec<ExternalRuntimeSkillReceiptEntry>,
}

impl Default for ExternalRuntimeSkillReceipt {
    fn default() -> Self {
        Self {
            version: EXTERNAL_RUNTIME_RECEIPT_VERSION,
            entries: Vec::new(),
        }
    }
}

fn normalized_root_string(root: &Path) -> Result<String> {
    Ok(normalize_external_runtime_path(root)?
        .to_string_lossy()
        .into_owned())
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct ReceiptKey {
    native_skills_root: String,
    install_name: String,
}

fn receipt_key(native_skills_root: &Path, install_name: &str) -> Result<ReceiptKey> {
    Ok(ReceiptKey {
        native_skills_root: normalized_root_string(native_skills_root)?,
        install_name: install_name.to_owned(),
    })
}

fn entry_key(entry: &ExternalRuntimeSkillReceiptEntry) -> ReceiptKey {
    ReceiptKey {
        native_skills_root: entry.native_skills_root.clone(),
        install_name: entry.install_name.clone(),
    }
}

fn normalize_entry_paths(entry: &mut ExternalRuntimeSkillReceiptEntry) -> Result<()> {
    entry.native_skills_root = normalized_root_string(Path::new(&entry.native_skills_root))?;
    entry.install_path = normalize_external_runtime_path(Path::new(&entry.install_path))?
        .to_string_lossy()
        .into_owned();
    Ok(())
}

fn normalize_entries(receipt: &mut ExternalRuntimeSkillReceipt) {
    receipt.entries.sort_by_key(entry_key);
}

pub fn read_external_runtime_receipt(path: &Path) -> Result<ExternalRuntimeSkillReceipt> {
    let payload = match fs::read(path) {
        Ok(payload) => payload,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ExternalRuntimeSkillReceipt::default());
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to read external runtime receipt `{}`",
                    path.display()
                )
            });
        }
    };
    let Ok(mut receipt) = serde_json::from_slice::<ExternalRuntimeSkillReceipt>(&payload) else {
        return Ok(ExternalRuntimeSkillReceipt::default());
    };
    if receipt.version != EXTERNAL_RUNTIME_RECEIPT_VERSION {
        return Ok(ExternalRuntimeSkillReceipt::default());
    }
    for entry in &mut receipt.entries {
        normalize_entry_paths(entry)?;
    }
    normalize_entries(&mut receipt);
    if receipt
        .entries
        .windows(2)
        .any(|pair| entry_key(&pair[0]) == entry_key(&pair[1]))
    {
        return Ok(ExternalRuntimeSkillReceipt::default());
    }
    Ok(receipt)
}

pub fn write_external_runtime_receipt_atomic(
    path: &Path,
    receipt: &ExternalRuntimeSkillReceipt,
) -> Result<()> {
    if receipt.version != EXTERNAL_RUNTIME_RECEIPT_VERSION {
        bail!(
            "external runtime receipt version must be `{EXTERNAL_RUNTIME_RECEIPT_VERSION}` (got `{}`)",
            receipt.version
        );
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create external runtime receipt directory `{}`",
                parent.display()
            )
        })?;
    }

    let mut normalized = receipt.clone();
    normalize_entries(&mut normalized);
    let mut payload = serde_json::to_string_pretty(&normalized)
        .context("failed to serialize external runtime receipt")?;
    payload.push('\n');
    let unique = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let temp = path.with_extension(format!("tmp.{}.{}.{}", std::process::id(), nanos, unique));
    fs::write(&temp, payload).with_context(|| {
        format!(
            "failed to write temporary external runtime receipt `{}`",
            temp.display()
        )
    })?;
    fs::rename(&temp, path).with_context(|| {
        format!(
            "failed to atomically replace external runtime receipt `{}` from `{}`",
            path.display(),
            temp.display()
        )
    })?;
    Ok(())
}

pub fn external_runtime_skill_is_current(
    receipt: &ExternalRuntimeSkillReceipt,
    expected: &ExternalRuntimeSkillReceiptEntry,
    destination: &Path,
) -> Result<bool> {
    let metadata = match fs::symlink_metadata(destination) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to inspect external skill destination `{}`",
                    destination.display()
                )
            });
        }
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Ok(false);
    }
    if fs::read(destination.join("SKILL.md")).is_err() {
        return Ok(false);
    }

    let root = Path::new(&expected.native_skills_root);
    let Some(actual) =
        find_external_runtime_receipt_entry(receipt, root, expected.install_name.as_str())?
    else {
        return Ok(false);
    };
    let expected_root = normalized_root_string(root)?;
    let expected_install_path = normalize_external_runtime_path(Path::new(&expected.install_path))?
        .to_string_lossy()
        .into_owned();
    let destination = normalize_external_runtime_path(destination)?
        .to_string_lossy()
        .into_owned();

    Ok(actual.runtime_id == expected.runtime_id
        && actual.runtime_kind == expected.runtime_kind
        && actual.native_skills_root == expected_root
        && actual.install_name == expected.install_name
        && actual.skill_slug == expected.skill_slug
        && actual.source_kind == expected.source_kind
        && actual.source_folder_hash == expected.source_folder_hash
        && actual.install_path == expected_install_path
        && actual.install_path == destination)
}

pub fn find_external_runtime_receipt_entry<'a>(
    receipt: &'a ExternalRuntimeSkillReceipt,
    native_skills_root: &Path,
    install_name: &str,
) -> Result<Option<&'a ExternalRuntimeSkillReceiptEntry>> {
    let key = receipt_key(native_skills_root, install_name)?;
    Ok(receipt.entries.iter().find(|entry| entry_key(entry) == key))
}

pub fn upsert_external_runtime_receipt_entry(
    receipt: &mut ExternalRuntimeSkillReceipt,
    mut entry: ExternalRuntimeSkillReceiptEntry,
) -> Result<()> {
    receipt.version = EXTERNAL_RUNTIME_RECEIPT_VERSION;
    normalize_entry_paths(&mut entry)?;

    let key = entry_key(&entry);
    if let Some(existing) = receipt
        .entries
        .iter_mut()
        .find(|candidate| entry_key(candidate) == key)
    {
        entry.installed_at_unix_ms = existing.installed_at_unix_ms;
        *existing = entry;
    } else {
        receipt.entries.push(entry);
    }
    normalize_entries(receipt);
    Ok(())
}

pub fn remove_external_runtime_receipt_entry(
    receipt: &mut ExternalRuntimeSkillReceipt,
    native_skills_root: &Path,
    install_name: &str,
) -> Result<Option<ExternalRuntimeSkillReceiptEntry>> {
    let key = receipt_key(native_skills_root, install_name)?;
    let Some(index) = receipt
        .entries
        .iter()
        .position(|entry| entry_key(entry) == key)
    else {
        return Ok(None);
    };
    Ok(Some(receipt.entries.remove(index)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(
        root: &Path,
        name: &str,
        slug: &str,
        timestamp: u64,
    ) -> ExternalRuntimeSkillReceiptEntry {
        ExternalRuntimeSkillReceiptEntry {
            runtime_id: "runtime-1".to_owned(),
            runtime_kind: "codex".to_owned(),
            native_skills_root: root.to_string_lossy().into_owned(),
            install_name: name.to_owned(),
            skill_slug: slug.to_owned(),
            source_kind: "registry".to_owned(),
            source_folder_hash: format!("hash-{timestamp}"),
            install_path: root.join(name).to_string_lossy().into_owned(),
            installed_at_unix_ms: timestamp,
            updated_at_unix_ms: timestamp,
        }
    }

    #[test]
    fn external_runtime_receipt_crud_is_keyed_by_root_and_name() {
        let root = Path::new("/tmp/runtime-a/../runtime-a/skills");
        let other_root = Path::new("/tmp/runtime-b/skills");
        let mut receipt = ExternalRuntimeSkillReceipt::default();

        upsert_external_runtime_receipt_entry(
            &mut receipt,
            entry(other_root, "shared", "other", 10),
        )
        .unwrap();
        upsert_external_runtime_receipt_entry(&mut receipt, entry(root, "shared", "first", 20))
            .unwrap();

        assert_eq!(receipt.entries.len(), 2);
        assert_eq!(receipt.entries[0].skill_slug, "first");
        assert_eq!(
            find_external_runtime_receipt_entry(
                &receipt,
                Path::new("/tmp/runtime-a/skills"),
                "shared"
            )
            .unwrap()
            .unwrap()
            .skill_slug,
            "first"
        );

        upsert_external_runtime_receipt_entry(&mut receipt, entry(root, "shared", "updated", 30))
            .unwrap();
        let updated = find_external_runtime_receipt_entry(&receipt, root, "shared")
            .unwrap()
            .unwrap();
        assert_eq!(updated.skill_slug, "updated");
        assert_eq!(updated.installed_at_unix_ms, 20);
        assert_eq!(updated.updated_at_unix_ms, 30);
        assert_eq!(receipt.entries[1].skill_slug, "other");

        let removed = remove_external_runtime_receipt_entry(&mut receipt, root, "shared")
            .unwrap()
            .unwrap();
        assert_eq!(removed.skill_slug, "updated");
        assert_eq!(receipt.entries.len(), 1);
        assert_eq!(receipt.entries[0].skill_slug, "other");
    }

    #[test]
    fn external_runtime_receipt_all_fields_round_trip_deterministically() {
        let root = Path::new("/tmp/runtime/skills");
        let mut receipt = ExternalRuntimeSkillReceipt::default();
        upsert_external_runtime_receipt_entry(&mut receipt, entry(root, "zeta", "zeta", 1))
            .unwrap();
        upsert_external_runtime_receipt_entry(&mut receipt, entry(root, "alpha", "alpha", 2))
            .unwrap();

        let first = serde_json::to_string_pretty(&receipt).unwrap();
        let round_trip: ExternalRuntimeSkillReceipt = serde_json::from_str(&first).unwrap();
        let second = serde_json::to_string_pretty(&round_trip).unwrap();
        assert_eq!(receipt, round_trip);
        assert_eq!(first, second);
        assert!(first.find("alpha").unwrap() < first.find("zeta").unwrap());
    }

    #[test]
    fn external_runtime_receipt_io_is_atomic_deterministic_and_resilient() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp
            .path()
            .join("state")
            .join(EXTERNAL_RUNTIME_RECEIPT_FILE_NAME);
        assert_eq!(
            read_external_runtime_receipt(&path).unwrap(),
            ExternalRuntimeSkillReceipt::default()
        );

        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"not json").unwrap();
        assert_eq!(
            read_external_runtime_receipt(&path).unwrap(),
            ExternalRuntimeSkillReceipt::default()
        );
        fs::write(&path, br#"{"version":2,"entries":[]}"#).unwrap();
        assert_eq!(
            read_external_runtime_receipt(&path).unwrap(),
            ExternalRuntimeSkillReceipt::default()
        );

        let root = temp.path().join("runtime/skills");
        let mut receipt = ExternalRuntimeSkillReceipt::default();
        upsert_external_runtime_receipt_entry(&mut receipt, entry(&root, "zeta", "zeta", 1))
            .unwrap();
        upsert_external_runtime_receipt_entry(&mut receipt, entry(&root, "alpha", "alpha", 2))
            .unwrap();
        write_external_runtime_receipt_atomic(&path, &receipt).unwrap();
        let first = fs::read(&path).unwrap();
        assert_eq!(first.last(), Some(&b'\n'));
        write_external_runtime_receipt_atomic(&path, &receipt).unwrap();
        assert_eq!(first, fs::read(&path).unwrap());
        assert_eq!(receipt, read_external_runtime_receipt(&path).unwrap());
        assert_eq!(
            fs::read_dir(path.parent().unwrap()).unwrap().count(),
            1,
            "successful atomic writes must not leave temporary siblings"
        );
    }

    #[test]
    fn external_runtime_receipt_current_requires_identity_directory_and_skill() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("runtime/skills");
        let destination = root.join("skill");
        let expected = entry(&root, "skill", "registry/skill", 10);
        let mut receipt = ExternalRuntimeSkillReceipt::default();
        upsert_external_runtime_receipt_entry(&mut receipt, expected.clone()).unwrap();

        assert!(!external_runtime_skill_is_current(&receipt, &expected, &destination).unwrap());
        fs::create_dir_all(&root).unwrap();
        fs::write(&destination, b"file-not-directory").unwrap();
        assert!(!external_runtime_skill_is_current(&receipt, &expected, &destination).unwrap());
        fs::remove_file(&destination).unwrap();
        fs::create_dir_all(&destination).unwrap();
        assert!(!external_runtime_skill_is_current(&receipt, &expected, &destination).unwrap());
        fs::write(destination.join("SKILL.md"), b"skill").unwrap();
        assert!(external_runtime_skill_is_current(&receipt, &expected, &destination).unwrap());

        for mutation in [
            "runtime_id",
            "runtime_kind",
            "skill_slug",
            "source_kind",
            "hash",
        ] {
            let mut mismatch = expected.clone();
            match mutation {
                "runtime_id" => mismatch.runtime_id.push_str("-other"),
                "runtime_kind" => mismatch.runtime_kind = "claude".to_owned(),
                "skill_slug" => mismatch.skill_slug.push_str("-other"),
                "source_kind" => mismatch.source_kind = "user".to_owned(),
                "hash" => mismatch.source_folder_hash.push_str("-other"),
                _ => unreachable!(),
            }
            assert!(
                !external_runtime_skill_is_current(&receipt, &mismatch, &destination).unwrap(),
                "{mutation} mismatch must be stale"
            );
        }
        let mut wrong_path = expected.clone();
        wrong_path.install_path = root.join("other").to_string_lossy().into_owned();
        assert!(!external_runtime_skill_is_current(&receipt, &wrong_path, &destination).unwrap());
        assert!(
            !external_runtime_skill_is_current(&receipt, &expected, &root.join("other")).unwrap()
        );
        let mut wrong_root = expected.clone();
        wrong_root.native_skills_root = temp
            .path()
            .join("other-root")
            .to_string_lossy()
            .into_owned();
        assert!(!external_runtime_skill_is_current(&receipt, &wrong_root, &destination).unwrap());
        let mut wrong_name = expected.clone();
        wrong_name.install_name = "other".to_owned();
        assert!(!external_runtime_skill_is_current(&receipt, &wrong_name, &destination).unwrap());

        fs::remove_file(destination.join("SKILL.md")).unwrap();
        fs::create_dir(destination.join("SKILL.md")).unwrap();
        assert!(!external_runtime_skill_is_current(&receipt, &expected, &destination).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn external_runtime_receipt_current_rejects_directory_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("runtime/skills");
        let real = temp.path().join("real");
        let destination = root.join("skill");
        fs::create_dir_all(&real).unwrap();
        fs::create_dir_all(&root).unwrap();
        fs::write(real.join("SKILL.md"), b"skill").unwrap();
        symlink(&real, &destination).unwrap();
        let expected = entry(&root, "skill", "registry/skill", 10);
        let mut receipt = ExternalRuntimeSkillReceipt::default();
        upsert_external_runtime_receipt_entry(&mut receipt, expected.clone()).unwrap();
        assert!(!external_runtime_skill_is_current(&receipt, &expected, &destination).unwrap());
    }
}
