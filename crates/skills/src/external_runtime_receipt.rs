use std::{
    fs,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use pioneer_protocol::SkillId;
use serde::{Deserialize, Serialize};

use crate::external_runtime_installer::normalize_external_runtime_path;

pub const EXTERNAL_RUNTIME_RECEIPT_VERSION: u32 = 2;
pub const EXTERNAL_RUNTIME_RECEIPT_FILE_NAME: &str = "cli-runtime-skills-lock.json";

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalRuntimeSkillReceiptEntry {
    pub skill_id: SkillId,
    pub owner: Option<String>,
    pub slug: String,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalRuntimeReceiptConversionCandidate {
    pub skill_id: SkillId,
    pub owner: Option<String>,
    pub slug: String,
    pub source_kind: String,
}

#[derive(Debug, Deserialize)]
struct ReceiptVersionEnvelope {
    version: u32,
}

#[derive(Debug, Deserialize)]
struct LegacyExternalRuntimeSkillReceiptV1 {
    version: u32,
    #[serde(default)]
    entries: Vec<LegacyExternalRuntimeSkillReceiptEntryV1>,
}

#[derive(Debug, Deserialize)]
struct LegacyExternalRuntimeSkillReceiptEntryV1 {
    runtime_id: String,
    runtime_kind: String,
    native_skills_root: String,
    install_name: String,
    skill_slug: String,
    source_kind: String,
    source_folder_hash: String,
    install_path: String,
    installed_at_unix_ms: u64,
    updated_at_unix_ms: u64,
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
    skill_id: SkillId,
}

fn receipt_key(
    skill_id: &SkillId,
    native_skills_root: &Path,
    install_name: &str,
) -> Result<ReceiptKey> {
    Ok(ReceiptKey {
        native_skills_root: normalized_root_string(native_skills_root)?,
        install_name: install_name.to_owned(),
        skill_id: skill_id.clone(),
    })
}

fn entry_key(entry: &ExternalRuntimeSkillReceiptEntry) -> ReceiptKey {
    ReceiptKey {
        native_skills_root: entry.native_skills_root.clone(),
        install_name: entry.install_name.clone(),
        skill_id: entry.skill_id.clone(),
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct DestinationKey {
    native_skills_root: String,
    install_name: String,
}

fn destination_key(entry: &ExternalRuntimeSkillReceiptEntry) -> DestinationKey {
    DestinationKey {
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

fn validate_normalized_entries(receipt: &ExternalRuntimeSkillReceipt) -> Result<()> {
    if receipt.entries.windows(2).any(|pair| {
        entry_key(&pair[0]) == entry_key(&pair[1])
            || destination_key(&pair[0]) == destination_key(&pair[1])
    }) {
        bail!("external runtime receipt contains duplicate v2 ownership entries");
    }
    Ok(())
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
    let mut receipt = serde_json::from_slice::<ExternalRuntimeSkillReceipt>(&payload)
        .with_context(|| {
            format!(
                "external runtime receipt `{}` is not valid v2 JSON",
                path.display()
            )
        })?;
    if receipt.version != EXTERNAL_RUNTIME_RECEIPT_VERSION {
        bail!(
            "external runtime receipt `{}` must use version `{EXTERNAL_RUNTIME_RECEIPT_VERSION}` (got `{}`)",
            path.display(),
            receipt.version
        );
    }
    for entry in &mut receipt.entries {
        normalize_entry_paths(entry)?;
    }
    normalize_entries(&mut receipt);
    validate_normalized_entries(&receipt).with_context(|| {
        format!(
            "external runtime receipt `{}` failed v2 ownership validation",
            path.display()
        )
    })?;
    Ok(receipt)
}

pub fn ensure_external_runtime_receipt_v2(
    path: &Path,
    candidates: &[ExternalRuntimeReceiptConversionCandidate],
) -> Result<ExternalRuntimeSkillReceipt> {
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
    let version: ReceiptVersionEnvelope = serde_json::from_slice(&payload).with_context(|| {
        format!(
            "failed to read external runtime receipt version from `{}`",
            path.display()
        )
    })?;
    if version.version == EXTERNAL_RUNTIME_RECEIPT_VERSION {
        return read_external_runtime_receipt(path);
    }
    if version.version != 1 {
        bail!(
            "external runtime receipt `{}` must use version `1` or `{EXTERNAL_RUNTIME_RECEIPT_VERSION}` (got `{}`)",
            path.display(),
            version.version
        );
    }

    let legacy = decode_legacy_external_runtime_receipt_v1(&payload, path)?;
    let mut converted = ExternalRuntimeSkillReceipt::default();
    for mut entry in legacy.entries {
        entry.native_skills_root = normalized_root_string(Path::new(&entry.native_skills_root))?;
        entry.install_path = normalize_external_runtime_path(Path::new(&entry.install_path))?
            .to_string_lossy()
            .into_owned();
        let matches = candidates
            .iter()
            .filter(|candidate| {
                candidate.source_kind == entry.source_kind
                    && conversion_candidate_label(candidate) == entry.skill_slug
            })
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            continue;
        }
        let candidate = matches[0];
        upsert_external_runtime_receipt_entry(
            &mut converted,
            ExternalRuntimeSkillReceiptEntry {
                skill_id: candidate.skill_id.clone(),
                owner: candidate.owner.clone(),
                slug: candidate.slug.clone(),
                runtime_id: entry.runtime_id,
                runtime_kind: entry.runtime_kind,
                native_skills_root: entry.native_skills_root,
                install_name: entry.install_name,
                skill_slug: entry.skill_slug,
                source_kind: entry.source_kind,
                source_folder_hash: entry.source_folder_hash,
                install_path: entry.install_path,
                installed_at_unix_ms: entry.installed_at_unix_ms,
                updated_at_unix_ms: entry.updated_at_unix_ms,
            },
        )?;
    }
    write_external_runtime_receipt_atomic(path, &converted)?;
    Ok(converted)
}

fn decode_legacy_external_runtime_receipt_v1(
    payload: &[u8],
    path: &Path,
) -> Result<LegacyExternalRuntimeSkillReceiptV1> {
    let legacy: LegacyExternalRuntimeSkillReceiptV1 = serde_json::from_slice(payload)
        .with_context(|| {
            format!(
                "failed to parse legacy v1 external runtime receipt `{}`",
                path.display()
            )
        })?;
    if legacy.version != 1 {
        bail!(
            "legacy external runtime receipt decoder only accepts version `1` in `{}`",
            path.display()
        );
    }
    Ok(legacy)
}

fn conversion_candidate_label(candidate: &ExternalRuntimeReceiptConversionCandidate) -> String {
    match candidate.owner.as_deref() {
        Some(owner) => format!("{owner}/{}", candidate.slug),
        None => candidate.slug.clone(),
    }
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
    for entry in &mut normalized.entries {
        normalize_entry_paths(entry)?;
    }
    normalize_entries(&mut normalized);
    validate_normalized_entries(&normalized)?;
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
    let Some(actual) = find_external_runtime_receipt_entry(
        receipt,
        &expected.skill_id,
        root,
        expected.install_name.as_str(),
    )?
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

    Ok(actual.skill_id == expected.skill_id
        && actual.owner == expected.owner
        && actual.slug == expected.slug
        && actual.runtime_id == expected.runtime_id
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
    skill_id: &SkillId,
    native_skills_root: &Path,
    install_name: &str,
) -> Result<Option<&'a ExternalRuntimeSkillReceiptEntry>> {
    let key = receipt_key(skill_id, native_skills_root, install_name)?;
    Ok(receipt.entries.iter().find(|entry| entry_key(entry) == key))
}

pub fn find_external_runtime_receipt_destination_entry<'a>(
    receipt: &'a ExternalRuntimeSkillReceipt,
    native_skills_root: &Path,
    install_name: &str,
) -> Result<Option<&'a ExternalRuntimeSkillReceiptEntry>> {
    let key = DestinationKey {
        native_skills_root: normalized_root_string(native_skills_root)?,
        install_name: install_name.to_owned(),
    };
    Ok(receipt
        .entries
        .iter()
        .find(|entry| destination_key(entry) == key))
}

pub fn upsert_external_runtime_receipt_entry(
    receipt: &mut ExternalRuntimeSkillReceipt,
    mut entry: ExternalRuntimeSkillReceiptEntry,
) -> Result<()> {
    receipt.version = EXTERNAL_RUNTIME_RECEIPT_VERSION;
    normalize_entry_paths(&mut entry)?;

    let key = entry_key(&entry);
    let destination = destination_key(&entry);
    if let Some(existing) = receipt
        .entries
        .iter_mut()
        .find(|candidate| entry_key(candidate) == key)
    {
        entry.installed_at_unix_ms = existing.installed_at_unix_ms;
        *existing = entry;
    } else {
        receipt
            .entries
            .retain(|candidate| destination_key(candidate) != destination);
        receipt.entries.push(entry);
    }
    normalize_entries(receipt);
    Ok(())
}

pub fn remove_external_runtime_receipt_entry(
    receipt: &mut ExternalRuntimeSkillReceipt,
    skill_id: &SkillId,
    native_skills_root: &Path,
    install_name: &str,
) -> Result<Option<ExternalRuntimeSkillReceiptEntry>> {
    let key = receipt_key(skill_id, native_skills_root, install_name)?;
    let Some(index) = receipt
        .entries
        .iter()
        .position(|entry| entry_key(entry) == key)
    else {
        return Ok(None);
    };
    Ok(Some(receipt.entries.remove(index)))
}

pub fn remove_external_runtime_receipt_destination_entry(
    receipt: &mut ExternalRuntimeSkillReceipt,
    native_skills_root: &Path,
    install_name: &str,
) -> Result<Option<ExternalRuntimeSkillReceiptEntry>> {
    let key = DestinationKey {
        native_skills_root: normalized_root_string(native_skills_root)?,
        install_name: install_name.to_owned(),
    };
    let Some(index) = receipt
        .entries
        .iter()
        .position(|entry| destination_key(entry) == key)
    else {
        return Ok(None);
    };
    Ok(Some(receipt.entries.remove(index)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_skill_id(seed: &str) -> SkillId {
        let mut value = seed
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .collect::<String>();
        value.truncate(21);
        while value.len() < 21 {
            value.push('R');
        }
        SkillId::new(value).expect("valid receipt test SkillId")
    }

    fn entry(
        root: &Path,
        name: &str,
        slug: &str,
        timestamp: u64,
    ) -> ExternalRuntimeSkillReceiptEntry {
        let leaf_slug = slug.rsplit('/').next().unwrap_or(slug);
        ExternalRuntimeSkillReceiptEntry {
            skill_id: test_skill_id(slug),
            owner: Some("workspace".to_owned()),
            slug: leaf_slug.to_owned(),
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

    fn conversion_candidate(
        skill_id: &str,
        owner: &str,
        slug: &str,
    ) -> ExternalRuntimeReceiptConversionCandidate {
        ExternalRuntimeReceiptConversionCandidate {
            skill_id: SkillId::new(skill_id).unwrap(),
            owner: Some(owner.to_owned()),
            slug: slug.to_owned(),
            source_kind: "registry".to_owned(),
        }
    }

    #[test]
    fn external_runtime_receipt_crud_is_keyed_by_exact_id_and_destination() {
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
                &test_skill_id("first"),
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
        let updated = find_external_runtime_receipt_entry(
            &receipt,
            &test_skill_id("updated"),
            root,
            "shared",
        )
        .unwrap()
        .unwrap();
        assert_eq!(updated.skill_slug, "updated");
        assert_eq!(updated.installed_at_unix_ms, 30);
        assert_eq!(updated.updated_at_unix_ms, 30);
        assert_eq!(receipt.entries[1].skill_slug, "other");

        let removed = remove_external_runtime_receipt_entry(
            &mut receipt,
            &test_skill_id("updated"),
            root,
            "shared",
        )
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
    fn external_runtime_receipt_keeps_same_label_for_distinct_ids_and_roots() {
        let first_root = Path::new("/tmp/runtime-a/skills");
        let second_root = Path::new("/tmp/runtime-b/skills");
        let first = entry(first_root, "shared", "shared", 1);
        let mut second = entry(second_root, "shared", "shared", 2);
        second.skill_id = test_skill_id("other-shared");
        let mut receipt = ExternalRuntimeSkillReceipt::default();

        upsert_external_runtime_receipt_entry(&mut receipt, first.clone()).unwrap();
        upsert_external_runtime_receipt_entry(&mut receipt, second.clone()).unwrap();

        assert_eq!(receipt.entries.len(), 2);
        assert_eq!(receipt.entries[0].slug, receipt.entries[1].slug);
        assert_ne!(receipt.entries[0].skill_id, receipt.entries[1].skill_id);
        assert!(
            find_external_runtime_receipt_entry(&receipt, &first.skill_id, first_root, "shared",)
                .unwrap()
                .is_some()
        );
        assert!(
            find_external_runtime_receipt_entry(&receipt, &second.skill_id, second_root, "shared",)
                .unwrap()
                .is_some()
        );
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
        assert!(read_external_runtime_receipt(&path).is_err());
        fs::write(&path, br#"{"version":1,"entries":[]}"#).unwrap();
        assert!(read_external_runtime_receipt(&path).is_err());

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
            "skill_id",
            "owner",
            "slug",
            "runtime_id",
            "runtime_kind",
            "skill_slug",
            "source_kind",
            "hash",
        ] {
            let mut mismatch = expected.clone();
            match mutation {
                "skill_id" => mismatch.skill_id = test_skill_id("other"),
                "owner" => mismatch.owner = Some("other".to_owned()),
                "slug" => mismatch.slug.push_str("-other"),
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

    #[test]
    fn legacy_receipt_conversion_keeps_only_unambiguous_entries_and_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(EXTERNAL_RUNTIME_RECEIPT_FILE_NAME);
        let root = temp.path().join("runtime/skills");
        let payload = serde_json::json!({
            "version": 1,
            "entries": [
                {
                    "runtime_id": "runtime-1",
                    "runtime_kind": "codex",
                    "native_skills_root": root,
                    "install_name": "alpha",
                    "skill_slug": "owner/alpha",
                    "source_kind": "registry",
                    "source_folder_hash": "hash-alpha",
                    "install_path": root.join("alpha"),
                    "installed_at_unix_ms": 10,
                    "updated_at_unix_ms": 11
                },
                {
                    "runtime_id": "runtime-1",
                    "runtime_kind": "codex",
                    "native_skills_root": root,
                    "install_name": "stale",
                    "skill_slug": "owner/stale",
                    "source_kind": "registry",
                    "source_folder_hash": "hash-stale",
                    "install_path": root.join("stale"),
                    "installed_at_unix_ms": 12,
                    "updated_at_unix_ms": 13
                }
            ]
        });
        fs::write(&path, serde_json::to_vec_pretty(&payload).unwrap()).unwrap();
        let candidates = [conversion_candidate(
            "AAAAAAAAAAAAAAAAAAAAA",
            "owner",
            "alpha",
        )];

        let first = ensure_external_runtime_receipt_v2(&path, &candidates).unwrap();
        assert_eq!(first.entries.len(), 1);
        assert_eq!(first.entries[0].skill_id.as_str(), "AAAAAAAAAAAAAAAAAAAAA");
        assert_eq!(first.entries[0].owner.as_deref(), Some("owner"));
        assert_eq!(first.entries[0].slug, "alpha");
        assert_eq!(first.entries[0].installed_at_unix_ms, 10);
        let second = ensure_external_runtime_receipt_v2(&path, &candidates).unwrap();
        assert_eq!(second, first);
        assert_eq!(read_external_runtime_receipt(&path).unwrap(), first);
    }

    #[test]
    fn legacy_receipt_conversion_drops_ambiguous_locator() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(EXTERNAL_RUNTIME_RECEIPT_FILE_NAME);
        let root = temp.path().join("runtime/skills");
        let payload = serde_json::json!({
            "version": 1,
            "entries": [{
                "runtime_id": "runtime-1",
                "runtime_kind": "codex",
                "native_skills_root": root,
                "install_name": "alpha",
                "skill_slug": "owner/alpha",
                "source_kind": "registry",
                "source_folder_hash": "hash-alpha",
                "install_path": root.join("alpha"),
                "installed_at_unix_ms": 10,
                "updated_at_unix_ms": 11
            }]
        });
        fs::write(&path, serde_json::to_vec_pretty(&payload).unwrap()).unwrap();
        let candidates = [
            conversion_candidate("AAAAAAAAAAAAAAAAAAAAA", "owner", "alpha"),
            conversion_candidate("BBBBBBBBBBBBBBBBBBBBB", "owner", "alpha"),
        ];

        let converted = ensure_external_runtime_receipt_v2(&path, &candidates).unwrap();
        assert!(converted.entries.is_empty());
        assert_eq!(read_external_runtime_receipt(&path).unwrap().version, 2);
    }
}
