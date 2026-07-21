use crate::contract::SkillTrustLevel;
use anyhow::{Context, Result, bail};
use pioneer_protocol::SkillId;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

const LOCK_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillLockEntry {
    pub skill_id: SkillId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    pub slug: String,
    pub source_kind: String,
    pub source_ref: String,
    pub install_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub trust_level: SkillTrustLevel,
    pub fingerprint: String,
    pub installed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillsLock {
    pub version: u32,
    #[serde(default)]
    pub entries: Vec<SkillLockEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillLockConversionCandidate {
    pub skill_id: SkillId,
    pub owner: Option<String>,
    pub slug: String,
    pub source_kind: String,
    pub source_ref: String,
    pub install_path: String,
    pub version: Option<String>,
    pub trust_level: SkillTrustLevel,
    pub fingerprint: String,
}

#[derive(Debug, Deserialize)]
struct LockVersionEnvelope {
    version: u32,
}

#[derive(Debug, Deserialize)]
struct LegacySkillsLockV1 {
    version: u32,
    #[serde(default)]
    entries: Vec<LegacySkillLockEntryV1>,
}

#[derive(Debug, Deserialize)]
struct LegacySkillLockEntryV1 {
    owner: String,
    slug: String,
    source_kind: String,
    #[serde(rename = "source_ref")]
    _source_ref: String,
    install_path: String,
    #[serde(default)]
    #[serde(rename = "version")]
    _version: Option<String>,
    #[serde(rename = "trust_level")]
    _trust_level: SkillTrustLevel,
    #[serde(rename = "fingerprint")]
    _fingerprint: String,
    installed_at: i64,
}

impl Default for SkillsLock {
    fn default() -> Self {
        Self {
            version: LOCK_VERSION,
            entries: Vec::new(),
        }
    }
}

pub fn read_skills_lock(path: &Path) -> Result<SkillsLock> {
    if !path.exists() {
        return Ok(SkillsLock::default());
    }

    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read skills lock file `{}`", path.display()))?;
    let lock: SkillsLock = toml::from_str(content.as_str())
        .with_context(|| format!("failed to parse skills lock file `{}`", path.display()))?;

    if lock.version != LOCK_VERSION {
        bail!(
            "unsupported skills lock version `{}` in `{}`; expected `{LOCK_VERSION}`",
            lock.version,
            path.display()
        );
    }

    Ok(normalize_lock(lock))
}

pub fn ensure_skills_lock_v2(
    path: &Path,
    candidates: &[SkillLockConversionCandidate],
) -> Result<SkillsLock> {
    if !path.exists() {
        return Ok(SkillsLock::default());
    }
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read skills lock file `{}`", path.display()))?;
    let version: LockVersionEnvelope = toml::from_str(content.as_str()).with_context(|| {
        format!(
            "failed to read skills lock version from `{}`",
            path.display()
        )
    })?;
    if version.version == LOCK_VERSION {
        return read_skills_lock(path);
    }
    if version.version != 1 {
        bail!(
            "unsupported skills lock version `{}` in `{}`; expected `1` or `{LOCK_VERSION}`",
            version.version,
            path.display()
        );
    }

    let legacy = decode_legacy_skills_lock_v1(content.as_str(), path)?;
    let mut converted = SkillsLock::default();
    for entry in legacy.entries {
        let exact_path = candidates
            .iter()
            .filter(|candidate| {
                candidate.source_kind == entry.source_kind
                    && candidate.install_path == entry.install_path
            })
            .collect::<Vec<_>>();
        let matched = if exact_path.len() == 1 {
            exact_path.first().copied()
        } else if exact_path.is_empty() {
            let locator = candidates
                .iter()
                .filter(|candidate| {
                    candidate.source_kind == entry.source_kind
                        && candidate.owner.as_deref() == Some(entry.owner.as_str())
                        && candidate.slug == entry.slug
                })
                .collect::<Vec<_>>();
            (locator.len() == 1).then(|| locator[0])
        } else {
            None
        };
        let Some(candidate) = matched else {
            continue;
        };
        upsert_lock_entry(
            &mut converted,
            SkillLockEntry {
                skill_id: candidate.skill_id.clone(),
                owner: candidate.owner.clone(),
                slug: candidate.slug.clone(),
                source_kind: candidate.source_kind.clone(),
                source_ref: candidate.source_ref.clone(),
                install_path: candidate.install_path.clone(),
                version: candidate.version.clone(),
                trust_level: candidate.trust_level,
                fingerprint: candidate.fingerprint.clone(),
                installed_at: entry.installed_at,
            },
        );
    }
    write_skills_lock_atomic(path, &converted)?;
    Ok(converted)
}

fn decode_legacy_skills_lock_v1(content: &str, path: &Path) -> Result<LegacySkillsLockV1> {
    let legacy: LegacySkillsLockV1 = toml::from_str(content).with_context(|| {
        format!(
            "failed to parse legacy v1 skills lock file `{}`",
            path.display()
        )
    })?;
    if legacy.version != 1 {
        bail!(
            "legacy skills lock decoder only accepts version `1` in `{}`",
            path.display()
        );
    }
    Ok(legacy)
}

pub fn write_skills_lock_atomic(path: &Path, lock: &SkillsLock) -> Result<()> {
    if lock.version != LOCK_VERSION {
        bail!(
            "skills lock version must be `{LOCK_VERSION}` (got `{}`)",
            lock.version
        );
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create parent directory for skills lock `{}`",
                parent.display()
            )
        })?;
    }

    let normalized = normalize_lock(lock.clone());
    let payload = toml::to_string_pretty(&normalized).context("failed to serialize skills lock")?;
    let tmp = path.with_extension(format!(
        "tmp.{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|value| value.as_nanos())
            .unwrap_or_default()
    ));

    fs::write(&tmp, payload)
        .with_context(|| format!("failed to write temporary lock file `{}`", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| {
        format!(
            "failed to atomically replace lock file `{}` with `{}`",
            path.display(),
            tmp.display()
        )
    })?;

    Ok(())
}

pub fn upsert_lock_entry(lock: &mut SkillsLock, entry: SkillLockEntry) {
    if lock.version != LOCK_VERSION {
        lock.version = LOCK_VERSION;
    }

    if let Some(existing) = lock
        .entries
        .iter_mut()
        .find(|candidate| candidate.skill_id == entry.skill_id)
    {
        *existing = entry;
    } else {
        lock.entries.push(entry);
    }

    lock.entries
        .sort_by(|left, right| left.skill_id.cmp(&right.skill_id));
}

pub fn remove_lock_entry(lock: &mut SkillsLock, skill_id: &SkillId) -> Option<SkillLockEntry> {
    let index = lock
        .entries
        .iter()
        .position(|entry| &entry.skill_id == skill_id)?;
    Some(lock.entries.remove(index))
}

pub fn find_lock_entry<'a>(lock: &'a SkillsLock, skill_id: &SkillId) -> Option<&'a SkillLockEntry> {
    lock.entries
        .iter()
        .find(|entry| &entry.skill_id == skill_id)
}

fn normalize_lock(mut lock: SkillsLock) -> SkillsLock {
    lock.entries
        .sort_by(|left, right| left.skill_id.cmp(&right.skill_id));
    lock.entries
        .dedup_by(|left, right| left.skill_id == right.skill_id);
    lock
}

#[cfg(test)]
mod tests {
    use super::{
        SkillLockConversionCandidate, SkillLockEntry, ensure_skills_lock_v2, find_lock_entry,
        read_skills_lock, remove_lock_entry, upsert_lock_entry, write_skills_lock_atomic,
    };
    use crate::contract::SkillTrustLevel;
    use pioneer_protocol::SkillId;
    use std::fs;

    fn temp_file(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("unix timestamp")
            .as_nanos();
        std::env::temp_dir().join(format!("pioneer-skills-lock-{name}-{nanos}.toml"))
    }

    fn sample_entry(
        skill_id: &str,
        owner: &str,
        slug: &str,
        source_kind: &str,
        fingerprint: &str,
    ) -> SkillLockEntry {
        SkillLockEntry {
            skill_id: SkillId::new(skill_id).unwrap(),
            owner: Some(owner.to_owned()),
            slug: slug.to_owned(),
            source_kind: source_kind.to_owned(),
            source_ref: "github.com/example/skill".to_owned(),
            install_path: format!("/tmp/{owner}/{slug}"),
            version: Some("1.0.0".to_owned()),
            trust_level: SkillTrustLevel::Verified,
            fingerprint: fingerprint.to_owned(),
            installed_at: 1_700_000_000,
        }
    }

    fn conversion_candidate(skill_id: &str, install_path: &str) -> SkillLockConversionCandidate {
        SkillLockConversionCandidate {
            skill_id: SkillId::new(skill_id).unwrap(),
            owner: Some("registry".to_owned()),
            slug: "alpha".to_owned(),
            source_kind: "registry".to_owned(),
            source_ref: "registry:alpha".to_owned(),
            install_path: install_path.to_owned(),
            version: Some("2.0.0".to_owned()),
            trust_level: SkillTrustLevel::Verified,
            fingerprint: format!("fingerprint-{skill_id}"),
        }
    }

    #[test]
    fn lock_roundtrip_and_upsert() {
        let path = temp_file("roundtrip");

        let mut lock = super::SkillsLock::default();
        upsert_lock_entry(
            &mut lock,
            sample_entry(
                "AAAAAAAAAAAAAAAAAAAAA",
                "registry",
                "alpha",
                "registry",
                "fp1",
            ),
        );
        upsert_lock_entry(
            &mut lock,
            sample_entry(
                "AAAAAAAAAAAAAAAAAAAAA",
                "registry",
                "alpha",
                "registry",
                "fp2",
            ),
        );
        upsert_lock_entry(
            &mut lock,
            sample_entry(
                "BBBBBBBBBBBBBBBBBBBBB",
                "registry",
                "alpha",
                "registry",
                "fp3",
            ),
        );

        write_skills_lock_atomic(path.as_path(), &lock).expect("write lock");
        let loaded = read_skills_lock(path.as_path()).expect("read lock");

        assert_eq!(loaded.entries.len(), 2);
        assert_eq!(
            find_lock_entry(&loaded, &SkillId::new("AAAAAAAAAAAAAAAAAAAAA").unwrap())
                .expect("alpha entry")
                .fingerprint,
            "fp2"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn unsupported_lock_version_fails_without_legacy_fallback() {
        let path = temp_file("version");
        fs::write(
            path.as_path(),
            "version = 999\n[[entries]]\nskill_id='AAAAAAAAAAAAAAAAAAAAA'\nowner='registry'\nslug='a'\nsource_kind='registry'\nsource_ref='x'\ninstall_path='/tmp/registry/a'\ntrust_level='verified'\nfingerprint='fp'\ninstalled_at=1\n",
        )
        .expect("write lock");

        let error = read_skills_lock(path.as_path()).expect_err("version mismatch expected");
        assert!(
            error
                .to_string()
                .contains("unsupported skills lock version")
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn normal_reader_rejects_v1_locator_entries() {
        let path = temp_file("legacy-v1");
        fs::write(
            path.as_path(),
            "version = 1\n[[entries]]\nowner='registry'\nslug='a'\nsource_kind='registry'\nsource_ref='x'\ninstall_path='/tmp/registry/a'\ntrust_level='verified'\nfingerprint='fp'\ninstalled_at=1\n",
        )
        .expect("write legacy lock");

        assert!(read_skills_lock(path.as_path()).is_err());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn remove_entry_is_deterministic() {
        let mut lock = super::SkillsLock::default();
        let first = SkillId::new("AAAAAAAAAAAAAAAAAAAAA").unwrap();
        upsert_lock_entry(
            &mut lock,
            sample_entry(first.as_str(), "registry", "a", "registry", "fp1"),
        );
        upsert_lock_entry(
            &mut lock,
            sample_entry("BBBBBBBBBBBBBBBBBBBBB", "registry", "a", "registry", "fp2"),
        );

        let removed = remove_lock_entry(&mut lock, &first).expect("entry removed");
        assert_eq!(removed.slug, "a");
        assert_eq!(lock.entries.len(), 1);
    }

    #[test]
    fn legacy_lock_conversion_prefers_exact_path_and_is_idempotent() {
        let path = temp_file("convert-path-first");
        fs::write(
            path.as_path(),
            "version = 1\n[[entries]]\nowner='registry'\nslug='alpha'\nsource_kind='registry'\nsource_ref='old'\ninstall_path='/old/b'\nversion='1.0.0'\ntrust_level='verified'\nfingerprint='old-fp'\ninstalled_at=7\n[[entries]]\nowner='registry'\nslug='alpha'\nsource_kind='registry'\nsource_ref='stale'\ninstall_path='/missing'\ntrust_level='verified'\nfingerprint='stale'\ninstalled_at=8\n",
        )
        .unwrap();
        let candidates = [
            conversion_candidate("AAAAAAAAAAAAAAAAAAAAA", "/old/a"),
            conversion_candidate("BBBBBBBBBBBBBBBBBBBBB", "/old/b"),
        ];

        let first = ensure_skills_lock_v2(path.as_path(), &candidates).unwrap();
        assert_eq!(first.entries.len(), 1);
        assert_eq!(first.entries[0].skill_id.as_str(), "BBBBBBBBBBBBBBBBBBBBB");
        assert_eq!(first.entries[0].installed_at, 7);
        assert_eq!(first.entries[0].version.as_deref(), Some("2.0.0"));
        let second = ensure_skills_lock_v2(path.as_path(), &candidates).unwrap();
        assert_eq!(second, first);
        assert_eq!(read_skills_lock(path.as_path()).unwrap(), first);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn legacy_lock_conversion_drops_unmatched_or_ambiguous_entries() {
        let path = temp_file("convert-stale");
        fs::write(
            path.as_path(),
            "version = 1\n[[entries]]\nowner='registry'\nslug='alpha'\nsource_kind='registry'\nsource_ref='old'\ninstall_path='/missing'\ntrust_level='verified'\nfingerprint='old-fp'\ninstalled_at=7\n",
        )
        .unwrap();
        let candidates = [
            conversion_candidate("AAAAAAAAAAAAAAAAAAAAA", "/old/a"),
            conversion_candidate("BBBBBBBBBBBBBBBBBBBBB", "/old/b"),
        ];

        let converted = ensure_skills_lock_v2(path.as_path(), &candidates).unwrap();
        assert!(converted.entries.is_empty());
        assert_eq!(read_skills_lock(path.as_path()).unwrap().version, 2);

        let _ = fs::remove_file(path);
    }
}
