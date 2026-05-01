use crate::contract::{SkillTrustLevel, qualified_skill_slug};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

const LOCK_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillLockEntry {
    pub owner: String,
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

    if let Some(existing) = lock.entries.iter_mut().find(|candidate| {
        candidate.slug == entry.slug
            && candidate.source_kind == entry.source_kind
            && candidate.owner == entry.owner
    }) {
        *existing = entry;
    } else {
        lock.entries.push(entry);
    }

    lock.entries.sort_by(|left, right| {
        left.source_kind
            .as_str()
            .cmp(right.source_kind.as_str())
            .then_with(|| left.owner.as_str().cmp(right.owner.as_str()))
            .then_with(|| left.slug.as_str().cmp(right.slug.as_str()))
    });
}

pub fn remove_lock_entry(
    lock: &mut SkillsLock,
    slug: &str,
    source_kind: &str,
) -> Option<SkillLockEntry> {
    let index = lock.entries.iter().position(|entry| {
        qualified_skill_slug(entry.owner.as_str(), entry.slug.as_str()) == slug
            && entry.source_kind == source_kind
    })?;
    Some(lock.entries.remove(index))
}

pub fn find_lock_entry<'a>(
    lock: &'a SkillsLock,
    slug: &str,
    source_kind: &str,
) -> Option<&'a SkillLockEntry> {
    lock.entries.iter().find(|entry| {
        qualified_skill_slug(entry.owner.as_str(), entry.slug.as_str()) == slug
            && entry.source_kind == source_kind
    })
}

fn normalize_lock(mut lock: SkillsLock) -> SkillsLock {
    lock.entries.sort_by(|left, right| {
        left.source_kind
            .as_str()
            .cmp(right.source_kind.as_str())
            .then_with(|| left.owner.as_str().cmp(right.owner.as_str()))
            .then_with(|| left.slug.as_str().cmp(right.slug.as_str()))
    });
    lock.entries.dedup_by(|left, right| {
        left.slug == right.slug
            && left.source_kind == right.source_kind
            && left.owner == right.owner
    });
    lock
}

#[cfg(test)]
mod tests {
    use super::{
        SkillLockEntry, find_lock_entry, read_skills_lock, remove_lock_entry, upsert_lock_entry,
        write_skills_lock_atomic,
    };
    use crate::contract::SkillTrustLevel;
    use std::fs;

    fn temp_file(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("unix timestamp")
            .as_nanos();
        std::env::temp_dir().join(format!("pioneer-skills-lock-{name}-{nanos}.toml"))
    }

    fn sample_entry(
        owner: &str,
        slug: &str,
        source_kind: &str,
        fingerprint: &str,
    ) -> SkillLockEntry {
        SkillLockEntry {
            owner: owner.to_owned(),
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

    #[test]
    fn lock_roundtrip_and_upsert() {
        let path = temp_file("roundtrip");

        let mut lock = super::SkillsLock::default();
        upsert_lock_entry(
            &mut lock,
            sample_entry("registry", "alpha", "registry", "fp1"),
        );
        upsert_lock_entry(
            &mut lock,
            sample_entry("registry", "alpha", "registry", "fp2"),
        );
        upsert_lock_entry(
            &mut lock,
            sample_entry("registry", "beta", "registry", "fp3"),
        );

        write_skills_lock_atomic(path.as_path(), &lock).expect("write lock");
        let loaded = read_skills_lock(path.as_path()).expect("read lock");

        assert_eq!(loaded.entries.len(), 2);
        assert_eq!(
            find_lock_entry(&loaded, "registry/alpha", "registry")
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
            "version = 999\n[[entries]]\nowner='registry'\nslug='a'\nsource_kind='registry'\nsource_ref='x'\ninstall_path='/tmp/registry/a'\ntrust_level='verified'\nfingerprint='fp'\ninstalled_at=1\n",
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
    fn remove_entry_is_deterministic() {
        let mut lock = super::SkillsLock::default();
        upsert_lock_entry(&mut lock, sample_entry("registry", "a", "registry", "fp1"));
        upsert_lock_entry(&mut lock, sample_entry("registry", "b", "registry", "fp2"));

        let removed =
            remove_lock_entry(&mut lock, "registry/a", "registry").expect("entry removed");
        assert_eq!(removed.slug, "a");
        assert_eq!(lock.entries.len(), 1);
    }
}
