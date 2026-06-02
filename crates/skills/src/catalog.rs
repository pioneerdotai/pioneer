use crate::compile::SkillDefinition;
use crate::contract::{
    SkillCatalogSnapshot, SkillSourceKind, parse_skill_from_file, qualified_skill_slug,
};
use anyhow::Result;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct SkillCatalogLoadParams {
    pub system_roots: Vec<PathBuf>,
    pub user_roots: Vec<PathBuf>,
    pub registry_roots: Vec<PathBuf>,
    pub max_skills_per_source: usize,
    pub max_file_bytes: usize,
}

fn now_unix_seconds() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

fn now_version() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0,
    }
}

fn canonicalize_optional(path: &Path) -> Option<PathBuf> {
    fs::canonicalize(path).ok()
}

fn is_within(root: &Path, candidate: &Path) -> bool {
    let root_components = root.components().collect::<Vec<_>>();
    let candidate_components = candidate.components().collect::<Vec<_>>();
    if candidate_components.len() < root_components.len() {
        return false;
    }
    root_components
        .iter()
        .zip(candidate_components.iter())
        .all(|(left, right)| left == right)
}

fn try_parse_skill_dir(
    skills: &mut Vec<SkillDefinition>,
    max_skills: usize,
    root_canonical: &Path,
    skill_dir_canonical: PathBuf,
    source_kind: &SkillSourceKind,
    max_file_bytes: usize,
) {
    if skills.len() >= max_skills {
        return;
    }

    let skill_file = skill_dir_canonical.join("SKILL.md");
    if !skill_file.exists() {
        return;
    }

    let Some(skill_file_canonical) = canonicalize_optional(skill_file.as_path()) else {
        return;
    };

    if !is_within(
        skill_dir_canonical.as_path(),
        skill_file_canonical.as_path(),
    ) {
        return;
    }

    match parse_skill_from_file(
        skill_file_canonical.as_path(),
        source_kind.clone(),
        root_canonical,
        max_file_bytes,
    ) {
        Ok(skill) => {
            let strict_error_count = skill
                .conformance
                .agentskills_strict
                .issues
                .iter()
                .filter(|issue| matches!(issue.level, crate::validation::IssueLevel::Error))
                .count();
            let openclaw_error_count = skill
                .conformance
                .openclaw_compat
                .issues
                .iter()
                .filter(|issue| matches!(issue.level, crate::validation::IssueLevel::Error))
                .count();

            if strict_error_count > 0 || openclaw_error_count > 0 {
                tracing::warn!(
                    slug = %skill.identity.slug,
                    source = %skill.identity.source_kind.as_db_value(),
                    strict_errors = strict_error_count,
                    openclaw_errors = openclaw_error_count,
                    path = %skill_file_canonical.display(),
                    "skill loaded with conformance issues"
                );
            }

            skills.push(skill);
        }
        Err(error) => {
            tracing::warn!(
                path = %skill_file_canonical.display(),
                error = %format!("{error:#}"),
                "failed to parse skill"
            );
        }
    }
}

fn catalog_key(skill: &SkillDefinition) -> String {
    format!(
        "{}:{}",
        skill.identity.source_kind.as_db_value(),
        qualified_skill_slug(skill.identity.owner.as_str(), skill.identity.slug.as_str())
    )
}

fn scan_root(
    root: &Path,
    source_kind: SkillSourceKind,
    max_skills: usize,
    max_file_bytes: usize,
) -> Vec<SkillDefinition> {
    let Some(root_canonical) = canonicalize_optional(root) else {
        return Vec::new();
    };

    let Ok(owner_entries) = fs::read_dir(root_canonical.as_path()) else {
        return Vec::new();
    };

    let mut skills = Vec::new();

    for owner_entry in owner_entries.flatten() {
        if skills.len() >= max_skills {
            break;
        }

        let Ok(owner_type) = owner_entry.file_type() else {
            continue;
        };
        if !owner_type.is_dir() {
            continue;
        }

        let owner_dir = owner_entry.path();

        let Some(owner_dir_canonical) = canonicalize_optional(owner_dir.as_path()) else {
            continue;
        };

        if !is_within(root_canonical.as_path(), owner_dir_canonical.as_path()) {
            continue;
        }

        let Ok(skill_entries) = fs::read_dir(owner_dir_canonical.as_path()) else {
            continue;
        };

        for skill_entry in skill_entries.flatten() {
            if skills.len() >= max_skills {
                break;
            }

            let Ok(skill_type) = skill_entry.file_type() else {
                continue;
            };
            if !skill_type.is_dir() {
                continue;
            }

            let skill_dir = skill_entry.path();

            let Some(skill_dir_canonical) = canonicalize_optional(skill_dir.as_path()) else {
                continue;
            };

            if !is_within(owner_dir_canonical.as_path(), skill_dir_canonical.as_path()) {
                continue;
            }

            try_parse_skill_dir(
                &mut skills,
                max_skills,
                root_canonical.as_path(),
                skill_dir_canonical,
                &source_kind,
                max_file_bytes,
            );
        }
    }

    skills
}

pub fn load_catalog(params: &SkillCatalogLoadParams) -> Result<SkillCatalogSnapshot> {
    let max_skills = params.max_skills_per_source.max(1);
    let max_file_bytes = params.max_file_bytes.max(1);

    let mut merged: HashMap<String, SkillDefinition> = HashMap::new();

    for root in &params.system_roots {
        for skill in scan_root(
            root.as_path(),
            SkillSourceKind::System,
            max_skills,
            max_file_bytes,
        ) {
            merged.insert(catalog_key(&skill), skill);
        }
    }

    for root in &params.registry_roots {
        for skill in scan_root(
            root.as_path(),
            SkillSourceKind::Registry,
            max_skills,
            max_file_bytes,
        ) {
            merged.insert(catalog_key(&skill), skill);
        }
    }

    for root in &params.user_roots {
        for skill in scan_root(
            root.as_path(),
            SkillSourceKind::User,
            max_skills,
            max_file_bytes,
        ) {
            merged.insert(catalog_key(&skill), skill);
        }
    }

    let mut skills = merged.into_values().collect::<Vec<_>>();

    skills.sort_by(|left, right| {
        left.identity
            .source_kind
            .as_db_value()
            .cmp(right.identity.source_kind.as_db_value())
            .then_with(|| {
                qualified_skill_slug(left.identity.owner.as_str(), left.identity.slug.as_str()).cmp(
                    &qualified_skill_slug(
                        right.identity.owner.as_str(),
                        right.identity.slug.as_str(),
                    ),
                )
            })
    });

    Ok(SkillCatalogSnapshot {
        version: now_version(),
        generated_at_unix: now_unix_seconds(),
        skills,
    })
}

#[cfg(test)]
mod tests {
    use super::{SkillCatalogLoadParams, load_catalog};
    use std::fs;

    fn temp_case(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("unix time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pioneer-skills-{name}-{nanos}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[test]
    fn same_slug_across_sources_coexist() {
        let system = temp_case("system");
        let user = temp_case("user");
        let registry = temp_case("registry");

        let write_skill = |root: &std::path::Path, name: &str, description: &str| {
            let dir = root.join("acme").join("shared-skill");
            fs::create_dir_all(&dir).expect("create skill dir");
            fs::write(
                dir.join("SKILL.md"),
                format!(
                    "---\nname: {name}\nslug: shared-skill\ndescription: {description}\n---\nInstructions"
                ),
            )
            .expect("write skill");
        };

        write_skill(system.as_path(), "System", "System version");
        write_skill(registry.as_path(), "Registry", "Registry version");
        write_skill(user.as_path(), "User", "User version");

        let snapshot = load_catalog(&SkillCatalogLoadParams {
            system_roots: vec![system.clone()],
            user_roots: vec![user.clone()],
            registry_roots: vec![registry.clone()],
            max_skills_per_source: 32,
            max_file_bytes: 64 * 1024,
        })
        .expect("load catalog");

        assert_eq!(snapshot.skills.len(), 3);
        assert!(snapshot.skills.iter().any(|skill| {
            skill.identity.name == "System"
                && matches!(
                    skill.identity.source_kind,
                    crate::contract::SkillSourceKind::System
                )
        }));
        assert!(snapshot.skills.iter().any(|skill| {
            skill.identity.name == "Registry"
                && matches!(
                    skill.identity.source_kind,
                    crate::contract::SkillSourceKind::Registry
                )
        }));
        assert!(snapshot.skills.iter().any(|skill| {
            skill.identity.name == "User"
                && matches!(
                    skill.identity.source_kind,
                    crate::contract::SkillSourceKind::User
                )
        }));

        let _ = fs::remove_dir_all(system);
        let _ = fs::remove_dir_all(user);
        let _ = fs::remove_dir_all(registry);
    }

    #[test]
    fn later_root_overrides_same_source_slug() {
        let first = temp_case("first-system-root");
        let second = temp_case("second-system-root");

        let write_skill = |root: &std::path::Path, name: &str, description: &str| {
            let dir = root.join("acme").join("shared-skill");
            fs::create_dir_all(&dir).expect("create skill dir");
            fs::write(
                dir.join("SKILL.md"),
                format!(
                    "---\nname: {name}\nslug: shared-skill\ndescription: {description}\n---\nInstructions"
                ),
            )
            .expect("write skill");
        };

        write_skill(first.as_path(), "First", "First version");
        write_skill(second.as_path(), "Second", "Second version");

        let snapshot = load_catalog(&SkillCatalogLoadParams {
            system_roots: vec![first.clone(), second.clone()],
            user_roots: Vec::new(),
            registry_roots: Vec::new(),
            max_skills_per_source: 32,
            max_file_bytes: 64 * 1024,
        })
        .expect("load catalog");

        assert_eq!(snapshot.skills.len(), 1);
        assert_eq!(snapshot.skills[0].identity.name, "Second");
        assert_eq!(
            snapshot.skills[0].identity.source_root,
            fs::canonicalize(second.as_path())
                .expect("canonicalize second root")
                .display()
                .to_string()
        );

        let _ = fs::remove_dir_all(first);
        let _ = fs::remove_dir_all(second);
    }
}
