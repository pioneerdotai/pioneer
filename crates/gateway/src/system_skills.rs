use anyhow::{Context, Result, bail};
use pioneer_protocol::SkillId;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

struct BundledSystemSkillFile {
    relative_path: &'static str,
    bytes: &'static [u8],
    unix_mode: u32,
}

struct BundledSystemSkillRecord {
    skill_id: &'static str,
    owner: &'static str,
    slug: &'static str,
    resource_path: &'static str,
    files: &'static [BundledSystemSkillFile],
}

include!(concat!(env!("OUT_DIR"), "/bundled_system_skills.rs"));

pub(crate) fn bundled_system_skill_catalog_entries(
    materialized_root: &Path,
) -> Result<Vec<pioneer_skills::BundledSkillCatalogEntry>> {
    BUNDLED_SYSTEM_SKILLS
        .iter()
        .map(|entry| {
            let skill_id = SkillId::new(entry.skill_id).map_err(anyhow::Error::new)?;
            Ok(pioneer_skills::BundledSkillCatalogEntry {
                skill_id,
                owner: Some(entry.owner.to_owned()),
                slug: entry.slug.to_owned(),
                source_root: materialized_root.to_path_buf(),
                install_path: materialized_root.join(entry.skill_id).join(entry.slug),
            })
        })
        .collect()
}

pub(crate) fn materialize_bundled_system_skill_roots(runtime_home: &Path) -> Result<Vec<String>> {
    if BUNDLED_SYSTEM_SKILLS.is_empty() {
        return Ok(Vec::new());
    }

    let base = runtime_home.join("skills").join("system");
    let bundle_hash = bundled_system_skills_hash();
    let root = base.join(bundle_hash.as_str());

    if root.exists() && validate_bundle_root(root.as_path()) {
        cleanup_old_bundle_roots(base.as_path(), bundle_hash.as_str());
        return Ok(vec![root.display().to_string()]);
    }

    if root.exists() {
        remove_path(root.as_path()).with_context(|| {
            format!(
                "failed to remove invalid bundled system skills root {}",
                root.display()
            )
        })?;
    }

    fs::create_dir_all(base.as_path()).with_context(|| {
        format!(
            "failed to create bundled system skills base {}",
            base.display()
        )
    })?;

    let staging = base.join(format!(
        ".staging-{bundle_hash}-{}",
        materialization_unique_suffix()
    ));
    if staging.exists() {
        remove_path(staging.as_path()).with_context(|| {
            format!(
                "failed to remove stale bundled system skills staging directory {}",
                staging.display()
            )
        })?;
    }

    materialize_bundle_root(staging.as_path()).with_context(|| {
        format!(
            "failed to materialize bundled system skills into {}",
            staging.display()
        )
    })?;

    if !validate_bundle_root(staging.as_path()) {
        let _ = remove_path(staging.as_path());
        bail!("materialized bundled system skills failed validation");
    }

    match fs::rename(staging.as_path(), root.as_path()) {
        Ok(_) => {}
        Err(error) if root.exists() && validate_bundle_root(root.as_path()) => {
            let _ = remove_path(staging.as_path());
            tracing::warn!(
                error = %format!("{error:#}"),
                root = %root.display(),
                "bundled system skills root was created concurrently"
            );
        }
        Err(error) => {
            let _ = remove_path(staging.as_path());
            return Err(error).with_context(|| {
                format!(
                    "failed to publish bundled system skills root {}",
                    root.display()
                )
            });
        }
    }

    cleanup_old_bundle_roots(base.as_path(), bundle_hash.as_str());
    Ok(vec![root.display().to_string()])
}

fn bundled_system_skills_hash() -> String {
    bundled_system_skills_hash_with_manifest(BUNDLED_SKILLS_MANIFEST_BYTES)
}

fn bundled_system_skills_hash_with_manifest(manifest_bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(manifest_bytes);
    hasher.update([0]);
    for skill in BUNDLED_SYSTEM_SKILLS {
        hasher.update(skill.skill_id.as_bytes());
        hasher.update([0]);
        hasher.update(skill.owner.as_bytes());
        hasher.update([0]);
        hasher.update(skill.slug.as_bytes());
        hasher.update([0]);
        hasher.update(skill.resource_path.as_bytes());
        hasher.update([0]);
        for file in skill.files {
            hasher.update(file.relative_path.as_bytes());
            hasher.update([0]);
            hasher.update(file.bytes);
            hasher.update([0]);
        }
    }
    hex::encode(hasher.finalize())
}

fn materialize_bundle_root(root: &Path) -> Result<()> {
    for skill in BUNDLED_SYSTEM_SKILLS {
        let package_root = root.join(skill.skill_id).join(skill.slug);
        for file in skill.files {
            let target = package_root.join(file.relative_path);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create directory {}", parent.display()))?;
            }
            fs::write(target.as_path(), file.bytes)
                .with_context(|| format!("failed to write {}", target.display()))?;
            set_file_mode(target.as_path(), file.unix_mode)
                .with_context(|| format!("failed to set permissions for {}", target.display()))?;
        }
    }
    Ok(())
}

fn validate_bundle_root(root: &Path) -> bool {
    if !root.is_dir() {
        return false;
    }

    BUNDLED_SYSTEM_SKILLS.iter().all(|skill| {
        skill.files.iter().all(|file| {
            let path = root
                .join(skill.skill_id)
                .join(skill.slug)
                .join(file.relative_path);
            let bytes_match = match fs::read(path.as_path()) {
                Ok(bytes) => bytes == file.bytes,
                Err(_) => return false,
            };
            bytes_match && file_mode_matches(path.as_path(), file.unix_mode)
        })
    })
}

#[cfg(unix)]
fn set_file_mode(path: &Path, mode: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(mode & 0o777))
}

#[cfg(not(unix))]
fn set_file_mode(_path: &Path, _mode: u32) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn file_mode_matches(path: &Path, expected_mode: u32) -> bool {
    use std::os::unix::fs::PermissionsExt;

    fs::metadata(path)
        .map(|metadata| metadata.permissions().mode() & 0o777 == expected_mode & 0o777)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn file_mode_matches(_path: &Path, _expected_mode: u32) -> bool {
    true
}

fn cleanup_old_bundle_roots(base: &Path, active_hash: &str) {
    let Ok(entries) = fs::read_dir(base) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name == active_hash {
            continue;
        }
        if !is_bundle_hash_dir_name(name) && !is_bundle_staging_dir_name(name) {
            continue;
        }

        if let Err(error) = fs::remove_dir_all(path.as_path()) {
            tracing::warn!(
                error = %format!("{error:#}"),
                path = %path.display(),
                "failed to clean up old bundled system skills root"
            );
        }
    }
}

fn remove_path(path: &Path) -> std::io::Result<()> {
    if path.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}

fn materialization_unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("{}-{nanos}", std::process::id())
}

fn is_bundle_hash_dir_name(name: &str) -> bool {
    name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_bundle_staging_dir_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix(".staging-") else {
        return false;
    };
    let Some((hash, suffix)) = rest.split_once('-') else {
        return false;
    };
    !suffix.is_empty() && is_bundle_hash_dir_name(hash)
}

#[cfg(test)]
mod tests {
    use super::{
        BUNDLED_SKILLS_MANIFEST_BYTES, BUNDLED_SYSTEM_SKILLS, bundled_system_skill_catalog_entries,
        bundled_system_skills_hash, bundled_system_skills_hash_with_manifest,
        is_bundle_hash_dir_name, is_bundle_staging_dir_name,
        materialize_bundled_system_skill_roots,
    };
    use pioneer_protocol::CLIAgentRuntimeKind;
    use pioneer_skills::{
        ResolvedSkill, SkillCatalogLoadParams, SkillImplicitInvocationPolicy, SkillPromptBudget,
        SkillResolvedReason, SkillRuntimeBudget, SkillSourceKind, SkillTrustLevel,
        build_skill_prompt, build_skill_runtime_plan, load_catalog,
    };

    #[test]
    fn materializes_bundled_browser_subagents_tasks_and_memory_skills() {
        let runtime_home = tempfile::tempdir().expect("runtime home");
        let roots = materialize_bundled_system_skill_roots(runtime_home.path())
            .expect("materialize bundled system skills");

        assert_eq!(roots.len(), 1);
        let root = std::path::PathBuf::from(&roots[0]);
        let browser_id = "24ZiAJnkBQ3WtGYx7XGeh";
        let skill_file = root.join(browser_id).join("browser/SKILL.md");
        assert!(skill_file.is_file());
        assert!(
            std::fs::read_to_string(skill_file.as_path())
                .expect("read system skill")
                .contains("slug: browser")
        );

        let catalog = load_catalog(&SkillCatalogLoadParams {
            installations: Vec::new(),
            bundled: bundled_system_skill_catalog_entries(root.as_path())
                .expect("load bundled manifest entries"),
            max_file_bytes: 1024 * 1024,
        })
        .expect("load catalog");

        let browser = catalog
            .skills
            .iter()
            .find(|skill| skill.identity.slug == "browser")
            .expect("bundled browser skill should load");
        assert!(matches!(
            browser.identity.source_kind,
            SkillSourceKind::System
        ));
        assert_eq!(browser.identity.owner.as_deref(), Some("pioneer"));

        let expected_asset_root = root.join(browser_id).join("browser").display().to_string();
        assert_eq!(browser.identity.skill_dir, expected_asset_root);

        let active = vec![ResolvedSkill {
            skill_id: browser.identity.skill_id.clone(),
            slug: "pioneer/browser".to_owned(),
            reason: SkillResolvedReason::ExplicitCapability,
            definition: browser.clone(),
        }];
        assert!(
            crate::cli_runtime::skills::ensure_cli_runtime_skills_exportable(active.as_slice())
                .is_ok()
        );
        for runtime_kind in [CLIAgentRuntimeKind::Codex, CLIAgentRuntimeKind::Claude] {
            assert!(
                crate::cli_runtime::skills::ensure_cli_runtime_skill_invocation_eligible(
                    runtime_kind,
                    "runtime",
                    active.as_slice(),
                )
                .is_ok()
            );
        }
        let prompt = build_skill_prompt(
            active.as_slice(),
            SkillPromptBudget {
                max_chars: 4_000,
                compact_mode_threshold: 6,
                include_read_skill_hint: true,
            },
        );
        assert!(
            prompt.text.contains(
                format!(
                    "Exact skill reference for read_skill: `skill:{}`",
                    browser.identity.skill_id
                )
                .as_str()
            )
        );
        assert!(
            prompt
                .text
                .contains(format!("Skill asset root: `{expected_asset_root}`").as_str())
        );

        let runtime_plan = build_skill_runtime_plan(
            active.as_slice(),
            SkillRuntimeBudget {
                enable_dynamic_tools: false,
                max_dynamic_tools_per_skill: 16,
                allow_shell_tools: false,
                allow_http_tools: false,
                allow_function_proxy_tools: false,
                allow_untrusted_install: false,
                min_trust_for_shell_tools: SkillTrustLevel::Verified,
                min_trust_for_http_tools: SkillTrustLevel::Community,
                min_trust_for_function_proxy_tools: SkillTrustLevel::Community,
            },
        );
        assert_eq!(
            runtime_plan
                .read_skill_index
                .get(format!("skill:{}", browser.identity.skill_id).as_str())
                .expect("system browser read_skill entry")
                .source
                .package_asset_root(),
            Some(expected_asset_root.as_str())
        );

        let subagents = catalog
            .skills
            .iter()
            .find(|skill| skill.identity.slug == "subagents")
            .expect("bundled subagents skill should load");
        assert!(matches!(
            subagents.identity.source_kind,
            SkillSourceKind::System
        ));
        assert_eq!(subagents.identity.owner.as_deref(), Some("pioneer"));
        assert_eq!(
            subagents.policy_hints.implicit_invocation,
            SkillImplicitInvocationPolicy::Required
        );
        assert!(subagents.policy_hints.catalog_hidden);
        let required = [ResolvedSkill {
            skill_id: subagents.identity.skill_id.clone(),
            slug: "pioneer/subagents".to_owned(),
            reason: SkillResolvedReason::ExplicitCapability,
            definition: subagents.clone(),
        }];
        assert!(
            crate::cli_runtime::skills::ensure_cli_runtime_skills_exportable(&required).is_err()
        );

        let expected_subagents_asset_root = root
            .join("7Ejrf61g4oqigkv3LMNE9")
            .join("subagents")
            .display()
            .to_string();
        let subagents_active = vec![ResolvedSkill {
            skill_id: subagents.identity.skill_id.clone(),
            slug: "pioneer/subagents".to_owned(),
            reason: SkillResolvedReason::Implicit,
            definition: subagents.clone(),
        }];
        let subagents_prompt = build_skill_prompt(
            subagents_active.as_slice(),
            SkillPromptBudget {
                max_chars: 4_000,
                compact_mode_threshold: 6,
                include_read_skill_hint: true,
            },
        );
        assert!(
            subagents_prompt
                .text
                .contains("[Internal Skill References]")
        );
        assert!(
            subagents_prompt
                .text
                .contains(format!("skill:{}", subagents.identity.skill_id).as_str())
        );
        assert!(!subagents_prompt.text.contains("Use when:"));

        let subagents_runtime_plan = build_skill_runtime_plan(
            subagents_active.as_slice(),
            SkillRuntimeBudget {
                enable_dynamic_tools: false,
                max_dynamic_tools_per_skill: 16,
                allow_shell_tools: false,
                allow_http_tools: false,
                allow_function_proxy_tools: false,
                allow_untrusted_install: false,
                min_trust_for_shell_tools: SkillTrustLevel::Verified,
                min_trust_for_http_tools: SkillTrustLevel::Community,
                min_trust_for_function_proxy_tools: SkillTrustLevel::Community,
            },
        );
        assert_eq!(
            subagents_runtime_plan
                .read_skill_index
                .get(format!("skill:{}", subagents.identity.skill_id).as_str())
                .expect("system subagents read_skill entry")
                .source
                .package_asset_root(),
            Some(expected_subagents_asset_root.as_str())
        );

        let tasks = catalog
            .skills
            .iter()
            .find(|skill| skill.identity.slug == "tasks")
            .expect("bundled tasks skill should load");
        assert!(matches!(
            tasks.identity.source_kind,
            SkillSourceKind::System
        ));
        assert_eq!(tasks.identity.owner.as_deref(), Some("pioneer"));
        assert_eq!(
            tasks.policy_hints.implicit_invocation,
            SkillImplicitInvocationPolicy::Required
        );
        assert!(tasks.policy_hints.catalog_hidden);

        let expected_tasks_asset_root = root
            .join("AzBYfaNgS6u1tiPaSlwnm")
            .join("tasks")
            .display()
            .to_string();
        let tasks_active = vec![ResolvedSkill {
            skill_id: tasks.identity.skill_id.clone(),
            slug: "pioneer/tasks".to_owned(),
            reason: SkillResolvedReason::Implicit,
            definition: tasks.clone(),
        }];
        let tasks_prompt = build_skill_prompt(
            tasks_active.as_slice(),
            SkillPromptBudget {
                max_chars: 4_000,
                compact_mode_threshold: 6,
                include_read_skill_hint: true,
            },
        );
        assert!(tasks_prompt.text.contains("[Internal Skill References]"));
        assert!(
            tasks_prompt
                .text
                .contains(format!("skill:{}", tasks.identity.skill_id).as_str())
        );
        assert!(!tasks_prompt.text.contains("Use when:"));

        let tasks_runtime_plan = build_skill_runtime_plan(
            tasks_active.as_slice(),
            SkillRuntimeBudget {
                enable_dynamic_tools: false,
                max_dynamic_tools_per_skill: 16,
                allow_shell_tools: false,
                allow_http_tools: false,
                allow_function_proxy_tools: false,
                allow_untrusted_install: false,
                min_trust_for_shell_tools: SkillTrustLevel::Verified,
                min_trust_for_http_tools: SkillTrustLevel::Community,
                min_trust_for_function_proxy_tools: SkillTrustLevel::Community,
            },
        );
        assert_eq!(
            tasks_runtime_plan
                .read_skill_index
                .get(format!("skill:{}", tasks.identity.skill_id).as_str())
                .expect("system tasks read_skill entry")
                .source
                .package_asset_root(),
            Some(expected_tasks_asset_root.as_str())
        );

        let memory = catalog
            .skills
            .iter()
            .find(|skill| skill.identity.slug == "memory")
            .expect("bundled memory skill should load");
        assert!(matches!(
            memory.identity.source_kind,
            SkillSourceKind::System
        ));
        assert_eq!(memory.identity.owner.as_deref(), Some("pioneer"));
        assert_eq!(
            memory.policy_hints.implicit_invocation,
            SkillImplicitInvocationPolicy::Required
        );
        assert!(memory.policy_hints.catalog_hidden);

        let expected_memory_asset_root = root
            .join("TYotuYDUMUbxl37lej8cv")
            .join("memory")
            .display()
            .to_string();
        let memory_active = vec![ResolvedSkill {
            skill_id: memory.identity.skill_id.clone(),
            slug: "pioneer/memory".to_owned(),
            reason: SkillResolvedReason::Implicit,
            definition: memory.clone(),
        }];
        let memory_prompt = build_skill_prompt(
            memory_active.as_slice(),
            SkillPromptBudget {
                max_chars: 4_000,
                compact_mode_threshold: 6,
                include_read_skill_hint: true,
            },
        );
        assert!(memory_prompt.text.contains("[Internal Skill References]"));
        assert!(
            memory_prompt
                .text
                .contains(format!("skill:{}", memory.identity.skill_id).as_str())
        );
        assert!(!memory_prompt.text.contains("Use when:"));

        let memory_runtime_plan = build_skill_runtime_plan(
            memory_active.as_slice(),
            SkillRuntimeBudget {
                enable_dynamic_tools: false,
                max_dynamic_tools_per_skill: 16,
                allow_shell_tools: false,
                allow_http_tools: false,
                allow_function_proxy_tools: false,
                allow_untrusted_install: false,
                min_trust_for_shell_tools: SkillTrustLevel::Verified,
                min_trust_for_http_tools: SkillTrustLevel::Community,
                min_trust_for_function_proxy_tools: SkillTrustLevel::Community,
            },
        );
        assert_eq!(
            memory_runtime_plan
                .read_skill_index
                .get(format!("skill:{}", memory.identity.skill_id).as_str())
                .expect("system memory read_skill entry")
                .source
                .package_asset_root(),
            Some(expected_memory_asset_root.as_str())
        );
    }

    #[test]
    fn materializes_all_regular_bundle_files() {
        let runtime_home = tempfile::tempdir().expect("runtime home");
        let roots = materialize_bundled_system_skill_roots(runtime_home.path())
            .expect("materialize bundled system skills");
        let root = std::path::PathBuf::from(&roots[0]);

        for skill in BUNDLED_SYSTEM_SKILLS {
            for file in skill.files {
                let path = root
                    .join(skill.skill_id)
                    .join(skill.slug)
                    .join(file.relative_path);
                assert!(path.is_file(), "{} should materialize", path.display());
                assert_eq!(
                    std::fs::read(path.as_path()).expect("read materialized file"),
                    file.bytes,
                    "{} should materialize exact bytes",
                    path.display()
                );
                assert!(
                    super::file_mode_matches(path.as_path(), file.unix_mode),
                    "{} should preserve mode",
                    path.display()
                );
            }
        }
        assert!(!root.join("bundled-system-skills.toml").exists());
    }

    #[test]
    fn bundled_manifest_drives_exact_stable_catalog_identity() {
        let expected = [
            (
                "24ZiAJnkBQ3WtGYx7XGeh",
                "pioneer",
                "browser",
                "pioneer/browser",
            ),
            (
                "TYotuYDUMUbxl37lej8cv",
                "pioneer",
                "memory",
                "pioneer/memory",
            ),
            (
                "7Ejrf61g4oqigkv3LMNE9",
                "pioneer",
                "subagents",
                "pioneer/subagents",
            ),
            ("AzBYfaNgS6u1tiPaSlwnm", "pioneer", "tasks", "pioneer/tasks"),
        ];
        let actual = BUNDLED_SYSTEM_SKILLS
            .iter()
            .map(|entry| (entry.skill_id, entry.owner, entry.slug, entry.resource_path))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);

        let first_root = std::path::Path::new("/bundle-revision-one");
        let second_root = std::path::Path::new("/bundle-revision-two");
        let first_ids = bundled_system_skill_catalog_entries(first_root)
            .expect("first revision entries")
            .into_iter()
            .map(|entry| entry.skill_id)
            .collect::<Vec<_>>();
        let second_ids = bundled_system_skill_catalog_entries(second_root)
            .expect("second revision entries")
            .into_iter()
            .map(|entry| entry.skill_id)
            .collect::<Vec<_>>();
        assert_eq!(first_ids, second_ids);
    }

    #[test]
    fn bundled_manifest_bytes_participate_in_bundle_hash() {
        let current = bundled_system_skills_hash();
        let mut changed_manifest = BUNDLED_SKILLS_MANIFEST_BYTES.to_vec();
        changed_manifest.extend_from_slice(b"\n# revision marker\n");
        assert_ne!(
            current,
            bundled_system_skills_hash_with_manifest(changed_manifest.as_slice())
        );
    }

    #[test]
    fn cleanup_only_removes_bundle_managed_directories() {
        let runtime_home = tempfile::tempdir().expect("runtime home");
        let base = runtime_home.path().join("skills").join("system");
        std::fs::create_dir_all(base.as_path()).expect("create system skills base");

        let active_hash = bundled_system_skills_hash();
        let old_hash = "0".repeat(64);
        assert!(is_bundle_hash_dir_name(old_hash.as_str()));
        assert!(is_bundle_staging_dir_name(
            format!(".staging-{old_hash}-test").as_str()
        ));

        let old_bundle = base.join(old_hash.as_str());
        let old_staging = base.join(format!(".staging-{old_hash}-test"));
        let custom_root = base.join("custom-system-root");
        std::fs::create_dir_all(old_bundle.as_path()).expect("create old bundle dir");
        std::fs::create_dir_all(old_staging.as_path()).expect("create old staging dir");
        std::fs::create_dir_all(custom_root.as_path()).expect("create custom dir");

        let roots = materialize_bundled_system_skill_roots(runtime_home.path())
            .expect("materialize bundled system skills");

        assert_eq!(roots.len(), 1);
        assert!(base.join(active_hash).is_dir());
        assert!(!old_bundle.exists());
        assert!(!old_staging.exists());
        assert!(custom_root.exists());
    }
}
