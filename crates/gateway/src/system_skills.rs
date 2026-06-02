use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

struct BundledSystemSkillFile {
    relative_path: &'static str,
    bytes: &'static [u8],
    unix_mode: u32,
}

include!(concat!(env!("OUT_DIR"), "/bundled_system_skills.rs"));

pub(crate) fn materialize_bundled_system_skill_roots(runtime_home: &Path) -> Result<Vec<String>> {
    if BUNDLED_SYSTEM_SKILL_FILES.is_empty() {
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
    let mut hasher = Sha256::new();
    for file in BUNDLED_SYSTEM_SKILL_FILES {
        hasher.update(file.relative_path.as_bytes());
        hasher.update([0]);
        hasher.update(file.bytes);
        hasher.update([0]);
    }
    hex::encode(hasher.finalize())
}

fn materialize_bundle_root(root: &Path) -> Result<()> {
    for file in BUNDLED_SYSTEM_SKILL_FILES {
        let target = root.join(file.relative_path);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory {}", parent.display()))?;
        }
        fs::write(target.as_path(), file.bytes)
            .with_context(|| format!("failed to write {}", target.display()))?;
        set_file_mode(target.as_path(), file.unix_mode)
            .with_context(|| format!("failed to set permissions for {}", target.display()))?;
    }
    Ok(())
}

fn validate_bundle_root(root: &Path) -> bool {
    if !root.is_dir() {
        return false;
    }

    BUNDLED_SYSTEM_SKILL_FILES.iter().all(|file| {
        let path = root.join(file.relative_path);
        let bytes_match = match fs::read(path.as_path()) {
            Ok(bytes) => bytes == file.bytes,
            Err(_) => return false,
        };
        bytes_match && file_mode_matches(path.as_path(), file.unix_mode)
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
        bundled_system_skills_hash, is_bundle_hash_dir_name, is_bundle_staging_dir_name,
        materialize_bundled_system_skill_roots,
    };
    use pioneer_skills::{
        ResolvedSkill, SkillCatalogLoadParams, SkillImplicitInvocationPolicy, SkillPromptBudget,
        SkillResolvedReason, SkillRuntimeBudget, SkillSourceKind, SkillTrustLevel,
        build_skill_prompt, build_skill_runtime_plan, load_catalog,
    };

    #[test]
    fn materializes_bundled_browser_and_subagents_skills() {
        let runtime_home = tempfile::tempdir().expect("runtime home");
        let roots = materialize_bundled_system_skill_roots(runtime_home.path())
            .expect("materialize bundled system skills");

        assert_eq!(roots.len(), 1);
        let root = std::path::PathBuf::from(&roots[0]);
        let skill_file = root.join("pioneer/browser/SKILL.md");
        assert!(skill_file.is_file());
        assert!(
            std::fs::read_to_string(skill_file.as_path())
                .expect("read system skill")
                .contains("slug: browser")
        );

        let catalog = load_catalog(&SkillCatalogLoadParams {
            system_roots: vec![root.clone()],
            user_roots: Vec::new(),
            registry_roots: Vec::new(),
            max_skills_per_source: 16,
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
        assert_eq!(browser.identity.owner, "pioneer");

        let root_canonical = std::fs::canonicalize(root.as_path()).expect("canonicalize root");
        let expected_asset_root = root_canonical.join("pioneer/browser").display().to_string();
        assert_eq!(browser.identity.skill_dir, expected_asset_root);

        let active = vec![ResolvedSkill {
            slug: "pioneer/browser".to_owned(),
            reason: SkillResolvedReason::ExplicitCapability,
            definition: browser.clone(),
        }];
        let prompt = build_skill_prompt(
            active.as_slice(),
            SkillPromptBudget {
                max_chars: 4_000,
                compact_mode_threshold: 6,
                include_read_skill_hint: true,
            },
        );
        assert!(
            prompt
                .text
                .contains("Skill slug for read_skill: `system:pioneer/browser`")
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
                .get("system:pioneer/browser")
                .expect("system browser read_skill entry")
                .skill_asset_root,
            expected_asset_root
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
        assert_eq!(subagents.identity.owner, "pioneer");
        assert_eq!(
            subagents.policy_hints.implicit_invocation,
            SkillImplicitInvocationPolicy::Required
        );
        assert!(subagents.policy_hints.catalog_hidden);

        let expected_subagents_asset_root = root_canonical
            .join("pioneer/subagents")
            .display()
            .to_string();
        let subagents_active = vec![ResolvedSkill {
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
            !subagents_prompt.text.contains("system:pioneer/subagents"),
            "catalog-hidden subagents skill should not appear in the ordinary skills prompt"
        );

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
                .get("system:pioneer/subagents")
                .expect("system subagents read_skill entry")
                .skill_asset_root,
            expected_subagents_asset_root
        );
    }

    #[test]
    fn materializes_all_regular_bundle_files() {
        let runtime_home = tempfile::tempdir().expect("runtime home");
        let roots = materialize_bundled_system_skill_roots(runtime_home.path())
            .expect("materialize bundled system skills");
        let root = std::path::PathBuf::from(&roots[0]);

        for file in super::BUNDLED_SYSTEM_SKILL_FILES {
            let path = root.join(file.relative_path);
            assert!(path.is_file(), "{} should materialize", file.relative_path);
            assert_eq!(
                std::fs::read(path.as_path()).expect("read materialized file"),
                file.bytes,
                "{} should materialize exact bytes",
                file.relative_path
            );
            assert!(
                super::file_mode_matches(path.as_path(), file.unix_mode),
                "{} should preserve mode",
                file.relative_path
            );
        }
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
