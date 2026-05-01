use crate::audit::{SkillAuditAction, SkillAuditDecision, SkillAuditEvent};
use crate::contract::{
    SkillSourceKind, parse_skill_from_file, qualified_skill_slug, split_qualified_skill_slug,
};
use crate::dependencies::{
    DependencyCheckInput, DependencyCheckResult, evaluate_skill_dependencies,
};
use crate::provenance::{
    SkillLockEntry, find_lock_entry, read_skills_lock, remove_lock_entry, upsert_lock_entry,
    write_skills_lock_atomic,
};
use crate::security::{SkillSecurityPolicy, ensure_install_path_contained, scan_skill_directory};
use anyhow::{Context, Result, bail};
use std::collections::VecDeque;
use std::fs;
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone)]
pub struct SkillInstallerPolicy {
    pub security: SkillSecurityPolicy,
    pub dependency_input: DependencyCheckInput,
    pub block_on_dependency_failures: bool,
    pub block_on_fingerprint_mismatch: bool,
}

impl Default for SkillInstallerPolicy {
    fn default() -> Self {
        Self {
            security: SkillSecurityPolicy::default(),
            dependency_input: DependencyCheckInput::baseline(),
            block_on_dependency_failures: true,
            block_on_fingerprint_mismatch: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InstallSkillRequest {
    pub source_kind: SkillSourceKind,
    pub source_ref: String,
    pub source_path: PathBuf,
    pub install_root: PathBuf,
    pub lock_path: PathBuf,
    pub now_unix: i64,
    pub policy: SkillInstallerPolicy,
}

#[derive(Debug, Clone)]
pub struct UpdateSkillRequest {
    pub source_kind: SkillSourceKind,
    pub source_ref: String,
    pub source_path: PathBuf,
    pub install_root: PathBuf,
    pub lock_path: PathBuf,
    pub expected_previous_fingerprint: Option<String>,
    pub now_unix: i64,
    pub policy: SkillInstallerPolicy,
}

#[derive(Debug, Clone)]
pub struct PrepareMaterializedSkillRequest {
    pub source_kind: SkillSourceKind,
    pub source_ref: String,
    pub materialized_source_path: PathBuf,
    pub policy: SkillInstallerPolicy,
}

#[derive(Debug, Clone)]
pub struct PreparedMaterializedSkill {
    pub definition: crate::compile::SkillDefinition,
    pub source_kind: SkillSourceKind,
    pub source_ref: String,
    pub source_path: PathBuf,
    pub dependency_report: DependencyCheckResult,
    pub security_report: crate::security::SecurityScanReport,
}

#[derive(Debug, Clone)]
pub struct CommitPreparedSkillRequest {
    pub operation: InstallOperation,
    pub prepared: PreparedMaterializedSkill,
    pub install_root: PathBuf,
    pub lock_path: PathBuf,
    pub expected_previous_fingerprint: Option<String>,
    pub now_unix: i64,
    pub policy: SkillInstallerPolicy,
}

#[derive(Debug, Clone)]
pub struct UninstallSkillRequest {
    pub slug: String,
    pub source_kind: SkillSourceKind,
    pub install_root: PathBuf,
    pub lock_path: PathBuf,
    pub now_unix: i64,
    pub policy: SkillInstallerPolicy,
}

#[derive(Debug, Clone)]
pub struct InstallSkillResult {
    pub definition: crate::compile::SkillDefinition,
    pub install_path: PathBuf,
    pub dependency_report: DependencyCheckResult,
    pub security_report: crate::security::SecurityScanReport,
    pub lock_entry: SkillLockEntry,
    pub audit_events: Vec<SkillAuditEvent>,
}

#[derive(Debug, Clone)]
pub struct UninstallSkillResult {
    pub removed_path: Option<PathBuf>,
    pub removed_lock_entry: Option<SkillLockEntry>,
    pub audit_events: Vec<SkillAuditEvent>,
}

pub fn install_skill(request: InstallSkillRequest) -> Result<InstallSkillResult> {
    cleanup_staging(request.install_root.as_path());

    let staging_dir = match stage_source(&request) {
        Ok(path) => path,
        Err(error) => {
            cleanup_staging(request.install_root.as_path());
            return Err(error);
        }
    };

    let install_result =
        install_from_staging(InstallOperation::Install, &request, staging_dir, None);

    if install_result.is_err() {
        cleanup_staging(request.install_root.as_path());
    }

    install_result
}

pub fn update_skill(request: UpdateSkillRequest) -> Result<InstallSkillResult> {
    cleanup_staging(request.install_root.as_path());

    let install_request = InstallSkillRequest {
        source_kind: request.source_kind.clone(),
        source_ref: request.source_ref.clone(),
        source_path: request.source_path.clone(),
        install_root: request.install_root.clone(),
        lock_path: request.lock_path.clone(),
        now_unix: request.now_unix,
        policy: request.policy.clone(),
    };

    let staging_dir = match stage_source(&install_request) {
        Ok(path) => path,
        Err(error) => {
            cleanup_staging(request.install_root.as_path());
            return Err(error);
        }
    };

    let install_result = install_from_staging(
        InstallOperation::Update,
        &install_request,
        staging_dir,
        request.expected_previous_fingerprint.clone(),
    );

    if install_result.is_err() {
        cleanup_staging(request.install_root.as_path());
    }

    install_result
}

pub fn uninstall_skill(request: UninstallSkillRequest) -> Result<UninstallSkillResult> {
    cleanup_staging(request.install_root.as_path());

    if split_qualified_skill_slug(request.slug.as_str()).is_none() {
        bail!("cannot uninstall skill: slug must use owner/slug");
    }

    let mut lock = read_skills_lock(request.lock_path.as_path())?;

    let source_kind = request.source_kind.as_db_value().to_owned();

    let lock_entry = find_lock_entry(&lock, request.slug.as_str(), source_kind.as_str())
        .cloned()
        .with_context(|| {
            format!(
                "cannot uninstall skill `{}` ({source_kind}): lock entry was not found",
                request.slug
            )
        })?;

    let resolved_path = PathBuf::from(lock_entry.install_path.as_str());

    let containment =
        ensure_install_path_contained(request.install_root.as_path(), resolved_path.as_path());
    if containment.has_blocking_findings() {
        bail!("uninstall blocked by containment policy");
    }

    let removed_path = if resolved_path.exists() {
        if resolved_path.is_dir() {
            fs::remove_dir_all(resolved_path.as_path()).with_context(|| {
                format!(
                    "failed to remove installed skill directory `{}`",
                    resolved_path.display()
                )
            })?;
        } else {
            fs::remove_file(resolved_path.as_path()).with_context(|| {
                format!(
                    "failed to remove installed skill path `{}`",
                    resolved_path.display()
                )
            })?;
        }
        Some(resolved_path.clone())
    } else {
        None
    };

    let removed_lock = remove_lock_entry(&mut lock, request.slug.as_str(), source_kind.as_str());
    write_skills_lock_atomic(request.lock_path.as_path(), &lock)?;

    let audit_event = SkillAuditEvent::uninstall(
        request.slug.clone(),
        source_kind.clone(),
        serde_json::json!({
            "install_path": resolved_path.display().to_string(),
            "was_present_on_disk": removed_path.is_some(),
            "had_lock_entry": removed_lock.is_some(),
        }),
        request.now_unix,
    );

    Ok(UninstallSkillResult {
        removed_path,
        removed_lock_entry: removed_lock,
        audit_events: vec![audit_event],
    })
}

#[derive(Debug, Clone, Copy)]
pub enum InstallOperation {
    Install,
    Update,
}

pub fn prepare_materialized_skill(
    request: PrepareMaterializedSkillRequest,
) -> Result<PreparedMaterializedSkill> {
    let source_root = request
        .materialized_source_path
        .parent()
        .unwrap_or(request.materialized_source_path.as_path())
        .to_path_buf();

    let security_report = scan_skill_directory(
        source_root.as_path(),
        request.materialized_source_path.as_path(),
        request.policy.security.max_install_file_bytes.max(1),
    );
    if security_report.has_blocking_findings() {
        bail!("install blocked by security scan findings");
    }

    let skill_file = request.materialized_source_path.join("SKILL.md");
    if !skill_file.is_file() {
        bail!(
            "staged skill `{}` is missing required SKILL.md",
            request.materialized_source_path.display()
        );
    }

    let definition = parse_skill_from_file(
        skill_file.as_path(),
        request.source_kind.clone(),
        source_root.as_path(),
        request.policy.security.max_install_file_bytes.max(1),
    )?;

    if !request.policy.security.allow_untrusted_install
        && matches!(
            definition.runtime.trust_level,
            crate::contract::SkillTrustLevel::Untrusted
        )
    {
        bail!(
            "untrusted skill `{}` is blocked by policy",
            definition.identity.slug
        );
    }

    let dependency_report =
        evaluate_skill_dependencies(&definition, &request.policy.dependency_input);

    if request.policy.block_on_dependency_failures && dependency_report.has_failures() {
        bail!(
            "install blocked by dependency failures for `{}`",
            definition.identity.slug
        );
    }

    Ok(PreparedMaterializedSkill {
        definition,
        source_kind: request.source_kind,
        source_ref: request.source_ref,
        source_path: request.materialized_source_path,
        dependency_report,
        security_report,
    })
}

pub fn commit_prepared_skill(request: CommitPreparedSkillRequest) -> Result<InstallSkillResult> {
    let prepared = request.prepared;
    let definition = prepared.definition.clone();
    let target_path = request
        .install_root
        .join(definition.identity.owner.as_str())
        .join(definition.identity.slug.as_str());

    fs::create_dir_all(request.install_root.as_path()).with_context(|| {
        format!(
            "failed to create install root `{}`",
            request.install_root.display()
        )
    })?;

    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create install parent directory `{}`",
                parent.display()
            )
        })?;
    }

    let containment =
        ensure_install_path_contained(request.install_root.as_path(), target_path.as_path());

    if containment.has_blocking_findings() {
        bail!(
            "target install path `{}` is outside install root",
            target_path.display()
        );
    }

    let mut lock = read_skills_lock(request.lock_path.as_path())?;

    let source_kind = prepared.source_kind.as_db_value().to_owned();

    let qualified_slug = qualified_skill_slug(
        definition.identity.owner.as_str(),
        definition.identity.slug.as_str(),
    );

    let previous_lock_entry =
        find_lock_entry(&lock, qualified_slug.as_str(), source_kind.as_str()).cloned();

    if let Some(expected) = request.expected_previous_fingerprint.as_deref()
        && let Some(previous) = &previous_lock_entry
        && previous.fingerprint != expected
    {
        if request.policy.block_on_fingerprint_mismatch {
            bail!(
                "update blocked: expected previous fingerprint `{expected}`, found `{}`",
                previous.fingerprint
            );
        }
    }

    match request.operation {
        InstallOperation::Install => {
            if target_path.exists() {
                bail!(
                    "skill `{}` already installed at `{}`",
                    definition.identity.slug,
                    target_path.display()
                );
            }
            fs::rename(prepared.source_path.as_path(), target_path.as_path()).with_context(
                || {
                    format!(
                        "failed to move staged skill into `{}`",
                        target_path.display()
                    )
                },
            )?;
        }
        InstallOperation::Update => {
            if !target_path.exists() {
                bail!(
                    "cannot update skill `{}` because it is not installed",
                    definition.identity.slug
                );
            }

            let backup_path = request.install_root.join(format!(
                ".backup-{}-{}",
                definition.identity.slug,
                unique_suffix()
            ));
            fs::rename(target_path.as_path(), backup_path.as_path()).with_context(|| {
                format!(
                    "failed to create backup for `{}` before update",
                    target_path.display()
                )
            })?;

            if let Err(error) = fs::rename(prepared.source_path.as_path(), target_path.as_path()) {
                let _ = fs::rename(backup_path.as_path(), target_path.as_path());
                return Err(error).with_context(|| {
                    format!(
                        "failed to move staged update into `{}`",
                        target_path.display()
                    )
                });
            }

            let _ = fs::remove_dir_all(backup_path.as_path());
        }
    }

    let lock_entry = SkillLockEntry {
        owner: definition.identity.owner.clone(),
        slug: definition.identity.slug.clone(),
        source_kind: source_kind.clone(),
        source_ref: prepared.source_ref.clone(),
        install_path: target_path.display().to_string(),
        version: definition.identity.version_hint.clone(),
        trust_level: definition.runtime.trust_level.clone(),
        fingerprint: definition.identity.fingerprint.clone(),
        installed_at: request.now_unix,
    };
    upsert_lock_entry(&mut lock, lock_entry.clone());
    write_skills_lock_atomic(request.lock_path.as_path(), &lock)?;

    let mut audit_events = Vec::new();

    match request.operation {
        InstallOperation::Install => audit_events.push(SkillAuditEvent::install(
            qualified_slug.clone(),
            source_kind.clone(),
            serde_json::json!({
                "fingerprint": definition.identity.fingerprint,
                "source_ref": prepared.source_ref,
                "install_path": target_path.display().to_string(),
                "dependency_failures": prepared.dependency_report.failing_diagnostics(),
                "security_decision": prepared.security_report.decision,
            }),
            request.now_unix,
        )),
        InstallOperation::Update => audit_events.push(SkillAuditEvent::update(
            qualified_slug.clone(),
            source_kind.clone(),
            serde_json::json!({
                "updated_from": previous_lock_entry.as_ref().map(|entry| entry.fingerprint.clone()),
                "updated_to": definition.identity.fingerprint,
                "source_ref": prepared.source_ref,
                "install_path": target_path.display().to_string(),
                "dependency_failures": prepared.dependency_report.failing_diagnostics(),
                "security_decision": prepared.security_report.decision,
            }),
            request.now_unix,
        )),
    }

    if let Some(expected) = request.expected_previous_fingerprint.as_deref()
        && let Some(previous) = previous_lock_entry
        && previous.fingerprint != expected
        && !request.policy.block_on_fingerprint_mismatch
    {
        audit_events.push(SkillAuditEvent::new(
            qualified_slug,
            source_kind,
            SkillAuditAction::SecurityWarn,
            SkillAuditDecision::Warning,
            Some("provenance.fingerprint_mismatch".to_owned()),
            serde_json::json!({
                "expected": expected,
                "actual": previous.fingerprint,
            }),
            request.now_unix,
        ));
    }

    Ok(InstallSkillResult {
        definition,
        install_path: target_path,
        dependency_report: prepared.dependency_report,
        security_report: prepared.security_report,
        lock_entry,
        audit_events,
    })
}

fn install_from_staging(
    operation: InstallOperation,
    request: &InstallSkillRequest,
    staging_dir: PathBuf,
    expected_previous_fingerprint: Option<String>,
) -> Result<InstallSkillResult> {
    let security_report = scan_skill_directory(
        request.install_root.as_path(),
        staging_dir.as_path(),
        request.policy.security.max_install_file_bytes.max(1),
    );
    if security_report.has_blocking_findings() {
        bail!("install blocked by security scan findings");
    }

    let skill_file = staging_dir.join("SKILL.md");
    if !skill_file.is_file() {
        bail!(
            "staged skill `{}` is missing required SKILL.md",
            staging_dir.display()
        );
    }

    let parse_source_root = staging_dir
        .parent()
        .unwrap_or(request.install_root.as_path())
        .to_path_buf();

    let definition = parse_skill_from_file(
        skill_file.as_path(),
        request.source_kind.clone(),
        parse_source_root.as_path(),
        request.policy.security.max_install_file_bytes.max(1),
    )?;

    if !request.policy.security.allow_untrusted_install
        && matches!(
            definition.runtime.trust_level,
            crate::contract::SkillTrustLevel::Untrusted
        )
    {
        bail!(
            "untrusted skill `{}` is blocked by policy",
            definition.identity.slug
        );
    }

    let dependency_report =
        evaluate_skill_dependencies(&definition, &request.policy.dependency_input);

    if request.policy.block_on_dependency_failures && dependency_report.has_failures() {
        bail!(
            "install blocked by dependency failures for `{}`",
            definition.identity.slug
        );
    }

    let target_path = request
        .install_root
        .join(definition.identity.owner.as_str())
        .join(definition.identity.slug.as_str());

    fs::create_dir_all(request.install_root.as_path()).with_context(|| {
        format!(
            "failed to create install root `{}`",
            request.install_root.display()
        )
    })?;

    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create install parent directory `{}`",
                parent.display()
            )
        })?;
    }

    let containment =
        ensure_install_path_contained(request.install_root.as_path(), target_path.as_path());

    if containment.has_blocking_findings() {
        bail!(
            "target install path `{}` is outside install root",
            target_path.display()
        );
    }

    let mut lock = read_skills_lock(request.lock_path.as_path())?;

    let source_kind = request.source_kind.as_db_value().to_owned();

    let qualified_slug = qualified_skill_slug(
        definition.identity.owner.as_str(),
        definition.identity.slug.as_str(),
    );

    let previous_lock_entry =
        find_lock_entry(&lock, qualified_slug.as_str(), source_kind.as_str()).cloned();

    if let Some(expected) = expected_previous_fingerprint.as_deref()
        && let Some(previous) = &previous_lock_entry
        && previous.fingerprint != expected
    {
        if request.policy.block_on_fingerprint_mismatch {
            bail!(
                "update blocked: expected previous fingerprint `{expected}`, found `{}`",
                previous.fingerprint
            );
        }
    }

    match operation {
        InstallOperation::Install => {
            if target_path.exists() {
                bail!(
                    "skill `{}` already installed at `{}`",
                    definition.identity.slug,
                    target_path.display()
                );
            }
            fs::rename(staging_dir.as_path(), target_path.as_path()).with_context(|| {
                format!(
                    "failed to move staged skill into `{}`",
                    target_path.display()
                )
            })?;
        }
        InstallOperation::Update => {
            if !target_path.exists() {
                bail!(
                    "cannot update skill `{}` because it is not installed",
                    definition.identity.slug
                );
            }

            let backup_path = request.install_root.join(format!(
                ".backup-{}-{}",
                definition.identity.slug,
                unique_suffix()
            ));
            fs::rename(target_path.as_path(), backup_path.as_path()).with_context(|| {
                format!(
                    "failed to create backup for `{}` before update",
                    target_path.display()
                )
            })?;

            if let Err(error) = fs::rename(staging_dir.as_path(), target_path.as_path()) {
                let _ = fs::rename(backup_path.as_path(), target_path.as_path());
                return Err(error).with_context(|| {
                    format!(
                        "failed to move staged update into `{}`",
                        target_path.display()
                    )
                });
            }

            let _ = fs::remove_dir_all(backup_path.as_path());
        }
    }

    let lock_entry = SkillLockEntry {
        owner: definition.identity.owner.clone(),
        slug: definition.identity.slug.clone(),
        source_kind: source_kind.clone(),
        source_ref: request.source_ref.clone(),
        install_path: target_path.display().to_string(),
        version: definition.identity.version_hint.clone(),
        trust_level: definition.runtime.trust_level.clone(),
        fingerprint: definition.identity.fingerprint.clone(),
        installed_at: request.now_unix,
    };
    upsert_lock_entry(&mut lock, lock_entry.clone());
    write_skills_lock_atomic(request.lock_path.as_path(), &lock)?;

    let mut audit_events = Vec::new();

    match operation {
        InstallOperation::Install => audit_events.push(SkillAuditEvent::install(
            qualified_slug.clone(),
            source_kind.clone(),
            serde_json::json!({
                "fingerprint": definition.identity.fingerprint,
                "source_ref": request.source_ref,
                "install_path": target_path.display().to_string(),
                "dependency_failures": dependency_report.failing_diagnostics(),
                "security_decision": security_report.decision,
            }),
            request.now_unix,
        )),
        InstallOperation::Update => audit_events.push(SkillAuditEvent::update(
            qualified_slug.clone(),
            source_kind.clone(),
            serde_json::json!({
                "updated_from": previous_lock_entry.as_ref().map(|entry| entry.fingerprint.clone()),
                "updated_to": definition.identity.fingerprint,
                "source_ref": request.source_ref,
                "install_path": target_path.display().to_string(),
                "dependency_failures": dependency_report.failing_diagnostics(),
                "security_decision": security_report.decision,
            }),
            request.now_unix,
        )),
    }

    if let Some(expected) = expected_previous_fingerprint.as_deref()
        && let Some(previous) = previous_lock_entry
        && previous.fingerprint != expected
        && !request.policy.block_on_fingerprint_mismatch
    {
        audit_events.push(SkillAuditEvent::new(
            qualified_slug,
            source_kind,
            SkillAuditAction::SecurityWarn,
            SkillAuditDecision::Warning,
            Some("provenance.fingerprint_mismatch".to_owned()),
            serde_json::json!({
                "expected": expected,
                "actual": previous.fingerprint,
            }),
            request.now_unix,
        ));
    }

    Ok(InstallSkillResult {
        definition,
        install_path: target_path,
        dependency_report,
        security_report,
        lock_entry,
        audit_events,
    })
}

fn stage_source(request: &InstallSkillRequest) -> Result<PathBuf> {
    let source_root = fs::canonicalize(request.source_path.as_path()).with_context(|| {
        format!(
            "failed to canonicalize source path `{}`",
            request.source_path.display()
        )
    })?;
    if !source_root.is_dir() {
        bail!(
            "skill source path `{}` must be a directory",
            source_root.display()
        );
    }

    let staging_root = request.install_root.join(".staging");
    fs::create_dir_all(staging_root.as_path())
        .with_context(|| format!("failed to create staging root `{}`", staging_root.display()))?;
    let staging_session_dir = staging_root.join(unique_suffix());
    fs::create_dir_all(staging_session_dir.as_path()).with_context(|| {
        format!(
            "failed to create staging directory `{}`",
            staging_session_dir.display()
        )
    })?;
    let source_dir_name = source_root
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("skill");
    let staged_skill_dir = staging_session_dir.join(source_dir_name);
    fs::create_dir_all(staged_skill_dir.as_path()).with_context(|| {
        format!(
            "failed to create staged skill directory `{}`",
            staged_skill_dir.display()
        )
    })?;

    copy_tree_secure(
        source_root.as_path(),
        staged_skill_dir.as_path(),
        request.policy.security.max_install_file_bytes.max(1),
        request.policy.security.max_install_archive_bytes.max(1),
    )?;

    Ok(staged_skill_dir)
}

fn copy_tree_secure(
    source_root: &Path,
    target_root: &Path,
    max_file_bytes: usize,
    max_total_bytes: usize,
) -> Result<()> {
    let mut queue = VecDeque::new();
    queue.push_back(PathBuf::new());
    let mut total_copied = 0usize;

    while let Some(relative) = queue.pop_front() {
        let source_dir = source_root.join(relative.as_path());
        let entries = fs::read_dir(source_dir.as_path()).with_context(|| {
            format!("failed to read source directory `{}`", source_dir.display())
        })?;

        for entry in entries.flatten() {
            let source_path = entry.path();
            let metadata = fs::symlink_metadata(source_path.as_path()).with_context(|| {
                format!("failed to read metadata for `{}`", source_path.display())
            })?;
            let name = entry.file_name();
            let rel_path = relative.join(name);
            reject_non_contained_relative_path(rel_path.as_path())?;
            let target_path = target_root.join(rel_path.as_path());

            if metadata.file_type().is_symlink() {
                bail!(
                    "installer blocked symlink `{}`; source archives must not contain symlinks",
                    source_path.display()
                );
            }

            if metadata.is_dir() {
                fs::create_dir_all(target_path.as_path()).with_context(|| {
                    format!(
                        "failed to create target directory `{}`",
                        target_path.display()
                    )
                })?;
                queue.push_back(rel_path);
                continue;
            }

            if metadata.is_file() {
                let file_size = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
                if file_size > max_file_bytes {
                    bail!(
                        "installer blocked `{}`: file size {} exceeds limit {}",
                        source_path.display(),
                        file_size,
                        max_file_bytes
                    );
                }
                total_copied = total_copied.saturating_add(file_size);
                if total_copied > max_total_bytes {
                    bail!(
                        "installer blocked source copy: total size {} exceeds limit {}",
                        total_copied,
                        max_total_bytes
                    );
                }

                if let Some(parent) = target_path.parent() {
                    fs::create_dir_all(parent).with_context(|| {
                        format!("failed to create parent directory `{}`", parent.display())
                    })?;
                }
                fs::copy(source_path.as_path(), target_path.as_path()).with_context(|| {
                    format!(
                        "failed to copy `{}` to `{}`",
                        source_path.display(),
                        target_path.display()
                    )
                })?;
            }
        }
    }

    Ok(())
}

fn reject_non_contained_relative_path(path: &Path) -> Result<()> {
    if path.is_absolute() {
        bail!("installer blocked absolute path entry `{}`", path.display());
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        bail!("installer blocked traversal entry `{}`", path.display());
    }
    Ok(())
}

fn cleanup_staging(install_root: &Path) {
    let staging = install_root.join(".staging");
    if !staging.exists() {
        return;
    }
    let _ = fs::remove_dir_all(staging);
}

fn unique_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_nanos())
        .unwrap_or_default();
    format!("stage-{nanos}")
}

#[cfg(test)]
mod tests {
    use super::{
        InstallSkillRequest, SkillInstallerPolicy, UninstallSkillRequest, UpdateSkillRequest,
        install_skill, uninstall_skill, update_skill,
    };
    use crate::contract::{SkillSourceKind, qualified_skill_slug};
    use std::fs;
    use std::path::PathBuf;

    fn temp_case(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("unix timestamp")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pioneer-skills-installer-{name}-{nanos}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn write_skill(dir: &std::path::Path, name: &str, description: &str) {
        let slug = name
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() {
                    ch.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect::<String>()
            .split('-')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>()
            .join("-");
        fs::create_dir_all(dir).expect("create skill dir");
        fs::write(
            dir.join("SKILL.md"),
            format!("---\nname: {name}\nslug: {slug}\ndescription: {description}\n---\nBody"),
        )
        .expect("write skill");
    }

    #[test]
    fn install_update_uninstall_roundtrip_is_deterministic() {
        let root = temp_case("roundtrip");
        let source = root.join("source");
        let install_root = root.join("install");
        let lock_path = install_root.join("skills-lock.toml");
        write_skill(source.as_path(), "Agent Browser", "v1");

        let install = install_skill(InstallSkillRequest {
            source_kind: SkillSourceKind::Registry,
            source_ref: "github.com/example/agent-browser".to_owned(),
            source_path: source.clone(),
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            now_unix: 1_700_000_000,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("install");
        assert!(install.install_path.exists());
        assert!(
            install.install_path.ends_with("pioneer/agent-browser"),
            "install path must be owner/slug, got {}",
            install.install_path.display()
        );
        assert_eq!(install.audit_events.len(), 1);

        write_skill(source.as_path(), "Agent Browser", "v2");
        let update = update_skill(UpdateSkillRequest {
            source_kind: SkillSourceKind::Registry,
            source_ref: "github.com/example/agent-browser".to_owned(),
            source_path: source.clone(),
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            expected_previous_fingerprint: Some(install.lock_entry.fingerprint.clone()),
            now_unix: 1_700_000_100,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("update");
        assert!(update.install_path.exists());
        assert_ne!(
            install.lock_entry.fingerprint,
            update.lock_entry.fingerprint
        );

        let uninstall = uninstall_skill(UninstallSkillRequest {
            slug: qualified_skill_slug(
                update.definition.identity.owner.as_str(),
                update.definition.identity.slug.as_str(),
            ),
            source_kind: SkillSourceKind::Registry,
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            now_unix: 1_700_000_200,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("uninstall");
        assert!(uninstall.removed_path.is_some());
        assert!(!update.install_path.exists());
        assert_eq!(uninstall.audit_events.len(), 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn installer_fails_atomically_when_file_exceeds_limit() {
        let root = temp_case("atomicity");
        let source = root.join("source");
        let install_root = root.join("install");
        let lock_path = install_root.join("skills-lock.toml");
        write_skill(source.as_path(), "Too Big", "desc");
        fs::write(source.join("huge.bin"), vec![0u8; 1024]).expect("write large file");

        let mut policy = SkillInstallerPolicy::default();
        policy.security.max_install_file_bytes = 64;

        let result = install_skill(InstallSkillRequest {
            source_kind: SkillSourceKind::Registry,
            source_ref: "github.com/example/big".to_owned(),
            source_path: source.clone(),
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            now_unix: 1_700_000_000,
            policy,
        });

        assert!(result.is_err());
        assert!(!lock_path.exists());
        let install_entries = fs::read_dir(install_root.as_path())
            .ok()
            .map(|entries| entries.count())
            .unwrap_or_default();
        assert_eq!(install_entries, 0);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn update_blocks_on_fingerprint_mismatch_by_default() {
        let root = temp_case("fingerprint");
        let source = root.join("source");
        let install_root = root.join("install");
        let lock_path = install_root.join("skills-lock.toml");
        write_skill(source.as_path(), "Skill", "v1");

        let install = install_skill(InstallSkillRequest {
            source_kind: SkillSourceKind::Registry,
            source_ref: "github.com/example/skill".to_owned(),
            source_path: source.clone(),
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            now_unix: 1_700_000_000,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("install");

        write_skill(source.as_path(), "Skill", "v2");
        let result = update_skill(UpdateSkillRequest {
            source_kind: SkillSourceKind::Registry,
            source_ref: "github.com/example/skill".to_owned(),
            source_path: source.clone(),
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            expected_previous_fingerprint: Some("wrong-fingerprint".to_owned()),
            now_unix: 1_700_000_100,
            policy: SkillInstallerPolicy::default(),
        });
        assert!(result.is_err());
        assert!(install.install_path.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn install_blocks_on_dependency_failures_by_default() {
        let root = temp_case("dependency");
        let source = root.join("source");
        let install_root = root.join("install");
        let lock_path = install_root.join("skills-lock.toml");
        fs::create_dir_all(source.as_path()).expect("create source skill dir");
        fs::write(
            source.join("SKILL.md"),
            r#"---
name: Skill
slug: skill
description: dependency check
dependencies:
  commands: ["definitely-missing-installer-phase4-bin"]
---
Body"#,
        )
        .expect("write skill");

        let result = install_skill(InstallSkillRequest {
            source_kind: SkillSourceKind::Registry,
            source_ref: "github.com/example/skill".to_owned(),
            source_path: source.clone(),
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            now_unix: 1_700_000_000,
            policy: SkillInstallerPolicy::default(),
        });

        assert!(result.is_err());
        assert!(!install_root.join("pioneer").join("skill").exists());
        assert!(!lock_path.exists());

        let _ = fs::remove_dir_all(root);
    }
}
