use crate::audit::{SkillAuditAction, SkillAuditDecision, SkillAuditEvent};
use crate::contract::{SkillSourceKind, parse_skill_from_file};
use crate::dependencies::{
    DependencyCheckInput, DependencyCheckResult, evaluate_skill_dependencies,
};
use crate::provenance::{
    SkillLockEntry, SkillsLock, find_lock_entry, read_skills_lock, remove_lock_entry,
    upsert_lock_entry, write_skills_lock_atomic,
};
use crate::security::{SkillSecurityPolicy, ensure_install_path_contained, scan_skill_directory};
use anyhow::{Context, Result, bail};
use pioneer_protocol::SkillId;
use std::collections::VecDeque;
use std::fs;
use std::io::ErrorKind;
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
    pub skill_id: SkillId,
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
    pub skill_id: SkillId,
    pub source_kind: SkillSourceKind,
    pub source_ref: String,
    pub source_path: PathBuf,
    pub install_root: PathBuf,
    pub lock_path: PathBuf,
    pub previous: PreviousSkillInstallation,
    pub expected_previous_fingerprint: Option<String>,
    pub now_unix: i64,
    pub policy: SkillInstallerPolicy,
}

#[derive(Debug, Clone)]
pub struct PreviousSkillInstallation {
    pub managed_install_path: Option<PathBuf>,
    pub fingerprint: String,
}

#[derive(Debug, Clone)]
pub struct PrepareMaterializedSkillRequest {
    pub skill_id: SkillId,
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
    pub previous: Option<PreviousSkillInstallation>,
    pub expected_previous_fingerprint: Option<String>,
    pub now_unix: i64,
    pub policy: SkillInstallerPolicy,
}

#[derive(Debug, Clone)]
pub struct UninstallSkillRequest {
    pub skill_id: SkillId,
    pub owner: Option<String>,
    pub slug: String,
    pub source_kind: String,
    pub install_path: PathBuf,
    pub remove_install_path: bool,
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
    prepared_commit_rollback: Option<PreparedCommitRollback>,
}

#[derive(Debug, Clone)]
struct PreparedCommitRollback {
    previous_path: Option<PathBuf>,
    backup_path: Option<PathBuf>,
    previous_lock_entry: Option<SkillLockEntry>,
}

#[derive(Debug, Clone)]
pub struct UninstallSkillResult {
    pub removed_path: Option<PathBuf>,
    pub removed_lock_entry: Option<SkillLockEntry>,
    pub audit_events: Vec<SkillAuditEvent>,
}

#[derive(Debug, Clone)]
pub struct ReversibleSkillRemovalTarget {
    pub skill_id: SkillId,
    pub slug: String,
    pub managed_install_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct StageReversibleSkillRemovalsRequest {
    pub targets: Vec<ReversibleSkillRemovalTarget>,
    pub install_root: PathBuf,
    pub lock_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct ReversibleSkillRemoval {
    pub skill_id: SkillId,
    pub original_path: Option<PathBuf>,
    pub was_present_on_disk: bool,
    pub removed_lock_entry: Option<SkillLockEntry>,
    staged_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct ReversibleSkillRemovalBatch {
    pub removals: Vec<ReversibleSkillRemoval>,
    install_root: PathBuf,
    lock_path: PathBuf,
    staging_session_dir: PathBuf,
    previous_lock: SkillsLock,
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
        commit_staged_skill(InstallOperation::Install, &request, staging_dir, None, None);

    if install_result.is_err() {
        cleanup_staging(request.install_root.as_path());
    }

    install_result
}

pub fn update_skill(request: UpdateSkillRequest) -> Result<InstallSkillResult> {
    cleanup_staging(request.install_root.as_path());

    let install_request = InstallSkillRequest {
        skill_id: request.skill_id,
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

    let install_result = commit_staged_skill(
        InstallOperation::Update,
        &install_request,
        staging_dir,
        Some(request.previous),
        request.expected_previous_fingerprint.clone(),
    );

    if install_result.is_err() {
        cleanup_staging(request.install_root.as_path());
    }

    install_result
}

pub fn uninstall_skill(request: UninstallSkillRequest) -> Result<UninstallSkillResult> {
    cleanup_staging(request.install_root.as_path());

    let mut lock = read_skills_lock(request.lock_path.as_path())?;

    let removed_path = if request.remove_install_path {
        let containment = ensure_install_path_contained(
            request.install_root.as_path(),
            request.install_path.as_path(),
        );
        if containment.has_blocking_findings() {
            bail!("uninstall blocked by containment policy");
        }
        remove_path_if_present(request.install_path.as_path())?
            .then(|| request.install_path.clone())
    } else {
        None
    };

    let removed_lock = remove_lock_entry(&mut lock, &request.skill_id);
    write_skills_lock_atomic(request.lock_path.as_path(), &lock)?;

    let audit_event = SkillAuditEvent::uninstall(
        request.skill_id,
        request.owner,
        request.slug,
        request.source_kind,
        serde_json::json!({
            "install_path": request.install_path.display().to_string(),
            "was_present_on_disk": removed_path.is_some(),
            "had_lock_entry": removed_lock.is_some(),
            "managed_path_removed": request.remove_install_path,
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

pub fn canonical_skill_install_path(
    install_root: &Path,
    skill_id: &SkillId,
    slug: &str,
) -> Result<PathBuf> {
    let mut components = Path::new(slug).components();
    let is_single_leaf = matches!(components.next(), Some(Component::Normal(_)))
        && components.next().is_none()
        && slug != "."
        && slug != "..";
    if !is_single_leaf {
        bail!("skill slug must be one relative path component");
    }
    Ok(install_root.join(skill_id.as_str()).join(slug))
}

pub fn finalize_prepared_skill_commit(result: &InstallSkillResult) {
    if let Some(backup_path) = result
        .prepared_commit_rollback
        .as_ref()
        .and_then(|rollback| rollback.backup_path.as_ref())
    {
        let _ = remove_path_if_present(backup_path);
    }
}

pub fn rollback_prepared_skill_commit(result: &InstallSkillResult, lock_path: &Path) -> Result<()> {
    let Some(rollback) = &result.prepared_commit_rollback else {
        return Ok(());
    };
    remove_path_if_present(result.install_path.as_path()).with_context(|| {
        format!(
            "failed to remove published package `{}`",
            result.install_path.display()
        )
    })?;
    if let (Some(backup_path), Some(previous_path)) =
        (&rollback.backup_path, &rollback.previous_path)
    {
        fs::rename(backup_path, previous_path).with_context(|| {
            format!(
                "failed to restore previous package `{}`",
                previous_path.display()
            )
        })?;
    }
    let mut lock = read_skills_lock(lock_path)?;
    if let Some(previous) = &rollback.previous_lock_entry {
        upsert_lock_entry(&mut lock, previous.clone());
    } else {
        remove_lock_entry(&mut lock, &result.definition.identity.skill_id);
    }
    write_skills_lock_atomic(lock_path, &lock)
}

pub fn stage_reversible_skill_removals(
    mut request: StageReversibleSkillRemovalsRequest,
) -> Result<ReversibleSkillRemovalBatch> {
    request
        .targets
        .sort_by(|left, right| left.skill_id.cmp(&right.skill_id));
    if request
        .targets
        .windows(2)
        .any(|pair| pair[0].skill_id == pair[1].skill_id)
    {
        bail!("reversible removal targets must have unique SkillId values");
    }

    for target in &request.targets {
        if let Some(path) = target.managed_install_path.as_ref() {
            let containment =
                ensure_install_path_contained(request.install_root.as_path(), path.as_path());
            if containment.has_blocking_findings() {
                bail!(
                    "reversible removal blocked by containment policy for skill `{}`",
                    target.skill_id
                );
            }
        }
    }
    for (index, left) in request.targets.iter().enumerate() {
        let Some(left_path) = left.managed_install_path.as_ref() else {
            continue;
        };
        for right in request.targets.iter().skip(index + 1) {
            let Some(right_path) = right.managed_install_path.as_ref() else {
                continue;
            };
            if left_path == right_path
                || left_path.starts_with(right_path)
                || right_path.starts_with(left_path)
            {
                bail!("reversible removal paths must be distinct and non-overlapping");
            }
        }
    }
    for target in &request.targets {
        let Some(path) = target.managed_install_path.as_ref() else {
            continue;
        };
        let expected = canonical_skill_install_path(
            request.install_root.as_path(),
            &target.skill_id,
            target.slug.as_str(),
        )?;
        if path != &expected {
            bail!(
                "reversible removal path `{}` is not the canonical path `{}` for skill `{}`",
                path.display(),
                expected.display(),
                target.skill_id
            );
        }
    }

    let previous_lock = read_skills_lock(request.lock_path.as_path())?;
    let staging_root = request.install_root.join(".staging");
    fs::create_dir_all(staging_root.as_path())
        .with_context(|| format!("failed to create staging root `{}`", staging_root.display()))?;
    let staging_session_dir = staging_root.join(format!("removal-{}", unique_suffix()));
    fs::create_dir_all(staging_session_dir.as_path()).with_context(|| {
        format!(
            "failed to create removal staging directory `{}`",
            staging_session_dir.display()
        )
    })?;

    let mut removals: Vec<ReversibleSkillRemoval> = Vec::with_capacity(request.targets.len());
    for (index, target) in request.targets.into_iter().enumerate() {
        let original_path = target.managed_install_path;
        let was_present_on_disk = original_path.as_ref().is_some_and(|path| path.exists());
        let staged_path =
            was_present_on_disk.then(|| staging_session_dir.join(target.skill_id.as_str()));
        if let (Some(original_path), Some(staged_path)) = (&original_path, &staged_path) {
            let stage_result = ensure_reversible_removal_path_contained(
                request.install_root.as_path(),
                staged_path.as_path(),
                "staging",
            )
            .and_then(|()| removal_failpoint("stage_move", index))
            .and_then(|()| {
                fs::rename(original_path.as_path(), staged_path.as_path()).with_context(|| {
                    format!(
                        "failed to stage removal of skill `{}` from `{}`",
                        target.skill_id,
                        original_path.display()
                    )
                })
            });
            if let Err(error) = stage_result {
                let rollback_errors =
                    restore_staged_removal_paths(&removals, request.install_root.as_path());
                if rollback_errors.is_empty() {
                    let _ = remove_empty_staging_session(staging_session_dir.as_path());
                }
                return Err(with_rollback_errors(error, rollback_errors));
            }
        }
        let removed_lock_entry = find_lock_entry(&previous_lock, &target.skill_id).cloned();
        removals.push(ReversibleSkillRemoval {
            skill_id: target.skill_id,
            original_path,
            was_present_on_disk,
            removed_lock_entry,
            staged_path,
        });
    }

    let mut updated_lock = previous_lock.clone();
    for removal in &removals {
        remove_lock_entry(&mut updated_lock, &removal.skill_id);
    }
    if let Err(error) = removal_failpoint("lock_write", 0).and_then(|()| {
        write_skills_lock_atomic(request.lock_path.as_path(), &updated_lock)
            .context("failed to write lock after staging reversible removals")
    }) {
        let rollback_errors =
            restore_staged_removal_paths(&removals, request.install_root.as_path());
        if rollback_errors.is_empty() {
            let _ = remove_empty_staging_session(staging_session_dir.as_path());
        }
        return Err(with_rollback_errors(error, rollback_errors));
    }

    Ok(ReversibleSkillRemovalBatch {
        removals,
        install_root: request.install_root,
        lock_path: request.lock_path,
        staging_session_dir,
        previous_lock,
    })
}

pub fn rollback_reversible_skill_removals(batch: &ReversibleSkillRemovalBatch) -> Result<()> {
    let mut errors = restore_staged_removal_paths(&batch.removals, batch.install_root.as_path());
    if let Err(error) = removal_failpoint("rollback_lock", 0).and_then(|()| {
        write_skills_lock_atomic(batch.lock_path.as_path(), &batch.previous_lock)
            .context("failed to restore skills lock after reversible removals")
    }) {
        errors.push(error);
    }
    if let Err(error) = remove_empty_staging_session(batch.staging_session_dir.as_path()) {
        errors.push(error);
    }
    finish_collected_errors("failed to roll back reversible skill removals", errors)
}

pub fn finalize_reversible_skill_removals(batch: &ReversibleSkillRemovalBatch) -> Result<()> {
    let mut errors = Vec::new();
    for (index, removal) in batch.removals.iter().enumerate() {
        let Some(staged_path) = removal.staged_path.as_ref() else {
            continue;
        };
        let finalize_result = ensure_reversible_removal_path_contained(
            batch.install_root.as_path(),
            staged_path.as_path(),
            "staged removal",
        )
        .and_then(|()| removal_failpoint("finalize_remove", index))
        .and_then(|()| {
            remove_path_if_present(staged_path.as_path())
                .with_context(|| format!("failed to finalize removal of `{}`", removal.skill_id))
                .map(|_| ())
        });
        if let Err(error) = finalize_result {
            errors.push(error);
        }
    }
    if let Err(error) = remove_empty_staging_session(batch.staging_session_dir.as_path()) {
        errors.push(error);
    }
    finish_collected_errors("failed to finalize reversible skill removals", errors)
}

fn restore_staged_removal_paths(
    removals: &[ReversibleSkillRemoval],
    install_root: &Path,
) -> Vec<anyhow::Error> {
    let mut errors = Vec::new();
    for (index, removal) in removals.iter().enumerate().rev() {
        let (Some(original_path), Some(staged_path)) =
            (removal.original_path.as_ref(), removal.staged_path.as_ref())
        else {
            continue;
        };
        if !staged_path.exists() {
            if !original_path.exists() {
                errors.push(anyhow::anyhow!(
                    "neither staged nor original path exists for skill `{}`",
                    removal.skill_id
                ));
            }
            continue;
        }
        if original_path.exists() {
            errors.push(anyhow::anyhow!(
                "cannot restore skill `{}` because `{}` already exists",
                removal.skill_id,
                original_path.display()
            ));
            continue;
        }
        let restore_result = ensure_reversible_removal_path_contained(
            install_root,
            staged_path.as_path(),
            "staged removal",
        )
        .and_then(|()| {
            ensure_reversible_removal_path_contained(
                install_root,
                original_path.as_path(),
                "original removal",
            )
        })
        .and_then(|()| removal_failpoint("rollback_move", index))
        .and_then(|()| {
            fs::rename(staged_path.as_path(), original_path.as_path()).with_context(|| {
                format!(
                    "failed to restore skill `{}` to `{}`",
                    removal.skill_id,
                    original_path.display()
                )
            })
        });
        if let Err(error) = restore_result {
            errors.push(error);
        }
    }
    errors
}

fn ensure_reversible_removal_path_contained(
    install_root: &Path,
    path: &Path,
    label: &str,
) -> Result<()> {
    let containment = ensure_install_path_contained(install_root, path);
    if containment.has_blocking_findings() {
        bail!(
            "{label} path `{}` is outside managed install root",
            path.display()
        );
    }
    Ok(())
}

fn remove_empty_staging_session(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    match fs::remove_dir(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::DirectoryNotEmpty => Ok(()),
        Err(error) => Err(error)
            .with_context(|| format!("failed to remove staging directory `{}`", path.display())),
    }
}

fn with_rollback_errors(
    primary: anyhow::Error,
    rollback_errors: Vec<anyhow::Error>,
) -> anyhow::Error {
    if rollback_errors.is_empty() {
        return primary;
    }
    anyhow::anyhow!(
        "{primary:#}; rollback also failed: {}",
        rollback_errors
            .iter()
            .map(|error| format!("{error:#}"))
            .collect::<Vec<_>>()
            .join("; ")
    )
}

fn finish_collected_errors(context: &str, errors: Vec<anyhow::Error>) -> Result<()> {
    if errors.is_empty() {
        return Ok(());
    }
    bail!(
        "{context}: {}",
        errors
            .iter()
            .map(|error| format!("{error:#}"))
            .collect::<Vec<_>>()
            .join("; ")
    )
}

#[cfg(not(test))]
fn removal_failpoint(_name: &str, _index: usize) -> Result<()> {
    Ok(())
}

#[cfg(test)]
thread_local! {
    static REMOVAL_FAILPOINTS: std::cell::RefCell<Vec<(&'static str, usize)>> = const {
        std::cell::RefCell::new(Vec::new())
    };
}

#[cfg(test)]
fn removal_failpoint(name: &str, index: usize) -> Result<()> {
    REMOVAL_FAILPOINTS.with(|failpoints| {
        if failpoints
            .borrow()
            .iter()
            .any(|(candidate, candidate_index)| *candidate == name && *candidate_index == index)
        {
            bail!("injected reversible removal failure at {name}[{index}]");
        }
        Ok(())
    })
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
        request.skill_id,
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
    let target_path = canonical_skill_install_path(
        request.install_root.as_path(),
        &definition.identity.skill_id,
        definition.identity.slug.as_str(),
    )?;

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

    let previous_lock_entry = find_lock_entry(&lock, &definition.identity.skill_id).cloned();

    let previous_installation = match request.operation {
        InstallOperation::Install => {
            if request.previous.is_some() {
                bail!("install must not include previous installation state");
            }
            None
        }
        InstallOperation::Update => Some(request.previous.as_ref().with_context(|| {
            format!(
                "cannot update skill `{}` without authoritative previous installation state",
                definition.identity.skill_id
            )
        })?),
    };

    if let Some(expected) = request.expected_previous_fingerprint.as_deref()
        && let Some(previous) = previous_installation
        && previous.fingerprint != expected
    {
        if request.policy.block_on_fingerprint_mismatch {
            bail!(
                "update blocked: expected previous fingerprint `{expected}`, found `{}`",
                previous.fingerprint
            );
        }
    }

    let mut update_backup = None;
    match request.operation {
        InstallOperation::Install => {
            if previous_lock_entry.is_some() {
                bail!(
                    "skill `{}` is already installed",
                    definition.identity.skill_id
                );
            }
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
            let previous = previous_installation.with_context(|| {
                format!(
                    "cannot update skill `{}` without authoritative previous installation state",
                    definition.identity.skill_id
                )
            })?;
            if let Some(previous_path) = previous.managed_install_path.as_ref() {
                let previous_containment = ensure_install_path_contained(
                    request.install_root.as_path(),
                    previous_path.as_path(),
                );
                if previous_containment.has_blocking_findings() {
                    bail!("update blocked by containment policy for previous installation");
                }
                if previous_path.exists() {
                    let backup_path = request
                        .install_root
                        .join(definition.identity.skill_id.as_str())
                        .join(format!(
                            ".backup-{}-{}",
                            definition.identity.slug,
                            unique_suffix()
                        ));
                    if target_path != *previous_path {
                        remove_path_if_present(target_path.as_path()).with_context(|| {
                            format!(
                                "failed to replace stale update target `{}`",
                                target_path.display()
                            )
                        })?;
                    }
                    fs::rename(previous_path.as_path(), backup_path.as_path()).with_context(
                        || {
                            format!(
                                "failed to create rollback copy for `{}` before update",
                                previous_path.display()
                            )
                        },
                    )?;
                    update_backup = Some((backup_path, previous_path.clone()));
                } else {
                    remove_path_if_present(target_path.as_path()).with_context(|| {
                        format!(
                            "failed to replace stale update target `{}`",
                            target_path.display()
                        )
                    })?;
                }
            } else {
                remove_path_if_present(target_path.as_path()).with_context(|| {
                    format!(
                        "failed to replace stale update target `{}`",
                        target_path.display()
                    )
                })?;
            }

            if let Err(error) = fs::rename(prepared.source_path.as_path(), target_path.as_path()) {
                if let Some((backup_path, previous_path)) = &update_backup {
                    let _ = fs::rename(backup_path.as_path(), previous_path.as_path());
                }
                return Err(error).with_context(|| {
                    format!(
                        "failed to move staged update into `{}`",
                        target_path.display()
                    )
                });
            }
        }
    }

    let lock_entry = SkillLockEntry {
        skill_id: definition.identity.skill_id.clone(),
        owner: definition.identity.owner.clone(),
        slug: definition.identity.slug.clone(),
        source_kind: source_kind.clone(),
        source_ref: prepared.source_ref.clone(),
        install_path: target_path.display().to_string(),
        version: definition.identity.version_hint.clone(),
        trust_level: definition.runtime.trust_level.clone(),
        fingerprint: definition.identity.fingerprint.clone(),
        installed_at: previous_lock_entry
            .as_ref()
            .map(|entry| entry.installed_at)
            .unwrap_or(request.now_unix),
    };
    upsert_lock_entry(&mut lock, lock_entry.clone());
    if let Err(error) = write_skills_lock_atomic(request.lock_path.as_path(), &lock) {
        let _ = remove_path_if_present(target_path.as_path());
        if let Some((backup_path, previous_path)) = &update_backup {
            let _ = fs::rename(backup_path.as_path(), previous_path.as_path());
        }
        return Err(error).context("failed to commit skills lock; package publication rolled back");
    }
    let prepared_commit_rollback = Some(PreparedCommitRollback {
        previous_path: update_backup
            .as_ref()
            .map(|(_, previous_path)| previous_path.clone()),
        backup_path: update_backup
            .as_ref()
            .map(|(backup_path, _)| backup_path.clone()),
        previous_lock_entry: previous_lock_entry.clone(),
    });

    let mut audit_events = Vec::new();

    match request.operation {
        InstallOperation::Install => audit_events.push(SkillAuditEvent::install(
            definition.identity.skill_id.clone(),
            definition.identity.owner.clone(),
            definition.identity.slug.clone(),
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
            definition.identity.skill_id.clone(),
            definition.identity.owner.clone(),
            definition.identity.slug.clone(),
            source_kind.clone(),
            serde_json::json!({
                "updated_from": previous_installation.map(|previous| previous.fingerprint.clone()),
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
        && let Some(previous) = previous_installation
        && previous.fingerprint != expected
        && !request.policy.block_on_fingerprint_mismatch
    {
        audit_events.push(SkillAuditEvent::new(
            definition.identity.skill_id.clone(),
            definition.identity.owner.clone(),
            definition.identity.slug.clone(),
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
        prepared_commit_rollback,
    })
}

fn commit_staged_skill(
    operation: InstallOperation,
    request: &InstallSkillRequest,
    staging_dir: PathBuf,
    previous: Option<PreviousSkillInstallation>,
    expected_previous_fingerprint: Option<String>,
) -> Result<InstallSkillResult> {
    let prepared = prepare_materialized_skill(PrepareMaterializedSkillRequest {
        skill_id: request.skill_id.clone(),
        source_kind: request.source_kind.clone(),
        source_ref: request.source_ref.clone(),
        materialized_source_path: staging_dir,
        policy: request.policy.clone(),
    })?;
    let mut result = commit_prepared_skill(CommitPreparedSkillRequest {
        operation,
        prepared,
        install_root: request.install_root.clone(),
        lock_path: request.lock_path.clone(),
        previous,
        expected_previous_fingerprint,
        now_unix: request.now_unix,
        policy: request.policy.clone(),
    })?;
    finalize_prepared_skill_commit(&result);
    result.prepared_commit_rollback = None;
    Ok(result)
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

fn remove_path_if_present(path: &Path) -> Result<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to inspect path `{}`", path.display()));
        }
    };
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        fs::remove_dir_all(path)
            .with_context(|| format!("failed to remove directory `{}`", path.display()))?;
    } else {
        fs::remove_file(path)
            .with_context(|| format!("failed to remove path `{}`", path.display()))?;
    }
    Ok(true)
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
        CommitPreparedSkillRequest, InstallOperation, InstallSkillRequest,
        PrepareMaterializedSkillRequest, PreviousSkillInstallation, REMOVAL_FAILPOINTS,
        ReversibleSkillRemovalTarget, SkillInstallerPolicy, StageReversibleSkillRemovalsRequest,
        UninstallSkillRequest, UpdateSkillRequest, commit_prepared_skill,
        finalize_reversible_skill_removals, install_skill, prepare_materialized_skill,
        rollback_prepared_skill_commit, rollback_reversible_skill_removals,
        stage_reversible_skill_removals, uninstall_skill, update_skill,
    };
    use crate::contract::SkillSourceKind;
    use crate::provenance::{find_lock_entry, read_skills_lock};
    use pioneer_protocol::SkillId;
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

    fn id(value: &str) -> SkillId {
        SkillId::new(value).unwrap()
    }

    struct RemovalFailpointGuard;

    impl Drop for RemovalFailpointGuard {
        fn drop(&mut self) {
            REMOVAL_FAILPOINTS.with(|failpoints| failpoints.borrow_mut().clear());
        }
    }

    fn removal_failpoints(points: Vec<(&'static str, usize)>) -> RemovalFailpointGuard {
        REMOVAL_FAILPOINTS.with(|failpoints| *failpoints.borrow_mut() = points);
        RemovalFailpointGuard
    }

    #[test]
    fn install_update_uninstall_roundtrip_is_deterministic() {
        let root = temp_case("roundtrip");
        let source = root.join("source");
        let install_root = root.join("install");
        let lock_path = install_root.join("skills-lock.toml");
        write_skill(source.as_path(), "Agent Browser", "v1");
        let skill_id = id("AAAAAAAAAAAAAAAAAAAAA");

        let install = install_skill(InstallSkillRequest {
            skill_id: skill_id.clone(),
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
            install
                .install_path
                .ends_with("AAAAAAAAAAAAAAAAAAAAA/agent-browser"),
            "install path must be skill-id/slug, got {}",
            install.install_path.display()
        );
        assert_eq!(install.audit_events.len(), 1);

        write_skill(source.as_path(), "Agent Browser", "v2");
        let update = update_skill(UpdateSkillRequest {
            skill_id: skill_id.clone(),
            source_kind: SkillSourceKind::Registry,
            source_ref: "github.com/example/agent-browser".to_owned(),
            source_path: source.clone(),
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            previous: PreviousSkillInstallation {
                managed_install_path: Some(install.install_path.clone()),
                fingerprint: install.lock_entry.fingerprint.clone(),
            },
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
            skill_id,
            owner: update.definition.identity.owner.clone(),
            slug: update.definition.identity.slug.clone(),
            source_kind: "registry".to_owned(),
            install_path: update.install_path.clone(),
            remove_install_path: true,
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
            skill_id: id("BBBBBBBBBBBBBBBBBBBBB"),
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
        let skill_id = id("CCCCCCCCCCCCCCCCCCCCC");

        let install = install_skill(InstallSkillRequest {
            skill_id: skill_id.clone(),
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
            skill_id,
            source_kind: SkillSourceKind::Registry,
            source_ref: "github.com/example/skill".to_owned(),
            source_path: source.clone(),
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            previous: PreviousSkillInstallation {
                managed_install_path: Some(install.install_path.clone()),
                fingerprint: install.lock_entry.fingerprint.clone(),
            },
            expected_previous_fingerprint: Some("wrong-fingerprint".to_owned()),
            now_unix: 1_700_000_100,
            policy: SkillInstallerPolicy::default(),
        });
        assert!(result.is_err());
        assert!(install.install_path.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn update_restores_missing_managed_package_without_lock_entry() {
        let root = temp_case("restore-missing");
        let source = root.join("source");
        let install_root = root.join("install");
        let lock_path = install_root.join("skills-lock.toml");
        let skill_id = id("JJJJJJJJJJJJJJJJJJJJJ");
        write_skill(source.as_path(), "Restorable", "same revision");
        let install = install_skill(InstallSkillRequest {
            skill_id: skill_id.clone(),
            source_kind: SkillSourceKind::User,
            source_ref: "source".to_owned(),
            source_path: source.clone(),
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            now_unix: 1,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("install package");
        fs::remove_dir_all(install.install_path.as_path()).expect("remove installed package");
        fs::remove_file(lock_path.as_path()).expect("remove lock entry with lock file");

        let restored = update_skill(UpdateSkillRequest {
            skill_id: skill_id.clone(),
            source_kind: SkillSourceKind::User,
            source_ref: "source".to_owned(),
            source_path: source,
            install_root,
            lock_path: lock_path.clone(),
            previous: PreviousSkillInstallation {
                managed_install_path: Some(install.install_path.clone()),
                fingerprint: install.lock_entry.fingerprint.clone(),
            },
            expected_previous_fingerprint: Some(install.lock_entry.fingerprint),
            now_unix: 2,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("restore missing package with same SkillId");

        assert_eq!(restored.definition.identity.skill_id, skill_id);
        assert!(restored.install_path.is_dir());
        let lock = read_skills_lock(lock_path.as_path()).expect("read recreated lock");
        assert!(find_lock_entry(&lock, &skill_id).is_some());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn update_and_uninstall_never_delete_external_import_source() {
        let root = temp_case("external-source");
        let external = root.join("external");
        let install_root = root.join("managed");
        let lock_path = install_root.join("skills-lock.toml");
        let skill_id = id("KKKKKKKKKKKKKKKKKKKKK");
        write_skill(external.as_path(), "External", "revision");

        let update = update_skill(UpdateSkillRequest {
            skill_id: skill_id.clone(),
            source_kind: SkillSourceKind::Registry,
            source_ref: "upload:new".to_owned(),
            source_path: external.clone(),
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            previous: PreviousSkillInstallation {
                managed_install_path: None,
                fingerprint: "pending".to_owned(),
            },
            expected_previous_fingerprint: Some("pending".to_owned()),
            now_unix: 2,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("update external import into managed storage");
        assert!(external.join("SKILL.md").is_file());
        assert!(update.install_path.is_dir());

        let pending_id = id("LLLLLLLLLLLLLLLLLLLLL");
        let uninstall = uninstall_skill(UninstallSkillRequest {
            skill_id: pending_id,
            owner: None,
            slug: "external".to_owned(),
            source_kind: "registry".to_owned(),
            install_path: external.clone(),
            remove_install_path: false,
            install_root,
            lock_path,
            now_unix: 3,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("uninstall pending external identity");
        assert!(uninstall.removed_path.is_none());
        assert!(external.join("SKILL.md").is_file());
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
            skill_id: id("DDDDDDDDDDDDDDDDDDDDD"),
            source_kind: SkillSourceKind::Registry,
            source_ref: "github.com/example/skill".to_owned(),
            source_path: source.clone(),
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            now_unix: 1_700_000_000,
            policy: SkillInstallerPolicy::default(),
        });

        assert!(result.is_err());
        assert!(
            !install_root
                .join("DDDDDDDDDDDDDDDDDDDDD")
                .join("skill")
                .exists()
        );
        assert!(!lock_path.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn duplicate_labels_have_isolated_id_paths_and_exact_uninstall() {
        let root = temp_case("duplicates");
        let source = root.join("source");
        let install_root = root.join("install");
        let lock_path = install_root.join("skills-lock.toml");
        write_skill(source.as_path(), "Duplicate", "same package");
        let first_id = id("EEEEEEEEEEEEEEEEEEEEE");
        let second_id = id("FFFFFFFFFFFFFFFFFFFFF");

        let first = install_skill(InstallSkillRequest {
            skill_id: first_id.clone(),
            source_kind: SkillSourceKind::User,
            source_ref: "first".to_owned(),
            source_path: source.clone(),
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            now_unix: 1,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("first install");
        let second = install_skill(InstallSkillRequest {
            skill_id: second_id.clone(),
            source_kind: SkillSourceKind::User,
            source_ref: "second".to_owned(),
            source_path: source.clone(),
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            now_unix: 2,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("second install");

        assert_ne!(first.install_path, second.install_path);
        assert!(first.install_path.exists());
        assert!(second.install_path.exists());
        uninstall_skill(UninstallSkillRequest {
            skill_id: first_id,
            owner: first.definition.identity.owner.clone(),
            slug: first.definition.identity.slug.clone(),
            source_kind: "user".to_owned(),
            install_path: first.install_path.clone(),
            remove_install_path: true,
            install_root: install_root.clone(),
            lock_path,
            now_unix: 3,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("exact uninstall");
        assert!(!first.install_path.exists());
        assert!(second.install_path.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn update_preserves_id_changes_leaf_and_preserves_package_bytes() {
        let root = temp_case("slug-change");
        let source = root.join("source");
        let install_root = root.join("install");
        let lock_path = install_root.join("skills-lock.toml");
        let skill_id = id("GGGGGGGGGGGGGGGGGGGGG");
        write_skill(source.as_path(), "Old Name", "v1");
        let original_asset = [0_u8, 1, 2, 3, 254, 255];
        fs::create_dir_all(source.join("assets")).unwrap();
        fs::write(source.join("assets/payload.bin"), original_asset).unwrap();

        let install = install_skill(InstallSkillRequest {
            skill_id: skill_id.clone(),
            source_kind: SkillSourceKind::Registry,
            source_ref: "source".to_owned(),
            source_path: source.clone(),
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            now_unix: 1,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("install old leaf");
        write_skill(source.as_path(), "New Name", "v2");

        let update = update_skill(UpdateSkillRequest {
            skill_id: skill_id.clone(),
            source_kind: SkillSourceKind::Registry,
            source_ref: "source".to_owned(),
            source_path: source.clone(),
            install_root: install_root.clone(),
            lock_path,
            previous: PreviousSkillInstallation {
                managed_install_path: Some(install.install_path.clone()),
                fingerprint: install.lock_entry.fingerprint.clone(),
            },
            expected_previous_fingerprint: Some(install.lock_entry.fingerprint),
            now_unix: 2,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("update renamed leaf");

        assert_eq!(update.definition.identity.skill_id, skill_id);
        assert!(
            update
                .install_path
                .ends_with("GGGGGGGGGGGGGGGGGGGGG/new-name")
        );
        assert!(!install.install_path.exists());
        assert_eq!(
            fs::read(update.install_path.join("assets/payload.bin")).unwrap(),
            original_asset
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepared_update_can_rollback_package_and_lock_after_database_failure() {
        let root = temp_case("prepared-db-rollback");
        let source = root.join("source");
        let materialized = root.join("materialized");
        let install_root = root.join("install");
        let lock_path = install_root.join("skills-lock.toml");
        let skill_id = id("HHHHHHHHHHHHHHHHHHHHH");
        write_skill(source.as_path(), "Old Name", "v1");
        let install = install_skill(InstallSkillRequest {
            skill_id: skill_id.clone(),
            source_kind: SkillSourceKind::Registry,
            source_ref: "source-v1".to_owned(),
            source_path: source,
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            now_unix: 1,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("install old package");

        write_skill(materialized.as_path(), "New Name", "v2");
        let prepared = prepare_materialized_skill(PrepareMaterializedSkillRequest {
            skill_id: skill_id.clone(),
            source_kind: SkillSourceKind::Registry,
            source_ref: "source-v2".to_owned(),
            materialized_source_path: materialized,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("prepare update");
        let update = commit_prepared_skill(CommitPreparedSkillRequest {
            operation: InstallOperation::Update,
            prepared,
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
            previous: Some(PreviousSkillInstallation {
                managed_install_path: Some(install.install_path.clone()),
                fingerprint: install.lock_entry.fingerprint.clone(),
            }),
            expected_previous_fingerprint: Some(install.lock_entry.fingerprint.clone()),
            now_unix: 2,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("publish prepared update");
        assert!(!install.install_path.exists());
        assert!(update.install_path.exists());

        rollback_prepared_skill_commit(&update, lock_path.as_path())
            .expect("roll back prepared update");
        assert!(install.install_path.exists());
        assert!(!update.install_path.exists());
        let lock = read_skills_lock(lock_path.as_path()).expect("read restored lock");
        let restored = find_lock_entry(&lock, &skill_id).expect("restored lock entry");
        assert_eq!(restored.install_path, install.lock_entry.install_path);
        assert_eq!(restored.fingerprint, install.lock_entry.fingerprint);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepared_install_can_rollback_package_and_lock_after_database_failure() {
        let root = temp_case("prepared-install-db-rollback");
        let materialized = root.join("materialized");
        let install_root = root.join("install");
        let lock_path = install_root.join("skills-lock.toml");
        let skill_id = id("IIIIIIIIIIIIIIIIIIIII");
        write_skill(materialized.as_path(), "New Skill", "v1");
        let prepared = prepare_materialized_skill(PrepareMaterializedSkillRequest {
            skill_id: skill_id.clone(),
            source_kind: SkillSourceKind::User,
            source_ref: "upload:test".to_owned(),
            materialized_source_path: materialized,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("prepare install");
        let install = commit_prepared_skill(CommitPreparedSkillRequest {
            operation: InstallOperation::Install,
            prepared,
            install_root,
            lock_path: lock_path.clone(),
            previous: None,
            expected_previous_fingerprint: None,
            now_unix: 1,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("publish prepared install");
        assert!(install.install_path.exists());

        rollback_prepared_skill_commit(&install, lock_path.as_path())
            .expect("roll back prepared install");
        assert!(!install.install_path.exists());
        let lock = read_skills_lock(lock_path.as_path()).expect("read rolled-back lock");
        assert!(find_lock_entry(&lock, &skill_id).is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reversible_batch_removal_stages_in_order_and_restores_paths_and_lock() {
        let root = temp_case("reversible-removal-rollback");
        let install_root = root.join("install");
        let lock_path = install_root.join("skills-lock.toml");
        let first = install_for_removal(
            root.as_path(),
            install_root.as_path(),
            lock_path.as_path(),
            "AAAAAAAAAAAAAAAAAAAAA",
            "First Skill",
        );
        let second = install_for_removal(
            root.as_path(),
            install_root.as_path(),
            lock_path.as_path(),
            "BBBBBBBBBBBBBBBBBBBBB",
            "Second Skill",
        );

        let batch = stage_reversible_skill_removals(StageReversibleSkillRemovalsRequest {
            targets: vec![
                ReversibleSkillRemovalTarget {
                    skill_id: second.definition.identity.skill_id.clone(),
                    slug: second.definition.identity.slug.clone(),
                    managed_install_path: Some(second.install_path.clone()),
                },
                ReversibleSkillRemovalTarget {
                    skill_id: first.definition.identity.skill_id.clone(),
                    slug: first.definition.identity.slug.clone(),
                    managed_install_path: Some(first.install_path.clone()),
                },
            ],
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
        })
        .expect("stage removals");

        assert_eq!(batch.removals[0].skill_id.as_str(), "AAAAAAAAAAAAAAAAAAAAA");
        assert_eq!(batch.removals[1].skill_id.as_str(), "BBBBBBBBBBBBBBBBBBBBB");
        assert!(!first.install_path.exists());
        assert!(!second.install_path.exists());
        let staged_lock = read_skills_lock(lock_path.as_path()).expect("read staged lock");
        assert!(find_lock_entry(&staged_lock, &first.definition.identity.skill_id).is_none());
        assert!(find_lock_entry(&staged_lock, &second.definition.identity.skill_id).is_none());

        rollback_reversible_skill_removals(&batch).expect("roll back removals");

        assert!(first.install_path.exists());
        assert!(second.install_path.exists());
        let restored_lock = read_skills_lock(lock_path.as_path()).expect("read restored lock");
        assert!(find_lock_entry(&restored_lock, &first.definition.identity.skill_id).is_some());
        assert!(find_lock_entry(&restored_lock, &second.definition.identity.skill_id).is_some());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reversible_batch_removal_finalize_deletes_only_staged_paths() {
        let root = temp_case("reversible-removal-finalize");
        let install_root = root.join("install");
        let lock_path = install_root.join("skills-lock.toml");
        let installed = install_for_removal(
            root.as_path(),
            install_root.as_path(),
            lock_path.as_path(),
            "CCCCCCCCCCCCCCCCCCCCC",
            "Removed Skill",
        );
        let unrelated = install_root.join("unrelated.txt");
        fs::write(unrelated.as_path(), b"keep").expect("write unrelated file");
        let batch = stage_reversible_skill_removals(StageReversibleSkillRemovalsRequest {
            targets: vec![ReversibleSkillRemovalTarget {
                skill_id: installed.definition.identity.skill_id.clone(),
                slug: installed.definition.identity.slug.clone(),
                managed_install_path: Some(installed.install_path.clone()),
            }],
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
        })
        .expect("stage removal");

        finalize_reversible_skill_removals(&batch).expect("finalize removal");

        assert!(!installed.install_path.exists());
        assert!(unrelated.exists());
        assert!(batch.removals.iter().all(|removal| {
            removal
                .staged_path
                .as_ref()
                .is_none_or(|path| !path.exists())
        }));
        assert!(
            find_lock_entry(
                &read_skills_lock(lock_path.as_path()).expect("read final lock"),
                &installed.definition.identity.skill_id,
            )
            .is_none()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reversible_batch_removal_missing_optional_path_preserves_uninstall_semantics() {
        let root = temp_case("reversible-removal-missing");
        let install_root = root.join("install");
        let lock_path = install_root.join("skills-lock.toml");
        let installed = install_for_removal(
            root.as_path(),
            install_root.as_path(),
            lock_path.as_path(),
            "DDDDDDDDDDDDDDDDDDDDD",
            "Missing Skill",
        );
        fs::remove_dir_all(installed.install_path.as_path()).expect("remove managed path first");

        let batch = stage_reversible_skill_removals(StageReversibleSkillRemovalsRequest {
            targets: vec![ReversibleSkillRemovalTarget {
                skill_id: installed.definition.identity.skill_id.clone(),
                slug: installed.definition.identity.slug.clone(),
                managed_install_path: Some(installed.install_path.clone()),
            }],
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
        })
        .expect("stage missing path");

        assert!(!batch.removals[0].was_present_on_disk);
        assert!(batch.removals[0].staged_path.is_none());
        rollback_reversible_skill_removals(&batch).expect("restore lock for missing path");
        assert!(
            find_lock_entry(
                &read_skills_lock(lock_path.as_path()).expect("read restored lock"),
                &installed.definition.identity.skill_id,
            )
            .is_some()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reversible_batch_removal_rejects_outside_noncanonical_and_overlapping_paths_before_moves() {
        let root = temp_case("reversible-removal-containment");
        let install_root = root.join("install");
        fs::create_dir_all(install_root.as_path()).expect("create install root");
        let outside = root.join("outside");
        fs::create_dir_all(outside.as_path()).expect("create outside path");
        let error = stage_reversible_skill_removals(StageReversibleSkillRemovalsRequest {
            targets: vec![ReversibleSkillRemovalTarget {
                skill_id: id("EEEEEEEEEEEEEEEEEEEEE"),
                slug: "outside".to_owned(),
                managed_install_path: Some(outside.clone()),
            }],
            install_root: install_root.clone(),
            lock_path: install_root.join("skills-lock.toml"),
        })
        .expect_err("outside path should fail")
        .to_string();
        assert!(error.contains("containment"));
        assert!(outside.exists());

        let wrong_canonical_path = install_root
            .join("EEEEEEEEEEEEEEEEEEEEE")
            .join("wrong-slug");
        fs::create_dir_all(wrong_canonical_path.as_path())
            .expect("create noncanonical managed path");
        let error = stage_reversible_skill_removals(StageReversibleSkillRemovalsRequest {
            targets: vec![ReversibleSkillRemovalTarget {
                skill_id: id("EEEEEEEEEEEEEEEEEEEEE"),
                slug: "expected-slug".to_owned(),
                managed_install_path: Some(wrong_canonical_path.clone()),
            }],
            install_root: install_root.clone(),
            lock_path: install_root.join("skills-lock.toml"),
        })
        .expect_err("noncanonical managed path should fail")
        .to_string();
        assert!(error.contains("is not the canonical path"));
        assert!(wrong_canonical_path.exists());

        let parent = install_root.join("parent");
        let child = parent.join("child");
        fs::create_dir_all(child.as_path()).expect("create overlapping paths");
        let error = stage_reversible_skill_removals(StageReversibleSkillRemovalsRequest {
            targets: vec![
                ReversibleSkillRemovalTarget {
                    skill_id: id("FFFFFFFFFFFFFFFFFFFFF"),
                    slug: "parent".to_owned(),
                    managed_install_path: Some(parent.clone()),
                },
                ReversibleSkillRemovalTarget {
                    skill_id: id("GGGGGGGGGGGGGGGGGGGGG"),
                    slug: "child".to_owned(),
                    managed_install_path: Some(child),
                },
            ],
            install_root,
            lock_path: root.join("lock.toml"),
        })
        .expect_err("overlapping paths should fail")
        .to_string();
        assert!(error.contains("non-overlapping"));
        assert!(parent.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reversible_batch_removal_move_and_lock_faults_restore_prior_state() {
        for (case_name, failpoints) in [
            ("move", vec![("stage_move", 1)]),
            ("lock", vec![("lock_write", 0)]),
        ] {
            let root = temp_case(case_name);
            let install_root = root.join("install");
            let lock_path = install_root.join("skills-lock.toml");
            let first = install_for_removal(
                root.as_path(),
                install_root.as_path(),
                lock_path.as_path(),
                "HHHHHHHHHHHHHHHHHHHHH",
                "First Fault Skill",
            );
            let second = install_for_removal(
                root.as_path(),
                install_root.as_path(),
                lock_path.as_path(),
                "IIIIIIIIIIIIIIIIIIIII",
                "Second Fault Skill",
            );
            let _guard = removal_failpoints(failpoints);

            let error = stage_reversible_skill_removals(StageReversibleSkillRemovalsRequest {
                targets: vec![
                    ReversibleSkillRemovalTarget {
                        skill_id: first.definition.identity.skill_id.clone(),
                        slug: first.definition.identity.slug.clone(),
                        managed_install_path: Some(first.install_path.clone()),
                    },
                    ReversibleSkillRemovalTarget {
                        skill_id: second.definition.identity.skill_id.clone(),
                        slug: second.definition.identity.slug.clone(),
                        managed_install_path: Some(second.install_path.clone()),
                    },
                ],
                install_root: install_root.clone(),
                lock_path: lock_path.clone(),
            })
            .expect_err("injected stage failure should fail")
            .to_string();

            assert!(error.contains("injected reversible removal failure"));
            assert!(first.install_path.exists());
            assert!(second.install_path.exists());
            let lock = read_skills_lock(lock_path.as_path()).expect("read unchanged lock");
            assert!(find_lock_entry(&lock, &first.definition.identity.skill_id).is_some());
            assert!(find_lock_entry(&lock, &second.definition.identity.skill_id).is_some());

            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn reversible_batch_removal_surfaces_rollback_finalize_and_compound_failures() {
        let root = temp_case("reversible-removal-fallible-cleanup");
        let install_root = root.join("install");
        let lock_path = install_root.join("skills-lock.toml");
        let installed = install_for_removal(
            root.as_path(),
            install_root.as_path(),
            lock_path.as_path(),
            "JJJJJJJJJJJJJJJJJJJJJ",
            "Fallible Skill",
        );
        let batch = stage_reversible_skill_removals(StageReversibleSkillRemovalsRequest {
            targets: vec![ReversibleSkillRemovalTarget {
                skill_id: installed.definition.identity.skill_id.clone(),
                slug: installed.definition.identity.slug.clone(),
                managed_install_path: Some(installed.install_path.clone()),
            }],
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
        })
        .expect("stage removal");
        {
            let _guard = removal_failpoints(vec![("rollback_move", 0)]);
            let error = rollback_reversible_skill_removals(&batch)
                .expect_err("rollback fault should surface")
                .to_string();
            assert!(error.contains("rollback_move"));
        }
        rollback_reversible_skill_removals(&batch).expect("retry rollback");
        assert!(installed.install_path.exists());

        let finalize_batch = stage_reversible_skill_removals(StageReversibleSkillRemovalsRequest {
            targets: vec![ReversibleSkillRemovalTarget {
                skill_id: installed.definition.identity.skill_id.clone(),
                slug: installed.definition.identity.slug.clone(),
                managed_install_path: Some(installed.install_path.clone()),
            }],
            install_root: install_root.clone(),
            lock_path: lock_path.clone(),
        })
        .expect("stage finalize removal");
        {
            let _guard = removal_failpoints(vec![("finalize_remove", 0)]);
            let error = finalize_reversible_skill_removals(&finalize_batch)
                .expect_err("finalize fault should surface")
                .to_string();
            assert!(error.contains("finalize_remove"));
        }
        finalize_reversible_skill_removals(&finalize_batch).expect("retry finalize");

        let _ = fs::remove_dir_all(root);

        let compound_root = temp_case("reversible-removal-compound");
        let compound_install = compound_root.join("install");
        let compound_lock = compound_install.join("skills-lock.toml");
        let first = install_for_removal(
            compound_root.as_path(),
            compound_install.as_path(),
            compound_lock.as_path(),
            "KKKKKKKKKKKKKKKKKKKKK",
            "Compound First",
        );
        let second = install_for_removal(
            compound_root.as_path(),
            compound_install.as_path(),
            compound_lock.as_path(),
            "LLLLLLLLLLLLLLLLLLLLL",
            "Compound Second",
        );
        let _guard = removal_failpoints(vec![("stage_move", 1), ("rollback_move", 0)]);
        let error = stage_reversible_skill_removals(StageReversibleSkillRemovalsRequest {
            targets: vec![
                ReversibleSkillRemovalTarget {
                    skill_id: first.definition.identity.skill_id.clone(),
                    slug: first.definition.identity.slug.clone(),
                    managed_install_path: Some(first.install_path.clone()),
                },
                ReversibleSkillRemovalTarget {
                    skill_id: second.definition.identity.skill_id.clone(),
                    slug: second.definition.identity.slug.clone(),
                    managed_install_path: Some(second.install_path.clone()),
                },
            ],
            install_root: compound_install,
            lock_path: compound_lock,
        })
        .expect_err("stage and rollback failures should be combined")
        .to_string();
        assert!(error.contains("stage_move"));
        assert!(error.contains("rollback also failed"));
        assert!(error.contains("rollback_move"));
        assert!(!first.install_path.exists());
        assert!(second.install_path.exists());
        let _ = fs::remove_dir_all(compound_root);
    }

    fn install_for_removal(
        case_root: &std::path::Path,
        install_root: &std::path::Path,
        lock_path: &std::path::Path,
        skill_id: &str,
        name: &str,
    ) -> super::InstallSkillResult {
        let source = case_root.join(format!("source-{skill_id}"));
        write_skill(source.as_path(), name, "removal test");
        install_skill(InstallSkillRequest {
            skill_id: id(skill_id),
            source_kind: SkillSourceKind::User,
            source_ref: format!("upload:{skill_id}"),
            source_path: source,
            install_root: install_root.to_path_buf(),
            lock_path: lock_path.to_path_buf(),
            now_unix: 1,
            policy: SkillInstallerPolicy::default(),
        })
        .expect("install removal fixture")
    }
}
