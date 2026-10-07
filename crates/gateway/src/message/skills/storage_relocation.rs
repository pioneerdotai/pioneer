use super::{MessageProcessor, installer_policy, resolve_root_path};
use anyhow::{Context, Result, bail};
use pioneer_crud::{CrudStore, SkillInstallationPatch, SkillInstallationRecord};
use pioneer_protocol::SkillId;
use pioneer_skills::{
    SkillInstallerPolicy, SkillLockEntry, SkillSourceKind, SkillTrustLevel,
    canonical_skill_install_path, read_skills_lock, remove_lock_entry, upsert_lock_entry,
    write_skills_lock_atomic_tracked,
};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::warn;

const IMPORT_PATH_PREFIX: &str = "import-path:";

#[derive(Debug, Clone)]
pub(crate) struct ConfiguredSkillImportRoot {
    pub(crate) source_kind: SkillSourceKind,
    pub(crate) scope_key: String,
    pub(crate) source_root: PathBuf,
    pub(crate) managed_root: PathBuf,
    pub(crate) source_is_pioneer_managed: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct ConfiguredRootImportConfig {
    pub(crate) roots: Vec<ConfiguredSkillImportRoot>,
    pub(crate) installer_policy: SkillInstallerPolicy,
    pub(crate) max_skills_per_root: usize,
    pub(crate) reserved_skill_ids: HashSet<SkillId>,
}

#[derive(Debug, Clone)]
pub(crate) struct ManagedSkillRoot {
    pub(crate) source_kind: SkillSourceKind,
    pub(crate) scope_key: String,
    pub(crate) managed_root: PathBuf,
}

#[derive(Debug, Clone)]
pub(crate) struct ManagedRootScanConfig {
    pub(crate) roots: Vec<ManagedSkillRoot>,
    pub(crate) installer_policy: SkillInstallerPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedSkillStorageMetadata {
    pub(crate) input_revision: pioneer_skills::SkillInputRevision,
    pub(crate) owner: Option<String>,
    pub(crate) slug: String,
    pub(crate) version: Option<String>,
    pub(crate) trust_level: String,
    pub(crate) fingerprint: String,
    pub(crate) source_ref: String,
}

#[derive(Debug, Clone)]
pub(crate) struct SkillStorageRelocationCandidate {
    pub(crate) expected_row: SkillInstallationRecord,
    pub(crate) source_path: PathBuf,
    pub(crate) install_root: PathBuf,
    pub(crate) destination: PathBuf,
    pub(crate) prepared_metadata: PreparedSkillStorageMetadata,
    pub(crate) remove_managed_source_after_switch: bool,
    pub(crate) managed_path_to_remove_after_switch: Option<PathBuf>,
    pub(crate) managed_lock_path: Option<PathBuf>,
    pub(crate) max_skill_file_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkillStorageRelocationOutcome {
    Switched,
    Stale,
}

impl MessageProcessor {
    pub(crate) fn configured_root_import_config(
        &self,
        workspace_id: &str,
    ) -> Result<ConfiguredRootImportConfig> {
        let context = self.skills_runtime_context(workspace_id)?;
        let system_managed_root = self
            .artifact_runtime_home
            .join("skills")
            .join("system")
            .join("imported");
        let mut roots = Vec::new();
        roots.extend(
            self.tool_loop_config
                .skills
                .system_import_roots
                .iter()
                .map(|raw| ConfiguredSkillImportRoot {
                    source_kind: SkillSourceKind::System,
                    scope_key: "system".to_owned(),
                    source_root: resolve_root_path(raw.as_str(), workspace_id),
                    managed_root: system_managed_root.clone(),
                    source_is_pioneer_managed: false,
                }),
        );
        roots.extend(
            self.tool_loop_config
                .skills
                .user_import_roots
                .iter()
                .map(|raw| {
                    configured_workspace_import_root(
                        raw,
                        workspace_id,
                        SkillSourceKind::User,
                        context.user_root.as_path(),
                    )
                }),
        );
        roots.extend(
            self.tool_loop_config
                .skills
                .registry_import_roots
                .iter()
                .map(|raw| {
                    configured_workspace_import_root(
                        raw,
                        workspace_id,
                        SkillSourceKind::Registry,
                        context.registry_root.as_path(),
                    )
                }),
        );
        Ok(ConfiguredRootImportConfig {
            roots,
            installer_policy: installer_policy(&context),
            max_skills_per_root: self.tool_loop_config.skills.max_skills_per_source.max(1),
            reserved_skill_ids: context
                .catalog_params
                .bundled
                .iter()
                .map(|entry| entry.skill_id.clone())
                .collect(),
        })
    }

    pub(crate) fn managed_root_scan_config(
        &self,
        workspace_id: &str,
    ) -> Result<ManagedRootScanConfig> {
        let context = self.skills_runtime_context(workspace_id)?;
        Ok(ManagedRootScanConfig {
            roots: vec![
                ManagedSkillRoot {
                    source_kind: SkillSourceKind::System,
                    scope_key: "system".to_owned(),
                    managed_root: self
                        .artifact_runtime_home
                        .join("skills")
                        .join("system")
                        .join("imported"),
                },
                ManagedSkillRoot {
                    source_kind: SkillSourceKind::User,
                    scope_key: workspace_id.to_owned(),
                    managed_root: context.user_root.clone(),
                },
                ManagedSkillRoot {
                    source_kind: SkillSourceKind::Registry,
                    scope_key: workspace_id.to_owned(),
                    managed_root: context.registry_root.clone(),
                },
            ],
            installer_policy: installer_policy(&context),
        })
    }
}

fn configured_workspace_import_root(
    raw: &str,
    workspace_id: &str,
    source_kind: SkillSourceKind,
    managed_root: &Path,
) -> ConfiguredSkillImportRoot {
    let source_root = resolve_root_path(raw, workspace_id);
    let source_is_pioneer_managed = normalize_absolute_path(source_root.as_path()).ok()
        == normalize_absolute_path(managed_root).ok();
    ConfiguredSkillImportRoot {
        source_kind,
        scope_key: workspace_id.to_owned(),
        source_root,
        managed_root: managed_root.to_path_buf(),
        source_is_pioneer_managed,
    }
}

pub(crate) fn row_metadata_matches(
    row: &SkillInstallationRecord,
    metadata: &PreparedSkillStorageMetadata,
) -> bool {
    row.owner == metadata.owner
        && row.slug == metadata.slug
        && row.version == metadata.version
        && row.trust_level == metadata.trust_level
        && row.fingerprint == metadata.fingerprint
        && row.source_ref == metadata.source_ref
}

pub(crate) fn normalize_import_source_path(path: &Path) -> Result<PathBuf> {
    fs::canonicalize(path)
        .with_context(|| format!("failed to resolve configured source `{}`", path.display()))
}

pub(crate) fn import_source_ref(path: &Path) -> Result<String> {
    let path = path
        .to_str()
        .context("configured skill source path must be valid UTF-8")?;
    Ok(format!("{IMPORT_PATH_PREFIX}{path}"))
}

pub(crate) fn trust_level_value(level: &SkillTrustLevel) -> &'static str {
    match level {
        SkillTrustLevel::Internal => "internal",
        SkillTrustLevel::Verified => "verified",
        SkillTrustLevel::Community => "community",
        SkillTrustLevel::Untrusted => "untrusted",
    }
}

#[cfg(test)]
thread_local! {
    pub(crate) static WATCH_PUBLICATION_FAULT: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
}
fn watch_publication_fault(_point: &str) -> bool {
    #[cfg(test)]
    {
        return WATCH_PUBLICATION_FAULT.with(|fault| fault.get() == Some(_point));
    }
    #[cfg(not(test))]
    {
        false
    }
}

/// Native reconciliation uses the existing installer lock, metadata/lock-file
/// rules and row fence. Replacing a tree is two same-filesystem renames; recursive
/// cleanup is returned to the watcher's bounded FS cursor, outside the lock.
pub(crate) enum PublicationProgress {
    Quantum(usize),
    Waiting(std::time::Instant),
    Finished(SkillStorageRelocationOutcome, usize),
}

// A publication is one owned stream. In particular, final input verification
// yields to the root worker while retaining its installer lock and backup.
pub(crate) fn publish_watched_candidate<'a>(
    crud_store: &'a CrudStore,
    skills_write_lock: &'a Arc<Mutex<()>>,
    candidate: SkillStorageRelocationCandidate,
    snapshot: pioneer_crud::SkillReconciliationSnapshot,
    workspace: Option<pioneer_entity::workspace::Model>,
    stage: Option<PathBuf>,
    recovery: Option<PathBuf>,
    stop: tokio_util::sync::CancellationToken,
    still_current: impl Fn() -> bool + Send + Sync + 'static,
    track_garbage: impl Fn(PathBuf, bool) -> Result<()> + Send + Sync + 'static,
    claim_path: impl Fn(&Path, bool) + Send + Sync + 'static,
) -> std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<PublicationProgress>> + Send + 'a>> {
    #[cfg(test)]
    let fault = WATCH_PUBLICATION_FAULT.with(|slot| slot.get());
    #[cfg(not(test))]
    let fault = None;
    let track_garbage = Arc::new(track_garbage);
    let claim_path = Arc::new(claim_path);
    Box::pin(async_stream::try_stream! {
        let lock = loop {
            if stop.is_cancelled() { Err::<(), anyhow::Error>(anyhow::anyhow!("skills watcher stopped"))?; }
            if let Ok(lock) = skills_write_lock.try_lock() { break lock; }
            // Another root can own this lock across verification quanta. Keep
            // this cursor, rather than blocking the only worker or spinning.
            yield PublicationProgress::Waiting(std::time::Instant::now() + std::time::Duration::from_secs(5));
        };
        if !still_current() { yield PublicationProgress::Finished(SkillStorageRelocationOutcome::Stale, 0); return; }
        let check = candidate.clone(); let same_path = stage.is_none();
        super::watcher::owned_fs(&stop, move || validate_candidate_paths(&check, same_path)).await?;
        for path in std::iter::once(candidate.source_path.clone()).chain(stage.clone()) {
            let mut check = verify_skill_inputs(path, candidate.prepared_metadata.input_revision.clone(), candidate.max_skill_file_bytes, stop.clone());
            while let Some(bytes) = futures_util::StreamExt::next(&mut check).await {
                let (bytes, done) = bytes?;
                yield PublicationProgress::Quantum(bytes);
                if done { break; }
            }
        }
        if !still_current() { yield PublicationProgress::Finished(SkillStorageRelocationOutcome::Stale, 0); return; }
        let confirm_only = recovery.is_some() && candidate.source_path == candidate.destination
            && row_metadata_matches(&snapshot.record, &candidate.prepared_metadata)
            && normalize_absolute_path(Path::new(&snapshot.record.install_path))? == candidate.destination;
        let tracker = track_garbage.clone(); let claim = claim_path.clone();
        let publication = publication_fs(&stop, fault, move || Publication::start(candidate, stage, recovery, confirm_only, tracker.as_ref(), claim.as_ref())).await?;
        yield PublicationProgress::Quantum(0);
        let mut check = verify_skill_inputs(publication.candidate.destination.clone(), publication.candidate.prepared_metadata.input_revision.clone(), publication.candidate.max_skill_file_bytes, stop.clone());
        let mut validation = Ok(());
        let mut final_bytes = 0;
        while let Some(result) = futures_util::StreamExt::next(&mut check).await {
            match result {
                Ok((bytes, done)) => {
                    if done { final_bytes = bytes; break; }
                    yield PublicationProgress::Quantum(bytes);
                }
                Err(error) => { validation = Err(error); break; }
            }
        }
        let started = std::sync::atomic::AtomicBool::new(false);
        let patch = candidate_database_patch(&publication.candidate);
        let database_work = async {
            if publication.confirm_only {
                crud_store.confirm_skill_reconciliation(&snapshot, workspace.as_ref(), &still_current, &started).await
            } else {
                crud_store.reconcile_skill_installation_with_progress(&snapshot, &patch, publication.now, workspace.as_ref(), &still_current, &started).await
            }
        };
        let mut switched = if let Err(error) = validation {
            Err(pioneer_crud::SkillReconciliationError { outcome: pioneer_crud::SkillReconciliationFailure::NotCommitted, error }.into())
        } else {
            tokio::select! { biased;
                _ = stop.cancelled() => Err(pioneer_crud::SkillReconciliationError {
                    outcome: if started.load(std::sync::atomic::Ordering::Acquire) { pioneer_crud::SkillReconciliationFailure::Unknown } else { pioneer_crud::SkillReconciliationFailure::NotCommitted },
                    error: anyhow::anyhow!("skills publication cancelled"),
                }.into()),
                result = database_work => result,
            }
        };
        if fault == Some("commit_ack") && matches!(switched, Ok(true)) {
            switched = Err(pioneer_crud::SkillReconciliationError { outcome: pioneer_crud::SkillReconciliationFailure::Unknown, error: anyhow::anyhow!("injected lost commit acknowledgement") }.into());
        }
        // Keep the final verification adjacent to the DB guard, then end that
        // outer quantum before finish reads/updates any recovery marker. This
        // also separates compensation from a partially failed verification read.
        yield PublicationProgress::Quantum(final_bytes);
        let tracker = track_garbage.clone(); let claim = claim_path.clone();
        // Compensation must still run when stop is cancelled. Always join it.
        let outcome = publication_fs(&tokio_util::sync::CancellationToken::new(), fault, move || publication.finish(switched, tracker.as_ref(), claim.as_ref())).await?;
        drop(lock);
        yield PublicationProgress::Finished(outcome, 0);
    })
}

async fn publication_fs<T: Send + 'static>(
    stop: &tokio_util::sync::CancellationToken,
    _fault: Option<&'static str>,
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    super::watcher::owned_fs(stop, move || {
        #[cfg(test)]
        struct Reset(Option<&'static str>);
        #[cfg(test)]
        impl Drop for Reset {
            fn drop(&mut self) {
                WATCH_PUBLICATION_FAULT.with(|slot| slot.set(self.0));
            }
        }
        #[cfg(test)]
        let _reset = Reset(WATCH_PUBLICATION_FAULT.with(|slot| slot.replace(_fault)));
        work()
    })
    .await
}

struct Publication {
    candidate: SkillStorageRelocationCandidate,
    stage: Option<PathBuf>,
    recovery: PathBuf,
    backup: Option<PathBuf>,
    previous_lock: Option<SkillLockEntry>,
    renamed: bool,
    reused_recovery: bool,
    confirm_only: bool,
    now: i64,
}
impl Publication {
    fn start(
        candidate: SkillStorageRelocationCandidate,
        stage: Option<PathBuf>,
        recovery: Option<PathBuf>,
        confirm_only: bool,
        track_garbage: &dyn Fn(PathBuf, bool) -> Result<()>,
        claim_path: &dyn Fn(&Path, bool),
    ) -> Result<Self> {
        claim_path(&candidate.destination, false);
        claim_path(&candidate.source_path, false);
        if let Some(parent) = candidate.destination.parent() {
            claim_path(parent, false);
        }
        let reused_recovery = recovery.is_some();
        let recovery = if let Some(recovery) = recovery {
            recovery
        } else if let Some(stage) = &stage {
            stage
                .parent()
                .context("stage wrapper missing")?
                .to_path_buf()
        } else {
            let parent = candidate
                .destination
                .parent()
                .context("destination parent missing")?;
            fs::create_dir_all(parent)?;
            super::watcher::reconcile::new_attempt_tracked(
                parent,
                candidate.install_root.clone(),
                |path| {
                    claim_path(path, true);
                    Ok(())
                },
            )?
        };
        track_garbage(recovery.clone(), false)?;
        super::watcher::reconcile::set_attempt_skill(
            &recovery,
            candidate.expected_row.skill_id.clone(),
        )?;
        super::watcher::reconcile::set_attempt_publishing(&recovery, true)?;
        let renamed = stage.is_none() && candidate.source_path != candidate.destination;
        let mut backup = None;
        if (stage.is_some() || renamed) && fs::symlink_metadata(&candidate.destination).is_ok() {
            let path = recovery.join("backup");
            fs::rename(&candidate.destination, &path)?;
            backup = Some(path);
        }
        let published = if let Some(stage) = &stage {
            fs::rename(stage, &candidate.destination)
        } else if renamed {
            fs::rename(&candidate.source_path, &candidate.destination)
        } else {
            Ok(())
        };
        if let Err(error) = published {
            if let Some(backup) = &backup {
                fs::rename(backup, &candidate.destination).context(
                    "publication failed and backup restoration failed; recovery retained",
                )?;
            }
            if !reused_recovery {
                super::watcher::reconcile::set_attempt_publishing(&recovery, false)?;
            }
            return Err(error.into());
        }
        let rollback_files = || -> Result<()> {
            if watch_publication_fault("restore_files") {
                bail!("injected file restoration failure");
            }
            if let Some(stage) = &stage {
                fs::rename(&candidate.destination, stage)
                    .context("failed to restore staged skill")?;
            } else if renamed {
                fs::rename(&candidate.destination, &candidate.source_path)
                    .context("failed to restore managed source")?;
            }
            if let Some(backup) = &backup {
                fs::rename(backup, &candidate.destination)
                    .context("failed to restore skill backup")?;
            }
            Ok(())
        };
        let now = crate::message::now_timestamp_secs();
        let previous_lock = match write_candidate_lock_entry_with_recovery(
            &candidate,
            now,
            Some(&recovery),
            claim_path,
        ) {
            Ok(previous) => previous,
            Err(error) => {
                if let Err(restore) = rollback_files() {
                    return Err(pioneer_crud::SkillReconciliationError {
                        outcome: pioneer_crud::SkillReconciliationFailure::NotCommitted,
                        error: restore.context(format!("lock publication failed ({error:#}); file restoration failed; recovery retained")),
                    }.into());
                }
                if !reused_recovery {
                    super::watcher::reconcile::set_attempt_publishing(&recovery, false)?;
                }
                return Err(pioneer_crud::SkillReconciliationError {
                    outcome: pioneer_crud::SkillReconciliationFailure::NotCommitted,
                    error: error.context("skill lock publication failed before database operation"),
                }
                .into());
            }
        };
        Ok(Self {
            candidate,
            stage,
            recovery,
            backup,
            previous_lock,
            renamed,
            reused_recovery,
            confirm_only,
            now,
        })
    }
    fn finish(
        self,
        switched: Result<bool>,
        track_garbage: &dyn Fn(PathBuf, bool) -> Result<()>,
        claim_path: &dyn Fn(&Path, bool),
    ) -> Result<SkillStorageRelocationOutcome> {
        let Self {
            candidate,
            stage,
            recovery,
            backup,
            previous_lock,
            renamed,
            reused_recovery,
            ..
        } = self;
        let rollback_files = || -> Result<()> {
            if watch_publication_fault("restore_files") {
                bail!("injected file restoration failure");
            }
            if let Some(stage) = &stage {
                fs::rename(&candidate.destination, stage)
                    .context("failed to restore staged skill")?;
            } else if renamed {
                fs::rename(&candidate.destination, &candidate.source_path)
                    .context("failed to restore managed source")?;
            }
            if let Some(backup) = &backup {
                fs::rename(backup, &candidate.destination)
                    .context("failed to restore skill backup")?;
            }
            Ok(())
        };
        match switched {
            Ok(true) => {
                // A post-commit marker/cleanup error does not undo an acknowledged
                // domain change or hide its notification. Ownership records the proof.
                if track_garbage(recovery.clone(), true).is_err()
                    || super::watcher::reconcile::set_attempt_publishing(&recovery, false).is_err()
                {
                    warn!("committed skill artifact cleanup deferred");
                }
            }
            Ok(false) => {
                restore_candidate_lock_entry_checked(
                    &candidate,
                    previous_lock.as_ref(),
                    claim_path,
                )
                .and_then(|()| rollback_files())
                .map_err(|error| pioneer_crud::SkillReconciliationError {
                    outcome: pioneer_crud::SkillReconciliationFailure::NotCommitted,
                    error: error.context("stale publication compensation failed; backup retained"),
                })?;
                if !reused_recovery {
                    super::watcher::reconcile::set_attempt_publishing(&recovery, false)?;
                }
                return Ok(SkillStorageRelocationOutcome::Stale);
            }
            Err(error) => {
                if error
                    .downcast_ref::<pioneer_crud::SkillReconciliationError>()
                    .is_some_and(|error| {
                        error.outcome == pioneer_crud::SkillReconciliationFailure::NotCommitted
                    })
                {
                    if let Err(restore) = restore_candidate_lock_entry_checked(
                        &candidate,
                        previous_lock.as_ref(),
                        claim_path,
                    )
                    .and_then(|()| rollback_files())
                    {
                        return Err(pioneer_crud::SkillReconciliationError {
                        outcome: pioneer_crud::SkillReconciliationFailure::NotCommitted,
                        error: restore.context(format!("publication rejected ({error:#}); compensation failed and backup retained")),
                    }.into());
                    }
                    if !reused_recovery {
                        super::watcher::reconcile::set_attempt_publishing(&recovery, false)?;
                    }
                }
                return Err(error);
            }
        }
        let mut old_paths = Vec::new();
        if candidate.remove_managed_source_after_switch
            && candidate.source_path != candidate.destination
        {
            old_paths.push(candidate.source_path.clone());
        }
        if let Some(path) = candidate.managed_path_to_remove_after_switch {
            old_paths.push(path);
        }
        for path in old_paths {
            if path != candidate.destination
                && path_is_existing_descendant(&candidate.install_root, &path)
            {
                let Some(parent) = path.parent() else {
                    warn!("committed skill source cleanup deferred: missing parent");
                    continue;
                };
                let garbage = match super::watcher::reconcile::new_attempt_tracked(
                    parent,
                    candidate.install_root.clone(),
                    |path| {
                        claim_path(path, true);
                        Ok(())
                    },
                ) {
                    Ok(path) => path,
                    Err(_) => {
                        warn!("committed skill source cleanup deferred");
                        continue;
                    }
                };
                claim_path(&path, false);
                if track_garbage(garbage.clone(), true).is_err()
                    || fs::rename(&path, garbage.join("payload")).is_err()
                {
                    warn!("committed skill source cleanup deferred");
                }
            }
        }
        Ok(SkillStorageRelocationOutcome::Switched)
    }
}

fn candidate_database_patch(candidate: &SkillStorageRelocationCandidate) -> SkillInstallationPatch {
    let install_path = Some(candidate.destination.display().to_string());
    SkillInstallationPatch {
        owner: Some(candidate.prepared_metadata.owner.clone()),
        slug: Some(candidate.prepared_metadata.slug.clone()),
        version: Some(candidate.prepared_metadata.version.clone()),
        source_ref: Some(candidate.prepared_metadata.source_ref.clone()),
        install_path,
        trust_level: Some(candidate.prepared_metadata.trust_level.clone()),
        fingerprint: Some(candidate.prepared_metadata.fingerprint.clone()),
        ..Default::default()
    }
}

fn write_candidate_lock_entry_with_recovery(
    candidate: &SkillStorageRelocationCandidate,
    now: i64,
    recovery: Option<&Path>,
    claim_path: &dyn Fn(&Path, bool),
) -> Result<Option<SkillLockEntry>> {
    let Some(lock_path) = candidate.managed_lock_path.as_deref() else {
        return Ok(None);
    };
    let mut lock = read_skills_lock(lock_path)?;
    let previous = lock
        .entries
        .iter()
        .find(|entry| entry.skill_id == candidate.expected_row.skill_id)
        .cloned();
    if let Some(recovery) = recovery {
        let path = recovery.join("previous-lock-entry.json");
        // Preserve the original entry (including installed_at) if compensation
        // fails and the worker stops. Reusing an uncertain backup must not replace
        // the only original lock facts with the later published entry.
        if !path.try_exists()? {
            use std::io::Write;
            let mut file = tempfile::NamedTempFile::new_in(recovery)?;
            file.write_all(&serde_json::to_vec(&previous)?)?;
            file.as_file().sync_all()?;
            file.persist(&path).map_err(|error| error.error)?;
            #[cfg(unix)]
            fs::File::open(recovery)?.sync_all()?;
        }
    }
    let installed_at = previous
        .as_ref()
        .map(|entry| entry.installed_at)
        .unwrap_or(now);
    upsert_lock_entry(
        &mut lock,
        SkillLockEntry {
            skill_id: candidate.expected_row.skill_id.clone(),
            owner: candidate.prepared_metadata.owner.clone(),
            slug: candidate.prepared_metadata.slug.clone(),
            source_kind: candidate.expected_row.source_kind.clone(),
            source_ref: candidate.prepared_metadata.source_ref.clone(),
            install_path: candidate.destination.display().to_string(),
            version: candidate.prepared_metadata.version.clone(),
            trust_level: parse_trust_level(candidate.prepared_metadata.trust_level.as_str())?,
            fingerprint: candidate.prepared_metadata.fingerprint.clone(),
            installed_at,
        },
    );
    write_skills_lock_atomic_tracked(lock_path, &lock, |path| claim_path(path, false))?;
    Ok(previous)
}

fn restore_candidate_lock_entry_checked(
    candidate: &SkillStorageRelocationCandidate,
    previous: Option<&SkillLockEntry>,
    claim_path: &dyn Fn(&Path, bool),
) -> Result<()> {
    if watch_publication_fault("restore_lock") {
        bail!("injected lock restoration failure");
    }
    let Some(lock_path) = candidate.managed_lock_path.as_deref() else {
        return Ok(());
    };
    (|| -> Result<()> {
        let mut lock = read_skills_lock(lock_path)?;
        if let Some(previous) = previous {
            upsert_lock_entry(&mut lock, previous.clone());
        } else {
            remove_lock_entry(&mut lock, &candidate.expected_row.skill_id);
        }
        write_skills_lock_atomic_tracked(lock_path, &lock, |path| claim_path(path, false))
    })()
}
fn parse_trust_level(value: &str) -> Result<SkillTrustLevel> {
    match value {
        "internal" => Ok(SkillTrustLevel::Internal),
        "verified" => Ok(SkillTrustLevel::Verified),
        "community" => Ok(SkillTrustLevel::Community),
        "untrusted" => Ok(SkillTrustLevel::Untrusted),
        other => bail!("unsupported skill trust level `{other}`"),
    }
}

fn validate_candidate_paths(
    candidate: &SkillStorageRelocationCandidate,
    allow_same_path: bool,
) -> Result<()> {
    if !candidate.source_path.is_dir() {
        bail!(
            "skill relocation source `{}` is not a directory",
            candidate.source_path.display()
        );
    }
    let expected_destination = canonical_skill_install_path(
        candidate.install_root.as_path(),
        &candidate.expected_row.skill_id,
        candidate.prepared_metadata.slug.as_str(),
    )?;
    if normalize_absolute_path(expected_destination.as_path())? != candidate.destination {
        bail!(
            "skill relocation destination `{}` is not canonical for `{}`",
            candidate.destination.display(),
            candidate.expected_row.skill_id
        );
    }
    if candidate.source_path == candidate.destination && !allow_same_path {
        bail!("canonical skill installation does not require relocation");
    }
    Ok(())
}

// FS publication guard, outside database capacity. This hashes only the two
// metadata inputs, without reparsing or rescanning security for each scope.
// External writers can still edit after this boundary; SQLite cannot lock them.
pub(crate) fn verify_skill_inputs(
    package: PathBuf,
    expected: pioneer_skills::SkillInputRevision,
    max_file_bytes: usize,
    stop: tokio_util::sync::CancellationToken,
) -> std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<(usize, bool)>> + Send>> {
    Box::pin(async_stream::try_stream! {
        let mut cursor = InputRevisionCheck::new(package, expected, max_file_bytes);
        loop {
            let (next, done, bytes) = super::watcher::owned_fs(&stop, move || {
                let (done, bytes) = cursor.step()?;
                Ok((cursor, done, bytes))
            }).await?;
            cursor = next;
            yield (bytes, done);
            if done { break; }
        }
    })
}

// Two metadata files share one byte budget. No parser or whole-package scan.
struct InputRevisionCheck {
    package: PathBuf,
    expected: pioneer_skills::SkillInputRevision,
    limit: usize,
    index: usize,
    file: Option<(fs::File, fs::Metadata, Sha256, usize)>,
    #[cfg(test)]
    read_bytes: usize,
}
impl InputRevisionCheck {
    fn new(package: PathBuf, expected: pioneer_skills::SkillInputRevision, limit: usize) -> Self {
        Self {
            package,
            expected,
            limit: limit.max(1),
            index: 0,
            file: None,
            #[cfg(test)]
            read_bytes: 0,
        }
    }
    fn step(&mut self) -> Result<(bool, usize)> {
        let mut consumed = 0;
        while self.index < 2 && consumed < 256 * 1024 {
            let (name, expected) = if self.index == 0 {
                ("SKILL.md", Some(self.expected.skill))
            } else {
                ("_meta.json", self.expected.sidecar)
            };
            if self.file.is_none() {
                let path = self.package.join(name);
                let metadata = match fs::symlink_metadata(&path) {
                    Err(error)
                        if error.kind() == std::io::ErrorKind::NotFound && expected.is_none() =>
                    {
                        self.index += 1;
                        continue;
                    }
                    Err(error) => Err::<_, anyhow::Error>(error.into())?,
                    Ok(metadata) => metadata,
                };
                if expected.is_none() || !metadata.is_file() || metadata.file_type().is_symlink() {
                    bail!("skill metadata input changed");
                }
                if metadata.len() > self.limit as u64 {
                    bail!("skill metadata input exceeds max_install_file_bytes");
                }
                let mut options = fs::OpenOptions::new();
                options.read(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.custom_flags(libc::O_NOFOLLOW);
                }
                let file = options.open(path)?;
                let initial = file.metadata()?;
                self.file = Some((file, initial, Sha256::new(), 0));
            }
            let (file, initial, digest, bytes) = self.file.as_mut().expect("input revision cursor");
            let allowance = self.limit.saturating_sub(*bytes).saturating_add(1);
            let mut buffer = vec![0; allowance.min(64 * 1024).min(256 * 1024 - consumed)];
            let read = file.read(&mut buffer)?;
            #[cfg(test)]
            super::watcher::reconcile::record_fs_read(&self.package.join(name), read);
            consumed += read;
            #[cfg(test)]
            {
                self.read_bytes += read;
            }
            *bytes += read;
            if *bytes > self.limit {
                bail!("skill metadata input exceeds max_install_file_bytes");
            }
            if read == 0 {
                let current = self.package.join(name);
                let metadata = fs::symlink_metadata(&current)?;
                let after = file.metadata()?;
                if !metadata.is_file()
                    || metadata.file_type().is_symlink()
                    || initial.len() != after.len()
                    || initial.modified().ok() != after.modified().ok()
                    || same_file::Handle::from_file(file.try_clone()?)?
                        != same_file::Handle::from_path(current)?
                {
                    bail!("skill metadata input replaced while validating");
                }
                let actual: [u8; 32] = digest.clone().finalize().into();
                if Some(actual) != expected {
                    bail!("skill metadata inputs changed since preparation");
                }
                self.file = None;
                self.index += 1;
            } else {
                digest.update(&buffer[..read]);
            }
        }
        Ok((self.index == 2, consumed))
    }
}

fn path_is_existing_descendant(root: &Path, path: &Path) -> bool {
    let Ok(root) = fs::canonicalize(root) else {
        return false;
    };
    let Ok(path) = fs::canonicalize(path) else {
        return false;
    };
    path != root && path.starts_with(root)
}

pub(crate) fn normalize_absolute_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("failed to resolve current directory for skill relocation")?
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

#[cfg(test)]
mod input_revision_tests {
    use super::*;
    use futures_util::StreamExt;

    fn revision(skill: &[u8], sidecar: Option<&[u8]>) -> pioneer_skills::SkillInputRevision {
        pioneer_skills::SkillInputRevision {
            skill: Sha256::digest(skill).into(),
            sidecar: sidecar.map(|bytes| Sha256::digest(bytes).into()),
        }
    }

    #[test]
    fn two_large_inputs_share_actual_byte_budget_and_detect_growth_and_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let skill = vec![b'x'; 600 * 1024];
        let sidecar = vec![b'y'; 700 * 1024];
        fs::write(directory.path().join("SKILL.md"), &skill).unwrap();
        fs::write(directory.path().join("_meta.json"), &sidecar).unwrap();
        let expected = revision(&skill, Some(&sidecar));
        let mut cursor = InputRevisionCheck::new(
            directory.path().to_path_buf(),
            expected.clone(),
            1024 * 1024,
        );
        let mut quanta = 0;
        loop {
            let before = cursor.read_bytes;
            let (done, bytes) = cursor.step().unwrap();
            assert_eq!(bytes, cursor.read_bytes - before);
            assert!(bytes <= 256 * 1024);
            quanta += 1;
            if done {
                break;
            }
        }
        assert!(quanta >= 6);
        assert_eq!(cursor.read_bytes, skill.len() + sidecar.len());

        let mut cursor =
            InputRevisionCheck::new(directory.path().to_path_buf(), expected.clone(), 650 * 1024);
        assert!(!cursor.step().unwrap().0);
        fs::write(directory.path().join("SKILL.md"), vec![b'x'; 900 * 1024]).unwrap();
        while cursor.step().is_ok() {}
        assert_eq!(
            cursor.read_bytes,
            650 * 1024 + 1,
            "stat cannot permit unlimited growth reads"
        );

        fs::write(directory.path().join("SKILL.md"), &skill).unwrap();
        let mut cursor =
            InputRevisionCheck::new(directory.path().to_path_buf(), expected, 1024 * 1024);
        cursor.step().unwrap();
        let replacement = directory.path().join("replacement");
        fs::write(&replacement, &skill).unwrap();
        fs::rename(replacement, directory.path().join("SKILL.md")).unwrap();
        assert!(cursor.step().is_ok());
        assert!(
            cursor.step().is_err(),
            "a correct hash of an unlinked old inode is insufficient"
        );
    }

    #[tokio::test]
    async fn revision_cursor_cancellation_and_sidecar_presence_keep_an_obligation() {
        let directory = tempfile::tempdir().unwrap();
        let skill = vec![b'x'; 600 * 1024];
        fs::write(directory.path().join("SKILL.md"), &skill).unwrap();
        let stop = tokio_util::sync::CancellationToken::new();
        let mut check = verify_skill_inputs(
            directory.path().to_path_buf(),
            revision(&skill, None),
            1024 * 1024,
            stop.clone(),
        );
        assert_eq!(check.next().await.unwrap().unwrap(), (256 * 1024, false));
        stop.cancel();
        assert!(check.next().await.unwrap().is_err());
        for present in [false, true] {
            let meta = directory.path().join("_meta.json");
            if present {
                fs::write(&meta, b"{}").unwrap();
            } else if meta.exists() {
                fs::remove_file(&meta).unwrap();
            }
            let mut check = verify_skill_inputs(
                directory.path().to_path_buf(),
                revision(&skill, present.then_some(b"{}".as_slice())),
                1024 * 1024,
                Default::default(),
            );
            assert_eq!(check.next().await.unwrap().unwrap().0, 256 * 1024);
            if present {
                fs::remove_file(&meta).unwrap();
            } else {
                fs::write(&meta, b"{}").unwrap();
            }
            let mut rejected = false;
            while let Some(result) = check.next().await {
                if result.is_err() {
                    rejected = true;
                    break;
                }
            }
            assert!(rejected, "sidecar absence/presence is part of the revision");
        }
    }
}
