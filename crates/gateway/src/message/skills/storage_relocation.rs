use super::{MessageProcessor, installer_policy, resolve_root_path};
use anyhow::{Context, Result, bail};
use pioneer_crud::{CrudStore, SkillInstallationPatch, SkillInstallationRecord};
use pioneer_protocol::SkillId;
use pioneer_skills::{
    PrepareMaterializedSkillRequest, SkillInstallerPolicy, SkillLockEntry, SkillSourceKind,
    SkillTrustLevel, canonical_skill_install_path, normalize_skill_slug, parse_skill_from_file,
    prepare_materialized_skill, read_skills_lock, remove_lock_entry, upsert_lock_entry,
    write_skills_lock_atomic,
};
use std::collections::HashSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::warn;

const IMPORT_PATH_PREFIX: &str = "import-path:";
const RELOCATION_ATTEMPT_SUFFIX: &str = ".pioneer-relocation-attempt";

#[derive(Debug, Clone)]
pub(crate) struct ExistingInstallationRelocationConfig {
    pub(crate) user_root_templates: Vec<String>,
    pub(crate) registry_root_templates: Vec<String>,
    pub(crate) max_skill_file_bytes: usize,
}

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
    pub(crate) force_refresh: bool,
    pub(crate) mode: ConfiguredRootImportMode,
    pub(crate) reserved_skill_ids: HashSet<SkillId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConfiguredRootImportMode {
    RegisterOnly,
    ImportAndRefresh,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ConfiguredRootImportSummary {
    pub(crate) packages_discovered: usize,
    pub(crate) rows_created: usize,
    pub(crate) switched: usize,
    pub(crate) unchanged: usize,
    pub(crate) stale: usize,
    pub(crate) failed: usize,
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ManagedRootScanSummary {
    pub(crate) rows_seen: usize,
    pub(crate) refreshed: usize,
    pub(crate) renamed: usize,
    pub(crate) unavailable: usize,
    pub(crate) unchanged: usize,
    pub(crate) stale: usize,
    pub(crate) failed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkillStorageCandidateMode {
    ExistingRelocation,
    ImportOrRefresh,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedSkillStorageMetadata {
    pub(crate) owner: Option<String>,
    pub(crate) slug: String,
    pub(crate) version: Option<String>,
    pub(crate) trust_level: String,
    pub(crate) fingerprint: String,
    pub(crate) source_ref: String,
}

impl PreparedSkillStorageMetadata {
    pub(crate) fn from_row(row: &SkillInstallationRecord) -> Self {
        Self {
            owner: row.owner.clone(),
            slug: row.slug.clone(),
            version: row.version.clone(),
            trust_level: row.trust_level.clone(),
            fingerprint: row.fingerprint.clone(),
            source_ref: row.source_ref.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SkillStorageRelocationCandidate {
    pub(crate) mode: SkillStorageCandidateMode,
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

#[derive(Debug)]
struct PreparedSkillStorageCopy {
    candidate: SkillStorageRelocationCandidate,
    attempt_path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkillStorageRelocationOutcome {
    Switched,
    Stale,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ExistingInstallationRelocationSummary {
    pub(crate) rows_seen: usize,
    pub(crate) skipped_canonical: usize,
    pub(crate) skipped_pending_import: usize,
    pub(crate) skipped_unsupported: usize,
    pub(crate) switched: usize,
    pub(crate) stale: usize,
    pub(crate) failed: usize,
}

impl MessageProcessor {
    pub(crate) fn configured_root_import_config(
        &self,
        workspace_id: &str,
        force_refresh: bool,
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
            force_refresh,
            mode: ConfiguredRootImportMode::ImportAndRefresh,
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

pub(crate) async fn relocate_existing_installations(
    crud_store: &CrudStore,
    skills_write_lock: &Arc<Mutex<()>>,
    config: &ExistingInstallationRelocationConfig,
) -> Result<ExistingInstallationRelocationSummary> {
    let rows = crud_store.list_skill_installations().await?;
    let mut summary = ExistingInstallationRelocationSummary::default();

    for row in rows {
        summary.rows_seen = summary.rows_seen.saturating_add(1);
        if row.source_ref.starts_with(IMPORT_PATH_PREFIX) {
            summary.skipped_pending_import = summary.skipped_pending_import.saturating_add(1);
            continue;
        }

        let Some(install_root_template) = install_root_template_for_row(&row, config) else {
            summary.skipped_unsupported = summary.skipped_unsupported.saturating_add(1);
            continue;
        };
        let install_root = resolve_root_path(install_root_template, row.scope_key.as_str());
        let destination = match canonical_skill_install_path(
            install_root.as_path(),
            &row.skill_id,
            row.slug.as_str(),
        )
        .and_then(|path| normalize_absolute_path(path.as_path()))
        {
            Ok(destination) => destination,
            Err(error) => {
                summary.failed = summary.failed.saturating_add(1);
                warn!(
                    skill_id = %row.skill_id,
                    error = %format!("{error:#}"),
                    "failed to derive canonical skill relocation destination"
                );
                continue;
            }
        };
        let source_path = match normalize_absolute_path(Path::new(row.install_path.as_str())) {
            Ok(source_path) => source_path,
            Err(error) => {
                summary.failed = summary.failed.saturating_add(1);
                warn!(
                    skill_id = %row.skill_id,
                    source_path = %row.install_path,
                    error = %format!("{error:#}"),
                    "failed to normalize skill relocation source"
                );
                continue;
            }
        };
        if source_path == destination {
            summary.skipped_canonical = summary.skipped_canonical.saturating_add(1);
            continue;
        }

        let remove_managed_source_after_switch =
            path_is_existing_descendant(install_root.as_path(), source_path.as_path());
        let prepared_metadata = PreparedSkillStorageMetadata::from_row(&row);
        let skill_id = row.skill_id.clone();
        let stored_source_path = row.install_path.clone();
        let managed_lock_path = install_root.join("skills-lock.toml");
        let candidate = SkillStorageRelocationCandidate {
            mode: SkillStorageCandidateMode::ExistingRelocation,
            expected_row: row,
            source_path,
            install_root,
            destination,
            prepared_metadata,
            remove_managed_source_after_switch,
            managed_path_to_remove_after_switch: None,
            managed_lock_path: Some(managed_lock_path),
            max_skill_file_bytes: config.max_skill_file_bytes.max(1),
        };

        match copy_and_switch_candidate(crud_store, skills_write_lock, candidate).await {
            Ok(SkillStorageRelocationOutcome::Switched) => {
                summary.switched = summary.switched.saturating_add(1);
            }
            Ok(SkillStorageRelocationOutcome::Stale) => {
                summary.stale = summary.stale.saturating_add(1);
            }
            Err(error) => {
                summary.failed = summary.failed.saturating_add(1);
                warn!(
                    skill_id = %skill_id,
                    source_path = %stored_source_path,
                    error = %format!("{error:#}"),
                    "failed to relocate installed skill"
                );
            }
        }
    }

    Ok(summary)
}

pub(crate) async fn import_configured_skill_roots(
    crud_store: &CrudStore,
    skills_write_lock: &Arc<Mutex<()>>,
    config: &ConfiguredRootImportConfig,
) -> Result<ConfiguredRootImportSummary> {
    let mut summary = ConfiguredRootImportSummary::default();
    let mut seen_sources = HashSet::new();

    for root in &config.roots {
        let packages = match discover_configured_packages(
            root.source_root.as_path(),
            config.max_skills_per_root.max(1),
        ) {
            Ok(packages) => packages,
            Err(error) => {
                summary.failed = summary.failed.saturating_add(1);
                warn!(
                    root = %root.source_root.display(),
                    error = %format!("{error:#}"),
                    "failed to discover configured skill import root"
                );
                continue;
            }
        };

        for source_path in packages {
            summary.packages_discovered = summary.packages_discovered.saturating_add(1);
            let source_path = match normalize_import_source_path(source_path.as_path()) {
                Ok(path) => path,
                Err(error) => {
                    summary.failed = summary.failed.saturating_add(1);
                    warn!(
                        path = %source_path.display(),
                        error = %format!("{error:#}"),
                        "failed to normalize configured skill source"
                    );
                    continue;
                }
            };
            let source_ref = match import_source_ref(source_path.as_path()) {
                Ok(source_ref) => source_ref,
                Err(error) => {
                    summary.failed = summary.failed.saturating_add(1);
                    warn!(
                        path = %source_path.display(),
                        error = %format!("{error:#}"),
                        "configured skill source path is unsupported"
                    );
                    continue;
                }
            };
            let source_key = format!(
                "{}:{}:{}",
                root.source_kind.as_db_value(),
                root.scope_key,
                source_ref
            );
            if !seen_sources.insert(source_key) {
                continue;
            }
            if root.source_is_pioneer_managed
                && managed_source_has_database_container(crud_store, root, source_path.as_path())
                    .await?
            {
                // Every leaf below a DB-backed ID container belongs exclusively to the managed
                // scanner. This prevents an extra or ambiguous leaf from becoming a new install.
                summary.unchanged = summary.unchanged.saturating_add(1);
                continue;
            }

            match import_one_configured_package(
                crud_store,
                skills_write_lock,
                root,
                &config.installer_policy,
                config.force_refresh,
                config.mode,
                &config.reserved_skill_ids,
                source_path,
                source_ref.clone(),
            )
            .await
            {
                Ok(ImportOneOutcome::CreatedAndSwitched) => {
                    summary.rows_created = summary.rows_created.saturating_add(1);
                    summary.switched = summary.switched.saturating_add(1);
                }
                Ok(ImportOneOutcome::CreatedPending) => {
                    summary.rows_created = summary.rows_created.saturating_add(1);
                    summary.failed = summary.failed.saturating_add(1);
                }
                Ok(ImportOneOutcome::Registered) => {
                    summary.rows_created = summary.rows_created.saturating_add(1);
                }
                Ok(ImportOneOutcome::Switched) => {
                    summary.switched = summary.switched.saturating_add(1);
                }
                Ok(ImportOneOutcome::Unchanged) => {
                    summary.unchanged = summary.unchanged.saturating_add(1);
                }
                Ok(ImportOneOutcome::Stale) => {
                    summary.stale = summary.stale.saturating_add(1);
                }
                Err(error) => {
                    summary.failed = summary.failed.saturating_add(1);
                    warn!(
                        source_ref,
                        error = %format!("{error:#}"),
                        "failed to import configured skill package"
                    );
                }
            }
        }
    }

    Ok(summary)
}

async fn managed_source_has_database_container(
    crud_store: &CrudStore,
    root: &ConfiguredSkillImportRoot,
    source_path: &Path,
) -> Result<bool> {
    let Some(container) = source_path.parent() else {
        return Ok(false);
    };
    let managed_root = normalize_import_source_path(root.source_root.as_path())?;
    if container.parent() != Some(managed_root.as_path()) {
        return Ok(false);
    }
    let Some(container_name) = container.file_name().and_then(|name| name.to_str()) else {
        return Ok(false);
    };
    let Ok(skill_id) = SkillId::new(container_name.to_owned()) else {
        return Ok(false);
    };
    Ok(crud_store
        .find_skill_installation(&skill_id)
        .await?
        .is_some_and(|row| {
            row.source_kind == root.source_kind.as_db_value() && row.scope_key == root.scope_key
        }))
}

pub(crate) async fn scan_managed_skill_roots(
    crud_store: &CrudStore,
    skills_write_lock: &Arc<Mutex<()>>,
    config: &ManagedRootScanConfig,
) -> Result<ManagedRootScanSummary> {
    let rows = crud_store.list_skill_installations().await?;
    let mut summary = ManagedRootScanSummary::default();

    for root in &config.roots {
        let managed_root = match normalize_absolute_path(root.managed_root.as_path()) {
            Ok(path) => path,
            Err(error) => {
                summary.failed = summary.failed.saturating_add(1);
                warn!(
                    root = %root.managed_root.display(),
                    error = %format!("{error:#}"),
                    "failed to normalize managed skill root"
                );
                continue;
            }
        };
        for row in rows.iter().filter(|row| {
            row.source_kind == root.source_kind.as_db_value() && row.scope_key == root.scope_key
        }) {
            summary.rows_seen = summary.rows_seen.saturating_add(1);
            match scan_one_managed_installation(
                crud_store,
                skills_write_lock,
                root,
                managed_root.as_path(),
                &config.installer_policy,
                row.clone(),
            )
            .await
            {
                Ok(ManagedScanOneOutcome::Refreshed) => {
                    summary.refreshed = summary.refreshed.saturating_add(1);
                }
                Ok(ManagedScanOneOutcome::Renamed) => {
                    summary.renamed = summary.renamed.saturating_add(1);
                }
                Ok(ManagedScanOneOutcome::Unavailable) => {
                    summary.unavailable = summary.unavailable.saturating_add(1);
                }
                Ok(ManagedScanOneOutcome::Unchanged) => {
                    summary.unchanged = summary.unchanged.saturating_add(1);
                }
                Ok(ManagedScanOneOutcome::Stale) => {
                    summary.stale = summary.stale.saturating_add(1);
                }
                Ok(ManagedScanOneOutcome::NotManaged) => {}
                Err(error) => {
                    summary.failed = summary.failed.saturating_add(1);
                    warn!(
                        skill_id = %row.skill_id,
                        error = %format!("{error:#}"),
                        "failed to refresh managed skill package"
                    );
                }
            }
        }
    }

    Ok(summary)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagedScanOneOutcome {
    Refreshed,
    Renamed,
    Unavailable,
    Unchanged,
    Stale,
    NotManaged,
}

async fn scan_one_managed_installation(
    crud_store: &CrudStore,
    skills_write_lock: &Arc<Mutex<()>>,
    root: &ManagedSkillRoot,
    managed_root: &Path,
    installer_policy: &SkillInstallerPolicy,
    row: SkillInstallationRecord,
) -> Result<ManagedScanOneOutcome> {
    let id_container = normalize_absolute_path(managed_root.join(row.skill_id.as_str()).as_path())?;
    let current_path = normalize_absolute_path(Path::new(row.install_path.as_str()))?;
    let current_is_in_id_container = current_path.parent() == Some(id_container.as_path());

    // Legacy and external import-pending rows are owned by relocation/import. A directory whose
    // name merely resembles their ID is not enough to retarget the persisted installation.
    if !current_is_in_id_container {
        return Ok(ManagedScanOneOutcome::NotManaged);
    }
    if !id_container.is_dir() {
        return Ok(ManagedScanOneOutcome::Unavailable);
    }

    let (source_path, leaf_renamed) =
        if current_path.is_dir() && current_path.join("SKILL.md").is_file() {
            (current_path.clone(), false)
        } else {
            let leaves = discover_managed_package_leaves(id_container.as_path())?;
            if leaves.len() != 1 {
                return Ok(ManagedScanOneOutcome::Unavailable);
            }
            (leaves[0].clone(), true)
        };

    let raw_leaf = source_path
        .file_name()
        .and_then(|leaf| leaf.to_str())
        .context("managed skill leaf must be valid UTF-8")?;
    let slug = normalize_skill_slug(raw_leaf);
    if slug.is_empty() {
        return Ok(ManagedScanOneOutcome::Unavailable);
    }
    let prepared = match prepare_materialized_skill(PrepareMaterializedSkillRequest {
        skill_id: row.skill_id.clone(),
        source_kind: root.source_kind,
        source_ref: row.source_ref.clone(),
        materialized_source_path: source_path.clone(),
        policy: installer_policy.clone(),
    }) {
        Ok(prepared) => prepared,
        Err(error) => {
            warn!(
                skill_id = %row.skill_id,
                path = %source_path.display(),
                error = %format!("{error:#}"),
                "managed skill package is unavailable"
            );
            return Ok(ManagedScanOneOutcome::Unavailable);
        }
    };
    let metadata = PreparedSkillStorageMetadata {
        owner: prepared.definition.identity.owner.clone(),
        // In a DB-backed ID container the leaf is the installed filesystem metadata. A manual
        // rename must not rewrite or derive identity from the package contents.
        slug,
        version: prepared.definition.identity.version_hint.clone(),
        trust_level: trust_level_value(&prepared.definition.runtime.trust_level).to_owned(),
        fingerprint: prepared.definition.identity.fingerprint.clone(),
        source_ref: row.source_ref.clone(),
    };
    let destination = normalize_absolute_path(
        canonical_skill_install_path(managed_root, &row.skill_id, metadata.slug.as_str())?
            .as_path(),
    )?;
    if source_path == destination && row_metadata_matches(&row, &metadata) {
        return Ok(ManagedScanOneOutcome::Unchanged);
    }

    match refresh_managed_installation(
        crud_store,
        skills_write_lock,
        row,
        source_path,
        destination,
        metadata,
        managed_root.join("skills-lock.toml"),
        installer_policy.security.max_install_file_bytes.max(1),
    )
    .await?
    {
        SkillStorageRelocationOutcome::Switched if leaf_renamed => {
            Ok(ManagedScanOneOutcome::Renamed)
        }
        SkillStorageRelocationOutcome::Switched => Ok(ManagedScanOneOutcome::Refreshed),
        SkillStorageRelocationOutcome::Stale => Ok(ManagedScanOneOutcome::Stale),
    }
}

async fn refresh_managed_installation(
    crud_store: &CrudStore,
    skills_write_lock: &Arc<Mutex<()>>,
    expected_row: SkillInstallationRecord,
    source_path: PathBuf,
    destination: PathBuf,
    prepared_metadata: PreparedSkillStorageMetadata,
    managed_lock_path: PathBuf,
    max_skill_file_bytes: usize,
) -> Result<SkillStorageRelocationOutcome> {
    let candidate = SkillStorageRelocationCandidate {
        mode: SkillStorageCandidateMode::ImportOrRefresh,
        expected_row,
        source_path,
        install_root: destination
            .parent()
            .and_then(Path::parent)
            .context("managed skill destination must have an ID-root parent")?
            .to_path_buf(),
        destination,
        prepared_metadata,
        remove_managed_source_after_switch: false,
        managed_path_to_remove_after_switch: None,
        managed_lock_path: Some(managed_lock_path),
        max_skill_file_bytes,
    };
    let _guard = skills_write_lock.lock().await;
    let current = crud_store
        .find_skill_installation(&candidate.expected_row.skill_id)
        .await
        .with_context(|| {
            format!(
                "failed to reread managed skill `{}` before refresh",
                candidate.expected_row.skill_id
            )
        })?;
    if current.as_ref() != Some(&candidate.expected_row) {
        return Ok(SkillStorageRelocationOutcome::Stale);
    }
    validate_candidate_paths(&candidate, true)?;
    if verify_source_revision(&candidate).is_err() {
        return Ok(SkillStorageRelocationOutcome::Stale);
    }

    let renamed = candidate.source_path != candidate.destination;
    if renamed {
        remove_any(candidate.destination.as_path()).with_context(|| {
            format!(
                "failed to replace normalized managed skill destination `{}`",
                candidate.destination.display()
            )
        })?;
        fs::rename(
            candidate.source_path.as_path(),
            candidate.destination.as_path(),
        )
        .with_context(|| {
            format!(
                "failed to normalize managed skill leaf `{}` to `{}`",
                candidate.source_path.display(),
                candidate.destination.display()
            )
        })?;
    }

    let now = crate::message::now_timestamp_secs();
    let previous_lock_entry = match write_candidate_lock_entry(&candidate, now) {
        Ok(previous) => previous,
        Err(error) => {
            if renamed {
                rollback_managed_leaf_rename(&candidate);
            }
            return Err(error);
        }
    };
    let patch = candidate_database_patch(&candidate);
    match crud_store
        .update_skill_installation(&candidate.expected_row.skill_id, &patch, now)
        .await
    {
        Ok(true) => Ok(SkillStorageRelocationOutcome::Switched),
        Ok(false) => {
            restore_candidate_lock_entry(&candidate, previous_lock_entry.as_ref());
            if renamed {
                rollback_managed_leaf_rename(&candidate);
            }
            Ok(SkillStorageRelocationOutcome::Stale)
        }
        Err(error) => {
            restore_candidate_lock_entry(&candidate, previous_lock_entry.as_ref());
            if renamed {
                rollback_managed_leaf_rename(&candidate);
            }
            Err(error).with_context(|| {
                format!(
                    "failed to persist managed skill refresh `{}`",
                    candidate.expected_row.skill_id
                )
            })
        }
    }
}

fn rollback_managed_leaf_rename(candidate: &SkillStorageRelocationCandidate) {
    if let Err(error) = fs::rename(
        candidate.destination.as_path(),
        candidate.source_path.as_path(),
    ) {
        warn!(
            skill_id = %candidate.expected_row.skill_id,
            source_path = %candidate.source_path.display(),
            destination = %candidate.destination.display(),
            error = %error,
            "failed to roll back managed skill leaf rename"
        );
    }
}

fn discover_managed_package_leaves(id_container: &Path) -> Result<Vec<PathBuf>> {
    let mut leaves = Vec::new();
    for entry in fs::read_dir(id_container).with_context(|| {
        format!(
            "failed to read managed SkillId container `{}`",
            id_container.display()
        )
    })? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            continue;
        }
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with('.'))
        {
            continue;
        }
        let path = entry.path();
        if path.join("SKILL.md").is_file() {
            leaves.push(normalize_absolute_path(path.as_path())?);
        }
    }
    leaves.sort();
    Ok(leaves)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImportOneOutcome {
    CreatedAndSwitched,
    CreatedPending,
    Registered,
    Switched,
    Unchanged,
    Stale,
}

async fn import_one_configured_package(
    crud_store: &CrudStore,
    skills_write_lock: &Arc<Mutex<()>>,
    root: &ConfiguredSkillImportRoot,
    installer_policy: &SkillInstallerPolicy,
    force_refresh: bool,
    mode: ConfiguredRootImportMode,
    reserved_skill_ids: &HashSet<SkillId>,
    source_path: PathBuf,
    source_ref: String,
) -> Result<ImportOneOutcome> {
    let (row, created) = {
        let _guard = skills_write_lock.lock().await;
        let existing = find_row_for_import_source(
            crud_store,
            root.source_kind.as_db_value(),
            root.scope_key.as_str(),
            source_path.as_path(),
            source_ref.as_str(),
        )
        .await?;
        match existing {
            Some(row) => (row, false),
            None => {
                let skill_id = allocate_import_skill_id(crud_store, reserved_skill_ids).await?;
                let fallback_slug = source_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(normalize_skill_slug)
                    .filter(|slug| !slug.is_empty())
                    .unwrap_or_else(|| "unnamed-skill".to_owned());
                let now = crate::message::now_timestamp_secs();
                let pending = SkillInstallationRecord {
                    skill_id,
                    owner: None,
                    slug: fallback_slug,
                    version: None,
                    source_kind: root.source_kind.as_db_value().to_owned(),
                    scope_key: root.scope_key.clone(),
                    source_ref: source_ref.clone(),
                    install_path: source_path.display().to_string(),
                    trust_level: "community".to_owned(),
                    fingerprint: String::new(),
                    updated_at_unix: now,
                };
                crud_store
                    .insert_skill_installation(&pending, now)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to persist pending configured skill `{}`",
                            pending.skill_id
                        )
                    })?;
                (pending, true)
            }
        }
    };
    if !created && root.source_is_pioneer_managed && row.source_ref != source_ref {
        // This is an existing working legacy/canonical row discovered through the managed root.
        // Existing-row relocation owns it and must preserve its current provenance metadata.
        return Ok(ImportOneOutcome::Unchanged);
    }
    if mode == ConfiguredRootImportMode::RegisterOnly {
        return Ok(if created {
            ImportOneOutcome::Registered
        } else {
            ImportOneOutcome::Unchanged
        });
    }

    let prepared = match prepare_materialized_skill(PrepareMaterializedSkillRequest {
        skill_id: row.skill_id.clone(),
        source_kind: root.source_kind.clone(),
        source_ref: source_ref.clone(),
        materialized_source_path: source_path.clone(),
        policy: installer_policy.clone(),
    }) {
        Ok(prepared) => prepared,
        Err(error) if created => {
            warn!(
                skill_id = %row.skill_id,
                source_ref,
                error = %format!("{error:#}"),
                "configured skill remains import-pending after validation failure"
            );
            return Ok(ImportOneOutcome::CreatedPending);
        }
        Err(error) => return Err(error).context("configured skill validation failed"),
    };

    let metadata = PreparedSkillStorageMetadata {
        owner: prepared.definition.identity.owner.clone(),
        slug: prepared.definition.identity.slug.clone(),
        version: prepared.definition.identity.version_hint.clone(),
        trust_level: trust_level_value(&prepared.definition.runtime.trust_level).to_owned(),
        fingerprint: prepared.definition.identity.fingerprint.clone(),
        source_ref: source_ref.clone(),
    };
    let destination = canonical_skill_install_path(
        root.managed_root.as_path(),
        &row.skill_id,
        metadata.slug.as_str(),
    )?;
    let destination = normalize_absolute_path(destination.as_path())?;
    let current_path = normalize_absolute_path(Path::new(row.install_path.as_str()))?;

    if !force_refresh
        && current_path == destination
        && destination.is_dir()
        && row_metadata_matches(&row, &metadata)
    {
        return Ok(ImportOneOutcome::Unchanged);
    }

    // A canonical managed package is handled by the managed-root scanner in P04-WP04.
    // Do not turn its current path into external import provenance.
    if current_path == source_path && current_path == destination {
        return Ok(ImportOneOutcome::Unchanged);
    }

    let previous_managed_path = (current_path != source_path
        && current_path != destination
        && path_is_existing_descendant(root.managed_root.as_path(), current_path.as_path()))
    .then_some(current_path);
    let skill_id = row.skill_id.clone();
    let candidate = SkillStorageRelocationCandidate {
        mode: SkillStorageCandidateMode::ImportOrRefresh,
        expected_row: row,
        source_path,
        install_root: root.managed_root.clone(),
        destination,
        prepared_metadata: metadata,
        remove_managed_source_after_switch: root.source_is_pioneer_managed,
        managed_path_to_remove_after_switch: previous_managed_path,
        managed_lock_path: Some(root.managed_root.join("skills-lock.toml")),
        max_skill_file_bytes: installer_policy.security.max_install_file_bytes.max(1),
    };
    let switch = match copy_and_switch_candidate(crud_store, skills_write_lock, candidate).await {
        Ok(outcome) => outcome,
        Err(error) if created => {
            warn!(
                skill_id = %skill_id,
                source_ref,
                error = %format!("{error:#}"),
                "configured skill remains import-pending after copy or switch failure"
            );
            return Ok(ImportOneOutcome::CreatedPending);
        }
        Err(error) => return Err(error),
    };
    match switch {
        SkillStorageRelocationOutcome::Switched if created => {
            Ok(ImportOneOutcome::CreatedAndSwitched)
        }
        SkillStorageRelocationOutcome::Switched => Ok(ImportOneOutcome::Switched),
        SkillStorageRelocationOutcome::Stale => Ok(ImportOneOutcome::Stale),
    }
}

fn row_metadata_matches(
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

async fn find_row_for_import_source(
    crud_store: &CrudStore,
    source_kind: &str,
    scope_key: &str,
    source_path: &Path,
    source_ref: &str,
) -> Result<Option<SkillInstallationRecord>> {
    let rows = crud_store.list_skill_installations().await?;
    let mut provenance_matches = rows
        .iter()
        .filter(|row| {
            row.source_kind == source_kind
                && row.scope_key == scope_key
                && row.source_ref == source_ref
        })
        .cloned()
        .collect::<Vec<_>>();
    if provenance_matches.len() > 1 {
        bail!("multiple skill rows share exact import provenance `{source_ref}`");
    }
    if let Some(row) = provenance_matches.pop() {
        return Ok(Some(row));
    }

    let mut path_matches = Vec::new();
    for row in rows {
        if row.source_kind != source_kind || row.scope_key != scope_key {
            continue;
        }
        let Ok(install_path) = normalize_absolute_path(Path::new(row.install_path.as_str())) else {
            continue;
        };
        if install_path == source_path {
            path_matches.push(row);
        }
    }
    if path_matches.len() > 1 {
        bail!(
            "multiple skill rows share exact configured source path `{}`",
            source_path.display()
        );
    }
    Ok(path_matches.pop())
}

async fn allocate_import_skill_id(
    crud_store: &CrudStore,
    reserved_skill_ids: &HashSet<SkillId>,
) -> Result<SkillId> {
    loop {
        let skill_id = SkillId::new(pioneer_protocol::generate_id(
            pioneer_protocol::SKILL_ID_LEN,
        ))
        .map_err(|error| anyhow::anyhow!("generated an invalid skill identity: {error}"))?;
        if !reserved_skill_ids.contains(&skill_id)
            && crud_store
                .find_skill_installation(&skill_id)
                .await?
                .is_none()
        {
            return Ok(skill_id);
        }
    }
}

fn discover_configured_packages(root: &Path, limit: usize) -> Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    if !root.is_dir() {
        bail!(
            "configured skill root `{}` is not a directory",
            root.display()
        );
    }
    let mut packages = Vec::new();
    let mut first_level = fs::read_dir(root)?
        .filter_map(std::result::Result::ok)
        .collect::<Vec<_>>();
    first_level.sort_by_key(|entry| entry.file_name());
    for entry in first_level {
        let metadata = fs::symlink_metadata(entry.path())?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            continue;
        }
        let direct = entry.path();
        if direct.join("SKILL.md").is_file() {
            packages.push(direct);
        } else {
            let mut second_level = fs::read_dir(direct.as_path())?
                .filter_map(std::result::Result::ok)
                .collect::<Vec<_>>();
            second_level.sort_by_key(|candidate| candidate.file_name());
            for candidate in second_level {
                let metadata = fs::symlink_metadata(candidate.path())?;
                if metadata.is_dir()
                    && !metadata.file_type().is_symlink()
                    && candidate.path().join("SKILL.md").is_file()
                {
                    packages.push(candidate.path());
                }
            }
        }
        if packages.len() >= limit {
            packages.truncate(limit);
            break;
        }
    }
    packages.sort();
    packages.dedup();
    Ok(packages)
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

fn trust_level_value(level: &SkillTrustLevel) -> &'static str {
    match level {
        SkillTrustLevel::Internal => "internal",
        SkillTrustLevel::Verified => "verified",
        SkillTrustLevel::Community => "community",
        SkillTrustLevel::Untrusted => "untrusted",
    }
}

fn install_root_template_for_row<'a>(
    row: &SkillInstallationRecord,
    config: &'a ExistingInstallationRelocationConfig,
) -> Option<&'a str> {
    match row.source_kind.as_str() {
        "user" => config.user_root_templates.first().map(String::as_str),
        "registry" => config.registry_root_templates.first().map(String::as_str),
        _ => None,
    }
}

pub(crate) async fn copy_and_switch_candidate(
    crud_store: &CrudStore,
    skills_write_lock: &Arc<Mutex<()>>,
    candidate: SkillStorageRelocationCandidate,
) -> Result<SkillStorageRelocationOutcome> {
    let prepared = prepare_candidate_copy(candidate)?;
    publish_prepared_copy(crud_store, skills_write_lock, prepared).await
}

fn prepare_candidate_copy(
    candidate: SkillStorageRelocationCandidate,
) -> Result<PreparedSkillStorageCopy> {
    validate_candidate_paths(&candidate, false)?;
    verify_source_revision(&candidate)?;

    let attempt_path = relocation_attempt_path(candidate.destination.as_path())?;
    remove_any(attempt_path.as_path()).with_context(|| {
        format!(
            "failed to remove stale relocation attempt `{}`",
            attempt_path.display()
        )
    })?;
    if let Err(error) = fs::create_dir_all(attempt_path.as_path()).with_context(|| {
        format!(
            "failed to create relocation attempt `{}`",
            attempt_path.display()
        )
    }) {
        let _ = remove_any(attempt_path.as_path());
        return Err(error);
    }
    if let Err(error) = copy_package_tree(candidate.source_path.as_path(), attempt_path.as_path()) {
        let _ = remove_any(attempt_path.as_path());
        return Err(error).with_context(|| {
            format!(
                "failed to copy skill `{}` into relocation attempt",
                candidate.expected_row.skill_id
            )
        });
    }
    if let Err(error) = verify_copied_revision(&candidate, attempt_path.as_path()) {
        let _ = remove_any(attempt_path.as_path());
        return Err(error);
    }

    Ok(PreparedSkillStorageCopy {
        candidate,
        attempt_path,
    })
}

async fn publish_prepared_copy(
    crud_store: &CrudStore,
    skills_write_lock: &Arc<Mutex<()>>,
    prepared: PreparedSkillStorageCopy,
) -> Result<SkillStorageRelocationOutcome> {
    let _guard = skills_write_lock.lock().await;
    let candidate = &prepared.candidate;

    let current = match crud_store
        .find_skill_installation(&candidate.expected_row.skill_id)
        .await
        .with_context(|| {
            format!(
                "failed to reread skill installation `{}` before relocation switch",
                candidate.expected_row.skill_id
            )
        }) {
        Ok(current) => current,
        Err(error) => {
            let _ = remove_any(prepared.attempt_path.as_path());
            return Err(error);
        }
    };
    if current.as_ref() != Some(&candidate.expected_row) {
        let _ = remove_any(prepared.attempt_path.as_path());
        return Ok(SkillStorageRelocationOutcome::Stale);
    }
    if verify_source_revision(candidate).is_err() {
        let _ = remove_any(prepared.attempt_path.as_path());
        return Ok(SkillStorageRelocationOutcome::Stale);
    }

    if let Err(error) = publish_candidate_files(candidate, prepared.attempt_path.as_path()) {
        let _ = remove_any(prepared.attempt_path.as_path());
        return Err(error);
    }

    let now = crate::message::now_timestamp_secs();
    let previous_lock_entry = match write_candidate_lock_entry(candidate, now) {
        Ok(previous) => previous,
        Err(error) => return Err(error),
    };
    let patch = candidate_database_patch(candidate);
    let switched = crud_store
        .update_skill_installation(&candidate.expected_row.skill_id, &patch, now)
        .await;
    match switched {
        Ok(true) => {}
        Ok(false) => {
            restore_candidate_lock_entry(candidate, previous_lock_entry.as_ref());
            return Ok(SkillStorageRelocationOutcome::Stale);
        }
        Err(error) => {
            restore_candidate_lock_entry(candidate, previous_lock_entry.as_ref());
            return Err(error).with_context(|| {
                format!(
                    "failed to switch skill installation `{}` to canonical path",
                    candidate.expected_row.skill_id
                )
            });
        }
    }
    drop(_guard);

    if candidate.remove_managed_source_after_switch
        && candidate.source_path != candidate.destination
    {
        cleanup_old_managed_source(
            candidate.install_root.as_path(),
            candidate.source_path.as_path(),
        );
    }
    if let Some(previous_path) = candidate.managed_path_to_remove_after_switch.as_deref()
        && previous_path != candidate.destination
        && path_is_existing_descendant(candidate.install_root.as_path(), previous_path)
    {
        cleanup_old_managed_source(candidate.install_root.as_path(), previous_path);
    }

    Ok(SkillStorageRelocationOutcome::Switched)
}

fn publish_candidate_files(
    candidate: &SkillStorageRelocationCandidate,
    attempt_path: &Path,
) -> Result<()> {
    remove_any(candidate.destination.as_path()).with_context(|| {
        format!(
            "failed to remove existing managed package `{}`",
            candidate.destination.display()
        )
    })?;
    fs::rename(attempt_path, candidate.destination.as_path()).with_context(|| {
        format!(
            "failed to publish managed package destination `{}`",
            candidate.destination.display()
        )
    })
}

fn candidate_database_patch(candidate: &SkillStorageRelocationCandidate) -> SkillInstallationPatch {
    let install_path = Some(candidate.destination.display().to_string());
    match candidate.mode {
        SkillStorageCandidateMode::ExistingRelocation => SkillInstallationPatch {
            install_path,
            ..Default::default()
        },
        SkillStorageCandidateMode::ImportOrRefresh => SkillInstallationPatch {
            owner: Some(candidate.prepared_metadata.owner.clone()),
            slug: Some(candidate.prepared_metadata.slug.clone()),
            version: Some(candidate.prepared_metadata.version.clone()),
            source_ref: Some(candidate.prepared_metadata.source_ref.clone()),
            install_path,
            trust_level: Some(candidate.prepared_metadata.trust_level.clone()),
            fingerprint: Some(candidate.prepared_metadata.fingerprint.clone()),
            ..Default::default()
        },
    }
}

fn write_candidate_lock_entry(
    candidate: &SkillStorageRelocationCandidate,
    now: i64,
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
    write_skills_lock_atomic(lock_path, &lock)?;
    Ok(previous)
}

fn restore_candidate_lock_entry(
    candidate: &SkillStorageRelocationCandidate,
    previous: Option<&SkillLockEntry>,
) {
    let Some(lock_path) = candidate.managed_lock_path.as_deref() else {
        return;
    };
    let result = (|| -> Result<()> {
        let mut lock = read_skills_lock(lock_path)?;
        if let Some(previous) = previous {
            upsert_lock_entry(&mut lock, previous.clone());
        } else {
            remove_lock_entry(&mut lock, &candidate.expected_row.skill_id);
        }
        write_skills_lock_atomic(lock_path, &lock)
    })();
    if let Err(error) = result {
        warn!(
            skill_id = %candidate.expected_row.skill_id,
            path = %lock_path.display(),
            error = %format!("{error:#}"),
            "failed to restore managed skills lock after relocation database failure"
        );
    }
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

fn verify_source_revision(candidate: &SkillStorageRelocationCandidate) -> Result<()> {
    verify_revision_at_path(
        &candidate.expected_row.skill_id,
        candidate.expected_row.source_kind.as_str(),
        candidate.source_path.as_path(),
        candidate.prepared_metadata.fingerprint.as_str(),
        candidate.max_skill_file_bytes,
    )
}

fn verify_copied_revision(
    candidate: &SkillStorageRelocationCandidate,
    attempt_path: &Path,
) -> Result<()> {
    verify_revision_at_path(
        &candidate.expected_row.skill_id,
        candidate.expected_row.source_kind.as_str(),
        attempt_path,
        candidate.prepared_metadata.fingerprint.as_str(),
        candidate.max_skill_file_bytes,
    )
}

fn verify_revision_at_path(
    skill_id: &SkillId,
    source_kind: &str,
    package_path: &Path,
    expected_fingerprint: &str,
    max_skill_file_bytes: usize,
) -> Result<()> {
    let source_kind = parse_source_kind(source_kind)?;
    let source_root = package_path.parent().unwrap_or(package_path);
    let definition = parse_skill_from_file(
        skill_id.clone(),
        package_path.join("SKILL.md").as_path(),
        source_kind,
        source_root,
        max_skill_file_bytes.max(1),
    )?;
    if definition.identity.fingerprint != expected_fingerprint {
        bail!(
            "skill source revision changed: expected `{expected_fingerprint}`, found `{}`",
            definition.identity.fingerprint
        );
    }
    Ok(())
}

fn parse_source_kind(value: &str) -> Result<SkillSourceKind> {
    match value {
        "system" => Ok(SkillSourceKind::System),
        "user" => Ok(SkillSourceKind::User),
        "registry" => Ok(SkillSourceKind::Registry),
        other => bail!("unsupported skill source kind `{other}`"),
    }
}

fn relocation_attempt_path(destination: &Path) -> Result<PathBuf> {
    let parent = destination
        .parent()
        .context("canonical skill destination must have a parent")?;
    let slug = destination
        .file_name()
        .and_then(|name| name.to_str())
        .context("canonical skill destination must have a UTF-8 leaf")?;
    Ok(parent.join(format!(".{slug}{RELOCATION_ATTEMPT_SUFFIX}")))
}

fn copy_package_tree(source: &Path, destination: &Path) -> Result<()> {
    for entry in fs::read_dir(source)
        .with_context(|| format!("failed to read package directory `{}`", source.display()))?
    {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(source_path.as_path())
            .with_context(|| format!("failed to read package entry `{}`", source_path.display()))?;
        if metadata.file_type().is_symlink() {
            bail!(
                "skill relocation does not follow symlink `{}`",
                source_path.display()
            );
        }
        if metadata.is_dir() {
            fs::create_dir(destination_path.as_path()).with_context(|| {
                format!(
                    "failed to create package directory `{}`",
                    destination_path.display()
                )
            })?;
            copy_package_tree(source_path.as_path(), destination_path.as_path())?;
        } else if metadata.is_file() {
            fs::copy(source_path.as_path(), destination_path.as_path()).with_context(|| {
                format!(
                    "failed to copy package file `{}` to `{}`",
                    source_path.display(),
                    destination_path.display()
                )
            })?;
        } else {
            bail!(
                "unsupported file type in skill package `{}`",
                source_path.display()
            );
        }
    }
    Ok(())
}

fn remove_any(path: &Path) -> std::io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            fs::remove_dir_all(path)
        }
        Ok(_) => fs::remove_file(path),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn cleanup_old_managed_source(install_root: &Path, source_path: &Path) {
    if let Err(error) = remove_any(source_path) {
        warn!(
            source_path = %source_path.display(),
            error = %error,
            "failed to remove old managed skill package after relocation"
        );
        return;
    }

    let Some(parent) = source_path.parent() else {
        return;
    };
    if normalize_absolute_path(parent).ok().as_ref()
        == normalize_absolute_path(install_root).ok().as_ref()
    {
        return;
    }
    match fs::remove_dir(parent) {
        Ok(()) => {}
        Err(error)
            if error.kind() == ErrorKind::NotFound
                || error.kind() == ErrorKind::DirectoryNotEmpty => {}
        Err(error) => warn!(
            path = %parent.display(),
            error = %error,
            "failed to remove empty legacy skill owner directory"
        ),
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
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use sea_orm::{ConnectionTrait, Database};
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn test_store() -> CrudStore {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("connect temporary database");
        Migrator::up(&connection, None)
            .await
            .expect("apply migrations");
        CrudStore::new(connection)
    }

    fn test_id(character: char) -> SkillId {
        SkillId::new(character.to_string().repeat(21)).expect("valid test SkillId")
    }

    fn write_package(path: &Path, body: &str) -> String {
        fs::create_dir_all(path.join("assets")).expect("create package directories");
        fs::write(
            path.join("SKILL.md"),
            format!("---\nname: Test\nslug: test-skill\n---\n{body}"),
        )
        .expect("write SKILL.md");
        fs::write(path.join("assets/value.txt"), body).expect("write package asset");
        parse_skill_from_file(
            test_id('Z'),
            path.join("SKILL.md").as_path(),
            SkillSourceKind::User,
            path.parent().expect("package parent"),
            64 * 1024,
        )
        .expect("parse package")
        .identity
        .fingerprint
    }

    fn row(
        id: char,
        source_kind: &str,
        scope: &str,
        source: &Path,
        fingerprint: &str,
    ) -> SkillInstallationRecord {
        SkillInstallationRecord {
            skill_id: test_id(id),
            owner: Some("owner".to_owned()),
            slug: "test-skill".to_owned(),
            version: Some("1.0.0".to_owned()),
            source_kind: source_kind.to_owned(),
            scope_key: scope.to_owned(),
            source_ref: "legacy:fixture".to_owned(),
            install_path: source.display().to_string(),
            trust_level: "community".to_owned(),
            fingerprint: fingerprint.to_owned(),
            updated_at_unix: 1_700_000_000,
        }
    }

    fn config(root: &Path) -> ExistingInstallationRelocationConfig {
        ExistingInstallationRelocationConfig {
            user_root_templates: vec![root.join("{workspaceId}/user").display().to_string()],
            registry_root_templates: vec![
                root.join("{workspaceId}/registry").display().to_string(),
            ],
            max_skill_file_bytes: 64 * 1024,
        }
    }

    fn candidate_for(
        row: SkillInstallationRecord,
        install_root: &Path,
    ) -> SkillStorageRelocationCandidate {
        let destination =
            canonical_skill_install_path(install_root, &row.skill_id, row.slug.as_str())
                .expect("canonical destination");
        SkillStorageRelocationCandidate {
            mode: SkillStorageCandidateMode::ExistingRelocation,
            expected_row: row.clone(),
            source_path: PathBuf::from(row.install_path.as_str()),
            install_root: install_root.to_path_buf(),
            destination,
            prepared_metadata: PreparedSkillStorageMetadata::from_row(&row),
            remove_managed_source_after_switch: true,
            managed_path_to_remove_after_switch: None,
            managed_lock_path: None,
            max_skill_file_bytes: 64 * 1024,
        }
    }

    fn write_import_package(path: &Path, body: &str) {
        fs::create_dir_all(path.join("assets")).expect("create import package");
        fs::write(
            path.join("SKILL.md"),
            format!("---\nname: Imported\n---\n{body}"),
        )
        .expect("write imported SKILL.md");
        fs::write(path.join("assets/value.txt"), body).expect("write imported asset");
    }

    fn import_root(
        source_kind: SkillSourceKind,
        scope_key: &str,
        source_root: PathBuf,
        managed_root: PathBuf,
    ) -> ConfiguredSkillImportRoot {
        ConfiguredSkillImportRoot {
            source_kind,
            scope_key: scope_key.to_owned(),
            source_root,
            managed_root,
            source_is_pioneer_managed: false,
        }
    }

    fn import_config(roots: Vec<ConfiguredSkillImportRoot>) -> ConfiguredRootImportConfig {
        ConfiguredRootImportConfig {
            roots,
            installer_policy: SkillInstallerPolicy::default(),
            max_skills_per_root: 64,
            force_refresh: false,
            mode: ConfiguredRootImportMode::ImportAndRefresh,
            reserved_skill_ids: HashSet::new(),
        }
    }

    fn managed_config(
        source_kind: SkillSourceKind,
        scope_key: &str,
        managed_root: PathBuf,
    ) -> ManagedRootScanConfig {
        ManagedRootScanConfig {
            roots: vec![ManagedSkillRoot {
                source_kind,
                scope_key: scope_key.to_owned(),
                managed_root,
            }],
            installer_policy: SkillInstallerPolicy::default(),
        }
    }

    fn managed_row(
        id: char,
        source_kind: &str,
        scope: &str,
        managed_root: &Path,
        slug: &str,
        body: &str,
    ) -> SkillInstallationRecord {
        let skill_id = test_id(id);
        let source = managed_root.join(skill_id.as_str()).join(slug);
        let fingerprint = write_package(source.as_path(), body);
        SkillInstallationRecord {
            skill_id,
            owner: None,
            slug: slug.to_owned(),
            version: None,
            source_kind: source_kind.to_owned(),
            scope_key: scope.to_owned(),
            source_ref: "manual:managed-fixture".to_owned(),
            install_path: source.display().to_string(),
            trust_level: "community".to_owned(),
            fingerprint,
            updated_at_unix: 1_700_000_000,
        }
    }

    #[tokio::test]
    async fn relocates_user_and_registry_rows_and_cleans_empty_legacy_owner() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        for (id, kind) in [('A', "user"), ('B', "registry")] {
            let install_root = temp.path().join("workspace-one").join(kind);
            let source = install_root.join("owner/test-skill");
            let fingerprint = write_package(source.as_path(), kind);
            let row = row(
                id,
                kind,
                "workspace-one",
                source.as_path(),
                fingerprint.as_str(),
            );
            store
                .insert_skill_installation(&row, row.updated_at_unix)
                .await
                .expect("insert row");
        }

        let summary = relocate_existing_installations(
            &store,
            &Arc::new(Mutex::new(())),
            &config(temp.path()),
        )
        .await
        .expect("relocation pass");
        assert_eq!(summary.switched, 2);
        for (id, kind) in [('A', "user"), ('B', "registry")] {
            let current = store
                .find_skill_installation(&test_id(id))
                .await
                .expect("find row")
                .expect("row");
            let expected = temp
                .path()
                .join("workspace-one")
                .join(kind)
                .join(id.to_string().repeat(21))
                .join("test-skill");
            assert_eq!(PathBuf::from(current.install_path), expected);
            assert!(expected.join("assets/value.txt").is_file());
            assert!(
                !temp
                    .path()
                    .join("workspace-one")
                    .join(kind)
                    .join("owner")
                    .exists()
            );
        }
    }

    #[tokio::test]
    async fn overwrites_existing_destination_from_scratch() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let root = temp.path().join("workspace-one/user");
        let source = root.join("owner/test-skill");
        let fingerprint = write_package(source.as_path(), "fresh");
        let row = row(
            'C',
            "user",
            "workspace-one",
            source.as_path(),
            fingerprint.as_str(),
        );
        let destination = canonical_skill_install_path(root.as_path(), &row.skill_id, "test-skill")
            .expect("destination");
        fs::create_dir_all(destination.as_path()).expect("create stale destination");
        fs::write(destination.join("stale.txt"), "stale").expect("write stale file");
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert row");

        let summary = relocate_existing_installations(
            &store,
            &Arc::new(Mutex::new(())),
            &config(temp.path()),
        )
        .await
        .expect("relocate");
        assert_eq!(summary.switched, 1);
        assert!(!destination.join("stale.txt").exists());
        assert_eq!(
            fs::read_to_string(destination.join("assets/value.txt")).expect("read asset"),
            "fresh"
        );
    }

    #[tokio::test]
    async fn copy_failure_keeps_old_row_and_retry_uses_persisted_state() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let source = temp.path().join("workspace-one/user/owner/test-skill");
        let row = row(
            'D',
            "user",
            "workspace-one",
            source.as_path(),
            "missing-fingerprint",
        );
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert row");
        let first = relocate_existing_installations(
            &store,
            &Arc::new(Mutex::new(())),
            &config(temp.path()),
        )
        .await
        .expect("first pass");
        assert_eq!(first.failed, 1);
        assert_eq!(
            store
                .find_skill_installation(&row.skill_id)
                .await
                .expect("find row")
                .expect("row")
                .install_path,
            row.install_path
        );

        let fingerprint = write_package(source.as_path(), "restored");
        store
            .update_skill_installation(
                &row.skill_id,
                &SkillInstallationPatch {
                    fingerprint: Some(fingerprint),
                    ..Default::default()
                },
                1_700_000_001,
            )
            .await
            .expect("refresh fingerprint");
        let second = relocate_existing_installations(
            &store,
            &Arc::new(Mutex::new(())),
            &config(temp.path()),
        )
        .await
        .expect("retry pass");
        assert_eq!(second.switched, 1);
    }

    #[tokio::test]
    async fn canonical_row_is_skipped() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let root = temp.path().join("workspace-one/user");
        let id = test_id('E');
        let source = root.join(id.as_str()).join("test-skill");
        let fingerprint = write_package(source.as_path(), "canonical");
        let row = row(
            'E',
            "user",
            "workspace-one",
            source.as_path(),
            fingerprint.as_str(),
        );
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert row");
        let summary = relocate_existing_installations(
            &store,
            &Arc::new(Mutex::new(())),
            &config(temp.path()),
        )
        .await
        .expect("pass");
        assert_eq!(summary.skipped_canonical, 1);
        assert_eq!(summary.switched, 0);
    }

    #[tokio::test]
    async fn update_race_discards_prepared_copy() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let root = temp.path().join("workspace-one/user");
        let source = root.join("owner/test-skill");
        let fingerprint = write_package(source.as_path(), "old");
        let row = row(
            'F',
            "user",
            "workspace-one",
            source.as_path(),
            fingerprint.as_str(),
        );
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert row");
        let candidate = candidate_for(row.clone(), root.as_path());
        let prepared = prepare_candidate_copy(candidate).expect("prepare copy");
        store
            .update_skill_installation(
                &row.skill_id,
                &SkillInstallationPatch {
                    fingerprint: Some("new-revision".to_owned()),
                    ..Default::default()
                },
                1_700_000_001,
            )
            .await
            .expect("concurrent update");

        let outcome = publish_prepared_copy(&store, &Arc::new(Mutex::new(())), prepared)
            .await
            .expect("publish decision");
        assert_eq!(outcome, SkillStorageRelocationOutcome::Stale);
        assert!(!root.join(row.skill_id.as_str()).join("test-skill").exists());
        assert!(source.exists());
    }

    #[tokio::test]
    async fn source_revision_change_during_copy_discards_prepared_copy() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let root = temp.path().join("workspace-one/user");
        let source = root.join("owner/test-skill");
        let fingerprint = write_package(source.as_path(), "old");
        let row = row(
            'K',
            "user",
            "workspace-one",
            source.as_path(),
            fingerprint.as_str(),
        );
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert row");
        let prepared = prepare_candidate_copy(candidate_for(row.clone(), root.as_path()))
            .expect("prepare copy");
        write_package(source.as_path(), "changed while copying");

        let outcome = publish_prepared_copy(&store, &Arc::new(Mutex::new(())), prepared)
            .await
            .expect("publish decision");
        assert_eq!(outcome, SkillStorageRelocationOutcome::Stale);
        assert_eq!(
            store
                .find_skill_installation(&row.skill_id)
                .await
                .expect("find row")
                .expect("row")
                .install_path,
            row.install_path
        );
        assert!(!root.join(row.skill_id.as_str()).join("test-skill").exists());
        assert!(source.exists());
    }

    #[tokio::test]
    async fn uninstall_race_does_not_recreate_row() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let root = temp.path().join("workspace-one/user");
        let source = root.join("owner/test-skill");
        let fingerprint = write_package(source.as_path(), "old");
        let row = row(
            'G',
            "user",
            "workspace-one",
            source.as_path(),
            fingerprint.as_str(),
        );
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert row");
        let prepared = prepare_candidate_copy(candidate_for(row.clone(), root.as_path()))
            .expect("prepare copy");
        store
            .delete_skill_installation(&row.skill_id)
            .await
            .expect("concurrent uninstall");

        let outcome = publish_prepared_copy(&store, &Arc::new(Mutex::new(())), prepared)
            .await
            .expect("publish decision");
        assert_eq!(outcome, SkillStorageRelocationOutcome::Stale);
        assert!(
            store
                .find_skill_installation(&row.skill_id)
                .await
                .expect("find row")
                .is_none()
        );
        assert!(!root.join(row.skill_id.as_str()).join("test-skill").exists());
    }

    #[tokio::test]
    async fn publish_failure_keeps_old_path_and_source() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let root = temp.path().join("workspace-one/user");
        let source = root.join("owner/test-skill");
        let fingerprint = write_package(source.as_path(), "old");
        let row = row(
            'H',
            "user",
            "workspace-one",
            source.as_path(),
            fingerprint.as_str(),
        );
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert row");
        let prepared = prepare_candidate_copy(candidate_for(row.clone(), root.as_path()))
            .expect("prepare copy");
        remove_any(prepared.attempt_path.as_path()).expect("remove attempt before publish");

        assert!(
            publish_prepared_copy(&store, &Arc::new(Mutex::new(())), prepared)
                .await
                .is_err()
        );
        assert_eq!(
            store
                .find_skill_installation(&row.skill_id)
                .await
                .expect("find row")
                .expect("row")
                .install_path,
            row.install_path
        );
        assert!(source.exists());
    }

    #[tokio::test]
    async fn database_switch_failure_keeps_old_path_and_source() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let root = temp.path().join("workspace-one/user");
        let source = root.join("owner/test-skill");
        let fingerprint = write_package(source.as_path(), "old");
        let row = row(
            'Q',
            "user",
            "workspace-one",
            source.as_path(),
            fingerprint.as_str(),
        );
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert row");
        let prepared = prepare_candidate_copy(candidate_for(row.clone(), root.as_path()))
            .expect("prepare copy");
        store
            .database_connection()
            .execute_unprepared(
                r#"
                CREATE TRIGGER reject_skill_install_path_switch
                BEFORE UPDATE OF install_path ON skill_installation
                BEGIN
                    SELECT RAISE(FAIL, 'injected install path switch failure');
                END
                "#,
            )
            .await
            .expect("install failure trigger");

        let error = publish_prepared_copy(&store, &Arc::new(Mutex::new(())), prepared)
            .await
            .expect_err("database switch should fail");
        assert!(
            format!("{error:#}").contains("failed to switch skill installation"),
            "unexpected error: {error:#}"
        );
        store
            .database_connection()
            .execute_unprepared("DROP TRIGGER reject_skill_install_path_switch")
            .await
            .expect("drop failure trigger");
        assert_eq!(
            store
                .find_skill_installation(&row.skill_id)
                .await
                .expect("find row")
                .expect("row")
                .install_path,
            row.install_path
        );
        assert!(source.exists());
        assert!(root.join(row.skill_id.as_str()).join("test-skill").exists());
    }

    #[tokio::test]
    async fn restart_removes_abandoned_attempt_and_copies_again() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let root = temp.path().join("workspace-one/user");
        let source = root.join("owner/test-skill");
        let fingerprint = write_package(source.as_path(), "current");
        let row = row(
            'I',
            "user",
            "workspace-one",
            source.as_path(),
            fingerprint.as_str(),
        );
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert row");
        let candidate = candidate_for(row.clone(), root.as_path());
        let abandoned = prepare_candidate_copy(candidate.clone()).expect("prepare abandoned copy");
        fs::write(abandoned.attempt_path.join("abandoned.txt"), "stale")
            .expect("mark abandoned copy");

        let outcome = copy_and_switch_candidate(&store, &Arc::new(Mutex::new(())), candidate)
            .await
            .expect("retry relocation");
        assert_eq!(outcome, SkillStorageRelocationOutcome::Switched);
        let destination = root.join(row.skill_id.as_str()).join("test-skill");
        assert!(!destination.join("abandoned.txt").exists());
        assert!(destination.join("assets/value.txt").is_file());
    }

    #[tokio::test]
    async fn restart_replaces_copy_published_before_database_switch() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let root = temp.path().join("workspace-one/user");
        let source = root.join("owner/test-skill");
        let fingerprint = write_package(source.as_path(), "current");
        let row = row(
            'V',
            "user",
            "workspace-one",
            source.as_path(),
            fingerprint.as_str(),
        );
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert row");
        let candidate = candidate_for(row.clone(), root.as_path());
        let prepared = prepare_candidate_copy(candidate.clone()).expect("prepare copy");
        publish_candidate_files(&prepared.candidate, prepared.attempt_path.as_path())
            .expect("simulate publication before process exit");
        fs::write(
            prepared.candidate.destination.join("abandoned.txt"),
            "stale",
        )
        .expect("mark abandoned published destination");
        assert_eq!(
            store
                .find_skill_installation(&row.skill_id)
                .await
                .expect("find row")
                .expect("row")
                .install_path,
            row.install_path
        );

        let outcome = copy_and_switch_candidate(&store, &Arc::new(Mutex::new(())), candidate)
            .await
            .expect("retry relocation");
        assert_eq!(outcome, SkillStorageRelocationOutcome::Switched);
        let destination = root.join(row.skill_id.as_str()).join("test-skill");
        assert!(!destination.join("abandoned.txt").exists());
        assert!(source.parent().is_some_and(|parent| !parent.exists()));
    }

    #[tokio::test]
    async fn external_source_is_preserved_after_switch() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let managed = temp.path().join("managed/workspace-one/user");
        let external = temp.path().join("external/test-skill");
        let fingerprint = write_package(external.as_path(), "external");
        let row = row(
            'J',
            "user",
            "workspace-one",
            external.as_path(),
            fingerprint.as_str(),
        );
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert row");
        let cfg = ExistingInstallationRelocationConfig {
            user_root_templates: vec![managed.display().to_string()],
            registry_root_templates: Vec::new(),
            max_skill_file_bytes: 64 * 1024,
        };

        let summary = relocate_existing_installations(&store, &Arc::new(Mutex::new(())), &cfg)
            .await
            .expect("relocate external row");
        assert_eq!(summary.switched, 1);
        assert!(external.exists());
        assert!(
            managed
                .join(row.skill_id.as_str())
                .join("test-skill")
                .exists()
        );
    }

    #[tokio::test]
    async fn configured_system_user_and_registry_packages_get_managed_id_copies() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let system_source = temp.path().join("configured/system/system-skill");
        let user_source = temp.path().join("configured/user/legacy-owner/user-skill");
        let registry_source = temp.path().join("configured/registry/registry-skill");
        write_import_package(system_source.as_path(), "system body");
        write_import_package(user_source.as_path(), "user body");
        write_import_package(registry_source.as_path(), "registry body");
        let system_managed = temp.path().join("managed/system/imported");
        let user_managed = temp.path().join("managed/workspace-one/user");
        let registry_managed = temp.path().join("managed/workspace-one/registry");
        let roots = vec![
            import_root(
                SkillSourceKind::System,
                "system",
                temp.path().join("configured/system"),
                system_managed.clone(),
            ),
            import_root(
                SkillSourceKind::User,
                "workspace-one",
                temp.path().join("configured/user"),
                user_managed.clone(),
            ),
            import_root(
                SkillSourceKind::Registry,
                "workspace-one",
                temp.path().join("configured/registry"),
                registry_managed.clone(),
            ),
        ];

        let summary =
            import_configured_skill_roots(&store, &Arc::new(Mutex::new(())), &import_config(roots))
                .await
                .expect("configured import pass");
        assert_eq!(summary.packages_discovered, 3);
        assert_eq!(summary.rows_created, 3);
        assert_eq!(summary.switched, 3);
        let rows = store
            .list_skill_installations()
            .await
            .expect("list imported rows");
        assert_eq!(rows.len(), 3);
        for row in rows {
            let expected_root = match row.source_kind.as_str() {
                "system" => system_managed.as_path(),
                "user" => user_managed.as_path(),
                "registry" => registry_managed.as_path(),
                other => panic!("unexpected source kind {other}"),
            };
            assert_eq!(row.owner, None);
            assert!(row.source_ref.starts_with(IMPORT_PATH_PREFIX));
            let source_path = PathBuf::from(
                row.source_ref
                    .strip_prefix(IMPORT_PATH_PREFIX)
                    .expect("import source path"),
            );
            let managed_path = PathBuf::from(row.install_path.as_str());
            assert_eq!(
                managed_path,
                expected_root
                    .join(row.skill_id.as_str())
                    .join(row.slug.as_str())
            );
            assert_eq!(
                fs::read(managed_path.join("SKILL.md")).expect("read managed SKILL.md"),
                fs::read(source_path.join("SKILL.md")).expect("read source SKILL.md")
            );
            assert_eq!(
                fs::read(managed_path.join("assets/value.txt")).expect("read managed asset"),
                fs::read(source_path.join("assets/value.txt")).expect("read source asset")
            );
        }
        assert!(system_source.exists());
        assert!(user_source.exists());
        assert!(registry_source.exists());
        assert!(
            !temp
                .path()
                .join("configured/system/skills-lock.toml")
                .exists()
        );
        assert!(
            !temp
                .path()
                .join("configured/user/skills-lock.toml")
                .exists()
        );
        assert!(
            !temp
                .path()
                .join("configured/registry/skills-lock.toml")
                .exists()
        );
        assert!(system_managed.join("skills-lock.toml").is_file());
        assert!(user_managed.join("skills-lock.toml").is_file());
        assert!(registry_managed.join("skills-lock.toml").is_file());
    }

    #[tokio::test]
    async fn register_only_pass_persists_id_before_later_publication() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let source_root = temp.path().join("configured/user");
        let source = source_root.join("registered-skill");
        let managed = temp.path().join("managed/workspace-one/user");
        write_import_package(source.as_path(), "registered body");
        let mut config = import_config(vec![import_root(
            SkillSourceKind::User,
            "workspace-one",
            source_root,
            managed.clone(),
        )]);
        config.mode = ConfiguredRootImportMode::RegisterOnly;

        let registered = import_configured_skill_roots(&store, &Arc::new(Mutex::new(())), &config)
            .await
            .expect("register configured source");
        assert_eq!(registered.rows_created, 1);
        assert_eq!(registered.switched, 0);
        assert_eq!(registered.failed, 0);
        let pending = store
            .list_skill_installations()
            .await
            .expect("pending row")
            .pop()
            .expect("one pending row");
        assert_eq!(
            PathBuf::from(pending.install_path.as_str()),
            fs::canonicalize(source.as_path()).expect("canonical source")
        );
        assert!(!managed.join(pending.skill_id.as_str()).exists());

        config.mode = ConfiguredRootImportMode::ImportAndRefresh;
        let published = import_configured_skill_roots(&store, &Arc::new(Mutex::new(())), &config)
            .await
            .expect("publish registered source");
        assert_eq!(published.rows_created, 0);
        assert_eq!(published.switched, 1);
        let current = store
            .find_skill_installation(&pending.skill_id)
            .await
            .expect("find current row")
            .expect("current row");
        assert_eq!(current.skill_id, pending.skill_id);
        assert_eq!(
            PathBuf::from(current.install_path),
            managed
                .join(current.skill_id.as_str())
                .join("registered-skill")
        );
    }

    #[tokio::test]
    async fn failed_import_persists_pending_id_and_retry_switches_same_row() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let source_root = temp.path().join("configured/user");
        let source = source_root.join("broken-skill");
        fs::create_dir_all(source.as_path()).expect("create invalid package");
        fs::write(source.join("SKILL.md"), "---\nname: Broken\n---\n")
            .expect("write invalid package");
        let managed = temp.path().join("managed/workspace-one/user");
        let config = import_config(vec![import_root(
            SkillSourceKind::User,
            "workspace-one",
            source_root,
            managed.clone(),
        )]);

        let first = import_configured_skill_roots(&store, &Arc::new(Mutex::new(())), &config)
            .await
            .expect("failed import pass");
        assert_eq!(first.rows_created, 1);
        assert_eq!(first.failed, 1);
        let pending = store
            .list_skill_installations()
            .await
            .expect("list pending row")
            .pop()
            .expect("pending row");
        let pending_id = pending.skill_id.clone();
        assert_eq!(
            PathBuf::from(pending.install_path.as_str()),
            fs::canonicalize(source.as_path()).expect("canonical source")
        );
        assert_eq!(pending.slug, "broken-skill");
        assert!(pending.source_ref.starts_with(IMPORT_PATH_PREFIX));
        assert!(!managed.join(pending_id.as_str()).exists());

        write_import_package(source.as_path(), "restored body");
        let retry = import_configured_skill_roots(&store, &Arc::new(Mutex::new(())), &config)
            .await
            .expect("retry import pass");
        assert_eq!(retry.rows_created, 0);
        assert_eq!(retry.switched, 1);
        let restored = store
            .list_skill_installations()
            .await
            .expect("list restored row")
            .pop()
            .expect("restored row");
        assert_eq!(restored.skill_id, pending_id);
        assert!(Path::new(restored.install_path.as_str()).is_dir());
        assert!(source.exists());
    }

    #[tokio::test]
    async fn repeat_source_scan_does_not_duplicate_row() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let source_root = temp.path().join("configured/user");
        let source = source_root.join("repeat-skill");
        write_import_package(source.as_path(), "same body");
        let config = import_config(vec![import_root(
            SkillSourceKind::User,
            "workspace-one",
            source_root,
            temp.path().join("managed/workspace-one/user"),
        )]);

        let first = import_configured_skill_roots(&store, &Arc::new(Mutex::new(())), &config)
            .await
            .expect("first scan");
        assert_eq!(first.rows_created, 1);
        let id = store.list_skill_installations().await.expect("rows")[0]
            .skill_id
            .clone();
        let second = import_configured_skill_roots(&store, &Arc::new(Mutex::new(())), &config)
            .await
            .expect("repeat scan");
        assert_eq!(second.rows_created, 0);
        assert_eq!(second.unchanged, 1);
        let rows = store
            .list_skill_installations()
            .await
            .expect("rows after repeat");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].skill_id, id);
    }

    #[tokio::test]
    async fn external_content_refresh_preserves_id_and_replaces_managed_copy() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let source_root = temp.path().join("configured/user");
        let source = source_root.join("refresh-skill");
        write_import_package(source.as_path(), "version one");
        let managed = temp.path().join("managed/workspace-one/user");
        let config = import_config(vec![import_root(
            SkillSourceKind::User,
            "workspace-one",
            source_root,
            managed.clone(),
        )]);
        import_configured_skill_roots(&store, &Arc::new(Mutex::new(())), &config)
            .await
            .expect("initial import");
        let initial = store
            .list_skill_installations()
            .await
            .expect("initial rows")[0]
            .clone();

        fs::write(source.join("assets/value.txt"), "version two")
            .expect("change external package asset");
        let mut refresh_config = config.clone();
        refresh_config.force_refresh = true;
        let refresh =
            import_configured_skill_roots(&store, &Arc::new(Mutex::new(())), &refresh_config)
                .await
                .expect("refresh import");
        assert_eq!(refresh.switched, 1);
        let current = store
            .list_skill_installations()
            .await
            .expect("current rows")[0]
            .clone();
        assert_eq!(current.skill_id, initial.skill_id);
        assert_eq!(current.fingerprint, initial.fingerprint);
        assert_eq!(
            fs::read_to_string(Path::new(current.install_path.as_str()).join("assets/value.txt"))
                .expect("read refreshed asset"),
            "version two"
        );
        assert_eq!(
            fs::read_to_string(source.join("assets/value.txt")).expect("read external asset"),
            "version two"
        );
    }

    #[tokio::test]
    async fn managed_edit_is_not_overwritten_by_unchanged_configured_source() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let source_root = temp.path().join("configured/user");
        let source = source_root.join("refresh-skill");
        write_import_package(source.as_path(), "external version");
        let managed = temp.path().join("managed/workspace-one/user");
        let config = import_config(vec![import_root(
            SkillSourceKind::User,
            "workspace-one",
            source_root,
            managed.clone(),
        )]);
        let lock = Arc::new(Mutex::new(()));
        import_configured_skill_roots(&store, &lock, &config)
            .await
            .expect("initial import");
        let initial = store
            .list_skill_installations()
            .await
            .expect("initial rows")[0]
            .clone();
        write_import_package(
            Path::new(initial.install_path.as_str()),
            "manual managed version",
        );

        let configured = import_configured_skill_roots(&store, &lock, &config)
            .await
            .expect("unchanged configured source scan");
        assert_eq!(configured.unchanged, 1);
        assert_eq!(
            fs::read_to_string(Path::new(initial.install_path.as_str()).join("assets/value.txt"))
                .expect("read managed edit"),
            "manual managed version"
        );

        let managed_scan = scan_managed_skill_roots(
            &store,
            &lock,
            &managed_config(SkillSourceKind::User, "workspace-one", managed),
        )
        .await
        .expect("managed scan");
        assert_eq!(managed_scan.refreshed, 1);
        let current = store
            .find_skill_installation(&initial.skill_id)
            .await
            .expect("find row")
            .expect("managed row");
        assert_eq!(current.skill_id, initial.skill_id);
        assert_eq!(
            fs::read_to_string(source.join("assets/value.txt")).expect("read external source"),
            "external version"
        );
    }

    #[tokio::test]
    async fn managed_content_edit_and_repeat_event_preserve_id() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let managed = temp.path().join("managed/workspace-one/user");
        let row = managed_row(
            'L',
            "user",
            "workspace-one",
            managed.as_path(),
            "test-skill",
            "version one",
        );
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert managed row");
        let changed_fingerprint = write_package(
            Path::new(row.install_path.as_str()),
            "version two with changed instructions",
        );
        let config = managed_config(SkillSourceKind::User, "workspace-one", managed.clone());

        let first = scan_managed_skill_roots(&store, &Arc::new(Mutex::new(())), &config)
            .await
            .expect("managed refresh");
        assert_eq!(first.refreshed, 1);
        let current = store
            .find_skill_installation(&row.skill_id)
            .await
            .expect("find row")
            .expect("managed row");
        assert_eq!(current.skill_id, row.skill_id);
        assert_eq!(current.fingerprint, changed_fingerprint);
        assert_eq!(
            fs::read_to_string(Path::new(current.install_path.as_str()).join("assets/value.txt"))
                .expect("read refreshed asset"),
            "version two with changed instructions"
        );

        let repeat = scan_managed_skill_roots(&store, &Arc::new(Mutex::new(())), &config)
            .await
            .expect("repeat managed refresh");
        assert_eq!(repeat.unchanged, 1);
        assert_eq!(
            store
                .list_skill_installations()
                .await
                .expect("rows after repeat")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn managed_leaf_rename_updates_slug_and_path_without_rewriting_package() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let managed = temp.path().join("managed/workspace-one/user");
        let row = managed_row(
            'M',
            "user",
            "workspace-one",
            managed.as_path(),
            "test-skill",
            "rename body",
        );
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert managed row");
        let original_package =
            fs::read_to_string(Path::new(row.install_path.as_str()).join("SKILL.md"))
                .expect("read original package");
        let raw_renamed = managed.join(row.skill_id.as_str()).join("Renamed Skill");
        fs::rename(Path::new(row.install_path.as_str()), raw_renamed.as_path())
            .expect("rename leaf manually");
        let config = managed_config(SkillSourceKind::User, "workspace-one", managed.clone());

        let summary = scan_managed_skill_roots(&store, &Arc::new(Mutex::new(())), &config)
            .await
            .expect("scan rename");
        assert_eq!(summary.renamed, 1);
        let current = store
            .find_skill_installation(&row.skill_id)
            .await
            .expect("find row")
            .expect("renamed row");
        let normalized_path = managed.join(row.skill_id.as_str()).join("renamed-skill");
        assert_eq!(current.skill_id, row.skill_id);
        assert_eq!(current.slug, "renamed-skill");
        assert_eq!(PathBuf::from(current.install_path), normalized_path);
        assert!(!raw_renamed.exists());
        assert_eq!(
            fs::read_to_string(normalized_path.join("SKILL.md")).expect("read renamed package"),
            original_package
        );
    }

    #[tokio::test]
    async fn scan_finishes_leaf_rename_interrupted_before_database_update() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let managed = temp.path().join("managed/workspace-one/user");
        let row = managed_row(
            'W',
            "user",
            "workspace-one",
            managed.as_path(),
            "old-skill",
            "rename body",
        );
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert managed row");
        let normalized_path = managed.join(row.skill_id.as_str()).join("renamed-skill");
        fs::rename(
            Path::new(row.install_path.as_str()),
            normalized_path.as_path(),
        )
        .expect("simulate published rename before database update");
        let config = managed_config(SkillSourceKind::User, "workspace-one", managed);

        let summary = scan_managed_skill_roots(&store, &Arc::new(Mutex::new(())), &config)
            .await
            .expect("finish interrupted rename");
        assert_eq!(summary.renamed, 1);
        let current = store
            .find_skill_installation(&row.skill_id)
            .await
            .expect("find row")
            .expect("renamed row");
        assert_eq!(current.slug, "renamed-skill");
        assert_eq!(PathBuf::from(current.install_path), normalized_path);
    }

    #[tokio::test]
    async fn arbitrary_id_like_managed_folder_gets_generated_identity() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let managed = temp.path().join("managed/workspace-one/user");
        let requested_text = "NNNNNNNNNNNNNNNNNNNNN";
        let source = managed.join(requested_text).join("manual-skill");
        write_import_package(source.as_path(), "manual body");
        let scan_config = managed_config(SkillSourceKind::User, "workspace-one", managed.clone());

        let scan = scan_managed_skill_roots(&store, &Arc::new(Mutex::new(())), &scan_config)
            .await
            .expect("scan unknown folder");
        assert_eq!(scan.rows_seen, 0);
        assert!(
            store
                .list_skill_installations()
                .await
                .expect("rows")
                .is_empty()
        );

        let import = ConfiguredRootImportConfig {
            roots: vec![ConfiguredSkillImportRoot {
                source_kind: SkillSourceKind::User,
                scope_key: "workspace-one".to_owned(),
                source_root: managed.clone(),
                managed_root: managed.clone(),
                source_is_pioneer_managed: true,
            }],
            installer_policy: SkillInstallerPolicy::default(),
            max_skills_per_root: 64,
            force_refresh: false,
            mode: ConfiguredRootImportMode::ImportAndRefresh,
            reserved_skill_ids: HashSet::new(),
        };
        let imported = import_configured_skill_roots(&store, &Arc::new(Mutex::new(())), &import)
            .await
            .expect("import unknown folder");
        assert_eq!(imported.rows_created, 1);
        let row = store
            .list_skill_installations()
            .await
            .expect("imported row")
            .pop()
            .expect("one row");
        assert_ne!(row.skill_id.as_str(), requested_text);
        assert_eq!(row.slug, "manual-skill");
        assert_eq!(
            PathBuf::from(row.install_path),
            managed.join(row.skill_id.as_str()).join("manual-skill")
        );
    }

    #[tokio::test]
    async fn extra_leaf_inside_known_id_container_is_not_a_new_installation() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let managed = temp.path().join("managed/workspace-one/user");
        let row = managed_row(
            'S',
            "user",
            "workspace-one",
            managed.as_path(),
            "test-skill",
            "current body",
        );
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert known row");
        write_import_package(
            managed
                .join(row.skill_id.as_str())
                .join("extra-skill")
                .as_path(),
            "extra body",
        );
        let import = ConfiguredRootImportConfig {
            roots: vec![ConfiguredSkillImportRoot {
                source_kind: SkillSourceKind::User,
                scope_key: "workspace-one".to_owned(),
                source_root: managed.clone(),
                managed_root: managed,
                source_is_pioneer_managed: true,
            }],
            installer_policy: SkillInstallerPolicy::default(),
            max_skills_per_root: 64,
            force_refresh: true,
            mode: ConfiguredRootImportMode::ImportAndRefresh,
            reserved_skill_ids: HashSet::new(),
        };

        let summary = import_configured_skill_roots(&store, &Arc::new(Mutex::new(())), &import)
            .await
            .expect("ignore known ID container leaves");
        assert_eq!(summary.rows_created, 0);
        let rows = store.list_skill_installations().await.expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].skill_id, row.skill_id);
    }

    #[tokio::test]
    async fn missing_and_invalid_managed_packages_keep_their_rows_unavailable() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let managed = temp.path().join("managed/workspace-one/user");
        let missing_id = test_id('O');
        let missing_path = managed.join(missing_id.as_str()).join("missing-skill");
        let missing = SkillInstallationRecord {
            skill_id: missing_id.clone(),
            owner: None,
            slug: "missing-skill".to_owned(),
            version: None,
            source_kind: "user".to_owned(),
            scope_key: "workspace-one".to_owned(),
            source_ref: "manual:missing".to_owned(),
            install_path: missing_path.display().to_string(),
            trust_level: "community".to_owned(),
            fingerprint: String::new(),
            updated_at_unix: 1_700_000_000,
        };
        store
            .insert_skill_installation(&missing, missing.updated_at_unix)
            .await
            .expect("insert missing row");
        let invalid = managed_row(
            'P',
            "user",
            "workspace-one",
            managed.as_path(),
            "invalid-skill",
            "valid before corruption",
        );
        store
            .insert_skill_installation(&invalid, invalid.updated_at_unix)
            .await
            .expect("insert invalid row");
        fs::write(
            Path::new(invalid.install_path.as_str()).join("SKILL.md"),
            "---\nname: Invalid\n---\n",
        )
        .expect("corrupt package");
        let config = managed_config(SkillSourceKind::User, "workspace-one", managed);

        let summary = scan_managed_skill_roots(&store, &Arc::new(Mutex::new(())), &config)
            .await
            .expect("scan unavailable rows");
        assert_eq!(summary.unavailable, 2);
        assert_eq!(
            store
                .list_skill_installations()
                .await
                .expect("preserved unavailable rows")
                .len(),
            2
        );
        assert!(
            store
                .find_skill_installation(&missing_id)
                .await
                .expect("find missing row")
                .is_some()
        );
    }

    #[tokio::test]
    async fn managed_refresh_update_race_keeps_newer_row_and_files() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let managed = temp.path().join("managed/workspace-one/user");
        let row = managed_row(
            'Q',
            "user",
            "workspace-one",
            managed.as_path(),
            "test-skill",
            "before edit",
        );
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert row");
        let edited_fingerprint = write_package(
            Path::new(row.install_path.as_str()),
            "manual edit before concurrent update",
        );
        let mut metadata = PreparedSkillStorageMetadata::from_row(&row);
        metadata.fingerprint = edited_fingerprint;
        store
            .update_skill_installation(
                &row.skill_id,
                &SkillInstallationPatch {
                    fingerprint: Some("concurrent-update".to_owned()),
                    ..Default::default()
                },
                1_700_000_001,
            )
            .await
            .expect("concurrent update");

        let outcome = refresh_managed_installation(
            &store,
            &Arc::new(Mutex::new(())),
            row.clone(),
            PathBuf::from(row.install_path.as_str()),
            PathBuf::from(row.install_path.as_str()),
            metadata,
            managed.join("skills-lock.toml"),
            64 * 1024,
        )
        .await
        .expect("publish stale refresh");
        assert_eq!(outcome, SkillStorageRelocationOutcome::Stale);
        assert_eq!(
            store
                .find_skill_installation(&row.skill_id)
                .await
                .expect("find row")
                .expect("row")
                .fingerprint,
            "concurrent-update"
        );
        assert!(Path::new(row.install_path.as_str()).is_dir());
    }

    #[tokio::test]
    async fn managed_refresh_uninstall_race_does_not_restore_row_or_files() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let managed = temp.path().join("managed/workspace-one/user");
        let row = managed_row(
            'R',
            "user",
            "workspace-one",
            managed.as_path(),
            "test-skill",
            "before uninstall",
        );
        store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .expect("insert row");
        store
            .delete_skill_installation(&row.skill_id)
            .await
            .expect("concurrent uninstall");
        remove_any(Path::new(row.install_path.as_str())).expect("remove uninstalled package");

        let outcome = refresh_managed_installation(
            &store,
            &Arc::new(Mutex::new(())),
            row.clone(),
            PathBuf::from(row.install_path.as_str()),
            PathBuf::from(row.install_path.as_str()),
            PreparedSkillStorageMetadata::from_row(&row),
            managed.join("skills-lock.toml"),
            64 * 1024,
        )
        .await
        .expect("publish stale refresh");
        assert_eq!(outcome, SkillStorageRelocationOutcome::Stale);
        assert!(
            store
                .find_skill_installation(&row.skill_id)
                .await
                .expect("find row")
                .is_none()
        );
        assert!(!Path::new(row.install_path.as_str()).exists());
    }

    #[tokio::test]
    async fn concurrent_first_lock_access_converts_v1_once_under_existing_lock() {
        let temp = TempDir::new().expect("tempdir");
        let lock_path = temp.path().join("skills-lock.toml");
        fs::write(
            lock_path.as_path(),
            "version = 1\n[[entries]]\nowner='owner'\nslug='test-skill'\nsource_kind='user'\nsource_ref='legacy'\ninstall_path='/legacy/test-skill'\ntrust_level='community'\nfingerprint='legacy-fingerprint'\ninstalled_at=7\n",
        )
        .expect("write v1 lock");
        let candidate = pioneer_skills::SkillLockConversionCandidate {
            skill_id: test_id('T'),
            owner: Some("owner".to_owned()),
            slug: "test-skill".to_owned(),
            source_kind: "user".to_owned(),
            source_ref: "legacy".to_owned(),
            install_path: "/legacy/test-skill".to_owned(),
            version: None,
            trust_level: SkillTrustLevel::Community,
            fingerprint: "legacy-fingerprint".to_owned(),
        };
        let lock = Arc::new(Mutex::new(()));
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let lock = lock.clone();
            let lock_path = lock_path.clone();
            let candidate = candidate.clone();
            tasks.push(tokio::spawn(async move {
                let _guard = lock.lock().await;
                pioneer_skills::ensure_skills_lock_v2(&lock_path, &[candidate]).expect("ensure v2")
            }));
        }
        let first = tasks.remove(0).await.expect("first ensure task");
        let second = tasks.remove(0).await.expect("second ensure task");
        assert_eq!(first, second);
        assert_eq!(first.entries.len(), 1);
        assert_eq!(first.entries[0].skill_id, test_id('T'));
        assert_eq!(read_skills_lock(&lock_path).expect("strict v2 read"), first);
    }

    #[tokio::test]
    async fn configured_import_partial_failure_continues_and_retries_same_pending_id() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store().await;
        let source_root = temp.path().join("configured/user");
        let valid = source_root.join("valid-skill");
        let broken = source_root.join("broken-skill");
        write_import_package(valid.as_path(), "valid body");
        fs::create_dir_all(broken.as_path()).expect("create broken package");
        fs::write(broken.join("SKILL.md"), "---\nname: Broken\n---\n")
            .expect("write broken package");
        let config = import_config(vec![import_root(
            SkillSourceKind::User,
            "workspace-one",
            source_root,
            temp.path().join("managed/workspace-one/user"),
        )]);

        let first = import_configured_skill_roots(&store, &Arc::new(Mutex::new(())), &config)
            .await
            .expect("partial import pass");
        assert_eq!(first.rows_created, 2);
        assert_eq!(first.switched, 1);
        assert_eq!(first.failed, 1);
        let pending = store
            .list_skill_installations()
            .await
            .expect("rows")
            .into_iter()
            .find(|row| row.slug == "broken-skill")
            .expect("pending row");
        let pending_id = pending.skill_id.clone();

        write_import_package(broken.as_path(), "repaired body");
        let retry = import_configured_skill_roots(&store, &Arc::new(Mutex::new(())), &config)
            .await
            .expect("retry import pass");
        assert_eq!(retry.rows_created, 0);
        assert_eq!(retry.switched, 1);
        let repaired = store
            .find_skill_installation(&pending_id)
            .await
            .expect("find repaired row")
            .expect("repaired row");
        assert_eq!(repaired.skill_id, pending_id);
        assert!(Path::new(repaired.install_path.as_str()).is_dir());
    }
}
