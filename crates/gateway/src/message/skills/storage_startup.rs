use super::storage_relocation::{
    ConfiguredRootImportMode, ConfiguredRootImportSummary, ExistingInstallationRelocationConfig,
    import_configured_skill_roots, relocate_existing_installations, scan_managed_skill_roots,
};
use super::*;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SkillStorageStartupSummary {
    pub(crate) discovered: usize,
    pub(crate) imported: usize,
    pub(crate) relocated: usize,
    pub(crate) skipped: usize,
    pub(crate) failed: usize,
}

impl MessageProcessor {
    pub(crate) async fn run_skill_storage_startup_pass(&self) -> SkillStorageStartupSummary {
        let mut summary = SkillStorageStartupSummary::default();
        let workspaces = match self.workspace_manager.list_workspaces().await {
            Ok(workspaces) => workspaces
                .into_iter()
                .filter(|workspace| workspace.is_active)
                .collect::<Vec<_>>(),
            Err(error) => {
                summary.failed = summary.failed.saturating_add(1);
                warn!(
                    operation = "list_workspaces",
                    error = %format!("{error:#}"),
                    "stable skill storage startup operation failed"
                );
                Vec::new()
            }
        };

        let workspace_ids = workspaces
            .iter()
            .map(|workspace| workspace.id.clone())
            .collect::<Vec<_>>();

        // Register source provenance and stable IDs before strict v2 lock conversion. Publication
        // happens in a second pass after both legacy file converters have completed.
        let registration = run_configured_import_phase(
            self,
            workspace_ids.as_slice(),
            ConfiguredRootImportMode::RegisterOnly,
        )
        .await;
        summary.discovered = summary
            .discovered
            .saturating_add(registration.packages_discovered);
        summary.failed = summary.failed.saturating_add(registration.failed);

        // File conversion uses the same mutation lock as first lifecycle/projection access.
        for workspace in &workspaces {
            let context = match self.skills_runtime_context(workspace.id.as_str()) {
                Ok(context) => context,
                Err(error) => {
                    summary.failed = summary.failed.saturating_add(1);
                    warn!(
                        operation = "lock_conversion_config",
                        workspace_id = %workspace.id,
                        error = %format!("{error:#}"),
                        "stable skill storage startup operation failed"
                    );
                    continue;
                }
            };
            for (source_kind, lock_path, scope_key) in [
                (
                    SkillSourceKind::User,
                    context.user_lock_path.as_path(),
                    workspace.id.as_str(),
                ),
                (
                    SkillSourceKind::Registry,
                    context.registry_lock_path.as_path(),
                    workspace.id.as_str(),
                ),
            ] {
                let _guard = self.acquire_skills_write_lock().await;
                if let Err(error) = self
                    .ensure_skills_lock_v2_locked(lock_path, source_kind, scope_key)
                    .await
                {
                    summary.failed = summary.failed.saturating_add(1);
                    warn!(
                        operation = "skills_lock_conversion",
                        workspace_id = %workspace.id,
                        source_kind = source_kind.as_db_value(),
                        path = %lock_path.display(),
                        error = %format!("{error:#}"),
                        "stable skill storage startup operation failed"
                    );
                }
            }
        }
        let system_lock = self
            .artifact_runtime_home
            .join("skills")
            .join("system")
            .join("imported")
            .join("skills-lock.toml");
        let _guard = self.acquire_skills_write_lock().await;
        if let Err(error) = self
            .ensure_skills_lock_v2_locked(system_lock.as_path(), SkillSourceKind::System, "system")
            .await
        {
            summary.failed = summary.failed.saturating_add(1);
            warn!(
                operation = "skills_lock_conversion",
                source_kind = "system",
                path = %system_lock.display(),
                error = %format!("{error:#}"),
                "stable skill storage startup operation failed"
            );
        }
        drop(_guard);
        match self.external_runtime_receipt_conversion_candidates().await {
            Ok(candidates) => {
                let receipt_path = self
                    .artifact_runtime_home
                    .join(pioneer_skills::EXTERNAL_RUNTIME_RECEIPT_FILE_NAME);
                let _guard = self.acquire_skills_write_lock().await;
                if let Err(error) = pioneer_skills::ensure_external_runtime_receipt_v2(
                    receipt_path.as_path(),
                    candidates.as_slice(),
                ) {
                    summary.failed = summary.failed.saturating_add(1);
                    warn!(
                        operation = "external_runtime_receipt_conversion",
                        path = %receipt_path.display(),
                        error = %format!("{error:#}"),
                        "stable skill storage startup operation failed"
                    );
                }
            }
            Err(error) => {
                summary.failed = summary.failed.saturating_add(1);
                warn!(
                    operation = "receipt_conversion_candidates",
                    error = %format!("{error:#}"),
                    "stable skill storage startup operation failed"
                );
            }
        }

        let imported = run_configured_import_phase(
            self,
            workspace_ids.as_slice(),
            ConfiguredRootImportMode::ImportAndRefresh,
        )
        .await;
        summary.imported = summary.imported.saturating_add(imported.switched);
        summary.skipped = summary
            .skipped
            .saturating_add(imported.unchanged)
            .saturating_add(imported.stale);
        summary.failed = summary.failed.saturating_add(imported.failed);

        // Conversion is complete before managed refresh and relocation need strict v2 locks.
        for (index, workspace) in workspaces.iter().enumerate() {
            match self.managed_root_scan_config(workspace.id.as_str()) {
                Ok(mut config) => {
                    if index > 0 {
                        config
                            .roots
                            .retain(|root| root.source_kind != SkillSourceKind::System);
                    }
                    match scan_managed_skill_roots(
                        self.crud_store.as_ref(),
                        &self.skills_write_lock,
                        &config,
                    )
                    .await
                    {
                        Ok(scan) => {
                            summary.relocated = summary
                                .relocated
                                .saturating_add(scan.refreshed)
                                .saturating_add(scan.renamed);
                            summary.skipped = summary.skipped.saturating_add(scan.unchanged);
                            summary.failed = summary.failed.saturating_add(scan.failed);
                        }
                        Err(error) => {
                            summary.failed = summary.failed.saturating_add(1);
                            warn!(
                                operation = "managed_scan",
                                workspace_id = %workspace.id,
                                error = %format!("{error:#}"),
                                "stable skill storage startup operation failed"
                            );
                        }
                    }
                }
                Err(error) => {
                    summary.failed = summary.failed.saturating_add(1);
                    warn!(
                        operation = "managed_scan_config",
                        workspace_id = %workspace.id,
                        error = %format!("{error:#}"),
                        "stable skill storage startup operation failed"
                    );
                }
            }
        }

        let relocation = ExistingInstallationRelocationConfig {
            user_root_templates: self.tool_loop_config.skills.user_roots.clone(),
            registry_root_templates: self.tool_loop_config.skills.registry_roots.clone(),
            max_skill_file_bytes: self.tool_loop_config.skills.max_skill_file_bytes.max(1),
        };
        match relocate_existing_installations(
            self.crud_store.as_ref(),
            &self.skills_write_lock,
            &relocation,
        )
        .await
        {
            Ok(relocation) => {
                summary.relocated = summary.relocated.saturating_add(relocation.switched);
                summary.skipped = summary
                    .skipped
                    .saturating_add(relocation.skipped_canonical)
                    .saturating_add(relocation.skipped_pending_import)
                    .saturating_add(relocation.skipped_unsupported);
                summary.failed = summary.failed.saturating_add(relocation.failed);
            }
            Err(error) => {
                summary.failed = summary.failed.saturating_add(1);
                warn!(
                    operation = "existing_relocation",
                    error = %format!("{error:#}"),
                    "stable skill storage startup operation failed"
                );
            }
        }

        tracing::info!(
            discovered = summary.discovered,
            imported = summary.imported,
            relocated = summary.relocated,
            skipped = summary.skipped,
            failed = summary.failed,
            "stable skill storage startup pass completed"
        );
        summary
    }
}

async fn run_configured_import_phase(
    processor: &MessageProcessor,
    workspace_ids: &[String],
    mode: ConfiguredRootImportMode,
) -> ConfiguredRootImportSummary {
    let mut total = ConfiguredRootImportSummary::default();
    for (index, workspace_id) in workspace_ids.iter().enumerate() {
        let mut config = match processor.configured_root_import_config(workspace_id, false) {
            Ok(config) => config,
            Err(error) => {
                total.failed = total.failed.saturating_add(1);
                warn!(
                    operation = "configured_import_config",
                    workspace_id,
                    error = %format!("{error:#}"),
                    "stable skill storage startup operation failed"
                );
                continue;
            }
        };
        config.mode = mode;
        if index > 0 {
            config
                .roots
                .retain(|root| root.source_kind != SkillSourceKind::System);
        }
        match import_configured_skill_roots(
            processor.crud_store.as_ref(),
            &processor.skills_write_lock,
            &config,
        )
        .await
        {
            Ok(summary) => {
                total.packages_discovered = total
                    .packages_discovered
                    .saturating_add(summary.packages_discovered);
                total.rows_created = total.rows_created.saturating_add(summary.rows_created);
                total.switched = total.switched.saturating_add(summary.switched);
                total.unchanged = total.unchanged.saturating_add(summary.unchanged);
                total.stale = total.stale.saturating_add(summary.stale);
                total.failed = total.failed.saturating_add(summary.failed);
            }
            Err(error) => {
                total.failed = total.failed.saturating_add(1);
                warn!(
                    operation = "configured_import",
                    workspace_id,
                    error = %format!("{error:#}"),
                    "stable skill storage startup operation failed"
                );
            }
        }
    }
    total
}
