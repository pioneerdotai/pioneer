use super::super::upload::MaterializedSkillSource;
use super::*;
use crate::message::apps::storage::{AppInstallAdmission, AppStorage, DiskReservation};
use pioneer_crud::PluginOwnershipWrite;

/// Internal source contract. Package members are host-owned, already staged
/// full trees. They never impersonate a finalized standalone skill upload.
pub(crate) struct SkillUpdateInput {
    pub workspace_id: String,
    pub skill_id: SkillId,
    pub expected_previous_fingerprint: Option<String>,
}
pub(crate) enum SkillInstallSource {
    UploadedSkill(SkillLifecycleSource),
    PackageMember(
        PluginOwnershipWrite,
        std::sync::Arc<tokio::sync::OwnedMutexGuard<()>>,
        Option<std::sync::Arc<AppInstallAdmission>>,
    ),
    OwnedUpload(SkillLifecycleSource, pioneer_crud::PluginNativeWrite),
}
pub(super) enum PreparedInstallSource {
    Uploaded(MaterializedSkillSource),
    OwnedUpload(MaterializedSkillSource, pioneer_crud::PluginNativeWrite),
    Package {
        source_dir: PathBuf,
        _cleanup: crate::message::package_delivery::TemporaryDirectory,
        ownership: PluginOwnershipWrite,
        _admission: std::sync::Arc<tokio::sync::OwnedMutexGuard<()>>,
        _app_install: Option<std::sync::Arc<AppInstallAdmission>>,
    },
}
impl PreparedInstallSource {
    pub async fn prepare(
        self,
        request: pioneer_skills::PrepareMaterializedSkillRequest,
    ) -> Result<(Self, Result<pioneer_skills::PreparedMaterializedSkill>)> {
        crate::message::package_delivery::prepare_cancellable(move |cancellation| {
            let prepared = pioneer_skills::prepare_materialized_skill(request);
            crate::message::package_delivery::check_preparation(cancellation)?;
            Ok((self, prepared))
        })
        .await
    }
    pub(super) fn is_app_member(&self) -> bool {
        matches!(
            self,
            Self::Package {
                _app_install: Some(_),
                ..
            }
        )
    }

    pub(super) fn commit(
        &self,
        storage: &AppStorage,
        request: pioneer_skills::CommitPreparedSkillRequest,
    ) -> (
        Result<pioneer_skills::InstallSkillResult>,
        Option<DiskReservation>,
    ) {
        let mut lock_space = None;
        let result = if self.is_app_member() {
            pioneer_skills::commit_prepared_skill_admitted(request, |path, bytes| {
                lock_space = Some(AppSkillLockWrite::Budgeted.reserve(storage, path, bytes)?);
                Ok(())
            })
        } else {
            pioneer_skills::commit_prepared_skill(request)
        };
        (result, lock_space)
    }

    pub fn uploaded(&self) -> Option<&MaterializedSkillSource> {
        match self {
            Self::Uploaded(upload) | Self::OwnedUpload(upload, _) => Some(upload),
            _ => None,
        }
    }
    pub fn upload_id(&self) -> Option<&str> {
        self.uploaded()
            .map(|source| source.upload.upload_id.as_str())
    }
    pub fn source_dir(&self) -> &Path {
        match self {
            Self::Uploaded(upload) | Self::OwnedUpload(upload, _) => &upload.source_dir,
            Self::Package { source_dir, .. } => source_dir,
        }
    }
    pub fn ownership(&self) -> Option<&PluginOwnershipWrite> {
        match self {
            Self::Package { ownership, .. } => Some(ownership),
            _ => None,
        }
    }
    pub fn native_write(&self) -> Option<&pioneer_crud::PluginNativeWrite> {
        match self {
            Self::OwnedUpload(_, native) => Some(native),
            _ => None,
        }
    }
    pub fn source_ref(&self) -> String {
        match self {
            Self::Uploaded(upload) | Self::OwnedUpload(upload, _) => {
                format!("upload:{}", upload.upload.upload_id)
            }
            Self::Package { ownership, .. } => {
                format!(
                    "{}:{}:{}",
                    ownership.kind.as_str(),
                    ownership.plugin_id,
                    ownership.member_key
                )
            }
        }
    }
}

impl MessageProcessor {
    pub(super) async fn materialize_install_source(
        &self,
        request_context: &RequestContext,
        workspace: &str,
        source: SkillInstallSource,
        context: &SkillsRuntimeContext,
        source_kind: SkillSourceKind,
        request_id: &RequestId,
    ) -> std::result::Result<PreparedInstallSource, JsonRpcErrorResponse> {
        let invalid = || {
            skills_error(
                Some(request_id.clone()),
                INVALID_PARAMS_CODE,
                SKILLS_ERROR_INVALID_REQUEST,
                "package skill source is unavailable or changed",
                json!({}),
            )
        };
        let authenticated_owner = AuthenticatedTransferOwner::from_request_context(request_context);
        match source {
            SkillInstallSource::UploadedSkill(source) => {
                let upload_id = parse_lifecycle_upload_id(source).map_err(|error| {
                    skills_error(
                        Some(request_id.clone()),
                        INVALID_PARAMS_CODE,
                        SKILLS_ERROR_INVALID_REQUEST,
                        "invalid lifecycle source",
                        json!({"error": error}),
                    )
                })?;
                self.materialize_uploaded_skill_source(
                    &authenticated_owner,
                    workspace,
                    &upload_id,
                    request_id,
                )
                .await
                .map(PreparedInstallSource::Uploaded)
            }
            SkillInstallSource::OwnedUpload(source, native) => {
                let upload_id = parse_lifecycle_upload_id(source).map_err(|_| invalid())?;
                let upload = self
                    .materialize_uploaded_skill_source(
                        &authenticated_owner,
                        workspace,
                        &upload_id,
                        request_id,
                    )
                    .await?;
                Ok(PreparedInstallSource::OwnedUpload(upload, native))
            }
            SkillInstallSource::PackageMember(ownership, admission, app_install) => {
                let parent = self
                    .crud_store
                    .find_package_installation(&ownership.plugin_id)
                    .await
                    .map_err(|_| invalid())?
                    .ok_or_else(invalid)?;
                if parent.kind != ownership.kind
                    || parent.workspace_id != workspace
                    || parent.revision != ownership.expected_revision
                    || !matches!(
                        parent.state.as_str(),
                        "installing" | "updating" | "interrupted"
                    )
                {
                    return Err(invalid());
                }
                // The parent operation loaded/fingerprinted the full package
                // before reserving it. Check the fixed member path and verify
                // this copied member below; do not rehash every sibling for
                // every child in the sequential install loop.
                let skills_root = match parent.kind {
                    pioneer_protocol::PackageKind::Plugin => "skills".to_owned(),
                    pioneer_protocol::PackageKind::App => {
                        let manifest = pioneer_apps::AppManifest::parse(
                            parent
                                .app_manifest_json
                                .as_deref()
                                .ok_or_else(invalid)?
                                .as_bytes(),
                        )
                        .map_err(|_| invalid())?;
                        let declared = manifest
                            .document()
                            .get("skills")
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(invalid)?;
                        declared
                            .strip_prefix("./")
                            .unwrap_or(declared)
                            .trim_end_matches('/')
                            .to_owned()
                    }
                };
                let path = format!("{skills_root}/{}", ownership.member_key);
                if ownership.member_key.is_empty()
                    || ownership.member_key.contains(['/', '\\', ':'])
                    || matches!(ownership.member_key.as_str(), "." | "..")
                    || ownership.member_path.as_deref() != Some(path.as_str())
                {
                    return Err(invalid());
                }
                let location =
                    install_location_for_source_kind(context, &source_kind).ok_or_else(invalid)?;
                let policy = installer_policy(context);
                let package_limits = self
                    .package_delivery_context()
                    .limits_for_package(parent.kind);
                let storage = self.app_storage.clone();
                let app = parent.kind == pioneer_protocol::PackageKind::App;
                crate::message::package_delivery::prepare_cancellable(
                    move |cancellation| -> Result<PreparedInstallSource> {
                        let mut cleanup =
                            crate::message::package_delivery::TemporaryDirectory::new()?;
                        let package = Path::new(&parent.package_path);
                        let source_dir = pioneer_packages::containment::resolve_contained(
                            package,
                            &package.join(path),
                        )?;
                        crate::message::package_delivery::check_preparation(cancellation)?;
                        let disk = if app {
                            anyhow::ensure!(
                                app_install.is_some(),
                                "apps.install_admission_missing"
                            );
                            Some(storage.reserve_skill_stage(
                                &location.install_root.join(".staging"),
                                policy.security.max_install_archive_bytes.max(1),
                                package_limits,
                            )?)
                        } else {
                            None
                        };
                        let staged = if app {
                            pioneer_skills::stage_app_skill_folder(
                                &source_dir,
                                &location.install_root,
                                &policy,
                                package_limits.expanded_bytes,
                                package_limits.entries,
                                || cancellation.is_cancelled(),
                            )?
                        } else {
                            pioneer_skills::stage_skill_folder(
                                &source_dir,
                                &location.install_root,
                                &policy,
                                true,
                            )?
                        };
                        cleanup.remove_on_drop(staged.cleanup_root);
                        // The staged files now count in filesystem free space.
                        // Native commit renames them and reserves its lockfile separately.
                        drop(disk);
                        let prepared = PreparedInstallSource::Package {
                            source_dir: staged.source_dir,
                            _cleanup: cleanup,
                            ownership,
                            _admission: admission,
                            _app_install: app_install,
                        };
                        let expected = prepared
                            .ownership()
                            .expect("package source")
                            .package_fingerprint
                            .as_str();
                        let digest = if app {
                            pioneer_packages::Snapshot::capture_file_backed(
                                prepared.source_dir(),
                                package_limits,
                                |_| false,
                                || cancellation.is_cancelled(),
                            )?
                            .checked_tree_digest_cancellable(|| cancellation.is_cancelled())?
                        } else {
                            pioneer_packages::Snapshot::capture(
                                prepared.source_dir(),
                                package_limits,
                                || cancellation.is_cancelled(),
                            )?
                            .tree_digest()
                        };
                        anyhow::ensure!(digest == expected, "plugins.package_changed");
                        Ok(prepared)
                    },
                )
                .await
                .map_err(|error| {
                    app_skill_capacity_error(request_id, &error).unwrap_or_else(invalid)
                })
            }
        }
    }
}

pub(super) fn app_skill_capacity_error(
    request: &RequestId,
    error: &anyhow::Error,
) -> Option<JsonRpcErrorResponse> {
    let diagnostic = AppStorage::capacity_error(&error.to_string())?;
    Some(JsonRpcErrorResponse::new(
        Some(request.clone()),
        INVALID_REQUEST_CODE,
        diagnostic,
    ))
}
