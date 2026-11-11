use super::super::upload::MaterializedSkillSource;
use super::*;
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
                format!("plugin:{}:{}", ownership.plugin_id, ownership.member_key)
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
            SkillInstallSource::PackageMember(ownership, admission) => {
                let parent = self
                    .crud_store
                    .find_plugin_installation(&ownership.plugin_id)
                    .await
                    .map_err(|_| invalid())?
                    .ok_or_else(invalid)?;
                if parent.workspace_id != workspace
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
                let path = format!("skills/{}", ownership.member_key);
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
                let package_limits = self.package_delivery_context().plugin_limits();
                crate::message::package_delivery::prepare_cancellable(
                    move |cancellation| -> Result<PreparedInstallSource> {
                        let mut cleanup =
                            crate::message::package_delivery::TemporaryDirectory::new()?;
                        let package = Path::new(&parent.package_path);
                        let source_dir = pioneer_plugins::containment::resolve_contained(
                            package,
                            &package.join(path),
                        )?;
                        crate::message::package_delivery::check_preparation(cancellation)?;
                        let staged = pioneer_skills::stage_skill_folder(
                            &source_dir,
                            &location.install_root,
                            &policy,
                            true,
                        )?;
                        cleanup.remove_on_drop(staged.cleanup_root);
                        let prepared = PreparedInstallSource::Package {
                            source_dir: staged.source_dir,
                            _cleanup: cleanup,
                            ownership,
                            _admission: admission,
                        };
                        let expected = prepared
                            .ownership()
                            .expect("package source")
                            .package_fingerprint
                            .as_str();
                        let copy = pioneer_plugins::Snapshot::capture(
                            prepared.source_dir(),
                            package_limits,
                            || cancellation.is_cancelled(),
                        )?;
                        anyhow::ensure!(copy.tree_digest() == expected, "plugins.package_changed");
                        Ok(prepared)
                    },
                )
                .await
                .map_err(|_| invalid())
            }
        }
    }
}
