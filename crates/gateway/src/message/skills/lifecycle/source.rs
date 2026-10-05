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
    PackageMember(PluginOwnershipWrite),
    OwnedUpload(SkillLifecycleSource, pioneer_crud::PluginNativeWrite),
}
pub(super) enum PreparedInstallSource {
    Uploaded(MaterializedSkillSource),
    OwnedUpload(MaterializedSkillSource, pioneer_crud::PluginNativeWrite),
    Package {
        source_dir: PathBuf,
        cleanup_root: PathBuf,
        ownership: PluginOwnershipWrite,
    },
}
impl PreparedInstallSource {
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
    pub fn cleanup_failure(&self) {
        match self {
            Self::Uploaded(source) | Self::OwnedUpload(source, _) => {
                let _ = std::fs::remove_dir_all(&source.cleanup_root);
            }
            Self::Package { cleanup_root, .. } => {
                let _ = std::fs::remove_dir_all(cleanup_root);
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
                    request_context.connection_id(),
                    workspace,
                    &upload_id,
                    context,
                    request_id,
                )
                .await
                .map(PreparedInstallSource::Uploaded)
            }
            SkillInstallSource::OwnedUpload(source, native) => {
                let upload_id = parse_lifecycle_upload_id(source).map_err(|_| invalid())?;
                let upload = self
                    .materialize_uploaded_skill_source(
                        request_context.connection_id(),
                        workspace,
                        &upload_id,
                        context,
                        request_id,
                    )
                    .await?;
                Ok(PreparedInstallSource::OwnedUpload(upload, native))
            }
            SkillInstallSource::PackageMember(ownership) => {
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
                let source_dir = pioneer_plugins::containment::resolve_contained(
                    Path::new(&parent.package_path),
                    &Path::new(&parent.package_path).join(path),
                )
                .map_err(|_| invalid())?;
                let location =
                    install_location_for_source_kind(context, &source_kind).ok_or_else(invalid)?;
                let staged = pioneer_skills::stage_skill_folder(
                    &source_dir,
                    &location.install_root,
                    &installer_policy(context),
                    true,
                )
                .map_err(|_| invalid())?;
                let verified = pioneer_plugins::Snapshot::capture(
                    &staged.source_dir,
                    Default::default(),
                    || false,
                )
                .is_ok_and(|copy| copy.tree_digest() == ownership.package_fingerprint);
                if !verified {
                    let _ = std::fs::remove_dir_all(&staged.cleanup_root);
                    return Err(invalid());
                }
                Ok(PreparedInstallSource::Package {
                    source_dir: staged.source_dir,
                    cleanup_root: staged.cleanup_root,
                    ownership,
                })
            }
        }
    }
}

impl Drop for PreparedInstallSource {
    fn drop(&mut self) {
        self.cleanup_failure();
    }
}
