//! Explicit downloaded-file retention and reveal workflow. Transfer integrity,
//! resume and destination publication reuse the established download helpers.
use super::{
    actions::{
        ArtifactLocalFile, copy_http_download_result_to_destination,
        existing_local_file_is_verified, plan_artifact_http_download_request,
    },
    http_download::ArtifactHttpDownloadService,
    operations::ArtifactDownloadState,
    workflow::{ArtifactActionIdentity, ArtifactActionKind, ArtifactActionState},
};
use crate::{
    core::{ClientCore, ClientScope},
    platform::ClientPath,
};
use pioneer_protocol::ArtifactRef;
use std::{path::PathBuf, sync::Arc};

#[derive(Clone, Debug)]
pub struct ArtifactLocalPresentationPlan {
    identity: ArtifactActionIdentity,
    artifact: ArtifactRef,
    file: ArtifactLocalFile,
}
impl ArtifactLocalPresentationPlan {
    pub fn identity(&self) -> &ArtifactActionIdentity {
        &self.identity
    }
    pub fn artifact(&self) -> &ArtifactRef {
        &self.artifact
    }
    pub fn file(&self) -> &ArtifactLocalFile {
        &self.file
    }
}
impl ClientCore {
    pub fn download_artifact_to_folder(
        self: &Arc<Self>,
        identity: &ArtifactActionIdentity,
        destination: ClientPath,
        runtime_home: PathBuf,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.start_artifact_preparation(identity, ArtifactActionKind::Download),
            "artifact operation cancelled"
        );
        let result = self.download_artifact_to_folder_inner(identity, destination, runtime_home);
        if let Err(error) = &result {
            let code = error
                .downcast_ref::<super::http_download::ArtifactHttpDownloadError>()
                .map_or("download_failed", |error| error.code());
            self.fail_artifact_preparation(identity, code.into());
        }
        result
    }
    fn download_artifact_to_folder_inner(
        self: &Arc<Self>,
        identity: &ArtifactActionIdentity,
        destination: ClientPath,
        runtime_home: PathBuf,
    ) -> anyhow::Result<()> {
        let action = self
            .artifact_action_snapshot(identity)
            .ok_or_else(|| anyhow::anyhow!("artifact operation cancelled"))?;
        let summary = self
            .artifact_snapshot(&identity.thread_id)
            .and_then(|input| {
                input
                    .items
                    .iter()
                    .find(|summary| {
                        summary.artifact.artifact_id == identity.artifact_id
                            && summary.artifact.version_id == identity.version_id
                            && summary.workspace_id == action.target.workspace_id
                    })
                    .cloned()
            })
            .ok_or_else(|| anyhow::anyhow!("artifact metadata unavailable"))?;
        let profile = self.snapshot(&ClientScope::Administration { workspace_id: None }).and_then(|p| p.snapshot().payload::<crate::gateway::identity_authorization::IdentityAuthorizationPublication>()).and_then(|identity| identity.endpoint_id.clone());
        let request = plan_artifact_http_download_request(profile, &summary)
            .map_err(|_| anyhow::anyhow!("artifact metadata unavailable"))?;
        let operation = self
            .begin_artifact_download_for_target(action.target.clone())
            .map_err(|_| anyhow::anyhow!("artifact download unavailable"))?;
        anyhow::ensure!(
            self.attach_artifact_download(identity, operation.identity()),
            "artifact operation cancelled"
        );
        let cancellation = operation.cancellation();
        let result = (|| {
            let session = super::access::artifact_http_session(
                &self.compatibility_runtime().ws_command_sender(),
            )?;
            let service = ArtifactHttpDownloadService::new(session, runtime_home);
            let progress = |progress| operation.update_progress(progress);
            let result = tokio::runtime::Runtime::new()?.block_on(service.download(
                request,
                cancellation.clone(),
                Some(&progress),
            ))?;
            anyhow::ensure!(
                !cancellation.is_cancelled() && self.artifact_action_matches(identity),
                "artifact operation cancelled"
            );
            anyhow::ensure!(
                self.begin_artifact_local_verification(identity),
                "artifact operation cancelled"
            );
            let local_file = copy_http_download_result_to_destination(
                &result,
                &summary.artifact.display_name,
                destination.as_path(),
            )?;
            anyhow::ensure!(!cancellation.is_cancelled(), "artifact operation cancelled");
            anyhow::ensure!(
                self.complete_artifact_local_download(identity, local_file),
                "artifact operation cancelled"
            );
            Ok(())
        })();
        operation.finish(
            if result.is_ok() {
                ArtifactDownloadState::Completed
            } else {
                ArtifactDownloadState::Failed
            },
            result.as_ref().err().map(|_| "download_failed".into()),
        );
        result
    }
    pub fn prepare_artifact_reveal(
        &self,
        identity: &ArtifactActionIdentity,
    ) -> anyhow::Result<ArtifactLocalPresentationPlan> {
        anyhow::ensure!(
            self.start_artifact_preparation(identity, ArtifactActionKind::Reveal),
            "artifact operation cancelled"
        );
        let result = (|| {
            let action = self
                .artifact_action_snapshot(identity)
                .ok_or_else(|| anyhow::anyhow!("artifact operation cancelled"))?;
            let file = action
                .local_file
                .ok_or_else(|| anyhow::anyhow!("local artifact unavailable"))?;
            let artifact = self
                .artifact_snapshot(&identity.thread_id)
                .and_then(|input| {
                    input
                        .items
                        .iter()
                        .find(|summary| {
                            summary.artifact.artifact_id == identity.artifact_id
                                && summary.artifact.version_id == identity.version_id
                        })
                        .map(|summary| summary.artifact.clone())
                })
                .ok_or_else(|| anyhow::anyhow!("artifact metadata unavailable"))?;
            anyhow::ensure!(
                existing_local_file_is_verified(&file, &artifact)?,
                "local artifact integrity failed"
            );
            anyhow::ensure!(
                self.prepare_artifact_presentation(identity, None),
                "artifact operation cancelled"
            );
            Ok(ArtifactLocalPresentationPlan {
                identity: identity.clone(),
                artifact,
                file,
            })
        })();
        if result.is_err() {
            self.change_artifact_action(identity, |action| {
                if action.state != ArtifactActionState::Resolving {
                    return false;
                }
                action.local_file = None;
                action.state = ArtifactActionState::Failed {
                    code: "local_copy_invalid".into(),
                };
                true
            });
        }
        result
    }
    fn artifact_action_matches(&self, identity: &ArtifactActionIdentity) -> bool {
        let mut matches = false;
        self.change_artifact_action(identity, |action| {
            matches = action.state == ArtifactActionState::Resolving;
            false
        });
        matches
    }
    pub(super) fn begin_artifact_local_verification(
        &self,
        identity: &ArtifactActionIdentity,
    ) -> bool {
        self.change_artifact_action(identity, |action| {
            if action.action != ArtifactActionKind::Download
                || action.state != ArtifactActionState::Resolving
            {
                return false;
            }
            action.state = ArtifactActionState::Verifying;
            true
        })
    }
    pub(super) fn complete_artifact_local_download(
        &self,
        identity: &ArtifactActionIdentity,
        file: ArtifactLocalFile,
    ) -> bool {
        self.change_artifact_action(identity, |action| {
            if action.action != ArtifactActionKind::Download
                || action.state != ArtifactActionState::Verifying
            {
                return false;
            }
            action.local_file = Some(file);
            action.state = ArtifactActionState::Completed;
            true
        })
    }
}
