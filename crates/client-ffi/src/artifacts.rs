//! Secret-preserving native artifact actions for first-party mobile shells.

use pioneer_client::artifacts::{
    access::ArtifactAccessError,
    http_download::ArtifactHttpDownloadResult,
    operations::{ArtifactDownloadIdentity, ArtifactDownloadOperationError, ArtifactDownloadState},
    workflow::ArtifactActionIdentity,
};
use pioneer_client::core::ClientCore;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};
use zeroize::Zeroize;

use crate::ClientFfiError;

pub(crate) const INVALID_ARTIFACT_ACTION_CODE: &str = "invalid_artifact_action";
pub(crate) const ARTIFACT_RECONFIGURATION_CODE: &str = "artifact_reconfiguration_required";

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientArtifactTargetRequest {
    pub identity: ArtifactActionIdentity,
    pub workspace_id: String,
    pub artifact_id: String,
    #[serde(default)]
    pub version_id: Option<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Serialize, PartialEq, Eq)]
pub struct ClientArtifactViewOpenResult {
    pub view_url: String,
    pub expires_at: u64,
}

impl std::fmt::Debug for ClientArtifactViewOpenResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientArtifactViewOpenResult")
            .field("view_url", &"[redacted]")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl Drop for ClientArtifactViewOpenResult {
    fn drop(&mut self) {
        self.view_url.zeroize();
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientArtifactDownloadRequest {
    pub identity: ArtifactActionIdentity,
    pub operation_id: String,
    #[serde(default)]
    pub thread_id: Option<String>,
    pub workspace_id: String,
    pub artifact_id: String,
    #[serde(default)]
    pub version_id: Option<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientArtifactDownloadOperationRequest {
    pub operation_id: String,
    pub generation: u64,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientArtifactDownloadResult {
    pub operation_id: String,
    pub local_file_path: String,
    pub display_name: String,
    pub artifact_id: String,
    pub version_id: String,
    pub size_bytes: u64,
    pub sha256: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClientArtifactDownloadState {
    Queued,
    Downloading,
    Completed,
    Failed,
    Cancelled,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientArtifactDownloadProgressResult {
    pub operation_id: String,
    pub generation: u64,
    pub state: ClientArtifactDownloadState,
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub resumed_from_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientArtifactDownloadCancelResult {
    pub operation_id: String,
    pub cancelled: bool,
}

pub(crate) fn download_progress(
    core: &ClientCore,
    request: ClientArtifactDownloadOperationRequest,
) -> Result<ClientArtifactDownloadProgressResult, ClientFfiError> {
    let input = core
        .artifact_download_snapshot(&request.operation_id)
        .ok_or_else(|| map_operation_error(ArtifactDownloadOperationError::NotFound))?;
    if input.identity.generation != request.generation {
        return Err(map_operation_error(
            ArtifactDownloadOperationError::NotFound,
        ));
    }
    Ok(ClientArtifactDownloadProgressResult {
        operation_id: request.operation_id,
        generation: input.identity.generation,
        state: match input.state {
            ArtifactDownloadState::Queued => ClientArtifactDownloadState::Queued,
            ArtifactDownloadState::Downloading => ClientArtifactDownloadState::Downloading,
            ArtifactDownloadState::Completed => ClientArtifactDownloadState::Completed,
            ArtifactDownloadState::Failed => ClientArtifactDownloadState::Failed,
            ArtifactDownloadState::Cancelled => ClientArtifactDownloadState::Cancelled,
        },
        downloaded_bytes: input.downloaded_bytes,
        total_bytes: input.total_bytes,
        resumed_from_bytes: input.resumed_from_bytes,
        error_code: input.error_code.clone(),
    })
}

pub(crate) fn cancel_download(
    core: &ClientCore,
    request: ClientArtifactDownloadOperationRequest,
) -> Result<ClientArtifactDownloadCancelResult, ClientFfiError> {
    let input = core
        .artifact_download_snapshot(&request.operation_id)
        .ok_or_else(|| map_operation_error(ArtifactDownloadOperationError::NotFound))?;
    Ok(ClientArtifactDownloadCancelResult {
        operation_id: request.operation_id,
        cancelled: core.cancel_artifact_download(&ArtifactDownloadIdentity {
            operation_id: input.identity.operation_id.clone(),
            generation: request.generation,
        }),
    })
}

fn map_operation_error(error: ArtifactDownloadOperationError) -> ClientFfiError {
    ClientFfiError::new(
        "artifact download operation unavailable",
        if error == ArtifactDownloadOperationError::Capacity {
            "artifact_download_capacity"
        } else {
            INVALID_ARTIFACT_ACTION_CODE
        },
    )
}

pub(crate) fn open_artifact_view(
    core: &ClientCore,
    request: ClientArtifactTargetRequest,
) -> Result<ClientArtifactViewOpenResult, ClientFfiError> {
    validate_target(
        core,
        &request.identity,
        &request.workspace_id,
        &request.artifact_id,
        request.version_id.as_deref(),
    )?;
    let plan = core
        .prepare_artifact_action_view(&request.identity, None)
        .map_err(map_access_error)?;
    Ok(ClientArtifactViewOpenResult {
        view_url: plan.url.expose_url().to_owned(),
        expires_at: plan.expires_at,
    })
}

pub(crate) fn download_artifact(
    core: &Arc<ClientCore>,
    runtime_home: PathBuf,
    request: ClientArtifactDownloadRequest,
) -> Result<ClientArtifactDownloadResult, ClientFfiError> {
    validate_target(
        core,
        &request.identity,
        &request.workspace_id,
        &request.artifact_id,
        request.version_id.as_deref(),
    )?;
    if request.thread_id.as_deref() != Some(request.identity.thread_id.as_str()) {
        return Err(map_operation_error(
            ArtifactDownloadOperationError::InvalidIdentity,
        ));
    }
    let (display_name, result) = core
        .download_artifact_action(
            &request.identity,
            request.operation_id.clone(),
            runtime_home,
        )
        .map_err(map_access_error)?;
    verified_result(request.operation_id, display_name, result)
}

fn validate_target(
    core: &ClientCore,
    identity: &ArtifactActionIdentity,
    workspace: &str,
    artifact: &str,
    version: Option<&str>,
) -> Result<(), ClientFfiError> {
    let action = core
        .artifact_action_snapshot(identity)
        .ok_or_else(|| map_operation_error(ArtifactDownloadOperationError::NotFound))?;
    if action.target.workspace_id != workspace
        || action.target.artifact_id != artifact
        || action.target.version_id.as_deref() != version
    {
        return Err(map_operation_error(
            ArtifactDownloadOperationError::InvalidIdentity,
        ));
    }
    Ok(())
}

fn map_access_error(error: ArtifactAccessError) -> ClientFfiError {
    ClientFfiError::new(error.message(), error.code())
}

fn verified_result(
    operation_id: String,
    display_name: String,
    result: ArtifactHttpDownloadResult,
) -> Result<ClientArtifactDownloadResult, ClientFfiError> {
    let local_file_path = result
        .local_path
        .as_path()
        .to_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ClientFfiError::new(
                "verified artifact path cannot be represented for the native shell",
                "artifact_local_path_invalid",
            )
        })?
        .to_owned();
    Ok(ClientArtifactDownloadResult {
        operation_id,
        local_file_path,
        display_name,
        artifact_id: result.artifact_id,
        version_id: result.version_id,
        size_bytes: result.size_bytes,
        sha256: result.sha256,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::artifacts::{
        http_download::ArtifactHttpDownloadProgress, operations::ArtifactDownloadTarget,
    };

    #[test]
    fn ffi_progress_and_cancellation_read_the_direct_client_download_owner() {
        let runtime = crate::ClientFfiRuntime::default();
        runtime.initialize(r#"{"platform":"ios"}"#).unwrap();
        let core = &runtime.client_runtime.core;
        let operation = core
            .begin_artifact_download(
                "synthetic".into(),
                ArtifactDownloadTarget {
                    thread_id: Some("thread".into()),
                    workspace_id: "workspace".into(),
                    artifact_id: "artifact".into(),
                    version_id: Some("version".into()),
                },
            )
            .unwrap();
        operation.update_progress(ArtifactHttpDownloadProgress {
            downloaded_bytes: 7,
            total_bytes: 10,
            resumed_from_bytes: 3,
        });
        let input = serde_json::json!({"operation_id": "synthetic", "generation": operation.identity().generation}).to_string();
        let wire = runtime.artifact_download_progress(&input).unwrap();
        let direct = core.artifact_download_snapshot("synthetic").unwrap();
        assert_eq!(wire.state, ClientArtifactDownloadState::Downloading);
        assert_eq!(wire.downloaded_bytes, direct.downloaded_bytes);
        assert_eq!(wire.resumed_from_bytes, direct.resumed_from_bytes);
        assert!(runtime.artifact_download_cancel(&input).unwrap().cancelled);
        assert!(!runtime.artifact_download_cancel(&input).unwrap().cancelled);
        assert!(operation.cancellation().is_cancelled());
        assert!(!operation.finish(ArtifactDownloadState::Completed, None));
        assert_eq!(
            runtime.artifact_download_progress(&input).unwrap().state,
            ClientArtifactDownloadState::Cancelled
        );
    }

    #[test]
    fn ffi_cancel_replay_cannot_cancel_a_reused_operation_id() {
        let runtime = crate::ClientFfiRuntime::default();
        runtime.initialize(r#"{"platform":"ios"}"#).unwrap();
        let core = &runtime.client_runtime.core;
        let target = ArtifactDownloadTarget {
            thread_id: Some("thread".into()),
            workspace_id: "workspace".into(),
            artifact_id: "artifact".into(),
            version_id: None,
        };
        let old = core
            .begin_artifact_download("operation".into(), target.clone())
            .unwrap();
        let request = serde_json::json!({"operation_id":"operation", "generation": old.identity().generation}).to_string();
        assert!(
            runtime
                .artifact_download_cancel(&request)
                .unwrap()
                .cancelled
        );
        let next = core
            .begin_artifact_download("operation".into(), target)
            .unwrap();
        assert!(
            !runtime
                .artifact_download_cancel(&request)
                .unwrap()
                .cancelled
        );
        assert!(runtime.artifact_download_progress(&request).is_err());
        assert!(!next.cancellation().is_cancelled());
        assert!(!old.finish(ArtifactDownloadState::Completed, None));
    }

    #[test]
    fn bridge_dtos_contain_no_access_or_authorization_fields() {
        let source = include_str!("artifacts.rs");
        assert!(!source.contains(&["pub access", "_token"].concat()));
        assert!(!source.contains(&["pub authorization", "_header"].concat()));
        assert!(!source.contains(&["refresh", "_token"].concat()));
    }

    #[test]
    fn browser_view_result_redacts_the_opaque_grant_from_debug() {
        let secret = "a".repeat(43);
        let result = ClientArtifactViewOpenResult {
            view_url: format!("https://gateway.example/storage/views/{secret}"),
            expires_at: 1_800_000_000,
        };
        let rendered = format!("{result:?}");
        assert!(!rendered.contains(secret.as_str()));
        assert!(rendered.contains("[redacted]"));
    }
}
