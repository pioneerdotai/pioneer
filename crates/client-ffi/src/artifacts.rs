//! Secret-preserving native artifact actions for first-party mobile shells.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use pioneer_client::{
    artifacts::http_download::{
        ArtifactHttpDownloadError, ArtifactHttpDownloadProgress,
        ArtifactHttpDownloadRequest, ArtifactHttpDownloadResult, ArtifactHttpDownloadService,
    },
    gateway::types::{GatewayEndpoint, GatewayEndpointKind},
    transport::{
        http::{
            BrowserViewUrl, GatewayHttpAccess, GatewayHttpAuthorityError, GatewayHttpError,
            GatewayHttpSession, GatewayHttpSessionAuthority,
        },
        ws::GatewayWsCommandSender,
    },
};
use pioneer_protocol::{
    ArtifactGetParams, ArtifactSummary, ArtifactViewGrantCreateParams,
    ArtifactViewGrantDisposition,
};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

use crate::ClientFfiError;

pub(crate) const INVALID_ARTIFACT_ACTION_CODE: &str = "invalid_artifact_action";
pub(crate) const ARTIFACT_AUTHENTICATION_CODE: &str = "artifact_authentication_required";
pub(crate) const ARTIFACT_RECONFIGURATION_CODE: &str = "artifact_reconfiguration_required";

const OPERATION_QUEUED: u8 = 0;
const OPERATION_DOWNLOADING: u8 = 1;
const OPERATION_COMPLETED: u8 = 2;
const OPERATION_FAILED: u8 = 3;
const OPERATION_CANCELLED: u8 = 4;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientArtifactTargetRequest {
    pub workspace_id: String,
    pub artifact_id: String,
    #[serde(default)]
    pub version_id: Option<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientArtifactViewOpenResult {
    pub view_url: String,
    pub expires_at: u64,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientArtifactDownloadRequest {
    pub operation_id: String,
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

#[derive(Default)]
pub(crate) struct ClientFfiArtifactDownloads {
    operations: Mutex<HashMap<String, Arc<ArtifactDownloadOperation>>>,
}

struct ArtifactDownloadOperation {
    cancellation: CancellationToken,
    state: AtomicU8,
    downloaded_bytes: AtomicU64,
    total_bytes: AtomicU64,
    resumed_from_bytes: AtomicU64,
    error_code: Mutex<Option<String>>,
}

impl ArtifactDownloadOperation {
    fn new() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            state: AtomicU8::new(OPERATION_QUEUED),
            downloaded_bytes: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            resumed_from_bytes: AtomicU64::new(0),
            error_code: Mutex::new(None),
        }
    }

    fn update_progress(&self, progress: ArtifactHttpDownloadProgress) {
        self.downloaded_bytes
            .store(progress.downloaded_bytes, Ordering::Relaxed);
        self.total_bytes
            .store(progress.total_bytes, Ordering::Relaxed);
        self.resumed_from_bytes
            .store(progress.resumed_from_bytes, Ordering::Relaxed);
        self.state.store(OPERATION_DOWNLOADING, Ordering::Release);
    }

    fn finish(&self, state: u8, error_code: Option<String>) {
        if let Ok(mut stored) = self.error_code.lock() {
            *stored = error_code;
        }
        self.state.store(state, Ordering::Release);
    }

    fn snapshot(&self, operation_id: String) -> ClientArtifactDownloadProgressResult {
        let state = match self.state.load(Ordering::Acquire) {
            OPERATION_QUEUED => ClientArtifactDownloadState::Queued,
            OPERATION_DOWNLOADING => ClientArtifactDownloadState::Downloading,
            OPERATION_COMPLETED => ClientArtifactDownloadState::Completed,
            OPERATION_CANCELLED => ClientArtifactDownloadState::Cancelled,
            _ => ClientArtifactDownloadState::Failed,
        };
        ClientArtifactDownloadProgressResult {
            operation_id,
            state,
            downloaded_bytes: self.downloaded_bytes.load(Ordering::Relaxed),
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            resumed_from_bytes: self.resumed_from_bytes.load(Ordering::Relaxed),
            error_code: self.error_code.lock().ok().and_then(|value| value.clone()),
        }
    }
}

impl ClientFfiArtifactDownloads {
    fn begin(
        &self,
        operation_id: &str,
    ) -> Result<Arc<ArtifactDownloadOperation>, ClientFfiError> {
        validate_operation_id(operation_id)?;
        let operation = Arc::new(ArtifactDownloadOperation::new());
        let mut operations = self.operations.lock().map_err(|_| {
            ClientFfiError::new(
                "artifact download operation lock is poisoned",
                ClientFfiError::GENERIC_CODE,
            )
        })?;
        if operations
            .get(operation_id)
            .is_some_and(|existing| {
                matches!(
                    existing.state.load(Ordering::Acquire),
                    OPERATION_QUEUED | OPERATION_DOWNLOADING
                )
            })
        {
            return Err(ClientFfiError::new(
                "artifact download operation is already active",
                INVALID_ARTIFACT_ACTION_CODE,
            ));
        }
        operations.insert(operation_id.to_owned(), operation.clone());
        Ok(operation)
    }

    pub(crate) fn progress(
        &self,
        request: ClientArtifactDownloadOperationRequest,
    ) -> Result<ClientArtifactDownloadProgressResult, ClientFfiError> {
        validate_operation_id(request.operation_id.as_str())?;
        let operations = self.operations.lock().map_err(|_| {
            ClientFfiError::new(
                "artifact download operation lock is poisoned",
                ClientFfiError::GENERIC_CODE,
            )
        })?;
        let operation = operations.get(request.operation_id.as_str()).ok_or_else(|| {
            ClientFfiError::new(
                "artifact download operation was not found",
                INVALID_ARTIFACT_ACTION_CODE,
            )
        })?;
        Ok(operation.snapshot(request.operation_id))
    }

    pub(crate) fn cancel(
        &self,
        request: ClientArtifactDownloadOperationRequest,
    ) -> Result<ClientArtifactDownloadCancelResult, ClientFfiError> {
        validate_operation_id(request.operation_id.as_str())?;
        let operations = self.operations.lock().map_err(|_| {
            ClientFfiError::new(
                "artifact download operation lock is poisoned",
                ClientFfiError::GENERIC_CODE,
            )
        })?;
        let operation = operations.get(request.operation_id.as_str()).ok_or_else(|| {
            ClientFfiError::new(
                "artifact download operation was not found",
                INVALID_ARTIFACT_ACTION_CODE,
            )
        })?;
        let active = matches!(
            operation.state.load(Ordering::Acquire),
            OPERATION_QUEUED | OPERATION_DOWNLOADING
        );
        if active {
            operation.cancellation.cancel();
            operation.finish(OPERATION_CANCELLED, Some("cancelled".to_owned()));
        }
        Ok(ClientArtifactDownloadCancelResult {
            operation_id: request.operation_id,
            cancelled: active,
        })
    }

    pub(crate) fn cancel_all(&self) {
        if let Ok(operations) = self.operations.lock() {
            for operation in operations.values() {
                if matches!(
                    operation.state.load(Ordering::Acquire),
                    OPERATION_QUEUED | OPERATION_DOWNLOADING
                ) {
                    operation.cancellation.cancel();
                    operation.finish(OPERATION_CANCELLED, Some("cancelled".to_owned()));
                }
            }
        }
    }
}

pub(crate) fn open_artifact_view(
    sender: &GatewayWsCommandSender,
    request: ClientArtifactTargetRequest,
) -> Result<ClientArtifactViewOpenResult, ClientFfiError> {
    let artifact = resolve_artifact(sender, request)?;
    let version_id = exact_version(&artifact)?;
    let grant = sender
        .artifact_view_grant_create(ArtifactViewGrantCreateParams {
            workspace_id: artifact.workspace_id.clone(),
            artifact_id: artifact.artifact.artifact_id.clone(),
            version_id,
            projection_kind: None,
            disposition: ArtifactViewGrantDisposition::Inline,
        })
        .map_err(map_rpc_error)?;
    let access = sender
        .current_gateway_http_access()
        .map_err(map_authority_error)?;
    let view = BrowserViewUrl::resolve(&access.gateway_base_url, grant.relative_url.as_str())
        .map_err(map_http_error)?;
    Ok(ClientArtifactViewOpenResult {
        view_url: view.expose_url().to_owned(),
        expires_at: grant.expires_at,
    })
}

pub(crate) fn download_artifact(
    sender: &GatewayWsCommandSender,
    downloads: &ClientFfiArtifactDownloads,
    runtime_home: PathBuf,
    request: ClientArtifactDownloadRequest,
) -> Result<ClientArtifactDownloadResult, ClientFfiError> {
    let operation = downloads.begin(request.operation_id.as_str())?;
    let result = download_artifact_inner(sender, runtime_home, &request, operation.as_ref());
    match result {
        Ok(result) => {
            operation.finish(OPERATION_COMPLETED, None);
            Ok(result)
        }
        Err(error) => {
            let state = if operation.cancellation.is_cancelled() {
                OPERATION_CANCELLED
            } else {
                OPERATION_FAILED
            };
            operation.finish(state, Some(error.code.clone()));
            Err(error)
        }
    }
}

fn download_artifact_inner(
    sender: &GatewayWsCommandSender,
    runtime_home: PathBuf,
    request: &ClientArtifactDownloadRequest,
    operation: &ArtifactDownloadOperation,
) -> Result<ClientArtifactDownloadResult, ClientFfiError> {
    let artifact = resolve_artifact(
        sender,
        ClientArtifactTargetRequest {
            workspace_id: request.workspace_id.clone(),
            artifact_id: request.artifact_id.clone(),
            version_id: request.version_id.clone(),
        },
    )?;
    let version_id = exact_version(&artifact)?;
    let size_bytes = artifact.artifact.size_bytes.ok_or_else(invalid_artifact)?;
    let sha256 = artifact
        .artifact
        .sha256
        .clone()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(invalid_artifact)?;
    let access = sender
        .current_gateway_http_access()
        .map_err(map_authority_error)?;
    let endpoint = GatewayEndpoint {
        id: access.gateway_id.as_str().to_owned(),
        name: "active Gateway".to_owned(),
        gateway_base_url: access.gateway_base_url.clone(),
        kind: GatewayEndpointKind::Remote,
        session_ref: Some(access.session_id.as_str().to_owned()),
        server_gateway_id: Some(access.gateway_id.clone()),
        workspace_id: Some(artifact.workspace_id.clone()),
        service_name: None,
    };
    let authority = Arc::new(FfiGatewayHttpAuthority {
        sender: sender.clone(),
    });
    let session = GatewayHttpSession::from_endpoint(&endpoint, access.session_id, authority)
        .map_err(map_http_error)?;
    let service = ArtifactHttpDownloadService::new(session, runtime_home);
    let native_request = ArtifactHttpDownloadRequest {
        gateway_profile_id: endpoint.id,
        workspace_id: artifact.workspace_id,
        artifact_id: artifact.artifact.artifact_id.clone(),
        version_id: version_id.clone(),
        display_name: artifact.artifact.display_name.clone(),
        expected_size_bytes: size_bytes,
        expected_sha256: sha256,
    };
    let runtime = Runtime::new().map_err(|_| {
        ClientFfiError::new(
            "artifact download runtime is unavailable",
            "artifact_download_unavailable",
        )
    })?;
    let progress = |progress: ArtifactHttpDownloadProgress| operation.update_progress(progress);
    let result = runtime
        .block_on(service.download(
            native_request,
            operation.cancellation.clone(),
            Some(&progress),
        ))
        .map_err(map_download_error)?;
    if operation.cancellation.is_cancelled() {
        return Err(map_download_error(ArtifactHttpDownloadError::Cancelled));
    }
    verified_result(request.operation_id.clone(), artifact.artifact.display_name, result)
}

fn resolve_artifact(
    sender: &GatewayWsCommandSender,
    request: ClientArtifactTargetRequest,
) -> Result<ArtifactSummary, ClientFfiError> {
    let workspace_id = non_empty(request.workspace_id)?;
    let artifact_id = non_empty(request.artifact_id)?;
    let version_id = request.version_id.and_then(|value| {
        let value = value.trim().to_owned();
        (!value.is_empty()).then_some(value)
    });
    sender
        .artifact_get(ArtifactGetParams {
            workspace_id,
            artifact_id,
            version_id,
        })
        .map(|response| response.artifact)
        .map_err(map_rpc_error)
}

fn exact_version(artifact: &ArtifactSummary) -> Result<String, ClientFfiError> {
    artifact
        .artifact
        .version_id
        .clone()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(invalid_artifact)
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

struct FfiGatewayHttpAuthority {
    sender: GatewayWsCommandSender,
}

#[async_trait]
impl GatewayHttpSessionAuthority for FfiGatewayHttpAuthority {
    async fn current_access(&self) -> Result<GatewayHttpAccess, GatewayHttpAuthorityError> {
        self.sender.current_gateway_http_access()
    }

    async fn coordinated_refresh(
        &self,
        rejected_generation: u64,
    ) -> Result<GatewayHttpAccess, GatewayHttpAuthorityError> {
        let current = self.sender.current_gateway_http_access()?;
        if current.generation != rejected_generation {
            Ok(current)
        } else {
            // Mobile refresh credentials remain owned by the existing session
            // coordinator. Native artifact I/O never starts a parallel refresh.
            Err(GatewayHttpAuthorityError::TemporarilyUnavailable)
        }
    }
}

fn validate_operation_id(operation_id: &str) -> Result<(), ClientFfiError> {
    if operation_id.is_empty()
        || operation_id.len() > 128
        || !operation_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(ClientFfiError::new(
            "invalid artifact download operation id",
            INVALID_ARTIFACT_ACTION_CODE,
        ));
    }
    Ok(())
}

fn non_empty(value: String) -> Result<String, ClientFfiError> {
    let value = value.trim().to_owned();
    if value.is_empty() || value.len() > 512 {
        return Err(invalid_artifact());
    }
    Ok(value)
}

fn invalid_artifact() -> ClientFfiError {
    ClientFfiError::new(
        "artifact identity or immutable metadata is incomplete",
        INVALID_ARTIFACT_ACTION_CODE,
    )
}

fn map_authority_error(error: GatewayHttpAuthorityError) -> ClientFfiError {
    match error {
        GatewayHttpAuthorityError::Terminal(_) => ClientFfiError::new(
            "Gateway session must be authenticated again",
            ARTIFACT_AUTHENTICATION_CODE,
        ),
        GatewayHttpAuthorityError::TemporarilyUnavailable => ClientFfiError::new(
            "Gateway session is unavailable for artifact access",
            ARTIFACT_AUTHENTICATION_CODE,
        ),
    }
}

fn map_rpc_error(error: anyhow::Error) -> ClientFfiError {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    let code = if lower.contains("unauthorized") || lower.contains("authentication") {
        ARTIFACT_AUTHENTICATION_CODE
    } else if lower.contains("forbidden") || lower.contains("not found") {
        "artifact_revoked_or_unavailable"
    } else {
        "artifact_action_failed"
    };
    ClientFfiError::new("artifact action failed", code)
}

fn map_http_error(error: GatewayHttpError) -> ClientFfiError {
    let code = match error {
        GatewayHttpError::InvalidEndpoint
        | GatewayHttpError::GatewayPinMismatch
        | GatewayHttpError::SessionMismatch => ARTIFACT_RECONFIGURATION_CODE,
        GatewayHttpError::AuthenticationTerminal(_)
        | GatewayHttpError::AuthenticationUnavailable
        | GatewayHttpError::Unauthorized => ARTIFACT_AUTHENTICATION_CODE,
        GatewayHttpError::Forbidden | GatewayHttpError::NotFound => {
            "artifact_revoked_or_unavailable"
        }
        _ => "artifact_action_failed",
    };
    ClientFfiError::new("artifact HTTP action failed", code)
}

fn map_download_error(error: ArtifactHttpDownloadError) -> ClientFfiError {
    let code = match error {
        ArtifactHttpDownloadError::Authentication => ARTIFACT_AUTHENTICATION_CODE,
        ArtifactHttpDownloadError::RevokedOrUnavailable => "artifact_revoked_or_unavailable",
        other => other.code(),
    };
    ClientFfiError::new("artifact download failed", code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_and_cancel_contract_is_secret_free_and_deterministic() {
        let downloads = ClientFfiArtifactDownloads::default();
        let operation = downloads.begin("mobile-download-1").expect("begin");
        operation.update_progress(ArtifactHttpDownloadProgress {
            downloaded_bytes: 7,
            total_bytes: 10,
            resumed_from_bytes: 3,
        });
        let progress = downloads
            .progress(ClientArtifactDownloadOperationRequest {
                operation_id: "mobile-download-1".to_owned(),
            })
            .expect("progress");
        assert_eq!(progress.state, ClientArtifactDownloadState::Downloading);
        assert_eq!(progress.downloaded_bytes, 7);
        assert_eq!(progress.resumed_from_bytes, 3);

        let cancelled = downloads
            .cancel(ClientArtifactDownloadOperationRequest {
                operation_id: "mobile-download-1".to_owned(),
            })
            .expect("cancel");
        assert!(cancelled.cancelled);
        assert!(operation.cancellation.is_cancelled());
        assert_eq!(
            downloads
                .progress(ClientArtifactDownloadOperationRequest {
                    operation_id: "mobile-download-1".to_owned(),
                })
                .expect("cancelled progress")
                .state,
            ClientArtifactDownloadState::Cancelled
        );
    }

    #[test]
    fn bridge_dtos_contain_no_access_or_authorization_fields() {
        let source = include_str!("artifacts.rs");
        assert!(!source.contains(&["pub access", "_token"].concat()));
        assert!(!source.contains(&["pub authorization", "_header"].concat()));
        assert!(!source.contains(&["refresh", "_token"].concat()));
    }
}
