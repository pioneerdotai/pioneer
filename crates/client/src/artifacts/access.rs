//! Shared artifact metadata, view-grant and authenticated transfer workflow.
//! Native shells supply their existing storage location and present the result.

use super::{
    http_download::{
        ArtifactHttpDownloadError, ArtifactHttpDownloadResult, ArtifactHttpDownloadService,
    },
    operations::{ArtifactDownloadOperation, ArtifactDownloadTarget},
};
use crate::transport::http_authority::GatewayWsHttpAuthority;
use crate::transport::{
    http::{BrowserViewUrl, GatewayHttpAuthorityError, GatewayHttpError, GatewayHttpSession},
    ws::GatewayWsCommandSender,
};
use pioneer_protocol::{
    ArtifactGetParams, ArtifactSummary, ArtifactViewGrantCreateParams, ArtifactViewGrantDisposition,
};
use std::{path::PathBuf, sync::Arc};

const INVALID_ARTIFACT_ACTION_CODE: &str = "invalid_artifact_action";
const ARTIFACT_AUTHENTICATION_CODE: &str = "artifact_authentication_required";
const ARTIFACT_RECONFIGURATION_CODE: &str = "artifact_reconfiguration_required";

#[derive(Clone, Debug)]
pub struct ArtifactAccessError {
    message: String,
    code: String,
}
impl ArtifactAccessError {
    fn new(message: &str, code: &str) -> Self {
        Self {
            message: message.into(),
            code: code.into(),
        }
    }
    pub fn code(&self) -> &str {
        &self.code
    }
    pub fn message(&self) -> &str {
        &self.message
    }
}
impl std::fmt::Display for ArtifactAccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for ArtifactAccessError {}

#[derive(Debug)]
pub struct ArtifactViewPlan {
    pub url: BrowserViewUrl,
    pub expires_at: u64,
}

fn prepare_artifact_view(
    sender: &GatewayWsCommandSender,
    artifact: &ArtifactSummary,
) -> Result<ArtifactViewPlan, ArtifactAccessError> {
    let version_id = exact_version(artifact)?;
    if artifact.workspace_id.trim().is_empty() || artifact.artifact.artifact_id.trim().is_empty() {
        return Err(invalid_artifact());
    }
    // View grant creation is a mutation without an idempotency key: one
    // request only. The session coordinator owns authentication recovery.
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
    let url = BrowserViewUrl::resolve(&access.gateway_base_url, grant.relative_url.as_str())
        .map_err(map_http_error)?;
    Ok(ArtifactViewPlan {
        url,
        expires_at: grant.expires_at,
    })
}

fn download_artifact(
    sender: &GatewayWsCommandSender,
    runtime_home: PathBuf,
    target: ArtifactDownloadTarget,
    operation: &ArtifactDownloadOperation,
) -> Result<(String, ArtifactHttpDownloadResult), ArtifactAccessError> {
    let artifact = resolve_artifact(sender, target)?;
    let access = sender
        .current_gateway_http_access()
        .map_err(map_authority_error)?;
    let request = super::actions::plan_artifact_http_download_request(
        Some(access.gateway_id.as_str().to_owned()),
        &artifact,
    )
    .map_err(|_| invalid_artifact())?;
    let authority = Arc::new(GatewayWsHttpAuthority {
        sender: sender.clone(),
    });
    let session = GatewayHttpSession::from_access(&access, authority).map_err(map_http_error)?;
    let service = ArtifactHttpDownloadService::new(session, runtime_home);
    let runtime = tokio::runtime::Runtime::new().map_err(|_| {
        ArtifactAccessError::new(
            "artifact download runtime is unavailable",
            "artifact_download_unavailable",
        )
    })?;
    let progress = |progress| operation.update_progress(progress);
    let result = runtime
        .block_on(service.download(request, operation.cancellation(), Some(&progress)))
        .map_err(map_download_error)?;
    if operation.cancellation().is_cancelled() {
        return Err(map_download_error(ArtifactHttpDownloadError::Cancelled));
    }
    if result
        .local_path
        .as_path()
        .to_str()
        .is_none_or(str::is_empty)
    {
        return Err(ArtifactAccessError::new(
            "verified artifact path cannot be represented for the native shell",
            "artifact_local_path_invalid",
        ));
    }
    Ok((artifact.artifact.display_name, result))
}

fn resolve_artifact(
    sender: &GatewayWsCommandSender,
    request: ArtifactDownloadTarget,
) -> Result<ArtifactSummary, ArtifactAccessError> {
    let workspace_id = non_empty(request.workspace_id)?;
    let artifact_id = non_empty(request.artifact_id)?;
    let version_id = request.version_id.and_then(|value| {
        let value = value.trim().to_owned();
        (!value.is_empty()).then_some(value)
    });
    let artifact = sender
        .artifact_get(ArtifactGetParams {
            workspace_id: workspace_id.clone(),
            artifact_id: artifact_id.clone(),
            version_id: version_id.clone(),
        })
        .map(|response| response.artifact)
        .map_err(map_rpc_error)?;
    if artifact.workspace_id != workspace_id
        || artifact.artifact.artifact_id != artifact_id
        || version_id
            .as_deref()
            .is_some_and(|expected| artifact.artifact.version_id.as_deref() != Some(expected))
    {
        return Err(invalid_artifact());
    }
    Ok(artifact)
}

fn exact_version(artifact: &ArtifactSummary) -> Result<String, ArtifactAccessError> {
    artifact
        .artifact
        .version_id
        .clone()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(invalid_artifact)
}

pub(super) fn artifact_http_session(
    sender: &GatewayWsCommandSender,
) -> anyhow::Result<GatewayHttpSession> {
    let access = sender
        .current_gateway_http_access()
        .map_err(map_authority_error)?;
    Ok(GatewayHttpSession::from_access(
        &access,
        Arc::new(GatewayWsHttpAuthority {
            sender: sender.clone(),
        }),
    )?)
}

fn non_empty(value: String) -> Result<String, ArtifactAccessError> {
    let value = value.trim().to_owned();
    if value.is_empty() || value.len() > 512 {
        return Err(invalid_artifact());
    }
    Ok(value)
}

fn invalid_artifact() -> ArtifactAccessError {
    ArtifactAccessError::new(
        "artifact identity or immutable metadata is incomplete",
        INVALID_ARTIFACT_ACTION_CODE,
    )
}

fn map_authority_error(error: GatewayHttpAuthorityError) -> ArtifactAccessError {
    match error {
        GatewayHttpAuthorityError::Terminal(_) => ArtifactAccessError::new(
            "Gateway session must be authenticated again",
            ARTIFACT_AUTHENTICATION_CODE,
        ),
        GatewayHttpAuthorityError::TemporarilyUnavailable => ArtifactAccessError::new(
            "Gateway session is unavailable for artifact access",
            ARTIFACT_AUTHENTICATION_CODE,
        ),
    }
}

fn map_rpc_error(error: anyhow::Error) -> ArtifactAccessError {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    let code = if lower.contains("unauthorized") || lower.contains("authentication") {
        ARTIFACT_AUTHENTICATION_CODE
    } else if lower.contains("forbidden") || lower.contains("not found") {
        "artifact_revoked_or_unavailable"
    } else {
        "artifact_action_failed"
    };
    ArtifactAccessError::new("artifact action failed", code)
}

fn map_http_error(error: GatewayHttpError) -> ArtifactAccessError {
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
    ArtifactAccessError::new("artifact HTTP action failed", code)
}

fn map_download_error(error: ArtifactHttpDownloadError) -> ArtifactAccessError {
    let code = match error {
        ArtifactHttpDownloadError::Authentication => ARTIFACT_AUTHENTICATION_CODE,
        ArtifactHttpDownloadError::RevokedOrUnavailable => "artifact_revoked_or_unavailable",
        other => other.code(),
    };
    ArtifactAccessError::new("artifact download failed", code)
}

impl crate::core::ClientCore {
    pub fn prepare_artifact_action_view(
        &self,
        identity: &super::workflow::ArtifactActionIdentity,
        summary: Option<ArtifactSummary>,
    ) -> Result<ArtifactViewPlan, ArtifactAccessError> {
        use super::workflow::ArtifactActionKind;
        if !self.start_artifact_preparation(identity, ArtifactActionKind::Open) {
            return Err(map_download_error(ArtifactHttpDownloadError::Cancelled));
        }
        let action = self
            .artifact_action_snapshot(identity)
            .ok_or_else(invalid_artifact)?;
        let result = (|| {
            let sender = self.compatibility_runtime().ws_command_sender();
            let artifact = match summary {
                Some(summary) => summary,
                None => resolve_artifact(&sender, action.target.clone())?,
            };
            if artifact.workspace_id != action.target.workspace_id
                || artifact.artifact.artifact_id != identity.artifact_id
                || identity
                    .version_id
                    .as_ref()
                    .is_some_and(|v| artifact.artifact.version_id.as_ref() != Some(v))
            {
                return Err(invalid_artifact());
            }
            prepare_artifact_view(&sender, &artifact)
        })();
        match result {
            Ok(plan) if self.prepare_artifact_presentation(identity, Some(plan.expires_at)) => {
                Ok(plan)
            }
            Ok(_) => Err(map_download_error(ArtifactHttpDownloadError::Cancelled)),
            Err(error) => {
                self.fail_artifact_preparation(identity, error.code.clone());
                Err(error)
            }
        }
    }

    pub fn download_artifact_action(
        self: &Arc<Self>,
        identity: &super::workflow::ArtifactActionIdentity,
        operation_id: String,
        runtime_home: PathBuf,
    ) -> Result<(String, ArtifactHttpDownloadResult), ArtifactAccessError> {
        use super::{operations::ArtifactDownloadState, workflow::ArtifactActionKind};
        if !self.start_artifact_preparation(identity, ArtifactActionKind::Share) {
            return Err(map_download_error(ArtifactHttpDownloadError::Cancelled));
        }
        let action = self
            .artifact_action_snapshot(identity)
            .ok_or_else(invalid_artifact)?;
        let operation = self
            .begin_artifact_download(operation_id, action.target.clone())
            .map_err(|error| {
                ArtifactAccessError::new(
                    "artifact download operation unavailable",
                    if error == super::operations::ArtifactDownloadOperationError::Capacity {
                        "artifact_download_capacity"
                    } else {
                        INVALID_ARTIFACT_ACTION_CODE
                    },
                )
            })?;
        if !self.attach_artifact_download(identity, operation.identity()) {
            return Err(map_download_error(ArtifactHttpDownloadError::Cancelled));
        }
        let result = download_artifact(
            &self.compatibility_runtime().ws_command_sender(),
            runtime_home,
            action.target,
            &operation,
        );
        match result {
            Ok(result) => {
                if operation.finish(ArtifactDownloadState::Completed, None)
                    && self.prepare_artifact_presentation(identity, None)
                {
                    Ok(result)
                } else {
                    Err(map_download_error(ArtifactHttpDownloadError::Cancelled))
                }
            }
            Err(error) => {
                let code = error.code.clone();
                if operation.cancellation().is_cancelled() {
                    Err(map_download_error(ArtifactHttpDownloadError::Cancelled))
                } else {
                    operation.finish(ArtifactDownloadState::Failed, Some(code));
                    Err(error)
                }
            }
        }
    }
}
