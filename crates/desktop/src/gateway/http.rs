//! Desktop binding for the shared authenticated Gateway HTTP session.

use std::{fmt, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use pioneer_client::{
    artifacts::http_download::{
        ArtifactHttpDownloadError, ArtifactHttpDownloadProgressSink, ArtifactHttpDownloadRequest,
        ArtifactHttpDownloadResult, ArtifactHttpDownloadService,
    },
    artifacts::preview::{ArtifactHttpPreviewService, ArtifactPreviewReadData},
    avatars::{
        AgentAvatarCacheResult, AvatarCacheError, AvatarCacheRequest, AvatarCacheResult,
        AvatarCacheService,
    },
    gateway::endpoint::GatewayBaseUrl,
    transport::{
        http::{
            BrowserViewUrl, GatewayHttpAccess, GatewayHttpAuthorityError, GatewayHttpError,
            GatewayHttpSession, GatewayHttpSessionAuthority,
        },
        ws::GatewayWsCommandSender,
    },
};
use pioneer_protocol::{ArtifactRef, AuthSessionId};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

use super::{DesktopSessionConnectionOutcome, GatewayRuntime};

#[derive(Clone)]
pub(crate) struct DesktopGatewayHttpClient {
    endpoint_id: String,
    gateway_base_url: GatewayBaseUrl,
    session_id: AuthSessionId,
    session: GatewayHttpSession,
    downloads: ArtifactHttpDownloadService,
    previews: ArtifactHttpPreviewService,
    avatars: AvatarCacheService,
    runtime: Arc<Runtime>,
}

impl fmt::Debug for DesktopGatewayHttpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DesktopGatewayHttpClient")
            .field("endpoint_id", &self.endpoint_id)
            .field("gateway_base_url", &self.gateway_base_url)
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

impl DesktopGatewayHttpClient {
    pub(crate) fn for_endpoint(
        endpoint: &pioneer_client::gateway::types::GatewayEndpoint,
        sender: GatewayWsCommandSender,
        runtime_home: PathBuf,
        client_core: Arc<pioneer_client::core::ClientCore>,
    ) -> Result<Self, GatewayHttpError> {
        let access = sender
            .current_gateway_http_access()
            .map_err(map_authority_error)?;
        if endpoint.server_gateway_id.as_ref() != Some(&access.gateway_id)
            || endpoint.gateway_base_url != access.gateway_base_url
            || endpoint.session_ref.is_none()
        {
            return Err(GatewayHttpError::InvalidEndpoint);
        }
        let authority = Arc::new(DesktopGatewayHttpAuthority {
            client_core,
            endpoint_id: endpoint.id.clone(),
            sender,
        });
        let session =
            GatewayHttpSession::from_endpoint(endpoint, access.session_id.clone(), authority)?;
        let downloads = ArtifactHttpDownloadService::new(session.clone(), runtime_home.clone());
        let previews = ArtifactHttpPreviewService::new(session.clone());
        let avatars = AvatarCacheService::new(
            session.clone(),
            runtime_home,
            access.gateway_id,
            access.session_id.clone(),
        );
        let runtime = Runtime::new().map_err(|_| GatewayHttpError::ServiceUnavailable)?;
        Ok(Self {
            endpoint_id: endpoint.id.clone(),
            gateway_base_url: endpoint.gateway_base_url.clone(),
            session_id: access.session_id,
            session,
            downloads,
            previews,
            avatars,
            runtime: Arc::new(runtime),
        })
    }

    pub(crate) fn resolve_member_avatar(
        &self,
        request: AvatarCacheRequest,
        cancellation: CancellationToken,
    ) -> Result<AvatarCacheResult, AvatarCacheError> {
        self.runtime
            .block_on(self.avatars.resolve(request, cancellation))
    }

    pub(crate) fn resolve_agent_avatar(
        &self,
        avatar_revision: String,
        cancellation: CancellationToken,
    ) -> Result<AgentAvatarCacheResult, AvatarCacheError> {
        self.runtime.block_on(
            self.avatars
                .resolve_agent_avatar(avatar_revision, cancellation),
        )
    }

    pub(crate) fn matches(
        &self,
        endpoint: &pioneer_client::gateway::types::GatewayEndpoint,
        access: &GatewayHttpAccess,
    ) -> bool {
        self.endpoint_id == endpoint.id
            && self.gateway_base_url == endpoint.gateway_base_url
            && self.gateway_base_url == access.gateway_base_url
            && self.session_id == access.session_id
            && endpoint.server_gateway_id.as_ref() == Some(&access.gateway_id)
    }

    pub(crate) fn resolve_view_url(
        &self,
        relative_url: &str,
    ) -> Result<BrowserViewUrl, GatewayHttpError> {
        self.session.resolve_view_url(relative_url)
    }

    pub(crate) fn download(
        &self,
        request: ArtifactHttpDownloadRequest,
        cancellation: CancellationToken,
        progress: Option<&dyn ArtifactHttpDownloadProgressSink>,
    ) -> Result<ArtifactHttpDownloadResult, ArtifactHttpDownloadError> {
        self.runtime
            .block_on(self.downloads.download(request, cancellation, progress))
    }

    pub(crate) fn fetch_artifact_thumbnail(
        &self,
        workspace_id: &str,
        artifact: &ArtifactRef,
        cancellation: CancellationToken,
    ) -> anyhow::Result<ArtifactPreviewReadData> {
        self.runtime.block_on(
            self.previews
                .fetch_thumbnail(workspace_id, artifact, cancellation),
        )
    }
}

struct DesktopGatewayHttpAuthority {
    client_core: Arc<pioneer_client::core::ClientCore>,
    endpoint_id: String,
    sender: GatewayWsCommandSender,
}

#[async_trait]
impl GatewayHttpSessionAuthority for DesktopGatewayHttpAuthority {
    async fn current_access(&self) -> Result<GatewayHttpAccess, GatewayHttpAuthorityError> {
        self.sender.current_gateway_http_access()
    }

    async fn coordinated_refresh(
        &self,
        rejected_generation: u64,
    ) -> Result<GatewayHttpAccess, GatewayHttpAuthorityError> {
        if let Ok(current) = self.sender.current_gateway_http_access()
            && current.generation != rejected_generation
        {
            return Ok(current);
        }
        let client_core = self.client_core.clone();
        let endpoint_id = self.endpoint_id.clone();
        let sender = self.sender.clone();
        tokio::task::spawn_blocking(move || {
            if let Ok(current) = sender.current_gateway_http_access()
                && current.generation != rejected_generation
            {
                return Ok(current);
            }
            let mut runtime = GatewayRuntime::load(client_core)
                .map_err(|_| GatewayHttpAuthorityError::TemporarilyUnavailable)?;
            match runtime
                .replace_gateway_session_access_after_rejection(
                    endpoint_id.as_str(),
                    &sender,
                    rejected_generation,
                )
                .map_err(|_| GatewayHttpAuthorityError::TemporarilyUnavailable)?
            {
                None | Some(DesktopSessionConnectionOutcome::Connected { .. }) => {
                    sender.current_gateway_http_access()
                }
                Some(DesktopSessionConnectionOutcome::Terminal(terminal)) => {
                    Err(GatewayHttpAuthorityError::Terminal(terminal.reason))
                }
            }
        })
        .await
        .map_err(|_| GatewayHttpAuthorityError::TemporarilyUnavailable)?
    }
}

fn map_authority_error(error: GatewayHttpAuthorityError) -> GatewayHttpError {
    match error {
        GatewayHttpAuthorityError::Terminal(reason) => {
            GatewayHttpError::AuthenticationTerminal(reason)
        }
        GatewayHttpAuthorityError::TemporarilyUnavailable => {
            GatewayHttpError::AuthenticationUnavailable
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::gateway::session_lifecycle::SessionTerminalReason;

    #[test]
    fn desktop_http_authority_source_does_not_extract_credentials() {
        let source = include_str!("http.rs");
        assert!(source.contains("DesktopGatewayHttpAuthority"));
        assert!(!source.contains(&["access", "_token.expose_secret"].concat()));
        assert!(!source.contains(&["refresh", "_token"].concat()));
    }

    #[test]
    fn terminal_authority_mapping_remains_typed() {
        assert_eq!(
            map_authority_error(GatewayHttpAuthorityError::Terminal(
                SessionTerminalReason::SessionRevoked,
            )),
            GatewayHttpError::AuthenticationTerminal(SessionTerminalReason::SessionRevoked)
        );
    }
}
