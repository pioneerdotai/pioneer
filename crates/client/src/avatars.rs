//! Avatar domain selectors and adapters over the shared image service.
pub use crate::images::{
    ImageCacheError as AvatarCacheError, ImageCacheService as AvatarCacheService,
    ImageCacheSource as AvatarCacheSource, ImagePublication as AvatarPublication,
    image_identity_key as avatar_identity_key, invalidate_image_cache as invalidate_avatar_cache,
};
use crate::platform::ClientPath;
use crate::{
    core::ClientCore,
    images::{ImageCacheRequest, ImageReference},
};
pub use pioneer_protocol::{MemberSummary, PrincipalId, ProfileAvatarMediaType};
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;

/// Resolve built-in CLI artwork from provider identity and the shell's appearance.
pub fn cli_runtime_avatar_revision(
    kind: pioneer_protocol::CLIAgentRuntimeKind,
    dark: bool,
) -> &'static str {
    use pioneer_protocol::{
        CLAUDE_AGENT_AVATAR_REVISION, CLIAgentRuntimeKind, CODEX_AGENT_AVATAR_REVISION,
        OPENCODE_AGENT_AVATAR_DARK_REVISION, OPENCODE_AGENT_AVATAR_LIGHT_REVISION,
    };
    match kind {
        CLIAgentRuntimeKind::Codex => CODEX_AGENT_AVATAR_REVISION,
        CLIAgentRuntimeKind::Claude => CLAUDE_AGENT_AVATAR_REVISION,
        CLIAgentRuntimeKind::OpenCode if dark => OPENCODE_AGENT_AVATAR_DARK_REVISION,
        CLIAgentRuntimeKind::OpenCode => OPENCODE_AGENT_AVATAR_LIGHT_REVISION,
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AvatarCacheRequest {
    pub principal_id: PrincipalId,
    pub avatar_revision: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AvatarCacheResult {
    pub local_path: ClientPath,
    pub principal_id: PrincipalId,
    pub avatar_revision: String,
    pub media_type: ProfileAvatarMediaType,
    pub source: AvatarCacheSource,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AgentAvatarCacheResult {
    pub local_path: ClientPath,
    pub avatar_revision: String,
    pub media_type: ProfileAvatarMediaType,
    pub source: AvatarCacheSource,
}

impl ImageCacheRequest {
    pub(crate) fn member(request: AvatarCacheRequest) -> Result<Self, AvatarCacheError> {
        Self::new(ImageReference::member_avatar(
            &request.principal_id,
            request.avatar_revision,
        ))
        .map(|image| image.with_resource_id(request.principal_id.to_string()))
    }
    pub(crate) fn agent(revision: String) -> Result<Self, AvatarCacheError> {
        Self::new(ImageReference::agent_avatar(revision.clone()))
            .map(|image| image.with_resource_id(format!("agent:{revision}")))
    }
}
impl AvatarCacheService {
    pub async fn resolve(
        &self,
        request: AvatarCacheRequest,
        cancellation: CancellationToken,
    ) -> Result<AvatarCacheResult, AvatarCacheError> {
        let principal_id = request.principal_id.clone();
        let result = self
            .resolve_image(ImageCacheRequest::member(request)?, cancellation)
            .await?;
        Ok(AvatarCacheResult {
            local_path: result.local_path,
            principal_id,
            avatar_revision: result.revision,
            media_type: result.media_type,
            source: result.source,
        })
    }
    pub async fn resolve_agent_avatar(
        &self,
        revision: String,
        cancellation: CancellationToken,
    ) -> Result<AgentAvatarCacheResult, AvatarCacheError> {
        let result = self
            .resolve_image(ImageCacheRequest::agent(revision)?, cancellation)
            .await?;
        Ok(AgentAvatarCacheResult {
            local_path: result.local_path,
            avatar_revision: result.revision,
            media_type: result.media_type,
            source: result.source,
        })
    }
}
impl ClientCore {
    pub fn avatar_cache_service(
        &self,
        runtime_home: PathBuf,
    ) -> Result<AvatarCacheService, AvatarCacheError> {
        self.image_cache_service(runtime_home)
    }
    pub async fn resolve_member_avatar(
        &self,
        service: &AvatarCacheService,
        request: AvatarCacheRequest,
        cancellation: CancellationToken,
    ) -> Result<AvatarCacheResult, AvatarCacheError> {
        let principal_id = request.principal_id.clone();
        let result = self
            .resolve_image(service, ImageCacheRequest::member(request)?, cancellation)
            .await?;
        Ok(AvatarCacheResult {
            local_path: result.local_path,
            principal_id,
            avatar_revision: result.revision,
            media_type: result.media_type,
            source: result.source,
        })
    }
    pub async fn resolve_agent_avatar(
        &self,
        service: &AvatarCacheService,
        revision: String,
        cancellation: CancellationToken,
    ) -> Result<AgentAvatarCacheResult, AvatarCacheError> {
        let result = self
            .resolve_image(service, ImageCacheRequest::agent(revision)?, cancellation)
            .await?;
        Ok(AgentAvatarCacheResult {
            local_path: result.local_path,
            avatar_revision: result.revision,
            media_type: result.media_type,
            source: result.source,
        })
    }
}
