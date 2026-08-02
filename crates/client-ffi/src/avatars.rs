//! Secret-free FFI projection for the native member-avatar cache.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pioneer_client::{
    avatars::{
        AvatarCacheError, AvatarCacheRequest, AvatarCacheService, AvatarCacheSource,
        invalidate_avatar_cache,
    },
    transport::{
        http::{
            GatewayHttpAccess, GatewayHttpAuthorityError, GatewayHttpSession,
            GatewayHttpSessionAuthority,
        },
        ws::GatewayWsCommandSender,
    },
};
use pioneer_protocol::{PrincipalId, ProfileAvatarMediaType};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

use crate::ClientFfiError;

pub(crate) const AVATAR_RECONFIGURATION_CODE: &str = "avatar_reconfiguration_required";

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientMemberAvatarCacheRequest {
    pub principal_id: PrincipalId,
    pub avatar_revision: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientMemberAvatarCacheResult {
    pub cached_image_path: String,
    pub principal_id: PrincipalId,
    pub avatar_revision: String,
    pub media_type: ProfileAvatarMediaType,
    pub source: AvatarCacheSource,
}

#[derive(Default)]
pub(crate) struct ClientFfiAvatarCache {
    operation_gate: Mutex<()>,
}

impl ClientFfiAvatarCache {
    pub(crate) fn resolve(
        &self,
        sender: &GatewayWsCommandSender,
        runtime_home: PathBuf,
        request: ClientMemberAvatarCacheRequest,
    ) -> Result<ClientMemberAvatarCacheResult, ClientFfiError> {
        let _operation = self.operation_gate.lock().map_err(|_| {
            ClientFfiError::new("avatar cache is unavailable", "avatar_cache_unavailable")
        })?;
        let access = sender
            .current_gateway_http_access()
            .map_err(map_authority_error)?;
        let authority = Arc::new(FfiAvatarHttpAuthority {
            sender: sender.clone(),
        });
        let session = GatewayHttpSession::from_access(&access, authority)
            .map_err(|_| {
                ClientFfiError::new(
                    "Gateway endpoint is unavailable for avatar access",
                    AVATAR_RECONFIGURATION_CODE,
                )
            })?;
        let service = AvatarCacheService::new(
            session,
            runtime_home,
            access.gateway_id,
            access.session_id,
        );
        let runtime = Runtime::new().map_err(|_| {
            ClientFfiError::new("avatar cache runtime is unavailable", "avatar_cache_unavailable")
        })?;
        let result = runtime
            .block_on(service.resolve(
                AvatarCacheRequest {
                    principal_id: request.principal_id,
                    avatar_revision: request.avatar_revision,
                },
                CancellationToken::new(),
            ))
            .map_err(map_cache_error)?;
        let cached_image_path = result
            .local_path
            .as_path()
            .to_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                ClientFfiError::new(
                    "avatar cache path cannot be represented for the native shell",
                    "avatar_cache_path_invalid",
                )
            })?
            .to_owned();
        Ok(ClientMemberAvatarCacheResult {
            cached_image_path,
            principal_id: result.principal_id,
            avatar_revision: result.avatar_revision,
            media_type: result.media_type,
            source: result.source,
        })
    }

    pub(crate) fn invalidate_all(&self, runtime_home: &Path) {
        let Ok(_operation) = self.operation_gate.lock() else {
            return;
        };
        let _ = invalidate_avatar_cache(runtime_home);
    }
}

struct FfiAvatarHttpAuthority {
    sender: GatewayWsCommandSender,
}

#[async_trait]
impl GatewayHttpSessionAuthority for FfiAvatarHttpAuthority {
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
            Err(GatewayHttpAuthorityError::TemporarilyUnavailable)
        }
    }
}

fn map_authority_error(error: GatewayHttpAuthorityError) -> ClientFfiError {
    match error {
        GatewayHttpAuthorityError::Terminal(_) => ClientFfiError::new(
            "Gateway session must be authenticated again",
            "avatar_authentication_required",
        ),
        GatewayHttpAuthorityError::TemporarilyUnavailable => ClientFfiError::new(
            "Gateway session must be refreshed before avatar access",
            "avatar_authentication_required",
        ),
    }
}

fn map_cache_error(error: AvatarCacheError) -> ClientFfiError {
    ClientFfiError::new("member avatar is unavailable", error.code())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ffi_result_serialization_contains_no_content_or_credentials() {
        let result = ClientMemberAvatarCacheResult {
            cached_image_path: "/owned/cache/avatar".to_owned(),
            principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
            avatar_revision: "a".repeat(64),
            media_type: ProfileAvatarMediaType::Png,
            source: AvatarCacheSource::Revalidated,
        };
        let value = serde_json::to_value(result).unwrap();
        assert!(value.get("cached_image_path").is_some());
        for forbidden in ["content", "content_base64", "access_token", "authorization"] {
            assert!(value.get(forbidden).is_none(), "forbidden FFI field {forbidden}");
        }
    }

    #[test]
    fn refresh_owned_by_shell_is_reported_as_authentication_required() {
        let error = map_authority_error(GatewayHttpAuthorityError::TemporarilyUnavailable);
        assert_eq!(error.code, "avatar_authentication_required");
    }
}
