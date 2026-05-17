use serde::{Deserialize, Serialize};

use crate::{KeystoreError, Result};

pub const PROVIDER_API_KEY_SERVICE: &str = "pioneer.gateway.provider_api_key";
pub const MCP_SECRET_SERVICE: &str = "pioneer.gateway.mcp_secret";
pub const USER_JWT_TOKEN_SERVICE: &str = "pioneer.gateway.user_jwt_token";
pub const SUPERUSER_JWT_TOKEN_SERVICE: &str = "pioneer.gateway.superuser_jwt_token";
pub const DESKTOP_GATEWAY_AUTH_TOKEN_SERVICE: &str = "pioneer.desktop.gateway_auth_token";

const SUPERUSER_JWT_TOKEN_USER: &str = "superuser";
const MAX_SPECIFIER_LEN: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretId {
    service: String,
    user: String,
}

impl SecretId {
    pub fn provider_api_key(provider: &str) -> Result<Self> {
        let provider = validate_user("provider", provider)?.to_ascii_lowercase();
        Self::from_service_user(PROVIDER_API_KEY_SERVICE, provider)
    }

    pub fn workspace_provider_api_key(workspace_id: &str, provider: &str) -> Result<Self> {
        let workspace_id = validate_user("workspace id", workspace_id)?;
        let provider = validate_user("provider", provider)?.to_ascii_lowercase();
        Self::from_service_user(
            PROVIDER_API_KEY_SERVICE,
            format!("workspace:{workspace_id}:provider:{provider}"),
        )
    }

    pub fn user_jwt_token(token_id: &str) -> Result<Self> {
        let token_id = validate_user("user jwt token id", token_id)?;
        Self::from_service_user(USER_JWT_TOKEN_SERVICE, token_id)
    }

    pub fn superuser_jwt_token() -> Self {
        Self {
            service: SUPERUSER_JWT_TOKEN_SERVICE.to_owned(),
            user: SUPERUSER_JWT_TOKEN_USER.to_owned(),
        }
    }

    pub fn mcp_secret(ref_id: &str) -> Result<Self> {
        let ref_id = validate_user("mcp ref_id", ref_id)?;
        Self::from_service_user(MCP_SECRET_SERVICE, ref_id)
    }

    pub fn desktop_gateway_auth_token(endpoint_id: &str) -> Result<Self> {
        let endpoint_id = validate_user("desktop endpoint id", endpoint_id)?;
        Self::from_service_user(DESKTOP_GATEWAY_AUTH_TOKEN_SERVICE, endpoint_id)
    }

    pub fn service(&self) -> &str {
        &self.service
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    pub(crate) fn from_service_user(
        service: impl Into<String>,
        user: impl Into<String>,
    ) -> Result<Self> {
        let service = validate_part("service", service.into())?;
        let user = validate_part("user", user.into())?;
        Ok(Self { service, user })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretKind {
    ProviderApiKey,
    McpSecret,
    UserJwtToken,
    SuperuserJwtToken,
    DesktopGatewayAuthToken,
}

impl SecretKind {
    pub fn service(self) -> &'static str {
        match self {
            SecretKind::ProviderApiKey => PROVIDER_API_KEY_SERVICE,
            SecretKind::McpSecret => MCP_SECRET_SERVICE,
            SecretKind::UserJwtToken => USER_JWT_TOKEN_SERVICE,
            SecretKind::SuperuserJwtToken => SUPERUSER_JWT_TOKEN_SERVICE,
            SecretKind::DesktopGatewayAuthToken => DESKTOP_GATEWAY_AUTH_TOKEN_SERVICE,
        }
    }

    pub fn from_service(service: &str) -> Option<Self> {
        match service {
            PROVIDER_API_KEY_SERVICE => Some(SecretKind::ProviderApiKey),
            MCP_SECRET_SERVICE => Some(SecretKind::McpSecret),
            USER_JWT_TOKEN_SERVICE => Some(SecretKind::UserJwtToken),
            SUPERUSER_JWT_TOKEN_SERVICE => Some(SecretKind::SuperuserJwtToken),
            DESKTOP_GATEWAY_AUTH_TOKEN_SERVICE => Some(SecretKind::DesktopGatewayAuthToken),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretMeta {
    pub kind: SecretKind,
    pub label: Option<String>,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
}

impl SecretMeta {
    pub fn new(kind: SecretKind, label: Option<String>, now_unix: i64) -> Self {
        Self {
            kind,
            label,
            created_at_unix: now_unix,
            updated_at_unix: now_unix,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretFilter {
    All,
    Kind(SecretKind),
    Service(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretEntryMeta {
    pub id: SecretId,
    pub kind: Option<SecretKind>,
    pub label: Option<String>,
    pub created_at_unix: Option<i64>,
    pub updated_at_unix: Option<i64>,
}

impl SecretEntryMeta {
    pub(crate) fn from_meta(id: SecretId, meta: SecretMeta) -> Self {
        Self {
            id,
            kind: Some(meta.kind),
            label: meta.label,
            created_at_unix: Some(meta.created_at_unix),
            updated_at_unix: Some(meta.updated_at_unix),
        }
    }
}

pub(crate) fn filter_matches(
    id: &SecretId,
    kind: Option<SecretKind>,
    filter: &SecretFilter,
) -> bool {
    match filter {
        SecretFilter::All => true,
        SecretFilter::Kind(expected) => {
            kind == Some(*expected) || SecretKind::from_service(id.service()) == Some(*expected)
        }
        SecretFilter::Service(service) => id.service() == service,
    }
}

fn validate_user(field: &str, value: &str) -> Result<String> {
    validate_part(field, value.trim().to_owned())
}

fn validate_part(field: &str, value: String) -> Result<String> {
    if value.is_empty() {
        return Err(KeystoreError::InvalidSecretId(format!(
            "{field} must not be empty"
        )));
    }

    if value.len() > MAX_SPECIFIER_LEN {
        return Err(KeystoreError::InvalidSecretId(format!(
            "{field} must be at most {MAX_SPECIFIER_LEN} bytes"
        )));
    }

    if value.contains('/') || value.contains('\\') {
        return Err(KeystoreError::InvalidSecretId(format!(
            "{field} must not contain path separators"
        )));
    }

    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_id_trims_and_normalizes() {
        let id = SecretId::provider_api_key("  OpenRouter  ").expect("provider id");

        assert_eq!(id.service(), PROVIDER_API_KEY_SERVICE);
        assert_eq!(id.user(), "openrouter");
    }

    #[test]
    fn workspace_provider_id_is_scoped_by_workspace() {
        let id = SecretId::workspace_provider_api_key("  ws_1  ", "  OpenRouter  ")
            .expect("provider id");

        assert_eq!(id.service(), PROVIDER_API_KEY_SERVICE);
        assert_eq!(id.user(), "workspace:ws_1:provider:openrouter");
    }

    #[test]
    fn empty_provider_id_is_rejected() {
        let err = SecretId::provider_api_key("   ").expect_err("empty provider rejected");

        assert!(matches!(err, KeystoreError::InvalidSecretId(_)));
    }

    #[test]
    fn path_semantics_are_rejected() {
        let err = SecretId::provider_api_key("../openrouter").expect_err("path rejected");

        assert!(matches!(err, KeystoreError::InvalidSecretId(_)));
    }

    #[test]
    fn mcp_secret_maps_to_stable_pair() {
        let id = SecretId::mcp_secret("gateway_settings:mcp:server:token").expect("mcp id");

        assert_eq!(id.service(), MCP_SECRET_SERVICE);
        assert_eq!(id.user(), "gateway_settings:mcp:server:token");
    }

    #[test]
    fn user_jwt_token_maps_to_future_non_superuser_pair() {
        let id = SecretId::user_jwt_token("user:alice:desktop").expect("user jwt token id");

        assert_eq!(id.service(), USER_JWT_TOKEN_SERVICE);
        assert_eq!(id.user(), "user:alice:desktop");
    }

    #[test]
    fn superuser_jwt_token_maps_to_current_token_pair() {
        let id = SecretId::superuser_jwt_token();

        assert_eq!(id.service(), SUPERUSER_JWT_TOKEN_SERVICE);
        assert_eq!(id.user(), SUPERUSER_JWT_TOKEN_USER);
    }

    #[test]
    fn desktop_token_maps_to_stable_pair() {
        let id = SecretId::desktop_gateway_auth_token("main-window").expect("desktop id");

        assert_eq!(id.service(), DESKTOP_GATEWAY_AUTH_TOKEN_SERVICE);
        assert_eq!(id.user(), "main-window");
    }
}
