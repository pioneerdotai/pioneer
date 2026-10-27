use serde::{Deserialize, Serialize};

use crate::{KeystoreError, Result};

pub const PROVIDER_API_KEY_SERVICE: &str = "pioneer.gateway.provider_api_key";
pub const PROVIDER_PROXY_SERVICE: &str = "pioneer.gateway.provider_proxy";
pub const PROVIDER_BASE_URL_SERVICE: &str = "pioneer.gateway.provider_base_url";
pub const CLI_RUNTIME_PROXY_SERVICE: &str = "pioneer.gateway.cli_runtime_proxy";
pub const MODEL_CATALOG_PROXY_SERVICE: &str = "pioneer.gateway.model_catalog_proxy";
pub const MCP_OAUTH_SERVICE: &str = "pioneer.gateway.mcp_oauth";
pub const MCP_SECRET_SERVICE: &str = "pioneer.gateway.mcp_secret";
pub const USER_JWT_TOKEN_SERVICE: &str = "pioneer.gateway.user_jwt_token";
pub const GATEWAY_ACCESS_JWT_SIGNING_KEY_SERVICE: &str = "pioneer.gateway.access_jwt_signing_key";
pub const GATEWAY_AUTH_CREDENTIAL_HMAC_KEY_SERVICE: &str =
    "pioneer.gateway.auth_credential_hmac_key";
pub const GATEWAY_REMOTE_ACCESS_SECRET_SERVICE: &str = "pioneer.gateway.remote_access_secret";
pub const TASK_WEBHOOK_SECRET_SERVICE: &str = "pioneer.gateway.task_webhook_secret";
pub const DESKTOP_GATEWAY_SESSION_SERVICE: &str = "pioneer.desktop.gateway_session";

const GATEWAY_AUTH_KEY_USER: &str = "gateway";
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

    pub fn workspace_provider_proxy(workspace_id: &str, provider: &str) -> Result<Self> {
        let workspace_id = validate_user("workspace id", workspace_id)?;
        let provider = validate_user("provider", provider)?.to_ascii_lowercase();
        Self::from_service_user(
            PROVIDER_PROXY_SERVICE,
            format!("workspace:{workspace_id}:provider:{provider}"),
        )
    }

    pub fn workspace_provider_base_url(workspace_id: &str, provider: &str) -> Result<Self> {
        let workspace_id = validate_user("workspace id", workspace_id)?;
        let provider = validate_user("provider", provider)?.to_ascii_lowercase();
        Self::from_service_user(
            PROVIDER_BASE_URL_SERVICE,
            format!("workspace:{workspace_id}:provider:{provider}"),
        )
    }

    pub fn workspace_cli_runtime_proxy(workspace_id: &str, runtime_id: &str) -> Result<Self> {
        let workspace_id = validate_user("workspace id", workspace_id)?;
        let runtime_id = validate_user("CLI runtime id", runtime_id)?;
        Self::from_service_user(
            CLI_RUNTIME_PROXY_SERVICE,
            format!("workspace:{workspace_id}:runtime:{runtime_id}"),
        )
    }

    pub fn model_catalog_proxy() -> Self {
        Self {
            service: MODEL_CATALOG_PROXY_SERVICE.to_owned(),
            user: GATEWAY_AUTH_KEY_USER.to_owned(),
        }
    }

    pub fn user_jwt_token(token_id: &str) -> Result<Self> {
        let token_id = validate_user("user jwt token id", token_id)?;
        Self::from_service_user(USER_JWT_TOKEN_SERVICE, token_id)
    }

    pub fn gateway_access_jwt_signing_key() -> Self {
        Self {
            service: GATEWAY_ACCESS_JWT_SIGNING_KEY_SERVICE.to_owned(),
            user: GATEWAY_AUTH_KEY_USER.to_owned(),
        }
    }

    pub fn gateway_auth_credential_hmac_key() -> Self {
        Self {
            service: GATEWAY_AUTH_CREDENTIAL_HMAC_KEY_SERVICE.to_owned(),
            user: GATEWAY_AUTH_KEY_USER.to_owned(),
        }
    }

    pub fn mcp_oauth(connection_id: &str) -> Result<Self> {
        Self::from_service_user(
            MCP_OAUTH_SERVICE,
            validate_user("OAuth connection", connection_id)?,
        )
    }

    pub fn mcp_secret(ref_id: &str) -> Result<Self> {
        let ref_id = validate_user("mcp ref_id", ref_id)?;
        Self::from_service_user(MCP_SECRET_SERVICE, ref_id)
    }

    pub fn desktop_gateway_session(session_ref: &str) -> Result<Self> {
        let session_ref = validate_user("desktop Gateway session ref", session_ref)?;
        Self::from_service_user(DESKTOP_GATEWAY_SESSION_SERVICE, session_ref)
    }

    pub fn gateway_remote_access_secret(secret_ref: &str) -> Result<Self> {
        let secret_ref = validate_user("gateway remote access secret ref", secret_ref)?;
        Self::from_service_user(GATEWAY_REMOTE_ACCESS_SECRET_SERVICE, secret_ref)
    }

    pub fn task_webhook_secret(secret_ref: &str) -> Result<Self> {
        Self::from_service_user(
            TASK_WEBHOOK_SECRET_SERVICE,
            validate_user("task webhook secret ref", secret_ref)?,
        )
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
    ProviderProxy,
    ProviderBaseUrl,
    CliRuntimeProxy,
    ModelCatalogProxy,
    McpSecret,
    McpOAuth,
    UserJwtToken,
    GatewayAccessJwtSigningKey,
    GatewayAuthCredentialHmacKey,
    GatewayRemoteAccessSecret,
    DesktopGatewaySession,
    TaskWebhookSecret,
}

impl SecretKind {
    pub fn service(self) -> &'static str {
        match self {
            SecretKind::ProviderApiKey => PROVIDER_API_KEY_SERVICE,
            SecretKind::ProviderProxy => PROVIDER_PROXY_SERVICE,
            SecretKind::ProviderBaseUrl => PROVIDER_BASE_URL_SERVICE,
            SecretKind::CliRuntimeProxy => CLI_RUNTIME_PROXY_SERVICE,
            SecretKind::ModelCatalogProxy => MODEL_CATALOG_PROXY_SERVICE,
            SecretKind::McpOAuth => MCP_OAUTH_SERVICE,
            SecretKind::McpSecret => MCP_SECRET_SERVICE,
            SecretKind::UserJwtToken => USER_JWT_TOKEN_SERVICE,
            SecretKind::GatewayAccessJwtSigningKey => GATEWAY_ACCESS_JWT_SIGNING_KEY_SERVICE,
            SecretKind::GatewayAuthCredentialHmacKey => GATEWAY_AUTH_CREDENTIAL_HMAC_KEY_SERVICE,
            SecretKind::GatewayRemoteAccessSecret => GATEWAY_REMOTE_ACCESS_SECRET_SERVICE,
            SecretKind::DesktopGatewaySession => DESKTOP_GATEWAY_SESSION_SERVICE,
            SecretKind::TaskWebhookSecret => TASK_WEBHOOK_SECRET_SERVICE,
        }
    }

    pub fn from_service(service: &str) -> Option<Self> {
        match service {
            PROVIDER_API_KEY_SERVICE => Some(SecretKind::ProviderApiKey),
            PROVIDER_PROXY_SERVICE => Some(SecretKind::ProviderProxy),
            PROVIDER_BASE_URL_SERVICE => Some(SecretKind::ProviderBaseUrl),
            CLI_RUNTIME_PROXY_SERVICE => Some(SecretKind::CliRuntimeProxy),
            MODEL_CATALOG_PROXY_SERVICE => Some(SecretKind::ModelCatalogProxy),
            MCP_OAUTH_SERVICE => Some(SecretKind::McpOAuth),
            MCP_SECRET_SERVICE => Some(SecretKind::McpSecret),
            USER_JWT_TOKEN_SERVICE => Some(SecretKind::UserJwtToken),
            GATEWAY_ACCESS_JWT_SIGNING_KEY_SERVICE => Some(SecretKind::GatewayAccessJwtSigningKey),
            GATEWAY_AUTH_CREDENTIAL_HMAC_KEY_SERVICE => {
                Some(SecretKind::GatewayAuthCredentialHmacKey)
            }
            GATEWAY_REMOTE_ACCESS_SECRET_SERVICE => Some(SecretKind::GatewayRemoteAccessSecret),
            DESKTOP_GATEWAY_SESSION_SERVICE => Some(SecretKind::DesktopGatewaySession),
            TASK_WEBHOOK_SECRET_SERVICE => Some(SecretKind::TaskWebhookSecret),
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
    fn secret_factories_preserve_stable_service_user_pairs() {
        for (case, id, service, user, kind) in [
            (
                "provider_id_trims_and_normalizes",
                SecretId::provider_api_key("  OpenRouter  ").unwrap(),
                PROVIDER_API_KEY_SERVICE,
                "openrouter",
                SecretKind::ProviderApiKey,
            ),
            (
                "workspace_provider_id_is_scoped_by_workspace",
                SecretId::workspace_provider_api_key("  ws_1  ", "  OpenRouter  ").unwrap(),
                PROVIDER_API_KEY_SERVICE,
                "workspace:ws_1:provider:openrouter",
                SecretKind::ProviderApiKey,
            ),
            (
                "workspace_provider_proxy_id_is_scoped_by_workspace",
                SecretId::workspace_provider_proxy("  ws_1  ", "  OpenRouter  ").unwrap(),
                PROVIDER_PROXY_SERVICE,
                "workspace:ws_1:provider:openrouter",
                SecretKind::ProviderProxy,
            ),
            (
                "workspace_provider_base_url_id_is_scoped_by_workspace",
                SecretId::workspace_provider_base_url("  ws_1  ", "  OpenAI  ").unwrap(),
                PROVIDER_BASE_URL_SERVICE,
                "workspace:ws_1:provider:openai",
                SecretKind::ProviderBaseUrl,
            ),
            (
                "workspace_cli_runtime_proxy_id_is_scoped_by_workspace",
                SecretId::workspace_cli_runtime_proxy("  ws_1  ", "  codex_work  ").unwrap(),
                CLI_RUNTIME_PROXY_SERVICE,
                "workspace:ws_1:runtime:codex_work",
                SecretKind::CliRuntimeProxy,
            ),
            (
                "model_catalog_proxy_id_is_gateway_global",
                SecretId::model_catalog_proxy(),
                MODEL_CATALOG_PROXY_SERVICE,
                GATEWAY_AUTH_KEY_USER,
                SecretKind::ModelCatalogProxy,
            ),
            (
                "mcp_secret_maps_to_stable_pair",
                SecretId::mcp_secret("gateway_settings:mcp:server:token").unwrap(),
                MCP_SECRET_SERVICE,
                "gateway_settings:mcp:server:token",
                SecretKind::McpSecret,
            ),
            (
                "user_jwt_token_maps_to_future_non_superuser_pair",
                SecretId::user_jwt_token("user:alice:desktop").unwrap(),
                USER_JWT_TOKEN_SERVICE,
                "user:alice:desktop",
                SecretKind::UserJwtToken,
            ),
            (
                "gateway_access_jwt_signing_key_maps_to_stable_pair",
                SecretId::gateway_access_jwt_signing_key(),
                GATEWAY_ACCESS_JWT_SIGNING_KEY_SERVICE,
                GATEWAY_AUTH_KEY_USER,
                SecretKind::GatewayAccessJwtSigningKey,
            ),
            (
                "desktop_session_maps_to_dedicated_stable_pair",
                SecretId::desktop_gateway_session("local").unwrap(),
                DESKTOP_GATEWAY_SESSION_SERVICE,
                "local",
                SecretKind::DesktopGatewaySession,
            ),
        ] {
            assert_eq!(id.service(), service, "{case}");
            assert_eq!(id.user(), user, "{case}");
            assert_eq!(SecretKind::from_service(id.service()), Some(kind), "{case}");
        }
    }

    #[test]
    fn provider_ids_reject_empty_and_path_components() {
        for provider in ["   ", "../openrouter"] {
            let error = SecretId::provider_api_key(provider).expect_err(provider);
            assert!(
                matches!(error, KeystoreError::InvalidSecretId(_)),
                "{provider:?}"
            );
        }
    }
}
