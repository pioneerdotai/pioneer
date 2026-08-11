use pioneer_client::{
    gateway::endpoint::GatewayBaseUrl,
    transport::ws::{
        GatewayWsSessionIdentity, GatewayWsSessionSpec, auth_exchange::AuthExchangeError,
    },
};
use pioneer_protocol::{
    AuthDeviceActivateParams, AuthDeviceActivationPresentation, AuthDeviceCreateResponse,
    AuthRefreshParams, AuthSecretString, AuthSessionId, DeviceId, GatewayId, PioneerAppUrlScheme,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::contracts::ClientGatewayWsTimings;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientGatewaySessionLifecycleRequest {
    pub endpoint_id: String,
    pub event: pioneer_client::gateway::session_lifecycle::SessionLifecycleEvent,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientGatewaySessionLifecycleResult {
    pub state: pioneer_client::gateway::session_lifecycle::SessionLifecycleState,
    pub effect: pioneer_client::gateway::session_lifecycle::SessionLifecycleEffect,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientDeviceActivationPresentationRequest {
    pub gateway_base_url: GatewayBaseUrl,
    pub created_device: AuthDeviceCreateResponse,
    pub app_url_scheme: PioneerAppUrlScheme,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, PartialEq, Eq)]
pub struct ClientDeviceActivationPresentationResult {
    pub device_id: DeviceId,
    pub session_id: AuthSessionId,
    pub gateway_id: GatewayId,
    pub gateway_base_url: GatewayBaseUrl,
    pub expires_at_unix: u64,
    pub manual_code: AuthSecretString,
    pub deep_link: AuthSecretString,
    pub qr_width: u64,
    pub qr_modules: Vec<bool>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientDeviceActivationParseRequest {
    pub uri: AuthSecretString,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, PartialEq, Eq)]
pub struct ClientDeviceActivationParseResult {
    pub gateway_base_url: GatewayBaseUrl,
    pub gateway_id: GatewayId,
    pub activation_code: AuthSecretString,
}

impl ClientDeviceActivationPresentationResult {
    pub fn from_request(
        request: ClientDeviceActivationPresentationRequest,
    ) -> Result<Self, String> {
        let gateway_base_url = request.gateway_base_url.clone();
        let presentation =
            pioneer_client::gateway::device_activation::DeviceActivationQrPresentation::from_created_device_with_scheme(
                &request.gateway_base_url,
                request.created_device,
                request.app_url_scheme,
            )
            .map_err(|error| error.to_string())?;
        Ok(Self {
            device_id: presentation.device_id.clone(),
            session_id: presentation.session_id.clone(),
            gateway_id: presentation.gateway_id.clone(),
            gateway_base_url,
            expires_at_unix: presentation.expires_at_unix,
            manual_code: AuthSecretString::new(presentation.manual_code()),
            deep_link: AuthSecretString::new(presentation.deep_link()),
            qr_width: u64::try_from(presentation.qr_width())
                .map_err(|_| "activation QR width is too large".to_owned())?,
            qr_modules: presentation.qr_modules().to_vec(),
        })
    }
}

impl ClientDeviceActivationParseResult {
    pub fn from_request(request: ClientDeviceActivationParseRequest) -> Result<Self, String> {
        let presentation = AuthDeviceActivationPresentation::parse(request.uri.expose_secret())?;
        let activation_code = AuthSecretString::new(presentation.activation_code());
        Ok(Self {
            gateway_base_url: presentation.gateway_base_url,
            gateway_id: presentation.gateway_id,
            activation_code,
        })
    }
}

macro_rules! redacted_activation_debug {
    ($type:ty) => {
        impl std::fmt::Debug for $type {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_struct(stringify!($type))
                    .field("secret", &"[redacted]")
                    .finish()
            }
        }
    };
}

redacted_activation_debug!(ClientDeviceActivationPresentationRequest);
redacted_activation_debug!(ClientDeviceActivationPresentationResult);
redacted_activation_debug!(ClientDeviceActivationParseRequest);
redacted_activation_debug!(ClientDeviceActivationParseResult);

pub const INVALID_AUTH_REQUEST_CODE: &str = "invalid_auth_request";
pub const AUTH_EXCHANGE_RUNTIME_CODE: &str = "auth_exchange_runtime_failed";

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientAuthRefreshRequest {
    pub gateway_base_url: GatewayBaseUrl,
    pub credential: AuthSecretString,
    pub params: AuthRefreshParams,
    #[serde(default = "default_exchange_timeout_ms")]
    pub timeout_ms: u64,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientAuthDeviceActivateRequest {
    pub gateway_base_url: GatewayBaseUrl,
    pub credential: AuthSecretString,
    pub params: AuthDeviceActivateParams,
    #[serde(default = "default_exchange_timeout_ms")]
    pub timeout_ms: u64,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientAuthSessionCleanupRequest {
    pub gateway_base_url: GatewayBaseUrl,
    pub access_token: AuthSecretString,
    pub session_id: AuthSessionId,
    #[serde(default = "default_exchange_timeout_ms")]
    pub timeout_ms: u64,
}

macro_rules! redacted_exchange_debug {
    ($type:ty) => {
        impl std::fmt::Debug for $type {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_struct(stringify!($type))
                    .field("gateway_base_url", &self.gateway_base_url)
                    .field("credential", &"[redacted]")
                    .field("params", &self.params)
                    .field("timeout_ms", &self.timeout_ms)
                    .finish()
            }
        }
    };
}

redacted_exchange_debug!(ClientAuthRefreshRequest);
redacted_exchange_debug!(ClientAuthDeviceActivateRequest);

impl std::fmt::Debug for ClientAuthSessionCleanupRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientAuthSessionCleanupRequest")
            .field("gateway_base_url", &self.gateway_base_url)
            .field("access_token", &"[redacted]")
            .field("session_id", &self.session_id)
            .field("timeout_ms", &self.timeout_ms)
            .finish()
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientGatewaySessionReplaceAccessRequest {
    pub endpoint: pioneer_client::gateway::types::GatewayEndpoint,
    pub server_gateway_id: pioneer_protocol::GatewayId,
    pub session_id: pioneer_protocol::AuthSessionId,
    pub device_id: pioneer_protocol::DeviceId,
    pub access_token: AuthSecretString,
    pub access_expires_at_unix: u64,
    pub refresh_leeway_seconds: u64,
    pub timings: ClientGatewayWsTimings,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientGatewaySessionReplaceAccessResult {
    pub connection_id: u64,
}

impl ClientGatewaySessionReplaceAccessRequest {
    pub fn into_session_spec(self) -> Result<GatewayWsSessionSpec, String> {
        if self.endpoint.server_gateway_id.as_ref() != Some(&self.server_gateway_id) {
            return Err("Gateway endpoint pin does not match session GatewayId".to_owned());
        }
        if self.endpoint.session_ref.is_none() {
            return Err("Gateway endpoint has no session reference".to_owned());
        }
        let timings = self
            .timings
            .to_gateway_ws_timings()
            .map_err(|error| error.to_string())?;
        Ok(GatewayWsSessionSpec {
            endpoint_id: self.endpoint.id,
            endpoint_name: self.endpoint.name,
            endpoint_kind: self.endpoint.kind,
            gateway_base_url: self.endpoint.gateway_base_url,
            identity: GatewayWsSessionIdentity {
                server_gateway_id: self.server_gateway_id,
                session_id: self.session_id,
                device_id: self.device_id,
                access_expires_at_unix: self.access_expires_at_unix,
                refresh_leeway_seconds: self.refresh_leeway_seconds,
            },
            access_token: self.access_token,
            timings,
        })
    }
}

pub fn auth_exchange_runtime(
    timeout_ms: u64,
) -> Result<
    (
        tokio::runtime::Runtime,
        pioneer_client::transport::ws::auth_exchange::AuthExchangeClient,
    ),
    String,
> {
    if !(1..=60_000).contains(&timeout_ms) {
        return Err("auth exchange timeout must be between 1 and 60000 ms".to_owned());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "failed to create auth exchange runtime".to_owned())?;
    Ok((
        runtime,
        pioneer_client::transport::ws::auth_exchange::AuthExchangeClient::new(
            Duration::from_millis(timeout_ms),
        ),
    ))
}

pub fn auth_exchange_error(error: AuthExchangeError) -> crate::ClientFfiError {
    let code = error
        .code
        .clone()
        .unwrap_or_else(|| error.kind.code().to_owned());
    crate::ClientFfiError::new(error.message, code)
}

const fn default_exchange_timeout_ms() -> u64 {
    15_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::transport::ws::auth_exchange::AuthExchangeErrorKind;

    #[test]
    fn exchange_request_debug_redacts_credential() {
        let raw = "prf_super_secret_refresh_value";
        let request = ClientAuthRefreshRequest {
            gateway_base_url: GatewayBaseUrl::parse_presentation("http://localhost:17878").unwrap(),
            credential: AuthSecretString::new(raw),
            params: AuthRefreshParams {
                refresh_request_id: "Q00000000000000000001".to_owned(),
                client_version: None,
            },
            timeout_ms: 100,
        };
        assert!(!format!("{request:?}").contains(raw));
    }

    #[test]
    fn cleanup_request_debug_redacts_access_token() {
        let raw = "access-super-secret-value";
        let request = ClientAuthSessionCleanupRequest {
            gateway_base_url: GatewayBaseUrl::parse_presentation("http://localhost:17878").unwrap(),
            access_token: AuthSecretString::new(raw),
            session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
            timeout_ms: 100,
        };
        assert!(!format!("{request:?}").contains(raw));
    }

    #[test]
    fn local_auth_exchange_errors_keep_stable_machine_codes() {
        let error = auth_exchange_error(AuthExchangeError {
            kind: AuthExchangeErrorKind::InvalidEndpoint,
            code: None,
            message: "invalid endpoint".to_owned(),
        });
        assert_eq!(error.code, "invalid_auth_endpoint");

        let server = auth_exchange_error(AuthExchangeError {
            kind: AuthExchangeErrorKind::Server,
            code: Some("session_revoked".to_owned()),
            message: "revoked".to_owned(),
        });
        assert_eq!(server.code, "session_revoked");
    }

    #[test]
    fn activation_direct_call_contract_round_trips_and_redacts_debug() {
        let token = "K7M4-P9Q2".to_owned();
        let gateway_id = GatewayId::new("G00000000000000000001").unwrap();
        let request = ClientDeviceActivationPresentationRequest {
            gateway_base_url: GatewayBaseUrl::parse_presentation(
                "https://gateway.example/pioneer/",
            )
            .unwrap(),
            created_device: AuthDeviceCreateResponse {
                device_id: DeviceId::new("D00000000000000000001").unwrap(),
                session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
                activation_code: AuthSecretString::new(token.clone()),
                expires_at_unix: 1_800_000_000,
                gateway_id: gateway_id.clone(),
            },
            app_url_scheme: PioneerAppUrlScheme::Development,
        };
        assert!(!format!("{request:?}").contains(&token));

        let presentation = ClientDeviceActivationPresentationResult::from_request(request).unwrap();
        assert_eq!(presentation.manual_code.expose_secret(), token);
        assert!(
            presentation
                .deep_link
                .expose_secret()
                .starts_with("pioneer-dev://activate")
        );
        assert_eq!(
            presentation.qr_modules.len(),
            usize::try_from(presentation.qr_width * presentation.qr_width).unwrap()
        );
        assert!(!format!("{presentation:?}").contains(&token));

        let parsed =
            ClientDeviceActivationParseResult::from_request(ClientDeviceActivationParseRequest {
                uri: presentation.deep_link.clone(),
            })
            .unwrap();
        assert_eq!(parsed.gateway_id, gateway_id);
        assert_eq!(
            parsed.gateway_base_url.as_str(),
            "https://gateway.example/pioneer/"
        );
        assert_eq!(parsed.activation_code.expose_secret(), token);
        assert!(!format!("{parsed:?}").contains(&token));
    }
}
