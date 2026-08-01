//! One-shot restricted authentication exchanges.
//!
//! This transport deliberately does not share the normal reconnect worker: a
//! restricted credential is attached to one handshake, one request is sent,
//! one response is accepted, and the socket must then close.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{SinkExt, StreamExt};
use pioneer_protocol::{
    AuthDeviceActivateParams, AuthLogoutResponse, AuthRefreshGrant, AuthRefreshParams,
    AuthSessionGrant, AuthSessionId, AuthSessionRevokeResponse, JSONRPC_VERSION,
    JsonRpcErrorResponse, JsonRpcRequest, JsonRpcResponse, REFRESH_CREDENTIAL_BODY_LEN,
    REFRESH_CREDENTIAL_PREFIX, RequestId, constants::methods,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt, time::Duration};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{Instant, timeout};
use tokio_tungstenite::{
    WebSocketStream, connect_async_with_config,
    tungstenite::{
        Message, client::IntoClientRequest, http::HeaderValue, protocol::WebSocketConfig,
    },
};
use url::Url;
use zeroize::Zeroizing;

const MAX_AUTH_EXCHANGE_REQUEST_BYTES: usize = 64 * 1024;
const MAX_AUTH_EXCHANGE_RESPONSE_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthExchangeErrorKind {
    InvalidEndpoint,
    CredentialMethodMismatch,
    /// The transport failed before the JSON-RPC request was dispatched.
    ///
    /// Retrying a one-use refresh credential is safe for this kind because
    /// the Gateway could not have rotated it yet.
    TransportBeforeRequest,
    Timeout,
    Transport,
    Protocol,
    Server,
}

impl AuthExchangeErrorKind {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidEndpoint => "invalid_auth_endpoint",
            Self::CredentialMethodMismatch => "auth_credential_method_mismatch",
            Self::TransportBeforeRequest => "auth_exchange_transport_before_request",
            Self::Timeout => "auth_exchange_timeout",
            Self::Transport => "auth_exchange_transport_failed",
            Self::Protocol => "auth_exchange_protocol_failed",
            Self::Server => "auth_exchange_server_failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthExchangeError {
    pub kind: AuthExchangeErrorKind,
    pub code: Option<String>,
    pub message: String,
}

impl AuthExchangeError {
    fn new(kind: AuthExchangeErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            code: None,
            message: message.into(),
        }
    }

    fn server(response: JsonRpcErrorResponse) -> Self {
        let code = response
            .error
            .data
            .as_ref()
            .and_then(|data| data.get("code"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        Self {
            kind: AuthExchangeErrorKind::Server,
            code,
            // The peer controls the JSON-RPC message. It must never become a
            // diagnostics or UI exfiltration path for the credential that was
            // sent in the handshake (a malicious peer can simply echo it).
            // Stable machine codes remain available for lifecycle decisions.
            message: "Gateway rejected the authentication exchange".to_owned(),
        }
    }
}

impl fmt::Display for AuthExchangeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(code) = &self.code {
            write!(formatter, "auth exchange failed ({code}): {}", self.message)
        } else {
            write!(formatter, "auth exchange failed: {}", self.message)
        }
    }
}

impl std::error::Error for AuthExchangeError {}

#[derive(Debug, Clone)]
pub struct AuthExchangeClient {
    timeout: Duration,
}

impl AuthExchangeClient {
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    pub async fn refresh(
        &self,
        address: &str,
        credential: &str,
        params: AuthRefreshParams,
    ) -> Result<AuthRefreshGrant, AuthExchangeError> {
        if !is_refresh_credential(credential) {
            return Err(AuthExchangeError::new(
                AuthExchangeErrorKind::CredentialMethodMismatch,
                "refresh requires a refresh credential",
            ));
        }
        self.exchange(address, credential, methods::AUTH_REFRESH, &params)
            .await
    }

    pub async fn activate_device(
        &self,
        address: &str,
        credential: &str,
        params: AuthDeviceActivateParams,
    ) -> Result<AuthSessionGrant, AuthExchangeError> {
        let canonical = Zeroizing::new(
            pioneer_protocol::normalize_device_activation_code(credential).map_err(|_| {
                AuthExchangeError::new(
                    AuthExchangeErrorKind::CredentialMethodMismatch,
                    "device activation requires an activation code",
                )
            })?,
        );
        self.exchange(
            address,
            canonical.as_str(),
            methods::AUTH_DEVICE_ACTIVATE,
            &params,
        )
        .await
    }

    pub async fn cleanup_session_once(
        &self,
        address: &str,
        access_credential: &str,
        session_id: AuthSessionId,
    ) -> Result<AuthSessionRevokeResponse, AuthExchangeError> {
        if classify_jwt_credential(access_credential) != Some(JwtCredentialClass::AccessV2) {
            return Err(AuthExchangeError::new(
                AuthExchangeErrorKind::CredentialMethodMismatch,
                "session cleanup requires an access credential",
            ));
        }
        // Cleanup must be scoped by the access credential itself. Using
        // auth/session/revoke with a response-provided target could revoke a
        // peer device if a malformed grant carried the wrong session ID.
        let response: AuthLogoutResponse = self
            .request_once(
                address,
                access_credential,
                methods::AUTH_LOGOUT,
                &serde_json::json!({}),
            )
            .await?;
        if response.session_id != session_id {
            return Err(AuthExchangeError::new(
                AuthExchangeErrorKind::Protocol,
                "cleanup response session identity mismatch",
            ));
        }
        Ok(AuthSessionRevokeResponse {
            session_id: response.session_id,
            revoked: response.revoked,
        })
    }

    async fn exchange<P, R>(
        &self,
        address: &str,
        credential: &str,
        method: &str,
        params: &P,
    ) -> Result<R, AuthExchangeError>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let (stream, remaining) = self.connect_with_credential(address, credential).await?;
        exchange_over_stream(stream, method, params, remaining).await
    }

    async fn request_once<P, R>(
        &self,
        address: &str,
        credential: &str,
        method: &str,
        params: &P,
    ) -> Result<R, AuthExchangeError>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let (stream, remaining) = self.connect_with_credential(address, credential).await?;
        request_once_over_stream(stream, method, params, remaining).await
    }

    async fn connect_with_credential(
        &self,
        address: &str,
        credential: &str,
    ) -> Result<(GatewayAuthWebSocket, Duration), AuthExchangeError> {
        let url = normalize_auth_ws_url(address)?;
        let mut request = url.into_client_request().map_err(|_| {
            AuthExchangeError::new(
                AuthExchangeErrorKind::InvalidEndpoint,
                "failed to prepare Gateway WebSocket request",
            )
        })?;
        let authorization_value = Zeroizing::new(format!("Bearer {credential}"));
        let authorization = HeaderValue::from_str(authorization_value.as_str()).map_err(|_| {
            AuthExchangeError::new(
                AuthExchangeErrorKind::CredentialMethodMismatch,
                "credential cannot be represented in an Authorization header",
            )
        })?;
        request.headers_mut().insert("authorization", authorization);
        let deadline = Instant::now() + self.timeout;
        let websocket_config = WebSocketConfig::default()
            .max_message_size(Some(MAX_AUTH_EXCHANGE_RESPONSE_BYTES))
            .max_frame_size(Some(MAX_AUTH_EXCHANGE_RESPONSE_BYTES));
        let (stream, _) = timeout(
            self.timeout,
            connect_async_with_config(request, Some(websocket_config), false),
        )
        .await
        .map_err(|_| {
            AuthExchangeError::new(
                AuthExchangeErrorKind::TransportBeforeRequest,
                "Gateway authentication handshake timed out",
            )
        })?
        .map_err(|error| {
            AuthExchangeError::new(
                AuthExchangeErrorKind::TransportBeforeRequest,
                format!("Gateway connection failed: {error}"),
            )
        })?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(AuthExchangeError::new(
                AuthExchangeErrorKind::TransportBeforeRequest,
                "Gateway authentication exchange timed out",
            ));
        }
        Ok((stream, remaining))
    }
}

type GatewayAuthWebSocket =
    WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub fn normalize_auth_ws_url(address: &str) -> Result<String, AuthExchangeError> {
    let trimmed = address.trim();
    if trimmed.is_empty() || trimmed.len() > 2_048 {
        return Err(AuthExchangeError::new(
            AuthExchangeErrorKind::InvalidEndpoint,
            "Gateway endpoint is empty",
        ));
    }
    let normalized = if let Some(rest) = trimmed.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if trimmed.contains("://") {
        trimmed.to_owned()
    } else {
        format!("ws://{trimmed}")
    };
    let parsed = Url::parse(&normalized).map_err(|_| {
        AuthExchangeError::new(
            AuthExchangeErrorKind::InvalidEndpoint,
            "Gateway endpoint is invalid",
        )
    })?;
    parsed.host_str().ok_or_else(|| {
        AuthExchangeError::new(
            AuthExchangeErrorKind::InvalidEndpoint,
            "Gateway endpoint has no host",
        )
    })?;
    if !parsed.username().is_empty() || parsed.password().is_some() || parsed.fragment().is_some() {
        return Err(AuthExchangeError::new(
            AuthExchangeErrorKind::InvalidEndpoint,
            "auth transport endpoint must not contain user credentials or a fragment",
        ));
    }
    match parsed.scheme() {
        "ws" | "wss" => Ok(normalized),
        _ => Err(AuthExchangeError::new(
            AuthExchangeErrorKind::InvalidEndpoint,
            "auth transport requires ws or wss",
        )),
    }
}

fn is_refresh_credential(value: &str) -> bool {
    let Some(body) = value.strip_prefix(REFRESH_CREDENTIAL_PREFIX) else {
        return false;
    };
    body.len() == REFRESH_CREDENTIAL_BODY_LEN
        && body
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JwtCredentialClass {
    AccessV2,
}

#[derive(serde::Deserialize)]
struct JwtClassifierClaims {
    #[serde(default)]
    ver: Option<u8>,
    #[serde(default)]
    typ: Option<String>,
    #[serde(default)]
    purpose: Option<String>,
}

fn classify_jwt_credential(value: &str) -> Option<JwtCredentialClass> {
    const MAX_CREDENTIAL_BYTES: usize = 8 * 1024;
    if value.is_empty() || value.len() > MAX_CREDENTIAL_BYTES {
        return None;
    }
    let mut segments = value.split('.');
    let _header = segments.next()?;
    let payload = segments.next()?;
    let _signature = segments.next()?;
    if segments.next().is_some() {
        return None;
    }
    let payload = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims: JwtClassifierClaims = serde_json::from_slice(payload.as_slice()).ok()?;
    match (claims.ver, claims.typ.as_deref(), claims.purpose.as_deref()) {
        (Some(2), Some("access"), Some("gateway_access")) => Some(JwtCredentialClass::AccessV2),
        _ => None,
    }
}

async fn exchange_over_stream<S, P, R>(
    stream: WebSocketStream<S>,
    method: &str,
    params: &P,
    exchange_timeout: Duration,
) -> Result<R, AuthExchangeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    P: Serialize + ?Sized,
    R: DeserializeOwned,
{
    request_response_over_stream(stream, method, params, exchange_timeout, true).await
}

async fn request_once_over_stream<S, P, R>(
    stream: WebSocketStream<S>,
    method: &str,
    params: &P,
    exchange_timeout: Duration,
) -> Result<R, AuthExchangeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    P: Serialize + ?Sized,
    R: DeserializeOwned,
{
    request_response_over_stream(stream, method, params, exchange_timeout, false).await
}

async fn request_response_over_stream<S, P, R>(
    mut stream: WebSocketStream<S>,
    method: &str,
    params: &P,
    exchange_timeout: Duration,
    require_server_close: bool,
) -> Result<R, AuthExchangeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    P: Serialize + ?Sized,
    R: DeserializeOwned,
{
    timeout(exchange_timeout, async {
        let request_id = RequestId::new(pioneer_protocol::generate_id(
            pioneer_protocol::REQUEST_ID_LEN,
        ))
        .map_err(|_| {
            AuthExchangeError::new(AuthExchangeErrorKind::Protocol, "invalid request id")
        })?;
        let request = JsonRpcRequest {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            id: request_id.clone(),
            method: method.to_owned(),
            params: Some(serde_json::to_value(params).map_err(|_| {
                AuthExchangeError::new(
                    AuthExchangeErrorKind::Protocol,
                    "failed to encode auth request",
                )
            })?),
        };
        let payload = serde_json::to_string(&request).map_err(|_| {
            AuthExchangeError::new(
                AuthExchangeErrorKind::Protocol,
                "failed to encode auth request",
            )
        })?;
        if payload.len() > MAX_AUTH_EXCHANGE_REQUEST_BYTES {
            return Err(AuthExchangeError::new(
                AuthExchangeErrorKind::Protocol,
                "auth request exceeds the restricted transport limit",
            ));
        }
        stream
            .send(Message::Text(payload.into()))
            .await
            .map_err(|_| {
                AuthExchangeError::new(
                    AuthExchangeErrorKind::Transport,
                    "failed to send auth request",
                )
            })?;

        const MAX_IGNORED_NOTIFICATIONS: usize = 16;
        let mut ignored_notifications = 0usize;
        let value = loop {
            match stream.next().await {
                Some(Ok(Message::Text(payload))) => {
                    if payload.len() > MAX_AUTH_EXCHANGE_RESPONSE_BYTES {
                        return Err(AuthExchangeError::new(
                            AuthExchangeErrorKind::Protocol,
                            "auth response exceeds the restricted transport limit",
                        ));
                    }
                    let value: serde_json::Value =
                        serde_json::from_str(payload.as_ref()).map_err(|_| {
                            AuthExchangeError::new(
                                AuthExchangeErrorKind::Protocol,
                                "malformed auth response",
                            )
                        })?;
                    if !require_server_close
                        && value.get("id").is_none()
                        && value.get("method").is_some()
                    {
                        ignored_notifications += 1;
                        if ignored_notifications > MAX_IGNORED_NOTIFICATIONS {
                            return Err(AuthExchangeError::new(
                                AuthExchangeErrorKind::Protocol,
                                "too many notifications before auth response",
                            ));
                        }
                        continue;
                    }
                    break value;
                }
                Some(Ok(Message::Ping(payload))) => {
                    stream.send(Message::Pong(payload)).await.map_err(|_| {
                        AuthExchangeError::new(
                            AuthExchangeErrorKind::Transport,
                            "failed to answer auth transport ping",
                        )
                    })?;
                }
                Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {}
                Some(Ok(Message::Close(_))) | None => {
                    return Err(AuthExchangeError::new(
                        AuthExchangeErrorKind::Transport,
                        "auth connection closed before a response",
                    ));
                }
                Some(Ok(Message::Binary(_))) => {
                    return Err(AuthExchangeError::new(
                        AuthExchangeErrorKind::Protocol,
                        "binary auth response is forbidden",
                    ));
                }
                Some(Err(_)) => {
                    return Err(AuthExchangeError::new(
                        AuthExchangeErrorKind::Transport,
                        "failed to read auth response",
                    ));
                }
            }
        };
        let outcome = if value.get("error").is_some() {
            let response: JsonRpcErrorResponse = serde_json::from_value(value).map_err(|_| {
                AuthExchangeError::new(
                    AuthExchangeErrorKind::Protocol,
                    "malformed auth error response",
                )
            })?;
            if response.id.as_ref() != Some(&request_id) || response.jsonrpc != JSONRPC_VERSION {
                return Err(AuthExchangeError::new(
                    AuthExchangeErrorKind::Protocol,
                    "auth response identity mismatch",
                ));
            }
            Err(AuthExchangeError::server(response))
        } else {
            let response: JsonRpcResponse = serde_json::from_value(value).map_err(|_| {
                AuthExchangeError::new(
                    AuthExchangeErrorKind::Protocol,
                    "malformed auth success response",
                )
            })?;
            if response.id != request_id || response.jsonrpc != JSONRPC_VERSION {
                return Err(AuthExchangeError::new(
                    AuthExchangeErrorKind::Protocol,
                    "auth response identity mismatch",
                ));
            }
            serde_json::from_value(response.result).map_err(|_| {
                AuthExchangeError::new(
                    AuthExchangeErrorKind::Protocol,
                    "invalid auth result payload",
                )
            })
        };

        if !require_server_close {
            let _ = stream.send(Message::Close(None)).await;
            return outcome;
        }
        match stream.next().await {
            Some(Ok(Message::Close(_))) | None => outcome,
            Some(Ok(Message::Ping(payload))) => {
                let _ = stream.send(Message::Pong(payload)).await;
                Err(AuthExchangeError::new(
                    AuthExchangeErrorKind::Protocol,
                    "auth server did not close after its response",
                ))
            }
            Some(Ok(_)) => Err(AuthExchangeError::new(
                AuthExchangeErrorKind::Protocol,
                "auth server sent more than one response",
            )),
            Some(Err(_)) => Err(AuthExchangeError::new(
                AuthExchangeErrorKind::Transport,
                "auth transport failed before close",
            )),
        }
    })
    .await
    .map_err(|_| {
        AuthExchangeError::new(AuthExchangeErrorKind::Timeout, "auth exchange timed out")
    })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        AuthDeviceSnapshot, AuthGatewaySnapshot, AuthPrincipalSnapshot, AuthSecretString,
        AuthSessionSnapshot, AuthSessionStatus, ClientKind, CredentialStorageOrder, DeviceId,
        DeviceStatus, GatewayId, PrincipalId, PrincipalKind, TokenFamilyId,
    };
    use tokio::io::DuplexStream;
    use tokio_tungstenite::tungstenite::protocol::Role;

    fn classified_jwt(payload: serde_json::Value) -> String {
        format!(
            "{}.{}.signature",
            URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256"}"#),
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap())
        )
    }

    fn grant() -> AuthSessionGrant {
        AuthSessionGrant {
            gateway: AuthGatewaySnapshot {
                id: GatewayId::new("G00000000000000000001").unwrap(),
            },
            principal: AuthPrincipalSnapshot {
                id: PrincipalId::new("P00000000000000000001").unwrap(),
                kind: PrincipalKind::Superuser,
                display_name: "Owner".to_owned(),
                nickname: "owner".to_owned(),
            },
            device: AuthDeviceSnapshot {
                id: DeviceId::new("D00000000000000000001").unwrap(),
                installation_id: "desktop".to_owned(),
                display_name: "Desktop".to_owned(),
                client_kind: ClientKind::Desktop,
                status: DeviceStatus::Active,
            },
            session: AuthSessionSnapshot {
                id: pioneer_protocol::AuthSessionId::new("S00000000000000000001").unwrap(),
                device_id: DeviceId::new("D00000000000000000001").unwrap(),
                token_family_id: TokenFamilyId::new("F00000000000000000001").unwrap(),
                status: AuthSessionStatus::Active,
                refresh_generation: 0,
                refresh_expires_at_unix: 100,
            },
            access_token: AuthSecretString::new("access-secret"),
            access_expires_at_unix: 50,
            refresh_token: AuthSecretString::new("refresh-secret"),
            refresh_expires_at_unix: 100,
            refresh_generation: 0,
            auth_protocol_version: pioneer_protocol::DEVICE_SESSION_AUTH_PROTOCOL_VERSION,
            credential_storage_order: CredentialStorageOrder::PersistRefreshBeforeActivatingAccess,
        }
    }

    async fn streams() -> (WebSocketStream<DuplexStream>, WebSocketStream<DuplexStream>) {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
        let server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        (client, server)
    }

    async fn request_id(server: &mut WebSocketStream<DuplexStream>) -> RequestId {
        let Message::Text(payload) = server.next().await.unwrap().unwrap() else {
            panic!("expected text request")
        };
        serde_json::from_str::<JsonRpcRequest>(&payload).unwrap().id
    }

    #[tokio::test]
    async fn one_response_then_close_returns_secret_directly() {
        let (client, mut server) = streams().await;
        let server_task = tokio::spawn(async move {
            let id = request_id(&mut server).await;
            let response = JsonRpcResponse::from_result(id, &grant()).unwrap();
            server
                .send(Message::Text(
                    serde_json::to_string(&response).unwrap().into(),
                ))
                .await
                .unwrap();
            server.send(Message::Close(None)).await.unwrap();
        });
        let result: AuthSessionGrant = exchange_over_stream(
            client,
            methods::AUTH_DEVICE_ACTIVATE,
            &serde_json::json!({}),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(result.refresh_token.expose_secret(), "refresh-secret");
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn timeout_transport_loss_and_malformed_response_are_typed() {
        let (client, mut server) = streams().await;
        let timeout_task = tokio::spawn(async move {
            let _ = request_id(&mut server).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        let timeout_error = exchange_over_stream::<_, _, AuthSessionGrant>(
            client,
            methods::AUTH_DEVICE_ACTIVATE,
            &serde_json::json!({}),
            Duration::from_millis(10),
        )
        .await
        .unwrap_err();
        assert_eq!(timeout_error.kind, AuthExchangeErrorKind::Timeout);
        timeout_task.abort();

        let (client, mut server) = streams().await;
        let loss_task = tokio::spawn(async move {
            let _ = request_id(&mut server).await;
            drop(server);
        });
        let loss = exchange_over_stream::<_, _, AuthSessionGrant>(
            client,
            methods::AUTH_DEVICE_ACTIVATE,
            &serde_json::json!({}),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert_eq!(loss.kind, AuthExchangeErrorKind::Transport);
        loss_task.await.unwrap();

        let (client, mut server) = streams().await;
        let malformed_task = tokio::spawn(async move {
            let _ = request_id(&mut server).await;
            server.send(Message::Text("not-json".into())).await.unwrap();
        });
        let malformed = exchange_over_stream::<_, _, AuthSessionGrant>(
            client,
            methods::AUTH_DEVICE_ACTIVATE,
            &serde_json::json!({}),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert_eq!(malformed.kind, AuthExchangeErrorKind::Protocol);
        malformed_task.await.unwrap();
    }

    #[test]
    fn local_auth_exchange_error_codes_are_stable() {
        assert_eq!(
            AuthExchangeErrorKind::InvalidEndpoint.code(),
            "invalid_auth_endpoint"
        );
        assert_eq!(
            AuthExchangeErrorKind::CredentialMethodMismatch.code(),
            "auth_credential_method_mismatch"
        );
        assert_eq!(
            AuthExchangeErrorKind::TransportBeforeRequest.code(),
            "auth_exchange_transport_before_request"
        );
        assert_eq!(
            AuthExchangeErrorKind::Timeout.code(),
            "auth_exchange_timeout"
        );
        assert_eq!(
            AuthExchangeErrorKind::Transport.code(),
            "auth_exchange_transport_failed"
        );
        assert_eq!(
            AuthExchangeErrorKind::Protocol.code(),
            "auth_exchange_protocol_failed"
        );
        assert_eq!(
            AuthExchangeErrorKind::Server.code(),
            "auth_exchange_server_failed"
        );
    }

    #[tokio::test]
    async fn connection_failure_is_known_to_precede_request_dispatch() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let result = AuthExchangeClient::new(Duration::from_secs(1))
            .connect_with_credential(format!("ws://{address}").as_str(), "credential")
            .await;
        let error = match result {
            Ok(_) => panic!("closed local port unexpectedly accepted a WebSocket connection"),
            Err(error) => error,
        };

        assert_eq!(error.kind, AuthExchangeErrorKind::TransportBeforeRequest);
        assert_eq!(error.kind.code(), "auth_exchange_transport_before_request");
    }

    #[tokio::test]
    async fn second_response_frame_is_rejected_and_server_code_is_preserved() {
        let (client, mut server) = streams().await;
        let second_task = tokio::spawn(async move {
            let id = request_id(&mut server).await;
            let response = JsonRpcResponse::from_result(id, &grant()).unwrap();
            let payload = serde_json::to_string(&response).unwrap();
            server
                .send(Message::Text(payload.clone().into()))
                .await
                .unwrap();
            server.send(Message::Text(payload.into())).await.unwrap();
        });
        let second = exchange_over_stream::<_, _, AuthSessionGrant>(
            client,
            methods::AUTH_DEVICE_ACTIVATE,
            &serde_json::json!({}),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert_eq!(second.kind, AuthExchangeErrorKind::Protocol);
        second_task.await.unwrap();

        let (client, mut server) = streams().await;
        let error_task = tokio::spawn(async move {
            let id = request_id(&mut server).await;
            let mut response = JsonRpcErrorResponse::new(
                Some(id),
                -32040,
                "malicious peer echoed prf_raw_fixture_secret",
            );
            response.error.data = Some(serde_json::json!({"code": "session_revoked"}));
            server
                .send(Message::Text(
                    serde_json::to_string(&response).unwrap().into(),
                ))
                .await
                .unwrap();
            server.send(Message::Close(None)).await.unwrap();
        });
        let server_error = exchange_over_stream::<_, _, AuthSessionGrant>(
            client,
            methods::AUTH_DEVICE_ACTIVATE,
            &serde_json::json!({}),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert_eq!(server_error.kind, AuthExchangeErrorKind::Server);
        assert_eq!(server_error.code.as_deref(), Some("session_revoked"));
        assert!(!format!("{server_error:?} {server_error}").contains("prf_raw_fixture_secret"));
        error_task.await.unwrap();
    }

    #[tokio::test]
    async fn one_shot_normal_request_ignores_bounded_notifications_before_response() {
        let (client, mut server) = streams().await;
        let server_task = tokio::spawn(async move {
            let id = request_id(&mut server).await;
            server
                .send(Message::Text(
                    serde_json::json!({
                        "jsonrpc": JSONRPC_VERSION,
                        "method": "auth/access_expiring",
                        "params": {
                            "session_id": "S00000000000000000001",
                            "access_expires_at_unix": 100
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let response = JsonRpcResponse::from_result(
                id,
                &AuthLogoutResponse {
                    session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
                    revoked: true,
                },
            )
            .unwrap();
            server
                .send(Message::Text(
                    serde_json::to_string(&response).unwrap().into(),
                ))
                .await
                .unwrap();
        });

        let response: AuthLogoutResponse = request_once_over_stream(
            client,
            methods::AUTH_LOGOUT,
            &serde_json::json!({}),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(response.revoked);
        server_task.await.unwrap();
    }

    #[test]
    fn auth_endpoint_accepts_plaintext_remote_and_rejects_credential_bearing_urls() {
        assert!(normalize_auth_ws_url("ws://127.0.0.1:17878").is_ok());
        assert!(normalize_auth_ws_url("ws://[::1]:17878").is_ok());
        assert!(normalize_auth_ws_url("localhost:17878").is_ok());
        assert!(normalize_auth_ws_url("wss://gateway.example.test/ws").is_ok());
        assert!(normalize_auth_ws_url("ws://192.0.2.10:17878").is_ok());
        assert!(normalize_auth_ws_url("gateway.example.test:17878").is_ok());
        assert_eq!(
            normalize_auth_ws_url("wss://user:password@gateway.example.test/ws")
                .unwrap_err()
                .kind,
            AuthExchangeErrorKind::InvalidEndpoint
        );
        assert_eq!(
            normalize_auth_ws_url("wss://gateway.example.test/ws#credential")
                .unwrap_err()
                .kind,
            AuthExchangeErrorKind::InvalidEndpoint
        );
    }

    #[test]
    fn direct_auth_calls_reject_cross_purpose_jwts_before_transport() {
        let access = classified_jwt(serde_json::json!({
            "ver": 2,
            "typ": "access",
            "purpose": "gateway_access"
        }));
        let unsupported_legacy = classified_jwt(serde_json::json!({
            "sub": "superuser",
            "role": "superuser"
        }));

        assert_eq!(
            classify_jwt_credential(access.as_str()),
            Some(JwtCredentialClass::AccessV2)
        );
        assert_eq!(classify_jwt_credential(unsupported_legacy.as_str()), None);
        assert_eq!(classify_jwt_credential("prf_not-a-jwt"), None);
    }
}
