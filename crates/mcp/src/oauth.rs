//! Lower-layer seam: the lifecycle owner supplies a shared rmcp client.
use crate::{McpRuntimeError, McpRuntimeErrorKind, McpServerInstallation};
use async_trait::async_trait;
use futures_util::{StreamExt, stream::BoxStream};
use http::{HeaderName, HeaderValue};
use rmcp::transport::{
    auth::{AuthClient, AuthError},
    streamable_http_client::{
        AuthRequiredError, InsufficientScopeError, StreamableHttpClient, StreamableHttpError,
        StreamableHttpPostResponse,
    },
};
use std::{collections::HashMap, sync::Arc};
use tracing::instrument::WithSubscriber;

#[derive(Debug)]
pub(crate) enum ManagedHttpError {
    Network(reqwest_0_13::Error),
    Forbidden,
    OAuth(McpRuntimeError),
}
impl std::fmt::Display for ManagedHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Network(e) => e.fmt(f),
            Self::Forbidden => f.write_str("MCP server denied access (403)"),
            Self::OAuth(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for ManagedHttpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Network(e) => Some(e),
            Self::Forbidden => None,
            Self::OAuth(error) => Some(error),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthFailureCause {
    pub generation: String,
    pub revision: u64,
}

pub type OAuthHttpClient = AuthClient<reqwest_0_13::Client>;

#[async_trait]
pub trait McpOAuthProvider: Send + Sync {
    async fn client(
        &self,
        id: &str,
        installation: &McpServerInstallation,
    ) -> Result<Option<OAuthHttpClient>, McpRuntimeError>;
    async fn challenge(&self, id: &str, challenge: &str, insufficient_scope: bool);
    async fn authorization_lost(&self, id: &str);
    async fn connection_established(
        &self,
        _id: &str,
        _installation: &McpServerInstallation,
        _authorized: bool,
    ) {
    }
    async fn transient_failure_from_session(
        &self,
        _id: &str,
        _installation: &McpServerInstallation,
        _client: Option<&OAuthHttpClient>,
    ) -> Option<OAuthFailureCause> {
        None
    }
    async fn challenge_from_session(
        &self,
        id: &str,
        _installation: &McpServerInstallation,
        _client: Option<&OAuthHttpClient>,
        challenge: &str,
        insufficient_scope: bool,
    ) {
        self.challenge(id, challenge, insufficient_scope).await;
    }
}

pub fn oauth_runtime_error(error: &AuthError) -> McpRuntimeError {
    let mut result = match error {
        AuthError::AuthorizationRequired | AuthError::TokenExpired => {
            McpRuntimeError::auth_required("OAuth sign-in required")
        }
        AuthError::TokenRefreshRejected(_) => {
            McpRuntimeError::auth_required("OAuth refresh token rejected; sign in again")
        }
        AuthError::InsufficientScope { .. } => {
            McpRuntimeError::auth_required("OAuth consent required for additional permissions")
        }
        AuthError::CredentialStoreError(_) => {
            McpRuntimeError::failed("OAuth credential storage unavailable")
        }
        AuthError::TokenRefreshFailed(_) | AuthError::HttpError(_) => {
            McpRuntimeError::failed("OAuth provider temporarily unavailable")
        }
        AuthError::RegistrationFailed(message)
            if message == "Stored redirect URI is unavailable on this device" =>
        {
            McpRuntimeError::failed("OAuth callback address does not match the saved registration")
        }
        AuthError::RegistrationFailed(message)
            if message == "Unsupported OAuth client authentication method" =>
        {
            McpRuntimeError::failed("OAuth client authentication method unsupported")
        }
        AuthError::RegistrationFailed(_) => McpRuntimeError::failed(
            "OAuth client registration unavailable; configure a registered client_id",
        ),
        AuthError::NoAuthorizationSupport => {
            McpRuntimeError::failed("Server did not publish OAuth metadata")
        }
        AuthError::AuthorizationServerMismatch { .. }
        | AuthError::AuthorizationServerMissingIssuer { .. } => {
            McpRuntimeError::auth_required("OAuth issuer validation failed")
        }
        AuthError::PkceUnsupported => {
            McpRuntimeError::failed("OAuth provider does not support PKCE S256")
        }
        _ => McpRuntimeError::failed("OAuth authorization failed"),
    };
    result.kind = match error {
        AuthError::TokenRefreshRejected(_) => McpRuntimeErrorKind::RefreshRejected,
        AuthError::InsufficientScope { .. } => McpRuntimeErrorKind::InsufficientScope,
        AuthError::TokenRefreshFailed(_) | AuthError::HttpError(_) => {
            McpRuntimeErrorKind::TransientRefresh
        }
        AuthError::CredentialStoreError(_) => McpRuntimeErrorKind::CredentialStore,
        _ => result.kind,
    };
    result
}

/// Capture challenges before the SDK erases transport details, and prevent raw
/// provider bodies/URLs from reaching SDK diagnostics or application logs.
#[derive(Clone)]
pub(crate) struct ManagedHttpClient {
    pub plain: reqwest_0_13::Client,
    pub authorized: Option<OAuthHttpClient>,
    pub owner: Option<Arc<dyn McpOAuthProvider>>,
    pub id: String,
    pub installation: McpServerInstallation,
}
impl ManagedHttpClient {
    async fn checked<T>(
        &self,
        result: Result<T, StreamableHttpError<reqwest_0_13::Error>>,
    ) -> Result<T, StreamableHttpError<ManagedHttpError>> {
        use StreamableHttpError::*;
        match result {
            Err(StreamableHttpError::AuthRequired(e)) => {
                if let Some(owner) = &self.owner {
                    owner
                        .challenge_from_session(
                            &self.id,
                            &self.installation,
                            self.authorized.as_ref(),
                            &e.www_authenticate_header,
                            false,
                        )
                        .await;
                }
                Err(StreamableHttpError::AuthRequired(AuthRequiredError::new(
                    "Bearer".into(),
                )))
            }
            Err(StreamableHttpError::InsufficientScope(e)) => {
                let challenge = rmcp::transport::auth::WWWAuthenticateParams::parse(
                    &e.www_authenticate_header,
                    &url::Url::parse("http://127.0.0.1").expect("static URL"),
                );
                if !challenge.is_insufficient_scope() {
                    return Err(Client(ManagedHttpError::Forbidden));
                }
                if let Some(owner) = &self.owner {
                    owner
                        .challenge_from_session(
                            &self.id,
                            &self.installation,
                            self.authorized.as_ref(),
                            &e.www_authenticate_header,
                            true,
                        )
                        .await;
                }
                Err(StreamableHttpError::InsufficientScope(
                    InsufficientScopeError::new("Bearer error=insufficient_scope".into(), None),
                ))
            }
            Err(StreamableHttpError::Auth(e)) => {
                if matches!(
                    e,
                    AuthError::AuthorizationRequired
                        | AuthError::TokenExpired
                        | AuthError::TokenRefreshRejected(_)
                ) {
                    if let Some(owner) = &self.owner {
                        owner
                            .challenge_from_session(
                                &self.id,
                                &self.installation,
                                self.authorized.as_ref(),
                                "Bearer",
                                false,
                            )
                            .await;
                    }
                }
                if matches!(
                    e,
                    AuthError::TokenRefreshFailed(_)
                        | AuthError::HttpError(_)
                        | AuthError::CredentialStoreError(_)
                ) {
                    if let Some(owner) = &self.owner {
                        let cause = owner
                            .transient_failure_from_session(
                                &self.id,
                                &self.installation,
                                self.authorized.as_ref(),
                            )
                            .await;
                        let mut runtime = oauth_runtime_error(&e);
                        runtime.oauth_failure = cause;
                        return Err(Client(ManagedHttpError::OAuth(runtime)));
                    }
                }
                let message = oauth_runtime_error(&e).message;
                let safe = match e {
                    AuthError::AuthorizationRequired | AuthError::TokenExpired => {
                        AuthError::AuthorizationRequired
                    }
                    AuthError::TokenRefreshRejected(_) => AuthError::TokenRefreshRejected(message),
                    AuthError::CredentialStoreError(_) => AuthError::CredentialStoreError(message),
                    AuthError::TokenRefreshFailed(_) | AuthError::HttpError(_) => {
                        AuthError::TokenRefreshFailed(message)
                    }
                    _ => AuthError::InternalError(message),
                };
                Err(StreamableHttpError::Auth(safe))
            }
            Err(StreamableHttpError::UnexpectedServerResponse(_)) => {
                Err(StreamableHttpError::UnexpectedServerResponse(
                    "HTTP server rejected the MCP request".into(),
                ))
            }
            Err(Client(e)) if e.status() == Some(reqwest_0_13::StatusCode::FORBIDDEN) => {
                Err(Client(ManagedHttpError::Forbidden))
            }
            Err(Client(e)) => Err(Client(ManagedHttpError::Network(e.without_url()))),
            Ok(value) => Ok(value),
            Err(Sse(_)) if self.authorized.is_some() => Err(UnexpectedServerResponse(
                "MCP stream decoding failed".into(),
            )),
            Err(Sse(e)) => Err(Sse(e)),
            Err(Io(e)) => Err(Io(e)),
            Err(UnexpectedEndOfStream) => Err(UnexpectedEndOfStream),
            Err(UnexpectedContentType(_)) => Err(UnexpectedContentType(None)),
            Err(ServerDoesNotSupportSse) => Err(ServerDoesNotSupportSse),
            Err(ServerDoesNotSupportDeleteSession) => Err(ServerDoesNotSupportDeleteSession),
            Err(TokioJoinError(e)) => Err(TokioJoinError(e)),
            Err(Deserialize(_)) if self.authorized.is_some() => Err(UnexpectedServerResponse(
                "MCP response decoding failed".into(),
            )),
            Err(Deserialize(e)) => Err(Deserialize(e)),
            Err(TransportChannelClosed) => Err(TransportChannelClosed),
            Err(MissingSessionIdInResponse) => Err(MissingSessionIdInResponse),
            Err(ReservedHeaderConflict(_)) => {
                Err(ReservedHeaderConflict("reserved MCP header".into()))
            }
            Err(SessionExpired) => Err(SessionExpired),
            Err(SessionRecoveryTimeout) => Err(SessionRecoveryTimeout),
            Err(ControlRequestTimeout) => Err(ControlRequestTimeout),
            Err(_) => Err(UnexpectedServerResponse("MCP transport failed".into())),
        }
    }
}

fn safe_oauth_stream(
    stream: BoxStream<'static, Result<sse_stream::Sse, sse_stream::Error>>,
) -> BoxStream<'static, Result<sse_stream::Sse, sse_stream::Error>> {
    Box::pin(stream.map(|event| match event {
        Ok(mut event) => {
            if let Some(data) = &event.data {
                match serde_json::from_str::<rmcp::model::ServerJsonRpcMessage>(data) {
                    Ok(rmcp::model::ServerJsonRpcMessage::Error(mut error)) => {
                        error.error.message = "MCP server rejected the request".into();
                        error.error.data = None;
                        event.data =
                            serde_json::to_string(&rmcp::model::ServerJsonRpcMessage::Error(error))
                                .ok();
                    }
                    Ok(_) => {}
                    Err(_) if !data.is_empty() => {
                        return Err(sse_stream::Error::Body(Box::new(std::io::Error::other(
                            "MCP stream decoding failed",
                        ))));
                    }
                    Err(_) => {}
                }
            }
            Ok(event)
        }
        Err(_) => Err(sse_stream::Error::Body(Box::new(std::io::Error::other(
            "MCP stream decoding failed",
        )))),
    }))
}

impl StreamableHttpClient for ManagedHttpClient {
    type Error = ManagedHttpError;
    async fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        auth_token: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), StreamableHttpError<Self::Error>> {
        let result = async {
            match &self.authorized {
                Some(c) => {
                    c.delete_session(uri, session_id, auth_token, custom_headers)
                        .await
                }
                None => {
                    self.plain
                        .delete_session(uri, session_id, auth_token, custom_headers)
                        .await
                }
            }
        }
        .with_subscriber(tracing::subscriber::NoSubscriber::default())
        .await;
        self.checked(result).await
    }
    async fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        auth_token: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<
        BoxStream<'static, Result<sse_stream::Sse, sse_stream::Error>>,
        StreamableHttpError<Self::Error>,
    > {
        let result = async {
            match &self.authorized {
                Some(c) => {
                    c.get_stream(uri, session_id, last_event_id, auth_token, custom_headers)
                        .await
                }
                None => {
                    self.plain
                        .get_stream(uri, session_id, last_event_id, auth_token, custom_headers)
                        .await
                }
            }
        }
        .with_subscriber(tracing::subscriber::NoSubscriber::default())
        .await;
        let stream = self.checked(result).await?;
        Ok(if self.authorized.is_some() {
            safe_oauth_stream(stream)
        } else {
            stream
        })
    }
    async fn post_message(
        &self,
        uri: Arc<str>,
        message: rmcp::model::ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_token: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
        let result = async {
            match &self.authorized {
                Some(c) => {
                    c.post_message(uri, message, session_id, auth_token, custom_headers)
                        .await
                }
                None => {
                    self.plain
                        .post_message(uri, message, session_id, auth_token, custom_headers)
                        .await
                }
            }
        }
        .with_subscriber(tracing::subscriber::NoSubscriber::default())
        .await;
        let result = match result {
            Ok(StreamableHttpPostResponse::Json(mut message, session))
                if self.authorized.is_some() =>
            {
                if let rmcp::model::ServerJsonRpcMessage::Error(error) = &mut message {
                    error.error.message = "MCP server rejected the request".into();
                    error.error.data = None;
                }
                Ok(StreamableHttpPostResponse::Json(message, session))
            }
            Ok(StreamableHttpPostResponse::Sse(stream, session)) if self.authorized.is_some() => {
                Ok(StreamableHttpPostResponse::Sse(
                    safe_oauth_stream(stream),
                    session,
                ))
            }
            other => other,
        };
        self.checked(result).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct CauseProvider {
        notified: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }
    #[async_trait]
    impl McpOAuthProvider for CauseProvider {
        async fn client(
            &self,
            _: &str,
            _: &McpServerInstallation,
        ) -> Result<Option<OAuthHttpClient>, McpRuntimeError> {
            Ok(None)
        }
        async fn challenge(&self, _: &str, _: &str, _: bool) {}
        async fn authorization_lost(&self, _: &str) {}
        async fn transient_failure_from_session(
            &self,
            _: &str,
            _: &McpServerInstallation,
            _: Option<&OAuthHttpClient>,
        ) -> Option<OAuthFailureCause> {
            self.notified.notify_one();
            self.release.notified().await;
            Some(OAuthFailureCause {
                generation: "manager-A".into(),
                revision: 7,
            })
        }
    }
    #[tokio::test]
    async fn notified_failure_cause_survives_delayed_transport_error_delivery() {
        let installation = crate::config::parse_install_config(
            r#"{"mcpServers":{"cause":{"url":"https://example.test/mcp"}}}"#,
            crate::config::InstallParseContext {
                scope_kind: crate::McpScopeKind::Workspace,
                scope_key: "workspace".into(),
                default_enabled: true,
                default_allow_implicit_invocation: false,
            },
        )
        .unwrap()
        .items
        .into_iter()
        .next()
        .unwrap()
        .installation
        .unwrap();
        let owner = Arc::new(CauseProvider {
            notified: Default::default(),
            release: Default::default(),
        });
        let client = ManagedHttpClient {
            plain: reqwest_0_13::Client::new(),
            authorized: None,
            owner: Some(owner.clone()),
            id: "installation".into(),
            installation,
        };
        let task = tokio::spawn(async move {
            client
                .checked::<()>(Err(StreamableHttpError::Auth(
                    AuthError::TokenRefreshFailed("provider-secret-canary".into()),
                )))
                .await
        });
        owner.notified.notified().await;
        assert!(
            !task.is_finished(),
            "provider notification precedes actor error delivery"
        );
        owner.release.notify_one();
        let error = task.await.unwrap().unwrap_err();
        assert!(!error.to_string().contains("canary"));
        match error {
            StreamableHttpError::Client(ManagedHttpError::OAuth(error)) => {
                assert_eq!(error.kind, McpRuntimeErrorKind::TransientRefresh);
                assert_eq!(
                    error.oauth_failure,
                    Some(OAuthFailureCause {
                        generation: "manager-A".into(),
                        revision: 7
                    })
                );
            }
            _ => panic!("typed OAuth failure cause was lost"),
        }
    }
    #[tokio::test]
    async fn stream_errors_remove_provider_payloads_before_sdk_diagnostics() {
        let events = futures_util::stream::iter([
            Ok(sse_stream::Sse { data: Some(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"access-canary","data":{"token":"refresh-canary"}}}"#.into()), ..Default::default() }),
            Ok(sse_stream::Sse { data: Some("malformed-token-canary".into()), ..Default::default() }),
        ]);
        let output = safe_oauth_stream(Box::pin(events))
            .collect::<Vec<_>>()
            .await;
        assert!(!format!("{output:?}").contains("canary"));
        let event = output[0].as_ref().unwrap();
        let json: serde_json::Value = serde_json::from_str(event.data.as_ref().unwrap()).unwrap();
        assert_eq!(json["id"], 1);
        assert_eq!(json["error"]["code"], -32603);
        assert!(json["error"].get("data").is_none());
        assert!(output[1].is_err());
    }
}
