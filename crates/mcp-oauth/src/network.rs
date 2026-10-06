//! Exchange rate policy around the SDK network boundary, not an OAuth protocol
//! implementation. Discovery, registration and token bodies remain owned by rmcp.
use rmcp::transport::auth::{OAuthHttpClient, OAuthHttpClientFuture, OAuthHttpRequest};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;

// rmcp 3.5 exposes no AuthType setter for authorization-code clients. This
// narrow SDK HTTP adapter applies the assigned per-client method, without
// changing AS metadata, grant logic, PKCE, resource binding or HTTP policy.
#[derive(Clone)]
struct ClientAuthentication {
    endpoint: String,
    client_id: String,
    client_secret: String,
}
pub(crate) struct ExchangeBudgetHttpClient {
    delegate: Arc<dyn OAuthHttpClient>,

    cancellation: CancellationToken,
    registration_transient: AtomicBool,
    budget: Mutex<(tokio::time::Instant, u32)>,
    registration: Mutex<
        Option<(
            serde_json::Value,
            rmcp::transport::auth::ClientRegistrationResponse,
        )>,
    >,
}
impl ExchangeBudgetHttpClient {
    pub(crate) fn new(delegate: Arc<dyn OAuthHttpClient>, cancellation: CancellationToken) -> Self {
        Self {
            delegate,

            cancellation,
            registration_transient: AtomicBool::new(false),
            budget: Mutex::new((tokio::time::Instant::now(), 0)),
            registration: Mutex::new(None),
        }
    }
    pub(crate) fn registration_was_transient(&self) -> bool {
        self.registration_transient.load(Ordering::Acquire)
    }
    pub(crate) fn reset_registration_failure(&self) {
        self.registration_transient.store(false, Ordering::Release);
    }
    pub(crate) async fn take_registration(
        &self,
    ) -> Option<(
        serde_json::Value,
        rmcp::transport::auth::ClientRegistrationResponse,
    )> {
        self.registration.lock().await.take()
    }
}
impl OAuthHttpClient for ExchangeBudgetHttpClient {
    fn execute(&self, request: OAuthHttpRequest) -> OAuthHttpClientFuture<'_> {
        Box::pin(async move {
            // rmcp returns only OAuthClientConfig from register_client. Retain
            // its full request/response for durable registration restoration;
            // all protocol validation and HTTP policy still belong to the SDK.
            let registration = serde_json::from_slice::<serde_json::Value>(request.request.body())
                .ok()
                .filter(|body| {
                    body.get("redirect_uris").is_some() && body.get("client_name").is_some()
                });
            if request.request.method() == "POST" {
                let mut budget = self.budget.lock().await;
                if budget.0.elapsed() >= Duration::from_secs(60) {
                    *budget = (tokio::time::Instant::now(), 0);
                }
                // One connection's registration, code exchanges and refreshes
                // cannot produce an unbounded burst across reconnect and tools.
                if budget.1 >= 8 {
                    if registration.is_some() {
                        self.registration_transient.store(true, Ordering::Release);
                    }
                    return Err(Box::new(std::io::Error::other(
                        "OAuth exchanges temporarily rate limited",
                    )) as _);
                }
                budget.1 += 1;
            }
            let result = tokio::select! {
                _=self.cancellation.cancelled()=>Err(Box::new(std::io::Error::other("OAuth connection cancelled")) as _),
                result=self.delegate.execute(request).with_subscriber(tracing::subscriber::NoSubscriber::default())=>result,
            };
            if registration.is_some() {
                let transient = result.as_ref().map_or(true, |r| {
                    r.status().is_server_error() || r.status().as_u16() == 429
                });
                self.registration_transient
                    .store(transient, Ordering::Release);
            }
            let response = result?;
            if response.status().is_success() {
                if let Some(request) = registration {
                    if let Ok(registration) = serde_json::from_slice(response.body()) {
                        *self.registration.lock().await = Some((request, registration));
                    }
                }
            }
            Ok(response)
        })
    }
}

/// Per-manager authentication: changing or preparing a registration must never
/// mutate the HTTP authentication of an older manager still serving a request.
pub(crate) struct ClientAuthenticationHttpClient {
    delegate: Arc<ExchangeBudgetHttpClient>,
    client_auth: std::sync::RwLock<Option<ClientAuthentication>>,
}
impl ClientAuthenticationHttpClient {
    pub(crate) fn new(delegate: Arc<ExchangeBudgetHttpClient>) -> Self {
        Self {
            delegate,
            client_auth: std::sync::RwLock::new(None),
        }
    }
    pub(crate) fn configure_authentication(
        &self,
        registration: &crate::store::Registration,
        metadata: &rmcp::transport::auth::AuthorizationMetadata,
    ) -> Result<(), rmcp::transport::auth::AuthError> {
        let assigned = registration
            .registration_response
            .as_ref()
            .and_then(|response| response.additional_fields.get("token_endpoint_auth_method"))
            .map(|value| {
                value.as_str().ok_or_else(|| {
                    rmcp::transport::auth::AuthError::RegistrationFailed(
                        "Unsupported OAuth client authentication method".into(),
                    )
                })
            })
            .transpose()?;
        let method = assigned
            .or(registration.token_endpoint_auth_method.as_deref())
            .unwrap_or(if registration.client_secret.is_some() {
                "client_secret_basic"
            } else {
                "none"
            });
        let supported = metadata
            .additional_fields
            .get("token_endpoint_auth_methods_supported")
            .and_then(serde_json::Value::as_array);
        if !matches!(
            method,
            "none" | "client_secret_basic" | "client_secret_post"
        ) || (method == "none") != registration.client_secret.is_none()
            || supported
                .is_some_and(|methods| !methods.iter().any(|value| value.as_str() == Some(method)))
            || (supported.is_none() && method == "client_secret_post")
        {
            return Err(rmcp::transport::auth::AuthError::RegistrationFailed(
                "Unsupported OAuth client authentication method".into(),
            ));
        }
        *self
            .client_auth
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = if method == "client_secret_post"
        {
            Some(ClientAuthentication {
                // oauth2 constructs its request from a parsed URL too. Compare
                // canonical URLs so an otherwise valid endpoint cannot silently
                // fall back to the SDK's Basic authentication due to spelling.
                endpoint: url::Url::parse(&metadata.token_endpoint)
                    .map_err(|_| {
                        rmcp::transport::auth::AuthError::MetadataError(
                            "OAuth token endpoint unavailable".into(),
                        )
                    })?
                    .to_string(),
                client_id: registration.client_id.clone(),
                client_secret: registration.client_secret.clone().unwrap(),
            })
        } else {
            None
        };
        Ok(())
    }
}
impl OAuthHttpClient for ClientAuthenticationHttpClient {
    fn execute(&self, mut request: OAuthHttpRequest) -> OAuthHttpClientFuture<'_> {
        Box::pin(async move {
            let auth = self
                .client_auth
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if let Some(auth) = auth.filter(|auth| {
                request.request.method() == "POST"
                    && request.request.uri().to_string() == auth.endpoint
            }) {
                let mut form = url::form_urlencoded::Serializer::new(String::new());
                for (name, value) in url::form_urlencoded::parse(request.request.body()) {
                    if name != "client_id" && name != "client_secret" {
                        form.append_pair(&name, &value);
                    }
                }
                form.append_pair("client_id", &auth.client_id)
                    .append_pair("client_secret", &auth.client_secret);
                *request.request.body_mut() = form.finish().into_bytes();
                request.request.headers_mut().remove("authorization");
                request.request.headers_mut().remove("content-length");
            }

            self.delegate.execute(request).await
        })
    }
}
