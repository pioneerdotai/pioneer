//! Authenticated native HTTP transport for immutable Gateway storage reads.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures_util::{Stream, StreamExt};
use pioneer_protocol::{AuthSecretString, AuthSessionId, GatewayId};
use reqwest::header::{
    AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG,
    IF_NONE_MATCH, IF_RANGE, RANGE,
};
use tokio_util::sync::CancellationToken;
use url::Url;
use zeroize::Zeroize;

#[cfg(test)]
use tokio::sync::Mutex;

use crate::gateway::endpoint::{
    GatewayBaseUrl, GatewayBaseUrlError, PIONEER_PROTOCOL_VERSION, PIONEER_PROTOCOL_VERSION_HEADER,
    canonical_storage_path,
};
use crate::gateway::session_lifecycle::SessionTerminalReason;
use crate::gateway::types::GatewayEndpoint;

const MAX_STORAGE_PATH_BYTES: usize = 2_048;
const MAX_RESPONSE_HEADER_BYTES: usize = 2_048;
const MAX_HTTP2_RESPONSE_HEADER_LIST_BYTES: u32 = 16 * 1024;
const REQUEST_ID_HEADER: &str = "Pioneer-Request-Id";
const RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(30);
const RESPONSE_BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct GatewayHttpAccess {
    pub gateway_base_url: GatewayBaseUrl,
    pub gateway_id: GatewayId,
    pub session_id: AuthSessionId,
    pub generation: u64,
    pub access_expires_at_unix: u64,
    pub access_token: AuthSecretString,
}

impl fmt::Debug for GatewayHttpAccess {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayHttpAccess")
            .field("gateway_base_url", &self.gateway_base_url)
            .field("gateway_id", &self.gateway_id)
            .field("session_id", &self.session_id)
            .field("generation", &self.generation)
            .field("access_expires_at_unix", &self.access_expires_at_unix)
            .field("access_token", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayHttpAuthorityError {
    Terminal(SessionTerminalReason),
    TemporarilyUnavailable,
}

#[async_trait]
pub trait GatewayHttpSessionAuthority: Send + Sync {
    /// Returns the current ephemeral access owned by the existing session
    /// lifecycle. Implementations must not create an HTTP-only token cache.
    async fn current_access(&self) -> Result<GatewayHttpAccess, GatewayHttpAuthorityError>;

    /// Enters the existing serialized refresh lifecycle. The rejected
    /// generation lets the owner avoid rotating a credential that another WS
    /// or HTTP request already refreshed.
    async fn coordinated_refresh(
        &self,
        rejected_generation: u64,
    ) -> Result<GatewayHttpAccess, GatewayHttpAuthorityError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayHttpMethod {
    Get,
    Head,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayHttpRequest {
    method: GatewayHttpMethod,
    storage_path: String,
    range: Option<String>,
    if_none_match: Option<String>,
    if_range: Option<String>,
}

impl GatewayHttpRequest {
    pub fn get(storage_path: impl Into<String>) -> Result<Self, GatewayHttpError> {
        Self::new(GatewayHttpMethod::Get, storage_path.into())
    }

    pub fn head(storage_path: impl Into<String>) -> Result<Self, GatewayHttpError> {
        Self::new(GatewayHttpMethod::Head, storage_path.into())
    }

    fn new(method: GatewayHttpMethod, storage_path: String) -> Result<Self, GatewayHttpError> {
        if storage_path.is_empty() || storage_path.len() > MAX_STORAGE_PATH_BYTES {
            return Err(GatewayHttpError::InvalidStoragePath);
        }
        let canonical =
            canonical_storage_path(storage_path.as_str()).map_err(map_base_url_error)?;
        if canonical.starts_with("storage/views/") {
            return Err(GatewayHttpError::InvalidStoragePath);
        }
        Ok(Self {
            method,
            storage_path: canonical,
            range: None,
            if_none_match: None,
            if_range: None,
        })
    }

    pub fn with_range(mut self, value: impl Into<String>) -> Result<Self, GatewayHttpError> {
        self.range = Some(validate_request_header(value.into())?);
        Ok(self)
    }

    pub fn with_if_none_match(
        mut self,
        value: impl Into<String>,
    ) -> Result<Self, GatewayHttpError> {
        self.if_none_match = Some(validate_request_header(value.into())?);
        Ok(self)
    }

    pub fn with_if_range(mut self, value: impl Into<String>) -> Result<Self, GatewayHttpError> {
        self.if_range = Some(validate_request_header(value.into())?);
        Ok(self)
    }

    #[cfg(test)]
    pub(crate) const fn method(&self) -> GatewayHttpMethod {
        self.method
    }

    #[cfg(test)]
    pub(crate) fn storage_path(&self) -> &str {
        self.storage_path.as_str()
    }

    #[cfg(test)]
    pub(crate) fn range(&self) -> Option<&str> {
        self.range.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn if_range(&self) -> Option<&str> {
        self.if_range.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn if_none_match(&self) -> Option<&str> {
        self.if_none_match.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayHttpResponseHead {
    pub status: u16,
    pub request_id: Option<String>,
    pub etag: Option<String>,
    pub content_length: Option<u64>,
    pub content_range: Option<String>,
    pub content_type: Option<String>,
    pub content_disposition: Option<String>,
}

pub struct GatewayHttpBody {
    stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>, GatewayHttpError>> + Send>>,
    cancellation: CancellationToken,
    idle_timeout: Duration,
}

impl fmt::Debug for GatewayHttpBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayHttpBody")
            .field("bytes", &"[streaming]")
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish()
    }
}

impl GatewayHttpBody {
    pub async fn next_chunk(&mut self) -> Option<Result<Vec<u8>, GatewayHttpError>> {
        tokio::select! {
            _ = self.cancellation.cancelled() => Some(Err(GatewayHttpError::Cancelled)),
            chunk = tokio::time::timeout(self.idle_timeout, self.stream.next()) => {
                match chunk {
                    Ok(chunk) => chunk,
                    Err(_) => Some(Err(GatewayHttpError::Transport)),
                }
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn from_test_chunks(
        chunks: Vec<Result<Vec<u8>, GatewayHttpError>>,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            stream: Box::pin(futures_util::stream::iter(chunks)),
            cancellation,
            idle_timeout: RESPONSE_BODY_IDLE_TIMEOUT,
        }
    }
}

#[derive(Debug)]
pub struct GatewayHttpResponse {
    pub head: GatewayHttpResponseHead,
    pub body: GatewayHttpBody,
}

pub struct BrowserViewUrl(String);

impl BrowserViewUrl {
    pub fn expose_url(&self) -> &str {
        self.0.as_str()
    }

    pub fn resolve(
        gateway_base_url: &GatewayBaseUrl,
        relative_url: &str,
    ) -> Result<Self, GatewayHttpError> {
        if relative_url.len() != 58
            || !relative_url.starts_with("/storage/views/")
            || !relative_url[15..]
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(GatewayHttpError::InvalidStoragePath);
        }
        let url = gateway_base_url
            .storage_url(relative_url)
            .map_err(map_base_url_error)?;
        ensure_same_origin(gateway_base_url, &url)?;
        Ok(Self(url.to_string()))
    }
}

impl fmt::Debug for BrowserViewUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BrowserViewUrl([redacted])")
    }
}

impl Drop for BrowserViewUrl {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayHttpError {
    InvalidEndpoint,
    InvalidStoragePath,
    InvalidHeader,
    GatewayPinMismatch,
    SessionMismatch,
    AuthenticationTerminal(SessionTerminalReason),
    AuthenticationUnavailable,
    Cancelled,
    Transport,
    InvalidResponse,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    RangeNotSatisfiable,
    TooManyRequests,
    ServiceUnavailable,
    Server,
}

impl GatewayHttpError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidEndpoint => "invalid_endpoint",
            Self::InvalidStoragePath => "invalid_storage_path",
            Self::InvalidHeader => "invalid_header",
            Self::GatewayPinMismatch => "gateway_pin_mismatch",
            Self::SessionMismatch => "session_mismatch",
            Self::AuthenticationTerminal(_) => "authentication_terminal",
            Self::AuthenticationUnavailable => "authentication_unavailable",
            Self::Cancelled => "cancelled",
            Self::Transport => "transport_failed",
            Self::InvalidResponse => "invalid_response",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::RangeNotSatisfiable => "range_not_satisfiable",
            Self::TooManyRequests => "too_many_requests",
            Self::ServiceUnavailable => "service_unavailable",
            Self::Server => "server_error",
        }
    }
}

impl fmt::Display for GatewayHttpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for GatewayHttpError {}

#[derive(Clone)]
pub struct GatewayHttpSession {
    gateway_base_url: GatewayBaseUrl,
    pinned_gateway_id: GatewayId,
    session_id: AuthSessionId,
    authority: Arc<dyn GatewayHttpSessionAuthority>,
    executor: Arc<dyn GatewayHttpExecutor>,
}

impl fmt::Debug for GatewayHttpSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayHttpSession")
            .field("gateway_base_url", &self.gateway_base_url)
            .field("pinned_gateway_id", &self.pinned_gateway_id)
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

impl GatewayHttpSession {
    pub fn from_endpoint(
        endpoint: &GatewayEndpoint,
        session_id: AuthSessionId,
        authority: Arc<dyn GatewayHttpSessionAuthority>,
    ) -> Result<Self, GatewayHttpError> {
        let pinned_gateway_id = endpoint
            .server_gateway_id
            .clone()
            .ok_or(GatewayHttpError::InvalidEndpoint)?;
        if endpoint.session_ref.is_none() {
            return Err(GatewayHttpError::InvalidEndpoint);
        }
        Self::new(
            endpoint.gateway_base_url.clone(),
            pinned_gateway_id,
            session_id,
            authority,
        )
    }

    pub fn from_access(
        access: &GatewayHttpAccess,
        authority: Arc<dyn GatewayHttpSessionAuthority>,
    ) -> Result<Self, GatewayHttpError> {
        Self::new(
            access.gateway_base_url.clone(),
            access.gateway_id.clone(),
            access.session_id.clone(),
            authority,
        )
    }

    fn new(
        gateway_base_url: GatewayBaseUrl,
        pinned_gateway_id: GatewayId,
        session_id: AuthSessionId,
        authority: Arc<dyn GatewayHttpSessionAuthority>,
    ) -> Result<Self, GatewayHttpError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .http2_max_header_list_size(MAX_HTTP2_RESPONSE_HEADER_LIST_BYTES)
            .build()
            .map_err(|_| GatewayHttpError::InvalidEndpoint)?;
        Ok(Self {
            gateway_base_url,
            pinned_gateway_id,
            session_id,
            authority,
            executor: Arc::new(ReqwestGatewayHttpExecutor { client }),
        })
    }

    pub fn resolve_view_url(&self, relative_url: &str) -> Result<BrowserViewUrl, GatewayHttpError> {
        BrowserViewUrl::resolve(&self.gateway_base_url, relative_url)
    }

    pub async fn execute(
        &self,
        request: GatewayHttpRequest,
        cancellation: CancellationToken,
    ) -> Result<GatewayHttpResponse, GatewayHttpError> {
        if cancellation.is_cancelled() {
            return Err(GatewayHttpError::Cancelled);
        }
        let mut access = self
            .authority
            .current_access()
            .await
            .map_err(map_authority_error)?;
        self.validate_access(&access)?;
        if access.access_expires_at_unix <= now_unix()? {
            access = self.refresh_access(access.generation).await?;
        }

        let first = self
            .execute_once(&request, &access, cancellation.clone())
            .await?;
        if first.head.status != 401 {
            return classify_response(first);
        }
        drop(first);

        let refreshed = self.refresh_access(access.generation).await?;
        let retry = self
            .execute_once(&request, &refreshed, cancellation)
            .await?;
        classify_response(retry)
    }

    async fn refresh_access(
        &self,
        rejected_generation: u64,
    ) -> Result<GatewayHttpAccess, GatewayHttpError> {
        let current = self
            .authority
            .current_access()
            .await
            .map_err(map_authority_error)?;
        self.validate_access(&current)?;
        if current.generation != rejected_generation && current.access_expires_at_unix > now_unix()?
        {
            return Ok(current);
        }
        let refreshed = self
            .authority
            .coordinated_refresh(rejected_generation)
            .await
            .map_err(map_authority_error)?;
        self.validate_access(&refreshed)?;
        if refreshed.access_expires_at_unix <= now_unix()? {
            return Err(GatewayHttpError::AuthenticationUnavailable);
        }
        Ok(refreshed)
    }

    async fn execute_once(
        &self,
        request: &GatewayHttpRequest,
        access: &GatewayHttpAccess,
        cancellation: CancellationToken,
    ) -> Result<GatewayHttpResponse, GatewayHttpError> {
        self.validate_access(access)?;
        let url = self
            .gateway_base_url
            .storage_url(request.storage_path.as_str())
            .map_err(map_base_url_error)?;
        ensure_same_origin(&self.gateway_base_url, &url)?;
        self.executor
            .execute(
                PreparedGatewayHttpRequest {
                    method: request.method,
                    url,
                    access_token: access.access_token.clone(),
                    range: request.range.clone(),
                    if_none_match: request.if_none_match.clone(),
                    if_range: request.if_range.clone(),
                },
                cancellation,
            )
            .await
    }

    fn validate_access(&self, access: &GatewayHttpAccess) -> Result<(), GatewayHttpError> {
        if access.gateway_base_url != self.gateway_base_url
            || access.gateway_id != self.pinned_gateway_id
        {
            return Err(GatewayHttpError::GatewayPinMismatch);
        }
        if access.session_id != self.session_id {
            return Err(GatewayHttpError::SessionMismatch);
        }
        Ok(())
    }

    #[cfg(test)]
    fn with_executor(
        gateway_base_url: GatewayBaseUrl,
        pinned_gateway_id: GatewayId,
        session_id: AuthSessionId,
        authority: Arc<dyn GatewayHttpSessionAuthority>,
        executor: Arc<dyn GatewayHttpExecutor>,
    ) -> Self {
        Self {
            gateway_base_url,
            pinned_gateway_id,
            session_id,
            authority,
            executor,
        }
    }
}

#[derive(Clone)]
struct PreparedGatewayHttpRequest {
    method: GatewayHttpMethod,
    url: Url,
    access_token: AuthSecretString,
    range: Option<String>,
    if_none_match: Option<String>,
    if_range: Option<String>,
}

impl fmt::Debug for PreparedGatewayHttpRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedGatewayHttpRequest")
            .field("method", &self.method)
            .field("url", &self.url)
            .field("access_token", &"[redacted]")
            .field("range", &self.range)
            .field("if_none_match", &self.if_none_match)
            .field("if_range", &self.if_range)
            .finish()
    }
}

#[async_trait]
trait GatewayHttpExecutor: Send + Sync {
    async fn execute(
        &self,
        request: PreparedGatewayHttpRequest,
        cancellation: CancellationToken,
    ) -> Result<GatewayHttpResponse, GatewayHttpError>;
}

struct ReqwestGatewayHttpExecutor {
    client: reqwest::Client,
}

#[async_trait]
impl GatewayHttpExecutor for ReqwestGatewayHttpExecutor {
    async fn execute(
        &self,
        request: PreparedGatewayHttpRequest,
        cancellation: CancellationToken,
    ) -> Result<GatewayHttpResponse, GatewayHttpError> {
        let method = match request.method {
            GatewayHttpMethod::Get => reqwest::Method::GET,
            GatewayHttpMethod::Head => reqwest::Method::HEAD,
        };
        let mut authorization = reqwest::header::HeaderValue::from_str(
            format!("Bearer {}", request.access_token.expose_secret()).as_str(),
        )
        .map_err(|_| GatewayHttpError::InvalidHeader)?;
        authorization.set_sensitive(true);
        let mut builder = self
            .client
            .request(method, request.url)
            .header(AUTHORIZATION, authorization)
            .header(PIONEER_PROTOCOL_VERSION_HEADER, PIONEER_PROTOCOL_VERSION);
        if let Some(value) = request.range {
            builder = builder.header(RANGE, value);
        }
        if let Some(value) = request.if_none_match {
            builder = builder.header(IF_NONE_MATCH, value);
        }
        if let Some(value) = request.if_range {
            builder = builder.header(IF_RANGE, value);
        }
        let response = tokio::select! {
            _ = cancellation.cancelled() => return Err(GatewayHttpError::Cancelled),
            response = tokio::time::timeout(RESPONSE_HEADER_TIMEOUT, builder.send()) => {
                response
                    .map_err(|_| GatewayHttpError::Transport)?
                    .map_err(|_| GatewayHttpError::Transport)?
            },
        };
        let head = response_head(&response)?;
        let body_stream = response.bytes_stream().map(|chunk| {
            chunk
                .map(|bytes| bytes.to_vec())
                .map_err(|_| GatewayHttpError::Transport)
        });
        Ok(GatewayHttpResponse {
            head,
            body: GatewayHttpBody {
                stream: Box::pin(body_stream),
                cancellation,
                idle_timeout: RESPONSE_BODY_IDLE_TIMEOUT,
            },
        })
    }
}

fn response_head(
    response: &reqwest::Response,
) -> Result<GatewayHttpResponseHead, GatewayHttpError> {
    Ok(GatewayHttpResponseHead {
        status: response.status().as_u16(),
        request_id: bounded_response_header(response, REQUEST_ID_HEADER)?,
        etag: bounded_response_header(response, ETAG.as_str())?,
        content_length: match single_response_header(response, CONTENT_LENGTH.as_str())? {
            Some(value) => Some(
                value
                    .to_str()
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .ok_or(GatewayHttpError::InvalidResponse)?,
            ),
            None => None,
        },
        content_range: bounded_response_header(response, CONTENT_RANGE.as_str())?,
        content_type: bounded_response_header(response, CONTENT_TYPE.as_str())?,
        content_disposition: bounded_response_header(response, CONTENT_DISPOSITION.as_str())?,
    })
}

fn bounded_response_header(
    response: &reqwest::Response,
    name: &str,
) -> Result<Option<String>, GatewayHttpError> {
    let Some(value) = single_response_header(response, name)? else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| GatewayHttpError::InvalidResponse)?;
    if value.len() > MAX_RESPONSE_HEADER_BYTES {
        return Err(GatewayHttpError::InvalidResponse);
    }
    Ok(Some(value.to_owned()))
}

fn single_response_header<'a>(
    response: &'a reqwest::Response,
    name: &str,
) -> Result<Option<&'a reqwest::header::HeaderValue>, GatewayHttpError> {
    let mut values = response.headers().get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(GatewayHttpError::InvalidResponse);
    }
    Ok(Some(value))
}

fn classify_response(
    response: GatewayHttpResponse,
) -> Result<GatewayHttpResponse, GatewayHttpError> {
    match response.head.status {
        200 | 206 | 304 => Ok(response),
        401 => Err(GatewayHttpError::Unauthorized),
        403 => Err(GatewayHttpError::Forbidden),
        404 => Err(GatewayHttpError::NotFound),
        409 => Err(GatewayHttpError::Conflict),
        416 => Err(GatewayHttpError::RangeNotSatisfiable),
        429 => Err(GatewayHttpError::TooManyRequests),
        503 => Err(GatewayHttpError::ServiceUnavailable),
        500..=599 => Err(GatewayHttpError::Server),
        _ => Err(GatewayHttpError::InvalidResponse),
    }
}

fn validate_request_header(value: String) -> Result<String, GatewayHttpError> {
    if value.is_empty()
        || value.len() > MAX_RESPONSE_HEADER_BYTES
        || value.bytes().any(|byte| byte == b'\r' || byte == b'\n')
        || reqwest::header::HeaderValue::from_str(value.as_str()).is_err()
    {
        return Err(GatewayHttpError::InvalidHeader);
    }
    Ok(value)
}

fn ensure_same_origin(base: &GatewayBaseUrl, target: &Url) -> Result<(), GatewayHttpError> {
    let base = Url::parse(base.as_str()).map_err(|_| GatewayHttpError::InvalidEndpoint)?;
    if base.scheme() != target.scheme()
        || base.host() != target.host()
        || base.port_or_known_default() != target.port_or_known_default()
    {
        return Err(GatewayHttpError::InvalidEndpoint);
    }
    Ok(())
}

fn map_base_url_error(error: GatewayBaseUrlError) -> GatewayHttpError {
    match error {
        GatewayBaseUrlError::InvalidStoragePath => GatewayHttpError::InvalidStoragePath,
        _ => GatewayHttpError::InvalidEndpoint,
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

fn now_unix() -> Result<u64, GatewayHttpError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| GatewayHttpError::AuthenticationUnavailable)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use tokio::sync::Barrier;

    struct FakeAuthority {
        access: Mutex<GatewayHttpAccess>,
        refreshes: AtomicUsize,
        terminal: Option<SessionTerminalReason>,
    }

    #[async_trait]
    impl GatewayHttpSessionAuthority for FakeAuthority {
        async fn current_access(&self) -> Result<GatewayHttpAccess, GatewayHttpAuthorityError> {
            if let Some(reason) = self.terminal {
                return Err(GatewayHttpAuthorityError::Terminal(reason));
            }
            Ok(self.access.lock().await.clone())
        }

        async fn coordinated_refresh(
            &self,
            rejected_generation: u64,
        ) -> Result<GatewayHttpAccess, GatewayHttpAuthorityError> {
            let mut access = self.access.lock().await;
            if access.generation == rejected_generation {
                self.refreshes.fetch_add(1, Ordering::SeqCst);
                access.generation += 1;
                access.access_token = AuthSecretString::new("fresh-access-secret");
                access.access_expires_at_unix = u64::MAX;
            }
            Ok(access.clone())
        }
    }

    struct FakeExecutor {
        statuses: Mutex<VecDeque<u16>>,
        requests: Mutex<Vec<PreparedGatewayHttpRequest>>,
    }

    #[async_trait]
    impl GatewayHttpExecutor for FakeExecutor {
        async fn execute(
            &self,
            request: PreparedGatewayHttpRequest,
            cancellation: CancellationToken,
        ) -> Result<GatewayHttpResponse, GatewayHttpError> {
            if cancellation.is_cancelled() {
                return Err(GatewayHttpError::Cancelled);
            }
            self.requests.lock().await.push(request);
            let status = self.statuses.lock().await.pop_front().unwrap_or(200);
            Ok(empty_response(status, cancellation))
        }
    }

    struct Concurrent401Executor {
        first_attempts: Arc<Barrier>,
        requests: AtomicUsize,
    }

    #[async_trait]
    impl GatewayHttpExecutor for Concurrent401Executor {
        async fn execute(
            &self,
            request: PreparedGatewayHttpRequest,
            cancellation: CancellationToken,
        ) -> Result<GatewayHttpResponse, GatewayHttpError> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            let status = if request.access_token.expose_secret() == "raw-access-secret" {
                self.first_attempts.wait().await;
                401
            } else {
                200
            };
            Ok(empty_response(status, cancellation))
        }
    }

    fn gateway_id() -> GatewayId {
        GatewayId::new("G00000000000000000001").unwrap()
    }

    fn session_id() -> AuthSessionId {
        AuthSessionId::new("S00000000000000000001").unwrap()
    }

    fn access() -> GatewayHttpAccess {
        GatewayHttpAccess {
            gateway_base_url: GatewayBaseUrl::parse_presentation("https://gateway.test/pioneer/")
                .unwrap(),
            gateway_id: gateway_id(),
            session_id: session_id(),
            generation: 1,
            access_expires_at_unix: u64::MAX,
            access_token: AuthSecretString::new("raw-access-secret"),
        }
    }

    fn empty_response(status: u16, cancellation: CancellationToken) -> GatewayHttpResponse {
        GatewayHttpResponse {
            head: GatewayHttpResponseHead {
                status,
                request_id: Some("R00000000000000000001".to_owned()),
                etag: None,
                content_length: Some(0),
                content_range: None,
                content_type: None,
                content_disposition: None,
            },
            body: GatewayHttpBody {
                stream: Box::pin(futures_util::stream::empty()),
                cancellation,
                idle_timeout: RESPONSE_BODY_IDLE_TIMEOUT,
            },
        }
    }

    fn session(authority: Arc<FakeAuthority>, executor: Arc<FakeExecutor>) -> GatewayHttpSession {
        GatewayHttpSession::with_executor(
            GatewayBaseUrl::parse_presentation("https://gateway.test/pioneer/").unwrap(),
            gateway_id(),
            session_id(),
            authority,
            executor,
        )
    }

    #[tokio::test]
    async fn native_request_uses_canonical_path_and_one_redacted_access_credential() {
        let authority = Arc::new(FakeAuthority {
            access: Mutex::new(access()),
            refreshes: AtomicUsize::new(0),
            terminal: None,
        });
        let executor = Arc::new(FakeExecutor {
            statuses: Mutex::new(VecDeque::from([200])),
            requests: Mutex::new(Vec::new()),
        });
        session(authority, executor.clone())
            .execute(
                GatewayHttpRequest::get("/storage/workspaces/w/artifacts/a/versions/v/content")
                    .unwrap(),
                CancellationToken::new(),
            )
            .await
            .unwrap();

        let requests = executor.requests.lock().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].url.as_str(),
            "https://gateway.test/pioneer/storage/workspaces/w/artifacts/a/versions/v/content"
        );
        let debug = format!("{:?}", requests[0]);
        assert!(!debug.contains("raw-access-secret"));
        assert!(debug.contains("[redacted]"));
    }

    #[tokio::test]
    async fn unauthorized_idempotent_read_uses_one_coordinated_refresh_and_retry() {
        let authority = Arc::new(FakeAuthority {
            access: Mutex::new(access()),
            refreshes: AtomicUsize::new(0),
            terminal: None,
        });
        let executor = Arc::new(FakeExecutor {
            statuses: Mutex::new(VecDeque::from([401, 200])),
            requests: Mutex::new(Vec::new()),
        });
        let response = session(authority.clone(), executor.clone())
            .execute(
                GatewayHttpRequest::head("storage/workspaces/w/artifacts/a/versions/v/content")
                    .unwrap(),
                CancellationToken::new(),
            )
            .await
            .unwrap();

        assert_eq!(response.head.status, 200);
        assert_eq!(authority.refreshes.load(Ordering::SeqCst), 1);
        let requests = executor.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].access_token.expose_secret(),
            "raw-access-secret"
        );
        assert_eq!(
            requests[1].access_token.expose_secret(),
            "fresh-access-secret"
        );
    }

    #[tokio::test]
    async fn concurrent_unauthorized_reads_share_the_same_refresh_rotation() {
        let authority = Arc::new(FakeAuthority {
            access: Mutex::new(access()),
            refreshes: AtomicUsize::new(0),
            terminal: None,
        });
        let executor = Arc::new(Concurrent401Executor {
            first_attempts: Arc::new(Barrier::new(2)),
            requests: AtomicUsize::new(0),
        });
        let session = GatewayHttpSession::with_executor(
            GatewayBaseUrl::parse_presentation("https://gateway.test/pioneer/").unwrap(),
            gateway_id(),
            session_id(),
            authority.clone(),
            executor.clone(),
        );
        let first = session.execute(
            GatewayHttpRequest::get("storage/workspaces/w/artifacts/a/versions/v/content").unwrap(),
            CancellationToken::new(),
        );
        let second = session.execute(
            GatewayHttpRequest::head("storage/workspaces/w/artifacts/a/versions/v/content")
                .unwrap(),
            CancellationToken::new(),
        );
        let (first, second) = tokio::join!(first, second);

        assert_eq!(first.unwrap().head.status, 200);
        assert_eq!(second.unwrap().head.status, 200);
        assert_eq!(authority.refreshes.load(Ordering::SeqCst), 1);
        assert_eq!(executor.requests.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn terminal_auth_gateway_mismatch_and_cancellation_are_typed_and_redacted() {
        let terminal = Arc::new(FakeAuthority {
            access: Mutex::new(access()),
            refreshes: AtomicUsize::new(0),
            terminal: Some(SessionTerminalReason::SessionRevoked),
        });
        let executor = Arc::new(FakeExecutor {
            statuses: Mutex::new(VecDeque::new()),
            requests: Mutex::new(Vec::new()),
        });
        let error = session(terminal, executor.clone())
            .execute(
                GatewayHttpRequest::get("storage/members/p/avatar/1").unwrap(),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error,
            GatewayHttpError::AuthenticationTerminal(SessionTerminalReason::SessionRevoked)
        );

        let mut mismatch = access();
        mismatch.gateway_id = GatewayId::new("G00000000000000000002").unwrap();
        let mismatch = Arc::new(FakeAuthority {
            access: Mutex::new(mismatch),
            refreshes: AtomicUsize::new(0),
            terminal: None,
        });
        let error = session(mismatch, executor.clone())
            .execute(
                GatewayHttpRequest::get("storage/members/p/avatar/1").unwrap(),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert_eq!(error, GatewayHttpError::GatewayPinMismatch);

        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let authority = Arc::new(FakeAuthority {
            access: Mutex::new(access()),
            refreshes: AtomicUsize::new(0),
            terminal: None,
        });
        let error = session(authority, executor)
            .execute(
                GatewayHttpRequest::get("storage/members/p/avatar/1").unwrap(),
                cancellation,
            )
            .await
            .unwrap_err();
        assert_eq!(error, GatewayHttpError::Cancelled);
        assert!(!format!("{error:?} {error}").contains("secret"));
    }

    #[tokio::test]
    async fn stalled_response_body_fails_with_a_bounded_transport_error() {
        let mut body = GatewayHttpBody {
            stream: Box::pin(futures_util::stream::pending()),
            cancellation: CancellationToken::new(),
            idle_timeout: Duration::from_millis(20),
        };

        assert_eq!(
            body.next_chunk().await,
            Some(Err(GatewayHttpError::Transport))
        );
    }

    #[test]
    fn view_urls_are_same_origin_prefix_preserving_and_debug_redacted() {
        let authority = Arc::new(FakeAuthority {
            access: Mutex::new(access()),
            refreshes: AtomicUsize::new(0),
            terminal: None,
        });
        let executor = Arc::new(FakeExecutor {
            statuses: Mutex::new(VecDeque::new()),
            requests: Mutex::new(Vec::new()),
        });
        let session = session(authority, executor);
        let token = "a".repeat(43);
        let view = session
            .resolve_view_url(format!("/storage/views/{token}").as_str())
            .unwrap();
        assert_eq!(
            view.expose_url(),
            format!("https://gateway.test/pioneer/storage/views/{token}")
        );
        assert_eq!(format!("{view:?}"), "BrowserViewUrl([redacted])");
        for invalid in [
            format!("https://attacker.test/storage/views/{token}"),
            format!("/storage/views/{token}?version=1"),
            "/storage/views/short".to_owned(),
        ] {
            assert!(session.resolve_view_url(invalid.as_str()).is_err());
        }
    }

    #[test]
    fn conflict_status_has_a_stable_typed_client_error() {
        let response = empty_response(409, CancellationToken::new());
        assert_eq!(
            classify_response(response).unwrap_err(),
            GatewayHttpError::Conflict
        );
    }
}
