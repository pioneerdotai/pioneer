use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::{ConnectInfo, Path, State};
use axum::http::header::{
    ACCEPT_RANGES, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE,
    ETAG, IF_NONE_MATCH, IF_RANGE, RANGE,
};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::Response;
use pioneer_artifacts::ArtifactError;
use pioneer_protocol::{ArtifactProjectionKind, RequestId};
use serde::Deserialize;
use tokio::io::{AsyncRead, ReadBuf};
use tokio_util::io::ReaderStream;
use tracing::Instrument;

use crate::authorization::{AuthorizationExternalError, external_error_for_decision};

use super::artifacts::{ArtifactHttpService, ArtifactHttpServiceError, AuthorizedArtifactContent};
use super::auth::authenticate_native_storage_request;
use super::errors::{HttpError, HttpErrorKind};
use super::state::GatewayHttpState;
use super::streams::{ManagedArtifactReader, StreamAdmissionError};
use super::view_grants::{ViewGrantDisposition, ViewGrantLease};

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("pioneer-request-id");
const X_CONTENT_TYPE_OPTIONS: HeaderName = HeaderName::from_static("x-content-type-options");
const CONTENT_SECURITY_POLICY: HeaderName = HeaderName::from_static("content-security-policy");
const REFERRER_POLICY: HeaderName = HeaderName::from_static("referrer-policy");
const STREAM_CHUNK_BYTES: usize = 64 * 1024;
const MAX_DISPOSITION_NAME_BYTES: usize = 180;
const PRIVATE_EXACT_CACHE_CONTROL: &str = "private, no-cache";
const PRIVATE_VIEW_CACHE_CONTROL: &str = "private, no-store";

#[derive(Debug, Deserialize)]
pub(super) struct ArtifactContentPath {
    workspace_id: String,
    artifact_id: String,
    version_id: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct ArtifactProjectionPath {
    workspace_id: String,
    artifact_id: String,
    version_id: String,
    projection_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ContentRouteTarget {
    Original,
    Projection(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Disposition {
    Inline,
    Attachment,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ContentResponsePolicy {
    cache_control: &'static str,
    disposition: Option<Disposition>,
    no_referrer: bool,
}

impl ContentResponsePolicy {
    const fn exact() -> Self {
        Self {
            cache_control: PRIVATE_EXACT_CACHE_CONTROL,
            disposition: None,
            no_referrer: false,
        }
    }

    pub(super) const fn view(disposition: ViewGrantDisposition) -> Self {
        Self {
            cache_control: PRIVATE_VIEW_CACHE_CONTROL,
            disposition: Some(match disposition {
                ViewGrantDisposition::Inline => Disposition::Inline,
                ViewGrantDisposition::Attachment => Disposition::Attachment,
            }),
            no_referrer: true,
        }
    }
}

impl Disposition {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Inline => "inline",
            Self::Attachment => "attachment",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NormalizedRange {
    start: u64,
    end_inclusive: u64,
}

impl NormalizedRange {
    const fn length(self) -> u64 {
        self.end_inclusive - self.start + 1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeRejection {
    Malformed,
    Multiple,
    Unsatisfiable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseSelection {
    NotModified,
    Full,
    Partial(NormalizedRange),
}

pub(super) async fn artifact_content_route(
    State(state): State<GatewayHttpState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    Path(path): Path<ArtifactContentPath>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    artifact_route(
        state,
        peer_addr,
        path.workspace_id,
        path.artifact_id,
        path.version_id,
        ContentRouteTarget::Original,
        method,
        headers,
    )
    .await
}

pub(super) async fn artifact_projection_route(
    State(state): State<GatewayHttpState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    Path(path): Path<ArtifactProjectionPath>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    artifact_route(
        state,
        peer_addr,
        path.workspace_id,
        path.artifact_id,
        path.version_id,
        ContentRouteTarget::Projection(path.projection_kind),
        method,
        headers,
    )
    .await
}

async fn artifact_route(
    state: GatewayHttpState,
    peer_addr: SocketAddr,
    workspace_id: String,
    artifact_id: String,
    version_id: String,
    target: ContentRouteTarget,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    let context = authenticate_native_storage_request(
        &state,
        peer_addr,
        &headers,
        match &target {
            ContentRouteTarget::Original => "storage/artifact/content",
            ContentRouteTarget::Projection(_) => "storage/artifact/projection",
        },
    )
    .await?;
    let request_id = context
        .request_id()
        .cloned()
        .expect("native HTTP authentication always assigns a request ID");
    let service = ArtifactHttpService::new(state.message_processor.clone());
    let request_span = context.request_span();
    let projection_kind = match &target {
        ContentRouteTarget::Original => None,
        ContentRouteTarget::Projection(value) => {
            Some(parse_projection_kind(value.as_str(), request_id.clone())?)
        }
    };
    let content_result = tokio::time::timeout(state.http_streams.limits().open_timeout(), async {
        match target {
            ContentRouteTarget::Original => {
                service
                    .authorize_exact_content(
                        &context,
                        workspace_id.as_str(),
                        artifact_id.as_str(),
                        version_id.as_str(),
                    )
                    .await
            }
            ContentRouteTarget::Projection(_) => {
                service
                    .authorize_exact_projection(
                        &context,
                        workspace_id.as_str(),
                        artifact_id.as_str(),
                        version_id.as_str(),
                        projection_kind.expect("projection route has a typed kind"),
                    )
                    .await
            }
        }
    }
    .instrument(request_span))
    .await
    .map_err(|_| HttpError::service_unavailable(request_id.clone()))?;
    let content = content_result.map_err(|error| map_content_error(error, request_id.clone()))?;
    serve_authorized_content(
        &state,
        &service,
        &context,
        content,
        method,
        headers,
        ContentResponsePolicy::exact(),
        None,
    )
    .await
}

pub(super) async fn serve_authorized_content(
    state: &GatewayHttpState,
    service: &ArtifactHttpService,
    context: &crate::request_context::AuthenticatedRequestContext,
    content: AuthorizedArtifactContent,
    method: Method,
    headers: HeaderMap,
    policy: ContentResponsePolicy,
    mut view_grant_lease: Option<ViewGrantLease>,
) -> Result<Response, HttpError> {
    let request_id = context
        .request_id()
        .cloned()
        .expect("authenticated content context always assigns a request ID");
    let etag = strong_content_etag(content.snapshot().sha256());
    let selection = select_response(&headers, content.snapshot().size_bytes(), etag.as_str())
        .map_err(|rejection| map_range_rejection(rejection, content.snapshot().size_bytes(), request_id.clone()))?;
    let send_body = method == Method::GET;

    match selection {
        ResponseSelection::NotModified => {
            representation_response(
                &content,
                &request_id,
                &etag,
                StatusCode::NOT_MODIFIED,
                None,
                Body::empty(),
                policy,
            )
        }
        ResponseSelection::Full => {
            let size = content.snapshot().size_bytes();
            let body = if send_body && size > 0 {
                open_stream_body(
                    &state,
                    &service,
                    &context,
                    &content,
                    0,
                    size,
                    request_id.clone(),
                    view_grant_lease.take(),
                )
                .await?
            } else {
                Body::empty()
            };
            representation_response(
                &content,
                &request_id,
                &etag,
                StatusCode::OK,
                Some((size, None)),
                body,
                policy,
            )
        }
        ResponseSelection::Partial(range) => {
            let length = range.length();
            state
                .http_streams
                .admit_range(&context.principal().session_id, length)
                .map_err(|error| map_stream_admission(error, request_id.clone()))?;
            let body = if send_body && length > 0 {
                open_stream_body(
                    &state,
                    &service,
                    &context,
                    &content,
                    range.start,
                    length,
                    request_id.clone(),
                    view_grant_lease.take(),
                )
                .await?
            } else {
                Body::empty()
            };
            representation_response(
                &content,
                &request_id,
                &etag,
                StatusCode::PARTIAL_CONTENT,
                Some((length, Some(range))),
                body,
                policy,
            )
        }
    }
}

fn parse_projection_kind(
    value: &str,
    request_id: RequestId,
) -> Result<ArtifactProjectionKind, HttpError> {
    match value {
        "plain_text" => Ok(ArtifactProjectionKind::PlainText),
        "thumbnail" => Ok(ArtifactProjectionKind::Thumbnail),
        "json_summary" => Ok(ArtifactProjectionKind::JsonSummary),
        "pdf_text" => Ok(ArtifactProjectionKind::PdfText),
        _ => Err(HttpError::bad_request(request_id)),
    }
}

async fn open_stream_body(
    state: &GatewayHttpState,
    service: &ArtifactHttpService,
    context: &crate::request_context::AuthenticatedRequestContext,
    content: &AuthorizedArtifactContent,
    offset: u64,
    length: u64,
    request_id: RequestId,
    view_grant_lease: Option<ViewGrantLease>,
) -> Result<Body, HttpError> {
    let lease = state
        .http_streams
        .acquire(
            &context.principal().session_id,
            context.principal().principal_id.as_str(),
            content.snapshot().workspace_id(),
            content.snapshot().artifact_id(),
        )
        .map_err(|error| map_stream_admission(error, request_id.clone()))?;
    let reader = tokio::time::timeout(
        state.http_streams.limits().open_timeout(),
        service
            .open_range(content, offset, length)
            .instrument(context.request_span()),
    )
    .await
    .map_err(|_| HttpError::service_unavailable(request_id.clone()))?
    .map_err(|error| map_content_error(error, request_id))?;
    Ok(stream_body(ViewGrantBoundReader {
        reader: ManagedArtifactReader::new(
            reader,
            length,
            lease,
            state.http_streams.limits().body_idle_timeout(),
        ),
        view_grant_lease,
    }))
}

struct ViewGrantBoundReader {
    reader: ManagedArtifactReader<pioneer_artifacts::ArtifactContentReader>,
    view_grant_lease: Option<ViewGrantLease>,
}

impl AsyncRead for ViewGrantBoundReader {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if let Some(lease) = this.view_grant_lease.as_mut()
            && lease.poll_invalidated(context).is_ready()
        {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "view grant invalidated",
            )));
        }
        Pin::new(&mut this.reader).poll_read(context, buffer)
    }
}

fn stream_body(reader: ViewGrantBoundReader) -> Body {
    // ReaderStream owns the exact-length reader. Dropping the response body on
    // disconnect drops the stream and its storage handle immediately.
    Body::from_stream(ReaderStream::with_capacity(reader, STREAM_CHUNK_BYTES))
}

fn map_stream_admission(error: StreamAdmissionError, request_id: RequestId) -> HttpError {
    match error {
        StreamAdmissionError::GlobalCapacity => HttpError::new(
            HttpErrorKind::ServiceUnavailable {
                retry_after_seconds: Some(1),
            },
            request_id,
        ),
        StreamAdmissionError::SessionCapacity
        | StreamAdmissionError::RangeTooLarge
        | StreamAdmissionError::TinyRangeRate => HttpError::new(
            HttpErrorKind::TooManyRequests {
                retry_after_seconds: 1,
            },
            request_id,
        ),
    }
}

fn representation_response(
    content: &AuthorizedArtifactContent,
    request_id: &RequestId,
    etag: &str,
    status: StatusCode,
    selected_length: Option<(u64, Option<NormalizedRange>)>,
    body: Body,
    policy: ContentResponsePolicy,
) -> Result<Response, HttpError> {
    let snapshot = content.snapshot();
    let mut response = Response::builder().status(status).body(body).map_err(|_| {
        HttpError::new(HttpErrorKind::Internal, request_id.clone())
    })?;
    let headers = response.headers_mut();
    insert_header(headers, ETAG, etag, request_id)?;
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    headers.insert(
        CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("sandbox; default-src 'none'"),
    );
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static(policy.cache_control),
    );
    if policy.no_referrer {
        headers.insert(REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    }
    insert_header(headers, REQUEST_ID_HEADER, request_id.as_str(), request_id)?;

    if let Some((length, range)) = selected_length {
        insert_header(headers, CONTENT_LENGTH, length.to_string().as_str(), request_id)?;
        let mime_type = response_mime_type(snapshot.effective_mime_type());
        insert_header(headers, CONTENT_TYPE, mime_type.as_str(), request_id)?;
        let requested_disposition = policy
            .disposition
            .unwrap_or_else(|| disposition_for_mime(snapshot.effective_mime_type()));
        let safe_disposition = if requested_disposition == Disposition::Inline
            && disposition_for_mime(snapshot.effective_mime_type()) == Disposition::Attachment
        {
            Disposition::Attachment
        } else {
            requested_disposition
        };
        let disposition = content_disposition(safe_disposition, snapshot.safe_display_name());
        insert_header(
            headers,
            CONTENT_DISPOSITION,
            disposition.as_str(),
            request_id,
        )?;
        if let Some(range) = range {
            insert_header(
                headers,
                CONTENT_RANGE,
                format!(
                    "bytes {}-{}/{}",
                    range.start,
                    range.end_inclusive,
                    snapshot.size_bytes()
                )
                .as_str(),
                request_id,
            )?;
        }
    }

    Ok(response)
}

fn response_mime_type(effective_mime_type: &str) -> String {
    if effective_mime_type == "text/plain" {
        "text/plain; charset=utf-8".to_owned()
    } else {
        effective_mime_type.to_owned()
    }
}

fn disposition_for_mime(effective_mime_type: &str) -> Disposition {
    match effective_mime_type {
        "image/png"
        | "image/jpeg"
        | "image/webp"
        | "image/gif"
        | "application/pdf"
        | "text/plain"
        | "audio/mpeg"
        | "audio/ogg"
        | "video/mp4"
        | "video/webm" => Disposition::Inline,
        _ => Disposition::Attachment,
    }
}

fn content_disposition(disposition: Disposition, safe_display_name: &str) -> String {
    let bounded = bounded_utf8_name(safe_display_name, MAX_DISPOSITION_NAME_BYTES);
    let fallback = bounded
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric()
                || matches!(character, ' ' | '.' | '_' | '-' | '(' | ')')
            {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    let encoded = encode_rfc8187(bounded);
    format!(
        "{}; filename=\"{}\"; filename*=UTF-8''{}",
        disposition.as_str(),
        fallback,
        encoded
    )
}

fn bounded_utf8_name(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut boundary = max_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &value[..boundary]
}

fn encode_rfc8187(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric()
            || matches!(byte, b'!' | b'#' | b'$' | b'&' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~')
        {
            encoded.push(char::from(byte));
        } else {
            const HEX: &[u8; 16] = b"0123456789ABCDEF";
            encoded.push('%');
            encoded.push(char::from(HEX[(byte >> 4) as usize]));
            encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
    }
    encoded
}

fn insert_header(
    headers: &mut HeaderMap,
    name: HeaderName,
    value: &str,
    request_id: &RequestId,
) -> Result<(), HttpError> {
    let value = HeaderValue::from_str(value)
        .map_err(|_| HttpError::new(HttpErrorKind::Internal, request_id.clone()))?;
    headers.insert(name, value);
    Ok(())
}

fn strong_content_etag(sha256: &str) -> String {
    format!("\"sha256-{sha256}\"")
}

fn select_response(
    headers: &HeaderMap,
    complete_length: u64,
    current_etag: &str,
) -> Result<ResponseSelection, RangeRejection> {
    if if_none_match_matches(headers, current_etag) {
        return Ok(ResponseSelection::NotModified);
    }

    let Some(range) = parse_single_range(headers, complete_length)? else {
        return Ok(ResponseSelection::Full);
    };
    if if_range_allows_range(headers, current_etag) {
        Ok(ResponseSelection::Partial(range))
    } else {
        Ok(ResponseSelection::Full)
    }
}

fn parse_single_range(
    headers: &HeaderMap,
    complete_length: u64,
) -> Result<Option<NormalizedRange>, RangeRejection> {
    let mut values = headers.get_all(RANGE).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(RangeRejection::Multiple);
    }
    let value = value.to_str().map_err(|_| RangeRejection::Malformed)?;
    let specification = value
        .strip_prefix("bytes=")
        .ok_or(RangeRejection::Malformed)?;
    if specification.contains(',') {
        return Err(RangeRejection::Multiple);
    }
    let (start, end) = specification
        .split_once('-')
        .ok_or(RangeRejection::Malformed)?;
    if start.is_empty() && end.is_empty() {
        return Err(RangeRejection::Malformed);
    }
    if complete_length == 0 {
        return Err(RangeRejection::Unsatisfiable);
    }

    if start.is_empty() {
        let suffix_length = parse_decimal(end)?;
        if suffix_length == 0 {
            return Err(RangeRejection::Unsatisfiable);
        }
        let length = suffix_length.min(complete_length);
        return Ok(Some(NormalizedRange {
            start: complete_length - length,
            end_inclusive: complete_length - 1,
        }));
    }

    let start = parse_decimal(start)?;
    if start >= complete_length {
        return Err(RangeRejection::Unsatisfiable);
    }
    let end_inclusive = if end.is_empty() {
        complete_length - 1
    } else {
        let requested_end = parse_decimal(end)?;
        if requested_end < start {
            return Err(RangeRejection::Malformed);
        }
        requested_end.min(complete_length - 1)
    };
    Ok(Some(NormalizedRange {
        start,
        end_inclusive,
    }))
}

fn parse_decimal(value: &str) -> Result<u64, RangeRejection> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(RangeRejection::Malformed);
    }
    value.parse().map_err(|_| RangeRejection::Malformed)
}

fn if_none_match_matches(headers: &HeaderMap, current_etag: &str) -> bool {
    headers.get_all(IF_NONE_MATCH).iter().any(|value| {
        value.to_str().ok().is_some_and(|value| {
            value.split(',').any(|candidate| {
                let candidate = candidate.trim();
                candidate == "*" || weak_etag(candidate) == weak_etag(current_etag)
            })
        })
    })
}

fn weak_etag(value: &str) -> &str {
    value.strip_prefix("W/").unwrap_or(value)
}

fn if_range_allows_range(headers: &HeaderMap, current_etag: &str) -> bool {
    let mut values = headers.get_all(IF_RANGE).iter();
    let Some(value) = values.next() else {
        return true;
    };
    if values.next().is_some() {
        return false;
    }
    value
        .to_str()
        .ok()
        .is_some_and(|candidate| candidate == current_etag)
}

fn map_range_rejection(
    rejection: RangeRejection,
    complete_length: u64,
    request_id: RequestId,
) -> HttpError {
    match rejection {
        RangeRejection::Malformed | RangeRejection::Multiple => HttpError::bad_request(request_id),
        RangeRejection::Unsatisfiable => HttpError::new(
            HttpErrorKind::RangeNotSatisfiable { complete_length },
            request_id,
        ),
    }
}

fn map_content_error(error: ArtifactHttpServiceError, request_id: RequestId) -> HttpError {
    let kind = match error {
        ArtifactHttpServiceError::Denied(decision) => external_error_for_decision(&decision)
            .map(http_kind_for_authorization_error)
            .unwrap_or(HttpErrorKind::Internal),
        ArtifactHttpServiceError::AuthorizationUnavailable => HttpErrorKind::ServiceUnavailable {
            retry_after_seconds: None,
        },
        ArtifactHttpServiceError::Content(error) => http_kind_for_artifact_error(&error),
    };
    HttpError::new(kind, request_id)
}

const fn http_kind_for_authorization_error(error: AuthorizationExternalError) -> HttpErrorKind {
    match error {
        AuthorizationExternalError::NotFound => HttpErrorKind::NotFound,
        AuthorizationExternalError::Forbidden => HttpErrorKind::Forbidden,
        AuthorizationExternalError::AuthenticationTerminal => HttpErrorKind::Unauthorized,
        AuthorizationExternalError::Validation => HttpErrorKind::BadRequest,
        AuthorizationExternalError::Unavailable => HttpErrorKind::ServiceUnavailable {
            retry_after_seconds: None,
        },
    }
}

fn http_kind_for_artifact_error(error: &ArtifactError) -> HttpErrorKind {
    match error {
        // After resource authorization, unavailable/deleted/not-ready content
        // is still externally indistinguishable from an absent version.
        ArtifactError::NotFound { .. } | ArtifactError::InvalidRequest { .. } => {
            HttpErrorKind::NotFound
        }
        ArtifactError::EmptyWorkspaceId
        | ArtifactError::InvalidWorkspaceId { .. } => HttpErrorKind::BadRequest,
        ArtifactError::ContentInvariant { .. }
        | ArtifactError::ExistingBlobCorruption { .. }
        | ArtifactError::ReadMissingBlob { .. }
        | ArtifactError::InvalidStorageKey { .. }
        | ArtifactError::StorageKeyTraversal { .. }
        | ArtifactError::MaterializedPathEscape { .. } => HttpErrorKind::Internal,
        ArtifactError::Database { .. }
        | ArtifactError::CrudStore { .. }
        | ArtifactError::Io { .. } => HttpErrorKind::ServiceUnavailable {
            retry_after_seconds: None,
        },
        _ => HttpErrorKind::Internal,
    }
}

#[cfg(test)]
mod tests {
    use axum::http::header::{IF_NONE_MATCH, IF_RANGE, RANGE};

    use super::*;

    const ETAG: &str = "\"sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"";

    fn headers(values: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in values {
            headers.append(
                HeaderName::from_static(name),
                HeaderValue::from_static(value),
            );
        }
        headers
    }

    #[test]
    fn range_normalization_covers_prefix_suffix_open_and_clamped_end() {
        for (value, expected) in [
            ("bytes=2-5", NormalizedRange { start: 2, end_inclusive: 5 }),
            ("bytes=-4", NormalizedRange { start: 6, end_inclusive: 9 }),
            ("bytes=7-", NormalizedRange { start: 7, end_inclusive: 9 }),
            ("bytes=8-99", NormalizedRange { start: 8, end_inclusive: 9 }),
            ("bytes=-99", NormalizedRange { start: 0, end_inclusive: 9 }),
        ] {
            let headers = headers(&[(RANGE.as_str(), value)]);
            assert_eq!(parse_single_range(&headers, 10), Ok(Some(expected)));
        }
    }

    #[test]
    fn malformed_multiple_and_unsatisfiable_ranges_remain_distinct() {
        for value in ["items=0-1", "bytes=-", "bytes=a-2", "bytes=5-2"] {
            assert_eq!(
                parse_single_range(&headers(&[(RANGE.as_str(), value)]), 10),
                Err(RangeRejection::Malformed)
            );
        }
        assert_eq!(
            parse_single_range(&headers(&[(RANGE.as_str(), "bytes=0-1,3-4")]), 10),
            Err(RangeRejection::Multiple)
        );
        let duplicate = headers(&[(RANGE.as_str(), "bytes=0-1"), (RANGE.as_str(), "bytes=3-4")]);
        assert_eq!(parse_single_range(&duplicate, 10), Err(RangeRejection::Multiple));
        assert_eq!(
            parse_single_range(&headers(&[(RANGE.as_str(), "bytes=10-")]), 10),
            Err(RangeRejection::Unsatisfiable)
        );
        assert_eq!(
            parse_single_range(&headers(&[(RANGE.as_str(), "bytes=0-")]), 0),
            Err(RangeRejection::Unsatisfiable)
        );
    }

    #[test]
    fn conditional_decision_table_precedes_open_and_range() {
        let not_modified = headers(&[(IF_NONE_MATCH.as_str(), ETAG), (RANGE.as_str(), "bytes=2-5")]);
        assert_eq!(select_response(&not_modified, 10, ETAG), Ok(ResponseSelection::NotModified));

        let weak_match = headers(&[(IF_NONE_MATCH.as_str(), "W/\"sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"")]);
        assert_eq!(select_response(&weak_match, 10, ETAG), Ok(ResponseSelection::NotModified));

        let matching_if_range = headers(&[(RANGE.as_str(), "bytes=2-5"), (IF_RANGE.as_str(), ETAG)]);
        assert_eq!(
            select_response(&matching_if_range, 10, ETAG),
            Ok(ResponseSelection::Partial(NormalizedRange { start: 2, end_inclusive: 5 }))
        );

        for mismatch in ["\"different\"", "W/\"sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"", "Sat, 01 Jan 2000 00:00:00 GMT"] {
            let request = headers(&[(RANGE.as_str(), "bytes=2-5"), (IF_RANGE.as_str(), mismatch)]);
            assert_eq!(select_response(&request, 10, ETAG), Ok(ResponseSelection::Full));
        }
    }

    #[test]
    fn hidden_and_missing_authorization_map_to_the_same_external_status() {
        assert_eq!(
            http_kind_for_authorization_error(AuthorizationExternalError::NotFound),
            HttpErrorKind::NotFound
        );
        assert_eq!(
            http_kind_for_artifact_error(&ArtifactError::NotFound { message: "missing".to_owned() }),
            HttpErrorKind::NotFound
        );
    }

    #[test]
    fn etag_is_strong_and_derived_only_from_the_immutable_digest() {
        let sha = "a".repeat(64);
        assert_eq!(strong_content_etag(&sha), format!("\"sha256-{sha}\""));
        assert!(!strong_content_etag(&sha).starts_with("W/"));
    }

    #[test]
    fn projection_kinds_are_exact_and_typed() {
        let request_id = RequestId::new("R00000000000000000061").unwrap();
        assert_eq!(
            parse_projection_kind("plain_text", request_id.clone()).unwrap(),
            ArtifactProjectionKind::PlainText
        );
        assert_eq!(
            parse_projection_kind("thumbnail", request_id.clone()).unwrap(),
            ArtifactProjectionKind::Thumbnail
        );
        assert!(parse_projection_kind("latest", request_id.clone()).is_err());
        assert!(parse_projection_kind("Thumbnail", request_id).is_err());
    }

    #[test]
    fn content_policy_forces_active_and_unknown_content_to_attachment() {
        for mime in [
            "text/html",
            "application/xhtml+xml",
            "image/svg+xml",
            "application/javascript",
            "text/javascript",
            "application/x-executable",
            "application/octet-stream",
            "application/x-unknown-active-content",
        ] {
            assert_eq!(disposition_for_mime(mime), Disposition::Attachment, "{mime}");
        }
        for mime in [
            "image/png",
            "image/jpeg",
            "image/webp",
            "image/gif",
            "application/pdf",
            "text/plain",
            "audio/mpeg",
            "audio/ogg",
            "video/mp4",
            "video/webm",
        ] {
            assert_eq!(disposition_for_mime(mime), Disposition::Inline, "{mime}");
        }
        assert_eq!(response_mime_type("text/plain"), "text/plain; charset=utf-8");
    }

    #[test]
    fn unicode_and_header_metacharacters_have_safe_bounded_disposition() {
        let disposition = content_disposition(
            Disposition::Inline,
            "отчёт \"Q3\"; résumé 你好.pdf",
        );
        assert!(disposition.starts_with("inline; filename=\""));
        assert!(disposition.contains("filename*=UTF-8''"));
        assert!(disposition.contains("%D0%BE%D1%82%D1%87%D1%91%D1%82"));
        assert!(!disposition.contains("\r"));
        assert!(!disposition.contains("\n"));
        assert!(disposition.len() < 1_000);

        let long_name = format!("{}-file.pdf", "界".repeat(400));
        assert!(content_disposition(Disposition::Attachment, &long_name).len() < 2_000);
    }

    #[test]
    fn exact_content_cache_and_security_contract_is_private_and_fixed() {
        assert_eq!(PRIVATE_EXACT_CACHE_CONTROL, "private, no-cache");
        assert!(!PRIVATE_EXACT_CACHE_CONTROL.contains("public"));
        assert_eq!(
            HeaderValue::from_static("sandbox; default-src 'none'"),
            "sandbox; default-src 'none'"
        );
    }

    #[test]
    fn view_policy_is_no_store_no_referrer_and_cannot_inline_active_content() {
        let inline = ContentResponsePolicy::view(ViewGrantDisposition::Inline);
        assert_eq!(inline.cache_control, "private, no-store");
        assert!(inline.no_referrer);
        assert_eq!(inline.disposition, Some(Disposition::Inline));
        assert_eq!(disposition_for_mime("text/html"), Disposition::Attachment);

        let attachment = ContentResponsePolicy::view(ViewGrantDisposition::Attachment);
        assert_eq!(attachment.disposition, Some(Disposition::Attachment));
    }
}
