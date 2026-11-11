//! Shared immutable image HTTP representation; authorization belongs to each route.
use super::errors::{HttpError, HttpErrorKind};
use axum::http::header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, ETAG, IF_NONE_MATCH};
use axum::{
    body::Body,
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode},
    response::Response,
};
use pioneer_protocol::{ImageMediaType, RequestId};
pub(super) const IMAGE_CACHE_CONTROL: &str = "private, max-age=31536000, immutable";
const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("pioneer-request-id");
const X_CONTENT_TYPE_OPTIONS: HeaderName = HeaderName::from_static("x-content-type-options");

pub(super) fn valid_revision(revision: &str) -> bool {
    revision.len() == 64
        && revision
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn image_representation_response(
    content: &[u8],
    media_type: ImageMediaType,
    revision: &str,
    method: Method,
    request_headers: &HeaderMap,
    request_id: &RequestId,
) -> Result<Response, HttpError> {
    let etag = format!("\"{revision}\"");
    let not_modified = if_none_match_matches(request_headers, etag.as_str());
    let status = if not_modified {
        StatusCode::NOT_MODIFIED
    } else {
        StatusCode::OK
    };
    let send_body = !not_modified && method == Method::GET;
    let body = if send_body {
        Body::from(content.to_vec())
    } else {
        Body::empty()
    };
    let mut response = Response::builder()
        .status(status)
        .body(body)
        .map_err(|_| HttpError::new(HttpErrorKind::Internal, request_id.clone()))?;
    let headers = response.headers_mut();
    insert_header(headers, ETAG, etag.as_str(), request_id)?;
    headers.insert(CACHE_CONTROL, HeaderValue::from_static(IMAGE_CACHE_CONTROL));
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    insert_header(headers, REQUEST_ID_HEADER, request_id.as_str(), request_id)?;

    if !not_modified {
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(media_type.as_str()));
        insert_header(
            headers,
            CONTENT_LENGTH,
            content.len().to_string().as_str(),
            request_id,
        )?;
    }
    tracing::debug!(
        event = "image_http_response",
        outcome = if not_modified { "304" } else { "200" },
        request_id = request_id.as_str(),
    );
    Ok(response)
}

fn if_none_match_matches(headers: &HeaderMap, current_etag: &str) -> bool {
    headers.get_all(IF_NONE_MATCH).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value.split(',').any(|candidate| {
                let candidate = candidate.trim();
                candidate == "*"
                    || candidate == current_etag
                    || candidate
                        .strip_prefix("W/")
                        .is_some_and(|weak| weak == current_etag)
            })
        })
    })
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
