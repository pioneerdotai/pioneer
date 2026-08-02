use axum::Json;
use axum::http::header::{CACHE_CONTROL, CONTENT_RANGE, RETRY_AFTER, WWW_AUTHENTICATE};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use pioneer_protocol::RequestId;
use serde::Serialize;

use crate::auth::{AuthError, AuthErrorCode};

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("pioneer-request-id");
const X_CONTENT_TYPE_OPTIONS: HeaderName = HeaderName::from_static("x-content-type-options");
const MAX_RETRY_AFTER_SECONDS: u64 = 3_600;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HttpErrorKind {
    Unauthorized,
    Forbidden,
    NotFound,
    BadRequest,
    Conflict,
    RangeNotSatisfiable { complete_length: u64 },
    TooManyRequests { retry_after_seconds: u64 },
    ServiceUnavailable { retry_after_seconds: Option<u64> },
    Internal,
}

impl HttpErrorKind {
    const fn status(self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::BadRequest => StatusCode::BAD_REQUEST,
            Self::Conflict => StatusCode::CONFLICT,
            Self::RangeNotSatisfiable { .. } => StatusCode::RANGE_NOT_SATISFIABLE,
            Self::TooManyRequests { .. } => StatusCode::TOO_MANY_REQUESTS,
            Self::ServiceUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    const fn safe_code(self) -> &'static str {
        match self {
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::NotFound => "not_found",
            Self::BadRequest => "bad_request",
            Self::Conflict => "conflict",
            Self::RangeNotSatisfiable { .. } => "range_not_satisfiable",
            Self::TooManyRequests { .. } => "too_many_requests",
            Self::ServiceUnavailable { .. } => "service_unavailable",
            Self::Internal => "internal_error",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HttpError {
    kind: HttpErrorKind,
    request_id: RequestId,
}

impl HttpError {
    pub(crate) const fn new(kind: HttpErrorKind, request_id: RequestId) -> Self {
        Self { kind, request_id }
    }

    pub(crate) const fn bad_request(request_id: RequestId) -> Self {
        Self::new(HttpErrorKind::BadRequest, request_id)
    }

    pub(crate) const fn unauthorized(request_id: RequestId) -> Self {
        Self::new(HttpErrorKind::Unauthorized, request_id)
    }

    pub(crate) const fn not_found(request_id: RequestId) -> Self {
        Self::new(HttpErrorKind::NotFound, request_id)
    }

    pub(crate) const fn service_unavailable(request_id: RequestId) -> Self {
        Self::new(
            HttpErrorKind::ServiceUnavailable {
                retry_after_seconds: None,
            },
            request_id,
        )
    }

    pub(crate) fn from_auth(error: &AuthError, request_id: RequestId) -> Self {
        match error.code() {
            AuthErrorCode::AuthNotReady | AuthErrorCode::ExchangeTimeout => {
                Self::service_unavailable(request_id)
            }
            _ => Self::unauthorized(request_id),
        }
    }

    #[cfg(test)]
    pub(crate) const fn kind(&self) -> HttpErrorKind {
        self.kind
    }
}

#[derive(Serialize)]
struct HttpErrorEnvelope<'a> {
    error: HttpErrorBody<'a>,
}

#[derive(Serialize)]
struct HttpErrorBody<'a> {
    code: &'static str,
    request_id: &'a str,
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status = self.kind.status();
        let body = Json(HttpErrorEnvelope {
            error: HttpErrorBody {
                code: self.kind.safe_code(),
                request_id: self.request_id.as_str(),
            },
        });
        let mut response = (status, body).into_response();
        let headers = response.headers_mut();
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
        headers.insert(
            REQUEST_ID_HEADER,
            HeaderValue::from_str(self.request_id.as_str())
                .expect("validated request ID is a valid response header"),
        );

        match self.kind {
            HttpErrorKind::Unauthorized => {
                headers.insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            }
            HttpErrorKind::RangeNotSatisfiable { complete_length } => {
                headers.insert(
                    CONTENT_RANGE,
                    HeaderValue::from_str(format!("bytes */{complete_length}").as_str())
                        .expect("numeric complete length is a valid Content-Range"),
                );
            }
            HttpErrorKind::TooManyRequests {
                retry_after_seconds,
            }
            | HttpErrorKind::ServiceUnavailable {
                retry_after_seconds: Some(retry_after_seconds),
            } => {
                let bounded = retry_after_seconds.clamp(1, MAX_RETRY_AFTER_SECONDS);
                headers.insert(
                    RETRY_AFTER,
                    HeaderValue::from_str(bounded.to_string().as_str())
                        .expect("bounded Retry-After seconds are a valid header"),
                );
            }
            _ => {}
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;

    use super::*;

    fn request_id() -> RequestId {
        RequestId::new("R00000000000000000001").expect("request ID")
    }

    #[tokio::test]
    async fn domain_error_matrix_is_typed_bounded_and_detail_free() {
        for (kind, expected_status, expected_code) in [
            (HttpErrorKind::Unauthorized, StatusCode::UNAUTHORIZED, "unauthorized"),
            (HttpErrorKind::Forbidden, StatusCode::FORBIDDEN, "forbidden"),
            (HttpErrorKind::NotFound, StatusCode::NOT_FOUND, "not_found"),
            (HttpErrorKind::BadRequest, StatusCode::BAD_REQUEST, "bad_request"),
            (HttpErrorKind::Conflict, StatusCode::CONFLICT, "conflict"),
            (
                HttpErrorKind::RangeNotSatisfiable {
                    complete_length: 41,
                },
                StatusCode::RANGE_NOT_SATISFIABLE,
                "range_not_satisfiable",
            ),
            (
                HttpErrorKind::TooManyRequests {
                    retry_after_seconds: 1,
                },
                StatusCode::TOO_MANY_REQUESTS,
                "too_many_requests",
            ),
            (
                HttpErrorKind::ServiceUnavailable {
                    retry_after_seconds: Some(2),
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "service_unavailable",
            ),
            (
                HttpErrorKind::Internal,
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
            ),
        ] {
            let response = HttpError::new(kind, request_id()).into_response();
            assert_eq!(response.status(), expected_status);
            let bytes = to_bytes(response.into_body(), 1_024)
                .await
                .expect("bounded error body");
            assert!(bytes.len() < 256);
            let body = String::from_utf8(bytes.to_vec()).expect("UTF-8 error body");
            assert!(body.contains(expected_code));
            assert!(body.contains("R00000000000000000001"));
            for forbidden in [
                "Authorization",
                "Bearer",
                "access_token",
                "SELECT ",
                "/Users/",
                "storage_key",
            ] {
                assert!(!body.contains(forbidden), "leaked `{forbidden}` in {body}");
            }
        }
    }

    #[test]
    fn auth_failures_do_not_disclose_lifecycle_or_principal_state() {
        for code in [
            AuthErrorCode::MissingCredential,
            AuthErrorCode::DuplicateCredential,
            AuthErrorCode::MalformedCredential,
            AuthErrorCode::UnsupportedCredential,
            AuthErrorCode::InvalidCredential,
            AuthErrorCode::CredentialExpired,
            AuthErrorCode::GatewayIdentityMismatch,
            AuthErrorCode::SessionRevoked,
            AuthErrorCode::SessionExpired,
            AuthErrorCode::SessionCompromised,
        ] {
            let error = HttpError::from_auth(&AuthError::new(code), request_id());
            assert_eq!(error.kind(), HttpErrorKind::Unauthorized);
        }
        assert!(matches!(
            HttpError::from_auth(&AuthError::new(AuthErrorCode::AuthNotReady), request_id()).kind(),
            HttpErrorKind::ServiceUnavailable { .. }
        ));
    }

    #[test]
    fn range_and_capacity_headers_are_bounded() {
        let range = HttpError::new(
            HttpErrorKind::RangeNotSatisfiable {
                complete_length: 99,
            },
            request_id(),
        )
        .into_response();
        assert_eq!(range.headers()[CONTENT_RANGE], "bytes */99");

        let capacity = HttpError::new(
            HttpErrorKind::TooManyRequests {
                retry_after_seconds: u64::MAX,
            },
            request_id(),
        )
        .into_response();
        assert_eq!(capacity.headers()[RETRY_AFTER], "3600");
    }
}
