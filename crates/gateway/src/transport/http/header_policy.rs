//! Uniform per-request header bounds for the Gateway HTTP/WS edge.

use axum::body::{Body, HttpBody as _};
use axum::http::{HeaderValue, Request, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use super::auth::new_request_id;
use super::errors::HttpError;

pub(super) const MAX_REQUEST_HEADER_BYTES: usize = 16 * 1024;
pub(crate) const MAX_WEBSOCKET_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_WEBSOCKET_FRAME_BYTES: usize = 16 * 1024 * 1024;
const VIEW_GRANT_PATH_PREFIX: &str = "/storage/views/";

pub(super) async fn enforce(request: Request<Body>, next: Next) -> Response {
    let is_view_grant_request = request.uri().path().starts_with(VIEW_GRANT_PATH_PREFIX);
    // Count the serialized `name: value\r\n` representation plus the final
    // empty line. Hyper also bounds header count; this limit owns aggregate
    // bytes and must not omit per-field framing overhead.
    let bytes = request.headers().iter().try_fold(2usize, |total, (name, value)| {
        total
            .checked_add(name.as_str().len())?
            .checked_add(value.as_bytes().len())
            .and_then(|total| total.checked_add(4))
    });
    let response = if bytes.is_none_or(|bytes| bytes > MAX_REQUEST_HEADER_BYTES) {
        HttpError::bad_request(new_request_id()).into_response()
    } else {
        let mut content_lengths = request.headers().get_all(header::CONTENT_LENGTH).iter();
        let content_length_is_valid = match content_lengths.next() {
            None => true,
            Some(value) => value.as_bytes() == b"0" && content_lengths.next().is_none(),
        };
        if !content_length_is_valid
            || request.headers().contains_key(header::TRANSFER_ENCODING)
            || !request.body().is_end_stream()
        {
            HttpError::bad_request(new_request_id()).into_response()
        } else {
            next.run(request).await
        }
    };

    protect_view_grant_referrer(response, is_view_grant_request)
}

fn protect_view_grant_referrer(
    mut response: Response,
    is_view_grant_request: bool,
) -> Response {
    if is_view_grant_request {
        response.headers_mut().insert(
            header::REFERRER_POLICY,
            HeaderValue::from_static("no-referrer"),
        );
    }
    response
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use axum::http::StatusCode;
    use axum::middleware;
    use axum::routing::get;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn oversized_headers_are_rejected_before_route_work() {
        let app = Router::new()
            .route("/", get(|| async { StatusCode::NO_CONTENT }))
            .layer(middleware::from_fn(enforce));
        let request = Request::get("/")
            .header("x-oversized", "a".repeat(MAX_REQUEST_HEADER_BYTES))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn request_bodies_are_rejected_before_route_work() {
        let app = Router::new()
            .route("/", get(|| async { StatusCode::NO_CONTENT }))
            .layer(middleware::from_fn(enforce));

        for request in [
            Request::get("/")
                .header(header::CONTENT_LENGTH, "1")
                .body(Body::from("x"))
                .unwrap(),
            Request::get("/")
                .header(header::TRANSFER_ENCODING, "chunked")
                .body(Body::empty())
                .unwrap(),
            Request::get("/")
                .header(header::CONTENT_LENGTH, "0")
                .header(header::CONTENT_LENGTH, "0")
                .body(Body::empty())
                .unwrap(),
            Request::get("/").body(Body::from("unframed-in-process-body")).unwrap(),
        ] {
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn view_grant_responses_never_expose_referrers_even_on_early_errors() {
        let app = Router::new()
            .route(
                "/storage/views/{opaque_grant}",
                get(|| async { StatusCode::NOT_FOUND }),
            )
            .layer(middleware::from_fn(enforce));

        for request in [
            Request::get("/storage/views/opaque")
                .body(Body::empty())
                .unwrap(),
            Request::get("/storage/views/opaque")
                .header(header::CONTENT_LENGTH, "1")
                .body(Body::from("x"))
                .unwrap(),
        ] {
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.headers()[header::REFERRER_POLICY], "no-referrer");
        }

        let ordinary = app
            .oneshot(
                Request::get("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(ordinary.headers().get(header::REFERRER_POLICY).is_none());
    }
}
