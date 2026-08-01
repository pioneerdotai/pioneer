use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use super::state::ReadinessState;

pub(crate) async fn health() -> Response {
    (StatusCode::OK, "ok").into_response()
}

pub(crate) async fn ready(readiness: ReadinessState) -> Response {
    if readiness.is_ready() {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response()
    }
}
