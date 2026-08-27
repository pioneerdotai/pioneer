use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use pioneer_protocol::GatewayReadinessSnapshot;

use super::state::ReadinessState;

pub(crate) async fn health() -> Response {
    (StatusCode::OK, "ok").into_response()
}

pub(crate) async fn ready(readiness: ReadinessState) -> Response {
    let status = readiness.status();
    let snapshot = Json(GatewayReadinessSnapshot { status });
    if status.accepts_sessions() {
        (StatusCode::OK, snapshot).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, snapshot).into_response()
    }
}
