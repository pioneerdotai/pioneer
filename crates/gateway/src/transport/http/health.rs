use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use pioneer_protocol::GatewayNativeLifecycleReadinessReport;
use std::sync::Arc;

use super::state::ReadinessState;
use crate::message::MessageProcessor;

pub(crate) async fn health() -> Response {
    (StatusCode::OK, "ok").into_response()
}

async fn native_report(
    readiness: ReadinessState,
    processor: Option<Arc<MessageProcessor>>,
) -> GatewayNativeLifecycleReadinessReport {
    let status = readiness.status();
    match processor {
        Some(processor) => processor.native_lifecycle_readiness_report(status).await,
        None => GatewayNativeLifecycleReadinessReport {
            status,
            accepting_new_turns: status.accepts_sessions(),
            generation: 0,
            checked_at_unix: chrono::Utc::now().timestamp(),
            components: Vec::new(),
        },
    }
}

pub(crate) async fn ready(
    readiness: ReadinessState,
    processor: Option<Arc<MessageProcessor>>,
) -> Response {
    readiness_response(native_report(readiness, processor).await)
}

fn readiness_response(report: GatewayNativeLifecycleReadinessReport) -> Response {
    let accepts = report.accepting_new_turns;
    let snapshot = Json(report);
    if accepts {
        (StatusCode::OK, snapshot).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, snapshot).into_response()
    }
}

pub(crate) async fn native_diagnostics(
    readiness: ReadinessState,
    processor: Option<Arc<MessageProcessor>>,
) -> Response {
    (
        StatusCode::OK,
        Json(native_report(readiness, processor).await),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn critical_native_lifecycle_report_fails_ready_closed() {
        let response = readiness_response(GatewayNativeLifecycleReadinessReport {
            status: pioneer_protocol::GatewayReadinessStatus::Degraded,
            accepting_new_turns: false,
            generation: 7,
            checked_at_unix: 1,
            components: Vec::new(),
        });
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
