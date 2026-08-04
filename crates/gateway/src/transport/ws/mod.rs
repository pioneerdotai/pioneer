pub(crate) mod admission;

use std::net::SocketAddr;

use axum::extract::State;
use axum::extract::connect_info::ConnectInfo;
use axum::extract::ws::WebSocketUpgrade;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use super::http::GatewayHttpState;
use super::http::header_policy::{MAX_WEBSOCKET_FRAME_BYTES, MAX_WEBSOCKET_MESSAGE_BYTES};

pub(crate) async fn root_websocket(
    State(state): State<GatewayHttpState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    if !state.is_ready() {
        tracing::debug!(
            event = "websocket_admission",
            outcome = "rejected",
            reason_code = "gateway_not_ready",
        );
        return (StatusCode::SERVICE_UNAVAILABLE, "gateway not ready").into_response();
    }
    let network = super::network::resolve_request_network(
        peer_addr,
        &headers,
        &state.config.gateway.trusted_proxy_peers,
    );
    let client_ip = network.client_ip().unwrap_or(peer_addr.ip());
    match admission::admit(&state, client_ip, &headers).await {
        Ok(admission) => {
            let (accepted, opened, closed, failed) = match &admission {
                admission::AdmittedConnection::Normal(_) => (
                    "normal_accepted",
                    "normal_opened",
                    "normal_closed",
                    "normal_failed",
                ),
                admission::AdmittedConnection::Restricted { .. } => (
                    "restricted_accepted",
                    "restricted_opened",
                    "restricted_closed",
                    "restricted_failed",
                ),
            };
            tracing::debug!(event = "websocket_admission", outcome = accepted);
            ws.max_message_size(MAX_WEBSOCKET_MESSAGE_BYTES)
                .max_frame_size(MAX_WEBSOCKET_FRAME_BYTES)
                .on_upgrade(move |socket| async move {
                    let connection = state.active_connections.register().await;
                    let cancellation = connection.cancellation_receiver();
                    tracing::debug!(event = "websocket_active_connections", outcome = opened);
                    let result = super::server::run_admitted_connection(
                        socket,
                        state,
                        admission,
                        cancellation,
                    )
                    .await;
                    tracing::debug!(
                        event = "websocket_active_connections",
                        outcome = if result.is_ok() { closed } else { failed },
                        reason_code = if result.is_ok() {
                            "peer_or_server_close"
                        } else {
                            "connection_error"
                        },
                    );
                    connection.unregister().await;
                })
        }
        Err(response) => {
            let reason_code = match response.status() {
                StatusCode::UNAUTHORIZED => "authentication_rejected",
                StatusCode::TOO_MANY_REQUESTS => "admission_capacity",
                StatusCode::REQUEST_TIMEOUT => "authentication_timeout",
                _ => "invalid_upgrade",
            };
            tracing::debug!(
                event = "websocket_admission",
                outcome = "rejected",
                reason_code,
            );
            response.into_response()
        }
    }
}
