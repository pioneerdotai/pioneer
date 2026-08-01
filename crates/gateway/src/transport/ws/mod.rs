pub(crate) mod admission;

use std::net::SocketAddr;

use axum::extract::connect_info::ConnectInfo;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};

use super::http::GatewayHttpState;

pub(crate) async fn root_websocket(
    State(state): State<GatewayHttpState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let network = super::network::resolve_request_network(
        peer_addr,
        &headers,
        &state.config.gateway.trusted_proxy_peers,
    );
    let client_ip = network.client_ip().unwrap_or(peer_addr.ip());
    match admission::admit(&state, client_ip, &headers).await {
        Ok(admission) => ws.on_upgrade(move |socket| async move {
            let connection = state.active_connections.register().await;
            let cancellation = connection.cancellation_receiver();
            let _ = super::server::run_admitted_connection(
                socket,
                state,
                admission,
                cancellation,
            )
            .await;
            connection.unregister().await;
        }),
        Err(response) => response.into_response(),
    }
}
