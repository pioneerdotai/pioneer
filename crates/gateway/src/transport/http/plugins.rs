use super::{
    auth::authenticate_native_storage_request,
    errors::HttpError,
    images::{image_representation_response, valid_revision},
    state::GatewayHttpState,
};
use axum::{
    extract::{ConnectInfo, Path, State},
    http::{HeaderMap, Method},
    response::Response,
};
use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Deserialize)]
pub(super) struct PluginIconPath {
    workspace_id: String,
    plugin_id: String,
    icon_revision: String,
}

pub(super) async fn plugin_icon_route(
    State(state): State<GatewayHttpState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    Path(path): Path<PluginIconPath>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    let context =
        authenticate_native_storage_request(&state, peer_addr, &headers, "storage/plugin/icon")
            .await?;
    let request_id = context
        .request_id()
        .cloned()
        .expect("native HTTP authentication assigns a request ID");
    if !valid_revision(&path.icon_revision) {
        return Err(HttpError::not_found(request_id));
    }
    let image = state
        .message_processor
        .plugin_icon_snapshot(
            &context,
            &path.workspace_id,
            &path.plugin_id,
            &path.icon_revision,
        )
        .await
        .map_err(|_| HttpError::not_found(request_id.clone()))?;
    image_representation_response(
        &image.content,
        image.media_type,
        &path.icon_revision,
        method,
        &headers,
        &request_id,
    )
}
