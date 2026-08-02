use std::net::SocketAddr;
use std::time::Duration;

use axum::http::HeaderMap;
use pioneer_protocol::{REQUEST_ID_LEN, RequestId, generate_id};

use crate::request_context::{
    AuthenticatedRequestContext, CanonicalMethod, SourceTransport,
};

use super::errors::HttpError;
use super::state::GatewayHttpState;

pub(crate) async fn authenticate_native_storage_request(
    state: &GatewayHttpState,
    peer_addr: SocketAddr,
    headers: &HeaderMap,
    operation: &'static str,
) -> Result<AuthenticatedRequestContext, HttpError> {
    let request_id = new_request_id();
    crate::transport::protocol::validate_protocol_version(headers)
        .map_err(|_| HttpError::bad_request(request_id.clone()))?;
    if !state.is_ready() {
        return Err(HttpError::service_unavailable(request_id));
    }

    let credential = state.auth.capture_access_headers(headers).map_err(|error| {
        tracing::debug!(
            event = "http_storage_auth_rejected",
            request_id = %request_id,
            reason_code = error.code().as_str(),
            outcome = "unauthorized",
        );
        HttpError::from_auth(&error, request_id.clone())
    })?;

    let deadline = Duration::from_secs(
        state.config.gateway.auth.auth_exchange_timeout_seconds,
    );
    let principal = match tokio::time::timeout(
        deadline,
        state.auth_service.authenticate_access(credential),
    )
    .await
    {
        Ok(Ok(principal)) => principal,
        Ok(Err(error)) => {
            tracing::debug!(
                event = "http_storage_auth_rejected",
                request_id = %request_id,
                reason_code = error.code().as_str(),
                outcome = "unauthorized",
            );
            return Err(HttpError::from_auth(&error, request_id));
        }
        Err(_) => {
            tracing::warn!(
                event = "http_storage_auth_timeout",
                request_id = %request_id,
                outcome = "service_unavailable",
            );
            return Err(HttpError::service_unavailable(request_id));
        }
    };

    Ok(AuthenticatedRequestContext::new(
        principal,
        Some(request_id),
        SourceTransport::HttpStorage,
        crate::transport::network::resolve_request_network(
            peer_addr,
            headers,
            &state.config.gateway.trusted_proxy_peers,
        ),
        CanonicalMethod::binary(operation),
    ))
}

pub(super) fn new_request_id() -> RequestId {
    RequestId::new(generate_id(REQUEST_ID_LEN))
        .expect("generated request ID satisfies the protocol contract")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_request_ids_are_server_generated_and_bounded() {
        let first = new_request_id();
        let second = new_request_id();
        assert_eq!(first.as_str().len(), REQUEST_ID_LEN);
        assert_eq!(second.as_str().len(), REQUEST_ID_LEN);
        assert_ne!(first, second);
    }
}
