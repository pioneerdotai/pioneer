use super::GatewayHttpState;
use super::auth::new_request_id;
use super::errors::HttpError;
use crate::local_operations::Action;
use axum::body::{Body, to_bytes};
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use std::net::SocketAddr;

pub(super) async fn owner_operation(
    State(state): State<GatewayHttpState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let request_id = new_request_id();
    // Browser requests and forwarded requests have no local OS authority.
    if !peer.ip().is_loopback()
        || headers.contains_key(header::ORIGIN)
        || headers.contains_key("forwarded")
        || headers.contains_key("x-forwarded-for")
    {
        return HttpError::unauthorized(request_id).into_response();
    }
    if !state.is_ready() {
        return HttpError::service_unavailable(request_id).into_response();
    }
    let bytes = match tokio::time::timeout(std::time::Duration::from_secs(10), to_bytes(body, 4096))
        .await
    {
        Ok(Ok(bytes)) => bytes,
        _ => return HttpError::bad_request(request_id).into_response(),
    };
    let signature = headers
        .get("pioneer-local-mac")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let processor = &state.message_processor;
    let request = match crate::local_operations::authenticate(
        &state.config,
        processor.local_operation_home(),
        processor.gateway_secrets(),
        &bytes,
        signature,
    ) {
        Ok(request) => request,
        Err(_) => return HttpError::unauthorized(request_id).into_response(),
    };
    // Existing owner services and scoped CRUD, never initialize another runtime.
    let result: anyhow::Result<serde_json::Value> = match request.action {
        Action::CreateDevice => state
            .auth_service
            .create_local_device_for_operation(&request.id)
            .await
            .map_err(Into::into)
            .and_then(|response| serde_json::to_value(response).map_err(Into::into)),
        Action::SecretsGc => crate::operations::secrets_gc_owned(
            &processor.crud_store,
            processor.gateway_secrets(),
            false,
        )
        .await
        .and_then(|response| serde_json::to_value(response).map_err(Into::into)),
        Action::ArtifactGc {
            workspace_id,
            now_unix_ms,
        } => processor
            .artifact_service
            .gc_execute(&workspace_id, now_unix_ms)
            .await
            .map_err(Into::into)
            .and_then(|response| serde_json::to_value(response).map_err(Into::into)),
    };
    let (status, result) = match result {
        Ok(response) => (StatusCode::OK, response),
        Err(error)
            if error
                .downcast_ref::<crate::auth::AuthError>()
                .is_some_and(|error| {
                    error.code() == crate::auth::AuthErrorCode::CredentialExpired
                }) =>
        {
            (
                StatusCode::CONFLICT,
                serde_json::json!({"error":"local_device_operation_closed"}),
            )
        }
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({"error": "operation_outcome_unknown"}),
        ),
    };
    let result = serde_json::to_vec(&result).expect("JSON value serialization");
    let signature = match crate::local_operations::result_signature(
        processor.gateway_secrets(),
        &bytes,
        status.as_u16(),
        &result,
    ) {
        Ok(signature) => signature,
        Err(_) => return HttpError::service_unavailable(request_id).into_response(),
    };
    let mut response = (status, result).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
    response
        .headers_mut()
        .insert("pioneer-local-result-mac", signature.parse().unwrap());
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert(header::REFERRER_POLICY, "no-referrer".parse().unwrap());
    response
}
