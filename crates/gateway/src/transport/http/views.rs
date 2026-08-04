use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{ConnectInfo, OriginalUri, Path, State};
use axum::http::{HeaderMap, Method};
use axum::response::Response;
use pioneer_artifacts::ArtifactError;
use pioneer_protocol::RequestId;
use serde::Deserialize;
use tracing::Instrument;
use zeroize::Zeroize;

use crate::request_context::{AuthenticatedRequestContext, CanonicalMethod, SourceTransport};

use super::auth::new_request_id;
use super::content::{ContentResponsePolicy, serve_authorized_content};
use super::errors::{HttpError, HttpErrorKind};
use super::state::GatewayHttpState;
use crate::artifact_delivery::{ArtifactDeliveryError, ArtifactDeliveryService};
use crate::view_grants::{ViewGrantError, ViewGrantLease};

#[derive(Deserialize)]
pub(super) struct ViewGrantPath {
    opaque_grant: String,
}

impl Drop for ViewGrantPath {
    fn drop(&mut self) {
        self.opaque_grant.zeroize();
    }
}

impl std::fmt::Debug for ViewGrantPath {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ViewGrantPath")
            .field("opaque_grant", &"[REDACTED]")
            .finish()
    }
}

pub(super) async fn view_grant_route(
    State(state): State<GatewayHttpState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    OriginalUri(original_uri): OriginalUri,
    Path(path): Path<ViewGrantPath>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    let request_id = new_request_id();
    if original_uri.query().is_some() {
        record_view_grant_route_rejection(&request_id, "query_not_allowed");
        return Err(HttpError::not_found(request_id));
    }
    let lease = match state.view_grants.resolve(path.opaque_grant.as_str()) {
        Ok(lease) => lease,
        Err(ViewGrantError::Concurrency) => {
            return Err(HttpError::new(
                HttpErrorKind::TooManyRequests {
                    retry_after_seconds: 1,
                },
                request_id,
            ));
        }
        Err(ViewGrantError::UnknownOrExpired | ViewGrantError::InvalidScope) => {
            return Err(HttpError::not_found(request_id));
        }
        Err(
            ViewGrantError::Clock
            | ViewGrantError::ShuttingDown
            | ViewGrantError::InvalidConfig
            | ViewGrantError::Capacity,
        ) => return Err(HttpError::service_unavailable(request_id)),
    };
    if lease.grant.protocol_version != crate::transport::protocol::PIONEER_PROTOCOL_VERSION_NUMBER {
        record_view_grant_route_rejection(&request_id, "protocol_version_mismatch");
        return Err(HttpError::not_found(request_id));
    }

    let auth_deadline =
        Duration::from_secs(state.config.gateway.auth.auth_exchange_timeout_seconds);
    let principal = tokio::time::timeout(
        auth_deadline,
        state.auth_service.resolve_active_view_grant_principal(
            &lease.grant.scope.gateway_id,
            &lease.grant.scope.principal_id,
            &lease.grant.scope.auth_session_id,
        ),
    )
    .await;
    let principal = match principal {
        Ok(Ok(principal)) => principal,
        Ok(Err(_)) => {
            state
                .view_grants
                .invalidate_session(&lease.grant.scope.auth_session_id);
            record_view_grant_route_rejection(&request_id, "inactive_auth_session");
            return Err(HttpError::not_found(request_id));
        }
        Err(_) => {
            record_view_grant_route_rejection(&request_id, "auth_timeout");
            return Err(HttpError::service_unavailable(request_id));
        }
    };
    let context = AuthenticatedRequestContext::new(
        Arc::new(principal),
        Some(request_id.clone()),
        SourceTransport::HttpStorage,
        crate::transport::network::resolve_request_network(
            peer_addr,
            &headers,
            &state.config.gateway.trusted_proxy_peers,
        ),
        CanonicalMethod::binary("storage/view"),
    );
    let service = ArtifactDeliveryService::new(state.message_processor.clone());
    let content_result = tokio::time::timeout(
        state.http_streams.limits().open_timeout(),
        async {
            match lease.grant.scope.projection_kind {
                Some(projection_kind) => {
                    service
                        .authorize_exact_projection(
                            &context,
                            lease.grant.scope.workspace_id.as_str(),
                            lease.grant.scope.artifact_id.as_str(),
                            lease.grant.scope.version_id.as_str(),
                            projection_kind,
                        )
                        .await
                }
                None => {
                    service
                        .authorize_exact_content(
                            &context,
                            lease.grant.scope.workspace_id.as_str(),
                            lease.grant.scope.artifact_id.as_str(),
                            lease.grant.scope.version_id.as_str(),
                        )
                        .await
                }
            }
        }
        .instrument(context.request_span()),
    )
    .await
    .map_err(|_| HttpError::service_unavailable(request_id.clone()))?;
    let content = content_result.map_err(|error| {
        record_view_grant_route_rejection(&request_id, "content_unavailable");
        map_view_content_error(error, request_id.clone())
    })?;
    validate_bound_snapshot(
        &lease,
        content.snapshot().sha256(),
        content.snapshot().workspace_id(),
        content.snapshot().artifact_id(),
        content.snapshot().artifact_version_id(),
    )
    .map_err(|_| {
        record_view_grant_route_rejection(&request_id, "bound_snapshot_mismatch");
        HttpError::not_found(request_id.clone())
    })?;
    if lease.invalidated() {
        record_view_grant_route_rejection(&request_id, "grant_invalidated");
        return Err(HttpError::not_found(request_id));
    }
    let policy = ContentResponsePolicy::view(lease.grant.scope.disposition);

    serve_authorized_content(
        &state,
        &service,
        &context,
        content,
        method,
        headers,
        policy,
        Some(lease),
    )
    .await
}

fn record_view_grant_route_rejection(request_id: &RequestId, reason_code: &'static str) {
    tracing::debug!(
        event = "view_grant_lifecycle",
        outcome = "rejected",
        request_id = request_id.as_str(),
        reason_code,
    );
}

fn validate_bound_snapshot(
    lease: &ViewGrantLease,
    sha256: &str,
    workspace_id: &str,
    artifact_id: &str,
    version_id: &str,
) -> Result<(), ()> {
    let mut actual_sha256 = [0_u8; 32];
    if hex::decode_to_slice(sha256, &mut actual_sha256).is_err()
        || actual_sha256 != lease.grant.scope.artifact_sha256
        || workspace_id != lease.grant.scope.workspace_id
        || artifact_id != lease.grant.scope.artifact_id
        || version_id != lease.grant.scope.version_id
    {
        return Err(());
    }
    Ok(())
}

fn map_view_content_error(error: ArtifactDeliveryError, request_id: RequestId) -> HttpError {
    let kind = match error {
        ArtifactDeliveryError::Denied(_) => HttpErrorKind::NotFound,
        ArtifactDeliveryError::AuthorizationUnavailable => HttpErrorKind::ServiceUnavailable {
            retry_after_seconds: None,
        },
        ArtifactDeliveryError::RepresentationChanged => HttpErrorKind::Conflict,
        ArtifactDeliveryError::Content(
            ArtifactError::NotFound { .. } | ArtifactError::InvalidRequest { .. },
        ) => HttpErrorKind::NotFound,
        ArtifactDeliveryError::Content(
            ArtifactError::Database { .. }
            | ArtifactError::CrudStore { .. }
            | ArtifactError::Io { .. },
        ) => HttpErrorKind::ServiceUnavailable {
            retry_after_seconds: None,
        },
        ArtifactDeliveryError::Content(_) => HttpErrorKind::Internal,
    };
    HttpError::new(kind, request_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_path_debug_never_contains_the_opaque_grant() {
        let path = ViewGrantPath {
            opaque_grant: "raw-secret-that-must-not-appear".to_owned(),
        };
        let debug = format!("{path:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains(path.opaque_grant.as_str()));
    }

    #[test]
    fn representation_change_is_reported_as_conflict_without_grant_disclosure() {
        let error = map_view_content_error(
            ArtifactDeliveryError::RepresentationChanged,
            RequestId::new("R00000000000000000001").unwrap(),
        );
        assert_eq!(error.kind(), HttpErrorKind::Conflict);
    }
}
