use axum::Router;
use axum::routing::get;

use super::GatewayHttpState;
use super::avatars::member_avatar_route;
use super::content::{artifact_content_route, artifact_projection_route};
use super::health::{health, ready};
use super::views::view_grant_route;

pub(crate) fn gateway_router(state: GatewayHttpState) -> Router {
    operational_router::<GatewayHttpState>(state.readiness())
        .route("/", get(crate::transport::ws::root_websocket))
        .route(
            "/storage/workspaces/{workspace_id}/artifacts/{artifact_id}/versions/{version_id}/content",
            get(artifact_content_route).head(artifact_content_route),
        )
        .route(
            "/storage/workspaces/{workspace_id}/artifacts/{artifact_id}/versions/{version_id}/projections/{projection_kind}",
            get(artifact_projection_route).head(artifact_projection_route),
        )
        .route(
            "/storage/views/{opaque_grant}",
            get(view_grant_route).head(view_grant_route),
        )
        .route(
            "/storage/members/{principal_id}/avatar/{avatar_revision}",
            get(member_avatar_route).head(member_avatar_route),
        )
        .with_state(state)
}

fn operational_router<S>(readiness: super::state::ReadinessState) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(move || ready(readiness.clone())))
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn health_and_readiness_are_minimal_and_unknown_paths_stay_unregistered() {
        let readiness = super::super::state::ReadinessState::default();
        let app = operational_router::<()>(readiness.clone());

        let health = app
            .clone()
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        let not_ready = app
            .clone()
            .oneshot(Request::get("/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(not_ready.status(), StatusCode::SERVICE_UNAVAILABLE);

        readiness.set_ready(true);
        let ready = app
            .clone()
            .oneshot(Request::get("/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(ready.status(), StatusCode::OK);

        for path in [
            "/ws",
            "/socket",
            "/api/v1/ws",
            "/healthz",
            "/readyz",
            "/webhooks/test",
        ] {
            let response = app
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "path {path}");
        }
    }
}
