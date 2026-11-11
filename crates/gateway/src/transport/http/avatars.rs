use std::net::SocketAddr;

use axum::extract::{ConnectInfo, Path, State};
#[cfg(test)]
use axum::http::header::{
    CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, IF_NONE_MATCH, X_CONTENT_TYPE_OPTIONS,
};
use axum::http::{HeaderMap, Method};
#[cfg(test)]
use axum::http::{HeaderValue, StatusCode};
use axum::response::Response;
use pioneer_protocol::{
    CLAUDE_AGENT_AVATAR_REVISION, CODEX_AGENT_AVATAR_REVISION, OPENCODE_AGENT_AVATAR_DARK_REVISION,
    OPENCODE_AGENT_AVATAR_LIGHT_REVISION, PIONEER_AGENT_AVATAR_REVISION, PrincipalId,
    ProfileAvatarMediaType, RequestId,
};
use serde::Deserialize;

use crate::authorization::{AuthorizationExternalError, external_error_for_decision};
use crate::member::{MemberAvatarSnapshot, MemberServiceError};

use super::auth::authenticate_native_storage_request;
use super::errors::{HttpError, HttpErrorKind};
use super::state::GatewayHttpState;

#[cfg(test)]
use super::images::IMAGE_CACHE_CONTROL as AVATAR_CACHE_CONTROL;
use super::images::{
    image_representation_response as avatar_representation_response, valid_revision,
};
const PIONEER_AGENT_AVATAR_BYTES: &[u8] = include_bytes!("../../../assets/pioneer-avatar.png");
const CODEX_AGENT_AVATAR_BYTES: &[u8] = include_bytes!("../../../assets/codex-avatar.png");
const CLAUDE_AGENT_AVATAR_BYTES: &[u8] = include_bytes!("../../../assets/claude-avatar.png");
const OPENCODE_AGENT_AVATAR_LIGHT_BYTES: &[u8] =
    include_bytes!("../../../assets/opencode-avatar-light.png");
const OPENCODE_AGENT_AVATAR_DARK_BYTES: &[u8] =
    include_bytes!("../../../assets/opencode-avatar-dark.png");

#[derive(Debug, Deserialize)]
pub(super) struct MemberAvatarPath {
    principal_id: String,
    avatar_revision: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct AgentAvatarPath {
    avatar_revision: String,
}

pub(super) async fn member_avatar_route(
    State(state): State<GatewayHttpState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    Path(path): Path<MemberAvatarPath>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    let context =
        authenticate_native_storage_request(&state, peer_addr, &headers, "storage/member/avatar")
            .await?;
    let request_id = context
        .request_id()
        .cloned()
        .expect("native HTTP authentication always assigns a request ID");
    let principal_id = PrincipalId::new(path.principal_id).map_err(|_| {
        record_avatar_hidden(&request_id, "invalid_principal_id");
        HttpError::not_found(request_id.clone())
    })?;
    if !valid_revision(path.avatar_revision.as_str()) {
        record_avatar_hidden(&request_id, "invalid_revision");
        return Err(HttpError::not_found(request_id));
    }

    let snapshot = state
        .message_processor
        .member_service()
        .avatar_snapshot(
            context.principal(),
            &principal_id,
            Some(path.avatar_revision.as_str()),
        )
        .await
        .map_err(|error| map_avatar_error(error, request_id.clone()))?;
    avatar_response(snapshot, method, &headers, &request_id)
}

pub(super) async fn agent_avatar_route(
    State(state): State<GatewayHttpState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    Path(path): Path<AgentAvatarPath>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    let context =
        authenticate_native_storage_request(&state, peer_addr, &headers, "storage/agent/avatar")
            .await?;
    let request_id = context
        .request_id()
        .cloned()
        .expect("native HTTP authentication always assigns a request ID");
    let content = match path.avatar_revision.as_str() {
        PIONEER_AGENT_AVATAR_REVISION => PIONEER_AGENT_AVATAR_BYTES,
        CODEX_AGENT_AVATAR_REVISION => CODEX_AGENT_AVATAR_BYTES,
        CLAUDE_AGENT_AVATAR_REVISION => CLAUDE_AGENT_AVATAR_BYTES,
        OPENCODE_AGENT_AVATAR_LIGHT_REVISION => OPENCODE_AGENT_AVATAR_LIGHT_BYTES,
        OPENCODE_AGENT_AVATAR_DARK_REVISION => OPENCODE_AGENT_AVATAR_DARK_BYTES,
        _ => {
            record_avatar_hidden(&request_id, "invalid_revision");
            return Err(HttpError::not_found(request_id));
        }
    };

    avatar_representation_response(
        content,
        ProfileAvatarMediaType::Png,
        path.avatar_revision.as_str(),
        method,
        &headers,
        &request_id,
    )
}

fn avatar_response(
    snapshot: MemberAvatarSnapshot,
    method: Method,
    request_headers: &HeaderMap,
    request_id: &RequestId,
) -> Result<Response, HttpError> {
    avatar_representation_response(
        snapshot.content(),
        snapshot.media_type(),
        snapshot.revision(),
        method,
        request_headers,
        request_id,
    )
}

fn map_avatar_error(error: MemberServiceError, request_id: RequestId) -> HttpError {
    let kind = match error {
        MemberServiceError::Authorization(decision) => {
            match external_error_for_decision(&decision)
                .unwrap_or(AuthorizationExternalError::NotFound)
            {
                AuthorizationExternalError::Forbidden => HttpErrorKind::Forbidden,
                AuthorizationExternalError::AuthenticationTerminal => HttpErrorKind::Unauthorized,
                AuthorizationExternalError::NotFound | AuthorizationExternalError::Validation => {
                    HttpErrorKind::NotFound
                }
                AuthorizationExternalError::Unavailable => HttpErrorKind::ServiceUnavailable {
                    retry_after_seconds: None,
                },
            }
        }
        MemberServiceError::RateLimited => HttpErrorKind::TooManyRequests {
            retry_after_seconds: 1,
        },
        MemberServiceError::Unavailable(_) | MemberServiceError::TargetUnavailable => {
            HttpErrorKind::ServiceUnavailable {
                retry_after_seconds: None,
            }
        }
        MemberServiceError::InvalidParams
        | MemberServiceError::InvalidTarget
        | MemberServiceError::Conflict(_) => HttpErrorKind::NotFound,
    };
    match &kind {
        HttpErrorKind::Forbidden | HttpErrorKind::NotFound => {
            record_avatar_hidden(&request_id, "not_disclosed")
        }
        _ => tracing::debug!(
            event = "avatar_http_response",
            outcome = "failed",
            request_id = request_id.as_str(),
            reason_code = "unavailable",
        ),
    }
    HttpError::new(kind, request_id)
}

fn record_avatar_hidden(request_id: &RequestId, reason_code: &'static str) {
    tracing::debug!(
        event = "avatar_http_response",
        outcome = "hidden",
        request_id = request_id.as_str(),
        reason_code,
    );
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use pioneer_protocol::ProfileAvatarMediaType;
    use sha2::{Digest as _, Sha256};

    use super::*;

    fn request_id() -> RequestId {
        RequestId::new("R00000000000000000001").unwrap()
    }

    fn snapshot() -> MemberAvatarSnapshot {
        MemberAvatarSnapshot::new(
            ProfileAvatarMediaType::Png,
            "a".repeat(64),
            vec![1, 2, 3, 4],
        )
    }

    #[tokio::test]
    async fn get_head_and_not_modified_share_immutable_private_headers() {
        let get =
            avatar_response(snapshot(), Method::GET, &HeaderMap::new(), &request_id()).unwrap();
        assert_eq!(get.status(), StatusCode::OK);
        assert_eq!(get.headers()[CONTENT_TYPE], "image/png");
        assert_eq!(get.headers()[CONTENT_LENGTH], "4");
        assert_eq!(get.headers()[CACHE_CONTROL], AVATAR_CACHE_CONTROL);
        assert_eq!(get.headers()[X_CONTENT_TYPE_OPTIONS], "nosniff");
        assert_eq!(
            to_bytes(get.into_body(), 8).await.unwrap().as_ref(),
            &[1, 2, 3, 4]
        );

        let head =
            avatar_response(snapshot(), Method::HEAD, &HeaderMap::new(), &request_id()).unwrap();
        assert_eq!(head.status(), StatusCode::OK);
        assert_eq!(head.headers()[CONTENT_LENGTH], "4");
        assert!(to_bytes(head.into_body(), 8).await.unwrap().is_empty());

        let mut conditional = HeaderMap::new();
        conditional.insert(
            IF_NONE_MATCH,
            HeaderValue::from_static(
                "W/\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"",
            ),
        );
        let not_modified =
            avatar_response(snapshot(), Method::GET, &conditional, &request_id()).unwrap();
        assert_eq!(not_modified.status(), StatusCode::NOT_MODIFIED);
        assert!(not_modified.headers().get(CONTENT_LENGTH).is_none());
        assert_eq!(not_modified.headers()[CACHE_CONTROL], AVATAR_CACHE_CONTROL);
        assert!(
            to_bytes(not_modified.into_body(), 8)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn revision_path_is_exact_lowercase_sha256_hex() {
        assert!(valid_revision("a".repeat(64).as_str()));
        assert!(!valid_revision("A".repeat(64).as_str()));
        assert!(!valid_revision("a".repeat(63).as_str()));
        assert!(!valid_revision(format!("{}g", "a".repeat(63)).as_str()));
    }

    #[test]
    fn embedded_agent_avatars_match_their_shared_immutable_revisions() {
        for (bytes, revision) in [
            (PIONEER_AGENT_AVATAR_BYTES, PIONEER_AGENT_AVATAR_REVISION),
            (CODEX_AGENT_AVATAR_BYTES, CODEX_AGENT_AVATAR_REVISION),
            (CLAUDE_AGENT_AVATAR_BYTES, CLAUDE_AGENT_AVATAR_REVISION),
            (
                OPENCODE_AGENT_AVATAR_LIGHT_BYTES,
                OPENCODE_AGENT_AVATAR_LIGHT_REVISION,
            ),
            (
                OPENCODE_AGENT_AVATAR_DARK_BYTES,
                OPENCODE_AGENT_AVATAR_DARK_REVISION,
            ),
        ] {
            assert_eq!(hex::encode(Sha256::digest(bytes)), revision);
            assert!(bytes.starts_with(b"\x89PNG\r\n\x1a\n"));
        }
    }
}
