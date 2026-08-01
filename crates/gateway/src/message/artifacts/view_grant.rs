use pioneer_artifacts::ArtifactError;
use pioneer_protocol::{
    ArtifactViewGrantCreateParams, ArtifactViewGrantCreateResponse,
    ArtifactViewGrantDisposition, JsonRpcErrorResponse, RequestId,
    constants::methods,
};

use super::super::*;
use crate::authorization::{AuthorizationExternalError, AuthorizedArtifact};
use crate::transport::{
    ViewGrantDisposition, ViewGrantError, ViewGrantScope,
};

const MAX_VIEW_GRANT_SCOPE_ID_BYTES: usize = 128;

impl MessageProcessor {
    pub(crate) async fn artifact_view_grant_create(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedArtifact,
        request_id: RequestId,
        params: ArtifactViewGrantCreateParams,
    ) {
        let connection_id = request_context.connection_id();
        let workspace_id = params.workspace_id.trim();
        let artifact_id = params.artifact_id.trim();
        let version_id = params.version_id.trim();
        if !valid_scope_id(workspace_id)
            || !valid_scope_id(artifact_id)
            || !valid_scope_id(version_id)
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    "view grant requires bounded workspace, artifact and exact version identifiers",
                ),
            )
            .await;
            return;
        }
        if authorization.workspace_id() != workspace_id
            || authorization.artifact_id() != artifact_id
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        let snapshot = match params.projection_kind {
            Some(projection_kind) => {
                self.artifact_service
                    .exact_projection_snapshot(
                        workspace_id,
                        artifact_id,
                        version_id,
                        projection_kind,
                    )
                    .await
            }
            None => {
                self.artifact_service
                    .exact_content_snapshot(workspace_id, artifact_id, version_id)
                    .await
            }
        };
        let snapshot = match snapshot {
            Ok(snapshot) => snapshot,
            Err(ArtifactError::NotFound { .. } | ArtifactError::InvalidRequest { .. }) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::NotFound.response(request_id),
                )
                .await;
                return;
            }
            Err(_) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::Unavailable.response(request_id),
                )
                .await;
                return;
            }
        };
        if snapshot.workspace_id() != workspace_id
            || snapshot.artifact_id() != artifact_id
            || snapshot.artifact_version_id() != version_id
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        let mut artifact_sha256 = [0_u8; 32];
        if hex::decode_to_slice(snapshot.sha256(), &mut artifact_sha256).is_err() {
            self.send_error(
                connection_id,
                AuthorizationExternalError::Unavailable.response(request_id),
            )
            .await;
            return;
        }
        let Some(service) = self.view_grant_service() else {
            self.send_error(
                connection_id,
                AuthorizationExternalError::Unavailable.response(request_id),
            )
            .await;
            return;
        };
        let issued = service.mint(ViewGrantScope {
            gateway_id: request_context.principal().gateway_id.clone(),
            principal_id: request_context.principal().principal_id.clone(),
            auth_session_id: request_context.principal().session_id.clone(),
            workspace_id: workspace_id.to_owned(),
            artifact_id: artifact_id.to_owned(),
            version_id: version_id.to_owned(),
            artifact_sha256,
            projection_kind: params.projection_kind,
            disposition: match params.disposition {
                ArtifactViewGrantDisposition::Inline => ViewGrantDisposition::Inline,
                ArtifactViewGrantDisposition::Attachment => ViewGrantDisposition::Attachment,
            },
        });
        let issued = match issued {
            Ok(issued) => issued,
            Err(ViewGrantError::Capacity) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        "view grant capacity is temporarily unavailable",
                    ),
                )
                .await;
                return;
            }
            Err(_) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::Unavailable.response(request_id),
                )
                .await;
                return;
            }
        };

        self.send_artifact_result(
            connection_id,
            request_id,
            &ArtifactViewGrantCreateResponse {
                relative_url: issued.secret.into_relative_url(),
                expires_at: issued.expires_at_unix,
            },
            methods::ARTIFACT_VIEW_GRANT_CREATE,
        )
        .await;
    }
}

fn valid_scope_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_VIEW_GRANT_SCOPE_ID_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_grant_scope_identifiers_are_non_empty_and_bounded() {
        assert!(valid_scope_id("artifact-1"));
        assert!(!valid_scope_id(""));
        assert!(valid_scope_id("x".repeat(MAX_VIEW_GRANT_SCOPE_ID_BYTES).as_str()));
        assert!(!valid_scope_id(
            "x".repeat(MAX_VIEW_GRANT_SCOPE_ID_BYTES + 1).as_str()
        ));
    }
}
