use std::sync::Arc;

use pioneer_artifacts::{
    ArtifactContentReader, ArtifactContentSnapshot, ArtifactError,
};
use pioneer_protocol::ArtifactProjectionKind;

use crate::authorization::{
    AuthorizationDecision, AuthorizationResolver, AuthorizationService, AuthorizedArtifact,
    ProofResolution, ResourceAction,
};
use crate::message::MessageProcessor;
use crate::request_context::AuthenticatedRequestContext;

#[derive(Clone)]
pub(crate) struct ArtifactHttpService {
    processor: Arc<MessageProcessor>,
}

#[derive(Debug)]
pub(crate) enum ArtifactHttpServiceError {
    Denied(AuthorizationDecision),
    AuthorizationUnavailable,
    Content(ArtifactError),
}

#[derive(Debug)]
pub(crate) struct AuthorizedArtifactContent {
    authorization: AuthorizedArtifact,
    snapshot: ArtifactContentSnapshot,
}

impl AuthorizedArtifactContent {
    pub(crate) fn snapshot(&self) -> &ArtifactContentSnapshot {
        &self.snapshot
    }
}

impl ArtifactHttpService {
    pub(crate) fn new(processor: Arc<MessageProcessor>) -> Self {
        Self { processor }
    }

    pub(crate) async fn authorize_exact_content(
        &self,
        request: &AuthenticatedRequestContext,
        workspace_id: &str,
        artifact_id: &str,
        artifact_version_id: &str,
    ) -> Result<AuthorizedArtifactContent, ArtifactHttpServiceError> {
        let authorization = self
            .authorize_artifact(request, workspace_id, artifact_id)
            .await?;
        let snapshot = self
            .processor
            .artifact_service
            .exact_content_snapshot(workspace_id, artifact_id, artifact_version_id)
            .await
            .map_err(ArtifactHttpServiceError::Content)?;
        bind_authorization_to_snapshot(authorization, snapshot)
    }

    pub(crate) async fn authorize_exact_projection(
        &self,
        request: &AuthenticatedRequestContext,
        workspace_id: &str,
        artifact_id: &str,
        artifact_version_id: &str,
        projection_kind: ArtifactProjectionKind,
    ) -> Result<AuthorizedArtifactContent, ArtifactHttpServiceError> {
        let authorization = self
            .authorize_artifact(request, workspace_id, artifact_id)
            .await?;
        let snapshot = self
            .processor
            .artifact_service
            .exact_projection_snapshot(
                workspace_id,
                artifact_id,
                artifact_version_id,
                projection_kind,
            )
            .await
            .map_err(ArtifactHttpServiceError::Content)?;
        bind_authorization_to_snapshot(authorization, snapshot)
    }

    pub(crate) async fn open_range(
        &self,
        content: &AuthorizedArtifactContent,
        offset: u64,
        length: u64,
    ) -> Result<ArtifactContentReader, ArtifactHttpServiceError> {
        if content.authorization.workspace_id() != content.snapshot.workspace_id()
            || content.authorization.artifact_id() != content.snapshot.artifact_id()
        {
            return Err(ArtifactHttpServiceError::Content(
                ArtifactError::ContentInvariant {
                    reason: "authorization proof does not match content snapshot",
                },
            ));
        }
        self.processor
            .artifact_service
            .open_content_range(&content.snapshot, offset, length)
            .await
            .map_err(ArtifactHttpServiceError::Content)
    }

    async fn authorize_artifact(
        &self,
        request: &AuthenticatedRequestContext,
        workspace_id: &str,
        artifact_id: &str,
    ) -> Result<AuthorizedArtifact, ArtifactHttpServiceError> {
        let action = ResourceAction::ArtifactRead;
        let action_gate = AuthorizationService::new().authorize_action(
            request.principal().kind,
            request.role_key(),
            action,
        );
        let resolution = AuthorizationResolver::new((*self.processor.crud_store).clone())
            .authorize_artifact(
                request.principal(),
                &action_gate,
                action,
                artifact_id,
                Some(workspace_id),
                None,
            )
            .await
            .map_err(|_| ArtifactHttpServiceError::AuthorizationUnavailable)?;
        match resolution {
            ProofResolution::Authorized(authorization) => Ok(authorization),
            ProofResolution::Denied(decision) => Err(ArtifactHttpServiceError::Denied(decision)),
        }
    }
}

fn bind_authorization_to_snapshot(
    authorization: AuthorizedArtifact,
    snapshot: ArtifactContentSnapshot,
) -> Result<AuthorizedArtifactContent, ArtifactHttpServiceError> {
    if authorization.workspace_id() != snapshot.workspace_id()
        || authorization.artifact_id() != snapshot.artifact_id()
    {
        return Err(ArtifactHttpServiceError::Content(
            ArtifactError::ContentInvariant {
                reason: "authorization proof does not match content snapshot",
            },
        ));
    }
    Ok(AuthorizedArtifactContent {
        authorization,
        snapshot,
    })
}
