use super::*;
use crate::authorization::{
    AuthorizationExternalError, AuthorizedWorkspace, AuthorizedWorkspaceCollection,
};

impl MessageProcessor {
    pub(super) async fn workspace_list(
        &self,
        request_context: &RequestContext,
        proof: &AuthorizedWorkspaceCollection,
        request_id: RequestId,
        _params: WorkspaceListParams,
    ) {
        let connection_id = request_context.connection_id();
        let workspaces = match self
            .workspace_manager
            .list_authorized_workspaces(proof)
            .await
        {
            Ok(workspaces) => workspaces,
            Err(error) => {
                let (code, message) = match &error {
                    WorkspaceError::Internal(message) => (
                        INVALID_REQUEST_CODE,
                        format!("failed to list workspaces: {message}"),
                    ),
                    _ => (
                        INVALID_PARAMS_CODE,
                        format!("invalid params for `{}`: {error}", methods::WORKSPACE_LIST),
                    ),
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), code, message),
                )
                .await;
                return;
            }
        };

        let response = WorkspaceListResponse { workspaces };
        let response = match JsonRpcResponse::from_result(request_id, &response) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send workspace/list response"
            );
        }
    }

    pub(super) async fn workspace_create(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: WorkspaceCreateParams,
    ) {
        let connection_id = request_context.connection_id();
        let workspace = match self
            .workspace_manager
            .create_workspace(params.workspace_id.as_str(), params.name.as_deref())
            .await
        {
            Ok(workspace) => workspace,
            Err(error) => {
                let (code, message) = match &error {
                    WorkspaceError::Internal(message) => (
                        INVALID_REQUEST_CODE,
                        format!("failed to create workspace: {message}"),
                    ),
                    _ => (
                        INVALID_PARAMS_CODE,
                        format!(
                            "invalid params for `{}`: {error}",
                            methods::WORKSPACE_CREATE
                        ),
                    ),
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), code, message),
                )
                .await;
                return;
            }
        };

        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace.id.clone()))
            .await;
        self.request_provider_warmup(workspace.id.clone());

        let response = WorkspaceCreateResponse {
            workspace: workspace.clone(),
        };
        let response = match JsonRpcResponse::from_result(request_id, &response) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send workspace/create response"
            );
            return;
        }

        self.notify_workspace_changed(WorkspaceChangeKind::Created, workspace)
            .await;
    }

    pub(super) async fn workspace_default(
        &self,
        request_context: &RequestContext,
        proof: &AuthorizedWorkspaceCollection,
        request_id: RequestId,
        _params: WorkspaceDefaultParams,
    ) {
        let connection_id = request_context.connection_id();
        let workspace = match self
            .workspace_manager
            .authorized_default_workspace(proof)
            .await
        {
            Ok(Some(workspace)) => workspace,
            Ok(None) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::NotFound.response(request_id),
                )
                .await;
                return;
            }
            Err(error) => {
                let (code, message) = match &error {
                    WorkspaceError::Internal(message) => (
                        INVALID_REQUEST_CODE,
                        format!("failed to ensure default workspace: {message}"),
                    ),
                    _ => (
                        INVALID_PARAMS_CODE,
                        format!(
                            "invalid params for `{}`: {error}",
                            methods::WORKSPACE_DEFAULT
                        ),
                    ),
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), code, message),
                )
                .await;
                return;
            }
        };
        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace.id.clone()))
            .await;

        let response = WorkspaceDefaultResponse { workspace };
        let response = match JsonRpcResponse::from_result(request_id, &response) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send workspace/default response"
            );
        }
    }

    pub(super) async fn workspace_select(
        &self,
        request_context: &RequestContext,
        proof: &AuthorizedWorkspace,
        request_id: RequestId,
        params: WorkspaceSelectParams,
    ) {
        let connection_id = request_context.connection_id();
        let changes_global_current = proof.decision().is_absolute() && params.make_current;
        let was_current = if changes_global_current {
            self.workspace_manager
                .select_workspace(proof.workspace_id(), false)
                .await
                .map(|workspace| workspace.is_current)
                .unwrap_or(false)
        } else {
            false
        };

        let workspace = match self
            .workspace_manager
            .select_authorized_workspace(proof, params.make_current)
            .await
        {
            Ok(workspace) => workspace,
            Err(error) => {
                let (code, message) = match &error {
                    WorkspaceError::Internal(message) => (
                        INVALID_REQUEST_CODE,
                        format!("failed to select workspace: {message}"),
                    ),
                    _ => (
                        INVALID_PARAMS_CODE,
                        format!(
                            "invalid params for `{}`: {error}",
                            methods::WORKSPACE_SELECT
                        ),
                    ),
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), code, message),
                )
                .await;
                return;
            }
        };

        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace.id.clone()))
            .await;

        let response = WorkspaceSelectResponse {
            workspace: workspace.clone(),
        };
        let response = match JsonRpcResponse::from_result(request_id, &response) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send workspace/select response"
            );
            return;
        }

        if changes_global_current && !was_current {
            self.notify_workspace_changed(WorkspaceChangeKind::CurrentChanged, workspace)
                .await;
        }
    }

    pub(super) async fn workspace_update(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: WorkspaceUpdateParams,
    ) {
        let connection_id = request_context.connection_id();
        let workspace = match self
            .workspace_manager
            .update_workspace(params.workspace_id.as_str(), params.name.as_deref())
            .await
        {
            Ok(workspace) => workspace,
            Err(error) => {
                let (code, message) = match &error {
                    WorkspaceError::Internal(message) => (
                        INVALID_REQUEST_CODE,
                        format!("failed to update workspace: {message}"),
                    ),
                    _ => (
                        INVALID_PARAMS_CODE,
                        format!(
                            "invalid params for `{}`: {error}",
                            methods::WORKSPACE_UPDATE
                        ),
                    ),
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), code, message),
                )
                .await;
                return;
            }
        };

        let response = WorkspaceUpdateResponse {
            workspace: workspace.clone(),
        };
        let response = match JsonRpcResponse::from_result(request_id, &response) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send workspace/update response"
            );
            return;
        }

        self.notify_workspace_changed(WorkspaceChangeKind::Updated, workspace)
            .await;
    }

    async fn notify_workspace_changed(&self, kind: WorkspaceChangeKind, workspace: Workspace) {
        let notification = WorkspaceChangedNotification { kind, workspace };
        self.send_notification_to_authorized_workspace_connections(
            notification.workspace.id.as_str(),
            events::WORKSPACE_CHANGED,
            &notification,
        )
        .await;
    }
}
