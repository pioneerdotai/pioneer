use super::super::*;
use pioneer_artifacts::ArtifactListFilter;
use pioneer_protocol::{
    ArtifactBindParams, ArtifactBindResponse, ArtifactDeleteParams, ArtifactDeleteResponse,
    ArtifactGetParams, ArtifactGetResponse, ArtifactListParams, ArtifactListResponse,
    ArtifactRestoreParams, ArtifactRestoreResponse,
};

impl MessageProcessor {
    pub(crate) async fn artifact_list(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ArtifactListParams,
        method: &'static str,
    ) {
        let workspace_id = match self
            .validate_artifact_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id.clone(),
                method,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };

        if let Err(error) = self
            .validate_artifact_list_scope(&workspace_id, &params, method)
            .await
        {
            self.send_error(connection_id, error.with_request_id(request_id))
                .await;
            return;
        }

        let filter = filter_from_params(params);
        let page = match self
            .artifact_service
            .list_artifacts(&workspace_id, filter)
            .await
        {
            Ok(page) => page,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to list artifacts for `{method}`: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        self.send_artifact_result(
            connection_id,
            request_id,
            &ArtifactListResponse {
                items: page.items,
                next_cursor: page.next_cursor,
            },
            method,
        )
        .await;
    }

    pub(crate) async fn artifact_get(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ArtifactGetParams,
    ) {
        let workspace_id = match self
            .validate_artifact_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::ARTIFACT_GET,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };

        match self
            .artifact_service
            .get_artifact(
                &workspace_id,
                &params.artifact_id,
                params.version_id.as_deref(),
            )
            .await
        {
            Ok(artifact) => {
                self.send_artifact_result(
                    connection_id,
                    request_id,
                    &ArtifactGetResponse { artifact },
                    methods::ARTIFACT_GET,
                )
                .await;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to get artifact: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    pub(crate) async fn artifact_bind(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ArtifactBindParams,
    ) {
        let workspace_id = match self
            .validate_artifact_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::ARTIFACT_BIND,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };

        let binding = match self
            .artifact_service
            .bind_artifact(BindArtifactRequest {
                workspace_id,
                artifact_id: params.artifact_id,
                version_id: params.version_id,
                target: ArtifactBindingTarget {
                    thread_id: params.thread_id,
                    turn_id: params.turn_id,
                    message_id: params.message_id,
                    turn_item_id: params.turn_item_id,
                    tool_call_id: params.tool_call_id,
                    task_id: params.task_id,
                    task_run_id: params.task_run_id,
                    binding_kind: params.binding_kind,
                    direction: params.direction,
                    role: params.role,
                    item_index: params.item_index,
                },
                metadata: Default::default(),
            })
            .await
        {
            Ok(binding) => binding,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to bind artifact: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        self.send_artifact_result(
            connection_id,
            request_id,
            &ArtifactBindResponse { binding },
            methods::ARTIFACT_BIND,
        )
        .await;
    }

    pub(crate) async fn artifact_delete(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ArtifactDeleteParams,
    ) {
        let workspace_id = match self
            .validate_artifact_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::ARTIFACT_DELETE,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };
        match self
            .artifact_service
            .delete_artifact(&workspace_id, &params.artifact_id)
            .await
        {
            Ok(status) => {
                self.send_artifact_result(
                    connection_id,
                    request_id,
                    &ArtifactDeleteResponse {
                        artifact_id: params.artifact_id,
                        status,
                    },
                    methods::ARTIFACT_DELETE,
                )
                .await;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to delete artifact: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    pub(crate) async fn artifact_restore(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ArtifactRestoreParams,
    ) {
        let workspace_id = match self
            .validate_artifact_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::ARTIFACT_RESTORE,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };
        match self
            .artifact_service
            .restore_artifact(&workspace_id, &params.artifact_id)
            .await
        {
            Ok(status) => {
                self.send_artifact_result(
                    connection_id,
                    request_id,
                    &ArtifactRestoreResponse {
                        artifact_id: params.artifact_id,
                        status,
                    },
                    methods::ARTIFACT_RESTORE,
                )
                .await;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to restore artifact: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    async fn validate_artifact_list_scope(
        &self,
        workspace_id: &str,
        params: &ArtifactListParams,
        method: &str,
    ) -> Result<(), ArtifactScopeError> {
        match method {
            methods::ARTIFACT_LIST_FOR_THREAD => {
                let thread_id = required_scope("thread_id", params.thread_id.as_deref())?;
                let thread = self
                    .crud_store
                    .get_thread_by_id(thread_id)
                    .await
                    .map_err(|error| ArtifactScopeError(error.to_string()))?
                    .ok_or_else(|| ArtifactScopeError(format!("thread `{thread_id}` not found")))?;
                if thread.workspace_id != workspace_id {
                    return Err(ArtifactScopeError(format!(
                        "thread `{thread_id}` does not belong to workspace `{workspace_id}`"
                    )));
                }
            }
            methods::ARTIFACT_LIST_FOR_TURN => {
                required_scope("turn_id", params.turn_id.as_deref())?;
            }
            methods::ARTIFACT_LIST_FOR_MESSAGE => {
                required_scope("message_id", params.message_id.as_deref())?;
            }
            _ => {}
        }
        Ok(())
    }
}

fn filter_from_params(params: ArtifactListParams) -> ArtifactListFilter {
    ArtifactListFilter {
        limit: params.limit,
        cursor: params.cursor,
        include_deleted: params.include_deleted,
        kinds: params.kinds,
        thread_id: params.thread_id,
        turn_id: params.turn_id,
        message_id: params.message_id,
        task_id: params.task_id,
        task_run_id: params.task_run_id,
    }
}

fn required_scope<'a>(field: &str, value: Option<&'a str>) -> Result<&'a str, ArtifactScopeError> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ArtifactScopeError(format!("`{field}` is required")))
}

struct ArtifactScopeError(String);

impl ArtifactScopeError {
    fn with_request_id(self, request_id: RequestId) -> JsonRpcErrorResponse {
        JsonRpcErrorResponse::new(Some(request_id), INVALID_PARAMS_CODE, self.0)
    }
}
