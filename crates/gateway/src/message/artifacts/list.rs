use super::super::*;
use pioneer_artifacts::ArtifactListFilter;
use pioneer_protocol::{
    ArtifactBindParams, ArtifactBindResponse, ArtifactDeleteParams, ArtifactDeleteResponse,
    ArtifactGetParams, ArtifactGetResponse, ArtifactListParams, ArtifactListResponse,
    ArtifactRestoreParams, ArtifactRestoreResponse, ThreadArtifactsChangedNotification,
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

        let filter = match self.filter_from_artifact_list_params(params, method).await {
            Ok(filter) => filter,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to resolve artifact list scope for `{method}`: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
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

    async fn filter_from_artifact_list_params(
        &self,
        params: ArtifactListParams,
        method: &'static str,
    ) -> anyhow::Result<ArtifactListFilter> {
        let mut filter = filter_from_params(params);
        if method == methods::ARTIFACT_LIST_FOR_THREAD {
            if let Some(thread_id) = filter.thread_id.as_deref() {
                let thread_ids = self.artifact_thread_scope_ids(thread_id).await?;
                if thread_ids.len() > 1 {
                    filter.thread_id = None;
                    filter.thread_ids = thread_ids;
                }
            }
        }
        Ok(filter)
    }

    pub(crate) async fn artifact_thread_scope_ids(
        &self,
        thread_id: &str,
    ) -> anyhow::Result<Vec<String>> {
        let root_thread_id = self
            .crud_store
            .get_task_thread_lineage(thread_id)
            .await?
            .map(|lineage| lineage.root_thread_id)
            .unwrap_or_else(|| thread_id.to_owned());
        let lineage_rows = self
            .crud_store
            .list_task_thread_lineage_by_root_thread(root_thread_id.as_str())
            .await?;

        let mut parent_by_child = HashMap::<String, String>::new();
        for lineage in &lineage_rows {
            parent_by_child.insert(
                lineage.child_thread_id.clone(),
                lineage.parent_thread_id.clone(),
            );
        }

        let mut seen = HashSet::<String>::new();
        let mut thread_ids = Vec::new();
        push_unique_thread_id(&mut thread_ids, &mut seen, thread_id.to_owned());
        for lineage in lineage_rows {
            if lineage_descends_from(
                lineage.child_thread_id.as_str(),
                thread_id,
                &parent_by_child,
            ) {
                push_unique_thread_id(&mut thread_ids, &mut seen, lineage.child_thread_id);
            }
        }
        Ok(thread_ids)
    }

    pub(crate) async fn send_thread_artifacts_changed_to_thread_and_ancestors(
        &self,
        workspace_id: &str,
        source_thread_id: &str,
        artifact_ids: Vec<String>,
        reason: &str,
        generated_at: i64,
    ) {
        if artifact_ids.is_empty() {
            return;
        }
        let target_thread_ids = self
            .artifact_thread_change_target_ids(source_thread_id)
            .await;
        for target_thread_id in target_thread_ids {
            self.send_notification_to_thread_subscribers(
                target_thread_id.as_str(),
                events::THREAD_ARTIFACTS_CHANGED,
                &ThreadArtifactsChangedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: target_thread_id.clone(),
                    artifact_ids: artifact_ids.clone(),
                    reason: reason.to_owned(),
                    generated_at,
                },
            )
            .await;
        }
    }

    async fn artifact_thread_change_target_ids(&self, thread_id: &str) -> Vec<String> {
        let mut seen = HashSet::<String>::new();
        let mut thread_ids = Vec::new();
        push_unique_thread_id(&mut thread_ids, &mut seen, thread_id.to_owned());

        let mut current = thread_id.to_owned();
        for _ in 0..64 {
            let lineage = match self
                .crud_store
                .get_task_thread_lineage(current.as_str())
                .await
            {
                Ok(Some(lineage)) => lineage,
                Ok(None) => break,
                Err(error) => {
                    warn!(
                        thread_id = current,
                        error = %error,
                        "failed to resolve ancestor thread artifact notification targets"
                    );
                    break;
                }
            };
            let parent_thread_id = lineage.parent_thread_id;
            if !seen.insert(parent_thread_id.clone()) {
                break;
            }
            thread_ids.push(parent_thread_id.clone());
            current = parent_thread_id;
        }

        thread_ids
    }
}

fn filter_from_params(params: ArtifactListParams) -> ArtifactListFilter {
    ArtifactListFilter {
        limit: params.limit,
        cursor: params.cursor,
        include_deleted: params.include_deleted,
        kinds: params.kinds,
        thread_id: params.thread_id,
        thread_ids: Vec::new(),
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

fn lineage_descends_from(
    child_thread_id: &str,
    ancestor_thread_id: &str,
    parent_by_child: &HashMap<String, String>,
) -> bool {
    let mut current = child_thread_id;
    for _ in 0..=parent_by_child.len() {
        let Some(parent) = parent_by_child.get(current) else {
            return false;
        };
        if parent == ancestor_thread_id {
            return true;
        }
        if parent == current {
            return false;
        }
        current = parent.as_str();
    }
    false
}

fn push_unique_thread_id(
    thread_ids: &mut Vec<String>,
    seen: &mut HashSet<String>,
    thread_id: String,
) {
    if seen.insert(thread_id.clone()) {
        thread_ids.push(thread_id);
    }
}

struct ArtifactScopeError(String);

impl ArtifactScopeError {
    fn with_request_id(self, request_id: RequestId) -> JsonRpcErrorResponse {
        JsonRpcErrorResponse::new(Some(request_id), INVALID_PARAMS_CODE, self.0)
    }
}
