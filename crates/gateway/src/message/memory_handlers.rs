use super::*;
use anyhow::{Result, anyhow};
use pioneer_memory::MemoryOperationContext;
use pioneer_protocol::{
    MemoryActor, MemoryActorKind, MemoryCandidatesApproveParams, MemoryCandidatesApproveResponse,
    MemoryCandidatesDecideParams, MemoryCandidatesDecideResponse,
    MemoryCandidatesEditAndApproveParams, MemoryCandidatesEditAndApproveResponse,
    MemoryCandidatesGetParams, MemoryCandidatesListParams, MemoryCandidatesMergeParams,
    MemoryCandidatesRejectParams, MemoryCandidatesSuppressSimilarParams, MemoryChangeKind,
    MemoryChangedNotification, MemoryForgetParams, MemoryForgetResponse,
    MemoryForgottenNotification, MemoryGetParams, MemoryListParams, MemoryProvenance,
    MemoryRememberParams, MemoryRememberResponse, MemoryScopeKind, MemorySearchParams,
    MemorySemanticWriteResponse,
};

impl MessageProcessor {
    pub(super) async fn memory_search(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: MemorySearchParams,
    ) {
        let connection_id = request_context.connection_id();
        let context = match self.memory_context(connection_id, None).await {
            Ok(context) => context,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_SEARCH,
                    error,
                )
                .await;
                return;
            }
        };

        let response = match self
            .run_memory_request(methods::MEMORY_SEARCH, |service| async move {
                service.search(context, params).await
            })
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_SEARCH,
                    error,
                )
                .await;
                return;
            }
        };

        self.send_memory_response(connection_id, request_id, methods::MEMORY_SEARCH, &response)
            .await;
    }

    pub(super) async fn memory_get(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: MemoryGetParams,
    ) {
        let connection_id = request_context.connection_id();
        let context = match self.memory_context(connection_id, None).await {
            Ok(context) => context,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_GET,
                    error,
                )
                .await;
                return;
            }
        };

        let response = match self
            .run_memory_request(methods::MEMORY_GET, |service| async move {
                service.get(context, params).await
            })
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_GET,
                    error,
                )
                .await;
                return;
            }
        };

        self.send_memory_response(connection_id, request_id, methods::MEMORY_GET, &response)
            .await;
    }

    pub(super) async fn memory_list(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: MemoryListParams,
    ) {
        let connection_id = request_context.connection_id();
        let context = match self.memory_context(connection_id, None).await {
            Ok(context) => context,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_LIST,
                    error,
                )
                .await;
                return;
            }
        };

        let response = match self
            .run_memory_request(methods::MEMORY_LIST, |service| async move {
                service.list(context, params).await
            })
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_LIST,
                    error,
                )
                .await;
                return;
            }
        };

        self.send_memory_response(connection_id, request_id, methods::MEMORY_LIST, &response)
            .await;
    }

    pub(super) async fn memory_remember(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        mut params: MemoryRememberParams,
    ) {
        let connection_id = request_context.connection_id();
        let actor = authenticated_memory_request_actor(request_context);
        bind_authenticated_memory_remember_actor(&mut params, actor.clone());
        let context = match self.memory_context(connection_id, Some(actor)).await {
            Ok(context) => context,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_REMEMBER,
                    error,
                )
                .await;
                return;
            }
        };
        let notify_context = context.clone();

        let response = match self
            .run_memory_request(methods::MEMORY_REMEMBER, |service| async move {
                service.remember(context, params).await
            })
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_REMEMBER,
                    error,
                )
                .await;
                return;
            }
        };

        self.send_memory_response(
            connection_id,
            request_id,
            methods::MEMORY_REMEMBER,
            &response,
        )
        .await;
        self.send_memory_changed_after_remember(connection_id, &notify_context, &response)
            .await;
    }

    pub(super) async fn memory_forget(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        mut params: MemoryForgetParams,
    ) {
        let connection_id = request_context.connection_id();
        params.actor = Some(authenticated_memory_request_actor(request_context));
        let context = match self
            .memory_context(connection_id, params.actor.clone())
            .await
        {
            Ok(context) => context,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_FORGET,
                    error,
                )
                .await;
                return;
            }
        };
        let reason = params.reason.clone();
        let dry_run = params.dry_run;

        let response = match self
            .run_memory_request(methods::MEMORY_FORGET, |service| async move {
                service.forget(context, params).await
            })
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_FORGET,
                    error,
                )
                .await;
                return;
            }
        };

        self.send_memory_response(connection_id, request_id, methods::MEMORY_FORGET, &response)
            .await;
        self.send_memory_forgotten_after_forget(connection_id, reason, dry_run, &response)
            .await;
    }

    pub(super) async fn memory_candidates_list(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: MemoryCandidatesListParams,
    ) {
        let connection_id = request_context.connection_id();
        let context = match self.memory_context(connection_id, None).await {
            Ok(context) => context,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_LIST,
                    error,
                )
                .await;
                return;
            }
        };

        let response = match self
            .run_memory_request(methods::MEMORY_CANDIDATES_LIST, |service| async move {
                service.list_candidates(context, params).await
            })
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_LIST,
                    error,
                )
                .await;
                return;
            }
        };

        self.send_memory_response(
            connection_id,
            request_id,
            methods::MEMORY_CANDIDATES_LIST,
            &response,
        )
        .await;
    }

    pub(super) async fn memory_candidates_get(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: MemoryCandidatesGetParams,
    ) {
        let connection_id = request_context.connection_id();
        let context = match self.memory_context(connection_id, None).await {
            Ok(context) => context,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_GET,
                    error,
                )
                .await;
                return;
            }
        };

        let response = match self
            .run_memory_request(methods::MEMORY_CANDIDATES_GET, |service| async move {
                service.get_candidate(context, params).await
            })
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_GET,
                    error,
                )
                .await;
                return;
            }
        };

        self.send_memory_response(
            connection_id,
            request_id,
            methods::MEMORY_CANDIDATES_GET,
            &response,
        )
        .await;
    }

    pub(super) async fn memory_candidates_decide(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        mut params: MemoryCandidatesDecideParams,
    ) {
        let connection_id = request_context.connection_id();
        params.actor = Some(authenticated_memory_request_actor(request_context));
        let context = match self
            .memory_context(connection_id, params.actor.clone())
            .await
        {
            Ok(context) => context,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_DECIDE,
                    error,
                )
                .await;
                return;
            }
        };
        let notify_context = context.clone();

        let response = match self
            .run_memory_request(methods::MEMORY_CANDIDATES_DECIDE, |service| async move {
                service.decide_candidate(context, params).await
            })
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_DECIDE,
                    error,
                )
                .await;
                return;
            }
        };

        self.send_memory_response(
            connection_id,
            request_id,
            methods::MEMORY_CANDIDATES_DECIDE,
            &response,
        )
        .await;
        self.send_memory_changed_after_candidate_decide(connection_id, &notify_context, &response)
            .await;
    }

    pub(super) async fn memory_candidates_approve(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        mut params: MemoryCandidatesApproveParams,
    ) {
        let connection_id = request_context.connection_id();
        params.actor = Some(authenticated_memory_request_actor(request_context));
        let context = match self
            .memory_context(connection_id, params.actor.clone())
            .await
        {
            Ok(context) => context,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_APPROVE,
                    error,
                )
                .await;
                return;
            }
        };
        let notify_context = context.clone();

        let response = match self
            .run_memory_request(methods::MEMORY_CANDIDATES_APPROVE, |service| async move {
                service.approve_candidate(context, params).await
            })
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_APPROVE,
                    error,
                )
                .await;
                return;
            }
        };

        self.send_memory_response(
            connection_id,
            request_id,
            methods::MEMORY_CANDIDATES_APPROVE,
            &response,
        )
        .await;
        self.send_memory_changed_after_candidate_approve(connection_id, &notify_context, &response)
            .await;
    }

    pub(super) async fn memory_candidates_reject(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        mut params: MemoryCandidatesRejectParams,
    ) {
        let connection_id = request_context.connection_id();
        params.actor = Some(authenticated_memory_request_actor(request_context));
        let context = match self
            .memory_context(connection_id, params.actor.clone())
            .await
        {
            Ok(context) => context,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_REJECT,
                    error,
                )
                .await;
                return;
            }
        };

        let response = match self
            .run_memory_request(methods::MEMORY_CANDIDATES_REJECT, |service| async move {
                service.reject_candidate(context, params).await
            })
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_REJECT,
                    error,
                )
                .await;
                return;
            }
        };

        self.send_memory_response(
            connection_id,
            request_id,
            methods::MEMORY_CANDIDATES_REJECT,
            &response,
        )
        .await;
    }

    pub(super) async fn memory_candidates_edit_and_approve(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        mut params: MemoryCandidatesEditAndApproveParams,
    ) {
        let connection_id = request_context.connection_id();
        params.actor = Some(authenticated_memory_request_actor(request_context));
        let context = match self
            .memory_context(connection_id, params.actor.clone())
            .await
        {
            Ok(context) => context,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_EDIT_AND_APPROVE,
                    error,
                )
                .await;
                return;
            }
        };
        let notify_context = context.clone();

        let response = match self
            .run_memory_request(
                methods::MEMORY_CANDIDATES_EDIT_AND_APPROVE,
                |service| async move { service.edit_and_approve_candidate(context, params).await },
            )
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_EDIT_AND_APPROVE,
                    error,
                )
                .await;
                return;
            }
        };

        self.send_memory_response(
            connection_id,
            request_id,
            methods::MEMORY_CANDIDATES_EDIT_AND_APPROVE,
            &response,
        )
        .await;
        self.send_memory_changed_after_candidate_edit_and_approve(
            connection_id,
            &notify_context,
            &response,
        )
        .await;
    }

    pub(super) async fn memory_candidates_merge(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        mut params: MemoryCandidatesMergeParams,
    ) {
        let connection_id = request_context.connection_id();
        params.actor = Some(authenticated_memory_request_actor(request_context));
        let context = match self
            .memory_context(connection_id, params.actor.clone())
            .await
        {
            Ok(context) => context,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_MERGE,
                    error,
                )
                .await;
                return;
            }
        };

        let response = match self
            .run_memory_request(methods::MEMORY_CANDIDATES_MERGE, |service| async move {
                service.merge_candidate(context, params).await
            })
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_MERGE,
                    error,
                )
                .await;
                return;
            }
        };

        self.send_memory_response(
            connection_id,
            request_id,
            methods::MEMORY_CANDIDATES_MERGE,
            &response,
        )
        .await;
    }

    pub(super) async fn memory_candidates_suppress_similar(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        mut params: MemoryCandidatesSuppressSimilarParams,
    ) {
        let connection_id = request_context.connection_id();
        params.actor = Some(authenticated_memory_request_actor(request_context));
        let context = match self
            .memory_context(connection_id, params.actor.clone())
            .await
        {
            Ok(context) => context,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_SUPPRESS_SIMILAR,
                    error,
                )
                .await;
                return;
            }
        };

        let response = match self
            .run_memory_request(
                methods::MEMORY_CANDIDATES_SUPPRESS_SIMILAR,
                |service| async move { service.suppress_similar_candidate(context, params).await },
            )
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.send_memory_service_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_SUPPRESS_SIMILAR,
                    error,
                )
                .await;
                return;
            }
        };

        self.send_memory_response(
            connection_id,
            request_id,
            methods::MEMORY_CANDIDATES_SUPPRESS_SIMILAR,
            &response,
        )
        .await;
    }

    async fn run_memory_request<F, Fut, T>(&self, _method: &'static str, operation: F) -> Result<T>
    where
        F: FnOnce(Arc<pioneer_memory::MemoryService>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.memory_runtime.ensure_enabled()?;
        operation(self.memory_runtime.service()).await
    }

    async fn memory_context(
        &self,
        connection_id: ConnectionId,
        actor: Option<MemoryActor>,
    ) -> Result<MemoryOperationContext> {
        let workspace_id = match self
            .session_manager
            .connection_workspace_id(connection_id)
            .await
        {
            Some(workspace_id) => workspace_id,
            None => {
                let workspace = self
                    .workspace_manager
                    .ensure_default_workspace()
                    .await
                    .map_err(|error| anyhow!("failed to resolve connection workspace: {error}"))?;
                self.session_manager
                    .set_connection_workspace(connection_id, Some(workspace.id.clone()))
                    .await;
                workspace.id
            }
        };

        Ok(self
            .memory_runtime
            .operation_context(Some(workspace_id), actor))
    }

    async fn send_memory_response<T: Serialize>(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        method: &'static str,
        response: &T,
    ) {
        let response = match JsonRpcResponse::from_result(request_id, response) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response for `{method}`: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                method,
                error = %format!("{error:#}"),
                "failed to send memory response"
            );
        }
    }

    async fn send_memory_service_error(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        method: &'static str,
        error: anyhow::Error,
    ) {
        let error_text = error.to_string();
        let (code, message) = if error_text == "memory runtime is disabled" {
            (INVALID_REQUEST_CODE, error_text)
        } else if is_memory_client_error(error_text.as_str()) {
            (
                INVALID_PARAMS_CODE,
                format!("invalid params for `{method}`: {error_text}"),
            )
        } else {
            warn!(
                connection_id,
                method,
                error = %format!("{error:#}"),
                "failed to process memory request"
            );
            (
                INVALID_REQUEST_CODE,
                format!("failed to process `{method}`"),
            )
        };

        self.send_error(
            connection_id,
            JsonRpcErrorResponse::new(Some(request_id), code, message),
        )
        .await;
    }

    async fn send_memory_changed_after_remember(
        &self,
        connection_id: ConnectionId,
        context: &MemoryOperationContext,
        response: &MemoryRememberResponse,
    ) {
        let notification = MemoryChangedNotification {
            memory_id: response.record.id.clone(),
            scope: response.record.scope.clone(),
            change_kind: if response.created {
                MemoryChangeKind::Created
            } else {
                MemoryChangeKind::Updated
            },
            record: Some(response.record.clone()),
        };

        self.send_memory_changed_notification(connection_id, context, &notification)
            .await;
    }

    async fn send_memory_changed_after_candidate_decide(
        &self,
        connection_id: ConnectionId,
        context: &MemoryOperationContext,
        response: &MemoryCandidatesDecideResponse,
    ) {
        let Some(record) = &response.record else {
            return;
        };
        let notification = MemoryChangedNotification {
            memory_id: record.id.clone(),
            scope: record.scope.clone(),
            change_kind: MemoryChangeKind::Created,
            record: Some(record.clone()),
        };

        self.send_memory_changed_notification(connection_id, context, &notification)
            .await;
    }

    async fn send_memory_changed_after_candidate_approve(
        &self,
        connection_id: ConnectionId,
        context: &MemoryOperationContext,
        response: &MemoryCandidatesApproveResponse,
    ) {
        let notification = MemoryChangedNotification {
            memory_id: response.record.id.clone(),
            scope: response.record.scope.clone(),
            change_kind: MemoryChangeKind::Created,
            record: Some(response.record.clone()),
        };

        self.send_memory_changed_notification(connection_id, context, &notification)
            .await;
    }

    async fn send_memory_changed_after_candidate_edit_and_approve(
        &self,
        connection_id: ConnectionId,
        context: &MemoryOperationContext,
        response: &MemoryCandidatesEditAndApproveResponse,
    ) {
        let notification = MemoryChangedNotification {
            memory_id: response.record.id.clone(),
            scope: response.record.scope.clone(),
            change_kind: MemoryChangeKind::Created,
            record: Some(response.record.clone()),
        };

        self.send_memory_changed_notification(connection_id, context, &notification)
            .await;
    }

    pub(crate) async fn send_memory_changed_after_tool_remember(
        &self,
        context: &MemoryOperationContext,
        response: &MemoryRememberResponse,
    ) {
        let Some(workspace_id) = context.workspace_id.as_deref() else {
            return;
        };
        let notification = MemoryChangedNotification {
            memory_id: response.record.id.clone(),
            scope: response.record.scope.clone(),
            change_kind: if response.created {
                MemoryChangeKind::Created
            } else {
                MemoryChangeKind::Updated
            },
            record: Some(response.record.clone()),
        };

        self.send_notification_to_workspace_connections(
            workspace_id,
            events::MEMORY_CHANGED,
            &notification,
        )
        .await;
    }

    pub(crate) async fn send_memory_changed_after_semantic_write(
        &self,
        context: &MemoryOperationContext,
        response: &MemorySemanticWriteResponse,
    ) {
        let Some(workspace_id) = context.workspace_id.as_deref() else {
            return;
        };
        if let Some(record) = response.record.as_ref() {
            let notification = MemoryChangedNotification {
                memory_id: record.id.clone(),
                scope: record.scope.clone(),
                change_kind: if response.created {
                    MemoryChangeKind::Created
                } else {
                    MemoryChangeKind::Updated
                },
                record: Some(record.clone()),
            };
            self.send_notification_to_workspace_connections(
                workspace_id,
                events::MEMORY_CHANGED,
                &notification,
            )
            .await;
        }
    }

    pub(crate) async fn send_memory_forgotten_after_tool_forget(
        &self,
        context: &MemoryOperationContext,
        reason: Option<String>,
        dry_run: bool,
        response: &MemoryForgetResponse,
    ) {
        if dry_run || response.forgotten_memory_ids.is_empty() {
            return;
        }
        let Some(workspace_id) = context.workspace_id.as_deref() else {
            return;
        };

        let notification = MemoryForgottenNotification {
            memory_ids: response.forgotten_memory_ids.clone(),
            reason,
        };
        self.send_notification_to_workspace_connections(
            workspace_id,
            events::MEMORY_FORGOTTEN,
            &notification,
        )
        .await;
    }

    async fn send_memory_changed_notification(
        &self,
        connection_id: ConnectionId,
        context: &MemoryOperationContext,
        notification: &MemoryChangedNotification,
    ) {
        match notification.scope.kind {
            MemoryScopeKind::Workspace | MemoryScopeKind::Thread | MemoryScopeKind::Task => {
                if let Some(workspace_id) = context.workspace_id.as_deref() {
                    self.send_notification_to_workspace_connections(
                        workspace_id,
                        events::MEMORY_CHANGED,
                        notification,
                    )
                    .await;
                    return;
                }
            }
            _ => {}
        }

        self.send_notification_to_connections(
            events::MEMORY_CHANGED,
            notification,
            vec![connection_id],
        )
        .await;
    }

    async fn send_memory_forgotten_after_forget(
        &self,
        connection_id: ConnectionId,
        reason: Option<String>,
        dry_run: bool,
        response: &MemoryForgetResponse,
    ) {
        if dry_run || response.forgotten_memory_ids.is_empty() {
            return;
        }

        let notification = MemoryForgottenNotification {
            memory_ids: response.forgotten_memory_ids.clone(),
            reason,
        };
        self.send_notification_to_connections(
            events::MEMORY_FORGOTTEN,
            &notification,
            vec![connection_id],
        )
        .await;
    }
}

fn authenticated_memory_request_actor(_request_context: &RequestContext) -> MemoryActor {
    // The legacy protocol field remains decodable for compatibility, but an
    // RPC cannot choose its own actor kind or identifier. Epic 2 intentionally
    // does not copy the stable PrincipalId into the pre-existing memory rows.
    MemoryActor {
        kind: MemoryActorKind::User,
        id: None,
    }
}

fn bind_authenticated_memory_remember_actor(params: &mut MemoryRememberParams, actor: MemoryActor) {
    match params.provenance.as_mut() {
        Some(provenance) => provenance.created_by = Some(actor),
        None => {
            params.provenance = Some(MemoryProvenance {
                source_thread_id: None,
                source_turn_id: None,
                source_item_id: None,
                created_by: Some(actor),
            });
        }
    }
}

fn is_memory_client_error(error: &str) -> bool {
    let normalized = error.to_ascii_lowercase();
    normalized.contains("cannot be empty")
        || normalized.contains("invalid")
        || normalized.contains("not found")
        || normalized.contains("does not exist")
        || normalized.contains("not pending")
        || normalized.contains("must not")
        || normalized.contains("must be")
}

#[cfg(test)]
mod tests {
    use super::{authenticated_memory_request_actor, bind_authenticated_memory_remember_actor};
    use crate::request_context::{CanonicalMethod, ConnectionContext, RequestContext};
    use crate::session::test_support::authenticated_test_superuser;
    use pioneer_protocol::{
        MemoryActor, MemoryActorKind, MemoryCategory, MemoryProvenance, MemoryRememberParams,
        MemoryScope, MemoryScopeKind, RequestId,
    };

    #[test]
    fn rpc_memory_actor_is_server_derived_and_contains_no_client_identifier() {
        let connection = ConnectionContext::new(17, authenticated_test_superuser());
        let context = RequestContext::new(
            &connection,
            Some(RequestId::new("R00000000000000000001").expect("request id")),
            CanonicalMethod::rpc("memory/candidates/decide"),
        );

        let actor = authenticated_memory_request_actor(&context);

        assert_eq!(actor.kind, MemoryActorKind::User);
        assert_eq!(actor.id, None);
    }

    #[test]
    fn memory_remember_preserves_source_refs_but_replaces_client_created_by() {
        let mut params = MemoryRememberParams {
            scope: MemoryScope {
                kind: MemoryScopeKind::Workspace,
                key: "workspace".to_owned(),
            },
            category: MemoryCategory::ProjectFact,
            namespace: None,
            key: None,
            content: "remember me".to_owned(),
            sensitivity: None,
            confidence: None,
            importance: None,
            provenance: Some(MemoryProvenance {
                source_thread_id: Some("source-thread".to_owned()),
                source_turn_id: Some("source-turn".to_owned()),
                source_item_id: Some("source-item".to_owned()),
                created_by: Some(MemoryActor {
                    kind: MemoryActorKind::System,
                    id: Some("client-controlled-id".to_owned()),
                }),
            }),
            source_context_kind: None,
            idempotency_key: None,
            supersedes: None,
            metadata: Default::default(),
        };
        let server_actor = MemoryActor {
            kind: MemoryActorKind::User,
            id: None,
        };

        bind_authenticated_memory_remember_actor(&mut params, server_actor.clone());

        let provenance = params.provenance.expect("server-owned provenance");
        assert_eq!(
            provenance.source_thread_id.as_deref(),
            Some("source-thread")
        );
        assert_eq!(provenance.source_turn_id.as_deref(), Some("source-turn"));
        assert_eq!(provenance.source_item_id.as_deref(), Some("source-item"));
        assert_eq!(provenance.created_by, Some(server_actor));
    }
}
