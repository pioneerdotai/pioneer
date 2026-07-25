use super::*;

impl MessageProcessor {
    pub(crate) async fn task_create_context_for_params(
        &self,
        params: &TaskCreateParams,
    ) -> anyhow::Result<pioneer_tasks::TaskCreateContext> {
        if params.trigger.spec.kind() != pioneer_protocol::TaskTriggerKind::Immediate {
            return Ok(pioneer_tasks::TaskCreateContext::default());
        }
        let attachment = params
            .lifecycle_policy
            .as_ref()
            .map(|policy| policy.attachment)
            .unwrap_or_else(|| {
                if params.created_by_turn_id.is_some() {
                    pioneer_protocol::TaskAttachmentMode::Attached
                } else {
                    pioneer_protocol::TaskAttachmentMode::Detached
                }
            });
        if attachment != pioneer_protocol::TaskAttachmentMode::Detached {
            return Ok(pioneer_tasks::TaskCreateContext::default());
        }

        let Some(source_turn_id) = params.created_by_turn_id.as_deref() else {
            // An immediate detached Task without a creator turn is frozen by
            // the executor at run admission, where the run identity exists.
            return Ok(pioneer_tasks::TaskCreateContext::default());
        };
        let conversation_thread_id = params
            .created_by_thread_id
            .clone()
            .or_else(|| {
                (params.owner_kind == pioneer_protocol::TaskOwnerKind::Thread)
                    .then(|| params.owner_id.clone())
                    .flatten()
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "immediate detached Task `{}` has no conversation thread to snapshot",
                    params.title
                )
            })?;
        let thread = self
            .crud_store
            .get_thread_by_id(conversation_thread_id.as_str())
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Task conversation thread `{conversation_thread_id}` does not exist"
                )
            })?;
        if thread.workspace_id != params.workspace_id {
            anyhow::bail!(
                "Task conversation thread `{conversation_thread_id}` belongs to another workspace"
            );
        }
        let fallback_model = params
            .agent_spec
            .as_ref()
            .and_then(|spec| spec.model.as_deref())
            .unwrap_or(thread.model.as_str());
        let fallback_model_provider = params
            .agent_spec
            .as_ref()
            .and_then(|spec| spec.model_provider.as_deref())
            .unwrap_or(thread.model_provider.as_str());
        let history = self
            .load_conversation_history_for_workspace_in_execution_excluding_turn(
                params.workspace_id.as_str(),
                conversation_thread_id.as_str(),
                conversation_thread_id.as_str(),
                source_turn_id,
                Some(source_turn_id),
                Some(fallback_model),
                Some(fallback_model_provider),
            )
            .await;

        Ok(pioneer_tasks::TaskCreateContext {
            conversation_snapshot: Some(pioneer_tasks::TaskRunConversationSnapshotSeed {
                conversation_thread_id,
                source_turn_id: Some(source_turn_id.to_owned()),
                history_json: serde_json::to_string(&history)
                    .context("failed to serialize detached Task conversation snapshot")?,
            }),
            ..Default::default()
        })
    }

    pub(super) async fn task_create(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TaskCreateParams,
    ) {
        let context = match self.task_create_context_for_params(&params).await {
            Ok(context) => context,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to freeze task context: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        match message_future(self.task_runtime.service().create_task(context, params)).await {
            Ok(response_payload) => {
                self.send_task_response(connection_id, request_id, &response_payload)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to create task: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    pub(super) async fn task_get(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TaskGetParams,
    ) {
        match self.task_runtime.service().get_task(params).await {
            Ok(response_payload) => {
                self.send_task_response(connection_id, request_id, &response_payload)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to get task: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    pub(super) async fn task_list(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TaskListParams,
    ) {
        match self.task_runtime.service().list_tasks(params).await {
            Ok(response_payload) => {
                self.send_task_response(connection_id, request_id, &response_payload)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to list tasks: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    pub(super) async fn task_tree(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TaskTreeTaskParams,
    ) {
        match self.task_runtime.service().get_task_tree(params).await {
            Ok(response_payload) => {
                self.send_task_response(connection_id, request_id, &response_payload)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to get task tree: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    pub(super) async fn task_events(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TaskEventsParams,
    ) {
        match self.task_runtime.service().get_task_events(params).await {
            Ok(response_payload) => {
                self.send_task_response(connection_id, request_id, &response_payload)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to get task events: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    pub(super) async fn task_wait(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TaskWaitParams,
    ) {
        match self
            .task_runtime
            .service()
            .wait_tasks(pioneer_tasks::TaskWaitContext::default(), params)
            .await
        {
            Ok(response_payload) => {
                self.send_task_response(connection_id, request_id, &response_payload)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to wait for task: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    pub(super) async fn task_accept(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TaskAcceptParams,
    ) {
        let context =
            pioneer_tasks::TaskMutationContext::user(format!("connection:{connection_id}"));
        match message_future(
            self.task_runtime
                .service()
                .accept_task_result_candidate(context, params),
        )
        .await
        {
            Ok(response_payload) => {
                self.send_task_response(connection_id, request_id, &response_payload)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to accept task result: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    pub(super) async fn task_revise(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TaskReviseParams,
    ) {
        let context =
            pioneer_tasks::TaskMutationContext::user(format!("connection:{connection_id}"));
        let response_payload = match message_future(
            self.task_runtime
                .service()
                .revise_task_result_candidate(context, params),
        )
        .await
        {
            Ok(revised) => {
                let task_agent_executor = self.task_agent_executor.clone();
                message_fresh_task(async move {
                    task_agent_executor.dispatch_revision_turn(revised).await
                })
                .await
                .context("task revision dispatch task failed")
                .and_then(|result| result)
            }
            Err(error) => Err(error),
        };
        match response_payload {
            Ok(response_payload) => {
                self.send_task_response(connection_id, request_id, &response_payload)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to revise task result: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    pub(super) async fn task_cancel(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TaskCancelParams,
    ) {
        match message_future(
            self.task_runtime
                .service()
                .cancel_task(pioneer_tasks::TaskMutationContext::default(), params),
        )
        .await
        {
            Ok(response_payload) => {
                self.send_task_response(connection_id, request_id, &response_payload)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to cancel task: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    pub(super) async fn task_detach(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TaskDetachParams,
    ) {
        match message_future(
            self.task_runtime
                .service()
                .detach_task(pioneer_tasks::TaskMutationContext::default(), params),
        )
        .await
        {
            Ok(response_payload) => {
                self.send_task_response(connection_id, request_id, &response_payload)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to detach task: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    pub(super) async fn task_reschedule(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TaskRescheduleParams,
    ) {
        match message_future(
            self.task_runtime
                .service()
                .reschedule_task(pioneer_tasks::TaskMutationContext::default(), params),
        )
        .await
        {
            Ok(response_payload) => {
                self.send_task_response(connection_id, request_id, &response_payload)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to reschedule task: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    pub(super) async fn task_pause(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TaskPauseParams,
    ) {
        match message_future(
            self.task_runtime
                .service()
                .pause_task(pioneer_tasks::TaskMutationContext::default(), params),
        )
        .await
        {
            Ok(response_payload) => {
                self.send_task_response(connection_id, request_id, &response_payload)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to pause task: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    pub(super) async fn task_resume(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TaskResumeParams,
    ) {
        match message_future(
            self.task_runtime
                .service()
                .resume_task(pioneer_tasks::TaskMutationContext::default(), params),
        )
        .await
        {
            Ok(response_payload) => {
                self.send_task_response(connection_id, request_id, &response_payload)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to resume task: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    pub(super) async fn task_agenda(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TaskAgendaParams,
    ) {
        match message_future(self.task_runtime.service().list_agenda(params)).await {
            Ok(response_payload) => {
                self.send_task_response(connection_id, request_id, &response_payload)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to list task agenda: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    pub(super) async fn task_deliveries(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TaskDeliveriesParams,
    ) {
        match self.task_runtime.service().list_deliveries(params).await {
            Ok(response_payload) => {
                self.send_task_response(connection_id, request_id, &response_payload)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to list task deliveries: {error:#}"),
                    ),
                )
                .await;
            }
        }
    }

    fn send_task_response<'a, T: Serialize + Sync + 'a>(
        &'a self,
        connection_id: ConnectionId,
        request_id: RequestId,
        response_payload: &'a T,
    ) -> MessageFuture<'a, ()> {
        message_future(async move {
            let response = match JsonRpcResponse::from_result(request_id, response_payload) {
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
                    "failed to send task response"
                );
            }
        })
    }
}
