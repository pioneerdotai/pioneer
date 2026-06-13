use super::*;

impl MessageProcessor {
    pub(super) async fn task_create(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TaskCreateParams,
    ) {
        match message_future(
            self.task_runtime
                .service()
                .create_task(pioneer_tasks::TaskCreateContext::default(), params),
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
                message_future(self.task_agent_executor.dispatch_revision_turn(revised)).await
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
