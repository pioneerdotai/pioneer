use super::*;
use crate::authorization::ResourceAction;

fn task_authorization_unavailable(
    request_id: RequestId,
    action: ResourceAction,
    resource_kind: &'static str,
    audit_class: &'static str,
) -> JsonRpcErrorResponse {
    crate::authorization::record_authorization_unavailable(
        action.safe_name(),
        resource_kind,
        audit_class,
    );
    crate::authorization::AuthorizationExternalError::Unavailable.response(request_id)
}

impl MessageProcessor {
    async fn require_member_task_proof(
        &self,
        request_context: &RequestContext,
        request_id: &RequestId,
        proof: Option<&crate::authorization::AuthorizedTask>,
        expected_task_id: &str,
        expected_action: ResourceAction,
    ) -> bool {
        if request_context.principal().kind != pioneer_protocol::PrincipalKind::User {
            return true;
        }
        if proof.is_some_and(|proof| {
            proof.task_id() == expected_task_id && proof.action() == expected_action
        }) {
            return true;
        }
        self.send_error(
            request_context.connection_id(),
            crate::authorization::AuthorizationExternalError::NotFound.response(request_id.clone()),
        )
        .await;
        false
    }

    async fn require_member_task_batch_proof(
        &self,
        request_context: &RequestContext,
        request_id: &RequestId,
        proofs: Option<&[crate::authorization::AuthorizedTask]>,
        params: &TaskWaitParams,
        expected_action: ResourceAction,
    ) -> bool {
        if request_context.principal().kind != pioneer_protocol::PrincipalKind::User {
            return true;
        }
        let Some(proofs) = proofs else {
            self.send_error(
                request_context.connection_id(),
                crate::authorization::AuthorizationExternalError::NotFound
                    .response(request_id.clone()),
            )
            .await;
            return false;
        };
        if !proofs.is_empty()
            && proofs.iter().all(|proof| proof.action() == expected_action)
            && params
                .task_ids
                .iter()
                .all(|task_id| proofs.iter().any(|proof| proof.task_id() == task_id))
        {
            return true;
        }
        self.send_error(
            request_context.connection_id(),
            crate::authorization::AuthorizationExternalError::NotFound.response(request_id.clone()),
        )
        .await;
        false
    }

    async fn task_root_access_filter(
        &self,
        request_context: &RequestContext,
        workspace_id: &str,
    ) -> anyhow::Result<Option<pioneer_crud::TaskRootAccessFilter>> {
        if request_context.principal().kind != pioneer_protocol::PrincipalKind::User {
            return Ok(None);
        }
        let threads = pioneer_crud::list_accessible_threads_for_principal(
            &self.crud_store.database_connection(),
            &request_context.principal().principal_id,
            workspace_id,
            u64::MAX,
        )
        .await?;
        Ok(Some(pioneer_crud::TaskRootAccessFilter {
            allowed_root_thread_ids: threads.into_iter().map(|thread| thread.id).collect(),
        }))
    }

    fn task_mutation_context(
        request_context: &RequestContext,
    ) -> pioneer_tasks::TaskMutationContext {
        pioneer_tasks::TaskMutationContext::user(
            request_context.principal().principal_id.to_string(),
        )
    }

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

        // Keep snapshot identity identical to the executor's restoration rule:
        // Composer work is sourced by its replayed launch turn, while ordinary
        // Tasks fall back to the turn that created them.
        let source_turn_id = params
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.composer_work.as_ref())
            .map(|composer_work| composer_work.launch.turn_id.as_str())
            .or(params.created_by_turn_id.as_deref());
        let Some(source_turn_id) = source_turn_id else {
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
        request_context: &RequestContext,
        authorized_thread: Option<&crate::authorization::AuthorizedThread>,
        request_id: RequestId,
        mut params: TaskCreateParams,
    ) {
        let connection_id = request_context.connection_id();
        if request_context.principal().kind == pioneer_protocol::PrincipalKind::User {
            let Some(authorized_thread) = authorized_thread else {
                self.send_error(
                    connection_id,
                    task_authorization_unavailable(
                        request_id,
                        ResourceAction::TaskRun,
                        "thread",
                        "execution",
                    ),
                )
                .await;
                return;
            };
            if authorized_thread.action() != ResourceAction::TaskRun {
                self.send_error(
                    connection_id,
                    crate::authorization::AuthorizationExternalError::NotFound.response(request_id),
                )
                .await;
                return;
            }
            params.workspace_id = authorized_thread.workspace_id().to_owned();
            params.created_by_thread_id = Some(authorized_thread.thread_id().to_owned());
            params.owner_kind = pioneer_protocol::TaskOwnerKind::User;
            params.owner_id = Some(request_context.principal().principal_id.to_string());
            if let Some(turn_id) = params.created_by_turn_id.as_deref() {
                match self
                    .crud_store
                    .get_turn(authorized_thread.thread_id(), turn_id)
                    .await
                {
                    Ok(Some((workspace_id, _)))
                        if workspace_id == authorized_thread.workspace_id() => {}
                    Ok(_) => {
                        self.send_error(
                            connection_id,
                            JsonRpcErrorResponse::new(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                "task creator turn does not belong to the authorized initiating thread",
                            ),
                        )
                        .await;
                        return;
                    }
                    Err(_) => {
                        self.send_error(
                            connection_id,
                            task_authorization_unavailable(
                                request_id,
                                ResourceAction::TaskRun,
                                "turn",
                                "execution",
                            ),
                        )
                        .await;
                        return;
                    }
                }
            }
        }
        let mut context = match self.task_create_context_for_params(&params).await {
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
        context.actor_id = Some(request_context.principal().principal_id.to_string());
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
        request_context: &RequestContext,
        authorized_task: Option<&crate::authorization::AuthorizedTask>,
        request_id: RequestId,
        params: TaskGetParams,
    ) {
        if !self
            .require_member_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskRead,
            )
            .await
        {
            return;
        }
        let connection_id = request_context.connection_id();
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
        request_context: &RequestContext,
        request_id: RequestId,
        params: TaskListParams,
    ) {
        let connection_id = request_context.connection_id();
        let access = match self
            .task_root_access_filter(request_context, params.workspace_id.as_str())
            .await
        {
            Ok(access) => access,
            Err(_) => {
                self.send_error(
                    connection_id,
                    task_authorization_unavailable(
                        request_id,
                        ResourceAction::TaskRead,
                        "task",
                        "read",
                    ),
                )
                .await;
                return;
            }
        };
        let response = match access.as_ref() {
            Some(access) => {
                self.task_runtime
                    .service()
                    .list_tasks_scoped(params, access)
                    .await
            }
            None => self.task_runtime.service().list_tasks(params).await,
        };
        match response {
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
        request_context: &RequestContext,
        authorized_task: Option<&crate::authorization::AuthorizedTask>,
        request_id: RequestId,
        params: TaskTreeTaskParams,
    ) {
        if !self
            .require_member_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskRead,
            )
            .await
        {
            return;
        }
        let connection_id = request_context.connection_id();
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
        request_context: &RequestContext,
        authorized_task: Option<&crate::authorization::AuthorizedTask>,
        request_id: RequestId,
        params: TaskEventsParams,
    ) {
        if !self
            .require_member_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskRead,
            )
            .await
        {
            return;
        }
        let connection_id = request_context.connection_id();
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
        request_context: &RequestContext,
        authorized_tasks: Option<&[crate::authorization::AuthorizedTask]>,
        request_id: RequestId,
        params: TaskWaitParams,
    ) {
        if !self
            .require_member_task_batch_proof(
                request_context,
                &request_id,
                authorized_tasks,
                &params,
                ResourceAction::TaskRead,
            )
            .await
        {
            return;
        }
        let connection_id = request_context.connection_id();
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
        request_context: &RequestContext,
        authorized_task: Option<&crate::authorization::AuthorizedTask>,
        request_id: RequestId,
        params: TaskAcceptParams,
    ) {
        if !self
            .require_member_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskManage,
            )
            .await
        {
            return;
        }
        let connection_id = request_context.connection_id();
        let context = Self::task_mutation_context(request_context);
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
        request_context: &RequestContext,
        authorized_task: Option<&crate::authorization::AuthorizedTask>,
        request_id: RequestId,
        params: TaskReviseParams,
    ) {
        if !self
            .require_member_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskManage,
            )
            .await
        {
            return;
        }
        let connection_id = request_context.connection_id();
        let context = Self::task_mutation_context(request_context);
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
        request_context: &RequestContext,
        authorized_task: Option<&crate::authorization::AuthorizedTask>,
        request_id: RequestId,
        params: TaskCancelParams,
    ) {
        if !self
            .require_member_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskManage,
            )
            .await
        {
            return;
        }
        let connection_id = request_context.connection_id();
        match message_future(
            self.task_runtime
                .service()
                .cancel_task(Self::task_mutation_context(request_context), params),
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
        request_context: &RequestContext,
        authorized_task: Option<&crate::authorization::AuthorizedTask>,
        request_id: RequestId,
        params: TaskDetachParams,
    ) {
        if !self
            .require_member_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskManage,
            )
            .await
        {
            return;
        }
        let connection_id = request_context.connection_id();
        match message_future(
            self.task_runtime
                .service()
                .detach_task(Self::task_mutation_context(request_context), params),
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
        request_context: &RequestContext,
        authorized_task: Option<&crate::authorization::AuthorizedTask>,
        request_id: RequestId,
        params: TaskRescheduleParams,
    ) {
        if !self
            .require_member_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskManage,
            )
            .await
        {
            return;
        }
        let connection_id = request_context.connection_id();
        match message_future(
            self.task_runtime
                .service()
                .reschedule_task(Self::task_mutation_context(request_context), params),
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
        request_context: &RequestContext,
        authorized_task: Option<&crate::authorization::AuthorizedTask>,
        request_id: RequestId,
        params: TaskPauseParams,
    ) {
        if !self
            .require_member_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskManage,
            )
            .await
        {
            return;
        }
        let connection_id = request_context.connection_id();
        match message_future(
            self.task_runtime
                .service()
                .pause_task(Self::task_mutation_context(request_context), params),
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
        request_context: &RequestContext,
        authorized_task: Option<&crate::authorization::AuthorizedTask>,
        request_id: RequestId,
        params: TaskResumeParams,
    ) {
        if !self
            .require_member_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskManage,
            )
            .await
        {
            return;
        }
        let connection_id = request_context.connection_id();
        match message_future(
            self.task_runtime
                .service()
                .resume_task(Self::task_mutation_context(request_context), params),
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
        request_context: &RequestContext,
        request_id: RequestId,
        params: TaskAgendaParams,
    ) {
        let connection_id = request_context.connection_id();
        let access = match self
            .task_root_access_filter(request_context, params.workspace_id.as_str())
            .await
        {
            Ok(access) => access,
            Err(_) => {
                self.send_error(
                    connection_id,
                    task_authorization_unavailable(
                        request_id,
                        ResourceAction::TaskRead,
                        "task",
                        "read",
                    ),
                )
                .await;
                return;
            }
        };
        let response = match access.as_ref() {
            Some(access) => {
                message_future(
                    self.task_runtime
                        .service()
                        .list_agenda_scoped(params, access),
                )
                .await
            }
            None => message_future(self.task_runtime.service().list_agenda(params)).await,
        };
        match response {
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
        request_context: &RequestContext,
        request_id: RequestId,
        params: TaskDeliveriesParams,
    ) {
        let connection_id = request_context.connection_id();
        let access = match self
            .task_root_access_filter(request_context, params.workspace_id.as_str())
            .await
        {
            Ok(access) => access,
            Err(_) => {
                self.send_error(
                    connection_id,
                    task_authorization_unavailable(
                        request_id,
                        ResourceAction::TaskRead,
                        "task",
                        "read",
                    ),
                )
                .await;
                return;
            }
        };
        let response = match access.as_ref() {
            Some(access) => {
                self.task_runtime
                    .service()
                    .list_deliveries_scoped(params, access)
                    .await
            }
            None => self.task_runtime.service().list_deliveries(params).await,
        };
        match response {
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
