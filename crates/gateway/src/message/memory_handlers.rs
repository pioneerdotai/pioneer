use super::*;
use anyhow::{Result, anyhow};
use pioneer_memory::{
    MemoryMutationBoundary, MemoryOperationContext, MemoryReadPolicy, MemorySourceAccessPolicy,
};
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
        let context = match self.memory_context(request_context, None).await {
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
        let context = match self.memory_context(request_context, None).await {
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
        let context = match self.memory_context(request_context, None).await {
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
        let mut context = match self.memory_context(request_context, Some(actor)).await {
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
        if let Err(error) = self
            .authorize_scoped_memory_remember(request_context, &mut context, &mut params)
            .await
        {
            self.send_memory_service_error(
                connection_id,
                request_id,
                methods::MEMORY_REMEMBER,
                error,
            )
            .await;
            return;
        }
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
        let mut context = match self
            .memory_context(request_context, params.actor.clone())
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
        if let Err(error) = self
            .authorize_scoped_memory_forget(request_context, &mut context, &params)
            .await
        {
            self.send_memory_service_error(
                connection_id,
                request_id,
                methods::MEMORY_FORGET,
                error,
            )
            .await;
            return;
        }

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
        let context = match self.memory_context(request_context, None).await {
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
        let context = match self.memory_context(request_context, None).await {
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
            .memory_context(request_context, params.actor.clone())
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
            .memory_context(request_context, params.actor.clone())
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
            .memory_context(request_context, params.actor.clone())
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
            .memory_context(request_context, params.actor.clone())
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
            .memory_context(request_context, params.actor.clone())
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
            .memory_context(request_context, params.actor.clone())
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
        request_context: &RequestContext,
        actor: Option<MemoryActor>,
    ) -> Result<MemoryOperationContext> {
        let connection_id = request_context.connection_id();
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

        let mut context = self
            .memory_runtime
            .operation_context(Some(workspace_id.clone()), actor);
        if crate::authorization::AuthorizationService::new().runtime_principal_policy(
            request_context.principal().kind,
            request_context.principal().role_key.as_ref(),
        ) == Some(crate::authorization::RuntimePrincipalPolicy::ScopedCollaboration)
        {
            let accessible_threads = self
                .crud_store
                .list_accessible_threads_for_principal(
                    &request_context.principal().principal_id,
                    workspace_id.as_str(),
                    u64::MAX,
                )
                .await
                .context("failed to resolve Member memory provenance scope")?;
            context.allow_global_user = false;
            context.allow_global_agent = false;
            context.read_policy = Some(MemoryReadPolicy {
                allow_normal: true,
                allow_personal: false,
                allow_secret_like: false,
                allow_regulated: false,
            });
            context.source_access = MemorySourceAccessPolicy::accessible_threads(
                accessible_threads.into_iter().map(|thread| thread.id),
            );
        }
        Ok(context)
    }

    async fn authorize_scoped_memory_remember(
        &self,
        request_context: &RequestContext,
        context: &mut MemoryOperationContext,
        params: &mut MemoryRememberParams,
    ) -> Result<()> {
        if !request_uses_scoped_collaboration_policy(request_context) {
            return Ok(());
        }
        let workspace_id = context
            .workspace_id
            .as_deref()
            .context("memory mutation has no workspace scope")?;
        let source_thread_id = params
            .provenance
            .as_ref()
            .and_then(|provenance| provenance.source_thread_id.as_deref())
            .context("collaborative memory creation requires a source thread")?
            .to_owned();
        match params.scope.kind {
            MemoryScopeKind::Thread if params.scope.key == source_thread_id => {}
            MemoryScopeKind::User | MemoryScopeKind::Workspace | MemoryScopeKind::Agent => {
                return Err(anyhow!(
                    "personal, workspace, and agent memory require a separate authorization scope"
                ));
            }
            MemoryScopeKind::Task => {
                return Err(anyhow!(
                    "direct task memory mutation requires an execution-owned memory context"
                ));
            }
            MemoryScopeKind::Thread => {
                return Err(anyhow!("memory scope does not match its authorized source"));
            }
        }
        let root_thread_id = self
            .authorize_scoped_memory_source_thread(
                request_context,
                workspace_id,
                source_thread_id.as_str(),
                crate::authorization::ResourceAction::MemoryCreateThread,
            )
            .await?;
        // A keyed remember is an atomic upsert, and a supersession both
        // creates a replacement and mutates an existing record. Requiring the
        // update action up front prevents a concurrent create from turning a
        // create-only grant into an unauthorized update.
        if params.key.is_some() || params.supersedes.is_some() {
            let update_root = self
                .authorize_scoped_memory_source_thread(
                    request_context,
                    workspace_id,
                    source_thread_id.as_str(),
                    crate::authorization::ResourceAction::MemoryUpdateThread,
                )
                .await?;
            if update_root != root_thread_id {
                return Err(anyhow!("memory upsert authority changed during admission"));
            }
        }
        params.scope.key = root_thread_id.clone();
        if let Some(provenance) = params.provenance.as_mut() {
            provenance.source_thread_id = Some(root_thread_id.clone());
        }
        context.thread_id = Some(root_thread_id.clone());
        context.mutation_boundary = MemoryMutationBoundary::thread_capsule(root_thread_id, None);
        Ok(())
    }

    async fn authorize_scoped_memory_forget(
        &self,
        request_context: &RequestContext,
        context: &mut MemoryOperationContext,
        params: &MemoryForgetParams,
    ) -> Result<()> {
        if !request_uses_scoped_collaboration_policy(request_context) {
            return Ok(());
        }
        let service = self.memory_runtime.service();
        let record = match &params.target {
            pioneer_protocol::MemoryForgetTarget::Id { memory_id } => {
                service
                    .get(
                        context.clone(),
                        MemoryGetParams {
                            memory_id: memory_id.clone(),
                            include_deleted: false,
                        },
                    )
                    .await?
                    .record
            }
            pioneer_protocol::MemoryForgetTarget::ScopedKey {
                scope,
                namespace,
                key,
            } => {
                service
                    .get_by_key(
                        context.clone(),
                        scope.clone(),
                        namespace.clone(),
                        key.clone(),
                    )
                    .await?
                    .record
            }
        }
        .context("memory is not available in the collaboration scope")?;
        let source_thread_id = record
            .provenance
            .source_thread_id
            .as_deref()
            .context("workspace-global memory requires moderator authority")?;
        let workspace_id = context
            .workspace_id
            .as_deref()
            .context("memory mutation has no workspace scope")?;
        let root_thread_id = self
            .authorize_scoped_memory_source_thread(
                request_context,
                workspace_id,
                source_thread_id,
                crate::authorization::ResourceAction::MemoryForgetThread,
            )
            .await?;
        if record.scope.kind != MemoryScopeKind::Thread || record.scope.key != root_thread_id {
            return Err(anyhow!(
                "workspace-global and cross-capsule memory require separate authority"
            ));
        }
        context.thread_id = Some(root_thread_id.clone());
        context.mutation_boundary = MemoryMutationBoundary::thread_capsule(root_thread_id, None);
        Ok(())
    }

    async fn authorize_scoped_memory_source_thread(
        &self,
        request_context: &RequestContext,
        workspace_id: &str,
        source_thread_id: &str,
        action: crate::authorization::ResourceAction,
    ) -> Result<String> {
        let root_thread_id = self
            .crud_store
            .get_task_thread_lineage(source_thread_id)
            .await?
            .map(|lineage| lineage.root_thread_id)
            .unwrap_or_else(|| source_thread_id.to_owned());
        let principal = request_context.principal();
        let action_gate = crate::authorization::AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            action,
        );
        match crate::authorization::AuthorizationResolver::new(self.crud_store.as_ref().clone())
            .authorize_thread(
                principal,
                &action_gate,
                action,
                root_thread_id.as_str(),
                Some(workspace_id),
            )
            .await?
        {
            crate::authorization::ProofResolution::Authorized(_) => Ok(root_thread_id),
            crate::authorization::ProofResolution::Denied(_) => Err(anyhow!(
                "memory is outside the authorized collaboration root"
            )),
        }
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
        initiating_principal_id: Option<&str>,
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

        self.send_memory_notification_for_scope(
            context,
            initiating_principal_id,
            &notification.scope,
            notification
                .record
                .as_ref()
                .and_then(|record| record.provenance.source_thread_id.as_deref())
                .or(context.thread_id.as_deref()),
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
            self.send_memory_notification_for_scope(
                context,
                None,
                &notification.scope,
                notification
                    .record
                    .as_ref()
                    .and_then(|record| record.provenance.source_thread_id.as_deref())
                    .or(context.thread_id.as_deref()),
                events::MEMORY_CHANGED,
                &notification,
            )
            .await;
        }
    }

    pub(crate) async fn send_memory_forgotten_after_tool_forget(
        &self,
        context: &MemoryOperationContext,
        initiating_principal_id: Option<&str>,
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
        if let Some(initiating_principal_id) = initiating_principal_id {
            self.send_principal_owned_notification(
                initiating_principal_id,
                events::MEMORY_FORGOTTEN,
                &notification,
            )
            .await;
            return;
        }
        if context
            .actor
            .as_ref()
            .is_some_and(|actor| actor.kind == MemoryActorKind::Assistant)
        {
            self.send_superuser_personal_notification(events::MEMORY_FORGOTTEN, &notification)
                .await;
            return;
        }
        if let Some(task_id) = context.task_id.as_deref() {
            if let Some(workspace_id) = context.workspace_id.as_deref() {
                self.send_notification_to_task_workspace_connections(
                    task_id,
                    workspace_id,
                    events::MEMORY_FORGOTTEN,
                    &notification,
                )
                .await;
            }
        } else if let Some(thread_id) = context.thread_id.as_deref() {
            self.send_notification_to_thread_subscribers(
                thread_id,
                events::MEMORY_FORGOTTEN,
                &notification,
            )
            .await;
        } else if let Some(workspace_id) = context.workspace_id.as_deref() {
            self.send_notification_to_workspace_connections(
                workspace_id,
                events::MEMORY_FORGOTTEN,
                &notification,
            )
            .await;
        }
    }

    pub(super) async fn send_memory_changed_notification(
        &self,
        connection_id: ConnectionId,
        context: &MemoryOperationContext,
        notification: &MemoryChangedNotification,
    ) {
        if self
            .send_memory_notification_for_scope(
                context,
                None,
                &notification.scope,
                notification
                    .record
                    .as_ref()
                    .and_then(|record| record.provenance.source_thread_id.as_deref())
                    .or(context.thread_id.as_deref()),
                events::MEMORY_CHANGED,
                notification,
            )
            .await
        {
            return;
        }

        self.send_notification_to_connections(
            events::MEMORY_CHANGED,
            notification,
            vec![connection_id],
        )
        .await;
    }

    async fn send_memory_notification_for_scope<T: Serialize>(
        &self,
        context: &MemoryOperationContext,
        initiating_principal_id: Option<&str>,
        scope: &pioneer_protocol::MemoryScope,
        source_thread_id: Option<&str>,
        method: &str,
        notification: &T,
    ) -> bool {
        match scope.kind {
            MemoryScopeKind::Workspace => {
                let Some(workspace_id) = context.workspace_id.as_deref() else {
                    return false;
                };
                if scope.key != workspace_id {
                    return false;
                }
                if let Some(source_thread_id) = source_thread_id
                    .map(str::trim)
                    .filter(|source_thread_id| !source_thread_id.is_empty())
                {
                    let candidate_connection_ids = self
                        .session_manager
                        .connection_ids_for_workspace(workspace_id)
                        .await;
                    self.send_thread_scoped_notification_to_connections(
                        source_thread_id,
                        method,
                        notification,
                        candidate_connection_ids,
                    )
                    .await;
                } else {
                    self.send_notification_to_workspace_connections(
                        workspace_id,
                        method,
                        notification,
                    )
                    .await;
                }
                true
            }
            MemoryScopeKind::Thread => {
                if scope.key.trim().is_empty() {
                    return false;
                }
                self.send_notification_to_thread_subscribers(
                    scope.key.as_str(),
                    method,
                    notification,
                )
                .await;
                true
            }
            MemoryScopeKind::Task => {
                let Some(workspace_id) = context.workspace_id.as_deref() else {
                    return false;
                };
                if scope.key.trim().is_empty() {
                    return false;
                }
                self.send_notification_to_task_workspace_connections(
                    scope.key.as_str(),
                    workspace_id,
                    method,
                    notification,
                )
                .await;
                true
            }
            MemoryScopeKind::User | MemoryScopeKind::Agent => {
                if let Some(initiating_principal_id) = initiating_principal_id {
                    self.send_principal_owned_notification(
                        initiating_principal_id,
                        method,
                        notification,
                    )
                    .await;
                } else {
                    self.send_superuser_personal_notification(method, notification)
                        .await;
                }
                true
            }
        }
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

fn authenticated_memory_request_actor(request_context: &RequestContext) -> MemoryActor {
    MemoryActor {
        kind: MemoryActorKind::User,
        id: Some(request_context.principal().principal_id.to_string()),
    }
}

fn request_uses_scoped_collaboration_policy(request_context: &RequestContext) -> bool {
    crate::authorization::AuthorizationService::new().runtime_principal_policy(
        request_context.principal().kind,
        request_context.principal().role_key.as_ref(),
    ) == Some(crate::authorization::RuntimePrincipalPolicy::ScopedCollaboration)
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
    fn rpc_memory_actor_is_server_derived_from_authenticated_principal() {
        let connection = ConnectionContext::new(17, authenticated_test_superuser());
        let context = RequestContext::new(
            &connection,
            Some(RequestId::new("R00000000000000000001").expect("request id")),
            CanonicalMethod::rpc("memory/candidates/decide"),
        );

        let actor = authenticated_memory_request_actor(&context);

        assert_eq!(actor.kind, MemoryActorKind::User);
        assert_eq!(
            actor.id.as_deref(),
            Some(authenticated_test_superuser().principal_id.as_str())
        );
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
