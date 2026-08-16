use super::*;
use crate::authorization::ResourceAction;

struct TaskObservationAdmission {
    budget: pioneer_protocol::TaskResourceBudget,
    _permit: crate::authorization::ObservationAdmissionPermit,
}

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

fn task_public_error(
    request_id: Option<RequestId>,
    stage: pioneer_protocol::PublicErrorStage,
    diagnostic: impl std::fmt::Display,
) -> JsonRpcErrorResponse {
    crate::public_error::agent_rpc_error(
        request_id,
        INVALID_REQUEST_CODE,
        pioneer_protocol::PublicErrorCode::Internal,
        stage,
        diagnostic,
    )
}

impl MessageProcessor {
    pub(crate) async fn task_execution_readmission_seed(
        &self,
        principal: &crate::auth::AuthenticatedSessionPrincipal,
        preferred_root_thread_id: Option<&str>,
        task_id: &str,
    ) -> anyhow::Result<Option<pioneer_tasks::TaskExecutionAdmissionSeed>> {
        let response = self
            .crud_store
            .get_task(task_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("task `{task_id}` not found"))?;
        if response.task.status != pioneer_protocol::TaskStatus::Blocked
            || response.task.executor_kind != pioneer_protocol::TaskExecutorKind::Agent
        {
            return Ok(None);
        }
        let authorization_policy = crate::authorization::AuthorizationService::new();
        let execution_resources = authorization_policy
            .execution_resource_policy(principal.kind, principal.role_key.as_ref())
            .ok_or_else(|| anyhow::anyhow!("Task role has no execution resource policy"))?;
        let task_resources = authorization_policy
            .task_resource_budget(principal.kind, principal.role_key.as_ref())
            .ok_or_else(|| anyhow::anyhow!("Task role has no Task resource budget"))?;
        if let Some(persisted) = self
            .crud_store
            .get_task_execution_admission(task_id)
            .await?
        {
            let context = crate::authorization::ExecutionAuthorizationContext::from_persisted_json(
                persisted.authorization_context_json.as_str(),
            )?;
            let seed = pioneer_tasks::TaskExecutionAdmissionSeed {
                workspace_id: persisted.workspace_id,
                root_thread_id: persisted.root_thread_id,
                initiating_principal_id: persisted.initiating_principal_id,
                authorization_context_json: persisted.authorization_context_json,
                role_key: context.role_key().to_owned(),
                policy_fingerprint: context.policy_fingerprint().to_owned(),
                execution_resources,
                task_resources,
            };
            self.validate_task_execution_admission_seed(&seed).await?;
            return Ok(Some(seed));
        }
        let root_thread_id = preferred_root_thread_id
            .map(str::to_owned)
            .or_else(|| response.task.created_by_thread_id.clone())
            .or_else(|| {
                (response.task.owner_kind == pioneer_protocol::TaskOwnerKind::Thread)
                    .then(|| response.task.owner_id.clone())
                    .flatten()
            })
            .ok_or_else(|| {
                anyhow::anyhow!("blocked Agent Task `{task_id}` has no authoritative root thread")
            })?;
        let root_thread = self
            .crud_store
            .get_thread_by_id(root_thread_id.as_str())
            .await?
            .filter(|thread| thread.workspace_id == response.task.workspace_id)
            .ok_or_else(|| {
                anyhow::anyhow!("blocked Agent Task `{task_id}` root thread is unavailable")
            })?;
        let mut request = crate::authorization::ExecutionAdmissionRequest::for_existing_task(
            &response,
            root_thread_id.as_str(),
            root_thread.model_provider.as_str(),
            root_thread.model.as_str(),
            None,
        )?;
        if !matches!(
            request.execution_backend,
            Some(pioneer_protocol::AgentExecutionBackend::CLIAgentRuntime { .. })
                | Some(pioneer_protocol::AgentExecutionBackend::ACPAgentRuntime { .. })
        ) {
            request.provider_authority_fingerprint = Some(
                self.provider_registry
                    .authority_fingerprint_for_workspace(
                        response.task.workspace_id.as_str(),
                        request.provider.as_str(),
                    )
                    .as_str()
                    .to_owned(),
            );
        }
        let revision = self.current_authorization_revision().await?;
        let requested_permission_cap = response
            .agent_specs
            .iter()
            .rev()
            .find(|spec| spec.run_id.is_none())
            .and_then(|spec| spec.permission_cap.as_ref());
        let context =
            crate::authorization::ExecutionAdmissionService::new(self.crud_store.as_ref().clone())
                .admit_context(principal, revision, &request, requested_permission_cap)
                .await?;
        let seed = pioneer_tasks::TaskExecutionAdmissionSeed {
            workspace_id: context.workspace_id().to_owned(),
            root_thread_id: context.root_thread_id().to_owned(),
            initiating_principal_id: context.initiating_principal_id().to_string(),
            authorization_context_json: context.to_persisted_json()?,
            role_key: context.role_key().to_owned(),
            policy_fingerprint: context.policy_fingerprint().to_owned(),
            execution_resources,
            task_resources,
        };
        self.validate_task_execution_admission_seed(&seed).await?;
        Ok(Some(seed))
    }

    async fn acquire_task_observation_page(
        &self,
        request_context: &RequestContext,
        request_id: &RequestId,
        workspace_id: &str,
    ) -> Option<TaskObservationAdmission> {
        let principal = request_context.principal();
        let policy = crate::authorization::AuthorizationService::new();
        let Some(role_key) = policy.resolved_role_key(principal.kind, principal.role_key.as_ref())
        else {
            self.send_error(
                request_context.connection_id(),
                crate::authorization::AuthorizationExternalError::Unavailable
                    .response(request_id.clone()),
            )
            .await;
            return None;
        };
        let Some(observation_policy) =
            policy.observation_resource_policy(principal.kind, principal.role_key.as_ref())
        else {
            self.send_error(
                request_context.connection_id(),
                crate::authorization::AuthorizationExternalError::Unavailable
                    .response(request_id.clone()),
            )
            .await;
            return None;
        };
        let Some(budget) = policy.task_resource_budget(principal.kind, principal.role_key.as_ref())
        else {
            self.send_error(
                request_context.connection_id(),
                crate::authorization::AuthorizationExternalError::Unavailable
                    .response(request_id.clone()),
            )
            .await;
            return None;
        };
        let permit = match self
            .observation_governor
            .acquire_page(
                principal.principal_id.as_str(),
                role_key,
                workspace_id,
                observation_policy,
            )
            .await
        {
            Ok(permit) => permit,
            Err(error) => {
                warn!(
                    principal_id = %principal.principal_id,
                    workspace_id,
                    error = %format!("{error:#}"),
                    "task observation page rejected by resource governor"
                );
                self.send_error(
                    request_context.connection_id(),
                    crate::authorization::AuthorizationExternalError::Unavailable
                        .response(request_id.clone()),
                )
                .await;
                return None;
            }
        };
        Some(TaskObservationAdmission {
            budget,
            _permit: permit,
        })
    }

    fn uses_scoped_task_policy(request_context: &RequestContext) -> bool {
        crate::authorization::AuthorizationService::new().runtime_principal_policy(
            request_context.principal().kind,
            request_context.principal().role_key.as_ref(),
        ) == Some(crate::authorization::RuntimePrincipalPolicy::ScopedCollaboration)
    }

    async fn require_scoped_task_delivery_policy(
        &self,
        request_context: &RequestContext,
        request_id: &RequestId,
        workspace_id: &str,
        policy: Option<&pioneer_protocol::TaskDeliveryPolicy>,
    ) -> bool {
        if !Self::uses_scoped_task_policy(request_context) {
            return true;
        }
        let Some(policy) = policy else {
            return true;
        };
        match policy.mode {
            pioneer_protocol::TaskDeliveryMode::None
            | pioneer_protocol::TaskDeliveryMode::UserNotification => true,
            pioneer_protocol::TaskDeliveryMode::Webhook => {
                self.send_error(
                    request_context.connection_id(),
                    crate::authorization::AuthorizationExternalError::Forbidden
                        .response(request_id.clone()),
                )
                .await;
                false
            }
            pioneer_protocol::TaskDeliveryMode::Thread => {
                let Some(thread_id) = policy
                    .thread_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                else {
                    self.send_error(
                        request_context.connection_id(),
                        crate::public_error::agent_rpc_error(
                            Some(request_id.clone()),
                            INVALID_PARAMS_CODE,
                            pioneer_protocol::PublicErrorCode::InvalidInput,
                            pioneer_protocol::PublicErrorStage::Admission,
                            "thread delivery requires delivery_policy.thread_id",
                        ),
                    )
                    .await;
                    return false;
                };
                let action = ResourceAction::MessageCreate;
                let gate = crate::authorization::AuthorizationService::new().authorize_action(
                    request_context.principal().kind,
                    request_context.principal().role_key.as_ref(),
                    action,
                );
                let resolver = crate::authorization::AuthorizationResolver::new(
                    self.crud_store.as_ref().clone(),
                );
                let mut resolution = resolver
                    .authorize_thread(
                        request_context.principal(),
                        &gate,
                        action,
                        thread_id,
                        Some(workspace_id),
                    )
                    .await;
                if matches!(
                    resolution.as_ref().ok().and_then(|value| value.denial()),
                    Some(crate::authorization::AuthorizationDecision::Deny {
                        reason: crate::authorization::DenyReason::MissingAuthoritativeResource,
                        ..
                    })
                ) {
                    resolution = resolver
                        .authorize_internal_thread_via_root(
                            request_context.principal(),
                            &gate,
                            action,
                            thread_id,
                            Some(workspace_id),
                        )
                        .await;
                }
                match resolution {
                    Ok(crate::authorization::ProofResolution::Authorized(_)) => true,
                    Ok(crate::authorization::ProofResolution::Denied(_)) => {
                        self.send_error(
                            request_context.connection_id(),
                            crate::authorization::AuthorizationExternalError::NotFound
                                .response(request_id.clone()),
                        )
                        .await;
                        false
                    }
                    Err(_) => {
                        self.send_error(
                            request_context.connection_id(),
                            task_authorization_unavailable(
                                request_id.clone(),
                                action,
                                "thread",
                                "execution",
                            ),
                        )
                        .await;
                        false
                    }
                }
            }
        }
    }

    async fn require_scoped_task_proof(
        &self,
        request_context: &RequestContext,
        request_id: &RequestId,
        proof: Option<&crate::authorization::AuthorizedTask>,
        expected_task_id: &str,
        expected_action: ResourceAction,
    ) -> bool {
        if !Self::uses_scoped_task_policy(request_context) {
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

    async fn require_scoped_task_batch_proof(
        &self,
        request_context: &RequestContext,
        request_id: &RequestId,
        proofs: Option<&[crate::authorization::AuthorizedTask]>,
        params: &TaskWaitParams,
        expected_action: ResourceAction,
    ) -> bool {
        if !Self::uses_scoped_task_policy(request_context) {
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

    /// Resolves operator disclosure against the exact Task. An action-level
    /// grant is final only for an absolute role; a future scoped operator role
    /// must pass the same authoritative Task facts as every mutation/read.
    async fn task_operator_projection_allowed(
        &self,
        request_context: &RequestContext,
        task_id: Option<&str>,
        workspace_id: Option<&str>,
    ) -> bool {
        let service = crate::authorization::AuthorizationService::new();
        let action = ResourceAction::TaskReadOperator;
        let gate = service.authorize_action(
            request_context.principal().kind,
            request_context.principal().role_key.as_ref(),
            action,
        );
        match gate {
            crate::authorization::ActionGateDecision::AllowAbsolute => true,
            crate::authorization::ActionGateDecision::Deny { .. } => false,
            crate::authorization::ActionGateDecision::RequireResource { .. } => {
                let Some(task_id) = task_id else {
                    return false;
                };
                matches!(
                    crate::authorization::AuthorizationResolver::new(
                        self.crud_store.as_ref().clone(),
                    )
                    .authorize_task(
                        request_context.principal(),
                        &gate,
                        action,
                        task_id,
                        workspace_id,
                        None,
                    )
                    .await,
                    Ok(crate::authorization::ProofResolution::Authorized(_))
                )
            }
        }
    }

    async fn task_root_access_filter(
        &self,
        request_context: &RequestContext,
        workspace_id: &str,
    ) -> anyhow::Result<Option<pioneer_crud::TaskRootAccessFilter>> {
        if !Self::uses_scoped_task_policy(request_context) {
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
        if crate::authorization::AuthorizationService::new().runtime_principal_policy(
            request_context.principal().kind,
            request_context.principal().role_key.as_ref(),
        ) == Some(crate::authorization::RuntimePrincipalPolicy::ScopedCollaboration)
        {
            // `System` tasks execute with Gateway-owned authority and therefore
            // are not a user-selectable escape hatch around execution admission.
            // Collaborative principals create Agent tasks; trusted internal
            // services may still create System tasks through the typed service
            // boundary.
            if params.executor_kind != pioneer_protocol::TaskExecutorKind::Agent {
                self.send_error(
                    connection_id,
                    crate::authorization::AuthorizationExternalError::Forbidden
                        .response(request_id),
                )
                .await;
                return;
            }
            let Some(authorized_thread) = authorized_thread else {
                self.send_error(
                    connection_id,
                    task_authorization_unavailable(
                        request_id,
                        ResourceAction::TaskCreate,
                        "thread",
                        "execution",
                    ),
                )
                .await;
                return;
            };
            if authorized_thread.action() != ResourceAction::TaskCreate {
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
                            crate::public_error::agent_rpc_error(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                pioneer_protocol::PublicErrorCode::NotFound,
                                pioneer_protocol::PublicErrorStage::Admission,
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
                                ResourceAction::TaskCreate,
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

        let trigger_kind = params.trigger.spec.kind();
        if params.delivery_policy.is_none() {
            let attachment = params
                .lifecycle_policy
                .as_ref()
                .map(|policy| policy.attachment)
                .unwrap_or_else(|| {
                    pioneer_tasks::default_lifecycle_policy(
                        trigger_kind,
                        params.created_by_turn_id.is_some(),
                    )
                    .attachment
                });
            params.delivery_policy = Some(pioneer_tasks::default_delivery_policy(
                trigger_kind,
                attachment,
                params.owner_kind,
                params.owner_id.as_deref(),
                authorized_thread
                    .map(|proof| proof.collaboration_root_thread_id())
                    .or(params.created_by_thread_id.as_deref()),
            ));
        }
        let current_thread_id = authorized_thread
            .map(|proof| proof.thread_id())
            .or(params.created_by_thread_id.as_deref())
            .or_else(|| {
                (params.owner_kind == pioneer_protocol::TaskOwnerKind::Thread)
                    .then_some(params.owner_id.as_deref())
                    .flatten()
            });
        let collaboration_root_thread_id = authorized_thread
            .map(|proof| proof.collaboration_root_thread_id())
            .or(current_thread_id);
        if let Some(policy) = params.delivery_policy.as_mut()
            && let Err(error) = crate::task_delivery_policy::resolve_task_delivery_policy(
                policy,
                crate::task_delivery_policy::TaskDeliveryThreadContext {
                    current_thread_id,
                    origin_thread_id: collaboration_root_thread_id,
                    collaboration_root_thread_id,
                },
            )
        {
            self.send_error(
                connection_id,
                crate::public_error::agent_rpc_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    pioneer_protocol::PublicErrorCode::InvalidInput,
                    pioneer_protocol::PublicErrorStage::Admission,
                    error,
                ),
            )
            .await;
            return;
        }
        if !self
            .require_scoped_task_delivery_policy(
                request_context,
                &request_id,
                params.workspace_id.as_str(),
                params.delivery_policy.as_ref(),
            )
            .await
        {
            return;
        }
        let mut task_execution_admission = None;
        if params.executor_kind == pioneer_protocol::TaskExecutorKind::Agent {
            let Some(root_thread_id) = authorized_thread
                .map(|proof| proof.collaboration_root_thread_id())
                .or(params.created_by_thread_id.as_deref())
            else {
                self.send_error(
                    connection_id,
                    crate::public_error::agent_rpc_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorCode::InvalidInput,
                        pioneer_protocol::PublicErrorStage::Admission,
                        "agent task requires an exact initiating thread",
                    ),
                )
                .await;
                return;
            };
            let root_thread = match self.crud_store.get_thread_by_id(root_thread_id).await {
                Ok(Some(thread)) if thread.workspace_id == params.workspace_id => thread,
                Ok(_) => {
                    self.send_error(
                        connection_id,
                        crate::authorization::AuthorizationExternalError::NotFound
                            .response(request_id),
                    )
                    .await;
                    return;
                }
                Err(_) => {
                    self.send_error(
                        connection_id,
                        task_authorization_unavailable(
                            request_id,
                            ResourceAction::TaskCreate,
                            "thread",
                            "execution",
                        ),
                    )
                    .await;
                    return;
                }
            };
            let mut execution_request =
                match crate::authorization::ExecutionAdmissionRequest::for_task(
                    &params,
                    root_thread_id,
                    root_thread.model_provider.as_str(),
                    root_thread.model.as_str(),
                    None,
                ) {
                    Ok(request) => request,
                    Err(error) => {
                        self.send_error(
                            connection_id,
                            crate::public_error::agent_rpc_error(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                pioneer_protocol::PublicErrorCode::InvalidInput,
                                pioneer_protocol::PublicErrorStage::Admission,
                                format!("invalid task execution intent: {error}"),
                            ),
                        )
                        .await;
                        return;
                    }
                };
            if !matches!(
                execution_request.execution_backend,
                Some(pioneer_protocol::AgentExecutionBackend::CLIAgentRuntime { .. })
                    | Some(pioneer_protocol::AgentExecutionBackend::ACPAgentRuntime { .. })
            ) {
                execution_request.provider_authority_fingerprint = Some(
                    self.provider_registry
                        .authority_fingerprint_for_workspace(
                            params.workspace_id.as_str(),
                            execution_request.provider.as_str(),
                        )
                        .as_str()
                        .to_owned(),
                );
            }
            let policy_revision = match self.authorization_invalidation_hub.current_revision().await
            {
                Ok(revision) => revision,
                Err(_) => {
                    self.send_error(
                        connection_id,
                        task_authorization_unavailable(
                            request_id,
                            ResourceAction::TaskCreate,
                            "execution_intent",
                            "execution",
                        ),
                    )
                    .await;
                    return;
                }
            };
            let requested_permission_cap = params
                .agent_spec
                .as_ref()
                .and_then(|spec| spec.permission_cap.as_ref());
            let admitted_context = match crate::authorization::ExecutionAdmissionService::new(
                self.crud_store.as_ref().clone(),
            )
            .admit_context(
                request_context.principal(),
                policy_revision,
                &execution_request,
                requested_permission_cap,
            )
            .await
            {
                Ok(context) => context,
                Err(_) => {
                    self.send_error(
                        connection_id,
                        crate::authorization::AuthorizationExternalError::Forbidden
                            .response(request_id),
                    )
                    .await;
                    return;
                }
            };
            let authorization_context_json = match admitted_context.to_persisted_json() {
                Ok(json) => json,
                Err(_) => {
                    self.send_error(
                        connection_id,
                        task_authorization_unavailable(
                            request_id,
                            ResourceAction::TaskCreate,
                            "execution_intent",
                            "persistence",
                        ),
                    )
                    .await;
                    return;
                }
            };
            task_execution_admission = Some(pioneer_tasks::TaskExecutionAdmissionSeed {
                workspace_id: admitted_context.workspace_id().to_owned(),
                root_thread_id: admitted_context.root_thread_id().to_owned(),
                initiating_principal_id: admitted_context.initiating_principal_id().to_string(),
                authorization_context_json,
                role_key: admitted_context.role_key().to_owned(),
                policy_fingerprint: admitted_context.policy_fingerprint().to_owned(),
                execution_resources: crate::authorization::AuthorizationService::new()
                    .execution_resource_policy(
                        request_context.principal().kind,
                        request_context.principal().role_key.as_ref(),
                    )
                    .expect("admitted Task role must have execution resources"),
                task_resources: crate::authorization::AuthorizationService::new()
                    .task_resource_budget(
                        request_context.principal().kind,
                        request_context.principal().role_key.as_ref(),
                    )
                    .expect("admitted Task role must have a Task resource budget"),
            });
        }
        let mut context = match self.task_create_context_for_params(&params).await {
            Ok(context) => context,
            Err(error) => {
                self.send_error(
                    connection_id,
                    task_public_error(
                        Some(request_id),
                        pioneer_protocol::PublicErrorStage::Preparation,
                        format!("failed to freeze task context: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        context.actor_id = Some(request_context.principal().principal_id.to_string());
        context.execution_admission = task_execution_admission;
        if let Some(seed) = context.execution_admission.as_ref()
            && self
                .validate_task_execution_admission_seed(seed)
                .await
                .is_err()
        {
            self.send_error(
                connection_id,
                task_authorization_unavailable(
                    request_id,
                    ResourceAction::TaskCreate,
                    "execution_intent",
                    "durable_start",
                ),
            )
            .await;
            return;
        }
        match message_future(self.task_runtime.service().create_task(context, params)).await {
            Ok(response_payload) => {
                self.send_task_response(connection_id, request_id, &response_payload)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    task_public_error(
                        Some(request_id),
                        pioneer_protocol::PublicErrorStage::Persistence,
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
            .require_scoped_task_proof(
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
        let Some(workspace_id) = authorized_task.map(|proof| proof.workspace_id()) else {
            self.send_error(
                request_context.connection_id(),
                task_authorization_unavailable(
                    request_id,
                    ResourceAction::TaskRead,
                    "task",
                    "observation",
                ),
            )
            .await;
            return;
        };
        let Some(_observation) = self
            .acquire_task_observation_page(request_context, &request_id, workspace_id)
            .await
        else {
            return;
        };
        let connection_id = request_context.connection_id();
        let operator_allowed = self
            .task_operator_projection_allowed(request_context, Some(params.task_id.as_str()), None)
            .await;
        match self.task_runtime.service().get_task(params).await {
            Ok(response_payload) => {
                let projected =
                    crate::task_projection::project_task_get(&response_payload, operator_allowed);
                self.send_task_response(connection_id, request_id, &projected)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    task_public_error(
                        Some(request_id),
                        pioneer_protocol::PublicErrorStage::Observation,
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
        let Some(observation) = self
            .acquire_task_observation_page(
                request_context,
                &request_id,
                params.workspace_id.as_str(),
            )
            .await
        else {
            return;
        };
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
                    .list_tasks_scoped_with_budget(params, access, observation.budget)
                    .await
            }
            None => {
                self.task_runtime
                    .service()
                    .list_tasks_with_budget(params, observation.budget)
                    .await
            }
        };
        match response {
            Ok(response_payload) => {
                let projected = crate::task_projection::project_task_list(&response_payload);
                self.send_task_response(connection_id, request_id, &projected)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    task_public_error(
                        Some(request_id),
                        pioneer_protocol::PublicErrorStage::Observation,
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
            .require_scoped_task_proof(
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
        let Some(workspace_id) = authorized_task.map(|proof| proof.workspace_id()) else {
            self.send_error(
                request_context.connection_id(),
                task_authorization_unavailable(
                    request_id,
                    ResourceAction::TaskRead,
                    "task",
                    "observation",
                ),
            )
            .await;
            return;
        };
        let Some(observation) = self
            .acquire_task_observation_page(request_context, &request_id, workspace_id)
            .await
        else {
            return;
        };
        let connection_id = request_context.connection_id();
        let operator_allowed = self
            .task_operator_projection_allowed(request_context, Some(params.task_id.as_str()), None)
            .await;
        match self
            .task_runtime
            .service()
            .get_task_tree_with_budget(params, observation.budget)
            .await
        {
            Ok(response_payload) => {
                let projected =
                    crate::task_projection::project_task_tree(&response_payload, operator_allowed);
                self.send_task_response(connection_id, request_id, &projected)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    task_public_error(
                        Some(request_id),
                        pioneer_protocol::PublicErrorStage::Observation,
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
            .require_scoped_task_proof(
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
        let Some(workspace_id) = authorized_task.map(|proof| proof.workspace_id()) else {
            self.send_error(
                request_context.connection_id(),
                task_authorization_unavailable(
                    request_id,
                    ResourceAction::TaskRead,
                    "task",
                    "observation",
                ),
            )
            .await;
            return;
        };
        let Some(observation) = self
            .acquire_task_observation_page(request_context, &request_id, workspace_id)
            .await
        else {
            return;
        };
        let connection_id = request_context.connection_id();
        let operator_allowed = self
            .task_operator_projection_allowed(request_context, Some(params.task_id.as_str()), None)
            .await;
        match self
            .task_runtime
            .service()
            .get_task_events_with_budget(params, observation.budget)
            .await
        {
            Ok(response_payload) => {
                let projected = crate::task_projection::project_task_events(
                    &response_payload,
                    operator_allowed,
                );
                self.send_task_response(connection_id, request_id, &projected)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    task_public_error(
                        Some(request_id),
                        pioneer_protocol::PublicErrorStage::Observation,
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
            .require_scoped_task_batch_proof(
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
        let wait_context = pioneer_tasks::TaskWaitContext {
            actor_id: Some(request_context.principal().principal_id.to_string()),
            task_resource_budget: crate::authorization::AuthorizationService::new()
                .task_resource_budget(
                    request_context.principal().kind,
                    request_context.principal().role_key.as_ref(),
                ),
        };
        match self
            .task_runtime
            .service()
            .wait_tasks(wait_context, params)
            .await
        {
            Ok(response_payload) => {
                let projected = crate::task_projection::project_task_wait(&response_payload);
                self.send_task_response(connection_id, request_id, &projected)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    task_public_error(
                        Some(request_id),
                        pioneer_protocol::PublicErrorStage::Observation,
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
            .require_scoped_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskReview,
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
                    task_public_error(
                        Some(request_id),
                        pioneer_protocol::PublicErrorStage::Persistence,
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
            .require_scoped_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskReview,
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
                    task_public_error(
                        Some(request_id),
                        pioneer_protocol::PublicErrorStage::Persistence,
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
            .require_scoped_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskCancel,
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
                    task_public_error(
                        Some(request_id),
                        pioneer_protocol::PublicErrorStage::Persistence,
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
            .require_scoped_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskDetach,
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
                    task_public_error(
                        Some(request_id),
                        pioneer_protocol::PublicErrorStage::Persistence,
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
            .require_scoped_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskScheduleManage,
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
                    task_public_error(
                        Some(request_id),
                        pioneer_protocol::PublicErrorStage::Persistence,
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
            .require_scoped_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskScheduleManage,
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
                    task_public_error(
                        Some(request_id),
                        pioneer_protocol::PublicErrorStage::Persistence,
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
            .require_scoped_task_proof(
                request_context,
                &request_id,
                authorized_task,
                params.task_id.as_str(),
                ResourceAction::TaskScheduleManage,
            )
            .await
        {
            return;
        }
        let connection_id = request_context.connection_id();
        let execution_admission = match self
            .task_execution_readmission_seed(
                request_context.principal(),
                authorized_task.and_then(|proof| proof.root_thread_id()),
                params.task_id.as_str(),
            )
            .await
        {
            Ok(admission) => admission,
            Err(_) => {
                self.send_error(
                    connection_id,
                    task_authorization_unavailable(
                        request_id,
                        ResourceAction::TaskCreate,
                        "execution_intent",
                        "readmission",
                    ),
                )
                .await;
                return;
            }
        };
        let mut mutation_context = Self::task_mutation_context(request_context);
        mutation_context.execution_admission = execution_admission;
        match message_future(
            self.task_runtime
                .service()
                .resume_task(mutation_context, params),
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
                    task_public_error(
                        Some(request_id),
                        pioneer_protocol::PublicErrorStage::Persistence,
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
        let Some(observation) = self
            .acquire_task_observation_page(
                request_context,
                &request_id,
                params.workspace_id.as_str(),
            )
            .await
        else {
            return;
        };
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
                message_future(self.task_runtime.service().list_agenda_scoped_with_budget(
                    params,
                    access,
                    observation.budget,
                ))
                .await
            }
            None => {
                message_future(
                    self.task_runtime
                        .service()
                        .list_agenda_with_budget(params, observation.budget),
                )
                .await
            }
        };
        match response {
            Ok(response_payload) => {
                let projected = crate::task_projection::project_task_agenda(&response_payload);
                self.send_task_response(connection_id, request_id, &projected)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    task_public_error(
                        Some(request_id),
                        pioneer_protocol::PublicErrorStage::Observation,
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
        let Some(observation) = self
            .acquire_task_observation_page(
                request_context,
                &request_id,
                params.workspace_id.as_str(),
            )
            .await
        else {
            return;
        };
        let operator_task_id = params.task_id.clone();
        let operator_workspace_id = params.workspace_id.clone();
        let operator_allowed = self
            .task_operator_projection_allowed(
                request_context,
                operator_task_id.as_deref(),
                Some(operator_workspace_id.as_str()),
            )
            .await;
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
                    .list_deliveries_scoped_with_budget(params, access, observation.budget)
                    .await
            }
            None => {
                self.task_runtime
                    .service()
                    .list_deliveries_with_budget(params, observation.budget)
                    .await
            }
        };
        match response {
            Ok(response_payload) => {
                let projected = crate::task_projection::project_task_deliveries(
                    &response_payload,
                    operator_allowed,
                );
                self.send_task_response(connection_id, request_id, &projected)
                    .await
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    task_public_error(
                        Some(request_id),
                        pioneer_protocol::PublicErrorStage::Observation,
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
                        task_public_error(
                            None,
                            pioneer_protocol::PublicErrorStage::Delivery,
                            format!("failed to encode task response: {error}"),
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
