use super::*;
use crate::message::artifacts::{ArtifactListAuthorization, ArtifactUploadAuthorization};
use crate::message::thread_handlers::ThreadAccessAuthorization;
use pioneer_protocol::{
    ArtifactBindParams, ArtifactCapabilitiesParams, ArtifactDeleteParams, ArtifactGetParams,
    ArtifactListForMessageParams, ArtifactListForThreadParams, ArtifactListForTurnParams,
    ArtifactListParams, ArtifactRestoreParams, ArtifactUploadAbortParams,
    ArtifactUploadFinishParams, ArtifactUploadStartParams, ArtifactViewGrantCreateParams,
    AuthSessionRevokeParams, CLIRuntimeGetParams, CLIRuntimeListParams, CLIRuntimeRefreshParams,
    CLIRuntimeReviewStartParams, CLIRuntimeStatusParams, CLIRuntimeThreadBindingGetParams,
    CLIRuntimeThreadCompactParams, CLIRuntimeThreadForkParams, CLIRuntimeTurnSteerParams,
    GatewaySettingsGetParams, GatewaySettingsUpdateParams, InvitationCreateParams,
    InvitationListParams, InvitationRevokeParams, McpInstallParams, McpListParams,
    McpPolicySetParams, MemberDeviceCreateParams, MemberListParams, MemberRemoveParams,
    MemberRestoreParams, MemberSuspendParams, MemoryCandidatesApproveParams,
    MemoryCandidatesDecideParams, MemoryCandidatesEditAndApproveParams, MemoryCandidatesGetParams,
    MemoryCandidatesListParams, MemoryCandidatesMergeParams, MemoryCandidatesRejectParams,
    MemoryCandidatesSuppressSimilarParams, MemoryForgetParams, MemoryGetParams, MemoryListParams,
    MemoryRememberParams, MemorySearchParams, SkillListParams, SkillsHealthParams,
    SkillsInstallParams, SkillsPackInstallParams, SkillsPackUninstallParams,
    SkillsPackUpdateParams, SkillsPolicyListParams, SkillsPolicySetParams, SkillsUninstallParams,
    SkillsUpdateParams, TaskAcceptParams, TaskAgendaParams, TaskCancelParams, TaskCreateParams,
    TaskDeliveriesParams, TaskDetachParams, TaskEventsParams, TaskGetParams, TaskListParams,
    TaskPauseParams, TaskRescheduleParams, TaskResumeParams, TaskReviseParams,
    TaskTreeParams as TaskTreeTaskParams, TaskWaitParams, ThreadAgentsDocArchiveParams,
    ThreadAgentsDocGetParams, ThreadAgentsDocResolveForThreadParams, ThreadAgentsDocSaveParams,
    ThreadReadParams, ThreadTimelinePageParams, TurnCancelParams, TurnMessageDeleteParams,
    TurnMessageEditParams, TurnMessageRevisionsPageParams, TurnPermissionRequestRespondParams,
    TurnResumeParams, TurnWorkItemsGetParams, TurnWorkPageParams, VoiceSessionCancelParams,
    VoiceSessionFinalizeParams, VoiceSessionStartParams, VoiceStatusParams,
    WorkspaceMemberAddParams, WorkspaceMemberListParams, WorkspaceMemberRemoveParams,
};
use tracing::Instrument as _;

use crate::authorization::{
    AuthorizationDecision, AuthorizationExternalError, AuthorizationResolver, AuthorizationService,
    AuthorizedArtifact, AuthorizedInvitation, AuthorizedInvitationCollection,
    AuthorizedInvitationGrants, AuthorizedMemberDirectory, AuthorizedMemberPrincipal,
    AuthorizedSession, AuthorizedTask, AuthorizedThread, AuthorizedTurn, AuthorizedWorkspace,
    AuthorizedWorkspaceCollection, DenyReason, DisclosurePolicy, MethodAuthorizationEntry,
    ProofResolution, RegistryLookupError, ResourceAction, ResourceResolverKind,
    external_error_for_decision, normal_method_entry, record_authorization_unavailable,
    record_method_decision, record_method_decision_for_action,
};

pub(super) enum RequestAdmission {
    Superuser,
    InvitationGrants(AuthorizedInvitationGrants),
    InvitationCollection(AuthorizedInvitationCollection),
    Invitation(AuthorizedInvitation),
    MemberDirectory(AuthorizedMemberDirectory),
    MemberPrincipal(AuthorizedMemberPrincipal),
    OwnSession(AuthorizedSession),
    WorkspaceCollection(AuthorizedWorkspaceCollection),
    Workspace(AuthorizedWorkspace),
    ThreadCreate(AuthorizedWorkspace),
    ThreadOpen(AuthorizedThread),
    ThreadManage(AuthorizedThread),
    ThreadParticipants(AuthorizedThread),
    Thread(AuthorizedThread),
    RuntimeDraft(crate::thread::RuntimeDraftAccess),
    Turn(AuthorizedTurn),
    Artifact(AuthorizedArtifact),
    Task(AuthorizedTask),
    TaskBatch(Vec<AuthorizedTask>),
}

impl RequestAdmission {
    fn own_session(&self) -> Option<&AuthorizedSession> {
        match self {
            Self::OwnSession(proof) => Some(proof),
            _ => None,
        }
    }

    fn workspace_collection(&self) -> Option<&AuthorizedWorkspaceCollection> {
        match self {
            Self::WorkspaceCollection(proof) => Some(proof),
            _ => None,
        }
    }

    fn workspace(&self) -> Option<&AuthorizedWorkspace> {
        match self {
            Self::Workspace(proof) => Some(proof),
            _ => None,
        }
    }

    fn thread_create(&self) -> Option<&AuthorizedWorkspace> {
        match self {
            Self::ThreadCreate(proof) => Some(proof),
            _ => None,
        }
    }

    fn thread_open(&self) -> Option<&AuthorizedThread> {
        match self {
            Self::ThreadOpen(proof) => Some(proof),
            _ => None,
        }
    }

    fn thread_manage(&self) -> Option<&AuthorizedThread> {
        match self {
            Self::ThreadManage(proof) => Some(proof),
            _ => None,
        }
    }

    fn thread_participants(&self) -> Option<&AuthorizedThread> {
        match self {
            Self::ThreadParticipants(proof) => Some(proof),
            _ => None,
        }
    }

    pub(super) fn thread(&self) -> Option<&AuthorizedThread> {
        match self {
            Self::Thread(proof) => Some(proof),
            _ => None,
        }
    }

    pub(super) fn runtime_draft(&self) -> Option<&crate::thread::RuntimeDraftAccess> {
        match self {
            Self::RuntimeDraft(access) => Some(access),
            _ => None,
        }
    }

    fn turn(&self) -> Option<&AuthorizedTurn> {
        match self {
            Self::Turn(proof) => Some(proof),
            _ => None,
        }
    }

    fn artifact(&self) -> Option<&AuthorizedArtifact> {
        match self {
            Self::Artifact(proof) => Some(proof),
            _ => None,
        }
    }

    fn task(&self) -> Option<&AuthorizedTask> {
        match self {
            Self::Task(proof) => Some(proof),
            _ => None,
        }
    }

    fn task_batch(&self) -> Option<&[AuthorizedTask]> {
        match self {
            Self::TaskBatch(proofs) => Some(proofs.as_slice()),
            _ => None,
        }
    }

    fn invitation_grants(&self) -> Option<&AuthorizedInvitationGrants> {
        match self {
            Self::InvitationGrants(proof) => Some(proof),
            _ => None,
        }
    }

    fn invitation_collection(&self) -> Option<&AuthorizedInvitationCollection> {
        match self {
            Self::InvitationCollection(proof) => Some(proof),
            _ => None,
        }
    }

    fn invitation(&self) -> Option<&AuthorizedInvitation> {
        match self {
            Self::Invitation(proof) => Some(proof),
            _ => None,
        }
    }

    fn member_directory(&self) -> Option<&AuthorizedMemberDirectory> {
        match self {
            Self::MemberDirectory(proof) => Some(proof),
            _ => None,
        }
    }

    fn member_principal(&self) -> Option<&AuthorizedMemberPrincipal> {
        match self {
            Self::MemberPrincipal(proof) => Some(proof),
            _ => None,
        }
    }
}

// Keep every RPC branch in its own erased future. A single async block around the
// whole dispatch match makes its generated poll function reserve stack space for
// the state of every handler, even though only one branch can ever run.
macro_rules! dispatch_request_future {
    ($method:expr; $($pattern:pat => $body:block)*) => {
        match $method {
            $(
                $pattern => message_future(async move $body),
            )*
        }
    };
}

fn vector_provider_key_name(
    provider: Option<pioneer_protocol::GatewayThreadEpisodicVectorProvider>,
) -> Option<&'static str> {
    match provider {
        Some(pioneer_protocol::GatewayThreadEpisodicVectorProvider::OpenAi) => Some("openai"),
        Some(pioneer_protocol::GatewayThreadEpisodicVectorProvider::OpenRouter) => {
            Some("openrouter")
        }
        Some(pioneer_protocol::GatewayThreadEpisodicVectorProvider::Local) | None => None,
    }
}

impl MessageProcessor {
    pub fn process_request<'a>(
        &'a self,
        connection: &'a crate::request_context::ConnectionContext,
        payload: &'a str,
    ) -> MessageFuture<'a, ()> {
        let request_value = match serde_json::from_str::<JsonValue>(payload) {
            Ok(value) => value,
            Err(_) => {
                let canonical_method =
                    crate::request_context::CanonicalMethod::rpc("jsonrpc/parse");
                let span =
                    crate::request_context::request_span(connection, None, &canonical_method);
                return instrument_message_future(
                    message_future(async move {
                        warn!(
                            rejection_reason = "parse_error",
                            "rejected JSON-RPC request"
                        );
                        self.send_error(
                            connection.connection_id(),
                            JsonRpcErrorResponse::new(
                                None,
                                PARSE_ERROR_CODE,
                                "failed to parse JSON-RPC payload",
                            ),
                        )
                        .await;
                    }),
                    span,
                );
            }
        };

        let request_id = parse_request_id(&request_value);
        let canonical_method = request_value
            .get("method")
            .and_then(JsonValue::as_str)
            .map(crate::request_context::CanonicalMethod::rpc)
            .unwrap_or_else(|| crate::request_context::CanonicalMethod::rpc(""));
        let request = match serde_json::from_value::<JsonRpcRequest>(request_value) {
            Ok(request) => request,
            Err(error) => {
                let span = crate::request_context::request_span(
                    connection,
                    request_id.as_ref(),
                    &canonical_method,
                );
                return instrument_message_future(
                    message_future(async move {
                        warn!(
                            rejection_reason = "invalid_request",
                            "rejected JSON-RPC request"
                        );
                        self.send_error(
                            connection.connection_id(),
                            JsonRpcErrorResponse::new(
                                request_id,
                                INVALID_REQUEST_CODE,
                                format!("invalid JSON-RPC request: {error}"),
                            ),
                        )
                        .await;
                    }),
                    span,
                );
            }
        };
        let context = crate::request_context::RequestContext::new(
            connection,
            Some(request.id.clone()),
            crate::request_context::CanonicalMethod::rpc(request.method.as_str()),
        );
        let span = context.request_span();

        if request.jsonrpc != JSONRPC_VERSION {
            return instrument_message_future(
                message_future(async move {
                    warn!(
                        rejection_reason = "unsupported_jsonrpc_version",
                        "rejected JSON-RPC request"
                    );
                    self.send_error(
                        context.connection_id(),
                        JsonRpcErrorResponse::new(
                            Some(request.id.clone()),
                            INVALID_REQUEST_CODE,
                            format!("unsupported jsonrpc version `{}`", request.jsonrpc),
                        ),
                    )
                    .await;
                }),
                span,
            );
        }

        let dispatched = message_future(async move {
            let admission = match self.authorize_normal_request(&context, &request).await {
                Ok(admission) => admission,
                Err(response) => {
                    self.send_error(context.connection_id(), response).await;
                    return;
                }
            };

            // turn/start already exposes its handler future directly, so keep its parsing path
            // separate from the large dispatch match after central admission.
            let handler = if request.method == methods::TURN_START {
                self.dispatch_turn_start(context, request, admission)
            } else if request.method == methods::SETTINGS_UPDATE {
                self.dispatch_settings_update(context, request)
            } else {
                self.process_request_inner(context, request, admission)
            };
            handler.await;
        });

        instrument_message_future(dispatched, span)
    }

    async fn resolve_task_request_proof(
        &self,
        context: &crate::request_context::RequestContext,
        request_id: &RequestId,
        entry: &MethodAuthorizationEntry,
        action_gate: &crate::authorization::ActionGateDecision,
        resolver: &AuthorizationResolver,
        task_id: &str,
        expected_workspace_id: Option<&str>,
        expected_root_thread_id: Option<&str>,
    ) -> Result<AuthorizedTask, JsonRpcErrorResponse> {
        let resolution = resolver
            .authorize_task(
                context.principal(),
                action_gate,
                entry.action,
                task_id.trim(),
                expected_workspace_id.map(str::trim),
                expected_root_thread_id.map(str::trim),
            )
            .await
            .map_err(|_| {
                record_authorization_unavailable(
                    entry.action.safe_name(),
                    entry.resolver.safe_name(),
                    entry.audit.safe_name(),
                );
                AuthorizationExternalError::Unavailable.response(request_id.clone())
            })?;
        match resolution {
            ProofResolution::Authorized(proof) => {
                record_method_decision(entry, proof.decision());
                Ok(proof)
            }
            ProofResolution::Denied(decision) => {
                record_method_decision(entry, &decision);
                Err(external_error_for_decision(&decision)
                    .unwrap_or(AuthorizationExternalError::NotFound)
                    .response(request_id.clone()))
            }
        }
    }

    async fn authorize_task_collection_workspace(
        &self,
        context: &crate::request_context::RequestContext,
        request_id: &RequestId,
        entry: &MethodAuthorizationEntry,
        action_gate: &crate::authorization::ActionGateDecision,
        resolver: &AuthorizationResolver,
        workspace_id: &str,
    ) -> Result<RequestAdmission, JsonRpcErrorResponse> {
        let resolution = resolver
            .authorize_workspace(
                context.principal(),
                action_gate,
                entry.action,
                workspace_id.trim(),
            )
            .await
            .map_err(|_| {
                record_authorization_unavailable(
                    entry.action.safe_name(),
                    entry.resolver.safe_name(),
                    entry.audit.safe_name(),
                );
                AuthorizationExternalError::Unavailable.response(request_id.clone())
            })?;
        match resolution {
            ProofResolution::Authorized(proof) => {
                record_method_decision(entry, proof.decision());
                Ok(RequestAdmission::Workspace(proof))
            }
            ProofResolution::Denied(decision) => {
                record_method_decision(entry, &decision);
                Err(external_error_for_decision(&decision)
                    .unwrap_or(AuthorizationExternalError::NotFound)
                    .response(request_id.clone()))
            }
        }
    }

    async fn authorize_member_task_request(
        &self,
        context: &crate::request_context::RequestContext,
        request: &JsonRpcRequest,
        entry: &MethodAuthorizationEntry,
        action_gate: &crate::authorization::ActionGateDecision,
        resolver: &AuthorizationResolver,
    ) -> Result<RequestAdmission, JsonRpcErrorResponse> {
        let invalid_params = || {
            let decision = AuthorizationDecision::Deny {
                reason: DenyReason::ResourceScopeMismatch,
                disclosure: DisclosurePolicy::Validation,
            };
            record_method_decision(entry, &decision);
            AuthorizationExternalError::Validation.response(request.id.clone())
        };

        if request.method == methods::TASK_CREATE {
            let params = serde_json::from_value::<TaskCreateParams>(
                request.params.clone().unwrap_or_else(empty_object_value),
            )
            .map_err(|_| invalid_params())?;
            let initiating_thread_id = params.created_by_thread_id.as_deref().or_else(|| {
                (params.owner_kind == pioneer_protocol::TaskOwnerKind::Thread)
                    .then_some(params.owner_id.as_deref())
                    .flatten()
            });
            let Some(initiating_thread_id) = initiating_thread_id else {
                return Err(invalid_params());
            };
            let resolution = resolver
                .authorize_thread(
                    context.principal(),
                    action_gate,
                    entry.action,
                    initiating_thread_id.trim(),
                    Some(params.workspace_id.trim()),
                )
                .await
                .map_err(|_| {
                    record_authorization_unavailable(
                        entry.action.safe_name(),
                        entry.resolver.safe_name(),
                        entry.audit.safe_name(),
                    );
                    AuthorizationExternalError::Unavailable.response(request.id.clone())
                })?;
            return match resolution {
                ProofResolution::Authorized(proof) => {
                    if let Some(parent_task_id) = params.parent_task_id.as_deref() {
                        self.resolve_task_request_proof(
                            context,
                            &request.id,
                            entry,
                            action_gate,
                            resolver,
                            parent_task_id,
                            Some(params.workspace_id.as_str()),
                            Some(proof.thread_id()),
                        )
                        .await?;
                    }
                    record_method_decision(entry, proof.decision());
                    Ok(RequestAdmission::Thread(proof))
                }
                ProofResolution::Denied(decision) => {
                    record_method_decision(entry, &decision);
                    Err(external_error_for_decision(&decision)
                        .unwrap_or(AuthorizationExternalError::NotFound)
                        .response(request.id.clone()))
                }
            };
        }

        if request.method == methods::TASK_WAIT {
            let params = serde_json::from_value::<TaskWaitParams>(
                request.params.clone().unwrap_or_else(empty_object_value),
            )
            .map_err(|_| invalid_params())?;
            if params.task_ids.is_empty() && params.run_ids.is_empty() {
                return Err(invalid_params());
            }
            let mut task_ids = params.task_ids;
            for run_id in params.run_ids {
                let Some(run) =
                    self.crud_store
                        .get_task_run(run_id.trim())
                        .await
                        .map_err(|_| {
                            record_authorization_unavailable(
                                entry.action.safe_name(),
                                entry.resolver.safe_name(),
                                entry.audit.safe_name(),
                            );
                            AuthorizationExternalError::Unavailable.response(request.id.clone())
                        })?
                else {
                    let decision = AuthorizationDecision::Deny {
                        reason: DenyReason::MissingAuthoritativeResource,
                        disclosure: entry.disclosure,
                    };
                    record_method_decision(entry, &decision);
                    return Err(AuthorizationExternalError::NotFound.response(request.id.clone()));
                };
                task_ids.push(run.task_id);
            }
            task_ids.sort();
            task_ids.dedup();
            let mut proofs = Vec::with_capacity(task_ids.len());
            for task_id in task_ids {
                proofs.push(
                    self.resolve_task_request_proof(
                        context,
                        &request.id,
                        entry,
                        action_gate,
                        resolver,
                        task_id.as_str(),
                        None,
                        None,
                    )
                    .await?,
                );
            }
            return Ok(RequestAdmission::TaskBatch(proofs));
        }

        if request.method == methods::TASK_LIST {
            let params = serde_json::from_value::<TaskListParams>(
                request.params.clone().unwrap_or_else(empty_object_value),
            )
            .map_err(|_| invalid_params())?;
            if let Some(task_id) = params
                .parent_task_id
                .as_deref()
                .or(params.root_task_id.as_deref())
            {
                return self
                    .resolve_task_request_proof(
                        context,
                        &request.id,
                        entry,
                        action_gate,
                        resolver,
                        task_id,
                        Some(params.workspace_id.as_str()),
                        None,
                    )
                    .await
                    .map(RequestAdmission::Task);
            }
            return self
                .authorize_task_collection_workspace(
                    context,
                    &request.id,
                    entry,
                    action_gate,
                    resolver,
                    params.workspace_id.as_str(),
                )
                .await;
        }

        if request.method == methods::TASK_AGENDA {
            let params = serde_json::from_value::<TaskAgendaParams>(
                request.params.clone().unwrap_or_else(empty_object_value),
            )
            .map_err(|_| invalid_params())?;
            return self
                .authorize_task_collection_workspace(
                    context,
                    &request.id,
                    entry,
                    action_gate,
                    resolver,
                    params.workspace_id.as_str(),
                )
                .await;
        }

        if request.method == methods::TASK_DELIVERIES {
            let params = serde_json::from_value::<TaskDeliveriesParams>(
                request.params.clone().unwrap_or_else(empty_object_value),
            )
            .map_err(|_| invalid_params())?;
            let task_id = match (params.task_id.as_deref(), params.run_id.as_deref()) {
                (Some(task_id), _) => Some(task_id.to_owned()),
                (None, Some(run_id)) => {
                    let run = self
                        .crud_store
                        .get_task_run(run_id.trim())
                        .await
                        .map_err(|_| {
                            record_authorization_unavailable(
                                entry.action.safe_name(),
                                entry.resolver.safe_name(),
                                entry.audit.safe_name(),
                            );
                            AuthorizationExternalError::Unavailable.response(request.id.clone())
                        })?;
                    let Some(run) = run else {
                        let decision = AuthorizationDecision::Deny {
                            reason: DenyReason::MissingAuthoritativeResource,
                            disclosure: entry.disclosure,
                        };
                        record_method_decision(entry, &decision);
                        return Err(
                            AuthorizationExternalError::NotFound.response(request.id.clone())
                        );
                    };
                    Some(run.task_id)
                }
                (None, None) => None,
            };
            if let Some(task_id) = task_id {
                return self
                    .resolve_task_request_proof(
                        context,
                        &request.id,
                        entry,
                        action_gate,
                        resolver,
                        task_id.as_str(),
                        Some(params.workspace_id.as_str()),
                        None,
                    )
                    .await
                    .map(RequestAdmission::Task);
            }
            return self
                .authorize_task_collection_workspace(
                    context,
                    &request.id,
                    entry,
                    action_gate,
                    resolver,
                    params.workspace_id.as_str(),
                )
                .await;
        }

        let params = request.params.as_ref().and_then(JsonValue::as_object);
        let task_id = params
            .and_then(|params| params.get("task_id").or_else(|| params.get("taskId")))
            .and_then(JsonValue::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(invalid_params)?;
        self.resolve_task_request_proof(
            context,
            &request.id,
            entry,
            action_gate,
            resolver,
            task_id,
            None,
            None,
        )
        .await
        .map(RequestAdmission::Task)
    }

    async fn authorize_runtime_draft_request(
        &self,
        context: &crate::request_context::RequestContext,
        request: &JsonRpcRequest,
        entry: &MethodAuthorizationEntry,
        action: ResourceAction,
        thread_id: &str,
        expected_workspace_id: Option<&str>,
    ) -> Result<Option<crate::thread::RuntimeDraftAccess>, JsonRpcErrorResponse> {
        let authorization = self
            .authorize_runtime_draft_for_request(context, action, thread_id, expected_workspace_id)
            .await
            .map_err(|_| {
                record_authorization_unavailable(
                    entry.action.safe_name(),
                    entry.resolver.safe_name(),
                    entry.audit.safe_name(),
                );
                AuthorizationExternalError::Unavailable.response(request.id.clone())
            })?;
        if let Some((access, decision)) = authorization {
            record_method_decision(entry, &decision);
            Ok(Some(access))
        } else {
            Ok(None)
        }
    }

    async fn authorize_normal_request(
        &self,
        context: &crate::request_context::RequestContext,
        request: &JsonRpcRequest,
    ) -> Result<RequestAdmission, JsonRpcErrorResponse> {
        let entry = match normal_method_entry(request.method.as_str()) {
            Ok(entry) => entry,
            Err(RegistryLookupError::Unmapped) => {
                return Err(JsonRpcErrorResponse::new(
                    Some(request.id.clone()),
                    METHOD_NOT_FOUND_CODE,
                    "method not found",
                ));
            }
            Err(RegistryLookupError::InvalidDefinition) => {
                record_authorization_unavailable("unmapped", "unmapped", "authorization_registry");
                return Err(AuthorizationExternalError::Unavailable.response(request.id.clone()));
            }
        };
        let service = AuthorizationService::new();
        if request.method == methods::THREAD_START {
            return self
                .authorize_thread_start_request(context, request, entry, &service)
                .await;
        }
        let action_gate =
            service.authorize_action(context.principal().kind, context.role_key(), entry.action);

        if let crate::authorization::ActionGateDecision::Deny { reason, disclosure } = &action_gate
        {
            let decision = AuthorizationDecision::Deny {
                reason: *reason,
                disclosure: *disclosure,
            };
            record_method_decision(entry, &decision);
            return Err(external_error_for_decision(&decision)
                .expect("denied action gate has external mapping")
                .response(request.id.clone()));
        }

        let resolver = AuthorizationResolver::new((*self.crud_store).clone());
        if matches!(
            request.method.as_str(),
            methods::WORKSPACE_MEMBER_LIST
                | methods::WORKSPACE_MEMBER_ADD
                | methods::WORKSPACE_MEMBER_REMOVE
        ) {
            let workspace_id = if request.method == methods::WORKSPACE_MEMBER_LIST {
                serde_json::from_value::<WorkspaceMemberListParams>(
                    request.params.clone().unwrap_or_else(empty_object_value),
                )
                .map_err(|_| AuthorizationExternalError::Validation.response(request.id.clone()))?
                .workspace_id
            } else if request.method == methods::WORKSPACE_MEMBER_ADD {
                serde_json::from_value::<WorkspaceMemberAddParams>(
                    request.params.clone().unwrap_or_else(empty_object_value),
                )
                .map_err(|_| AuthorizationExternalError::Validation.response(request.id.clone()))?
                .workspace_id
            } else {
                serde_json::from_value::<WorkspaceMemberRemoveParams>(
                    request.params.clone().unwrap_or_else(empty_object_value),
                )
                .map_err(|_| AuthorizationExternalError::Validation.response(request.id.clone()))?
                .workspace_id
            };
            let resolution = resolver
                .authorize_workspace(
                    context.principal(),
                    &action_gate,
                    entry.action,
                    workspace_id.as_str(),
                )
                .await
                .map_err(|_| {
                    record_authorization_unavailable(
                        entry.action.safe_name(),
                        entry.resolver.safe_name(),
                        entry.audit.safe_name(),
                    );
                    AuthorizationExternalError::Unavailable.response(request.id.clone())
                })?;
            return match resolution {
                ProofResolution::Authorized(proof) => {
                    record_method_decision(entry, proof.decision());
                    Ok(RequestAdmission::Workspace(proof))
                }
                ProofResolution::Denied(decision) => {
                    record_method_decision(entry, &decision);
                    Err(external_error_for_decision(&decision)
                        .unwrap_or(AuthorizationExternalError::NotFound)
                        .response(request.id.clone()))
                }
            };
        }
        if request.method == methods::MEMBER_LIST {
            serde_json::from_value::<MemberListParams>(
                request.params.clone().unwrap_or_else(empty_object_value),
            )
            .map_err(|_| AuthorizationExternalError::Validation.response(request.id.clone()))?;
            let resolution = resolver.authorize_member_directory(context.principal(), &action_gate);
            return match resolution {
                ProofResolution::Authorized(proof) => {
                    record_method_decision(entry, proof.decision());
                    Ok(RequestAdmission::MemberDirectory(proof))
                }
                ProofResolution::Denied(decision) => {
                    record_method_decision(entry, &decision);
                    Err(external_error_for_decision(&decision)
                        .unwrap_or(AuthorizationExternalError::NotFound)
                        .response(request.id.clone()))
                }
            };
        }
        if matches!(
            request.method.as_str(),
            methods::MEMBER_SUSPEND
                | methods::MEMBER_RESTORE
                | methods::MEMBER_REMOVE
                | methods::MEMBER_DEVICE_CREATE
        ) {
            let params_value = request.params.clone().unwrap_or_else(empty_object_value);
            let target_principal_id = if request.method == methods::MEMBER_SUSPEND {
                serde_json::from_value::<MemberSuspendParams>(params_value)
                    .map_err(|_| {
                        AuthorizationExternalError::Validation.response(request.id.clone())
                    })?
                    .principal_id
            } else if request.method == methods::MEMBER_RESTORE {
                serde_json::from_value::<MemberRestoreParams>(params_value)
                    .map_err(|_| {
                        AuthorizationExternalError::Validation.response(request.id.clone())
                    })?
                    .principal_id
            } else if request.method == methods::MEMBER_REMOVE {
                serde_json::from_value::<MemberRemoveParams>(params_value)
                    .map_err(|_| {
                        AuthorizationExternalError::Validation.response(request.id.clone())
                    })?
                    .principal_id
            } else {
                serde_json::from_value::<MemberDeviceCreateParams>(params_value)
                    .map_err(|_| {
                        AuthorizationExternalError::Validation.response(request.id.clone())
                    })?
                    .principal_id
            };
            let database = self.crud_store.database_connection();
            let resolution = resolver
                .authorize_member_principal(
                    &database,
                    context.principal(),
                    &action_gate,
                    entry.action,
                    &target_principal_id,
                )
                .await
                .map_err(|_| {
                    record_authorization_unavailable(
                        entry.action.safe_name(),
                        entry.resolver.safe_name(),
                        entry.audit.safe_name(),
                    );
                    AuthorizationExternalError::Unavailable.response(request.id.clone())
                })?;
            return match resolution {
                ProofResolution::Authorized(proof) => {
                    record_method_decision(entry, proof.decision());
                    Ok(RequestAdmission::MemberPrincipal(proof))
                }
                ProofResolution::Denied(decision) => {
                    record_method_decision(entry, &decision);
                    Err(external_error_for_decision(&decision)
                        .unwrap_or(AuthorizationExternalError::NotFound)
                        .response(request.id.clone()))
                }
            };
        }
        if request.method == methods::INVITE_CREATE {
            let params = serde_json::from_value::<InvitationCreateParams>(
                request.params.clone().unwrap_or_else(empty_object_value),
            )
            .map_err(|_| {
                let decision = AuthorizationDecision::Deny {
                    reason: DenyReason::ResourceScopeMismatch,
                    disclosure: DisclosurePolicy::Validation,
                };
                record_method_decision(entry, &decision);
                AuthorizationExternalError::Validation.response(request.id.clone())
            })?;
            let database = self.crud_store.database_connection();
            let resolution = resolver
                .authorize_invitation_grants(
                    &database,
                    context.principal(),
                    &action_gate,
                    params.workspace_ids.as_slice(),
                )
                .await
                .map_err(|_| {
                    record_authorization_unavailable(
                        entry.action.safe_name(),
                        entry.resolver.safe_name(),
                        entry.audit.safe_name(),
                    );
                    AuthorizationExternalError::Unavailable.response(request.id.clone())
                })?;
            return match resolution {
                ProofResolution::Authorized(proof) => {
                    record_method_decision(entry, proof.decision());
                    Ok(RequestAdmission::InvitationGrants(proof))
                }
                ProofResolution::Denied(decision) => {
                    record_method_decision(entry, &decision);
                    Err(external_error_for_decision(&decision)
                        .unwrap_or(AuthorizationExternalError::NotFound)
                        .response(request.id.clone()))
                }
            };
        }
        if request.method == methods::INVITE_LIST {
            serde_json::from_value::<InvitationListParams>(
                request.params.clone().unwrap_or_else(empty_object_value),
            )
            .map_err(|_| AuthorizationExternalError::Validation.response(request.id.clone()))?;
            let resolution =
                resolver.authorize_invitation_collection(context.principal(), &action_gate);
            return match resolution {
                ProofResolution::Authorized(proof) => {
                    record_method_decision(entry, proof.decision());
                    Ok(RequestAdmission::InvitationCollection(proof))
                }
                ProofResolution::Denied(decision) => {
                    record_method_decision(entry, &decision);
                    Err(external_error_for_decision(&decision)
                        .unwrap_or(AuthorizationExternalError::NotFound)
                        .response(request.id.clone()))
                }
            };
        }
        if request.method == methods::INVITE_REVOKE {
            let params = serde_json::from_value::<InvitationRevokeParams>(
                request.params.clone().unwrap_or_else(empty_object_value),
            )
            .map_err(|_| AuthorizationExternalError::Validation.response(request.id.clone()))?;
            let database = self.crud_store.database_connection();
            let resolution = resolver
                .authorize_invitation(
                    &database,
                    context.principal(),
                    &action_gate,
                    &params.invitation_id,
                )
                .await
                .map_err(|_| {
                    record_authorization_unavailable(
                        entry.action.safe_name(),
                        entry.resolver.safe_name(),
                        entry.audit.safe_name(),
                    );
                    AuthorizationExternalError::Unavailable.response(request.id.clone())
                })?;
            return match resolution {
                ProofResolution::Authorized(proof) => {
                    record_method_decision(entry, proof.decision());
                    Ok(RequestAdmission::Invitation(proof))
                }
                ProofResolution::Denied(decision) => {
                    record_method_decision(entry, &decision);
                    Err(external_error_for_decision(&decision)
                        .unwrap_or(AuthorizationExternalError::NotFound)
                        .response(request.id.clone()))
                }
            };
        }
        if !action_gate.is_final_allow()
            && (entry.resolver == ResourceResolverKind::Task
                || request.method == methods::TASK_CREATE)
        {
            return self
                .authorize_member_task_request(context, request, entry, &action_gate, &resolver)
                .await;
        }
        if matches!(
            request.method.as_str(),
            methods::ARTIFACT_UPLOAD_START
                | methods::ARTIFACT_UPLOAD_FINISH
                | methods::ARTIFACT_UPLOAD_ABORT
        ) {
            return self
                .authorize_artifact_transfer_request(
                    context,
                    request,
                    entry,
                    &action_gate,
                    &resolver,
                )
                .await;
        }
        if entry.resolver == ResourceResolverKind::Workspace
            && !action_gate.is_final_allow()
            && matches!(
                request.method.as_str(),
                methods::MEMORY_SEARCH
                    | methods::MEMORY_GET
                    | methods::MEMORY_LIST
                    | methods::MEMORY_REMEMBER
                    | methods::MEMORY_FORGET
                    | methods::MEMORY_CANDIDATES_LIST
                    | methods::MEMORY_CANDIDATES_GET
                    | methods::MEMORY_CANDIDATES_DECIDE
                    | methods::MEMORY_CANDIDATES_APPROVE
                    | methods::MEMORY_CANDIDATES_REJECT
                    | methods::MEMORY_CANDIDATES_EDIT_AND_APPROVE
                    | methods::MEMORY_CANDIDATES_MERGE
                    | methods::MEMORY_CANDIDATES_SUPPRESS_SIMILAR
            )
        {
            let Some(workspace_id) = self
                .session_manager
                .connection_workspace_id(context.connection_id())
                .await
            else {
                let decision = AuthorizationDecision::Deny {
                    reason: DenyReason::MissingAuthoritativeResource,
                    disclosure: entry.disclosure,
                };
                record_method_decision(entry, &decision);
                return Err(external_error_for_decision(&decision)
                    .unwrap_or(AuthorizationExternalError::NotFound)
                    .response(request.id.clone()));
            };
            let resolution = resolver
                .authorize_workspace(
                    context.principal(),
                    &action_gate,
                    entry.action,
                    workspace_id.as_str(),
                )
                .await
                .map_err(|_| {
                    record_authorization_unavailable(
                        entry.action.safe_name(),
                        entry.resolver.safe_name(),
                        entry.audit.safe_name(),
                    );
                    AuthorizationExternalError::Unavailable.response(request.id.clone())
                })?;
            return match resolution {
                ProofResolution::Authorized(proof) => {
                    record_method_decision(entry, proof.decision());
                    Ok(RequestAdmission::Workspace(proof))
                }
                ProofResolution::Denied(decision) => {
                    record_method_decision(entry, &decision);
                    Err(external_error_for_decision(&decision)
                        .unwrap_or(AuthorizationExternalError::NotFound)
                        .response(request.id.clone()))
                }
            };
        }
        if entry.resolver == ResourceResolverKind::Capability
            && entry.action == ResourceAction::ProviderUse
        {
            let params = request.params.as_ref().and_then(JsonValue::as_object);
            let workspace_id = match request.method.as_str() {
                methods::VOICE_SESSION_START => params
                    .and_then(|params| params.get("context"))
                    .and_then(JsonValue::as_object)
                    .and_then(|context| {
                        context
                            .get("workspace_id")
                            .or_else(|| context.get("workspaceId"))
                    })
                    .and_then(JsonValue::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .map(str::to_owned),
                methods::VOICE_SESSION_FINALIZE | methods::VOICE_SESSION_CANCEL => {
                    let session_id = params
                        .and_then(|params| {
                            params.get("session_id").or_else(|| params.get("sessionId"))
                        })
                        .and_then(JsonValue::as_str)
                        .filter(|value| !value.trim().is_empty());
                    session_id.and_then(|session_id| {
                        let owner = AuthenticatedTransferOwner::from_request_context(context);
                        self.voice_sessions
                            .lookup_authenticated_session(session_id, &owner)
                            .ok()
                            .map(|session| session.workspace_id)
                    })
                }
                _ => params
                    .and_then(|params| {
                        params
                            .get("workspace_id")
                            .or_else(|| params.get("workspaceId"))
                    })
                    .and_then(JsonValue::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .map(str::to_owned),
            };
            let workspace_id = workspace_id.or_else(|| {
                let owner = AuthenticatedTransferOwner::from_request_context(context);
                self.voice_sessions
                    .active_session_for_owner(&owner)
                    .map(|session| session.workspace_id)
            });
            let workspace_id = match workspace_id {
                Some(workspace_id) => Some(workspace_id),
                None => {
                    self.session_manager
                        .connection_workspace_id(context.connection_id())
                        .await
                }
            };
            let Some(workspace_id) = workspace_id else {
                let decision = AuthorizationDecision::Deny {
                    reason: DenyReason::MissingAuthoritativeResource,
                    disclosure: entry.disclosure,
                };
                record_method_decision(entry, &decision);
                return Err(AuthorizationExternalError::NotFound.response(request.id.clone()));
            };
            let resolution = resolver
                .authorize_workspace(
                    context.principal(),
                    &action_gate,
                    entry.action,
                    workspace_id.trim(),
                )
                .await
                .map_err(|_| {
                    record_authorization_unavailable(
                        entry.action.safe_name(),
                        entry.resolver.safe_name(),
                        entry.audit.safe_name(),
                    );
                    AuthorizationExternalError::Unavailable.response(request.id.clone())
                })?;
            return match resolution {
                ProofResolution::Authorized(proof) => {
                    record_method_decision(entry, proof.decision());
                    Ok(RequestAdmission::Workspace(proof))
                }
                ProofResolution::Denied(decision) => {
                    record_method_decision(entry, &decision);
                    Err(external_error_for_decision(&decision)
                        .unwrap_or(AuthorizationExternalError::NotFound)
                        .response(request.id.clone()))
                }
            };
        }
        match entry.resolver {
            ResourceResolverKind::WorkspaceCollection => {
                let resolution = resolver.authorize_workspace_collection(
                    context.principal(),
                    &action_gate,
                    entry.action,
                );
                return match resolution {
                    ProofResolution::Authorized(proof) => {
                        record_method_decision(entry, proof.decision());
                        Ok(RequestAdmission::WorkspaceCollection(proof))
                    }
                    ProofResolution::Denied(decision) => {
                        record_method_decision(entry, &decision);
                        Err(external_error_for_decision(&decision)
                            .unwrap_or(AuthorizationExternalError::Forbidden)
                            .response(request.id.clone()))
                    }
                };
            }
            ResourceResolverKind::Workspace if request.method == methods::WORKSPACE_SELECT => {
                let params = serde_json::from_value::<WorkspaceSelectParams>(
                    request.params.clone().unwrap_or_else(empty_object_value),
                )
                .map_err(|_| {
                    let decision = AuthorizationDecision::Deny {
                        reason: DenyReason::ResourceScopeMismatch,
                        disclosure: DisclosurePolicy::Validation,
                    };
                    record_method_decision(entry, &decision);
                    AuthorizationExternalError::Validation.response(request.id.clone())
                })?;
                let resolution = resolver
                    .authorize_workspace(
                        context.principal(),
                        &action_gate,
                        entry.action,
                        params.workspace_id.as_str(),
                    )
                    .await
                    .map_err(|_| {
                        record_authorization_unavailable(
                            entry.action.safe_name(),
                            entry.resolver.safe_name(),
                            entry.audit.safe_name(),
                        );
                        AuthorizationExternalError::Unavailable.response(request.id.clone())
                    })?;
                return match resolution {
                    ProofResolution::Authorized(proof) => {
                        record_method_decision(entry, proof.decision());
                        Ok(RequestAdmission::Workspace(proof))
                    }
                    ProofResolution::Denied(decision) => {
                        record_method_decision(entry, &decision);
                        Err(external_error_for_decision(&decision)
                            .unwrap_or(AuthorizationExternalError::Forbidden)
                            .response(request.id.clone()))
                    }
                };
            }
            ResourceResolverKind::Workspace if request.method == methods::THREAD_TREE => {
                let params = serde_json::from_value::<ThreadTreeParams>(
                    request.params.clone().unwrap_or_else(empty_object_value),
                )
                .map_err(|_| {
                    let decision = AuthorizationDecision::Deny {
                        reason: DenyReason::ResourceScopeMismatch,
                        disclosure: DisclosurePolicy::Validation,
                    };
                    record_method_decision(entry, &decision);
                    AuthorizationExternalError::Validation.response(request.id.clone())
                })?;
                let resolution = resolver
                    .authorize_workspace(
                        context.principal(),
                        &action_gate,
                        entry.action,
                        params.workspace_id.trim(),
                    )
                    .await
                    .map_err(|_| {
                        record_authorization_unavailable(
                            entry.action.safe_name(),
                            entry.resolver.safe_name(),
                            entry.audit.safe_name(),
                        );
                        AuthorizationExternalError::Unavailable.response(request.id.clone())
                    })?;
                return match resolution {
                    ProofResolution::Authorized(proof) => {
                        record_method_decision(entry, proof.decision());
                        Ok(RequestAdmission::Workspace(proof))
                    }
                    ProofResolution::Denied(decision) => {
                        record_method_decision(entry, &decision);
                        Err(external_error_for_decision(&decision)
                            .unwrap_or(AuthorizationExternalError::Forbidden)
                            .response(request.id.clone()))
                    }
                };
            }
            ResourceResolverKind::Workspace
                if matches!(
                    request.method.as_str(),
                    methods::ARTIFACT_CAPABILITIES | methods::ARTIFACT_LIST | methods::SKILLS_LIST
                ) =>
            {
                let workspace_id = if request.method == methods::ARTIFACT_CAPABILITIES {
                    serde_json::from_value::<ArtifactCapabilitiesParams>(
                        request.params.clone().unwrap_or_else(empty_object_value),
                    )
                    .map_err(|_| {
                        let decision = AuthorizationDecision::Deny {
                            reason: DenyReason::ResourceScopeMismatch,
                            disclosure: DisclosurePolicy::Validation,
                        };
                        record_method_decision(entry, &decision);
                        AuthorizationExternalError::Validation.response(request.id.clone())
                    })?
                    .workspace_id
                } else if request.method == methods::SKILLS_LIST {
                    serde_json::from_value::<SkillListParams>(
                        request.params.clone().unwrap_or_else(empty_object_value),
                    )
                    .map_err(|_| {
                        let decision = AuthorizationDecision::Deny {
                            reason: DenyReason::ResourceScopeMismatch,
                            disclosure: DisclosurePolicy::Validation,
                        };
                        record_method_decision(entry, &decision);
                        AuthorizationExternalError::Validation.response(request.id.clone())
                    })?
                    .workspace_id
                } else {
                    serde_json::from_value::<ArtifactListParams>(
                        request.params.clone().unwrap_or_else(empty_object_value),
                    )
                    .map_err(|_| {
                        let decision = AuthorizationDecision::Deny {
                            reason: DenyReason::ResourceScopeMismatch,
                            disclosure: DisclosurePolicy::Validation,
                        };
                        record_method_decision(entry, &decision);
                        AuthorizationExternalError::Validation.response(request.id.clone())
                    })?
                    .workspace_id
                };
                let resolution = resolver
                    .authorize_workspace(
                        context.principal(),
                        &action_gate,
                        entry.action,
                        workspace_id.trim(),
                    )
                    .await
                    .map_err(|_| {
                        record_authorization_unavailable(
                            entry.action.safe_name(),
                            entry.resolver.safe_name(),
                            entry.audit.safe_name(),
                        );
                        AuthorizationExternalError::Unavailable.response(request.id.clone())
                    })?;
                return match resolution {
                    ProofResolution::Authorized(proof) => {
                        record_method_decision(entry, proof.decision());
                        Ok(RequestAdmission::Workspace(proof))
                    }
                    ProofResolution::Denied(decision) => {
                        record_method_decision(entry, &decision);
                        Err(external_error_for_decision(&decision)
                            .unwrap_or(AuthorizationExternalError::Forbidden)
                            .response(request.id.clone()))
                    }
                };
            }
            ResourceResolverKind::Thread if request.method == methods::THREAD_UPDATE => {
                let params = serde_json::from_value::<ThreadUpdateParams>(
                    request.params.clone().unwrap_or_else(empty_object_value),
                )
                .map_err(|_| {
                    let decision = AuthorizationDecision::Deny {
                        reason: DenyReason::ResourceScopeMismatch,
                        disclosure: DisclosurePolicy::Validation,
                    };
                    record_method_decision(entry, &decision);
                    AuthorizationExternalError::Validation.response(request.id.clone())
                })?;
                let resolution = resolver
                    .authorize_thread(
                        context.principal(),
                        &action_gate,
                        entry.action,
                        params.thread_id.trim(),
                        Some(params.workspace_id.trim()),
                    )
                    .await
                    .map_err(|_| {
                        record_authorization_unavailable(
                            entry.action.safe_name(),
                            entry.resolver.safe_name(),
                            entry.audit.safe_name(),
                        );
                        AuthorizationExternalError::Unavailable.response(request.id.clone())
                    })?;
                return match resolution {
                    ProofResolution::Authorized(proof) => {
                        record_method_decision(entry, proof.decision());
                        Ok(RequestAdmission::ThreadManage(proof))
                    }
                    ProofResolution::Denied(decision) => {
                        record_method_decision(entry, &decision);
                        Err(external_error_for_decision(&decision)
                            .unwrap_or(AuthorizationExternalError::Forbidden)
                            .response(request.id.clone()))
                    }
                };
            }
            ResourceResolverKind::Thread
                if matches!(
                    request.method.as_str(),
                    methods::THREAD_PARTICIPANTS_LIST
                        | methods::THREAD_PARTICIPANTS_ADD
                        | methods::THREAD_PARTICIPANTS_REMOVE
                ) =>
            {
                let params = request.params.as_ref().and_then(JsonValue::as_object);
                let workspace_id = params
                    .and_then(|params| params.get("workspace_id"))
                    .and_then(JsonValue::as_str)
                    .filter(|value| !value.trim().is_empty());
                let thread_id = params
                    .and_then(|params| params.get("thread_id"))
                    .and_then(JsonValue::as_str)
                    .filter(|value| !value.trim().is_empty());
                let (Some(workspace_id), Some(thread_id)) = (workspace_id, thread_id) else {
                    let decision = AuthorizationDecision::Deny {
                        reason: DenyReason::ResourceScopeMismatch,
                        disclosure: DisclosurePolicy::Validation,
                    };
                    record_method_decision(entry, &decision);
                    return Err(AuthorizationExternalError::Validation.response(request.id.clone()));
                };
                let resolution = resolver
                    .authorize_thread(
                        context.principal(),
                        &action_gate,
                        entry.action,
                        thread_id.trim(),
                        Some(workspace_id.trim()),
                    )
                    .await
                    .map_err(|_| {
                        record_authorization_unavailable(
                            entry.action.safe_name(),
                            entry.resolver.safe_name(),
                            entry.audit.safe_name(),
                        );
                        AuthorizationExternalError::Unavailable.response(request.id.clone())
                    })?;
                return match resolution {
                    ProofResolution::Authorized(proof) => {
                        record_method_decision(entry, proof.decision());
                        Ok(RequestAdmission::ThreadParticipants(proof))
                    }
                    ProofResolution::Denied(decision) => {
                        record_method_decision(entry, &decision);
                        Err(external_error_for_decision(&decision)
                            .unwrap_or(AuthorizationExternalError::Forbidden)
                            .response(request.id.clone()))
                    }
                };
            }
            ResourceResolverKind::Thread
                if matches!(
                    request.method.as_str(),
                    methods::CLI_RUNTIME_THREAD_BINDING_GET
                        | methods::CLI_RUNTIME_THREAD_FORK
                        | methods::CLI_RUNTIME_THREAD_COMPACT
                        | methods::CLI_RUNTIME_REVIEW_START
                ) =>
            {
                let params = request.params.clone().unwrap_or_else(empty_object_value);
                let scope = match request.method.as_str() {
                    methods::CLI_RUNTIME_THREAD_BINDING_GET => {
                        serde_json::from_value::<CLIRuntimeThreadBindingGetParams>(params)
                            .map(|params| (params.workspace_id, params.thread_id))
                    }
                    methods::CLI_RUNTIME_THREAD_FORK => {
                        serde_json::from_value::<CLIRuntimeThreadForkParams>(params)
                            .map(|params| (params.workspace_id, params.source_thread_id))
                    }
                    methods::CLI_RUNTIME_THREAD_COMPACT => {
                        serde_json::from_value::<CLIRuntimeThreadCompactParams>(params)
                            .map(|params| (params.workspace_id, params.thread_id))
                    }
                    methods::CLI_RUNTIME_REVIEW_START => {
                        serde_json::from_value::<CLIRuntimeReviewStartParams>(params)
                            .map(|params| (params.workspace_id, params.thread_id))
                    }
                    _ => unreachable!("guard restricts CLI runtime thread methods"),
                }
                .map_err(|_| {
                    let decision = AuthorizationDecision::Deny {
                        reason: DenyReason::ResourceScopeMismatch,
                        disclosure: DisclosurePolicy::Validation,
                    };
                    record_method_decision(entry, &decision);
                    AuthorizationExternalError::Validation.response(request.id.clone())
                })?;
                let (workspace_id, thread_id) = scope;
                if workspace_id.trim().is_empty() || thread_id.trim().is_empty() {
                    let decision = AuthorizationDecision::Deny {
                        reason: DenyReason::ResourceScopeMismatch,
                        disclosure: DisclosurePolicy::Validation,
                    };
                    record_method_decision(entry, &decision);
                    return Err(AuthorizationExternalError::Validation.response(request.id.clone()));
                }
                let resolution = resolver
                    .authorize_thread(
                        context.principal(),
                        &action_gate,
                        entry.action,
                        thread_id.trim(),
                        Some(workspace_id.trim()),
                    )
                    .await
                    .map_err(|_| {
                        record_authorization_unavailable(
                            entry.action.safe_name(),
                            entry.resolver.safe_name(),
                            entry.audit.safe_name(),
                        );
                        AuthorizationExternalError::Unavailable.response(request.id.clone())
                    })?;
                return match resolution {
                    ProofResolution::Authorized(proof) => {
                        record_method_decision(entry, proof.decision());
                        Ok(RequestAdmission::Thread(proof))
                    }
                    ProofResolution::Denied(decision) => {
                        record_method_decision(entry, &decision);
                        Err(external_error_for_decision(&decision)
                            .unwrap_or(AuthorizationExternalError::NotFound)
                            .response(request.id.clone()))
                    }
                };
            }
            ResourceResolverKind::Thread
                if matches!(
                    request.method.as_str(),
                    methods::THREAD_GET
                        | methods::THREAD_TIMELINE_PAGE
                        | methods::THREAD_READ
                        | methods::THREAD_UNSUBSCRIBE
                        | methods::TURN_START
                        | methods::ARTIFACT_LIST_FOR_THREAD
                        | methods::ARTIFACT_LIST_FOR_MESSAGE
                        | methods::THREAD_AGENTS_DOC_GET
                        | methods::THREAD_AGENTS_DOC_SAVE
                        | methods::THREAD_AGENTS_DOC_ARCHIVE
                        | methods::THREAD_AGENTS_DOC_RESOLVE_FOR_THREAD
                ) =>
            {
                let params = request.params.as_ref().and_then(JsonValue::as_object);
                let thread_id = params
                    .and_then(|params| params.get("thread_id").or_else(|| params.get("threadId")))
                    .and_then(JsonValue::as_str)
                    .filter(|value| !value.trim().is_empty());
                let Some(thread_id) = thread_id else {
                    if action_gate.is_final_allow()
                        && matches!(
                            request.method.as_str(),
                            methods::THREAD_AGENTS_DOC_GET
                                | methods::THREAD_AGENTS_DOC_SAVE
                                | methods::THREAD_AGENTS_DOC_ARCHIVE
                        )
                    {
                        let decision = AuthorizationDecision::AllowSuperuser;
                        record_method_decision(entry, &decision);
                        return Ok(RequestAdmission::Superuser);
                    }
                    let decision = AuthorizationDecision::Deny {
                        reason: DenyReason::ResourceScopeMismatch,
                        disclosure: DisclosurePolicy::Validation,
                    };
                    record_method_decision(entry, &decision);
                    return Err(AuthorizationExternalError::Validation.response(request.id.clone()));
                };
                let expected_workspace_id = params
                    .and_then(|params| {
                        params
                            .get("workspace_id")
                            .or_else(|| params.get("workspaceId"))
                    })
                    .and_then(JsonValue::as_str)
                    .filter(|value| !value.trim().is_empty());
                let mut resolution = resolver
                    .authorize_thread(
                        context.principal(),
                        &action_gate,
                        entry.action,
                        thread_id.trim(),
                        expected_workspace_id.map(str::trim),
                    )
                    .await
                    .map_err(|_| {
                        record_authorization_unavailable(
                            entry.action.safe_name(),
                            entry.resolver.safe_name(),
                            entry.audit.safe_name(),
                        );
                        AuthorizationExternalError::Unavailable.response(request.id.clone())
                    })?;
                if resolution.denial().is_some()
                    && matches!(
                        request.method.as_str(),
                        methods::THREAD_AGENTS_DOC_GET
                            | methods::THREAD_AGENTS_DOC_SAVE
                            | methods::THREAD_AGENTS_DOC_ARCHIVE
                            | methods::THREAD_AGENTS_DOC_RESOLVE_FOR_THREAD
                    )
                    && let Some(workspace_id) = expected_workspace_id
                {
                    resolution = resolver
                        .authorize_internal_thread_via_root(
                            context.principal(),
                            &action_gate,
                            entry.action,
                            thread_id.trim(),
                            workspace_id.trim(),
                        )
                        .await
                        .map_err(|_| {
                            record_authorization_unavailable(
                                entry.action.safe_name(),
                                entry.resolver.safe_name(),
                                entry.audit.safe_name(),
                            );
                            AuthorizationExternalError::Unavailable.response(request.id.clone())
                        })?;
                }
                return match resolution {
                    ProofResolution::Authorized(proof) => {
                        record_method_decision(entry, proof.decision());
                        Ok(RequestAdmission::Thread(proof))
                    }
                    ProofResolution::Denied(decision)
                        if matches!(
                            request.method.as_str(),
                            methods::THREAD_GET | methods::THREAD_UNSUBSCRIBE | methods::TURN_START
                        ) && matches!(
                            decision,
                            AuthorizationDecision::Deny {
                                reason: DenyReason::MissingAuthoritativeResource,
                                ..
                            }
                        ) =>
                    {
                        if let Some(access) = self
                            .authorize_runtime_draft_request(
                                context,
                                request,
                                entry,
                                entry.action,
                                thread_id.trim(),
                                expected_workspace_id.map(str::trim),
                            )
                            .await?
                        {
                            Ok(RequestAdmission::RuntimeDraft(access))
                        } else {
                            record_method_decision(entry, &decision);
                            Err(external_error_for_decision(&decision)
                                .unwrap_or(AuthorizationExternalError::Forbidden)
                                .response(request.id.clone()))
                        }
                    }
                    ProofResolution::Denied(decision) => {
                        record_method_decision(entry, &decision);
                        Err(external_error_for_decision(&decision)
                            .unwrap_or(AuthorizationExternalError::Forbidden)
                            .response(request.id.clone()))
                    }
                };
            }
            ResourceResolverKind::Turn => {
                let params = request.params.as_ref().and_then(JsonValue::as_object);
                let derives_scope_from_pending = matches!(
                    request.method.as_str(),
                    methods::TURN_PERMISSION_REQUEST_RESPOND | methods::CLI_RUNTIME_REQUEST_RESPOND
                );
                let pending_scope = if request.method == methods::TURN_PERMISSION_REQUEST_RESPOND {
                    let request_id = params
                        .and_then(|params| {
                            params.get("request_id").or_else(|| params.get("requestId"))
                        })
                        .and_then(JsonValue::as_str)
                        .filter(|value| !value.trim().is_empty());
                    let Some(request_id) = request_id else {
                        let decision = AuthorizationDecision::Deny {
                            reason: DenyReason::ResourceScopeMismatch,
                            disclosure: DisclosurePolicy::Validation,
                        };
                        record_method_decision(entry, &decision);
                        return Err(
                            AuthorizationExternalError::Validation.response(request.id.clone())
                        );
                    };
                    self.native_permission_pending_requests
                        .lock()
                        .await
                        .get(request_id.trim())
                        .map(|pending| {
                            (
                                pending.workspace_id.clone(),
                                pending.thread_id.clone(),
                                pending.turn_id.clone(),
                            )
                        })
                } else if request.method == methods::CLI_RUNTIME_REQUEST_RESPOND {
                    let request_id = params
                        .and_then(|params| {
                            params.get("request_id").or_else(|| params.get("requestId"))
                        })
                        .and_then(JsonValue::as_str)
                        .filter(|value| !value.trim().is_empty());
                    let requested_workspace_id = params
                        .and_then(|params| {
                            params
                                .get("workspace_id")
                                .or_else(|| params.get("workspaceId"))
                        })
                        .and_then(JsonValue::as_str)
                        .filter(|value| !value.trim().is_empty());
                    let requested_runtime_id = params
                        .and_then(|params| {
                            params.get("runtime_id").or_else(|| params.get("runtimeId"))
                        })
                        .and_then(JsonValue::as_str)
                        .filter(|value| !value.trim().is_empty());
                    let (
                        Some(request_id),
                        Some(requested_workspace_id),
                        Some(requested_runtime_id),
                    ) = (request_id, requested_workspace_id, requested_runtime_id)
                    else {
                        let decision = AuthorizationDecision::Deny {
                            reason: DenyReason::ResourceScopeMismatch,
                            disclosure: DisclosurePolicy::Validation,
                        };
                        record_method_decision(entry, &decision);
                        return Err(
                            AuthorizationExternalError::Validation.response(request.id.clone())
                        );
                    };
                    self.crud_store
                        .get_cli_runtime_pending_request(request_id.trim())
                        .await
                        .map_err(|_| {
                            record_authorization_unavailable(
                                entry.action.safe_name(),
                                entry.resolver.safe_name(),
                                entry.audit.safe_name(),
                            );
                            AuthorizationExternalError::Unavailable.response(request.id.clone())
                        })?
                        .filter(|pending| {
                            pending.workspace_id == requested_workspace_id.trim()
                                && pending.runtime_id == requested_runtime_id.trim()
                        })
                        .and_then(|pending| {
                            pending
                                .turn_id
                                .map(|turn_id| (pending.workspace_id, pending.thread_id, turn_id))
                        })
                } else {
                    None
                };
                let turn_id = pending_scope
                    .as_ref()
                    .map(|(_, _, turn_id)| turn_id.as_str())
                    .or_else(|| {
                        params
                            .and_then(|params| {
                                params.get("turn_id").or_else(|| params.get("turnId"))
                            })
                            .and_then(JsonValue::as_str)
                            .filter(|value| !value.trim().is_empty())
                    });
                let thread_id = pending_scope
                    .as_ref()
                    .map(|(_, thread_id, _)| thread_id.as_str())
                    .or_else(|| {
                        params
                            .and_then(|params| {
                                params.get("thread_id").or_else(|| params.get("threadId"))
                            })
                            .and_then(JsonValue::as_str)
                            .filter(|value| !value.trim().is_empty())
                    });
                let workspace_id = pending_scope
                    .as_ref()
                    .map(|(workspace_id, _, _)| workspace_id.as_str())
                    .or_else(|| {
                        params
                            .and_then(|params| {
                                params
                                    .get("workspace_id")
                                    .or_else(|| params.get("workspaceId"))
                            })
                            .and_then(JsonValue::as_str)
                            .filter(|value| !value.trim().is_empty())
                    });
                let requires_explicit_thread = request.method != methods::ARTIFACT_LIST_FOR_TURN;
                let Some(turn_id) = turn_id else {
                    let decision = AuthorizationDecision::Deny {
                        reason: if derives_scope_from_pending {
                            DenyReason::MissingAuthoritativeResource
                        } else {
                            DenyReason::ResourceScopeMismatch
                        },
                        disclosure: if derives_scope_from_pending {
                            entry.disclosure
                        } else {
                            DisclosurePolicy::Validation
                        },
                    };
                    record_method_decision(entry, &decision);
                    return Err(external_error_for_decision(&decision)
                        .unwrap_or(if derives_scope_from_pending {
                            AuthorizationExternalError::NotFound
                        } else {
                            AuthorizationExternalError::Validation
                        })
                        .response(request.id.clone()));
                };
                if requires_explicit_thread && thread_id.is_none() {
                    let decision = AuthorizationDecision::Deny {
                        reason: DenyReason::ResourceScopeMismatch,
                        disclosure: DisclosurePolicy::Validation,
                    };
                    record_method_decision(entry, &decision);
                    return Err(AuthorizationExternalError::Validation.response(request.id.clone()));
                }
                let resolution = resolver
                    .authorize_turn(
                        context.principal(),
                        &action_gate,
                        entry.action,
                        turn_id.trim(),
                        workspace_id.map(str::trim),
                        thread_id.map(str::trim),
                    )
                    .await
                    .map_err(|_| {
                        record_authorization_unavailable(
                            entry.action.safe_name(),
                            entry.resolver.safe_name(),
                            entry.audit.safe_name(),
                        );
                        AuthorizationExternalError::Unavailable.response(request.id.clone())
                    })?;
                return match resolution {
                    ProofResolution::Authorized(proof) => {
                        record_method_decision(entry, proof.decision());
                        Ok(RequestAdmission::Turn(proof))
                    }
                    ProofResolution::Denied(decision) => {
                        record_method_decision(entry, &decision);
                        Err(external_error_for_decision(&decision)
                            .unwrap_or(AuthorizationExternalError::Forbidden)
                            .response(request.id.clone()))
                    }
                };
            }
            ResourceResolverKind::Artifact
                if matches!(
                    request.method.as_str(),
                    methods::ARTIFACT_GET
                        | methods::ARTIFACT_VIEW_GRANT_CREATE
                        | methods::ARTIFACT_DELETE
                        | methods::ARTIFACT_RESTORE
                        | methods::ARTIFACT_BIND
                ) =>
            {
                let params = request.params.as_ref().and_then(JsonValue::as_object);
                let artifact_id = params
                    .and_then(|params| {
                        params
                            .get("artifact_id")
                            .or_else(|| params.get("artifactId"))
                    })
                    .and_then(JsonValue::as_str)
                    .filter(|value| !value.trim().is_empty());
                let workspace_id = params
                    .and_then(|params| {
                        params
                            .get("workspace_id")
                            .or_else(|| params.get("workspaceId"))
                    })
                    .and_then(JsonValue::as_str)
                    .filter(|value| !value.trim().is_empty());
                let thread_id = params
                    .and_then(|params| params.get("thread_id").or_else(|| params.get("threadId")))
                    .and_then(JsonValue::as_str)
                    .filter(|value| !value.trim().is_empty());
                let (Some(artifact_id), Some(workspace_id)) = (artifact_id, workspace_id) else {
                    let decision = AuthorizationDecision::Deny {
                        reason: DenyReason::ResourceScopeMismatch,
                        disclosure: DisclosurePolicy::Validation,
                    };
                    record_method_decision(entry, &decision);
                    return Err(AuthorizationExternalError::Validation.response(request.id.clone()));
                };
                let resolution = resolver
                    .authorize_artifact(
                        context.principal(),
                        &action_gate,
                        entry.action,
                        artifact_id.trim(),
                        Some(workspace_id.trim()),
                        thread_id.map(str::trim),
                    )
                    .await
                    .map_err(|_| {
                        record_authorization_unavailable(
                            entry.action.safe_name(),
                            entry.resolver.safe_name(),
                            entry.audit.safe_name(),
                        );
                        AuthorizationExternalError::Unavailable.response(request.id.clone())
                    })?;
                return match resolution {
                    ProofResolution::Authorized(proof) => {
                        record_method_decision(entry, proof.decision());
                        Ok(RequestAdmission::Artifact(proof))
                    }
                    ProofResolution::Denied(decision) => {
                        record_method_decision(entry, &decision);
                        Err(external_error_for_decision(&decision)
                            .unwrap_or(AuthorizationExternalError::Forbidden)
                            .response(request.id.clone()))
                    }
                };
            }
            ResourceResolverKind::OwnSession => {}
            _ if action_gate.is_final_allow() => {
                let decision = AuthorizationDecision::AllowSuperuser;
                record_method_decision(entry, &decision);
                return Ok(RequestAdmission::Superuser);
            }
            _ => {
                // Resource families are admitted for Members only after their exact
                // resolver/proof is connected by the owning Epic 4 phase.
                let decision = AuthorizationDecision::Deny {
                    reason: DenyReason::MissingAuthoritativeResource,
                    disclosure: entry.disclosure,
                };
                record_method_decision(entry, &decision);
                return Err(external_error_for_decision(&decision)
                    .expect("resource denial has external mapping")
                    .response(request.id.clone()));
            }
        }

        let session_id = if request.method == methods::AUTH_SESSION_REVOKE {
            let params = serde_json::from_value::<AuthSessionRevokeParams>(
                request.params.clone().unwrap_or_else(empty_object_value),
            )
            .map_err(|_| {
                let decision = AuthorizationDecision::Deny {
                    reason: DenyReason::ResourceScopeMismatch,
                    disclosure: DisclosurePolicy::Validation,
                };
                record_method_decision(entry, &decision);
                AuthorizationExternalError::Validation.response(request.id.clone())
            })?;
            params.session_id
        } else {
            context.principal().session_id.clone()
        };

        let resolution = resolver
            .authorize_session(context.principal(), &action_gate, entry.action, &session_id)
            .await
            .map_err(|_| {
                record_authorization_unavailable(
                    entry.action.safe_name(),
                    entry.resolver.safe_name(),
                    entry.audit.safe_name(),
                );
                AuthorizationExternalError::Unavailable.response(request.id.clone())
            })?;
        match resolution {
            ProofResolution::Authorized(proof) => {
                record_method_decision(entry, proof.decision());
                Ok(RequestAdmission::OwnSession(proof))
            }
            ProofResolution::Denied(decision) => {
                record_method_decision(entry, &decision);
                Err(external_error_for_decision(&decision)
                    .unwrap_or(AuthorizationExternalError::Forbidden)
                    .response(request.id.clone()))
            }
        }
    }

    async fn authorize_artifact_transfer_request(
        &self,
        context: &crate::request_context::RequestContext,
        request: &JsonRpcRequest,
        entry: &'static crate::authorization::MethodAuthorizationEntry,
        action_gate: &crate::authorization::ActionGateDecision,
        resolver: &AuthorizationResolver,
    ) -> Result<RequestAdmission, JsonRpcErrorResponse> {
        let validation = || {
            let decision = AuthorizationDecision::Deny {
                reason: DenyReason::ResourceScopeMismatch,
                disclosure: DisclosurePolicy::Validation,
            };
            record_method_decision(entry, &decision);
            AuthorizationExternalError::Validation.response(request.id.clone())
        };
        let unavailable = || {
            record_authorization_unavailable(
                entry.action.safe_name(),
                entry.resolver.safe_name(),
                entry.audit.safe_name(),
            );
            AuthorizationExternalError::Unavailable.response(request.id.clone())
        };
        let owner = AuthenticatedTransferOwner::from_request_context(context);

        if request.method == methods::ARTIFACT_UPLOAD_START {
            let params = serde_json::from_value::<ArtifactUploadStartParams>(
                request.params.clone().unwrap_or_else(empty_object_value),
            )
            .map_err(|_| validation())?;
            if params.workspace_id.trim().is_empty() {
                return Err(validation());
            }
            if params
                .planned_turn_id
                .as_deref()
                .filter(|turn_id| !turn_id.trim().is_empty())
                .is_some()
            {
                let Some(thread_id) = params
                    .thread_id
                    .as_deref()
                    .filter(|thread_id| !thread_id.trim().is_empty())
                else {
                    return Err(validation());
                };
                let turn_action = ResourceAction::ThreadWrite;
                let turn_gate = AuthorizationService::new().authorize_action(
                    context.principal().kind,
                    context.role_key(),
                    turn_action,
                );
                return self
                    .finish_thread_transfer_or_runtime_draft(
                        context,
                        request,
                        entry,
                        turn_action,
                        resolver
                            .authorize_thread(
                                context.principal(),
                                &turn_gate,
                                turn_action,
                                thread_id.trim(),
                                Some(params.workspace_id.trim()),
                            )
                            .await
                            .map_err(|_| unavailable())?,
                        thread_id.trim(),
                        params.workspace_id.trim(),
                    )
                    .await;
            }
            if let Some(thread_id) = params
                .thread_id
                .as_deref()
                .filter(|thread_id| !thread_id.trim().is_empty())
            {
                return self
                    .finish_thread_transfer_or_runtime_draft(
                        context,
                        request,
                        entry,
                        entry.action,
                        resolver
                            .authorize_thread(
                                context.principal(),
                                action_gate,
                                entry.action,
                                thread_id.trim(),
                                Some(params.workspace_id.trim()),
                            )
                            .await
                            .map_err(|_| unavailable())?,
                        thread_id.trim(),
                        params.workspace_id.trim(),
                    )
                    .await;
            }
            let resolution = resolver
                .authorize_workspace(
                    context.principal(),
                    action_gate,
                    entry.action,
                    params.workspace_id.trim(),
                )
                .await
                .map_err(|_| unavailable())?;
            return self.finish_workspace_transfer_resolution(
                resolution,
                entry,
                request.id.clone(),
            );
        }
        if !matches!(
            request.method.as_str(),
            methods::ARTIFACT_UPLOAD_FINISH | methods::ARTIFACT_UPLOAD_ABORT
        ) {
            return Err(validation());
        }
        let params = request.params.as_ref().and_then(JsonValue::as_object);
        let workspace_id = params
            .and_then(|value| {
                value
                    .get("workspace_id")
                    .or_else(|| value.get("workspaceId"))
            })
            .and_then(JsonValue::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(validation)?;
        let upload_id = params
            .and_then(|value| value.get("upload_id").or_else(|| value.get("uploadId")))
            .and_then(JsonValue::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(validation)?;
        let session = self
            .artifact_uploads
            .lookup_for_owner(
                &owner,
                workspace_id.trim(),
                upload_id.trim(),
                now_timestamp_secs(),
            )
            .await
            .map_err(|_| {
                let decision = AuthorizationDecision::Deny {
                    reason: DenyReason::MissingAuthoritativeResource,
                    disclosure: entry.disclosure,
                };
                record_method_decision(entry, &decision);
                AuthorizationExternalError::NotFound.response(request.id.clone())
            })?;
        if let Some(thread_id) = session
            .thread_id
            .as_deref()
            .filter(|_| session.planned_turn_id.is_some())
        {
            let turn_action = ResourceAction::ThreadWrite;
            let turn_gate = AuthorizationService::new().authorize_action(
                context.principal().kind,
                context.role_key(),
                turn_action,
            );
            return self
                .finish_thread_transfer_or_runtime_draft(
                    context,
                    request,
                    entry,
                    turn_action,
                    resolver
                        .authorize_thread(
                            context.principal(),
                            &turn_gate,
                            turn_action,
                            thread_id,
                            Some(session.workspace_id.as_str()),
                        )
                        .await
                        .map_err(|_| unavailable())?,
                    thread_id,
                    session.workspace_id.as_str(),
                )
                .await;
        }
        if let Some(thread_id) = session.thread_id.as_deref() {
            return self
                .finish_thread_transfer_or_runtime_draft(
                    context,
                    request,
                    entry,
                    entry.action,
                    resolver
                        .authorize_thread(
                            context.principal(),
                            action_gate,
                            entry.action,
                            thread_id,
                            Some(session.workspace_id.as_str()),
                        )
                        .await
                        .map_err(|_| unavailable())?,
                    thread_id,
                    session.workspace_id.as_str(),
                )
                .await;
        }
        let resolution = resolver
            .authorize_workspace(
                context.principal(),
                action_gate,
                entry.action,
                session.workspace_id.as_str(),
            )
            .await
            .map_err(|_| unavailable())?;
        self.finish_workspace_transfer_resolution(resolution, entry, request.id.clone())
    }

    fn finish_workspace_transfer_resolution(
        &self,
        resolution: ProofResolution<AuthorizedWorkspace>,
        entry: &'static crate::authorization::MethodAuthorizationEntry,
        request_id: RequestId,
    ) -> Result<RequestAdmission, JsonRpcErrorResponse> {
        match resolution {
            ProofResolution::Authorized(proof) => {
                record_method_decision(entry, proof.decision());
                Ok(RequestAdmission::Workspace(proof))
            }
            ProofResolution::Denied(decision) => {
                record_method_decision(entry, &decision);
                Err(external_error_for_decision(&decision)
                    .unwrap_or(AuthorizationExternalError::NotFound)
                    .response(request_id))
            }
        }
    }

    async fn finish_thread_transfer_or_runtime_draft(
        &self,
        context: &crate::request_context::RequestContext,
        request: &JsonRpcRequest,
        entry: &'static crate::authorization::MethodAuthorizationEntry,
        action: ResourceAction,
        resolution: ProofResolution<AuthorizedThread>,
        thread_id: &str,
        workspace_id: &str,
    ) -> Result<RequestAdmission, JsonRpcErrorResponse> {
        match resolution {
            ProofResolution::Authorized(proof) => {
                record_method_decision(entry, proof.decision());
                Ok(RequestAdmission::Thread(proof))
            }
            ProofResolution::Denied(decision)
                if matches!(
                    decision,
                    AuthorizationDecision::Deny {
                        reason: DenyReason::MissingAuthoritativeResource,
                        ..
                    }
                ) =>
            {
                if let Some(access) = self
                    .authorize_runtime_draft_request(
                        context,
                        request,
                        entry,
                        action,
                        thread_id,
                        Some(workspace_id),
                    )
                    .await?
                {
                    Ok(RequestAdmission::RuntimeDraft(access))
                } else {
                    record_method_decision(entry, &decision);
                    Err(external_error_for_decision(&decision)
                        .unwrap_or(AuthorizationExternalError::NotFound)
                        .response(request.id.clone()))
                }
            }
            ProofResolution::Denied(decision) => {
                record_method_decision(entry, &decision);
                Err(external_error_for_decision(&decision)
                    .unwrap_or(AuthorizationExternalError::NotFound)
                    .response(request.id.clone()))
            }
        }
    }

    async fn authorize_thread_start_request(
        &self,
        context: &crate::request_context::RequestContext,
        request: &JsonRpcRequest,
        entry: &'static crate::authorization::MethodAuthorizationEntry,
        service: &AuthorizationService,
    ) -> Result<RequestAdmission, JsonRpcErrorResponse> {
        let params = serde_json::from_value::<ThreadStartParams>(
            request.params.clone().unwrap_or_else(empty_object_value),
        )
        .map_err(|error| {
            let decision = AuthorizationDecision::Deny {
                reason: DenyReason::ResourceScopeMismatch,
                disclosure: DisclosurePolicy::Validation,
            };
            record_method_decision_for_action(entry, ResourceAction::ThreadCreate, &decision);
            JsonRpcErrorResponse::new(
                Some(request.id.clone()),
                INVALID_PARAMS_CODE,
                format!("invalid params for `{}`: {error}", methods::THREAD_START),
            )
        })?;
        if params.thread_id.trim().is_empty() || params.workspace_id.trim().is_empty() {
            let decision = AuthorizationDecision::Deny {
                reason: DenyReason::ResourceScopeMismatch,
                disclosure: DisclosurePolicy::Validation,
            };
            record_method_decision_for_action(entry, ResourceAction::ThreadCreate, &decision);
            return Err(AuthorizationExternalError::Validation.response(request.id.clone()));
        }

        let scope = pioneer_crud::resolve_thread_start_authorization_scope(
            &self.crud_store.database_connection(),
            params.thread_id.trim(),
            params.workspace_id.trim(),
        )
        .await
        .map_err(|_| {
            record_authorization_unavailable(
                ResourceAction::ThreadCreate.safe_name(),
                entry.resolver.safe_name(),
                entry.audit.safe_name(),
            );
            AuthorizationExternalError::Unavailable.response(request.id.clone())
        })?;
        let resolver = AuthorizationResolver::new((*self.crud_store).clone());

        match scope {
            pioneer_crud::ThreadStartAuthorizationScope::Missing => {
                let action = ResourceAction::ThreadCreate;
                let gate =
                    service.authorize_action(context.principal().kind, context.role_key(), action);
                let resolution = resolver
                    .authorize_workspace(
                        context.principal(),
                        &gate,
                        action,
                        params.workspace_id.trim(),
                    )
                    .await
                    .map_err(|_| {
                        record_authorization_unavailable(
                            action.safe_name(),
                            entry.resolver.safe_name(),
                            entry.audit.safe_name(),
                        );
                        AuthorizationExternalError::Unavailable.response(request.id.clone())
                    })?;
                match resolution {
                    ProofResolution::Authorized(proof) => {
                        record_method_decision_for_action(entry, action, proof.decision());
                        Ok(RequestAdmission::ThreadCreate(proof))
                    }
                    ProofResolution::Denied(decision) => {
                        record_method_decision_for_action(entry, action, &decision);
                        Err(external_error_for_decision(&decision)
                            .unwrap_or(AuthorizationExternalError::Forbidden)
                            .response(request.id.clone()))
                    }
                }
            }
            pioneer_crud::ThreadStartAuthorizationScope::Existing => {
                let action = ResourceAction::ThreadRead;
                let gate =
                    service.authorize_action(context.principal().kind, context.role_key(), action);
                let resolution = resolver
                    .authorize_thread(
                        context.principal(),
                        &gate,
                        action,
                        params.thread_id.trim(),
                        Some(params.workspace_id.trim()),
                    )
                    .await
                    .map_err(|_| {
                        record_authorization_unavailable(
                            action.safe_name(),
                            entry.resolver.safe_name(),
                            entry.audit.safe_name(),
                        );
                        AuthorizationExternalError::Unavailable.response(request.id.clone())
                    })?;
                match resolution {
                    ProofResolution::Authorized(proof) => {
                        record_method_decision_for_action(entry, action, proof.decision());
                        Ok(RequestAdmission::ThreadOpen(proof))
                    }
                    ProofResolution::Denied(decision) => {
                        record_method_decision_for_action(entry, action, &decision);
                        Err(external_error_for_decision(&decision)
                            .unwrap_or(AuthorizationExternalError::Forbidden)
                            .response(request.id.clone()))
                    }
                }
            }
            pioneer_crud::ThreadStartAuthorizationScope::ParentMismatch => {
                let decision = AuthorizationDecision::Deny {
                    reason: DenyReason::ResourceScopeMismatch,
                    disclosure: DisclosurePolicy::NotFound,
                };
                record_method_decision_for_action(entry, ResourceAction::ThreadRead, &decision);
                Err(AuthorizationExternalError::NotFound.response(request.id.clone()))
            }
        }
    }

    fn dispatch_turn_start<'a>(
        &'a self,
        context: crate::request_context::RequestContext,
        request: JsonRpcRequest,
        admission: RequestAdmission,
    ) -> MessageFuture<'a, ()> {
        let connection_id = context.connection_id();
        let params_value = request.params.unwrap_or_else(empty_object_value);
        let client_author_override =
            super::message_turn::contains_client_author_snapshot(&params_value);
        match serde_json::from_value::<TurnStartParams>(params_value) {
            Ok(mut params) => message_future(async move {
                if client_author_override {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request.id.clone()),
                            INVALID_PARAMS_CODE,
                            "invalid params for `turn/start`: Turn author is server-owned",
                        ),
                    )
                    .await;
                    return;
                }
                let thread = match self
                    .thread_manager
                    .thread_get(params.thread_id.trim())
                    .await
                {
                    Some(thread) => thread,
                    None => {
                        self.send_error(
                            connection_id,
                            JsonRpcErrorResponse::new(
                                Some(request.id),
                                INVALID_REQUEST_CODE,
                                format!("thread `{}` is not loaded", params.thread_id.trim()),
                            ),
                        )
                        .await;
                        return;
                    }
                };
                let effective_mode =
                    super::message_turn::effective_turn_mode(params.mode, thread.mode);
                params.mode = Some(effective_mode);

                if effective_mode == pioneer_protocol::ThreadMode::Message {
                    match super::message_turn::MessageTurnAdmission::from_dispatch(
                        &context,
                        &admission,
                        params.thread_id.trim(),
                    ) {
                        Ok(message_admission) => {
                            self.turn_start_message(
                                &context,
                                message_admission,
                                request.id,
                                params,
                                client_author_override,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_REQUEST_CODE,
                                    format!("failed to bind message authorization: {error:#}"),
                                ),
                            )
                            .await;
                        }
                    }
                    return;
                }

                let execution_admission = if let Some(proof) = admission.thread() {
                    debug_assert_eq!(proof.thread_id(), params.thread_id.trim());
                    crate::authorization::ExecutionAuthorizationAdmission::from_authorized_thread(
                        &context,
                        proof,
                        self.authorization_invalidation_hub.current_revision(),
                    )
                } else if let Some(access) = admission.runtime_draft() {
                    debug_assert_eq!(access.thread_id(), params.thread_id.trim());
                    crate::authorization::ExecutionAuthorizationAdmission::from_authorized_runtime_draft(
                        &context,
                        access.clone(),
                        self.authorization_invalidation_hub.current_revision(),
                    )
                } else {
                    unreachable!("central admission supplies a persisted thread or runtime draft")
                };
                match execution_admission {
                    Ok(execution_admission) => {
                        self.turn_start(&context, execution_admission, request.id, params)
                            .await;
                    }
                    Err(error) => {
                        self.send_error(
                            connection_id,
                            JsonRpcErrorResponse::new(
                                Some(request.id),
                                INVALID_REQUEST_CODE,
                                format!("failed to bind execution authorization: {error:#}"),
                            ),
                        )
                        .await;
                    }
                }
            }),
            Err(error) => message_future(async move {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request.id),
                        INVALID_PARAMS_CODE,
                        format!("invalid params for `{}`: {error}", methods::TURN_START),
                    ),
                )
                .await;
            }),
        }
    }

    fn dispatch_settings_update<'a>(
        &'a self,
        context: crate::request_context::RequestContext,
        request: JsonRpcRequest,
    ) -> MessageFuture<'a, ()> {
        let connection_id = context.connection_id();
        let params_value = request.params.unwrap_or_else(empty_object_value);
        let params = match serde_json::from_value::<GatewaySettingsUpdateParams>(params_value) {
            Ok(params) => params,
            Err(error) => {
                return message_future(async move {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request.id),
                            INVALID_PARAMS_CODE,
                            format!("invalid params for `{}`: {error}", methods::SETTINGS_UPDATE),
                        ),
                    )
                    .await;
                });
            }
        };

        message_future(async move {
            match self.update_gateway_settings(&context, params.update).await {
                Ok(settings) => {
                    let result = pioneer_protocol::GatewaySettingsUpdateResponse { settings };
                    match JsonRpcResponse::from_result(request.id, &result) {
                        Ok(response) => {
                            if let Err(error) = self.send_json(connection_id, &response).await {
                                warn!(
                                    error = %format!("{error:#}"),
                                    "failed to send gateway settings update response"
                                );
                            }
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    None,
                                    INVALID_REQUEST_CODE,
                                    format!(
                                        "failed to encode `{}` response: {error}",
                                        methods::SETTINGS_UPDATE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                Err(error) => {
                    let response = if error
                        .downcast_ref::<VoiceReconfigurationBusyError>()
                        .is_some()
                    {
                        voice_reconfiguration_busy_error(Some(request.id))
                    } else {
                        JsonRpcErrorResponse::new(
                            Some(request.id),
                            INVALID_REQUEST_CODE,
                            format!("failed to update gateway settings: {error:#}"),
                        )
                    };
                    self.send_error(connection_id, response).await;
                }
            }
        })
    }

    fn process_request_inner<'a>(
        &'a self,
        context: crate::request_context::RequestContext,
        request: JsonRpcRequest,
        admission: RequestAdmission,
    ) -> MessageFuture<'a, ()> {
        let connection_id = context.connection_id();
        let method = request.method.clone();
        dispatch_request_future! {
            method.as_str();
                methods::WORKSPACE_MEMBER_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<WorkspaceMemberListParams>(params_value) {
                        Ok(params) => {
                            self.workspace_member_list(
                                &context,
                                admission.workspace().expect(
                                    "central admission supplies workspace member-list proof",
                                ),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!("invalid params for `{}`: {error}", methods::WORKSPACE_MEMBER_LIST),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::WORKSPACE_MEMBER_ADD => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<WorkspaceMemberAddParams>(params_value) {
                        Ok(params) => {
                            self.workspace_member_add(
                                &context,
                                admission.workspace().expect(
                                    "central admission supplies workspace member-add proof",
                                ),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!("invalid params for `{}`: {error}", methods::WORKSPACE_MEMBER_ADD),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::WORKSPACE_MEMBER_REMOVE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<WorkspaceMemberRemoveParams>(params_value) {
                        Ok(params) => {
                            self.workspace_member_remove(
                                &context,
                                admission.workspace().expect(
                                    "central admission supplies workspace member-remove proof",
                                ),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!("invalid params for `{}`: {error}", methods::WORKSPACE_MEMBER_REMOVE),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MEMBER_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<MemberListParams>(params_value) {
                        Ok(params) => {
                            self.member_list(
                                &context,
                                admission.member_directory().expect(
                                    "central admission supplies member directory proof",
                                ),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!("invalid params for `{}`: {error}", methods::MEMBER_LIST),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MEMBER_SUSPEND => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<MemberSuspendParams>(params_value) {
                        Ok(params) => {
                            self.member_suspend(
                                &context,
                                admission.member_principal().expect(
                                    "central admission supplies Member principal proof",
                                ),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!("invalid params for `{}`: {error}", methods::MEMBER_SUSPEND),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MEMBER_RESTORE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<MemberRestoreParams>(params_value) {
                        Ok(params) => {
                            self.member_restore(
                                &context,
                                admission.member_principal().expect(
                                    "central admission supplies Member principal proof",
                                ),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!("invalid params for `{}`: {error}", methods::MEMBER_RESTORE),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MEMBER_REMOVE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<MemberRemoveParams>(params_value) {
                        Ok(params) => {
                            self.member_remove(
                                &context,
                                admission.member_principal().expect(
                                    "central admission supplies Member principal proof",
                                ),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!("invalid params for `{}`: {error}", methods::MEMBER_REMOVE),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MEMBER_DEVICE_CREATE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<MemberDeviceCreateParams>(params_value) {
                        Ok(params) => {
                            self.member_device_create(
                                &context,
                                admission.member_principal().expect(
                                    "central admission supplies Member principal proof",
                                ),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!("invalid params for `{}`: {error}", methods::MEMBER_DEVICE_CREATE),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::INVITE_CREATE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<InvitationCreateParams>(params_value) {
                        Ok(params) => {
                            self.invitation_create(
                                &context,
                                admission.invitation_grants().expect(
                                    "central admission supplies invitation grant proof",
                                ),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::INVITE_CREATE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::INVITE_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<InvitationListParams>(params_value) {
                        Ok(params) => {
                            self.invitation_list(
                                &context,
                                admission.invitation_collection().expect(
                                    "central admission supplies invitation collection proof",
                                ),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::INVITE_LIST
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::INVITE_REVOKE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<InvitationRevokeParams>(params_value) {
                        Ok(params) => {
                            self.invitation_revoke(
                                &context,
                                admission.invitation().expect(
                                    "central admission supplies invitation proof",
                                ),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::INVITE_REVOKE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::AUTH_ME => {
                    if request.params.as_ref().is_some_and(|params| {
                        params.as_object().is_none_or(|value| !value.is_empty())
                    }) {
                        self.send_error(
                            connection_id,
                            JsonRpcErrorResponse::new(
                                Some(request.id),
                                INVALID_PARAMS_CODE,
                                "auth/me does not accept params",
                            ),
                        )
                        .await;
                    } else {
                        self.auth_me(
                            &context,
                            admission
                                .own_session()
                                .expect("central admission supplies own-session proof"),
                            request.id,
                        )
                        .await;
                    }
                }
                methods::AUTH_SESSION_LIST => {
                    if request.params.as_ref().is_some_and(|params| {
                        params.as_object().is_none_or(|value| !value.is_empty())
                    }) {
                        self.send_error(
                            connection_id,
                            JsonRpcErrorResponse::new(
                                Some(request.id),
                                INVALID_PARAMS_CODE,
                                "auth/session/list does not accept params",
                            ),
                        )
                        .await;
                    } else {
                        self.auth_session_list(
                            &context,
                            admission
                                .own_session()
                                .expect("central admission supplies own-session proof"),
                            request.id,
                        )
                        .await;
                    }
                }
                methods::AUTH_SESSION_REVOKE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<AuthSessionRevokeParams>(params_value) {
                        Ok(params) => {
                            self.auth_session_revoke(
                                &context,
                                admission
                                    .own_session()
                                    .expect("central admission supplies own-session proof"),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::AUTH_SESSION_REVOKE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::AUTH_LOGOUT => {
                    if request.params.as_ref().is_some_and(|params| {
                        params.as_object().is_none_or(|value| !value.is_empty())
                    }) {
                        self.send_error(
                            connection_id,
                            JsonRpcErrorResponse::new(
                                Some(request.id),
                                INVALID_PARAMS_CODE,
                                "auth/logout does not accept params",
                            ),
                        )
                        .await;
                    } else {
                        self.auth_logout(
                            &context,
                            admission
                                .own_session()
                                .expect("central admission supplies own-session proof"),
                            request.id,
                        )
                        .await;
                    }
                }
                methods::AUTH_DEVICE_CREATE => {
                    if request.params.as_ref().is_some_and(|params| {
                        params.as_object().is_none_or(|value| !value.is_empty())
                    }) {
                        self.send_error(
                            connection_id,
                            JsonRpcErrorResponse::new(
                                Some(request.id),
                                INVALID_PARAMS_CODE,
                                "auth/device/create does not accept params",
                            ),
                        )
                        .await;
                    } else {
                        self.auth_device_create(
                            &context,
                            admission
                                .own_session()
                                .expect("central admission supplies own-session proof"),
                            request.id,
                        )
                        .await;
                    }
                }
                methods::WORKSPACE_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<WorkspaceListParams>(params_value) {
                        Ok(params) => {
                            self.workspace_list(
                                &context,
                                admission.workspace_collection().expect(
                                    "central admission supplies workspace-collection proof",
                                ),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::WORKSPACE_LIST
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::WORKSPACE_CREATE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<WorkspaceCreateParams>(params_value) {
                        Ok(params) => {
                            self.workspace_create(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::WORKSPACE_CREATE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::WORKSPACE_DEFAULT => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<WorkspaceDefaultParams>(params_value) {
                        Ok(params) => {
                            self.workspace_default(
                                &context,
                                admission.workspace_collection().expect(
                                    "central admission supplies workspace-collection proof",
                                ),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::WORKSPACE_DEFAULT
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::WORKSPACE_SELECT => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<WorkspaceSelectParams>(params_value) {
                        Ok(params) => {
                            self.workspace_select(
                                &context,
                                admission
                                    .workspace()
                                    .expect("central admission supplies workspace proof"),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::WORKSPACE_SELECT
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::WORKSPACE_UPDATE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<WorkspaceUpdateParams>(params_value) {
                        Ok(params) => {
                            self.workspace_update(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::WORKSPACE_UPDATE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::VOICE_STATUS => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<VoiceStatusParams>(params_value) {
                        Ok(params) => self.voice_status(&context, request.id, params).await,
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::VOICE_STATUS
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::VOICE_SESSION_START => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<VoiceSessionStartParams>(params_value) {
                        Ok(params) => {
                            self.voice_session_start(&context, request.id, params)
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::VOICE_SESSION_START
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::VOICE_SESSION_FINALIZE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<VoiceSessionFinalizeParams>(params_value) {
                        Ok(params) => {
                            self.voice_session_finalize(&context, request.id, params)
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::VOICE_SESSION_FINALIZE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::VOICE_SESSION_CANCEL => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<VoiceSessionCancelParams>(params_value) {
                        Ok(params) => {
                            self.voice_session_cancel(&context, request.id, params)
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::VOICE_SESSION_CANCEL
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MEMORY_SEARCH => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<MemorySearchParams>(params_value) {
                        Ok(params) => self.memory_search(&context, request.id, params).await,
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::MEMORY_SEARCH
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MEMORY_GET => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<MemoryGetParams>(params_value) {
                        Ok(params) => self.memory_get(&context, request.id, params).await,
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::MEMORY_GET
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MEMORY_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<MemoryListParams>(params_value) {
                        Ok(params) => self.memory_list(&context, request.id, params).await,
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::MEMORY_LIST
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MEMORY_REMEMBER => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<MemoryRememberParams>(params_value) {
                        Ok(params) => {
                            self.memory_remember(&context, request.id, params)
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::MEMORY_REMEMBER
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MEMORY_FORGET => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<MemoryForgetParams>(params_value) {
                        Ok(params) => self.memory_forget(&context, request.id, params).await,
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::MEMORY_FORGET
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MEMORY_CANDIDATES_LIST => {
                    self.dispatch_memory_candidates_list(&context, request.id, request.params)
                        .await;
                }
                methods::MEMORY_CANDIDATES_GET => {
                    self.dispatch_memory_candidates_get(&context, request.id, request.params)
                        .await;
                }
                methods::MEMORY_CANDIDATES_DECIDE => {
                    self.dispatch_memory_candidates_decide(&context,
                        request.id,
                        request.params,
                    )
                    .await;
                }
                methods::MEMORY_CANDIDATES_APPROVE => {
                    self.dispatch_memory_candidates_approve(&context,
                        request.id,
                        request.params,
                    )
                    .await;
                }
                methods::MEMORY_CANDIDATES_REJECT => {
                    self.dispatch_memory_candidates_reject(&context,
                        request.id,
                        request.params,
                    )
                    .await;
                }
                methods::MEMORY_CANDIDATES_EDIT_AND_APPROVE => {
                    self.dispatch_memory_candidates_edit_and_approve(&context,
                        request.id,
                        request.params,
                    )
                    .await;
                }
                methods::MEMORY_CANDIDATES_MERGE => {
                    self.dispatch_memory_candidates_merge(&context,
                        request.id,
                        request.params,
                    )
                    .await;
                }
                methods::MEMORY_CANDIDATES_SUPPRESS_SIMILAR => {
                    self.dispatch_memory_candidates_suppress_similar(&context,
                        request.id,
                        request.params,
                    )
                    .await;
                }
                methods::THREAD_START => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ThreadStartParams>(params_value) {
                        Ok(params) => {
                            if let Some(proof) = admission.thread_create() {
                                self.thread_create_and_start(
                                    &context,
                                    proof,
                                    request.id,
                                    params,
                                )
                                .await;
                            } else {
                                self.thread_open(
                                    &context,
                                    admission
                                        .thread_open()
                                        .expect("central admission supplies exact thread proof"),
                                    request.id,
                                    params,
                                )
                                .await;
                            }
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::THREAD_START
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::THREAD_TREE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ThreadTreeParams>(params_value) {
                        Ok(params) => {
                            self.thread_tree(
                                &context,
                                admission
                                    .workspace()
                                    .expect("central admission supplies exact workspace proof"),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::THREAD_TREE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::THREAD_UPDATE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    if ["creator", "created_by", "createdBy", "author"]
                        .iter()
                        .any(|field| params_value.get(field).is_some())
                    {
                        self.send_error(
                            connection_id,
                            JsonRpcErrorResponse::new(
                                Some(request.id),
                                INVALID_PARAMS_CODE,
                                "thread creator is server-owned and cannot be changed",
                            ),
                        )
                        .await;
                        return;
                    }
                    match serde_json::from_value::<ThreadUpdateParams>(params_value) {
                        Ok(params) => {
                            self.thread_update(
                                &context,
                                admission
                                    .thread_manage()
                                    .expect("central admission supplies exact management proof"),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::THREAD_UPDATE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::THREAD_PARTICIPANTS_LIST
                | methods::THREAD_PARTICIPANTS_ADD
                | methods::THREAD_PARTICIPANTS_REMOVE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    let parsed = if method == methods::THREAD_PARTICIPANTS_LIST {
                        serde_json::from_value::<ThreadParticipantsListParams>(params_value)
                            .map(|params| {
                                (
                                    params.workspace_id,
                                    params.thread_id,
                                    super::thread_handlers::ThreadParticipantOperation::List,
                                )
                            })
                    } else {
                        serde_json::from_value::<ThreadParticipantMutationParams>(params_value)
                            .map(|params| {
                                let operation = if method == methods::THREAD_PARTICIPANTS_ADD {
                                    super::thread_handlers::ThreadParticipantOperation::Add(
                                        params.principal_id,
                                    )
                                } else {
                                    super::thread_handlers::ThreadParticipantOperation::Remove(
                                        params.principal_id,
                                    )
                                };
                                (params.workspace_id, params.thread_id, operation)
                            })
                    };
                    let (workspace_id, thread_id, operation) = match parsed {
                        Ok(parsed) => parsed,
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{method}`: {error}"
                                    ),
                                ),
                            )
                            .await;
                            return;
                        }
                    };
                    self.thread_participants(
                        &context,
                        admission
                            .thread_participants()
                            .expect("central admission supplies participant-management proof"),
                        request.id,
                        workspace_id.as_str(),
                        thread_id.as_str(),
                        operation,
                    )
                    .await;
                }
                methods::THREAD_MOVE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ThreadMoveParams>(params_value) {
                        Ok(params) => {
                            self.thread_move(&context, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::THREAD_MOVE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::THREAD_FOLDER_CREATE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ThreadFolderCreateParams>(params_value) {
                        Ok(params) => {
                            self.thread_folder_create(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::THREAD_FOLDER_CREATE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::THREAD_FOLDER_MOVE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ThreadFolderMoveParams>(params_value) {
                        Ok(params) => {
                            self.thread_folder_move(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::THREAD_FOLDER_MOVE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::THREAD_FOLDER_DELETE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ThreadFolderDeleteParams>(params_value) {
                        Ok(params) => {
                            self.thread_folder_delete(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::THREAD_FOLDER_DELETE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::THREAD_AGENTS_DOC_GET => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ThreadAgentsDocGetParams>(params_value) {
                        Ok(params) => {
                            self.thread_agents_doc_get(
                                &context,
                                request.id,
                                params,
                                admission.thread(),
                            )
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::THREAD_AGENTS_DOC_GET
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::THREAD_AGENTS_DOC_SAVE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ThreadAgentsDocSaveParams>(params_value) {
                        Ok(params) => {
                            self.thread_agents_doc_save(
                                &context,
                                request.id,
                                params,
                                admission.thread(),
                            )
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::THREAD_AGENTS_DOC_SAVE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::THREAD_AGENTS_DOC_ARCHIVE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ThreadAgentsDocArchiveParams>(params_value) {
                        Ok(params) => {
                            self.thread_agents_doc_archive(
                                &context,
                                request.id,
                                params,
                                admission.thread(),
                            )
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::THREAD_AGENTS_DOC_ARCHIVE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::THREAD_AGENTS_DOC_RESOLVE_FOR_THREAD => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ThreadAgentsDocResolveForThreadParams>(
                        params_value,
                    ) {
                        Ok(params) => {
                            self.thread_agents_doc_resolve_for_thread(&context,
                                request.id,
                                params,
                                admission.thread(),
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::THREAD_AGENTS_DOC_RESOLVE_FOR_THREAD
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::THREAD_GET => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ThreadGetParams>(params_value) {
                        Ok(params) => {
                            let authorization = if let Some(proof) = admission.thread() {
                                ThreadAccessAuthorization::Persisted(proof)
                            } else if let Some(access) = admission.runtime_draft() {
                                ThreadAccessAuthorization::RuntimeDraft(access)
                            } else {
                                unreachable!(
                                    "central admission supplies persisted thread or runtime draft"
                                )
                            };
                            debug_assert_eq!(authorization.thread_id(), params.thread_id.trim());
                            self.thread_get(&context, authorization, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::THREAD_GET
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::THREAD_TIMELINE_PAGE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ThreadTimelinePageParams>(params_value) {
                        Ok(params) => {
                            let proof = admission
                                .thread()
                                .expect("central admission supplies thread proof");
                            debug_assert_eq!(proof.thread_id(), params.thread_id.trim());
                            self.thread_timeline_page(&context, proof, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::THREAD_TIMELINE_PAGE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::THREAD_READ => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ThreadReadParams>(params_value) {
                        Ok(params) => {
                            let proof = admission
                                .thread()
                                .expect("central admission supplies thread proof");
                            debug_assert_eq!(proof.thread_id(), params.thread_id.trim());
                            self.thread_read(&context, proof, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::THREAD_READ
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TURN_WORK_PAGE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TurnWorkPageParams>(params_value) {
                        Ok(params) => {
                            let proof = admission
                                .turn()
                                .expect("central admission supplies turn proof");
                            debug_assert_eq!(proof.thread_id(), params.thread_id.trim());
                            debug_assert_eq!(proof.turn_id(), params.turn_id.trim());
                            self.turn_work_page(&context, proof, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TURN_WORK_PAGE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TURN_WORK_ITEMS_GET => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TurnWorkItemsGetParams>(params_value) {
                        Ok(params) => {
                            let proof = admission
                                .turn()
                                .expect("central admission supplies turn proof");
                            debug_assert_eq!(proof.thread_id(), params.thread_id.trim());
                            debug_assert_eq!(proof.turn_id(), params.turn_id.trim());
                            self.turn_work_items_get(&context, proof, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TURN_WORK_ITEMS_GET
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TURN_CANCEL => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TurnCancelParams>(params_value) {
                        Ok(params) => {
                            let proof = admission
                                .turn()
                                .expect("central admission supplies turn proof");
                            debug_assert_eq!(proof.thread_id(), params.thread_id.trim());
                            debug_assert_eq!(proof.turn_id(), params.turn_id.trim());
                            self.turn_cancel(&context, proof, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TURN_CANCEL
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TURN_MESSAGE_EDIT => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TurnMessageEditParams>(params_value) {
                        Ok(params) => {
                            let proof = admission
                                .turn()
                                .expect("central admission supplies message Turn proof");
                            debug_assert_eq!(proof.thread_id(), params.thread_id.trim());
                            debug_assert_eq!(proof.turn_id(), params.turn_id.trim());
                            self.turn_message_edit(&context, proof, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TURN_MESSAGE_EDIT
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TURN_MESSAGE_DELETE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TurnMessageDeleteParams>(params_value) {
                        Ok(params) => {
                            let proof = admission
                                .turn()
                                .expect("central admission supplies message Turn proof");
                            debug_assert_eq!(proof.thread_id(), params.thread_id.trim());
                            debug_assert_eq!(proof.turn_id(), params.turn_id.trim());
                            self.turn_message_delete(&context, proof, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TURN_MESSAGE_DELETE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TURN_MESSAGE_REVISIONS_PAGE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TurnMessageRevisionsPageParams>(params_value) {
                        Ok(params) => {
                            let proof = admission
                                .turn()
                                .expect("central admission supplies message Turn proof");
                            debug_assert_eq!(proof.thread_id(), params.thread_id.trim());
                            debug_assert_eq!(proof.turn_id(), params.turn_id.trim());
                            self.turn_message_revisions_page(
                                &context,
                                proof,
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TURN_MESSAGE_REVISIONS_PAGE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TURN_RESUME => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TurnResumeParams>(params_value) {
                        Ok(params) => {
                            let proof = admission
                                .turn()
                                .expect("central admission supplies turn proof");
                            debug_assert_eq!(proof.thread_id(), params.thread_id.trim());
                            debug_assert_eq!(proof.turn_id(), params.turn_id.trim());
                            self.turn_resume(&context, proof, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TURN_RESUME
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TURN_GET => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    let params = if params_value.is_null() {
                        Ok(TurnGetParams::default())
                    } else {
                        serde_json::from_value::<TurnGetParams>(params_value)
                    };

                    match params {
                        Ok(params) => {
                            let proof = admission
                                .turn()
                                .expect("central admission supplies turn proof");
                            debug_assert_eq!(proof.thread_id(), params.thread_id.trim());
                            debug_assert_eq!(proof.turn_id(), params.turn_id.trim());
                            self.turn_get(&context, proof, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!("invalid params for `{}`: {error}", methods::TURN_GET),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TURN_ITEMS => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    let params = if params_value.is_null() {
                        Ok(TurnItemsParams::default())
                    } else {
                        serde_json::from_value::<TurnItemsParams>(params_value)
                    };

                    match params {
                        Ok(params) => {
                            let proof = admission
                                .turn()
                                .expect("central admission supplies turn proof");
                            debug_assert_eq!(proof.thread_id(), params.thread_id.trim());
                            debug_assert_eq!(proof.turn_id(), params.turn_id.trim());
                            self.turn_items(&context, proof, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TURN_ITEMS
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TURN_PERMISSION_REQUEST_RESPOND => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TurnPermissionRequestRespondParams>(params_value)
                    {
                        Ok(params) => {
                            let proof = admission
                                .turn()
                                .expect("central admission supplies pending-turn proof");
                            self.turn_permission_request_respond(
                                &context,
                                proof,
                                request.id,
                                params,
                            )
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TURN_PERMISSION_REQUEST_RESPOND
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::THREAD_UNSUBSCRIBE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ThreadUnsubscribeParams>(params_value) {
                        Ok(params) => {
                            let authorization = if let Some(proof) = admission.thread() {
                                ThreadAccessAuthorization::Persisted(proof)
                            } else if let Some(access) = admission.runtime_draft() {
                                ThreadAccessAuthorization::RuntimeDraft(access)
                            } else {
                                unreachable!(
                                    "central admission supplies persisted thread or runtime draft"
                                )
                            };
                            debug_assert_eq!(authorization.thread_id(), params.thread_id.trim());
                            self.thread_unsubscribe(&context, authorization, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::THREAD_UNSUBSCRIBE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::PROVIDER_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ProviderListParams>(params_value) {
                        Ok(params) => {
                            self.provider_list(&context, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::PROVIDER_LIST
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::PROVIDER_CONFIGURE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ProviderConfigureParams>(params_value) {
                        Ok(params) => {
                            self.provider_configure(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::PROVIDER_CONFIGURE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::CLI_RUNTIME_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeListParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_list(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::CLI_RUNTIME_LIST
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::CLI_RUNTIME_GET => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeGetParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_get(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::CLI_RUNTIME_GET
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::CLI_RUNTIME_STATUS => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeStatusParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_status(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::CLI_RUNTIME_STATUS
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::CLI_RUNTIME_REFRESH => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeRefreshParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_refresh(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::CLI_RUNTIME_REFRESH
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::CLI_RUNTIME_LIST_MODELS => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeListModelsParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_list_models(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::CLI_RUNTIME_LIST_MODELS
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::CLI_RUNTIME_THREAD_BINDING_GET => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeThreadBindingGetParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_thread_binding_get(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::CLI_RUNTIME_THREAD_BINDING_GET
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::CLI_RUNTIME_THREAD_COMPACT => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeThreadCompactParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_thread_compact(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::CLI_RUNTIME_THREAD_COMPACT
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::CLI_RUNTIME_THREAD_FORK => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeThreadForkParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_thread_fork(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::CLI_RUNTIME_THREAD_FORK
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::CLI_RUNTIME_TURN_STEER => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeTurnSteerParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_turn_steer(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::CLI_RUNTIME_TURN_STEER
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::CLI_RUNTIME_REVIEW_START => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeReviewStartParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_review_start(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::CLI_RUNTIME_REVIEW_START
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::CLI_RUNTIME_LOGIN_START => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeLoginStartParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_login_start(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::CLI_RUNTIME_LOGIN_START
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::CLI_RUNTIME_LOGIN_CANCEL => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeLoginCancelParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_login_cancel(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::CLI_RUNTIME_LOGIN_CANCEL
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::CLI_RUNTIME_PROXY_SET => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeProxySetParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_proxy_set(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::CLI_RUNTIME_PROXY_SET
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::CLI_RUNTIME_PROXY_DELETE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeProxyDeleteParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_proxy_delete(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::CLI_RUNTIME_PROXY_DELETE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::CLI_RUNTIME_REQUEST_RESPOND => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeRequestRespondParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_request_respond(
                                &context,
                                admission.turn(),
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::CLI_RUNTIME_REQUEST_RESPOND
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::SETTINGS_GET => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<GatewaySettingsGetParams>(params_value) {
                        Ok(_params) => match self.gateway_settings_snapshot(&context).await {
                            Ok(settings) => {
                                let result =
                                    pioneer_protocol::GatewaySettingsGetResponse { settings };
                                match JsonRpcResponse::from_result(request.id, &result) {
                                    Ok(response) => {
                                        if let Err(error) =
                                            self.send_json(connection_id, &response).await
                                        {
                                            warn!(
                                                error = %format!("{error:#}"),
                                                "failed to send gateway settings get response"
                                            );
                                        }
                                    }
                                    Err(error) => {
                                        self.send_error(
                                            connection_id,
                                            JsonRpcErrorResponse::new(
                                                None,
                                                INVALID_REQUEST_CODE,
                                                format!(
                                                    "failed to encode `{}` response: {error}",
                                                    methods::SETTINGS_GET
                                                ),
                                            ),
                                        )
                                        .await;
                                    }
                                }
                            }
                            Err(error) => {
                                self.send_error(
                                    connection_id,
                                    JsonRpcErrorResponse::new(
                                        Some(request.id),
                                        INVALID_REQUEST_CODE,
                                        format!("failed to load gateway settings: {error:#}"),
                                    ),
                                )
                                .await;
                            }
                        },
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::SETTINGS_GET
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::PROVIDER_MODELS_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ProviderListModelsParams>(params_value) {
                        Ok(params) => {
                            self.provider_list_models(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::PROVIDER_MODELS_LIST
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::PROVIDER_EMBEDDING_MODELS_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ProviderListModelsParams>(params_value) {
                        Ok(params) => {
                            self.provider_list_embedding_models(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::PROVIDER_EMBEDDING_MODELS_LIST
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::PROVIDER_TRANSCRIPTION_MODELS_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ProviderListModelsParams>(params_value) {
                        Ok(params) => {
                            self.provider_list_transcription_models(&context,
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::PROVIDER_TRANSCRIPTION_MODELS_LIST
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::PROVIDER_SET_API_KEY => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ProviderSetApiKeyParams>(params_value) {
                        Ok(params) => {
                            self.provider_set_api_key(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::PROVIDER_SET_API_KEY
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::PROVIDER_DELETE_API_KEY => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ProviderDeleteApiKeyParams>(params_value) {
                        Ok(params) => {
                            self.provider_delete_api_key(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::PROVIDER_DELETE_API_KEY
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MCP_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<McpListParams>(params_value) {
                        Ok(params) => {
                            self.mcp_list(&context, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!("invalid params for `{}`: {error}", methods::MCP_LIST),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MCP_INSTALL => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<McpInstallParams>(params_value) {
                        Ok(params) => {
                            self.mcp_install(&context, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::MCP_INSTALL
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MCP_POLICY_SET => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<McpPolicySetParams>(params_value) {
                        Ok(params) => {
                            self.mcp_policy_set(&context, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::MCP_POLICY_SET
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MCP_SERVER_RESTART => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<McpServerRestartParams>(params_value) {
                        Ok(params) => {
                            self.mcp_server_restart(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::MCP_SERVER_RESTART
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MCP_UNINSTALL => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<McpUninstallParams>(params_value) {
                        Ok(params) => {
                            self.mcp_uninstall(&context, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::MCP_UNINSTALL
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::MCP_SERVER_DETAILS => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<McpServerDetailsParams>(params_value) {
                        Ok(params) => {
                            self.mcp_server_details(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::MCP_SERVER_DETAILS
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::SKILLS_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<SkillListParams>(params_value) {
                        Ok(params) => {
                            self.skills_list(&context, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::SKILLS_LIST
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::SKILLS_INSTALL => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<SkillsInstallParams>(params_value) {
                        Ok(params) => {
                            self.skills_install(&context, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::SKILLS_INSTALL
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::SKILLS_PACK_INSTALL => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<SkillsPackInstallParams>(params_value) {
                        Ok(params) => {
                            self.skills_pack_install(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::SKILLS_PACK_INSTALL
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::SKILLS_PACK_UPDATE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<SkillsPackUpdateParams>(params_value) {
                        Ok(params) => {
                            self.skills_pack_update(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::SKILLS_PACK_UPDATE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::SKILLS_PACK_UNINSTALL => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<SkillsPackUninstallParams>(params_value) {
                        Ok(params) => {
                            self.skills_pack_uninstall(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::SKILLS_PACK_UNINSTALL
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::SKILLS_UPDATE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<SkillsUpdateParams>(params_value) {
                        Ok(params) => {
                            self.skills_update(&context, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::SKILLS_UPDATE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::SKILLS_UNINSTALL => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<SkillsUninstallParams>(params_value) {
                        Ok(params) => {
                            self.skills_uninstall(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::SKILLS_UNINSTALL
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::SKILLS_UPLOAD_START => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<SkillsUploadStartParams>(params_value) {
                        Ok(params) => {
                            self.skills_upload_start(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::SKILLS_UPLOAD_START
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::SKILLS_UPLOAD_FINISH => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<SkillsUploadFinishParams>(params_value) {
                        Ok(params) => {
                            self.skills_upload_finish(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::SKILLS_UPLOAD_FINISH
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::SKILLS_UPLOAD_ABORT => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<SkillsUploadAbortParams>(params_value) {
                        Ok(params) => {
                            self.skills_upload_abort(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::SKILLS_UPLOAD_ABORT
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::ARTIFACT_CAPABILITIES => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactCapabilitiesParams>(params_value) {
                        Ok(params) => {
                            self.artifact_capabilities(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::ARTIFACT_CAPABILITIES
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::ARTIFACT_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactListParams>(params_value) {
                        Ok(params) => {
                            self.artifact_list(
                                &context,
                                ArtifactListAuthorization::Workspace(
                                    admission
                                        .workspace()
                                        .expect("central admission supplies workspace proof"),
                                ),
                                request.id,
                                params,
                                methods::ARTIFACT_LIST,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::ARTIFACT_LIST
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::ARTIFACT_LIST_FOR_THREAD => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactListForThreadParams>(params_value) {
                        Ok(params) => {
                            self.artifact_list(
                                &context,
                                ArtifactListAuthorization::Thread(
                                    admission
                                        .thread()
                                        .expect("central admission supplies thread proof"),
                                ),
                                request.id,
                                params,
                                methods::ARTIFACT_LIST_FOR_THREAD,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::ARTIFACT_LIST_FOR_THREAD
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::ARTIFACT_LIST_FOR_TURN => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactListForTurnParams>(params_value) {
                        Ok(params) => {
                            self.artifact_list(
                                &context,
                                ArtifactListAuthorization::Turn(
                                    admission
                                        .turn()
                                        .expect("central admission supplies turn proof"),
                                ),
                                request.id,
                                params,
                                methods::ARTIFACT_LIST_FOR_TURN,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::ARTIFACT_LIST_FOR_TURN
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::ARTIFACT_LIST_FOR_MESSAGE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactListForMessageParams>(params_value) {
                        Ok(params) => {
                            self.artifact_list(
                                &context,
                                ArtifactListAuthorization::Thread(
                                    admission
                                        .thread()
                                        .expect("central admission supplies thread proof"),
                                ),
                                request.id,
                                params,
                                methods::ARTIFACT_LIST_FOR_MESSAGE,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::ARTIFACT_LIST_FOR_MESSAGE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::ARTIFACT_GET => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactGetParams>(params_value) {
                        Ok(params) => {
                            self.artifact_get(
                                &context,
                                admission
                                    .artifact()
                                    .expect("central admission supplies artifact proof"),
                                request.id,
                                params,
                            )
                            .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::ARTIFACT_GET
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::ARTIFACT_VIEW_GRANT_CREATE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactViewGrantCreateParams>(params_value) {
                        Ok(params) => {
                            self.artifact_view_grant_create(
                                &context,
                                admission
                                    .artifact()
                                    .expect("central admission supplies artifact proof"),
                                request.id,
                                params,
                            )
                            .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::ARTIFACT_VIEW_GRANT_CREATE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::ARTIFACT_DELETE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactDeleteParams>(params_value) {
                        Ok(params) => {
                            self.artifact_delete(
                                &context,
                                admission
                                    .artifact()
                                    .expect("central admission supplies artifact proof"),
                                request.id,
                                params,
                            )
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::ARTIFACT_DELETE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::ARTIFACT_RESTORE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactRestoreParams>(params_value) {
                        Ok(params) => {
                            self.artifact_restore(
                                &context,
                                admission
                                    .artifact()
                                    .expect("central admission supplies artifact proof"),
                                request.id,
                                params,
                            )
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::ARTIFACT_RESTORE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::ARTIFACT_BIND => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactBindParams>(params_value) {
                        Ok(params) => {
                            self.artifact_bind(
                                &context,
                                admission
                                    .artifact()
                                    .expect("central admission supplies artifact proof"),
                                request.id,
                                params,
                            )
                            .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::ARTIFACT_BIND
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::ARTIFACT_UPLOAD_START => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactUploadStartParams>(params_value) {
                        Ok(params) => {
                            let authorization = match (
                                admission.workspace(),
                                admission.thread(),
                                admission.runtime_draft(),
                            ) {
                                (Some(proof), None, None) => {
                                    ArtifactUploadAuthorization::Workspace(proof)
                                }
                                (None, Some(proof), None) => {
                                    ArtifactUploadAuthorization::Thread(proof)
                                }
                                (None, None, Some(access)) => {
                                    ArtifactUploadAuthorization::RuntimeDraft(access)
                                }
                                _ => unreachable!(
                                    "central admission supplies one upload target proof"
                                ),
                            };
                            self.artifact_upload_start(
                                &context,
                                authorization,
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::ARTIFACT_UPLOAD_START
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::ARTIFACT_UPLOAD_FINISH => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactUploadFinishParams>(params_value) {
                        Ok(params) => {
                            let authorization = match (
                                admission.workspace(),
                                admission.thread(),
                                admission.runtime_draft(),
                            ) {
                                (Some(proof), None, None) => {
                                    ArtifactUploadAuthorization::Workspace(proof)
                                }
                                (None, Some(proof), None) => {
                                    ArtifactUploadAuthorization::Thread(proof)
                                }
                                (None, None, Some(access)) => {
                                    ArtifactUploadAuthorization::RuntimeDraft(access)
                                }
                                _ => unreachable!(
                                    "central admission supplies one upload target proof"
                                ),
                            };
                            self.artifact_upload_finish(
                                &context,
                                authorization,
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::ARTIFACT_UPLOAD_FINISH
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::ARTIFACT_UPLOAD_ABORT => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactUploadAbortParams>(params_value) {
                        Ok(params) => {
                            let authorization = match (
                                admission.workspace(),
                                admission.thread(),
                                admission.runtime_draft(),
                            ) {
                                (Some(proof), None, None) => {
                                    ArtifactUploadAuthorization::Workspace(proof)
                                }
                                (None, Some(proof), None) => {
                                    ArtifactUploadAuthorization::Thread(proof)
                                }
                                (None, None, Some(access)) => {
                                    ArtifactUploadAuthorization::RuntimeDraft(access)
                                }
                                _ => unreachable!(
                                    "central admission supplies one upload target proof"
                                ),
                            };
                            self.artifact_upload_abort(
                                &context,
                                authorization,
                                request.id,
                                params,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::ARTIFACT_UPLOAD_ABORT
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::SKILLS_HEALTH => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<SkillsHealthParams>(params_value) {
                        Ok(params) => {
                            self.skills_health(&context, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::SKILLS_HEALTH
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::SKILLS_POLICY_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<SkillsPolicyListParams>(params_value) {
                        Ok(params) => {
                            self.skills_policy_list(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::SKILLS_POLICY_LIST
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::SKILLS_POLICY_SET => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<SkillsPolicySetParams>(params_value) {
                        Ok(params) => {
                            self.skills_policy_set(&context, request.id, params)
                                .await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::SKILLS_POLICY_SET
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TASK_CREATE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TaskCreateParams>(params_value) {
                        Ok(params) => {
                            self.task_create(&context, admission.thread(), request.id, params)
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TASK_CREATE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TASK_GET => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TaskGetParams>(params_value) {
                        Ok(params) => {
                            self.task_get(&context, admission.task(), request.id, params)
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!("invalid params for `{}`: {error}", methods::TASK_GET),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TASK_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TaskListParams>(params_value) {
                        Ok(params) => self.task_list(&context, request.id, params).await,
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!("invalid params for `{}`: {error}", methods::TASK_LIST),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TASK_TREE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TaskTreeTaskParams>(params_value) {
                        Ok(params) => {
                            self.task_tree(&context, admission.task(), request.id, params)
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!("invalid params for `{}`: {error}", methods::TASK_TREE),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TASK_EVENTS => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TaskEventsParams>(params_value) {
                        Ok(params) => {
                            self.task_events(&context, admission.task(), request.id, params)
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TASK_EVENTS
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TASK_WAIT => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TaskWaitParams>(params_value) {
                        Ok(params) => {
                            self.task_wait(
                                &context,
                                admission.task_batch(),
                                request.id,
                                params,
                            )
                            .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!("invalid params for `{}`: {error}", methods::TASK_WAIT),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TASK_ACCEPT => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TaskAcceptParams>(params_value) {
                        Ok(params) => {
                            self.task_accept(&context, admission.task(), request.id, params)
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TASK_ACCEPT
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TASK_REVISE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TaskReviseParams>(params_value) {
                        Ok(params) => {
                            message_future(self.task_revise(
                                &context,
                                admission.task(),
                                request.id,
                                params,
                            ))
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TASK_REVISE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TASK_CANCEL => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TaskCancelParams>(params_value) {
                        Ok(params) => {
                            self.task_cancel(&context, admission.task(), request.id, params)
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TASK_CANCEL
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TASK_RESCHEDULE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TaskRescheduleParams>(params_value) {
                        Ok(params) => {
                            self.task_reschedule(
                                &context,
                                admission.task(),
                                request.id,
                                params,
                            )
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TASK_RESCHEDULE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TASK_PAUSE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TaskPauseParams>(params_value) {
                        Ok(params) => {
                            self.task_pause(&context, admission.task(), request.id, params)
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TASK_PAUSE
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TASK_RESUME => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TaskResumeParams>(params_value) {
                        Ok(params) => {
                            self.task_resume(&context, admission.task(), request.id, params)
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TASK_RESUME
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TASK_AGENDA => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TaskAgendaParams>(params_value) {
                        Ok(params) => self.task_agenda(&context, request.id, params).await,
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TASK_AGENDA
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TASK_DELIVERIES => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TaskDeliveriesParams>(params_value) {
                        Ok(params) => {
                            self.task_deliveries(&context, request.id, params)
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TASK_DELIVERIES
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::TASK_DETACH => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TaskDetachParams>(params_value) {
                        Ok(params) => {
                            self.task_detach(&context, admission.task(), request.id, params)
                                .await
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TASK_DETACH
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                _ => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request.id),
                            METHOD_NOT_FOUND_CODE,
                            format!("method `{}` is not supported", request.method),
                        ),
                    )
                    .await;
                }
        }
    }

    async fn dispatch_memory_candidates_list(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: Option<JsonValue>,
    ) {
        let connection_id = request_context.connection_id();
        let params_value = params.unwrap_or_else(empty_object_value);
        match serde_json::from_value::<MemoryCandidatesListParams>(params_value) {
            Ok(params) => {
                self.memory_candidates_list(request_context, request_id, params)
                    .await;
            }
            Err(error) => {
                self.send_invalid_params_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_LIST,
                    error,
                )
                .await;
            }
        }
    }

    async fn dispatch_memory_candidates_get(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: Option<JsonValue>,
    ) {
        let connection_id = request_context.connection_id();
        let params_value = params.unwrap_or_else(empty_object_value);
        match serde_json::from_value::<MemoryCandidatesGetParams>(params_value) {
            Ok(params) => {
                self.memory_candidates_get(request_context, request_id, params)
                    .await;
            }
            Err(error) => {
                self.send_invalid_params_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_GET,
                    error,
                )
                .await;
            }
        }
    }

    async fn dispatch_memory_candidates_decide(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: Option<JsonValue>,
    ) {
        let connection_id = request_context.connection_id();
        let params_value = params.unwrap_or_else(empty_object_value);
        match serde_json::from_value::<MemoryCandidatesDecideParams>(params_value) {
            Ok(params) => {
                self.memory_candidates_decide(request_context, request_id, params)
                    .await;
            }
            Err(error) => {
                self.send_invalid_params_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_DECIDE,
                    error,
                )
                .await;
            }
        }
    }

    async fn dispatch_memory_candidates_approve(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: Option<JsonValue>,
    ) {
        let connection_id = request_context.connection_id();
        let params_value = params.unwrap_or_else(empty_object_value);
        match serde_json::from_value::<MemoryCandidatesApproveParams>(params_value) {
            Ok(params) => {
                self.memory_candidates_approve(request_context, request_id, params)
                    .await;
            }
            Err(error) => {
                self.send_invalid_params_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_APPROVE,
                    error,
                )
                .await;
            }
        }
    }

    async fn dispatch_memory_candidates_reject(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: Option<JsonValue>,
    ) {
        let connection_id = request_context.connection_id();
        let params_value = params.unwrap_or_else(empty_object_value);
        match serde_json::from_value::<MemoryCandidatesRejectParams>(params_value) {
            Ok(params) => {
                self.memory_candidates_reject(request_context, request_id, params)
                    .await;
            }
            Err(error) => {
                self.send_invalid_params_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_REJECT,
                    error,
                )
                .await;
            }
        }
    }

    async fn dispatch_memory_candidates_edit_and_approve(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: Option<JsonValue>,
    ) {
        let connection_id = request_context.connection_id();
        let params_value = params.unwrap_or_else(empty_object_value);
        match serde_json::from_value::<MemoryCandidatesEditAndApproveParams>(params_value) {
            Ok(params) => {
                self.memory_candidates_edit_and_approve(request_context, request_id, params)
                    .await;
            }
            Err(error) => {
                self.send_invalid_params_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_EDIT_AND_APPROVE,
                    error,
                )
                .await;
            }
        }
    }

    async fn dispatch_memory_candidates_merge(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: Option<JsonValue>,
    ) {
        let connection_id = request_context.connection_id();
        let params_value = params.unwrap_or_else(empty_object_value);
        match serde_json::from_value::<MemoryCandidatesMergeParams>(params_value) {
            Ok(params) => {
                self.memory_candidates_merge(request_context, request_id, params)
                    .await;
            }
            Err(error) => {
                self.send_invalid_params_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_MERGE,
                    error,
                )
                .await;
            }
        }
    }

    async fn dispatch_memory_candidates_suppress_similar(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: Option<JsonValue>,
    ) {
        let connection_id = request_context.connection_id();
        let params_value = params.unwrap_or_else(empty_object_value);
        match serde_json::from_value::<MemoryCandidatesSuppressSimilarParams>(params_value) {
            Ok(params) => {
                self.memory_candidates_suppress_similar(request_context, request_id, params)
                    .await;
            }
            Err(error) => {
                self.send_invalid_params_error(
                    connection_id,
                    request_id,
                    methods::MEMORY_CANDIDATES_SUPPRESS_SIMILAR,
                    error,
                )
                .await;
            }
        }
    }

    async fn gateway_settings_snapshot(
        &self,
        request_context: &RequestContext,
    ) -> anyhow::Result<pioneer_protocol::GatewaySettingsSnapshot> {
        let connection_id = request_context.connection_id();
        let config =
            pioneer_config::AppConfig::load().context("failed to load app config for settings")?;
        let settings_file_name = crate::settings::normalize_settings_file_name(
            config.gateway.settings_file_name.as_str(),
        )?;
        let settings_path = self.artifact_runtime_home.join(settings_file_name.as_str());
        let settings = crate::settings::load_or_create_gateway_settings(
            settings_path.as_path(),
            config.gateway.settings_version,
            settings_file_name.as_str(),
        )?;
        let workspace_id = self
            .session_manager
            .connection_workspace_id(connection_id)
            .await;
        self.gateway_settings_snapshot_from_settings(
            &config.gateway,
            &settings,
            workspace_id.as_deref(),
        )
        .await
    }

    async fn update_gateway_settings(
        &self,
        request_context: &RequestContext,
        update: pioneer_protocol::GatewaySettingsUpdate,
    ) -> anyhow::Result<pioneer_protocol::GatewaySettingsSnapshot> {
        let connection_id = request_context.connection_id();
        let _settings_guard = self.gateway_settings_update_lock.lock().await;
        if update.self_improvement.is_some() && self.self_improvement_supervisor.is_none() {
            anyhow::bail!(
                "self-improvement settings cannot be updated without the runtime supervisor"
            );
        }
        let config =
            pioneer_config::AppConfig::load().context("failed to load app config for settings")?;
        let settings_file_name = crate::settings::normalize_settings_file_name(
            config.gateway.settings_file_name.as_str(),
        )?;
        let settings_path = self.artifact_runtime_home.join(settings_file_name.as_str());
        let mut settings = crate::settings::load_or_create_gateway_settings(
            settings_path.as_path(),
            config.gateway.settings_version,
            settings_file_name.as_str(),
        )?;
        let voice_reconfiguration = match update.voice_input.as_ref() {
            Some(voice_input) => settings.voice_input_update_would_reconfigure(voice_input)?,
            None => false,
        };
        if voice_reconfiguration
            && self
                .voice_sessions
                .has_active_sessions()
                .context("failed to inspect active voice sessions")?
        {
            return Err(VoiceReconfigurationBusyError.into());
        }
        let workspace_id = self
            .session_manager
            .connection_workspace_id(connection_id)
            .await;
        if let Some(self_improvement) = update.self_improvement.as_ref() {
            let workspace_id = workspace_id
                .as_deref()
                .context("workspace context is required to update self-improvement settings")?;
            let desired =
                crate::settings::self_improvement_settings_from_protocol(self_improvement.clone())?;
            crate::self_improvement::settings::validate_authoritative_selections_for_workspace(
                &desired,
                self.provider_registry.as_ref(),
                Some(workspace_id),
            )?;
        }
        if update
            .thread_episodic
            .as_ref()
            .and_then(|thread_episodic| thread_episodic.vector_search.as_ref())
            .is_some()
            && workspace_id.is_none()
        {
            anyhow::bail!(
                "workspace context is required to update thread episodic vector search settings"
            );
        }

        let previous_general_settings = settings.effective_general_settings(&config.gateway);
        let previous_workspace_vector_search = workspace_id.as_deref().map(|workspace_id| {
            settings
                .effective_thread_episodic_settings_for_workspace(
                    &config.gateway.thread_episodic,
                    Some(workspace_id),
                )
                .vector_search
        });
        let changes =
            settings.apply_protocol_update_for_workspace(update, workspace_id.as_deref())?;
        let disabled_local_embedding_model = previous_workspace_vector_search
            .as_ref()
            .zip(workspace_id.as_deref())
            .and_then(|(previous, workspace_id)| {
                let current = settings
                    .effective_thread_episodic_settings_for_workspace(
                        &config.gateway.thread_episodic,
                        Some(workspace_id),
                    )
                    .vector_search;
                crate::thread_episodic_embedding::local_embedding_model_disabled_by_transition(
                    previous, &current,
                )
                .map(|model| (workspace_id.to_owned(), model))
            });
        if changes.remote_access.changed {
            if let Some(key) = changes.remote_access.key.as_deref() {
                self.gateway_secrets.put_remote_access_secret(
                    changes.remote_access.secret_ref.as_str(),
                    key,
                    Some("remote access".to_owned()),
                )?;
            } else if changes.remote_access.clear_key {
                self.gateway_secrets
                    .delete_remote_access_secret(changes.remote_access.secret_ref.as_str())?;
            }
        }
        if let Some(keepawake) = changes.general.keepawake {
            self.apply_keepawake_setting(keepawake)
                .context("failed to apply keepawake setting")?;
        }
        if let Err(error) =
            crate::settings::save_gateway_settings(settings_path.as_path(), &settings)
        {
            if changes.general.keepawake.is_some() {
                if let Err(rollback_error) =
                    self.apply_keepawake_setting(previous_general_settings.keepawake)
                {
                    warn!(
                        error = %format!("{rollback_error:#}"),
                        "failed to roll back keepawake setting after settings save failure"
                    );
                }
            }
            return Err(error);
        }

        if changes.self_improvement {
            if let Some(supervisor) = self.self_improvement_supervisor.as_ref() {
                let workspace_id = changes
                    .self_improvement_workspace_id
                    .as_deref()
                    .context("Self-improvement update lost its workspace scope")?;
                Box::pin(supervisor.apply_desired_for_workspace(
                    workspace_id,
                    settings.effective_self_improvement_settings_for_workspace(Some(workspace_id)),
                    chrono::Utc::now().timestamp(),
                ))
                .await
                .context("failed to apply Self-improvement settings")?;
            }
        }

        if changes.remote_access.changed {
            if let Some(supervisor) = self.remote_access_supervisor.as_ref() {
                supervisor
                    .apply(crate::remote_access_desired_state(
                        &config.gateway.remote_access,
                        &settings,
                        self.gateway_secrets.as_ref(),
                    )?)
                    .await
                    .context("failed to apply remote access settings")?;
            }
        }

        if changes.voice_input.changed || changes.voice_input.retry_install {
            if let Some(supervisor) = self.voice_input_supervisor.as_ref() {
                let desired = crate::voice::supervisor::VoiceInputDesiredState::from_config(
                    &settings.effective_voice_input_config(&config.gateway.voice),
                );
                let applied = supervisor
                    .apply_desired(desired, changes.voice_input.retry_install)
                    .context("failed to apply Voice Input settings")?;
                if applied.cleanup_disabled_models {
                    supervisor
                        .cleanup_disabled_models()
                        .await
                        .context("failed to remove disabled Voice Input model installations")?;
                }
                if let Some(reconcile) = applied.reconcile {
                    let worker = supervisor.clone();
                    tokio::spawn(async move {
                        worker.reconcile(reconcile).await;
                    });
                }
            }
        }

        let snapshot = self
            .gateway_settings_snapshot_from_settings(
                &config.gateway,
                &settings,
                workspace_id.as_deref(),
            )
            .await?;
        let mut snapshot = snapshot;
        let mut vector_refill_started = false;
        if changes.memory {
            let memory_settings =
                crate::settings::GatewayMemorySettings::from_protocol(snapshot.memory.clone());
            let loop_config =
                crate::memory_loop_config_from_gateway_memory_settings(&memory_settings);
            self.apply_memory_loop_config(loop_config);
            self.reinstall_memory_hook_runtime_if_bound().await;
        }
        if changes.thread_episodic {
            let thread_episodic_settings =
                settings.effective_thread_episodic_settings(&config.gateway.thread_episodic);
            let runtime_config = crate::thread_episodic_runtime_config_from_gateway_settings(
                &thread_episodic_settings,
            );
            self.apply_thread_episodic_runtime_config(runtime_config)
                .await;
            let workspace_vector_search_configs =
                settings.workspace_thread_episodic_vector_search_configs();
            self.apply_thread_episodic_workspace_vector_search_configs(
                workspace_vector_search_configs.clone(),
            );
            if let Some((disabled_workspace_id, model)) = disabled_local_embedding_model.as_ref() {
                self.thread_episodic_workspace_refill_supervisor
                    .cancel_and_wait(disabled_workspace_id.as_str())
                    .await;
                let model_still_in_use =
                    crate::thread_episodic_embedding::configs_use_enabled_local_embedding_model(
                        std::iter::once(&thread_episodic_settings.vector_search)
                            .chain(workspace_vector_search_configs.values()),
                        model.as_str(),
                    );
                if !model_still_in_use {
                    crate::thread_episodic_embedding::remove_local_embedding_model_install(
                        self.artifact_runtime_home.as_path(),
                        model.as_str(),
                    )
                    .await
                    .map_err(anyhow::Error::msg)
                    .with_context(|| {
                        format!("failed to remove disabled local embedding model `{model}`")
                    })?;
                }
            }
            let vector_refill_startable =
                crate::settings::thread_episodic_vector_refill_is_startable(
                    &snapshot.thread_episodic.vector_search,
                );
            if changes.thread_episodic_vector_projection_changed
                && thread_episodic_settings.enabled
                && vector_refill_startable
            {
                if let Some(workspace_id) = changes
                    .thread_episodic_vector_projection_workspace_id
                    .as_deref()
                    .or(workspace_id.as_deref())
                {
                    let workspace_thread_episodic_settings = settings
                        .effective_thread_episodic_settings_for_workspace(
                            &config.gateway.thread_episodic,
                            Some(workspace_id),
                        );
                    crate::database::startup::spawn_thread_episodic_workspace_capsule_refill_for_workspace(
                        self.crud_store.clone(),
                        self.thread_episodic_storage_root.clone(),
                        workspace_id.to_owned(),
                        workspace_thread_episodic_settings.vector_search,
                        thread_episodic_settings.vector_search.clone(),
                        workspace_vector_search_configs,
                        self.provider_registry.clone(),
                        self.artifact_runtime_home.clone(),
                        Some(self.thread_episodic_vector_refill_status_sender()),
                        self.thread_episodic_workspace_refill_supervisor.clone(),
                    )
                    .await;
                    vector_refill_started = true;
                } else {
                    crate::database::startup::spawn_thread_episodic_workspace_capsule_refill(
                        self.crud_store.clone(),
                        self.thread_episodic_storage_root.clone(),
                        thread_episodic_settings.vector_search.clone(),
                        workspace_vector_search_configs,
                        self.provider_registry.clone(),
                        self.artifact_runtime_home.clone(),
                        Some(self.thread_episodic_vector_refill_status_sender()),
                        self.thread_episodic_workspace_refill_supervisor.clone(),
                    );
                    vector_refill_started = true;
                }
            }
            self.reinstall_memory_hook_runtime_if_bound().await;
        }
        if vector_refill_started {
            crate::settings::mark_thread_episodic_vector_refill_running_if_ready(
                &mut snapshot.thread_episodic.vector_search,
            );
        }

        Ok(snapshot)
    }

    async fn gateway_settings_snapshot_from_settings(
        &self,
        config: &pioneer_config::GatewayConfig,
        settings: &crate::settings::GatewaySettings,
        workspace_id: Option<&str>,
    ) -> anyhow::Result<pioneer_protocol::GatewaySettingsSnapshot> {
        let has_remote_access_key = match settings.remote_access_secret_ref() {
            Some(secret_ref) => self.gateway_secrets.has_remote_access_secret(secret_ref)?,
            None => false,
        };
        let remote_access_status = self
            .remote_access_supervisor
            .as_ref()
            .map(|supervisor| supervisor.status_snapshot())
            .unwrap_or_default();
        let mut snapshot = settings.snapshot_with_remote_access_status_for_workspace(
            config,
            workspace_id,
            has_remote_access_key,
            remote_access_status,
        );
        let desired = settings.effective_self_improvement_settings_for_workspace(workspace_id);
        let authoritative =
            crate::self_improvement::settings::resolve_authoritative_settings_for_workspace(
                &desired,
                self.provider_registry.as_ref(),
                workspace_id,
            );
        snapshot.self_improvement =
            crate::settings::self_improvement_settings_to_protocol(&authoritative.desired_config());
        self.apply_vector_provider_key_presence(&mut snapshot, workspace_id)?;
        crate::settings::apply_thread_episodic_vector_search_status(
            &mut snapshot.thread_episodic.vector_search,
        );
        snapshot.voice_input.runtime = self
            .voice_input_supervisor
            .as_ref()
            .map(|supervisor| supervisor.runtime_snapshot())
            .unwrap_or_default();
        self.apply_vector_local_model_status(&mut snapshot);
        self.apply_vector_refill_projection_status(&mut snapshot, workspace_id)
            .await?;
        Ok(snapshot)
    }

    async fn apply_vector_refill_projection_status(
        &self,
        snapshot: &mut pioneer_protocol::GatewaySettingsSnapshot,
        workspace_id: Option<&str>,
    ) -> anyhow::Result<()> {
        let thread_episodic_settings =
            crate::settings::GatewayThreadEpisodicSettings::from_protocol(
                snapshot.thread_episodic.clone(),
            );
        let Some(workspace_id) = workspace_id else {
            snapshot.thread_episodic.vector_search.refill_status =
                pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Unknown;
            return Ok(());
        };
        let projection_target =
            crate::database::startup::thread_episodic_workspace_capsule_refill::ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
                &thread_episodic_settings.vector_search,
            );
        let marker_status =
            crate::database::startup::thread_episodic_workspace_capsule_refill::refill_status_for_workspace_target(
                self.crud_store.as_ref(),
                workspace_id,
                &projection_target,
            )
            .await?;
        snapshot.thread_episodic.vector_search.refill_status =
            if thread_episodic_settings.vector_search.enabled {
                marker_status
            } else if matches!(
                marker_status,
                pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Complete
            ) {
                pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Disabled
            } else {
                marker_status
            };
        Ok(())
    }

    pub(crate) fn apply_vector_provider_key_presence(
        &self,
        snapshot: &mut pioneer_protocol::GatewaySettingsSnapshot,
        workspace_id: Option<&str>,
    ) -> anyhow::Result<()> {
        let vector_search = &mut snapshot.thread_episodic.vector_search;
        vector_search.provider_key.present = false;

        if !vector_search.provider_key.required {
            return Ok(());
        }
        let Some(workspace_id) = workspace_id else {
            return Ok(());
        };
        let Some(provider) = vector_provider_key_name(vector_search.provider) else {
            return Ok(());
        };

        vector_search.provider_key.present = self
            .gateway_secrets
            .get_workspace_provider_api_key(workspace_id, provider)?
            .is_some();
        Ok(())
    }

    fn apply_vector_local_model_status(
        &self,
        snapshot: &mut pioneer_protocol::GatewaySettingsSnapshot,
    ) {
        let vector_search = &mut snapshot.thread_episodic.vector_search;
        vector_search.local_model_status =
            crate::thread_episodic_embedding::local_embedding_model_status(
                self.artifact_runtime_home.as_path(),
                vector_search.enabled,
                vector_search.provider,
                vector_search
                    .model
                    .as_deref()
                    .or(vector_search.local_model.as_deref())
                    .unwrap_or(""),
            );
    }

    async fn send_invalid_params_error(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        method: &'static str,
        error: serde_json::Error,
    ) {
        self.send_error(
            connection_id,
            JsonRpcErrorResponse::new(
                Some(request_id),
                INVALID_PARAMS_CODE,
                format!("invalid params for `{method}`: {error}"),
            ),
        )
        .await;
    }

    #[cfg(test)]
    pub(crate) async fn process_request_for_connection(
        &self,
        connection_id: ConnectionId,
        payload: &str,
    ) {
        let context = self
            .session_manager
            .connection_context(connection_id)
            .await
            .expect("test connection must be registered with a principal");
        self.process_request(&context, payload).await;
    }

    pub async fn connection_closed(&self, connection_id: ConnectionId) {
        self.artifact_uploads.abort_connection(connection_id).await;
        self.skill_upload_owners
            .lock()
            .await
            .retain(|_, owner| owner.connection_id != connection_id);
        let removed_voice_sessions = self.voice_sessions.cleanup_connection(connection_id);
        for session in &removed_voice_sessions {
            let _ = self
                .voice_session_buffers
                .remove_session(session.session_id.as_str());
        }
        if !removed_voice_sessions.is_empty() {
            debug!(
                connection_id,
                session_ids = ?removed_voice_sessions
                    .iter()
                    .map(|session| session.session_id.as_str())
                    .collect::<Vec<_>>(),
                "removed active voice sessions after connection closed"
            );
        }

        let removed_thread_ids = self.thread_manager.connection_closed(connection_id).await;
        if removed_thread_ids.is_empty() {
            return;
        }

        for thread_id in &removed_thread_ids {
            self.teardown_agent_thread(thread_id).await;
        }

        debug!(
            connection_id,
            removed_thread_ids = ?removed_thread_ids,
            "unloaded idle thread runtime state after connection closed"
        );
    }
}

fn instrument_message_future<'a>(
    future: MessageFuture<'a, ()>,
    span: tracing::Span,
) -> MessageFuture<'a, ()> {
    message_future(future.instrument(span))
}

const VOICE_RECONFIGURATION_BUSY_CODE: &str = "voice_reconfiguration_busy";

#[derive(Debug)]
struct VoiceReconfigurationBusyError;

impl std::fmt::Display for VoiceReconfigurationBusyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("voice input cannot be reconfigured while a voice session is active")
    }
}

impl std::error::Error for VoiceReconfigurationBusyError {}

fn voice_reconfiguration_busy_error(
    request_id: Option<pioneer_protocol::RequestId>,
) -> JsonRpcErrorResponse {
    JsonRpcErrorResponse {
        jsonrpc: JSONRPC_VERSION.to_owned(),
        id: request_id,
        error: pioneer_protocol::JsonRpcError {
            code: INVALID_REQUEST_CODE,
            message: format!(
                "{VOICE_RECONFIGURATION_BUSY_CODE}: voice input cannot be reconfigured while a voice session is active"
            ),
            data: Some(serde_json::json!({
                "code": VOICE_RECONFIGURATION_BUSY_CODE,
                "details": {},
            })),
        },
    }
}
