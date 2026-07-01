use super::*;
use pioneer_protocol::{
    ArtifactBindParams, ArtifactCapabilitiesParams, ArtifactDeleteParams,
    ArtifactDownloadAbortParams, ArtifactDownloadChunkParams, ArtifactDownloadFinishParams,
    ArtifactDownloadStartParams, ArtifactGetParams, ArtifactListForMessageParams,
    ArtifactListForThreadParams, ArtifactListForTurnParams, ArtifactListParams, ArtifactReadParams,
    ArtifactRestoreParams, ArtifactUploadAbortParams, ArtifactUploadFinishParams,
    ArtifactUploadStartParams, CLIRuntimeGetParams, CLIRuntimeListParams, CLIRuntimeRefreshParams,
    CLIRuntimeStatusParams, CLIRuntimeThreadBindingGetParams, CLIRuntimeThreadCompactParams,
    CLIRuntimeThreadForkParams, CLIRuntimeTurnSteerParams, GatewaySettingsGetParams,
    GatewaySettingsUpdateParams, McpInstallParams, McpListParams, McpPolicySetParams,
    MemoryCandidatesApproveParams, MemoryCandidatesDecideParams,
    MemoryCandidatesEditAndApproveParams, MemoryCandidatesGetParams, MemoryCandidatesListParams,
    MemoryCandidatesMergeParams, MemoryCandidatesRejectParams,
    MemoryCandidatesSuppressSimilarParams, MemoryForgetParams, MemoryGetParams, MemoryListParams,
    MemoryRememberParams, MemorySearchParams, SkillListParams, SkillsHealthParams,
    SkillsInstallParams, SkillsPolicyListParams, SkillsPolicySetParams, SkillsUninstallParams,
    SkillsUpdateParams, TaskAcceptParams, TaskAgendaParams, TaskCancelParams, TaskCreateParams,
    TaskDeliveriesParams, TaskDetachParams, TaskEventsParams, TaskGetParams, TaskListParams,
    TaskPauseParams, TaskRescheduleParams, TaskResumeParams, TaskReviseParams,
    TaskTreeParams as TaskTreeTaskParams, TaskWaitParams, ThreadAgentsDocArchiveParams,
    ThreadAgentsDocGetParams, ThreadAgentsDocResolveForThreadParams, ThreadAgentsDocSaveParams,
    ThreadTimelinePageParams, TurnCancelParams, TurnPermissionRequestRespondParams,
    TurnResumeParams, TurnWorkPageParams, VoiceSessionCancelParams, VoiceSessionFinalizeParams,
    VoiceSessionStartParams, VoiceStatusParams,
};

impl MessageProcessor {
    pub fn process_request<'a>(
        &'a self,
        connection_id: ConnectionId,
        payload: &'a str,
    ) -> MessageFuture<'a, ()> {
        self.process_request_inner(connection_id, payload)
    }

    fn process_request_inner<'a>(
        &'a self,
        connection_id: ConnectionId,
        payload: &'a str,
    ) -> MessageFuture<'a, ()> {
        message_future(async move {
            let request_value = match serde_json::from_str::<JsonValue>(payload) {
                Ok(value) => value,
                Err(_) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            None,
                            PARSE_ERROR_CODE,
                            "failed to parse JSON-RPC payload",
                        ),
                    )
                    .await;
                    return;
                }
            };

            let request_id = parse_request_id(&request_value);

            let request = match serde_json::from_value::<JsonRpcRequest>(request_value) {
                Ok(request) => request,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            request_id,
                            INVALID_REQUEST_CODE,
                            format!("invalid JSON-RPC request: {error}"),
                        ),
                    )
                    .await;
                    return;
                }
            };

            if request.jsonrpc != JSONRPC_VERSION {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request.id),
                        INVALID_REQUEST_CODE,
                        format!("unsupported jsonrpc version `{}`", request.jsonrpc),
                    ),
                )
                .await;
                return;
            }

            match request.method.as_str() {
                methods::WORKSPACE_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<WorkspaceListParams>(params_value) {
                        Ok(params) => {
                            self.workspace_list(connection_id, request.id, params).await;
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
                            self.workspace_create(connection_id, request.id, params)
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
                            self.workspace_default(connection_id, request.id, params)
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
                            self.workspace_select(connection_id, request.id, params)
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
                            self.workspace_update(connection_id, request.id, params)
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
                        Ok(params) => self.voice_status(connection_id, request.id, params).await,
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
                            self.voice_session_start(connection_id, request.id, params)
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
                            self.voice_session_finalize(connection_id, request.id, params)
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
                            self.voice_session_cancel(connection_id, request.id, params)
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
                        Ok(params) => self.memory_search(connection_id, request.id, params).await,
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
                        Ok(params) => self.memory_get(connection_id, request.id, params).await,
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
                        Ok(params) => self.memory_list(connection_id, request.id, params).await,
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
                            self.memory_remember(connection_id, request.id, params)
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
                        Ok(params) => self.memory_forget(connection_id, request.id, params).await,
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
                    self.dispatch_memory_candidates_list(connection_id, request.id, request.params)
                        .await;
                }
                methods::MEMORY_CANDIDATES_GET => {
                    self.dispatch_memory_candidates_get(connection_id, request.id, request.params)
                        .await;
                }
                methods::MEMORY_CANDIDATES_DECIDE => {
                    self.dispatch_memory_candidates_decide(
                        connection_id,
                        request.id,
                        request.params,
                    )
                    .await;
                }
                methods::MEMORY_CANDIDATES_APPROVE => {
                    self.dispatch_memory_candidates_approve(
                        connection_id,
                        request.id,
                        request.params,
                    )
                    .await;
                }
                methods::MEMORY_CANDIDATES_REJECT => {
                    self.dispatch_memory_candidates_reject(
                        connection_id,
                        request.id,
                        request.params,
                    )
                    .await;
                }
                methods::MEMORY_CANDIDATES_EDIT_AND_APPROVE => {
                    self.dispatch_memory_candidates_edit_and_approve(
                        connection_id,
                        request.id,
                        request.params,
                    )
                    .await;
                }
                methods::MEMORY_CANDIDATES_MERGE => {
                    self.dispatch_memory_candidates_merge(
                        connection_id,
                        request.id,
                        request.params,
                    )
                    .await;
                }
                methods::MEMORY_CANDIDATES_SUPPRESS_SIMILAR => {
                    self.dispatch_memory_candidates_suppress_similar(
                        connection_id,
                        request.id,
                        request.params,
                    )
                    .await;
                }
                methods::THREAD_START => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ThreadStartParams>(params_value) {
                        Ok(params) => {
                            self.thread_start(connection_id, request.id, params).await;
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
                            self.thread_tree(connection_id, request.id, params).await;
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
                    match serde_json::from_value::<ThreadUpdateParams>(params_value) {
                        Ok(params) => {
                            self.thread_update(connection_id, request.id, params).await;
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
                methods::THREAD_MOVE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ThreadMoveParams>(params_value) {
                        Ok(params) => {
                            self.thread_move(connection_id, request.id, params).await;
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
                            self.thread_folder_create(connection_id, request.id, params)
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
                            self.thread_folder_move(connection_id, request.id, params)
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
                            self.thread_folder_delete(connection_id, request.id, params)
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
                            self.thread_agents_doc_get(connection_id, request.id, params)
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
                            self.thread_agents_doc_save(connection_id, request.id, params)
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
                            self.thread_agents_doc_archive(connection_id, request.id, params)
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
                            self.thread_agents_doc_resolve_for_thread(
                                connection_id,
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
                            self.thread_get(connection_id, request.id, params).await;
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
                            self.thread_timeline_page(connection_id, request.id, params)
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
                methods::TURN_WORK_PAGE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TurnWorkPageParams>(params_value) {
                        Ok(params) => {
                            self.turn_work_page(connection_id, request.id, params).await;
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
                methods::TURN_START => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TurnStartParams>(params_value) {
                        Ok(params) => {
                            self.turn_start(connection_id, request.id, params).await;
                        }
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::TURN_START
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
                            self.turn_cancel(connection_id, request.id, params).await;
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
                methods::TURN_RESUME => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<TurnResumeParams>(params_value) {
                        Ok(params) => {
                            self.turn_resume(connection_id, request.id, params).await;
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
                            self.turn_get(connection_id, request.id, params).await;
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
                            self.turn_items(connection_id, request.id, params).await;
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
                            self.turn_permission_request_respond(connection_id, request.id, params)
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
                            self.thread_unsubscribe(connection_id, request.id, params)
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
                            self.provider_list(connection_id, request.id, params).await;
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
                methods::CLI_RUNTIME_LIST => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeListParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_list(connection_id, request.id, params)
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
                            self.cli_runtime_get(connection_id, request.id, params)
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
                            self.cli_runtime_status(connection_id, request.id, params)
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
                            self.cli_runtime_refresh(connection_id, request.id, params)
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
                            self.cli_runtime_list_models(connection_id, request.id, params)
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
                            self.cli_runtime_thread_binding_get(connection_id, request.id, params)
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
                            self.cli_runtime_thread_compact(connection_id, request.id, params)
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
                            self.cli_runtime_thread_fork(connection_id, request.id, params)
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
                            self.cli_runtime_turn_steer(connection_id, request.id, params)
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
                            self.cli_runtime_review_start(connection_id, request.id, params)
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
                            self.cli_runtime_login_start(connection_id, request.id, params)
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
                            self.cli_runtime_login_cancel(connection_id, request.id, params)
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
                methods::CLI_RUNTIME_REQUEST_RESPOND => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<CLIRuntimeRequestRespondParams>(params_value) {
                        Ok(params) => {
                            self.cli_runtime_request_respond(connection_id, request.id, params)
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
                        Ok(_params) => match self.gateway_settings_snapshot() {
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
                methods::SETTINGS_UPDATE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<GatewaySettingsUpdateParams>(params_value) {
                        Ok(params) => match self.update_gateway_settings(params.update).await {
                            Ok(settings) => {
                                let result =
                                    pioneer_protocol::GatewaySettingsUpdateResponse { settings };
                                match JsonRpcResponse::from_result(request.id, &result) {
                                    Ok(response) => {
                                        if let Err(error) =
                                            self.send_json(connection_id, &response).await
                                        {
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
                                self.send_error(
                                    connection_id,
                                    JsonRpcErrorResponse::new(
                                        Some(request.id),
                                        INVALID_REQUEST_CODE,
                                        format!("failed to update gateway settings: {error:#}"),
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
                                        methods::SETTINGS_UPDATE
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
                            self.provider_list_models(connection_id, request.id, params)
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
                methods::PROVIDER_SET_API_KEY => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ProviderSetApiKeyParams>(params_value) {
                        Ok(params) => {
                            self.provider_set_api_key(connection_id, request.id, params)
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
                            self.provider_delete_api_key(connection_id, request.id, params)
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
                            self.mcp_list(connection_id, request.id, params).await;
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
                            self.mcp_install(connection_id, request.id, params).await;
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
                            self.mcp_policy_set(connection_id, request.id, params).await;
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
                            self.mcp_server_restart(connection_id, request.id, params)
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
                            self.mcp_uninstall(connection_id, request.id, params).await;
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
                            self.mcp_server_details(connection_id, request.id, params)
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
                            self.skills_list(connection_id, request.id, params).await;
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
                            self.skills_install(connection_id, request.id, params).await;
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
                methods::SKILLS_UPDATE => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<SkillsUpdateParams>(params_value) {
                        Ok(params) => {
                            self.skills_update(connection_id, request.id, params).await;
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
                            self.skills_uninstall(connection_id, request.id, params)
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
                            self.skills_upload_start(connection_id, request.id, params)
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
                            self.skills_upload_finish(connection_id, request.id, params)
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
                            self.skills_upload_abort(connection_id, request.id, params)
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
                            self.artifact_capabilities(connection_id, request.id, params)
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
                                connection_id,
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
                                connection_id,
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
                                connection_id,
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
                                connection_id,
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
                        Ok(params) => self.artifact_get(connection_id, request.id, params).await,
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
                methods::ARTIFACT_READ => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactReadParams>(params_value) {
                        Ok(params) => self.artifact_read(connection_id, request.id, params).await,
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                JsonRpcErrorResponse::new(
                                    Some(request.id),
                                    INVALID_PARAMS_CODE,
                                    format!(
                                        "invalid params for `{}`: {error}",
                                        methods::ARTIFACT_READ
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
                            self.artifact_delete(connection_id, request.id, params)
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
                            self.artifact_restore(connection_id, request.id, params)
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
                        Ok(params) => self.artifact_bind(connection_id, request.id, params).await,
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
                            self.artifact_upload_start(connection_id, request.id, params)
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
                            self.artifact_upload_finish(connection_id, request.id, params)
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
                            self.artifact_upload_abort(connection_id, request.id, params)
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
                methods::ARTIFACT_DOWNLOAD_START => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactDownloadStartParams>(params_value) {
                        Ok(params) => {
                            self.artifact_download_start(connection_id, request.id, params)
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
                                        methods::ARTIFACT_DOWNLOAD_START
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::ARTIFACT_DOWNLOAD_CHUNK => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactDownloadChunkParams>(params_value) {
                        Ok(params) => {
                            self.artifact_download_chunk(connection_id, request.id, params)
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
                                        methods::ARTIFACT_DOWNLOAD_CHUNK
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::ARTIFACT_DOWNLOAD_FINISH => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactDownloadFinishParams>(params_value) {
                        Ok(params) => {
                            self.artifact_download_finish(connection_id, request.id, params)
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
                                        methods::ARTIFACT_DOWNLOAD_FINISH
                                    ),
                                ),
                            )
                            .await;
                        }
                    }
                }
                methods::ARTIFACT_DOWNLOAD_ABORT => {
                    let params_value = request.params.unwrap_or_else(empty_object_value);
                    match serde_json::from_value::<ArtifactDownloadAbortParams>(params_value) {
                        Ok(params) => {
                            self.artifact_download_abort(connection_id, request.id, params)
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
                                        methods::ARTIFACT_DOWNLOAD_ABORT
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
                            self.skills_health(connection_id, request.id, params).await;
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
                            self.skills_policy_list(connection_id, request.id, params)
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
                            self.skills_policy_set(connection_id, request.id, params)
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
                        Ok(params) => self.task_create(connection_id, request.id, params).await,
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
                        Ok(params) => self.task_get(connection_id, request.id, params).await,
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
                        Ok(params) => self.task_list(connection_id, request.id, params).await,
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
                        Ok(params) => self.task_tree(connection_id, request.id, params).await,
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
                        Ok(params) => self.task_events(connection_id, request.id, params).await,
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
                        Ok(params) => self.task_wait(connection_id, request.id, params).await,
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
                        Ok(params) => self.task_accept(connection_id, request.id, params).await,
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
                            message_future(self.task_revise(connection_id, request.id, params))
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
                        Ok(params) => self.task_cancel(connection_id, request.id, params).await,
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
                            self.task_reschedule(connection_id, request.id, params)
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
                        Ok(params) => self.task_pause(connection_id, request.id, params).await,
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
                        Ok(params) => self.task_resume(connection_id, request.id, params).await,
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
                        Ok(params) => self.task_agenda(connection_id, request.id, params).await,
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
                            self.task_deliveries(connection_id, request.id, params)
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
                        Ok(params) => self.task_detach(connection_id, request.id, params).await,
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
        })
    }

    async fn dispatch_memory_candidates_list(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: Option<JsonValue>,
    ) {
        let params_value = params.unwrap_or_else(empty_object_value);
        match serde_json::from_value::<MemoryCandidatesListParams>(params_value) {
            Ok(params) => {
                self.memory_candidates_list(connection_id, request_id, params)
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
        connection_id: ConnectionId,
        request_id: RequestId,
        params: Option<JsonValue>,
    ) {
        let params_value = params.unwrap_or_else(empty_object_value);
        match serde_json::from_value::<MemoryCandidatesGetParams>(params_value) {
            Ok(params) => {
                self.memory_candidates_get(connection_id, request_id, params)
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
        connection_id: ConnectionId,
        request_id: RequestId,
        params: Option<JsonValue>,
    ) {
        let params_value = params.unwrap_or_else(empty_object_value);
        match serde_json::from_value::<MemoryCandidatesDecideParams>(params_value) {
            Ok(params) => {
                self.memory_candidates_decide(connection_id, request_id, params)
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
        connection_id: ConnectionId,
        request_id: RequestId,
        params: Option<JsonValue>,
    ) {
        let params_value = params.unwrap_or_else(empty_object_value);
        match serde_json::from_value::<MemoryCandidatesApproveParams>(params_value) {
            Ok(params) => {
                self.memory_candidates_approve(connection_id, request_id, params)
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
        connection_id: ConnectionId,
        request_id: RequestId,
        params: Option<JsonValue>,
    ) {
        let params_value = params.unwrap_or_else(empty_object_value);
        match serde_json::from_value::<MemoryCandidatesRejectParams>(params_value) {
            Ok(params) => {
                self.memory_candidates_reject(connection_id, request_id, params)
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
        connection_id: ConnectionId,
        request_id: RequestId,
        params: Option<JsonValue>,
    ) {
        let params_value = params.unwrap_or_else(empty_object_value);
        match serde_json::from_value::<MemoryCandidatesEditAndApproveParams>(params_value) {
            Ok(params) => {
                self.memory_candidates_edit_and_approve(connection_id, request_id, params)
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
        connection_id: ConnectionId,
        request_id: RequestId,
        params: Option<JsonValue>,
    ) {
        let params_value = params.unwrap_or_else(empty_object_value);
        match serde_json::from_value::<MemoryCandidatesMergeParams>(params_value) {
            Ok(params) => {
                self.memory_candidates_merge(connection_id, request_id, params)
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
        connection_id: ConnectionId,
        request_id: RequestId,
        params: Option<JsonValue>,
    ) {
        let params_value = params.unwrap_or_else(empty_object_value);
        match serde_json::from_value::<MemoryCandidatesSuppressSimilarParams>(params_value) {
            Ok(params) => {
                self.memory_candidates_suppress_similar(connection_id, request_id, params)
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

    fn gateway_settings_snapshot(
        &self,
    ) -> anyhow::Result<pioneer_protocol::GatewaySettingsSnapshot> {
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
        self.gateway_settings_snapshot_from_settings(&config.gateway, &settings)
    }

    async fn update_gateway_settings(
        &self,
        update: pioneer_protocol::GatewaySettingsUpdate,
    ) -> anyhow::Result<pioneer_protocol::GatewaySettingsSnapshot> {
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

        let previous_general_settings = settings.effective_general_settings(&config.gateway);
        let changes = settings.apply_protocol_update(update)?;
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

        let snapshot = self.gateway_settings_snapshot_from_settings(&config.gateway, &settings)?;
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
                crate::settings::GatewayThreadEpisodicSettings::from_protocol(
                    snapshot.thread_episodic.clone(),
                );
            let runtime_config = crate::thread_episodic_runtime_config_from_gateway_settings(
                &thread_episodic_settings,
            );
            self.apply_thread_episodic_runtime_config(runtime_config)
                .await;
            self.reinstall_memory_hook_runtime_if_bound().await;
        }

        Ok(snapshot)
    }

    fn gateway_settings_snapshot_from_settings(
        &self,
        config: &pioneer_config::GatewayConfig,
        settings: &crate::settings::GatewaySettings,
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
        Ok(settings.snapshot_with_remote_access_status(
            config,
            has_remote_access_key,
            remote_access_status,
        ))
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

    pub async fn connection_closed(&self, connection_id: ConnectionId) {
        self.artifact_uploads.abort_connection(connection_id).await;
        self.artifact_downloads
            .abort_connection(connection_id)
            .await;
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
            "removed detached idle threads after connection closed"
        );
    }
}
