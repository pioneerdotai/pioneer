use super::*;
use crate::composer::turn_prepare as client_composer_turn_prepare;
use crate::transport::ws::command_sender as client_ws_commands;
use crate::{
    artifacts::upload::ArtifactUploadTransport, platform::ClientFileSystem,
    skills::catalog::SkillSnapshotTransport,
};
use pioneer_protocol::{
    ThreadFilePatchHistoryPageParams, ThreadFilePatchHistoryPageResponse,
    ThreadPatchStepsPageParams, ThreadPatchStepsPageResponse, ThreadReadParams, ThreadReadResponse,
    TurnMessageDeleteParams, TurnMessageDeleteResponse, TurnMessageEditParams,
    TurnMessageEditResponse, TurnMessageRevisionsPageParams, TurnMessageRevisionsPageResponse,
    TurnPatchDiffGetParams, TurnPatchDiffGetResponse, TurnPatchRecordGetParams,
    TurnPatchRecordGetResponse, TurnPatchStepsPageParams, TurnPatchStepsPageResponse,
};

impl crate::rpc::JsonRpcRequestTransport for GatewayWsCommandSender {
    fn send_json_rpc_request(
        &self,
        request_id: String,
        payload: String,
        response_tx: crate::rpc::JsonRpcResponseSender,
    ) -> std::result::Result<(), String> {
        self.command_tx
            .send(GatewayWsCommand::Request {
                request_id,
                payload,
                response_tx,
            })
            .map_err(|_| crate::rpc::WEBSOCKET_WORKER_UNAVAILABLE_MESSAGE.to_owned())
    }
}

impl ArtifactUploadTransport for GatewayWsCommandSender {
    fn artifact_upload_start(
        &self,
        params: ArtifactUploadStartParams,
    ) -> Result<ArtifactUploadStartResponse> {
        GatewayWsCommandSender::artifact_upload_start(self, params)
    }

    fn send_artifact_upload_chunk(
        &self,
        workspace_id: String,
        upload_id: String,
        offset: u64,
        chunk: Vec<u8>,
    ) -> Result<ArtifactUploadChunkAckNotification> {
        GatewayWsCommandSender::send_artifact_upload_chunk(
            self,
            workspace_id,
            upload_id,
            offset,
            chunk,
        )
    }

    fn artifact_upload_finish(
        &self,
        params: ArtifactUploadFinishParams,
    ) -> Result<ArtifactUploadFinishResponse> {
        GatewayWsCommandSender::artifact_upload_finish(self, params)
    }

    fn artifact_upload_abort(
        &self,
        params: ArtifactUploadAbortParams,
    ) -> Result<ArtifactUploadAbortResponse> {
        GatewayWsCommandSender::artifact_upload_abort(self, params)
    }
}

impl client_composer_turn_prepare::ComposerTurnPrepareTransport for GatewayWsCommandSender {
    fn artifact_capabilities(
        &self,
        params: ArtifactCapabilitiesParams,
    ) -> Result<ArtifactCapabilitiesResponse> {
        GatewayWsCommandSender::artifact_capabilities(self, params)
    }
}

impl SkillSnapshotTransport for GatewayWsCommandSender {
    fn skills_list(&self, params: SkillListParams) -> Result<SkillListResponse> {
        GatewayWsCommandSender::skills_list(self, params)
    }

    fn skills_health(&self, params: SkillsHealthParams) -> Result<SkillsHealthResponse> {
        GatewayWsCommandSender::skills_health(self, params)
    }
}

impl GatewayWsCommandSender {
    pub(crate) fn gateway_state_event_is_current(&self, event: &GatewayWsEvent) -> bool {
        let id = super::super::event_connection_id(event);
        self.connection_generations.lock().is_ok_and(|generations| {
            generations.active == Some(id) || generations.pending == Some(id)
        })
    }

    fn begin_connection_attempt(&self) -> Result<GatewayConnectionAttempt<'_>> {
        let id = self
            .next_connection_id
            .fetch_add(1, Ordering::Relaxed)
            .checked_add(1)
            .expect("Gateway connection generation exhausted");
        self.connection_generations
            .lock()
            .map_err(|_| anyhow!("Gateway connection owner poisoned"))?
            .pending = Some(id);
        Ok(GatewayConnectionAttempt { sender: self, id })
    }

    pub(crate) fn accepts_gateway_event(&self, event: &GatewayWsEvent) -> bool {
        let id = super::super::event_connection_id(event);
        self.connection_generations
            .lock()
            .is_ok_and(|mut generations| {
                if generations.active == Some(id) || generations.pending == Some(id) {
                    return true;
                }
                if generations.retiring == Some(id)
                    && matches!(event, GatewayWsEvent::Disconnected { .. })
                {
                    generations.retiring = None;
                    return true;
                }
                false
            })
    }

    pub fn connect_and_wait(&self, spec: GatewayWsConnectSpec) -> Result<u64> {
        let attempt = self.begin_connection_attempt()?;
        let connection_id = attempt.id;
        let session_access = http_access_from_spec(&spec, connection_id);

        let (result_tx, result_rx) = mpsc::channel();

        self.command_tx
            .send(GatewayWsCommand::Connect {
                connection_id,
                spec,
                initial_result_tx: Some(result_tx),
                retry_initial_failure: false,
            })
            .map_err(|_| anyhow!("websocket worker is not available"))?;

        let result = result_rx
            .recv()
            .map_err(|_| anyhow!("websocket worker dropped initial connect result"))?;

        result.map_err(anyhow::Error::msg)?;
        attempt.complete(session_access)
    }

    pub fn connect_with_retry(&self, spec: GatewayWsConnectSpec) -> Result<u64> {
        let attempt = self.begin_connection_attempt()?;
        let connection_id = attempt.id;
        let session_access = http_access_from_spec(&spec, connection_id);

        self.command_tx
            .send(GatewayWsCommand::Connect {
                connection_id,
                spec,
                initial_result_tx: None,
                retry_initial_failure: true,
            })
            .map_err(|_| anyhow!("websocket worker is not available"))?;

        // The Desktop action gate additionally requires a Connected event, so
        // this snapshot cannot authorize HTTP while the retrying WS is merely
        // connecting. Publishing it here keeps the sender as the single owner
        // of the ephemeral credential without copying it into shell state.
        attempt.complete(session_access)
    }

    pub fn replace_access_and_wait(&self, spec: GatewayWsConnectSpec) -> Result<u64> {
        let attempt = self.begin_connection_attempt()?;
        let connection_id = attempt.id;
        let session_access = http_access_from_spec(&spec, connection_id);
        let (result_tx, result_rx) = mpsc::channel();
        self.command_tx
            .send(GatewayWsCommand::Replace {
                connection_id,
                spec,
                result_tx,
            })
            .map_err(|_| anyhow!("websocket worker is not available"))?;
        result_rx
            .recv()
            .map_err(|_| anyhow!("websocket worker dropped access replacement result"))?
            .map_err(anyhow::Error::msg)?;
        attempt.complete(session_access)
    }

    pub fn shutdown(&self) -> Result<()> {
        *self
            .connection_generations
            .lock()
            .map_err(|_| anyhow!("Gateway connection owner poisoned"))? =
            GatewayConnectionGenerations::default();
        self.replace_http_session_access(None)?;
        self.command_tx
            .send(GatewayWsCommand::Shutdown)
            .map_err(|_| anyhow!("websocket worker is not available"))
    }

    pub fn disconnect(&self) -> Result<()> {
        {
            let mut generations = self
                .connection_generations
                .lock()
                .map_err(|_| anyhow!("Gateway connection owner poisoned"))?;
            generations.retiring = generations.active.take();
            generations.pending = None;
        }
        self.replace_http_session_access(None)?;
        self.command_tx
            .send(GatewayWsCommand::Disconnect)
            .map_err(|_| anyhow!("websocket worker is not available"))
    }

    /// Retire only the named connection, preserving a newer active or pending attempt.
    pub(crate) fn disconnect_connection(&self, connection_id: u64) -> Result<()> {
        let mut generations = self
            .connection_generations
            .lock()
            .map_err(|_| anyhow!("Gateway connection owner poisoned"))?;
        if generations.active == Some(connection_id) {
            generations.retiring = generations.active.take();
        }
        if generations.pending == Some(connection_id) {
            generations.pending = None;
        }
        {
            let mut access = self
                .session_access
                .lock()
                .map_err(|_| anyhow!("Gateway session access lock is poisoned"))?;
            if access
                .as_ref()
                .is_some_and(|access| access.generation == connection_id)
            {
                *access = None;
            }
        }
        self.command_tx
            .send(GatewayWsCommand::DisconnectConnection { connection_id })
            .map_err(|_| anyhow!("websocket worker is not available"))
    }

    pub fn current_gateway_http_access(
        &self,
    ) -> std::result::Result<GatewayHttpAccess, GatewayHttpAuthorityError> {
        self.session_access
            .lock()
            .map_err(|_| GatewayHttpAuthorityError::TemporarilyUnavailable)?
            .clone()
            .ok_or(GatewayHttpAuthorityError::TemporarilyUnavailable)
    }

    pub(super) fn replace_http_session_access(
        &self,
        access: Option<GatewayHttpAccess>,
    ) -> Result<()> {
        *self
            .session_access
            .lock()
            .map_err(|_| anyhow!("Gateway session access lock is poisoned"))? = access;
        Ok(())
    }

    pub fn auth_me(&self) -> Result<AuthMeResponse> {
        client_ws_commands::auth_me(self)
    }

    pub fn authorization_capabilities(
        &self,
        params: AuthorizationCapabilitiesParams,
    ) -> Result<AuthorizationCapabilitySnapshot> {
        client_ws_commands::authorization_capabilities(self, params)
    }

    pub fn auth_profile_update(
        &self,
        params: AuthProfileUpdateParams,
    ) -> Result<AuthProfileUpdateResponse> {
        client_ws_commands::auth_profile_update(self, params)
    }

    pub fn auth_session_list(&self) -> Result<AuthSessionListResponse> {
        client_ws_commands::auth_session_list(self)
    }

    pub fn auth_session_revoke(
        &self,
        params: AuthSessionRevokeParams,
    ) -> Result<AuthSessionRevokeResponse> {
        client_ws_commands::auth_session_revoke(self, params)
    }

    pub fn auth_logout(&self) -> Result<AuthLogoutResponse> {
        client_ws_commands::auth_logout(self)
    }

    pub fn auth_device_create(&self) -> Result<AuthDeviceCreateResponse> {
        client_ws_commands::auth_device_create(self)
    }

    pub fn invitation_create(
        &self,
        params: InvitationCreateParams,
    ) -> Result<InvitationCreateResponse> {
        client_ws_commands::invitation_create(self, params)
    }

    pub fn invitation_list(&self, params: InvitationListParams) -> Result<InvitationListResponse> {
        client_ws_commands::invitation_list(self, params)
    }

    pub fn invitation_revoke(
        &self,
        params: InvitationRevokeParams,
    ) -> Result<InvitationRevokeResponse> {
        client_ws_commands::invitation_revoke(self, params)
    }

    pub fn member_list(&self, params: MemberListParams) -> Result<MemberListResponse> {
        client_ws_commands::member_list(self, params)
    }

    pub fn member_suspend(&self, params: MemberSuspendParams) -> Result<MemberMutationResponse> {
        client_ws_commands::member_suspend(self, params)
    }

    pub fn member_restore(&self, params: MemberRestoreParams) -> Result<MemberMutationResponse> {
        client_ws_commands::member_restore(self, params)
    }

    pub fn member_remove(&self, params: MemberRemoveParams) -> Result<MemberMutationResponse> {
        client_ws_commands::member_remove(self, params)
    }

    pub fn member_device_create(
        &self,
        params: MemberDeviceCreateParams,
    ) -> Result<MemberDeviceCreateResponse> {
        client_ws_commands::member_device_create(self, params)
    }

    pub fn workspace_member_list(
        &self,
        params: WorkspaceMemberListParams,
    ) -> Result<WorkspaceMemberListResponse> {
        client_ws_commands::workspace_member_list(self, params)
    }

    pub fn workspace_member_add(
        &self,
        params: WorkspaceMemberAddParams,
    ) -> Result<WorkspaceMemberMutationResponse> {
        client_ws_commands::workspace_member_add(self, params)
    }

    pub fn workspace_member_remove(
        &self,
        params: WorkspaceMemberRemoveParams,
    ) -> Result<WorkspaceMemberMutationResponse> {
        client_ws_commands::workspace_member_remove(self, params)
    }

    pub fn thread_start(&self, params: ThreadStartParams) -> Result<ThreadStartResponse> {
        client_ws_commands::thread_start(self, params)
    }

    pub fn thread_tree(&self, params: ThreadTreeParams) -> Result<ThreadTreeResponse> {
        client_ws_commands::thread_tree(self, params)
    }
    pub fn thread_get(&self, params: ThreadGetParams) -> Result<ThreadGetResponse> {
        client_ws_commands::thread_get(self, params)
    }

    pub fn thread_update(&self, params: ThreadUpdateParams) -> Result<ThreadUpdateResponse> {
        client_ws_commands::thread_update(self, params)
    }

    pub fn thread_participants_list(
        &self,
        params: ThreadParticipantsListParams,
    ) -> Result<ThreadParticipantsResponse> {
        client_ws_commands::thread_participants_list(self, params)
    }

    pub fn thread_participant_add(
        &self,
        params: ThreadParticipantMutationParams,
    ) -> Result<ThreadParticipantsResponse> {
        client_ws_commands::thread_participant_add(self, params)
    }

    pub fn thread_participant_remove(
        &self,
        params: ThreadParticipantMutationParams,
    ) -> Result<ThreadParticipantsResponse> {
        client_ws_commands::thread_participant_remove(self, params)
    }

    pub fn thread_move(&self, params: ThreadMoveParams) -> Result<ThreadMoveResponse> {
        client_ws_commands::thread_move(self, params)
    }

    pub fn thread_folder_create(
        &self,
        params: ThreadFolderCreateParams,
    ) -> Result<ThreadFolderCreateResponse> {
        client_ws_commands::thread_folder_create(self, params)
    }

    pub fn thread_folder_move(
        &self,
        params: ThreadFolderMoveParams,
    ) -> Result<ThreadFolderMoveResponse> {
        client_ws_commands::thread_folder_move(self, params)
    }

    pub fn thread_folder_delete(
        &self,
        params: ThreadFolderDeleteParams,
    ) -> Result<ThreadFolderDeleteResponse> {
        client_ws_commands::thread_folder_delete(self, params)
    }

    pub fn thread_agents_doc_get(
        &self,
        params: ThreadAgentsDocGetParams,
    ) -> Result<ThreadAgentsDocGetResponse> {
        client_ws_commands::thread_agents_doc_get(self, params)
    }

    pub fn thread_agents_doc_save(
        &self,
        params: ThreadAgentsDocSaveParams,
    ) -> Result<ThreadAgentsDocSaveResponse> {
        client_ws_commands::thread_agents_doc_save(self, params)
    }

    pub fn thread_agents_doc_archive(
        &self,
        params: ThreadAgentsDocArchiveParams,
    ) -> Result<ThreadAgentsDocArchiveResponse> {
        client_ws_commands::thread_agents_doc_archive(self, params)
    }
    pub fn thread_agents_doc_resolve_for_thread(
        &self,
        params: ThreadAgentsDocResolveForThreadParams,
    ) -> Result<ThreadAgentsDocResolveForThreadResponse> {
        client_ws_commands::thread_agents_doc_resolve_for_thread(self, params)
    }

    pub fn thread_timeline_page(
        &self,
        params: ThreadTimelinePageParams,
    ) -> Result<ThreadTimelinePageResponse> {
        client_ws_commands::thread_timeline_page(self, params)
    }

    pub fn thread_patch_steps_page(
        &self,
        params: ThreadPatchStepsPageParams,
    ) -> Result<ThreadPatchStepsPageResponse> {
        client_ws_commands::thread_patch_steps_page(self, params)
    }

    pub fn thread_file_patch_history_page(
        &self,
        params: ThreadFilePatchHistoryPageParams,
    ) -> Result<ThreadFilePatchHistoryPageResponse> {
        client_ws_commands::thread_file_patch_history_page(self, params)
    }

    pub fn workspace_default(&self) -> Result<WorkspaceDefaultResponse> {
        client_ws_commands::workspace_default(self)
    }

    pub fn workspace_list(&self) -> Result<WorkspaceListResponse> {
        client_ws_commands::workspace_list(self)
    }

    pub fn workspace_create(
        &self,
        params: WorkspaceCreateParams,
    ) -> Result<WorkspaceCreateResponse> {
        client_ws_commands::workspace_create(self, params)
    }

    pub fn workspace_select(
        &self,
        params: WorkspaceSelectParams,
    ) -> Result<WorkspaceSelectResponse> {
        client_ws_commands::workspace_select(self, params)
    }

    pub fn workspace_update(
        &self,
        params: WorkspaceUpdateParams,
    ) -> Result<WorkspaceUpdateResponse> {
        client_ws_commands::workspace_update(self, params)
    }

    pub fn thread_unsubscribe(&self, thread_id: String) -> Result<ThreadUnsubscribeResponse> {
        client_ws_commands::thread_unsubscribe(self, thread_id)
    }

    pub fn turn_start(&self, params: TurnStartParams) -> Result<TurnStartResponse> {
        client_ws_commands::turn_start(self, params)
    }

    pub fn turn_message_edit(
        &self,
        params: TurnMessageEditParams,
    ) -> Result<TurnMessageEditResponse> {
        client_ws_commands::turn_message_edit(self, params)
    }

    pub fn turn_message_delete(
        &self,
        params: TurnMessageDeleteParams,
    ) -> Result<TurnMessageDeleteResponse> {
        client_ws_commands::turn_message_delete(self, params)
    }

    pub fn turn_message_revisions_page(
        &self,
        params: TurnMessageRevisionsPageParams,
    ) -> Result<TurnMessageRevisionsPageResponse> {
        client_ws_commands::turn_message_revisions_page(self, params)
    }

    pub fn thread_read(&self, params: ThreadReadParams) -> Result<ThreadReadResponse> {
        client_ws_commands::thread_read(self, params)
    }

    pub fn turn_cancel(&self, params: TurnCancelParams) -> Result<TurnCancelResponse> {
        client_ws_commands::turn_cancel(self, params)
    }

    pub fn voice_status(&self, params: VoiceStatusParams) -> Result<VoiceStatusResponse> {
        client_ws_commands::voice_status(self, params)
    }

    pub fn voice_session_start(
        &self,
        params: VoiceSessionStartParams,
    ) -> Result<VoiceSessionStartResponse> {
        client_ws_commands::voice_session_start(self, params)
    }

    pub fn voice_session_finalize(
        &self,
        params: VoiceSessionFinalizeParams,
    ) -> Result<VoiceSessionFinalizeResponse> {
        client_ws_commands::voice_session_finalize(self, params)
    }

    pub fn voice_session_cancel(
        &self,
        params: VoiceSessionCancelParams,
    ) -> Result<VoiceSessionCancelResponse> {
        client_ws_commands::voice_session_cancel(self, params)
    }

    pub fn provider_list(&self, params: ProviderListParams) -> Result<ProviderListResponse> {
        client_ws_commands::provider_list(self, params)
    }

    pub fn cli_runtime_list(&self, params: CLIRuntimeListParams) -> Result<CLIRuntimeListResponse> {
        client_ws_commands::cli_runtime_list(self, params)
    }

    pub fn cli_runtime_list_models(
        &self,
        params: CLIRuntimeListModelsParams,
    ) -> Result<CLIRuntimeListModelsResponse> {
        client_ws_commands::cli_runtime_list_models(self, params)
    }

    pub fn cli_runtime_status(
        &self,
        params: CLIRuntimeStatusParams,
    ) -> Result<CLIRuntimeStatusResponse> {
        client_ws_commands::cli_runtime_status(self, params)
    }

    pub fn cli_runtime_thread_binding_get(
        &self,
        params: CLIRuntimeThreadBindingGetParams,
    ) -> Result<CLIRuntimeThreadBindingGetResponse> {
        client_ws_commands::cli_runtime_thread_binding_get(self, params)
    }

    pub fn cli_runtime_thread_compact(
        &self,
        params: CLIRuntimeThreadCompactParams,
    ) -> Result<CLIRuntimeThreadCompactResponse> {
        client_ws_commands::cli_runtime_thread_compact(self, params)
    }

    pub fn cli_runtime_thread_fork(
        &self,
        params: CLIRuntimeThreadForkParams,
    ) -> Result<CLIRuntimeThreadForkResponse> {
        client_ws_commands::cli_runtime_thread_fork(self, params)
    }

    pub fn cli_runtime_turn_steer(
        &self,
        params: CLIRuntimeTurnSteerParams,
    ) -> Result<CLIRuntimeTurnSteerResponse> {
        client_ws_commands::cli_runtime_turn_steer(self, params)
    }

    pub fn cli_runtime_review_start(
        &self,
        params: CLIRuntimeReviewStartParams,
    ) -> Result<CLIRuntimeReviewStartResponse> {
        client_ws_commands::cli_runtime_review_start(self, params)
    }

    pub fn cli_runtime_refresh(
        &self,
        params: CLIRuntimeRefreshParams,
    ) -> Result<CLIRuntimeRefreshResponse> {
        client_ws_commands::cli_runtime_refresh(self, params)
    }

    pub fn cli_runtime_login_start(
        &self,
        params: CLIRuntimeLoginStartParams,
    ) -> Result<CLIRuntimeLoginStartResponse> {
        client_ws_commands::cli_runtime_login_start(self, params)
    }

    pub fn cli_runtime_login_cancel(
        &self,
        params: CLIRuntimeLoginCancelParams,
    ) -> Result<CLIRuntimeLoginCancelResponse> {
        client_ws_commands::cli_runtime_login_cancel(self, params)
    }

    pub fn cli_runtime_request_respond(
        &self,
        params: CLIRuntimeRequestRespondParams,
    ) -> Result<CLIRuntimeRequestRespondResponse> {
        client_ws_commands::cli_runtime_request_respond(self, params)
    }

    pub fn turn_permission_request_respond(
        &self,
        params: TurnPermissionRequestRespondParams,
    ) -> Result<TurnPermissionRequestRespondResponse> {
        client_ws_commands::turn_permission_request_respond(self, params)
    }

    pub fn gateway_settings_get(&self) -> Result<GatewaySettingsGetResponse> {
        client_ws_commands::gateway_settings_get(self)
    }

    pub fn gateway_settings_update(
        &self,
        update: GatewaySettingsUpdate,
    ) -> Result<GatewaySettingsUpdateResponse> {
        client_ws_commands::gateway_settings_update(self, update)
    }

    pub fn provider_list_models(
        &self,
        params: ProviderListModelsParams,
    ) -> Result<ProviderListModelsResponse> {
        client_ws_commands::provider_list_models(self, params)
    }

    pub fn provider_list_embedding_models(
        &self,
        params: ProviderListModelsParams,
    ) -> Result<ProviderListModelsResponse> {
        client_ws_commands::provider_list_embedding_models(self, params)
    }

    pub fn provider_list_transcription_models(
        &self,
        params: ProviderListModelsParams,
    ) -> Result<ProviderListModelsResponse> {
        client_ws_commands::provider_list_transcription_models(self, params)
    }

    pub fn provider_set_api_key(
        &self,
        params: ProviderSetApiKeyParams,
    ) -> Result<ProviderSetApiKeyResponse> {
        client_ws_commands::provider_set_api_key(self, params)
    }

    pub fn provider_configure(
        &self,
        params: ProviderConfigureParams,
    ) -> Result<ProviderConfigureResponse> {
        client_ws_commands::provider_configure(self, params)
    }

    pub fn provider_delete_api_key(
        &self,
        params: ProviderDeleteApiKeyParams,
    ) -> Result<ProviderDeleteApiKeyResponse> {
        client_ws_commands::provider_delete_api_key(self, params)
    }

    pub fn cli_runtime_proxy_set(
        &self,
        params: CLIRuntimeProxySetParams,
    ) -> Result<CLIRuntimeProxySetResponse> {
        client_ws_commands::cli_runtime_proxy_set(self, params)
    }

    pub fn cli_runtime_proxy_delete(
        &self,
        params: CLIRuntimeProxyDeleteParams,
    ) -> Result<CLIRuntimeProxyDeleteResponse> {
        client_ws_commands::cli_runtime_proxy_delete(self, params)
    }

    pub fn turn_get(&self, params: TurnGetParams) -> Result<TurnGetResponse> {
        client_ws_commands::turn_get(self, params)
    }

    pub fn turn_items_page(&self, params: TurnItemsParams) -> Result<TurnItemsResponse> {
        client_ws_commands::turn_items_page(self, params)
    }

    pub fn turn_patch_steps_page(
        &self,
        params: TurnPatchStepsPageParams,
    ) -> Result<TurnPatchStepsPageResponse> {
        client_ws_commands::turn_patch_steps_page(self, params)
    }

    pub fn turn_patch_record_get(
        &self,
        params: TurnPatchRecordGetParams,
    ) -> Result<TurnPatchRecordGetResponse> {
        client_ws_commands::turn_patch_record_get(self, params)
    }

    pub fn turn_patch_diff_get(
        &self,
        params: TurnPatchDiffGetParams,
    ) -> Result<TurnPatchDiffGetResponse> {
        client_ws_commands::turn_patch_diff_get(self, params)
    }

    pub fn task_user_notification_list(
        &self,
        params: TaskUserNotificationListParams,
    ) -> Result<TaskUserNotificationListResponse> {
        client_ws_commands::task_user_notification_list(self, params)
    }

    pub fn task_user_notification_acknowledge(
        &self,
        params: TaskUserNotificationAcknowledgeParams,
    ) -> Result<TaskUserNotificationAcknowledgeResponse> {
        client_ws_commands::task_user_notification_acknowledge(self, params)
    }

    pub fn turn_work_page(&self, params: TurnWorkPageParams) -> Result<TurnWorkPageResponse> {
        client_ws_commands::turn_work_page(self, params)
    }

    pub fn turn_work_items_get(
        &self,
        params: TurnWorkItemsGetParams,
    ) -> Result<TurnWorkItemsGetResponse> {
        client_ws_commands::turn_work_items_get(self, params)
    }

    pub fn task_accept(&self, params: TaskAcceptParams) -> Result<TaskAcceptResponse> {
        client_ws_commands::task_accept(self, params)
    }

    pub fn task_revise(&self, params: TaskReviseParams) -> Result<TaskReviseResponse> {
        client_ws_commands::task_revise(self, params)
    }

    pub fn task_cancel(&self, params: TaskCancelParams) -> Result<TaskCancelResponse> {
        client_ws_commands::task_cancel(self, params)
    }

    pub fn skills_list(&self, params: SkillListParams) -> Result<SkillListResponse> {
        client_ws_commands::skills_list(self, params)
    }

    pub fn mcp_list(&self, params: McpListParams) -> Result<McpListResponse> {
        client_ws_commands::mcp_list(self, params)
    }

    pub fn mcp_install(&self, params: McpInstallParams) -> Result<McpInstallResponse> {
        client_ws_commands::mcp_install(self, params)
    }

    pub fn mcp_policy_set(&self, params: McpPolicySetParams) -> Result<McpPolicySetResponse> {
        client_ws_commands::mcp_policy_set(self, params)
    }

    pub fn mcp_server_restart(
        &self,
        params: McpServerRestartParams,
    ) -> Result<McpServerRestartResponse> {
        client_ws_commands::mcp_server_restart(self, params)
    }

    pub fn mcp_uninstall(&self, params: McpUninstallParams) -> Result<McpUninstallResponse> {
        client_ws_commands::mcp_uninstall(self, params)
    }

    pub fn mcp_server_details(
        &self,
        params: McpServerDetailsParams,
    ) -> Result<McpServerDetailsResponse> {
        client_ws_commands::mcp_server_details(self, params)
    }

    pub fn skills_install(&self, params: SkillsInstallParams) -> Result<SkillsInstallResponse> {
        client_ws_commands::skills_install(self, params)
    }

    pub fn skills_pack_install(
        &self,
        params: SkillsPackInstallParams,
    ) -> Result<SkillsPackInstallResponse> {
        client_ws_commands::skills_pack_install(self, params)
    }

    pub fn skills_pack_update(
        &self,
        params: SkillsPackUpdateParams,
    ) -> Result<SkillsPackUpdateResponse> {
        client_ws_commands::skills_pack_update(self, params)
    }

    pub fn skills_pack_uninstall(
        &self,
        params: SkillsPackUninstallParams,
    ) -> Result<SkillsPackUninstallResponse> {
        client_ws_commands::skills_pack_uninstall(self, params)
    }

    pub fn skills_upload_start(
        &self,
        params: SkillsUploadStartParams,
    ) -> Result<SkillsUploadStartResponse> {
        client_ws_commands::skills_upload_start(self, params)
    }

    pub fn skills_upload_finish(
        &self,
        params: SkillsUploadFinishParams,
    ) -> Result<SkillsUploadFinishResponse> {
        client_ws_commands::skills_upload_finish(self, params)
    }

    pub fn skills_upload_abort(
        &self,
        params: SkillsUploadAbortParams,
    ) -> Result<SkillsUploadAbortResponse> {
        client_ws_commands::skills_upload_abort(self, params)
    }

    pub fn artifact_capabilities(
        &self,
        params: ArtifactCapabilitiesParams,
    ) -> Result<ArtifactCapabilitiesResponse> {
        client_ws_commands::artifact_capabilities(self, params)
    }
    pub fn artifact_list(&self, params: ArtifactListParams) -> Result<ArtifactListResponse> {
        client_ws_commands::artifact_list(self, params)
    }

    pub fn artifact_list_for_thread(
        &self,
        params: ArtifactListForThreadParams,
    ) -> Result<ArtifactListResponse> {
        client_ws_commands::artifact_list_for_thread(self, params)
    }
    pub fn artifact_list_for_turn(
        &self,
        params: ArtifactListForTurnParams,
    ) -> Result<ArtifactListResponse> {
        client_ws_commands::artifact_list_for_turn(self, params)
    }
    pub fn artifact_list_for_message(
        &self,
        params: ArtifactListForMessageParams,
    ) -> Result<ArtifactListResponse> {
        client_ws_commands::artifact_list_for_message(self, params)
    }
    pub fn artifact_get(&self, params: ArtifactGetParams) -> Result<ArtifactGetResponse> {
        client_ws_commands::artifact_get(self, params)
    }

    pub fn artifact_view_grant_create(
        &self,
        params: ArtifactViewGrantCreateParams,
    ) -> Result<ArtifactViewGrantCreateResponse> {
        client_ws_commands::artifact_view_grant_create(self, params)
    }

    pub fn thread_file_view_grant_create(
        &self,
        params: ThreadFileViewGrantCreateParams,
    ) -> Result<ThreadFileViewGrantCreateResponse> {
        client_ws_commands::thread_file_view_grant_create(self, params)
    }

    pub fn artifact_delete(&self, params: ArtifactDeleteParams) -> Result<ArtifactDeleteResponse> {
        client_ws_commands::artifact_delete(self, params)
    }
    pub fn artifact_restore(
        &self,
        params: ArtifactRestoreParams,
    ) -> Result<ArtifactRestoreResponse> {
        client_ws_commands::artifact_restore(self, params)
    }
    pub fn artifact_bind(&self, params: ArtifactBindParams) -> Result<ArtifactBindResponse> {
        client_ws_commands::artifact_bind(self, params)
    }

    pub fn artifact_upload_start(
        &self,
        params: ArtifactUploadStartParams,
    ) -> Result<ArtifactUploadStartResponse> {
        client_ws_commands::artifact_upload_start(self, params)
    }

    pub fn artifact_upload_finish(
        &self,
        params: ArtifactUploadFinishParams,
    ) -> Result<ArtifactUploadFinishResponse> {
        client_ws_commands::artifact_upload_finish(self, params)
    }

    pub fn artifact_upload_abort(
        &self,
        params: ArtifactUploadAbortParams,
    ) -> Result<ArtifactUploadAbortResponse> {
        client_ws_commands::artifact_upload_abort(self, params)
    }

    pub fn send_artifact_upload_chunk(
        &self,
        workspace_id: String,
        upload_id: String,
        offset: u64,
        chunk: Vec<u8>,
    ) -> Result<ArtifactUploadChunkAckNotification> {
        let payload = crate::transport::ws::frames::encode_artifact_upload_chunk_frame(
            workspace_id,
            upload_id.clone(),
            offset,
            chunk.as_slice(),
        )?;

        let (response_tx, response_rx) = mpsc::channel();
        self.command_tx
            .send(GatewayWsCommand::ArtifactBinaryUploadChunk {
                upload_id,
                offset,
                payload,
                response_tx,
            })
            .map_err(|_| anyhow!("websocket worker is not available"))?;

        let response = response_rx
            .recv_timeout(UPLOAD_CHUNK_ACK_TIMEOUT)
            .map_err(|_| anyhow!("timed out waiting for artifact upload chunk ack"))?;

        response.map_err(anyhow::Error::msg)
    }

    pub fn send_voice_audio_chunk(
        &self,
        session_id: String,
        sequence: u64,
        audio_format: VoiceAudioFormat,
        captured_at_unix_ms: Option<u64>,
        duration_ms: Option<u32>,
        pcm_chunk: Vec<u8>,
    ) -> Result<()> {
        let payload = crate::transport::ws::frames::encode_voice_audio_chunk_frame(
            session_id,
            sequence,
            audio_format,
            captured_at_unix_ms,
            duration_ms,
            pcm_chunk.as_slice(),
        )?;

        self.command_tx
            .send(GatewayWsCommand::VoiceBinaryChunk { payload })
            .map_err(|_| anyhow!("websocket worker is not available"))?;

        Ok(())
    }

    pub fn prepare_composer_turn_with_file_system(
        &self,
        file_system: &impl ClientFileSystem,
        request: client_composer_turn_prepare::PrepareComposerTurnRequest,
    ) -> Result<client_composer_turn_prepare::PreparedComposerTurn> {
        client_composer_turn_prepare::prepare_composer_turn(self, file_system, request)
    }

    pub fn send_skill_upload_chunk(
        &self,
        workspace_id: String,
        upload_id: String,
        offset: u64,
        chunk: Vec<u8>,
    ) -> Result<SkillsUploadChunkAckNotification> {
        let payload = crate::transport::ws::frames::encode_skill_upload_chunk_frame(
            workspace_id,
            upload_id.clone(),
            offset,
            chunk.as_slice(),
        )?;

        let (response_tx, response_rx) = mpsc::channel();
        self.command_tx
            .send(GatewayWsCommand::BinaryUploadChunk {
                upload_id,
                offset,
                payload,
                response_tx,
            })
            .map_err(|_| anyhow!("websocket worker is not available"))?;

        let response = response_rx
            .recv_timeout(UPLOAD_CHUNK_ACK_TIMEOUT)
            .map_err(|_| anyhow!("timed out waiting for skill upload chunk ack"))?;

        response.map_err(anyhow::Error::msg)
    }

    pub fn skills_update(&self, params: SkillsUpdateParams) -> Result<SkillsUpdateResponse> {
        client_ws_commands::skills_update(self, params)
    }

    pub fn skills_uninstall(
        &self,
        params: SkillsUninstallParams,
    ) -> Result<SkillsUninstallResponse> {
        client_ws_commands::skills_uninstall(self, params)
    }

    pub fn skills_health(&self, params: SkillsHealthParams) -> Result<SkillsHealthResponse> {
        client_ws_commands::skills_health(self, params)
    }
    pub fn skills_policy_list(
        &self,
        params: SkillsPolicyListParams,
    ) -> Result<SkillsPolicyListResponse> {
        client_ws_commands::skills_policy_list(self, params)
    }

    pub fn skills_policy_set(
        &self,
        params: SkillsPolicySetParams,
    ) -> Result<SkillsPolicySetResponse> {
        client_ws_commands::skills_policy_set(self, params)
    }
    pub fn request_typed<T, P>(&self, method: &str, params: &P, timeout: Duration) -> Result<T>
    where
        T: DeserializeOwned,
        P: serde::Serialize,
    {
        let params_value =
            serde_json::to_value(params).context("failed to encode JSON-RPC params")?;
        let result = self.request_value(method, params_value, timeout)?;

        crate::rpc::deserialize_json_rpc_result(method, result)
    }
    pub fn request_value(
        &self,
        method: &str,
        params: JsonValue,
        timeout: Duration,
    ) -> Result<JsonValue> {
        crate::rpc::send_json_rpc_request_value(self, method, params, timeout)
    }
}

fn http_access_from_spec(
    spec: &GatewayWsConnectSpec,
    generation: u64,
) -> Option<GatewayHttpAccess> {
    let session = spec.session.as_ref()?;
    let access_token = spec.auth_token.clone()?;
    Some(GatewayHttpAccess {
        gateway_base_url: spec.gateway_base_url.clone(),
        gateway_id: session.server_gateway_id.clone(),
        session_id: session.session_id.clone(),
        generation,
        access_expires_at_unix: session.access_expires_at_unix,
        access_token,
    })
}

#[cfg(test)]
mod connection_generation_tests {
    use super::*;
    fn event(id: u64) -> GatewayWsEvent {
        GatewayWsEvent::Connecting {
            connection_id: id,
            endpoint_id: "synthetic".into(),
            endpoint_name: "Synthetic".into(),
            endpoint_kind: crate::gateway::types::GatewayEndpointKind::Remote,
        }
    }
    #[test]
    fn typed_ingress_reduces_owned_settings_once_and_routes_only_unported_features() {
        use crate::core::{ClientCore, ClientScope};
        use crate::gateway::event_router::GatewayEventRoute;
        use pioneer_protocol::{GatewayNotification, GatewayVoiceInputStatusChangedNotification};
        let core = ClientCore::new();
        let sender = core.compatibility_runtime().ws_command_sender();
        let id = sender
            .begin_connection_attempt()
            .unwrap()
            .complete(None)
            .unwrap();
        let settings = GatewayWsEvent::Notification {
            connection_id: id,
            notification: GatewayNotification::GatewayVoiceInputStatusChanged(
                GatewayVoiceInputStatusChangedNotification {
                    settings: Default::default(),
                },
            ),
        };
        assert_eq!(core.route_gateway_event(&settings), None);
        let published = core.snapshot(&ClientScope::Settings).unwrap().snapshot();
        assert_eq!(core.route_gateway_event(&settings), None);
        assert!(Arc::ptr_eq(
            &published,
            &core.snapshot(&ClientScope::Settings).unwrap().snapshot()
        ));
        let feature = GatewayWsEvent::Notification {
            connection_id: id,
            notification: GatewayNotification::Unknown(
                pioneer_protocol::UnknownGatewayNotification {
                    method: "synthetic/event".into(),
                    workspace_id: None,
                    thread_id: None,
                    turn_id: None,
                    item_id: None,
                    params: serde_json::json!({}),
                },
            ),
        };
        assert_eq!(
            core.route_gateway_event(&feature),
            Some(GatewayEventRoute::Unknown)
        );
        assert!(core.drain_gateway_compatibility_events().is_empty());
        sender.disconnect_connection(id).unwrap();
        assert_eq!(core.route_gateway_event(&feature), None);
        assert_eq!(core.route_gateway_event(&settings), None);
        assert!(Arc::ptr_eq(
            &published,
            &core.snapshot(&ClientScope::Settings).unwrap().snapshot()
        ));
        core.shutdown();
        assert_eq!(core.route_gateway_event(&settings), None);
    }

    #[test]
    fn stale_retirement_preserves_new_active_and_pending_connections() {
        let client = GatewayWsClient::new();
        let sender = client.command_sender();
        let old = sender
            .begin_connection_attempt()
            .unwrap()
            .complete(None)
            .unwrap();
        let pending = sender.begin_connection_attempt().unwrap();
        let candidate = pending.id;
        sender.disconnect_connection(old).unwrap();
        assert!(sender.accepts_gateway_event(&event(candidate)));
        let current = pending.complete(None).unwrap();
        sender.disconnect_connection(old).unwrap();
        assert!(sender.accepts_gateway_event(&event(current)));
        sender.disconnect_connection(current).unwrap();
        assert!(!sender.accepts_gateway_event(&event(current)));
    }

    #[test]
    fn replacement_cancellation_keeps_active_generation_and_superseded_completion_cannot_commit() {
        let client = GatewayWsClient::new();
        let sender = client.command_sender();
        let first = sender.begin_connection_attempt().unwrap();
        let active = first.complete(None).unwrap();
        let replacement = sender.begin_connection_attempt().unwrap();
        let abandoned = replacement.id;
        assert!(sender.accepts_gateway_event(&event(active)));
        assert!(sender.accepts_gateway_event(&event(abandoned)));
        drop(replacement);
        assert!(!sender.accepts_gateway_event(&event(abandoned)));
        assert!(sender.accepts_gateway_event(&event(active)));
        let stale = sender.begin_connection_attempt().unwrap();
        let latest = sender.begin_connection_attempt().unwrap();
        assert!(stale.complete(None).is_err());
        let current = latest.complete(None).unwrap();
        assert!(!sender.accepts_gateway_event(&event(active)));
        assert!(sender.accepts_gateway_event(&event(current)));
        sender.disconnect().unwrap();
        assert!(!sender.accepts_gateway_event(&event(current)));
        let retiring = GatewayWsEvent::Disconnected {
            connection_id: current,
            endpoint_id: "synthetic".into(),
            endpoint_name: "Synthetic".into(),
            endpoint_kind: crate::gateway::types::GatewayEndpointKind::Remote,
            gateway_base_url: crate::gateway::endpoint::GatewayBaseUrl::parse_presentation(
                "https://gateway.invalid",
            )
            .unwrap(),
            reason: "synthetic disconnect".into(),
        };
        assert!(!sender.gateway_state_event_is_current(&retiring));
        assert!(sender.accepts_gateway_event(&retiring));
        assert!(!sender.accepts_gateway_event(&retiring));
    }
}
