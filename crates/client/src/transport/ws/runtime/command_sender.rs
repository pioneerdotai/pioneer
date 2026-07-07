use super::*;
use crate::composer::turn_prepare as client_composer_turn_prepare;
use crate::transport::ws::command_sender as client_ws_commands;
use crate::{
    artifacts::{
        actions::ArtifactCachedDownloadClient,
        download::{
            self as client_artifact_download, ArtifactDownloadChunkWaiter,
            ArtifactDownloadFileCache, ArtifactDownloadRequest, ArtifactDownloadResult,
            ArtifactDownloadTransport,
        },
        upload::ArtifactUploadTransport,
    },
    platform::ClientFileSystem,
    skills::catalog::SkillSnapshotTransport,
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

impl ArtifactDownloadTransport for GatewayWsCommandSender {
    fn artifact_download_start(
        &self,
        params: ArtifactDownloadStartParams,
    ) -> Result<ArtifactDownloadStartResponse> {
        GatewayWsCommandSender::artifact_download_start(self, params)
    }

    fn register_artifact_download_chunk(
        &self,
        download_id: &str,
        offset: u64,
    ) -> Result<Box<dyn ArtifactDownloadChunkWaiter>> {
        Ok(Box::new(GatewayWsArtifactDownloadChunkWaiter {
            response_rx: GatewayWsCommandSender::register_artifact_download_chunk(
                self,
                download_id,
                offset,
            )?,
        }))
    }

    fn artifact_download_chunk(
        &self,
        params: ArtifactDownloadChunkParams,
    ) -> Result<ArtifactDownloadChunkResponse> {
        GatewayWsCommandSender::artifact_download_chunk(self, params)
    }

    fn artifact_download_finish(
        &self,
        params: ArtifactDownloadFinishParams,
    ) -> Result<ArtifactDownloadFinishResponse> {
        GatewayWsCommandSender::artifact_download_finish(self, params)
    }

    fn artifact_download_abort(
        &self,
        params: ArtifactDownloadAbortParams,
    ) -> Result<ArtifactDownloadAbortResponse> {
        GatewayWsCommandSender::artifact_download_abort(self, params)
    }
}

impl ArtifactCachedDownloadClient for GatewayWsCommandSender {
    fn download_artifact_to_cache(
        &self,
        request: ArtifactDownloadRequest,
    ) -> Result<ArtifactDownloadResult> {
        GatewayWsCommandSender::download_artifact_to_cache(self, request)
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
    pub fn connect_and_wait(&self, spec: GatewayWsConnectSpec) -> Result<u64> {
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed) + 1;

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

        result.map(|_| connection_id).map_err(anyhow::Error::msg)
    }

    pub fn connect_with_retry(&self, spec: GatewayWsConnectSpec) -> Result<u64> {
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed) + 1;

        self.command_tx
            .send(GatewayWsCommand::Connect {
                connection_id,
                spec,
                initial_result_tx: None,
                retry_initial_failure: true,
            })
            .map_err(|_| anyhow!("websocket worker is not available"))?;

        Ok(connection_id)
    }

    pub fn shutdown(&self) -> Result<()> {
        self.command_tx
            .send(GatewayWsCommand::Shutdown)
            .map_err(|_| anyhow!("websocket worker is not available"))
    }

    pub fn disconnect(&self) -> Result<()> {
        self.command_tx
            .send(GatewayWsCommand::Disconnect)
            .map_err(|_| anyhow!("websocket worker is not available"))
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

    pub fn provider_set_api_key(
        &self,
        params: ProviderSetApiKeyParams,
    ) -> Result<ProviderSetApiKeyResponse> {
        client_ws_commands::provider_set_api_key(self, params)
    }

    pub fn provider_delete_api_key(
        &self,
        params: ProviderDeleteApiKeyParams,
    ) -> Result<ProviderDeleteApiKeyResponse> {
        client_ws_commands::provider_delete_api_key(self, params)
    }

    pub fn turn_get(&self, params: TurnGetParams) -> Result<TurnGetResponse> {
        client_ws_commands::turn_get(self, params)
    }

    pub fn turn_items(&self, params: TurnItemsParams) -> Result<TurnItemsResponse> {
        client_ws_commands::turn_items(self, params)
    }

    pub fn turn_work_page(&self, params: TurnWorkPageParams) -> Result<TurnWorkPageResponse> {
        client_ws_commands::turn_work_page(self, params)
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

    pub fn artifact_read(&self, params: ArtifactReadParams) -> Result<ArtifactReadResponse> {
        client_ws_commands::artifact_read(self, params)
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

    pub fn artifact_download_start(
        &self,
        params: ArtifactDownloadStartParams,
    ) -> Result<ArtifactDownloadStartResponse> {
        client_ws_commands::artifact_download_start(self, params)
    }

    pub fn artifact_download_chunk(
        &self,
        params: ArtifactDownloadChunkParams,
    ) -> Result<ArtifactDownloadChunkResponse> {
        client_ws_commands::artifact_download_chunk(self, params)
    }

    pub fn artifact_download_finish(
        &self,
        params: ArtifactDownloadFinishParams,
    ) -> Result<ArtifactDownloadFinishResponse> {
        client_ws_commands::artifact_download_finish(self, params)
    }

    pub fn artifact_download_abort(
        &self,
        params: ArtifactDownloadAbortParams,
    ) -> Result<ArtifactDownloadAbortResponse> {
        client_ws_commands::artifact_download_abort(self, params)
    }
    pub fn download_artifact_to_cache(
        &self,
        request: ArtifactDownloadRequest,
    ) -> Result<ArtifactDownloadResult> {
        let runtime_home = self
            .artifact_cache_root
            .lock()
            .map_err(|_| anyhow!("artifact cache root lock is poisoned"))?
            .clone()
            .ok_or_else(|| anyhow!("artifact cache root is not configured"))?;
        self.download_artifact_to_cache_with_runtime_home(request, runtime_home)
    }

    pub fn download_artifact_to_cache_with_runtime_home(
        &self,
        request: ArtifactDownloadRequest,
        runtime_home: PathBuf,
    ) -> Result<ArtifactDownloadResult> {
        client_artifact_download::download_artifact_to_cache(
            self,
            &ArtifactDownloadFileCache::new(runtime_home),
            request,
        )
    }

    pub fn set_artifact_cache_root(&self, runtime_home: PathBuf) -> Result<()> {
        *self
            .artifact_cache_root
            .lock()
            .map_err(|_| anyhow!("artifact cache root lock is poisoned"))? = Some(runtime_home);
        Ok(())
    }

    fn register_artifact_download_chunk(
        &self,
        download_id: &str,
        offset: u64,
    ) -> Result<Receiver<std::result::Result<ArtifactDownloadChunkPayload, String>>> {
        let (response_tx, response_rx) = mpsc::channel();
        let (registered_tx, registered_rx) = mpsc::channel();
        self.command_tx
            .send(GatewayWsCommand::ArtifactDownloadRegisterChunk {
                download_id: download_id.to_owned(),
                offset,
                response_tx,
                registered_tx,
            })
            .map_err(|_| anyhow!("websocket worker is not available"))?;
        registered_rx
            .recv_timeout(RPC_REQUEST_TIMEOUT)
            .map_err(|_| anyhow!("timed out registering artifact download chunk waiter"))?
            .map_err(anyhow::Error::msg)?;
        Ok(response_rx)
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

struct GatewayWsArtifactDownloadChunkWaiter {
    response_rx: Receiver<std::result::Result<ArtifactDownloadChunkPayload, String>>,
}

impl ArtifactDownloadChunkWaiter for GatewayWsArtifactDownloadChunkWaiter {
    fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<crate::transport::ws::download::ArtifactDownloadChunkPayload> {
        self.response_rx
            .recv_timeout(timeout)
            .map_err(|_| anyhow!("timed out waiting for artifact download chunk"))?
            .map_err(anyhow::Error::msg)
    }
}
