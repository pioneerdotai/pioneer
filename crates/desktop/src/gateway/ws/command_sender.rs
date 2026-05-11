use super::*;

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
        if params.thread_id.trim().is_empty() {
            return Err(anyhow!("thread_id is required for thread/start"));
        }
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for thread/start"));
        }

        self.send_request_typed(methods::THREAD_START, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn thread_tree(&self, params: ThreadTreeParams) -> Result<ThreadTreeResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for thread/tree"));
        }

        self.send_request_typed(methods::THREAD_TREE, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn thread_move(&self, params: ThreadMoveParams) -> Result<ThreadMoveResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for thread/move"));
        }
        if params.thread_id.trim().is_empty() {
            return Err(anyhow!("thread_id is required for thread/move"));
        }

        self.send_request_typed(methods::THREAD_MOVE, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn thread_folder_create(
        &self,
        params: ThreadFolderCreateParams,
    ) -> Result<ThreadFolderCreateResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for thread/folder/create"));
        }
        if params.name.trim().is_empty() {
            return Err(anyhow!("name is required for thread/folder/create"));
        }

        self.send_request_typed(methods::THREAD_FOLDER_CREATE, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn thread_folder_move(
        &self,
        params: ThreadFolderMoveParams,
    ) -> Result<ThreadFolderMoveResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for thread/folder/move"));
        }
        if params.folder_id.trim().is_empty() {
            return Err(anyhow!("folder_id is required for thread/folder/move"));
        }

        self.send_request_typed(methods::THREAD_FOLDER_MOVE, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn thread_folder_delete(
        &self,
        params: ThreadFolderDeleteParams,
    ) -> Result<ThreadFolderDeleteResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for thread/folder/delete"));
        }
        if params.folder_id.trim().is_empty() {
            return Err(anyhow!("folder_id is required for thread/folder/delete"));
        }

        self.send_request_typed(methods::THREAD_FOLDER_DELETE, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn thread_history(&self, params: ThreadHistoryParams) -> Result<ThreadHistoryResponse> {
        if params.thread_id.trim().is_empty() {
            return Err(anyhow!("thread_id is required for thread/history"));
        }

        self.send_request_typed(methods::THREAD_HISTORY, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn workspace_default(&self) -> Result<WorkspaceDefaultResponse> {
        self.send_request_typed(
            methods::WORKSPACE_DEFAULT,
            &WorkspaceDefaultParams::default(),
            RPC_REQUEST_TIMEOUT,
        )
    }

    pub fn thread_unsubscribe(&self, thread_id: String) -> Result<ThreadUnsubscribeResponse> {
        self.send_request_typed(
            methods::THREAD_UNSUBSCRIBE,
            &ThreadUnsubscribeParams { thread_id },
            RPC_UNSUBSCRIBE_TIMEOUT,
        )
    }

    pub fn turn_start(&self, params: TurnStartParams) -> Result<TurnStartResponse> {
        if params.thread_id.trim().is_empty() {
            return Err(anyhow!("thread_id is required for turn/start"));
        }
        if params.turn_id.trim().is_empty() {
            return Err(anyhow!("turn_id is required for turn/start"));
        }

        self.send_request_typed(methods::TURN_START, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn turn_cancel(&self, params: TurnCancelParams) -> Result<TurnCancelResponse> {
        if params.thread_id.trim().is_empty() {
            return Err(anyhow!("thread_id is required for turn/cancel"));
        }
        if params.turn_id.trim().is_empty() {
            return Err(anyhow!("turn_id is required for turn/cancel"));
        }

        self.send_request_typed(methods::TURN_CANCEL, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn provider_list(&self) -> Result<ProviderListResponse> {
        self.send_request_typed(
            methods::PROVIDER_LIST,
            &ProviderListParams {},
            RPC_REQUEST_TIMEOUT,
        )
    }

    pub fn provider_list_models(
        &self,
        params: ProviderListModelsParams,
    ) -> Result<ProviderListModelsResponse> {
        if params.provider.trim().is_empty() {
            return Err(anyhow!("provider is required for provider/list_models"));
        }

        self.send_request_typed(
            methods::PROVIDER_MODELS_LIST,
            &params,
            Duration::from_secs(30),
        )
    }

    pub fn provider_set_api_key(
        &self,
        params: ProviderSetApiKeyParams,
    ) -> Result<ProviderSetApiKeyResponse> {
        if params.provider.trim().is_empty() {
            return Err(anyhow!("provider is required for provider/set_api_key"));
        }
        if params.api_key.trim().is_empty() {
            return Err(anyhow!("api_key is required for provider/set_api_key"));
        }

        self.send_request_typed(methods::PROVIDER_SET_API_KEY, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn provider_delete_api_key(
        &self,
        params: ProviderDeleteApiKeyParams,
    ) -> Result<ProviderDeleteApiKeyResponse> {
        if params.provider.trim().is_empty() {
            return Err(anyhow!("provider is required for provider/delete_api_key"));
        }

        self.send_request_typed(
            methods::PROVIDER_DELETE_API_KEY,
            &params,
            RPC_REQUEST_TIMEOUT,
        )
    }

    pub fn turn_get(&self, params: TurnGetParams) -> Result<TurnGetResponse> {
        self.send_request_typed(methods::TURN_GET, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn turn_items(&self, params: TurnItemsParams) -> Result<TurnItemsResponse> {
        self.send_request_typed(methods::TURN_ITEMS, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn turn_timeline(&self, params: TurnTimelineParams) -> Result<TurnTimelineResponse> {
        if params.thread_id.trim().is_empty() {
            return Err(anyhow!("thread_id is required for turn/timeline"));
        }
        if params.turn_id.trim().is_empty() {
            return Err(anyhow!("turn_id is required for turn/timeline"));
        }

        self.send_request_typed(methods::TURN_TIMELINE, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn skills_list(&self, params: SkillListParams) -> Result<SkillListResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for skills/list"));
        }

        self.send_request_typed(methods::SKILLS_LIST, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn mcp_list(&self, params: McpListParams) -> Result<McpListResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for mcp/list"));
        }

        self.send_request_typed(methods::MCP_LIST, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn mcp_install(&self, params: McpInstallParams) -> Result<McpInstallResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for mcp/install"));
        }
        if params.config_json.trim().is_empty() {
            return Err(anyhow!("config_json is required for mcp/install"));
        }

        self.send_request_typed(methods::MCP_INSTALL, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn mcp_policy_set(&self, params: McpPolicySetParams) -> Result<McpPolicySetResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for mcp/policy/set"));
        }
        if params.name.trim().is_empty() {
            return Err(anyhow!("name is required for mcp/policy/set"));
        }

        self.send_request_typed(methods::MCP_POLICY_SET, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn mcp_server_restart(
        &self,
        params: McpServerRestartParams,
    ) -> Result<McpServerRestartResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for mcp/server/restart"));
        }
        if params.name.trim().is_empty() {
            return Err(anyhow!("name is required for mcp/server/restart"));
        }

        self.send_request_typed(methods::MCP_SERVER_RESTART, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn mcp_uninstall(&self, params: McpUninstallParams) -> Result<McpUninstallResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for mcp/uninstall"));
        }
        if params.name.trim().is_empty() {
            return Err(anyhow!("name is required for mcp/uninstall"));
        }

        self.send_request_typed(methods::MCP_UNINSTALL, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn mcp_server_details(
        &self,
        params: McpServerDetailsParams,
    ) -> Result<McpServerDetailsResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for mcp/server/details"));
        }
        if params.server_id.trim().is_empty() {
            return Err(anyhow!("server_id is required for mcp/server/details"));
        }

        self.send_request_typed(methods::MCP_SERVER_DETAILS, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn skills_install(&self, params: SkillsInstallParams) -> Result<SkillsInstallResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for skills/install"));
        }

        self.send_request_typed(methods::SKILLS_INSTALL, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn skills_upload_start(
        &self,
        params: SkillsUploadStartParams,
    ) -> Result<SkillsUploadStartResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for skills/upload/start"));
        }
        if params.file_name.trim().is_empty() {
            return Err(anyhow!("file_name is required for skills/upload/start"));
        }
        if params.compressed_size_bytes == 0 {
            return Err(anyhow!(
                "compressed_size_bytes must be positive for skills/upload/start"
            ));
        }
        if params.sha256.trim().is_empty() {
            return Err(anyhow!("sha256 is required for skills/upload/start"));
        }

        self.send_request_typed(methods::SKILLS_UPLOAD_START, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn skills_upload_finish(
        &self,
        params: SkillsUploadFinishParams,
    ) -> Result<SkillsUploadFinishResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for skills/upload/finish"));
        }
        if params.upload_id.trim().is_empty() {
            return Err(anyhow!("upload_id is required for skills/upload/finish"));
        }

        self.send_request_typed(methods::SKILLS_UPLOAD_FINISH, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn skills_upload_abort(
        &self,
        params: SkillsUploadAbortParams,
    ) -> Result<SkillsUploadAbortResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for skills/upload/abort"));
        }
        if params.upload_id.trim().is_empty() {
            return Err(anyhow!("upload_id is required for skills/upload/abort"));
        }

        self.send_request_typed(methods::SKILLS_UPLOAD_ABORT, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn artifact_capabilities(
        &self,
        params: ArtifactCapabilitiesParams,
    ) -> Result<ArtifactCapabilitiesResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!(
                "workspace_id is required for artifact/capabilities"
            ));
        }
        self.send_request_typed(methods::ARTIFACT_CAPABILITIES, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn artifact_list_for_thread(
        &self,
        params: ArtifactListForThreadParams,
    ) -> Result<ArtifactListResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for artifact/list/thread"));
        }
        if params
            .thread_id
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .is_empty()
        {
            return Err(anyhow!("thread_id is required for artifact/list/thread"));
        }

        self.send_request_typed(
            methods::ARTIFACT_LIST_FOR_THREAD,
            &params,
            RPC_REQUEST_TIMEOUT,
        )
    }

    pub fn artifact_read(&self, params: ArtifactReadParams) -> Result<ArtifactReadResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for artifact/read"));
        }
        if params.artifact_id.trim().is_empty() {
            return Err(anyhow!("artifact_id is required for artifact/read"));
        }

        self.send_request_typed(methods::ARTIFACT_READ, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn artifact_download_start(
        &self,
        params: ArtifactDownloadStartParams,
    ) -> Result<ArtifactDownloadStartResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!(
                "workspace_id is required for artifact/download/start"
            ));
        }
        if params.artifact_id.trim().is_empty() {
            return Err(anyhow!(
                "artifact_id is required for artifact/download/start"
            ));
        }

        self.send_request_typed(
            methods::ARTIFACT_DOWNLOAD_START,
            &params,
            RPC_REQUEST_TIMEOUT,
        )
    }

    pub fn artifact_download_chunk(
        &self,
        params: ArtifactDownloadChunkParams,
    ) -> Result<ArtifactDownloadChunkResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!(
                "workspace_id is required for artifact/download/chunk"
            ));
        }
        if params.download_id.trim().is_empty() {
            return Err(anyhow!(
                "download_id is required for artifact/download/chunk"
            ));
        }
        if params.len == 0 {
            return Err(anyhow!("len must be positive for artifact/download/chunk"));
        }

        self.send_request_typed(
            methods::ARTIFACT_DOWNLOAD_CHUNK,
            &params,
            RPC_REQUEST_TIMEOUT,
        )
    }

    pub fn artifact_download_finish(
        &self,
        params: ArtifactDownloadFinishParams,
    ) -> Result<ArtifactDownloadFinishResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!(
                "workspace_id is required for artifact/download/finish"
            ));
        }
        if params.download_id.trim().is_empty() {
            return Err(anyhow!(
                "download_id is required for artifact/download/finish"
            ));
        }

        self.send_request_typed(
            methods::ARTIFACT_DOWNLOAD_FINISH,
            &params,
            RPC_REQUEST_TIMEOUT,
        )
    }

    pub fn artifact_download_abort(
        &self,
        params: ArtifactDownloadAbortParams,
    ) -> Result<ArtifactDownloadAbortResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!(
                "workspace_id is required for artifact/download/abort"
            ));
        }
        if params.download_id.trim().is_empty() {
            return Err(anyhow!(
                "download_id is required for artifact/download/abort"
            ));
        }

        self.send_request_typed(
            methods::ARTIFACT_DOWNLOAD_ABORT,
            &params,
            RPC_REQUEST_TIMEOUT,
        )
    }

    #[allow(dead_code)]
    pub fn download_artifact_to_cache(
        &self,
        request: DesktopArtifactDownloadRequest,
    ) -> Result<DesktopArtifactDownloadResult> {
        let runtime_home = state::runtime_home_dir()?;
        self.download_artifact_to_cache_with_runtime_home(request, runtime_home)
    }

    pub(super) fn download_artifact_to_cache_with_runtime_home(
        &self,
        request: DesktopArtifactDownloadRequest,
        runtime_home: PathBuf,
    ) -> Result<DesktopArtifactDownloadResult> {
        validate_artifact_download_request(&request)?;
        let _ =
            prune_artifact_download_cache(runtime_home.as_path(), ARTIFACT_DOWNLOAD_CACHE_MAX_AGE);
        let start = self.artifact_download_start(ArtifactDownloadStartParams {
            workspace_id: request.workspace_id.clone(),
            artifact_id: request.artifact_id.clone(),
            version_id: request.version_id.clone(),
            preferred_chunk_size_bytes: None,
        })?;
        let version_id = start
            .artifact
            .version_id
            .clone()
            .or(request.version_id.clone())
            .unwrap_or_else(|| "latest".to_owned());
        let cache_paths = build_artifact_download_cache_path(
            runtime_home.as_path(),
            request.gateway_profile_id.as_str(),
            request.workspace_id.as_str(),
            request.artifact_id.as_str(),
            version_id.as_str(),
            start.file_name.as_str(),
        )?;
        let result = self.download_artifact_to_cache_inner(&request, &start, &cache_paths);
        if result.is_err() {
            let _ = self.artifact_download_abort(ArtifactDownloadAbortParams {
                workspace_id: request.workspace_id,
                download_id: start.download_id,
            });
            let _ = fs::remove_file(cache_paths.part_path.as_path());
        }
        result
    }

    fn download_artifact_to_cache_inner(
        &self,
        request: &DesktopArtifactDownloadRequest,
        start: &ArtifactDownloadStartResponse,
        cache_paths: &ArtifactDownloadCachePaths,
    ) -> Result<DesktopArtifactDownloadResult> {
        if let Some(parent) = cache_paths.part_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create artifact download cache {}",
                    parent.display()
                )
            })?;
        }
        let _ = fs::remove_file(cache_paths.part_path.as_path());
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(cache_paths.part_path.as_path())
            .with_context(|| {
                format!(
                    "failed to create artifact download part file {}",
                    cache_paths.part_path.display()
                )
            })?;

        let mut offset = 0_u64;
        let chunk_size = start
            .recommended_chunk_size_bytes
            .min(start.max_chunk_size_bytes)
            .max(1);
        let version_id = start
            .artifact
            .version_id
            .as_deref()
            .or(request.version_id.as_deref())
            .unwrap_or("latest");
        while offset < start.size_bytes {
            let len = (start.size_bytes - offset).min(chunk_size);
            let frame_rx =
                self.register_artifact_download_chunk(start.download_id.as_str(), offset)?;
            let response = self.artifact_download_chunk(ArtifactDownloadChunkParams {
                workspace_id: request.workspace_id.clone(),
                download_id: start.download_id.clone(),
                offset,
                len,
            })?;
            if !response.queued || response.offset != offset || response.len != len {
                return Err(anyhow!("artifact/download/chunk returned an invalid range"));
            }
            let payload = frame_rx
                .recv_timeout(DOWNLOAD_CHUNK_TIMEOUT)
                .map_err(|_| anyhow!("timed out waiting for artifact download chunk"))?
                .map_err(anyhow::Error::msg)?;
            validate_artifact_download_chunk_payload(
                &payload,
                request.workspace_id.as_str(),
                start.download_id.as_str(),
                request.artifact_id.as_str(),
                version_id,
                offset,
                len,
                start.size_bytes,
            )?;
            write_chunk_at(&mut file, offset, payload.bytes.as_slice())?;
            offset = offset.saturating_add(len);
        }
        file.sync_data()
            .context("failed to sync artifact download")?;
        drop(file);

        let actual_size = fs::metadata(cache_paths.part_path.as_path())
            .with_context(|| {
                format!(
                    "failed to stat artifact download part file {}",
                    cache_paths.part_path.display()
                )
            })?
            .len();
        if actual_size != start.size_bytes {
            return Err(anyhow!(
                "artifact download size mismatch: expected {}, got {}",
                start.size_bytes,
                actual_size
            ));
        }
        let actual_sha256 = sha256_file(cache_paths.part_path.as_path())?;
        if actual_sha256 != start.sha256 {
            return Err(anyhow!("artifact download sha256 mismatch"));
        }

        let _ = fs::remove_file(cache_paths.final_path.as_path());
        fs::rename(
            cache_paths.part_path.as_path(),
            cache_paths.final_path.as_path(),
        )
        .with_context(|| {
            format!(
                "failed to finalize artifact download {}",
                cache_paths.final_path.display()
            )
        })?;
        self.artifact_download_finish(ArtifactDownloadFinishParams {
            workspace_id: request.workspace_id.clone(),
            download_id: start.download_id.clone(),
        })?;
        Ok(DesktopArtifactDownloadResult {
            local_path: cache_paths.final_path.clone(),
            artifact: start.artifact.clone(),
            size_bytes: start.size_bytes,
            sha256: start.sha256.clone(),
        })
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
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!(
                "workspace_id is required for artifact/upload/start"
            ));
        }
        if params.file_name.trim().is_empty() {
            return Err(anyhow!("file_name is required for artifact/upload/start"));
        }
        if params.client_attachment_id.trim().is_empty() {
            return Err(anyhow!(
                "client_attachment_id is required for artifact/upload/start"
            ));
        }
        if params.sha256.trim().is_empty() {
            return Err(anyhow!("sha256 is required for artifact/upload/start"));
        }

        self.send_request_typed(methods::ARTIFACT_UPLOAD_START, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn artifact_upload_finish(
        &self,
        params: ArtifactUploadFinishParams,
    ) -> Result<ArtifactUploadFinishResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!(
                "workspace_id is required for artifact/upload/finish"
            ));
        }
        if params.upload_id.trim().is_empty() {
            return Err(anyhow!("upload_id is required for artifact/upload/finish"));
        }

        self.send_request_typed(
            methods::ARTIFACT_UPLOAD_FINISH,
            &params,
            RPC_REQUEST_TIMEOUT,
        )
    }

    pub fn artifact_upload_abort(
        &self,
        params: ArtifactUploadAbortParams,
    ) -> Result<ArtifactUploadAbortResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!(
                "workspace_id is required for artifact/upload/abort"
            ));
        }
        if params.upload_id.trim().is_empty() {
            return Err(anyhow!("upload_id is required for artifact/upload/abort"));
        }

        self.send_request_typed(methods::ARTIFACT_UPLOAD_ABORT, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn send_artifact_upload_chunk(
        &self,
        workspace_id: String,
        upload_id: String,
        offset: u64,
        chunk: Vec<u8>,
    ) -> Result<ArtifactUploadChunkAckNotification> {
        if workspace_id.trim().is_empty() {
            return Err(anyhow!(
                "workspace_id is required for artifact upload chunk"
            ));
        }
        if upload_id.trim().is_empty() {
            return Err(anyhow!("upload_id is required for artifact upload chunk"));
        }
        if chunk.is_empty() {
            return Err(anyhow!("artifact upload chunk cannot be empty"));
        }

        let payload =
            encode_artifact_upload_chunk_frame(workspace_id, upload_id.clone(), offset, &chunk)?;

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

    pub fn upload_artifact_file(
        &self,
        request: DesktopArtifactUploadRequest,
    ) -> Result<ArtifactRef> {
        if request.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for artifact file upload"));
        }
        if request.client_attachment_id.trim().is_empty() {
            return Err(anyhow!(
                "client_attachment_id is required for artifact file upload"
            ));
        }
        let metadata = fs::metadata(request.path.as_path())
            .with_context(|| format!("failed to stat `{}`", request.path.display()))?;
        if !metadata.is_file() {
            return Err(anyhow!("artifact upload path is not a regular file"));
        }
        let file_name = request
            .path
            .file_name()
            .and_then(|value| value.to_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow!("artifact upload path has no file name"))?;
        let size_bytes = metadata.len();
        let sha256 = sha256_file(request.path.as_path())?;
        let mime_type = request.mime_type.clone().or_else(|| {
            mime_guess::from_path(request.path.as_path())
                .first()
                .map(|mime| mime.essence_str().to_owned())
        });

        let start = self.artifact_upload_start(ArtifactUploadStartParams {
            workspace_id: request.workspace_id.clone(),
            thread_id: request.thread_id.clone(),
            planned_turn_id: request.planned_turn_id.clone(),
            client_attachment_id: request.client_attachment_id.clone(),
            file_name,
            mime_type,
            size_bytes,
            sha256,
            source_kind: ArtifactUploadSourceKind::UserComposer,
        })?;

        let result = self.upload_artifact_file_chunks_and_finish(&request, &start);
        if result.is_err() {
            let _ = self.artifact_upload_abort(ArtifactUploadAbortParams {
                workspace_id: request.workspace_id,
                upload_id: start.upload_id,
            });
        }
        result
    }

    fn upload_artifact_file_chunks_and_finish(
        &self,
        request: &DesktopArtifactUploadRequest,
        start: &ArtifactUploadStartResponse,
    ) -> Result<ArtifactRef> {
        let chunk_size = usize::try_from(
            start
                .recommended_chunk_size_bytes
                .min(start.max_chunk_size_bytes)
                .max(1),
        )
        .unwrap_or(256 * 1024);
        let mut file = fs::File::open(request.path.as_path())
            .with_context(|| format!("failed to open `{}`", request.path.display()))?;
        let mut offset = 0_u64;
        let mut buffer = vec![0_u8; chunk_size];
        loop {
            let read = file
                .read(buffer.as_mut_slice())
                .with_context(|| format!("failed to read `{}`", request.path.display()))?;
            if read == 0 {
                break;
            }
            let chunk = buffer[..read].to_vec();
            let ack = self.send_artifact_upload_chunk(
                request.workspace_id.clone(),
                start.upload_id.clone(),
                offset,
                chunk,
            )?;
            offset = ack.next_offset;
        }

        let finish = self.artifact_upload_finish(ArtifactUploadFinishParams {
            workspace_id: request.workspace_id.clone(),
            upload_id: start.upload_id.clone(),
        })?;
        Ok(finish.artifact)
    }

    pub fn send_skill_upload_chunk(
        &self,
        workspace_id: String,
        upload_id: String,
        offset: u64,
        chunk: Vec<u8>,
    ) -> Result<SkillsUploadChunkAckNotification> {
        if workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for skill upload chunk"));
        }
        if upload_id.trim().is_empty() {
            return Err(anyhow!("upload_id is required for skill upload chunk"));
        }
        if chunk.is_empty() {
            return Err(anyhow!("skill upload chunk cannot be empty"));
        }

        let header = SkillsUploadChunkHeader {
            workspace_id,
            upload_id: upload_id.clone(),
            offset,
            len: u64::try_from(chunk.len()).context("skill upload chunk length overflow")?,
            chunk_sha256: Some(hex::encode(Sha256::digest(chunk.as_slice()))),
        };
        let header_bytes =
            serde_json::to_vec(&header).context("failed to encode skill upload chunk header")?;
        let header_len =
            u32::try_from(header_bytes.len()).context("skill upload chunk header is too large")?;

        let mut payload =
            Vec::with_capacity(UPLOAD_FRAME_MAGIC.len() + 4 + header_bytes.len() + chunk.len());
        payload.extend_from_slice(UPLOAD_FRAME_MAGIC);
        payload.extend_from_slice(&header_len.to_be_bytes());
        payload.extend_from_slice(header_bytes.as_slice());
        payload.extend_from_slice(chunk.as_slice());

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
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for skills/update"));
        }
        if params.slug.trim().is_empty() {
            return Err(anyhow!("slug is required for skills/update"));
        }
        if !is_qualified_skill_slug(params.slug.as_str()) {
            return Err(anyhow!("slug must use owner/slug for skills/update"));
        }

        self.send_request_typed(methods::SKILLS_UPDATE, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn skills_uninstall(
        &self,
        params: SkillsUninstallParams,
    ) -> Result<SkillsUninstallResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for skills/uninstall"));
        }
        if params.slug.trim().is_empty() {
            return Err(anyhow!("slug is required for skills/uninstall"));
        }
        if !is_qualified_skill_slug(params.slug.as_str()) {
            return Err(anyhow!("slug must use owner/slug for skills/uninstall"));
        }

        self.send_request_typed(methods::SKILLS_UNINSTALL, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn skills_health(&self, params: SkillsHealthParams) -> Result<SkillsHealthResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for skills/health"));
        }
        if params
            .skills
            .iter()
            .any(|target| !is_qualified_skill_slug(target.slug.as_str()))
        {
            return Err(anyhow!("skills/health targets must use owner/slug in slug"));
        }

        self.send_request_typed(methods::SKILLS_HEALTH, &params, RPC_REQUEST_TIMEOUT)
    }

    pub fn skills_policy_set(
        &self,
        params: SkillsPolicySetParams,
    ) -> Result<SkillsPolicySetResponse> {
        if params.workspace_id.trim().is_empty() {
            return Err(anyhow!("workspace_id is required for skills/policy/set"));
        }
        if params.skill_slug.trim().is_empty() {
            return Err(anyhow!("skill_slug is required for skills/policy/set"));
        }
        if !is_qualified_skill_slug(params.skill_slug.as_str()) {
            return Err(anyhow!(
                "skill_slug must use owner/slug for skills/policy/set"
            ));
        }
        if params.source_kind.trim().is_empty() {
            return Err(anyhow!("source_kind is required for skills/policy/set"));
        }

        self.send_request_typed(methods::SKILLS_POLICY_SET, &params, RPC_REQUEST_TIMEOUT)
    }

    fn send_request_typed<T, P>(&self, method: &str, params: &P, timeout: Duration) -> Result<T>
    where
        T: DeserializeOwned,
        P: serde::Serialize,
    {
        let params_value =
            serde_json::to_value(params).context("failed to encode JSON-RPC params")?;

        let result = self.send_request_value(method, params_value, timeout)?;

        serde_json::from_value(result)
            .with_context(|| format!("failed to decode `{method}` response payload"))
    }

    fn send_request_value(
        &self,
        method: &str,
        params: JsonValue,
        timeout: Duration,
    ) -> Result<JsonValue> {
        let (request_id, payload) = self.build_request_payload(method, params)?;

        let (response_tx, response_rx) = mpsc::channel();

        self.command_tx
            .send(GatewayWsCommand::Request {
                request_id,
                payload,
                response_tx,
            })
            .map_err(|_| anyhow!("websocket worker is not available"))?;

        let response = response_rx
            .recv_timeout(timeout)
            .map_err(|_| anyhow!("timed out waiting for `{method}` response"))?;

        response.map_err(anyhow::Error::msg)
    }

    fn build_request_payload(&self, method: &str, params: JsonValue) -> Result<(String, String)> {
        let request_id = generate_id(REQUEST_ID_LEN);
        let request = JsonRpcRequest {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            id: RequestId::new(request_id.clone())
                .map_err(|error| anyhow!("failed to build request id: {error}"))?,
            method: method.to_owned(),
            params: Some(params),
        };
        let payload =
            serde_json::to_string(&request).context("failed to serialize JSON-RPC request")?;
        Ok((request_id, payload))
    }
}

pub(crate) fn encode_artifact_upload_chunk_frame(
    workspace_id: String,
    upload_id: String,
    offset: u64,
    chunk: &[u8],
) -> Result<Vec<u8>> {
    let header = ArtifactUploadChunkHeader {
        workspace_id,
        upload_id,
        offset,
        len: u64::try_from(chunk.len()).context("artifact upload chunk length overflow")?,
        chunk_sha256: Some(hex::encode(Sha256::digest(chunk))),
    };
    let header_bytes =
        serde_json::to_vec(&header).context("failed to encode artifact upload chunk header")?;
    let header_len =
        u32::try_from(header_bytes.len()).context("artifact upload chunk header is too large")?;

    let mut payload = Vec::with_capacity(
        ARTIFACT_UPLOAD_FRAME_MAGIC.len() + 4 + header_bytes.len() + chunk.len(),
    );
    payload.extend_from_slice(ARTIFACT_UPLOAD_FRAME_MAGIC);
    payload.extend_from_slice(&header_len.to_be_bytes());
    payload.extend_from_slice(header_bytes.as_slice());
    payload.extend_from_slice(chunk);
    Ok(payload)
}

fn sha256_file(path: &std::path::Path) -> Result<String> {
    let mut file =
        fs::File::open(path).with_context(|| format!("failed to open `{}`", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("failed to read `{}`", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn validate_artifact_download_request(request: &DesktopArtifactDownloadRequest) -> Result<()> {
    if request.gateway_profile_id.trim().is_empty() {
        return Err(anyhow!(
            "gateway_profile_id is required for artifact download"
        ));
    }
    if request.workspace_id.trim().is_empty() {
        return Err(anyhow!("workspace_id is required for artifact download"));
    }
    if request.artifact_id.trim().is_empty() {
        return Err(anyhow!("artifact_id is required for artifact download"));
    }
    Ok(())
}

fn write_chunk_at(file: &mut fs::File, offset: u64, bytes: &[u8]) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    file.seek(SeekFrom::Start(offset))
        .context("failed to seek artifact download part file")?;
    file.write_all(bytes)
        .context("failed to write artifact download chunk")?;
    Ok(())
}
