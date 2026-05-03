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
