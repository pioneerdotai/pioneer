use super::*;

impl MessageProcessor {
    pub(crate) async fn mcp_server_restart(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: McpServerRestartParams,
    ) {
        let workspace_id = match self
            .validate_mcp_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::MCP_SERVER_RESTART,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };

        if params.name.trim().is_empty() {
            self.send_error(
                connection_id,
                mcp_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    MCP_ERROR_INVALID_REQUEST,
                    "MCP server name is required",
                    json!({"name": params.name}),
                ),
            )
            .await;
            return;
        }

        let scope_kind = match McpScopeKind::from_str(params.scope_kind.as_str()) {
            Ok(scope_kind) => scope_kind,
            Err(error) => {
                self.send_error(
                    connection_id,
                    mcp_error(
                        Some(request_id.clone()),
                        INVALID_PARAMS_CODE,
                        MCP_ERROR_INVALID_REQUEST,
                        "invalid MCP scope kind",
                        json!({"error": error}),
                    ),
                )
                .await;
                return;
            }
        };
        let scope_key = match &scope_kind {
            McpScopeKind::Workspace => workspace_id.clone(),
            McpScopeKind::User => "default".to_owned(),
        };

        let row = match self
            .mcp_service
            .restart_server(scope_kind.as_str(), scope_key.as_str(), params.name.trim())
            .await
        {
            Ok(Some(row)) => row,
            Ok(None) => {
                self.send_error(
                    connection_id,
                    mcp_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        MCP_ERROR_NOT_FOUND,
                        "MCP server installation was not found",
                        json!({
                            "scope_kind": params.scope_kind,
                            "scope_key": scope_key,
                            "name": params.name,
                        }),
                    ),
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    mcp_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INTERNAL,
                        "failed to restart MCP server",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };

        let catalog = match row.id.as_deref() {
            Some(server_installation_id) => match self
                .crud_store
                .find_mcp_server_catalog_snapshot(server_installation_id)
                .await
            {
                Ok(catalog) => catalog,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        mcp_error(
                            Some(request_id.clone()),
                            INVALID_REQUEST_CODE,
                            MCP_ERROR_INTERNAL,
                            "failed to load MCP catalog snapshot",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            },
            None => None,
        };
        let runtime_snapshots = self
            .mcp_service
            .runtime_snapshot(scope_kind.as_str(), scope_key.as_str())
            .await;
        let runtime = row.id.as_deref().and_then(|id| runtime_snapshots.get(id));
        let server =
            match list_item_from_record_with_catalog_and_runtime(&row, catalog.as_ref(), runtime) {
                Ok(server) => server,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        mcp_error(
                            Some(request_id.clone()),
                            INVALID_REQUEST_CODE,
                            MCP_ERROR_INTERNAL,
                            "failed to map MCP server installation",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            };
        let payload = McpServerRestartResponse {
            accepted: true,
            server,
        };
        let response = match JsonRpcResponse::from_result(request_id.clone(), &payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    mcp_error(
                        None,
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INTERNAL,
                        "failed to encode mcp/server/restart response",
                        json!({"error": format!("{error:#}")}),
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
                "failed to send mcp/server/restart response"
            );
        }
    }
}
