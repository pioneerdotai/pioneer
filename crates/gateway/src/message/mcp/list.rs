use super::*;

impl MessageProcessor {
    pub(crate) async fn mcp_list(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: McpListParams,
    ) {
        let connection_id = request_context.connection_id();
        let workspace_id = match self
            .validate_mcp_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::MCP_LIST,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };

        let rows = match self
            .crud_store
            .list_mcp_server_installations("workspace", workspace_id.as_str())
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                self.send_error(
                    connection_id,
                    mcp_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INTERNAL,
                        "failed to load MCP server installations",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };

        let runtime_snapshots = self
            .mcp_service
            .runtime_snapshot("workspace", workspace_id.as_str())
            .await;
        let member = crate::authorization::AuthorizationService::new().role_disclosure_policy(
            request_context.principal().kind,
            request_context.principal().role_key.as_ref(),
        ) == Some(crate::authorization::RoleDisclosurePolicy::Collaborator);
        let mut servers = Vec::with_capacity(rows.len());
        for row in &rows {
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
            let runtime = row.id.as_deref().and_then(|id| runtime_snapshots.get(id));
            let item = match list_item_from_record_with_catalog_and_runtime(
                row,
                catalog.as_ref(),
                runtime,
            ) {
                Ok(item) => item,
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
            // Discovery for an ordinary workspace member is a catalog of
            // usable capabilities, not a management inventory. Disabled
            // installations remain visible only to Gateway administrators.
            if member && !item.policy.enabled {
                continue;
            }
            if !crate::authorization::AuthorizationService::new().mcp_server_allowed(
                request_context.principal().kind,
                request_context.principal().role_key.as_ref(),
                item.id.as_str(),
            ) {
                continue;
            }
            servers.push(item);
        }
        let response_payload = McpListResponse {
            snapshot_version: self.current_mcp_snapshot_version(),
            generated_at: now_timestamp_secs(),
            servers,
        };

        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    mcp_error(
                        None,
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INTERNAL,
                        "failed to encode mcp/list response",
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
                "failed to send mcp/list response"
            );
        }
    }
}
