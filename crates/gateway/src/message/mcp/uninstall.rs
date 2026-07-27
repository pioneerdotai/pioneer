use super::*;

impl MessageProcessor {
    pub(crate) async fn mcp_uninstall(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: McpUninstallParams,
    ) {
        let connection_id = request_context.connection_id();
        let workspace_id = match self
            .validate_mcp_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::MCP_UNINSTALL,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };

        let name = params.name.trim().to_owned();
        if name.is_empty() {
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
        let scope_key = match scope_kind {
            McpScopeKind::Workspace => workspace_id.clone(),
            McpScopeKind::User => "default".to_owned(),
        };

        let row = match self
            .crud_store
            .find_mcp_server_installation(scope_kind.as_str(), scope_key.as_str(), name.as_str())
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
                            "name": name,
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
                        "failed to query MCP server installation",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };

        let secret_ref_ids = match parse_mcp_secret_ref_ids(row.secret_refs_json.as_str()) {
            Ok(ref_ids) => ref_ids,
            Err(error) => {
                self.send_error(
                    connection_id,
                    mcp_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INTERNAL,
                        "failed to decode MCP secret refs",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };

        let now = now_timestamp_secs();
        let server_id = row.id.clone().unwrap_or_default();
        let audit = McpAuditEventRecord {
            turn_id: None,
            server_installation_id: row.id.clone(),
            server_name: row.name.clone(),
            raw_tool_name: None,
            callable_name: None,
            catalog_version: None,
            action: McpChangedAction::Uninstall.as_str().to_owned(),
            decision: "allowed".to_owned(),
            reason_code: None,
            details_json: serde_json::to_string(&json!({
                "scope_kind": row.scope_kind,
                "scope_key": row.scope_key,
                "source_kind": row.source_kind,
                "transport_kind": row.transport_kind,
                "fingerprint": row.fingerprint,
            }))
            .unwrap_or_else(|_| "{}".to_owned()),
            created_at_unix: now,
        };

        if let Err(error) = self
            .crud_store
            .delete_mcp_server_installation_with_audit(&row, &audit)
            .await
        {
            self.send_error(
                connection_id,
                mcp_error(
                    Some(request_id.clone()),
                    INVALID_REQUEST_CODE,
                    MCP_ERROR_INTERNAL,
                    "failed to delete MCP server installation",
                    json!({"error": format!("{error:#}")}),
                ),
            )
            .await;
            return;
        }

        let response_payload = McpUninstallResponse {
            removed: true,
            server_id,
            name: row.name.clone(),
            scope_kind: params.scope_kind,
            audit: McpLifecycleAuditSummary { events_written: 1 },
        };
        let response = match JsonRpcResponse::from_result(request_id.clone(), &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    mcp_error(
                        None,
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INTERNAL,
                        "failed to encode mcp/uninstall response",
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
                "failed to send mcp/uninstall response"
            );
        }

        self.notify_mcp_changed(
            workspace_id.as_str(),
            vec![McpChangedItem {
                name: row.name,
                source_kind: McpSourceKind::Config,
                action: McpChangedAction::Uninstall,
            }],
            now,
        )
        .await;

        if let Err(error) = self
            .mcp_service
            .reload_workspace(workspace_id.as_str())
            .await
        {
            warn!(
                workspace_id = workspace_id.as_str(),
                error = %format!("{error:#}"),
                "failed to reload MCP runtime after uninstall"
            );
        }

        let cleanup_report = self
            .gateway_secrets
            .delete_mcp_secrets(secret_ref_ids.iter().map(String::as_str));
        warn_mcp_secret_delete_report("mcp_uninstall", &cleanup_report);
    }
}
