use super::*;

impl MessageProcessor {
    pub(crate) async fn mcp_policy_set(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: McpPolicySetParams,
    ) {
        let workspace_id = match self
            .validate_mcp_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::MCP_POLICY_SET,
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
        if params.enabled.is_none() && params.allow_implicit_invocation.is_none() {
            self.send_error(
                connection_id,
                mcp_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    MCP_ERROR_INVALID_REQUEST,
                    "enabled or allow_implicit_invocation is required",
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

        let mut record = match self
            .crud_store
            .find_mcp_server_installation(
                scope_kind.as_str(),
                scope_key.as_str(),
                params.name.trim(),
            )
            .await
        {
            Ok(Some(record)) => record,
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
                        "failed to query MCP server installation",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };

        let before_enabled = record.enabled;
        let before_allow_implicit_invocation = record.allow_implicit_invocation;
        if let Some(enabled) = params.enabled {
            record.enabled = enabled;
        }
        if let Some(allow_implicit_invocation) = params.allow_implicit_invocation {
            record.allow_implicit_invocation = allow_implicit_invocation;
        }

        let now = now_timestamp_secs();
        let audit = McpAuditEventRecord {
            turn_id: None,
            server_installation_id: None,
            server_name: record.name.clone(),
            raw_tool_name: None,
            callable_name: None,
            catalog_version: None,
            action: McpChangedAction::Policy.as_str().to_owned(),
            decision: "allowed".to_owned(),
            reason_code: None,
            details_json: serde_json::to_string(&json!({
                "scope_kind": record.scope_kind,
                "scope_key": record.scope_key,
                "source_kind": record.source_kind,
                "before": {
                    "enabled": before_enabled,
                    "allow_implicit_invocation": before_allow_implicit_invocation,
                },
                "after": {
                    "enabled": record.enabled,
                    "allow_implicit_invocation": record.allow_implicit_invocation,
                },
            }))
            .unwrap_or_else(|_| "{}".to_owned()),
            created_at_unix: now,
        };

        let installation_id = match self
            .crud_store
            .upsert_mcp_server_installation_with_audit(&record, &audit, now)
            .await
        {
            Ok(id) => id,
            Err(error) => {
                self.send_error(
                    connection_id,
                    mcp_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INTERNAL,
                        "failed to persist MCP server policy",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };
        record.id = Some(installation_id);

        let server = list_item_from_record(&record);
        let payload = McpPolicySetResponse {
            policy: McpServerPolicy {
                workspace_id: workspace_id.clone(),
                name: record.name.clone(),
                scope_kind: params.scope_kind,
                enabled: record.enabled,
                allow_implicit_invocation: record.allow_implicit_invocation,
            },
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
                        "failed to encode mcp/policy/set response",
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
                "failed to send mcp/policy/set response"
            );
            return;
        }

        self.notify_mcp_changed(
            workspace_id.as_str(),
            vec![McpChangedItem {
                name: record.name,
                source_kind: McpSourceKind::Config,
                action: McpChangedAction::Policy,
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
                "failed to reload MCP runtime after policy change"
            );
        }
    }
}
