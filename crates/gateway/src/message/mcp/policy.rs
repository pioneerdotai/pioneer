use super::*;

struct McpPolicyChange {
    payload: McpPolicySetResponse,
    now: i64,
    lifecycle: tokio::sync::OwnedMutexGuard<()>,
    plugin: Option<super::super::plugins::NativePluginMutation>,
    request_id: RequestId,
}

impl MessageProcessor {
    pub(crate) async fn mcp_policy_set(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: McpPolicySetParams,
    ) {
        let change = match self
            .prepare_mcp_policy_change(request_context, request_id.clone(), params)
            .await
        {
            Ok(change) => change,
            Err(error) => {
                self.send_error(request_context.connection_id(), error)
                    .await;
                return;
            }
        };
        // Owned native changes reply only after the parent outcome is settled.
        // Standalone retains the legacy response-before-publication ordering.
        if change.plugin.is_some() {
            match self.finish_mcp_policy_change(change).await {
                Ok(payload) => match JsonRpcResponse::from_result(request_id, &payload) {
                    Ok(response) => {
                        let _ = self
                            .send_json(request_context.connection_id(), &response)
                            .await;
                    }
                    Err(_) => {
                        self.send_error(
                            request_context.connection_id(),
                            mcp_error(
                                None,
                                INVALID_REQUEST_CODE,
                                MCP_ERROR_INTERNAL,
                                "failed to encode mcp/policy/set response",
                                json!({}),
                            ),
                        )
                        .await;
                    }
                },
                Err(error) => {
                    self.send_error(request_context.connection_id(), error)
                        .await
                }
            }
            return;
        }
        let response = match JsonRpcResponse::from_result(request_id, &change.payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    request_context.connection_id(),
                    mcp_error(
                        None,
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INTERNAL,
                        "failed to encode mcp/policy/set response",
                        json!({"error":format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };
        if let Err(error) = self
            .send_json(request_context.connection_id(), &response)
            .await
        {
            warn!(error=%error, "failed to send mcp/policy/set response");
            return;
        }
        let _ = self.finish_mcp_policy_change(change).await;
    }

    /// Runs the same admitted native mutation and its existing post-commit
    /// publications/reload without requiring a websocket reply.
    pub(crate) async fn set_mcp_policy(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: McpPolicySetParams,
    ) -> std::result::Result<McpPolicySetResponse, JsonRpcErrorResponse> {
        let change = self
            .prepare_mcp_policy_change(request_context, request_id, params)
            .await?;
        self.finish_mcp_policy_change(change).await
    }

    async fn prepare_mcp_policy_change(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: McpPolicySetParams,
    ) -> std::result::Result<McpPolicyChange, JsonRpcErrorResponse> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(8);
        let connection_id = request_context.connection_id();
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
                return Err(error);
            }
        };

        if params.name.trim().is_empty() {
            return Err(mcp_error(
                Some(request_id),
                INVALID_PARAMS_CODE,
                MCP_ERROR_INVALID_REQUEST,
                "MCP server name is required",
                json!({"name": params.name}),
            ));
        }
        if params.enabled.is_none() && params.allow_implicit_invocation.is_none() {
            return Err(mcp_error(
                Some(request_id),
                INVALID_PARAMS_CODE,
                MCP_ERROR_INVALID_REQUEST,
                "enabled or allow_implicit_invocation is required",
                json!({"name": params.name}),
            ));
        }

        let scope_kind = match McpScopeKind::from_str(params.scope_kind.as_str()) {
            Ok(scope_kind) => scope_kind,
            Err(error) => {
                return Err(mcp_error(
                    Some(request_id.clone()),
                    INVALID_PARAMS_CODE,
                    MCP_ERROR_INVALID_REQUEST,
                    "invalid MCP scope kind",
                    json!({"error": error}),
                ));
            }
        };
        let scope_key = match &scope_kind {
            McpScopeKind::Workspace => workspace_id.clone(),
            McpScopeKind::User => "default".to_owned(),
        };

        let fields: Vec<&str> = [
            params.enabled.map(|_| "enabled"),
            params
                .allow_implicit_invocation
                .map(|_| "allow_implicit_invocation"),
        ]
        .into_iter()
        .flatten()
        .collect();
        let initial = self
            .crud_store
            .find_mcp_server_installation(scope_kind.as_str(), &scope_key, params.name.trim())
            .await
            .map_err(|_| {
                mcp_error(
                    Some(request_id.clone()),
                    INVALID_REQUEST_CODE,
                    MCP_ERROR_INTERNAL,
                    "failed to query MCP server installation",
                    json!({}),
                )
            })?;
        let plugin = match initial.as_ref().and_then(|row| row.id.as_deref()) {
            Some(id) => self
                .begin_native_plugin_change(
                    request_context,
                    &request_id,
                    &workspace_id,
                    "mcp",
                    id,
                    "policy",
                    &fields,
                    deadline,
                )
                .await
                .map_err(|error| {
                    mcp_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INVALID_REQUEST,
                        super::super::plugins::native_plugin_error_code(&error),
                        json!({}),
                    )
                })?,
            None => None,
        };
        let lifecycle = self
            .mcp_service
            .installation_lifecycle_guard(scope_kind.as_str(), &scope_key, params.name.trim())
            .await;
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
                return Err(mcp_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    MCP_ERROR_NOT_FOUND,
                    "MCP server installation was not found",
                    json!({
                        "scope_kind": params.scope_kind,
                        "scope_key": scope_key,
                        "name": params.name,
                    }),
                ));
            }
            Err(error) => {
                return Err(mcp_error(
                    Some(request_id.clone()),
                    INVALID_REQUEST_CODE,
                    MCP_ERROR_INTERNAL,
                    "failed to query MCP server installation",
                    json!({"error": format!("{error:#}")}),
                ));
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

        let write = self
            .crud_store
            .upsert_mcp_server_installation_with_plugin_native_change(
                &record,
                &audit,
                None,
                plugin.as_ref().map(|change| &change.write),
                now,
            );
        let outcome = if plugin.is_some() {
            tokio::time::timeout_at(deadline, write)
                .await
                .unwrap_or_else(|_| Err(anyhow::anyhow!("plugins.native_commit_unconfirmed")))
        } else {
            write.await
        };
        let installation_id = match outcome {
            Ok(id) => id,
            Err(error) => {
                if let Some(change) = &plugin {
                    let _ = self
                        .interrupt_native_plugin_change(change, "plugins.native_commit_unconfirmed")
                        .await;
                }
                return Err(mcp_error(
                    Some(request_id.clone()),
                    INVALID_REQUEST_CODE,
                    MCP_ERROR_INTERNAL,
                    "failed to persist MCP server policy",
                    if plugin.is_some() {
                        json!({"code":"plugins.native_commit_unconfirmed"})
                    } else {
                        json!({"error": format!("{error:#}")})
                    },
                ));
            }
        };
        record.id = Some(installation_id);

        let mut server = list_item_from_record(&record);
        server.plugin_owner = plugin.as_ref().map(|change| pioneer_protocol::PluginOwner {
            plugin_id: change.write.plugin_id.clone(),
            member_key: change.write.member_key.clone(),
        });
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
        Ok(McpPolicyChange {
            payload,
            now,
            lifecycle,
            plugin,
            request_id,
        })
    }

    async fn finish_mcp_policy_change(
        &self,
        change: McpPolicyChange,
    ) -> std::result::Result<McpPolicySetResponse, JsonRpcErrorResponse> {
        let McpPolicyChange {
            payload,
            now,
            lifecycle,
            plugin,
            request_id,
        } = change;
        let workspace_id = &payload.policy.workspace_id;
        self.notify_mcp_changed(
            workspace_id.as_str(),
            vec![McpChangedItem {
                name: payload.policy.name.clone(),
                source_kind: McpSourceKind::Config,
                action: McpChangedAction::Policy,
            }],
            now,
        )
        .await;

        drop(lifecycle);
        if let Some(change) = plugin {
            self.finish_native_plugin_change(change)
                .await
                .map_err(|_| {
                    mcp_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INVALID_REQUEST,
                        "plugins.interrupted",
                        json!({}),
                    )
                })?;
            return Ok(payload);
        }
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
        Ok(payload)
    }
}
