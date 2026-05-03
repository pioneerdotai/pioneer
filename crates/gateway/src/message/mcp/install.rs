use super::*;

impl MessageProcessor {
    pub(crate) async fn mcp_install(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: McpInstallParams,
    ) {
        let workspace_id = match self
            .validate_mcp_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::MCP_INSTALL,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };

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

        let plan = match parse_install_config(
            params.config_json.as_str(),
            InstallParseContext {
                scope_kind: scope_kind.clone(),
                scope_key: scope_key.clone(),
                default_enabled: params.enabled,
                default_allow_implicit_invocation: params.allow_implicit_invocation,
            },
        ) {
            Ok(plan) => plan,
            Err(error) => {
                self.send_error(
                    connection_id,
                    mcp_error(
                        Some(request_id.clone()),
                        INVALID_PARAMS_CODE,
                        MCP_ERROR_INVALID_REQUEST,
                        "invalid MCP install config",
                        json!({"diagnostic": error.diagnostic}),
                    ),
                )
                .await;
                return;
            }
        };

        let now = now_timestamp_secs();
        let mut response_items = Vec::new();
        let mut changed = Vec::new();
        let mut events_written = 0usize;

        for item in plan.items {
            let diagnostics = item
                .diagnostics
                .iter()
                .map(to_protocol_validation)
                .collect::<Vec<_>>();

            let Some(installation) = item.installation else {
                response_items.push(McpInstallResult {
                    name: item.name,
                    status: McpInstallResultStatus::ValidationError,
                    diagnostics,
                    server: None,
                });
                continue;
            };

            let existing = match self
                .crud_store
                .find_mcp_server_installation(
                    installation.scope_kind.as_str(),
                    installation.scope_key.as_str(),
                    installation.name.as_str(),
                )
                .await
            {
                Ok(existing) => existing,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        mcp_error(
                            Some(request_id.clone()),
                            INVALID_REQUEST_CODE,
                            MCP_ERROR_INTERNAL,
                            "failed to query existing MCP server installation",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            };

            let old_secret_ref_ids = match existing.as_ref() {
                Some(existing) => {
                    match parse_mcp_secret_ref_ids(existing.secret_refs_json.as_str()) {
                        Ok(ref_ids) => ref_ids,
                        Err(error) => {
                            self.send_error(
                                connection_id,
                                mcp_error(
                                    Some(request_id.clone()),
                                    INVALID_REQUEST_CODE,
                                    MCP_ERROR_INTERNAL,
                                    "failed to decode existing MCP secret refs",
                                    json!({"error": format!("{error:#}")}),
                                ),
                            )
                            .await;
                            return;
                        }
                    }
                }
                None => std::collections::BTreeSet::new(),
            };
            let new_secret_ref_ids = mcp_secret_ref_ids(&installation.secret_refs);
            let mut written_secret_ref_ids = std::collections::BTreeSet::new();
            for secret in &item.secrets {
                let label = mcp_secret_label(
                    installation.name.as_str(),
                    installation.secret_refs.as_slice(),
                    secret.ref_id.as_str(),
                );
                if let Err(error) = self.gateway_secrets.put_mcp_secret(
                    secret.ref_id.as_str(),
                    secret.value.as_str(),
                    Some(label),
                ) {
                    let cleanup_refs = written_secret_ref_ids
                        .difference(&old_secret_ref_ids)
                        .map(String::as_str);
                    let cleanup_report = self.gateway_secrets.delete_mcp_secrets(cleanup_refs);
                    warn_mcp_secret_delete_report(
                        "mcp_install_keystore_write_failure",
                        &cleanup_report,
                    );
                    self.send_error(
                        connection_id,
                        mcp_error(
                            Some(request_id.clone()),
                            INVALID_REQUEST_CODE,
                            MCP_ERROR_INTERNAL,
                            "failed to save MCP secrets",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
                written_secret_ref_ids.insert(secret.ref_id.clone());
            }

            let mut record = match installation_record_from_domain(&installation) {
                Ok(record) => record,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        mcp_error(
                            Some(request_id.clone()),
                            INVALID_REQUEST_CODE,
                            MCP_ERROR_INTERNAL,
                            "failed to encode MCP server installation",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            };

            if let Some(existing) = existing.as_ref() {
                record.id = existing.id.clone();
                record.enabled = existing.enabled;
                record.allow_implicit_invocation = existing.allow_implicit_invocation;
            }

            let changed_action = if existing.is_some() {
                McpChangedAction::Update
            } else {
                McpChangedAction::Install
            };
            let action = changed_action.as_str();
            let audit = McpAuditEventRecord {
                turn_id: None,
                server_installation_id: None,
                server_name: record.name.clone(),
                raw_tool_name: None,
                callable_name: None,
                catalog_version: None,
                action: action.to_owned(),
                decision: "allowed".to_owned(),
                reason_code: None,
                details_json: serde_json::to_string(&json!({
                    "scope_kind": record.scope_kind,
                    "scope_key": record.scope_key,
                    "source_kind": record.source_kind,
                    "transport_kind": record.transport_kind,
                    "fingerprint": record.fingerprint,
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
                    let cleanup_refs = written_secret_ref_ids
                        .difference(&old_secret_ref_ids)
                        .map(String::as_str);
                    let cleanup_report = self.gateway_secrets.delete_mcp_secrets(cleanup_refs);
                    warn_mcp_secret_delete_report("mcp_install_db_failure", &cleanup_report);
                    self.send_error(
                        connection_id,
                        mcp_error(
                            Some(request_id.clone()),
                            INVALID_REQUEST_CODE,
                            MCP_ERROR_INTERNAL,
                            "failed to persist MCP server installation",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            };
            record.id = Some(installation_id);
            events_written = events_written.saturating_add(1);

            let stale_refs = old_secret_ref_ids
                .difference(&new_secret_ref_ids)
                .map(String::as_str);
            let stale_delete_report = self.gateway_secrets.delete_mcp_secrets(stale_refs);
            warn_mcp_secret_delete_report("mcp_install_stale_refs", &stale_delete_report);

            changed.push(McpChangedItem {
                name: record.name.clone(),
                source_kind: McpSourceKind::Config,
                action: changed_action,
            });

            response_items.push(McpInstallResult {
                name: record.name.clone(),
                status: if changed_action == McpChangedAction::Update {
                    McpInstallResultStatus::Updated
                } else {
                    McpInstallResultStatus::Installed
                },
                diagnostics,
                server: Some(list_item_from_record(&record)),
            });
        }

        let successful = response_items
            .iter()
            .filter(|item| {
                matches!(
                    item.status,
                    McpInstallResultStatus::Installed | McpInstallResultStatus::Updated
                )
            })
            .count();
        let validation_errors = response_items
            .iter()
            .any(|item| item.status == McpInstallResultStatus::ValidationError);
        let status = match (successful, validation_errors) {
            (0, _) => McpInstallStatus::ValidationError,
            (_, true) => McpInstallStatus::Partial,
            _ => McpInstallStatus::Ok,
        };
        let response_payload = McpInstallResponse {
            status,
            servers: response_items,
            audit: McpLifecycleAuditSummary { events_written },
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
                        "failed to encode mcp/install response",
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
                "failed to send mcp/install response"
            );
            return;
        }

        if !changed.is_empty() {
            let snapshot_version = self.next_mcp_snapshot_version();
            let notification = McpChangedNotification {
                workspace_id: workspace_id.clone(),
                snapshot_version,
                changed,
            };
            self.send_notification_to_workspace_connections(
                workspace_id.as_str(),
                events::MCP_CHANGED,
                &notification,
            )
            .await;
        }

        if let Err(error) = self
            .mcp_service
            .reload_workspace(workspace_id.as_str())
            .await
        {
            warn!(
                workspace_id = workspace_id.as_str(),
                error = %format!("{error:#}"),
                "failed to reload MCP runtime after install"
            );
        }
    }
}
