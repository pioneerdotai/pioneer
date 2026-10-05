use super::*;

impl MessageProcessor {
    pub(crate) async fn mcp_install(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: McpInstallParams,
    ) {
        let connection_id = request_context.connection_id();
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

        match self
            .install_mcp_plan(
                request_context,
                request_id.clone(),
                &workspace_id,
                plan,
                None,
                params.oauth_redirect_uri.as_deref(),
                params.oauth_callback_unavailable,
            )
            .await
        {
            Ok(payload) => match JsonRpcResponse::from_result(request_id, &payload) {
                Ok(response) => {
                    if let Err(error) = self.send_json(connection_id, &response).await {
                        warn!(connection_id, error = %error, "failed to send mcp/install response");
                    }
                }
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
                    .await
                }
            },
            Err(error) => self.send_error(connection_id, error).await,
        }
    }

    /// The genuine native installer, shared by the legacy parser and portable
    /// package adapter. Ownership is committed with the native row and audit.
    pub(crate) async fn install_mcp_plan(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        workspace_id: &str,
        plan: pioneer_mcp::McpInstallPlan,
        ownership: Option<&pioneer_crud::PluginOwnershipWrite>,
        oauth_redirect_uri: Option<&str>,
        oauth_callback_unavailable: bool,
    ) -> std::result::Result<McpInstallResponse, JsonRpcErrorResponse> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(8);
        let mut owned_id = None;
        if ownership.is_none() {
            for item in &plan.items {
                if let Some(installation) = &item.installation {
                    if let Some(row) = self
                        .crud_store
                        .find_mcp_server_installation(
                            installation.scope_kind.as_str(),
                            &installation.scope_key,
                            &installation.name,
                        )
                        .await
                        .map_err(|_| {
                            mcp_error(
                                Some(request_id.clone()),
                                INVALID_REQUEST_CODE,
                                MCP_ERROR_INTERNAL,
                                "failed to read MCP installation",
                                json!({}),
                            )
                        })?
                    {
                        if let Some(id) = row.id {
                            if self
                                .crud_store
                                .find_mcp_plugin_owner(&id)
                                .await
                                .map_err(|_| {
                                    mcp_error(
                                        Some(request_id.clone()),
                                        INVALID_REQUEST_CODE,
                                        MCP_ERROR_INTERNAL,
                                        "failed to read MCP ownership",
                                        json!({}),
                                    )
                                })?
                                .is_some()
                            {
                                owned_id = Some(id);
                            }
                        }
                    }
                }
            }
        }
        if owned_id.is_some() && plan.items.len() != 1 {
            return Err(mcp_error(
                Some(request_id),
                INVALID_PARAMS_CODE,
                MCP_ERROR_INVALID_REQUEST,
                "edit one plugin component at a time",
                json!({}),
            ));
        }
        let mut native = match owned_id {
            Some(id) => self
                .begin_native_plugin_change(
                    request_context,
                    &request_id,
                    workspace_id,
                    "mcp",
                    &id,
                    "update",
                    &[
                        "transport",
                        "auth",
                        "secret_refs",
                        "display_name",
                        "required",
                    ],
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
        let owned = native.is_some();
        let work = self.install_mcp_plan_with_native_change(
            request_context,
            request_id.clone(),
            workspace_id,
            plan,
            ownership,
            native.as_mut(),
            oauth_redirect_uri,
            oauth_callback_unavailable,
        );
        let result = if owned {
            tokio::time::timeout_at(deadline, work)
                .await
                .unwrap_or_else(|_| {
                    Err(mcp_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INTERNAL,
                        "plugin change deadline exceeded",
                        json!({}),
                    ))
                })
        } else {
            work.await
        };
        match (result, native) {
            (Ok(payload), Some(change)) => self
                .finish_native_plugin_change(change)
                .await
                .map(|_| payload)
                .map_err(|error| {
                    mcp_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INTERNAL,
                        super::super::plugins::native_plugin_error_code(&error),
                        json!({}),
                    )
                }),
            (Err(_), Some(change)) => {
                let _ = self
                    .interrupt_native_plugin_change(&change, "plugins.component_update_failed")
                    .await;
                Err(mcp_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    MCP_ERROR_INTERNAL,
                    "plugin component update requires repair",
                    json!({}),
                ))
            }
            (result, None) => result,
        }
    }

    async fn install_mcp_plan_with_native_change(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        workspace_id: &str,
        plan: pioneer_mcp::McpInstallPlan,
        ownership: Option<&pioneer_crud::PluginOwnershipWrite>,
        mut native: Option<&mut super::super::plugins::NativePluginMutation>,
        oauth_redirect_uri: Option<&str>,
        oauth_callback_unavailable: bool,
    ) -> std::result::Result<McpInstallResponse, JsonRpcErrorResponse> {
        let connection_id = request_context.connection_id();
        let workspace_id = self
            .validate_mcp_workspace(
                connection_id,
                request_id.clone(),
                workspace_id.to_owned(),
                methods::MCP_INSTALL,
            )
            .await?;
        let now = now_timestamp_secs();
        let mut response_items = Vec::new();
        let mut changed = Vec::new();
        let mut events_written = 0usize;

        if ownership.is_some() && plan.items.len() != 1 {
            return Err(mcp_error(
                Some(request_id),
                INVALID_PARAMS_CODE,
                MCP_ERROR_INVALID_REQUEST,
                "owned MCP install must contain exactly one server",
                json!({}),
            ));
        }
        for mut item in plan.items {
            let diagnostics = item
                .diagnostics
                .iter()
                .map(to_protocol_validation)
                .collect::<Vec<_>>();

            let Some(mut installation) = item.installation.take() else {
                response_items.push(McpInstallResult {
                    name: item.name,
                    status: McpInstallResultStatus::ValidationError,
                    diagnostics,
                    server: None,
                });
                continue;
            };

            let _lifecycle = self
                .mcp_service
                .installation_lifecycle_guard(
                    installation.scope_kind.as_str(),
                    &installation.scope_key,
                    &installation.name,
                )
                .await;
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
                    return Err(mcp_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INTERNAL,
                        "failed to query existing MCP server installation",
                        json!({"error": format!("{error:#}")}),
                    ));
                }
            };

            if let Some(owner) = ownership {
                if installation.scope_kind != McpScopeKind::Workspace
                    || installation.scope_key != workspace_id
                    || installation.name
                        != super::portable::internal_mcp_name(&owner.plugin_id, &owner.member_key)
                    || existing
                        .as_ref()
                        .is_some_and(|row| row.id.as_deref() != Some(owner.child_id.as_str()))
                {
                    return Err(mcp_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        MCP_ERROR_INVALID_REQUEST,
                        "plugin MCP identity or scope conflict",
                        json!({}),
                    ));
                }
                if let Some(row) = &existing {
                    let linked = self
                        .crud_store
                        .find_mcp_plugin_owner(row.id.as_deref().unwrap_or(""))
                        .await
                        .map_err(|_| {
                            mcp_error(
                                Some(request_id.clone()),
                                INVALID_REQUEST_CODE,
                                MCP_ERROR_INTERNAL,
                                "failed to check MCP ownership",
                                json!({}),
                            )
                        })?;
                    if !linked.is_some_and(|link| {
                        link.plugin_id == owner.plugin_id && link.member_key == owner.member_key
                    }) {
                        return Err(mcp_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            MCP_ERROR_INVALID_REQUEST,
                            "MCP installation belongs to another source",
                            json!({}),
                        ));
                    }
                }
            } else {
                let linked = match existing.as_ref().and_then(|row| row.id.as_deref()) {
                    Some(id) => self
                        .crud_store
                        .find_mcp_plugin_owner(id)
                        .await
                        .map_err(|_| {
                            mcp_error(
                                Some(request_id.clone()),
                                INVALID_REQUEST_CODE,
                                MCP_ERROR_INTERNAL,
                                "failed to check MCP ownership",
                                json!({}),
                            )
                        })?,
                    None => None,
                };
                if (linked.is_some() && native.is_none())
                    || (native.is_none()
                        && pioneer_crud::validate_standalone_mcp_name(
                            &installation.name,
                            existing.is_some(),
                        )
                        .is_err())
                {
                    return Err(mcp_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        MCP_ERROR_INVALID_REQUEST,
                        "reserved or plugin-owned MCP installation",
                        json!({}),
                    ));
                }
            }
            if let Some(row) = &existing {
                if ownership.is_some() || native.is_some() {
                    let current =
                        crate::mcp_service::installation_from_record(row).map_err(|_| {
                            mcp_error(
                                Some(request_id.clone()),
                                INVALID_REQUEST_CODE,
                                MCP_ERROR_INTERNAL,
                                "native MCP configuration is unavailable",
                                json!({}),
                            )
                        })?;
                    installation.source_ref = current.source_ref.clone();
                    if ownership.is_some() {
                        let link = self
                            .crud_store
                            .find_mcp_plugin_owner(row.id.as_deref().unwrap_or(""))
                            .await
                            .map_err(|_| {
                                mcp_error(
                                    Some(request_id.clone()),
                                    INVALID_REQUEST_CODE,
                                    MCP_ERROR_INTERNAL,
                                    "native MCP ownership is unavailable",
                                    json!({}),
                                )
                            })?;
                        let fields: std::collections::BTreeSet<String> = serde_json::from_str(
                            &link
                                .ok_or_else(|| {
                                    mcp_error(
                                        Some(request_id.clone()),
                                        INVALID_REQUEST_CODE,
                                        MCP_ERROR_INTERNAL,
                                        "native MCP ownership changed",
                                        json!({}),
                                    )
                                })?
                                .override_fields_json,
                        )
                        .map_err(|_| {
                            mcp_error(
                                Some(request_id.clone()),
                                INVALID_REQUEST_CODE,
                                MCP_ERROR_INTERNAL,
                                "native MCP overrides are unavailable",
                                json!({}),
                            )
                        })?;
                        if fields.contains("transport") {
                            installation.transport = current.transport;
                        }
                        if fields.contains("auth") {
                            installation.auth = current.auth;
                        }
                        if fields.contains("secret_refs") {
                            installation.secret_refs = current.secret_refs;
                        }
                        if fields.contains("display_name") {
                            installation.display_name = current.display_name;
                        }
                        if fields.contains("required") {
                            installation.required = current.required;
                        }
                    }
                }
            }
            if let Some(change) = native.as_deref_mut() {
                // Legacy parsing is unchanged. Only an owned edit allocates fresh
                // opaque refs so an interrupted write cannot replace active secrets.
                let mut remap = std::collections::BTreeMap::new();
                for secret in &mut item.secrets {
                    let new = format!("plugin_secret_{}", pioneer_protocol::generate_id(21));
                    remap.insert(secret.ref_id.clone(), new.clone());
                    secret.ref_id = new;
                }
                remap_owned_secret_refs(&mut installation, &remap);
                let new_refs = mcp_secret_ref_ids(&installation.secret_refs);
                let mut before: serde_json::Value =
                    serde_json::from_str(&change.guard.parent.pending_json.clone().unwrap())
                        .map_err(|_| {
                            mcp_error(
                                Some(request_id.clone()),
                                INVALID_REQUEST_CODE,
                                MCP_ERROR_INTERNAL,
                                "plugin pending plan is unavailable",
                                json!({}),
                            )
                        })?;
                before["cleanup_refs"] = json!(remap.values().collect::<Vec<_>>());
                self.crud_store
                    .replace_plugin_pending(
                        &change.guard.parent.id,
                        change.guard.parent.revision,
                        &serde_json::to_string(&before).unwrap(),
                    )
                    .await
                    .map_err(|_| {
                        mcp_error(
                            Some(request_id.clone()),
                            INVALID_REQUEST_CODE,
                            MCP_ERROR_INTERNAL,
                            "plugin pending plan changed",
                            json!({}),
                        )
                    })?;
                change.guard.parent.pending_json = Some(serde_json::to_string(&before).unwrap());
                let old_refs = existing
                    .as_ref()
                    .map(|row| parse_mcp_secret_ref_ids(&row.secret_refs_json))
                    .transpose()
                    .map_err(|_| {
                        mcp_error(
                            Some(request_id.clone()),
                            INVALID_REQUEST_CODE,
                            MCP_ERROR_INTERNAL,
                            "native MCP references are unavailable",
                            json!({}),
                        )
                    })?
                    .unwrap_or_default();
                before["cleanup_refs"] = json!(old_refs.difference(&new_refs).collect::<Vec<_>>());
                before["native_committed"] = json!(true);
                change.write.pending_after = serde_json::to_string(&before).unwrap();
            }
            let old_secret_ref_ids = match existing.as_ref() {
                Some(existing) => {
                    match parse_mcp_secret_ref_ids(existing.secret_refs_json.as_str()) {
                        Ok(ref_ids) => ref_ids,
                        Err(error) => {
                            return Err(mcp_error(
                                Some(request_id.clone()),
                                INVALID_REQUEST_CODE,
                                MCP_ERROR_INTERNAL,
                                "failed to decode existing MCP secret refs",
                                json!({"error": format!("{error:#}")}),
                            ));
                        }
                    }
                }
                None => std::collections::BTreeSet::new(),
            };
            installation.fingerprint = pioneer_mcp::fingerprint_installation(&installation);
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
                    let cleanup_report =
                        self.gateway_secrets
                            .delete_mcp_secrets(if native.is_some() {
                                Vec::new()
                            } else {
                                cleanup_refs.collect()
                            });
                    warn_mcp_secret_delete_report(
                        "mcp_install_keystore_write_failure",
                        &cleanup_report,
                    );
                    return Err(mcp_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INTERNAL,
                        "failed to save MCP secrets",
                        json!({"error": format!("{error:#}")}),
                    ));
                }
                written_secret_ref_ids.insert(secret.ref_id.clone());
            }

            let mut record = match installation_record_from_domain(&installation) {
                Ok(record) => record,
                Err(error) => {
                    return Err(mcp_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INTERNAL,
                        "failed to encode MCP server installation",
                        json!({"error": format!("{error:#}")}),
                    ));
                }
            };

            if let Some(owner) = ownership {
                record.id = Some(owner.child_id.clone());
            }
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
                .upsert_mcp_server_installation_with_plugin_native_change(
                    &record,
                    &audit,
                    ownership,
                    native.as_deref().map(|change| &change.write),
                    now,
                )
                .await
            {
                Ok(id) => id,
                Err(error) => {
                    let cleanup_refs = written_secret_ref_ids
                        .difference(&old_secret_ref_ids)
                        .map(String::as_str);
                    let cleanup_report =
                        self.gateway_secrets
                            .delete_mcp_secrets(if native.is_some() {
                                Vec::new()
                            } else {
                                cleanup_refs.collect()
                            });
                    warn_mcp_secret_delete_report("mcp_install_db_failure", &cleanup_report);
                    return Err(mcp_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INTERNAL,
                        "failed to persist MCP server installation",
                        json!({"error": format!("{error:#}")}),
                    ));
                }
            };
            record.id = Some(installation_id.clone());
            if matches!(
                installation.transport,
                McpTransportConfig::StreamableHttp { .. }
            ) {
                let oauth = self.mcp_service.oauth();
                let outcome = oauth.synchronize(&installation_id, &installation).await;
                if (ownership.is_some() || native.is_some()) && outcome.is_err() {
                    return Err(mcp_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INTERNAL,
                        "native MCP authorization synchronization requires repair",
                        json!({}),
                    ));
                }
                if outcome.is_ok() {
                    if oauth_callback_unavailable {
                        let _ = oauth
                            .begin_install_without_callback(
                                &installation_id,
                                &installation,
                                connection_id,
                                &workspace_id,
                            )
                            .await;
                    } else if let Some(redirect) = oauth_redirect_uri {
                        if let Err(error) = oauth
                            .begin_install_in_workspace(
                                &installation_id,
                                &installation,
                                connection_id,
                                redirect,
                                &workspace_id,
                            )
                            .await
                        {
                            warn!(reason=%error.message,"MCP OAuth preparation failed");
                        }
                    }
                }
            }
            if !matches!(
                installation.transport,
                McpTransportConfig::StreamableHttp { .. }
            ) {
                if let Err(error) = self.mcp_service.oauth().disconnect(&installation_id).await {
                    if ownership.is_some() || native.is_some() {
                        return Err(mcp_error(
                            Some(request_id.clone()),
                            INVALID_REQUEST_CODE,
                            MCP_ERROR_INTERNAL,
                            "native MCP authorization cleanup requires repair",
                            json!({}),
                        ));
                    }
                    warn!(reason=%error.message,"MCP OAuth cleanup deferred");
                }
            }
            events_written = events_written.saturating_add(1);

            let stale_refs = old_secret_ref_ids
                .difference(&new_secret_ref_ids)
                .map(String::as_str);
            let stale_delete_report = self.gateway_secrets.delete_mcp_secrets(stale_refs);
            warn_mcp_secret_delete_report("mcp_install_stale_refs", &stale_delete_report);
            if (ownership.is_some() || native.is_some()) && !stale_delete_report.failed.is_empty() {
                return Err(mcp_error(
                    Some(request_id.clone()),
                    INVALID_REQUEST_CODE,
                    MCP_ERROR_INTERNAL,
                    "native MCP secret cleanup requires repair",
                    json!({}),
                ));
            }

            changed.push(McpChangedItem {
                name: record.name.clone(),
                source_kind: McpSourceKind::Config,
                action: changed_action,
            });

            let mut published_server = list_item_from_record(&record);
            published_server.plugin_owner = native
                .as_deref()
                .map(|change| pioneer_protocol::PluginOwner {
                    plugin_id: change.write.plugin_id.clone(),
                    member_key: change.write.member_key.clone(),
                })
                .or_else(|| {
                    ownership.map(|owner| pioneer_protocol::PluginOwner {
                        plugin_id: owner.plugin_id.clone(),
                        member_key: owner.member_key.clone(),
                    })
                });
            response_items.push(McpInstallResult {
                name: record.name.clone(),
                status: if changed_action == McpChangedAction::Update {
                    McpInstallResultStatus::Updated
                } else {
                    McpInstallResultStatus::Installed
                },
                diagnostics,
                server: Some(published_server),
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

        if !changed.is_empty() {
            let snapshot_version = self.next_mcp_snapshot_version();
            let notification = McpChangedNotification {
                workspace_id: workspace_id.clone(),
                snapshot_version,
                changed,
            };
            self.send_gateway_management_notification(events::MCP_CHANGED, &notification)
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
        Ok(response_payload)
    }
}

/// Fresh opaque refs for an owned edit. Literal values and native standalone
/// parser interpretation never pass through this mapping.
fn remap_owned_secret_refs(
    installation: &mut McpServerInstallation,
    remap: &std::collections::BTreeMap<String, String>,
) {
    fn value(
        value: &mut pioneer_mcp::McpConfigValue,
        remap: &std::collections::BTreeMap<String, String>,
    ) {
        if let pioneer_mcp::McpConfigValue::SecretRef { ref_id } = value {
            if let Some(new) = remap.get(ref_id) {
                *ref_id = new.clone();
            }
        }
    }
    match &mut installation.transport {
        McpTransportConfig::Stdio { env, .. } => {
            for entry in env.values_mut() {
                value(entry, remap);
            }
        }
        McpTransportConfig::StreamableHttp { headers, .. } => {
            for entry in headers.values_mut() {
                value(entry, remap);
            }
        }
    }
    if let Some(oauth) = installation.auth.oauth.as_mut() {
        if let Some(new) = oauth
            .client_secret_ref
            .as_ref()
            .and_then(|old| remap.get(old))
        {
            oauth.client_secret_ref = Some(new.clone());
        }
    }
    for secret in &mut installation.secret_refs {
        if let Some(new) = remap.get(&secret.ref_id) {
            secret.ref_id = new.clone();
        }
    }
    installation.fingerprint = pioneer_mcp::fingerprint_installation(installation);
}

#[cfg(test)]
mod owned_secret_tests {
    // Source regression only: NOT_RUN / NOT_COMPILED.
    use super::*;
    #[test]
    fn owned_edit_allocates_new_opaque_refs_without_reinterpreting_literals() {
        let mut installation = parse_install_config(
            r#"{"mcpServers":{"native":{"url":"https://example.org/mcp"}}}"#,
            InstallParseContext {
                scope_kind: McpScopeKind::Workspace,
                scope_key: "ws".into(),
                default_enabled: true,
                default_allow_implicit_invocation: false,
            },
        )
        .unwrap()
        .items
        .remove(0)
        .installation
        .unwrap();
        if let McpTransportConfig::StreamableHttp { headers, .. } = &mut installation.transport {
            headers.insert(
                "Authorization".into(),
                pioneer_mcp::McpConfigValue::SecretRef {
                    ref_id: "old".into(),
                },
            );
            headers.insert(
                "X-Literal".into(),
                pioneer_mcp::McpConfigValue::Literal {
                    value: "old".into(),
                },
            );
        } else {
            panic!("expected native HTTP configuration");
        }
        installation.auth.oauth = Some(pioneer_mcp::McpOAuthConfig {
            client_secret_ref: Some("old".into()),
            ..Default::default()
        });
        installation.secret_refs = vec![pioneer_mcp::McpSecretRef {
            ref_id: "old".into(),
            name: "Authorization".into(),
            source: "header".into(),
        }];
        let old = pioneer_mcp::fingerprint_installation(&installation);
        remap_owned_secret_refs(
            &mut installation,
            &std::collections::BTreeMap::from([("old".into(), "fresh".into())]),
        );
        let McpTransportConfig::StreamableHttp { headers, .. } = &installation.transport else {
            unreachable!()
        };
        assert_eq!(
            headers["Authorization"],
            pioneer_mcp::McpConfigValue::SecretRef {
                ref_id: "fresh".into()
            }
        );
        assert_eq!(
            headers["X-Literal"],
            pioneer_mcp::McpConfigValue::Literal {
                value: "old".into()
            }
        );
        assert_eq!(
            installation
                .auth
                .oauth
                .unwrap()
                .client_secret_ref
                .as_deref(),
            Some("fresh")
        );
        assert_eq!(installation.secret_refs[0].ref_id, "fresh");
        assert_ne!(installation.fingerprint, old);
    }
}
