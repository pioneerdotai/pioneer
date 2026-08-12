use super::*;
use serde_json::{Map as JsonMap, Value as JsonValue};

const MCP_DETAILS_AUDIT_LIMIT: u64 = 50;
const MCP_DETAILS_BINDING_LIMIT: u64 = 50;

impl MessageProcessor {
    pub(crate) async fn mcp_server_details(
        &self,
        request_context: &RequestContext,
        _authorization: &crate::authorization::AuthorizedCapability,
        request_id: RequestId,
        params: McpServerDetailsParams,
    ) {
        let connection_id = request_context.connection_id();
        let member = request_context.principal().kind == pioneer_protocol::PrincipalKind::User;
        let workspace_id = match self
            .validate_mcp_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::MCP_SERVER_DETAILS,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };

        let server_id = params.server_id.trim().to_owned();
        if server_id.is_empty() {
            self.send_error(
                connection_id,
                mcp_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    MCP_ERROR_INVALID_REQUEST,
                    "MCP server id is required",
                    json!({"server_id": params.server_id}),
                ),
            )
            .await;
            return;
        }

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
        let Some(row) = rows
            .into_iter()
            .find(|row| row.id.as_deref() == Some(server_id.as_str()))
        else {
            self.send_error(
                connection_id,
                mcp_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    MCP_ERROR_NOT_FOUND,
                    "MCP server installation was not found",
                    json!({"workspace_id": workspace_id, "server_id": server_id}),
                ),
            )
            .await;
            return;
        };

        let catalog = match self
            .crud_store
            .find_mcp_server_catalog_snapshot(server_id.as_str())
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
        };

        let runtime_snapshots = self
            .mcp_service
            .runtime_snapshot(row.scope_kind.as_str(), row.scope_key.as_str())
            .await;
        let runtime = runtime_snapshots.get(server_id.as_str());
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

        // Members need the catalog to select and understand an MCP server.
        // Management audit and cross-turn binding history are not part of
        // that operational capability and may reveal other users' activity.
        let audit = if member {
            Vec::new()
        } else {
            match self
                .crud_store
                .list_recent_mcp_audit_event_records_for_server_id(
                    server_id.as_str(),
                    MCP_DETAILS_AUDIT_LIMIT,
                )
                .await
            {
                Ok(rows) => rows.into_iter().map(audit_summary_from_record).collect(),
                Err(error) => {
                    self.send_error(
                        connection_id,
                        mcp_error(
                            Some(request_id.clone()),
                            INVALID_REQUEST_CODE,
                            MCP_ERROR_INTERNAL,
                            "failed to load MCP audit events",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            }
        };

        let recent_bindings = if member {
            Vec::new()
        } else {
            match self
                .crud_store
                .list_recent_turn_mcp_bindings_for_server(
                    server_id.as_str(),
                    MCP_DETAILS_BINDING_LIMIT,
                )
                .await
            {
                Ok(rows) => rows.into_iter().map(binding_summary_from_record).collect(),
                Err(error) => {
                    self.send_error(
                        connection_id,
                        mcp_error(
                            Some(request_id.clone()),
                            INVALID_REQUEST_CODE,
                            MCP_ERROR_INTERNAL,
                            "failed to load MCP turn bindings",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            }
        };

        let health = health_details_from_runtime(&server, runtime);
        let response_payload = McpServerDetailsResponse {
            snapshot_version: self.current_mcp_snapshot_version(),
            generated_at: now_timestamp_secs(),
            server,
            catalog: catalog_details(catalog.as_ref()),
            health,
            audit,
            recent_bindings,
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
                        "failed to encode mcp/server/details response",
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
                "failed to send mcp/server/details response"
            );
        }
    }
}

fn health_details_from_runtime(
    server: &McpListItem,
    runtime: Option<&pioneer_mcp::McpServerRuntimeSnapshot>,
) -> McpServerHealthDetails {
    McpServerHealthDetails {
        runtime: server.runtime.clone(),
        status: server.status,
        status_reason: server.status_reason.clone(),
        last_error: server.runtime.last_error.clone(),
        retry_attempt: runtime.map(|runtime| runtime.retry_attempt),
        next_retry_at: runtime.and_then(|runtime| runtime.next_retry_at_unix),
        catalog_version: runtime.and_then(|runtime| runtime.catalog_version.clone()),
        stderr_tail: None,
    }
}

fn catalog_details(catalog: Option<&McpServerCatalogSnapshotRecord>) -> McpServerCatalogDetails {
    let Some(catalog) = catalog else {
        return McpServerCatalogDetails {
            catalog_version: None,
            generated_at: None,
            server_info: JsonValue::Object(JsonMap::new()),
            server_instructions_hash: None,
            tools: Vec::new(),
            resources: Vec::new(),
            resource_templates: Vec::new(),
            prompts: Vec::new(),
        };
    };

    McpServerCatalogDetails {
        catalog_version: Some(catalog.catalog_version.clone()),
        generated_at: Some(catalog.generated_at_unix),
        server_info: parse_json_value(catalog.server_info_json.as_str()),
        server_instructions_hash: catalog.server_instructions_hash.clone(),
        tools: parse_json_array(catalog.tools_json.as_str())
            .into_iter()
            .filter_map(tool_catalog_item)
            .collect(),
        resources: parse_json_array(catalog.resources_json.as_str())
            .into_iter()
            .map(resource_catalog_item)
            .collect(),
        resource_templates: parse_json_array(catalog.resource_templates_json.as_str())
            .into_iter()
            .map(resource_template_catalog_item)
            .collect(),
        prompts: parse_json_array(catalog.prompts_json.as_str())
            .into_iter()
            .filter_map(prompt_catalog_item)
            .collect(),
    }
}

fn tool_catalog_item(value: JsonValue) -> Option<McpToolCatalogItem> {
    let name = string_field(&value, &["name"])?.trim().to_owned();
    if name.is_empty() {
        return None;
    }
    let annotations = value
        .get("annotations")
        .filter(|value| value.is_object())
        .map(tool_annotations);
    Some(McpToolCatalogItem {
        name,
        title: string_field(&value, &["title"]),
        description: string_field(&value, &["description"]),
        input_schema_summary: value
            .get("inputSchema")
            .or_else(|| value.get("input_schema"))
            .and_then(schema_summary),
        annotations,
    })
}

fn tool_annotations(value: &JsonValue) -> McpToolAnnotationSummary {
    McpToolAnnotationSummary {
        title: string_field(value, &["title"]),
        read_only_hint: bool_field(value, &["readOnlyHint", "read_only_hint"]),
        destructive_hint: bool_field(value, &["destructiveHint", "destructive_hint"]),
        idempotent_hint: bool_field(value, &["idempotentHint", "idempotent_hint"]),
        open_world_hint: bool_field(value, &["openWorldHint", "open_world_hint"]),
    }
}

fn resource_catalog_item(value: JsonValue) -> McpResourceCatalogItem {
    McpResourceCatalogItem {
        uri: string_field(&value, &["uri"]),
        name: string_field(&value, &["name"]),
        title: string_field(&value, &["title"]),
        mime_type: string_field(&value, &["mimeType", "mime_type"]),
        description: string_field(&value, &["description"]),
    }
}

fn resource_template_catalog_item(value: JsonValue) -> McpResourceTemplateCatalogItem {
    McpResourceTemplateCatalogItem {
        uri_template: string_field(&value, &["uriTemplate", "uri_template"]),
        name: string_field(&value, &["name"]),
        title: string_field(&value, &["title"]),
        mime_type: string_field(&value, &["mimeType", "mime_type"]),
        description: string_field(&value, &["description"]),
    }
}

fn prompt_catalog_item(value: JsonValue) -> Option<McpPromptCatalogItem> {
    let name = string_field(&value, &["name"])?.trim().to_owned();
    if name.is_empty() {
        return None;
    }
    let arguments_count = value
        .get("arguments")
        .and_then(JsonValue::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    Some(McpPromptCatalogItem {
        name,
        title: string_field(&value, &["title"]),
        description: string_field(&value, &["description"]),
        arguments_count,
    })
}

fn schema_summary(value: &JsonValue) -> Option<JsonValue> {
    if !value.is_object() {
        return None;
    }

    let mut summary = JsonMap::new();
    if let Some(schema_type) = string_field(value, &["type"]) {
        summary.insert("type".to_owned(), JsonValue::String(schema_type));
    }
    if let Some(properties) = value.get("properties").and_then(JsonValue::as_object) {
        summary.insert(
            "properties_count".to_owned(),
            JsonValue::from(properties.len() as u64),
        );
        summary.insert(
            "properties".to_owned(),
            JsonValue::Array(
                properties
                    .keys()
                    .take(24)
                    .map(|key| JsonValue::String(key.clone()))
                    .collect(),
            ),
        );
    }
    if let Some(required) = value.get("required").and_then(JsonValue::as_array) {
        summary.insert(
            "required_count".to_owned(),
            JsonValue::from(required.len() as u64),
        );
        summary.insert(
            "required".to_owned(),
            JsonValue::Array(
                required
                    .iter()
                    .filter_map(JsonValue::as_str)
                    .take(24)
                    .map(|key| JsonValue::String(key.to_owned()))
                    .collect(),
            ),
        );
    }

    if summary.is_empty() {
        None
    } else {
        Some(JsonValue::Object(summary))
    }
}

fn audit_summary_from_record(record: McpAuditEventRecord) -> McpAuditEventSummary {
    McpAuditEventSummary {
        turn_id: record.turn_id,
        server_installation_id: record.server_installation_id,
        server_name: record.server_name,
        raw_tool_name: record.raw_tool_name,
        callable_name: record.callable_name,
        catalog_version: record.catalog_version,
        action: record.action,
        decision: record.decision,
        reason_code: record.reason_code,
        details: sanitized_details(record.details_json.as_str()),
        created_at: record.created_at_unix,
    }
}

fn binding_summary_from_record(
    record: pioneer_crud::TurnMcpBindingRecord,
) -> McpTurnBindingSummary {
    McpTurnBindingSummary {
        server_installation_id: record.server_installation_id,
        server_name: record.server_name,
        raw_tool_name: record.raw_tool_name,
        callable_name: record.callable_name,
        catalog_version: record.catalog_version,
        fingerprint: record.fingerprint,
        selection_reason: record.selection_reason,
        capability_id: record.capability_id,
    }
}

fn sanitized_details(details_json: &str) -> JsonValue {
    let mut value = parse_json_value(details_json);
    sanitize_json_value(&mut value, None, 0);
    value
}

fn sanitize_json_value(value: &mut JsonValue, key: Option<&str>, depth: usize) {
    if key.is_some_and(is_sensitive_key) {
        *value = JsonValue::String("[redacted]".to_owned());
        return;
    }
    if depth >= 5 {
        *value = JsonValue::String("[truncated]".to_owned());
        return;
    }

    match value {
        JsonValue::Object(map) => {
            let keys = map.keys().cloned().collect::<Vec<_>>();
            for key in keys {
                if let Some(child) = map.get_mut(key.as_str()) {
                    sanitize_json_value(child, Some(key.as_str()), depth + 1);
                }
            }
            if map.len() > 64 {
                let mut trimmed = JsonMap::new();
                for (key, value) in std::mem::take(map).into_iter().take(64) {
                    trimmed.insert(key, value);
                }
                trimmed.insert("_truncated".to_owned(), JsonValue::Bool(true));
                *map = trimmed;
            }
        }
        JsonValue::Array(items) => {
            for item in items.iter_mut().take(64) {
                sanitize_json_value(item, key, depth + 1);
            }
            if items.len() > 64 {
                items.truncate(64);
                items.push(JsonValue::String("[truncated]".to_owned()));
            }
        }
        JsonValue::String(text) if text.len() > 512 => {
            text.truncate(512);
            text.push_str("...");
        }
        _ => {}
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("secret")
        || key.contains("token")
        || key.contains("password")
        || key.contains("authorization")
        || key.contains("bearer")
        || key == "env"
        || key == "headers"
        || key == "header"
        || key == "arguments"
        || key == "args"
        || key == "output"
        || key == "result"
}

fn parse_json_value(value: &str) -> JsonValue {
    serde_json::from_str(value).unwrap_or_else(|_| JsonValue::Object(JsonMap::new()))
}

fn parse_json_array(value: &str) -> Vec<JsonValue> {
    serde_json::from_str::<Vec<JsonValue>>(value).unwrap_or_default()
}

fn string_field(value: &JsonValue, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(JsonValue::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn bool_field(value: &JsonValue, keys: &[&str]) -> Option<bool> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(JsonValue::as_bool))
}
