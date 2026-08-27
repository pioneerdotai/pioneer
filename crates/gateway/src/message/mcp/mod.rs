use super::*;
use anyhow::{Context, Result};
use pioneer_crud::{
    McpAuditEventRecord, McpServerCatalogSnapshotRecord, McpServerInstallationRecord,
};
use pioneer_mcp::{
    InstallParseContext, McpScopeKind, McpSecretRef, McpServerInstallation, McpTransportConfig,
    parse_install_config,
};
use serde_json::json;
use std::collections::BTreeSet;
use std::str::FromStr;
use tracing::warn;

use crate::secrets::McpSecretDeleteReport;

const MCP_ERROR_INVALID_REQUEST: &str = "mcp.invalid_request";
const MCP_ERROR_NOT_FOUND: &str = "mcp.not_found";
const MCP_ERROR_INTERNAL: &str = "mcp.internal_error";

mod details;
mod install;
mod list;
mod policy;
mod restart;
mod uninstall;

impl MessageProcessor {
    pub(crate) async fn start_mcp_workspace_supervisor(self: &std::sync::Arc<Self>) {
        let this = self.clone();
        let _handle = tokio::spawn(async move {
            let trace = pioneer_observability::GatewayOperationTrace::start(
                pioneer_observability::GatewayOperation::McpWorkspaceInitialize,
            );
            let workspaces_stage =
                trace.stage(pioneer_observability::GatewayOperationStage::McpWorkspacesLoad);
            let workspaces = match this.workspace_manager.list_workspaces().await {
                Ok(workspaces) => {
                    workspaces_stage.succeed();
                    workspaces
                        .into_iter()
                        .filter(|workspace| workspace.is_active)
                        .collect::<Vec<_>>()
                }
                Err(error) => {
                    drop(workspaces_stage);
                    warn!(
                        error = %error,
                        "failed to list workspaces for MCP startup supervisor"
                    );
                    trace.finish_failure();
                    return;
                }
            };

            let mut failed = false;
            for workspace in workspaces {
                let reload_stage =
                    trace.stage(pioneer_observability::GatewayOperationStage::McpWorkspaceReload);
                if let Err(error) = this
                    .mcp_service
                    .reload_workspace(workspace.id.as_str())
                    .await
                {
                    drop(reload_stage);
                    failed = true;
                    warn!(
                        workspace_id = workspace.id.as_str(),
                        error = %format!("{error:#}"),
                        "failed to start MCP workspace runtime"
                    );
                } else {
                    reload_stage.succeed();
                }
            }
            if failed {
                trace.finish_failure();
            } else {
                trace.finish_success();
            }
        });
    }
}

fn mcp_error(
    request_id: Option<RequestId>,
    jsonrpc_code: i64,
    code: &'static str,
    message: impl Into<String>,
    details: serde_json::Value,
) -> JsonRpcErrorResponse {
    let message = message.into();
    let public_code = match code {
        MCP_ERROR_INVALID_REQUEST => pioneer_protocol::PublicErrorCode::InvalidInput,
        MCP_ERROR_NOT_FOUND => pioneer_protocol::PublicErrorCode::NotFound,
        _ => pioneer_protocol::PublicErrorCode::Internal,
    };
    let public_error = crate::public_error::map_agent_failure(
        public_code,
        pioneer_protocol::PublicErrorStage::Discovery,
        format!("{code}: {message}; details={details}"),
    );
    JsonRpcErrorResponse {
        jsonrpc: pioneer_protocol::JSONRPC_VERSION.to_owned(),
        id: request_id,
        error: pioneer_protocol::JsonRpcError {
            code: jsonrpc_code,
            message: public_error.message.clone(),
            data: Some(json!({
                "public_error": public_error,
            })),
        },
    }
}

fn to_protocol_validation(
    diagnostic: &pioneer_mcp::McpValidationDiagnostic,
) -> McpValidationDiagnostic {
    McpValidationDiagnostic {
        code: diagnostic.code.clone(),
        level: match diagnostic.level {
            pioneer_mcp::McpDiagnosticLevel::Warning => McpDiagnosticLevel::Warning,
            pioneer_mcp::McpDiagnosticLevel::Error => McpDiagnosticLevel::Error,
        },
        message: diagnostic.message.clone(),
        field_path: diagnostic.field_path.clone(),
    }
}

fn installation_record_from_domain(
    installation: &McpServerInstallation,
) -> Result<McpServerInstallationRecord> {
    Ok(McpServerInstallationRecord {
        id: None,
        scope_kind: installation.scope_kind.as_str().to_owned(),
        scope_key: installation.scope_key.clone(),
        name: installation.name.clone(),
        display_name: installation.display_name.clone(),
        source_kind: installation.source_kind.as_str().to_owned(),
        source_ref: serde_json::to_string(&installation.source_ref)
            .context("failed to encode MCP source_ref")?,
        transport_kind: installation.transport_kind().to_owned(),
        transport_json: serde_json::to_string(&installation.transport)
            .context("failed to encode MCP transport")?,
        auth_json: serde_json::to_string(&installation.auth)
            .context("failed to encode MCP auth config")?,
        secret_refs_json: serde_json::to_string(&installation.secret_refs)
            .context("failed to encode MCP secret refs")?,
        enabled: installation.enabled,
        allow_implicit_invocation: installation.allow_implicit_invocation,
        required: installation.required,
        fingerprint: installation.fingerprint.clone(),
        updated_at_unix: 0,
    })
}

fn parse_mcp_secret_ref_ids(secret_refs_json: &str) -> Result<BTreeSet<String>> {
    let refs = serde_json::from_str::<Vec<McpSecretRef>>(secret_refs_json)
        .context("failed to decode MCP secret refs")?;
    Ok(refs
        .into_iter()
        .map(|secret_ref| secret_ref.ref_id)
        .collect())
}

fn mcp_secret_ref_ids(refs: &[McpSecretRef]) -> BTreeSet<String> {
    refs.iter()
        .map(|secret_ref| secret_ref.ref_id.clone())
        .collect()
}

fn mcp_secret_label(server_name: &str, refs: &[McpSecretRef], ref_id: &str) -> String {
    refs.iter()
        .find(|secret_ref| secret_ref.ref_id == ref_id)
        .map(|secret_ref| format!("{server_name}:{}:{}", secret_ref.source, secret_ref.name))
        .unwrap_or_else(|| ref_id.to_owned())
}

fn warn_mcp_secret_delete_report(context: &str, report: &McpSecretDeleteReport) {
    if report.failed.is_empty() {
        return;
    }

    warn!(
        cleanup_context = context,
        attempted = report.attempted,
        deleted = report.deleted,
        missing = report.missing,
        failed = ?report.failed,
        "failed to delete MCP secrets"
    );
}

fn list_item_from_record(record: &McpServerInstallationRecord) -> McpListItem {
    list_item_from_record_with_catalog_and_runtime(record, None, None)
        .expect("fresh MCP installation record should map to protocol item")
}

fn list_item_from_record_with_catalog_and_runtime(
    record: &McpServerInstallationRecord,
    catalog: Option<&McpServerCatalogSnapshotRecord>,
    runtime: Option<&pioneer_mcp::McpServerRuntimeSnapshot>,
) -> Result<McpListItem> {
    let runtime_state = runtime
        .map(|runtime| protocol_runtime_state(runtime.state))
        .unwrap_or_else(|| {
            if record.enabled {
                McpRuntimeState::NotStarted
            } else {
                McpRuntimeState::Disabled
            }
        });
    Ok(McpListItem {
        id: record.id.clone().unwrap_or_default(),
        name: record.name.clone(),
        display_name: record.display_name.clone(),
        scope: protocol_scope_kind(record.scope_kind.as_str())?,
        policy: McpPolicyState {
            enabled: record.enabled,
            allow_implicit_invocation: record.allow_implicit_invocation,
        },
        required: record.required,
        runtime: McpRuntimeStatus {
            state: runtime_state,
            live: runtime.map(|runtime| runtime.live).unwrap_or(false),
            last_seen_at: runtime.and_then(|runtime| runtime.last_seen_at_unix),
        },
        tools_count: catalog
            .map(|catalog| count_json_array(catalog.tools_json.as_str()))
            .unwrap_or(0),
        resources_count: catalog
            .map(|catalog| count_json_array(catalog.resources_json.as_str()))
            .unwrap_or(0),
        resource_templates_count: catalog
            .map(|catalog| count_json_array(catalog.resource_templates_json.as_str()))
            .unwrap_or(0),
        prompts_count: catalog
            .map(|catalog| count_json_array(catalog.prompts_json.as_str()))
            .unwrap_or(0),
        status: McpServerStatus::from(runtime_state),
    })
}

fn protocol_runtime_state(state: pioneer_mcp::McpRuntimeState) -> McpRuntimeState {
    match state {
        pioneer_mcp::McpRuntimeState::NotStarted => McpRuntimeState::NotStarted,
        pioneer_mcp::McpRuntimeState::Disabled => McpRuntimeState::Disabled,
        pioneer_mcp::McpRuntimeState::Starting => McpRuntimeState::Starting,
        pioneer_mcp::McpRuntimeState::Ready => McpRuntimeState::Ready,
        pioneer_mcp::McpRuntimeState::Degraded => McpRuntimeState::Degraded,
        pioneer_mcp::McpRuntimeState::AuthRequired => McpRuntimeState::AuthRequired,
        pioneer_mcp::McpRuntimeState::Failed => McpRuntimeState::Failed,
        pioneer_mcp::McpRuntimeState::Stopping => McpRuntimeState::Stopping,
        pioneer_mcp::McpRuntimeState::Stopped => McpRuntimeState::Stopped,
        pioneer_mcp::McpRuntimeState::Restarting => McpRuntimeState::Restarting,
    }
}

fn count_json_array(value: &str) -> usize {
    serde_json::from_str::<Vec<serde_json::Value>>(value)
        .map(|items| items.len())
        .unwrap_or(0)
}

fn protocol_scope_kind(value: &str) -> Result<pioneer_protocol::McpScopeKind> {
    match value {
        "workspace" => Ok(pioneer_protocol::McpScopeKind::Workspace),
        "user" => Ok(pioneer_protocol::McpScopeKind::User),
        other => anyhow::bail!("unknown MCP scope kind `{other}`"),
    }
}

fn protocol_source_kind(value: &str) -> Result<McpSourceKind> {
    match value {
        "config" => Ok(McpSourceKind::Config),
        other => anyhow::bail!("unknown MCP source kind `{other}`"),
    }
}

fn transport_summary(kind: &str, transport_json: &str) -> Result<McpTransportSummary> {
    let parsed = serde_json::from_str::<McpTransportConfig>(transport_json)
        .with_context(|| format!("failed to decode normalized MCP transport `{kind}`"))?;
    match parsed {
        McpTransportConfig::Stdio { command, .. } => Ok(McpTransportSummary::Stdio { command }),
        McpTransportConfig::StreamableHttp { url, .. } => {
            Ok(McpTransportSummary::StreamableHttp { url })
        }
    }
}

impl MessageProcessor {
    pub(crate) async fn notify_mcp_changed(
        &self,
        workspace_id: &str,
        changed: Vec<McpChangedItem>,
        _event_timestamp_secs: i64,
    ) {
        if changed.is_empty() {
            return;
        }

        self.publish_resource_selector_change(workspace_id).await;

        let snapshot_version = self.next_mcp_snapshot_version();
        let notification = McpChangedNotification {
            workspace_id: workspace_id.to_owned(),
            snapshot_version,
            changed,
        };
        self.send_gateway_management_notification(events::MCP_CHANGED, &notification)
            .await;
    }

    async fn validate_mcp_workspace(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        workspace_id: String,
        method: &str,
    ) -> std::result::Result<String, JsonRpcErrorResponse> {
        if workspace_id.trim().is_empty() {
            return Err(mcp_error(
                Some(request_id),
                INVALID_PARAMS_CODE,
                MCP_ERROR_INVALID_REQUEST,
                format!("`workspace_id` is required for `{method}`"),
                json!({}),
            ));
        }

        let workspace_id = self
            .workspace_manager
            .validate_workspace_id(workspace_id.as_str())
            .await
            .map_err(|error| match error {
                WorkspaceError::Internal(message) => mcp_error(
                    Some(request_id.clone()),
                    INVALID_REQUEST_CODE,
                    MCP_ERROR_INTERNAL,
                    format!("failed to validate workspace: {message}"),
                    json!({}),
                ),
                WorkspaceError::WorkspaceNotFound(_) | WorkspaceError::WorkspaceInactive(_) => {
                    mcp_error(
                        Some(request_id.clone()),
                        INVALID_PARAMS_CODE,
                        MCP_ERROR_NOT_FOUND,
                        format!("workspace `{}` is unavailable", workspace_id),
                        json!({"workspace_id": workspace_id}),
                    )
                }
                WorkspaceError::InvalidWorkspaceId
                | WorkspaceError::InvalidWorkspaceName
                | WorkspaceError::NoWorkspaceUpdateFields => mcp_error(
                    Some(request_id.clone()),
                    INVALID_PARAMS_CODE,
                    MCP_ERROR_INVALID_REQUEST,
                    format!("invalid workspace_id for `{method}`"),
                    json!({"workspace_id": workspace_id}),
                ),
            })?;

        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;

        Ok(workspace_id)
    }
}
