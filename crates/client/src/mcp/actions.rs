//! MCP action orchestration.

use super::list;
use pioneer_protocol::{
    McpDiagnosticLevel, McpInstallParams, McpInstallResponse, McpInstallResultStatus,
    McpInstallStatus, McpListItem, McpPolicySetParams, McpScopeKind, McpServerDetailsResponse,
    McpServerRestartParams, McpServerStatus, McpUninstallParams,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpActionUnavailable {
    GatewayNotConnected,
    WorkspaceNotSelected,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct McpActionScope {
    pub connection_id: u64,
    pub workspace_id: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpActionScopePlan {
    Send(McpActionScope),
    Unavailable(McpActionUnavailable),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpConfigValidationError {
    InvalidJson { error: String },
    ServersRequired,
    ServersEmpty,
    ServerNameEmpty,
    ServerConfigObject { name: String },
    CommandOrUrlRequired { name: String },
    CommandUrlExclusive { name: String },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpInstallFieldIssue {
    ServerValidationError {
        name: String,
    },
    Diagnostic {
        name: String,
        level: McpDiagnosticLevel,
        message: String,
        field_path: Option<String>,
    },
}

pub fn normalize_mcp_config_json(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        None
    } else {
        Some(raw.to_owned())
    }
}

pub fn validate_mcp_config_for_submit(raw: &str) -> Result<(), McpConfigValidationError> {
    let value = serde_json::from_str::<serde_json::Value>(raw).map_err(|error| {
        McpConfigValidationError::InvalidJson {
            error: error.to_string(),
        }
    })?;
    let servers = value
        .get("mcpServers")
        .and_then(serde_json::Value::as_object)
        .ok_or(McpConfigValidationError::ServersRequired)?;
    if servers.is_empty() {
        return Err(McpConfigValidationError::ServersEmpty);
    }

    for (name, server) in servers {
        if name.trim().is_empty() {
            return Err(McpConfigValidationError::ServerNameEmpty);
        }
        let Some(server) = server.as_object() else {
            return Err(McpConfigValidationError::ServerConfigObject { name: name.clone() });
        };
        let has_command = server.contains_key("command");
        let has_url = server.contains_key("url");
        match (has_command, has_url) {
            (true, false) | (false, true) => {}
            (false, false) => {
                return Err(McpConfigValidationError::CommandOrUrlRequired { name: name.clone() });
            }
            (true, true) => {
                return Err(McpConfigValidationError::CommandUrlExclusive { name: name.clone() });
            }
        }
    }

    Ok(())
}

pub fn plan_mcp_action_scope(
    gateway_connected: bool,
    connection_id: Option<u64>,
    workspace_id: Option<String>,
) -> McpActionScopePlan {
    let Some(connection_id) = available_connection_id(gateway_connected, connection_id) else {
        return McpActionScopePlan::Unavailable(McpActionUnavailable::GatewayNotConnected);
    };
    let Some(workspace_id) = workspace_id else {
        return McpActionScopePlan::Unavailable(McpActionUnavailable::WorkspaceNotSelected);
    };

    McpActionScopePlan::Send(McpActionScope {
        connection_id,
        workspace_id,
    })
}

pub fn mcp_install_params(
    workspace_id: impl Into<String>,
    config_json: impl Into<String>,
) -> McpInstallParams {
    McpInstallParams {
        workspace_id: workspace_id.into(),
        config_json: config_json.into(),
        scope_kind: McpScopeKind::Workspace,
        enabled: true,
        allow_implicit_invocation: false,
    }
}

pub fn mcp_policy_set_params(
    workspace_id: impl Into<String>,
    name: impl Into<String>,
    enabled: bool,
    allow_implicit_invocation: bool,
) -> McpPolicySetParams {
    McpPolicySetParams {
        workspace_id: workspace_id.into(),
        name: name.into(),
        scope_kind: McpScopeKind::Workspace,
        enabled: Some(enabled),
        allow_implicit_invocation: Some(allow_implicit_invocation),
    }
}

pub fn mcp_server_restart_params(
    workspace_id: impl Into<String>,
    name: impl Into<String>,
) -> McpServerRestartParams {
    McpServerRestartParams {
        workspace_id: workspace_id.into(),
        name: name.into(),
        scope_kind: McpScopeKind::Workspace,
    }
}

pub fn mcp_uninstall_params(
    workspace_id: impl Into<String>,
    name: impl Into<String>,
) -> McpUninstallParams {
    McpUninstallParams {
        workspace_id: workspace_id.into(),
        name: name.into(),
        scope_kind: McpScopeKind::Workspace,
    }
}

pub fn mcp_policy_values(servers: &[McpListItem], name: &str) -> Option<(bool, bool)> {
    let name = list::normalize_mcp_server_name(name)?;
    servers
        .iter()
        .find(|server| server.name == name)
        .map(|server| {
            (
                server.policy.enabled,
                server.policy.allow_implicit_invocation,
            )
        })
}

pub fn apply_local_mcp_policy(
    servers: &mut [McpListItem],
    details: &mut Option<McpServerDetailsResponse>,
    name: &str,
    enabled: bool,
    allow_implicit_invocation: bool,
) {
    for server in servers {
        if server.name == name {
            server.policy.enabled = enabled;
            server.policy.allow_implicit_invocation = allow_implicit_invocation;
            if !enabled {
                server.status = McpServerStatus::Disabled;
            }
        }
    }

    if let Some(details) = details.as_mut() {
        if details.server.name == name {
            details.server.policy.enabled = enabled;
            details.server.policy.allow_implicit_invocation = allow_implicit_invocation;
            if !enabled {
                details.server.status = McpServerStatus::Disabled;
                details.health.status = McpServerStatus::Disabled;
            }
        }
    }
}

pub fn mcp_action_matches_connection(
    action_connection_id: u64,
    current_connection_id: Option<u64>,
) -> bool {
    current_connection_id == Some(action_connection_id)
}

fn available_connection_id(gateway_connected: bool, connection_id: Option<u64>) -> Option<u64> {
    gateway_connected.then_some(connection_id).flatten()
}

pub fn mcp_action_should_refresh_details(selected_server_id: Option<&str>) -> bool {
    selected_server_id.is_some()
}

pub fn apply_mcp_uninstall_success(
    selected_server_id: &mut Option<String>,
    server_details: &mut Option<McpServerDetailsResponse>,
) {
    *selected_server_id = None;
    *server_details = None;
}

pub fn mcp_install_has_success(response: &McpInstallResponse) -> bool {
    response.servers.iter().any(|item| {
        matches!(
            item.status,
            McpInstallResultStatus::Installed | McpInstallResultStatus::Updated
        )
    })
}

pub fn mcp_install_should_close_dialog(response: &McpInstallResponse) -> bool {
    response.status == McpInstallStatus::Ok
}

pub fn mcp_install_response_field_issues(
    response: &McpInstallResponse,
) -> Vec<McpInstallFieldIssue> {
    let mut issues = Vec::new();

    for item in response
        .servers
        .iter()
        .filter(|item| item.status == McpInstallResultStatus::ValidationError)
    {
        if item.diagnostics.is_empty() {
            issues.push(McpInstallFieldIssue::ServerValidationError {
                name: item.name.clone(),
            });
            continue;
        }

        for diagnostic in &item.diagnostics {
            issues.push(McpInstallFieldIssue::Diagnostic {
                name: item.name.clone(),
                level: diagnostic.level,
                message: diagnostic.message.clone(),
                field_path: diagnostic.field_path.clone(),
            });
        }
    }

    issues
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        McpInstallResult, McpLifecycleAuditSummary, McpPolicyState, McpRuntimeState,
        McpRuntimeStatus, McpServerCatalogDetails, McpServerHealthDetails, McpSourceKind,
        McpTransportSummary, McpValidationDiagnostic,
    };

    fn server(id: &str, name: &str) -> McpListItem {
        McpListItem {
            id: id.to_owned(),
            name: name.to_owned(),
            display_name: None,
            scope: McpScopeKind::Workspace,
            source_kind: McpSourceKind::Config,
            transport: McpTransportSummary::Stdio {
                command: "node".to_owned(),
            },
            policy: McpPolicyState {
                enabled: true,
                allow_implicit_invocation: false,
            },
            required: false,
            fingerprint: format!("{id}:{name}:fingerprint"),
            runtime: McpRuntimeStatus {
                state: McpRuntimeState::Ready,
                live: true,
                last_seen_at: None,
                last_error: None,
            },
            tools_count: 1,
            resources_count: 0,
            resource_templates_count: 0,
            prompts_count: 0,
            status: McpServerStatus::Ready,
            status_reason: None,
        }
    }

    fn details(id: &str, name: &str) -> McpServerDetailsResponse {
        let server = server(id, name);
        McpServerDetailsResponse {
            snapshot_version: 1,
            generated_at: 10,
            health: McpServerHealthDetails {
                runtime: server.runtime.clone(),
                status: server.status,
                status_reason: None,
                last_error: None,
                retry_attempt: None,
                next_retry_at: None,
                catalog_version: None,
                stderr_tail: None,
            },
            server,
            catalog: McpServerCatalogDetails {
                catalog_version: None,
                generated_at: None,
                server_info: serde_json::Value::Null,
                server_instructions_hash: None,
                tools: Vec::new(),
                resources: Vec::new(),
                resource_templates: Vec::new(),
                prompts: Vec::new(),
            },
            audit: Vec::new(),
            recent_bindings: Vec::new(),
        }
    }

    #[test]
    fn config_validation_accepts_command_or_url_only() {
        validate_mcp_config_for_submit(r#"{"mcpServers":{"github":{"command":"gh"}}}"#)
            .expect("command config");
        validate_mcp_config_for_submit(r#"{"mcpServers":{"web":{"url":"https://mcp.test"}}}"#)
            .expect("url config");

        assert!(matches!(
            validate_mcp_config_for_submit(r#"{"mcpServers":{"bad":{}}}"#),
            Err(McpConfigValidationError::CommandOrUrlRequired { name }) if name == "bad"
        ));
        assert!(matches!(
            validate_mcp_config_for_submit(r#"{"mcpServers":{"bad":{"command":"n","url":"u"}}}"#),
            Err(McpConfigValidationError::CommandUrlExclusive { name }) if name == "bad"
        ));
    }

    #[test]
    fn action_params_default_to_workspace_scope() {
        let install = mcp_install_params("workspace", "{}");
        assert_eq!(install.workspace_id, "workspace");
        assert_eq!(install.scope_kind, McpScopeKind::Workspace);
        assert!(install.enabled);
        assert!(!install.allow_implicit_invocation);

        let policy = mcp_policy_set_params("workspace", "github", false, true);
        assert_eq!(policy.name, "github");
        assert_eq!(policy.enabled, Some(false));
        assert_eq!(policy.allow_implicit_invocation, Some(true));

        let restart = mcp_server_restart_params("workspace", "github");
        assert_eq!(restart.name, "github");
        let uninstall = mcp_uninstall_params("workspace", "github");
        assert_eq!(uninstall.name, "github");
    }

    #[test]
    fn action_scope_plan_reports_connection_and_workspace_availability() {
        assert_eq!(
            plan_mcp_action_scope(true, Some(7), Some("workspace".to_owned())),
            McpActionScopePlan::Send(McpActionScope {
                connection_id: 7,
                workspace_id: "workspace".to_owned(),
            })
        );
        assert!(matches!(
            plan_mcp_action_scope(false, Some(7), Some("workspace".to_owned())),
            McpActionScopePlan::Unavailable(McpActionUnavailable::GatewayNotConnected)
        ));
        assert!(matches!(
            plan_mcp_action_scope(true, None, Some("workspace".to_owned())),
            McpActionScopePlan::Unavailable(McpActionUnavailable::GatewayNotConnected)
        ));
        assert!(matches!(
            plan_mcp_action_scope(true, Some(7), None),
            McpActionScopePlan::Unavailable(McpActionUnavailable::WorkspaceNotSelected)
        ));
    }

    #[test]
    fn local_policy_projection_updates_list_and_details() {
        let mut servers = vec![server("id-github", "github")];
        let mut details = Some(details("id-github", "github"));

        assert_eq!(mcp_policy_values(&servers, " github "), Some((true, false)));

        apply_local_mcp_policy(&mut servers, &mut details, "github", false, true);

        assert_eq!(servers[0].policy.enabled, false);
        assert_eq!(servers[0].policy.allow_implicit_invocation, true);
        assert_eq!(servers[0].status, McpServerStatus::Disabled);
        let details = details.expect("details");
        assert_eq!(details.server.policy.enabled, false);
        assert_eq!(details.health.status, McpServerStatus::Disabled);
    }

    #[test]
    fn install_response_helpers_extract_success_and_validation_issues() {
        let response = McpInstallResponse {
            status: McpInstallStatus::Partial,
            servers: vec![
                McpInstallResult {
                    name: "github".to_owned(),
                    status: McpInstallResultStatus::Updated,
                    diagnostics: Vec::new(),
                    server: None,
                },
                McpInstallResult {
                    name: "bad".to_owned(),
                    status: McpInstallResultStatus::ValidationError,
                    diagnostics: vec![McpValidationDiagnostic {
                        code: "missing_command".to_owned(),
                        level: McpDiagnosticLevel::Error,
                        message: "missing command".to_owned(),
                        field_path: Some("command".to_owned()),
                    }],
                    server: None,
                },
            ],
            audit: McpLifecycleAuditSummary { events_written: 1 },
        };

        assert!(mcp_install_has_success(&response));
        assert!(!mcp_install_should_close_dialog(&response));
        let issues = mcp_install_response_field_issues(&response);
        assert_eq!(issues.len(), 1);
        assert!(matches!(
            &issues[0],
            McpInstallFieldIssue::Diagnostic { name, level, field_path, .. }
                if name == "bad"
                    && *level == McpDiagnosticLevel::Error
                    && field_path.as_deref() == Some("command")
        ));
    }

    #[test]
    fn action_connection_and_uninstall_helpers_are_deterministic() {
        assert!(mcp_action_matches_connection(5, Some(5)));
        assert!(!mcp_action_matches_connection(5, Some(6)));
        assert!(mcp_action_should_refresh_details(Some("server")));
        assert!(!mcp_action_should_refresh_details(None));

        let mut selected = Some("server".to_owned());
        let mut details = Some(details("server", "github"));
        apply_mcp_uninstall_success(&mut selected, &mut details);
        assert_eq!(selected, None);
        assert!(details.is_none());
    }
}
