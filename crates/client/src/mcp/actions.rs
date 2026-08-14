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

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpInstallFieldError {
    ValidationIssues(Vec<McpInstallFieldIssue>),
    Failure { message: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum McpInstallFinishOutcome {
    Response(McpInstallResponse),
    Failure { field_error: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpInstallFinishReduction {
    pub pending: McpPendingReduction,
    pub field_error: Option<McpInstallFieldError>,
    pub close_dialog: bool,
    pub clear_mcp_error: bool,
    pub queue_refresh: bool,
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
                if let Some(management) = details.management.as_mut() {
                    management.health.status = McpServerStatus::Disabled;
                }
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpActionTarget {
    pub name: String,
}

impl McpActionTarget {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum McpActionFinishKind {
    Policy(McpActionTarget),
    Restart(McpActionTarget),
    Uninstall(McpActionTarget),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum McpActionFinishOutcome {
    Success,
    Failure { error: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpPendingReduction {
    pub target: McpActionTarget,
    pub pending: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpActionFinishReduction {
    pub pending: McpPendingReduction,
    pub error: Option<String>,
    pub queue_refresh: bool,
    pub queue_details_refresh: bool,
    pub clear_selected_details: bool,
    pub rollback_policy: bool,
}

pub fn reduce_mcp_action_finish(
    kind: McpActionFinishKind,
    outcome: McpActionFinishOutcome,
    selected_server_id: Option<&str>,
) -> McpActionFinishReduction {
    let (target, clear_selected_details, rollbackable_policy) = match kind {
        McpActionFinishKind::Policy(target) => (target, false, true),
        McpActionFinishKind::Restart(target) => (target, false, false),
        McpActionFinishKind::Uninstall(target) => (target, true, false),
    };
    let pending = McpPendingReduction {
        target,
        pending: false,
    };

    match outcome {
        McpActionFinishOutcome::Success => McpActionFinishReduction {
            pending,
            error: None,
            queue_refresh: true,
            queue_details_refresh: !clear_selected_details
                && mcp_action_should_refresh_details(selected_server_id),
            clear_selected_details,
            rollback_policy: false,
        },
        McpActionFinishOutcome::Failure { error } => McpActionFinishReduction {
            pending,
            error: Some(error),
            queue_refresh: false,
            queue_details_refresh: false,
            clear_selected_details: false,
            rollback_policy: rollbackable_policy,
        },
    }
}

pub fn reduce_mcp_install_finish(outcome: McpInstallFinishOutcome) -> McpInstallFinishReduction {
    let pending = McpPendingReduction {
        target: McpActionTarget::new(list::MCP_INSTALL_PENDING_KEY),
        pending: false,
    };

    match outcome {
        McpInstallFinishOutcome::Response(response) => {
            let close_dialog = mcp_install_should_close_dialog(&response);
            McpInstallFinishReduction {
                pending,
                field_error: if close_dialog {
                    None
                } else {
                    Some(McpInstallFieldError::ValidationIssues(
                        mcp_install_response_field_issues(&response),
                    ))
                },
                close_dialog,
                clear_mcp_error: close_dialog,
                queue_refresh: mcp_install_has_success(&response) || close_dialog,
            }
        }
        McpInstallFinishOutcome::Failure { field_error } => McpInstallFinishReduction {
            pending,
            field_error: Some(McpInstallFieldError::Failure {
                message: field_error,
            }),
            close_dialog: false,
            clear_mcp_error: false,
            queue_refresh: false,
        },
    }
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
        McpInstallResult, McpLifecycleAuditSummary, McpManagementDetails, McpPolicyState,
        McpRuntimeState, McpRuntimeStatus, McpServerCatalogDetails, McpServerHealthDetails,
        McpSourceKind, McpTransportSummary, McpValidationDiagnostic,
    };

    fn server(id: &str, name: &str) -> McpListItem {
        McpListItem {
            id: id.to_owned(),
            name: name.to_owned(),
            display_name: None,
            scope: McpScopeKind::Workspace,
            policy: McpPolicyState {
                enabled: true,
                allow_implicit_invocation: false,
            },
            required: false,
            runtime: McpRuntimeStatus {
                state: McpRuntimeState::Ready,
                live: true,
                last_seen_at: None,
            },
            tools_count: 1,
            resources_count: 0,
            resource_templates_count: 0,
            prompts_count: 0,
            status: McpServerStatus::Ready,
        }
    }

    fn details(id: &str, name: &str) -> McpServerDetailsResponse {
        let server = server(id, name);
        McpServerDetailsResponse {
            snapshot_version: 1,
            generated_at: 10,
            management: Some(McpManagementDetails {
                scope: McpScopeKind::Workspace,
                source_kind: McpSourceKind::Config,
                transport: McpTransportSummary::Stdio {
                    command: "node".to_owned(),
                },
                fingerprint: format!("{id}:{name}:fingerprint"),
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
                audit: Vec::new(),
                recent_bindings: Vec::new(),
            }),
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
        assert_eq!(
            details.management.expect("management").health.status,
            McpServerStatus::Disabled
        );
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
    fn install_finish_reducer_projects_partial_validation_response() {
        let response = McpInstallResponse {
            status: McpInstallStatus::Partial,
            servers: vec![
                McpInstallResult {
                    name: "github".to_owned(),
                    status: McpInstallResultStatus::Installed,
                    diagnostics: Vec::new(),
                    server: None,
                },
                McpInstallResult {
                    name: "bad".to_owned(),
                    status: McpInstallResultStatus::ValidationError,
                    diagnostics: Vec::new(),
                    server: None,
                },
            ],
            audit: McpLifecycleAuditSummary { events_written: 1 },
        };

        let reduction = reduce_mcp_install_finish(McpInstallFinishOutcome::Response(response));

        assert_eq!(reduction.pending.target.name, list::MCP_INSTALL_PENDING_KEY);
        assert!(!reduction.pending.pending);
        assert!(reduction.queue_refresh);
        assert!(!reduction.close_dialog);
        assert!(!reduction.clear_mcp_error);
        assert!(matches!(
            reduction.field_error,
            Some(McpInstallFieldError::ValidationIssues(ref issues))
                if matches!(
                    issues.as_slice(),
                    [McpInstallFieldIssue::ServerValidationError { name }] if name == "bad"
                )
        ));
    }

    #[test]
    fn install_finish_reducer_projects_success_and_failure() {
        let response = McpInstallResponse {
            status: McpInstallStatus::Ok,
            servers: vec![McpInstallResult {
                name: "github".to_owned(),
                status: McpInstallResultStatus::Updated,
                diagnostics: Vec::new(),
                server: None,
            }],
            audit: McpLifecycleAuditSummary { events_written: 1 },
        };
        let success = reduce_mcp_install_finish(McpInstallFinishOutcome::Response(response));
        assert!(success.close_dialog);
        assert!(success.clear_mcp_error);
        assert!(success.queue_refresh);
        assert!(success.field_error.is_none());

        let failure = reduce_mcp_install_finish(McpInstallFinishOutcome::Failure {
            field_error: "install failed".to_owned(),
        });
        assert_eq!(
            failure.field_error,
            Some(McpInstallFieldError::Failure {
                message: "install failed".to_owned()
            })
        );
        assert!(!failure.queue_refresh);
        assert!(!failure.close_dialog);
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

    #[test]
    fn action_finish_reducer_projects_policy_success() {
        let reduction = reduce_mcp_action_finish(
            McpActionFinishKind::Policy(McpActionTarget::new("github")),
            McpActionFinishOutcome::Success,
            Some("id-github"),
        );

        assert_eq!(reduction.pending.target.name, "github");
        assert!(!reduction.pending.pending);
        assert!(reduction.error.is_none());
        assert!(reduction.queue_refresh);
        assert!(reduction.queue_details_refresh);
        assert!(!reduction.clear_selected_details);
        assert!(!reduction.rollback_policy);
    }

    #[test]
    fn action_finish_reducer_projects_uninstall_success() {
        let reduction = reduce_mcp_action_finish(
            McpActionFinishKind::Uninstall(McpActionTarget::new("github")),
            McpActionFinishOutcome::Success,
            Some("id-github"),
        );

        assert!(reduction.queue_refresh);
        assert!(!reduction.queue_details_refresh);
        assert!(reduction.clear_selected_details);
        assert!(!reduction.rollback_policy);
    }

    #[test]
    fn action_finish_reducer_requests_policy_rollback_on_failure() {
        let reduction = reduce_mcp_action_finish(
            McpActionFinishKind::Policy(McpActionTarget::new("github")),
            McpActionFinishOutcome::Failure {
                error: "policy failed".to_owned(),
            },
            Some("id-github"),
        );

        assert_eq!(reduction.error.as_deref(), Some("policy failed"));
        assert!(!reduction.queue_refresh);
        assert!(!reduction.queue_details_refresh);
        assert!(!reduction.clear_selected_details);
        assert!(reduction.rollback_policy);
    }
}
