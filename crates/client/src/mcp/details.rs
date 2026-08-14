//! MCP details state.

use super::list;
use pioneer_protocol::{McpListItem, McpServerDetailsParams, McpServerDetailsResponse};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct McpDetailsRefreshSuccessReduction {
    pub servers: Vec<McpListItem>,
    pub selected_server_id: Option<String>,
    pub server_details: Option<McpServerDetailsResponse>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct McpDetailsRefreshFailureReduction {
    pub error: String,
}

pub fn mcp_server_details_params(
    workspace_id: impl Into<String>,
    server_id: impl Into<String>,
) -> McpServerDetailsParams {
    McpServerDetailsParams {
        workspace_id: workspace_id.into(),
        server_id: server_id.into(),
    }
}

pub fn selected_mcp_server<'a>(
    servers: &'a [McpListItem],
    selected_server_id: Option<&str>,
) -> Option<&'a McpListItem> {
    selected_server_id.and_then(|server_id| list::find_mcp_server_by_id(servers, server_id))
}

pub fn mcp_details_server(
    servers: &[McpListItem],
    selected_server_id: Option<&str>,
    details: Option<&McpServerDetailsResponse>,
) -> Option<McpListItem> {
    details
        .map(|details| details.server.clone())
        .or_else(|| selected_mcp_server(servers, selected_server_id).cloned())
}

pub fn mcp_details_match_selected(
    details: Option<&McpServerDetailsResponse>,
    selected_server_id: Option<&str>,
) -> bool {
    match (details, selected_server_id) {
        (Some(details), Some(server_id)) => details.server.id == server_id,
        _ => false,
    }
}

pub fn apply_mcp_details_response(
    servers: &mut [McpListItem],
    selected_server_id: &mut Option<String>,
    server_details: &mut Option<McpServerDetailsResponse>,
    details: McpServerDetailsResponse,
) {
    *selected_server_id = Some(details.server.id.clone());
    for server in servers {
        if server.id == details.server.id {
            *server = details.server.clone();
        }
    }
    *server_details = Some(details);
}

pub fn reduce_mcp_details_refresh_success(
    mut servers: Vec<McpListItem>,
    mut selected_server_id: Option<String>,
    mut server_details: Option<McpServerDetailsResponse>,
    details: McpServerDetailsResponse,
) -> McpDetailsRefreshSuccessReduction {
    apply_mcp_details_response(
        servers.as_mut_slice(),
        &mut selected_server_id,
        &mut server_details,
        details,
    );

    McpDetailsRefreshSuccessReduction {
        servers,
        selected_server_id,
        server_details,
    }
}

pub fn reduce_mcp_details_refresh_failure(
    error: impl Into<String>,
) -> McpDetailsRefreshFailureReduction {
    McpDetailsRefreshFailureReduction {
        error: error.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        McpManagementDetails, McpPolicyState, McpRuntimeState, McpRuntimeStatus, McpScopeKind,
        McpServerCatalogDetails, McpServerHealthDetails, McpServerStatus, McpSourceKind,
        McpTransportSummary,
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
    fn details_params_preserve_workspace_and_server() {
        let params = mcp_server_details_params("workspace", "server");
        assert_eq!(params.workspace_id, "workspace");
        assert_eq!(params.server_id, "server");
    }

    #[test]
    fn details_server_prefers_details_over_list_selection() {
        let servers = vec![server("id-list", "list")];
        let details = details("id-details", "details");

        let projected =
            mcp_details_server(&servers, Some("id-list"), Some(&details)).expect("details server");
        assert_eq!(projected.id, "id-details");

        let projected = mcp_details_server(&servers, Some("id-list"), None).expect("list server");
        assert_eq!(projected.id, "id-list");
    }

    #[test]
    fn details_response_updates_selection_and_matching_list_item() {
        let mut servers = vec![server("id-github", "old-name")];
        let mut selected = None;
        let mut stored_details = None;

        apply_mcp_details_response(
            &mut servers,
            &mut selected,
            &mut stored_details,
            details("id-github", "github"),
        );

        assert_eq!(selected.as_deref(), Some("id-github"));
        assert_eq!(servers[0].name, "github");
        assert!(mcp_details_match_selected(
            stored_details.as_ref(),
            selected.as_deref()
        ));
    }

    #[test]
    fn details_refresh_success_reduction_updates_selection_and_matching_list_item() {
        let reduction = reduce_mcp_details_refresh_success(
            vec![server("id-github", "old-name")],
            None,
            None,
            details("id-github", "github"),
        );

        assert_eq!(reduction.selected_server_id.as_deref(), Some("id-github"));
        assert_eq!(reduction.servers[0].name, "github");
        assert!(mcp_details_match_selected(
            reduction.server_details.as_ref(),
            reduction.selected_server_id.as_deref()
        ));
    }

    #[test]
    fn details_refresh_failure_reduction_carries_display_error() {
        let reduction = reduce_mcp_details_refresh_failure("load failed");

        assert_eq!(reduction.error, "load failed");
    }
}
