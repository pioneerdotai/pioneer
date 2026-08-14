//! MCP notification refresh decisions.

use crate::notifications::router::should_refresh_workspace_bound_data;
use pioneer_protocol::{
    McpChangedNotification, McpListItem, McpServerCatalogChangedNotification,
    McpServerDetailsResponse, McpServerStatusChangedNotification,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpRefreshReduction {
    pub workspace_id: String,
    pub workspace_matches: bool,
    pub queue_mcp_refresh: bool,
    pub queue_mcp_details_refresh: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpServerStatusChangedReduction {
    pub notification: McpServerStatusChangedNotification,
    pub workspace_matches: bool,
    pub selected_server_matches: bool,
    pub update_selected_details: bool,
    pub queue_mcp_details_refresh: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpServerCatalogChangedReduction {
    pub notification: McpServerCatalogChangedNotification,
    pub workspace_matches: bool,
    pub selected_server_matches: bool,
    pub queue_mcp_details_refresh: bool,
}

pub fn reduce_mcp_changed_notification(
    notification: McpChangedNotification,
    current_workspace: Option<&str>,
    selected_server_id: Option<&str>,
) -> McpRefreshReduction {
    let workspace_matches =
        should_refresh_workspace_bound_data(current_workspace, notification.workspace_id.as_str());

    McpRefreshReduction {
        workspace_id: notification.workspace_id,
        workspace_matches,
        queue_mcp_refresh: workspace_matches,
        queue_mcp_details_refresh: workspace_matches && selected_server_id.is_some(),
    }
}

pub fn reduce_mcp_server_status_changed_notification(
    notification: McpServerStatusChangedNotification,
    current_workspace: Option<&str>,
    selected_server_id: Option<&str>,
    has_selected_details: bool,
) -> McpServerStatusChangedReduction {
    let workspace_matches =
        should_refresh_workspace_bound_data(current_workspace, notification.workspace_id.as_str());
    let selected_server_matches = selected_server_id == Some(notification.server.id.as_str());

    McpServerStatusChangedReduction {
        notification,
        workspace_matches,
        selected_server_matches,
        update_selected_details: workspace_matches
            && selected_server_matches
            && has_selected_details,
        queue_mcp_details_refresh: workspace_matches
            && selected_server_matches
            && !has_selected_details,
    }
}

pub fn apply_mcp_server_status_changed_to_catalog(
    servers: &mut [McpListItem],
    notification: &McpServerStatusChangedNotification,
) {
    for server in servers {
        if server.id == notification.server.id {
            server.runtime = notification.server.runtime.clone();
            server.status = notification.server.status;
        }
    }
}

pub fn apply_mcp_server_status_changed_to_details(
    details: &mut McpServerDetailsResponse,
    notification: &McpServerStatusChangedNotification,
) {
    details.server.runtime = notification.server.runtime.clone();
    details.server.status = notification.server.status;
    if let Some(management) = details.management.as_mut() {
        management.health.runtime = notification.server.runtime.clone();
        management.health.status = notification.server.status;
    }
}

pub fn reduce_mcp_server_catalog_changed_notification(
    notification: McpServerCatalogChangedNotification,
    current_workspace: Option<&str>,
    selected_server_id: Option<&str>,
) -> McpServerCatalogChangedReduction {
    let workspace_matches =
        should_refresh_workspace_bound_data(current_workspace, notification.workspace_id.as_str());
    let selected_server_matches = selected_server_id == Some(notification.server_id.as_str());

    McpServerCatalogChangedReduction {
        notification,
        workspace_matches,
        selected_server_matches,
        queue_mcp_details_refresh: workspace_matches && selected_server_matches,
    }
}

pub fn apply_mcp_server_catalog_changed_to_catalog(
    servers: &mut [McpListItem],
    notification: &McpServerCatalogChangedNotification,
) {
    for server in servers {
        if server.id == notification.server_id {
            server.tools_count = notification.tools_count;
            server.resources_count = notification.resources_count;
            server.resource_templates_count = notification.resource_templates_count;
            server.prompts_count = notification.prompts_count;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        McpManagementDetails, McpPolicyState, McpRuntimeState, McpRuntimeStatus, McpScopeKind,
        McpServerCatalogDetails, McpServerHealthDetails, McpServerStatus, McpServerStatusItem,
        McpSourceKind, McpTransportSummary,
    };

    fn mcp_runtime(state: McpRuntimeState, live: bool) -> McpRuntimeStatus {
        McpRuntimeStatus {
            state,
            live,
            last_seen_at: None,
        }
    }

    fn mcp_server(id: &str, status: McpServerStatus) -> McpListItem {
        McpListItem {
            id: id.to_owned(),
            name: id.to_owned(),
            display_name: None,
            scope: McpScopeKind::Workspace,
            policy: McpPolicyState {
                enabled: true,
                allow_implicit_invocation: false,
            },
            required: false,
            runtime: mcp_runtime(McpRuntimeState::Ready, true),
            tools_count: 1,
            resources_count: 2,
            resource_templates_count: 3,
            prompts_count: 4,
            status,
        }
    }

    fn mcp_details(server: McpListItem) -> McpServerDetailsResponse {
        McpServerDetailsResponse {
            snapshot_version: 1,
            generated_at: 1,
            server,
            catalog: McpServerCatalogDetails {
                catalog_version: None,
                generated_at: None,
                server_info: serde_json::json!({}),
                server_instructions_hash: None,
                tools: Vec::new(),
                resources: Vec::new(),
                resource_templates: Vec::new(),
                prompts: Vec::new(),
            },
            management: Some(McpManagementDetails {
                scope: McpScopeKind::Workspace,
                source_kind: McpSourceKind::Config,
                transport: McpTransportSummary::Stdio {
                    command: "server".to_owned(),
                },
                fingerprint: "fingerprint".to_owned(),
                health: McpServerHealthDetails {
                    runtime: mcp_runtime(McpRuntimeState::Ready, true),
                    status: McpServerStatus::Ready,
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
        }
    }

    #[test]
    fn mcp_changed_queues_list_and_details_only_for_matching_workspace() {
        let matching = reduce_mcp_changed_notification(
            McpChangedNotification {
                workspace_id: "ws_a".to_owned(),
                snapshot_version: 1,
                changed: Vec::new(),
            },
            Some("ws_a"),
            Some("server_a"),
        );
        assert!(matching.workspace_matches);
        assert!(matching.queue_mcp_refresh);
        assert!(matching.queue_mcp_details_refresh);

        let no_selected_server = reduce_mcp_changed_notification(
            McpChangedNotification {
                workspace_id: "ws_a".to_owned(),
                snapshot_version: 1,
                changed: Vec::new(),
            },
            Some("ws_a"),
            None,
        );
        assert!(no_selected_server.queue_mcp_refresh);
        assert!(!no_selected_server.queue_mcp_details_refresh);

        let foreign_workspace = reduce_mcp_changed_notification(
            McpChangedNotification {
                workspace_id: "ws_b".to_owned(),
                snapshot_version: 1,
                changed: Vec::new(),
            },
            Some("ws_a"),
            Some("server_a"),
        );
        assert!(!foreign_workspace.workspace_matches);
        assert!(!foreign_workspace.queue_mcp_refresh);
        assert!(!foreign_workspace.queue_mcp_details_refresh);
    }

    #[test]
    fn status_changed_patches_catalog_and_selected_details() {
        let mut servers = vec![mcp_server("server_a", McpServerStatus::Starting)];
        let notification = McpServerStatusChangedNotification {
            workspace_id: "ws_a".to_owned(),
            snapshot_version: 2,
            server: McpServerStatusItem {
                id: "server_a".to_owned(),
                name: "server_a".to_owned(),
                scope_kind: McpScopeKind::Workspace,
                runtime: mcp_runtime(McpRuntimeState::Failed, false),
                status: McpServerStatus::Failed,
            },
        };

        let reduction = reduce_mcp_server_status_changed_notification(
            notification.clone(),
            Some("ws_a"),
            Some("server_a"),
            true,
        );
        assert!(reduction.workspace_matches);
        assert!(reduction.selected_server_matches);
        assert!(reduction.update_selected_details);
        assert!(!reduction.queue_mcp_details_refresh);

        let missing_details = reduce_mcp_server_status_changed_notification(
            notification.clone(),
            Some("ws_a"),
            Some("server_a"),
            false,
        );
        assert!(!missing_details.update_selected_details);
        assert!(missing_details.queue_mcp_details_refresh);

        let mut details = mcp_details(servers[0].clone());
        apply_mcp_server_status_changed_to_catalog(&mut servers, &notification);
        apply_mcp_server_status_changed_to_details(&mut details, &notification);

        assert_eq!(servers[0].runtime.state, McpRuntimeState::Failed);
        assert_eq!(servers[0].status, McpServerStatus::Failed);
        assert_eq!(details.server.status, McpServerStatus::Failed);
        assert_eq!(
            details.management.expect("management").health.runtime.state,
            McpRuntimeState::Failed
        );
    }

    #[test]
    fn catalog_changed_patches_counts_and_refreshes_selected_details() {
        let mut servers = vec![mcp_server("server_a", McpServerStatus::Ready)];
        let notification = McpServerCatalogChangedNotification {
            workspace_id: "ws_a".to_owned(),
            snapshot_version: 3,
            server_id: "server_a".to_owned(),
            name: "server_a".to_owned(),
            catalog_version: "cat_2".to_owned(),
            tools_count: 10,
            resources_count: 11,
            resource_templates_count: 12,
            prompts_count: 13,
        };

        let matching = reduce_mcp_server_catalog_changed_notification(
            notification.clone(),
            Some("ws_a"),
            Some("server_a"),
        );
        assert!(matching.workspace_matches);
        assert!(matching.selected_server_matches);
        assert!(matching.queue_mcp_details_refresh);

        let foreign_selected = reduce_mcp_server_catalog_changed_notification(
            notification.clone(),
            Some("ws_a"),
            Some("server_b"),
        );
        assert!(foreign_selected.workspace_matches);
        assert!(!foreign_selected.selected_server_matches);
        assert!(!foreign_selected.queue_mcp_details_refresh);

        apply_mcp_server_catalog_changed_to_catalog(&mut servers, &notification);
        assert_eq!(servers[0].tools_count, 10);
        assert_eq!(servers[0].resources_count, 11);
        assert_eq!(servers[0].resource_templates_count, 12);
        assert_eq!(servers[0].prompts_count, 13);
    }
}
