//! MCP list state.

use pioneer_protocol::{McpListItem, McpListParams, McpServerDetailsResponse};
use std::collections::HashSet;

pub const MCP_INSTALL_PENDING_KEY: &str = "__install__";

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct McpListState {
    pub servers: Vec<McpListItem>,
    pub selected_server_id: Option<String>,
    pub server_details: Option<McpServerDetailsResponse>,
    pub loading: bool,
    pub details_loading: bool,
    pub error: Option<String>,
    pub refresh_requested: bool,
    pub details_refresh_requested: bool,
    pub poller_started: bool,
    pub pending_actions: HashSet<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct McpSnapshotApplication {
    pub selected_was_present: bool,
    pub selected_still_present: bool,
    pub selected_removed: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct McpListRefreshSuccessReduction {
    pub servers: Vec<McpListItem>,
    pub pending_actions: HashSet<String>,
    pub selected_server_id: Option<String>,
    pub server_details: Option<McpServerDetailsResponse>,
    pub application: McpSnapshotApplication,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct McpListRefreshFailureReduction {
    pub error: String,
}

pub fn normalize_mcp_server_name(name: &str) -> Option<String> {
    let name = name.trim();
    if name.is_empty() {
        None
    } else {
        Some(name.to_owned())
    }
}

pub fn mcp_list_params(workspace_id: impl Into<String>) -> McpListParams {
    McpListParams {
        workspace_id: workspace_id.into(),
    }
}

pub fn mcp_server_exists_by_id(servers: &[McpListItem], server_id: &str) -> bool {
    find_mcp_server_by_id(servers, server_id).is_some()
}

pub fn find_mcp_server_by_id<'a>(
    servers: &'a [McpListItem],
    server_id: &str,
) -> Option<&'a McpListItem> {
    servers.iter().find(|server| server.id == server_id)
}

pub fn find_mcp_server_by_name<'a>(
    servers: &'a [McpListItem],
    name: &str,
) -> Option<&'a McpListItem> {
    let Some(name) = normalize_mcp_server_name(name) else {
        return None;
    };

    servers.iter().find(|server| server.name == name)
}

pub fn resolve_mcp_server_id_from_timeline(
    servers: &[McpListItem],
    server_id_or_name: impl Into<String>,
) -> String {
    let server_id_or_name = server_id_or_name.into();
    servers
        .iter()
        .find(|server| server.id == server_id_or_name || server.name == server_id_or_name)
        .map(|server| server.id.clone())
        .unwrap_or(server_id_or_name)
}

pub fn is_mcp_pending(pending_actions: &HashSet<String>, name: &str) -> bool {
    pending_actions.contains(name)
}

pub fn set_mcp_pending(pending_actions: &mut HashSet<String>, name: &str, pending: bool) {
    if pending {
        pending_actions.insert(name.to_owned());
    } else {
        pending_actions.remove(name);
    }
}

pub fn prune_mcp_pending_actions(pending_actions: &mut HashSet<String>, servers: &[McpListItem]) {
    pending_actions.retain(|name| {
        name == MCP_INSTALL_PENDING_KEY || servers.iter().any(|server| server.name == *name)
    });
}

pub fn apply_mcp_snapshot(
    servers: &mut Vec<McpListItem>,
    pending_actions: &mut HashSet<String>,
    selected_server_id: &mut Option<String>,
    server_details: &mut Option<McpServerDetailsResponse>,
    next_servers: Vec<McpListItem>,
) -> McpSnapshotApplication {
    prune_mcp_pending_actions(pending_actions, next_servers.as_slice());
    *servers = next_servers;

    let Some(server_id) = selected_server_id.clone() else {
        return McpSnapshotApplication::default();
    };

    let selected_still_present = mcp_server_exists_by_id(servers, server_id.as_str());
    if selected_still_present {
        McpSnapshotApplication {
            selected_was_present: true,
            selected_still_present: true,
            selected_removed: false,
        }
    } else {
        *selected_server_id = None;
        *server_details = None;
        McpSnapshotApplication {
            selected_was_present: true,
            selected_still_present: false,
            selected_removed: true,
        }
    }
}

pub fn reduce_mcp_list_refresh_success(
    mut servers: Vec<McpListItem>,
    mut pending_actions: HashSet<String>,
    mut selected_server_id: Option<String>,
    mut server_details: Option<McpServerDetailsResponse>,
    next_servers: Vec<McpListItem>,
) -> McpListRefreshSuccessReduction {
    let application = apply_mcp_snapshot(
        &mut servers,
        &mut pending_actions,
        &mut selected_server_id,
        &mut server_details,
        next_servers,
    );

    McpListRefreshSuccessReduction {
        servers,
        pending_actions,
        selected_server_id,
        server_details,
        application,
    }
}

pub fn reduce_mcp_list_refresh_failure(error: impl Into<String>) -> McpListRefreshFailureReduction {
    McpListRefreshFailureReduction {
        error: error.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        McpPolicyState, McpRuntimeState, McpRuntimeStatus, McpScopeKind, McpServerStatus,
    };

    pub(crate) fn server(id: &str, name: &str) -> McpListItem {
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

    #[test]
    fn mcp_list_helpers_normalize_names_and_build_params() {
        assert_eq!(
            normalize_mcp_server_name(" github "),
            Some("github".to_owned())
        );
        assert_eq!(normalize_mcp_server_name(" "), None);

        let params = mcp_list_params("workspace");
        assert_eq!(params.workspace_id, "workspace");
    }

    #[test]
    fn mcp_server_lookup_and_timeline_resolution_use_id_or_name() {
        let servers = vec![server("id-github", "github")];

        assert!(mcp_server_exists_by_id(&servers, "id-github"));
        assert_eq!(
            find_mcp_server_by_name(&servers, " github ")
                .expect("server")
                .id,
            "id-github"
        );
        assert_eq!(
            resolve_mcp_server_id_from_timeline(&servers, "github"),
            "id-github"
        );
        assert_eq!(
            resolve_mcp_server_id_from_timeline(&servers, "unknown"),
            "unknown"
        );
    }

    #[test]
    fn pending_actions_keep_install_and_existing_servers_only() {
        let mut pending = HashSet::from([
            MCP_INSTALL_PENDING_KEY.to_owned(),
            "github".to_owned(),
            "missing".to_owned(),
        ]);

        prune_mcp_pending_actions(&mut pending, &[server("id-github", "github")]);

        assert!(is_mcp_pending(&pending, MCP_INSTALL_PENDING_KEY));
        assert!(is_mcp_pending(&pending, "github"));
        assert!(!is_mcp_pending(&pending, "missing"));

        set_mcp_pending(&mut pending, "github", false);
        assert!(!is_mcp_pending(&pending, "github"));
    }

    #[test]
    fn snapshot_application_prunes_pending_and_clears_missing_selection() {
        let mut servers = vec![server("id-old", "old")];
        let mut pending = HashSet::from(["old".to_owned()]);
        let mut selected = Some("id-old".to_owned());
        let mut details = None;

        let result = apply_mcp_snapshot(
            &mut servers,
            &mut pending,
            &mut selected,
            &mut details,
            vec![server("id-new", "new")],
        );

        assert!(result.selected_was_present);
        assert!(result.selected_removed);
        assert_eq!(selected, None);
        assert!(!is_mcp_pending(&pending, "old"));
        assert_eq!(servers[0].id, "id-new");
    }

    #[test]
    fn snapshot_application_keeps_existing_selection() {
        let mut servers = Vec::new();
        let mut pending = HashSet::new();
        let mut selected = Some("id-github".to_owned());
        let mut details = None;

        let result = apply_mcp_snapshot(
            &mut servers,
            &mut pending,
            &mut selected,
            &mut details,
            vec![server("id-github", "github")],
        );

        assert!(result.selected_still_present);
        assert_eq!(selected.as_deref(), Some("id-github"));
    }

    #[test]
    fn list_refresh_success_reduction_applies_snapshot_and_selection_state() {
        let reduction = reduce_mcp_list_refresh_success(
            vec![server("id-old", "old")],
            HashSet::from(["old".to_owned()]),
            Some("id-old".to_owned()),
            None,
            vec![server("id-new", "new")],
        );

        assert_eq!(reduction.servers[0].id, "id-new");
        assert_eq!(reduction.selected_server_id, None);
        assert!(reduction.server_details.is_none());
        assert!(reduction.application.selected_removed);
        assert!(!reduction.pending_actions.contains("old"));
    }

    #[test]
    fn list_refresh_failure_reduction_carries_display_error() {
        let reduction = reduce_mcp_list_refresh_failure("load failed");

        assert_eq!(reduction.error, "load failed");
    }
}
