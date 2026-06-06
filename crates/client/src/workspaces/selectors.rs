//! Workspace selectors.

use crate::gateway::registry;
use pioneer_protocol::Workspace;

pub fn normalize_workspace_id(value: Option<String>) -> Option<String> {
    value.and_then(registry::normalize_workspace_id)
}

pub fn workspace_by_id<'a>(
    workspaces: &'a [Workspace],
    workspace_id: &str,
) -> Option<&'a Workspace> {
    workspaces
        .iter()
        .find(|workspace| workspace.id == workspace_id)
}

pub fn active_workspaces(workspaces: &[Workspace]) -> Vec<&Workspace> {
    workspaces
        .iter()
        .filter(|workspace| workspace.is_active)
        .collect()
}

pub fn workspace_display_name(workspace: &Workspace) -> Option<&str> {
    let name = workspace.name.trim();
    (!name.is_empty()).then_some(name)
}

pub fn resolve_active_workspace_id<'a>(
    persisted_workspace_id: Option<&str>,
    workspaces: &'a [Workspace],
) -> Option<&'a str> {
    if let Some(workspace_id) = persisted_workspace_id.and_then(|workspace_id| {
        let trimmed = workspace_id.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    }) {
        if let Some(workspace) = workspaces
            .iter()
            .find(|workspace| workspace.is_active && workspace.id == workspace_id)
        {
            return Some(workspace.id.as_str());
        }
    }

    workspaces
        .iter()
        .find(|workspace| workspace.is_active && workspace.is_current)
        .or_else(|| workspaces.iter().find(|workspace| workspace.is_active))
        .map(|workspace| workspace.id.as_str())
}

pub fn workspace_switch_target_is_known_active(
    workspaces: &[Workspace],
    target_workspace_id: &str,
) -> bool {
    if workspaces.is_empty() {
        return true;
    }

    workspaces
        .iter()
        .any(|workspace| workspace.is_active && workspace.id == target_workspace_id)
}

pub fn resolve_workspace_scope(
    active_workspace_id: Option<&str>,
    preferred_workspace_id: Option<&str>,
    runtime_workspace_id: Option<&str>,
) -> Option<String> {
    normalize_workspace_id(
        active_workspace_id
            .or(preferred_workspace_id)
            .or(runtime_workspace_id)
            .map(str::to_owned),
    )
}

pub fn workspace_switch_is_noop(
    current_workspace_id: Option<&str>,
    target_workspace_id: &str,
) -> bool {
    normalize_workspace_id(current_workspace_id.map(str::to_owned)).as_deref()
        == normalize_workspace_id(Some(target_workspace_id.to_owned())).as_deref()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workspace(id: &str, is_active: bool, is_current: bool) -> Workspace {
        Workspace {
            id: id.to_owned(),
            name: format!("{id} workspace"),
            is_active,
            is_current,
            created_at: 1,
            updated_at: 2,
        }
    }

    #[test]
    fn active_workspace_prefers_valid_persisted_then_current_then_first_active() {
        let workspaces = vec![
            workspace("ws_1", true, true),
            workspace("ws_2", true, false),
        ];
        assert_eq!(
            resolve_active_workspace_id(Some("ws_2"), workspaces.as_slice()),
            Some("ws_2")
        );
        assert_eq!(
            resolve_active_workspace_id(Some("missing"), workspaces.as_slice()),
            Some("ws_1")
        );

        let no_current = vec![
            workspace("ws_inactive", false, true),
            workspace("ws_3", true, false),
        ];
        assert_eq!(
            resolve_active_workspace_id(None, no_current.as_slice()),
            Some("ws_3")
        );
    }

    #[test]
    fn active_workspaces_filters_inactive_catalog_entries() {
        let workspaces = vec![
            workspace("ws_1", true, true),
            workspace("ws_inactive", false, false),
            workspace("ws_2", true, false),
        ];

        let active = active_workspaces(workspaces.as_slice())
            .into_iter()
            .map(|workspace| workspace.id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(active, vec!["ws_1", "ws_2"]);
    }

    #[test]
    fn workspace_display_name_uses_trimmed_non_empty_name() {
        let mut item = workspace("ws_1", true, true);
        item.name = "  Project Alpha  ".to_owned();
        assert_eq!(workspace_display_name(&item), Some("Project Alpha"));

        item.name = "   ".to_owned();
        assert_eq!(workspace_display_name(&item), None);
    }

    #[test]
    fn workspace_target_known_active_allows_empty_catalog_during_bootstrap() {
        assert!(workspace_switch_target_is_known_active(&[], "ws_a"));

        let workspaces = vec![
            workspace("ws_a", true, false),
            workspace("ws_b", false, true),
        ];
        assert!(workspace_switch_target_is_known_active(
            workspaces.as_slice(),
            "ws_a"
        ));
        assert!(!workspace_switch_target_is_known_active(
            workspaces.as_slice(),
            "ws_b"
        ));
    }

    #[test]
    fn workspace_scope_and_switch_noop_use_normalized_ids() {
        assert_eq!(
            resolve_workspace_scope(None, Some("  ws_preferred  "), Some("ws_runtime")).as_deref(),
            Some("ws_preferred")
        );
        assert_eq!(
            resolve_workspace_scope(None, None, Some("  ws_runtime  ")).as_deref(),
            Some("ws_runtime")
        );
        assert!(workspace_switch_is_noop(Some("  ws_a  "), "ws_a"));
        assert!(!workspace_switch_is_noop(Some("ws_a"), "ws_b"));
        assert!(!workspace_switch_is_noop(None, "ws_a"));
    }
}
