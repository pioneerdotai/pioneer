//! Workspace action orchestration.

use crate::workspaces::selectors::{
    normalize_workspace_id, resolve_active_workspace_id, workspace_switch_is_noop,
    workspace_switch_target_is_known_active,
};
use pioneer_protocol::{
    Workspace, WorkspaceChangeKind, WorkspaceChangedNotification, WorkspaceCreateParams,
    WorkspaceSelectParams, WorkspaceUpdateParams, generate_id,
};

pub const WORKSPACE_ID_LEN: usize = 21;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkspaceBootstrapAfterList {
    SelectWorkspace { workspace_id: String },
    LoadDefaultWorkspace,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkspaceActionRejection {
    EmptyName,
    Busy,
    Unchanged,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkspaceCreatePlan {
    Skip(WorkspaceActionRejection),
    Request(WorkspaceCreateParams),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkspaceRenamePlan {
    Skip(WorkspaceActionRejection),
    Request(WorkspaceUpdateParams),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkspaceSwitchPlan {
    MissingWorkspaceId,
    Busy,
    Noop,
    UnknownTarget { workspace_id: String },
    Switch { workspace_id: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceMutationApplied {
    pub workspace_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceBootstrapOutcome {
    pub workspace_id: String,
    pub workspaces: Vec<Workspace>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct WorkspaceSelectionReduction {
    pub workspace_id: String,
    pub persist_active_gateway_workspace_id: String,
    pub set_preferred_workspace_id: String,
    pub refresh_thread_list: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct WorkspaceBootstrapSuccessReduction {
    pub workspaces: Vec<Workspace>,
    pub selected: WorkspaceSelectionReduction,
    pub clear_workspaces_error: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct WorkspaceSwitchSuccessReduction {
    pub workspaces: Vec<Workspace>,
    pub selected: WorkspaceSelectionReduction,
    pub clear_thread_list_loading: bool,
    pub refresh_workspace_bound_screens: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct WorkspaceCreateSuccessReduction {
    pub workspaces: Vec<Workspace>,
    pub workspace_id: String,
    pub switch_workspace_id: String,
    pub clear_workspaces_error: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct WorkspaceRenameSuccessReduction {
    pub workspaces: Vec<Workspace>,
    pub workspace_id: String,
    pub clear_workspaces_error: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayWorkspacePreferencePersistPlan {
    pub gateway_id: String,
    pub workspace_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspacePreferenceReduction {
    pub set_preferred_workspace_id: Option<Option<String>>,
    pub persist_active_gateway_workspace_id: Option<String>,
    pub queue_thread_list_refresh: bool,
}

pub fn generate_workspace_id() -> String {
    generate_id(WORKSPACE_ID_LEN)
}

pub fn plan_workspace_create(
    name: impl Into<String>,
    action_in_progress: bool,
) -> WorkspaceCreatePlan {
    plan_workspace_create_with_id(name, action_in_progress, generate_workspace_id())
}

pub fn plan_workspace_create_with_id(
    name: impl Into<String>,
    action_in_progress: bool,
    workspace_id: String,
) -> WorkspaceCreatePlan {
    let name = name.into().trim().to_owned();
    if name.is_empty() {
        return WorkspaceCreatePlan::Skip(WorkspaceActionRejection::EmptyName);
    }
    if action_in_progress {
        return WorkspaceCreatePlan::Skip(WorkspaceActionRejection::Busy);
    }

    WorkspaceCreatePlan::Request(WorkspaceCreateParams {
        workspace_id,
        name: Some(name),
        make_current: false,
    })
}

pub fn plan_workspace_rename(
    workspace_id: String,
    name: impl Into<String>,
    action_in_progress: bool,
    current_workspace: Option<&Workspace>,
) -> WorkspaceRenamePlan {
    let name = name.into().trim().to_owned();
    if name.is_empty() {
        return WorkspaceRenamePlan::Skip(WorkspaceActionRejection::EmptyName);
    }
    if action_in_progress {
        return WorkspaceRenamePlan::Skip(WorkspaceActionRejection::Busy);
    }
    if current_workspace.is_some_and(|workspace| workspace.name == name) {
        return WorkspaceRenamePlan::Skip(WorkspaceActionRejection::Unchanged);
    }

    WorkspaceRenamePlan::Request(WorkspaceUpdateParams {
        workspace_id,
        name: Some(name),
    })
}

pub fn plan_workspace_switch_from_ui(
    requested_workspace_id: impl Into<String>,
    action_in_progress: bool,
    current_workspace_id: Option<&str>,
    workspaces: &[Workspace],
) -> WorkspaceSwitchPlan {
    let Some(workspace_id) = normalize_workspace_id(Some(requested_workspace_id.into())) else {
        return WorkspaceSwitchPlan::MissingWorkspaceId;
    };

    if action_in_progress {
        return WorkspaceSwitchPlan::Busy;
    }

    if workspace_switch_is_noop(current_workspace_id, workspace_id.as_str()) {
        return WorkspaceSwitchPlan::Noop;
    }

    if !workspace_switch_target_is_known_active(workspaces, workspace_id.as_str()) {
        return WorkspaceSwitchPlan::UnknownTarget { workspace_id };
    }

    WorkspaceSwitchPlan::Switch { workspace_id }
}

pub fn workspace_action_result_matches_connection(
    expected_connection_id: u64,
    current_connection_id: Option<u64>,
) -> bool {
    current_connection_id == Some(expected_connection_id)
}

pub fn workspace_select_params(
    workspace_id: impl Into<String>,
    make_current: bool,
) -> WorkspaceSelectParams {
    WorkspaceSelectParams {
        workspace_id: workspace_id.into(),
        make_current,
    }
}

pub fn plan_active_gateway_workspace_persist(
    active_gateway_id: Option<&str>,
    workspace_id: impl Into<String>,
) -> Option<GatewayWorkspacePreferencePersistPlan> {
    active_gateway_id.map(|gateway_id| GatewayWorkspacePreferencePersistPlan {
        gateway_id: gateway_id.to_owned(),
        workspace_id: workspace_id.into(),
    })
}

pub fn plan_workspace_bootstrap_after_list(
    persisted_workspace_id: Option<&str>,
    workspaces: &[Workspace],
) -> WorkspaceBootstrapAfterList {
    match resolve_active_workspace_id(persisted_workspace_id, workspaces) {
        Some(workspace_id) => WorkspaceBootstrapAfterList::SelectWorkspace {
            workspace_id: workspace_id.to_owned(),
        },
        None => WorkspaceBootstrapAfterList::LoadDefaultWorkspace,
    }
}

pub fn apply_workspace_default_for_bootstrap(
    workspaces: &mut Vec<Workspace>,
    workspace: Workspace,
) -> Option<String> {
    let workspace_id = normalize_workspace_id(Some(workspace.id.clone()))?;
    upsert_workspace_catalog_item(workspaces, workspace);
    Some(workspace_id)
}

pub fn apply_workspace_select_response_to_catalog(
    workspaces: &mut Vec<Workspace>,
    workspace: Workspace,
) -> String {
    let workspace_id = workspace.id.clone();
    upsert_workspace_catalog_item(workspaces, workspace);
    workspace_id
}

pub fn reduce_workspace_bootstrap_success(
    outcome: WorkspaceBootstrapOutcome,
) -> WorkspaceBootstrapSuccessReduction {
    WorkspaceBootstrapSuccessReduction {
        selected: workspace_selection_reduction(outcome.workspace_id),
        workspaces: outcome.workspaces,
        clear_workspaces_error: true,
    }
}

pub fn reduce_workspace_switch_success(
    mut workspaces: Vec<Workspace>,
    workspace: Workspace,
) -> WorkspaceSwitchSuccessReduction {
    let workspace_id = apply_workspace_select_response_to_catalog(&mut workspaces, workspace);
    WorkspaceSwitchSuccessReduction {
        workspaces,
        selected: workspace_selection_reduction(workspace_id),
        clear_thread_list_loading: true,
        refresh_workspace_bound_screens: true,
    }
}

pub fn reduce_workspace_create_success(
    mut workspaces: Vec<Workspace>,
    workspace: Workspace,
) -> WorkspaceCreateSuccessReduction {
    let applied = apply_workspace_create_response_to_catalog(&mut workspaces, workspace);
    WorkspaceCreateSuccessReduction {
        switch_workspace_id: applied.workspace_id.clone(),
        workspace_id: applied.workspace_id,
        workspaces,
        clear_workspaces_error: true,
    }
}

pub fn reduce_workspace_rename_success(
    mut workspaces: Vec<Workspace>,
    workspace: Workspace,
) -> WorkspaceRenameSuccessReduction {
    let applied = apply_workspace_update_response_to_catalog(&mut workspaces, workspace);
    WorkspaceRenameSuccessReduction {
        workspace_id: applied.workspace_id,
        workspaces,
        clear_workspaces_error: true,
    }
}

pub fn apply_workspace_changed_to_catalog(
    workspaces: &mut Vec<Workspace>,
    notification: &WorkspaceChangedNotification,
) {
    let workspace = notification.workspace.clone();
    if matches!(notification.kind, WorkspaceChangeKind::CurrentChanged) && workspace.is_current {
        for existing in workspaces.iter_mut() {
            if existing.id != workspace.id {
                existing.is_current = false;
            }
        }
    }

    upsert_workspace_catalog_item(workspaces, workspace);
}

pub fn reduce_workspace_preference_after_catalog_change(
    preferred_workspace_id: Option<&str>,
    workspaces: &[Workspace],
) -> WorkspacePreferenceReduction {
    let Some(preferred_workspace_id) = preferred_workspace_id.map(str::trim) else {
        return WorkspacePreferenceReduction {
            set_preferred_workspace_id: None,
            persist_active_gateway_workspace_id: None,
            queue_thread_list_refresh: false,
        };
    };
    if preferred_workspace_id.is_empty() {
        return WorkspacePreferenceReduction {
            set_preferred_workspace_id: None,
            persist_active_gateway_workspace_id: None,
            queue_thread_list_refresh: false,
        };
    }

    let preferred_still_active = workspaces
        .iter()
        .any(|workspace| workspace.is_active && workspace.id == preferred_workspace_id);
    if preferred_still_active {
        return WorkspacePreferenceReduction {
            set_preferred_workspace_id: None,
            persist_active_gateway_workspace_id: None,
            queue_thread_list_refresh: false,
        };
    }

    let fallback_workspace_id = resolve_active_workspace_id(None, workspaces).map(str::to_owned);
    WorkspacePreferenceReduction {
        set_preferred_workspace_id: Some(fallback_workspace_id.clone()),
        persist_active_gateway_workspace_id: fallback_workspace_id.clone(),
        queue_thread_list_refresh: fallback_workspace_id.is_some(),
    }
}

pub fn apply_workspace_create_response_to_catalog(
    workspaces: &mut Vec<Workspace>,
    workspace: Workspace,
) -> WorkspaceMutationApplied {
    let workspace_id = workspace.id.clone();
    upsert_workspace_catalog_item(workspaces, workspace);
    WorkspaceMutationApplied { workspace_id }
}

pub fn apply_workspace_update_response_to_catalog(
    workspaces: &mut Vec<Workspace>,
    workspace: Workspace,
) -> WorkspaceMutationApplied {
    let workspace_id = workspace.id.clone();
    upsert_workspace_catalog_item(workspaces, workspace);
    WorkspaceMutationApplied { workspace_id }
}

pub fn upsert_workspace_catalog_item(workspaces: &mut Vec<Workspace>, workspace: Workspace) {
    if let Some(existing) = workspaces
        .iter_mut()
        .find(|candidate| candidate.id == workspace.id)
    {
        *existing = workspace;
    } else {
        workspaces.push(workspace);
    }
}

fn workspace_selection_reduction(workspace_id: String) -> WorkspaceSelectionReduction {
    WorkspaceSelectionReduction {
        persist_active_gateway_workspace_id: workspace_id.clone(),
        set_preferred_workspace_id: workspace_id.clone(),
        workspace_id,
        refresh_thread_list: true,
    }
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
    fn bootstrap_after_list_selects_existing_or_requests_default() {
        let workspaces = vec![
            workspace("ws_1", true, true),
            workspace("ws_2", true, false),
        ];

        assert_eq!(
            plan_workspace_bootstrap_after_list(Some("ws_2"), workspaces.as_slice()),
            WorkspaceBootstrapAfterList::SelectWorkspace {
                workspace_id: "ws_2".to_owned()
            }
        );
        assert_eq!(
            plan_workspace_bootstrap_after_list(None, &[]),
            WorkspaceBootstrapAfterList::LoadDefaultWorkspace
        );
    }

    #[test]
    fn create_plan_trims_name_and_builds_request() {
        let plan = plan_workspace_create_with_id("  New workspace  ", false, "ws_new".to_owned());
        assert_eq!(
            plan,
            WorkspaceCreatePlan::Request(WorkspaceCreateParams {
                workspace_id: "ws_new".to_owned(),
                name: Some("New workspace".to_owned()),
                make_current: false,
            })
        );

        assert_eq!(
            plan_workspace_create_with_id(" ", false, "ws_new".to_owned()),
            WorkspaceCreatePlan::Skip(WorkspaceActionRejection::EmptyName)
        );
        assert_eq!(
            plan_workspace_create_with_id("Name", true, "ws_new".to_owned()),
            WorkspaceCreatePlan::Skip(WorkspaceActionRejection::Busy)
        );
    }

    #[test]
    fn rename_plan_detects_empty_busy_and_unchanged_states() {
        let current = workspace("ws_a", true, true);
        assert_eq!(
            plan_workspace_rename("ws_a".to_owned(), "  Next  ", false, Some(&current)),
            WorkspaceRenamePlan::Request(WorkspaceUpdateParams {
                workspace_id: "ws_a".to_owned(),
                name: Some("Next".to_owned()),
            })
        );
        assert_eq!(
            plan_workspace_rename("ws_a".to_owned(), "", false, Some(&current)),
            WorkspaceRenamePlan::Skip(WorkspaceActionRejection::EmptyName)
        );
        assert_eq!(
            plan_workspace_rename("ws_a".to_owned(), "Next", true, Some(&current)),
            WorkspaceRenamePlan::Skip(WorkspaceActionRejection::Busy)
        );
        assert_eq!(
            plan_workspace_rename("ws_a".to_owned(), "ws_a workspace", false, Some(&current)),
            WorkspaceRenamePlan::Skip(WorkspaceActionRejection::Unchanged)
        );
    }

    #[test]
    fn workspace_action_result_connection_guard_matches_desktop_stale_check() {
        assert!(workspace_action_result_matches_connection(7, Some(7)));
        assert!(!workspace_action_result_matches_connection(7, Some(8)));
        assert!(!workspace_action_result_matches_connection(7, None));
    }

    #[test]
    fn switch_plan_preserves_desktop_preflight_order() {
        let workspaces = vec![
            workspace("ws_a", true, true),
            workspace("ws_b", true, false),
        ];
        assert_eq!(
            plan_workspace_switch_from_ui(" ", false, Some("ws_a"), workspaces.as_slice()),
            WorkspaceSwitchPlan::MissingWorkspaceId
        );
        assert_eq!(
            plan_workspace_switch_from_ui("ws_b", true, Some("ws_a"), workspaces.as_slice()),
            WorkspaceSwitchPlan::Busy
        );
        assert_eq!(
            plan_workspace_switch_from_ui(" ws_a ", false, Some("ws_a"), workspaces.as_slice()),
            WorkspaceSwitchPlan::Noop
        );
        assert_eq!(
            plan_workspace_switch_from_ui("ws_missing", false, Some("ws_a"), workspaces.as_slice()),
            WorkspaceSwitchPlan::UnknownTarget {
                workspace_id: "ws_missing".to_owned()
            }
        );
        assert_eq!(
            plan_workspace_switch_from_ui("ws_b", false, Some("ws_a"), workspaces.as_slice()),
            WorkspaceSwitchPlan::Switch {
                workspace_id: "ws_b".to_owned()
            }
        );
    }

    #[test]
    fn gateway_workspace_preference_persist_plan_requires_active_gateway() {
        assert_eq!(
            plan_active_gateway_workspace_persist(Some("gateway_a"), "ws_a"),
            Some(GatewayWorkspacePreferencePersistPlan {
                gateway_id: "gateway_a".to_owned(),
                workspace_id: "ws_a".to_owned(),
            })
        );
        assert_eq!(plan_active_gateway_workspace_persist(None, "ws_a"), None);
    }

    #[test]
    fn default_and_select_response_helpers_update_catalog() {
        let mut workspaces = Vec::new();
        let default_id = apply_workspace_default_for_bootstrap(
            &mut workspaces,
            workspace("ws_default", true, true),
        );
        assert_eq!(default_id.as_deref(), Some("ws_default"));
        assert_eq!(workspaces.len(), 1);

        let selected_id = apply_workspace_select_response_to_catalog(
            &mut workspaces,
            workspace("ws_default", true, false),
        );
        assert_eq!(selected_id, "ws_default");
        assert_eq!(workspaces.len(), 1);
        assert!(!workspaces[0].is_current);

        let create_applied = apply_workspace_create_response_to_catalog(
            &mut workspaces,
            workspace("ws_created", true, false),
        );
        assert_eq!(create_applied.workspace_id, "ws_created");
        assert_eq!(workspaces.len(), 2);

        let update_applied = apply_workspace_update_response_to_catalog(
            &mut workspaces,
            workspace("ws_created", true, true),
        );
        assert_eq!(update_applied.workspace_id, "ws_created");
        assert_eq!(workspaces.len(), 2);
    }

    #[test]
    fn workspace_bootstrap_success_reduction_selects_workspace_and_clears_error() {
        let workspaces = vec![workspace("ws_a", true, true)];

        let reduction = reduce_workspace_bootstrap_success(WorkspaceBootstrapOutcome {
            workspace_id: "ws_a".to_owned(),
            workspaces: workspaces.clone(),
        });

        assert_eq!(reduction.workspaces, workspaces);
        assert_eq!(
            reduction.selected,
            WorkspaceSelectionReduction {
                workspace_id: "ws_a".to_owned(),
                persist_active_gateway_workspace_id: "ws_a".to_owned(),
                set_preferred_workspace_id: "ws_a".to_owned(),
                refresh_thread_list: true,
            }
        );
        assert!(reduction.clear_workspaces_error);
    }

    #[test]
    fn workspace_switch_success_reduction_updates_catalog_and_refreshes_bound_screens() {
        let workspaces = vec![workspace("ws_a", true, true)];

        let reduction = reduce_workspace_switch_success(workspaces, workspace("ws_b", true, false));

        assert_eq!(
            reduction
                .workspaces
                .iter()
                .map(|workspace| workspace.id.as_str())
                .collect::<Vec<_>>(),
            vec!["ws_a", "ws_b"]
        );
        assert_eq!(reduction.selected.workspace_id, "ws_b");
        assert_eq!(
            reduction.selected.persist_active_gateway_workspace_id,
            "ws_b"
        );
        assert_eq!(reduction.selected.set_preferred_workspace_id, "ws_b");
        assert!(reduction.selected.refresh_thread_list);
        assert!(reduction.clear_thread_list_loading);
        assert!(reduction.refresh_workspace_bound_screens);
    }

    #[test]
    fn workspace_create_success_reduction_updates_catalog_and_requests_switch() {
        let workspaces = vec![workspace("ws_a", true, true)];

        let reduction =
            reduce_workspace_create_success(workspaces, workspace("ws_new", true, false));

        assert_eq!(reduction.workspace_id, "ws_new");
        assert_eq!(reduction.switch_workspace_id, "ws_new");
        assert!(reduction.clear_workspaces_error);
        assert_eq!(
            reduction
                .workspaces
                .iter()
                .map(|workspace| workspace.id.as_str())
                .collect::<Vec<_>>(),
            vec!["ws_a", "ws_new"]
        );
    }

    #[test]
    fn workspace_rename_success_reduction_updates_catalog_without_switch() {
        let workspaces = vec![workspace("ws_a", true, true)];

        let reduction = reduce_workspace_rename_success(workspaces, workspace("ws_a", true, true));

        assert_eq!(reduction.workspace_id, "ws_a");
        assert!(reduction.clear_workspaces_error);
        assert_eq!(reduction.workspaces.len(), 1);
        assert_eq!(reduction.workspaces[0].id, "ws_a");
    }

    #[test]
    fn workspace_changed_reconciles_current_and_preferred_fallback() {
        let mut catalog = vec![
            workspace("ws_a", true, true),
            workspace("ws_b", true, false),
        ];
        apply_workspace_changed_to_catalog(
            &mut catalog,
            &WorkspaceChangedNotification {
                kind: WorkspaceChangeKind::CurrentChanged,
                workspace: workspace("ws_b", true, true),
            },
        );
        assert!(
            !catalog
                .iter()
                .find(|workspace| workspace.id == "ws_a")
                .unwrap()
                .is_current
        );
        assert!(
            catalog
                .iter()
                .find(|workspace| workspace.id == "ws_b")
                .unwrap()
                .is_current
        );

        let reduction = reduce_workspace_preference_after_catalog_change(
            Some("ws_missing"),
            catalog.as_slice(),
        );
        assert_eq!(
            reduction.set_preferred_workspace_id,
            Some(Some("ws_b".to_owned()))
        );
        assert_eq!(
            reduction.persist_active_gateway_workspace_id.as_deref(),
            Some("ws_b")
        );
        assert!(reduction.queue_thread_list_refresh);
    }
}
