use pioneer_client::{
    rpc::JsonRpcRequestTransport,
    transport::ws::command_sender as ws_commands,
    workspaces::actions::{
        WorkspaceCreatePlan, WorkspaceCreateSuccessReduction, WorkspaceRenamePlan,
        WorkspaceRenameSuccessReduction, WorkspaceSwitchPlan, WorkspaceSwitchSuccessReduction,
        plan_workspace_create, plan_workspace_rename, plan_workspace_switch_from_ui,
        reduce_workspace_create_success, reduce_workspace_rename_success,
        reduce_workspace_switch_success, workspace_select_params,
    },
};
use pioneer_protocol::Workspace;
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceSwitchRequest {
    pub workspace_id: String,
    #[serde(default)]
    pub current_workspace_id: Option<String>,
    #[serde(default)]
    pub workspaces: Vec<Workspace>,
    #[serde(default)]
    pub action_in_progress: bool,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum WorkspaceSwitchResult {
    Switched {
        reduction: WorkspaceSwitchSuccessReduction,
    },
    MissingWorkspaceId,
    Busy,
    Noop,
    UnknownTarget {
        workspace_id: String,
    },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceCreateRequest {
    pub name: String,
    #[serde(default)]
    pub workspaces: Vec<Workspace>,
    #[serde(default)]
    pub action_in_progress: bool,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum WorkspaceCreateResult {
    Created {
        reduction: WorkspaceCreateSuccessReduction,
    },
    EmptyName,
    Busy,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRenameRequest {
    pub workspace_id: String,
    pub name: String,
    #[serde(default)]
    pub workspaces: Vec<Workspace>,
    #[serde(default)]
    pub action_in_progress: bool,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum WorkspaceRenameResult {
    Renamed {
        reduction: WorkspaceRenameSuccessReduction,
    },
    EmptyName,
    Busy,
    Unchanged,
}

pub fn switch_workspace<TTransport>(
    transport: &TTransport,
    request: WorkspaceSwitchRequest,
) -> anyhow::Result<WorkspaceSwitchResult>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    let workspace_id = match plan_workspace_switch_from_ui(
        request.workspace_id,
        request.action_in_progress,
        request.current_workspace_id.as_deref(),
        request.workspaces.as_slice(),
    ) {
        WorkspaceSwitchPlan::Switch { workspace_id } => workspace_id,
        WorkspaceSwitchPlan::MissingWorkspaceId => {
            return Ok(WorkspaceSwitchResult::MissingWorkspaceId);
        }
        WorkspaceSwitchPlan::Busy => return Ok(WorkspaceSwitchResult::Busy),
        WorkspaceSwitchPlan::Noop => return Ok(WorkspaceSwitchResult::Noop),
        WorkspaceSwitchPlan::UnknownTarget { workspace_id } => {
            return Ok(WorkspaceSwitchResult::UnknownTarget { workspace_id });
        }
    };

    let response =
        ws_commands::workspace_select(transport, workspace_select_params(workspace_id, false))?;
    let reduction = reduce_workspace_switch_success(request.workspaces, response.workspace);
    Ok(WorkspaceSwitchResult::Switched { reduction })
}

pub fn create_workspace<TTransport>(
    transport: &TTransport,
    request: WorkspaceCreateRequest,
) -> anyhow::Result<WorkspaceCreateResult>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    let params = match plan_workspace_create(request.name, request.action_in_progress) {
        WorkspaceCreatePlan::Request(params) => params,
        WorkspaceCreatePlan::Skip(
            pioneer_client::workspaces::actions::WorkspaceActionRejection::EmptyName,
        ) => {
            return Ok(WorkspaceCreateResult::EmptyName);
        }
        WorkspaceCreatePlan::Skip(
            pioneer_client::workspaces::actions::WorkspaceActionRejection::Busy,
        ) => {
            return Ok(WorkspaceCreateResult::Busy);
        }
        WorkspaceCreatePlan::Skip(
            pioneer_client::workspaces::actions::WorkspaceActionRejection::Unchanged,
        ) => {
            unreachable!("workspace create cannot produce an unchanged rejection")
        }
    };

    let response = ws_commands::workspace_create(transport, params)?;
    let reduction = reduce_workspace_create_success(request.workspaces, response.workspace);
    Ok(WorkspaceCreateResult::Created { reduction })
}

pub fn rename_workspace<TTransport>(
    transport: &TTransport,
    request: WorkspaceRenameRequest,
) -> anyhow::Result<WorkspaceRenameResult>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    let current_workspace = request
        .workspaces
        .iter()
        .find(|workspace| workspace.id == request.workspace_id);
    let params = match plan_workspace_rename(
        request.workspace_id,
        request.name,
        request.action_in_progress,
        current_workspace,
    ) {
        WorkspaceRenamePlan::Request(params) => params,
        WorkspaceRenamePlan::Skip(
            pioneer_client::workspaces::actions::WorkspaceActionRejection::EmptyName,
        ) => {
            return Ok(WorkspaceRenameResult::EmptyName);
        }
        WorkspaceRenamePlan::Skip(
            pioneer_client::workspaces::actions::WorkspaceActionRejection::Busy,
        ) => {
            return Ok(WorkspaceRenameResult::Busy);
        }
        WorkspaceRenamePlan::Skip(
            pioneer_client::workspaces::actions::WorkspaceActionRejection::Unchanged,
        ) => {
            return Ok(WorkspaceRenameResult::Unchanged);
        }
    };

    let response = ws_commands::workspace_update(transport, params)?;
    let reduction = reduce_workspace_rename_success(request.workspaces, response.workspace);
    Ok(WorkspaceRenameResult::Renamed { reduction })
}
