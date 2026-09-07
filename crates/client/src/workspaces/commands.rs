use crate::{
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

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
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

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
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

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceCreateRequest {
    pub name: String,
    #[serde(default)]
    pub workspaces: Vec<Workspace>,
    #[serde(default)]
    pub action_in_progress: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum WorkspaceCreateResult {
    Created {
        reduction: WorkspaceCreateSuccessReduction,
    },
    EmptyName,
    Busy,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
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

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
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
            crate::workspaces::actions::WorkspaceActionRejection::EmptyName,
        ) => {
            return Ok(WorkspaceCreateResult::EmptyName);
        }
        WorkspaceCreatePlan::Skip(crate::workspaces::actions::WorkspaceActionRejection::Busy) => {
            return Ok(WorkspaceCreateResult::Busy);
        }
        WorkspaceCreatePlan::Skip(
            crate::workspaces::actions::WorkspaceActionRejection::Unchanged,
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
            crate::workspaces::actions::WorkspaceActionRejection::EmptyName,
        ) => {
            return Ok(WorkspaceRenameResult::EmptyName);
        }
        WorkspaceRenamePlan::Skip(crate::workspaces::actions::WorkspaceActionRejection::Busy) => {
            return Ok(WorkspaceRenameResult::Busy);
        }
        WorkspaceRenamePlan::Skip(
            crate::workspaces::actions::WorkspaceActionRejection::Unchanged,
        ) => {
            return Ok(WorkspaceRenameResult::Unchanged);
        }
    };

    let response = ws_commands::workspace_update(transport, params)?;
    let reduction = reduce_workspace_rename_success(request.workspaces, response.workspace);
    Ok(WorkspaceRenameResult::Renamed { reduction })
}

impl crate::core::ClientCore {
    fn begin_workspace_action(
        &self,
        operation: super::catalog::WorkspaceCatalogOperation,
    ) -> anyhow::Result<(u64, Option<u64>, Vec<Workspace>)> {
        let connection = self.gateway_http_generation();
        let mut owner = self
            .workspace_catalog
            .lock()
            .expect("workspace catalog poisoned");
        anyhow::ensure!(
            !self.is_stopped() && owner.request.is_none(),
            "Workspace catalog is unavailable or busy"
        );
        owner.generation += 1;
        let generation = owner.generation;
        owner.request_catalog = owner.publication.workspaces.clone();
        owner.request = Some((generation, connection));
        owner.publication.action_pending = true;
        owner.publication.operation = Some(operation);
        owner.publication.error = None;
        self.publish_workspace_catalog(&mut owner);
        Ok((generation, connection, owner.publication.workspaces.clone()))
    }
    fn complete_workspace_action(
        &self,
        generation: u64,
        connection: Option<u64>,
        workspaces: Option<Vec<Workspace>>,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        let mut owner = self
            .workspace_catalog
            .lock()
            .expect("workspace catalog poisoned");
        anyhow::ensure!(
            !self.is_stopped()
                && owner.request == Some((generation, connection))
                && self.gateway_http_generation() == connection,
            "Workspace action is stale"
        );
        owner.request = None;
        owner.publication.action_pending = false;
        owner.publication.error = error;
        if let Some(mut workspaces) = workspaces {
            super::catalog::merge_catalog_changes(
                &owner.request_catalog,
                &owner.publication.workspaces,
                &mut workspaces,
            );
            owner.publication.workspaces = workspaces;
        }
        owner.request_catalog.clear();
        self.publish_workspace_catalog(&mut owner);
        Ok(())
    }
    pub fn switch_workspace(&self, workspace_id: String) -> anyhow::Result<WorkspaceSwitchResult> {
        let navigation = self.navigation_snapshot();
        if navigation.workspace_id() == Some(workspace_id.as_str()) {
            return Ok(WorkspaceSwitchResult::Noop);
        }
        let (generation, connection, workspaces) =
            self.begin_workspace_action(super::catalog::WorkspaceCatalogOperation::Select)?;
        let target = workspace_id.clone();
        let optimistic = workspaces
            .iter()
            .any(|workspace| workspace.id == target && workspace.is_active);
        if optimistic {
            self.navigate(
                crate::navigation::NavigationIntent::SelectWorkspace {
                    workspace_id: Some(target.clone()),
                },
                None,
            );
        }
        let navigation_revision = self
            .snapshot(&crate::core::ClientScope::Navigation)
            .map(|p| p.revisions().scoped().get());
        let result = switch_workspace(
            &self.compatibility_runtime().ws_command_sender(),
            WorkspaceSwitchRequest {
                workspace_id,
                current_workspace_id: navigation.workspace_id().map(str::to_owned),
                workspaces,
                action_in_progress: false,
            },
        );
        let workspaces = match &result {
            Ok(WorkspaceSwitchResult::Switched { reduction }) => Some(reduction.workspaces.clone()),
            _ => None,
        };
        self.complete_workspace_action(
            generation,
            connection,
            workspaces,
            result.as_ref().err().map(|e| format!("{e:#}")),
        )?;
        if optimistic && !matches!(&result, Ok(WorkspaceSwitchResult::Switched { .. })) {
            self.navigate(
                crate::navigation::NavigationIntent::SelectWorkspace {
                    workspace_id: navigation.workspace_id().map(str::to_owned),
                },
                navigation_revision,
            );
        }
        if let Ok(WorkspaceSwitchResult::Switched { reduction }) = &result {
            self.navigate(
                crate::navigation::NavigationIntent::SelectWorkspace {
                    workspace_id: Some(reduction.selected.workspace_id.clone()),
                },
                navigation_revision,
            );
        }
        result
    }
    pub fn create_workspace(&self, name: String) -> anyhow::Result<WorkspaceCreateResult> {
        let (generation, connection, workspaces) =
            self.begin_workspace_action(super::catalog::WorkspaceCatalogOperation::Create)?;
        let result = create_workspace(
            &self.compatibility_runtime().ws_command_sender(),
            WorkspaceCreateRequest {
                name,
                workspaces,
                action_in_progress: false,
            },
        );
        let workspaces = match &result {
            Ok(WorkspaceCreateResult::Created { reduction }) => Some(reduction.workspaces.clone()),
            _ => None,
        };
        self.complete_workspace_action(
            generation,
            connection,
            workspaces,
            result.as_ref().err().map(|e| format!("{e:#}")),
        )?;
        result
    }
    pub fn rename_workspace(
        &self,
        workspace_id: String,
        name: String,
    ) -> anyhow::Result<WorkspaceRenameResult> {
        let (generation, connection, workspaces) =
            self.begin_workspace_action(super::catalog::WorkspaceCatalogOperation::Rename)?;
        let result = rename_workspace(
            &self.compatibility_runtime().ws_command_sender(),
            WorkspaceRenameRequest {
                workspace_id,
                name,
                workspaces,
                action_in_progress: false,
            },
        );
        let workspaces = match &result {
            Ok(WorkspaceRenameResult::Renamed { reduction }) => Some(reduction.workspaces.clone()),
            _ => None,
        };
        self.complete_workspace_action(
            generation,
            connection,
            workspaces,
            result.as_ref().err().map(|e| format!("{e:#}")),
        )?;
        result
    }
}
