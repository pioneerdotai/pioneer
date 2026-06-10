//! Workspace bootstrap workflow shared by client shells.

use crate::{
    rpc::JsonRpcRequestTransport,
    transport::ws::command_sender as ws_commands,
    workspaces::actions::{
        WorkspaceBootstrapAfterList, WorkspaceBootstrapOutcome, WorkspaceBootstrapSuccessReduction,
        apply_workspace_default_for_bootstrap, apply_workspace_select_response_to_catalog,
        plan_workspace_bootstrap_after_list, reduce_workspace_bootstrap_success,
        workspace_select_params,
    },
};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceBootstrapRequest {
    #[serde(default)]
    pub persisted_workspace_id: Option<String>,
}

#[derive(Debug)]
pub enum WorkspaceBootstrapError {
    DefaultWorkspaceEmpty,
    Transport(anyhow::Error),
}

pub fn bootstrap_workspace_catalog<TTransport>(
    transport: &TTransport,
    request: WorkspaceBootstrapRequest,
) -> Result<WorkspaceBootstrapSuccessReduction, WorkspaceBootstrapError>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    let mut workspaces = ws_commands::workspace_list(transport)?.workspaces;
    let mut workspace_id = match plan_workspace_bootstrap_after_list(
        request.persisted_workspace_id.as_deref(),
        workspaces.as_slice(),
    ) {
        WorkspaceBootstrapAfterList::SelectWorkspace { workspace_id } => workspace_id,
        WorkspaceBootstrapAfterList::LoadDefaultWorkspace => {
            let workspace = ws_commands::workspace_default(transport)?.workspace;
            apply_workspace_default_for_bootstrap(&mut workspaces, workspace)
                .ok_or(WorkspaceBootstrapError::DefaultWorkspaceEmpty)?
        }
    };

    let response = ws_commands::workspace_select(
        transport,
        workspace_select_params(workspace_id.clone(), false),
    )?;
    workspace_id = apply_workspace_select_response_to_catalog(&mut workspaces, response.workspace);

    Ok(reduce_workspace_bootstrap_success(
        WorkspaceBootstrapOutcome {
            workspace_id,
            workspaces,
        },
    ))
}

impl fmt::Display for WorkspaceBootstrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DefaultWorkspaceEmpty => write!(f, "default workspace id is empty"),
            Self::Transport(error) => write!(f, "{error:#}"),
        }
    }
}

impl Error for WorkspaceBootstrapError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::DefaultWorkspaceEmpty => None,
            Self::Transport(error) => Some(error.root_cause()),
        }
    }
}

impl From<anyhow::Error> for WorkspaceBootstrapError {
    fn from(error: anyhow::Error) -> Self {
        Self::Transport(error)
    }
}
