//! FFI boundary schema export.
//!
//! These schemas describe bridge-owned request/result/event DTOs. Shared domain
//! schemas are exported by `pioneer-client`.

use schemars::{Schema, schema_for};
use std::{fs, path::Path};

pub struct SchemaDocument {
    pub file_name: &'static str,
    pub schema: Schema,
}

macro_rules! schema_doc {
    ($file_name:literal, $ty:ty) => {
        SchemaDocument {
            file_name: $file_name,
            schema: schema_for!($ty),
        }
    };
}

pub fn client_ffi_schema_documents() -> Vec<SchemaDocument> {
    let mut documents = vec![
        schema_doc!(
            "add_and_activate_remote_gateway_registry_plan.json",
            crate::gateway::AddAndActivateRemoteGatewayRegistryPlan
        ),
        schema_doc!(
            "add_remote_gateway_plan.json",
            crate::gateway::AddRemoteGatewayPlan
        ),
        schema_doc!(
            "client_error_event.json",
            crate::contracts::ClientErrorEvent
        ),
        schema_doc!("client_event.json", crate::contracts::ClientEvent),
        schema_doc!(
            "client_active_thread_clear_result.json",
            crate::active_thread::ClientActiveThreadClearResult
        ),
        schema_doc!(
            "client_active_thread_event_request.json",
            crate::active_thread::ClientActiveThreadEventRequest
        ),
        schema_doc!(
            "client_active_thread_open_request.json",
            crate::active_thread::ClientActiveThreadOpenRequest
        ),
        schema_doc!(
            "client_active_thread_send_text_request.json",
            crate::active_thread::ClientActiveThreadSendTextRequest
        ),
        schema_doc!(
            "client_active_thread_send_text_result.json",
            crate::active_thread::ClientActiveThreadSendTextResult
        ),
        schema_doc!(
            "client_active_thread_snapshot.json",
            crate::active_thread::ClientActiveThreadSnapshot
        ),
        schema_doc!(
            "client_active_thread_snapshot_request.json",
            crate::active_thread::ClientActiveThreadSnapshotRequest
        ),
        schema_doc!(
            "client_gateway_connect_request.json",
            crate::contracts::ClientGatewayConnectRequest
        ),
        schema_doc!(
            "client_gateway_connect_result.json",
            crate::contracts::ClientGatewayConnectResult
        ),
        schema_doc!(
            "client_gateway_ws_timings.json",
            crate::contracts::ClientGatewayWsTimings
        ),
        schema_doc!(
            "plan_activate_gateway_request.json",
            crate::gateway::PlanActivateGatewayRequest
        ),
        schema_doc!(
            "plan_add_remote_gateway_request.json",
            crate::gateway::PlanAddRemoteGatewayRequest
        ),
        schema_doc!(
            "plan_delete_remote_gateway_request.json",
            crate::gateway::PlanDeleteRemoteGatewayRequest
        ),
        schema_doc!(
            "plan_set_gateway_workspace_request.json",
            crate::gateway::PlanSetGatewayWorkspaceRequest
        ),
        schema_doc!(
            "plan_update_remote_gateway_request.json",
            crate::gateway::PlanUpdateRemoteGatewayRequest
        ),
        schema_doc!(
            "remote_gateway_validation_request.json",
            crate::gateway::RemoteGatewayValidationRequest
        ),
        schema_doc!(
            "thread_tree_level.json",
            crate::threads::ClientThreadTreeLevel
        ),
        schema_doc!(
            "thread_tree_level_request.json",
            crate::threads::ThreadTreeLevelRequest
        ),
        schema_doc!(
            "thread_tree_query_data.json",
            crate::threads::ClientThreadTreeQueryData
        ),
        schema_doc!(
            "thread_tree_refresh_request.json",
            crate::threads::ThreadTreeRefreshRequest
        ),
        schema_doc!(
            "thread_tree_snapshot.json",
            crate::threads::ClientThreadTreeSnapshot
        ),
        schema_doc!(
            "workspace_create_request.json",
            crate::workspaces::WorkspaceCreateRequest
        ),
        schema_doc!(
            "workspace_create_result.json",
            crate::workspaces::WorkspaceCreateResult
        ),
        schema_doc!(
            "workspace_rename_request.json",
            crate::workspaces::WorkspaceRenameRequest
        ),
        schema_doc!(
            "workspace_rename_result.json",
            crate::workspaces::WorkspaceRenameResult
        ),
        schema_doc!(
            "workspace_switch_request.json",
            crate::workspaces::WorkspaceSwitchRequest
        ),
        schema_doc!(
            "workspace_switch_result.json",
            crate::workspaces::WorkspaceSwitchResult
        ),
    ];

    documents.sort_by(|left, right| left.file_name.cmp(right.file_name));
    documents
}

pub fn write_client_ffi_schemas(
    output_directory: impl AsRef<Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let output_directory = output_directory.as_ref();
    fs::create_dir_all(output_directory)?;

    for document in client_ffi_schema_documents() {
        let schema_json = serde_json::to_string_pretty(&document.schema)?;
        let path = output_directory.join(document.file_name);
        fs::write(path, schema_json)?;
    }

    Ok(())
}
