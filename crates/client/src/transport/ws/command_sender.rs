//! WebSocket typed command helpers.

use crate::rpc::validation::{
    require_condition, require_non_empty_field, require_optional_non_empty_field,
    validate_task_review_target,
};
use crate::rpc::{
    JsonRpcRequestTransport, RPC_REQUEST_TIMEOUT, RPC_UNSUBSCRIBE_TIMEOUT,
    send_json_rpc_request_typed,
};
use anyhow::Result;
use pioneer_protocol::constants::methods;
use pioneer_protocol::{
    ArtifactBindParams, ArtifactBindResponse, ArtifactCapabilitiesParams,
    ArtifactCapabilitiesResponse, ArtifactDeleteParams, ArtifactDeleteResponse,
    ArtifactDownloadAbortParams, ArtifactDownloadAbortResponse, ArtifactDownloadChunkParams,
    ArtifactDownloadChunkResponse, ArtifactDownloadFinishParams, ArtifactDownloadFinishResponse,
    ArtifactDownloadStartParams, ArtifactDownloadStartResponse, ArtifactGetParams,
    ArtifactGetResponse, ArtifactListForMessageParams, ArtifactListForThreadParams,
    ArtifactListForTurnParams, ArtifactListParams, ArtifactListResponse, ArtifactReadParams,
    ArtifactReadResponse, ArtifactRestoreParams, ArtifactRestoreResponse,
    ArtifactUploadAbortParams, ArtifactUploadAbortResponse, ArtifactUploadFinishParams,
    ArtifactUploadFinishResponse, ArtifactUploadStartParams, ArtifactUploadStartResponse,
    CLIRuntimeListModelsParams, CLIRuntimeListModelsResponse, CLIRuntimeListParams,
    CLIRuntimeListResponse, CLIRuntimeLoginCancelParams, CLIRuntimeLoginCancelResponse,
    CLIRuntimeLoginStartParams, CLIRuntimeLoginStartResponse, CLIRuntimeRefreshParams,
    CLIRuntimeRefreshResponse, CLIRuntimeRequestRespondParams, CLIRuntimeRequestRespondResponse,
    CLIRuntimeReviewStartParams, CLIRuntimeReviewStartResponse, CLIRuntimeStatusParams,
    CLIRuntimeStatusResponse, CLIRuntimeThreadBindingGetParams, CLIRuntimeThreadBindingGetResponse,
    CLIRuntimeThreadCompactParams, CLIRuntimeThreadCompactResponse, CLIRuntimeThreadForkParams,
    CLIRuntimeThreadForkResponse, CLIRuntimeTurnSteerParams, CLIRuntimeTurnSteerResponse,
    GatewaySettingsGetParams, GatewaySettingsGetResponse, GatewaySettingsUpdate,
    GatewaySettingsUpdateParams, GatewaySettingsUpdateResponse, McpInstallParams,
    McpInstallResponse, McpListParams, McpListResponse, McpPolicySetParams, McpPolicySetResponse,
    McpServerDetailsParams, McpServerDetailsResponse, McpServerRestartParams,
    McpServerRestartResponse, McpUninstallParams, McpUninstallResponse, ProviderDeleteApiKeyParams,
    ProviderDeleteApiKeyResponse, ProviderListModelsParams, ProviderListModelsResponse,
    ProviderListParams, ProviderListResponse, ProviderSetApiKeyParams, ProviderSetApiKeyResponse,
    SkillListParams, SkillListResponse, SkillsHealthParams, SkillsHealthResponse,
    SkillsInstallParams, SkillsInstallResponse, SkillsPolicyListParams, SkillsPolicyListResponse,
    SkillsPolicySetParams, SkillsPolicySetResponse, SkillsUninstallParams, SkillsUninstallResponse,
    SkillsUpdateParams, SkillsUpdateResponse, SkillsUploadAbortParams, SkillsUploadAbortResponse,
    SkillsUploadFinishParams, SkillsUploadFinishResponse, SkillsUploadStartParams,
    SkillsUploadStartResponse, TaskAcceptParams, TaskAcceptResponse, TaskCancelParams,
    TaskCancelResponse, TaskReviseParams, TaskReviseResponse, ThreadAgentsDocArchiveParams,
    ThreadAgentsDocArchiveResponse, ThreadAgentsDocGetParams, ThreadAgentsDocGetResponse,
    ThreadAgentsDocResolveForThreadParams, ThreadAgentsDocResolveForThreadResponse,
    ThreadAgentsDocSaveParams, ThreadAgentsDocSaveResponse, ThreadFolderCreateParams,
    ThreadFolderCreateResponse, ThreadFolderDeleteParams, ThreadFolderDeleteResponse,
    ThreadFolderMoveParams, ThreadFolderMoveResponse, ThreadGetParams, ThreadGetResponse,
    ThreadHistoryParams, ThreadHistoryResponse, ThreadMoveParams, ThreadMoveResponse,
    ThreadStartParams, ThreadStartResponse, ThreadTreeParams, ThreadTreeResponse,
    ThreadUnsubscribeParams, ThreadUnsubscribeResponse, ThreadUpdateParams, ThreadUpdateResponse,
    TurnCancelParams, TurnCancelResponse, TurnGetParams, TurnGetResponse, TurnItemsParams,
    TurnItemsResponse, TurnStartParams, TurnStartResponse, TurnTimelineParams,
    TurnTimelineResponse, WorkspaceCreateParams, WorkspaceCreateResponse, WorkspaceDefaultParams,
    WorkspaceDefaultResponse, WorkspaceListParams, WorkspaceListResponse, WorkspaceSelectParams,
    WorkspaceSelectResponse, WorkspaceUpdateParams, WorkspaceUpdateResponse,
};
use pioneer_skills::is_qualified_skill_slug;
use std::time::Duration;

pub const PROVIDER_MODELS_TIMEOUT: Duration = Duration::from_secs(30);

pub fn thread_start<TTransport>(
    transport: &TTransport,
    params: ThreadStartParams,
) -> Result<ThreadStartResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::THREAD_START,
    )?;
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::THREAD_START,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::THREAD_START,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn thread_tree<TTransport>(
    transport: &TTransport,
    params: ThreadTreeParams,
) -> Result<ThreadTreeResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::THREAD_TREE,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::THREAD_TREE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn thread_get<TTransport>(
    transport: &TTransport,
    params: ThreadGetParams,
) -> Result<ThreadGetResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(params.thread_id.as_str(), "thread_id", methods::THREAD_GET)?;

    send_json_rpc_request_typed(transport, methods::THREAD_GET, &params, RPC_REQUEST_TIMEOUT)
}

pub fn thread_update<TTransport>(
    transport: &TTransport,
    params: ThreadUpdateParams,
) -> Result<ThreadUpdateResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::THREAD_UPDATE,
    )?;
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::THREAD_UPDATE,
    )?;
    require_condition(
        params.name.is_some(),
        "at least one field is required for thread/update",
    )?;
    require_optional_non_empty_field(params.name.as_deref(), "name", methods::THREAD_UPDATE)?;

    send_json_rpc_request_typed(
        transport,
        methods::THREAD_UPDATE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn thread_move<TTransport>(
    transport: &TTransport,
    params: ThreadMoveParams,
) -> Result<ThreadMoveResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::THREAD_MOVE,
    )?;
    require_non_empty_field(params.thread_id.as_str(), "thread_id", methods::THREAD_MOVE)?;

    send_json_rpc_request_typed(
        transport,
        methods::THREAD_MOVE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn thread_history<TTransport>(
    transport: &TTransport,
    params: ThreadHistoryParams,
) -> Result<ThreadHistoryResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::THREAD_HISTORY,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::THREAD_HISTORY,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn thread_folder_create<TTransport>(
    transport: &TTransport,
    params: ThreadFolderCreateParams,
) -> Result<ThreadFolderCreateResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::THREAD_FOLDER_CREATE,
    )?;
    require_non_empty_field(params.name.as_str(), "name", methods::THREAD_FOLDER_CREATE)?;

    send_json_rpc_request_typed(
        transport,
        methods::THREAD_FOLDER_CREATE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn thread_folder_move<TTransport>(
    transport: &TTransport,
    params: ThreadFolderMoveParams,
) -> Result<ThreadFolderMoveResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::THREAD_FOLDER_MOVE,
    )?;
    require_non_empty_field(
        params.folder_id.as_str(),
        "folder_id",
        methods::THREAD_FOLDER_MOVE,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::THREAD_FOLDER_MOVE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn thread_folder_delete<TTransport>(
    transport: &TTransport,
    params: ThreadFolderDeleteParams,
) -> Result<ThreadFolderDeleteResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::THREAD_FOLDER_DELETE,
    )?;
    require_non_empty_field(
        params.folder_id.as_str(),
        "folder_id",
        methods::THREAD_FOLDER_DELETE,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::THREAD_FOLDER_DELETE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn thread_agents_doc_get<TTransport>(
    transport: &TTransport,
    params: ThreadAgentsDocGetParams,
) -> Result<ThreadAgentsDocGetResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::THREAD_AGENTS_DOC_GET,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::THREAD_AGENTS_DOC_GET,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn thread_agents_doc_save<TTransport>(
    transport: &TTransport,
    params: ThreadAgentsDocSaveParams,
) -> Result<ThreadAgentsDocSaveResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::THREAD_AGENTS_DOC_SAVE,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::THREAD_AGENTS_DOC_SAVE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn thread_agents_doc_archive<TTransport>(
    transport: &TTransport,
    params: ThreadAgentsDocArchiveParams,
) -> Result<ThreadAgentsDocArchiveResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::THREAD_AGENTS_DOC_ARCHIVE,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::THREAD_AGENTS_DOC_ARCHIVE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn thread_agents_doc_resolve_for_thread<TTransport>(
    transport: &TTransport,
    params: ThreadAgentsDocResolveForThreadParams,
) -> Result<ThreadAgentsDocResolveForThreadResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::THREAD_AGENTS_DOC_RESOLVE_FOR_THREAD,
    )?;
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::THREAD_AGENTS_DOC_RESOLVE_FOR_THREAD,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::THREAD_AGENTS_DOC_RESOLVE_FOR_THREAD,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn thread_unsubscribe<TTransport>(
    transport: &TTransport,
    thread_id: String,
) -> Result<ThreadUnsubscribeResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(
        transport,
        methods::THREAD_UNSUBSCRIBE,
        &ThreadUnsubscribeParams { thread_id },
        RPC_UNSUBSCRIBE_TIMEOUT,
    )
}

pub fn workspace_default<TTransport>(transport: &TTransport) -> Result<WorkspaceDefaultResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(
        transport,
        methods::WORKSPACE_DEFAULT,
        &WorkspaceDefaultParams::default(),
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn workspace_list<TTransport>(transport: &TTransport) -> Result<WorkspaceListResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(
        transport,
        methods::WORKSPACE_LIST,
        &WorkspaceListParams::default(),
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn workspace_create<TTransport>(
    transport: &TTransport,
    params: WorkspaceCreateParams,
) -> Result<WorkspaceCreateResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::WORKSPACE_CREATE,
    )?;
    require_optional_non_empty_field(params.name.as_deref(), "name", methods::WORKSPACE_CREATE)?;

    send_json_rpc_request_typed(
        transport,
        methods::WORKSPACE_CREATE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn workspace_select<TTransport>(
    transport: &TTransport,
    params: WorkspaceSelectParams,
) -> Result<WorkspaceSelectResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::WORKSPACE_SELECT,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::WORKSPACE_SELECT,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn workspace_update<TTransport>(
    transport: &TTransport,
    params: WorkspaceUpdateParams,
) -> Result<WorkspaceUpdateResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::WORKSPACE_UPDATE,
    )?;
    require_condition(
        params.name.is_some(),
        "at least one field is required for workspace/update",
    )?;
    require_optional_non_empty_field(params.name.as_deref(), "name", methods::WORKSPACE_UPDATE)?;

    send_json_rpc_request_typed(
        transport,
        methods::WORKSPACE_UPDATE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn turn_start<TTransport>(
    transport: &TTransport,
    params: TurnStartParams,
) -> Result<TurnStartResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(params.thread_id.as_str(), "thread_id", methods::TURN_START)?;
    require_non_empty_field(params.turn_id.as_str(), "turn_id", methods::TURN_START)?;

    send_json_rpc_request_typed(transport, methods::TURN_START, &params, RPC_REQUEST_TIMEOUT)
}

pub fn turn_cancel<TTransport>(
    transport: &TTransport,
    params: TurnCancelParams,
) -> Result<TurnCancelResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(params.thread_id.as_str(), "thread_id", methods::TURN_CANCEL)?;
    require_non_empty_field(params.turn_id.as_str(), "turn_id", methods::TURN_CANCEL)?;

    send_json_rpc_request_typed(
        transport,
        methods::TURN_CANCEL,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn turn_get<TTransport>(
    transport: &TTransport,
    params: TurnGetParams,
) -> Result<TurnGetResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(transport, methods::TURN_GET, &params, RPC_REQUEST_TIMEOUT)
}

pub fn turn_items<TTransport>(
    transport: &TTransport,
    params: TurnItemsParams,
) -> Result<TurnItemsResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(transport, methods::TURN_ITEMS, &params, RPC_REQUEST_TIMEOUT)
}

pub fn turn_timeline<TTransport>(
    transport: &TTransport,
    params: TurnTimelineParams,
) -> Result<TurnTimelineResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::TURN_TIMELINE,
    )?;
    require_non_empty_field(params.turn_id.as_str(), "turn_id", methods::TURN_TIMELINE)?;

    send_json_rpc_request_typed(
        transport,
        methods::TURN_TIMELINE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn provider_list<TTransport>(
    transport: &TTransport,
    params: ProviderListParams,
) -> Result<ProviderListResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::PROVIDER_LIST,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::PROVIDER_LIST,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn cli_runtime_list<TTransport>(
    transport: &TTransport,
    params: CLIRuntimeListParams,
) -> Result<CLIRuntimeListResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::CLI_RUNTIME_LIST,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::CLI_RUNTIME_LIST,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn cli_runtime_list_models<TTransport>(
    transport: &TTransport,
    params: CLIRuntimeListModelsParams,
) -> Result<CLIRuntimeListModelsResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::CLI_RUNTIME_LIST_MODELS,
    )?;
    require_non_empty_field(
        params.runtime_id.as_str(),
        "runtime_id",
        methods::CLI_RUNTIME_LIST_MODELS,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::CLI_RUNTIME_LIST_MODELS,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn cli_runtime_status<TTransport>(
    transport: &TTransport,
    params: CLIRuntimeStatusParams,
) -> Result<CLIRuntimeStatusResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::CLI_RUNTIME_STATUS,
    )?;
    require_non_empty_field(
        params.runtime_id.as_str(),
        "runtime_id",
        methods::CLI_RUNTIME_STATUS,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::CLI_RUNTIME_STATUS,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn cli_runtime_thread_binding_get<TTransport>(
    transport: &TTransport,
    params: CLIRuntimeThreadBindingGetParams,
) -> Result<CLIRuntimeThreadBindingGetResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::CLI_RUNTIME_THREAD_BINDING_GET,
    )?;
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::CLI_RUNTIME_THREAD_BINDING_GET,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::CLI_RUNTIME_THREAD_BINDING_GET,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn cli_runtime_thread_compact<TTransport>(
    transport: &TTransport,
    params: CLIRuntimeThreadCompactParams,
) -> Result<CLIRuntimeThreadCompactResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::CLI_RUNTIME_THREAD_COMPACT,
    )?;
    require_non_empty_field(
        params.runtime_id.as_str(),
        "runtime_id",
        methods::CLI_RUNTIME_THREAD_COMPACT,
    )?;
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::CLI_RUNTIME_THREAD_COMPACT,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::CLI_RUNTIME_THREAD_COMPACT,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn cli_runtime_thread_fork<TTransport>(
    transport: &TTransport,
    params: CLIRuntimeThreadForkParams,
) -> Result<CLIRuntimeThreadForkResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::CLI_RUNTIME_THREAD_FORK,
    )?;
    require_non_empty_field(
        params.runtime_id.as_str(),
        "runtime_id",
        methods::CLI_RUNTIME_THREAD_FORK,
    )?;
    require_non_empty_field(
        params.source_thread_id.as_str(),
        "source_thread_id",
        methods::CLI_RUNTIME_THREAD_FORK,
    )?;
    require_non_empty_field(
        params.fork_thread_id.as_str(),
        "fork_thread_id",
        methods::CLI_RUNTIME_THREAD_FORK,
    )?;
    require_optional_non_empty_field(
        params.name.as_deref(),
        "name",
        methods::CLI_RUNTIME_THREAD_FORK,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::CLI_RUNTIME_THREAD_FORK,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn cli_runtime_turn_steer<TTransport>(
    transport: &TTransport,
    params: CLIRuntimeTurnSteerParams,
) -> Result<CLIRuntimeTurnSteerResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::CLI_RUNTIME_TURN_STEER,
    )?;
    require_non_empty_field(
        params.runtime_id.as_str(),
        "runtime_id",
        methods::CLI_RUNTIME_TURN_STEER,
    )?;
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::CLI_RUNTIME_TURN_STEER,
    )?;
    require_non_empty_field(
        params.turn_id.as_str(),
        "turn_id",
        methods::CLI_RUNTIME_TURN_STEER,
    )?;
    require_non_empty_field(
        params.message.as_str(),
        "message",
        methods::CLI_RUNTIME_TURN_STEER,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::CLI_RUNTIME_TURN_STEER,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn cli_runtime_review_start<TTransport>(
    transport: &TTransport,
    params: CLIRuntimeReviewStartParams,
) -> Result<CLIRuntimeReviewStartResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::CLI_RUNTIME_REVIEW_START,
    )?;
    require_non_empty_field(
        params.runtime_id.as_str(),
        "runtime_id",
        methods::CLI_RUNTIME_REVIEW_START,
    )?;
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::CLI_RUNTIME_REVIEW_START,
    )?;
    require_optional_non_empty_field(
        params.turn_id.as_deref(),
        "turn_id",
        methods::CLI_RUNTIME_REVIEW_START,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::CLI_RUNTIME_REVIEW_START,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn cli_runtime_refresh<TTransport>(
    transport: &TTransport,
    params: CLIRuntimeRefreshParams,
) -> Result<CLIRuntimeRefreshResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::CLI_RUNTIME_REFRESH,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::CLI_RUNTIME_REFRESH,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn cli_runtime_login_start<TTransport>(
    transport: &TTransport,
    params: CLIRuntimeLoginStartParams,
) -> Result<CLIRuntimeLoginStartResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::CLI_RUNTIME_LOGIN_START,
    )?;
    require_non_empty_field(
        params.runtime_id.as_str(),
        "runtime_id",
        methods::CLI_RUNTIME_LOGIN_START,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::CLI_RUNTIME_LOGIN_START,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn cli_runtime_login_cancel<TTransport>(
    transport: &TTransport,
    params: CLIRuntimeLoginCancelParams,
) -> Result<CLIRuntimeLoginCancelResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::CLI_RUNTIME_LOGIN_CANCEL,
    )?;
    require_non_empty_field(
        params.runtime_id.as_str(),
        "runtime_id",
        methods::CLI_RUNTIME_LOGIN_CANCEL,
    )?;
    require_non_empty_field(
        params.login_id.as_str(),
        "login_id",
        methods::CLI_RUNTIME_LOGIN_CANCEL,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::CLI_RUNTIME_LOGIN_CANCEL,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn cli_runtime_request_respond<TTransport>(
    transport: &TTransport,
    params: CLIRuntimeRequestRespondParams,
) -> Result<CLIRuntimeRequestRespondResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::CLI_RUNTIME_REQUEST_RESPOND,
    )?;
    require_non_empty_field(
        params.runtime_id.as_str(),
        "runtime_id",
        methods::CLI_RUNTIME_REQUEST_RESPOND,
    )?;
    require_non_empty_field(
        params.request_id.as_str(),
        "request_id",
        methods::CLI_RUNTIME_REQUEST_RESPOND,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::CLI_RUNTIME_REQUEST_RESPOND,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn gateway_settings_get<TTransport>(
    transport: &TTransport,
) -> Result<GatewaySettingsGetResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(
        transport,
        methods::SETTINGS_GET,
        &GatewaySettingsGetParams::default(),
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn gateway_settings_update<TTransport>(
    transport: &TTransport,
    update: GatewaySettingsUpdate,
) -> Result<GatewaySettingsUpdateResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(
        transport,
        methods::SETTINGS_UPDATE,
        &GatewaySettingsUpdateParams { update },
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn provider_list_models<TTransport>(
    transport: &TTransport,
    params: ProviderListModelsParams,
) -> Result<ProviderListModelsResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::PROVIDER_MODELS_LIST,
    )?;
    require_non_empty_field(
        params.provider.as_str(),
        "provider",
        methods::PROVIDER_MODELS_LIST,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::PROVIDER_MODELS_LIST,
        &params,
        PROVIDER_MODELS_TIMEOUT,
    )
}

pub fn provider_set_api_key<TTransport>(
    transport: &TTransport,
    params: ProviderSetApiKeyParams,
) -> Result<ProviderSetApiKeyResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::PROVIDER_SET_API_KEY,
    )?;
    require_non_empty_field(
        params.provider.as_str(),
        "provider",
        methods::PROVIDER_SET_API_KEY,
    )?;
    require_non_empty_field(
        params.api_key.as_str(),
        "api_key",
        methods::PROVIDER_SET_API_KEY,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::PROVIDER_SET_API_KEY,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn provider_delete_api_key<TTransport>(
    transport: &TTransport,
    params: ProviderDeleteApiKeyParams,
) -> Result<ProviderDeleteApiKeyResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::PROVIDER_DELETE_API_KEY,
    )?;
    require_non_empty_field(
        params.provider.as_str(),
        "provider",
        methods::PROVIDER_DELETE_API_KEY,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::PROVIDER_DELETE_API_KEY,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn skills_list<TTransport>(
    transport: &TTransport,
    params: SkillListParams,
) -> Result<SkillListResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::SKILLS_LIST,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::SKILLS_LIST,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn skills_install<TTransport>(
    transport: &TTransport,
    params: SkillsInstallParams,
) -> Result<SkillsInstallResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::SKILLS_INSTALL,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::SKILLS_INSTALL,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn skills_upload_start<TTransport>(
    transport: &TTransport,
    params: SkillsUploadStartParams,
) -> Result<SkillsUploadStartResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::SKILLS_UPLOAD_START,
    )?;
    require_non_empty_field(
        params.file_name.as_str(),
        "file_name",
        methods::SKILLS_UPLOAD_START,
    )?;
    require_condition(
        params.compressed_size_bytes > 0,
        "compressed_size_bytes must be positive for skills/upload/start",
    )?;
    require_non_empty_field(
        params.sha256.as_str(),
        "sha256",
        methods::SKILLS_UPLOAD_START,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::SKILLS_UPLOAD_START,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn skills_upload_finish<TTransport>(
    transport: &TTransport,
    params: SkillsUploadFinishParams,
) -> Result<SkillsUploadFinishResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::SKILLS_UPLOAD_FINISH,
    )?;
    require_non_empty_field(
        params.upload_id.as_str(),
        "upload_id",
        methods::SKILLS_UPLOAD_FINISH,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::SKILLS_UPLOAD_FINISH,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn skills_upload_abort<TTransport>(
    transport: &TTransport,
    params: SkillsUploadAbortParams,
) -> Result<SkillsUploadAbortResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::SKILLS_UPLOAD_ABORT,
    )?;
    require_non_empty_field(
        params.upload_id.as_str(),
        "upload_id",
        methods::SKILLS_UPLOAD_ABORT,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::SKILLS_UPLOAD_ABORT,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn skills_update<TTransport>(
    transport: &TTransport,
    params: SkillsUpdateParams,
) -> Result<SkillsUpdateResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::SKILLS_UPDATE,
    )?;
    require_non_empty_field(params.slug.as_str(), "slug", methods::SKILLS_UPDATE)?;
    require_condition(
        is_qualified_skill_slug(params.slug.as_str()),
        "slug must use owner/slug for skills/update",
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::SKILLS_UPDATE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn skills_uninstall<TTransport>(
    transport: &TTransport,
    params: SkillsUninstallParams,
) -> Result<SkillsUninstallResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::SKILLS_UNINSTALL,
    )?;
    require_non_empty_field(params.slug.as_str(), "slug", methods::SKILLS_UNINSTALL)?;
    require_condition(
        is_qualified_skill_slug(params.slug.as_str()),
        "slug must use owner/slug for skills/uninstall",
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::SKILLS_UNINSTALL,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn skills_health<TTransport>(
    transport: &TTransport,
    params: SkillsHealthParams,
) -> Result<SkillsHealthResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::SKILLS_HEALTH,
    )?;
    require_condition(
        !params
            .skills
            .iter()
            .any(|target| !is_qualified_skill_slug(target.slug.as_str())),
        "skills/health targets must use owner/slug in slug",
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::SKILLS_HEALTH,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn skills_policy_list<TTransport>(
    transport: &TTransport,
    params: SkillsPolicyListParams,
) -> Result<SkillsPolicyListResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::SKILLS_POLICY_LIST,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::SKILLS_POLICY_LIST,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn skills_policy_set<TTransport>(
    transport: &TTransport,
    params: SkillsPolicySetParams,
) -> Result<SkillsPolicySetResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::SKILLS_POLICY_SET,
    )?;
    require_non_empty_field(
        params.skill_slug.as_str(),
        "skill_slug",
        methods::SKILLS_POLICY_SET,
    )?;
    require_condition(
        is_qualified_skill_slug(params.skill_slug.as_str()),
        "skill_slug must use owner/slug for skills/policy/set",
    )?;
    require_non_empty_field(
        params.source_kind.as_str(),
        "source_kind",
        methods::SKILLS_POLICY_SET,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::SKILLS_POLICY_SET,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn mcp_list<TTransport>(
    transport: &TTransport,
    params: McpListParams,
) -> Result<McpListResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::MCP_LIST,
    )?;

    send_json_rpc_request_typed(transport, methods::MCP_LIST, &params, RPC_REQUEST_TIMEOUT)
}

pub fn mcp_install<TTransport>(
    transport: &TTransport,
    params: McpInstallParams,
) -> Result<McpInstallResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::MCP_INSTALL,
    )?;
    require_non_empty_field(
        params.config_json.as_str(),
        "config_json",
        methods::MCP_INSTALL,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::MCP_INSTALL,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn mcp_policy_set<TTransport>(
    transport: &TTransport,
    params: McpPolicySetParams,
) -> Result<McpPolicySetResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::MCP_POLICY_SET,
    )?;
    require_non_empty_field(params.name.as_str(), "name", methods::MCP_POLICY_SET)?;

    send_json_rpc_request_typed(
        transport,
        methods::MCP_POLICY_SET,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn mcp_server_restart<TTransport>(
    transport: &TTransport,
    params: McpServerRestartParams,
) -> Result<McpServerRestartResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::MCP_SERVER_RESTART,
    )?;
    require_non_empty_field(params.name.as_str(), "name", methods::MCP_SERVER_RESTART)?;

    send_json_rpc_request_typed(
        transport,
        methods::MCP_SERVER_RESTART,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn mcp_uninstall<TTransport>(
    transport: &TTransport,
    params: McpUninstallParams,
) -> Result<McpUninstallResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::MCP_UNINSTALL,
    )?;
    require_non_empty_field(params.name.as_str(), "name", methods::MCP_UNINSTALL)?;

    send_json_rpc_request_typed(
        transport,
        methods::MCP_UNINSTALL,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn mcp_server_details<TTransport>(
    transport: &TTransport,
    params: McpServerDetailsParams,
) -> Result<McpServerDetailsResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::MCP_SERVER_DETAILS,
    )?;
    require_non_empty_field(
        params.server_id.as_str(),
        "server_id",
        methods::MCP_SERVER_DETAILS,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::MCP_SERVER_DETAILS,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn task_accept<TTransport>(
    transport: &TTransport,
    params: TaskAcceptParams,
) -> Result<TaskAcceptResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    validate_task_review_target(
        params.task_id.as_str(),
        params.run_id.as_str(),
        params.candidate_id.as_str(),
        methods::TASK_ACCEPT,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::TASK_ACCEPT,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn task_revise<TTransport>(
    transport: &TTransport,
    params: TaskReviseParams,
) -> Result<TaskReviseResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    validate_task_review_target(
        params.task_id.as_str(),
        params.run_id.as_str(),
        params.candidate_id.as_str(),
        methods::TASK_REVISE,
    )?;
    require_non_empty_field(params.feedback.as_str(), "feedback", methods::TASK_REVISE)?;

    send_json_rpc_request_typed(
        transport,
        methods::TASK_REVISE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn task_cancel<TTransport>(
    transport: &TTransport,
    params: TaskCancelParams,
) -> Result<TaskCancelResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(params.task_id.as_str(), "task_id", methods::TASK_CANCEL)?;

    send_json_rpc_request_typed(
        transport,
        methods::TASK_CANCEL,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_capabilities<TTransport>(
    transport: &TTransport,
    params: ArtifactCapabilitiesParams,
) -> Result<ArtifactCapabilitiesResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_CAPABILITIES,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_CAPABILITIES,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_list<TTransport>(
    transport: &TTransport,
    params: ArtifactListParams,
) -> Result<ArtifactListResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_LIST,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_LIST,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_list_for_thread<TTransport>(
    transport: &TTransport,
    params: ArtifactListForThreadParams,
) -> Result<ArtifactListResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_LIST_FOR_THREAD,
    )?;
    require_condition(
        !params
            .thread_id
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .is_empty(),
        "thread_id is required for artifact/list/thread",
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_LIST_FOR_THREAD,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_list_for_turn<TTransport>(
    transport: &TTransport,
    params: ArtifactListForTurnParams,
) -> Result<ArtifactListResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_LIST_FOR_TURN,
    )?;
    require_condition(
        !params
            .turn_id
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .is_empty(),
        "turn_id is required for artifact/list/turn",
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_LIST_FOR_TURN,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_list_for_message<TTransport>(
    transport: &TTransport,
    params: ArtifactListForMessageParams,
) -> Result<ArtifactListResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_LIST_FOR_MESSAGE,
    )?;
    require_condition(
        !params
            .message_id
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .is_empty(),
        "message_id is required for artifact/list/message",
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_LIST_FOR_MESSAGE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_get<TTransport>(
    transport: &TTransport,
    params: ArtifactGetParams,
) -> Result<ArtifactGetResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_GET,
    )?;
    require_non_empty_field(
        params.artifact_id.as_str(),
        "artifact_id",
        methods::ARTIFACT_GET,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_GET,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_read<TTransport>(
    transport: &TTransport,
    params: ArtifactReadParams,
) -> Result<ArtifactReadResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_READ,
    )?;
    require_non_empty_field(
        params.artifact_id.as_str(),
        "artifact_id",
        methods::ARTIFACT_READ,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_READ,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_delete<TTransport>(
    transport: &TTransport,
    params: ArtifactDeleteParams,
) -> Result<ArtifactDeleteResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_DELETE,
    )?;
    require_non_empty_field(
        params.artifact_id.as_str(),
        "artifact_id",
        methods::ARTIFACT_DELETE,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_DELETE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_restore<TTransport>(
    transport: &TTransport,
    params: ArtifactRestoreParams,
) -> Result<ArtifactRestoreResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_RESTORE,
    )?;
    require_non_empty_field(
        params.artifact_id.as_str(),
        "artifact_id",
        methods::ARTIFACT_RESTORE,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_RESTORE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_bind<TTransport>(
    transport: &TTransport,
    params: ArtifactBindParams,
) -> Result<ArtifactBindResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_BIND,
    )?;
    require_non_empty_field(
        params.artifact_id.as_str(),
        "artifact_id",
        methods::ARTIFACT_BIND,
    )?;
    require_optional_non_empty_field(
        params.thread_id.as_deref(),
        "thread_id",
        methods::ARTIFACT_BIND,
    )?;
    require_optional_non_empty_field(params.turn_id.as_deref(), "turn_id", methods::ARTIFACT_BIND)?;
    require_optional_non_empty_field(
        params.message_id.as_deref(),
        "message_id",
        methods::ARTIFACT_BIND,
    )?;
    require_optional_non_empty_field(
        params.turn_item_id.as_deref(),
        "turn_item_id",
        methods::ARTIFACT_BIND,
    )?;
    require_optional_non_empty_field(
        params.tool_call_id.as_deref(),
        "tool_call_id",
        methods::ARTIFACT_BIND,
    )?;
    require_optional_non_empty_field(params.task_id.as_deref(), "task_id", methods::ARTIFACT_BIND)?;
    require_optional_non_empty_field(
        params.task_run_id.as_deref(),
        "task_run_id",
        methods::ARTIFACT_BIND,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_BIND,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_download_start<TTransport>(
    transport: &TTransport,
    params: ArtifactDownloadStartParams,
) -> Result<ArtifactDownloadStartResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_DOWNLOAD_START,
    )?;
    require_non_empty_field(
        params.artifact_id.as_str(),
        "artifact_id",
        methods::ARTIFACT_DOWNLOAD_START,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_DOWNLOAD_START,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_download_chunk<TTransport>(
    transport: &TTransport,
    params: ArtifactDownloadChunkParams,
) -> Result<ArtifactDownloadChunkResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_DOWNLOAD_CHUNK,
    )?;
    require_non_empty_field(
        params.download_id.as_str(),
        "download_id",
        methods::ARTIFACT_DOWNLOAD_CHUNK,
    )?;
    require_condition(
        params.len > 0,
        "len must be positive for artifact/download/chunk",
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_DOWNLOAD_CHUNK,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_download_finish<TTransport>(
    transport: &TTransport,
    params: ArtifactDownloadFinishParams,
) -> Result<ArtifactDownloadFinishResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_DOWNLOAD_FINISH,
    )?;
    require_non_empty_field(
        params.download_id.as_str(),
        "download_id",
        methods::ARTIFACT_DOWNLOAD_FINISH,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_DOWNLOAD_FINISH,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_download_abort<TTransport>(
    transport: &TTransport,
    params: ArtifactDownloadAbortParams,
) -> Result<ArtifactDownloadAbortResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_DOWNLOAD_ABORT,
    )?;
    require_non_empty_field(
        params.download_id.as_str(),
        "download_id",
        methods::ARTIFACT_DOWNLOAD_ABORT,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_DOWNLOAD_ABORT,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_upload_start<TTransport>(
    transport: &TTransport,
    params: ArtifactUploadStartParams,
) -> Result<ArtifactUploadStartResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_UPLOAD_START,
    )?;
    require_non_empty_field(
        params.file_name.as_str(),
        "file_name",
        methods::ARTIFACT_UPLOAD_START,
    )?;
    require_non_empty_field(
        params.client_attachment_id.as_str(),
        "client_attachment_id",
        methods::ARTIFACT_UPLOAD_START,
    )?;
    require_non_empty_field(
        params.sha256.as_str(),
        "sha256",
        methods::ARTIFACT_UPLOAD_START,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_UPLOAD_START,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_upload_finish<TTransport>(
    transport: &TTransport,
    params: ArtifactUploadFinishParams,
) -> Result<ArtifactUploadFinishResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_UPLOAD_FINISH,
    )?;
    require_non_empty_field(
        params.upload_id.as_str(),
        "upload_id",
        methods::ARTIFACT_UPLOAD_FINISH,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_UPLOAD_FINISH,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn artifact_upload_abort<TTransport>(
    transport: &TTransport,
    params: ArtifactUploadAbortParams,
) -> Result<ArtifactUploadAbortResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_UPLOAD_ABORT,
    )?;
    require_non_empty_field(
        params.upload_id.as_str(),
        "upload_id",
        methods::ARTIFACT_UPLOAD_ABORT,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_UPLOAD_ABORT,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{JsonRpcResponseSender, WEBSOCKET_WORKER_UNAVAILABLE_MESSAGE};
    use pioneer_protocol::{
        ArtifactUploadSourceKind, JsonRpcRequest, McpScopeKind, SkillArchiveFormat,
        SkillHealthTarget, SkillLifecycleSource, TaskCancelScope, Workspace,
    };
    use serde_json::json;

    struct FakeTransport;

    impl JsonRpcRequestTransport for FakeTransport {
        fn send_json_rpc_request(
            &self,
            _request_id: String,
            payload: String,
            response_tx: JsonRpcResponseSender,
        ) -> std::result::Result<(), String> {
            let request: JsonRpcRequest =
                serde_json::from_str(payload.as_str()).expect("request payload");
            let result = match request.method.as_str() {
                methods::WORKSPACE_LIST => json!({
                    "workspaces": [workspace_json()]
                }),
                methods::THREAD_TREE => json!({
                    "workspace_id": "ws_1",
                    "threads": [],
                    "folders": [],
                    "placements": [],
                    "agents_docs": []
                }),
                methods::PROVIDER_LIST => json!({
                    "providers": [{"name": "openai"}]
                }),
                _ => return Err(WEBSOCKET_WORKER_UNAVAILABLE_MESSAGE.to_owned()),
            };

            response_tx.send(Ok(result)).expect("response send");
            Ok(())
        }
    }

    struct PanicTransport;

    impl JsonRpcRequestTransport for PanicTransport {
        fn send_json_rpc_request(
            &self,
            _request_id: String,
            _payload: String,
            _response_tx: JsonRpcResponseSender,
        ) -> std::result::Result<(), String> {
            panic!("validation should run before transport dispatch");
        }
    }

    fn workspace_json() -> serde_json::Value {
        json!({
            "id": "ws_1",
            "name": "Main",
            "is_active": true,
            "is_current": true,
            "created_at": 1,
            "updated_at": 2
        })
    }

    #[test]
    fn ws_command_sender_workspace_list_decodes_response() {
        let response = workspace_list(&FakeTransport).expect("workspace list");

        assert_eq!(
            response.workspaces,
            vec![Workspace {
                id: "ws_1".to_owned(),
                name: "Main".to_owned(),
                is_active: true,
                is_current: true,
                created_at: 1,
                updated_at: 2,
            }]
        );
    }

    #[test]
    fn ws_command_sender_thread_tree_decodes_response() {
        let response = thread_tree(
            &FakeTransport,
            ThreadTreeParams {
                workspace_id: "ws_1".to_owned(),
            },
        )
        .expect("thread tree");

        assert_eq!(response.workspace_id, "ws_1");
        assert!(response.threads.is_empty());
        assert!(response.folders.is_empty());
        assert!(response.placements.is_empty());
        assert!(response.agents_docs.is_empty());
    }

    #[test]
    fn ws_command_sender_basic_thread_validation_matches_desktop_contract() {
        assert_eq!(
            format!(
                "{:#}",
                thread_start(
                    &PanicTransport,
                    ThreadStartParams {
                        thread_id: " ".to_owned(),
                        workspace_id: "ws_1".to_owned(),
                        name: None,
                        model: None,
                        model_provider: None,
                        sandbox: None,
                        mode: None,
                        origin_kind: None,
                        sidebar_visibility: None,
                        agent_nickname: None,
                        agent_role: None,
                    },
                )
                .expect_err("thread id should be required")
            ),
            "thread_id is required for thread/start"
        );
        assert_eq!(
            format!(
                "{:#}",
                thread_start(
                    &PanicTransport,
                    ThreadStartParams {
                        thread_id: "thread_1".to_owned(),
                        workspace_id: " ".to_owned(),
                        name: None,
                        model: None,
                        model_provider: None,
                        sandbox: None,
                        mode: None,
                        origin_kind: None,
                        sidebar_visibility: None,
                        agent_nickname: None,
                        agent_role: None,
                    },
                )
                .expect_err("workspace id should be required")
            ),
            "workspace_id is required for thread/start"
        );
        assert_eq!(
            format!(
                "{:#}",
                thread_update(
                    &PanicTransport,
                    ThreadUpdateParams {
                        workspace_id: "ws_1".to_owned(),
                        thread_id: "thread_1".to_owned(),
                        name: None,
                    },
                )
                .expect_err("thread update field should be required")
            ),
            "at least one field is required for thread/update"
        );
    }

    #[test]
    fn ws_command_sender_workspace_validation_matches_desktop_contract() {
        assert_eq!(
            format!(
                "{:#}",
                workspace_create(
                    &PanicTransport,
                    WorkspaceCreateParams {
                        workspace_id: " ".to_owned(),
                        name: None,
                        make_current: false,
                    },
                )
                .expect_err("workspace id should be required")
            ),
            "workspace_id is required for workspace/create"
        );
        assert_eq!(
            format!(
                "{:#}",
                workspace_update(
                    &PanicTransport,
                    WorkspaceUpdateParams {
                        workspace_id: "ws_1".to_owned(),
                        name: None,
                    },
                )
                .expect_err("workspace update field should be required")
            ),
            "at least one field is required for workspace/update"
        );
        assert_eq!(
            format!(
                "{:#}",
                workspace_update(
                    &PanicTransport,
                    WorkspaceUpdateParams {
                        workspace_id: "ws_1".to_owned(),
                        name: Some(" ".to_owned()),
                    },
                )
                .expect_err("workspace name should be non-empty")
            ),
            "name must not be empty for workspace/update"
        );
        assert_eq!(
            format!(
                "{:#}",
                workspace_select(
                    &PanicTransport,
                    WorkspaceSelectParams {
                        workspace_id: " ".to_owned(),
                        make_current: true,
                    },
                )
                .expect_err("workspace id should be required")
            ),
            "workspace_id is required for workspace/select"
        );
    }

    #[test]
    fn ws_command_sender_thread_folder_validation_matches_desktop_contract() {
        assert_eq!(
            format!(
                "{:#}",
                thread_folder_create(
                    &PanicTransport,
                    ThreadFolderCreateParams {
                        workspace_id: " ".to_owned(),
                        parent_folder_id: None,
                        name: "Folder".to_owned(),
                    },
                )
                .expect_err("workspace id should be required")
            ),
            "workspace_id is required for thread/folder/create"
        );
        assert_eq!(
            format!(
                "{:#}",
                thread_folder_move(
                    &PanicTransport,
                    ThreadFolderMoveParams {
                        workspace_id: "ws_1".to_owned(),
                        folder_id: " ".to_owned(),
                        parent_folder_id: None,
                    },
                )
                .expect_err("folder id should be required")
            ),
            "folder_id is required for thread/folder/move"
        );
    }

    #[test]
    fn ws_command_sender_agents_doc_validation_matches_desktop_contract() {
        assert_eq!(
            format!(
                "{:#}",
                thread_agents_doc_get(
                    &PanicTransport,
                    ThreadAgentsDocGetParams {
                        workspace_id: " ".to_owned(),
                        folder_id: None,
                    },
                )
                .expect_err("workspace id should be required")
            ),
            "workspace_id is required for thread/agents_doc/get"
        );
        assert_eq!(
            format!(
                "{:#}",
                thread_agents_doc_resolve_for_thread(
                    &PanicTransport,
                    ThreadAgentsDocResolveForThreadParams {
                        workspace_id: "ws_1".to_owned(),
                        thread_id: " ".to_owned(),
                    },
                )
                .expect_err("thread id should be required")
            ),
            "thread_id is required for thread/agents_doc/resolve_for_thread"
        );
    }

    #[test]
    fn ws_command_sender_turn_validation_matches_desktop_contract() {
        assert_eq!(
            format!(
                "{:#}",
                turn_start(
                    &PanicTransport,
                    TurnStartParams {
                        thread_id: " ".to_owned(),
                        turn_id: "turn_1".to_owned(),
                        input: Vec::new(),
                        capabilities: Vec::new(),
                        model: None,
                        model_provider: None,
                        sandbox_policy: None,
                        mode: None,
                        execution_backend: None,
                        reasoning: None,
                        cli_runtime_options: None,
                    },
                )
                .expect_err("thread id should be required")
            ),
            "thread_id is required for turn/start"
        );
        assert_eq!(
            format!(
                "{:#}",
                turn_start(
                    &PanicTransport,
                    TurnStartParams {
                        thread_id: "thread_1".to_owned(),
                        turn_id: " ".to_owned(),
                        input: Vec::new(),
                        capabilities: Vec::new(),
                        model: None,
                        model_provider: None,
                        sandbox_policy: None,
                        mode: None,
                        execution_backend: None,
                        reasoning: None,
                        cli_runtime_options: None,
                    },
                )
                .expect_err("turn id should be required")
            ),
            "turn_id is required for turn/start"
        );
        assert_eq!(
            format!(
                "{:#}",
                turn_timeline(
                    &PanicTransport,
                    TurnTimelineParams {
                        thread_id: "thread_1".to_owned(),
                        turn_id: " ".to_owned(),
                        compose_tasks: true,
                        include_collapsed_task_events: false,
                        max_child_items_per_task: None,
                    },
                )
                .expect_err("turn id should be required")
            ),
            "turn_id is required for turn/timeline"
        );
    }

    #[test]
    fn ws_command_sender_provider_list_decodes_response() {
        let response = provider_list(
            &FakeTransport,
            ProviderListParams {
                workspace_id: "ws_1".to_owned(),
            },
        )
        .expect("provider list");

        assert_eq!(response.providers.len(), 1);
        assert_eq!(response.providers[0].name, "openai");
    }

    #[test]
    fn ws_command_sender_provider_validation_matches_desktop_contract() {
        assert_eq!(
            format!(
                "{:#}",
                provider_list(
                    &PanicTransport,
                    ProviderListParams {
                        workspace_id: " ".to_owned(),
                    },
                )
                .expect_err("workspace id should be required")
            ),
            "workspace_id is required for provider/list"
        );
        assert_eq!(
            format!(
                "{:#}",
                provider_list_models(
                    &PanicTransport,
                    ProviderListModelsParams {
                        workspace_id: "ws_1".to_owned(),
                        provider: " ".to_owned(),
                    },
                )
                .expect_err("provider should be required")
            ),
            "provider is required for provider/models/list"
        );
        assert_eq!(
            format!(
                "{:#}",
                provider_set_api_key(
                    &PanicTransport,
                    ProviderSetApiKeyParams {
                        workspace_id: "ws_1".to_owned(),
                        provider: "openai".to_owned(),
                        api_key: " ".to_owned(),
                    },
                )
                .expect_err("api key should be required")
            ),
            "api_key is required for provider/set_api_key"
        );
    }

    #[test]
    fn ws_command_sender_skills_validation_matches_desktop_contract() {
        assert_eq!(
            format!(
                "{:#}",
                skills_upload_start(
                    &PanicTransport,
                    SkillsUploadStartParams {
                        workspace_id: "ws_1".to_owned(),
                        file_name: "skill.tar.gz".to_owned(),
                        archive_format: SkillArchiveFormat::TarGz,
                        compressed_size_bytes: 0,
                        uncompressed_size_hint_bytes: None,
                        sha256: "abc".to_owned(),
                    },
                )
                .expect_err("compressed size should be positive")
            ),
            "compressed_size_bytes must be positive for skills/upload/start"
        );
        assert_eq!(
            format!(
                "{:#}",
                skills_update(
                    &PanicTransport,
                    SkillsUpdateParams {
                        workspace_id: "ws_1".to_owned(),
                        slug: "bad-slug".to_owned(),
                        source_kind: "user".to_owned(),
                        source: SkillLifecycleSource::UploadedArchive {
                            upload_id: "upload_1".to_owned(),
                        },
                        expected_previous_fingerprint: None,
                    },
                )
                .expect_err("slug should be qualified")
            ),
            "slug must use owner/slug for skills/update"
        );
        assert_eq!(
            format!(
                "{:#}",
                skills_health(
                    &PanicTransport,
                    SkillsHealthParams {
                        workspace_id: "ws_1".to_owned(),
                        skills: vec![SkillHealthTarget {
                            slug: "bad-slug".to_owned(),
                            source_kind: "user".to_owned(),
                        }],
                        audit_limit: 16,
                    },
                )
                .expect_err("health target slug should be qualified")
            ),
            "skills/health targets must use owner/slug in slug"
        );
    }

    #[test]
    fn ws_command_sender_mcp_validation_matches_desktop_contract() {
        assert_eq!(
            format!(
                "{:#}",
                mcp_install(
                    &PanicTransport,
                    McpInstallParams {
                        workspace_id: "ws_1".to_owned(),
                        config_json: " ".to_owned(),
                        scope_kind: McpScopeKind::Workspace,
                        enabled: true,
                        allow_implicit_invocation: false,
                    },
                )
                .expect_err("config json should be required")
            ),
            "config_json is required for mcp/install"
        );
        assert_eq!(
            format!(
                "{:#}",
                mcp_server_details(
                    &PanicTransport,
                    McpServerDetailsParams {
                        workspace_id: "ws_1".to_owned(),
                        server_id: " ".to_owned(),
                    },
                )
                .expect_err("server id should be required")
            ),
            "server_id is required for mcp/server/details"
        );
    }

    #[test]
    fn ws_command_sender_task_review_validation_matches_desktop_contract() {
        assert_eq!(
            format!(
                "{:#}",
                task_accept(
                    &PanicTransport,
                    TaskAcceptParams {
                        task_id: " ".to_owned(),
                        run_id: "run_1".to_owned(),
                        candidate_id: "candidate_1".to_owned(),
                        reason: None,
                    },
                )
                .expect_err("task id should be required")
            ),
            "task_id is required for task/accept"
        );
        assert_eq!(
            format!(
                "{:#}",
                task_revise(
                    &PanicTransport,
                    TaskReviseParams {
                        task_id: "task_1".to_owned(),
                        run_id: " ".to_owned(),
                        candidate_id: "candidate_1".to_owned(),
                        feedback: "feedback".to_owned(),
                        additional_instructions: Vec::new(),
                    },
                )
                .expect_err("run id should be required")
            ),
            "run_id is required for task/revise"
        );
        assert_eq!(
            format!(
                "{:#}",
                task_cancel(
                    &PanicTransport,
                    TaskCancelParams {
                        task_id: " ".to_owned(),
                        reason: None,
                        scope: TaskCancelScope::AttachedSubtree,
                    },
                )
                .expect_err("task id should be required")
            ),
            "task_id is required for task/cancel"
        );
        assert_eq!(
            format!(
                "{:#}",
                task_accept(
                    &PanicTransport,
                    TaskAcceptParams {
                        task_id: "task_1".to_owned(),
                        run_id: "run_1".to_owned(),
                        candidate_id: " ".to_owned(),
                        reason: None,
                    },
                )
                .expect_err("candidate id should be required")
            ),
            "candidate_id is required for task/accept"
        );
        assert_eq!(
            format!(
                "{:#}",
                task_revise(
                    &PanicTransport,
                    TaskReviseParams {
                        task_id: "task_1".to_owned(),
                        run_id: "run_1".to_owned(),
                        candidate_id: "candidate_1".to_owned(),
                        feedback: " ".to_owned(),
                        additional_instructions: Vec::new(),
                    },
                )
                .expect_err("feedback should be required")
            ),
            "feedback is required for task/revise"
        );
    }

    #[test]
    fn ws_command_sender_artifact_validation_matches_desktop_contract() {
        assert_eq!(
            format!(
                "{:#}",
                artifact_capabilities(
                    &PanicTransport,
                    ArtifactCapabilitiesParams {
                        workspace_id: " ".to_owned(),
                    },
                )
                .expect_err("workspace id should be required")
            ),
            "workspace_id is required for artifact/capabilities"
        );
        assert_eq!(
            format!(
                "{:#}",
                artifact_list_for_thread(
                    &PanicTransport,
                    ArtifactListForThreadParams {
                        workspace_id: "ws_1".to_owned(),
                        thread_id: Some(" ".to_owned()),
                        ..ArtifactListForThreadParams::default()
                    },
                )
                .expect_err("thread id should be required")
            ),
            "thread_id is required for artifact/list/thread"
        );
        assert_eq!(
            format!(
                "{:#}",
                artifact_download_chunk(
                    &PanicTransport,
                    ArtifactDownloadChunkParams {
                        workspace_id: "ws_1".to_owned(),
                        download_id: "download_1".to_owned(),
                        offset: 0,
                        len: 0,
                    },
                )
                .expect_err("len should be positive")
            ),
            "len must be positive for artifact/download/chunk"
        );
        assert_eq!(
            format!(
                "{:#}",
                artifact_upload_start(
                    &PanicTransport,
                    ArtifactUploadStartParams {
                        workspace_id: "ws_1".to_owned(),
                        thread_id: None,
                        planned_turn_id: None,
                        client_attachment_id: " ".to_owned(),
                        file_name: "artifact.txt".to_owned(),
                        mime_type: None,
                        size_bytes: 1,
                        sha256: "abc".to_owned(),
                        source_kind: ArtifactUploadSourceKind::UserComposer,
                    },
                )
                .expect_err("client attachment id should be required")
            ),
            "client_attachment_id is required for artifact/upload/start"
        );
    }
}
