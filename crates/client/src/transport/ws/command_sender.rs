//! WebSocket typed command helpers.

use crate::rpc::validation::{
    require_condition, require_non_empty_field, require_optional_non_empty_field,
    validate_task_review_target,
};
use crate::rpc::{
    JsonRpcAuthorizationFailure, JsonRpcRequestTransport, RPC_REQUEST_TIMEOUT,
    RPC_UNSUBSCRIBE_TIMEOUT, json_rpc_authorization_failure, json_rpc_response_error,
    send_json_rpc_request_typed,
};
use anyhow::{Result, anyhow};
use pioneer_protocol::constants::methods;
use pioneer_protocol::{
    ArtifactBindParams, ArtifactBindResponse, ArtifactCapabilitiesParams,
    ArtifactCapabilitiesResponse, ArtifactDeleteParams, ArtifactDeleteResponse, ArtifactGetParams,
    ArtifactGetResponse, ArtifactListForMessageParams, ArtifactListForThreadParams,
    ArtifactListForTurnParams, ArtifactListParams, ArtifactListResponse, ArtifactRestoreParams,
    ArtifactRestoreResponse, ArtifactUploadAbortParams, ArtifactUploadAbortResponse,
    ArtifactUploadFinishParams, ArtifactUploadFinishResponse, ArtifactUploadStartParams,
    ArtifactUploadStartResponse, ArtifactViewGrantCreateParams, ArtifactViewGrantCreateResponse,
    AuthDeviceCreateResponse, AuthLogoutResponse, AuthMeResponse, AuthProfileUpdateParams,
    AuthProfileUpdateResponse, AuthSessionListResponse, AuthSessionRevokeParams,
    AuthSessionRevokeResponse, AuthorizationCapabilitiesParams, AuthorizationCapabilitySnapshot,
    CLIRuntimeListModelsParams, CLIRuntimeListModelsResponse, CLIRuntimeListParams,
    CLIRuntimeListResponse, CLIRuntimeLoginCancelParams, CLIRuntimeLoginCancelResponse,
    CLIRuntimeLoginStartParams, CLIRuntimeLoginStartResponse, CLIRuntimeProxyDeleteParams,
    CLIRuntimeProxyDeleteResponse, CLIRuntimeProxySetParams, CLIRuntimeProxySetResponse,
    CLIRuntimeRefreshParams, CLIRuntimeRefreshResponse, CLIRuntimeRequestRespondParams,
    CLIRuntimeRequestRespondResponse, CLIRuntimeReviewStartParams, CLIRuntimeReviewStartResponse,
    CLIRuntimeStatusParams, CLIRuntimeStatusResponse, CLIRuntimeThreadBindingGetParams,
    CLIRuntimeThreadBindingGetResponse, CLIRuntimeThreadCompactParams,
    CLIRuntimeThreadCompactResponse, CLIRuntimeThreadForkParams, CLIRuntimeThreadForkResponse,
    CLIRuntimeTurnSteerParams, CLIRuntimeTurnSteerResponse, GatewaySettingsGetParams,
    GatewaySettingsGetResponse, GatewaySettingsUpdate, GatewaySettingsUpdateParams,
    GatewaySettingsUpdateResponse, InvitationCreateParams, InvitationCreateResponse,
    InvitationListParams, InvitationListResponse, InvitationRevokeParams, InvitationRevokeResponse,
    McpInstallParams, McpInstallResponse, McpListParams, McpListResponse, McpPolicySetParams,
    McpPolicySetResponse, McpServerDetailsParams, McpServerDetailsResponse, McpServerRestartParams,
    McpServerRestartResponse, McpUninstallParams, McpUninstallResponse, MemberDeviceCreateParams,
    MemberDeviceCreateResponse, MemberListParams, MemberListResponse, MemberMutationResponse,
    MemberRemoveParams, MemberRestoreParams, MemberSuspendParams, ProviderConfigureParams,
    ProviderConfigureResponse, ProviderDeleteApiKeyParams, ProviderDeleteApiKeyResponse,
    ProviderListModelsParams, ProviderListModelsResponse, ProviderListParams, ProviderListResponse,
    ProviderSetApiKeyParams, ProviderSetApiKeyResponse, SkillListParams, SkillListResponse,
    SkillsHealthParams, SkillsHealthResponse, SkillsInstallParams, SkillsInstallResponse,
    SkillsPackInstallParams, SkillsPackInstallResponse, SkillsPackUninstallParams,
    SkillsPackUninstallResponse, SkillsPackUpdateParams, SkillsPackUpdateResponse,
    SkillsPolicyListParams, SkillsPolicyListResponse, SkillsPolicySetParams,
    SkillsPolicySetResponse, SkillsUninstallParams, SkillsUninstallResponse, SkillsUpdateParams,
    SkillsUpdateResponse, SkillsUploadAbortParams, SkillsUploadAbortResponse,
    SkillsUploadFinishParams, SkillsUploadFinishResponse, SkillsUploadStartParams,
    SkillsUploadStartResponse, TaskAcceptParams, TaskAcceptResponse, TaskCancelParams,
    TaskCancelResponse, TaskReviseParams, TaskReviseResponse, ThreadAgentsDocArchiveParams,
    ThreadAgentsDocArchiveResponse, ThreadAgentsDocGetParams, ThreadAgentsDocGetResponse,
    ThreadAgentsDocResolveForThreadParams, ThreadAgentsDocResolveForThreadResponse,
    ThreadAgentsDocSaveParams, ThreadAgentsDocSaveResponse, ThreadFolderCreateParams,
    ThreadFolderCreateResponse, ThreadFolderDeleteParams, ThreadFolderDeleteResponse,
    ThreadFolderMoveParams, ThreadFolderMoveResponse, ThreadGetParams, ThreadGetResponse,
    ThreadMoveParams, ThreadMoveResponse, ThreadParticipantMutationParams,
    ThreadParticipantsListParams, ThreadParticipantsResponse, ThreadReadParams, ThreadReadResponse,
    ThreadStartParams, ThreadStartResponse, ThreadTimelinePageParams, ThreadTimelinePageResponse,
    ThreadTreeParams, ThreadTreeResponse, ThreadUnsubscribeParams, ThreadUnsubscribeResponse,
    ThreadUpdateParams, ThreadUpdateResponse, TurnCancelParams, TurnCancelResponse, TurnGetParams,
    TurnGetResponse, TurnItemsParams, TurnItemsResponse, TurnMessageDeleteParams,
    TurnMessageDeleteResponse, TurnMessageEditParams, TurnMessageEditResponse,
    TurnMessageErrorReason, TurnMessageRevisionsPageParams, TurnMessageRevisionsPageResponse,
    TurnPermissionRequestRespondParams, TurnPermissionRequestRespondResponse, TurnStartParams,
    TurnStartResponse, TurnWorkItemsGetParams, TurnWorkItemsGetResponse, TurnWorkPageParams,
    TurnWorkPageResponse, VoiceAudioFormat, VoiceSessionCancelParams, VoiceSessionCancelResponse,
    VoiceSessionFinalizeParams, VoiceSessionFinalizeResponse, VoiceSessionStartParams,
    VoiceSessionStartResponse, VoiceStatusParams, VoiceStatusResponse, WorkspaceCreateParams,
    WorkspaceCreateResponse, WorkspaceDefaultParams, WorkspaceDefaultResponse, WorkspaceListParams,
    WorkspaceListResponse, WorkspaceMemberAddParams, WorkspaceMemberListParams,
    WorkspaceMemberListResponse, WorkspaceMemberMutationResponse, WorkspaceMemberRemoveParams,
    WorkspaceSelectParams, WorkspaceSelectResponse, WorkspaceUpdateParams, WorkspaceUpdateResponse,
    validate_voice_streaming_audio_format,
};
use std::time::Duration;

pub const PROVIDER_MODELS_TIMEOUT: Duration = Duration::from_secs(30);

const AUTH_ERROR_MACHINE_CODES: &[&str] = &[
    "credential_expired",
    "gateway_identity_mismatch",
    "invalid_credential",
    "session_compromised",
    "session_expired",
    "session_revoked",
    "nickname_unavailable",
    "avatar_invalid",
];

fn send_auth_json_rpc_request_typed<TResponse, TParams, TTransport>(
    transport: &TTransport,
    method: &str,
    params: &TParams,
) -> Result<TResponse>
where
    TResponse: serde::de::DeserializeOwned,
    TParams: serde::Serialize,
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(transport, method, params, RPC_REQUEST_TIMEOUT)
        .map_err(sanitize_auth_rpc_error)
}

fn sanitize_auth_rpc_error(error: anyhow::Error) -> anyhow::Error {
    if let Some(machine_code) = json_rpc_response_error(&error)
        .and_then(|response| response.machine_code())
        .filter(|code| AUTH_ERROR_MACHINE_CODES.contains(code))
    {
        return anyhow!(machine_code.to_owned());
    }
    if json_rpc_authorization_failure(&error)
        == Some(JsonRpcAuthorizationFailure::AuthenticationTerminal)
    {
        return anyhow!("authentication_terminal");
    }

    let rendered = format!("{error:#}");
    let machine_code = AUTH_ERROR_MACHINE_CODES
        .iter()
        .copied()
        .find(|code| rendered.contains(code));
    match machine_code {
        // Keep the stable code exact so lifecycle reducers can classify it
        // without parsing a peer-controlled error message.
        Some(code) => anyhow!(code),
        None => anyhow!("Gateway authentication request failed"),
    }
}

pub fn auth_me<TTransport>(transport: &TTransport) -> Result<AuthMeResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_auth_json_rpc_request_typed(transport, methods::AUTH_ME, &serde_json::json!({}))
}

pub fn authorization_capabilities<TTransport>(
    transport: &TTransport,
    params: AuthorizationCapabilitiesParams,
) -> Result<AuthorizationCapabilitySnapshot>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    let snapshot: AuthorizationCapabilitySnapshot =
        send_auth_json_rpc_request_typed(transport, methods::AUTHORIZATION_CAPABILITIES, &params)?;
    anyhow::ensure!(
        snapshot.schema_version
            == pioneer_protocol::AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
        "Gateway returned an unsupported authorization capability schema"
    );
    anyhow::ensure!(
        snapshot.workspace.as_ref().is_none_or(|workspace| {
            params.workspace_id.as_deref() == Some(workspace.workspace_id.as_str())
        }),
        "Gateway returned capabilities for a different workspace"
    );
    anyhow::ensure!(
        snapshot.thread.as_ref().is_none_or(|thread| {
            params.workspace_id.as_deref() == Some(thread.workspace_id.as_str())
                && params.thread_id.as_deref() == Some(thread.thread_id.as_str())
        }),
        "Gateway returned capabilities for a different thread"
    );
    Ok(snapshot)
}

pub fn auth_profile_update<TTransport>(
    transport: &TTransport,
    params: AuthProfileUpdateParams,
) -> Result<AuthProfileUpdateResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_auth_json_rpc_request_typed(transport, methods::AUTH_PROFILE_UPDATE, &params)
}

pub fn auth_session_list<TTransport>(transport: &TTransport) -> Result<AuthSessionListResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_auth_json_rpc_request_typed(
        transport,
        methods::AUTH_SESSION_LIST,
        &serde_json::json!({}),
    )
}

pub fn auth_session_revoke<TTransport>(
    transport: &TTransport,
    params: AuthSessionRevokeParams,
) -> Result<AuthSessionRevokeResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_auth_json_rpc_request_typed(transport, methods::AUTH_SESSION_REVOKE, &params)
}

pub fn auth_logout<TTransport>(transport: &TTransport) -> Result<AuthLogoutResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_auth_json_rpc_request_typed(transport, methods::AUTH_LOGOUT, &serde_json::json!({}))
}

pub fn auth_device_create<TTransport>(transport: &TTransport) -> Result<AuthDeviceCreateResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_auth_json_rpc_request_typed(
        transport,
        methods::AUTH_DEVICE_CREATE,
        &serde_json::json!({}),
    )
}

pub fn invitation_create<TTransport>(
    transport: &TTransport,
    params: InvitationCreateParams,
) -> Result<InvitationCreateResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(
        transport,
        methods::INVITE_CREATE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn invitation_list<TTransport>(
    transport: &TTransport,
    params: InvitationListParams,
) -> Result<InvitationListResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(
        transport,
        methods::INVITE_LIST,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn invitation_revoke<TTransport>(
    transport: &TTransport,
    params: InvitationRevokeParams,
) -> Result<InvitationRevokeResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(
        transport,
        methods::INVITE_REVOKE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn member_list<TTransport>(
    transport: &TTransport,
    params: MemberListParams,
) -> Result<MemberListResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(
        transport,
        methods::MEMBER_LIST,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn member_suspend<TTransport>(
    transport: &TTransport,
    params: MemberSuspendParams,
) -> Result<MemberMutationResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(
        transport,
        methods::MEMBER_SUSPEND,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn member_restore<TTransport>(
    transport: &TTransport,
    params: MemberRestoreParams,
) -> Result<MemberMutationResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(
        transport,
        methods::MEMBER_RESTORE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn member_remove<TTransport>(
    transport: &TTransport,
    params: MemberRemoveParams,
) -> Result<MemberMutationResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(
        transport,
        methods::MEMBER_REMOVE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn member_device_create<TTransport>(
    transport: &TTransport,
    params: MemberDeviceCreateParams,
) -> Result<MemberDeviceCreateResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(
        transport,
        methods::MEMBER_DEVICE_CREATE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn workspace_member_list<TTransport>(
    transport: &TTransport,
    params: WorkspaceMemberListParams,
) -> Result<WorkspaceMemberListResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(
        transport,
        methods::WORKSPACE_MEMBER_LIST,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn workspace_member_add<TTransport>(
    transport: &TTransport,
    params: WorkspaceMemberAddParams,
) -> Result<WorkspaceMemberMutationResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(
        transport,
        methods::WORKSPACE_MEMBER_ADD,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn workspace_member_remove<TTransport>(
    transport: &TTransport,
    params: WorkspaceMemberRemoveParams,
) -> Result<WorkspaceMemberMutationResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    send_json_rpc_request_typed(
        transport,
        methods::WORKSPACE_MEMBER_REMOVE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

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
        params.name.is_some() || params.visibility.is_some() || params.archived.is_some(),
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

pub fn thread_participants_list<TTransport>(
    transport: &TTransport,
    params: ThreadParticipantsListParams,
) -> Result<ThreadParticipantsResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::THREAD_PARTICIPANTS_LIST,
    )?;
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::THREAD_PARTICIPANTS_LIST,
    )?;
    send_json_rpc_request_typed(
        transport,
        methods::THREAD_PARTICIPANTS_LIST,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn thread_participant_add<TTransport>(
    transport: &TTransport,
    params: ThreadParticipantMutationParams,
) -> Result<ThreadParticipantsResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::THREAD_PARTICIPANTS_ADD,
    )?;
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::THREAD_PARTICIPANTS_ADD,
    )?;
    send_json_rpc_request_typed(
        transport,
        methods::THREAD_PARTICIPANTS_ADD,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn thread_participant_remove<TTransport>(
    transport: &TTransport,
    params: ThreadParticipantMutationParams,
) -> Result<ThreadParticipantsResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::THREAD_PARTICIPANTS_REMOVE,
    )?;
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::THREAD_PARTICIPANTS_REMOVE,
    )?;
    send_json_rpc_request_typed(
        transport,
        methods::THREAD_PARTICIPANTS_REMOVE,
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

pub fn thread_timeline_page<TTransport>(
    transport: &TTransport,
    params: ThreadTimelinePageParams,
) -> Result<ThreadTimelinePageResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::THREAD_TIMELINE_PAGE,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::THREAD_TIMELINE_PAGE,
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

pub fn turn_message_edit<TTransport>(
    transport: &TTransport,
    params: TurnMessageEditParams,
) -> Result<TurnMessageEditResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::TURN_MESSAGE_EDIT,
    )?;
    require_non_empty_field(
        params.turn_id.as_str(),
        "turn_id",
        methods::TURN_MESSAGE_EDIT,
    )?;
    send_json_rpc_request_typed(
        transport,
        methods::TURN_MESSAGE_EDIT,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

/// Returns the stable server reason for a failed Message mutation.
///
/// Callers use `RevisionConflict` as a refetch signal. The shared client does
/// not maintain an optimistic second copy of the message, so recovery is the
/// existing authoritative Turn/timeline reload rather than a rollback state
/// machine.
pub fn turn_message_error_reason(error: &anyhow::Error) -> Option<TurnMessageErrorReason> {
    let code = crate::rpc::json_rpc_response_error(error)?.machine_code()?;
    match code {
        "invalid_input" => Some(TurnMessageErrorReason::InvalidInput),
        "invalid_target" => Some(TurnMessageErrorReason::InvalidTarget),
        "immutable_message" => Some(TurnMessageErrorReason::ImmutableMessage),
        "deleted_message" => Some(TurnMessageErrorReason::DeletedMessage),
        "revision_conflict" => Some(TurnMessageErrorReason::RevisionConflict),
        _ => None,
    }
}

pub fn turn_message_delete<TTransport>(
    transport: &TTransport,
    params: TurnMessageDeleteParams,
) -> Result<TurnMessageDeleteResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::TURN_MESSAGE_DELETE,
    )?;
    require_non_empty_field(
        params.turn_id.as_str(),
        "turn_id",
        methods::TURN_MESSAGE_DELETE,
    )?;
    send_json_rpc_request_typed(
        transport,
        methods::TURN_MESSAGE_DELETE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn turn_message_revisions_page<TTransport>(
    transport: &TTransport,
    params: TurnMessageRevisionsPageParams,
) -> Result<TurnMessageRevisionsPageResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::TURN_MESSAGE_REVISIONS_PAGE,
    )?;
    require_non_empty_field(
        params.turn_id.as_str(),
        "turn_id",
        methods::TURN_MESSAGE_REVISIONS_PAGE,
    )?;
    send_json_rpc_request_typed(
        transport,
        methods::TURN_MESSAGE_REVISIONS_PAGE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn thread_read<TTransport>(
    transport: &TTransport,
    params: ThreadReadParams,
) -> Result<ThreadReadResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(params.thread_id.as_str(), "thread_id", methods::THREAD_READ)?;
    require_non_empty_field(
        params.through_turn_id.as_str(),
        "through_turn_id",
        methods::THREAD_READ,
    )?;
    send_json_rpc_request_typed(
        transport,
        methods::THREAD_READ,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
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

pub fn turn_work_page<TTransport>(
    transport: &TTransport,
    params: TurnWorkPageParams,
) -> Result<TurnWorkPageResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::TURN_WORK_PAGE,
    )?;
    require_non_empty_field(params.turn_id.as_str(), "turn_id", methods::TURN_WORK_PAGE)?;

    send_json_rpc_request_typed(
        transport,
        methods::TURN_WORK_PAGE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn turn_work_items_get<TTransport>(
    transport: &TTransport,
    params: TurnWorkItemsGetParams,
) -> Result<TurnWorkItemsGetResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.thread_id.as_str(),
        "thread_id",
        methods::TURN_WORK_ITEMS_GET,
    )?;
    require_non_empty_field(
        params.turn_id.as_str(),
        "turn_id",
        methods::TURN_WORK_ITEMS_GET,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::TURN_WORK_ITEMS_GET,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn voice_status<TTransport>(
    transport: &TTransport,
    params: VoiceStatusParams,
) -> Result<VoiceStatusResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_optional_non_empty_field(
        params.workspace_id.as_deref(),
        "workspace_id",
        methods::VOICE_STATUS,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::VOICE_STATUS,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn voice_session_start<TTransport>(
    transport: &TTransport,
    params: VoiceSessionStartParams,
) -> Result<VoiceSessionStartResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.context.workspace_id.as_str(),
        "workspace_id",
        methods::VOICE_SESSION_START,
    )?;
    require_non_empty_field(
        params.context.thread_id.as_str(),
        "thread_id",
        methods::VOICE_SESSION_START,
    )?;
    require_non_empty_field(
        params.context.turn_id.as_str(),
        "turn_id",
        methods::VOICE_SESSION_START,
    )?;
    validate_voice_audio_format(&params.audio_format, methods::VOICE_SESSION_START)?;

    send_json_rpc_request_typed(
        transport,
        methods::VOICE_SESSION_START,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn voice_session_finalize<TTransport>(
    transport: &TTransport,
    params: VoiceSessionFinalizeParams,
) -> Result<VoiceSessionFinalizeResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.session_id.as_str(),
        "session_id",
        methods::VOICE_SESSION_FINALIZE,
    )?;
    require_non_empty_field(
        params.context.workspace_id.as_str(),
        "workspace_id",
        methods::VOICE_SESSION_FINALIZE,
    )?;
    require_non_empty_field(
        params.context.thread_id.as_str(),
        "thread_id",
        methods::VOICE_SESSION_FINALIZE,
    )?;
    require_non_empty_field(
        params.context.turn_id.as_str(),
        "turn_id",
        methods::VOICE_SESSION_FINALIZE,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::VOICE_SESSION_FINALIZE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn voice_session_cancel<TTransport>(
    transport: &TTransport,
    params: VoiceSessionCancelParams,
) -> Result<VoiceSessionCancelResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.session_id.as_str(),
        "session_id",
        methods::VOICE_SESSION_CANCEL,
    )?;
    require_optional_non_empty_field(
        params.reason.as_deref(),
        "reason",
        methods::VOICE_SESSION_CANCEL,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::VOICE_SESSION_CANCEL,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

fn validate_voice_audio_format(format: &VoiceAudioFormat, method: &str) -> Result<()> {
    validate_voice_streaming_audio_format(format)
        .map_err(|error| anyhow!("{error} for {method}"))?;
    Ok(())
}

pub fn turn_permission_request_respond<TTransport>(
    transport: &TTransport,
    params: TurnPermissionRequestRespondParams,
) -> Result<TurnPermissionRequestRespondResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.request_id.as_str(),
        "request_id",
        methods::TURN_PERMISSION_REQUEST_RESPOND,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::TURN_PERMISSION_REQUEST_RESPOND,
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

pub fn provider_list_embedding_models<TTransport>(
    transport: &TTransport,
    params: ProviderListModelsParams,
) -> Result<ProviderListModelsResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::PROVIDER_EMBEDDING_MODELS_LIST,
    )?;
    require_non_empty_field(
        params.provider.as_str(),
        "provider",
        methods::PROVIDER_EMBEDDING_MODELS_LIST,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::PROVIDER_EMBEDDING_MODELS_LIST,
        &params,
        PROVIDER_MODELS_TIMEOUT,
    )
}

pub fn provider_list_transcription_models<TTransport>(
    transport: &TTransport,
    params: ProviderListModelsParams,
) -> Result<ProviderListModelsResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::PROVIDER_TRANSCRIPTION_MODELS_LIST,
    )?;
    require_non_empty_field(
        params.provider.as_str(),
        "provider",
        methods::PROVIDER_TRANSCRIPTION_MODELS_LIST,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::PROVIDER_TRANSCRIPTION_MODELS_LIST,
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

pub fn provider_configure<TTransport>(
    transport: &TTransport,
    params: ProviderConfigureParams,
) -> Result<ProviderConfigureResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::PROVIDER_CONFIGURE,
    )?;
    require_non_empty_field(
        params.provider.as_str(),
        "provider",
        methods::PROVIDER_CONFIGURE,
    )?;
    require_optional_non_empty_field(
        params.api_key.as_deref(),
        "api_key",
        methods::PROVIDER_CONFIGURE,
    )?;
    require_optional_non_empty_field(
        params.proxy_url.as_deref(),
        "proxy_url",
        methods::PROVIDER_CONFIGURE,
    )?;
    require_condition(
        params.api_key.is_some() || params.proxy_url.is_some() || params.clear_proxy,
        "at least one field is required for provider/configure",
    )?;
    require_condition(
        !(params.proxy_url.is_some() && params.clear_proxy),
        "`proxy_url` and `clear_proxy` cannot both be set for provider/configure",
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::PROVIDER_CONFIGURE,
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

pub fn cli_runtime_proxy_set<TTransport>(
    transport: &TTransport,
    params: CLIRuntimeProxySetParams,
) -> Result<CLIRuntimeProxySetResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::CLI_RUNTIME_PROXY_SET,
    )?;
    require_non_empty_field(
        params.runtime_id.as_str(),
        "runtime_id",
        methods::CLI_RUNTIME_PROXY_SET,
    )?;
    require_non_empty_field(
        params.proxy_url.as_str(),
        "proxy_url",
        methods::CLI_RUNTIME_PROXY_SET,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::CLI_RUNTIME_PROXY_SET,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn cli_runtime_proxy_delete<TTransport>(
    transport: &TTransport,
    params: CLIRuntimeProxyDeleteParams,
) -> Result<CLIRuntimeProxyDeleteResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::CLI_RUNTIME_PROXY_DELETE,
    )?;
    require_non_empty_field(
        params.runtime_id.as_str(),
        "runtime_id",
        methods::CLI_RUNTIME_PROXY_DELETE,
    )?;

    send_json_rpc_request_typed(
        transport,
        methods::CLI_RUNTIME_PROXY_DELETE,
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

pub fn skills_pack_install<TTransport>(
    transport: &TTransport,
    params: SkillsPackInstallParams,
) -> Result<SkillsPackInstallResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::SKILLS_PACK_INSTALL,
    )?;
    send_json_rpc_request_typed(
        transport,
        methods::SKILLS_PACK_INSTALL,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn skills_pack_update<TTransport>(
    transport: &TTransport,
    params: SkillsPackUpdateParams,
) -> Result<SkillsPackUpdateResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::SKILLS_PACK_UPDATE,
    )?;
    send_json_rpc_request_typed(
        transport,
        methods::SKILLS_PACK_UPDATE,
        &params,
        RPC_REQUEST_TIMEOUT,
    )
}

pub fn skills_pack_uninstall<TTransport>(
    transport: &TTransport,
    params: SkillsPackUninstallParams,
) -> Result<SkillsPackUninstallResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::SKILLS_PACK_UNINSTALL,
    )?;
    send_json_rpc_request_typed(
        transport,
        methods::SKILLS_PACK_UNINSTALL,
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

pub fn artifact_view_grant_create<TTransport>(
    transport: &TTransport,
    params: ArtifactViewGrantCreateParams,
) -> Result<ArtifactViewGrantCreateResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    require_non_empty_field(
        params.workspace_id.as_str(),
        "workspace_id",
        methods::ARTIFACT_VIEW_GRANT_CREATE,
    )?;
    require_non_empty_field(
        params.artifact_id.as_str(),
        "artifact_id",
        methods::ARTIFACT_VIEW_GRANT_CREATE,
    )?;
    require_non_empty_field(
        params.version_id.as_str(),
        "version_id",
        methods::ARTIFACT_VIEW_GRANT_CREATE,
    )?;
    send_json_rpc_request_typed(
        transport,
        methods::ARTIFACT_VIEW_GRANT_CREATE,
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
        SkillHealthTarget, SkillId, SkillLifecycleSource, SkillPackId, TaskCancelScope,
        ThreadVisibility, TurnCapability, VoiceAudioEncoding, VoiceSessionStartContext,
        VoiceTurnContext, Workspace,
    };
    use serde_json::json;
    use std::sync::Mutex;

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
                methods::PROVIDER_TRANSCRIPTION_MODELS_LIST => {
                    let params = request.params.as_ref().expect("request params");
                    assert_eq!(params["workspace_id"], json!("ws_1"));
                    assert_eq!(params["provider"], json!("local"));
                    json!({
                        "provider": "local",
                        "models": []
                    })
                }
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

    #[derive(Default)]
    struct RecordingFailureTransport {
        requests: Mutex<Vec<JsonRpcRequest>>,
    }

    impl JsonRpcRequestTransport for RecordingFailureTransport {
        fn send_json_rpc_request(
            &self,
            _request_id: String,
            payload: String,
            _response_tx: JsonRpcResponseSender,
        ) -> std::result::Result<(), String> {
            self.requests
                .lock()
                .expect("request lock")
                .push(serde_json::from_str(&payload).expect("request payload"));
            Err(WEBSOCKET_WORKER_UNAVAILABLE_MESSAGE.to_owned())
        }
    }

    fn params<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> T {
        serde_json::from_value(value).expect("valid typed params")
    }

    #[test]
    fn epic5_authenticated_wrappers_use_the_complete_normal_method_set() {
        let transport = RecordingFailureTransport::default();
        let workspace_id = "WAAAAAAAAAAAAAAAAAAAA";
        let principal_id = "PAAAAAAAAAAAAAAAAAAAA";
        let invitation_id = "IAAAAAAAAAAAAAAAAAAAA";

        let _ = invitation_create(&transport, params(json!({"workspace_ids": [workspace_id]})));
        let _ = invitation_list(&transport, InvitationListParams::default());
        let _ = invitation_revoke(&transport, params(json!({"invitation_id": invitation_id})));
        let _ = member_list(&transport, MemberListParams::default());
        let _ = member_suspend(&transport, params(json!({"principal_id": principal_id})));
        let _ = member_restore(&transport, params(json!({"principal_id": principal_id})));
        let _ = member_remove(&transport, params(json!({"principal_id": principal_id})));
        let _ = member_device_create(&transport, params(json!({"principal_id": principal_id})));
        let _ = workspace_member_list(&transport, params(json!({"workspace_id": workspace_id})));
        let _ = workspace_member_add(
            &transport,
            params(json!({
                "workspace_id": workspace_id,
                "principal_id": principal_id
            })),
        );
        let _ = workspace_member_remove(
            &transport,
            params(json!({
                "workspace_id": workspace_id,
                "principal_id": principal_id
            })),
        );

        let requests = transport.requests.lock().expect("request lock");
        assert_eq!(
            requests
                .iter()
                .map(|request| request.method.as_str())
                .collect::<Vec<_>>(),
            vec![
                methods::INVITE_CREATE,
                methods::INVITE_LIST,
                methods::INVITE_REVOKE,
                methods::MEMBER_LIST,
                methods::MEMBER_SUSPEND,
                methods::MEMBER_RESTORE,
                methods::MEMBER_REMOVE,
                methods::MEMBER_DEVICE_CREATE,
                methods::WORKSPACE_MEMBER_LIST,
                methods::WORKSPACE_MEMBER_ADD,
                methods::WORKSPACE_MEMBER_REMOVE,
            ]
        );
    }

    #[test]
    fn epic7_participant_wrappers_use_existing_methods_and_protocol_dtos() {
        let transport = RecordingFailureTransport::default();
        let workspace_id = "WAAAAAAAAAAAAAAAAAAAA";
        let thread_id = "TAAAAAAAAAAAAAAAAAAAA";
        let principal_id = "PAAAAAAAAAAAAAAAAAAAA";

        let _ = thread_participants_list(
            &transport,
            params(json!({
                "workspace_id": workspace_id,
                "thread_id": thread_id
            })),
        );
        let _ = thread_participant_add(
            &transport,
            params(json!({
                "workspace_id": workspace_id,
                "thread_id": thread_id,
                "principal_id": principal_id
            })),
        );
        let _ = thread_participant_remove(
            &transport,
            params(json!({
                "workspace_id": workspace_id,
                "thread_id": thread_id,
                "principal_id": principal_id
            })),
        );

        let requests = transport.requests.lock().expect("request lock");
        assert_eq!(
            requests
                .iter()
                .map(|request| request.method.as_str())
                .collect::<Vec<_>>(),
            vec![
                methods::THREAD_PARTICIPANTS_LIST,
                methods::THREAD_PARTICIPANTS_ADD,
                methods::THREAD_PARTICIPANTS_REMOVE,
            ]
        );
    }

    #[test]
    fn epic6_turn_centric_wrappers_serialize_existing_methods_and_ids() {
        let transport = RecordingFailureTransport::default();
        let thread_id = "TAAAAAAAAAAAAAAAAAAAA";
        let turn_id = "UAAAAAAAAAAAAAAAAAAAA";

        let _ = turn_start(
            &transport,
            params(json!({
                "thread_id": thread_id,
                "turn_id": turn_id,
                "input": [{"type": "text", "text": "hello", "textElements": []}],
                "mode": "Message"
            })),
        );
        let _ = turn_message_edit(
            &transport,
            params(json!({
                "thread_id": thread_id,
                "turn_id": turn_id,
                "expected_revision": 0,
                "input": [{"type": "text", "text": "edited", "textElements": []}]
            })),
        );
        let _ = turn_message_delete(
            &transport,
            params(json!({
                "thread_id": thread_id,
                "turn_id": turn_id,
                "expected_revision": 1
            })),
        );
        let _ = turn_message_revisions_page(
            &transport,
            params(json!({
                "thread_id": thread_id,
                "turn_id": turn_id,
                "limit": 20
            })),
        );
        let _ = thread_read(
            &transport,
            params(json!({
                "thread_id": thread_id,
                "through_turn_id": turn_id
            })),
        );

        let requests = transport.requests.lock().expect("request lock");
        assert_eq!(
            requests
                .iter()
                .map(|request| request.method.as_str())
                .collect::<Vec<_>>(),
            vec![
                methods::TURN_START,
                methods::TURN_MESSAGE_EDIT,
                methods::TURN_MESSAGE_DELETE,
                methods::TURN_MESSAGE_REVISIONS_PAGE,
                methods::THREAD_READ,
            ]
        );
        for request in requests.iter() {
            let payload = request.params.as_ref().expect("request params");
            assert_eq!(payload["thread_id"], json!(thread_id));
            assert_eq!(
                payload
                    .get("turn_id")
                    .or_else(|| payload.get("through_turn_id")),
                Some(&json!(turn_id))
            );
            assert!(payload.get("source_message_id").is_none());
            assert!(payload.get("bearer").is_none());
            assert!(payload.get("bytes").is_none());
        }
    }

    #[test]
    fn auth_rpc_errors_preserve_machine_codes_without_peer_controlled_messages() {
        let secret = "eyJhbGciOiJIUzI1NiJ9.peer-controlled.signature";
        let structured = sanitize_auth_rpc_error(anyhow::Error::new(
            crate::rpc::JsonRpcResponseError::server(
                Some(pioneer_protocol::AUTHENTICATION_TERMINAL_CODE),
                format!("malicious peer echoed {secret}"),
                Some("session_revoked".to_owned()),
            ),
        ));
        assert_eq!(format!("{structured:#}"), "session_revoked");

        let unknown_terminal = sanitize_auth_rpc_error(anyhow::Error::new(
            crate::rpc::JsonRpcResponseError::server(
                Some(pioneer_protocol::AUTHENTICATION_TERMINAL_CODE),
                format!("malicious peer echoed {secret}"),
                Some("future_terminal_reason".to_owned()),
            ),
        ));
        assert_eq!(format!("{unknown_terminal:#}"), "authentication_terminal");

        let sanitized =
            sanitize_auth_rpc_error(anyhow!("malicious peer echoed {secret} [session_revoked]"));
        let rendered = format!("{sanitized:#}");

        assert_eq!(rendered, "session_revoked");
        assert!(!rendered.contains(secret));

        let generic = format!(
            "{:#}",
            sanitize_auth_rpc_error(anyhow!("malicious peer echoed {secret}"))
        );
        assert_eq!(generic, "Gateway authentication request failed");
        assert!(!generic.contains(secret));
    }

    #[test]
    fn message_revision_conflict_is_a_typed_authoritative_refetch_signal() {
        let error = anyhow::Error::new(crate::rpc::JsonRpcResponseError::server(
            Some(pioneer_protocol::INVALID_REQUEST_CODE),
            "message revision conflict",
            Some("revision_conflict".to_owned()),
        ));

        assert_eq!(
            turn_message_error_reason(&error),
            Some(TurnMessageErrorReason::RevisionConflict)
        );
    }

    struct ExactSkillTurnTransport;

    impl JsonRpcRequestTransport for ExactSkillTurnTransport {
        fn send_json_rpc_request(
            &self,
            _request_id: String,
            payload: String,
            _response_tx: JsonRpcResponseSender,
        ) -> std::result::Result<(), String> {
            let request: JsonRpcRequest =
                serde_json::from_str(payload.as_str()).expect("request payload");
            let params = request.params.expect("turn params");
            let capabilities = match request.method.as_str() {
                methods::TURN_START => &params["capabilities"],
                methods::VOICE_SESSION_FINALIZE => &params["context"]["capabilities"],
                method => panic!("unexpected method {method}"),
            };
            assert_eq!(capabilities[0]["id"], json!("skill:TTTTTTTTTTTTTTTTTTTTT"));
            assert_eq!(
                capabilities[0]["kind"]["skillId"],
                json!("TTTTTTTTTTTTTTTTTTTTT")
            );
            Err(WEBSOCKET_WORKER_UNAVAILABLE_MESSAGE.to_owned())
        }
    }

    struct ExactSkillPackTransport;

    impl JsonRpcRequestTransport for ExactSkillPackTransport {
        fn send_json_rpc_request(
            &self,
            _request_id: String,
            payload: String,
            response_tx: JsonRpcResponseSender,
        ) -> std::result::Result<(), String> {
            let request: JsonRpcRequest =
                serde_json::from_str(payload.as_str()).expect("request payload");
            let params = request.params.expect("pack lifecycle params");
            assert_eq!(params["workspace_id"], json!("workspace"));
            let pack = json!({
                "id": "PPPPPPPPPPPPPPPPPPPPP",
                "name": "Research",
                "source_kind": "user",
                "created_at": 1,
                "updated_at": 2
            });
            let result = match request.method.as_str() {
                methods::SKILLS_PACK_INSTALL => {
                    assert_eq!(params["source"]["type"], json!("uploaded_archive"));
                    assert_eq!(params["source"]["upload_id"], json!("upload-install"));
                    assert_eq!(params["target_source_kind"], json!("user"));
                    json!({
                        "status": "installed",
                        "pack": pack,
                        "skills": [],
                        "audit": { "events_written": 0 }
                    })
                }
                methods::SKILLS_PACK_UPDATE => {
                    assert_eq!(params["pack_id"], json!("PPPPPPPPPPPPPPPPPPPPP"));
                    assert_eq!(params["source"]["type"], json!("uploaded_archive"));
                    assert_eq!(params["source"]["upload_id"], json!("upload-update"));
                    json!({
                        "status": "updated",
                        "pack": pack,
                        "skills": [],
                        "audit": { "events_written": 0 }
                    })
                }
                methods::SKILLS_PACK_UNINSTALL => {
                    assert_eq!(params["pack_id"], json!("PPPPPPPPPPPPPPPPPPPPP"));
                    json!({
                        "status": "uninstalled",
                        "pack": pack,
                        "removed_skills": [],
                        "audit": { "events_written": 0 }
                    })
                }
                method => panic!("unexpected method {method}"),
            };

            response_tx.send(Ok(result)).expect("response send");
            Ok(())
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

    fn voice_session_start_params() -> VoiceSessionStartParams {
        VoiceSessionStartParams {
            context: VoiceSessionStartContext {
                workspace_id: "ws_1".to_owned(),
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
            },
            audio_format: VoiceAudioFormat {
                sample_rate_hz: 16_000,
                channels: 1,
                encoding: VoiceAudioEncoding::PcmS16Le,
            },
        }
    }

    fn voice_turn_context() -> VoiceTurnContext {
        VoiceTurnContext {
            workspace_id: "ws_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            prepared_input: Vec::new(),
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: None,
            execution_backend: None,
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: None,
        }
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
    fn ws_command_sender_uses_exact_skill_pack_lifecycle_methods() {
        let pack_id = SkillPackId::new("P".repeat(21)).expect("pack id");
        let install = skills_pack_install(
            &ExactSkillPackTransport,
            SkillsPackInstallParams {
                workspace_id: "workspace".to_owned(),
                source: SkillLifecycleSource::UploadedArchive {
                    upload_id: "upload-install".to_owned(),
                },
                target_source_kind: "user".to_owned(),
            },
        )
        .expect("pack install");
        assert_eq!(install.status, "installed");

        let update = skills_pack_update(
            &ExactSkillPackTransport,
            SkillsPackUpdateParams {
                workspace_id: "workspace".to_owned(),
                pack_id: pack_id.clone(),
                source: SkillLifecycleSource::UploadedArchive {
                    upload_id: "upload-update".to_owned(),
                },
            },
        )
        .expect("pack update");
        assert_eq!(update.pack.id, pack_id);

        let uninstall = skills_pack_uninstall(
            &ExactSkillPackTransport,
            SkillsPackUninstallParams {
                workspace_id: "workspace".to_owned(),
                pack_id,
            },
        )
        .expect("pack uninstall");
        assert_eq!(uninstall.status, "uninstalled");
        assert!(uninstall.removed_skills.is_empty());
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
                        visibility: None,
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
                        visibility: None,
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
                        visibility: None,
                        archived: None,
                    },
                )
                .expect_err("thread update field should be required")
            ),
            "at least one field is required for thread/update"
        );
    }

    #[test]
    fn ws_command_sender_accepts_visibility_or_archive_as_thread_update_fields() {
        for params in [
            ThreadUpdateParams {
                workspace_id: "ws_1".to_owned(),
                thread_id: "thread_1".to_owned(),
                name: None,
                visibility: Some(ThreadVisibility::Private),
                archived: None,
            },
            ThreadUpdateParams {
                workspace_id: "ws_1".to_owned(),
                thread_id: "thread_1".to_owned(),
                name: None,
                visibility: None,
                archived: Some(true),
            },
        ] {
            let error = thread_update(&FakeTransport, params)
                .expect_err("fake transport has no thread/update response");
            assert_eq!(format!("{error:#}"), WEBSOCKET_WORKER_UNAVAILABLE_MESSAGE);
        }
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
                        thread_id: None,
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
                        reply_to_turn_id: None,
                        mentioned_principal_ids: Vec::new(),
                        execution_backend: None,
                        reasoning: None,
                        permission_profile: None,
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
                        reply_to_turn_id: None,
                        mentioned_principal_ids: Vec::new(),
                        execution_backend: None,
                        reasoning: None,
                        permission_profile: None,
                        cli_runtime_options: None,
                    },
                )
                .expect_err("turn id should be required")
            ),
            "turn_id is required for turn/start"
        );

        let skill_id = SkillId::new("T".repeat(21)).expect("valid skill id");
        let error = turn_start(
            &ExactSkillTurnTransport,
            TurnStartParams {
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
                input: Vec::new(),
                capabilities: vec![TurnCapability {
                    id: pioneer_protocol::skill_capability_key(&skill_id),
                    kind: pioneer_protocol::TurnCapabilityKind::Skill {
                        skill_id,
                        pack_id: None,
                    },
                    label: Some("owner/slug".to_owned()),
                }],
                model: None,
                model_provider: None,
                sandbox_policy: None,
                mode: None,
                reply_to_turn_id: None,
                mentioned_principal_ids: Vec::new(),
                execution_backend: None,
                reasoning: None,
                permission_profile: None,
                cli_runtime_options: None,
            },
        )
        .expect_err("test transport stops after payload inspection");
        assert_eq!(format!("{error:#}"), WEBSOCKET_WORKER_UNAVAILABLE_MESSAGE);
    }

    #[test]
    fn ws_command_sender_voice_validation_matches_contract() {
        assert_eq!(
            format!(
                "{:#}",
                voice_status(
                    &PanicTransport,
                    VoiceStatusParams {
                        workspace_id: Some(" ".to_owned()),
                    },
                )
                .expect_err("workspace id should be non-empty")
            ),
            "workspace_id must not be empty for voice/status"
        );

        let mut start = voice_session_start_params();
        start.context.thread_id = " ".to_owned();
        assert_eq!(
            format!(
                "{:#}",
                voice_session_start(&PanicTransport, start)
                    .expect_err("thread id should be required")
            ),
            "thread_id is required for voice/session/start"
        );

        let mut start = voice_session_start_params();
        start.audio_format.sample_rate_hz = 0;
        assert_eq!(
            format!(
                "{:#}",
                voice_session_start(&PanicTransport, start)
                    .expect_err("sample rate should match voice target")
            ),
            "voice audio sample_rate_hz must be 16000, got 0 for voice/session/start"
        );

        assert_eq!(
            format!(
                "{:#}",
                voice_session_finalize(
                    &PanicTransport,
                    VoiceSessionFinalizeParams {
                        session_id: " ".to_owned(),
                        context: voice_turn_context(),
                    },
                )
                .expect_err("session id should be required")
            ),
            "session_id is required for voice/session/finalize"
        );

        assert_eq!(
            format!(
                "{:#}",
                voice_session_cancel(
                    &PanicTransport,
                    VoiceSessionCancelParams {
                        session_id: "voice_session_1".to_owned(),
                        reason: Some(" ".to_owned()),
                    },
                )
                .expect_err("reason should be non-empty")
            ),
            "reason must not be empty for voice/session/cancel"
        );

        let skill_id = SkillId::new("T".repeat(21)).expect("valid skill id");
        let mut context = voice_turn_context();
        context.capabilities = vec![TurnCapability {
            id: pioneer_protocol::skill_capability_key(&skill_id),
            kind: pioneer_protocol::TurnCapabilityKind::Skill {
                skill_id,
                pack_id: None,
            },
            label: Some("owner/slug".to_owned()),
        }];
        let error = voice_session_finalize(
            &ExactSkillTurnTransport,
            VoiceSessionFinalizeParams {
                session_id: "voice_session_1".to_owned(),
                context,
            },
        )
        .expect_err("test transport stops after payload inspection");
        assert_eq!(format!("{error:#}"), WEBSOCKET_WORKER_UNAVAILABLE_MESSAGE);
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
    fn provider_list_transcription_models_uses_exact_method_and_payload() {
        let response = provider_list_transcription_models(
            &FakeTransport,
            ProviderListModelsParams {
                workspace_id: "ws_1".to_owned(),
                provider: "local".to_owned(),
            },
        )
        .expect("transcription model list");

        assert_eq!(response.provider, "local");
        assert!(response.models.is_empty());
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
                provider_list_embedding_models(
                    &PanicTransport,
                    ProviderListModelsParams {
                        workspace_id: "ws_1".to_owned(),
                        provider: " ".to_owned(),
                    },
                )
                .expect_err("provider should be required")
            ),
            "provider is required for provider/embedding_models/list"
        );
        assert_eq!(
            format!(
                "{:#}",
                provider_list_transcription_models(
                    &PanicTransport,
                    ProviderListModelsParams {
                        workspace_id: "ws_1".to_owned(),
                        provider: " ".to_owned(),
                    },
                )
                .expect_err("provider should be required")
            ),
            "provider is required for provider/transcription_models/list"
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
        let skill_id = SkillId::new("S".repeat(21)).expect("valid skill id");
        assert_eq!(
            format!(
                "{:#}",
                skills_update(
                    &PanicTransport,
                    SkillsUpdateParams {
                        workspace_id: "".to_owned(),
                        skill_id: skill_id.clone(),
                        source: SkillLifecycleSource::UploadedArchive {
                            upload_id: "upload_1".to_owned(),
                        },
                        expected_previous_fingerprint: None,
                    },
                )
                .expect_err("workspace should be required")
            ),
            "workspace_id is required for skills/update"
        );
        assert_eq!(
            format!(
                "{:#}",
                skills_health(
                    &PanicTransport,
                    SkillsHealthParams {
                        workspace_id: "".to_owned(),
                        skills: vec![SkillHealthTarget { skill_id }],
                        audit_limit: 16,
                    },
                )
                .expect_err("workspace should be required")
            ),
            "workspace_id is required for skills/health"
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
