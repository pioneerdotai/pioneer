use crate::constants::events;
use crate::{
    AccessChangedNotification, ArtifactCreatedNotification, ArtifactDeletedNotification,
    ArtifactProjectionUpdatedNotification, ArtifactUpdatedNotification,
    ArtifactUploadProgressNotification, AuthAccessExpiringNotification,
    AuthSessionRevokedNotification, AuthorizationProjectionChangedNotification,
    CLIRuntimeAccountUpdatedNotification, CLIRuntimeAppsChangedNotification,
    CLIRuntimeRequestOpenedNotification, CLIRuntimeRequestResolvedNotification,
    CLIRuntimeStatusChangedNotification, ContextCompressedNotification,
    ContextCompressingNotification, GatewayRemoteAccessStatusChangedNotification,
    GatewayThreadEpisodicVectorRefillStatusChangedNotification,
    GatewayVoiceInputStatusChangedNotification, InvitationChangedNotification,
    ItemCompletedNotification, ItemDeltaNotification, ItemDeltaStream,
    ItemRecoveryAttachedNotification, ItemRecoveryExhaustedNotification,
    ItemRecoveryOpenedNotification, ItemRecoverySucceededNotification,
    ItemRetryAttemptStartedNotification, ItemRetryScheduledNotification, ItemStartedNotification,
    ItemTimeoutDetectedNotification, ItemToolRetryExhaustedNotification,
    ItemToolRetryResolvedNotification, ItemToolRetryScheduledNotification, ItemUpdatedNotification,
    JsonRpcNotification, McpChangedNotification, McpServerCatalogChangedNotification,
    McpServerStatusChangedNotification, MemberChangedNotification,
    MemoryCandidateCreatedNotification, MemoryChangedNotification, MemoryForgottenNotification,
    SkillsChangedNotification, SkillsUploadChunkAckNotification, TaskCancelledNotification,
    TaskCompletedNotification, TaskCreatedNotification, TaskDeliveryCancelledNotification,
    TaskDeliveryDeliveredNotification, TaskDeliveryFailedNotification,
    TaskDeliveryQueuedNotification, TaskDeliveryStartedNotification, TaskDetachedNotification,
    TaskFailedNotification, TaskPausedNotification, TaskProgressNotification,
    TaskQueuedNotification, TaskRecoveredNotification, TaskRescheduledNotification,
    TaskResumedNotification, TaskRunCompletedNotification, TaskRunCreatedNotification,
    TaskRunFailedNotification, TaskRunStartedNotification, TaskScheduledNotification,
    TaskTreeChangedNotification as TaskTreeChangedTaskNotification, TaskUpdatedNotification,
    TaskUserNotificationDeliveredNotification, ThreadAgentsDocChangedNotification,
    ThreadArtifactsChangedNotification, ThreadClosedNotification,
    ThreadParticipantsChangedNotification, ThreadReadCursorChangedNotification,
    ThreadStartedNotification, ThreadTimelineBlocksChangedNotification,
    ThreadTreeChangedNotification, ThreadUpdatedNotification, TurnBlockedNotification,
    TurnCompletedNotification, TurnExecutionWindowBlockedNotification,
    TurnExecutionWindowCheckpointedNotification, TurnExecutionWindowContinuedNotification,
    TurnExecutionWindowExhaustedNotification, TurnExecutionWindowStartedNotification,
    TurnFailedNotification, TurnPermissionRequestOpenedNotification,
    TurnPermissionRequestResolvedNotification, TurnStartedNotification,
    TurnToolLoopBudgetExceededNotification, TurnWorkItemsChangedNotification,
    TurnWorkStateChangedNotification, VoiceSessionResultNotification, WorkspaceChangedNotification,
    WorkspaceMembersChangedNotification,
};
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct UnknownGatewayNotification {
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    pub params: JsonValue,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(tag = "kind", content = "params", rename_all = "snake_case")]
pub enum GatewayNotification {
    AccessChanged(AccessChangedNotification),
    AuthorizationProjectionChanged(AuthorizationProjectionChangedNotification),
    AuthSessionRevoked(AuthSessionRevokedNotification),
    AuthAccessExpiring(AuthAccessExpiringNotification),
    InvitationChanged(InvitationChangedNotification),
    MemberChanged(MemberChangedNotification),
    WorkspaceMembersChanged(WorkspaceMembersChangedNotification),
    WorkspaceChanged(WorkspaceChangedNotification),
    ThreadStarted(ThreadStartedNotification),
    ThreadClosed(ThreadClosedNotification),
    ThreadUpdated(ThreadUpdatedNotification),
    ThreadParticipantsChanged(ThreadParticipantsChangedNotification),
    ThreadTreeChanged(ThreadTreeChangedNotification),
    ThreadAgentsDocChanged(ThreadAgentsDocChangedNotification),
    ThreadTimelineBlocksChanged(ThreadTimelineBlocksChangedNotification),
    ThreadReadCursorChanged(ThreadReadCursorChangedNotification),
    TurnStarted(TurnStartedNotification),
    TurnCompleted(TurnCompletedNotification),
    TurnFailed(TurnFailedNotification),
    TurnBlocked(TurnBlockedNotification),
    TurnWorkItemsChanged(TurnWorkItemsChangedNotification),
    TurnWorkStateChanged(TurnWorkStateChangedNotification),
    TurnPermissionRequestOpened(TurnPermissionRequestOpenedNotification),
    TurnPermissionRequestResolved(TurnPermissionRequestResolvedNotification),
    TurnExecutionWindowStarted(TurnExecutionWindowStartedNotification),
    TurnExecutionWindowExhausted(TurnExecutionWindowExhaustedNotification),
    TurnExecutionWindowCheckpointed(TurnExecutionWindowCheckpointedNotification),
    TurnExecutionWindowContinued(TurnExecutionWindowContinuedNotification),
    TurnExecutionWindowBlocked(TurnExecutionWindowBlockedNotification),
    ItemStarted(ItemStartedNotification),
    ItemDelta(ItemDeltaNotification),
    ItemTimeoutDetected(ItemTimeoutDetectedNotification),
    ItemRecoveryOpened(ItemRecoveryOpenedNotification),
    ItemRecoveryAttached(ItemRecoveryAttachedNotification),
    ItemRetryScheduled(ItemRetryScheduledNotification),
    ItemRetryAttemptStarted(ItemRetryAttemptStartedNotification),
    ItemRecoverySucceeded(ItemRecoverySucceededNotification),
    ItemRecoveryExhausted(ItemRecoveryExhaustedNotification),
    ItemToolRetryScheduled(ItemToolRetryScheduledNotification),
    ItemToolRetryResolved(ItemToolRetryResolvedNotification),
    ItemToolRetryExhausted(ItemToolRetryExhaustedNotification),
    ItemCompleted(ItemCompletedNotification),
    ItemUpdated(ItemUpdatedNotification),
    TurnToolLoopBudgetExceeded(TurnToolLoopBudgetExceededNotification),
    ContextCompressing(ContextCompressingNotification),
    ContextCompressed(ContextCompressedNotification),
    SkillsChanged(SkillsChangedNotification),
    SkillsUploadChunkAck(SkillsUploadChunkAckNotification),
    McpChanged(McpChangedNotification),
    McpServerStatusChanged(McpServerStatusChangedNotification),
    McpServerCatalogChanged(McpServerCatalogChangedNotification),
    ArtifactCreated(ArtifactCreatedNotification),
    ArtifactUpdated(ArtifactUpdatedNotification),
    ArtifactDeleted(ArtifactDeletedNotification),
    ThreadArtifactsChanged(ThreadArtifactsChangedNotification),
    ArtifactProjectionUpdated(ArtifactProjectionUpdatedNotification),
    ArtifactUploadProgress(ArtifactUploadProgressNotification),
    TaskCreated(TaskCreatedNotification),
    TaskScheduled(TaskScheduledNotification),
    TaskQueued(TaskQueuedNotification),
    TaskRunCreated(TaskRunCreatedNotification),
    TaskRunStarted(TaskRunStartedNotification),
    TaskProgress(TaskProgressNotification),
    TaskRunCompleted(TaskRunCompletedNotification),
    TaskRunFailed(TaskRunFailedNotification),
    TaskRunBlocked(TaskRunFailedNotification),
    TaskRunCancelled(TaskRunFailedNotification),
    TaskCompleted(TaskCompletedNotification),
    TaskFailed(TaskFailedNotification),
    TaskBlocked(TaskFailedNotification),
    TaskCancelled(TaskCancelledNotification),
    TaskDetached(TaskDetachedNotification),
    TaskUpdated(TaskUpdatedNotification),
    TaskRescheduled(TaskRescheduledNotification),
    TaskPaused(TaskPausedNotification),
    TaskResumed(TaskResumedNotification),
    TaskDeliveryQueued(TaskDeliveryQueuedNotification),
    TaskDeliveryStarted(TaskDeliveryStartedNotification),
    TaskDeliveryDelivered(TaskDeliveryDeliveredNotification),
    TaskDeliveryFailed(TaskDeliveryFailedNotification),
    TaskDeliveryCancelled(TaskDeliveryCancelledNotification),
    TaskUserNotificationDelivered(TaskUserNotificationDeliveredNotification),
    TaskTreeChanged(TaskTreeChangedTaskNotification),
    TaskRecovered(TaskRecoveredNotification),
    MemoryChanged(MemoryChangedNotification),
    MemoryCandidateCreated(MemoryCandidateCreatedNotification),
    MemoryForgotten(MemoryForgottenNotification),
    #[serde(rename = "cli_runtime_status_changed")]
    CLIRuntimeStatusChanged(CLIRuntimeStatusChangedNotification),
    #[serde(rename = "cli_runtime_account_updated")]
    CLIRuntimeAccountUpdated(CLIRuntimeAccountUpdatedNotification),
    #[serde(rename = "cli_runtime_request_opened")]
    CLIRuntimeRequestOpened(CLIRuntimeRequestOpenedNotification),
    #[serde(rename = "cli_runtime_request_resolved")]
    CLIRuntimeRequestResolved(CLIRuntimeRequestResolvedNotification),
    #[serde(rename = "cli_runtime_apps_changed")]
    CLIRuntimeAppsChanged(CLIRuntimeAppsChangedNotification),
    #[serde(rename = "gateway_remote_access_status_changed")]
    GatewayRemoteAccessStatusChanged(GatewayRemoteAccessStatusChangedNotification),
    #[serde(rename = "gateway_thread_episodic_vector_refill_status_changed")]
    GatewayThreadEpisodicVectorRefillStatusChanged(
        GatewayThreadEpisodicVectorRefillStatusChangedNotification,
    ),
    #[serde(rename = "gateway_voice_input_status_changed")]
    GatewayVoiceInputStatusChanged(GatewayVoiceInputStatusChangedNotification),
    VoiceSessionResult(VoiceSessionResultNotification),
    Unknown(UnknownGatewayNotification),
}

impl GatewayNotification {
    pub fn from_jsonrpc(notification: JsonRpcNotification) -> Option<Self> {
        let method = notification.method;

        let params = notification.params?;

        match method.as_str() {
            events::ACCESS_CHANGED => {
                match serde_json::from_value::<AccessChangedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::AccessChanged(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::AUTHORIZATION_PROJECTION_CHANGED => {
                match serde_json::from_value::<AuthorizationProjectionChangedNotification>(
                    params.clone(),
                ) {
                    Ok(notification) => Some(Self::AuthorizationProjectionChanged(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::AUTH_SESSION_REVOKED => {
                match serde_json::from_value::<AuthSessionRevokedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::AuthSessionRevoked(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::AUTH_ACCESS_EXPIRING => {
                match serde_json::from_value::<AuthAccessExpiringNotification>(params.clone()) {
                    Ok(notification) => Some(Self::AuthAccessExpiring(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::INVITATION_CHANGED => {
                match serde_json::from_value::<InvitationChangedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::InvitationChanged(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::MEMBER_CHANGED => {
                match serde_json::from_value::<MemberChangedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::MemberChanged(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::WORKSPACE_MEMBERS_CHANGED => {
                match serde_json::from_value::<WorkspaceMembersChangedNotification>(params.clone())
                {
                    Ok(notification) => Some(Self::WorkspaceMembersChanged(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::WORKSPACE_CHANGED => {
                serde_json::from_value::<WorkspaceChangedNotification>(params)
                    .ok()
                    .map(Self::WorkspaceChanged)
            }
            events::THREAD_STARTED => serde_json::from_value::<ThreadStartedNotification>(params)
                .ok()
                .map(Self::ThreadStarted),
            events::THREAD_CLOSED => serde_json::from_value::<ThreadClosedNotification>(params)
                .ok()
                .map(Self::ThreadClosed),
            events::THREAD_UPDATED => serde_json::from_value::<ThreadUpdatedNotification>(params)
                .ok()
                .map(Self::ThreadUpdated),
            events::THREAD_PARTICIPANTS_CHANGED => {
                match serde_json::from_value::<ThreadParticipantsChangedNotification>(
                    params.clone(),
                ) {
                    Ok(notification) => Some(Self::ThreadParticipantsChanged(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::THREAD_TREE_CHANGED => {
                serde_json::from_value::<ThreadTreeChangedNotification>(params)
                    .ok()
                    .map(Self::ThreadTreeChanged)
            }
            events::THREAD_AGENTS_DOC_CHANGED => {
                serde_json::from_value::<ThreadAgentsDocChangedNotification>(params)
                    .ok()
                    .map(Self::ThreadAgentsDocChanged)
            }
            events::THREAD_TIMELINE_BLOCKS_CHANGED => {
                serde_json::from_value::<ThreadTimelineBlocksChangedNotification>(params)
                    .ok()
                    .map(Self::ThreadTimelineBlocksChanged)
            }
            events::THREAD_READ_CURSOR_CHANGED => {
                serde_json::from_value::<ThreadReadCursorChangedNotification>(params)
                    .ok()
                    .map(Self::ThreadReadCursorChanged)
            }
            events::TURN_STARTED => serde_json::from_value::<TurnStartedNotification>(params)
                .ok()
                .map(Self::TurnStarted),
            events::TURN_COMPLETED => serde_json::from_value::<TurnCompletedNotification>(params)
                .ok()
                .map(Self::TurnCompleted),
            events::TURN_FAILED => serde_json::from_value::<TurnFailedNotification>(params)
                .ok()
                .map(Self::TurnFailed),
            events::TURN_BLOCKED => serde_json::from_value::<TurnBlockedNotification>(params)
                .ok()
                .map(Self::TurnBlocked),
            events::TURN_WORK_ITEMS_CHANGED => {
                serde_json::from_value::<TurnWorkItemsChangedNotification>(params)
                    .ok()
                    .map(Self::TurnWorkItemsChanged)
            }
            events::TURN_WORK_STATE_CHANGED => {
                serde_json::from_value::<TurnWorkStateChangedNotification>(params)
                    .ok()
                    .map(Self::TurnWorkStateChanged)
            }
            events::TURN_PERMISSION_REQUEST_OPENED => {
                match serde_json::from_value::<TurnPermissionRequestOpenedNotification>(
                    params.clone(),
                ) {
                    Ok(notification) => Some(Self::TurnPermissionRequestOpened(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::TURN_PERMISSION_REQUEST_RESOLVED => {
                match serde_json::from_value::<TurnPermissionRequestResolvedNotification>(
                    params.clone(),
                ) {
                    Ok(notification) => Some(Self::TurnPermissionRequestResolved(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::TURN_EXECUTION_WINDOW_STARTED => {
                parse_execution_window_notification::<TurnExecutionWindowStartedNotification>(
                    method,
                    params,
                    Self::TurnExecutionWindowStarted,
                )
            }
            events::TURN_EXECUTION_WINDOW_EXHAUSTED => {
                parse_execution_window_notification::<TurnExecutionWindowExhaustedNotification>(
                    method,
                    params,
                    Self::TurnExecutionWindowExhausted,
                )
            }
            events::TURN_EXECUTION_WINDOW_CHECKPOINTED => {
                parse_execution_window_notification::<TurnExecutionWindowCheckpointedNotification>(
                    method,
                    params,
                    Self::TurnExecutionWindowCheckpointed,
                )
            }
            events::TURN_EXECUTION_WINDOW_CONTINUED => {
                parse_execution_window_notification::<TurnExecutionWindowContinuedNotification>(
                    method,
                    params,
                    Self::TurnExecutionWindowContinued,
                )
            }
            events::TURN_EXECUTION_WINDOW_BLOCKED => {
                parse_execution_window_notification::<TurnExecutionWindowBlockedNotification>(
                    method,
                    params,
                    Self::TurnExecutionWindowBlocked,
                )
            }
            events::ITEM_STARTED => {
                match serde_json::from_value::<ItemStartedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemStarted(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_AGENT_MESSAGE_DELTA => {
                parse_item_delta_notification(params.clone(), ItemDeltaStream::AgentMessage, method)
            }
            events::ITEM_COMMAND_EXECUTION_OUTPUT_DELTA => {
                parse_item_delta_notification(params.clone(), ItemDeltaStream::Stdout, method)
            }
            events::ITEM_FILE_CHANGE_OUTPUT_DELTA => {
                parse_item_delta_notification(params.clone(), ItemDeltaStream::FileChange, method)
            }
            events::ITEM_TOOL_PROGRESS => {
                parse_item_delta_notification(params.clone(), ItemDeltaStream::ToolProgress, method)
            }
            events::ITEM_COMPLETED => {
                match serde_json::from_value::<ItemCompletedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemCompleted(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_UPDATED => {
                match serde_json::from_value::<ItemUpdatedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemUpdated(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_TIMEOUT_DETECTED => {
                match serde_json::from_value::<ItemTimeoutDetectedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemTimeoutDetected(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_RECOVERY_OPENED => {
                match serde_json::from_value::<ItemRecoveryOpenedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemRecoveryOpened(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_RECOVERY_ATTACHED => {
                match serde_json::from_value::<ItemRecoveryAttachedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemRecoveryAttached(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_RETRY_SCHEDULED => {
                match serde_json::from_value::<ItemRetryScheduledNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemRetryScheduled(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_RETRY_ATTEMPT_STARTED => {
                match serde_json::from_value::<ItemRetryAttemptStartedNotification>(params.clone())
                {
                    Ok(notification) => Some(Self::ItemRetryAttemptStarted(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_RECOVERY_SUCCEEDED => {
                match serde_json::from_value::<ItemRecoverySucceededNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemRecoverySucceeded(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_RECOVERY_EXHAUSTED => {
                match serde_json::from_value::<ItemRecoveryExhaustedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemRecoveryExhausted(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_TOOL_RETRY_SCHEDULED => {
                match serde_json::from_value::<ItemToolRetryScheduledNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemToolRetryScheduled(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_TOOL_RETRY_RESOLVED => {
                match serde_json::from_value::<ItemToolRetryResolvedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemToolRetryResolved(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_TOOL_RETRY_EXHAUSTED => {
                match serde_json::from_value::<ItemToolRetryExhaustedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemToolRetryExhausted(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::TURN_TOOL_LOOP_BUDGET_EXCEEDED => {
                match serde_json::from_value::<TurnToolLoopBudgetExceededNotification>(
                    params.clone(),
                ) {
                    Ok(notification) => Some(Self::TurnToolLoopBudgetExceeded(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::CONTEXT_COMPRESSING => {
                serde_json::from_value::<ContextCompressingNotification>(params)
                    .ok()
                    .map(Self::ContextCompressing)
            }
            events::CONTEXT_COMPRESSED => {
                serde_json::from_value::<ContextCompressedNotification>(params)
                    .ok()
                    .map(Self::ContextCompressed)
            }
            events::SKILLS_CHANGED => serde_json::from_value::<SkillsChangedNotification>(params)
                .ok()
                .map(Self::SkillsChanged),
            events::SKILLS_UPLOAD_CHUNK_ACK => {
                serde_json::from_value::<SkillsUploadChunkAckNotification>(params)
                    .ok()
                    .map(Self::SkillsUploadChunkAck)
            }
            events::MCP_CHANGED => serde_json::from_value::<McpChangedNotification>(params)
                .ok()
                .map(Self::McpChanged),
            events::MCP_SERVER_STATUS_CHANGED => {
                serde_json::from_value::<McpServerStatusChangedNotification>(params)
                    .ok()
                    .map(Self::McpServerStatusChanged)
            }
            events::MCP_SERVER_CATALOG_CHANGED => {
                serde_json::from_value::<McpServerCatalogChangedNotification>(params)
                    .ok()
                    .map(Self::McpServerCatalogChanged)
            }
            events::CLI_RUNTIME_STATUS_CHANGED => parse_cli_runtime_notification::<
                CLIRuntimeStatusChangedNotification,
            >(
                method, params, Self::CLIRuntimeStatusChanged
            ),
            events::CLI_RUNTIME_ACCOUNT_UPDATED => parse_cli_runtime_notification::<
                CLIRuntimeAccountUpdatedNotification,
            >(
                method, params, Self::CLIRuntimeAccountUpdated
            ),
            events::CLI_RUNTIME_REQUEST_OPENED => parse_cli_runtime_notification::<
                CLIRuntimeRequestOpenedNotification,
            >(
                method, params, Self::CLIRuntimeRequestOpened
            ),
            events::CLI_RUNTIME_REQUEST_RESOLVED => {
                parse_cli_runtime_notification::<CLIRuntimeRequestResolvedNotification>(
                    method,
                    params,
                    Self::CLIRuntimeRequestResolved,
                )
            }
            events::CLI_RUNTIME_APPS_CHANGED => parse_cli_runtime_notification::<
                CLIRuntimeAppsChangedNotification,
            >(
                method, params, Self::CLIRuntimeAppsChanged
            ),
            events::GATEWAY_REMOTE_ACCESS_STATUS_CHANGED => {
                match serde_json::from_value::<GatewayRemoteAccessStatusChangedNotification>(
                    params.clone(),
                ) {
                    Ok(notification) => Some(Self::GatewayRemoteAccessStatusChanged(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::GATEWAY_THREAD_EPISODIC_VECTOR_REFILL_STATUS_CHANGED => {
                match serde_json::from_value::<
                    GatewayThreadEpisodicVectorRefillStatusChangedNotification,
                >(params.clone())
                {
                    Ok(notification) => Some(Self::GatewayThreadEpisodicVectorRefillStatusChanged(
                        notification,
                    )),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::GATEWAY_VOICE_INPUT_STATUS_CHANGED => {
                match serde_json::from_value::<GatewayVoiceInputStatusChangedNotification>(
                    params.clone(),
                ) {
                    Ok(notification) => Some(Self::GatewayVoiceInputStatusChanged(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::VOICE_SESSION_RESULT => {
                match serde_json::from_value::<VoiceSessionResultNotification>(params.clone()) {
                    Ok(notification) => Some(Self::VoiceSessionResult(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ARTIFACT_CREATED => {
                serde_json::from_value::<ArtifactCreatedNotification>(params)
                    .ok()
                    .map(Self::ArtifactCreated)
            }
            events::ARTIFACT_UPDATED => {
                serde_json::from_value::<ArtifactUpdatedNotification>(params)
                    .ok()
                    .map(Self::ArtifactUpdated)
            }
            events::ARTIFACT_DELETED => {
                serde_json::from_value::<ArtifactDeletedNotification>(params)
                    .ok()
                    .map(Self::ArtifactDeleted)
            }
            events::THREAD_ARTIFACTS_CHANGED => {
                serde_json::from_value::<ThreadArtifactsChangedNotification>(params)
                    .ok()
                    .map(Self::ThreadArtifactsChanged)
            }
            events::ARTIFACT_PROJECTION_UPDATED => {
                serde_json::from_value::<ArtifactProjectionUpdatedNotification>(params)
                    .ok()
                    .map(Self::ArtifactProjectionUpdated)
            }
            events::ARTIFACT_UPLOAD_PROGRESS => {
                serde_json::from_value::<ArtifactUploadProgressNotification>(params)
                    .ok()
                    .map(Self::ArtifactUploadProgress)
            }
            events::TASK_CREATED => serde_json::from_value::<TaskCreatedNotification>(params)
                .ok()
                .map(Self::TaskCreated),
            events::TASK_SCHEDULED => serde_json::from_value::<TaskScheduledNotification>(params)
                .ok()
                .map(Self::TaskScheduled),
            events::TASK_QUEUED => serde_json::from_value::<TaskQueuedNotification>(params)
                .ok()
                .map(Self::TaskQueued),
            events::TASK_RUN_CREATED => {
                serde_json::from_value::<TaskRunCreatedNotification>(params)
                    .ok()
                    .map(Self::TaskRunCreated)
            }
            events::TASK_RUN_STARTED => {
                serde_json::from_value::<TaskRunStartedNotification>(params)
                    .ok()
                    .map(Self::TaskRunStarted)
            }
            events::TASK_PROGRESS => serde_json::from_value::<TaskProgressNotification>(params)
                .ok()
                .map(Self::TaskProgress),
            events::TASK_RUN_COMPLETED => {
                serde_json::from_value::<TaskRunCompletedNotification>(params)
                    .ok()
                    .map(Self::TaskRunCompleted)
            }
            events::TASK_RUN_FAILED => serde_json::from_value::<TaskRunFailedNotification>(params)
                .ok()
                .map(Self::TaskRunFailed),
            events::TASK_RUN_BLOCKED => serde_json::from_value::<TaskRunFailedNotification>(params)
                .ok()
                .map(Self::TaskRunBlocked),
            events::TASK_RUN_CANCELLED => {
                serde_json::from_value::<TaskRunFailedNotification>(params)
                    .ok()
                    .map(Self::TaskRunCancelled)
            }
            events::TASK_COMPLETED => serde_json::from_value::<TaskCompletedNotification>(params)
                .ok()
                .map(Self::TaskCompleted),
            events::TASK_FAILED => serde_json::from_value::<TaskFailedNotification>(params)
                .ok()
                .map(Self::TaskFailed),
            events::TASK_BLOCKED => serde_json::from_value::<TaskFailedNotification>(params)
                .ok()
                .map(Self::TaskBlocked),
            events::TASK_CANCELLED => serde_json::from_value::<TaskCancelledNotification>(params)
                .ok()
                .map(Self::TaskCancelled),
            events::TASK_DETACHED => serde_json::from_value::<TaskDetachedNotification>(params)
                .ok()
                .map(Self::TaskDetached),
            events::TASK_UPDATED => serde_json::from_value::<TaskUpdatedNotification>(params)
                .ok()
                .map(Self::TaskUpdated),
            events::TASK_RESCHEDULED => {
                serde_json::from_value::<TaskRescheduledNotification>(params)
                    .ok()
                    .map(Self::TaskRescheduled)
            }
            events::TASK_PAUSED => serde_json::from_value::<TaskPausedNotification>(params)
                .ok()
                .map(Self::TaskPaused),
            events::TASK_RESUMED => serde_json::from_value::<TaskResumedNotification>(params)
                .ok()
                .map(Self::TaskResumed),
            events::TASK_DELIVERY_QUEUED => {
                serde_json::from_value::<TaskDeliveryQueuedNotification>(params)
                    .ok()
                    .map(Self::TaskDeliveryQueued)
            }
            events::TASK_DELIVERY_STARTED => {
                serde_json::from_value::<TaskDeliveryStartedNotification>(params)
                    .ok()
                    .map(Self::TaskDeliveryStarted)
            }
            events::TASK_DELIVERY_DELIVERED => {
                serde_json::from_value::<TaskDeliveryDeliveredNotification>(params)
                    .ok()
                    .map(Self::TaskDeliveryDelivered)
            }
            events::TASK_DELIVERY_FAILED => {
                serde_json::from_value::<TaskDeliveryFailedNotification>(params)
                    .ok()
                    .map(Self::TaskDeliveryFailed)
            }
            events::TASK_DELIVERY_CANCELLED => {
                serde_json::from_value::<TaskDeliveryCancelledNotification>(params)
                    .ok()
                    .map(Self::TaskDeliveryCancelled)
            }
            events::TASK_USER_NOTIFICATION_DELIVERED => {
                serde_json::from_value::<TaskUserNotificationDeliveredNotification>(params)
                    .ok()
                    .map(Self::TaskUserNotificationDelivered)
            }
            events::TASK_TREE_CHANGED => {
                serde_json::from_value::<TaskTreeChangedTaskNotification>(params)
                    .ok()
                    .map(Self::TaskTreeChanged)
            }
            events::TASK_RECOVERED => serde_json::from_value::<TaskRecoveredNotification>(params)
                .ok()
                .map(Self::TaskRecovered),
            events::MEMORY_CHANGED => {
                match serde_json::from_value::<MemoryChangedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::MemoryChanged(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::MEMORY_CANDIDATE_CREATED => {
                match serde_json::from_value::<MemoryCandidateCreatedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::MemoryCandidateCreated(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::MEMORY_FORGOTTEN => {
                match serde_json::from_value::<MemoryForgottenNotification>(params.clone()) {
                    Ok(notification) => Some(Self::MemoryForgotten(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            _ if method.starts_with("item/")
                || method.starts_with("turn/")
                || method.starts_with("context/")
                || method.starts_with("task/")
                || method.starts_with("memory/")
                || method.starts_with("workspace/")
                || method.starts_with("gateway/")
                || method.starts_with("artifact/")
                || method.starts_with("cli_runtime/")
                || method.starts_with("voice/")
                || method.starts_with("auth/")
                || method.starts_with("access/")
                || method.starts_with("thread/artifacts_") =>
            {
                Some(Self::Unknown(unknown_notification(method, params)))
            }
            _ => None,
        }
    }
}

fn parse_item_delta_notification(
    params: JsonValue,
    default_stream: ItemDeltaStream,
    method: String,
) -> Option<GatewayNotification> {
    match serde_json::from_value::<ItemDeltaNotification>(params.clone()) {
        Ok(mut notification) => {
            if notification.stream.is_none() {
                notification.stream = Some(default_stream);
            }
            Some(GatewayNotification::ItemDelta(notification))
        }
        Err(_) => Some(GatewayNotification::Unknown(unknown_notification(
            method, params,
        ))),
    }
}

fn unknown_notification(method: String, params: JsonValue) -> UnknownGatewayNotification {
    let (workspace_id, thread_id, turn_id, item_id) =
        extract_workspace_thread_turn_item(params.as_object());

    UnknownGatewayNotification {
        method,
        workspace_id,
        thread_id,
        turn_id,
        item_id,
        params,
    }
}

fn parse_cli_runtime_notification<T>(
    method: String,
    params: JsonValue,
    wrap: impl FnOnce(T) -> GatewayNotification,
) -> Option<GatewayNotification>
where
    T: DeserializeOwned,
{
    match serde_json::from_value::<T>(params.clone()) {
        Ok(notification) => Some(wrap(notification)),
        Err(_) => Some(GatewayNotification::Unknown(unknown_notification(
            method, params,
        ))),
    }
}

fn parse_execution_window_notification<T>(
    method: String,
    params: JsonValue,
    wrap: impl FnOnce(T) -> GatewayNotification,
) -> Option<GatewayNotification>
where
    T: DeserializeOwned,
{
    match serde_json::from_value::<T>(params.clone()) {
        Ok(notification) => Some(wrap(notification)),
        Err(_) => Some(GatewayNotification::Unknown(unknown_notification(
            method, params,
        ))),
    }
}

fn extract_workspace_thread_turn_item(
    object: Option<&serde_json::Map<String, JsonValue>>,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let Some(object) = object else {
        return (None, None, None, None);
    };

    let workspace_id = object
        .get("workspace_id")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let thread_id = object
        .get("thread_id")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let turn_id = object
        .get("turn_id")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let item_id = object
        .get("item_id")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);

    (workspace_id, thread_id, turn_id, item_id)
}

#[cfg(test)]
mod tests {
    use super::GatewayNotification;
    use crate::constants::events;
    use crate::{
        AccessChangeKind, ExecutionWindowExhaustionReason, ExecutionWindowStatus, ItemDeltaStream,
        JsonRpcNotification, MemoryCandidateCreatedNotification, MemoryChangedNotification,
        MemoryForgottenNotification, RecoveryAction, RecoveryJobStatus, RecoveryTrigger,
        ToolLoopBudgetAction, ToolLoopBudgetLimitKind, ToolRetryErrorClass,
        ToolRetryExhaustionKind, ToolRetryResolution, TurnItemType, WorkspaceChangeKind,
    };
    use serde_json::json;

    fn cli_runtime_summary_json() -> serde_json::Value {
        json!({
            "runtime_id": "codex_personal",
            "kind": "codex",
            "display_name": "Codex Personal",
            "enabled": true,
            "status": { "state": "ready" },
            "capabilities": {
                "supports_threads": true,
                "supports_resume": true,
                "supports_fork": true,
                "supports_steer": true,
                "supports_interrupt": true,
                "supports_approvals": true,
                "supports_file_change_approvals": true,
                "supports_command_approvals": true,
                "supports_user_input_requests": true,
                "supports_model_list": true,
                "supports_apps": false,
                "supports_review": true,
                "supports_compaction": true,
                "supports_goal": true,
                "supports_diff_updates": true,
                "supports_history_read": true,
                "supports_thread_archive": true,
                "supports_auth_management": true,
                "supports_generated_schema_probe": true
            }
        })
    }

    #[test]
    fn maps_known_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "turn/started",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn": {
                    "id": "turn_123",
                    "status": "InProgress",
                    "permission_profile": {
                        "mode": "full_access",
                        "source": "defaulted",
                        "effective_policy": {
                            "default_behavior": "allow",
                            "file_read": "allow",
                            "file_write": "allow",
                            "shell_command": "allow",
                            "network": "allow",
                            "mcp_read": "allow",
                            "mcp_write_or_unknown": "allow",
                            "dynamic_skill_tool": "allow",
                            "computer_use": "allow",
                            "task_subagent": "allow"
                        }
                    }
                }
            }
        }))
        .expect("notification should decode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("known notification should map");
        assert!(matches!(mapped, GatewayNotification::TurnStarted(_)));
    }

    #[test]
    fn maps_voice_session_result_notification() {
        let notification = JsonRpcNotification::from_params(
            events::VOICE_SESSION_RESULT,
            &json!({
                "session_id": "voice_123",
                "outcome": "no_speech",
                "turn_id": "turn_123",
                "error": {
                    "kind": "no_speech",
                    "message": "No speech detected."
                }
            }),
        )
        .expect("voice result notification should encode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("voice result should map");
        match mapped {
            GatewayNotification::VoiceSessionResult(notification) => {
                assert_eq!(notification.session_id, "voice_123");
                assert_eq!(notification.outcome, crate::VoiceSessionOutcome::NoSpeech);
                assert_eq!(
                    notification.error.as_ref().map(|error| error.kind),
                    Some(crate::VoiceErrorKind::NoSpeech)
                );
            }
            other => panic!("expected voice session result, got {other:?}"),
        }
    }

    #[test]
    fn maps_workspace_changed_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "workspace/changed",
            "params": {
                "kind": "updated",
                "workspace": {
                    "id": "ws_000000000000000001",
                    "name": "Renamed",
                    "is_active": true,
                    "is_current": false,
                    "created_at": 1,
                    "updated_at": 2
                }
            }
        }))
        .expect("notification should decode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("workspace change should map");
        match mapped {
            GatewayNotification::WorkspaceChanged(notification) => {
                assert_eq!(notification.kind, WorkspaceChangeKind::Updated);
                assert_eq!(notification.workspace.id, "ws_000000000000000001");
            }
            other => panic!("expected workspace changed, got {other:?}"),
        }
    }

    #[test]
    fn maps_gateway_remote_access_status_changed_notification() {
        let notification = JsonRpcNotification::from_params(
            "gateway/remote_access/status_changed",
            &json!({
                "status": {
                    "state": "failed",
                    "error_kind": "relay_connect_failed",
                    "message": "failed to connect",
                    "updated_at_unix": 1
                }
            }),
        )
        .expect("remote access status notification should encode");

        let mapped = GatewayNotification::from_jsonrpc(notification)
            .expect("remote access status changed should map");
        match mapped {
            GatewayNotification::GatewayRemoteAccessStatusChanged(notification) => {
                assert_eq!(
                    notification.status.state,
                    crate::GatewayRemoteAccessState::Failed
                );
                assert_eq!(
                    notification.status.error_kind,
                    Some(crate::GatewayRemoteAccessErrorKind::RelayConnectFailed)
                );
            }
            other => panic!("expected remote access status changed, got {other:?}"),
        }
    }

    #[test]
    fn maps_gateway_thread_episodic_vector_refill_status_changed_notification() {
        let notification = JsonRpcNotification::from_params(
            "gateway/thread_episodic/vector_refill/status_changed",
            &json!({
                "workspace_id": "workspace_a",
                "status": "complete"
            }),
        )
        .expect("vector refill status notification should encode");

        let mapped = GatewayNotification::from_jsonrpc(notification)
            .expect("vector refill status changed should map");
        match mapped {
            GatewayNotification::GatewayThreadEpisodicVectorRefillStatusChanged(notification) => {
                assert_eq!(notification.workspace_id, "workspace_a");
                assert_eq!(
                    notification.status,
                    crate::GatewayThreadEpisodicVectorRefillStatus::Complete
                );
            }
            other => panic!("expected vector refill status changed, got {other:?}"),
        }
    }

    #[test]
    fn maps_gateway_voice_input_status_changed_notification() {
        let notification = JsonRpcNotification::from_params(
            events::GATEWAY_VOICE_INPUT_STATUS_CHANGED,
            &json!({
                "settings": {
                    "enabled": true,
                    "provider": "local",
                    "model": "parakeet-tdt-0.6b-v3",
                    "runtime": {
                        "phase": "ready",
                        "effective_enabled": true,
                        "model": "parakeet-tdt-0.6b-v3"
                    }
                }
            }),
        )
        .expect("voice status notification should encode");

        let mapped = GatewayNotification::from_jsonrpc(notification)
            .expect("voice status notification should map");
        match mapped {
            GatewayNotification::GatewayVoiceInputStatusChanged(notification) => {
                assert!(notification.settings.enabled);
                assert!(notification.settings.runtime.effective_enabled);
                assert_eq!(
                    notification.settings.runtime.phase,
                    crate::GatewayVoiceInputRuntimePhase::Ready
                );
            }
            other => panic!("expected voice status changed, got {other:?}"),
        }
    }

    #[test]
    fn maps_memory_notifications() {
        let changed = JsonRpcNotification::from_params(
            "memory/changed",
            &json!({
                "memory_id": "mem_1",
                "scope": {
                    "kind": "workspace",
                    "key": "ws_1"
                },
                "change_kind": "created"
            }),
        )
        .expect("memory changed notification should encode");
        let mapped = GatewayNotification::from_jsonrpc(changed).expect("memory changed should map");
        assert!(matches!(
            mapped,
            GatewayNotification::MemoryChanged(MemoryChangedNotification { .. })
        ));

        let candidate_created = JsonRpcNotification::from_params(
            "memory/candidate_created",
            &json!({
                "candidate": {
                    "id": "cand_1",
                    "scope": {
                        "kind": "workspace",
                        "key": "ws_1"
                    },
                    "category": "preference",
                    "candidate_text": "The user likes compact summaries.",
                    "confidence": 0.8,
                    "reason": "explicit statement",
                    "provenance": {
                        "source_thread_id": "thread_1"
                    },
                    "status": "pending",
                    "created_at": 1700000000
                }
            }),
        )
        .expect("candidate created notification should encode");
        let mapped = GatewayNotification::from_jsonrpc(candidate_created)
            .expect("candidate created should map");
        assert!(matches!(
            mapped,
            GatewayNotification::MemoryCandidateCreated(MemoryCandidateCreatedNotification { .. })
        ));

        let forgotten = JsonRpcNotification::from_params(
            "memory/forgotten",
            &json!({
                "memory_ids": ["mem_1"],
                "reason": "user request"
            }),
        )
        .expect("memory forgotten notification should encode");
        let mapped =
            GatewayNotification::from_jsonrpc(forgotten).expect("memory forgotten should map");
        assert!(matches!(
            mapped,
            GatewayNotification::MemoryForgotten(MemoryForgottenNotification { .. })
        ));
    }

    #[test]
    fn maps_cli_runtime_notifications() {
        let status_changed = JsonRpcNotification::from_params(
            events::CLI_RUNTIME_STATUS_CHANGED,
            &json!({
                "workspace_id": "ws_1",
                "runtime": cli_runtime_summary_json(),
                "future_status_field": { "ignored": true }
            }),
        )
        .expect("status notification should encode");
        match GatewayNotification::from_jsonrpc(status_changed).expect("status should map") {
            GatewayNotification::CLIRuntimeStatusChanged(notification) => {
                assert_eq!(notification.workspace_id, "ws_1");
                assert_eq!(notification.runtime.runtime_id, "codex_personal");
            }
            other => panic!("expected cli runtime status notification, got {other:?}"),
        }

        let account_updated = JsonRpcNotification::from_params(
            events::CLI_RUNTIME_ACCOUNT_UPDATED,
            &json!({
                "workspace_id": "ws_1",
                "runtime_id": "codex_personal",
                "kind": "codex",
                "account": {
                    "authenticated": true,
                    "email": "user@example.com"
                },
                "status": { "state": "ready" }
            }),
        )
        .expect("account notification should encode");
        assert!(matches!(
            GatewayNotification::from_jsonrpc(account_updated).expect("account should map"),
            GatewayNotification::CLIRuntimeAccountUpdated(_)
        ));

        let request_opened = JsonRpcNotification::from_params(
            events::CLI_RUNTIME_REQUEST_OPENED,
            &json!({
                "workspace_id": "ws_1",
                "runtime_id": "codex_personal",
                "request_id": "req_1",
                "thread_id": "thread_1",
                "turn_id": "turn_1",
                "request": {
                    "kind": "command_approval",
                    "title": "Run command",
                    "payload": { "command": "cargo check" }
                }
            }),
        )
        .expect("request opened notification should encode");
        assert!(matches!(
            GatewayNotification::from_jsonrpc(request_opened).expect("request opened should map"),
            GatewayNotification::CLIRuntimeRequestOpened(_)
        ));

        let request_resolved = JsonRpcNotification::from_params(
            events::CLI_RUNTIME_REQUEST_RESOLVED,
            &json!({
                "workspace_id": "ws_1",
                "runtime_id": "codex_personal",
                "request_id": "req_1",
                "resolution": { "status": "approved" }
            }),
        )
        .expect("request resolved notification should encode");
        assert!(matches!(
            GatewayNotification::from_jsonrpc(request_resolved)
                .expect("request resolved should map"),
            GatewayNotification::CLIRuntimeRequestResolved(_)
        ));

        let apps_changed = JsonRpcNotification::from_params(
            events::CLI_RUNTIME_APPS_CHANGED,
            &json!({
                "workspace_id": "ws_1",
                "runtime_id": "codex_personal",
                "apps": []
            }),
        )
        .expect("apps notification should encode");
        assert!(matches!(
            GatewayNotification::from_jsonrpc(apps_changed).expect("apps should map"),
            GatewayNotification::CLIRuntimeAppsChanged(_)
        ));
    }

    #[test]
    fn cli_runtime_gateway_notifications_serialize_with_normal_snake_case_kinds() {
        let cases = [
            (
                events::CLI_RUNTIME_STATUS_CHANGED,
                "cli_runtime_status_changed",
                json!({
                    "workspace_id": "ws_1",
                    "runtime": cli_runtime_summary_json()
                }),
            ),
            (
                events::CLI_RUNTIME_ACCOUNT_UPDATED,
                "cli_runtime_account_updated",
                json!({
                    "workspace_id": "ws_1",
                    "runtime_id": "codex_personal",
                    "kind": "codex",
                    "account": null,
                    "status": { "state": "ready" }
                }),
            ),
            (
                events::CLI_RUNTIME_REQUEST_OPENED,
                "cli_runtime_request_opened",
                json!({
                    "workspace_id": "ws_1",
                    "runtime_id": "codex_personal",
                    "request_id": "req_1",
                    "request": { "kind": "command_approval" }
                }),
            ),
            (
                events::CLI_RUNTIME_REQUEST_RESOLVED,
                "cli_runtime_request_resolved",
                json!({
                    "workspace_id": "ws_1",
                    "runtime_id": "codex_personal",
                    "request_id": "req_1",
                    "resolution": { "status": "approved" }
                }),
            ),
            (
                events::CLI_RUNTIME_APPS_CHANGED,
                "cli_runtime_apps_changed",
                json!({
                    "workspace_id": "ws_1",
                    "runtime_id": "codex_personal",
                    "apps": []
                }),
            ),
        ];

        for (method, expected_kind, params) in cases {
            let notification = JsonRpcNotification::from_params(method, &params)
                .expect("notification should encode");
            let mapped =
                GatewayNotification::from_jsonrpc(notification).expect("notification should map");
            let serialized = serde_json::to_value(mapped).expect("notification should serialize");

            assert_eq!(serialized["kind"], expected_kind);
        }
    }

    #[test]
    fn maps_turn_permission_request_notifications() {
        let opened = JsonRpcNotification::from_params(
            events::TURN_PERMISSION_REQUEST_OPENED,
            &json!({
                "request": {
                    "request_id": "req_native",
                    "workspace_id": "ws_1",
                    "thread_id": "thread_1",
                    "turn_id": "turn_1",
                    "tool_name": "shell",
                    "action": "shell_command",
                    "scope_hash": "scope_1",
                    "reason": "policy_requires_approval",
                    "summary": "cargo check"
                }
            }),
        )
        .expect("permission request opened notification should encode");
        assert!(matches!(
            GatewayNotification::from_jsonrpc(opened).expect("opened should map"),
            GatewayNotification::TurnPermissionRequestOpened(_)
        ));

        let resolved = JsonRpcNotification::from_params(
            events::TURN_PERMISSION_REQUEST_RESOLVED,
            &json!({
                "workspace_id": "ws_1",
                "thread_id": "thread_1",
                "turn_id": "turn_1",
                "request_id": "req_native",
                "resolution": "allow_for_turn"
            }),
        )
        .expect("permission request resolved notification should encode");
        assert!(matches!(
            GatewayNotification::from_jsonrpc(resolved).expect("resolved should map"),
            GatewayNotification::TurnPermissionRequestResolved(_)
        ));
    }

    #[test]
    fn maps_future_cli_runtime_notification_to_unknown() {
        let notification = JsonRpcNotification::from_params(
            "cli_runtime/generated_schema_probe",
            &json!({
                "workspace_id": "ws_1",
                "runtime_id": "codex_personal",
                "payload": { "future": true }
            }),
        )
        .expect("future notification should encode");

        match GatewayNotification::from_jsonrpc(notification).expect("future event should map") {
            GatewayNotification::Unknown(notification) => {
                assert_eq!(notification.method, "cli_runtime/generated_schema_probe");
                assert_eq!(notification.workspace_id.as_deref(), Some("ws_1"));
            }
            other => panic!("expected unknown cli runtime notification, got {other:?}"),
        }
    }

    #[test]
    fn maps_thread_updated_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "thread/updated",
            "params": {
                "thread": {
                    "workspace_id": "ws_123",
                    "id": "thr_123",
                    "name": "First title",
                    "preview": "",
                    "mode": "Chat",
                    "model": "gpt-5.4",
                    "model_provider": "openai",
                    "created_at": 0,
                    "updated_at": 0,
                    "status": "Idle",
                    "turns": []
                }
            }
        }))
        .expect("notification should decode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("known notification should map");
        assert!(matches!(mapped, GatewayNotification::ThreadUpdated(_)));
    }

    #[test]
    fn maps_access_changed_without_protected_payload() {
        let notification = JsonRpcNotification::from_params(
            events::ACCESS_CHANGED,
            &crate::AccessChangedNotification {
                authorization_revision: 7,
                workspace_id: "ws_123".to_owned(),
                thread_id: Some("thread_123".to_owned()),
                outcome: crate::AccessChangeOutcome::Retained,
                change: AccessChangeKind::ThreadVisibility,
            },
        )
        .expect("notification should encode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("notification should map");
        assert!(matches!(mapped, GatewayNotification::AccessChanged(_)));
        let serialized = serde_json::to_value(mapped).expect("notification should serialize");
        assert_eq!(serialized["params"]["authorization_revision"], 7);
        assert_eq!(serialized["params"]["thread_id"], "thread_123");
        assert!(serialized["params"].get("principal_id").is_none());
    }

    #[test]
    fn maps_typed_authorization_projection_change_without_policy_contents() {
        let notification = JsonRpcNotification::from_params(
            events::AUTHORIZATION_PROJECTION_CHANGED,
            &crate::AuthorizationProjectionChangedNotification {
                policy_generation: crate::PolicyGeneration::new(8).unwrap(),
                change: crate::AuthorizationChangeKind::ThreadAcl,
                affected: crate::AuthorizationChangeScope::PrincipalThread {
                    principal_id: crate::PrincipalId::new("P00000000000000000001").unwrap(),
                    workspace_id: "ws_123".to_owned(),
                    thread_id: "thread_123".to_owned(),
                },
            },
        )
        .expect("notification should encode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("notification should map");
        assert!(matches!(
            mapped,
            GatewayNotification::AuthorizationProjectionChanged(_)
        ));
        let serialized = serde_json::to_value(mapped).expect("notification should serialize");
        assert_eq!(serialized["params"]["policy_generation"], 8);
        assert_eq!(
            serialized["params"]["affected"]["scope"],
            "principal_thread"
        );
        assert!(serialized["params"].get("actions").is_none());
        assert!(serialized["params"].get("policy").is_none());
    }

    #[test]
    fn maps_malformed_item_notification_to_unknown() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123"
            }
        }))
        .expect("notification should decode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("malformed item should map");
        assert!(matches!(mapped, GatewayNotification::Unknown(_)));
    }

    #[test]
    fn maps_item_recovery_opened_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "item/recovery_opened",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "item_id": "item_123",
                "item_type": "agent_message",
                "recovery_job_id": "rec_123",
                "trigger": "provider_error",
                "action": "retry_with_backoff",
                "attempt_number": 1
            }
        }))
        .expect("notification should decode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("recovery opened should map");
        match mapped {
            GatewayNotification::ItemRecoveryOpened(notification) => {
                assert_eq!(notification.workspace_id, "ws_123");
                assert_eq!(notification.thread_id, "thr_123");
                assert_eq!(notification.turn_id, "turn_123");
                assert_eq!(notification.item_id, "item_123");
                assert_eq!(notification.item_type, TurnItemType::AgentMessage);
                assert_eq!(notification.recovery_job_id, "rec_123");
                assert_eq!(notification.trigger, RecoveryTrigger::ProviderError);
                assert_eq!(notification.action, RecoveryAction::RetryWithBackoff);
                assert_eq!(notification.attempt_number, 1);
            }
            other => panic!("expected item recovery opened, got {other:?}"),
        }
    }

    #[test]
    fn maps_item_recovery_attached_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "item/recovery_attached",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "item_id": "item_456",
                "item_type": "command_execution",
                "recovery_job_id": "rec_123",
                "recovery_item_id": "item_123",
                "recovery_item_type": "agent_message",
                "trigger": "timeout",
                "action": "retry_attempt",
                "existing_status": "active",
                "next_attempt_number": 2
            }
        }))
        .expect("notification should decode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("recovery attached should map");
        match mapped {
            GatewayNotification::ItemRecoveryAttached(notification) => {
                assert_eq!(notification.workspace_id, "ws_123");
                assert_eq!(notification.thread_id, "thr_123");
                assert_eq!(notification.turn_id, "turn_123");
                assert_eq!(notification.item_id, "item_456");
                assert_eq!(notification.item_type, TurnItemType::CommandExecution);
                assert_eq!(notification.recovery_job_id, "rec_123");
                assert_eq!(notification.recovery_item_id, "item_123");
                assert_eq!(notification.recovery_item_type, TurnItemType::AgentMessage);
                assert_eq!(notification.trigger, RecoveryTrigger::Timeout);
                assert_eq!(notification.action, RecoveryAction::RetryAttempt);
                assert_eq!(notification.existing_status, RecoveryJobStatus::Active);
                assert_eq!(notification.next_attempt_number, 2);
            }
            other => panic!("expected item recovery attached, got {other:?}"),
        }
    }

    #[test]
    fn maps_item_tool_retry_scheduled_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "item/tool/retry_scheduled",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "item_id": "item_tool_123",
                "item_type": "web_fetch",
                "tool_retry_episode_id": "tool_retry_turn_123_1",
                "tool_name": "web_fetch",
                "attempt_number": 2,
                "error_class": "timeout",
                "retry_hint": "retry with a smaller request",
                "budgets": [
                    {"kind": "episode", "used": 1, "limit": 3}
                ],
                "failure_signature_fingerprint": "sig_123",
                "reason": "recoverable_tool_output"
            }
        }))
        .expect("notification should decode");

        let mapped = GatewayNotification::from_jsonrpc(notification)
            .expect("tool retry scheduled should map");
        match mapped {
            GatewayNotification::ItemToolRetryScheduled(notification) => {
                assert_eq!(notification.workspace_id, "ws_123");
                assert_eq!(notification.item_type, TurnItemType::WebFetch);
                assert_eq!(notification.error_class, ToolRetryErrorClass::Timeout);
                assert_eq!(notification.budgets.len(), 1);
                assert_eq!(notification.budgets[0].used, 1);
            }
            other => panic!("expected item tool retry scheduled, got {other:?}"),
        }
    }

    #[test]
    fn maps_item_tool_retry_resolved_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "item/tool/retry_resolved",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "item_id": "item_tool_123",
                "item_type": "web_fetch",
                "tool_retry_episode_id": "tool_retry_turn_123_1",
                "tool_name": "web_fetch",
                "attempt_number": 3,
                "resolution": "succeeded",
                "budgets": [
                    {"kind": "episode", "used": 1, "limit": 3}
                ],
                "reason": "successful_tool_output"
            }
        }))
        .expect("notification should decode");

        let mapped = GatewayNotification::from_jsonrpc(notification)
            .expect("tool retry resolved should map");
        match mapped {
            GatewayNotification::ItemToolRetryResolved(notification) => {
                assert_eq!(notification.item_id, "item_tool_123");
                assert_eq!(notification.resolution, ToolRetryResolution::Succeeded);
                assert_eq!(notification.budgets[0].limit, 3);
            }
            other => panic!("expected item tool retry resolved, got {other:?}"),
        }
    }

    #[test]
    fn maps_item_tool_retry_exhausted_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "item/tool/retry_exhausted",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "item_id": "item_tool_123",
                "item_type": "web_fetch",
                "tool_retry_episode_id": "tool_retry_turn_123_1",
                "tool_name": "web_fetch",
                "attempt_number": 4,
                "error_class": "timeout",
                "exhaustion_kind": "failure_signature",
                "budgets": [
                    {"kind": "failure_signature", "used": 2, "limit": 2}
                ],
                "failure_signature_fingerprint": "sig_123",
                "reason": "same_failure_signature"
            }
        }))
        .expect("notification should decode");

        let mapped = GatewayNotification::from_jsonrpc(notification)
            .expect("tool retry exhausted should map");
        match mapped {
            GatewayNotification::ItemToolRetryExhausted(notification) => {
                assert_eq!(
                    notification.exhaustion_kind,
                    ToolRetryExhaustionKind::FailureSignature
                );
                assert_eq!(notification.failure_signature_fingerprint, "sig_123");
            }
            other => panic!("expected item tool retry exhausted, got {other:?}"),
        }
    }

    #[test]
    fn maps_turn_tool_loop_budget_exceeded_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "turn/tool_loop/budget_exceeded",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "limit_kind": "agent_rounds",
                "limit": 32,
                "observed": 33,
                "action": "continue_in_next_window",
                "reason": "agent_rounds_exceeded"
            }
        }))
        .expect("notification should decode");

        let mapped = GatewayNotification::from_jsonrpc(notification)
            .expect("tool loop budget notification should map");
        match mapped {
            GatewayNotification::TurnToolLoopBudgetExceeded(notification) => {
                assert_eq!(
                    notification.limit_kind,
                    ToolLoopBudgetLimitKind::AgentRounds
                );
                assert_eq!(
                    notification.action,
                    ToolLoopBudgetAction::ContinueInNextWindow
                );
                assert_eq!(notification.observed, 33);
            }
            other => panic!("expected tool loop budget exceeded, got {other:?}"),
        }
    }

    #[test]
    fn maps_execution_window_lifecycle_notifications() {
        let started = JsonRpcNotification::from_params(
            "turn/execution_window/started",
            &json!({
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "window_id": "win_1",
                "window_index": 1,
                "status": "running",
                "started_at_unix_ms": 1000
            }),
        )
        .expect("started notification should encode");
        match GatewayNotification::from_jsonrpc(started).expect("started should map") {
            GatewayNotification::TurnExecutionWindowStarted(notification) => {
                assert_eq!(notification.window_id, "win_1");
                assert_eq!(notification.status, ExecutionWindowStatus::Running);
            }
            other => panic!("expected execution window started, got {other:?}"),
        }

        let exhausted = JsonRpcNotification::from_params(
            "turn/execution_window/exhausted",
            &json!({
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "window_id": "win_1",
                "window_index": 1,
                "status": "exhausted",
                "exhaustion_reason": "max_tool_calls_per_window",
                "limit": 512,
                "observed": 513,
                "agent_round_count": 20,
                "tool_call_count": 513,
                "started_at_unix_ms": 1000,
                "exhausted_at_unix_ms": 2000,
                "reason": "tool-call window budget exhausted"
            }),
        )
        .expect("exhausted notification should encode");
        match GatewayNotification::from_jsonrpc(exhausted).expect("exhausted should map") {
            GatewayNotification::TurnExecutionWindowExhausted(notification) => {
                assert_eq!(
                    notification.exhaustion_reason,
                    ExecutionWindowExhaustionReason::MaxToolCallsPerWindow
                );
                assert_eq!(notification.observed, 513);
            }
            other => panic!("expected execution window exhausted, got {other:?}"),
        }

        let checkpointed = JsonRpcNotification::from_params(
            "turn/execution_window/checkpointed",
            &json!({
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "window_id": "win_1",
                "window_index": 1,
                "status": "checkpointed",
                "checkpoint_id": "chk_1",
                "checkpoint_kind": "window_exhausted",
                "payload_bytes": 1024,
                "created_at_unix_ms": 2100
            }),
        )
        .expect("checkpointed notification should encode");
        match GatewayNotification::from_jsonrpc(checkpointed).expect("checkpointed should map") {
            GatewayNotification::TurnExecutionWindowCheckpointed(notification) => {
                assert_eq!(notification.checkpoint_id, "chk_1");
                assert_eq!(notification.payload_bytes, 1024);
            }
            other => panic!("expected execution window checkpointed, got {other:?}"),
        }

        let continued = JsonRpcNotification::from_params(
            "turn/execution_window/continued",
            &json!({
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "window_id": "win_2",
                "window_index": 2,
                "status": "continued",
                "previous_window_id": "win_1",
                "previous_window_index": 1,
                "checkpoint_id": "chk_1",
                "continued_at_unix_ms": 2200
            }),
        )
        .expect("continued notification should encode");
        match GatewayNotification::from_jsonrpc(continued).expect("continued should map") {
            GatewayNotification::TurnExecutionWindowContinued(notification) => {
                assert_eq!(notification.window_id, "win_2");
                assert_eq!(notification.previous_window_id, "win_1");
            }
            other => panic!("expected execution window continued, got {other:?}"),
        }

        let blocked = JsonRpcNotification::from_params(
            "turn/execution_window/blocked",
            &json!({
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "window_id": "win_3",
                "window_index": 3,
                "status": "blocked",
                "exhaustion_reason": "max_wall_clock_ms_per_window",
                "checkpoint_id": "chk_3",
                "total_windows": 3,
                "total_tool_calls": 900,
                "reason": "total continuation budget exhausted",
                "blocked_at_unix_ms": 3000
            }),
        )
        .expect("blocked notification should encode");
        match GatewayNotification::from_jsonrpc(blocked).expect("blocked should map") {
            GatewayNotification::TurnExecutionWindowBlocked(notification) => {
                assert_eq!(notification.status, ExecutionWindowStatus::Blocked);
                assert_eq!(notification.checkpoint_id.as_deref(), Some("chk_3"));
            }
            other => panic!("expected execution window blocked, got {other:?}"),
        }
    }

    #[test]
    fn maps_malformed_execution_window_notification_to_unknown() {
        let notification = JsonRpcNotification::from_params(
            "turn/execution_window/exhausted",
            &json!({
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123"
            }),
        )
        .expect("malformed window notification should encode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("malformed window should map");
        assert!(matches!(mapped, GatewayNotification::Unknown(_)));
    }

    #[test]
    fn maps_malformed_tool_retry_notification_to_unknown() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "item/tool/retry_scheduled",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123"
            }
        }))
        .expect("notification should decode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("malformed item should map");
        assert!(matches!(mapped, GatewayNotification::Unknown(_)));
    }

    #[test]
    fn maps_semantic_timeline_notifications() {
        let blocks_changed = JsonRpcNotification::from_params(
            events::THREAD_TIMELINE_BLOCKS_CHANGED,
            &json!({
                "workspaceId": "ws_1",
                "threadId": "thr_1",
                "changedBlockIds": ["block_1"],
                "reason": "live_event"
            }),
        )
        .expect("blocks changed notification should encode");
        assert!(matches!(
            GatewayNotification::from_jsonrpc(blocks_changed)
                .expect("blocks changed notification should map"),
            GatewayNotification::ThreadTimelineBlocksChanged(_)
        ));

        let work_items_changed = JsonRpcNotification::from_params(
            events::TURN_WORK_ITEMS_CHANGED,
            &json!({
                "workspaceId": "ws_1",
                "threadId": "thr_1",
                "turnId": "turn_1",
                "changedWorkItemIds": ["work_item_1"],
                "reason": "live_event"
            }),
        )
        .expect("work items notification should encode");
        assert!(matches!(
            GatewayNotification::from_jsonrpc(work_items_changed)
                .expect("work items notification should map"),
            GatewayNotification::TurnWorkItemsChanged(_)
        ));

        let work_state_changed = JsonRpcNotification::from_params(
            events::TURN_WORK_STATE_CHANGED,
            &json!({
                "workspaceId": "ws_1",
                "threadId": "thr_1",
                "turnId": "turn_1",
                "work": {
                    "turnId": "turn_1",
                    "presentation": "expanded_live",
                    "state": "running",
                    "workCount": 1,
                    "visibleWorkCount": 1,
                    "hiddenWorkCount": 0,
                    "hasMoreBefore": false,
                    "hasMoreAfter": true
                },
                "reason": "state_changed"
            }),
        )
        .expect("work state notification should encode");
        assert!(matches!(
            GatewayNotification::from_jsonrpc(work_state_changed)
                .expect("work state notification should map"),
            GatewayNotification::TurnWorkStateChanged(_)
        ));
    }

    #[test]
    fn schema_documents_include_tool_retry_notifications_and_replay_payloads() {
        let schema_names = crate::protocol_schema_documents()
            .into_iter()
            .map(|document| document.file_name)
            .collect::<Vec<_>>();

        for expected in [
            "item_tool_retry_scheduled_notification.json",
            "item_tool_retry_resolved_notification.json",
            "item_tool_retry_exhausted_notification.json",
            "turn_tool_loop_budget_exceeded_notification.json",
            "turn_execution_window_started_notification.json",
            "turn_execution_window_exhausted_notification.json",
            "turn_execution_window_checkpointed_notification.json",
            "turn_execution_window_continued_notification.json",
            "turn_execution_window_blocked_notification.json",
            "execution_window_status.json",
            "execution_window_exhaustion_reason.json",
            "turn_item_event_payload.json",
            "thread_history_event_payload.json",
            "tool_recovery_policy_snapshot.json",
            "tool_output_policy_snapshot.json",
            "tool_output_summary.json",
            "tool_display_payload.json",
            "tool_storage_payload.json",
            "tool_recovery_view.json",
            "tool_recovery_retry_class.json",
            "tool_recovery_idempotency_mode.json",
            "gateway_notification.json",
            "workspace_select_params.json",
            "workspace_select_response.json",
            "workspace_update_params.json",
            "workspace_update_response.json",
            "workspace_change_kind.json",
            "workspace_changed_notification.json",
            "gateway_remote_access_status_changed_notification.json",
            "thread_timeline_page_params.json",
            "thread_timeline_page_response.json",
            "thread_timeline_blocks_changed_notification.json",
            "timeline_block.json",
            "timeline_block_kind.json",
            "timeline_cursor.json",
            "turn_work_block.json",
            "turn_work_page_params.json",
            "turn_work_page_response.json",
            "turn_work_items_changed_notification.json",
            "turn_work_state_changed_notification.json",
            "mcp_list_params.json",
            "mcp_list_response.json",
            "mcp_scope_kind.json",
            "mcp_source_kind.json",
            "mcp_install_params.json",
            "mcp_install_response.json",
            "mcp_install_status.json",
            "mcp_install_result_status.json",
            "mcp_policy_state.json",
            "mcp_policy_set_params.json",
            "mcp_policy_set_response.json",
            "mcp_changed_notification.json",
            "mcp_changed_action.json",
            "mcp_server_status_changed_notification.json",
            "mcp_server_catalog_changed_notification.json",
            "task.json",
            "task_run.json",
            "task_agent_spec.json",
            "task_event.json",
            "thread_lineage.json",
            "task_turn_item.json",
            "task_created_notification.json",
            "task_run_started_notification.json",
            "task_completed_notification.json",
            "thread_origin_kind.json",
            "thread_sidebar_visibility.json",
        ] {
            assert!(
                schema_names.iter().any(|name| *name == expected),
                "missing schema document {expected}"
            );
        }
    }

    #[test]
    fn keeps_explicit_generic_stream_on_item_agent_message_delta() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "item/agent_message/delta",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "item_id": "item_123",
                "delta": "reasoning chunk",
                "stream": "generic"
            }
        }))
        .expect("notification should decode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("item delta should map");
        match mapped {
            GatewayNotification::ItemDelta(notification) => {
                assert_eq!(notification.stream, Some(ItemDeltaStream::Generic));
            }
            other => panic!("expected item delta, got {other:?}"),
        }
    }

    #[test]
    fn ignores_irrelevant_notification_methods() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "system/health",
            "params": {}
        }))
        .expect("notification should decode");

        assert!(GatewayNotification::from_jsonrpc(notification).is_none());
    }

    #[test]
    fn maps_epic5_refetch_notifications_without_secret_payloads() {
        let cases = [
            (
                events::INVITATION_CHANGED,
                json!({
                    "revision": 1,
                    "invitation_id": "I00000000000000000001"
                }),
                "invitation",
            ),
            (
                events::MEMBER_CHANGED,
                json!({
                    "revision": 2,
                    "principal_id": "P00000000000000000001"
                }),
                "member",
            ),
            (
                events::WORKSPACE_MEMBERS_CHANGED,
                json!({
                    "revision": 3,
                    "workspace_id": "W00000000000000000001"
                }),
                "workspace",
            ),
        ];

        for (method, params, expected) in cases {
            let encoded = serde_json::to_string(&params).unwrap();
            assert!(!encoded.contains("pinv1_"));
            assert!(!encoded.contains("token"));
            let notification = JsonRpcNotification::from_params(method, &params).unwrap();
            let mapped = GatewayNotification::from_jsonrpc(notification).unwrap();
            assert!(
                matches!(
                    (&mapped, expected),
                    (GatewayNotification::InvitationChanged(_), "invitation")
                        | (GatewayNotification::MemberChanged(_), "member")
                        | (GatewayNotification::WorkspaceMembersChanged(_), "workspace")
                ),
                "unexpected Epic 5 notification {mapped:?}"
            );
        }
    }
}
