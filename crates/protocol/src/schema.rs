use schemars::{Schema, schema_for};
use std::fs;
use std::path::Path;

use crate::timeline::{
    ThreadTimelineBlocksChangedNotification, ThreadTimelinePageParams, ThreadTimelinePageResponse,
    TimelineBlock, TimelineBlockKind, TimelineChangeReason, TimelineCursor, TimelinePageAnchor,
    TimelinePageInfo, TurnWorkBlock, TurnWorkItem, TurnWorkItemStatus,
    TurnWorkItemsChangedNotification, TurnWorkPageParams, TurnWorkPageResponse,
    TurnWorkPresentation, TurnWorkState, TurnWorkStateChangedNotification,
};

use crate::artifact::{
    ArtifactBindParams, ArtifactBindResponse, ArtifactBindingDirection, ArtifactBindingKind,
    ArtifactBindingSummary, ArtifactCapabilitiesParams, ArtifactCapabilitiesResponse,
    ArtifactCreatedByKind, ArtifactCreatedNotification, ArtifactDeleteParams,
    ArtifactDeleteResponse, ArtifactDeletedNotification, ArtifactDownloadAbortParams,
    ArtifactDownloadAbortResponse, ArtifactDownloadCapabilities, ArtifactDownloadChunkHeader,
    ArtifactDownloadChunkParams, ArtifactDownloadChunkResponse, ArtifactDownloadFinishParams,
    ArtifactDownloadFinishResponse, ArtifactDownloadProgressNotification,
    ArtifactDownloadStartParams, ArtifactDownloadStartResponse, ArtifactGetParams,
    ArtifactGetResponse, ArtifactKind, ArtifactListForMessageParams, ArtifactListForThreadParams,
    ArtifactListForTurnParams, ArtifactListParams, ArtifactListResponse, ArtifactPrepareKind,
    ArtifactPrepareParams, ArtifactPrepareResponse, ArtifactPreviewRef, ArtifactProjectionKind,
    ArtifactProjectionStatus, ArtifactProjectionUpdatedNotification, ArtifactReadParams,
    ArtifactReadResponse, ArtifactRef, ArtifactRegisterParams, ArtifactRegisterResponse,
    ArtifactRestoreParams, ArtifactRestoreResponse, ArtifactRole, ArtifactStatus, ArtifactSummary,
    ArtifactUpdatedNotification, ArtifactUploadAbortParams, ArtifactUploadAbortResponse,
    ArtifactUploadCapabilities, ArtifactUploadChunkAckNotification, ArtifactUploadChunkHeader,
    ArtifactUploadFinishParams, ArtifactUploadFinishResponse, ArtifactUploadProgressNotification,
    ArtifactUploadSourceKind, ArtifactUploadStartParams, ArtifactUploadStartResponse,
    ThreadArtifactsChangedNotification,
};

use crate::{
    AgentDurableEvent, AgentExecutionBackend, AgentMessagePhase, AgentProgressEvent, ByteRange,
    CLIAgentRuntimeKind, CLIAgentRuntimeSandboxPolicy, CLIRuntimeAccountUpdatedNotification,
    CLIRuntimeAppsChangedNotification, CLIRuntimeGetParams, CLIRuntimeGetResponse,
    CLIRuntimeListModelsParams, CLIRuntimeListModelsResponse, CLIRuntimeListParams,
    CLIRuntimeListResponse, CLIRuntimeLoginCancelParams, CLIRuntimeLoginCancelResponse,
    CLIRuntimeLoginStartParams, CLIRuntimeLoginStartResponse, CLIRuntimeLoginStartType,
    CLIRuntimePendingRequest, CLIRuntimePendingRequestStatus, CLIRuntimeRefreshParams,
    CLIRuntimeRefreshResponse, CLIRuntimeRequestKind, CLIRuntimeRequestOpenedNotification,
    CLIRuntimeRequestResolution, CLIRuntimeRequestResolvedNotification,
    CLIRuntimeRequestRespondParams, CLIRuntimeRequestRespondResponse, CLIRuntimeReviewDelivery,
    CLIRuntimeReviewStartParams, CLIRuntimeReviewStartResponse, CLIRuntimeReviewTarget,
    CLIRuntimeStatusChangedNotification, CLIRuntimeStatusParams, CLIRuntimeStatusResponse,
    CLIRuntimeThreadBinding, CLIRuntimeThreadBindingGetParams, CLIRuntimeThreadBindingGetResponse,
    CLIRuntimeThreadCompactParams, CLIRuntimeThreadCompactResponse, CLIRuntimeThreadForkParams,
    CLIRuntimeThreadForkResponse, CLIRuntimeTurnSteerParams, CLIRuntimeTurnSteerResponse,
    ContextCompressedNotification, ContextCompressingNotification, DurableEventCausalityKey,
    ExecutionCheckpointOriginalRequestSummary, ExecutionCheckpointPayload,
    ExecutionCheckpointProviderBudgetSummary, ExecutionCheckpointStrictObligation,
    ExecutionCheckpointToolCallSummary, ExecutionCheckpointToolSummary,
    ExecutionCheckpointWindowSummary, ExecutionWindowExhaustionReason, ExecutionWindowStatus,
    GatewayCliRuntimeInstanceSettings, GatewayCliRuntimeSettings, GatewayGeneralSettings,
    GatewayGeneralSettingsUpdate, GatewayMemoryModelSelection, GatewayMemoryModelSelectionSource,
    GatewayMemorySettings, GatewayNotification, GatewayRemoteAccessErrorKind,
    GatewayRemoteAccessSettings, GatewayRemoteAccessSettingsUpdate, GatewayRemoteAccessState,
    GatewayRemoteAccessStatusChangedNotification, GatewayRemoteAccessStatusSnapshot,
    GatewayRemoteAccessTransport, GatewaySettingsGetParams, GatewaySettingsGetResponse,
    GatewaySettingsSnapshot, GatewaySettingsUpdate, GatewaySettingsUpdateParams,
    GatewaySettingsUpdateResponse, GatewayThreadEpisodicSettings,
    GatewayThreadEpisodicSettingsUpdate, ItemCompletedNotification, ItemDeltaNotification,
    ItemRecoveryAttachedNotification, ItemRecoveryExhaustedNotification,
    ItemRecoveryOpenedNotification, ItemRecoverySucceededNotification,
    ItemRetryAttemptStartedNotification, ItemRetryScheduledNotification, ItemStartedNotification,
    ItemTimeoutDetectedNotification, ItemToolRetryExhaustedNotification,
    ItemToolRetryResolvedNotification, ItemToolRetryScheduledNotification, ItemUpdatedNotification,
    MarkdownBlock, MarkdownDocument, MarkdownInline, MarkdownList, MarkdownListItem, MarkdownMark,
    MarkdownMarkKind, McpAuditEventSummary, McpChangedAction, McpChangedItem,
    McpChangedNotification, McpDiagnosticLevel, McpInstallParams, McpInstallResponse,
    McpInstallResult, McpInstallResultStatus, McpInstallStatus, McpLifecycleAuditSummary,
    McpListItem, McpListParams, McpListResponse, McpPolicySetParams, McpPolicySetResponse,
    McpPolicyState, McpPromptCatalogItem, McpResourceCatalogItem, McpResourceTemplateCatalogItem,
    McpRuntimeState, McpRuntimeStatus, McpScopeKind, McpServerCatalogChangedNotification,
    McpServerCatalogDetails, McpServerDetailsParams, McpServerDetailsResponse,
    McpServerHealthDetails, McpServerPolicy, McpServerRestartParams, McpServerRestartResponse,
    McpServerStatus, McpServerStatusChangedNotification, McpServerStatusItem, McpSourceKind,
    McpToolAnnotationSummary, McpToolCatalogItem, McpTransportSummary, McpTurnBindingSummary,
    McpUninstallParams, McpUninstallResponse, McpValidationDiagnostic, MemoryActor,
    MemoryActorKind, MemoryAttribute, MemoryAttributeCardinality, MemoryCandidate,
    MemoryCandidateCreatedNotification, MemoryCandidateDecision, MemoryCandidatePolicyDecision,
    MemoryCandidatePolicyInput, MemoryCandidatePolicyOutput, MemoryCandidateScore,
    MemoryCandidateScoreBucket, MemoryCandidateStatus, MemoryCandidatesApproveParams,
    MemoryCandidatesApproveResponse, MemoryCandidatesDecideParams, MemoryCandidatesDecideResponse,
    MemoryCandidatesEditAndApproveParams, MemoryCandidatesEditAndApproveResponse,
    MemoryCandidatesGetParams, MemoryCandidatesGetResponse, MemoryCandidatesListParams,
    MemoryCandidatesListResponse, MemoryCandidatesMergeParams, MemoryCandidatesMergeResponse,
    MemoryCandidatesRejectParams, MemoryCandidatesRejectResponse,
    MemoryCandidatesSuppressSimilarParams, MemoryCandidatesSuppressSimilarResponse,
    MemoryCanonicalKey, MemoryCategory, MemoryChangeKind, MemoryChangedNotification,
    MemoryDurability, MemoryEvidenceActorRole, MemoryEvidenceClass, MemoryExplicitness,
    MemoryExtractorCertainty, MemoryFactClass, MemoryForgetParams, MemoryForgetResponse,
    MemoryForgetTarget, MemoryForgottenNotification, MemoryGetParams, MemoryGetResponse,
    MemoryIntent, MemoryLifetimeClass, MemoryListParams, MemoryListResponse, MemoryOwnershipClass,
    MemoryProvenance, MemoryQualityAction, MemoryQualityDecision, MemoryQualityReasonCode,
    MemoryRecord, MemoryRememberParams, MemoryRememberResponse, MemoryScope, MemoryScopeClarity,
    MemoryScopeHint, MemoryScopeKind, MemorySearchHit, MemorySearchParams, MemorySearchResponse,
    MemorySemanticFields, MemorySemanticWriteDisposition, MemorySemanticWriteParams,
    MemorySemanticWriteResponse, MemorySensitivity, MemorySensitivityHint, MemorySourceContextKind,
    MemoryStatus, MemorySubject, MemoryWriteEvidence, MemoryWriteRelation, PermissionBehavior,
    ProgressCoalescingKey, PromptManifest, PromptManifestDiagnostic, PromptManifestDiagnosticCode,
    PromptManifestHookContributionKind, PromptManifestHookPhase, PromptManifestHookSource,
    PromptManifestHookSourceEntry, PromptManifestHookTruncation, PromptManifestProfile,
    ProviderDeleteApiKeyParams, ProviderDeleteApiKeyResponse, ProviderFailureClass,
    ProviderFailureDetails, ProviderFailureStage, ProviderListModelsParams,
    ProviderListModelsResponse, ProviderListParams, ProviderListResponse, ProviderSetApiKeyParams,
    ProviderSetApiKeyResponse, ProviderTransportKind, RuntimeAccountSnapshot, RuntimeAppInfo,
    RuntimeCapabilities, RuntimeDiagnostic, RuntimeDiagnosticLevel, RuntimeModelInfo,
    RuntimeStatus, RuntimeSummary, SandboxMode, SandboxPolicy, SkillArchiveFormat,
    SkillAuditTimelineItem, SkillChangedItem, SkillDependencyDiagnostic, SkillHealthItem,
    SkillHealthSummary, SkillHealthTarget, SkillInstallState, SkillLifecycleAuditSummary,
    SkillLifecycleResultSkill, SkillLifecycleSource, SkillListItem, SkillListParams,
    SkillListResponse, SkillPolicyState, SkillSecurityFinding, SkillTrustGateStatus,
    SkillWorkspacePolicy, SkillsChangedNotification, SkillsHealthParams, SkillsHealthResponse,
    SkillsInstallParams, SkillsInstallResponse, SkillsPolicyListParams, SkillsPolicyListResponse,
    SkillsPolicySetParams, SkillsPolicySetResponse, SkillsUninstallParams, SkillsUninstallResponse,
    SkillsUpdateParams, SkillsUpdateResponse, SkillsUploadAbortParams, SkillsUploadAbortResponse,
    SkillsUploadChunkAckNotification, SkillsUploadChunkHeader, SkillsUploadFinishParams,
    SkillsUploadFinishResponse, SkillsUploadStartParams, SkillsUploadStartResponse, Task,
    TaskAcceptParams, TaskAcceptResponse, TaskAgendaItem, TaskAgendaParams, TaskAgendaResponse,
    TaskAgentContext, TaskAgentContextMode, TaskAgentContextPolicy, TaskAgentInput,
    TaskAgentInputAttachment, TaskAgentInputAttachmentKind, TaskAgentInputReference,
    TaskAgentInputReferenceKind, TaskAgentInputVariable, TaskAgentPrompt, TaskAgentResultContract,
    TaskAgentResultFormat, TaskAgentReviewMode, TaskAgentReviewPolicy, TaskAgentSpec,
    TaskAgentSpecInput, TaskAgentToolPolicy, TaskAgentWriteMode, TaskArtifact, TaskAttachmentMode,
    TaskCancelParams, TaskCancelResponse, TaskCancelScope, TaskCancelledNotification,
    TaskCompletedNotification, TaskCompletionBehavior, TaskConcurrencyConflictPolicy,
    TaskConcurrencyPolicy, TaskCreateParams, TaskCreateResponse, TaskCreatedNotification,
    TaskDeliveriesParams, TaskDeliveriesResponse, TaskDelivery, TaskDeliveryAttempt,
    TaskDeliveryAttemptStatus, TaskDeliveryCancelledNotification,
    TaskDeliveryDeliveredNotification, TaskDeliveryFailedNotification, TaskDeliveryFormat,
    TaskDeliveryMode, TaskDeliveryPolicy, TaskDeliveryQueuedNotification,
    TaskDeliveryStartedNotification, TaskDeliveryStatus, TaskDependency, TaskDependencyCondition,
    TaskDependencyTriggerMode, TaskDependencyTriggerPolicy, TaskDetachParams, TaskDetachResponse,
    TaskDetachedNotification, TaskError, TaskErrorClass, TaskEvent, TaskEventPayload,
    TaskEventsParams, TaskEventsResponse, TaskExecutorKind, TaskExternalTriggerFilter,
    TaskFailedNotification, TaskGetParams, TaskGetResponse, TaskLifecyclePolicy, TaskListParams,
    TaskListResponse, TaskManualActor, TaskMetadata, TaskNotificationContext, TaskOwnerKind,
    TaskParentTerminalAction, TaskPauseParams, TaskPauseResponse, TaskPausedNotification,
    TaskProgressDetails, TaskProgressNotification, TaskQueuedNotification,
    TaskRecoveredNotification, TaskRescheduleParams, TaskRescheduleResponse,
    TaskRescheduledNotification, TaskResult, TaskResultCandidate, TaskResultReviewEvent,
    TaskResultReviewResolutionStrategy, TaskResultReviewerSpec, TaskResumeParams,
    TaskResumeResponse, TaskResumedNotification, TaskRetryBackoffKind, TaskRetryPolicy,
    TaskReviseParams, TaskReviseResponse, TaskRun, TaskRunCompletedNotification,
    TaskRunCreatedNotification, TaskRunExecution, TaskRunExecutionStatus,
    TaskRunFailedNotification, TaskRunStartedNotification, TaskRunStatus, TaskRunThreadBinding,
    TaskRunTurn, TaskScheduledNotification, TaskSchema, TaskStatus, TaskThreadLineage,
    TaskTimeoutPolicy, TaskTree, TaskTreeChangedNotification as TaskTreeChangedTaskNotification,
    TaskTreeParams, TaskTreeResponse, TaskTrigger, TaskTriggerInput, TaskTriggerKind,
    TaskTriggerSpec, TaskTriggerStatus, TaskTurnItem, TaskUpdateParams, TaskUpdateResponse,
    TaskUpdatedNotification, TaskValue, TaskWaitItem, TaskWaitParams, TaskWaitResponse,
    TaskWaitReviewAction, TaskWaitReviewItem, TaskWaitRevisionBlockedReason, TaskWriteLock,
    TaskWriteLockConflict, TaskWriteLockScopeKind, TaskWriteLockStatus, TextElement, Thread,
    ThreadAgentsDocArchiveParams, ThreadAgentsDocArchiveResponse,
    ThreadAgentsDocChangedNotification, ThreadAgentsDocGetParams, ThreadAgentsDocGetResponse,
    ThreadAgentsDocPayload, ThreadAgentsDocResolveForThreadParams,
    ThreadAgentsDocResolveForThreadResponse, ThreadAgentsDocResolvedPayload,
    ThreadAgentsDocSaveParams, ThreadAgentsDocSaveReason, ThreadAgentsDocSaveResponse,
    ThreadAgentsDocStatus, ThreadAgentsDocSummary, ThreadClosedNotification,
    ThreadEpisodicAdaptiveDiagnostics, ThreadEpisodicAdaptiveStrategy, ThreadEpisodicChunk,
    ThreadEpisodicChunkId, ThreadEpisodicChunkStatus, ThreadEpisodicHit, ThreadEpisodicItemId,
    ThreadEpisodicRecallDiagnostic, ThreadEpisodicRecallDiagnosticCode, ThreadEpisodicRecallInput,
    ThreadEpisodicRecallOutput, ThreadEpisodicRecallPolicyContext, ThreadEpisodicScoreBreakdown,
    ThreadEpisodicSearchMode, ThreadEpisodicSourceActorRole, ThreadEpisodicSourceContext,
    ThreadEpisodicSourceProvenance, ThreadEpisodicThreadId, ThreadEpisodicTurnId,
    ThreadEpisodicVisibility, ThreadEpisodicWorkspaceId, ThreadFolder, ThreadFolderCreateParams,
    ThreadFolderCreateResponse, ThreadFolderDeleteParams, ThreadFolderDeleteResponse,
    ThreadFolderMoveParams, ThreadFolderMoveResponse, ThreadGetParams, ThreadGetResponse,
    ThreadHistoryEvent, ThreadHistoryEventPayload, ThreadLineage, ThreadMode, ThreadMoveParams,
    ThreadMoveResponse, ThreadOriginKind, ThreadPlacement, ThreadSidebarVisibility,
    ThreadStartParams, ThreadStartResponse, ThreadStartedNotification, ThreadStatus,
    ThreadTreeChangedNotification, ThreadTreeParams, ThreadTreeResponse, ThreadUnsubscribeParams,
    ThreadUnsubscribeResponse, ThreadUnsubscribeStatus, ThreadUpdateParams, ThreadUpdateResponse,
    ThreadUpdatedNotification, ToolDisplayPayload, ToolLoopBudgetAction, ToolLoopBudgetLimitKind,
    ToolMetadata, ToolMetadataRawKind, ToolMetadataValue, ToolOutputPolicySnapshot,
    ToolOutputSummary, ToolPermissionPolicySnapshot, ToolRecoveryIdempotencyMode,
    ToolRecoveryPolicySnapshot, ToolRecoveryRetryClass, ToolRecoveryView, ToolRetryBudgetKind,
    ToolRetryBudgetUsage, ToolRetryErrorClass, ToolRetryExhaustionKind, ToolRetryResolution,
    ToolStoragePayload, Turn, TurnAcceptedCapability, TurnBlockedNotification,
    TurnBlockedResumeMetadata, TurnCLIRuntimeOptions, TurnCancelParams, TurnCancelResponse,
    TurnCapability, TurnCapabilityAcceptedReason, TurnCapabilityKind, TurnCapabilityRejectedReason,
    TurnCompletedNotification, TurnExecutionWindowBlockedNotification,
    TurnExecutionWindowCheckpointedNotification, TurnExecutionWindowContinuedNotification,
    TurnExecutionWindowExhaustedNotification, TurnExecutionWindowStartedNotification,
    TurnFailedNotification, TurnGetParams, TurnGetResponse, TurnItem, TurnItemAttemptStatus,
    TurnItemEvent, TurnItemEventPayload, TurnItemTimeoutReason, TurnItemType, TurnItemsParams,
    TurnItemsResponse, TurnKind, TurnMcpServerCapabilitySummary, TurnMcpToolCapabilitySummary,
    TurnOrigin, TurnPermissionActionKind, TurnPermissionApprovalRequest,
    TurnPermissionApprovalRequestDetail, TurnPermissionApprovalResolution,
    TurnPermissionAuditDecision, TurnPermissionAuditEvent, TurnPermissionAuditEventKind,
    TurnPermissionAuditRequestKey, TurnPermissionDecisionReason, TurnPermissionMode,
    TurnPermissionProfileCap, TurnPermissionProfileSelection, TurnPermissionProfileSnapshot,
    TurnPermissionProfileSource, TurnPermissionRequestOpenedNotification,
    TurnPermissionRequestResolvedNotification, TurnPermissionRequestRespondParams,
    TurnPermissionRequestRespondResponse, TurnReasoningSelection, TurnRejectedCapability,
    TurnResumeParams, TurnResumeResponse, TurnSkillCapabilitySummary, TurnStartParams,
    TurnStartResponse, TurnStartedNotification, TurnStatus, TurnStatusChangedNotification,
    TurnToolLoopBudgetExceededNotification, UnknownGatewayNotification, UserInput,
    VoiceAudioEncoding, VoiceAudioFormat, VoiceChunkAckNotification, VoiceChunkFrameHeader,
    VoiceError, VoiceErrorKind, VoiceSessionCancelParams, VoiceSessionCancelResponse,
    VoiceSessionFinalizeParams, VoiceSessionFinalizeResponse, VoiceSessionOutcome,
    VoiceSessionResultNotification, VoiceSessionStartParams, VoiceSessionStartResponse,
    VoiceStatus, VoiceStatusParams, VoiceStatusResponse, VoiceTurnContext, Workspace,
    WorkspaceChangeKind, WorkspaceChangedNotification, WorkspaceCreateParams,
    WorkspaceCreateResponse, WorkspaceDefaultParams, WorkspaceDefaultResponse, WorkspaceListParams,
    WorkspaceListResponse, WorkspaceSelectParams, WorkspaceSelectResponse, WorkspaceUpdateParams,
    WorkspaceUpdateResponse,
};

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

pub fn protocol_schema_documents() -> Vec<SchemaDocument> {
    vec![
        schema_doc!("agent_durable_event.json", AgentDurableEvent),
        schema_doc!("agent_progress_event.json", AgentProgressEvent),
        schema_doc!("durable_event_causality_key.json", DurableEventCausalityKey),
        schema_doc!("progress_coalescing_key.json", ProgressCoalescingKey),
        schema_doc!("turn_accepted_capability.json", TurnAcceptedCapability),
        schema_doc!(
            "turn_capability_accepted_reason.json",
            TurnCapabilityAcceptedReason
        ),
        schema_doc!(
            "turn_capability_rejected_reason.json",
            TurnCapabilityRejectedReason
        ),
        schema_doc!(
            "turn_permission_audit_decision.json",
            TurnPermissionAuditDecision
        ),
        schema_doc!("turn_permission_audit_event.json", TurnPermissionAuditEvent),
        schema_doc!(
            "turn_permission_audit_event_kind.json",
            TurnPermissionAuditEventKind
        ),
        schema_doc!(
            "turn_permission_audit_request_key.json",
            TurnPermissionAuditRequestKey
        ),
        schema_doc!("turn_rejected_capability.json", TurnRejectedCapability),
        schema_doc!("workspace.json", Workspace),
        schema_doc!("workspace_list_params.json", WorkspaceListParams),
        schema_doc!("workspace_list_response.json", WorkspaceListResponse),
        schema_doc!("workspace_create_params.json", WorkspaceCreateParams),
        schema_doc!("workspace_create_response.json", WorkspaceCreateResponse),
        schema_doc!("workspace_select_params.json", WorkspaceSelectParams),
        schema_doc!("workspace_select_response.json", WorkspaceSelectResponse),
        schema_doc!("workspace_update_params.json", WorkspaceUpdateParams),
        schema_doc!("workspace_update_response.json", WorkspaceUpdateResponse),
        schema_doc!("workspace_default_params.json", WorkspaceDefaultParams),
        schema_doc!("workspace_default_response.json", WorkspaceDefaultResponse),
        schema_doc!("workspace_change_kind.json", WorkspaceChangeKind),
        schema_doc!(
            "workspace_changed_notification.json",
            WorkspaceChangedNotification
        ),
        schema_doc!("artifact_kind.json", ArtifactKind),
        schema_doc!("artifact_status.json", ArtifactStatus),
        schema_doc!("artifact_created_by_kind.json", ArtifactCreatedByKind),
        schema_doc!("artifact_binding_kind.json", ArtifactBindingKind),
        schema_doc!("artifact_binding_direction.json", ArtifactBindingDirection),
        schema_doc!("artifact_role.json", ArtifactRole),
        schema_doc!("artifact_projection_kind.json", ArtifactProjectionKind),
        schema_doc!("artifact_projection_status.json", ArtifactProjectionStatus),
        schema_doc!("artifact_upload_source_kind.json", ArtifactUploadSourceKind),
        schema_doc!("artifact_prepare_kind.json", ArtifactPrepareKind),
        schema_doc!("artifact_prepare_params.json", ArtifactPrepareParams),
        schema_doc!("artifact_prepare_response.json", ArtifactPrepareResponse),
        schema_doc!("artifact_register_params.json", ArtifactRegisterParams),
        schema_doc!("artifact_register_response.json", ArtifactRegisterResponse),
        schema_doc!("artifact_preview_ref.json", ArtifactPreviewRef),
        schema_doc!("artifact_ref.json", ArtifactRef),
        schema_doc!("artifact_binding_summary.json", ArtifactBindingSummary),
        schema_doc!("artifact_summary.json", ArtifactSummary),
        schema_doc!(
            "artifact_capabilities_params.json",
            ArtifactCapabilitiesParams
        ),
        schema_doc!(
            "artifact_capabilities_response.json",
            ArtifactCapabilitiesResponse
        ),
        schema_doc!(
            "artifact_upload_capabilities.json",
            ArtifactUploadCapabilities
        ),
        schema_doc!(
            "artifact_download_capabilities.json",
            ArtifactDownloadCapabilities
        ),
        schema_doc!("artifact_list_params.json", ArtifactListParams),
        schema_doc!(
            "artifact_list_for_thread_params.json",
            ArtifactListForThreadParams
        ),
        schema_doc!(
            "artifact_list_for_turn_params.json",
            ArtifactListForTurnParams
        ),
        schema_doc!(
            "artifact_list_for_message_params.json",
            ArtifactListForMessageParams
        ),
        schema_doc!("artifact_list_response.json", ArtifactListResponse),
        schema_doc!("artifact_get_params.json", ArtifactGetParams),
        schema_doc!("artifact_get_response.json", ArtifactGetResponse),
        schema_doc!("artifact_read_params.json", ArtifactReadParams),
        schema_doc!("artifact_read_response.json", ArtifactReadResponse),
        schema_doc!(
            "artifact_upload_start_params.json",
            ArtifactUploadStartParams
        ),
        schema_doc!(
            "artifact_upload_start_response.json",
            ArtifactUploadStartResponse
        ),
        schema_doc!(
            "artifact_upload_chunk_header.json",
            ArtifactUploadChunkHeader
        ),
        schema_doc!(
            "artifact_upload_chunk_ack_notification.json",
            ArtifactUploadChunkAckNotification
        ),
        schema_doc!(
            "artifact_upload_finish_params.json",
            ArtifactUploadFinishParams
        ),
        schema_doc!(
            "artifact_upload_finish_response.json",
            ArtifactUploadFinishResponse
        ),
        schema_doc!(
            "artifact_upload_abort_params.json",
            ArtifactUploadAbortParams
        ),
        schema_doc!(
            "artifact_upload_abort_response.json",
            ArtifactUploadAbortResponse
        ),
        schema_doc!(
            "artifact_download_start_params.json",
            ArtifactDownloadStartParams
        ),
        schema_doc!(
            "artifact_download_start_response.json",
            ArtifactDownloadStartResponse
        ),
        schema_doc!(
            "artifact_download_chunk_params.json",
            ArtifactDownloadChunkParams
        ),
        schema_doc!(
            "artifact_download_chunk_header.json",
            ArtifactDownloadChunkHeader
        ),
        schema_doc!(
            "artifact_download_chunk_response.json",
            ArtifactDownloadChunkResponse
        ),
        schema_doc!(
            "artifact_download_finish_params.json",
            ArtifactDownloadFinishParams
        ),
        schema_doc!(
            "artifact_download_finish_response.json",
            ArtifactDownloadFinishResponse
        ),
        schema_doc!(
            "artifact_download_abort_params.json",
            ArtifactDownloadAbortParams
        ),
        schema_doc!(
            "artifact_download_abort_response.json",
            ArtifactDownloadAbortResponse
        ),
        schema_doc!("artifact_bind_params.json", ArtifactBindParams),
        schema_doc!("artifact_bind_response.json", ArtifactBindResponse),
        schema_doc!("artifact_delete_params.json", ArtifactDeleteParams),
        schema_doc!("artifact_delete_response.json", ArtifactDeleteResponse),
        schema_doc!("artifact_restore_params.json", ArtifactRestoreParams),
        schema_doc!("artifact_restore_response.json", ArtifactRestoreResponse),
        schema_doc!(
            "artifact_created_notification.json",
            ArtifactCreatedNotification
        ),
        schema_doc!(
            "artifact_updated_notification.json",
            ArtifactUpdatedNotification
        ),
        schema_doc!(
            "artifact_deleted_notification.json",
            ArtifactDeletedNotification
        ),
        schema_doc!(
            "thread_artifacts_changed_notification.json",
            ThreadArtifactsChangedNotification
        ),
        schema_doc!(
            "artifact_projection_updated_notification.json",
            ArtifactProjectionUpdatedNotification
        ),
        schema_doc!(
            "artifact_upload_progress_notification.json",
            ArtifactUploadProgressNotification
        ),
        schema_doc!(
            "artifact_download_progress_notification.json",
            ArtifactDownloadProgressNotification
        ),
        schema_doc!("memory_scope_kind.json", MemoryScopeKind),
        schema_doc!("memory_scope.json", MemoryScope),
        schema_doc!("memory_category.json", MemoryCategory),
        schema_doc!("memory_status.json", MemoryStatus),
        schema_doc!("memory_sensitivity.json", MemorySensitivity),
        schema_doc!("memory_source_context_kind.json", MemorySourceContextKind),
        schema_doc!("memory_evidence_actor_role.json", MemoryEvidenceActorRole),
        schema_doc!("memory_fact_class.json", MemoryFactClass),
        schema_doc!("memory_lifetime_class.json", MemoryLifetimeClass),
        schema_doc!("memory_ownership_class.json", MemoryOwnershipClass),
        schema_doc!("memory_evidence_class.json", MemoryEvidenceClass),
        schema_doc!("memory_quality_action.json", MemoryQualityAction),
        schema_doc!("memory_quality_reason_code.json", MemoryQualityReasonCode),
        schema_doc!("memory_quality_decision.json", MemoryQualityDecision),
        schema_doc!("memory_actor_kind.json", MemoryActorKind),
        schema_doc!("memory_actor.json", MemoryActor),
        schema_doc!("memory_provenance.json", MemoryProvenance),
        schema_doc!("memory_intent.json", MemoryIntent),
        schema_doc!("memory_explicitness.json", MemoryExplicitness),
        schema_doc!("memory_subject.json", MemorySubject),
        schema_doc!("memory_attribute.json", MemoryAttribute),
        schema_doc!("memory_scope_hint.json", MemoryScopeHint),
        schema_doc!("memory_durability.json", MemoryDurability),
        schema_doc!("memory_sensitivity_hint.json", MemorySensitivityHint),
        schema_doc!("memory_extractor_certainty.json", MemoryExtractorCertainty),
        schema_doc!(
            "memory_attribute_cardinality.json",
            MemoryAttributeCardinality
        ),
        schema_doc!("memory_write_relation.json", MemoryWriteRelation),
        schema_doc!(
            "memory_semantic_write_disposition.json",
            MemorySemanticWriteDisposition
        ),
        schema_doc!("memory_write_evidence.json", MemoryWriteEvidence),
        schema_doc!("memory_semantic_fields.json", MemorySemanticFields),
        schema_doc!("memory_canonical_key.json", MemoryCanonicalKey),
        schema_doc!(
            "memory_semantic_write_params.json",
            MemorySemanticWriteParams
        ),
        schema_doc!(
            "memory_semantic_write_response.json",
            MemorySemanticWriteResponse
        ),
        schema_doc!("memory_record.json", MemoryRecord),
        schema_doc!("memory_search_params.json", MemorySearchParams),
        schema_doc!("memory_search_hit.json", MemorySearchHit),
        schema_doc!("memory_search_response.json", MemorySearchResponse),
        schema_doc!("memory_get_params.json", MemoryGetParams),
        schema_doc!("memory_get_response.json", MemoryGetResponse),
        schema_doc!("memory_list_params.json", MemoryListParams),
        schema_doc!("memory_list_response.json", MemoryListResponse),
        schema_doc!("memory_remember_params.json", MemoryRememberParams),
        schema_doc!("memory_remember_response.json", MemoryRememberResponse),
        schema_doc!("memory_forget_target.json", MemoryForgetTarget),
        schema_doc!("memory_forget_params.json", MemoryForgetParams),
        schema_doc!("memory_forget_response.json", MemoryForgetResponse),
        schema_doc!("memory_candidate_status.json", MemoryCandidateStatus),
        schema_doc!(
            "memory_candidate_policy_decision.json",
            MemoryCandidatePolicyDecision
        ),
        schema_doc!(
            "memory_candidate_score_bucket.json",
            MemoryCandidateScoreBucket
        ),
        schema_doc!("memory_candidate_score.json", MemoryCandidateScore),
        schema_doc!("memory_scope_clarity.json", MemoryScopeClarity),
        schema_doc!(
            "memory_candidate_policy_input.json",
            MemoryCandidatePolicyInput
        ),
        schema_doc!(
            "memory_candidate_policy_output.json",
            MemoryCandidatePolicyOutput
        ),
        schema_doc!("memory_candidate.json", MemoryCandidate),
        schema_doc!("memory_candidate_decision.json", MemoryCandidateDecision),
        schema_doc!(
            "memory_candidates_get_params.json",
            MemoryCandidatesGetParams
        ),
        schema_doc!(
            "memory_candidates_get_response.json",
            MemoryCandidatesGetResponse
        ),
        schema_doc!(
            "memory_candidates_list_params.json",
            MemoryCandidatesListParams
        ),
        schema_doc!(
            "memory_candidates_list_response.json",
            MemoryCandidatesListResponse
        ),
        schema_doc!(
            "memory_candidates_decide_params.json",
            MemoryCandidatesDecideParams
        ),
        schema_doc!(
            "memory_candidates_decide_response.json",
            MemoryCandidatesDecideResponse
        ),
        schema_doc!(
            "memory_candidates_approve_params.json",
            MemoryCandidatesApproveParams
        ),
        schema_doc!(
            "memory_candidates_approve_response.json",
            MemoryCandidatesApproveResponse
        ),
        schema_doc!(
            "memory_candidates_reject_params.json",
            MemoryCandidatesRejectParams
        ),
        schema_doc!(
            "memory_candidates_reject_response.json",
            MemoryCandidatesRejectResponse
        ),
        schema_doc!(
            "memory_candidates_edit_and_approve_params.json",
            MemoryCandidatesEditAndApproveParams
        ),
        schema_doc!(
            "memory_candidates_edit_and_approve_response.json",
            MemoryCandidatesEditAndApproveResponse
        ),
        schema_doc!(
            "memory_candidates_merge_params.json",
            MemoryCandidatesMergeParams
        ),
        schema_doc!(
            "memory_candidates_merge_response.json",
            MemoryCandidatesMergeResponse
        ),
        schema_doc!(
            "memory_candidates_suppress_similar_params.json",
            MemoryCandidatesSuppressSimilarParams
        ),
        schema_doc!(
            "memory_candidates_suppress_similar_response.json",
            MemoryCandidatesSuppressSimilarResponse
        ),
        schema_doc!("memory_change_kind.json", MemoryChangeKind),
        schema_doc!(
            "memory_changed_notification.json",
            MemoryChangedNotification
        ),
        schema_doc!(
            "memory_candidate_created_notification.json",
            MemoryCandidateCreatedNotification
        ),
        schema_doc!(
            "memory_forgotten_notification.json",
            MemoryForgottenNotification
        ),
        schema_doc!(
            "thread_episodic_workspace_id.json",
            ThreadEpisodicWorkspaceId
        ),
        schema_doc!("thread_episodic_thread_id.json", ThreadEpisodicThreadId),
        schema_doc!("thread_episodic_turn_id.json", ThreadEpisodicTurnId),
        schema_doc!("thread_episodic_item_id.json", ThreadEpisodicItemId),
        schema_doc!("thread_episodic_chunk_id.json", ThreadEpisodicChunkId),
        schema_doc!(
            "thread_episodic_source_actor_role.json",
            ThreadEpisodicSourceActorRole
        ),
        schema_doc!(
            "thread_episodic_source_context.json",
            ThreadEpisodicSourceContext
        ),
        schema_doc!(
            "thread_episodic_chunk_status.json",
            ThreadEpisodicChunkStatus
        ),
        schema_doc!("thread_episodic_visibility.json", ThreadEpisodicVisibility),
        schema_doc!(
            "thread_episodic_source_provenance.json",
            ThreadEpisodicSourceProvenance
        ),
        schema_doc!("thread_episodic_chunk.json", ThreadEpisodicChunk),
        schema_doc!("thread_episodic_search_mode.json", ThreadEpisodicSearchMode),
        schema_doc!(
            "thread_episodic_adaptive_strategy.json",
            ThreadEpisodicAdaptiveStrategy
        ),
        schema_doc!(
            "thread_episodic_score_breakdown.json",
            ThreadEpisodicScoreBreakdown
        ),
        schema_doc!(
            "thread_episodic_adaptive_diagnostics.json",
            ThreadEpisodicAdaptiveDiagnostics
        ),
        schema_doc!("thread_episodic_hit.json", ThreadEpisodicHit),
        schema_doc!(
            "thread_episodic_recall_policy_context.json",
            ThreadEpisodicRecallPolicyContext
        ),
        schema_doc!(
            "thread_episodic_recall_input.json",
            ThreadEpisodicRecallInput
        ),
        schema_doc!(
            "thread_episodic_recall_diagnostic_code.json",
            ThreadEpisodicRecallDiagnosticCode
        ),
        schema_doc!(
            "thread_episodic_recall_diagnostic.json",
            ThreadEpisodicRecallDiagnostic
        ),
        schema_doc!(
            "thread_episodic_recall_output.json",
            ThreadEpisodicRecallOutput
        ),
        schema_doc!("thread.json", Thread),
        schema_doc!("thread_status.json", ThreadStatus),
        schema_doc!("thread_mode.json", ThreadMode),
        schema_doc!("thread_origin_kind.json", ThreadOriginKind),
        schema_doc!("thread_sidebar_visibility.json", ThreadSidebarVisibility),
        schema_doc!("thread_start_params.json", ThreadStartParams),
        schema_doc!("thread_start_response.json", ThreadStartResponse),
        schema_doc!("thread_tree_params.json", ThreadTreeParams),
        schema_doc!("thread_tree_response.json", ThreadTreeResponse),
        schema_doc!("thread_update_params.json", ThreadUpdateParams),
        schema_doc!("thread_update_response.json", ThreadUpdateResponse),
        schema_doc!("thread_get_params.json", ThreadGetParams),
        schema_doc!("thread_get_response.json", ThreadGetResponse),
        schema_doc!("thread_history_event.json", ThreadHistoryEvent),
        schema_doc!(
            "thread_history_event_payload.json",
            ThreadHistoryEventPayload
        ),
        schema_doc!("thread_timeline_page_params.json", ThreadTimelinePageParams),
        schema_doc!(
            "thread_timeline_page_response.json",
            ThreadTimelinePageResponse
        ),
        schema_doc!(
            "thread_timeline_blocks_changed_notification.json",
            ThreadTimelineBlocksChangedNotification
        ),
        schema_doc!("thread_unsubscribe_params.json", ThreadUnsubscribeParams),
        schema_doc!(
            "thread_unsubscribe_response.json",
            ThreadUnsubscribeResponse
        ),
        schema_doc!("thread_unsubscribe_status.json", ThreadUnsubscribeStatus),
        schema_doc!("thread_folder.json", ThreadFolder),
        schema_doc!("thread_placement.json", ThreadPlacement),
        schema_doc!("thread_folder_create_params.json", ThreadFolderCreateParams),
        schema_doc!(
            "thread_folder_create_response.json",
            ThreadFolderCreateResponse
        ),
        schema_doc!("thread_folder_move_params.json", ThreadFolderMoveParams),
        schema_doc!("thread_folder_move_response.json", ThreadFolderMoveResponse),
        schema_doc!("thread_folder_delete_params.json", ThreadFolderDeleteParams),
        schema_doc!(
            "thread_folder_delete_response.json",
            ThreadFolderDeleteResponse
        ),
        schema_doc!("thread_agents_doc_status.json", ThreadAgentsDocStatus),
        schema_doc!(
            "thread_agents_doc_save_reason.json",
            ThreadAgentsDocSaveReason
        ),
        schema_doc!("thread_agents_doc_payload.json", ThreadAgentsDocPayload),
        schema_doc!("thread_agents_doc_summary.json", ThreadAgentsDocSummary),
        schema_doc!(
            "thread_agents_doc_resolved_payload.json",
            ThreadAgentsDocResolvedPayload
        ),
        schema_doc!(
            "thread_agents_doc_get_params.json",
            ThreadAgentsDocGetParams
        ),
        schema_doc!(
            "thread_agents_doc_get_response.json",
            ThreadAgentsDocGetResponse
        ),
        schema_doc!(
            "thread_agents_doc_save_params.json",
            ThreadAgentsDocSaveParams
        ),
        schema_doc!(
            "thread_agents_doc_save_response.json",
            ThreadAgentsDocSaveResponse
        ),
        schema_doc!(
            "thread_agents_doc_archive_params.json",
            ThreadAgentsDocArchiveParams
        ),
        schema_doc!(
            "thread_agents_doc_archive_response.json",
            ThreadAgentsDocArchiveResponse
        ),
        schema_doc!(
            "thread_agents_doc_resolve_for_thread_params.json",
            ThreadAgentsDocResolveForThreadParams
        ),
        schema_doc!(
            "thread_agents_doc_resolve_for_thread_response.json",
            ThreadAgentsDocResolveForThreadResponse
        ),
        schema_doc!(
            "thread_agents_doc_changed_notification.json",
            ThreadAgentsDocChangedNotification
        ),
        schema_doc!("thread_move_params.json", ThreadMoveParams),
        schema_doc!("thread_move_response.json", ThreadMoveResponse),
        schema_doc!(
            "thread_started_notification.json",
            ThreadStartedNotification
        ),
        schema_doc!("thread_closed_notification.json", ThreadClosedNotification),
        schema_doc!(
            "thread_updated_notification.json",
            ThreadUpdatedNotification
        ),
        schema_doc!(
            "thread_tree_changed_notification.json",
            ThreadTreeChangedNotification
        ),
        schema_doc!("task_value.json", TaskValue),
        schema_doc!("task.json", Task),
        schema_doc!("task_status.json", TaskStatus),
        schema_doc!("task_attachment_mode.json", TaskAttachmentMode),
        schema_doc!("task_parent_terminal_action.json", TaskParentTerminalAction),
        schema_doc!("task_completion_behavior.json", TaskCompletionBehavior),
        schema_doc!("task_lifecycle_policy.json", TaskLifecyclePolicy),
        schema_doc!("task_delivery_mode.json", TaskDeliveryMode),
        schema_doc!("task_delivery_format.json", TaskDeliveryFormat),
        schema_doc!("task_delivery_policy.json", TaskDeliveryPolicy),
        schema_doc!("task_delivery_status.json", TaskDeliveryStatus),
        schema_doc!(
            "task_delivery_attempt_status.json",
            TaskDeliveryAttemptStatus
        ),
        schema_doc!("task_delivery.json", TaskDelivery),
        schema_doc!("task_delivery_attempt.json", TaskDeliveryAttempt),
        schema_doc!("task_retry_backoff_kind.json", TaskRetryBackoffKind),
        schema_doc!("task_retry_policy.json", TaskRetryPolicy),
        schema_doc!("task_timeout_policy.json", TaskTimeoutPolicy),
        schema_doc!(
            "task_concurrency_conflict_policy.json",
            TaskConcurrencyConflictPolicy
        ),
        schema_doc!("task_concurrency_policy.json", TaskConcurrencyPolicy),
        schema_doc!("task_cancel_scope.json", TaskCancelScope),
        schema_doc!("task_write_lock_scope_kind.json", TaskWriteLockScopeKind),
        schema_doc!("task_write_lock_status.json", TaskWriteLockStatus),
        schema_doc!("task_write_lock.json", TaskWriteLock),
        schema_doc!("task_write_lock_conflict.json", TaskWriteLockConflict),
        schema_doc!("task_metadata.json", TaskMetadata),
        schema_doc!("task_artifact.json", TaskArtifact),
        schema_doc!("task_result.json", TaskResult),
        schema_doc!("task_error_class.json", TaskErrorClass),
        schema_doc!("task_error.json", TaskError),
        schema_doc!("task_trigger_kind.json", TaskTriggerKind),
        schema_doc!("task_manual_actor.json", TaskManualActor),
        schema_doc!(
            "task_external_trigger_filter.json",
            TaskExternalTriggerFilter
        ),
        schema_doc!(
            "task_dependency_trigger_mode.json",
            TaskDependencyTriggerMode
        ),
        schema_doc!(
            "task_dependency_trigger_policy.json",
            TaskDependencyTriggerPolicy
        ),
        schema_doc!("task_trigger_spec.json", TaskTriggerSpec),
        schema_doc!("task_trigger_status.json", TaskTriggerStatus),
        schema_doc!("task_executor_kind.json", TaskExecutorKind),
        schema_doc!("task_run_status.json", TaskRunStatus),
        schema_doc!("task_run_execution_status.json", TaskRunExecutionStatus),
        schema_doc!("task_owner_kind.json", TaskOwnerKind),
        schema_doc!("task_trigger.json", TaskTrigger),
        schema_doc!("task_run.json", TaskRun),
        schema_doc!("task_run_execution.json", TaskRunExecution),
        schema_doc!("task_agent_review_mode.json", TaskAgentReviewMode),
        schema_doc!(
            "task_result_review_resolution_strategy.json",
            TaskResultReviewResolutionStrategy
        ),
        schema_doc!("task_result_reviewer_spec.json", TaskResultReviewerSpec),
        schema_doc!("task_agent_review_policy.json", TaskAgentReviewPolicy),
        schema_doc!("task_agent_spec.json", TaskAgentSpec),
        schema_doc!("task_dependency_condition.json", TaskDependencyCondition),
        schema_doc!("task_dependency.json", TaskDependency),
        schema_doc!("task_event_payload.json", TaskEventPayload),
        schema_doc!("task_event.json", TaskEvent),
        schema_doc!("thread_lineage.json", ThreadLineage),
        schema_doc!("task_thread_lineage.json", TaskThreadLineage),
        schema_doc!("task_run_thread_binding.json", TaskRunThreadBinding),
        schema_doc!("task_run_turn.json", TaskRunTurn),
        schema_doc!("task_result_candidate.json", TaskResultCandidate),
        schema_doc!("task_result_review_event.json", TaskResultReviewEvent),
        schema_doc!("task_tree.json", TaskTree),
        schema_doc!("task_trigger_input.json", TaskTriggerInput),
        schema_doc!("task_agent_input_variable.json", TaskAgentInputVariable),
        schema_doc!(
            "task_agent_input_attachment_kind.json",
            TaskAgentInputAttachmentKind
        ),
        schema_doc!("task_agent_input_attachment.json", TaskAgentInputAttachment),
        schema_doc!(
            "task_agent_input_reference_kind.json",
            TaskAgentInputReferenceKind
        ),
        schema_doc!("task_agent_input_reference.json", TaskAgentInputReference),
        schema_doc!("task_agent_input.json", TaskAgentInput),
        schema_doc!("task_agent_prompt.json", TaskAgentPrompt),
        schema_doc!("task_agent_context_mode.json", TaskAgentContextMode),
        schema_doc!("task_agent_context.json", TaskAgentContext),
        schema_doc!("task_agent_context_policy.json", TaskAgentContextPolicy),
        schema_doc!("task_agent_write_mode.json", TaskAgentWriteMode),
        schema_doc!("task_agent_tool_policy.json", TaskAgentToolPolicy),
        schema_doc!("task_agent_result_format.json", TaskAgentResultFormat),
        schema_doc!("task_schema.json", TaskSchema),
        schema_doc!("task_agent_result_contract.json", TaskAgentResultContract),
        schema_doc!("task_agent_spec_input.json", TaskAgentSpecInput),
        schema_doc!("task_create_params.json", TaskCreateParams),
        schema_doc!("task_create_response.json", TaskCreateResponse),
        schema_doc!("task_get_params.json", TaskGetParams),
        schema_doc!("task_get_response.json", TaskGetResponse),
        schema_doc!("task_list_params.json", TaskListParams),
        schema_doc!("task_list_response.json", TaskListResponse),
        schema_doc!("task_tree_params.json", TaskTreeParams),
        schema_doc!("task_tree_response.json", TaskTreeResponse),
        schema_doc!("task_events_params.json", TaskEventsParams),
        schema_doc!("task_events_response.json", TaskEventsResponse),
        schema_doc!("task_accept_params.json", TaskAcceptParams),
        schema_doc!("task_accept_response.json", TaskAcceptResponse),
        schema_doc!("task_revise_params.json", TaskReviseParams),
        schema_doc!("task_revise_response.json", TaskReviseResponse),
        schema_doc!("task_cancel_params.json", TaskCancelParams),
        schema_doc!("task_cancel_response.json", TaskCancelResponse),
        schema_doc!("task_update_params.json", TaskUpdateParams),
        schema_doc!("task_update_response.json", TaskUpdateResponse),
        schema_doc!("task_reschedule_params.json", TaskRescheduleParams),
        schema_doc!("task_reschedule_response.json", TaskRescheduleResponse),
        schema_doc!("task_pause_params.json", TaskPauseParams),
        schema_doc!("task_pause_response.json", TaskPauseResponse),
        schema_doc!("task_resume_params.json", TaskResumeParams),
        schema_doc!("task_resume_response.json", TaskResumeResponse),
        schema_doc!("task_detach_params.json", TaskDetachParams),
        schema_doc!("task_detach_response.json", TaskDetachResponse),
        schema_doc!("task_wait_params.json", TaskWaitParams),
        schema_doc!("task_wait_item.json", TaskWaitItem),
        schema_doc!("task_wait_review_action.json", TaskWaitReviewAction),
        schema_doc!(
            "task_wait_revision_blocked_reason.json",
            TaskWaitRevisionBlockedReason
        ),
        schema_doc!("task_wait_review_item.json", TaskWaitReviewItem),
        schema_doc!("task_wait_response.json", TaskWaitResponse),
        schema_doc!("task_agenda_params.json", TaskAgendaParams),
        schema_doc!("task_agenda_item.json", TaskAgendaItem),
        schema_doc!("task_agenda_response.json", TaskAgendaResponse),
        schema_doc!("task_deliveries_params.json", TaskDeliveriesParams),
        schema_doc!("task_deliveries_response.json", TaskDeliveriesResponse),
        schema_doc!("task_notification_context.json", TaskNotificationContext),
        schema_doc!("task_progress_details.json", TaskProgressDetails),
        schema_doc!("task_created_notification.json", TaskCreatedNotification),
        schema_doc!(
            "task_scheduled_notification.json",
            TaskScheduledNotification
        ),
        schema_doc!("task_queued_notification.json", TaskQueuedNotification),
        schema_doc!(
            "task_run_created_notification.json",
            TaskRunCreatedNotification
        ),
        schema_doc!(
            "task_run_started_notification.json",
            TaskRunStartedNotification
        ),
        schema_doc!("task_progress_notification.json", TaskProgressNotification),
        schema_doc!(
            "task_run_completed_notification.json",
            TaskRunCompletedNotification
        ),
        schema_doc!(
            "task_run_failed_notification.json",
            TaskRunFailedNotification
        ),
        schema_doc!(
            "task_completed_notification.json",
            TaskCompletedNotification
        ),
        schema_doc!("task_failed_notification.json", TaskFailedNotification),
        schema_doc!(
            "task_cancelled_notification.json",
            TaskCancelledNotification
        ),
        schema_doc!("task_detached_notification.json", TaskDetachedNotification),
        schema_doc!("task_updated_notification.json", TaskUpdatedNotification),
        schema_doc!(
            "task_rescheduled_notification.json",
            TaskRescheduledNotification
        ),
        schema_doc!("task_paused_notification.json", TaskPausedNotification),
        schema_doc!("task_resumed_notification.json", TaskResumedNotification),
        schema_doc!(
            "task_delivery_queued_notification.json",
            TaskDeliveryQueuedNotification
        ),
        schema_doc!(
            "task_delivery_started_notification.json",
            TaskDeliveryStartedNotification
        ),
        schema_doc!(
            "task_delivery_delivered_notification.json",
            TaskDeliveryDeliveredNotification
        ),
        schema_doc!(
            "task_delivery_failed_notification.json",
            TaskDeliveryFailedNotification
        ),
        schema_doc!(
            "task_delivery_cancelled_notification.json",
            TaskDeliveryCancelledNotification
        ),
        schema_doc!(
            "task_tree_changed_notification.json",
            TaskTreeChangedTaskNotification
        ),
        schema_doc!(
            "task_recovered_notification.json",
            TaskRecoveredNotification
        ),
        schema_doc!("task_turn_item.json", TaskTurnItem),
        schema_doc!("turn.json", Turn),
        schema_doc!("prompt_manifest.json", PromptManifest),
        schema_doc!("prompt_manifest_diagnostic.json", PromptManifestDiagnostic),
        schema_doc!(
            "prompt_manifest_diagnostic_code.json",
            PromptManifestDiagnosticCode
        ),
        schema_doc!(
            "prompt_manifest_hook_contribution_kind.json",
            PromptManifestHookContributionKind
        ),
        schema_doc!("prompt_manifest_hook_phase.json", PromptManifestHookPhase),
        schema_doc!("prompt_manifest_hook_source.json", PromptManifestHookSource),
        schema_doc!(
            "prompt_manifest_hook_source_entry.json",
            PromptManifestHookSourceEntry
        ),
        schema_doc!(
            "prompt_manifest_hook_truncation.json",
            PromptManifestHookTruncation
        ),
        schema_doc!("prompt_manifest_profile.json", PromptManifestProfile),
        schema_doc!("turn_status.json", TurnStatus),
        schema_doc!("turn_start_params.json", TurnStartParams),
        schema_doc!("voice_status.json", VoiceStatus),
        schema_doc!("voice_status_params.json", VoiceStatusParams),
        schema_doc!("voice_status_response.json", VoiceStatusResponse),
        schema_doc!("voice_error_kind.json", VoiceErrorKind),
        schema_doc!("voice_error.json", VoiceError),
        schema_doc!("voice_audio_encoding.json", VoiceAudioEncoding),
        schema_doc!("voice_audio_format.json", VoiceAudioFormat),
        schema_doc!("voice_chunk_frame_header.json", VoiceChunkFrameHeader),
        schema_doc!("voice_turn_context.json", VoiceTurnContext),
        schema_doc!("voice_session_start_params.json", VoiceSessionStartParams),
        schema_doc!(
            "voice_session_start_response.json",
            VoiceSessionStartResponse
        ),
        schema_doc!(
            "voice_session_finalize_params.json",
            VoiceSessionFinalizeParams
        ),
        schema_doc!(
            "voice_session_finalize_response.json",
            VoiceSessionFinalizeResponse
        ),
        schema_doc!("voice_session_cancel_params.json", VoiceSessionCancelParams),
        schema_doc!(
            "voice_session_cancel_response.json",
            VoiceSessionCancelResponse
        ),
        schema_doc!("voice_session_outcome.json", VoiceSessionOutcome),
        schema_doc!(
            "voice_session_result_notification.json",
            VoiceSessionResultNotification
        ),
        schema_doc!(
            "voice_chunk_ack_notification.json",
            VoiceChunkAckNotification
        ),
        schema_doc!("turn_reasoning_selection.json", TurnReasoningSelection),
        schema_doc!("turn_permission_mode.json", TurnPermissionMode),
        schema_doc!("turn_permission_action_kind.json", TurnPermissionActionKind),
        schema_doc!(
            "turn_permission_decision_reason.json",
            TurnPermissionDecisionReason
        ),
        schema_doc!(
            "turn_permission_profile_selection.json",
            TurnPermissionProfileSelection
        ),
        schema_doc!("turn_permission_profile_cap.json", TurnPermissionProfileCap),
        schema_doc!(
            "turn_permission_profile_snapshot.json",
            TurnPermissionProfileSnapshot
        ),
        schema_doc!(
            "turn_permission_profile_source.json",
            TurnPermissionProfileSource
        ),
        schema_doc!(
            "turn_permission_approval_request.json",
            TurnPermissionApprovalRequest
        ),
        schema_doc!(
            "turn_permission_approval_request_detail.json",
            TurnPermissionApprovalRequestDetail
        ),
        schema_doc!(
            "turn_permission_approval_resolution.json",
            TurnPermissionApprovalResolution
        ),
        schema_doc!(
            "turn_permission_request_opened_notification.json",
            TurnPermissionRequestOpenedNotification
        ),
        schema_doc!(
            "turn_permission_request_resolved_notification.json",
            TurnPermissionRequestResolvedNotification
        ),
        schema_doc!(
            "turn_permission_request_respond_params.json",
            TurnPermissionRequestRespondParams
        ),
        schema_doc!(
            "turn_permission_request_respond_response.json",
            TurnPermissionRequestRespondResponse
        ),
        schema_doc!(
            "tool_permission_policy_snapshot.json",
            ToolPermissionPolicySnapshot
        ),
        schema_doc!("permission_behavior.json", PermissionBehavior),
        schema_doc!("agent_execution_backend.json", AgentExecutionBackend),
        schema_doc!("cli_agent_runtime_kind.json", CLIAgentRuntimeKind),
        schema_doc!(
            "cli_agent_runtime_sandbox_policy.json",
            CLIAgentRuntimeSandboxPolicy
        ),
        schema_doc!("turn_cli_runtime_options.json", TurnCLIRuntimeOptions),
        schema_doc!("runtime_summary.json", RuntimeSummary),
        schema_doc!("runtime_status.json", RuntimeStatus),
        schema_doc!("runtime_model_info.json", RuntimeModelInfo),
        schema_doc!("runtime_app_info.json", RuntimeAppInfo),
        schema_doc!("runtime_capabilities.json", RuntimeCapabilities),
        schema_doc!("runtime_account_snapshot.json", RuntimeAccountSnapshot),
        schema_doc!("runtime_diagnostic.json", RuntimeDiagnostic),
        schema_doc!("runtime_diagnostic_level.json", RuntimeDiagnosticLevel),
        schema_doc!("cli_runtime_pending_request.json", CLIRuntimePendingRequest),
        schema_doc!("cli_runtime_request_kind.json", CLIRuntimeRequestKind),
        schema_doc!(
            "cli_runtime_request_resolution.json",
            CLIRuntimeRequestResolution
        ),
        schema_doc!("cli_runtime_list_params.json", CLIRuntimeListParams),
        schema_doc!("cli_runtime_list_response.json", CLIRuntimeListResponse),
        schema_doc!("cli_runtime_get_params.json", CLIRuntimeGetParams),
        schema_doc!("cli_runtime_get_response.json", CLIRuntimeGetResponse),
        schema_doc!("cli_runtime_status_params.json", CLIRuntimeStatusParams),
        schema_doc!("cli_runtime_status_response.json", CLIRuntimeStatusResponse),
        schema_doc!("cli_runtime_refresh_params.json", CLIRuntimeRefreshParams),
        schema_doc!(
            "cli_runtime_refresh_response.json",
            CLIRuntimeRefreshResponse
        ),
        schema_doc!(
            "cli_runtime_list_models_params.json",
            CLIRuntimeListModelsParams
        ),
        schema_doc!(
            "cli_runtime_list_models_response.json",
            CLIRuntimeListModelsResponse
        ),
        schema_doc!(
            "cli_runtime_thread_binding_get_params.json",
            CLIRuntimeThreadBindingGetParams
        ),
        schema_doc!(
            "cli_runtime_thread_binding_get_response.json",
            CLIRuntimeThreadBindingGetResponse
        ),
        schema_doc!("cli_runtime_thread_binding.json", CLIRuntimeThreadBinding),
        schema_doc!(
            "cli_runtime_thread_fork_params.json",
            CLIRuntimeThreadForkParams
        ),
        schema_doc!(
            "cli_runtime_thread_fork_response.json",
            CLIRuntimeThreadForkResponse
        ),
        schema_doc!(
            "cli_runtime_thread_compact_params.json",
            CLIRuntimeThreadCompactParams
        ),
        schema_doc!(
            "cli_runtime_thread_compact_response.json",
            CLIRuntimeThreadCompactResponse
        ),
        schema_doc!(
            "cli_runtime_turn_steer_params.json",
            CLIRuntimeTurnSteerParams
        ),
        schema_doc!(
            "cli_runtime_turn_steer_response.json",
            CLIRuntimeTurnSteerResponse
        ),
        schema_doc!("cli_runtime_review_delivery.json", CLIRuntimeReviewDelivery),
        schema_doc!("cli_runtime_review_target.json", CLIRuntimeReviewTarget),
        schema_doc!(
            "cli_runtime_review_start_params.json",
            CLIRuntimeReviewStartParams
        ),
        schema_doc!(
            "cli_runtime_review_start_response.json",
            CLIRuntimeReviewStartResponse
        ),
        schema_doc!(
            "cli_runtime_login_start_type.json",
            CLIRuntimeLoginStartType
        ),
        schema_doc!(
            "cli_runtime_login_start_params.json",
            CLIRuntimeLoginStartParams
        ),
        schema_doc!(
            "cli_runtime_login_start_response.json",
            CLIRuntimeLoginStartResponse
        ),
        schema_doc!(
            "cli_runtime_login_cancel_params.json",
            CLIRuntimeLoginCancelParams
        ),
        schema_doc!(
            "cli_runtime_login_cancel_response.json",
            CLIRuntimeLoginCancelResponse
        ),
        schema_doc!(
            "cli_runtime_request_respond_params.json",
            CLIRuntimeRequestRespondParams
        ),
        schema_doc!(
            "cli_runtime_request_respond_response.json",
            CLIRuntimeRequestRespondResponse
        ),
        schema_doc!(
            "cli_runtime_pending_request_status.json",
            CLIRuntimePendingRequestStatus
        ),
        schema_doc!(
            "cli_runtime_status_changed_notification.json",
            CLIRuntimeStatusChangedNotification
        ),
        schema_doc!(
            "cli_runtime_account_updated_notification.json",
            CLIRuntimeAccountUpdatedNotification
        ),
        schema_doc!(
            "cli_runtime_request_opened_notification.json",
            CLIRuntimeRequestOpenedNotification
        ),
        schema_doc!(
            "cli_runtime_request_resolved_notification.json",
            CLIRuntimeRequestResolvedNotification
        ),
        schema_doc!(
            "cli_runtime_apps_changed_notification.json",
            CLIRuntimeAppsChangedNotification
        ),
        schema_doc!("turn_capability.json", TurnCapability),
        schema_doc!("turn_capability_kind.json", TurnCapabilityKind),
        schema_doc!(
            "turn_skill_capability_summary.json",
            TurnSkillCapabilitySummary
        ),
        schema_doc!(
            "turn_mcp_server_capability_summary.json",
            TurnMcpServerCapabilitySummary
        ),
        schema_doc!(
            "turn_mcp_tool_capability_summary.json",
            TurnMcpToolCapabilitySummary
        ),
        schema_doc!("turn_start_response.json", TurnStartResponse),
        schema_doc!("turn_cancel_params.json", TurnCancelParams),
        schema_doc!("turn_cancel_response.json", TurnCancelResponse),
        schema_doc!("turn_resume_params.json", TurnResumeParams),
        schema_doc!("turn_resume_response.json", TurnResumeResponse),
        schema_doc!("turn_get_params.json", TurnGetParams),
        schema_doc!("turn_get_response.json", TurnGetResponse),
        schema_doc!("turn_items_params.json", TurnItemsParams),
        schema_doc!("turn_item_event.json", TurnItemEvent),
        schema_doc!("turn_item_event_payload.json", TurnItemEventPayload),
        schema_doc!("turn_items_response.json", TurnItemsResponse),
        schema_doc!("timeline_cursor.json", TimelineCursor),
        schema_doc!("timeline_page_anchor.json", TimelinePageAnchor),
        schema_doc!("timeline_page_info.json", TimelinePageInfo),
        schema_doc!("timeline_block.json", TimelineBlock),
        schema_doc!("timeline_block_kind.json", TimelineBlockKind),
        schema_doc!("timeline_change_reason.json", TimelineChangeReason),
        schema_doc!("turn_work_block.json", TurnWorkBlock),
        schema_doc!("turn_work_presentation.json", TurnWorkPresentation),
        schema_doc!("turn_work_state.json", TurnWorkState),
        schema_doc!("turn_work_item.json", TurnWorkItem),
        schema_doc!("turn_work_item_status.json", TurnWorkItemStatus),
        schema_doc!("turn_work_page_params.json", TurnWorkPageParams),
        schema_doc!("turn_work_page_response.json", TurnWorkPageResponse),
        schema_doc!(
            "turn_work_items_changed_notification.json",
            TurnWorkItemsChangedNotification
        ),
        schema_doc!(
            "turn_work_state_changed_notification.json",
            TurnWorkStateChangedNotification
        ),
        schema_doc!("turn_kind.json", TurnKind),
        schema_doc!("turn_origin.json", TurnOrigin),
        schema_doc!("agent_message_phase.json", AgentMessagePhase),
        schema_doc!("turn_item.json", TurnItem),
        schema_doc!(
            "execution_checkpoint_payload.json",
            ExecutionCheckpointPayload
        ),
        schema_doc!(
            "execution_checkpoint_original_request_summary.json",
            ExecutionCheckpointOriginalRequestSummary
        ),
        schema_doc!(
            "execution_checkpoint_window_summary.json",
            ExecutionCheckpointWindowSummary
        ),
        schema_doc!(
            "execution_checkpoint_provider_budget_summary.json",
            ExecutionCheckpointProviderBudgetSummary
        ),
        schema_doc!(
            "execution_checkpoint_tool_summary.json",
            ExecutionCheckpointToolSummary
        ),
        schema_doc!(
            "execution_checkpoint_tool_call_summary.json",
            ExecutionCheckpointToolCallSummary
        ),
        schema_doc!(
            "execution_checkpoint_strict_obligation.json",
            ExecutionCheckpointStrictObligation
        ),
        schema_doc!("turn_started_notification.json", TurnStartedNotification),
        schema_doc!(
            "turn_completed_notification.json",
            TurnCompletedNotification
        ),
        schema_doc!("turn_failed_notification.json", TurnFailedNotification),
        schema_doc!("turn_blocked_notification.json", TurnBlockedNotification),
        schema_doc!(
            "turn_blocked_resume_metadata.json",
            TurnBlockedResumeMetadata
        ),
        schema_doc!(
            "turn_status_changed_notification.json",
            TurnStatusChangedNotification
        ),
        schema_doc!("item_started_notification.json", ItemStartedNotification),
        schema_doc!("item_delta_notification.json", ItemDeltaNotification),
        schema_doc!(
            "item_completed_notification.json",
            ItemCompletedNotification
        ),
        schema_doc!("item_updated_notification.json", ItemUpdatedNotification),
        schema_doc!(
            "item_timeout_detected_notification.json",
            ItemTimeoutDetectedNotification
        ),
        schema_doc!(
            "item_recovery_opened_notification.json",
            ItemRecoveryOpenedNotification
        ),
        schema_doc!(
            "item_recovery_attached_notification.json",
            ItemRecoveryAttachedNotification
        ),
        schema_doc!(
            "item_retry_scheduled_notification.json",
            ItemRetryScheduledNotification
        ),
        schema_doc!(
            "item_retry_attempt_started_notification.json",
            ItemRetryAttemptStartedNotification
        ),
        schema_doc!(
            "item_recovery_succeeded_notification.json",
            ItemRecoverySucceededNotification
        ),
        schema_doc!(
            "item_recovery_exhausted_notification.json",
            ItemRecoveryExhaustedNotification
        ),
        schema_doc!(
            "item_tool_retry_scheduled_notification.json",
            ItemToolRetryScheduledNotification
        ),
        schema_doc!(
            "item_tool_retry_resolved_notification.json",
            ItemToolRetryResolvedNotification
        ),
        schema_doc!(
            "item_tool_retry_exhausted_notification.json",
            ItemToolRetryExhaustedNotification
        ),
        schema_doc!(
            "turn_tool_loop_budget_exceeded_notification.json",
            TurnToolLoopBudgetExceededNotification
        ),
        schema_doc!(
            "turn_execution_window_started_notification.json",
            TurnExecutionWindowStartedNotification
        ),
        schema_doc!(
            "turn_execution_window_exhausted_notification.json",
            TurnExecutionWindowExhaustedNotification
        ),
        schema_doc!(
            "turn_execution_window_checkpointed_notification.json",
            TurnExecutionWindowCheckpointedNotification
        ),
        schema_doc!(
            "turn_execution_window_continued_notification.json",
            TurnExecutionWindowContinuedNotification
        ),
        schema_doc!(
            "turn_execution_window_blocked_notification.json",
            TurnExecutionWindowBlockedNotification
        ),
        schema_doc!("execution_window_status.json", ExecutionWindowStatus),
        schema_doc!(
            "execution_window_exhaustion_reason.json",
            ExecutionWindowExhaustionReason
        ),
        schema_doc!("tool_retry_error_class.json", ToolRetryErrorClass),
        schema_doc!("tool_retry_budget_kind.json", ToolRetryBudgetKind),
        schema_doc!("tool_retry_budget_usage.json", ToolRetryBudgetUsage),
        schema_doc!("tool_retry_resolution.json", ToolRetryResolution),
        schema_doc!("tool_retry_exhaustion_kind.json", ToolRetryExhaustionKind),
        schema_doc!("tool_loop_budget_limit_kind.json", ToolLoopBudgetLimitKind),
        schema_doc!("tool_loop_budget_action.json", ToolLoopBudgetAction),
        schema_doc!(
            "tool_recovery_policy_snapshot.json",
            ToolRecoveryPolicySnapshot
        ),
        schema_doc!("tool_output_policy_snapshot.json", ToolOutputPolicySnapshot),
        schema_doc!("tool_metadata.json", ToolMetadata),
        schema_doc!("tool_metadata_value.json", ToolMetadataValue),
        schema_doc!("tool_metadata_raw_kind.json", ToolMetadataRawKind),
        schema_doc!("tool_output_summary.json", ToolOutputSummary),
        schema_doc!("tool_display_payload.json", ToolDisplayPayload),
        schema_doc!("tool_storage_payload.json", ToolStoragePayload),
        schema_doc!("tool_recovery_view.json", ToolRecoveryView),
        schema_doc!("tool_recovery_retry_class.json", ToolRecoveryRetryClass),
        schema_doc!(
            "tool_recovery_idempotency_mode.json",
            ToolRecoveryIdempotencyMode
        ),
        schema_doc!("turn_item_type.json", TurnItemType),
        schema_doc!("turn_item_attempt_status.json", TurnItemAttemptStatus),
        schema_doc!("turn_item_timeout_reason.json", TurnItemTimeoutReason),
        schema_doc!("provider_failure_class.json", ProviderFailureClass),
        schema_doc!("provider_transport_kind.json", ProviderTransportKind),
        schema_doc!("provider_failure_stage.json", ProviderFailureStage),
        schema_doc!("provider_failure_details.json", ProviderFailureDetails),
        schema_doc!("provider_list_params.json", ProviderListParams),
        schema_doc!("provider_list_response.json", ProviderListResponse),
        schema_doc!("provider_list_models_params.json", ProviderListModelsParams),
        schema_doc!(
            "provider_list_models_response.json",
            ProviderListModelsResponse
        ),
        schema_doc!(
            "gateway_memory_model_selection_source.json",
            GatewayMemoryModelSelectionSource
        ),
        schema_doc!(
            "gateway_memory_model_selection.json",
            GatewayMemoryModelSelection
        ),
        schema_doc!("gateway_memory_settings.json", GatewayMemorySettings),
        schema_doc!(
            "gateway_thread_episodic_settings.json",
            GatewayThreadEpisodicSettings
        ),
        schema_doc!(
            "gateway_thread_episodic_settings_update.json",
            GatewayThreadEpisodicSettingsUpdate
        ),
        schema_doc!(
            "gateway_cli_runtime_settings.json",
            GatewayCliRuntimeSettings
        ),
        schema_doc!(
            "gateway_cli_runtime_instance_settings.json",
            GatewayCliRuntimeInstanceSettings
        ),
        schema_doc!(
            "gateway_remote_access_transport.json",
            GatewayRemoteAccessTransport
        ),
        schema_doc!("gateway_remote_access_state.json", GatewayRemoteAccessState),
        schema_doc!(
            "gateway_remote_access_error_kind.json",
            GatewayRemoteAccessErrorKind
        ),
        schema_doc!(
            "gateway_remote_access_status_snapshot.json",
            GatewayRemoteAccessStatusSnapshot
        ),
        schema_doc!(
            "gateway_remote_access_status_changed_notification.json",
            GatewayRemoteAccessStatusChangedNotification
        ),
        schema_doc!(
            "gateway_remote_access_settings.json",
            GatewayRemoteAccessSettings
        ),
        schema_doc!(
            "gateway_remote_access_settings_update.json",
            GatewayRemoteAccessSettingsUpdate
        ),
        schema_doc!("gateway_general_settings.json", GatewayGeneralSettings),
        schema_doc!(
            "gateway_general_settings_update.json",
            GatewayGeneralSettingsUpdate
        ),
        schema_doc!("gateway_settings_snapshot.json", GatewaySettingsSnapshot),
        schema_doc!("gateway_settings_update.json", GatewaySettingsUpdate),
        schema_doc!("gateway_settings_get_params.json", GatewaySettingsGetParams),
        schema_doc!(
            "gateway_settings_get_response.json",
            GatewaySettingsGetResponse
        ),
        schema_doc!(
            "gateway_settings_update_params.json",
            GatewaySettingsUpdateParams
        ),
        schema_doc!(
            "gateway_settings_update_response.json",
            GatewaySettingsUpdateResponse
        ),
        schema_doc!(
            "context_compressing_notification.json",
            ContextCompressingNotification
        ),
        schema_doc!(
            "context_compressed_notification.json",
            ContextCompressedNotification
        ),
        schema_doc!("user_input.json", UserInput),
        schema_doc!("text_element.json", TextElement),
        schema_doc!("byte_range.json", ByteRange),
        schema_doc!("sandbox_mode.json", SandboxMode),
        schema_doc!("sandbox_policy.json", SandboxPolicy),
        schema_doc!("provider_set_api_key_params.json", ProviderSetApiKeyParams),
        schema_doc!(
            "provider_set_api_key_response.json",
            ProviderSetApiKeyResponse
        ),
        schema_doc!(
            "provider_delete_api_key_params.json",
            ProviderDeleteApiKeyParams
        ),
        schema_doc!(
            "provider_delete_api_key_response.json",
            ProviderDeleteApiKeyResponse
        ),
        schema_doc!("skills_list_params.json", SkillListParams),
        schema_doc!("skills_list_response.json", SkillListResponse),
        schema_doc!("skills_list_item.json", SkillListItem),
        schema_doc!("skill_install_state.json", SkillInstallState),
        schema_doc!("skill_policy_state.json", SkillPolicyState),
        schema_doc!("skill_health_summary.json", SkillHealthSummary),
        schema_doc!(
            "skill_dependency_diagnostic.json",
            SkillDependencyDiagnostic
        ),
        schema_doc!("skill_security_finding.json", SkillSecurityFinding),
        schema_doc!("skills_install_params.json", SkillsInstallParams),
        schema_doc!("skills_install_response.json", SkillsInstallResponse),
        schema_doc!("skills_update_params.json", SkillsUpdateParams),
        schema_doc!("skills_update_response.json", SkillsUpdateResponse),
        schema_doc!("skills_uninstall_params.json", SkillsUninstallParams),
        schema_doc!("skills_uninstall_response.json", SkillsUninstallResponse),
        schema_doc!("skill_lifecycle_source.json", SkillLifecycleSource),
        schema_doc!("skill_archive_format.json", SkillArchiveFormat),
        schema_doc!("skills_upload_start_params.json", SkillsUploadStartParams),
        schema_doc!(
            "skills_upload_start_response.json",
            SkillsUploadStartResponse
        ),
        schema_doc!("skills_upload_finish_params.json", SkillsUploadFinishParams),
        schema_doc!(
            "skills_upload_finish_response.json",
            SkillsUploadFinishResponse
        ),
        schema_doc!("skills_upload_abort_params.json", SkillsUploadAbortParams),
        schema_doc!(
            "skills_upload_abort_response.json",
            SkillsUploadAbortResponse
        ),
        schema_doc!("skills_upload_chunk_header.json", SkillsUploadChunkHeader),
        schema_doc!(
            "skills_upload_chunk_ack_notification.json",
            SkillsUploadChunkAckNotification
        ),
        schema_doc!(
            "skill_lifecycle_result_skill.json",
            SkillLifecycleResultSkill
        ),
        schema_doc!(
            "skill_lifecycle_audit_summary.json",
            SkillLifecycleAuditSummary
        ),
        schema_doc!("skills_policy_list_params.json", SkillsPolicyListParams),
        schema_doc!("skills_policy_list_response.json", SkillsPolicyListResponse),
        schema_doc!("skill_workspace_policy.json", SkillWorkspacePolicy),
        schema_doc!("skills_policy_set_params.json", SkillsPolicySetParams),
        schema_doc!("skills_policy_set_response.json", SkillsPolicySetResponse),
        schema_doc!("skills_health_params.json", SkillsHealthParams),
        schema_doc!("skills_health_response.json", SkillsHealthResponse),
        schema_doc!("skill_health_target.json", SkillHealthTarget),
        schema_doc!("skill_health_item.json", SkillHealthItem),
        schema_doc!("skill_trust_gate_status.json", SkillTrustGateStatus),
        schema_doc!("skill_audit_timeline_item.json", SkillAuditTimelineItem),
        schema_doc!(
            "skills_changed_notification.json",
            SkillsChangedNotification
        ),
        schema_doc!("skill_changed_item.json", SkillChangedItem),
        schema_doc!("mcp_list_params.json", McpListParams),
        schema_doc!("mcp_list_response.json", McpListResponse),
        schema_doc!("mcp_list_item.json", McpListItem),
        schema_doc!("mcp_scope_kind.json", McpScopeKind),
        schema_doc!("mcp_source_kind.json", McpSourceKind),
        schema_doc!("mcp_install_params.json", McpInstallParams),
        schema_doc!("mcp_install_response.json", McpInstallResponse),
        schema_doc!("mcp_install_status.json", McpInstallStatus),
        schema_doc!("mcp_install_result.json", McpInstallResult),
        schema_doc!("mcp_install_result_status.json", McpInstallResultStatus),
        schema_doc!("mcp_policy_state.json", McpPolicyState),
        schema_doc!("mcp_policy_set_params.json", McpPolicySetParams),
        schema_doc!("mcp_policy_set_response.json", McpPolicySetResponse),
        schema_doc!("mcp_server_restart_params.json", McpServerRestartParams),
        schema_doc!("mcp_server_restart_response.json", McpServerRestartResponse),
        schema_doc!("mcp_uninstall_params.json", McpUninstallParams),
        schema_doc!("mcp_uninstall_response.json", McpUninstallResponse),
        schema_doc!("mcp_server_details_params.json", McpServerDetailsParams),
        schema_doc!("mcp_server_details_response.json", McpServerDetailsResponse),
        schema_doc!("mcp_server_catalog_details.json", McpServerCatalogDetails),
        schema_doc!("mcp_tool_catalog_item.json", McpToolCatalogItem),
        schema_doc!("mcp_tool_annotation_summary.json", McpToolAnnotationSummary),
        schema_doc!("mcp_resource_catalog_item.json", McpResourceCatalogItem),
        schema_doc!(
            "mcp_resource_template_catalog_item.json",
            McpResourceTemplateCatalogItem
        ),
        schema_doc!("mcp_prompt_catalog_item.json", McpPromptCatalogItem),
        schema_doc!("mcp_server_health_details.json", McpServerHealthDetails),
        schema_doc!("mcp_audit_event_summary.json", McpAuditEventSummary),
        schema_doc!("mcp_turn_binding_summary.json", McpTurnBindingSummary),
        schema_doc!("mcp_server_policy.json", McpServerPolicy),
        schema_doc!("mcp_transport_summary.json", McpTransportSummary),
        schema_doc!("mcp_runtime_status.json", McpRuntimeStatus),
        schema_doc!("mcp_runtime_state.json", McpRuntimeState),
        schema_doc!("mcp_server_status.json", McpServerStatus),
        schema_doc!("mcp_validation_diagnostic.json", McpValidationDiagnostic),
        schema_doc!("mcp_diagnostic_level.json", McpDiagnosticLevel),
        schema_doc!("mcp_lifecycle_audit_summary.json", McpLifecycleAuditSummary),
        schema_doc!("mcp_changed_notification.json", McpChangedNotification),
        schema_doc!("mcp_changed_item.json", McpChangedItem),
        schema_doc!("mcp_changed_action.json", McpChangedAction),
        schema_doc!(
            "mcp_server_status_changed_notification.json",
            McpServerStatusChangedNotification
        ),
        schema_doc!("mcp_server_status_item.json", McpServerStatusItem),
        schema_doc!(
            "mcp_server_catalog_changed_notification.json",
            McpServerCatalogChangedNotification
        ),
        schema_doc!("markdown_document.json", MarkdownDocument),
        schema_doc!("markdown_block.json", MarkdownBlock),
        schema_doc!("markdown_inline.json", MarkdownInline),
        schema_doc!("markdown_list.json", MarkdownList),
        schema_doc!("markdown_list_item.json", MarkdownListItem),
        schema_doc!("markdown_mark.json", MarkdownMark),
        schema_doc!("markdown_mark_kind.json", MarkdownMarkKind),
        schema_doc!("gateway_notification.json", GatewayNotification),
        schema_doc!(
            "unknown_gateway_notification.json",
            UnknownGatewayNotification
        ),
    ]
}

pub fn write_protocol_schemas(
    output_directory: impl AsRef<Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let output_directory = output_directory.as_ref();
    fs::create_dir_all(output_directory)?;

    for document in protocol_schema_documents() {
        let schema_json = serde_json::to_string_pretty(&document.schema)?;
        let path = output_directory.join(document.file_name);
        fs::write(path, schema_json)?;
    }

    Ok(())
}
