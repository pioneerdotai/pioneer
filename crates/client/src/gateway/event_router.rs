//! Typed transport classification for the process-local Client ingress.

use crate::transport::ws::GatewayWsEvent;
use pioneer_protocol::GatewayNotification;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayEventRoute {
    Connection,
    Authorization,
    Session,
    Administration,
    Workspace,
    Settings,
    Memory,
    Provider,
    PendingRequest,
    Mcp,
    Skills,
    TaskNotification,
    Unknown,
    Thread,
}

impl GatewayEventRoute {
    pub fn classify(event: &GatewayWsEvent) -> Self {
        let GatewayWsEvent::Notification { notification, .. } = event else {
            return Self::Connection;
        };
        use GatewayNotification::*;
        match notification {
            AccessChanged(_) | AuthorizationProjectionChanged(_) => Self::Authorization,
            AuthSessionRevoked(_) | AuthAccessExpiring(_) => Self::Session,
            InvitationChanged(_) | MemberChanged(_) | WorkspaceMembersChanged(_) => {
                Self::Administration
            }
            WorkspaceChanged(_) | ThreadTreeChanged(_) => Self::Workspace,
            GatewayRemoteAccessStatusChanged(_)
            | GatewayThreadEpisodicVectorRefillStatusChanged(_)
            | GatewayVoiceInputStatusChanged(_) => Self::Settings,
            MemoryChanged(_) | MemoryCandidateCreated(_) | MemoryForgotten(_) => Self::Memory,
            CLIRuntimeStatusChanged(_) | CLIRuntimeAccountUpdated(_) | CLIRuntimeAppsChanged(_) => {
                Self::Provider
            }
            CLIRuntimeRequestOpened(_)
            | CLIRuntimeRequestResolved(_)
            | TurnPermissionRequestOpened(_)
            | TurnPermissionRequestResolved(_) => Self::PendingRequest,
            McpChanged(_) | McpServerStatusChanged(_) | McpServerCatalogChanged(_) => Self::Mcp,
            SkillsChanged(_) | SkillsUploadChunkAck(_) => Self::Skills,
            TaskCreated(_)
            | TaskScheduled(_)
            | TaskQueued(_)
            | TaskRunCreated(_)
            | TaskRunStarted(_)
            | TaskProgress(_)
            | TaskRunCompleted(_)
            | TaskRunFailed(_)
            | TaskRunBlocked(_)
            | TaskRunCancelled(_)
            | TaskCompleted(_)
            | TaskFailed(_)
            | TaskBlocked(_)
            | TaskCancelled(_)
            | TaskDetached(_)
            | TaskUpdated(_)
            | TaskRescheduled(_)
            | TaskPaused(_)
            | TaskResumed(_)
            | TaskDeliveryQueued(_)
            | TaskDeliveryStarted(_)
            | TaskDeliveryDelivered(_)
            | TaskDeliveryFailed(_)
            | TaskDeliveryCancelled(_)
            | TaskUserNotificationDelivered(_)
            | TaskTreeChanged(_)
            | TaskRecovered(_) => Self::TaskNotification,
            Unknown(_) => Self::Unknown,
            ThreadStarted(_)
            | ThreadClosed(_)
            | ThreadUpdated(_)
            | ThreadParticipantsChanged(_)
            | ThreadAgentsDocChanged(_)
            | ThreadTimelineBlocksChanged(_)
            | ThreadReadCursorChanged(_)
            | TurnStarted(_)
            | TurnCompleted(_)
            | TurnFailed(_)
            | TurnBlocked(_)
            | TurnWorkItemsChanged(_)
            | TurnWorkStateChanged(_)
            | TurnExecutionWindowStarted(_)
            | TurnExecutionWindowExhausted(_)
            | TurnExecutionWindowCheckpointed(_)
            | TurnExecutionWindowContinued(_)
            | TurnExecutionWindowBlocked(_)
            | ItemStarted(_)
            | ItemDelta(_)
            | ItemTimeoutDetected(_)
            | ItemRecoveryOpened(_)
            | ItemRecoveryAttached(_)
            | ItemRetryScheduled(_)
            | ItemRetryAttemptStarted(_)
            | ItemRecoverySucceeded(_)
            | ItemRecoveryExhausted(_)
            | ItemToolRetryScheduled(_)
            | ItemToolRetryResolved(_)
            | ItemToolRetryExhausted(_)
            | ItemCompleted(_)
            | ItemUpdated(_)
            | TurnToolLoopBudgetExceeded(_)
            | ContextCompressing(_)
            | ContextCompressed(_)
            | ArtifactCreated(_)
            | ArtifactUpdated(_)
            | ArtifactDeleted(_)
            | ThreadArtifactsChanged(_)
            | ArtifactProjectionUpdated(_)
            | ArtifactUploadProgress(_)
            | VoiceSessionResult(_) => Self::Thread,
        }
    }
}
