//! Shared client runtime primitives.
//!
//! This module owns shell-neutral orchestration that sits above the websocket
//! transport. Shell crates still own rendering, localization, dialogs, and
//! platform adapters, but websocket event filtering and protocol event
//! reduction belong here so desktop and mobile do not grow separate client
//! loops.

use crate::{
    notifications::router::{
        ArtifactDeletedRefreshReduction, ArtifactThreadRefreshReduction,
        ConversationEventReduction, McpRefreshReduction, McpServerCatalogChangedReduction,
        McpServerStatusChangedReduction, SkillsRefreshReduction, ThreadArtifactsRefreshReduction,
        ThreadClosedReduction, ThreadStartedContext, ThreadStartedReduction,
        ThreadUpdatedReduction, TurnLifecycleReduction, TurnTimelineRefreshReduction,
        WorkspacePreferenceReduction, WorkspaceRefreshReduction,
        apply_workspace_changed_to_catalog, reduce_artifact_created_notification,
        reduce_artifact_deleted_notification, reduce_artifact_updated_notification,
        reduce_item_completed_notification, reduce_item_delta_notification,
        reduce_item_recovery_attached_notification, reduce_item_recovery_exhausted_notification,
        reduce_item_recovery_opened_notification, reduce_item_recovery_succeeded_notification,
        reduce_item_retry_attempt_started_notification, reduce_item_retry_scheduled_notification,
        reduce_item_started_notification, reduce_item_timeout_detected_notification,
        reduce_item_tool_retry_exhausted_notification,
        reduce_item_tool_retry_resolved_notification,
        reduce_item_tool_retry_scheduled_notification, reduce_item_updated_notification,
        reduce_mcp_changed_notification, reduce_mcp_server_catalog_changed_notification,
        reduce_mcp_server_status_changed_notification, reduce_skills_changed_notification,
        reduce_thread_agents_doc_changed_notification,
        reduce_thread_artifacts_changed_notification, reduce_thread_closed_notification,
        reduce_thread_started_notification, reduce_thread_tree_changed_notification,
        reduce_thread_updated_notification, reduce_turn_blocked_notification,
        reduce_turn_completed_notification, reduce_turn_execution_window_blocked_notification,
        reduce_turn_execution_window_checkpointed_notification,
        reduce_turn_execution_window_continued_notification,
        reduce_turn_execution_window_exhausted_notification,
        reduce_turn_execution_window_started_notification, reduce_turn_failed_notification,
        reduce_turn_started_notification, reduce_turn_timeline_changed_notification,
        reduce_turn_tool_loop_budget_exceeded_notification,
        reduce_workspace_preference_after_catalog_change,
    },
    state::reducers::{
        GatewayConnectionEvent, GatewayConnectionReduction, reduce_gateway_connection_event,
    },
    transport::ws::{
        GatewayWsClient, GatewayWsCommandSender, GatewayWsEvent, should_apply_ws_event,
    },
};
use pioneer_protocol::{
    ArtifactSummary, GatewayNotification, Workspace, WorkspaceChangedNotification,
};

#[derive(Clone)]
pub struct ClientRuntime {
    ws_client: GatewayWsClient,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClientRuntimeWsEventContext {
    pub queue_skills_refresh: bool,
    pub should_resume_in_flight_turn: bool,
}

#[derive(Clone, Debug)]
pub enum ClientRuntimeWsEvent {
    Connection(GatewayConnectionReduction),
    Notification(GatewayNotification),
}

#[derive(Clone, Copy, Debug)]
pub struct ClientRuntimeNotificationContext<'a> {
    pub pending_thread_id: Option<&'a str>,
    pub active_thread_id: Option<&'a str>,
    pub active_workspace_id: Option<&'a str>,
    pub notification_thread_workspace_matches: bool,
    pub active_thread_artifacts: &'a [ArtifactSummary],
    pub preferred_workspace_id: Option<&'a str>,
    pub workspaces: &'a [Workspace],
    pub mcp_workspace_id: Option<&'a str>,
    pub mcp_selected_server_id: Option<&'a str>,
    pub mcp_details_loaded: bool,
}

impl Default for ClientRuntimeNotificationContext<'_> {
    fn default() -> Self {
        Self {
            pending_thread_id: None,
            active_thread_id: None,
            active_workspace_id: None,
            notification_thread_workspace_matches: false,
            active_thread_artifacts: &[],
            preferred_workspace_id: None,
            workspaces: &[],
            mcp_workspace_id: None,
            mcp_selected_server_id: None,
            mcp_details_loaded: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum ClientRuntimeNotification {
    ThreadStarted(ThreadStartedReduction),
    TurnLifecycle(TurnLifecycleReduction),
    ConversationEvent(ConversationEventReduction),
    ThreadClosed(ThreadClosedReduction),
    WorkspaceRefresh(WorkspaceRefreshReduction),
    ThreadUpdated(ThreadUpdatedReduction),
    SkillsRefresh(SkillsRefreshReduction),
    McpRefresh(McpRefreshReduction),
    McpServerStatusChanged(McpServerStatusChangedReduction),
    McpServerCatalogChanged(McpServerCatalogChangedReduction),
    ThreadArtifactsRefresh(ThreadArtifactsRefreshReduction),
    ArtifactThreadRefresh(ArtifactThreadRefreshReduction),
    ArtifactDeletedRefresh(ArtifactDeletedRefreshReduction),
    TurnTimelineRefresh(TurnTimelineRefreshReduction),
    WorkspaceChanged {
        notification: WorkspaceChangedNotification,
        preference: WorkspacePreferenceReduction,
    },
}

pub trait ClientRuntimePostEventSink {
    fn refresh_thread_list_if_requested(&mut self) -> bool;
    fn refresh_skills_if_requested(&mut self) -> bool;
    fn refresh_mcp_if_requested(&mut self) -> bool;
    fn refresh_mcp_details_if_requested(&mut self) -> bool;
    fn drive_thread_start_queue(&mut self) -> bool;
    fn drive_turn_resume_queue(&mut self) -> bool;
    fn tick_thread_conversations(&mut self) -> bool;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClientRuntimePostEventOutcome {
    pub events_applied: bool,
    pub refreshed_thread_list: bool,
    pub refreshed_skills: bool,
    pub refreshed_mcp: bool,
    pub refreshed_mcp_details: bool,
    pub drove_thread_start: bool,
    pub drove_turn_resume: bool,
    pub ticked_thread_conversations: bool,
}

impl ClientRuntimePostEventOutcome {
    pub fn should_notify(self) -> bool {
        self.events_applied
            || self.refreshed_thread_list
            || self.refreshed_skills
            || self.refreshed_mcp
            || self.refreshed_mcp_details
            || self.drove_thread_start
            || self.drove_turn_resume
            || self.ticked_thread_conversations
    }
}

impl ClientRuntime {
    pub fn new() -> Self {
        Self {
            ws_client: GatewayWsClient::new(),
        }
    }

    pub fn ws_command_sender(&self) -> GatewayWsCommandSender {
        self.ws_client.command_sender()
    }

    pub fn recv_ws_event(&self) -> Option<GatewayWsEvent> {
        self.ws_client.recv_event()
    }

    pub fn drain_ws_events(&self) -> Vec<GatewayWsEvent> {
        self.ws_client.drain_events()
    }

    pub fn drain_applicable_ws_events(
        &self,
        active_connection_id: Option<u64>,
        first_event: Option<GatewayWsEvent>,
    ) -> Vec<GatewayWsEvent> {
        first_event
            .into_iter()
            .chain(self.drain_ws_events())
            .filter(|event| should_apply_ws_event(active_connection_id, event))
            .collect()
    }

    pub fn reduce_ws_event(
        &self,
        event: GatewayWsEvent,
        context: ClientRuntimeWsEventContext,
    ) -> ClientRuntimeWsEvent {
        reduce_gateway_ws_event(event, context)
    }

    pub fn reduce_gateway_notification(
        &self,
        notification: GatewayNotification,
        context: ClientRuntimeNotificationContext<'_>,
    ) -> Option<ClientRuntimeNotification> {
        reduce_gateway_notification(notification, context)
    }

    pub fn drive_post_event_batch<Sink>(
        &self,
        events_applied: bool,
        sink: &mut Sink,
    ) -> ClientRuntimePostEventOutcome
    where
        Sink: ClientRuntimePostEventSink,
    {
        drive_post_event_batch(events_applied, sink)
    }
}

impl Default for ClientRuntime {
    fn default() -> Self {
        Self::new()
    }
}

pub fn reduce_gateway_ws_event(
    event: GatewayWsEvent,
    context: ClientRuntimeWsEventContext,
) -> ClientRuntimeWsEvent {
    match event {
        GatewayWsEvent::Connecting {
            endpoint_name,
            endpoint_kind,
            ..
        } => ClientRuntimeWsEvent::Connection(reduce_gateway_connection_event(
            GatewayConnectionEvent::Connecting {
                endpoint_name,
                endpoint_kind,
            },
        )),
        GatewayWsEvent::Connected {
            endpoint_name,
            address,
            ..
        } => ClientRuntimeWsEvent::Connection(reduce_gateway_connection_event(
            GatewayConnectionEvent::Connected {
                endpoint_name,
                address,
                queue_skills_refresh: context.queue_skills_refresh,
            },
        )),
        GatewayWsEvent::Reconnecting {
            endpoint_name,
            attempt,
            delay_ms,
            reason,
            ..
        } => ClientRuntimeWsEvent::Connection(reduce_gateway_connection_event(
            GatewayConnectionEvent::Reconnecting {
                endpoint_name,
                attempt,
                delay_ms,
                reason,
                should_resume_in_flight_turn: context.should_resume_in_flight_turn,
            },
        )),
        GatewayWsEvent::Disconnected {
            endpoint_name,
            endpoint_kind,
            address,
            reason,
            ..
        } => ClientRuntimeWsEvent::Connection(reduce_gateway_connection_event(
            GatewayConnectionEvent::Disconnected {
                endpoint_name,
                endpoint_kind,
                address,
                reason,
                should_resume_in_flight_turn: context.should_resume_in_flight_turn,
            },
        )),
        GatewayWsEvent::ConnectFailed {
            endpoint_name,
            endpoint_kind,
            address,
            error,
            ..
        } => ClientRuntimeWsEvent::Connection(reduce_gateway_connection_event(
            GatewayConnectionEvent::ConnectFailed {
                endpoint_name,
                endpoint_kind,
                address,
                error,
                should_resume_in_flight_turn: context.should_resume_in_flight_turn,
            },
        )),
        GatewayWsEvent::Notification { notification, .. } => {
            ClientRuntimeWsEvent::Notification(notification)
        }
    }
}

pub fn drive_post_event_batch<Sink>(
    events_applied: bool,
    sink: &mut Sink,
) -> ClientRuntimePostEventOutcome
where
    Sink: ClientRuntimePostEventSink,
{
    ClientRuntimePostEventOutcome {
        events_applied,
        refreshed_thread_list: sink.refresh_thread_list_if_requested(),
        refreshed_skills: sink.refresh_skills_if_requested(),
        refreshed_mcp: sink.refresh_mcp_if_requested(),
        refreshed_mcp_details: sink.refresh_mcp_details_if_requested(),
        drove_thread_start: sink.drive_thread_start_queue(),
        drove_turn_resume: sink.drive_turn_resume_queue(),
        ticked_thread_conversations: sink.tick_thread_conversations(),
    }
}

pub fn reduce_gateway_notification(
    notification: GatewayNotification,
    context: ClientRuntimeNotificationContext<'_>,
) -> Option<ClientRuntimeNotification> {
    match notification {
        GatewayNotification::ThreadStarted(notification) => Some(
            ClientRuntimeNotification::ThreadStarted(reduce_thread_started_notification(
                notification,
                ThreadStartedContext {
                    pending_thread_id: context.pending_thread_id,
                    active_thread_id: context.active_thread_id,
                    active_workspace_id: context.active_workspace_id,
                },
            )),
        ),
        GatewayNotification::TurnStarted(notification) => {
            Some(ClientRuntimeNotification::TurnLifecycle(
                reduce_turn_started_notification(notification),
            ))
        }
        GatewayNotification::TurnCompleted(notification) => {
            Some(ClientRuntimeNotification::TurnLifecycle(
                reduce_turn_completed_notification(notification),
            ))
        }
        GatewayNotification::TurnFailed(notification) => Some(
            ClientRuntimeNotification::TurnLifecycle(reduce_turn_failed_notification(notification)),
        ),
        GatewayNotification::TurnBlocked(notification) => {
            Some(ClientRuntimeNotification::TurnLifecycle(
                reduce_turn_blocked_notification(notification),
            ))
        }
        GatewayNotification::ItemStarted(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_item_started_notification(notification),
            ))
        }
        GatewayNotification::ItemDelta(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_item_delta_notification(notification),
            ))
        }
        GatewayNotification::ItemCompleted(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_item_completed_notification(notification),
            ))
        }
        GatewayNotification::ItemUpdated(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_item_updated_notification(notification),
            ))
        }
        GatewayNotification::ItemTimeoutDetected(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_item_timeout_detected_notification(notification),
            ))
        }
        GatewayNotification::ItemRecoveryOpened(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_item_recovery_opened_notification(notification),
            ))
        }
        GatewayNotification::ItemRecoveryAttached(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_item_recovery_attached_notification(notification),
            ))
        }
        GatewayNotification::ItemRetryScheduled(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_item_retry_scheduled_notification(notification),
            ))
        }
        GatewayNotification::ItemRetryAttemptStarted(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_item_retry_attempt_started_notification(notification),
            ))
        }
        GatewayNotification::ItemRecoverySucceeded(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_item_recovery_succeeded_notification(notification),
            ))
        }
        GatewayNotification::ItemRecoveryExhausted(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_item_recovery_exhausted_notification(notification),
            ))
        }
        GatewayNotification::ItemToolRetryScheduled(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_item_tool_retry_scheduled_notification(notification),
            ))
        }
        GatewayNotification::ItemToolRetryResolved(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_item_tool_retry_resolved_notification(notification),
            ))
        }
        GatewayNotification::ItemToolRetryExhausted(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_item_tool_retry_exhausted_notification(notification),
            ))
        }
        GatewayNotification::TurnToolLoopBudgetExceeded(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_turn_tool_loop_budget_exceeded_notification(notification),
            ))
        }
        GatewayNotification::TurnExecutionWindowStarted(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_turn_execution_window_started_notification(notification),
            ))
        }
        GatewayNotification::TurnExecutionWindowExhausted(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_turn_execution_window_exhausted_notification(notification),
            ))
        }
        GatewayNotification::TurnExecutionWindowCheckpointed(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_turn_execution_window_checkpointed_notification(notification),
            ))
        }
        GatewayNotification::TurnExecutionWindowContinued(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_turn_execution_window_continued_notification(notification),
            ))
        }
        GatewayNotification::TurnExecutionWindowBlocked(notification) => {
            Some(ClientRuntimeNotification::ConversationEvent(
                reduce_turn_execution_window_blocked_notification(notification),
            ))
        }
        GatewayNotification::ThreadClosed(notification) => Some(
            ClientRuntimeNotification::ThreadClosed(reduce_thread_closed_notification(
                notification,
                context.notification_thread_workspace_matches,
            )),
        ),
        GatewayNotification::ThreadTreeChanged(notification) => {
            Some(ClientRuntimeNotification::WorkspaceRefresh(
                reduce_thread_tree_changed_notification(notification, context.active_workspace_id),
            ))
        }
        GatewayNotification::ThreadAgentsDocChanged(notification) => {
            Some(ClientRuntimeNotification::WorkspaceRefresh(
                reduce_thread_agents_doc_changed_notification(
                    notification,
                    context.active_workspace_id,
                ),
            ))
        }
        GatewayNotification::ThreadUpdated(notification) => {
            Some(ClientRuntimeNotification::ThreadUpdated(
                reduce_thread_updated_notification(notification),
            ))
        }
        GatewayNotification::SkillsChanged(notification) => {
            Some(ClientRuntimeNotification::SkillsRefresh(
                reduce_skills_changed_notification(notification, context.active_workspace_id),
            ))
        }
        GatewayNotification::McpChanged(notification) => Some(
            ClientRuntimeNotification::McpRefresh(reduce_mcp_changed_notification(
                notification,
                context.mcp_workspace_id,
                context.mcp_selected_server_id,
            )),
        ),
        GatewayNotification::McpServerStatusChanged(notification) => {
            Some(ClientRuntimeNotification::McpServerStatusChanged(
                reduce_mcp_server_status_changed_notification(
                    notification,
                    context.mcp_workspace_id,
                    context.mcp_selected_server_id,
                    context.mcp_details_loaded,
                ),
            ))
        }
        GatewayNotification::McpServerCatalogChanged(notification) => {
            Some(ClientRuntimeNotification::McpServerCatalogChanged(
                reduce_mcp_server_catalog_changed_notification(
                    notification,
                    context.mcp_workspace_id,
                    context.mcp_selected_server_id,
                ),
            ))
        }
        GatewayNotification::ThreadArtifactsChanged(notification) => {
            Some(ClientRuntimeNotification::ThreadArtifactsRefresh(
                reduce_thread_artifacts_changed_notification(
                    notification,
                    context.notification_thread_workspace_matches,
                ),
            ))
        }
        GatewayNotification::ArtifactCreated(notification) => {
            Some(ClientRuntimeNotification::ArtifactThreadRefresh(
                reduce_artifact_created_notification(notification),
            ))
        }
        GatewayNotification::ArtifactUpdated(notification) => {
            Some(ClientRuntimeNotification::ArtifactThreadRefresh(
                reduce_artifact_updated_notification(notification),
            ))
        }
        GatewayNotification::ArtifactDeleted(notification) => {
            Some(ClientRuntimeNotification::ArtifactDeletedRefresh(
                reduce_artifact_deleted_notification(
                    notification,
                    context.active_thread_id,
                    context.active_thread_artifacts,
                ),
            ))
        }
        GatewayNotification::TurnTimelineChanged(notification) => {
            Some(ClientRuntimeNotification::TurnTimelineRefresh(
                reduce_turn_timeline_changed_notification(notification),
            ))
        }
        GatewayNotification::WorkspaceChanged(notification) => {
            let mut workspaces = context.workspaces.to_vec();
            apply_workspace_changed_to_catalog(&mut workspaces, &notification);
            let preference = reduce_workspace_preference_after_catalog_change(
                context.preferred_workspace_id,
                &workspaces,
            );
            Some(ClientRuntimeNotification::WorkspaceChanged {
                notification,
                preference,
            })
        }
        GatewayNotification::ContextCompressing(_)
        | GatewayNotification::ContextCompressed(_)
        | GatewayNotification::Unknown(_)
        | GatewayNotification::SkillsUploadChunkAck(_)
        | GatewayNotification::ArtifactProjectionUpdated(_)
        | GatewayNotification::ArtifactUploadProgress(_)
        | GatewayNotification::ArtifactDownloadProgress(_)
        | GatewayNotification::TaskCreated(_)
        | GatewayNotification::TaskScheduled(_)
        | GatewayNotification::TaskQueued(_)
        | GatewayNotification::TaskRunCreated(_)
        | GatewayNotification::TaskRunStarted(_)
        | GatewayNotification::TaskProgress(_)
        | GatewayNotification::TaskRunCompleted(_)
        | GatewayNotification::TaskRunFailed(_)
        | GatewayNotification::TaskRunBlocked(_)
        | GatewayNotification::TaskRunCancelled(_)
        | GatewayNotification::TaskCompleted(_)
        | GatewayNotification::TaskFailed(_)
        | GatewayNotification::TaskBlocked(_)
        | GatewayNotification::TaskCancelled(_)
        | GatewayNotification::TaskDetached(_)
        | GatewayNotification::TaskUpdated(_)
        | GatewayNotification::TaskRescheduled(_)
        | GatewayNotification::TaskPaused(_)
        | GatewayNotification::TaskResumed(_)
        | GatewayNotification::TaskDeliveryQueued(_)
        | GatewayNotification::TaskDeliveryStarted(_)
        | GatewayNotification::TaskDeliveryDelivered(_)
        | GatewayNotification::TaskDeliveryFailed(_)
        | GatewayNotification::TaskDeliveryCancelled(_)
        | GatewayNotification::TaskTreeChanged(_)
        | GatewayNotification::TaskRecovered(_)
        | GatewayNotification::MemoryChanged(_)
        | GatewayNotification::MemoryCandidateCreated(_)
        | GatewayNotification::MemoryForgotten(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        gateway::{timings::GatewayWsTimings, types::GatewayEndpointKind},
        notifications::effects::ClientEffect,
        state::{
            client_state::{GatewayConnectionState, GatewayStatusLevel},
            reducers::GatewayStatusMessage,
        },
        transport::ws::GatewayWsConnectSpec,
    };
    use pioneer_protocol::{
        SkillsChangedNotification, UnknownGatewayNotification, Workspace, WorkspaceChangeKind,
        WorkspaceChangedNotification,
    };
    use serde_json::json;
    use std::time::Duration;

    fn timings() -> GatewayWsTimings {
        GatewayWsTimings {
            connect_timeout: Duration::from_millis(100),
            ping_interval: Duration::from_millis(100),
            pong_timeout: Duration::from_millis(100),
            reconnect_initial: Duration::from_millis(10),
            reconnect_max: Duration::from_millis(100),
            reconnect_jitter_percent: 0,
        }
    }

    fn connect_spec(endpoint_id: &str) -> GatewayWsConnectSpec {
        GatewayWsConnectSpec {
            endpoint_id: endpoint_id.to_owned(),
            endpoint_name: "Remote".to_owned(),
            endpoint_kind: GatewayEndpointKind::Remote,
            address: "127.0.0.1:17878".to_owned(),
            auth_token: None,
            timings: timings(),
        }
    }

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

    #[derive(Default)]
    struct RecordingPostEventSink {
        calls: Vec<&'static str>,
        refresh_thread_list: bool,
        refresh_skills: bool,
        refresh_mcp: bool,
        refresh_mcp_details: bool,
        drive_thread_start: bool,
        drive_turn_resume: bool,
        tick_thread_conversations: bool,
    }

    impl ClientRuntimePostEventSink for RecordingPostEventSink {
        fn refresh_thread_list_if_requested(&mut self) -> bool {
            self.calls.push("refresh_thread_list");
            self.refresh_thread_list
        }

        fn refresh_skills_if_requested(&mut self) -> bool {
            self.calls.push("refresh_skills");
            self.refresh_skills
        }

        fn refresh_mcp_if_requested(&mut self) -> bool {
            self.calls.push("refresh_mcp");
            self.refresh_mcp
        }

        fn refresh_mcp_details_if_requested(&mut self) -> bool {
            self.calls.push("refresh_mcp_details");
            self.refresh_mcp_details
        }

        fn drive_thread_start_queue(&mut self) -> bool {
            self.calls.push("drive_thread_start");
            self.drive_thread_start
        }

        fn drive_turn_resume_queue(&mut self) -> bool {
            self.calls.push("drive_turn_resume");
            self.drive_turn_resume
        }

        fn tick_thread_conversations(&mut self) -> bool {
            self.calls.push("tick_thread_conversations");
            self.tick_thread_conversations
        }
    }

    #[test]
    fn runtime_filters_events_by_active_connection() {
        let runtime = ClientRuntime::new();
        let events = runtime.drain_applicable_ws_events(
            Some(2),
            Some(GatewayWsEvent::Connected {
                connection_id: 1,
                endpoint_id: "old".to_owned(),
                endpoint_name: "Old".to_owned(),
                address: "127.0.0.1:1".to_owned(),
            }),
        );
        assert!(events.is_empty());

        let events = runtime.drain_applicable_ws_events(
            Some(2),
            Some(GatewayWsEvent::Connected {
                connection_id: 2,
                endpoint_id: "new".to_owned(),
                endpoint_name: "New".to_owned(),
                address: "127.0.0.1:2".to_owned(),
            }),
        );
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn runtime_reduces_connected_ws_event_with_effects() {
        let event = GatewayWsEvent::Connected {
            connection_id: 7,
            endpoint_id: "remote".to_owned(),
            endpoint_name: "Remote".to_owned(),
            address: "127.0.0.1:17878".to_owned(),
        };

        let reduced = reduce_gateway_ws_event(
            event,
            ClientRuntimeWsEventContext {
                queue_skills_refresh: true,
                should_resume_in_flight_turn: false,
            },
        );

        let ClientRuntimeWsEvent::Connection(reduction) = reduced else {
            panic!("expected connection reduction");
        };

        assert_eq!(
            reduction.connection_state,
            GatewayConnectionState::Connected
        );
        assert_eq!(reduction.status_level, GatewayStatusLevel::Connected);
        assert!(matches!(
            reduction.status,
            GatewayStatusMessage::ConnectedEndpoint { .. }
        ));
        assert_eq!(
            reduction.effects,
            vec![
                ClientEffect::RefreshWorkspaceList,
                ClientEffect::RefreshGatewaySettings,
                ClientEffect::QueueSkillsRefresh,
                ClientEffect::EnqueueInFlightTurnsForResume,
            ]
        );
    }

    #[test]
    fn runtime_reduces_reconnecting_event_with_resume_context() {
        let event = GatewayWsEvent::Reconnecting {
            connection_id: 7,
            endpoint_id: "remote".to_owned(),
            endpoint_name: "Remote".to_owned(),
            attempt: 2,
            delay_ms: 250,
            reason: "temporary".to_owned(),
        };

        let reduced = reduce_gateway_ws_event(
            event,
            ClientRuntimeWsEventContext {
                queue_skills_refresh: false,
                should_resume_in_flight_turn: true,
            },
        );

        let ClientRuntimeWsEvent::Connection(reduction) = reduced else {
            panic!("expected connection reduction");
        };

        assert_eq!(
            reduction.connection_state,
            GatewayConnectionState::Reconnecting
        );
        assert!(!reduction.clear_active_thread);
        assert_eq!(reduction.gateway_error.as_deref(), Some("temporary"));
    }

    #[test]
    fn runtime_preserves_notifications_without_shell_conversion() {
        let workspace = Workspace {
            id: "workspace-1".to_owned(),
            name: "Workspace".to_owned(),
            is_active: true,
            is_current: true,
            created_at: 1,
            updated_at: 2,
        };
        let notification = GatewayNotification::WorkspaceChanged(WorkspaceChangedNotification {
            kind: WorkspaceChangeKind::Updated,
            workspace: workspace.clone(),
        });
        let event = GatewayWsEvent::Notification {
            connection_id: 7,
            notification,
        };

        let reduced = reduce_gateway_ws_event(event, ClientRuntimeWsEventContext::default());

        let ClientRuntimeWsEvent::Notification(GatewayNotification::WorkspaceChanged(actual)) =
            reduced
        else {
            panic!("expected workspace notification");
        };
        assert_eq!(actual.workspace, workspace);
    }

    #[test]
    fn runtime_preserves_unknown_notifications() {
        let event = GatewayWsEvent::Notification {
            connection_id: 7,
            notification: GatewayNotification::Unknown(UnknownGatewayNotification {
                method: "custom.event".to_owned(),
                workspace_id: None,
                thread_id: None,
                turn_id: None,
                item_id: None,
                params: json!({"ok": true}),
            }),
        };

        let reduced = reduce_gateway_ws_event(event, ClientRuntimeWsEventContext::default());

        let ClientRuntimeWsEvent::Notification(GatewayNotification::Unknown(actual)) = reduced
        else {
            panic!("expected unknown notification");
        };
        assert_eq!(actual.method, "custom.event");
        assert_eq!(actual.params, json!({"ok": true}));
    }

    #[test]
    fn runtime_reduces_workspace_changed_notification_with_preference_context() {
        let workspaces = vec![
            workspace("ws_a", true, false),
            workspace("ws_b", true, true),
        ];
        let notification = GatewayNotification::WorkspaceChanged(WorkspaceChangedNotification {
            kind: WorkspaceChangeKind::Updated,
            workspace: workspace("ws_a", false, false),
        });

        let reduced = reduce_gateway_notification(
            notification,
            ClientRuntimeNotificationContext {
                preferred_workspace_id: Some("ws_a"),
                workspaces: workspaces.as_slice(),
                ..Default::default()
            },
        );

        let Some(ClientRuntimeNotification::WorkspaceChanged {
            notification,
            preference,
        }) = reduced
        else {
            panic!("expected workspace changed reduction");
        };

        assert_eq!(notification.workspace.id, "ws_a");
        assert_eq!(
            preference.set_preferred_workspace_id,
            Some(Some("ws_b".to_owned()))
        );
        assert_eq!(
            preference.persist_active_gateway_workspace_id.as_deref(),
            Some("ws_b")
        );
        assert!(preference.queue_thread_list_refresh);
    }

    #[test]
    fn runtime_reduces_skills_changed_notification_with_workspace_scope() {
        let notification = GatewayNotification::SkillsChanged(SkillsChangedNotification {
            workspace_id: "ws_a".to_owned(),
            snapshot_version: 42,
            reason: "updated".to_owned(),
            changes: Vec::new(),
            created_at: 123,
        });

        let reduced = reduce_gateway_notification(
            notification,
            ClientRuntimeNotificationContext {
                active_workspace_id: Some("ws_a"),
                ..Default::default()
            },
        );

        let Some(ClientRuntimeNotification::SkillsRefresh(reduction)) = reduced else {
            panic!("expected skills refresh reduction");
        };
        assert_eq!(reduction.workspace_id, "ws_a");
        assert!(reduction.queue_skills_refresh);
    }

    #[test]
    fn runtime_drives_post_event_batch_in_client_order() {
        let mut sink = RecordingPostEventSink {
            refresh_thread_list: true,
            refresh_skills: false,
            refresh_mcp: true,
            refresh_mcp_details: false,
            drive_thread_start: true,
            drive_turn_resume: false,
            tick_thread_conversations: true,
            ..Default::default()
        };

        let outcome = drive_post_event_batch(false, &mut sink);

        assert_eq!(
            sink.calls,
            vec![
                "refresh_thread_list",
                "refresh_skills",
                "refresh_mcp",
                "refresh_mcp_details",
                "drive_thread_start",
                "drive_turn_resume",
                "tick_thread_conversations",
            ]
        );
        assert!(!outcome.events_applied);
        assert!(outcome.refreshed_thread_list);
        assert!(!outcome.refreshed_skills);
        assert!(outcome.refreshed_mcp);
        assert!(!outcome.refreshed_mcp_details);
        assert!(outcome.drove_thread_start);
        assert!(!outcome.drove_turn_resume);
        assert!(outcome.ticked_thread_conversations);
        assert!(outcome.should_notify());
    }

    #[test]
    fn runtime_post_event_outcome_notifies_for_applied_events_only() {
        let mut sink = RecordingPostEventSink::default();

        let outcome = ClientRuntime::new().drive_post_event_batch(true, &mut sink);

        assert_eq!(
            sink.calls,
            vec![
                "refresh_thread_list",
                "refresh_skills",
                "refresh_mcp",
                "refresh_mcp_details",
                "drive_thread_start",
                "drive_turn_resume",
                "tick_thread_conversations",
            ]
        );
        assert_eq!(
            outcome,
            ClientRuntimePostEventOutcome {
                events_applied: true,
                ..Default::default()
            }
        );
        assert!(outcome.should_notify());

        let mut sink = RecordingPostEventSink::default();
        let idle = drive_post_event_batch(false, &mut sink);
        assert!(!idle.should_notify());
    }

    #[test]
    fn runtime_exposes_shared_ws_sender() {
        let runtime = ClientRuntime::new();
        let _sender = runtime.ws_command_sender();
        let _spec = connect_spec("remote");
    }
}
