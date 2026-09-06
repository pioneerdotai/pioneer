//! One typed classification and one compatibility recipient per transport event.

use crate::transport::ws::GatewayWsEvent;
use pioneer_protocol::GatewayNotification;
use std::{
    collections::VecDeque,
    sync::{Condvar, Mutex},
};

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

pub struct GatewayCompatibilityEvent {
    route: GatewayEventRoute,
    event: GatewayWsEvent,
}

impl GatewayCompatibilityEvent {
    pub fn route(&self) -> GatewayEventRoute {
        self.route
    }
    pub fn into_event(self) -> GatewayWsEvent {
        self.event
    }
}

#[derive(Default)]
struct QueueState {
    closed: bool,
    events: VecDeque<GatewayCompatibilityEvent>,
}

#[derive(Default)]
pub(crate) struct GatewayCompatibilityQueue {
    state: Mutex<QueueState>,
    ready: Condvar,
    space: Condvar,
    async_ready: tokio::sync::Notify,
}

impl GatewayCompatibilityQueue {
    const CAPACITY: usize = 256;

    pub(crate) fn push(&self, route: GatewayEventRoute, event: GatewayWsEvent) -> bool {
        let mut state = self.state.lock().expect("Gateway delivery queue poisoned");
        while !state.closed && state.events.len() == Self::CAPACITY {
            state = self
                .space
                .wait(state)
                .expect("Gateway delivery queue poisoned");
        }
        if state.closed {
            return false;
        }
        state
            .events
            .push_back(GatewayCompatibilityEvent { route, event });
        self.ready.notify_one();
        self.async_ready.notify_one();
        true
    }

    pub(crate) async fn receive_async(&self) -> Option<GatewayCompatibilityEvent> {
        loop {
            let ready = self.async_ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            {
                let mut state = self.state.lock().expect("Gateway delivery queue poisoned");
                if let Some(event) = state.events.pop_front() {
                    self.space.notify_one();
                    return Some(event);
                }
                if state.closed {
                    return None;
                }
            }
            ready.await;
        }
    }

    pub(crate) fn receive(&self) -> Option<GatewayCompatibilityEvent> {
        let mut state = self.state.lock().expect("Gateway delivery queue poisoned");
        while !state.closed && state.events.is_empty() {
            state = self
                .ready
                .wait(state)
                .expect("Gateway delivery queue poisoned");
        }
        let event = state.events.pop_front();
        self.space.notify_one();
        event
    }

    pub(crate) fn drain(&self) -> Vec<GatewayCompatibilityEvent> {
        let mut state = self.state.lock().expect("Gateway delivery queue poisoned");
        let events = state.events.drain(..).collect();
        self.space.notify_one();
        events
    }

    pub(crate) fn finish(&self) {
        self.state
            .lock()
            .expect("Gateway delivery queue poisoned")
            .closed = true;
        self.ready.notify_all();
        self.async_ready.notify_waiters();
        self.space.notify_all();
    }

    pub(crate) fn close(&self) {
        let mut state = self.state.lock().expect("Gateway delivery queue poisoned");
        state.closed = true;
        state.events.clear();
        self.ready.notify_all();
        self.async_ready.notify_waiters();
        self.space.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::types::GatewayEndpointKind;
    fn event(id: u64) -> GatewayWsEvent {
        GatewayWsEvent::Connecting {
            connection_id: id,
            endpoint_id: "synthetic".into(),
            endpoint_name: "Synthetic".into(),
            endpoint_kind: GatewayEndpointKind::Remote,
        }
    }

    #[tokio::test]
    async fn cancelling_a_window_wait_does_not_consume_the_next_delivery() {
        let queue = GatewayCompatibilityQueue::default();
        {
            let pending = queue.receive_async();
            tokio::pin!(pending);
            assert!(futures_util::poll!(pending).is_pending());
        }
        assert!(queue.push(GatewayEventRoute::Connection, event(7)));
        let next = queue.receive_async().await.unwrap();
        assert_eq!(
            crate::transport::ws::event_connection_id(&next.into_event()),
            7
        );
        queue.close();
        assert!(queue.receive_async().await.is_none());
    }

    #[test]
    fn bounded_queue_preserves_order_and_shutdown_wakes_a_blocked_producer() {
        let queue = std::sync::Arc::new(GatewayCompatibilityQueue::default());
        for id in 0..256 {
            assert!(queue.push(GatewayEventRoute::Connection, event(id)));
        }
        let producer_queue = queue.clone();
        let (started, ready) = std::sync::mpsc::channel();
        let (finished, result) = std::sync::mpsc::channel();
        let producer = std::thread::spawn(move || {
            started.send(()).unwrap();
            finished
                .send(producer_queue.push(GatewayEventRoute::Connection, event(256)))
                .unwrap();
        });
        ready.recv().unwrap();
        assert!(
            result
                .recv_timeout(std::time::Duration::from_millis(20))
                .is_err()
        );
        let first = queue.receive().unwrap();
        assert_eq!(first.route(), GatewayEventRoute::Connection);
        assert_eq!(
            crate::transport::ws::event_connection_id(&first.into_event()),
            0
        );
        assert!(result.recv().unwrap());
        producer.join().unwrap();
        let ids: Vec<_> = queue
            .drain()
            .into_iter()
            .map(|event| crate::transport::ws::event_connection_id(&event.into_event()))
            .collect();
        assert_eq!(ids, (1..=256).collect::<Vec<_>>());
        for id in 0..256 {
            assert!(queue.push(GatewayEventRoute::Connection, event(id)));
        }
        let producer_queue = queue.clone();
        let producer = std::thread::spawn(move || {
            producer_queue.push(GatewayEventRoute::Connection, event(256))
        });
        queue.close();
        assert!(!producer.join().unwrap());
        assert!(queue.receive().is_none());
    }
}
