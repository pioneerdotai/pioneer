use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pioneer_protocol::{AgentDurableEvent, AgentProgressEvent, TurnItemType};
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};

use crate::progress::{ProgressCoalescer, ProgressCoalescerConfig};
use crate::snapshot::{ExecutionSnapshotEvent, SnapshotCoalescer, SnapshotEventReceiver};
use crate::{DEFAULT_DURABLE_EVENT_CHANNEL_CAPACITY, DEFAULT_LIVE_EVENT_CHANNEL_CAPACITY};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionEventHubError {
    DurableLaneClosed,
    CommitAcknowledgementDropped,
    CommitRejected(String),
}

impl Display for ExecutionEventHubError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DurableLaneClosed => write!(f, "execution durable event lane is closed"),
            Self::CommitAcknowledgementDropped => {
                write!(
                    f,
                    "execution durable event commit acknowledgement was dropped"
                )
            }
            Self::CommitRejected(message) => {
                write!(f, "execution durable event commit was rejected: {message}")
            }
        }
    }
}

impl Error for ExecutionEventHubError {}

#[derive(Debug)]
struct DurableEventEnvelope {
    event: Box<AgentDurableEvent>,
    committed_tx: Option<oneshot::Sender<Result<(), String>>>,
}

/// Receiver for the durable lane. Events published with a commit waiter must
/// be acknowledged explicitly after their durable projection succeeds.
#[derive(Debug)]
pub struct DurableEventReceiver {
    lane: Arc<DurableLane>,
    pending_commit: Option<oneshot::Sender<Result<(), String>>>,
}

impl DurableEventReceiver {
    pub async fn recv(&mut self) -> Option<AgentDurableEvent> {
        self.acknowledge_last(Err(
            "durable event consumer requested the next event without acknowledging the previous commit"
                .to_owned(),
        ));
        let envelope = self.lane.receiver.lock().await.recv().await?;
        self.pending_commit = envelope.committed_tx;
        Some(*envelope.event)
    }

    pub fn acknowledge_last(&mut self, result: Result<(), String>) {
        if let Some(committed_tx) = self.pending_commit.take() {
            let _ = committed_tx.send(result);
        }
    }
}

impl Drop for DurableEventReceiver {
    fn drop(&mut self) {
        self.acknowledge_last(Err(
            "durable event consumer was dropped before acknowledging the commit".to_owned(),
        ));
        self.lane.receiver_claimed.store(false, Ordering::Release);
    }
}

/// The raw receiver stays owned by the hub for the whole hub lifetime. A
/// gateway listener only leases access to it. Dropping or panicking a listener
/// therefore releases the lease without closing the channel or discarding the
/// queued envelopes; a replacement listener resumes from the same queue.
#[derive(Debug)]
struct DurableLane {
    receiver: Mutex<mpsc::Receiver<DurableEventEnvelope>>,
    receiver_claimed: AtomicBool,
}

#[derive(Debug)]
pub struct ExecutionEventHub {
    durable_tx: mpsc::Sender<DurableEventEnvelope>,
    durable_lane: Arc<DurableLane>,
    progress: ProgressCoalescer,
    snapshot: SnapshotCoalescer,
    snapshot_rx: Mutex<Option<SnapshotEventReceiver>>,
    committed_tx: broadcast::Sender<AgentDurableEvent>,
}

impl ExecutionEventHub {
    pub fn new() -> Self {
        Self::with_capacity(
            DEFAULT_DURABLE_EVENT_CHANNEL_CAPACITY,
            DEFAULT_LIVE_EVENT_CHANNEL_CAPACITY,
        )
    }

    pub fn with_capacity(durable_capacity: usize, live_capacity: usize) -> Self {
        Self::with_progress_config(
            durable_capacity,
            live_capacity,
            ProgressCoalescerConfig::default(),
        )
    }

    pub fn with_progress_config(
        durable_capacity: usize,
        live_capacity: usize,
        progress_config: ProgressCoalescerConfig,
    ) -> Self {
        let (durable_tx, durable_rx) = mpsc::channel(durable_capacity.max(1));
        let (committed_tx, _) = broadcast::channel(live_capacity.max(1));
        let snapshot = SnapshotCoalescer::new();
        let durable_lane = Arc::new(DurableLane {
            receiver: Mutex::new(durable_rx),
            receiver_claimed: AtomicBool::new(false),
        });
        Self {
            durable_tx,
            durable_lane,
            progress: ProgressCoalescer::new(live_capacity, progress_config),
            snapshot_rx: Mutex::new(Some(snapshot.receiver())),
            snapshot,
            committed_tx,
        }
    }

    pub fn publish_durable(
        &self,
        event: AgentDurableEvent,
    ) -> impl Future<Output = Result<(), ExecutionEventHubError>> + Send + '_ {
        self.publish_durable_envelope(Box::new(event), None)
    }

    pub fn publish_durable_and_wait(
        &self,
        event: AgentDurableEvent,
    ) -> impl Future<Output = Result<(), ExecutionEventHubError>> + Send + '_ {
        let event = Box::new(event);
        async move {
            let (committed_tx, committed_rx) = oneshot::channel();
            self.publish_durable_envelope(event, Some(committed_tx))
                .await?;
            committed_rx
                .await
                .map_err(|_| ExecutionEventHubError::CommitAcknowledgementDropped)?
                .map_err(ExecutionEventHubError::CommitRejected)
        }
    }

    async fn publish_durable_envelope(
        &self,
        event: Box<AgentDurableEvent>,
        committed_tx: Option<oneshot::Sender<Result<(), String>>>,
    ) -> Result<(), ExecutionEventHubError> {
        self.flush_progress_for_durable(&event).await;
        self.flush_snapshots_for_durable(&event);
        self.durable_tx
            .send(DurableEventEnvelope {
                event,
                committed_tx,
            })
            .await
            .map_err(|_| ExecutionEventHubError::DurableLaneClosed)
    }

    pub fn publish_progress(&self, event: AgentProgressEvent) {
        self.progress.offer(event);
    }

    pub fn publish_snapshot(&self, event: ExecutionSnapshotEvent) -> bool {
        self.snapshot.offer(event)
    }

    pub async fn take_snapshot_receiver(&self) -> Option<SnapshotEventReceiver> {
        self.snapshot_rx.lock().await.take()
    }

    fn flush_snapshots_for_durable(&self, event: &AgentDurableEvent) {
        match event {
            AgentDurableEvent::TurnCompleted {
                thread_id, turn_id, ..
            }
            | AgentDurableEvent::TurnFailed {
                thread_id, turn_id, ..
            }
            | AgentDurableEvent::TurnBlocked {
                thread_id, turn_id, ..
            }
            | AgentDurableEvent::TurnInterrupted {
                thread_id, turn_id, ..
            } => self.snapshot.notify_turn(thread_id, turn_id),
            _ => {}
        }
    }

    pub async fn flush_progress_for_durable(&self, event: &AgentDurableEvent) {
        self.progress.flush_for_durable(event).await;
    }

    pub async fn flush_progress_for_item(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
    ) {
        self.progress
            .flush_item(workspace_id, thread_id, turn_id, item_id)
            .await;
    }

    pub async fn shutdown_progress(&self) {
        self.progress.flush_all().await;
        self.snapshot.close();
    }

    pub fn publish_heartbeat(
        &self,
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
    ) {
        self.progress
            .offer_heartbeat(workspace_id, thread_id, turn_id, item_id, item_type);
    }

    pub fn publish_confirmed_activity(
        &self,
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
    ) {
        self.progress.offer_confirmed_activity(
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            item_type,
        );
    }

    pub fn subscribe_live(&self) -> broadcast::Receiver<AgentProgressEvent> {
        self.progress.subscribe_live()
    }

    pub fn subscribe_committed(&self) -> broadcast::Receiver<AgentDurableEvent> {
        self.committed_tx.subscribe()
    }

    pub fn publish_committed(&self, event: AgentDurableEvent) {
        let _ = self.committed_tx.send(event);
    }

    pub async fn take_durable_receiver(&self) -> Option<DurableEventReceiver> {
        self.durable_lane
            .receiver_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        Some(DurableEventReceiver {
            lane: self.durable_lane.clone(),
            pending_commit: None,
        })
    }

    /// Returns true once the durable consumer has gone away. Callers that
    /// cache hubs use this to replace a poisoned execution lane instead of
    /// handing every later event to a permanently closed channel.
    pub fn durable_lane_is_closed(&self) -> bool {
        self.durable_tx.is_closed()
    }

    /// Reports whether a consumer currently owns the durable receiver lease.
    /// Dropping a receiver releases this lease without closing the hub so a
    /// supervisor can replace or restart the listener without losing backlog.
    pub fn durable_receiver_is_claimed(&self) -> bool {
        self.durable_lane.receiver_claimed.load(Ordering::Acquire)
    }
}

impl Default for ExecutionEventHub {
    fn default() -> Self {
        Self::new()
    }
}

impl AsRef<ExecutionEventHub> for ExecutionEventHub {
    fn as_ref(&self) -> &ExecutionEventHub {
        self
    }
}
