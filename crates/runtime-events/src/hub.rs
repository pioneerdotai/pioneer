use pioneer_observability::queues::{QueueBytes, QueueKind, QueueObserver, QueueOutcome};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use pioneer_protocol::{AgentDurableEvent, AgentProgressEvent, TurnItemType};
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};

use crate::progress::{ProgressCoalescer, ProgressCoalescerConfig};
use crate::snapshot::{ExecutionSnapshotEvent, SnapshotCoalescer, SnapshotEventReceiver};
use crate::{DEFAULT_DURABLE_EVENT_CHANNEL_CAPACITY, DEFAULT_LIVE_EVENT_CHANNEL_CAPACITY};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionEventHubError {
    DurableLaneClosed,
    CommitAcknowledgementDropped,
    CommitRejected(DurableCommitRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableCommitRejectionKind {
    Retryable,
    Permanent,
    SupersededByDurableInterruption,
}

/// A safe, low-cardinality reason why a durable projection was rejected.
///
/// The gateway keeps raw storage and authorization errors in its local logs;
/// publishers only need the retry decision and a stable diagnostic code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableCommitRejection {
    kind: DurableCommitRejectionKind,
    code: &'static str,
    message: &'static str,
}

impl DurableCommitRejection {
    pub const fn retryable(code: &'static str, message: &'static str) -> Self {
        Self {
            kind: DurableCommitRejectionKind::Retryable,
            code,
            message,
        }
    }

    pub const fn permanent(code: &'static str, message: &'static str) -> Self {
        Self {
            kind: DurableCommitRejectionKind::Permanent,
            code,
            message,
        }
    }

    /// The consumer confirmed an accepted Interrupted canonical event with
    /// durable obligations for this turn. This is not a successful execution write.
    pub const fn superseded_by_durable_interruption() -> Self {
        Self {
            kind: DurableCommitRejectionKind::SupersededByDurableInterruption,
            code: "durable_interruption_accepted",
            message: "execution was superseded by confirmed durable interruption",
        }
    }

    pub const fn kind(&self) -> DurableCommitRejectionKind {
        self.kind
    }

    pub const fn code(&self) -> &'static str {
        self.code
    }

    pub const fn is_retryable(&self) -> bool {
        matches!(self.kind, DurableCommitRejectionKind::Retryable)
    }
}

impl Display for DurableCommitRejection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
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
    work: pioneer_observability::work::WorkCarrier,
    event: Box<AgentDurableEvent>,
    committed_tx: Option<oneshot::Sender<Result<(), DurableCommitRejection>>>,
    enqueued_at: Instant,
    turn_transition: Option<Arc<tokio::sync::OwnedMutexGuard<()>>>,
    cli_blocked_guard: Option<pioneer_protocol::CliRuntimeBlockedTurnGuard>,
}

/// Receiver for the durable lane. Events published with a commit waiter must
/// be acknowledged explicitly after their durable projection succeeds.
#[derive(Debug)]
pub struct DurableEventReceiver {
    pending_work: pioneer_observability::work::WorkCarrier,
    pending_queue: Option<QueueObserver>,
    lane: Arc<DurableLane>,
    pending_commit: Option<oneshot::Sender<Result<(), DurableCommitRejection>>>,
    pending_enqueue_age: Option<Duration>,
    pending_turn_transition: Option<Arc<tokio::sync::OwnedMutexGuard<()>>>,
    pending_cli_blocked_guard: Option<pioneer_protocol::CliRuntimeBlockedTurnGuard>,
}

impl DurableEventReceiver {
    pub async fn recv(&mut self) -> Option<AgentDurableEvent> {
        self.acknowledge_last(Err(DurableCommitRejection::retryable(
            "consumer_missing_ack",
            "durable event consumer requested the next event without acknowledging the previous commit",
        )));
        let (envelope, observer) = {
            let mut receiver = self.lane.receiver.lock().await;
            // Tokio recv itself is poll_fn(poll_recv). Couple only the native
            // nonblocking poll/mutation to send+current publication. The depth
            // guard is dropped on EVERY Pending; no observation lock survives
            // an await, channel-capacity wait, ACK or application handler.
            std::future::poll_fn(|cx| {
                let mut depth = self
                    .lane
                    .depth
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match receiver.poll_recv(cx) {
                    std::task::Poll::Ready(Some(envelope)) => {
                        *depth -= 1;
                        let observer = self.lane.observer.in_current_epoch();
                        observer.current(*depth, QueueBytes::Unsupported);
                        std::task::Poll::Ready(Some((envelope, observer)))
                    }
                    std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
                    std::task::Poll::Pending => std::task::Poll::Pending,
                }
            })
            .await?
        };
        #[cfg(test)]
        AFTER_DURABLE_POP.with(|hook| {
            let callback = hook.borrow_mut().take();
            if let Some(callback) = callback {
                callback();
            }
        });
        observer.dequeued(QueueBytes::Unsupported);
        self.pending_queue = Some(observer);
        self.pending_work = envelope.work.received(
            pioneer_observability::work::Boundary::EventLane,
            pioneer_observability::work::WorkKind::Event,
            pioneer_observability::work::ExecutorRole::Event,
        );
        self.pending_commit = envelope.committed_tx;
        self.pending_turn_transition = envelope.turn_transition;
        self.pending_cli_blocked_guard = envelope.cli_blocked_guard;
        self.pending_enqueue_age = Some(envelope.enqueued_at.elapsed());
        Some(*envelope.event)
    }

    /// Queue residence measured when the current durable envelope was
    /// received. It contains no identifiers and is cleared with the ACK.
    pub fn pending_work(&self) -> pioneer_observability::work::WorkCarrier {
        self.pending_work.clone()
    }

    pub fn pending_enqueue_age(&self) -> Option<Duration> {
        self.pending_enqueue_age
    }

    pub fn cli_blocked_guard(&self) -> Option<&pioneer_protocol::CliRuntimeBlockedTurnGuard> {
        self.pending_cli_blocked_guard.as_ref()
    }

    pub fn turn_transition(&self) -> Option<&Arc<tokio::sync::OwnedMutexGuard<()>>> {
        self.pending_turn_transition.as_ref()
    }

    pub fn owns_turn_transition(&self) -> bool {
        self.pending_turn_transition.is_some()
    }

    pub fn acknowledge_last(&mut self, result: Result<(), DurableCommitRejection>) {
        if let Some(observer) = self.pending_queue.take() {
            observer.outcome(
                if result.is_ok() {
                    QueueOutcome::Committed
                } else {
                    QueueOutcome::CommitRejected
                },
                1,
            );
            observer.outcome(QueueOutcome::Processed, 1);
        }
        self.pending_work = Default::default();
        self.pending_enqueue_age = None;
        if let Some(committed_tx) = self.pending_commit.take() {
            let _ = committed_tx.send(result);
        }
        self.pending_turn_transition = None;
        self.pending_cli_blocked_guard = None;
    }
}

impl Drop for DurableEventReceiver {
    fn drop(&mut self) {
        self.acknowledge_last(Err(DurableCommitRejection::retryable(
            "consumer_dropped",
            "durable event consumer was dropped before acknowledging the commit",
        )));
        self.lane.receiver_claimed.store(false, Ordering::Release);
    }
}

/// The raw receiver stays owned by the hub for the whole hub lifetime. A
/// gateway listener only leases access to it. Dropping or panicking a listener
/// therefore releases the lease without closing the channel or discarding the
/// queued envelopes; a replacement listener resumes from the same queue.
#[derive(Debug)]
struct DurableLane {
    // Fixed owner metadata, including backlog created while recording is off.
    // Never held over reserve/recv await, ACK or handler execution.
    depth: StdMutex<usize>,
    observer: QueueObserver,
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
            depth: StdMutex::new(0),
            observer: QueueObserver::new(
                QueueKind::EventDurable,
                Some(durable_capacity.max(1)),
                "unsupported: durable domain payload not serialized for observation",
            ),
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
        self.publish_durable_envelope(Box::new(event), None, None, None)
    }

    pub fn publish_durable_and_wait(
        &self,
        event: AgentDurableEvent,
    ) -> impl Future<Output = Result<(), ExecutionEventHubError>> + Send + '_ {
        self.publish_durable_and_wait_with_turn_transition(event, None)
    }

    /// Keep a CLI session transition owned until the queued lifecycle handler
    /// has finished, even if its publisher is cancelled while waiting for ACK.
    /// This shares the existing ordered, bounded durable lane.
    pub fn publish_durable_and_wait_with_turn_transition(
        &self,
        event: AgentDurableEvent,
        turn_transition: Option<Arc<tokio::sync::OwnedMutexGuard<()>>>,
    ) -> impl Future<Output = Result<(), ExecutionEventHubError>> + Send + '_ {
        self.publish_cli_runtime_blocked_and_wait(event, turn_transition, None)
    }

    /// CLI source facts for the atomic Blocked commit or native terminal delivery.
    pub fn publish_cli_runtime_blocked_and_wait(
        &self,
        event: AgentDurableEvent,
        turn_transition: Option<Arc<tokio::sync::OwnedMutexGuard<()>>>,
        cli_blocked_guard: Option<pioneer_protocol::CliRuntimeBlockedTurnGuard>,
    ) -> impl Future<Output = Result<(), ExecutionEventHubError>> + Send + '_ {
        let event = Box::new(event);
        async move {
            let (committed_tx, committed_rx) = oneshot::channel();
            self.publish_durable_envelope(
                event,
                Some(committed_tx),
                turn_transition,
                cli_blocked_guard,
            )
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
        committed_tx: Option<oneshot::Sender<Result<(), DurableCommitRejection>>>,
        turn_transition: Option<Arc<tokio::sync::OwnedMutexGuard<()>>>,
        cli_blocked_guard: Option<pioneer_protocol::CliRuntimeBlockedTurnGuard>,
    ) -> Result<(), ExecutionEventHubError> {
        self.flush_progress_for_durable(&event).await;
        self.flush_snapshots_for_durable(&event);
        // Start queue residence only after prerequisite coalescer flushes.
        // The duration still includes bounded-channel backpressure, but not
        // unrelated pre-enqueue projection work.
        let enqueued_at = Instant::now();
        let mut send_wait = self.durable_lane.observer.send_wait();
        let permit = pioneer_observability::live::waiting(
            pioneer_observability::live::WaitKind::ChannelCapacity,
            self.durable_tx.reserve(),
        )
        .await
        .map_err(|_| ExecutionEventHubError::DurableLaneClosed)?;
        send_wait.complete();
        let envelope = DurableEventEnvelope {
            work: pioneer_observability::work::WorkCarrier::child(
                pioneer_observability::work::Boundary::EventLane,
                pioneer_observability::work::WorkKind::Event,
                pioneer_observability::work::Trigger::Event,
                pioneer_observability::work::ExecutorRole::Event,
            )
            .queued(),
            event,
            committed_tx,
            enqueued_at,
            turn_transition,
            cli_blocked_guard,
        };
        {
            let mut depth = self
                .durable_lane
                .depth
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let observer = self.durable_lane.observer.in_current_epoch();
            observer.enqueued(QueueBytes::Unsupported);
            *depth += 1;
            permit.send(envelope); // nonblocking; reserve and handler are outside
            observer.current(*depth, QueueBytes::Unsupported);
        }
        Ok(())
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

    pub fn pending_progress_targets_for_item(
        &self,
        workspace_id: &str,
        item_id: &str,
    ) -> Vec<(String, String)> {
        self.progress
            .pending_targets_for_item(workspace_id, item_id)
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

    pub fn subscribe_live(&self) -> crate::ProgressEventReceiver {
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
            pending_work: Default::default(),
            pending_queue: None,
            lane: self.durable_lane.clone(),
            pending_commit: None,
            pending_enqueue_age: None,
            pending_turn_transition: None,
            pending_cli_blocked_guard: None,
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

#[cfg(test)]
thread_local! { pub(crate) static AFTER_DURABLE_POP: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = std::cell::RefCell::new(None); }

#[cfg(test)]
mod depth_tests {
    use super::*;
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };
    #[tokio::test]
    async fn durable_receipt_delayed_by_new_send_keeps_depth_ack_and_epoch() {
        let _lock = crate::OBSERVATION_TEST_LOCK.lock().unwrap();
        let old = pioneer_observability::telemetry_enabled();
        pioneer_observability::set_telemetry_enabled(false);
        pioneer_observability::set_telemetry_enabled(true);
        let mut hub = ExecutionEventHub::with_capacity(2, 2);
        Arc::get_mut(&mut hub.durable_lane).unwrap().observer = QueueObserver::new(
            QueueKind::EventDurable,
            Some(2),
            "fixture durable current order",
        );
        let hub = Arc::new(hub);
        let mut receiver = hub.take_durable_receiver().await.unwrap();
        {
            let mut idle = pin!(receiver.recv());
            assert!(
                idle.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert!(hub.durable_lane.depth.try_lock().is_ok()); // no owner observation lock through Pending
        } // cancelling an empty receive removes no event or permit
        hub.publish_durable(crate::tests::completed("A"))
            .await
            .unwrap();
        let during = hub.clone();
        let latest_stamp = std::rc::Rc::new(std::cell::Cell::new(0));
        let stamp = latest_stamp.clone();
        AFTER_DURABLE_POP.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                // The native receive already removed A; the actual publish factory
                // can complete without waiting because capacity is available.
                let mut send = pin!(during.publish_durable(crate::tests::completed("B")));
                assert!(matches!(
                    send.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                    Poll::Ready(Ok(()))
                ));
                stamp.set(
                    pioneer_observability::live::snapshot()
                        .unwrap()
                        .queues
                        .iter()
                        .find(|q| q.byte_definition == "fixture durable current order")
                        .unwrap()
                        .last_observed_ms,
                );
            }))
        });
        assert_eq!(receiver.recv().await, Some(crate::tests::completed("A")));
        let read = || {
            pioneer_observability::live::snapshot()
                .unwrap()
                .queues
                .into_iter()
                .find(|q| q.byte_definition == "fixture durable current order")
                .unwrap()
        };
        assert_eq!(hub.durable_lane.receiver.lock().await.len(), 1);
        let q = read();
        assert_eq!((q.depth, q.enqueued, q.dequeued, q.processed), (1, 2, 1, 0));
        assert!(q.initial_depth_known);
        assert!(q.last_observed_ms <= q.snapshot_ms);
        assert_eq!(q.last_observed_ms, latest_stamp.get()); // late A milestone cannot replace B source time
        receiver.acknowledge_last(Ok(()));
        assert_eq!((read().processed, read().committed), (1, 1));
        assert_eq!(receiver.recv().await, Some(crate::tests::completed("B")));
        let stamp = read().last_observed_ms;
        receiver.acknowledge_last(Err(DurableCommitRejection::retryable("fixture", "fixture")));
        let q = read();
        assert_eq!(
            (
                q.depth,
                q.enqueued,
                q.dequeued,
                q.processed,
                q.commit_rejected
            ),
            (0, 2, 2, 2, 1)
        );
        assert_eq!(q.last_observed_ms, stamp); // ACK is a milestone, not a new depth sample
        // An old pending ACK must not register/mutate E2; real backlog survives.
        hub.publish_durable(crate::tests::completed("old"))
            .await
            .unwrap();
        receiver.recv().await.unwrap();
        pioneer_observability::set_telemetry_enabled(false);
        hub.publish_durable(crate::tests::completed("off"))
            .await
            .unwrap();
        pioneer_observability::set_telemetry_enabled(true);
        receiver.acknowledge_last(Ok(()));
        assert!(
            pioneer_observability::live::snapshot()
                .unwrap()
                .queues
                .iter()
                .all(|q| q.byte_definition != "fixture durable current order")
        );
        hub.publish_durable(crate::tests::completed("new"))
            .await
            .unwrap();
        assert_eq!(
            read().depth,
            hub.durable_lane.receiver.lock().await.len() as u64
        );
        assert_eq!(read().depth, 2);
        assert_eq!(receiver.recv().await, Some(crate::tests::completed("off")));
        assert_eq!(read().depth, 1);
        receiver.acknowledge_last(Ok(()));
        assert_eq!(receiver.recv().await, Some(crate::tests::completed("new")));
        receiver.acknowledge_last(Ok(()));
        assert_eq!(read().depth, 0);
        pioneer_observability::set_telemetry_enabled(old);
    }
}
