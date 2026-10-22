use pioneer_observability::queues::{QueueBytes, QueueKind, QueueObserver, QueueOutcome};
use pioneer_observability::work::{Boundary, ExecutorRole, WorkCarrier, WorkItem, WorkKind};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use pioneer_protocol::ItemUpdatedNotification;
use tokio::sync::Notify;

/// Latest-wins state updates. These are persisted projections, not lifecycle
/// journal entries, so replacing an older value for the same item is lossless.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionSnapshotEvent {
    ItemUpdated {
        notification: ItemUpdatedNotification,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SnapshotCoalescingKey {
    workspace_id: String,
    thread_id: String,
    turn_id: String,
    item_id: String,
}

impl ExecutionSnapshotEvent {
    fn key(&self) -> SnapshotCoalescingKey {
        match self {
            Self::ItemUpdated { notification } => SnapshotCoalescingKey {
                workspace_id: notification.workspace_id.clone(),
                thread_id: notification.thread_id.clone(),
                turn_id: notification.turn_id.clone(),
                item_id: notification.item.item_id().to_owned(),
            },
        }
    }

    fn belongs_to_turn(&self, thread_id: &str, turn_id: &str) -> bool {
        match self {
            Self::ItemUpdated { notification } => {
                notification.thread_id == thread_id && notification.turn_id == turn_id
            }
        }
    }
}

#[derive(Debug, Default)]
struct SnapshotCoalescerState {
    pending: HashMap<SnapshotCoalescingKey, WorkItem<ExecutionSnapshotEvent>>,
}

#[derive(Debug)]
struct SnapshotCoalescerInner {
    observer: QueueObserver,
    state: StdMutex<SnapshotCoalescerState>,
    notify: Notify,
    closed: AtomicBool,
}

#[derive(Debug, Clone)]
pub(crate) struct SnapshotCoalescer {
    inner: Arc<SnapshotCoalescerInner>,
}

impl SnapshotCoalescer {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(SnapshotCoalescerInner {
                observer: QueueObserver::new(
                    QueueKind::EventSnapshot,
                    None,
                    "unsupported: latest snapshot payload not serialized for observation",
                ),
                state: StdMutex::new(SnapshotCoalescerState::default()),
                notify: Notify::new(),
                closed: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn offer(&self, event: ExecutionSnapshotEvent) -> bool {
        if self.inner.closed.load(Ordering::Acquire) {
            return false;
        }
        let mut state = self
            .inner
            .state
            .lock()
            .expect("snapshot coalescer poisoned");
        let observer = self.inner.observer.in_current_epoch();
        let key = event.key();
        if state.pending.contains_key(&key) {
            observer.outcome(QueueOutcome::Coalesced, 1);
        } else {
            observer.enqueued(QueueBytes::Unsupported);
        }
        let mut item = WorkItem::new(
            event,
            Boundary::EventLane,
            WorkKind::Event,
            ExecutorRole::Event,
        );
        if let Some(previous) = state.pending.get(&key) {
            let mut work = previous.carrier.clone();
            work.merge_link(&item.carrier);
            item.carrier = work;
            item.enqueued_monotonic_ms = previous.enqueued_monotonic_ms;
        }
        state.pending.insert(key, item);
        observer.current(state.pending.len(), QueueBytes::Unsupported);
        drop(state);
        self.inner.notify.notify_one();
        true
    }

    pub(crate) fn receiver(&self) -> SnapshotEventReceiver {
        SnapshotEventReceiver {
            pending_work: Default::default(),
            inner: self.inner.clone(),
        }
    }

    pub(crate) fn notify_turn(&self, thread_id: &str, turn_id: &str) {
        let has_pending = self
            .inner
            .state
            .lock()
            .expect("snapshot coalescer poisoned")
            .pending
            .values()
            .any(|event| event.belongs_to_turn(thread_id, turn_id));
        if has_pending {
            self.inner.notify.notify_one();
        }
    }

    pub(crate) fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.notify.notify_waiters();
    }
}

#[derive(Debug)]
pub struct SnapshotEventReceiver {
    pending_work: WorkCarrier,
    inner: Arc<SnapshotCoalescerInner>,
}

impl SnapshotEventReceiver {
    pub fn pending_work(&self) -> WorkCarrier {
        self.pending_work.clone()
    }
    pub async fn recv(&mut self) -> Option<ExecutionSnapshotEvent> {
        self.pending_work = Default::default();
        loop {
            let notified = self.inner.notify.notified();
            let (event, observer) = {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .expect("snapshot coalescer poisoned");
                let key = state.pending.keys().next().cloned();
                let event = key.and_then(|key| state.pending.remove(&key));
                let observer = self.inner.observer.in_current_epoch();
                observer.current(state.pending.len(), QueueBytes::Unsupported);
                (event, observer)
            };
            if let Some(event) = event {
                #[cfg(test)]
                AFTER_POP.with(|hook| {
                    let callback = hook.borrow_mut().take();
                    if let Some(callback) = callback {
                        callback();
                    }
                });
                observer.dequeued(QueueBytes::Unsupported);
                observer.outcome(QueueOutcome::Processed, 1);
                let (event, work) =
                    event.received(Boundary::EventLane, WorkKind::Event, ExecutorRole::Event);
                self.pending_work = work.execution_child(
                    pioneer_observability::work::WorkKind::Event,
                    pioneer_observability::work::ExecutorRole::Event,
                );
                return Some(event);
            }
            if self.inner.closed.load(Ordering::Acquire) {
                return None;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
thread_local! { pub(crate) static AFTER_POP: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = std::cell::RefCell::new(None); }

#[cfg(test)]
mod depth_tests {
    use super::*;
    #[tokio::test]
    async fn popped_snapshot_milestone_cannot_erase_new_replacement_or_epoch() {
        let _lock = crate::OBSERVATION_TEST_LOCK.lock().unwrap();
        let old = pioneer_observability::telemetry_enabled();
        pioneer_observability::set_telemetry_enabled(false);
        pioneer_observability::set_telemetry_enabled(true);
        let mut coalescer = SnapshotCoalescer::new();
        Arc::get_mut(&mut coalescer.inner).unwrap().observer = QueueObserver::new(
            QueueKind::EventSnapshot,
            None,
            "fixture snapshot current order",
        );
        let mut receiver = coalescer.receiver();
        coalescer.offer(crate::tests::updated("A"));
        let next = coalescer.clone();
        let latest_stamp = std::rc::Rc::new(std::cell::Cell::new(0));
        let stamp = latest_stamp.clone();
        AFTER_POP.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                next.offer(crate::tests::updated("B"));
                next.offer(crate::tests::updated("B replacement"));
                stamp.set(
                    pioneer_observability::live::snapshot()
                        .unwrap()
                        .queues
                        .iter()
                        .find(|q| q.byte_definition == "fixture snapshot current order")
                        .unwrap()
                        .last_observed_ms,
                );
            }))
        });
        assert_eq!(receiver.recv().await, Some(crate::tests::updated("A")));
        let read = || {
            pioneer_observability::live::snapshot()
                .unwrap()
                .queues
                .into_iter()
                .find(|q| q.byte_definition == "fixture snapshot current order")
                .unwrap()
        };
        assert_eq!(coalescer.inner.state.lock().unwrap().pending.len(), 1);
        let q = read();
        assert_eq!(
            (q.depth, q.enqueued, q.dequeued, q.coalesced, q.processed),
            (1, 2, 1, 1, 1)
        );
        assert!(q.initial_depth_known && q.last_observed_ms <= q.snapshot_ms);
        assert_eq!(q.last_observed_ms, latest_stamp.get()); // delayed A milestone cannot renew B freshness
        assert_eq!(
            receiver.recv().await,
            Some(crate::tests::updated("B replacement"))
        );
        assert_eq!((read().depth, read().processed), (0, 2));
        coalescer.offer(crate::tests::updated("old"));
        let next = coalescer.clone();
        AFTER_POP.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                pioneer_observability::set_telemetry_enabled(false);
                pioneer_observability::set_telemetry_enabled(true);
                next.offer(crate::tests::updated("new epoch"));
            }))
        });
        receiver.recv().await.unwrap();
        let q = read();
        assert_eq!((q.depth, q.enqueued, q.dequeued, q.processed), (1, 1, 0, 0));
        receiver.recv().await.unwrap();
        assert_eq!((read().depth, read().processed), (0, 1));
        coalescer.close();
        assert_eq!(receiver.recv().await, None);
        pioneer_observability::set_telemetry_enabled(old);
    }
}
