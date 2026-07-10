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
    pending: HashMap<SnapshotCoalescingKey, ExecutionSnapshotEvent>,
}

#[derive(Debug)]
struct SnapshotCoalescerInner {
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
        self.inner
            .state
            .lock()
            .expect("snapshot coalescer poisoned")
            .pending
            .insert(event.key(), event);
        self.inner.notify.notify_one();
        true
    }

    pub(crate) fn receiver(&self) -> SnapshotEventReceiver {
        SnapshotEventReceiver {
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
    inner: Arc<SnapshotCoalescerInner>,
}

impl SnapshotEventReceiver {
    pub async fn recv(&mut self) -> Option<ExecutionSnapshotEvent> {
        loop {
            let notified = self.inner.notify.notified();
            let event = {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .expect("snapshot coalescer poisoned");
                let key = state.pending.keys().next().cloned();
                key.and_then(|key| state.pending.remove(&key))
            };
            if let Some(event) = event {
                return Some(event);
            }
            if self.inner.closed.load(Ordering::Acquire) {
                return None;
            }
            notified.await;
        }
    }
}
