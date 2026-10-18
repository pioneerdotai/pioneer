//! Shared admission for recording workers and their finalization requests.
//! Only accepted finalization releases ordered ingress at its ACK.
use crate::session::ConnectionId;
use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

const MAX_VOICE_WORKERS: usize = 4;
tokio::task_local! {
    static ACCEPTED: RefCell<Option<oneshot::Sender<()>>>;
}

pub(crate) fn acknowledge() {
    let _ = ACCEPTED.try_with(|sender| {
        if let Some(sender) = sender.borrow_mut().take() {
            let _ = sender.send(());
        }
    });
}

pub(crate) async fn with_ack<F: std::future::Future>(
    sender: oneshot::Sender<()>,
    future: F,
) -> F::Output {
    ACCEPTED.scope(RefCell::new(Some(sender)), future).await
}

#[derive(Clone)]
pub(crate) struct VoiceFinalizations {
    slots: Arc<Semaphore>,
    connections: Arc<Mutex<HashMap<ConnectionId, Arc<AtomicBool>>>>,
    pub(crate) tasks: TaskTracker,
    pub(crate) shutdown: CancellationToken,
}
impl Default for VoiceFinalizations {
    fn default() -> Self {
        Self {
            slots: Arc::new(Semaphore::new(MAX_VOICE_WORKERS)),
            connections: Arc::default(),
            tasks: TaskTracker::new(),
            shutdown: CancellationToken::new(),
        }
    }
}
impl VoiceFinalizations {
    // No waiting queue: recording overload is rejected before another worker
    // or native call is allocated. Cancel never releases an in-flight slot.
    pub(crate) fn reserve(&self, connection: ConnectionId) -> Option<VoiceFinalizationLease> {
        let tracked = self.tasks.token();
        let slot = self.slots.clone().try_acquire_owned().ok()?;
        let mut connections = self.connections.lock().ok()?;
        if connections.contains_key(&connection) {
            return None;
        }
        connections.insert(connection, Arc::new(AtomicBool::new(false)));
        Some(VoiceFinalizationLease {
            _tracked: tracked,
            _slot: slot,
            connection,
            connections: self.connections.clone(),
        })
    }
    // The recording already owns native capacity. Gate its one outstanding
    // finalize request through the same admission entry, without a second slot.
    pub(crate) fn begin_finalize(&self, connection: ConnectionId) -> Option<VoiceFinalizeRequest> {
        let tracked = self.tasks.token();
        if self.shutdown.is_cancelled() {
            return None;
        }
        let connections = self.connections.lock().ok()?;
        let pending = connections.get(&connection)?.clone();
        pending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        Some(VoiceFinalizeRequest {
            pending,
            _tracked: tracked,
        })
    }

    pub(crate) fn has_active_work(&self) -> bool {
        self.connections
            .lock()
            .map(|entries| !entries.is_empty())
            .unwrap_or(true)
    }

    pub(crate) fn close(&self) {
        self.slots.close();
        self.shutdown.cancel();
        self.tasks.close();
    }
}
pub(crate) struct VoiceFinalizeRequest {
    pending: Arc<AtomicBool>,
    _tracked: tokio_util::task::task_tracker::TaskTrackerToken,
}
impl Drop for VoiceFinalizeRequest {
    fn drop(&mut self) {
        self.pending.store(false, Ordering::Release);
    }
}
pub(crate) struct VoiceFinalizationLease {
    _tracked: tokio_util::task::task_tracker::TaskTrackerToken,
    _slot: OwnedSemaphorePermit,
    connection: ConnectionId,
    connections: Arc<Mutex<HashMap<ConnectionId, Arc<AtomicBool>>>>,
}
impl Drop for VoiceFinalizationLease {
    fn drop(&mut self) {
        if let Ok(mut connections) = self.connections.lock() {
            connections.remove(&self.connection);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn voice_finalization_capacity_and_shutdown_retain_owned_slots() {
        let workers = VoiceFinalizations::default();
        let shared = workers.clone();
        let first = workers.reserve(1).unwrap();
        assert!(shared.reserve(1).is_none());
        assert!(TaskTracker::ptr_eq(&workers.tasks, &shared.tasks));
        let request = workers.begin_finalize(1).unwrap();
        assert!(shared.begin_finalize(1).is_none());
        assert!(shared.begin_finalize(99).is_none());
        drop(request);
        assert!(workers.begin_finalize(1).is_some());
        let others: Vec<_> = (2..=4)
            .map(|owner| shared.reserve(owner).unwrap())
            .collect();
        assert!(workers.reserve(5).is_none());
        assert!(shared.reserve(5).is_none());
        shared.close();
        assert!(workers.reserve(5).is_none());
        assert!(workers.shutdown.is_cancelled());
        assert!(shared.shutdown.is_cancelled());
        // Close does not abort/release a native owner's lease. Draining finishes
        // only when all owned tasks/leases complete (no model/runtime involved).
        assert!(!workers.tasks.is_empty());
        drop(first);
        drop(others);
        workers.tasks.wait().await;
        assert!(workers.tasks.is_empty());
    }
}
