//! Bounded, explicitly owned exception to ordered RPC completion. Only accepted
//! voice finalization returns to ingress at ACK; all other RPCs remain ordered.
use crate::session::ConnectionId;
use std::{
    cell::RefCell,
    collections::HashSet,
    sync::{Arc, Mutex},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

const MAX_FINALIZATIONS: usize = 4;
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
    connections: Arc<Mutex<HashSet<ConnectionId>>>,
    pub(crate) tasks: TaskTracker,
    pub(crate) shutdown: CancellationToken,
}
impl Default for VoiceFinalizations {
    fn default() -> Self {
        Self {
            slots: Arc::new(Semaphore::new(MAX_FINALIZATIONS)),
            connections: Arc::default(),
            tasks: TaskTracker::new(),
            shutdown: CancellationToken::new(),
        }
    }
}
impl VoiceFinalizations {
    // No waiting queue: overload/repeated finalize is rejected before another
    // task or native call is allocated. Cancel never releases the native slot.
    pub(crate) fn reserve(&self, connection: ConnectionId) -> Option<VoiceFinalizationLease> {
        let tracked = self.tasks.token();
        let slot = self.slots.clone().try_acquire_owned().ok()?;
        let mut connections = self.connections.lock().ok()?;
        if !connections.insert(connection) {
            return None;
        }
        Some(VoiceFinalizationLease {
            _tracked: tracked,
            _slot: slot,
            connection,
            connections: self.connections.clone(),
        })
    }
    pub(crate) fn close(&self) {
        self.slots.close();
        self.shutdown.cancel();
        self.tasks.close();
    }
}
pub(crate) struct VoiceFinalizationLease {
    _tracked: tokio_util::task::task_tracker::TaskTrackerToken,
    _slot: OwnedSemaphorePermit,
    connection: ConnectionId,
    connections: Arc<Mutex<HashSet<ConnectionId>>>,
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
