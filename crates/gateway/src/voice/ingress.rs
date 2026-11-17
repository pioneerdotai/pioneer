//! Per-recording admission and completion fence. Payload capacity belongs to the connection.
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
struct State {
    sealed: bool,
    pending: usize,
}
#[derive(Debug, Default)]
pub(crate) struct VoiceIngress {
    state: Mutex<State>,
    drained: Notify,
    #[cfg(test)]
    pub(crate) proof_pause: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
    pub(crate) cancelled: CancellationToken,
}
impl PartialEq for VoiceIngress {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}
impl VoiceIngress {
    pub(crate) fn admit(self: &Arc<Self>) -> Option<VoiceIngressTicket> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.sealed {
            return None;
        }
        state.pending += 1;
        Some(VoiceIngressTicket(self.clone()))
    }
    #[cfg(test)]
    pub(crate) async fn pause_proof_once(&self) {
        let pause = self.proof_pause.lock().unwrap().take();
        if let Some((entered, release)) = pause {
            let _ = entered.send(());
            let _ = release.await;
        }
    }
    pub(crate) fn effect<T>(&self, effect: impl FnOnce() -> T) -> Option<T> {
        let _state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.cancelled.is_cancelled() {
            return None;
        }
        Some(effect())
    }
    pub(crate) fn seal(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sealed = true;
    }
    pub(crate) fn cancel(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.sealed = true;
        self.cancelled.cancel();
    }
    pub(crate) async fn drain(&self) {
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pending
                == 0
            {
                return;
            }
            notified.await;
        }
    }
}
pub(crate) struct VoiceIngressTicket(pub(crate) Arc<VoiceIngress>);
impl Drop for VoiceIngressTicket {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending -= 1;
        if state.pending == 0 {
            self.0.drained.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    #[tokio::test]
    async fn sealed_recording_waits_for_active_and_queued_tickets_and_cannot_reopen() {
        let recording = Arc::new(VoiceIngress::default());
        let active = recording.admit().unwrap();
        let queued = recording.admit().unwrap();
        recording.seal();
        assert!(recording.admit().is_none());
        let drain = recording.drain();
        tokio::pin!(drain);
        assert!(drain.as_mut().now_or_never().is_none());
        drop(queued);
        assert!(drain.as_mut().now_or_never().is_none());
        drop(active);
        drain.await;
        recording.cancel();
        recording.cancel();
        assert!(recording.cancelled.is_cancelled());
        let replacement = Arc::new(VoiceIngress::default());
        assert!(replacement.admit().is_some());
        assert!(!replacement.cancelled.is_cancelled());
    }
}
