//! Bounded, owned capture handoff. No DSP or transport work runs in admission.
//!
//! Desktop uses this directly; bridge adapters may use the same primitive with
//! their own owned source buffers. Budgets include the item being processed.
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc::{self, Receiver, SyncSender, TrySendError},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaptureQueueError {
    Closed,
    Overflow,
    Failed,
}

struct Shared<T> {
    sender: Mutex<Option<SyncSender<(T, usize)>>>,
    bytes: AtomicUsize,
    byte_limit: usize,
    failed: AtomicBool,
    fenced: AtomicBool,
    cancelled: AtomicBool,
}

/// A physical capture's admission fence. Closing is idempotent and serializes
/// with bounded copies already admitted; it never waits for worker capacity.
pub struct CaptureQueueSender<T>(Arc<Shared<T>>);
impl<T> Clone for CaptureQueueSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
pub struct CaptureQueueReceiver<T> {
    receiver: Receiver<(T, usize)>,
    shared: Arc<Shared<T>>,
}

pub struct CaptureQueueItem<T> {
    value: T,
    bytes: usize,
    shared: Arc<Shared<T>>,
}
impl<T> CaptureQueueItem<T> {
    pub fn value(&self) -> &T {
        &self.value
    }
}
impl<T> Drop for CaptureQueueItem<T> {
    fn drop(&mut self) {
        self.shared.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

pub fn capture_queue<T>(
    event_limit: usize,
    byte_limit: usize,
) -> (CaptureQueueSender<T>, CaptureQueueReceiver<T>) {
    let (sender, receiver) = mpsc::sync_channel(event_limit);
    let shared = Arc::new(Shared {
        sender: Mutex::new(Some(sender)),
        bytes: AtomicUsize::new(0),
        byte_limit,
        failed: AtomicBool::new(false),
        fenced: AtomicBool::new(false),
        cancelled: AtomicBool::new(false),
    });
    (
        CaptureQueueSender(shared.clone()),
        CaptureQueueReceiver { receiver, shared },
    )
}
impl<T> CaptureQueueSender<T> {
    /// Reserve before copying borrowed/reusable capture memory. The closure
    /// must perform only a bounded owned copy; it must not call DSP or IO.
    pub fn try_push_with(
        &self,
        bytes: usize,
        own: impl FnOnce() -> T,
    ) -> Result<(), CaptureQueueError> {
        let sender = self.0.sender.lock().expect("capture admission poisoned");
        if self.0.fenced.load(Ordering::Acquire) {
            return Err(CaptureQueueError::Closed);
        }
        let Some(sender) = sender.as_ref() else {
            return Err(CaptureQueueError::Closed);
        };
        if self.has_failed() {
            return Err(CaptureQueueError::Failed);
        }
        if self
            .0
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.0.byte_limit)
            })
            .is_err()
        {
            self.fail();
            return Err(CaptureQueueError::Overflow);
        }
        match sender.try_send((own(), bytes)) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.0.bytes.fetch_sub(bytes, Ordering::AcqRel);
                self.fail();
                Err(match error {
                    TrySendError::Full(_) => CaptureQueueError::Overflow,
                    TrySendError::Disconnected(_) => CaptureQueueError::Failed,
                })
            }
        }
    }
    /// Fence admission while keeping the worker alive until native stop has
    /// supplied its callback/error barrier. Call close afterwards to drain.
    pub fn fence(&self) {
        let _sender = self.0.sender.lock().expect("capture admission poisoned");
        self.0.fenced.store(true, Ordering::Release);
    }
    pub fn close(&self) {
        let mut sender = self.0.sender.lock().expect("capture admission poisoned");
        self.0.fenced.store(true, Ordering::Release);
        sender.take();
    }
    /// Reject invalid source metadata only while admission is still open.
    /// Late callbacks after stop cannot poison the accepted recording.
    pub fn reject(&self) -> bool {
        let mut sender = self.0.sender.lock().expect("capture admission poisoned");
        if self.0.fenced.load(Ordering::Acquire) {
            return false;
        }
        self.0.failed.store(true, Ordering::Release);
        self.0.fenced.store(true, Ordering::Release);
        sender.take();
        true
    }
    pub fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::Release);
        self.close();
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }
    pub fn fail(&self) {
        self.0.failed.store(true, Ordering::Release);
    }
    pub fn has_failed(&self) -> bool {
        self.0.failed.load(Ordering::Acquire)
    }
}
impl<T> CaptureQueueReceiver<T> {
    /// Worker-only blocking receive. Never call this from capture or UI.
    pub fn recv(&self) -> Option<CaptureQueueItem<T>> {
        let (value, bytes) = self.receiver.recv().ok()?;
        Some(CaptureQueueItem {
            value,
            bytes,
            shared: self.shared.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handoff_owns_reused_source_and_orders_controls_before_close() {
        let (sender, receiver) = capture_queue(3, 8);
        let mut source = vec![1_u8, 2, 3, 4];
        sender
            .try_push_with(source.len(), || Some(source.clone()))
            .unwrap();
        source.fill(9);
        sender.try_push_with(0, || None).unwrap();
        sender.fence();
        assert_eq!(
            sender.try_push_with(4, || panic!("late source must not be read")),
            Err(CaptureQueueError::Closed)
        );
        assert!(
            !sender.reject(),
            "late invalid callback must not poison accepted input"
        );
        assert!(!sender.has_failed());
        sender.close();
        assert_eq!(receiver.recv().unwrap().value(), &Some(vec![1, 2, 3, 4]));
        assert_eq!(receiver.recv().unwrap().value(), &None);
        assert!(receiver.recv().is_none());
    }

    #[test]
    fn byte_budget_includes_inflight_processing_and_overflow_is_sticky() {
        let (sender, receiver) = capture_queue(4, 4);
        sender.try_push_with(4, || vec![0_u8; 4]).unwrap();
        let active = receiver.recv().unwrap();
        assert_eq!(
            sender.try_push_with(1, || panic!("overflow must reject before copy")),
            Err(CaptureQueueError::Overflow)
        );
        drop(active);
        assert_eq!(
            sender.try_push_with(1, || vec![1]),
            Err(CaptureQueueError::Failed)
        );
        sender.close();
        assert!(receiver.recv().is_none());
    }

    #[test]
    fn control_events_cannot_bypass_bounded_queue_and_cancel_closes_admission() {
        let (sender, receiver) = capture_queue(1, 4);
        sender.try_push_with(0, || ()).unwrap();
        assert_eq!(
            sender.try_push_with(0, || ()),
            Err(CaptureQueueError::Overflow)
        );
        assert!(sender.has_failed());
        sender.cancel();
        sender.cancel();
        assert!(sender.is_cancelled());
        assert_eq!(
            sender.try_push_with(0, || ()),
            Err(CaptureQueueError::Closed)
        );
        drop(receiver.recv());
        assert!(receiver.recv().is_none());
    }
}
