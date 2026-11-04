use axum::extract::ws::Message;
use pioneer_observability::{
    queues::{QueueBytes, QueueKind, QueueObserver, QueueOutcome},
    work::{Boundary, ExecutorRole, WorkItem, WorkKind},
};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, mpsc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdmissionClass {
    Data,
    ReplyControl,
}

#[derive(Default)]
struct Usage {
    count: usize,
    bytes: usize,
    data_count: usize,
    data_bytes: usize,
    closed: bool,
    aborted: bool,
    drain_deadline: Option<tokio::time::Instant>,
}
struct Admission {
    usage: Mutex<Usage>,
    changed: Notify,
    count: usize,
    data_count: usize,
    bytes: usize,
    data_bytes: usize,
}
impl Admission {
    fn close(&self) {
        let mut u = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        u.closed = true;
        u.drain_deadline
            .get_or_insert_with(|| tokio::time::Instant::now() + super::OUTBOUND_CLOSE_GRACE);
        drop(u);
        self.changed.notify_waiters();
    }
    fn abort(&self) {
        let mut u = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        u.closed = true;
        u.aborted = true;
        drop(u);
        self.changed.notify_waiters();
    }
}
/// Held through socket completion, independently of observer dequeue accounting.
pub(crate) struct Reservation {
    admission: Arc<Admission>,
    class: AdmissionClass,
    bytes: usize,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let mut u = self
            .admission
            .usage
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        u.count -= 1;
        u.bytes -= self.bytes;
        if self.class == AdmissionClass::Data {
            u.data_count -= 1;
            u.data_bytes -= self.bytes;
        }
        drop(u);
        self.admission.changed.notify_waiters();
    }
}
pub(crate) struct OutboundEnvelope {
    pub(crate) item: WorkItem<Message>,
    pub(crate) reservation: Reservation,
}
pub(crate) struct OutboundReceiver {
    rx: mpsc::Receiver<OutboundEnvelope>,
    admission: Arc<Admission>,
}
impl OutboundReceiver {
    pub(crate) fn seal(&mut self) {
        self.admission.close();
        self.rx.close();
    }
    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> Result<OutboundEnvelope, mpsc::error::TryRecvError> {
        self.rx.try_recv()
    }
    pub(crate) async fn recv(&mut self) -> Option<OutboundEnvelope> {
        self.rx.recv().await
    }
}
impl Drop for OutboundReceiver {
    fn drop(&mut self) {
        self.admission.abort();
    }
}
#[derive(Clone)]
pub(crate) struct SessionOutbound {
    tx: mpsc::Sender<OutboundEnvelope>,
    admission: Arc<Admission>,
    observer: QueueObserver,
}
impl SessionOutbound {
    pub(crate) fn channel(
        capacity: usize,
        bytes: usize,
        reserved_bytes: usize,
    ) -> (Self, OutboundReceiver) {
        assert!(capacity >= 2 && capacity <= tokio::sync::Semaphore::MAX_PERMITS);
        assert!(reserved_bytes > 0 && reserved_bytes < bytes);
        let reserve = 16.min((capacity / 8).max(1));
        let admission = Arc::new(Admission {
            usage: Mutex::new(Usage::default()),
            changed: Notify::new(),
            count: capacity,
            data_count: capacity - reserve,
            bytes,
            data_bytes: bytes - reserved_bytes,
        });
        let (tx, rx) = mpsc::channel(capacity);
        let observer = QueueObserver::new(
            QueueKind::GatewayOutbound,
            Some(capacity),
            "WebSocket payload bytes; framing/envelope excluded",
        );
        (
            Self {
                tx,
                admission: admission.clone(),
                observer,
            },
            OutboundReceiver { rx, admission },
        )
    }
    pub(crate) fn close(&self) {
        self.admission.close();
    }
    pub(crate) fn abort(&self) {
        self.admission.abort();
    }
    /// Graceful seal stops producers immediately but permits FIFO drain until
    /// its original deadline. Repeated seal cannot extend that deadline.
    pub(crate) async fn writer_stopped(&self) {
        loop {
            let notified = self.admission.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (aborted, deadline) = {
                let u = self
                    .admission
                    .usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                (u.aborted, u.drain_deadline)
            };
            if aborted {
                return;
            }
            match deadline {
                Some(deadline) => tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => return,
                    _ = notified => {},
                },
                None => notified.await,
            }
        }
    }
    pub(crate) async fn closed(&self) {
        loop {
            let notified = self.admission.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .admission
                .usage
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .closed
            {
                return;
            }
            notified.await;
        }
    }
    pub(crate) fn capacity(&self) -> usize {
        let u = self
            .admission
            .usage
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if u.closed {
            0
        } else {
            self.admission.count - u.count
        }
    }
    /// Synchronous admission and enqueue are one transaction. No reservation is
    /// held while waiting; auth gates can call this at the actual enqueue.
    pub(crate) fn try_send_class(
        &self,
        message: Message,
        class: AdmissionClass,
    ) -> Result<(), mpsc::error::TrySendError<Message>> {
        let bytes = message_bytes(&message);
        let mut u = self
            .admission
            .usage
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if u.closed || self.tx.is_closed() {
            return Err(mpsc::error::TrySendError::Closed(message));
        }
        let ceiling = if class == AdmissionClass::Data {
            self.admission.data_bytes
        } else {
            self.admission.bytes
        };
        if bytes > ceiling {
            u.closed = true;
            u.aborted = true;
            drop(u);
            self.admission.changed.notify_waiters();
            tracing::debug!(
                event = "websocket_outbound_queue",
                outcome = "rejected",
                reason_code = "payload_oversized"
            );
            self.observer.outcome(QueueOutcome::Rejected, 1);
            return Err(mpsc::error::TrySendError::Closed(message));
        }
        // Subtraction avoids overflow for all accepted configurations.
        if u.count == self.admission.count
            || bytes > self.admission.bytes - u.bytes
            || (class == AdmissionClass::Data
                && (u.data_count == self.admission.data_count
                    || bytes > self.admission.data_bytes - u.data_bytes))
        {
            self.observer.outcome(QueueOutcome::Rejected, 1);
            return Err(mpsc::error::TrySendError::Full(message));
        }
        let permit = match self.tx.try_reserve() {
            Ok(p) => p,
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(mpsc::error::TrySendError::Full(message));
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(mpsc::error::TrySendError::Closed(message));
            }
        };
        let item = WorkItem::new(
            message,
            Boundary::EventLane,
            WorkKind::Event,
            ExecutorRole::Gateway,
        )
        .observed(self.observer.clone(), QueueBytes::Known(bytes as u64));
        u.count += 1;
        u.bytes += bytes;
        if class == AdmissionClass::Data {
            u.data_count += 1;
            u.data_bytes += bytes;
        }
        // Receiver teardown closes admission under this same mutex before
        // dropping the physical receiver. Keep it through the nonblocking send:
        // a reserved channel slot cannot be published after receiver teardown.
        // All fallible preparation precedes charging the RAII reservation.
        let reservation = Reservation {
            admission: self.admission.clone(),
            class,
            bytes,
        };
        permit.send(OutboundEnvelope { item, reservation });
        drop(u);
        Ok(())
    }
    pub(crate) fn try_send(
        &self,
        message: Message,
    ) -> Result<(), mpsc::error::TrySendError<Message>> {
        self.try_send_class(message, AdmissionClass::ReplyControl)
    }
    pub(crate) fn try_send_data(
        &self,
        message: Message,
    ) -> Result<(), mpsc::error::TrySendError<Message>> {
        self.try_send_class(message, AdmissionClass::Data)
    }
    pub(crate) async fn send(
        &self,
        mut message: Message,
    ) -> Result<(), mpsc::error::SendError<Message>> {
        let mut wait = self.observer.send_wait();
        let _live_wait = pioneer_observability::live::WaitGuard::new(
            pioneer_observability::live::WaitKind::ChannelCapacity,
        );
        loop {
            let notified = self.admission.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.try_send(message) {
                Ok(()) => {
                    wait.complete();
                    return Ok(());
                }
                Err(mpsc::error::TrySendError::Closed(m)) => return Err(mpsc::error::SendError(m)),
                Err(mpsc::error::TrySendError::Full(m)) => message = m,
            }
            notified.await;
        }
    }
}
fn message_bytes(message: &Message) -> usize {
    match message {
        Message::Text(t) => t.len(),
        Message::Binary(b) | Message::Ping(b) | Message::Pong(b) => b.len(),
        Message::Close(f) => f.as_ref().map_or(0, |f| f.reason.len() + 2),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    fn text(bytes: usize) -> Message {
        Message::Text("x".repeat(bytes).into())
    }

    #[tokio::test]
    async fn concurrent_data_leaves_reply_reserve_and_preserves_fifo() {
        let (tx, mut rx) = SessionOutbound::channel(16, 100, 20);
        let barrier = Arc::new(tokio::sync::Barrier::new(33));
        let mut producers = Vec::new();
        for _ in 0..32 {
            let tx = tx.clone();
            let barrier = barrier.clone();
            producers.push(tokio::spawn(async move {
                barrier.wait().await;
                tx.try_send_data(text(1)).is_ok()
            }));
        }
        barrier.wait().await;
        let mut accepted = 0;
        for producer in producers {
            accepted += usize::from(producer.await.unwrap());
        }
        assert_eq!(accepted, 14);
        tx.send(Message::Text("reply-one".into())).await.unwrap();
        tx.try_send(Message::Text("reply-two".into())).unwrap();
        assert!(tx.try_send(Message::Pong(Vec::new().into())).is_err());
        for _ in 0..14 {
            let frame = rx.recv().await.unwrap();
            let (message, _) =
                frame
                    .item
                    .received(Boundary::EventLane, WorkKind::Event, ExecutorRole::Gateway);
            assert!(matches!(message, Message::Text(t) if t == "x"));
        }
        for expected in ["reply-one", "reply-two"] {
            let frame = rx.recv().await.unwrap();
            let (message, _) =
                frame
                    .item
                    .received(Boundary::EventLane, WorkKind::Event, ExecutorRole::Gateway);
            assert!(matches!(message, Message::Text(t) if t == expected));
        }
    }

    #[tokio::test]
    async fn writer_held_large_reply_keeps_bytes_until_completion_and_wait_cancel_is_free() {
        let (tx, mut rx) = SessionOutbound::channel(8, 100, 20);
        tx.send(text(90)).await.unwrap();
        let held = rx.recv().await.unwrap();
        // Observer dequeue deliberately occurs before the write completes.
        let (_message, _work) =
            held.item
                .received(Boundary::EventLane, WorkKind::Event, ExecutorRole::Gateway);
        let mut pending = Box::pin(tx.send(text(20)));
        assert!(pending.as_mut().now_or_never().is_none());
        drop(pending);
        assert!(tx.try_send(text(11)).is_err());
        drop(_message);
        drop(_work);
        drop(held.reservation);
        tx.try_send(text(100)).unwrap();
        drop(rx);
        assert!(tx.send(text(1)).await.is_err());
    }

    #[tokio::test]
    async fn data_byte_reserve_and_oversized_are_terminal_without_waiting() {
        let (tx, mut rx) = SessionOutbound::channel(8, 100, 20);
        tx.try_send_data(text(80)).unwrap();
        assert!(tx.try_send_data(text(1)).is_err());
        tx.send(text(20)).await.unwrap();
        drop(rx.recv().await.unwrap());
        drop(rx.recv().await.unwrap());
        assert!(tx.send(text(101)).now_or_never().unwrap().is_err());
        assert!(tx.try_send(text(1)).is_err());
        tx.closed()
            .now_or_never()
            .expect("oversized retires admission");
        let (tx, _rx) = SessionOutbound::channel(8, 100, 20);
        assert!(tx.try_send_data(text(81)).is_err());
        assert!(tx.try_send(text(1)).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn graceful_seal_rejects_new_producers_without_extending_drain_deadline() {
        let (tx, _rx) = SessionOutbound::channel(2, 100, 20);
        tx.send(text(100)).await.unwrap();
        let mut waiting = Box::pin(tx.send(text(1)));
        assert!(waiting.as_mut().now_or_never().is_none());
        tx.close();
        assert!(waiting.await.is_err());
        assert!(tx.writer_stopped().now_or_never().is_none());
        tokio::time::advance(std::time::Duration::from_millis(200)).await;
        tx.close();
        assert!(tx.writer_stopped().now_or_never().is_none());
        tokio::time::advance(std::time::Duration::from_millis(50)).await;
        tx.writer_stopped()
            .now_or_never()
            .expect("repeat close must not extend the original 250ms grace");
        let (tx, _rx) = SessionOutbound::channel(2, 100, 20);
        tx.close();
        tx.abort();
        tx.writer_stopped()
            .now_or_never()
            .expect("abort must escalate seal immediately");
    }

    #[tokio::test]
    async fn writer_panic_retires_pending_and_late_producers() {
        let (tx, mut rx) = SessionOutbound::channel(2, 100, 20);
        tx.send(text(100)).await.unwrap();
        let mut waiting = Box::pin(tx.send(text(1)));
        assert!(waiting.as_mut().now_or_never().is_none());
        let writer = tokio::spawn(async move {
            let _held = rx.recv().await.unwrap();
            panic!("writer failed while owning payload");
        });
        assert!(writer.await.unwrap_err().is_panic());
        assert!(waiting.await.is_err());
        assert!(tx.try_send_data(text(1)).is_err());
    }

    #[tokio::test]
    async fn retirement_and_receiver_failure_wake_waiters_and_release_envelopes() {
        for explicit_close in [false, true] {
            let (tx, mut rx) = SessionOutbound::channel(2, 100, 20);
            tx.send(text(100)).await.unwrap();
            let held = rx.recv().await.unwrap();
            let mut waiting = Box::pin(tx.send(text(1)));
            assert!(waiting.as_mut().now_or_never().is_none());
            if explicit_close {
                tx.close();
            } else {
                drop(rx);
            }
            assert!(waiting.await.is_err());
            drop(held);
            assert!(tx.send(text(1)).await.is_err());
        }
    }
}
