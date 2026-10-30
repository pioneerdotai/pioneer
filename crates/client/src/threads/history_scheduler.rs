//! Bounded pending work for the existing, single history controller worker.
//! Wake publication and the worker's wait predicate share this mutex. No RPC or
//! domain lock is acquired while waiting. Limits are defensive resource budgets,
//! not measured throughput optima.
use super::registry::ThreadControllerRequest;
use pioneer_observability::{
    queues::{QueueBytes, QueueObserver, QueueOutcome},
    work::{Boundary, ExecutorRole, WorkItem, WorkKind},
};
use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

pub(crate) const MAX_JOBS: usize = 16;
// Interactive arrivals cannot consume the last durable-recovery reservation.
pub(crate) const MAX_ORDINARY_JOBS: usize = MAX_JOBS - 1;
const INTERACTIVE_BURST: usize = 4;
pub(super) const MAX_COMMAND_BYTES: usize = 1024 * 1024;

type Item = WorkItem<ThreadControllerRequest>;

fn discard(item: &Item, reason: QueueOutcome) {
    if let Some((queue, bytes)) = &item.queue {
        queue.dequeued(*bytes);
        // Stale/Dropped retain the reason; cancelling the processing guard
        // balances dequeue without reporting an execution as Processed.
        let _processing = queue.processing();
        queue.outcome(reason, 1);
    }
}

struct Pending {
    jobs: VecDeque<Item>,
    executing: bool,
    executing_recovery: bool,
    wake: bool,
    closed: bool,
    senders: usize,
    interactive_streak: usize,
}
struct Shared {
    pending: Mutex<Pending>,
    ready: Condvar,
}
pub(crate) struct ThreadControllerSender {
    shared: Arc<Shared>,
    observer: QueueObserver,
}
pub(crate) struct ThreadControllerReceiver {
    shared: Arc<Shared>,
}

pub(crate) fn channel() -> (ThreadControllerSender, ThreadControllerReceiver) {
    let shared = Arc::new(Shared {
        pending: Mutex::new(Pending {
            jobs: VecDeque::new(),
            executing: false,
            executing_recovery: false,
            wake: false,
            closed: false,
            senders: 1,
            interactive_streak: 0,
        }),
        ready: Condvar::new(),
    });
    (
        ThreadControllerSender {
            shared: shared.clone(),
            observer: QueueObserver::new(
                pioneer_observability::queues::QueueKind::ClientController,
                Some(MAX_JOBS),
                "bounded history jobs; wake is not payload",
            ),
        },
        ThreadControllerReceiver { shared },
    )
}
impl Clone for ThreadControllerSender {
    fn clone(&self) -> Self {
        self.shared
            .pending
            .lock()
            .expect("history pending poisoned")
            .senders += 1;
        Self {
            shared: self.shared.clone(),
            observer: self.observer.clone(),
        }
    }
}
impl Drop for ThreadControllerSender {
    fn drop(&mut self) {
        let mut pending = self
            .shared
            .pending
            .lock()
            .expect("history pending poisoned");
        pending.senders -= 1;
        if pending.senders == 0 {
            pending.closed = true;
            self.shared.ready.notify_all();
        }
    }
}
impl ThreadControllerSender {
    pub(crate) fn is_closed(&self) -> bool {
        self.shared
            .pending
            .lock()
            .expect("history pending poisoned")
            .closed
    }
    pub(crate) fn retire_stale(&self, mut current: impl FnMut(&ThreadControllerRequest) -> bool) {
        let mut pending = self
            .shared
            .pending
            .lock()
            .expect("history pending poisoned");
        pending.jobs.retain(|item| {
            if current(&item.value) {
                return true;
            }
            discard(item, QueueOutcome::Stale);
            false
        });
    }
    pub(crate) fn send(
        &self,
        value: ThreadControllerRequest,
    ) -> Result<(), std::sync::mpsc::SendError<ThreadControllerRequest>> {
        let mut pending = self
            .shared
            .pending
            .lock()
            .expect("history pending poisoned");
        if matches!(value, ThreadControllerRequest::Resume) {
            if pending.closed {
                return Err(std::sync::mpsc::SendError(value));
            }
            pending.wake = true;
            self.shared.ready.notify_one();
            return Ok(());
        }
        let item = WorkItem::new(
            value,
            Boundary::StdThread,
            WorkKind::ClientCommand,
            ExecutorRole::ClientController,
        )
        .observed(self.observer.clone(), QueueBytes::Unsupported);
        if pending.closed {
            if let Some((queue, bytes)) = &item.queue {
                queue.rejected_send(*bytes);
            }
            return Err(std::sync::mpsc::SendError(item.value));
        }
        if let Some(existing) = pending
            .jobs
            .iter_mut()
            .find(|queued| queued.value.same_job(&item.value))
        {
            existing.carrier.merge_link(&item.carrier);
            if let Some((queue, bytes)) = &item.queue {
                queue.coalesced_send(*bytes);
            }
            return Ok(());
        }
        let ordinary = pending
            .jobs
            .iter()
            .filter(|job| !job.value.is_recovery())
            .count()
            + usize::from(pending.executing && !pending.executing_recovery);
        if pending.jobs.len() + usize::from(pending.executing) >= MAX_JOBS
            || (!item.value.is_recovery() && ordinary >= MAX_ORDINARY_JOBS)
            || !item.value.command_fits(MAX_COMMAND_BYTES)
        {
            if let Some((queue, bytes)) = &item.queue {
                queue.rejected_send(*bytes);
            }
            return Err(std::sync::mpsc::SendError(item.value));
        }
        pending.jobs.push_back(item);
        pending.wake = true;
        self.shared.ready.notify_one();
        Ok(())
    }
}
impl ThreadControllerReceiver {
    /// Caller may hold the domain registry only for this nonblocking selection.
    /// Registry -> pending is the sole lock order; producers release domain locks
    /// before enqueue. Priority is evaluated now, rather than captured at enqueue.
    pub(crate) fn take_by(
        &self,
        mut interactive: impl FnMut(&ThreadControllerRequest) -> bool,
    ) -> Option<Item> {
        let mut pending = self
            .shared
            .pending
            .lock()
            .expect("history pending poisoned");
        assert!(!pending.executing);
        let low = pending
            .jobs
            .iter()
            .position(|item| !interactive(&item.value));
        let high = pending
            .jobs
            .iter()
            .position(|item| interactive(&item.value));
        let index = if pending.interactive_streak >= INTERACTIVE_BURST {
            low.or(high)
        } else {
            high.or(low)
        }?;
        if Some(index) == high {
            pending.interactive_streak = pending.interactive_streak.saturating_add(1);
        } else {
            pending.interactive_streak = 0;
        }
        pending.executing = true;
        pending.executing_recovery = pending.jobs[index].value.is_recovery();
        pending.jobs.remove(index)
    }
    /// The executing job owns a reserved slot, including while the RPC runs.
    pub(crate) fn finish(&self, continuation: Option<Item>) {
        let mut pending = self
            .shared
            .pending
            .lock()
            .expect("history pending poisoned");
        assert!(pending.executing);
        pending.executing = false;
        pending.executing_recovery = false;
        if let Some(item) = continuation {
            if !pending.closed {
                pending.jobs.push_back(item);
            } else {
                discard(&item, QueueOutcome::Dropped);
            }
        }
    }
    pub(crate) fn wait(&self, delay: Option<Duration>) -> bool {
        let pending = self
            .shared
            .pending
            .lock()
            .expect("history pending poisoned");
        let mut pending = match delay {
            Some(delay) => {
                self.shared
                    .ready
                    .wait_timeout_while(pending, delay, |p| {
                        !p.closed && !p.wake && p.jobs.is_empty()
                    })
                    .expect("history wait poisoned")
                    .0
            }
            None => self
                .shared
                .ready
                .wait_while(pending, |p| !p.closed && !p.wake && p.jobs.is_empty())
                .expect("history wait poisoned"),
        };
        pending.wake = false;
        !pending.closed
    }
    #[cfg(test)]
    pub(crate) fn try_recv(&self) -> Result<Item, std::sync::mpsc::TryRecvError> {
        let mut pending = self.shared.pending.lock().unwrap();
        pending.jobs.pop_front().ok_or(if pending.closed {
            std::sync::mpsc::TryRecvError::Disconnected
        } else {
            std::sync::mpsc::TryRecvError::Empty
        })
    }
    #[cfg(test)]
    pub(crate) fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Item, std::sync::mpsc::RecvTimeoutError> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Ok(item) = self.try_recv() {
                return Ok(item);
            }
            let mut pending = self.shared.pending.lock().unwrap();
            if pending.closed {
                return Err(std::sync::mpsc::RecvTimeoutError::Disconnected);
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return Err(std::sync::mpsc::RecvTimeoutError::Timeout);
            }
            pending = self
                .shared
                .ready
                .wait_timeout_while(pending, deadline - now, |p| !p.closed && p.jobs.is_empty())
                .unwrap()
                .0;
            drop(pending);
        }
    }
    #[cfg(test)]
    pub(crate) fn try_iter(&self) -> impl Iterator<Item = Item> + '_ {
        std::iter::from_fn(|| self.try_recv().ok())
    }
}
impl Drop for ThreadControllerReceiver {
    fn drop(&mut self) {
        let mut pending = self
            .shared
            .pending
            .lock()
            .expect("history pending poisoned");
        pending.closed = true;
        for item in pending.jobs.drain(..) {
            discard(&item, QueueOutcome::Dropped);
        }
        self.shared.ready.notify_all();
    }
}

/// Count serialized retained payload without allocating a second payload buffer.
pub(crate) fn fits_serialized(value: &impl serde::Serialize, limit: usize) -> bool {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_sub(bytes.len())
                .ok_or_else(|| std::io::Error::other("history payload budget exceeded"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Counter(limit), value).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(id: &str) -> ThreadControllerRequest {
        ThreadControllerRequest::Binding {
            id: id.into(),
            workspace: "ws".into(),
            generation: 1,
        }
    }
    fn id(request: &ThreadControllerRequest) -> &str {
        let ThreadControllerRequest::Binding { id, .. } = request else {
            panic!("expected binding payload")
        };
        id
    }

    #[test]
    fn coalesced_wakes_use_no_payload_slots_and_a_quantum_keeps_its_reservation() {
        let (sender, receiver) = channel();
        for _ in 0..10_000 {
            sender.send(ThreadControllerRequest::Resume).unwrap();
        }
        for index in 0..MAX_ORDINARY_JOBS {
            sender.send(binding(&format!("job-{index}"))).unwrap();
        }
        assert!(sender.send(binding("full")).is_err());
        let first = receiver.take_by(|_| false).unwrap();
        assert!(sender.send(binding("reserved")).is_err());
        receiver.finish(Some(first));
        let mut count = 0;
        while let Some(item) = receiver.take_by(|_| false) {
            assert!(id(&item.value).starts_with("job-"));
            count += 1;
            receiver.finish(None);
        }
        assert_eq!(count, MAX_ORDINARY_JOBS);
        assert!(matches!(
            receiver.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
    }

    #[test]
    fn recovery_is_selected_within_a_finite_interactive_burst() {
        let (sender, receiver) = channel();
        sender.send(binding("recovery")).unwrap();
        sender.send(binding("interactive-0")).unwrap();
        for index in 0..INTERACTIVE_BURST {
            let item = receiver
                .take_by(|request| id(request).starts_with("interactive-"))
                .unwrap();
            assert!(id(&item.value).starts_with("interactive-"));
            receiver.finish(None);
            sender
                .send(binding(&format!("interactive-{}", index + 1)))
                .unwrap();
        }
        let item = receiver
            .take_by(|request| id(request).starts_with("interactive-"))
            .unwrap();
        assert_eq!(id(&item.value), "recovery");
        receiver.finish(None);
        assert!(receiver.take_by(|_| true).is_some());
        receiver.finish(None);
    }

    #[test]
    fn discarded_payloads_finish_processing_without_losing_the_reason_or_epoch() {
        use pioneer_observability::live::{self, JournalPayload};
        let _lock = crate::OBSERVABILITY_TEST_LOCK.lock().unwrap();
        struct Restore(bool);
        impl Drop for Restore {
            fn drop(&mut self) {
                pioneer_observability::set_telemetry_enabled(self.0);
            }
        }
        let _restore = Restore(pioneer_observability::telemetry_enabled());
        for path in ["stale", "teardown", "closed-continuation", "old-epoch"] {
            pioneer_observability::set_telemetry_enabled(false);
            pioneer_observability::set_telemetry_enabled(true);
            let (mut sender, receiver) = channel();
            sender.observer = QueueObserver::new(
                pioneer_observability::queues::QueueKind::ClientController,
                Some(MAX_JOBS),
                "fixture history discard",
            );
            sender.send(binding("discarded")).unwrap();
            let queue_id = live::snapshot()
                .unwrap()
                .queues
                .iter()
                .find(|q| q.byte_definition == "fixture history discard")
                .unwrap()
                .id;
            // Exercise the real WorkItem receive/processing contract for an
            // executed payload; cancellation accounting belongs to the scheduler.
            let execute = |item: Item, continued: bool| {
                let queue = item.queue.as_ref().unwrap().0.clone();
                let (value, work) = item.received(
                    Boundary::StdThread,
                    WorkKind::ClientCommand,
                    ExecutorRole::ClientController,
                );
                let mut processing = queue.processing();
                let next = work.run(|| {
                    continued.then(|| {
                        WorkItem::new(
                            value,
                            Boundary::StdThread,
                            WorkKind::ClientCommand,
                            ExecutorRole::ClientController,
                        )
                        .observed(queue, QueueBytes::Unsupported)
                    })
                });
                processing.processed();
                next
            };
            match path {
                "stale" => {
                    sender.retire_stale(|_| false);
                    sender.send(binding("normal")).unwrap();
                    assert!(execute(receiver.take_by(|_| false).unwrap(), false).is_none());
                    receiver.finish(None);
                    drop(receiver);
                }
                "teardown" => drop(receiver),
                "closed-continuation" => {
                    let continuation = execute(receiver.take_by(|_| false).unwrap(), true);
                    // Finish the preceding real processing before closing the
                    // owner, then offer its already observed continuation.
                    drop(sender);
                    receiver.finish(continuation);
                    drop(receiver);
                    let snapshot = live::snapshot().unwrap();
                    let terminal = snapshot
                        .journal
                        .iter()
                        .find_map(|record| match &record.payload {
                            JournalPayload::Queue(q) if q.id == queue_id => Some(q),
                            _ => None,
                        })
                        .unwrap();
                    assert_eq!((terminal.depth, terminal.in_processing), (0, 0));
                    assert_eq!(
                        (terminal.dropped, terminal.cancelled, terminal.processed),
                        (1, 1, 1)
                    );
                    assert_eq!(terminal.abandoned_processing, 0);
                    continue;
                }
                "old-epoch" => {
                    pioneer_observability::set_telemetry_enabled(false);
                    pioneer_observability::set_telemetry_enabled(true);
                    sender.retire_stale(|_| false);
                    drop(receiver);
                    drop(sender);
                    let snapshot = live::snapshot().unwrap();
                    assert!(
                        !snapshot
                            .queues
                            .iter()
                            .any(|q| q.byte_definition == "fixture history discard")
                    );
                    assert!(!snapshot.journal.iter().any(|record| matches!(&record.payload, JournalPayload::Queue(q) if q.byte_definition == "fixture history discard")));
                    continue;
                }
                _ => unreachable!(),
            }
            let snapshot = live::snapshot().unwrap();
            let queue = snapshot.queues.iter().find(|q| q.id == queue_id).unwrap();
            assert_eq!((queue.depth, queue.in_processing), (0, 0));
            assert_eq!(queue.cancelled, 1);
            assert_eq!(
                (queue.stale, queue.dropped, queue.processed),
                if path == "stale" {
                    (1, 0, 1)
                } else {
                    (0, 1, 0)
                }
            );
            assert_eq!(queue.accounting_errors, 0);
            drop(sender);
            let snapshot = live::snapshot().unwrap();
            let terminal = snapshot
                .journal
                .iter()
                .find_map(|record| match &record.payload {
                    JournalPayload::Queue(q) if q.id == queue_id => Some(q),
                    _ => None,
                })
                .unwrap();
            assert_eq!(
                (
                    terminal.depth,
                    terminal.in_processing,
                    terminal.abandoned_processing
                ),
                (0, 0, 0)
            );
        }
    }

    #[test]
    fn wake_publication_before_or_during_wait_cannot_be_lost() {
        for publish_before_wait in [true, false] {
            for _ in 0..8 {
                let (sender, receiver) = channel();
                if publish_before_wait {
                    sender.send(ThreadControllerRequest::Resume).unwrap();
                }
                let (at_wait, started) = std::sync::mpsc::channel();
                let (completed, completion) = std::sync::mpsc::channel();
                let worker = std::thread::spawn(move || {
                    at_wait.send(()).unwrap();
                    completed.send(receiver.wait(None)).unwrap();
                });
                started.recv_timeout(Duration::from_secs(1)).unwrap();
                if !publish_before_wait {
                    sender.send(ThreadControllerRequest::Resume).unwrap();
                }
                assert!(completion.recv_timeout(Duration::from_secs(1)).unwrap());
                worker.join().unwrap();
            }
        }
    }
}
