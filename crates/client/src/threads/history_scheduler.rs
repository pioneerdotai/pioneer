//! Bounded pending and active work for the existing history controller owner.
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
    #[cfg(test)]
    test_reservation: Option<u64>,
    active: Vec<Active>,
    next_id: u64,
    wake: bool,
    closed: bool,
    senders: usize,
    interactive_streak: usize,
}
struct Active {
    id: u64,
    identity: super::registry::HistoryIdentity,
    recovery: bool,
    retired: bool,
    response: Option<crate::rpc::JsonRpcResponseSender>,
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
            #[cfg(test)]
            test_reservation: None,
            active: Vec::new(),
            next_id: 0,
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
    /// Registry -> pending. Return handles so cancellation callbacks run after
    /// both locks have been released, with applicability already revoked.
    pub(crate) fn retire_history(
        &self,
        mut current: impl FnMut(&super::registry::HistoryIdentity) -> bool,
        queued_current: impl FnMut(&ThreadControllerRequest) -> bool,
    ) -> Vec<crate::rpc::JsonRpcResponseSender> {
        self.retire_stale(queued_current);
        let mut pending = self
            .shared
            .pending
            .lock()
            .expect("history pending poisoned");
        let mut retired = Vec::new();
        for active in &mut pending.active {
            if !active.retired && !current(&active.identity) {
                active.retired = true;
                if let Some(response) = active.response.take() {
                    retired.push(response);
                }
            }
        }
        if !retired.is_empty() {
            pending.wake = true;
            self.shared.ready.notify_one();
        }
        retired
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
        if pending.active.iter().any(|a| {
            !a.retired && a.identity.semantic() && a.identity == item.value.history_identity()
        }) {
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
            + pending.active.iter().filter(|a| !a.recovery).count();
        if pending.jobs.len() + pending.active.len() >= MAX_JOBS
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
    pub(crate) fn take_ready_by(
        &self,
        mut interactive: impl FnMut(&ThreadControllerRequest) -> bool,
    ) -> Option<(u64, Item)> {
        let mut p = self
            .shared
            .pending
            .lock()
            .expect("history pending poisoned");
        let barrier_waiting = p.jobs.iter().any(|item| {
            !matches!(
                item.value,
                ThreadControllerRequest::Semantic(_) | ThreadControllerRequest::Subscribe(_)
            )
        });
        let eligible = |item: &Item| {
            if matches!(item.value, ThreadControllerRequest::Subscribe(_)) {
                return true;
            }
            let key = item.value.history_identity();
            p.active.is_empty()
                || (key.semantic()
                    && !barrier_waiting
                    && p.active.len() < 2
                    && p.active
                        .iter()
                        .all(|a| a.identity.semantic() && a.identity.thread() != key.thread()))
        };
        let low = p
            .jobs
            .iter()
            .position(|i| eligible(i) && !interactive(&i.value));
        let high = p
            .jobs
            .iter()
            .position(|i| eligible(i) && interactive(&i.value));
        let index = if p.interactive_streak >= INTERACTIVE_BURST {
            low.or(high)
        } else {
            high.or(low)
        }?;
        if Some(index) == high {
            p.interactive_streak = p.interactive_streak.saturating_add(1);
        } else {
            p.interactive_streak = 0;
        }
        let item = p.jobs.remove(index).unwrap();
        p.next_id = p
            .next_id
            .checked_add(1)
            .expect("history reservation exhausted");
        let id = p.next_id;
        p.active.push(Active {
            id,
            identity: item.value.history_identity(),
            recovery: item.value.is_recovery(),
            retired: false,
            response: None,
        });
        Some((id, item))
    }
    /// Caller holds registry: refine a selected queued identity to its prepared
    /// attempt, before attaching a waiter. No callback runs under either lock.
    pub(crate) fn refine(
        &self,
        id: u64,
        identity: super::registry::HistoryIdentity,
        current: bool,
    ) {
        let mut p = self
            .shared
            .pending
            .lock()
            .expect("history pending poisoned");
        let a = p
            .active
            .iter_mut()
            .find(|a| a.id == id)
            .expect("history reservation");
        a.identity = identity;
        a.retired |= !current;
    }
    pub(crate) fn attach(&self, id: u64, response: crate::rpc::JsonRpcResponseSender) -> bool {
        let mut p = self
            .shared
            .pending
            .lock()
            .expect("history pending poisoned");
        let Some(a) = p.active.iter_mut().find(|a| a.id == id && !a.retired) else {
            return false;
        };
        a.response = Some(response);
        true
    }
    pub(crate) fn applicable(&self, id: u64) -> bool {
        self.shared
            .pending
            .lock()
            .expect("history pending poisoned")
            .active
            .iter()
            .any(|a| a.id == id && !a.retired)
    }
    pub(crate) fn finish_active(&self, id: u64, continuation: Option<Item>) {
        let mut p = self
            .shared
            .pending
            .lock()
            .expect("history pending poisoned");
        let index = p
            .active
            .iter()
            .position(|a| a.id == id)
            .expect("history reservation");
        p.active.remove(index);
        if let Some(item) = continuation {
            if !p.closed {
                p.jobs.push_back(item);
            } else {
                discard(&item, QueueOutcome::Dropped);
            }
        }
    }
    pub(crate) fn completion_wake(&self) -> Arc<dyn Fn() + Send + Sync> {
        let weak = Arc::downgrade(&self.shared);
        Arc::new(move || {
            if let Some(shared) = weak.upgrade() {
                let mut p = shared.pending.lock().expect("history pending poisoned");
                p.wake = true;
                shared.ready.notify_one();
            }
        })
    }
    /// Selection was exhausted before this call. Producers and completions set
    /// wake under the same mutex; a full set of slots does not make us spin.
    pub(crate) fn wait_ready(&self, delay: Option<Duration>) -> bool {
        let p = self
            .shared
            .pending
            .lock()
            .expect("history pending poisoned");
        let mut p = match delay {
            Some(d) => {
                self.shared
                    .ready
                    .wait_timeout_while(p, d, |p| !p.closed && !p.wake)
                    .expect("history wait poisoned")
                    .0
            }
            None => self
                .shared
                .ready
                .wait_while(p, |p| !p.closed && !p.wake)
                .expect("history wait poisoned"),
        };
        p.wake = false;
        !p.closed
    }
    // Synchronous fixtures exercise the same reservation/selection algorithm.
    #[cfg(test)]
    pub(crate) fn take_by(
        &self,
        interactive: impl FnMut(&ThreadControllerRequest) -> bool,
    ) -> Option<Item> {
        let (id, item) = self.take_ready_by(interactive)?;
        let mut p = self.shared.pending.lock().unwrap();
        assert!(p.test_reservation.replace(id).is_none());
        Some(item)
    }
    #[cfg(test)]
    pub(crate) fn finish(&self, continuation: Option<Item>) {
        let id = self
            .shared
            .pending
            .lock()
            .unwrap()
            .test_reservation
            .take()
            .expect("fixture reservation");
        self.finish_active(id, continuation);
    }
    #[cfg(test)]
    pub(crate) fn wait(&self, delay: Option<Duration>) -> bool {
        self.wait_ready(delay)
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
        let responses: Vec<_> = pending
            .active
            .drain(..)
            .filter_map(|a| a.response)
            .collect();
        self.shared.ready.notify_all();
        drop(pending);
        for response in responses {
            response.cancel();
        }
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
