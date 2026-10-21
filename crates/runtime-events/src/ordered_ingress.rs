use pioneer_observability::queues::{QueueBytes, QueueKind, QueueObserver, QueueOutcome};
use pioneer_observability::work::{Boundary, ExecutorRole, WorkItem, WorkKind};
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};

#[derive(Debug, Clone)]
pub struct OrderedIngressConfig {
    pub flush_interval: Duration,
    pub max_pending_coalesced_keys: usize,
    pub max_pending_durable_events: usize,
}

impl Default for OrderedIngressConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_millis(25),
            max_pending_coalesced_keys: 4096,
            max_pending_durable_events: 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderedIngressClass<K> {
    Durable,
    Coalesced(K),
}

pub trait OrderedIngressEvent: Send + 'static + Sized {
    type Key: Clone + Eq + Hash + Send + 'static;

    fn ingress_class(&self) -> OrderedIngressClass<Self::Key>;
    fn coalesce(&mut self, newer: Self);
}

#[derive(Debug)]
pub enum OrderedIngressOffer<T> {
    Accepted,
    CoalescedKeyLimit(T),
    Closed(T),
}

struct OrderedCoalescedSegment<T: OrderedIngressEvent> {
    order: VecDeque<T::Key>,
    by_key: HashMap<T::Key, WorkItem<T>>,
    ready_at: tokio::time::Instant,
    force_ready: bool,
}

impl<T: OrderedIngressEvent> OrderedCoalescedSegment<T> {
    fn new(config: &OrderedIngressConfig) -> Self {
        Self {
            order: VecDeque::new(),
            by_key: HashMap::new(),
            ready_at: tokio::time::Instant::now() + config.flush_interval,
            force_ready: false,
        }
    }

    fn pop_front(&mut self) -> Option<WorkItem<T>> {
        while let Some(key) = self.order.pop_front() {
            if let Some(event) = self.by_key.remove(&key) {
                return Some(event);
            }
        }
        None
    }
}

enum OrderedIngressSegment<T: OrderedIngressEvent> {
    Coalesced(OrderedCoalescedSegment<T>),
    Durable {
        event: WorkItem<T>,
        _permit: OwnedSemaphorePermit,
    },
}

struct OrderedIngressState<T: OrderedIngressEvent> {
    pending: VecDeque<OrderedIngressSegment<T>>,
    pending_coalesced_keys: usize,
    closed: bool,
}

impl<T: OrderedIngressEvent> Default for OrderedIngressState<T> {
    fn default() -> Self {
        Self {
            pending: VecDeque::new(),
            pending_coalesced_keys: 0,
            closed: false,
        }
    }
}

struct OrderedIngressInner<T: OrderedIngressEvent> {
    state: StdMutex<OrderedIngressState<T>>,
    notify: Notify,
    durable_capacity: Arc<Semaphore>,
    config: OrderedIngressConfig,
    durable_observer: QueueObserver,
    coalesced_observer: QueueObserver,
}

pub struct OrderedEventIngress<T: OrderedIngressEvent> {
    inner: Arc<OrderedIngressInner<T>>,
}

impl<T: OrderedIngressEvent> Clone for OrderedEventIngress<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: OrderedIngressEvent> OrderedEventIngress<T> {
    pub fn spawn(output_tx: mpsc::Sender<WorkItem<T>>, config: OrderedIngressConfig) -> Self {
        Self::spawn_owned(output_tx, config).0
    }

    /// Return the worker to callers whose short-lived transport must join all
    /// of its resources before starting another service attempt.
    pub fn spawn_owned(
        output_tx: mpsc::Sender<WorkItem<T>>,
        config: OrderedIngressConfig,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let ingress = Self {
            inner: Arc::new(OrderedIngressInner {
                state: StdMutex::new(OrderedIngressState::default()),
                notify: Notify::new(),
                durable_capacity: Arc::new(Semaphore::new(
                    config.max_pending_durable_events.max(1),
                )),
                durable_observer: QueueObserver::new(
                    QueueKind::OrderedDurable,
                    Some(config.max_pending_durable_events.max(1)),
                    "unsupported: event payload not serialized",
                ),
                coalesced_observer: QueueObserver::new(
                    QueueKind::OrderedCoalesced,
                    Some(config.max_pending_coalesced_keys.max(1)),
                    "unsupported: event payload not serialized",
                ),
                config,
            }),
        };
        let worker = tokio::spawn(pioneer_observability::work::inherit(
            Boundary::OrderedIngress,
            WorkKind::WorkerLifecycle,
            ExecutorRole::Event,
            run_ordered_ingress(ingress.clone(), output_tx),
        ));
        (ingress, worker)
    }

    pub async fn offer(&self, event: T) -> OrderedIngressOffer<T> {
        let class = event.ingress_class();
        let durable_permit = if matches!(class, OrderedIngressClass::Durable) {
            let mut wait = self.inner.durable_observer.send_wait();
            let permit = pioneer_observability::live::waiting(
                pioneer_observability::live::WaitKind::ChannelCapacity,
                self.inner.durable_capacity.clone().acquire_owned(),
            )
            .await;
            wait.complete();
            match permit {
                Ok(permit) => Some(permit),
                Err(_) => {
                    self.inner
                        .durable_observer
                        .outcome(QueueOutcome::Rejected, 1);
                    return OrderedIngressOffer::Closed(event);
                }
            }
        } else {
            None
        };
        let mut state = self.inner.state.lock().expect("ordered ingress poisoned");
        if state.closed {
            return OrderedIngressOffer::Closed(event);
        }
        let mut event = WorkItem::new(
            event,
            Boundary::OrderedIngress,
            WorkKind::Event,
            ExecutorRole::Event,
        );
        match class {
            OrderedIngressClass::Durable => {
                event =
                    event.observed(self.inner.durable_observer.clone(), QueueBytes::Unsupported);
                if let Some(OrderedIngressSegment::Coalesced(segment)) = state.pending.back_mut() {
                    segment.force_ready = true;
                }
                state.pending.push_back(OrderedIngressSegment::Durable {
                    event,
                    _permit: durable_permit.expect("durable event must own a capacity permit"),
                });
            }
            OrderedIngressClass::Coalesced(key) => {
                if let Some(OrderedIngressSegment::Coalesced(segment)) = state.pending.back_mut()
                    && let Some(existing) = segment.by_key.get_mut(&key)
                {
                    self.inner
                        .coalesced_observer
                        .outcome(QueueOutcome::Coalesced, 1);
                    existing.carrier.merge_link(&event.carrier);
                    existing.value.coalesce(event.value);
                    drop(state);
                    self.inner.notify.notify_one();
                    return OrderedIngressOffer::Accepted;
                }
                if state.pending_coalesced_keys
                    >= self.inner.config.max_pending_coalesced_keys.max(1)
                {
                    self.inner
                        .coalesced_observer
                        .outcome(QueueOutcome::Rejected, 1);
                    return OrderedIngressOffer::CoalescedKeyLimit(event.value);
                }
                if !matches!(
                    state.pending.back(),
                    Some(OrderedIngressSegment::Coalesced(_))
                ) {
                    state.pending.push_back(OrderedIngressSegment::Coalesced(
                        OrderedCoalescedSegment::new(&self.inner.config),
                    ));
                }
                let Some(OrderedIngressSegment::Coalesced(segment)) = state.pending.back_mut()
                else {
                    unreachable!("coalesced segment must be present");
                };
                event = event.observed(
                    self.inner.coalesced_observer.clone(),
                    QueueBytes::Unsupported,
                );
                segment.order.push_back(key.clone());
                segment.by_key.insert(key, event);
                state.pending_coalesced_keys = state.pending_coalesced_keys.saturating_add(1);
            }
        }
        drop(state);
        self.inner.notify.notify_one();
        OrderedIngressOffer::Accepted
    }

    pub fn close(&self) {
        self.inner
            .state
            .lock()
            .expect("ordered ingress poisoned")
            .closed = true;
        self.inner.durable_capacity.close();
        self.inner.notify.notify_waiters();
    }
}

async fn run_ordered_ingress<T: OrderedIngressEvent>(
    ingress: OrderedEventIngress<T>,
    output_tx: mpsc::Sender<WorkItem<T>>,
) {
    loop {
        let notified = ingress.inner.notify.notified();
        let (closed, ready_at) = {
            let state = ingress
                .inner
                .state
                .lock()
                .expect("ordered ingress poisoned");
            let ready_at = match state.pending.front() {
                Some(OrderedIngressSegment::Durable { .. }) => Some(tokio::time::Instant::now()),
                Some(OrderedIngressSegment::Coalesced(segment)) => Some(
                    if segment.force_ready || tokio::time::Instant::now() >= segment.ready_at {
                        tokio::time::Instant::now()
                    } else {
                        segment.ready_at
                    },
                ),
                None => None,
            };
            (state.closed, ready_at)
        };
        let Some(ready_at) = ready_at else {
            if closed {
                return;
            }
            pioneer_observability::live::waiting(
                pioneer_observability::live::WaitKind::QueueItem,
                notified,
            )
            .await;
            continue;
        };
        if tokio::time::Instant::now() < ready_at {
            let remaining = ready_at.saturating_duration_since(tokio::time::Instant::now());
            let _sleep = pioneer_observability::live::WaitGuard::sleeping_until(
                pioneer_observability::work::clock_ms()
                    .saturating_add(remaining.as_millis().min(u64::MAX as u128) as u64),
            );
            tokio::select! {
                _ = tokio::time::sleep_until(ready_at) => {}
                _ = notified => {}
            }
            continue;
        }

        let Ok(permit) = pioneer_observability::live::waiting(
            pioneer_observability::live::WaitKind::ChannelCapacity,
            output_tx.reserve(),
        )
        .await
        else {
            ingress.close();
            return;
        };
        let event = {
            let mut state = ingress
                .inner
                .state
                .lock()
                .expect("ordered ingress poisoned");
            if matches!(
                state.pending.front(),
                Some(OrderedIngressSegment::Durable { .. })
            ) {
                let Some(OrderedIngressSegment::Durable { event, .. }) = state.pending.pop_front()
                else {
                    unreachable!("front durable segment changed");
                };
                Some(event)
            } else {
                let (event, segment_empty) = match state.pending.front_mut() {
                    Some(OrderedIngressSegment::Coalesced(segment)) => {
                        let event = segment.pop_front();
                        let empty = segment.by_key.is_empty();
                        (event, empty)
                    }
                    Some(OrderedIngressSegment::Durable { .. }) => unreachable!(),
                    None => (None, false),
                };
                if event.is_some() {
                    state.pending_coalesced_keys = state.pending_coalesced_keys.saturating_sub(1);
                }
                if segment_empty {
                    state.pending.pop_front();
                }
                event
            }
        };
        if let Some(mut event) = event {
            if let Some((queue, bytes)) = event.queue.take() {
                queue.dequeued(bytes);
                queue.outcome(QueueOutcome::Processed, 1);
            }
            permit.send(event);
        }
    }
}
