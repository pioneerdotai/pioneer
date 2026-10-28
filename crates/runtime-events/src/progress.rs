use pioneer_observability::queues::{QueueBytes, QueueKind, QueueObserver, QueueOutcome};
use pioneer_observability::work::{Boundary, ExecutorRole, WorkCarrier, WorkItem, WorkKind};
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::Duration;

use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, ItemDeltaNotification, ItemDeltaStream,
    ItemHeartbeatSource, ProgressCoalescingKey, TurnItemType,
};
use serde_json::Value as JsonValue;
use tokio::sync::broadcast;
use tracing::debug;

#[derive(Debug, Default)]
struct OwnedProgressFlush {
    task: Option<Arc<tokio::sync::Mutex<ProgressFlushCompletion>>>,
    wake: Arc<tokio::sync::Notify>,
    failure: Option<String>,
    closed: bool,
}
#[derive(Debug)]
struct ProgressFlushCompletion {
    task: Option<tokio::task::JoinHandle<()>>,
    failure: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProgressCoalescerConfig {
    pub flush_interval: Duration,
    pub max_pending_keys: usize,
    pub max_append_bytes_per_key: usize,
    pub max_snapshot_bytes_per_key: usize,
    pub max_flush_batch_size: usize,
}

impl Default for ProgressCoalescerConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_millis(150),
            max_pending_keys: 4096,
            max_append_bytes_per_key: 64 * 1024,
            max_snapshot_bytes_per_key: 16 * 1024,
            max_flush_batch_size: 256,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProgressMergeBehavior {
    Append,
    Snapshot,
}

impl ProgressMergeBehavior {
    fn for_stream(stream: ItemDeltaStream) -> Self {
        match stream {
            ItemDeltaStream::AgentMessage
            | ItemDeltaStream::Generic
            | ItemDeltaStream::Stdout
            | ItemDeltaStream::Stderr
            | ItemDeltaStream::FileChange => Self::Append,
            ItemDeltaStream::ToolProgress => Self::Snapshot,
        }
    }
}

#[derive(Debug, Clone)]
struct PendingProgress {
    work: WorkCarrier,
    enqueued_monotonic_ms: Option<u64>,
    notification: ItemDeltaNotification,
    behavior: ProgressMergeBehavior,
}

impl PendingProgress {
    fn new(notification: ItemDeltaNotification, behavior: ProgressMergeBehavior) -> Self {
        Self {
            work: pioneer_observability::work::capture(),
            enqueued_monotonic_ms: pioneer_observability::telemetry_enabled()
                .then(|| pioneer_observability::work::clock_ms()),
            notification,
            behavior,
        }
    }

    fn merge(&mut self, mut next: ItemDeltaNotification, config: &ProgressCoalescerConfig) {
        self.work
            .merge_link(&pioneer_observability::work::capture());
        match self.behavior {
            ProgressMergeBehavior::Append => {
                self.notification.delta.push_str(next.delta.as_str());
                let truncated = bound_delta(
                    &mut self.notification.delta,
                    config.max_append_bytes_per_key,
                );
                if next.markdown.is_some() {
                    self.notification.markdown = next.markdown.take();
                }
                if next.markdown_version.is_some() {
                    self.notification.markdown_version = next.markdown_version;
                }
                if next.payload.is_some() {
                    self.notification.payload = next.payload.take();
                }
                annotate_progress_payload(&mut self.notification, true, truncated);
            }
            ProgressMergeBehavior::Snapshot => {
                let truncated = bound_delta(&mut next.delta, config.max_snapshot_bytes_per_key);
                annotate_progress_payload(&mut next, true, truncated);
                self.notification = next;
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct HeartbeatKey {
    thread_id: String,
    turn_id: String,
    item_id: String,
}

#[derive(Debug, Clone)]
struct PendingHeartbeat {
    work: WorkCarrier,
    enqueued_monotonic_ms: Option<u64>,
    workspace_id: String,
    thread_id: String,
    turn_id: String,
    item_id: String,
    item_type: TurnItemType,
    source: ItemHeartbeatSource,
}

#[derive(Debug, Default)]
struct ProgressCoalescerState {
    pending: HashMap<ProgressCoalescingKey, PendingProgress>,
    pending_delta_bytes: u64,
    heartbeats: HashMap<HeartbeatKey, PendingHeartbeat>,
    flush_scheduled: bool,
    closed: bool,
}

#[derive(Debug)]
struct ProgressCoalescerInner {
    observer: QueueObserver,
    delivery_observer: QueueObserver,
    delivery_observation: StdMutex<()>,
    state: StdMutex<ProgressCoalescerState>,
    live_tx: broadcast::Sender<WorkItem<AgentProgressEvent>>,
    config: ProgressCoalescerConfig,
    owned_flush: Option<StdMutex<OwnedProgressFlush>>,
}

#[derive(Debug, Clone)]
pub struct ProgressCoalescer {
    inner: Arc<ProgressCoalescerInner>,
}

impl ProgressCoalescer {
    pub fn new(live_capacity: usize, config: ProgressCoalescerConfig) -> Self {
        let (live_tx, _) = broadcast::channel(live_capacity.max(1));
        Self {
            inner: Arc::new(ProgressCoalescerInner {
                state: StdMutex::new(ProgressCoalescerState::default()),
                delivery_observation: StdMutex::new(()),
                live_tx,
                delivery_observer: QueueObserver::new(
                    QueueKind::EventProgressBroadcast,
                    Some(live_capacity.max(1).next_power_of_two()),
                    "unsupported payload bytes; depth=retained broadcast outputs; enqueue=accepted merged outputs, processed=receiver deliveries, delivery_dropped=lagged deliveries; fanout has no FIFO event conservation",
                ),
                observer: QueueObserver::new(
                    QueueKind::EventProgress,
                    Some(config.max_pending_keys),
                    "estimate: pending delta UTF-8 bytes only; payload/markdown/envelope excluded",
                ),
                config,
                owned_flush: None,
            }),
        }
    }

    /// Same coalescer/scheduling path, retaining actual flush workers for a
    /// short-lived native owner. Legacy constructors retain their behavior.
    pub fn new_owned(live_capacity: usize, config: ProgressCoalescerConfig) -> Self {
        let mut result = Self::new(live_capacity, config);
        Arc::get_mut(&mut result.inner)
            .expect("fresh coalescer")
            .owned_flush = Some(StdMutex::new(OwnedProgressFlush::default()));
        result
    }
    pub async fn shutdown_and_wait(&self) -> Result<(), String> {
        let Some(workers) = &self.inner.owned_flush else {
            return Err("progress flush ownership is unknown".into());
        };
        let tasks = {
            let mut workers = workers.lock().expect("progress worker ownership poisoned");
            workers.closed = true;
            self.inner
                .state
                .lock()
                .expect("progress coalescer poisoned")
                .closed = true;
            workers.wake.notify_one();
            workers.task.clone()
        };
        for completion in tasks.into_iter() {
            let mut completion = completion.lock().await;
            if let Some(task) = completion.task.as_mut() {
                let result = task.await;
                completion.task.take();
                if result.is_err() {
                    completion.failure = Some("progress flush worker failed".into());
                }
            }
            if let Some(error) = &completion.failure {
                workers
                    .lock()
                    .expect("progress ownership poisoned")
                    .failure
                    .get_or_insert(error.clone());
            }
        }
        // No recursive worker or offer can register after the closed gate.
        self.flush_all().await;
        let mut workers = workers.lock().expect("progress ownership poisoned");
        workers.task.take();
        match &workers.failure {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    pub fn subscribe_live(&self) -> ProgressEventReceiver {
        ProgressEventReceiver {
            receiver: Some(self.inner.live_tx.subscribe()),
            owner: Arc::downgrade(&self.inner),
            pending_work: Default::default(),
        }
    }

    pub fn offer(&self, event: AgentProgressEvent) {
        let Some(notification) = progress_event_to_item_delta(event) else {
            return;
        };
        self.offer_notification(notification);
    }

    pub fn offer_heartbeat(
        &self,
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
    ) {
        self.offer_heartbeat_with_source(
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            item_type,
            ItemHeartbeatSource::OwnerLease,
        );
    }

    pub fn offer_confirmed_activity(
        &self,
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
    ) {
        self.offer_heartbeat_with_source(
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            item_type,
            ItemHeartbeatSource::ConfirmedExternalActivity,
        );
    }

    fn offer_heartbeat_with_source(
        &self,
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        source: ItemHeartbeatSource,
    ) {
        let mut should_schedule = false;
        {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("progress coalescer poisoned");
            if state.closed {
                return;
            }
            let key = HeartbeatKey {
                thread_id: thread_id.clone(),
                turn_id: turn_id.clone(),
                item_id: item_id.clone(),
            };
            let is_new = !state.heartbeats.contains_key(&key);
            if is_new && self.total_pending_keys(&state) >= self.inner.config.max_pending_keys {
                self.inner.observer.outcome(QueueOutcome::Rejected, 1);
                debug!(
                    thread_id,
                    turn_id, item_id, "dropping redundant heartbeat at coalescer key limit"
                );
                return;
            }
            let source = if matches!(source, ItemHeartbeatSource::ConfirmedExternalActivity)
                || state.heartbeats.get(&key).is_some_and(|heartbeat| {
                    matches!(
                        heartbeat.source,
                        ItemHeartbeatSource::ConfirmedExternalActivity
                    )
                }) {
                ItemHeartbeatSource::ConfirmedExternalActivity
            } else {
                ItemHeartbeatSource::OwnerLease
            };
            if is_new {
                self.inner.observer.enqueued(QueueBytes::Estimate(0));
            } else {
                self.inner.observer.outcome(QueueOutcome::Coalesced, 1);
            }
            let mut work = pioneer_observability::work::capture();
            let mut enqueued_monotonic_ms = pioneer_observability::telemetry_enabled()
                .then(pioneer_observability::work::clock_ms);
            if let Some(previous) = state.heartbeats.get(&key) {
                let mut combined = previous.work.clone();
                combined.merge_link(&work);
                work = combined;
                enqueued_monotonic_ms = previous.enqueued_monotonic_ms;
            }
            state.heartbeats.insert(
                key,
                PendingHeartbeat {
                    work,
                    enqueued_monotonic_ms,
                    workspace_id,
                    thread_id,
                    turn_id,
                    item_id,
                    item_type,
                    source,
                },
            );
            if !state.flush_scheduled {
                state.flush_scheduled = true;
                should_schedule = true;
            }
        }
        if should_schedule {
            self.schedule_flush();
        }
    }

    pub async fn flush_key(&self, key: &ProgressCoalescingKey) {
        let events = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("progress coalescer poisoned");
            remove_progress(&mut state, key)
                .map(|pending| vec![progress_event(pending)])
                .unwrap_or_default()
        };
        self.send_live_events(events);
    }

    /// Existing coalescer keys are capped by max_pending_keys. Empty historic
    /// targets need no flush; no extra tracking state is kept here.
    pub fn pending_targets_for_item(
        &self,
        workspace_id: &str,
        item_id: &str,
    ) -> Vec<(String, String)> {
        let state = self
            .inner
            .state
            .lock()
            .expect("progress coalescer poisoned");
        let mut targets = Vec::new();
        for key in state
            .pending
            .keys()
            .filter(|k| k.workspace_id == workspace_id && k.item_id == item_id)
        {
            let target = (key.thread_id.clone(), key.turn_id.clone());
            if !targets.contains(&target) {
                targets.push(target);
            }
        }
        for key in state
            .heartbeats
            .values()
            .filter(|k| k.workspace_id == workspace_id && k.item_id == item_id)
        {
            let target = (key.thread_id.clone(), key.turn_id.clone());
            if !targets.contains(&target) {
                targets.push(target);
            }
        }
        targets
    }

    pub async fn flush_item(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
    ) {
        let events = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("progress coalescer poisoned");
            let keys = state
                .pending
                .keys()
                .filter(|key| {
                    key.workspace_id == workspace_id
                        && key.thread_id == thread_id
                        && key.turn_id == turn_id
                        && key.item_id == item_id
                })
                .cloned()
                .collect::<Vec<_>>();
            let heartbeat_keys = state
                .heartbeats
                .keys()
                .filter(|key| {
                    key.thread_id == thread_id && key.turn_id == turn_id && key.item_id == item_id
                })
                .cloned()
                .collect::<Vec<_>>();
            let mut events = Vec::with_capacity(keys.len() + heartbeat_keys.len());
            for key in keys {
                if let Some(pending) = remove_progress(&mut state, &key) {
                    events.push(progress_event(pending));
                }
            }
            for key in heartbeat_keys {
                if let Some(heartbeat) = state.heartbeats.remove(&key) {
                    events.push(heartbeat_event(heartbeat));
                }
            }
            update_flush_scheduled(&mut state);
            events
        };
        self.send_live_events(events);
    }

    pub async fn flush_turn(&self, thread_id: &str, turn_id: &str) {
        let events = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("progress coalescer poisoned");
            let keys = state
                .pending
                .keys()
                .filter(|key| key.thread_id == thread_id && key.turn_id == turn_id)
                .cloned()
                .collect::<Vec<_>>();
            let heartbeat_keys = state
                .heartbeats
                .keys()
                .filter(|key| key.thread_id == thread_id && key.turn_id == turn_id)
                .cloned()
                .collect::<Vec<_>>();
            let mut events = Vec::with_capacity(keys.len() + heartbeat_keys.len());
            for key in keys {
                if let Some(pending) = remove_progress(&mut state, &key) {
                    events.push(progress_event(pending));
                }
            }
            for key in heartbeat_keys {
                if let Some(heartbeat) = state.heartbeats.remove(&key) {
                    events.push(heartbeat_event(heartbeat));
                }
            }
            update_flush_scheduled(&mut state);
            events
        };
        self.send_live_events(events);
    }

    pub async fn flush_for_durable(&self, event: &AgentDurableEvent) {
        match event {
            AgentDurableEvent::ItemCompleted { notification }
            | AgentDurableEvent::TurnFinalizationPrepared { notification, .. } => {
                self.flush_item(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    notification.item.item_id(),
                )
                .await;
            }
            AgentDurableEvent::TurnCompleted {
                thread_id, turn_id, ..
            }
            | AgentDurableEvent::TurnFailed {
                thread_id, turn_id, ..
            }
            | AgentDurableEvent::TurnBlocked {
                thread_id, turn_id, ..
            }
            | AgentDurableEvent::TurnInterrupted {
                thread_id, turn_id, ..
            } => {
                self.flush_turn(thread_id, turn_id).await;
            }
            AgentDurableEvent::TaskEvent { event } if event.is_terminal() => {
                if let (Some(thread_id), Some(turn_id)) =
                    (event.thread_id.as_deref(), event.turn_id.as_deref())
                {
                    self.flush_turn(thread_id, turn_id).await;
                } else {
                    self.flush_all().await;
                }
            }
            _ => {}
        }
    }

    pub async fn flush_all(&self) {
        loop {
            let (events, has_more) = self.drain_batch();
            if events.is_empty() && !has_more {
                break;
            }
            self.send_live_events(events);
            if !has_more {
                break;
            }
        }
    }

    fn offer_notification(&self, notification: ItemDeltaNotification) {
        let key = progress_key_from_notification(&notification);
        let behavior = ProgressMergeBehavior::for_stream(key.stream);
        let mut should_schedule = false;
        {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("progress coalescer poisoned");
            if state.closed {
                return;
            }
            let is_new = !state.pending.contains_key(&key);
            if is_new && self.total_pending_keys(&state) >= self.inner.config.max_pending_keys {
                self.inner.observer.outcome(QueueOutcome::Rejected, 1);
                debug!(
                    workspace_id = notification.workspace_id,
                    thread_id = notification.thread_id,
                    turn_id = notification.turn_id,
                    item_id = notification.item_id,
                    ?behavior,
                    "dropping redundant progress at coalescer key limit"
                );
                return;
            }
            if let Some(pending) = state.pending.get_mut(&key) {
                self.inner.observer.outcome(QueueOutcome::Coalesced, 1);
                let before = pending.notification.delta.len() as u64;
                pending.merge(notification, &self.inner.config);
                let after = pending.notification.delta.len() as u64;
                state.pending_delta_bytes = state
                    .pending_delta_bytes
                    .saturating_sub(before)
                    .saturating_add(after);
            } else {
                let mut pending = PendingProgress::new(notification, behavior);
                let limit = match behavior {
                    ProgressMergeBehavior::Append => self.inner.config.max_append_bytes_per_key,
                    ProgressMergeBehavior::Snapshot => self.inner.config.max_snapshot_bytes_per_key,
                };
                let truncated = bound_delta(&mut pending.notification.delta, limit);
                annotate_progress_payload(&mut pending.notification, true, truncated);
                state.pending_delta_bytes = state
                    .pending_delta_bytes
                    .saturating_add(pending.notification.delta.len() as u64);
                self.inner
                    .observer
                    .enqueued(QueueBytes::Estimate(pending.notification.delta.len() as u64));
                state.pending.insert(key, pending);
            }
            if !state.flush_scheduled {
                state.flush_scheduled = true;
                should_schedule = true;
            }
        }
        {
            let state = self
                .inner
                .state
                .lock()
                .expect("progress coalescer poisoned");
            self.inner.observer.current(
                state.pending.len().saturating_add(state.heartbeats.len()),
                QueueBytes::Estimate(state.pending_delta_bytes),
            );
        }
        if should_schedule {
            self.schedule_flush();
        }
    }

    fn total_pending_keys(&self, state: &ProgressCoalescerState) -> usize {
        state.pending.len().saturating_add(state.heartbeats.len())
    }

    fn schedule_flush(&self) {
        let coalescer = self.clone();
        if tokio::runtime::Handle::try_current().is_err() {
            if let Ok(mut state) = self.inner.state.lock() {
                state.flush_scheduled = false;
            }
            debug!("progress coalescer could not schedule flush outside a tokio runtime");
            return;
        }
        let timer = WorkCarrier::independent(
            Boundary::Timer,
            WorkKind::WorkerLifecycle,
            pioneer_observability::work::Trigger::Timer,
            ExecutorRole::Event,
        );
        if let Some(workers) = &self.inner.owned_flush {
            let mut workers = workers.lock().expect("progress ownership poisoned");
            if workers.closed {
                return;
            }
            if workers.task.is_none() {
                // One actual flush worker for this owned coalescer. Manual
                // flushes and new offers only wake it; no recursive task queue.
                let wake = workers.wake.clone();
                workers.task = Some(Arc::new(tokio::sync::Mutex::new(ProgressFlushCompletion {
                    task: Some(tokio::spawn(timer.scope(async move {
                        loop {
                            wake.notified().await;
                            if coalescer
                                .inner
                                .state
                                .lock()
                                .expect("progress coalescer poisoned")
                                .closed
                            {
                                break;
                            }
                            loop {
                                pioneer_observability::live::sleeping(
                                    coalescer.inner.config.flush_interval,
                                    tokio::time::sleep(coalescer.inner.config.flush_interval),
                                )
                                .await;
                                let (events, has_more) = coalescer.drain_batch();
                                coalescer.send_live_events(events);
                                if coalescer
                                    .inner
                                    .state
                                    .lock()
                                    .expect("progress coalescer poisoned")
                                    .closed
                                {
                                    return;
                                }
                                if !has_more {
                                    break;
                                }
                            }
                        }
                    }))),
                    failure: None,
                })));
            }
            workers.wake.notify_one();
        } else {
            // Preserve the legacy flush scheduling path outside native CLI hubs.
            tokio::spawn(timer.scope(async move {
                pioneer_observability::live::sleeping(
                    coalescer.inner.config.flush_interval,
                    tokio::time::sleep(coalescer.inner.config.flush_interval),
                )
                .await;
                let (events, has_more) = coalescer.drain_batch();
                coalescer.send_live_events(events);
                if has_more {
                    coalescer.schedule_flush();
                }
            }));
        }
    }

    fn drain_batch(&self) -> (Vec<WorkItem<AgentProgressEvent>>, bool) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("progress coalescer poisoned");
        let max = self.inner.config.max_flush_batch_size.max(1);
        let mut events = Vec::new();
        let progress_keys = state.pending.keys().take(max).cloned().collect::<Vec<_>>();
        for key in progress_keys {
            if let Some(pending) = remove_progress(&mut state, &key) {
                events.push(progress_event(pending));
            }
        }
        if events.len() < max {
            let heartbeat_keys = state
                .heartbeats
                .keys()
                .take(max - events.len())
                .cloned()
                .collect::<Vec<_>>();
            for key in heartbeat_keys {
                if let Some(heartbeat) = state.heartbeats.remove(&key) {
                    events.push(heartbeat_event(heartbeat));
                }
            }
        }
        let has_more = !(state.pending.is_empty() && state.heartbeats.is_empty());
        state.flush_scheduled = has_more;
        (events, has_more)
    }

    fn send_live_events(&self, events: Vec<WorkItem<AgentProgressEvent>>) {
        for event in events {
            let delivery_observer = self.inner.delivery_observer.in_current_epoch();
            self.inner.observer.dequeued(QueueBytes::Unsupported);
            self.inner.observer.outcome(QueueOutcome::Processed, 1);
            if self.inner.live_tx.send(event).is_err() {
                self.inner
                    .observer
                    .outcome(QueueOutcome::DeliveryDropped, 1);
                delivery_observer.outcome(QueueOutcome::Rejected, 1);
            } else {
                delivery_observer.enqueued(QueueBytes::Unsupported);
            }
            if pioneer_observability::telemetry_enabled() {
                #[cfg(test)]
                BEFORE_BROADCAST_DEPTH.with(|hook| {
                    let callback = hook.borrow_mut().take();
                    if let Some(callback) = callback {
                        callback();
                    }
                });
                self.inner.observe_broadcast_depth(&delivery_observer);
            }
        }
        let state = self
            .inner
            .state
            .lock()
            .expect("progress coalescer poisoned");
        self.inner.observer.current(
            state.pending.len().saturating_add(state.heartbeats.len()),
            QueueBytes::Estimate(state.pending_delta_bytes),
        );
    }
}

fn update_flush_scheduled(state: &mut ProgressCoalescerState) {
    if state.pending.is_empty() && state.heartbeats.is_empty() {
        state.flush_scheduled = false;
    }
}

fn heartbeat_event(heartbeat: PendingHeartbeat) -> WorkItem<AgentProgressEvent> {
    let work = heartbeat.work;
    let at = heartbeat.enqueued_monotonic_ms;
    WorkItem::from_carrier(
        AgentProgressEvent::ItemHeartbeat {
            workspace_id: heartbeat.workspace_id,
            thread_id: heartbeat.thread_id,
            turn_id: heartbeat.turn_id,
            item_id: heartbeat.item_id,
            item_type: heartbeat.item_type,
            source: heartbeat.source,
        },
        work,
        at,
    )
}

fn progress_event_to_item_delta(event: AgentProgressEvent) -> Option<ItemDeltaNotification> {
    match event {
        AgentProgressEvent::ItemDelta { notification } => Some(notification),
        AgentProgressEvent::ItemHeartbeat { .. } => None,
        AgentProgressEvent::ToolOutputDelta {
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            stream,
            delta,
            payload,
        } => Some(ItemDeltaNotification {
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            delta,
            stream: Some(stream),
            payload,
            markdown: None,
            markdown_version: None,
        }),
        AgentProgressEvent::TaskProgress {
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            task_id,
            run_id,
            summary,
        } => Some(ItemDeltaNotification {
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            delta: summary,
            stream: Some(ItemDeltaStream::ToolProgress),
            payload: Some(serde_json::json!({
                "kind": "task_progress",
                "task_id": task_id,
                "run_id": run_id,
            })),
            markdown: None,
            markdown_version: None,
        }),
    }
}

fn progress_key_from_notification(notification: &ItemDeltaNotification) -> ProgressCoalescingKey {
    ProgressCoalescingKey {
        workspace_id: notification.workspace_id.clone(),
        thread_id: notification.thread_id.clone(),
        turn_id: notification.turn_id.clone(),
        item_id: notification.item_id.clone(),
        stream: notification.stream.unwrap_or(ItemDeltaStream::Generic),
    }
}

fn bound_delta(delta: &mut String, max_bytes: usize) -> bool {
    if delta.len() <= max_bytes {
        return false;
    }
    if max_bytes == 0 {
        delta.clear();
        return true;
    }
    const SUFFIX: &str = "\n[progress truncated]";
    let target = max_bytes.saturating_sub(SUFFIX.len());
    let mut boundary = target.min(delta.len());
    while boundary > 0 && !delta.is_char_boundary(boundary) {
        boundary -= 1;
    }
    if boundary == 0 {
        boundary = max_bytes.min(delta.len());
        while boundary > 0 && !delta.is_char_boundary(boundary) {
            boundary -= 1;
        }
        delta.truncate(boundary);
    } else {
        delta.truncate(boundary);
        delta.push_str(SUFFIX);
    }
    true
}

fn annotate_progress_payload(
    notification: &mut ItemDeltaNotification,
    coalesced: bool,
    truncated: bool,
) {
    let mut payload = match notification.payload.take() {
        Some(JsonValue::Object(map)) => JsonValue::Object(map),
        Some(value) => serde_json::json!({ "value": value }),
        None => serde_json::json!({}),
    };
    if let JsonValue::Object(map) = &mut payload {
        map.insert("coalesced".to_owned(), JsonValue::Bool(coalesced));
        if truncated {
            map.insert("truncated".to_owned(), JsonValue::Bool(true));
        }
    }
    notification.payload = Some(payload);
}

#[cfg(test)]
mod fanout_tests {
    use super::*;
    #[test]
    fn observed_broadcast_preserves_lagged_delivery_and_sender_close() {
        let coalescer = ProgressCoalescer::new(2, ProgressCoalescerConfig::default());
        let mut receiver = coalescer.subscribe_live();
        for index in 0..3 {
            let event = AgentProgressEvent::TaskProgress {
                workspace_id: "w".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item_id: "task".into(),
                task_id: "task".into(),
                run_id: None,
                summary: index.to_string(),
            };
            coalescer.send_live_events(vec![WorkItem::from_carrier(
                event,
                WorkCarrier::default(),
                None,
            )]);
        }
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(1))
        ));
        for expected in ["1", "2"] {
            match receiver.try_recv().unwrap() {
                AgentProgressEvent::TaskProgress { summary, .. } => assert_eq!(summary, expected),
                _ => panic!("unexpected progress event"),
            }
        }
        drop(coalescer);
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
    }
    #[tokio::test(start_paused = true)]
    async fn terminal_flush_targets_only_existing_bounded_buffers_and_respects_workspace() {
        let coalescer = ProgressCoalescer::new(16, ProgressCoalescerConfig::default());
        let mut rx = coalescer.subscribe_live();
        for (workspace, turn, item) in [
            ("w", "creation", "task_t"),
            ("w", "occurrence", "task_t"),
            ("other", "foreign", "task_t"),
            ("w", "other_item", "task_other"),
        ] {
            coalescer.offer(AgentProgressEvent::TaskProgress {
                workspace_id: workspace.into(),
                thread_id: "thread".into(),
                turn_id: turn.into(),
                item_id: item.into(),
                task_id: "t".into(),
                run_id: None,
                summary: "pending".into(),
            });
        }
        let mut targets = coalescer.pending_targets_for_item("w", "task_t");
        targets.sort();
        assert_eq!(
            targets,
            vec![
                ("thread".into(), "creation".into()),
                ("thread".into(), "occurrence".into())
            ]
        );
        for (thread, turn) in targets {
            coalescer.flush_item("w", &thread, &turn, "task_t").await;
        }
        assert!(coalescer.pending_targets_for_item("w", "task_t").is_empty());
        assert_eq!(
            coalescer.pending_targets_for_item("other", "task_t").len(),
            1
        );
        assert_eq!(
            coalescer.pending_targets_for_item("w", "task_other").len(),
            1
        );
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
    }
}

fn progress_event(pending: PendingProgress) -> WorkItem<AgentProgressEvent> {
    WorkItem::from_carrier(
        AgentProgressEvent::ItemDelta {
            notification: pending.notification,
        },
        pending.work,
        pending.enqueued_monotonic_ms,
    )
}
#[derive(Debug)]
pub struct ProgressEventReceiver {
    receiver: Option<broadcast::Receiver<WorkItem<AgentProgressEvent>>>,
    // A receiver must not keep the sender alive or change channel-close behavior.
    owner: Weak<ProgressCoalescerInner>,
    pending_work: WorkCarrier,
}
impl ProgressCoalescerInner {
    fn observe_broadcast_depth(&self, observer: &QueueObserver) {
        let _observation = self
            .delivery_observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Both send and delivery paths read AFTER taking this lock. A late
        // caller rereads native unread depth, never publishes a saved len.
        observer.current(self.live_tx.len(), QueueBytes::Unsupported);
    }
}
impl ProgressEventReceiver {
    fn observe_delivery(&self, received: bool, lagged: u64) {
        if !pioneer_observability::telemetry_enabled() {
            return;
        }
        if let Some(owner) = self.owner.upgrade() {
            let observer = owner.delivery_observer.in_current_epoch();
            if received {
                observer.outcome(QueueOutcome::Processed, 1);
            }
            if lagged > 0 {
                observer.outcome(QueueOutcome::DeliveryDropped, lagged);
            }
            owner.observe_broadcast_depth(&observer);
        }
    }
    pub async fn recv(&mut self) -> Result<AgentProgressEvent, broadcast::error::RecvError> {
        self.pending_work = Default::default();
        let result = self.receiver.as_mut().expect("live receiver").recv().await;
        self.observe_delivery(
            result.is_ok(),
            match &result {
                Err(broadcast::error::RecvError::Lagged(count)) => *count,
                _ => 0,
            },
        );
        let item = result?;
        let (value, work) =
            item.received(Boundary::EventLane, WorkKind::Event, ExecutorRole::Event);
        self.pending_work = work.execution_child(WorkKind::Event, ExecutorRole::Event);
        Ok(value)
    }
    pub fn try_recv(&mut self) -> Result<AgentProgressEvent, broadcast::error::TryRecvError> {
        self.pending_work = Default::default();
        let result = self.receiver.as_mut().expect("live receiver").try_recv();
        self.observe_delivery(
            result.is_ok(),
            match &result {
                Err(broadcast::error::TryRecvError::Lagged(count)) => *count,
                _ => 0,
            },
        );
        let item = result?;
        let (value, work) =
            item.received(Boundary::EventLane, WorkKind::Event, ExecutorRole::Event);
        self.pending_work = work.execution_child(WorkKind::Event, ExecutorRole::Event);
        Ok(value)
    }
    pub fn pending_work(&self) -> WorkCarrier {
        self.pending_work.clone()
    }
}

impl Drop for ProgressEventReceiver {
    fn drop(&mut self) {
        // Removing a subscriber releases its native unread slots. Drop it before
        // the fresh depth read; never create a replacement subscriber/sender.
        drop(self.receiver.take());
        if pioneer_observability::telemetry_enabled() {
            if let Some(owner) = self.owner.upgrade() {
                owner.observe_broadcast_depth(&owner.delivery_observer.in_current_epoch());
            }
        }
    }
}

fn remove_progress(
    state: &mut ProgressCoalescerState,
    key: &ProgressCoalescingKey,
) -> Option<PendingProgress> {
    let pending = state.pending.remove(key)?;
    state.pending_delta_bytes = state
        .pending_delta_bytes
        .saturating_sub(pending.notification.delta.len() as u64);
    Some(pending)
}

#[cfg(test)]
thread_local! { pub(crate) static BEFORE_BROADCAST_DEPTH: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = std::cell::RefCell::new(None); }

#[cfg(test)]
mod depth_tests {
    use super::*;
    use std::{cell::RefCell, rc::Rc};
    fn event() -> WorkItem<AgentProgressEvent> {
        WorkItem::from_carrier(
            AgentProgressEvent::TaskProgress {
                workspace_id: "w".into(),
                thread_id: "t".into(),
                turn_id: "turn".into(),
                item_id: "i".into(),
                task_id: "task".into(),
                run_id: None,
                summary: "fixture".into(),
            },
            WorkCarrier::default(),
            None,
        )
    }
    #[test]
    fn late_sender_rereads_unread_depth_after_real_delivery_and_epoch_change() {
        let _lock = crate::OBSERVATION_TEST_LOCK.lock().unwrap();
        let old = pioneer_observability::telemetry_enabled();
        pioneer_observability::set_telemetry_enabled(false);
        pioneer_observability::set_telemetry_enabled(true);
        let mut coalescer = ProgressCoalescer::new(2, ProgressCoalescerConfig::default());
        Arc::get_mut(&mut coalescer.inner)
            .unwrap()
            .delivery_observer = QueueObserver::new(
            QueueKind::EventProgressBroadcast,
            Some(2),
            "fixture broadcast current order",
        );
        let receiver = Rc::new(RefCell::new(coalescer.subscribe_live()));
        let during = receiver.clone();
        BEFORE_BROADCAST_DEPTH.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                during.borrow_mut().try_recv().unwrap();
            }))
        });
        coalescer.send_live_events(vec![event()]);
        let read = || {
            pioneer_observability::live::snapshot()
                .unwrap()
                .queues
                .into_iter()
                .find(|q| q.byte_definition == "fixture broadcast current order")
                .unwrap()
        };
        assert_eq!(coalescer.inner.live_tx.len(), 0);
        let q = read();
        assert_eq!((q.depth, q.enqueued, q.processed), (0, 1, 1));
        assert!(q.initial_depth_known && q.last_observed_ms <= q.snapshot_ms);
        let mut second = coalescer.subscribe_live();
        for _ in 0..3 {
            coalescer.send_live_events(vec![event()]);
        }
        assert!(matches!(
            receiver.borrow_mut().try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(1))
        ));
        assert!(matches!(
            second.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(1))
        ));
        for _ in 0..2 {
            receiver.borrow_mut().try_recv().unwrap();
        }
        assert_eq!(read().depth, 2); // other subscriber still retains both unread slots
        for _ in 0..2 {
            second.try_recv().unwrap();
        }
        assert_eq!((read().depth, read().delivery_dropped), (0, 2));
        BEFORE_BROADCAST_DEPTH.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(|| {
                pioneer_observability::set_telemetry_enabled(false);
                pioneer_observability::set_telemetry_enabled(true);
            }))
        });
        coalescer.send_live_events(vec![event()]); // old pinned sender cannot publish E2
        assert!(
            pioneer_observability::live::snapshot()
                .unwrap()
                .queues
                .iter()
                .all(|q| q.byte_definition != "fixture broadcast current order")
        );
        receiver.borrow_mut().try_recv().unwrap();
        assert_eq!(read().depth, 1); // exact native unread baseline in new epoch
        second.try_recv().unwrap();
        assert_eq!(read().depth, 0);
        coalescer.send_live_events(vec![event()]);
        drop(receiver); // second subscriber still retains this slot
        assert_eq!(read().depth, 1);
        drop(second); // last native unread reference is released without a recv callback
        assert_eq!(coalescer.inner.live_tx.len(), 0);
        assert_eq!(read().depth, 0);
        let mut closed = coalescer.subscribe_live();
        drop(coalescer);
        assert!(matches!(
            closed.try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
        pioneer_observability::set_telemetry_enabled(old);
    }
}
