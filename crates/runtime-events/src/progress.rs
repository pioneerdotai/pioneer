use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, ItemDeltaNotification, ItemDeltaStream,
    ItemHeartbeatSource, ProgressCoalescingKey, TurnItemType,
};
use serde_json::Value as JsonValue;
use tokio::sync::broadcast;
use tracing::debug;

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
    notification: ItemDeltaNotification,
    behavior: ProgressMergeBehavior,
}

impl PendingProgress {
    fn new(notification: ItemDeltaNotification, behavior: ProgressMergeBehavior) -> Self {
        Self {
            notification,
            behavior,
        }
    }

    fn merge(&mut self, mut next: ItemDeltaNotification, config: &ProgressCoalescerConfig) {
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
    heartbeats: HashMap<HeartbeatKey, PendingHeartbeat>,
    flush_scheduled: bool,
}

#[derive(Debug)]
struct ProgressCoalescerInner {
    state: StdMutex<ProgressCoalescerState>,
    live_tx: broadcast::Sender<AgentProgressEvent>,
    config: ProgressCoalescerConfig,
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
                live_tx,
                config,
            }),
        }
    }

    pub fn subscribe_live(&self) -> broadcast::Receiver<AgentProgressEvent> {
        self.inner.live_tx.subscribe()
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
            let key = HeartbeatKey {
                thread_id: thread_id.clone(),
                turn_id: turn_id.clone(),
                item_id: item_id.clone(),
            };
            let is_new = !state.heartbeats.contains_key(&key);
            if is_new && self.total_pending_keys(&state) >= self.inner.config.max_pending_keys {
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
            state.heartbeats.insert(
                key,
                PendingHeartbeat {
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
            state
                .pending
                .remove(key)
                .map(|pending| {
                    vec![AgentProgressEvent::ItemDelta {
                        notification: pending.notification,
                    }]
                })
                .unwrap_or_default()
        };
        self.send_live_events(events);
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
                if let Some(pending) = state.pending.remove(&key) {
                    events.push(AgentProgressEvent::ItemDelta {
                        notification: pending.notification,
                    });
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
                if let Some(pending) = state.pending.remove(&key) {
                    events.push(AgentProgressEvent::ItemDelta {
                        notification: pending.notification,
                    });
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
            let is_new = !state.pending.contains_key(&key);
            if is_new && self.total_pending_keys(&state) >= self.inner.config.max_pending_keys {
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
                pending.merge(notification, &self.inner.config);
            } else {
                let mut pending = PendingProgress::new(notification, behavior);
                let limit = match behavior {
                    ProgressMergeBehavior::Append => self.inner.config.max_append_bytes_per_key,
                    ProgressMergeBehavior::Snapshot => self.inner.config.max_snapshot_bytes_per_key,
                };
                let truncated = bound_delta(&mut pending.notification.delta, limit);
                annotate_progress_payload(&mut pending.notification, true, truncated);
                state.pending.insert(key, pending);
            }
            if !state.flush_scheduled {
                state.flush_scheduled = true;
                should_schedule = true;
            }
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
        tokio::spawn(async move {
            tokio::time::sleep(coalescer.inner.config.flush_interval).await;
            let (events, has_more) = coalescer.drain_batch();
            coalescer.send_live_events(events);
            if has_more {
                coalescer.schedule_flush();
            }
        });
    }

    fn drain_batch(&self) -> (Vec<AgentProgressEvent>, bool) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("progress coalescer poisoned");
        let max = self.inner.config.max_flush_batch_size.max(1);
        let mut events = Vec::new();
        let progress_keys = state.pending.keys().take(max).cloned().collect::<Vec<_>>();
        for key in progress_keys {
            if let Some(pending) = state.pending.remove(&key) {
                events.push(AgentProgressEvent::ItemDelta {
                    notification: pending.notification,
                });
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

    fn send_live_events(&self, events: Vec<AgentProgressEvent>) {
        for event in events {
            let _ = self.inner.live_tx.send(event);
        }
    }
}

fn update_flush_scheduled(state: &mut ProgressCoalescerState) {
    if state.pending.is_empty() && state.heartbeats.is_empty() {
        state.flush_scheduled = false;
    }
}

fn heartbeat_event(heartbeat: PendingHeartbeat) -> AgentProgressEvent {
    AgentProgressEvent::ItemHeartbeat {
        workspace_id: heartbeat.workspace_id,
        thread_id: heartbeat.thread_id,
        turn_id: heartbeat.turn_id,
        item_id: heartbeat.item_id,
        item_type: heartbeat.item_type,
        source: heartbeat.source,
    }
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
