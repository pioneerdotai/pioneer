use crate::cli_runtime::session_instance::CliSessionInstanceId;
use crate::cli_runtime::turn_binding::{
    CLI_RUNTIME_TURN_STATUS_RUNNING, CLI_RUNTIME_TURN_STATUS_STARTING,
};
use pioneer_cli_agent_runtime::event::RuntimeEvent;
use pioneer_crud::{
    CliRuntimeNativeTurnOwner, CliRuntimeTurnBindingRecord, RunningTurnItemAttempt,
};
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct CliRuntimeCommandHeartbeatKey {
    pub(crate) workspace_id: String,
    pub(crate) runtime_id: String,
    pub(crate) thread_id: String,
    pub(crate) turn_id: String,
    pub(crate) item_id: String,
}

#[derive(Clone, Debug)]
struct CliRuntimeCommandHeartbeatState {
    incarnation: Arc<()>,
    native_thread_id: Option<String>,
    native_turn_id: String,
    observer: Option<CliSessionInstanceId>,
    last_heartbeat_at_unix: i64,
    last_attempt_at_unix: i64,
    retry_at: Option<tokio::time::Instant>,
    deferred_gate: Option<Arc<Mutex<()>>>,
}

impl CliRuntimeCommandHeartbeatState {
    fn accepts_observer(&self, observer: &CliSessionInstanceId) -> bool {
        self.observer.as_ref().is_none_or(|current| {
            current.boot_id() == observer.boot_id() && current.generation() <= observer.generation()
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CliRuntimeCommandHeartbeatDueItem {
    pub(crate) key: CliRuntimeCommandHeartbeatKey,
    incarnation: Arc<()>,
    native_thread_id: Option<String>,
    native_turn_id: String,
}

impl CliRuntimeCommandHeartbeatDueItem {
    pub(crate) fn matches_active_native_turn_owner(
        &self,
        owner: &CliRuntimeNativeTurnOwner,
    ) -> bool {
        let binding = &owner.binding;
        binding.turn_id == self.key.turn_id
            && owner.attempt.turn_id == self.key.turn_id
            && binding.workspace_id == self.key.workspace_id
            && binding.runtime_id == self.key.runtime_id
            && binding.thread_id == self.key.thread_id
            && self
                .native_thread_id
                .as_deref()
                .is_none_or(|native_thread_id| binding.native_thread_id == native_thread_id)
            && owner.attempt.status.is_active()
            && owner.segment.as_ref().is_none_or(|segment| {
                segment.status == pioneer_crud::CliRuntimeExecutionSegmentStatus::Running
            })
            && matches!(
                binding.status.as_str(),
                CLI_RUNTIME_TURN_STATUS_STARTING | CLI_RUNTIME_TURN_STATUS_RUNNING
            )
    }

    pub(crate) fn native_turn_id(&self) -> &str {
        self.native_turn_id.as_str()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CliRuntimeCommandHeartbeatTracker {
    active: Arc<Mutex<BTreeMap<CliRuntimeCommandHeartbeatKey, CliRuntimeCommandHeartbeatState>>>,
    due_after: Arc<std::sync::Mutex<Option<CliRuntimeCommandHeartbeatKey>>>,
    #[cfg(test)]
    last_rehydrated_at_unix: Arc<Mutex<Option<i64>>>,
    interval_secs: i64,
}

impl CliRuntimeCommandHeartbeatTracker {
    pub(crate) fn new(interval_secs: u64) -> Self {
        Self {
            active: Arc::new(Mutex::new(BTreeMap::new())),
            due_after: Default::default(),
            #[cfg(test)]
            last_rehydrated_at_unix: Arc::new(Mutex::new(None)),
            interval_secs: i64::try_from(interval_secs.max(1)).unwrap_or(i64::MAX),
        }
    }

    #[cfg(test)]
    pub(crate) const fn interval_secs(&self) -> i64 {
        self.interval_secs
    }

    pub(crate) async fn update_from_runtime_event(
        &self,
        instance: &CliSessionInstanceId,
        turn_binding: &CliRuntimeTurnBindingRecord,
        event: &RuntimeEvent,
        now_unix: i64,
    ) {
        match event {
            RuntimeEvent::ItemStarted(started)
                if is_command_execution_kind(started.item_kind.as_str()) =>
            {
                self.register(
                    instance,
                    turn_binding,
                    started.native_item_id.as_str(),
                    started.native_thread_id.clone(),
                    started.native_turn_id.clone(),
                    now_unix,
                )
                .await;
            }
            RuntimeEvent::ItemCompleted(completed)
                if is_command_execution_kind(completed.item_kind.as_str()) =>
            {
                self.remove_item(
                    instance,
                    turn_binding.turn_id.as_str(),
                    completed.native_item_id.as_str(),
                    completed.native_turn_id.as_str(),
                )
                .await;
            }
            _ if event.turn_terminal_kind().is_some() => {
                self.remove_turn(
                    instance,
                    turn_binding.turn_id.as_str(),
                    super::turn_binding::cli_runtime_native_turn_id_for_event(event),
                )
                .await;
            }
            _ => {}
        }
    }

    pub(crate) async fn register(
        &self,
        instance: &CliSessionInstanceId,
        turn_binding: &CliRuntimeTurnBindingRecord,
        item_id: &str,
        native_thread_id: Option<String>,
        native_turn_id: String,
        now_unix: i64,
    ) {
        let key = CliRuntimeCommandHeartbeatKey {
            workspace_id: turn_binding.workspace_id.clone(),
            runtime_id: turn_binding.runtime_id.clone(),
            thread_id: turn_binding.thread_id.clone(),
            turn_id: turn_binding.turn_id.clone(),
            item_id: item_id.to_owned(),
        };
        let state = CliRuntimeCommandHeartbeatState {
            incarnation: Arc::new(()),
            native_thread_id,
            native_turn_id,
            observer: Some(instance.clone()),
            last_heartbeat_at_unix: now_unix,
            last_attempt_at_unix: now_unix,
            retry_at: None,
            deferred_gate: None,
        };
        let mut active = self.active.lock().await;
        if active
            .get(&key)
            .is_none_or(|current| current.accepts_observer(instance))
        {
            active.insert(key, state);
        }
    }

    pub(crate) async fn restore_from_durable(
        &self,
        observer: Option<&CliSessionInstanceId>,
        turn_binding: &CliRuntimeTurnBindingRecord,
        attempt: &RunningTurnItemAttempt,
        native_turn_id: String,
    ) -> CliRuntimeCommandHeartbeatKey {
        let key = CliRuntimeCommandHeartbeatKey {
            workspace_id: turn_binding.workspace_id.clone(),
            runtime_id: turn_binding.runtime_id.clone(),
            thread_id: turn_binding.thread_id.clone(),
            turn_id: attempt.turn_id.clone(),
            item_id: attempt.item_id.clone(),
        };
        let durable_heartbeat = attempt
            .last_heartbeat_at_unix
            .unwrap_or(attempt.started_at_unix);
        let restored_key = key.clone();
        match self.active.lock().await.entry(key) {
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                if state.native_thread_id.as_deref() != Some(turn_binding.native_thread_id.as_str())
                    || state.native_turn_id != native_turn_id
                    || state.observer.as_ref() != observer
                    || durable_heartbeat > state.last_heartbeat_at_unix
                {
                    state.incarnation = Arc::new(());
                    state.deferred_gate = None;
                    state.retry_at = None;
                }
                state.native_thread_id = Some(turn_binding.native_thread_id.clone());
                state.native_turn_id = native_turn_id;
                state.observer = observer.cloned();
                state.last_heartbeat_at_unix = state.last_heartbeat_at_unix.max(durable_heartbeat);
            }
            Entry::Vacant(entry) => {
                entry.insert(CliRuntimeCommandHeartbeatState {
                    incarnation: Arc::new(()),
                    native_thread_id: Some(turn_binding.native_thread_id.clone()),
                    native_turn_id,
                    observer: observer.cloned(),
                    last_heartbeat_at_unix: durable_heartbeat,
                    last_attempt_at_unix: durable_heartbeat,
                    retry_at: None,
                    deferred_gate: None,
                });
            }
        }
        restored_key
    }

    pub(crate) async fn remove_item(
        &self,
        instance: &CliSessionInstanceId,
        turn_id: &str,
        item_id: &str,
        native_turn_id: &str,
    ) {
        let key = CliRuntimeCommandHeartbeatKey {
            workspace_id: instance.key().workspace_id.clone(),
            runtime_id: instance.key().runtime_id.clone(),
            thread_id: instance.key().thread_id.clone(),
            turn_id: turn_id.to_owned(),
            item_id: item_id.to_owned(),
        };
        let mut active = self.active.lock().await;
        if active.get(&key).is_some_and(|state| {
            state.native_turn_id == native_turn_id && state.accepts_observer(instance)
        }) {
            active.remove(&key);
        }
    }
    pub(crate) async fn remove_turn(
        &self,
        instance: &CliSessionInstanceId,
        turn_id: &str,
        native_turn_id: Option<&str>,
    ) {
        self.active.lock().await.retain(|key, state| {
            !(key.turn_id == turn_id
                && key.workspace_id == instance.key().workspace_id
                && key.runtime_id == instance.key().runtime_id
                && key.thread_id == instance.key().thread_id
                && state.accepts_observer(instance)
                && native_turn_id.is_none_or(|id| state.native_turn_id == id))
        });
    }

    pub(crate) async fn detach_observer(&self, instance: &CliSessionInstanceId) {
        for state in self.active.lock().await.values_mut() {
            if state.observer.as_ref() == Some(instance) {
                state.observer = None;
                state.incarnation = Arc::new(());
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn should_rehydrate(&self, now_unix: i64, interval_secs: i64) -> bool {
        let mut last_rehydrated = self.last_rehydrated_at_unix.lock().await;
        let due = last_rehydrated.is_none_or(|last| {
            now_unix < last || now_unix.saturating_sub(last) >= interval_secs.max(1)
        });
        if due {
            *last_rehydrated = Some(now_unix);
        }
        due
    }

    pub(crate) async fn remove_key(&self, expected: &CliRuntimeCommandHeartbeatDueItem) {
        let mut active = self.active.lock().await;
        if active
            .get(&expected.key)
            .is_some_and(|state| Arc::ptr_eq(&state.incarnation, &expected.incarnation))
        {
            active.remove(&expected.key);
        }
    }

    pub(crate) async fn mark_attempt_failed(
        &self,
        expected: &CliRuntimeCommandHeartbeatDueItem,
        now_unix: i64,
    ) {
        if let Some(item) = self.active.lock().await.get_mut(&expected.key) {
            if !Arc::ptr_eq(&item.incarnation, &expected.incarnation) {
                return;
            }
            item.retry_at = None;
            item.deferred_gate = None;
            item.last_attempt_at_unix = now_unix;
        }
    }

    pub(crate) async fn mark_heartbeat_succeeded(
        &self,
        expected: &CliRuntimeCommandHeartbeatDueItem,
        now_unix: i64,
    ) {
        if let Some(item) = self.active.lock().await.get_mut(&expected.key) {
            if !Arc::ptr_eq(&item.incarnation, &expected.incarnation) {
                return;
            }
            item.retry_at = None;
            item.deferred_gate = None;
            item.last_attempt_at_unix = now_unix;
            item.last_heartbeat_at_unix = now_unix;
        }
    }

    pub(crate) async fn retain_discovered_commands(
        &self,
        keys: &std::collections::HashSet<CliRuntimeCommandHeartbeatKey>,
    ) {
        self.active.lock().await.retain(|key, _| keys.contains(key));
    }
    /// A deferred authoritative observation retains the previous control2s
    /// retry, rather than spinning an unchanged expired heartbeat deadline.
    pub(crate) async fn mark_deferred(
        &self,
        expected: &CliRuntimeCommandHeartbeatDueItem,
        _now_unix: i64,
        gate: Option<Arc<Mutex<()>>>,
    ) {
        if let Some(item) = self.active.lock().await.get_mut(&expected.key) {
            if !Arc::ptr_eq(&item.incarnation, &expected.incarnation) {
                return;
            }
            item.deferred_gate = gate;
            item.retry_at = Some(tokio::time::Instant::now() + std::time::Duration::from_secs(2));
        }
    }
    pub(crate) async fn next_wait(&self, now_unix: i64) -> pioneer_sqlite::background::Plan {
        use pioneer_sqlite::background::Plan;
        let active = self.active.lock().await;
        let mut wall: Option<i64> = None;
        let mut monotonic: Option<tokio::time::Instant> = None;
        for item in active.values() {
            if item.deferred_gate.is_some() {
                continue;
            }
            let heartbeat = item
                .last_heartbeat_at_unix
                .saturating_add(self.interval_secs);
            if let Some(at) = item.retry_at.filter(|_| heartbeat <= now_unix) {
                monotonic = Some(monotonic.map_or(at, |old| old.min(at)));
            } else {
                let at = if item.retry_at.is_some() {
                    heartbeat
                } else {
                    heartbeat.max(item.last_attempt_at_unix.saturating_add(self.interval_secs))
                };
                wall = Some(wall.map_or(at, |old| old.min(at)));
            }
        }
        let plan = wall.map_or(Plan::Dormant, |at| {
            Plan::WallDeadline(pioneer_crud::unix_to_datetime(at).into())
        });
        monotonic.map_or(plan, |at| plan.with_monotonic_deadline(at))
    }
    pub(crate) async fn dependency_released(&self) {
        let gates = self
            .active
            .lock()
            .await
            .values()
            .filter_map(|state| state.deferred_gate.clone())
            .collect::<Vec<_>>();
        let gate = pioneer_sqlite::background::wait_for_mutex_release(gates).await;
        let mut active = self.active.lock().await;
        for state in active.values_mut() {
            if state
                .deferred_gate
                .as_ref()
                .is_some_and(|old| Arc::ptr_eq(old, &gate))
            {
                state.deferred_gate = None;
                state.retry_at = None;
            }
        }
    }
    #[cfg(test)]
    async fn next_deadline(&self) -> Option<i64> {
        self.active
            .lock()
            .await
            .values()
            .map(|item| {
                item.last_heartbeat_at_unix
                    .max(item.last_attempt_at_unix)
                    .saturating_add(self.interval_secs)
            })
            .min()
    }
    #[cfg(test)]
    pub(crate) async fn due_items(&self, now_unix: i64) -> Vec<CliRuntimeCommandHeartbeatDueItem> {
        self.due_items_bounded(now_unix, usize::MAX).await
    }
    pub(crate) async fn due_items_bounded(
        &self,
        now_unix: i64,
        limit: usize,
    ) -> Vec<CliRuntimeCommandHeartbeatDueItem> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        if limit == 0 {
            return Vec::new();
        }
        let active = self.active.lock().await;
        let mut cursor = self.due_after.lock().unwrap();
        let tail = active.range((cursor.clone().map_or(Unbounded, Excluded), Unbounded));
        let head = active
            .range((Unbounded, cursor.clone().map_or(Unbounded, Included)))
            .take(if cursor.is_some() { usize::MAX } else { 0 });
        let mut due = Vec::new();
        for (key, item) in tail.chain(head) {
            let heartbeat_due =
                now_unix.saturating_sub(item.last_heartbeat_at_unix) >= self.interval_secs;
            let attempt_due = if let Some(gate) = item.deferred_gate.as_ref() {
                gate.try_lock().is_ok()
            } else {
                item.retry_at.map_or_else(
                    || now_unix.saturating_sub(item.last_attempt_at_unix) >= self.interval_secs,
                    |at| at <= tokio::time::Instant::now(),
                )
            };
            if heartbeat_due && attempt_due {
                due.push(CliRuntimeCommandHeartbeatDueItem {
                    key: key.clone(),
                    incarnation: item.incarnation.clone(),
                    native_thread_id: item.native_thread_id.clone(),
                    native_turn_id: item.native_turn_id.clone(),
                });
                if due.len() >= limit {
                    break;
                }
            }
        }
        *cursor = due.last().map(|item| item.key.clone());
        due
    }
}

pub(crate) fn is_command_execution_kind(kind: &str) -> bool {
    kind.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect::<String>()
        == "commandexecution"
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use pioneer_cli_agent_runtime::event::{RuntimeErrorEvent, RuntimeTurnRetrying};

    fn timestamp() -> sea_orm::entity::prelude::DateTimeWithTimeZone {
        Utc.timestamp_opt(1, 0)
            .single()
            .expect("valid timestamp")
            .fixed_offset()
    }

    fn session_instance() -> CliSessionInstanceId {
        CliSessionInstanceId::unmanaged_for_test(
            crate::cli_runtime::manager::CLIAgentRuntimeSessionKey::new(
                "workspace",
                "codex",
                "thread",
            )
            .expect("session key should build"),
            1,
        )
        .unwrap()
    }

    fn session_instance_with_generation(generation: u64) -> CliSessionInstanceId {
        CliSessionInstanceId::unmanaged_for_test(
            crate::cli_runtime::manager::CLIAgentRuntimeSessionKey::new(
                "workspace",
                "codex",
                "thread",
            )
            .expect("session key should build"),
            generation,
        )
        .unwrap()
    }

    fn turn_binding(status: &str) -> CliRuntimeTurnBindingRecord {
        CliRuntimeTurnBindingRecord {
            input_mapping_payload_id: None,
            input_mapping_read_hold: None,
            input_mapping_revision: 0,
            input_mapping_identity: String::new(),
            turn_id: "turn".to_owned(),
            workspace_id: "workspace".to_owned(),
            runtime_id: "codex".to_owned(),
            thread_id: "thread".to_owned(),
            continuation_thread_id: "thread".to_owned(),
            runtime_kind: "codex".to_owned(),
            native_thread_id: "native-thread".to_owned(),
            native_turn_id: Some("native-turn".to_owned()),
            native_goal_status: None,
            native_goal_turn_id: None,
            native_goal_observed_at: None,
            request_id: None,
            status: status.to_owned(),
            model: None,
            cwd: None,
            sandbox_json: None,
            approval_policy: None,
            input_mapping_json: "{}".to_owned(),
            mcp: None,
            created_at: timestamp(),
            updated_at: timestamp(),
        }
    }

    #[tokio::test]
    async fn due_items_do_not_advance_heartbeat_until_success() {
        let tracker = CliRuntimeCommandHeartbeatTracker::new(60);
        let instance = session_instance();
        let binding = turn_binding(CLI_RUNTIME_TURN_STATUS_RUNNING);
        tracker
            .register(
                &instance,
                &binding,
                "item",
                Some("native-thread".to_owned()),
                "native-turn".to_owned(),
                100,
            )
            .await;

        let due = tracker.due_items(160).await;
        assert_eq!(due.len(), 1);
        let due_again = tracker.due_items(160).await;
        assert_eq!(due_again.len(), 1);

        tracker.mark_heartbeat_succeeded(&due[0], 160).await;
        assert!(tracker.due_items(160).await.is_empty());
    }

    #[tokio::test]
    async fn failed_attempt_delays_retry_without_marking_heartbeat_success() {
        let tracker = CliRuntimeCommandHeartbeatTracker::new(60);
        let instance = session_instance();
        let binding = turn_binding(CLI_RUNTIME_TURN_STATUS_RUNNING);
        tracker
            .register(
                &instance,
                &binding,
                "item",
                Some("native-thread".to_owned()),
                "native-turn".to_owned(),
                100,
            )
            .await;

        assert_eq!(tracker.next_deadline().await, Some(160));
        let due = tracker.due_items(160).await;
        assert_eq!(due.len(), 1);
        tracker.mark_attempt_failed(&due[0], 160).await;

        assert_eq!(tracker.next_deadline().await, Some(220));
        assert!(tracker.due_items(190).await.is_empty());
        assert_eq!(tracker.due_items(220).await.len(), 1);
        tokio::time::pause();
        tracker.mark_deferred(&due[0], 220, None).await;
        assert!(tracker.due_items(221).await.is_empty());
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        assert_eq!(tracker.due_items_bounded(222, 1).await.len(), 1);
    }

    #[tokio::test]
    async fn retrying_turn_keeps_command_heartbeat_active() {
        let tracker = CliRuntimeCommandHeartbeatTracker::new(60);
        let instance = session_instance();
        let binding = turn_binding(CLI_RUNTIME_TURN_STATUS_RUNNING);
        tracker
            .register(
                &instance,
                &binding,
                "item",
                Some("native-thread".to_owned()),
                "native-turn".to_owned(),
                100,
            )
            .await;

        tracker
            .update_from_runtime_event(
                &instance,
                &binding,
                &RuntimeEvent::TurnRetrying(RuntimeTurnRetrying {
                    native_thread_id: Some("native-thread".to_owned()),
                    native_turn_id: Some("native-turn".to_owned()),
                    message: "Reconnecting... 2/5".to_owned(),
                    code: Some("stream_disconnected".to_owned()),
                    native: None,
                }),
                120,
            )
            .await;
        tracker
            .update_from_runtime_event(
                &instance,
                &binding,
                &RuntimeEvent::Error(RuntimeErrorEvent {
                    native_thread_id: Some("native-thread".to_owned()),
                    native_turn_id: Some("native-turn".to_owned()),
                    message: "legacy retryable error".to_owned(),
                    code: None,
                    retryable: true,
                    native: None,
                }),
                130,
            )
            .await;

        assert_eq!(tracker.due_items(160).await.len(), 1);
    }

    #[tokio::test]
    async fn observer_detach_preserves_execution_until_new_generation_observes_terminal() {
        let tracker = CliRuntimeCommandHeartbeatTracker::new(60);
        let first_observer = session_instance_with_generation(1);
        let replacement_observer = session_instance_with_generation(2);
        let binding = turn_binding(CLI_RUNTIME_TURN_STATUS_RUNNING);
        tracker
            .register(
                &first_observer,
                &binding,
                "item",
                Some("native-thread".to_owned()),
                "native-turn".to_owned(),
                100,
            )
            .await;

        tracker.detach_observer(&first_observer).await;

        assert_eq!(
            tracker.due_items(160).await.len(),
            1,
            "observer loss must not delete the execution identity"
        );
        let old_due = tracker.due_items(160).await.remove(0);
        tracker
            .register(
                &replacement_observer,
                &binding,
                "item",
                Some("native-thread".into()),
                "native-turn".into(),
                100,
            )
            .await;
        tracker.remove_key(&old_due).await;
        tracker.mark_heartbeat_succeeded(&old_due, 10_000).await;
        assert_eq!(
            tracker.next_deadline().await,
            Some(160),
            "old ACK cannot remove or postpone replacement registration"
        );
        tracker
            .update_from_runtime_event(
                &replacement_observer,
                &binding,
                &RuntimeEvent::ItemCompleted(
                    pioneer_cli_agent_runtime::event::RuntimeItemCompleted {
                        native_thread_id: Some("native-thread".to_owned()),
                        native_turn_id: "native-turn".to_owned(),
                        native_item_id: "item".to_owned(),
                        item_kind: "commandExecution".to_owned(),
                        text: None,
                        summary: Vec::new(),
                        content: Vec::new(),
                        phase: Default::default(),
                        metadata: None,
                        native_item_redacted: None,
                        native: None,
                    },
                ),
                161,
            )
            .await;
        assert!(
            tracker.due_items(10_000).await.is_empty(),
            "terminal observation from a replacement generation must stop supervision"
        );
    }
}
