use crate::cli_runtime::session_instance::CliSessionInstanceId;
use crate::cli_runtime::turn_binding::{
    CLI_RUNTIME_TURN_STATUS_RUNNING, CLI_RUNTIME_TURN_STATUS_STARTING,
};
use pioneer_cli_agent_runtime::event::RuntimeEvent;
use pioneer_crud::{CliRuntimeNativeTurnOwner, CliRuntimeTurnBindingRecord};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CliRuntimeCommandHeartbeatKey {
    pub(crate) workspace_id: String,
    pub(crate) runtime_id: String,
    pub(crate) thread_id: String,
    pub(crate) session_generation: u64,
    pub(crate) turn_id: String,
    pub(crate) item_id: String,
}

#[derive(Clone, Debug)]
struct CliRuntimeCommandHeartbeatState {
    native_thread_id: Option<String>,
    native_turn_id: String,
    last_heartbeat_at_unix: i64,
    last_attempt_at_unix: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct CliRuntimeCommandHeartbeatDueItem {
    pub(crate) key: CliRuntimeCommandHeartbeatKey,
    native_thread_id: Option<String>,
    native_turn_id: String,
}

impl CliRuntimeCommandHeartbeatDueItem {
    pub(crate) fn matches_active_turn_binding(
        &self,
        binding: &CliRuntimeTurnBindingRecord,
    ) -> bool {
        binding.turn_id == self.key.turn_id
            && binding.workspace_id == self.key.workspace_id
            && binding.runtime_id == self.key.runtime_id
            && binding.thread_id == self.key.thread_id
            && self
                .native_thread_id
                .as_deref()
                .is_none_or(|native_thread_id| binding.native_thread_id == native_thread_id)
            && binding.native_turn_id.as_deref() == Some(self.native_turn_id.as_str())
            && matches!(
                binding.status.as_str(),
                CLI_RUNTIME_TURN_STATUS_STARTING | CLI_RUNTIME_TURN_STATUS_RUNNING
            )
    }

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
    active: Arc<Mutex<HashMap<CliRuntimeCommandHeartbeatKey, CliRuntimeCommandHeartbeatState>>>,
    interval_secs: i64,
}

impl CliRuntimeCommandHeartbeatTracker {
    pub(crate) fn new(interval_secs: u64) -> Self {
        Self {
            active: Arc::new(Mutex::new(HashMap::new())),
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
                )
                .await;
            }
            _ if event.turn_terminal_kind().is_some() => {
                self.remove_turn(instance, turn_binding.turn_id.as_str())
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
        self.active.lock().await.insert(
            CliRuntimeCommandHeartbeatKey {
                workspace_id: instance.key().workspace_id.clone(),
                runtime_id: instance.key().runtime_id.clone(),
                thread_id: instance.key().thread_id.clone(),
                session_generation: instance.generation(),
                turn_id: turn_binding.turn_id.clone(),
                item_id: item_id.to_owned(),
            },
            CliRuntimeCommandHeartbeatState {
                native_thread_id,
                native_turn_id,
                last_heartbeat_at_unix: now_unix,
                last_attempt_at_unix: now_unix,
            },
        );
    }

    pub(crate) async fn remove_item(
        &self,
        instance: &CliSessionInstanceId,
        turn_id: &str,
        item_id: &str,
    ) {
        self.active
            .lock()
            .await
            .remove(&CliRuntimeCommandHeartbeatKey {
                workspace_id: instance.key().workspace_id.clone(),
                runtime_id: instance.key().runtime_id.clone(),
                thread_id: instance.key().thread_id.clone(),
                session_generation: instance.generation(),
                turn_id: turn_id.to_owned(),
                item_id: item_id.to_owned(),
            });
    }

    pub(crate) async fn remove_turn(&self, instance: &CliSessionInstanceId, turn_id: &str) {
        self.active.lock().await.retain(|key, _| {
            key.turn_id != turn_id
                || key.workspace_id != instance.key().workspace_id
                || key.runtime_id != instance.key().runtime_id
                || key.thread_id != instance.key().thread_id
                || key.session_generation != instance.generation()
        });
    }

    pub(crate) async fn remove_session(&self, instance: &CliSessionInstanceId) {
        self.active.lock().await.retain(|item_key, _| {
            item_key.workspace_id != instance.key().workspace_id
                || item_key.runtime_id != instance.key().runtime_id
                || item_key.thread_id != instance.key().thread_id
                || item_key.session_generation != instance.generation()
        });
    }

    pub(crate) async fn remove_key(&self, key: &CliRuntimeCommandHeartbeatKey) {
        self.active.lock().await.remove(key);
    }

    pub(crate) async fn mark_attempt_failed(
        &self,
        key: &CliRuntimeCommandHeartbeatKey,
        now_unix: i64,
    ) {
        if let Some(item) = self.active.lock().await.get_mut(key) {
            item.last_attempt_at_unix = now_unix;
        }
    }

    pub(crate) async fn mark_heartbeat_succeeded(
        &self,
        key: &CliRuntimeCommandHeartbeatKey,
        now_unix: i64,
    ) {
        if let Some(item) = self.active.lock().await.get_mut(key) {
            item.last_attempt_at_unix = now_unix;
            item.last_heartbeat_at_unix = now_unix;
        }
    }

    pub(crate) async fn due_items(&self, now_unix: i64) -> Vec<CliRuntimeCommandHeartbeatDueItem> {
        self.active
            .lock()
            .await
            .iter()
            .filter_map(|(key, item)| {
                let heartbeat_due =
                    now_unix.saturating_sub(item.last_heartbeat_at_unix) >= self.interval_secs;
                let attempt_due =
                    now_unix.saturating_sub(item.last_attempt_at_unix) >= self.interval_secs;
                if heartbeat_due && attempt_due {
                    Some(CliRuntimeCommandHeartbeatDueItem {
                        key: key.clone(),
                        native_thread_id: item.native_thread_id.clone(),
                        native_turn_id: item.native_turn_id.clone(),
                    })
                } else {
                    None
                }
            })
            .collect()
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

    fn turn_binding(status: &str) -> CliRuntimeTurnBindingRecord {
        CliRuntimeTurnBindingRecord {
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

        tracker.mark_heartbeat_succeeded(&due[0].key, 160).await;
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

        let due = tracker.due_items(160).await;
        assert_eq!(due.len(), 1);
        tracker.mark_attempt_failed(&due[0].key, 160).await;

        assert!(tracker.due_items(190).await.is_empty());
        assert_eq!(tracker.due_items(220).await.len(), 1);
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
}
