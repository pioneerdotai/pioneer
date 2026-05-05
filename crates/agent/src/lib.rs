mod agent_loop;
mod chat;
mod manager_recovery;
#[cfg(test)]
mod manager_tests;
mod memory;

use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, ItemDeltaNotification, ItemDeltaStream,
    ProgressCoalescingKey, ProviderFailureDetails, ThreadMode, TurnItemType, UserInput,
};
#[cfg(test)]
use pioneer_protocol::{
    ItemCompletedNotification, ItemStartedNotification, ItemToolRetryExhaustedNotification,
    ItemToolRetryResolvedNotification, ItemToolRetryScheduledNotification, PromptManifest,
    ToolOutputPolicySnapshot, TurnToolLoopBudgetExceededNotification,
};
use pioneer_provider::{ChatMessage, ProviderRegistry};
#[cfg(test)]
use pioneer_skills::SkillAuditEvent;
use pioneer_skills::{SkillPolicyKey, SkillTrustLevel};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use manager_recovery::apply_recovery_adjustments;
pub use memory::{
    AgentMemoryProvider, AgentMemoryTurnPolicyProvider, MemoryActiveContextPolicy,
    MemoryClassifierFallbackPolicy, MemoryExtractionPolicy, MemoryMutationToolPolicy,
    MemoryPolicyReasonCode, MemoryPolicySource, MemoryPromptPolicy, MemoryReadToolPolicy,
    MemoryRecallItem, MemoryRecallPolicy, MemoryRecallRequest, MemoryRecallSnapshot,
    MemoryToolMaterialization, MemoryTurnContext, MemoryTurnPolicy, MemoryTurnPolicyContext,
    MemoryTurnPolicyOverride, MemoryTurnPolicyRequest,
};
use pioneer_tools::{
    ComputerUseToolsConfig, ToolLoopBudgetConfig, ToolRetryBudgetConfig, WebToolsConfig,
};

const EVENT_CHANNEL_CAPACITY: usize = 1024;
const DURABLE_EVENT_CHANNEL_CAPACITY: usize = 1024;
const COMMAND_CHANNEL_CAPACITY: usize = 256;

#[derive(Debug, Clone)]
pub struct ToolLoopConfig {
    pub web: WebToolsConfig,
    pub computer_use: ComputerUseToolsConfig,
    pub skills: SkillsLoopConfig,
    pub budget: ToolLoopBudgetConfig,
    pub retry: ToolRetryBudgetConfig,
}

#[derive(Debug, Clone)]
pub struct SkillsLoopConfig {
    pub enabled: bool,
    pub max_skills_per_source: usize,
    pub max_skill_file_bytes: usize,
    pub prompt_max_chars: usize,
    pub allow_implicit_invocation: bool,
    pub system_roots: Vec<String>,
    pub user_roots: Vec<String>,
    pub workspace_roots: Vec<String>,
    pub registry_roots: Vec<String>,
    pub validation: SkillsValidationLoopConfig,
    pub security: SkillsSecurityLoopConfig,
    pub dependencies: SkillsDependenciesLoopConfig,
    pub runtime: SkillsRuntimeLoopConfig,
}

#[derive(Debug, Clone)]
pub struct SkillsRuntimeLoopConfig {
    pub enable_dynamic_tools: bool,
    pub enable_read_skill: bool,
    pub max_dynamic_tools_per_skill: usize,
    pub read_skill_max_chars: usize,
    pub compact_mode_threshold: usize,
    pub allow_shell_tools: bool,
    pub allow_http_tools: bool,
    pub allow_function_proxy_tools: bool,
}

#[derive(Debug, Clone)]
pub struct SkillsValidationLoopConfig {
    pub strict_agentskills: bool,
    pub accept_openclaw_profile: bool,
}

#[derive(Debug, Clone)]
pub struct SkillsSecurityLoopConfig {
    pub allow_untrusted_install: bool,
    pub min_trust_for_shell_tools: SkillTrustLevel,
    pub min_trust_for_http_tools: SkillTrustLevel,
    pub min_trust_for_function_proxy_tools: SkillTrustLevel,
    pub max_install_archive_bytes: usize,
    pub max_install_archive_compressed_bytes: usize,
    pub max_install_archive_uncompressed_bytes: usize,
    pub max_install_archive_entries: usize,
    pub max_install_file_bytes: usize,
    pub upload_ttl_secs: u64,
    pub upload_recommended_chunk_size_bytes: usize,
    pub upload_max_chunk_size_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct SkillsDependenciesLoopConfig {
    pub preflight_on_resolve: bool,
    pub runtime_recheck_on_tool_call: bool,
}

impl SkillsLoopConfig {
    pub fn normalized(&self) -> Self {
        Self {
            enabled: self.enabled,
            max_skills_per_source: self.max_skills_per_source.max(1),
            max_skill_file_bytes: self.max_skill_file_bytes.max(1),
            prompt_max_chars: self.prompt_max_chars.max(1),
            allow_implicit_invocation: self.allow_implicit_invocation,
            system_roots: self.system_roots.clone(),
            user_roots: self.user_roots.clone(),
            workspace_roots: self.workspace_roots.clone(),
            registry_roots: self.registry_roots.clone(),
            validation: self.validation.normalized(),
            security: self.security.normalized(),
            dependencies: self.dependencies.normalized(),
            runtime: self.runtime.normalized(),
        }
    }
}

impl SkillsRuntimeLoopConfig {
    pub fn normalized(&self) -> Self {
        Self {
            enable_dynamic_tools: self.enable_dynamic_tools,
            enable_read_skill: self.enable_read_skill,
            max_dynamic_tools_per_skill: self.max_dynamic_tools_per_skill.max(1),
            read_skill_max_chars: self.read_skill_max_chars.max(1),
            compact_mode_threshold: self.compact_mode_threshold,
            allow_shell_tools: self.allow_shell_tools,
            allow_http_tools: self.allow_http_tools,
            allow_function_proxy_tools: self.allow_function_proxy_tools,
        }
    }
}

impl SkillsValidationLoopConfig {
    pub fn normalized(&self) -> Self {
        Self {
            strict_agentskills: self.strict_agentskills,
            accept_openclaw_profile: self.accept_openclaw_profile,
        }
    }
}

impl SkillsSecurityLoopConfig {
    pub fn normalized(&self) -> Self {
        Self {
            allow_untrusted_install: self.allow_untrusted_install,
            min_trust_for_shell_tools: self.min_trust_for_shell_tools.clone(),
            min_trust_for_http_tools: self.min_trust_for_http_tools.clone(),
            min_trust_for_function_proxy_tools: self.min_trust_for_function_proxy_tools.clone(),
            max_install_archive_bytes: self.max_install_archive_bytes.max(1),
            max_install_archive_compressed_bytes: self.max_install_archive_compressed_bytes.max(1),
            max_install_archive_uncompressed_bytes: self
                .max_install_archive_uncompressed_bytes
                .max(1),
            max_install_archive_entries: self.max_install_archive_entries.max(1),
            max_install_file_bytes: self.max_install_file_bytes.max(1),
            upload_ttl_secs: self.upload_ttl_secs.max(60),
            upload_recommended_chunk_size_bytes: self.upload_recommended_chunk_size_bytes.max(1),
            upload_max_chunk_size_bytes: self.upload_max_chunk_size_bytes.max(1),
        }
    }
}

impl SkillsDependenciesLoopConfig {
    pub fn normalized(&self) -> Self {
        Self {
            preflight_on_resolve: self.preflight_on_resolve,
            runtime_recheck_on_tool_call: self.runtime_recheck_on_tool_call,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WorkspaceSkillPolicy {
    pub enabled: Option<bool>,
    pub allow_implicit_invocation: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentMcpAvailability {
    pub available_mcp: Vec<String>,
    pub blocked_mcp: Vec<String>,
}

#[derive(Clone, Default)]
pub struct AgentMcpMaterialization {
    pub bundles: Vec<pioneer_tools::ToolExtensionBundle>,
    pub available_mcp: Vec<String>,
    pub blocked_mcp: Vec<String>,
    pub diagnostics: Vec<String>,
}

#[async_trait::async_trait]
pub trait AgentMcpToolProvider: Send + Sync {
    async fn mcp_availability(&self, workspace_id: &str) -> Result<AgentMcpAvailability, String>;

    async fn materialize_mcp_tools(
        &self,
        workspace_id: &str,
        turn_id: &str,
    ) -> Result<AgentMcpMaterialization, String>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskTurnContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingAttachedTask {
    pub task_id: String,
    pub run_id: Option<String>,
    pub title: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalTaskObservation {
    pub task_id: String,
    pub run_id: Option<String>,
    pub title: String,
    pub status: String,
    pub summary: Option<String>,
    pub error_message: Option<String>,
    pub child_thread_id: Option<String>,
    pub child_turn_id: Option<String>,
}

#[derive(Clone, Default)]
pub struct TaskToolMaterialization {
    pub bundles: Vec<pioneer_tools::ToolExtensionBundle>,
    pub diagnostics: Vec<String>,
}

#[async_trait::async_trait]
pub trait TaskToolProvider: Send + Sync {
    async fn materialize_task_tools(
        &self,
        context: TaskTurnContext,
    ) -> Result<TaskToolMaterialization, String>;

    async fn pending_attached_tasks(
        &self,
        context: TaskTurnContext,
    ) -> Result<Vec<PendingAttachedTask>, String>;

    async fn terminal_attached_task_observations(
        &self,
        context: TaskTurnContext,
    ) -> Result<Vec<TerminalTaskObservation>, String>;

    async fn cleanup_attached_tasks(
        &self,
        context: TaskTurnContext,
        reason: String,
    ) -> Result<(), String>;
}

impl ToolLoopConfig {
    pub fn normalized(&self) -> Self {
        Self {
            web: self.web.normalized(),
            computer_use: self.computer_use.normalized(),
            skills: self.skills.normalized(),
            budget: self.budget.normalized(),
            retry: self.retry.normalized(),
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub enum AgentEvent {
    PromptManifestCompiled {
        thread_id: String,
        turn_id: String,
        manifest: PromptManifest,
    },
    TurnSkillsResolved {
        thread_id: String,
        turn_id: String,
        bindings: Vec<pioneer_protocol::TurnSkillBinding>,
    },
    SkillAuditEvents {
        thread_id: String,
        turn_id: String,
        events: Vec<SkillAuditEvent>,
    },
    TurnLlmContextAppended {
        thread_id: String,
        turn_id: String,
        item_id: String,
        attempt_id: Option<String>,
        sequence: i64,
        source: String,
        tool_name: String,
        payload: pioneer_tools::ToolResultView,
        output_policy_snapshot: ToolOutputPolicySnapshot,
    },
    ItemStarted(ItemStartedNotification),
    ItemDelta(ItemDeltaNotification),
    ItemCompleted(ItemCompletedNotification),
    ItemToolRetryScheduled(ItemToolRetryScheduledNotification),
    ItemToolRetryResolved(ItemToolRetryResolvedNotification),
    ItemToolRetryExhausted(ItemToolRetryExhaustedNotification),
    TurnToolLoopBudgetExceeded(TurnToolLoopBudgetExceededNotification),
    ItemHeartbeat {
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
    },
    ProviderFailureDetected {
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        failure: ProviderFailureDetails,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    },
    RecoveryAttemptSucceeded {
        thread_id: String,
        turn_id: String,
        recovery: pioneer_protocol::RecoveryAttemptContext,
    },
    TurnCompleted {
        thread_id: String,
        turn_id: String,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    },
    TurnFailed {
        thread_id: String,
        turn_id: String,
        error: String,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentEventHubError {
    DurableLaneClosed,
}

impl Display for AgentEventHubError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DurableLaneClosed => write!(f, "agent durable event lane is closed"),
        }
    }
}

impl Error for AgentEventHubError {}

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
                    turn_id,
                    item_id,
                    "dropping heartbeat progress because progress coalescer key limit is reached"
                );
                return;
            }
            state.heartbeats.insert(
                key,
                PendingHeartbeat {
                    workspace_id,
                    thread_id,
                    turn_id,
                    item_id,
                    item_type,
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
                    events.push(AgentProgressEvent::ItemHeartbeat {
                        workspace_id: heartbeat.workspace_id,
                        thread_id: heartbeat.thread_id,
                        turn_id: heartbeat.turn_id,
                        item_id: heartbeat.item_id,
                        item_type: heartbeat.item_type,
                    });
                }
            }
            if state.pending.is_empty() && state.heartbeats.is_empty() {
                state.flush_scheduled = false;
            }
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
                    events.push(AgentProgressEvent::ItemHeartbeat {
                        workspace_id: heartbeat.workspace_id,
                        thread_id: heartbeat.thread_id,
                        turn_id: heartbeat.turn_id,
                        item_id: heartbeat.item_id,
                        item_type: heartbeat.item_type,
                    });
                }
            }
            if state.pending.is_empty() && state.heartbeats.is_empty() {
                state.flush_scheduled = false;
            }
            events
        };
        self.send_live_events(events);
    }

    pub async fn flush_for_durable(&self, event: &AgentDurableEvent) {
        match event {
            AgentDurableEvent::ItemCompleted { notification } => {
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
                    "dropping progress because progress coalescer key limit is reached"
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
                    events.push(AgentProgressEvent::ItemHeartbeat {
                        workspace_id: heartbeat.workspace_id,
                        thread_id: heartbeat.thread_id,
                        turn_id: heartbeat.turn_id,
                        item_id: heartbeat.item_id,
                        item_type: heartbeat.item_type,
                    });
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

#[derive(Debug)]
pub struct AgentEventHub {
    durable_tx: mpsc::Sender<AgentDurableEvent>,
    durable_rx: Mutex<Option<mpsc::Receiver<AgentDurableEvent>>>,
    progress: ProgressCoalescer,
    committed_tx: broadcast::Sender<AgentDurableEvent>,
}

impl AgentEventHub {
    pub fn new() -> Self {
        Self::with_capacity(DURABLE_EVENT_CHANNEL_CAPACITY, EVENT_CHANNEL_CAPACITY)
    }

    pub fn with_capacity(durable_capacity: usize, live_capacity: usize) -> Self {
        Self::with_progress_config(
            durable_capacity,
            live_capacity,
            ProgressCoalescerConfig::default(),
        )
    }

    pub fn with_progress_config(
        durable_capacity: usize,
        live_capacity: usize,
        progress_config: ProgressCoalescerConfig,
    ) -> Self {
        let (durable_tx, durable_rx) = mpsc::channel(durable_capacity.max(1));
        let (committed_tx, _) = broadcast::channel(live_capacity.max(1));
        Self {
            durable_tx,
            durable_rx: Mutex::new(Some(durable_rx)),
            progress: ProgressCoalescer::new(live_capacity, progress_config),
            committed_tx,
        }
    }

    pub async fn publish_durable(
        &self,
        event: AgentDurableEvent,
    ) -> Result<(), AgentEventHubError> {
        self.flush_progress_for_durable(&event).await;
        self.durable_tx
            .send(event)
            .await
            .map_err(|_| AgentEventHubError::DurableLaneClosed)
    }

    pub fn publish_progress(&self, event: AgentProgressEvent) {
        self.progress.offer(event);
    }

    pub async fn flush_progress_for_durable(&self, event: &AgentDurableEvent) {
        self.progress.flush_for_durable(event).await;
    }

    pub async fn flush_progress_for_item(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
    ) {
        self.progress
            .flush_item(workspace_id, thread_id, turn_id, item_id)
            .await;
    }

    pub async fn shutdown_progress(&self) {
        self.progress.flush_all().await;
    }

    pub fn publish_heartbeat(
        &self,
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
    ) {
        self.progress
            .offer_heartbeat(workspace_id, thread_id, turn_id, item_id, item_type);
    }

    pub fn subscribe_live(&self) -> broadcast::Receiver<AgentProgressEvent> {
        self.progress.subscribe_live()
    }

    pub fn subscribe_committed(&self) -> broadcast::Receiver<AgentDurableEvent> {
        self.committed_tx.subscribe()
    }

    pub fn publish_committed(&self, event: AgentDurableEvent) {
        let _ = self.committed_tx.send(event);
    }

    pub async fn take_durable_receiver(&self) -> Option<mpsc::Receiver<AgentDurableEvent>> {
        self.durable_rx.lock().await.take()
    }
}

impl Default for AgentEventHub {
    fn default() -> Self {
        Self::new()
    }
}

impl AsRef<AgentEventHub> for AgentEventHub {
    fn as_ref(&self) -> &AgentEventHub {
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentStartError {
    ThreadNotFound,
    TurnAlreadyRunning,
    ThreadWorkspaceMismatch {
        expected_workspace_id: String,
        actual_workspace_id: String,
    },
    Internal(String),
}

impl Display for AgentStartError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ThreadNotFound => write!(f, "thread is not registered in agent manager"),
            Self::TurnAlreadyRunning => write!(f, "thread already has a running turn"),
            Self::ThreadWorkspaceMismatch {
                expected_workspace_id,
                actual_workspace_id,
            } => write!(
                f,
                "thread workspace mismatch: expected `{expected_workspace_id}`, got `{actual_workspace_id}`"
            ),
            Self::Internal(error) => write!(f, "internal agent error: {error}"),
        }
    }
}

impl Error for AgentStartError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentControlError {
    ThreadNotFound,
    NoActiveTurn,
    TurnMismatch,
    AttemptNotRunning,
    Internal(String),
}

impl Display for AgentControlError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ThreadNotFound => write!(f, "thread is not registered in agent manager"),
            Self::NoActiveTurn => write!(f, "thread has no active turn"),
            Self::TurnMismatch => write!(f, "active turn does not match the requested turn"),
            Self::AttemptNotRunning => write!(f, "turn item attempt is not running"),
            Self::Internal(error) => write!(f, "internal agent control error: {error}"),
        }
    }
}

impl Error for AgentControlError {}

#[derive(Debug, Clone)]
pub struct RetainedToolLlmContext {
    pub item_id: String,
    pub tool_name: String,
    pub arguments: String,
    pub sequence: i64,
    pub payload: JsonValue,
}

#[derive(Debug, Clone)]
pub struct RecoveryAttemptRequest {
    pub recovery_job_id: String,
    pub recovery_attempt_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub force_non_stream: bool,
    pub refresh_provider_auth: bool,
    pub compact_history: bool,
    pub continue_generation: bool,
    pub model_override: Option<String>,
    pub retained_llm_context: Vec<RetainedToolLlmContext>,
}

#[derive(Debug, Clone, Default)]
struct TurnExecutionOptions {
    force_non_stream: bool,
    continue_generation_hint: bool,
}

#[derive(Debug, Clone)]
struct ActiveTurnRequest {
    turn_id: String,
    mode: ThreadMode,
    model: String,
    provider_name: String,
    workspace_skill_policies: HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
    input: Vec<UserInput>,
    history: Vec<ChatMessage>,
    retained_llm_context: Vec<RetainedToolLlmContext>,
    execution_options: TurnExecutionOptions,
}

#[derive(Debug, Clone)]
enum TurnTaskFailure {
    Terminal(String),
    ProviderFailure {
        item_id: String,
        item_type: TurnItemType,
        failure: ProviderFailureDetails,
    },
}

#[derive(Debug)]
enum AgentCommand {
    StartTurn {
        turn_id: String,
        mode: ThreadMode,
        model: String,
        provider_name: String,
        workspace_skill_policies: HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
        input: Vec<UserInput>,
        history: Vec<ChatMessage>,
        ack: oneshot::Sender<Result<(), AgentStartError>>,
    },
    TurnTaskFinished {
        turn_id: String,
        run_id: u64,
        result: Result<(), TurnTaskFailure>,
    },
    CancelAttempt {
        turn_id: String,
        item_id: String,
        ack: oneshot::Sender<Result<(), AgentControlError>>,
    },
    CancelTurn {
        turn_id: String,
        reason: String,
        ack: oneshot::Sender<Result<(), AgentControlError>>,
    },
    StartRecoveryAttempt {
        request: RecoveryAttemptRequest,
        ack: oneshot::Sender<Result<(), AgentControlError>>,
    },
    RecoveryAttemptSucceeded {
        turn_id: String,
        run_id: u64,
        recovery: pioneer_protocol::RecoveryAttemptContext,
    },
    Shutdown,
}

#[derive(Clone)]
struct TurnExecutionControl {
    attempt_controls: Arc<tokio::sync::Mutex<HashMap<String, AttemptControl>>>,
    command_tx: mpsc::Sender<AgentCommand>,
    run_id: u64,
}

#[derive(Clone)]
struct AttemptControl {
    cancellation_token: CancellationToken,
    recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
}

impl TurnExecutionControl {
    fn new(command_tx: mpsc::Sender<AgentCommand>, run_id: u64) -> Self {
        Self {
            attempt_controls: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            command_tx,
            run_id,
        }
    }

    async fn register_attempt(&self, item_id: String) -> CancellationToken {
        let token = CancellationToken::new();
        self.attempt_controls.lock().await.insert(
            item_id,
            AttemptControl {
                cancellation_token: token.clone(),
                recovery: None,
            },
        );
        token
    }

    async fn complete_attempt(&self, turn_id: &str, item_id: &str) {
        let recovery = self
            .attempt_controls
            .lock()
            .await
            .remove(item_id)
            .and_then(|control| control.recovery);

        self.succeed_recovery_attempt(turn_id, recovery).await;
    }

    async fn succeed_recovery_attempt(
        &self,
        turn_id: &str,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    ) {
        let Some(recovery) = recovery else {
            return;
        };
        let _ = self
            .command_tx
            .send(AgentCommand::RecoveryAttemptSucceeded {
                turn_id: turn_id.to_owned(),
                run_id: self.run_id,
                recovery,
            })
            .await;
    }

    async fn cancel_attempt(&self, item_id: &str) -> bool {
        let token = self
            .attempt_controls
            .lock()
            .await
            .get(item_id)
            .map(|control| control.cancellation_token.clone());
        if let Some(token) = token {
            token.cancel();
            true
        } else {
            false
        }
    }

    async fn cancel_attempt_for_recovery(
        &self,
        item_id: &str,
        recovery: pioneer_protocol::RecoveryAttemptContext,
    ) -> bool {
        let token = {
            let mut controls = self.attempt_controls.lock().await;
            let Some(control) = controls.get_mut(item_id) else {
                return false;
            };
            control.recovery = Some(recovery);
            control.cancellation_token.clone()
        };

        token.cancel();
        true
    }

    async fn cancel_all_attempts(&self) {
        let tokens = self
            .attempt_controls
            .lock()
            .await
            .values()
            .map(|control| control.cancellation_token.clone())
            .collect::<Vec<_>>();
        for token in tokens {
            token.cancel();
        }
    }
}

struct AgentThreadHandle {
    workspace_id: String,
    command_tx: mpsc::Sender<AgentCommand>,
    event_hub: Arc<AgentEventHub>,
    loop_handle: JoinHandle<()>,
}

#[derive(Default)]
struct AgentManagerState {
    threads: HashMap<String, AgentThreadHandle>,
}

pub struct AgentManager {
    state: RwLock<AgentManagerState>,
    provider_registry: Arc<ProviderRegistry>,
    tool_loop_config: ToolLoopConfig,
    mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    task_tool_provider: RwLock<Option<Arc<dyn TaskToolProvider>>>,
    memory_provider: RwLock<Option<Arc<dyn AgentMemoryProvider>>>,
    memory_turn_policy_provider: RwLock<Option<Arc<dyn AgentMemoryTurnPolicyProvider>>>,
}

impl AgentManager {
    pub fn new(provider_registry: Arc<ProviderRegistry>, tool_loop_config: ToolLoopConfig) -> Self {
        Self::new_with_mcp(provider_registry, tool_loop_config, None)
    }

    pub fn new_with_mcp(
        provider_registry: Arc<ProviderRegistry>,
        tool_loop_config: ToolLoopConfig,
        mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    ) -> Self {
        Self::new_with_mcp_and_memory(provider_registry, tool_loop_config, mcp_tool_provider, None)
    }

    pub fn new_with_mcp_and_memory(
        provider_registry: Arc<ProviderRegistry>,
        tool_loop_config: ToolLoopConfig,
        mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
        memory_provider: Option<Arc<dyn AgentMemoryProvider>>,
    ) -> Self {
        Self {
            state: RwLock::new(AgentManagerState::default()),
            provider_registry,
            tool_loop_config: tool_loop_config.normalized(),
            mcp_tool_provider,
            task_tool_provider: RwLock::new(None),
            memory_provider: RwLock::new(memory_provider),
            memory_turn_policy_provider: RwLock::new(None),
        }
    }

    pub async fn set_task_tool_provider(&self, provider: Option<Arc<dyn TaskToolProvider>>) {
        *self.task_tool_provider.write().await = provider;
    }

    pub async fn set_memory_provider(&self, provider: Option<Arc<dyn AgentMemoryProvider>>) {
        *self.memory_provider.write().await = provider;
    }

    pub async fn set_memory_turn_policy_provider(
        &self,
        provider: Option<Arc<dyn AgentMemoryTurnPolicyProvider>>,
    ) {
        *self.memory_turn_policy_provider.write().await = provider;
    }

    pub async fn has_memory_provider(&self) -> bool {
        self.memory_provider.read().await.is_some()
    }

    pub async fn ensure_thread(
        &self,
        thread_id: &str,
        workspace_id: &str,
    ) -> Result<(), AgentStartError> {
        if let Some(existing_workspace_id) = self
            .state
            .read()
            .await
            .threads
            .get(thread_id)
            .map(|thread| thread.workspace_id.clone())
        {
            if existing_workspace_id != workspace_id {
                return Err(AgentStartError::ThreadWorkspaceMismatch {
                    expected_workspace_id: existing_workspace_id,
                    actual_workspace_id: workspace_id.to_owned(),
                });
            }
            return Ok(());
        }

        let thread_id_owned = thread_id.to_owned();
        let workspace_id_owned = workspace_id.to_owned();

        let (command_tx, command_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        let event_hub = Arc::new(AgentEventHub::new());

        let loop_handle = tokio::spawn(agent_loop::run_agent_loop(
            thread_id_owned,
            workspace_id_owned.clone(),
            self.provider_registry.clone(),
            self.tool_loop_config.clone(),
            self.mcp_tool_provider.clone(),
            self.task_tool_provider.read().await.clone(),
            self.memory_provider.read().await.clone(),
            self.memory_turn_policy_provider.read().await.clone(),
            command_tx.clone(),
            command_rx,
            event_hub.clone(),
        ));

        self.state.write().await.threads.insert(
            thread_id.to_owned(),
            AgentThreadHandle {
                workspace_id: workspace_id_owned,
                command_tx,
                event_hub,
                loop_handle,
            },
        );

        Ok(())
    }

    pub async fn start_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        mode: ThreadMode,
        model: &str,
        provider_name: &str,
        workspace_skill_policies: HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
        input: Vec<UserInput>,
        history: Vec<ChatMessage>,
    ) -> Result<(), AgentStartError> {
        let command_tx = {
            let state = self.state.read().await;
            let Some(thread) = state.threads.get(thread_id) else {
                return Err(AgentStartError::ThreadNotFound);
            };
            thread.command_tx.clone()
        };

        let (ack_tx, ack_rx) = oneshot::channel();

        command_tx
            .send(AgentCommand::StartTurn {
                turn_id: turn_id.to_owned(),
                mode,
                model: model.to_owned(),
                provider_name: provider_name.to_owned(),
                workspace_skill_policies,
                input,
                history,
                ack: ack_tx,
            })
            .await
            .map_err(|_| AgentStartError::ThreadNotFound)?;

        ack_rx.await.unwrap_or_else(|_| {
            Err(AgentStartError::Internal(
                "agent loop dropped ack".to_owned(),
            ))
        })
    }

    pub async fn subscribe_progress(
        &self,
        thread_id: &str,
    ) -> Option<broadcast::Receiver<AgentProgressEvent>> {
        let state = self.state.read().await;
        state
            .threads
            .get(thread_id)
            .map(|thread| thread.event_hub.subscribe_live())
    }

    pub async fn subscribe_committed(
        &self,
        thread_id: &str,
    ) -> Option<broadcast::Receiver<AgentDurableEvent>> {
        let state = self.state.read().await;
        state
            .threads
            .get(thread_id)
            .map(|thread| thread.event_hub.subscribe_committed())
    }

    pub async fn take_durable_receiver(
        &self,
        thread_id: &str,
    ) -> Option<mpsc::Receiver<AgentDurableEvent>> {
        let hub = {
            let state = self.state.read().await;
            state
                .threads
                .get(thread_id)
                .map(|thread| thread.event_hub.clone())
        }?;
        hub.take_durable_receiver().await
    }

    pub async fn publish_committed(&self, thread_id: &str, event: AgentDurableEvent) {
        let hub = {
            let state = self.state.read().await;
            state
                .threads
                .get(thread_id)
                .map(|thread| thread.event_hub.clone())
        };
        if let Some(hub) = hub {
            hub.publish_committed(event);
        }
    }

    pub async fn publish_progress(&self, thread_id: &str, event: AgentProgressEvent) -> bool {
        let hub = {
            let state = self.state.read().await;
            state
                .threads
                .get(thread_id)
                .map(|thread| thread.event_hub.clone())
        };
        if let Some(hub) = hub {
            hub.publish_progress(event);
            true
        } else {
            false
        }
    }

    pub async fn flush_progress_for_item(
        &self,
        thread_id: &str,
        workspace_id: &str,
        turn_id: &str,
        item_id: &str,
    ) -> bool {
        let hub = {
            let state = self.state.read().await;
            state
                .threads
                .get(thread_id)
                .map(|thread| thread.event_hub.clone())
        };
        if let Some(hub) = hub {
            hub.flush_progress_for_item(workspace_id, thread_id, turn_id, item_id)
                .await;
            true
        } else {
            false
        }
    }

    pub async fn cancel_attempt(
        &self,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
    ) -> Result<(), AgentControlError> {
        let command_tx = {
            let state = self.state.read().await;
            let Some(thread) = state.threads.get(thread_id) else {
                return Err(AgentControlError::ThreadNotFound);
            };
            thread.command_tx.clone()
        };

        let (ack_tx, ack_rx) = oneshot::channel();

        command_tx
            .send(AgentCommand::CancelAttempt {
                turn_id: turn_id.to_owned(),
                item_id: item_id.to_owned(),
                ack: ack_tx,
            })
            .await
            .map_err(|_| AgentControlError::ThreadNotFound)?;

        ack_rx.await.unwrap_or_else(|_| {
            Err(AgentControlError::Internal(
                "agent loop dropped cancel ack".to_owned(),
            ))
        })
    }

    pub async fn cancel_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        reason: &str,
    ) -> Result<(), AgentControlError> {
        let command_tx = {
            let state = self.state.read().await;
            let Some(thread) = state.threads.get(thread_id) else {
                return Err(AgentControlError::ThreadNotFound);
            };
            thread.command_tx.clone()
        };

        let (ack_tx, ack_rx) = oneshot::channel();

        command_tx
            .send(AgentCommand::CancelTurn {
                turn_id: turn_id.to_owned(),
                reason: reason.to_owned(),
                ack: ack_tx,
            })
            .await
            .map_err(|_| AgentControlError::ThreadNotFound)?;

        ack_rx.await.unwrap_or_else(|_| {
            Err(AgentControlError::Internal(
                "agent loop dropped cancel turn ack".to_owned(),
            ))
        })
    }

    pub async fn start_recovery_attempt(
        &self,
        thread_id: &str,
        request: RecoveryAttemptRequest,
    ) -> Result<(), AgentControlError> {
        let command_tx = {
            let state = self.state.read().await;
            let Some(thread) = state.threads.get(thread_id) else {
                return Err(AgentControlError::ThreadNotFound);
            };
            thread.command_tx.clone()
        };

        let (ack_tx, ack_rx) = oneshot::channel();

        command_tx
            .send(AgentCommand::StartRecoveryAttempt {
                request,
                ack: ack_tx,
            })
            .await
            .map_err(|_| AgentControlError::ThreadNotFound)?;

        ack_rx.await.unwrap_or_else(|_| {
            Err(AgentControlError::Internal(
                "agent loop dropped recovery ack".to_owned(),
            ))
        })
    }

    pub async fn remove_thread(&self, thread_id: &str) {
        let thread = self.state.write().await.threads.remove(thread_id);
        let Some(thread) = thread else {
            return;
        };

        let _ = thread.command_tx.send(AgentCommand::Shutdown).await;
        thread.loop_handle.abort();
    }

    pub async fn has_thread(&self, thread_id: &str) -> bool {
        self.state.read().await.threads.contains_key(thread_id)
    }
}

#[cfg(test)]
mod event_class_tests {
    use super::*;
    use pioneer_protocol::{ItemDeltaStream, TurnItem};
    use tokio::time::{Duration, sleep, timeout};

    fn reasoning_item(id: &str) -> TurnItem {
        TurnItem::Reasoning {
            id: id.to_owned(),
            summary: Vec::new(),
            content: Vec::new(),
        }
    }

    fn durable_turn_completed(turn_id: &str) -> AgentDurableEvent {
        AgentDurableEvent::TurnCompleted {
            thread_id: "thread_1".to_owned(),
            turn_id: turn_id.to_owned(),
            recovery: None,
        }
    }

    fn test_progress_config() -> ProgressCoalescerConfig {
        ProgressCoalescerConfig {
            flush_interval: Duration::from_secs(60),
            max_pending_keys: 16,
            max_append_bytes_per_key: 128,
            max_snapshot_bytes_per_key: 64,
            max_flush_batch_size: 16,
        }
    }

    fn delta_notification(
        item_id: &str,
        stream: ItemDeltaStream,
        delta: impl Into<String>,
    ) -> ItemDeltaNotification {
        ItemDeltaNotification {
            workspace_id: "ws_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            item_id: item_id.to_owned(),
            delta: delta.into(),
            stream: Some(stream),
            payload: None,
            markdown: None,
            markdown_version: None,
        }
    }

    #[tokio::test]
    async fn durable_lane_applies_backpressure_when_full() {
        let hub = Arc::new(AgentEventHub::with_capacity(1, 1));
        let mut durable_rx = hub
            .take_durable_receiver()
            .await
            .expect("durable receiver should be available once");

        hub.publish_durable(durable_turn_completed("turn_1"))
            .await
            .expect("first event should enqueue");

        let pending_hub = hub.clone();
        let pending = tokio::spawn(async move {
            pending_hub
                .publish_durable(durable_turn_completed("turn_2"))
                .await
        });

        sleep(Duration::from_millis(25)).await;
        assert!(
            !pending.is_finished(),
            "full durable queue must apply backpressure instead of dropping"
        );

        assert!(durable_rx.recv().await.is_some());
        pending
            .await
            .expect("publish task should complete")
            .expect("second event should enqueue after capacity is freed");
        assert!(matches!(
            durable_rx.recv().await,
            Some(AgentDurableEvent::TurnCompleted { turn_id, .. }) if turn_id == "turn_2"
        ));
    }

    #[tokio::test]
    async fn closed_durable_lane_is_reported_to_publisher() {
        let hub = AgentEventHub::with_capacity(1, 1);
        let durable_rx = hub
            .take_durable_receiver()
            .await
            .expect("durable receiver should be available once");
        drop(durable_rx);

        let error = hub
            .publish_durable(durable_turn_completed("turn_1"))
            .await
            .expect_err("closed durable lane must fail publishing");

        assert_eq!(error, AgentEventHubError::DurableLaneClosed);
    }

    #[tokio::test]
    async fn lagged_live_lane_does_not_drop_durable_events() {
        let hub = AgentEventHub::with_progress_config(8, 1, test_progress_config());
        let mut durable_rx = hub
            .take_durable_receiver()
            .await
            .expect("durable receiver should be available once");
        let _lagged_live_rx = hub.subscribe_live();

        for index in 0..16 {
            hub.publish_progress(AgentProgressEvent::ItemDelta {
                notification: ItemDeltaNotification {
                    workspace_id: "ws_1".to_owned(),
                    thread_id: "thread_1".to_owned(),
                    turn_id: "turn_1".to_owned(),
                    item_id: "item_1".to_owned(),
                    delta: format!("delta {index}"),
                    stream: Some(ItemDeltaStream::AgentMessage),
                    payload: None,
                    markdown: None,
                    markdown_version: None,
                },
            });
        }

        let mut live_rx_after_progress = hub.subscribe_live();
        hub.publish_durable(AgentDurableEvent::TurnCompleted {
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            recovery: None,
        })
        .await
        .expect("durable publish must not depend on live receiver health");

        let event = timeout(Duration::from_secs(1), durable_rx.recv())
            .await
            .expect("durable event should arrive")
            .expect("durable lane should remain open");
        assert!(matches!(event, AgentDurableEvent::TurnCompleted { .. }));

        let event = timeout(Duration::from_secs(1), live_rx_after_progress.recv())
            .await
            .expect("pending progress should flush before durable completion")
            .expect("live lane should remain open");
        assert!(
            matches!(event, AgentProgressEvent::ItemDelta { .. }),
            "durable events must not be mirrored into the lossy live lane"
        );
        assert!(
            timeout(Duration::from_millis(25), live_rx_after_progress.recv())
                .await
                .is_err(),
            "progress should be coalesced into a bounded live update"
        );
    }

    #[tokio::test]
    async fn raw_progress_waits_for_coalescer_flush() {
        let hub = AgentEventHub::with_progress_config(8, 8, test_progress_config());
        let mut live_rx = hub.subscribe_live();

        hub.publish_progress(AgentProgressEvent::ItemDelta {
            notification: delta_notification(
                "item_1",
                ItemDeltaStream::AgentMessage,
                "first delta",
            ),
        });

        assert!(
            timeout(Duration::from_millis(25), live_rx.recv())
                .await
                .is_err(),
            "raw progress must not bypass the coalescer"
        );

        hub.shutdown_progress().await;
        assert!(matches!(
            timeout(Duration::from_secs(1), live_rx.recv()).await,
            Ok(Ok(AgentProgressEvent::ItemDelta { notification })) if notification.delta == "first delta"
        ));
    }

    #[tokio::test]
    async fn append_progress_is_coalesced_and_bounded() {
        let hub = AgentEventHub::with_progress_config(
            8,
            8,
            ProgressCoalescerConfig {
                max_append_bytes_per_key: 14,
                ..test_progress_config()
            },
        );
        let mut live_rx = hub.subscribe_live();

        for delta in ["aaa", "bbb", "ccc", "ddd", "eee", "fff"] {
            hub.publish_progress(AgentProgressEvent::ItemDelta {
                notification: delta_notification("item_1", ItemDeltaStream::AgentMessage, delta),
            });
        }

        hub.shutdown_progress().await;
        let notification = match timeout(Duration::from_secs(1), live_rx.recv()).await {
            Ok(Ok(AgentProgressEvent::ItemDelta { notification })) => notification,
            other => panic!("expected coalesced item delta, got {other:?}"),
        };
        assert!(notification.delta.len() <= 14);
        assert!(
            notification
                .payload
                .as_ref()
                .and_then(|payload| payload.get("truncated"))
                .and_then(JsonValue::as_bool)
                .unwrap_or(false)
        );
        assert!(
            timeout(Duration::from_millis(25), live_rx.recv())
                .await
                .is_err(),
            "append progress should flush as one bounded update for one key"
        );
    }

    #[tokio::test]
    async fn snapshot_progress_replaces_older_snapshots() {
        let hub = AgentEventHub::with_progress_config(8, 8, test_progress_config());
        let mut live_rx = hub.subscribe_live();

        for stage in ["queued", "running", "almost done"] {
            hub.publish_progress(AgentProgressEvent::ItemDelta {
                notification: delta_notification("tool_1", ItemDeltaStream::ToolProgress, stage),
            });
        }

        hub.shutdown_progress().await;
        assert!(matches!(
            timeout(Duration::from_secs(1), live_rx.recv()).await,
            Ok(Ok(AgentProgressEvent::ItemDelta { notification })) if notification.delta == "almost done"
        ));
        assert!(
            timeout(Duration::from_millis(25), live_rx.recv())
                .await
                .is_err(),
            "snapshot progress should replace stale snapshots"
        );
    }

    #[tokio::test]
    async fn item_completed_flushes_progress_before_durable_event() {
        let hub = AgentEventHub::with_progress_config(8, 8, test_progress_config());
        let mut live_rx = hub.subscribe_live();
        let mut durable_rx = hub
            .take_durable_receiver()
            .await
            .expect("durable receiver should be available once");

        hub.publish_progress(AgentProgressEvent::ItemDelta {
            notification: delta_notification("item_1", ItemDeltaStream::Generic, "thinking"),
        });
        hub.publish_durable(AgentDurableEvent::ItemCompleted {
            notification: ItemCompletedNotification {
                workspace_id: "ws_1".to_owned(),
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
                item: reasoning_item("item_1"),
            },
        })
        .await
        .expect("durable completion should publish");

        assert!(matches!(
            timeout(Duration::from_secs(1), live_rx.recv()).await,
            Ok(Ok(AgentProgressEvent::ItemDelta { notification })) if notification.delta == "thinking"
        ));
        assert!(matches!(
            timeout(Duration::from_secs(1), durable_rx.recv()).await,
            Ok(Some(AgentDurableEvent::ItemCompleted { notification })) if notification.item.item_id() == "item_1"
        ));
    }

    #[tokio::test]
    async fn heartbeats_are_coalesced() {
        let hub = AgentEventHub::with_progress_config(8, 8, test_progress_config());
        let mut live_rx = hub.subscribe_live();

        for _ in 0..4 {
            hub.publish_heartbeat(
                "ws_1".to_owned(),
                "thread_1".to_owned(),
                "turn_1".to_owned(),
                "item_1".to_owned(),
                TurnItemType::Reasoning,
            );
        }

        hub.shutdown_progress().await;
        assert!(matches!(
            timeout(Duration::from_secs(1), live_rx.recv()).await,
            Ok(Ok(AgentProgressEvent::ItemHeartbeat { item_id, .. })) if item_id == "item_1"
        ));
        assert!(
            timeout(Duration::from_millis(25), live_rx.recv())
                .await
                .is_err(),
            "heartbeat progress should be rate-limited per item"
        );
    }

    #[tokio::test]
    async fn task_progress_uses_snapshot_semantics() {
        let hub = AgentEventHub::with_progress_config(8, 8, test_progress_config());
        let mut live_rx = hub.subscribe_live();

        for summary in ["started", "halfway", "done soon"] {
            hub.publish_progress(AgentProgressEvent::TaskProgress {
                workspace_id: "ws_1".to_owned(),
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
                item_id: "task_item_task_1".to_owned(),
                task_id: "task_1".to_owned(),
                run_id: Some("run_1".to_owned()),
                summary: summary.to_owned(),
            });
        }

        hub.shutdown_progress().await;
        assert!(matches!(
            timeout(Duration::from_secs(1), live_rx.recv()).await,
            Ok(Ok(AgentProgressEvent::ItemDelta { notification }))
                if notification.stream == Some(ItemDeltaStream::ToolProgress)
                    && notification.delta == "done soon"
        ));
    }

    #[tokio::test]
    async fn committed_lane_is_emitted_explicitly_after_durable_publish() {
        let hub = AgentEventHub::with_capacity(8, 8);
        let mut committed_rx = hub.subscribe_committed();

        hub.publish_durable(durable_turn_completed("turn_1"))
            .await
            .expect("durable publish should succeed");
        assert!(
            timeout(Duration::from_millis(25), committed_rx.recv())
                .await
                .is_err(),
            "raw durable ingress must not notify committed subscribers"
        );

        hub.publish_committed(AgentDurableEvent::TurnCompleted {
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            recovery: None,
        });
        assert!(matches!(
            timeout(Duration::from_secs(1), committed_rx.recv()).await,
            Ok(Ok(AgentDurableEvent::TurnCompleted { turn_id, .. })) if turn_id == "turn_1"
        ));
    }
}
