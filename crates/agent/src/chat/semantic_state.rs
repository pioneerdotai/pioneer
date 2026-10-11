//! Mutable state belongs to the semantic Turn, not to a provider window.
//! The existing execution checkpoint is the durable owner of this projection.
use super::tool_retry_lifecycle::{ToolRetryLifecycleCheckpoint, ToolRetryLifecycleTracker};
use super::{ChatTurnError, ExecutionCheckpointContext, TaskMutationFinalizationGuard};
use pioneer_hooks::{TurnPostTurnDomainEventSummary, TurnPostTurnToolEventSummary};
use pioneer_tools::{ToolRetryBudgetConfig, ToolRetryCheckpoint, ToolRetryController};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const MAX_BYTES: usize = 64 * 1024;
const MAX_MARKERS: usize = 256;
pub(super) const MAX_SUMMARY_EVENTS: usize = 256;
pub(super) const MAX_SUMMARY_TEXT_BYTES: usize = 16 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeSemanticState {
    pub schema_version: u32,
    pub checkpoint_revision: u64,
    pub retry: ToolRetryCheckpoint,
    pub retry_lifecycle: ToolRetryLifecycleCheckpoint,
    pub pending_retry_instruction: Option<String>,
    pub applied_retry_instruction: Option<String>,
    pub observed_review_required_signatures: BTreeSet<String>,
    pub observed_terminal_task_ids: BTreeSet<String>,
    pub task_mutation_guard: TaskMutationFinalizationGuard,
    pub post_turn_assistant_text: String,
    pub post_turn_tool_events: Vec<TurnPostTurnToolEventSummary>,
    pub post_turn_domain_events: Vec<TurnPostTurnDomainEventSummary>,
}

/// Serialize borrowed state with a physical byte ceiling before cloning any
/// checkpoint JSON. Failure is explicit; it never drops an obligation/counter.
fn encode<T: Serialize>(state: &T) -> Result<Vec<u8>, ChatTurnError> {
    struct Writer(Vec<u8>);
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self
                .0
                .len()
                .checked_add(bytes.len())
                .is_none_or(|n| n > MAX_BYTES)
            {
                return Err(std::io::Error::other("semantic checkpoint byte limit"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer(Vec::new());
    serde_json::to_writer(&mut writer, state).map_err(|_| unavailable())?;
    Ok(writer.0)
}

fn unavailable() -> ChatTurnError {
    ChatTurnError::Blocked("native semantic checkpoint is unavailable or incompatible".to_owned())
}

pub(crate) fn checkpoint_compatible(
    payload: &pioneer_protocol::ExecutionCheckpointPayload,
    budget: &ToolRetryBudgetConfig,
) -> bool {
    if payload.schema_version != pioneer_protocol::EXECUTION_CHECKPOINT_PAYLOAD_SCHEMA_VERSION {
        return false;
    }
    if payload
        .native_event_frontier
        .as_ref()
        .is_some_and(|frontier| {
            frontier.schema_version != 1
                || frontier.event_sequence < 0
                || frontier.context_sequence < -1
        })
    {
        return false;
    }
    let Some(value) = payload.native_semantic_state.as_ref() else {
        return false;
    };
    encode(value)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<NativeSemanticState>(&bytes).ok())
        .is_some_and(|state| state.validate(&payload.turn_id, budget).is_ok())
}

impl NativeSemanticState {
    pub(super) fn restore(
        context: Option<&ExecutionCheckpointContext>,
        workspace: &str,
        thread: &str,
        turn: &str,
        window_index: u32,
        budget: &ToolRetryBudgetConfig,
        new_turn: bool,
    ) -> Result<Self, ChatTurnError> {
        let Some(context) = context else {
            if window_index != 1 || !new_turn {
                return Err(unavailable());
            }
            return Ok(Self {
                schema_version: 2,
                checkpoint_revision: 0,
                retry: ToolRetryController::new(budget.clone())
                    .checkpoint()
                    .map_err(|_| unavailable())?,
                retry_lifecycle: ToolRetryLifecycleTracker::default().checkpoint(),
                pending_retry_instruction: None,
                applied_retry_instruction: None,
                observed_review_required_signatures: BTreeSet::new(),
                observed_terminal_task_ids: BTreeSet::new(),
                task_mutation_guard: TaskMutationFinalizationGuard::default(),
                post_turn_assistant_text: String::new(),
                post_turn_tool_events: Vec::new(),
                post_turn_domain_events: Vec::new(),
            });
        };
        let payload = &context.payload;
        if payload.schema_version != pioneer_protocol::EXECUTION_CHECKPOINT_PAYLOAD_SCHEMA_VERSION
            || payload.workspace_id != workspace
            || payload.thread_id != thread
            || payload.turn_id != turn
            || context.window_index != payload.window.window_index
            || payload.window.window_index > window_index
            || payload
                .native_event_frontier
                .as_ref()
                .is_some_and(|frontier| {
                    frontier.schema_version != 1
                        || frontier.event_sequence < 0
                        || frontier.context_sequence < -1
                })
        {
            return Err(unavailable());
        }
        let value = payload
            .native_semantic_state
            .as_ref()
            .ok_or_else(unavailable)?;
        let state: Self = serde_json::from_slice(&encode(value)?).map_err(|_| unavailable())?;
        state.validate(turn, budget)?;
        Ok(state)
    }

    pub(super) fn to_value(&self) -> Result<serde_json::Value, ChatTurnError> {
        serde_json::from_slice(&encode(self)?).map_err(|_| unavailable())
    }

    fn validate(&self, turn: &str, budget: &ToolRetryBudgetConfig) -> Result<(), ChatTurnError> {
        if self.schema_version != 2
            || self
                .pending_retry_instruction
                .as_ref()
                .is_some_and(|s| s.len() > MAX_SUMMARY_TEXT_BYTES)
            || self
                .applied_retry_instruction
                .as_ref()
                .is_some_and(|s| s.len() > MAX_SUMMARY_TEXT_BYTES)
            || self.observed_review_required_signatures.len() > MAX_MARKERS
            || self.observed_terminal_task_ids.len() > MAX_MARKERS
            || self
                .observed_review_required_signatures
                .iter()
                .chain(&self.observed_terminal_task_ids)
                .any(|s| s.is_empty() || s.len() > 4096)
            || !self.task_mutation_guard.checkpoint_valid()
            || self.post_turn_assistant_text.len() > MAX_SUMMARY_TEXT_BYTES
            || self.post_turn_tool_events.len() > MAX_SUMMARY_EVENTS
            || self.post_turn_domain_events.len() > MAX_SUMMARY_EVENTS
        {
            return Err(unavailable());
        }
        ToolRetryController::from_checkpoint(budget.clone(), self.retry.clone())
            .map_err(|_| unavailable())?;
        ToolRetryLifecycleTracker::from_checkpoint(
            turn,
            self.retry_lifecycle.clone(),
            self.retry.active_episode.is_some(),
        )
        .map_err(|_| unavailable())?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn capture(
    checkpoint_revision: u64,
    turn: &str,
    budget: &ToolRetryBudgetConfig,
    controller: &ToolRetryController,
    lifecycle: &ToolRetryLifecycleTracker,
    pending: &Option<String>,
    applied: &Option<String>,
    reviews: &BTreeSet<String>,
    terminal: &BTreeSet<String>,
    task: &TaskMutationFinalizationGuard,
    text: &str,
    tools: &[TurnPostTurnToolEventSummary],
    domains: &[TurnPostTurnDomainEventSummary],
) -> Result<serde_json::Value, ChatTurnError> {
    #[derive(Serialize)]
    struct Borrowed<'a> {
        schema_version: u32,
        checkpoint_revision: u64,
        retry: ToolRetryCheckpoint,
        retry_lifecycle: ToolRetryLifecycleCheckpoint,
        pending_retry_instruction: &'a Option<String>,
        applied_retry_instruction: &'a Option<String>,
        observed_review_required_signatures: &'a BTreeSet<String>,
        observed_terminal_task_ids: &'a BTreeSet<String>,
        task_mutation_guard: &'a TaskMutationFinalizationGuard,
        post_turn_assistant_text: &'a str,
        post_turn_tool_events: &'a [TurnPostTurnToolEventSummary],
        post_turn_domain_events: &'a [TurnPostTurnDomainEventSummary],
    }
    let encoded = encode(&Borrowed {
        schema_version: 2,
        checkpoint_revision,
        retry: controller.checkpoint().map_err(|_| unavailable())?,
        retry_lifecycle: lifecycle.checkpoint(),
        pending_retry_instruction: pending,
        applied_retry_instruction: applied,
        observed_review_required_signatures: reviews,
        observed_terminal_task_ids: terminal,
        task_mutation_guard: task,
        post_turn_assistant_text: text,
        post_turn_tool_events: tools,
        post_turn_domain_events: domains,
    })?;
    let state: NativeSemanticState = serde_json::from_slice(&encoded).map_err(|_| unavailable())?;
    state.validate(turn, budget)?;
    serde_json::from_slice(&encoded).map_err(|_| unavailable())
}
