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
    #[serde(deserialize_with = "required_wait")]
    pub attached_task_wait: Option<pioneer_protocol::NativeAttachedTaskWait>,
    pub controls: super::native_control::NativeControlState,
    pub provider_history_roots: Vec<pioneer_compaction::SourceRef>,
    pub no_progress_mode: pioneer_tools::ToolNoProgressMode,
    pub progress_frontier: pioneer_tools::ToolProgressFrontier,
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

fn required_wait<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<pioneer_protocol::NativeAttachedTaskWait>, D::Error> {
    Option::<pioneer_protocol::NativeAttachedTaskWait>::deserialize(deserializer)
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
    mode: pioneer_tools::ToolNoProgressMode,
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
        .is_some_and(|state| {
            state.no_progress_mode == mode
                && payload.window.progress.is_some_and(|progress| {
                    progress.valid()
                        && progress.frontier == state.progress_frontier.observation_counts()
                })
                && state.validate(&payload.turn_id, budget).is_ok()
                && pioneer_tools::ToolNoProgressGuard::checkpoint_progress_compatible(
                    &payload.tool_no_progress,
                    &state.progress_frontier,
                )
        })
}

pub(super) fn history_roots_valid(roots: &[pioneer_compaction::SourceRef]) -> bool {
    roots.len() <= 64
        && roots.iter().enumerate().all(|(index, root)| {
            root.scope.starts_with("checkpoint:")
                && root.scope.len() > "checkpoint:".len()
                && root.scope.len() <= 512
                && !root.id.is_empty()
                && root.id.len() <= pioneer_compaction::CHECKPOINT_ID_MAX_BYTES
                && !root.version.is_empty()
                && root.version.len() <= 256
                && !roots[..index].contains(root)
        })
}

pub(super) fn history_roots_from_request(
    messages: &[pioneer_provider::ChatMessage],
) -> Result<Vec<pioneer_compaction::SourceRef>, ChatTurnError> {
    let mut roots = Vec::new();
    for source in messages
        .iter()
        .filter_map(|message| message.provenance.as_ref())
        .flat_map(|origin| &origin.sources)
        .filter(|source| source.scope.starts_with("checkpoint:"))
    {
        if source.scope.len() > 512
            || source.id.is_empty()
            || source.id.len() > pioneer_compaction::CHECKPOINT_ID_MAX_BYTES
            || source.version.is_empty()
            || source.version.len() > 256
        {
            return Err(unavailable());
        }
        let root = pioneer_compaction::SourceRef {
            scope: source.scope.clone(),
            id: source.id.clone(),
            version: source.version.clone(),
        };
        if !roots.contains(&root) {
            if roots.len() >= 64 {
                return Err(unavailable());
            }
            roots.push(root);
        }
    }
    if !history_roots_valid(&roots) {
        return Err(unavailable());
    }
    Ok(roots)
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
        mode: pioneer_tools::ToolNoProgressMode,
    ) -> Result<Self, ChatTurnError> {
        let Some(context) = context else {
            if window_index != 1 || !new_turn {
                return Err(unavailable());
            }
            return Ok(Self {
                schema_version: crate::NATIVE_SEMANTIC_STATE_SCHEMA_VERSION,
                controls: Default::default(),
                provider_history_roots: Vec::new(),
                no_progress_mode: mode,
                progress_frontier: Default::default(),
                checkpoint_revision: 0,
                attached_task_wait: None,
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
        if state.no_progress_mode != mode
            || !payload.window.progress.is_some_and(|progress| {
                progress.valid()
                    && progress.frontier == state.progress_frontier.observation_counts()
            })
            || !pioneer_tools::ToolNoProgressGuard::checkpoint_progress_compatible(
                &payload.tool_no_progress,
                &state.progress_frontier,
            )
        {
            return Err(unavailable());
        }
        Ok(state)
    }

    pub(super) fn to_value(&self) -> Result<serde_json::Value, ChatTurnError> {
        serde_json::from_slice(&encode(self)?).map_err(|_| unavailable())
    }

    fn validate(&self, turn: &str, budget: &ToolRetryBudgetConfig) -> Result<(), ChatTurnError> {
        if self.schema_version != crate::NATIVE_SEMANTIC_STATE_SCHEMA_VERSION
            || self.attached_task_wait.as_ref().is_some_and(|wait| {
                !wait.valid()
                    || wait.checkpoint_id != pioneer_protocol::native_semantic_checkpoint_id(turn)
            })
            || !self.progress_frontier.valid()
            || !self.controls.valid()
            || !history_roots_valid(&self.provider_history_roots)
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
                .any(|signature| {
                    signature.len() != 64 || !signature.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
            || self
                .observed_terminal_task_ids
                .iter()
                .any(|id| id.is_empty() || id.len() > 4096)
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
    attached_task_wait: &Option<pioneer_protocol::NativeAttachedTaskWait>,
    turn: &str,
    budget: &ToolRetryBudgetConfig,
    controls: &super::native_control::NativeControlState,
    history_roots: &[pioneer_compaction::SourceRef],
    mode: pioneer_tools::ToolNoProgressMode,
    progress_frontier: &pioneer_tools::ToolProgressFrontier,
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
        attached_task_wait: &'a Option<pioneer_protocol::NativeAttachedTaskWait>,
        controls: &'a super::native_control::NativeControlState,
        provider_history_roots: &'a [pioneer_compaction::SourceRef],
        no_progress_mode: pioneer_tools::ToolNoProgressMode,
        progress_frontier: &'a pioneer_tools::ToolProgressFrontier,
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
        schema_version: crate::NATIVE_SEMANTIC_STATE_SCHEMA_VERSION,
        checkpoint_revision,
        attached_task_wait,
        controls,
        provider_history_roots: history_roots,
        no_progress_mode: mode,
        progress_frontier,
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

#[cfg(test)]
mod history_root_tests {
    use super::*;

    #[test]
    fn compound_summary_roots_roundtrip_and_remain_bounded() {
        let root = pioneer_compaction::SourceRef {
            scope: "checkpoint:native:workspace:thread".into(),
            id: format!("{}:summary:{}", "o".repeat(21), u32::MAX),
            version: "a".repeat(64),
        };
        assert!(root.id.len() > 21);
        let decoded = serde_json::from_slice::<Vec<pioneer_compaction::SourceRef>>(
            &encode(&vec![root.clone()]).unwrap(),
        )
        .unwrap();
        assert!(history_roots_valid(&decoded));
        let mut oversized = root.clone();
        oversized.id = "x".repeat(pioneer_compaction::CHECKPOINT_ID_MAX_BYTES + 1);
        assert!(!history_roots_valid(&[oversized]));
        assert!(!history_roots_valid(&[root.clone(), root]));
    }
}
