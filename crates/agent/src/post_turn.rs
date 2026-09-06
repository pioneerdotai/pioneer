//! Shared durable post-turn hook preparation for native and external runtimes.
//!
//! Adapters provide the completed turn and its actor/ownership context. Hook
//! snapshots, payload bounds, acceptance gates and execution stay shared.

use pioneer_hooks::{TurnPostTurnToolErrorClass, TurnPostTurnToolOutcomeStatus};
use pioneer_protocol::{ToolErrorClass, ToolOutcomeStatus};

pub fn post_turn_tool_outcome_status(status: ToolOutcomeStatus) -> TurnPostTurnToolOutcomeStatus {
    match status {
        ToolOutcomeStatus::Ok => TurnPostTurnToolOutcomeStatus::Ok,
        ToolOutcomeStatus::RecoverableError => TurnPostTurnToolOutcomeStatus::RecoverableError,
        ToolOutcomeStatus::FatalError => TurnPostTurnToolOutcomeStatus::FatalError,
        ToolOutcomeStatus::PartialSuccess => TurnPostTurnToolOutcomeStatus::PartialSuccess,
    }
}

pub fn post_turn_tool_error_class(error_class: ToolErrorClass) -> TurnPostTurnToolErrorClass {
    match error_class {
        ToolErrorClass::InvalidArguments => TurnPostTurnToolErrorClass::InvalidArguments,
        ToolErrorClass::NotFound => TurnPostTurnToolErrorClass::NotFound,
        ToolErrorClass::ToolNotVisible => TurnPostTurnToolErrorClass::ToolNotVisible,
        ToolErrorClass::PermissionDenied => TurnPostTurnToolErrorClass::PermissionDenied,
        ToolErrorClass::CommandNotFound => TurnPostTurnToolErrorClass::CommandNotFound,
        ToolErrorClass::Timeout => TurnPostTurnToolErrorClass::Timeout,
        ToolErrorClass::Cancelled => TurnPostTurnToolErrorClass::Cancelled,
        ToolErrorClass::ExecutionFailed => TurnPostTurnToolErrorClass::ExecutionFailed,
        ToolErrorClass::NeedsNarrowing => TurnPostTurnToolErrorClass::NeedsNarrowing,
        ToolErrorClass::Internal => TurnPostTurnToolErrorClass::Internal,
        ToolErrorClass::OutputTruncated => TurnPostTurnToolErrorClass::OutputTruncated,
        ToolErrorClass::Unknown => TurnPostTurnToolErrorClass::Unknown,
    }
}

use crate::hooks::{
    AgentTurnHookContext, AgentTurnPostTurnHookDispatch, DurablePostTurnHookRuntimeSnapshot,
    EffectiveTurnPromptContextSet, build_post_turn_phase_request, run_agent_turn_policy_hook_phase,
};
use crate::{AgentManager, AgentTurnHookRuntimeContext};
use pioneer_hooks::{
    HookPhase, HookPhaseRequest, HookRuntime, TurnPostTurnHookInput, TurnPrePolicyHookInput,
};
use pioneer_protocol::{
    NativeTerminalEffectGate, NativeTerminalEffectKind, NativeTerminalEffectPayload,
    NativeTerminalEffectPreparation, NativeTerminalEffectPreparationFailure,
    NativeTerminalEffectSpec,
};
use std::sync::Arc;

/// The adapter must provide actual turn input/output, not a provider prompt
/// containing injected instructions or recalled conversation history.
pub struct CompletedTurnHookInput {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub runtime_context: AgentTurnHookRuntimeContext,
    pub input: TurnPostTurnHookInput,
}

pub(crate) fn prepare_dispatch_effect(
    turn_id: &str,
    runtime: &HookRuntime,
    dispatch: AgentTurnPostTurnHookDispatch,
) -> NativeTerminalEffectSpec {
    let gate = if dispatch.awaits_task_result_acceptance() {
        NativeTerminalEffectGate::AcceptedTaskResult
    } else {
        NativeTerminalEffectGate::TerminalCommit
    };
    prepare_effect(
        turn_id,
        runtime,
        gate,
        dispatch.into_durable_phase_request(),
    )
}

fn prepare_effect(
    turn_id: &str,
    runtime: &HookRuntime,
    gate: NativeTerminalEffectGate,
    request: Result<HookPhaseRequest, pioneer_hooks::HookIdError>,
) -> NativeTerminalEffectSpec {
    // Leave envelope headroom below CRUD's 256 KiB admission limit.
    const MAX_HOOK_EFFECT_PAYLOAD_BYTES: usize = 255 * 1024;
    let payload = (|| -> Result<_, NativeTerminalEffectPreparationFailure> {
        let request = serde_json::to_value(
            request.map_err(|_| NativeTerminalEffectPreparationFailure::InvalidHookRequest)?,
        )
        .map_err(|_| NativeTerminalEffectPreparationFailure::InvalidHookRequest)?;
        let subscriptions = runtime
            .subscriptions()
            .subscriptions_for_phase(HookPhase::TurnPostTurn)
            .map_err(|_| NativeTerminalEffectPreparationFailure::SubscriptionSnapshotUnavailable)?;
        let snapshot = DurablePostTurnHookRuntimeSnapshot::capture(runtime, subscriptions)
            .map_err(|_| NativeTerminalEffectPreparationFailure::HandlerSnapshotUnavailable)?;
        let runtime_snapshot = serde_json::to_value(snapshot)
            .map_err(|_| NativeTerminalEffectPreparationFailure::SnapshotSerializationFailed)?;
        let payload = NativeTerminalEffectPayload::PostTurnHook {
            request,
            runtime_snapshot,
        };
        let encoded = serde_json::to_vec(&payload)
            .map_err(|_| NativeTerminalEffectPreparationFailure::PayloadSerializationFailed)?;
        if encoded.len() > MAX_HOOK_EFFECT_PAYLOAD_BYTES {
            return Err(NativeTerminalEffectPreparationFailure::PayloadTooLarge);
        }
        Ok(payload)
    })();
    NativeTerminalEffectSpec {
        effect_id: format!("{turn_id}:terminal-effect:post-turn"),
        effect_kind: NativeTerminalEffectKind::PostTurnHook,
        gate,
        payload: payload.unwrap_or_else(|failure| {
            NativeTerminalEffectPayload::PostTurnHookPreparationFailed { failure }
        }),
        max_attempts: 5,
    }
}

impl AgentManager {
    /// Capture before adapter I/O so configuration cannot change halfway through
    /// preparation. No adapter transcript/authorization reads are needed when
    /// the runtime has no post-turn subscribers.
    pub async fn capture_post_turn_runtime(&self) -> Result<Option<PostTurnHookRuntime>, String> {
        let state = self.runtime_dependencies.state.read().await;
        let Some(runtime) = state.hook_runtime.clone() else {
            return Ok(None);
        };
        if runtime
            .subscriptions()
            .subscriptions_for_phase(HookPhase::TurnPostTurn)
            .map_err(|error| error.to_string())?
            .is_empty()
        {
            return Ok(None);
        }
        Ok(Some(PostTurnHookRuntime {
            runtime,
            generation: state.generation,
            dispatch_policy: state.post_turn_hook_dispatch_policy,
        }))
    }
}

pub struct PostTurnHookRuntime {
    runtime: Arc<HookRuntime>,
    generation: u64,
    dispatch_policy: crate::AgentPostTurnHookDispatchPolicy,
}

impl PostTurnHookRuntime {
    /// Prepare a terminal obligation without running extraction or writing
    /// memory. The caller persists it BEFORE the canonical terminal commit.
    /// Replays must reuse the persisted obligation, not prepare a new snapshot.
    pub async fn prepare_completed_turn_hook(
        &self,
        turn: CompletedTurnHookInput,
    ) -> Result<Option<NativeTerminalEffectPreparation>, String> {
        let Self {
            runtime,
            generation,
            dispatch_policy,
        } = self;
        if !dispatch_policy.should_dispatch(turn.input.status) {
            return Ok(None);
        }
        let context = AgentTurnHookContext::with_runtime_context(
            &turn.workspace_id,
            &turn.thread_id,
            &turn.turn_id,
            turn.runtime_context,
        );
        let policies = run_agent_turn_policy_hook_phase(
            Some(runtime),
            &context,
            TurnPrePolicyHookInput::from_parts(
                turn.input
                    .user_text
                    .as_ref()
                    .map(|text| text.text.clone())
                    .unwrap_or_default(),
                turn.input.model.clone(),
                turn.input.model_provider.clone(),
            ),
        )
        .await
        .map_err(|error| error.safe_message().to_owned())?;
        let (gate, request) = build_post_turn_phase_request(
            context,
            &policies,
            &EffectiveTurnPromptContextSet::empty(),
            turn.input,
        );
        let effect = prepare_effect(&turn.turn_id, runtime, gate, request);
        Ok(Some(NativeTerminalEffectPreparation {
            batch_id: format!("{}:terminal-effects:external:{generation}", turn.turn_id),
            workspace_id: turn.workspace_id,
            thread_id: turn.thread_id,
            turn_id: turn.turn_id,
            runtime_generation: *generation,
            effects: vec![effect],
        }))
    }
}
