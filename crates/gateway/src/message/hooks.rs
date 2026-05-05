use super::now_timestamp_secs;
use pioneer_hooks::{
    HookActor, HookActorId, HookActorKind, HookCompactionId, HookContext, HookContextMode,
    HookDiagnostic, HookInput, HookPhase, HookPhaseRequest, HookRunSummary, HookRuntime,
    HookRuntimeError, HookThreadId, HookTurnId, HookWorkspaceId, TurnPreCompactionHookInput,
    TurnPreCompactionHookInputLimits, TurnPreCompactionRawTurnRetention,
    TurnPreCompactionRetentionPolicy, TurnPreCompactionSourceKind, TurnPreCompactionSourceRange,
    TurnPreCompactionSummaryPolicy, TurnPreCompactionSummaryStorage,
    TurnPreCompactionSummaryStrategy, TurnPreCompactionTokenBudget, TurnPreCompactionTrigger,
};
use std::sync::Arc;
use tracing::{debug, warn};

pub(super) struct PreCompactionHookDispatch {
    pub context: HookContext,
    pub input: TurnPreCompactionHookInput,
}

pub(super) struct PreCompactionHookInputParts<'a> {
    pub workspace_id: &'a str,
    pub thread_id: &'a str,
    pub turn_id: &'a str,
    pub compaction_id: String,
    pub loaded_completed_turn_count: usize,
    pub source_entry_count: usize,
    pub max_loaded_turns: usize,
    pub existing_summary_turn_count: Option<i64>,
    pub max_context_tokens: usize,
    pub response_reserve_tokens: usize,
    pub history_budget_tokens: usize,
    pub estimated_current_tokens: usize,
    pub compression_threshold_tokens: usize,
    pub target_summary_tokens: usize,
    pub compression_threshold_bps: u16,
    pub compression_target_bps: u16,
    pub existing_summary: Option<&'a str>,
}

pub(super) struct PreCompactionHookOutcome {
    pub diagnostics: Vec<HookDiagnostic>,
    pub runs: Vec<HookRunSummary>,
}

pub(super) struct PreCompactionHookError {
    pub safe_message: String,
    pub runtime_error: HookRuntimeError,
}

pub(super) async fn run_pre_compaction_hook_phase(
    runtime: Option<&Arc<HookRuntime>>,
    dispatch: PreCompactionHookDispatch,
) -> Result<PreCompactionHookOutcome, PreCompactionHookError> {
    let Some(runtime) = runtime else {
        return Ok(PreCompactionHookOutcome {
            diagnostics: Vec::new(),
            runs: Vec::new(),
        });
    };

    let request = HookPhaseRequest::new(
        HookPhase::TurnPreCompaction,
        dispatch.context,
        HookInput::turn_pre_compaction(dispatch.input),
    );

    match runtime.run_phase(request).await {
        Ok(response) => {
            for diagnostic in &response.diagnostics {
                warn!(
                    code = diagnostic.code.as_str(),
                    message = diagnostic.message.as_str(),
                    severity = ?diagnostic.severity,
                    "pre-compaction hook diagnostic"
                );
            }
            if !response.contributions.is_empty() {
                debug!(
                    contribution_count = response.contributions.len(),
                    "ignoring pre-compaction hook contributions"
                );
            }
            Ok(PreCompactionHookOutcome {
                diagnostics: response.diagnostics,
                runs: response.runs,
            })
        }
        Err(runtime_error) => Err(PreCompactionHookError {
            safe_message: "pre-compaction hook phase failed".to_owned(),
            runtime_error,
        }),
    }
}

pub(super) fn build_pre_compaction_hook_dispatch(
    parts: PreCompactionHookInputParts<'_>,
) -> Result<PreCompactionHookDispatch, pioneer_hooks::HookIdError> {
    let context =
        gateway_compaction_hook_context(parts.workspace_id, parts.thread_id, Some(parts.turn_id))?;
    let input = TurnPreCompactionHookInput::from_parts(
        HookWorkspaceId::new(parts.workspace_id.to_owned())?,
        HookThreadId::new(parts.thread_id.to_owned())?,
        Some(HookTurnId::new(parts.turn_id.to_owned())?),
        HookCompactionId::new(parts.compaction_id)?,
        TurnPreCompactionTrigger::ContextBudgetThreshold,
        TurnPreCompactionSourceRange {
            source_kind: TurnPreCompactionSourceKind::ConversationHistory,
            loaded_completed_turn_count: parts.loaded_completed_turn_count,
            source_entry_count: parts.source_entry_count,
            max_loaded_turns: parts.max_loaded_turns,
            existing_summary_turn_count: parts.existing_summary_turn_count,
        },
        TurnPreCompactionTokenBudget {
            max_context_tokens: parts.max_context_tokens,
            response_reserve_tokens: parts.response_reserve_tokens,
            history_budget_tokens: parts.history_budget_tokens,
            estimated_current_tokens: parts.estimated_current_tokens,
            compression_threshold_tokens: parts.compression_threshold_tokens,
            target_summary_tokens: parts.target_summary_tokens,
        },
        TurnPreCompactionSummaryPolicy {
            strategy: TurnPreCompactionSummaryStrategy::ProgressiveFullHistorySummary,
            compression_threshold_bps: parts.compression_threshold_bps,
            compression_target_bps: parts.compression_target_bps,
        },
        TurnPreCompactionRetentionPolicy {
            raw_turn_retention: TurnPreCompactionRawTurnRetention::RetainOriginalTurns,
            summary_storage: TurnPreCompactionSummaryStorage::ThreadSummary,
        },
        parts.existing_summary,
        TurnPreCompactionHookInputLimits::default(),
    );
    Ok(PreCompactionHookDispatch { context, input })
}

fn gateway_compaction_hook_context(
    workspace_id: &str,
    thread_id: &str,
    turn_id: Option<&str>,
) -> Result<HookContext, pioneer_hooks::HookIdError> {
    Ok(HookContext {
        workspace_id: Some(HookWorkspaceId::new(workspace_id.to_owned())?),
        thread_id: Some(HookThreadId::new(thread_id.to_owned())?),
        turn_id: turn_id
            .map(|turn_id| HookTurnId::new(turn_id.to_owned()))
            .transpose()?,
        mode: Some(HookContextMode::System),
        actor: Some(HookActor {
            kind: HookActorKind::Service,
            id: Some(HookActorId::new("gateway_context_compaction")?),
        }),
        now_unix: Some(now_timestamp_secs()),
        ..HookContext::default()
    })
}
