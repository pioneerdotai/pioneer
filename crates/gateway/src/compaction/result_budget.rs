//! N3: shrink saved textual results to the room left in a complete request.
//! Calls and source identities remain intact; this is not summary coverage.
use anyhow::Result;
use pioneer_agent::compaction::request::{
    EvaluatedRequest, MediaEstimate, NativeRequestProjection,
};
use pioneer_compaction::{ModelBudget, SourceRef};
use pioneer_crud::CrudStore;
use pioneer_provider::Role;

pub(super) async fn shrink_results(
    store: &CrudStore,
    workspace: &str,
    full: &EvaluatedRequest,
    media: &[MediaEstimate],
    budget: &ModelBudget,
    recovery: bool,
) -> Result<Option<EvaluatedRequest>> {
    let candidates: Vec<_> = full
        .request
        .messages
        .iter()
        .enumerate()
        .filter_map(|(i, m)| {
            // Typed media keeps its original indexing and accounting. Reduce text
            // only when a trusted, exact durable locator can be recovered below.
            (m.role == Role::Tool && m.content_parts.is_empty() && m.provenance.is_some())
                .then_some(i)
        })
        .collect();
    if candidates.is_empty() {
        return Ok(None);
    }
    let result_tokens = candidates.iter().fold(0_u64, |sum, i| {
        sum.saturating_add(full.message_input_tokens[*i])
    });
    let fixed = full.estimated_input_tokens.saturating_sub(result_tokens);
    let window = if recovery {
        (budget.context as u128 * 9 / 10) as u64
    } else {
        budget.context
    };
    let input_limit = window
        .saturating_sub(full.output_reserve)
        .saturating_sub(budget.separate_reasoning)
        .min(budget.input_limit.unwrap_or(u64::MAX));
    let raw_limit = (input_limit as u128 * 100 / 105) as u64;
    // Keep enough space for an ordinary scoped reference. If even these small
    // results cannot fit, the existing whole-round compaction planner takes over.
    let per_result = (raw_limit.saturating_sub(fixed).saturating_sub(64) / candidates.len() as u64)
        .clamp(512, pioneer_compaction::RESULT_TOKENS);
    let mut request = full.request.clone();
    let mut changed = false;
    for index in candidates {
        if full.message_input_tokens[index] <= per_result {
            continue;
        }
        let message = &request.messages[index];
        let origin = message.provenance.as_ref().unwrap();
        if origin.workspace_id != workspace || origin.sources.len() != 1 {
            continue;
        }
        let source = &origin.sources[0];
        let Some((kind, turn)) = source.scope.split_once(':') else {
            continue;
        };
        if !matches!(kind, "context" | "item") {
            continue;
        }
        let source = SourceRef {
            scope: source.scope.clone(),
            id: source.id.clone(),
            version: source.version.clone(),
        };
        let Some(item) = store
            .compaction_replay_item_id(workspace, &origin.thread_id, &source)
            .await?
        else {
            continue;
        };
        let reference = serde_json::json!({"workspace_id":workspace,"thread_id":origin.thread_id,"turn_id":turn,"item_id":item}).to_string();
        // An oversized reference/framing remains available to normal context
        // preparation; it must not block the turn before compaction can run.
        if let Ok(reduced) = pioneer_agent::compaction::restored_tool_result_with_budget(
            message, &reference, per_result,
        ) {
            if reduced.content != message.content {
                request.messages[index] = reduced;
                changed = true;
            }
        }
    }
    if !changed {
        return Ok(None);
    }
    Ok(Some(NativeRequestProjection::full(
        request,
        media.to_vec(),
        budget.clone(),
        recovery,
    )?))
}
