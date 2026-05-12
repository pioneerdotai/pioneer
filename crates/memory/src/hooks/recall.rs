use super::*;

pub(super) fn memory_turn_context_from_prompt_context_request(
    request: &HookHandlerRequest,
    input: &TurnPrePromptContextHookInput,
) -> HookResult<MemoryTurnContext> {
    let workspace_id = required_context_id(
        request.context.workspace_id.as_ref().map(|id| id.as_str()),
        "workspace_id",
    )?;
    let thread_id = required_context_id(
        request.context.thread_id.as_ref().map(|id| id.as_str()),
        "thread_id",
    )?;
    let turn_id = required_context_id(
        request.context.turn_id.as_ref().map(|id| id.as_str()),
        "turn_id",
    )?;
    Ok(MemoryTurnContext {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        mode: ThreadMode::Agent,
        input_text: input.input_text.clone(),
        task_id: request
            .context
            .task_id
            .as_ref()
            .map(|id| id.as_str().to_owned()),
        agent_id: request
            .context
            .agent_id
            .as_ref()
            .map(|id| id.as_str().to_owned()),
    })
}

pub(super) fn memory_recall_request(input_text: &str) -> MemoryRecallRequest {
    MemoryRecallRequest {
        query: input_text.to_owned(),
        categories: Vec::new(),
        top_k: Some(5),
        max_chars: Some(1_500),
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct DeterministicRecallContextSummary {
    pub(super) memory_ids: BTreeSet<String>,
    pub(super) rendered_line_fingerprints: BTreeSet<String>,
    pub(super) context_count: usize,
    pub(super) context_chars: usize,
    pub(super) sufficient: bool,
}

pub(super) fn deterministic_recall_context_summary(
    prompt_context_set: &pioneer_hooks::HookPromptContextSet,
    config: &MemoryActiveRecallConfig,
) -> DeterministicRecallContextSummary {
    let mut summary = DeterministicRecallContextSummary::default();
    for entry in prompt_context_set.entries() {
        if entry.domain.as_str() != MEMORY_POLICY_DOMAIN
            || entry.contribution_id.as_str() != MEMORY_DETERMINISTIC_RECALL_CONTRIBUTION_ID
        {
            continue;
        }
        summary.context_count += 1;
        summary.context_chars += entry.content.as_str().chars().count();
        summary
            .rendered_line_fingerprints
            .extend(rendered_line_fingerprints(entry.content.as_str()));
        for source_ref in &entry.source_refs {
            if source_ref.kind.as_str() == "memory" {
                summary.memory_ids.insert(source_ref.id.as_str().to_owned());
            }
        }
    }
    summary.sufficient = !summary.memory_ids.is_empty()
        && (summary.memory_ids.len() >= config.deterministic_sufficient_min_items
            || summary.context_chars >= config.deterministic_sufficient_min_chars);
    summary
}

pub(super) async fn resolve_active_memory_decision(
    provider: Option<&Arc<dyn AgentActiveMemoryDecisionProvider>>,
    context: &MemoryTurnContext,
    input: &TurnPrePromptContextHookInput,
    policy: &MemoryTurnPolicy,
    config: &MemoryActiveRecallConfig,
    deterministic: &DeterministicRecallContextSummary,
) -> ActiveMemoryDecision {
    if !policy.allow_pre_turn_recall()
        || policy.active_memory == MemoryActiveContextPolicy::Disabled
    {
        return ActiveMemoryDecision {
            status: ActiveMemoryDecisionStatus::Skip,
            reason_code: ActiveMemoryDecisionReasonCode::PolicyDisabled,
            confidence: policy.confidence,
            query_hints: Vec::new(),
            diagnostics: Vec::new(),
        };
    }
    match config.mode {
        MemoryActiveRecallMode::Disabled => {
            return ActiveMemoryDecision {
                status: ActiveMemoryDecisionStatus::Skip,
                reason_code: ActiveMemoryDecisionReasonCode::ConfigDisabled,
                confidence: 1.0,
                query_hints: Vec::new(),
                diagnostics: Vec::new(),
            };
        }
        MemoryActiveRecallMode::DeterministicOnly => {
            return ActiveMemoryDecision {
                status: ActiveMemoryDecisionStatus::Skip,
                reason_code: ActiveMemoryDecisionReasonCode::DeterministicOnly,
                confidence: 1.0,
                query_hints: Vec::new(),
                diagnostics: Vec::new(),
            };
        }
        MemoryActiveRecallMode::StrictDebug => {
            return ActiveMemoryDecision {
                status: ActiveMemoryDecisionStatus::Run,
                reason_code: ActiveMemoryDecisionReasonCode::StrictDebug,
                confidence: 1.0,
                query_hints: Vec::new(),
                diagnostics: Vec::new(),
            };
        }
        MemoryActiveRecallMode::Hybrid => {}
    }

    if deterministic.sufficient {
        return ActiveMemoryDecision {
            status: ActiveMemoryDecisionStatus::Skip,
            reason_code: ActiveMemoryDecisionReasonCode::DeterministicSufficient,
            confidence: 0.9,
            query_hints: Vec::new(),
            diagnostics: Vec::new(),
        };
    }

    if let Some(provider) = provider {
        let request = MemoryActiveRecallDecisionRequest {
            deterministic_context_count: deterministic.context_count,
            deterministic_context_chars: deterministic.context_chars,
            deterministic_memory_ids: deterministic.memory_ids.iter().cloned().collect(),
            config_mode: config.mode,
        };
        match provider
            .resolve_active_memory_decision_json(
                MemoryActiveRecallDecisionContext {
                    workspace_id: context.workspace_id.clone(),
                    thread_id: context.thread_id.clone(),
                    turn_id: context.turn_id.clone(),
                    mode: context.mode,
                    input_text_preview: truncate_chars(input.input_text.as_str(), 1_000),
                },
                request,
            )
            .await
        {
            Ok(json) => match parse_active_memory_decision_json(json.as_str()) {
                Ok(decision) => {
                    return decision;
                }
                Err(_) => {
                    return ActiveMemoryDecision {
                        status: ActiveMemoryDecisionStatus::Skip,
                        reason_code: ActiveMemoryDecisionReasonCode::ProviderUncertain,
                        confidence: 0.0,
                        query_hints: Vec::new(),
                        diagnostics: vec!["memory.active_recall.invalid_json".to_owned()],
                    };
                }
            },
            Err(_) => {
                return ActiveMemoryDecision {
                    status: ActiveMemoryDecisionStatus::Skip,
                    reason_code: ActiveMemoryDecisionReasonCode::ProviderUncertain,
                    confidence: 0.0,
                    query_hints: Vec::new(),
                    diagnostics: vec!["memory.active_recall.provider_failed".to_owned()],
                };
            }
        }
    }

    local_active_memory_decision(input.input_text.as_str(), "")
}
pub(super) fn memory_recall_prompt_context_contribution(
    recall_snapshot: MemoryRecallSnapshot,
) -> Option<PromptContextContribution> {
    if recall_snapshot.items.is_empty() {
        return None;
    }
    let truncated = recall_snapshot.truncated;
    let source_refs = memory_recall_source_refs(recall_snapshot.items.as_slice());
    let prompt_items = recall_snapshot
        .items
        .into_iter()
        .map(memory_recall_prompt_item)
        .collect::<Vec<_>>();
    let (content, truncated) =
        render_memory_recall_context_block(prompt_items.as_slice(), truncated);
    let content = HookPromptContent::new(content).ok()?;
    Some(PromptContextContribution {
        contribution_id: HookContributionId::new(MEMORY_DETERMINISTIC_RECALL_CONTRIBUTION_ID)
            .expect("static contribution id is valid"),
        domain: memory_policy_domain(),
        priority: 500,
        content,
        max_chars: Some(1_500),
        source_refs,
        diagnostics: Vec::new(),
        truncated,
    })
}

pub(super) fn memory_recall_source_refs(items: &[MemoryRecallItem]) -> Vec<HookSourceRef> {
    let mut seen = BTreeSet::new();
    items
        .iter()
        .filter_map(|item| {
            let memory_id = item.memory_id.trim();
            if memory_id.is_empty() || !seen.insert(memory_id.to_owned()) {
                return None;
            }
            Some(HookSourceRef {
                kind: HookSourceKind::Custom("memory".to_owned()),
                id: HookSourceId::new(memory_id.to_owned()).ok()?,
                label: None,
            })
        })
        .collect()
}
