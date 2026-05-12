use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ActiveMemoryDecisionStatus {
    Skip,
    Run,
    Uncertain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ActiveMemoryDecisionReasonCode {
    PolicyDisabled,
    ConfigDisabled,
    DeterministicOnly,
    DeterministicSufficient,
    TrivialSelfContained,
    MemoryLikely,
    StrictDebug,
    ProviderRun,
    ProviderSkip,
    ProviderUncertain,
}

impl ActiveMemoryDecisionReasonCode {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::PolicyDisabled => "policy_disabled",
            Self::ConfigDisabled => "config_disabled",
            Self::DeterministicOnly => "deterministic_only",
            Self::DeterministicSufficient => "deterministic_sufficient",
            Self::TrivialSelfContained => "trivial_self_contained",
            Self::MemoryLikely => "memory_likely",
            Self::StrictDebug => "strict_debug",
            Self::ProviderRun => "provider_run",
            Self::ProviderSkip => "provider_skip",
            Self::ProviderUncertain => "provider_uncertain",
        }
    }

    pub(super) fn diagnostic_code(self) -> &'static str {
        match self {
            Self::PolicyDisabled => "memory.active_recall.policy_disabled",
            Self::ConfigDisabled => "memory.active_recall.config_disabled",
            Self::DeterministicOnly => "memory.active_recall.deterministic_only",
            Self::DeterministicSufficient => "memory.active_recall.deterministic_sufficient",
            Self::TrivialSelfContained => "memory.active_recall.skipped",
            Self::MemoryLikely | Self::StrictDebug | Self::ProviderRun => {
                "memory.active_recall.started"
            }
            Self::ProviderSkip => "memory.active_recall.skipped",
            Self::ProviderUncertain => "memory.active_recall.uncertain",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ActiveMemoryDecision {
    pub(super) status: ActiveMemoryDecisionStatus,
    pub(super) reason_code: ActiveMemoryDecisionReasonCode,
    pub(super) confidence: f32,
    pub(super) query_hints: Vec<String>,
    pub(super) diagnostics: Vec<String>,
}

pub(super) fn local_active_memory_decision(
    input_text: &str,
    diagnostic: &str,
) -> ActiveMemoryDecision {
    let mut diagnostics = Vec::new();
    if !diagnostic.trim().is_empty() {
        diagnostics.push(diagnostic.to_owned());
    }
    if is_trivial_self_contained_turn(input_text) {
        return ActiveMemoryDecision {
            status: ActiveMemoryDecisionStatus::Skip,
            reason_code: ActiveMemoryDecisionReasonCode::TrivialSelfContained,
            confidence: 0.75,
            query_hints: Vec::new(),
            diagnostics,
        };
    }
    ActiveMemoryDecision {
        status: ActiveMemoryDecisionStatus::Run,
        reason_code: ActiveMemoryDecisionReasonCode::MemoryLikely,
        confidence: 0.65,
        query_hints: vec![MEMORY_ACTIVE_RECALL_GENERIC_QUERY.to_owned()],
        diagnostics,
    }
}

pub(super) fn active_memory_decision_observability_diagnostic(
    decision: &ActiveMemoryDecision,
    deterministic: &DeterministicRecallContextSummary,
) -> HookDiagnostic {
    memory_safe_info_diagnostic(
        "memory.active_recall.decision",
        format!(
            "memory active recall decision: status={} reason={} confidence={:.2} deterministic_sufficient={} deterministic_contexts={} deterministic_chars={}",
            active_memory_decision_status_name(decision.status),
            decision.reason_code.as_str(),
            decision.confidence,
            deterministic.sufficient,
            deterministic.context_count,
            deterministic.context_chars
        ),
    )
}

pub(super) fn active_memory_dedup_observability_diagnostic(
    deterministic: &DeterministicRecallContextSummary,
    dedup: &ActiveRecallDedupResult,
) -> HookDiagnostic {
    let mut diagnostic = memory_safe_info_diagnostic(
        "memory.active_recall.dedup",
        format!(
            "memory active recall dedup: deterministic_recall_count={} active_raw_count={} active_duplicate_count={} active_rendered_count={} duplicate_only={}",
            deterministic.memory_ids.len(),
            dedup.raw_count,
            dedup.duplicate_count(),
            dedup.rendered_count(),
            dedup.duplicate_only()
        ),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "deterministic_recall_count",
        deterministic.memory_ids.len(),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_raw_count",
        dedup.raw_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_duplicate_id_count",
        dedup.duplicate_id_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_duplicate_line_count",
        dedup.duplicate_line_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_rendered_count",
        dedup.rendered_count(),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("active_duplicate_only"),
        HookValue::Bool(dedup.duplicate_only()),
    );
    diagnostic
}

pub(super) fn memory_prompt_recall_dedup_diagnostic(
    context: &MemoryRecallPromptContext,
) -> HookDiagnostic {
    let mut diagnostic = memory_safe_info_diagnostic(
        "memory.prompt_recall.dedup",
        format!(
            "memory prompt recall dedup: deterministic_recall_count={} active_raw_count={} active_duplicate_count={} active_rendered_count={} active_synthesis_rendered={} active_duplicate_only={}",
            context.deterministic_memory_count,
            context.active_raw_count,
            context.active_duplicate_count(),
            context.active_rendered_count,
            context.active_synthesis_rendered,
            context.active_duplicate_only()
        ),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "deterministic_recall_count",
        context.deterministic_memory_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_raw_count",
        context.active_raw_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_duplicate_id_count",
        context.active_duplicate_id_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_duplicate_line_count",
        context.active_duplicate_line_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_rendered_count",
        context.active_rendered_count,
    );
    diagnostic.metadata.insert(
        hook_metadata_key("active_synthesis_rendered"),
        HookValue::Bool(context.active_synthesis_rendered),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("active_duplicate_only"),
        HookValue::Bool(context.active_duplicate_only()),
    );
    diagnostic
}

pub(super) fn active_memory_decision_status_name(
    status: ActiveMemoryDecisionStatus,
) -> &'static str {
    match status {
        ActiveMemoryDecisionStatus::Skip => "skip",
        ActiveMemoryDecisionStatus::Run => "run",
        ActiveMemoryDecisionStatus::Uncertain => "uncertain",
    }
}

pub(super) fn parse_active_memory_decision_json(
    raw: &str,
) -> Result<ActiveMemoryDecision, serde_json::Error> {
    let parsed = serde_json::from_str::<ActiveMemoryDecisionJson>(raw.trim())?;
    let status = match parsed.status {
        ActiveMemoryDecisionJsonStatus::Skip => ActiveMemoryDecisionStatus::Skip,
        ActiveMemoryDecisionJsonStatus::Run => ActiveMemoryDecisionStatus::Run,
        ActiveMemoryDecisionJsonStatus::Uncertain => ActiveMemoryDecisionStatus::Uncertain,
    };
    let reason_code = match status {
        ActiveMemoryDecisionStatus::Skip => ActiveMemoryDecisionReasonCode::ProviderSkip,
        ActiveMemoryDecisionStatus::Run => ActiveMemoryDecisionReasonCode::ProviderRun,
        ActiveMemoryDecisionStatus::Uncertain => ActiveMemoryDecisionReasonCode::ProviderUncertain,
    };
    Ok(ActiveMemoryDecision {
        status,
        reason_code,
        confidence: parsed.confidence.clamp(0.0, 1.0),
        query_hints: parsed
            .query_hints
            .into_iter()
            .filter_map(|hint| bounded_nonempty_text(hint.as_str(), 240))
            .take(3)
            .collect(),
        diagnostics: parsed
            .diagnostics
            .into_iter()
            .filter_map(|diagnostic| bounded_nonempty_text(diagnostic.as_str(), 160))
            .collect(),
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ActiveMemoryDecisionJson {
    status: ActiveMemoryDecisionJsonStatus,
    #[serde(default)]
    confidence: f32,
    #[serde(default)]
    query_hints: Vec<String>,
    #[serde(default)]
    diagnostics: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ActiveMemoryDecisionJsonStatus {
    Skip,
    Run,
    Uncertain,
}

pub(super) fn is_trivial_self_contained_turn(input_text: &str) -> bool {
    let trimmed = input_text.trim();
    if trimmed.is_empty() {
        return true;
    }
    let word_count = trimmed.split_whitespace().count();
    let char_count = trimmed.chars().count();
    word_count <= 5 && char_count <= 48
}

pub(super) fn active_memory_query_plan(
    input_text: &str,
    decision: &ActiveMemoryDecision,
    config: &MemoryActiveRecallConfig,
) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut queries = Vec::new();
    for query in decision
        .query_hints
        .iter()
        .map(String::as_str)
        .chain([MEMORY_ACTIVE_RECALL_GENERIC_QUERY, input_text])
    {
        let Some(query) = bounded_nonempty_text(query, 500) else {
            continue;
        };
        let key = query.to_lowercase();
        if seen.insert(key) {
            queries.push(query);
        }
        if queries.len() >= config.max_queries {
            break;
        }
    }
    queries
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct ActiveRecallDedupResult {
    pub(super) items: Vec<MemoryRecallItem>,
    pub(super) raw_count: usize,
    pub(super) duplicate_id_count: usize,
    pub(super) duplicate_line_count: usize,
}

impl ActiveRecallDedupResult {
    pub(super) fn rendered_count(&self) -> usize {
        self.items.len()
    }

    pub(super) fn duplicate_count(&self) -> usize {
        self.duplicate_id_count + self.duplicate_line_count
    }

    pub(super) fn duplicate_only(&self) -> bool {
        self.raw_count > 0 && self.rendered_count() == 0 && self.duplicate_count() > 0
    }
}

pub(super) fn dedup_active_recall_items_with_lines(
    items: Vec<MemoryRecallItem>,
    deterministic_ids: &BTreeSet<String>,
    deterministic_line_fingerprints: &BTreeSet<String>,
) -> ActiveRecallDedupResult {
    let mut seen = deterministic_ids.clone();
    let mut seen_lines = deterministic_line_fingerprints.clone();
    let mut result = ActiveRecallDedupResult {
        raw_count: items.len(),
        ..ActiveRecallDedupResult::default()
    };
    let mut deduped = Vec::new();
    for item in items {
        let memory_id = item.memory_id.trim();
        if memory_id.is_empty() || !seen.insert(memory_id.to_owned()) {
            result.duplicate_id_count += 1;
            continue;
        }
        if let Some(fingerprint) = memory_recall_item_rendered_line_fingerprint(&item)
            && !seen_lines.insert(fingerprint)
        {
            result.duplicate_line_count += 1;
            continue;
        }
        deduped.push(item);
    }
    result.items = deduped;
    result
}

pub(super) fn memory_recall_item_rendered_line_fingerprint(
    item: &MemoryRecallItem,
) -> Option<String> {
    let prompt_item = memory_recall_prompt_item(item.clone());
    let (line, _) = render_memory_recall_context_block(&[prompt_item], false);
    rendered_line_fingerprint(line.as_str())
}

pub(super) fn rendered_line_fingerprints(content: &str) -> BTreeSet<String> {
    content
        .lines()
        .filter_map(rendered_line_fingerprint)
        .collect()
}

pub(super) fn active_memory_context_lines(content: &str) -> Vec<&str> {
    content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && *line != "Active memory context:")
        .collect()
}

pub(super) fn rendered_memory_line_id(line: &str) -> Option<String> {
    let line = line.trim();
    let metadata = line.strip_prefix("- [")?;
    let end = metadata
        .char_indices()
        .find_map(|(index, ch)| (ch == ',' || ch == ']').then_some(index))?;
    let memory_id = metadata[..end].trim();
    if memory_id.is_empty() {
        None
    } else {
        Some(memory_id.to_owned())
    }
}

pub(super) fn rendered_line_fingerprint(line: &str) -> Option<String> {
    let normalized = line.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

pub(super) fn memory_active_recall_prompt_context_contribution(
    items: Vec<MemoryRecallItem>,
    snapshot_truncated: bool,
    config: &MemoryActiveRecallConfig,
) -> Option<PromptContextContribution> {
    if items.is_empty() {
        return None;
    }
    let source_refs = memory_recall_source_refs(items.as_slice());
    let prompt_items = items
        .into_iter()
        .map(memory_recall_prompt_item)
        .collect::<Vec<_>>();
    let (content, rendered_truncated) =
        render_memory_recall_context_block(prompt_items.as_slice(), snapshot_truncated);
    if content.trim().is_empty() {
        return None;
    }
    let mut content = content;
    let mut truncated = rendered_truncated;
    let content_chars = content.chars().count();
    if content_chars > config.max_prompt_chars {
        content = truncate_chars(content.as_str(), config.max_prompt_chars);
        truncated = true;
    }
    Some(PromptContextContribution {
        contribution_id: HookContributionId::new(MEMORY_ACTIVE_RECALL_CONTRIBUTION_ID)
            .expect("static contribution id is valid"),
        domain: memory_policy_domain(),
        priority: 490,
        content: HookPromptContent::new(content).ok()?,
        max_chars: Some(config.max_prompt_chars),
        source_refs,
        diagnostics: Vec::new(),
        truncated,
    })
}

pub(super) fn bounded_nonempty_text(value: &str, max_chars: usize) -> Option<String> {
    let trimmed = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if trimmed.is_empty() {
        None
    } else {
        Some(truncate_chars(trimmed.as_str(), max_chars))
    }
}

pub(super) fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}
