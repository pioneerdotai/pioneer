use super::*;

const DEFAULT_SYNTHESIS_MAX_ITEMS: usize = 5;
const DEFAULT_SYNTHESIS_MAX_ITEM_CHARS: usize = 280;
const DEFAULT_SYNTHESIS_MAX_TOTAL_CHARS: usize = 1_500;
const DEFAULT_SYNTHESIS_MAX_DIAGNOSTICS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum MemoryRecallSynthesisSource {
    Deterministic,
    Active,
}

impl MemoryRecallSynthesisSource {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::Active => "active",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum MemoryRecallSynthesisContextKind {
    Context,
}

impl MemoryRecallSynthesisContextKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Context => "context",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum MemoryActiveSynthesisSourceKind {
    DurableMemory,
    CurrentThreadContext,
    RecentPromptContext,
}

impl MemoryActiveSynthesisSourceKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::DurableMemory => "durable_memory",
            Self::CurrentThreadContext => "current_thread_context",
            Self::RecentPromptContext => "recent_prompt_context",
        }
    }

    fn rank(self) -> usize {
        match self {
            Self::DurableMemory => 0,
            Self::CurrentThreadContext => 1,
            Self::RecentPromptContext => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MemoryActiveSynthesisSource {
    pub(super) kind: MemoryActiveSynthesisSourceKind,
    pub(super) source_id: String,
    pub(super) content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) canonical_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) text_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) score: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) updated_at: Option<i64>,
}

impl MemoryActiveSynthesisSource {
    pub(super) fn durable_memory(memory_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            kind: MemoryActiveSynthesisSourceKind::DurableMemory,
            source_id: memory_id.into(),
            content: content.into(),
            canonical_key: None,
            text_hash: None,
            score: None,
            updated_at: None,
        }
    }

    pub(super) fn current_thread_context(
        thread_source_id: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        Self {
            kind: MemoryActiveSynthesisSourceKind::CurrentThreadContext,
            source_id: thread_source_id.into(),
            content: content.into(),
            canonical_key: None,
            text_hash: None,
            score: None,
            updated_at: None,
        }
    }

    #[cfg(test)]
    pub(super) fn recent_prompt_context(
        context_source_id: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        Self {
            kind: MemoryActiveSynthesisSourceKind::RecentPromptContext,
            source_id: context_source_id.into(),
            content: content.into(),
            canonical_key: None,
            text_hash: None,
            score: None,
            updated_at: None,
        }
    }

    pub(super) fn rendered_source_id(&self) -> Option<String> {
        rendered_synthesis_source_id(self.kind, self.source_id.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MemoryActiveSynthesisInput {
    #[serde(default)]
    pub(super) sources: Vec<MemoryActiveSynthesisSource>,
    pub(super) context_kind: MemoryRecallSynthesisContextKind,
    pub(super) budget: MemoryRecallSynthesisBudget,
}

impl MemoryActiveSynthesisInput {
    pub(super) fn new(
        sources: Vec<MemoryActiveSynthesisSource>,
        budget: MemoryRecallSynthesisBudget,
    ) -> Self {
        Self {
            sources,
            context_kind: MemoryRecallSynthesisContextKind::Context,
            budget,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MemoryActiveSynthesizedContextItem {
    pub(super) summary: String,
    #[serde(default)]
    pub(super) source_ids: Vec<String>,
}

impl MemoryActiveSynthesizedContextItem {
    pub(super) fn prompt_line(&self) -> String {
        if self.source_ids.is_empty() {
            format!("- active synthesis: {}", self.summary)
        } else {
            format!(
                "- active synthesis: {} Sources: {}",
                self.summary,
                self.source_ids.join(", ")
            )
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MemoryActiveSynthesisOutput {
    pub(super) context_kind: MemoryRecallSynthesisContextKind,
    #[serde(default)]
    pub(super) items: Vec<MemoryActiveSynthesizedContextItem>,
    #[serde(default)]
    pub(super) source_ids: Vec<String>,
    #[serde(default)]
    pub(super) diagnostics: Vec<String>,
    #[serde(default)]
    pub(super) truncated: bool,
    #[serde(default)]
    pub(super) raw_source_count: usize,
    #[serde(default)]
    pub(super) dropped_source_count: usize,
    #[serde(default)]
    pub(super) truncated_source_count: usize,
}

impl MemoryActiveSynthesisOutput {
    pub(super) fn rendered_text(&self) -> String {
        self.items
            .iter()
            .map(MemoryActiveSynthesizedContextItem::prompt_line)
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub(super) fn duplicate_count(&self) -> usize {
        self.dropped_source_count
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum MemoryActiveSynthesisDedupReason {
    ExactSourceId,
    CanonicalKey,
    TextHash,
}

impl MemoryActiveSynthesisDedupReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::ExactSourceId => "exact_source_id",
            Self::CanonicalKey => "canonical_key",
            Self::TextHash => "text_hash",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MemoryActiveSynthesisDroppedSource {
    pub(super) reason: MemoryActiveSynthesisDedupReason,
    pub(super) kept_source_id: String,
    pub(super) dropped_source_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MemoryActiveSynthesisDedupResult {
    pub(super) input: MemoryActiveSynthesisInput,
    #[serde(default)]
    pub(super) dropped_sources: Vec<MemoryActiveSynthesisDroppedSource>,
    #[serde(default)]
    pub(super) diagnostics: Vec<String>,
}

impl MemoryActiveSynthesisDedupResult {
    pub(super) fn duplicate_count(&self) -> usize {
        self.dropped_sources.len()
    }
}

pub(super) fn ordered_active_synthesis_input(
    durable_items: Vec<MemoryRecallItem>,
    thread_items: Vec<MemoryEpisodicRecallItem>,
    recent_context: Vec<MemoryActiveSynthesisSource>,
    budget: MemoryRecallSynthesisBudget,
) -> MemoryActiveSynthesisInput {
    let mut sources = active_synthesis_sources_from_durable_memory(durable_items);
    sources.extend(active_synthesis_sources_from_thread_context(thread_items));
    sources.extend(recent_context);
    sources.sort_by(compare_active_synthesis_sources);
    MemoryActiveSynthesisInput::new(sources, budget)
}

pub(super) fn synthesize_active_memory_context(
    input: MemoryActiveSynthesisInput,
) -> MemoryActiveSynthesisOutput {
    let raw_source_count = input.sources.len();
    let dedup = dedup_active_synthesis_input(input);
    let budget = dedup.input.budget.normalized();
    let total_sources = dedup.input.sources.len();
    let dropped_source_count = dedup.duplicate_count();
    let mut output = MemoryActiveSynthesisOutput {
        context_kind: dedup.input.context_kind,
        items: Vec::new(),
        source_ids: Vec::new(),
        diagnostics: dedup.diagnostics,
        truncated: false,
        raw_source_count,
        dropped_source_count,
        truncated_source_count: 0,
    };
    if dedup.input.sources.is_empty() {
        return output;
    }

    let mut remaining_chars = budget.max_total_chars;
    let mut seen_rendered_lines = BTreeSet::new();
    for (index, source) in dedup.input.sources.into_iter().enumerate() {
        if output.items.len() >= budget.max_items {
            output.truncated = true;
            output.truncated_source_count += total_sources.saturating_sub(index);
            push_synthesis_diagnostic(
                &mut output.diagnostics,
                budget.max_diagnostics,
                "memory.active_synthesis.truncated:max_items",
            );
            break;
        }

        let Some(summary) = synthesized_item_text(source.content.as_str(), budget.max_item_chars)
        else {
            push_synthesis_diagnostic(
                &mut output.diagnostics,
                budget.max_diagnostics,
                "memory.active_synthesis.suppressed:empty_content",
            );
            continue;
        };
        let source_ids = source.rendered_source_id().into_iter().collect::<Vec<_>>();
        if source_ids.is_empty() {
            push_synthesis_diagnostic(
                &mut output.diagnostics,
                budget.max_diagnostics,
                "memory.active_synthesis.suppressed:missing_source_id",
            );
            continue;
        }
        let item = MemoryActiveSynthesizedContextItem {
            summary,
            source_ids,
        };
        let line = item.prompt_line();
        let Some(fingerprint) = rendered_line_fingerprint(line.as_str()) else {
            continue;
        };
        if !seen_rendered_lines.insert(fingerprint) {
            output.dropped_source_count += 1;
            push_synthesis_diagnostic(
                &mut output.diagnostics,
                budget.max_diagnostics,
                "memory.active_synthesis.dropped_duplicate:rendered_line",
            );
            continue;
        }
        let line_chars = line.chars().count();
        let separator_chars = usize::from(!output.items.is_empty());
        if separator_chars + line_chars > remaining_chars {
            output.truncated = true;
            output.truncated_source_count += total_sources.saturating_sub(index);
            push_synthesis_diagnostic(
                &mut output.diagnostics,
                budget.max_diagnostics,
                "memory.active_synthesis.truncated:max_total_chars",
            );
            break;
        }
        remaining_chars = remaining_chars.saturating_sub(separator_chars + line_chars);
        output.source_ids.extend(item.source_ids.clone());
        output.items.push(item);
    }
    if output.duplicate_count() > 0 {
        let duplicate_count = output.duplicate_count();
        push_synthesis_diagnostic(
            &mut output.diagnostics,
            budget.max_diagnostics,
            format!("memory.active_synthesis.duplicates_suppressed:{duplicate_count}"),
        );
    }
    output
}

pub(super) fn dedup_active_synthesis_input(
    input: MemoryActiveSynthesisInput,
) -> MemoryActiveSynthesisDedupResult {
    let mut seen_source_ids: BTreeMap<String, String> = BTreeMap::new();
    let mut seen_canonical_keys: BTreeMap<String, String> = BTreeMap::new();
    let mut seen_text_hashes: BTreeMap<String, String> = BTreeMap::new();
    let mut kept_sources = Vec::new();
    let mut dropped_sources = Vec::new();
    let mut diagnostics = Vec::new();

    for source in input.sources {
        let rendered_source_id = source
            .rendered_source_id()
            .unwrap_or_else(|| format!("{}:unknown", source.kind.as_str()));
        if let Some(kept_source_id) = seen_source_ids.get(&rendered_source_id) {
            push_active_synthesis_dropped_source(
                &mut dropped_sources,
                &mut diagnostics,
                MemoryActiveSynthesisDedupReason::ExactSourceId,
                kept_source_id,
                rendered_source_id.as_str(),
            );
            continue;
        }

        let canonical_key = source
            .canonical_key
            .as_deref()
            .and_then(normalized_optional_key);
        if let Some(canonical_key) = canonical_key.as_deref()
            && let Some(kept_source_id) = seen_canonical_keys.get(canonical_key)
        {
            push_active_synthesis_dropped_source(
                &mut dropped_sources,
                &mut diagnostics,
                MemoryActiveSynthesisDedupReason::CanonicalKey,
                kept_source_id,
                rendered_source_id.as_str(),
            );
            continue;
        }

        let text_hash = source
            .text_hash
            .as_deref()
            .and_then(normalized_optional_key)
            .or_else(|| {
                rendered_line_fingerprint(source.content.as_str())
                    .and_then(|fingerprint| normalized_optional_key(fingerprint.as_str()))
            });
        if let Some(text_hash) = text_hash.as_deref()
            && let Some(kept_source_id) = seen_text_hashes.get(text_hash)
        {
            push_active_synthesis_dropped_source(
                &mut dropped_sources,
                &mut diagnostics,
                MemoryActiveSynthesisDedupReason::TextHash,
                kept_source_id,
                rendered_source_id.as_str(),
            );
            continue;
        }

        seen_source_ids.insert(rendered_source_id.clone(), rendered_source_id.clone());
        if let Some(canonical_key) = canonical_key {
            seen_canonical_keys.insert(canonical_key, rendered_source_id.clone());
        }
        if let Some(text_hash) = text_hash {
            seen_text_hashes.insert(text_hash, rendered_source_id.clone());
        }
        kept_sources.push(source);
    }

    MemoryActiveSynthesisDedupResult {
        input: MemoryActiveSynthesisInput {
            sources: kept_sources,
            context_kind: input.context_kind,
            budget: input.budget,
        },
        dropped_sources,
        diagnostics,
    }
}

fn push_active_synthesis_dropped_source(
    dropped_sources: &mut Vec<MemoryActiveSynthesisDroppedSource>,
    diagnostics: &mut Vec<String>,
    reason: MemoryActiveSynthesisDedupReason,
    kept_source_id: &str,
    dropped_source_id: &str,
) {
    dropped_sources.push(MemoryActiveSynthesisDroppedSource {
        reason,
        kept_source_id: kept_source_id.to_owned(),
        dropped_source_id: dropped_source_id.to_owned(),
    });
    push_synthesis_diagnostic(
        diagnostics,
        DEFAULT_SYNTHESIS_MAX_DIAGNOSTICS,
        format!(
            "memory.active_synthesis.dropped_duplicate:{}:kept={}:dropped={}",
            reason.as_str(),
            kept_source_id,
            dropped_source_id
        ),
    );
}

fn normalized_optional_key(value: &str) -> Option<String> {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_lowercase())
    }
}

fn active_synthesis_sources_from_durable_memory(
    items: Vec<MemoryRecallItem>,
) -> Vec<MemoryActiveSynthesisSource> {
    items
        .into_iter()
        .map(|item| {
            let mut source =
                MemoryActiveSynthesisSource::durable_memory(item.memory_id, item.content);
            source.canonical_key = item.key;
            source.text_hash = rendered_line_fingerprint(source.content.as_str());
            source.score = item.score;
            source.updated_at = Some(item.updated_at);
            source
        })
        .collect()
}

fn active_synthesis_sources_from_thread_context(
    items: Vec<MemoryEpisodicRecallItem>,
) -> Vec<MemoryActiveSynthesisSource> {
    items
        .into_iter()
        .filter(|item| item.visibility.is_prompt_visible())
        .map(|item| {
            let mut source =
                MemoryActiveSynthesisSource::current_thread_context(item.id, item.content);
            source.text_hash = rendered_line_fingerprint(source.content.as_str());
            source.score = item.score.or(item.provenance.retrieval_score);
            source.updated_at = item.updated_at_unix.or(item.provenance.timestamp_unix);
            source
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MemoryRecallSynthesisBudget {
    pub(super) max_items: usize,
    pub(super) max_item_chars: usize,
    pub(super) max_total_chars: usize,
    pub(super) max_diagnostics: usize,
}

impl Default for MemoryRecallSynthesisBudget {
    fn default() -> Self {
        Self {
            max_items: DEFAULT_SYNTHESIS_MAX_ITEMS,
            max_item_chars: DEFAULT_SYNTHESIS_MAX_ITEM_CHARS,
            max_total_chars: DEFAULT_SYNTHESIS_MAX_TOTAL_CHARS,
            max_diagnostics: DEFAULT_SYNTHESIS_MAX_DIAGNOSTICS,
        }
    }
}

impl MemoryRecallSynthesisBudget {
    pub(super) fn normalized(self) -> Self {
        Self {
            max_items: self.max_items.max(1),
            max_item_chars: self.max_item_chars.max(1),
            max_total_chars: self.max_total_chars.max(1),
            max_diagnostics: self.max_diagnostics.max(1),
        }
    }

    pub(super) fn for_active_config(config: &MemoryActiveRecallConfig) -> Self {
        Self {
            max_items: (config.max_queries * config.top_k_per_query as usize).max(1),
            max_item_chars: DEFAULT_SYNTHESIS_MAX_ITEM_CHARS,
            max_total_chars: config.max_prompt_chars.max(1),
            max_diagnostics: DEFAULT_SYNTHESIS_MAX_DIAGNOSTICS,
        }
        .normalized()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct MemoryRecallSynthesisInput {
    pub(super) source: MemoryRecallSynthesisSource,
    pub(super) items: Vec<MemoryRecallItem>,
    pub(super) deterministic_memory_ids: BTreeSet<String>,
    pub(super) deterministic_line_fingerprints: BTreeSet<String>,
    pub(super) input_text_preview: Option<String>,
    pub(super) budget: MemoryRecallSynthesisBudget,
}

impl MemoryRecallSynthesisInput {
    pub(super) fn deterministic(
        snapshot: MemoryRecallSnapshot,
        budget: MemoryRecallSynthesisBudget,
    ) -> Self {
        Self {
            source: MemoryRecallSynthesisSource::Deterministic,
            items: snapshot.items,
            deterministic_memory_ids: BTreeSet::new(),
            deterministic_line_fingerprints: BTreeSet::new(),
            input_text_preview: None,
            budget,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct MemoryRecallSynthesisGroup {
    pub(super) scope: MemoryScope,
    pub(super) scope_label: String,
    pub(super) category: MemoryCategory,
    pub(super) fact_class: Option<MemoryFactClass>,
    pub(super) key: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct MemorySynthesizedRecallItem {
    pub(super) memory_id: String,
    pub(super) group: MemoryRecallSynthesisGroup,
    pub(super) scope: MemoryScope,
    pub(super) scope_label: String,
    pub(super) category: MemoryCategory,
    pub(super) fact_class: Option<MemoryFactClass>,
    pub(super) key: Option<String>,
    pub(super) synthesized_text: String,
    pub(super) source_refs: Vec<HookSourceRef>,
    pub(super) score: Option<f32>,
    pub(super) updated_at: i64,
}

impl MemorySynthesizedRecallItem {
    pub(super) fn prompt_line(&self) -> String {
        let label = synthesized_item_label(self.scope_label.as_str(), self.category);
        format!("- {label}: {}", self.synthesized_text)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct MemoryRecallSynthesis {
    pub(super) source: MemoryRecallSynthesisSource,
    pub(super) context_kind: MemoryRecallSynthesisContextKind,
    pub(super) items: Vec<MemorySynthesizedRecallItem>,
    pub(super) source_refs: Vec<HookSourceRef>,
    pub(super) diagnostics: Vec<String>,
    pub(super) truncated: bool,
    pub(super) truncated_item_count: usize,
    pub(super) raw_input_count: usize,
    pub(super) duplicate_memory_id_count: usize,
    pub(super) duplicate_rendered_content_count: usize,
    pub(super) suppressed_deterministic_id_count: usize,
}

impl MemoryRecallSynthesis {
    pub(super) fn empty(source: MemoryRecallSynthesisSource) -> Self {
        Self {
            source,
            context_kind: MemoryRecallSynthesisContextKind::Context,
            items: Vec::new(),
            source_refs: Vec::new(),
            diagnostics: Vec::new(),
            truncated: false,
            truncated_item_count: 0,
            raw_input_count: 0,
            duplicate_memory_id_count: 0,
            duplicate_rendered_content_count: 0,
            suppressed_deterministic_id_count: 0,
        }
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub(super) fn duplicate_count(&self) -> usize {
        self.duplicate_memory_id_count
            + self.duplicate_rendered_content_count
            + self.suppressed_deterministic_id_count
    }

    pub(super) fn rendered_text(&self) -> String {
        self.items
            .iter()
            .map(MemorySynthesizedRecallItem::prompt_line)
            .collect::<Vec<_>>()
            .join("\n")
    }
}

pub(super) struct MemoryRecallSynthesizer;

impl MemoryRecallSynthesizer {
    pub(super) fn synthesize(input: MemoryRecallSynthesisInput) -> MemoryRecallSynthesis {
        let budget = input.budget.normalized();
        let mut synthesis = MemoryRecallSynthesis::empty(input.source);
        synthesis.raw_input_count = input.items.len();
        if input.items.is_empty() {
            return synthesis;
        }

        let mut seen_ids = BTreeSet::new();
        let mut seen_lines = input.deterministic_line_fingerprints.clone();
        let mut remaining_chars = budget.max_total_chars;
        let mut items = input.items;
        items.sort_by(compare_recall_items_for_synthesis);

        let total_items = items.len();
        for (index, item) in items.into_iter().enumerate() {
            if synthesis.items.len() >= budget.max_items {
                synthesis.truncated = true;
                synthesis.truncated_item_count += total_items.saturating_sub(index);
                push_synthesis_diagnostic(
                    &mut synthesis.diagnostics,
                    budget.max_diagnostics,
                    "memory.recall_synthesis.truncated:max_items",
                );
                break;
            }

            let memory_id = item.memory_id.trim();
            if memory_id.is_empty() || !seen_ids.insert(memory_id.to_owned()) {
                synthesis.duplicate_memory_id_count += 1;
                continue;
            }
            if input.source == MemoryRecallSynthesisSource::Active
                && input.deterministic_memory_ids.contains(memory_id)
            {
                synthesis.suppressed_deterministic_id_count += 1;
                continue;
            }

            let Some(synthesized_text) =
                synthesized_item_text(item.content.as_str(), budget.max_item_chars)
            else {
                push_synthesis_diagnostic(
                    &mut synthesis.diagnostics,
                    budget.max_diagnostics,
                    "memory.recall_synthesis.suppressed:empty_content",
                );
                continue;
            };

            let synthesized = MemorySynthesizedRecallItem {
                memory_id: memory_id.to_owned(),
                group: MemoryRecallSynthesisGroup {
                    scope: item.scope.clone(),
                    scope_label: scope_label(&item.scope),
                    category: item.category,
                    fact_class: fact_class_for_recall_category(item.category),
                    key: item.key.clone(),
                },
                scope_label: scope_label(&item.scope),
                fact_class: fact_class_for_recall_category(item.category),
                source_refs: vec![memory_recall_source_ref(memory_id)],
                scope: item.scope,
                category: item.category,
                key: item.key,
                synthesized_text,
                score: item.score,
                updated_at: item.updated_at,
            };
            let line = synthesized.prompt_line();
            let Some(fingerprint) = rendered_line_fingerprint(line.as_str()) else {
                continue;
            };
            if !seen_lines.insert(fingerprint) {
                synthesis.duplicate_rendered_content_count += 1;
                continue;
            }

            let line_chars = line.chars().count();
            let separator_chars = usize::from(!synthesis.items.is_empty());
            if separator_chars + line_chars > remaining_chars {
                synthesis.truncated = true;
                synthesis.truncated_item_count += total_items.saturating_sub(index);
                push_synthesis_diagnostic(
                    &mut synthesis.diagnostics,
                    budget.max_diagnostics,
                    "memory.recall_synthesis.truncated:max_total_chars",
                );
                break;
            }
            remaining_chars = remaining_chars.saturating_sub(separator_chars + line_chars);
            synthesis
                .source_refs
                .extend(synthesized.source_refs.clone());
            synthesis.items.push(synthesized);
        }

        if synthesis.duplicate_count() > 0 {
            let duplicate_count = synthesis.duplicate_count();
            push_synthesis_diagnostic(
                &mut synthesis.diagnostics,
                budget.max_diagnostics,
                format!("memory.recall_synthesis.duplicates_suppressed:{duplicate_count}"),
            );
        }
        synthesis
    }
}

pub(super) fn memory_recall_synthesis_observability_diagnostic(
    synthesis: &MemoryRecallSynthesis,
) -> HookDiagnostic {
    let mut diagnostic = memory_safe_info_diagnostic(
        "memory.recall_synthesis",
        format!(
            "memory recall synthesis: source={} raw_count={} synthesized_count={} duplicate_count={} truncated={}",
            synthesis.source.as_str(),
            synthesis.raw_input_count,
            synthesis.items.len(),
            synthesis.duplicate_count(),
            synthesis.truncated
        ),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("source"),
        HookValue::Text(synthesis.source.as_str().to_owned()),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "raw_count",
        synthesis.raw_input_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "synthesized_count",
        synthesis.items.len(),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "duplicate_memory_id_count",
        synthesis.duplicate_memory_id_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "duplicate_rendered_content_count",
        synthesis.duplicate_rendered_content_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "truncated_item_count",
        synthesis.truncated_item_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "source_ref_count",
        synthesis.source_refs.len(),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "suppressed_deterministic_id_count",
        synthesis.suppressed_deterministic_id_count,
    );
    diagnostic.metadata.insert(
        hook_metadata_key("truncated"),
        HookValue::Bool(synthesis.truncated),
    );
    diagnostic
}

pub(super) fn memory_active_synthesis_observability_diagnostic(
    synthesis: &MemoryActiveSynthesisOutput,
) -> HookDiagnostic {
    let mut diagnostic = memory_safe_info_diagnostic(
        "memory.active_synthesis",
        format!(
            "memory active synthesis: raw_count={} synthesized_count={} duplicate_count={} truncated={}",
            synthesis.raw_source_count,
            synthesis.items.len(),
            synthesis.duplicate_count(),
            synthesis.truncated
        ),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("context_kind"),
        HookValue::Text(synthesis.context_kind.as_str().to_owned()),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "raw_count",
        synthesis.raw_source_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "synthesized_count",
        synthesis.items.len(),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "duplicate_count",
        synthesis.duplicate_count(),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "truncated_source_count",
        synthesis.truncated_source_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "source_ref_count",
        synthesis.source_ids.len(),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("truncated"),
        HookValue::Bool(synthesis.truncated),
    );
    diagnostic
}

pub(super) fn memory_recall_source_ref(memory_id: &str) -> HookSourceRef {
    HookSourceRef {
        kind: HookSourceKind::Custom("memory".to_owned()),
        id: HookSourceId::new(memory_id).expect("memory id should be valid source id"),
        label: None,
    }
}

pub(super) fn active_synthesis_source_refs(
    synthesis: &MemoryActiveSynthesisOutput,
) -> Vec<HookSourceRef> {
    let mut refs = Vec::new();
    let mut seen = BTreeSet::new();
    for source_id in &synthesis.source_ids {
        if !seen.insert(source_id.as_str()) {
            continue;
        }
        let Some(source_ref) = active_synthesis_source_ref(source_id) else {
            continue;
        };
        refs.push(source_ref);
    }
    refs
}

fn active_synthesis_source_ref(source_id: &str) -> Option<HookSourceRef> {
    let source_id = source_id.trim();
    if source_id.is_empty() {
        return None;
    }
    let (kind, source_ref_id) = if let Some(memory_id) = source_id.strip_prefix("memory:") {
        ("memory", memory_id)
    } else if source_id.starts_with("thread:") {
        ("thread_context", source_id)
    } else if source_id.starts_with("recent:") {
        ("recent_prompt_context", source_id)
    } else {
        ("active_synthesis_source", source_id)
    };
    Some(HookSourceRef {
        kind: HookSourceKind::Custom(kind.to_owned()),
        id: HookSourceId::new(source_ref_id.to_owned()).ok()?,
        label: None,
    })
}

fn rendered_synthesis_source_id(
    kind: MemoryActiveSynthesisSourceKind,
    source_id: &str,
) -> Option<String> {
    let source_id = source_id.trim();
    if source_id.is_empty() {
        return None;
    }
    match kind {
        MemoryActiveSynthesisSourceKind::DurableMemory => {
            source_id.strip_prefix("memory:").map_or_else(
                || Some(format!("memory:{source_id}")),
                |_| Some(source_id.to_owned()),
            )
        }
        MemoryActiveSynthesisSourceKind::CurrentThreadContext => {
            source_id.strip_prefix("thread:").map_or_else(
                || Some(format!("thread:{source_id}")),
                |_| Some(source_id.to_owned()),
            )
        }
        MemoryActiveSynthesisSourceKind::RecentPromptContext => {
            source_id.strip_prefix("recent:").map_or_else(
                || Some(format!("recent:{source_id}")),
                |_| Some(source_id.to_owned()),
            )
        }
    }
}

fn compare_active_synthesis_sources(
    left: &MemoryActiveSynthesisSource,
    right: &MemoryActiveSynthesisSource,
) -> std::cmp::Ordering {
    active_synthesis_source_rank(left)
        .cmp(&active_synthesis_source_rank(right))
        .then_with(|| compare_score_desc(left.score, right.score))
        .then_with(|| right.updated_at.cmp(&left.updated_at))
        .then_with(|| left.kind.cmp(&right.kind))
        .then_with(|| {
            left.rendered_source_id()
                .unwrap_or_default()
                .cmp(&right.rendered_source_id().unwrap_or_default())
        })
        .then_with(|| left.content.cmp(&right.content))
}

fn active_synthesis_source_rank(source: &MemoryActiveSynthesisSource) -> usize {
    if source
        .canonical_key
        .as_deref()
        .is_some_and(|key| !key.trim().is_empty())
    {
        return 0;
    }
    if source
        .rendered_source_id()
        .as_deref()
        .is_some_and(|source_id| !source_id.trim().is_empty())
        && source.kind != MemoryActiveSynthesisSourceKind::RecentPromptContext
    {
        return 1;
    }
    if source
        .text_hash
        .as_deref()
        .is_some_and(|hash| !hash.trim().is_empty())
    {
        return 2;
    }
    10 + source.kind.rank()
}

fn compare_recall_items_for_synthesis(
    left: &MemoryRecallItem,
    right: &MemoryRecallItem,
) -> std::cmp::Ordering {
    compare_score_desc(left.score, right.score)
        .then_with(|| category_label(left.category).cmp(category_label(right.category)))
        .then_with(|| left.key.cmp(&right.key))
        .then_with(|| right.updated_at.cmp(&left.updated_at))
        .then_with(|| left.memory_id.cmp(&right.memory_id))
}

fn compare_score_desc(left: Option<f32>, right: Option<f32>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(left), Some(right)) => right
            .partial_cmp(&left)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn synthesized_item_label(scope: &str, category: MemoryCategory) -> String {
    let category = category_label(category).replace('_', " ");
    format!("{} {}", display_scope_label(scope), category)
}

fn display_scope_label(scope: &str) -> String {
    scope
        .split(':')
        .next()
        .unwrap_or(scope)
        .replace('_', " ")
        .trim()
        .to_owned()
}

fn synthesized_item_text(content: &str, max_chars: usize) -> Option<String> {
    let trimmed = content.split_whitespace().collect::<Vec<_>>().join(" ");
    if trimmed.is_empty() {
        None
    } else {
        Some(truncate_synthesis_chars(trimmed.as_str(), max_chars))
    }
}

fn truncate_synthesis_chars(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn push_synthesis_diagnostic(
    diagnostics: &mut Vec<String>,
    max_diagnostics: usize,
    diagnostic: impl Into<String>,
) {
    if diagnostics.len() < max_diagnostics {
        diagnostics.push(diagnostic.into());
    }
}

fn fact_class_for_recall_category(category: MemoryCategory) -> Option<MemoryFactClass> {
    match category {
        MemoryCategory::Identity => Some(MemoryFactClass::UserIdentity),
        MemoryCategory::Preference => Some(MemoryFactClass::StableUserPreference),
        MemoryCategory::Biography => Some(MemoryFactClass::UserBiography),
        MemoryCategory::Relationship => Some(MemoryFactClass::UserRelationship),
        MemoryCategory::RecurringInstruction => Some(MemoryFactClass::RecurringUserInstruction),
        MemoryCategory::ProjectPolicy => Some(MemoryFactClass::ProjectPolicy),
        MemoryCategory::ProjectDecision => Some(MemoryFactClass::ProjectDecision),
        MemoryCategory::Procedure => Some(MemoryFactClass::ProjectProcedure),
        MemoryCategory::Constraint => Some(MemoryFactClass::ProjectConstraint),
        MemoryCategory::Todo => Some(MemoryFactClass::ThreadLocalState),
        MemoryCategory::ProjectFact
        | MemoryCategory::CommunicationStyle
        | MemoryCategory::Custom => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn synthesis_item(memory_id: &str, content: &str) -> MemoryRecallItem {
        MemoryRecallItem {
            memory_id: memory_id.to_owned(),
            scope: MemoryScope {
                kind: MemoryScopeKind::User,
                key: "global".to_owned(),
            },
            category: MemoryCategory::Identity,
            key: Some("name".to_owned()),
            content: content.to_owned(),
            score: Some(0.9),
            updated_at: 1,
        }
    }

    fn synthesis_project_item(
        memory_id: &str,
        category: MemoryCategory,
        key: &str,
        content: &str,
        score: f32,
    ) -> MemoryRecallItem {
        MemoryRecallItem {
            memory_id: memory_id.to_owned(),
            scope: MemoryScope {
                kind: MemoryScopeKind::Workspace,
                key: "ws".to_owned(),
            },
            category,
            key: Some(key.to_owned()),
            content: content.to_owned(),
            score: Some(score),
            updated_at: 1,
        }
    }

    fn synthesis_thread_item(
        source_id: &str,
        content: &str,
        score: f32,
        timestamp: i64,
    ) -> MemoryEpisodicRecallItem {
        MemoryEpisodicRecallItem {
            id: source_id.to_owned(),
            content: content.to_owned(),
            title: None,
            provenance: MemoryEpisodicRecallProvenance {
                workspace_id: "ws".to_owned(),
                thread_id: Some("thread".to_owned()),
                turn_id: Some("turn".to_owned()),
                task_id: None,
                timestamp_unix: Some(timestamp),
                source: MemoryEpisodicRecallSourceKind::CurrentThread,
                retrieval_score: Some(score),
                boundary: MemoryEpisodicRecallBoundary::Snippet,
            },
            score: Some(score),
            updated_at_unix: Some(timestamp),
            visibility: MemoryEpisodicRecallVisibility::Public,
        }
    }

    #[test]
    fn active_synthesis_contract_serializes_multi_source_input() {
        let mut durable = MemoryActiveSynthesisSource::durable_memory(
            "mem_name",
            "Пользователя зовут Александр.",
        );
        durable.canonical_key = Some("user_name".to_owned());
        durable.text_hash = Some("hash-name".to_owned());
        durable.score = Some(0.91);
        durable.updated_at = Some(42);
        let input = MemoryActiveSynthesisInput::new(
            vec![
                durable,
                MemoryActiveSynthesisSource::current_thread_context(
                    "thread:turn_41/item_1/chunk_0",
                    "Earlier in this thread the user rejected phrase lists.",
                ),
                MemoryActiveSynthesisSource::recent_prompt_context(
                    "turn_input",
                    "Continue the proposal work.",
                ),
            ],
            MemoryRecallSynthesisBudget::default(),
        );

        let encoded = serde_json::to_value(&input).expect("input serializes");
        assert_eq!(encoded["contextKind"], "context");
        assert_eq!(encoded["sources"][0]["kind"], "durable_memory");
        assert_eq!(encoded["sources"][1]["kind"], "current_thread_context");
        assert_eq!(encoded["sources"][2]["kind"], "recent_prompt_context");

        let decoded: MemoryActiveSynthesisInput =
            serde_json::from_value(encoded).expect("input deserializes");
        assert_eq!(decoded.sources.len(), 3);
        assert_eq!(
            decoded.sources[0].rendered_source_id().as_deref(),
            Some("memory:mem_name")
        );
        assert_eq!(
            decoded.sources[1].rendered_source_id().as_deref(),
            Some("thread:turn_41/item_1/chunk_0")
        );
        assert_eq!(
            decoded.sources[2].rendered_source_id().as_deref(),
            Some("recent:turn_input")
        );
    }

    #[test]
    fn active_synthesis_output_roundtrips_context_with_source_ids() {
        let output = MemoryActiveSynthesisOutput {
            context_kind: MemoryRecallSynthesisContextKind::Context,
            items: vec![MemoryActiveSynthesizedContextItem {
                summary: "User is continuing memory architecture work.".to_owned(),
                source_ids: vec![
                    "memory:mem_project".to_owned(),
                    "thread:turn_41/item_1/chunk_0".to_owned(),
                ],
            }],
            source_ids: vec![
                "memory:mem_project".to_owned(),
                "thread:turn_41/item_1/chunk_0".to_owned(),
            ],
            diagnostics: vec!["memory.active_synthesis.context_not_instruction".to_owned()],
            truncated: false,
            raw_source_count: 2,
            dropped_source_count: 0,
            truncated_source_count: 0,
        };

        let encoded = serde_json::to_string(&output).expect("output serializes");
        assert!(encoded.contains("memory:mem_project"));
        assert!(encoded.contains("thread:turn_41/item_1/chunk_0"));
        let decoded: MemoryActiveSynthesisOutput =
            serde_json::from_str(encoded.as_str()).expect("output deserializes");

        assert_eq!(
            decoded.context_kind,
            MemoryRecallSynthesisContextKind::Context
        );
        assert_eq!(decoded.context_kind.as_str(), "context");
        assert_eq!(decoded.items[0].source_ids.len(), 2);
        assert_eq!(decoded.source_ids.len(), 2);
    }

    #[test]
    fn active_synthesis_merge_orders_mixed_sources_deterministically() {
        let durable_without_key = synthesis_project_item(
            "mem_unkeyed",
            MemoryCategory::ProjectDecision,
            "",
            "Unkeyed durable fact.",
            0.99,
        );
        let durable_with_key = synthesis_project_item(
            "mem_keyed",
            MemoryCategory::ProjectDecision,
            "architecture_decision",
            "Use hook runtime for memory.",
            0.7,
        );
        let recent =
            MemoryActiveSynthesisSource::recent_prompt_context("turn_input", "Continue this work.");
        let input = ordered_active_synthesis_input(
            vec![durable_without_key, durable_with_key],
            vec![synthesis_thread_item(
                "thread:turn_41/item_1/chunk_0",
                "The user rejected phrase-list classifiers.",
                0.8,
                10,
            )],
            vec![recent],
            MemoryRecallSynthesisBudget::default(),
        );

        let source_ids = input
            .sources
            .iter()
            .filter_map(MemoryActiveSynthesisSource::rendered_source_id)
            .collect::<Vec<_>>();
        assert_eq!(
            source_ids,
            vec![
                "memory:mem_keyed",
                "memory:mem_unkeyed",
                "thread:turn_41/item_1/chunk_0",
                "recent:turn_input",
            ]
        );
    }

    #[test]
    fn active_synthesis_merge_preserves_source_type_boundaries() {
        let input = ordered_active_synthesis_input(
            vec![synthesis_item("mem_name", "Пользователя зовут Александр.")],
            vec![synthesis_thread_item(
                "thread:turn_1/item_1/chunk_0",
                "Thread-local implementation detail.",
                0.8,
                5,
            )],
            vec![MemoryActiveSynthesisSource::recent_prompt_context(
                "turn_input",
                "What did we decide?",
            )],
            MemoryRecallSynthesisBudget::default(),
        );

        let kinds = input
            .sources
            .iter()
            .map(|source| source.kind)
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            vec![
                MemoryActiveSynthesisSourceKind::DurableMemory,
                MemoryActiveSynthesisSourceKind::CurrentThreadContext,
                MemoryActiveSynthesisSourceKind::RecentPromptContext,
            ]
        );
        assert_eq!(input.sources.len(), 3);
    }

    #[test]
    fn active_synthesis_deduplicates_exact_source_ids() {
        let input = MemoryActiveSynthesisInput::new(
            vec![
                MemoryActiveSynthesisSource::current_thread_context(
                    "thread:turn_1/item_1/chunk_0",
                    "First copy.",
                ),
                MemoryActiveSynthesisSource::current_thread_context(
                    "thread:turn_1/item_1/chunk_0",
                    "Second copy.",
                ),
            ],
            MemoryRecallSynthesisBudget::default(),
        );

        let result = dedup_active_synthesis_input(input);

        assert_eq!(result.input.sources.len(), 1);
        assert_eq!(result.duplicate_count(), 1);
        assert_eq!(
            result.dropped_sources[0].reason,
            MemoryActiveSynthesisDedupReason::ExactSourceId
        );
        assert_eq!(
            result.dropped_sources[0].dropped_source_id,
            "thread:turn_1/item_1/chunk_0"
        );
    }

    #[test]
    fn active_synthesis_deduplicates_canonical_key_preferring_durable_fact() {
        let mut durable = MemoryActiveSynthesisSource::durable_memory(
            "mem_name",
            "Пользователя зовут Александр.",
        );
        durable.canonical_key = Some("user_name".to_owned());
        let mut weaker = MemoryActiveSynthesisSource::current_thread_context(
            "thread:turn_1/item_1/chunk_0",
            "Меня зовут Александр.",
        );
        weaker.canonical_key = Some(" user_name ".to_owned());
        let input = MemoryActiveSynthesisInput::new(
            vec![durable, weaker],
            MemoryRecallSynthesisBudget::default(),
        );

        let result = dedup_active_synthesis_input(input);

        assert_eq!(result.input.sources.len(), 1);
        assert_eq!(
            result.input.sources[0].rendered_source_id().as_deref(),
            Some("memory:mem_name")
        );
        assert_eq!(
            result.dropped_sources[0].reason,
            MemoryActiveSynthesisDedupReason::CanonicalKey
        );
        assert_eq!(result.dropped_sources[0].kept_source_id, "memory:mem_name");
        assert_eq!(
            result.dropped_sources[0].dropped_source_id,
            "thread:turn_1/item_1/chunk_0"
        );
    }

    #[test]
    fn active_synthesis_deduplicates_text_hash_with_dropped_source_diagnostics() {
        let input = ordered_active_synthesis_input(
            vec![synthesis_item("mem_name", "Пользователя зовут Александр.")],
            vec![synthesis_thread_item(
                "thread:turn_1/item_1/chunk_0",
                "Пользователя зовут Александр.",
                0.8,
                5,
            )],
            Vec::new(),
            MemoryRecallSynthesisBudget::default(),
        );

        let result = dedup_active_synthesis_input(input);

        assert_eq!(result.input.sources.len(), 1);
        assert_eq!(
            result.dropped_sources[0].reason,
            MemoryActiveSynthesisDedupReason::TextHash
        );
        assert!(result.diagnostics.iter().any(|diagnostic| {
            diagnostic.contains("kept=memory:mem_name")
                && diagnostic.contains("dropped=thread:turn_1/item_1/chunk_0")
        }));
    }

    #[test]
    fn active_synthesis_renders_compact_context_with_visible_source_ids() {
        let input = ordered_active_synthesis_input(
            vec![synthesis_project_item(
                "mem_arch",
                MemoryCategory::ProjectDecision,
                "hook_runtime",
                "Use hook runtime for memory domains.",
                0.95,
            )],
            vec![synthesis_thread_item(
                "thread:turn_41/item_1/chunk_0",
                "The user rejected phrase-list intent detection.",
                0.88,
                20,
            )],
            Vec::new(),
            MemoryRecallSynthesisBudget::default(),
        );

        let synthesis = synthesize_active_memory_context(input);
        let rendered = synthesis.rendered_text();

        assert_eq!(
            synthesis.context_kind,
            MemoryRecallSynthesisContextKind::Context
        );
        assert!(rendered.contains("Sources: memory:mem_arch"));
        assert!(rendered.contains("Sources: thread:turn_41/item_1/chunk_0"));
        assert!(!rendered.contains("score="));
    }

    #[test]
    fn active_synthesis_source_refs_preserve_durable_and_thread_provenance() {
        let synthesis = MemoryActiveSynthesisOutput {
            context_kind: MemoryRecallSynthesisContextKind::Context,
            items: Vec::new(),
            source_ids: vec![
                "memory:mem_arch".to_owned(),
                "thread:turn_41/item_1/chunk_0".to_owned(),
            ],
            diagnostics: Vec::new(),
            truncated: false,
            raw_source_count: 2,
            dropped_source_count: 0,
            truncated_source_count: 0,
        };

        let refs = active_synthesis_source_refs(&synthesis);

        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].kind.as_str(), "memory");
        assert_eq!(refs[0].id.as_str(), "mem_arch");
        assert_eq!(refs[1].kind.as_str(), "thread_context");
        assert_eq!(refs[1].id.as_str(), "thread:turn_41/item_1/chunk_0");
    }

    #[test]
    fn active_synthesis_budget_truncates_long_thread_context_deterministically() {
        let input = ordered_active_synthesis_input(
            Vec::new(),
            vec![
                synthesis_thread_item(
                    "thread:turn_1/item_1/chunk_0",
                    "First thread context item is useful and should fit.",
                    0.95,
                    10,
                ),
                synthesis_thread_item(
                    "thread:turn_2/item_1/chunk_0",
                    "Second thread context item is too much for the prompt budget.",
                    0.94,
                    9,
                ),
            ],
            Vec::new(),
            MemoryRecallSynthesisBudget {
                max_items: 5,
                max_item_chars: 80,
                max_total_chars: 115,
                max_diagnostics: 8,
            },
        );

        let synthesis = synthesize_active_memory_context(input);

        assert!(synthesis.truncated);
        assert_eq!(synthesis.items.len(), 1);
        assert_eq!(synthesis.truncated_source_count, 1);
        assert_eq!(
            synthesis.rendered_text(),
            "- active synthesis: First thread context item is useful and should fit. Sources: thread:turn_1/item_1/chunk_0"
        );
        assert_eq!(
            synthesis.source_ids,
            vec!["thread:turn_1/item_1/chunk_0".to_owned()]
        );
    }

    #[test]
    fn active_synthesis_prompt_text_does_not_render_raw_candidate_metadata() {
        let input = ordered_active_synthesis_input(
            Vec::new(),
            vec![synthesis_thread_item(
                "thread:turn_1/item_1/chunk_0",
                "User asked to keep thread context separate.",
                0.95,
                10,
            )],
            Vec::new(),
            MemoryRecallSynthesisBudget::default(),
        );

        let rendered = synthesize_active_memory_context(input).rendered_text();

        assert!(rendered.contains("Sources: thread:turn_1/item_1/chunk_0"));
        assert!(!rendered.contains("role="));
        assert!(!rendered.contains("context=message"));
        assert!(!rendered.contains("score="));
        assert!(!rendered.contains("boundary="));
    }

    #[test]
    fn recall_synthesis_empty_input_is_empty_context() {
        let synthesis = MemoryRecallSynthesizer::synthesize(MemoryRecallSynthesisInput {
            source: MemoryRecallSynthesisSource::Deterministic,
            items: Vec::new(),
            deterministic_memory_ids: BTreeSet::new(),
            deterministic_line_fingerprints: BTreeSet::new(),
            input_text_preview: None,
            budget: MemoryRecallSynthesisBudget::default(),
        });

        assert!(synthesis.is_empty());
        assert_eq!(
            synthesis.context_kind,
            MemoryRecallSynthesisContextKind::Context
        );
        assert!(synthesis.source_refs.is_empty());
        assert!(synthesis.diagnostics.is_empty());
    }

    #[test]
    fn recall_synthesis_contract_preserves_refs_without_prompting_ids() {
        let synthesis = MemoryRecallSynthesizer::synthesize(MemoryRecallSynthesisInput {
            source: MemoryRecallSynthesisSource::Deterministic,
            items: vec![synthesis_item("mem_name", "Пользователя зовут Александр.")],
            deterministic_memory_ids: BTreeSet::new(),
            deterministic_line_fingerprints: BTreeSet::new(),
            input_text_preview: None,
            budget: MemoryRecallSynthesisBudget::default(),
        });

        assert_eq!(synthesis.items.len(), 1);
        assert_eq!(synthesis.source_refs.len(), 1);
        assert_eq!(synthesis.source_refs[0].id.as_str(), "mem_name");
        let rendered = synthesis.rendered_text();
        assert!(rendered.contains("Пользователя зовут Александр."));
        assert!(!rendered.contains("mem_name"));
        assert!(!rendered.contains("score="));
    }

    #[test]
    fn recall_synthesis_groups_project_memories_and_orders_by_score() {
        let synthesis = MemoryRecallSynthesizer::synthesize(MemoryRecallSynthesisInput {
            source: MemoryRecallSynthesisSource::Deterministic,
            items: vec![
                synthesis_project_item(
                    "mem_low",
                    MemoryCategory::ProjectDecision,
                    "decision-low",
                    "Use old broad memory search.",
                    0.2,
                ),
                synthesis_project_item(
                    "mem_high",
                    MemoryCategory::ProjectPolicy,
                    "policy-high",
                    "Use hook runtime for memory domains.",
                    0.95,
                ),
            ],
            deterministic_memory_ids: BTreeSet::new(),
            deterministic_line_fingerprints: BTreeSet::new(),
            input_text_preview: None,
            budget: MemoryRecallSynthesisBudget::default(),
        });

        assert_eq!(synthesis.items.len(), 2);
        assert_eq!(synthesis.items[0].memory_id, "mem_high");
        assert_eq!(
            synthesis.items[0].group.fact_class,
            Some(MemoryFactClass::ProjectPolicy)
        );
        assert_eq!(
            synthesis.items[1].group.fact_class,
            Some(MemoryFactClass::ProjectDecision)
        );
        let rendered = synthesis.rendered_text();
        assert!(rendered.contains("workspace project policy: Use hook runtime"));
        assert!(rendered.contains("workspace project decision: Use old broad memory search."));
    }

    #[test]
    fn recall_synthesis_suppresses_duplicate_ids_and_rendered_text() {
        let synthesis = MemoryRecallSynthesizer::synthesize(MemoryRecallSynthesisInput {
            source: MemoryRecallSynthesisSource::Deterministic,
            items: vec![
                synthesis_item("mem_one", "Пользователя зовут Александр."),
                synthesis_item("mem_one", "Пользователя зовут Александр."),
                synthesis_item("mem_two", "Пользователя зовут Александр."),
            ],
            deterministic_memory_ids: BTreeSet::new(),
            deterministic_line_fingerprints: BTreeSet::new(),
            input_text_preview: None,
            budget: MemoryRecallSynthesisBudget::default(),
        });

        assert_eq!(synthesis.items.len(), 1);
        assert_eq!(synthesis.duplicate_memory_id_count, 1);
        assert_eq!(synthesis.duplicate_rendered_content_count, 1);
        assert!(
            synthesis.diagnostics.iter().any(|diagnostic| {
                diagnostic == "memory.recall_synthesis.duplicates_suppressed:2"
            })
        );
    }

    #[test]
    fn recall_synthesis_suppresses_active_items_already_in_deterministic_context() {
        let mut deterministic_ids = BTreeSet::new();
        deterministic_ids.insert("mem_name".to_owned());
        let synthesis = MemoryRecallSynthesizer::synthesize(MemoryRecallSynthesisInput {
            source: MemoryRecallSynthesisSource::Active,
            items: vec![
                synthesis_item("mem_name", "Пользователя зовут Александр."),
                synthesis_item("mem_city", "Пользователь любит Порту."),
            ],
            deterministic_memory_ids: deterministic_ids,
            deterministic_line_fingerprints: BTreeSet::new(),
            input_text_preview: None,
            budget: MemoryRecallSynthesisBudget::default(),
        });

        assert_eq!(synthesis.items.len(), 1);
        assert_eq!(synthesis.items[0].memory_id, "mem_city");
        assert_eq!(synthesis.suppressed_deterministic_id_count, 1);
    }

    #[test]
    fn recall_synthesis_item_budget_truncates_with_diagnostic() {
        let synthesis = MemoryRecallSynthesizer::synthesize(MemoryRecallSynthesisInput {
            source: MemoryRecallSynthesisSource::Deterministic,
            items: vec![
                synthesis_item("mem_1", "one"),
                synthesis_item("mem_2", "two"),
                synthesis_item("mem_3", "three"),
            ],
            deterministic_memory_ids: BTreeSet::new(),
            deterministic_line_fingerprints: BTreeSet::new(),
            input_text_preview: None,
            budget: MemoryRecallSynthesisBudget {
                max_items: 2,
                ..MemoryRecallSynthesisBudget::default()
            },
        });

        assert_eq!(synthesis.items.len(), 2);
        assert!(synthesis.truncated);
        assert_eq!(synthesis.truncated_item_count, 1);
        assert!(
            synthesis
                .diagnostics
                .iter()
                .any(|diagnostic| { diagnostic == "memory.recall_synthesis.truncated:max_items" })
        );
    }

    #[test]
    fn recall_synthesis_char_budget_truncates_without_splitting_chars() {
        let synthesis = MemoryRecallSynthesizer::synthesize(MemoryRecallSynthesisInput {
            source: MemoryRecallSynthesisSource::Deterministic,
            items: vec![
                synthesis_item("mem_1", "коротко"),
                synthesis_item("mem_2", "очень длинное значение которое не должно пройти"),
            ],
            deterministic_memory_ids: BTreeSet::new(),
            deterministic_line_fingerprints: BTreeSet::new(),
            input_text_preview: None,
            budget: MemoryRecallSynthesisBudget {
                max_items: 5,
                max_total_chars: 40,
                ..MemoryRecallSynthesisBudget::default()
            },
        });

        assert!(synthesis.truncated);
        assert_eq!(synthesis.truncated_item_count, 1);
        assert!(
            synthesis
                .rendered_text()
                .is_char_boundary(synthesis.rendered_text().len())
        );
    }

    #[test]
    fn recall_synthesis_diagnostics_are_not_prompt_text() {
        let synthesis = MemoryRecallSynthesizer::synthesize(MemoryRecallSynthesisInput {
            source: MemoryRecallSynthesisSource::Deterministic,
            items: vec![synthesis_item("mem_1", "User prefers concise context.")],
            deterministic_memory_ids: BTreeSet::new(),
            deterministic_line_fingerprints: BTreeSet::new(),
            input_text_preview: None,
            budget: MemoryRecallSynthesisBudget::default(),
        });

        let rendered = synthesis.rendered_text();
        assert!(!rendered.contains("memory.recall_synthesis"));
        assert!(!rendered.contains("source_ref"));
        assert!(!rendered.contains("mem_1"));
    }
}
