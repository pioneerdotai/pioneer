use super::*;

const DEFAULT_SYNTHESIS_MAX_ITEMS: usize = 5;
const DEFAULT_SYNTHESIS_MAX_ITEM_CHARS: usize = 280;
const DEFAULT_SYNTHESIS_MAX_TOTAL_CHARS: usize = 1_500;
const DEFAULT_SYNTHESIS_MAX_DIAGNOSTICS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MemoryRecallSynthesisContextKind {
    Context,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    pub(super) fn active(
        items: Vec<MemoryRecallItem>,
        deterministic_memory_ids: BTreeSet<String>,
        deterministic_line_fingerprints: BTreeSet<String>,
        budget: MemoryRecallSynthesisBudget,
    ) -> Self {
        Self {
            source: MemoryRecallSynthesisSource::Active,
            items,
            deterministic_memory_ids,
            deterministic_line_fingerprints,
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

pub(super) fn memory_recall_source_ref(memory_id: &str) -> HookSourceRef {
    HookSourceRef {
        kind: HookSourceKind::Custom("memory".to_owned()),
        id: HookSourceId::new(memory_id).expect("memory id should be valid source id"),
        label: None,
    }
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
