use super::*;

pub(super) fn memory_tool_names_from_prompt_compile_input(
    input: &TurnPrePromptCompileHookInput,
) -> Vec<String> {
    let available = input
        .available_tool_names
        .iter()
        .map(|name| name.as_str())
        .collect::<BTreeSet<_>>();

    [
        MEMORY_SEARCH_TOOL,
        MEMORY_LIST_TOOL,
        MEMORY_GET_TOOL,
        MEMORY_REMEMBER_TOOL,
        MEMORY_FORGET_TOOL,
    ]
    .into_iter()
    .filter(|name| available.contains(name))
    .map(str::to_owned)
    .collect()
}

#[derive(Debug, Clone, Default)]
pub(super) struct MemoryRecallPromptContext {
    pub(super) deterministic_content: Option<String>,
    pub(super) active_content: Option<String>,
    pub(super) thread_content: Option<String>,
    pub(super) task_content: Option<String>,
    pub(super) count: usize,
    pub(super) deterministic_count: usize,
    pub(super) deterministic_memory_count: usize,
    pub(super) active_raw_count: usize,
    pub(super) active_duplicate_id_count: usize,
    pub(super) active_duplicate_line_count: usize,
    pub(super) active_rendered_count: usize,
    pub(super) active_synthesis_rendered: bool,
    pub(super) truncated: bool,
}

impl MemoryRecallPromptContext {
    pub(super) fn active_duplicate_count(&self) -> usize {
        self.active_duplicate_id_count + self.active_duplicate_line_count
    }

    pub(super) fn active_duplicate_only(&self) -> bool {
        self.active_raw_count > 0
            && self.active_rendered_count == 0
            && self.active_duplicate_count() > 0
    }
}

pub(super) fn memory_recall_context_from_prompt_context_set(
    prompt_context_set: &pioneer_hooks::HookPromptContextSet,
    prompt_policy: MemoryRecallPromptPolicy,
) -> MemoryRecallPromptContext {
    if prompt_policy == MemoryRecallPromptPolicy::ForgetOnly {
        return MemoryRecallPromptContext::default();
    }

    let mut context = MemoryRecallPromptContext::default();
    let mut deterministic_content = String::new();
    let mut active_content = String::new();
    let mut thread_content = String::new();
    let mut task_content = String::new();
    let mut deterministic_ids = BTreeSet::new();
    let mut seen_line_fingerprints = BTreeSet::new();
    let mut active_ids = BTreeSet::new();
    let mut consumed_thread_source_ids = BTreeSet::new();
    let mut thread_line_fingerprints = BTreeSet::new();
    for entry in prompt_context_set.entries() {
        if !is_memory_prompt_context_entry(entry) {
            continue;
        }
        match entry.contribution_id.as_str() {
            MEMORY_DETERMINISTIC_RECALL_CONTRIBUTION_ID => {
                let entry_content = entry.content.as_str().trim();
                if entry_content.is_empty() {
                    continue;
                }
                if !deterministic_content.is_empty() {
                    deterministic_content.push('\n');
                }
                deterministic_content.push_str(entry_content);
                context.count += 1;
                context.deterministic_count += 1;
                context.truncated |= entry.truncated;
                seen_line_fingerprints.extend(rendered_line_fingerprints(entry_content));
                for source_ref in &entry.source_refs {
                    if source_ref.kind.as_str() == "memory"
                        && deterministic_ids.insert(source_ref.id.as_str().to_owned())
                    {
                        context.deterministic_memory_count += 1;
                    }
                }
            }
            MEMORY_ACTIVE_RECALL_CONTRIBUTION_ID => {
                let entry_content = entry.content.as_str().trim();
                if entry_content.is_empty() {
                    continue;
                }
                context.count += 1;
                context.truncated |= entry.truncated;
                for line in active_memory_context_lines(entry_content) {
                    context.active_raw_count += 1;
                    let parsed_id = rendered_memory_line_id(line);
                    if let Some(memory_id) = parsed_id.as_deref() {
                        if deterministic_ids.contains(memory_id)
                            || !active_ids.insert(memory_id.to_owned())
                        {
                            context.active_duplicate_id_count += 1;
                            continue;
                        }
                    }
                    let Some(fingerprint) = rendered_line_fingerprint(line) else {
                        continue;
                    };
                    if !seen_line_fingerprints.insert(fingerprint) {
                        context.active_duplicate_line_count += 1;
                        continue;
                    }
                    if !active_content.is_empty() {
                        active_content.push('\n');
                    }
                    active_content.push_str(line);
                    consumed_thread_source_ids.extend(rendered_thread_source_ids(line));
                    context.active_rendered_count += 1;
                    context.active_synthesis_rendered |= parsed_id.is_none();
                }
            }
            MEMORY_THREAD_CONTEXT_CONTRIBUTION_ID
            | MEMORY_RELATED_THREAD_CONTEXT_CONTRIBUTION_ID
            | MEMORY_WORKSPACE_THREAD_CONTEXT_CONTRIBUTION_ID => {
                append_thread_prompt_context_entry(
                    &mut thread_content,
                    entry.content.as_str(),
                    &mut consumed_thread_source_ids,
                    &mut thread_line_fingerprints,
                );
                context.count += 1;
                context.truncated |= entry.truncated;
            }
            MEMORY_TASK_CONTEXT_CONTRIBUTION_ID => {
                append_prompt_context_entry(&mut task_content, entry.content.as_str());
                context.count += 1;
                context.truncated |= entry.truncated;
            }
            _ => {}
        }
    }
    context.deterministic_content = if deterministic_content.trim().is_empty() {
        None
    } else {
        Some(deterministic_content)
    };
    context.active_content = if active_content.trim().is_empty() {
        None
    } else {
        Some(active_content)
    };
    context.thread_content = if thread_content.trim().is_empty() {
        None
    } else {
        Some(thread_content)
    };
    context.task_content = if task_content.trim().is_empty() {
        None
    } else {
        Some(task_content)
    };
    context
}

fn is_memory_prompt_context_entry(entry: &pioneer_hooks::HookPromptContextEntry) -> bool {
    entry.domain.as_str() == MEMORY_POLICY_DOMAIN
        || matches!(
            entry.contribution_id.as_str(),
            MEMORY_THREAD_CONTEXT_CONTRIBUTION_ID
                | MEMORY_RELATED_THREAD_CONTEXT_CONTRIBUTION_ID
                | MEMORY_WORKSPACE_THREAD_CONTEXT_CONTRIBUTION_ID
                | MEMORY_TASK_CONTEXT_CONTRIBUTION_ID
        )
}

fn append_prompt_context_entry(output: &mut String, content: &str) {
    let content = content.trim();
    if content.is_empty() {
        return;
    }
    if !output.is_empty() {
        output.push('\n');
    }
    output.push_str(content);
}

fn append_thread_prompt_context_entry(
    output: &mut String,
    content: &str,
    consumed_thread_source_ids: &mut BTreeSet<String>,
    seen_line_fingerprints: &mut BTreeSet<String>,
) {
    for line in content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let source_ids = rendered_thread_source_ids(line);
        if !source_ids.is_empty()
            && source_ids
                .iter()
                .any(|source_id| consumed_thread_source_ids.contains(source_id))
        {
            continue;
        }
        let Some(fingerprint) = rendered_line_fingerprint(line) else {
            continue;
        };
        if !seen_line_fingerprints.insert(fingerprint) {
            continue;
        }
        consumed_thread_source_ids.extend(source_ids);
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str(line);
    }
}

fn rendered_thread_source_ids(line: &str) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    let mut remaining = line;
    while let Some(index) = remaining.find("thread:") {
        let candidate = &remaining[index..];
        let source_id = candidate
            .chars()
            .take_while(|ch| ch.is_ascii_alphanumeric() || matches!(*ch, ':' | '_' | '-' | '/'))
            .collect::<String>();
        if source_id.len() > "thread:".len() && source_id.matches('/').count() >= 2 {
            ids.insert(source_id.clone());
        }
        remaining = &candidate[source_id.len()..];
    }
    ids
}

pub(super) fn render_memory_manifest(manifest: &MemoryManifest) -> String {
    let mut lines = Vec::new();
    if manifest.active.is_empty() && manifest.candidates.is_empty() {
        lines.push("No active or candidate memories in the bounded manifest.".to_owned());
    }
    for item in &manifest.active {
        lines.push(format!(
            "- active id={} scope={} category={} key={} updated={} status={} content={}",
            item.memory_id,
            scope_label(&item.scope),
            category_label(item.category),
            item.key.as_deref().unwrap_or("none"),
            item.updated_at,
            memory_status_label(item.status),
            item.content_preview
        ));
    }
    for item in &manifest.candidates {
        lines.push(format!(
            "- candidate id={} scope={} category={} key={} created={} status={} content={}",
            item.candidate_id,
            scope_label(&item.scope),
            category_label(item.category),
            item.key.as_deref().unwrap_or("none"),
            item.created_at,
            candidate_status_label(item.status),
            item.content_preview
        ));
    }
    if manifest.truncated {
        lines.push("Manifest was truncated.".to_owned());
    }
    for diagnostic in &manifest.diagnostics {
        lines.push(format!(
            "Diagnostic: {}",
            safe_memory_policy_diagnostic(diagnostic)
        ));
    }
    lines.join("\n")
}
#[cfg(test)]
pub(crate) fn memory_recall_prompt_input(
    available_tool_names: Vec<String>,
    policy: MemoryRecallPromptPolicy,
    recall_snapshot: MemoryRecallSnapshot,
) -> MemoryRecallPromptInput {
    MemoryRecallPromptInput {
        available_tool_names,
        policy,
        recalled_items: recall_snapshot
            .items
            .into_iter()
            .map(memory_recall_prompt_item)
            .collect(),
        recalled_context: None,
        active_context: None,
        thread_context: None,
        task_context: None,
        truncated: recall_snapshot.truncated,
    }
}

pub(super) fn memory_prompt_section_contributions_from_context(
    available_tool_names: Vec<String>,
    policy: MemoryRecallPromptPolicy,
    recall_context: MemoryRecallPromptContext,
    truncated: bool,
) -> Vec<HookContribution> {
    let mut contributions = Vec::new();
    if let Some(contribution) =
        memory_recall_prompt_section_contribution_from_input(MemoryRecallPromptInput {
            available_tool_names,
            policy,
            recalled_items: Vec::new(),
            recalled_context: recall_context
                .deterministic_content
                .as_deref()
                .and_then(|content| {
                    MemoryRecallPromptContextBlock::from_text(content, recall_context.truncated)
                }),
            active_context: recall_context
                .active_content
                .as_deref()
                .and_then(|content| {
                    MemoryRecallPromptContextBlock::from_text(content, recall_context.truncated)
                }),
            thread_context: None,
            task_context: recall_context.task_content.as_deref().and_then(|content| {
                MemoryRecallPromptContextBlock::from_text(content, recall_context.truncated)
            }),
            truncated: truncated || recall_context.truncated,
        })
    {
        contributions.push(contribution);
    }
    if let Some(contribution) =
        thread_context_prompt_section_contribution_from_context(&recall_context, truncated)
    {
        contributions.push(contribution);
    }
    contributions
}

pub(super) fn memory_recall_prompt_section_contribution_from_input(
    prompt_input: MemoryRecallPromptInput,
) -> Option<HookContribution> {
    let prompt = render_memory_recall_prompt(&prompt_input)?;
    Some(HookContribution::PromptSection(PromptSectionContribution {
        contribution_id: HookContributionId::new(MEMORY_PROMPT_CONTRACT_CONTRIBUTION_ID)
            .expect("static contribution id is valid"),
        section_id: HookSectionId::new(MEMORY_PROMPT_CONTRACT_SECTION_ID)
            .expect("static section id is valid"),
        title: None,
        domain: HookDomain::new("memory").expect("static domain is valid"),
        priority: 500,
        content: HookPromptContent::new(prompt).ok()?,
        max_chars: None,
        source_refs: Vec::new(),
        diagnostics: Vec::new(),
        truncated: false,
    }))
}

fn thread_context_prompt_section_contribution_from_context(
    recall_context: &MemoryRecallPromptContext,
    truncated: bool,
) -> Option<HookContribution> {
    let context = recall_context
        .thread_content
        .as_deref()
        .and_then(|content| {
            MemoryRecallPromptContextBlock::from_text(content, recall_context.truncated)
        })?;
    let (prompt, section_truncated) =
        render_thread_context_prompt(&context, truncated || recall_context.truncated)?;
    Some(HookContribution::PromptSection(PromptSectionContribution {
        contribution_id: HookContributionId::new(MEMORY_THREAD_CONTEXT_PROMPT_CONTRIBUTION_ID)
            .expect("static contribution id is valid"),
        section_id: HookSectionId::new(MEMORY_THREAD_CONTEXT_PROMPT_SECTION_ID)
            .expect("static section id is valid"),
        title: Some(
            HookPromptSectionTitle::new(MEMORY_THREAD_CONTEXT_PROMPT_SECTION_TITLE)
                .expect("static section title is valid"),
        ),
        domain: HookDomain::new("thread_context").expect("static domain is valid"),
        priority: 490,
        content: HookPromptContent::new(prompt).ok()?,
        max_chars: None,
        source_refs: Vec::new(),
        diagnostics: Vec::new(),
        truncated: section_truncated,
    }))
}

#[cfg(test)]
pub(super) fn memory_recall_prompt_item(item: MemoryRecallItem) -> MemoryRecallPromptItem {
    MemoryRecallPromptItem {
        memory_id: item.memory_id,
        scope_label: scope_label(&item.scope),
        category_label: category_label(item.category).to_owned(),
        key: item.key,
        content: item.content,
        score: item.score,
        updated_at_label: date_label(item.updated_at),
    }
}
