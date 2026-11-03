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
