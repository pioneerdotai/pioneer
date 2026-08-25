use crate::boundary::PROMT_CACHE_BOUNDARY;
use crate::bundle::{
    CompiledPromptBundle, PromptCompileInput, PromptSourceManifestEntry, PromptSourceStatus,
};
use crate::compile::policy;
use crate::constants::files::{BootstrapFileKind, CANONICAL_FILE_ORDER};
use crate::content;
use crate::diagnostics::{PromptDiagnostic, PromptDiagnosticCode};
use crate::fingerprint::sha256_hex;
use crate::profile::PromptProfile;
use crate::render::text::render_sections;
use crate::section::{
    DynamicPromptSectionInput, FILESYSTEM_CAPABILITY_RUNTIME_SECTION_ID,
    PromptRuntimeBuiltInSectionId, PromptRuntimeSectionId, PromptRuntimeSectionInput,
    PromptSection, PromptSectionId, PromptStability,
};
use crate::sources::budget::{BudgetedBootstrapFile, apply_budgets};
use crate::sources::files::load_bootstrap_files;

const DEFAULT_DYNAMIC_PROMPT_SECTION_MAX_CHARS: usize = 8_000;
const DEFAULT_DYNAMIC_PROMPT_SECTIONS_MAX_TOTAL_CHARS: usize = 16_000;
const DEFAULT_DYNAMIC_PROMPT_SECTIONS_MAX_COUNT: usize = 16;
const DEFAULT_RUNTIME_PROMPT_SECTIONS_MAX_TOTAL_CHARS: usize = 24_000;
const DEFAULT_AGENTS_MD_PROMPT_SECTION_MAX_CHARS: usize = 20_000;

fn build_identity_section() -> PromptSection {
    PromptSection {
        id: PromptSectionId::IdentityBase,
        stability: PromptStability::Stable,
        title: content::SECTION_TITLE_IDENTITY_BASE.to_owned(),
        content: content::IDENTITY_BASE_PROMPT.to_owned(),
        sources: Vec::new(),
    }
}

fn build_safety_section() -> PromptSection {
    PromptSection {
        id: PromptSectionId::AssistantSafety,
        stability: PromptStability::Stable,
        title: content::SECTION_TITLE_ASSISTANT_SAFETY.to_owned(),
        content: content::ASSISTANT_SAFETY_LINES.join("\n"),
        sources: Vec::new(),
    }
}

fn build_artifact_output_contract_section() -> PromptSection {
    PromptSection {
        id: PromptSectionId::ArtifactOutputContract,
        stability: PromptStability::Stable,
        title: content::SECTION_TITLE_ARTIFACT_OUTPUT_CONTRACT.to_owned(),
        content: content::ARTIFACT_OUTPUT_CONTRACT_PROMPT.to_owned(),
        sources: Vec::new(),
    }
}

fn build_tool_usage_policy_section() -> PromptSection {
    PromptSection {
        id: PromptSectionId::ToolUsagePolicy,
        stability: PromptStability::Stable,
        title: content::SECTION_TITLE_TOOL_USAGE_POLICY.to_owned(),
        content: content::TOOL_USAGE_POLICY_PROMPT.to_owned(),
        sources: Vec::new(),
    }
}

fn build_tool_recovery_policy_section() -> PromptSection {
    PromptSection {
        id: PromptSectionId::ToolRecoveryPolicy,
        stability: PromptStability::Stable,
        title: content::SECTION_TITLE_TOOL_RECOVERY_POLICY.to_owned(),
        content: content::TOOL_RECOVERY_POLICY_PROMPT.to_owned(),
        sources: Vec::new(),
    }
}

fn build_subagents_policy_section() -> PromptSection {
    PromptSection {
        id: PromptSectionId::SubagentsPolicy,
        stability: PromptStability::Dynamic,
        title: content::SECTION_TITLE_SUBAGENTS_POLICY.to_owned(),
        content: content::SUBAGENTS_POLICY_PROMPT.to_owned(),
        sources: Vec::new(),
    }
}

fn build_tasks_policy_section() -> PromptSection {
    PromptSection {
        id: PromptSectionId::TasksPolicy,
        stability: PromptStability::Dynamic,
        title: content::SECTION_TITLE_TASKS_POLICY.to_owned(),
        content: content::TASKS_POLICY_PROMPT.to_owned(),
        sources: Vec::new(),
    }
}

fn format_file_block(file: &BudgetedBootstrapFile, include_evolution_note: bool) -> String {
    let mut block = content::IDENTITY_FILE_BLOCK_TEMPLATE
        .replace(content::IDENTITY_FILE_BLOCK_NAME_TOKEN, file.name.as_str())
        .replace(
            content::IDENTITY_FILE_BLOCK_PATH_TOKEN,
            file.path.display().to_string().as_str(),
        )
        .replace(
            content::IDENTITY_FILE_BLOCK_CONTENT_TOKEN,
            file.content.trim(),
        );

    if include_evolution_note {
        if file.content.trim().is_empty() {
            block.push('\n');
        } else {
            block.push_str("\n\n");
        }
        block.push_str(content::IDENTITY_FILE_EVOLUTION_NOTE_TEMPLATE);
    }

    block
}

fn build_identity_file_section(
    id: PromptSectionId,
    title: &str,
    file: &BudgetedBootstrapFile,
) -> Option<PromptSection> {
    if file.content.trim().is_empty()
        && !matches!(
            id,
            PromptSectionId::SoulCore | PromptSectionId::IdentityCore
        )
    {
        return None;
    }

    let include_evolution_note = matches!(
        id,
        PromptSectionId::SoulCore | PromptSectionId::IdentityCore
    );

    Some(PromptSection {
        id,
        stability: PromptStability::Stable,
        title: title.to_owned(),
        content: format_file_block(file, include_evolution_note),
        sources: vec![file.path.display().to_string()],
    })
}

fn diagnostic_matches_file(diagnostic: &PromptDiagnostic, canonical_name: &str) -> bool {
    diagnostic
        .file
        .as_deref()
        .is_some_and(|file| file.ends_with(canonical_name))
}

fn build_source_manifest(
    workspace_root: &std::path::Path,
    profile: PromptProfile,
    files: &[BudgetedBootstrapFile],
    diagnostics: &[PromptDiagnostic],
) -> Vec<PromptSourceManifestEntry> {
    if !policy::include_workspace_context(profile) {
        return Vec::new();
    }

    let mut manifest = Vec::with_capacity(CANONICAL_FILE_ORDER.len());

    for kind in CANONICAL_FILE_ORDER {
        let canonical_name = kind.canonical_name();
        let entry_path = workspace_root.join(canonical_name).display().to_string();
        let has_truncation_diagnostic = diagnostics.iter().any(|diagnostic| {
            diagnostic_matches_file(diagnostic, canonical_name)
                && matches!(
                    diagnostic.code,
                    PromptDiagnosticCode::FileTruncated
                        | PromptDiagnosticCode::TotalBudgetTruncated
                )
        });

        if diagnostics.iter().any(|diagnostic| {
            diagnostic.code == PromptDiagnosticCode::MissingFile
                && diagnostic_matches_file(diagnostic, canonical_name)
        }) {
            manifest.push(PromptSourceManifestEntry {
                file: canonical_name.to_owned(),
                path: entry_path,
                status: PromptSourceStatus::Missing,
                chars: 0,
            });
            continue;
        }

        if diagnostics.iter().any(|diagnostic| {
            diagnostic.code == PromptDiagnosticCode::FileReadError
                && diagnostic_matches_file(diagnostic, canonical_name)
        }) {
            manifest.push(PromptSourceManifestEntry {
                file: canonical_name.to_owned(),
                path: entry_path,
                status: PromptSourceStatus::ReadError,
                chars: 0,
            });
            continue;
        }

        if let Some(file) = files.iter().find(|file| file.kind == kind) {
            manifest.push(PromptSourceManifestEntry {
                file: canonical_name.to_owned(),
                path: file.path.display().to_string(),
                status: if has_truncation_diagnostic {
                    PromptSourceStatus::Truncated
                } else {
                    PromptSourceStatus::Loaded
                },
                chars: file.content.chars().count(),
            });
            continue;
        }

        if has_truncation_diagnostic {
            manifest.push(PromptSourceManifestEntry {
                file: canonical_name.to_owned(),
                path: entry_path,
                status: PromptSourceStatus::Truncated,
                chars: 0,
            });
            continue;
        }

        manifest.push(PromptSourceManifestEntry {
            file: canonical_name.to_owned(),
            path: entry_path,
            status: PromptSourceStatus::Missing,
            chars: 0,
        });
    }

    manifest
}

fn build_dynamic_prompt_section(
    input: &DynamicPromptSectionInput,
    diagnostics: &mut Vec<PromptDiagnostic>,
    remaining_total_chars: &mut usize,
) -> Option<PromptSection> {
    build_runtime_prompt_section(
        &PromptRuntimeSectionInput::dynamic(input.clone()),
        diagnostics,
        remaining_total_chars,
    )
}

fn build_runtime_prompt_section(
    input: &PromptRuntimeSectionInput,
    diagnostics: &mut Vec<PromptDiagnostic>,
    remaining_total_chars: &mut usize,
) -> Option<PromptSection> {
    let section_id = input.id.manifest_id();
    let content = input.content.trim();
    if content.is_empty() {
        diagnostics.push(PromptDiagnostic::dynamic_section_omitted(
            section_id.as_str(),
            "content was empty",
        ));
        return None;
    }

    if *remaining_total_chars == 0 {
        diagnostics.push(PromptDiagnostic::dynamic_section_omitted(
            section_id.as_str(),
            "dynamic section budget was exhausted",
        ));
        return None;
    }

    let default_max_chars = runtime_section_default_max_chars(&input.id);
    let section_limit = input
        .max_chars
        .unwrap_or(default_max_chars)
        .min(default_max_chars)
        .min(*remaining_total_chars);

    if section_limit == 0 {
        diagnostics.push(PromptDiagnostic::dynamic_section_omitted(
            section_id.as_str(),
            "section character limit was zero",
        ));
        return None;
    }

    let content_chars = content.chars().count();
    let mut truncated = input.truncated;
    let content = if content_chars > section_limit {
        truncated = true;
        content.chars().take(section_limit).collect::<String>()
    } else {
        content.to_owned()
    };

    if truncated {
        diagnostics.push(PromptDiagnostic::dynamic_section_truncated(
            section_id.as_str(),
            content_chars,
            content.chars().count(),
        ));
    }

    *remaining_total_chars = remaining_total_chars.saturating_sub(content.chars().count());

    let title = input
        .title
        .as_deref()
        .map(str::trim)
        .filter(|title| !title.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| input.id.default_title());

    Some(PromptSection {
        id: input.id.prompt_section_id(),
        stability: PromptStability::Dynamic,
        title,
        content,
        sources: Vec::new(),
    })
}

fn runtime_section_default_max_chars(id: &PromptRuntimeSectionId) -> usize {
    match id {
        PromptRuntimeSectionId::BuiltIn(
            PromptRuntimeBuiltInSectionId::PioneerCliRuntimeInstructions,
        )
        | PromptRuntimeSectionId::BuiltIn(
            PromptRuntimeBuiltInSectionId::PioneerCliRuntimeContext,
        )
        | PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::ThreadContext)
        | PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::SelectedSkills)
        | PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::SelectedCapabilities)
        | PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::CurrentPermissions) => {
            DEFAULT_DYNAMIC_PROMPT_SECTION_MAX_CHARS
        }
        PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::AgentsMd) => {
            DEFAULT_AGENTS_MD_PROMPT_SECTION_MAX_CHARS
        }
        PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::MemoryRecall)
        | PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::ExecutionContinuation)
        | PromptRuntimeSectionId::Dynamic(_) => DEFAULT_DYNAMIC_PROMPT_SECTION_MAX_CHARS,
    }
}

fn runtime_section_order(id: &PromptRuntimeSectionId) -> u8 {
    match id {
        PromptRuntimeSectionId::BuiltIn(
            PromptRuntimeBuiltInSectionId::PioneerCliRuntimeInstructions,
        ) => 0,
        PromptRuntimeSectionId::BuiltIn(
            PromptRuntimeBuiltInSectionId::PioneerCliRuntimeContext,
        ) => 1,
        PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::AgentsMd) => 2,
        PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::MemoryRecall) => 3,
        PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::ThreadContext) => 4,
        PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::SelectedSkills) => 5,
        PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::SelectedCapabilities) => 6,
        PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::CurrentPermissions) => 7,
        PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::ExecutionContinuation) => 8,
        PromptRuntimeSectionId::Dynamic(dynamic_id)
            if dynamic_id.as_str() == FILESYSTEM_CAPABILITY_RUNTIME_SECTION_ID =>
        {
            0
        }
        PromptRuntimeSectionId::Dynamic(_) => 9,
    }
}

fn build_dynamic_prompt_sections(
    dynamic_sections: &[DynamicPromptSectionInput],
    diagnostics: &mut Vec<PromptDiagnostic>,
) -> Vec<PromptSection> {
    let mut sections = Vec::new();
    let mut remaining_total_chars = DEFAULT_DYNAMIC_PROMPT_SECTIONS_MAX_TOTAL_CHARS;

    for input in dynamic_sections {
        if sections.len() >= DEFAULT_DYNAMIC_PROMPT_SECTIONS_MAX_COUNT {
            diagnostics.push(PromptDiagnostic::dynamic_section_omitted(
                input.id.as_str(),
                "dynamic section count limit was reached",
            ));
            continue;
        }

        if let Some(section) =
            build_dynamic_prompt_section(input, diagnostics, &mut remaining_total_chars)
        {
            sections.push(section);
        }
    }

    sections
}

fn build_runtime_prompt_sections(
    runtime_sections: &[PromptRuntimeSectionInput],
    diagnostics: &mut Vec<PromptDiagnostic>,
) -> Vec<PromptSection> {
    let mut sections = Vec::new();
    let mut remaining_total_chars = DEFAULT_RUNTIME_PROMPT_SECTIONS_MAX_TOTAL_CHARS;

    let mut ordered_runtime_sections = runtime_sections.iter().enumerate().collect::<Vec<_>>();
    ordered_runtime_sections
        .sort_by_key(|(index, input)| (runtime_section_order(&input.id), *index));

    for (_, input) in ordered_runtime_sections {
        if sections.len() >= DEFAULT_DYNAMIC_PROMPT_SECTIONS_MAX_COUNT {
            let section_id = input.id.manifest_id();
            diagnostics.push(PromptDiagnostic::dynamic_section_omitted(
                section_id.as_str(),
                "dynamic section count limit was reached",
            ));
            continue;
        }

        if let Some(section) =
            build_runtime_prompt_section(input, diagnostics, &mut remaining_total_chars)
        {
            sections.push(section);
        }
    }

    sections
}

fn build_runtime_dynamic_sections(
    input: &PromptCompileInput,
    diagnostics: &mut Vec<PromptDiagnostic>,
) -> Vec<PromptSection> {
    let mut sections = Vec::new();

    sections.extend(build_runtime_prompt_sections(
        input.runtime_sections.as_slice(),
        diagnostics,
    ));

    sections.extend(build_dynamic_prompt_sections(
        input.dynamic_sections.as_slice(),
        diagnostics,
    ));

    if input.continue_generation_hint {
        sections.push(PromptSection {
            id: PromptSectionId::RecoveryContinuation,
            stability: PromptStability::Dynamic,
            title: content::SECTION_TITLE_RECOVERY_CONTINUATION.to_owned(),
            content: content::RECOVERY_CONTINUATION_PROMPT.to_owned(),
            sources: Vec::new(),
        });
    }

    if let Some(skills_prompt) = input.skills_prompt.as_deref().map(str::trim)
        && !skills_prompt.is_empty()
    {
        sections.push(PromptSection {
            id: PromptSectionId::SkillsRuntimePrompt,
            stability: PromptStability::Dynamic,
            title: content::SECTION_TITLE_SKILLS_RUNTIME.to_owned(),
            content: skills_prompt.to_owned(),
            sources: Vec::new(),
        });
    }

    if let Some(retry_instruction) = input.retry_instruction.as_deref().map(str::trim)
        && !retry_instruction.is_empty()
    {
        sections.push(PromptSection {
            id: PromptSectionId::RetryRuntimeInstruction,
            stability: PromptStability::Dynamic,
            title: content::SECTION_TITLE_RETRY_INSTRUCTION.to_owned(),
            content: retry_instruction.to_owned(),
            sources: Vec::new(),
        });
    }

    if let Some(dynamic_context) = input.dynamic_context.as_deref().map(str::trim)
        && !dynamic_context.is_empty()
    {
        sections.push(PromptSection {
            id: PromptSectionId::DynamicContext,
            stability: PromptStability::Dynamic,
            title: content::SECTION_TITLE_DYNAMIC_CONTEXT.to_owned(),
            content: dynamic_context.to_owned(),
            sources: Vec::new(),
        });
    }

    if let Some(extra_system) = input.extra_system.as_deref().map(str::trim)
        && !extra_system.is_empty()
    {
        sections.push(PromptSection {
            id: PromptSectionId::ExtraSystem,
            stability: PromptStability::Dynamic,
            title: content::SECTION_TITLE_EXTRA_SYSTEM.to_owned(),
            content: extra_system.to_owned(),
            sources: Vec::new(),
        });
    }

    sections
}

pub fn compile_prompt(input: PromptCompileInput) -> anyhow::Result<CompiledPromptBundle> {
    let profile = input.profile;
    let mut diagnostics = Vec::<PromptDiagnostic>::new();
    let mut source_manifest = Vec::<PromptSourceManifestEntry>::new();

    let mut sections = Vec::<PromptSection>::new();

    if policy::include_identity_base(profile) {
        sections.push(build_identity_section());
    }

    if policy::include_safety(profile) {
        sections.push(build_safety_section());
    }

    if policy::include_artifact_output_contract(profile) {
        sections.push(build_artifact_output_contract_section());
    }

    let has_runtime_filesystem_capability = input.runtime_sections.iter().any(|section| {
        section.id.manifest_id() == crate::section::FILESYSTEM_CAPABILITY_RUNTIME_SECTION_ID
    });
    if policy::include_tool_usage_policy(profile) && !has_runtime_filesystem_capability {
        sections.push(build_tool_usage_policy_section());
    }

    if policy::include_workspace_context(profile) {
        let (loaded_files, mut file_diagnostics) =
            load_bootstrap_files(input.workspace_root.as_path(), profile);

        diagnostics.append(&mut file_diagnostics);

        let (budgeted_files, mut budget_diagnostics) = apply_budgets(
            loaded_files,
            input.limits.max_chars_per_file,
            input.limits.max_chars_total,
        );

        diagnostics.append(&mut budget_diagnostics);

        source_manifest = build_source_manifest(
            input.workspace_root.as_path(),
            profile,
            budgeted_files.as_slice(),
            diagnostics.as_slice(),
        );

        let mut soul_file: Option<BudgetedBootstrapFile> = None;
        let mut identity_file: Option<BudgetedBootstrapFile> = None;
        let mut user_file: Option<BudgetedBootstrapFile> = None;

        for file in budgeted_files {
            match file.kind {
                BootstrapFileKind::Soul => soul_file = Some(file),
                BootstrapFileKind::Identity => identity_file = Some(file),
                BootstrapFileKind::User => user_file = Some(file),
            }
        }

        if let Some(file) = soul_file.as_ref()
            && let Some(section) = build_identity_file_section(
                PromptSectionId::SoulCore,
                content::SECTION_TITLE_SOUL_CORE,
                file,
            )
        {
            sections.push(section);
        }

        if let Some(file) = identity_file.as_ref()
            && let Some(section) = build_identity_file_section(
                PromptSectionId::IdentityCore,
                content::SECTION_TITLE_IDENTITY_CORE,
                file,
            )
        {
            sections.push(section);
        }

        if let Some(file) = user_file.as_ref()
            && let Some(section) = build_identity_file_section(
                PromptSectionId::UserPersona,
                content::SECTION_TITLE_USER_PERSONA,
                file,
            )
        {
            sections.push(section);
        }
    }

    if policy::include_tool_recovery_policy(profile, input.include_tool_recovery_policy) {
        sections.push(build_tool_recovery_policy_section());
    }

    if input.include_task_orchestration_policy {
        sections.push(build_subagents_policy_section());
        sections.push(build_tasks_policy_section());
    }

    sections.extend(build_runtime_dynamic_sections(&input, &mut diagnostics));

    if profile == PromptProfile::AssistantNone {
        sections.retain(|section| section.id == PromptSectionId::IdentityBase);
    }

    let stable_sections = sections
        .iter()
        .filter(|section| section.stability == PromptStability::Stable)
        .cloned()
        .collect::<Vec<_>>();
    let dynamic_sections = sections
        .iter()
        .filter(|section| section.stability == PromptStability::Dynamic)
        .cloned()
        .collect::<Vec<_>>();

    let stable_system_text = render_sections(stable_sections.as_slice());
    let dynamic_system_text = render_sections(dynamic_sections.as_slice());

    let full_system_text = match (
        stable_system_text.trim().is_empty(),
        dynamic_system_text.trim().is_empty(),
    ) {
        (true, true) => String::new(),
        (false, true) => stable_system_text.clone(),
        (true, false) => format!("{PROMT_CACHE_BOUNDARY}{dynamic_system_text}"),
        (false, false) => {
            format!("{stable_system_text}{PROMT_CACHE_BOUNDARY}{dynamic_system_text}")
        }
    };

    Ok(CompiledPromptBundle {
        compiler_version: env!("CARGO_PKG_VERSION"),
        profile,
        full_system_text: full_system_text.clone(),
        stable_system_text: stable_system_text.clone(),
        dynamic_system_text: dynamic_system_text.clone(),
        boundary_marker: PROMT_CACHE_BOUNDARY,
        fingerprint_stable: sha256_hex(stable_system_text.as_str()),
        fingerprint_dynamic: sha256_hex(dynamic_system_text.as_str()),
        fingerprint_full: sha256_hex(full_system_text.as_str()),
        sections,
        source_manifest,
        diagnostics,
    })
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_DYNAMIC_PROMPT_SECTIONS_MAX_COUNT, compile_prompt};
    use crate::bundle::{PromptCompileInput, PromptLimits};
    use crate::diagnostics::PromptDiagnosticCode;
    use crate::profile::PromptProfile;
    use crate::section::{
        DynamicPromptSectionInput, FILESYSTEM_CAPABILITY_RUNTIME_SECTION_ID,
        PromptDynamicSectionId, PromptRuntimeBuiltInSectionId, PromptRuntimeSectionId,
        PromptRuntimeSectionInput, PromptSectionId, PromptStability,
    };

    fn temp_workspace(name: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "pioneer_promt_compile_{name}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp workspace");
        root
    }

    fn dynamic_section(id: &str, title: Option<&str>, content: &str) -> DynamicPromptSectionInput {
        DynamicPromptSectionInput {
            id: PromptDynamicSectionId::new(id).expect("valid dynamic section id"),
            title: title.map(str::to_owned),
            content: content.to_owned(),
            max_chars: None,
            truncated: false,
        }
    }

    fn memory_recall_runtime_section(content: &str) -> PromptRuntimeSectionInput {
        PromptRuntimeSectionInput {
            id: PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::MemoryRecall),
            title: None,
            content: content.to_owned(),
            max_chars: None,
            truncated: false,
        }
    }

    fn agents_md_runtime_section(
        content: &str,
        max_chars: Option<usize>,
    ) -> PromptRuntimeSectionInput {
        PromptRuntimeSectionInput {
            id: PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::AgentsMd),
            title: None,
            content: content.to_owned(),
            max_chars,
            truncated: false,
        }
    }

    fn execution_continuation_runtime_section(content: &str) -> PromptRuntimeSectionInput {
        PromptRuntimeSectionInput {
            id: PromptRuntimeSectionId::BuiltIn(
                PromptRuntimeBuiltInSectionId::ExecutionContinuation,
            ),
            title: None,
            content: content.to_owned(),
            max_chars: None,
            truncated: false,
        }
    }

    #[test]
    fn deterministic_output_for_same_input() {
        let root = temp_workspace("deterministic");
        std::fs::write(root.join("SOUL.md"), "Voice: direct and concise").expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "Name: Pioneer").expect("write IDENTITY");
        std::fs::write(root.join("USER.md"), "Name: Alex").expect("write USER");

        let input = PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: Some("[Skills]\n- skill-a".to_owned()),
            retry_instruction: Some("retry with corrected arguments".to_owned()),
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: true,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: Some("dynamic".to_owned()),
            extra_system: None,
            limits: PromptLimits::default(),
        };

        let first = compile_prompt(input.clone()).expect("compile 1");
        let second = compile_prompt(input).expect("compile 2");
        assert_eq!(first.full_system_text, second.full_system_text);
        assert_eq!(first.fingerprint_full, second.fingerprint_full);
        assert_eq!(first.source_manifest, second.source_manifest);
        assert_eq!(first.diagnostics, second.diagnostics);
    }

    #[test]
    fn assistant_none_keeps_only_identity() {
        let root = temp_workspace("none");
        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantNone,
            skills_prompt: Some("[Skills]\n- skill-a".to_owned()),
            retry_instruction: Some("retry".to_owned()),
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: true,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: Some("dynamic".to_owned()),
            extra_system: Some("extra".to_owned()),
            limits: PromptLimits::default(),
        })
        .expect("compile");

        assert!(compiled.stable_system_text.contains("personal assistant"));
        assert!(compiled.dynamic_system_text.is_empty());
    }

    #[test]
    fn runtime_section_order_is_deterministic() {
        let root = temp_workspace("runtime_order");
        std::fs::write(root.join("SOUL.md"), "Voice: direct and concise").expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "Name: Pioneer").expect("write IDENTITY");
        std::fs::write(root.join("USER.md"), "Name: Alex").expect("write USER");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: Some("[Skills]\n- skill-a".to_owned()),
            retry_instruction: Some("retry".to_owned()),
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: true,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: Some("ctx".to_owned()),
            extra_system: Some("extra".to_owned()),
            limits: PromptLimits::default(),
        })
        .expect("compile");

        let section_ids = compiled
            .sections
            .iter()
            .map(|section| section.id.clone())
            .collect::<Vec<_>>();

        assert_eq!(
            section_ids,
            vec![
                PromptSectionId::IdentityBase,
                PromptSectionId::AssistantSafety,
                PromptSectionId::ArtifactOutputContract,
                PromptSectionId::ToolUsagePolicy,
                PromptSectionId::SoulCore,
                PromptSectionId::IdentityCore,
                PromptSectionId::UserPersona,
                PromptSectionId::ToolRecoveryPolicy,
                PromptSectionId::RecoveryContinuation,
                PromptSectionId::SkillsRuntimePrompt,
                PromptSectionId::RetryRuntimeInstruction,
                PromptSectionId::DynamicContext,
                PromptSectionId::ExtraSystem,
            ]
        );
    }

    #[test]
    fn native_filesystem_capability_replaces_static_tool_policy_and_is_prioritized() {
        let root = temp_workspace("filesystem_capability");
        let capability_id = PromptDynamicSectionId::new(FILESYSTEM_CAPABILITY_RUNTIME_SECTION_ID)
            .expect("filesystem capability section id");
        let mut runtime_sections = vec![
            memory_recall_runtime_section("memory recall"),
            PromptRuntimeSectionInput {
                id: PromptRuntimeSectionId::Dynamic(capability_id),
                title: Some("Native Filesystem Capability".to_owned()),
                content: "capability-specific filesystem contract".to_owned(),
                max_chars: None,
                truncated: false,
            },
        ];
        for index in 0..DEFAULT_DYNAMIC_PROMPT_SECTIONS_MAX_COUNT {
            runtime_sections.push(PromptRuntimeSectionInput {
                id: PromptRuntimeSectionId::Dynamic(
                    PromptDynamicSectionId::new(format!("extra_{index}"))
                        .expect("valid extra section id"),
                ),
                title: None,
                content: "extra".to_owned(),
                max_chars: None,
                truncated: false,
            });
        }

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: false,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections,
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        assert!(
            compiled
                .full_system_text
                .contains("capability-specific filesystem contract")
        );
        assert!(
            !compiled
                .full_system_text
                .contains(crate::content::TOOL_USAGE_POLICY_PROMPT)
        );
        assert_eq!(
            compiled
                .sections
                .iter()
                .filter(
                    |section| section.id.manifest_id() == FILESYSTEM_CAPABILITY_RUNTIME_SECTION_ID
                )
                .count(),
            1
        );
    }

    #[test]
    fn subagents_and_tasks_policies_are_dynamic_and_opt_in() {
        let root = temp_workspace("subagents_tasks_policies");
        std::fs::write(root.join("SOUL.md"), "Voice: direct and concise").expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "Name: Pioneer").expect("write IDENTITY");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: true,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        let subagents_section = compiled
            .sections
            .iter()
            .find(|section| section.id == PromptSectionId::SubagentsPolicy)
            .expect("subagents section should be present");
        assert_eq!(
            subagents_section.stability,
            crate::section::PromptStability::Dynamic
        );
        let tasks_section = compiled
            .sections
            .iter()
            .find(|section| section.id == PromptSectionId::TasksPolicy)
            .expect("tasks section should be present");
        assert_eq!(
            tasks_section.stability,
            crate::section::PromptStability::Dynamic
        );
        assert!(compiled.dynamic_system_text.contains("## Subagents"));
        assert!(compiled.dynamic_system_text.contains("## Tasks"));
        assert!(
            compiled
                .dynamic_system_text
                .contains("find `pioneer/subagents` in Internal Skill References")
        );
        assert!(
            compiled
                .dynamic_system_text
                .contains("find `pioneer/tasks` in Internal Skill References")
        );
        assert!(
            compiled
                .dynamic_system_text
                .contains("Do not finish the parent turn while attached subagent work")
        );
        assert!(
            compiled
                .dynamic_system_text
                .contains("Delivery is separate from execution")
        );
        assert!(
            !compiled
                .dynamic_system_text
                .contains("## Task Orchestration")
        );
        assert!(!compiled.dynamic_system_text.contains("task_create"));
        assert!(!compiled.dynamic_system_text.contains("task_accept"));
    }

    #[test]
    fn tool_usage_policy_renders_apply_patch_guidance() {
        let root = temp_workspace("tool_usage_policy");
        std::fs::write(root.join("SOUL.md"), "Voice: direct and concise").expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "Name: Pioneer").expect("write IDENTITY");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        let tool_usage_section = compiled
            .sections
            .iter()
            .find(|section| section.id == PromptSectionId::ToolUsagePolicy)
            .expect("tool usage section should be present");
        assert_eq!(tool_usage_section.stability, PromptStability::Stable);
        assert!(compiled.stable_system_text.contains("## Tool Usage"));
        assert!(
            compiled
                .stable_system_text
                .contains("`apply_patch` as the only general text mutator")
        );
        assert!(
            compiled
                .stable_system_text
                .contains("exact version token returned by `read_file`")
        );
        assert!(compiled.stable_system_text.contains("source code"));
        assert!(compiled.stable_system_text.contains("configs"));
        assert!(compiled.stable_system_text.contains("If-Match"));
        assert!(
            compiled
                .stable_system_text
                .contains("Use `write_stdin` only")
        );
        assert!(
            compiled
                .stable_system_text
                .contains("Do not use `exec_command`, sed, perl")
        );
        assert!(compiled.stable_system_text.contains("ordinary file edits"));
        assert!(
            compiled
                .stable_system_text
                .contains("structured patch result")
        );
        assert!(!compiled.stable_system_text.contains("write_file"));
        assert!(!compiled.stable_system_text.contains("edit_file"));
        assert!(!compiled.stable_system_text.contains("partial edits"));
    }

    #[test]
    fn memory_recall_section_is_dynamic_and_opt_in() {
        let root = temp_workspace("memory_recall");
        std::fs::write(root.join("SOUL.md"), "Voice: direct and concise").expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "Name: Pioneer").expect("write IDENTITY");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: vec![memory_recall_runtime_section(
                "Available memory tools: memory_search",
            )],
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        let memory_section = compiled
            .sections
            .iter()
            .find(|section| section.id == PromptSectionId::MemoryRecall)
            .expect("memory recall section should be present");
        assert_eq!(
            memory_section.stability,
            crate::section::PromptStability::Dynamic
        );
        assert!(compiled.dynamic_system_text.contains("## Memory Recall"));
        assert!(
            compiled
                .dynamic_system_text
                .contains("Available memory tools: memory_search")
        );
    }

    #[test]
    fn agents_md_runtime_section_renders_as_builtin_dynamic_section() {
        let root = temp_workspace("agents_md_runtime");
        std::fs::write(root.join("SOUL.md"), "Voice: direct and concise").expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "Name: Pioneer").expect("write IDENTITY");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: vec![agents_md_runtime_section(
                "Follow project-specific instructions.",
                Some(20_000),
            )],
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        let agents_section = compiled
            .sections
            .iter()
            .find(|section| section.id == PromptSectionId::AgentsMd)
            .expect("AGENTS.md section should be present");
        assert_eq!(agents_section.title, "AGENTS.md");
        assert_eq!(
            agents_section.stability,
            crate::section::PromptStability::Dynamic
        );
        assert!(compiled.dynamic_system_text.contains("## AGENTS.md"));
        assert!(
            compiled
                .dynamic_system_text
                .contains("Follow project-specific instructions.")
        );
        assert!(
            compiled
                .sections
                .iter()
                .any(|section| section.id.manifest_id() == "agents_md"),
            "manifest section ids should include agents_md"
        );
    }

    #[test]
    fn agents_md_runtime_section_omits_empty_content_with_diagnostic() {
        let root = temp_workspace("agents_md_empty");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: vec![agents_md_runtime_section("   ", Some(20_000))],
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        assert!(!compiled.dynamic_system_text.contains("## AGENTS.md"));
        assert!(compiled.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == PromptDiagnosticCode::DynamicSectionOmitted
                && diagnostic.section_id.as_deref() == Some("agents_md")
        }));
    }

    #[test]
    fn agents_md_runtime_section_truncates_at_builtin_limit_with_diagnostic() {
        let root = temp_workspace("agents_md_truncated");
        let content = "a".repeat(20_005);

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: vec![agents_md_runtime_section(content.as_str(), Some(25_000))],
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        let agents_section = compiled
            .sections
            .iter()
            .find(|section| section.id == PromptSectionId::AgentsMd)
            .expect("AGENTS.md section should be present");
        assert_eq!(agents_section.content.chars().count(), 20_000);
        assert!(compiled.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == PromptDiagnosticCode::DynamicSectionTruncated
                && diagnostic.section_id.as_deref() == Some("agents_md")
        }));
    }

    #[test]
    fn agents_md_runtime_section_orders_before_other_runtime_sections() {
        let root = temp_workspace("agents_md_ordering");
        let runtime_dynamic = PromptRuntimeSectionInput {
            id: PromptRuntimeSectionId::Dynamic(
                PromptDynamicSectionId::new("runtime.custom").expect("valid runtime id"),
            ),
            title: Some("Runtime Custom".to_owned()),
            content: "runtime custom content".to_owned(),
            max_chars: None,
            truncated: false,
        };

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: vec![
                runtime_dynamic,
                execution_continuation_runtime_section("execution continuation content"),
                memory_recall_runtime_section("memory recall content"),
                agents_md_runtime_section("agents content", Some(20_000)),
            ],
            dynamic_sections: vec![dynamic_section(
                "test.phase10.after",
                Some("After"),
                "after",
            )],
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        let section_ids = compiled
            .sections
            .iter()
            .map(|section| section.id.manifest_id())
            .collect::<Vec<_>>();
        let agents_index = section_ids
            .iter()
            .position(|id| id == "agents_md")
            .expect("agents_md section present");
        let memory_index = section_ids
            .iter()
            .position(|id| id == "memory_recall")
            .expect("memory recall section present");
        let runtime_dynamic_index = section_ids
            .iter()
            .position(|id| id == "runtime.custom")
            .expect("runtime dynamic section present");
        let execution_continuation_index = section_ids
            .iter()
            .position(|id| id == "execution_continuation")
            .expect("execution continuation section present");
        let hook_index = section_ids
            .iter()
            .position(|id| id == "test.phase10.after")
            .expect("dynamic section present");

        assert!(agents_index < memory_index);
        assert!(memory_index < execution_continuation_index);
        assert!(execution_continuation_index < runtime_dynamic_index);
        assert!(memory_index < runtime_dynamic_index);
        assert!(runtime_dynamic_index < hook_index);
    }

    #[test]
    fn dynamic_prompt_section_appears_in_compiled_prompt() {
        let root = temp_workspace("dynamic_prompt_section");
        std::fs::write(root.join("SOUL.md"), "Voice: direct and concise").expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "Name: Pioneer").expect("write IDENTITY");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: vec![dynamic_section(
                "test.phase10.alpha",
                Some("Alpha Hook Section"),
                "alpha hook section content",
            )],
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        assert!(
            compiled
                .dynamic_system_text
                .contains("## Alpha Hook Section")
        );
        assert!(
            compiled
                .dynamic_system_text
                .contains("alpha hook section content")
        );
        assert!(
            compiled
                .sections
                .iter()
                .any(|section| { section.id.manifest_id() == "test.phase10.alpha" })
        );
    }

    #[test]
    fn dynamic_prompt_sections_preserve_input_order_and_slot() {
        let root = temp_workspace("dynamic_prompt_section_order");
        std::fs::write(root.join("SOUL.md"), "Voice: direct and concise").expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "Name: Pioneer").expect("write IDENTITY");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: Some("[Skills]\n- skill-a".to_owned()),
            retry_instruction: Some("retry".to_owned()),
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: true,
            runtime_sections: vec![memory_recall_runtime_section("memory recall content")],
            dynamic_sections: vec![
                dynamic_section("test.phase10.first", Some("First Hook"), "first content"),
                dynamic_section("test.phase10.second", Some("Second Hook"), "second content"),
            ],
            dynamic_context: Some("ctx".to_owned()),
            extra_system: Some("extra".to_owned()),
            limits: PromptLimits::default(),
        })
        .expect("compile");

        let section_ids = compiled
            .sections
            .iter()
            .map(|section| section.id.manifest_id())
            .collect::<Vec<_>>();
        let memory_index = section_ids
            .iter()
            .position(|id| id == "memory_recall")
            .expect("memory recall section present");
        let first_index = section_ids
            .iter()
            .position(|id| id == "test.phase10.first")
            .expect("first dynamic section present");
        let second_index = section_ids
            .iter()
            .position(|id| id == "test.phase10.second")
            .expect("second dynamic section present");
        let recovery_index = section_ids
            .iter()
            .position(|id| id == "recovery_continuation")
            .expect("recovery continuation section present");

        assert!(memory_index < first_index);
        assert!(first_index < second_index);
        assert!(second_index < recovery_index);
    }

    #[test]
    fn dynamic_prompt_section_changes_dynamic_fingerprint_only() {
        let root = temp_workspace("dynamic_prompt_fingerprint");
        std::fs::write(root.join("SOUL.md"), "Voice: direct and concise").expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "Name: Pioneer").expect("write IDENTITY");
        std::fs::write(root.join("USER.md"), "Name: Alex").expect("write USER");

        let baseline = compile_prompt(PromptCompileInput {
            workspace_root: root.clone(),
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile baseline");

        let changed = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: vec![dynamic_section(
                "test.phase10.dynamic",
                Some("Dynamic Hook"),
                "dynamic hook content",
            )],
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile changed");

        assert_eq!(baseline.fingerprint_stable, changed.fingerprint_stable);
        assert_ne!(baseline.fingerprint_dynamic, changed.fingerprint_dynamic);
        assert_ne!(baseline.fingerprint_full, changed.fingerprint_full);
    }

    #[test]
    fn dynamic_prompt_section_truncation_records_diagnostic() {
        let root = temp_workspace("dynamic_prompt_section_truncation");
        let mut section = dynamic_section(
            "test.phase10.truncated",
            Some("Truncated Hook"),
            "0123456789abcdef",
        );
        section.max_chars = Some(8);

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: vec![section],
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        assert!(compiled.dynamic_system_text.contains("01234567"));
        assert!(!compiled.dynamic_system_text.contains("89abcdef"));
        assert!(compiled.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == PromptDiagnosticCode::DynamicSectionTruncated
                && diagnostic.section_id.as_deref() == Some("test.phase10.truncated")
                && !diagnostic.message.contains("0123456789abcdef")
        }));
    }

    #[test]
    fn dynamic_prompt_section_omission_records_diagnostic() {
        let root = temp_workspace("dynamic_prompt_section_omission");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: vec![dynamic_section("test.phase10.empty", Some("Empty"), "   ")],
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        assert!(!compiled.dynamic_system_text.contains("## Empty"));
        assert!(compiled.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == PromptDiagnosticCode::DynamicSectionOmitted
                && diagnostic.section_id.as_deref() == Some("test.phase10.empty")
        }));
    }

    #[test]
    fn assistant_none_filters_dynamic_prompt_sections() {
        let root = temp_workspace("assistant_none_dynamic_sections");
        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantNone,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: vec![dynamic_section(
                "test.phase10.filtered",
                Some("Filtered Hook"),
                "filtered hook content",
            )],
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        assert!(!compiled.full_system_text.contains("Filtered Hook"));
        assert!(!compiled.full_system_text.contains("filtered hook content"));
        assert_eq!(
            compiled
                .sections
                .iter()
                .map(|section| section.id.manifest_id())
                .collect::<Vec<_>>(),
            vec!["identity_base".to_owned()]
        );
    }

    #[test]
    fn non_identity_files_are_ignored_in_identity_only_mode() {
        let root = temp_workspace("identity_only_mode");
        std::fs::write(root.join("AGENTS.md"), "Agent rules").expect("write AGENTS");
        std::fs::write(root.join("TOOLS.md"), "tool notes").expect("write TOOLS");
        std::fs::write(root.join("HEARTBEAT.md"), "heartbeat tasks").expect("write HEARTBEAT");
        std::fs::write(root.join("BOOTSTRAP.md"), "first run checklist").expect("write BOOTSTRAP");
        std::fs::write(root.join("MEMORY.md"), "long-term memory").expect("write MEMORY");
        std::fs::write(root.join("SOUL.md"), "Voice: direct and concise").expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "Name: Pioneer").expect("write IDENTITY");
        std::fs::write(root.join("USER.md"), "Name: Alex").expect("write USER");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        let section_ids = compiled
            .sections
            .iter()
            .map(|section| section.id.clone())
            .collect::<Vec<_>>();
        assert!(section_ids.contains(&PromptSectionId::SoulCore));
        assert!(section_ids.contains(&PromptSectionId::IdentityCore));
        assert!(section_ids.contains(&PromptSectionId::UserPersona));
        assert!(
            !compiled.full_system_text.contains("AGENTS.md")
                && !compiled.full_system_text.contains("TOOLS.md")
                && !compiled.full_system_text.contains("HEARTBEAT.md")
                && !compiled.full_system_text.contains("BOOTSTRAP.md")
                && !compiled.full_system_text.contains("MEMORY.md")
        );
    }

    #[test]
    fn changing_identity_files_changes_stable_fingerprint() {
        fn compile_fingerprint(
            root: &std::path::Path,
            soul: &str,
            identity: &str,
            user: &str,
        ) -> String {
            std::fs::write(root.join("SOUL.md"), soul).expect("write SOUL");
            std::fs::write(root.join("IDENTITY.md"), identity).expect("write IDENTITY");
            std::fs::write(root.join("USER.md"), user).expect("write USER");
            compile_prompt(PromptCompileInput {
                workspace_root: root.to_path_buf(),
                profile: PromptProfile::AssistantFull,
                skills_prompt: None,
                retry_instruction: None,
                include_tool_recovery_policy: true,
                include_task_orchestration_policy: false,
                continue_generation_hint: false,
                runtime_sections: Vec::new(),
                dynamic_sections: Vec::new(),
                dynamic_context: None,
                extra_system: None,
                limits: PromptLimits::default(),
            })
            .expect("compile")
            .fingerprint_stable
        }

        let root = temp_workspace("identity_fingerprint");
        let baseline = compile_fingerprint(&root, "Voice: direct", "Name: Pioneer", "Name: Alex");
        let soul_changed = compile_fingerprint(
            &root,
            "Voice: direct and concise",
            "Name: Pioneer",
            "Name: Alex",
        );
        let identity_changed = compile_fingerprint(
            &root,
            "Voice: direct",
            "Name: Pioneer Assistant",
            "Name: Alex",
        );
        let user_changed =
            compile_fingerprint(&root, "Voice: direct", "Name: Pioneer", "Name: Alexander");

        assert_ne!(baseline, soul_changed);
        assert_ne!(baseline, identity_changed);
        assert_ne!(baseline, user_changed);
    }

    #[test]
    fn changing_dynamic_input_does_not_change_stable_fingerprint() {
        let root = temp_workspace("stable_vs_dynamic");
        std::fs::write(root.join("SOUL.md"), "Voice: direct and concise").expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "Name: Pioneer").expect("write IDENTITY");
        std::fs::write(root.join("USER.md"), "Name: Alex").expect("write USER");

        let baseline = compile_prompt(PromptCompileInput {
            workspace_root: root.clone(),
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile baseline");

        let dynamic_changed = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: Some("[Skills]\n- sample.skill".to_owned()),
            retry_instruction: Some("retry with corrected args".to_owned()),
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: true,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: Some("session dynamic context".to_owned()),
            extra_system: Some("runtime override".to_owned()),
            limits: PromptLimits::default(),
        })
        .expect("compile dynamic changed");

        assert_eq!(
            baseline.fingerprint_stable,
            dynamic_changed.fingerprint_stable
        );
        assert_ne!(
            baseline.fingerprint_dynamic,
            dynamic_changed.fingerprint_dynamic
        );
        assert_ne!(baseline.fingerprint_full, dynamic_changed.fingerprint_full);
    }

    #[test]
    fn missing_identity_files_emit_diagnostics_without_failing_compile() {
        let root = temp_workspace("missing_identity");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        let codes = compiled
            .diagnostics
            .iter()
            .map(|diagnostic| diagnostic.code)
            .collect::<Vec<_>>();
        assert!(
            codes.contains(&PromptDiagnosticCode::MissingFile),
            "expected MissingFile diagnostics"
        );

        let section_ids = compiled
            .sections
            .iter()
            .map(|section| section.id.clone())
            .collect::<Vec<_>>();
        assert!(!section_ids.contains(&PromptSectionId::SoulCore));
        assert!(!section_ids.contains(&PromptSectionId::IdentityCore));
        assert!(!section_ids.contains(&PromptSectionId::UserPersona));

        let source_manifest = &compiled.source_manifest;
        assert_eq!(source_manifest.len(), 3);
        assert_eq!(
            source_manifest[0].status,
            crate::bundle::PromptSourceStatus::Missing
        );
        assert_eq!(
            source_manifest[1].status,
            crate::bundle::PromptSourceStatus::Missing
        );
        assert_eq!(
            source_manifest[2].status,
            crate::bundle::PromptSourceStatus::Missing
        );
    }

    #[test]
    fn empty_runtime_identity_files_are_kept_without_default_fallback() {
        let root = temp_workspace("empty_identity");
        std::fs::write(root.join("SOUL.md"), "\n \n").expect("write empty SOUL");
        std::fs::write(root.join("IDENTITY.md"), "").expect("write empty IDENTITY");
        std::fs::write(root.join("USER.md"), "Name: Alex").expect("write USER");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        let section_ids = compiled
            .sections
            .iter()
            .map(|section| section.id.clone())
            .collect::<Vec<_>>();
        assert!(section_ids.contains(&PromptSectionId::SoulCore));
        assert!(section_ids.contains(&PromptSectionId::IdentityCore));
        assert!(section_ids.contains(&PromptSectionId::UserPersona));
        assert!(!compiled.full_system_text.contains("## Core Truths"));
        assert!(!compiled.full_system_text.contains("### Who Am I?"));
    }

    #[test]
    fn defaults_are_replaced_by_custom_identity_files() {
        let root = temp_workspace("custom_replaces_defaults");
        std::fs::write(root.join("SOUL.md"), "Voice: exact and serious").expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "Name: Pioneer Custom").expect("write IDENTITY");
        std::fs::write(root.join("USER.md"), "Name: Alex").expect("write USER");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        assert!(compiled.full_system_text.contains("### SOUL.md"));
        assert!(compiled.full_system_text.contains("### IDENTITY.md"));
        assert!(
            !compiled
                .full_system_text
                .contains("You're not a chatbot. You're becoming someone.")
        );
        assert!(
            !compiled
                .full_system_text
                .contains("A flat, efficient response is just a worse Google.")
        );
    }

    #[test]
    fn runtime_identity_files_include_evolution_note() {
        let root = temp_workspace("runtime_identity_note");
        std::fs::write(root.join("SOUL.md"), "Voice: exact and serious").expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "Name: Pioneer Custom").expect("write IDENTITY");
        std::fs::write(root.join("USER.md"), "Name: Alex").expect("write USER");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root.clone(),
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        let note = "This file is yours to evolve. As you learn who you are, update it.";
        assert_eq!(compiled.full_system_text.matches(note).count(), 2);
        assert!(!compiled.full_system_text.contains("File path:"));
    }

    #[test]
    fn seed_files_are_used_after_runtime_identity_files_are_created() {
        let root = temp_workspace("defaults_missing_soul_identity");
        crate::ensure_runtime_identity_files(root.as_path()).expect("ensure runtime identity");
        std::fs::write(root.join("USER.md"), "Name: Alex").expect("write USER");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        assert!(compiled.full_system_text.contains("## Soul Core"));
        assert!(compiled.full_system_text.contains("## Core Truths"));
        assert!(
            compiled
                .full_system_text
                .contains("You're not a chatbot. You're becoming someone.")
        );
        assert!(compiled.full_system_text.contains("## Identity Core"));
        assert!(compiled.full_system_text.contains("### Who Am I?"));
        assert!(compiled.full_system_text.contains("- Name: Pioneer"));
        assert!(
            compiled
                .full_system_text
                .contains("- Creature: software-native assistant")
        );
        assert!(compiled.full_system_text.contains("### USER.md"));
    }

    #[test]
    fn read_error_on_identity_file_emits_diagnostic_without_failing_compile() {
        let root = temp_workspace("read_error_identity");
        std::fs::create_dir_all(root.join("SOUL.md")).expect("create directory named SOUL.md");
        std::fs::write(root.join("IDENTITY.md"), "Name: Pioneer").expect("write IDENTITY");
        std::fs::write(root.join("USER.md"), "Name: Alex").expect("write USER");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits::default(),
        })
        .expect("compile");

        assert!(compiled.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == PromptDiagnosticCode::FileReadError
                && diagnostic
                    .file
                    .as_deref()
                    .is_some_and(|file| file.ends_with("SOUL.md"))
        }));

        let source_manifest = &compiled.source_manifest;
        let soul = source_manifest
            .iter()
            .find(|entry| entry.file == "SOUL.md")
            .expect("soul entry must exist");
        assert_eq!(soul.status, crate::bundle::PromptSourceStatus::ReadError);
    }

    #[test]
    fn truncation_marks_source_manifest_entry_as_truncated() {
        let root = temp_workspace("truncated_source_manifest");
        std::fs::write(root.join("SOUL.md"), "A".repeat(120)).expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "B".repeat(120)).expect("write IDENTITY");
        std::fs::write(root.join("USER.md"), "C".repeat(120)).expect("write USER");

        let compiled = compile_prompt(PromptCompileInput {
            workspace_root: root,
            profile: PromptProfile::AssistantFull,
            skills_prompt: None,
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: None,
            limits: PromptLimits {
                max_chars_per_file: 40,
                max_chars_total: 90,
            },
        })
        .expect("compile");

        let source_manifest = &compiled.source_manifest;
        assert!(
            source_manifest
                .iter()
                .any(|entry| entry.status == crate::bundle::PromptSourceStatus::Truncated),
            "expected at least one truncated source entry"
        );
    }
}
