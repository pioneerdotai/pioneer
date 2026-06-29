use crate::{
    CompiledPromptBundle, PromptCompileInput, PromptLimits, PromptProfile,
    PromptRuntimeBuiltInSectionId, PromptRuntimeSectionId, PromptRuntimeSectionInput,
    compile_prompt, current_permission_guidance,
};
use pioneer_protocol::TurnPermissionProfileSnapshot;
use std::path::Path;

const PIONEER_CONTEXT_MAX_CHARS: usize = 4_000;
const MEMORY_CONTEXT_MAX_CHARS: usize = 6_000;
const THREAD_CONTEXT_MAX_CHARS: usize = 6_000;
const CURRENT_PERMISSIONS_CONTEXT_MAX_CHARS: usize = 1_500;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeContextInput {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub runtime_id: String,
    pub runtime_label: Option<String>,
    pub model: Option<String>,
    pub cwd: Option<String>,
    pub permission_profile: TurnPermissionProfileSnapshot,
    pub memory_recall_context: Option<CliRuntimeContextText>,
    pub thread_context: Option<CliRuntimeContextText>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeContextText {
    pub text: String,
    pub truncated: bool,
}

pub fn compile_cli_runtime_context_bundle(
    prompt_root: &Path,
    input: CliRuntimeContextInput,
) -> anyhow::Result<CompiledPromptBundle> {
    compile_prompt(PromptCompileInput {
        workspace_root: prompt_root.to_path_buf(),
        profile: PromptProfile::CliRuntime,
        skills_prompt: None,
        retry_instruction: None,
        include_tool_recovery_policy: false,
        include_task_orchestration_policy: false,
        continue_generation_hint: false,
        runtime_sections: cli_runtime_context_sections(&input),
        dynamic_sections: Vec::new(),
        dynamic_context: None,
        extra_system: None,
        limits: PromptLimits::default(),
    })
}

fn cli_runtime_context_sections(input: &CliRuntimeContextInput) -> Vec<PromptRuntimeSectionInput> {
    let mut sections = vec![
        builtin_section(
            PromptRuntimeBuiltInSectionId::PioneerCliRuntimeContext,
            render_pioneer_context(input),
            Some(PIONEER_CONTEXT_MAX_CHARS),
            false,
        ),
        builtin_section(
            PromptRuntimeBuiltInSectionId::MemoryRecall,
            optional_text(input.memory_recall_context.as_ref()),
            Some(MEMORY_CONTEXT_MAX_CHARS),
            input
                .memory_recall_context
                .as_ref()
                .is_some_and(|context| context.truncated),
        ),
        builtin_section(
            PromptRuntimeBuiltInSectionId::ThreadContext,
            optional_text(input.thread_context.as_ref()),
            Some(THREAD_CONTEXT_MAX_CHARS),
            input
                .thread_context
                .as_ref()
                .is_some_and(|context| context.truncated),
        ),
    ];
    if let Some(content) = current_permission_guidance(&input.permission_profile) {
        sections.push(builtin_section(
            PromptRuntimeBuiltInSectionId::CurrentPermissions,
            content,
            Some(CURRENT_PERMISSIONS_CONTEXT_MAX_CHARS),
            false,
        ));
    }
    sections
}

fn builtin_section(
    id: PromptRuntimeBuiltInSectionId,
    content: String,
    max_chars: Option<usize>,
    truncated: bool,
) -> PromptRuntimeSectionInput {
    PromptRuntimeSectionInput {
        id: PromptRuntimeSectionId::BuiltIn(id),
        title: None,
        content,
        max_chars,
        truncated,
    }
}

fn optional_text(value: Option<&CliRuntimeContextText>) -> String {
    value
        .map(|context| context.text.trim().to_owned())
        .unwrap_or_default()
}

fn render_pioneer_context(input: &CliRuntimeContextInput) -> String {
    let runtime_label = input
        .runtime_label
        .as_deref()
        .and_then(normalized_optional)
        .unwrap_or("CLI runtime");
    let mut lines = vec![
        format!("This is compact Pioneer context for a {runtime_label}-backed turn."),
        "Treat it as context from Pioneer, not as API-provider tool-loop instructions.".to_owned(),
        "Native sandbox, approval, and filesystem behavior are controlled by CLI runtime configuration.".to_owned(),
        String::new(),
        format!("Runtime: {runtime_label} ({})", input.runtime_id.trim()),
        format!("Workspace: {}", input.workspace_id.trim()),
        format!("Thread: {}", input.thread_id.trim()),
        format!("Turn: {}", input.turn_id.trim()),
    ];
    if let Some(model) = input.model.as_deref().and_then(normalized_optional) {
        lines.push(format!("Model: {model}"));
    }
    if let Some(cwd) = input.cwd.as_deref().and_then(normalized_optional) {
        lines.push(format!("Working directory: {cwd}"));
    }
    lines.join("\n")
}

fn normalized_optional(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::{
        CliRuntimeContextInput, CliRuntimeContextText, compile_cli_runtime_context_bundle,
    };

    fn temp_workspace(name: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "pioneer_promt_cli_runtime_{name}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp workspace");
        root
    }

    fn base_input() -> CliRuntimeContextInput {
        CliRuntimeContextInput {
            workspace_id: "workspace_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            runtime_id: "codex-default".to_owned(),
            runtime_label: Some("Codex CLI".to_owned()),
            model: Some("gpt-5-codex".to_owned()),
            cwd: Some("/workspace".to_owned()),
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
            memory_recall_context: None,
            thread_context: None,
        }
    }

    #[test]
    fn cli_runtime_profile_does_not_render_api_provider_prompt_sections() {
        let root = temp_workspace("no_api_prompt");
        std::fs::write(root.join("SOUL.md"), "do not include").expect("write SOUL");

        let bundle = compile_cli_runtime_context_bundle(root.as_path(), base_input())
            .expect("compile context");

        assert_eq!(bundle.profile, crate::PromptProfile::CliRuntime);
        assert!(bundle.full_system_text.contains("Pioneer context"));
        assert!(!bundle.full_system_text.contains("Tool Usage"));
        assert!(!bundle.full_system_text.contains("Artifact output contract"));
        assert!(!bundle.full_system_text.contains("do not include"));
        assert_eq!(
            bundle
                .sections
                .iter()
                .map(|section| section.id.manifest_id())
                .collect::<Vec<_>>(),
            vec!["pioneer_cli_runtime_context"]
        );
        let omitted_sections = bundle
            .diagnostics
            .iter()
            .filter(|diagnostic| {
                diagnostic.code == crate::PromptDiagnosticCode::DynamicSectionOmitted
            })
            .filter_map(|diagnostic| diagnostic.section_id.as_deref())
            .collect::<Vec<_>>();
        assert!(omitted_sections.contains(&"memory_recall"));
        assert!(omitted_sections.contains(&"thread_context"));
    }

    #[test]
    fn cli_runtime_context_manifest_reports_optional_sections() {
        let root = temp_workspace("sections");
        let mut input = base_input();
        input.memory_recall_context = Some(CliRuntimeContextText {
            text: "remembered project convention".to_owned(),
            truncated: false,
        });
        input.thread_context = Some(CliRuntimeContextText {
            text: "user: continue the migration".to_owned(),
            truncated: true,
        });

        let bundle =
            compile_cli_runtime_context_bundle(root.as_path(), input).expect("compile context");

        let section_ids = bundle
            .sections
            .iter()
            .map(|section| section.id.manifest_id())
            .collect::<Vec<_>>();
        assert_eq!(
            section_ids,
            vec![
                "pioneer_cli_runtime_context",
                "memory_recall",
                "thread_context"
            ]
        );
        assert!(
            bundle.diagnostics.iter().any(|diagnostic| {
                diagnostic.section_id.as_deref() == Some("thread_context")
                    && diagnostic.code == crate::PromptDiagnosticCode::DynamicSectionTruncated
            }),
            "thread context truncation should be visible in diagnostics"
        );
    }

    #[test]
    fn cli_runtime_context_includes_restricted_current_permissions() {
        let root = temp_workspace("current_permissions");
        let mut input = base_input();
        input.permission_profile = pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
            pioneer_protocol::TurnPermissionMode::Supervised,
            pioneer_protocol::TurnPermissionProfileSource::Composer,
        );

        let bundle =
            compile_cli_runtime_context_bundle(root.as_path(), input).expect("compile context");

        assert!(
            bundle
                .dynamic_system_text
                .contains("## Current Permissions")
        );
        assert!(bundle.dynamic_system_text.contains("- mode: supervised"));
        assert!(
            bundle
                .dynamic_system_text
                .contains("may require user approval")
        );
        assert_eq!(
            bundle
                .sections
                .iter()
                .map(|section| section.id.manifest_id())
                .collect::<Vec<_>>(),
            vec!["pioneer_cli_runtime_context", "current_permissions"]
        );
    }
}
