use pioneer_promt::{PromptCompileInput, PromptLimits, PromptProfile, compile_prompt};
use std::path::PathBuf;

fn fixture_root(name: &str) -> PathBuf {
    let root = PathBuf::from(format!("/tmp/pioneer_promt_snapshot_{name}"));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create fixture root");
    root
}

fn write_fixture_files(root: &std::path::Path) {
    std::fs::write(root.join("SOUL.md"), "Voice: direct and concise").expect("write SOUL");
    std::fs::write(root.join("IDENTITY.md"), "Name: Pioneer").expect("write IDENTITY");
    std::fs::write(root.join("USER.md"), "Name: Alex").expect("write USER");
}

#[test]
fn assistant_full_snapshot() {
    let root = fixture_root("full");
    write_fixture_files(&root);

    let compiled = compile_prompt(PromptCompileInput {
        workspace_root: root,
        profile: PromptProfile::AssistantFull,
        skills_prompt: Some("[Skills]\n- sample.skill".to_owned()),
        retry_instruction: Some("retry with corrected arguments".to_owned()),
        include_tool_recovery_policy: true,
        include_task_orchestration_policy: false,
        continue_generation_hint: true,
        memory_recall: None,
        dynamic_context: Some("session dynamic context".to_owned()),
        extra_system: Some("extra runtime system".to_owned()),
        limits: PromptLimits::default(),
    })
    .expect("compile full");

    insta::assert_snapshot!("assistant_full", compiled.full_system_text);
}

#[test]
fn assistant_minimal_snapshot() {
    let root = fixture_root("minimal");
    write_fixture_files(&root);

    let compiled = compile_prompt(PromptCompileInput {
        workspace_root: root,
        profile: PromptProfile::AssistantMinimal,
        skills_prompt: Some("[Skills]\n- sample.skill".to_owned()),
        retry_instruction: None,
        include_tool_recovery_policy: true,
        include_task_orchestration_policy: false,
        continue_generation_hint: false,
        memory_recall: None,
        dynamic_context: Some("session dynamic context".to_owned()),
        extra_system: None,
        limits: PromptLimits::default(),
    })
    .expect("compile minimal");

    insta::assert_snapshot!("assistant_minimal", compiled.full_system_text);
}

#[test]
fn assistant_none_snapshot() {
    let root = fixture_root("none");
    write_fixture_files(&root);

    let compiled = compile_prompt(PromptCompileInput {
        workspace_root: root,
        profile: PromptProfile::AssistantNone,
        skills_prompt: Some("[Skills]\n- sample.skill".to_owned()),
        retry_instruction: Some("retry with corrected arguments".to_owned()),
        include_tool_recovery_policy: true,
        include_task_orchestration_policy: false,
        continue_generation_hint: true,
        memory_recall: None,
        dynamic_context: Some("session dynamic context".to_owned()),
        extra_system: Some("extra runtime system".to_owned()),
        limits: PromptLimits::default(),
    })
    .expect("compile none");

    insta::assert_snapshot!("assistant_none", compiled.full_system_text);
    assert!(compiled.dynamic_system_text.is_empty());
}

#[test]
fn truncation_snapshot() {
    let root = fixture_root("truncation");
    write_fixture_files(&root);
    std::fs::write(root.join("SOUL.md"), "B".repeat(120)).expect("overwrite SOUL");
    std::fs::write(root.join("IDENTITY.md"), "C".repeat(120)).expect("overwrite IDENTITY");
    std::fs::write(root.join("USER.md"), "D".repeat(120)).expect("overwrite USER");

    let compiled = compile_prompt(PromptCompileInput {
        workspace_root: root,
        profile: PromptProfile::AssistantFull,
        skills_prompt: None,
        retry_instruction: None,
        include_tool_recovery_policy: true,
        include_task_orchestration_policy: false,
        continue_generation_hint: false,
        memory_recall: None,
        dynamic_context: None,
        extra_system: None,
        limits: PromptLimits {
            max_chars_per_file: 40,
            max_chars_total: 50,
        },
    })
    .expect("compile truncation");

    insta::assert_snapshot!("truncation_full", compiled.full_system_text);
    let truncated_count = compiled
        .diagnostics
        .iter()
        .filter(|d| d.message.contains("truncated"))
        .count();
    assert!(truncated_count >= 3, "expected >=3 truncation diagnostics");
}
