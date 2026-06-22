use pioneer_promt::{
    CompiledPromptBundle, ExecutionContinuationRuntimeFactsInput, PromptCompileInput, PromptLimits,
    PromptProfile, PromptRuntimeBuiltInSectionId, PromptRuntimeSectionId,
    PromptRuntimeSectionInput, REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID, compile_prompt,
    execution_continuation_section_with_runtime_facts, runtime_sections_with_request_tools_catalog,
};
use pioneer_protocol::{
    ExecutionCheckpointOriginalRequestSummary, ExecutionCheckpointPayload,
    ExecutionCheckpointProviderBudgetSummary, ExecutionCheckpointToolCallSummary,
    ExecutionCheckpointToolSummary, ExecutionCheckpointWindowSummary,
    ExecutionWindowExhaustionReason, ToolCallStatus, ToolMetadata, TurnItemType,
};
use serde_json::json;
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

fn continuation_checkpoint_payload() -> ExecutionCheckpointPayload {
    ExecutionCheckpointPayload {
        schema_version: 1,
        workspace_id: "workspace_snapshot".to_owned(),
        thread_id: "thread_snapshot".to_owned(),
        turn_id: "turn_snapshot".to_owned(),
        original_request: ExecutionCheckpointOriginalRequestSummary {
            input_count: 1,
            text_preview: Some("Create report from checked files.".to_owned()),
            text_truncated: false,
            attachment_count: 0,
            attachment_kinds: Vec::new(),
        },
        window: ExecutionCheckpointWindowSummary {
            window_id: Some("turn_snapshot:window:1".to_owned()),
            window_index: 1,
            started_at_unix_ms: Some(1_000),
            completed_at_unix_ms: Some(2_000),
            agent_round_count: 4,
            tool_call_count: 9,
            provider_token_count: Some(456),
            exhaustion_reason: Some(ExecutionWindowExhaustionReason::MaxToolCallsPerWindow),
        },
        provider_budget: ExecutionCheckpointProviderBudgetSummary {
            model: Some("model-snapshot".to_owned()),
            model_provider: Some("provider-snapshot".to_owned()),
            agent_round_count: 4,
            tool_call_count: 9,
            provider_token_count: Some(456),
            provider_usage_available: true,
            exhaustion_reason: Some(ExecutionWindowExhaustionReason::MaxToolCallsPerWindow),
            exhausted_limit: Some(9),
            exhausted_observed: Some(9),
        },
        tools: ExecutionCheckpointToolSummary {
            requested_count: 9,
            executed_count: 8,
            unexecuted_count: 1,
            total_count: 9,
            succeeded_count: 7,
            failed_count: 1,
            in_progress_count: 0,
            detail_limit: 32,
            details_truncated: false,
            details: vec![ExecutionCheckpointToolCallSummary {
                item_id: "tool_read".to_owned(),
                tool_name: "read_file".to_owned(),
                item_type: TurnItemType::FileChange,
                status: ToolCallStatus::Completed,
                success: Some(true),
                error_class: None,
                retry_error_class: None,
                metadata: ToolMetadata::from_json(json!({ "path": "/tmp/source.md" })),
            }],
        },
        strict_obligations: Vec::new(),
    }
}

fn execution_continuation_runtime_section(
    checkpoint: &ExecutionCheckpointPayload,
    prior_visible_assistant_text: Option<&str>,
) -> PromptRuntimeSectionInput {
    let section = execution_continuation_section_with_runtime_facts(
        &ExecutionContinuationRuntimeFactsInput {
            checkpoint,
            prior_visible_assistant_text,
        },
    );

    PromptRuntimeSectionInput {
        id: PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::ExecutionContinuation),
        title: Some(section.title),
        content: section.content,
        max_chars: None,
        truncated: false,
    }
}

fn compile_continuation_prompt(profile: PromptProfile) -> CompiledPromptBundle {
    let root = fixture_root(match profile {
        PromptProfile::AssistantFull => "continuation_full",
        PromptProfile::AssistantMinimal => "continuation_minimal",
        PromptProfile::AssistantNone => "continuation_none",
        PromptProfile::CliRuntime => "continuation_cli_runtime",
    });
    write_fixture_files(&root);

    let checkpoint = continuation_checkpoint_payload();
    let continuation_section = execution_continuation_runtime_section(
        &checkpoint,
        Some("I drafted the outline and verified two files."),
    );

    compile_prompt(PromptCompileInput {
        workspace_root: root,
        profile,
        skills_prompt: None,
        retry_instruction: None,
        include_tool_recovery_policy: true,
        include_task_orchestration_policy: false,
        continue_generation_hint: true,
        runtime_sections: runtime_sections_with_request_tools_catalog(
            &[continuation_section],
            true,
        ),
        dynamic_sections: Vec::new(),
        dynamic_context: None,
        extra_system: None,
        limits: PromptLimits::default(),
    })
    .expect("compile continuation prompt")
}

fn continuation_snapshot_view(profile: PromptProfile, compiled: &CompiledPromptBundle) -> String {
    let section_ids = compiled
        .sections
        .iter()
        .map(|section| format!("- {}", section.id.manifest_id()))
        .collect::<Vec<_>>()
        .join("\n");
    let selected_sections = compiled
        .sections
        .iter()
        .filter(|section| {
            let manifest_id = section.id.manifest_id();
            manifest_id == "execution_continuation"
                || manifest_id == "recovery_continuation"
                || manifest_id == REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID
        })
        .map(|section| format!("## {}\n{}", section.title, section.content))
        .collect::<Vec<_>>()
        .join("\n\n");

    format!("profile={profile:?}\nsection_ids:\n{section_ids}\n\n{selected_sections}",)
}

fn assert_continuation_prompt_contract(compiled: &CompiledPromptBundle) {
    assert!(
        compiled
            .full_system_text
            .contains("## Execution Continuation")
    );
    assert!(compiled.full_system_text.contains("## Tool Usage"));
    assert!(compiled.full_system_text.contains("write_file"));
    assert!(compiled.full_system_text.contains("edit_file"));
    assert!(compiled.full_system_text.contains("call request_tools"));
    assert!(
        !compiled
            .full_system_text
            .contains("Tool loop budget is exhausted. Do not call more tools.")
    );

    let lower = compiled.full_system_text.to_ascii_lowercase();
    for forbidden in [
        "normalized argument shape",
        "failure fingerprint",
        "command parser",
        "parse every command",
        "universal command",
        "curl",
    ] {
        assert!(
            !lower.contains(forbidden),
            "continuation prompt should not contain universal strategy guidance `{forbidden}`"
        );
    }
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
        runtime_sections: Vec::new(),
        dynamic_sections: Vec::new(),
        dynamic_context: Some("session dynamic context".to_owned()),
        extra_system: Some("extra runtime system".to_owned()),
        limits: PromptLimits::default(),
    })
    .expect("compile full");

    insta::assert_snapshot!("assistant_full", compiled.full_system_text);
    assert!(
        compiled
            .full_system_text
            .contains("## Artifact output contract")
    );
    assert!(compiled.full_system_text.contains("artifact_prepare"));
    assert!(compiled.full_system_text.contains("artifact_register"));
    assert!(
        compiled
            .full_system_text
            .contains("$PIONEER_ARTIFACT_OUTPUT_DIR")
    );
    assert!(
        !compiled
            .full_system_text
            .to_ascii_lowercase()
            .contains("scan")
    );
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
        runtime_sections: Vec::new(),
        dynamic_sections: Vec::new(),
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
        runtime_sections: Vec::new(),
        dynamic_sections: Vec::new(),
        dynamic_context: Some("session dynamic context".to_owned()),
        extra_system: Some("extra runtime system".to_owned()),
        limits: PromptLimits::default(),
    })
    .expect("compile none");

    insta::assert_snapshot!("assistant_none", compiled.full_system_text);
    assert!(compiled.dynamic_system_text.is_empty());
}

#[test]
fn continuation_prompt_assistant_full_snapshot() {
    let compiled = compile_continuation_prompt(PromptProfile::AssistantFull);

    insta::assert_snapshot!(continuation_snapshot_view(PromptProfile::AssistantFull, &compiled), @r###"
profile=AssistantFull
section_ids:
- identity_base
- assistant_safety
- artifact_output_contract
- tool_usage_policy
- soul_core
- identity_core
- user_persona
- tool_recovery_policy
- execution_continuation
- request_tools.hidden_domains
- recovery_continuation

## Execution Continuation
This is the same user turn continuing in a new execution window. Continue from the saved execution-window state without restarting the request. Do not replay prior failed tool calls verbatim; use the available prior results and choose the next necessary action.

Checkpoint: schema_version=1, workspace_id=workspace_snapshot, thread_id=thread_snapshot, turn_id=turn_snapshot
Original request preview: Create report from checked files.
Completed window: index=1, agent_rounds=4, tool_calls=9, provider_tokens=456
Window exhaustion reason: max_tool_calls_per_window
Observed exhausted budget: limit=9, observed=9
Tool summary: requested=9, executed=8, succeeded=7, failed=1, in_progress=0, unexecuted=1
Tool detail: item_id=tool_read, tool=read_file, status=completed, success=true, metadata={"path":"/tmp/source.md"}
Strict unresolved obligations: none reported by runtime validators.
Prior visible assistant text: I drafted the outline and verified two files.

## Hidden Tool Domains
Some tool domains and their tools are hidden until requested. If you need a hidden domain and its tools are not currently visible, call request_tools.

Domains:
- memory: memory_search, memory_list, memory_get, memory_remember, memory_forget.
- task: task_create, task_wait, task_accept, task_revise, task_cancel, task_update, task_detach, task_list, task_get, task_reschedule, task_pause, task_resume.
- artifact: artifact_prepare, artifact_register, artifact_read.
- computer_use: computer_use.

## Recovery Continuation
Previous attempt was interrupted by output limits. Continue from where it stopped without repeating prior text.
"###);
    assert_continuation_prompt_contract(&compiled);
}

#[test]
fn continuation_prompt_assistant_minimal_snapshot() {
    let compiled = compile_continuation_prompt(PromptProfile::AssistantMinimal);

    insta::assert_snapshot!(continuation_snapshot_view(PromptProfile::AssistantMinimal, &compiled), @r###"
profile=AssistantMinimal
section_ids:
- identity_base
- assistant_safety
- artifact_output_contract
- tool_usage_policy
- soul_core
- identity_core
- user_persona
- tool_recovery_policy
- execution_continuation
- request_tools.hidden_domains
- recovery_continuation

## Execution Continuation
This is the same user turn continuing in a new execution window. Continue from the saved execution-window state without restarting the request. Do not replay prior failed tool calls verbatim; use the available prior results and choose the next necessary action.

Checkpoint: schema_version=1, workspace_id=workspace_snapshot, thread_id=thread_snapshot, turn_id=turn_snapshot
Original request preview: Create report from checked files.
Completed window: index=1, agent_rounds=4, tool_calls=9, provider_tokens=456
Window exhaustion reason: max_tool_calls_per_window
Observed exhausted budget: limit=9, observed=9
Tool summary: requested=9, executed=8, succeeded=7, failed=1, in_progress=0, unexecuted=1
Tool detail: item_id=tool_read, tool=read_file, status=completed, success=true, metadata={"path":"/tmp/source.md"}
Strict unresolved obligations: none reported by runtime validators.
Prior visible assistant text: I drafted the outline and verified two files.

## Hidden Tool Domains
Some tool domains and their tools are hidden until requested. If you need a hidden domain and its tools are not currently visible, call request_tools.

Domains:
- memory: memory_search, memory_list, memory_get, memory_remember, memory_forget.
- task: task_create, task_wait, task_accept, task_revise, task_cancel, task_update, task_detach, task_list, task_get, task_reschedule, task_pause, task_resume.
- artifact: artifact_prepare, artifact_register, artifact_read.
- computer_use: computer_use.

## Recovery Continuation
Previous attempt was interrupted by output limits. Continue from where it stopped without repeating prior text.
"###);
    assert_continuation_prompt_contract(&compiled);
}

#[test]
fn continuation_window_facts_are_bounded() {
    let root = fixture_root("continuation_bounded");
    write_fixture_files(&root);

    let mut checkpoint = continuation_checkpoint_payload();
    checkpoint
        .tools
        .details
        .push(ExecutionCheckpointToolCallSummary {
            item_id: "tool_large".to_owned(),
            tool_name: "read_file".to_owned(),
            item_type: TurnItemType::FileChange,
            status: ToolCallStatus::Completed,
            success: Some(true),
            error_class: None,
            retry_error_class: None,
            metadata: ToolMetadata::from_json(json!({ "diagnostic": "x".repeat(2_000) })),
        });
    let long_prior_visible_text = "y".repeat(2_100);
    let continuation_section =
        execution_continuation_runtime_section(&checkpoint, Some(long_prior_visible_text.as_str()));

    let compiled = compile_prompt(PromptCompileInput {
        workspace_root: root,
        profile: PromptProfile::AssistantFull,
        skills_prompt: None,
        retry_instruction: None,
        include_tool_recovery_policy: true,
        include_task_orchestration_policy: false,
        continue_generation_hint: true,
        runtime_sections: runtime_sections_with_request_tools_catalog(
            &[continuation_section],
            true,
        ),
        dynamic_sections: Vec::new(),
        dynamic_context: None,
        extra_system: None,
        limits: PromptLimits::default(),
    })
    .expect("compile bounded continuation prompt");

    assert!(compiled.full_system_text.contains("...<truncated>"));
    assert!(
        compiled
            .full_system_text
            .contains("Prior visible assistant text omitted because it exceeded 2000 chars.")
    );
    assert!(!compiled.full_system_text.contains(&"x".repeat(700)));
    assert!(!compiled.full_system_text.contains(&"y".repeat(700)));
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
        runtime_sections: Vec::new(),
        dynamic_sections: Vec::new(),
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
