pub mod boundary;
pub mod bundle;
pub mod cli_runtime_context;
pub mod compile;
pub mod constants;
mod content;
pub mod diagnostics;
pub mod fingerprint;
pub mod hooks;
pub mod profile;
pub mod render;
pub mod runtime_files;
pub mod section;
pub mod sources;

pub use bundle::{
    CompiledPromptBundle, PromptCompileInput, PromptLimits, PromptSourceManifestEntry,
    PromptSourceStatus,
};
pub use cli_runtime_context::{
    CliRuntimeContextInput, CliRuntimeContextText, compile_cli_runtime_context_bundle,
};
pub use compile::compile_prompt;
pub use diagnostics::{PromptDiagnostic, PromptDiagnosticCode};
pub use hooks::{
    AGENTS_DOC_PROMPT_HOOK_PACKAGE_ID, AgentsDocPromptResolver, ResolvedAgentsDocPrompt,
    agents_doc_package,
};
pub use profile::PromptProfile;
pub use render::{
    execution_continuation::{
        ExecutionContinuationRuntimeFactsInput, execution_continuation_section,
        execution_continuation_section_with_runtime_facts, render_execution_continuation_prompt,
        render_execution_continuation_runtime_facts,
    },
    memory_active_recall_contract::{
        MemoryActiveRecallProviderOutputContractInput, MemoryActiveRecallProviderOutputPath,
        render_memory_active_recall_provider_output_contract,
        render_memory_active_recall_provider_output_example,
    },
    memory_post_turn_extractor::{
        MemoryPostTurnExtractorPromptInput, render_memory_post_turn_extractor_prompt,
    },
    memory_recall::{
        MemoryRecallPromptContextBlock, MemoryRecallPromptInput, MemoryRecallPromptItem,
        MemoryRecallPromptPolicy, render_memory_recall_context_block, render_memory_recall_prompt,
        render_thread_context_prompt,
    },
    memory_turn_policy::{
        MemoryTurnPolicyClassifierPromptInput, render_memory_turn_policy_classifier_prompt,
    },
    request_tools::{
        REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID, REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_TITLE,
        render_request_tools_hidden_domain_catalog_prompt,
        request_tools_hidden_domain_catalog_section, runtime_sections_with_request_tools_catalog,
    },
    task_run::{TaskRevisionPromptInput, TaskRunPromptCompiler, TaskRunPromptInput},
    tool_loop::tool_loop_final_answer_instruction,
    tool_retry::{ToolRetryInstructionKind, render_tool_retry_instruction},
    turn_preflight::{
        TurnPreflightMemoryActiveRecallPromptInput, TurnPreflightPromptInput,
        render_turn_preflight_prompt,
    },
};
pub use runtime_files::{RuntimeIdentityFilesReport, ensure_runtime_identity_files};
pub use section::{
    DynamicPromptSectionInput, PromptDynamicSectionId, PromptRuntimeBuiltInSectionId,
    PromptRuntimeSectionId, PromptRuntimeSectionInput, PromptSection, PromptSectionId,
    PromptSectionIdError, PromptStability,
};
