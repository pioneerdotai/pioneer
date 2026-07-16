use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptStability {
    Stable,
    Dynamic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptSectionIdError {
    reason: &'static str,
}

impl PromptSectionIdError {
    fn new(reason: &'static str) -> Self {
        Self { reason }
    }
}

impl fmt::Display for PromptSectionIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.reason)
    }
}

impl std::error::Error for PromptSectionIdError {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PromptDynamicSectionId(String);

impl PromptDynamicSectionId {
    pub fn new(value: impl Into<String>) -> Result<Self, PromptSectionIdError> {
        let value = value.into();
        validate_dynamic_section_id(value.as_str())?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for PromptDynamicSectionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptRuntimeBuiltInSectionId {
    PioneerCliRuntimeInstructions,
    PioneerCliRuntimeContext,
    AgentsMd,
    MemoryRecall,
    ThreadContext,
    SelectedSkills,
    SelectedCapabilities,
    CurrentPermissions,
    ExecutionContinuation,
}

impl PromptRuntimeBuiltInSectionId {
    pub fn from_manifest_id(value: &str) -> Option<Self> {
        match value {
            "pioneer_cli_runtime_instructions" => Some(Self::PioneerCliRuntimeInstructions),
            "pioneer_cli_runtime_context" => Some(Self::PioneerCliRuntimeContext),
            "agents_md" => Some(Self::AgentsMd),
            "memory_recall" => Some(Self::MemoryRecall),
            "thread_context" => Some(Self::ThreadContext),
            "selected_skills" => Some(Self::SelectedSkills),
            "selected_capabilities" => Some(Self::SelectedCapabilities),
            "current_permissions" => Some(Self::CurrentPermissions),
            "execution_continuation" => Some(Self::ExecutionContinuation),
            _ => None,
        }
    }

    pub fn manifest_id(self) -> &'static str {
        match self {
            Self::PioneerCliRuntimeInstructions => "pioneer_cli_runtime_instructions",
            Self::PioneerCliRuntimeContext => "pioneer_cli_runtime_context",
            Self::AgentsMd => "agents_md",
            Self::MemoryRecall => "memory_recall",
            Self::ThreadContext => "thread_context",
            Self::SelectedSkills => "selected_skills",
            Self::SelectedCapabilities => "selected_capabilities",
            Self::CurrentPermissions => "current_permissions",
            Self::ExecutionContinuation => "execution_continuation",
        }
    }

    pub fn prompt_section_id(self) -> PromptSectionId {
        match self {
            Self::PioneerCliRuntimeInstructions => PromptSectionId::PioneerCliRuntimeInstructions,
            Self::PioneerCliRuntimeContext => PromptSectionId::PioneerCliRuntimeContext,
            Self::AgentsMd => PromptSectionId::AgentsMd,
            Self::MemoryRecall => PromptSectionId::MemoryRecall,
            Self::ThreadContext => PromptSectionId::ThreadContext,
            Self::SelectedSkills => PromptSectionId::SelectedSkills,
            Self::SelectedCapabilities => PromptSectionId::SelectedCapabilities,
            Self::CurrentPermissions => PromptSectionId::CurrentPermissions,
            Self::ExecutionContinuation => PromptSectionId::ExecutionContinuation,
        }
    }

    pub fn default_title(self) -> &'static str {
        match self {
            Self::PioneerCliRuntimeInstructions => {
                crate::content::SECTION_TITLE_PIONEER_CLI_RUNTIME_INSTRUCTIONS
            }
            Self::PioneerCliRuntimeContext => {
                crate::content::SECTION_TITLE_PIONEER_CLI_RUNTIME_CONTEXT
            }
            Self::AgentsMd => crate::content::SECTION_TITLE_AGENTS_MD,
            Self::MemoryRecall => crate::content::SECTION_TITLE_MEMORY_RECALL,
            Self::ThreadContext => crate::content::SECTION_TITLE_THREAD_CONTEXT,
            Self::SelectedSkills => crate::content::SECTION_TITLE_SELECTED_SKILLS,
            Self::SelectedCapabilities => crate::content::SECTION_TITLE_SELECTED_CAPABILITIES,
            Self::CurrentPermissions => crate::content::SECTION_TITLE_CURRENT_PERMISSIONS,
            Self::ExecutionContinuation => crate::content::SECTION_TITLE_EXECUTION_CONTINUATION,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum PromptRuntimeSectionId {
    BuiltIn(PromptRuntimeBuiltInSectionId),
    Dynamic(PromptDynamicSectionId),
}

impl PromptRuntimeSectionId {
    pub fn manifest_id(&self) -> String {
        match self {
            Self::BuiltIn(id) => id.manifest_id().to_owned(),
            Self::Dynamic(id) => id.as_str().to_owned(),
        }
    }

    pub fn prompt_section_id(&self) -> PromptSectionId {
        match self {
            Self::BuiltIn(id) => id.prompt_section_id(),
            Self::Dynamic(id) => PromptSectionId::Dynamic(id.clone()),
        }
    }

    pub fn default_title(&self) -> String {
        match self {
            Self::BuiltIn(id) => id.default_title().to_owned(),
            Self::Dynamic(id) => format!("Dynamic: {}", id.as_str()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptSectionId {
    IdentityBase,
    AssistantSafety,
    ArtifactOutputContract,
    ToolUsagePolicy,
    SoulCore,
    IdentityCore,
    UserPersona,
    ToolRecoveryPolicy,
    TaskOrchestrationPolicy,
    SubagentsPolicy,
    TasksPolicy,
    PioneerCliRuntimeInstructions,
    PioneerCliRuntimeContext,
    AgentsMd,
    MemoryRecall,
    ThreadContext,
    SelectedSkills,
    SelectedCapabilities,
    CurrentPermissions,
    RecoveryContinuation,
    ExecutionContinuation,
    SkillsRuntimePrompt,
    RetryRuntimeInstruction,
    DynamicContext,
    ExtraSystem,
    Dynamic(PromptDynamicSectionId),
}

impl PromptSectionId {
    pub fn manifest_id(&self) -> String {
        let id = match self {
            Self::IdentityBase => "identity_base",
            Self::AssistantSafety => "assistant_safety",
            Self::ArtifactOutputContract => "artifact_output_contract",
            Self::ToolUsagePolicy => "tool_usage_policy",
            Self::SoulCore => "soul_core",
            Self::IdentityCore => "identity_core",
            Self::UserPersona => "user_persona",
            Self::ToolRecoveryPolicy => "tool_recovery_policy",
            Self::TaskOrchestrationPolicy => "task_orchestration_policy",
            Self::SubagentsPolicy => "subagents_policy",
            Self::TasksPolicy => "tasks_policy",
            Self::PioneerCliRuntimeInstructions => "pioneer_cli_runtime_instructions",
            Self::PioneerCliRuntimeContext => "pioneer_cli_runtime_context",
            Self::AgentsMd => "agents_md",
            Self::MemoryRecall => "memory_recall",
            Self::ThreadContext => "thread_context",
            Self::SelectedSkills => "selected_skills",
            Self::SelectedCapabilities => "selected_capabilities",
            Self::CurrentPermissions => "current_permissions",
            Self::RecoveryContinuation => "recovery_continuation",
            Self::ExecutionContinuation => "execution_continuation",
            Self::SkillsRuntimePrompt => "skills_runtime_prompt",
            Self::RetryRuntimeInstruction => "retry_runtime_instruction",
            Self::DynamicContext => "dynamic_context",
            Self::ExtraSystem => "extra_system",
            Self::Dynamic(id) => return id.as_str().to_owned(),
        };
        id.to_owned()
    }

    pub fn is_builtin_manifest_id(value: &str) -> bool {
        matches!(
            value,
            "identity_base"
                | "assistant_safety"
                | "artifact_output_contract"
                | "tool_usage_policy"
                | "soul_core"
                | "identity_core"
                | "user_persona"
                | "tool_recovery_policy"
                | "task_orchestration_policy"
                | "subagents_policy"
                | "tasks_policy"
                | "pioneer_cli_runtime_instructions"
                | "pioneer_cli_runtime_context"
                | "agents_md"
                | "memory_recall"
                | "thread_context"
                | "selected_skills"
                | "selected_capabilities"
                | "current_permissions"
                | "recovery_continuation"
                | "execution_continuation"
                | "skills_runtime_prompt"
                | "retry_runtime_instruction"
                | "dynamic_context"
                | "extra_system"
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DynamicPromptSectionInput {
    pub id: PromptDynamicSectionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_chars: Option<usize>,
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptRuntimeSectionInput {
    pub id: PromptRuntimeSectionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_chars: Option<usize>,
    #[serde(default)]
    pub truncated: bool,
}

impl PromptRuntimeSectionInput {
    pub fn dynamic(input: DynamicPromptSectionInput) -> Self {
        Self {
            id: PromptRuntimeSectionId::Dynamic(input.id),
            title: input.title,
            content: input.content,
            max_chars: input.max_chars,
            truncated: input.truncated,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptSection {
    pub id: PromptSectionId,
    pub stability: PromptStability,
    pub title: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<String>,
}

impl PromptSection {
    pub fn as_rendered_text(&self) -> String {
        if self.content.trim().is_empty() {
            return String::new();
        }
        format!("## {}\n{}", self.title.trim(), self.content.trim())
    }
}

fn validate_dynamic_section_id(value: &str) -> Result<(), PromptSectionIdError> {
    if value.is_empty() {
        return Err(PromptSectionIdError::new(
            "dynamic prompt section id cannot be empty",
        ));
    }
    if value.trim() != value {
        return Err(PromptSectionIdError::new(
            "dynamic prompt section id cannot contain leading or trailing whitespace",
        ));
    }
    if value.chars().any(char::is_whitespace) {
        return Err(PromptSectionIdError::new(
            "dynamic prompt section id cannot contain whitespace",
        ));
    }
    if PromptSectionId::is_builtin_manifest_id(value) {
        return Err(PromptSectionIdError::new(
            "dynamic prompt section id cannot collide with a built-in prompt section id",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_section_id_rejects_builtin_collision() {
        assert!(PromptDynamicSectionId::new("agents_md").is_err());
        assert!(PromptDynamicSectionId::new("memory_recall").is_err());
        assert!(PromptDynamicSectionId::new("identity_base").is_err());
    }

    #[test]
    fn agents_md_builtin_round_trips_manifest_identity() {
        let id = PromptRuntimeBuiltInSectionId::from_manifest_id("agents_md")
            .expect("agents_md builtin should resolve");
        assert_eq!(id, PromptRuntimeBuiltInSectionId::AgentsMd);
        assert_eq!(id.manifest_id(), "agents_md");
        assert_eq!(id.prompt_section_id(), PromptSectionId::AgentsMd);
        assert_eq!(id.default_title(), "AGENTS.md");
        assert!(PromptSectionId::is_builtin_manifest_id("agents_md"));
    }

    #[test]
    fn execution_continuation_builtin_round_trips_manifest_identity() {
        let id = PromptRuntimeBuiltInSectionId::from_manifest_id("execution_continuation")
            .expect("execution_continuation builtin should resolve");
        assert_eq!(id, PromptRuntimeBuiltInSectionId::ExecutionContinuation);
        assert_eq!(id.manifest_id(), "execution_continuation");
        assert_eq!(
            id.prompt_section_id(),
            PromptSectionId::ExecutionContinuation
        );
        assert_eq!(id.default_title(), "Execution Continuation");
        assert!(PromptSectionId::is_builtin_manifest_id(
            "execution_continuation"
        ));
    }

    #[test]
    fn dynamic_section_id_accepts_domain_qualified_id() {
        let id = PromptDynamicSectionId::new("memory.recall").expect("valid dynamic id");
        assert_eq!(id.as_str(), "memory.recall");
        assert_eq!(
            PromptSectionId::Dynamic(id).manifest_id(),
            "memory.recall".to_owned()
        );
    }
}
