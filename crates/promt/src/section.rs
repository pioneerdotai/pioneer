use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptStability {
    Stable,
    Dynamic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptSectionId {
    IdentityBase,
    AssistantSafety,
    SoulCore,
    IdentityCore,
    UserPersona,
    ToolRecoveryPolicy,
    TaskOrchestrationPolicy,
    RecoveryContinuation,
    SkillsRuntimePrompt,
    RetryRuntimeInstruction,
    DynamicContext,
    ExtraSystem,
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
