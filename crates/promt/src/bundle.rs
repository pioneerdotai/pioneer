use crate::diagnostics::PromptDiagnostic;
use crate::profile::PromptProfile;
use crate::section::{DynamicPromptSectionInput, PromptRuntimeSectionInput, PromptSection};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct PromptCompileInput {
    pub workspace_root: PathBuf,
    pub profile: PromptProfile,
    pub skills_prompt: Option<String>,
    pub retry_instruction: Option<String>,
    pub include_tool_recovery_policy: bool,
    pub include_task_orchestration_policy: bool,
    pub continue_generation_hint: bool,
    pub runtime_sections: Vec<PromptRuntimeSectionInput>,
    pub dynamic_sections: Vec<DynamicPromptSectionInput>,
    pub dynamic_context: Option<String>,
    pub extra_system: Option<String>,
    pub limits: PromptLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptLimits {
    pub max_chars_per_file: usize,
    pub max_chars_total: usize,
}

impl Default for PromptLimits {
    fn default() -> Self {
        Self {
            max_chars_per_file: 20_000,
            max_chars_total: 150_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptSourceStatus {
    Loaded,
    Missing,
    ReadError,
    Truncated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptSourceManifestEntry {
    pub file: String,
    pub path: String,
    pub status: PromptSourceStatus,
    pub chars: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledPromptBundle {
    pub compiler_version: &'static str,
    pub profile: PromptProfile,
    pub full_system_text: String,
    pub stable_system_text: String,
    pub dynamic_system_text: String,
    pub boundary_marker: &'static str,
    pub fingerprint_stable: String,
    pub fingerprint_dynamic: String,
    pub fingerprint_full: String,
    pub sections: Vec<PromptSection>,
    #[serde(default)]
    pub source_manifest: Vec<PromptSourceManifestEntry>,
    pub diagnostics: Vec<PromptDiagnostic>,
}
