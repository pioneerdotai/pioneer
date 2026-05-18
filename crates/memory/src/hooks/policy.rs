use super::*;

#[derive(Clone, Default)]
pub struct MemoryToolMaterialization {
    pub bundles: Vec<pioneer_tools::ToolExtensionBundle>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryRecallPolicy {
    Allow,
    Disabled,
}

impl MemoryRecallPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Disabled => "disabled",
        }
    }

    pub(super) fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "allow" => Ok(Self::Allow),
            "disabled" => Ok(Self::Disabled),
            _ => Err(MemoryPolicyDecodeError::invalid_value("recall")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPromptPolicy {
    Full,
    ReadOnly,
    ForgetOnly,
    Disabled,
}

impl MemoryPromptPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::ReadOnly => "read_only",
            Self::ForgetOnly => "forget_only",
            Self::Disabled => "disabled",
        }
    }

    pub(super) fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "full" => Ok(Self::Full),
            "read_only" => Ok(Self::ReadOnly),
            "forget_only" => Ok(Self::ForgetOnly),
            "disabled" => Ok(Self::Disabled),
            _ => Err(MemoryPolicyDecodeError::invalid_value("prompt")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryReadToolPolicy {
    Allow,
    ForgetOnly,
    Disabled,
}

impl MemoryReadToolPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::ForgetOnly => "forget_only",
            Self::Disabled => "disabled",
        }
    }

    pub(super) fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "allow" => Ok(Self::Allow),
            "forget_only" => Ok(Self::ForgetOnly),
            "disabled" => Ok(Self::Disabled),
            _ => Err(MemoryPolicyDecodeError::invalid_value("read_tools")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryMutationToolPolicy {
    Allow,
    Disabled,
}

impl MemoryMutationToolPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Disabled => "disabled",
        }
    }

    pub(super) fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "allow" => Ok(Self::Allow),
            "disabled" => Ok(Self::Disabled),
            _ => Err(MemoryPolicyDecodeError::invalid_value("mutation_tool")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryExtractionPolicy {
    Allow,
    Disabled,
}

impl MemoryExtractionPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Disabled => "disabled",
        }
    }

    pub(super) fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "allow" => Ok(Self::Allow),
            "disabled" => Ok(Self::Disabled),
            _ => Err(MemoryPolicyDecodeError::invalid_value(
                "post_turn_extraction",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryActiveContextPolicy {
    Allow,
    Disabled,
}

impl MemoryActiveContextPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Disabled => "disabled",
        }
    }

    pub(super) fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "allow" => Ok(Self::Allow),
            "disabled" => Ok(Self::Disabled),
            _ => Err(MemoryPolicyDecodeError::invalid_value("active_memory")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPolicySource {
    StructuredOverride,
    PreMemoryClassifier,
    DefaultFallback,
}

impl MemoryPolicySource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StructuredOverride => "structured_override",
            Self::PreMemoryClassifier => "pre_memory_classifier",
            Self::DefaultFallback => "default_fallback",
        }
    }

    pub(super) fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "structured_override" => Ok(Self::StructuredOverride),
            "pre_memory_classifier" => Ok(Self::PreMemoryClassifier),
            "default_fallback" => Ok(Self::DefaultFallback),
            _ => Err(MemoryPolicyDecodeError::invalid_value("source")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPolicyReasonCode {
    DefaultAllowRead,
    MemoryNoUse,
    MemoryNoSave,
    ExplicitRemember,
    ExplicitForget,
    StructuredOverride,
    ClassifierUnavailable,
    ClassifierInvalidJson,
    ClassifierLowConfidence,
    ChatModeDisabled,
    MemoryRuntimeDisabled,
}

impl MemoryPolicyReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DefaultAllowRead => "default_allow_read",
            Self::MemoryNoUse => "memory_no_use",
            Self::MemoryNoSave => "memory_no_save",
            Self::ExplicitRemember => "explicit_remember",
            Self::ExplicitForget => "explicit_forget",
            Self::StructuredOverride => "structured_override",
            Self::ClassifierUnavailable => "classifier_unavailable",
            Self::ClassifierInvalidJson => "classifier_invalid_json",
            Self::ClassifierLowConfidence => "classifier_low_confidence",
            Self::ChatModeDisabled => "chat_mode_disabled",
            Self::MemoryRuntimeDisabled => "memory_runtime_disabled",
        }
    }

    pub(crate) fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "default_allow_read" => Ok(Self::DefaultAllowRead),
            "memory_no_use" => Ok(Self::MemoryNoUse),
            "memory_no_save" => Ok(Self::MemoryNoSave),
            "explicit_remember" => Ok(Self::ExplicitRemember),
            "explicit_forget" => Ok(Self::ExplicitForget),
            "structured_override" => Ok(Self::StructuredOverride),
            "classifier_unavailable" => Ok(Self::ClassifierUnavailable),
            "classifier_invalid_json" => Ok(Self::ClassifierInvalidJson),
            "classifier_low_confidence" => Ok(Self::ClassifierLowConfidence),
            "chat_mode_disabled" => Ok(Self::ChatModeDisabled),
            "memory_runtime_disabled" => Ok(Self::MemoryRuntimeDisabled),
            _ => Err(MemoryPolicyDecodeError::invalid_value("reason_code")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryClassifierFallbackPolicy {
    DefaultAllow,
    StrictDeny,
    AllowReadOnly,
}

impl MemoryClassifierFallbackPolicy {
    pub(super) fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "default_allow" => Ok(Self::DefaultAllow),
            "strict_deny" => Ok(Self::StrictDeny),
            "allow_read_only" => Ok(Self::AllowReadOnly),
            _ => Err(MemoryPolicyDecodeError::invalid_value("fallback")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryPolicyDecodeError {
    field: &'static str,
    reason: &'static str,
}

impl MemoryPolicyDecodeError {
    pub(super) fn missing(field: &'static str) -> Self {
        Self {
            field,
            reason: "missing",
        }
    }

    pub(super) fn invalid_type(field: &'static str) -> Self {
        Self {
            field,
            reason: "invalid_type",
        }
    }

    pub(super) fn invalid_value(field: &'static str) -> Self {
        Self {
            field,
            reason: "invalid_value",
        }
    }
}

impl fmt::Display for MemoryPolicyDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.field, self.reason)
    }
}

impl std::error::Error for MemoryPolicyDecodeError {}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryTurnPolicy {
    pub recall: MemoryRecallPolicy,
    pub prompt: MemoryPromptPolicy,
    pub read_tools: MemoryReadToolPolicy,
    pub remember_tool: MemoryMutationToolPolicy,
    pub forget_tool: MemoryMutationToolPolicy,
    pub post_turn_extraction: MemoryExtractionPolicy,
    pub active_memory: MemoryActiveContextPolicy,
    pub explicit_remember: bool,
    pub explicit_forget: bool,
    pub forget_target_hint: Option<String>,
    pub reason_code: MemoryPolicyReasonCode,
    pub confidence: f32,
    pub source: MemoryPolicySource,
    pub detected_language: Option<String>,
    pub diagnostics: Vec<String>,
}

impl MemoryTurnPolicy {
    pub fn normal_default_allow() -> Self {
        Self {
            recall: MemoryRecallPolicy::Allow,
            prompt: MemoryPromptPolicy::Full,
            read_tools: MemoryReadToolPolicy::Allow,
            remember_tool: MemoryMutationToolPolicy::Allow,
            forget_tool: MemoryMutationToolPolicy::Allow,
            post_turn_extraction: MemoryExtractionPolicy::Allow,
            active_memory: MemoryActiveContextPolicy::Allow,
            explicit_remember: false,
            explicit_forget: false,
            forget_target_hint: None,
            reason_code: MemoryPolicyReasonCode::DefaultAllowRead,
            confidence: 1.0,
            source: MemoryPolicySource::PreMemoryClassifier,
            detected_language: None,
            diagnostics: Vec::new(),
        }
    }

    pub fn proactive_write_allowed() -> Self {
        Self::normal_default_allow()
    }

    pub fn explicit_remember() -> Self {
        Self {
            explicit_remember: true,
            reason_code: MemoryPolicyReasonCode::ExplicitRemember,
            ..Self::normal_default_allow()
        }
    }

    pub fn explicit_forget(forget_target_hint: Option<String>) -> Self {
        Self {
            recall: MemoryRecallPolicy::Disabled,
            prompt: MemoryPromptPolicy::ForgetOnly,
            read_tools: MemoryReadToolPolicy::ForgetOnly,
            remember_tool: MemoryMutationToolPolicy::Disabled,
            forget_tool: MemoryMutationToolPolicy::Allow,
            post_turn_extraction: MemoryExtractionPolicy::Disabled,
            active_memory: MemoryActiveContextPolicy::Disabled,
            explicit_remember: false,
            explicit_forget: true,
            forget_target_hint,
            reason_code: MemoryPolicyReasonCode::ExplicitForget,
            confidence: 1.0,
            source: MemoryPolicySource::PreMemoryClassifier,
            detected_language: None,
            diagnostics: Vec::new(),
        }
    }

    pub fn no_use() -> Self {
        Self {
            recall: MemoryRecallPolicy::Disabled,
            prompt: MemoryPromptPolicy::Disabled,
            read_tools: MemoryReadToolPolicy::Disabled,
            remember_tool: MemoryMutationToolPolicy::Disabled,
            forget_tool: MemoryMutationToolPolicy::Disabled,
            post_turn_extraction: MemoryExtractionPolicy::Disabled,
            active_memory: MemoryActiveContextPolicy::Disabled,
            explicit_remember: false,
            explicit_forget: false,
            forget_target_hint: None,
            reason_code: MemoryPolicyReasonCode::MemoryNoUse,
            confidence: 1.0,
            source: MemoryPolicySource::PreMemoryClassifier,
            detected_language: None,
            diagnostics: Vec::new(),
        }
    }

    pub fn no_save() -> Self {
        Self {
            recall: MemoryRecallPolicy::Allow,
            prompt: MemoryPromptPolicy::ReadOnly,
            read_tools: MemoryReadToolPolicy::Allow,
            remember_tool: MemoryMutationToolPolicy::Disabled,
            forget_tool: MemoryMutationToolPolicy::Allow,
            post_turn_extraction: MemoryExtractionPolicy::Disabled,
            active_memory: MemoryActiveContextPolicy::Allow,
            explicit_remember: false,
            explicit_forget: false,
            forget_target_hint: None,
            reason_code: MemoryPolicyReasonCode::MemoryNoSave,
            confidence: 1.0,
            source: MemoryPolicySource::PreMemoryClassifier,
            detected_language: None,
            diagnostics: Vec::new(),
        }
    }

    pub fn default_allow_fallback(reason_code: MemoryPolicyReasonCode) -> Self {
        Self::normal_default_allow().with_source(
            MemoryPolicySource::DefaultFallback,
            reason_code,
            0.0,
        )
    }

    pub fn permissive_classifier_fallback(reason_code: MemoryPolicyReasonCode) -> Self {
        Self {
            recall: MemoryRecallPolicy::Allow,
            prompt: MemoryPromptPolicy::Full,
            read_tools: MemoryReadToolPolicy::Allow,
            remember_tool: MemoryMutationToolPolicy::Allow,
            forget_tool: MemoryMutationToolPolicy::Allow,
            post_turn_extraction: MemoryExtractionPolicy::Allow,
            active_memory: MemoryActiveContextPolicy::Allow,
            explicit_remember: false,
            explicit_forget: false,
            forget_target_hint: None,
            reason_code,
            confidence: 0.0,
            source: MemoryPolicySource::DefaultFallback,
            detected_language: None,
            diagnostics: Vec::new(),
        }
    }

    pub fn strict_deny_fallback(reason_code: MemoryPolicyReasonCode) -> Self {
        Self::no_use().with_source(MemoryPolicySource::DefaultFallback, reason_code, 0.0)
    }

    pub fn allow_read_only_fallback(reason_code: MemoryPolicyReasonCode) -> Self {
        Self::no_save().with_source(MemoryPolicySource::DefaultFallback, reason_code, 0.0)
    }

    pub fn with_source(
        mut self,
        source: MemoryPolicySource,
        reason_code: MemoryPolicyReasonCode,
        confidence: f32,
    ) -> Self {
        self.source = source;
        self.reason_code = reason_code;
        self.confidence = confidence.clamp(0.0, 1.0);
        self
    }

    pub fn with_diagnostic(mut self, diagnostic: impl Into<String>) -> Self {
        self.diagnostics.push(diagnostic.into());
        self
    }

    pub fn with_detected_language(mut self, language: Option<String>) -> Self {
        self.detected_language = language.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        });
        self
    }

    pub(crate) fn allow_pre_turn_recall(&self) -> bool {
        self.recall == MemoryRecallPolicy::Allow
    }

    pub(crate) fn allow_memory_prompt(&self) -> bool {
        self.prompt != MemoryPromptPolicy::Disabled
    }

    pub(crate) fn allows_any_memory_tool(&self) -> bool {
        self.allows_memory_tool(MEMORY_SEARCH_TOOL)
            || self.allows_memory_tool(MEMORY_GET_TOOL)
            || self.allows_memory_tool(MEMORY_REMEMBER_TOOL)
            || self.allows_memory_tool(MEMORY_FORGET_TOOL)
    }

    pub(crate) fn allows_memory_tool(&self, tool_name: &str) -> bool {
        match tool_name {
            MEMORY_SEARCH_TOOL | MEMORY_GET_TOOL => {
                matches!(
                    self.read_tools,
                    MemoryReadToolPolicy::Allow | MemoryReadToolPolicy::ForgetOnly
                )
            }
            MEMORY_REMEMBER_TOOL => self.remember_tool == MemoryMutationToolPolicy::Allow,
            MEMORY_FORGET_TOOL => self.forget_tool == MemoryMutationToolPolicy::Allow,
            _ => false,
        }
    }

    pub(crate) fn recall_prompt_policy(&self) -> Option<MemoryRecallPromptPolicy> {
        match self.prompt {
            MemoryPromptPolicy::Full => Some(MemoryRecallPromptPolicy::Full),
            MemoryPromptPolicy::ReadOnly => Some(MemoryRecallPromptPolicy::ReadOnly),
            MemoryPromptPolicy::ForgetOnly => Some(MemoryRecallPromptPolicy::ForgetOnly),
            MemoryPromptPolicy::Disabled => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryTurnPolicyContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub mode: ThreadMode,
    pub input_text: String,
    pub model: Option<String>,
    pub model_provider: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryTurnPolicyOverride {
    pub policy: MemoryTurnPolicy,
}

impl MemoryTurnPolicyOverride {
    pub fn new(policy: MemoryTurnPolicy) -> Self {
        Self { policy }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryTurnPolicyRequest {
    pub default_policy: MemoryTurnPolicy,
    pub structured_override: Option<MemoryTurnPolicyOverride>,
    pub classifier_enabled: bool,
    pub fallback: MemoryClassifierFallbackPolicy,
}

impl Default for MemoryTurnPolicyRequest {
    fn default() -> Self {
        Self {
            default_policy: MemoryTurnPolicy::normal_default_allow(),
            structured_override: None,
            classifier_enabled: true,
            fallback: MemoryClassifierFallbackPolicy::DefaultAllow,
        }
    }
}
