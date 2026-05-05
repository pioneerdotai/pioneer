use pioneer_agent::{
    AgentMemoryTurnPolicyProvider, MemoryActiveContextPolicy, MemoryClassifierFallbackPolicy,
    MemoryExtractionPolicy, MemoryMutationToolPolicy, MemoryPolicyReasonCode, MemoryPolicySource,
    MemoryPromptPolicy, MemoryReadToolPolicy, MemoryRecallPolicy, MemoryTurnPolicy,
    MemoryTurnPolicyContext, MemoryTurnPolicyRequest,
};
use pioneer_promt::{
    MemoryTurnPolicyClassifierPromptInput, render_memory_turn_policy_classifier_prompt,
};
use pioneer_protocol::ThreadMode;
use pioneer_provider::{ChatMessage, ChatRequest, ProviderRegistry};
use serde::Deserialize;
use std::sync::Arc;

pub(crate) struct GatewayMemoryTurnPolicyProvider {
    provider_registry: Arc<ProviderRegistry>,
}

impl GatewayMemoryTurnPolicyProvider {
    pub(crate) fn new(provider_registry: Arc<ProviderRegistry>) -> Self {
        Self { provider_registry }
    }
}

#[async_trait::async_trait]
impl AgentMemoryTurnPolicyProvider for GatewayMemoryTurnPolicyProvider {
    async fn resolve_memory_turn_policy(
        &self,
        context: MemoryTurnPolicyContext,
        request: MemoryTurnPolicyRequest,
    ) -> Result<MemoryTurnPolicy, String> {
        let provider_name = context
            .model_provider
            .as_deref()
            .ok_or_else(|| "missing model provider for memory policy classification".to_owned())?;
        let model = context
            .model
            .as_deref()
            .ok_or_else(|| "missing model for memory policy classification".to_owned())?;
        let provider = self
            .provider_registry
            .get_or_create(provider_name)
            .map_err(|error| format!("failed to create memory policy provider: {error}"))?;

        let prompt =
            render_memory_turn_policy_classifier_prompt(&MemoryTurnPolicyClassifierPromptInput {
                user_input: context.input_text,
                thread_mode_label: thread_mode_label(context.mode).to_owned(),
                memory_enabled: true,
                classifier_fallback_label: fallback_label(request.fallback).to_owned(),
            });

        let response = provider
            .chat(ChatRequest {
                model: model.to_owned(),
                messages: vec![ChatMessage::user(prompt)],
                temperature: Some(0.0),
                max_tokens: Some(420),
                tools: None,
                tool_choice: None,
                parallel_tool_calls: None,
                compiled_prompt: None,
            })
            .await
            .map_err(|error| format!("memory policy classifier request failed: {error:#}"))?;

        parse_memory_turn_policy_response(response.text.as_str())
    }
}

fn thread_mode_label(mode: ThreadMode) -> &'static str {
    match mode {
        ThreadMode::Agent => "agent",
        ThreadMode::Chat => "chat",
    }
}

fn fallback_label(fallback: MemoryClassifierFallbackPolicy) -> &'static str {
    match fallback {
        MemoryClassifierFallbackPolicy::DefaultAllow => "default_allow",
        MemoryClassifierFallbackPolicy::StrictDeny => "strict_deny",
        MemoryClassifierFallbackPolicy::AllowReadOnly => "allow_read_only",
    }
}

pub(crate) fn parse_memory_turn_policy_response(
    response: &str,
) -> Result<MemoryTurnPolicy, String> {
    let parsed = serde_json::from_str::<ClassifierPolicyResponse>(response.trim())
        .map_err(|error| format!("classifier_invalid_json: {error}"))?;
    if !(0.0..=1.0).contains(&parsed.confidence) {
        return Err(format!(
            "classifier_invalid_json: confidence {} outside [0, 1]",
            parsed.confidence
        ));
    }

    let reason_code = parse_reason_code(parsed.reason_code.as_str())?;
    let mut policy = MemoryTurnPolicy {
        recall: parsed.recall.into(),
        prompt: parsed.prompt.into(),
        read_tools: parsed.read_tools.into(),
        remember_tool: parsed.remember_tool.into(),
        forget_tool: parsed.forget_tool.into(),
        post_turn_extraction: parsed.post_turn_extraction.into(),
        active_memory: parsed.active_memory.into(),
        explicit_remember: parsed.explicit_remember,
        explicit_forget: parsed.explicit_forget,
        forget_target_hint: parsed
            .forget_target_hint
            .filter(|hint| !hint.trim().is_empty()),
        reason_code,
        confidence: parsed.confidence,
        source: MemoryPolicySource::PreMemoryClassifier,
        diagnostics: Vec::new(),
    };
    policy.diagnostics.push(format!(
        "memory.policy.resolved: source=pre_memory_classifier intent={} reason={} confidence={:.2} language={}",
        parsed.intent.as_str(),
        policy.reason_code.as_str(),
        policy.confidence,
        parsed.language.trim()
    ));
    Ok(policy)
}

fn parse_reason_code(value: &str) -> Result<MemoryPolicyReasonCode, String> {
    match value {
        "default_allow_read" => Ok(MemoryPolicyReasonCode::DefaultAllowRead),
        "memory_no_use" => Ok(MemoryPolicyReasonCode::MemoryNoUse),
        "memory_no_save" => Ok(MemoryPolicyReasonCode::MemoryNoSave),
        "explicit_remember" => Ok(MemoryPolicyReasonCode::ExplicitRemember),
        "explicit_forget" => Ok(MemoryPolicyReasonCode::ExplicitForget),
        "structured_override" => Ok(MemoryPolicyReasonCode::StructuredOverride),
        "classifier_unavailable" => Ok(MemoryPolicyReasonCode::ClassifierUnavailable),
        "classifier_invalid_json" => Ok(MemoryPolicyReasonCode::ClassifierInvalidJson),
        "classifier_low_confidence" => Ok(MemoryPolicyReasonCode::ClassifierLowConfidence),
        "chat_mode_disabled" => Ok(MemoryPolicyReasonCode::ChatModeDisabled),
        "memory_runtime_disabled" => Ok(MemoryPolicyReasonCode::MemoryRuntimeDisabled),
        other => Err(format!(
            "classifier_invalid_json: unknown reasonCode `{other}`"
        )),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClassifierPolicyResponse {
    intent: ClassifierIntent,
    recall: ClassifierRecallPolicy,
    prompt: ClassifierPromptPolicy,
    read_tools: ClassifierReadToolPolicy,
    remember_tool: ClassifierMutationToolPolicy,
    forget_tool: ClassifierMutationToolPolicy,
    post_turn_extraction: ClassifierExtractionPolicy,
    active_memory: ClassifierActiveContextPolicy,
    explicit_remember: bool,
    explicit_forget: bool,
    forget_target_hint: Option<String>,
    language: String,
    confidence: f32,
    reason_code: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ClassifierIntent {
    Normal,
    MemoryNoUse,
    MemoryNoSave,
    ExplicitRemember,
    ExplicitForget,
    Mixed,
}

impl ClassifierIntent {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::MemoryNoUse => "memory_no_use",
            Self::MemoryNoSave => "memory_no_save",
            Self::ExplicitRemember => "explicit_remember",
            Self::ExplicitForget => "explicit_forget",
            Self::Mixed => "mixed",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ClassifierRecallPolicy {
    Allow,
    Disabled,
}

impl From<ClassifierRecallPolicy> for MemoryRecallPolicy {
    fn from(value: ClassifierRecallPolicy) -> Self {
        match value {
            ClassifierRecallPolicy::Allow => Self::Allow,
            ClassifierRecallPolicy::Disabled => Self::Disabled,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ClassifierPromptPolicy {
    Full,
    ReadOnly,
    ForgetOnly,
    Disabled,
}

impl From<ClassifierPromptPolicy> for MemoryPromptPolicy {
    fn from(value: ClassifierPromptPolicy) -> Self {
        match value {
            ClassifierPromptPolicy::Full => Self::Full,
            ClassifierPromptPolicy::ReadOnly => Self::ReadOnly,
            ClassifierPromptPolicy::ForgetOnly => Self::ForgetOnly,
            ClassifierPromptPolicy::Disabled => Self::Disabled,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ClassifierReadToolPolicy {
    Allow,
    ForgetOnly,
    Disabled,
}

impl From<ClassifierReadToolPolicy> for MemoryReadToolPolicy {
    fn from(value: ClassifierReadToolPolicy) -> Self {
        match value {
            ClassifierReadToolPolicy::Allow => Self::Allow,
            ClassifierReadToolPolicy::ForgetOnly => Self::ForgetOnly,
            ClassifierReadToolPolicy::Disabled => Self::Disabled,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ClassifierMutationToolPolicy {
    Allow,
    Disabled,
}

impl From<ClassifierMutationToolPolicy> for MemoryMutationToolPolicy {
    fn from(value: ClassifierMutationToolPolicy) -> Self {
        match value {
            ClassifierMutationToolPolicy::Allow => Self::Allow,
            ClassifierMutationToolPolicy::Disabled => Self::Disabled,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ClassifierExtractionPolicy {
    Allow,
    Disabled,
}

impl From<ClassifierExtractionPolicy> for MemoryExtractionPolicy {
    fn from(value: ClassifierExtractionPolicy) -> Self {
        match value {
            ClassifierExtractionPolicy::Allow => Self::Allow,
            ClassifierExtractionPolicy::Disabled => Self::Disabled,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ClassifierActiveContextPolicy {
    Allow,
    Disabled,
}

impl From<ClassifierActiveContextPolicy> for MemoryActiveContextPolicy {
    fn from(value: ClassifierActiveContextPolicy) -> Self {
        match value {
            ClassifierActiveContextPolicy::Allow => Self::Allow,
            ClassifierActiveContextPolicy::Disabled => Self::Disabled,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_no_use_policy() {
        let policy = parse_memory_turn_policy_response(
            r#"{
                "intent": "memory_no_use",
                "recall": "disabled",
                "prompt": "disabled",
                "readTools": "disabled",
                "rememberTool": "disabled",
                "forgetTool": "disabled",
                "postTurnExtraction": "disabled",
                "activeMemory": "disabled",
                "explicitRemember": false,
                "explicitForget": false,
                "forgetTargetHint": null,
                "language": "es",
                "confidence": 0.91,
                "reasonCode": "memory_no_use"
            }"#,
        )
        .expect("valid classifier policy");

        assert_eq!(policy.recall, MemoryRecallPolicy::Disabled);
        assert_eq!(policy.prompt, MemoryPromptPolicy::Disabled);
        assert_eq!(policy.read_tools, MemoryReadToolPolicy::Disabled);
        assert_eq!(policy.remember_tool, MemoryMutationToolPolicy::Disabled);
        assert_eq!(policy.forget_tool, MemoryMutationToolPolicy::Disabled);
        assert_eq!(policy.reason_code, MemoryPolicyReasonCode::MemoryNoUse);
        assert_eq!(policy.source, MemoryPolicySource::PreMemoryClassifier);
    }

    #[test]
    fn parses_no_save_as_read_only_policy() {
        let policy = parse_memory_turn_policy_response(
            r#"{
                "intent": "memory_no_save",
                "recall": "allow",
                "prompt": "read_only",
                "readTools": "allow",
                "rememberTool": "disabled",
                "forgetTool": "allow",
                "postTurnExtraction": "disabled",
                "activeMemory": "disabled",
                "explicitRemember": false,
                "explicitForget": false,
                "forgetTargetHint": null,
                "language": "de",
                "confidence": 0.87,
                "reasonCode": "memory_no_save"
            }"#,
        )
        .expect("valid classifier policy");

        assert_eq!(policy.recall, MemoryRecallPolicy::Allow);
        assert_eq!(policy.prompt, MemoryPromptPolicy::ReadOnly);
        assert_eq!(policy.read_tools, MemoryReadToolPolicy::Allow);
        assert_eq!(policy.remember_tool, MemoryMutationToolPolicy::Disabled);
        assert_eq!(policy.forget_tool, MemoryMutationToolPolicy::Allow);
    }

    #[test]
    fn rejects_unknown_enum_values_and_invalid_confidence() {
        let unknown = parse_memory_turn_policy_response(
            r#"{
                "intent": "normal",
                "recall": "sometimes",
                "prompt": "full",
                "readTools": "allow",
                "rememberTool": "allow",
                "forgetTool": "allow",
                "postTurnExtraction": "disabled",
                "activeMemory": "disabled",
                "explicitRemember": false,
                "explicitForget": false,
                "forgetTargetHint": null,
                "language": "und",
                "confidence": 0.8,
                "reasonCode": "default_allow_read"
            }"#,
        );
        assert!(unknown.is_err());

        let invalid_confidence = parse_memory_turn_policy_response(
            r#"{
                "intent": "normal",
                "recall": "allow",
                "prompt": "full",
                "readTools": "allow",
                "rememberTool": "allow",
                "forgetTool": "allow",
                "postTurnExtraction": "disabled",
                "activeMemory": "disabled",
                "explicitRemember": false,
                "explicitForget": false,
                "forgetTargetHint": null,
                "language": "und",
                "confidence": 2.0,
                "reasonCode": "default_allow_read"
            }"#,
        );
        assert!(invalid_confidence.is_err());
    }
}
