use pioneer_memory::hooks::{
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

// TODO(memory): re-enable LLM-backed memory policy resolution after the policy
// classifier path is ready for production use again.
const MEMORY_POLICY_LLM_CLASSIFIER_ENABLED: bool = false;

pub(crate) struct GatewayMemoryTurnPolicyProvider {
    provider_registry: Arc<ProviderRegistry>,
}

impl GatewayMemoryTurnPolicyProvider {
    pub(crate) fn new(provider_registry: Arc<ProviderRegistry>) -> Self {
        Self { provider_registry }
    }

    async fn resolve_memory_turn_policy_via_llm(
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
            .get_or_create_for_workspace(context.workspace_id.as_str(), provider_name)
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
                max_tokens: None,
                tools: None,
                tool_choice: None,
                parallel_tool_calls: None,
                compiled_prompt: None,
            })
            .await
            .map_err(|error| format!("memory policy classifier request failed: {error:#}"))?;

        parse_memory_turn_policy_response(response.text.as_str())
    }

    fn default_allow_policy_without_classifier() -> MemoryTurnPolicy {
        MemoryTurnPolicy::normal_default_allow()
            .with_source(
                MemoryPolicySource::DefaultFallback,
                MemoryPolicyReasonCode::DefaultAllowRead,
                1.0,
            )
            .with_diagnostic("memory.policy.classifier_disabled: using local default allow policy")
    }
}

#[async_trait::async_trait]
impl AgentMemoryTurnPolicyProvider for GatewayMemoryTurnPolicyProvider {
    async fn resolve_memory_turn_policy(
        &self,
        context: MemoryTurnPolicyContext,
        request: MemoryTurnPolicyRequest,
    ) -> Result<MemoryTurnPolicy, String> {
        if MEMORY_POLICY_LLM_CLASSIFIER_ENABLED {
            return self
                .resolve_memory_turn_policy_via_llm(context, request)
                .await;
        }

        Ok(Self::default_allow_policy_without_classifier())
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
        detected_language: normalized_language(parsed.language.as_str()),
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

fn normalized_language(language: &str) -> Option<String> {
    let language = language.trim();
    if language.is_empty() {
        None
    } else {
        Some(language.to_owned())
    }
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
    use futures_util::StreamExt;
    use pioneer_provider::{ChatRequest, ChatResponse, Provider, StreamChunk};
    use std::sync::Mutex;

    #[tokio::test]
    async fn policy_provider_returns_default_allow_without_classifier_request() {
        struct CapturingProvider {
            request: Arc<Mutex<Option<ChatRequest>>>,
        }

        #[async_trait::async_trait]
        impl Provider for CapturingProvider {
            fn name(&self) -> &str {
                "capture"
            }

            async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
                *self.request.lock().expect("request lock poisoned") = Some(request);
                Ok(ChatResponse {
                    text: "{}".to_owned(),
                    usage: None,
                    reasoning_content: None,
                    tool_calls: Vec::new(),
                })
            }

            async fn stream_chat(
                &self,
                _request: ChatRequest,
            ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>>
            {
                Ok(futures_util::stream::empty().boxed())
            }
        }

        let captured_request = Arc::new(Mutex::new(None));
        let provider = Arc::new(CapturingProvider {
            request: captured_request.clone(),
        });
        let registry = Arc::new(ProviderRegistry::with_provider("capture", provider));
        let gateway_provider = GatewayMemoryTurnPolicyProvider::new(registry);

        let policy = gateway_provider
            .resolve_memory_turn_policy(
                MemoryTurnPolicyContext {
                    workspace_id: "ws".to_owned(),
                    thread_id: "thread".to_owned(),
                    turn_id: "turn".to_owned(),
                    input_text: "No guardes esto.".to_owned(),
                    mode: pioneer_protocol::ThreadMode::Agent,
                    model: Some("test-model".to_owned()),
                    model_provider: Some("capture".to_owned()),
                },
                MemoryTurnPolicyRequest::default(),
            )
            .await
            .expect("default policy resolves");

        assert_eq!(policy.recall, MemoryRecallPolicy::Allow);
        assert_eq!(policy.prompt, MemoryPromptPolicy::Full);
        assert_eq!(policy.read_tools, MemoryReadToolPolicy::Allow);
        assert_eq!(policy.remember_tool, MemoryMutationToolPolicy::Allow);
        assert_eq!(policy.forget_tool, MemoryMutationToolPolicy::Allow);
        assert_eq!(policy.active_memory, MemoryActiveContextPolicy::Allow);
        assert_eq!(policy.reason_code, MemoryPolicyReasonCode::DefaultAllowRead);
        assert_eq!(policy.source, MemoryPolicySource::DefaultFallback);
        assert!(
            policy
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.contains("memory.policy.classifier_disabled"))
        );
        assert!(
            captured_request
                .lock()
                .expect("request lock poisoned")
                .is_none(),
            "temporary classifier bypass must not call the provider"
        );
    }

    #[tokio::test]
    async fn llm_classifier_path_is_preserved_behind_disabled_switch() {
        struct CapturingProvider {
            request: Arc<Mutex<Option<ChatRequest>>>,
        }

        #[async_trait::async_trait]
        impl Provider for CapturingProvider {
            fn name(&self) -> &str {
                "capture"
            }

            async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
                *self.request.lock().expect("request lock poisoned") = Some(request);
                Ok(ChatResponse {
                    text: r#"{
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
                        "confidence": 0.72,
                        "reasonCode": "default_allow_read"
                    }"#
                    .to_owned(),
                    usage: None,
                    reasoning_content: None,
                    tool_calls: Vec::new(),
                })
            }

            async fn stream_chat(
                &self,
                _request: ChatRequest,
            ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>>
            {
                Ok(futures_util::stream::empty().boxed())
            }
        }

        let captured_request = Arc::new(Mutex::new(None));
        let provider = Arc::new(CapturingProvider {
            request: captured_request.clone(),
        });
        let registry = Arc::new(ProviderRegistry::with_provider("capture", provider));
        let gateway_provider = GatewayMemoryTurnPolicyProvider::new(registry);

        let policy = gateway_provider
            .resolve_memory_turn_policy_via_llm(
                MemoryTurnPolicyContext {
                    workspace_id: "ws".to_owned(),
                    thread_id: "thread".to_owned(),
                    turn_id: "turn".to_owned(),
                    input_text: "No guardes esto.".to_owned(),
                    mode: pioneer_protocol::ThreadMode::Agent,
                    model: Some("test-model".to_owned()),
                    model_provider: Some("capture".to_owned()),
                },
                MemoryTurnPolicyRequest::default(),
            )
            .await
            .expect("classifier request succeeds");

        assert_eq!(policy.reason_code, MemoryPolicyReasonCode::DefaultAllowRead);
        let request = captured_request
            .lock()
            .expect("request lock poisoned")
            .clone()
            .expect("request captured");
        assert_eq!(request.model, "test-model");
        assert_eq!(request.temperature, Some(0.0));
        assert!(request.tools.is_none());
        assert!(request.compiled_prompt.is_none());
        let classifier_prompt = request
            .messages
            .first()
            .expect("classifier prompt message exists")
            .content
            .as_str();
        assert!(classifier_prompt.contains("No guardes esto."));
        assert!(classifier_prompt.contains("Do not search memory"));
        assert!(!classifier_prompt.contains("## Memory Recall"));
    }

    #[test]
    fn parses_normal_default_allow_policy() {
        let policy = parse_memory_turn_policy_response(
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
                "confidence": 0.83,
                "reasonCode": "default_allow_read"
            }"#,
        )
        .expect("valid normal classifier policy");

        assert_eq!(policy.recall, MemoryRecallPolicy::Allow);
        assert_eq!(policy.prompt, MemoryPromptPolicy::Full);
        assert_eq!(policy.read_tools, MemoryReadToolPolicy::Allow);
        assert_eq!(policy.remember_tool, MemoryMutationToolPolicy::Allow);
        assert_eq!(policy.forget_tool, MemoryMutationToolPolicy::Allow);
        assert_eq!(policy.reason_code, MemoryPolicyReasonCode::DefaultAllowRead);
        assert_eq!(policy.detected_language.as_deref(), Some("und"));
    }

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
        assert_eq!(policy.detected_language.as_deref(), Some("es"));
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
        assert_eq!(policy.detected_language.as_deref(), Some("de"));
    }

    #[test]
    fn parses_explicit_forget_policy() {
        let policy = parse_memory_turn_policy_response(
            r#"{
                "intent": "explicit_forget",
                "recall": "disabled",
                "prompt": "forget_only",
                "readTools": "forget_only",
                "rememberTool": "disabled",
                "forgetTool": "allow",
                "postTurnExtraction": "disabled",
                "activeMemory": "disabled",
                "explicitRemember": false,
                "explicitForget": true,
                "forgetTargetHint": "birthday",
                "language": "en",
                "confidence": 0.94,
                "reasonCode": "explicit_forget"
            }"#,
        )
        .expect("valid forget classifier policy");

        assert_eq!(policy.recall, MemoryRecallPolicy::Disabled);
        assert_eq!(policy.prompt, MemoryPromptPolicy::ForgetOnly);
        assert_eq!(policy.read_tools, MemoryReadToolPolicy::ForgetOnly);
        assert_eq!(policy.remember_tool, MemoryMutationToolPolicy::Disabled);
        assert_eq!(policy.forget_tool, MemoryMutationToolPolicy::Allow);
        assert!(policy.explicit_forget);
        assert_eq!(policy.forget_target_hint.as_deref(), Some("birthday"));
        assert_eq!(policy.reason_code, MemoryPolicyReasonCode::ExplicitForget);
    }

    #[test]
    fn rejects_unknown_enum_values_and_invalid_confidence() {
        let invalid_json = parse_memory_turn_policy_response("{not json");
        assert!(invalid_json.is_err());

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
