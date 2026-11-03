pub(crate) mod anthropic;
mod azure_openai;
mod bedrock;
mod chat_completions_api_compatible;
pub(crate) mod responses_api_compatible;
pub use responses_api_compatible::ResponsesApiCompatibleProvider;
pub(crate) mod copilot;
pub(crate) mod deepseek;
mod echo;
pub mod embedding;
pub(crate) mod gemini;
pub(crate) mod glm;
mod local;
pub(crate) mod ollama;
pub(crate) mod openai;
pub(crate) mod openrouter;
pub(crate) mod retired;
pub(crate) mod telnyx;
pub(crate) mod vercel_ai_gateway;
pub use vercel_ai_gateway::VercelAiGatewayProvider;

pub use anthropic::AnthropicProvider;
pub use azure_openai::AzureOpenAiProvider;
pub use bedrock::BedrockProvider;
pub use chat_completions_api_compatible::{AuthStyle, ChatCompletionsApiCompatibleProvider};
pub use copilot::CopilotProvider;
pub use deepseek::DeepSeekProvider;
pub use echo::EchoProvider;
pub use gemini::GeminiProvider;
pub use glm::GlmProvider;
pub use local::{
    LOCAL_EMBEDDING_MODELS, LOCAL_TRANSCRIPTION_MODELS, LocalEmbeddingModelInfo, LocalProvider,
    LocalTranscriptionArtifactKind, LocalTranscriptionEngine, LocalTranscriptionModelInfo,
    local_embedding_model_info, local_transcription_model_info,
};
pub use ollama::OllamaProvider;
pub use openai::OpenAiProvider;
pub use openrouter::OpenRouterProvider;
pub use telnyx::TelnyxProvider;

#[cfg(test)]
mod usage_tests;

#[cfg(test)]
mod history_test_support;

#[cfg(test)]
mod streaming_tests;

/// Parse actual native supported/unknown facts without inferring model support.
pub(crate) fn read_native_supports(
    value: &serde_json::Value,
    prefix: &str,
    facts: &mut std::collections::BTreeMap<String, Option<bool>>,
) {
    if value.is_null() {
        facts.insert(format!("{prefix}.supported"), None);
    }
    if let Some(object) = value.as_object() {
        for (key, value) in object {
            if key == "supported" {
                facts.insert(format!("{prefix}.supported"), value.as_bool());
            } else if let Some(supported) = value.get("supported") {
                facts.insert(format!("{prefix}.{key}"), supported.as_bool());
            }
        }
    }
}

pub(crate) fn apply_native_reasoning(
    capabilities: &mut pioneer_protocol::ProviderModelCapabilities,
    facts: std::collections::BTreeMap<String, Option<bool>>,
) {
    use pioneer_protocol::{ProviderModelReasoningCapabilities, ReasoningCapabilitySource};
    if facts.is_empty() {
        return;
    }
    capabilities.thinking = facts.get("thinking.supported").copied().flatten();
    let effort = facts.get("effort.supported").copied().flatten();
    let mut effort_options = Vec::new();
    if effort != Some(false) {
        for (key, value) in &facts {
            if let Some(level) = key
                .strip_prefix("effort.")
                .filter(|e| crate::ReasoningEffort::from_str(e).is_some())
                .filter(|_| *value == Some(true))
            {
                effort_options.push(level.into());
            }
        }
    }
    let thinking = capabilities.thinking;
    let thinking_mode = thinking != Some(false)
        && facts
            .iter()
            .any(|(key, value)| key.starts_with("thinking.types.") && *value == Some(true));
    let supported = if thinking == Some(false) && effort == Some(false) {
        Some(false)
    } else if thinking == Some(true)
        || thinking_mode
        || effort == Some(true)
        || !effort_options.is_empty()
    {
        Some(true)
    } else {
        None
    };
    capabilities.reasoning = Some(ProviderModelReasoningCapabilities {
        native: facts,
        supported,
        effort_options,
        source: Some(ReasoningCapabilitySource::ProviderMetadata),
        ..Default::default()
    });
}
