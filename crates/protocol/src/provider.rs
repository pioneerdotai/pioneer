use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn is_false(value: &bool) -> bool {
    !*value
}

/// Returns the built-in endpoint used when a provider has no custom override.
/// Providers whose endpoint is environment-driven or otherwise not a stable
/// preset return `None`.
pub fn default_provider_base_url(provider_name: &str) -> Option<&'static str> {
    match provider_name.trim().to_ascii_lowercase().as_str() {
        "openai" => Some("https://api.openai.com/v1"),
        "anthropic" => Some("https://api.anthropic.com"),
        "openrouter" => Some("https://openrouter.ai/api/v1"),
        "deepseek" => Some("https://api.deepseek.com"),
        "gemini" | "google" | "google-gemini" => {
            Some("https://generativelanguage.googleapis.com/v1beta")
        }
        "ollama" => Some("http://localhost:11434"),
        "telnyx" => Some("https://api.telnyx.com/v2/ai"),
        "copilot" | "github-copilot" => Some("https://api.githubcopilot.com"),
        "glm" | "zhipu" | "bigmodel" | "glm-global" | "zhipu-global" | "glm-cn" | "zhipu-cn" => {
            Some("https://open.bigmodel.cn/api/paas/v4")
        }
        "groq" => Some("https://api.groq.com/openai/v1"),
        "mistral" => Some("https://api.mistral.ai/v1"),
        "xai" | "grok" => Some("https://api.x.ai"),
        "together" | "together-ai" => Some("https://api.together.xyz"),
        "fireworks" | "fireworks-ai" => Some("https://api.fireworks.ai/inference/v1"),
        "novita" => Some("https://api.novita.ai/openai"),
        "perplexity" => Some("https://api.perplexity.ai"),
        "cohere" => Some("https://api.cohere.com/compatibility"),
        "venice" => Some("https://api.venice.ai"),
        "cerebras" => Some("https://api.cerebras.ai/v1"),
        "sambanova" => Some("https://api.sambanova.ai/v1"),
        "hyperbolic" => Some("https://api.hyperbolic.xyz/v1"),
        "deepinfra" | "deep-infra" => Some("https://api.deepinfra.com/v1/openai"),
        "huggingface" | "hf" => Some("https://router.huggingface.co/v1"),
        "ai21" | "ai21-labs" => Some("https://api.ai21.com/studio/v1"),
        "reka" => Some("https://api.reka.ai/v1"),
        "baseten" => Some("https://inference.baseten.co/v1"),
        "nscale" => Some("https://inference.api.nscale.com/v1"),
        "anyscale" => Some("https://api.endpoints.anyscale.com/v1"),
        "nebius" => Some("https://api.studio.nebius.ai/v1"),
        "friendli" | "friendliai" => Some("https://api.friendli.ai/serverless/v1"),
        "lepton" | "lepton-ai" => Some("https://llama3-1-405b.lepton.run/api/v1"),
        "siliconflow" | "silicon-flow" => Some("https://api.siliconflow.cn/v1"),
        "aihubmix" => Some("https://aihubmix.com/v1"),
        "astrai" => Some("https://as-trai.com/v1"),
        "stepfun" | "step" => Some("https://api.stepfun.com/v1"),
        "baichuan" => Some("https://api.baichuan-ai.com/v1"),
        "yi" | "01ai" | "lingyiwanwu" => Some("https://api.lingyiwanwu.com/v1"),
        "hunyuan" | "tencent" => Some("https://api.hunyuan.cloud.tencent.com/v1"),
        "ovhcloud" | "ovh" => Some("https://api.ai.cloud.ovh.net/v1"),
        "nvidia" | "nvidia-nim" => Some("https://integrate.api.nvidia.com/v1"),
        "synthetic" => Some("https://api.synthetic.new/openai/v1"),
        "doubao" | "volcengine" | "ark" => Some("https://ark.cn-beijing.volces.com/api/v3"),
        "qianfan" | "baidu" => Some("https://aip.baidubce.com"),
        "lmstudio" | "lm-studio" => Some("http://localhost:1234/v1"),
        "llamacpp" | "llama.cpp" => Some("http://localhost:8080/v1"),
        "sglang" => Some("http://localhost:30000/v1"),
        "vllm" => Some("http://localhost:8000/v1"),
        "osaurus" => Some("http://localhost:1337/v1"),
        "litellm" | "lite-llm" => Some("http://localhost:4000/v1"),
        "custom" | "compatible" | "openai-compatible" => Some("http://localhost:8000/v1"),
        "local" | "bedrock" | "aws-bedrock" | "azure_openai" | "azure-openai" | "azure" => None,
        _ => None,
    }
}

/// Returns whether the provider uses a configured base URL at runtime.
pub fn provider_supports_base_url_override(provider_name: &str) -> bool {
    let provider_name = provider_name.trim().to_ascii_lowercase();
    default_provider_base_url(&provider_name).is_some()
        && !matches!(
            provider_name.as_str(),
            "deepseek"
                | "gemini"
                | "google"
                | "google-gemini"
                | "ollama"
                | "telnyx"
                | "copilot"
                | "github-copilot"
                | "glm"
                | "zhipu"
                | "bigmodel"
                | "glm-global"
                | "zhipu-global"
                | "glm-cn"
                | "zhipu-cn"
        )
}

// --- Provider list ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderListParams {
    pub workspace_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderListResponse {
    pub providers: Vec<ProviderSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderSummary {
    pub name: String,
    #[serde(default)]
    pub capabilities: ProviderSummaryCapabilities,
    #[serde(default, skip_serializing_if = "is_false")]
    pub api_key_configured: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ProviderSummaryCapabilities {
    #[serde(default)]
    pub embeddings: bool,
    #[serde(default)]
    pub transcription: bool,
    /// Gateway-authoritative eligibility for the API-only self-improvement
    /// model selectors.
    #[serde(default)]
    pub self_improvement: bool,
}

// --- Provider list models ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderListModelsParams {
    pub workspace_id: String,
    pub provider: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderListModelsResponse {
    pub provider: String,
    pub models: Vec<ProviderModelInfo>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ProviderModelPricing {
    pub input_token: Option<f64>,
    pub output_token: Option<f64>,
    pub image: Option<f64>,
    pub request: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ProviderModelLimits {
    pub max_input_tokens: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub context_window: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningCapabilitySource {
    ProviderMetadata,
    CliMetadata,
    StaticRegistry,
    ConfigOverride,
    Unknown,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ProviderModelReasoningCapabilities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supported: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub effort_options: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mandatory: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_token_budget: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<ReasoningCapabilitySource>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ProviderModelCapabilities {
    pub vision: Option<bool>,
    pub tool_calling: Option<bool>,
    pub json_output: Option<bool>,
    pub streaming: Option<bool>,
    pub embeddings: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcription: Option<bool>,
    pub thinking: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<ProviderModelReasoningCapabilities>,
    pub fine_tuning: Option<bool>,
    pub input_modalities: Option<Vec<String>>,
    pub output_modalities: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ProviderTranscriptionModelMetadata {
    pub engine: String,
    pub download_size_mb: u64,
    pub accuracy_score: u8,
    pub speed_score: u8,
    pub supports_translation: bool,
    pub supported_languages: Vec<String>,
    pub supports_language_selection: bool,
    pub recommended: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderModelInfo {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub created: Option<i64>,
    pub provider: String,
    pub owned_by: Option<String>,
    pub limits: ProviderModelLimits,
    pub capabilities: ProviderModelCapabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcription: Option<ProviderTranscriptionModelMetadata>,
    pub pricing: Option<ProviderModelPricing>,
    pub active: Option<bool>,
    pub family: Option<String>,
    pub lifecycle_status: Option<String>,
}

// --- Provider API key management ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderConfigureParams {
    pub workspace_id: String,
    pub provider: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,
    #[serde(default)]
    pub clear_proxy: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(default)]
    pub clear_base_url: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderConfigureResponse {
    pub provider: String,
    #[serde(default)]
    pub api_key_updated: bool,
    #[serde(default)]
    pub proxy_updated: bool,
    #[serde(default)]
    pub proxy_deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,
    #[serde(default)]
    pub base_url_updated: bool,
    #[serde(default)]
    pub base_url_deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderSetApiKeyParams {
    pub workspace_id: String,
    pub provider: String,
    pub api_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderSetApiKeyResponse {
    pub provider: String,
    pub updated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderDeleteApiKeyParams {
    pub workspace_id: String,
    pub provider: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderDeleteApiKeyResponse {
    pub provider: String,
    pub deleted: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn provider_model_capabilities_round_trips_reasoning_metadata() {
        let capabilities: ProviderModelCapabilities = serde_json::from_value(json!({
            "thinking": true,
            "reasoning": {
                "supported": true,
                "effort_options": ["low", "high"],
                "default_effort": "medium",
                "mandatory": false,
                "supports_token_budget": true,
                "source": "provider_metadata"
            }
        }))
        .expect("capabilities should decode");

        assert_eq!(capabilities.thinking, Some(true));
        assert_eq!(
            capabilities.reasoning,
            Some(ProviderModelReasoningCapabilities {
                supported: Some(true),
                effort_options: vec!["low".to_owned(), "high".to_owned()],
                default_effort: Some("medium".to_owned()),
                mandatory: Some(false),
                supports_token_budget: Some(true),
                source: Some(ReasoningCapabilitySource::ProviderMetadata),
            })
        );

        let encoded = serde_json::to_value(capabilities).expect("capabilities should encode");
        assert_eq!(encoded["thinking"], json!(true));
        assert_eq!(
            encoded["reasoning"],
            json!({
                "supported": true,
                "effort_options": ["low", "high"],
                "default_effort": "medium",
                "mandatory": false,
                "supports_token_budget": true,
                "source": "provider_metadata"
            })
        );
    }

    #[test]
    fn provider_model_capabilities_decode_without_reasoning_metadata() {
        let capabilities: ProviderModelCapabilities = serde_json::from_value(json!({
            "thinking": false
        }))
        .expect("legacy capabilities should decode");

        assert_eq!(capabilities.thinking, Some(false));
        assert!(capabilities.reasoning.is_none());
        assert!(capabilities.transcription.is_none());
    }

    #[test]
    fn legacy_provider_payloads_decode_without_transcription_fields() {
        let summary: ProviderSummaryCapabilities =
            serde_json::from_value(json!({ "embeddings": true }))
                .expect("legacy provider summary capabilities should decode");
        assert!(summary.embeddings);
        assert!(!summary.transcription);

        let model: ProviderModelInfo = serde_json::from_value(json!({
            "id": "legacy-model",
            "name": "Legacy model",
            "description": null,
            "created": null,
            "provider": "legacy",
            "owned_by": null,
            "limits": {
                "max_input_tokens": null,
                "max_output_tokens": null,
                "context_window": null
            },
            "capabilities": {},
            "pricing": null,
            "active": true,
            "family": null,
            "lifecycle_status": null
        }))
        .expect("legacy provider model should decode");

        assert!(model.capabilities.transcription.is_none());
        assert!(model.transcription.is_none());

        let encoded = serde_json::to_value(model).expect("legacy provider model should encode");
        assert!(encoded.get("transcription").is_none());
        assert!(encoded["capabilities"].get("transcription").is_none());
    }

    #[test]
    fn provider_transcription_metadata_round_trips_without_trusted_fields() {
        let metadata = ProviderTranscriptionModelMetadata {
            engine: "parakeet".to_owned(),
            download_size_mb: 456,
            accuracy_score: 80,
            speed_score: 85,
            supports_translation: false,
            supported_languages: vec!["en".to_owned(), "ru".to_owned()],
            supports_language_selection: false,
            recommended: true,
        };

        let encoded = serde_json::to_value(&metadata).expect("metadata should encode");
        let decoded: ProviderTranscriptionModelMetadata =
            serde_json::from_value(encoded.clone()).expect("metadata should decode");

        assert_eq!(decoded, metadata);
        for trusted_field in [
            "url",
            "sha256",
            "artifact_file_name",
            "install_dir_name",
            "runtime_file_name",
        ] {
            assert!(
                encoded.get(trusted_field).is_none(),
                "trusted field leaked: {trusted_field}"
            );
        }

        let schema = schemars::schema_for!(ProviderTranscriptionModelMetadata);
        let schema_json = serde_json::to_string(&schema).expect("schema should encode");
        for trusted_field in [
            "url",
            "sha256",
            "artifact_file_name",
            "install_dir_name",
            "runtime_file_name",
        ] {
            assert!(
                !schema_json.contains(trusted_field),
                "trusted field leaked into schema: {trusted_field}"
            );
        }
    }

    #[test]
    fn provider_summary_and_configure_round_trip_base_url() {
        let summary: ProviderSummary = serde_json::from_value(json!({
            "name": "openai",
            "base_url": "https://api.example.com/v1"
        }))
        .expect("summary should decode");
        assert_eq!(
            summary.base_url.as_deref(),
            Some("https://api.example.com/v1")
        );

        let configure: ProviderConfigureParams = serde_json::from_value(json!({
            "workspace_id": "ws_default",
            "provider": "openai",
            "base_url": "https://api.example.com/v1",
            "clear_base_url": false
        }))
        .expect("configure params should decode");
        assert_eq!(
            configure.base_url.as_deref(),
            Some("https://api.example.com/v1")
        );
        assert!(!configure.clear_base_url);

        let legacy_configure: ProviderConfigureParams = serde_json::from_value(json!({
            "workspace_id": "ws_default",
            "provider": "openai"
        }))
        .expect("legacy configure params should decode");
        assert!(legacy_configure.base_url.is_none());
        assert!(!legacy_configure.clear_base_url);
    }
}
