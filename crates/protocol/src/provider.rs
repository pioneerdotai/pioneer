use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
    #[serde(default)]
    pub api_key_configured: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ProviderSummaryCapabilities {
    #[serde(default)]
    pub embeddings: bool,
    #[serde(default)]
    pub transcription: bool,
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
}
