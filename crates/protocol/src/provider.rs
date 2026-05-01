use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --- Provider list ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderListParams {}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderListResponse {
    pub providers: Vec<ProviderSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderSummary {
    pub name: String,
}

// --- Provider list models ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderListModelsParams {
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ProviderModelCapabilities {
    pub vision: Option<bool>,
    pub tool_calling: Option<bool>,
    pub json_output: Option<bool>,
    pub streaming: Option<bool>,
    pub thinking: Option<bool>,
    pub fine_tuning: Option<bool>,
    pub input_modalities: Option<Vec<String>>,
    pub output_modalities: Option<Vec<String>>,
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
    pub pricing: Option<ProviderModelPricing>,
    pub active: Option<bool>,
    pub family: Option<String>,
    pub lifecycle_status: Option<String>,
}

// --- Provider API key management ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderSetApiKeyParams {
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
    pub provider: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderDeleteApiKeyResponse {
    pub provider: String,
    pub deleted: bool,
}
