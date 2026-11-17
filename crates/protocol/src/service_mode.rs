//! Model-bound Native service choices and frozen request parameters.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServiceModeParameters {
    Neutral,
    OpenAiStandard,
    AnthropicStandard,
    BedrockStandard,
    OpenAiTier { tier: OpenAiTier },
    OpenAiPro,
    AnthropicFast { beta: Option<String> },
    BedrockNestedUltrafast,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OpenAiTier {
    Priority,
    Flex,
    Ultrafast,
}

/// Retains conditional/partial source prices without flattening context bands
/// or treating absent prices as Standard. Pricing must prove applicability
/// before passing these rates to the existing estimator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ServiceModeDescriptor {
    pub id: String,
    pub label: String,
    pub parameters: ServiceModeParameters,
    pub source_provider: String,
    pub source_model: String,
    pub source_api: String,
    pub source_url: Option<String>,
    pub cost: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SourceServiceMode {
    Parsed {
        descriptor: ServiceModeDescriptor,
    },
    Malformed,
    /// Preserve the source ID so a saved choice cannot silently become Standard.
    Unmapped,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "status", content = "modes", rename_all = "snake_case")]
pub enum SourceServiceModes {
    Unknown,
    Malformed,
    /// An explicitly empty map is distinct from unknown metadata.
    Known(BTreeMap<String, SourceServiceMode>),
}

/// Explicit choice uses the existing endpoint/credential/model authority proof.
/// Absence means Standard; Inherit belongs to the enclosing model selection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NativeServiceModeSelection {
    pub id: String,
    pub authority: String,
}

/// Frozen semantic contract. Prices are captured separately per physical request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NativeServiceModePlan {
    pub id: String,
    pub parameters: ServiceModeParameters,
}
