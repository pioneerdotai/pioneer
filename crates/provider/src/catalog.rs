//! Runtime model catalog. Readers retain a validated snapshot while background
//! updates publish a replacement. Unavailable until saved or fetched data loads.
mod fetch;
pub mod generator;
pub mod runtime;
use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock, RwLock},
};

use pioneer_protocol::ProviderModelInfo;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const UNKNOWN_CONTEXT_WINDOW: u64 = 128_000;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OriginKind {
    Source,
    Override,
    Fallback,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LimitOrigin {
    pub kind: OriginKind,
    pub expression: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogModel {
    pub id: String,
    pub name: String,
    pub provider: String,
    pub api: String,
    pub base_url: String,
    pub context_window: u64,
    pub max_tokens: u64,
    pub reasoning: bool,
    pub input: Vec<String>,
    pub cost: Value,
    // Preserve the complete catalog contract, including API-specific compatibility,
    // thinking maps, pricing tiers and future Pi fields.
    #[serde(flatten)]
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ModelOrigins {
    context_window: LimitOrigin,
    max_tokens: LimitOrigin,
    /// Pioneer supplement: Pi keeps only the combined window/output in Model.
    #[serde(default)]
    input_limit: Option<InputLimit>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct InputLimit {
    value: u64,
    origin: LimitOrigin,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogLimits {
    pub context_window: u64,
    pub context_origin: OriginKind,
    pub max_output: Option<u64>,
    pub max_input: Option<u64>,
}

pub struct ModelCatalog {
    models: BTreeMap<String, BTreeMap<String, CatalogModel>>,
    origins: BTreeMap<String, BTreeMap<String, ModelOrigins>>,
}

impl ModelCatalog {
    pub fn parse(models: &str, origins: &str) -> anyhow::Result<Self> {
        let catalog = Self {
            models: serde_json::from_str(models)?,
            origins: serde_json::from_str(origins)?,
        };
        anyhow::ensure!(!catalog.models.is_empty(), "empty model catalog");
        for (provider, models) in &catalog.models {
            for (id, model) in models {
                anyhow::ensure!(
                    model.id == *id && model.provider == *provider,
                    "catalog identity mismatch"
                );
                anyhow::ensure!(
                    model.context_window > 0 && model.max_tokens > 0,
                    "invalid catalog limit"
                );
                anyhow::ensure!(
                    catalog
                        .origins
                        .get(provider)
                        .and_then(|models| models.get(id))
                        .is_some(),
                    "missing limit provenance"
                );
            }
        }
        for models in catalog.origins.values() {
            for origins in models.values() {
                if let Some(limit) = &origins.input_limit {
                    anyhow::ensure!(limit.value > 0, "invalid separate input limit");
                }
            }
        }
        Ok(catalog)
    }

    pub fn model(&self, provider: &str, id: &str) -> Option<&CatalogModel> {
        let provider = match provider {
            "gemini" => "google",
            "bedrock" => "amazon-bedrock",
            "copilot" => "github-copilot",
            "glm" => "zai",
            "azure_openai" | "azure-openai" => "azure-openai-responses",
            other => other,
        };
        self.models.get(provider)?.get(id)
    }

    pub fn limits(&self, provider: &str, id: &str) -> CatalogLimits {
        let Some(model) = self.model(provider, id) else {
            return CatalogLimits {
                context_window: UNKNOWN_CONTEXT_WINDOW,
                context_origin: OriginKind::Fallback,
                max_output: None,
                max_input: None,
            };
        };
        let origin = &self.origins[&model.provider][id];
        CatalogLimits {
            context_window: if origin.context_window.kind == OriginKind::Fallback {
                UNKNOWN_CONTEXT_WINDOW
            } else {
                model.context_window
            },
            context_origin: origin.context_window.kind.clone(),
            max_input: origin
                .input_limit
                .as_ref()
                .filter(|limit| limit.value > 0 && limit.origin.kind != OriginKind::Fallback)
                .map(|limit| limit.value),
            max_output: (origin.max_tokens.kind != OriginKind::Fallback)
                .then_some(model.max_tokens),
        }
    }

    /// Populate the existing discovery path without advertising unsupported APIs
    /// or inventing a measured provider limit for a synthetic fallback.
    /// User overrides are applied by the workspace resolver after discovery.
    pub fn enrich(&self, provider: &str, models: &mut [ProviderModelInfo]) {
        for model in models {
            let Some(entry) = self.model(provider, &model.id) else {
                continue;
            };
            let limits = self.limits(provider, &model.id);
            if limits.context_origin != OriginKind::Fallback {
                model.limits.context_window = Some(limits.context_window);
            }
            if let Some(max_input) = limits.max_input {
                model.limits.max_input_tokens = Some(max_input);
            }
            if let Some(max_output) = limits.max_output {
                model.limits.max_output_tokens = Some(max_output);
            }
            if model.name.is_none() {
                model.name = Some(entry.name.clone());
            }
            model.capabilities.thinking.get_or_insert(entry.reasoning);
            // The same updateable map drives discovery and outgoing bodies.
            // Requested keys are UI efforts; map values are vendor wire values.
            let source_efforts = entry
                .metadata
                .get("sourceGeneration")
                .and_then(|s| s["reasoningOptions"].as_array())
                .map(|options| {
                    options
                        .iter()
                        .filter(|o| o["type"] == "effort")
                        .flat_map(|o| o["values"].as_array().into_iter().flatten())
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if entry.metadata.get("thinkingLevelMap").is_some() || !source_efforts.is_empty() {
                let mut reasoning = model.capabilities.reasoning.clone().unwrap_or_default();
                reasoning.supported = Some(entry.reasoning);
                if !source_efforts.is_empty() {
                    reasoning.effort_options = source_efforts;
                }
                if let Some(map) = entry
                    .metadata
                    .get("thinkingLevelMap")
                    .and_then(Value::as_object)
                {
                    for (key, value) in map {
                        let effort = if key == "off" { "none" } else { key };
                        reasoning.effort_options.retain(|e| e != effort);
                        if value.is_string() {
                            reasoning.effort_options.push(effort.into());
                        }
                    }
                    reasoning.mandatory = map.get("off").map(|value| value.is_null());
                }
                if entry
                    .metadata
                    .get("compat")
                    .is_some_and(|c| c["supportsReasoningEffort"] == false)
                {
                    reasoning.effort_options.retain(|e| e == "none");
                }
                reasoning.source =
                    Some(pioneer_protocol::ReasoningCapabilitySource::StaticRegistry);
                if reasoning
                    .default_effort
                    .as_ref()
                    .is_some_and(|default| !reasoning.effort_options.contains(default))
                {
                    reasoning.default_effort = None;
                }
                model.capabilities.reasoning = Some(reasoning);
            }
            if provider == "gemini" && model.id.starts_with("gemini-2.5-") {
                // generateContent has budgets, not qualitative levels. There is
                // no product effort-to-budget policy; do not advertise one.
                let mut reasoning = model.capabilities.reasoning.clone().unwrap_or_default();
                reasoning.supported = Some(entry.reasoning);
                reasoning.effort_options.clear();
                let supports_off = matches!(
                    model.id.as_str(),
                    "gemini-2.5-flash" | "gemini-2.5-flash-lite"
                );
                if supports_off {
                    reasoning.effort_options.push("none".into());
                }
                reasoning.default_effort = None;
                reasoning.mandatory = if supports_off {
                    Some(false)
                } else if model.id == "gemini-2.5-pro" {
                    Some(true)
                } else {
                    None // an unknown variant needs its own documented off contract
                };
                reasoning.supports_token_budget = Some(false); // no numeric product control yet
                model.capabilities.reasoning = Some(reasoning);
            }
            if let Some(reasoning) = model.capabilities.reasoning.as_mut() {
                reasoning.effort_options.retain(|effort| {
                    crate::types::ReasoningEffort::from_str(effort).is_some_and(|effort| {
                        crate::generation::effort_supported(provider, entry, effort)
                    })
                });
                if reasoning
                    .default_effort
                    .as_ref()
                    .is_some_and(|effort| !reasoning.effort_options.contains(effort))
                {
                    reasoning.default_effort = None;
                }
                // Absence of an off option alone does not establish mandatory
                // thinking (an unknown contract might support no controls).
                if crate::generation::protocol_mandatory(provider, &entry.id) {
                    reasoning.mandatory = Some(true);
                }
            }
            model.capabilities.tool_calling.get_or_insert(true);
            model
                .capabilities
                .input_modalities
                .get_or_insert_with(|| entry.input.clone());
        }
    }
}

#[derive(Default)]
struct CatalogStore(RwLock<Option<Arc<ModelCatalog>>>);
impl CatalogStore {
    fn snapshot(&self) -> anyhow::Result<Arc<ModelCatalog>> {
        self.0
            .read()
            .expect("model catalog lock")
            .clone()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Model catalog is not loaded yet. Retry after the catalog has loaded."
                )
            })
    }
    fn publish(&self, catalog: ModelCatalog) {
        *self.0.write().expect("model catalog lock") = Some(Arc::new(catalog));
    }
}
fn catalog_store() -> &'static CatalogStore {
    static STORE: OnceLock<CatalogStore> = OnceLock::new();
    STORE.get_or_init(CatalogStore::default)
}

/// A validated, downloaded catalog is required; absence never supplies synthetic limits.
pub fn model_catalog() -> anyhow::Result<Arc<ModelCatalog>> {
    catalog_store().snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_catalog() -> ModelCatalog {
        ModelCatalog::parse(
            include_str!("../tests/fixtures/catalog/models.json"),
            include_str!("../tests/fixtures/catalog/provenance.json"),
        )
        .unwrap()
    }

    #[test]
    fn unavailable_catalog_does_not_supply_unknown_model_fallback() {
        let store = CatalogStore::default();
        assert!(
            store
                .snapshot()
                .err()
                .unwrap()
                .to_string()
                .contains("not loaded")
        );
        store.publish(fixture_catalog());
        assert_eq!(
            store
                .snapshot()
                .unwrap()
                .limits("custom", "unknown")
                .context_window,
            128_000
        );
    }

    #[test]
    fn complete_catalog_loads_and_preserves_metadata() {
        let catalog = fixture_catalog();
        assert!(catalog.models.len() >= 30);
        let model = catalog.model("openai", "gpt-5.4").unwrap();
        assert!(model.metadata.contains_key("compat"));
        assert_eq!(
            catalog.limits("openai", "gpt-5.4").context_origin,
            OriginKind::Override
        );
    }

    #[test]
    fn unknown_endpoint_is_explicit_fallback() {
        assert_eq!(
            fixture_catalog().limits("custom", "unknown"),
            CatalogLimits {
                context_window: 128_000,
                context_origin: OriginKind::Fallback,
                max_output: None,
                max_input: None,
            }
        );
    }

    #[test]
    fn missing_provenance_rejected() {
        assert!(
            ModelCatalog::parse(include_str!("../tests/fixtures/catalog/models.json"), "{}")
                .is_err()
        );
    }
}
