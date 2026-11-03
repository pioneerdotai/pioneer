//! Runtime model catalog. Readers retain a validated snapshot while background
//! updates publish a replacement. Unavailable until saved or fetched data loads.
mod fetch;
mod input;
pub use input::InputCapabilityState;
pub(crate) use input::effective_input_model;
pub mod generator;
pub mod runtime;
#[cfg(test)]
pub(crate) mod tool_tests;
use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock, RwLock},
};

use pioneer_protocol::ProviderModelInfo;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// An explicit all-string marker list is evidence; absent/null/malformed is unknown.
/// Shared by periodic sources and authority-native model discovery.
pub(crate) fn tool_support_from_marker_list(value: &Value, marker: &str) -> Option<bool> {
    value
        .as_array()
        .filter(|values| values.iter().all(Value::is_string))
        .map(|values| values.iter().any(|value| value == marker))
}

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
    /// Source capability, independent of brand and of token limit fallbacks.
    #[serde(default)]
    tool_calling: Option<bool>,
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

pub type ToolCapabilities = BTreeMap<String, BTreeMap<String, bool>>;

/// Registry aliases identify a provider; catalog aliases identify its source.
/// Regional/coding-plan source authorities are deliberately not collapsed.
pub(crate) fn catalog_provider(provider: &str) -> String {
    let canonical = crate::provider_definition(provider);
    let provider = canonical.as_ref().map_or(provider, |d| d.name);
    match provider {
        "gemini" => "google",
        "bedrock" => "amazon-bedrock",
        "copilot" => "github-copilot",
        "zai" => "zai-standard",
        "zai-coding" => "zai",
        "glm-coding" => "zai-coding-cn",
        "zhipuai" => "glm",
        "zhipuai-coding-plan" => "zai-coding-cn",
        "azure-openai" => "azure-openai-responses",
        other => other,
    }
    .to_owned()
}

pub struct ModelCatalog {
    models: BTreeMap<String, BTreeMap<String, CatalogModel>>,
    origins: BTreeMap<String, BTreeMap<String, ModelOrigins>>,
    snapshot_id: String,
    tool_capabilities: Option<ToolCapabilities>,
}

impl ModelCatalog {
    pub fn tool_support(&self, provider: &str, id: &str) -> Option<bool> {
        let provider = catalog_provider(provider);
        let model = self.models.get(&provider).and_then(|models| models.get(id));
        let explicit = model.and_then(|m| m.metadata.get("toolCalling")?.as_bool());
        if let Some(capabilities) = &self.tool_capabilities {
            return merge_tool_support(
                explicit,
                capabilities.get(&provider).and_then(|m| m.get(id)).copied(),
            );
        }
        // Older snapshots have only optional per-model provenance. New source
        // supplements are independent of tool-capable list membership/routing.
        merge_tool_support(
            explicit,
            self.origins
                .get(&provider)
                .and_then(|m| m.get(id))
                .and_then(|o| o.tool_calling),
        )
    }
    pub fn parse(models: &str, origins: &str) -> anyhow::Result<Self> {
        Self::parse_with_capabilities(models, origins, None)
    }
    pub fn parse_with_capabilities(
        models: &str,
        origins: &str,
        tool_capabilities: Option<ToolCapabilities>,
    ) -> anyhow::Result<Self> {
        let mut catalog = Self {
            models: serde_json::from_str(models)?,
            origins: serde_json::from_str(origins)?,
            snapshot_id: {
                use sha2::{Digest, Sha256};
                hex::encode(Sha256::digest(models.as_bytes()))
            },
            tool_capabilities,
        };
        // Old Vercel cache records describe a different wire contract. Isolate
        // only those records; last-good evidence for every other source survives.
        if let Some(models) = catalog.models.get_mut("vercel-ai-gateway") {
            let stale: Vec<String> = models
                .iter()
                .filter(|(_, m)| {
                    m.api != "openai-responses" || m.base_url != "https://ai-gateway.vercel.sh/v1"
                })
                .map(|(id, _)| id.clone())
                .collect();
            for id in stale {
                models.remove(&id);
                if let Some(origins) = catalog.origins.get_mut("vercel-ai-gateway") {
                    origins.remove(&id);
                }
                if let Some(capabilities) = catalog
                    .tool_capabilities
                    .as_mut()
                    .and_then(|c| c.get_mut("vercel-ai-gateway"))
                {
                    capabilities.remove(&id);
                }
            }
        }
        if let Some(capabilities) = catalog
            .tool_capabilities
            .as_mut()
            .and_then(|c| c.get_mut("vercel-ai-gateway"))
        {
            // A tool-only legacy supplement carries no Responses route proof.
            // Preserve explicit denials; positives require a current model.
            capabilities.retain(|id, supported| {
                !*supported
                    || catalog
                        .models
                        .get("vercel-ai-gateway")
                        .is_some_and(|models| models.contains_key(id))
            });
        }
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

    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    pub fn model(&self, provider: &str, id: &str) -> Option<&CatalogModel> {
        let provider = catalog_provider(provider);
        self.models.get(&provider)?.get(id)
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
        self.enrich_for_tool_scope(provider, models, true);
    }

    pub(crate) fn enrich_for_tool_scope(
        &self,
        provider: &str,
        models: &mut [ProviderModelInfo],
        use_tool_sources: bool,
    ) {
        for model in models {
            if let Some(input) = &model.capabilities.input_modalities {
                model.capabilities.vision =
                    Some(input.iter().any(|v| v.eq_ignore_ascii_case("image")));
            }
            if use_tool_sources {
                let source = self.tool_support(provider, &model.id);
                model.capabilities.tool_calling =
                    merge_tool_support(model.capabilities.tool_calling, source);
            }
            enrich_reasoning(
                provider,
                model,
                self.model(provider, &model.id).filter(|_| use_tool_sources),
            );
            let Some(entry) = self.model(provider, &model.id).filter(|_| use_tool_sources) else {
                continue;
            };
            let limits = self.limits(provider, &model.id);
            if limits.context_origin != OriginKind::Fallback {
                model
                    .limits
                    .context_window
                    .get_or_insert(limits.context_window);
            }
            if let Some(max_input) = limits.max_input {
                model.limits.max_input_tokens.get_or_insert(max_input);
            }
            if let Some(max_output) = limits.max_output {
                model.limits.max_output_tokens.get_or_insert(max_output);
            }
            if model.name.is_none() {
                model.name = Some(entry.name.clone());
            }
            // Catalog token tariffs use USD per million tokens; discovery DTOs
            // use USD per token. Preserve explicit native zero and unknowns.
            let tariff = |key: &str| {
                if entry
                    .metadata
                    .get("pricingUnits")
                    .and_then(Value::as_str)
                    .is_some_and(|units| units != "USD_per_million_tokens")
                    || entry
                        .metadata
                        .get("pricingSource")
                        .and_then(|source| source.get("unknownRateContract"))
                        .is_some_and(|value| !value.is_null())
                    || entry.cost.get("tiers").is_some_and(|tiers| {
                        !tiers.is_null() && tiers.as_array().is_none_or(|tiers| !tiers.is_empty())
                    })
                {
                    return None;
                }
                entry
                    .cost
                    .get(key)
                    .and_then(Value::as_f64)
                    .filter(|v| v.is_finite() && *v >= 0.0)
                    .map(|v| v / 1_000_000.0)
            };
            let input = tariff("input");
            let output = tariff("output");
            if input.is_some() || output.is_some() {
                let pricing = model.pricing.get_or_insert_with(Default::default);
                pricing.input_token = pricing.input_token.or(input);
                pricing.output_token = pricing.output_token.or(output);
            }
            let effective = effective_input_model(provider, &model.id, Some(entry), Some(model))
                .expect("catalog model");
            let input = model
                .capabilities
                .input_modalities
                .clone()
                .or_else(|| entry.input_is_known().then(|| entry.input.clone()));
            model.capabilities.input_modalities = input.map(|mut input| {
                // Limited native summaries cannot erase supported catalog kinds
                // outside their vocabulary. Raw evidence remains in the scope.
                for name in &entry.input {
                    let kind = match name.to_ascii_lowercase().as_str() {
                        "pdf" | "file" | "document" => crate::InputContentType::File,
                        "audio" => crate::InputContentType::Audio,
                        "video" => crate::InputContentType::Video,
                        _ => continue,
                    };
                    if !input::discovery_input_covers(provider, kind)
                        && effective.input_capability(kind) == InputCapabilityState::Supported
                        && !input.iter().any(|v| v.eq_ignore_ascii_case(name))
                    {
                        input.push(name.clone());
                    }
                }
                input
                    .into_iter()
                    .filter(|name| {
                        let kind = match name.to_ascii_lowercase().as_str() {
                            "image" => crate::InputContentType::Image,
                            "audio" => crate::InputContentType::Audio,
                            "video" => crate::InputContentType::Video,
                            "file" | "pdf" | "document" => crate::InputContentType::File,
                            "text" => crate::InputContentType::Text,
                            _ => return true,
                        };
                        effective.input_capability(kind) != InputCapabilityState::Unsupported
                    })
                    .collect()
            });
            model.capabilities.vision =
                match effective.input_capability(crate::InputContentType::Image) {
                    InputCapabilityState::Supported => Some(true),
                    InputCapabilityState::Unsupported => Some(false),
                    InputCapabilityState::Unknown => None,
                };
            if let Some(output) = entry
                .metadata
                .get("output")
                .and_then(|v| serde_json::from_value::<Vec<String>>(v.clone()).ok())
            {
                model.capabilities.output_modalities.get_or_insert(output);
            }
        }
    }
}

/// Resolve existing metadata per field: provider, then catalog, then unknown.
/// The protocol mapper filters candidates without conferring model support.
fn enrich_reasoning(provider: &str, model: &mut ProviderModelInfo, entry: Option<&CatalogModel>) {
    use pioneer_protocol::ReasoningCapabilitySource as Source;
    let catalog_present = entry.is_some();
    let mut reasoning = model.capabilities.reasoning.clone().unwrap_or_default();
    let mut native = reasoning.native.clone();
    if let Some(supported) = reasoning.supported {
        native
            .entry("reasoning.supported".into())
            .or_insert(Some(supported));
    }
    if reasoning.native.is_empty() && !reasoning.effort_options.is_empty() {
        native.insert("effort.enum".into(), Some(true));
        for effort in &reasoning.effort_options {
            native.insert(format!("effort.{effort}"), Some(true));
        }
    }
    if let Some(thinking) = model.capabilities.thinking {
        native
            .entry("thinking.supported".into())
            .or_insert(Some(thinking));
    }
    let provider_support = native
        .get("reasoning.supported")
        .copied()
        .flatten()
        .or(reasoning.supported)
        // Messages and Converse publish thinking modes separately from effort.
        .or_else(|| {
            (!matches!(provider, "anthropic" | "bedrock"))
                .then_some(model.capabilities.thinking)
                .flatten()
        });
    let catalog_thinking = entry.map(|entry| entry.reasoning);
    let catalog_mandatory = entry
        .and_then(|e| e.metadata.get("thinkingLevelMap"))
        .and_then(|map| map.get("off"))
        .filter(|v| v.is_string() || v.is_null())
        .map(Value::is_null);
    let entry = crate::generation::reasoning_model(provider, entry, &native);
    let mut candidates = Vec::new();
    if let Some(entry) = entry.as_ref() {
        if let Some(options) = entry
            .metadata
            .get("sourceGeneration")
            .and_then(|s| s["reasoningOptions"].as_array())
        {
            candidates.extend(
                options
                    .iter()
                    .filter(|o| o["type"] == "effort")
                    .flat_map(|o| o["values"].as_array().into_iter().flatten())
                    .filter_map(Value::as_str)
                    .map(str::to_owned),
            );
        }
        if let Some(map) = entry
            .metadata
            .get("thinkingLevelMap")
            .and_then(Value::as_object)
        {
            for (key, value) in map {
                let effort = if key == "off" { "none" } else { key };
                candidates.retain(|e| e != effort);
                if value.is_string() {
                    candidates.push(effort.to_owned());
                }
            }
        }
    }
    // A confirmed native enum (including an empty one) is closed. Partial
    // facts override only their keys, leaving missing catalog fields available.
    let closed = native.get("effort.enum").copied().flatten().is_some();
    if !closed {
        for effort in candidates {
            if !reasoning.effort_options.contains(&effort) {
                reasoning.effort_options.push(effort);
            }
        }
    }
    for (key, supported) in &native {
        let Some(level) = key
            .strip_prefix("effort.")
            .filter(|e| crate::ReasoningEffort::from_str(e).is_some())
        else {
            continue;
        };
        if let Some(supported) = supported {
            reasoning.effort_options.retain(|e| e != level);
            if *supported {
                reasoning.effort_options.push(level.into());
            }
        }
    }
    if let Some(disabled) = native.get("thinking.types.disabled").copied().flatten() {
        reasoning.effort_options.retain(|effort| effort != "none");
        if disabled {
            reasoning.effort_options.push("none".into());
        }
    }
    if native.get("effort.enum") == Some(&Some(false)) {
        reasoning.effort_options.clear();
    } else if native.get("effort.supported") == Some(&Some(false)) {
        // Messages/Converse disabled thinking is a separate control from effort.
        // Its confirmation still comes from the native modes/catalog map.
        reasoning.effort_options.retain(|effort| effort == "none");
    }
    reasoning.supported = provider_support
        .or_else(|| entry.as_ref().map(|e| e.reasoning))
        .or_else(|| (!reasoning.effort_options.is_empty()).then_some(true));
    reasoning.mandatory = native
        .get("mandatory")
        .copied()
        .flatten()
        .or(reasoning.mandatory)
        .or_else(|| {
            native
                .get("thinking.types.disabled")
                .copied()
                .flatten()
                .map(|disabled| !disabled)
        })
        .or_else(|| (native.get("effort.none") == Some(&Some(true))).then_some(false))
        .or(catalog_mandatory);
    reasoning.supports_token_budget = native
        .get("supports_token_budget")
        .copied()
        .flatten()
        .or(reasoning.supports_token_budget);
    if provider == "openrouter" {
        crate::providers::openrouter::preserve_native_reasoning(&mut reasoning);
    }
    if reasoning.supported == Some(false) {
        reasoning.effort_options.clear();
    }
    reasoning.effort_options.retain(|e| {
        crate::ReasoningEffort::from_str(e).is_some_and(|e| {
            crate::generation::effort_supported_with_native(
                provider,
                &model.id,
                entry.as_ref(),
                e,
                &native,
            )
        })
    });
    reasoning.effort_options.sort_by_key(|e| {
        ["none", "minimal", "low", "medium", "high", "xhigh", "max"]
            .iter()
            .position(|v| *v == e)
            .unwrap_or(usize::MAX)
    });
    reasoning.effort_options.dedup();
    model.capabilities.thinking = model.capabilities.thinking.or(catalog_thinking);
    if catalog_present || reasoning.source.is_none() && entry.is_some() {
        reasoning.source = Some(Source::Unknown);
    }
    if reasoning != Default::default() {
        model.capabilities.reasoning = Some(reasoning);
    }
}

/// Explicit negatives from discovery or metadata/source veto permission.
/// Otherwise discovery wins, then catalog; missing remains unknown.
pub(crate) fn merge_tool_support(discovery: Option<bool>, catalog: Option<bool>) -> Option<bool> {
    if discovery == Some(false) || catalog == Some(false) {
        Some(false)
    } else {
        discovery.or(catalog)
    }
}

/// Local catalog bootstrap has not supplied a validated snapshot yet.
#[derive(Debug, Clone, Copy)]
pub struct ModelCatalogUnavailable;

impl std::fmt::Display for ModelCatalogUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Model catalog is not loaded yet. Retry after the catalog has loaded.")
    }
}
impl std::error::Error for ModelCatalogUnavailable {}

#[derive(Default)]
struct CatalogStore(RwLock<Option<Arc<ModelCatalog>>>);
impl CatalogStore {
    fn snapshot(&self) -> anyhow::Result<Arc<ModelCatalog>> {
        self.0
            .read()
            .expect("model catalog lock")
            .clone()
            .ok_or_else(|| ModelCatalogUnavailable.into())
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

    #[test]
    fn tool_support_is_model_specific_and_unknown_is_preserved() {
        let mut models: Value =
            serde_json::from_str(include_str!("../tests/fixtures/catalog/models.json")).unwrap();
        let mut origins: Value =
            serde_json::from_str(include_str!("../tests/fixtures/catalog/provenance.json"))
                .unwrap();
        models["openai"]["gpt-5.4"]["toolCalling"] = Value::Bool(false);
        origins["openai"]["gpt-5.4"]["toolCalling"] = Value::Bool(true);
        let catalog = ModelCatalog::parse(&models.to_string(), &origins.to_string()).unwrap();
        assert_eq!(catalog.tool_support("openai", "gpt-5.4"), Some(false));
        assert_eq!(catalog.tool_support("openai", "unknown"), None);
        assert_eq!(catalog.tool_support("openai", "gpt-5-nano"), None);

        let model = catalog.model("openai", "gpt-5.4").unwrap();
        assert!(model.metadata.contains_key("compat"));
        assert_eq!(
            catalog.limits("openai", "gpt-5.4").context_origin,
            OriginKind::Override
        );
    }

    fn fixture_catalog() -> ModelCatalog {
        ModelCatalog::parse(
            include_str!("../tests/fixtures/catalog/models.json"),
            include_str!("../tests/fixtures/catalog/provenance.json"),
        )
        .unwrap()
    }

    fn reasoning_catalog(map: Value) -> ModelCatalog {
        let model = serde_json::json!({
            "id":"aurora-unlisted-927", "name":"Aurora", "provider":"openai",
            "api":"openai-responses", "baseUrl":"https://api.openai.com/v1",
            "contextWindow":32000, "maxTokens":4000, "reasoning":true,
            "input":["text"], "cost":{"input":2.0,"output":8.0},
            "thinkingLevelMap":map
        });
        ModelCatalog::parse(
            &serde_json::json!({"openai":{"aurora-unlisted-927":model}}).to_string(),
            &serde_json::json!({"openai":{"aurora-unlisted-927":{
                "contextWindow":{"kind":"source","expression":"provider context"},
                "maxTokens":{"kind":"source","expression":"provider output"},
                "inputLimit":{"value":24000,"origin":{"kind":"source","expression":"provider input"}}
            }}}).to_string(),
        ).unwrap()
    }

    #[test]
    fn discovery_fills_missing_fields_preserving_native_false_zero_and_limits() {
        let catalog =
            reasoning_catalog(serde_json::json!({"off":"none","low":"low","high":"high"}));
        let mut model = tool_tests::discovered("aurora-unlisted-927", None);
        model.limits.context_window = Some(64000);
        model.limits.max_output_tokens = Some(7000);
        model.pricing = Some(pioneer_protocol::ProviderModelPricing {
            input_token: Some(0.0),
            ..Default::default()
        });
        model.capabilities.reasoning = Some(pioneer_protocol::ProviderModelReasoningCapabilities {
            supported: Some(false),
            mandatory: Some(false),
            ..Default::default()
        });
        catalog.enrich("openai", std::slice::from_mut(&mut model));
        assert_eq!(model.limits.context_window, Some(64000));
        assert_eq!(model.limits.max_output_tokens, Some(7000));
        assert_eq!(model.limits.max_input_tokens, Some(24000));
        let pricing = model.pricing.unwrap();
        assert_eq!(pricing.input_token, Some(0.0));
        assert_eq!(pricing.output_token, Some(0.000008));
        let reasoning = model.capabilities.reasoning.unwrap();
        assert_eq!(reasoning.supported, Some(false));
        assert_eq!(reasoning.mandatory, Some(false));
        assert!(reasoning.effort_options.is_empty());
    }

    #[test]
    fn unknown_catalog_tariffs_remain_unknown_and_explicit_zero_is_free() {
        for (cost, expected) in [(Value::Null, None), (serde_json::json!(0), Some(0.0))] {
            let mut catalog = reasoning_catalog(serde_json::json!({}));
            catalog
                .models
                .get_mut("openai")
                .unwrap()
                .get_mut("aurora-unlisted-927")
                .unwrap()
                .cost = serde_json::json!({"input":cost,"output":null});
            let mut model = tool_tests::discovered("aurora-unlisted-927", None);
            catalog.enrich("openai", std::slice::from_mut(&mut model));
            assert_eq!(model.pricing.as_ref().and_then(|p| p.input_token), expected);
            assert_eq!(model.pricing.as_ref().and_then(|p| p.output_token), None);
        }
    }

    #[test]
    fn conditional_and_non_currency_catalog_prices_are_not_flat_token_tariffs() {
        for metadata in [
            serde_json::json!({"pricingUnits":"premium_requests"}),
            serde_json::json!({"pricingSource":{"unknownRateContract":{"reason":"variable"}}}),
            serde_json::json!({"tiers":[{"inputTokensAbove":1000,"input":4.0}]}),
        ] {
            let mut catalog = reasoning_catalog(serde_json::json!({}));
            let entry = catalog
                .models
                .get_mut("openai")
                .unwrap()
                .get_mut("aurora-unlisted-927")
                .unwrap();
            if let Some(tiers) = metadata.get("tiers") {
                entry.cost["tiers"] = tiers.clone();
            } else {
                entry
                    .metadata
                    .extend(serde_json::from_value::<BTreeMap<String, Value>>(metadata).unwrap());
            }
            let mut model = tool_tests::discovered("aurora-unlisted-927", None);
            catalog.enrich("openai", std::slice::from_mut(&mut model));
            assert!(model.pricing.is_none());
        }
    }

    #[test]
    fn partial_native_reasoning_fills_catalog_fields_but_closed_enums_stay_closed() {
        let catalog = reasoning_catalog(serde_json::json!({"off":null,"low":"low","high":"high"}));
        for closed in [false, true] {
            let mut model = tool_tests::discovered("aurora-unlisted-927", None);
            let mut native = BTreeMap::from([
                ("effort.low".into(), Some(false)),
                ("effort.medium".into(), Some(true)),
            ]);
            if closed {
                native.insert("effort.enum".into(), Some(true));
            }
            model.capabilities.reasoning =
                Some(pioneer_protocol::ProviderModelReasoningCapabilities {
                    native,
                    supported: Some(true),
                    mandatory: Some(false),
                    ..Default::default()
                });
            catalog.enrich("openai", std::slice::from_mut(&mut model));
            let reasoning = model.capabilities.reasoning.unwrap();
            assert_eq!(reasoning.mandatory, Some(false));
            assert!(!reasoning.effort_options.iter().any(|e| e == "low"));
            assert!(reasoning.effort_options.iter().any(|e| e == "medium"));
            assert_eq!(
                reasoning.effort_options.iter().any(|e| e == "high"),
                !closed
            );
        }
    }

    #[test]
    fn catalog_updates_control_unlisted_model_choices_and_unknown_stays_unknown() {
        for effort in ["low", "high"] {
            let catalog = reasoning_catalog(serde_json::json!({(effort):effort}));
            let mut models = vec![
                tool_tests::discovered("aurora-unlisted-927", None),
                tool_tests::discovered("another-unlisted-928", None),
            ];
            catalog.enrich("openai", &mut models);
            assert_eq!(
                models[0]
                    .capabilities
                    .reasoning
                    .as_ref()
                    .unwrap()
                    .effort_options,
                [effort]
            );
            assert!(models[1].capabilities.reasoning.is_none());
            assert!(models[1].pricing.is_none());
            assert!(models[1].limits.max_output_tokens.is_none());
        }
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
