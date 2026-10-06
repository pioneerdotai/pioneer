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
        let catalog = Self {
            models: serde_json::from_str(models)?,
            origins: serde_json::from_str(origins)?,
            tool_capabilities,
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

/// Field priority: native facts > matching catalog keys > documented fallback.
/// Actual adapter/mode restrictions always bound the result. A partial map
/// overrides only present keys; omitted off cannot erase known mandatory.
fn enrich_reasoning(provider: &str, model: &mut ProviderModelInfo, entry: Option<&CatalogModel>) {
    use pioneer_protocol::ReasoningCapabilitySource as Source;
    let fallback = crate::reasoning_registry::reasoning_capabilities_for_model(provider, &model.id);
    let documented_off = fallback
        .as_ref()
        .is_some_and(|r| r.effort_options.iter().any(|e| e == "none"));
    let native = model
        .capabilities
        .reasoning
        .as_ref()
        .map(|r| r.native.clone())
        .unwrap_or_default();
    let catalog_thinking = entry.map(|e| e.reasoning);
    let entry = crate::generation::reasoning_model(provider, entry, &native);
    if model.capabilities.reasoning.is_none()
        && fallback.is_none()
        && entry.as_ref().is_none_or(|e| {
            !e.metadata.contains_key("thinkingLevelMap")
                && e.metadata
                    .get("sourceGeneration")
                    .is_none_or(|s| s.get("reasoningOptions").is_none())
        })
    {
        if let Some(entry) = entry {
            model.capabilities.thinking.get_or_insert(entry.reasoning);
        }
        return;
    }
    let mut reasoning = model
        .capabilities
        .reasoning
        .clone()
        .or(fallback.clone())
        .unwrap_or_default();
    if let Some(fallback) = fallback {
        for effort in fallback.effort_options {
            if !reasoning.effort_options.contains(&effort) {
                reasoning.effort_options.push(effort);
            }
        }
        reasoning.default_effort = reasoning.default_effort.or(fallback.default_effort);
        reasoning.mandatory = reasoning.mandatory.or(fallback.mandatory);
        reasoning.supported = reasoning.supported.or(fallback.supported);
    }
    if let Some(entry) = entry.as_ref() {
        reasoning.supported = Some(entry.reasoning);
        model.capabilities.thinking = native
            .get("thinking.supported")
            .copied()
            .flatten()
            // Keep the pre-existing discovery bool for adapters outside the
            // native profiles changed here (for example OpenRouter).
            .or_else(|| {
                (!matches!(provider, "anthropic" | "gemini" | "bedrock"))
                    .then_some(model.capabilities.thinking)
                    .flatten()
            })
            .or(if provider == "openrouter" {
                catalog_thinking
            } else {
                Some(entry.reasoning)
            });
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
            if let Some(off) = map.get("off") {
                reasoning.mandatory = Some(off.is_null());
            }
        }
        if entry
            .metadata
            .get("compat")
            .is_some_and(|c| c["supportsReasoningEffort"] == false)
        {
            reasoning.effort_options.retain(|e| e == "none");
        }
        // Catalog/mixed fields do not originate in StaticRegistry.
        reasoning.source = Some(Source::Unknown);
    }
    // Qualitative source enums do not describe the separate disabled mode.
    // Fill that documented control only for bounded optional native profiles;
    // explicit off vetoes and native mode denials still win below.
    if documented_off
        && entry.as_ref().is_none_or(|e| {
            e.metadata
                .get("thinkingLevelMap")
                .is_none_or(|m| m.get("off") != Some(&Value::Null))
        })
        && !reasoning.effort_options.iter().any(|e| e == "none")
    {
        reasoning.effort_options.push("none".into());
    }
    // Restore native sublevels after catalog replacement. Native thinking and
    // effort support are independent; mode denials are checked by the mapper.
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
    if native.get("effort.supported") == Some(&Some(false)) {
        reasoning.effort_options.retain(|e| e == "none");
    }
    if provider == "gemini" && model.id.starts_with("gemini-2.5-") {
        reasoning.effort_options.retain(|e| e == "none");
        reasoning.default_effort = None;
        reasoning.supports_token_budget = Some(false); // no numeric product control
    }
    if provider == "openrouter" {
        crate::providers::openrouter::preserve_native_reasoning(&mut reasoning);
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
    if !native.iter().any(|(k, v)| {
        k.strip_prefix("default_effort.")
            .is_some_and(|e| crate::ReasoningEffort::from_str(e).is_some())
            && *v == Some(true)
    }) && reasoning
        .default_effort
        .as_ref()
        .is_some_and(|e| !reasoning.effort_options.contains(e))
    {
        reasoning.default_effort = None;
    }
    if crate::generation::protocol_mandatory(provider, &model.id) {
        reasoning.mandatory = Some(true);
    }
    if provider == "gemini" && native.get("thinking.supported") == Some(&Some(false))
        || native.get("thinking.supported") == Some(&Some(false))
            && native.get("effort.supported") == Some(&Some(false))
    {
        reasoning.supported = Some(false);
    } else if provider != "openrouter" && native.values().any(|v| *v == Some(true)) {
        reasoning.supported = Some(true);
    }
    if !native.is_empty() && reasoning.source == Some(Source::StaticRegistry) {
        reasoning.source = Some(Source::Unknown);
    }
    // Unknown remains absent rather than advertising reasoning from an ID.
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
    }

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
