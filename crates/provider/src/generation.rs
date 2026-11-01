//! Last-mile generation settings. Limits remain owned by the updateable catalog;
//! this module supplies protocol rules, never a second model/limit catalog.
//!
//! Provider facts take precedence field by field over the existing catalog.
//! Missing facts remain unknown; omission keeps the provider's native defaults.
//! This module encodes protocol fields and validates explicit selections.
use anyhow::{Result, bail, ensure};
use serde_json::{Map, Value, json};

use crate::{
    catalog::{CatalogModel, ModelCatalog, model_catalog},
    types::{ChatRequest, ReasoningConfig, ReasoningEffort},
};

pub(crate) type Fields = Map<String, Value>;

fn native_glm_profile(provider: &str) -> bool {
    matches!(provider, "glm" | "zai" | "glm-coding" | "zai-coding")
}

pub(crate) fn selected_off(reasoning: Option<ReasoningConfig>) -> bool {
    matches!(
        reasoning,
        Some(ReasoningConfig::Disabled | ReasoningConfig::Effort(ReasoningEffort::None))
    )
}

pub(crate) fn validate_cap(provider: &str, request: &ChatRequest) -> Result<()> {
    validate_cap_with_catalog(model_catalog().ok().as_deref(), provider, request)
}

pub(crate) fn required_cap_with_catalog(
    catalog: Option<&ModelCatalog>,
    provider: &str,
    request: &ChatRequest,
    product_default: u32,
) -> Result<u32> {
    validate_cap_with_catalog(catalog, provider, request)?;
    if let Some(cap) = request.max_tokens {
        return Ok(cap);
    }
    let limit = resolved_output_limit(catalog, provider, &request.model);
    let cap = limit.map_or(product_default, |limit| {
        u64::from(product_default).min(limit) as u32
    });
    ensure!(
        cap > 0,
        "confirmed model output limit does not allow a positive generation cap"
    );
    Ok(cap)
}

pub(crate) fn validate_cap_with_catalog(
    catalog: Option<&ModelCatalog>,
    provider: &str,
    request: &ChatRequest,
) -> Result<()> {
    if let Some(cap) = request.max_tokens {
        ensure!(cap > 0, "generation cap must be positive");
        let limit = resolved_output_limit(catalog, provider, &request.model);
        ensure!(
            limit.is_none_or(|limit| u64::from(cap) <= limit),
            "generation cap exceeds the current catalog limit for {provider}/{}",
            request.model
        );
    }
    Ok(())
}

fn resolved_output_limit(catalog: Option<&ModelCatalog>, provider: &str, id: &str) -> Option<u64> {
    // Discovery is the raw snapshot owned by the current provider authority.
    // Read it before projected native models or the public catalog.
    crate::attachments::admission::current()
        .and_then(|state| state.model(id))
        .and_then(|model| model.limits.max_output_tokens)
        .or_else(|| {
            native_model(provider, id)
                .as_ref()
                .filter(|model| {
                    provider != "vercel-ai-gateway"
                        || model
                            .metadata
                            .get("vercelLimitOrigins")
                            .is_some_and(|origins| origins["maxTokens"]["kind"] == "source")
                })
                .map(|model| model.max_tokens)
                .filter(|limit| *limit > 0)
        })
        .or_else(|| {
            uses_public_catalog(provider)
                .then(|| catalog.and_then(|catalog| catalog.limits(provider, id).max_output))
                .flatten()
        })
}

pub(crate) fn chat_fields(provider: &str, request: &ChatRequest) -> Result<Fields> {
    chat_fields_from_catalog(model_catalog().ok().as_deref(), provider, request)
}

// Models facts stay in the existing authority-bound provider instance. Scope
// them only while preparing/sending that provider's request, never globally.
pub(crate) type NativeReasoning = std::collections::BTreeMap<String, Option<bool>>;
tokio::task_local! {
    static NATIVE_REASONING: (String, bool, std::collections::BTreeMap<String, NativeReasoning>);
    static NATIVE_MODELS: (String, bool, std::collections::BTreeMap<String, CatalogModel>);
}
/// Model source facts, including unknowns, from the selected authority. Never installed
/// in the public catalog, and are retained only through request preparation.
pub(crate) async fn with_native_models<T>(
    provider: &str,
    public_catalog: bool,
    models: std::collections::BTreeMap<String, CatalogModel>,
    future: impl std::future::Future<Output = T>,
) -> T {
    NATIVE_MODELS
        .scope((provider.into(), public_catalog, models), future)
        .await
}
pub(crate) fn native_model(provider: &str, model: &str) -> Option<CatalogModel> {
    NATIVE_MODELS
        .try_with(|(owner, _, models)| {
            (owner == provider)
                .then(|| models.get(model).cloned())
                .flatten()
        })
        .ok()
        .flatten()
}
pub(crate) fn uses_public_catalog(provider: &str) -> bool {
    NATIVE_MODELS
        .try_with(|(owner, public, _)| owner != provider || *public)
        .unwrap_or_else(|_| public_reasoning_catalog(provider))
}
pub(crate) async fn with_native_reasoning<T>(
    provider: &str,
    public_catalog: bool,
    models: std::collections::BTreeMap<String, NativeReasoning>,
    future: impl std::future::Future<Output = T>,
) -> T {
    NATIVE_REASONING
        .scope((provider.into(), public_catalog, models), future)
        .await
}
fn native_reasoning(provider: &str, model: &str) -> NativeReasoning {
    NATIVE_REASONING
        .try_with(|(owner, _, models)| {
            if owner == provider {
                models.get(model).cloned().unwrap_or_default()
            } else {
                Default::default()
            }
        })
        .unwrap_or_default()
}
fn public_reasoning_catalog(provider: &str) -> bool {
    NATIVE_REASONING
        .try_with(|(owner, public, _)| owner != provider || *public)
        .unwrap_or(true)
}

/// Project authority-native facts over the existing catalog for the protocol
/// consumers. A provider-only model needs no catalog identity to expose controls.
pub(crate) fn reasoning_model(
    provider: &str,
    entry: Option<&CatalogModel>,
    native: &NativeReasoning,
) -> Option<CatalogModel> {
    let fact = |key: &str| native.get(key).copied().flatten();
    let positive_level = native.iter().any(|(key, value)| {
        key.strip_prefix("effort.")
            .is_some_and(|effort| ReasoningEffort::from_str(effort).is_some())
            && *value == Some(true)
    });
    let positive_mode = fact("thinking.supported") != Some(false)
        && native
            .iter()
            .any(|(key, value)| key.starts_with("thinking.types.") && *value == Some(true));
    let native_support = fact("reasoning.supported")
        .or_else(|| {
            (provider == "gemini")
                .then(|| fact("thinking.supported"))
                .flatten()
        })
        .or_else(|| {
            (fact("effort.supported") == Some(false) && fact("thinking.supported") == Some(false))
                .then_some(false)
        })
        .or_else(|| {
            (fact("effort.supported") == Some(true)
                || fact("thinking.supported") == Some(true)
                || fact("effort.supported") != Some(false) && positive_level
                || positive_mode
                || fact("supports_token_budget") == Some(true))
            .then_some(true)
        });
    let mut model = match entry {
        Some(entry) => entry.clone(),
        None if native_support.is_some() => CatalogModel {
            id: String::new(),
            name: String::new(),
            provider: provider.into(),
            api: match provider {
                "anthropic" => "anthropic-messages",
                "gemini" => "google-generative-ai",
                "bedrock" => "bedrock-converse",
                "vercel-ai-gateway" => "openai-responses",
                _ => "openai-completions",
            }
            .into(),
            base_url: String::new(),
            context_window: 0,
            max_tokens: 0,
            reasoning: false,
            input: Vec::new(),
            cost: Value::Null,
            metadata: Default::default(),
        },
        None => return None,
    };
    if let Some(supported) = native_support {
        model.reasoning = supported;
    }
    let compat = model
        .metadata
        .entry("compat".into())
        .or_insert_with(|| json!({}));
    if !compat.is_object() {
        *compat = json!({});
    }
    if let Some(supported) = fact("effort.supported").or(fact("effort.enum")) {
        compat["supportsReasoningEffort"] = json!(supported);
    } else if positive_level {
        compat["supportsReasoningEffort"] = json!(true);
    }
    if let Some(adaptive) = fact("thinking.types.adaptive") {
        compat["forceAdaptiveThinking"] = json!(adaptive);
    }
    if fact("thinking.supported") == Some(false) {
        compat["forceAdaptiveThinking"] = json!(false);
    }
    // A complete provider enum is closed. Remove inherited mappings before
    // projecting individual positives/negatives; partial facts override only keys.
    if fact("effort.enum") == Some(true) {
        model.metadata.insert("thinkingLevelMap".into(), json!({}));
        if let Some(source) = model
            .metadata
            .get_mut("sourceGeneration")
            .and_then(Value::as_object_mut)
        {
            if let Some(options) = source
                .get_mut("reasoningOptions")
                .and_then(Value::as_array_mut)
            {
                options.retain(|option| option["type"] != "effort");
            }
        }
    }
    for (key, supported) in native {
        let Some(level) = key
            .strip_prefix("effort.")
            .filter(|effort| ReasoningEffort::from_str(effort).is_some())
        else {
            continue;
        };
        let Some(supported) = supported else { continue };
        let map = model
            .metadata
            .entry("thinkingLevelMap".into())
            .or_insert_with(|| json!({}));
        if !map.is_object() {
            *map = json!({});
        }
        map[if level == "none" { "off" } else { level }] = if *supported {
            json!(level)
        } else {
            Value::Null
        };
    }
    // Disabled mode is independent of qualitative effort in Messages/Converse.
    if let Some(disabled) = fact("thinking.types.disabled") {
        let map = model
            .metadata
            .entry("thinkingLevelMap".into())
            .or_insert_with(|| json!({}));
        if !map.is_object() {
            *map = json!({});
        }
        map["off"] = if disabled { json!("none") } else { Value::Null };
    }
    if fact("mandatory") == Some(true) {
        let map = model
            .metadata
            .entry("thinkingLevelMap".into())
            .or_insert_with(|| json!({}));
        if !map.is_object() {
            *map = json!({});
        }
        map["off"] = Value::Null;
    }
    Some(model)
}

fn resolved_reasoning_model(
    catalog: Option<&ModelCatalog>,
    provider: &str,
    id: &str,
) -> Option<CatalogModel> {
    let source = uses_public_catalog(provider)
        .then(|| catalog.and_then(|catalog| catalog.model(provider, id)))
        .flatten();
    let native = native_model(provider, id);
    let mut entry = native.clone().or_else(|| source.cloned());
    if let (Some(native), Some(source), Some(entry)) = (native.as_ref(), source, entry.as_mut()) {
        // Vercel's concrete runtime shape uses false for an absent capability.
        // Only raw endpoint evidence can make that an authoritative denial.
        if provider == "vercel-ai-gateway"
            && native_reasoning(provider, id)
                .get("reasoning.supported")
                .copied()
                .flatten()
                .is_none()
        {
            entry.reasoning = source.reasoning;
        }
        // Native model snapshots are sparse for individual generation fields.
        // Fill missing/null fields, retaining explicit false and map null vetoes.
        for key in ["compat", "sourceGeneration"] {
            if let Some(source) = source.metadata.get(key).and_then(Value::as_object) {
                let target = entry
                    .metadata
                    .entry(key.into())
                    .or_insert_with(|| json!({}));
                if target.is_null() {
                    *target = json!({});
                }
                if let Some(target) = target.as_object_mut() {
                    for (field, value) in source {
                        if target.get(field).is_none_or(Value::is_null) {
                            target.insert(field.clone(), value.clone());
                        }
                    }
                }
            }
        }
        let native_options = native
            .metadata
            .get("sourceGeneration")
            .and_then(|source| source.get("reasoningOptions"));
        // An explicit native array, including empty, is a closed control list.
        if !native_options.is_some_and(Value::is_array) {
            if let Some(source) = source
                .metadata
                .get("thinkingLevelMap")
                .and_then(Value::as_object)
            {
                let target = entry
                    .metadata
                    .entry("thinkingLevelMap".into())
                    .or_insert_with(|| json!({}));
                if target.is_null() {
                    *target = json!({});
                }
                if let Some(target) = target.as_object_mut() {
                    for (field, value) in source {
                        target.entry(field.clone()).or_insert_with(|| value.clone());
                    }
                }
            }
        }
    }
    reasoning_model(provider, entry.as_ref(), &native_reasoning(provider, id))
}

fn validate_native_reasoning(
    provider: &str,
    request: &ChatRequest,
    fields: &Fields,
    native: &NativeReasoning,
) -> Result<()> {
    let denies = |key: &str| native.get(key) == Some(&Some(false));
    ensure!(
        request.reasoning.is_none() || !denies("reasoning.supported"),
        "native model facts deny reasoning support"
    );
    ensure!(
        !selected_off(request.reasoning) || native.get("mandatory") != Some(&Some(true)),
        "native model facts require reasoning; off cannot be honored"
    );
    if let Some(reasoning) = request.reasoning {
        let effort = match reasoning {
            ReasoningConfig::Disabled => "none",
            ReasoningConfig::Effort(e) => e.as_str(),
        };
        ensure!(
            native.get("effort.enum") != Some(&Some(true))
                || native.get(&format!("effort.{effort}")) == Some(&Some(true))
                || selected_off(request.reasoning)
                    && native.get("thinking.types.disabled") == Some(&Some(true)),
            "native model facts closed effort list does not include selected effort"
        );
        ensure!(
            !denies(&format!("effort.{effort}")),
            "native model facts deny selected effort"
        );
        ensure!(
            selected_off(request.reasoning)
                || !denies("effort.supported") && !denies("effort.enum"),
            "native model facts deny effort selection"
        );
    }
    ensure!(
        !matches!(provider, "anthropic" | "bedrock")
            || !selected_off(request.reasoning)
            || !denies("thinking.types.disabled"),
        "native Models API denies disabled thinking"
    );
    ensure!(
        provider != "gemini" || request.reasoning.is_none() || !denies("thinking.supported"),
        "native Models API denies thinking support"
    );
    if provider == "openrouter" {
        ensure!(
            request.reasoning.is_none() || !denies("reasoning.supported"),
            "native OpenRouter metadata denies reasoning support"
        );
        ensure!(
            !selected_off(request.reasoning) || native.get("mandatory") != Some(&Some(true)),
            "native OpenRouter metadata requires reasoning; off cannot be honored"
        );
        ensure!(
            request.reasoning.is_none() || !denies("effort.enum"),
            "native OpenRouter metadata does not expose effort selection"
        );
        if let Some(selected) = request.reasoning {
            let selected = match selected {
                ReasoningConfig::Disabled => "none",
                ReasoningConfig::Effort(e) => e.as_str(),
            };
            ensure!(
                !denies(&format!("effort.{selected}")),
                "native OpenRouter enum denies requested effort"
            );
        }
        if let Some(effort) = fields.get("reasoning").and_then(|v| v["effort"].as_str()) {
            ensure!(
                matches!(
                    effort,
                    "max" | "xhigh" | "high" | "medium" | "low" | "minimal" | "none"
                ),
                "invalid OpenRouter reasoning effort enum"
            );
            ensure!(
                !denies(&format!("effort.{effort}")),
                "native OpenRouter enum denies selected effort"
            );
            ensure!(
                !selected_off(request.reasoning) || effort == "none",
                "OpenRouter off mapping must disable reasoning"
            );
        }
    }
    for effort in [
        fields
            .get("output_config")
            .and_then(|value| value["effort"].as_str()),
        fields
            .get("reasoning")
            .and_then(|value| value["effort"].as_str()),
        fields.get("reasoning_effort").and_then(Value::as_str),
    ]
    .into_iter()
    .flatten()
    {
        ensure!(
            (selected_off(request.reasoning) || !denies("effort.supported"))
                && !denies(&format!("effort.{effort}")),
            "native Models API denies selected effort"
        );
    }
    if let Some(mode) = fields
        .get("thinking")
        .and_then(|v| v["type"].as_str())
        .filter(|mode| *mode != "disabled")
    {
        ensure!(
            !denies("thinking.supported") && !denies(&format!("thinking.types.{mode}")),
            "native Models API denies selected thinking mode"
        );
    }
    Ok(())
}

pub(crate) fn chat_fields_from_catalog(
    catalog: Option<&ModelCatalog>,
    provider: &str,
    request: &ChatRequest,
) -> Result<Fields> {
    validate_cap_with_catalog(catalog, provider, request)?;
    let native = native_reasoning(provider, &request.model);
    let model = resolved_reasoning_model(catalog, provider, &request.model);
    validate_native_reasoning(provider, request, &Fields::new(), &native)?;
    let fields = chat_fields_with_model(provider, request, model.as_ref())?;
    validate_native_reasoning(provider, request, &fields, &native)?;
    Ok(fields)
}

fn mapped_effort(
    model: Option<&CatalogModel>,
    reasoning: ReasoningConfig,
) -> Result<Option<String>> {
    let key = match reasoning {
        ReasoningConfig::Disabled | ReasoningConfig::Effort(ReasoningEffort::None) => "off",
        ReasoningConfig::Effort(effort) => effort.as_str(),
    };
    if let Some(value) = model
        .and_then(|m| m.metadata.get("thinkingLevelMap"))
        .and_then(|map| map.get(key))
    {
        let Some(value) = value.as_str() else {
            bail!("selected reasoning `{key}` is unsupported by the model's catalog thinking map")
        };
        return Ok(Some(value.into()));
    }
    // A partial map only overrides its keys; preserve the source's explicit
    // effort names as an identity map when the selected protocol supports them.
    let wire = if key == "off" { "none" } else { key };
    if model
        .and_then(|m| m.metadata.get("sourceGeneration"))
        .and_then(|s| s["reasoningOptions"].as_array())
        .is_some_and(|options| {
            options
                .iter()
                .filter(|o| o["type"] == "effort")
                .flat_map(|o| o["values"].as_array().into_iter().flatten())
                .any(|value| value.as_str() == Some(wire))
        })
    {
        return Ok(Some(wire.into()));
    }
    Ok(None)
}

fn openai_fields(
    provider: &str,
    request: &ChatRequest,
    model: Option<&CatalogModel>,
) -> Result<Fields> {
    openai_api_fields(provider, request, model, false)
}

pub(crate) fn responses_fields(provider: &str, request: &ChatRequest) -> Result<Fields> {
    // Reuse the existing authority-scoped catalog fixture without publishing a
    // process-global test catalog. Production still reads the runtime snapshot.
    #[cfg(test)]
    let fixture_catalog = crate::attachments::admission::current().and_then(|s| s.catalog.clone());
    #[cfg(not(test))]
    let fixture_catalog: Option<std::sync::Arc<ModelCatalog>> = None;
    let catalog = fixture_catalog.or_else(|| model_catalog().ok());
    responses_fields_from_catalog(catalog.as_deref(), provider, request)
}

fn responses_fields_from_catalog(
    catalog: Option<&ModelCatalog>,
    provider: &str,
    request: &ChatRequest,
) -> Result<Fields> {
    ensure!(
        request
            .temperature
            .is_none_or(|value| value.is_finite() && (0.0..=2.0).contains(&value)),
        "Responses temperature must be finite and within 0..=2"
    );
    validate_cap_with_catalog(catalog, provider, request)?;
    let model = resolved_reasoning_model(catalog, provider, &request.model);
    let native = native_reasoning(provider, &request.model);
    validate_native_reasoning(provider, request, &Fields::new(), &native)?;
    let fields = if provider == "openai" {
        openai_api_fields(provider, request, model.as_ref(), true)?
    } else {
        responses_fields_with_model(provider, request, model.as_ref())?
    };
    validate_native_reasoning(provider, request, &fields, &native)?;
    Ok(fields)
}

fn responses_fields_with_model(
    provider: &str,
    request: &ChatRequest,
    model: Option<&CatalogModel>,
) -> Result<Fields> {
    if provider == "vercel-ai-gateway" {
        ensure!(
            request
                .temperature
                .is_none_or(|value| value.is_finite() && (0.0..=2.0).contains(&value)),
            "Responses temperature must be finite and within 0..=2"
        );
    }
    validate_temperature(model, request)?;
    let mut fields = Fields::new();
    // This is a Responses protocol field, independent of model-specific
    // sampling/reasoning support. Known output ceilings were checked above.
    if let Some(cap) = request.max_tokens {
        fields.insert("max_output_tokens".into(), json!(cap));
    }
    if request.reasoning.is_none() && request.temperature.is_none() {
        return Ok(fields); // Omitted sampling/reasoning use the service defaults.
    }
    if let Some(reasoning) = request.reasoning {
        let model = model.ok_or_else(|| anyhow::anyhow!("unverified Responses effort contract"))?;
        ensure!(model.reasoning, "selected model does not support reasoning");
        let toggle_off = provider == "vercel-ai-gateway"
            && selected_off(Some(reasoning))
            && model
                .metadata
                .get("sourceGeneration")
                .and_then(|source| source["reasoningOptions"].as_array())
                .is_some_and(|options| options.iter().any(|option| option["type"] == "toggle"));
        ensure!(
            toggle_off
                || model
                    .metadata
                    .get("compat")
                    .is_none_or(|c| c["supportsReasoningEffort"] != false),
            "selected model explicitly does not support reasoning effort"
        );
        let effort = if toggle_off {
            // Encode the catalog's off control using the chosen shared `none`
            // mapping (see compatibility.rs). An effort enum alone cannot
            // establish an off control or a reasoning default.
            Some("none".into())
        } else {
            mapped_effort(Some(model), reasoning)?
        }
        .ok_or_else(|| anyhow::anyhow!("unverified Responses effort contract"))?;
        let native = native_reasoning(provider, &request.model);
        let denies = |key: &str| native.get(key) == Some(&Some(false));
        let selected = match reasoning {
            ReasoningConfig::Disabled => "none",
            ReasoningConfig::Effort(effort) => effort.as_str(),
        };
        // A catalog mapping for another API is not a Responses contract.
        // A selected control advertised by this authority remains authoritative.
        ensure!(
            model.api == "openai-responses"
                || native.get(&format!("effort.{selected}")) == Some(&Some(true))
                || selected_off(request.reasoning)
                    && native.get("thinking.types.disabled") == Some(&Some(true)),
            "catalog model uses a different API profile; unverified Responses effort contract"
        );
        ensure!(
            !denies("reasoning.supported")
                && (toggle_off || !denies("effort.supported") && !denies("effort.enum"))
                && !denies(&format!("effort.{selected}"))
                && !denies(&format!("effort.{effort}")),
            "native model facts deny selected Responses effort"
        );
        ensure!(
            !selected_off(request.reasoning) || native.get("mandatory") != Some(&Some(true)),
            "native model facts require reasoning; off cannot be honored"
        );
        ensure!(
            matches!(
                effort.as_str(),
                "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
            ),
            "unsupported Responses effort mapping"
        );
        ensure!(
            !selected_off(request.reasoning) || effort == "none",
            "off mapping must disable reasoning"
        );
        fields.insert("reasoning".into(), json!({"effort":effort}));
    }
    if let Some(temperature) = request.temperature {
        fields.insert("temperature".into(), json!(temperature));
    }
    Ok(fields)
}

fn openai_api_fields(
    _provider: &str,
    request: &ChatRequest,
    model: Option<&CatalogModel>,
    responses: bool,
) -> Result<Fields> {
    validate_temperature(model, request)?;
    let id = request.model.as_str();
    let reasoner = model.is_some_and(|model| model.reasoning);
    let mut fields = Fields::new();
    let compat = model.and_then(|m| m.metadata.get("compat"));
    let cap_field = if responses {
        "max_output_tokens"
    } else {
        compat
            .and_then(|c| c["maxTokensField"].as_str())
            .unwrap_or(if reasoner {
                "max_completion_tokens"
            } else {
                "max_tokens"
            })
    };
    ensure!(
        matches!(cap_field, "max_tokens" | "max_completion_tokens")
            || responses && cap_field == "max_output_tokens",
        "unsupported Chat cap field {cap_field}"
    );
    if let Some(cap) = request.max_tokens {
        fields.insert(cap_field.into(), json!(cap));
    }
    if let Some(reasoning) = request.reasoning {
        ensure!(
            model.is_none_or(|model| model.reasoning),
            "model `{id}` does not support reasoning"
        );
        ensure!(
            compat.is_none_or(|compat| compat["supportsReasoningEffort"] != false),
            "selected OpenAI profile explicitly does not support reasoning effort"
        );
        let effort = mapped_effort(model, reasoning)?.ok_or_else(|| {
            anyhow::anyhow!("model `{id}` has no confirmed mapping for selected reasoning effort")
        })?;
        ensure!(
            matches!(
                effort.as_str(),
                "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
            ),
            "unsupported OpenAI effort mapping `{effort}`"
        );
        ensure!(
            !selected_off(request.reasoning) || effort == "none",
            "off mapping must disable reasoning"
        );
        if responses {
            fields.insert("reasoning".into(), json!({"effort":effort}));
        } else {
            fields.insert("reasoning_effort".into(), json!(effort));
        }
    }
    if let Some(temperature) = request.temperature {
        fields.insert("temperature".into(), json!(temperature));
    }
    Ok(fields)
}

fn chat_fields_with_model(
    provider: &str,
    request: &ChatRequest,
    model: Option<&CatalogModel>,
) -> Result<Fields> {
    if matches!(provider, "openai" | "azure-openai" | "azure_openai") {
        return openai_fields(provider, request, model);
    }
    let compat = model.and_then(|m| m.metadata.get("compat"));
    let mut fields = Fields::new();
    // Compatible is a protocol profile, not OpenAI model identity. In particular
    // a hosted DeepSeek/GLM model is not the direct vendor contract.
    let cap_field = compat
        .and_then(|c| c["maxTokensField"].as_str())
        .unwrap_or("max_tokens");
    ensure!(
        matches!(cap_field, "max_tokens" | "max_completion_tokens"),
        "unsupported Chat cap field {cap_field}"
    );
    if let Some(cap) = request.max_tokens {
        fields.insert(cap_field.into(), json!(cap));
        // SiliconFlow counts visible output and thoughts separately. Split the
        // already admitted total reserve instead of rejecting every normal
        // request to a reasoning model. The server default thinking mode stays
        // unchanged; only its documented numeric upper bound is supplied.
        // https://docs.siliconflow.cn/docs/api/chat-completions-post
        if provider == "siliconflow"
            && !selected_off(request.reasoning)
            && model.is_some_and(|m| m.reasoning)
        {
            ensure!(
                cap >= 129,
                "SiliconFlow total generation reserve must allow at least 128 thinking tokens and one output token"
            );
            let thinking = 4096.min((cap / 2).max(128));
            fields.insert("thinking_budget".into(), json!(thinking));
            fields.insert(cap_field.into(), json!(cap - thinking));
        }
    }
    if let Some(t) = request.temperature {
        ensure!(
            compat.is_none_or(|c| c["supportsTemperature"] != false)
                && model.is_none_or(|m| m
                    .metadata
                    .get("sourceGeneration")
                    .is_none_or(|s| s["temperature"] != false)),
            "model does not support temperature"
        );
        fields.insert("temperature".into(), json!(t));
    }
    let Some(reasoning) = request.reasoning else {
        return Ok(fields);
    };
    let off = selected_off(request.reasoning);
    ensure!(
        model.is_none_or(|model| model.reasoning),
        "selected model does not support reasoning"
    );
    ensure!(
        off || compat.is_none_or(|compat| compat["supportsReasoningEffort"] != false),
        "selected profile explicitly does not support qualitative reasoning effort"
    );
    let mapped = mapped_effort(model, reasoning)?;
    ensure!(
        provider == "openrouter"
            // https://docs.mistral.ai/api/endpoint/chat: the Chat effort field
            // exists even when the source's preferred API is Conversations.
            || provider == "mistral" && mapped.is_some()
            || model.is_none_or(|m| !matches!(
                m.api.as_str(),
                "anthropic-messages" | "mistral-conversations"
            )),
        "catalog model uses a different API profile; this Chat adapter has no documented reasoning translation"
    );
    let toggle_off = off && compat.is_some_and(|compat| compat["supportsThinkingToggle"] == true)
        || off
            && model
                .and_then(|model| model.metadata.get("sourceGeneration"))
                .and_then(|source| source["reasoningOptions"].as_array())
                .is_some_and(|options| options.iter().any(|option| option["type"] == "toggle"));
    let effort = mapped
        .clone()
        .or_else(|| toggle_off.then(|| "none".into()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "{provider}/{} has no confirmed mapping for selected reasoning",
                request.model
            )
        })?;
    let format = if provider == "deepseek" {
        "deepseek"
    } else if native_glm_profile(provider) {
        "zai"
    } else if provider == "siliconflow" {
        "siliconflow"
    } else if provider == "openrouter" {
        "openrouter"
    } else {
        compat
            .and_then(|c| c["thinkingFormat"].as_str())
            .unwrap_or("openai")
    };
    match format {
        "novita" => {
            ensure!(
                off,
                "Novita enable_thinking is a toggle, not qualitative effort"
            );
            ensure!(
                compat.is_some_and(|c| c["supportsThinkingToggle"] == true),
                "Novita model has no verified thinking off control"
            );
            fields.insert("enable_thinking".into(), json!(false));
        }
        "siliconflow" => {
            ensure!(
                off,
                "SiliconFlow thinking requires a separate numeric budget; qualitative effort cannot supply one"
            );
            ensure!(
                compat.is_some_and(|c| c["supportsThinkingToggle"] == true),
                "SiliconFlow model does not document a thinking off control"
            );
            fields.insert("enable_thinking".into(), json!(false));
        }
        "deepseek" | "zai" => {
            fields.insert(
                "thinking".into(),
                json!({"type":if off {"disabled"} else {"enabled"}}),
            );
            if !off {
                ensure!(
                    matches!(effort.as_str(), "low" | "high" | "max"),
                    "invalid thinking protocol effort `{effort}`"
                );
                fields.insert("reasoning_effort".into(), json!(effort));
                if format == "deepseek" {
                    ensure!(
                        request.temperature.is_none(),
                        "DeepSeek thinking mode does not support temperature"
                    );
                }
            }
        }
        "openrouter" => {
            ensure!(
                model.is_some(),
                "OpenRouter reasoning requires catalog model metadata"
            );
            ensure!(
                !off || mapped.is_some(),
                "OpenRouter model has no verified reasoning off mapping"
            );
            fields.insert("reasoning".into(), json!({"effort":effort}));
        }
        "together" => {
            ensure!(
                model.is_some(),
                "Together thinking requires catalog model metadata"
            );
            if off {
                ensure!(
                    mapped.is_some()
                        || model
                            .and_then(|m| m.metadata.get("sourceGeneration"))
                            .and_then(|s| s["reasoningOptions"].as_array())
                            .is_some_and(|options| options.iter().any(|o| o["type"] == "toggle")),
                    "Together model has no verified thinking off control"
                );
            }
            fields.insert("reasoning".into(), json!({"enabled":!off}));
            if !off {
                ensure!(
                    compat.is_some_and(|c| c["supportsReasoningEffort"] == true)
                        && mapped.is_some(),
                    "model supports thinking toggle, not effort"
                );
                fields.insert("reasoning_effort".into(), json!(effort));
            }
        }
        "qwen-chat-template" => {
            ensure!(
                off,
                "model supports thinking toggle, not qualitative effort"
            );
            fields.insert(
                "chat_template_kwargs".into(),
                json!({"enable_thinking":false}),
            );
        }
        "qwen" => {
            fields.insert("enable_thinking".into(), json!(!off));
            if !off {
                ensure!(
                    compat.is_some_and(|c| c["supportsReasoningEffort"] == true)
                        && mapped.is_some(),
                    "model does not document selected effort"
                );
                fields.insert("reasoning_effort".into(), json!(effort));
            }
        }
        "openai" | "string-thinking" => {
            if provider == "cohere" {
                ensure!(
                    matches!(effort.as_str(), "none" | "high"),
                    "Cohere compatibility reasoning_effort supports only none/high"
                );
            }
            ensure!(
                (provider == "ollama" && off
                    || compat.is_none_or(|c| c["supportsReasoningEffort"] != false))
                    && mapped.is_some(),
                "{provider}/{} has no documented mapping for selected reasoning `{effort}`",
                request.model
            );
            fields.insert(
                if format == "string-thinking" {
                    "thinking"
                } else {
                    "reasoning_effort"
                }
                .into(),
                json!(effort),
            );
        }
        _ => bail!(
            "thinking format `{format}` is not implemented by the Chat adapter; selected setting cannot be sent"
        ),
    }
    Ok(fields)
}

pub(crate) fn gemini_thinking_with_catalog(
    catalog: Option<&ModelCatalog>,
    request: &ChatRequest,
) -> Result<Option<Value>> {
    let native = native_reasoning("gemini", &request.model);
    let model = resolved_reasoning_model(catalog, "gemini", &request.model);
    validate_temperature(model.as_ref(), request)?;
    let fields = gemini_thinking_with_model(model.as_ref(), request)?;
    validate_native_reasoning("gemini", request, &Fields::new(), &native)?;
    Ok(fields)
}

fn gemini_thinking_with_model(
    model: Option<&CatalogModel>,
    request: &ChatRequest,
) -> Result<Option<Value>> {
    let Some(reasoning) = request.reasoning else {
        return Ok(None);
    };
    ensure!(
        model.is_none_or(|m| m.reasoning),
        "catalog model does not support thinking controls"
    );
    ensure!(
        model
            .and_then(|model| model.metadata.get("compat"))
            .is_none_or(|compat| compat["supportsReasoningEffort"] != false),
        "selected Gemini profile explicitly does not support reasoning effort"
    );
    let mapped = mapped_effort(model, reasoning)?.ok_or_else(|| {
        anyhow::anyhow!("Gemini model has no confirmed mapping for selected thinking control")
    })?;
    if selected_off(request.reasoning) {
        ensure!(
            matches!(mapped.as_str(), "none" | "0"),
            "off mapping must disable thinking"
        );
        return Ok(Some(json!({"thinkingBudget":0})));
    }
    let level = mapped.to_ascii_uppercase();
    ensure!(
        matches!(level.as_str(), "MINIMAL" | "LOW" | "MEDIUM" | "HIGH"),
        "invalid native generateContent ThinkingLevel `{level}`"
    );
    Ok(Some(json!({"thinkingLevel":level})))
}

#[cfg(test)]
pub(crate) fn test_request(model: &str) -> ChatRequest {
    ChatRequest {
        model: model.into(),
        messages: vec![crate::types::ChatMessage::user("Hello")],
        temperature: None,
        max_tokens: Some(1024),
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: None,
    }
}

/// Explicit negative source capabilities apply to every native serializer.
fn validate_temperature(model: Option<&CatalogModel>, request: &ChatRequest) -> Result<()> {
    if request.temperature.is_some() {
        ensure!(
            model.is_none_or(|m| m
                .metadata
                .get("compat")
                .is_none_or(|c| c["supportsTemperature"] != false)
                && m.metadata
                    .get("sourceGeneration")
                    .is_none_or(|s| s["temperature"] != false)),
            "selected model explicitly does not support temperature"
        );
    }
    Ok(())
}

/// Messages/Converse Claude caps include thinking and visible output. Adaptive
/// thinking does not create a second reserve outside that cap.
/// https://platform.claude.com/docs/en/build-with-claude/thinking
/// https://platform.claude.com/docs/en/build-with-claude/effort
pub(crate) fn anthropic_fields_with_catalog(
    catalog: Option<&ModelCatalog>,
    provider: &str,
    request: &ChatRequest,
) -> Result<Fields> {
    validate_cap_with_catalog(catalog, provider, request)?;
    let native = native_reasoning(provider, &request.model);
    let model = resolved_reasoning_model(catalog, provider, &request.model);
    let fields = anthropic_fields_with_model(provider, request, model.as_ref())?;
    validate_native_reasoning(provider, request, &fields, &native)?;
    Ok(fields)
}

fn anthropic_fields_with_model(
    provider: &str,
    request: &ChatRequest,
    model: Option<&CatalogModel>,
) -> Result<Fields> {
    validate_temperature(model, request)?;
    let compat = model.and_then(|m| m.metadata.get("compat"));
    let mut fields = Fields::new();
    let Some(reasoning) = request.reasoning else {
        return Ok(fields);
    };
    ensure!(
        model.is_none_or(|model| model.reasoning),
        "selected model does not support reasoning controls"
    );
    let wire = mapped_effort(model, reasoning)?.ok_or_else(|| {
        anyhow::anyhow!("model has no confirmed mapping for selected thinking control")
    })?;
    if selected_off(request.reasoning) {
        ensure!(wire == "none", "off mapping must disable thinking");
        fields.insert("thinking".into(), json!({"type":"disabled"}));
        return Ok(fields);
    }
    ensure!(
        compat.is_none_or(|compat| compat["supportsReasoningEffort"] != false),
        "profile explicitly does not support reasoning effort"
    );
    ensure!(
        matches!(wire.as_str(), "low" | "medium" | "high" | "xhigh" | "max"),
        "invalid Claude effort enum `{wire}`"
    );
    fields.insert("output_config".into(), json!({"effort":wire}));
    if provider == "bedrock" {
        // This beta is required by the Opus 4.5 wire contract, only after
        // provider/catalog facts admit the selected effort above. It supplies
        // no capability facts for this or any other model.
        // https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-anthropic-claude-messages-request-response.html
        let id = ["us.", "eu.", "jp.", "au.", "global."]
            .iter()
            .find_map(|prefix| request.model.strip_prefix(prefix))
            .unwrap_or(&request.model);
        if matches!(
            id,
            "anthropic.claude-opus-4-5" | "anthropic.claude-opus-4-5-20251101-v1:0"
        ) {
            fields.insert("anthropic_beta".into(), json!(["effort-2025-11-24"]));
        }
    }
    if compat.is_some_and(|compat| compat["forceAdaptiveThinking"] == true) {
        ensure!(
            request.temperature.is_none(),
            "Claude thinking does not support a sampling temperature setting"
        );
        fields.insert("thinking".into(), json!({"type":"adaptive"}));
    }
    Ok(fields)
}

/// One effective-mode decision for budget projection, history preparation and
/// replay validation. Explicit none and Disabled both override server default.
pub(crate) fn deepseek_effective_thinking(request: &ChatRequest) -> bool {
    if selected_off(request.reasoning) {
        return false;
    }
    crate::history::deepseek_thinking_required(
        &request.model,
        request.reasoning.is_some()
            || native_reasoning("deepseek", &request.model).get("default_enabled")
                == Some(&Some(true))
            || native_reasoning("deepseek", &request.model).get("mandatory") == Some(&Some(true)),
        &request.messages,
    )
}

/// Discovery uses the very same protocol decision as generation. This request
/// contains no tools/sampling/cap: those per-request constraints are checked at
/// send time; catalog limits are still read through ModelCatalog::limits.
pub(crate) fn effort_supported_for_profile(
    provider: &str,
    id: &str,
    model: Option<&CatalogModel>,
    effort: ReasoningEffort,
) -> bool {
    effort_supported_with_native(provider, id, model, effort, &NativeReasoning::new())
}

pub(crate) fn effort_supported_with_native(
    provider: &str,
    id: &str,
    model: Option<&CatalogModel>,
    effort: ReasoningEffort,
    native: &NativeReasoning,
) -> bool {
    let model = reasoning_model(provider, model, native);
    let model = model.as_ref();
    let request = ChatRequest {
        model: id.into(),
        messages: vec![],
        temperature: None,
        max_tokens: None,
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: Some(ReasoningConfig::Effort(effort)),
        compiled_prompt: None,
    };
    let fields = match provider {
        "anthropic" | "bedrock" => anthropic_fields_with_model(provider, &request, model),
        "gemini" => gemini_thinking_with_model(model, &request).map(|_| Fields::new()),
        "vercel-ai-gateway" => responses_fields_with_model(provider, &request, model),
        _ => chat_fields_with_model(provider, &request, model),
    };
    fields
        .is_ok_and(|fields| validate_native_reasoning(provider, &request, &fields, native).is_ok())
}

#[cfg(test)]
pub(crate) fn test_catalog(fresh: bool) -> ModelCatalog {
    if fresh {
        use crate::catalog::generator::{SourceSnapshot, generate};
        let source: SourceSnapshot =
            serde_json::from_str(include_str!("../tests/fixtures/catalog/sources.json")).unwrap();
        let generated = generate(&source, true).unwrap();
        ModelCatalog::parse(
            &serde_json::to_string(&generated.models).unwrap(),
            &serde_json::to_string(&generated.provenance).unwrap(),
        )
        .unwrap()
    } else {
        ModelCatalog::parse(
            include_str!("../tests/fixtures/catalog/models.json"),
            include_str!("../tests/fixtures/catalog/provenance.json"),
        )
        .unwrap()
    }
}

#[cfg(test)]
pub(crate) fn test_catalog_model(
    provider: &str,
    id: &str,
    template: &str,
    metadata: Value,
) -> ModelCatalog {
    let mut models: Value =
        serde_json::from_str(include_str!("../tests/fixtures/catalog/models.json")).unwrap();
    let mut origins: Value =
        serde_json::from_str(include_str!("../tests/fixtures/catalog/provenance.json")).unwrap();
    let catalog_key = crate::catalog::catalog_provider(provider);
    let mut model = models
        .get(&catalog_key)
        .and_then(|models| models.get(template))
        .filter(|model| model.is_object())
        .expect("complete catalog model template")
        .clone();
    let template_model: CatalogModel = serde_json::from_value(model.clone())
        .expect("catalog template has all required model fields");
    let origin = origins
        .get(&catalog_key)
        .and_then(|models| models.get(template))
        .filter(|origin| origin.is_object())
        .expect("catalog provenance template")
        .clone();
    for field in ["contextWindow", "maxTokens"] {
        serde_json::from_value::<crate::catalog::LimitOrigin>(origin[field].clone())
            .expect("complete limit provenance template");
    }
    model["id"] = json!(id);
    for (key, value) in metadata.as_object().unwrap() {
        model[key] = value.clone();
    }
    models[&catalog_key][id] = model;
    origins[&catalog_key][id] = origin;
    let catalog = ModelCatalog::parse(&models.to_string(), &origins.to_string()).unwrap();
    let actual = catalog
        .model(provider, id)
        .expect("runtime provider resolves patched fixture");
    assert_eq!(actual.id, id);
    assert_eq!(
        actual.api,
        metadata
            .get("api")
            .and_then(Value::as_str)
            .unwrap_or(&template_model.api)
    );
    for (key, value) in metadata.as_object().unwrap() {
        assert_eq!(
            &serde_json::to_value(actual).unwrap()[key],
            value,
            "patched catalog field {key}"
        );
    }
    catalog
}

#[cfg(test)]
pub(crate) fn test_discovery(
    catalog: &ModelCatalog,
    provider: &str,
    id: &str,
) -> pioneer_protocol::ProviderModelInfo {
    let mut models = [pioneer_protocol::ProviderModelInfo {
        id: id.into(),
        name: None,
        description: None,
        created: None,
        provider: provider.into(),
        owned_by: None,
        limits: Default::default(),
        capabilities: Default::default(),
        transcription: None,
        pricing: None,
        active: None,
        family: None,
        lifecycle_status: None,
    }];
    catalog.enrich(provider, &mut models);
    models.into_iter().next().unwrap()
}

#[cfg(test)]
pub(crate) fn test_source_temperature(provider: &str, id: &str) -> ModelCatalog {
    use crate::catalog::generator::{SOURCE_URLS, SourceSnapshot, generate};
    let mut source: SourceSnapshot =
        serde_json::from_str(include_str!("../tests/fixtures/catalog/sources.json")).unwrap();
    source.sources.get_mut(SOURCE_URLS[0]).unwrap().body[provider]["models"][id]["temperature"] =
        json!(false);
    let generated = generate(&source, true).unwrap();
    ModelCatalog::parse(
        &serde_json::to_string(&generated.models).unwrap(),
        &serde_json::to_string(&generated.provenance).unwrap(),
    )
    .unwrap()
}

#[cfg(test)]
mod resolved_reasoning_tests {
    use super::*;

    fn model(api: &str, metadata: Value) -> CatalogModel {
        let mut model = CatalogModel {
            id: "unlisted-research-engine-2049".into(),
            name: "Research engine".into(),
            provider: "fixture".into(),
            api: api.into(),
            base_url: String::new(),
            context_window: 8192,
            max_tokens: 4096,
            reasoning: true,
            input: vec!["text".into()],
            cost: Value::Null,
            metadata: Default::default(),
        };
        model.metadata = serde_json::from_value(metadata).unwrap();
        model
    }

    #[test]
    fn unknown_reasoning_keeps_native_defaults_and_rejects_explicit_effort() {
        let mut request = test_request("unlisted-research-engine-2049");
        for provider in ["openai", "azure-openai", "openrouter", "deepseek", "glm"] {
            let fields = chat_fields_from_catalog(None, provider, &request).unwrap();
            assert!(!fields.contains_key("reasoning"));
            assert!(!fields.contains_key("reasoning_effort"));
            assert!(!fields.contains_key("thinking"));
        }
        assert!(responses_fields_from_catalog(None, "openai", &request).is_ok());
        assert!(responses_fields_from_catalog(None, "vercel-ai-gateway", &request).is_ok());
        assert_eq!(gemini_thinking_with_catalog(None, &request).unwrap(), None);
        assert!(
            anthropic_fields_with_catalog(None, "anthropic", &request)
                .unwrap()
                .is_empty()
        );
        request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::High));
        for provider in ["openai", "azure-openai", "openrouter", "deepseek", "glm"] {
            assert!(
                chat_fields_from_catalog(None, provider, &request).is_err(),
                "{provider}"
            );
        }
        assert!(responses_fields_from_catalog(None, "openai", &request).is_err());
        assert!(gemini_thinking_with_catalog(None, &request).is_err());
        assert!(anthropic_fields_with_catalog(None, "anthropic", &request).is_err());
    }

    #[tokio::test]
    async fn arbitrary_provider_model_reaches_each_protocol_without_catalog() {
        let id = "unlisted-research-engine-2049";
        let mut request = test_request(id);
        request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::High));
        for provider in [
            "openai",
            "azure-openai",
            "openrouter",
            "anthropic",
            "bedrock",
            "gemini",
            "vercel-ai-gateway",
        ] {
            let facts = NativeReasoning::from([
                ("effort.high".into(), Some(true)),
                ("effort.enum".into(), Some(true)),
            ]);
            with_native_reasoning(provider, true, [(id.into(), facts)].into(), async {
                let fields = match provider {
                    "anthropic" | "bedrock" => {
                        anthropic_fields_with_catalog(None, provider, &request).unwrap()
                    }
                    "gemini" => {
                        assert_eq!(
                            gemini_thinking_with_catalog(None, &request).unwrap(),
                            Some(json!({"thinkingLevel":"HIGH"}))
                        );
                        return;
                    }
                    "vercel-ai-gateway" => {
                        responses_fields_from_catalog(None, provider, &request).unwrap()
                    }
                    _ => chat_fields_from_catalog(None, provider, &request).unwrap(),
                };
                match provider {
                    "anthropic" | "bedrock" => {
                        assert_eq!(fields["output_config"], json!({"effort":"high"}))
                    }
                    "openrouter" | "vercel-ai-gateway" => {
                        assert_eq!(fields["reasoning"], json!({"effort":"high"}))
                    }
                    _ => assert_eq!(fields["reasoning_effort"], "high"),
                }
                if provider == "openai" {
                    let fields = responses_fields_from_catalog(None, provider, &request).unwrap();
                    assert_eq!(fields["reasoning"], json!({"effort":"high"}));
                    assert_eq!(fields["max_output_tokens"], 1024);
                }
            })
            .await;
        }
    }

    #[tokio::test]
    async fn scoped_native_fields_fill_from_catalog_without_losing_false() {
        let id = "unlisted-research-engine-2049";
        let catalog = test_catalog_model(
            "openai",
            id,
            "gpt-5.4",
            json!({
                "thinkingLevelMap":{"low":"low", "high":"high"},
                "compat":{"supportsTemperature":true, "maxTokensField":"max_completion_tokens"},
                "maxTokens":2048
            }),
        );
        let mut native = model(
            "openai-responses",
            json!({
                "compat":{"supportsTemperature":false},
                "thinkingLevelMap":{"high":null}
            }),
        );
        native.max_tokens = 0; // This native snapshot has no output-limit fact.
        let mut request = test_request(id);
        request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::Low));
        with_native_models("openai", true, [(id.into(), native)].into(), async {
            let fields = responses_fields_from_catalog(Some(&catalog), "openai", &request).unwrap();
            assert_eq!(fields["reasoning"], json!({"effort":"low"}));
            request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::High));
            assert!(responses_fields_from_catalog(Some(&catalog), "openai", &request).is_err());
            request.reasoning = None;
            request.temperature = Some(0.5);
            assert!(responses_fields_from_catalog(Some(&catalog), "openai", &request).is_err());
            request.temperature = None;
            request.max_tokens = Some(2049);
            assert!(validate_cap_with_catalog(Some(&catalog), "openai", &request).is_err());
            request.max_tokens = None;
            assert_eq!(
                required_cap_with_catalog(Some(&catalog), "openai", &request, 4096).unwrap(),
                2048
            );
        })
        .await;
    }

    #[test]
    fn partial_provider_facts_override_only_defined_catalog_fields() {
        let entry = model(
            "openai-responses",
            json!({
                "thinkingLevelMap":{"low":"low", "high":null, "max":"max"},
                "compat":{"supportsReasoningEffort":false}
            }),
        );
        let facts = NativeReasoning::from([
            ("effort.high".into(), Some(true)),
            ("effort.max".into(), Some(false)),
            ("effort.low".into(), None),
        ]);
        let resolved = reasoning_model("openai", Some(&entry), &facts).unwrap();
        let mut request = test_request(&entry.id);
        for (effort, accepted) in [
            (ReasoningEffort::Low, true),
            (ReasoningEffort::High, true),
            (ReasoningEffort::Max, false),
        ] {
            request.reasoning = Some(ReasoningConfig::Effort(effort));
            assert_eq!(
                openai_api_fields("openai", &request, Some(&resolved), true).is_ok(),
                accepted
            );
        }
        // The same catalog's explicit false still wins when native support is unknown.
        let unknown = NativeReasoning::from([("effort.supported".into(), None)]);
        let resolved = reasoning_model("openai", Some(&entry), &unknown).unwrap();
        request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::Low));
        assert!(openai_api_fields("openai", &request, Some(&resolved), true).is_err());
    }

    #[test]
    fn closed_provider_enum_cannot_gain_catalog_efforts() {
        let entry = model(
            "openai-responses",
            json!({
                "thinkingLevelMap":{"low":"low", "high":"high", "off":"none"},
                "sourceGeneration":{"reasoningOptions":[{"type":"effort", "values":["low","high","max"]}]}
            }),
        );
        let facts = NativeReasoning::from([
            ("effort.enum".into(), Some(true)),
            ("effort.high".into(), Some(true)),
        ]);
        let resolved = reasoning_model("openai", Some(&entry), &facts).unwrap();
        for (effort, accepted) in [
            (ReasoningEffort::High, true),
            (ReasoningEffort::Low, false),
            (ReasoningEffort::Max, false),
            (ReasoningEffort::None, false),
        ] {
            assert_eq!(
                effort_supported_with_native("openai", &entry.id, Some(&resolved), effort, &facts),
                accepted
            );
        }
    }

    #[test]
    fn a_catalog_toggle_cannot_expand_a_closed_native_effort_list() {
        let entry = model(
            "openai-responses",
            json!({
                "sourceGeneration":{"reasoningOptions":[{"type":"toggle"}]},
                "thinkingLevelMap":{"off":"none","high":"high"}
            }),
        );
        let mut native = NativeReasoning::from([
            ("effort.enum".into(), Some(true)),
            ("effort.high".into(), Some(true)),
        ]);
        assert!(!effort_supported_with_native(
            "vercel-ai-gateway",
            &entry.id,
            Some(&entry),
            ReasoningEffort::None,
            &native
        ));
        native.insert("thinking.types.disabled".into(), Some(true));
        assert!(effort_supported_with_native(
            "vercel-ai-gateway",
            &entry.id,
            Some(&entry),
            ReasoningEffort::None,
            &native
        ));
    }

    #[test]
    fn catalog_update_alone_changes_effort_and_wire_mapping() {
        let mut request = test_request("unlisted-research-engine-2049");
        request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::High));
        let old = model(
            "google-generative-ai",
            json!({"thinkingLevelMap":{"high":null}}),
        );
        let current = model(
            "google-generative-ai",
            json!({"thinkingLevelMap":{"high":"HIGH"}}),
        );
        assert!(gemini_thinking_with_model(Some(&old), &request).is_err());
        assert_eq!(
            gemini_thinking_with_model(Some(&current), &request).unwrap(),
            Some(json!({"thinkingLevel":"HIGH"}))
        );
    }

    #[test]
    fn defaults_and_negative_effort_facts_do_not_invent_reasoning_support() {
        for facts in [
            NativeReasoning::from([("mandatory".into(), Some(true))]),
            NativeReasoning::from([("default_effort.high".into(), Some(true))]),
            NativeReasoning::from([("effort.supported".into(), Some(false))]),
            NativeReasoning::from([("effort.high".into(), None)]),
        ] {
            assert!(!effort_supported_with_native(
                "openai",
                "unlisted-research-engine-2049",
                None,
                ReasoningEffort::High,
                &facts,
            ));
        }
    }

    #[test]
    fn native_denials_and_mandatory_are_preserved() {
        let entry = model(
            "anthropic-messages",
            json!({
                "thinkingLevelMap":{"high":"high", "off":"none"},
                "compat":{"forceAdaptiveThinking":true}
            }),
        );
        let facts = NativeReasoning::from([
            ("thinking.types.adaptive".into(), Some(false)),
            ("thinking.supported".into(), Some(false)),
            ("effort.high".into(), Some(true)),
            ("mandatory".into(), Some(true)),
        ]);
        let resolved = reasoning_model("anthropic", Some(&entry), &facts).unwrap();
        let mut request = test_request(&entry.id);
        request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::High));
        let fields = anthropic_fields_with_model("anthropic", &request, Some(&resolved)).unwrap();
        assert_eq!(fields["output_config"], json!({"effort":"high"}));
        assert!(!fields.contains_key("thinking"));
        request.reasoning = Some(ReasoningConfig::Disabled);
        assert!(anthropic_fields_with_model("anthropic", &request, Some(&resolved)).is_err());
        let denied = reasoning_model(
            "anthropic",
            Some(&entry),
            &NativeReasoning::from([("reasoning.supported".into(), Some(false))]),
        )
        .unwrap();
        request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::High));
        assert!(anthropic_fields_with_model("anthropic", &request, Some(&denied)).is_err());
    }
}
