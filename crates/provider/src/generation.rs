//! Last-mile generation settings. Limits remain owned by the updateable catalog;
//! this module supplies protocol rules, never a second model/limit catalog.
//!
//! Precedence: protocol constraints > explicit catalog compat/thinking map >
//! documented family fallback > error for an unverified explicit setting.
//! Omission means server default; Disabled and Effort(None) are explicit off.
//! Sources for family fallbacks:
//! https://developers.openai.com/api/docs/guides/reasoning
//! https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/reasoning
//! https://api-docs.deepseek.com/guides/thinking_mode/
//! https://docs.z.ai/guides/capabilities/thinking-mode
//! https://docs.z.ai/guides/llm/glm-5.2
//! https://docs.z.ai/guides/llm/glm-5.3
//! https://developers.openai.com/api/docs/models/gpt-5.1
//! https://developers.openai.com/api/docs/guides/latest-model
//! https://ai.google.dev/gemini-api/docs/generate-content/thinking
//! https://docs.aws.amazon.com/bedrock/latest/userguide/claude-messages-adaptive-thinking.html
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
    let limit = catalog.and_then(|c| c.limits(provider, &request.model).max_output);
    Ok(limit.map_or(product_default, |limit| {
        u64::from(product_default).min(limit) as u32
    }))
}

pub(crate) fn validate_cap_with_catalog(
    catalog: Option<&ModelCatalog>,
    provider: &str,
    request: &ChatRequest,
) -> Result<()> {
    if let Some(cap) = request.max_tokens {
        ensure!(cap > 0, "generation cap must be positive");
        let limit = catalog.and_then(|c| c.limits(provider, &request.model).max_output);
        ensure!(
            limit.is_none_or(|limit| u64::from(cap) <= limit),
            "generation cap exceeds the current catalog limit for {provider}/{}",
            request.model
        );
    }
    Ok(())
}

pub(crate) fn chat_fields(provider: &str, request: &ChatRequest) -> Result<Fields> {
    chat_fields_from_catalog(model_catalog().ok().as_deref(), provider, request)
}

// Models facts stay in the existing authority-bound provider instance. Scope
// them only while preparing/sending that provider's request, never globally.
pub(crate) type NativeReasoning = std::collections::BTreeMap<String, Option<bool>>;
tokio::task_local! {
    static NATIVE_REASONING: (String, bool, std::collections::BTreeMap<String, NativeReasoning>);
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

/// Native facts override matching catalog keys; documented fallback remains in
/// the protocol mapper. Missing/null native facts cannot erase catalog facts.
pub(crate) fn reasoning_model(
    provider: &str,
    entry: Option<&CatalogModel>,
    native: &NativeReasoning,
) -> Option<CatalogModel> {
    let mut model = entry
        .filter(|m| match provider {
            "anthropic" => m.api == "anthropic-messages",
            "gemini" => m.api == "google-generative-ai",
            "bedrock" => matches!(
                m.api.as_str(),
                "bedrock-converse" | "bedrock-converse-stream"
            ),
            _ => true,
        })
        .cloned()?;
    let id = model
        .metadata
        .get("sourceGeneration")
        .and_then(|s| s["resolvedModelId"].as_str())
        .unwrap_or(&model.id);
    // The old snapshot wrongly inherited Gemini 3 Pro's medium veto. This
    // correction applies only to 3.1 Pro generateContent, before native facts.
    if model.api == "google-generative-ai" && id.starts_with("gemini-3.1-pro") {
        if let Some(map) = model
            .metadata
            .get_mut("thinkingLevelMap")
            .and_then(Value::as_object_mut)
        {
            if map.get("medium") == Some(&Value::Null) {
                map.insert("medium".into(), json!("MEDIUM"));
            }
        }
    }
    let effort = native.get("effort.supported").copied().flatten();
    let thinking = native.get("thinking.supported").copied().flatten();
    let positive_level = native.iter().any(|(k, v)| {
        k.strip_prefix("effort.")
            .is_some_and(|e| ReasoningEffort::from_str(e).is_some())
            && *v == Some(true)
    });
    if effort == Some(true) || effort != Some(false) && positive_level || thinking == Some(true) {
        model.reasoning = true;
    } else if provider == "gemini" && thinking == Some(false) {
        model.reasoning = false;
    }
    if let Some(supported) = effort.or((effort != Some(false) && positive_level).then_some(true)) {
        let compat = model
            .metadata
            .entry("compat".into())
            .or_insert_with(|| json!({}));
        if !compat.is_object() {
            *compat = json!({});
        }
        compat["supportsReasoningEffort"] = json!(supported);
    }
    for (key, supported) in native {
        let Some(level) = key
            .strip_prefix("effort.")
            .filter(|e| ReasoningEffort::from_str(e).is_some())
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
        let key = if provider == "openrouter" && level == "none" {
            "off"
        } else {
            level
        };
        map[key] = if *supported {
            json!(level)
        } else {
            Value::Null
        };
    }
    if provider == "openrouter" {
        if let Some(supported) = native.get("reasoning.supported").copied().flatten() {
            model.reasoning = supported;
        }
    }
    Some(model)
}
fn validate_native_reasoning(
    provider: &str,
    request: &ChatRequest,
    fields: &Fields,
    native: &NativeReasoning,
) -> Result<()> {
    let denies = |key: &str| native.get(key) == Some(&Some(false));
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
    if let Some(effort) = fields
        .get("output_config")
        .and_then(|v| v["effort"].as_str())
    {
        ensure!(
            !denies("effort.supported") && !denies(&format!("effort.{effort}")),
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
    // Native OpenRouter metadata shares the existing authority scope. Other
    // compatible adapters keep their existing catalog/protocol resolution.
    let native = native_reasoning(provider, &request.model);
    let native_model = (provider == "openrouter")
        .then(|| {
            reasoning_model(
                provider,
                catalog
                    .filter(|_| public_reasoning_catalog(provider))
                    .and_then(|c| c.model(provider, &request.model)),
                &native,
            )
        })
        .flatten();
    let model = if provider == "openrouter" {
        native_model.as_ref()
    } else {
        catalog.and_then(|c| c.model(provider, &request.model))
    };
    validate_native_reasoning(provider, request, &Fields::new(), &native)?;
    let fields = chat_fields_with_model(provider, request, model)?;
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

fn openai_reasoner(id: &str) -> bool {
    ([
        "gpt-5",
        "gpt-5.1",
        "gpt-5.2",
        "gpt-5.3",
        "gpt-5.4",
        "gpt-5.5",
        "gpt-5.6",
        "gpt-6-astra",
        "gpt-6-sol",
        "gpt-6-luna",
        "gpt-6.1-sol",
    ]
    .iter()
    .any(|family| id == *family || id.starts_with(&format!("{family}-")))
        && !id.contains("chat"))
        || ["o1", "o3", "o4"]
            .iter()
            .any(|p| id == *p || id.starts_with(&format!("{p}-")))
}

fn openai_fields(
    provider: &str,
    request: &ChatRequest,
    model: Option<&CatalogModel>,
) -> Result<Fields> {
    validate_temperature(model, request)?;
    let id = request.model.as_str();
    let reasoner = openai_reasoner(id) || model.is_some_and(|m| m.reasoning);
    // A source's preferred Responses profile alone does not prove that an
    // unfamiliar model/deployment accepts Chat Completions generation fields.
    let known = model.is_some_and(|m| m.api == "openai-completions")
        || openai_reasoner(id)
        || id.starts_with("gpt-4")
        || id.starts_with("gpt-3.5");
    ensure!(
        known
            || request.max_tokens.is_none()
                && request.temperature.is_none()
                && request.reasoning.is_none(),
        "cannot resolve generation controls for {provider} model/deployment `{id}`; use a known base model ID (deployment routing is configured separately)"
    );
    ensure!(
        !id.contains("codex") && !id.ends_with("-pro") && !id.contains("-pro-"),
        "model `{id}` requires a Responses adapter; this provider uses Chat Completions"
    );
    let tools = request.tools.as_ref().is_some_and(|t| !t.is_empty())
        || request
            .messages
            .iter()
            .any(|m| m.tool_calls.as_ref().is_some_and(|t| !t.is_empty()));
    let mandatory = id.starts_with("gpt-6-astra") || id.starts_with("gpt-6.1-sol");
    ensure!(
        !(mandatory && tools),
        "model `{id}` requires Responses for function calling"
    );
    ensure!(
        !((id.starts_with("gpt-5.6")
            || id.starts_with("gpt-6-sol")
            || id.starts_with("gpt-6-luna"))
            && tools
            && !selected_off(request.reasoning)),
        "model `{id}` requires Responses to combine tools and reasoning; explicitly select none only if desired"
    );
    let mut fields = Fields::new();
    let compat = model.and_then(|m| m.metadata.get("compat"));
    let cap_field = if reasoner {
        "max_completion_tokens"
    } else {
        compat
            .and_then(|c| c["maxTokensField"].as_str())
            .unwrap_or("max_tokens")
    };
    ensure!(
        matches!(cap_field, "max_tokens" | "max_completion_tokens"),
        "unsupported Chat cap field {cap_field}"
    );
    if let Some(cap) = request.max_tokens {
        fields.insert(cap_field.into(), json!(cap));
    }
    if let Some(reasoning) = request.reasoning {
        if !reasoner {
            ensure!(
                selected_off(request.reasoning),
                "model `{id}` does not support reasoning"
            );
        } else {
            ensure!(
                model
                    .and_then(|m| m.metadata.get("compat"))
                    .is_none_or(|c| c["supportsReasoningEffort"] != false)
                    || selected_off(request.reasoning),
                "selected OpenAI profile explicitly does not support reasoning effort"
            );
            let mapped = mapped_effort(model, reasoning)?;
            let effort = mapped.unwrap_or_else(|| match reasoning {
                ReasoningConfig::Disabled => "none".into(),
                ReasoningConfig::Effort(e) => e.as_str().into(),
            });
            // Validate the mapped value against the actual Chat/model/platform
            // contract; dynamic maps cannot widen a protocol enum.
            ensure!(
                !mandatory || !selected_off(request.reasoning),
                "model `{id}` has mandatory reasoning"
            );
            ensure!(
                openai_reasoner(id),
                "unknown OpenAI family has no verified effort contract"
            );
            ensure!(
                !selected_off(request.reasoning) || effort == "none",
                "off mapping must disable reasoning, not select effort `{effort}`"
            );
            let azure = matches!(provider, "azure-openai" | "azure_openai");
            ensure!(
                !(azure && id.starts_with("o1-mini")),
                "Azure o1-mini does not support reasoning_effort"
            );
            let allowed = if id.starts_with("gpt-5.1") {
                matches!(effort.as_str(), "none" | "low" | "medium" | "high")
            } else if id.starts_with("gpt-5.2")
                || id.starts_with("gpt-5.3")
                || id.starts_with("gpt-5.4")
                || id.starts_with("gpt-5.5")
            {
                matches!(
                    effort.as_str(),
                    "none" | "low" | "medium" | "high" | "xhigh"
                )
            } else if id.starts_with("gpt-5.6") || id.starts_with("gpt-6") {
                matches!(
                    effort.as_str(),
                    "none" | "low" | "medium" | "high" | "xhigh" | "max"
                )
            } else if id == "gpt-5" || id.starts_with("gpt-5-") {
                matches!(effort.as_str(), "minimal" | "low" | "medium" | "high")
            } else {
                matches!(effort.as_str(), "low" | "medium" | "high")
            };
            ensure!(
                allowed && !(mandatory && matches!(effort.as_str(), "none" | "minimal")),
                "model `{id}` does not support reasoning effort `{effort}`"
            );
            ensure!(
                !(azure && effort == "max"),
                "Azure max effort requires Responses; this adapter uses Chat Completions"
            );
            ensure!(
                !(azure
                    && effort == "xhigh"
                    && !(id.starts_with("gpt-6")
                        || id.starts_with("gpt-5.6")
                        || id.starts_with("gpt-5.5")
                        || id.starts_with("gpt-5.4"))),
                "Azure Chat model `{id}` does not support xhigh effort"
            );
            fields.insert("reasoning_effort".into(), json!(effort));
        }
    }
    if let Some(temperature) = request.temperature {
        let none = fields.get("reasoning_effort").is_some_and(|e| e == "none");
        let supports = compat.is_none_or(|c| c["supportsTemperature"] != false)
            && (!reasoner
                || ((id.starts_with("gpt-5.1")
                    || id.starts_with("gpt-5.2")
                    || id.starts_with("gpt-5.4")
                    || id.starts_with("gpt-5.5")
                    || id.starts_with("gpt-5.6")
                    || id.starts_with("gpt-6-sol")
                    || id.starts_with("gpt-6-luna"))
                    && none));
        ensure!(
            supports,
            "temperature is unsupported for the selected reasoning mode of `{id}`"
        );
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
    if provider == "deepseek"
        && (request.model.starts_with("deepseek-v4")
            || matches!(
                request.model.as_str(),
                "deepseek-flash" | "deepseek-pro" | "deepseek-reasoner"
            ))
        && !selected_off(request.reasoning)
    {
        ensure!(
            request.temperature.is_none(),
            "DeepSeek thinking mode (including server default) does not support temperature"
        );
    }
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
        !(native_glm_profile(provider)
            && off
            && request.model.to_ascii_lowercase().starts_with("glm-5.3")),
        "GLM-5.3 has mandatory thinking"
    );
    if model.is_some_and(|m| !m.reasoning) {
        ensure!(off, "selected catalog model does not support reasoning");
        return Ok(fields);
    }
    ensure!(
        off || compat.is_none_or(|c| c["supportsReasoningEffort"] != false),
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
    let effort = mapped.clone().unwrap_or_else(|| match reasoning {
        ReasoningConfig::Disabled => "none".into(),
        ReasoningConfig::Effort(e) => e.as_str().into(),
    });
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
        "deepseek" => {
            let id = request.model.as_str();
            ensure!(
                provider == "deepseek"
                    || !off
                    || compat.is_some_and(|c| c["supportsThinkingToggle"] == true),
                "hosted DeepSeek profile has no verified thinking off control"
            );
            ensure!(
                provider == "deepseek" || mapped.is_some(),
                "hosted DeepSeek profile has no verified mapping for the selected control"
            );
            ensure!(
                model.is_some()
                    || id.starts_with("deepseek-v4")
                    || matches!(
                        id,
                        "deepseek-chat" | "deepseek-reasoner" | "deepseek-flash" | "deepseek-pro"
                    ),
                "unknown DeepSeek thinking contract"
            );
            ensure!(
                !matches!(id, "deepseek-chat" | "deepseek-reasoner") || mapped.is_some(),
                "legacy DeepSeek alias has no verified thinking control; select a model with documented controls"
            );
            fields.insert(
                "thinking".into(),
                json!({"type":if off {"disabled"} else {"enabled"}}),
            );
            if !off {
                ensure!(
                    matches!(effort.as_str(), "low" | "high" | "max"),
                    "DeepSeek effort `{effort}` is unsupported by this model map"
                );
                fields.insert("reasoning_effort".into(), json!(effort));
                ensure!(
                    request.temperature.is_none(),
                    "DeepSeek thinking mode does not support temperature"
                );
            }
        }
        "zai" => {
            let id = request.model.to_ascii_lowercase();
            ensure!(
                native_glm_profile(provider) || mapped.is_some(),
                "hosted GLM profile has no verified mapping for the selected control"
            );
            let toggle_family = ["glm-4.5", "glm-4.6", "glm-4.7", "glm-5", "glm-5.2"]
                .iter()
                .any(|family| id == *family || id.starts_with(&format!("{family}-")));
            ensure!(
                model.is_some() || toggle_family || id == "glm-5.3" || id.starts_with("glm-5.3-"),
                "unknown GLM thinking contract"
            );
            ensure!(
                !(off && id.starts_with("glm-5.3")),
                "GLM-5.3 has mandatory thinking"
            );
            ensure!(
                !off || toggle_family
                    || mapped.is_some()
                    || model
                        .and_then(|m| m.metadata.get("sourceGeneration"))
                        .and_then(|s| s["reasoningOptions"].as_array())
                        .is_some_and(|options| options.iter().any(|o| o["type"] == "toggle")),
                "unknown GLM family has no documented thinking off control"
            );
            fields.insert(
                "thinking".into(),
                json!({"type":if off {"disabled"} else {"enabled"}}),
            );
            if !off {
                // An effort slider cannot mean merely enable on older toggle-only models.
                ensure!(
                    id.starts_with("glm-5.2")
                        || id.starts_with("glm-5.3")
                        || compat.is_some_and(|c| c["supportsReasoningEffort"] == true),
                    "this GLM model supports a thinking toggle, not qualitative effort"
                );
                ensure!(
                    matches!(effort.as_str(), "high" | "max")
                        || id.starts_with("glm-5.3") && effort == "low",
                    "unsupported GLM effort `{effort}`"
                );
                fields.insert("reasoning_effort".into(), json!(effort));
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
                compat.is_none_or(|c| c["supportsReasoningEffort"] != false) && mapped.is_some(),
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
    let model = catalog.and_then(|c| c.model("gemini", &request.model));
    let native = native_reasoning("gemini", &request.model);
    let model = reasoning_model(
        "gemini",
        model.filter(|_| public_reasoning_catalog("gemini")),
        &native,
    );
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
    let id = request.model.as_str();
    let family_id = model
        .as_ref()
        .and_then(|m| m.metadata.get("sourceGeneration"))
        .and_then(|s| s["resolvedModelId"].as_str())
        .unwrap_or(id);
    let off = selected_off(request.reasoning);
    if family_id.starts_with("gemini-2.5-") {
        ensure!(
            off,
            "Gemini 2.5 generateContent uses thinkingBudget; qualitative effort has no product budget mapping"
        );
        ensure!(
            matches!(family_id, "gemini-2.5-flash" | "gemini-2.5-flash-lite"),
            "Gemini 2.5 thinking cannot be disabled for this model (Pro is mandatory; unknown variants require their own contract)"
        );
        return Ok(Some(json!({"thinkingBudget":0})));
    }
    ensure!(
        !off,
        "Gemini thinking cannot be disabled for this family; omission means the server default, not off"
    );
    ensure!(
        model
            .and_then(|m| m.metadata.get("compat"))
            .is_none_or(|c| c["supportsReasoningEffort"] != false),
        "selected Gemini profile explicitly does not support reasoning effort"
    );
    let mapped = mapped_effort(model, reasoning)?;
    let ReasoningConfig::Effort(effort) = reasoning else {
        unreachable!()
    };
    let level = mapped.unwrap_or_else(|| effort.as_str().into());
    let level = level.to_ascii_uppercase();
    ensure!(
        matches!(level.as_str(), "MINIMAL" | "LOW" | "MEDIUM" | "HIGH"),
        "invalid native generateContent ThinkingLevel `{level}`"
    );
    let allowed = crate::reasoning_registry::reasoning_capabilities_for_model("gemini", family_id)
        .is_some_and(|r| {
            r.effort_options
                .iter()
                .any(|e| e.eq_ignore_ascii_case(&level))
        });
    ensure!(
        allowed,
        "Gemini model `{id}` has no documented support for thinking level `{level}`"
    );
    Ok(Some(json!({"thinkingLevel":level.to_ascii_uppercase()})))
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

// Only documented Bedrock model/inference-profile forms are normalized. ARNs
// and application profile aliases do not establish an underlying Claude family.
pub(crate) fn claude_id<'a>(provider: &str, id: &'a str) -> Option<&'a str> {
    if provider != "bedrock" {
        return id.starts_with("claude-").then_some(id);
    }
    let id = ["us.", "eu.", "jp.", "au.", "global."]
        .iter()
        .find_map(|prefix| id.strip_prefix(prefix))
        .unwrap_or(id);
    let id = id.strip_prefix("anthropic.")?;
    let known = [
        "claude-opus-4-7",
        "claude-opus-4-8",
        "claude-opus-4-6-v1",
        "claude-sonnet-4-6",
        "claude-opus-4-5",
        "claude-opus-4-5-20251101-v1:0",
        "claude-fable-5",
        "claude-fable-5-1",
        "claude-mythos-5",
        "claude-mythos-5-1",
        "claude-mythos-preview",
        "claude-opus-5",
        "claude-opus-5-5",
        "claude-sonnet-5",
        "claude-sonnet-5-5",
    ];
    // Older dated platform IDs may disable manual thinking, but have no
    // qualitative fallback. Catalog API identity supplies their off contract.
    let legacy_dated = id
        .strip_suffix("-v1:0")
        .and_then(|id| id.rsplit_once('-'))
        .is_some_and(|(family, date)| {
            date.len() == 8
                && date.bytes().all(|b| b.is_ascii_digit())
                && [
                    "claude-3-7-sonnet",
                    "claude-sonnet-4",
                    "claude-sonnet-4-5",
                    "claude-opus-4",
                    "claude-opus-4-1",
                    "claude-haiku-4-5",
                ]
                .contains(&family)
        });
    (known.contains(&id) || legacy_dated).then_some(id)
}

pub(crate) fn claude_family(id: &str, family: &str) -> bool {
    if id == family {
        return true;
    }
    let Some(suffix) = id
        .strip_prefix(family)
        .and_then(|suffix| suffix.strip_prefix('-'))
    else {
        return false;
    };
    // A version component is a different family, not a dated release of this
    // one. Avoid interpreting an unknown future 5.x model as documented 5.0.
    if matches!(suffix, "v1" | "v1:0") {
        return true;
    }
    let (date, rest) = suffix.split_once('-').unwrap_or((suffix, ""));
    date.len() == 8
        && date.bytes().all(|b| b.is_ascii_digit())
        && matches!(rest, "" | "v1" | "v1:0")
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
    let model = reasoning_model(
        provider,
        catalog
            .filter(|_| public_reasoning_catalog(provider))
            .and_then(|c| c.model(provider, &request.model)),
        &native,
    );
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
    let normalized_id = claude_id(provider, &request.model).map(|id| id.replace('.', "-"));
    let id = normalized_id.as_deref();
    let mandatory = id.is_some_and(|id| {
        [
            "claude-opus-5-5",
            "claude-fable-5",
            "claude-mythos-5",
            "claude-fable-5-1",
            "claude-mythos-5-1",
            "claude-mythos-preview",
            "claude-sonnet-5-5",
        ]
        .iter()
        .any(|family| claude_family(id, family))
    });
    ensure!(
        !mandatory || !selected_off(request.reasoning),
        "Claude family has mandatory thinking; off cannot be honored"
    );
    if let Some(id) = id {
        let sampling_restricted = [
            "claude-opus-4-7",
            "claude-opus-4-8",
            "claude-opus-5",
            "claude-opus-5-5",
            "claude-sonnet-5",
            "claude-sonnet-5-5",
            "claude-fable-5",
            "claude-fable-5-1",
            "claude-mythos-5",
            "claude-mythos-5-1",
            "claude-mythos-preview",
        ]
        .iter()
        .any(|family| claude_family(id, family));
        ensure!(
            !sampling_restricted || request.temperature.is_none(),
            "Claude family does not support sampling temperature, including default/off thinking"
        );
        // Legacy thinking and adaptive thinking also prohibit selected sampling.
        let default_on = mandatory;
        let thinking_selected = request.reasoning.is_some()
            && !selected_off(request.reasoning)
            && !claude_family(id, "claude-opus-4-5");
        ensure!(
            request.temperature.is_none() || !default_on && !thinking_selected,
            "Claude thinking does not support a sampling temperature setting"
        );
    }
    let mut fields = Fields::new();
    let Some(reasoning) = request.reasoning else {
        return Ok(fields);
    };
    let id = id.ok_or_else(|| {
        anyhow::anyhow!(
            "underlying Claude family cannot be resolved for selected reasoning controls"
        )
    })?;
    let off = selected_off(request.reasoning);
    ensure!(
        !off || model.is_none_or(|m| m.reasoning),
        "catalog reasoning capability conflicts with explicit Claude off"
    );
    ensure!(
        off || model.is_none_or(|m| m.reasoning),
        "catalog model does not support effort controls"
    );
    let mapped = mapped_effort(model, reasoning)?;
    let registry =
        crate::reasoning_registry::reasoning_capabilities_for_model(provider, &request.model);
    let adaptive = [
        "claude-opus-4-6",
        "claude-opus-4-7",
        "claude-opus-4-8",
        "claude-sonnet-4-6",
        "claude-opus-5",
        "claude-opus-5-5",
        "claude-sonnet-5",
        "claude-sonnet-5-5",
        "claude-fable-5",
        "claude-fable-5-1",
        "claude-mythos-5",
        "claude-mythos-5-1",
        "claude-mythos-preview",
    ]
    .iter()
    .any(|family| claude_family(id, family));
    if off {
        ensure!(
            registry.is_some() || adaptive || model.is_some(),
            "unknown Claude off contract"
        );
        fields.insert("thinking".into(), json!({"type":"disabled"}));
        return Ok(fields);
    }
    ensure!(
        compat.is_none_or(|c| c["supportsReasoningEffort"] != false),
        "profile explicitly does not support reasoning effort"
    );
    let ReasoningConfig::Effort(effort) = reasoning else {
        unreachable!()
    };
    let wire = mapped.unwrap_or_else(|| effort.as_str().into());
    ensure!(
        matches!(wire.as_str(), "low" | "medium" | "high" | "xhigh" | "max"),
        "invalid Claude effort enum `{wire}`"
    );
    // The subset belongs to the actual platform, not just the Claude brand.
    let known_effort = registry.is_some_and(|r| r.effort_options.contains(&wire));
    ensure!(
        known_effort,
        "Claude model does not document selected qualitative effort (legacy thinking needs numeric budget)"
    );
    if provider == "bedrock" {
        ensure!(
            !claude_family(id, "claude-opus-4-8"),
            "Bedrock Opus 4.8 adaptive profile remains unverified"
        );
        if claude_family(id, "claude-opus-4-5") {
            ensure!(
                matches!(wire.as_str(), "low" | "medium" | "high"),
                "Bedrock Opus 4.5 supports low/medium/high"
            );
            fields.insert("anthropic_beta".into(), json!(["effort-2025-11-24"]));
        }
    }
    fields.insert("output_config".into(), json!({"effort":wire}));
    if adaptive {
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
            || request.model.starts_with("deepseek-v4")
            || matches!(request.model.as_str(), "deepseek-flash" | "deepseek-pro"),
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
        _ => chat_fields_with_model(provider, &request, model),
    };
    fields
        .is_ok_and(|fields| validate_native_reasoning(provider, &request, &fields, native).is_ok())
}

pub(crate) fn protocol_mandatory(provider: &str, id: &str) -> bool {
    match provider {
        "openai" | "azure-openai" | "azure_openai" => {
            id.starts_with("gpt-6-astra") || id.starts_with("gpt-6.1-sol")
        }
        "glm" | "zai" | "glm-coding" | "zai-coding" => {
            id.to_ascii_lowercase().starts_with("glm-5.3")
        }
        "anthropic" | "bedrock" => claude_id(provider, id).is_some_and(|id| {
            let id = id.replace('.', "-");
            [
                "claude-opus-5-5",
                "claude-fable-5",
                "claude-fable-5-1",
                "claude-mythos-5",
                "claude-mythos-5-1",
                "claude-mythos-preview",
                "claude-sonnet-5-5",
            ]
            .iter()
            .any(|family| claude_family(&id, family))
        }),
        "gemini" => crate::reasoning_registry::reasoning_capabilities_for_model(provider, id)
            .is_some_and(|r| r.mandatory == Some(true)),
        _ => false,
    }
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
