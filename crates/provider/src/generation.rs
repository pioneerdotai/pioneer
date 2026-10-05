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

pub(crate) fn chat_fields_from_catalog(
    catalog: Option<&ModelCatalog>,
    provider: &str,
    request: &ChatRequest,
) -> Result<Fields> {
    validate_cap_with_catalog(catalog, provider, request)?;
    chat_fields_with_model(
        provider,
        request,
        catalog.and_then(|c| c.model(provider, &request.model)),
    )
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
    // https://docs.siliconflow.cn/cn/api-reference/chat-completions/chat-completions
    // max_tokens excludes thoughts. NativeRequestProjection reserves one total
    // generation cap and has no independently selected thinking budget. Until
    // that product control exists, never promise the reserve bounds thinking.
    if provider == "siliconflow" && request.max_tokens.is_some() {
        ensure!(
            selected_off(request.reasoning) || model.is_some_and(|m| !m.reasoning),
            "SiliconFlow max_tokens bounds visible output only; thinking with a prepared total reserve requires a separately budgeted thinking cap (unsupported)"
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
    validate_temperature(model, request)?;
    gemini_thinking_with_model(model, request)
}

fn gemini_thinking_with_model(
    model: Option<&CatalogModel>,
    request: &ChatRequest,
) -> Result<Option<Value>> {
    let Some(reasoning) = request.reasoning else {
        return Ok(None);
    };
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
    let allowed = if family_id.starts_with("gemini-3.1-flash-lite-image") {
        matches!(level.as_str(), "MINIMAL" | "HIGH")
    } else if family_id.starts_with("gemini-3-pro") {
        matches!(level.as_str(), "LOW" | "HIGH")
    } else if family_id.starts_with("gemini-3.1-pro")
        || family_id.starts_with("gemini-3.7-flash")
        || family_id.starts_with("gemini-3.8-flash")
    {
        matches!(level.as_str(), "LOW" | "MEDIUM" | "HIGH")
    } else {
        [
            "gemini-3-flash",
            "gemini-3.1-flash-lite",
            "gemini-3.5-flash",
            "gemini-3.6-flash",
        ]
        .iter()
        .any(|family| family_id == *family || family_id.starts_with(&format!("{family}-")))
    };
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
fn claude_id<'a>(provider: &str, id: &'a str) -> Option<&'a str> {
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
        "claude-sonnet-5",
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

fn claude_family(id: &str, family: &str) -> bool {
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
    anthropic_fields_with_model(
        provider,
        request,
        catalog.and_then(|c| c.model(provider, &request.model)),
    )
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
    if off && model.is_some_and(|m| !m.reasoning) {
        return Ok(fields);
    }
    let mapped = mapped_effort(model, reasoning)?;
    let normalized = id
        .strip_suffix("-v1:0")
        .or_else(|| id.strip_suffix("-v1"))
        .unwrap_or(id);
    let registry_id = [
        "claude-opus-4-5",
        "claude-opus-4-6",
        "claude-opus-4-7",
        "claude-opus-4-8",
        "claude-sonnet-4-6",
    ]
    .into_iter()
    .find(|family| claude_family(normalized, family))
    .unwrap_or(normalized);
    let registry =
        crate::reasoning_registry::reasoning_capabilities_for_model("anthropic", registry_id);
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
    // Registry supplies documented direct-model vocabulary; a map can remap a
    // user effort but cannot widen that vocabulary or enable manual-only models.
    let modern = [
        "claude-opus-5",
        "claude-opus-5-5",
        "claude-sonnet-5",
        "claude-sonnet-5-5",
        "claude-fable-5",
        "claude-fable-5-1",
        "claude-mythos-5",
        "claude-mythos-5-1",
    ]
    .iter()
    .any(|family| claude_family(id, family));
    // AWS Converse documents xhigh for the exact Opus 4.6-v1 platform
    // profile, whereas direct Messages does not. claude_id has already bounded
    // exact/regional identity; a map cannot grant this to another model.
    // https://docs.aws.amazon.com/bedrock/latest/userguide/claude-messages-adaptive-thinking.html
    let aws_opus_46 = provider == "bedrock" && id == "claude-opus-4-6-v1";
    let known_effort = registry.is_some_and(|r| r.effort_options.contains(&wire))
        || modern
        || aws_opus_46 && wire == "xhigh";
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
pub(crate) fn effort_supported(
    provider: &str,
    model: &CatalogModel,
    effort: ReasoningEffort,
) -> bool {
    effort_supported_for_profile(provider, &model.id, Some(model), effort)
}

pub(crate) fn effort_supported_for_profile(
    provider: &str,
    id: &str,
    model: Option<&CatalogModel>,
    effort: ReasoningEffort,
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
    match provider {
        "anthropic" | "bedrock" => anthropic_fields_with_model(provider, &request, model).is_ok(),
        "gemini" => gemini_thinking_with_model(model, &request).is_ok(),
        _ => chat_fields_with_model(provider, &request, model).is_ok(),
    }
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
    let mut model = models[provider][template].clone();
    model["id"] = json!(id);
    for (key, value) in metadata.as_object().unwrap() {
        model[key] = value.clone();
    }
    models[provider][id] = model;
    origins[provider][id] = origins[provider][template].clone();
    ModelCatalog::parse(&models.to_string(), &origins.to_string()).unwrap()
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
