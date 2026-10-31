//! Usage is response evidence, never a token estimate or a response payload.
//! API-specific category rules are independent of model pricing.
use crate::types::TokenUsage;
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};

pub(crate) type ChatUsage = WireUsage<0>;
pub(crate) type AnthropicUsage = WireUsage<1>;
pub(crate) type BedrockUsage = WireUsage<2>;
pub(crate) type GeminiUsage = WireUsage<3>;
pub(crate) fn normalize_responses(value: &Value) -> anyhow::Result<TokenUsage> {
    anyhow::ensure!(value.is_object(), "invalid Responses usage");
    Ok(WireUsage::<4>(bounded_usage(value)).normalized())
}

#[derive(Debug)]
pub(crate) struct WireUsage<const API: u8>(Value);
impl<'de, const API: u8> Deserialize<'de> for WireUsage<API> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        if !value.is_object() {
            return Err(serde::de::Error::custom("usage must be an object"));
        }
        Ok(Self(bounded_usage(&value)))
    }
}

/// Explicit metric names only. Arbitrary strings, keys, IDs and payloads are
/// discarded even if a compatible server places them inside `usage`.
/// At most 128 nodes, depth 4 and 16 array entries are retained.
pub fn bounded_usage(value: &Value) -> Value {
    fn visit(value: &Value, depth: usize, budget: &mut usize, key: &str) -> Option<Value> {
        if depth > 4 || *budget == 0 {
            return None;
        }
        *budget -= 1;
        match value {
            Value::Number(_) | Value::Null | Value::Bool(_) => Some(value.clone()),
            Value::String(s)
                if matches!(
                    key,
                    "ttl"
                        | "currency"
                        | "service_tier"
                        | "serviceTier"
                        | "modality"
                        | "inference_geo"
                ) && matches!(
                    s.as_str(),
                    "us" | "global"
                        | "5m"
                        | "1h"
                        | "USD"
                        | "credits"
                        | "standard"
                        | "unspecified"
                        | "default"
                        | "auto"
                        | "flex"
                        | "priority"
                        | "TEXT"
                        | "IMAGE"
                        | "AUDIO"
                        | "VIDEO"
                ) =>
            {
                Some(value.clone())
            }
            Value::String(_)
                if matches!(
                    key,
                    "ttl"
                        | "currency"
                        | "service_tier"
                        | "serviceTier"
                        | "modality"
                        | "inference_geo"
                ) =>
            {
                Some(Value::String("unknown".into()))
            }
            Value::Object(values) => Some(Value::Object(
                values
                    .iter()
                    .filter_map(|(k, v)| {
                        let allowed = matches!(
                            k.as_str(),
                            "prompt_tokens"
                                | "completion_tokens"
                                | "total_tokens"
                                | "input_tokens"
                                | "output_tokens"
                                | "prompt_tokens_details"
                                | "completion_tokens_details"
                                | "input_tokens_details"
                                | "output_tokens_details"
                                | "cached_tokens"
                                | "cache_write_tokens"
                                | "cache_creation_input_tokens"
                                | "cache_creation_tokens"
                                | "created_cache_tokens"
                                | "cache_read_input_tokens"
                                | "cache_creation"
                                | "claude_cache_tokens_details"
                                | "cache_write_5_minutes_input_tokens"
                                | "cache_write_1_hour_input_tokens"
                                | "ephemeral_5m_input_tokens"
                                | "ephemeral_1h_input_tokens"
                                | "reasoning_tokens"
                                | "audio_tokens"
                                | "image_tokens"
                                | "video_tokens"
                                | "text_tokens"
                                | "accepted_prediction_tokens"
                                | "rejected_prediction_tokens"
                                | "prompt_cache_hit_tokens"
                                | "prompt_cache_miss_tokens"
                                | "cached_prompt_tokens"
                                | "num_cached_tokens"
                                | "inputTokens"
                                | "outputTokens"
                                | "totalTokens"
                                | "cacheReadInputTokens"
                                | "cacheWriteInputTokens"
                                | "cacheDetails"
                                | "ttl"
                                | "promptTokenCount"
                                | "candidatesTokenCount"
                                | "thoughtsTokenCount"
                                | "cachedContentTokenCount"
                                | "totalTokenCount"
                                | "toolUsePromptTokenCount"
                                | "promptTokensDetails"
                                | "cacheTokensDetails"
                                | "candidatesTokensDetails"
                                | "toolUsePromptTokensDetails"
                                | "modality"
                                | "tokenCount"
                                | "cost"
                                | "total_cost"
                                | "input_tokens_cost"
                                | "output_tokens_cost"
                                | "request_cost"
                                | "citation_tokens_cost"
                                | "reasoning_tokens_cost"
                                | "search_queries_cost"
                                | "citation_tokens"
                                | "num_search_queries"
                                | "cost_details"
                                | "upstream_inference_cost"
                                | "currency"
                                | "inference_geo"
                                | "service_tier"
                                | "serviceTier"
                                | "prompt_eval_count"
                                | "prompt_eval_cached_count"
                                | "eval_count"
                                | "server_tool_use"
                                | "web_search_requests"
                                | "web_fetch_requests"
                        );
                        allowed
                            .then(|| visit(v, depth + 1, budget, k).map(|v| (k.clone(), v)))
                            .flatten()
                    })
                    .collect(),
            )),
            Value::Array(values) => Some(Value::Array(
                values
                    .iter()
                    .take(16)
                    .filter_map(|v| visit(v, depth + 1, budget, key))
                    .collect(),
            )),
            _ => None,
        }
    }
    visit(value, 0, &mut 128, "").unwrap_or_else(|| json!({}))
}

impl<const API: u8> WireUsage<API> {
    pub(crate) fn normalized(&self) -> TokenUsage {
        let raw = &self.0;
        let n = |key: &str| raw[key].as_u64();
        let (input, output, read, write, reasoning, semantics) = match API {
            // Anthropic input is uncached; cache categories are exclusive.
            1 => (
                n("input_tokens"),
                n("output_tokens"),
                n("cache_read_input_tokens"),
                n("cache_creation_input_tokens"),
                None,
                "exclusive_cache",
            ),
            // AWS Converse cache guide, not generic TokenUsage prose:
            // https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html
            2 => (
                n("inputTokens"),
                n("outputTokens"),
                n("cacheReadInputTokens"),
                n("cacheWriteInputTokens"),
                None,
                "exclusive_cache",
            ),
            3 => (
                n("promptTokenCount"),
                n("candidatesTokenCount"),
                n("cachedContentTokenCount"),
                None,
                n("thoughtsTokenCount"),
                "inclusive_cache_separate_thoughts",
            ),
            4 => (
                n("input_tokens"),
                n("output_tokens"),
                raw["input_tokens_details"]["cached_tokens"].as_u64(),
                raw["input_tokens_details"]["cache_write_tokens"].as_u64(),
                raw["output_tokens_details"]["reasoning_tokens"].as_u64(),
                "inclusive_cache_and_reasoning",
            ),
            _ => (
                n("prompt_tokens"),
                n("completion_tokens"),
                raw["prompt_tokens_details"]["cached_tokens"]
                    .as_u64()
                    .or_else(|| n("cached_tokens"))
                    .or_else(|| n("prompt_cache_hit_tokens"))
                    .or_else(|| n("cached_prompt_tokens"))
                    .or_else(|| n("num_cached_tokens")),
                raw["prompt_tokens_details"]["cache_write_tokens"].as_u64(),
                raw["completion_tokens_details"]["reasoning_tokens"].as_u64(),
                "inclusive_cache_and_reasoning",
            ),
        };
        let total_input = if matches!(API, 1 | 2) {
            input.and_then(|i| i.checked_add(read?)?.checked_add(write?))
        } else {
            input
        };
        let total_output = if API == 3 {
            match (output, reasoning) {
                (None, None) => None,
                (o, r) => o.unwrap_or(0).checked_add(r.unwrap_or(0)),
            }
        } else {
            output
        };
        TokenUsage {
            input_tokens: total_input,
            output_tokens: total_output,
            uncached_input_tokens: if matches!(API, 1 | 2) {
                input
            } else {
                input.and_then(|i| i.checked_sub(read?)?.checked_sub(write?))
            },
            cache_read_input_tokens: read,
            cache_write_input_tokens: write,
            reasoning_tokens: reasoning,
            reported_total_tokens: n("total_tokens")
                .or_else(|| n("totalTokens"))
                .or_else(|| n("totalTokenCount")),
            service_tier: raw["service_tier"]
                .as_str()
                .or_else(|| raw["serviceTier"].as_str())
                .map(str::to_owned),
            semantics: Some(semantics.into()),
            raw_usage: Some(raw.clone()),
            ..Default::default()
        }
    }
}

/// Apply envelope semantics after cumulative SSE fields have been merged by
/// the existing registry. Provider identity and the actual endpoint are captured
/// before dispatch, rather than inferred from model names or usage payloads.
fn apply_chat_usage_policy(usage: &mut TokenUsage, provider: &str, actual_route: Option<&str>) {
    if !matches!(provider, "aihubmix" | "venice") {
        return;
    }
    let expected = crate::definition::provider_definition(provider)
        .and_then(|d| d.default_base_url)
        .and_then(|base| route(base, "/chat/completions"));
    if expected.is_none() || expected.as_deref() != actual_route {
        return;
    }
    let Some(raw) = usage.raw_usage.as_ref() else {
        return;
    };
    if provider == "aihubmix" {
        let Some(details) = raw
            .get("claude_cache_tokens_details")
            .filter(|v| v.is_object())
        else {
            return;
        };
        // The documented Chat example has prompt_tokens=22 and cache read=6266.
        // It does not explicitly define Chat prompt_tokens as exclusive input.
        // Keep observed native categories, but do not invent a complete total
        // by either treating 22 as inclusive or summing unproven categories.
        // https://docs.aihubmix.com/cn/blogs/aihubmix-openai-upgrade-claude
        usage.input_tokens = None;
        usage.uncached_input_tokens = None;
        usage.cache_read_input_tokens = details["cache_read_input_tokens"]
            .as_u64()
            .or_else(|| raw["prompt_tokens_details"]["cached_tokens"].as_u64())
            .or(usage.cache_read_input_tokens);
        usage.cache_write_input_tokens = details["cache_creation_input_tokens"]
            .as_u64()
            .or(usage.cache_write_input_tokens);
        usage.semantics = Some("aihubmix_claude_input_unresolved".into());
    } else {
        // Venice's paid-write alias is nested and part of inclusive input.
        // Canonical field wins even when explicitly zero. Never add aliases.
        // https://docs.venice.ai/guides/features/prompt-caching
        usage.cache_write_input_tokens = raw["prompt_tokens_details"]["cache_write_tokens"]
            .as_u64()
            .or_else(|| raw["prompt_tokens_details"]["cache_creation_input_tokens"].as_u64())
            .or(usage.cache_write_input_tokens);
        usage.uncached_input_tokens = usage.input_tokens.and_then(|input| {
            input
                .checked_sub(usage.cache_read_input_tokens?)?
                .checked_sub(usage.cache_write_input_tokens?)
        });
    }
}

/// Only the documented response/header field supplied by an adapter is used.
/// No search through arbitrary JSON or headers for possible identifiers.
pub fn native_id(id: Option<&str>) -> Option<String> {
    id.filter(|s| {
        !s.is_empty()
            && s.len() <= 256
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
    })
    .map(str::to_owned)
}

impl TokenUsage {
    pub(crate) fn with_native_id(mut self, id: Option<&str>) -> Self {
        self.generation_id = native_id(id);
        self
    }
    pub(crate) fn with_request_id(mut self, id: Option<&str>) -> Self {
        self.request_id = native_id(id);
        self
    }
}

#[cfg(test)]
mod tests;

/// Keep reported cost and a conservative catalog estimate separate. A catalog
/// is captured before dispatch; later catalog refreshes cannot rewrite history.
pub fn accounting(
    usage: &TokenUsage,
    provider: &str,
    model: &str,
    catalog: Option<&crate::catalog::ModelCatalog>,
) -> Value {
    accounting_with_entry(usage, provider, model, catalog, None)
}

pub(crate) fn accounting_with_entry(
    usage: &TokenUsage,
    provider: &str,
    model: &str,
    catalog: Option<&crate::catalog::ModelCatalog>,
    native: Option<&crate::catalog::CatalogModel>,
) -> Value {
    let raw = usage.raw_usage.as_ref();
    let reported = raw
        .and_then(|r| {
            r["cost"].as_f64().or_else(|| {
                (provider == "perplexity")
                    .then(|| r["cost"]["total_cost"].as_f64())
                    .flatten()
            })
        })
        .filter(|v| v.is_finite() && *v >= 0.);
    let reported_cost = reported.map(|amount|json!({
        "amount":amount,
        "currency":if provider == "openrouter" {Some("credits")} else {raw.and_then(|r|r["currency"].as_str())},
        "provenance":"provider_response_usage.cost",
        "details":raw.map(|r|r["cost_details"].clone()),
    }));
    let requested_entry = native
        .filter(|entry| entry.id == model)
        .or_else(|| catalog.and_then(|c| c.model(provider, model)));
    let dynamic_router =
        provider == "openrouter" && matches!(model, "openrouter/auto" | "openrouter/auto-beta");
    let billing_model = match usage.reported_model.as_deref() {
        Some(reported) if reported != model => {
            // Auto Router documents response.model as the selected billed model.
            // Other aliases/fallbacks require a separate evidenced mapping.
            if dynamic_router { Some(reported) } else { None }
        }
        _ if dynamic_router => None,
        _ => Some(model),
    };
    let entry = billing_model.and_then(|id| {
        native
            .filter(|entry| entry.id == id)
            .or_else(|| catalog.and_then(|c| c.model(provider, id)))
    });
    let eligibility = if dynamic_router && billing_model.is_none() {
        Err("missing_selected_billing_model")
    } else {
        pricing_eligibility(usage, provider, billing_model, entry)
    };
    // A requested-model snapshot is reference evidence only when billing identity
    // is unresolved. It must never be presented as an actual-attempt estimate.
    let snapshot_entry = entry.or(requested_entry);
    let mut pricing = snapshot_entry.map(|e| {
        json!({"catalog_snapshot":catalog.map(|c|c.snapshot_id()),
        "provider":e.provider,"model":e.id,"catalog_api":e.api,
        "basis":if eligibility.is_ok(){"observed_billing_identity"}else{"reference_tariff_only"},
        "cost":e.cost,"source":e.metadata.get("pricingSource"),
        "captured_at":e.metadata.get("pricingCapturedAt"),
        "units":e.metadata.get("pricingUnits"),
        "version":e.metadata.get("pricingEvidenceVersion")})
    });
    let oversized = pricing
        .as_ref()
        .is_some_and(|p| p.to_string().len() > 8 * 1024);
    if oversized {
        pricing = Some(
            json!({"catalog_snapshot":catalog.map(|c|c.snapshot_id()),"metadata_omitted":"exceeds_8192_bytes"}),
        );
    }
    let estimate = if oversized || eligibility.is_err() {
        None
    } else {
        entry.and_then(|e| estimate(usage, &e.cost, &e.metadata))
    };
    json!({"reported_cost":reported_cost,"estimated_cost":estimate,
        "catalog_snapshot":catalog.map(|c|c.snapshot_id()),
        "pricing":pricing,"pricing_eligibility":{
            "eligible":eligibility.is_ok(),"reason":eligibility.err(),
            "requested_model":model,"reported_model":usage.reported_model,
            "billing_model":if eligibility.is_ok(){billing_model}else{None},
            "source":if provider == "openrouter" && matches!(model, "openrouter/auto" | "openrouter/auto-beta") && billing_model != Some(model) {
                "https://openrouter.ai/docs/guides/routing/routers/auto-router"
            } else if provider == "openai" && entry.is_some_and(|e| e.api == "openai-responses") {
                "https://developers.openai.com/api/docs/pricing"
            } else {"captured_catalog_api_and_endpoint"}},
        "price_status":if snapshot_entry.is_none(){"unknown_tariff"}else if estimate.is_none(){"unknown_or_conditional_price"}else{"catalog_estimate"}})
}

fn pricing_eligibility(
    usage: &TokenUsage,
    provider: &str,
    billing_model: Option<&str>,
    entry: Option<&crate::catalog::CatalogModel>,
) -> Result<(), &'static str> {
    if billing_model.is_none() {
        return Err("unconfirmed_reported_model_mapping");
    }
    if matches!(
        provider,
        "custom" | "local" | "azure-openai" | "azure_openai" | "copilot" | "glm"
    ) {
        return Err("deployment_private_or_subscription_billing_contract");
    }
    let entry = entry.ok_or("unknown_billing_tariff")?;
    // OpenAI documents shared model token tariffs for Chat and Responses:
    // https://developers.openai.com/api/docs/pricing (APIs not priced separately).
    // This narrow API equivalence does not confer endpoint/model equivalence.
    let matching_api = (provider == "vercel-ai-gateway"
        && matches!(
            (usage.api.as_deref(), entry.api.as_str()),
            (Some("responses"), "openai-responses") | (Some("embeddings"), "openai-embeddings")
        ))
        || (provider == "openai"
            && ((usage.api.as_deref() == Some("chat_completions")
                && entry.api == "openai-responses")
                || usage.api.as_deref() == Some("responses")
                    && matches!(
                        entry.api.as_str(),
                        "openai-responses" | "openai-completions"
                    )))
        || matches!(
            (usage.api.as_deref(), entry.api.as_str()),
            (Some("chat_completions"), "openai-completions")
                | (Some("messages"), "anthropic-messages")
                | (Some("generate_content"), "google-generative-ai")
                | (Some("bedrock_converse"), "bedrock-converse-stream")
        );
    if !matching_api {
        return Err("unconfirmed_api_pricing_contract");
    }
    let actual = usage.route.as_deref().ok_or("missing_billing_route")?;
    let template = actual
        .split_once(";path=")
        .ok_or("unconfirmed_billing_route")?
        .1;
    // Compare the existing opaque identity to the captured public catalog
    // endpoint, including protocol base path. No host/path/query is exported.
    if route(&entry.base_url, template).as_deref() != Some(actual) {
        return Err("route_does_not_match_catalog_tariff");
    }
    Ok(())
}

/// Explicit adapter evidence on failures, without raw headers/payloads/errors.
#[derive(Clone)]
pub struct ObservedProviderUsage {
    usage: TokenUsage,
    // Identify the original top-level cause without copying its text/payload.
    // Anyhow contexts prepend causes; additional metadata/stage contexts may
    // therefore be skipped for classification only. Never format the full chain
    // as a persisted diagnostic to recover classification hints.
    classification_chain_len: usize,
}
impl std::fmt::Debug for ObservedProviderUsage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ObservedProviderUsage")
    }
}
impl std::fmt::Display for ObservedProviderUsage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("provider usage evidence available")
    }
}
impl std::error::Error for ObservedProviderUsage {}
pub fn error_usage(error: &anyhow::Error) -> Option<&TokenUsage> {
    error
        .downcast_ref::<ObservedProviderUsage>()
        .map(|e| &e.usage)
}
pub(crate) fn with_error_usage(error: anyhow::Error, usage: &TokenUsage) -> anyhow::Error {
    let classification_chain_len = error
        .downcast_ref::<ObservedProviderUsage>()
        .map(|e| e.classification_chain_len)
        .unwrap_or_else(|| error.chain().count());
    error.context(ObservedProviderUsage {
        usage: usage.clone(),
        classification_chain_len,
    })
}

/// Original cause for private fallback classification, transparent to usage
/// enrichment and subsequent context. This is not a diagnostic/telemetry API:
/// callers must keep the safe outer Display for persisted error messages.
/// Typed adapter errors remain reachable through anyhow downcast_ref.
pub fn classification_source(error: &anyhow::Error) -> &(dyn std::error::Error + 'static) {
    let skip = error
        .downcast_ref::<ObservedProviderUsage>()
        .map(|e| {
            error
                .chain()
                .count()
                .saturating_sub(e.classification_chain_len)
        })
        .unwrap_or(0);
    error.chain().nth(skip).unwrap_or_else(|| error.as_ref())
}

fn estimate(
    usage: &TokenUsage,
    cost: &Value,
    metadata: &std::collections::BTreeMap<String, Value>,
) -> Option<Value> {
    // Do not apply the standard text/default-retention tariff to a reported
    // modality or TTL whose pricing contract is not represented here.
    fn conditional_usage(value: &Value, key: &str) -> bool {
        match value {
            Value::Object(map) => map.iter().any(|(k, v)| conditional_usage(v, k)),
            Value::Array(values) => values.iter().any(|v| conditional_usage(v, key)),
            Value::Number(n)
                if matches!(
                    key,
                    "ephemeral_1h_input_tokens"
                        | "audio_tokens"
                        | "image_tokens"
                        | "video_tokens"
                        | "web_search_requests"
                        | "web_fetch_requests"
                ) =>
            {
                n.as_f64().is_some_and(|n| n > 0.)
            }
            Value::String(s) if key == "ttl" => s != "5m",
            Value::Number(n) if key == "ttl" => n.as_u64() != Some(300),
            Value::String(s) if key == "modality" => s != "TEXT",
            Value::String(s) if key == "inference_geo" => s != "global",
            _ => false,
        }
    }
    if usage
        .raw_usage
        .as_ref()
        .is_some_and(|r| conditional_usage(r, ""))
    {
        return None;
    }
    if usage
        .service_tier
        .as_deref()
        .is_some_and(|tier| !matches!(tier, "default" | "standard" | "STANDARD"))
    {
        return None;
    }
    if metadata.get("pricingEvidenceVersion")?.as_u64() != Some(1)
        || metadata.get("pricingUnits")?.as_str() != Some("USD_per_million_tokens")
    {
        return None;
    }
    // TTL, modality, non-token fees and opaque tier conditions need a separate
    // pricing contract. Preserve them, never flatten them into a zero charge.
    let source = metadata.get("pricingSource");
    if source.is_some_and(|s| s.get("unknownRateContract").is_some_and(|v| !v.is_null())) {
        return None;
    }
    if source
        .and_then(|s| s.get("raw"))
        .and_then(Value::as_object)
        .is_some_and(|r| {
            r.keys().any(|k| {
                !matches!(
                    k.as_str(),
                    "input"
                        | "output"
                        | "cache_read"
                        | "cache_write"
                        | "prompt"
                        | "completion"
                        | "input_cache_read"
                        | "input_cache_write"
                ) && !r[k].is_null()
                    && r[k].as_f64() != Some(0.)
                    && r[k].as_str() != Some("0")
            })
        })
    {
        return None;
    }
    if cost.as_object()?.keys().any(|k| {
        !matches!(
            k.as_str(),
            "input"
                | "output"
                | "cacheRead"
                | "cacheWrite"
                | "cache_read"
                | "cache_write"
                | "tiers"
        )
    }) {
        return None;
    }
    let input = usage.input_tokens?;
    let output = usage.output_tokens?;
    let read = usage.cache_read_input_tokens?;
    let write = usage.cache_write_input_tokens?;
    let uncached = input.checked_sub(read)?.checked_sub(write)?;
    let mut selected = cost.clone();
    let mut threshold = None;
    for tier in cost["tiers"].as_array().into_iter().flatten() {
        let above = tier["inputTokensAbove"].as_u64()?;
        if tier.as_object()?.keys().any(|k| {
            !matches!(
                k.as_str(),
                "inputTokensAbove" | "input" | "output" | "cacheRead" | "cacheWrite"
            )
        }) {
            return None;
        }
        if input > above && threshold.is_none_or(|t| above > t) {
            selected = tier.clone();
            threshold = Some(above);
        }
    }
    let priced = |count: u64, key: &str| -> Option<f64> {
        if count == 0 {
            return Some(0.);
        }
        let rate = selected[key]
            .as_f64()
            .filter(|v| v.is_finite() && *v >= 0.)?;
        Some(count as f64 * rate / 1_000_000.)
    };
    let amount = priced(uncached, "input")?
        + priced(read, "cacheRead")?
        + priced(write, "cacheWrite")?
        + priced(output, "output")?;
    amount.is_finite().then(|| {
        json!({"amount":amount,"currency":"USD","provenance":"catalog_estimate",
        "units":"USD_per_million_tokens","input_tokens_above":threshold})
    })
}

/// Authority-native facts precede permitted public-catalog facts per field.
/// Unknown/private profiles receive no speculative OpenAI extension. Sources:
/// Groq API reference; Fireworks/Venice/Friendli chat reference;
/// HF inference-providers chat task; DeepSeek create-chat-completion.
pub(crate) fn stream_options(provider: &str, model: &str, base: &str) -> Option<Value> {
    let catalog = crate::generation::uses_public_catalog(provider)
        .then(|| crate::catalog::model_catalog().ok())
        .flatten();
    stream_options_in_catalog(provider, model, base, catalog.as_deref())
}

fn chat_root(base: &str) -> &str {
    let base = base.trim_end_matches('/');
    base.strip_suffix("/chat/completions")
        .unwrap_or(base)
        .trim_end_matches('/')
}

fn compat_bool(entry: &crate::catalog::CatalogModel, field: &str) -> Option<bool> {
    entry.metadata.get("compat")?.get(field)?.as_bool()
}

fn stream_options_in_catalog(
    provider: &str,
    model: &str,
    base: &str,
    catalog: Option<&crate::catalog::ModelCatalog>,
) -> Option<Value> {
    let native = crate::generation::native_model(provider, model);
    let source = crate::generation::uses_public_catalog(provider)
        .then(|| catalog.and_then(|c| c.model(provider, model)))
        .flatten();
    let api = native
        .as_ref()
        .map(|m| m.api.as_str())
        .filter(|api| !api.is_empty())
        .or_else(|| source.map(|m| m.api.as_str()));
    if api.is_some_and(|api| api != "openai-completions") {
        return None;
    }
    let override_value = native
        .as_ref()
        .and_then(|m| compat_bool(m, "supportsUsageInStreaming"))
        .or_else(|| {
            source.and_then(|m| {
                let supported = compat_bool(m, "supportsUsageInStreaming")?;
                // Negative evidence remains authoritative. A positive public fact
                // cannot admit a different endpoint; full Chat URLs are equivalent.
                (!supported || chat_root(&m.base_url) == chat_root(base)).then_some(supported)
            })
        });
    let existing_default = matches!(
        provider,
        "groq" | "fireworks" | "venice" | "huggingface" | "friendli" | "deepseek" | "novita"
    );
    // Explicit server profiles select these protocols even at operator URLs.
    // Custom endpoints do not inherit that admission. Hosted additions stay
    // on the exact documented route; Nebius Studio is not Token Factory.
    let documented = existing_default
        || matches!(provider, "sglang" | "vllm" | "litellm")
        || (matches!(provider, "baseten" | "nscale")
            && crate::definition::provider_definition(provider)
                .and_then(|d| d.default_base_url)
                .is_some_and(|stock| chat_root(stock) == chat_root(base)));
    override_value
        .unwrap_or(documented)
        .then(|| json!({"include_usage":true}))
}

pub(crate) fn route(base: &str, template: &str) -> Option<String> {
    use sha2::{Digest, Sha256};
    let parsed = url::Url::parse(base).ok()?;
    let host = parsed.host_str()?;
    // Workspace endpoints may themselves contain private authority or path
    // names. Preserve a reproducible route identity without publishing them.
    // Query/userinfo are excluded; API version has its own typed field.
    let identity = format!(
        "{}://{}{}{}",
        parsed.scheme(),
        host,
        parsed.port().map(|p| format!(":{p}")).unwrap_or_default(),
        parsed.path()
    );
    let digest = Sha256::digest(identity.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Some(format!(
        "{}:endpoint_sha256:{digest};path={template}",
        parsed.scheme()
    ))
}

pub(crate) struct UsageContext {
    provider: String,
    model: String,
    api: String,
    api_version: Option<String>,
    route: Option<String>,
    attempt: String,
    catalog: Option<std::sync::Arc<crate::catalog::ModelCatalog>>,
    native: Option<crate::catalog::CatalogModel>,
}
impl UsageContext {
    pub(crate) fn capture(provider: &dyn crate::Provider, model: &str) -> Self {
        Self {
            provider: provider.name().into(),
            model: model.into(),
            api: provider.usage_api().into(),
            api_version: provider.usage_api_version(),
            route: provider.usage_route(),
            attempt: pioneer_protocol::generate_id(21),
            catalog: crate::catalog::model_catalog()
                .ok()
                .filter(|_| provider.name() != "vercel-ai-gateway"),
            native: provider.model_catalog_entry(model),
        }
    }
    pub(crate) fn capture_request(
        provider: &dyn crate::Provider,
        request: &crate::ChatRequest,
    ) -> anyhow::Result<Self> {
        let profile = provider.request_usage_profile(request)?;
        let mut context = Self::capture(provider, &request.model);
        context.api = profile.api;
        context.api_version = profile.api_version;
        context.route = profile.route;
        Ok(context)
    }
    pub(crate) fn enrich_error(&self, error: anyhow::Error) -> anyhow::Error {
        if let Some(mut usage) = error_usage(&error).cloned() {
            self.enrich(&mut usage);
            with_error_usage(error, &usage)
        } else {
            error
        }
    }
    pub(crate) fn enrich(&self, usage: &mut TokenUsage) {
        usage.provider = Some(self.provider.clone());
        usage.model = Some(self.model.clone());
        usage.api = Some(self.api.clone());
        usage.api_version = self.api_version.clone();
        usage.route = self.route.clone();
        usage.physical_attempt_id = Some(self.attempt.clone());
        if self.api == "chat_completions" {
            apply_chat_usage_policy(usage, &self.provider, self.route.as_deref());
        }
        usage.accounting = Some(accounting_with_entry(
            usage,
            &self.provider,
            &self.model,
            self.catalog.as_deref(),
            self.native.as_ref(),
        ));
    }
}

/// Native Claude only, default retention. Available facts must agree with Messages;
/// custom proxies and other vendors are not assumed to accept Claude markers.
/// https://platform.claude.com/docs/en/build-with-claude/prompt-caching
pub(crate) fn anthropic_cache_control(base: &str, model: &str) -> Option<Value> {
    let catalog = crate::generation::uses_public_catalog("anthropic")
        .then(|| crate::catalog::model_catalog().ok())
        .flatten();
    anthropic_cache_control_in_catalog(base, model, catalog.as_deref())
}

fn anthropic_cache_control_in_catalog(
    base: &str,
    model: &str,
    catalog: Option<&crate::catalog::ModelCatalog>,
) -> Option<Value> {
    if base.trim_end_matches('/') != crate::providers::anthropic::BASE_URL {
        return None;
    }
    let native = crate::generation::native_model("anthropic", model);
    let source = crate::generation::uses_public_catalog("anthropic")
        .then(|| catalog.and_then(|c| c.model("anthropic", model)))
        .flatten();
    let api = native
        .as_ref()
        .map(|m| m.api.as_str())
        .filter(|api| !api.is_empty())
        .or_else(|| source.map(|m| m.api.as_str()));
    let supported = native
        .as_ref()
        .and_then(|m| compat_bool(m, "supportsPromptCaching"))
        .or_else(|| source.and_then(|m| compat_bool(m, "supportsPromptCaching")));
    // The native API documents caching on all active Claude models. Pricing
    // evidence is a separate accounting contract, never capability evidence.
    if api.is_some_and(|api| api != "anthropic-messages") || supported == Some(false) {
        return None;
    }
    Some(json!({"type":"ephemeral"}))
}

/// OpenRouter documents automatic Claude caching on its Chat route. The
/// existing catalog supplies the format fact; neither a model name nor a
/// cache-read tariff proves that an arbitrary endpoint accepts this field.
pub(crate) fn openrouter_cache_control(base: &str, model: &str) -> Option<Value> {
    let catalog = crate::catalog::model_catalog().ok()?;
    openrouter_cache_control_in_catalog(base, model, &catalog)
}

fn openrouter_cache_control_in_catalog(
    base: &str,
    model: &str,
    catalog: &crate::catalog::ModelCatalog,
) -> Option<Value> {
    if base.trim_end_matches('/') != crate::providers::openrouter::BASE_URL {
        return None;
    }
    let entry = catalog.model("openrouter", model)?;
    let compat = entry.metadata.get("compat")?;
    (entry.api == "openai-completions"
        && compat.get("cacheControlFormat").and_then(Value::as_str) == Some("anthropic")
        && compat.get("supportsPromptCaching").and_then(Value::as_bool) != Some(false))
    .then(|| json!({"type":"ephemeral"}))
}

impl TokenUsage {
    pub(crate) fn with_reported_model(mut self, model: Option<&str>) -> Self {
        self.reported_model = model
            .filter(|s| {
                !s.is_empty()
                    && s.len() <= 256
                    && s.bytes().all(|b| {
                        b.is_ascii_alphanumeric()
                            || matches!(b, b'/' | b'-' | b'_' | b'.' | b':' | b'@')
                    })
            })
            .map(str::to_owned);
        self
    }
}

impl TokenUsage {
    pub(crate) fn with_service_tier(mut self, tier: Option<&str>) -> Self {
        self.service_tier = tier.map(|s| {
            if matches!(
                s,
                "default" | "flex" | "priority" | "standard" | "scale" | "auto"
            ) {
                s.to_owned()
            } else {
                "unknown".into()
            }
        });
        self
    }
}
