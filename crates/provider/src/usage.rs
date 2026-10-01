//! Usage is response evidence, never a token estimate or a response payload.
//! API-specific category rules are independent of model pricing.
use crate::types::TokenUsage;
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};

pub(crate) type ChatUsage = WireUsage<0>;
pub(crate) type AnthropicUsage = WireUsage<1>;
pub(crate) type BedrockUsage = WireUsage<2>;
pub(crate) type GeminiUsage = WireUsage<3>;

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
                                | "cached_tokens"
                                | "cache_write_tokens"
                                | "cache_creation_input_tokens"
                                | "cache_read_input_tokens"
                                | "cache_creation"
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
            _ => (
                n("prompt_tokens"),
                n("completion_tokens"),
                raw["prompt_tokens_details"]["cached_tokens"]
                    .as_u64()
                    .or_else(|| n("prompt_cache_hit_tokens"))
                    .or_else(|| n("cached_prompt_tokens"))
                    .or_else(|| n("num_cached_tokens")),
                raw["prompt_tokens_details"]["cache_write_tokens"].as_u64(),
                raw["completion_tokens_details"]["reasoning_tokens"].as_u64(),
                "inclusive_cache_and_reasoning",
            ),
        };
        let total_input = if matches!(API, 1 | 2) {
            input.and_then(|i| {
                i.checked_add(read.unwrap_or(0))?
                    .checked_add(write.unwrap_or(0))
            })
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
    let entry = catalog.and_then(|c| c.model(provider, model));
    let mut pricing = entry.map(|e| {
        json!({"catalog_snapshot":catalog.map(|c|c.snapshot_id()),
        "provider":e.provider,"model":e.id,"catalog_api":e.api,
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
    let estimate = if oversized {
        None
    } else {
        entry.and_then(|e| estimate(usage, &e.cost, &e.metadata))
    };
    json!({"reported_cost":reported_cost,"estimated_cost":estimate,
        "pricing":pricing,"price_status":if entry.is_none(){"unknown_tariff"}else if estimate.is_none(){"unknown_or_conditional_price"}else{"catalog_estimate"}})
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

/// Explicit catalog compatibility takes precedence for matching Chat profiles.
/// Unknown/private profiles receive no speculative OpenAI extension. Sources:
/// Groq API reference; Fireworks/Venice/Friendli chat reference;
/// HF inference-providers chat task; DeepSeek create-chat-completion.
pub(crate) fn stream_options(provider: &str, model: &str) -> Option<Value> {
    let override_value = crate::catalog::model_catalog().ok().and_then(|c| {
        c.model(provider, model)
            .filter(|m| m.api == "openai-completions")
            .and_then(|m| m.metadata.get("compat"))
            .and_then(|v| v.get("supportsUsageInStreaming"))
            .and_then(Value::as_bool)
    });
    let documented = matches!(
        provider,
        "groq" | "fireworks" | "venice" | "huggingface" | "friendli" | "deepseek" | "novita"
    );
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
            catalog: crate::catalog::model_catalog().ok(),
        }
    }
    pub(crate) fn enrich(&self, usage: &mut TokenUsage) {
        usage.provider = Some(self.provider.clone());
        usage.model = Some(self.model.clone());
        usage.api = Some(self.api.clone());
        usage.api_version = self.api_version.clone();
        usage.route = self.route.clone();
        usage.physical_attempt_id = Some(self.attempt.clone());
        usage.accounting = Some(accounting(
            usage,
            &self.provider,
            &self.model,
            self.catalog.as_deref(),
        ));
    }
}

/// Native Claude only, default retention. Profile and catalog model must agree;
/// custom proxies and other vendors are not assumed to accept Claude markers.
/// https://platform.claude.com/docs/en/build-with-claude/prompt-caching
pub(crate) fn anthropic_cache_control(base: &str, model: &str) -> Option<Value> {
    if url::Url::parse(base).ok()?.host_str() != Some("api.anthropic.com")
        || !model.starts_with("claude-")
    {
        return None;
    }
    let catalog = crate::catalog::model_catalog().ok()?;
    anthropic_cache_control_in_catalog(base, model, &catalog)
}

fn anthropic_cache_control_in_catalog(
    base: &str,
    model: &str,
    catalog: &crate::catalog::ModelCatalog,
) -> Option<Value> {
    if url::Url::parse(base).ok()?.host_str() != Some("api.anthropic.com")
        || !model.starts_with("claude-")
    {
        return None;
    }
    let entry = catalog.model("anthropic", model)?;
    if entry
        .metadata
        .get("pricingEvidenceVersion")
        .and_then(Value::as_u64)
        != Some(1)
        || entry.api != "anthropic-messages"
        || !entry.cost["cacheRead"]
            .as_f64()
            .is_some_and(|rate| rate.is_finite() && rate > 0.)
        || entry
            .metadata
            .get("compat")
            .and_then(|v| v.get("supportsPromptCaching"))
            .and_then(Value::as_bool)
            == Some(false)
    {
        return None;
    }
    Some(json!({"type":"ephemeral"}))
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
