//! Usage is response evidence, never a token estimate or a response payload.
//! API-specific category rules are independent of model pricing.
use crate::types::TokenUsage;
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
mod manifest;
mod tariff;
pub(crate) mod transport;
pub use manifest::{embedding_request_manifest, request_manifest};
pub use tariff::add_amounts as add_decimal_amounts;

pub(crate) type ChatUsage = WireUsage<0>;
pub(crate) type AnthropicUsage = WireUsage<1>;
pub(crate) type BedrockUsage = WireUsage<2>;
pub(crate) type GeminiUsage = WireUsage<3>;
pub(crate) fn normalize_responses(value: &Value) -> anyhow::Result<TokenUsage> {
    anyhow::ensure!(value.is_object(), "invalid Responses usage");
    Ok(WireUsage::<4>(bounded_usage(value)).normalized())
}

/// The same API-specific counters can accompany an HTTP failure. Decode only
/// the allowlisted usage object, never retain the error text/body/headers.
pub(crate) fn http_error_usage(
    body: &str,
    api: &str,
    request_id: Option<&str>,
) -> Option<TokenUsage> {
    let value: Value = serde_json::from_str(body).ok()?;
    let raw = if api == "generate_content" {
        value.get("usageMetadata")
    } else {
        value.get("usage")
    }?;
    if !raw.is_object() {
        return None;
    }
    let raw = bounded_usage(raw);
    let mut usage = match api {
        "messages" => WireUsage::<1>(raw).normalized(),
        "bedrock_converse" => WireUsage::<2>(raw).normalized(),
        "generate_content" => WireUsage::<3>(raw).normalized(),
        "responses" => WireUsage::<4>(raw).normalized(),
        "chat_completions" | "embeddings" => WireUsage::<0>(raw).normalized(),
        _ => return None,
    }
    .with_reported_model(value["model"].as_str())
    .with_service_tier(value["service_tier"].as_str());
    if api == "embeddings" {
        usage.output_tokens = Some(0);
        usage.reasoning_tokens = None;
    }
    usage.request_id = native_id(request_id);
    Some(usage)
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
                        | "speed"
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
                        | "fast"
                        | "ultrafast"
                        | "scale"
                        | "reserved"
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
                        | "speed"
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
                                | "speed"
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

/// Contract identifiers shared by CLI evidence producers and persistence.
pub fn cli_statistics_identifier(value: &Value) -> Option<&str> {
    value.as_str().filter(|s| {
        !s.is_empty()
            && s.len() <= 128
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.:/".contains(&b))
    })
}

/// CLI-only evidence extension. Never called by API wire deserializers.
/// Bounded independently of API metrics: 8 models, 8 responses, 128-byte
/// identifiers and 12 KiB total, leaving room below the 32 KiB journal limit.
/// Arrays are evidence, never additional normalized spend.
pub fn bounded_cli_statistics(value: &Value) -> Value {
    fn ident(value: &Value) -> Option<Value> {
        cli_statistics_identifier(value).map(|s| Value::String(s.into()))
    }
    fn nullable_counters(value: &Value, names: &[&str]) -> Value {
        let mut out = json!({});
        for name in names {
            if let Some(n) = value[*name].as_u64() {
                out[*name] = n.into();
            } else if value.get(*name).is_some_and(Value::is_null) {
                out[*name] = Value::Null;
            }
        }
        out
    }
    fn fields(value: &Value, numbers: &[&str], strings: &[&str]) -> Value {
        let mut out = json!({});
        for key in numbers {
            if let Some(n) = value[*key].as_u64() {
                out[*key] = n.into();
            }
        }
        for key in strings {
            if let Some(s) = ident(&value[*key]) {
                out[*key] = s;
            }
        }
        for key in ["costUSD", "cost_usd", "total_cost_usd"] {
            if let Some(n) = value[key].as_f64().filter(|n| n.is_finite() && *n >= 0.) {
                out[key] = json!(n);
            }
        }
        out
    }
    let mut out = fields(
        value,
        &[
            "duration_ms",
            "duration_api_ms",
            "num_turns",
            "ttft_ms",
            "ttft_stream_ms",
            "time_to_request_ms",
            "time_to_request_from_spawn_ms",
            "input_tokens",
            "output_tokens",
            "cached_input_tokens",
            "cache_write_input_tokens",
            "reasoning_output_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
            "reasoning_tokens",
            "total_tokens",
            "visible_output_tokens",
        ],
        &[
            "runtime",
            "scope",
            "semantics",
            "model_usage_scope",
            "cost_scope",
            "cost_basis",
        ],
    );
    let mut models = serde_json::Map::new();
    if let Some(values) = value["model_usage"].as_object() {
        for (name, model) in values
            .iter()
            .filter(|(name, _)| ident(&json!(name)).is_some())
            .take(8)
        {
            let mut entry = fields(
                model,
                &[
                    "inputTokens",
                    "outputTokens",
                    "thinkingTokens",
                    "cacheReadInputTokens",
                    "cacheCreationInputTokens",
                    "cacheCreationEphemeral5mInputTokens",
                    "cacheCreationEphemeral1hInputTokens",
                    "webSearchRequests",
                    "contextWindow",
                    "maxOutputTokens",
                ],
                &["canonicalModel", "provider"],
            );
            if matches!(
                model["costBasis"].as_str(),
                Some("list" | "managed" | "unknown")
            ) {
                entry["costBasis"] = model["costBasis"].clone();
            }
            models.insert(name.clone(), entry);
        }
        out["model_usage"] = models.into();
        out["model_usage_truncated"] =
            json!(values.len() > out["model_usage"].as_object().unwrap().len());
    }
    // Aggregate evidence covers all responses, independently of retained detail.
    if value["native_tokens"].is_object() {
        let mut native =
            nullable_counters(&value["native_tokens"], &["input", "output", "reasoning"]);
        if value["native_tokens"]["cache"].is_object() {
            native["cache"] =
                nullable_counters(&value["native_tokens"]["cache"], &["read", "write"]);
        }
        out["native_tokens"] = native;
    }
    if value["known_partial_tokens"].is_object() {
        out["known_partial_tokens"] = nullable_counters(
            &value["known_partial_tokens"],
            &["input", "output", "reasoning", "cache_read", "cache_write"],
        );
    }
    let token_fields = ["input", "output", "reasoning", "total", "read", "write"];
    if let Some(responses) = value["responses"].as_array() {
        let retained: Vec<_> = responses
            .iter()
            .take(8)
            .map(|response| {
                let mut entry = fields(
                    response,
                    &["duration_ms"],
                    &["id", "sessionID", "parentID", "modelID", "providerID"],
                );
                entry["model"] = fields(&response["model"], &[], &["id", "providerID", "variant"]);
                entry["time"] = fields(
                    &response["time"],
                    &["created", "streamed", "completed"],
                    &[],
                );
                entry["native_tokens"] = fields(&response["native_tokens"], &token_fields, &[]);
                entry["native_tokens"]["cache"] =
                    fields(&response["native_tokens"]["cache"], &["read", "write"], &[]);
                entry
            })
            .collect();
        out["responses_truncated"] = json!(responses.len() > retained.len());
        out["responses"] = retained.into();
    }
    if value["native_usage"].is_object() {
        let native = &value["native_usage"];
        let numbers = [
            "inputTokens",
            "cachedInputTokens",
            "cacheWriteInputTokens",
            "outputTokens",
            "reasoningOutputTokens",
            "totalTokens",
            "modelContextWindow",
            "input_tokens",
            "output_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
            "thinking_tokens",
            "reasoning_tokens",
        ];
        let mut entry = fields(native, &numbers, &[]);
        for key in ["total", "last"] {
            if native[key].is_object() {
                entry[key] = fields(&native[key], &numbers, &[]);
            }
        }
        for (key, numbers) in [
            (
                "cache_creation",
                &["ephemeral_5m_input_tokens", "ephemeral_1h_input_tokens"][..],
            ),
            (
                "server_tool_use",
                &["web_search_requests", "web_fetch_requests"][..],
            ),
        ] {
            if native[key].is_object() {
                entry[key] = fields(&native[key], numbers, &[]);
            }
        }
        out["native_usage"] = entry;
    }
    for key in ["responses_truncated", "model_usage_truncated"] {
        if value[key] == true {
            out[key] = json!(true);
        }
    }
    // Hard byte budget: discard tail evidence, retain aggregate counters/timing.
    for key in ["responses", "model_usage"] {
        if out.to_string().len() > 12 * 1024 {
            out.as_object_mut().unwrap().remove(key);
            out[&format!("{key}_truncated")] = json!(true);
        }
    }
    out
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
                n("cache_creation_input_tokens").or_else(|| {
                    raw["cache_creation"]["ephemeral_5m_input_tokens"]
                        .as_u64()?
                        .checked_add(raw["cache_creation"]["ephemeral_1h_input_tokens"].as_u64()?)
                }),
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
            // A partial snapshot cannot establish the omitted category as zero.
            output
                .zip(reasoning)
                .and_then(|(visible, thoughts)| visible.checked_add(thoughts))
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
            r.get("cost").filter(|v| v.is_number()).or_else(|| {
                (provider == "perplexity")
                    .then(|| r["cost"].get("total_cost"))
                    .flatten()
            })
        })
        .filter(|value| value.is_number() && !value.to_string().starts_with('-'));
    let reported_cost = reported.map(|amount|json!({
        "amount":amount,"amount_lexical":amount.to_string(),"amount_decimal":tariff::add_amounts("0",&amount.to_string()),
        "unresolved_reason":tariff::add_amounts("0",&amount.to_string()).is_none().then_some("unsupported_reported_decimal_precision_or_range"),
        "currency":if provider == "openrouter" && ["/chat/completions","/embeddings"].into_iter().any(|path|usage.route==route(crate::providers::openrouter::BASE_URL,path)) {Some("credits")} else {raw.and_then(|r|r["currency"].as_str())},
        "provenance":"provider_response_usage.cost",
        "details":raw.map(|r|r["cost_details"].clone()),
    }));
    let requested_entry = native
        .filter(|entry| entry.id == model)
        .or_else(|| catalog.and_then(|c| c.billing_model(provider, model)));
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
            .or_else(|| catalog.and_then(|c| c.billing_model(provider, id)))
    });
    let eligibility = if dynamic_router && billing_model.is_none() {
        Err("missing_selected_billing_model")
    } else {
        pricing_eligibility(usage, provider, billing_model, entry)
    };
    // A requested-model snapshot is reference evidence only when billing identity
    // is unresolved. It must never be presented as an actual-attempt estimate.
    let snapshot_entry = entry.or(requested_entry);
    let pricing = snapshot_entry.map(|e| {
        let retained = catalog.and_then(|c|c.retained_pricing_json(provider,&e.id))
            .and_then(tariff::lossless_evidence);
        let retained_cost = retained.as_ref().and_then(|v|v.get("cost")).filter(|v|!v.is_null()).unwrap_or(&e.cost);
        let mut source = retained.as_ref().and_then(|v|v.get("source")).filter(|v|!v.is_null()).cloned()
            .or_else(||e.metadata.get("pricingSource").cloned());
        if let Some(object) = source.as_mut().and_then(Value::as_object_mut) {
            if object
                .get("url")
                .and_then(Value::as_str)
                .is_some_and(|url| {
                    !matches!(
                        url,
                        "https://models.dev/api.json"
                            | "https://openrouter.ai/api/v1/models"
                            | "https://ai-gateway.vercel.sh/v1/models"
                    )
                })
            {
                object.insert("url".into(), json!("redacted_nonpublic_tariff_source"));
                object.remove("raw");object.insert("raw_coverage".into(),json!("nonpublic_source_not_retained"));
            }
        }
        json!({"catalog_snapshot":catalog.map(|c|c.snapshot_id()),
        "provider":e.provider,"model":e.id,"catalog_api":e.api,
        "basis":if eligibility.is_ok(){"observed_billing_identity"}else{"reference_tariff_only"},
        "cost":retained_cost,"source":source,
        "raw_catalog_pricing_json":source.as_ref().filter(|s|s["url"]!="redacted_nonpublic_tariff_source")
            .and_then(|_|catalog.and_then(|c|c.retained_pricing_json(provider,&e.id))),
        "source_captured_at":e.metadata.get("pricingCapturedAt"),"source_effective_from":e.metadata.get("pricingEffectiveFrom"),
        "normalizer_version":1,"resolver_version":1,
        "captured_at":e.metadata.get("pricingCapturedAt"),
        "units":e.metadata.get("pricingUnits"),
        "version":e.metadata.get("pricingEvidenceVersion")})
    });
    // Durable stores enforce the independent evidence bound before dispatch.
    let estimate = if eligibility.is_err() {
        None
    } else {
        entry.and_then(|e| {
            let mut metadata = e.metadata.clone();
            if let Some(source) = pricing.as_ref().and_then(|v| v.get("source")) {
                metadata.insert("pricingSource".into(), source.clone());
            }
            estimate(
                usage,
                pricing
                    .as_ref()
                    .and_then(|v| v.get("cost"))
                    .unwrap_or(&e.cost),
                &metadata,
            )
        })
    };
    let resolution = if eligibility.is_err() {
        None
    } else {
        entry.and_then(|e| {
            let mut metadata = e.metadata.clone();
            if let Some(source) = pricing.as_ref().and_then(|v| v.get("source")) {
                metadata.insert("pricingSource".into(), source.clone());
            }
            guarded_resolution(
                usage,
                pricing
                    .as_ref()
                    .and_then(|v| v.get("cost"))
                    .unwrap_or(&e.cost),
                &metadata,
            )
        })
    };
    let selected = select_cost_basis(reported_cost.as_ref(), estimate.as_ref());
    json!({"reported_cost":reported_cost,"estimated_cost":estimate,"selected_cost":selected,"cost_resolution":resolution,
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
        "unresolved_reasons":if let Err(reason) = eligibility {json!([reason])}
            else if estimate.is_none() {resolution.as_ref().and_then(|v|v.get("reasons")).cloned().unwrap_or_else(||json!(["unsupported_conditions_or_incomplete_usage"]))} else {json!([])},
        "calculator_version":1,"usage_coverage":usage_coverage(usage),
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
    if provider == "vercel-ai-gateway"
        && entry.base_url != crate::providers::vercel_ai_gateway::BASE_URL
    {
        return Err("nonpublic_tariff_applicability_unconfirmed");
    }

    // OpenAI documents shared model token tariffs for Chat and Responses:
    // https://developers.openai.com/api/docs/pricing (APIs not priced separately).
    // This narrow API equivalence does not confer endpoint/model equivalence.
    let matching_api = (provider == "vercel-ai-gateway"
        && matches!(
            (usage.api.as_deref(), entry.api.as_str()),
            (Some("responses"), "openai-responses") | (Some("embeddings"), "openai-embeddings")
        ))
        || (matches!(provider, "openai" | "openrouter")
            && usage.api.as_deref() == Some("embeddings")
            && entry.api == "openai-embeddings")
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

fn guarded_resolution(
    usage: &TokenUsage,
    cost: &Value,
    metadata: &std::collections::BTreeMap<String, Value>,
) -> Option<Value> {
    guarded_resolution_with_service_rates(usage, cost, metadata, false)
}

/// Same meter calculation; the caller must prove that these captured rates
/// apply to the actual reported service before relaxing the Standard guard.
fn guarded_resolution_with_service_rates(
    usage: &TokenUsage,
    cost: &Value,
    metadata: &std::collections::BTreeMap<String, Value>,
    confirmed_service_rates: bool,
) -> Option<Value> {
    let unresolved = |reason: &str| {
        Some(json!({"version":1,"status":"unresolved","reasons":[reason],"lines":[]}))
    };
    if usage.accounting.as_ref().is_some_and(|a| {
        a["requested_modalities"]
            .as_array()
            .is_some_and(|v| !v.is_empty())
    }) {
        return unresolved("unsupported_application_modality_pricing_contract");
    }
    if let Some(conditions) = usage
        .accounting
        .as_ref()
        .and_then(|a| a.get("adapter_conditions"))
    {
        if conditions["coverage"] == "uninspectable_adapter_body" {
            return unresolved("adapter_pricing_conditions_uninspected");
        }
        let requested = conditions["requested_service_tier"].as_str();
        if requested.is_some_and(|s| !matches!(s, "default" | "standard"))
            && usage.service_tier.is_none()
        {
            return unresolved("effective_service_tier_unconfirmed");
        }
        if conditions["requested_region_present"] == true {
            return unresolved("requested_region_pricing_contract_unconfirmed");
        }
        if conditions["prompt_cache_retention"]
            .as_str()
            .is_some_and(|s| s != "in-memory")
        {
            return unresolved("unsupported_requested_cache_retention_contract");
        }
        if conditions["cache_control_ttls"]
            .as_array()
            .is_some_and(|v| v.iter().any(|s| s == "unrecognized"))
        {
            return unresolved("unsupported_requested_cache_ttl");
        }
    }
    // Do not apply the standard text/default-retention tariff to a reported
    // modality or TTL whose pricing contract is not represented here.
    fn conditional_usage(value: &Value, key: &str) -> bool {
        match value {
            Value::Object(map) => map.iter().any(|(k, v)| conditional_usage(v, k)),
            Value::Array(values) => values.iter().any(|v| conditional_usage(v, key)),
            Value::Number(n) if matches!(key, "audio_tokens" | "image_tokens" | "video_tokens") => {
                n.as_f64().is_some_and(|n| n > 0.)
            }
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
        return unresolved("unsupported_modality_or_region_contract");
    }
    if !confirmed_service_rates
        && (usage
            .raw_usage
            .as_ref()
            .and_then(|raw| raw["speed"].as_str())
            .is_some_and(|speed| speed != "standard")
            || usage
                .service_tier
                .as_deref()
                .is_some_and(|tier| !matches!(tier, "default" | "standard" | "STANDARD")))
    {
        return unresolved("unsupported_effective_service_tier");
    }
    if metadata
        .get("pricingEvidenceVersion")
        .and_then(Value::as_u64)
        != Some(1)
        || metadata.get("pricingUnits").and_then(Value::as_str) != Some("USD_per_million_tokens")
    {
        return unresolved("unsupported_tariff_evidence_version_or_units");
    }
    // TTL, modality, non-token fees and opaque tier conditions need a separate
    // pricing contract. Preserve them, never flatten them into a zero charge.
    let source = metadata.get("pricingSource");
    if source.is_some_and(|s| s.get("unknownRateContract").is_some_and(|v| !v.is_null())) {
        return unresolved("unconfirmed_source_rate_contract");
    }
    let mut normalized = cost.clone();
    let mut unresolved = Vec::new();
    if let Some(raw) = source.and_then(|s| s.get("raw")).and_then(Value::as_object) {
        let router = source.is_some_and(|s| {
            s["url"] == "https://openrouter.ai/api/v1/models" && s["units"] == "USD_per_token"
        });
        let vercel = source.is_some_and(|s| {
            s["url"] == "https://ai-gateway.vercel.sh/v1/models" && s["units"] == "USD_per_token"
        });
        if vercel {
            normalized = match tariff::vercel_rates(&Value::Object(raw.clone())) {
                Ok(rates) => rates,
                Err(reason) => {
                    return Some(
                        json!({"version":1,"status":"unresolved","reasons":[reason],"lines":[]}),
                    );
                }
            };
        }
        if router {
            normalized = match tariff::openrouter_rates(
                &Value::Object(raw.clone()),
                usage.input_tokens,
            ) {
                Ok(rates) => rates,
                Err(reason) => {
                    return Some(
                        json!({"version":1,"status":"unresolved","reasons":[reason],"lines":[]}),
                    );
                }
            };
        }
        for (key, value) in raw {
            if !matches!(
                key.as_str(),
                "input"
                    | "output"
                    | "cache_read"
                    | "cache_write"
                    | "prompt"
                    | "completion"
                    | "input_cache_read"
                    | "input_cache_write"
                    | "tiers"
                    | "context_over_200k"
            ) && !(router && key == "overrides")
                && !(vercel && key == "varies_by_provider" && value == false)
                && !value.is_null()
            {
                if matches!(
                    key.as_str(),
                    "request"
                        | "image"
                        | "audio"
                        | "web_search"
                        | "internal_reasoning"
                        | "input_audio_cache"
                        | "input_audio"
                        | "output_audio"
                ) {
                    let zero = value
                        .as_str()
                        .map(str::to_owned)
                        .or_else(|| value.as_number().map(ToString::to_string))
                        .and_then(|v| tariff::add_amounts("0", &v))
                        .as_deref()
                        == Some("0");
                    if !zero {
                        unresolved
                            .push(json!({"meter":key,"reason":"unsupported_additional_meter"}));
                    }
                } else {
                    // An unknown condition can change every base rate. Such a
                    // base is not a known subtotal, even if it looks numeric.
                    return Some(json!({"version":1,"status":"unresolved","reasons":[{
                        "condition":key,"reason":"unsupported_tariff_condition"}],"lines":[]}));
                }
            }
        }
        if !router {
            for field in ["tiers", "context_over_200k"] {
                if raw
                    .get(field)
                    .is_some_and(|v| !v.is_null() && cost.get(field) != Some(v))
                {
                    return Some(
                        json!({"version":1,"status":"unresolved","reasons":["conflicting_source_tiers"],"lines":[]}),
                    );
                }
            }
        }
    }
    fn long_retention(value: &Value, key: &str) -> bool {
        match value {
            Value::Object(fields) => fields.iter().any(|(key, value)| long_retention(value, key)),
            Value::Array(values) => values.iter().any(|v| long_retention(v, key)),
            Value::Number(n) if key == "ephemeral_1h_input_tokens" => {
                n.as_u64().is_some_and(|n| n > 0)
            }
            Value::String(s) if key == "ttl" => s != "5m",
            Value::Number(n) if key == "ttl" => n.as_u64() != Some(300),
            _ => false,
        }
    }
    if usage
        .raw_usage
        .as_ref()
        .is_some_and(|raw| long_retention(raw, ""))
        || (usage.cache_write_input_tokens != Some(0)
            && usage.accounting.as_ref().is_some_and(|a| {
                a["adapter_conditions"]["cache_control_ttls"]
                    .as_array()
                    .is_some_and(|ttls| ttls.iter().any(|ttl| ttl == "1h"))
            }))
    {
        // TTL changes only the write meter under the captured text contract.
        // Preserve other known components; never borrow a multiplier from
        // another API or silently apply the default cache-write price.
        fn suppress_write_rates(value: &mut Value) {
            match value {
                Value::Object(fields) => {
                    for key in ["cacheWrite", "cache_write"] {
                        fields.remove(key);
                    }
                    for child in fields.values_mut() {
                        suppress_write_rates(child);
                    }
                }
                Value::Array(values) => {
                    for child in values {
                        suppress_write_rates(child);
                    }
                }
                _ => {}
            }
        }
        suppress_write_rates(&mut normalized);
        unresolved
            .push(json!({"meter":"cacheWrite","reason":"unsupported_cache_retention_contract"}));
    }
    fn server_units(value: &Value, key: &str) -> bool {
        match value {
            Value::Object(fields) => fields.iter().any(|(k, v)| server_units(v, k)),
            Value::Array(values) => values.iter().any(|v| server_units(v, key)),
            Value::Number(n)
                if matches!(
                    key,
                    "web_search_requests"
                        | "web_fetch_requests"
                        | "num_search_queries"
                        | "toolUsePromptTokenCount"
                ) =>
            {
                n.as_u64().is_some_and(|n| n > 0)
            }
            _ => false,
        }
    }
    if usage
        .raw_usage
        .as_ref()
        .is_some_and(|raw| server_units(raw, ""))
    {
        unresolved.push(json!({"meter":"provider_server_tools","reason":"known_service_units_without_applicable_tariff"}));
    }
    let mut resolution = tariff::resolve(usage, &normalized);
    if !unresolved.is_empty() {
        if let Some(reasons) = resolution["reasons"].as_array_mut() {
            reasons.extend(unresolved);
        }
        if resolution["status"] == "complete" {
            resolution["status"] = "partial".into();
        }
    }
    Some(resolution)
}

fn estimate(
    usage: &TokenUsage,
    cost: &Value,
    metadata: &std::collections::BTreeMap<String, Value>,
) -> Option<Value> {
    estimate_from_resolution(guarded_resolution(usage, cost, metadata)?)
}

fn estimate_from_resolution(resolution: Value) -> Option<Value> {
    if resolution["status"] != "complete" {
        return None;
    }
    let exact = resolution["known_subtotal_decimal"].as_str()?;
    let numeric: Value = serde_json::from_str(exact).ok()?;
    Some(
        json!({"amount":numeric,"amount_decimal":exact,"currency":"USD",
        "provenance":"catalog_estimate","units":"USD_per_million_tokens",
        "input_tokens_above":resolution["input_tokens_above"],"resolution":resolution}),
    )
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

#[derive(Clone)]
pub struct UsageContext {
    provider: String,
    captured_at: chrono::DateTime<chrono::Utc>,
    diagnostic_losses: std::sync::Arc<std::sync::atomic::AtomicU64>,
    transport_observations: std::sync::Arc<transport::BodyObservations>,
    requested_modalities: Vec<String>,
    adapter_conditions: std::sync::Arc<std::sync::Mutex<Value>>,
    authority: Option<String>,
    model: String,
    api: String,
    api_version: Option<String>,
    route: Option<String>,
    attempt: String,
    service_mode: Option<pioneer_protocol::NativeServiceModePlan>,
    captured_at_ms: i64,
    service_fields: Option<Value>,
    catalog: Option<std::sync::Arc<crate::catalog::ModelCatalog>>,
    native: Option<crate::catalog::CatalogModel>,
}
tokio::task_local! {
    static PREPARED_USAGE: std::cell::RefCell<Option<UsageContext>>;
    static ADAPTER_CONDITIONS: std::sync::Arc<std::sync::Mutex<Value>>;
}

impl UsageContext {
    /// Scope a single physical call after its initial evidence has committed.
    /// The reservation is consumed once; subsequent calls capture new evidence.
    pub async fn scope<F: std::future::Future>(&self, future: F) -> F::Output {
        transport::attempt_scope(
            self.attempt.clone(),
            ADAPTER_CONDITIONS.scope(
                self.adapter_conditions.clone(),
                PREPARED_USAGE.scope(std::cell::RefCell::new(Some(self.clone())), future),
            ),
        )
        .await
    }
    pub async fn scope_with_observations<F, H, HF>(
        &self,
        future: F,
        handler: H,
    ) -> anyhow::Result<F::Output>
    where
        F: std::future::Future,
        H: FnMut(Value) -> HF,
        HF: std::future::Future<Output = anyhow::Result<()>>,
    {
        transport::observe_scope(
            self.scope(future),
            handler,
            self.transport_observations.clone(),
        )
        .await
    }
    pub async fn observe_transport<F, H, HF>(
        &self,
        future: F,
        handler: H,
    ) -> anyhow::Result<F::Output>
    where
        F: std::future::Future,
        H: FnMut(Value) -> HF,
        HF: std::future::Future<Output = anyhow::Result<()>>,
    {
        transport::observe_scope(
            transport::attempt_scope(
                self.attempt.clone(),
                ADAPTER_CONDITIONS.scope(self.adapter_conditions.clone(), future),
            ),
            handler,
            self.transport_observations.clone(),
        )
        .await
    }
    pub fn prepare_operation(provider: &dyn crate::Provider, model: &str, api: &str) -> Self {
        let mut context = Self::capture(provider, model);
        let profile = provider.embedding_usage_profile();
        context.api = api.into();
        context.api_version = profile.api_version;
        context.route = profile.route;
        context
    }
    pub fn finalize_usage(&self, usage: &mut TokenUsage) {
        self.enrich(usage);
    }
    pub fn record_diagnostic_loss(&self) {
        self.diagnostic_losses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn initial_usage(&self) -> TokenUsage {
        let mut usage = TokenUsage::default();
        self.enrich(&mut usage);
        usage
    }
    pub fn prepare_request(
        provider: &dyn crate::Provider,
        request: &crate::ChatRequest,
    ) -> anyhow::Result<Self> {
        let profile = provider.request_usage_profile(request)?;
        let mut context = Self::capture(provider, &request.model);
        context.service_fields = request
            .service_mode
            .as_ref()
            .map(|_| crate::service_mode::request_fields(provider.name(), &profile.api, request))
            .transpose()?;
        context.service_mode = request.service_mode.clone();
        context.api = profile.api;
        context.api_version = profile.api_version;
        context.route = profile.route;
        context.requested_modalities = request
            .messages
            .iter()
            .flat_map(|m| &m.content_parts)
            .filter_map(|part| match part {
                crate::MessageContentPart::Text { .. } => None,
                crate::MessageContentPart::File { .. } => Some("file"),
                crate::MessageContentPart::Image { .. } => Some("image"),
                crate::MessageContentPart::Audio { .. } => Some("audio"),
                crate::MessageContentPart::Video { .. } => Some("video"),
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(str::to_owned)
            .collect();
        Ok(context)
    }

    pub(crate) fn capture(provider: &dyn crate::Provider, model: &str) -> Self {
        let diagnostic_losses = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        Self {
            provider: provider.name().into(),
            captured_at: chrono::Utc::now(),
            transport_observations: transport::BodyObservations::new(diagnostic_losses.clone()),
            diagnostic_losses,
            requested_modalities: Vec::new(),
            adapter_conditions: std::sync::Arc::new(std::sync::Mutex::new(json!({}))),
            authority: provider.authority_fingerprint().map(str::to_owned),
            model: model.into(),
            api: provider.usage_api().into(),
            api_version: provider.usage_api_version(),
            route: provider.usage_route(),
            attempt: pioneer_protocol::generate_id(21),
            service_mode: None,
            service_fields: None,
            captured_at_ms: chrono::Utc::now().timestamp_millis(),
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
        if let Ok(context) = PREPARED_USAGE.try_with(|slot| -> anyhow::Result<Self> {
            let context = slot
                .borrow_mut()
                .take()
                .ok_or_else(|| anyhow::anyhow!("request attempt reservation already consumed"))?;
            anyhow::ensure!(
                context.provider == provider.name()
                    && context.authority.as_deref() == provider.authority_fingerprint()
                    && context.model == request.model
                    && context.service_mode == request.service_mode
                    && context.api == profile.api
                    && context.route == profile.route
                    && context.api_version == profile.api_version,
                "prepared request profile changed before dispatch"
            );
            Ok(context)
        }) {
            return context;
        }
        Self::prepare_request(provider, request)
    }

    fn captured_mode_cost(
        &self,
        usage: &TokenUsage,
        descriptor: &pioneer_protocol::ServiceModeDescriptor,
    ) -> Option<(Value, Option<Value>)> {
        let catalog = self.catalog.as_ref()?;
        let entry = catalog.model(&self.provider, &self.model)?;
        service_mode_cost(
            usage,
            &entry.cost,
            &entry.metadata,
            catalog.context_tiers(&self.provider, &self.model)?,
            descriptor,
        )
    }

    pub(crate) fn enrich_error(&self, error: anyhow::Error) -> anyhow::Error {
        let previous = error_usage(&error).cloned();
        if previous.is_none() && self.service_mode.is_none() {
            return error;
        }
        let mut usage = previous.unwrap_or_default();
        self.enrich(&mut usage);
        if let (Some(_), Some(accounting)) = (&self.service_mode, &mut usage.accounting) {
            accounting["service_mode"]["prepared_parameters"] =
                self.service_fields.clone().unwrap_or(Value::Null);
            accounting["service_mode"]["sent_parameters"] = Value::Null;
            accounting["service_mode"]["dispatch_status"] = json!("unconfirmed_on_error");
        }
        with_error_usage(error, &usage)
    }
    pub(crate) fn enrich(&self, usage: &mut TokenUsage) {
        let unconfirmed_service_dispatch = usage.physical_attempt_id.as_deref()
            == Some(self.attempt.as_str())
            && usage.accounting.as_ref().is_some_and(|accounting| {
                accounting["service_mode"]["dispatch_status"] == "unconfirmed_on_error"
            });
        usage.provider = Some(self.provider.clone());
        usage.model = Some(self.model.clone());
        usage.api = Some(self.api.clone());
        usage.api_version = self.api_version.clone();
        usage.route = self.route.clone();
        usage.physical_attempt_id = Some(self.attempt.clone());
        if self.provider == "openai"
            && self.api == "embeddings"
            && self.route == route(crate::providers::openai::BASE_URL, "/embeddings")
        {
            // The actual Embeddings schema bills input only and exposes no
            // cache/output meters. Do not apply this to arbitrary proxies.
            // https://developers.openai.com/api/reference/resources/embeddings/methods/create
            usage.cache_read_input_tokens = Some(0);
            usage.cache_write_input_tokens = Some(0);
            usage.uncached_input_tokens = usage.input_tokens;
            usage.output_tokens = Some(0);
            usage.semantics = Some("openai_embeddings_input_only_cache_not_applicable_v1".into());
        }
        if self.api == "chat_completions" {
            apply_chat_usage_policy(usage, &self.provider, self.route.as_deref());
        }
        let native_reported_cost = usage
            .accounting
            .as_ref()
            .and_then(|a| a.get("reported_cost"))
            .filter(|v| v.is_object())
            .cloned();
        if !usage.accounting.as_ref().is_some_and(Value::is_object) {
            usage.accounting = Some(json!({}));
        }
        usage.accounting.as_mut().expect("accounting object")["requested_modalities"] =
            json!(self.requested_modalities);
        let adapter_conditions = self
            .adapter_conditions
            .lock()
            .expect("adapter conditions")
            .clone();
        usage.accounting.as_mut().expect("accounting object")["adapter_conditions"] =
            adapter_conditions.clone();
        usage.accounting = Some(accounting_with_entry(
            usage,
            &self.provider,
            &self.model,
            self.catalog.as_deref(),
            self.native.as_ref(),
        ));
        if let Some(accounting) = &mut usage.accounting {
            accounting["attempt_tariff_captured_at"] = json!(self.captured_at);
            accounting["requested_modalities"] = json!(self.requested_modalities);
            accounting["adapter_conditions"] = adapter_conditions;
            accounting["request_conditions_basis"] =
                "captured_application_request_before_adapter_transforms".into();

            accounting["transport_observation_coverage"] = self.transport_observations.coverage();
            accounting["diagnostic_losses"] = self
                .diagnostic_losses
                .load(std::sync::atomic::Ordering::Acquire)
                .into();
        }
        if let (Some(accounting), Some(reported)) = (&mut usage.accounting, native_reported_cost) {
            if accounting["reported_cost"].is_null() {
                accounting["reported_cost"] = reported;
            }
        }
        if let Some(plan) = &self.service_mode {
            // Keep captured request conditions available to the shared tariff
            // guards while evaluating the selected native service rates.
            let mut accounting = usage
                .accounting
                .as_ref()
                .expect("accounting was captured above")
                .clone();
            let source = self
                .catalog
                .as_ref()
                .and_then(|catalog| catalog.source_service_modes(&self.provider, &self.model));
            let descriptor = match source {
                Some(pioneer_protocol::SourceServiceModes::Known(modes)) => {
                    modes.values().find_map(|state| match state {
                        pioneer_protocol::SourceServiceMode::Parsed { descriptor }
                            if descriptor.id == plan.id
                                && descriptor.parameters == plan.parameters =>
                        {
                            Some(descriptor)
                        }
                        _ => None,
                    })
                }
                _ => None,
            };
            accounting["service_mode"] = json!({"requested":plan.id,"parameters":plan.parameters,"sent_parameters":self.service_fields,"reported_service_tier":usage.service_tier,"reported_speed":usage.raw_usage.as_ref().and_then(|raw| raw.get("speed")),"catalog_snapshot":self.catalog.as_ref().map(|catalog| catalog.snapshot_id()),"captured_at_ms":self.captured_at_ms,"source":descriptor});
            if unconfirmed_service_dispatch {
                accounting["service_mode"]["prepared_parameters"] =
                    self.service_fields.clone().unwrap_or(Value::Null);
                accounting["service_mode"]["sent_parameters"] = Value::Null;
                accounting["service_mode"]["dispatch_status"] = json!("unconfirmed_on_error");
            }
            if let Some(pricing) = accounting["pricing"].as_object_mut() {
                pricing.insert("service_mode".into(), json!({"version":1,"plan":plan,"descriptor":descriptor,
                    "context_tiers":self.catalog.as_ref().and_then(|c| c.context_tiers(&self.provider, &self.model))}));
            }
            if plan.id != "standard" {
                let (downgrade, matches_report) = service_mode_report(usage, &plan.parameters);
                let (resolution, estimate) = if downgrade {
                    (
                        accounting
                            .get("cost_resolution")
                            .filter(|v| !v.is_null())
                            .cloned(),
                        accounting
                            .get("estimated_cost")
                            .filter(|v| !v.is_null())
                            .cloned(),
                    )
                } else if matches_report && accounting["pricing_eligibility"]["eligible"] == true {
                    descriptor
                        .and_then(|descriptor| self.captured_mode_cost(usage, descriptor))
                        .map(|(resolution, estimate)| (Some(resolution), estimate))
                        .unwrap_or_default()
                } else {
                    (None, None)
                };
                accounting["estimated_cost"] = estimate.clone().unwrap_or(Value::Null);
                // An incomplete calculation still owns meter lines and precise
                // unresolved reasons. Keep that evidence without inventing a total.
                accounting["cost_resolution"] = resolution
                    .unwrap_or_else(|| json!({"version":1,"status":"unresolved","reasons":["unsupported_or_unconfirmed_service_price"],"lines":[]}));
                accounting["unresolved_reasons"] = if estimate.is_some() {
                    json!([])
                } else {
                    accounting["cost_resolution"]["reasons"].clone()
                };
                accounting["price_status"] = json!(if estimate.is_some() {
                    "captured_service_catalog_estimate"
                } else {
                    "unknown_or_conditional_service_price"
                });
            }
            usage.accounting = Some(accounting);
        }
        if let Some(accounting) = &mut usage.accounting {
            let selected = select_cost_basis(
                accounting.get("reported_cost"),
                accounting.get("estimated_cost"),
            );
            accounting["selected_cost"] = json!(selected);
        }
    }
}

fn service_mode_report(
    usage: &TokenUsage,
    parameters: &pioneer_protocol::ServiceModeParameters,
) -> (bool, bool) {
    use pioneer_protocol::{OpenAiTier, ServiceModeParameters as P};
    let reported = usage.service_tier.as_deref();
    let speed = usage
        .raw_usage
        .as_ref()
        .and_then(|raw| raw["speed"].as_str());
    let downgrade = match parameters {
        // Anthropic capacity tier and inference speed are separate
        // dimensions. Missing/unknown speed proves neither mode.
        P::AnthropicFast { .. } => speed == Some("standard"),
        P::OpenAiTier { .. } | P::BedrockNestedUltrafast => {
            matches!(reported, Some("default" | "standard"))
        }
        _ => false,
    };
    let matches_report = match parameters {
        P::OpenAiTier {
            tier: OpenAiTier::Priority,
        } => matches!(reported, Some("priority" | "fast")),
        P::OpenAiTier {
            tier: OpenAiTier::Flex,
        } => reported == Some("flex"),
        P::OpenAiTier {
            tier: OpenAiTier::Ultrafast,
        }
        | P::BedrockNestedUltrafast => reported == Some("ultrafast"),
        P::AnthropicFast { .. } => speed == Some("fast"),
        _ => false,
    };
    (downgrade, matches_report)
}

// The live attempt and historical reconciliation use the same captured inputs.
fn service_mode_cost(
    usage: &TokenUsage,
    cost: &Value,
    metadata: &std::collections::BTreeMap<String, Value>,
    tiers: &pioneer_protocol::ProviderContextTiers,
    descriptor: &pioneer_protocol::ServiceModeDescriptor,
) -> Option<(Value, Option<Value>)> {
    use pioneer_protocol::ProviderContextTiers;
    if cost.as_object().is_none_or(|cost| {
        cost.keys().any(|key| {
            !matches!(
                key.as_str(),
                "input"
                    | "output"
                    | "cache_read"
                    | "cache_write"
                    | "cacheRead"
                    | "cacheWrite"
                    | "tiers"
                    | "context_over_200k"
            )
        })
    }) {
        return None;
    }
    let context_ceiling = match tiers {
        ProviderContextTiers::Tiered { windows, .. } => windows.first().copied(),
        ProviderContextTiers::NoTiers => None,
        _ => return None,
    };
    let base_band = match tiers {
        ProviderContextTiers::NoTiers => true,
        ProviderContextTiers::Tiered { windows, .. } => usage.input_tokens? <= *windows.first()?,
        _ => false,
    };
    if !base_band {
        return None;
    } // no invented context × service multiplier
    let mut rates = descriptor.cost.clone()?;
    for (snake, camel) in [("cache_read", "cacheRead"), ("cache_write", "cacheWrite")] {
        if let Some(value) = rates.get(snake).cloned() {
            if rates.get(camel).is_some_and(|other| other != &value) {
                return None;
            }
            rates[camel] = value;
            rates.as_object_mut()?.remove(snake);
        }
    }
    let mut metadata = metadata.clone();
    let source = metadata.get_mut("pricingSource")?.as_object_mut()?;
    if source
        .get("raw")
        .and_then(Value::as_object)
        .is_some_and(|raw| {
            raw.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "input"
                        | "output"
                        | "cache_read"
                        | "cache_write"
                        | "cacheRead"
                        | "cacheWrite"
                        | "tiers"
                        | "context_over_200k"
                )
            })
        })
    {
        return None;
    }
    source.insert("raw".into(), rates.clone());
    let resolution = guarded_resolution_with_service_rates(usage, &rates, &metadata, true)?;
    let estimate = estimate_from_resolution(resolution.clone()).map(|mut estimate| {
        estimate["service_mode"] = json!(descriptor.id);
        estimate["source_context_input_ceiling"] = json!(context_ceiling);
        estimate["source_pricing_evidence_version"] = metadata
            .get("pricingEvidenceVersion")
            .cloned()
            .unwrap_or(Value::Null);
        estimate
    });
    Some((resolution, estimate))
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
                "default"
                    | "flex"
                    | "priority"
                    | "fast"
                    | "ultrafast"
                    | "standard"
                    | "scale"
                    | "auto"
                    | "reserved"
            ) {
                s.to_owned()
            } else {
                "unknown".into()
            }
        });
        self
    }
}

/// Reproduce a resolution from retained historical evidence only. Never consult
/// the live catalog, and never turn a reference tariff into a billing contract.
pub fn resolve_retained_tariff(usage: &TokenUsage, evidence: &Value) -> Value {
    if evidence["basis"] != "observed_billing_identity" {
        return json!({"version":1,"status":"unresolved","reasons":["unconfirmed_historical_applicability"],"lines":[]});
    }
    let metadata = std::collections::BTreeMap::from([
        ("pricingEvidenceVersion".into(), evidence["version"].clone()),
        ("pricingUnits".into(), evidence["units"].clone()),
        ("pricingSource".into(), evidence["source"].clone()),
    ]);
    if let Some(mode) = evidence.get("service_mode") {
        let unresolved = || json!({"version":1,"status":"unresolved","reasons":["unsupported_or_unconfirmed_service_price"],"lines":[]});
        if mode["version"] != 1 {
            return unresolved();
        }
        let Ok(plan) =
            serde_json::from_value::<pioneer_protocol::NativeServiceModePlan>(mode["plan"].clone())
        else {
            return unresolved();
        };
        if plan.id != "standard" {
            let (downgrade, matches_report) = service_mode_report(usage, &plan.parameters);
            if !downgrade {
                if !matches_report {
                    return unresolved();
                }
                let Ok(descriptor) = serde_json::from_value::<
                    pioneer_protocol::ServiceModeDescriptor,
                >(mode["descriptor"].clone()) else {
                    return unresolved();
                };
                let Ok(tiers) = serde_json::from_value::<pioneer_protocol::ProviderContextTiers>(
                    mode["context_tiers"].clone(),
                ) else {
                    return unresolved();
                };
                if descriptor.id != plan.id || descriptor.parameters != plan.parameters {
                    return unresolved();
                }
                return service_mode_cost(usage, &evidence["cost"], &metadata, &tiers, &descriptor)
                    .map(|(resolution, _)| resolution)
                    .unwrap_or_else(unresolved);
            }
        }
    }
    guarded_resolution(usage,&evidence["cost"],&metadata).unwrap_or_else(||json!({"version":1,"status":"unresolved","reasons":["unsupported_historical_conditions"],"lines":[]}))
}

/// A selection is a view of one charge, never reported + estimated spend.
pub fn select_cost_basis(reported: Option<&Value>, estimated: Option<&Value>) -> Option<Value> {
    if let Some(reported) =
        reported.filter(|v| v.is_object() && v.get("amount").is_some_and(Value::is_number))
    {
        return Some(
            json!({"basis":"provider_reported","amount":reported["amount"],"amount_lexical":reported["amount_lexical"],"amount_decimal":reported["amount_decimal"],"unresolved_reason":reported["unresolved_reason"],"currency":reported["currency"],"provenance":reported["provenance"]}),
        );
    }
    estimated.filter(|v|v.is_object()).map(|v|json!({"basis":"catalog_estimate","amount_decimal":v["amount_decimal"],"currency":v["currency"]}))
}

tokio::task_local! { static CAUSAL_PARENT: String; }
/// Runtime-only causal link. Never populated from provider/model JSON.
pub async fn scope_causal_parent<F: std::future::Future>(parent: String, future: F) -> F::Output {
    CAUSAL_PARENT.scope(parent, future).await
}
pub fn current_causal_parent() -> Option<String> {
    CAUSAL_PARENT.try_with(Clone::clone).ok()
}

/// Content-free local point on the existing observability process clock. The
/// clock's actual resolution is milliseconds; conversion does not add precision.
pub fn observation_point() -> Value {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    json!({"process_run_id":pioneer_observability::identity::run_identity().run_id,
        "ts_unix_ms":chrono::Utc::now().timestamp_millis(),
        "mono_ns":pioneer_observability::work::clock_ms().checked_mul(1_000_000),
        "clock_domain":"observability_work_clock_v1","clock_resolution_ns":1_000_000,
        "local_sequence":SEQUENCE.fetch_add(1,std::sync::atomic::Ordering::Relaxed)})
}

fn usage_coverage(usage: &TokenUsage) -> Value {
    let missing = [
        ("input", usage.input_tokens),
        ("output", usage.output_tokens),
        ("uncached_input", usage.uncached_input_tokens),
        ("cache_read", usage.cache_read_input_tokens),
        ("cache_write", usage.cache_write_input_tokens),
    ]
    .into_iter()
    .filter_map(|(meter, value)| value.is_none().then_some(meter))
    .collect::<Vec<_>>();
    let inconsistent = inconsistent_quantities(usage);
    let missing_all = usage.input_tokens.is_none()
        && usage.output_tokens.is_none()
        && usage.uncached_input_tokens.is_none()
        && usage.cache_read_input_tokens.is_none()
        && usage.cache_write_input_tokens.is_none();
    json!({"status":if inconsistent{"inconsistent"}else if missing_all{"missing"}else if missing.is_empty(){"complete"}else{"partial"},
        "missing_meters":missing,"input_semantics":"inclusive_uncached_plus_cache_read_plus_cache_write","reasoning_semantics":"subset_of_inclusive_output",
        "cache_applicability":if usage.semantics.as_deref()==Some("openai_embeddings_input_only_cache_not_applicable_v1"){"not_applicable"}else{"unknown_without_meter_evidence"}})
}

// Optional categories constrain known totals without filling omitted meters.
fn inconsistent_quantities(usage: &TokenUsage) -> bool {
    let categories = [
        usage.uncached_input_tokens,
        usage.cache_read_input_tokens,
        usage.cache_write_input_tokens,
    ];
    let sum = categories
        .into_iter()
        .flatten()
        .try_fold(0u64, u64::checked_add);
    let input_bad = sum.is_none()
        || usage.input_tokens.is_some_and(|input| {
            sum.is_none_or(|known| known > input)
                || (categories.iter().all(Option::is_some) && sum != Some(input))
        });
    let output_bad = usage
        .output_tokens
        .zip(usage.reasoning_tokens)
        .is_some_and(|(output, reasoning)| reasoning > output);
    let split_bad = usage.raw_usage.as_ref().is_some_and(|raw| {
        let split = raw["cache_creation"]["ephemeral_5m_input_tokens"]
            .as_u64()
            .zip(raw["cache_creation"]["ephemeral_1h_input_tokens"].as_u64());
        split.is_some_and(|(short, long)| {
            short.checked_add(long).is_none_or(|sum| {
                usage
                    .cache_write_input_tokens
                    .is_some_and(|known| known != sum)
            })
        })
    });
    fn invalid_counter(value: &Value, key: &str) -> bool {
        let quantity = key.ends_with("_tokens")
            || key.ends_with("Tokens")
            || key.ends_with("TokenCount")
            || key == "tokenCount";
        if quantity && !value.is_null() && value.as_u64().is_none() {
            return true;
        }
        match value {
            Value::Object(fields) => fields
                .iter()
                .any(|(key, value)| invalid_counter(value, key)),
            Value::Array(values) => values.iter().any(|value| invalid_counter(value, key)),
            _ => false,
        }
    }
    input_bad
        || output_bad
        || split_bad
        || usage
            .raw_usage
            .as_ref()
            .is_some_and(|raw| invalid_counter(raw, ""))
}
