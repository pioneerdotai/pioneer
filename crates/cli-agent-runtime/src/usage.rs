//! Contract-specific numeric allowlists. Never pass arbitrary terminal payloads
//! through this boundary: usage containers can contain credentials too.
use serde_json::{Map, Value, json};

pub fn counters(value: &Value, fields: &[&str]) -> Value {
    Value::Object(
        fields
            .iter()
            .filter_map(|key| value[*key].as_u64().map(|n| ((*key).to_owned(), n.into())))
            .collect(),
    )
}

pub fn codex_app_usage(value: &Value) -> Value {
    let fields = [
        "inputTokens",
        "cachedInputTokens",
        "cacheWriteInputTokens",
        "outputTokens",
        "reasoningOutputTokens",
        "totalTokens",
    ];
    let mut native = json!({"total":counters(&value["total"], &fields),"last":counters(&value["last"], &fields)});
    if let Some(n) = value["modelContextWindow"].as_u64() {
        native["modelContextWindow"] = n.into();
    }
    let total = &native["total"];
    json!({"runtime":"codex", "scope":"session_snapshot", "semantics":"codex_inclusive_cache_inclusive_reasoning", "input_tokens":total["inputTokens"], "output_tokens":total["outputTokens"], "cache_read_input_tokens":total["cachedInputTokens"], "cache_creation_input_tokens":total["cacheWriteInputTokens"], "reasoning_tokens":total["reasoningOutputTokens"], "total_tokens":total["totalTokens"], "native_usage":native})
}

pub fn claude_usage(value: &Value) -> Value {
    let mut result = counters(
        value,
        &[
            "input_tokens",
            "output_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
            "thinking_tokens",
            "reasoning_tokens",
        ],
    );
    for (key, fields) in [
        (
            "cache_creation",
            &["ephemeral_5m_input_tokens", "ephemeral_1h_input_tokens"][..],
        ),
        (
            "server_tool_use",
            &["web_search_requests", "web_fetch_requests"][..],
        ),
    ] {
        if value[key].is_object() {
            result[key] = counters(&value[key], fields);
        }
    }
    result
}

// Identifier fields only, bounded bytes and alphabet; never arbitrary prose.
fn contract_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:/".contains(&b))
}

pub fn claude_result(value: &Value) -> Value {
    let usage = claude_usage(&value["usage"]);
    let input = usage["input_tokens"]
        .as_u64()
        .and_then(|n| n.checked_add(usage["cache_read_input_tokens"].as_u64()?))
        .and_then(|n| n.checked_add(usage["cache_creation_input_tokens"].as_u64()?));
    let mut result = json!({"runtime":"claude", "scope":"turn_snapshot", "semantics":"claude_exclusive_cache_inclusive_reasoning", "input_tokens":input, "output_tokens":usage["output_tokens"], "cache_read_input_tokens":usage["cache_read_input_tokens"], "cache_creation_input_tokens":usage["cache_creation_input_tokens"], "native_usage":usage, "model_usage_scope":"session_snapshot_including_subagents", "cost_scope":"session_snapshot_including_subagents", "cost_basis":"claude_sdk_estimate"});
    for key in [
        "duration_ms",
        "duration_api_ms",
        "num_turns",
        "ttft_ms",
        "ttft_stream_ms",
        "time_to_request_ms",
        "time_to_request_from_spawn_ms",
    ] {
        if let Some(n) = value[key].as_u64() {
            result[key] = n.into();
        }
    }
    if let Some(cost) = value["total_cost_usd"]
        .as_f64()
        .filter(|n| n.is_finite() && *n >= 0.)
    {
        result["total_cost_usd"] = json!(cost);
    }
    let mut models = Map::new();
    if let Some(values) = value["modelUsage"].as_object() {
        for (name, model) in values
            .iter()
            .filter(|(name, _)| contract_identifier(name))
            .take(8)
        {
            let mut fields = counters(
                model,
                &[
                    "inputTokens",
                    "outputTokens",
                    "thinkingTokens",
                    "cacheCreationEphemeral5mInputTokens",
                    "cacheCreationEphemeral1hInputTokens",
                    "cacheReadInputTokens",
                    "cacheCreationInputTokens",
                    "webSearchRequests",
                    "contextWindow",
                    "maxOutputTokens",
                ],
            );
            if let Some(cost) = model["costUSD"]
                .as_f64()
                .filter(|n| n.is_finite() && *n >= 0.)
            {
                fields["costUSD"] = json!(cost);
            }
            for key in ["canonicalModel", "provider"] {
                if let Some(value) = model[key].as_str().filter(|s| contract_identifier(s)) {
                    fields[key] = value.into();
                }
            }
            if matches!(
                model["costBasis"].as_str(),
                Some("list" | "managed" | "unknown")
            ) {
                fields["costBasis"] = model["costBasis"].clone();
            }
            models.insert(name.clone(), fields);
        }
    }
    result["model_usage_truncated"] = json!(
        value["modelUsage"]
            .as_object()
            .is_some_and(|m| m.len() > models.len())
    );
    result["model_usage"] = models.into();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn claude_extended_sdk_fields_are_optional_evidence_not_extra_tokens() {
        let usage = claude_result(&json!({
            "usage":{"input_tokens":1,"cache_read_input_tokens":2,"cache_creation_input_tokens":3,"output_tokens":5},
            "ttft_ms":0,"ttft_stream_ms":1,"time_to_request_ms":2,"time_to_request_from_spawn_ms":3,
            "time_to_first_token_ms":999,"result":"SECRET",
            "modelUsage":{"claude-opus-4-7":{
                "thinkingTokens":4,"cacheCreationEphemeral5mInputTokens":2,"cacheCreationEphemeral1hInputTokens":1,
                "canonicalModel":"claude-opus-4-7","provider":"bedrock","costBasis":"managed","apiKey":"SECRET"
            }}
        }));
        assert_eq!(usage["input_tokens"], 6);
        assert_eq!(usage["output_tokens"], 5);
        assert_eq!(usage["ttft_ms"], 0);
        assert_eq!(usage["ttft_stream_ms"], 1);
        assert_eq!(usage["time_to_request_ms"], 2);
        assert_eq!(usage["time_to_request_from_spawn_ms"], 3);
        let model = &usage["model_usage"]["claude-opus-4-7"];
        assert_eq!(model["thinkingTokens"], 4);
        assert_eq!(model["cacheCreationEphemeral5mInputTokens"], 2);
        assert_eq!(model["cacheCreationEphemeral1hInputTokens"], 1);
        assert_eq!(model["canonicalModel"], "claude-opus-4-7");
        assert_eq!(model["provider"], "bedrock");
        assert_eq!(model["costBasis"], "managed");
        assert!(usage["time_to_first_token_ms"].is_null());
        assert!(!usage.to_string().contains("SECRET"));
        let old = claude_result(&json!({"modelUsage":{"old":{"inputTokens":1}}}));
        assert!(old["ttft_ms"].is_null());
        assert!(old["model_usage"]["old"]["costBasis"].is_null());
    }

    #[test]
    fn stats_allowlist_retains_zero_and_missing_without_secrets() {
        let usage = codex_app_usage(
            &json!({"total":{"inputTokens":0,"accessToken":"SECRET","outputTokens":-1},"last":{"cachedInputTokens":5},"token":"SECRET"}),
        );
        assert_eq!(usage["input_tokens"], 0);
        assert!(usage["output_tokens"].is_null());
        assert!(!usage.to_string().contains("SECRET"));
        assert_eq!(usage["native_usage"]["last"]["cachedInputTokens"], 5);
        let usage = claude_result(
            &json!({"usage":{"input_tokens":1,"cache_read_input_tokens":2,"cache_creation_input_tokens":3,"output_tokens":0,"authorization":"SECRET"},"total_cost_usd":0,"modelUsage":{"model":{"inputTokens":8,"apiKey":"SECRET"}},"result":"SECRET"}),
        );
        assert_eq!(usage["input_tokens"], 6);
        assert_eq!(usage["output_tokens"], 0);
        assert!(!usage.to_string().contains("SECRET"));
        assert!(claude_result(&json!({"usage":{"input_tokens":1}}))["input_tokens"].is_null());
    }
}
