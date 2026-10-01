use super::*;
use std::collections::BTreeMap;
fn decode<const API: u8>(value: Value) -> TokenUsage {
    serde_json::from_value::<WireUsage<API>>(value)
        .unwrap()
        .normalized()
}
#[test]
fn exclusive_cache_absent_read_write_and_zero_are_preserved() {
    for (api, value) in [
        (1, json!({"input_tokens":10,"output_tokens":8})),
        (2, json!({"inputTokens":10,"outputTokens":8})),
    ] {
        let u = if api == 1 {
            decode::<1>(value)
        } else {
            decode::<2>(value)
        };
        assert_eq!(u.input_tokens, Some(10));
        assert_eq!(u.cache_read_input_tokens, None);
    }
    let u = decode::<1>(
        json!({"input_tokens":10,"cache_read_input_tokens":100,"cache_creation_input_tokens":20,"output_tokens":8}),
    );
    assert_eq!(u.input_tokens, Some(130));
    assert_eq!(u.uncached_input_tokens, Some(10));
    assert_eq!(u.cache_write_input_tokens, Some(20));
    let u = decode::<2>(
        json!({"inputTokens":10,"cacheReadInputTokens":100,"cacheWriteInputTokens":20,"outputTokens":8,"totalTokens":138}),
    );
    assert_eq!(u.input_tokens, Some(130));
    assert_eq!(u.reported_total_tokens, Some(138));
    let u = decode::<1>(
        json!({"input_tokens":10,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}),
    );
    assert_eq!(u.cache_read_input_tokens, Some(0));
    assert_eq!(u.output_tokens, None);
}
#[test]
fn inclusive_chat_reasoning_is_not_added_to_output() {
    let u = decode::<0>(json!({"prompt_tokens":140,"completion_tokens":19,
        "prompt_tokens_details":{"cached_tokens":100,"cache_write_tokens":20},
        "completion_tokens_details":{"reasoning_tokens":10},"total_tokens":159}));
    assert_eq!(u.input_tokens, Some(140));
    assert_eq!(u.output_tokens, Some(19));
    assert_eq!(u.reasoning_tokens, Some(10));
    assert_eq!(u.uncached_input_tokens, Some(20));
}
#[test]
fn gemini_candidates_and_thoughts_are_separate_from_inclusive_total() {
    let u = decode::<3>(json!({"promptTokenCount":140,"cachedContentTokenCount":100,
        "candidatesTokenCount":9,"thoughtsTokenCount":10,"totalTokenCount":159}));
    assert_eq!(u.input_tokens, Some(140));
    assert_eq!(u.output_tokens, Some(19));
    assert_eq!(u.reasoning_tokens, Some(10));
    assert_eq!(u.reported_total_tokens, Some(159));
    assert_eq!(
        decode::<3>(json!({"candidatesTokenCount":0})).output_tokens,
        Some(0)
    );
    assert_eq!(decode::<3>(json!({})).output_tokens, None);
}
#[test]
fn partial_anthropic_snapshot_and_duplicate_terminal_do_not_drop_input() {
    let mut u = decode::<1>(
        json!({"input_tokens":10,"cache_creation_input_tokens":20,"cache_read_input_tokens":100,"output_tokens":1}),
    );
    let terminal = decode::<1>(json!({"output_tokens":8}));
    u.update(&terminal);
    u.update(&terminal);
    assert_eq!(u.input_tokens, Some(130));
    assert_eq!(u.output_tokens, Some(8));
    assert_eq!(
        u.raw_usage.as_ref().unwrap()["cache_read_input_tokens"],
        100
    );
    let encoded = serde_json::to_value(&u).unwrap();
    let restored: TokenUsage = serde_json::from_value(encoded).unwrap();
    assert_eq!(restored, u);
    let legacy: TokenUsage =
        serde_json::from_value(json!({"input_tokens":5,"output_tokens":2})).unwrap();
    assert_eq!(legacy.cache_read_input_tokens, None);
}
#[test]
fn telemetry_does_not_retain_secrets_prompts_arbitrary_ids_or_unbounded_arrays() {
    let u = decode::<0>(
        json!({"prompt_tokens":1,"messages":[{"content":"SECRET PROMPT"}],
        "api_key":"SECRET", "request_id":"arbitrary", "cost_details":{"prompt":"SECRET","upstream_inference_cost":0.2},
        "cacheDetails": vec![json!({"ttl":"5m","inputTokens":1});1000]}),
    );
    let raw = u.raw_usage.unwrap();
    let text = raw.to_string();
    assert!(!text.contains("SECRET"));
    assert!(!text.contains("arbitrary"));
    assert!(raw["cacheDetails"].as_array().unwrap().len() <= 16);
    assert_eq!(native_id(Some("secret\nheader")), None);
    let route_id = route(
        "https://user:secret@example.org/private-key?api_key=secret",
        "/chat/completions",
    )
    .unwrap();
    assert!(route_id.starts_with("https:endpoint_sha256:"));
    assert!(route_id.ends_with(";path=/chat/completions"));
    assert!(!route_id.contains("secret"));
    assert!(!route_id.contains("example.org"));
    assert!(!route_id.contains("private-key"));
    assert_eq!(
        Some(route_id.clone()),
        route(
            "https://example.org/private-key?other=query",
            "/chat/completions"
        )
    );
    assert_ne!(
        Some(route_id),
        route("https://example.org/another-path", "/chat/completions")
    );
}
fn metadata() -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("pricingEvidenceVersion".into(), json!(1)),
        ("pricingUnits".into(), json!("USD_per_million_tokens")),
    ])
}
#[test]
fn tariff_absence_cache_units_tiers_and_non_token_charges_are_not_free() {
    let u = decode::<0>(json!({"prompt_tokens":300,"completion_tokens":20,
        "prompt_tokens_details":{"cached_tokens":100,"cache_write_tokens":20}}));
    let rates = json!({"input":2,"output":10,"cacheRead":0.2,"cacheWrite":2.5});
    let e = estimate(&u, &rates, &metadata()).unwrap();
    assert!((e["amount"].as_f64().unwrap() - 0.00063).abs() < 1e-10);
    let mut rates_unknown = rates.clone();
    rates_unknown["cacheRead"] = Value::Null;
    assert!(estimate(&u, &rates_unknown, &metadata()).is_none());
    let mut wrong = metadata();
    wrong.insert("pricingUnits".into(), json!("USD_per_token"));
    assert!(estimate(&u, &rates, &wrong).is_none());
    assert!(estimate(&u, &rates, &BTreeMap::new()).is_none());
    let absent = decode::<0>(json!({"prompt_tokens":300,"completion_tokens":20}));
    assert!(estimate(&absent, &rates, &metadata()).is_none());
    let mut tiered = rates.clone();
    tiered["tiers"] =
        json!([{"inputTokensAbove":200,"input":4,"output":20,"cacheRead":0.4,"cacheWrite":5}]);
    assert_eq!(
        estimate(&u, &tiered, &metadata()).unwrap()["input_tokens_above"],
        200
    );
    tiered["tiers"] = json!([{"tier":{"type":"context","size":200},"input":4}]);
    assert!(estimate(&u, &tiered, &metadata()).is_none());
    let mut fee = metadata();
    fee.insert("pricingSource".into(), json!({"raw":{"request":"0.01"}}));
    assert!(estimate(&u, &rates, &fee).is_none());
}
#[test]
fn provider_cost_is_not_overwritten_by_estimate_or_unknown_tariff() {
    let u = decode::<0>(
        json!({"prompt_tokens":100,"completion_tokens":10,"cost":0.4,
        "cost_details":{"upstream_inference_cost":0.3}}),
    );
    let result = accounting(&u, "openrouter", "unknown", None);
    assert_eq!(result["reported_cost"]["amount"], 0.4);
    assert_eq!(result["reported_cost"]["currency"], "credits");
    assert!(result["estimated_cost"].is_null());
    assert_eq!(result["price_status"], "unknown_tariff");
    let catalog = crate::catalog::ModelCatalog::parse(
        include_str!("../../tests/fixtures/catalog/models.json"),
        include_str!("../../tests/fixtures/catalog/provenance.json"),
    )
    .unwrap();
    let r = accounting(&u, "openai", "gpt-5.4", Some(&catalog));
    assert_eq!(r["pricing"]["catalog_snapshot"], catalog.snapshot_id());
    assert_eq!(r["reported_cost"]["amount"], 0.4);
}
#[test]
fn streaming_extensions_are_not_assumed_for_unknown_profiles() {
    for p in ["custom", "nscale", "mistral", "openrouter", "cohere"] {
        assert!(stream_options(p, "fixture-unknown").is_none(), "{p}");
    }
    for p in [
        "groq",
        "fireworks",
        "venice",
        "friendli",
        "huggingface",
        "deepseek",
    ] {
        assert_eq!(
            stream_options(p, "fixture-unknown").unwrap()["include_usage"],
            true,
            "{p}"
        );
    }
}

#[test]
fn cache_control_does_not_enable_paid_long_retention_or_mark_other_routes() {
    assert!(anthropic_cache_control("https://custom.example/v1", "claude-fixture").is_none());
    assert!(anthropic_cache_control("https://api.anthropic.com", "unregistered-fixture").is_none());
    assert!(
        anthropic_cache_control("https://api.anthropic.com", "claude-unregistered-fixture")
            .is_none()
    );
}

#[test]
fn reported_and_estimated_cost_coexist_with_catalog_snapshot() {
    let models = json!({"openrouter":{"fixture":{"id":"fixture","name":"Fixture","provider":"openrouter","api":"openai-completions","baseUrl":"https://openrouter.ai/api/v1","contextWindow":1000,"maxTokens":100,"reasoning":false,"input":["text"],"cost":{"input":2,"output":10,"cacheRead":0.2,"cacheWrite":2.5},"pricingEvidenceVersion":1,"pricingUnits":"USD_per_million_tokens"}}});
    let provenance = json!({"openrouter":{"fixture":{"contextWindow":{"kind":"source","expression":"fixture"},"maxTokens":{"kind":"source","expression":"fixture"}}}});
    let catalog =
        crate::catalog::ModelCatalog::parse(&models.to_string(), &provenance.to_string()).unwrap();
    let usage = decode::<0>(
        json!({"prompt_tokens":300,"completion_tokens":20,"prompt_tokens_details":{"cached_tokens":100,"cache_write_tokens":20},"cost":0.4}),
    );
    let a = accounting(&usage, "openrouter", "fixture", Some(&catalog));
    assert_eq!(a["reported_cost"]["amount"], 0.4);
    assert_eq!(a["reported_cost"]["currency"], "credits");
    assert_eq!(a["estimated_cost"]["currency"], "USD");
    assert!((a["estimated_cost"]["amount"].as_f64().unwrap() - 0.00063).abs() < 1e-10);
    assert_eq!(a["pricing"]["catalog_snapshot"], catalog.snapshot_id());
}

#[test]
fn partial_gemini_thought_snapshot_preserves_candidates_without_double_counting() {
    let mut usage = decode::<3>(
        json!({"promptTokenCount":140,"candidatesTokenCount":9,"thoughtsTokenCount":2}),
    );
    let final_usage =
        decode::<3>(json!({"thoughtsTokenCount":10,"totalTokenCount":159,"serviceTier":"flex"}));
    usage.update(&final_usage);
    usage.update(&final_usage);
    assert_eq!(usage.output_tokens, Some(19));
    assert_eq!(usage.input_tokens, Some(140));
    assert_eq!(usage.service_tier.as_deref(), Some("flex"));
}
#[test]
fn nondefault_service_tier_does_not_use_standard_catalog_rate() {
    let u = TokenUsage {
        input_tokens: Some(1),
        output_tokens: Some(1),
        cache_read_input_tokens: Some(0),
        cache_write_input_tokens: Some(0),
        service_tier: Some("priority".into()),
        ..Default::default()
    };
    let metadata = BTreeMap::from([
        ("pricingEvidenceVersion".into(), json!(1)),
        ("pricingUnits".into(), json!("USD_per_million_tokens")),
    ]);
    assert!(estimate(&u, &json!({"input":1,"output":1}), &metadata).is_none());
}
#[test]
fn nested_reported_cost_survives_without_inventing_currency() {
    let u =
        decode::<0>(json!({"cost":{"total_cost":0.012,"request_cost":0.005,"untrusted":"SECRET"}}));
    let a = accounting(&u, "perplexity", "sonar", None);
    assert_eq!(a["reported_cost"]["amount"], 0.012);
    assert!(a["reported_cost"]["currency"].is_null());
    assert!(!u.raw_usage.unwrap().to_string().contains("SECRET"));
}

#[test]
fn long_retention_and_modality_usage_do_not_receive_a_standard_text_estimate() {
    let metadata = BTreeMap::from([
        ("pricingEvidenceVersion".into(), json!(1)),
        ("pricingUnits".into(), json!("USD_per_million_tokens")),
    ]);
    for raw in [
        json!({"cache_creation":{"ephemeral_1h_input_tokens":10}}),
        json!({"promptTokensDetails":[{"modality":"AUDIO","tokenCount":10}]}),
        json!({"prompt_tokens_details":{"audio_tokens":10}}),
    ] {
        let u = TokenUsage {
            input_tokens: Some(10),
            output_tokens: Some(1),
            cache_read_input_tokens: Some(0),
            cache_write_input_tokens: Some(0),
            raw_usage: Some(raw),
            ..Default::default()
        };
        assert!(estimate(&u, &json!({"input":1,"output":1}), &metadata).is_none());
    }
}

#[test]
fn native_cache_marker_requires_evidenced_model_profile_and_default_retention() {
    let model = json!({"id":"claude-fixture","name":"Fixture","provider":"anthropic","api":"anthropic-messages","baseUrl":"https://api.anthropic.com","contextWindow":1000,"maxTokens":100,"reasoning":false,"input":["text"],"cost":{"input":2,"output":10,"cacheRead":0.2,"cacheWrite":2.5},"pricingEvidenceVersion":1});
    let origin = json!({"anthropic":{"claude-fixture":{"contextWindow":{"kind":"source","expression":"fixture"},"maxTokens":{"kind":"source","expression":"fixture"}}}});
    let catalog = |model: Value| {
        crate::catalog::ModelCatalog::parse(
            &json!({"anthropic":{"claude-fixture":model}}).to_string(),
            &origin.to_string(),
        )
        .unwrap()
    };
    assert_eq!(
        anthropic_cache_control_in_catalog(
            "https://api.anthropic.com",
            "claude-fixture",
            &catalog(model.clone())
        ),
        Some(json!({"type":"ephemeral"}))
    );
    assert!(
        anthropic_cache_control_in_catalog(
            "https://private-proxy.example",
            "claude-fixture",
            &catalog(model.clone())
        )
        .is_none()
    );
    for variant in [0, 1, 2] {
        let mut m = model.clone();
        match variant {
            0 => {
                m.as_object_mut().unwrap().remove("pricingEvidenceVersion");
            }
            1 => m["api"] = json!("openai-completions"),
            _ => m["compat"] = json!({"supportsPromptCaching":false}),
        };
        assert!(
            anthropic_cache_control_in_catalog(
                "https://api.anthropic.com",
                "claude-fixture",
                &catalog(m)
            )
            .is_none()
        );
    }
}
