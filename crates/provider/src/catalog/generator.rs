//! Normalize public model metadata for the runtime catalog.
use super::{InputLimit, LimitOrigin, ModelCatalog, OriginKind};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[path = "compatibility.rs"]
mod compatibility;
#[path = "overrides.rs"]
mod overrides;
#[path = "rules.rs"]
mod rules;
#[path = "sources.rs"]
mod sources;

pub const SOURCE_URLS: [&str; 4] = [
    "https://models.dev/api.json",
    "https://openrouter.ai/api/v1/models",
    "https://ai-gateway.vercel.sh/v1/models",
    "https://integrate.api.nvidia.com/v1/models",
];

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceSnapshot {
    pub captured_at: String,
    pub sources: BTreeMap<String, SourceResponse>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceResponse {
    #[serde(default)]
    pub status: u16,
    #[serde(default)]
    pub body: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
impl SourceSnapshot {
    pub fn validate(&self) -> Result<()> {
        chrono::DateTime::parse_from_rfc3339(&self.captured_at)
            .context("invalid source capture timestamp")?;
        for url in SOURCE_URLS {
            let source = self
                .sources
                .get(url)
                .with_context(|| format!("missing catalog source: {url}"))?;
            ensure!(
                source.status == 200 && source.error.is_none() && source.body.is_object(),
                "incomplete catalog source: {url}"
            );
        }
        ensure!(
            self.sources[SOURCE_URLS[0]]
                .body
                .as_object()
                .is_some_and(|providers| providers.values().any(|p| p["models"].is_object())),
            "invalid models.dev source structure"
        );
        for url in &SOURCE_URLS[1..] {
            ensure!(
                self.sources[*url].body["data"]
                    .as_array()
                    .is_some_and(|models| !models.is_empty()),
                "invalid or empty catalog source: {url}"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GeneratedCatalog {
    pub models: BTreeMap<String, BTreeMap<String, Value>>,
    pub provenance: BTreeMap<String, BTreeMap<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_capabilities: Option<super::ToolCapabilities>,
}
impl GeneratedCatalog {
    pub fn validate(&self) -> Result<()> {
        ModelCatalog::parse_with_capabilities(
            &serde_json::to_string(&self.models)?,
            &serde_json::to_string(&self.provenance)?,
            self.tool_capabilities.clone(),
        )?;
        for (provider, models) in &self.models {
            ensure!(
                !provider.is_empty()
                    && provider
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
                "invalid provider filename"
            );
            ensure!(!models.is_empty(), "empty provider catalog");
            for model in models.values() {
                for field in ["id", "name", "api", "provider"] {
                    ensure!(
                        model[field].as_str().is_some_and(|s| !s.is_empty()),
                        "invalid catalog {field}"
                    );
                }
                ensure!(model["baseUrl"].is_string(), "invalid base URL");
                ensure!(
                    model["reasoning"].is_boolean(),
                    "invalid reasoning metadata"
                );
                ensure!(
                    model["input"].as_array().is_some_and(|a| a
                        .iter()
                        .all(|v| v.as_str().is_some_and(|s| !s.is_empty()))),
                    "invalid model modalities"
                );
                for field in ["input", "output", "cacheRead", "cacheWrite"] {
                    ensure!(
                        model["cost"][field].is_null()
                            || model["cost"][field]
                                .as_f64()
                                .is_some_and(|v| v.is_finite() && v >= 0.),
                        "invalid model pricing"
                    );
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Candidate {
    model: Value,
    context_origin: LimitOrigin,
    output_origin: LimitOrigin,
    reasoning_options: Value,
    input_limit: Option<u64>,
    tool_calling: Option<bool>,
}
impl Candidate {
    fn id(&self) -> &str {
        text(&self.model["id"])
    }
    fn provider(&self) -> &str {
        text(&self.model["provider"])
    }
    fn api(&self) -> &str {
        text(&self.model["api"])
    }
    fn compat(&mut self, patch: Value) {
        merge(&mut self.model["compat"], patch);
    }
    fn thinking(&mut self, patch: Value) {
        merge(&mut self.model["thinkingLevelMap"], patch);
    }
    fn override_limit(&mut self, field: &str, value: u64, reason: &str) {
        self.model[field] = json!(value);
        let origin = LimitOrigin {
            kind: OriginKind::Override,
            expression: reason.into(),
        };
        if field == "contextWindow" {
            self.context_origin = origin;
        } else {
            self.output_origin = origin;
        }
    }
}
fn text(value: &Value) -> &str {
    value.as_str().unwrap_or("")
}
fn has(value: &Value, needle: &str) -> bool {
    value
        .as_array()
        .is_some_and(|v| v.iter().any(|x| x == needle))
}
fn number(value: &Value) -> f64 {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .filter(|v: &f64| v.is_finite())
        .unwrap_or(0.)
}
fn round(value: f64) -> f64 {
    (value * 1_000_000.).round() / 1_000_000.
}
fn merge(target: &mut Value, patch: Value) {
    if !target.is_object() {
        *target = json!({});
    }
    if let Some(values) = patch.as_object() {
        target.as_object_mut().unwrap().extend(values.clone());
    }
}
// Match source defaults without converting malformed metadata into valid prices.
fn missing_or_falsy(value: &Value) -> bool {
    value.is_null() || value == false || value.as_f64() == Some(0.) || value == ""
}
fn source_price(value: &Value) -> Value {
    value.clone()
}
fn cost(source: &Value) -> Value {
    // Preserve tier conditions, TTL prices and non-token fees from the source.
    let mut result = source.as_object().cloned().unwrap_or_default();
    for (key, field) in [
        ("input", "input"),
        ("output", "output"),
        ("cacheRead", "cache_read"),
        ("cacheWrite", "cache_write"),
    ] {
        result.insert(key.into(), source_price(&source[field]));
    }
    Value::Object(result)
}
fn limit(value: &Value, fallback: u64, source: &str) -> (Value, LimitOrigin) {
    if missing_or_falsy(value) {
        (
            json!(fallback),
            LimitOrigin {
                kind: OriginKind::Fallback,
                expression: format!("{source} or {fallback}"),
            },
        )
    } else {
        (
            value.clone(),
            LimitOrigin {
                kind: OriginKind::Source,
                expression: source.into(),
            },
        )
    }
}
fn base(
    provider: &str,
    id: &str,
    api: &str,
    url: &str,
    source: &Value,
    defaults: (u64, u64),
) -> Candidate {
    let (context, context_origin) = limit(
        &source["limit"]["context"],
        defaults.0,
        "models.dev.limit.context",
    );
    let (output, output_origin) = limit(
        &source["limit"]["output"],
        defaults.1,
        "models.dev.limit.output",
    );
    Candidate {
        model: json!({"id":id,"name":source["name"].as_str().filter(|s|!s.is_empty()).unwrap_or(id),
        "provider":provider,"api":api,"baseUrl":url,"reasoning":source["reasoning"]==true,
        "input":source["modalities"]["input"].as_array().cloned().unwrap_or_default(),
        "inputOrigin":{"kind":if source["modalities"]["input"].is_array(){"source"}else{"fallback"},"expression":"models.dev.modalities.input"},
        "sourceMetadata":source,
        "inputConstraints":source["inputConstraints"],
        "output":source["modalities"]["output"],
        "cost":cost(&source["cost"]),"pricingSource":{"url":SOURCE_URLS[0],"units":"USD_per_million_tokens","raw":source["cost"]},"contextWindow":context,"maxTokens":output,
        "sourceGeneration":{"temperature":source["temperature"],"reasoningOptions":source["reasoning_options"]}}),
        context_origin,
        output_origin,
        reasoning_options: source["reasoning_options"].clone(),
        input_limit: source["limit"]["input"].as_u64().filter(|limit| *limit > 0),
        tool_calling: source["tool_call"].as_bool(),
    }
}

/// Full pinned-reference transformation, including dynamic entries not present
/// in the saved fixture. First source wins identity collisions, as in Pi.
pub fn generate(snapshot: &SourceSnapshot, strict: bool) -> Result<GeneratedCatalog> {
    snapshot.validate()?;
    let (mut candidates, specialized) = sources::models_dev(
        &snapshot.sources[SOURCE_URLS[0]].body,
        &snapshot.sources[SOURCE_URLS[3]].body,
        strict,
    )?;
    candidates.extend(sources::openrouter(&snapshot.sources[SOURCE_URLS[1]].body));
    candidates.extend(sources::vercel(&snapshot.sources[SOURCE_URLS[2]].body));
    candidates.retain(|m| {
        !(m.provider() == "xai" && rules::XAI_BUILTIN_EXCLUDED_MODEL_IDS.contains(&m.id())
            || matches!(m.provider(), "opencode" | "opencode-go")
                && m.id() == "gpt-5.3-codex-spark")
    });
    overrides::apply(&mut candidates)?;
    let supplements = sources::registered_supplements(
        &snapshot.sources[SOURCE_URLS[0]].body,
        &specialized,
        &candidates,
    );
    candidates.extend(supplements);
    for model in &mut candidates {
        compatibility::apply(model);
        model.model["pricingEvidenceVersion"] = json!(1);
        model.model["pricingCapturedAt"] = json!(snapshot.captured_at);
        model.model["pricingUnits"] = json!(if model.provider() == "github-copilot" {
            "unknown_subscription_units"
        } else {
            "USD_per_million_tokens"
        });
    }
    compatibility::fallbacks(&mut candidates);
    let mut output = GeneratedCatalog {
        models: BTreeMap::new(),
        provenance: BTreeMap::new(),
        tool_capabilities: Some(sources::tool_capabilities(snapshot)),
    };
    for candidate in candidates {
        let provider = candidate.provider().to_owned();
        let id = candidate.id().to_owned();
        let entries = output.models.entry(provider.clone()).or_default();
        if entries.contains_key(&id) {
            continue;
        }
        let mut origins =
            json!({"contextWindow":candidate.context_origin,"maxTokens":candidate.output_origin});
        if let Some(supported) = candidate.tool_calling {
            origins["toolCalling"] = json!(supported);
        }
        if let Some(value) = candidate.input_limit {
            // Keep the Pi model contract unchanged; retain this additional
            // authoritative constraint beside its source provenance.
            origins["inputLimit"] = serde_json::to_value(InputLimit {
                value,
                origin: LimitOrigin {
                    kind: OriginKind::Source,
                    expression: "models.dev.limit.input".into(),
                },
            })?;
        }
        output
            .provenance
            .entry(provider)
            .or_default()
            .insert(id.clone(), origins);
        entries.insert(id, candidate.model);
    }
    output.validate()?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot() -> SourceSnapshot {
        serde_json::from_str(include_str!("../../tests/fixtures/catalog/sources.json")).unwrap()
    }
    fn differences(path: &str, actual: &Value, expected: &Value, out: &mut Vec<String>) {
        match (actual, expected) {
            (Value::Number(a), Value::Number(b)) if a.as_f64() == b.as_f64() => {}
            (Value::Object(a), Value::Object(b)) => {
                let keys: std::collections::BTreeSet<_> = a.keys().chain(b.keys()).collect();
                for k in keys {
                    match (a.get(k), b.get(k)) {
                        (Some(a), Some(b)) => differences(&format!("{path}/{k}"), a, b, out),
                        (a, b) => out.push(format!("{path}/{k}: actual {a:?}; expected {b:?}")),
                    }
                }
            }
            (Value::Array(a), Value::Array(b)) if a.len() == b.len() => {
                for (i, (a, b)) in a.iter().zip(b).enumerate() {
                    differences(&format!("{path}/{i}"), a, b, out);
                }
            }
            (a, b) if a == b => {}
            (a, b) => out.push(format!("{path}: actual {a}; expected {b}")),
        }
    }

    fn project_pinned_cost_fields(actual: &mut Value, expected: &Value, raw: &Value) {
        let prices = actual.as_object_mut().unwrap();
        // Only source-backed additions absent from Pi's normalized contract
        // may be projected out. Changed known rates still reach differences.
        for (field, value) in prices.iter() {
            if expected.get(field).is_none() {
                assert_eq!(
                    raw.get(field),
                    Some(value),
                    "additional cost field {field} must retain its exact source value"
                );
            }
        }
        prices.retain(|field, _| expected.get(field).is_some());
    }
    #[test]
    fn missing_source_price_is_unknown_and_explicit_zero_is_free() {
        let c = cost(
            &json!({"input":2,"output":0,"tiers":[{"tier":{"type":"context","size":200000},"input":4}]}),
        );
        assert_eq!(c["input"], 2);
        assert_eq!(c["output"], 0);
        assert!(c["cacheRead"].is_null());
        assert!(c["cacheWrite"].is_null());
        assert_eq!(c["tiers"][0]["tier"]["size"], 200000);
        assert_eq!(source_price(&json!(false)), false);
    }

    #[test]
    fn separate_input_limit_reaches_runtime_without_changing_pi_model_fields() {
        let mut source = snapshot();
        let model = &mut source.sources.get_mut(SOURCE_URLS[0]).unwrap().body["openai"]["models"]["gpt-5-nano"];
        model["limit"]["input"] = json!(272_000);
        let generated = generate(&source, true).unwrap();
        let catalog = ModelCatalog::parse(
            &serde_json::to_string(&generated.models).unwrap(),
            &serde_json::to_string(&generated.provenance).unwrap(),
        )
        .unwrap();
        let limits = catalog.limits("openai", "gpt-5-nano");
        assert_eq!(limits.max_input, Some(272_000));
        assert_eq!(limits.context_window, 400_000);
        assert!(
            generated.models["openai"]["gpt-5-nano"]
                .get("inputLimit")
                .is_none()
        );
        assert_eq!(
            generated.provenance["openai"]["gpt-5-nano"]["inputLimit"]["origin"]["kind"],
            "source"
        );
        assert_eq!(catalog.limits("openai", "unknown").max_input, None);
    }

    #[test]
    fn pinned_non_media_catalog_preserves_pi_contract_and_distinguishes_unknown_pricing() {
        let mut generated = generate(&snapshot(), true).unwrap();
        // Pioneer exposes standard CN/global profiles as well as Pi's coding
        // plans. The supplements are verified against their own source below.
        for supplement in ["glm", "zai-standard"] {
            assert!(generated.models.remove(supplement).is_some());
            assert!(generated.provenance.remove(supplement).is_some());
        }
        // Pi's explicit DeepSeek definitions predate the pinned source snapshot.
        // Pioneer keeps source limits/modalities/prices authoritative. This
        // explicit golden overlay covers the full changed provider subtree,
        // including the source-only alias, without dropping it from comparison.
        let deepseek: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/catalog/pioneer-deepseek.json"
        ))
        .unwrap();
        let mut reference: Value =
            serde_json::from_str(include_str!("../../tests/fixtures/catalog/provenance.json"))
                .unwrap();
        reference["deepseek"] = deepseek["provenance"].clone();
        let mut origins = Vec::new();
        for (p, models) in &generated.provenance {
            if reference.get(p).is_none() {
                continue;
            }
            for (id, fields) in models {
                for field in ["contextWindow", "maxTokens"] {
                    if reference[p][id].is_null() {
                        continue;
                    }
                    differences(
                        &format!("{p}/{id}/{field}/kind"),
                        &fields[field]["kind"],
                        &reference[p][id][field]["kind"],
                        &mut origins,
                    );
                }
            }
        }
        assert!(
            origins.is_empty(),
            "origin differences:\n{}",
            origins.join("\n")
        );
        let unprojected = serde_json::to_value(generated.models).unwrap();
        let mut actual = unprojected.clone();
        let mut expected: Value =
            serde_json::from_str(include_str!("../../tests/fixtures/catalog/models.json")).unwrap();
        expected["deepseek"] = deepseek["models"].clone();
        // Preserve the old reader fixture while the generator fixes 3.1 Pro.
        for models in expected.as_object_mut().unwrap().values_mut() {
            for model in models.as_object_mut().unwrap().values_mut() {
                if model["api"] == "google-generative-ai"
                    && model["id"]
                        .as_str()
                        .is_some_and(|id| id.starts_with("gemini-3.1-pro"))
                {
                    model["thinkingLevelMap"]["medium"] = json!("MEDIUM");
                }
            }
        }
        // Compare the pinned Pi surface, which deliberately had text/image
        // only. Pioneer media supplements and new registered source routes
        // have their own propagation fixtures below.
        // The Pi fixture collapsed missing prices into zero. Keep every
        // known price under exact comparison; allow unknown only when the
        // source omitted that exact category.
        let captured_at = snapshot().captured_at;
        let providers = actual.as_object_mut().unwrap();
        providers.retain(|provider, _| !expected[provider].is_null());
        for (provider, models) in providers {
            models
                .as_object_mut()
                .unwrap()
                .retain(|id, _| !expected[provider][id].is_null());
            for (id, model) in models.as_object_mut().unwrap() {
                // The Pi fixture predates the media contract and supplements.
                let image = has(&model["input"], "image");
                model["input"] = if image {
                    json!(["text", "image"])
                } else {
                    json!(["text"])
                };

                let source = model.get("pricingSource").cloned().unwrap_or(Value::Null);
                if model.get("pricingEvidenceVersion").is_some() {
                    assert_eq!(model["pricingEvidenceVersion"], 1);
                    assert_eq!(model["pricingCapturedAt"], captured_at);
                    assert_eq!(
                        model["pricingUnits"],
                        if provider == "github-copilot" {
                            json!("unknown_subscription_units")
                        } else {
                            json!("USD_per_million_tokens")
                        }
                    );
                }
                for field in [
                    "pricingSource",
                    "pricingEvidenceVersion",
                    "pricingCapturedAt",
                    "pricingUnits",
                    "sourceGeneration",
                    "inputOrigin",
                    "sourceMetadata",
                    "inputConstraints",
                    "output",
                ] {
                    model.as_object_mut().unwrap().remove(field);
                }
                let reference = &expected[provider][id]["cost"];
                if let (Some(fallbacks), Some(old)) = (
                    model
                        .get_mut("compat")
                        .and_then(|compat| compat.get_mut("allowedFallbackModels"))
                        .and_then(Value::as_array_mut),
                    expected[provider][id]["compat"]["allowedFallbackModels"].as_array(),
                ) {
                    assert_eq!(fallbacks.len(), old.len());
                    for (fallback, old) in fallbacks.iter_mut().zip(old) {
                        let canonical = &unprojected[fallback["provider"].as_str().unwrap()]
                            [fallback["model"].as_str().unwrap()];
                        assert_eq!(fallback["cost"], canonical["cost"]);
                        project_pinned_cost_fields(
                            &mut fallback["cost"],
                            &old["cost"],
                            &canonical["pricingSource"]["raw"],
                        );
                    }
                }
                let prices = model["cost"].as_object_mut().unwrap();
                // Native source fields remain in production; the old fixture
                // compares normalized fields only. Separate tests assert their
                // retention and conservative handling of opaque tier conditions.
                prices.retain(|key, _| reference.get(key).is_some());
                for (field, native) in [
                    ("input", "input"),
                    ("output", "output"),
                    ("cacheRead", "cache_read"),
                    ("cacheWrite", "cache_write"),
                ] {
                    if provider == "openrouter"
                        && matches!(id.as_str(), "openrouter/auto" | "openrouter/auto-beta")
                        && matches!(field, "input" | "output")
                    {
                        let native = if field == "input" {
                            "prompt"
                        } else {
                            "completion"
                        };
                        assert_eq!(source["raw"][native], "-1");
                        assert_eq!(
                            source["unknownRateContract"]["reason"],
                            "dynamic_selected_model_tariff"
                        );
                        assert_eq!(prices[field], Value::Null);
                        assert_eq!(reference[field], -1_000_000);
                        // Comparison-only projection of the exact old sentinel.
                        prices.insert(field.into(), reference[field].clone());
                    }
                    if prices.get(field) == Some(&Value::Null)
                        && reference[field].as_f64() == Some(0.)
                    {
                        let router = match field {
                            "input" => "prompt",
                            "output" => "completion",
                            "cacheRead" => "input_cache_read",
                            _ => "input_cache_write",
                        };
                        assert!(
                            source["raw"][native].is_null() && source["raw"][router].is_null(),
                            "{provider}/{id}/{field}: explicit zero must not become unknown"
                        );
                        prices.insert(field.into(), reference[field].clone());
                    }
                }
                if let (Some(tiers), Some(old)) = (
                    prices.get_mut("tiers").and_then(Value::as_array_mut),
                    reference["tiers"].as_array(),
                ) {
                    assert_eq!(tiers.len(), old.len());
                    for (index, (tier, old)) in tiers.iter_mut().zip(old).enumerate() {
                        project_pinned_cost_fields(tier, old, &source["raw"]["tiers"][index]);
                        for (field, native) in [
                            ("input", "input"),
                            ("output", "output"),
                            ("cacheRead", "cache_read"),
                            ("cacheWrite", "cache_write"),
                        ] {
                            if tier[field].is_null() && old[field].as_f64() == Some(0.) {
                                assert!(source["raw"][native].is_null());
                                tier[field] = old[field].clone();
                            }
                        }
                    }
                }
            }
        }
        let mut diff = Vec::new();
        differences("", &actual, &expected, &mut diff);
        assert!(
            diff.is_empty(),
            "{} differences:\n{}",
            diff.len(),
            diff.iter().take(60).cloned().collect::<Vec<_>>().join("\n")
        );
    }
}

#[cfg(test)]
mod behavior_tests {
    use super::*;
    fn snapshot() -> SourceSnapshot {
        serde_json::from_str(include_str!("../../tests/fixtures/catalog/sources.json")).unwrap()
    }
    #[test]
    fn glm_region_and_product_catalogs_use_their_own_sources() {
        let mut source = snapshot();
        let data = &mut source.sources.get_mut(SOURCE_URLS[0]).unwrap().body;
        for (upstream, context, price) in [("zai", 61001, 1.25), ("zhipuai", 62002, 2.5)] {
            data[upstream]["models"]["fixture-standard"] = json!({
                "tool_call":true, "name":"Fixture standard", "limit":{"context":context,"output":1024},
                "modalities":{"input":["text"]}, "cost":{"input":price}
            });
        }
        let generated = generate(&source, true).unwrap();
        // Runtime profile aliases must use their specialized catalog source,
        // without a second generic row under the runtime provider name.
        for runtime in ["zai-coding", "glm-coding"] {
            assert!(!generated.models.contains_key(runtime));
        }
        let reader = ModelCatalog::parse(
            &serde_json::to_string(&generated.models).unwrap(),
            &serde_json::to_string(&generated.provenance).unwrap(),
        )
        .unwrap();
        for (runtime, url, context, price) in [
            ("zai", "https://api.z.ai/api/paas/v4", 61001, 1.25),
            ("glm", "https://open.bigmodel.cn/api/paas/v4", 62002, 2.5),
        ] {
            let model = reader.model(runtime, "fixture-standard").unwrap();
            assert_eq!(model.base_url, url);
            assert_eq!(model.api, "openai-completions");
            assert_eq!(model.cost["input"], price);
            assert_eq!(
                reader.limits(runtime, "fixture-standard").context_window,
                context
            );
            assert_eq!(
                reader.limits(runtime, "fixture-standard").context_origin,
                OriginKind::Source
            );
        }
        assert!(reader.model("zai-coding", "fixture-standard").is_none());
        assert!(reader.model("glm-coding", "fixture-standard").is_none());
        assert_eq!(
            reader.model("zai-coding", "glm-5.2").unwrap().base_url,
            "https://api.z.ai/api/coding/paas/v4"
        );
        assert_eq!(
            reader.model("glm-coding", "glm-5.2").unwrap().base_url,
            "https://open.bigmodel.cn/api/coding/paas/v4"
        );
    }
    #[test]
    fn new_entries_are_transformed_and_unknown_limits_remain_unknown() {
        let mut source = snapshot();
        source.sources.get_mut(SOURCE_URLS[0]).unwrap().body["openai"]["models"]["fixture-new"] = json!({"name":"New source model","tool_call":true,"reasoning":true,"reasoning_options":[{"type":"effort","values":["none","high"]}],"modalities":{"input":["text","image"]},"cost":{"input":2.25}});
        let result = generate(&source, true).unwrap();
        let model = &result.models["openai"]["fixture-new"];
        assert_eq!(model["name"], "New source model");
        assert_eq!(model["input"], json!(["text", "image"]));
        assert_eq!(model["cost"]["input"], 2.25);
        assert_eq!(model["thinkingLevelMap"]["off"], "none");
        assert_eq!(model["thinkingLevelMap"]["high"], "high");
        assert_eq!(
            result.provenance["openai"]["fixture-new"]["toolCalling"],
            true
        );
        let reader = ModelCatalog::parse(
            &serde_json::to_string(&result.models).unwrap(),
            &serde_json::to_string(&result.provenance).unwrap(),
        )
        .unwrap();
        assert_eq!(
            reader.limits("openai", "fixture-new").context_window,
            128000
        );
        assert_eq!(
            reader.limits("azure-openai", "fixture-new").max_output,
            None
        );
        assert!(
            result.models["azure-openai-responses"]["fixture-new"]["thinkingLevelMap"].is_null()
        );
    }
    #[test]
    fn source_limits_propagate_but_explicit_overrides_win() {
        let mut source = snapshot();
        let data = &mut source.sources.get_mut(SOURCE_URLS[0]).unwrap().body;
        data["openai"]["models"]["fixture-limit"] =
            json!({"tool_call":true,"limit":{"context":654321,"output":54321}});
        data["openai"]["models"]["gpt-5.4"]["limit"] = json!({"context":987654,"output":123456});
        let result = generate(&source, true).unwrap();
        assert_eq!(
            result.models["openai"]["fixture-limit"]["contextWindow"],
            654321
        );
        assert_eq!(
            result.provenance["openai"]["fixture-limit"]["contextWindow"]["kind"],
            "source"
        );
        assert_eq!(result.models["openai"]["gpt-5.4"]["contextWindow"], 272000);
        assert_eq!(
            result.models["azure-openai-responses"]["gpt-5.4"]["contextWindow"],
            1050000
        );
        assert_eq!(
            result.provenance["openai"]["gpt-5.4"]["contextWindow"]["kind"],
            "override"
        );
    }
    #[test]
    fn strict_allowlist_and_malformed_sources_fail_explicitly() {
        let mut source = snapshot();
        source.sources.get_mut(SOURCE_URLS[0]).unwrap().body["alibaba-token-plan"]["models"]
            .as_object_mut()
            .unwrap()
            .remove("glm-5.2");
        assert!(generate(&source, true).is_err());
        assert!(generate(&source, false).is_ok());
        for url in SOURCE_URLS {
            let mut source = snapshot();
            source.sources.get_mut(url).unwrap().status = 503;
            assert!(generate(&source, false).is_err());
            let mut source = snapshot();
            source.sources.get_mut(url).unwrap().body = json!({});
            assert!(generate(&source, false).is_err());
        }
    }
    #[test]
    fn source_filters_aliases_and_endpoint_routing_are_dynamic() {
        let mut source = snapshot();
        let data = &mut source.sources.get_mut(SOURCE_URLS[0]).unwrap().body;
        let model =
            json!({"tool_call":true,"name":"Fixture","limit":{"context":65536,"output":1024}});
        data["cloudflare-ai-gateway"]["models"]["openai/fixture-new"] = model.clone();
        data["cloudflare-ai-gateway"]["models"]["anthropic/fixture-new"] = model.clone();
        data["openai"]["models"]["fixture-no-tools"] = json!({"tool_call":false});
        data["google"]["models"]["gemini-3.5-flash"]["limit"]["context"] = json!(123456);
        data["together"]["models"]["fixture-deprecated"] =
            json!({"tool_call":true,"status":"deprecated"});
        data["xai"]["models"]["grok-3"] = model;
        let result = generate(&source, true).unwrap();
        // Pi Object.entries uses source insertion order: openai was inserted first.
        // This must not depend on features enabled by another workspace crate.
        assert_eq!(
            result.models["cloudflare-ai-gateway"]["fixture-new"]["api"],
            "openai-responses"
        );
        assert_eq!(
            result.models["google"]["gemini-flash-latest"]["contextWindow"],
            123456
        );
        assert!(!result.models["openai"].contains_key("fixture-no-tools"));
        assert!(
            result
                .models
                .get("together")
                .is_none_or(|m| !m.contains_key("fixture-deprecated"))
        );
        assert!(!result.models["xai"].contains_key("grok-3"));
    }
    #[test]
    fn effort_helper_omits_unverified_values_and_maps_known_values() {
        assert_eq!(
            compatibility::effort_map(
                &json!([{"type":"toggle"},{"type":"effort","values":[null,"default"]}])
            ),
            None
        );
        let map =
            compatibility::effort_map(&json!([{"type":"effort","values":["none","low","max"]}]))
                .unwrap();
        assert_eq!(
            map,
            json!({"off":"none","minimal":null,"low":"low","medium":null,"high":null,"xhigh":null,"max":"max"})
        );
    }
}

#[cfg(test)]
mod validation_tests {
    use super::*;
    #[test]
    fn malformed_source_cost_is_rejected_instead_of_coerced_to_zero() {
        for invalid in [json!(true), json!("bad-price"), json!([1])] {
            let mut snapshot: SourceSnapshot =
                serde_json::from_str(include_str!("../../tests/fixtures/catalog/sources.json"))
                    .unwrap();
            snapshot.sources.get_mut(SOURCE_URLS[0]).unwrap().body["openai"]["models"]["fixture-invalid-price"] =
                json!({"tool_call":true,"cost":{"input":invalid}});
            assert!(generate(&snapshot, true).is_err());
        }
    }
}

#[cfg(test)]
mod dynamic_pricing_regressions {
    use super::*;
    fn pinned() -> SourceSnapshot {
        serde_json::from_str(include_str!("../../tests/fixtures/catalog/sources.json")).unwrap()
    }
    #[test]
    fn full_pinned_generation_accepts_documented_auto_unknown_at_both_strictness_levels() {
        let strict_output = generate(&pinned(), true).unwrap();
        for strict in [true, false] {
            let output = generate(&pinned(), strict).unwrap();
            assert_eq!(output.models, strict_output.models);
            assert_eq!(output.provenance, strict_output.provenance);
            output.validate().unwrap();
            for id in ["openrouter/auto", "openrouter/auto-beta"] {
                let m = &output.models["openrouter"][id];
                assert!(m["cost"]["input"].is_null());
                assert!(m["cost"]["output"].is_null());
                assert_eq!(m["pricingSource"]["raw"]["prompt"], "-1");
                assert_eq!(m["pricingSource"]["raw"]["completion"], "-1");
                assert_eq!(
                    m["pricingSource"]["unknownRateContract"]["reason"],
                    "dynamic_selected_model_tariff"
                );
                assert_eq!(m["api"], "openai-completions");
            }
        }
    }
    #[test]
    fn sentinel_exception_never_allows_other_negative_or_malformed_rates() {
        for strict in [true, false] {
            for value in [
                json!("-2"),
                json!(-2),
                json!("NaN"),
                json!("inf"),
                json!("1e308"),
                json!(1e308),
                json!("bad"),
                json!(false),
            ] {
                let mut source = pinned();
                let entries = source.sources.get_mut(SOURCE_URLS[1]).unwrap().body["data"]
                    .as_array_mut()
                    .unwrap();
                let auto = entries
                    .iter_mut()
                    .find(|m| m["id"] == "openrouter/auto")
                    .unwrap();
                auto["pricing"]["prompt"] = value;
                assert!(generate(&source, strict).is_err());
            }
            let mut source = pinned();
            let entries = source.sources.get_mut(SOURCE_URLS[1]).unwrap().body["data"]
                .as_array_mut()
                .unwrap();
            let auto = entries
                .iter()
                .find(|m| m["id"] == "openrouter/auto")
                .unwrap()
                .clone();
            let mut unsupported = auto;
            unsupported["id"] = json!("fixture/unknown-sentinel");
            entries.push(unsupported);
            assert!(generate(&source, strict).is_err());
        }
    }
    #[test]
    fn unsupported_source_and_cache_sentinels_are_still_invalid() {
        let mut source = pinned();
        let entries = source.sources.get_mut(SOURCE_URLS[1]).unwrap().body["data"]
            .as_array_mut()
            .unwrap();
        let auto = entries
            .iter_mut()
            .find(|m| m["id"] == "openrouter/auto")
            .unwrap();
        auto["pricing"]["input_cache_read"] = json!("-1");
        assert!(generate(&source, false).is_err());
        let mut source = pinned();
        let entries = source.sources.get_mut(SOURCE_URLS[2]).unwrap().body["data"]
            .as_array_mut()
            .unwrap();
        let mut model = entries
            .iter()
            .find(|m| {
                m["tags"]
                    .as_array()
                    .is_some_and(|a| a.iter().any(|tag| tag == "tool-use"))
            })
            .unwrap()
            .clone();
        model["id"] = json!("fixture/negative-vercel");
        model["pricing"]["input"] = json!("-1");
        entries.push(model);
        assert!(generate(&source, false).is_err());
    }
    #[test]
    fn explicit_free_zero_preserves_entire_nonpricing_contract() {
        let source = pinned();
        let baseline = generate(&source, true).unwrap();
        let mut free = source;
        let auto = free.sources.get_mut(SOURCE_URLS[1]).unwrap().body["data"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|m| m["id"] == "openrouter/auto")
            .unwrap();
        auto["pricing"]["prompt"] = json!("0");
        auto["pricing"]["completion"] = json!("0");
        let output = generate(&free, true).unwrap();
        assert_eq!(
            output.models["openrouter"]["openrouter/auto"]["cost"]["input"],
            0.
        );
        let mut before = baseline.models["openrouter"]["openrouter/auto"].clone();
        let mut after = output.models["openrouter"]["openrouter/auto"].clone();
        for field in ["cost", "pricingSource"] {
            before.as_object_mut().unwrap().remove(field);
            after.as_object_mut().unwrap().remove(field);
        }
        assert_eq!(before, after);
        assert_eq!(baseline.provenance, output.provenance);
    }
}

#[cfg(test)]
mod media_propagation_tests {
    use super::*;
    #[test]
    fn source_modalities_and_constraints_survive_all_dynamic_transform_paths() {
        let mut snapshot: SourceSnapshot =
            serde_json::from_str(include_str!("../../tests/fixtures/catalog/sources.json"))
                .unwrap();
        let input = json!(["text", "image", "audio", "video", "pdf"]);
        let source = json!({"name":"media fixture","tool_call":true,"modalities":{"input":input,"output":["text","audio"]},"inputConstraints":{"audio":{"mimeTypes":["audio/wav"],"maxBytes":4096}},"limit":{"context":64000,"output":4000}});
        for provider in ["openai", "google", "groq", "venice"] {
            snapshot.sources.get_mut(SOURCE_URLS[0]).unwrap().body[provider]["models"]["fixture-media"] =
                source.clone();
        }
        // Source identity may use an existing alias or declare the same
        // registered endpoint. Neither path requires a manual model registry.
        let data = &mut snapshot.sources.get_mut(SOURCE_URLS[0]).unwrap().body;
        data["volcengine"]["models"]["fixture-media"] = source.clone();
        data["novita-ai"]["models"]["fixture-media"] = source.clone();
        data["novita-ai"]["api"] = json!(
            crate::definition::provider_definition("novita")
                .unwrap()
                .default_base_url
                .unwrap()
        );
        snapshot.sources.get_mut(SOURCE_URLS[0]).unwrap().body["groq"]["models"]["fixture-unknown"] =
            json!({"tool_call":true});
        snapshot.sources.get_mut(SOURCE_URLS[1]).unwrap().body["data"].as_array_mut().unwrap().push(json!({"id":"fixture/media","name":"media fixture","supported_parameters":["tools"],"architecture":{"input_modalities":input,"output_modalities":["text","audio"]}}));
        snapshot.sources.get_mut(SOURCE_URLS[2]).unwrap().body["data"].as_array_mut().unwrap().push(json!({"id":"fixture/media","tags":["tool-use"],"input_modalities":input,"output_modalities":["text","audio"]}));
        let generated = generate(&snapshot, true).unwrap();
        generated.validate().unwrap();
        for (provider, id) in [
            ("openai", "fixture-media"),
            ("google", "fixture-media"),
            ("groq", "fixture-media"),
            ("venice", "fixture-media"),
            ("novita", "fixture-media"),
            ("doubao", "fixture-media"),
            ("openrouter", "fixture/media"),
            ("vercel-ai-gateway", "fixture/media"),
        ] {
            let model = &generated.models[provider][id];
            assert_eq!(model["input"], input, "{provider}");
            assert_eq!(model["output"], json!(["text", "audio"]), "{provider}");
            assert_eq!(model["inputOrigin"]["kind"], "source");
        }
        assert_eq!(
            generated.models["groq"]["fixture-unknown"]["input"],
            json!([])
        );
        assert_eq!(
            generated.models["groq"]["fixture-unknown"]["inputOrigin"]["kind"],
            "fallback"
        );
        assert_eq!(
            generated.models["venice"]["fixture-media"]["inputConstraints"],
            source["inputConstraints"]
        );
        // Source limits retain their provenance; media support never fabricates
        // a measured token count or a provider billing limit.
        assert_eq!(
            generated.provenance["venice"]["fixture-media"]["contextWindow"]["kind"],
            "source"
        );
    }
}

#[cfg(test)]
mod partial_source_tests {
    use super::*;
    #[test]
    fn positive_vercel_vision_tag_is_partial_while_explicit_input_array_is_complete() {
        let mut snapshot: SourceSnapshot =
            serde_json::from_str(include_str!("../../tests/fixtures/catalog/sources.json"))
                .unwrap();
        snapshot.sources.get_mut(SOURCE_URLS[2]).unwrap().body["data"]
            .as_array_mut()
            .unwrap()
            .push(json!({"id":"fixture/vision-tag","tags":["tool-use","vision"]}));
        snapshot.sources.get_mut(SOURCE_URLS[2]).unwrap().body["data"].as_array_mut().unwrap().push(json!({"id":"fixture/vision-array","tags":["tool-use","vision"],"input_modalities":["text","image"]}));
        let generated = generate(&snapshot, true).unwrap();
        let catalog = ModelCatalog::parse(
            &serde_json::to_string(&generated.models).unwrap(),
            &serde_json::to_string(&generated.provenance).unwrap(),
        )
        .unwrap();
        use super::super::InputCapabilityState;
        use crate::InputContentType;
        let partial = catalog
            .model("vercel-ai-gateway", "fixture/vision-tag")
            .unwrap();
        assert_eq!(
            partial.input_capability(InputContentType::Image),
            InputCapabilityState::Supported
        );
        for kind in [
            InputContentType::Audio,
            InputContentType::Video,
            InputContentType::File,
        ] {
            assert_eq!(
                partial.input_capability(kind),
                InputCapabilityState::Unknown
            );
        }
        assert_eq!(
            catalog
                .model("vercel-ai-gateway", "fixture/vision-array")
                .unwrap()
                .input_capability(InputContentType::Audio),
            InputCapabilityState::Unsupported
        );
    }
}

#[cfg(test)]
mod specialized_zero_regressions {
    use super::*;
    #[test]
    fn native_openrouter_route_owns_filters_and_conflicting_model_metadata() {
        for strict in [false, true] {
            let mut s: SourceSnapshot =
                serde_json::from_str(include_str!("../../tests/fixtures/catalog/sources.json"))
                    .unwrap();
            let conflicting = json!({"tool_call":true,"modalities":{"input":["image"]},"limit":{"context":999,"output":999}});
            s.sources.get_mut(SOURCE_URLS[0]).unwrap().body["openrouter"]["models"]["fixture/native-negative"] =
                conflicting.clone();
            s.sources.get_mut(SOURCE_URLS[0]).unwrap().body["openrouter"]["models"]["fixture/native-positive"] =
                conflicting;
            s.sources.get_mut(SOURCE_URLS[1]).unwrap().body = json!({"data":[
                {"id":"fixture/native-negative","name":"Native negative","supported_parameters":[],"architecture":{"input_modalities":["text"],"output_modalities":["text"]}},
                {"id":"fixture/native-positive","name":"Native positive","supported_parameters":["tools"],"architecture":{"input_modalities":["text"],"output_modalities":["text"]},"context_length":64000,"top_provider":{"max_completion_tokens":4000}}
            ]});
            let generated = generate(&s, strict).unwrap();
            assert!(
                generated.models["openrouter"]
                    .get("fixture/native-negative")
                    .is_none()
            );
            let native = &generated.models["openrouter"]["fixture/native-positive"];
            assert_eq!(native["input"], json!(["text"]));
            assert_eq!(native["contextWindow"], 64000);
            assert_eq!(native["maxTokens"], 4000);
            s.sources.get_mut(SOURCE_URLS[1]).unwrap().body["data"]
                .as_array_mut()
                .unwrap()
                .pop();
            let zero = generate(&s, strict).unwrap();
            assert!(zero.models.get("openrouter").is_none_or(|models| {
                models.get("fixture/native-negative").is_none()
                    && models.get("fixture/native-positive").is_none()
            }));
        }
    }

    #[test]
    fn registered_supplements_preserve_existing_explicit_priority_and_refresh_new_ids() {
        let mut s: SourceSnapshot =
            serde_json::from_str(include_str!("../../tests/fixtures/catalog/sources.json"))
                .unwrap();
        let changed = json!({"tool_call":true,"name":"dynamic","modalities":{"input":["text"]},"cost":{"input":99,"output":99},"limit":{"context":64000,"output":4000}});
        s.sources.get_mut(SOURCE_URLS[0]).unwrap().body["deepseek"]["models"]["deepseek-v4-flash"] =
            changed.clone();
        s.sources.get_mut(SOURCE_URLS[0]).unwrap().body["anthropic"]["models"]["claude-opus-4-6"] =
            changed.clone();
        s.sources.get_mut(SOURCE_URLS[0]).unwrap().body["deepseek"]["models"]["fixture-dynamic"] =
            changed;
        let generated = generate(&s, true).unwrap();
        let known = &generated.models["deepseek"]["deepseek-v4-flash"];
        // DeepSeek's existing IDs, like new IDs, are source-owned. Do not
        // reintroduce stale pinned limits/prices to exercise override priority.
        assert_eq!(known["contextWindow"], 64000);
        assert_eq!(known["maxTokens"], 4000);
        // The source supplies input/output rates only; missing cache rates
        // remain unknown rather than being interpreted as free caching.
        assert_eq!(
            known["cost"],
            json!({"input":99,"output":99,"cacheRead":null,"cacheWrite":null})
        );
        assert_eq!(
            generated.provenance["deepseek"]["deepseek-v4-flash"]["contextWindow"]["kind"],
            "source"
        );
        // A genuine explicit context override retains priority, while the
        // independently source-owned output limit continues to refresh.
        assert_eq!(
            generated.models["anthropic"]["claude-opus-4-6"]["contextWindow"],
            1_000_000
        );
        assert_eq!(
            generated.models["anthropic"]["claude-opus-4-6"]["maxTokens"],
            4000
        );
        assert_eq!(
            generated.provenance["anthropic"]["claude-opus-4-6"]["contextWindow"]["kind"],
            "override"
        );
        let dynamic = &generated.models["deepseek"]["fixture-dynamic"];
        assert_eq!(dynamic["contextWindow"], 64000);
        assert_eq!(dynamic["maxTokens"], 4000);
        assert_eq!(dynamic["cost"]["input"].as_f64(), Some(99.0));
        assert_eq!(
            generated.provenance["deepseek"]["fixture-dynamic"]["contextWindow"]["kind"],
            "source"
        );
        s.sources.get_mut(SOURCE_URLS[0]).unwrap().body["deepseek"]["models"]["fixture-dynamic"]
            ["limit"]["context"] = json!(96000);
        assert_eq!(
            generate(&s, true).unwrap().models["deepseek"]["fixture-dynamic"]["contextWindow"],
            96000
        );
    }

    #[test]
    fn nvidia_zero_eligible_never_refills_from_generic_source() {
        for strict in [false, true] {
            for (source_id, live_id, input, output, accepted) in [
                (
                    "stale/id",
                    "different/live",
                    json!(["text"]),
                    json!(["text"]),
                    false,
                ),
                (
                    "google/gemma-2-2b-it",
                    "google/gemma-2-2b-it",
                    json!(["text"]),
                    json!(["text"]),
                    false,
                ),
                (
                    "native/id",
                    "native/id",
                    json!(["image"]),
                    json!(["text"]),
                    false,
                ),
                (
                    "native/id",
                    "native/id",
                    json!(["text"]),
                    json!(["image"]),
                    false,
                ),
                (
                    "Native_ID",
                    "native.id",
                    json!(["text", "image"]),
                    json!(["text"]),
                    true,
                ),
            ] {
                let mut s: SourceSnapshot =
                    serde_json::from_str(include_str!("../../tests/fixtures/catalog/sources.json"))
                        .unwrap();
                s.sources.get_mut(SOURCE_URLS[0]).unwrap().body["nvidia"]["models"] = json!({source_id:{"name":"native","tool_call":true,"modalities":{"input":input,"output":output},"limit":{"context":4096,"output":1024},"cost":{"input":0,"output":0}}});
                s.sources.get_mut(SOURCE_URLS[3]).unwrap().body = json!({"data":[{"id":live_id}]});
                s.validate().unwrap();
                let generated = generate(&s, strict).unwrap();
                let models = generated.models.get("nvidia");
                assert_eq!(
                    models.is_some_and(|m| !m.is_empty()),
                    accepted,
                    "{source_id}, {strict}"
                );
                if accepted {
                    let row = &generated.models["nvidia"][live_id];
                    assert_eq!(row["id"], live_id);
                    assert_eq!(row["headers"]["NVCF-POLL-SECONDS"], "3600");
                    assert_eq!(row["compat"]["supportsDeveloperRole"], false);
                    assert_eq!(row["compat"]["supportsStore"], false);
                }
            }
        }
    }
}
