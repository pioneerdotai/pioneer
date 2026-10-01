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
}
impl GeneratedCatalog {
    pub fn validate(&self) -> Result<()> {
        ModelCatalog::parse(
            &serde_json::to_string(&self.models)?,
            &serde_json::to_string(&self.provenance)?,
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
                        model["cost"][field].as_f64().is_some_and(|v| v.is_finite()),
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
    if missing_or_falsy(value) {
        json!(0)
    } else {
        value.clone()
    }
}
fn cost(source: &Value) -> Value {
    json!({"input":source_price(&source["input"]), "output":source_price(&source["output"]),
        "cacheRead":source_price(&source["cache_read"]), "cacheWrite":source_price(&source["cache_write"])})
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
        "cost":cost(&source["cost"]),"contextWindow":context,"maxTokens":output}),
        context_origin,
        output_origin,
        reasoning_options: source["reasoning_options"].clone(),
        input_limit: source["limit"]["input"].as_u64().filter(|limit| *limit > 0),
    }
}

/// Full pinned-reference transformation, including dynamic entries not present
/// in the saved fixture. First source wins identity collisions, as in Pi.
pub fn generate(snapshot: &SourceSnapshot, strict: bool) -> Result<GeneratedCatalog> {
    snapshot.validate()?;
    let mut candidates = sources::models_dev(
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
    for model in &mut candidates {
        compatibility::apply(model);
    }
    compatibility::fallbacks(&mut candidates);
    let mut output = GeneratedCatalog {
        models: BTreeMap::new(),
        provenance: BTreeMap::new(),
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
    fn pinned_non_media_catalog_contract_matches_pi_reference() {
        let generated = generate(&snapshot(), true).unwrap();
        let reference: Value =
            serde_json::from_str(include_str!("../../tests/fixtures/catalog/provenance.json"))
                .unwrap();
        let mut origins = Vec::new();
        for (p, models) in &generated.provenance {
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
        let mut actual = serde_json::to_value(generated.models).unwrap();
        let expected: Value =
            serde_json::from_str(include_str!("../../tests/fixtures/catalog/models.json")).unwrap();
        // Compare the pinned Pi surface, which deliberately had text/image
        // only. Pioneer media supplements and new registered source routes
        // have their own propagation fixtures below.
        let providers = actual.as_object_mut().unwrap();
        providers.retain(|p, _| !expected[p].is_null());
        for (p, models) in providers {
            models
                .as_object_mut()
                .unwrap()
                .retain(|id, _| !expected[p][id].is_null());
            for model in models.as_object_mut().unwrap().values_mut() {
                let image = has(&model["input"], "image");
                model["input"] = if image {
                    json!(["text", "image"])
                } else {
                    json!(["text"])
                };
                for field in [
                    "inputOrigin",
                    "sourceMetadata",
                    "inputConstraints",
                    "output",
                ] {
                    model.as_object_mut().unwrap().remove(field);
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
