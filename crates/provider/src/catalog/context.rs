//! Context-price conditions only. Never changes technical limits or accounting.
use super::CatalogModel;
use pioneer_protocol::ProviderContextTiers;
use serde_json::{Value, json};
use std::collections::BTreeSet;

const RATES: [&str; 4] = ["input", "output", "cacheRead", "cacheWrite"];

fn rate_field<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.get(key).or_else(|| match key {
        "cacheRead" => value.get("cache_read"),
        "cacheWrite" => value.get("cache_write"),
        _ => None,
    })
}

fn rate(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse().ok())
        .filter(|v| v.is_finite() && *v >= 0.0)
}

fn scaled(value: &Value) -> Result<Value, &'static str> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    let value = rate(value).ok_or("invalid Vercel rate")? * 1_000_000.0;
    if !value.is_finite() {
        return Err("overflowing Vercel rate");
    }
    // Match the existing source adapter's six decimal per-million precision.
    let rounded = (value * 1_000_000.0).round() / 1_000_000.0;
    if !rounded.is_finite() {
        return Err("overflowing Vercel rate");
    }
    Ok(json!(rounded))
}

/// Vercel brackets have inclusive min and exclusive max, in USD/token.
/// Missing arrays/rates inherit the base; explicit null stays unknown.
/// Returning an error preserves a malformed known contract rather than erasing it.
pub(super) fn vercel_cost(pricing: &Value, context_window: u64) -> Result<Value, &'static str> {
    let fields = ["input", "output", "input_cache_read", "input_cache_write"];
    let mut cost = json!({});
    let mut starts = BTreeSet::new();
    for (key, source) in RATES.iter().zip(fields) {
        cost[*key] = scaled(&pricing[source])?;
        let array_key = format!("{source}_tiers");
        let Some(value) = pricing.get(&array_key) else {
            continue;
        };
        let brackets = value.as_array().ok_or("invalid Vercel brackets")?;
        let mut end = Some(0);
        for bracket in brackets {
            if bracket.as_object().is_none_or(|object| {
                object
                    .keys()
                    .any(|key| !matches!(key.as_str(), "min" | "max" | "cost"))
            }) {
                return Err("non-context Vercel bracket condition");
            }
            let min = match bracket.get("min") {
                None => 0,
                Some(v) => v.as_u64().ok_or("invalid Vercel min")?,
            };
            let max = match bracket.get("max") {
                None => None,
                Some(v) => Some(v.as_u64().ok_or("invalid Vercel max")?),
            };
            if end != Some(min) || max.is_some_and(|max| max <= min) {
                return Err("unordered or overlapping Vercel brackets");
            }
            if bracket
                .get("cost")
                .is_some_and(|v| !v.is_null() && rate(v).is_none())
            {
                return Err("invalid Vercel rate");
            }
            if min > 0 {
                starts.insert(min);
            } else if let Some(value) = bracket.get("cost") {
                let initial = scaled(value)?;
                if !cost[*key].is_null() && !initial.is_null() && cost[*key] != initial {
                    return Err("conflicting Vercel base rate");
                }
                cost[*key] = initial;
            }
            end = max;
        }
        if !brackets.is_empty() && end.is_some_and(|end| end <= context_window) {
            return Err("incomplete Vercel brackets");
        }
    }
    let mut tiers = Vec::new();
    for start in starts {
        let mut tier = cost.clone();
        tier["inputTokensAbove"] = json!(start - 1);
        for (key, source) in RATES.iter().zip(fields) {
            if let Some(bracket) =
                pricing[format!("{source}_tiers")]
                    .as_array()
                    .and_then(|brackets| {
                        brackets.iter().find(|b| {
                            b["min"].as_u64().unwrap_or(0) <= start
                                && b["max"].as_u64().is_none_or(|end| start < end)
                        })
                    })
            {
                if let Some(value) = bracket.get("cost") {
                    tier[*key] = scaled(value)?;
                }
            }
        }
        tiers.push(tier);
    }
    if !tiers.is_empty() {
        cost["tiers"] = json!(tiers);
    }
    Ok(cost)
}

pub(crate) fn normalize(model: &CatalogModel) -> ProviderContextTiers {
    if matches!(
        model.provider.as_str(),
        "azure" | "azure_openai" | "azure-openai" | "azure-openai-responses"
    ) {
        return ProviderContextTiers::Excluded;
    }
    // Old Vercel snapshots carry only raw brackets. Use the same source adapter,
    // strictly for this provider; raw copied OpenAI evidence grants nothing.
    let vercel;
    let cost = if model.provider == "vercel-ai-gateway" {
        let raw = &model.metadata.get("pricingSource").unwrap_or(&Value::Null)["raw"];
        if raw
            .as_object()
            .is_some_and(|p| p.keys().any(|k| k.ends_with("_tiers")))
        {
            vercel = match vercel_cost(raw, model.context_window) {
                Ok(cost) => cost,
                Err(_) => return ProviderContextTiers::Invalid,
            };
            &vercel
        } else {
            &model.cost
        }
    } else {
        &model.cost
    };
    let Some(value) = cost.get("tiers") else {
        return ProviderContextTiers::NoTiers;
    };
    let Some(tiers) = value.as_array() else {
        return ProviderContextTiers::Invalid;
    };
    if tiers.is_empty() {
        return ProviderContextTiers::NoTiers;
    }
    if RATES
        .iter()
        .any(|key| rate_field(cost, key).is_some_and(|v| !v.is_null() && rate(v).is_none()))
    {
        return ProviderContextTiers::Invalid;
    }
    let mut previous = 0;
    let mut windows = Vec::new();
    let mut known = model.metadata.get("pricingUnits").and_then(Value::as_str)
        == Some("USD_per_million_tokens");
    let base: Vec<_> = RATES
        .iter()
        .map(|key| rate_field(cost, key).and_then(rate))
        .collect();
    let mut last = base.clone();
    known &= base[0].is_some() && base[1].is_some();
    known &= !model
        .metadata
        .get("pricingSource")
        .is_some_and(|source| source["raw"]["varies_by_provider"] == true);
    for tier in tiers {
        if RATES
            .iter()
            .any(|key| rate_field(tier, key).is_some_and(|v| !v.is_null() && rate(v).is_none()))
        {
            return ProviderContextTiers::Invalid;
        }
        let explicit = tier.get("tier");
        if explicit.is_some_and(|t| !t.is_object() || t["type"].as_str().is_none()) {
            return ProviderContextTiers::Invalid;
        }
        if explicit.is_some_and(|t| t["type"] != "context") {
            if tier.get("inputTokensAbove").is_some() {
                return ProviderContextTiers::Invalid;
            }
            continue;
        }
        if tier.as_object().is_none_or(|object| {
            object.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "input"
                        | "output"
                        | "cacheRead"
                        | "cacheWrite"
                        | "cache_read"
                        | "cache_write"
                        | "tier"
                        | "inputTokensAbove"
                )
            })
        }) {
            return ProviderContextTiers::Invalid;
        }
        if explicit.is_some_and(|t| {
            t.as_object()
                .is_some_and(|o| o.keys().any(|k| !matches!(k.as_str(), "type" | "size")))
        }) {
            return ProviderContextTiers::Invalid;
        }
        if explicit.is_some() && explicit.and_then(|t| t["size"].as_u64()).is_none() {
            return ProviderContextTiers::Invalid;
        }
        if tier
            .get("inputTokensAbove")
            .is_some_and(|v| v.as_u64().is_none())
        {
            return ProviderContextTiers::Invalid;
        }
        let above = tier.get("inputTokensAbove").and_then(Value::as_u64);
        let size = explicit.and_then(|t| t["size"].as_u64());
        let Some(boundary) = above.or(size) else {
            return ProviderContextTiers::Invalid;
        };
        if boundary == 0
            || boundary <= previous
            || (above.is_some() && size.is_some() && above != size)
        {
            return ProviderContextTiers::Invalid;
        }
        previous = boundary;
        let next: Vec<_> = RATES
            .iter()
            .enumerate()
            .map(|(i, key)| rate_field(tier, key).map_or(base[i], rate))
            .collect();
        for (old, new) in last.iter().zip(&next) {
            match (old, new) {
                (Some(old), Some(new)) if new < old => return ProviderContextTiers::Invalid,
                (Some(_), None) | (None, Some(_)) => known = false,
                _ => {}
            }
        }
        known &= next[0].is_some() && next[1].is_some();
        last = next;
        if boundary < model.context_window {
            windows.push(boundary);
        }
    }
    if previous == 0 {
        return ProviderContextTiers::NoTiers;
    }
    if model.context_window == 0 {
        return ProviderContextTiers::Invalid;
    }
    windows.push(model.context_window);
    ProviderContextTiers::Tiered {
        windows,
        price_order_confirmed: known,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn model(provider: &str, window: u64, cost: Value) -> CatalogModel {
        CatalogModel {
            id: "model".into(),
            name: "Model".into(),
            provider: provider.into(),
            api: "openai-responses".into(),
            base_url: String::new(),
            context_window: window,
            max_tokens: 16000,
            reasoning: false,
            input: vec!["text".into()],
            cost,
            metadata: [("pricingUnits".into(), json!("USD_per_million_tokens"))].into(),
        }
    }
    #[test]
    fn context_windows_default_to_first_threshold_and_require_valid_explicit_expansion() {
        for (threshold, window, shape) in [
            (
                272000,
                1050000,
                json!({"inputTokensAbove":272000,"input":4,"output":15}),
            ),
            (
                200000,
                1048576,
                json!({"tier":{"type":"context","size":200000},"input":4}),
            ),
            (
                100000,
                1000000,
                json!({"tier":{"type":"context","size":100000},"output":15}),
            ),
        ] {
            let mut m = model(
                "openai",
                window,
                json!({"input":2,"output":10,"tiers":[shape]}),
            );
            m.cost["context_over_200k"] = json!({"input":999});
            let metadata = normalize(&m);
            assert_eq!(metadata.resolve(window, None), Ok(threshold));
            assert_eq!(metadata.resolve(window, Some(window)), Ok(window));
            assert!(metadata.resolve(window, Some(threshold + 1)).is_err());
            m.context_window = threshold;
            assert_eq!(
                normalize(&m),
                ProviderContextTiers::Tiered {
                    windows: vec![threshold],
                    price_order_confirmed: true,
                }
            );
        }
        let metadata = normalize(&model(
            "aihubmix",
            256000,
            json!({"input":1,"output":2,
            "tiers":[{"inputTokensAbove":32000,"input":2}, {"inputTokensAbove":128000,"input":3}]}),
        ));
        assert_eq!(
            metadata,
            ProviderContextTiers::Tiered {
                windows: vec![32000, 128000, 256000],
                price_order_confirmed: true,
            }
        );
    }
    #[test]
    fn invalid_unknown_and_excluded_contracts_cannot_grant_expansion() {
        for tiers in [
            json!(null),
            json!({}),
            json!([{"inputTokensAbove":0}]),
            json!([{"inputTokensAbove":3.5}]),
            json!([{"inputTokensAbove":10,"service_tier":"fast"}]),
            json!([{"inputTokensAbove":10,"tier":{"type":"service","size":10}}]),
            json!([{"inputTokensAbove":10},{"inputTokensAbove":10}]),
            json!([{"inputTokensAbove":20},{"inputTokensAbove":10}]),
            json!([{"inputTokensAbove":10,"input":0.5}]),
            json!([{"inputTokensAbove":10,"tier":{"type":"context","size":11}}]),
        ] {
            let m = model("openai", 100, json!({"input":1,"output":2,"tiers":tiers}));
            assert_eq!(normalize(&m), ProviderContextTiers::Invalid);
            assert!(normalize(&m).resolve(100, None).is_err());
        }
        let mut m = model(
            "openai",
            1000,
            json!({"input":1,"output":2,
            "tiers":[{"inputTokensAbove":100,"input":null}]}),
        );
        let unknown = normalize(&m);
        assert_eq!(unknown.resolve(1000, None), Ok(100));
        assert!(unknown.resolve(1000, Some(1000)).is_err());
        m.provider = "azure-openai-responses".into();
        assert_eq!(normalize(&m), ProviderContextTiers::Excluded);
        m.provider = "other".into();
        m.cost = json!({"input":1,"output":2,"context_over_200k":{"input":3}});
        assert_eq!(normalize(&m), ProviderContextTiers::NoTiers);
        assert_eq!(normalize(&m).resolve(1000, None), Ok(1000));
    }
    #[test]
    fn vercel_merges_independent_brackets_and_preserves_unknowns_without_changing_accounting() {
        let pricing = json!({"input":"0.000002","output":"0.00001", "input_cache_read":"0.0000001",
            "input_tiers":[{"min":0,"max":272001,"cost":"0.000002"},{"min":272001,"cost":"0.000004"}],
            "output_tiers":[{"min":0,"max":500001,"cost":"0.00001"},{"min":500001,"cost":"0.000015"}],
            "input_cache_read_tiers":[{"min":0,"max":272001,"cost":"0.0000001"},{"min":272001}],
            "input_cache_write_tiers":[{"min":0,"max":600001},{"min":600001,"cost":null}]});
        let cost = vercel_cost(&pricing, 1050000).unwrap();
        assert_eq!(cost["tiers"][0]["inputTokensAbove"], 272000);
        assert_eq!(cost["tiers"][1]["inputTokensAbove"], 500000);
        assert_eq!(cost["tiers"][2]["inputTokensAbove"], 600000);
        assert_eq!(cost["tiers"][0]["input"], 4.0);
        assert_eq!(cost["tiers"][0]["output"], 10.0);
        assert_eq!(cost["tiers"][0]["cacheRead"], 0.1);
        assert!(cost["tiers"][2]["cacheWrite"].is_null());
        let mut m = model("vercel-ai-gateway", 1050000, json!({"input":2,"output":10}));
        m.metadata
            .insert("pricingSource".into(), json!({"raw":pricing}));
        let metadata = normalize(&m);
        assert_eq!(metadata.resolve(1050000, None), Ok(272000));
        assert!(m.cost.get("tiers").is_none());
        m.provider = "azure-openai-responses".into();
        assert_eq!(normalize(&m), ProviderContextTiers::Excluded);
        for brackets in [
            json!([{"min":0,"max":10},{"min":9}]),
            json!([{"min":0,"max":10},{"min":11}]),
            json!([{"min":0,"max":1.5}]),
            json!([{"min":0,"cost":"invalid"}]),
            json!([{"min":0,"cost":"0.000002","service_tier":"fast"}]),
        ] {
            assert!(vercel_cost(&json!({"input_tiers":brackets}), 1050000).is_err());
        }
    }
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    #[test]
    fn public_metadata_is_scoped_and_bad_model_does_not_discard_other_models() {
        let models = json!({"openai": {
            "good": {"id":"good","name":"Good","provider":"openai","api":"openai-responses",
                "baseUrl":"https://api.openai.com/v1","contextWindow":1050000,"maxTokens":16000,
                "reasoning":false,"input":["text"],"pricingUnits":"USD_per_million_tokens",
                "cost":{"input":2,"output":10,"tiers":[{"inputTokensAbove":272000,"input":4}]}},
            "bad": {"id":"bad","name":"Bad","provider":"openai","api":"openai-responses",
                "baseUrl":"https://api.openai.com/v1","contextWindow":1050000,"maxTokens":16000,
                "reasoning":false,"input":["text"],"cost":{"tiers":[{"inputTokensAbove":0}]}}
        }});
        let origin = json!({"contextWindow":{"kind":"source","expression":"fixture"},
            "maxTokens":{"kind":"source","expression":"fixture"}});
        let origins = json!({"openai":{"good":origin,"bad":origin}});
        let catalog =
            super::super::ModelCatalog::parse(&models.to_string(), &origins.to_string()).unwrap();
        let mut discovered = [
            crate::catalog::tool_tests::discovered("good", None),
            crate::catalog::tool_tests::discovered("bad", None),
        ];
        catalog.enrich_for_tool_scope("openai", &mut discovered, false);
        assert!(discovered.iter().all(|model| model.context_tiers.is_none()));
        catalog.enrich("openai", &mut discovered);
        assert_eq!(
            discovered[0]
                .context_tiers
                .as_ref()
                .unwrap()
                .resolve(1050000, None),
            Ok(272000)
        );
        assert_eq!(
            discovered[1].context_tiers,
            Some(ProviderContextTiers::Invalid)
        );
        // Publication endpoints, including a cap between thresholds, must be
        // accepted by the original execution metadata with the same real cap.
        let execution = catalog.context_tiers("openai", "good").unwrap();
        for cap in [128000, 272000, 800000, 1050000, 1200000] {
            let mut dto = crate::catalog::tool_tests::discovered("good", None);
            dto.limits.context_window = Some(cap);
            catalog.enrich("openai", std::slice::from_mut(&mut dto));
            let ProviderContextTiers::Tiered { windows, .. } = dto.context_tiers.unwrap() else {
                panic!("expected published tier endpoints");
            };
            assert_eq!(execution.resolve(cap, None), Ok(windows[0]));
            for window in windows {
                assert_eq!(execution.resolve(cap, Some(window)), Ok(window));
            }
        }
        assert_eq!(execution.resolve(800000, Some(800000)), Ok(800000));
        assert!(execution.resolve(800000, Some(700000)).is_err());
        assert!(execution.resolve(800000, Some(1050000)).is_err());
        // A narrower discovery limit hides all unreachable catalog choices.
        discovered[0].limits.context_window = Some(128000);
        catalog.enrich("openai", &mut discovered[..1]);
        assert_eq!(
            discovered[0]
                .context_tiers
                .as_ref()
                .unwrap()
                .resolve(128000, None),
            Ok(128000)
        );
        // Native authority evidence wins over the public model with the same id.
        discovered[0].context_tiers = Some(ProviderContextTiers::NoTiers);
        catalog.enrich("openai", &mut discovered[..1]);
        assert_eq!(
            discovered[0].context_tiers,
            Some(ProviderContextTiers::NoTiers)
        );
    }
}
