//! Historical text tariff resolution. Decimal strings are authoritative; the
//! legacy numeric amount is only a compatibility projection.
use super::*;

const RATE_SCALE: i128 = 1_000_000_000_000;
const AMOUNT_SCALE: i128 = RATE_SCALE * 1_000_000;

// Rates are USD/million tokens, with twelve decimal places. Refuse excess
// precision/overflow rather than round an intermediate financial amount.
fn decimal(value: &Value) -> Option<i128> {
    let text = match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => return None,
    };
    let (mantissa, exponent) = text.split_once(['e', 'E']).unwrap_or((&text, "0"));
    let exponent: i32 = exponent.parse().ok()?;
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let digits: i128 = format!("{whole}{fraction}").parse().ok()?;
    let power = 12i32
        .checked_add(exponent)?
        .checked_sub(fraction.len().try_into().ok()?)?;
    if power >= 0 {
        digits.checked_mul(10i128.checked_pow(power.try_into().ok()?)?)
    } else {
        let divisor = 10i128.checked_pow(power.checked_neg()?.try_into().ok()?)?;
        (digits % divisor == 0).then_some(digits / divisor)
    }
}
fn amount(value: i128) -> String {
    let fraction = format!("{:018}", value % AMOUNT_SCALE);
    let fraction = fraction.trim_end_matches('0');
    if fraction.is_empty() {
        (value / AMOUNT_SCALE).to_string()
    } else {
        format!("{}.{fraction}", value / AMOUNT_SCALE)
    }
}
fn rate(rates: &Value, camel: &str, snake: &str) -> Result<Option<i128>, &'static str> {
    let a = rates.get(camel).filter(|v| !v.is_null());
    let b = rates.get(snake).filter(|v| !v.is_null());
    let parse = |v| decimal(v).ok_or("invalid_rate_precision_or_value");
    let a = a.map(parse).transpose()?;
    let b = b.map(parse).transpose()?;
    if a.is_some() && b.is_some() && a != b {
        return Err("conflicting_rate_aliases");
    }
    Ok(a.or(b))
}

fn selected(cost: &Value, input: Option<u64>) -> Result<(Value, Option<u64>), &'static str> {
    let object = cost.as_object().ok_or("unsupported_rate_shape")?;
    if object.keys().any(|k| {
        !matches!(
            k.as_str(),
            "input"
                | "output"
                | "cacheRead"
                | "cacheWrite"
                | "cache_read"
                | "cache_write"
                | "tiers"
                | "context_over_200k"
        )
    }) {
        return Err("unsupported_rate_condition");
    }
    for (a, b) in [
        ("input", "input"),
        ("output", "output"),
        ("cacheRead", "cache_read"),
        ("cacheWrite", "cache_write"),
    ] {
        rate(cost, a, b)?;
    }
    let mut selected = cost.clone();
    let mut threshold = None;
    let mut seen = std::collections::BTreeSet::new();
    if let Some(tiers) = cost.get("tiers") {
        for tier in tiers.as_array().ok_or("unsupported_tier_shape")? {
            let input = input.ok_or("unknown_tier_input")?;
            let object = tier.as_object().ok_or("unsupported_tier_shape")?;
            if object.keys().any(|k| {
                !matches!(
                    k.as_str(),
                    "tier"
                        | "inputTokensAbove"
                        | "input"
                        | "output"
                        | "cacheRead"
                        | "cacheWrite"
                        | "cache_read"
                        | "cache_write"
                )
            }) {
                return Err("unsupported_tier_condition");
            }
            let structured = if let Some(condition) = tier.get("tier") {
                let condition = condition.as_object().ok_or("unsupported_tier_condition")?;
                if condition.len() != 2
                    || condition.get("type").and_then(Value::as_str) != Some("context")
                {
                    return Err("unsupported_tier_condition");
                }
                Some(
                    condition
                        .get("size")
                        .and_then(Value::as_u64)
                        .ok_or("unsupported_tier_condition")?,
                )
            } else {
                None
            };
            let legacy = tier
                .get("inputTokensAbove")
                .map(|v| v.as_u64().ok_or("unsupported_tier_condition"))
                .transpose()?;
            if structured.is_some() && legacy.is_some() && structured != legacy {
                return Err("conflicting_tier_thresholds");
            }
            let above = structured.or(legacy).ok_or("unsupported_tier_condition")?;
            if !seen.insert(above) {
                return Err("duplicate_tier_threshold");
            }
            // Validate aliases even for an unselected tier.
            for (a, b) in [
                ("input", "input"),
                ("output", "output"),
                ("cacheRead", "cache_read"),
                ("cacheWrite", "cache_write"),
            ] {
                rate(tier, a, b)?;
            }
            if input > above && threshold.is_none_or(|t| above > t) {
                selected = tier.clone();
                threshold = Some(above);
            }
        }
    }
    // This legacy name is not evidence of a threshold. Only accept it as a
    // redundant copy of one structured tier, never select from its name.
    if let Some(alias) = cost.get("context_over_200k") {
        let agrees = cost["tiers"].as_array().is_some_and(|tiers| {
            tiers.iter().any(|tier| {
                [
                    ("input", "input"),
                    ("output", "output"),
                    ("cacheRead", "cache_read"),
                    ("cacheWrite", "cache_write"),
                ]
                .into_iter()
                .all(|(a, b)| matches!((rate(alias,a,b),rate(tier,a,b)), (Ok(x),Ok(y)) if x == y))
            })
        });
        if !agrees {
            return Err("unproven_legacy_tier");
        }
    }
    Ok((selected, threshold))
}

pub(super) fn resolve(usage: &TokenUsage, cost: &Value) -> Value {
    let (rates, threshold) = match selected(cost, usage.input_tokens) {
        Ok(v) => v,
        Err(reason) => {
            return json!({"version":1,"status":"unresolved","reasons":[reason],"lines":[]});
        }
    };
    let split = usage.raw_usage.as_ref().and_then(|raw| {
        raw["cache_creation"]["ephemeral_5m_input_tokens"]
            .as_u64()?
            .checked_add(raw["cache_creation"]["ephemeral_1h_input_tokens"].as_u64()?)
    });
    let inconsistent = split
        .zip(usage.cache_write_input_tokens)
        .is_some_and(|(a, b)| a != b)
        || usage
            .input_tokens
            .zip(usage.cache_read_input_tokens)
            .is_some_and(|(i, r)| r > i)
        || usage
            .input_tokens
            .zip(usage.cache_write_input_tokens)
            .is_some_and(|(i, w)| w > i)
        || usage
            .output_tokens
            .zip(usage.reasoning_tokens)
            .is_some_and(|(o, r)| r > o)
        || usage
            .input_tokens
            .zip(
                usage
                    .cache_read_input_tokens
                    .zip(usage.cache_write_input_tokens),
            )
            .is_some_and(|(i, (r, w))| r.checked_add(w).is_none_or(|v| v > i));
    if inconsistent || super::inconsistent_quantities(usage) {
        return json!({"version":1,"status":"unresolved","reasons":["inconsistent_usage"],"lines":[]});
    }
    let derived_uncached = usage.input_tokens.and_then(|i| {
        i.checked_sub(usage.cache_read_input_tokens?)?
            .checked_sub(usage.cache_write_input_tokens?)
    });
    if derived_uncached
        .zip(usage.uncached_input_tokens)
        .is_some_and(|(a, b)| a != b)
    {
        return json!({"version":1,"status":"unresolved","reasons":["inconsistent_usage"],"lines":[]});
    }
    let uncached = derived_uncached.or(usage.uncached_input_tokens);
    let mut lines = Vec::new();
    let mut reasons = Vec::new();
    let mut subtotal = 0i128;
    for (meter, alias, quantity) in [
        ("input", "input", uncached),
        ("cacheRead", "cache_read", usage.cache_read_input_tokens),
        ("cacheWrite", "cache_write", usage.cache_write_input_tokens),
        ("output", "output", usage.output_tokens),
    ] {
        let Some(quantity) = quantity else {
            reasons.push(json!({"meter":meter,"reason":"unknown_usage"}));
            continue;
        };
        let rate = match rate(&rates, meter, alias) {
            Ok(Some(rate)) => rate,
            Ok(None) if quantity == 0 => {
                // Zero observed quantity needs no rate, but does not prove a zero tariff.
                lines.push(json!({"meter":meter,"quantity":0,"rate":null,"amount_decimal":"0","basis":"zero_quantity"}));
                continue;
            }
            Ok(None) => {
                reasons.push(json!({"meter":meter,"reason":"unknown_rate"}));
                continue;
            }
            Err(reason) => {
                reasons.push(json!({"meter":meter,"reason":reason}));
                continue;
            }
        };
        let Some(component) = rate.checked_mul(i128::from(quantity)) else {
            reasons.push(json!({"meter":meter,"reason":"arithmetic_overflow"}));
            continue;
        };
        let Some(total) = subtotal.checked_add(component) else {
            reasons.push(json!({"meter":meter,"reason":"arithmetic_overflow"}));
            continue;
        };
        subtotal = total;
        lines.push(json!({"meter":meter,"quantity":quantity,"unit":"tokens",
            "rate_scaled_1e12":rate.to_string(),"rate_unit":"USD_per_million_tokens",
            "amount_decimal":amount(component),"currency":"USD","basis":"catalog_estimate"}));
    }
    json!({"version":1,"status":if reasons.is_empty(){"complete"}else if lines.is_empty(){"unresolved"}else{"partial"},
        "currency":"USD","known_subtotal_decimal":(!lines.is_empty()).then(||amount(subtotal)),"lines":lines,
        "reasons":reasons,"input_tokens_above":threshold,"threshold_operator":"gt",
        "precision":"checked_decimal_18_USD"})
}

/// Add retained decimal amounts without currency conversion or float summation.
/// Callers must group by currency AND price basis before invoking this helper.
pub fn add_amounts(a: &str, b: &str) -> Option<String> {
    fn parse(text: &str) -> Option<i128> {
        let (mantissa, exponent) = text.split_once(['e', 'E']).unwrap_or((text, "0"));
        let exponent: i32 = exponent.parse().ok()?;
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        if whole.is_empty()
            || !whole
                .bytes()
                .chain(fraction.bytes())
                .all(|b| b.is_ascii_digit())
        {
            return None;
        }
        let digits: i128 = format!("{whole}{fraction}").parse().ok()?;
        let power = 18i32
            .checked_add(exponent)?
            .checked_sub(fraction.len().try_into().ok()?)?;
        if power >= 0 {
            digits.checked_mul(10i128.checked_pow(power.try_into().ok()?)?)
        } else {
            let divisor = 10i128.checked_pow(power.checked_neg()?.try_into().ok()?)?;
            (digits % divisor == 0).then_some(digits / divisor)
        }
    }

    Some(amount(parse(a)?.checked_add(parse(b)?)?))
}

fn per_token_to_million(value: &Value) -> Option<String> {
    let text = match value {
        Value::String(v) => v.clone(),
        Value::Number(v) => v.to_string(),
        _ => return None,
    };
    let (mantissa, exponent) = text.split_once(['e', 'E']).unwrap_or((&text, "0"));
    let exponent = exponent.parse::<i32>().ok()?.checked_add(6)?;
    let scaled = decimal(&Value::String(format!("{mantissa}e{exponent}")))?;
    Some(amount(scaled.checked_mul(1_000_000)?))
}
/// Vercel's existing Models response prices input/output in USD/token. Unknown
/// tiers/other units remain guarded; picker normalization is unchanged.
pub(super) fn vercel_rates(raw: &Value) -> Result<Value, &'static str> {
    let mut result = json!({});
    for (key, meter) in [
        ("input", "input"),
        ("output", "output"),
        ("input_cache_read", "cacheRead"),
        ("input_cache_write", "cacheWrite"),
    ] {
        if let Some(value) = raw.get(key).filter(|v| !v.is_null()) {
            result[meter] = per_token_to_million(value)
                .ok_or("invalid_source_rate")?
                .into();
        }
    }
    Ok(result)
}

/// OpenRouter's retained Models contract: thresholds are strict; matching
/// overrides apply in array order per key and inherit unspecified base keys.
/// https://github.com/OpenRouterTeam/docs/blob/main/guides/overview/models.mdx
pub(super) fn openrouter_rates(raw: &Value, input: Option<u64>) -> Result<Value, &'static str> {
    fn assign(target: &mut Value, source: &Value) -> Result<(), &'static str> {
        for (wire, meter) in [
            ("prompt", "input"),
            ("completion", "output"),
            ("input_cache_read", "cacheRead"),
            ("input_cache_write", "cacheWrite"),
        ] {
            if let Some(value) = source.get(wire).filter(|v| !v.is_null()) {
                target[meter] = per_token_to_million(value)
                    .ok_or("invalid_source_rate")?
                    .into();
            }
        }
        Ok(())
    }
    let mut result = json!({});
    assign(&mut result, raw)?;
    if let Some(overrides) = raw.get("overrides") {
        let overrides = overrides.as_array().ok_or("unsupported_override_shape")?;
        let input = input.ok_or("unknown_tier_input")?;
        for entry in overrides {
            let object = entry.as_object().ok_or("unsupported_override_shape")?;
            if object.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "min_prompt_tokens"
                        | "prompt"
                        | "completion"
                        | "input_cache_read"
                        | "input_cache_write"
                )
            }) {
                return Err("unsupported_override_condition");
            }
            let threshold = entry["min_prompt_tokens"]
                .as_u64()
                .ok_or("unsupported_override_condition")?;
            if input > threshold {
                assign(&mut result, entry)?;
            }
        }
    }
    Ok(result)
}

/// Preserve the lexical decimal of financial meters from the captured catalog.
/// Structural counts/conditions retain their JSON types for resolver validation.
pub(super) fn lossless_evidence(raw: &str) -> Option<Value> {
    fn decode(raw: &serde_json::value::RawValue, key: &str) -> Option<Value> {
        let body = raw.get().trim();
        if body.starts_with('{') {
            let fields: std::collections::BTreeMap<String, Box<serde_json::value::RawValue>> =
                serde_json::from_str(body).ok()?;
            return Some(Value::Object(
                fields
                    .into_iter()
                    .map(|(k, v)| {
                        let value = decode(&v, &k)?;
                        Some((k, value))
                    })
                    .collect::<Option<_>>()?,
            ));
        }
        if body.starts_with('[') {
            let entries: Vec<Box<serde_json::value::RawValue>> = serde_json::from_str(body).ok()?;
            return Some(Value::Array(
                entries
                    .iter()
                    .map(|v| decode(v, key))
                    .collect::<Option<_>>()?,
            ));
        }
        if matches!(
            key,
            "input"
                | "output"
                | "cacheRead"
                | "cacheWrite"
                | "cache_read"
                | "cache_write"
                | "prompt"
                | "completion"
                | "input_cache_read"
                | "input_cache_write"
                | "request"
                | "image"
                | "web_search"
                | "internal_reasoning"
                | "input_audio_cache"
                | "input_audio"
                | "output_audio"
        ) && body.as_bytes().first().is_some_and(u8::is_ascii_digit)
        {
            Some(Value::String(body.to_owned()))
        } else {
            serde_json::from_str(body).ok()
        }
    }
    let raw: Box<serde_json::value::RawValue> = serde_json::from_str(raw).ok()?;
    decode(&raw, "")
}
