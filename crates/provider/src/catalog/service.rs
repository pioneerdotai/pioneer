//! Snapshot-owned source facts. These are not account entitlements or adapter
//! capabilities: admission must intersect them with the actual authority/API.
use super::CatalogModel;
use serde_json::Value;

pub use pioneer_protocol::{
    OpenAiTier, ServiceModeDescriptor, ServiceModeParameters, SourceServiceMode, SourceServiceModes,
};

fn only(value: &Value, keys: &[&str]) -> bool {
    value
        .as_object()
        .is_some_and(|object| object.keys().all(|key| keys.contains(&key.as_str())))
}

fn parameters(model: &CatalogModel, id: &str, provider: &Value) -> Option<ServiceModeParameters> {
    if !only(provider, &["body", "headers"]) {
        return None;
    }
    let body = &provider["body"];
    let no_headers = provider
        .get("headers")
        .is_none_or(|v| v == &serde_json::json!({}));
    match (model.provider.as_str(), model.api.as_str()) {
        ("openai", "openai-responses" | "openai-completions")
        | ("fireworks", "openai-completions")
            if no_headers =>
        {
            if only(body, &["service_tier"]) {
                let tier = match (id, body["service_tier"].as_str()?) {
                    ("fast" | "priority", "fast" | "priority") => OpenAiTier::Priority,
                    ("flex", "flex") if model.provider == "openai" => OpenAiTier::Flex,
                    ("ultrafast", "ultrafast") if model.api == "openai-responses" => {
                        OpenAiTier::Ultrafast
                    }
                    _ => return None,
                };
                return Some(ServiceModeParameters::OpenAiTier { tier });
            }
            if model.reasoning
                && model.provider == "openai"
                && model.api == "openai-responses"
                && id == "pro"
                && body == &serde_json::json!({"reasoning":{"mode":"pro"}})
            {
                return Some(ServiceModeParameters::OpenAiPro);
            }
            None
        }
        ("anthropic", "anthropic-messages")
            if id == "fast" && body == &serde_json::json!({"speed":"fast"}) =>
        {
            let beta = match provider.get("headers") {
                None => None,
                Some(headers) if headers == &serde_json::json!({}) => None,
                Some(headers)
                    if headers == &serde_json::json!({"anthropic-beta":"fast-mode-2026-02-01"}) =>
                {
                    Some("fast-mode-2026-02-01".into())
                }
                _ => return None,
            };
            Some(ServiceModeParameters::AnthropicFast { beta })
        }
        ("amazon-bedrock", "bedrock-converse-stream")
            if no_headers
                && id == "ultrafast"
                && body
                    == &serde_json::json!({"additionalModelRequestFields":{"service_tier":"ultrafast"}}) =>
        {
            Some(ServiceModeParameters::BedrockNestedUltrafast)
        }
        // Azure/deployments and other authorities cannot borrow public modes.
        _ => None,
    }
}

/// Retain only bounded numeric tariff/condition evidence. Arbitrary catalog
/// strings/keys never enter durable usage facts. Unknown price is not zero.
fn cost_evidence(value: &Value) -> Option<Value> {
    fn visit(value: &Value, depth: usize, key: &str) -> bool {
        if depth > 4 {
            return false;
        }
        match value {
            Value::Null => true,
            Value::Number(number) => number.as_f64().is_some_and(|v| v.is_finite() && v >= 0.0),
            Value::String(text) if key == "type" => text == "context",
            Value::String(text) => {
                text.len() <= 64 && text.parse::<f64>().is_ok_and(|v| v.is_finite() && v >= 0.0)
            }
            Value::Array(values) => {
                values.len() <= 32 && values.iter().all(|v| visit(v, depth + 1, key))
            }
            Value::Object(fields) => {
                fields.len() <= 16
                    && fields.iter().all(|(k, v)| {
                        matches!(
                            k.as_str(),
                            "input"
                                | "output"
                                | "cacheRead"
                                | "cacheWrite"
                                | "cache_read"
                                | "cache_write"
                                | "tiers"
                                | "tier"
                                | "type"
                                | "size"
                                | "inputTokensAbove"
                        ) && visit(v, depth + 1, k)
                    })
            }
            _ => false,
        }
    }
    (value.is_object() && visit(value, 0, "") && value.to_string().len() <= 4096)
        .then(|| value.clone())
}

pub(super) fn normalize(model: &CatalogModel) -> SourceServiceModes {
    let Some(source) = model.metadata.get("sourceMetadata") else {
        return SourceServiceModes::Unknown;
    };
    if !source.is_object() {
        return SourceServiceModes::Malformed;
    }
    let Some(experimental) = source.get("experimental") else {
        return SourceServiceModes::Unknown;
    };
    if !experimental.is_object() {
        return SourceServiceModes::Malformed;
    }
    let Some(modes) = experimental.get("modes") else {
        return SourceServiceModes::Unknown;
    };
    let Some(modes) = modes.as_object() else {
        return SourceServiceModes::Malformed;
    };
    SourceServiceModes::Known(
        modes
            .iter()
            .map(|(id, value)| {
                let body = &value["provider"]["body"];
                let malformed = !value.is_object()
                    || !value["provider"].is_object()
                    || !body.is_object()
                    || ["service_tier", "speed"]
                        .iter()
                        .any(|key| body.get(*key).is_some_and(|value| !value.is_string()))
                    || body.get("reasoning").is_some_and(|value| {
                        !value.is_object()
                            || value.get("mode").is_some_and(|mode| !mode.is_string())
                    })
                    || value["provider"]
                        .get("headers")
                        .is_some_and(|headers| !headers.is_object());
                let state = if malformed {
                    SourceServiceMode::Malformed
                } else if !only(value, &["provider", "cost"]) {
                    // An unrecognized limit/parameter can change the preset's
                    // meaning. Never execute just the subset understood here.
                    SourceServiceMode::Unmapped
                } else {
                    match parameters(model, id, &value["provider"]) {
                        Some(parameters) => SourceServiceMode::Parsed {
                            descriptor: ServiceModeDescriptor {
                                id: if model.provider == "openai" && id == "priority" {
                                    "fast"
                                } else {
                                    id.as_str()
                                }
                                .to_owned(),
                                label: match id.as_str() {
                                    "priority" if model.provider == "fireworks" => "Priority",
                                    "priority" | "fast" => "Fast",
                                    "flex" => "Flex",
                                    "ultrafast" => "Ultrafast",
                                    "pro" => "Pro",
                                    _ => unreachable!("parameters rejects unknown mode IDs"),
                                }
                                .into(),
                                parameters,
                                source_provider: model.provider.clone(),
                                source_model: model.id.clone(),
                                source_api: model.api.clone(),
                                source_url: model
                                    .metadata
                                    .get("pricingSource")
                                    .and_then(|s| s["url"].as_str())
                                    .map(str::to_owned),
                                cost: value.get("cost").and_then(cost_evidence),
                            },
                        },
                        None => SourceServiceMode::Unmapped,
                    }
                };
                (id.clone(), state)
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ModelCatalog;
    use serde_json::json;

    fn catalog(provider: &str, api: &str, modes: Option<Value>) -> ModelCatalog {
        let mut model = json!({
            "id":"fixture", "name":"Fixture", "provider":provider,
            "api":api, "baseUrl":"https://example.invalid",
            "contextWindow":32000, "maxTokens":4000, "reasoning":true,
            "input":["text"], "cost":{"input":2,"output":10}
        });
        if let Some(modes) = modes {
            model["sourceMetadata"] = json!({"experimental":{"modes":modes}});
        }
        ModelCatalog::parse(
            &json!({provider:{"fixture":model}}).to_string(),
            &json!({provider:{"fixture":{
                "contextWindow":{"kind":"source","expression":"fixture"},
                "maxTokens":{"kind":"source","expression":"fixture"}
            }}})
            .to_string(),
        )
        .unwrap()
    }

    #[test]
    fn publication_keeps_pro_distinct_and_partial_conditional_prices_verbatim() {
        let price = json!({"input":4,"output":null,"tiers":[{
            "inputTokensAbove":272000,"input":8
        }]});
        let snapshot = catalog(
            "openai",
            "openai-responses",
            Some(json!({
                "pro":{"provider":{"body":{"reasoning":{"mode":"pro"}}}},
                "priority":{"cost":price,"provider":{"body":{"service_tier":"fast"}}},
                "broken":null,
                "bad_type":{"provider":{"body":{"service_tier":123}}},
                "future":{"provider":{"body":{"service_tier":"future"}}},
                "injected":{"provider":{"body":{"service_tier":"priority","model":"other"}}}
            })),
        );
        let Some(SourceServiceModes::Known(modes)) =
            snapshot.source_service_modes("openai", "fixture")
        else {
            panic!("expected published source facts");
        };
        let SourceServiceMode::Parsed { descriptor: pro } = &modes["pro"] else {
            panic!("missing Pro source facts");
        };
        assert_eq!(pro.parameters, ServiceModeParameters::OpenAiPro);
        assert_eq!(pro.id, "pro");
        assert_eq!(pro.cost, None);
        let SourceServiceMode::Parsed { descriptor: fast } = &modes["priority"] else {
            panic!("missing Fast source facts");
        };
        assert_eq!(fast.id, "fast");
        assert_eq!(
            fast.parameters,
            ServiceModeParameters::OpenAiTier {
                tier: OpenAiTier::Priority
            }
        );
        assert_eq!(fast.cost, Some(price));
        assert_eq!(modes["broken"], SourceServiceMode::Malformed);
        assert_eq!(modes["bad_type"], SourceServiceMode::Malformed);
        assert_eq!(modes["future"], SourceServiceMode::Unmapped);
        assert_eq!(modes["injected"], SourceServiceMode::Unmapped);
    }

    #[test]
    fn source_protocol_and_provider_boundaries_do_not_borrow_openai_modes() {
        for (provider, api, parsed) in [
            ("openai", "openai-responses", true),
            ("openai", "openai-completions", false),
            ("azure-openai-responses", "openai-responses", false),
            ("other", "openai-responses", false),
        ] {
            let snapshot = catalog(
                provider,
                api,
                Some(json!({
                    "pro":{"provider":{"body":{"reasoning":{"mode":"pro"}}}},
                    "ultrafast":{"provider":{"body":{"service_tier":"ultrafast"}}}
                })),
            );
            let Some(SourceServiceModes::Known(modes)) =
                snapshot.source_service_modes(provider, "fixture")
            else {
                panic!("expected source map for {provider}/{api}");
            };
            for id in ["pro", "ultrafast"] {
                assert_eq!(
                    matches!(modes[id], SourceServiceMode::Parsed { .. }),
                    parsed,
                    "{provider}/{api}/{id}"
                );
            }
            assert_eq!(snapshot.source_service_modes(provider, "missing"), None);
        }
    }

    #[test]
    fn source_native_shapes_preserve_beta_and_reject_additional_controls() {
        for (provider, api, id, source, expected) in [
            (
                "fireworks",
                "openai-completions",
                "priority",
                json!({"body":{"service_tier":"priority"}}),
                ServiceModeParameters::OpenAiTier {
                    tier: OpenAiTier::Priority,
                },
            ),
            (
                "anthropic",
                "anthropic-messages",
                "fast",
                json!({"body":{"speed":"fast"},"headers":{"anthropic-beta":"fast-mode-2026-02-01"}}),
                ServiceModeParameters::AnthropicFast {
                    beta: Some("fast-mode-2026-02-01".into()),
                },
            ),
            (
                "amazon-bedrock",
                "bedrock-converse-stream",
                "ultrafast",
                json!({"body":{"additionalModelRequestFields":{"service_tier":"ultrafast"}}}),
                ServiceModeParameters::BedrockNestedUltrafast,
            ),
        ] {
            let snapshot = catalog(provider, api, Some(json!({id:{"provider":source}})));
            let Some(SourceServiceModes::Known(modes)) =
                snapshot.source_service_modes(provider, "fixture")
            else {
                panic!("expected source map");
            };
            let SourceServiceMode::Parsed { descriptor } = &modes[id] else {
                panic!("expected parsed native shape");
            };
            assert_eq!(descriptor.parameters, expected);

            for extra in [
                json!({"body":source["body"],"headers":{"Authorization":"never-copy-this"}}),
                json!({"body":{"model":"other"}}),
            ] {
                let snapshot = catalog(provider, api, Some(json!({id:{"provider":extra}})));
                let Some(SourceServiceModes::Known(modes)) =
                    snapshot.source_service_modes(provider, "fixture")
                else {
                    panic!("expected source map");
                };
                assert_eq!(modes[id], SourceServiceMode::Unmapped);
            }
        }
    }

    #[test]
    fn absent_empty_and_malformed_source_maps_are_distinct_across_snapshots() {
        let old = catalog("openai", "openai-responses", None);
        let empty = catalog("openai", "openai-responses", Some(json!({})));
        let malformed = catalog("openai", "openai-responses", Some(json!([])));
        assert_eq!(
            old.source_service_modes("openai", "fixture"),
            Some(&SourceServiceModes::Unknown)
        );
        assert_eq!(
            empty.source_service_modes("openai", "fixture"),
            Some(&SourceServiceModes::Known(std::collections::BTreeMap::new()))
        );
        assert_eq!(
            malformed.source_service_modes("openai", "fixture"),
            Some(&SourceServiceModes::Malformed)
        );
        assert_ne!(old.snapshot_id(), empty.snapshot_id());
        assert_eq!(
            old.source_service_modes("openai", "fixture"),
            Some(&SourceServiceModes::Unknown)
        );
    }
}

/// Intersect source facts with the installed adapter, never just a shared wire enum.
pub(crate) fn for_adapter(
    mut source: SourceServiceModes,
    provider: &str,
    api: &str,
    native_reasoning: Option<bool>,
) -> SourceServiceModes {
    if let SourceServiceModes::Known(modes) = &mut source {
        for state in modes.values_mut() {
            if let SourceServiceMode::Parsed { descriptor } = state {
                let supported = match &descriptor.parameters {
                    ServiceModeParameters::OpenAiPro => {
                        provider == "openai"
                            && api == "responses"
                            && native_reasoning != Some(false)
                    }
                    ServiceModeParameters::OpenAiTier {
                        tier: OpenAiTier::Ultrafast,
                    } => provider == "openai" && api == "responses",
                    ServiceModeParameters::OpenAiTier { .. } => {
                        matches!(provider, "openai" | "fireworks")
                            && matches!(api, "responses" | "chat_completions")
                    }
                    ServiceModeParameters::AnthropicFast { .. } => {
                        provider == "anthropic" && api == "messages"
                    }
                    ServiceModeParameters::BedrockNestedUltrafast => {
                        provider == "bedrock" && api == "bedrock_converse"
                    }
                    _ => false,
                };
                if !supported {
                    *state = SourceServiceMode::Unmapped;
                }
            }
        }
    }
    source
}

pub async fn capture_plan(
    provider: &dyn crate::Provider,
    model: &str,
    choice: Option<&pioneer_protocol::NativeServiceModeSelection>,
) -> anyhow::Result<pioneer_protocol::NativeServiceModePlan> {
    use anyhow::ensure;
    use pioneer_protocol::NativeServiceModePlan;
    let Some(choice) = choice else {
        return Ok(NativeServiceModePlan {
            id: "standard".into(),
            parameters: provider.standard_service_mode(model),
        });
    };
    let expected = provider
        .authority_fingerprint()
        .map(|a| super::context_choice_authority(a, model));
    ensure!(
        expected.as_deref() == Some(choice.authority.as_str()),
        "Service tier belongs to another provider authority/model; reopen the model picker"
    );
    if choice.id == "standard" {
        return Ok(NativeServiceModePlan {
            id: "standard".into(),
            parameters: provider.standard_service_mode(model),
        });
    }
    let Some(SourceServiceModes::Known(modes)) = provider.model_service_modes(model).await? else {
        anyhow::bail!(
            "Service tier metadata is unavailable; reselect Standard or reopen the model picker"
        );
    };
    let descriptor = modes
        .values()
        .find_map(|state| match state {
            SourceServiceMode::Parsed { descriptor } if descriptor.id == choice.id => {
                Some(descriptor)
            }
            _ => None,
        })
        .ok_or_else(|| {
            anyhow::anyhow!("Selected service tier is no longer supported; reselect explicitly")
        })?;
    Ok(NativeServiceModePlan {
        id: descriptor.id.clone(),
        parameters: descriptor.parameters.clone(),
    })
}

/// Revalidate meaning, not current prices. Never reinterpret a saved ID silently.
pub async fn validate_plan(
    provider: &dyn crate::Provider,
    model: &str,
    plan: &pioneer_protocol::NativeServiceModePlan,
) -> anyhow::Result<()> {
    let authority = provider
        .authority_fingerprint()
        .map(|a| super::context_choice_authority(a, model))
        .unwrap_or_default();
    let choice = pioneer_protocol::NativeServiceModeSelection {
        id: plan.id.clone(),
        authority,
    };
    let current = capture_plan(provider, model, (plan.id != "standard").then_some(&choice)).await?;
    anyhow::ensure!(
        &current == plan,
        "Service tier contract changed since admission; explicit reselection is required"
    );
    Ok(())
}
