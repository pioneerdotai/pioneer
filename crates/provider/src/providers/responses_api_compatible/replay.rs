use crate::{ProviderReplayState, continuation::Retention};
use anyhow::{Result, bail, ensure};
use serde_json::{Value, json};

pub(super) const DIALECT: &str = "responses";

pub(crate) fn is_dialect(state: &ProviderReplayState) -> bool {
    state.payload.get("wire_api").and_then(Value::as_str) == Some(DIALECT)
}
pub(super) fn input_items(output: &[Value]) -> Result<Vec<Value>> {
    let mut result = Vec::with_capacity(output.len());
    let mut calls = std::collections::BTreeSet::new();
    for item in output {
        let mut input = serde_json::Map::new();
        match item["type"].as_str() {
            Some("message") => {
                ensure!(
                    item["role"] == "assistant",
                    "unsupported Responses message role"
                );
                validate_phase(item)?;
                let parts = item["content"]
                    .as_array()
                    .ok_or_else(|| anyhow::anyhow!("invalid Responses content"))?;
                let mut content = Vec::new();
                for part in parts {
                    match part["type"].as_str() {
                        Some("output_text") => {
                            let text = part["text"]
                                .as_str()
                                .ok_or_else(|| anyhow::anyhow!("invalid Responses text"))?;
                            let mut output =
                                json!({"type":"output_text","text":text,"annotations":[]});
                            for field in ["annotations", "logprobs"] {
                                if let Some(value) = part.get(field).filter(|v| !v.is_null()) {
                                    ensure!(
                                        value.is_array(),
                                        "invalid Responses output text metadata"
                                    );
                                    output[field] = value.clone();
                                }
                            }
                            content.push(output);
                        }
                        Some("refusal") => {
                            let text = part["refusal"]
                                .as_str()
                                .ok_or_else(|| anyhow::anyhow!("invalid Responses refusal"))?;
                            content.push(json!({"type":"refusal","refusal":text}));
                        }
                        _ => bail!("unsupported Responses content part"),
                    }
                }
                input.insert("type".into(), json!("message"));
                input.insert("role".into(), json!("assistant"));
                input.insert("content".into(), json!(content));
                if let Some(status) = item.get("status").filter(|s| !s.is_null()) {
                    input.insert("status".into(), status.clone());
                }
                if let Some(phase) = item.get("phase") {
                    input.insert("phase".into(), phase.clone());
                }
            }
            Some("function_call") => {
                validate_function_profile(item)?;
                let id = required_string(item, "call_id")?;
                ensure!(calls.insert(id), "duplicate Responses call_id");
                let name = required_string(item, "name")?;
                ensure!(
                    name.len() <= 64
                        && name
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-')),
                    "invalid Responses function name"
                );
                let args = required_string(item, "arguments")?;
                ensure!(
                    valid_arguments(args),
                    "invalid Responses function arguments"
                );
                input.insert("type".into(), json!("function_call"));
                for field in ["call_id", "name", "arguments"] {
                    input.insert(field.into(), item[field].clone());
                }
            }
            Some("reasoning") => {
                required_string(item, "id")?;
                let summary = item["summary"]
                    .as_array()
                    .ok_or_else(|| anyhow::anyhow!("invalid Responses reasoning"))?;
                ensure!(
                    summary
                        .iter()
                        .all(|p| p["type"] == "summary_text" && p["text"].is_string()),
                    "unsupported Responses reasoning summary"
                );
                input.insert("type".into(), json!("reasoning"));
                input.insert("summary".into(), item["summary"].clone());
                if let Some(encrypted) = item.get("encrypted_content") {
                    ensure!(
                        encrypted.is_null() || encrypted.is_string(),
                        "invalid encrypted reasoning shape"
                    );
                    input.insert("encrypted_content".into(), encrypted.clone());
                }
                // Readable reasoning content is an input-supported field too.
                if let Some(content) = item.get("content") {
                    ensure!(
                        content.is_null()
                            || content.as_array().is_some_and(|parts| parts
                                .iter()
                                .all(|p| p["type"] == "reasoning_text" && p["text"].is_string())),
                        "unsupported Responses reasoning content"
                    );
                    input.insert("content".into(), content.clone());
                }
            }
            _ => bail!("unsupported Responses output item"),
        }
        // Replay output messages as output items, including their identities.
        if let Some(id) = item.get("id") {
            ensure!(
                id.as_str().is_some_and(|s| !s.is_empty()),
                "invalid Responses item identity"
            );
            input.insert("id".into(), id.clone());
        }
        result.push(Value::Object(input));
    }
    Ok(result)
}
// Validation does not retain an untrusted decoded arguments tree alongside the
// replay and stream buffers. The existing JSON recursion limit still applies.
fn valid_arguments(args: &str) -> bool {
    use serde::Deserializer;
    use serde::de::{IgnoredAny, MapAccess, Visitor};
    struct Object;
    impl<'de> Visitor<'de> for Object {
        type Value = ();
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("JSON object arguments")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<(), A::Error> {
            while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
            Ok(())
        }
    }
    let mut decoder = serde_json::Deserializer::from_str(args);
    decoder.deserialize_map(Object).is_ok() && decoder.end().is_ok()
}
pub(super) fn required_string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("missing or invalid Responses identity/field"))
}
pub(super) fn validate_function_profile(item: &Value) -> Result<()> {
    ensure!(
        item.get("async").is_none_or(|v| v == false || v.is_null())
            && item
                .get("caller")
                .is_none_or(|v| v.is_null() || v["type"] == "direct")
            && item.get("namespace").is_none_or(|v| v.is_null() || v == ""),
        "unsupported Responses function execution profile"
    );
    Ok(())
}
pub(super) fn validate_phase(item: &Value) -> Result<()> {
    ensure!(
        item.get("phase")
            .is_none_or(|p| p.is_null() || p == "commentary" || p == "final_answer"),
        "unsupported Responses message phase"
    );
    Ok(())
}
pub(super) fn output(state: &ProviderReplayState) -> Result<&[Value]> {
    ensure!(
        is_dialect(state) && state.payload["schema_version"] == 1,
        "unsupported Responses replay dialect/version"
    );
    ensure!(
        !state.provider.is_empty()
            && state.model.as_ref().is_some_and(|model| !model.is_empty())
            && state.payload["authority_scope"]
                .as_str()
                .is_some_and(|scope| !scope.is_empty()),
        "invalid Responses replay authority/model"
    );
    let output = state.payload["output"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("invalid Responses replay output"))?;
    ensure!(output.len() <= 256, "Responses replay item limit");
    ensure!(
        output.iter().all(|item| item
            .get("status")
            .is_none_or(|s| s.is_null() || s == "completed")),
        "unfinished Responses replay item"
    );
    input_items(output)?;
    Ok(output)
}
pub(super) fn compatible(
    state: &ProviderReplayState,
    provider: &str,
    model: &str,
    authority: &str,
) -> bool {
    state.is_compatible_with(provider, model)
        && state.payload["authority_scope"] == authority
        && output(state).is_ok()
}
pub(crate) fn retention(state: &ProviderReplayState) -> Retention {
    let Ok(items) = output(state) else {
        return Retention::Unsupported;
    };
    if items
        .iter()
        .any(|i| i["type"] == "reasoning" || i["type"] == "function_call")
    {
        Retention::ActiveTurn
    } else {
        Retention::Ordinary
    }
}
/// Readable view replaces common aggregates; no ciphertext is exposed.
pub(crate) fn portable(state: &ProviderReplayState) -> Option<(String, String)> {
    if !is_dialect(state) {
        return None;
    }
    // Even an unknown version can expose only the recognized readable leaves.
    let items = state.payload["output"].as_array()?;
    let mut text = Vec::new();
    let mut reasoning = Vec::new();
    for item in items {
        if item["type"] == "message" {
            let parts = item["content"].as_array()?;
            let body = parts
                .iter()
                .filter_map(|p| match p["type"].as_str() {
                    Some("output_text") => p["text"].as_str(),
                    Some("refusal") => p["refusal"].as_str(),
                    _ => None,
                })
                .collect::<String>();
            text.push(if item["phase"] == "commentary" {
                format!("[Assistant commentary]\n{body}")
            } else {
                body
            });
        } else if item["type"] == "reasoning" {
            reasoning.extend(
                item["summary"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|p| p["text"].as_str())
                    .map(str::to_owned),
            );
            reasoning.extend(
                item["content"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|p| p["text"].as_str())
                    .map(str::to_owned),
            );
        }
    }
    if text.is_empty() && reasoning.is_empty() {
        return None;
    }
    Some((text.join("\n\n"), reasoning.join("")))
}
