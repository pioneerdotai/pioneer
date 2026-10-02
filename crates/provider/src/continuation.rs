//! Protocol rules for immutable continuation, not model availability/limits.
//! Protocol sources (reviewed 2026-10-02):
//! https://platform.claude.com/docs/en/build-with-claude/preserved-thinking
//! https://docs.aws.amazon.com/bedrock/latest/userguide/conversation-inference.html
//! https://ai.google.dev/gemini-api/docs/generate-content/thought-signatures
//! https://openrouter.ai/docs/guides/best-practices/reasoning-tokens
use crate::{ChatMessage, ProviderReplayState};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retention {
    Ordinary,
    ActiveTurn,
    Prefix,
    /// Opaque relay/unknown contract: retain, and refuse replay without a supported proof path.
    Unsupported,
}
impl Retention {
    pub fn preserves_prefix(self) -> bool {
        matches!(self, Self::Prefix | Self::Unsupported)
    }
}

// Generation rules only for the native Messages route. Hosted names do not
// inherit them. Unknown/new generations are deliberately not declared unbound.
fn claude_prefix_rule(model: Option<&str>) -> Option<bool> {
    let model = model?.to_ascii_lowercase().replace('.', "-");
    if model.starts_with("claude-fable-5-1")
        || model.starts_with("claude-opus-5-5")
        || model.starts_with("claude-sonnet-5-5")
    {
        return Some(true);
    }
    if model.starts_with("claude-3-")
        || model.starts_with("claude-4-")
        || model.starts_with("claude-sonnet-4")
        || model.starts_with("claude-opus-4")
        || model.starts_with("claude-haiku-4")
        || model == "claude-opus-5"
        || model == "claude-sonnet-5"
        || model.starts_with("claude-mythos-5-1")
    {
        return Some(false);
    }
    None
}

pub fn retention(state: &ProviderReplayState) -> Retention {
    let p = &state.payload;
    if p["api_profile"] == "unverified-relay" {
        return Retention::Unsupported;
    }
    // Consume the existing catalog API identity when available; it cannot
    // weaken an observed signed/native requirement (reasoning=false may be stale).
    if let (Some(model), Ok(catalog)) = (state.model.as_deref(), crate::catalog::model_catalog()) {
        if let Some(entry) = catalog.model(&state.provider, model) {
            let matches_route = match state.provider.as_str() {
                "anthropic" => entry.api == "anthropic-messages",
                "bedrock" => entry.api.starts_with("bedrock-converse"),
                "gemini" => entry.api == "google-generative-ai",
                _ => true,
            };
            if !matches_route {
                return Retention::Unsupported;
            }
        }
    }
    match state.provider.as_str() {
        "anthropic" => {
            let Some(blocks) = p["blocks"].as_array() else {
                return Retention::Unsupported;
            };
            let thinking = blocks
                .iter()
                .any(|b| matches!(b["type"].as_str(), Some("thinking" | "redacted_thinking")));
            if !thinking {
                return if blocks
                    .iter()
                    .all(|b| matches!(b["type"].as_str(), Some("text" | "tool_use")))
                {
                    Retention::Ordinary
                } else {
                    Retention::Unsupported
                };
            }
            match claude_prefix_rule(state.model.as_deref()) {
                Some(false) => Retention::ActiveTurn,
                Some(true) => Retention::Prefix,
                None => Retention::Unsupported,
            }
        }
        "bedrock" => {
            // All reasoningContent signatures in Converse are bound to previous
            // messages; redacted bytes are opaque and must not be assumed unsigned.
            if p["blocks"].as_array().is_some_and(|b| !b.is_empty()) {
                Retention::Prefix
            } else {
                Retention::Unsupported
            }
        }
        "gemini" => {
            if let Some(parts) = p["parts"].as_array() {
                if parts
                    .iter()
                    .any(|part| part.get("thoughtSignature").is_some_and(|v| !v.is_null()))
                {
                    return Retention::ActiveTurn;
                }
                // A proven GenerateContent 2.5 unsigned response has no prefix
                // binding. Gemini 3 calls require signatures; unknown families
                // are not promoted to ordinary by an unsigned container.
                if state
                    .model
                    .as_deref()
                    .is_some_and(|m| m.starts_with("gemini-2.5-"))
                    && parts.iter().all(|part| {
                        part.get("text").is_some() || part.get("functionCall").is_some()
                    })
                {
                    Retention::Ordinary
                } else if parts.iter().all(|part| part.get("text").is_some()) {
                    Retention::Ordinary
                } else {
                    Retention::Unsupported
                }
            } else if p["function_call_signatures"]
                .as_array()
                .is_some_and(|s| s.iter().any(|s| s.is_string()))
            {
                Retention::ActiveTurn
            } else {
                Retention::Unsupported
            }
        }
        "openrouter" => {
            let Some(details) = p["reasoning_details"].as_array() else {
                return Retention::Unsupported;
            };
            if details.iter().any(|d| {
                d["type"] == "reasoning.encrypted"
                    || d.get("signature").is_some_and(|v| !v.is_null())
            }) {
                // Chat route does not expose original upstream system/tools or
                // account authority. Even a Claude model name is not that proof.
                Retention::Unsupported
            } else {
                Retention::ActiveTurn
            }
        }
        _ if p["schema_version"] == 1 && p["assistant_message"].is_object() => {
            // Version 1 is the existing compatible producer's readable schema,
            // not a license to discard unknown signed/native extensions.
            if p.as_object().is_some_and(|object| {
                object
                    .keys()
                    .any(|key| !matches!(key.as_str(), "schema_version" | "assistant_message"))
            }) || p["assistant_message"]
                .as_object()
                .unwrap()
                .keys()
                .any(|key| !matches!(key.as_str(), "content" | "reasoning_content" | "tool_calls"))
            {
                return Retention::Unsupported;
            }
            match p["assistant_message"].get("reasoning_content") {
                Some(Value::String(_)) => Retention::ActiveTurn,
                None | Some(Value::Null) => Retention::Ordinary,
                _ => Retention::Unsupported,
            }
        }
        _ => Retention::Unsupported,
    }
}

pub fn message_retention(message: &ChatMessage) -> Retention {
    message
        .provider_replay_state
        .as_ref()
        .map(retention)
        .unwrap_or(Retention::Ordinary)
}

/// Refuse keep-tail rewriting before admission/summarizer execution. Whole
/// removed state is not replayed. No thresholds/frequency or drop policy change.
pub fn validate_compaction(
    messages: &[ChatMessage],
    selected: &std::collections::BTreeSet<usize>,
) -> Result<()> {
    for (index, message) in messages.iter().enumerate() {
        if !selected.contains(&index) && message_retention(message).preserves_prefix() {
            ensure!(
                !selected.iter().any(|selected| *selected < index),
                "unsupported client compaction: retained native state depends on the unchanged preceding prefix"
            );
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PrefixProof {
    version: u32,
    provider: String,
    model: String,
    // Random instance authority: same immutable credential/endpoint object.
    // No credential, account ID or raw system/tools/text is persisted.
    authority: String,
    messages: usize,
    prefix_sha256: String,
    response_sha256: String,
}
fn sorted(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let ordered: std::collections::BTreeMap<_, _> =
                map.iter().map(|(k, v)| (k.clone(), sorted(v))).collect();
            serde_json::to_value(ordered).expect("JSON map")
        }
        Value::Array(a) => Value::Array(a.iter().map(sorted).collect()),
        other => other.clone(),
    }
}
fn hash(value: &Value) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&sorted(
        value,
    ))?)))
}
fn prefix(body: &Value, count: usize) -> Result<Value> {
    let messages = body["messages"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("native prefix lacks messages"))?;
    ensure!(count <= messages.len(), "native prefix shortened");
    Ok(
        serde_json::json!({"system":body.get("system"),"tools":body.get("tools"),"toolConfig":body.get("toolConfig"),"messages":&messages[..count]}),
    )
}
fn response_content(state: &ProviderReplayState) -> &Value {
    &state.payload["blocks"]
}

pub(crate) fn bind_prefix(
    state: &mut ProviderReplayState,
    model: &str,
    authority: &str,
    body: &Value,
) -> Result<()> {
    state.model = Some(model.into());
    if retention(state) != Retention::Prefix {
        return Ok(());
    }
    let count = body["messages"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("native prefix lacks messages"))?
        .len();
    let proof = PrefixProof {
        version: 1,
        provider: state.provider.clone(),
        model: model.into(),
        authority: authority.into(),
        messages: count,
        prefix_sha256: hash(&prefix(body, count)?)?,
        response_sha256: hash(response_content(state))?,
    };
    state.payload["prefix_proof"] = serde_json::to_value(proof)?;
    Ok(())
}

pub(crate) fn validate_prefix(
    body: &Value,
    states: impl Iterator<Item = (usize, ProviderReplayState)>,
    model: &str,
    authority: &str,
) -> Result<()> {
    for (index, state) in states {
        match retention(&state) {
            Retention::Unsupported => anyhow::bail!(
                "native replay unsupported: selected API/model or upstream binding authority is unproven"
            ),
            Retention::Prefix => {
                let proof: PrefixProof = serde_json::from_value(
                    state.payload["prefix_proof"].clone(),
                )
                .map_err(|_| {
                    anyhow::anyhow!(
                        "native replay lacks durable outbound prefix proof (legacy/restart)"
                    )
                })?;
                ensure!(
                    proof.version == 1
                        && proof.provider == state.provider
                        && proof.model == model
                        && state.model.as_deref() == Some(model)
                        && proof.authority == authority,
                    "native replay authority changed: credential/endpoint instance or model; restart/fork requires verified authority"
                );
                ensure!(
                    proof.messages == index
                        && proof.prefix_sha256 == hash(&prefix(body, index)?)?
                        && proof.response_sha256 == hash(response_content(&state))?,
                    "native replay prefix changed: system/tools/previous messages or thinking sequence"
                );
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_registry_profile_uses_actual_state_without_a_brand_only_retention_rule() {
        for profile in crate::provider_definitions() {
            let provider = profile.name;
            let message = ChatMessage::assistant("ordinary answer");
            assert_eq!(message_retention(&message), Retention::Ordinary);
            let unknown = ProviderReplayState::for_model(
                provider,
                "unknown",
                serde_json::json!({"opaque":"unrecognized"}),
            );
            assert_eq!(retention(&unknown), Retention::Unsupported, "{provider}");
            if !matches!(provider, "anthropic" | "gemini" | "bedrock" | "openrouter") {
                let basic = ProviderReplayState::for_model(
                    provider,
                    "selected",
                    serde_json::json!({"schema_version":1,"assistant_message":{"content":"answer"}}),
                );
                assert_eq!(retention(&basic), Retention::Ordinary, "{provider}");
                let reasoning = ProviderReplayState::for_model(
                    provider,
                    "selected",
                    serde_json::json!({"schema_version":1,"assistant_message":{"reasoning_content":"reason"}}),
                );
                assert_eq!(retention(&reasoning), Retention::ActiveTurn, "{provider}");
                let extended = ProviderReplayState::for_model(
                    provider,
                    "selected",
                    serde_json::json!({"schema_version":1,"assistant_message":{"content":"answer","signature":"opaque"}}),
                );
                assert_eq!(
                    retention(&extended),
                    Retention::Unsupported,
                    "unknown required extension {provider}"
                );
            }
        }
    }
}
