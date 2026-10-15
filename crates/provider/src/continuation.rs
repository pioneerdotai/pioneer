//! Protocol rules for immutable continuation, not model availability/limits.
//! Protocol sources (reviewed 2026-10-02; OpenRouter detail leaf rechecked 2026-10-05):
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
    /// Unrecognized required native representation: retain, but refuse replay.
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
    if [
        "claude-fable-5-1",
        "claude-opus-5-5",
        "claude-sonnet-5-5",
        "claude-haiku-5-5",
    ]
    .iter()
    .any(|family| model == *family || model.starts_with(&format!("{family}-")))
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
    // Older clients stamped configured Messages proxies as unverified relays.
    // An endpoint override alone does not alter the accepted native schema.
    // The producing adapter owns the native representation. A refreshed
    // catalog's preferred API must not invalidate already accepted history.
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
                // An unknown alias may use a prefix-bound generation. Keep
                // the prefix through compaction; native signatures still work
                // without an optional client-issued proof.
                None => Retention::Prefix,
            }
        }
        "bedrock" => {
            // All reasoningContent signatures in Converse are bound to previous
            // messages; redacted bytes are opaque and must not be assumed unsigned.
            if p["blocks"].as_array().is_some_and(|b| !b.is_empty()) {
                Retention::Prefix
            } else if p["native_content"].as_array().is_some_and(|blocks| {
                !blocks.is_empty()
                    && blocks
                        .iter()
                        .all(|block| block.is_object() && block.get("reasoningContent").is_none())
            }) {
                Retention::Ordinary
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
                // Unsigned function calls are valid for non-thinking models
                // and Gemini 2.5. A deployment alias is not evidence that an
                // accepted response is invalid. Keep unclassified tool rounds
                // intact and let GenerateContent enforce its signature rules;
                // never synthesize a signature or omit native parts.
                if parts
                    .iter()
                    .all(|part| part.get("text").is_some() || part.get("functionCall").is_some())
                {
                    if parts.iter().all(|part| part.get("text").is_some())
                        || state.model.as_deref().is_some_and(|m| {
                            m.starts_with("gemini-1.5-")
                                || m.starts_with("gemini-2.0-")
                                || m.starts_with("gemini-2.5-")
                        })
                    {
                        Retention::Ordinary
                    } else {
                        Retention::ActiveTurn
                    }
                } else {
                    Retention::Unsupported
                }
            } else if p["function_call_signatures"]
                .as_array()
                .is_some_and(|s| s.iter().all(|s| s.is_null() || s.is_string()))
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
            // Empty arrays contain no continuation blocks. The response producer
            // already omits replay state for them; accept equivalent legacy state.
            if details.is_empty() {
                return Retention::Ordinary;
            }
            // OpenRouter documents replay of encrypted and signed details as
            // well as readable ones. Preserve the original objects and order;
            // the client is not required to decrypt or attest upstream accounts.
            // https://openrouter.ai/docs/guides/best-practices/reasoning-tokens#reasoning-detail-types
            if details.iter().all(|detail| {
                let Some(object) = detail.as_object() else {
                    return false;
                };
                if !object
                    .get("id")
                    .is_none_or(|value| value.is_null() || value.is_string())
                    || !object
                        .get("format")
                        .is_none_or(|value| value.is_null() || value.is_string())
                    || !object
                        .get("index")
                        .is_none_or(|value| value.is_null() || value.is_number())
                    || !object
                        .get("signature")
                        .is_none_or(|value| value.is_null() || value.is_string())
                {
                    return false;
                }
                match object.get("type").and_then(Value::as_str) {
                    Some("reasoning.text") => object.get("text").is_some_and(Value::is_string),
                    Some("reasoning.summary") => {
                        object.get("summary").is_some_and(Value::is_string)
                    }
                    Some("reasoning.encrypted") => object.get("data").is_some_and(Value::is_string),
                    _ => false,
                }
            }) {
                Retention::ActiveTurn
            } else {
                // Malformed or unrecognized representations remain stored;
                // do not silently discard required continuation data.
                Retention::Unsupported
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
    // Stable digest of the protocol endpoint. Credential rotation is handled
    // by the registry; native account/signature ownership is checked by the API.
    // Legacy v1 records used an instance nonce.
    authority: String,
    messages: usize,
    prefix_sha256: String,
    /// Converse binds previous messages. Its request-level system/toolConfig
    /// are not part of that documented message signature contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    messages_sha256: Option<String>,
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

/// Stable local transport identity. Recreating an adapter with the same
/// endpoint must not invalidate a durable native signature. API key/session
/// token rotation does not establish a different provider account.
pub(crate) fn replay_authority(provider: &str, components: &[&str]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"pioneer-native-replay-authority-v2");
    for component in std::iter::once(provider).chain(components.iter().copied()) {
        digest.update((component.len() as u64).to_be_bytes());
        digest.update(component.as_bytes());
    }
    hex::encode(digest.finalize())
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

fn message_prefix(body: &Value, count: usize) -> Result<Value> {
    let messages = body["messages"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("native prefix lacks messages"))?;
    ensure!(count <= messages.len(), "native prefix shortened");
    Ok(Value::Array(messages[..count].to_vec()))
}

/// Claude's prefix-bound generations support operator instructions and tool
/// changes appended to messages. Save/reconstruct their actual wire positions,
/// rather than overwriting the system/tools preceding already signed answers.
/// This metadata contains request instructions, never credentials.
pub(crate) fn stabilize_anthropic_prefix(
    body: &mut Value,
    states: &[ProviderReplayState],
) -> Result<bool> {
    let Some(context) = states.iter().rev().find_map(|state| {
        (retention(state) == Retention::Prefix)
            .then(|| state.payload.get("prefix_context"))
            .flatten()
    }) else {
        return Ok(false);
    };
    let requested_system = body.get("system").cloned().unwrap_or(Value::Null);
    let requested_tools = body.get("tools").cloned().unwrap_or(Value::Null);
    let root_system = context.get("system").cloned().unwrap_or(Value::Null);
    let root_tools = context.get("tools").cloned().unwrap_or(Value::Null);
    let mut effective_system = root_system.clone();
    let mut effective_tools = root_tools.as_array().cloned().unwrap_or_default();
    let mut requires_tools_beta = false;
    let messages = body["messages"]
        .as_array_mut()
        .ok_or_else(|| anyhow::anyhow!("native prefix lacks messages"))?;
    let insertions = context["insertions"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("invalid native prefix context"))?;
    for insertion in insertions {
        let index = insertion["index"]
            .as_u64()
            .and_then(|index| usize::try_from(index).ok())
            .ok_or_else(|| anyhow::anyhow!("invalid native prefix position"))?;
        let message = &insertion["message"];
        ensure!(
            index <= messages.len() && message["role"] == "system",
            "native prefix shortened or malformed"
        );
        let blocks = message["content"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("invalid native system content"))?;
        for block in blocks {
            match block["type"].as_str() {
                Some("text") => effective_system = block["text"].clone(),
                Some("tool_removal") => {
                    requires_tools_beta = true;
                    ensure!(
                        block["tool"]["type"] == "tool_reference"
                            && block["tool"]["name"].is_string(),
                        "invalid native tool removal"
                    );
                    effective_tools.retain(|tool| tool["name"] != block["tool"]["name"]);
                }
                Some("tool_addition") => {
                    requires_tools_beta = true;
                    let definition = &block["tool"]["definition"];
                    ensure!(
                        block["tool"]["type"] == "tool_definition"
                            && definition["name"].is_string(),
                        "invalid native tool addition"
                    );
                    effective_tools.retain(|tool| tool["name"] != definition["name"]);
                    effective_tools.push(definition.clone());
                }
                _ => anyhow::bail!("invalid native prefix context block"),
            }
        }
        messages.insert(index, message.clone());
    }
    let mut updates = Vec::new();
    if requested_system != effective_system {
        updates.push(
            serde_json::json!({"type":"text", "text": requested_system.as_str().unwrap_or("")}),
        );
    }
    let wanted = requested_tools.as_array().cloned().unwrap_or_default();
    for tool in &effective_tools {
        if !wanted.iter().any(|wanted| wanted["name"] == tool["name"]) {
            updates.push(serde_json::json!({"type":"tool_removal", "tool":{"type":"tool_reference", "name":tool["name"]}}));
            requires_tools_beta = true;
        }
    }
    for tool in &wanted {
        if !effective_tools.iter().any(|old| old == tool) {
            updates.push(serde_json::json!({"type":"tool_addition", "tool":{"type":"tool_definition", "definition":tool}}));
            requires_tools_beta = true;
        }
    }
    if !updates.is_empty() {
        ensure!(
            messages.last().is_some_and(|m| m["role"] == "user"),
            "native system update must follow user input or tool results"
        );
        messages.push(serde_json::json!({"role":"system", "content":updates}));
    }
    for (key, value) in [("system", root_system), ("tools", root_tools)] {
        if value.is_null() {
            body.as_object_mut().unwrap().remove(key);
        } else {
            body[key] = value;
        }
    }
    Ok(requires_tools_beta)
}

pub(crate) fn anthropic_inline_tools(body: &Value) -> bool {
    body["messages"].as_array().is_some_and(|messages| {
        messages.iter().any(|message| {
            message["role"] == "system"
                && message["content"].as_array().is_some_and(|blocks| {
                    blocks.iter().any(|block| {
                        matches!(
                            block["type"].as_str(),
                            Some("tool_addition" | "tool_removal")
                        )
                    })
                })
        })
    })
}
fn response_content(state: &ProviderReplayState) -> &Value {
    if state.provider == "bedrock" {
        state
            .payload
            .get("native_content")
            .unwrap_or(&state.payload["blocks"])
    } else {
        &state.payload["blocks"]
    }
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
    if state.provider == "anthropic" && claude_prefix_rule(Some(model)).is_none() {
        // Native signatures are valid for unlisted aliases too. An unlisted
        // alias does not prove support for our inline-system/tools transport.
        // Preserve conservatively during compaction and defer signature
        // verification to Messages without inventing an API restriction.
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
        messages_sha256: (state.provider == "bedrock")
            .then(|| hash(&message_prefix(body, count)?))
            .transpose()?,
        response_sha256: hash(response_content(state))?,
    };
    if state.provider == "anthropic" {
        let insertions: Vec<_> = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .filter(|(_, message)| message["role"] == "system")
            .map(|(index, message)| serde_json::json!({"index":index, "message":message}))
            .collect();
        state.payload["prefix_context"] = serde_json::json!({
            "system":body.get("system"), "tools":body.get("tools"), "insertions":insertions,
        });
    }
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
                // Native APIs validate their signatures. Old accepted records
                // did not contain our optional local proof; its absence is not
                // an API capability failure and must not strand those sessions.
                let Some(raw_proof) = state.payload.get("prefix_proof") else {
                    continue;
                };
                let proof: PrefixProof = serde_json::from_value(raw_proof.clone())?;
                ensure!(
                    proof.version == 1
                        && proof.provider == state.provider
                        && proof.model == model
                        && state.model.as_deref() == Some(model)
                        // Version-1 records used a random 32-character instance
                        // nonce. It cannot establish a credential change. Keep
                        // their prefix/content checks and native validation.
                        && (proof.authority.len() == 32 || proof.authority == authority),
                    "native replay authority changed: endpoint or model"
                );
                // Legacy Messages proofs did not retain the request root or
                // native positions. They cannot reconstruct dynamic runtime
                // instructions and must not invent stricter rules than the
                // vendor for accounts that accepted this durable history.
                let reconstructed =
                    state.provider != "anthropic" || state.payload.get("prefix_context").is_some();
                let prefix_matches = if state.provider == "bedrock" {
                    // Old proofs mixed mutable system/toolConfig into their
                    // digest. They cannot prove a previous-message rewrite;
                    // native Converse still validates the supplied signature.
                    proof.messages == index
                        && match &proof.messages_sha256 {
                            Some(digest) => *digest == hash(&message_prefix(body, index)?)?,
                            None => true,
                        }
                } else {
                    proof.messages == index && proof.prefix_sha256 == hash(&prefix(body, index)?)?
                };
                ensure!(
                    (!reconstructed || prefix_matches)
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
pub(crate) mod tests {
    use super::*;
    // Hypothetical unknown variants are policy fixtures, not observed responses.
    pub(crate) fn openrouter_detail_cases() -> Vec<(Value, Retention)> {
        use serde_json::json;
        vec![
            (
                json!([{"type":"reasoning.text","text":"first","signature":null,"format":"anthropic-claude-v1","index":0},{"type":"reasoning.summary","summary":"second","index":1}]),
                Retention::ActiveTurn,
            ),
            (
                json!([{"type":"reasoning.text","text":"","id":null}]),
                Retention::ActiveTurn,
            ),
            (
                json!([{"type":"reasoning.summary","summary":"","signature":null}]),
                Retention::ActiveTurn,
            ),
            (
                json!([{"type":"reasoning.encrypted","data":"opaque"}]),
                Retention::ActiveTurn,
            ),
            (
                json!([{"type":"reasoning.text","text":"signed","signature":"opaque"}]),
                Retention::ActiveTurn,
            ),
            (
                json!([{"type":"reasoning.text","text":"signed","signature":""}]),
                Retention::ActiveTurn,
            ),
            (
                json!([{"type":"reasoning.summary","summary":"known"},{"type":"reasoning.native-v-next","data":"opaque"}]),
                Retention::Unsupported,
            ),
            (
                json!([{"type":"reasoning.native-v-next","data":"opaque"}]),
                Retention::Unsupported,
            ),
            (
                json!([{"data":"opaque","format":"openai-responses-v1"}]),
                Retention::Unsupported,
            ),
            (json!(["opaque", null, 42]), Retention::Unsupported),
            (
                json!([{"type":"reasoning.text","data":"opaque"}]),
                Retention::Unsupported,
            ),
            (
                json!([{"type":"reasoning.text","text":"known","opaque_extension":"unknown"}]),
                Retention::ActiveTurn,
            ),
            (
                json!([{"type":"reasoning.text","text":"known","id":{"opaque":"unknown"}}]),
                Retention::Unsupported,
            ),
            (
                json!([{"type":"reasoning.summary","summary":"known","text":{"opaque":"unknown"}}]),
                Retention::ActiveTurn,
            ),
            (
                json!([{"type":"reasoning.text","text":"native","summary":null,"format":null,"index":null}]),
                Retention::ActiveTurn,
            ),
            (json!([]), Retention::Ordinary),
        ]
    }

    #[test]
    fn openrouter_readable_details_require_positive_contract_and_preserve_original_state() {
        for (details, expected) in openrouter_detail_cases() {
            let state = ProviderReplayState::for_model(
                "openrouter",
                "selected",
                serde_json::json!({"reasoning_details":details}),
            );
            let original = state.clone();
            assert_eq!(retention(&state), expected, "{}", state.payload);
            let mut tail = ChatMessage::assistant("answer");
            tail.provider_replay_state = Some(state.clone());
            let messages = [ChatMessage::user("old prefix"), tail];
            assert_eq!(
                validate_compaction(&messages, &std::collections::BTreeSet::from([0])).is_err(),
                expected.preserves_prefix()
            );
            assert!(
                validate_compaction(&messages, &std::collections::BTreeSet::from([0, 1])).is_ok()
            );
            let body = serde_json::json!({"messages":[{"role":"user","content":"rewritten prefix"},{"role":"assistant","content":"answer"}]});
            assert_eq!(
                validate_prefix(
                    &body,
                    std::iter::once((1, state.clone())),
                    "selected",
                    "fixture"
                )
                .is_err(),
                expected == Retention::Unsupported
            );
            assert_eq!(state, original);
        }
    }
    #[test]
    fn gemini_legacy_unsigned_tool_state_is_preserved_for_native_validation() {
        for model in ["gemini-2.0-flash", "gemini-2.5-flash", "deployment-alias"] {
            let state = ProviderReplayState::for_model(
                "gemini",
                model,
                serde_json::json!({"schema_version":1,"function_call_signatures":[null]}),
            );
            assert_eq!(retention(&state), Retention::ActiveTurn);
        }
        let malformed = ProviderReplayState::for_model(
            "gemini",
            "deployment-alias",
            serde_json::json!({"schema_version":1,"function_call_signatures":[42]}),
        );
        assert_eq!(retention(&malformed), Retention::Unsupported);
    }

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
