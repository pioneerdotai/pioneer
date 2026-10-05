//! Outbound tool policy. Protocol rules override model compatibility metadata;
//! unknown controls fail before inference instead of silently weakening a limit.
use crate::{ChatMessage, ChatRequest, Role, ToolChoice};
use anyhow::{Result, bail, ensure};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

// Sources and profile-by-profile evidence: docs/provider-tools.md.
pub(crate) fn native_parallel_control(provider: &str) -> bool {
    let provider = policy_id(provider);
    matches!(
        provider,
        "openai"
            | "azure-openai"
            | "openrouter"
            | "groq"
            | "mistral"
            | "xai"
            | "fireworks"
            | "venice"
            | "cerebras"
            | "friendli"
            | "synthetic"
    )
}

fn policy_id(provider: &str) -> &str {
    crate::provider_definition(provider).map_or(provider, |d| d.name)
}

tokio::task_local! {
    static DISCOVERY_TOOLS: (String, bool, BTreeMap<String, bool>);
}

/// Scoped to the authority-bound provider instance, never a brand-global cache.
pub(crate) async fn with_discovery_tools<T>(
    provider: &str,
    use_public_catalog: bool,
    capabilities: BTreeMap<String, bool>,
    future: impl std::future::Future<Output = T>,
) -> T {
    DISCOVERY_TOOLS
        .scope(
            (
                policy_id(provider).to_owned(),
                use_public_catalog,
                capabilities,
            ),
            future,
        )
        .await
}

pub(crate) fn tool_support_with_catalog(
    provider: &str,
    model: &str,
    catalog: Option<&crate::catalog::ModelCatalog>,
) -> Option<bool> {
    let context = DISCOVERY_TOOLS
        .try_with(|(owner, public, c)| {
            (owner == policy_id(provider)).then(|| (*public, c.get(model).copied()))
        })
        .ok()
        .flatten();
    let (public, discovery) = context.unwrap_or((true, None));
    crate::catalog::merge_tool_support(
        discovery,
        public
            .then(|| catalog.and_then(|c| c.tool_support(provider, model)))
            .flatten(),
    )
}

pub(crate) fn model_tool_support(provider: &str, model: &str) -> Option<bool> {
    let catalog = crate::catalog::model_catalog().ok();
    tool_support_with_catalog(provider, model, catalog.as_deref())
}

fn validate_tool_name(provider: &str, name: &str) -> Result<()> {
    let provider = policy_id(provider);
    let max_name = if matches!(provider, "anthropic" | "gemini") {
        128
    } else {
        64
    };
    ensure!(
        !name.is_empty()
            && name.len() <= max_name
            && name.bytes().all(|b| b.is_ascii_alphanumeric()
                || b == b'_'
                || b == b'-'
                || provider == "gemini" && matches!(b, b'.' | b':')),
        "provider `{provider}`: invalid tool name"
    );
    Ok(())
}

pub(crate) fn prepare_request(provider: &str, request: ChatRequest) -> Result<ChatRequest> {
    let catalog = crate::catalog::model_catalog().ok();
    prepare_request_with_catalog(provider, request, catalog.as_deref())
}

pub(crate) fn prepare_request_with_catalog(
    provider: &str,
    mut request: ChatRequest,
    catalog: Option<&crate::catalog::ModelCatalog>,
) -> Result<ChatRequest> {
    let provider = policy_id(provider);
    let use_public_catalog = DISCOVERY_TOOLS
        .try_with(|(owner, public, _)| owner != provider || *public)
        .unwrap_or(true);
    let catalog = catalog.filter(|_| use_public_catalog);
    let tools = request.tools.as_deref().unwrap_or_default();
    let mut names = BTreeSet::new();
    for tool in tools {
        validate_tool_name(provider, &tool.name)?;
        ensure!(
            names.insert(tool.name.as_str()),
            "provider `{provider}`: duplicate tool name"
        );
        ensure!(
            tool.parameters.is_object(),
            "provider `{provider}`: tool parameters must be a JSON schema object"
        );
        ensure!(
            tool.parameters.get("type").is_none_or(|t| t == "object"),
            "provider `{provider}`: function parameters must describe an object"
        );
    }
    match request.tool_choice.as_ref() {
        Some(ToolChoice::Tool { name }) => ensure!(
            names.contains(name.as_str()),
            "provider `{provider}`: selected tool is not in request.tools"
        ),
        Some(ToolChoice::Required) => ensure!(
            !tools.is_empty(),
            "provider `{provider}`: Required needs at least one tool"
        ),
        _ => {}
    }

    let disabled = matches!(request.tool_choice, Some(ToolChoice::None));
    // A disabled choice does not authorize definitions on a non-tool model.
    // Fail before sending; do not project tool history into text to evade it.
    let has_tool_history = request
        .messages
        .iter()
        .any(|m| m.role == Role::Tool || m.tool_calls.as_ref().is_some_and(|c| !c.is_empty()));
    ensure!(
        tools.is_empty() && !has_tool_history
            || tool_support_with_catalog(provider, &request.model, catalog) != Some(false),
        "provider `{provider}`: catalog/discovery model does not support tool definitions/history"
    );
    // Converse has no None union member. Omitting the tool config is equivalent
    // for a fresh text request, but cannot safely continue toolUse/toolResult.
    if provider == "bedrock"
        && disabled
        && request
            .messages
            .iter()
            .any(|m| m.role == Role::Tool || m.tool_calls.as_ref().is_some_and(|c| !c.is_empty()))
    {
        bail!("provider `bedrock`: None cannot be represented with tool continuation history");
    }
    if disabled
        && matches!(
            provider,
            "bedrock"
                | "cohere"
                | "ollama"
                | "glm"
                | "zai"
                | "glm-coding"
                | "zai-coding"
                | "novita"
        )
    {
        request.tools = None;
        request.tool_choice = None;
        request.parallel_tool_calls = None;
        return Ok(request);
    }
    if tools.is_empty() {
        request.tools = None;
        request.tool_choice = None;
        request.parallel_tool_calls = None;
        return Ok(request);
    }
    // These families expose the tools field on Responses, not Chat, even if
    // a caller disables new calls. Azure opaque deployments cannot be inferred.
    if matches!(provider, "openai" | "azure-openai")
        && ["gpt-6-astra", "gpt-6.1-sol"]
            .iter()
            .any(|p| request.model == *p || request.model.starts_with(&format!("{p}-")))
    {
        bail!("provider `{provider}`: this model requires Responses API for tools");
    }
    // Published Messages contract: these families reject forced tool use even
    // with thinking disabled. This is a protocol supplement, not a model list.
    if provider == "anthropic"
        && matches!(
            request.tool_choice,
            Some(ToolChoice::Required | ToolChoice::Tool { .. })
        )
    {
        let model = request.model.replace('.', "-");
        ensure!(
            ![
                "claude-opus-5-5",
                "claude-sonnet-5-5",
                "claude-fable-5-1",
                "claude-mythos-5-1"
            ]
            .iter()
            .any(|family| model
                .strip_prefix(family)
                .is_some_and(|tail| tail.is_empty() || tail.starts_with('-'))),
            "provider `anthropic`: this model family does not support Required/named tool choice"
        );
    }
    if matches!(
        provider,
        "cohere" | "ollama" | "glm" | "zai" | "glm-coding" | "zai-coding" | "novita"
    ) {
        ensure!(
            matches!(request.tool_choice, None | Some(ToolChoice::Auto)),
            "provider `{provider}`: Required/named tool choice is not supported by this API profile"
        );
        request.tool_choice = None; // Native/default Auto, not a forced mode.
    }
    if provider == "synthetic" && matches!(request.tool_choice, Some(ToolChoice::Required)) {
        bail!("provider `synthetic`: Required is not in the published Chat tool choice contract");
    }
    if provider == "bedrock" && matches!(request.tool_choice, Some(ToolChoice::Tool { .. })) {
        ensure!(
            request.model.contains("anthropic.claude-3") || request.model.contains("amazon.nova"),
            "provider `bedrock`: named tool choice requires a verified Claude 3 or Nova model identity"
        );
    }
    if !disabled && provider != "anthropic" {
        let metadata_control = catalog
            .and_then(|c| c.model(provider, &request.model))
            .and_then(|m| m.metadata.get("compat").cloned())
            .and_then(|c| c.get("supportsParallelToolCalls").and_then(|v| v.as_bool()));
        let verified_chat_control = catalog
            .and_then(|c| c.model(provider, &request.model))
            .is_some_and(|m| m.api == "openai-completions" && metadata_control == Some(true));
        let native = (native_parallel_control(provider) && metadata_control != Some(false))
            || verified_chat_control
                && !matches!(
                    provider,
                    "cohere"
                        | "ollama"
                        | "gemini"
                        | "bedrock"
                        | "glm"
                        | "zai"
                        | "glm-coding"
                        | "zai-coding"
                );
        if request.parallel_tool_calls == Some(false) && !native {
            bail!(
                "provider `{provider}`: parallel=false cannot be enforced by this API/model profile"
            );
        }
        // true permits multiple calls; it does not require a parallel call.
        // Providers without a switch retain their native/default behavior.
        if !native {
            request.parallel_tool_calls = None;
        }
    } else if disabled {
        request.parallel_tool_calls = None;
    }
    Ok(request)
}

fn wire_id(provider: &str, id: &str) -> Result<String> {
    let provider = policy_id(provider);
    ensure!(!id.is_empty(), "provider `{provider}`: empty tool call ID");
    if provider == "mistral" {
        if id.len() == 9 && id.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Ok(id.to_owned());
        }
        // Fixed hash, never lossy truncation/sanitization. Collisions are rejected
        // by the caller, so adding history cannot silently retarget an old call.
        return Ok(hex::encode(Sha256::digest(id.as_bytes()))[..9].to_owned());
    }
    if provider == "bedrock" {
        let max_len: usize = 64;
        if id.len() > max_len
            || !id.bytes().all(|b| {
                b.is_ascii_alphanumeric()
                    || b == b'_'
                    || b == b'-'
                    || provider == "bedrock" && matches!(b, b'.' | b':')
            })
        {
            return Ok(
                format!("call_{}", hex::encode(Sha256::digest(id.as_bytes())))[..max_len.min(69)]
                    .to_owned(),
            );
        }
    }
    Ok(id.to_owned())
}

/// Validate completed outbound rounds, then normalize both sides of every
/// binding. Operates on the prepared view, never on persisted canonical IDs.
pub(crate) fn prepare_history(provider: &str, messages: &mut [ChatMessage]) -> Result<()> {
    let mut pending = BTreeMap::<String, (String, String)>::new();
    let mut used_wire = BTreeMap::<String, String>::new();
    for message in messages {
        if message.role == Role::Tool {
            let id = message.tool_call_id.as_ref().ok_or_else(|| {
                anyhow::anyhow!("provider `{provider}`: tool result is missing call ID")
            })?;
            let (name, normalized) = pending.remove(id).ok_or_else(|| {
                anyhow::anyhow!("provider `{provider}`: orphan or duplicate tool result")
            })?;
            ensure!(
                message.name.as_ref().is_none_or(|n| n == &name),
                "provider `{provider}`: tool result name does not match call ID"
            );
            message.name = Some(name);
            message.tool_call_id = Some(normalized);
            continue;
        }
        ensure!(
            pending.is_empty(),
            "provider `{provider}`: missing results before next message/round"
        );
        for call in message.tool_calls.iter_mut().flatten() {
            validate_tool_name(provider, &call.name)?;
            ensure!(
                message.role == Role::Assistant,
                "provider `{provider}`: tool calls need assistant role"
            );
            let input: serde_json::Value = serde_json::from_str(&call.arguments).map_err(|_| {
                anyhow::anyhow!("provider `{provider}`: tool arguments are invalid JSON")
            })?;
            ensure!(
                input.is_object(),
                "provider `{provider}`: tool arguments must be a JSON object"
            );
            let normalized = wire_id(provider, &call.id)?;
            if let Some(original) = used_wire.insert(normalized.clone(), call.id.clone()) {
                ensure!(
                    original == call.id,
                    "provider `{provider}`: normalized tool ID collision"
                );
            }
            ensure!(
                pending
                    .insert(call.id.clone(), (call.name.clone(), normalized.clone()))
                    .is_none(),
                "provider `{provider}`: duplicate tool call ID in one round"
            );
            // Replay may include opaque data bound to these IDs. Never rewrite it.
            ensure!(
                normalized == call.id || message.provider_replay_state.is_none(),
                "provider `{provider}`: restricted tool ID requires replay-aware normalization"
            );
            call.id = normalized;
        }
    }
    ensure!(
        pending.is_empty(),
        "provider `{provider}`: missing tool results at end of history"
    );
    Ok(())
}

/// Legacy/template-compatible projection in call order (including repeated
/// names), retaining original attachment indexes. Current Ollama native types
/// also support optional tool_call_id; preserving that wire field belongs to
/// G02 R2. Ordering alone does not establish native ID association.
pub(crate) fn ordered_tool_results(messages: &[ChatMessage]) -> Vec<usize> {
    let mut order = Vec::with_capacity(messages.len());
    let mut i = 0;
    while i < messages.len() {
        order.push(i);
        let calls = messages[i].tool_calls.as_deref().unwrap_or_default();
        i += 1;
        if calls.is_empty() {
            continue;
        }
        let start = i;
        while i < messages.len() && messages[i].role == Role::Tool {
            i += 1;
        }
        if i - start == calls.len() {
            let indexes: BTreeMap<_, _> = (start..i)
                .filter_map(|index| {
                    messages[index]
                        .tool_call_id
                        .as_deref()
                        .map(|id| (id, index))
                })
                .collect();
            let sorted: Option<Vec<_>> = calls
                .iter()
                .map(|call| indexes.get(call.id.as_str()).copied())
                .collect();
            if let Some(sorted) = sorted {
                order.extend(sorted);
                continue;
            }
        }
        order.extend(start..i);
    }
    order
}

#[cfg(test)]
pub(crate) fn test_request() -> ChatRequest {
    ChatRequest {
        model: "test-tool-model".into(),
        messages: vec![ChatMessage::user("use tools")],
        temperature: None,
        max_tokens: None,
        tools: Some(vec![crate::ToolDefinition {
            name: "lookup".into(),
            description: "Lookup".into(),
            parameters: serde_json::json!({"type":"object","properties":{"key":{"type":"string"}},"required":["key"],"additionalProperties":false}),
        }]),
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProviderToolCall;

    pub(crate) fn round(ids: &[&str]) -> Vec<ChatMessage> {
        let mut assistant = ChatMessage::assistant("");
        assistant.tool_calls = Some(
            ids.iter()
                .map(|id| ProviderToolCall {
                    id: (*id).into(),
                    name: "lookup".into(),
                    arguments: r#"{"key":"value"}"#.into(),
                })
                .collect(),
        );
        let mut messages = vec![assistant];
        messages.extend(
            ids.iter()
                .rev()
                .map(|id| ChatMessage::tool_result(*id, "lookup", *id)),
        );
        messages
    }

    #[test]
    fn every_registry_profile_handles_explicit_parallel_permission_and_limit() {
        for profile in crate::provider_definitions() {
            if profile.name == "local" {
                continue;
            }
            for parallel in [None, Some(true), Some(false)] {
                let mut request = test_request();
                request.parallel_tool_calls = parallel;
                let result = prepare_request(profile.name, request);
                let enforceable = matches!(
                    profile.name,
                    "anthropic"
                        | "openai"
                        | "azure-openai"
                        | "openrouter"
                        | "groq"
                        | "mistral"
                        | "xai"
                        | "fireworks"
                        | "venice"
                        | "cerebras"
                        | "friendli"
                        | "synthetic"
                );
                assert_eq!(
                    result.is_ok(),
                    parallel != Some(false) || enforceable,
                    "{} {parallel:?}",
                    profile.name
                );
                if let Ok(request) = result {
                    if !enforceable {
                        assert!(request.parallel_tool_calls.is_none(), "{}", profile.name);
                    }
                }
            }
        }
    }

    #[test]
    fn modes_missing_name_empty_tools_and_none_equivalence() {
        for profile in crate::provider_definitions() {
            if profile.name == "local" {
                continue;
            }
            let mut missing = test_request();
            missing.tool_choice = Some(ToolChoice::Tool {
                name: "missing".into(),
            });
            assert!(
                prepare_request(profile.name, missing).is_err(),
                "{}",
                profile.name
            );
            let mut empty = test_request();
            empty.tools = Some(vec![]);
            empty.tool_choice = Some(ToolChoice::Required);
            assert!(
                prepare_request(profile.name, empty).is_err(),
                "{}",
                profile.name
            );
            for choice in [
                ToolChoice::Auto,
                ToolChoice::None,
                ToolChoice::Required,
                ToolChoice::Tool {
                    name: "lookup".into(),
                },
            ] {
                let mut request = test_request();
                request.model = "anthropic.claude-3-5-sonnet-20240620-v1:0".into();
                request.tool_choice = Some(choice.clone());
                let forced = matches!(choice, ToolChoice::Required | ToolChoice::Tool { .. });
                let unsupported = matches!(
                    profile.name,
                    "cohere" | "ollama" | "glm" | "zai" | "glm-coding" | "zai-coding" | "novita"
                ) && forced
                    || profile.name == "synthetic" && matches!(choice, ToolChoice::Required);
                let normalized = prepare_request(profile.name, request);
                assert_eq!(
                    normalized.is_err(),
                    unsupported,
                    "{} {choice:?}",
                    profile.name
                );
                if let Ok(request) = normalized {
                    if matches!(choice, ToolChoice::None)
                        && matches!(
                            profile.name,
                            "bedrock"
                                | "cohere"
                                | "ollama"
                                | "glm"
                                | "zai"
                                | "glm-coding"
                                | "zai-coding"
                                | "novita"
                        )
                    {
                        assert!(request.tools.is_none());
                    }
                }
            }
        }
    }

    #[test]
    fn history_ids_remain_paired_across_two_parallel_rounds_and_provider_switches() {
        let mut original = round(&["a/b", "a?b", "ABC123xyz"]);
        original.extend(round(&["a/b", "other-call"]));
        for profile in crate::provider_definitions() {
            if profile.name == "local" {
                continue;
            }
            let mut messages = original.clone();
            prepare_history(profile.name, &mut messages).unwrap();
            assert_eq!(
                messages[0].tool_calls.as_ref().unwrap()[0].id,
                messages[3].tool_call_id.as_ref().unwrap().as_str()
            );
            assert_eq!(
                messages[4].tool_calls.as_ref().unwrap()[0].id,
                messages[6].tool_call_id.as_ref().unwrap().as_str()
            );
            if profile.name == "mistral" {
                assert_eq!(messages[0].tool_calls.as_ref().unwrap()[2].id, "ABC123xyz");
                for call in messages.iter().flat_map(|m| m.tool_calls.iter().flatten()) {
                    assert_eq!(call.id.len(), 9);
                    assert!(call.id.bytes().all(|b| b.is_ascii_alphanumeric()));
                }
                assert_ne!(
                    messages[0].tool_calls.as_ref().unwrap()[0].id,
                    messages[0].tool_calls.as_ref().unwrap()[1].id
                );
            }
            let first = messages.clone();
            prepare_history(profile.name, &mut messages).unwrap();
            assert_eq!(
                messages
                    .iter()
                    .map(|m| (&m.tool_calls, &m.tool_call_id))
                    .collect::<Vec<_>>(),
                first
                    .iter()
                    .map(|m| (&m.tool_calls, &m.tool_call_id))
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(original[0].tool_calls.as_ref().unwrap()[0].id, "a/b");
        let mut ollama = original.clone();
        prepare_history("ollama", &mut ollama).unwrap();
        assert_eq!(ordered_tool_results(&ollama), vec![0, 3, 2, 1, 4, 6, 5]);
    }

    #[test]
    fn malformed_history_fails_without_retargeting_calls() {
        let mut missing = round(&["one", "two"]);
        missing.pop();
        assert!(prepare_history("openai", &mut missing).is_err());
        let mut duplicate = round(&["one", "one"]);
        assert!(prepare_history("mistral", &mut duplicate).is_err());
        let mut wrong_name = round(&["one"]);
        wrong_name[1].name = Some("wrong".into());
        assert!(prepare_history("gemini", &mut wrong_name).is_err());
        let mut invalid_name = round(&["one"]);
        invalid_name[0].tool_calls.as_mut().unwrap()[0].name = "invalid/name".into();
        assert!(prepare_history("openai", &mut invalid_name).is_err());
        let mut orphan = vec![ChatMessage::tool_result("orphan", "lookup", "result")];
        assert!(prepare_history("bedrock", &mut orphan).is_err());
        let mut malformed = round(&["one"]);
        malformed[0].tool_calls.as_mut().unwrap()[0].arguments = "[]".into();
        assert!(prepare_history("anthropic", &mut malformed).is_err());
        let mut collision = round(&["foreign/id"]);
        let hashed = wire_id("mistral", "foreign/id").unwrap();
        collision.extend(round(&[&hashed]));
        assert!(prepare_history("mistral", &mut collision).is_err());
    }

    #[test]
    fn bedrock_none_with_history_and_responses_only_chat_tools_reject() {
        let mut request = test_request();
        request.messages = round(&["call"]);
        request.tool_choice = Some(ToolChoice::None);
        assert!(prepare_request("bedrock", request).is_err());
        for provider in ["openai", "azure-openai"] {
            for model in ["gpt-6-astra", "gpt-6.1-sol", "gpt-6.1-sol-2026-09-01"] {
                let mut request = test_request();
                request.model = model.into();
                assert!(prepare_request(provider, request).is_err());
            }
        }
    }

    #[test]
    fn bedrock_named_choice_requires_verified_model_identity() {
        for (model, supported) in [
            ("us.anthropic.claude-3-5-sonnet-20240620-v1:0", true),
            ("amazon.nova-pro-v1:0", true),
            ("anthropic.claude-opus-4-1-v1:0", false),
            ("meta.llama3-3-70b-instruct-v1:0", false),
            ("opaque-deployment", false),
        ] {
            let mut request = test_request();
            request.model = model.into();
            request.tool_choice = Some(ToolChoice::Tool {
                name: "lookup".into(),
            });
            assert_eq!(
                prepare_request("bedrock", request).is_ok(),
                supported,
                "{model}"
            );
        }
    }

    #[test]
    fn names_and_schema_validation_follow_native_profile() {
        let mut request = test_request();
        request.tools.as_mut().unwrap()[0].name = "x".repeat(100);
        assert!(prepare_request("anthropic", request.clone()).is_ok());
        assert!(prepare_request("openai", request).is_err());
        let mut request = test_request();
        request.tools.as_mut().unwrap()[0].name = "ns.tool:lookup".into();
        assert!(prepare_request("gemini", request.clone()).is_ok());
        assert!(prepare_request("mistral", request).is_err());
        let mut request = test_request();
        request.tools.as_mut().unwrap()[0].parameters = serde_json::json!([]);
        assert!(prepare_request("cohere", request).is_err());
    }

    #[test]
    fn anthropic_forced_choice_is_model_specific() {
        for model in [
            "claude-opus-5-5",
            "claude-sonnet-5.5",
            "claude-fable-5-1",
            "claude-mythos-5-1-20260901",
        ] {
            for choice in [
                ToolChoice::Auto,
                ToolChoice::None,
                ToolChoice::Required,
                ToolChoice::Tool {
                    name: "lookup".into(),
                },
            ] {
                let mut request = test_request();
                request.model = model.into();
                request.tool_choice = Some(choice.clone());
                assert_eq!(
                    prepare_request("anthropic", request).is_err(),
                    matches!(choice, ToolChoice::Required | ToolChoice::Tool { .. })
                );
            }
        }
        let mut request = test_request();
        request.model = "claude-opus-5".into();
        request.tool_choice = Some(ToolChoice::Required);
        assert!(prepare_request("anthropic", request).is_ok());
    }
}
