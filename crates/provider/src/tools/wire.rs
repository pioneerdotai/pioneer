//! A request-owned bijection. Durable names stay canonical; opaque native state
//! stays native. No mutable global registry and no lossy slug sanitization.
use crate::{ChatMessage, ChatRequest, ProviderToolCall, ToolChoice};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone)]
pub(crate) struct ToolWireManifest {
    canonical_to_wire: BTreeMap<String, String>,
    wire_to_canonical: BTreeMap<String, String>,
    advertised: BTreeSet<String>,
}

fn legal(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn alias(name: &str) -> String {
    if legal(name) {
        name.to_owned()
    } else {
        format!(
            "p1v1{}",
            &hex::encode(Sha256::digest(name.as_bytes()))[..60]
        )
    }
}

impl ToolWireManifest {
    pub(crate) fn new(provider: &str, request: &ChatRequest) -> anyhow::Result<Self> {
        let mut result = Self {
            canonical_to_wire: BTreeMap::new(),
            wire_to_canonical: BTreeMap::new(),
            advertised: BTreeSet::new(),
        };
        let tools = request.tools.as_deref().unwrap_or_default();
        if tools.len() > 256 {
            return Err(super::schema::rejected());
        }
        let mut bytes = 0usize;
        for tool in tools {
            bytes += super::schema::validate(provider, &tool.parameters)?;
            if bytes > super::schema::MAX_MANIFEST_BYTES {
                return Err(super::schema::rejected());
            }
            result.insert(&tool.name)?;
            if !result.advertised.insert(tool.name.clone()) {
                return Err(super::schema::rejected());
            }
        }
        for message in &request.messages {
            if message.role == crate::Role::Tool
                && let Some(name) = &message.name
            {
                result.insert(name)?;
            }
            for call in message.tool_calls.iter().flatten() {
                result.insert(&call.name)?;
            }
        }
        if let Some(ToolChoice::Tool { name }) = &request.tool_choice
            && !result.advertised.contains(name)
        {
            return Err(super::schema::rejected());
        }
        Ok(result)
    }

    fn insert(&mut self, name: &str) -> anyhow::Result<()> {
        if name.is_empty() || name.len() > 4096 {
            return Err(super::schema::rejected());
        }
        if self.canonical_to_wire.contains_key(name) {
            return Ok(());
        }
        if self.canonical_to_wire.len() >= 2048 {
            return Err(super::schema::rejected());
        }
        let wire = alias(name);
        if self
            .wire_to_canonical
            .get(&wire)
            .is_some_and(|old| old != name)
        {
            return Err(super::schema::rejected());
        }
        self.wire_to_canonical.insert(wire.clone(), name.to_owned());
        self.canonical_to_wire.insert(name.to_owned(), wire);
        Ok(())
    }

    fn project_name(&self, name: &mut String) -> anyhow::Result<()> {
        *name = self
            .canonical_to_wire
            .get(name)
            .ok_or_else(super::schema::rejected)?
            .clone();
        Ok(())
    }

    pub(crate) fn project(&self, request: &mut ChatRequest) -> anyhow::Result<()> {
        for tool in request.tools.iter_mut().flatten() {
            self.project_name(&mut tool.name)?;
        }
        if let Some(ToolChoice::Tool { name }) = &mut request.tool_choice {
            self.project_name(name)?;
        }
        for message in &mut request.messages {
            if message.role == crate::Role::Tool
                && let Some(name) = &mut message.name
            {
                self.project_name(name)?;
            }
            for call in message.tool_calls.iter_mut().flatten() {
                self.project_name(&mut call.name)?;
            }
            validate_replay_names(message)?;
        }
        Ok(())
    }

    pub(crate) fn restore_request(&self, request: &mut ChatRequest) -> anyhow::Result<()> {
        for tool in request.tools.iter_mut().flatten() {
            self.restore_name(&mut tool.name, false)?;
        }
        if let Some(ToolChoice::Tool { name }) = &mut request.tool_choice {
            self.restore_name(name, false)?;
        }
        for message in &mut request.messages {
            if message.role == crate::Role::Tool
                && let Some(name) = &mut message.name
            {
                self.restore_name(name, false)?;
            }
            for call in message.tool_calls.iter_mut().flatten() {
                self.restore_name(&mut call.name, false)?;
            }
        }
        Ok(())
    }

    fn restore_name(&self, name: &mut String, advertised: bool) -> anyhow::Result<()> {
        let rejected = || crate::failure::ProviderTransportFailure {
            classification: crate::ProviderFailureClassification::new(
                pioneer_protocol::ProviderFailureClass::ProviderRejected,
            ),
        };
        let canonical = self.wire_to_canonical.get(name).ok_or_else(rejected)?;
        if advertised && !self.advertised.contains(canonical) {
            return Err(rejected().into());
        }
        *name = canonical.clone();
        Ok(())
    }

    pub(crate) fn restore_calls(&self, calls: &mut [ProviderToolCall]) -> anyhow::Result<()> {
        for call in calls {
            self.restore_name(&mut call.name, true)?;
        }
        Ok(())
    }
}

fn validate_replay_names(message: &ChatMessage) -> anyhow::Result<()> {
    let Some(state) = &message.provider_replay_state else {
        return Ok(());
    };
    let payload = &state.payload;
    let names: Vec<&str> = if let Some(parts) = payload.get("parts").and_then(|v| v.as_array()) {
        parts
            .iter()
            .filter_map(|v| v.get("functionCall")?.get("name")?.as_str())
            .collect()
    } else if let Some(blocks) = payload
        .get("native_content")
        .or_else(|| payload.get("blocks"))
        .and_then(|v| v.as_array())
    {
        blocks
            .iter()
            .filter_map(|v| {
                v.get("toolUse")
                    .and_then(|v| v.get("name"))
                    .and_then(|v| v.as_str())
                    .or_else(|| {
                        (v.get("type").and_then(|v| v.as_str()) == Some("tool_use"))
                            .then(|| v.get("name")?.as_str())
                            .flatten()
                    })
            })
            .collect()
    } else if let Some(calls) = payload
        .get("assistant_message")
        .and_then(|v| v.get("tool_calls"))
        .and_then(|v| v.as_array())
    {
        calls
            .iter()
            .filter_map(|v| v.get("function")?.get("name")?.as_str())
            .collect()
    } else {
        Vec::new()
    };
    // Native block order must remain paired with the visible canonical calls.
    // Legacy state that needs a name rewrite gets an explicit safe outcome.
    if !names.is_empty()
        && !names
            .iter()
            .copied()
            .eq(message.tool_calls.iter().flatten().map(|c| c.name.as_str()))
    {
        return Err(super::schema::rejected());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChatMessage, ToolDefinition};
    fn request(names: &[&str]) -> ChatRequest {
        ChatRequest {
            model: "fixture".into(),
            messages: vec![ChatMessage::user("use tools")],
            temperature: None,
            max_tokens: None,
            tools: Some(
                names
                    .iter()
                    .map(|name| ToolDefinition {
                        name: (*name).into(),
                        description: "fixture".into(),
                        parameters: serde_json::json!({"type":"object"}),
                    })
                    .collect(),
            ),
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        }
    }
    #[test]
    fn alias_is_legal_deterministic_and_bijective_for_choice_history_and_returns() {
        let long = format!("skill.17.{}", "long-slug".repeat(30));
        let mut request = request(&["plain", "skill.12.render", &long]);
        request.tool_choice = Some(ToolChoice::Tool {
            name: "skill.12.render".into(),
        });
        let call = ProviderToolCall {
            id: "opaque-id".into(),
            name: "skill.12.render".into(),
            arguments: "{}".into(),
        };
        request.messages.push(ChatMessage::assistant_tool_calls(
            None::<String>,
            vec![call.clone()],
        ));
        request
            .messages
            .push(ChatMessage::tool_result(&call.id, &call.name, "ok"));
        let original = request.clone();
        let budget = original.tool_wire_budget_view().unwrap();
        assert_eq!(budget.tools.as_ref().unwrap()[1].name.len(), 64);
        assert_eq!(original.tools.as_ref().unwrap()[1].name, "skill.12.render");
        let manifest = ToolWireManifest::new("openai", &request).unwrap();
        manifest.project(&mut request).unwrap();
        let aliases: Vec<_> = request
            .tools
            .as_ref()
            .unwrap()
            .iter()
            .map(|t| t.name.clone())
            .collect();
        assert_eq!(aliases[0], "plain");
        assert!(aliases.iter().all(|n| legal(n)));
        assert_ne!(aliases[1], aliases[2]);
        assert!(
            matches!(request.tool_choice.as_ref(),Some(ToolChoice::Tool {name}) if name == &aliases[1])
        );
        assert_eq!(
            request.messages[1].tool_calls.as_ref().unwrap()[0].name,
            aliases[1]
        );
        assert_eq!(request.messages[2].name.as_ref(), Some(&aliases[1]));
        let mut returned = vec![ProviderToolCall {
            name: aliases[1].clone(),
            ..call.clone()
        }];
        manifest.restore_calls(&mut returned).unwrap();
        assert_eq!(returned, vec![call]);
        manifest.restore_request(&mut request).unwrap();
        assert_eq!(request.messages, original.messages);
        assert_eq!(
            serde_json::to_value(&request.tool_choice).unwrap(),
            serde_json::to_value(&original.tool_choice).unwrap()
        );
        assert_eq!(
            request
                .tools
                .as_ref()
                .unwrap()
                .iter()
                .map(|t| &t.name)
                .collect::<Vec<_>>(),
            original
                .tools
                .as_ref()
                .unwrap()
                .iter()
                .map(|t| &t.name)
                .collect::<Vec<_>>()
        );
        let mut cold = original.clone();
        cold.messages =
            serde_json::from_value(serde_json::to_value(original.messages).unwrap()).unwrap();
        let cold_manifest = ToolWireManifest::new("gemini", &cold).unwrap();
        assert_eq!(manifest.canonical_to_wire, cold_manifest.canonical_to_wire);
    }
    #[test]
    fn returned_history_only_tool_is_a_provider_rejection_not_a_schema_error() {
        let mut request = request(&[]);
        request.messages.push(ChatMessage::assistant_tool_calls(
            None::<String>,
            vec![ProviderToolCall {
                id: "prior-call".into(),
                name: "read".into(),
                arguments: "{}".into(),
            }],
        ));
        let manifest = ToolWireManifest::new("openai", &request).unwrap();
        let mut calls = vec![ProviderToolCall {
            id: "undeclared-call".into(),
            name: "read".into(),
            arguments: "{}".into(),
        }];
        let error = manifest.restore_calls(&mut calls).unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<crate::failure::ProviderTransportFailure>()
                .unwrap()
                .classification
                .class,
            pioneer_protocol::ProviderFailureClass::ProviderRejected
        );
    }

    #[test]
    fn reserved_alias_collision_duplicate_and_unknown_return_fail_closed() {
        let reserved = alias("skill.12.render");
        assert!(
            ToolWireManifest::new("openai", &request(&["skill.12.render", &reserved])).is_err()
        );
        assert!(ToolWireManifest::new("openai", &request(&["read", "read"])).is_err());
        let manifest = ToolWireManifest::new("openai", &request(&["read"])).unwrap();
        let mut calls = vec![ProviderToolCall {
            id: "call".into(),
            name: "other".into(),
            arguments: "{}".into(),
        }];
        let error = manifest.restore_calls(&mut calls).unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<crate::failure::ProviderTransportFailure>()
                .unwrap()
                .classification
                .class,
            pioneer_protocol::ProviderFailureClass::ProviderRejected
        );
    }
    #[test]
    fn native_signed_names_are_checked_without_editing_opaque_payload() {
        for (provider, payload) in [
            (
                "gemini",
                serde_json::json!({"schema_version":2,"parts":[{"functionCall":{"name":alias("skill.1.run"),"args":{}},"thoughtSignature":"opaque"}]}),
            ),
            (
                "anthropic",
                serde_json::json!({"schema_version":2,"blocks":[{"type":"thinking","signature":"opaque"},{"type":"tool_use","name":alias("skill.1.run"),"id":"call","input":{}}]}),
            ),
            (
                "bedrock",
                serde_json::json!({"native_content":[{"toolUse":{"name":alias("skill.1.run"),"toolUseId":"call","input":{}}}]}),
            ),
        ] {
            let mut request = request(&["skill.1.run"]);
            let mut message = ChatMessage::assistant_tool_calls(
                None::<String>,
                vec![ProviderToolCall {
                    id: "call".into(),
                    name: "skill.1.run".into(),
                    arguments: "{}".into(),
                }],
            );
            message.provider_replay_state =
                Some(crate::ProviderReplayState::new(provider, payload));
            let original = message.provider_replay_state.clone();
            request.messages.push(message);
            ToolWireManifest::new(provider, &request)
                .unwrap()
                .project(&mut request)
                .unwrap();
            assert_eq!(request.messages[1].provider_replay_state, original);
        }
        let mut legacy = request(&["skill.1.run"]);
        let mut message = ChatMessage::assistant_tool_calls(
            None::<String>,
            vec![ProviderToolCall {
                id: "call".into(),
                name: "skill.1.run".into(),
                arguments: "{}".into(),
            }],
        );
        message.provider_replay_state = Some(crate::ProviderReplayState::new(
            "gemini",
            serde_json::json!({"parts":[{"functionCall":{"name":"skill.1.run"},"thoughtSignature":"opaque"}]}),
        ));
        legacy.messages.push(message);
        assert!(
            ToolWireManifest::new("gemini", &legacy)
                .unwrap()
                .project(&mut legacy)
                .is_err()
        );
    }
}
