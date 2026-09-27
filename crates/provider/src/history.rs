//! Provider-specific continuation state is durable canonical data, but only its
//! owner may put it back on the wire.  This module derives an outbound view of
//! completed history without changing the stored messages.

use crate::{
    CanonicalProviderRoundEnvelope, ChatMessage, ChatRequest, ProviderReplayState, ReasoningConfig,
    Role,
};
use anyhow::{Result, anyhow};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

type UnitKey = (String, String, String, String);

#[derive(Default)]
struct UnitCompletion {
    attributed: bool,
    complete: bool,
    calls: BTreeMap<(String, String), usize>,
    results: BTreeMap<(String, String), usize>,
}

fn unit_key(message: &ChatMessage) -> Option<UnitKey> {
    let origin = message.provenance.as_ref()?;
    Some((
        origin.workspace_id.clone(),
        origin.thread_id.clone(),
        origin.context_thread.clone().unwrap_or_default(),
        origin.unit_id.clone(),
    ))
}

fn completed_message_indexes(messages: &[ChatMessage]) -> BTreeSet<usize> {
    let mut units: BTreeMap<UnitKey, UnitCompletion> = BTreeMap::new();
    let mut keys = Vec::with_capacity(messages.len());

    for message in messages {
        let Some(origin) = message.provenance.as_ref() else {
            keys.push(None);
            continue;
        };
        let key = unit_key(message).expect("provenance was checked");
        let unit = units.entry(key.clone()).or_insert_with(|| UnitCompletion {
            attributed: true,
            complete: true,
            ..Default::default()
        });
        unit.complete &= origin.complete
            && !origin.unit_id.is_empty()
            && !origin.sources.is_empty()
            && origin.sources.iter().all(|source| {
                !source.scope.is_empty() && !source.id.is_empty() && !source.version.is_empty()
            });
        for call in message.tool_calls.iter().flatten() {
            *unit
                .calls
                .entry((call.id.clone(), call.name.clone()))
                .or_default() += 1;
        }
        if message.role == Role::Tool {
            match (message.tool_call_id.as_ref(), message.name.as_ref()) {
                (Some(id), Some(name)) => {
                    *unit.results.entry((id.clone(), name.clone())).or_default() += 1;
                }
                _ => unit.complete = false,
            }
        }
        keys.push(Some(key));
    }

    units.values_mut().for_each(|unit| {
        unit.complete &= unit.attributed && unit.calls == unit.results;
    });
    keys.into_iter()
        .enumerate()
        .filter_map(|(index, key)| {
            key.and_then(|key| units.get(&key))
                .is_some_and(|unit| unit.complete)
                .then_some(index)
        })
        .collect()
}

#[derive(Deserialize)]
struct CompatibleReplay {
    schema_version: u32,
    assistant_message: CompatibleReplayMessage,
}

#[derive(Deserialize)]
struct CompatibleReplayMessage {
    reasoning_content: Option<String>,
}

fn string_at(value: &serde_json::Value, path: &[&str]) -> Option<String> {
    let mut value = value;
    for component in path {
        value = value.get(*component)?;
    }
    value
        .as_str()
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

/// Returns only provider-declared, human-readable reasoning. Opaque signatures,
/// encrypted blocks and tokens deliberately have no portable representation.
fn portable_reasoning(state: &ProviderReplayState) -> Option<String> {
    if let Ok(replay) = serde_json::from_value::<CompatibleReplay>(state.payload.clone())
        && replay.schema_version == 1
        && let Some(reasoning) = replay.assistant_message.reasoning_content
        && !reasoning.is_empty()
    {
        return Some(reasoning);
    }

    let parts = match state.provider.as_str() {
        "anthropic" => state
            .payload
            .get("blocks")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter(|block| {
                block.get("type").and_then(serde_json::Value::as_str) == Some("thinking")
            })
            .filter_map(|block| {
                string_at(block, &["thinking"]).or_else(|| string_at(block, &["text"]))
            })
            .collect::<Vec<_>>(),
        "bedrock" => state
            .payload
            .get("blocks")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|block| {
                string_at(block, &["reasoningText", "text"])
                    .or_else(|| string_at(block, &["reasoning_text", "text"]))
            })
            .collect::<Vec<_>>(),
        "openrouter" => state
            .payload
            .get("reasoning_details")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(
                |detail| match detail.get("type").and_then(serde_json::Value::as_str) {
                    Some("reasoning.summary") => string_at(detail, &["summary"]),
                    Some("reasoning.text") => string_at(detail, &["text"]),
                    _ => None,
                },
            )
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    // Anthropic and Bedrock producers concatenate readable thinking blocks
    // without a separator when populating common reasoning_content.
    (!parts.is_empty()).then(|| {
        parts.join(
            if matches!(state.provider.as_str(), "anthropic" | "bedrock") {
                ""
            } else {
                "\n"
            },
        )
    })
}

fn remove_provider_state(message: &mut ChatMessage) {
    let common = message
        .reasoning_content
        .as_deref()
        .filter(|text| !text.trim().is_empty());
    let replay = message
        .provider_replay_state
        .as_ref()
        .and_then(portable_reasoning)
        .filter(|text| !text.trim().is_empty());
    message.reasoning_content = match (common, replay.as_deref()) {
        (Some(common), Some(replay)) if common.trim() != replay.trim() => {
            Some(format!("{common}\n\n{replay}"))
        }
        (Some(common), _) => Some(common.to_owned()),
        (None, Some(replay)) => Some(replay.to_owned()),
        (None, None) => None,
    };
    message.provider_replay_state = None;
}

/// Portable view for a completed historical message (including CLI history
/// rendering and typed historical observations). This never mutates storage.
pub fn portable_history_message(message: &ChatMessage) -> ChatMessage {
    let mut portable = message.clone();
    remove_provider_state(&mut portable);
    portable
}

#[derive(Debug)]
pub struct IncompatibleProviderReplayContinuation {
    source_provider: String,
    source_model: Option<String>,
    target_provider: String,
    target_model: String,
}

impl fmt::Display for IncompatibleProviderReplayContinuation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid request: active provider replay state from `{}/{}` cannot be continued by `{}/{}`",
            self.source_provider,
            self.source_model.as_deref().unwrap_or("unknown-model"),
            self.target_provider,
            self.target_model
        )
    }
}

impl std::error::Error for IncompatibleProviderReplayContinuation {}

const PORTABLE_REASONING_PREFIX: &str = "[Historical reasoning; portable unsigned text]\n";
const PORTABLE_TOOL_ROUND_PREFIX: &str =
    "[Completed historical tool round; portable non-executable transcript]\n";

fn target_serializes_common_reasoning(provider: &str) -> bool {
    !matches!(provider, "anthropic" | "bedrock" | "gemini")
}

fn expose_reasoning_as_content(message: &mut ChatMessage) {
    let Some(reasoning) = message.reasoning_content.take() else {
        return;
    };
    if reasoning.is_empty() {
        return;
    }
    let portable = format!("{PORTABLE_REASONING_PREFIX}{reasoning}");
    if message.content.is_empty() {
        message.content = portable;
    } else {
        message.content = format!("{portable}\n\n{}", message.content);
    }
}

#[derive(serde::Serialize)]
struct PortableToolRoundMetadata<'a> {
    role: &'a Role,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<&'a [crate::ProviderToolCall]>,
}

fn portable_tool_round_message(message: &ChatMessage) -> Result<ChatMessage> {
    let metadata = PortableToolRoundMetadata {
        role: &message.role,
        tool_call_id: message.tool_call_id.as_deref(),
        name: message.name.as_deref(),
        tool_calls: message.tool_calls.as_deref(),
    };
    // Keep each original message in its original position. In particular a
    // user message between a call and its result must not be pulled into a
    // single transcript at the call site. Typed parts remain typed so the
    // ordinary attachment preflight can materialize and budget them.
    let mut portable = ChatMessage::user(format!(
        "{PORTABLE_TOOL_ROUND_PREFIX}{}\n{}",
        serde_json::to_string(&metadata)?,
        message.content
    ));
    portable.content_parts = message.content_parts.clone();
    portable.provenance = message.provenance.clone();
    Ok(portable)
}

pub(crate) fn deepseek_thinking_required(
    model: &str,
    reasoning_enabled: bool,
    messages: &[ChatMessage],
) -> bool {
    model.to_ascii_lowercase().contains("reasoner")
        || reasoning_enabled
        || messages.iter().any(|message| {
            message.role == Role::Assistant
                && (message
                    .reasoning_content
                    .as_deref()
                    .is_some_and(|text| !text.trim().is_empty())
                    || message.provider_replay_state.as_ref().is_some_and(|state| {
                        state.is_compatible_with("deepseek", model)
                            && serde_json::from_value::<CompatibleReplay>(state.payload.clone())
                                .is_ok_and(|replay| {
                                    replay.schema_version == 1
                                        && replay.assistant_message.reasoning_content.is_some()
                                })
                    }))
        })
}

/// Render a verified canonical provider-round source as portable summarizer
/// material. The source bytes remain the integrity authority; this projection
/// is created only after loading those exact bytes and is never persisted back.
pub fn portable_history_payload(payload: &str) -> String {
    let Ok(mut envelope) = serde_json::from_str::<CanonicalProviderRoundEnvelope>(payload) else {
        return payload.to_owned();
    };
    remove_provider_state(&mut envelope.message);
    serde_json::to_string(&envelope).unwrap_or_else(|_| payload.to_owned())
}

/// The accepted legacy Task basis is a typed array, not arbitrary user JSON.
/// Its original bytes remain authoritative for source identity and integrity.
pub fn portable_task_basis_payload(payload: &str) -> Result<String> {
    let messages: Vec<ChatMessage> = serde_json::from_str(payload)?;
    Ok(serde_json::to_string(
        &messages
            .iter()
            .map(portable_history_message)
            .collect::<Vec<_>>(),
    )?)
}

/// Build the representation that a selected provider will actually receive.
/// Canonical messages remain untouched, including their provider-owned replay.
pub fn project_messages_for_provider(
    provider: &str,
    model: &str,
    messages: &[ChatMessage],
) -> Result<Vec<ChatMessage>> {
    project_messages(provider, model, false, messages)
}

fn project_messages(
    provider: &str,
    model: &str,
    reasoning_enabled: bool,
    messages: &[ChatMessage],
) -> Result<Vec<ChatMessage>> {
    let completed = completed_message_indexes(messages);
    let mut projected = messages
        .iter()
        .enumerate()
        .map(|(index, message)| -> Result<_> {
            let mut projected = message.clone();
            if let Some(state) = projected.provider_replay_state.as_ref()
                && !state.is_compatible_with(provider, model)
            {
                if !completed.contains(&index) {
                    return Err(anyhow!(IncompatibleProviderReplayContinuation {
                        source_provider: state.provider.clone(),
                        source_model: state.model.clone(),
                        target_provider: provider.to_owned(),
                        target_model: model.to_owned(),
                    }));
                }
                remove_provider_state(&mut projected);
            }
            if completed.contains(&index)
                && !target_serializes_common_reasoning(provider)
                && projected
                    .reasoning_content
                    .as_deref()
                    .is_some_and(|common| {
                        !common.trim().is_empty()
                            && projected
                                .provider_replay_state
                                .as_ref()
                                .and_then(portable_reasoning)
                                .is_none_or(|replay| replay.trim() != common.trim())
                    })
            {
                expose_reasoning_as_content(&mut projected);
            }
            Ok(projected)
        })
        .collect::<Result<Vec<_>>>()?;

    let deepseek_thinking =
        provider == "deepseek" && deepseek_thinking_required(model, reasoning_enabled, &projected);
    if !deepseek_thinking {
        return Ok(projected);
    }

    for (index, message) in projected.iter_mut().enumerate() {
        if completed.contains(&index)
            && message.role == Role::Assistant
            && message
                .tool_calls
                .as_ref()
                .is_some_and(|calls| !calls.is_empty())
            && message.provider_replay_state.as_ref().is_some_and(|state| {
                state.is_compatible_with(provider, model)
                    && !serde_json::from_value::<CompatibleReplay>(state.payload.clone()).is_ok_and(
                        |replay| {
                            replay.schema_version == 1
                                && replay.assistant_message.reasoning_content.is_some()
                        },
                    )
            })
        {
            // Same provider/model is not sufficient for a non-thinking round
            // in a thinking request. Completed history can become a portable
            // non-executable transcript; an active continuation cannot.
            remove_provider_state(message);
        }
    }

    // A legacy Task basis can attribute several independent provider rounds to
    // one provenance unit. Select calls and their results by call identity,
    // without changing the unit (and therefore its coverage or ownership).
    let mut collapse = BTreeSet::<usize>::new();
    let mut results = BTreeMap::<(UnitKey, String, String), Vec<usize>>::new();
    for (index, message) in projected.iter().enumerate() {
        if completed.contains(&index)
            && message.role == Role::Tool
            && let (Some(key), Some(id), Some(name)) = (
                unit_key(message),
                message.tool_call_id.as_ref(),
                message.name.as_ref(),
            )
        {
            results
                .entry((key, id.clone(), name.clone()))
                .or_default()
                .push(index);
        }
    }
    let mut call_ordinals = BTreeMap::<(UnitKey, String, String), usize>::new();
    for (index, message) in projected.iter().enumerate() {
        if !completed.contains(&index) || message.role != Role::Assistant {
            continue;
        }
        let (Some(key), Some(calls)) = (unit_key(message), message.tool_calls.as_ref()) else {
            continue;
        };
        if calls.is_empty() {
            continue;
        }
        let transcript = message.provider_replay_state.is_none()
            && message
                .reasoning_content
                .as_deref()
                .is_none_or(|reasoning| reasoning.trim().is_empty());
        if transcript {
            collapse.insert(index);
        }
        for call in calls {
            let identity = (key.clone(), call.id.clone(), call.name.clone());
            let ordinal = call_ordinals.entry(identity.clone()).or_default();
            if transcript
                && let Some(result_index) = results.get(&identity).and_then(|v| v.get(*ordinal))
            {
                collapse.insert(*result_index);
            }
            *ordinal += 1;
        }
    }
    if collapse.is_empty() {
        return Ok(projected);
    }

    projected
        .drain(..)
        .enumerate()
        .map(|(index, message)| {
            if collapse.contains(&index) {
                portable_tool_round_message(&message)
            } else {
                Ok(message)
            }
        })
        .collect()
}

pub fn project_request_for_provider(
    provider: &str,
    mut request: ChatRequest,
) -> Result<ChatRequest> {
    let reasoning_enabled = matches!(request.reasoning, Some(ReasoningConfig::Effort(_)));
    request.messages = project_messages(
        provider,
        request.model.as_str(),
        reasoning_enabled,
        &request.messages,
    )?;
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MessageProvenance, MessageSourceRef, ProviderToolCall};

    fn complete(message: &mut ChatMessage, unit: &str) {
        message.provenance = Some(MessageProvenance {
            logical_turn_id: Some("turn".into()),
            workspace_id: "workspace".into(),
            thread_id: "thread".into(),
            context_thread: None,
            unit_id: unit.into(),
            sources: vec![MessageSourceRef {
                scope: "event:turn".into(),
                id: format!("{unit}:source"),
                version: "revision:1".into(),
            }],
            complete: true,
            protected_input: false,
            inherited: false,
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
        });
    }

    #[test]
    fn foreign_completed_replay_becomes_portable_without_mutating_canonical_message() {
        let replay = ProviderReplayState::new(
            "deepseek",
            serde_json::json!({
                "schema_version": 1,
                "assistant_message": {
                    "content": null,
                    "reasoning_content": "inspect before calling",
                    "tool_calls": []
                }
            }),
        );
        let mut assistant = ChatMessage::assistant("done");
        assistant.provider_replay_state = Some(replay.clone());
        complete(&mut assistant, "answer");

        let projected =
            project_messages_for_provider("openrouter", "target-model", &[assistant.clone()])
                .unwrap();
        assert_eq!(
            projected[0].reasoning_content.as_deref(),
            Some("inspect before calling")
        );
        assert!(projected[0].provider_replay_state.is_none());
        assert_eq!(assistant.provider_replay_state.as_ref(), Some(&replay));
        assert!(assistant.reasoning_content.is_none());
        assert_eq!(
            project_messages_for_provider("openrouter", "target-model", &projected).unwrap(),
            projected,
            "repeated adaptation must not accumulate reasoning or messages"
        );
    }

    #[test]
    fn typed_legacy_task_basis_keeps_order_and_readable_reasoning_without_opaque_replay() {
        let mut assistant = ChatMessage::assistant("saved answer");
        assistant.provider_replay_state = Some(ProviderReplayState::for_model(
            "openrouter",
            "source-model",
            serde_json::json!({"reasoning_details":[
                {"type":"reasoning.summary","summary":"readable rationale"},
                {"type":"reasoning.encrypted","data":"opaque-token"}
            ]}),
        ));
        let original = serde_json::to_string(&vec![
            ChatMessage::user("question"),
            assistant,
            ChatMessage::assistant("final result"),
        ])
        .unwrap();
        let projected = portable_task_basis_payload(&original).unwrap();
        let messages: Vec<ChatMessage> = serde_json::from_str(&projected).unwrap();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].content, "question");
        assert_eq!(
            messages[1].reasoning_content.as_deref(),
            Some("readable rationale")
        );
        assert_eq!(messages[2].content, "final result");
        assert!(
            messages
                .iter()
                .all(|message| message.provider_replay_state.is_none())
        );
        assert!(!projected.contains("opaque-token"));
        assert_eq!(portable_task_basis_payload(&projected).unwrap(), projected);
        assert!(original.contains("opaque-token"));
    }

    #[test]
    fn compatible_replay_is_exact_and_incomplete_foreign_tool_round_is_fail_closed() {
        let replay = ProviderReplayState::for_model(
            "deepseek",
            "deepseek-chat",
            serde_json::json!({"opaque":"exact"}),
        );
        let mut assistant = ChatMessage::assistant_tool_calls_with_provider_state(
            None::<String>,
            Some("reasoning"),
            vec![ProviderToolCall {
                id: "call".into(),
                name: "read".into(),
                arguments: "{}".into(),
            }],
            Some(replay.clone()),
        );
        complete(&mut assistant, "round");

        let same = project_messages_for_provider("deepseek", "deepseek-chat", &[assistant.clone()])
            .unwrap();
        assert_eq!(same[0].provider_replay_state.as_ref(), Some(&replay));
        let error = project_messages_for_provider("openrouter", "model", &[assistant.clone()])
            .expect_err("a call without its result is not a completed unit");
        assert!(
            error
                .downcast_ref::<IncompatibleProviderReplayContinuation>()
                .is_some()
        );

        let mut result = ChatMessage::tool_result("call", "read", "result");
        complete(&mut result, "round");
        let foreign =
            project_messages_for_provider("openrouter", "model", &[assistant, result]).unwrap();
        assert!(foreign[0].provider_replay_state.is_none());
        assert_eq!(foreign[1].content, "result");
    }

    #[test]
    fn portable_reasoning_combines_distinct_common_and_replay_text_once() {
        for (common, expected) in [
            (None, Some("replay")),
            (Some(""), Some("replay")),
            (Some("  \t"), Some("replay")),
            (Some("replay"), Some("replay")),
            (Some(" replay "), Some(" replay ")),
            (Some("common"), Some("common\n\nreplay")),
        ] {
            let mut message = ChatMessage::assistant("answer");
            message.reasoning_content = common.map(str::to_owned);
            message.provider_replay_state = Some(ProviderReplayState::for_model(
                "openrouter",
                "source-model",
                serde_json::json!({"reasoning_details":[
                    {"type":"reasoning.encrypted","data":"opaque"},
                    {"type":"reasoning.summary","summary":"replay"}
                ]}),
            ));
            complete(&mut message, "answer");
            let projected =
                project_messages_for_provider("deepseek", "deepseek-chat", &[message]).unwrap();
            assert_eq!(projected[0].reasoning_content.as_deref(), expected);
            assert!(projected[0].provider_replay_state.is_none());
            assert_eq!(
                project_messages_for_provider("deepseek", "deepseek-chat", &projected).unwrap(),
                projected
            );
        }
    }

    #[test]
    fn anthropic_and_bedrock_multiblock_reasoning_matches_producer_concatenation() {
        for (provider, payload) in [
            (
                "anthropic",
                serde_json::json!({"blocks":[
                    {"type":"thinking","thinking":"first ","signature":"opaque-one"},
                    {"type":"thinking","thinking":"second","signature":"opaque-two"}
                ]}),
            ),
            (
                "bedrock",
                serde_json::json!({"blocks":[
                    {"reasoningText":{"text":"first ","signature":"opaque-one"}},
                    {"reasoningText":{"text":"second","signature":"opaque-two"}}
                ]}),
            ),
        ] {
            let mut canonical = ChatMessage::assistant("answer");
            canonical.reasoning_content = Some("first second".into());
            canonical.provider_replay_state = Some(ProviderReplayState::for_model(
                provider,
                "source-model",
                payload,
            ));
            complete(&mut canonical, "answer");
            let portable = portable_history_message(&canonical);
            assert_eq!(portable.reasoning_content.as_deref(), Some("first second"));
            assert!(portable.provider_replay_state.is_none());
            assert_eq!(portable_history_message(&portable), portable);
            let outbound =
                project_messages_for_provider("openrouter", "target", &[canonical.clone()])
                    .unwrap();
            assert_eq!(
                outbound[0].reasoning_content.as_deref(),
                Some("first second")
            );
            assert_eq!(canonical.reasoning_content.as_deref(), Some("first second"));

            canonical.reasoning_content = Some("additional common".into());
            let extra = portable_history_message(&canonical);
            assert_eq!(
                extra.reasoning_content.as_deref(),
                Some("additional common\n\nfirst second")
            );
            assert!(
                !serde_json::to_string(&extra)
                    .unwrap()
                    .contains("opaque-one")
            );
        }
    }

    #[test]
    fn completed_interleaved_units_keep_order_sources_and_typed_parts() {
        use crate::{AttachmentDataSource, MessageAttachment, MessageContentPart};
        let replay = ProviderReplayState::for_model(
            "openrouter",
            "source-model",
            serde_json::json!({"reasoning_details":[]}),
        );
        let call = |id: &str, unit: &str| {
            let mut message = ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                None::<String>,
                vec![ProviderToolCall {
                    id: id.into(),
                    name: "inspect".into(),
                    arguments: "{}".into(),
                }],
                Some(replay.clone()),
            );
            complete(&mut message, unit);
            message
        };
        let result = |id: &str, unit: &str| {
            let mut message = ChatMessage::tool_result(id, "inspect", format!("result {id}"));
            complete(&mut message, unit);
            message
        };
        let mut with_image = result("a", "unit-a");
        with_image
            .content_parts
            .push(MessageContentPart::image(MessageAttachment {
                mime_type: "image/png".into(),
                name: Some("snapshot.png".into()),
                size_bytes: Some(4),
                sha256: None,
                source: AttachmentDataSource::Bytes {
                    base64_data: "AQIDBA==".into(),
                },
                artifact: None,
            }));
        let canonical = vec![
            call("a", "unit-a"),
            ChatMessage::user("steer"),
            call("b", "unit-b"),
            with_image,
            result("b", "unit-b"),
        ];
        let projected =
            project_messages_for_provider("deepseek", "deepseek-reasoner", &canonical).unwrap();
        assert_eq!(projected.len(), canonical.len());
        assert_eq!(projected[1], canonical[1]);
        assert!(projected[0].content.contains("\"id\":\"a\""));
        assert!(projected[2].content.contains("\"id\":\"b\""));
        assert!(projected[3].content.contains("result a"));
        assert!(projected[4].content.contains("result b"));
        for index in [0, 2, 3, 4] {
            assert_eq!(projected[index].provenance, canonical[index].provenance);
            assert!(projected[index].tool_calls.is_none());
            assert!(projected[index].tool_call_id.is_none());
        }
        assert_eq!(projected[3].content_parts, canonical[3].content_parts);
        assert!(!projected[3].content.contains("AQIDBA=="));
        assert_eq!(
            project_messages_for_provider("deepseek", "deepseek-reasoner", &projected).unwrap(),
            projected
        );
        assert_eq!(canonical[0].provider_replay_state.as_ref(), Some(&replay));
    }

    #[test]
    fn model_change_and_legacy_state_require_portable_completed_history() {
        let scoped = ProviderReplayState::for_model(
            "openrouter",
            "source-model",
            serde_json::json!({"opaque":"exact"}),
        );
        let mut completed = ChatMessage::assistant("answer");
        completed.provider_replay_state = Some(scoped.clone());
        complete(&mut completed, "answer");
        let changed =
            project_messages_for_provider("openrouter", "different-model", &[completed.clone()])
                .unwrap();
        assert!(changed[0].provider_replay_state.is_none());
        assert_eq!(completed.provider_replay_state.as_ref(), Some(&scoped));

        let mut active = completed.clone();
        active.provenance = None;
        assert!(
            project_messages_for_provider("openrouter", "different-model", &[active])
                .unwrap_err()
                .downcast_ref::<IncompatibleProviderReplayContinuation>()
                .is_some()
        );

        let legacy = ProviderReplayState::new("openrouter", serde_json::json!({"legacy":true}));
        let mut completed_legacy = ChatMessage::assistant("legacy answer");
        completed_legacy.provider_replay_state = Some(legacy.clone());
        complete(&mut completed_legacy, "legacy");
        let projected =
            project_messages_for_provider("openrouter", "any-model", &[completed_legacy]).unwrap();
        assert!(projected[0].provider_replay_state.is_none());

        let mut active_legacy = ChatMessage::assistant("partial");
        active_legacy.provider_replay_state = Some(legacy);
        assert!(
            project_messages_for_provider("openrouter", "any-model", &[active_legacy])
                .unwrap_err()
                .downcast_ref::<IncompatibleProviderReplayContinuation>()
                .is_some()
        );
    }

    #[test]
    fn content_only_reasoning_projection_is_idempotent_and_keeps_canonical_state() {
        let replay = ProviderReplayState::for_model(
            "openrouter",
            "source-model",
            serde_json::json!({"reasoning_details":[{
                "type":"reasoning.summary","summary":"meaningful rationale"
            }]}),
        );
        let mut canonical = ChatMessage::assistant("answer");
        canonical.provider_replay_state = Some(replay.clone());
        complete(&mut canonical, "answer");

        let once =
            project_messages_for_provider("anthropic", "claude", &[canonical.clone()]).unwrap();
        let twice = project_messages_for_provider("anthropic", "claude", &once).unwrap();
        assert_eq!(once, twice);
        assert!(once[0].content.contains("meaningful rationale"));
        assert!(once[0].reasoning_content.is_none());
        assert_eq!(canonical.provider_replay_state.as_ref(), Some(&replay));
        assert_eq!(canonical.content, "answer");
    }

    #[test]
    fn completed_tool_round_keeps_calls_results_and_order_while_dropping_only_foreign_state() {
        let replay = ProviderReplayState::new(
            "openrouter",
            serde_json::json!({
                "reasoning_details":[
                    {"type":"reasoning.encrypted","data":"do-not-render"},
                    {"type":"reasoning.summary","summary":"portable summary"}
                ]
            }),
        );
        let mut assistant = ChatMessage::assistant_tool_calls_with_provider_state(
            None::<String>,
            None::<String>,
            vec![ProviderToolCall {
                id: "call".into(),
                name: "read".into(),
                arguments: "{}".into(),
            }],
            Some(replay),
        );
        let mut result = ChatMessage::tool_result("call", "read", "result");
        complete(&mut assistant, "round");
        complete(&mut result, "round");

        let projected =
            project_messages_for_provider("deepseek", "deepseek-chat", &[assistant, result])
                .unwrap();
        assert_eq!(projected.len(), 2);
        assert_eq!(projected[0].tool_calls.as_ref().unwrap()[0].id, "call");
        assert_eq!(
            projected[0].reasoning_content.as_deref(),
            Some("portable summary")
        );
        assert!(
            !projected[0]
                .reasoning_content
                .as_deref()
                .unwrap()
                .contains("do-not-render")
        );
        assert_eq!(projected[1].tool_call_id.as_deref(), Some("call"));
        assert_eq!(projected[1].content, "result");
    }

    #[test]
    fn compaction_payload_keeps_envelope_identity_and_reasoning_without_opaque_state() {
        let envelope = CanonicalProviderRoundEnvelope {
            version: 1,
            round_id: "round".into(),
            termination: crate::ProviderTermination::ToolCalls,
            message: ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                None::<String>,
                vec![ProviderToolCall {
                    id: "call".into(),
                    name: "inspect".into(),
                    arguments: "{}".into(),
                }],
                Some(ProviderReplayState::new(
                    "openrouter",
                    serde_json::json!({
                        "reasoning_details": [
                            {"type":"reasoning.encrypted","data":"opaque-secret"},
                            {"type":"reasoning.summary","summary":"portable reasoning"}
                        ]
                    }),
                )),
            ),
            calls: vec![crate::ProviderCallIdentity {
                provider_call_id: "call".into(),
                turn_item_id: "item".into(),
                ordinal: 0,
            }],
        };
        let canonical = serde_json::to_string(&envelope).unwrap();

        let projected = portable_history_payload(&canonical);
        let decoded: CanonicalProviderRoundEnvelope = serde_json::from_str(&projected).unwrap();

        assert_eq!(decoded.round_id, "round");
        assert_eq!(decoded.calls, envelope.calls);
        assert_eq!(
            decoded.message.reasoning_content.as_deref(),
            Some("portable reasoning")
        );
        assert!(decoded.message.provider_replay_state.is_none());
        assert!(!projected.contains("opaque-secret"));
        assert_eq!(serde_json::to_string(&envelope).unwrap(), canonical);
    }
}
