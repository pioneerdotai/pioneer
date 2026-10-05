//! Canonical request origins become indivisible planner units. This pass never
//! infers identity from equal text and never reconciles or executes tools.
use anyhow::{Result, ensure};
use pioneer_compaction::{HistoryUnit, SourceRef, SourceRole};
use pioneer_provider::{ChatMessage, Role};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy)]
pub enum PendingOriginKind {
    Input,
    Assistant,
    ToolResult,
    ToolItem,
}
/// A locator is resolved against durable metadata before it can become eligible
/// work. It contains no guessed revision or copied transcript.
pub fn pending_origin(
    workspace: &str,
    thread: &str,
    turn: &str,
    unit: &str,
    kind: PendingOriginKind,
    item: &str,
) -> pioneer_provider::MessageProvenance {
    let prefix = match kind {
        PendingOriginKind::Input => "pending-input",
        PendingOriginKind::Assistant => "pending-assistant",
        PendingOriginKind::ToolResult => "pending-tool",
        PendingOriginKind::ToolItem => "pending-item",
    };
    pioneer_provider::MessageProvenance {
        logical_turn_id: None,
        workspace_id: workspace.into(),
        thread_id: thread.into(),
        context_thread: None,
        unit_id: format!("{turn}:{unit}"),
        sources: vec![pioneer_provider::MessageSourceRef {
            scope: format!("{prefix}:{turn}"),
            id: item.into(),
            version: String::new(),
        }],
        source_aliases: vec![],
        ambiguous_input_aliases: vec![],
        complete: true,
        protected_input: matches!(kind, PendingOriginKind::Input),
        inherited: false,
    }
}

fn execution_turn_key(origin: &pioneer_provider::MessageProvenance) -> Option<(String, String)> {
    // logical_turn_id identifies Task delivery ownership, not the physical
    // provider turn. Use exact source scopes, including resolved item sources.
    let turns = origin
        .sources
        .iter()
        .filter_map(|source| {
            let (kind, turn) = source.scope.split_once(':')?;
            matches!(
                kind,
                "context"
                    | "item"
                    | "event"
                    | "input"
                    | "pending-assistant"
                    | "pending-tool"
                    | "pending-input"
            )
            .then_some(turn)
        })
        .collect::<BTreeSet<_>>();
    (turns.len() == 1).then(|| {
        (
            origin.thread_id.clone(),
            (*turns.first().expect("one turn")).to_owned(),
        )
    })
}

pub struct NativeHistoryLayout {
    pub units: Vec<HistoryUnit>,
    pub message_indexes: Vec<Vec<usize>>,
    pub source_threads: BTreeMap<SourceRef, String>,
}
impl NativeHistoryLayout {
    pub fn from_messages(
        workspace: &str,
        thread: &str,
        messages: &[ChatMessage],
        message_tokens: &[u64],
    ) -> Result<Self> {
        ensure!(
            messages.len() == message_tokens.len(),
            "incomplete message input estimates"
        );
        let mut result = Self {
            units: vec![],
            message_indexes: vec![],
            source_threads: BTreeMap::new(),
        };
        let mut groups = BTreeMap::new();
        for (index, message) in messages.iter().enumerate() {
            let Some(origin) = &message.provenance else {
                // Unattributed input is retained. Current user/steering is
                // protected until its durable source mapping is supplied.
                result.units.push(HistoryUnit {
                    sources: vec![SourceRef {
                        scope: format!("runtime:{thread}"),
                        id: index.to_string(),
                        version: hex::encode(Sha256::digest(serde_json::to_vec(message)?)),
                    }],
                    role: SourceRole::ReferenceOnly,
                    tokens: message_tokens[index],
                    complete: false,
                    protected_input: true,
                });
                result.message_indexes.push(vec![index]);
                continue;
            };
            ensure!(
                origin.workspace_id == workspace
                    && !origin.sources.is_empty()
                    && !origin.unit_id.is_empty(),
                "invalid message origin scope"
            );
            let context_owner = origin
                .context_thread
                .as_deref()
                .unwrap_or(&origin.thread_id);
            ensure!(!context_owner.is_empty(), "invalid context owner");
            let role = if origin.inherited || context_owner != thread {
                SourceRole::Inherited
            } else {
                SourceRole::Own
            };
            let key = (origin.thread_id.clone(), origin.unit_id.clone());
            let unit_index = *groups.entry(key).or_insert_with(|| {
                let index = result.units.len();
                result.units.push(HistoryUnit {
                    sources: vec![],
                    role: role.clone(),
                    tokens: 0,
                    complete: true,
                    protected_input: false,
                });
                result.message_indexes.push(vec![]);
                index
            });
            let unit = &mut result.units[unit_index];
            ensure!(unit.role == role, "mixed inherited and own unit");
            unit.complete &= origin.complete
                && origin
                    .sources
                    .iter()
                    .all(|source| !source.version.is_empty());
            unit.protected_input |= origin.protected_input || message.role == Role::System;
            unit.tokens = unit.tokens.saturating_add(message_tokens[index]);
            result.message_indexes[unit_index].push(index);
            for reference in &origin.sources {
                let reference = SourceRef {
                    scope: reference.scope.clone(),
                    id: reference.id.clone(),
                    version: reference.version.clone(),
                };
                if let Some(previous) = result
                    .source_threads
                    .insert(reference.clone(), origin.thread_id.clone())
                {
                    ensure!(previous == origin.thread_id, "ambiguous source ownership");
                }
                if !unit.sources.contains(&reference) {
                    unit.sources.push(reference);
                }
            }
        }
        for (unit, indexes) in result.units.iter_mut().zip(&result.message_indexes) {
            let mut calls = BTreeMap::new();
            let mut outcomes = BTreeMap::new();
            for index in indexes {
                let message = &messages[*index];
                for call in message.tool_calls.iter().flatten() {
                    ensure!(
                        calls.insert(call.id.as_str(), call.name.as_str()).is_none(),
                        "duplicate call in canonical unit"
                    );
                }
                if message.role == Role::Tool {
                    if let (Some(id), Some(name)) =
                        (message.tool_call_id.as_deref(), message.name.as_deref())
                    {
                        ensure!(
                            outcomes.insert(id, name).is_none(),
                            "duplicate terminal tool outcome"
                        );
                    } else {
                        unit.complete = false;
                    }
                }
            }
            unit.complete &= calls == outcomes;
        }
        // A completed call/result pair is not necessarily the end of the
        // assistant turn. The selected native continuation profile may still need
        // state from earlier rounds of that same turn. Mark those units pending
        // until a final assistant response is present; Emergency respects this
        // correctness boundary as it already respects incomplete tool pairs.
        let active_turns = messages
            .iter()
            .filter_map(|message| {
                let origin = message.provenance.as_ref()?;
                (message.role == Role::User && origin.protected_input)
                    .then(|| execution_turn_key(origin))
                    .flatten()
            })
            .collect::<BTreeSet<_>>();
        let mut native_turns = BTreeSet::new();
        let mut last_assistant = BTreeMap::new();
        for message in messages {
            let Some(origin) = message.provenance.as_ref() else {
                continue;
            };
            let Some(key) = execution_turn_key(origin) else {
                continue;
            };
            if message.role == Role::Assistant {
                last_assistant.insert(
                    key.clone(),
                    message
                        .tool_calls
                        .as_ref()
                        .is_some_and(|calls| !calls.is_empty()),
                );
                if message.provider_replay_state.as_ref().is_some_and(|state| {
                    pioneer_provider::continuation::retention(state)
                        != pioneer_provider::continuation::Retention::Ordinary
                }) {
                    native_turns.insert(key);
                }
            }
        }
        for (unit, indexes) in result.units.iter_mut().zip(&result.message_indexes) {
            if indexes.iter().any(|index| {
                messages[*index].provenance.as_ref().is_some_and(|origin| {
                    execution_turn_key(origin).is_some_and(|key| {
                        active_turns.contains(&key)
                            && native_turns.contains(&key)
                            && last_assistant.get(&key) == Some(&true)
                    })
                })
            }) {
                unit.complete = false;
            }
        }
        let mut seen = BTreeSet::new();
        for unit in &result.units {
            for reference in &unit.sources {
                ensure!(
                    seen.insert((reference.scope.clone(), reference.id.clone())),
                    "source occurs in multiple request units"
                );
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_provider::{MessageProvenance, MessageSourceRef, ProviderToolCall};
    fn origin(message: &mut ChatMessage, unit: &str, source: &str, protected: bool) {
        message.provenance = Some(MessageProvenance {
            logical_turn_id: None,
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            context_thread: None,
            unit_id: unit.into(),
            sources: vec![MessageSourceRef {
                scope: "context:turn".into(),
                id: source.into(),
                version: "revision:1".into(),
            }],
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
            complete: true,
            protected_input: protected,
            inherited: false,
        });
    }

    #[test]
    fn native_multi_round_turn_stays_pending_until_final_assistant_response() {
        let mut input = ChatMessage::user("current input");
        origin(&mut input, "input", "input-source", true);
        let mut messages = vec![input];
        for index in 0..2 {
            let id = format!("call-{index}");
            let mut assistant = ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                None::<String>,
                vec![ProviderToolCall {
                    id: id.clone(),
                    name: "read".into(),
                    arguments: "{}".into(),
                }],
                Some(pioneer_provider::ProviderReplayState::for_model(
                    "anthropic",
                    "fixture",
                    serde_json::json!({"schema_version":2,"blocks":[{"type":"thinking","thinking":"","signature":"signed"},{"type":"tool_use","id":id,"name":"read","input":{}}]}),
                )),
            );
            origin(
                &mut assistant,
                &format!("round-{index}"),
                &format!("a-{index}"),
                false,
            );
            let mut result = ChatMessage::tool_result(id, "read", "output");
            origin(
                &mut result,
                &format!("round-{index}"),
                &format!("r-{index}"),
                false,
            );
            messages.extend([assistant, result]);
        }
        let layout =
            NativeHistoryLayout::from_messages("ws", "thread", &messages, &[1; 5]).unwrap();
        assert!(layout.units[1..].iter().all(|unit| !unit.complete));
        let mut final_message = ChatMessage::assistant("final");
        origin(&mut final_message, "final", "final-source", false);
        messages.push(final_message);
        let closed =
            NativeHistoryLayout::from_messages("ws", "thread", &messages, &[1; 6]).unwrap();
        assert!(closed.units.iter().all(|unit| unit.complete));
        assert_eq!(closed.message_indexes[1], [1, 2]);
        assert_eq!(closed.message_indexes[2], [3, 4]);
    }
    #[test]
    fn retention_uses_actual_state_with_hot_and_cold_scopes_in_both_planner_modes() {
        use pioneer_compaction::{CompactionMode, CoverageDomain, ModelBudget, plan_compaction};
        use pioneer_provider::ProviderReplayState;
        for (provider, model, payload, required) in [
            (
                "bedrock",
                "anthropic.claude-sonnet-4-6",
                serde_json::json!({"blocks":[{"reasoningText":{"text":"","signature":"signed"}},{"redactedContent":"opaque"}]}),
                true,
            ),
            (
                "openrouter",
                "anthropic/claude-sonnet-5.5",
                serde_json::json!({"reasoning_details":[{"type":"reasoning.encrypted","format":"anthropic-claude-v1","data":"opaque"}]}),
                true,
            ),
            (
                "openrouter",
                "selected",
                serde_json::json!({"reasoning_details":[{"type":"reasoning.text","text":"readable","signature":null},{"type":"reasoning.summary","summary":"summary"}]}),
                true,
            ),
            (
                "openrouter",
                "selected",
                serde_json::json!({"reasoning_details":[{"type":"reasoning.native-v-next","data":"opaque"}]}),
                true,
            ),
            (
                "openrouter",
                "selected",
                serde_json::json!({"reasoning_details":[{"type":"reasoning.summary","summary":"known"},{"type":"reasoning.native-v-next","data":"opaque"}]}),
                true,
            ),
            (
                "openrouter",
                "selected",
                serde_json::json!({"reasoning_details":[{"data":"opaque"}]}),
                true,
            ),
            (
                "openrouter",
                "selected",
                serde_json::json!({"reasoning_details":["opaque"]}),
                true,
            ),
            (
                "openrouter",
                "selected",
                serde_json::json!({"reasoning_details":[]}),
                false,
            ),
            (
                "anthropic",
                "claude-sonnet-4-6",
                serde_json::json!({"schema_version":2,"blocks":[{"type":"text","text":"ordinary"},{"type":"tool_use","id":"call","name":"read","input":{}}]}),
                false,
            ),
            (
                "gemini",
                "gemini-2.5-flash",
                serde_json::json!({"schema_version":2,"parts":[{"functionCall":{"name":"read","args":{}}}]}),
                false,
            ),
            (
                "anthropic",
                "unknown-generation",
                serde_json::json!({"blocks":[{"type":"redacted_thinking","data":"opaque"}]}),
                true,
            ),
            (
                "deepseek",
                "deepseek-v4-flash",
                serde_json::json!({"schema_version":1,"assistant_message":{"reasoning_content":"","content":null,"tool_calls":[]}}),
                true,
            ),
        ] {
            for hot in [false, true] {
                let mut input = ChatMessage::user("active");
                origin(&mut input, "input", "input", true);
                let mut messages = vec![input];
                for round in 0..2 {
                    let id = format!("call-{round}");
                    let mut assistant = ChatMessage::assistant_tool_calls_with_provider_state(
                        None::<String>,
                        None::<String>,
                        vec![ProviderToolCall {
                            id: id.clone(),
                            name: "read".into(),
                            arguments: "{}".into(),
                        }],
                        Some(ProviderReplayState::for_model(
                            provider,
                            model,
                            payload.clone(),
                        )),
                    );
                    origin(
                        &mut assistant,
                        &format!("round-{round}"),
                        &format!("a-{round}"),
                        false,
                    );
                    let mut result = ChatMessage::tool_result(id, "read", "outcome");
                    origin(
                        &mut result,
                        &format!("round-{round}"),
                        &format!("r-{round}"),
                        false,
                    );
                    messages.extend([assistant, result]);
                }
                for message in &mut messages {
                    let origin = message.provenance.as_mut().unwrap();
                    origin.sources[0].scope = if hot {
                        match message.role {
                            Role::User => "pending-input:turn",
                            Role::Tool => "pending-tool:turn",
                            _ => "pending-assistant:turn",
                        }
                    } else {
                        "context:turn"
                    }
                    .into();
                    origin.sources[0].version = if hot { "" } else { "revision:1" }.into();
                }
                let layout =
                    NativeHistoryLayout::from_messages("ws", "thread", &messages, &[100; 5])
                        .unwrap();
                assert_eq!(layout.units[1].complete, !required, "{provider}/{hot}");
                for mode in [CompactionMode::Normal, CompactionMode::Emergency] {
                    let plan = plan_compaction(
                        &layout.units,
                        &ModelBudget::new(Some(32768), None, None),
                        256,
                        0,
                        512,
                        mode,
                        CoverageDomain::WorkingContext,
                        true,
                        "fixture",
                    );
                    assert_eq!(plan.is_err(), required, "{provider}/{hot}/{mode:?}");
                }
                let mut final_message = ChatMessage::assistant("final");
                origin(&mut final_message, "final", "final", false);
                final_message.provenance.as_mut().unwrap().sources[0].scope = "context:turn".into();
                messages.push(final_message);
                let closed =
                    NativeHistoryLayout::from_messages("ws", "thread", &messages, &[100; 6])
                        .unwrap();
                assert!(closed.units.iter().all(|unit| unit.complete));
                // Final closure restores pair/unit eligibility, but unknown or
                // binding state still forbids prefix rewriting while retained.
                let expected_refusal =
                    messages[1]
                        .provider_replay_state
                        .as_ref()
                        .is_some_and(|state| {
                            pioneer_provider::continuation::retention(state).preserves_prefix()
                        });
                assert_eq!(
                    pioneer_provider::continuation::validate_compaction(
                        &messages,
                        &BTreeSet::from([0]),
                    )
                    .is_err(),
                    expected_refusal
                );
            }
        }
    }

    #[test]
    fn whole_round_remains_pending_until_outcome_and_preserves_intervening_steering() {
        let mut assistant = ChatMessage::assistant_tool_calls(
            None::<String>,
            vec![ProviderToolCall {
                id: "call".into(),
                name: "tool".into(),
                arguments: "{}".into(),
            }],
        );
        origin(&mut assistant, "round", "assistant", false);
        let mut steering = ChatMessage::user("accepted steering");
        origin(&mut steering, "steering", "input", true);
        let pending = NativeHistoryLayout::from_messages(
            "ws",
            "thread",
            &[assistant.clone(), steering.clone()],
            &[10, 10],
        )
        .unwrap();
        assert!(!pending.units[0].complete);
        assert!(pending.units[1].protected_input);
        let mut tool = ChatMessage::tool_result("call", "tool", "completed output");
        origin(&mut tool, "round", "result", false);
        let complete = NativeHistoryLayout::from_messages(
            "ws",
            "thread",
            &[assistant, steering, tool],
            &[10, 10, 10],
        )
        .unwrap();
        assert!(complete.units[0].complete);
        assert_eq!(complete.message_indexes[0], vec![0, 2]);
        assert_eq!(complete.units[0].sources.len(), 2);
        assert_eq!(complete.units[1].role, SourceRole::Own);
    }
    #[test]
    fn equal_text_never_defines_identity_and_inherited_sources_stay_separate() {
        let mut own = ChatMessage::user("same");
        origin(&mut own, "own", "a", false);
        let mut inherited = ChatMessage::user("same");
        origin(&mut inherited, "basis", "h", false);
        inherited.provenance.as_mut().unwrap().thread_id = "parent".into();
        let layout = NativeHistoryLayout::from_messages(
            "ws",
            "thread",
            &[own.clone(), inherited.clone(), ChatMessage::user("same")],
            &[1, 1, 1],
        )
        .unwrap();
        assert_eq!(layout.units.len(), 3);
        assert_eq!(layout.units[0].role, SourceRole::Own);
        assert_eq!(layout.units[1].role, SourceRole::Inherited);
        assert!(layout.units[2].protected_input);
        assert!(
            NativeHistoryLayout::from_messages("other", "thread", &[own, inherited], &[1, 1])
                .is_err()
        );
    }

    #[test]
    fn adopted_work_changes_context_ownership_without_changing_storage_scope() {
        let mut inherited = ChatMessage::user("shared H");
        origin(&mut inherited, "basis", "h", false);
        inherited.provenance.as_mut().unwrap().thread_id = "parent".into();
        inherited.provenance.as_mut().unwrap().inherited = true;
        let mut a = ChatMessage::assistant("same independent work");
        origin(&mut a, "round", "a", false);
        let mut b = a.clone();
        a.provenance.as_mut().unwrap().thread_id = "A".into();
        a.provenance.as_mut().unwrap().context_thread = Some("C".into());
        b.provenance.as_mut().unwrap().thread_id = "B".into();
        b.provenance.as_mut().unwrap().sources[0].id = "b".into();
        b.provenance.as_mut().unwrap().context_thread = Some("C".into());
        let layout = NativeHistoryLayout::from_messages(
            "ws",
            "C",
            &[inherited, a.clone(), b.clone()],
            &[1, 1, 1],
        )
        .unwrap();
        assert_eq!(layout.units[0].role, SourceRole::Inherited);
        assert_eq!(layout.units[1].role, SourceRole::Own);
        assert_eq!(layout.units[2].role, SourceRole::Own);
        assert_eq!(layout.source_threads[&layout.units[1].sources[0]], "A");
        assert_eq!(layout.source_threads[&layout.units[2].sources[0]], "B");
        let elsewhere = NativeHistoryLayout::from_messages("ws", "D", &[a, b], &[1, 1]).unwrap();
        assert!(
            elsewhere
                .units
                .iter()
                .all(|unit| unit.role == SourceRole::Inherited)
        );
    }
}
