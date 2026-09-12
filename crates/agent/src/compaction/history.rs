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
        complete: true,
        protected_input: matches!(kind, PendingOriginKind::Input),
        inherited: false,
    }
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
            complete: true,
            protected_input: protected,
            inherited: false,
        });
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
