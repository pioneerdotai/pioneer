//! Compose accepted execution branches by exact source coverage. The Gateway
//! supplies checkpoint closures after validating them against durable metadata;
//! this module cannot infer coverage from summary text or a turn count.
use super::history::NativeHistoryLayout;
use anyhow::{Result, ensure};
use pioneer_compaction::{SourceRef, SourceRole};
use pioneer_provider::ChatMessage;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScopedHistorySource {
    pub thread: String,
    pub source: SourceRef,
}

pub struct AcceptedContextBranch<'a> {
    pub thread: &'a str,
    pub messages: &'a [ChatMessage],
    /// Every summary reference needs an exact, version-checked leaf closure.
    /// No entry means unknown coverage, never an empty summary.
    pub checkpoints: &'a BTreeMap<ScopedHistorySource, BTreeSet<ScopedHistorySource>>,
}

/// The supplied summaries are valid individually but require an exact original
/// projection before their coverage can be composed without duplication.
#[derive(Debug)]
pub struct CompatibleProjectionRequired {
    pub affected: BTreeSet<ScopedHistorySource>,
}
impl std::fmt::Display for CompatibleProjectionRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("overlapping history requires a compatible exact source projection")
    }
}
impl std::error::Error for CompatibleProjectionRequired {}

struct Unit {
    messages: Vec<(usize, ChatMessage)>,
    leaves: BTreeSet<ScopedHistorySource>,
    own: bool,
}

/// H appears once; accepted own work from A/B becomes own work of C without
/// changing the storage thread on any source. Inputs to this function are
/// accepted runtime snapshots, not arbitrary model-supplied references.
/// A partially overlapping summary requires the Gateway to rematerialize a
/// compatible source projection. It is never "subtracted" from another text.
pub fn compose_context(
    workspace: &str,
    destination: &str,
    branches: &[AcceptedContextBranch<'_>],
) -> Result<Vec<ChatMessage>> {
    ensure!(!destination.is_empty(), "missing destination context");
    let mut units: Vec<Unit> = Vec::new();
    // Accepted units are pairwise disjoint. Index every leaf as it is accepted
    // so overlap detection stays proportional to the number of sources instead
    // of comparing every unit with every unit on long histories.
    let mut leaf_owners = HashMap::<ScopedHistorySource, usize>::new();
    let mut revisions = HashMap::new();
    let mut branch_offset = 0;
    for branch in branches {
        ensure!(
            branch
                .messages
                .iter()
                .all(|message| message.provenance.is_some()),
            "accepted branch has unattributed history"
        );
        let layout = NativeHistoryLayout::from_messages(
            workspace,
            branch.thread,
            branch.messages,
            &vec![0; branch.messages.len()],
        )?;
        for (unit, indexes) in layout.units.iter().zip(&layout.message_indexes) {
            // The snapshot of an active parent contains completed work only.
            // An unfinished provider call and its partial results stay together
            // in that parent's execution context.
            if !unit.complete {
                continue;
            }
            ensure!(
                unit.role != SourceRole::ReferenceOnly,
                "accepted branch has unattributed history"
            );
            let own = unit.role == SourceRole::Own && !unit.protected_input;
            let mut leaves = BTreeSet::new();
            for source in &unit.sources {
                let scoped = ScopedHistorySource {
                    thread: layout.source_threads[source].clone(),
                    source: source.clone(),
                };
                if source.scope.starts_with("checkpoint:") {
                    let closure = branch.checkpoints.get(&scoped).ok_or_else(|| {
                        anyhow::anyhow!("summary requires a compatible exact source projection")
                    })?;
                    ensure!(!closure.is_empty(), "empty checkpoint coverage");
                    ensure!(
                        closure
                            .iter()
                            .all(|leaf| !leaf.source.scope.starts_with("checkpoint:")
                                && !leaf.source.version.is_empty()
                                && !leaf.thread.is_empty()),
                        "checkpoint coverage has unresolved leaves"
                    );
                    leaves.extend(closure.iter().cloned());
                } else {
                    leaves.insert(scoped);
                }
            }
            for leaf in &leaves {
                let identity = (
                    leaf.thread.clone(),
                    leaf.source.scope.clone(),
                    leaf.source.id.clone(),
                );
                if let Some(version) = revisions.insert(identity, leaf.source.version.clone()) {
                    ensure!(
                        version == leaf.source.version,
                        "accepted branches contain conflicting source revisions"
                    );
                }
            }
            let owners = leaves
                .iter()
                .filter_map(|leaf| leaf_owners.get(leaf).copied())
                .collect::<HashSet<_>>();
            let duplicate = match owners.len() {
                0 => None,
                1 => {
                    let index = *owners.iter().next().expect("one overlap owner");
                    let previous = &units[index];
                    if previous.leaves != leaves {
                        return Err(CompatibleProjectionRequired {
                            affected: previous.leaves.union(&leaves).cloned().collect(),
                        }
                        .into());
                    }
                    Some(index)
                }
                _ => {
                    let affected = owners
                        .iter()
                        .flat_map(|index| units[*index].leaves.iter().cloned())
                        .chain(leaves.iter().cloned())
                        .collect();
                    return Err(CompatibleProjectionRequired { affected }.into());
                }
            };
            if let Some(index) = duplicate {
                // A source inherited by one branch can be accepted own work of
                // another. Its single projection then remains eligible in C.
                units[index].own |= own;
            } else {
                let index = units.len();
                for leaf in &leaves {
                    leaf_owners.insert(leaf.clone(), index);
                }
                units.push(Unit {
                    messages: indexes
                        .iter()
                        .map(|index| (branch_offset + *index, branch.messages[*index].clone()))
                        .collect(),
                    leaves,
                    own,
                });
            }
        }
        branch_offset += branch.messages.len();
    }
    let mut messages = Vec::new();
    for unit in units {
        for (index, mut message) in unit.messages {
            let origin = message
                .provenance
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("accepted history lost its source"))?;
            origin.context_thread = Some(destination.into());
            origin.inherited = !unit.own;
            messages.push((index, message));
        }
    }
    messages.sort_by_key(|(index, _)| *index);
    Ok(messages.into_iter().map(|(_, message)| message).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_provider::{MessageProvenance, MessageSourceRef};

    fn message(thread: &str, id: &str, inherited: bool) -> ChatMessage {
        let mut message = ChatMessage::assistant("identical independent text");
        message.provenance = Some(MessageProvenance {
            logical_turn_id: None,
            workspace_id: "ws".into(),
            thread_id: thread.into(),
            context_thread: None,
            unit_id: id.into(),
            sources: vec![MessageSourceRef {
                scope: format!("event:{thread}"),
                id: id.into(),
                version: "revision:1".into(),
            }],
            complete: true,
            protected_input: false,
            inherited,
        });
        message
    }
    #[test]
    fn compaction_composition_keeps_h_once_and_own_a_b_independent() {
        let h = message("H", "shared", true);
        let a = vec![h.clone(), message("A", "a", false)];
        let b = vec![h, message("B", "b", false)];
        let closures = BTreeMap::new();
        let composed = compose_context(
            "ws",
            "C",
            &[
                AcceptedContextBranch {
                    thread: "A",
                    messages: &a,
                    checkpoints: &closures,
                },
                AcceptedContextBranch {
                    thread: "B",
                    messages: &b,
                    checkpoints: &closures,
                },
            ],
        )
        .unwrap();
        assert_eq!(composed.len(), 3);
        let layout = NativeHistoryLayout::from_messages("ws", "C", &composed, &[1, 1, 1]).unwrap();
        assert_eq!(
            layout
                .units
                .iter()
                .map(|unit| &unit.role)
                .collect::<Vec<_>>(),
            vec![&SourceRole::Inherited, &SourceRole::Own, &SourceRole::Own]
        );
        assert_eq!(composed[1].provenance.as_ref().unwrap().thread_id, "A");
        assert_eq!(composed[2].provenance.as_ref().unwrap().thread_id, "B");
        assert!(a.iter().chain(&b).all(|message| {
            message
                .provenance
                .as_ref()
                .unwrap()
                .context_thread
                .is_none()
        }));
    }
    #[test]
    fn compaction_composition_rejects_conflicting_versions_and_unknown_summary() {
        let a = vec![message("H", "shared", true)];
        let mut b = a.clone();
        b[0].provenance.as_mut().unwrap().sources[0].version = "revision:2".into();
        let closures = BTreeMap::new();
        assert!(
            compose_context(
                "ws",
                "C",
                &[
                    AcceptedContextBranch {
                        thread: "A",
                        messages: &a,
                        checkpoints: &closures
                    },
                    AcceptedContextBranch {
                        thread: "B",
                        messages: &b,
                        checkpoints: &closures
                    },
                ]
            )
            .is_err()
        );
        b[0].provenance.as_mut().unwrap().sources[0].scope = "checkpoint:H".into();
        assert!(
            compose_context(
                "ws",
                "C",
                &[AcceptedContextBranch {
                    thread: "B",
                    messages: &b,
                    checkpoints: &closures
                }]
            )
            .is_err()
        );
    }

    #[test]
    fn compaction_composition_never_imports_future_coverage_across_fork_boundary() {
        let history = (1..=100)
            .map(|i| message("parent", &i.to_string(), true))
            .collect::<Vec<_>>();
        let mut summary = message("parent", "summary-100", true);
        summary.provenance.as_mut().unwrap().sources[0].scope = "checkpoint:parent".into();
        let reference = |message: &ChatMessage| {
            let origin = message.provenance.as_ref().unwrap();
            let source = &origin.sources[0];
            ScopedHistorySource {
                thread: origin.thread_id.clone(),
                source: SourceRef {
                    scope: source.scope.clone(),
                    id: source.id.clone(),
                    version: source.version.clone(),
                },
            }
        };
        let closures =
            BTreeMap::from([(reference(&summary), history.iter().map(reference).collect())]);
        let summary = [summary];
        assert!(
            compose_context(
                "ws",
                "fork",
                &[
                    AcceptedContextBranch {
                        thread: "parent",
                        messages: &history[..60],
                        checkpoints: &closures
                    },
                    AcceptedContextBranch {
                        thread: "parent",
                        messages: &summary,
                        checkpoints: &closures
                    },
                ]
            )
            .unwrap_err()
            .to_string()
            .contains("compatible exact source projection")
        );
        let selected = compose_context(
            "ws",
            "fork",
            &[AcceptedContextBranch {
                thread: "parent",
                messages: &history[..60],
                checkpoints: &closures,
            }],
        )
        .unwrap();
        assert_eq!(selected.len(), 60);
        assert_eq!(
            selected
                .last()
                .unwrap()
                .provenance
                .as_ref()
                .unwrap()
                .sources[0]
                .id,
            "60"
        );
    }

    #[test]
    fn compaction_composition_keeps_event_order_and_leaves_pending_round_in_parent() {
        use pioneer_provider::{ProviderToolCall, Role};
        let mut assistant = ChatMessage::assistant_tool_calls(
            None::<String>,
            vec![ProviderToolCall {
                id: "call".into(),
                name: "tool".into(),
                arguments: "{}".into(),
            }],
        );
        assistant.provenance = message("A", "round", false).provenance;
        let mut result = ChatMessage::tool_result("call", "tool", "done");
        result.provenance = assistant.provenance.clone();
        result.provenance.as_mut().unwrap().sources[0].id = "result".into();
        let mut steering = message("A", "steering", false);
        steering.role = Role::User;
        steering.provenance.as_mut().unwrap().protected_input = true;
        let mut pending = assistant.clone();
        pending.provenance = message("A", "pending", false).provenance;
        let history = vec![assistant.clone(), steering.clone(), result.clone(), pending];
        let closures = BTreeMap::new();
        let output = compose_context(
            "ws",
            "C",
            &[AcceptedContextBranch {
                thread: "A",
                messages: &history,
                checkpoints: &closures,
            }],
        )
        .unwrap();
        assert_eq!(
            output
                .iter()
                .map(|message| message.role.clone())
                .collect::<Vec<_>>(),
            vec![Role::Assistant, Role::User, Role::Tool]
        );
        assert_eq!(output[0].tool_calls, assistant.tool_calls);
        assert_eq!(output[1].content, steering.content);
        assert_eq!(output[2].content, result.content);
        assert!(
            output
                .iter()
                .all(|message| message.provenance.as_ref().unwrap().unit_id != "pending")
        );
    }
}
