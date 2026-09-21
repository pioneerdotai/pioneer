//! Compose accepted execution branches by exact source coverage. The Gateway
//! supplies checkpoint closures after validating them against durable metadata;
//! this module cannot infer coverage from summary text or a turn count.
use super::history::NativeHistoryLayout;
use anyhow::{Result, ensure};
use pioneer_compaction::{SourceRef, SourceRole};
use pioneer_provider::ChatMessage;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ScopedHistorySource {
    pub thread: String,
    pub source: SourceRef,
}

pub struct AcceptedContextBranch<'a> {
    pub thread: &'a str,
    pub messages: &'a [ChatMessage],
    /// Every summary reference needs its saved historical leaf closure. The
    /// versions identify publication-time records rather than today's rows.
    /// No entry means unknown coverage, never an empty summary.
    pub checkpoints: &'a BTreeMap<ScopedHistorySource, BTreeSet<ScopedHistorySource>>,
}

struct Unit {
    messages: Vec<(usize, ChatMessage)>,
    leaves: BTreeSet<ScopedHistorySource>,
    identities: BTreeSet<(String, String, String)>,
    own: bool,
    checkpoint: bool,
}

/// Two raw composite input units overlap without being identical. The Gateway
/// can split only those canonical input rows before retrying composition; this
/// is never used to rematerialize a summary.
#[derive(Debug)]
pub struct SplitRawInputsRequired {
    pub affected: BTreeSet<ScopedHistorySource>,
}

impl std::fmt::Display for SplitRawInputsRequired {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("overlapping raw history requires canonical input splitting")
    }
}

impl std::error::Error for SplitRawInputsRequired {}

/// H appears once; accepted own work from A/B becomes own work of C without
/// changing the storage thread on any source. Inputs to this function are
/// accepted runtime snapshots, not arbitrary model-supplied references.
/// Summaries are atomic. An equal checkpoint is emitted once and a summary
/// whose historical coverage contains another whole unit may replace it.
/// Distinct partially-overlapping summaries are both retained; their text is
/// never split and their originals are never rematerialized for deduplication.
pub fn compose_context(
    workspace: &str,
    destination: &str,
    branches: &[AcceptedContextBranch<'_>],
) -> Result<Vec<ChatMessage>> {
    ensure!(!destination.is_empty(), "missing destination context");
    let mut units: Vec<Unit> = Vec::new();
    let mut revisions = BTreeMap::new();
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
            let mut checkpoint = false;
            for source in &unit.sources {
                let scoped = ScopedHistorySource {
                    thread: layout.source_threads[source].clone(),
                    source: source.clone(),
                };
                if source.scope.starts_with("checkpoint:") {
                    checkpoint = true;
                    let closure = branch.checkpoints.get(&scoped).ok_or_else(|| {
                        anyhow::anyhow!("summary requires compatible historical coverage")
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
                    let identity = (
                        scoped.thread.clone(),
                        scoped.source.scope.clone(),
                        scoped.source.id.clone(),
                    );
                    if let Some(version) = revisions.insert(identity, scoped.source.version.clone())
                    {
                        ensure!(
                            version == scoped.source.version,
                            "accepted branches contain conflicting source revisions"
                        );
                    }
                    leaves.insert(scoped);
                }
            }
            let identities = leaves
                .iter()
                .map(|leaf| {
                    (
                        leaf.thread.clone(),
                        leaf.source.scope.clone(),
                        leaf.source.id.clone(),
                    )
                })
                .collect::<BTreeSet<_>>();
            let mut duplicate = None;
            let mut replaced = Vec::new();
            for (index, previous) in units.iter().enumerate() {
                if previous.identities.is_disjoint(&identities) {
                    continue;
                }
                if previous.identities == identities {
                    if checkpoint && !previous.checkpoint {
                        replaced.push(index);
                        continue;
                    }
                    duplicate = Some(index);
                    break;
                }
                if checkpoint && previous.identities.is_subset(&identities) {
                    replaced.push(index);
                } else if previous.checkpoint && identities.is_subset(&previous.identities) {
                    duplicate = Some(index);
                    break;
                } else if !checkpoint && !previous.checkpoint {
                    return Err(SplitRawInputsRequired {
                        affected: previous.leaves.union(&leaves).cloned().collect(),
                    }
                    .into());
                }
            }
            if let Some(index) = duplicate {
                // A source inherited by one branch can be accepted own work of
                // another. Summary ownership is not promoted: a WorkingContext
                // result remains inherited even if another branch repeats it.
                if !units[index].checkpoint {
                    units[index].own |= own;
                }
            } else {
                for index in replaced.into_iter().rev() {
                    units.remove(index);
                }
                units.push(Unit {
                    messages: indexes
                        .iter()
                        .map(|index| (branch_offset + *index, branch.messages[*index].clone()))
                        .collect(),
                    leaves,
                    identities,
                    own,
                    checkpoint,
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
    fn compaction_composition_treats_an_admitted_summary_as_one_unit() {
        let mut history = (1..=100)
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
        history[0].provenance.as_mut().unwrap().sources[0].version = "revision:2".into();
        let summary = [summary];
        // Snapshot/fork admission is enforced by the Gateway before this
        // function. Once admitted, a summary is indivisible and replaces a
        // whole raw unit contained by its historical coverage even if today's
        // raw revision differs from the saved historical revision.
        let selected = compose_context(
            "ws",
            "fork",
            &[
                AcceptedContextBranch {
                    thread: "parent",
                    messages: &history[..60],
                    checkpoints: &closures,
                },
                AcceptedContextBranch {
                    thread: "parent",
                    messages: &summary,
                    checkpoints: &closures,
                },
            ],
        )
        .unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(
            selected
                .first()
                .unwrap()
                .provenance
                .as_ref()
                .unwrap()
                .sources[0]
                .id,
            "summary-100"
        );
    }

    #[test]
    fn compaction_composition_keeps_distinct_partially_overlapping_summaries() {
        let leaf = |id: &str| ScopedHistorySource {
            thread: "parent".into(),
            source: SourceRef {
                scope: "event:turn".into(),
                id: id.into(),
                version: "event-revision:1".into(),
            },
        };
        let summary = |id: &str| {
            let mut message = message("parent", id, true);
            message.provenance.as_mut().unwrap().sources[0].scope =
                "checkpoint:parent-owner".into();
            message
        };
        let a = summary("summary-a");
        let b = summary("summary-b");
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
        let closures = BTreeMap::from([
            (reference(&a), BTreeSet::from([leaf("x"), leaf("y")])),
            (reference(&b), BTreeSet::from([leaf("y"), leaf("z")])),
        ]);

        let composed = compose_context(
            "ws",
            "child",
            &[
                AcceptedContextBranch {
                    thread: "parent",
                    messages: std::slice::from_ref(&a),
                    checkpoints: &closures,
                },
                AcceptedContextBranch {
                    thread: "parent",
                    messages: std::slice::from_ref(&b),
                    checkpoints: &closures,
                },
            ],
        )
        .unwrap();
        assert_eq!(composed.len(), 2);
        assert_eq!(
            composed[0].provenance.as_ref().unwrap().sources[0].id,
            "summary-a"
        );
        assert_eq!(
            composed[1].provenance.as_ref().unwrap().sources[0].id,
            "summary-b"
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
