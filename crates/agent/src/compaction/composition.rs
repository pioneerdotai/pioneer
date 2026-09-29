//! Compose accepted execution branches by exact source coverage. The Gateway
//! supplies checkpoint closures after validating them against durable metadata;
//! this module cannot infer coverage from summary text or a turn count.
use super::history::NativeHistoryLayout;
use anyhow::{Result, ensure};
use pioneer_compaction::{CoverageDomain, SourceRef, SourceRole};
use pioneer_provider::{ChatMessage, MessageSourceAlias, MessageSourceIdentity, MessageSourceRef};
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
    pub checkpoint_domains: &'a BTreeMap<ScopedHistorySource, CoverageDomain>,
}

struct Unit {
    messages: Vec<(usize, ChatMessage)>,
    leaves: BTreeSet<ScopedHistorySource>,
    identities: BTreeSet<(String, String, String)>,
    input_aliases: ExactInputClaims,
    own: bool,
    checkpoint: bool,
    checkpoint_domain: Option<CoverageDomain>,
}

pub type ExactMessageSource = (String, MessageSourceRef);

/// Keep every exact competing owner until the final carrier is known. A
/// resolved replay graph cannot represent the claims needed when one summary
/// absorbs another or an exact leaf revision is filtered out. Callers still
/// validate raw ownership or checkpoint leaf closure before adding claims.
#[derive(Clone, Default)]
pub struct ExactInputClaims {
    pub owners: BTreeMap<ExactMessageSource, BTreeSet<ExactMessageSource>>,
    pub ambiguous: BTreeSet<ExactMessageSource>,
}

impl ExactInputClaims {
    fn add(&mut self, copy: ExactMessageSource, represented: ExactMessageSource) {
        self.owners.entry(copy).or_default().insert(represented);
    }

    pub fn add_alias(&mut self, alias: &MessageSourceAlias) {
        self.add(
            (alias.thread_id.clone(), alias.source.clone()),
            (
                alias.represented_thread_id.clone(),
                alias.represented_source.clone(),
            ),
        );
    }

    pub fn merge(&mut self, other: Self) {
        self.ambiguous.extend(other.ambiguous);
        for (copy, owners) in other.owners {
            self.owners.entry(copy).or_default().extend(owners);
        }
        self.mark_competing_owners();
    }

    pub fn mark_competing_owners(&mut self) {
        self.ambiguous.extend(
            self.owners
                .iter()
                .filter(|(_, owners)| owners.len() > 1)
                .map(|(copy, _)| copy.clone()),
        );
    }

    fn proves(&self, copy: &ExactMessageSource, owner: &ExactMessageSource) -> bool {
        !self.ambiguous.contains(copy)
            && copy.1.scope.starts_with("input:")
            && owner.1.scope.starts_with("input:")
            && copy.1.version == owner.1.version
            && self
                .owners
                .get(copy)
                .is_some_and(|owners| owners.len() == 1 && owners.contains(owner))
    }

    pub fn aliases(&self) -> Vec<MessageSourceAlias> {
        self.owners
            .iter()
            .flat_map(|(copy, owners)| {
                owners.iter().map(move |represented| MessageSourceAlias {
                    represented_thread_id: represented.0.clone(),
                    represented_source: represented.1.clone(),
                    thread_id: copy.0.clone(),
                    source: copy.1.clone(),
                })
            })
            .collect()
    }

    pub fn conflicts(&self) -> Vec<MessageSourceIdentity> {
        self.ambiguous
            .iter()
            .map(|(thread_id, source)| MessageSourceIdentity {
                thread_id: thread_id.clone(),
                source: source.clone(),
            })
            .collect()
    }
}

fn exact_input(source: &ScopedHistorySource) -> ExactMessageSource {
    (
        source.thread.clone(),
        MessageSourceRef {
            scope: source.source.scope.clone(),
            id: source.source.id.clone(),
            version: source.source.version.clone(),
        },
    )
}

pub fn summary_covers(
    covering: &BTreeSet<ScopedHistorySource>,
    covered: &BTreeSet<ScopedHistorySource>,
    claims: &ExactInputClaims,
) -> bool {
    covered.is_subset(&summary_comparison_leaves(covering, claims))
}

pub fn summary_comparison_leaves(
    covering: &BTreeSet<ScopedHistorySource>,
    claims: &ExactInputClaims,
) -> BTreeSet<ScopedHistorySource> {
    // This is an in-memory admission view; stored checkpoint coverage stays exact.
    let scoped = |source: &ExactMessageSource| ScopedHistorySource {
        thread: source.0.clone(),
        source: SourceRef {
            scope: source.1.scope.clone(),
            id: source.1.id.clone(),
            version: source.1.version.clone(),
        },
    };
    let mut edges = Vec::new();
    for (copy, owners) in &claims.owners {
        let Some(owner) = owners.iter().next() else {
            continue;
        };
        if claims.proves(copy, owner) {
            edges.push((scoped(copy), scoped(owner)));
        }
    }
    let mut comparable = covering.clone();
    loop {
        let previous_len = comparable.len();
        for (copy, owner) in &edges {
            if comparable.contains(copy) {
                comparable.insert(owner.clone());
            }
            if comparable.contains(owner) {
                comparable.insert(copy.clone());
            }
        }
        if comparable.len() == previous_len {
            break;
        }
    }
    comparable
}

pub fn summary_copy_preference(
    leaves: &BTreeSet<ScopedHistorySource>,
    claims: &ExactInputClaims,
) -> usize {
    leaves
        .iter()
        .filter(|leaf| {
            let copy = exact_input(leaf);
            claims.owners.get(&copy).is_some_and(|owners| {
                owners
                    .iter()
                    .next()
                    .is_some_and(|owner| claims.proves(&copy, owner))
            })
        })
        .count()
}

fn collect_raw_source_aliases(
    messages: &[(usize, ChatMessage)],
    claims: &mut ExactInputClaims,
) -> Result<()> {
    for (_, message) in messages {
        let origin = message
            .provenance
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("accepted history lost its source"))?;
        for alias in &origin.source_aliases {
            let represented = (
                alias.represented_thread_id.clone(),
                alias.represented_source.clone(),
            );
            ensure!(
                represented.0 == origin.thread_id && origin.sources.contains(&represented.1),
                "input alias does not identify its represented raw source"
            );
            let source = (alias.thread_id.clone(), alias.source.clone());
            if represented != source {
                claims.add_alias(alias);
            }
        }
    }
    Ok(())
}

fn apply_raw_source_aliases(
    retained: &mut [(usize, ChatMessage)],
    claims: &ExactInputClaims,
) -> Result<()> {
    let mut retained_sources = BTreeMap::<ExactMessageSource, usize>::new();
    for (message_index, (_, message)) in retained.iter().enumerate() {
        let origin = message
            .provenance
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("accepted history lost its source"))?;
        for source in &origin.sources {
            retained_sources.insert((origin.thread_id.clone(), source.clone()), message_index);
        }
    }
    let mut by_message =
        BTreeMap::<usize, BTreeSet<(ExactMessageSource, ExactMessageSource)>>::new();
    for (source, represented) in &claims.owners {
        if represented.len() != 1 {
            continue;
        }
        let represented = represented.iter().next().expect("one alias owner").clone();
        if let Some(message) = retained_sources.get(&represented) {
            by_message
                .entry(*message)
                .or_default()
                .insert((represented, source.clone()));
        }
    }
    for (message_index, (_, message)) in retained.iter_mut().enumerate() {
        let origin = message
            .provenance
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("accepted history lost its source"))?;
        origin.source_aliases = by_message
            .remove(&message_index)
            .unwrap_or_default()
            .into_iter()
            .map(|(represented, source)| MessageSourceAlias {
                represented_thread_id: represented.0,
                represented_source: represented.1,
                thread_id: source.0,
                source: source.1,
            })
            .collect();
        origin.ambiguous_input_aliases = if message_index == 0 {
            claims.conflicts()
        } else {
            Vec::new()
        };
    }
    Ok(())
}

fn merge_raw_source_aliases(
    retained: &mut [(usize, ChatMessage)],
    incoming: &[(usize, ChatMessage)],
    claims: &mut ExactInputClaims,
) -> Result<()> {
    collect_raw_source_aliases(incoming, claims)?;
    claims.mark_competing_owners();
    apply_raw_source_aliases(retained, claims)
}

fn collect_checkpoint_aliases(
    messages: &[(usize, ChatMessage)],
    leaves: &BTreeSet<ScopedHistorySource>,
    claims: &mut ExactInputClaims,
) -> Result<()> {
    for (_, message) in messages {
        let origin = message.provenance.as_ref().expect("accepted origin");
        for alias in &origin.source_aliases {
            let represented = (
                alias.represented_thread_id.clone(),
                alias.represented_source.clone(),
            );
            ensure!(
                leaves.contains(&ScopedHistorySource {
                    thread: represented.0.clone(),
                    source: SourceRef {
                        scope: represented.1.scope.clone(),
                        id: represented.1.id.clone(),
                        version: represented.1.version.clone(),
                    },
                }) && represented.1.scope.starts_with("input:")
                    && alias.source.scope.starts_with("input:"),
                "checkpoint input alias is outside its exact leaf closure"
            );
            claims.add_alias(alias);
        }
    }
    Ok(())
}

fn apply_checkpoint_aliases(unit: &mut Unit) -> Result<()> {
    ensure!(unit.checkpoint, "alias carrier is not a checkpoint");
    ensure!(
        unit.messages.len() == 1 || unit.input_aliases.owners.is_empty(),
        "checkpoint alias carrier is ambiguous"
    );
    // Primary replacement compares historical identities without revision.
    // Alias proof does not: a claim from a replaced A@v2 raw unit cannot be
    // attached to a checkpoint whose exact leaf is only A@v1. Drop the whole
    // conflicting claim, not merely the out-of-closure owner, so filtering
    // cannot manufacture a unique proof from two competing revisions.
    unit.input_aliases.owners.retain(|copy, represented| {
        let all_exact = represented.iter().all(|(thread, source)| {
            unit.leaves.contains(&ScopedHistorySource {
                thread: thread.clone(),
                source: SourceRef {
                    scope: source.scope.clone(),
                    id: source.id.clone(),
                    version: source.version.clone(),
                },
            })
        });
        if !all_exact || represented.len() > 1 {
            unit.input_aliases.ambiguous.insert(copy.clone());
        }
        all_exact
    });
    let Some((_, message)) = unit.messages.first_mut() else {
        return Ok(());
    };
    let origin = message.provenance.as_mut().expect("accepted origin");
    origin.source_aliases = unit.input_aliases.aliases();
    origin.ambiguous_input_aliases = unit.input_aliases.conflicts();
    Ok(())
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
            let mut incoming_messages = indexes
                .iter()
                .map(|index| (branch_offset + *index, branch.messages[*index].clone()))
                .collect::<Vec<_>>();
            let mut input_aliases = ExactInputClaims::default();
            input_aliases.ambiguous = incoming_messages
                .iter()
                .flat_map(|(_, message)| message.provenance.iter())
                .flat_map(|origin| &origin.ambiguous_input_aliases)
                .map(|alias| (alias.thread_id.clone(), alias.source.clone()))
                .collect::<BTreeSet<_>>();
            if checkpoint {
                collect_checkpoint_aliases(&incoming_messages, &leaves, &mut input_aliases)?;
            } else {
                collect_raw_source_aliases(&incoming_messages, &mut input_aliases)?;
                input_aliases.mark_competing_owners();
                apply_raw_source_aliases(&mut incoming_messages, &input_aliases)?;
            }
            let checkpoint_sources = unit
                .sources
                .iter()
                .filter(|source| source.scope.starts_with("checkpoint:"))
                .collect::<Vec<_>>();
            let checkpoint_domain =
                if let [source] = checkpoint_sources.as_slice() {
                    let scoped = ScopedHistorySource {
                        thread: layout.source_threads[*source].clone(),
                        source: (*source).clone(),
                    };
                    Some(*branch.checkpoint_domains.get(&scoped).ok_or_else(|| {
                        anyhow::anyhow!("checkpoint coverage domain is unavailable")
                    })?)
                } else {
                    None
                };
            let mut duplicate = None;
            let mut replaced = Vec::new();
            for (index, previous) in units.iter().enumerate() {
                if checkpoint
                    && previous.checkpoint
                    && (checkpoint_domain.is_none()
                        || checkpoint_domain != previous.checkpoint_domain
                        || (!previous.leaves.is_subset(&leaves)
                            && !leaves.is_subset(&previous.leaves)))
                {
                    continue;
                }
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
                if units[index].checkpoint {
                    let unit = &mut units[index];
                    unit.input_aliases.merge(input_aliases);
                    apply_checkpoint_aliases(unit)?;
                } else {
                    units[index].own |= own;
                    if !checkpoint {
                        let unit = &mut units[index];
                        unit.input_aliases.ambiguous.extend(input_aliases.ambiguous);
                        merge_raw_source_aliases(
                            &mut unit.messages,
                            &incoming_messages,
                            &mut unit.input_aliases,
                        )?;
                    }
                }
            } else {
                for index in &replaced {
                    input_aliases.merge(units[*index].input_aliases.clone());
                }
                for index in replaced.into_iter().rev() {
                    units.remove(index);
                }
                let mut unit = Unit {
                    messages: incoming_messages,
                    leaves,
                    identities,
                    input_aliases,
                    own,
                    checkpoint,
                    checkpoint_domain,
                };
                if unit.checkpoint {
                    apply_checkpoint_aliases(&mut unit)?;
                }
                units.push(unit);
            }
        }
        branch_offset += branch.messages.len();
    }
    let mut claims = ExactInputClaims::default();
    for unit in &units {
        claims.merge(unit.input_aliases.clone());
    }
    let mut keep = vec![true; units.len()];
    for left in 0..units.len() {
        if !keep[left] || !units[left].checkpoint {
            continue;
        }
        for right in left + 1..units.len() {
            if !keep[right]
                || !units[right].checkpoint
                || units[left].checkpoint_domain.is_none()
                || units[left].checkpoint_domain != units[right].checkpoint_domain
            {
                continue;
            }
            let left_covers = summary_covers(&units[left].leaves, &units[right].leaves, &claims);
            let right_covers = summary_covers(&units[right].leaves, &units[left].leaves, &claims);
            match (left_covers, right_covers) {
                (true, false) => keep[right] = false,
                (false, true) => {
                    keep[left] = false;
                    break;
                }
                (true, true) => {
                    if summary_copy_preference(&units[left].leaves, &claims)
                        >= summary_copy_preference(&units[right].leaves, &claims)
                    {
                        keep[right] = false;
                    } else {
                        keep[left] = false;
                        break;
                    }
                }
                (false, false) => {}
            }
        }
    }
    // A removed summary can be the only carrier of one side of a competing
    // input claim. Keep that conflict on the summary which covered it, even
    // when an independent copy was sufficient to remove the old summary.
    let mut transferred_conflicts = BTreeMap::<usize, BTreeSet<ExactMessageSource>>::new();
    for (index, unit) in units.iter().enumerate() {
        if keep[index] || !unit.checkpoint {
            continue;
        }
        let conflicts = unit
            .input_aliases
            .owners
            .keys()
            .chain(&unit.input_aliases.ambiguous)
            .filter(|copy| claims.ambiguous.contains(*copy))
            .cloned()
            .collect::<BTreeSet<_>>();
        if conflicts.is_empty() {
            continue;
        }
        let survivor = units
            .iter()
            .enumerate()
            .find(|(other, candidate)| {
                keep[*other]
                    && candidate.checkpoint_domain == unit.checkpoint_domain
                    && summary_covers(&candidate.leaves, &unit.leaves, &claims)
            })
            .map(|(other, _)| other)
            .ok_or_else(|| anyhow::anyhow!("removed summary lost its covering checkpoint"))?;
        transferred_conflicts
            .entry(survivor)
            .or_default()
            .extend(conflicts);
    }
    for (index, conflicts) in transferred_conflicts {
        units[index].input_aliases.ambiguous.extend(conflicts);
        apply_checkpoint_aliases(&mut units[index])?;
    }
    let mut messages = Vec::new();
    for (unit, keep) in units.into_iter().zip(keep) {
        if !keep {
            continue;
        }
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
    use pioneer_provider::{MessageProvenance, MessageSourceAlias, MessageSourceRef};

    fn test_domains(
        checkpoints: &BTreeMap<ScopedHistorySource, BTreeSet<ScopedHistorySource>>,
    ) -> BTreeMap<ScopedHistorySource, CoverageDomain> {
        checkpoints
            .keys()
            .cloned()
            .map(|source| (source, CoverageDomain::WorkingContext))
            .collect()
    }

    fn input_source(turn: &str, id: &str, revision: u64) -> MessageSourceRef {
        MessageSourceRef {
            scope: format!("input:{turn}"),
            id: id.into(),
            version: format!("input-revision:{revision}"),
        }
    }

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
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
            complete: true,
            protected_input: false,
            inherited,
        });
        message
    }

    fn summary_leaf(id: &str) -> ScopedHistorySource {
        ScopedHistorySource {
            thread: "parent".into(),
            source: SourceRef {
                scope: "input:turn".into(),
                id: id.into(),
                version: "input-revision:1".into(),
            },
        }
    }

    fn test_summary(id: &str, aliases: &[(&str, &str)]) -> ChatMessage {
        let mut summary = message("parent", id, true);
        let origin = summary.provenance.as_mut().unwrap();
        origin.sources[0].scope = "checkpoint:owner".into();
        origin.source_aliases = aliases
            .iter()
            .map(|(copy, owner)| MessageSourceAlias {
                represented_thread_id: "parent".into(),
                represented_source: input_source("turn", owner, 1),
                thread_id: "parent".into(),
                source: input_source("turn", copy, 1),
            })
            .collect();
        summary
    }

    fn summary_closure(
        summary: &ChatMessage,
        leaves: &[&str],
    ) -> BTreeMap<ScopedHistorySource, BTreeSet<ScopedHistorySource>> {
        let origin = summary.provenance.as_ref().unwrap();
        let source = &origin.sources[0];
        BTreeMap::from([(
            ScopedHistorySource {
                thread: origin.thread_id.clone(),
                source: SourceRef {
                    scope: source.scope.clone(),
                    id: source.id.clone(),
                    version: source.version.clone(),
                },
            },
            leaves.iter().map(|id| summary_leaf(id)).collect(),
        )])
    }

    fn compose_summaries(items: &[(&ChatMessage, &[&str], CoverageDomain)]) -> Vec<ChatMessage> {
        let messages = items
            .iter()
            .map(|(summary, _, _)| vec![(*summary).clone()])
            .collect::<Vec<_>>();
        let closures = items
            .iter()
            .map(|(summary, leaves, _)| summary_closure(summary, leaves))
            .collect::<Vec<_>>();
        let domains = items
            .iter()
            .zip(&closures)
            .map(|((_, _, domain), closure)| {
                closure.keys().cloned().map(|key| (key, *domain)).collect()
            })
            .collect::<Vec<BTreeMap<_, _>>>();
        let branches = (0..items.len())
            .map(|index| AcceptedContextBranch {
                thread: "parent",
                messages: &messages[index],
                checkpoints: &closures[index],
                checkpoint_domains: &domains[index],
            })
            .collect::<Vec<_>>();
        compose_context("ws", "child", &branches).unwrap()
    }

    fn input_message(with_alias: bool) -> ChatMessage {
        let source = input_source("turn-a", "A", 1);
        let mut message = ChatMessage::user("question");
        message.provenance = Some(MessageProvenance {
            logical_turn_id: None,
            workspace_id: "ws".into(),
            thread_id: "parent".into(),
            context_thread: None,
            unit_id: "turn-a:user-input".into(),
            sources: vec![source.clone()],
            source_aliases: with_alias
                .then(|| MessageSourceAlias {
                    represented_thread_id: "parent".into(),
                    represented_source: source,
                    thread_id: "child".into(),
                    source: input_source("turn-b", "B", 1),
                })
                .into_iter()
                .collect(),
            ambiguous_input_aliases: vec![],
            complete: true,
            protected_input: false,
            inherited: true,
        });
        message
    }

    fn conflicting_input_message(alias_owner: &str) -> ChatMessage {
        let sources = ["A", "C"]
            .into_iter()
            .map(|id| input_source("turn-a", id, 1))
            .collect::<Vec<_>>();
        let represented_source = sources
            .iter()
            .find(|source| source.id == alias_owner)
            .unwrap()
            .clone();
        let mut message = ChatMessage::user("composite question");
        message.provenance = Some(MessageProvenance {
            logical_turn_id: None,
            workspace_id: "ws".into(),
            thread_id: "parent".into(),
            context_thread: None,
            unit_id: "turn-a:user-input".into(),
            sources,
            source_aliases: vec![MessageSourceAlias {
                represented_thread_id: "parent".into(),
                represented_source,
                thread_id: "child".into(),
                source: input_source("turn-b", "B", 1),
            }],
            ambiguous_input_aliases: vec![],
            complete: true,
            protected_input: false,
            inherited: true,
        });
        message
    }

    #[test]
    fn identical_raw_branches_merge_exact_aliases_independently_of_order() {
        let without = vec![input_message(false)];
        let with = vec![input_message(true)];
        let checkpoints = BTreeMap::new();
        let domains = test_domains(&checkpoints);
        for branches in [
            [without.as_slice(), with.as_slice()],
            [with.as_slice(), without.as_slice()],
        ] {
            let composed = compose_context(
                "ws",
                "destination",
                &branches
                    .into_iter()
                    .map(|messages| AcceptedContextBranch {
                        thread: "parent",
                        messages,
                        checkpoints: &checkpoints,
                        checkpoint_domains: &domains,
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            assert_eq!(composed.len(), 1);
            let aliases = &composed[0].provenance.as_ref().unwrap().source_aliases;
            assert_eq!(aliases.len(), 1);
            assert_eq!(aliases[0].represented_source.id, "A");
            assert_eq!(aliases[0].source.id, "B");
        }
    }

    #[test]
    fn checkpoint_replacement_keeps_exact_raw_alias_in_both_orders() {
        let raw = vec![input_message(true)];
        let leaf = ScopedHistorySource {
            thread: "parent".into(),
            source: SourceRef {
                scope: "input:turn-a".into(),
                id: "A".into(),
                version: "input-revision:1".into(),
            },
        };
        let checkpoint_source = SourceRef {
            scope: "checkpoint:owner".into(),
            id: "S_A".into(),
            version: "checkpoint-identity".into(),
        };
        let mut summary = ChatMessage::user("saved summary");
        summary.provenance = Some(MessageProvenance {
            logical_turn_id: None,
            workspace_id: "ws".into(),
            thread_id: "parent".into(),
            context_thread: None,
            unit_id: "checkpoint:owner:S_A".into(),
            sources: vec![MessageSourceRef {
                scope: checkpoint_source.scope.clone(),
                id: checkpoint_source.id.clone(),
                version: checkpoint_source.version.clone(),
            }],
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
            complete: true,
            protected_input: false,
            inherited: true,
        });
        let summary = vec![summary];
        let closure = BTreeMap::from([(
            ScopedHistorySource {
                thread: "parent".into(),
                source: checkpoint_source,
            },
            BTreeSet::from([leaf]),
        )]);
        let empty = BTreeMap::new();
        for reverse in [false, true] {
            let branches = if reverse {
                [
                    AcceptedContextBranch {
                        thread: "parent",
                        messages: &summary,
                        checkpoints: &closure,
                        checkpoint_domains: &test_domains(&closure),
                    },
                    AcceptedContextBranch {
                        thread: "parent",
                        messages: &raw,
                        checkpoints: &empty,
                        checkpoint_domains: &test_domains(&empty),
                    },
                ]
            } else {
                [
                    AcceptedContextBranch {
                        thread: "parent",
                        messages: &raw,
                        checkpoints: &empty,
                        checkpoint_domains: &test_domains(&empty),
                    },
                    AcceptedContextBranch {
                        thread: "parent",
                        messages: &summary,
                        checkpoints: &closure,
                        checkpoint_domains: &test_domains(&closure),
                    },
                ]
            };
            let composed = compose_context("ws", "destination", &branches).unwrap();
            assert_eq!(composed.len(), 1);
            let origin = composed[0].provenance.as_ref().unwrap();
            assert_eq!(origin.sources[0].id, "S_A");
            assert_eq!(origin.source_aliases.len(), 1);
            assert_eq!(origin.source_aliases[0].represented_source.id, "A");
            assert_eq!(origin.source_aliases[0].source.id, "B");
            let repeated = compose_context(
                "ws",
                "destination",
                &[AcceptedContextBranch {
                    thread: "parent",
                    messages: &composed,
                    checkpoints: &closure,
                    checkpoint_domains: &test_domains(&closure),
                }],
            )
            .unwrap();
            assert_eq!(
                repeated[0]
                    .provenance
                    .as_ref()
                    .unwrap()
                    .source_aliases
                    .len(),
                1
            );
        }
    }

    #[test]
    fn checkpoint_replacement_never_transfers_another_leaf_revision_alias() {
        let mut raw_v2 = input_message(true);
        let raw_origin = raw_v2.provenance.as_mut().unwrap();
        raw_origin.sources[0].version = "input-revision:2".into();
        raw_origin.source_aliases[0].represented_source.version = "input-revision:2".into();
        let raw = vec![raw_v2];
        let checkpoint = |id: &str, version: &str, alias: bool| {
            let mut summary = ChatMessage::user(format!("summary {id}"));
            summary.provenance = Some(MessageProvenance {
                logical_turn_id: None,
                workspace_id: "ws".into(),
                thread_id: "parent".into(),
                context_thread: None,
                unit_id: format!("checkpoint:owner:{id}"),
                sources: vec![MessageSourceRef {
                    scope: "checkpoint:owner".into(),
                    id: id.into(),
                    version: format!("identity-{id}"),
                }],
                source_aliases: alias
                    .then(|| MessageSourceAlias {
                        represented_thread_id: "parent".into(),
                        represented_source: MessageSourceRef {
                            scope: "input:turn-a".into(),
                            id: "A".into(),
                            version: version.into(),
                        },
                        thread_id: "child".into(),
                        source: MessageSourceRef {
                            scope: "input:turn-b".into(),
                            id: "B".into(),
                            version: "input-revision:1".into(),
                        },
                    })
                    .into_iter()
                    .collect(),
                ambiguous_input_aliases: vec![],
                complete: true,
                protected_input: false,
                inherited: true,
            });
            (
                vec![summary],
                BTreeMap::from([(
                    ScopedHistorySource {
                        thread: "parent".into(),
                        source: SourceRef {
                            scope: "checkpoint:owner".into(),
                            id: id.into(),
                            version: format!("identity-{id}"),
                        },
                    },
                    BTreeSet::from([ScopedHistorySource {
                        thread: "parent".into(),
                        source: SourceRef {
                            scope: "input:turn-a".into(),
                            id: "A".into(),
                            version: version.into(),
                        },
                    }]),
                )]),
            )
        };
        let (v1, v1_closure) = checkpoint("S_v1", "input-revision:1", false);
        let empty = BTreeMap::new();
        for reverse in [false, true] {
            let branches = if reverse {
                [
                    AcceptedContextBranch {
                        thread: "parent",
                        messages: &v1,
                        checkpoints: &v1_closure,
                        checkpoint_domains: &test_domains(&v1_closure),
                    },
                    AcceptedContextBranch {
                        thread: "parent",
                        messages: &raw,
                        checkpoints: &empty,
                        checkpoint_domains: &test_domains(&empty),
                    },
                ]
            } else {
                [
                    AcceptedContextBranch {
                        thread: "parent",
                        messages: &raw,
                        checkpoints: &empty,
                        checkpoint_domains: &test_domains(&empty),
                    },
                    AcceptedContextBranch {
                        thread: "parent",
                        messages: &v1,
                        checkpoints: &v1_closure,
                        checkpoint_domains: &test_domains(&v1_closure),
                    },
                ]
            };
            let composed = compose_context("ws", "destination", &branches).unwrap();
            assert_eq!(composed.len(), 1);
            assert_eq!(
                composed[0].provenance.as_ref().unwrap().sources[0].id,
                "S_v1"
            );
            assert!(
                composed[0]
                    .provenance
                    .as_ref()
                    .unwrap()
                    .source_aliases
                    .is_empty()
            );
            let repeated = compose_context(
                "ws",
                "destination",
                &[AcceptedContextBranch {
                    thread: "parent",
                    messages: &composed,
                    checkpoints: &v1_closure,
                    checkpoint_domains: &test_domains(&v1_closure),
                }],
            )
            .unwrap();
            assert_eq!(repeated, composed);
        }
        let (v2, v2_closure) = checkpoint("S_v2", "input-revision:2", true);
        for (first, first_closure, second, second_closure, expected) in [
            (&v1, &v1_closure, &v2, &v2_closure, 0),
            (&v2, &v2_closure, &v1, &v1_closure, 1),
        ] {
            let composed = compose_context(
                "ws",
                "destination",
                &[
                    AcceptedContextBranch {
                        thread: "parent",
                        messages: first,
                        checkpoints: first_closure,
                        checkpoint_domains: &test_domains(first_closure),
                    },
                    AcceptedContextBranch {
                        thread: "parent",
                        messages: second,
                        checkpoints: second_closure,
                        checkpoint_domains: &test_domains(second_closure),
                    },
                ],
            )
            .unwrap();
            assert_eq!(composed.len(), 1);
            assert_eq!(
                composed[0]
                    .provenance
                    .as_ref()
                    .unwrap()
                    .source_aliases
                    .len(),
                expected
            );
        }
    }

    #[test]
    fn independent_checkpoint_branches_keep_conflicting_input_proofs_separate() {
        let checkpoint = |id: &str, represented: &str| {
            let mut message = ChatMessage::user(format!("summary {id}"));
            message.provenance = Some(MessageProvenance {
                logical_turn_id: None,
                workspace_id: "ws".into(),
                thread_id: "parent".into(),
                context_thread: None,
                unit_id: format!("checkpoint:owner:{id}"),
                sources: vec![MessageSourceRef {
                    scope: "checkpoint:owner".into(),
                    id: id.into(),
                    version: format!("identity-{id}"),
                }],
                source_aliases: vec![MessageSourceAlias {
                    represented_thread_id: "parent".into(),
                    represented_source: MessageSourceRef {
                        scope: "input:turn-a".into(),
                        id: represented.into(),
                        version: "input-revision:1".into(),
                    },
                    thread_id: "child".into(),
                    source: MessageSourceRef {
                        scope: "input:turn-b".into(),
                        id: "B".into(),
                        version: "input-revision:1".into(),
                    },
                }],
                ambiguous_input_aliases: vec![],
                complete: true,
                protected_input: false,
                inherited: true,
            });
            message
        };
        let branches = [vec![checkpoint("S_A", "A")], vec![checkpoint("S_C", "C")]];
        let closures = ["A", "C"]
            .into_iter()
            .map(|leaf| {
                BTreeMap::from([(
                    ScopedHistorySource {
                        thread: "parent".into(),
                        source: SourceRef {
                            scope: "checkpoint:owner".into(),
                            id: format!("S_{}", leaf),
                            version: format!("identity-S_{leaf}"),
                        },
                    },
                    BTreeSet::from([ScopedHistorySource {
                        thread: "parent".into(),
                        source: SourceRef {
                            scope: "input:turn-a".into(),
                            id: leaf.into(),
                            version: "input-revision:1".into(),
                        },
                    }]),
                )])
            })
            .collect::<Vec<_>>();
        for order in [[0, 1], [1, 0]] {
            let domains = closures.iter().map(test_domains).collect::<Vec<_>>();
            let composed = compose_context(
                "ws",
                "destination",
                &order.map(|index| AcceptedContextBranch {
                    thread: "parent",
                    messages: &branches[index],
                    checkpoints: &closures[index],
                    checkpoint_domains: &domains[index],
                }),
            )
            .unwrap();
            assert_eq!(composed.len(), 2);
            let owners = composed
                .iter()
                .map(|message| {
                    message.provenance.as_ref().unwrap().source_aliases[0]
                        .represented_source
                        .id
                        .as_str()
                })
                .collect::<BTreeSet<_>>();
            assert_eq!(owners, BTreeSet::from(["A", "C"]));
        }
    }

    #[test]
    fn absorbed_checkpoint_keeps_conflicting_input_evidence_in_both_orders() {
        let leaf = |id: &str| ScopedHistorySource {
            thread: "parent".into(),
            source: SourceRef {
                scope: "input:turn-a".into(),
                id: id.into(),
                version: "input-revision:1".into(),
            },
        };
        let copy = MessageSourceRef {
            scope: "input:turn-b".into(),
            id: "B".into(),
            version: "input-revision:1".into(),
        };
        let checkpoint = |id: &str, represented: &str| {
            let mut message = ChatMessage::user(format!("summary {id}"));
            message.provenance = Some(MessageProvenance {
                logical_turn_id: None,
                workspace_id: "ws".into(),
                thread_id: "parent".into(),
                context_thread: None,
                unit_id: format!("checkpoint:owner:{id}"),
                sources: vec![MessageSourceRef {
                    scope: "checkpoint:owner".into(),
                    id: id.into(),
                    version: format!("identity-{id}"),
                }],
                source_aliases: vec![MessageSourceAlias {
                    represented_thread_id: "parent".into(),
                    represented_source: MessageSourceRef {
                        scope: "input:turn-a".into(),
                        id: represented.into(),
                        version: "input-revision:1".into(),
                    },
                    thread_id: "child".into(),
                    source: copy.clone(),
                }],
                ambiguous_input_aliases: vec![],
                complete: true,
                protected_input: false,
                inherited: true,
            });
            message
        };
        let branches = [vec![checkpoint("S_A", "A")], vec![checkpoint("S_AC", "C")]];
        let closures = [
            BTreeMap::from([(
                ScopedHistorySource {
                    thread: "parent".into(),
                    source: SourceRef {
                        scope: "checkpoint:owner".into(),
                        id: "S_A".into(),
                        version: "identity-S_A".into(),
                    },
                },
                BTreeSet::from([leaf("A")]),
            )]),
            BTreeMap::from([(
                ScopedHistorySource {
                    thread: "parent".into(),
                    source: SourceRef {
                        scope: "checkpoint:owner".into(),
                        id: "S_AC".into(),
                        version: "identity-S_AC".into(),
                    },
                },
                BTreeSet::from([leaf("A"), leaf("C")]),
            )]),
        ];
        for order in [[0, 1], [1, 0]] {
            let domains = closures.iter().map(test_domains).collect::<Vec<_>>();
            let composed = compose_context(
                "ws",
                "destination",
                &order.map(|index| AcceptedContextBranch {
                    thread: "parent",
                    messages: &branches[index],
                    checkpoints: &closures[index],
                    checkpoint_domains: &domains[index],
                }),
            )
            .unwrap();
            assert_eq!(composed.len(), 1);
            let origin = composed[0].provenance.as_ref().unwrap();
            assert_eq!(origin.sources[0].id, "S_AC");
            assert!(
                origin
                    .ambiguous_input_aliases
                    .iter()
                    .any(|marker| { marker.thread_id == "child" && marker.source == copy })
            );
            let repeated = compose_context(
                "ws",
                "destination",
                &[AcceptedContextBranch {
                    thread: "parent",
                    messages: &composed,
                    checkpoints: &closures[1],
                    checkpoint_domains: &test_domains(&closures[1]),
                }],
            )
            .unwrap();
            assert_eq!(repeated, composed);
        }
    }

    #[test]
    fn conflicting_raw_alias_owners_are_dropped_independently_of_order() {
        let a = vec![conflicting_input_message("A")];
        let c = vec![conflicting_input_message("C")];
        let checkpoints = BTreeMap::new();
        let domains = test_domains(&checkpoints);
        for branches in [[a.as_slice(), c.as_slice()], [c.as_slice(), a.as_slice()]] {
            let composed = compose_context(
                "ws",
                "destination",
                &branches
                    .into_iter()
                    .map(|messages| AcceptedContextBranch {
                        thread: "parent",
                        messages,
                        checkpoints: &checkpoints,
                        checkpoint_domains: &domains,
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            assert_eq!(composed.len(), 1);
            assert!(
                composed[0]
                    .provenance
                    .as_ref()
                    .unwrap()
                    .source_aliases
                    .is_empty()
            );
        }
    }

    #[test]
    fn equal_raw_source_combines_large_disjoint_alias_proofs_in_both_orders() {
        let branches = [0, 1].map(|branch| {
            let mut message = input_message(false);
            let origin = message.provenance.as_mut().unwrap();
            origin.source_aliases = (0..129)
                .map(|index| MessageSourceAlias {
                    represented_thread_id: "parent".into(),
                    represented_source: origin.sources[0].clone(),
                    thread_id: format!("copy-thread-{branch}-{index}"),
                    source: MessageSourceRef {
                        scope: format!("input:copy-turn-{branch}-{index}"),
                        id: format!("copy-{branch}-{index}"),
                        version: "input-revision:1".into(),
                    },
                })
                .collect();
            vec![message]
        });
        let checkpoints = BTreeMap::new();
        let domains = test_domains(&checkpoints);
        for order in [[0, 1], [1, 0]] {
            let composed = compose_context(
                "ws",
                "destination",
                &order.map(|index| AcceptedContextBranch {
                    thread: "parent",
                    messages: &branches[index],
                    checkpoints: &checkpoints,
                    checkpoint_domains: &domains,
                }),
            )
            .unwrap();
            assert_eq!(composed.len(), 1);
            let aliases = &composed[0].provenance.as_ref().unwrap().source_aliases;
            assert_eq!(aliases.len(), 258);
            assert!(aliases.iter().any(|alias| alias.source.id == "copy-0-0"));
            assert!(aliases.iter().any(|alias| alias.source.id == "copy-1-128"));
        }
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
                    checkpoint_domains: &test_domains(&closures),
                },
                AcceptedContextBranch {
                    thread: "B",
                    messages: &b,
                    checkpoints: &closures,
                    checkpoint_domains: &test_domains(&closures),
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
                        checkpoints: &closures,
                        checkpoint_domains: &test_domains(&closures),
                    },
                    AcceptedContextBranch {
                        thread: "B",
                        messages: &b,
                        checkpoints: &closures,
                        checkpoint_domains: &test_domains(&closures),
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
                    checkpoints: &closures,
                    checkpoint_domains: &test_domains(&closures),
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
                    checkpoint_domains: &test_domains(&closures),
                },
                AcceptedContextBranch {
                    thread: "parent",
                    messages: &summary,
                    checkpoints: &closures,
                    checkpoint_domains: &test_domains(&closures),
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
                    checkpoint_domains: &test_domains(&closures),
                },
                AcceptedContextBranch {
                    thread: "parent",
                    messages: std::slice::from_ref(&b),
                    checkpoints: &closures,
                    checkpoint_domains: &test_domains(&closures),
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
                checkpoint_domains: &test_domains(&closures),
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

    #[test]
    fn confirmed_copies_select_covering_summary_in_both_orders_and_again() {
        let old = test_summary("S_A", &[("B", "A"), ("C", "A")]);
        let covering = test_summary("S_BC", &[]);
        for reverse in [false, true] {
            let items = if reverse {
                [
                    (&covering, &["B", "C"][..], CoverageDomain::WorkingContext),
                    (&old, &["A"][..], CoverageDomain::WorkingContext),
                ]
            } else {
                [
                    (&old, &["A"][..], CoverageDomain::WorkingContext),
                    (&covering, &["B", "C"][..], CoverageDomain::WorkingContext),
                ]
            };
            let selected = compose_summaries(&items);
            assert_eq!(selected.len(), 1);
            assert_eq!(
                selected[0].provenance.as_ref().unwrap().sources[0].id,
                "S_BC"
            );
            let repeated = compose_summaries(&[
                (&selected[0], &["B", "C"], CoverageDomain::WorkingContext),
                (&old, &["A"], CoverageDomain::WorkingContext),
            ]);
            assert_eq!(repeated.len(), 1);
            assert_eq!(
                repeated[0].provenance.as_ref().unwrap().sources[0].id,
                "S_BC"
            );
        }
        let copy_b = test_summary("S_B", &[]);
        let copy_c = test_summary("S_C", &[]);
        for (first, first_leaf, second, second_leaf) in
            [(&copy_b, "B", &copy_c, "C"), (&copy_c, "C", &copy_b, "B")]
        {
            let selected = compose_summaries(&[
                (&old, &["A"], CoverageDomain::WorkingContext),
                (first, &[first_leaf], CoverageDomain::WorkingContext),
                (second, &[second_leaf], CoverageDomain::WorkingContext),
            ]);
            assert_eq!(selected.len(), 1);
            assert_eq!(
                selected[0].provenance.as_ref().unwrap().sources[0].id,
                first.provenance.as_ref().unwrap().sources[0].id
            );
        }
    }

    #[test]
    fn summary_comparison_preserves_partial_unproven_and_other_domain_history() {
        let old = test_summary("S_A", &[("B", "A"), ("C", "A")]);
        let covering = test_summary("S_BC", &[]);
        let working = CoverageDomain::WorkingContext;
        assert_eq!(
            compose_summaries(&[
                (&old, &["A", "unique"], working),
                (&covering, &["B", "C"], working),
            ])
            .len(),
            2
        );
        let unproven = test_summary("S_A", &[]);
        assert_eq!(
            compose_summaries(&[
                (&unproven, &["A"], working),
                (&covering, &["B", "C"], working),
            ])
            .len(),
            2
        );
        let mut claims = ExactInputClaims::default();
        claims.add_alias(&old.provenance.as_ref().unwrap().source_aliases[0]);
        let mut revised_copy = summary_leaf("B");
        revised_copy.source.version = "input-revision:2".into();
        assert!(!summary_covers(
            &BTreeSet::from([revised_copy]),
            &BTreeSet::from([summary_leaf("A")]),
            &claims,
        ));
        let mut chain = ExactInputClaims::default();
        let b_to_a = test_summary("S_A", &[("B", "A")]);
        chain.add_alias(&b_to_a.provenance.as_ref().unwrap().source_aliases[0]);
        let a_to_root = test_summary("S_root", &[("A", "root")]);
        chain.add_alias(&a_to_root.provenance.as_ref().unwrap().source_aliases[0]);
        assert!(summary_covers(
            &BTreeSet::from([summary_leaf("B")]),
            &BTreeSet::from([summary_leaf("root")]),
            &chain,
        ));
        let conflict = test_summary("S_other", &[("A", "other")]);
        chain.add_alias(&conflict.provenance.as_ref().unwrap().source_aliases[0]);
        chain.mark_competing_owners();
        assert!(!summary_covers(
            &BTreeSet::from([summary_leaf("B")]),
            &BTreeSet::from([summary_leaf("root")]),
            &chain,
        ));
        let revised = test_summary("S_A_revised", &[]);
        let mut revised_closure = summary_closure(&revised, &["A"]);
        let mut revised_leaf = summary_leaf("A");
        revised_leaf.source.version = "input-revision:2".into();
        *revised_closure.values_mut().next().unwrap() = BTreeSet::from([revised_leaf]);
        let old_closure = summary_closure(&old, &["A"]);
        let old_messages = [old.clone()];
        let revised_messages = [revised];
        let different_revisions = compose_context(
            "ws",
            "child",
            &[
                AcceptedContextBranch {
                    thread: "parent",
                    messages: &old_messages,
                    checkpoints: &old_closure,
                    checkpoint_domains: &test_domains(&old_closure),
                },
                AcceptedContextBranch {
                    thread: "parent",
                    messages: &revised_messages,
                    checkpoints: &revised_closure,
                    checkpoint_domains: &test_domains(&revised_closure),
                },
            ],
        )
        .unwrap();
        assert_eq!(different_revisions.len(), 2);
        assert_eq!(
            compose_summaries(&[
                (&old, &["A"], CoverageDomain::OwnContribution),
                (&covering, &["B", "C"], working),
            ])
            .len(),
            2
        );
        let conflict = test_summary("S_X", &[("B", "X")]);
        for reverse in [false, true] {
            let mut branches = vec![
                (&old, &["A"][..], working),
                (&covering, &["B", "C"][..], working),
                (&conflict, &["X"][..], working),
            ];
            if reverse {
                branches.reverse();
            }
            let selected = compose_summaries(&branches);
            let ids = |messages: &[ChatMessage]| {
                messages
                    .iter()
                    .map(|message| message.provenance.as_ref().unwrap().sources[0].id.clone())
                    .collect::<BTreeSet<_>>()
            };
            assert_eq!(
                ids(&selected),
                BTreeSet::from(["S_BC".into(), "S_X".into()])
            );
            let b = MessageSourceIdentity {
                thread_id: "parent".into(),
                source: input_source("turn", "B", 1),
            };
            assert!(selected.iter().any(|message| {
                message
                    .provenance
                    .as_ref()
                    .unwrap()
                    .ambiguous_input_aliases
                    .contains(&b)
            }));
            let mut closures = summary_closure(&covering, &["B", "C"]);
            closures.extend(summary_closure(&conflict, &["X"]));
            let repeated = compose_context(
                "ws",
                "grandchild",
                &[AcceptedContextBranch {
                    thread: "child",
                    messages: &selected,
                    checkpoints: &closures,
                    checkpoint_domains: &test_domains(&closures),
                }],
            )
            .unwrap();
            assert_eq!(
                ids(&repeated),
                BTreeSet::from(["S_BC".into(), "S_X".into()])
            );
        }
    }
}
