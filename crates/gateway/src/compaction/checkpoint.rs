//! Materialize a published checkpoint from exact request-source coverage. This
//! never guesses that a count, timestamp, or equal text represents a source.
use super::*;
use pioneer_provider::{
    ChatMessage, MessageProvenance, MessageSourceAlias, MessageSourceIdentity, MessageSourceRef,
};
use std::collections::{BTreeMap, BTreeSet};

const MAX_CHECKPOINT_ANCESTRY: usize = 65_536;

#[derive(Debug)]
struct ProjectionBoundary(&'static str);
impl std::fmt::Display for ProjectionBoundary {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}
impl std::error::Error for ProjectionBoundary {}

pub(super) struct ProjectionContext<'a> {
    pub(super) workspace: &'a str,
    pub(super) context_thread: &'a str,
    pub(super) source_thread: &'a str,
    pub(super) owner: &'a str,
    pub(super) allowed: &'a BTreeSet<String>,
    /// Only a head captured as part of this context's accepted boundary may
    /// stand in for covered rows that have since been deleted. Foreign/frozen
    /// candidates must still fit their immutable represented boundary.
    pub(super) allow_historical_gaps: bool,
}

impl<'a> ProjectionContext<'a> {
    #[cfg(test)]
    fn local(
        workspace: &'a str,
        thread: &'a str,
        owner: &'a str,
        allowed: &'a BTreeSet<String>,
    ) -> Self {
        Self {
            workspace,
            context_thread: thread,
            source_thread: thread,
            owner,
            allowed,
            allow_historical_gaps: true,
        }
    }
}

/// Select a whole compatible checkpoint from the captured head's ancestry.
/// A later summary must never import work past a TaskRun/fork boundary. Failure
/// to find an exact projection leaves the original selected history intact;
/// storage errors and malformed coverage remain errors, not a silent fallback.
#[cfg(test)]
pub(crate) async fn project_compatible_checkpoint(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    owner: &str,
    head: &str,
    allowed: &BTreeSet<String>,
    messages: &mut Vec<ChatMessage>,
) -> Result<Option<String>> {
    let mut resolver = super::coverage::CheckpointGraphResolver::default();
    project_compatible_checkpoint_with_resolver(
        store,
        ProjectionContext {
            workspace,
            context_thread: thread,
            source_thread: thread,
            owner,
            allowed,
            allow_historical_gaps: false,
        },
        head,
        messages,
        &mut resolver,
    )
    .await
}

pub(super) async fn project_compatible_checkpoint_with_resolver(
    store: &CrudStore,
    context: ProjectionContext<'_>,
    head: &str,
    messages: &mut Vec<ChatMessage>,
    resolver: &mut super::coverage::CheckpointGraphResolver,
) -> Result<Option<String>> {
    let mut candidate = Some(head.to_owned());
    let mut seen = BTreeSet::new();
    while let Some(id) = candidate {
        ensure!(seen.insert(id.clone()), "cyclic checkpoint ancestry");
        ensure!(
            seen.len() <= MAX_CHECKPOINT_ANCESTRY,
            "checkpoint ancestry exceeds supported quantum"
        );
        let checkpoint = resolver
            .ancestry_edges(store, context.workspace, &id)
            .await?;
        ensure!(
            checkpoint.owner == context.owner,
            "checkpoint belongs to another context"
        );
        if store
            .compaction_checkpoint_source(context.workspace, context.source_thread, &id)
            .await?
            .is_some()
        {
            match project_checkpoint_in_context(store, &context, &id, messages, resolver).await {
                Ok(()) => return Ok(Some(id)),
                Err(error) if error.downcast_ref::<ProjectionBoundary>().is_some() => {}
                Err(error) => return Err(error),
            }
        }
        candidate = checkpoint.previous;
    }
    Ok(None)
}

/// Reuse completed work from accepted child contexts without changing their
/// immutable output manifests. A pure contribution replaces only accepted OWN
/// work. A working-context checkpoint may also replace its exact accepted H,
/// but remains inherited and is never exported as the child's contribution.
#[cfg(test)]
pub(crate) async fn project_accepted_checkpoints(
    store: &CrudStore,
    workspace: &str,
    context_thread: &str,
    allowed: &BTreeSet<String>,
    messages: &mut Vec<ChatMessage>,
) -> Result<()> {
    let mut resolver = super::coverage::CheckpointGraphResolver::default();
    project_accepted_checkpoints_with_resolver(
        store,
        workspace,
        context_thread,
        allowed,
        messages,
        &mut resolver,
    )
    .await
}

pub(super) async fn project_accepted_checkpoints_with_resolver(
    store: &CrudStore,
    workspace: &str,
    context_thread: &str,
    allowed: &BTreeSet<String>,
    messages: &mut Vec<ChatMessage>,
    resolver: &mut super::coverage::CheckpointGraphResolver,
) -> Result<()> {
    project_accepted_checkpoints_with_boundary_evidence(
        store,
        workspace,
        context_thread,
        allowed,
        messages,
        None,
        None,
        resolver,
    )
    .await
}

pub(super) struct ProjectionBoundaryEvidence<'a> {
    /// The authenticated, ordered source boundary before model-visibility
    /// filtering. These messages are used only to admit a checkpoint and are
    /// never copied into the projected model history.
    pub messages: &'a [ChatMessage],
    /// For each model-visible message, its exact ordinal in `messages`.
    pub model_ordinals: &'a [usize],
}

pub(super) async fn project_accepted_checkpoints_with_boundary_evidence(
    store: &CrudStore,
    workspace: &str,
    context_thread: &str,
    allowed: &BTreeSet<String>,
    messages: &mut Vec<ChatMessage>,
    boundary: Option<ProjectionBoundaryEvidence<'_>>,
    mut replacements: Option<
        &mut BTreeMap<pioneer_agent::compaction::composition::ScopedHistorySource, BTreeSet<usize>>,
    >,
    resolver: &mut super::coverage::CheckpointGraphResolver,
) -> Result<()> {
    if let Some(boundary) = &boundary {
        ensure!(
            boundary.model_ordinals.len() == messages.len()
                && boundary
                    .model_ordinals
                    .iter()
                    .enumerate()
                    .all(|(index, ordinal)| {
                        boundary
                            .messages
                            .get(*ordinal)
                            .and_then(|message| message.provenance.as_ref())
                            == messages
                                .get(index)
                                .and_then(|message| message.provenance.as_ref())
                    }),
            "checkpoint boundary evidence does not match model history"
        );
    }
    let admitted_messages = boundary
        .as_ref()
        .map_or(messages.as_slice(), |boundary| boundary.messages);
    // Discovery follows only source owners that are actually represented in
    // the accepted request. Inherited H may have been frozen before its owner
    // published a working-context checkpoint, so it is a candidate source even
    // without an OWN import. This is not an application grant: the published
    // root scope and exact whole-message boundary are checked below.
    let threads: BTreeSet<_> = admitted_messages
        .iter()
        .filter_map(|message| {
            let origin = message.provenance.as_ref()?;
            let context_owner = origin
                .context_thread
                .as_deref()
                .unwrap_or(&origin.thread_id);
            (origin.thread_id != context_thread
                && (origin.inherited || context_owner == context_thread))
                .then(|| origin.thread_id.clone())
        })
        .collect();
    for source_thread in threads {
        ensure!(
            allowed.contains(&source_thread),
            "checkpoint source scope is not accepted"
        );
        let owner = super::native::native_owner(workspace, &source_thread);
        let mut candidate = store.compaction_head(&owner).await?;
        let mut seen = BTreeSet::new();
        while let Some(id) = candidate {
            ensure!(seen.insert(id.clone()), "cyclic checkpoint ancestry");
            ensure!(
                seen.len() <= MAX_CHECKPOINT_ANCESTRY,
                "checkpoint ancestry exceeds supported quantum"
            );
            let edges = resolver.ancestry_edges(store, workspace, &id).await?;
            ensure!(
                edges.owner == owner,
                "checkpoint belongs to another context"
            );
            if store
                .compaction_checkpoint_source(workspace, &source_thread, &id)
                .await?
                .is_some()
            {
                match project_checkpoint_in_context_with_boundary(
                    store,
                    &ProjectionContext {
                        workspace,
                        context_thread,
                        source_thread: &source_thread,
                        owner: &owner,
                        allowed,
                        allow_historical_gaps: false,
                    },
                    &id,
                    messages,
                    boundary.as_ref(),
                    resolver,
                )
                .await
                {
                    Ok(selected_boundary) => {
                        if !selected_boundary.is_empty()
                            && let Some(replacements) = replacements.as_deref_mut()
                        {
                            let source = store
                                .compaction_checkpoint_source(workspace, &source_thread, &id)
                                .await?
                                .ok_or_else(|| {
                                    anyhow::anyhow!("projected checkpoint disappeared")
                                })?;
                            replacements.insert(
                                pioneer_agent::compaction::composition::ScopedHistorySource {
                                    thread: source_thread.clone(),
                                    source,
                                },
                                selected_boundary,
                            );
                        }
                        break;
                    }
                    Err(error) if error.downcast_ref::<ProjectionBoundary>().is_some() => {}
                    Err(error) => return Err(error),
                }
            }
            candidate = edges.previous;
        }
    }
    Ok(())
}

#[derive(Clone)]
struct Expanded {
    root: SourceRef,
    leaves: BTreeSet<pioneer_agent::compaction::composition::ScopedHistorySource>,
    replay_aliases: BTreeMap<
        pioneer_agent::compaction::composition::ScopedHistorySource,
        pioneer_agent::compaction::composition::ScopedHistorySource,
    >,
    input_replay_aliases: BTreeSet<pioneer_agent::compaction::composition::ScopedHistorySource>,
    ambiguous_input_aliases: BTreeSet<pioneer_agent::compaction::composition::ScopedHistorySource>,
    emergency_inputs: BTreeSet<pioneer_agent::compaction::composition::ScopedHistorySource>,
    coverage_domain: pioneer_compaction::CoverageDomain,
}

async fn expand(
    store: &CrudStore,
    head: &str,
    context: &ProjectionContext<'_>,
    resolver: &mut super::coverage::CheckpointGraphResolver,
) -> Result<Expanded> {
    let root = store
        .compaction_checkpoint_source(context.workspace, context.source_thread, head)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint is not a published scoped source"))?;
    ensure!(
        root.scope == format!("checkpoint:{}", context.owner),
        "checkpoint owner mismatch"
    );
    // Historical coverage proves only the accepted boundary. The published
    // root itself was checked above; its old leaves need not be live today.
    let graph = resolver
        .resolve(store, context.workspace, Some(context.allowed), &root)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint root is unavailable"))?;
    let leaves = graph.leaves.clone();
    let prepared = resolver
        .projection_metadata(store, context.workspace, &graph)
        .await?;
    ensure!(!leaves.is_empty(), "checkpoint has no historical coverage");
    Ok(Expanded {
        root,
        leaves,
        replay_aliases: graph.replay_aliases.clone(),
        input_replay_aliases: graph.input_replay_aliases.clone(),
        ambiguous_input_aliases: graph.ambiguous_input_aliases.clone(),
        emergency_inputs: prepared.emergency_inputs,
        coverage_domain: prepared.coverage_domain,
    })
}

/// Load one published checkpoint as an atomic history message. Boundary
/// selection is performed by the caller; this helper validates and loads only
/// the selected root object and preserves its OWN/WorkingContext provenance.
pub(super) async fn checkpoint_message_with_resolver(
    store: &CrudStore,
    context: ProjectionContext<'_>,
    head: &str,
    resolver: &mut super::coverage::CheckpointGraphResolver,
) -> Result<ChatMessage> {
    let expanded = expand(store, head, &context, resolver).await?;
    checkpoint_message_from_expanded(store, &context, head, &expanded, resolver).await
}

async fn checkpoint_message_from_expanded(
    store: &CrudStore,
    context: &ProjectionContext<'_>,
    head: &str,
    expanded: &Expanded,
    resolver: &mut super::coverage::CheckpointGraphResolver,
) -> Result<ChatMessage> {
    let checkpoint = resolver
        .projection_body(store, context.workspace, &expanded.root)
        .await?;
    // Body loading may yield. Recheck the root object and identity, not the
    // historical leaves that produced it.
    let source = store
        .compaction_checkpoint_source(context.workspace, context.source_thread, head)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint was invalidated during projection"))?;
    ensure!(
        source == expanded.root,
        "checkpoint identity changed during projection"
    );
    ensure!(
        checkpoint.id == source.id && checkpoint.identity_sha256 == source.version,
        "checkpoint body identity changed during projection"
    );
    let mut summary = ChatMessage::user(format!(
        "Summary of completed work (historical data):\n{}",
        checkpoint.summary
    ));
    summary.provenance = Some(MessageProvenance {
        logical_turn_id: None,
        workspace_id: context.workspace.into(),
        thread_id: context.source_thread.into(),
        context_thread: (context.source_thread != context.context_thread)
            .then(|| context.context_thread.into()),
        unit_id: format!("checkpoint:{}:{head}", context.owner),
        sources: vec![MessageSourceRef {
            scope: source.scope,
            id: source.id,
            version: source.version,
        }],
        source_aliases: expanded
            .replay_aliases
            .iter()
            .filter(|(replay, _)| expanded.input_replay_aliases.contains(*replay))
            .map(|(replay, represented)| MessageSourceAlias {
                represented_thread_id: represented.thread.clone(),
                represented_source: MessageSourceRef {
                    scope: represented.source.scope.clone(),
                    id: represented.source.id.clone(),
                    version: represented.source.version.clone(),
                },
                thread_id: replay.thread.clone(),
                source: MessageSourceRef {
                    scope: replay.source.scope.clone(),
                    id: replay.source.id.clone(),
                    version: replay.source.version.clone(),
                },
            })
            .collect(),
        ambiguous_input_aliases: expanded
            .ambiguous_input_aliases
            .iter()
            .map(|replay| MessageSourceIdentity {
                thread_id: replay.thread.clone(),
                source: MessageSourceRef {
                    scope: replay.source.scope.clone(),
                    id: replay.source.id.clone(),
                    version: replay.source.version.clone(),
                },
            })
            .collect(),
        complete: true,
        protected_input: false,
        inherited: expanded.coverage_domain == pioneer_compaction::CoverageDomain::WorkingContext,
    });
    Ok(summary)
}

/// A replacement summary may carry suppression proof for an exact input leaf
/// that it covers. The represented source stays that leaf, never the summary
/// source; conflicting claims are retained as independent inputs.
pub(super) struct InputAliasEvidence {
    pub aliases: Vec<MessageSourceAlias>,
    pub ambiguous: Vec<MessageSourceIdentity>,
}

pub(super) fn append_graph_input_evidence(
    graph: &super::coverage::ResolvedCheckpointGraph,
    aliases: &mut Vec<MessageSourceAlias>,
    ambiguous: &mut Vec<MessageSourceIdentity>,
) {
    aliases.extend(
        graph
            .replay_aliases
            .iter()
            .filter_map(|(copy, represented)| {
                graph
                    .input_replay_aliases
                    .contains(copy)
                    .then(|| MessageSourceAlias {
                        represented_thread_id: represented.thread.clone(),
                        represented_source: MessageSourceRef {
                            scope: represented.source.scope.clone(),
                            id: represented.source.id.clone(),
                            version: represented.source.version.clone(),
                        },
                        thread_id: copy.thread.clone(),
                        source: MessageSourceRef {
                            scope: copy.source.scope.clone(),
                            id: copy.source.id.clone(),
                            version: copy.source.version.clone(),
                        },
                    })
            }),
    );
    ambiguous.extend(
        graph
            .ambiguous_input_aliases
            .iter()
            .map(|copy| MessageSourceIdentity {
                thread_id: copy.thread.clone(),
                source: MessageSourceRef {
                    scope: copy.source.scope.clone(),
                    id: copy.source.id.clone(),
                    version: copy.source.version.clone(),
                },
            }),
    );
}

/// Add immutable graph evidence to a selected checkpoint message. This is a
/// context-preparation copy; neither the published summary nor its frozen
/// literal representation is rewritten.
pub(super) fn merge_graph_input_evidence(
    origin: &mut MessageProvenance,
    graph: &super::coverage::ResolvedCheckpointGraph,
) {
    let mut aliases = origin.source_aliases.clone();
    let mut ambiguous = origin.ambiguous_input_aliases.clone();
    append_graph_input_evidence(graph, &mut aliases, &mut ambiguous);
    let evidence = transferred_input_evidence(&graph.leaves, &aliases, &ambiguous);
    origin.source_aliases = evidence.aliases;
    origin.ambiguous_input_aliases = evidence.ambiguous;
}

pub(super) fn transferred_input_evidence<'a>(
    leaves: &BTreeSet<pioneer_agent::compaction::composition::ScopedHistorySource>,
    aliases: impl IntoIterator<Item = &'a pioneer_provider::MessageSourceAlias>,
    ambiguous: impl IntoIterator<Item = &'a MessageSourceIdentity>,
) -> InputAliasEvidence {
    use pioneer_agent::compaction::composition::ScopedHistorySource;
    let mut claims = pioneer_agent::compaction::composition::ExactInputClaims::default();
    claims.ambiguous = ambiguous
        .into_iter()
        .map(|alias| (alias.thread_id.clone(), alias.source.clone()))
        .collect::<BTreeSet<_>>();
    for alias in aliases {
        let represented = ScopedHistorySource {
            thread: alias.represented_thread_id.clone(),
            source: SourceRef {
                scope: alias.represented_source.scope.clone(),
                id: alias.represented_source.id.clone(),
                version: alias.represented_source.version.clone(),
            },
        };
        if represented.source.scope.starts_with("input:")
            && alias.source.scope.starts_with("input:")
        {
            if !leaves.contains(&represented) {
                claims
                    .ambiguous
                    .insert((alias.thread_id.clone(), alias.source.clone()));
                continue;
            }
            claims.add_alias(alias);
        }
    }
    claims.mark_competing_owners();
    InputAliasEvidence {
        aliases: claims.aliases(),
        ambiguous: claims.conflicts(),
    }
}

/// Request origin locators and accepted scopes have already been resolved. A
/// checkpoint that crosses a fork/snapshot boundary cannot be inserted here;
/// exact-current validation of the raw rows left after projection follows.
#[cfg(test)]
pub(crate) async fn project_checkpoint(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    owner: &str,
    head: &str,
    allowed: &BTreeSet<String>,
    messages: &mut Vec<ChatMessage>,
) -> Result<()> {
    let mut resolver = super::coverage::CheckpointGraphResolver::default();
    project_checkpoint_with_resolver(
        store,
        ProjectionContext::local(workspace, thread, owner, allowed),
        head,
        messages,
        &mut resolver,
    )
    .await
}

pub(super) async fn project_checkpoint_with_resolver(
    store: &CrudStore,
    context: ProjectionContext<'_>,
    head: &str,
    messages: &mut Vec<ChatMessage>,
    resolver: &mut super::coverage::CheckpointGraphResolver,
) -> Result<()> {
    project_checkpoint_in_context(store, &context, head, messages, resolver).await
}

async fn project_checkpoint_in_context(
    store: &CrudStore,
    context: &ProjectionContext<'_>,
    head: &str,
    messages: &mut Vec<ChatMessage>,
    resolver: &mut super::coverage::CheckpointGraphResolver,
) -> Result<()> {
    project_checkpoint_in_context_with_boundary(store, context, head, messages, None, resolver)
        .await
        .map(|_| ())
}

pub(super) async fn project_checkpoint_in_context_with_boundary(
    store: &CrudStore,
    context: &ProjectionContext<'_>,
    head: &str,
    messages: &mut Vec<ChatMessage>,
    boundary: Option<&ProjectionBoundaryEvidence<'_>>,
    resolver: &mut super::coverage::CheckpointGraphResolver,
) -> Result<BTreeSet<usize>> {
    // A request may already contain this summary alongside covered originals
    // (for example after joining frozen branches). Normalize exact coverage in
    // that case too: the presence of a head reference does not prove that its
    // body is authoritative or that covered source messages were removed.
    let expanded = expand(store, head, context, resolver).await?;
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct HistoricalIdentity {
        thread: String,
        scope: String,
        id: String,
    }
    let identity =
        |source: &pioneer_agent::compaction::composition::ScopedHistorySource| HistoricalIdentity {
            thread: source.thread.clone(),
            scope: source.source.scope.clone(),
            id: source.source.id.clone(),
        };
    let covered = expanded
        .leaves
        .iter()
        .map(identity)
        .collect::<BTreeSet<_>>();
    // Input-copy replay participates in boundary admission, but it cannot
    // remove a raw input until *all* selected checkpoint claims have been
    // compared by the shared history normalizer. Tool replay stays atomic.
    let removable_replay_aliases = expanded
        .replay_aliases
        .keys()
        .filter(|replay| !expanded.input_replay_aliases.contains(*replay))
        .cloned()
        .collect::<BTreeSet<_>>();
    let emergency_inputs = expanded
        .emergency_inputs
        .iter()
        .map(identity)
        .collect::<BTreeSet<_>>();
    let admitted_messages = boundary.map_or(messages.as_slice(), |boundary| boundary.messages);
    let (represented, admitted_leaves) =
        checkpoint_message_leaves(store, context, &expanded, admitted_messages, resolver).await?;
    let current_leaves = if boundary.is_some() {
        checkpoint_message_leaves(store, context, &expanded, messages, resolver)
            .await?
            .1
    } else {
        admitted_leaves.clone()
    };
    let mut represented_coverage = represented.clone();
    for (replay, source) in &expanded.replay_aliases {
        if represented.contains(replay) {
            represented_coverage.insert(source.clone());
        }
    }
    if expanded
        .leaves
        .difference(&represented_coverage)
        .next()
        .is_some()
    {
        ensure!(
            context.allow_historical_gaps,
            ProjectionBoundary("checkpoint exceeds the selected history boundary")
        );
    }
    let select = |candidate_messages: &[ChatMessage],
                  leaves_by_message: &BTreeMap<
        usize,
        (
            BTreeSet<pioneer_agent::compaction::composition::ScopedHistorySource>,
            BTreeSet<SourceRef>,
        ),
    >|
     -> Result<(BTreeSet<usize>, bool)> {
        let mut selected = BTreeSet::new();
        let mut covered_by_other_checkpoint = false;
        for (index, (leaves, checkpoints)) in leaves_by_message {
            // A strictly larger durable checkpoint is already authoritative.
            // Equality does not suppress this candidate: its own stale copies
            // still need replacement by the current durable body.
            if !checkpoints.contains(&expanded.root)
                && !checkpoints.is_empty()
                && expanded.leaves.is_subset(leaves)
                && expanded.leaves != *leaves
            {
                covered_by_other_checkpoint = true;
                continue;
            }
            let covered_leaf =
                |leaf: &pioneer_agent::compaction::composition::ScopedHistorySource| {
                    covered.contains(&identity(leaf)) || removable_replay_aliases.contains(leaf)
                };
            if !leaves.iter().any(covered_leaf) || !leaves.iter().all(covered_leaf) {
                continue;
            }
            let origin = candidate_messages[*index]
                .provenance
                .as_ref()
                .expect("origin selected above");
            ensure!(
                origin.complete
                    && (!origin.protected_input
                        || leaves
                            .iter()
                            .map(identity)
                            .all(|leaf| emergency_inputs.contains(&leaf)))
                    && candidate_messages[*index].role != pioneer_provider::Role::System,
                "checkpoint cannot replace pending or protected input"
            );
            selected.insert(*index);
        }
        if !selected.is_empty() {
            let layout = pioneer_agent::compaction::history::NativeHistoryLayout::from_messages(
                context.workspace,
                context.context_thread,
                candidate_messages,
                &vec![0; candidate_messages.len()],
            )?;
            for (unit, indexes) in layout.units.iter().zip(&layout.message_indexes) {
                if indexes.iter().any(|index| selected.contains(index)) {
                    ensure!(
                        unit.complete && indexes.iter().all(|index| selected.contains(index)),
                        ProjectionBoundary("checkpoint splits a pending or whole canonical round")
                    );
                }
            }
        }
        Ok((selected, covered_by_other_checkpoint))
    };
    let (selected_boundary, _) = select(admitted_messages, &admitted_leaves)?;
    // The immutable boundary proves exact historical coverage. Model removal
    // uses indexes in today's list, which may contain earlier summaries.
    let (selected, covered_by_other_checkpoint) = select(messages, &current_leaves)?;
    let mut summary =
        checkpoint_message_from_expanded(store, context, head, &expanded, resolver).await?;
    let summary_origin = summary.provenance.as_ref().expect("checkpoint origin");
    let mut aliases = summary_origin.source_aliases.clone();
    let mut ambiguous = summary_origin.ambiguous_input_aliases.clone();
    for index in &selected {
        if let Some(origin) = &messages[*index].provenance {
            let mut origin = origin.clone();
            for source in origin.sources.clone() {
                if source.scope.starts_with("checkpoint:") {
                    let source_ref = SourceRef {
                        scope: source.scope,
                        id: source.id,
                        version: source.version,
                    };
                    let graph = resolver
                        .resolve(store, context.workspace, Some(context.allowed), &source_ref)
                        .await?
                        .ok_or_else(|| {
                            anyhow::anyhow!("selected checkpoint graph is unavailable")
                        })?;
                    merge_graph_input_evidence(&mut origin, &graph);
                }
            }
            aliases.extend(origin.source_aliases);
            ambiguous.extend(origin.ambiguous_input_aliases);
        }
    }
    let evidence = transferred_input_evidence(&expanded.leaves, &aliases, &ambiguous);
    let origin = summary.provenance.as_mut().expect("checkpoint origin");
    origin.source_aliases = evidence.aliases;
    origin.ambiguous_input_aliases = evidence.ambiguous;
    // main's larger-summary suppression must retain the absorbed input proof,
    // just as replacing a smaller selected checkpoint does.
    if covered_by_other_checkpoint {
        for (index, (leaves, checkpoints)) in &current_leaves {
            if !checkpoints.contains(&expanded.root)
                && !checkpoints.is_empty()
                && expanded.leaves.is_subset(leaves)
                && expanded.leaves != *leaves
            {
                let origin = messages[*index]
                    .provenance
                    .as_mut()
                    .expect("checkpoint origin");
                let retained = transferred_input_evidence(
                    leaves,
                    origin
                        .source_aliases
                        .iter()
                        .chain(summary.provenance.as_ref().unwrap().source_aliases.iter()),
                    origin.ambiguous_input_aliases.iter().chain(
                        summary
                            .provenance
                            .as_ref()
                            .unwrap()
                            .ambiguous_input_aliases
                            .iter(),
                    ),
                );
                origin.source_aliases = retained.aliases;
                origin.ambiguous_input_aliases = retained.ambiguous;
            }
        }
    }
    let first = selected.first().copied().unwrap_or(0);
    let insert_summary = !covered_by_other_checkpoint;
    let mut projected =
        Vec::with_capacity(messages.len() + usize::from(insert_summary) - selected.len());
    if messages.is_empty() && insert_summary {
        projected.push(summary.clone());
    }
    for (index, message) in messages.iter().enumerate() {
        if index == first && insert_summary {
            projected.push(summary.clone());
        }
        if !selected.contains(&index) {
            projected.push(message.clone());
        }
    }
    *messages = projected;
    Ok(selected_boundary)
}

async fn checkpoint_message_leaves(
    store: &CrudStore,
    context: &ProjectionContext<'_>,
    expanded: &Expanded,
    messages: &[ChatMessage],
    resolver: &mut super::coverage::CheckpointGraphResolver,
) -> Result<(
    BTreeSet<pioneer_agent::compaction::composition::ScopedHistorySource>,
    BTreeMap<
        usize,
        (
            BTreeSet<pioneer_agent::compaction::composition::ScopedHistorySource>,
            BTreeSet<SourceRef>,
        ),
    >,
)> {
    let mut represented = BTreeSet::new();
    let mut leaves_by_message = BTreeMap::new();
    let mut cached = BTreeMap::<SourceRef, Expanded>::new();
    for (index, message) in messages.iter().enumerate() {
        let Some(origin) = &message.provenance else {
            continue;
        };
        let context_owner = origin
            .context_thread
            .as_deref()
            .unwrap_or(&origin.thread_id);
        let replaceable = match expanded.coverage_domain {
            pioneer_compaction::CoverageDomain::OwnContribution => {
                !origin.inherited && context_owner == context.context_thread
            }
            pioneer_compaction::CoverageDomain::WorkingContext => {
                context.allowed.contains(&origin.thread_id)
                    && (origin.inherited || context_owner == context.context_thread)
            }
        };
        if !replaceable {
            continue;
        }
        let mut leaves = BTreeSet::new();
        let mut checkpoints = BTreeSet::new();
        for source in &origin.sources {
            if let Some(source_owner) = source.scope.strip_prefix("checkpoint:") {
                let source_ref = SourceRef {
                    scope: source.scope.clone(),
                    id: source.id.clone(),
                    version: source.version.clone(),
                };
                checkpoints.insert(source_ref.clone());
                if !cached.contains_key(&source_ref) {
                    let source_checkpoint = expand(
                        store,
                        &source.id,
                        &ProjectionContext {
                            workspace: context.workspace,
                            context_thread: context.context_thread,
                            source_thread: &origin.thread_id,
                            owner: source_owner,
                            allowed: context.allowed,
                            allow_historical_gaps: false,
                        },
                        resolver,
                    )
                    .await?;
                    ensure!(
                        source_checkpoint.root == source_ref,
                        "checkpoint source revision changed"
                    );
                    cached.insert(source_ref.clone(), source_checkpoint);
                }
                leaves.extend(cached[&source_ref].leaves.iter().cloned());
            } else {
                leaves.insert(
                    pioneer_agent::compaction::composition::ScopedHistorySource {
                        thread: origin.thread_id.clone(),
                        source: SourceRef {
                            scope: source.scope.clone(),
                            id: source.id.clone(),
                            version: source.version.clone(),
                        },
                    },
                );
            }
        }
        represented.extend(leaves.iter().cloned());
        leaves_by_message.insert(index, (leaves, checkpoints));
    }
    Ok((represented, leaves_by_message))
}
