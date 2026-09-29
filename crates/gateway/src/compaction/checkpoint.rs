//! Materialize a published checkpoint from exact request-source coverage. This
//! never guesses that a count, timestamp, or equal text represents a source.
use super::*;
use pioneer_agent::compaction::composition::{
    ExactInputClaims, ScopedHistorySource, summary_comparison_leaves, summary_copy_preference,
    summary_covers,
};
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
                Err(error)
                    if id != head
                        && error
                            .downcast_ref::<super::coverage::EmptyCheckpointCoverage>()
                            .is_some() => {}
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
    // Freeze competing claims before the first replacement, including when
    // the accepted history still contains only raw inputs. A later owner's
    // checkpoint may otherwise reveal a conflict after a summary is gone.
    let mut input_claims =
        projection_input_claims(store, workspace, allowed, admitted_messages, resolver).await?;
    let mut candidates = Vec::new();
    for source_thread in threads {
        ensure!(
            allowed.contains(&source_thread),
            "checkpoint source scope is not accepted"
        );
        let owner = super::native::native_owner(workspace, &source_thread);
        let head = store.compaction_head(&owner).await?;
        if let Some(head) = &head {
            if let Some(source) = store
                .compaction_checkpoint_source(workspace, &source_thread, head)
                .await?
            {
                let graph = resolver
                    .resolve(store, workspace, Some(allowed), &source)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("checkpoint input evidence is unavailable"))?;
                add_graph_input_claims(&mut input_claims, &graph);
            }
        }
        candidates.push((source_thread, owner, head));
    }
    input_claims.mark_competing_owners();
    for (source_thread, owner, mut candidate) in candidates {
        let head = candidate.clone();
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
                match project_checkpoint_with_input_claims(
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
                    Some(&input_claims),
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
                    Err(error)
                        if head.as_deref() != Some(id.as_str())
                            && error
                                .downcast_ref::<super::coverage::EmptyCheckpointCoverage>()
                                .is_some() => {}
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
    project_checkpoint_with_input_claims(store, context, head, messages, boundary, None, resolver)
        .await
}

pub(super) fn add_graph_input_claims(
    claims: &mut ExactInputClaims,
    graph: &super::coverage::ResolvedCheckpointGraph,
) {
    let mut aliases = Vec::new();
    let mut conflicts = Vec::new();
    append_graph_input_evidence(graph, &mut aliases, &mut conflicts);
    for alias in &aliases {
        claims.add_alias(alias);
    }
    claims.ambiguous.extend(
        conflicts
            .into_iter()
            .map(|conflict| (conflict.thread_id, conflict.source)),
    );
}

// Read only evidence attached to the accepted view or validated published
// graphs. Equal payloads and current database rows cannot grant coverage.
async fn projection_input_claims(
    store: &CrudStore,
    workspace: &str,
    allowed: &BTreeSet<String>,
    messages: &[ChatMessage],
    resolver: &mut super::coverage::CheckpointGraphResolver,
) -> Result<ExactInputClaims> {
    let mut claims = ExactInputClaims::default();
    for message in messages {
        let Some(origin) = &message.provenance else {
            continue;
        };
        claims.ambiguous.extend(
            origin
                .ambiguous_input_aliases
                .iter()
                .map(|conflict| (conflict.thread_id.clone(), conflict.source.clone())),
        );
        if origin.source_aliases.is_empty()
            && !origin
                .sources
                .iter()
                .any(|source| source.scope.starts_with("checkpoint:"))
        {
            continue;
        }
        let mut leaves = BTreeSet::new();
        for source in &origin.sources {
            let source = SourceRef {
                scope: source.scope.clone(),
                id: source.id.clone(),
                version: source.version.clone(),
            };
            if source.scope.starts_with("checkpoint:") {
                let graph = match resolver
                    .resolve(store, workspace, Some(allowed), &source)
                    .await?
                {
                    Some(graph) => graph,
                    None => {
                        if let Some(saved) = store
                            .compaction_checkpoint_source(workspace, &origin.thread_id, &source.id)
                            .await?
                        {
                            ensure!(saved == source, "checkpoint source revision changed");
                        }
                        anyhow::bail!("accepted checkpoint input evidence is unavailable");
                    }
                };
                leaves.extend(graph.leaves.iter().cloned());
                add_graph_input_claims(&mut claims, &graph);
            } else {
                leaves.insert(ScopedHistorySource {
                    thread: origin.thread_id.clone(),
                    source,
                });
            }
        }
        for alias in &origin.source_aliases {
            let represented = scoped_input(&alias.represented_thread_id, &alias.represented_source);
            ensure!(
                alias.source.scope.starts_with("input:")
                    && represented.source.scope.starts_with("input:")
                    && leaves.contains(&represented),
                "input alias is outside its carrier coverage"
            );
            claims.add_alias(alias);
        }
    }
    claims.mark_competing_owners();
    Ok(claims)
}

fn scoped_input(thread: &str, source: &MessageSourceRef) -> ScopedHistorySource {
    ScopedHistorySource {
        thread: thread.to_owned(),
        source: SourceRef {
            scope: source.scope.clone(),
            id: source.id.clone(),
            version: source.version.clone(),
        },
    }
}

// A published summary may cover B while the accepted raw message represents
// A and carries the exact alias B -> A. Rebase only this proven, unambiguous
// single input; do not turn aliases into additional canonical leaves.
fn input_representative_replacements(
    context: &ProjectionContext<'_>,
    expanded: &Expanded,
    messages: &[ChatMessage],
    claims: &ExactInputClaims,
) -> BTreeMap<ScopedHistorySource, MessageSourceAlias> {
    let mut replacements = BTreeMap::new();
    for message in messages {
        let Some(origin) = &message.provenance else {
            continue;
        };
        if message.role != pioneer_provider::Role::User
            || !origin.complete
            || origin.protected_input
            || origin.workspace_id != context.workspace
            || !context.allowed.contains(&origin.thread_id)
            || origin.sources.len() != 1
            || !origin.sources[0].scope.starts_with("input:")
        {
            continue;
        }
        let representative = (origin.thread_id.clone(), origin.sources[0].clone());
        let represented = scoped_input(&representative.0, &representative.1);
        if expanded.leaves.contains(&represented) || claims.ambiguous.contains(&representative) {
            continue;
        }
        // Every proof that would lose its raw carrier must remain unambiguous.
        if origin.source_aliases.iter().any(|alias| {
            let copy = (alias.thread_id.clone(), alias.source.clone());
            alias.represented_thread_id != representative.0
                || alias.represented_source != representative.1
                || claims.ambiguous.contains(&copy)
                || !claims
                    .owners
                    .get(&copy)
                    .is_some_and(|owners| owners.len() == 1 && owners.contains(&representative))
        }) {
            continue;
        }
        let covered = origin
            .source_aliases
            .iter()
            .filter(|alias| {
                context.allowed.contains(&alias.thread_id)
                    && expanded
                        .leaves
                        .contains(&scoped_input(&alias.thread_id, &alias.source))
            })
            .collect::<Vec<_>>();
        let [alias] = covered.as_slice() else {
            continue;
        };
        let target = (alias.thread_id.clone(), alias.source.clone());
        if claims
            .owners
            .get(&representative)
            .is_some_and(|owners| owners.len() != 1 || !owners.contains(&target))
        {
            continue;
        }
        replacements.insert(
            represented,
            MessageSourceAlias {
                represented_thread_id: target.0,
                represented_source: target.1,
                thread_id: representative.0,
                source: representative.1,
            },
        );
    }
    replacements
}

async fn project_checkpoint_with_input_claims(
    store: &CrudStore,
    context: &ProjectionContext<'_>,
    head: &str,
    messages: &mut Vec<ChatMessage>,
    boundary: Option<&ProjectionBoundaryEvidence<'_>>,
    input_claims: Option<&ExactInputClaims>,
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
    let needs_replacement = admitted_messages
        .iter()
        .chain(messages.iter())
        .any(|message| {
            message.provenance.as_ref().is_some_and(|origin| {
                origin.sources.len() == 1
                    && origin.sources[0].scope.starts_with("input:")
                    && !expanded
                        .leaves
                        .contains(&scoped_input(&origin.thread_id, &origin.sources[0]))
                    && origin.source_aliases.iter().any(|alias| {
                        expanded
                            .leaves
                            .contains(&scoped_input(&alias.thread_id, &alias.source))
                    })
            })
        });
    let needs_summary_comparison = admitted_messages
        .iter()
        .chain(messages.iter())
        .any(|message| {
            message.provenance.as_ref().is_some_and(|origin| {
                origin
                    .sources
                    .iter()
                    .any(|source| source.scope.starts_with("checkpoint:"))
            })
        });
    let needs_input_evidence = needs_replacement || needs_summary_comparison;
    let mut claims = if needs_input_evidence {
        projection_input_claims(
            store,
            context.workspace,
            context.allowed,
            admitted_messages,
            resolver,
        )
        .await?
    } else {
        ExactInputClaims::default()
    };
    if needs_input_evidence && boundary.is_some() {
        claims.merge(
            projection_input_claims(
                store,
                context.workspace,
                context.allowed,
                messages,
                resolver,
            )
            .await?,
        );
    }
    if let Some(input_claims) = input_claims {
        claims.merge(input_claims.clone());
    } else if needs_input_evidence {
        // Local-head projection also runs before foreign checkpoint discovery.
        // It must see competing claims before replacing a raw input or a
        // summary through an input copy. Ordinary projections without either
        // comparison skip this metadata-only preflight.
        let owners = admitted_messages
            .iter()
            .filter_map(|message| {
                let origin = message.provenance.as_ref()?;
                let context_owner = origin
                    .context_thread
                    .as_deref()
                    .unwrap_or(&origin.thread_id);
                (origin.thread_id != context.context_thread
                    && (origin.inherited || context_owner == context.context_thread))
                    .then(|| origin.thread_id.clone())
            })
            .collect::<BTreeSet<_>>();
        for thread in owners {
            ensure!(
                context.allowed.contains(&thread),
                "checkpoint input source scope is not accepted"
            );
            let owner = super::native::native_owner(context.workspace, &thread);
            if let Some(head) = store.compaction_head(&owner).await?
                && let Some(source) = store
                    .compaction_checkpoint_source(context.workspace, &thread, &head)
                    .await?
            {
                let graph = resolver
                    .resolve(store, context.workspace, Some(context.allowed), &source)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("checkpoint input evidence is unavailable"))?;
                add_graph_input_claims(&mut claims, &graph);
            }
        }
    }
    for (copy, represented) in &expanded.replay_aliases {
        if expanded.input_replay_aliases.contains(copy) {
            claims.add_alias(&MessageSourceAlias {
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
            });
        }
    }
    claims
        .ambiguous
        .extend(expanded.ambiguous_input_aliases.iter().map(|source| {
            (
                source.thread.clone(),
                MessageSourceRef {
                    scope: source.source.scope.clone(),
                    id: source.source.id.clone(),
                    version: source.source.version.clone(),
                },
            )
        }));
    claims.mark_competing_owners();
    let (_, admitted_leaves) =
        checkpoint_message_leaves(store, context, &expanded, admitted_messages, resolver).await?;
    let current_leaves = if boundary.is_some() {
        checkpoint_message_leaves(store, context, &expanded, messages, resolver)
            .await?
            .1
    } else {
        admitted_leaves.clone()
    };
    let admitted_replacements =
        input_representative_replacements(context, &expanded, admitted_messages, &claims);
    let current_replacements =
        input_representative_replacements(context, &expanded, messages, &claims);
    // A checkpoint can include another checkpoint's exact leaves across
    // domains. Domain equality is required only for copy-alias comparison.
    let mut represented_coverage = admitted_leaves
        .values()
        .flat_map(|(leaves, _, _)| leaves.iter().cloned())
        .collect::<BTreeSet<_>>();
    for (source, alias) in &admitted_replacements {
        if represented_coverage.contains(source) {
            represented_coverage.insert(scoped_input(
                &alias.represented_thread_id,
                &alias.represented_source,
            ));
        }
    }
    for (replay, source) in &expanded.replay_aliases {
        if represented_coverage.contains(replay) {
            represented_coverage.insert(source.clone());
        }
    }
    for (leaves, checkpoints, domain) in admitted_leaves.values() {
        if checkpoints.is_empty() || *domain != Some(expanded.coverage_domain) {
            continue;
        }
        let comparable = summary_comparison_leaves(leaves, &claims);
        represented_coverage.extend(expanded.leaves.intersection(&comparable).cloned());
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
    // Exact checkpoint containment is independent of coverage domain.
    // A copy alias can establish dominance only within one domain.
    let existing_dominates =
        |leaves: &BTreeSet<ScopedHistorySource>,
         domain: Option<pioneer_compaction::CoverageDomain>| {
            (expanded.leaves.is_subset(leaves) && expanded.leaves != *leaves)
                || (domain == Some(expanded.coverage_domain)
                    && summary_covers(leaves, &expanded.leaves, &claims)
                    && (!summary_covers(&expanded.leaves, leaves, &claims)
                        || summary_copy_preference(leaves, &claims)
                            >= summary_copy_preference(&expanded.leaves, &claims)))
        };
    let candidate_dominates = |leaves: &BTreeSet<ScopedHistorySource>| {
        summary_covers(&expanded.leaves, leaves, &claims)
            && (!summary_covers(leaves, &expanded.leaves, &claims)
                || summary_copy_preference(&expanded.leaves, &claims)
                    > summary_copy_preference(leaves, &claims))
    };
    let select = |candidate_messages: &[ChatMessage],
                  replacements: &BTreeMap<ScopedHistorySource, MessageSourceAlias>,
                  leaves_by_message: &BTreeMap<
        usize,
        (
            BTreeSet<pioneer_agent::compaction::composition::ScopedHistorySource>,
            BTreeSet<SourceRef>,
            Option<pioneer_compaction::CoverageDomain>,
        ),
    >|
     -> Result<(BTreeSet<usize>, bool)> {
        let mut selected = BTreeSet::new();
        let mut covered_by_other_checkpoint = false;
        for (index, (leaves, checkpoints, domain)) in leaves_by_message {
            // An applicable durable checkpoint already represents this
            // candidate. Its own stale copies still need replacement by the
            // current durable body.
            if !checkpoints.contains(&expanded.root)
                && !checkpoints.is_empty()
                && existing_dominates(leaves, *domain)
            {
                covered_by_other_checkpoint = true;
                continue;
            }
            let covered_leaf =
                |leaf: &pioneer_agent::compaction::composition::ScopedHistorySource| {
                    covered.contains(&identity(leaf))
                        || removable_replay_aliases.contains(leaf)
                        || replacements.contains_key(leaf)
                };
            let covered_summary = !checkpoints.is_empty()
                && *domain == Some(expanded.coverage_domain)
                && candidate_dominates(leaves);
            if !covered_summary
                && (!leaves.iter().any(covered_leaf) || !leaves.iter().all(covered_leaf))
            {
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
    let (selected_boundary, _) =
        select(admitted_messages, &admitted_replacements, &admitted_leaves)?;
    // The immutable boundary proves exact historical coverage. Model removal
    // uses indexes in today's list, which may contain earlier summaries.
    let (selected, covered_by_other_checkpoint) =
        select(messages, &current_replacements, &current_leaves)?;
    let mut summary =
        checkpoint_message_from_expanded(store, context, head, &expanded, resolver).await?;
    let summary_origin = summary.provenance.as_ref().expect("checkpoint origin");
    let mut aliases = summary_origin.source_aliases.clone();
    let mut ambiguous = summary_origin.ambiguous_input_aliases.clone();
    let mut replaced_alias_sources = aliases
        .iter()
        .map(|alias| (alias.thread_id.clone(), alias.source.clone()))
        .collect::<BTreeSet<_>>();
    for index in &selected {
        if let Some(origin) = &messages[*index].provenance {
            let mut origin = origin.clone();
            if origin.sources.len() == 1
                && let Some(replacement) =
                    current_replacements.get(&scoped_input(&origin.thread_id, &origin.sources[0]))
            {
                // Anchor transferred proof in B, the actual published leaf.
                // A and its other exact copies remain aliases, not new coverage.
                origin.source_aliases = origin
                    .source_aliases
                    .into_iter()
                    .filter_map(|alias| {
                        if alias.thread_id == replacement.represented_thread_id
                            && alias.source == replacement.represented_source
                        {
                            None
                        } else {
                            Some(MessageSourceAlias {
                                represented_thread_id: replacement.represented_thread_id.clone(),
                                represented_source: replacement.represented_source.clone(),
                                thread_id: alias.thread_id,
                                source: alias.source,
                            })
                        }
                    })
                    .collect();
                origin.source_aliases.push(replacement.clone());
            }
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
            replaced_alias_sources.extend(
                origin
                    .source_aliases
                    .iter()
                    .map(|alias| (alias.thread_id.clone(), alias.source.clone())),
            );
            if !current_leaves[index].1.is_empty()
                && !current_leaves[index].0.is_subset(&expanded.leaves)
            {
                // This summary was selected through a copy claim. Its old
                // aliases are still available on its published checkpoint;
                // only claims rooted in the new leaf closure can be carried.
                origin.source_aliases.retain(|alias| {
                    expanded.leaves.contains(&scoped_input(
                        &alias.represented_thread_id,
                        &alias.represented_source,
                    ))
                });
            }
            aliases.extend(origin.source_aliases);
            ambiguous.extend(origin.ambiguous_input_aliases);
        }
    }
    ambiguous.extend(claims.conflicts().into_iter().filter(|marker| {
        replaced_alias_sources.contains(&(marker.thread_id.clone(), marker.source.clone()))
            || expanded
                .leaves
                .contains(&scoped_input(&marker.thread_id, &marker.source))
    }));
    let evidence = transferred_input_evidence(&expanded.leaves, &aliases, &ambiguous);
    let origin = summary.provenance.as_mut().expect("checkpoint origin");
    origin.source_aliases = evidence.aliases;
    origin.ambiguous_input_aliases = evidence.ambiguous;
    // main's larger-summary suppression must retain the absorbed input proof,
    // just as replacing a smaller selected checkpoint does.
    if covered_by_other_checkpoint {
        for (index, (leaves, checkpoints, domain)) in &current_leaves {
            if !checkpoints.contains(&expanded.root)
                && !checkpoints.is_empty()
                && *domain == Some(expanded.coverage_domain)
                && existing_dominates(leaves, *domain)
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
                        .chain(summary.provenance.as_ref().unwrap().source_aliases.iter())
                        .filter(|alias| {
                            leaves.contains(&scoped_input(
                                &alias.represented_thread_id,
                                &alias.represented_source,
                            ))
                        }),
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
            Option<pioneer_compaction::CoverageDomain>,
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
        let domain = if checkpoints.len() == 1 {
            Some(cached[checkpoints.first().expect("one checkpoint")].coverage_domain)
        } else {
            None
        };
        leaves_by_message.insert(index, (leaves, checkpoints, domain));
    }
    Ok((represented, leaves_by_message))
}
