//! Materialize a published checkpoint from exact request-source coverage. This
//! never guesses that a count, timestamp, or equal text represents a source.
use super::*;
use pioneer_provider::{ChatMessage, MessageProvenance, MessageSourceRef};
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
    // Discovery follows only source owners that are actually represented in
    // the accepted request. Inherited H may have been frozen before its owner
    // published a working-context checkpoint, so it is a candidate source even
    // without an OWN import. This is not an application grant: the published
    // root scope and exact whole-message boundary are checked below.
    let threads: BTreeSet<_> = messages
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
                match project_checkpoint_in_context(
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
                    resolver,
                )
                .await
                {
                    Ok(()) => break,
                    Err(error) if error.downcast_ref::<ProjectionBoundary>().is_some() => {}
                    Err(error) => return Err(error),
                }
            }
            candidate = edges.previous;
        }
    }
    Ok(())
}

struct Expanded {
    root: SourceRef,
    leaves: BTreeSet<pioneer_agent::compaction::composition::ScopedHistorySource>,
    replay_aliases: BTreeMap<
        pioneer_agent::compaction::composition::ScopedHistorySource,
        pioneer_agent::compaction::composition::ScopedHistorySource,
    >,
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
        complete: true,
        protected_input: false,
        inherited: expanded.coverage_domain == pioneer_compaction::CoverageDomain::WorkingContext,
    });
    Ok(summary)
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
        .chain(expanded.replay_aliases.keys())
        .map(identity)
        .collect::<BTreeSet<_>>();
    let emergency_inputs = expanded
        .emergency_inputs
        .iter()
        .map(identity)
        .collect::<BTreeSet<_>>();
    let mut represented = BTreeSet::new();
    let mut leaves_by_message = BTreeMap::new();
    let mut cached = BTreeMap::<
        SourceRef,
        BTreeSet<pioneer_agent::compaction::composition::ScopedHistorySource>,
    >::new();
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
                    cached.insert(source_ref.clone(), source_checkpoint.leaves);
                }
                leaves.extend(cached[&source_ref].iter().cloned());
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
    // Boundary admission above is exact, including historical versions. This
    // version-free key is used only after admission to remove today's copy of
    // an already covered identity; an edit must not turn it into a new tail.
    let mut selected = BTreeSet::new();
    let mut covered_by_other_checkpoint = false;
    for (index, (leaves, checkpoints)) in leaves_by_message {
        // This is the reciprocal of the replacement check below (and of the
        // bidirectional checkpoint containment rule in composition.rs). Use
        // exact saved leaves here: a source revision is part of historical
        // checkpoint coverage, even though admitted current raw rows are
        // removed by version-free identity below. The candidate's own message
        // is deliberately excluded: all of its copies must be selected and
        // replaced by the authoritative saved body.
        let represents_candidate = checkpoints.contains(&expanded.root);
        if !represents_candidate
            && !checkpoints.is_empty()
            && expanded.leaves.is_subset(&leaves)
            && expanded.leaves != leaves
        {
            covered_by_other_checkpoint = true;
            continue;
        }
        let identities = leaves.iter().map(identity).collect::<BTreeSet<_>>();
        if identities.is_disjoint(&covered) {
            continue;
        }
        // Distinct partially-overlapping summaries are atomic. Keep both;
        // replace an existing unit only when the new checkpoint contains it.
        if !identities.is_subset(&covered) {
            continue;
        }
        let origin = messages[index]
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
                && messages[index].role != pioneer_provider::Role::System,
            "checkpoint cannot replace pending or protected input"
        );
        selected.insert(index);
    }
    if !selected.is_empty() {
        let layout = pioneer_agent::compaction::history::NativeHistoryLayout::from_messages(
            context.workspace,
            context.context_thread,
            messages,
            &vec![0; messages.len()],
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
    let summary =
        checkpoint_message_from_expanded(store, context, head, &expanded, resolver).await?;
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
    Ok(())
}
