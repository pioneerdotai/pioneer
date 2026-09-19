//! Materialize a published checkpoint from exact request-source coverage. This
//! never guesses that a count, timestamp, or equal text represents a source.
use super::*;
use pioneer_provider::{ChatMessage, MessageProvenance, MessageSourceRef};
use std::collections::{BTreeMap, BTreeSet};

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
        ProjectionContext::local(workspace, thread, owner, allowed),
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
    // without an OWN import. This is not an application grant: scope, current
    // DAG leaves and exact whole-message coverage are checked below.
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
            let edges = resolver.ancestry_edges(store, workspace, &id).await?;
            ensure!(
                edges.owner == owner,
                "checkpoint belongs to another context"
            );
            if let Some(root) = store
                .compaction_checkpoint_source(workspace, &source_thread, &id)
                .await?
                && let Some(graph) = resolver.resolve(store, workspace, None, &root).await?
                && graph.scopes.is_subset(allowed)
            {
                match project_checkpoint_in_context(
                    store,
                    &ProjectionContext {
                        workspace,
                        context_thread,
                        source_thread: &source_thread,
                        owner: &owner,
                        allowed,
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
    leaves: BTreeSet<SourceRef>,
    emergency_inputs: BTreeSet<SourceRef>,
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
        .ok_or_else(|| anyhow::anyhow!("checkpoint is not a current scoped source"))?;
    ensure!(
        root.scope == format!("checkpoint:{}", context.owner),
        "checkpoint owner mismatch"
    );
    // Discover and validate the complete DAG using metadata before reading any
    // foreign summary. A narrower Task/fork projection tries an older head.
    let graph = resolver
        .resolve(store, context.workspace, None, &root)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint coverage source changed or disappeared"))?;
    ensure!(
        graph.scopes.is_subset(context.allowed),
        ProjectionBoundary("checkpoint crosses the selected source scope")
    );
    // Recheck every exact source against the allowed context. Cached graph
    // metadata is not an authorization grant and current status may have
    // changed since metadata discovery.
    let graph = resolver
        .resolve(store, context.workspace, Some(context.allowed), &root)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint coverage source changed or disappeared"))?;
    let leaves: BTreeSet<SourceRef> = graph
        .leaves
        .iter()
        .map(|leaf| leaf.source.clone())
        .collect();
    let prepared = resolver
        .projection_metadata(store, context.workspace, &graph)
        .await?;
    ensure!(!leaves.is_empty(), "checkpoint has no canonical coverage");
    Ok(Expanded {
        root,
        leaves,
        emergency_inputs: prepared.emergency_inputs,
        coverage_domain: prepared.coverage_domain,
    })
}

/// Request origins have already been resolved and scope/version checked. A
/// checkpoint that crosses a fork/snapshot boundary cannot be inserted here.
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
    let prefix = format!("checkpoint:{}", context.owner);
    // A request may already contain this summary alongside covered originals
    // (for example after joining frozen branches). Normalize exact coverage in
    // that case too: the presence of a head reference does not prove that its
    // body is authoritative or that covered source messages were removed.
    let expanded = expand(store, head, context, resolver).await?;
    let mut represented = BTreeSet::new();
    let mut leaves_by_message = BTreeMap::new();
    let mut cached = BTreeMap::<SourceRef, BTreeSet<SourceRef>>::new();
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
        for source in &origin.sources {
            if let Some(source_owner) = source.scope.strip_prefix("checkpoint:") {
                let source_ref = SourceRef {
                    scope: source.scope.clone(),
                    id: source.id.clone(),
                    version: source.version.clone(),
                };
                if !cached.contains_key(&source_ref) {
                    cached.insert(
                        source_ref.clone(),
                        expand(
                            store,
                            &source.id,
                            &ProjectionContext {
                                workspace: context.workspace,
                                context_thread: context.context_thread,
                                source_thread: &origin.thread_id,
                                owner: source_owner,
                                allowed: context.allowed,
                            },
                            resolver,
                        )
                        .await?
                        .leaves,
                    );
                }
                leaves.extend(cached[&source_ref].iter().cloned());
            } else {
                leaves.insert(SourceRef {
                    scope: source.scope.clone(),
                    id: source.id.clone(),
                    version: source.version.clone(),
                });
            }
        }
        represented.extend(leaves.iter().cloned());
        leaves_by_message.insert(index, leaves);
    }
    ensure!(
        expanded.leaves.is_subset(&represented),
        ProjectionBoundary("checkpoint exceeds the selected history boundary")
    );
    let mut selected = BTreeSet::new();
    for (index, leaves) in leaves_by_message {
        if leaves.is_disjoint(&expanded.leaves) {
            continue;
        }
        ensure!(
            leaves.is_subset(&expanded.leaves),
            ProjectionBoundary("checkpoint splits a projected history message")
        );
        let origin = messages[index]
            .provenance
            .as_ref()
            .expect("origin selected above");
        ensure!(
            origin.complete
                && (!origin.protected_input || leaves.is_subset(&expanded.emergency_inputs))
                && messages[index].role != pioneer_provider::Role::System,
            "checkpoint cannot replace pending or protected input"
        );
        selected.insert(index);
    }
    ensure!(
        !selected.is_empty(),
        "checkpoint has no replaceable source messages"
    );
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
    let checkpoint = resolver
        .projection_body(store, context.workspace, &expanded.root)
        .await?;
    // Body loading may yield while a canonical dependency changes. Complete
    // the final asynchronous validation sequence before applying: revalidate
    // the exact graph and bind the current root to the identity used for both
    // graph metadata and the selected body.
    let graph = resolver
        .resolve(
            store,
            context.workspace,
            Some(context.allowed),
            &expanded.root,
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint was invalidated during projection"))?;
    ensure!(
        graph.checkpoints.contains(&expanded.root),
        "checkpoint root disappeared from its prepared graph"
    );
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
        unit_id: prefix,
        sources: vec![MessageSourceRef {
            scope: source.scope,
            id: source.id,
            version: source.version,
        }],
        complete: true,
        protected_input: false,
        inherited: expanded.coverage_domain == pioneer_compaction::CoverageDomain::WorkingContext,
    });
    let first = *selected.first().expect("nonempty selection");
    let mut projected = Vec::with_capacity(messages.len() + 1 - selected.len());
    for (index, message) in messages.iter().enumerate() {
        if index == first {
            projected.push(summary.clone());
        }
        if !selected.contains(&index) {
            projected.push(message.clone());
        }
    }
    *messages = projected;
    Ok(())
}
