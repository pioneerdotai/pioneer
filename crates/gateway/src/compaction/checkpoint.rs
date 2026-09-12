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

/// Select a whole compatible checkpoint from the captured head's ancestry.
/// A later summary must never import work past a TaskRun/fork boundary. Failure
/// to find an exact projection leaves the original selected history intact;
/// storage errors and malformed coverage remain errors, not a silent fallback.
pub(crate) async fn project_compatible_checkpoint(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    owner: &str,
    head: &str,
    allowed: &BTreeSet<String>,
    messages: &mut Vec<ChatMessage>,
) -> Result<Option<String>> {
    let mut candidate = Some(head.to_owned());
    let mut seen = BTreeSet::new();
    while let Some(id) = candidate {
        ensure!(seen.insert(id.clone()), "cyclic checkpoint ancestry");
        let checkpoint = store
            .compaction_checkpoint(&id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("checkpoint ancestry is missing"))?;
        ensure!(
            checkpoint.owner == owner,
            "checkpoint belongs to another context"
        );
        if store
            .compaction_checkpoint_source(workspace, thread, &id)
            .await?
            .is_some()
        {
            match project_checkpoint(store, workspace, thread, owner, &id, allowed, messages).await
            {
                Ok(()) => return Ok(Some(id)),
                Err(error) if error.downcast_ref::<ProjectionBoundary>().is_some() => {}
                Err(error) => return Err(error),
            }
        }
        candidate = checkpoint.previous;
    }
    Ok(None)
}

struct Expanded {
    checkpoint: Checkpoint,
    leaves: BTreeSet<SourceRef>,
    emergency_inputs: BTreeSet<SourceRef>,
}
async fn expand(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    owner: &str,
    head: &str,
    allowed: &BTreeSet<String>,
) -> Result<Expanded> {
    let root = store
        .compaction_checkpoint_source(workspace, thread, head)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint is not a current scoped source"))?;
    ensure!(
        root.scope == format!("checkpoint:{owner}"),
        "checkpoint owner mismatch"
    );
    // Discover and validate the complete DAG using metadata before reading any
    // foreign summary. A narrower Task/fork projection tries an older head.
    let scopes = super::coverage::checkpoint_scopes(store, workspace, &root).await?;
    ensure!(
        scopes.is_subset(allowed),
        ProjectionBoundary("checkpoint crosses the selected source scope")
    );
    super::coverage::checkpoint_leaves(store, workspace, allowed, &root).await?;
    let checkpoint = store
        .compaction_checkpoint(head)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint source is missing"))?;
    let mut leaves = BTreeSet::new();
    let mut emergency_inputs = BTreeSet::new();
    let mut done = BTreeSet::new();
    let mut visiting = BTreeSet::new();
    let mut pending = vec![(head.to_owned(), false)];
    while let Some((id, exiting)) = pending.pop() {
        if exiting {
            visiting.remove(&id);
            done.insert(id);
            continue;
        }
        if done.contains(&id) {
            continue;
        }
        ensure!(visiting.insert(id.clone()), "cyclic checkpoint coverage");
        let node = store
            .compaction_checkpoint(&id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("checkpoint coverage link is missing"))?;
        ensure!(
            node.format_version == pioneer_compaction::FORMAT_VERSION,
            "checkpoint format mismatch"
        );
        let operation = store
            .compaction_operation(&node.operation_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("checkpoint operation is missing"))?;
        let snapshot: OperationSnapshot = serde_json::from_str(&operation.snapshot)?;
        let emergency = snapshot.plan.mode == pioneer_compaction::CompactionMode::Emergency;
        pending.push((id, true));
        let mut parents = BTreeSet::new();
        if let Some(previous) = node.previous {
            parents.insert(previous);
        }
        for source in node.coverage {
            if source.scope.starts_with("checkpoint:") {
                parents.insert(source.id);
            } else {
                if emergency && source.scope.starts_with("input:") {
                    emergency_inputs.insert(source.clone());
                }
                leaves.insert(source);
            }
        }
        pending.extend(parents.into_iter().map(|id| (id, false)));
    }
    ensure!(!leaves.is_empty(), "checkpoint has no canonical coverage");
    Ok(Expanded {
        checkpoint,
        leaves,
        emergency_inputs,
    })
}

/// Request origins have already been resolved and scope/version checked. A
/// checkpoint that crosses a fork/snapshot boundary cannot be inserted here.
pub(crate) async fn project_checkpoint(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    owner: &str,
    head: &str,
    allowed: &BTreeSet<String>,
    messages: &mut Vec<ChatMessage>,
) -> Result<()> {
    let prefix = format!("checkpoint:{owner}");
    // A request may already contain this summary alongside covered originals
    // (for example after joining frozen branches). Normalize exact coverage in
    // that case too: the presence of a head reference does not prove that its
    // body is authoritative or that covered source messages were removed.
    let expanded = expand(store, workspace, thread, owner, head, allowed).await?;
    let mut represented = BTreeSet::new();
    let mut leaves_by_message = BTreeMap::new();
    let mut cached = BTreeMap::<String, BTreeSet<SourceRef>>::new();
    for (index, message) in messages.iter().enumerate() {
        let Some(origin) = &message.provenance else {
            continue;
        };
        if origin
            .context_thread
            .as_deref()
            .unwrap_or(&origin.thread_id)
            != thread
            || origin.inherited
        {
            continue;
        }
        let mut leaves = BTreeSet::new();
        for source in &origin.sources {
            if let Some(source_owner) = source.scope.strip_prefix("checkpoint:") {
                if !cached.contains_key(&source.id) {
                    cached.insert(
                        source.id.clone(),
                        expand(
                            store,
                            workspace,
                            &origin.thread_id,
                            source_owner,
                            &source.id,
                            allowed,
                        )
                        .await?
                        .leaves,
                    );
                }
                leaves.extend(cached[&source.id].iter().cloned());
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
        workspace,
        thread,
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
    let source = store
        .compaction_checkpoint_source(workspace, thread, head)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint was invalidated during projection"))?;
    let mut summary = ChatMessage::user(format!(
        "Summary of completed work (historical data):\n{}",
        expanded.checkpoint.summary
    ));
    summary.provenance = Some(MessageProvenance {
        logical_turn_id: None,
        workspace_id: workspace.into(),
        thread_id: thread.into(),
        context_thread: None,
        unit_id: prefix,
        sources: vec![MessageSourceRef {
            scope: source.scope,
            id: source.id,
            version: source.version,
        }],
        complete: true,
        protected_input: false,
        inherited: false,
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
