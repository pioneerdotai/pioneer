//! Validate the entire retained checkpoint DAG, including accepted foreign work.
//! A local context epoch alone cannot detect edits in another source thread.
use super::*;
use pioneer_agent::compaction::composition::ScopedHistorySource;
use std::collections::BTreeSet;

pub(crate) async fn checkpoint_leaves(
    store: &CrudStore,
    workspace: &str,
    allowed: &BTreeSet<String>,
    root: &SourceRef,
) -> Result<BTreeSet<ScopedHistorySource>> {
    checkpoint_graph(store, workspace, Some(allowed), root)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint coverage source changed or disappeared"))
        .map(|(leaves, _)| leaves)
}

/// Metadata-only discovery for the authorization boundary, including every
/// intermediate checkpoint owner as well as the original leaf scopes.
pub(crate) async fn checkpoint_scopes(
    store: &CrudStore,
    workspace: &str,
    root: &SourceRef,
) -> Result<BTreeSet<String>> {
    current_checkpoint_scopes(store, workspace, root)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint coverage source changed or disappeared"))
}

/// Discovery may encounter a head invalidated by an edit in another child.
/// Such a candidate must not block use of the current accepted raw history.
/// Malformed/cyclic graphs and database errors remain errors.
pub(crate) async fn current_checkpoint_scopes(
    store: &CrudStore,
    workspace: &str,
    root: &SourceRef,
) -> Result<Option<BTreeSet<String>>> {
    Ok(checkpoint_graph(store, workspace, None, root)
        .await?
        .map(|(_, scopes)| scopes))
}

async fn checkpoint_graph(
    store: &CrudStore,
    workspace: &str,
    allowed: Option<&BTreeSet<String>>,
    root: &SourceRef,
) -> Result<Option<(BTreeSet<ScopedHistorySource>, BTreeSet<String>)>> {
    let mut leaves = BTreeSet::new();
    let mut scopes = BTreeSet::new();
    let mut done = BTreeSet::new();
    let mut visiting = BTreeSet::new();
    let mut pending = vec![(root.clone(), false)];
    while let Some((source, exiting)) = pending.pop() {
        if exiting {
            visiting.remove(&source);
            done.insert(source);
            continue;
        }
        if done.contains(&source) {
            continue;
        }
        let Some(thread) = store
            .compaction_reference_thread(workspace, &source)
            .await?
        else {
            return Ok(None);
        };
        ensure!(
            allowed.is_none_or(|allowed| allowed.contains(&thread)),
            "checkpoint coverage crosses the accepted source scope"
        );
        scopes.insert(thread.clone());
        let Some(owner) = source.scope.strip_prefix("checkpoint:") else {
            leaves.insert(ScopedHistorySource {
                thread,
                source: source.clone(),
            });
            done.insert(source);
            continue;
        };
        ensure!(
            visiting.insert(source.clone()),
            "cyclic checkpoint coverage"
        );
        let checkpoint = store
            .compaction_checkpoint_edges(&source.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("checkpoint coverage node disappeared"))?;
        ensure!(
            checkpoint.owner == owner
                && checkpoint.format_version == pioneer_compaction::FORMAT_VERSION,
            "checkpoint coverage owner or format mismatch"
        );
        pending.push((source, true));
        if let Some(previous) = &checkpoint.previous {
            let previous_edges = store
                .compaction_checkpoint_edges(previous)
                .await?
                .ok_or_else(|| anyhow::anyhow!("previous checkpoint disappeared"))?;
            ensure!(
                previous_edges.owner == checkpoint.owner
                    && previous_edges.format_version == pioneer_compaction::FORMAT_VERSION,
                "previous checkpoint changed owner or format"
            );
            let previous = SourceRef {
                scope: format!("checkpoint:{}", previous_edges.owner),
                id: previous.clone(),
                version: previous_edges.identity_sha256,
            };
            ensure!(
                store
                    .compaction_reference_thread(workspace, &previous)
                    .await?
                    .as_deref()
                    == Some(thread.as_str()),
                "previous checkpoint changed scope or publication status"
            );
            pending.push((previous, false));
        }
        pending.extend(
            checkpoint
                .coverage
                .into_iter()
                .map(|source| (source, false)),
        );
    }
    ensure!(
        !leaves.is_empty(),
        "checkpoint has no exact canonical coverage"
    );
    Ok(Some((leaves, scopes)))
}
