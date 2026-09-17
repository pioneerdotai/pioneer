//! Exact original projection used only when accepted checkpoint coverage overlaps.
//! Summaries remain stored unchanged. No facts are removed from summary text.
use super::*;
use pioneer_agent::compaction::composition::ScopedHistorySource;
use pioneer_provider::{ChatMessage, MessageProvenance};
use std::collections::{BTreeMap, BTreeSet};

/// Split only canonical input rows, whose order is already fixed in provenance.
/// This allows two snapshots of an input extended by steering to share their
/// common rows once without importing the later rows into the earlier snapshot.
async fn split_inputs(
    store: &CrudStore,
    workspace: &str,
    message: ChatMessage,
) -> Result<Vec<ChatMessage>> {
    let origin = message
        .provenance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("missing source origin"))?;
    if !origin
        .sources
        .iter()
        .all(|source| source.scope.starts_with("input:"))
    {
        return Ok(vec![message]);
    }
    let mut result = Vec::new();
    for reference in &origin.sources {
        let source = SourceRef {
            scope: reference.scope.clone(),
            id: reference.id.clone(),
            version: reference.version.clone(),
        };
        let payload =
            super::history::reference_payload(store, workspace, &origin.thread_id, &source).await?;
        let mut projection = super::history::input_message(&[serde_json::from_str(&payload)?])?;
        let mut provenance = origin.clone();
        provenance.sources = vec![reference.clone()];
        provenance.unit_id = format!("canonical-input:{}", reference.id);
        projection.provenance = Some(provenance);
        result.push(projection);
    }
    Ok(result)
}

pub(crate) async fn rematerialize_overlap(
    store: &CrudStore,
    workspace: &str,
    allowed: &BTreeSet<String>,
    messages: &[ChatMessage],
    checkpoints: &BTreeMap<ScopedHistorySource, BTreeSet<ScopedHistorySource>>,
    affected: &BTreeSet<ScopedHistorySource>,
) -> Result<Vec<ChatMessage>> {
    for thread in affected
        .iter()
        .map(|source| &source.thread)
        .collect::<BTreeSet<_>>()
    {
        ensure!(
            allowed.contains(thread),
            "overlap history is outside accepted scope"
        );
        super::history::prepare_history(&store, workspace, thread).await?;
    }
    let fence = store.compaction_history_read_fence().await?;
    let mut originals = BTreeMap::<String, Vec<ChatMessage>>::new();
    let mut result = Vec::new();
    for message in messages {
        let origin = message
            .provenance
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing source origin"))?;
        let checkpoint = origin
            .sources
            .iter()
            .find(|source| source.scope.starts_with("checkpoint:"));
        let closure = checkpoint.and_then(|reference| {
            checkpoints.get(&ScopedHistorySource {
                thread: origin.thread_id.clone(),
                source: SourceRef {
                    scope: reference.scope.clone(),
                    id: reference.id.clone(),
                    version: reference.version.clone(),
                },
            })
        });
        let Some(closure) = closure.filter(|closure| !closure.is_disjoint(affected)) else {
            result.extend(split_inputs(&store, workspace, message.clone()).await?);
            continue;
        };
        ensure!(origin.sources.len() == 1, "mixed checkpoint projection");
        let mut represented = BTreeSet::new();
        for source_thread in closure
            .iter()
            .map(|leaf| &leaf.thread)
            .collect::<BTreeSet<_>>()
        {
            ensure!(
                allowed.contains(source_thread),
                "compatible projection crosses accepted source scope"
            );
            if !originals.contains_key(source_thread) {
                let selected = checkpoints
                    .values()
                    .filter(|leaves| !leaves.is_disjoint(affected))
                    .flat_map(|leaves| leaves.iter())
                    .filter(|leaf| &leaf.thread == source_thread)
                    .map(|leaf| leaf.source.clone())
                    .collect();
                let raw = super::history::load_exact_line_history(
                    &store,
                    workspace,
                    source_thread,
                    &fence,
                    &selected,
                )
                .await?;
                let mut split = Vec::new();
                for message in raw {
                    split.extend(split_inputs(&store, workspace, message).await?);
                }
                originals.insert(source_thread.clone(), split);
            }
            for raw in &originals[source_thread] {
                let raw_origin = raw
                    .provenance
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("original has no source origin"))?;
                let leaves = raw_origin
                    .sources
                    .iter()
                    .map(|reference| ScopedHistorySource {
                        thread: raw_origin.thread_id.clone(),
                        source: SourceRef {
                            scope: reference.scope.clone(),
                            id: reference.id.clone(),
                            version: reference.version.clone(),
                        },
                    })
                    .collect::<BTreeSet<_>>();
                if leaves.is_disjoint(closure) {
                    continue;
                }
                ensure!(
                    leaves.is_subset(closure),
                    "original unit crosses checkpoint boundary"
                );
                let mut projection = raw.clone();
                let mut provenance: MessageProvenance = raw_origin.clone();
                provenance.context_thread = Some(
                    origin
                        .context_thread
                        .as_deref()
                        .unwrap_or(&origin.thread_id)
                        .to_owned(),
                );
                provenance.inherited = origin.inherited;
                for leaf in &leaves {
                    if let Some(command) = store
                        .compaction_task_delivery_command(workspace, &leaf.thread, &leaf.source)
                        .await?
                    {
                        ensure!(
                            provenance
                                .logical_turn_id
                                .as_ref()
                                .is_none_or(|previous| previous == &command),
                            "compatible unit spans different Task outcomes"
                        );
                        provenance.logical_turn_id = Some(command);
                    }
                }
                projection.provenance = Some(provenance);
                represented.extend(leaves);
                result.push(projection);
            }
        }
        ensure!(
            &represented == closure,
            "checkpoint originals changed or cannot form an exact projection"
        );
    }
    Ok(result)
}
