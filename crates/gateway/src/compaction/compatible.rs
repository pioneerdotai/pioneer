//! Normalize overlapping unsummarized input rows. Published summaries remain
//! atomic and are never expanded through this module.
use super::*;
use pioneer_agent::compaction::composition::ScopedHistorySource;
use pioneer_provider::{ChatMessage, MessageProvenance};
use std::collections::BTreeSet;

async fn split_inputs(
    store: &CrudStore,
    workspace: &str,
    message: ChatMessage,
) -> Result<Vec<ChatMessage>> {
    let origin = message
        .provenance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("missing source origin"))?;
    if origin.sources.len() < 2
        || !origin
            .sources
            .iter()
            .all(|source| source.scope.starts_with("input:"))
    {
        return Ok(vec![message]);
    }
    let mut result = Vec::with_capacity(origin.sources.len());
    for reference in &origin.sources {
        let source = SourceRef {
            scope: reference.scope.clone(),
            id: reference.id.clone(),
            version: reference.version.clone(),
        };
        let payload =
            super::history::reference_payload(store, workspace, &origin.thread_id, &source).await?;
        let mut projection = super::history::input_message(&[serde_json::from_str(&payload)?])?;
        let mut provenance: MessageProvenance = origin.clone();
        provenance.sources = vec![reference.clone()];
        provenance.unit_id = format!("canonical-input:{}", reference.id);
        projection.provenance = Some(provenance);
        result.push(projection);
    }
    Ok(result)
}

pub(crate) async fn split_raw_overlap(
    store: &CrudStore,
    workspace: &str,
    allowed: &BTreeSet<String>,
    messages: &[ChatMessage],
    affected: &BTreeSet<ScopedHistorySource>,
) -> Result<Vec<ChatMessage>> {
    let mut result = Vec::new();
    for message in messages {
        let origin = message
            .provenance
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing source origin"))?;
        ensure!(
            allowed.contains(&origin.thread_id),
            "overlap history is outside accepted scope"
        );
        let overlaps = origin.sources.iter().any(|reference| {
            affected.contains(&ScopedHistorySource {
                thread: origin.thread_id.clone(),
                source: SourceRef {
                    scope: reference.scope.clone(),
                    id: reference.id.clone(),
                    version: reference.version.clone(),
                },
            })
        });
        if overlaps {
            result.extend(split_inputs(store, workspace, message.clone()).await?);
        } else {
            result.push(message.clone());
        }
    }
    Ok(result)
}
