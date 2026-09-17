//! Resolve only trusted runtime locators into versioned canonical references.
use anyhow::{Result, ensure};
use pioneer_crud::{CrudStore, compaction::PagedSource};
use pioneer_provider::{ChatMessage, MessageSourceRef};
use std::collections::BTreeSet;

pub(crate) async fn resolve_message_origins(
    store: &CrudStore,
    workspace: &str,
    current_thread: &str,
    current_turn: &str,
    authorized_threads: &BTreeSet<String>,
    messages: &mut [ChatMessage],
) -> Result<()> {
    for message in messages {
        let Some(origin) = &mut message.provenance else {
            continue;
        };
        ensure!(
            origin.workspace_id == workspace
                && authorized_threads.contains(&origin.thread_id)
                && origin
                    .context_thread
                    .as_ref()
                    .is_none_or(|owner| authorized_threads.contains(owner)),
            "message source scope is not authorized"
        );
        let mut resolved = Vec::new();
        for source in &origin.sources {
            if !source.version.is_empty() {
                resolved.push(source.clone());
                continue;
            }
            let (kind, turn) = source
                .scope
                .split_once(':')
                .ok_or_else(|| anyhow::anyhow!("invalid runtime source locator"))?;
            if kind == "pending-input" {
                let mut after = 0;
                loop {
                    let page = store
                        .compaction_source_metadata_page(
                            workspace,
                            &origin.thread_id,
                            turn,
                            PagedSource::Input,
                            after,
                        )
                        .await?;
                    if page.entries.is_empty() {
                        break;
                    }
                    ensure!(
                        page.next_sequence > after,
                        "input source page made no progress"
                    );
                    after = page.next_sequence;
                    resolved.extend(page.entries.into_iter().map(|entry| MessageSourceRef {
                        scope: entry.reference.scope,
                        id: entry.reference.id,
                        version: entry.reference.version,
                    }));
                }
                origin.protected_input = origin.thread_id == current_thread && turn == current_turn;
                continue;
            }
            let reference = match kind {
                "pending-assistant" => {
                    store
                        .compaction_context_reference_for_item(
                            workspace,
                            &origin.thread_id,
                            turn,
                            &source.id,
                            "assistant_round",
                        )
                        .await?
                }
                "pending-tool" => {
                    store
                        .compaction_context_reference_for_item(
                            workspace,
                            &origin.thread_id,
                            turn,
                            &source.id,
                            "tool_result_v2",
                        )
                        .await?
                }
                "pending-item" => {
                    store
                        .compaction_tool_item_reference(
                            workspace,
                            &origin.thread_id,
                            turn,
                            &source.id,
                        )
                        .await?
                }
                _ => anyhow::bail!("unsupported runtime source locator"),
            }
            .ok_or_else(|| anyhow::anyhow!("acknowledged canonical source is missing"))?;
            resolved.push(MessageSourceRef {
                scope: reference.scope,
                id: reference.id,
                version: reference.version,
            });
        }
        ensure!(!resolved.is_empty(), "message has no durable source");
        for batch in resolved.chunks(pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize) {
            let sources = batch
                .iter()
                .map(|source| pioneer_compaction::SourceRef {
                    scope: source.scope.clone(),
                    id: source.id.clone(),
                    version: source.version.clone(),
                })
                .collect::<Vec<_>>();
            ensure!(
                store
                    .compaction_sources_current(workspace, &origin.thread_id, &sources)
                    .await?,
                "working history source changed; reload its canonical projection"
            );
        }
        for source in &resolved {
            if source.scope.starts_with("checkpoint:") {
                super::coverage::checkpoint_leaves(
                    &store,
                    workspace,
                    authorized_threads,
                    &pioneer_compaction::SourceRef {
                        scope: source.scope.clone(),
                        id: source.id.clone(),
                        version: source.version.clone(),
                    },
                )
                .await?;
            }
        }
        origin.sources = resolved;
    }
    Ok(())
}
