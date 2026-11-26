use anyhow::{Result, ensure};
use pioneer_agent::compaction::thread_pages::{self, ResultReplay};
use pioneer_compaction::SourceRef;
use pioneer_crud::CrudStore;
use pioneer_protocol::ThreadToolResultReadParams;
use pioneer_provider::ChatMessage;

pub(crate) fn presentation_target<'a>(source: &str, item: Option<&'a str>) -> Option<&'a str> {
    if source == "runtime_context_v1" {
        item.and_then(thread_pages::presentation_target)
    } else {
        None
    }
}

pub(crate) fn replay_target<'a>(source: &str, item: Option<&'a str>) -> Option<&'a str> {
    if source == "runtime_context_v1" {
        item.and_then(thread_pages::replay_target)
    } else {
        None
    }
}

pub(crate) async fn read_replay(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    source: Option<&SourceRef>,
) -> Result<Option<ResultReplay>> {
    match source {
        Some(source) => Ok(Some(
            read_private_record(store, workspace, thread, source).await?,
        )),
        None => Ok(None),
    }
}

/// Read a presentation captured at the same point as the raw result. The
/// payload has no authority to change which source the reader may disclose.
pub(crate) async fn restore_tool_result(
    store: &CrudStore,
    message: &ChatMessage,
    reference: &str,
    presentation: Option<&SourceRef>,
    replay: Option<&ResultReplay>,
) -> Result<ChatMessage> {
    if let Some(ResultReplay::Page { page }) = replay {
        ensure!(
            thread_pages::is_thread_reader(message),
            "thread page replay identity mismatch"
        );
        let mut restored = thread_pages::restore_frame(message, Some(page))?;
        if let Some(source) = presentation {
            let params: ThreadToolResultReadParams = serde_json::from_str(reference)?;
            restored.content = read_presentation(store, &params, source).await?;
        }
        return Ok(restored);
    }
    pioneer_agent::compaction::restored_tool_result_message(message, reference)
}

async fn read_presentation(
    store: &CrudStore,
    params: &ThreadToolResultReadParams,
    source: &SourceRef,
) -> Result<String> {
    let presentation: ChatMessage =
        read_private_record(store, &params.workspace_id, &params.thread_id, source).await?;
    thread_pages::verify_presentation(&presentation, params)
}

async fn read_private_record<T: serde::de::DeserializeOwned>(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    source: &SourceRef,
) -> Result<T> {
    let payload = store
        .compaction_reference_payload(workspace, thread, source)
        .await?
        .ok_or_else(|| anyhow::anyhow!("tool result replay record disappeared"))?;
    let view: pioneer_tools::ToolResultView = serde_json::from_str(&payload)?;
    let pioneer_tools::ToolResultView::Json {
        value,
        truncated: false,
    } = view
    else {
        anyhow::bail!("tool result replay record is incomplete")
    };
    Ok(serde_json::from_value(value)?)
}

async fn find_marker(
    store: &CrudStore,
    params: &ThreadToolResultReadParams,
    marker: &str,
) -> Result<Option<SourceRef>> {
    store
        .compaction_context_reference_for_item(
            &params.workspace_id,
            &params.thread_id,
            &params.turn_id,
            marker,
            "runtime_context_v1",
        )
        .await
}

/// Recovery can capture history before the previous execution has completely
/// shut down. Recheck the saved choice and the dispatch fence in the new owner
/// before treating that snapshot's candidate as unwritten and unsent.
pub(super) async fn recheck_pending_page(
    store: &CrudStore,
    params: &ThreadToolResultReadParams,
    message: &mut ChatMessage,
) -> Result<bool> {
    if let Some(source) = find_marker(
        store,
        params,
        &thread_pages::presentation_item(&params.item_id),
    )
    .await?
    {
        message.content = read_presentation(store, params, &source).await?;
        return Ok(false);
    }
    let origin = message
        .provenance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("recovered thread page has no canonical round"))?;
    Ok(
        find_marker(store, params, &thread_pages::dispatch_item(&origin.unit_id))
            .await?
            .is_none(),
    )
}

pub(super) async fn frozen_replay(
    store: &CrudStore,
    reference: &str,
) -> Result<Option<ResultReplay>> {
    let params: ThreadToolResultReadParams = serde_json::from_str(reference)?;
    let source = find_marker(store, &params, &thread_pages::replay_item(&params.item_id)).await?;
    read_replay(
        store,
        &params.workspace_id,
        &params.thread_id,
        source.as_ref(),
    )
    .await
}

/// Source-backed Frozen descriptors already verify the exact recorded wire
/// hash. Offer a retained pointer as a candidate alongside the full result.
pub(super) async fn frozen_presentation(
    store: &CrudStore,
    message: &ChatMessage,
    reference: &str,
    replay: Option<&ResultReplay>,
) -> Result<Option<ChatMessage>> {
    if !matches!(replay, Some(ResultReplay::Page { .. })) {
        return Ok(None);
    }
    let params: ThreadToolResultReadParams = serde_json::from_str(reference)?;
    let Some(source) = find_marker(
        store,
        &params,
        &thread_pages::presentation_item(&params.item_id),
    )
    .await?
    else {
        return Ok(None);
    };
    Ok(Some(
        restore_tool_result(store, message, reference, Some(&source), replay).await?,
    ))
}
