//! Reconcile the narrow crash boundary between the acknowledged terminal shell
//! item and its provider replay row. Reads retained facts; never invokes a tool.
use super::*;
use pioneer_protocol::{ToolCallStatus, ToolStoragePayload, TurnItem};
use pioneer_provider::{ChatMessage, ModelInputItem};

pub(crate) async fn retained_shell_outcome(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    item_id: &str,
    provider_call_id: &str,
    tool_name: &str,
) -> Result<Option<(SourceRef, ChatMessage)>> {
    let Some(reference) = store
        .compaction_tool_item_reference(workspace, thread, turn, item_id)
        .await?
    else {
        return Ok(None);
    };
    super::history::prepare_references(store, workspace, thread, std::slice::from_ref(&reference))
        .await?;
    let payload = super::history::reference_payload(store, workspace, thread, &reference)
        .await
        .map_err(|_| anyhow::anyhow!("terminal tool source changed during recovery"))?;
    let item: TurnItem = serde_json::from_str(&payload)?;
    ensure!(item.item_id() == item_id, "terminal tool identity mismatch");
    let value = serde_json::to_value(&item)?;
    ensure!(
        value.get("toolName").and_then(serde_json::Value::as_str) == Some(tool_name),
        "terminal tool name mismatch"
    );
    let status: ToolCallStatus = serde_json::from_value(value["status"].clone())?;
    if status == ToolCallStatus::InProgress {
        return Ok(None);
    }
    let storage: ToolStoragePayload = serde_json::from_value(value["storage"].clone())?;
    let ToolStoragePayload::Shell { .. } = storage else {
        return Ok(None);
    };
    let mut observation = serde_json::to_value(storage)?;
    let fields = observation
        .as_object_mut()
        .expect("tagged shell storage is an object");
    fields.remove("kind");
    fields.insert("status".into(), serde_json::to_value(status)?);
    fields.insert("success".into(), value["success"].clone());
    fields.insert("tool_outcome".into(), value["outcome"].clone());
    fields.insert("recovered_from_terminal_item".into(), true.into());
    let full = ModelInputItem::tool_result(
        provider_call_id,
        tool_name,
        observation.to_string(),
        Some(observation),
    )
    .into_chat_message();
    let locator = serde_json::json!({"workspace_id": workspace, "thread_id": thread, "turn_id": turn, "item_id": item_id}).to_string();
    let message = pioneer_agent::compaction::restored_tool_result_message(&full, &locator)?;
    Ok(Some((reference, message)))
}

pub(crate) async fn retained_tool_policy(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    item_id: &str,
) -> Result<Option<pioneer_protocol::ToolRecoveryPolicySnapshot>> {
    let Some(source) = store
        .compaction_item_reference(workspace, thread, turn, item_id)
        .await?
    else {
        return Ok(None);
    };
    super::history::prepare_references(store, workspace, thread, std::slice::from_ref(&source))
        .await?;
    let payload = super::history::reference_payload(&store, workspace, thread, &source).await?;
    let item: TurnItem = serde_json::from_str(&payload)?;
    ensure!(
        item.item_id() == item_id,
        "retained policy item identity mismatch"
    );
    Ok(item.recovery_policy().cloned())
}
