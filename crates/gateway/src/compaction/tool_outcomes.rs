//! Reconcile the narrow crash boundary between the acknowledged terminal shell
//! item and its provider replay row. Reads retained facts; never invokes a tool.
use super::*;
use pioneer_protocol::{ToolCallStatus, ToolStoragePayload, TurnItem};
use pioneer_provider::{ChatMessage, ModelInputItem};

async fn shell_outcome(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    item_id: &str,
    provider_call_id: &str,
    tool_name: &str,
    compact: bool,
) -> Result<
    Option<(
        SourceRef,
        ChatMessage,
        pioneer_protocol::ToolOutputPolicySnapshot,
    )>,
> {
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
    let policy: pioneer_protocol::ToolOutputPolicySnapshot =
        serde_json::from_value(value["outputPolicy"].clone())?;
    let full = if compact {
        // This receipt precedes the terminal item and contains no output bytes.
        let controls = store
            .shell_control_receipt(workspace, thread, turn, item_id)
            .await?
            .map(|payload| serde_json::from_str::<pioneer_tools::ToolResultView>(&payload))
            .transpose()?;
        if let Some(pioneer_tools::ToolResultView::Json {
            value: controls,
            truncated: false,
        }) = controls
        {
            for field in ["session_id", "output_stats", "command"] {
                if let Some(value) = controls.get(field) {
                    observation[field] = value.clone();
                }
            }
        }
        let fields = observation
            .as_object_mut()
            .expect("shell observation object");
        fields.entry("stdout").or_insert_with(|| "".into());
        fields.entry("stderr").or_insert_with(|| "".into());
        if let Some(command) = value.get("command") {
            fields.entry("command").or_insert_with(|| command.clone());
        }
        if let Some(session) = value.pointer("/arguments/session_id") {
            fields
                .entry("session_id")
                .or_insert_with(|| session.clone());
        }
        let outcome = value
            .get("outcome")
            .filter(|outcome| !outcome.is_null())
            .map(|outcome| serde_json::from_value::<pioneer_tools::ToolOutcome>(outcome.clone()))
            .transpose()?
            .unwrap_or_default();
        let projection = pioneer_tools::project_tool_result(pioneer_tools::ToolProjectionInput {
            call_id: provider_call_id,
            tool_name,
            arguments: &value["arguments"],
            raw_output_text: "",
            raw_output_json: &observation,
            success: value["success"] == true,
            outcome: &outcome,
            output_policy: &policy,
            output_projection: &pioneer_tools::ToolOutputProjectionKind::DynamicShell,
        });
        ensure!(
            !matches!(projection.llm_view, pioneer_tools::ToolResultView::Empty),
            "shell recovery metadata exceeds output policy budget"
        );
        let mut model = projection.llm_payload();
        // Missing historical classification stays unknown; do not infer a retry
        // class merely because recovery is constructing the first model view.
        model["tool_outcome"] = value
            .get("outcome")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        ModelInputItem::tool_result(provider_call_id, tool_name, model.to_string(), Some(model))
            .into_chat_message()
    } else {
        ModelInputItem::tool_result(
            provider_call_id,
            tool_name,
            observation.to_string(),
            Some(observation),
        )
        .into_chat_message()
    };
    let locator = serde_json::json!({"workspace_id": workspace, "thread_id": thread, "turn_id": turn, "item_id": item_id}).to_string();
    let message = if compact {
        pioneer_agent::compaction::recovered_shell_result_message(
            full,
            &policy,
            matches!(
                policy.storage,
                pioneer_protocol::StorageOutputPolicy::Full { .. }
            ),
            &locator,
        )?
    } else {
        pioneer_agent::compaction::restored_tool_result_message(&full, &locator)?
    };
    Ok(Some((reference, message, policy)))
}

/// Historical frozen references without replay_source must retain their old bytes.
pub(crate) async fn retained_shell_outcome(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    item_id: &str,
    provider_call_id: &str,
    tool_name: &str,
) -> Result<Option<(SourceRef, ChatMessage)>> {
    Ok(shell_outcome(
        store,
        workspace,
        thread,
        turn,
        item_id,
        provider_call_id,
        tool_name,
        false,
    )
    .await?
    .map(|(source, message, _)| (source, message)))
}

/// Only called for canonical calls with no durable result. Commit the exact
/// representation before admitting it to restored model history; never run shell.
pub(crate) async fn recover_undelivered_shell_outcome(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    item_id: &str,
    provider_call_id: &str,
    tool_name: &str,
) -> Result<Option<(SourceRef, ChatMessage)>> {
    let Some((source, message, policy)) = shell_outcome(
        store,
        workspace,
        thread,
        turn,
        item_id,
        provider_call_id,
        tool_name,
        true,
    )
    .await?
    else {
        return Ok(None);
    };
    let payload = pioneer_tools::ToolResultView::Json {
        value: serde_json::to_value(&message)?,
        truncated: false,
    };
    let entry = pioneer_crud::NewTurnLlmContextEntry {
        turn_id: turn.into(),
        item_id: Some(item_id.into()),
        attempt_id: None,
        sequence: 0,
        source: "tool_result_v2".into(),
        tool_name: Some(tool_name.into()),
        payload: serde_json::to_string(&payload)?,
        output_policy_snapshot: serde_json::to_string(&policy)?,
        created_at: chrono::Utc::now().fixed_offset(),
        expires_at: None,
    };
    let committed = store
        .commit_recovered_shell_result(workspace, thread, turn, item_id, &source, entry)
        .await?;
    let view: pioneer_tools::ToolResultView = serde_json::from_str(&committed.payload)?;
    let pioneer_tools::ToolResultView::Json {
        value,
        truncated: false,
    } = view
    else {
        anyhow::bail!("invalid recovered shell canonical result");
    };
    let message: ChatMessage = serde_json::from_value(value)?;
    ensure!(
        message.tool_call_id.as_deref() == Some(provider_call_id)
            && message.name.as_deref() == Some(tool_name),
        "recovered result binding mismatch"
    );
    Ok(Some((source, message)))
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
