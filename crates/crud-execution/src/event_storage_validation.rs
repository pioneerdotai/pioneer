//! Permanent event payload and execution ownership validation.
use crate::turn_execution;
use anyhow::{Context, Result};
use pioneer_crud_conversations::{events::TurnEventPayload, turn_reads as turn};
use pioneer_protocol::{
    StorageOutputPolicy, TimelineOutputPolicy, ToolCallStatus, ToolDisplayPayload,
    ToolStoragePayload, TurnItem,
};
use sea_orm::ConnectionTrait;

pub(super) fn validate_turn_event_for_permanent_storage(event: &TurnEventPayload) -> Result<()> {
    match event {
        TurnEventPayload::TurnStarted(payload) if payload.actor.is_none() => {
            anyhow::bail!("new turn/started event is missing its required actor")
        }
        TurnEventPayload::ItemStarted(notification) => {
            validate_tool_payload_for_permanent_storage(&notification.item)
        }
        TurnEventPayload::ItemCompleted(notification) => {
            validate_tool_payload_for_permanent_storage(&notification.item)?;
            validate_terminal_tool_payload(&notification.item)
        }
        TurnEventPayload::ItemUpdated(notification) => {
            validate_tool_payload_for_permanent_storage(&notification.item)
        }
        _ => Ok(()),
    }
}

pub async fn validate_turn_event_durable_owner<C>(
    connection: &C,
    event: &TurnEventPayload,
) -> Result<()>
where
    C: ConnectionTrait,
{
    if matches!(event, TurnEventPayload::TurnStarted(_)) {
        return Ok(());
    }
    if turn::find_turn_by_thread_and_id(connection, event.thread_id(), event.turn_id())
        .await?
        .is_none()
    {
        anyhow::bail!(
            "refusing to append orphan `{}` event for missing turn `{}` in thread `{}`",
            event.event_type(),
            event.turn_id(),
            event.thread_id()
        );
    }
    Ok(())
}

pub async fn validate_turn_event_execution_owner<C>(
    connection: &C,
    event: &TurnEventPayload,
    expected_owner_id: Option<&str>,
) -> Result<()>
where
    C: ConnectionTrait,
{
    let Some(expected_owner_id) = expected_owner_id else {
        return Ok(());
    };
    let Some(execution) = turn_execution::find(connection, event.turn_id()).await? else {
        // Rows created before the execution-control migration have no durable
        // owner and retain their established event path.
        return Ok(());
    };
    if execution.owner_id != expected_owner_id {
        anyhow::bail!(
            "refusing `{}` event for Turn `{}` from stale execution owner",
            event.event_type(),
            event.turn_id()
        );
    }
    Ok(())
}

fn validate_tool_payload_for_permanent_storage(item: &TurnItem) -> Result<()> {
    let Some(tool_payload) = tool_payload_parts(item) else {
        return Ok(());
    };

    let is_in_progress = tool_payload.status == ToolCallStatus::InProgress;

    validate_tool_display_policy_shape(&tool_payload, is_in_progress)?;
    validate_tool_storage_policy_shape(&tool_payload, is_in_progress)?;

    let storage_json = serde_json::to_value(tool_payload.storage)
        .context("failed to serialize tool storage for validation")?;
    let display_json = serde_json::to_value(tool_payload.display)
        .context("failed to serialize tool display for validation")?;

    if let Some(key) = contains_any_key(&storage_json, &["llmView", "llm_view"]) {
        anyhow::bail!(
            "tool item `{}` attempted to persist retained llm context key `{key}`",
            tool_payload.tool_name
        );
    }
    if let Some(key) = contains_any_key(&display_json, &["llmView", "llm_view"]) {
        anyhow::bail!(
            "tool item `{}` attempted to display retained llm context key `{key}`",
            tool_payload.tool_name
        );
    }

    Ok(())
}

fn validate_terminal_tool_payload(item: &TurnItem) -> Result<()> {
    let Some(tool_payload) = tool_payload_parts(item) else {
        return Ok(());
    };

    if tool_payload.status == ToolCallStatus::InProgress {
        anyhow::bail!(
            "terminal tool item `{}` cannot remain in_progress",
            tool_payload.tool_name
        );
    }

    Ok(())
}

struct ToolPayloadParts<'a> {
    tool_name: &'a str,
    status: ToolCallStatus,
    output_policy: &'a pioneer_protocol::ToolOutputPolicySnapshot,
    display: &'a ToolDisplayPayload,
    storage: &'a ToolStoragePayload,
}

fn tool_payload_parts(item: &TurnItem) -> Option<ToolPayloadParts<'_>> {
    match item {
        TurnItem::CommandExecution {
            tool_name,
            status,
            output_policy,
            display,
            storage,
            ..
        }
        | TurnItem::FileChange {
            tool_name,
            status,
            output_policy,
            display,
            storage,
            ..
        }
        | TurnItem::WebSearch {
            tool_name,
            status,
            output_policy,
            display,
            storage,
            ..
        }
        | TurnItem::WebFetch {
            tool_name,
            status,
            output_policy,
            display,
            storage,
            ..
        }
        | TurnItem::Download {
            tool_name,
            status,
            output_policy,
            display,
            storage,
            ..
        }
        | TurnItem::DynamicToolCall {
            tool_name,
            status,
            output_policy,
            display,
            storage,
            ..
        } => Some(ToolPayloadParts {
            tool_name,
            status: *status,
            output_policy,
            display,
            storage,
        }),
        _ => None,
    }
}

fn validate_tool_display_policy_shape(
    tool_payload: &ToolPayloadParts<'_>,
    is_in_progress: bool,
) -> Result<()> {
    if is_in_progress && matches!(tool_payload.display, ToolDisplayPayload::Progress { .. }) {
        return Ok(());
    }

    match &tool_payload.output_policy.timeline {
        TimelineOutputPolicy::Full { max_bytes } => {
            if let ToolDisplayPayload::Shell { .. } = tool_payload.display {
                validate_json_size(
                    "display",
                    tool_payload.tool_name,
                    tool_payload.display,
                    *max_bytes,
                )?;
            }
            Ok(())
        }
        TimelineOutputPolicy::Summary { max_chars } => match tool_payload.display {
            ToolDisplayPayload::Summary(summary) => {
                validate_summary_chars("display", tool_payload.tool_name, summary, *max_chars)
            }
            ToolDisplayPayload::Hidden => Ok(()),
            _ => anyhow::bail!(
                "tool item `{}` display payload does not match summary timeline policy",
                tool_payload.tool_name
            ),
        },
        TimelineOutputPolicy::MetadataOnly | TimelineOutputPolicy::Hidden => {
            if matches!(tool_payload.display, ToolDisplayPayload::Hidden) {
                Ok(())
            } else {
                anyhow::bail!(
                    "tool item `{}` display payload must be hidden for metadata-only/hidden timeline policy",
                    tool_payload.tool_name
                )
            }
        }
    }
}

fn validate_tool_storage_policy_shape(
    tool_payload: &ToolPayloadParts<'_>,
    is_in_progress: bool,
) -> Result<()> {
    if is_in_progress && matches!(tool_payload.storage, ToolStoragePayload::Metadata { .. }) {
        return Ok(());
    }

    match &tool_payload.output_policy.storage {
        StorageOutputPolicy::Full { max_bytes } => {
            match tool_payload.storage {
                ToolStoragePayload::Shell { .. } => {
                    validate_json_size(
                        "storage",
                        tool_payload.tool_name,
                        tool_payload.storage,
                        *max_bytes,
                    )?;
                }
                ToolStoragePayload::Summary(_)
                | ToolStoragePayload::Metadata { .. }
                | ToolStoragePayload::None => {}
            }
            Ok(())
        }
        StorageOutputPolicy::Summary { max_chars } => match tool_payload.storage {
            ToolStoragePayload::Summary(summary) => {
                validate_summary_chars("storage", tool_payload.tool_name, summary, *max_chars)
            }
            ToolStoragePayload::None => Ok(()),
            _ => anyhow::bail!(
                "tool item `{}` storage payload does not match summary storage policy",
                tool_payload.tool_name
            ),
        },
        StorageOutputPolicy::MetadataOnly => {
            if matches!(
                tool_payload.storage,
                ToolStoragePayload::Metadata { .. } | ToolStoragePayload::None
            ) {
                Ok(())
            } else {
                anyhow::bail!(
                    "tool item `{}` storage payload must be metadata-only",
                    tool_payload.tool_name
                )
            }
        }
        StorageOutputPolicy::None => {
            if matches!(tool_payload.storage, ToolStoragePayload::None) {
                Ok(())
            } else {
                anyhow::bail!(
                    "tool item `{}` storage payload must be empty",
                    tool_payload.tool_name
                )
            }
        }
    }
}

fn validate_summary_chars(
    channel: &str,
    tool_name: &str,
    summary: &pioneer_protocol::ToolOutputSummary,
    max_chars: usize,
) -> Result<()> {
    let visible_chars = summary.title.chars().count()
        + summary
            .lines
            .iter()
            .map(|line| line.chars().count())
            .sum::<usize>();
    if visible_chars > max_chars {
        anyhow::bail!(
            "tool item `{tool_name}` {channel} summary exceeds policy limit: {visible_chars} > {max_chars}"
        );
    }
    Ok(())
}

fn validate_json_size<T: serde::Serialize>(
    channel: &str,
    tool_name: &str,
    payload: &T,
    max_bytes: usize,
) -> Result<()> {
    let size = serde_json::to_vec(payload)
        .context("failed to serialize tool payload for size validation")?
        .len();
    if size > max_bytes {
        anyhow::bail!(
            "tool item `{tool_name}` {channel} payload exceeds policy limit: {size} > {max_bytes}"
        );
    }
    Ok(())
}

fn contains_any_key(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => {
            for (key, nested) in map {
                if keys.iter().any(|candidate| key == candidate) {
                    return Some(key.clone());
                }
                if let Some(found) = contains_any_key(nested, keys) {
                    return Some(found);
                }
            }
            None
        }
        serde_json::Value::Array(items) => {
            items.iter().find_map(|item| contains_any_key(item, keys))
        }
        _ => None,
    }
}

#[cfg(test)]
#[path = "event_storage_validation_tests.rs"]
mod tests;
