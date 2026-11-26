//! A complete saved page owns its raw cursor and access checks. The working
//! request can use a different presentation without changing that source.
use anyhow::{Result, ensure};
use pioneer_protocol::ThreadToolResultReadParams;
use pioneer_provider::{ChatMessage, Role, types::canonical_json_string};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

const REPLAY_ITEM_PREFIX: &str = "result-replay:";
const DISPATCH_ITEM_PREFIX: &str = "thread-page-dispatch:";
const PENDING_REFERENCE: &str = "pending_thread_page_reference";
const PRESENTATION_KIND: &str = "thread_page_presentation";
const PRESENTATION_ITEM_PREFIX: &str = "thread-page-presentation:";

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Frame {
    Json {
        fields: Map<String, Value>,
        removed: Vec<String>,
    },
    Text(String),
}

pub fn is_thread_reader(message: &ChatMessage) -> bool {
    message.role == Role::Tool
        && matches!(
            message.name.as_deref(),
            Some(
                "threads_list"
                    | "threads_history_read"
                    | "threads_children_list"
                    | "threads_tools_result_read"
            )
        )
}

fn metadata(message: &mut ChatMessage) -> &mut Value {
    message.application_context.get_or_insert_with(|| json!({}))
}

#[derive(Serialize, Deserialize)]
pub struct PageReplay {
    frame: Frame,
    pub deferrable: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResultReplay {
    Page {
        page: PageReplay,
    },
    Task {
        outcomes: std::collections::BTreeSet<crate::ObservedTaskOutcome>,
    },
}

/// Store only the differences introduced by the model framer in the
/// normal JSON case, in a separate private record.
pub(crate) fn capture_page(
    raw: &ChatMessage,
    model: &ChatMessage,
    deferrable: bool,
) -> ResultReplay {
    let frame = match (
        serde_json::from_str::<Map<String, Value>>(&raw.content),
        serde_json::from_str::<Map<String, Value>>(&model.content),
    ) {
        (Ok(mut page), Ok(model_fields)) => {
            let removed: Vec<_> = page
                .keys()
                .filter(|key| !model_fields.contains_key(*key))
                .cloned()
                .collect();
            let fields: Map<_, _> = model_fields
                .into_iter()
                .filter(|(key, value)| page.get(key) != Some(value))
                .collect();
            for key in &removed {
                page.remove(key);
            }
            page.extend(fields.clone());
            if canonical_json_string(&Value::Object(page)) == model.content {
                Frame::Json { fields, removed }
            } else {
                Frame::Text(model.content.clone())
            }
        }
        _ => Frame::Text(model.content.clone()),
    };
    ResultReplay::Page {
        page: PageReplay { frame, deferrable },
    }
}

pub fn restore_frame(raw: &ChatMessage, replay: Option<&PageReplay>) -> Result<ChatMessage> {
    let mut message = raw.clone();
    message.content = match replay.map(|saved| &saved.frame) {
        Some(Frame::Text(text)) => text.clone(),
        Some(Frame::Json { fields, removed }) => {
            let mut page: Map<String, Value> = serde_json::from_str(&raw.content)?;
            for key in removed {
                page.remove(key);
            }
            page.extend(fields.clone());
            canonical_json_string(&Value::Object(page))
        }
        None => legacy_page_content(raw).unwrap_or_else(|| raw.content.clone()),
    };
    ensure!(
        message.content.len() <= pioneer_protocol::TOOL_RESPONSE_MAX_BYTES,
        "saved thread page exceeds projection limit"
    );
    Ok(message)
}

fn legacy_page_content(raw: &ChatMessage) -> Option<String> {
    let mut page = serde_json::from_str::<Value>(&raw.content).ok()?;
    let fields = page.as_object_mut()?;
    // Require the native success response shape. Keep errors and entries that
    // already held their framed model response as recorded.
    if fields.contains_key("tool_outcome")
        || fields.contains_key("partial_output")
        || !fields.get("eof").is_some_and(Value::is_boolean)
    {
        return None;
    }
    let is_page = match raw.name.as_deref()? {
        "threads_history_read" => fields.get("events")?.is_array(),
        "threads_list" | "threads_children_list" => fields.get("threads")?.is_array(),
        "threads_tools_result_read" => fields.get("text")?.is_string(),
        _ => false,
    };
    if !is_page {
        return None;
    }
    fields
        .entry("truncated".to_owned())
        .or_insert(Value::Bool(false));
    fields.insert(
        "tool_outcome".into(),
        serde_json::to_value(pioneer_tools::ToolOutcome::ok()).ok()?,
    );
    fields.insert(
        "partial_output".into(),
        json!({
            "is_partial": false, "reason": null, "continuation_available": false,
        }),
    );
    Some(canonical_json_string(&page))
}

pub(crate) fn mark_fresh(message: &mut ChatMessage, reference: Value) {
    metadata(message)[PENDING_REFERENCE] = reference;
}

pub fn mark_recovered(message: &mut ChatMessage, mut reference: Value) {
    reference["check_dispatch"] = Value::Bool(true);
    mark_fresh(message, reference);
}

pub(crate) fn fresh_reference(message: &ChatMessage) -> Option<&Value> {
    message.application_context.as_ref()?.get(PENDING_REFERENCE)
}

pub fn same_result(first: &ChatMessage, second: &ChatMessage) -> bool {
    is_thread_reader(first)
        && first.name == second.name
        && first.tool_call_id == second.tool_call_id
        && first
            .provenance
            .as_ref()
            .zip(second.provenance.as_ref())
            .is_some_and(|(a, b)| {
                a.workspace_id == b.workspace_id
                    && a.thread_id == b.thread_id
                    && a.unit_id == b.unit_id
            })
}

pub fn take_fresh_reference(
    message: &mut ChatMessage,
) -> Result<Option<(ThreadToolResultReadParams, bool)>> {
    if !is_thread_reader(message) {
        return Ok(None);
    }
    let Some(metadata) = message
        .application_context
        .as_mut()
        .and_then(Value::as_object_mut)
    else {
        return Ok(None);
    };
    let reference = metadata.remove(PENDING_REFERENCE);
    if metadata.is_empty() {
        message.application_context = None;
    }
    let Some(reference) = reference else {
        return Ok(None);
    };
    let check_dispatch = reference["check_dispatch"].as_bool().unwrap_or(false);
    Ok(Some((serde_json::from_value(reference)?, check_dispatch)))
}

pub fn presentation_item(item: &str) -> String {
    format!("{PRESENTATION_ITEM_PREFIX}{item}")
}

pub fn presentation_target(item: &str) -> Option<&str> {
    item.strip_prefix(PRESENTATION_ITEM_PREFIX)
}

pub fn pointer_content(reference: &ThreadToolResultReadParams) -> String {
    // The stored reference uses the same four fields as the existing
    // threads_tools_result_read input. Its output has a different cursor.
    json!({
        "fullPageSaved": true,
        "readTool": "threads_tools_result_read",
        "reference": saved_reference(reference),
        "instructions": "Read the complete saved page using reference. Follow the result-reader cursor until eof before using any continuation cursor from the saved page.",
    })
    .to_string()
}

fn saved_reference(reference: &ThreadToolResultReadParams) -> Value {
    json!({
        "workspace_id": reference.workspace_id,
        "thread_id": reference.thread_id,
        "turn_id": reference.turn_id,
        "item_id": reference.item_id,
    })
}

pub fn verify_presentation(
    saved: &ChatMessage,
    reference: &ThreadToolResultReadParams,
) -> Result<String> {
    let pointer: Value = serde_json::from_str(&saved.content)?;
    ensure!(
        saved.role == Role::User
            && is_presentation(saved)
            && pointer["fullPageSaved"] == true
            && pointer["readTool"] == "threads_tools_result_read"
            && pointer["reference"] == saved_reference(reference),
        "invalid retained thread page presentation"
    );
    Ok(saved.content.clone())
}

fn is_presentation(message: &ChatMessage) -> bool {
    message
        .application_context
        .as_ref()
        .and_then(|metadata| metadata.get("kind"))
        .and_then(Value::as_str)
        == Some(PRESENTATION_KIND)
}

pub fn dispatch_item(unit: &str) -> String {
    format!("{DISPATCH_ITEM_PREFIX}{unit}")
}

pub fn dispatch_target(item: &str) -> Option<&str> {
    item.strip_prefix(DISPATCH_ITEM_PREFIX)
}

pub(crate) async fn persist_dispatch(
    events: &crate::AgentEventHub,
    thread: &str,
    turn: &str,
    unit: &str,
) -> std::result::Result<(), crate::AgentEventHubError> {
    persist_marker(
        events,
        thread,
        turn,
        dispatch_item(unit),
        json!({"kind":"thread_page_dispatch_v1"}),
    )
    .await
}

pub async fn persist_presentation(
    context: &super::controller::NativeContext,
    reference: &ThreadToolResultReadParams,
) -> Result<()> {
    let mut message = ChatMessage::user(pointer_content(reference));
    message.application_context = Some(json!({"kind": PRESENTATION_KIND}));
    persist_marker(
        &context.events,
        &context.thread_id,
        &context.turn_id,
        presentation_item(&reference.item_id),
        serde_json::to_value(message)?,
    )
    .await?;
    Ok(())
}

pub fn replay_item(item: &str) -> String {
    format!("{REPLAY_ITEM_PREFIX}{item}")
}

pub fn replay_target(item: &str) -> Option<&str> {
    item.strip_prefix(REPLAY_ITEM_PREFIX)
}

pub(crate) async fn persist_replay(
    events: &crate::AgentEventHub,
    thread: &str,
    turn: &str,
    item: &str,
    replay: &ResultReplay,
) -> Result<()> {
    // Settle metadata first. A reader may see the unmodified result as soon as
    // its canonical row commits. A crash before that row cannot expose a view.
    persist_marker(
        events,
        thread,
        turn,
        replay_item(item),
        serde_json::to_value(replay)?,
    )
    .await?;
    Ok(())
}

async fn persist_marker(
    events: &crate::AgentEventHub,
    thread: &str,
    turn: &str,
    item: String,
    value: Value,
) -> std::result::Result<(), crate::AgentEventHubError> {
    events
        .publish_durable_and_wait(
            pioneer_protocol::AgentDurableEvent::TurnLlmContextAppended {
                thread_id: thread.to_owned(),
                turn_id: turn.to_owned(),
                item_id: item,
                attempt_id: None,
                sequence: 0,
                source: "runtime_context_v1".to_owned(),
                tool_name: String::new(),
                payload: pioneer_protocol::ToolResultView::Json {
                    value,
                    truncated: false,
                },
                output_policy_snapshot: pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name(
                    "runtime_context",
                ),
            },
        )
        .await
}
