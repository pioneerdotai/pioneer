//! Source-aware canonical line loading. All database reads release Maintenance
//! capacity before decoding. A shared fence can freeze several related lines.
use super::*;

use pioneer_crud::{
    CanonicalTurnEventPayload as Event,
    compaction::{HistoryReadFence, PagedSource, SourceRecord},
};
use pioneer_provider::{
    CanonicalProviderRoundEnvelope, ChatMessage, MessageProvenance, MessageSourceRef, Role,
};
use std::collections::{BTreeMap, BTreeSet};

/// The requesting operation can finish work not yet completed by proactive
/// maintenance. Each quantum releases SQLite capacity; cancellation leaves a
/// durable cursor for either owner. Capture the history fence only after this
/// returns.
pub(crate) async fn prepare_history(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
) -> Result<()> {
    while !store
        .compaction_prepare_history_quantum(workspace, thread)
        .await?
    {
        tokio::task::yield_now().await;
    }
    Ok(())
}

pub(crate) async fn source_payload(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    source: &mut SourceRecord,
) -> Result<String> {
    if let Some(payload) = source.payload.take() {
        return Ok(payload);
    }
    reference_payload(store, workspace, thread, &source.reference).await
}

pub(crate) async fn reference_payload(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    reference: &SourceRef,
) -> Result<String> {
    let mut text = String::new();
    let mut offset = 0;
    loop {
        let fragment = store
            .compaction_reference_fragment(workspace, thread, reference, offset)
            .await?
            .ok_or_else(|| anyhow::anyhow!("selected history source disappeared"))?;
        text.push_str(&fragment.text);
        let Some(next) = fragment.next_character else {
            return Ok(text);
        };
        ensure!(next > offset, "history fragment made no progress");
        offset = next;
    }
}

async fn metadata(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    kind: PagedSource,
    high_water: i64,
    fence: &HistoryReadFence,
) -> Result<Vec<SourceRecord>> {
    let capture_order = match kind {
        PagedSource::Input => fence.input_order,
        PagedSource::Event => fence.event_order,
        PagedSource::ProviderContext => fence.context_order,
    };
    let mut records = Vec::new();
    let mut after = 0;
    while after < high_water {
        let page = store
            .compaction_source_page_at_fence(workspace, thread, turn, kind, after, capture_order)
            .await?;
        ensure!(
            page.next_sequence > after,
            "history discovery lost its captured boundary"
        );
        after = page.next_sequence;
        records.extend(
            page.entries
                .into_iter()
                .filter(|row| row.sequence <= high_water),
        );
    }
    Ok(records)
}
fn origin(
    workspace: &str,
    thread: &str,
    turn: &str,
    unit: &str,
    sources: Vec<SourceRef>,
) -> MessageProvenance {
    MessageProvenance {
        logical_turn_id: None,
        workspace_id: workspace.into(),
        thread_id: thread.into(),
        context_thread: None,
        unit_id: format!("{turn}:{unit}"),
        sources: sources
            .into_iter()
            .map(|source| MessageSourceRef {
                scope: source.scope,
                id: source.id,
                version: source.version,
            })
            .collect(),
        complete: true,
        protected_input: false,
        inherited: false,
    }
}
pub(crate) fn input_message(inputs: &[pioneer_protocol::UserInput]) -> Result<ChatMessage> {
    // Historical attachments remain typed original references. Current-turn
    // attachment materialization is owned by the native request compiler.
    let mut text = Vec::new();
    for input in inputs {
        match input {
            pioneer_protocol::UserInput::Text { text: value, .. } => text.push(value.clone()),
            reference => text.push(format!(
                "Historical input reference: {}",
                serde_json::to_string(reference)?
            )),
        }
    }
    Ok(ChatMessage::user(text.join("\n")))
}
struct Round {
    sequence: i64,
    envelope: CanonicalProviderRoundEnvelope,
    assistant_source: SourceRef,
    results: BTreeMap<String, (SourceRef, ChatMessage)>,
}
fn finish_round(
    workspace: &str,
    thread: &str,
    turn: &str,
    round: Round,
    active: bool,
    output: &mut Vec<(i64, Vec<ChatMessage>)>,
) -> Result<()> {
    let calls = round
        .envelope
        .message
        .tool_calls
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("canonical round has no calls"))?;
    ensure!(
        round.envelope.version == 1
            && !round.envelope.round_id.trim().is_empty()
            && round.envelope.message.role == Role::Assistant
            && round.envelope.termination == pioneer_provider::ProviderTermination::ToolCalls
            && !calls.is_empty()
            && calls.len() == round.envelope.calls.len(),
        "invalid canonical provider round"
    );
    let mut ids = BTreeSet::new();
    let mut items = BTreeSet::new();
    for (ordinal, (call, identity)) in calls.iter().zip(&round.envelope.calls).enumerate() {
        ensure!(
            identity.ordinal as usize == ordinal
                && !call.id.trim().is_empty()
                && !call.name.trim().is_empty()
                && !identity.turn_item_id.trim().is_empty()
                && serde_json::from_str::<serde_json::Value>(&call.arguments).is_ok()
                && identity.provider_call_id == call.id
                && ids.insert(call.id.clone())
                && items.insert(identity.turn_item_id.clone()),
            "ambiguous canonical call identity"
        );
    }
    ensure!(
        round.results.keys().all(|item| items.contains(item)),
        "foreign result in canonical round"
    );
    let complete = items.iter().all(|item| round.results.contains_key(item));
    if !complete && active {
        return Ok(());
    }
    let unit = &round.envelope.round_id;
    let mut assistant = round.envelope.message.clone();
    assistant.provenance = Some(origin(
        workspace,
        thread,
        turn,
        unit,
        vec![round.assistant_source],
    ));
    let mut messages = vec![assistant];
    for (call, identity) in calls.iter().zip(&round.envelope.calls) {
        if let Some((source, message)) = round.results.get(&identity.turn_item_id) {
            ensure!(
                message.role == Role::Tool
                    && message.tool_call_id.as_deref() == Some(call.id.as_str())
                    && message.name.as_deref() == Some(call.name.as_str()),
                "canonical result identity mismatch"
            );
            let mut message = message.clone();
            message.provenance = Some(origin(workspace, thread, turn, unit, vec![source.clone()]));
            messages.push(message);
        }
    }
    if !complete {
        // Preserve actual interrupted work as observations, without fabricating
        // a terminal tool result or asking the runtime to execute anything.
        for message in &mut messages {
            let provenance = message.provenance.take();
            let observed = serde_json::to_string(message)?;
            *message = ChatMessage::user(format!(
                "Interrupted canonical round; some tool outcomes are unknown. Historical observation, not a new call:\n{observed}"
            ));
            message.provenance = provenance;
        }
    }
    output.push((round.sequence, messages));
    Ok(())
}

/// Read this line's own work. Inherited TaskRun bases and accepted deliveries
/// are composed by the caller through their own persisted source references.
#[cfg(test)]
pub(crate) async fn load_line_history(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    excluded_turn: Option<&str>,
    fence: &HistoryReadFence,
) -> Result<Vec<ChatMessage>> {
    load_line_history_inner(
        store,
        workspace,
        thread,
        excluded_turn,
        fence,
        false,
        HistorySelection::All,
    )
    .await
}

pub(crate) async fn load_task_line_history(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    excluded_turn: Option<&str>,
    fence: &HistoryReadFence,
) -> Result<Vec<ChatMessage>> {
    load_line_history_inner(
        store,
        workspace,
        thread,
        excluded_turn,
        fence,
        true,
        HistorySelection::All,
    )
    .await
}

/// Load only the canonical suffix not already represented by an accepted
/// frozen Task basis. The boundary turn itself is included because Composer
/// snapshots deliberately exclude their current input turn.
pub(crate) async fn load_task_line_history_from(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    excluded_turn: Option<&str>,
    from_turn: &str,
    fence: &HistoryReadFence,
) -> Result<Vec<ChatMessage>> {
    load_line_history_inner(
        store,
        workspace,
        thread,
        excluded_turn,
        fence,
        true,
        HistorySelection::FromTurn(from_turn),
    )
    .await
}

/// Reconstruct only the supplied exact canonical leaves. Later unrelated rows
/// may contribute metadata to discovery, but their payloads are never decoded.
pub(crate) async fn load_exact_line_history(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    fence: &HistoryReadFence,
    selected: &BTreeSet<SourceRef>,
) -> Result<Vec<ChatMessage>> {
    load_line_history_inner(
        store,
        workspace,
        thread,
        None,
        fence,
        false,
        HistorySelection::Sources(selected),
    )
    .await
}

/// Freeze only work physically produced by this child through the completed
/// result turn. Later turns are filtered before any payload is loaded. Inherited
/// TaskRun context is deliberately not a delivered contribution.
pub(crate) async fn load_task_output_history(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    through_turn: &str,
    fence: &HistoryReadFence,
) -> Result<Vec<ChatMessage>> {
    load_line_history_inner(
        store,
        workspace,
        thread,
        None,
        fence,
        false,
        HistorySelection::ThroughTurn(through_turn),
    )
    .await
}

enum HistorySelection<'a> {
    All,
    Sources(&'a BTreeSet<SourceRef>),
    ThroughTurn(&'a str),
    FromTurn(&'a str),
}

async fn load_line_history_inner(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    excluded_turn: Option<&str>,
    fence: &HistoryReadFence,
    causal_task_context: bool,
    selection: HistorySelection<'_>,
) -> Result<Vec<ChatMessage>> {
    let (selected, through_turn, from_turn) = match selection {
        HistorySelection::All => (None, None, None),
        HistorySelection::Sources(sources) => (Some(sources), None, None),
        HistorySelection::ThroughTurn(turn) => (None, Some(turn), None),
        HistorySelection::FromTurn(turn) => (None, None, Some(turn)),
    };
    let store = store.with_maintenance_access();
    let mut turns = Vec::new();
    let mut after = String::new();
    loop {
        let page = store
            .compaction_history_turn_page(workspace, thread, &after, fence)
            .await?;
        let Some(last) = page.last() else { break };
        ensure!(last.id > after, "history turn page made no progress");
        after = last.id.clone();
        turns.extend(page);
    }
    // IDs do not encode chronology. In particular, a detached Task answer may
    // share its parent's creation second and sort before its source request.
    turns.sort_by(|a, b| {
        (
            &a.created_at,
            a.creation_order,
            a.legacy_creation_order,
            &a.id,
        )
            .cmp(&(
                &b.created_at,
                b.creation_order,
                b.legacy_creation_order,
                &b.id,
            ))
    });
    if let Some(through_turn) = through_turn {
        let end = turns
            .iter()
            .position(|turn| turn.id == through_turn)
            .ok_or_else(|| anyhow::anyhow!("completed output turn is outside its history fence"))?;
        ensure!(
            turns[end].status == "completed",
            "output turn is not completed"
        );
        turns.truncate(end + 1);
    }
    if let Some(from_turn) = from_turn {
        let start = turns
            .iter()
            .position(|turn| turn.id == from_turn)
            .ok_or_else(|| anyhow::anyhow!("accepted basis turn is outside its history fence"))?;
        turns.drain(..start);
    }
    turns.retain(|turn| Some(turn.id.as_str()) != excluded_turn);
    // Refresh relationship metadata before deciding whether earlier Task
    // commands are closed by later occurrence/delivery turns. A cold cache
    // must not drop a command merely because its outcome sorts after it.
    if let Some(selected) = selected {
        let source_turns = selected
            .iter()
            .filter_map(|source| source.scope.split_once(':').map(|(_, turn)| turn))
            .collect::<BTreeSet<_>>();
        turns.retain(|turn| source_turns.contains(turn.id.as_str()));
    }
    let mut events_by_turn = Vec::with_capacity(turns.len());
    for turn in &turns {
        let mut events = metadata(
            &store,
            workspace,
            thread,
            &turn.id,
            PagedSource::Event,
            turn.event_high_water,
            fence,
        )
        .await?;
        events.retain(|event| selected.is_none_or(|sources| sources.contains(&event.reference)));
        for event in &mut events {
            if event.projection_kind.is_none() {
                let payload = source_payload(&store, workspace, thread, event).await?;
                let parsed: Event = serde_json::from_str(&payload)?;
                ensure!(
                    store
                        .compaction_record_event_projection(
                            workspace,
                            thread,
                            &event.reference,
                            &parsed
                        )
                        .await?,
                    "canonical event changed while refreshing history metadata"
                );
                let (item, kind) = pioneer_crud::compaction::event_projection_metadata(&parsed);
                event.item_id = item;
                event.projection_kind = Some(kind.into());
                event.payload = Some(payload);
            }
        }
        events_by_turn.push(events);
    }
    let mut history = Vec::new();
    for (turn, events) in turns.into_iter().zip(events_by_turn) {
        // A later terminal transition cannot expose the pending portion of an
        // active parent's already captured snapshot. Use only events below its
        // fence; mutable turn.status is not a historical boundary.
        let captured_active = events
            .iter()
            .filter_map(|event| {
                use pioneer_protocol::constants::events as kinds;
                match event.source_type.as_str() {
                    kinds::TURN_COMPLETED | kinds::TURN_FAILED | kinds::TURN_BLOCKED => Some(false),
                    kinds::TURN_STARTED
                    | kinds::TURN_EXECUTION_WINDOW_STARTED
                    | kinds::TURN_EXECUTION_WINDOW_CONTINUED => Some(true),
                    kinds::ITEM_STARTED if event.projection_kind.as_deref() == Some("start") => {
                        Some(true)
                    }
                    _ => None,
                }
            })
            .next_back()
            .unwrap_or(true);
        let failure_sequences = events
            .iter()
            .filter(|row| {
                matches!(
                    row.source_type.as_str(),
                    pioneer_protocol::constants::events::TURN_FAILED
                        | pioneer_protocol::constants::events::TURN_BLOCKED
                )
            })
            .map(|row| row.sequence)
            .collect::<BTreeSet<_>>();
        let delivery_outcome_sequences = events
            .iter()
            .filter(|row| {
                row.source_type == pioneer_protocol::constants::events::ITEM_COMPLETED
                    && row
                        .item_id
                        .as_deref()
                        .and_then(pioneer_protocol::task_delivery_id_from_result_item_id)
                        .is_some()
            })
            .map(|row| row.sequence)
            .collect::<BTreeSet<_>>();
        let starts = events
            .iter()
            .filter(|row| row.projection_kind.as_deref() == Some("start"))
            .filter_map(|row| row.item_id.as_ref().map(|id| (id.clone(), row.sequence)))
            .collect::<BTreeMap<_, _>>();
        let mut contexts = metadata(
            &store,
            workspace,
            thread,
            &turn.id,
            PagedSource::ProviderContext,
            turn.context_high_water,
            fence,
        )
        .await?;
        if let Some(selected) = selected {
            let mut exact = Vec::new();
            for row in contexts {
                let keep = if selected.contains(&row.reference) {
                    true
                } else if row.source_type == "tool_result_v2" {
                    if let Some(item) = row.item_id.as_deref() {
                        store
                            .compaction_tool_item_reference(workspace, thread, &turn.id, item)
                            .await?
                            .is_some_and(|reference| selected.contains(&reference))
                    } else {
                        false
                    }
                } else {
                    false
                };
                if keep {
                    exact.push(row);
                }
            }
            contexts = exact;
        }
        let mut aliases = contexts
            .iter()
            .filter_map(|row| row.item_id.clone())
            .collect::<BTreeSet<_>>();
        let mut ordered = Vec::<(i64, Vec<ChatMessage>)>::new();
        let has_event_input = events.iter().any(|row| {
            matches!(
                row.projection_kind.as_deref(),
                Some("input" | "input_revision" | "input_deleted")
            )
        });
        let use_input_rows = if let Some(selected) = selected {
            selected
                .iter()
                .any(|source| source.scope == format!("input:{}", turn.id))
        } else {
            turn.input_high_water > 0
                && (matches!(turn.send_mode.as_deref(), Some("agent" | "chat")) || !has_event_input)
        };
        if use_input_rows {
            let rows = metadata(
                &store,
                workspace,
                thread,
                &turn.id,
                PagedSource::Input,
                turn.input_high_water,
                fence,
            )
            .await?;
            let mut inputs = Vec::new();
            let mut sources = Vec::new();
            for mut row in rows {
                if selected.is_some_and(|sources| !sources.contains(&row.reference)) {
                    continue;
                }
                inputs.push(serde_json::from_str::<pioneer_protocol::UserInput>(
                    &source_payload(&store, workspace, thread, &mut row).await?,
                )?);
                sources.push(row.reference);
            }
            if !inputs.is_empty() {
                let mut message = input_message(&inputs)?;
                message.provenance =
                    Some(origin(workspace, thread, &turn.id, "user-input", sources));
                ordered.push((0, vec![message]));
            }
        }
        let mut pending: Option<Round> = None;
        for mut row in contexts {
            let payload = source_payload(&store, workspace, thread, &mut row).await?;
            match row.source_type.as_str() {
                "assistant_round" => {
                    if let Some(round) = pending.take() {
                        finish_round(
                            workspace,
                            thread,
                            &turn.id,
                            round,
                            captured_active,
                            &mut ordered,
                        )?;
                    }
                    if let Ok(envelope) =
                        serde_json::from_str::<CanonicalProviderRoundEnvelope>(&payload)
                    {
                        aliases.extend(envelope.calls.iter().map(|call| call.turn_item_id.clone()));
                        pending = Some(Round {
                            sequence: starts
                                .get(&envelope.round_id)
                                .copied()
                                .unwrap_or(row.sequence),
                            envelope,
                            assistant_source: row.reference,
                            results: BTreeMap::new(),
                        });
                    } else {
                        let mut message = ChatMessage::user(format!(
                            "Legacy provider observation (available original):\n{payload}"
                        ));
                        message.provenance = Some(origin(
                            workspace,
                            thread,
                            &turn.id,
                            &row.reference.id,
                            vec![row.reference.clone()],
                        ));
                        ordered.push((row.sequence, vec![message]));
                    }
                }
                "provider_observation" => {
                    let mut message = provider_observation(&payload)?;
                    message.provenance = Some(origin(
                        workspace,
                        thread,
                        &turn.id,
                        &row.reference.id,
                        vec![row.reference.clone()],
                    ));
                    let order = row
                        .item_id
                        .as_ref()
                        .and_then(|id| starts.get(id))
                        .copied()
                        .unwrap_or(row.sequence);
                    ordered.push((order, vec![message]));
                }
                "tool_result_v2" => {
                    let round = pending
                        .as_mut()
                        .ok_or_else(|| anyhow::anyhow!("canonical tool result has no round"))?;
                    let item = row.item_id.ok_or_else(|| {
                        anyhow::anyhow!("tool result has no durable item identity")
                    })?;
                    let view: pioneer_tools::ToolResultView = serde_json::from_str(&payload)?;
                    let pioneer_tools::ToolResultView::Json {
                        value,
                        truncated: false,
                    } = view
                    else {
                        anyhow::bail!("canonical result is incomplete")
                    };
                    let full: ChatMessage = serde_json::from_value(value)?;
                    let reference = serde_json::json!({"workspace_id":workspace,"thread_id":thread,"turn_id":turn.id,"item_id":item}).to_string();
                    let message =
                        pioneer_agent::compaction::restored_tool_result_message(&full, &reference)?;
                    let source = store
                        .compaction_tool_item_reference(workspace, thread, &turn.id, &item)
                        .await?
                        .unwrap_or(row.reference);
                    ensure!(
                        round.results.insert(item, (source, message)).is_none(),
                        "duplicate canonical tool result"
                    );
                }
                _ => {
                    let mut message = ChatMessage::user(format!(
                        "Legacy provider observation; outcome is not inferred:\n{payload}"
                    ));
                    message.provenance = Some(origin(
                        workspace,
                        thread,
                        &turn.id,
                        &row.reference.id,
                        vec![row.reference.clone()],
                    ));
                    ordered.push((row.sequence, vec![message]));
                }
            }
        }
        if let Some(round) = pending {
            finish_round(
                workspace,
                thread,
                &turn.id,
                round,
                captured_active,
                &mut ordered,
            )?;
        }
        let latest_input = events
            .iter()
            .filter(|row| {
                matches!(
                    row.projection_kind.as_deref(),
                    Some("input" | "input_revision" | "input_deleted")
                )
            })
            .map(|row| row.sequence)
            .max();
        let last_input_revision = events
            .iter()
            .filter(|row| {
                matches!(
                    row.projection_kind.as_deref(),
                    Some("input_revision" | "input_deleted")
                )
            })
            .map(|row| row.sequence)
            .max();
        let mut last_attachment_copies = BTreeMap::<String, i64>::new();
        for row in &events {
            if row.projection_kind.as_deref() == Some("input_copy")
                && let Some(item) = &row.item_id
            {
                last_attachment_copies
                    .entry(item.clone())
                    .and_modify(|sequence| *sequence = (*sequence).max(row.sequence))
                    .or_insert(row.sequence);
            }
        }
        for mut row in events {
            let kind = row.projection_kind.as_deref().unwrap_or("observation");
            if kind == "input_copy"
                && (last_input_revision.is_some_and(|revision| row.sequence <= revision)
                    || row.item_id.as_ref().is_some_and(|item| {
                        last_attachment_copies.get(item) != Some(&row.sequence)
                    }))
            {
                continue;
            }
            if matches!(kind, "start" | "technical")
                || row.item_id.as_ref().is_some_and(|id| aliases.contains(id))
            {
                continue;
            }
            if matches!(kind, "input" | "input_revision" | "input_deleted")
                && (use_input_rows || Some(row.sequence) != latest_input)
            {
                continue;
            }
            let payload = source_payload(&store, workspace, thread, &mut row).await?;
            let event: Event = serde_json::from_str(&payload)?;
            ensure!(
                event.workspace_id() == workspace
                    && event.thread_id() == thread
                    && event.turn_id() == turn.id,
                "canonical event scope mismatch"
            );
            let Some(mut message) = event_message(event)? else {
                continue;
            };
            // Only delivery results and terminal Task outcomes can map a
            // physical event to another logical command. Resolving every
            // historical event performed a pair of relationship queries for
            // tens of thousands of ordinary messages before each turn start.
            let task_outcome = (row.source_type
                == pioneer_protocol::constants::events::ITEM_COMPLETED
                && row
                    .item_id
                    .as_deref()
                    .and_then(pioneer_protocol::task_delivery_id_from_result_item_id)
                    .is_some())
                || matches!(
                    row.source_type.as_str(),
                    pioneer_protocol::constants::events::TURN_FAILED
                        | pioneer_protocol::constants::events::TURN_BLOCKED
                );
            let logical_turn_id = if task_outcome {
                store
                    .compaction_task_delivery_command(workspace, thread, &row.reference)
                    .await?
            } else {
                None
            };
            let mut provenance = origin(
                workspace,
                thread,
                &turn.id,
                &row.reference.id,
                vec![row.reference.clone()],
            );
            provenance.logical_turn_id = logical_turn_id;
            message.provenance = Some(provenance);
            ordered.push((row.sequence, vec![message]));
        }
        if causal_task_context {
            let boundary = store
                .compaction_history_causal_boundary(workspace, thread, &turn.id, fence)
                .await?;
            let completed_work = ordered
                .iter()
                .flat_map(|(_, messages)| messages)
                .any(|message| matches!(message.role, Role::Assistant | Role::Tool));
            if boundary.delegated_command
                && !completed_work
                && failure_sequences.is_empty()
                && !boundary.delivered_outcome
            {
                // An unfinished sibling command is not the next Task's input.
                continue;
            }
            if boundary.task_transport {
                // Task cards/occurrence inputs are UI transport copies. Keep
                // the acknowledged outcome and actual failures, with their own
                // source IDs, instead of copying the command from the card.
                for (sequence, messages) in &mut ordered {
                    if !failure_sequences.contains(sequence)
                        && !delivery_outcome_sequences.contains(sequence)
                    {
                        messages
                            .retain(|message| matches!(message.role, Role::Assistant | Role::Tool));
                    }
                }
            }
        }
        ordered.sort_by_key(|(sequence, _)| *sequence);
        history.extend(ordered.into_iter().flat_map(|(_, messages)| messages));
    }
    Ok(history)
}

/// Failed partial output remains historical data, including opaque provider
/// fields and incomplete tool-call text. It can never instruct tool replay.
pub(crate) fn provider_observation(payload: &str) -> Result<ChatMessage> {
    let envelope: CanonicalProviderRoundEnvelope = serde_json::from_str(payload)?;
    ensure!(
        envelope.version == 1
            && !envelope.round_id.is_empty()
            && envelope.termination == pioneer_provider::ProviderTermination::ProviderError
            && envelope.message.role == Role::Assistant
            && envelope.calls.is_empty(),
        "invalid failed provider observation"
    );
    Ok(ChatMessage::user(format!(
        "Unsuccessful provider response (historical data, not a completed answer or executable tool call):\n{}",
        serde_json::to_string(&envelope.message)?,
    )))
}

/// The same canonical renderer is used by cold loading and frozen references.
pub(crate) fn event_message(event: Event) -> Result<Option<ChatMessage>> {
    Ok(Some(match event {
        Event::TurnStarted(value) if !value.input.is_empty() => input_message(&value.input)?,
        Event::TurnMessageEdited(value) if !value.input.is_empty() => input_message(&value.input)?,
        Event::TurnStarted(_) | Event::TurnMessageEdited(_) => return Ok(None),
        Event::TurnMessageDeleted(_) => return Ok(None),
        Event::ItemCompleted(value) => match value.item {
            pioneer_protocol::TurnItem::UserMessage { attachments, .. } => {
                if attachments.is_empty() {
                    return Ok(None);
                }
                // Keep the resolved version recorded with the original message.
                // Do not look up today's current artifact or repeat the user text.
                ChatMessage::user(format!(
                    "Historical attachment references (metadata only; content is not reattached):\n{}",
                    serde_json::to_string(&attachments)?,
                ))
            }
            pioneer_protocol::TurnItem::AgentMessage { text, .. } => ChatMessage::assistant(text),
            pioneer_protocol::TurnItem::Reasoning {
                summary, content, ..
            } => ChatMessage::assistant(format!(
                "Reasoning recorded for a previous response:\n{}",
                content
                    .into_iter()
                    .chain(summary)
                    .collect::<Vec<_>>()
                    .join("\n")
            )),
            item => ChatMessage::user(format!(
                "Recorded historical event:\n{}",
                serde_json::to_string(&item)?
            )),
        },
        Event::TurnCompleted(value) => {
            ChatMessage::user(format!("Historical turn status: {:?}", value.turn.status))
        }
        Event::TurnFailed(value) => ChatMessage::user(format!(
            "Historical turn {:?}: {:?}",
            value.turn.status, value.turn.error
        )),
        Event::TurnBlocked(value) => {
            ChatMessage::user(format!("Historical turn blocked: {:?}", value.turn.error))
        }
        other => ChatMessage::user(format!(
            "Recorded historical status; not a successful model response:\n{}",
            serde_json::to_string(&other)?
        )),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn malformed_or_failed_round_cannot_become_completed_inherited_work() {
        let envelope = CanonicalProviderRoundEnvelope {
            version: 1,
            round_id: "round".into(),
            termination: pioneer_provider::ProviderTermination::ToolCalls,
            message: ChatMessage::assistant_tool_calls(
                None::<String>,
                vec![pioneer_provider::ProviderToolCall {
                    id: "call".into(),
                    name: "read_file".into(),
                    arguments: "{}".into(),
                }],
            ),
            calls: vec![pioneer_provider::ProviderCallIdentity {
                provider_call_id: "call".into(),
                turn_item_id: "item".into(),
                ordinal: 0,
            }],
        };
        for bad in 0..5 {
            let mut envelope = envelope.clone();
            match bad {
                0 => envelope.termination = pioneer_provider::ProviderTermination::ProviderError,
                1 => envelope.round_id.clear(),
                2 => envelope.message.tool_calls.as_mut().unwrap()[0]
                    .name
                    .clear(),
                3 => envelope.message.tool_calls.as_mut().unwrap()[0].arguments = "{".into(),
                _ => envelope.calls[0].turn_item_id.clear(),
            }
            let round = Round {
                sequence: 1,
                envelope,
                assistant_source: SourceRef {
                    scope: "context:turn".into(),
                    id: "source".into(),
                    version: "revision:1".into(),
                },
                results: BTreeMap::new(),
            };
            assert!(finish_round("ws", "thread", "turn", round, true, &mut Vec::new()).is_err());
        }
    }
}
