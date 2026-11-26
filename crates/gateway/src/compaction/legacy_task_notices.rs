use anyhow::Result;
use pioneer_agent::{
    ObservedTaskOutcome, RetainedProviderHistoryMessage, task_observation::save_observations,
};
use pioneer_crud::{CanonicalTurnEventPayload, CrudStore, compaction::PagedSource};
use pioneer_protocol::TurnItem;
use pioneer_protocol::constants::events::ITEM_COMPLETED;
use pioneer_provider::{
    ChatMessage, Role,
    tool_declarations::{ToolDeclarations, message_hash},
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

fn kind(message: &ChatMessage) -> Option<&str> {
    message.application_context.as_ref()?.get("kind")?.as_str()
}

/// The confirmed declaration proof binds the original wire order. An exact
/// current-input anchor lets recovery check its own suffix without knowing the
/// history supplied by earlier turns. Provider preflight still validates the
/// complete prefix (including that earlier history) before retaining positions.
pub(crate) async fn restore(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    retained: &mut Vec<RetainedProviderHistoryMessage>,
) -> Result<()> {
    let scope = json!([workspace, thread, turn]);
    let Some(declarations) = retained
        .iter()
        .filter(|entry| kind(&entry.message) == Some("system_snapshot"))
        .filter_map(|entry| {
            let metadata = entry.message.application_context.as_ref()?;
            (metadata["tool_declarations_confirmed"] == true
                && metadata["tool_disclosure_scope"] == scope)
                .then_some((entry.sequence, metadata))
        })
        .max_by_key(|(sequence, _)| *sequence)
        .and_then(|(_, metadata)| ToolDeclarations::from_metadata(metadata))
    else {
        return Ok(());
    };
    let Some(prefix) = declarations.confirmed_message_prefix() else {
        return Ok(());
    };
    let mut inputs = retained
        .iter()
        .filter(|entry| kind(&entry.message) == Some("turn_input"));
    let Some(input) = inputs.next() else {
        return Ok(());
    };
    if inputs.next().is_some() || input.message.provenance.is_none() {
        return Ok(());
    }
    let input_hash = message_hash(&input.message)?;
    let mut anchors = prefix
        .iter()
        .enumerate()
        .filter(|(_, hash)| *hash == &input_hash);
    let Some((start, _)) = anchors.next() else {
        return Ok(());
    };
    if anchors.next().is_some() {
        return Ok(());
    }
    let expected = &prefix[start..];

    let mut wire_entries = retained
        .iter()
        .enumerate()
        .filter(|(_, entry)| kind(&entry.message) != Some("system_snapshot"))
        .collect::<Vec<_>>();
    wire_entries.sort_by_key(|(_, entry)| entry.sequence);
    let mut messages = wire_entries
        .iter()
        .map(|(_, entry)| entry.message.clone())
        .collect::<Vec<_>>();
    // Use the same source ordering as native preflight for the known
    // messages. The legacy notices themselves are placed by hashes only.
    if super::origins::resolve_message_origin_locators(
        store,
        workspace,
        thread,
        turn,
        &BTreeSet::from([thread.to_owned()]),
        &mut messages,
    )
    .await
    .is_err()
    {
        return Ok(());
    }
    super::frozen::order_history_by_creation(store, workspace, &mut messages).await?;
    let hashes = messages
        .iter()
        .map(message_hash)
        .collect::<Result<Vec<_>>>()?;
    if hashes.first() != Some(&input_hash) {
        return Ok(());
    }

    let known = hashes.iter().collect::<HashSet<_>>();
    let mut missing = HashSet::new();
    let mut next = 0;
    for hash in expected {
        if hashes.get(next) == Some(hash) {
            next += 1;
        } else if known.contains(hash) || !missing.insert(hash.clone()) {
            return Ok(()); // A move or repeated unknown slot is ambiguous.
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    let Some(mut notices) = read_notices(store, workspace, thread, turn, &missing).await? else {
        return Ok(());
    };
    // The ordinary preflight will repeat source validation on the assembled
    // request. Here a replaced current source invalidates this optional proof.
    if super::origins::validate_message_origins(store, workspace, &messages[..next])
        .await
        .is_err()
    {
        return Ok(());
    }
    let mut insertions = BTreeMap::<usize, Vec<_>>::new();
    let mut restored_hashes = Vec::with_capacity(expected.len());
    let mut next = 0;
    for hash in expected {
        if hashes.get(next) == Some(hash) {
            restored_hashes.push(hashes[next].clone());
            next += 1;
            continue;
        }
        // Source chronology orders the dated messages within their wire
        // slots. An undated notice can occupy the proven gap without moving
        // those slots or changing the durable disclosure/snapshot frontier.
        let (position, sequence) = if let Some(&(index, entry)) = wire_entries.get(next) {
            if entry.message.role == Role::Tool || messages[next].role == Role::Tool {
                return Ok(()); // A terminal notice cannot split a call/result group.
            }
            (index, entry.sequence)
        } else {
            let &(index, entry) = wire_entries.last().expect("input anchor was verified");
            (index + 1, entry.sequence)
        };
        let message = notices
            .remove(hash)
            .expect("every missing slot was verified");
        restored_hashes.push(message_hash(&message)?);
        insertions
            .entry(position)
            .or_default()
            .push(RetainedProviderHistoryMessage { sequence, message });
    }
    if restored_hashes.as_slice() != expected {
        return Ok(());
    }
    // Wire assembly and tool disclosure both use stable sequence order.
    // Preserve stored snapshots and entries beyond the confirmed prefix too.
    let originals = std::mem::take(retained);
    retained.reserve(originals.len() + missing.len());
    let end = originals.len();
    for (position, entry) in originals.into_iter().enumerate() {
        retained.extend(insertions.remove(&position).into_iter().flatten());
        retained.push(entry);
    }
    retained.extend(insertions.remove(&end).into_iter().flatten());
    Ok(())
}

async fn read_notices(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    missing: &HashSet<String>,
) -> Result<Option<HashMap<String, ChatMessage>>> {
    let fence = store
        .compaction_source_high_water(workspace, thread, turn, PagedSource::Event)
        .await?;
    let mut matches = HashMap::new();
    let mut after = 0;
    while after < fence {
        let page = store
            .compaction_source_metadata_page(workspace, thread, turn, PagedSource::Event, after)
            .await?;
        if page.next_sequence <= after {
            return Ok(None);
        }
        after = page.next_sequence;
        let mut rows = page
            .entries
            .into_iter()
            .take_while(|row| row.sequence <= fence)
            .filter(|row| {
                row.source_type == ITEM_COMPLETED
                    && row
                        .projection_kind
                        .as_deref()
                        .is_none_or(|kind| kind == "observation")
            })
            .collect::<VecDeque<_>>();
        while !rows.is_empty() {
            let batch =
                super::history::take_source_payload_batch(store, workspace, thread, &mut rows)
                    .await?;
            for (row, payload) in batch {
                let CanonicalTurnEventPayload::ItemCompleted(notification) =
                    serde_json::from_str(&payload)?
                else {
                    return Ok(None);
                };
                let Some((message, outcomes)) = terminal_notice(&notification.item) else {
                    continue;
                };
                let hash = message_hash(&message)?;
                if !missing.contains(&hash) {
                    continue;
                }
                let item_id = notification.item.item_id();
                if notification.workspace_id != workspace
                    || notification.thread_id != thread
                    || notification.turn_id != turn
                    || row.item_id.as_deref().is_some_and(|id| id != item_id)
                {
                    return Ok(None);
                }
                let candidate = (item_id.to_owned(), message.content, outcomes);
                match matches.entry(hash) {
                    std::collections::hash_map::Entry::Vacant(slot) => {
                        slot.insert(candidate);
                    }
                    std::collections::hash_map::Entry::Occupied(slot)
                        if slot.get() != &candidate =>
                    {
                        return Ok(None);
                    }
                    std::collections::hash_map::Entry::Occupied(_) => {}
                }
            }
        }
    }
    // Do not stop at the first matching completion. Distinct durable items
    // with the same hash leave no proof of which outcome was sent.
    if matches.len() != missing.len() {
        return Ok(None);
    }
    let mut found = HashMap::with_capacity(matches.len());
    for (hash, (item_id, content, outcomes)) in matches {
        let Some(source) = store
            .compaction_item_reference(workspace, thread, turn, &item_id)
            .await?
        else {
            return Ok(None);
        };
        let Some(payload) = store
            .compaction_reference_payload(workspace, thread, &source)
            .await?
        else {
            return Ok(None);
        };
        let exact_item: TurnItem = serde_json::from_str(&payload)?;
        if exact_item.item_id() != item_id {
            return Ok(None);
        }
        let Some((mut exact, exact_outcomes)) = terminal_notice(&exact_item) else {
            return Ok(None);
        };
        if exact.content != content || exact_outcomes != outcomes {
            return Ok(None);
        }
        save_observations(&mut exact, &outcomes);
        found.insert(hash, exact);
    }
    Ok(Some(found))
}

fn terminal_notice(item: &TurnItem) -> Option<(ChatMessage, BTreeSet<ObservedTaskOutcome>)> {
    let TurnItem::SystemEvent {
        message,
        code: Some(code),
        details: Some(details),
        ..
    } = item
    else {
        return None;
    };
    if code != "task.terminal.observed" {
        return None;
    }
    let observations: Vec<Value> = serde_json::from_str(
        message
            .strip_prefix("Attached task results are available:\n")?
            .strip_suffix("\nUse these TaskRun results/errors in the next response. Full details are available with task_get.")?,
    ).ok()?;
    let outcomes = observations
        .iter()
        .map(|value| {
            Some(ObservedTaskOutcome::TaskRevision {
                task_id: value.get("taskId")?.as_str()?.to_owned(),
                revision: value.get("taskRevision")?.as_i64()?,
            })
        })
        .collect::<Option<BTreeSet<_>>>()?;
    let stored =
        serde_json::from_value::<BTreeSet<ObservedTaskOutcome>>(details.get("outcomes")?.clone())
            .ok()?;
    (!outcomes.is_empty() && outcomes == stored)
        .then(|| (ChatMessage::user(message.clone()), outcomes))
}
