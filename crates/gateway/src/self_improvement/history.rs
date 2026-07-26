use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use pioneer_crud::{
    CanonicalTurnEventPayload, CanonicalTurnEventRecord, SelfImprovementFrozenSourceRange,
};
use pioneer_protocol::{
    AgentMessagePhase, ToolCallStatus, ToolStoragePayload, TurnItem, TurnPermissionAuditEventKind,
    TurnStatus, UserInput, UserMessageAttachment,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

pub(crate) const CHUNK_ANALYSIS_MAX_REQUEST_INPUT_BYTES: usize = 128 * 1024;
pub(crate) const CHUNK_ANALYSIS_MAX_TOKEN_UPPER_BOUND: usize = 128 * 1024;
pub(crate) const CHUNK_ANALYSIS_NON_HISTORY_RESERVE_BYTES: usize = 64 * 1024;
pub(crate) const CHUNK_ANALYSIS_NON_HISTORY_TOKEN_RESERVE: usize = 64 * 1024;
pub(crate) const HISTORY_CHUNK_MAX_SERIALIZED_BYTES: usize =
    CHUNK_ANALYSIS_MAX_REQUEST_INPUT_BYTES - CHUNK_ANALYSIS_NON_HISTORY_RESERVE_BYTES;
pub(crate) const HISTORY_CHUNK_MAX_TOKEN_UPPER_BOUND: usize =
    CHUNK_ANALYSIS_MAX_TOKEN_UPPER_BOUND - CHUNK_ANALYSIS_NON_HISTORY_TOKEN_RESERVE;
const HISTORY_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HistoryChunkLimits {
    pub max_serialized_bytes: usize,
    pub max_token_upper_bound: usize,
}

impl Default for HistoryChunkLimits {
    fn default() -> Self {
        Self {
            max_serialized_bytes: HISTORY_CHUNK_MAX_SERIALIZED_BYTES,
            max_token_upper_bound: HISTORY_CHUNK_MAX_TOKEN_UPPER_BOUND,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HistoryEvidenceRole {
    NewAnchor,
    ContextOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HistoryToolStage {
    Started,
    Completed,
    Updated,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub(crate) enum SelfImprovementHistoryContent {
    UserText {
        text: String,
    },
    Attachment {
        attachment_kind: String,
        metadata: Value,
    },
    AssistantMessage {
        phase: String,
        text: String,
    },
    Tool {
        stage: HistoryToolStage,
        item_id: String,
        tool_name: String,
        status: String,
        arguments: Value,
        stored_result: Value,
        metadata: Value,
    },
    PermissionOutcome {
        event_kind: String,
        action_kind: Option<String>,
        tool_name: Option<String>,
        decision: Option<String>,
        reason: Option<String>,
    },
    Terminal {
        status: String,
        error: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SelfImprovementHistoryBlock {
    pub block_key: String,
    pub event_id: String,
    pub event_thread_id: String,
    pub event_turn_id: String,
    pub sequence: i64,
    pub input_index: Option<usize>,
    pub fragment_index: u32,
    pub fragment_count: u32,
    pub evidence_role: HistoryEvidenceRole,
    pub content: SelfImprovementHistoryContent,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SelfImprovementHistoryTurn {
    pub turn_id: String,
    pub blocks: Vec<SelfImprovementHistoryBlock>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SelfImprovementHistoryThread {
    pub thread_id: String,
    pub turns: Vec<SelfImprovementHistoryTurn>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SelfImprovementHistorySnapshot {
    pub schema_version: u32,
    pub workspace_id: String,
    pub source_lower_exclusive: i64,
    pub source_upper_inclusive: i64,
    pub threads: Vec<SelfImprovementHistoryThread>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SelfImprovementHistoryChunk {
    pub schema_version: u32,
    pub workspace_id: String,
    pub source_lower_exclusive: i64,
    pub source_upper_inclusive: i64,
    pub chunk_index: u32,
    pub chunk_count: u32,
    pub threads: Vec<SelfImprovementHistoryThread>,
    pub fingerprint: String,
}

#[derive(Debug, Serialize)]
struct FingerprintInput<'a> {
    schema_version: u32,
    workspace_id: &'a str,
    source_lower_exclusive: i64,
    source_upper_inclusive: i64,
    chunk_index: u32,
    chunk_count: u32,
    threads: &'a [SelfImprovementHistoryThread],
}

pub(crate) fn build_model_safe_full_thread_snapshot(
    frozen_range: &SelfImprovementFrozenSourceRange,
    records: &[CanonicalTurnEventRecord],
) -> Result<SelfImprovementHistorySnapshot> {
    frozen_range.validate()?;
    let workspace_id = frozen_range.workspace_id.as_str();
    let source_lower_exclusive = frozen_range.source_lower_exclusive;
    let source_upper_inclusive = frozen_range.source_upper_inclusive;
    let mut selected_parent_turns = HashSet::new();
    let mut selected_thread_ids = HashSet::new();
    let mut selected_terminal_events = HashMap::new();
    for source in &frozen_range.anchors {
        selected_parent_turns.insert((source.thread_id.clone(), source.turn_id.clone()));
        selected_thread_ids.insert(source.thread_id.clone());
        selected_terminal_events.insert(
            source.terminal_event_id.clone(),
            (
                source.thread_id.clone(),
                source.turn_id.clone(),
                source.task_delivery_id.clone(),
            ),
        );
    }
    let terminal_boundaries = frozen_range
        .thread_terminal_boundaries
        .iter()
        .map(|boundary| {
            (
                boundary.thread_id.clone(),
                boundary.terminal_event_id.clone(),
            )
        })
        .collect::<HashMap<_, _>>();

    let mut threads = Vec::<SelfImprovementHistoryThread>::new();
    let mut thread_indexes = HashMap::<String, usize>::new();
    let mut last_sequence_by_turn = HashMap::<(String, String), i64>::new();
    let mut seen_event_ids = HashSet::new();
    let mut closed_threads = HashSet::<String>::new();
    let mut threads_at_terminal_boundary = HashSet::<String>::new();
    let mut represented_anchor_terminals = HashSet::<String>::new();
    let mut current_exchange: Option<LogicalHistoryExchange> = None;

    for record in records {
        if record.payload.workspace_id() != workspace_id
            || record.payload.thread_id() != record.thread_id
            || record.payload.turn_id() != record.turn_id
        {
            bail!(
                "canonical history record `{}` has mismatched payload identity",
                record.event_id
            );
        }
        if !seen_event_ids.insert(record.event_id.as_str()) {
            bail!(
                "canonical history record `{}` appears more than once",
                record.event_id
            );
        }
        let turn_key = (record.thread_id.clone(), record.turn_id.clone());
        if last_sequence_by_turn
            .insert(turn_key.clone(), record.sequence)
            .is_some_and(|previous| previous >= record.sequence)
        {
            bail!(
                "canonical history events for turn `{}` are not strictly ordered",
                record.turn_id
            );
        }
        if closed_threads.contains(record.thread_id.as_str()) {
            bail!(
                "canonical history record `{}` appears after the frozen terminal boundary for thread `{}`",
                record.event_id,
                record.thread_id
            );
        }

        if let CanonicalTurnEventPayload::TurnStarted(payload) = &record.payload
            && payload.turn.turn_kind == pioneer_protocol::TurnKind::Conversation
            && payload.turn.origin == pioneer_protocol::TurnOrigin::User
            && selected_thread_ids.contains(payload.thread.id.as_str())
        {
            if current_exchange.is_some() {
                bail!(
                    "canonical history started exchange `{}` before the previous exchange closed",
                    payload.turn.id
                );
            }
            if closed_threads.contains(payload.thread.id.as_str()) {
                bail!(
                    "canonical history exchange `{}` appears after its frozen thread boundary",
                    payload.turn.id
                );
            }
            if !matches!(
                payload.thread.origin_kind,
                pioneer_protocol::ThreadOriginKind::Collaborative
                    | pioneer_protocol::ThreadOriginKind::DirectMessage
                    | pioneer_protocol::ThreadOriginKind::User
            ) {
                bail!(
                    "canonical history exchange `{}` is not user-visible",
                    payload.turn.id
                );
            }
            current_exchange = Some(LogicalHistoryExchange {
                conversation_thread_id: payload.thread.id.clone(),
                parent_turn_id: payload.turn.id.clone(),
                origin_kind: payload.thread.origin_kind,
                blocks: Vec::new(),
            });
        }

        let exchange = current_exchange.as_mut().with_context(|| {
            format!(
                "canonical history record `{}` is outside a logical conversation exchange",
                record.event_id
            )
        })?;
        let evidence_role = if selected_parent_turns.contains(&(
            exchange.conversation_thread_id.clone(),
            exchange.parent_turn_id.clone(),
        )) {
            HistoryEvidenceRole::NewAnchor
        } else {
            HistoryEvidenceRole::ContextOnly
        };
        let collaborative_admission_completion = exchange.origin_kind
            == pioneer_protocol::ThreadOriginKind::Collaborative
            && record.thread_id == exchange.conversation_thread_id
            && record.turn_id == exchange.parent_turn_id
            && matches!(
                &record.payload,
                CanonicalTurnEventPayload::TurnCompleted(notification)
                    if notification.turn.status == TurnStatus::Completed
            );
        let collaborative_delivery_failure = exchange.origin_kind
            == pioneer_protocol::ThreadOriginKind::Collaborative
            && record.thread_id == exchange.conversation_thread_id
            && matches!(
                &record.payload,
                CanonicalTurnEventPayload::ItemCompleted(notification)
                    if matches!(
                        &notification.item,
                        TurnItem::SystemEvent {
                            level: pioneer_protocol::SystemEventLevel::Error,
                            ..
                        }
                    )
                        && completed_task_delivery_id(&record.payload).is_some()
            );
        let blocks = if collaborative_admission_completion {
            Vec::new()
        } else if collaborative_delivery_failure {
            let CanonicalTurnEventPayload::ItemCompleted(notification) = &record.payload else {
                unreachable!("Collaborative delivery failure was matched above");
            };
            let TurnItem::SystemEvent { code, .. } = &notification.item else {
                unreachable!("Collaborative delivery failure must be a SystemEvent");
            };
            vec![block(
                record,
                None,
                evidence_role,
                SelfImprovementHistoryContent::Terminal {
                    status: "failed".to_owned(),
                    error: Some(
                        code.clone()
                            .unwrap_or_else(|| "task_delivery_failed".to_owned()),
                    ),
                },
            )]
        } else {
            map_record(record, evidence_role)?
        };
        exchange.blocks.extend(blocks);

        if let Some((expected_thread_id, expected_turn_id, expected_delivery_id)) =
            selected_terminal_events.get(record.event_id.as_str())
        {
            let foreground_terminal = expected_delivery_id.is_none()
                && matches!(
                    &record.payload,
                    CanonicalTurnEventPayload::TurnCompleted(notification)
                        if notification.turn.status == TurnStatus::Completed
                );
            let collaborative_terminal =
                expected_delivery_id.as_deref().is_some_and(|delivery_id| {
                    completed_task_delivery_id(&record.payload) == Some(delivery_id)
                });
            if exchange.conversation_thread_id != *expected_thread_id
                || exchange.parent_turn_id != *expected_turn_id
                || (!foreground_terminal && !collaborative_terminal)
            {
                bail!(
                    "canonical history selected anchor terminal `{}` is invalid",
                    record.event_id
                );
            }
            represented_anchor_terminals.insert(record.event_id.clone());
        }

        let exchange_closed = match exchange.origin_kind {
            pioneer_protocol::ThreadOriginKind::DirectMessage
            | pioneer_protocol::ThreadOriginKind::User => {
                record.thread_id == exchange.conversation_thread_id
                    && record.turn_id == exchange.parent_turn_id
                    && matches!(
                        &record.payload,
                        CanonicalTurnEventPayload::TurnCompleted(_)
                            | CanonicalTurnEventPayload::TurnFailed(_)
                            | CanonicalTurnEventPayload::TurnBlocked(_)
                    )
            }
            pioneer_protocol::ThreadOriginKind::Collaborative => {
                record.thread_id == exchange.conversation_thread_id
                    && completed_task_delivery_id(&record.payload).is_some()
            }
            pioneer_protocol::ThreadOriginKind::TaskRun
            | pioneer_protocol::ThreadOriginKind::System => false,
        };
        if !exchange_closed {
            continue;
        }

        let exchange = current_exchange
            .take()
            .expect("logical history exchange exists while closing");
        if exchange.blocks.is_empty() {
            bail!(
                "canonical history exchange `{}` has no model-visible blocks",
                exchange.parent_turn_id
            );
        }
        let thread_index = match thread_indexes.get(exchange.conversation_thread_id.as_str()) {
            Some(index) => *index,
            None => {
                let index = threads.len();
                threads.push(SelfImprovementHistoryThread {
                    thread_id: exchange.conversation_thread_id.clone(),
                    turns: Vec::new(),
                });
                thread_indexes.insert(exchange.conversation_thread_id.clone(), index);
                index
            }
        };
        threads[thread_index]
            .turns
            .push(SelfImprovementHistoryTurn {
                turn_id: exchange.parent_turn_id.clone(),
                blocks: exchange.blocks,
            });
        if terminal_boundaries
            .get(exchange.conversation_thread_id.as_str())
            .is_some_and(|event_id| event_id == &record.event_id)
        {
            threads_at_terminal_boundary.insert(exchange.conversation_thread_id.clone());
            closed_threads.insert(exchange.conversation_thread_id);
        }
    }

    if let Some(exchange) = current_exchange {
        bail!(
            "canonical history exchange `{}` is incomplete",
            exchange.parent_turn_id
        );
    }
    if represented_anchor_terminals.len() != frozen_range.anchors.len()
        || threads_at_terminal_boundary.len() != terminal_boundaries.len()
    {
        bail!("canonical history did not represent every selected anchor terminal boundary");
    }

    Ok(SelfImprovementHistorySnapshot {
        schema_version: HISTORY_SCHEMA_VERSION,
        workspace_id: workspace_id.to_owned(),
        source_lower_exclusive,
        source_upper_inclusive,
        threads,
    })
}

struct LogicalHistoryExchange {
    conversation_thread_id: String,
    parent_turn_id: String,
    origin_kind: pioneer_protocol::ThreadOriginKind,
    blocks: Vec<SelfImprovementHistoryBlock>,
}

fn completed_task_delivery_id(payload: &CanonicalTurnEventPayload) -> Option<&str> {
    let CanonicalTurnEventPayload::ItemCompleted(notification) = payload else {
        return None;
    };
    pioneer_protocol::task_delivery_id_from_result_item_id(notification.item.item_id())
}

pub(crate) fn plan_history_chunks(
    snapshot: &SelfImprovementHistorySnapshot,
    limits: HistoryChunkLimits,
) -> Result<Vec<SelfImprovementHistoryChunk>> {
    if snapshot.workspace_id.trim().is_empty()
        || snapshot.source_lower_exclusive < 0
        || snapshot.source_upper_inclusive <= snapshot.source_lower_exclusive
        || snapshot.threads.is_empty()
        || limits.max_serialized_bytes == 0
        || limits.max_token_upper_bound == 0
    {
        bail!("self-improvement history chunk planner input is invalid");
    }

    let mut drafts = Vec::<Vec<SelfImprovementHistoryThread>>::new();
    let mut current = Vec::<SelfImprovementHistoryThread>::new();
    for thread in &snapshot.threads {
        if thread.thread_id.trim().is_empty() || thread.turns.is_empty() {
            bail!("self-improvement history chunk planner found an empty thread");
        }
        for turn in &thread.turns {
            if turn.turn_id.trim().is_empty() || turn.blocks.is_empty() {
                bail!("self-improvement history chunk planner found an empty turn");
            }
            for block in &turn.blocks {
                if block.event_id.trim().is_empty()
                    || block.fragment_index != 0
                    || block.fragment_count != 1
                {
                    bail!("self-improvement history chunk planner found an invalid source block");
                }
            }

            if try_place_history_unit(
                snapshot,
                limits,
                thread.thread_id.as_str(),
                turn.turn_id.as_str(),
                turn.blocks.as_slice(),
                &mut current,
                &mut drafts,
            )? {
                continue;
            }

            for event_blocks in contiguous_event_groups(turn.blocks.as_slice()) {
                if try_place_history_unit(
                    snapshot,
                    limits,
                    thread.thread_id.as_str(),
                    turn.turn_id.as_str(),
                    event_blocks,
                    &mut current,
                    &mut drafts,
                )? {
                    continue;
                }

                for block in event_blocks {
                    if try_place_history_unit(
                        snapshot,
                        limits,
                        thread.thread_id.as_str(),
                        turn.turn_id.as_str(),
                        std::slice::from_ref(block),
                        &mut current,
                        &mut drafts,
                    )? {
                        continue;
                    }
                    for fragment in fragment_text_block(
                        snapshot,
                        limits,
                        thread.thread_id.as_str(),
                        turn.turn_id.as_str(),
                        block,
                    )? {
                        if !try_place_history_unit(
                            snapshot,
                            limits,
                            thread.thread_id.as_str(),
                            turn.turn_id.as_str(),
                            std::slice::from_ref(&fragment),
                            &mut current,
                            &mut drafts,
                        )? {
                            bail!(
                                "self-improvement history text fragment exceeds the chunk contract"
                            );
                        }
                    }
                }
            }
        }
    }
    if !current.is_empty() {
        drafts.push(current);
    }
    if drafts.is_empty() {
        bail!("self-improvement history chunk planner produced no chunks");
    }

    let chunk_count = u32::try_from(drafts.len())
        .context("self-improvement history chunk count exceeds the contract")?;
    drafts
        .into_iter()
        .enumerate()
        .map(|(index, threads)| {
            let chunk_index = u32::try_from(index)
                .context("self-improvement history chunk index exceeds the contract")?;
            finalize_history_chunk(snapshot, limits, chunk_index, chunk_count, threads)
        })
        .collect()
}

pub(crate) fn validate_history_chunk_contract(
    chunk: &SelfImprovementHistoryChunk,
    limits: HistoryChunkLimits,
) -> Result<()> {
    if chunk.workspace_id.trim().is_empty()
        || chunk.source_lower_exclusive < 0
        || chunk.source_upper_inclusive <= chunk.source_lower_exclusive
        || chunk.chunk_count == 0
        || chunk.chunk_index >= chunk.chunk_count
        || chunk.threads.is_empty()
    {
        bail!("self-improvement history chunk identity is invalid");
    }
    for thread in &chunk.threads {
        if thread.thread_id.trim().is_empty() || thread.turns.is_empty() {
            bail!("self-improvement history chunk contains an empty thread");
        }
        for turn in &thread.turns {
            if turn.turn_id.trim().is_empty() || turn.blocks.is_empty() {
                bail!("self-improvement history chunk contains an empty turn");
            }
            for block in &turn.blocks {
                if block.event_id.trim().is_empty()
                    || block.event_thread_id.trim().is_empty()
                    || block.event_turn_id.trim().is_empty()
                    || block.fragment_count == 0
                    || block.fragment_index >= block.fragment_count
                {
                    bail!("self-improvement history chunk contains an invalid block");
                }
            }
        }
    }
    let fingerprint = compute_history_chunk_fingerprint(chunk)?;
    if fingerprint != chunk.fingerprint {
        bail!("self-improvement history chunk fingerprint is invalid");
    }
    let encoded = serde_json::to_vec(chunk)
        .context("failed to verify self-improvement history chunk size")?;
    if encoded.len() > limits.max_serialized_bytes
        || conservative_token_upper_bound(encoded.as_slice()) > limits.max_token_upper_bound
    {
        bail!("self-improvement history chunk exceeds the request contract");
    }
    Ok(())
}

pub(crate) fn compute_history_chunk_fingerprint(
    chunk: &SelfImprovementHistoryChunk,
) -> Result<String> {
    let fingerprint_input = FingerprintInput {
        schema_version: chunk.schema_version,
        workspace_id: chunk.workspace_id.as_str(),
        source_lower_exclusive: chunk.source_lower_exclusive,
        source_upper_inclusive: chunk.source_upper_inclusive,
        chunk_index: chunk.chunk_index,
        chunk_count: chunk.chunk_count,
        threads: chunk.threads.as_slice(),
    };
    Ok(hex::encode(Sha256::digest(
        serde_json::to_vec(&fingerprint_input)
            .context("failed to fingerprint self-improvement history chunk")?,
    )))
}

fn contiguous_event_groups(
    blocks: &[SelfImprovementHistoryBlock],
) -> Vec<&[SelfImprovementHistoryBlock]> {
    let mut groups = Vec::new();
    let mut start = 0;
    while start < blocks.len() {
        let event_id = blocks[start].event_id.as_str();
        let mut end = start + 1;
        while end < blocks.len() && blocks[end].event_id == event_id {
            end += 1;
        }
        groups.push(&blocks[start..end]);
        start = end;
    }
    groups
}

fn try_place_history_unit(
    snapshot: &SelfImprovementHistorySnapshot,
    limits: HistoryChunkLimits,
    thread_id: &str,
    turn_id: &str,
    blocks: &[SelfImprovementHistoryBlock],
    current: &mut Vec<SelfImprovementHistoryThread>,
    drafts: &mut Vec<Vec<SelfImprovementHistoryThread>>,
) -> Result<bool> {
    let mut candidate = current.clone();
    append_history_unit(&mut candidate, thread_id, turn_id, blocks);
    if history_chunk_threads_fit(snapshot, limits, candidate.as_slice())? {
        *current = candidate;
        return Ok(true);
    }

    if !current.is_empty() {
        drafts.push(std::mem::take(current));
    }
    let mut candidate = Vec::new();
    append_history_unit(&mut candidate, thread_id, turn_id, blocks);
    if history_chunk_threads_fit(snapshot, limits, candidate.as_slice())? {
        *current = candidate;
        return Ok(true);
    }
    Ok(false)
}

fn append_history_unit(
    threads: &mut Vec<SelfImprovementHistoryThread>,
    thread_id: &str,
    turn_id: &str,
    blocks: &[SelfImprovementHistoryBlock],
) {
    if threads
        .last()
        .is_none_or(|thread| thread.thread_id != thread_id)
    {
        threads.push(SelfImprovementHistoryThread {
            thread_id: thread_id.to_owned(),
            turns: Vec::new(),
        });
    }
    let thread = threads
        .last_mut()
        .expect("history thread was just inserted");
    if thread
        .turns
        .last()
        .is_none_or(|turn| turn.turn_id != turn_id)
    {
        thread.turns.push(SelfImprovementHistoryTurn {
            turn_id: turn_id.to_owned(),
            blocks: Vec::new(),
        });
    }
    thread
        .turns
        .last_mut()
        .expect("history turn was just inserted")
        .blocks
        .extend_from_slice(blocks);
}

fn history_chunk_threads_fit(
    snapshot: &SelfImprovementHistorySnapshot,
    limits: HistoryChunkLimits,
    threads: &[SelfImprovementHistoryThread],
) -> Result<bool> {
    let encoded_len = history_chunk_probe_encoded_len(snapshot, threads)?;
    Ok(encoded_len <= limits.max_serialized_bytes && encoded_len <= limits.max_token_upper_bound)
}

fn history_chunk_probe_encoded_len(
    snapshot: &SelfImprovementHistorySnapshot,
    threads: &[SelfImprovementHistoryThread],
) -> Result<usize> {
    let probe = SelfImprovementHistoryChunk {
        schema_version: snapshot.schema_version,
        workspace_id: snapshot.workspace_id.clone(),
        source_lower_exclusive: snapshot.source_lower_exclusive,
        source_upper_inclusive: snapshot.source_upper_inclusive,
        chunk_index: u32::MAX,
        chunk_count: u32::MAX,
        threads: threads.to_vec(),
        fingerprint: "0".repeat(64),
    };
    let encoded =
        serde_json::to_vec(&probe).context("failed to size self-improvement history chunk")?;
    Ok(encoded.len())
}

fn conservative_token_upper_bound(encoded: &[u8]) -> usize {
    // A tokenizer cannot produce more useful input units than this byte ceiling.
    // Using bytes as the upper bound is deliberately conservative for UTF-8.
    encoded.len()
}

fn finalize_history_chunk(
    snapshot: &SelfImprovementHistorySnapshot,
    limits: HistoryChunkLimits,
    chunk_index: u32,
    chunk_count: u32,
    threads: Vec<SelfImprovementHistoryThread>,
) -> Result<SelfImprovementHistoryChunk> {
    let mut chunk = SelfImprovementHistoryChunk {
        schema_version: snapshot.schema_version,
        workspace_id: snapshot.workspace_id.clone(),
        source_lower_exclusive: snapshot.source_lower_exclusive,
        source_upper_inclusive: snapshot.source_upper_inclusive,
        chunk_index,
        chunk_count,
        threads,
        fingerprint: String::new(),
    };
    chunk.fingerprint = compute_history_chunk_fingerprint(&chunk)?;
    let encoded =
        serde_json::to_vec(&chunk).context("failed to encode self-improvement history chunk")?;
    if encoded.len() > limits.max_serialized_bytes
        || conservative_token_upper_bound(encoded.as_slice()) > limits.max_token_upper_bound
    {
        bail!("self-improvement history chunk exceeds the finalized contract");
    }
    Ok(chunk)
}

fn fragment_text_block(
    snapshot: &SelfImprovementHistorySnapshot,
    limits: HistoryChunkLimits,
    thread_id: &str,
    turn_id: &str,
    block: &SelfImprovementHistoryBlock,
) -> Result<Vec<SelfImprovementHistoryBlock>> {
    let text = match &block.content {
        SelfImprovementHistoryContent::UserText { text }
        | SelfImprovementHistoryContent::AssistantMessage { text, .. } => text.as_str(),
        _ => {
            bail!(
                "self-improvement history contains oversized indivisible metadata event `{}`",
                block.event_id
            )
        }
    };
    if text.is_empty() {
        bail!(
            "self-improvement history contains oversized empty text event `{}`",
            block.event_id
        );
    }

    let mut boundaries = text
        .char_indices()
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    boundaries.push(text.len());
    let mut fragments = Vec::new();
    let mut start_boundary = 0usize;
    while start_boundary + 1 < boundaries.len() {
        let mut low = start_boundary + 1;
        let mut high = boundaries.len() - 1;
        let mut best = None;
        while low <= high {
            let middle = low + (high - low) / 2;
            let candidate =
                fragment_probe_block(block, &text[boundaries[start_boundary]..boundaries[middle]]);
            let fits = history_unit_fits_empty(
                snapshot,
                limits,
                thread_id,
                turn_id,
                std::slice::from_ref(&candidate),
            )?;
            if fits {
                best = Some(middle);
                low = middle + 1;
            } else if middle == 0 {
                break;
            } else {
                high = middle - 1;
            }
        }
        let end_boundary = best.with_context(|| {
            format!(
                "self-improvement history text event `{}` cannot fit even one UTF-8 scalar",
                block.event_id
            )
        })?;
        fragments.push(text[boundaries[start_boundary]..boundaries[end_boundary]].to_owned());
        start_boundary = end_boundary;
    }

    let fragment_count = u32::try_from(fragments.len())
        .context("self-improvement history fragment count exceeds the contract")?;
    fragments
        .into_iter()
        .enumerate()
        .map(|(index, text)| {
            let fragment_index = u32::try_from(index)
                .context("self-improvement history fragment index exceeds the contract")?;
            let mut fragment = block.clone();
            fragment.block_key = format!(
                "{}:fragment:{}-of-{}",
                block.block_key,
                fragment_index + 1,
                fragment_count
            );
            fragment.fragment_index = fragment_index;
            fragment.fragment_count = fragment_count;
            replace_block_text(&mut fragment, text);
            Ok(fragment)
        })
        .collect()
}

fn fragment_probe_block(
    block: &SelfImprovementHistoryBlock,
    text: &str,
) -> SelfImprovementHistoryBlock {
    let mut candidate = block.clone();
    candidate.block_key = format!("{}:fragment:{}-of-{}", block.block_key, u32::MAX, u32::MAX);
    candidate.fragment_index = u32::MAX;
    candidate.fragment_count = u32::MAX;
    replace_block_text(&mut candidate, text.to_owned());
    candidate
}

fn replace_block_text(block: &mut SelfImprovementHistoryBlock, text: String) {
    match &mut block.content {
        SelfImprovementHistoryContent::UserText {
            text: fragment_text,
        }
        | SelfImprovementHistoryContent::AssistantMessage {
            text: fragment_text,
            ..
        } => *fragment_text = text,
        _ => unreachable!("only text blocks reach history fragmentation"),
    }
}

fn history_unit_fits_empty(
    snapshot: &SelfImprovementHistorySnapshot,
    limits: HistoryChunkLimits,
    thread_id: &str,
    turn_id: &str,
    blocks: &[SelfImprovementHistoryBlock],
) -> Result<bool> {
    let mut threads = Vec::new();
    append_history_unit(&mut threads, thread_id, turn_id, blocks);
    history_chunk_threads_fit(snapshot, limits, threads.as_slice())
}

fn map_record(
    record: &CanonicalTurnEventRecord,
    evidence_role: HistoryEvidenceRole,
) -> Result<Vec<SelfImprovementHistoryBlock>> {
    let mut blocks = Vec::new();
    match &record.payload {
        CanonicalTurnEventPayload::TurnStarted(payload) => {
            for (input_index, input) in payload.input.iter().enumerate() {
                if let Some(content) = map_user_input(input)? {
                    blocks.push(block(record, Some(input_index), evidence_role, content));
                }
            }
        }
        CanonicalTurnEventPayload::ItemStarted(notification) => {
            if let Some(content) = map_item(&notification.item, HistoryToolStage::Started)? {
                blocks.push(block(record, None, evidence_role, content));
            }
        }
        CanonicalTurnEventPayload::ItemCompleted(notification) => {
            if let Some(content) = map_item(&notification.item, HistoryToolStage::Completed)? {
                blocks.push(block(record, None, evidence_role, content));
            }
        }
        CanonicalTurnEventPayload::ItemUpdated(notification) => {
            if let Some(content) = map_item(&notification.item, HistoryToolStage::Updated)? {
                blocks.push(block(record, None, evidence_role, content));
            }
        }
        CanonicalTurnEventPayload::TurnPermissionAudit(event)
            if matches!(
                event.event_kind,
                TurnPermissionAuditEventKind::ApprovalRequested
                    | TurnPermissionAuditEventKind::ApprovalResolved
                    | TurnPermissionAuditEventKind::DecisionAllowed
                    | TurnPermissionAuditEventKind::DecisionDenied
            ) =>
        {
            blocks.push(block(
                record,
                None,
                evidence_role,
                SelfImprovementHistoryContent::PermissionOutcome {
                    event_kind: json_enum(&event.event_kind)?,
                    action_kind: event.action_kind.as_ref().map(json_enum).transpose()?,
                    tool_name: event.tool_name.clone(),
                    decision: event.decision.as_ref().map(json_enum).transpose()?,
                    reason: event.reason.as_ref().map(json_enum).transpose()?,
                },
            ));
        }
        CanonicalTurnEventPayload::TurnCompleted(notification) => {
            blocks.push(terminal_block(
                record,
                evidence_role,
                notification.turn.status,
                notification.turn.error.clone(),
            ));
        }
        CanonicalTurnEventPayload::TurnFailed(notification) => {
            blocks.push(terminal_block(
                record,
                evidence_role,
                notification.turn.status,
                notification.turn.error.clone(),
            ));
        }
        CanonicalTurnEventPayload::TurnBlocked(notification) => {
            blocks.push(terminal_block(
                record,
                evidence_role,
                notification.turn.status,
                notification.turn.error.clone(),
            ));
        }
        CanonicalTurnEventPayload::ItemTimeoutDetected(_)
        | CanonicalTurnEventPayload::ItemRecoveryOpened(_)
        | CanonicalTurnEventPayload::ItemRecoveryAttached(_)
        | CanonicalTurnEventPayload::ItemRetryScheduled(_)
        | CanonicalTurnEventPayload::ItemRetryAttemptStarted(_)
        | CanonicalTurnEventPayload::ItemRecoverySucceeded(_)
        | CanonicalTurnEventPayload::ItemRecoveryExhausted(_)
        | CanonicalTurnEventPayload::ItemToolRetryScheduled(_)
        | CanonicalTurnEventPayload::ItemToolRetryResolved(_)
        | CanonicalTurnEventPayload::ItemToolRetryExhausted(_)
        | CanonicalTurnEventPayload::TurnToolLoopBudgetExceeded(_)
        | CanonicalTurnEventPayload::TurnExecutionWindowStarted(_)
        | CanonicalTurnEventPayload::TurnExecutionWindowExhausted(_)
        | CanonicalTurnEventPayload::TurnExecutionWindowCheckpointed(_)
        | CanonicalTurnEventPayload::TurnExecutionWindowContinued(_)
        | CanonicalTurnEventPayload::TurnExecutionWindowBlocked(_)
        | CanonicalTurnEventPayload::TurnPermissionAudit(_) => {}
    }
    Ok(blocks)
}

fn map_user_input(input: &UserInput) -> Result<Option<SelfImprovementHistoryContent>> {
    Ok(Some(match input {
        UserInput::Text { text, .. } => {
            SelfImprovementHistoryContent::UserText { text: text.clone() }
        }
        UserInput::Image { url } => attachment("image", json!({"url": url})),
        UserInput::LocalImage { path } => attachment("local_image", json!({"path": path})),
        UserInput::File { url } => attachment("file", json!({"url": url})),
        UserInput::LocalFile { path } => attachment("local_file", json!({"path": path})),
        UserInput::Audio { url } => attachment("audio", json!({"url": url})),
        UserInput::LocalAudio { path } => attachment("local_audio", json!({"path": path})),
        UserInput::Video { url } => attachment("video", json!({"url": url})),
        UserInput::LocalVideo { path } => attachment("local_video", json!({"path": path})),
        UserInput::Artifact {
            artifact_id,
            version_id,
        } => attachment(
            "artifact",
            json!({"artifactId": artifact_id, "versionId": version_id}),
        ),
        UserInput::Mention { name, path } => {
            attachment("mention", json!({"name": name, "path": path}))
        }
    }))
}

fn map_item(
    item: &TurnItem,
    stage: HistoryToolStage,
) -> Result<Option<SelfImprovementHistoryContent>> {
    Ok(match item {
        TurnItem::UserMessage { attachments, .. } => {
            if stage != HistoryToolStage::Completed {
                return Ok(None);
            }
            let metadata = attachments
                .iter()
                .filter(|attachment| {
                    matches!(
                        attachment,
                        UserMessageAttachment::Skill { .. }
                            | UserMessageAttachment::SkillPack { .. }
                            | UserMessageAttachment::McpServer { .. }
                            | UserMessageAttachment::McpTool { .. }
                    )
                })
                .map(safe_attachment_metadata)
                .collect::<Result<Vec<_>>>()?;
            (!metadata.is_empty()).then_some(SelfImprovementHistoryContent::Attachment {
                attachment_kind: "turn_capabilities".to_owned(),
                metadata: Value::Array(metadata),
            })
        }
        TurnItem::AgentMessage { text, phase, .. } if stage == HistoryToolStage::Completed => {
            Some(SelfImprovementHistoryContent::AssistantMessage {
                phase: match phase {
                    AgentMessagePhase::FinalAnswer => "final_answer",
                    AgentMessagePhase::Commentary => "commentary",
                }
                .to_owned(),
                text: text.clone(),
            })
        }
        TurnItem::AgentMessage { .. } => None,
        TurnItem::CommandExecution {
            id,
            tool_name,
            arguments,
            status,
            storage,
            command,
            cwd,
            success,
            outcome,
            ..
        } => Some(tool(
            stage,
            id,
            tool_name,
            *status,
            arguments,
            storage,
            json!({
                "command": command,
                "cwd": cwd,
                "success": success,
                "outcome": outcome,
            }),
        )?),
        TurnItem::FileChange {
            id,
            tool_name,
            arguments,
            status,
            storage,
            changed_files,
            exit_code,
            success,
            outcome,
            ..
        } => Some(tool(
            stage,
            id,
            tool_name,
            *status,
            arguments,
            storage,
            json!({
                "changedFiles": changed_files,
                "exitCode": exit_code,
                "success": success,
                "outcome": outcome,
            }),
        )?),
        TurnItem::WebSearch {
            id,
            tool_name,
            arguments,
            status,
            storage,
            query,
            provider,
            took_ms,
            result_count,
            results,
            success,
            outcome,
            ..
        } => Some(tool(
            stage,
            id,
            tool_name,
            *status,
            arguments,
            storage,
            json!({
                "query": query,
                "provider": provider,
                "tookMs": took_ms,
                "resultCount": result_count,
                "results": results,
                "success": success,
                "outcome": outcome,
            }),
        )?),
        TurnItem::WebFetch {
            id,
            tool_name,
            arguments,
            status,
            storage,
            url,
            final_url,
            status_code,
            content_type,
            extract_mode,
            resolved_mode,
            bytes_received,
            elapsed_ms,
            truncated,
            title,
            word_count,
            links,
            success,
            outcome,
            ..
        } => Some(tool(
            stage,
            id,
            tool_name,
            *status,
            arguments,
            storage,
            json!({
                "url": url,
                "finalUrl": final_url,
                "statusCode": status_code,
                "contentType": content_type,
                "extractMode": extract_mode,
                "resolvedMode": resolved_mode,
                "bytesReceived": bytes_received,
                "elapsedMs": elapsed_ms,
                "truncated": truncated,
                "title": title,
                "wordCount": word_count,
                "links": links,
                "success": success,
                "outcome": outcome,
            }),
        )?),
        TurnItem::Download {
            id,
            tool_name,
            arguments,
            status,
            storage,
            url,
            final_url,
            status_code,
            path,
            bytes_written,
            sha256,
            content_type,
            elapsed_ms,
            truncated,
            success,
            outcome,
            ..
        } => Some(tool(
            stage,
            id,
            tool_name,
            *status,
            arguments,
            storage,
            json!({
                "url": url,
                "finalUrl": final_url,
                "statusCode": status_code,
                "path": path,
                "bytesWritten": bytes_written,
                "sha256": sha256,
                "contentType": content_type,
                "elapsedMs": elapsed_ms,
                "truncated": truncated,
                "success": success,
                "outcome": outcome,
            }),
        )?),
        TurnItem::DynamicToolCall {
            id,
            tool_name,
            arguments,
            status,
            storage,
            success,
            outcome,
            ..
        } => Some(tool(
            stage,
            id,
            tool_name,
            *status,
            arguments,
            storage,
            json!({"success": success, "outcome": outcome}),
        )?),
        TurnItem::Reasoning { .. } | TurnItem::SystemEvent { .. } | TurnItem::Task { .. } => None,
    })
}

fn tool(
    stage: HistoryToolStage,
    item_id: &str,
    tool_name: &str,
    status: ToolCallStatus,
    arguments: &Value,
    storage: &ToolStoragePayload,
    metadata: Value,
) -> Result<SelfImprovementHistoryContent> {
    let mut arguments = arguments.clone();
    redact_known_credentials(&mut arguments);
    let mut stored_result =
        serde_json::to_value(storage).context("failed to encode stored tool result")?;
    redact_known_credentials(&mut stored_result);
    let mut metadata = metadata;
    redact_known_credentials(&mut metadata);
    Ok(SelfImprovementHistoryContent::Tool {
        stage,
        item_id: item_id.to_owned(),
        tool_name: tool_name.to_owned(),
        status: match status {
            ToolCallStatus::InProgress => "in_progress",
            ToolCallStatus::Completed => "completed",
            ToolCallStatus::Failed => "failed",
        }
        .to_owned(),
        arguments,
        stored_result,
        metadata,
    })
}

fn safe_attachment_metadata(attachment: &UserMessageAttachment) -> Result<Value> {
    let mut value =
        serde_json::to_value(attachment).context("failed to encode attachment metadata")?;
    redact_known_credentials(&mut value);
    Ok(value)
}

fn attachment(kind: &str, mut metadata: Value) -> SelfImprovementHistoryContent {
    redact_known_credentials(&mut metadata);
    SelfImprovementHistoryContent::Attachment {
        attachment_kind: kind.to_owned(),
        metadata,
    }
}

fn terminal_block(
    record: &CanonicalTurnEventRecord,
    evidence_role: HistoryEvidenceRole,
    status: TurnStatus,
    error: Option<String>,
) -> SelfImprovementHistoryBlock {
    block(
        record,
        None,
        evidence_role,
        SelfImprovementHistoryContent::Terminal {
            status: match status {
                TurnStatus::InProgress => "in_progress",
                TurnStatus::Completed => "completed",
                TurnStatus::Failed => "failed",
                TurnStatus::Interrupted => "interrupted",
                TurnStatus::Blocked => "blocked",
            }
            .to_owned(),
            error,
        },
    )
}

fn block(
    record: &CanonicalTurnEventRecord,
    input_index: Option<usize>,
    evidence_role: HistoryEvidenceRole,
    content: SelfImprovementHistoryContent,
) -> SelfImprovementHistoryBlock {
    SelfImprovementHistoryBlock {
        block_key: input_index.map_or_else(
            || record.event_id.clone(),
            |index| format!("{}:input:{index}", record.event_id),
        ),
        event_id: record.event_id.clone(),
        event_thread_id: record.thread_id.clone(),
        event_turn_id: record.turn_id.clone(),
        sequence: record.sequence,
        input_index,
        fragment_index: 0,
        fragment_count: 1,
        evidence_role,
        content,
    }
}

fn json_enum<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_value(value)
        .context("failed to encode canonical enum")?
        .as_str()
        .map(str::to_owned)
        .context("canonical enum did not encode as a string")
}

fn redact_known_credentials(value: &mut Value) {
    match value {
        Value::Object(map) => redact_object(map),
        Value::Array(values) => values.iter_mut().for_each(redact_known_credentials),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn redact_object(map: &mut Map<String, Value>) {
    for (key, value) in map {
        if is_credential_key(key) {
            *value = Value::String("[redacted]".to_owned());
        } else {
            redact_known_credentials(value);
        }
    }
}

fn is_credential_key(key: &str) -> bool {
    matches!(
        key.chars()
            .filter(|character| character.is_ascii_alphanumeric())
            .flat_map(char::to_lowercase)
            .collect::<String>()
            .as_str(),
        "apikey"
            | "authorization"
            | "accesstoken"
            | "refreshtoken"
            | "bearertoken"
            | "clientsecret"
            | "cookie"
            | "credential"
            | "credentials"
            | "password"
            | "privatekey"
            | "secret"
            | "token"
    )
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use pioneer_crud::{
        CanonicalTurnEventPayload, CanonicalTurnStartedEventPayload,
        SelfImprovementFrozenSourceRange, SelfImprovementSourceTurnRecord,
    };
    use pioneer_protocol::{
        ItemCompletedNotification, ItemStartedNotification, SandboxMode, SystemEventLevel, Thread,
        ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus, ToolCallStatus,
        ToolDisplayPayload, ToolMetadata, ToolOutputPolicySnapshot, ToolStoragePayload, Turn,
        TurnCompletedNotification, TurnFailedNotification, TurnItem, TurnKind, TurnOrigin,
        TurnPermissionActionKind, TurnPermissionAuditDecision, TurnPermissionAuditEvent,
        TurnPermissionAuditEventKind, TurnPermissionDecisionReason, TurnPermissionMode,
        TurnPermissionProfileSource, TurnSkillCapabilitySummary, TurnStatus, UserInput,
        UserMessageAttachment, default_turn_permission_profile_snapshot,
    };

    use super::*;

    fn source(id: i64, turn_id: &str, terminal_event_id: &str) -> SelfImprovementSourceTurnRecord {
        SelfImprovementSourceTurnRecord {
            id,
            workspace_id: "ws_history".to_owned(),
            thread_id: "thread_history".to_owned(),
            turn_id: turn_id.to_owned(),
            parent_turn_created_at_unix: 1_900_000_000 + id,
            task_delivery_id: None,
            terminal_event_id: terminal_event_id.to_owned(),
            terminal_at_unix: 1_900_000_000 + id,
            created_at_unix: 1_900_000_000 + id,
        }
    }

    fn frozen_range(
        source_lower_exclusive: i64,
        sources: &[SelfImprovementSourceTurnRecord],
    ) -> SelfImprovementFrozenSourceRange {
        SelfImprovementFrozenSourceRange::new(
            "ws_history",
            source_lower_exclusive,
            sources.last().expect("range requires an anchor").id,
            sources.to_vec(),
        )
        .expect("source range must freeze")
    }

    fn thread() -> Thread {
        history_thread(
            "thread_history",
            ThreadOriginKind::User,
            ThreadSidebarVisibility::Visible,
        )
    }

    fn history_thread(
        thread_id: &str,
        origin_kind: ThreadOriginKind,
        sidebar_visibility: ThreadSidebarVisibility,
    ) -> Thread {
        Thread {
            workspace_id: "ws_history".to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "hidden-model".to_owned(),
            model_provider: "hidden-provider".to_owned(),
            reasoning_effort: Some("hidden-reasoning".to_owned()),
            created_at: 1_900_000_000,
            updated_at: 1_900_000_000,
            status: ThreadStatus::Active,
            origin_kind,
            sidebar_visibility,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        }
    }

    fn turn(turn_id: &str, status: TurnStatus) -> Turn {
        history_turn(turn_id, status, TurnKind::Conversation, TurnOrigin::User)
    }

    fn history_turn(
        turn_id: &str,
        status: TurnStatus,
        turn_kind: TurnKind,
        origin: TurnOrigin,
    ) -> Turn {
        Turn {
            id: turn_id.to_owned(),
            status,
            turn_kind,
            origin,
            error: None,
            prompt_manifest: None,
            permission_profile: default_turn_permission_profile_snapshot(),
        }
    }

    fn record(
        event_id: &str,
        turn_id: &str,
        sequence: i64,
        payload: CanonicalTurnEventPayload,
    ) -> CanonicalTurnEventRecord {
        record_in(event_id, "thread_history", turn_id, sequence, payload)
    }

    fn record_in(
        event_id: &str,
        thread_id: &str,
        turn_id: &str,
        sequence: i64,
        payload: CanonicalTurnEventPayload,
    ) -> CanonicalTurnEventRecord {
        CanonicalTurnEventRecord {
            event_id: event_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            sequence,
            created_at: chrono::FixedOffset::east_opt(0)
                .unwrap()
                .timestamp_opt(1_900_000_000 + sequence, 0)
                .unwrap(),
            payload,
        }
    }

    fn history_records(secret: &str) -> Vec<CanonicalTurnEventRecord> {
        let old_turn = turn("turn_old", TurnStatus::InProgress);
        let new_turn = turn("turn_new", TurnStatus::InProgress);
        vec![
            record(
                "old-start",
                "turn_old",
                1,
                CanonicalTurnEventPayload::TurnStarted(CanonicalTurnStartedEventPayload {
                    thread: thread(),
                    sandbox_mode: SandboxMode::FullAccess,
                    turn: old_turn.clone(),
                    input: vec![UserInput::Text {
                        text: "old procedure".to_owned(),
                        text_elements: Vec::new(),
                    }],
                    reasoning_effort: Some("must-not-leak".to_owned()),
                }),
            ),
            record(
                "old-terminal",
                "turn_old",
                2,
                CanonicalTurnEventPayload::TurnFailed(TurnFailedNotification {
                    workspace_id: "ws_history".to_owned(),
                    thread_id: "thread_history".to_owned(),
                    turn: Turn {
                        status: TurnStatus::Interrupted,
                        error: Some("visible interruption".to_owned()),
                        ..old_turn
                    },
                }),
            ),
            record(
                "new-start",
                "turn_new",
                1,
                CanonicalTurnEventPayload::TurnStarted(CanonicalTurnStartedEventPayload {
                    thread: thread(),
                    sandbox_mode: SandboxMode::FullAccess,
                    turn: new_turn.clone(),
                    input: vec![
                        UserInput::Text {
                            text: "new procedure".to_owned(),
                            text_elements: Vec::new(),
                        },
                        UserInput::Artifact {
                            artifact_id: "artifact-a".to_owned(),
                            version_id: Some("version-a".to_owned()),
                        },
                    ],
                    reasoning_effort: Some(secret.to_owned()),
                }),
            ),
            record(
                "new-reasoning",
                "turn_new",
                2,
                CanonicalTurnEventPayload::ItemCompleted(ItemCompletedNotification {
                    workspace_id: "ws_history".to_owned(),
                    thread_id: "thread_history".to_owned(),
                    turn_id: "turn_new".to_owned(),
                    item: TurnItem::Reasoning {
                        id: "reasoning-new".to_owned(),
                        summary: vec![secret.to_owned()],
                        content: vec![secret.to_owned()],
                    },
                }),
            ),
            record(
                "new-system",
                "turn_new",
                3,
                CanonicalTurnEventPayload::ItemCompleted(ItemCompletedNotification {
                    workspace_id: "ws_history".to_owned(),
                    thread_id: "thread_history".to_owned(),
                    turn_id: "turn_new".to_owned(),
                    item: TurnItem::SystemEvent {
                        id: "system-new".to_owned(),
                        level: SystemEventLevel::Info,
                        message: secret.to_owned(),
                        code: Some("internal_preflight".to_owned()),
                        details: Some(json!({"memoryRecall": secret})),
                    },
                }),
            ),
            record(
                "new-tool",
                "turn_new",
                4,
                CanonicalTurnEventPayload::ItemCompleted(ItemCompletedNotification {
                    workspace_id: "ws_history".to_owned(),
                    thread_id: "thread_history".to_owned(),
                    turn_id: "turn_new".to_owned(),
                    item: TurnItem::DynamicToolCall {
                        id: "tool-new".to_owned(),
                        tool_name: "visible_tool".to_owned(),
                        arguments: json!({"query": "visible", "apiKey": secret}),
                        status: ToolCallStatus::Completed,
                        recovery_policy: None,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("visible_tool"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::Metadata {
                            metadata: ToolMetadata::from_json(json!({"result": "visible"})),
                        },
                        recovery: None,
                        success: Some(true),
                        outcome: None,
                        observation: None,
                    },
                }),
            ),
            record(
                "new-assistant",
                "turn_new",
                5,
                CanonicalTurnEventPayload::ItemCompleted(ItemCompletedNotification {
                    workspace_id: "ws_history".to_owned(),
                    thread_id: "thread_history".to_owned(),
                    turn_id: "turn_new".to_owned(),
                    item: TurnItem::AgentMessage {
                        id: "assistant-new".to_owned(),
                        text: "visible assistant procedure".to_owned(),
                        phase: AgentMessagePhase::FinalAnswer,
                        markdown: None,
                        markdown_version: None,
                    },
                }),
            ),
            record(
                "new-permission",
                "turn_new",
                6,
                CanonicalTurnEventPayload::TurnPermissionAudit(TurnPermissionAuditEvent {
                    workspace_id: "ws_history".to_owned(),
                    thread_id: "thread_history".to_owned(),
                    turn_id: "turn_new".to_owned(),
                    event_kind: TurnPermissionAuditEventKind::DecisionDenied,
                    profile_mode: TurnPermissionMode::Supervised,
                    profile_source: TurnPermissionProfileSource::Composer,
                    security_snapshot_id: None,
                    security_snapshot_version: None,
                    security_reason_code: None,
                    security_capability: None,
                    item_id: Some("tool-new".to_owned()),
                    tool_call_id: Some("call-new".to_owned()),
                    tool_name: Some("visible_tool".to_owned()),
                    action_kind: Some(TurnPermissionActionKind::FileRead),
                    request_key: None,
                    decision: Some(TurnPermissionAuditDecision::Deny),
                    reason: Some(TurnPermissionDecisionReason::SandboxDenied),
                    cached: false,
                }),
            ),
            record(
                "new-terminal",
                "turn_new",
                7,
                CanonicalTurnEventPayload::TurnCompleted(TurnCompletedNotification {
                    workspace_id: "ws_history".to_owned(),
                    thread_id: "thread_history".to_owned(),
                    turn: Turn {
                        status: TurnStatus::Completed,
                        ..new_turn
                    },
                }),
            ),
        ]
    }

    #[test]
    fn message_projection_uses_one_authoritative_text_revision() {
        let user = TurnItem::UserMessage {
            id: "user-message".to_owned(),
            text: "text already represented by turn/start".to_owned(),
            attachments: vec![UserMessageAttachment::Skill {
                capability: TurnSkillCapabilitySummary {
                    skill_id: pioneer_protocol::SkillId::new("AAAAAAAAAAAAAAAAAAAAA")
                        .expect("valid skill ID"),
                    label: "Selected skill".to_owned(),
                    owner: Some("workspace".to_owned()),
                    slug: "selected-skill".to_owned(),
                    source_kind: "user".to_owned(),
                    pack: None,
                },
            }],
        };
        assert!(
            map_item(&user, HistoryToolStage::Started)
                .expect("started user item must map")
                .is_none()
        );
        let completed_user = map_item(&user, HistoryToolStage::Completed)
            .expect("completed user item must map")
            .expect("visible capability metadata must remain");
        let encoded = serde_json::to_string(&completed_user).expect("history content must encode");
        assert!(!encoded.contains("text already represented by turn/start"));
        assert!(encoded.contains("AAAAAAAAAAAAAAAAAAAAA"));

        let assistant = TurnItem::AgentMessage {
            id: "assistant-message".to_owned(),
            text: "exact completed assistant message".to_owned(),
            phase: AgentMessagePhase::FinalAnswer,
            markdown: None,
            markdown_version: None,
        };
        assert!(
            map_item(&assistant, HistoryToolStage::Started)
                .expect("started assistant item must map")
                .is_none()
        );
        assert!(
            map_item(&assistant, HistoryToolStage::Updated)
                .expect("updated assistant item must map")
                .is_none()
        );
        assert!(matches!(
            map_item(&assistant, HistoryToolStage::Completed)
                .expect("completed assistant item must map"),
            Some(SelfImprovementHistoryContent::AssistantMessage { text, .. })
                if text == "exact completed assistant message"
        ));
    }

    #[test]
    fn full_snapshot_is_deterministic_ordered_and_marks_new_evidence() {
        let sources = [source(7, "turn_new", "new-terminal")];
        let frozen_range = frozen_range(6, &sources);
        let first =
            build_model_safe_full_thread_snapshot(&frozen_range, &history_records("secret"))
                .expect("history should map");
        let second =
            build_model_safe_full_thread_snapshot(&frozen_range, &history_records("secret"))
                .expect("history should map deterministically");
        assert_eq!(first, second);
        assert_eq!(first.threads[0].turns[0].turn_id, "turn_old");
        assert_eq!(first.threads[0].turns[1].turn_id, "turn_new");
        assert!(
            first.threads[0].turns[0]
                .blocks
                .iter()
                .all(|block| block.evidence_role == HistoryEvidenceRole::ContextOnly)
        );
        assert!(
            first.threads[0].turns[1]
                .blocks
                .iter()
                .all(|block| block.evidence_role == HistoryEvidenceRole::NewAnchor)
        );
        assert_eq!(
            first.threads[0].turns[1].blocks[0].block_key,
            "new-start:input:0"
        );
        assert_eq!(
            first.threads[0].turns[1].blocks[1].block_key,
            "new-start:input:1"
        );
        let new_event_ids = first.threads[0].turns[1]
            .blocks
            .iter()
            .map(|block| block.event_id.as_str())
            .collect::<Vec<_>>();
        assert!(new_event_ids.contains(&"new-tool"));
        assert!(new_event_ids.contains(&"new-assistant"));
        assert!(new_event_ids.contains(&"new-permission"));
        assert_eq!(new_event_ids.last().copied(), Some("new-terminal"));
    }

    #[test]
    fn collaborative_snapshot_is_one_parent_exchange_with_exact_child_identity() {
        let delivery_id = "delivery_mapper";
        let source = SelfImprovementSourceTurnRecord {
            id: 7,
            workspace_id: "ws_history".to_owned(),
            thread_id: "thread_history".to_owned(),
            turn_id: "turn_parent".to_owned(),
            parent_turn_created_at_unix: 1_900_000_001,
            task_delivery_id: Some(delivery_id.to_owned()),
            terminal_event_id: "delivery-terminal".to_owned(),
            terminal_at_unix: 1_900_000_007,
            created_at_unix: 1_900_000_007,
        };
        let parent_thread = history_thread(
            "thread_history",
            ThreadOriginKind::Collaborative,
            ThreadSidebarVisibility::Visible,
        );
        let parent_turn = history_turn(
            "turn_parent",
            TurnStatus::InProgress,
            TurnKind::Conversation,
            TurnOrigin::User,
        );
        let child_thread = history_thread(
            "thread_child",
            ThreadOriginKind::TaskRun,
            ThreadSidebarVisibility::Hidden,
        );
        let child_turn = history_turn(
            "turn_child",
            TurnStatus::InProgress,
            TurnKind::Conversation,
            TurnOrigin::User,
        );
        let delivery_item = TurnItem::AgentMessage {
            id: pioneer_protocol::task_delivery_result_item_id(delivery_id),
            text: "Verified checksum and published safely.".to_owned(),
            phase: AgentMessagePhase::FinalAnswer,
            markdown: None,
            markdown_version: None,
        };
        let records = vec![
            record_in(
                "parent-start",
                "thread_history",
                "turn_parent",
                1,
                CanonicalTurnEventPayload::TurnStarted(CanonicalTurnStartedEventPayload {
                    thread: parent_thread,
                    sandbox_mode: SandboxMode::FullAccess,
                    turn: parent_turn.clone(),
                    input: vec![UserInput::Text {
                        text: "Publish after checksum verification.".to_owned(),
                        text_elements: Vec::new(),
                    }],
                    reasoning_effort: None,
                }),
            ),
            record_in(
                "parent-admitted",
                "thread_history",
                "turn_parent",
                2,
                CanonicalTurnEventPayload::TurnCompleted(TurnCompletedNotification {
                    workspace_id: "ws_history".to_owned(),
                    thread_id: "thread_history".to_owned(),
                    turn: Turn {
                        status: TurnStatus::Completed,
                        ..parent_turn
                    },
                }),
            ),
            record_in(
                "child-start",
                "thread_child",
                "turn_child",
                1,
                CanonicalTurnEventPayload::TurnStarted(CanonicalTurnStartedEventPayload {
                    thread: child_thread,
                    sandbox_mode: SandboxMode::FullAccess,
                    turn: child_turn.clone(),
                    input: vec![UserInput::Text {
                        text: "Publish after checksum verification.".to_owned(),
                        text_elements: Vec::new(),
                    }],
                    reasoning_effort: None,
                }),
            ),
            record_in(
                "child-tool",
                "thread_child",
                "turn_child",
                2,
                CanonicalTurnEventPayload::ItemCompleted(ItemCompletedNotification {
                    workspace_id: "ws_history".to_owned(),
                    thread_id: "thread_child".to_owned(),
                    turn_id: "turn_child".to_owned(),
                    item: TurnItem::DynamicToolCall {
                        id: "tool-child".to_owned(),
                        tool_name: "request_tools".to_owned(),
                        arguments: json!({
                            "domains": ["memory"],
                            "reason": "Verify causal tool mapping."
                        }),
                        status: ToolCallStatus::Completed,
                        recovery_policy: None,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("request_tools"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::Metadata {
                            metadata: ToolMetadata::from_json(json!({"verified": true})),
                        },
                        recovery: None,
                        success: Some(true),
                        outcome: None,
                        observation: None,
                    },
                }),
            ),
            record_in(
                "child-terminal",
                "thread_child",
                "turn_child",
                3,
                CanonicalTurnEventPayload::TurnCompleted(TurnCompletedNotification {
                    workspace_id: "ws_history".to_owned(),
                    thread_id: "thread_child".to_owned(),
                    turn: Turn {
                        status: TurnStatus::Completed,
                        ..child_turn
                    },
                }),
            ),
            record_in(
                "delivery-start",
                "thread_history",
                "run_parent",
                1,
                CanonicalTurnEventPayload::ItemStarted(ItemStartedNotification {
                    workspace_id: "ws_history".to_owned(),
                    thread_id: "thread_history".to_owned(),
                    turn_id: "run_parent".to_owned(),
                    item: delivery_item.clone(),
                }),
            ),
            record_in(
                "delivery-terminal",
                "thread_history",
                "run_parent",
                2,
                CanonicalTurnEventPayload::ItemCompleted(ItemCompletedNotification {
                    workspace_id: "ws_history".to_owned(),
                    thread_id: "thread_history".to_owned(),
                    turn_id: "run_parent".to_owned(),
                    item: delivery_item,
                }),
            ),
        ];

        let snapshot = build_model_safe_full_thread_snapshot(&frozen_range(6, &[source]), &records)
            .expect("Collaborative causal exchange must map");
        assert_eq!(snapshot.threads.len(), 1);
        assert_eq!(snapshot.threads[0].thread_id, "thread_history");
        assert_eq!(snapshot.threads[0].turns.len(), 1);
        assert_eq!(snapshot.threads[0].turns[0].turn_id, "turn_parent");
        let blocks = &snapshot.threads[0].turns[0].blocks;
        assert!(
            blocks
                .iter()
                .all(|block| block.evidence_role == HistoryEvidenceRole::NewAnchor)
        );
        assert!(blocks.iter().any(|block| {
            block.event_id == "child-tool"
                && block.event_thread_id == "thread_child"
                && block.event_turn_id == "turn_child"
        }));
        assert!(blocks.iter().any(|block| {
            block.event_id == "delivery-terminal"
                && block.event_thread_id == "thread_history"
                && block.event_turn_id == "run_parent"
        }));
        assert!(
            blocks
                .iter()
                .all(|block| block.event_id != "parent-admitted"),
            "the early Collaborative admission completion is not a model-visible terminal"
        );
    }

    #[test]
    fn collaborative_failed_delivery_is_bounded_context_with_safe_terminal_error() {
        let selected_delivery_id = "delivery_mapper_selected";
        let selected_source = SelfImprovementSourceTurnRecord {
            id: 7,
            workspace_id: "ws_history".to_owned(),
            thread_id: "thread_history".to_owned(),
            turn_id: "turn_selected".to_owned(),
            parent_turn_created_at_unix: 1_900_000_002,
            task_delivery_id: Some(selected_delivery_id.to_owned()),
            terminal_event_id: "selected-delivery-terminal".to_owned(),
            terminal_at_unix: 1_900_000_010,
            created_at_unix: 1_900_000_010,
        };
        let parent_thread = history_thread(
            "thread_history",
            ThreadOriginKind::Collaborative,
            ThreadSidebarVisibility::Visible,
        );
        let failed_turn = history_turn(
            "turn_failed",
            TurnStatus::InProgress,
            TurnKind::Conversation,
            TurnOrigin::User,
        );
        let selected_turn = history_turn(
            "turn_selected",
            TurnStatus::InProgress,
            TurnKind::Conversation,
            TurnOrigin::User,
        );
        let failed_delivery = TurnItem::SystemEvent {
            id: pioneer_protocol::task_delivery_result_item_id("delivery_mapper_failed"),
            level: SystemEventLevel::Error,
            message: "provider failed with internal credential-bearing diagnostics".to_owned(),
            code: Some("provider_unavailable".to_owned()),
            details: None,
        };
        let selected_delivery = TurnItem::AgentMessage {
            id: pioneer_protocol::task_delivery_result_item_id(selected_delivery_id),
            text: "Selected exchange completed.".to_owned(),
            phase: AgentMessagePhase::FinalAnswer,
            markdown: None,
            markdown_version: None,
        };
        let records = vec![
            record(
                "failed-parent-start",
                "turn_failed",
                1,
                CanonicalTurnEventPayload::TurnStarted(CanonicalTurnStartedEventPayload {
                    thread: parent_thread.clone(),
                    sandbox_mode: SandboxMode::FullAccess,
                    turn: failed_turn.clone(),
                    input: vec![UserInput::Text {
                        text: "Attempt the earlier operation.".to_owned(),
                        text_elements: Vec::new(),
                    }],
                    reasoning_effort: None,
                }),
            ),
            record(
                "failed-parent-admitted",
                "turn_failed",
                2,
                CanonicalTurnEventPayload::TurnCompleted(TurnCompletedNotification {
                    workspace_id: "ws_history".to_owned(),
                    thread_id: "thread_history".to_owned(),
                    turn: Turn {
                        status: TurnStatus::Completed,
                        ..failed_turn
                    },
                }),
            ),
            record(
                "failed-delivery-terminal",
                "run_failed",
                1,
                CanonicalTurnEventPayload::ItemCompleted(ItemCompletedNotification {
                    workspace_id: "ws_history".to_owned(),
                    thread_id: "thread_history".to_owned(),
                    turn_id: "run_failed".to_owned(),
                    item: failed_delivery,
                }),
            ),
            record(
                "selected-parent-start",
                "turn_selected",
                1,
                CanonicalTurnEventPayload::TurnStarted(CanonicalTurnStartedEventPayload {
                    thread: parent_thread,
                    sandbox_mode: SandboxMode::FullAccess,
                    turn: selected_turn.clone(),
                    input: vec![UserInput::Text {
                        text: "Attempt the later operation.".to_owned(),
                        text_elements: Vec::new(),
                    }],
                    reasoning_effort: None,
                }),
            ),
            record(
                "selected-parent-admitted",
                "turn_selected",
                2,
                CanonicalTurnEventPayload::TurnCompleted(TurnCompletedNotification {
                    workspace_id: "ws_history".to_owned(),
                    thread_id: "thread_history".to_owned(),
                    turn: Turn {
                        status: TurnStatus::Completed,
                        ..selected_turn
                    },
                }),
            ),
            record(
                "selected-delivery-terminal",
                "run_selected",
                1,
                CanonicalTurnEventPayload::ItemCompleted(ItemCompletedNotification {
                    workspace_id: "ws_history".to_owned(),
                    thread_id: "thread_history".to_owned(),
                    turn_id: "run_selected".to_owned(),
                    item: selected_delivery,
                }),
            ),
        ];

        let snapshot =
            build_model_safe_full_thread_snapshot(&frozen_range(6, &[selected_source]), &records)
                .expect("failed Collaborative exchange before the selected boundary must map");
        assert_eq!(snapshot.threads[0].turns.len(), 2);
        let failed = &snapshot.threads[0].turns[0];
        assert_eq!(failed.turn_id, "turn_failed");
        assert!(
            failed
                .blocks
                .iter()
                .all(|block| block.evidence_role == HistoryEvidenceRole::ContextOnly)
        );
        assert!(failed.blocks.iter().any(|block| {
            matches!(
                &block.content,
                SelfImprovementHistoryContent::Terminal {
                    status,
                    error: Some(error),
                } if status == "failed" && error == "provider_unavailable"
            )
        }));
        assert!(
            !serde_json::to_string(&snapshot)
                .expect("history snapshot must encode")
                .contains("credential-bearing diagnostics"),
            "raw internal delivery errors must not enter model-visible history"
        );
        assert!(
            snapshot.threads[0].turns[1]
                .blocks
                .iter()
                .all(|block| block.evidence_role == HistoryEvidenceRole::NewAnchor)
        );
    }

    #[test]
    fn private_turn_fields_never_enter_the_snapshot() {
        let secret = "provider-secret-sentinel";
        let sources = [source(7, "turn_new", "new-terminal")];
        let snapshot = build_model_safe_full_thread_snapshot(
            &frozen_range(6, &sources),
            &history_records(secret),
        )
        .expect("history should map");
        let encoded = serde_json::to_string(&snapshot).expect("snapshot should encode");
        assert!(!encoded.contains(secret));
        assert!(!encoded.contains("hidden-model"));
        assert!(!encoded.contains("hidden-provider"));
        assert!(!encoded.contains("prompt_manifest"));
        assert!(!encoded.contains("internal_preflight"));
        assert!(encoded.contains("artifact-a"));
        assert!(encoded.contains("visible_tool"));
        assert!(encoded.contains("visible assistant procedure"));
        assert!(encoded.contains("decision_denied"));
        assert!(encoded.contains("sandbox_denied"));
        assert!(encoded.contains("visible interruption"));
        assert!(encoded.contains("interrupted"));
        assert!(encoded.contains("[redacted]"));
        assert!(encoded.contains("new_anchor"));
    }

    #[test]
    fn full_snapshot_rejects_missing_anchor_and_planner_keeps_oversized_tail() {
        let sources = [source(7, "turn_new", "new-terminal")];
        let frozen_range = frozen_range(6, &sources);
        let mut missing = history_records("secret");
        missing.retain(|record| record.turn_id != "turn_new");
        assert!(
            build_model_safe_full_thread_snapshot(&frozen_range, &missing)
                .unwrap_err()
                .to_string()
                .contains("every selected anchor terminal boundary")
        );

        let mut oversized = history_records("secret");
        let CanonicalTurnEventPayload::TurnStarted(payload) = &mut oversized[2].payload else {
            panic!("fixture must be turn/start");
        };
        payload.input = vec![UserInput::Text {
            text: "x".repeat(HISTORY_CHUNK_MAX_SERIALIZED_BYTES.saturating_mul(3)),
            text_elements: Vec::new(),
        }];
        let snapshot = build_model_safe_full_thread_snapshot(&frozen_range, &oversized)
            .expect("full-thread snapshot must not impose the later chunk size limit");
        assert_eq!(snapshot.threads[0].turns[1].blocks[0].event_id, "new-start");
        let chunks = plan_history_chunks(&snapshot, HistoryChunkLimits::default())
            .expect("deterministic planner must preserve the oversized tail");
        assert!(chunks.len() > 1);
        let reconstructed = chunks
            .iter()
            .flat_map(|chunk| chunk.threads.iter())
            .flat_map(|thread| thread.turns.iter())
            .flat_map(|turn| turn.blocks.iter())
            .filter(|block| block.event_id == "new-start")
            .filter_map(|block| match &block.content {
                SelfImprovementHistoryContent::UserText { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        assert_eq!(
            reconstructed.len(),
            HISTORY_CHUNK_MAX_SERIALIZED_BYTES.saturating_mul(3)
        );
    }

    #[test]
    fn full_snapshot_rejects_cross_workspace_and_reopened_turn_streams() {
        let sources = [source(7, "turn_new", "new-terminal")];
        let frozen_range = frozen_range(6, &sources);

        let mut cross_workspace = history_records("secret");
        let CanonicalTurnEventPayload::TurnStarted(payload) = &mut cross_workspace[0].payload
        else {
            panic!("fixture must begin with turn/start");
        };
        payload.thread.workspace_id = "ws_other".to_owned();
        assert!(
            build_model_safe_full_thread_snapshot(&frozen_range, &cross_workspace)
                .unwrap_err()
                .to_string()
                .contains("mismatched payload identity")
        );

        let mut reopened_turn = history_records("secret");
        let old_terminal = reopened_turn.remove(1);
        reopened_turn.insert(reopened_turn.len() - 1, old_terminal);
        assert!(
            build_model_safe_full_thread_snapshot(&frozen_range, &reopened_turn)
                .unwrap_err()
                .to_string()
                .contains("before the previous exchange closed")
        );

        let mut after_boundary = history_records("secret");
        after_boundary.push(record(
            "after-boundary-hidden",
            "turn_new",
            8,
            CanonicalTurnEventPayload::ItemCompleted(ItemCompletedNotification {
                workspace_id: "ws_history".to_owned(),
                thread_id: "thread_history".to_owned(),
                turn_id: "turn_new".to_owned(),
                item: TurnItem::SystemEvent {
                    id: "hidden-after-boundary".to_owned(),
                    level: SystemEventLevel::Info,
                    message: "must never be observed".to_owned(),
                    code: None,
                    details: None,
                },
            }),
        ));
        assert!(
            build_model_safe_full_thread_snapshot(&frozen_range, &after_boundary)
                .unwrap_err()
                .to_string()
                .contains("after the frozen terminal boundary")
        );
    }

    #[test]
    fn chunk_plan_is_deterministic_bounded_and_complete() {
        let sources = [source(7, "turn_new", "new-terminal")];
        let frozen_range = frozen_range(6, &sources);
        let snapshot =
            build_model_safe_full_thread_snapshot(&frozen_range, &history_records("secret"))
                .expect("snapshot must map");
        let first =
            plan_history_chunks(&snapshot, HistoryChunkLimits::default()).expect("must plan");
        let second =
            plan_history_chunks(&snapshot, HistoryChunkLimits::default()).expect("must repeat");

        assert_eq!(first, second);
        assert!(!first.is_empty());
        for (index, chunk) in first.iter().enumerate() {
            assert_eq!(chunk.chunk_index, index as u32);
            assert_eq!(chunk.chunk_count, first.len() as u32);
            assert_eq!(chunk.fingerprint.len(), 64);
            let encoded = serde_json::to_vec(chunk).expect("chunk must encode");
            assert!(encoded.len() <= HISTORY_CHUNK_MAX_SERIALIZED_BYTES);
            assert!(
                conservative_token_upper_bound(encoded.as_slice())
                    <= HISTORY_CHUNK_MAX_TOKEN_UPPER_BOUND
            );
        }

        let planned_blocks = first
            .iter()
            .flat_map(|chunk| &chunk.threads)
            .flat_map(|thread| &thread.turns)
            .flat_map(|turn| &turn.blocks)
            .collect::<Vec<_>>();
        let source_blocks = snapshot
            .threads
            .iter()
            .flat_map(|thread| &thread.turns)
            .flat_map(|turn| &turn.blocks)
            .collect::<Vec<_>>();
        assert_eq!(planned_blocks, source_blocks);

        let exact_limit = history_chunk_probe_encoded_len(&snapshot, snapshot.threads.as_slice())
            .expect("probe must encode");
        let exact = plan_history_chunks(
            &snapshot,
            HistoryChunkLimits {
                max_serialized_bytes: exact_limit,
                max_token_upper_bound: exact_limit,
            },
        )
        .expect("an exact-boundary chunk must fit");
        assert_eq!(exact.len(), 1);
        let below_exact = plan_history_chunks(
            &snapshot,
            HistoryChunkLimits {
                max_serialized_bytes: exact_limit - 1,
                max_token_upper_bound: exact_limit - 1,
            },
        );
        assert!(
            below_exact.is_err()
                || below_exact
                    .as_ref()
                    .is_ok_and(|chunks| chunks.len() > exact.len())
        );
    }

    #[test]
    fn chunk_plan_prefers_turn_then_event_boundaries() {
        let sources = [source(7, "turn_new", "new-terminal")];
        let frozen_range = frozen_range(6, &sources);
        let mut records = history_records("secret");
        let CanonicalTurnEventPayload::TurnStarted(old) = &mut records[0].payload else {
            panic!("old turn must begin with turn/start");
        };
        old.input = vec![UserInput::Text {
            text: "a".repeat(38 * 1024),
            text_elements: Vec::new(),
        }];
        let CanonicalTurnEventPayload::TurnStarted(new) = &mut records[2].payload else {
            panic!("new turn must begin with turn/start");
        };
        new.input = vec![UserInput::Text {
            text: "b".repeat(38 * 1024),
            text_elements: Vec::new(),
        }];
        let snapshot = build_model_safe_full_thread_snapshot(&frozen_range, &records)
            .expect("large snapshot must map");
        let turn_plan =
            plan_history_chunks(&snapshot, HistoryChunkLimits::default()).expect("must plan");
        assert!(turn_plan.len() >= 2);
        assert!(
            turn_plan
                .iter()
                .flat_map(|chunk| &chunk.threads)
                .flat_map(|thread| &thread.turns)
                .flat_map(|turn| &turn.blocks)
                .all(|block| block.fragment_count == 1)
        );
        assert_ne!(
            turn_plan[0].threads[0].turns[0].turn_id,
            turn_plan[1].threads[0].turns[0].turn_id
        );

        let mut records = history_records("secret");
        let CanonicalTurnEventPayload::TurnStarted(new) = &mut records[2].payload else {
            panic!("new turn must begin with turn/start");
        };
        new.input = vec![UserInput::Text {
            text: "c".repeat(35 * 1024),
            text_elements: Vec::new(),
        }];
        let CanonicalTurnEventPayload::ItemCompleted(assistant) = &mut records[6].payload else {
            panic!("fixture must contain assistant event");
        };
        let TurnItem::AgentMessage { text, .. } = &mut assistant.item else {
            panic!("fixture item must be assistant text");
        };
        *text = "d".repeat(35 * 1024);
        let snapshot = build_model_safe_full_thread_snapshot(&frozen_range, &records)
            .expect("large-event snapshot must map");
        let event_plan =
            plan_history_chunks(&snapshot, HistoryChunkLimits::default()).expect("must plan");
        assert!(event_plan.len() >= 2);
        let new_turn_chunks = event_plan
            .iter()
            .filter(|chunk| {
                chunk
                    .threads
                    .iter()
                    .flat_map(|thread| &thread.turns)
                    .any(|turn| turn.turn_id == "turn_new")
            })
            .count();
        assert!(new_turn_chunks >= 2);
        assert!(
            event_plan
                .iter()
                .flat_map(|chunk| &chunk.threads)
                .flat_map(|thread| &thread.turns)
                .flat_map(|turn| &turn.blocks)
                .all(|block| block.fragment_count == 1)
        );
    }

    #[test]
    fn chunk_plan_fragments_utf8_text_without_losing_the_tail() {
        let sources = [source(7, "turn_new", "new-terminal")];
        let frozen_range = frozen_range(6, &sources);
        let expected = "аб🙂終".repeat(30_000);
        let mut records = history_records("secret");
        let CanonicalTurnEventPayload::TurnStarted(payload) = &mut records[2].payload else {
            panic!("new turn must begin with turn/start");
        };
        payload.input = vec![UserInput::Text {
            text: expected.clone(),
            text_elements: Vec::new(),
        }];
        let snapshot = build_model_safe_full_thread_snapshot(&frozen_range, &records)
            .expect("very long UTF-8 snapshot must map");
        let plan =
            plan_history_chunks(&snapshot, HistoryChunkLimits::default()).expect("must fragment");
        assert!(plan.len() > 1);

        let fragments = plan
            .iter()
            .flat_map(|chunk| &chunk.threads)
            .flat_map(|thread| &thread.turns)
            .flat_map(|turn| &turn.blocks)
            .filter(|block| block.event_id == "new-start" && block.input_index == Some(0))
            .collect::<Vec<_>>();
        assert!(fragments.len() > 1);
        assert_eq!(
            fragments
                .iter()
                .map(|block| block.fragment_index)
                .collect::<Vec<_>>(),
            (0..fragments.len() as u32).collect::<Vec<_>>()
        );
        assert!(
            fragments
                .iter()
                .all(|block| block.fragment_count == fragments.len() as u32)
        );
        let reconstructed = fragments
            .iter()
            .map(|block| match &block.content {
                SelfImprovementHistoryContent::UserText { text } => text.as_str(),
                _ => panic!("fragmented block must remain user text"),
            })
            .collect::<String>();
        assert_eq!(reconstructed, expected);
        assert!(!reconstructed.is_empty());
        assert_eq!(
            fragments.last().unwrap().fragment_index + 1,
            fragments.last().unwrap().fragment_count
        );
    }

    #[test]
    fn chunk_plan_fails_closed_for_empty_and_oversized_metadata() {
        let sources = [source(7, "turn_new", "new-terminal")];
        let frozen_range = frozen_range(6, &sources);
        let mut snapshot =
            build_model_safe_full_thread_snapshot(&frozen_range, &history_records("secret"))
                .expect("snapshot must map");
        let mut empty = snapshot.clone();
        empty.threads.clear();
        assert!(
            plan_history_chunks(&empty, HistoryChunkLimits::default())
                .unwrap_err()
                .to_string()
                .contains("planner input is invalid")
        );

        let tool = snapshot
            .threads
            .iter_mut()
            .flat_map(|thread| &mut thread.turns)
            .flat_map(|turn| &mut turn.blocks)
            .find(|block| block.event_id == "new-tool")
            .expect("tool block must exist");
        let SelfImprovementHistoryContent::Tool { metadata, .. } = &mut tool.content else {
            panic!("fixture must map a tool");
        };
        *metadata = json!({"indivisible": "x".repeat(HISTORY_CHUNK_MAX_SERIALIZED_BYTES * 2)});
        assert!(
            plan_history_chunks(&snapshot, HistoryChunkLimits::default())
                .unwrap_err()
                .to_string()
                .contains("oversized indivisible metadata")
        );
    }

    #[test]
    fn chunk_fingerprint_changes_with_content_identity_only() {
        let sources = [source(7, "turn_new", "new-terminal")];
        let frozen_range = frozen_range(6, &sources);
        let baseline =
            build_model_safe_full_thread_snapshot(&frozen_range, &history_records("secret"))
                .expect("snapshot must map");
        let baseline_plan =
            plan_history_chunks(&baseline, HistoryChunkLimits::default()).expect("must plan");
        let repeat =
            plan_history_chunks(&baseline, HistoryChunkLimits::default()).expect("must repeat");
        assert_eq!(baseline_plan, repeat);

        let mut changed = baseline;
        let SelfImprovementHistoryContent::UserText { text } =
            &mut changed.threads[0].turns[0].blocks[0].content
        else {
            panic!("fixture must begin with user text");
        };
        text.push('!');
        let changed_plan =
            plan_history_chunks(&changed, HistoryChunkLimits::default()).expect("must replan");
        assert_ne!(baseline_plan[0].fingerprint, changed_plan[0].fingerprint);
    }
}
