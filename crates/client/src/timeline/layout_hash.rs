//! Stable timeline layout hash helpers.

use super::{
    labels::{format_elapsed_ms, timeline_entry_text},
    rows::{TimelineCoalescedToolsKind, TimelineCoalescedToolsRow, TimelineRow, TimelineRowKind},
};
use crate::conversation::ConversationViewState;
use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
};

pub fn timeline_rows_layout_hash(
    projection: &ConversationViewState,
    rows: &[TimelineRow],
    expanded: &HashSet<String>,
) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    rows.len().hash(&mut hasher);
    for row in rows {
        row.key.hash(&mut hasher);
        timeline_row_layout_hash(projection, row, expanded).hash(&mut hasher);
    }
    hasher.finish()
}

pub fn timeline_row_layout_hash(
    projection: &ConversationViewState,
    row: &TimelineRow,
    expanded: &HashSet<String>,
) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    row.key.hash(&mut hasher);

    match &row.kind {
        TimelineRowKind::TurnWorkToggle(group) => {
            1u8.hash(&mut hasher);
            group.anchor_entry_id.hash(&mut hasher);
            group.elapsed_ms.hash(&mut hasher);
            group.is_open.hash(&mut hasher);
        }
        TimelineRowKind::CoalescedTools(group) => {
            2u8.hash(&mut hasher);
            group.toggle_key.hash(&mut hasher);
            group.count.hash(&mut hasher);
            group.is_open.hash(&mut hasher);
            group.kind.hash(&mut hasher);
        }
        TimelineRowKind::RunningTurn(running_turn) => {
            3u8.hash(&mut hasher);
            running_turn.turn_id.hash(&mut hasher);
            running_turn.started_at_unix_ms.hash(&mut hasher);
        }
        TimelineRowKind::Item { timeline_index } => {
            0u8.hash(&mut hasher);
            timeline_index.hash(&mut hasher);

            if let Some(entry) = projection.timeline.get(*timeline_index) {
                entry.id.hash(&mut hasher);
                entry.turn_id.hash(&mut hasher);
                entry.item_id.hash(&mut hasher);
                entry.item_index.hash(&mut hasher);

                if let Some(item_view) = projection.item_for_timeline_entry(entry) {
                    item_view.item_type.hash(&mut hasher);
                    item_view.status.hash(&mut hasher);
                    item_view.updated_at_unix_ms.hash(&mut hasher);

                    let text = timeline_entry_text(item_view);
                    let text_bytes = text.as_bytes();
                    text_bytes.len().hash(&mut hasher);
                    text_bytes
                        .first()
                        .copied()
                        .unwrap_or_default()
                        .hash(&mut hasher);
                    text_bytes
                        .last()
                        .copied()
                        .unwrap_or_default()
                        .hash(&mut hasher);

                    if let Some(markdown) = &item_view.partial_markdown {
                        markdown.blocks.len().hash(&mut hasher);
                    }
                    if let Some(markdown) = &item_view.final_markdown {
                        markdown.blocks.len().hash(&mut hasher);
                    }
                }

                expanded.contains(entry.id.as_str()).hash(&mut hasher);
            }
        }
    }

    hasher.finish()
}

pub fn timeline_row_text_len(projection: &ConversationViewState, row: &TimelineRow) -> usize {
    match &row.kind {
        TimelineRowKind::Item { timeline_index } => projection
            .timeline
            .get(*timeline_index)
            .and_then(|entry| projection.item_for_timeline_entry(entry))
            .map(|item_view| timeline_entry_text(item_view).len())
            .unwrap_or_default(),
        TimelineRowKind::TurnWorkToggle(group) => {
            let mut len = TURN_WORK_GROUP_COMPLETED_TEXT_LEN_ESTIMATE;
            if let Some(elapsed_ms) = group.elapsed_ms {
                len = len.saturating_add(format_elapsed_ms(elapsed_ms).len());
            }
            len
        }
        TimelineRowKind::CoalescedTools(group) => coalesced_tools_text_len_estimate(group),
        TimelineRowKind::RunningTurn(_) => RUNNING_TURN_TEXT_LEN_ESTIMATE,
    }
}

const TURN_WORK_GROUP_COMPLETED_TEXT_LEN_ESTIMATE: usize = 9;
const RUNNING_TURN_TEXT_LEN_ESTIMATE: usize = 12;

fn coalesced_tools_text_len_estimate(group: &TimelineCoalescedToolsRow) -> usize {
    let count_len = group.count.to_string().len();
    let label_len = match group.kind {
        TimelineCoalescedToolsKind::CompletedTaskTools => 21,
        TimelineCoalescedToolsKind::RepeatedTaskWait => 24,
    };
    count_len.saturating_add(1).saturating_add(label_len)
}

pub fn timeline_row_toggle_key(row: &TimelineRow) -> Option<&str> {
    match &row.kind {
        TimelineRowKind::TurnWorkToggle(group) => Some(group.toggle_key.as_str()),
        TimelineRowKind::CoalescedTools(group) => Some(group.toggle_key.as_str()),
        TimelineRowKind::Item { .. } | TimelineRowKind::RunningTurn(_) => None,
    }
}
