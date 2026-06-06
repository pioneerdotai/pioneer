//! Stable timeline layout hash helpers.

use super::{
    labels::{coalesced_tools_label, format_elapsed_ms, timeline_work_group_completed_label},
    rows::{TimelineRow, TimelineRowKind},
};
use crate::conversation::{ConversationViewState, ItemView};
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

                    let text = item_view
                        .final_text
                        .as_deref()
                        .unwrap_or(item_view.partial_text.as_str());
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
            let mut len = timeline_work_group_completed_label().len();
            if let Some(elapsed_ms) = group.elapsed_ms {
                len = len.saturating_add(format_elapsed_ms(elapsed_ms).len());
            }
            len
        }
        TimelineRowKind::CoalescedTools(group) => coalesced_tools_label(group).len(),
    }
}

pub fn timeline_row_toggle_key(row: &TimelineRow) -> Option<&str> {
    match &row.kind {
        TimelineRowKind::TurnWorkToggle(group) => Some(group.toggle_key.as_str()),
        TimelineRowKind::CoalescedTools(group) => Some(group.toggle_key.as_str()),
        TimelineRowKind::Item { .. } => None,
    }
}

fn timeline_entry_text(item_view: &ItemView) -> &str {
    item_view
        .final_text
        .as_deref()
        .unwrap_or(item_view.partial_text.as_str())
}
