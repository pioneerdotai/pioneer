use super::{
    TimelinePendingRequestRow, TimelineRenderModel, TimelineRenderRow,
    model::{TimelineRow, TimelineRowKind, TurnWorkGroupRow},
};
use crate::app::{
    conversation::{ConversationViewState, ItemView, TimelineEntry, TimelineEntryStatus},
    root::{CLIRuntimePendingRequestEntry, PioneerDesktop},
};
use pioneer_client::timeline::{
    labels::RunningTurnDisplay,
    semantic::{self, SemanticTimelineRow, SemanticTimelineRowId, SemanticTimelineRowKind},
};
use pioneer_protocol::{
    AgentMessagePhase, CLIRuntimePendingRequestStatus, TimelineBlock, TimelineBlockKind, TurnItem,
    TurnItemType, TurnWorkBlock, TurnWorkItem, TurnWorkItemStatus, TurnWorkPresentation,
    TurnWorkState,
};
use std::{
    collections::{HashMap, HashSet},
    rc::Rc,
};

pub(in crate::app::thread::view::timeline) const SEMANTIC_TURN_WORK_GROUP_PREFIX: &str =
    "semantic-turn-work-group::";

impl PioneerDesktop {
    pub(crate) fn semantic_timeline_render_model(
        &self,
        active_thread_id: Option<&str>,
    ) -> TimelineRenderModel {
        let Some(active_thread_id) = active_thread_id else {
            return TimelineRenderModel::empty();
        };

        {
            let state = self.thread_timeline_view_state.borrow();
            if state.cached_semantic_model_active_thread_id.as_deref() == Some(active_thread_id)
                && state.cached_semantic_model_revision == self.semantic_timeline_revision
                && let Some(model) = state.cached_semantic_model.as_ref()
            {
                return model.clone();
            }
        }

        let Some(flattened) =
            semantic::flatten_semantic_timeline(&self.semantic_timelines, active_thread_id)
        else {
            return TimelineRenderModel::empty();
        };
        let semantic_rows = Rc::new(flattened);

        let live_work_started_at = live_work_started_at_by_turn(semantic_rows.rows.as_slice());
        let mut inserted_running_rows = HashSet::<String>::new();
        let mut projection = ConversationViewState::default();
        let mut rows = Vec::<TimelineRenderRow>::new();
        let mut semantic_row_ids = HashMap::<String, SemanticTimelineRowId>::new();

        for (index, row) in semantic_rows.rows.iter().enumerate() {
            let rows_before = rows.len();
            push_semantic_row(&mut projection, &mut rows, row);
            for render_row in &rows[rows_before..] {
                semantic_row_ids.insert(render_row.key().to_owned(), row.id.clone());
            }

            let current_turn_id = semantic_row_turn_id(row);
            let next_turn_id = semantic_rows
                .rows
                .get(index + 1)
                .and_then(semantic_row_turn_id);
            if let Some(turn_id) = current_turn_id
                && next_turn_id != Some(turn_id)
                && let Some(started_at_unix_ms) = live_work_started_at.get(turn_id).copied()
                && inserted_running_rows.insert(turn_id.to_owned())
            {
                let running_key = format!("semantic-running-turn::{turn_id}");
                rows.push(TimelineRenderRow::Timeline(TimelineRow {
                    key: running_key.clone(),
                    kind: TimelineRowKind::RunningTurn(RunningTurnDisplay {
                        turn_id: turn_id.to_owned(),
                        started_at_unix_ms,
                    }),
                }));
                semantic_row_ids.insert(running_key, row.id.clone());
            }
        }

        projection.revision = self.semantic_timeline_revision;

        let model = TimelineRenderModel {
            projection: Rc::new(projection),
            rows: Rc::new(rows),
            semantic_row_ids: Rc::new(semantic_row_ids),
            semantic_rows,
        };

        {
            let mut state = self.thread_timeline_view_state.borrow_mut();
            state.cached_semantic_model_active_thread_id = Some(active_thread_id.to_owned());
            state.cached_semantic_model_revision = self.semantic_timeline_revision;
            state.cached_semantic_model = Some(model.clone());
        }

        model
    }
}

fn push_semantic_row(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRenderRow>,
    row: &SemanticTimelineRow,
) {
    match &row.kind {
        SemanticTimelineRowKind::UserBlock { block } => push_user_block(projection, rows, block),
        SemanticTimelineRowKind::WorkHeader {
            block,
            work,
            expanded,
            ..
        } => push_work_header(rows, block, work, *expanded),
        SemanticTimelineRowKind::WorkItem { item } => push_work_item(projection, rows, item),
        SemanticTimelineRowKind::AssistantMessage { block } => {
            push_assistant_block(projection, rows, block);
        }
        SemanticTimelineRowKind::PendingRequest { block } => {
            if let Some(row) = pending_request_row_from_block(block) {
                rows.push(row);
            }
        }
        SemanticTimelineRowKind::TurnState { block } => push_turn_state(rows, block),
    }
}

fn push_user_block(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRenderRow>,
    block: &TimelineBlock,
) {
    let TimelineBlockKind::UserMessage {
        item_id,
        text,
        attachments,
        ..
    } = &block.kind
    else {
        return;
    };
    let item_id = item_id.as_deref().unwrap_or(block.block_id.as_str());
    let turn_id = block.turn_id.as_deref().unwrap_or(block.block_id.as_str());
    let item = TurnItem::UserMessage {
        id: item_id.to_owned(),
        text: text.clone(),
        attachments: attachments.clone(),
    };
    push_item_row(
        projection,
        rows,
        ItemRowInput {
            entry_id: block.block_id.clone(),
            item_id: item_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item_type: "user_message".to_owned(),
            status: TimelineEntryStatus::Completed,
            started_at_unix_ms: block.started_at_unix_ms.or(block.updated_at_unix_ms),
            updated_at_unix_ms: block.updated_at_unix_ms.or(block.started_at_unix_ms),
            completed_at_unix_ms: block.updated_at_unix_ms.or(block.started_at_unix_ms),
            partial_text: text.clone(),
            final_text: Some(text.clone()),
            partial_markdown: None,
            final_markdown: None,
            item,
            opaque_meta: None,
        },
    );
}

fn push_assistant_block(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRenderRow>,
    block: &TimelineBlock,
) {
    let TimelineBlockKind::AssistantMessage {
        item_id,
        text,
        status,
        markdown,
    } = &block.kind
    else {
        return;
    };
    let turn_id = block.turn_id.as_deref().unwrap_or(block.block_id.as_str());
    let item = TurnItem::AgentMessage {
        id: item_id.clone(),
        text: text.clone(),
        phase: AgentMessagePhase::FinalAnswer,
        markdown: markdown.clone(),
        markdown_version: None,
    };
    push_item_row(
        projection,
        rows,
        ItemRowInput {
            entry_id: block.block_id.clone(),
            item_id: item_id.clone(),
            turn_id: turn_id.to_owned(),
            item_type: "agent_message".to_owned(),
            status: work_item_status(*status),
            started_at_unix_ms: block.started_at_unix_ms.or(block.updated_at_unix_ms),
            updated_at_unix_ms: block.updated_at_unix_ms.or(block.started_at_unix_ms),
            completed_at_unix_ms: if is_terminal_work_status(*status) {
                block.updated_at_unix_ms.or(block.started_at_unix_ms)
            } else {
                None
            },
            partial_text: text.clone(),
            final_text: is_terminal_work_status(*status).then(|| text.clone()),
            partial_markdown: markdown.clone(),
            final_markdown: is_terminal_work_status(*status)
                .then(|| markdown.clone())
                .flatten(),
            item,
            opaque_meta: None,
        },
    );
}

fn push_work_header(
    rows: &mut Vec<TimelineRenderRow>,
    block: &TimelineBlock,
    work: &TurnWorkBlock,
    expanded: bool,
) {
    if work.presentation != TurnWorkPresentation::CollapsedAfterFinal || work.work_count == 0 {
        return;
    }
    let toggle_key = semantic_turn_work_toggle_key(work.turn_id.as_str());
    rows.push(TimelineRenderRow::Timeline(TimelineRow {
        key: toggle_key.clone(),
        kind: TimelineRowKind::TurnWorkToggle(TurnWorkGroupRow {
            toggle_key,
            anchor_entry_id: block.block_id.clone(),
            elapsed_ms: work.elapsed_ms,
            is_open: expanded,
        }),
    }));
}

fn push_work_item(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRenderRow>,
    item: &TurnWorkItem,
) {
    let (text, markdown) = turn_item_text_and_markdown(&item.item);
    push_item_row(
        projection,
        rows,
        ItemRowInput {
            entry_id: item.work_item_id.clone(),
            item_id: item.item_id.clone(),
            turn_id: item.turn_id.clone(),
            item_type: turn_item_type_label(item.item_type).to_owned(),
            status: work_item_status(item.status),
            started_at_unix_ms: item.started_at_unix_ms.or(item.completed_at_unix_ms),
            updated_at_unix_ms: item.completed_at_unix_ms.or(item.started_at_unix_ms),
            completed_at_unix_ms: completed_at_for_status(item.status, item.completed_at_unix_ms),
            partial_text: text.clone(),
            final_text: is_terminal_work_status(item.status).then_some(text.clone()),
            partial_markdown: markdown.clone(),
            final_markdown: is_terminal_work_status(item.status)
                .then(|| markdown.clone())
                .flatten(),
            item: item.item.clone(),
            opaque_meta: item.metadata.clone(),
        },
    );
}

fn push_turn_state(rows: &mut Vec<TimelineRenderRow>, block: &TimelineBlock) {
    let TimelineBlockKind::TurnState { state, .. } = &block.kind else {
        return;
    };
    if !matches!(state, TurnWorkState::Starting | TurnWorkState::Running) {
        return;
    }
    let Some(turn_id) = block.turn_id.as_deref() else {
        return;
    };
    rows.push(TimelineRenderRow::Timeline(TimelineRow {
        key: format!("semantic-turn-state::{turn_id}::{}", block.block_id),
        kind: TimelineRowKind::RunningTurn(RunningTurnDisplay {
            turn_id: turn_id.to_owned(),
            started_at_unix_ms: block.started_at_unix_ms.or(block.updated_at_unix_ms),
        }),
    }));
}

struct ItemRowInput {
    entry_id: String,
    item_id: String,
    turn_id: String,
    item_type: String,
    status: TimelineEntryStatus,
    started_at_unix_ms: Option<i64>,
    updated_at_unix_ms: Option<i64>,
    completed_at_unix_ms: Option<i64>,
    partial_text: String,
    final_text: Option<String>,
    partial_markdown: Option<pioneer_protocol::MarkdownDocument>,
    final_markdown: Option<pioneer_protocol::MarkdownDocument>,
    item: TurnItem,
    opaque_meta: Option<serde_json::Value>,
}

fn push_item_row(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRenderRow>,
    input: ItemRowInput,
) {
    let item_index = projection.items.len();
    let timeline_index = projection.timeline.len();
    projection.items.push(ItemView {
        id: input.item_id.clone(),
        turn_id: input.turn_id.clone(),
        item_type: input.item_type,
        status: input.status,
        started_at_unix_ms: input.started_at_unix_ms,
        updated_at_unix_ms: input.updated_at_unix_ms,
        completed_at_unix_ms: input.completed_at_unix_ms,
        partial_text: input.partial_text,
        final_text: input.final_text,
        partial_markdown: input.partial_markdown,
        final_markdown: input.final_markdown,
        item: input.item,
        timeline_origin: None,
        opaque_meta: input.opaque_meta,
    });
    projection.timeline.push(TimelineEntry {
        id: input.entry_id.clone(),
        turn_id: input.turn_id,
        item_id: input.item_id,
        item_index,
    });
    rows.push(TimelineRenderRow::Timeline(TimelineRow {
        key: input.entry_id,
        kind: TimelineRowKind::Item { timeline_index },
    }));
}

fn pending_request_row_from_block(block: &TimelineBlock) -> Option<TimelineRenderRow> {
    let TimelineBlockKind::PendingRequest {
        runtime_id,
        request_id,
        status,
        item_id,
        request,
    } = &block.kind
    else {
        return None;
    };
    if *status != CLIRuntimePendingRequestStatus::Pending {
        return None;
    }
    Some(TimelineRenderRow::PendingRequest(
        TimelinePendingRequestRow {
            key: format!("timeline-cli-runtime-request::{request_id}"),
            entry: CLIRuntimePendingRequestEntry {
                workspace_id: block.workspace_id.clone(),
                runtime_id: runtime_id.clone(),
                request_id: request_id.clone(),
                thread_id: Some(block.thread_id.clone()),
                turn_id: block.turn_id.clone(),
                item_id: item_id.clone(),
                request: request.clone(),
            },
        },
    ))
}

fn live_work_started_at_by_turn(rows: &[SemanticTimelineRow]) -> HashMap<String, Option<i64>> {
    rows.iter()
        .filter_map(|row| {
            let SemanticTimelineRowKind::WorkHeader { work, .. } = &row.kind else {
                return None;
            };
            if work.presentation != TurnWorkPresentation::ExpandedLive {
                return None;
            }
            if !matches!(work.state, TurnWorkState::Starting | TurnWorkState::Running) {
                return None;
            }
            Some((work.turn_id.clone(), work.started_at_unix_ms))
        })
        .collect()
}

fn semantic_row_turn_id(row: &SemanticTimelineRow) -> Option<&str> {
    match &row.kind {
        SemanticTimelineRowKind::WorkHeader { work, .. } => Some(work.turn_id.as_str()),
        SemanticTimelineRowKind::WorkItem { item } => Some(item.turn_id.as_str()),
        SemanticTimelineRowKind::UserBlock { block }
        | SemanticTimelineRowKind::AssistantMessage { block }
        | SemanticTimelineRowKind::PendingRequest { block }
        | SemanticTimelineRowKind::TurnState { block } => block.turn_id.as_deref(),
    }
}

fn semantic_turn_work_toggle_key(turn_id: &str) -> String {
    format!("{SEMANTIC_TURN_WORK_GROUP_PREFIX}{turn_id}")
}

fn work_item_status(status: TurnWorkItemStatus) -> TimelineEntryStatus {
    match status {
        TurnWorkItemStatus::Running => TimelineEntryStatus::Running,
        TurnWorkItemStatus::Completed => TimelineEntryStatus::Completed,
        TurnWorkItemStatus::Blocked => TimelineEntryStatus::Blocked,
        TurnWorkItemStatus::Failed => TimelineEntryStatus::Failed,
        TurnWorkItemStatus::Cancelled => TimelineEntryStatus::Cancelled,
    }
}

fn is_terminal_work_status(status: TurnWorkItemStatus) -> bool {
    !matches!(status, TurnWorkItemStatus::Running)
}

fn completed_at_for_status(
    status: TurnWorkItemStatus,
    completed_at_unix_ms: Option<i64>,
) -> Option<i64> {
    is_terminal_work_status(status)
        .then_some(completed_at_unix_ms)
        .flatten()
}

fn turn_item_type_label(item_type: TurnItemType) -> &'static str {
    match item_type {
        TurnItemType::UserMessage => "user_message",
        TurnItemType::AgentMessage => "agent_message",
        TurnItemType::Reasoning => "reasoning",
        TurnItemType::SystemEvent => "system_event",
        TurnItemType::Task => "task",
        TurnItemType::CommandExecution => "command_execution",
        TurnItemType::FileChange => "file_change",
        TurnItemType::WebSearch => "web_search",
        TurnItemType::WebFetch => "web_fetch",
        TurnItemType::Download => "download",
        TurnItemType::DynamicToolCall => "dynamic_tool_call",
    }
}

fn turn_item_text_and_markdown(
    item: &TurnItem,
) -> (String, Option<pioneer_protocol::MarkdownDocument>) {
    match item {
        TurnItem::UserMessage { text, .. } => (text.clone(), None),
        TurnItem::AgentMessage { text, markdown, .. } => (text.clone(), markdown.clone()),
        TurnItem::Reasoning {
            summary, content, ..
        } => {
            let text = if content.is_empty() {
                summary.join("\n")
            } else {
                content.join("\n")
            };
            (text, None)
        }
        TurnItem::SystemEvent { message, .. } => (message.clone(), None),
        _ => (String::new(), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{MarkdownDocument, TimelineBlock, TimelineCursor};

    #[test]
    fn assistant_block_preserves_final_markdown_in_desktop_projection() {
        let markdown = MarkdownDocument::from_plain_text("final markdown");
        let block = assistant_block(Some(markdown.clone()));
        let mut projection = ConversationViewState::default();
        let mut rows = Vec::new();

        push_assistant_block(&mut projection, &mut rows, &block);

        assert_eq!(rows.len(), 1);
        assert_eq!(projection.items.len(), 1);
        assert_eq!(
            projection.items[0].final_text.as_deref(),
            Some("final **markdown**")
        );
        assert_eq!(projection.items[0].partial_markdown, Some(markdown.clone()));
        assert_eq!(projection.items[0].final_markdown, Some(markdown));
    }

    #[test]
    fn work_header_toggle_is_emitted_only_for_collapsed_after_final() {
        let block = work_block(TurnWorkPresentation::CollapsedAfterFinal, 70_000);
        let work = match &block.kind {
            TimelineBlockKind::TurnWork { work } => work,
            other => panic!("expected work block, got {other:?}"),
        };
        let mut rows = Vec::new();

        push_work_header(&mut rows, &block, work, false);

        assert_eq!(rows.len(), 1);
        assert!(matches!(
            &rows[0],
            TimelineRenderRow::Timeline(TimelineRow {
                key,
                kind: TimelineRowKind::TurnWorkToggle(group),
            }) if key == "semantic-turn-work-group::turn_a"
                && group.toggle_key == "semantic-turn-work-group::turn_a"
                && !group.is_open
        ));

        let live_block = work_block(TurnWorkPresentation::ExpandedLive, 70_000);
        let live_work = match &live_block.kind {
            TimelineBlockKind::TurnWork { work } => work,
            other => panic!("expected work block, got {other:?}"),
        };
        let mut live_rows = Vec::new();
        push_work_header(&mut live_rows, &live_block, live_work, true);
        assert!(
            live_rows.is_empty(),
            "expanded live work should render work rows/running row, not a collapsed Работал toggle"
        );
    }

    #[test]
    fn turn_state_rows_render_only_starting_or_running() {
        let mut rows = Vec::new();
        push_turn_state(&mut rows, &turn_state_block(TurnWorkState::Running));
        assert_eq!(rows.len(), 1);
        assert!(matches!(
            &rows[0],
            TimelineRenderRow::Timeline(TimelineRow {
                kind: TimelineRowKind::RunningTurn(display),
                ..
            }) if display.turn_id == "turn_a"
        ));

        let mut terminal_rows = Vec::new();
        push_turn_state(
            &mut terminal_rows,
            &turn_state_block(TurnWorkState::Completed),
        );
        assert!(terminal_rows.is_empty());
    }

    fn assistant_block(markdown: Option<MarkdownDocument>) -> TimelineBlock {
        TimelineBlock {
            workspace_id: "workspace_a".to_owned(),
            thread_id: "thread_a".to_owned(),
            block_id: "block_assistant".to_owned(),
            turn_id: Some("turn_a".to_owned()),
            sort_key: "003".to_owned(),
            started_at_unix_ms: Some(3),
            updated_at_unix_ms: Some(3),
            kind: TimelineBlockKind::AssistantMessage {
                item_id: "assistant_final".to_owned(),
                text: "final **markdown**".to_owned(),
                status: TurnWorkItemStatus::Completed,
                markdown,
            },
        }
    }

    fn work_block(presentation: TurnWorkPresentation, work_count: u64) -> TimelineBlock {
        TimelineBlock {
            workspace_id: "workspace_a".to_owned(),
            thread_id: "thread_a".to_owned(),
            block_id: "block_work".to_owned(),
            turn_id: Some("turn_a".to_owned()),
            sort_key: "002".to_owned(),
            started_at_unix_ms: Some(1),
            updated_at_unix_ms: Some(2),
            kind: TimelineBlockKind::TurnWork {
                work: TurnWorkBlock {
                    turn_id: "turn_a".to_owned(),
                    presentation,
                    state: TurnWorkState::Completed,
                    started_at_unix_ms: Some(1),
                    completed_at_unix_ms: Some(2),
                    elapsed_ms: Some(1),
                    work_count,
                    visible_work_count: work_count,
                    hidden_work_count: 0,
                    has_more_before: false,
                    has_more_after: work_count > 0,
                    before_cursor: None,
                    after_cursor: Some(TimelineCursor {
                        value: "after-work".to_owned(),
                    }),
                    first_work_item_id: None,
                    last_work_item_id: None,
                },
            },
        }
    }

    fn turn_state_block(state: TurnWorkState) -> TimelineBlock {
        TimelineBlock {
            workspace_id: "workspace_a".to_owned(),
            thread_id: "thread_a".to_owned(),
            block_id: "block_state".to_owned(),
            turn_id: Some("turn_a".to_owned()),
            sort_key: "004".to_owned(),
            started_at_unix_ms: Some(4),
            updated_at_unix_ms: Some(4),
            kind: TimelineBlockKind::TurnState {
                state,
                message: None,
            },
        }
    }
}
