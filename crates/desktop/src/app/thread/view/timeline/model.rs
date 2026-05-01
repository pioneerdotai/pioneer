use super::items::format_elapsed_ms;
use crate::app::conversation::{ConversationViewState, ItemView};
use pioneer_protocol::{TaskStatus, ToolCallStatus, TurnItem};
use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
};

const TIMELINE_TURN_WORK_GROUP_PREFIX: &str = "timeline-turn-work-group::";
const TIMELINE_COALESCED_TOOLS_PREFIX: &str = "timeline-coalesced-tools::";
const LIVE_TASK_VISIBLE_COMPLETED_TOOL_ROWS: usize = 2;

#[derive(Debug, Clone)]
pub(crate) struct TurnWorkGroupRow {
    pub(super) toggle_key: String,
    pub(super) anchor_entry_id: String,
    pub(super) elapsed_ms: Option<u64>,
    pub(super) is_open: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum TimelineCoalescedToolsKind {
    CompletedTaskTools,
    RepeatedTaskWait,
}

#[derive(Debug, Clone)]
pub(crate) struct TimelineCoalescedToolsRow {
    pub(super) toggle_key: String,
    pub(super) count: usize,
    pub(super) is_open: bool,
    pub(super) kind: TimelineCoalescedToolsKind,
}

#[derive(Debug, Clone)]
pub(crate) enum TimelineRowKind {
    Item { timeline_index: usize },
    TurnWorkToggle(TurnWorkGroupRow),
    CoalescedTools(TimelineCoalescedToolsRow),
}

#[derive(Debug, Clone)]
pub(crate) struct TimelineRow {
    pub(super) key: String,
    pub(super) kind: TimelineRowKind,
}

pub(super) fn timeline_turn_work_group_key(anchor_entry_id: &str) -> String {
    format!("{TIMELINE_TURN_WORK_GROUP_PREFIX}{anchor_entry_id}")
}

fn timeline_coalesced_tools_key(group_id: &str) -> String {
    format!("{TIMELINE_COALESCED_TOOLS_PREFIX}{group_id}")
}

pub(super) fn build_timeline_rows(
    projection: &ConversationViewState,
    expanded: &HashSet<String>,
) -> Vec<TimelineRow> {
    let timeline = &projection.timeline;
    if timeline.is_empty() {
        return Vec::new();
    }

    let turn_completed = projection
        .turns
        .iter()
        .map(|turn| {
            let completed = turn.completed_at_unix_ms.is_some()
                && turn.error.as_deref().is_none_or(str::is_empty);
            (turn.id.as_str(), completed)
        })
        .collect::<HashMap<_, _>>();

    let mut groups_by_anchor_index = HashMap::<usize, TurnWorkGroupRow>::new();
    let mut work_member_to_anchor_index = HashMap::<usize, usize>::new();

    let mut index = 0;
    while index < timeline.len() {
        let turn_id = timeline[index].turn_id.as_str();
        let turn_start = index;
        while index < timeline.len() && timeline[index].turn_id == turn_id {
            index = index.saturating_add(1);
        }
        let turn_end = index;

        if !turn_completed.get(turn_id).copied().unwrap_or(false) {
            continue;
        }

        let mut cursor = turn_start;
        while cursor < turn_end {
            let Some(user_index) = (cursor..turn_end).find(|ix| {
                projection
                    .item_for_timeline_entry(&timeline[*ix])
                    .is_some_and(|item_view| matches!(item_view.item, TurnItem::UserMessage { .. }))
            }) else {
                break;
            };

            let Some(agent_index) = ((user_index + 1)..turn_end).find(|ix| {
                projection
                    .item_for_timeline_entry(&timeline[*ix])
                    .is_some_and(is_parent_agent_message)
            }) else {
                break;
            };

            let work_indices = ((user_index + 1)..agent_index)
                .filter(|ix| {
                    projection
                        .item_for_timeline_entry(&timeline[*ix])
                        .is_some_and(|item_view| {
                            !matches!(item_view.item, TurnItem::UserMessage { .. })
                                && !is_parent_agent_message(item_view)
                        })
                })
                .collect::<Vec<_>>();

            if !work_indices.is_empty() {
                let anchor_entry_id = timeline[user_index].id.clone();
                let toggle_key = timeline_turn_work_group_key(anchor_entry_id.as_str());
                let group = TurnWorkGroupRow {
                    toggle_key: toggle_key.clone(),
                    anchor_entry_id,
                    elapsed_ms: timeline_work_group_elapsed_ms(projection, work_indices.as_slice()),
                    is_open: expanded.contains(toggle_key.as_str()),
                };

                groups_by_anchor_index.insert(user_index, group);
                for work_index in work_indices {
                    work_member_to_anchor_index.insert(work_index, user_index);
                }
            }

            cursor = agent_index.saturating_add(1);
        }
    }

    let task_anchor_by_task_id = timeline
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| {
            let item_view = projection.item_for_timeline_entry(entry)?;
            let TurnItem::Task { item } = &item_view.item else {
                return None;
            };
            Some((item.task_id.as_str(), index))
        })
        .collect::<HashMap<_, _>>();

    for (timeline_index, entry) in timeline.iter().enumerate() {
        let Some(item_view) = projection.item_for_timeline_entry(entry) else {
            continue;
        };
        let Some(task_id) = task_timeline_meta_task_id(item_view) else {
            continue;
        };
        let Some(anchor_index) = task_anchor_by_task_id.get(task_id).copied() else {
            continue;
        };
        if anchor_index == timeline_index {
            continue;
        }
        let anchor_entry_id = timeline[anchor_index].id.clone();
        let toggle_key = timeline_turn_work_group_key(anchor_entry_id.as_str());
        groups_by_anchor_index
            .entry(anchor_index)
            .or_insert_with(|| TurnWorkGroupRow {
                toggle_key: toggle_key.clone(),
                anchor_entry_id,
                elapsed_ms: None,
                is_open: expanded.contains(toggle_key.as_str()),
            });
        work_member_to_anchor_index
            .entry(timeline_index)
            .or_insert(anchor_index);
    }

    let mut coalesced_groups_by_start_index = HashMap::<usize, TimelineCoalescedToolsRow>::new();
    let mut coalesced_member_to_start_index = HashMap::<usize, usize>::new();

    let task_anchor_by_task_id_any_status = timeline
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| {
            let item_view = projection.item_for_timeline_entry(entry)?;
            let TurnItem::Task { item } = &item_view.item else {
                return None;
            };
            Some((item.task_id.as_str(), (index, item.status)))
        })
        .collect::<HashMap<_, _>>();

    for (task_id, (anchor_index, status)) in task_anchor_by_task_id_any_status {
        if matches!(
            status,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
        ) {
            continue;
        }

        let completed_tool_indices = timeline
            .iter()
            .enumerate()
            .filter_map(|(timeline_index, entry)| {
                if timeline_index == anchor_index {
                    return None;
                }
                let item_view = projection.item_for_timeline_entry(entry)?;
                (task_timeline_meta_task_id(item_view) == Some(task_id)
                    && is_completed_dynamic_tool(item_view))
                .then_some(timeline_index)
            })
            .collect::<Vec<_>>();

        if completed_tool_indices.len() <= LIVE_TASK_VISIBLE_COMPLETED_TOOL_ROWS {
            continue;
        }
        let hidden_count = completed_tool_indices.len() - LIVE_TASK_VISIBLE_COMPLETED_TOOL_ROWS;
        let hidden_indices = &completed_tool_indices[..hidden_count];
        register_coalesced_group(
            &mut coalesced_groups_by_start_index,
            &mut coalesced_member_to_start_index,
            hidden_indices,
            format!("task-tools-{task_id}"),
            TimelineCoalescedToolsKind::CompletedTaskTools,
            expanded,
        );
    }

    let mut task_wait_groups = HashMap::<String, Vec<usize>>::new();
    for (timeline_index, entry) in timeline.iter().enumerate() {
        let Some(item_view) = projection.item_for_timeline_entry(entry) else {
            continue;
        };
        let Some(signature) = completed_task_wait_signature(item_view) else {
            continue;
        };
        task_wait_groups
            .entry(format!("{}::{signature}", entry.turn_id))
            .or_default()
            .push(timeline_index);
    }
    for (signature, indices) in task_wait_groups {
        if indices.len() <= 1 {
            continue;
        }
        let hidden_indices = &indices[1..];
        register_coalesced_group(
            &mut coalesced_groups_by_start_index,
            &mut coalesced_member_to_start_index,
            hidden_indices,
            format!("task-wait-{signature}"),
            TimelineCoalescedToolsKind::RepeatedTaskWait,
            expanded,
        );
    }

    let mut rows = Vec::with_capacity(timeline.len());
    for (timeline_index, entry) in timeline.iter().enumerate() {
        if let Some(anchor_index) = work_member_to_anchor_index.get(&timeline_index).copied()
            && groups_by_anchor_index
                .get(&anchor_index)
                .is_some_and(|group| !group.is_open)
        {
            continue;
        }

        if let Some(group) = coalesced_groups_by_start_index
            .get(&timeline_index)
            .cloned()
        {
            rows.push(TimelineRow {
                key: group.toggle_key.clone(),
                kind: TimelineRowKind::CoalescedTools(group),
            });
        }

        if let Some(group_start_index) = coalesced_member_to_start_index
            .get(&timeline_index)
            .copied()
            && coalesced_groups_by_start_index
                .get(&group_start_index)
                .is_some_and(|group| !group.is_open)
        {
            continue;
        }

        rows.push(TimelineRow {
            key: entry.id.clone(),
            kind: TimelineRowKind::Item { timeline_index },
        });

        if let Some(group) = groups_by_anchor_index.get(&timeline_index).cloned() {
            rows.push(TimelineRow {
                key: group.toggle_key.clone(),
                kind: TimelineRowKind::TurnWorkToggle(group),
            });
        }
    }

    rows
}

pub(super) fn timeline_rows_layout_hash(
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

pub(super) fn timeline_row_layout_hash(
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
            std::mem::discriminant(&group.kind).hash(&mut hasher);
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

pub(super) fn timeline_row_text_len(
    projection: &ConversationViewState,
    row: &TimelineRow,
) -> usize {
    match &row.kind {
        TimelineRowKind::Item { timeline_index } => projection
            .timeline
            .get(*timeline_index)
            .and_then(|entry| projection.item_for_timeline_entry(entry))
            .map(|item_view| timeline_entry_text(item_view).len())
            .unwrap_or_default(),
        TimelineRowKind::TurnWorkToggle(group) => {
            let mut len = t!("timeline.work_group.completed").len();
            if let Some(elapsed_ms) = group.elapsed_ms {
                len = len.saturating_add(format_elapsed_ms(elapsed_ms).len());
            }
            len
        }
        TimelineRowKind::CoalescedTools(group) => coalesced_tools_label(group).len(),
    }
}

pub(super) fn timeline_row_toggle_key(row: &TimelineRow) -> Option<&str> {
    match &row.kind {
        TimelineRowKind::TurnWorkToggle(group) => Some(group.toggle_key.as_str()),
        TimelineRowKind::CoalescedTools(group) => Some(group.toggle_key.as_str()),
        TimelineRowKind::Item { .. } => None,
    }
}

fn register_coalesced_group(
    groups_by_start_index: &mut HashMap<usize, TimelineCoalescedToolsRow>,
    member_to_start_index: &mut HashMap<usize, usize>,
    hidden_indices: &[usize],
    group_id: String,
    kind: TimelineCoalescedToolsKind,
    expanded: &HashSet<String>,
) {
    let Some(start_index) = hidden_indices.iter().copied().min() else {
        return;
    };
    let toggle_key = timeline_coalesced_tools_key(group_id.as_str());
    groups_by_start_index
        .entry(start_index)
        .or_insert_with(|| TimelineCoalescedToolsRow {
            toggle_key: toggle_key.clone(),
            count: hidden_indices.len(),
            is_open: expanded.contains(toggle_key.as_str()),
            kind,
        });
    for hidden_index in hidden_indices {
        member_to_start_index
            .entry(*hidden_index)
            .or_insert(start_index);
    }
}

pub(super) fn coalesced_tools_label(group: &TimelineCoalescedToolsRow) -> String {
    match group.kind {
        TimelineCoalescedToolsKind::CompletedTaskTools => {
            format!("{} completed tool calls", group.count)
        }
        TimelineCoalescedToolsKind::RepeatedTaskWait => {
            format!("{} repeated task_wait calls", group.count)
        }
    }
}

fn timeline_entry_text(item_view: &ItemView) -> &str {
    item_view
        .final_text
        .as_deref()
        .unwrap_or(item_view.partial_text.as_str())
}

fn timeline_item_started_at(item_view: &ItemView) -> Option<i64> {
    item_view
        .started_at_unix_ms
        .or(item_view.updated_at_unix_ms)
        .or(item_view.completed_at_unix_ms)
}

fn timeline_item_ended_at(item_view: &ItemView) -> Option<i64> {
    item_view
        .completed_at_unix_ms
        .or(item_view.updated_at_unix_ms)
        .or(item_view.started_at_unix_ms)
}

fn timeline_work_group_elapsed_ms(
    projection: &ConversationViewState,
    work_item_indices: &[usize],
) -> Option<u64> {
    let mut started_at = None::<i64>;
    let mut ended_at = None::<i64>;

    for work_index in work_item_indices {
        let Some(work_entry) = projection.timeline.get(*work_index) else {
            continue;
        };
        let Some(item_view) = projection.item_for_timeline_entry(work_entry) else {
            continue;
        };

        if let Some(started) = timeline_item_started_at(item_view) {
            started_at = Some(started_at.map_or(started, |value| value.min(started)));
        }
        if let Some(ended) = timeline_item_ended_at(item_view) {
            ended_at = Some(ended_at.map_or(ended, |value| value.max(ended)));
        }
    }

    match (started_at, ended_at) {
        (Some(started), Some(ended)) => Some(ended.saturating_sub(started) as u64),
        _ => None,
    }
}

fn task_timeline_meta_task_id(item_view: &ItemView) -> Option<&str> {
    let meta = item_view.opaque_meta.as_ref()?;
    if meta.get("timeline_group")?.as_str()? != "task" {
        return None;
    }
    meta.get("task_id")?.as_str()
}

fn is_parent_agent_message(item_view: &ItemView) -> bool {
    matches!(item_view.item, TurnItem::AgentMessage { .. })
        && task_timeline_meta_task_id(item_view).is_none()
}

fn is_completed_dynamic_tool(item_view: &ItemView) -> bool {
    matches!(
        &item_view.item,
        TurnItem::DynamicToolCall {
            status: ToolCallStatus::Completed,
            ..
        }
    )
}

fn completed_task_wait_signature(item_view: &ItemView) -> Option<String> {
    let TurnItem::DynamicToolCall {
        tool_name,
        arguments,
        status,
        ..
    } = &item_view.item
    else {
        return None;
    };
    if tool_name != "task_wait" || *status != ToolCallStatus::Completed {
        return None;
    }
    Some(arguments.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::conversation::{
        ItemView, TimelineEntry, TimelineEntryStatus, TurnPhase, TurnView,
    };
    use pioneer_protocol::{
        SystemEventLevel, TaskExecutorKind, TaskTriggerKind, TaskTurnItem, ToolCallStatus,
        ToolDisplayPayload, ToolOutputPolicySnapshot, ToolStoragePayload,
    };

    #[test]
    fn completed_task_group_collapses_members_by_default() {
        let projection = ConversationViewState {
            timeline: vec![
                TimelineEntry {
                    id: "entry_task".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_id: "task_anchor".to_owned(),
                    item_index: 0,
                },
                TimelineEntry {
                    id: "entry_task_event".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_id: "task_event_1".to_owned(),
                    item_index: 1,
                },
                TimelineEntry {
                    id: "entry_child_tool".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_id: "child_tool_1".to_owned(),
                    item_index: 2,
                },
            ],
            items: vec![
                ItemView {
                    id: "task_anchor".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "task".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(1),
                    updated_at_unix_ms: Some(3),
                    completed_at_unix_ms: Some(3),
                    partial_text: "Task".to_owned(),
                    final_text: Some("Task".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::Task {
                        item: TaskTurnItem {
                            id: "task_anchor".to_owned(),
                            task_id: "task_1".to_owned(),
                            run_id: Some("run_1".to_owned()),
                            parent_task_id: None,
                            root_task_id: None,
                            title: "Task".to_owned(),
                            status: TaskStatus::Completed,
                            trigger_kind: TaskTriggerKind::Immediate,
                            executor_kind: TaskExecutorKind::Agent,
                            child_thread_id: None,
                            child_turn_id: None,
                            agent_role: None,
                            depth: 0,
                            max_depth: 3,
                            next_fire_at: None,
                            result_preview: None,
                            error_preview: None,
                            created_at: 1,
                            updated_at: 3,
                        },
                    },
                    opaque_meta: None,
                },
                ItemView {
                    id: "task_event_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "system_event".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(2),
                    updated_at_unix_ms: Some(2),
                    completed_at_unix_ms: Some(2),
                    partial_text: "Task run started".to_owned(),
                    final_text: Some("Task run started".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::SystemEvent {
                        id: "task_event_1".to_owned(),
                        level: SystemEventLevel::Info,
                        message: "Task run started".to_owned(),
                        code: Some(
                            pioneer_protocol::constants::events::TASK_RUN_STARTED.to_owned(),
                        ),
                        details: None,
                    },
                    opaque_meta: Some(serde_json::json!({
                        "timeline_group": "task",
                        "task_id": "task_1",
                    })),
                },
                ItemView {
                    id: "child_tool_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "system_event".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(3),
                    updated_at_unix_ms: Some(3),
                    completed_at_unix_ms: Some(3),
                    partial_text: "Child tool".to_owned(),
                    final_text: Some("Child tool".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::SystemEvent {
                        id: "child_tool_1".to_owned(),
                        level: SystemEventLevel::Info,
                        message: "Child tool".to_owned(),
                        code: None,
                        details: None,
                    },
                    opaque_meta: Some(serde_json::json!({
                        "timeline_group": "task",
                        "task_id": "task_1",
                    })),
                },
            ],
            ..ConversationViewState::default()
        };

        let rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(rows.len(), 2);
        assert!(matches!(
            rows[0].kind,
            TimelineRowKind::Item { timeline_index: 0 }
        ));
        assert!(matches!(rows[1].kind, TimelineRowKind::TurnWorkToggle(_)));
    }

    #[test]
    fn running_task_group_coalesces_older_completed_child_tools() {
        let mut timeline = Vec::new();
        let mut items = Vec::new();
        timeline.push(TimelineEntry {
            id: "entry_task".to_owned(),
            turn_id: "turn_parent".to_owned(),
            item_id: "task_anchor".to_owned(),
            item_index: 0,
        });
        items.push(ItemView {
            id: "task_anchor".to_owned(),
            turn_id: "turn_parent".to_owned(),
            item_type: "task".to_owned(),
            status: TimelineEntryStatus::Running,
            started_at_unix_ms: Some(1),
            updated_at_unix_ms: Some(1),
            completed_at_unix_ms: None,
            partial_text: "Task".to_owned(),
            final_text: None,
            partial_markdown: None,
            final_markdown: None,
            item: TurnItem::Task {
                item: TaskTurnItem {
                    id: "task_anchor".to_owned(),
                    task_id: "task_1".to_owned(),
                    run_id: Some("run_1".to_owned()),
                    parent_task_id: None,
                    root_task_id: None,
                    title: "Task".to_owned(),
                    status: TaskStatus::Running,
                    trigger_kind: TaskTriggerKind::Immediate,
                    executor_kind: TaskExecutorKind::Agent,
                    child_thread_id: None,
                    child_turn_id: None,
                    agent_role: None,
                    depth: 0,
                    max_depth: 3,
                    next_fire_at: None,
                    result_preview: None,
                    error_preview: None,
                    created_at: 1,
                    updated_at: 1,
                },
            },
            opaque_meta: None,
        });

        for index in 0..5 {
            let id = format!("tool_{index}");
            timeline.push(TimelineEntry {
                id: format!("entry_{id}"),
                turn_id: "turn_parent".to_owned(),
                item_id: id.clone(),
                item_index: index + 1,
            });
            items.push(ItemView {
                id: id.clone(),
                turn_id: "turn_parent".to_owned(),
                item_type: "dynamic_tool_call".to_owned(),
                status: TimelineEntryStatus::Completed,
                started_at_unix_ms: Some(2 + index as i64),
                updated_at_unix_ms: Some(2 + index as i64),
                completed_at_unix_ms: Some(2 + index as i64),
                partial_text: id.clone(),
                final_text: Some(id.clone()),
                partial_markdown: None,
                final_markdown: None,
                item: TurnItem::DynamicToolCall {
                    id: id.clone(),
                    tool_name: "grep_files".to_owned(),
                    arguments: serde_json::json!({"pattern": "Task"}),
                    status: ToolCallStatus::Completed,
                    recovery_policy: None,
                    output_policy: ToolOutputPolicySnapshot::for_tool_name("grep_files"),
                    display: ToolDisplayPayload::Hidden,
                    storage: ToolStoragePayload::None,
                    recovery: None,
                    success: Some(true),
                    outcome: None,
                    observation: None,
                },
                opaque_meta: Some(serde_json::json!({
                    "timeline_group": "task",
                    "task_id": "task_1",
                })),
            });
        }

        let projection = ConversationViewState {
            timeline,
            items,
            ..ConversationViewState::default()
        };

        let rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(rows.len(), 2);
        assert!(matches!(
            rows[0].kind,
            TimelineRowKind::Item { timeline_index: 0 }
        ));
        assert!(matches!(rows[1].kind, TimelineRowKind::TurnWorkToggle(_)));

        let rows = build_timeline_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_task")]),
        );
        assert!(rows.iter().any(|row| {
            matches!(
                row.kind,
                TimelineRowKind::CoalescedTools(TimelineCoalescedToolsRow { count: 3, .. })
            )
        }));
        assert_eq!(
            rows.iter()
                .filter(|row| matches!(row.kind, TimelineRowKind::Item { .. }))
                .count(),
            3
        );
    }

    #[test]
    fn repeated_task_wait_rows_are_grouped() {
        let mut timeline = Vec::new();
        let mut items = Vec::new();
        for index in 0..3 {
            let id = format!("wait_{index}");
            timeline.push(TimelineEntry {
                id: format!("entry_{id}"),
                turn_id: "turn_parent".to_owned(),
                item_id: id.clone(),
                item_index: index,
            });
            items.push(ItemView {
                id: id.clone(),
                turn_id: "turn_parent".to_owned(),
                item_type: "dynamic_tool_call".to_owned(),
                status: TimelineEntryStatus::Completed,
                started_at_unix_ms: Some(index as i64),
                updated_at_unix_ms: Some(index as i64),
                completed_at_unix_ms: Some(index as i64),
                partial_text: "task_wait".to_owned(),
                final_text: Some("task_wait".to_owned()),
                partial_markdown: None,
                final_markdown: None,
                item: TurnItem::DynamicToolCall {
                    id,
                    tool_name: "task_wait".to_owned(),
                    arguments: serde_json::json!({"taskIds": ["task_1"]}),
                    status: ToolCallStatus::Completed,
                    recovery_policy: None,
                    output_policy: ToolOutputPolicySnapshot::for_tool_name("dynamic"),
                    display: ToolDisplayPayload::Hidden,
                    storage: ToolStoragePayload::None,
                    recovery: None,
                    success: Some(true),
                    outcome: None,
                    observation: None,
                },
                opaque_meta: None,
            });
        }

        let projection = ConversationViewState {
            timeline,
            items,
            ..ConversationViewState::default()
        };
        let rows = build_timeline_rows(&projection, &HashSet::new());

        assert!(rows.iter().any(|row| {
            matches!(
                row.kind,
                TimelineRowKind::CoalescedTools(TimelineCoalescedToolsRow { count: 2, .. })
            )
        }));
    }

    #[test]
    fn child_agent_message_does_not_end_parent_work_group() {
        let projection = ConversationViewState {
            timeline: vec![
                TimelineEntry {
                    id: "entry_user".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_id: "user_1".to_owned(),
                    item_index: 0,
                },
                TimelineEntry {
                    id: "entry_task".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_id: "task_anchor".to_owned(),
                    item_index: 1,
                },
                TimelineEntry {
                    id: "entry_child_agent".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_id: "child_agent_1".to_owned(),
                    item_index: 2,
                },
                TimelineEntry {
                    id: "entry_child_tool".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_id: "child_tool_1".to_owned(),
                    item_index: 3,
                },
                TimelineEntry {
                    id: "entry_parent_agent".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_id: "parent_agent_1".to_owned(),
                    item_index: 4,
                },
            ],
            items: vec![
                ItemView {
                    id: "user_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "user_message".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(1),
                    updated_at_unix_ms: Some(1),
                    completed_at_unix_ms: Some(1),
                    partial_text: "Run subagent".to_owned(),
                    final_text: Some("Run subagent".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::UserMessage {
                        id: "user_1".to_owned(),
                        text: "Run subagent".to_owned(),
                        attachments: Vec::new(),
                    },
                    opaque_meta: None,
                },
                ItemView {
                    id: "task_anchor".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "task".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(2),
                    updated_at_unix_ms: Some(4),
                    completed_at_unix_ms: Some(4),
                    partial_text: "Task".to_owned(),
                    final_text: Some("Task".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::Task {
                        item: TaskTurnItem {
                            id: "task_anchor".to_owned(),
                            task_id: "task_1".to_owned(),
                            run_id: Some("run_1".to_owned()),
                            parent_task_id: None,
                            root_task_id: None,
                            title: "Task".to_owned(),
                            status: TaskStatus::Completed,
                            trigger_kind: TaskTriggerKind::Immediate,
                            executor_kind: TaskExecutorKind::Agent,
                            child_thread_id: Some("child_thread_1".to_owned()),
                            child_turn_id: Some("child_turn_1".to_owned()),
                            agent_role: None,
                            depth: 0,
                            max_depth: 3,
                            next_fire_at: None,
                            result_preview: None,
                            error_preview: None,
                            created_at: 2,
                            updated_at: 4,
                        },
                    },
                    opaque_meta: None,
                },
                ItemView {
                    id: "child_agent_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "agent_message".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(3),
                    updated_at_unix_ms: Some(3),
                    completed_at_unix_ms: Some(3),
                    partial_text: "Child result".to_owned(),
                    final_text: Some("Child result".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::AgentMessage {
                        id: "child_agent_1".to_owned(),
                        text: "Child result".to_owned(),
                        markdown: None,
                        markdown_version: None,
                    },
                    opaque_meta: Some(serde_json::json!({
                        "timeline_group": "task",
                        "task_id": "task_1",
                        "run_id": "run_1",
                        "child_thread_id": "child_thread_1",
                        "child_turn_id": "child_turn_1",
                    })),
                },
                ItemView {
                    id: "child_tool_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "dynamic_tool_call".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(3),
                    updated_at_unix_ms: Some(3),
                    completed_at_unix_ms: Some(3),
                    partial_text: "grep_files".to_owned(),
                    final_text: Some("grep_files".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::DynamicToolCall {
                        id: "child_tool_1".to_owned(),
                        tool_name: "grep_files".to_owned(),
                        arguments: serde_json::json!({"pattern": "Task"}),
                        status: ToolCallStatus::Completed,
                        recovery_policy: None,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("grep_files"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::None,
                        recovery: None,
                        success: Some(true),
                        outcome: None,
                        observation: None,
                    },
                    opaque_meta: Some(serde_json::json!({
                        "timeline_group": "task",
                        "task_id": "task_1",
                        "run_id": "run_1",
                        "child_thread_id": "child_thread_1",
                        "child_turn_id": "child_turn_1",
                    })),
                },
                ItemView {
                    id: "parent_agent_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "agent_message".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(5),
                    updated_at_unix_ms: Some(5),
                    completed_at_unix_ms: Some(5),
                    partial_text: "Parent final".to_owned(),
                    final_text: Some("Parent final".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::AgentMessage {
                        id: "parent_agent_1".to_owned(),
                        text: "Parent final".to_owned(),
                        markdown: None,
                        markdown_version: None,
                    },
                    opaque_meta: None,
                },
            ],
            turns: vec![TurnView {
                id: "turn_parent".to_owned(),
                phase: TurnPhase::Completed,
                started_at_unix_ms: Some(1),
                completed_at_unix_ms: Some(5),
                error: None,
            }],
            ..ConversationViewState::default()
        };

        let rows = build_timeline_rows(&projection, &HashSet::new());
        let visible_item_ids = rows
            .iter()
            .filter_map(|row| match row.kind {
                TimelineRowKind::Item { timeline_index } => projection
                    .timeline
                    .get(timeline_index)
                    .map(|entry| entry.item_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(visible_item_ids, vec!["user_1", "parent_agent_1"]);
        assert!(rows.iter().any(|row| matches!(
            row.kind,
            TimelineRowKind::TurnWorkToggle(TurnWorkGroupRow { .. })
        )));
    }
}
