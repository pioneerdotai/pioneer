//! Timeline row models and work-group selectors.

use crate::{
    conversation::{ConversationViewState, ItemView, TimelineEntryStatus},
    timeline::labels::task_wait_review_display,
};
use pioneer_protocol::{TaskStatus, TaskTriggerKind, TaskTurnItem, ToolCallStatus, TurnItem};
use std::collections::{HashMap, HashSet};

const TIMELINE_TURN_WORK_GROUP_PREFIX: &str = "timeline-turn-work-group::";
const TIMELINE_COALESCED_TOOLS_PREFIX: &str = "timeline-coalesced-tools::";
const LIVE_TASK_VISIBLE_COMPLETED_TOOL_ROWS: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TaskTimelineGroupKey<'a> {
    task_id: &'a str,
    run_id: Option<&'a str>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TurnWorkGroupRow {
    pub toggle_key: String,
    pub anchor_entry_id: String,
    pub elapsed_ms: Option<u64>,
    pub is_open: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub enum TimelineCoalescedToolsKind {
    CompletedTaskTools,
    RepeatedTaskWait,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TimelineCoalescedToolsRow {
    pub toggle_key: String,
    pub count: usize,
    pub is_open: bool,
    pub kind: TimelineCoalescedToolsKind,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum TimelineRowKind {
    Item { timeline_index: usize },
    TurnWorkToggle(TurnWorkGroupRow),
    CoalescedTools(TimelineCoalescedToolsRow),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TimelineRow {
    pub key: String,
    pub kind: TimelineRowKind,
}

pub fn timeline_turn_work_group_key(anchor_entry_id: &str) -> String {
    format!("{TIMELINE_TURN_WORK_GROUP_PREFIX}{anchor_entry_id}")
}

fn timeline_coalesced_tools_key(group_id: &str) -> String {
    format!("{TIMELINE_COALESCED_TOOLS_PREFIX}{group_id}")
}

pub fn build_timeline_rows(
    projection: &ConversationViewState,
    expanded: &HashSet<String>,
) -> Vec<TimelineRow> {
    let timeline = &projection.timeline;
    let grouped_rows = build_timeline_group_rows(projection, expanded);
    if grouped_rows.is_empty() {
        return Vec::new();
    }

    let mut coalesced_groups_by_start_index = HashMap::<usize, TimelineCoalescedToolsRow>::new();
    let mut coalesced_member_to_start_index = HashMap::<usize, usize>::new();

    let mut task_anchor_by_group_key_any_status =
        HashMap::<TaskTimelineGroupKey<'_>, (usize, TaskStatus)>::new();
    for (index, entry) in timeline.iter().enumerate() {
        let Some(item_view) = projection.item_for_timeline_entry(entry) else {
            continue;
        };
        let TurnItem::Task { item } = &item_view.item else {
            continue;
        };
        task_anchor_by_group_key_any_status
            .entry(task_anchor_group_key(entry.turn_id.as_str(), item))
            .or_insert((index, item.status));
    }

    for (group_key, (anchor_index, status)) in task_anchor_by_group_key_any_status {
        if matches!(
            status,
            TaskStatus::Completed
                | TaskStatus::Blocked
                | TaskStatus::Failed
                | TaskStatus::Cancelled
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
                (task_timeline_origin_group_key(item_view) == Some(group_key)
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
            format!("task-tools-{}", task_timeline_group_id(group_key)),
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

    let mut rows = Vec::with_capacity(grouped_rows.len());
    for grouped_row in grouped_rows {
        let TimelineRow { key, kind } = grouped_row;
        match kind {
            TimelineRowKind::Item { timeline_index } => {
                push_timeline_item_rows(
                    &mut rows,
                    timeline_index,
                    key.as_str(),
                    &coalesced_groups_by_start_index,
                    &coalesced_member_to_start_index,
                );
            }
            TimelineRowKind::TurnWorkToggle(group) => {
                rows.push(TimelineRow {
                    key,
                    kind: TimelineRowKind::TurnWorkToggle(group),
                });
            }
            TimelineRowKind::CoalescedTools(group) => {
                rows.push(TimelineRow {
                    key,
                    kind: TimelineRowKind::CoalescedTools(group),
                });
            }
        }
    }

    pin_bottom_approval_rows(projection, rows)
}

pub fn build_timeline_group_rows(
    projection: &ConversationViewState,
    expanded: &HashSet<String>,
) -> Vec<TimelineRow> {
    let timeline = &projection.timeline;
    if timeline.is_empty() {
        return Vec::new();
    }

    let mut groups_by_anchor_index = HashMap::<usize, TurnWorkGroupRow>::new();
    let mut work_member_to_anchor_index = HashMap::<usize, usize>::new();
    let mut work_members_by_anchor_index = HashMap::<usize, Vec<usize>>::new();

    let mut turn_indices = HashMap::<&str, Vec<usize>>::new();
    for (timeline_index, entry) in timeline.iter().enumerate() {
        turn_indices
            .entry(entry.turn_id.as_str())
            .or_default()
            .push(timeline_index);
    }

    for indices in turn_indices.values() {
        let mut cursor = 0;
        while cursor < indices.len() {
            let Some(user_pos) = (cursor..indices.len()).find(|pos| {
                let timeline_index = indices[*pos];
                projection
                    .item_for_timeline_entry(&timeline[timeline_index])
                    .is_some_and(|item_view| matches!(item_view.item, TurnItem::UserMessage { .. }))
            }) else {
                break;
            };

            let Some(agent_pos) = ((user_pos + 1)..indices.len()).find(|pos| {
                let timeline_index = indices[*pos];
                projection
                    .item_for_timeline_entry(&timeline[timeline_index])
                    .is_some_and(is_terminal_parent_agent_message)
            }) else {
                break;
            };

            let user_index = indices[user_pos];
            let agent_index = indices[agent_pos];
            let mut work_indices = ((user_pos + 1)..agent_pos)
                .filter_map(|pos| {
                    let timeline_index = indices[pos];
                    projection
                        .item_for_timeline_entry(&timeline[timeline_index])
                        .is_some_and(|item_view| {
                            !matches!(item_view.item, TurnItem::UserMessage { .. })
                                && !is_parent_agent_message(item_view)
                                && !is_bottom_pinned_approval_item(item_view)
                        })
                        .then_some(timeline_index)
                })
                .collect::<Vec<_>>();
            let extra_task_event_indices = ((user_index + 1)..agent_index)
                .filter(|timeline_index| {
                    !work_indices.contains(timeline_index)
                        && projection
                            .item_for_timeline_entry(&timeline[*timeline_index])
                            .is_some_and(is_task_system_work_event)
                })
                .collect::<Vec<_>>();
            work_indices.extend(extra_task_event_indices);
            work_indices.sort_unstable();

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
                    register_work_group_member(
                        &mut work_member_to_anchor_index,
                        &mut work_members_by_anchor_index,
                        work_index,
                        user_index,
                    );
                }
            }

            cursor = agent_pos.saturating_add(1);
        }
    }

    let mut task_anchor_by_group_key = HashMap::<TaskTimelineGroupKey<'_>, usize>::new();
    for (index, entry) in timeline.iter().enumerate() {
        let Some(item_view) = projection.item_for_timeline_entry(entry) else {
            continue;
        };
        let TurnItem::Task { item } = &item_view.item else {
            continue;
        };
        task_anchor_by_group_key
            .entry(task_anchor_group_key(entry.turn_id.as_str(), item))
            .or_insert(index);
    }

    for (timeline_index, entry) in timeline.iter().enumerate() {
        let Some(item_view) = projection.item_for_timeline_entry(entry) else {
            continue;
        };
        if is_bottom_pinned_approval_item(item_view) {
            continue;
        }
        let Some(group_key) = task_timeline_origin_group_key(item_view) else {
            continue;
        };
        let Some(anchor_index) = task_anchor_by_group_key.get(&group_key).copied() else {
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
        register_work_group_member(
            &mut work_member_to_anchor_index,
            &mut work_members_by_anchor_index,
            timeline_index,
            anchor_index,
        );
    }

    let mut rows = Vec::with_capacity(timeline.len());
    for (timeline_index, entry) in timeline.iter().enumerate() {
        if work_member_to_anchor_index.contains_key(&timeline_index) {
            continue;
        }

        rows.push(TimelineRow {
            key: entry.id.clone(),
            kind: TimelineRowKind::Item { timeline_index },
        });

        if let Some(group) = groups_by_anchor_index.get(&timeline_index).cloned() {
            let group_is_open = group.is_open;
            rows.push(TimelineRow {
                key: group.toggle_key.clone(),
                kind: TimelineRowKind::TurnWorkToggle(group),
            });
            if group_is_open
                && let Some(member_indices) = work_members_by_anchor_index.get(&timeline_index)
            {
                for member_index in member_indices {
                    let Some(member_entry) = timeline.get(*member_index) else {
                        continue;
                    };
                    rows.push(TimelineRow {
                        key: member_entry.id.clone(),
                        kind: TimelineRowKind::Item {
                            timeline_index: *member_index,
                        },
                    });
                }
            }
        }
    }

    rows
}

fn pin_bottom_approval_rows(
    projection: &ConversationViewState,
    rows: Vec<TimelineRow>,
) -> Vec<TimelineRow> {
    if !rows
        .iter()
        .any(|row| is_bottom_pinned_approval_row(projection, row))
    {
        return rows;
    }

    let mut normal_rows = Vec::with_capacity(rows.len());
    let mut approval_rows = Vec::new();
    for row in rows {
        if is_bottom_pinned_approval_row(projection, &row) {
            approval_rows.push(row);
        } else {
            normal_rows.push(row);
        }
    }
    normal_rows.extend(approval_rows);
    normal_rows
}

fn is_bottom_pinned_approval_row(projection: &ConversationViewState, row: &TimelineRow) -> bool {
    let TimelineRowKind::Item { timeline_index } = &row.kind else {
        return false;
    };
    let Some(entry) = projection.timeline.get(*timeline_index) else {
        return false;
    };
    projection
        .item_for_timeline_entry(entry)
        .is_some_and(is_bottom_pinned_approval_item)
}

fn push_timeline_item_rows(
    rows: &mut Vec<TimelineRow>,
    timeline_index: usize,
    entry_id: &str,
    coalesced_groups_by_start_index: &HashMap<usize, TimelineCoalescedToolsRow>,
    coalesced_member_to_start_index: &HashMap<usize, usize>,
) {
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
        return;
    }

    rows.push(TimelineRow {
        key: entry_id.to_owned(),
        kind: TimelineRowKind::Item { timeline_index },
    });
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

fn register_work_group_member(
    work_member_to_anchor_index: &mut HashMap<usize, usize>,
    work_members_by_anchor_index: &mut HashMap<usize, Vec<usize>>,
    member_index: usize,
    anchor_index: usize,
) {
    if work_member_to_anchor_index.contains_key(&member_index) {
        return;
    }
    work_member_to_anchor_index.insert(member_index, anchor_index);
    work_members_by_anchor_index
        .entry(anchor_index)
        .or_default()
        .push(member_index);
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

fn task_anchor_group_key<'a>(turn_id: &'a str, item: &'a TaskTurnItem) -> TaskTimelineGroupKey<'a> {
    let run_id = match item.run_id.as_deref() {
        Some(run_id) if item.trigger_kind == TaskTriggerKind::Immediate => Some(run_id),
        Some(run_id) if run_id == turn_id => Some(run_id),
        Some(_) if item.trigger_kind != TaskTriggerKind::Immediate => Some(turn_id),
        _ => None,
    };
    TaskTimelineGroupKey {
        task_id: item.task_id.as_str(),
        run_id,
    }
}

fn task_timeline_origin_group_key(item_view: &ItemView) -> Option<TaskTimelineGroupKey<'_>> {
    let origin = item_view.timeline_origin.as_ref()?;
    Some(TaskTimelineGroupKey {
        task_id: origin.task_id.as_deref()?,
        run_id: origin.run_id.as_deref(),
    })
}

fn task_timeline_group_id(group_key: TaskTimelineGroupKey<'_>) -> String {
    match group_key.run_id {
        Some(run_id) => format!("{}::{run_id}", group_key.task_id),
        None => group_key.task_id.to_owned(),
    }
}

fn is_parent_agent_message(item_view: &ItemView) -> bool {
    matches!(item_view.item, TurnItem::AgentMessage { .. })
        && task_timeline_origin_group_key(item_view).is_none()
        && !is_commentary_agent_message(item_view)
}

fn is_terminal_parent_agent_message(item_view: &ItemView) -> bool {
    is_parent_agent_message(item_view)
        && matches!(
            item_view.status,
            TimelineEntryStatus::Completed
                | TimelineEntryStatus::Blocked
                | TimelineEntryStatus::Failed
                | TimelineEntryStatus::Cancelled
        )
}

fn is_commentary_agent_message(item_view: &ItemView) -> bool {
    let TurnItem::AgentMessage { phase, .. } = &item_view.item else {
        return false;
    };
    *phase == pioneer_protocol::AgentMessagePhase::Commentary
}

fn is_task_system_work_event(item_view: &ItemView) -> bool {
    let TurnItem::SystemEvent { code, .. } = &item_view.item else {
        return false;
    };
    task_timeline_origin_group_key(item_view).is_some()
        || code
            .as_deref()
            .is_some_and(|code| code.starts_with("task/"))
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
    if is_bottom_pinned_approval_item(item_view) {
        return None;
    }
    Some(arguments.to_string())
}

fn is_bottom_pinned_approval_item(item_view: &ItemView) -> bool {
    let TurnItem::DynamicToolCall {
        tool_name, display, ..
    } = &item_view.item
    else {
        return false;
    };
    task_wait_review_display(tool_name, display)
        .is_some_and(|review| review.items.iter().any(|item| item.user_controls_allowed()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::{
        ConversationViewState, ItemView, TimelineEntry, TimelineEntryStatus,
    };
    use pioneer_protocol::{
        SystemEventLevel, TaskExecutorKind, TaskStatus, TaskTriggerKind, TaskTurnItem,
        ToolCallStatus, ToolDisplayPayload, ToolMetadata, ToolOutputPolicySnapshot,
        ToolOutputSummary, ToolStoragePayload, TurnItem,
    };

    fn timeline_entry(id: &str, turn_id: &str, item_id: &str, item_index: usize) -> TimelineEntry {
        TimelineEntry {
            id: id.to_owned(),
            turn_id: turn_id.to_owned(),
            item_id: item_id.to_owned(),
            item_index,
        }
    }

    fn item_view(
        id: &str,
        turn_id: &str,
        item_type: &str,
        item: TurnItem,
        timeline_origin: Option<pioneer_protocol::TimelineOrigin>,
    ) -> ItemView {
        ItemView {
            id: id.to_owned(),
            turn_id: turn_id.to_owned(),
            item_type: item_type.to_owned(),
            status: TimelineEntryStatus::Completed,
            started_at_unix_ms: Some(1),
            updated_at_unix_ms: Some(2),
            completed_at_unix_ms: Some(2),
            partial_text: id.to_owned(),
            final_text: Some(id.to_owned()),
            partial_markdown: None,
            final_markdown: None,
            item,
            timeline_origin,
            opaque_meta: None,
        }
    }

    fn visible_item_ids<'a>(
        projection: &'a ConversationViewState,
        rows: &[TimelineRow],
    ) -> Vec<&'a str> {
        rows.iter()
            .filter_map(|row| match row.kind {
                TimelineRowKind::Item { timeline_index } => projection
                    .timeline
                    .get(timeline_index)
                    .map(|entry| entry.item_id.as_str()),
                TimelineRowKind::TurnWorkToggle(_) => None,
                TimelineRowKind::CoalescedTools(_) => None,
            })
            .collect()
    }

    fn task_wait_user_approval_item(id: &str, turn_id: &str) -> ItemView {
        item_view(
            id,
            turn_id,
            "dynamic_tool_call",
            TurnItem::DynamicToolCall {
                id: id.to_owned(),
                tool_name: "task_wait".to_owned(),
                arguments: serde_json::json!({ "taskId": "task_1" }),
                status: ToolCallStatus::Completed,
                recovery_policy: None,
                output_policy: ToolOutputPolicySnapshot::for_tool_name("task_wait"),
                display: ToolDisplayPayload::Summary(ToolOutputSummary {
                    title: "task_wait completed".to_owned(),
                    lines: Vec::new(),
                    metadata: ToolMetadata::from_json(serde_json::json!({
                        "sanitizedResult": {
                            "reviewRequiredCount": 1,
                            "mode": "user_approval",
                            "reviewRequired": [{
                                "taskId": "task_1",
                                "runId": "run_1",
                                "candidateId": "candidate_1",
                                "reviewMode": "user_approval",
                                "userApprovalRequired": true,
                                "allowedActions": ["task_accept"]
                            }]
                        }
                    })),
                    truncated: false,
                }),
                storage: ToolStoragePayload::None,
                recovery: None,
                success: Some(true),
                outcome: None,
                observation: None,
            },
            None,
        )
    }

    #[test]
    fn work_between_user_and_terminal_parent_answer_collapses() {
        let projection = ConversationViewState {
            timeline: vec![
                timeline_entry("entry_user", "turn_parent", "user_1", 0),
                timeline_entry("entry_tool", "turn_parent", "tool_1", 1),
                timeline_entry("entry_task_event", "turn_task_event", "task_event_1", 2),
                timeline_entry("entry_agent", "turn_parent", "agent_1", 3),
            ],
            items: vec![
                item_view(
                    "user_1",
                    "turn_parent",
                    "user_message",
                    TurnItem::UserMessage {
                        id: "user_1".to_owned(),
                        text: "Do work".to_owned(),
                        attachments: Vec::new(),
                    },
                    None,
                ),
                item_view(
                    "tool_1",
                    "turn_parent",
                    "dynamic_tool_call",
                    TurnItem::DynamicToolCall {
                        id: "tool_1".to_owned(),
                        tool_name: "task_list".to_owned(),
                        arguments: serde_json::json!({}),
                        status: ToolCallStatus::Completed,
                        recovery_policy: None,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("task_list"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::None,
                        recovery: None,
                        success: Some(true),
                        outcome: None,
                        observation: None,
                    },
                    None,
                ),
                item_view(
                    "task_event_1",
                    "turn_parent",
                    "system_event",
                    TurnItem::SystemEvent {
                        id: "task_event_1".to_owned(),
                        level: SystemEventLevel::Info,
                        message: "Task cancelled".to_owned(),
                        code: Some("task/cancelled".to_owned()),
                        details: None,
                    },
                    Some(pioneer_protocol::TimelineOrigin {
                        kind: pioneer_protocol::TimelineOriginKind::TaskEvent,
                        task_id: Some("task_1".to_owned()),
                        run_id: None,
                        child_thread_id: None,
                        child_turn_id: None,
                        origin_event_id: Some("task_event_1".to_owned()),
                        origin_turn_item_id: None,
                        origin_sequence: 1,
                        occurred_at: 2,
                        lane: pioneer_protocol::TimelineLane::Task,
                    }),
                ),
                item_view(
                    "agent_1",
                    "turn_parent",
                    "agent_message",
                    TurnItem::AgentMessage {
                        id: "agent_1".to_owned(),
                        text: "Done".to_owned(),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                    None,
                ),
            ],
            ..ConversationViewState::default()
        };

        let collapsed_rows = build_timeline_group_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &collapsed_rows),
            vec!["user_1", "agent_1"]
        );
        assert!(collapsed_rows.iter().any(|row| matches!(
            row.kind,
            TimelineRowKind::TurnWorkToggle(TurnWorkGroupRow { .. })
        )));

        let expanded_rows = build_timeline_group_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_user")]),
        );
        assert_eq!(
            visible_item_ids(&projection, &expanded_rows),
            vec!["user_1", "tool_1", "task_event_1", "agent_1"]
        );
    }

    #[test]
    fn reasoning_and_tools_before_terminal_answer_collapse_as_turn_work() {
        let projection = ConversationViewState {
            timeline: vec![
                timeline_entry("entry_user", "turn_parent", "user_1", 0),
                timeline_entry("entry_reasoning", "turn_parent", "reasoning_1", 1),
                timeline_entry("entry_tool", "turn_parent", "tool_1", 2),
                timeline_entry("entry_agent", "turn_parent", "agent_1", 3),
            ],
            items: vec![
                item_view(
                    "user_1",
                    "turn_parent",
                    "user_message",
                    TurnItem::UserMessage {
                        id: "user_1".to_owned(),
                        text: "Inspect project".to_owned(),
                        attachments: Vec::new(),
                    },
                    None,
                ),
                item_view(
                    "reasoning_1",
                    "turn_parent",
                    "reasoning",
                    TurnItem::Reasoning {
                        id: "reasoning_1".to_owned(),
                        summary: vec!["I will inspect the project.".to_owned()],
                        content: Vec::new(),
                    },
                    None,
                ),
                item_view(
                    "tool_1",
                    "turn_parent",
                    "dynamic_tool_call",
                    TurnItem::DynamicToolCall {
                        id: "tool_1".to_owned(),
                        tool_name: "exec_command".to_owned(),
                        arguments: serde_json::json!({}),
                        status: ToolCallStatus::Completed,
                        recovery_policy: None,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("exec_command"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::None,
                        recovery: None,
                        success: Some(true),
                        outcome: None,
                        observation: None,
                    },
                    None,
                ),
                item_view(
                    "agent_1",
                    "turn_parent",
                    "agent_message",
                    TurnItem::AgentMessage {
                        id: "agent_1".to_owned(),
                        text: "Done".to_owned(),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                    None,
                ),
            ],
            ..ConversationViewState::default()
        };

        let collapsed_rows = build_timeline_group_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &collapsed_rows),
            vec!["user_1", "agent_1"]
        );
        assert!(collapsed_rows.iter().any(|row| matches!(
            row.kind,
            TimelineRowKind::TurnWorkToggle(TurnWorkGroupRow { .. })
        )));

        let expanded_rows = build_timeline_group_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_user")]),
        );
        assert_eq!(
            visible_item_ids(&projection, &expanded_rows),
            vec!["user_1", "reasoning_1", "tool_1", "agent_1"]
        );
    }

    #[test]
    fn expanded_turn_work_keeps_completed_empty_reasoning() {
        let projection = ConversationViewState {
            timeline: vec![
                timeline_entry("entry_user", "turn_parent", "user_1", 0),
                timeline_entry("entry_empty_reasoning", "turn_parent", "reasoning_empty", 1),
                timeline_entry("entry_tool", "turn_parent", "tool_1", 2),
                timeline_entry("entry_agent", "turn_parent", "agent_1", 3),
            ],
            items: vec![
                item_view(
                    "user_1",
                    "turn_parent",
                    "user_message",
                    TurnItem::UserMessage {
                        id: "user_1".to_owned(),
                        text: "Inspect project".to_owned(),
                        attachments: Vec::new(),
                    },
                    None,
                ),
                ItemView {
                    id: "reasoning_empty".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "reasoning".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(2),
                    updated_at_unix_ms: Some(3),
                    completed_at_unix_ms: Some(3),
                    partial_text: String::new(),
                    final_text: None,
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::Reasoning {
                        id: "reasoning_empty".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                item_view(
                    "tool_1",
                    "turn_parent",
                    "dynamic_tool_call",
                    TurnItem::DynamicToolCall {
                        id: "tool_1".to_owned(),
                        tool_name: "exec_command".to_owned(),
                        arguments: serde_json::json!({}),
                        status: ToolCallStatus::Completed,
                        recovery_policy: None,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("exec_command"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::None,
                        recovery: None,
                        success: Some(true),
                        outcome: None,
                        observation: None,
                    },
                    None,
                ),
                item_view(
                    "agent_1",
                    "turn_parent",
                    "agent_message",
                    TurnItem::AgentMessage {
                        id: "agent_1".to_owned(),
                        text: "Done".to_owned(),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                    None,
                ),
            ],
            ..ConversationViewState::default()
        };

        let collapsed_rows = build_timeline_group_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &collapsed_rows),
            vec!["user_1", "agent_1"]
        );

        let expanded_rows = build_timeline_group_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_user")]),
        );
        assert_eq!(
            visible_item_ids(&projection, &expanded_rows),
            vec!["user_1", "reasoning_empty", "tool_1", "agent_1"]
        );
    }

    #[test]
    fn commentary_agent_message_is_work_item_not_terminal_answer() {
        let projection = ConversationViewState {
            timeline: vec![
                timeline_entry("entry_user", "turn_parent", "user_1", 0),
                timeline_entry("entry_commentary", "turn_parent", "commentary_1", 1),
                timeline_entry("entry_tool", "turn_parent", "tool_1", 2),
                timeline_entry("entry_final", "turn_parent", "final_1", 3),
            ],
            items: vec![
                item_view(
                    "user_1",
                    "turn_parent",
                    "user_message",
                    TurnItem::UserMessage {
                        id: "user_1".to_owned(),
                        text: "Inspect project".to_owned(),
                        attachments: Vec::new(),
                    },
                    None,
                ),
                item_view(
                    "commentary_1",
                    "turn_parent",
                    "agent_message",
                    TurnItem::AgentMessage {
                        id: "commentary_1".to_owned(),
                        text: "I will inspect the project.".to_owned(),
                        phase: pioneer_protocol::AgentMessagePhase::Commentary,
                        markdown: None,
                        markdown_version: None,
                    },
                    None,
                ),
                item_view(
                    "tool_1",
                    "turn_parent",
                    "dynamic_tool_call",
                    TurnItem::DynamicToolCall {
                        id: "tool_1".to_owned(),
                        tool_name: "exec_command".to_owned(),
                        arguments: serde_json::json!({}),
                        status: ToolCallStatus::Completed,
                        recovery_policy: None,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("exec_command"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::None,
                        recovery: None,
                        success: Some(true),
                        outcome: None,
                        observation: None,
                    },
                    None,
                ),
                item_view(
                    "final_1",
                    "turn_parent",
                    "agent_message",
                    TurnItem::AgentMessage {
                        id: "final_1".to_owned(),
                        text: "Done".to_owned(),
                        phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
                        markdown: None,
                        markdown_version: None,
                    },
                    None,
                ),
            ],
            ..ConversationViewState::default()
        };

        let collapsed_rows = build_timeline_group_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &collapsed_rows),
            vec!["user_1", "final_1"]
        );

        let expanded_rows = build_timeline_group_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_user")]),
        );
        assert_eq!(
            visible_item_ids(&projection, &expanded_rows),
            vec!["user_1", "commentary_1", "tool_1", "final_1"]
        );
    }

    #[test]
    fn child_task_origin_rows_group_under_task_anchor() {
        let origin = pioneer_protocol::TimelineOrigin {
            kind: pioneer_protocol::TimelineOriginKind::ChildTurn,
            task_id: Some("task_1".to_owned()),
            run_id: Some("run_1".to_owned()),
            child_thread_id: Some("child_thread".to_owned()),
            child_turn_id: Some("child_turn".to_owned()),
            origin_event_id: None,
            origin_turn_item_id: Some("child_tool".to_owned()),
            origin_sequence: 1,
            occurred_at: 2,
            lane: pioneer_protocol::TimelineLane::ChildTool,
        };
        let projection = ConversationViewState {
            timeline: vec![
                timeline_entry("entry_task", "turn_parent", "task_anchor", 0),
                timeline_entry("entry_child_tool", "turn_parent", "child_tool", 1),
            ],
            items: vec![
                item_view(
                    "task_anchor",
                    "turn_parent",
                    "task",
                    TurnItem::Task {
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
                            child_thread_id: Some("child_thread".to_owned()),
                            child_turn_id: Some("child_turn".to_owned()),
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
                    None,
                ),
                item_view(
                    "child_tool",
                    "turn_parent",
                    "dynamic_tool_call",
                    TurnItem::DynamicToolCall {
                        id: "child_tool".to_owned(),
                        tool_name: "read_file".to_owned(),
                        arguments: serde_json::json!({}),
                        status: ToolCallStatus::Completed,
                        recovery_policy: None,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("read_file"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::None,
                        recovery: None,
                        success: Some(true),
                        outcome: None,
                        observation: None,
                    },
                    Some(origin),
                ),
            ],
            ..ConversationViewState::default()
        };

        let collapsed_rows = build_timeline_group_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &collapsed_rows),
            vec!["task_anchor"]
        );

        let expanded_rows = build_timeline_group_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_task")]),
        );
        assert_eq!(
            visible_item_ids(&projection, &expanded_rows),
            vec!["task_anchor", "child_tool"]
        );
    }

    #[test]
    fn running_task_coalesces_old_completed_child_tools() {
        let child_origin = |item_id: &str| pioneer_protocol::TimelineOrigin {
            kind: pioneer_protocol::TimelineOriginKind::ChildTurn,
            task_id: Some("task_1".to_owned()),
            run_id: Some("run_1".to_owned()),
            child_thread_id: Some("child_thread".to_owned()),
            child_turn_id: Some("child_turn".to_owned()),
            origin_event_id: None,
            origin_turn_item_id: Some(item_id.to_owned()),
            origin_sequence: 1,
            occurred_at: 2,
            lane: pioneer_protocol::TimelineLane::ChildTool,
        };
        let mut timeline = vec![timeline_entry(
            "entry_task",
            "turn_parent",
            "task_anchor",
            0,
        )];
        let mut items = vec![item_view(
            "task_anchor",
            "turn_parent",
            "task",
            TurnItem::Task {
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
                    child_thread_id: Some("child_thread".to_owned()),
                    child_turn_id: Some("child_turn".to_owned()),
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
            None,
        )];
        for index in 0..5 {
            let item_id = format!("child_tool_{index}");
            timeline.push(timeline_entry(
                &format!("entry_child_tool_{index}"),
                "turn_parent",
                item_id.as_str(),
                index + 1,
            ));
            items.push(item_view(
                item_id.as_str(),
                "turn_parent",
                "dynamic_tool_call",
                TurnItem::DynamicToolCall {
                    id: item_id.clone(),
                    tool_name: "read_file".to_owned(),
                    arguments: serde_json::json!({ "path": index }),
                    status: ToolCallStatus::Completed,
                    recovery_policy: None,
                    output_policy: ToolOutputPolicySnapshot::for_tool_name("read_file"),
                    display: ToolDisplayPayload::Hidden,
                    storage: ToolStoragePayload::None,
                    recovery: None,
                    success: Some(true),
                    outcome: None,
                    observation: None,
                },
                Some(child_origin(item_id.as_str())),
            ));
        }
        let projection = ConversationViewState {
            timeline,
            items,
            ..ConversationViewState::default()
        };

        let expanded_rows = build_timeline_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_task")]),
        );
        assert_eq!(
            visible_item_ids(&projection, &expanded_rows),
            vec!["task_anchor", "child_tool_3", "child_tool_4"]
        );
        let group = expanded_rows
            .iter()
            .find_map(|row| match &row.kind {
                TimelineRowKind::CoalescedTools(group) => Some(group),
                _ => None,
            })
            .expect("completed child tools should coalesce");
        assert_eq!(group.count, 3);
        assert_eq!(group.kind, TimelineCoalescedToolsKind::CompletedTaskTools);
    }

    #[test]
    fn repeated_task_wait_calls_coalesce_after_first_visible_row() {
        let mut timeline = Vec::new();
        let mut items = Vec::new();
        for index in 0..3 {
            let item_id = format!("wait_{index}");
            timeline.push(timeline_entry(
                &format!("entry_wait_{index}"),
                "turn_parent",
                item_id.as_str(),
                index,
            ));
            items.push(item_view(
                item_id.as_str(),
                "turn_parent",
                "dynamic_tool_call",
                TurnItem::DynamicToolCall {
                    id: item_id.clone(),
                    tool_name: "task_wait".to_owned(),
                    arguments: serde_json::json!({ "taskId": "task_1" }),
                    status: ToolCallStatus::Completed,
                    recovery_policy: None,
                    output_policy: ToolOutputPolicySnapshot::for_tool_name("task_wait"),
                    display: ToolDisplayPayload::Hidden,
                    storage: ToolStoragePayload::None,
                    recovery: None,
                    success: Some(true),
                    outcome: None,
                    observation: None,
                },
                None,
            ));
        }
        let projection = ConversationViewState {
            timeline,
            items,
            ..ConversationViewState::default()
        };

        let collapsed_rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &collapsed_rows),
            vec!["wait_0"]
        );
        let group = collapsed_rows
            .iter()
            .find_map(|row| match &row.kind {
                TimelineRowKind::CoalescedTools(group) => Some(group),
                _ => None,
            })
            .expect("repeated task_wait rows should coalesce");
        assert_eq!(group.count, 2);
        assert_eq!(group.kind, TimelineCoalescedToolsKind::RepeatedTaskWait);

        let expanded_rows =
            build_timeline_rows(&projection, &HashSet::from([group.toggle_key.clone()]));
        assert_eq!(
            visible_item_ids(&projection, &expanded_rows),
            vec!["wait_0", "wait_1", "wait_2"]
        );
    }

    #[test]
    fn user_approval_request_rows_pin_after_later_timeline_items() {
        let projection = ConversationViewState {
            timeline: vec![
                timeline_entry("entry_user", "turn_parent", "user_1", 0),
                timeline_entry("entry_approval", "turn_parent", "approval_1", 1),
                timeline_entry("entry_tool_after", "turn_parent", "tool_after", 2),
            ],
            items: vec![
                item_view(
                    "user_1",
                    "turn_parent",
                    "user_message",
                    TurnItem::UserMessage {
                        id: "user_1".to_owned(),
                        text: "Wait for review".to_owned(),
                        attachments: Vec::new(),
                    },
                    None,
                ),
                task_wait_user_approval_item("approval_1", "turn_parent"),
                item_view(
                    "tool_after",
                    "turn_parent",
                    "dynamic_tool_call",
                    TurnItem::DynamicToolCall {
                        id: "tool_after".to_owned(),
                        tool_name: "read_file".to_owned(),
                        arguments: serde_json::json!({ "path": "README.md" }),
                        status: ToolCallStatus::Completed,
                        recovery_policy: None,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("read_file"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::None,
                        recovery: None,
                        success: Some(true),
                        outcome: None,
                        observation: None,
                    },
                    None,
                ),
            ],
            ..ConversationViewState::default()
        };

        let rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &rows),
            vec!["user_1", "tool_after", "approval_1"]
        );
    }

    #[test]
    fn user_approval_request_rows_do_not_collapse_into_turn_work() {
        let projection = ConversationViewState {
            timeline: vec![
                timeline_entry("entry_user", "turn_parent", "user_1", 0),
                timeline_entry("entry_approval", "turn_parent", "approval_1", 1),
                timeline_entry("entry_agent", "turn_parent", "agent_1", 2),
            ],
            items: vec![
                item_view(
                    "user_1",
                    "turn_parent",
                    "user_message",
                    TurnItem::UserMessage {
                        id: "user_1".to_owned(),
                        text: "Review this".to_owned(),
                        attachments: Vec::new(),
                    },
                    None,
                ),
                task_wait_user_approval_item("approval_1", "turn_parent"),
                item_view(
                    "agent_1",
                    "turn_parent",
                    "agent_message",
                    TurnItem::AgentMessage {
                        id: "agent_1".to_owned(),
                        text: "Waiting for approval".to_owned(),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                    None,
                ),
            ],
            ..ConversationViewState::default()
        };

        let rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &rows),
            vec!["user_1", "agent_1", "approval_1"]
        );
        assert!(!rows.iter().any(|row| matches!(
            row.kind,
            TimelineRowKind::TurnWorkToggle(TurnWorkGroupRow { .. })
        )));
    }
}

#[cfg(test)]
mod desktop_parity_tests {
    use super::*;
    use crate::conversation::{
        Conversation, ConversationViewState, ItemView, TimelineEntry, TimelineEntryStatus,
        TurnPhase, TurnView,
    };
    use pioneer_protocol::{
        SystemEventLevel, TaskEvent, TaskEventPayload, TaskExecutorKind, TaskStatus,
        TaskTriggerKind, TaskTurnItem, ThreadHistoryEvent, ThreadHistoryEventPayload, TimelineItem,
        TimelineLane, TimelineOrigin, TimelineOriginKind, TimelinePayload, ToolCallStatus,
        ToolDisplayPayload, ToolOutputPolicySnapshot, ToolStoragePayload, Turn, TurnItem,
        TurnStatus, TurnTimelineResponse,
    };
    use std::collections::HashSet;

    fn timeline_entry(id: &str, turn_id: &str, item_id: &str, item_index: usize) -> TimelineEntry {
        TimelineEntry {
            id: id.to_owned(),
            turn_id: turn_id.to_owned(),
            item_id: item_id.to_owned(),
            item_index,
        }
    }

    fn task_anchor_item(
        id: &str,
        task_id: &str,
        turn_id: &str,
        run_id: &str,
        trigger_kind: TaskTriggerKind,
    ) -> ItemView {
        ItemView {
            id: id.to_owned(),
            turn_id: turn_id.to_owned(),
            item_type: "task".to_owned(),
            status: TimelineEntryStatus::Completed,
            started_at_unix_ms: Some(1),
            updated_at_unix_ms: Some(2),
            completed_at_unix_ms: Some(2),
            partial_text: "Task".to_owned(),
            final_text: Some("Task".to_owned()),
            partial_markdown: None,
            final_markdown: None,
            item: TurnItem::Task {
                item: TaskTurnItem {
                    id: id.to_owned(),
                    task_id: task_id.to_owned(),
                    run_id: Some(run_id.to_owned()),
                    parent_task_id: None,
                    root_task_id: None,
                    title: "Task".to_owned(),
                    status: TaskStatus::Completed,
                    trigger_kind,
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
                    updated_at: 2,
                },
            },
            timeline_origin: None,
            opaque_meta: None,
        }
    }

    fn task_timeline_origin(task_id: &str, run_id: &str) -> pioneer_protocol::TimelineOrigin {
        pioneer_protocol::TimelineOrigin {
            kind: pioneer_protocol::TimelineOriginKind::ChildTurn,
            task_id: Some(task_id.to_owned()),
            run_id: Some(run_id.to_owned()),
            child_thread_id: Some("child_thread".to_owned()),
            child_turn_id: Some("child_turn".to_owned()),
            origin_event_id: None,
            origin_turn_item_id: None,
            origin_sequence: 1,
            occurred_at: 1,
            lane: pioneer_protocol::TimelineLane::ChildTool,
        }
    }

    fn task_child_tool(id: &str, task_id: &str, run_id: &str) -> ItemView {
        ItemView {
            id: id.to_owned(),
            turn_id: run_id.to_owned(),
            item_type: "dynamic_tool_call".to_owned(),
            status: TimelineEntryStatus::Completed,
            started_at_unix_ms: Some(2),
            updated_at_unix_ms: Some(3),
            completed_at_unix_ms: Some(3),
            partial_text: "tool".to_owned(),
            final_text: Some("tool".to_owned()),
            partial_markdown: None,
            final_markdown: None,
            item: TurnItem::DynamicToolCall {
                id: id.to_owned(),
                tool_name: "read_file".to_owned(),
                arguments: serde_json::json!({"path": "file"}),
                status: ToolCallStatus::Completed,
                recovery_policy: None,
                output_policy: ToolOutputPolicySnapshot::for_tool_name("read_file"),
                display: ToolDisplayPayload::Hidden,
                storage: ToolStoragePayload::None,
                recovery: None,
                success: Some(true),
                outcome: None,
                observation: None,
            },
            timeline_origin: Some(task_timeline_origin(task_id, run_id)),
            opaque_meta: None,
        }
    }

    fn task_child_agent(id: &str, task_id: &str, run_id: &str) -> ItemView {
        ItemView {
            id: id.to_owned(),
            turn_id: run_id.to_owned(),
            item_type: "agent_message".to_owned(),
            status: TimelineEntryStatus::Completed,
            started_at_unix_ms: Some(3),
            updated_at_unix_ms: Some(4),
            completed_at_unix_ms: Some(4),
            partial_text: "Child result".to_owned(),
            final_text: Some("Child result".to_owned()),
            partial_markdown: None,
            final_markdown: None,
            item: TurnItem::AgentMessage {
                id: id.to_owned(),
                text: "Child result".to_owned(),
                phase: Default::default(),
                markdown: None,
                markdown_version: None,
            },
            timeline_origin: Some(task_timeline_origin(task_id, run_id)),
            opaque_meta: None,
        }
    }

    fn visible_item_ids<'a>(
        projection: &'a ConversationViewState,
        rows: &[TimelineRow],
    ) -> Vec<&'a str> {
        rows.iter()
            .filter_map(|row| match row.kind {
                TimelineRowKind::Item { timeline_index } => projection
                    .timeline
                    .get(timeline_index)
                    .map(|entry| entry.item_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
    }

    #[test]
    fn completed_parent_agent_message_collapses_turn_work_without_turn_metadata() {
        let projection = ConversationViewState {
            timeline: vec![
                timeline_entry("entry_user", "turn_parent", "user_1", 0),
                timeline_entry("entry_task_list", "turn_parent", "task_list_1", 1),
                timeline_entry("entry_task_cancel", "turn_parent", "task_cancel_1", 2),
                timeline_entry(
                    "entry_task_cancelled",
                    "turn_task_event",
                    "task_cancelled_1",
                    3,
                ),
                timeline_entry("entry_parent_agent", "turn_parent", "parent_agent_1", 4),
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
                    partial_text: "Cancel this task".to_owned(),
                    final_text: Some("Cancel this task".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::UserMessage {
                        id: "user_1".to_owned(),
                        text: "Cancel this task".to_owned(),
                        attachments: Vec::new(),
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "task_list_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "dynamic_tool_call".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(2),
                    updated_at_unix_ms: Some(2),
                    completed_at_unix_ms: Some(2),
                    partial_text: "task_list".to_owned(),
                    final_text: Some("task_list".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::DynamicToolCall {
                        id: "task_list_1".to_owned(),
                        tool_name: "task_list".to_owned(),
                        arguments: serde_json::json!({"ownerKind": "thread"}),
                        status: ToolCallStatus::Completed,
                        recovery_policy: None,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("task_list"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::None,
                        recovery: None,
                        success: Some(true),
                        outcome: None,
                        observation: None,
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "task_cancel_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "dynamic_tool_call".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(3),
                    updated_at_unix_ms: Some(3),
                    completed_at_unix_ms: Some(3),
                    partial_text: "task_cancel".to_owned(),
                    final_text: Some("task_cancel".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::DynamicToolCall {
                        id: "task_cancel_1".to_owned(),
                        tool_name: "task_cancel".to_owned(),
                        arguments: serde_json::json!({"taskId": "task_1"}),
                        status: ToolCallStatus::Completed,
                        recovery_policy: None,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("task_cancel"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::None,
                        recovery: None,
                        success: Some(true),
                        outcome: None,
                        observation: None,
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "task_cancelled_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "system_event".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(4),
                    updated_at_unix_ms: Some(4),
                    completed_at_unix_ms: Some(4),
                    partial_text: "Task cancelled".to_owned(),
                    final_text: Some("Task cancelled".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::SystemEvent {
                        id: "task_cancelled_1".to_owned(),
                        level: SystemEventLevel::Info,
                        message: "Task cancelled".to_owned(),
                        code: Some(pioneer_protocol::constants::events::TASK_CANCELLED.to_owned()),
                        details: None,
                    },
                    timeline_origin: Some(pioneer_protocol::TimelineOrigin {
                        kind: pioneer_protocol::TimelineOriginKind::TaskEvent,
                        task_id: Some("task_1".to_owned()),
                        run_id: None,
                        child_thread_id: None,
                        child_turn_id: None,
                        origin_event_id: Some("task_event_1".to_owned()),
                        origin_turn_item_id: None,
                        origin_sequence: 1,
                        occurred_at: 4,
                        lane: pioneer_protocol::TimelineLane::Task,
                    }),
                    opaque_meta: None,
                },
                ItemView {
                    id: "parent_agent_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "agent_message".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(5),
                    updated_at_unix_ms: Some(5),
                    completed_at_unix_ms: Some(5),
                    partial_text: "Task cancelled.".to_owned(),
                    final_text: Some("Task cancelled.".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::AgentMessage {
                        id: "parent_agent_1".to_owned(),
                        text: "Task cancelled.".to_owned(),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
            ],
            turns: Vec::new(),
            ..ConversationViewState::default()
        };

        let collapsed_rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &collapsed_rows),
            vec!["user_1", "parent_agent_1"]
        );
        assert!(collapsed_rows.iter().any(|row| matches!(
            row.kind,
            TimelineRowKind::TurnWorkToggle(TurnWorkGroupRow { .. })
        )));

        let expanded_rows = build_timeline_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_user")]),
        );
        assert_eq!(
            visible_item_ids(&projection, &expanded_rows),
            vec![
                "user_1",
                "task_list_1",
                "task_cancel_1",
                "task_cancelled_1",
                "parent_agent_1"
            ]
        );
    }

    #[test]
    fn completed_parent_work_collapses_when_other_turn_items_are_interleaved() {
        let mut projection = ConversationViewState {
            timeline: vec![
                timeline_entry("entry_user", "turn_parent", "user_1", 0),
                timeline_entry("entry_other_user", "turn_other", "other_user", 1),
                timeline_entry("entry_task_list", "turn_parent", "task_list_1", 2),
                timeline_entry("entry_other_agent", "turn_other", "other_agent", 3),
                timeline_entry("entry_parent_agent", "turn_parent", "parent_agent_1", 4),
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
                    partial_text: "List tasks".to_owned(),
                    final_text: Some("List tasks".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::UserMessage {
                        id: "user_1".to_owned(),
                        text: "List tasks".to_owned(),
                        attachments: Vec::new(),
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "other_user".to_owned(),
                    turn_id: "turn_other".to_owned(),
                    item_type: "user_message".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(2),
                    updated_at_unix_ms: Some(2),
                    completed_at_unix_ms: Some(2),
                    partial_text: "Other".to_owned(),
                    final_text: Some("Other".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::UserMessage {
                        id: "other_user".to_owned(),
                        text: "Other".to_owned(),
                        attachments: Vec::new(),
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "task_list_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "dynamic_tool_call".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(3),
                    updated_at_unix_ms: Some(3),
                    completed_at_unix_ms: Some(3),
                    partial_text: "task_list".to_owned(),
                    final_text: Some("task_list".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::DynamicToolCall {
                        id: "task_list_1".to_owned(),
                        tool_name: "task_list".to_owned(),
                        arguments: serde_json::json!({"ownerKind": "thread"}),
                        status: ToolCallStatus::Completed,
                        recovery_policy: None,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("task_list"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::None,
                        recovery: None,
                        success: Some(true),
                        outcome: None,
                        observation: None,
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "other_agent".to_owned(),
                    turn_id: "turn_other".to_owned(),
                    item_type: "agent_message".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(4),
                    updated_at_unix_ms: Some(4),
                    completed_at_unix_ms: Some(4),
                    partial_text: "Other done".to_owned(),
                    final_text: Some("Other done".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::AgentMessage {
                        id: "other_agent".to_owned(),
                        text: "Other done".to_owned(),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "parent_agent_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "agent_message".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(5),
                    updated_at_unix_ms: Some(5),
                    completed_at_unix_ms: Some(5),
                    partial_text: "Done".to_owned(),
                    final_text: Some("Done".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::AgentMessage {
                        id: "parent_agent_1".to_owned(),
                        text: "Done".to_owned(),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
            ],
            ..ConversationViewState::default()
        };

        let rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &rows),
            vec!["user_1", "other_user", "other_agent", "parent_agent_1"]
        );
        assert!(rows.iter().any(|row| {
            matches!(
                row.kind,
                TimelineRowKind::TurnWorkToggle(TurnWorkGroupRow { .. })
            )
        }));

        let expanded_rows = build_timeline_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_user")]),
        );
        assert_eq!(
            visible_item_ids(&projection, &expanded_rows),
            vec![
                "user_1",
                "task_list_1",
                "other_user",
                "other_agent",
                "parent_agent_1"
            ]
        );

        projection.timeline.swap(1, 2);
        let rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &rows),
            vec!["user_1", "other_user", "other_agent", "parent_agent_1"]
        );
    }

    #[test]
    fn hydrated_cancel_turn_with_composed_task_cancelled_event_collapses_work() {
        let workspace_id = "ws_000000000000000001";
        let thread_id = "thread_cancel";
        let turn_id = "turn_cancel";

        let task_list = TurnItem::DynamicToolCall {
            id: "task_list_call".to_owned(),
            tool_name: "task_list".to_owned(),
            arguments: serde_json::json!({"ownerKind": "thread"}),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("task_list"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::None,
            recovery: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };
        let task_cancel = TurnItem::DynamicToolCall {
            id: "task_cancel_call".to_owned(),
            tool_name: "task_cancel".to_owned(),
            arguments: serde_json::json!({"taskId": "task_1"}),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("task_cancel"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::None,
            recovery: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };

        let mut conversation = Conversation::new(thread_id);
        conversation.hydrate_history(&[
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 1,
                created_at: 1_000,
                payload: ThreadHistoryEventPayload::TurnStarted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: Turn {
                        id: turn_id.to_owned(),
                        status: TurnStatus::InProgress,
                        turn_kind: Default::default(),
                        origin: Default::default(),
                        error: None,
                        prompt_manifest: None,
                    },
                    input: Vec::new(),
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 2,
                created_at: 1_000,
                payload: ThreadHistoryEventPayload::ItemStarted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::UserMessage {
                        id: "user_cancel".to_owned(),
                        text: "Cancel this task".to_owned(),
                        attachments: Vec::new(),
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 3,
                created_at: 1_000,
                payload: ThreadHistoryEventPayload::ItemStarted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::Reasoning {
                        id: "reasoning_1".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 4,
                created_at: 1_000,
                payload: ThreadHistoryEventPayload::ItemCompleted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::UserMessage {
                        id: "user_cancel".to_owned(),
                        text: "Cancel this task".to_owned(),
                        attachments: Vec::new(),
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 5,
                created_at: 10_000,
                payload: ThreadHistoryEventPayload::ItemCompleted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::Reasoning {
                        id: "reasoning_1".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 6,
                created_at: 10_000,
                payload: ThreadHistoryEventPayload::ItemStarted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: task_list.clone(),
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 7,
                created_at: 10_000,
                payload: ThreadHistoryEventPayload::ItemCompleted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: task_list,
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 8,
                created_at: 10_000,
                payload: ThreadHistoryEventPayload::ItemStarted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::Reasoning {
                        id: "reasoning_2".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 9,
                created_at: 17_000,
                payload: ThreadHistoryEventPayload::ItemCompleted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::Reasoning {
                        id: "reasoning_2".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 10,
                created_at: 17_000,
                payload: ThreadHistoryEventPayload::ItemStarted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: task_cancel.clone(),
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 11,
                created_at: 17_000,
                payload: ThreadHistoryEventPayload::ItemCompleted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: task_cancel,
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 12,
                created_at: 17_000,
                payload: ThreadHistoryEventPayload::ItemStarted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::Reasoning {
                        id: "reasoning_3".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 13,
                created_at: 23_000,
                payload: ThreadHistoryEventPayload::ItemCompleted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::Reasoning {
                        id: "reasoning_3".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 14,
                created_at: 23_000,
                payload: ThreadHistoryEventPayload::ItemStarted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::AgentMessage {
                        id: "agent_final".to_owned(),
                        text: String::new(),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 15,
                created_at: 23_000,
                payload: ThreadHistoryEventPayload::ItemCompleted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::AgentMessage {
                        id: "agent_final".to_owned(),
                        text: "Task cancelled.".to_owned(),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 16,
                created_at: 23_000,
                payload: ThreadHistoryEventPayload::TurnCompleted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: Turn {
                        id: turn_id.to_owned(),
                        status: TurnStatus::Completed,
                        turn_kind: Default::default(),
                        origin: Default::default(),
                        error: None,
                        prompt_manifest: None,
                    },
                },
            },
        ]);

        conversation.apply_composed_turn_timeline(&TurnTimelineResponse {
            thread_id: thread_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            turn_id: turn_id.to_owned(),
            last_sequence: 1,
            items: vec![TimelineItem {
                id: "task:task_1:1".to_owned(),
                origin: TimelineOrigin {
                    kind: TimelineOriginKind::TaskEvent,
                    task_id: Some("task_1".to_owned()),
                    run_id: None,
                    child_thread_id: None,
                    child_turn_id: None,
                    origin_event_id: Some("task_event_cancelled".to_owned()),
                    origin_turn_item_id: None,
                    origin_sequence: 1,
                    occurred_at: 17_000,
                    lane: TimelineLane::Task,
                },
                payload: TimelinePayload::TaskEvent {
                    event: TaskEvent {
                        id: "task_event_cancelled".to_owned(),
                        task_id: "task_1".to_owned(),
                        run_id: None,
                        thread_id: None,
                        turn_id: None,
                        sequence: 1,
                        event_type: pioneer_protocol::constants::events::TASK_CANCELLED.to_owned(),
                        idempotency_key: None,
                        payload: TaskEventPayload::TaskCancelled {
                            task_id: "task_1".to_owned(),
                            reason: Some("User requested cancellation".to_owned()),
                            completed_at: 17,
                        },
                        created_at: 17,
                    },
                },
            }],
        });

        let rows = build_timeline_rows(conversation.projection(), &HashSet::new());
        assert_eq!(
            visible_item_ids(conversation.projection(), &rows),
            vec!["user_cancel", "agent_final"]
        );
        assert!(rows.iter().any(|row| matches!(
            row.kind,
            TimelineRowKind::TurnWorkToggle(TurnWorkGroupRow { .. })
        )));
    }

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
                    timeline_origin: None,
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
                    timeline_origin: Some(task_timeline_origin("task_1", "run_1")),
                    opaque_meta: None,
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
                    timeline_origin: Some(task_timeline_origin("task_1", "run_1")),
                    opaque_meta: None,
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
            timeline_origin: None,
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
                timeline_origin: Some(task_timeline_origin("task_1", "run_1")),
                opaque_meta: None,
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
    fn recurring_task_runs_with_same_task_id_use_distinct_work_groups() {
        let projection = ConversationViewState {
            timeline: vec![
                timeline_entry("entry_task_run_1", "run_1", "task_anchor_run_1", 0),
                timeline_entry("entry_child_tool_run_1", "run_1", "child_tool_run_1", 1),
                timeline_entry("entry_child_agent_run_1", "run_1", "child_agent_run_1", 2),
                timeline_entry("entry_task_run_2", "run_2", "task_anchor_run_2", 0),
                timeline_entry("entry_child_tool_run_2", "run_2", "child_tool_run_2", 1),
                timeline_entry("entry_child_agent_run_2", "run_2", "child_agent_run_2", 2),
            ],
            items: vec![
                task_anchor_item(
                    "task_anchor_run_1",
                    "task_1",
                    "run_1",
                    "run_1",
                    TaskTriggerKind::Immediate,
                ),
                task_child_tool("child_tool_run_1", "task_1", "run_1"),
                task_child_agent("child_agent_run_1", "task_1", "run_1"),
                task_anchor_item(
                    "task_anchor_run_2",
                    "task_1",
                    "run_2",
                    "run_2",
                    TaskTriggerKind::Immediate,
                ),
                task_child_tool("child_tool_run_2", "task_1", "run_2"),
                task_child_agent("child_agent_run_2", "task_1", "run_2"),
            ],
            ..ConversationViewState::default()
        };

        let collapsed_rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &collapsed_rows),
            vec!["task_anchor_run_1", "task_anchor_run_2"]
        );
        assert_eq!(
            collapsed_rows
                .iter()
                .filter(|row| matches!(row.kind, TimelineRowKind::TurnWorkToggle(_)))
                .count(),
            2
        );

        let run_1_expanded_rows = build_timeline_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_task_run_1")]),
        );
        assert_eq!(
            visible_item_ids(&projection, &run_1_expanded_rows),
            vec![
                "task_anchor_run_1",
                "child_tool_run_1",
                "child_agent_run_1",
                "task_anchor_run_2",
            ]
        );
    }

    #[test]
    fn task_group_members_render_under_anchor_even_when_timeline_order_is_early() {
        let projection = ConversationViewState {
            timeline: vec![
                timeline_entry("entry_child_tool", "run_1", "child_tool_1", 1),
                timeline_entry("entry_task_anchor", "run_1", "task_anchor_run_1", 0),
                timeline_entry("entry_child_agent", "run_1", "child_agent_1", 2),
            ],
            items: vec![
                task_child_tool("child_tool_1", "task_1", "run_1"),
                task_anchor_item(
                    "task_anchor_run_1",
                    "task_1",
                    "run_1",
                    "run_1",
                    TaskTriggerKind::Interval,
                ),
                task_child_agent("child_agent_1", "task_1", "run_1"),
            ],
            ..ConversationViewState::default()
        };

        let collapsed_rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &collapsed_rows),
            vec!["task_anchor_run_1"]
        );

        let expanded_rows = build_timeline_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_task_anchor")]),
        );
        assert_eq!(
            visible_item_ids(&projection, &expanded_rows),
            vec!["task_anchor_run_1", "child_tool_1", "child_agent_1"]
        );
    }

    #[test]
    fn stale_scheduled_task_anchor_does_not_steal_later_run_group() {
        let mut stale_anchor = match task_anchor_item(
            "task_task_1",
            "task_1",
            "run_1",
            "run_2",
            TaskTriggerKind::Interval,
        )
        .item
        {
            TurnItem::Task { item } => item,
            _ => unreachable!(),
        };
        stale_anchor.id = "task_task_1".to_owned();
        stale_anchor.trigger_kind = TaskTriggerKind::Interval;
        let mut run_2_anchor = match task_anchor_item(
            "task_run_run_2",
            "task_1",
            "run_2",
            "run_2",
            TaskTriggerKind::Interval,
        )
        .item
        {
            TurnItem::Task { item } => item,
            _ => unreachable!(),
        };
        run_2_anchor.id = "task_run_run_2".to_owned();
        run_2_anchor.trigger_kind = TaskTriggerKind::Interval;

        let projection = ConversationViewState {
            timeline: vec![
                TimelineEntry {
                    id: "entry_stale_run_1_anchor".to_owned(),
                    turn_id: "run_1".to_owned(),
                    item_id: "task_task_1".to_owned(),
                    item_index: 0,
                },
                TimelineEntry {
                    id: "entry_child_tool_run_1".to_owned(),
                    turn_id: "run_1".to_owned(),
                    item_id: "child_tool_run_1".to_owned(),
                    item_index: 1,
                },
                TimelineEntry {
                    id: "entry_run_2_anchor".to_owned(),
                    turn_id: "run_2".to_owned(),
                    item_id: "task_run_run_2".to_owned(),
                    item_index: 0,
                },
                TimelineEntry {
                    id: "entry_child_tool_run_2".to_owned(),
                    turn_id: "run_2".to_owned(),
                    item_id: "child_tool_run_2".to_owned(),
                    item_index: 1,
                },
            ],
            items: vec![
                ItemView {
                    id: "task_task_1".to_owned(),
                    turn_id: "run_1".to_owned(),
                    item_type: "task".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(1),
                    updated_at_unix_ms: Some(2),
                    completed_at_unix_ms: Some(2),
                    partial_text: "Task".to_owned(),
                    final_text: Some("Task".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::Task { item: stale_anchor },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                task_child_tool("child_tool_run_1", "task_1", "run_1"),
                ItemView {
                    id: "task_run_run_2".to_owned(),
                    turn_id: "run_2".to_owned(),
                    item_type: "task".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(3),
                    updated_at_unix_ms: Some(4),
                    completed_at_unix_ms: Some(4),
                    partial_text: "Task".to_owned(),
                    final_text: Some("Task".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::Task { item: run_2_anchor },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                task_child_tool("child_tool_run_2", "task_1", "run_2"),
            ],
            ..ConversationViewState::default()
        };

        let run_1_rows = build_timeline_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_stale_run_1_anchor")]),
        );
        let run_1_visible = visible_item_ids(&projection, &run_1_rows);
        assert!(run_1_visible.contains(&"child_tool_run_1"));
        assert!(!run_1_visible.contains(&"child_tool_run_2"));

        let run_2_rows = build_timeline_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_run_2_anchor")]),
        );
        let run_2_visible = visible_item_ids(&projection, &run_2_rows);
        assert!(!run_2_visible.contains(&"child_tool_run_1"));
        assert!(run_2_visible.contains(&"child_tool_run_2"));
        assert_eq!(
            run_2_rows
                .iter()
                .filter(|row| matches!(row.kind, TimelineRowKind::TurnWorkToggle(_)))
                .count(),
            2
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
                timeline_origin: None,
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
                    timeline_origin: None,
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
                    timeline_origin: None,
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
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                    timeline_origin: Some(task_timeline_origin("task_1", "run_1")),
                    opaque_meta: None,
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
                    timeline_origin: Some(task_timeline_origin("task_1", "run_1")),
                    opaque_meta: None,
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
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
            ],
            turns: vec![TurnView {
                id: "turn_parent".to_owned(),
                phase: TurnPhase::Completed,
                started_at_unix_ms: Some(1),
                completed_at_unix_ms: Some(5),
                error: None,
                resume: None,
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
