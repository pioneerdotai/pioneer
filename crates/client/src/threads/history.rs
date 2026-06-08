//! Thread history helpers.

use pioneer_protocol::{
    ThreadHistoryEvent, ThreadHistoryEventPayload, ThreadHistoryParams, ThreadHistoryResponse,
    TurnItem, TurnTimelineParams, TurnTimelineResponse,
};

#[derive(Clone, Debug, PartialEq)]
pub enum ThreadHistoryLoadSuccessReduction {
    Apply(ThreadHistoryApplyReduction),
    IgnoreMismatchedResponse {
        expected_thread_id: String,
        actual_thread_id: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThreadHistoryApplyReduction {
    pub thread_id: String,
    pub workspace_id: String,
    pub clear_draft_thread_id: Option<String>,
    pub events: Vec<ThreadHistoryEvent>,
    pub timelines: Vec<TurnTimelineResponse>,
    pub mark_history_loaded: bool,
    pub sync_composer_model_selection: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadHistoryLoadFailureReduction {
    pub mark_history_loaded: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ComposedTurnTimelineRefreshReduction {
    pub thread_id: String,
    pub timeline: TurnTimelineResponse,
}

pub fn thread_history_params(
    thread_id: impl Into<String>,
    limit: Option<u32>,
) -> ThreadHistoryParams {
    ThreadHistoryParams {
        thread_id: thread_id.into(),
        limit,
    }
}

pub fn reduce_thread_history_load_success(
    expected_thread_id: &str,
    response: ThreadHistoryResponse,
    timelines: Vec<TurnTimelineResponse>,
) -> ThreadHistoryLoadSuccessReduction {
    if response.thread_id != expected_thread_id {
        return ThreadHistoryLoadSuccessReduction::IgnoreMismatchedResponse {
            expected_thread_id: expected_thread_id.to_owned(),
            actual_thread_id: response.thread_id,
        };
    }

    let clear_draft_thread_id = (!response.events.is_empty()).then(|| response.thread_id.clone());

    ThreadHistoryLoadSuccessReduction::Apply(ThreadHistoryApplyReduction {
        thread_id: response.thread_id,
        workspace_id: response.workspace_id,
        clear_draft_thread_id,
        events: response.events,
        timelines,
        mark_history_loaded: true,
        sync_composer_model_selection: true,
    })
}

pub fn reduce_thread_history_load_failure() -> ThreadHistoryLoadFailureReduction {
    ThreadHistoryLoadFailureReduction {
        mark_history_loaded: false,
    }
}

pub fn reduce_composed_turn_timeline_refresh_success(
    timeline: TurnTimelineResponse,
) -> ComposedTurnTimelineRefreshReduction {
    ComposedTurnTimelineRefreshReduction {
        thread_id: timeline.thread_id.clone(),
        timeline,
    }
}

pub fn turn_ids_with_task_anchors(response: &ThreadHistoryResponse) -> Vec<String> {
    let mut turn_ids = Vec::new();
    for event in &response.events {
        let has_task_anchor = match &event.payload {
            ThreadHistoryEventPayload::ItemStarted { item, .. }
            | ThreadHistoryEventPayload::ItemCompleted { item, .. }
            | ThreadHistoryEventPayload::ItemUpdated { item, .. } => {
                matches!(item, TurnItem::Task { .. })
            }
            _ => false,
        };
        if has_task_anchor && !turn_ids.iter().any(|turn_id| turn_id == &event.turn_id) {
            turn_ids.push(event.turn_id.clone());
        }
    }
    turn_ids
}

pub fn composed_task_turn_timeline_params(
    response: &ThreadHistoryResponse,
) -> Vec<TurnTimelineParams> {
    turn_ids_with_task_anchors(response)
        .into_iter()
        .map(|turn_id| composed_task_turn_timeline_param(response.thread_id.clone(), turn_id))
        .collect()
}

pub fn composed_task_turn_timeline_param(
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
) -> TurnTimelineParams {
    TurnTimelineParams {
        thread_id: thread_id.into(),
        turn_id: turn_id.into(),
        compose_tasks: true,
        include_collapsed_task_events: false,
        max_child_items_per_task: Some(500),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        SystemEventLevel, TaskExecutorKind, TaskStatus, TaskTriggerKind, TaskTurnItem,
        ThreadHistoryEvent, TurnItem,
    };

    fn event(turn_id: &str, item: TurnItem) -> ThreadHistoryEvent {
        ThreadHistoryEvent {
            turn_id: turn_id.to_owned(),
            sequence: 1,
            created_at: 1,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: "ws_a".to_owned(),
                thread_id: "thread_a".to_owned(),
                turn_id: turn_id.to_owned(),
                item,
            },
        }
    }

    #[test]
    fn history_turn_timeline_params_are_built_for_unique_task_anchor_turns() {
        let response = ThreadHistoryResponse {
            thread_id: "thread_a".to_owned(),
            workspace_id: "ws_a".to_owned(),
            events: vec![
                event(
                    "turn_1",
                    TurnItem::Task {
                        item: TaskTurnItem {
                            id: "task_anchor_1".to_owned(),
                            task_id: "task_1".to_owned(),
                            run_id: None,
                            parent_task_id: None,
                            root_task_id: None,
                            title: "Task".to_owned(),
                            status: TaskStatus::Running,
                            trigger_kind: TaskTriggerKind::Manual,
                            executor_kind: TaskExecutorKind::Agent,
                            child_thread_id: None,
                            child_turn_id: None,
                            agent_role: None,
                            depth: 0,
                            max_depth: 1,
                            next_fire_at: None,
                            result_preview: None,
                            error_preview: None,
                            created_at: 1,
                            updated_at: 1,
                        },
                    },
                ),
                event(
                    "turn_1",
                    TurnItem::Task {
                        item: TaskTurnItem {
                            id: "task_anchor_2".to_owned(),
                            task_id: "task_2".to_owned(),
                            run_id: None,
                            parent_task_id: None,
                            root_task_id: None,
                            title: "Task 2".to_owned(),
                            status: TaskStatus::Running,
                            trigger_kind: TaskTriggerKind::Manual,
                            executor_kind: TaskExecutorKind::Agent,
                            child_thread_id: None,
                            child_turn_id: None,
                            agent_role: None,
                            depth: 0,
                            max_depth: 1,
                            next_fire_at: None,
                            result_preview: None,
                            error_preview: None,
                            created_at: 1,
                            updated_at: 1,
                        },
                    },
                ),
                event(
                    "turn_2",
                    TurnItem::SystemEvent {
                        id: "system".to_owned(),
                        level: SystemEventLevel::Info,
                        message: "not a task".to_owned(),
                        code: None,
                        details: None,
                    },
                ),
            ],
        };

        let params = composed_task_turn_timeline_params(&response);

        assert_eq!(params.len(), 1);
        assert_eq!(params[0].thread_id, "thread_a");
        assert_eq!(params[0].turn_id, "turn_1");
        assert!(params[0].compose_tasks);
        assert!(!params[0].include_collapsed_task_events);
        assert_eq!(params[0].max_child_items_per_task, Some(500));
    }

    #[test]
    fn history_load_success_reduction_applies_matching_response() {
        let events = vec![event(
            "turn_1",
            TurnItem::SystemEvent {
                id: "system".to_owned(),
                level: SystemEventLevel::Info,
                message: "loaded".to_owned(),
                code: None,
                details: None,
            },
        )];
        let timelines = vec![TurnTimelineResponse {
            thread_id: "thread_a".to_owned(),
            workspace_id: "ws_a".to_owned(),
            turn_id: "turn_1".to_owned(),
            items: Vec::new(),
            last_sequence: 10,
        }];

        let reduction = reduce_thread_history_load_success(
            "thread_a",
            ThreadHistoryResponse {
                thread_id: "thread_a".to_owned(),
                workspace_id: "ws_a".to_owned(),
                events: events.clone(),
            },
            timelines.clone(),
        );

        let ThreadHistoryLoadSuccessReduction::Apply(apply) = reduction else {
            panic!("matching history response should apply");
        };
        assert_eq!(apply.thread_id, "thread_a");
        assert_eq!(apply.workspace_id, "ws_a");
        assert_eq!(apply.clear_draft_thread_id.as_deref(), Some("thread_a"));
        assert_eq!(apply.events, events);
        assert_eq!(apply.timelines, timelines);
        assert!(apply.mark_history_loaded);
        assert!(apply.sync_composer_model_selection);
    }

    #[test]
    fn history_load_success_reduction_does_not_clear_draft_for_empty_history() {
        let reduction = reduce_thread_history_load_success(
            "thread_a",
            ThreadHistoryResponse {
                thread_id: "thread_a".to_owned(),
                workspace_id: "ws_a".to_owned(),
                events: Vec::new(),
            },
            Vec::new(),
        );

        let ThreadHistoryLoadSuccessReduction::Apply(apply) = reduction else {
            panic!("matching empty history response should apply");
        };
        assert_eq!(apply.clear_draft_thread_id, None);
        assert!(apply.mark_history_loaded);
    }

    #[test]
    fn history_load_success_reduction_ignores_mismatched_response() {
        let reduction = reduce_thread_history_load_success(
            "thread_expected",
            ThreadHistoryResponse {
                thread_id: "thread_actual".to_owned(),
                workspace_id: "ws_a".to_owned(),
                events: Vec::new(),
            },
            Vec::new(),
        );

        assert_eq!(
            reduction,
            ThreadHistoryLoadSuccessReduction::IgnoreMismatchedResponse {
                expected_thread_id: "thread_expected".to_owned(),
                actual_thread_id: "thread_actual".to_owned(),
            }
        );
    }

    #[test]
    fn history_load_failure_reduction_marks_history_unloaded() {
        assert_eq!(
            reduce_thread_history_load_failure(),
            ThreadHistoryLoadFailureReduction {
                mark_history_loaded: false,
            }
        );
    }

    #[test]
    fn composed_turn_timeline_refresh_reduction_extracts_thread_lookup_key() {
        let timeline = TurnTimelineResponse {
            thread_id: "thread_a".to_owned(),
            workspace_id: "ws_a".to_owned(),
            turn_id: "turn_1".to_owned(),
            items: Vec::new(),
            last_sequence: 10,
        };

        let reduction = reduce_composed_turn_timeline_refresh_success(timeline.clone());

        assert_eq!(reduction.thread_id, "thread_a");
        assert_eq!(reduction.timeline, timeline);
    }
}
