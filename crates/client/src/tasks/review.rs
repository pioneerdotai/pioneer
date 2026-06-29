//! Task review action state.

use crate::timeline::labels::{TaskWaitReviewDisplayItem, task_review_action_key};
use pioneer_protocol::{TaskAcceptParams, TaskCancelParams, TaskCancelScope, TaskReviseParams};
use std::collections::{HashMap, HashSet};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub enum TaskReviewAction {
    Accept,
    Revise,
    Cancel,
}

impl TaskReviewAction {
    pub fn key(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Revise => "revise",
            Self::Cancel => "cancel",
        }
    }

    pub fn protocol_action(self) -> &'static str {
        match self {
            Self::Accept => "task_accept",
            Self::Revise => "task_revise",
            Self::Cancel => "task_cancel",
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum TaskReviewPlanError {
    UserControlsNotAllowed,
    MissingTaskId,
    MissingRunId,
    MissingCandidateId,
    ActionNotAllowed { action: TaskReviewAction },
    BlankFeedback,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TaskReviewActionState {
    actions_in_flight: HashSet<String>,
    action_errors: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskReviewActionRequest<TParams> {
    pub action_key: String,
    pub candidate_id: String,
    pub params: TParams,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TaskReviewTarget {
    task_id: String,
    run_id: String,
    candidate_id: String,
}

impl TaskReviewActionState {
    pub fn is_in_flight(&self, candidate_id: &str, action: TaskReviewAction) -> bool {
        self.actions_in_flight
            .contains(task_review_action_key(candidate_id, action.key()).as_str())
    }

    pub fn any_in_flight(&self, candidate_id: &str) -> bool {
        [
            TaskReviewAction::Accept,
            TaskReviewAction::Revise,
            TaskReviewAction::Cancel,
        ]
        .iter()
        .any(|action| self.is_in_flight(candidate_id, *action))
    }

    pub fn error(&self, candidate_id: &str) -> Option<&str> {
        self.action_errors.get(candidate_id).map(String::as_str)
    }

    pub fn begin_action(&mut self, candidate_id: &str, action: TaskReviewAction) -> Option<String> {
        let action_key = task_review_action_key(candidate_id, action.key());
        if !self.actions_in_flight.insert(action_key.clone()) {
            return None;
        }

        self.action_errors.remove(candidate_id);
        Some(action_key)
    }

    pub fn finish_action(&mut self, action_key: &str, candidate_id: &str, error: Option<String>) {
        self.actions_in_flight.remove(action_key);
        if let Some(error) = error {
            self.action_errors.insert(candidate_id.to_owned(), error);
        }
    }

    pub fn set_error(&mut self, candidate_id: &str, error: impl Into<String>) {
        self.action_errors
            .insert(candidate_id.to_owned(), error.into());
    }
}

pub fn task_review_action_enabled(
    item: &TaskWaitReviewDisplayItem,
    action: TaskReviewAction,
    state: &TaskReviewActionState,
) -> bool {
    validate_task_review_target(item, action).is_ok() && !state.any_in_flight(&item.candidate_id)
}

pub fn plan_task_review_accept(
    item: &TaskWaitReviewDisplayItem,
    reason: Option<String>,
    state: &mut TaskReviewActionState,
) -> Result<Option<TaskReviewActionRequest<TaskAcceptParams>>, TaskReviewPlanError> {
    let target = validate_task_review_target(item, TaskReviewAction::Accept)?;
    let Some(action_key) = state.begin_action(&target.candidate_id, TaskReviewAction::Accept)
    else {
        return Ok(None);
    };

    Ok(Some(TaskReviewActionRequest {
        action_key,
        candidate_id: target.candidate_id.clone(),
        params: TaskAcceptParams {
            task_id: target.task_id,
            run_id: target.run_id,
            candidate_id: target.candidate_id,
            reason,
        },
    }))
}

pub fn plan_task_review_revise(
    item: &TaskWaitReviewDisplayItem,
    feedback: impl Into<String>,
    state: &mut TaskReviewActionState,
) -> Result<Option<TaskReviewActionRequest<TaskReviseParams>>, TaskReviewPlanError> {
    let target = validate_task_review_target(item, TaskReviewAction::Revise)?;
    let feedback = validate_revision_feedback(feedback.into().as_str())?;
    let Some(action_key) = state.begin_action(&target.candidate_id, TaskReviewAction::Revise)
    else {
        return Ok(None);
    };

    Ok(Some(TaskReviewActionRequest {
        action_key,
        candidate_id: target.candidate_id.clone(),
        params: TaskReviseParams {
            task_id: target.task_id,
            run_id: target.run_id,
            candidate_id: target.candidate_id,
            feedback,
            additional_instructions: Vec::new(),
        },
    }))
}

pub fn plan_task_review_cancel(
    item: &TaskWaitReviewDisplayItem,
    reason: Option<String>,
    state: &mut TaskReviewActionState,
) -> Result<Option<TaskReviewActionRequest<TaskCancelParams>>, TaskReviewPlanError> {
    let target = validate_task_review_target(item, TaskReviewAction::Cancel)?;
    let Some(action_key) = state.begin_action(&target.candidate_id, TaskReviewAction::Cancel)
    else {
        return Ok(None);
    };

    Ok(Some(TaskReviewActionRequest {
        action_key,
        candidate_id: target.candidate_id,
        params: TaskCancelParams {
            task_id: target.task_id,
            reason,
            scope: TaskCancelScope::AttachedSubtree,
        },
    }))
}

pub fn validate_revision_feedback(feedback: &str) -> Result<String, TaskReviewPlanError> {
    let feedback = feedback.trim();
    if feedback.is_empty() {
        return Err(TaskReviewPlanError::BlankFeedback);
    }

    Ok(feedback.to_owned())
}

fn validate_task_review_target(
    item: &TaskWaitReviewDisplayItem,
    action: TaskReviewAction,
) -> Result<TaskReviewTarget, TaskReviewPlanError> {
    if !item.user_controls_allowed() {
        return Err(TaskReviewPlanError::UserControlsNotAllowed);
    }
    if item.task_id.trim().is_empty() {
        return Err(TaskReviewPlanError::MissingTaskId);
    }
    let Some(run_id) = item
        .run_id
        .as_deref()
        .map(str::trim)
        .filter(|run_id| !run_id.is_empty())
    else {
        return Err(TaskReviewPlanError::MissingRunId);
    };
    if item.candidate_id.trim().is_empty() {
        return Err(TaskReviewPlanError::MissingCandidateId);
    }
    if !item.allows_action(action.protocol_action()) {
        return Err(TaskReviewPlanError::ActionNotAllowed { action });
    }

    Ok(TaskReviewTarget {
        task_id: item.task_id.trim().to_owned(),
        run_id: run_id.to_owned(),
        candidate_id: item.candidate_id.trim().to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn review_item() -> TaskWaitReviewDisplayItem {
        TaskWaitReviewDisplayItem {
            task_id: "task_1".to_owned(),
            run_id: Some("run_1".to_owned()),
            title: Some("Review task".to_owned()),
            status: Some("waiting_review".to_owned()),
            candidate_id: "candidate_1".to_owned(),
            candidate_status: Some("pending".to_owned()),
            review_mode: Some("user_approval".to_owned()),
            permission_mode: None,
            permission_source: None,
            user_approval_required: true,
            round: Some(1),
            summary: None,
            result_preview: None,
            extraction_error_preview: None,
            diagnostics: Vec::new(),
            max_revision_rounds: Some(2),
            remaining_revision_rounds: Some(1),
            allowed_actions: vec![
                "task_accept".to_owned(),
                "task_revise".to_owned(),
                "task_cancel".to_owned(),
            ],
            revision_blocked_reason: None,
        }
    }

    #[test]
    fn action_state_tracks_in_flight_and_errors_by_candidate() {
        let mut state = TaskReviewActionState::default();

        let action_key = state
            .begin_action("candidate_1", TaskReviewAction::Accept)
            .expect("action key");

        assert!(state.is_in_flight("candidate_1", TaskReviewAction::Accept));
        assert!(state.any_in_flight("candidate_1"));
        assert!(
            state
                .begin_action("candidate_1", TaskReviewAction::Accept)
                .is_none()
        );

        state.finish_action(action_key.as_str(), "candidate_1", Some("boom".to_owned()));

        assert!(!state.any_in_flight("candidate_1"));
        assert_eq!(state.error("candidate_1"), Some("boom"));
    }

    #[test]
    fn plan_accept_begins_state_and_builds_params() {
        let mut state = TaskReviewActionState::default();
        let request = plan_task_review_accept(
            &review_item(),
            Some("Accepted in desktop".to_owned()),
            &mut state,
        )
        .expect("valid plan")
        .expect("request");

        assert!(state.is_in_flight("candidate_1", TaskReviewAction::Accept));
        assert_eq!(request.action_key, "task-review:candidate_1:accept");
        assert_eq!(request.candidate_id, "candidate_1");
        assert_eq!(request.params.task_id, "task_1");
        assert_eq!(request.params.run_id, "run_1");
        assert_eq!(request.params.candidate_id, "candidate_1");
        assert_eq!(
            request.params.reason.as_deref(),
            Some("Accepted in desktop")
        );
    }

    #[test]
    fn plan_revise_trims_feedback_and_rejects_blank_feedback() {
        let mut state = TaskReviewActionState::default();
        let request = plan_task_review_revise(&review_item(), "  more detail  ", &mut state)
            .expect("valid plan")
            .expect("request");

        assert_eq!(request.params.feedback, "more detail");

        let mut state = TaskReviewActionState::default();
        assert_eq!(
            plan_task_review_revise(&review_item(), "   ", &mut state),
            Err(TaskReviewPlanError::BlankFeedback)
        );
        assert!(!state.any_in_flight("candidate_1"));
    }

    #[test]
    fn plan_cancel_uses_attached_subtree_scope_and_reason() {
        let mut state = TaskReviewActionState::default();
        let request = plan_task_review_cancel(
            &review_item(),
            Some("Cancelled during result review".to_owned()),
            &mut state,
        )
        .expect("valid plan")
        .expect("request");

        assert!(state.is_in_flight("candidate_1", TaskReviewAction::Cancel));
        assert_eq!(request.params.task_id, "task_1");
        assert_eq!(
            request.params.reason.as_deref(),
            Some("Cancelled during result review")
        );
        assert_eq!(request.params.scope, TaskCancelScope::AttachedSubtree);
    }

    #[test]
    fn plan_rejects_missing_run_and_disallowed_action() {
        let mut missing_run = review_item();
        missing_run.run_id = None;
        assert!(matches!(
            plan_task_review_accept(
                &missing_run,
                Some("Accepted in desktop".to_owned()),
                &mut TaskReviewActionState::default()
            ),
            Err(TaskReviewPlanError::MissingRunId)
        ));

        let mut disallowed = review_item();
        disallowed.allowed_actions = vec!["task_accept".to_owned()];
        assert!(matches!(
            plan_task_review_revise(
                &disallowed,
                "feedback",
                &mut TaskReviewActionState::default()
            ),
            Err(TaskReviewPlanError::ActionNotAllowed {
                action: TaskReviewAction::Revise
            })
        ));
    }

    #[test]
    fn task_review_action_enabled_matches_target_action_and_state() {
        let item = review_item();
        let mut state = TaskReviewActionState::default();

        assert!(task_review_action_enabled(
            &item,
            TaskReviewAction::Accept,
            &state
        ));

        state.begin_action("candidate_1", TaskReviewAction::Cancel);

        assert!(!task_review_action_enabled(
            &item,
            TaskReviewAction::Accept,
            &state
        ));
    }
}
