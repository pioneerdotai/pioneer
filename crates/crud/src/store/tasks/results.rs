//! Task result candidates, review events, and final answers.

use crate::CrudStore;
use crate::TaskReviewInvariantSnapshot;
use anyhow::Result;
use pioneer_protocol::TaskResultCandidate;
use pioneer_protocol::TaskResultCandidateStatus;
use pioneer_protocol::TaskResultReviewEvent;
use std::collections::HashMap;
use std::collections::HashSet;

impl CrudStore {
    pub async fn upsert_task_result_candidate(
        &self,
        candidate: TaskResultCandidate,
    ) -> Result<TaskResultCandidate> {
        self.task_view_store()
            .upsert_task_result_candidate(candidate)
            .await
    }

    pub async fn get_task_result_candidate(&self, id: &str) -> Result<Option<TaskResultCandidate>> {
        self.task_view_store().get_task_result_candidate(id).await
    }

    pub async fn get_task_result_candidate_by_turn(
        &self,
        task_run_turn_id: &str,
    ) -> Result<Option<TaskResultCandidate>> {
        self.task_view_store()
            .get_task_result_candidate_by_turn(task_run_turn_id)
            .await
    }

    pub async fn list_task_result_candidates(
        &self,
        run_id: &str,
    ) -> Result<Vec<TaskResultCandidate>> {
        self.task_view_store()
            .list_task_result_candidates(run_id)
            .await
    }

    pub async fn get_accepted_task_result_candidate(
        &self,
        run_id: &str,
    ) -> Result<Option<TaskResultCandidate>> {
        self.task_view_store()
            .get_accepted_task_result_candidate(run_id)
            .await
    }

    pub async fn get_pending_task_result_candidate(
        &self,
        run_id: &str,
    ) -> Result<Option<TaskResultCandidate>> {
        self.task_view_store()
            .get_pending_task_result_candidate(run_id)
            .await
    }

    pub async fn update_task_result_candidate_resolution(
        &self,
        id: &str,
        status: TaskResultCandidateStatus,
        final_review_event_id: Option<&str>,
        resolved_at: Option<i64>,
        updated_at: i64,
    ) -> Result<Option<TaskResultCandidate>> {
        self.task_view_store()
            .update_task_result_candidate_resolution(
                id,
                status,
                final_review_event_id,
                resolved_at,
                updated_at,
            )
            .await
    }

    pub async fn upsert_task_result_review_event(
        &self,
        review_event: TaskResultReviewEvent,
    ) -> Result<TaskResultReviewEvent> {
        self.task_view_store()
            .upsert_task_result_review_event(review_event)
            .await
    }

    pub async fn get_task_result_review_event(
        &self,
        id: &str,
    ) -> Result<Option<TaskResultReviewEvent>> {
        self.task_view_store()
            .get_task_result_review_event(id)
            .await
    }

    pub async fn list_task_result_review_events(
        &self,
        candidate_id: &str,
    ) -> Result<Vec<TaskResultReviewEvent>> {
        self.task_view_store()
            .list_task_result_review_events(candidate_id)
            .await
    }

    pub async fn list_task_result_review_events_for_run(
        &self,
        run_id: &str,
    ) -> Result<Vec<TaskResultReviewEvent>> {
        self.task_view_store()
            .list_task_result_review_events_for_run(run_id)
            .await
    }

    pub async fn load_task_review_invariant_snapshot(
        &self,
    ) -> Result<Option<TaskReviewInvariantSnapshot>> {
        pioneer_crud_tasks::review_invariants::load_task_review_invariant_snapshot(&self.connection)
            .await
            .map(Some)
    }

    pub async fn get_task_result_candidates_by_turns(
        &self,
        ids: &[String],
    ) -> Result<HashMap<String, TaskResultCandidate>> {
        self.task_view_store()
            .get_task_result_candidates_by_turns(ids)
            .await
    }

    pub async fn get_final_agent_message_keys(
        &self,
        workspace: &str,
        targets: &[(String, String, String)],
    ) -> Result<HashSet<(String, String, String)>> {
        self.conversation_display_store()
            .get_final_agent_message_keys(workspace, targets)
            .await
    }
}
