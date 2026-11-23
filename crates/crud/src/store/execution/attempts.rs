use crate::{
    CrudStore, RunningAttemptDeadlineRepairCandidate, RunningTurnItemAttempt, TimeoutCandidate,
    TimeoutCandidateCursor, TurnItemAttemptDeadlines, TurnLivenessRecord,
};
use anyhow::Result;
use pioneer_protocol::TurnItemType;

impl CrudStore {
    pub async fn configure_turn_item_attempt_deadlines(
        &self,
        turn_id: &str,
        item_id: &str,
        heartbeat_at_unix: i64,
        lease_expires_at_unix: Option<i64>,
        idle_deadline_at_unix: Option<i64>,
        hard_deadline_at_unix: Option<i64>,
    ) -> Result<bool> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .configure_turn_item_attempt_deadlines(
                turn_id,
                item_id,
                heartbeat_at_unix,
                lease_expires_at_unix,
                idle_deadline_at_unix,
                hard_deadline_at_unix,
            )
            .await
    }

    pub async fn heartbeat_turn_item_attempt(
        &self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        heartbeat_at_unix: i64,
        lease_expires_at_unix: Option<i64>,
        idle_deadline_at_unix: Option<i64>,
        hard_deadline_at_unix: Option<i64>,
    ) -> Result<bool> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .heartbeat_turn_item_attempt(
                turn_id,
                item_id,
                item_type,
                heartbeat_at_unix,
                lease_expires_at_unix,
                idle_deadline_at_unix,
                hard_deadline_at_unix,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn heartbeat_turn_item_attempt_with_activity_kind(
        &self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        heartbeat_at_unix: i64,
        lease_expires_at_unix: Option<i64>,
        idle_deadline_at_unix: Option<i64>,
        hard_deadline_at_unix: Option<i64>,
        activity_kind: &str,
    ) -> Result<bool> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .heartbeat_turn_item_attempt_with_activity_kind(
                turn_id,
                item_id,
                item_type,
                heartbeat_at_unix,
                lease_expires_at_unix,
                idle_deadline_at_unix,
                hard_deadline_at_unix,
                activity_kind,
            )
            .await
    }

    pub async fn observe_turn_runtime_activity(
        &self,
        turn_id: &str,
        activity_kind: &str,
        observed_at_unix: i64,
    ) -> Result<bool> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .observe_turn_runtime_activity(turn_id, activity_kind, observed_at_unix)
            .await
    }

    pub async fn get_turn_liveness(&self, turn_id: &str) -> Result<Option<TurnLivenessRecord>> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .get_turn_liveness(turn_id)
            .await
    }

    pub async fn list_timeout_candidates(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<Vec<TimeoutCandidate>> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .list_timeout_candidates(now_unix, limit)
            .await
    }

    /// Continue the raw ID cursor even if runtime evidence or human wait causes
    /// the caller to skip all returned candidates. Reset only for a new source
    /// sweep. This keeps old blocked rows from hiding later expired attempts.
    pub async fn list_timeout_candidate_page(
        &self,
        now_unix: i64,
        limit: u64,
        after: Option<&TimeoutCandidateCursor>,
    ) -> Result<(Vec<TimeoutCandidate>, Option<TimeoutCandidateCursor>)> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .list_timeout_candidate_page(now_unix, limit, after)
            .await
    }

    pub async fn list_timeout_candidate_page_for_turn(
        &self,
        turn_id: &str,
        now_unix: i64,
        limit: u64,
        after: Option<&TimeoutCandidateCursor>,
    ) -> Result<(Vec<TimeoutCandidate>, Option<TimeoutCandidateCursor>)> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .list_timeout_candidate_page_for_turn(turn_id, now_unix, limit, after)
            .await
    }

    pub async fn suppress_timeout_candidate_recovery(
        &self,
        candidate: &TimeoutCandidate,
        reason: &str,
        context: serde_json::Value,
        now_unix: i64,
    ) -> Result<bool> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .suppress_timeout_candidate_recovery(candidate, reason, context, now_unix)
            .await
    }

    pub async fn list_running_attempts_missing_deadlines(
        &self,
        limit: u64,
    ) -> Result<Vec<RunningAttemptDeadlineRepairCandidate>> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .list_running_attempts_missing_deadlines(limit)
            .await
    }

    pub async fn list_running_turn_item_attempts_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<Vec<RunningTurnItemAttempt>> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .list_running_turn_item_attempts_for_turn(turn_id)
            .await
    }

    pub async fn get_running_turn_item_attempt(
        &self,
        turn_id: &str,
        item_id: &str,
    ) -> Result<Option<RunningTurnItemAttempt>> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .get_running_turn_item_attempt(turn_id, item_id)
            .await
    }

    pub async fn list_running_turn_item_attempts_by_type(
        &self,
        item_type: TurnItemType,
        limit: u64,
    ) -> Result<Vec<RunningTurnItemAttempt>> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .list_running_turn_item_attempts_by_type(item_type, limit)
            .await
    }

    /// Bounded keyset page; continuation reflects selected rows, not restored
    /// observers or successfully decoded execution classes.
    pub async fn list_running_turn_item_attempt_page_by_type(
        &self,
        item_type: TurnItemType,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<(Vec<RunningTurnItemAttempt>, Option<String>)> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .list_running_turn_item_attempt_page_by_type(item_type, after_id, limit)
            .await
    }

    pub async fn running_turn_item_attempt_upper_id_by_type(
        &self,
        item_type: TurnItemType,
    ) -> Result<Option<String>> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .running_turn_item_attempt_upper_id_by_type(item_type)
            .await
    }

    pub async fn list_running_turn_item_attempt_page_by_type_bounded(
        &self,
        item_type: TurnItemType,
        after_id: Option<&str>,
        upper_id: &str,
        limit: u64,
    ) -> Result<(Vec<RunningTurnItemAttempt>, Option<String>)> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .list_running_turn_item_attempt_page_by_type_bounded(
                item_type, after_id, upper_id, limit,
            )
            .await
    }

    pub async fn list_running_turn_item_attempt_page_by_type_bounded_for_turn(
        &self,
        turn_id: &str,
        item_type: TurnItemType,
        after_id: Option<&str>,
        upper_id: &str,
        limit: u64,
    ) -> Result<(Vec<RunningTurnItemAttempt>, Option<String>)> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .list_running_turn_item_attempt_page_by_type_bounded_for_turn(
                turn_id, item_type, after_id, upper_id, limit,
            )
            .await
    }

    pub async fn list_unqueued_timeout_candidates(
        &self,
        limit: u64,
    ) -> Result<Vec<TimeoutCandidate>> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .list_unqueued_timeout_candidates(limit)
            .await
    }

    pub async fn list_unqueued_timeout_candidate_page(
        &self,
        limit: u64,
        after: Option<&str>,
        upper: Option<&str>,
    ) -> Result<(Vec<TimeoutCandidate>, Option<String>)> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .list_unqueued_timeout_candidate_page(limit, after, upper)
            .await
    }

    pub async fn transition_timeout_candidate(
        &self,
        candidate: &TimeoutCandidate,
        now_unix: i64,
    ) -> Result<bool> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .transition_timeout_candidate(candidate, now_unix)
            .await
    }

    pub async fn get_timed_out_attempt_candidate(
        &self,
        attempt_id: &str,
    ) -> Result<Option<TimeoutCandidate>> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .get_timed_out_attempt_candidate(attempt_id)
            .await
    }

    pub async fn reactivate_timed_out_attempt(
        &self,
        attempt_id: &str,
        heartbeat_at_unix: i64,
        deadlines: TurnItemAttemptDeadlines,
    ) -> Result<bool> {
        pioneer_crud_execution::attempt_supervision::AttemptSupervision::new(&self.connection)
            .reactivate_timed_out_attempt(attempt_id, heartbeat_at_unix, deadlines)
            .await
    }
}
