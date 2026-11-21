use crate::{
    CrudStore, NewThreadEpisodicExclusionRecord, NewThreadEpisodicRecallEventRecord,
    NewThreadEpisodicThreadDirectoryRecord, ThreadEpisodicExclusionRecord,
    ThreadEpisodicIndexAttemptOutcome, ThreadEpisodicIndexClaimBatch,
    ThreadEpisodicIndexJobCompletionUpdate, ThreadEpisodicIndexJobFailureUpdate,
    ThreadEpisodicIndexJobRecord, ThreadEpisodicItemIndexedUpdate, ThreadEpisodicItemRecord,
    ThreadEpisodicRecallEventRecord, ThreadEpisodicThreadDirectoryRecord,
    ThreadEpisodicThreadDirectorySelection,
};
use anyhow::Result;

impl CrudStore {
    /// Round-to-wait handoff: use the frozen discovery time as the lower bound,
    /// so deadlines crossed while processing the round are still returned.
    pub async fn next_future_thread_episodic_index_job_at(
        &self,
        after_unix: i64,
    ) -> Result<Option<i64>> {
        self.memory_store()
            .next_future_thread_episodic_index_job_at(after_unix)
            .await
    }

    pub async fn claim_thread_episodic_index_job_if_due(
        &self,
        job_id: &str,
        now_unix: i64,
        max_attempts: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.memory_store()
            .claim_thread_episodic_index_job_if_due(job_id, now_unix, max_attempts)
            .await
    }

    pub async fn claim_thread_episodic_index_job_from_snapshot(
        &self,
        snapshot: &ThreadEpisodicIndexJobRecord,
        now_unix: i64,
        max_attempts: i64,
        mutation_started: Option<&std::sync::atomic::AtomicBool>,
        accepting_claims: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.memory_store()
            .claim_thread_episodic_index_job_from_snapshot(
                snapshot,
                now_unix,
                max_attempts,
                mutation_started,
                accepting_claims,
            )
            .await
    }

    pub async fn claim_due_thread_episodic_index_jobs(
        &self,
        now_unix: i64,
        limit: u64,
        max_attempts: i64,
    ) -> Result<Vec<ThreadEpisodicIndexJobRecord>> {
        self.memory_store()
            .claim_due_thread_episodic_index_jobs(now_unix, limit, max_attempts)
            .await
    }

    /// Discover one bounded portion under a reader, then commit each candidate
    /// separately. Do not replay the portion after a lock/ambiguous commit error.
    /// Caller owns workspace execution; failures carry input snapshots only.
    pub async fn claim_due_thread_episodic_index_jobs_for_workspace(
        &self,
        workspace_id: &str,
        now_unix: i64,
        limit: u64,
        max_attempts: i64,
    ) -> Result<ThreadEpisodicIndexClaimBatch> {
        self.memory_store()
            .claim_due_thread_episodic_index_jobs_for_workspace(
                workspace_id,
                now_unix,
                limit,
                max_attempts,
            )
            .await
    }

    pub async fn complete_thread_episodic_index_job(
        &self,
        job_id: &str,
        update: ThreadEpisodicIndexJobCompletionUpdate,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.memory_store()
            .complete_thread_episodic_index_job(job_id, update, now_unix)
            .await
    }

    pub async fn complete_thread_episodic_index_attempt(
        &self,
        job_id: &str,
        expected_attempt_count: i64,
        expected_source_payload: &str,
        item_update: ThreadEpisodicItemIndexedUpdate,
        job_update: ThreadEpisodicIndexJobCompletionUpdate,
        now_unix: i64,
    ) -> Result<ThreadEpisodicIndexAttemptOutcome> {
        self.memory_store()
            .complete_thread_episodic_index_attempt(
                job_id,
                expected_attempt_count,
                expected_source_payload,
                item_update,
                job_update,
                now_unix,
            )
            .await
    }

    pub async fn fail_thread_episodic_index_job(
        &self,
        job_id: &str,
        update: ThreadEpisodicIndexJobFailureUpdate,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.memory_store()
            .fail_thread_episodic_index_job(job_id, update, now_unix)
            .await
    }

    pub async fn fail_thread_episodic_index_attempt(
        &self,
        job_id: &str,
        expected_attempt_count: i64,
        expected_source_payload: &str,
        update: ThreadEpisodicIndexJobFailureUpdate,
        now_unix: i64,
    ) -> Result<ThreadEpisodicIndexAttemptOutcome> {
        self.memory_store()
            .fail_thread_episodic_index_attempt(
                job_id,
                expected_attempt_count,
                expected_source_payload,
                update,
                now_unix,
            )
            .await
    }

    pub async fn requeue_thread_episodic_index_attempt(
        &self,
        job_id: &str,
        expected_attempt_count: i64,
        now_unix: i64,
        last_error: Option<&str>,
    ) -> Result<ThreadEpisodicIndexAttemptOutcome> {
        self.memory_store()
            .requeue_thread_episodic_index_attempt(
                job_id,
                expected_attempt_count,
                now_unix,
                last_error,
            )
            .await
    }

    pub async fn cancel_thread_episodic_index_attempt(
        &self,
        job_id: &str,
        expected_attempt_count: i64,
        last_error: &str,
        now_unix: i64,
    ) -> Result<ThreadEpisodicIndexAttemptOutcome> {
        self.memory_store()
            .cancel_thread_episodic_index_attempt(
                job_id,
                expected_attempt_count,
                last_error,
                now_unix,
            )
            .await
    }

    pub async fn fail_thread_episodic_index_attempt_without_source_validation(
        &self,
        job_id: &str,
        expected_attempt_count: i64,
        update: ThreadEpisodicIndexJobFailureUpdate,
        now_unix: i64,
    ) -> Result<ThreadEpisodicIndexAttemptOutcome> {
        self.memory_store()
            .fail_thread_episodic_index_attempt_without_source_validation(
                job_id,
                expected_attempt_count,
                update,
                now_unix,
            )
            .await
    }

    /// Writer-side state observation serializes with a possibly ambiguous claim
    /// commit. A reader-pool snapshot alone could still precede that commit.
    /// An unchanged input may only receive scheduling backoff; an observed
    /// Running attempt is settled using its exact identity and attempt fence.
    pub async fn settle_thread_episodic_owned_index_attempt(
        &self,
        expected: &ThreadEpisodicIndexJobRecord,
        input: Option<&ThreadEpisodicIndexJobRecord>,
        update: ThreadEpisodicIndexJobFailureUpdate,
        now_unix: i64,
    ) -> Result<()> {
        self.memory_store()
            .settle_thread_episodic_owned_index_attempt(expected, input, update, now_unix)
            .await
    }

    pub async fn recover_thread_episodic_index_attempt_after_persistence_error(
        &self,
        job_id: &str,
        expected_attempt_count: i64,
        update: ThreadEpisodicIndexJobFailureUpdate,
        now_unix: i64,
    ) -> Result<ThreadEpisodicIndexAttemptOutcome> {
        self.memory_store()
            .recover_thread_episodic_index_attempt_after_persistence_error(
                job_id,
                expected_attempt_count,
                update,
                now_unix,
            )
            .await
    }

    pub async fn cancel_thread_episodic_index_job(
        &self,
        job_id: &str,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.memory_store()
            .cancel_thread_episodic_index_job(job_id, last_error, now_unix)
            .await
    }

    pub async fn mark_thread_episodic_item_indexed(
        &self,
        index_item_id: &str,
        update: ThreadEpisodicItemIndexedUpdate,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicItemRecord>> {
        self.memory_store()
            .mark_thread_episodic_item_indexed(index_item_id, update, now_unix)
            .await
    }

    pub async fn mark_thread_episodic_item_failed(
        &self,
        index_item_id: &str,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicItemRecord>> {
        self.memory_store()
            .mark_thread_episodic_item_failed(index_item_id, now_unix)
            .await
    }

    pub async fn tombstone_thread_episodic_items_for_item(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        now_unix: i64,
    ) -> Result<Vec<ThreadEpisodicItemRecord>> {
        self.memory_store()
            .tombstone_thread_episodic_items_for_item(
                workspace_id,
                thread_id,
                turn_id,
                item_id,
                now_unix,
            )
            .await
    }

    pub async fn tombstone_thread_episodic_items_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        now_unix: i64,
    ) -> Result<Vec<ThreadEpisodicItemRecord>> {
        self.memory_store()
            .tombstone_thread_episodic_items_for_thread(workspace_id, thread_id, now_unix)
            .await
    }

    pub async fn find_thread_episodic_exclusion_by_item(
        &self,
        workspace_id: &str,
        thread_id: &str,
        index_item_id: &str,
    ) -> Result<Option<ThreadEpisodicExclusionRecord>> {
        self.memory_store()
            .find_thread_episodic_exclusion_by_item(workspace_id, thread_id, index_item_id)
            .await
    }

    pub async fn thread_episodic_source_occurrence_is_excluded(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
    ) -> Result<bool> {
        self.memory_store()
            .thread_episodic_source_occurrence_is_excluded(
                workspace_id,
                thread_id,
                turn_id,
                item_id,
            )
            .await
    }

    pub async fn exclude_thread_episodic_item(
        &self,
        exclusion: NewThreadEpisodicExclusionRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicExclusionRecord> {
        self.memory_store()
            .exclude_thread_episodic_item(exclusion, now_unix)
            .await
    }

    pub async fn list_thread_episodic_exclusions_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicExclusionRecord>> {
        self.memory_store()
            .list_thread_episodic_exclusions_for_thread(workspace_id, thread_id, limit)
            .await
    }

    pub async fn list_thread_episodic_recall_events_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicRecallEventRecord>> {
        self.memory_store()
            .list_thread_episodic_recall_events_for_thread(workspace_id, thread_id, limit)
            .await
    }

    pub async fn insert_thread_episodic_recall_event(
        &self,
        event: NewThreadEpisodicRecallEventRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicRecallEventRecord> {
        self.memory_store()
            .insert_thread_episodic_recall_event(event, now_unix)
            .await
    }

    pub async fn find_thread_episodic_thread_directory_entry(
        &self,
        workspace_id: &str,
        thread_id: &str,
    ) -> Result<Option<ThreadEpisodicThreadDirectoryRecord>> {
        self.memory_store()
            .find_thread_episodic_thread_directory_entry(workspace_id, thread_id)
            .await
    }

    pub async fn upsert_thread_episodic_thread_directory_entry(
        &self,
        record: NewThreadEpisodicThreadDirectoryRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicThreadDirectoryRecord> {
        self.memory_store()
            .upsert_thread_episodic_thread_directory_entry(record, now_unix)
            .await
    }

    pub async fn list_selectable_thread_episodic_thread_directory_entries(
        &self,
        selection: ThreadEpisodicThreadDirectorySelection,
    ) -> Result<Vec<ThreadEpisodicThreadDirectoryRecord>> {
        self.memory_store()
            .list_selectable_thread_episodic_thread_directory_entries(selection)
            .await
    }

    pub async fn list_thread_episodic_thread_directory_entries_for_workspace(
        &self,
        workspace_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicThreadDirectoryRecord>> {
        self.memory_store()
            .list_thread_episodic_thread_directory_entries_for_workspace(workspace_id, limit)
            .await
    }

    pub async fn list_thread_episodic_thread_directory_entries_for_workspace_scoped(
        &self,
        workspace_id: &str,
        allowed_thread_ids: &[String],
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicThreadDirectoryRecord>> {
        self.memory_store()
            .list_thread_episodic_thread_directory_entries_for_workspace_scoped(
                workspace_id,
                allowed_thread_ids,
                limit,
            )
            .await
    }

    pub async fn count_active_thread_episodic_items_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
    ) -> Result<i64> {
        self.memory_store()
            .count_active_thread_episodic_items_for_thread(workspace_id, thread_id)
            .await
    }
}
