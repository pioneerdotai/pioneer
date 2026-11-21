use crate::{
    CrudStore, NewThreadEpisodicEmbeddingArtifactRecord, NewThreadEpisodicIndexJobRecord,
    NewThreadEpisodicItemRecord, ThreadEpisodicCanonicalItem,
    ThreadEpisodicEmbeddingArtifactRecord, ThreadEpisodicIndexJobRecord, ThreadEpisodicItemRecord,
    ThreadEpisodicRefillSourceCounts, ThreadEpisodicRefillThread, ThreadEpisodicSearchQuery,
    ThreadEpisodicSearchResult, ThreadEpisodicSourceReconcileOutcome,
};
use anyhow::Result;

impl CrudStore {
    pub async fn search_thread_episodic_projection(
        &self,
        request: &ThreadEpisodicSearchQuery,
    ) -> Result<ThreadEpisodicSearchResult> {
        self.memory_store()
            .search_thread_episodic_projection(request)
            .await
    }

    pub async fn delete_all_thread_episodic_items(&self) -> Result<u64> {
        self.memory_store().delete_all_thread_episodic_items().await
    }

    pub async fn delete_thread_episodic_items_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<u64> {
        self.memory_store()
            .delete_thread_episodic_items_for_workspace(workspace_id)
            .await
    }

    pub async fn delete_rebuildable_thread_episodic_items_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<u64> {
        self.memory_store()
            .delete_rebuildable_thread_episodic_items_for_workspace(workspace_id)
            .await
    }

    pub async fn delete_all_thread_episodic_exclusions(&self) -> Result<u64> {
        self.memory_store()
            .delete_all_thread_episodic_exclusions()
            .await
    }

    pub async fn delete_thread_episodic_exclusions_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<u64> {
        self.memory_store()
            .delete_thread_episodic_exclusions_for_workspace(workspace_id)
            .await
    }

    pub async fn delete_all_thread_episodic_index_jobs(&self) -> Result<u64> {
        self.memory_store()
            .delete_all_thread_episodic_index_jobs()
            .await
    }

    pub async fn delete_thread_episodic_index_jobs_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<u64> {
        self.memory_store()
            .delete_thread_episodic_index_jobs_for_workspace(workspace_id)
            .await
    }

    pub async fn delete_all_thread_episodic_thread_directory_entries(&self) -> Result<u64> {
        self.memory_store()
            .delete_all_thread_episodic_thread_directory_entries()
            .await
    }

    pub async fn delete_thread_episodic_thread_directory_entries_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<u64> {
        self.memory_store()
            .delete_thread_episodic_thread_directory_entries_for_workspace(workspace_id)
            .await
    }

    pub async fn thread_search_source_batch(
        &self,
        workspace_id: &str,
        ids: &[String],
    ) -> Result<
        Vec<(
            ThreadEpisodicItemRecord,
            ThreadEpisodicCanonicalItem,
            Option<String>,
        )>,
    > {
        self.memory_store()
            .thread_search_source_batch(workspace_id, ids)
            .await
    }

    pub async fn find_thread_episodic_item(
        &self,
        index_item_id: &str,
    ) -> Result<Option<ThreadEpisodicItemRecord>> {
        self.memory_store()
            .find_thread_episodic_item(index_item_id)
            .await
    }

    pub async fn find_thread_episodic_embedding_artifact(
        &self,
        artifact_id: &str,
    ) -> Result<Option<ThreadEpisodicEmbeddingArtifactRecord>> {
        self.memory_store()
            .find_thread_episodic_embedding_artifact(artifact_id)
            .await
    }

    pub async fn insert_thread_episodic_embedding_artifact_if_absent(
        &self,
        artifact: NewThreadEpisodicEmbeddingArtifactRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicEmbeddingArtifactRecord> {
        self.memory_store()
            .insert_thread_episodic_embedding_artifact_if_absent(artifact, now_unix)
            .await
    }

    pub async fn touch_thread_episodic_embedding_artifact(
        &self,
        artifact_id: &str,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicEmbeddingArtifactRecord>> {
        self.memory_store()
            .touch_thread_episodic_embedding_artifact(artifact_id, now_unix)
            .await
    }

    pub async fn upsert_thread_episodic_item(
        &self,
        item: NewThreadEpisodicItemRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicItemRecord> {
        self.memory_store()
            .upsert_thread_episodic_item(item, now_unix)
            .await
    }

    pub async fn reconcile_thread_episodic_source_version(
        &self,
        expected_source_payload: &str,
        mut item: NewThreadEpisodicItemRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicSourceReconcileOutcome> {
        self.memory_store()
            .reconcile_thread_episodic_source_version(expected_source_payload, item, now_unix)
            .await
    }

    pub async fn retire_thread_episodic_source_occurrence(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        expected_source_payload: Option<&str>,
        now_unix: i64,
    ) -> Result<ThreadEpisodicSourceReconcileOutcome> {
        self.memory_store()
            .retire_thread_episodic_source_occurrence(
                workspace_id,
                thread_id,
                turn_id,
                item_id,
                expected_source_payload,
                now_unix,
            )
            .await
    }

    pub async fn find_thread_episodic_item_by_source_identity(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        text_hash: &str,
    ) -> Result<Option<ThreadEpisodicItemRecord>> {
        self.memory_store()
            .find_thread_episodic_item_by_source_identity(
                workspace_id,
                thread_id,
                turn_id,
                item_id,
                text_hash,
            )
            .await
    }

    pub async fn list_thread_episodic_items_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicItemRecord>> {
        self.memory_store()
            .list_thread_episodic_items_for_thread(workspace_id, thread_id, limit)
            .await
    }

    pub async fn list_recallable_thread_episodic_items_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicItemRecord>> {
        self.memory_store()
            .list_recallable_thread_episodic_items_for_thread(workspace_id, thread_id, limit)
            .await
    }

    pub async fn find_thread_episodic_index_job(
        &self,
        job_id: &str,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.memory_store()
            .find_thread_episodic_index_job(job_id)
            .await
    }

    pub async fn find_thread_episodic_index_job_by_item(
        &self,
        index_item_id: &str,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.memory_store()
            .find_thread_episodic_index_job_by_item(index_item_id)
            .await
    }

    pub async fn delete_incomplete_thread_episodic_index_job(&self, job_id: &str) -> Result<bool> {
        self.memory_store()
            .delete_incomplete_thread_episodic_index_job(job_id)
            .await
    }

    pub async fn delete_thread_episodic_index_job_if_suppressed(
        &self,
        workspace_id: &str,
        job_id: &str,
    ) -> Result<bool> {
        self.memory_store()
            .delete_thread_episodic_index_job_if_suppressed(workspace_id, job_id)
            .await
    }

    pub async fn list_thread_episodic_refill_workspace_ids(&self) -> Result<Vec<String>> {
        self.memory_store()
            .list_thread_episodic_refill_workspace_ids()
            .await
    }

    pub async fn list_thread_episodic_refill_threads(
        &self,
    ) -> Result<Vec<ThreadEpisodicRefillThread>> {
        self.memory_store()
            .list_thread_episodic_refill_threads()
            .await
    }

    pub async fn list_thread_episodic_refill_threads_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<ThreadEpisodicRefillThread>> {
        self.memory_store()
            .list_thread_episodic_refill_threads_for_workspace(workspace_id)
            .await
    }

    pub async fn list_thread_episodic_refill_threads_for_workspace_and_event_ids(
        &self,
        workspace_id: &str,
        event_ids: &[&str],
    ) -> Result<Vec<ThreadEpisodicRefillThread>> {
        self.memory_store()
            .list_thread_episodic_refill_threads_for_workspace_and_event_ids(
                workspace_id,
                event_ids,
            )
            .await
    }

    pub async fn count_thread_episodic_refill_sources(
        &self,
    ) -> Result<ThreadEpisodicRefillSourceCounts> {
        self.memory_store()
            .count_thread_episodic_refill_sources()
            .await
    }

    pub async fn count_thread_episodic_refill_sources_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<ThreadEpisodicRefillSourceCounts> {
        self.memory_store()
            .count_thread_episodic_refill_sources_for_workspace(workspace_id)
            .await
    }

    pub async fn list_thread_episodic_refill_indexable_items(
        &self,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicItemRecord>> {
        self.memory_store()
            .list_thread_episodic_refill_indexable_items(limit)
            .await
    }

    pub async fn list_thread_episodic_refill_indexable_items_for_workspace(
        &self,
        workspace_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicItemRecord>> {
        self.memory_store()
            .list_thread_episodic_refill_indexable_items_for_workspace(workspace_id, limit)
            .await
    }

    pub async fn enqueue_thread_episodic_refill_index_jobs(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<usize> {
        self.memory_store()
            .enqueue_thread_episodic_refill_index_jobs(now_unix, limit)
            .await
    }

    pub async fn enqueue_thread_episodic_refill_index_jobs_for_workspace(
        &self,
        workspace_id: &str,
        now_unix: i64,
        limit: u64,
    ) -> Result<usize> {
        self.memory_store()
            .enqueue_thread_episodic_refill_index_jobs_for_workspace(workspace_id, now_unix, limit)
            .await
    }

    pub async fn count_incomplete_thread_episodic_index_jobs(&self) -> Result<u64> {
        self.memory_store()
            .count_incomplete_thread_episodic_index_jobs()
            .await
    }

    pub async fn count_incomplete_thread_episodic_index_jobs_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<u64> {
        self.memory_store()
            .count_incomplete_thread_episodic_index_jobs_for_workspace(workspace_id)
            .await
    }

    pub async fn terminal_thread_episodic_index_job_exists_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<bool> {
        self.memory_store()
            .terminal_thread_episodic_index_job_exists_for_workspace(workspace_id)
            .await
    }

    pub async fn unfinished_thread_episodic_index_job_exists_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<bool> {
        self.memory_store()
            .unfinished_thread_episodic_index_job_exists_for_workspace(workspace_id)
            .await
    }

    pub async fn thread_episodic_index_job_exists_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<bool> {
        self.memory_store()
            .thread_episodic_index_job_exists_for_workspace(workspace_id)
            .await
    }

    pub async fn count_canceled_thread_episodic_index_jobs_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<u64> {
        self.memory_store()
            .count_canceled_thread_episodic_index_jobs_for_workspace(workspace_id)
            .await
    }

    pub async fn list_canceled_thread_episodic_index_jobs_for_workspace(
        &self,
        workspace_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicIndexJobRecord>> {
        self.memory_store()
            .list_canceled_thread_episodic_index_jobs_for_workspace(workspace_id, limit)
            .await
    }

    pub async fn list_canceled_thread_episodic_index_jobs_for_workspace_threads_after_id(
        &self,
        workspace_id: &str,
        thread_ids: &[String],
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicIndexJobRecord>> {
        self.memory_store()
            .list_canceled_thread_episodic_index_jobs_for_workspace_threads_after_id(
                workspace_id,
                thread_ids,
                after_id,
                limit,
            )
            .await
    }

    pub async fn next_scheduled_thread_episodic_index_job_at_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<Option<i64>> {
        self.memory_store()
            .next_scheduled_thread_episodic_index_job_at_for_workspace(workspace_id)
            .await
    }

    pub async fn insert_thread_episodic_index_job_if_absent(
        &self,
        job: NewThreadEpisodicIndexJobRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicIndexJobRecord> {
        self.memory_store()
            .insert_thread_episodic_index_job_if_absent(job, now_unix)
            .await
    }

    pub async fn list_thread_episodic_index_jobs_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicIndexJobRecord>> {
        self.memory_store()
            .list_thread_episodic_index_jobs_for_thread(workspace_id, thread_id, limit)
            .await
    }

    pub async fn list_failed_or_stale_thread_episodic_index_jobs_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        stale_before_unix: i64,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicIndexJobRecord>> {
        self.memory_store()
            .list_failed_or_stale_thread_episodic_index_jobs_for_thread(
                workspace_id,
                thread_id,
                stale_before_unix,
                limit,
            )
            .await
    }

    pub async fn retry_failed_or_stale_thread_episodic_index_job(
        &self,
        job_id: &str,
        stale_before_unix: i64,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.memory_store()
            .retry_failed_or_stale_thread_episodic_index_job(job_id, stale_before_unix, now_unix)
            .await
    }

    pub async fn requeue_canceled_thread_episodic_index_job(
        &self,
        job_id: &str,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.memory_store()
            .requeue_canceled_thread_episodic_index_job(job_id, now_unix)
            .await
    }

    /// Recover only under exclusive workspace ownership or before runtime
    /// admission. Each bounded transaction confirms durable results, closes
    /// exhausted attempts, and requeues only the remaining execution budget.
    /// The count includes all three recovery outcomes.
    pub async fn requeue_running_thread_episodic_index_jobs_for_workspace(
        &self,
        workspace_id: &str,
        now_unix: i64,
        max_attempts: i64,
    ) -> Result<u64> {
        self.memory_store()
            .requeue_running_thread_episodic_index_jobs_for_workspace(
                workspace_id,
                now_unix,
                max_attempts,
            )
            .await
    }

    pub async fn list_due_thread_episodic_index_jobs_after(
        &self,
        now_unix: i64,
        after: Option<&ThreadEpisodicIndexJobRecord>,
        through: Option<&ThreadEpisodicIndexJobRecord>,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicIndexJobRecord>> {
        self.memory_store()
            .list_due_thread_episodic_index_jobs_after(now_unix, after, through, limit)
            .await
    }

    pub async fn thread_episodic_discovery_round_end(
        &self,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.memory_store()
            .thread_episodic_discovery_round_end(now_unix)
            .await
    }

    pub async fn next_scheduled_thread_episodic_index_job_at(&self) -> Result<Option<i64>> {
        self.memory_store()
            .next_scheduled_thread_episodic_index_job_at()
            .await
    }
}
