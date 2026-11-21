use crate::{
    AgentMemoryCandidateDecisionRecord, AgentMemoryCandidateListFilter, AgentMemoryCandidateRecord,
    AgentMemoryCandidateStatusUpdateRecord, AgentMemoryControlRecord, AgentMemoryEventRecord,
    AgentMemoryListFilter, AgentMemoryPolicyDecisionRecord, AgentMemoryQualityDecisionRecord,
    AgentMemoryQuarantineRecord, AgentMemoryRepairJobRecord, CrudStore, MemoryActorRecord,
    MemoryScopeResolution, MemoryWorkspaceGuard, NewAgentMemoryCandidate,
    NewAgentMemoryControlRecord, NewAgentMemoryEvent, NewAgentMemoryPolicyDecision,
    NewAgentMemoryQualityDecision, NewAgentMemoryQuarantine, NewAgentMemoryRepairJob,
    ResolveAgentMemoryQuarantine,
};
use anyhow::Result;
use pioneer_crud_access::policy;
use pioneer_protocol::{MemoryCandidateStatus, MemoryScope};

impl CrudStore {
    pub(crate) fn memory_store(&self) -> pioneer_crud_memory::MemoryStore {
        let store = pioneer_crud_memory::MemoryStore::new(self.connection.clone());
        #[cfg(any(test, feature = "test-support"))]
        let store = store.with_episodic_claim_test_state(&self.episodic_claim_test_state);
        store
    }

    pub async fn put_memory_payload(
        &self,
        id: &str,
        scope: MemoryScope,
        content: &str,
    ) -> Result<i64> {
        self.memory_store()
            .put_memory_payload(id, scope, content)
            .await
    }

    pub async fn get_memory_payload(&self, id: &str, scope: MemoryScope) -> Result<Option<String>> {
        self.memory_store().get_memory_payload(id, scope).await
    }

    pub async fn get_memory_payload_versioned(
        &self,
        id: &str,
        scope: MemoryScope,
    ) -> Result<Option<(String, i64)>> {
        self.memory_store()
            .get_memory_payload_versioned(id, scope)
            .await
    }

    pub async fn delete_memory_payload(
        &self,
        id: &str,
        scope: MemoryScope,
        repair_scope_hash: Option<&str>,
    ) -> Result<bool> {
        self.memory_store()
            .delete_memory_payload(id, scope, repair_scope_hash)
            .await
    }

    pub async fn search_memory_payloads(
        &self,
        query: &str,
        filter: crate::AgentMemoryListFilter,
        sensitivities: &[pioneer_protocol::MemorySensitivity],
        now_unix: i64,
        limit: u32,
    ) -> Result<Vec<(String, String)>> {
        self.memory_store()
            .search_memory_payloads(query, filter, sensitivities, now_unix, limit)
            .await
    }

    /// The full payload is read outside capacity by the existing backend. This
    /// short transaction revalidates owner, lifecycle, quarantine and (for a
    /// durable backend) payload revision before clearing repair-needed state.
    pub async fn confirm_agent_memory_payload_repair(
        &self,
        expected: &crate::AgentMemoryControlRecord,
        revision: Option<i64>,
        now_unix: i64,
    ) -> Result<()> {
        self.memory_store()
            .confirm_agent_memory_payload_repair(expected, revision, now_unix)
            .await
    }

    pub async fn resolve_memory_scope(&self, scope: MemoryScope) -> Result<MemoryScopeResolution> {
        self.memory_store().resolve_memory_scope(scope).await
    }

    pub async fn resolve_memory_scopes(
        &self,
        scopes: impl IntoIterator<Item = MemoryScope>,
    ) -> Result<Vec<MemoryScopeResolution>> {
        self.memory_store().resolve_memory_scopes(scopes).await
    }

    pub async fn insert_agent_memory_record(
        &self,
        record: NewAgentMemoryControlRecord,
        event: Option<NewAgentMemoryEvent>,
        event_timestamp_secs: i64,
    ) -> Result<AgentMemoryControlRecord> {
        self.memory_store()
            .insert_agent_memory_record(record, event, event_timestamp_secs)
            .await
    }

    pub async fn upsert_active_agent_memory_record(
        &self,
        record: NewAgentMemoryControlRecord,
        event: Option<NewAgentMemoryEvent>,
        event_timestamp_secs: i64,
    ) -> Result<AgentMemoryControlRecord> {
        self.memory_store()
            .upsert_active_agent_memory_record(record, event, event_timestamp_secs)
            .await
    }

    /// Publish a prepared memory payload. The expected active ID fences the
    /// read/put/publish gap: an INSERT race cannot attach B's frame to row A.
    /// Supersession and replacement publication share this SQL-only transaction.
    /// None means the active identity changed; the caller must reload/retry.
    pub async fn publish_agent_memory_record(
        &self,
        record: NewAgentMemoryControlRecord,
        expected_active_id: Option<String>,
        supersedes: Option<String>,
        canonical_identity: Option<pioneer_protocol::MemoryCanonicalKey>,
        payload_revision: Option<i64>,
        publication_content: Option<String>,
        now_unix: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        self.memory_store()
            .publish_agent_memory_record(
                record,
                expected_active_id,
                supersedes,
                canonical_identity,
                payload_revision,
                publication_content,
                now_unix,
            )
            .await
    }

    pub async fn get_agent_memory_by_identity(
        &self,
        scope: MemoryScope,
        namespace: &str,
        key: &str,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        self.memory_store()
            .get_agent_memory_by_identity(scope, namespace, key)
            .await
    }

    /// Bind an unchanged duplicate only while it is still the active target.
    pub async fn bind_agent_memory_identity(
        &self,
        memory_id: &str,
        identity: pioneer_protocol::MemoryCanonicalKey,
    ) -> Result<()> {
        self.memory_store()
            .bind_agent_memory_identity(memory_id, identity)
            .await
    }

    pub async fn get_agent_memory_record(
        &self,
        memory_id: &str,
        include_non_active: bool,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        self.memory_store()
            .get_agent_memory_record(memory_id, include_non_active)
            .await
    }

    pub async fn get_active_agent_memory_by_key(
        &self,
        scope: MemoryScope,
        namespace: Option<&str>,
        key: &str,
        workspace_guard: Option<MemoryWorkspaceGuard>,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        self.memory_store()
            .get_active_agent_memory_by_key(scope, namespace, key, workspace_guard)
            .await
    }

    pub async fn update_agent_memory_metadata(
        &self,
        memory_id: &str,
        metadata_json: Option<String>,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        self.memory_store()
            .update_agent_memory_metadata(memory_id, metadata_json, event_timestamp_secs)
            .await
    }

    pub async fn list_agent_memory_records(
        &self,
        filter: AgentMemoryListFilter,
    ) -> Result<Vec<AgentMemoryControlRecord>> {
        self.memory_store().list_agent_memory_records(filter).await
    }

    /// Read one bounded inventory page. The caller carries the last scanned
    /// (created_at, id), including rows later hidden by payload/read policy.
    pub async fn list_agent_memory_record_page(
        &self,
        filter: AgentMemoryListFilter,
        before: Option<(i64, String)>,
        now_unix: i64,
    ) -> Result<Vec<AgentMemoryControlRecord>> {
        self.memory_store()
            .list_agent_memory_record_page(filter, before, now_unix)
            .await
    }

    pub async fn mark_agent_memory_deleted(
        &self,
        memory_id: &str,
        actor: Option<MemoryActorRecord>,
        reason: Option<String>,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        self.memory_store()
            .mark_agent_memory_deleted(memory_id, actor, reason, event_timestamp_secs)
            .await
    }

    pub async fn mark_agent_memory_superseded(
        &self,
        memory_id: &str,
        superseded_by: &str,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        self.memory_store()
            .mark_agent_memory_superseded(memory_id, superseded_by, event_timestamp_secs)
            .await
    }

    pub async fn mark_agent_memory_expired(
        &self,
        memory_id: &str,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        self.memory_store()
            .mark_agent_memory_expired(memory_id, event_timestamp_secs)
            .await
    }

    pub async fn mark_agent_memory_repair_status(
        &self,
        memory_id: &str,
        repair_status: &str,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        self.memory_store()
            .mark_agent_memory_repair_status(memory_id, repair_status, event_timestamp_secs)
            .await
    }

    pub async fn record_agent_memory_access(
        &self,
        memory_id: &str,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        self.memory_store()
            .record_agent_memory_access(memory_id, event_timestamp_secs)
            .await
    }

    pub async fn list_agent_memory_events(
        &self,
        memory_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryEventRecord>> {
        self.memory_store()
            .list_agent_memory_events(memory_id, limit)
            .await
    }

    pub async fn list_agent_memory_candidate_events(
        &self,
        candidate_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryEventRecord>> {
        self.memory_store()
            .list_agent_memory_candidate_events(candidate_id, limit)
            .await
    }

    pub async fn list_workspace_agent_memory_events(
        &self,
        workspace_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryEventRecord>> {
        self.memory_store()
            .list_workspace_agent_memory_events(workspace_id, limit)
            .await
    }

    pub async fn create_agent_memory_quarantine_marker(
        &self,
        quarantine: NewAgentMemoryQuarantine,
    ) -> Result<AgentMemoryQuarantineRecord> {
        self.memory_store()
            .create_agent_memory_quarantine_marker(quarantine)
            .await
    }

    pub async fn get_active_agent_memory_quarantine(
        &self,
        memory_id: &str,
    ) -> Result<Option<AgentMemoryQuarantineRecord>> {
        self.memory_store()
            .get_active_agent_memory_quarantine(memory_id)
            .await
    }

    pub async fn list_active_agent_memory_quarantines(
        &self,
        memory_ids: &[String],
    ) -> Result<Vec<AgentMemoryQuarantineRecord>> {
        self.memory_store()
            .list_active_agent_memory_quarantines(memory_ids)
            .await
    }

    pub async fn resolve_agent_memory_quarantine(
        &self,
        resolution: ResolveAgentMemoryQuarantine,
    ) -> Result<Option<AgentMemoryQuarantineRecord>> {
        self.memory_store()
            .resolve_agent_memory_quarantine(resolution)
            .await
    }

    pub async fn list_agent_memory_quarantine_history(
        &self,
        memory_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryQuarantineRecord>> {
        self.memory_store()
            .list_agent_memory_quarantine_history(memory_id, limit)
            .await
    }

    pub async fn insert_agent_memory_candidate(
        &self,
        candidate: NewAgentMemoryCandidate,
        event_timestamp_secs: i64,
    ) -> Result<AgentMemoryCandidateRecord> {
        self.memory_store()
            .insert_agent_memory_candidate(candidate, event_timestamp_secs)
            .await
    }

    /// The dedupe decision and creation event share the serialized transaction.
    pub async fn insert_agent_memory_candidate_with_outcome(
        &self,
        mut candidate: NewAgentMemoryCandidate,
        event_timestamp_secs: i64,
    ) -> Result<(AgentMemoryCandidateRecord, bool)> {
        self.memory_store()
            .insert_agent_memory_candidate_with_outcome(candidate, event_timestamp_secs)
            .await
    }

    pub async fn get_agent_memory_candidate_by_dedupe(
        &self,
        scope: MemoryScope,
        namespace: Option<&str>,
        dedupe_key: &str,
        statuses: Vec<MemoryCandidateStatus>,
        workspace_guard: Option<MemoryWorkspaceGuard>,
    ) -> Result<Option<AgentMemoryCandidateRecord>> {
        self.memory_store()
            .get_agent_memory_candidate_by_dedupe(
                scope,
                namespace,
                dedupe_key,
                statuses,
                workspace_guard,
            )
            .await
    }

    pub async fn get_agent_memory_candidate(
        &self,
        candidate_id: &str,
        workspace_guard: Option<MemoryWorkspaceGuard>,
    ) -> Result<Option<AgentMemoryCandidateRecord>> {
        self.memory_store()
            .get_agent_memory_candidate(candidate_id, workspace_guard)
            .await
    }

    /// Compare metadata bytes inside the serialized transaction; preparation
    /// and any conflict reread/recompute belong outside this writer operation.
    pub async fn update_agent_memory_candidate_metadata_if_unchanged(
        &self,
        candidate_id: &str,
        expected_metadata_json: Option<String>,
        metadata_json: Option<String>,
    ) -> Result<Option<AgentMemoryCandidateRecord>> {
        self.memory_store()
            .update_agent_memory_candidate_metadata_if_unchanged(
                candidate_id,
                expected_metadata_json,
                metadata_json,
            )
            .await
    }

    pub async fn update_agent_memory_candidate_status(
        &self,
        update: AgentMemoryCandidateStatusUpdateRecord,
    ) -> Result<Option<AgentMemoryCandidateRecord>> {
        self.memory_store()
            .update_agent_memory_candidate_status(update)
            .await
    }

    pub async fn list_agent_memory_candidates(
        &self,
        filter: AgentMemoryCandidateListFilter,
    ) -> Result<Vec<AgentMemoryCandidateRecord>> {
        self.memory_store()
            .list_agent_memory_candidates(filter)
            .await
    }

    pub async fn decide_agent_memory_candidate(
        &self,
        decision: AgentMemoryCandidateDecisionRecord,
    ) -> Result<Option<AgentMemoryCandidateRecord>> {
        self.memory_store()
            .decide_agent_memory_candidate(decision)
            .await
    }

    pub async fn insert_agent_memory_policy_decision(
        &self,
        decision: NewAgentMemoryPolicyDecision,
    ) -> Result<AgentMemoryPolicyDecisionRecord> {
        self.memory_store()
            .insert_agent_memory_policy_decision(decision)
            .await
    }

    pub async fn list_agent_memory_policy_decisions_for_memory(
        &self,
        memory_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryPolicyDecisionRecord>> {
        self.memory_store()
            .list_agent_memory_policy_decisions_for_memory(memory_id, limit)
            .await
    }

    pub async fn list_agent_memory_policy_decisions_for_candidate(
        &self,
        candidate_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryPolicyDecisionRecord>> {
        self.memory_store()
            .list_agent_memory_policy_decisions_for_candidate(candidate_id, limit)
            .await
    }

    pub async fn list_agent_memory_policy_decisions_for_thread(
        &self,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryPolicyDecisionRecord>> {
        self.memory_store()
            .list_agent_memory_policy_decisions_for_thread(thread_id, limit)
            .await
    }

    pub async fn insert_agent_memory_quality_decision(
        &self,
        decision: NewAgentMemoryQualityDecision,
    ) -> Result<AgentMemoryQualityDecisionRecord> {
        self.memory_store()
            .insert_agent_memory_quality_decision(decision)
            .await
    }

    pub async fn list_agent_memory_quality_decisions_for_memory(
        &self,
        memory_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryQualityDecisionRecord>> {
        self.memory_store()
            .list_agent_memory_quality_decisions_for_memory(memory_id, limit)
            .await
    }

    pub async fn list_agent_memory_quality_decisions_for_candidate(
        &self,
        candidate_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryQualityDecisionRecord>> {
        self.memory_store()
            .list_agent_memory_quality_decisions_for_candidate(candidate_id, limit)
            .await
    }

    pub async fn list_agent_memory_quality_decisions_for_thread(
        &self,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryQualityDecisionRecord>> {
        self.memory_store()
            .list_agent_memory_quality_decisions_for_thread(thread_id, limit)
            .await
    }

    pub async fn enqueue_agent_memory_repair_job(
        &self,
        job: NewAgentMemoryRepairJob,
        event_timestamp_secs: i64,
    ) -> Result<AgentMemoryRepairJobRecord> {
        self.memory_store()
            .enqueue_agent_memory_repair_job(job, event_timestamp_secs)
            .await
    }

    pub async fn get_agent_memory_repair_job(
        &self,
        job_id: &str,
    ) -> Result<Option<AgentMemoryRepairJobRecord>> {
        self.memory_store()
            .get_agent_memory_repair_job(job_id)
            .await
    }

    pub async fn list_agent_memory_repair_jobs_for_memory(
        &self,
        memory_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryRepairJobRecord>> {
        self.memory_store()
            .list_agent_memory_repair_jobs_for_memory(memory_id, limit)
            .await
    }

    pub async fn claim_due_agent_memory_repair_jobs(
        &self,
        now_unix: i64,
        lock_ttl_secs: i64,
        locked_by: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryRepairJobRecord>> {
        self.memory_store()
            .claim_due_agent_memory_repair_jobs(now_unix, lock_ttl_secs, locked_by, limit)
            .await
    }

    pub async fn mark_agent_memory_repair_job_running(
        &self,
        job_id: &str,
        locked_by: &str,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryRepairJobRecord>> {
        self.memory_store()
            .mark_agent_memory_repair_job_running(job_id, locked_by, event_timestamp_secs)
            .await
    }

    pub async fn mark_agent_memory_repair_job_completed(
        &self,
        job_id: &str,
        locked_by: &str,
        result_json: Option<String>,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryRepairJobRecord>> {
        self.memory_store()
            .mark_agent_memory_repair_job_completed(
                job_id,
                locked_by,
                result_json,
                event_timestamp_secs,
            )
            .await
    }

    pub async fn mark_agent_memory_repair_job_failed(
        &self,
        job_id: &str,
        locked_by: &str,
        last_error: String,
        retry_at_unix: Option<i64>,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryRepairJobRecord>> {
        self.memory_store()
            .mark_agent_memory_repair_job_failed(
                job_id,
                locked_by,
                last_error,
                retry_at_unix,
                event_timestamp_secs,
            )
            .await
    }
}
